//! Lease, deliver, acknowledge, reap, inspect.
//!
//! A lease is a row with an expiry, so a dispatcher that dies mid-flight is
//! recovered by any process that calls
//! [`ObservationJournalReaderV1::reap_expired_leases`] — no daemon, no external
//! coordinator, no in-memory state.

use rusqlite::{ErrorCode, OptionalExtension, ToSql, Transaction, params};

use crate::error::ObservationJournalError;
use crate::identity::{
    DeliveryReceiptIdV1, DispatchLeaseIdV1, ObservationIdV1, ObservationIdempotencyKeyV1,
    SourceSequenceV1,
};
use crate::inspection::{
    JournalInspectionFilterV1, JournalInspectionPageV1, JournalInspectionRowV1,
};
use crate::lease::{AttemptOutcomeV1, LeaseRequestV1, LeasedObservationV1};
use crate::port::ObservationJournalReaderV1;
use crate::receipt::{
    ObservationCommittedEffectV1, ObservationDeliveryReceiptV1, ObservationOutcomeV1,
    ProviderEffectSummaryV1,
};
use crate::settlement::SourceAuthorityV1;
use crate::state::DeliveryStateV1;

use super::SqliteObservationJournal;
use super::row::{
    LEASE_SELECT_COLUMNS, RECEIPT_SELECT_COLUMNS, decode_leased, decode_receipt, encode_json,
    read_u32, read_u64, sql_i64,
};
use crate::retention::RetentionPolicyV1;

const INSERT_RECEIPT: &str = r#"
INSERT INTO tdmem_observation_receipt_v1 (
    observation_id, attempt_number, receipt_id, idempotency_key, payload_sha256,
    extensions_digest, provider_id, provider_instance_id, registration_revision,
    state_generation_before, state_generation_after, outcome, committed_effect,
    provider_effect_summary_json, provider_receipt_digest, started_at_micros,
    finished_at_micros, warnings_json
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
"#;

const SELECT_JOURNAL_ROW: &str = r#"
SELECT payload_sha256, extensions_digest, provider_id, registration_revision
FROM tdmem_observation_journal_v1
WHERE observation_id = ?1
"#;

const SELECT_DELIVERY_STATE: &str = r#"
SELECT state FROM tdmem_observation_delivery_v1 WHERE observation_id = ?1
"#;

const SELECT_HIGHEST_RECEIPT_ATTEMPT: &str = r#"
SELECT COALESCE(MAX(attempt_number), 0) FROM tdmem_observation_receipt_v1
WHERE observation_id = ?1
"#;

/// Settles the delivery row from the attempt that is actually in flight.
///
/// The attempt number is *not* written here: the lease claim already consumed
/// it. Matching on it is what keeps a stale dispatcher — one whose lease was
/// reaped and whose row has since been leased again — from advancing a row that
/// belongs to a later attempt.
const ADVANCE_DELIVERY: &str = r#"
UPDATE tdmem_observation_delivery_v1
SET state = ?1, next_attempt_at_micros = ?2, last_outcome = ?3,
    last_committed_effect = ?4, last_receipt_id = ?5, last_provider_instance_id = ?6,
    lease_owner = NULL, lease_id = NULL, lease_expires_at_micros = NULL,
    updated_at_micros = ?7
WHERE observation_id = ?8 AND state = 'leased' AND attempt_number = ?9
"#;

/// Claims one row *and consumes an attempt number in the same statement*.
///
/// A reaped or lost lease never gives its number back, so two dispatchers can
/// never derive the same receipt id for one row, and a row whose leases keep
/// lapsing still walks towards `max_attempts` instead of retrying forever.
const CLAIM_LEASE: &str = r#"
UPDATE tdmem_observation_delivery_v1
SET state = 'leased', lease_id = ?1, lease_owner = ?2, lease_expires_at_micros = ?3,
    attempt_number = attempt_number + 1, last_provider_instance_id = ?4,
    updated_at_micros = ?5
WHERE idempotency_key = ?6 AND state = ?7 AND attempt_number = ?8
"#;

const REAP_LEASES: &str = r#"
UPDATE tdmem_observation_delivery_v1
SET state = 'pending', lease_id = NULL, lease_owner = NULL, lease_expires_at_micros = NULL,
    updated_at_micros = ?1
WHERE idempotency_key IN (
    SELECT idempotency_key FROM tdmem_observation_delivery_v1
    WHERE state = 'leased' AND lease_expires_at_micros <= ?1
    ORDER BY lease_expires_at_micros, idempotency_key
    LIMIT ?2)
"#;

const RELEASE_LEASE: &str = r#"
UPDATE tdmem_observation_delivery_v1
SET state = 'pending', lease_id = NULL, lease_owner = NULL, lease_expires_at_micros = NULL,
    next_attempt_at_micros = ?1, updated_at_micros = ?1
WHERE lease_id = ?2 AND state = 'leased'
"#;

const EXPIRE_PAST_DEADLINE: &str = r#"
SELECT d.idempotency_key, d.observation_id, d.attempt_number, d.state
FROM tdmem_observation_delivery_v1 d
JOIN tdmem_observation_journal_v1 j ON j.idempotency_key = d.idempotency_key
WHERE d.provider_id = ?1 AND d.registration_revision = ?2
  AND d.state IN ('pending', 'effect_unknown')
  AND j.deadline_micros <= ?3
ORDER BY d.source_sequence, d.idempotency_key
LIMIT ?4
"#;

/// Rows that have consumed every attempt the policy allows.
///
/// Attempts are consumed by the claim, so a row can arrive here having never
/// produced a receipt — every one of its leases lapsed. It is terminalized with
/// a receipt of its own rather than being leased a further time, which is what
/// makes `max_attempts` a real bound rather than a bound on *recorded* attempts.
const SELECT_ATTEMPTS_EXHAUSTED: &str = r#"
SELECT d.idempotency_key, d.observation_id, d.attempt_number, d.state
FROM tdmem_observation_delivery_v1 d
WHERE d.provider_id = ?1 AND d.registration_revision = ?2
  AND d.state IN ('pending', 'effect_unknown')
  AND d.attempt_number >= ?3
ORDER BY d.source_sequence, d.idempotency_key
LIMIT ?4
"#;

pub(crate) struct ExpireDeliveryRequest<'a> {
    pub(crate) observation_id: &'a str,
    pub(crate) idempotency_key: &'a str,
    pub(crate) attempt_number: u32,
    pub(crate) current_state: DeliveryStateV1,
    pub(crate) next_state: DeliveryStateV1,
    pub(crate) outcome: ObservationOutcomeV1,
    pub(crate) reason: &'a str,
    pub(crate) now_unix_micros: i64,
}

impl SqliteObservationJournal {
    /// Marks one delivery terminal with a terminal receipt of its own rather
    /// than deleting it. ADR-0005 invariant 7: nothing is silently dropped.
    ///
    /// The terminal receipt takes a slot no receipt already occupies —
    /// `max(attempts consumed, highest receipt attempt) + 1` — because a
    /// dispatcher whose lease lapsed may have landed a receipt for an attempt
    /// the delivery row never learned about. If the insert still reports a
    /// collision the whole call fails closed and the enclosing transaction
    /// rolls back: silently proceeding would point `last_receipt_id` at a
    /// receipt describing an entirely different outcome, erasing the settlement
    /// proof of an attempt that really happened.
    pub(crate) fn expire_delivery(
        transaction: &Transaction<'_>,
        request: ExpireDeliveryRequest<'_>,
    ) -> Result<(), ObservationJournalError> {
        let ExpireDeliveryRequest {
            observation_id,
            idempotency_key,
            attempt_number,
            current_state,
            next_state,
            outcome,
            reason,
            now_unix_micros,
        } = request;
        if !current_state.can_transition_to(next_state) {
            return Err(ObservationJournalError::Corrupt {
                table: "tdmem_observation_delivery_v1",
                field: "state",
            });
        }
        let parsed = ObservationIdV1::parse(observation_id)?;
        let journal = read_journal_row(transaction, observation_id)?;
        let highest_receipt: i64 = transaction.query_row(
            SELECT_HIGHEST_RECEIPT_ATTEMPT,
            params![observation_id],
            |row| row.get(0),
        )?;
        let attempt = attempt_number
            .max(read_u32(highest_receipt, "attempt_number")?)
            .saturating_add(1);
        let receipt_id = DeliveryReceiptIdV1::derive(&parsed, attempt);
        let summary = ProviderEffectSummaryV1 {
            effect_count: 0,
            stable_memory_refs: Vec::new(),
            provider_trace_refs: Vec::new(),
            no_effect_reason: Some(reason.to_owned()),
        };
        let inserted = insert_receipt_row(
            transaction,
            observation_id,
            attempt,
            receipt_id.as_str(),
            idempotency_key,
            &journal.payload_sha256,
            &journal.extensions_digest,
            journal.provider_id.as_str(),
            // No provider instance handled this attempt: the journal terminated
            // it, so the receipt records no instance rather than inventing one.
            None,
            journal.registration_revision,
            None,
            None,
            outcome,
            ObservationCommittedEffectV1::None,
            &encode_json(&summary, "provider_effect_summary_json")?,
            None,
            now_unix_micros,
            now_unix_micros,
        )?;
        if !inserted {
            return Err(ObservationJournalError::Corrupt {
                table: "tdmem_observation_receipt_v1",
                field: "attempt_number",
            });
        }
        let changed = transaction.execute(
            "UPDATE tdmem_observation_delivery_v1 \
             SET state = ?1, attempt_number = ?2, last_outcome = ?3, last_committed_effect = 'none', \
                 last_receipt_id = ?4, lease_owner = NULL, lease_id = NULL, \
                 lease_expires_at_micros = NULL, updated_at_micros = ?5 \
             WHERE idempotency_key = ?6 AND state = ?7",
            params![
                next_state.as_wire(),
                i64::from(attempt),
                outcome.as_wire(),
                receipt_id.as_str(),
                now_unix_micros,
                idempotency_key,
                current_state.as_wire(),
            ],
        )?;
        if changed != 1 {
            // The row moved under us between the read and this write. Refuse
            // rather than terminalize a state we never inspected.
            return Err(ObservationJournalError::Corrupt {
                table: "tdmem_observation_delivery_v1",
                field: "state",
            });
        }
        Ok(())
    }
}

/// The immutable journal facts a receipt must agree with.
struct JournalFactsV1 {
    payload_sha256: String,
    extensions_digest: String,
    provider_id: String,
    registration_revision: i64,
}

/// Terminalizes every row one bounded selection returns.
///
/// `select` must yield `(idempotency_key, observation_id, attempt_number,
/// state)` and must be bounded by the caller. Each row gets a terminal receipt
/// of its own, so a delivery that ends without a provider ever answering is
/// still auditable rather than silently dropped.
pub(crate) fn terminalize_deliveries(
    transaction: &Transaction<'_>,
    select: &str,
    parameters: &[&dyn ToSql],
    next_state: DeliveryStateV1,
    outcome: ObservationOutcomeV1,
    reason: &str,
    now_unix_micros: i64,
) -> Result<u32, ObservationJournalError> {
    let mut selected: Vec<(String, String, u32, DeliveryStateV1)> = Vec::new();
    {
        let mut statement = transaction.prepare(select)?;
        let mut rows = statement.query(parameters)?;
        while let Some(row) = rows.next()? {
            selected.push((
                row.get(0)?,
                row.get(1)?,
                read_u32(row.get::<_, i64>(2)?, "attempt_number")?,
                DeliveryStateV1::from_wire(&row.get::<_, String>(3)?)?,
            ));
        }
    }
    let mut terminalized = 0_u32;
    for (idempotency_key, observation_id, attempt_number, current_state) in selected {
        SqliteObservationJournal::expire_delivery(
            transaction,
            ExpireDeliveryRequest {
                observation_id: &observation_id,
                idempotency_key: &idempotency_key,
                attempt_number,
                current_state,
                next_state,
                outcome,
                reason,
                now_unix_micros,
            },
        )?;
        terminalized = terminalized.saturating_add(1);
    }
    Ok(terminalized)
}

/// What settling one delivery row from one receipt did.
struct SettlementV1 {
    state: DeliveryStateV1,
    next_attempt_at_unix_micros: i64,
    advanced: bool,
}

/// Applies one receipt to the delivery row it describes.
///
/// The update only lands while the row is still leased against exactly that
/// attempt, so a receipt arriving after the lease was reaped is recorded but
/// cannot disturb whatever attempt owns the row now.
fn settle_delivery(
    transaction: &Transaction<'_>,
    receipt: &ObservationDeliveryReceiptV1,
    policy: &RetentionPolicyV1,
) -> Result<SettlementV1, ObservationJournalError> {
    let retryable = receipt.is_retryable();
    let exhausted = retryable && receipt.attempt_number >= policy.max_attempts;
    let state = if exhausted {
        DeliveryStateV1::Exhausted
    } else {
        receipt.implied_state()
    };
    let next_attempt_at_unix_micros = if exhausted || !retryable {
        receipt.finished_at_unix_micros
    } else {
        receipt
            .finished_at_unix_micros
            .saturating_add(policy.next_attempt_delay(receipt.attempt_number))
    };
    let changed = transaction.execute(
        ADVANCE_DELIVERY,
        params![
            state.as_wire(),
            next_attempt_at_unix_micros,
            receipt.outcome.as_wire(),
            receipt.committed_effect.as_wire(),
            receipt.receipt_id.as_str(),
            receipt.provider_instance_id.as_deref(),
            receipt.finished_at_unix_micros,
            receipt.observation_id.as_str(),
            i64::from(receipt.attempt_number),
        ],
    )?;
    Ok(SettlementV1 {
        state,
        next_attempt_at_unix_micros,
        advanced: changed == 1,
    })
}

fn read_delivery_state(
    transaction: &Transaction<'_>,
    observation_id: &str,
) -> Result<DeliveryStateV1, ObservationJournalError> {
    let state: Option<String> = transaction
        .query_row(SELECT_DELIVERY_STATE, params![observation_id], |row| {
            row.get(0)
        })
        .optional()?;
    let state = state.ok_or_else(|| ObservationJournalError::UnknownObservation {
        observation_id: observation_id.to_owned(),
    })?;
    DeliveryStateV1::from_wire(&state)
}

fn read_receipt(
    transaction: &Transaction<'_>,
    observation_id: &str,
    attempt_number: u32,
) -> Result<ObservationDeliveryReceiptV1, ObservationJournalError> {
    let select = format!(
        "SELECT {RECEIPT_SELECT_COLUMNS} FROM tdmem_observation_receipt_v1 \
         WHERE observation_id = ?1 AND attempt_number = ?2"
    );
    let mut statement = transaction.prepare(&select)?;
    let mut rows = statement.query(params![observation_id, i64::from(attempt_number)])?;
    let row = rows.next()?.ok_or(ObservationJournalError::Corrupt {
        table: "tdmem_observation_receipt_v1",
        field: "attempt_number",
    })?;
    decode_receipt(row)
}

fn read_journal_row(
    transaction: &Transaction<'_>,
    observation_id: &str,
) -> Result<JournalFactsV1, ObservationJournalError> {
    transaction
        .query_row(SELECT_JOURNAL_ROW, params![observation_id], |row| {
            Ok(JournalFactsV1 {
                payload_sha256: row.get(0)?,
                extensions_digest: row.get(1)?,
                provider_id: row.get(2)?,
                registration_revision: row.get(3)?,
            })
        })
        .optional()?
        .ok_or_else(|| ObservationJournalError::UnknownObservation {
            observation_id: observation_id.to_owned(),
        })
}

#[allow(clippy::too_many_arguments)]
fn insert_receipt_row(
    transaction: &Transaction<'_>,
    observation_id: &str,
    attempt_number: u32,
    receipt_id: &str,
    idempotency_key: &str,
    payload_sha256: &str,
    extensions_digest: &str,
    provider_id: &str,
    provider_instance_id: Option<&str>,
    registration_revision: i64,
    state_generation_before: Option<i64>,
    state_generation_after: Option<i64>,
    outcome: ObservationOutcomeV1,
    committed_effect: ObservationCommittedEffectV1,
    provider_effect_summary_json: &str,
    provider_receipt_digest: Option<&str>,
    started_at_micros: i64,
    finished_at_micros: i64,
) -> Result<bool, ObservationJournalError> {
    let warnings_json = encode_json(&Vec::<String>::new(), "warnings_json")?;
    match transaction.execute(
        INSERT_RECEIPT,
        params![
            observation_id,
            i64::from(attempt_number),
            receipt_id,
            idempotency_key,
            payload_sha256,
            extensions_digest,
            provider_id,
            provider_instance_id,
            registration_revision,
            state_generation_before,
            state_generation_after,
            outcome.as_wire(),
            committed_effect.as_wire(),
            provider_effect_summary_json,
            provider_receipt_digest,
            started_at_micros,
            finished_at_micros,
            warnings_json,
        ],
    ) {
        Ok(_) => Ok(true),
        Err(rusqlite::Error::SqliteFailure(error, message))
            if error.code == ErrorCode::ConstraintViolation
                && matches!(error.extended_code, 1555 | 2067) =>
        {
            let _ = message;
            Ok(false)
        }
        Err(error) => Err(error.into()),
    }
}

impl ObservationJournalReaderV1 for SqliteObservationJournal {
    fn lease_pending(
        &self,
        request: &LeaseRequestV1,
    ) -> Result<Vec<LeasedObservationV1>, ObservationJournalError> {
        request.validate()?;
        let policy = *self.policy();
        let registration = sql_i64(request.registration_revision, "registration_revision")?;
        self.with_transaction(|transaction| {
            // (1) Rows whose observation deadline already passed are expired
            // with a terminal receipt instead of being quietly skipped forever.
            terminalize_deliveries(
                transaction,
                EXPIRE_PAST_DEADLINE,
                params![
                    &request.provider_id,
                    registration,
                    request.now_unix_micros,
                    i64::from(request.max_items),
                ],
                DeliveryStateV1::Expired,
                ObservationOutcomeV1::DeadlineExceeded,
                "observation_deadline_elapsed",
                request.now_unix_micros,
            )?;

            // (2) Rows that have consumed every allowed attempt are exhausted
            // here, not handed out again. Attempts are consumed by the claim,
            // so this is the bound that holds even when every lease lapsed
            // before its dispatcher could record anything.
            terminalize_deliveries(
                transaction,
                SELECT_ATTEMPTS_EXHAUSTED,
                params![
                    &request.provider_id,
                    registration,
                    i64::from(policy.max_attempts),
                    i64::from(request.max_items),
                ],
                DeliveryStateV1::Exhausted,
                ObservationOutcomeV1::ProviderUnavailable,
                "max_delivery_attempts_consumed",
                request.now_unix_micros,
            )?;

            let select = format!(
                "SELECT {LEASE_SELECT_COLUMNS}, d.state \
                 FROM tdmem_observation_delivery_v1 d \
                 JOIN tdmem_observation_journal_v1 j ON j.idempotency_key = d.idempotency_key \
                 WHERE d.provider_id = ?1 AND d.registration_revision = ?2 \
                   AND d.state IN ('pending', 'effect_unknown') \
                   AND d.next_attempt_at_micros <= ?3 \
                   AND j.deadline_micros > ?3 \
                   AND j.payload_bytes IS NOT NULL \
                   AND (?4 IS NULL OR d.exact_scope_sha256 = ?4) \
                 ORDER BY d.source_sequence, d.idempotency_key \
                 LIMIT ?5"
            );
            let mut candidates: Vec<(LeasedObservationV1, DeliveryStateV1, u32)> = Vec::new();
            {
                let mut statement = transaction.prepare(&select)?;
                let mut rows = statement.query(params![
                    &request.provider_id,
                    registration,
                    request.now_unix_micros,
                    request.exact_scope_sha256.as_deref(),
                    i64::from(request.max_items),
                ])?;
                let mut leased_bytes: u64 = 0;
                while let Some(row) = rows.next()? {
                    let state = DeliveryStateV1::from_wire(&row.get::<_, String>(29)?)?;
                    let lease_expires = request
                        .now_unix_micros
                        .saturating_add(request.lease_duration_micros);
                    let item = decode_leased(
                        row,
                        &request.provider_instance_id,
                        &request.lease_owner,
                        request.now_unix_micros,
                        lease_expires,
                    )?;
                    let weight = u64::try_from(item.payload.bytes.len()).unwrap_or(u64::MAX);
                    if !candidates.is_empty()
                        && leased_bytes.saturating_add(weight) > request.max_bytes
                    {
                        break;
                    }
                    leased_bytes = leased_bytes.saturating_add(weight);
                    let consumed = item.attempt_number.saturating_sub(1);
                    candidates.push((item, state, consumed));
                }
            }

            let mut leased = Vec::with_capacity(candidates.len());
            for (item, previous_state, consumed_attempts) in candidates {
                let changed = transaction.execute(
                    CLAIM_LEASE,
                    params![
                        item.lease_id.as_str(),
                        &request.lease_owner,
                        item.lease_expires_at_unix_micros,
                        &request.provider_instance_id,
                        request.now_unix_micros,
                        item.idempotency_key.as_str(),
                        previous_state.as_wire(),
                        i64::from(consumed_attempts),
                    ],
                )?;
                if changed == 1 {
                    leased.push(item);
                }
            }
            Ok(leased)
        })
    }

    fn record_attempt(
        &self,
        receipt: &ObservationDeliveryReceiptV1,
    ) -> Result<AttemptOutcomeV1, ObservationJournalError> {
        receipt.validate()?;
        let policy = *self.policy();
        self.with_transaction(|transaction| {
            let journal = read_journal_row(transaction, receipt.observation_id.as_str())?;
            // Delivered bytes are journal bytes, so a receipt describing other
            // content cannot be attributed to this observation.
            if journal.payload_sha256 != receipt.payload_sha256 {
                return Err(ObservationJournalError::ReceiptDigestMismatch {
                    field: "payload_sha256",
                });
            }
            if journal.extensions_digest != receipt.extensions_digest {
                return Err(ObservationJournalError::ReceiptDigestMismatch {
                    field: "extensions_digest",
                });
            }
            // The instance may legitimately differ from the admitted one — a
            // provider restarts — but the registration it was addressed to may
            // not: that pair is what the idempotency key is derived over.
            if journal.provider_id != receipt.provider_id.as_str() {
                return Err(ObservationJournalError::ReceiptDigestMismatch {
                    field: "provider_id",
                });
            }
            if journal.registration_revision
                != sql_i64(receipt.registration_revision, "registration_revision")?
            {
                return Err(ObservationJournalError::ReceiptDigestMismatch {
                    field: "registration_revision",
                });
            }

            let summary_json = encode_json(
                &receipt.provider_effect_summary,
                "provider_effect_summary_json",
            )?;
            let inserted = insert_receipt_row(
                transaction,
                receipt.observation_id.as_str(),
                receipt.attempt_number,
                receipt.receipt_id.as_str(),
                receipt.idempotency_key.as_str(),
                &receipt.payload_sha256,
                &receipt.extensions_digest,
                receipt.provider_id.as_str(),
                receipt.provider_instance_id.as_deref(),
                sql_i64(receipt.registration_revision, "registration_revision")?,
                receipt
                    .state_generation_before
                    .map(|value| sql_i64(value, "state_generation_before"))
                    .transpose()?,
                receipt
                    .state_generation_after
                    .map(|value| sql_i64(value, "state_generation_after"))
                    .transpose()?,
                receipt.outcome,
                receipt.committed_effect,
                &summary_json,
                receipt.provider_receipt_digest.as_deref(),
                receipt.started_at_unix_micros,
                receipt.finished_at_unix_micros,
            )?;
            if !inserted {
                // This attempt already has a receipt and receipts are never
                // rewritten. The row still has to be settled — from the
                // *standing* receipt, which is the authority for what that
                // attempt did. Returning here without settling would leave a
                // row leased against an attempt that is already finished, to be
                // reaped and re-leased forever.
                let standing = read_receipt(
                    transaction,
                    receipt.observation_id.as_str(),
                    receipt.attempt_number,
                )?;
                let settled = settle_delivery(transaction, &standing, &policy)?;
                let state = match settled.advanced {
                    true => settled.state,
                    false => read_delivery_state(transaction, receipt.observation_id.as_str())?,
                };
                return Ok(AttemptOutcomeV1::DuplicateReceipt { state });
            }

            let settled = settle_delivery(transaction, receipt, &policy)?;
            if !settled.advanced {
                // The lease lapsed and was reaped, or another attempt already
                // advanced the row. The receipt stands: nothing is lost, and
                // the attempt number this dispatcher held is never handed out
                // again.
                return Ok(AttemptOutcomeV1::LeaseLost {
                    receipt_id: receipt.receipt_id.clone(),
                });
            }
            Ok(AttemptOutcomeV1::Recorded {
                state: settled.state,
                next_attempt_at_unix_micros: (!settled.state.is_terminal())
                    .then_some(settled.next_attempt_at_unix_micros),
            })
        })
    }

    fn release_lease(
        &self,
        lease: &DispatchLeaseIdV1,
        retry_after_unix_micros: i64,
    ) -> Result<(), ObservationJournalError> {
        self.with_transaction(|transaction| {
            let changed = transaction.execute(
                RELEASE_LEASE,
                params![retry_after_unix_micros, lease.as_str()],
            )?;
            if changed == 0 {
                return Err(ObservationJournalError::UnknownLease {
                    lease_id: lease.as_str().to_owned(),
                });
            }
            Ok(())
        })
    }

    fn reap_expired_leases(
        &self,
        now_unix_micros: i64,
        budget: u32,
    ) -> Result<u32, ObservationJournalError> {
        self.with_transaction(|transaction| {
            let changed =
                transaction.execute(REAP_LEASES, params![now_unix_micros, i64::from(budget)])?;
            read_u32(i64::try_from(changed).unwrap_or(i64::MAX), "reaped_leases")
        })
    }

    fn inspect(
        &self,
        filter: &JournalInspectionFilterV1,
    ) -> Result<JournalInspectionPageV1, ObservationJournalError> {
        let limit = if filter.limit == 0 { 100 } else { filter.limit };
        let state_filter = if filter.states.is_empty() {
            String::new()
        } else {
            let joined = filter
                .states
                .iter()
                .map(|state| format!("'{}'", state.as_wire()))
                .collect::<Vec<_>>()
                .join(", ");
            format!(" AND d.state IN ({joined})")
        };
        let predicate = format!(
            "FROM tdmem_observation_delivery_v1 d \
             JOIN tdmem_observation_journal_v1 j ON j.idempotency_key = d.idempotency_key \
             WHERE (?1 IS NULL OR d.provider_id = ?1) \
               AND (?2 IS NULL OR j.provider_instance_id = ?2) \
               AND (?3 IS NULL OR d.exact_scope_sha256 = ?3) \
               AND (?4 IS NULL OR j.source_authority = ?4) \
               AND (?5 IS NULL OR j.forget_source_key = ?5) \
               AND (?6 IS NULL OR j.admitted_at_micros > ?6) \
               AND (?7 IS NULL OR j.admitted_at_micros < ?7) \
               AND (?8 IS NULL OR d.idempotency_key > ?8){state_filter}"
        );
        self.with_connection(|connection| {
            let authority = filter
                .source_authority
                .map(|authority| authority.as_wire().to_owned());
            let forget = filter
                .forget_source_key
                .as_ref()
                .map(|key| key.as_str().to_owned());
            let provider = filter.provider_id.as_deref();
            let instance = filter.provider_instance_id.as_deref();
            let scope = filter.exact_scope_sha256.as_deref();
            let authority = authority.as_deref();
            let forget = forget.as_deref();
            let after = filter.admitted_after_unix_micros;
            let before = filter.admitted_before_unix_micros;
            let cursor = filter.after_cursor.as_deref();
            let total: i64 = connection.query_row(
                &format!("SELECT COUNT(*) {predicate}"),
                params![provider, instance, scope, authority, forget, after, before, cursor],
                |row| row.get(0),
            )?;

            let select = format!(
                "SELECT j.observation_id, d.idempotency_key, d.provider_id, j.provider_instance_id, \
                        d.exact_scope_sha256, j.source_authority, j.source_stream, j.source_sequence, \
                        j.observation_kind, j.payload_contract, j.payload_sha256, j.extensions_digest, \
                        j.privacy_classification, j.retention_class, j.forget_source_key, d.state, \
                        d.attempt_number, d.next_attempt_at_micros, j.admitted_at_micros, \
                        j.deadline_micros, j.payload_bytes IS NOT NULL, j.content_forgotten_at_micros, \
                        d.registration_revision, d.last_provider_instance_id \
                 {predicate} ORDER BY d.idempotency_key LIMIT {limit}"
            );
            let mut statement = connection.prepare(&select)?;
            let mut rows = statement.query(params![
                provider, instance, scope, authority, forget, after, before, cursor
            ])?;
            let mut decoded = Vec::new();
            while let Some(row) = rows.next()? {
                decoded.push(JournalInspectionRowV1 {
                    observation_id: ObservationIdV1::parse(&row.get::<_, String>(0)?)?,
                    idempotency_key: ObservationIdempotencyKeyV1::parse(&row.get::<_, String>(1)?)?,
                    provider_id: row.get(2)?,
                    provider_instance_id: row.get(3)?,
                    exact_scope_sha256: row.get(4)?,
                    source_authority: SourceAuthorityV1::from_wire(&row.get::<_, String>(5)?)?,
                    source_stream: row.get(6)?,
                    source_sequence: SourceSequenceV1(read_u64(
                        row.get::<_, i64>(7)?,
                        "source_sequence",
                    )?),
                    observation_kind: row.get(8)?,
                    payload_contract: row.get(9)?,
                    payload_sha256: row.get(10)?,
                    extensions_digest: row.get(11)?,
                    privacy_classification: crate::envelope::PrivacyClassificationV1::from_wire(
                        &row.get::<_, String>(12)?,
                    )?,
                    retention_class: crate::envelope::RetentionClassV1::from_wire(
                        &row.get::<_, String>(13)?,
                    )?,
                    forget_source_key: crate::identity::ForgetSourceKeyV1::new(
                        row.get::<_, String>(14)?,
                    )?,
                    state: DeliveryStateV1::from_wire(&row.get::<_, String>(15)?)?,
                    attempt_number: read_u32(row.get::<_, i64>(16)?, "attempt_number")?,
                    next_attempt_at_unix_micros: row.get(17)?,
                    admitted_at_unix_micros: row.get(18)?,
                    deadline_unix_micros: row.get(19)?,
                    content_present: row.get::<_, i64>(20)? != 0,
                    content_forgotten_at_unix_micros: row.get(21)?,
                    registration_revision: read_u64(
                        row.get::<_, i64>(22)?,
                        "registration_revision",
                    )?,
                    last_provider_instance_id: row.get(23)?,
                });
            }
            let next_cursor = (decoded.len() == limit as usize)
                .then(|| {
                    decoded
                        .last()
                        .map(|row| row.idempotency_key.as_str().to_owned())
                })
                .flatten();
            Ok(JournalInspectionPageV1 {
                rows: decoded,
                total_rows: read_u64(total, "total_rows")?,
                next_cursor,
            })
        })
    }

    fn receipts_for(
        &self,
        observation_id: &ObservationIdV1,
    ) -> Result<Vec<ObservationDeliveryReceiptV1>, ObservationJournalError> {
        self.with_connection(|connection| {
            let select = format!(
                "SELECT {RECEIPT_SELECT_COLUMNS} FROM tdmem_observation_receipt_v1 \
                 WHERE observation_id = ?1 ORDER BY attempt_number"
            );
            let mut statement = connection.prepare(&select)?;
            let mut rows = statement.query(params![observation_id.as_str()])?;
            let mut receipts = Vec::new();
            while let Some(row) = rows.next()? {
                receipts.push(decode_receipt(row)?);
            }
            Ok(receipts)
        })
    }
}
