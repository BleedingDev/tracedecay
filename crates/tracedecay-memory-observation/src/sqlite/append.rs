//! Admission: the durable boundary between a committed TraceDecay action and
//! provider delivery.
//!
//! # Why "causally bound" rather than one transaction
//!
//! The canonical settlement authorities live behind
//! `crates/tracedecay-store/**`, `crates/tracedecay-global-db/**`, and
//! `crates/tracedecay-rusqlite-runtime/**`, which
//! `product/upstream/patch-footprint-policy.json` marks as forbidden exception
//! zones, and ADR-0005 explicitly rejects "one distributed transaction across
//! TraceDecay and provider stores". So admission delivers the other half of the
//! acceptance criterion: the append is one atomic unit keyed by exact settled
//! source identity, and a crash between the canonical commit and the append is
//! *recoverable* rather than lost, because the authority's own cursor is ahead
//! of [`ObservationDispatchPortV1::replay_cursor`] and re-emission collides on
//! the content-derived idempotency key instead of duplicating an effect.
//!
//! [`SqliteObservationJournal::append_admitted_in_transaction`] additionally
//! lets a co-located caller run the append inside its own transaction, so true
//! atomicity is available the moment a caller can offer it.
//!
//! [`ObservationDispatchPortV1::replay_cursor`]: crate::ObservationDispatchPortV1::replay_cursor

use rusqlite::{OptionalExtension, Transaction, params};

use crate::envelope::{AdmittedObservationV1, WithheldAdmissionV1};
use crate::error::ObservationJournalError;
use crate::identity::{ObservationIdV1, ObservationIdempotencyKeyV1, SourceSequenceV1};
use crate::inspection::{
    ObservationLaneKeyV1, QueuePressureV1, ReplayCursorV1, ReplayDispositionV1,
};
use crate::port::{AppendOutcomeV1, ObservationDispatchPortV1};
use crate::settlement::SourceStreamKeyV1;
use crate::state::DeliveryStateV1;

use super::SqliteObservationJournal;
use super::row::{StoredExactScopeV1, encode_extensions, encode_json, read_u64, sql_i64};

const INSERT_JOURNAL: &str = r#"
INSERT OR IGNORE INTO tdmem_observation_journal_v1 (
    idempotency_key, observation_id, exact_scope_sha256, provider_id, provider_instance_id,
    registration_revision, ready_receipt_digest, source_authority, source_stream,
    source_event_id, source_event_revision, source_event_sha256, source_sequence,
    settlement_receipt_json, exact_scope_json, observation_kind, payload_contract,
    payload_sha256, payload_bytes, payload_byte_len, extensions_digest, extensions_json,
    provenance_origin, provenance_sha256, privacy_classification, retention_class,
    redaction_revision, content_policy_revision, forget_source_key, expires_at_micros,
    occurred_at_micros, admitted_at_micros, deadline_micros, request_id, envelope_sha256,
    sanitization_receipt_id, sanitizer_revision, source_payload_sha256,
    sanitization_receipt_json
) VALUES (
    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19,
    ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33, ?34, ?35, ?36,
    ?37, ?38, ?39
)
"#;

const INSERT_DELIVERY: &str = r#"
INSERT OR IGNORE INTO tdmem_observation_delivery_v1 (
    idempotency_key, observation_id, provider_id, registration_revision, state,
    attempt_number, next_attempt_at_micros, source_sequence, exact_scope_sha256,
    queue_bytes, updated_at_micros
) VALUES (?1, ?2, ?3, ?4, 'pending', 0, ?5, ?6, ?7, ?8, ?9)
"#;

const SELECT_BY_KEY: &str = r#"
SELECT j.observation_id, j.payload_sha256, d.state
FROM tdmem_observation_journal_v1 j
JOIN tdmem_observation_delivery_v1 d ON d.idempotency_key = j.idempotency_key
WHERE j.idempotency_key = ?1
"#;

const SELECT_BY_SOURCE: &str = r#"
SELECT observation_id, idempotency_key, source_event_id, source_event_revision
FROM tdmem_observation_journal_v1
WHERE provider_id = ?1 AND registration_revision = ?2 AND source_authority = ?3
  AND exact_scope_sha256 = ?4 AND source_stream = ?5 AND source_sequence = ?6
"#;

const SELECT_WITHHELD_AT_SEQUENCE: &str = r#"
SELECT 1 FROM tdmem_observation_withheld_v2
WHERE source_authority = ?1 AND exact_scope_sha256 = ?2 AND source_stream = ?3
  AND source_sequence = ?4
LIMIT 1
"#;

const SELECT_TARGET_CURSOR: &str = r#"
SELECT last_admitted_sequence, last_source_event_id, last_source_event_revision
FROM tdmem_observation_target_cursor_v1
WHERE provider_id = ?1 AND registration_revision = ?2 AND source_authority = ?3
  AND exact_scope_sha256 = ?4 AND source_stream = ?5
"#;

const UPSERT_TARGET_CURSOR: &str = r#"
INSERT INTO tdmem_observation_target_cursor_v1 (
    provider_id, registration_revision, source_authority, exact_scope_sha256, source_stream,
    last_admitted_sequence, last_source_event_id, last_source_event_revision, updated_at_micros
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
ON CONFLICT (provider_id, registration_revision, source_authority, exact_scope_sha256,
             source_stream) DO UPDATE SET
    last_admitted_sequence = excluded.last_admitted_sequence,
    last_source_event_id = excluded.last_source_event_id,
    last_source_event_revision = excluded.last_source_event_revision,
    updated_at_micros = excluded.updated_at_micros
WHERE excluded.last_admitted_sequence
      >= tdmem_observation_target_cursor_v1.last_admitted_sequence
"#;

const SELECT_BY_OBSERVATION_ID: &str = r#"
SELECT idempotency_key FROM tdmem_observation_journal_v1 WHERE observation_id = ?1
"#;

const SELECT_CURSOR: &str = r#"
SELECT last_admitted_sequence, last_source_event_id, last_source_event_revision,
       last_settlement_proof_sha256, last_disposition, updated_at_micros
FROM tdmem_observation_replay_cursor_v1
WHERE source_authority = ?1 AND exact_scope_sha256 = ?2 AND source_stream = ?3
"#;

/// Advances the ingress replay position for an *admitted* event.
///
/// The cursor is a replay position for the stream as a whole, so it takes the
/// maximum: a legitimate fan-out to a lagging registration re-uses an earlier
/// sequence and must not drag the stream's replay position backwards. Per-target
/// regression is `tdmem_observation_target_cursor_v1`'s job, not this one's.
const UPSERT_CURSOR: &str = r#"
INSERT INTO tdmem_observation_replay_cursor_v1 (
    source_authority, exact_scope_sha256, source_stream, last_admitted_sequence,
    last_source_event_id, last_source_event_revision, last_settlement_proof_sha256,
    last_disposition, updated_at_micros
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
ON CONFLICT (source_authority, exact_scope_sha256, source_stream) DO UPDATE SET
    last_admitted_sequence = MAX(
        excluded.last_admitted_sequence,
        tdmem_observation_replay_cursor_v1.last_admitted_sequence),
    last_source_event_id = CASE
        WHEN excluded.last_admitted_sequence
             >= tdmem_observation_replay_cursor_v1.last_admitted_sequence
        THEN excluded.last_source_event_id
        ELSE tdmem_observation_replay_cursor_v1.last_source_event_id END,
    last_source_event_revision = CASE
        WHEN excluded.last_admitted_sequence
             >= tdmem_observation_replay_cursor_v1.last_admitted_sequence
        THEN excluded.last_source_event_revision
        ELSE tdmem_observation_replay_cursor_v1.last_source_event_revision END,
    last_settlement_proof_sha256 = CASE
        WHEN excluded.last_admitted_sequence
             >= tdmem_observation_replay_cursor_v1.last_admitted_sequence
        THEN excluded.last_settlement_proof_sha256
        ELSE tdmem_observation_replay_cursor_v1.last_settlement_proof_sha256 END,
    last_disposition = CASE
        WHEN excluded.last_admitted_sequence
             >= tdmem_observation_replay_cursor_v1.last_admitted_sequence
        THEN excluded.last_disposition
        ELSE tdmem_observation_replay_cursor_v1.last_disposition END,
    updated_at_micros = excluded.updated_at_micros
"#;

/// Advances the ingress replay position for a *withheld* event.
///
/// Strictly greater, never equal. A withheld record must never overwrite an
/// `admitted` cursor standing at the same sequence: doing so would erase that
/// position's settlement proof and re-label a delivered event as refused. A
/// repeat of the same withheld position is therefore a no-op, which is exactly
/// what idempotent re-emission needs.
const UPSERT_WITHHELD_CURSOR: &str = r#"
INSERT INTO tdmem_observation_replay_cursor_v1 (
    source_authority, exact_scope_sha256, source_stream, last_admitted_sequence,
    last_source_event_id, last_source_event_revision, last_settlement_proof_sha256,
    last_disposition, updated_at_micros
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, 'withheld', ?7)
ON CONFLICT (source_authority, exact_scope_sha256, source_stream) DO UPDATE SET
    last_admitted_sequence = excluded.last_admitted_sequence,
    last_source_event_id = excluded.last_source_event_id,
    last_source_event_revision = excluded.last_source_event_revision,
    last_settlement_proof_sha256 = NULL,
    last_disposition = 'withheld',
    updated_at_micros = excluded.updated_at_micros
WHERE excluded.last_admitted_sequence
      > tdmem_observation_replay_cursor_v1.last_admitted_sequence
"#;

/// Queue pressure is per provider *registration*: capacity has to be the same
/// bound the lease and the idempotency key see, or a restarted instance would
/// admit a second queue's worth of work against a queue it cannot drain.
const SELECT_PRESSURE: &str = r#"
SELECT COUNT(*), COALESCE(SUM(d.queue_bytes), 0), MIN(j.admitted_at_micros)
FROM tdmem_observation_delivery_v1 d
JOIN tdmem_observation_journal_v1 j ON j.idempotency_key = d.idempotency_key
WHERE d.provider_id = ?1 AND d.registration_revision = ?2
  AND d.state IN ('pending', 'leased', 'effect_unknown')
"#;

const INSERT_WITHHELD: &str = r#"
INSERT OR IGNORE INTO tdmem_observation_withheld_v2 (
    source_authority, exact_scope_sha256, source_stream, source_sequence, receipt_id,
    source_event_id, source_event_revision, reason, source_payload_sha256,
    extensions_digest, sanitizer_revision, finding_count, findings_digest, forget_source_key,
    withheld_at_micros
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
"#;

impl SqliteObservationJournal {
    /// Appends one admitted observation using an explicit admission clock.
    pub fn append_admitted_at(
        &self,
        admitted: &AdmittedObservationV1,
        now_unix_micros: i64,
    ) -> Result<AppendOutcomeV1, ObservationJournalError> {
        self.with_transaction(|transaction| {
            self.append_admitted_in_transaction(transaction, admitted, now_unix_micros)
        })
    }

    /// Appends one admitted observation inside a caller-owned transaction.
    ///
    /// A caller that can `ATTACH` this journal to its own connection gets a
    /// genuinely atomic append. Keeping this off the port keeps the port
    /// storage-neutral.
    pub fn append_admitted_in_transaction(
        &self,
        transaction: &Transaction<'_>,
        admitted: &AdmittedObservationV1,
        now_unix_micros: i64,
    ) -> Result<AppendOutcomeV1, ObservationJournalError> {
        admitted.validate()?;

        if now_unix_micros >= admitted.deadline_unix_micros {
            return Ok(AppendOutcomeV1::RejectedDeadlineExpired {
                deadline_unix_micros: admitted.deadline_unix_micros,
            });
        }

        let exact_scope_sha256 = admitted.exact_scope_sha256();
        let authority = admitted.source.source_authority.as_wire();
        let stream = admitted.source.source_stream.as_str();
        let sequence = sql_i64(admitted.source.source_sequence.0, "source_sequence")?;
        let registration = sql_i64(
            admitted.target.registration_revision,
            "registration_revision",
        )?;

        // (1) Same key already journalled? Then it is the same content: the key
        // is derived over `payload_sha256` and `validate()` re-derived it, so a
        // stored digest that disagrees is store corruption rather than a caller
        // outcome, and admission fails closed instead of reporting a duplicate.
        let existing: Option<(String, String, String)> = transaction
            .query_row(
                SELECT_BY_KEY,
                params![admitted.idempotency_key.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        if let Some((observation_id, payload_sha256, state)) = existing {
            if payload_sha256 != admitted.payload.sha256 {
                return Err(ObservationJournalError::Corrupt {
                    table: "tdmem_observation_journal_v1",
                    field: "payload_sha256",
                });
            }
            return Ok(AppendOutcomeV1::DuplicateIdempotencyKey {
                observation_id: ObservationIdV1::parse(&observation_id)?,
                state: DeliveryStateV1::from_wire(&state)?,
            });
        }

        // (2) Same settled event already journalled for this registration at
        // this sequence under a different key? The sanitizer corpus moved; this
        // is a duplicate, not a new observation.
        let by_source: Option<(String, String, String, i64)> = transaction
            .query_row(
                SELECT_BY_SOURCE,
                params![
                    admitted.target.provider_id.as_str(),
                    registration,
                    authority,
                    &exact_scope_sha256,
                    stream,
                    sequence,
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        if let Some((observation_id, key, source_event_id, source_event_revision)) = by_source {
            let stored_revision = read_u64(source_event_revision, "source_event_revision")?;
            return if source_event_id == admitted.source.source_event_id
                && stored_revision == admitted.source.source_event_revision
            {
                Ok(AppendOutcomeV1::DuplicateSourceEvent {
                    observation_id: ObservationIdV1::parse(&observation_id)?,
                    stored_idempotency_key: ObservationIdempotencyKeyV1::parse(&key)?,
                })
            } else {
                Ok(AppendOutcomeV1::SourceSequenceConflict {
                    stored_source_event_id: source_event_id,
                    stored_source_event_revision: stored_revision,
                })
            };
        }

        // (3) A sequence the sanitizer withheld is closed outright, to every
        // registration: re-admitting it would smuggle refused content past the
        // decision that refused it.
        let withheld: Option<i64> = transaction
            .query_row(
                SELECT_WITHHELD_AT_SEQUENCE,
                params![authority, &exact_scope_sha256, stream, sequence],
                |row| row.get(0),
            )
            .optional()?;
        if withheld.is_some() {
            return Ok(AppendOutcomeV1::RejectedWithheldSource {
                source_sequence: admitted.source.source_sequence,
            });
        }

        // (4) Monotonicity, scoped to this provider registration. A sequence at
        // or below *this target's* position is a regression unless it is the
        // same settled event being re-admitted at the position it already
        // holds. Scoping this per target is what makes late fan-out work: the
        // stream's ingress cursor may already be far ahead because another
        // registration consumed later events, and that says nothing about
        // whether this registration has seen sequence n.
        let target_cursor: Option<(i64, String, i64)> = transaction
            .query_row(
                SELECT_TARGET_CURSOR,
                params![
                    admitted.target.provider_id.as_str(),
                    registration,
                    authority,
                    &exact_scope_sha256,
                    stream,
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        if let Some((last_sequence, last_event_id, last_revision)) = target_cursor {
            let same_event = last_event_id == admitted.source.source_event_id
                && read_u64(last_revision, "last_source_event_revision")?
                    == admitted.source.source_event_revision;
            let regression = sequence < last_sequence || (sequence == last_sequence && !same_event);
            if regression {
                return Ok(AppendOutcomeV1::RejectedSourceSequenceRegression {
                    last_admitted: SourceSequenceV1(read_u64(
                        last_sequence,
                        "last_admitted_sequence",
                    )?),
                });
            }
        }

        // (5) Bounded queue. Exhaustion is a typed outcome, never a silent drop.
        let queue_bytes = admitted.queue_bytes();
        let pressure = read_pressure(
            transaction,
            &ObservationLaneKeyV1::of(&admitted.target),
            self.policy(),
        )?;
        if pressure.would_exceed(queue_bytes) {
            return Ok(AppendOutcomeV1::RejectedCapacity {
                queue_items: pressure.queue_items,
                queue_bytes: pressure.queue_bytes,
            });
        }

        // (6) Journal, delivery, and both cursors advance as one unit.
        let settlement_json = encode_json(&admitted.source, "settlement_receipt_json")?;
        let scope_json = encode_json(
            &StoredExactScopeV1::from_scope(&admitted.exact_scope),
            "exact_scope_json",
        )?;
        let extensions_json = encode_extensions(&admitted.extensions)?;
        let sanitization = &admitted.sanitization;
        let inserted = transaction.execute(
            INSERT_JOURNAL,
            params![
                admitted.idempotency_key.as_str(),
                admitted.observation_id.as_str(),
                &exact_scope_sha256,
                admitted.target.provider_id.as_str(),
                &admitted.target.provider_instance_id,
                registration,
                &admitted.target.ready_receipt_digest,
                authority,
                stream,
                &admitted.source.source_event_id,
                sql_i64(
                    admitted.source.source_event_revision,
                    "source_event_revision"
                )?,
                &admitted.source.source_event_sha256,
                sequence,
                settlement_json,
                scope_json,
                admitted.observation_kind.as_str(),
                admitted.payload.contract_id.as_str(),
                &admitted.payload.sha256,
                &admitted.payload.bytes,
                sql_i64(
                    u64::try_from(admitted.payload.bytes.len()).unwrap_or(u64::MAX),
                    "payload_byte_len",
                )?,
                &admitted.extensions_digest,
                extensions_json,
                admitted.provenance_origin.as_wire(),
                &admitted.provenance_sha256,
                admitted.privacy.classification.as_wire(),
                admitted.privacy.retention_class.as_wire(),
                i64::from(admitted.privacy.redaction_revision),
                i64::from(admitted.privacy.content_policy_revision),
                admitted.privacy.forget_source_key.as_str(),
                admitted.privacy.expires_at_unix_micros,
                admitted.occurred_at_unix_micros,
                admitted.admitted_at_unix_micros,
                admitted.deadline_unix_micros,
                &admitted.request_id,
                &admitted.envelope_sha256,
                sanitization.receipt_id.as_str(),
                sanitization.sanitizer_revision.as_str(),
                sanitization.source_payload_sha256.as_str(),
                sanitization.receipt_json.as_str(),
            ],
        )?;
        if inserted != 1 {
            // Every unique constraint reachable here was already checked
            // above, except observation id reuse: two distinct observations
            // must never claim one row identity, and a caller that mints ids
            // badly has to hear about it rather than lose the append.
            let reused: Option<String> = transaction
                .query_row(
                    SELECT_BY_OBSERVATION_ID,
                    params![admitted.observation_id.as_str()],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(holder) = reused {
                return Err(ObservationJournalError::InvalidObservationId {
                    detail: format!(
                        "observation id {} is already held by idempotency key {holder}",
                        admitted.observation_id.as_str()
                    ),
                });
            }
            // Otherwise another writer won a unique index between our reads
            // and this insert. Refuse rather than guess.
            return Err(ObservationJournalError::Corrupt {
                table: "tdmem_observation_journal_v1",
                field: "idempotency_key",
            });
        }

        transaction.execute(
            INSERT_DELIVERY,
            params![
                admitted.idempotency_key.as_str(),
                admitted.observation_id.as_str(),
                admitted.target.provider_id.as_str(),
                registration,
                admitted.admitted_at_unix_micros,
                sequence,
                &exact_scope_sha256,
                sql_i64(queue_bytes.max(1), "queue_bytes")?,
                now_unix_micros,
            ],
        )?;

        transaction.execute(
            UPSERT_CURSOR,
            params![
                authority,
                &exact_scope_sha256,
                stream,
                sequence,
                &admitted.source.source_event_id,
                admitted.source.source_event_revision.to_string(),
                Some(admitted.source.settlement_proof_sha256.as_str()),
                ReplayDispositionV1::Admitted.as_wire(),
                now_unix_micros,
            ],
        )?;

        transaction.execute(
            UPSERT_TARGET_CURSOR,
            params![
                admitted.target.provider_id.as_str(),
                registration,
                authority,
                &exact_scope_sha256,
                stream,
                sequence,
                &admitted.source.source_event_id,
                sql_i64(
                    admitted.source.source_event_revision,
                    "source_event_revision"
                )?,
                now_unix_micros,
            ],
        )?;

        Ok(AppendOutcomeV1::Appended {
            observation_id: admitted.observation_id.clone(),
            source_sequence: admitted.source.source_sequence,
        })
    }

    /// Records one withheld source event at an explicit clock.
    ///
    /// The withheld audit row is written unconditionally — that record is what
    /// closes the position to every registration — but the ingress cursor only
    /// moves for a *strictly newer* sequence. A withheld record arriving at a
    /// position already marked admitted therefore leaves that position's
    /// disposition and settlement proof intact instead of overwriting proof of
    /// a delivery that really happened, and a repeated withheld record at the
    /// same position is a no-op.
    pub fn record_withheld_at(
        &self,
        withheld: &WithheldAdmissionV1,
        now_unix_micros: i64,
    ) -> Result<(), ObservationJournalError> {
        withheld.validate()?;
        let sequence = sql_i64(withheld.source_sequence, "source_sequence")?;
        self.with_transaction(|transaction| {
            transaction.execute(
                INSERT_WITHHELD,
                params![
                    &withheld.source_authority,
                    &withheld.exact_scope_sha256,
                    &withheld.source_stream,
                    sequence,
                    &withheld.receipt_id,
                    &withheld.source_event_id,
                    &withheld.source_event_revision,
                    &withheld.reason,
                    &withheld.source_payload_sha256,
                    &withheld.extensions_digest,
                    &withheld.sanitizer_revision,
                    i64::from(withheld.finding_count),
                    &withheld.findings_digest,
                    withheld.forget_source_key.as_str(),
                    now_unix_micros,
                ],
            )?;
            transaction.execute(
                UPSERT_WITHHELD_CURSOR,
                params![
                    &withheld.source_authority,
                    &withheld.exact_scope_sha256,
                    &withheld.source_stream,
                    sequence,
                    &withheld.source_event_id,
                    &withheld.source_event_revision,
                    now_unix_micros,
                ],
            )?;
            Ok(())
        })
    }
}

fn read_pressure(
    transaction: &Transaction<'_>,
    lane: &ObservationLaneKeyV1,
    policy: &crate::retention::RetentionPolicyV1,
) -> Result<QueuePressureV1, ObservationJournalError> {
    let (items, bytes, oldest): (i64, i64, Option<i64>) = transaction.query_row(
        SELECT_PRESSURE,
        params![
            lane.provider_id.as_str(),
            sql_i64(lane.registration_revision, "registration_revision")?,
        ],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    Ok(QueuePressureV1 {
        queue_items: read_u64(items, "queue_items")?,
        queue_bytes: read_u64(bytes, "queue_bytes")?,
        oldest_admitted_at_unix_micros: oldest,
        max_queue_items: policy.max_queue_items,
        max_queue_bytes: policy.max_queue_bytes,
    })
}

impl ObservationDispatchPortV1 for SqliteObservationJournal {
    fn append_admitted(
        &self,
        admitted: &AdmittedObservationV1,
    ) -> Result<AppendOutcomeV1, ObservationJournalError> {
        self.append_admitted_at(admitted, admitted.admitted_at_unix_micros)
    }

    fn record_withheld(
        &self,
        withheld: &WithheldAdmissionV1,
    ) -> Result<(), ObservationJournalError> {
        self.record_withheld_at(withheld, unix_now_micros())
    }

    fn replay_cursor(
        &self,
        stream: &SourceStreamKeyV1,
    ) -> Result<Option<ReplayCursorV1>, ObservationJournalError> {
        stream.validate()?;
        self.with_connection(|connection| {
            let row: Option<(i64, String, String, Option<String>, String, i64)> = connection
                .query_row(
                    SELECT_CURSOR,
                    params![
                        stream.source_authority.as_wire(),
                        &stream.exact_scope_sha256,
                        stream.source_stream.as_str(),
                    ],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                        ))
                    },
                )
                .optional()?;
            row.map(
                |(sequence, event_id, revision, proof, disposition, updated)| {
                    Ok(ReplayCursorV1 {
                        last_admitted_sequence: SourceSequenceV1(read_u64(
                            sequence,
                            "last_admitted_sequence",
                        )?),
                        last_source_event_id: event_id,
                        last_source_event_revision: revision,
                        last_settlement_proof_sha256: proof,
                        last_disposition: ReplayDispositionV1::from_wire(&disposition)?,
                        updated_at_unix_micros: updated,
                    })
                },
            )
            .transpose()
        })
    }

    fn lane_pressure(
        &self,
        lane: &ObservationLaneKeyV1,
    ) -> Result<QueuePressureV1, ObservationJournalError> {
        lane.validate()?;
        let policy = *self.policy();
        self.with_transaction(|transaction| read_pressure(transaction, lane, &policy))
    }
}

/// Wall-clock micros, used only where the caller supplied no clock.
fn unix_now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_micros()).ok())
        .unwrap_or(i64::MAX)
}
