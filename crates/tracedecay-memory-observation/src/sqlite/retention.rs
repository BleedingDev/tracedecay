//! Bounded retention sweeps and verifiable privacy deletion.

use rusqlite::{Transaction, params};

use crate::error::ObservationJournalError;
use crate::identity::ForgetSourceKeyV1;
use crate::port::ObservationRetentionPortV1;
use crate::receipt::ObservationOutcomeV1;
use crate::retention::{
    ForgetReceiptV1, ForgetSourceRequestV1, ForgetVerificationV1, RetentionPolicyV1,
    RetentionSweepReceiptV1,
};
use crate::state::DeliveryStateV1;

use super::SqliteObservationJournal;
use super::dispatch::{ExpireDeliveryRequest, terminalize_deliveries};
use super::row::{read_u32, read_u64};

/// Effective expiry: `min(admitted privacy expiry, admitted_at + class age)`.
/// A provider can never widen it — no receipt column feeds this expression.
const EFFECTIVE_EXPIRY: &str = "MIN(j.expires_at_micros, j.admitted_at_micros + CASE j.retention_class \
    WHEN 'ephemeral' THEN ?1 WHEN 'session' THEN ?2 WHEN 'project' THEN ?3 ELSE ?4 END)";

/// The delivery states that still expect a provider to answer.
const UNSETTLED_STATES: &str = "('pending', 'leased', 'effect_unknown')";

/// The delivery states that will never be leased again.
const SETTLED_STATES: &str = "('acknowledged', 'duplicate_acknowledged', 'rejected', \
    'cancelled', 'expired', 'exhausted', 'forgotten')";

/// Content and the hygiene evidence that describes it are purged together.
///
/// The binding is not audit: its receipt JSON restates the pre-sanitization
/// digest of the very bytes being deleted, so leaving it behind would leave a
/// digest of forgotten content at rest — and clearing only part of it would
/// leave a column combination the schema forbids and no reader can decode.
const PURGE_COLUMNS: &str = "payload_bytes = NULL, extensions_json = NULL, \
    sanitization_receipt_id = NULL, sanitizer_revision = NULL, \
    source_payload_sha256 = NULL, sanitization_receipt_json = NULL";

impl SqliteObservationJournal {
    fn purge_content(
        transaction: &Transaction<'_>,
        idempotency_key: &str,
        now_unix_micros: i64,
    ) -> Result<(), ObservationJournalError> {
        transaction.execute(
            &format!(
                "UPDATE tdmem_observation_journal_v1 SET {PURGE_COLUMNS}, \
                 content_forgotten_at_micros = COALESCE(content_forgotten_at_micros, ?1) \
                 WHERE idempotency_key = ?2"
            ),
            params![now_unix_micros, idempotency_key],
        )?;
        Ok(())
    }
}

impl ObservationRetentionPortV1 for SqliteObservationJournal {
    fn retention_policy(&self) -> &RetentionPolicyV1 {
        self.policy()
    }

    fn sweep_expired(
        &self,
        now_unix_micros: i64,
        budget: u32,
    ) -> Result<RetentionSweepReceiptV1, ObservationJournalError> {
        let policy = *self.policy();
        let batch = i64::from(budget.min(policy.sweep_batch_rows).max(1));
        let deletable_cutoff = now_unix_micros.saturating_sub(policy.receipt_retention_micros);
        let receipt = self.with_transaction(|transaction| {
            let mut receipt = RetentionSweepReceiptV1::default();

            // (1) Content whose effective expiry has passed and that still
            // holds bytes. Non-terminal deliveries are expired with a terminal
            // receipt first; only then is content purged.
            let select_candidates = format!(
                "SELECT d.idempotency_key, d.observation_id, d.attempt_number, d.state \
                 FROM tdmem_observation_journal_v1 j \
                 JOIN tdmem_observation_delivery_v1 d ON d.idempotency_key = j.idempotency_key \
                 WHERE j.payload_bytes IS NOT NULL AND {EFFECTIVE_EXPIRY} <= ?5 \
                 ORDER BY j.admitted_at_micros, j.idempotency_key LIMIT ?6"
            );
            let mut candidates: Vec<(String, String, u32, DeliveryStateV1)> = Vec::new();
            {
                let mut statement = transaction.prepare(&select_candidates)?;
                let mut rows = statement.query(params![
                    policy.ephemeral_max_age_micros,
                    policy.session_max_age_micros,
                    policy.project_max_age_micros,
                    policy.profile_max_age_micros,
                    now_unix_micros,
                    batch,
                ])?;
                while let Some(row) = rows.next()? {
                    candidates.push((
                        row.get(0)?,
                        row.get(1)?,
                        read_u32(row.get::<_, i64>(2)?, "attempt_number")?,
                        DeliveryStateV1::from_wire(&row.get::<_, String>(3)?)?,
                    ));
                }
            }
            for (key, observation_id, attempts, state) in candidates {
                if !state.is_terminal() {
                    Self::expire_delivery(
                        transaction,
                        ExpireDeliveryRequest {
                            observation_id: &observation_id,
                            idempotency_key: &key,
                            attempt_number: attempts,
                            current_state: state,
                            next_state: DeliveryStateV1::Expired,
                            outcome: ObservationOutcomeV1::DeadlineExceeded,
                            reason: "retention_expiry",
                            now_unix_micros,
                        },
                    )?;
                    receipt.deliveries_expired = receipt.deliveries_expired.saturating_add(1);
                }
                Self::purge_content(transaction, &key, now_unix_micros)?;
                receipt.payloads_purged = receipt.payloads_purged.saturating_add(1);
            }

            // (2) Rows whose content is already gone but whose delivery never
            // settled. They can never be delivered — there is nothing left to
            // deliver — and they are not deletable while non-terminal, so
            // without this they would sit in the queue forever and the sweep
            // would keep reporting nothing left to do. Terminalizing them here
            // is what makes convergence real rather than dependent on some
            // dispatcher calling `lease_pending` and noticing.
            receipt.deliveries_forgotten = terminalize_deliveries(
                transaction,
                &format!(
                    "SELECT d.idempotency_key, d.observation_id, d.attempt_number, d.state \
                     FROM tdmem_observation_journal_v1 j \
                     JOIN tdmem_observation_delivery_v1 d ON d.idempotency_key = j.idempotency_key \
                     WHERE j.payload_bytes IS NULL AND d.state IN {UNSETTLED_STATES} \
                     ORDER BY j.admitted_at_micros, j.idempotency_key LIMIT ?1"
                ),
                params![batch],
                DeliveryStateV1::Forgotten,
                ObservationOutcomeV1::Cancelled,
                "content_forgotten",
                now_unix_micros,
            )?;

            // (3) Rows whose delivery is terminal and whose audit window has
            // closed are removed entirely, receipts included.
            let mut deletable: Vec<(String, String)> = Vec::new();
            {
                let mut statement = transaction.prepare(&format!(
                    "SELECT j.idempotency_key, j.observation_id \
                     FROM tdmem_observation_journal_v1 j \
                     JOIN tdmem_observation_delivery_v1 d ON d.idempotency_key = j.idempotency_key \
                     WHERE j.content_forgotten_at_micros IS NOT NULL \
                       AND j.content_forgotten_at_micros <= ?1 \
                       AND d.state IN {SETTLED_STATES} \
                     ORDER BY j.content_forgotten_at_micros, j.idempotency_key LIMIT ?2"
                ))?;
                let mut rows = statement.query(params![deletable_cutoff, batch])?;
                while let Some(row) = rows.next()? {
                    deletable.push((row.get(0)?, row.get(1)?));
                }
            }
            for (key, observation_id) in deletable {
                let receipts = transaction.execute(
                    "DELETE FROM tdmem_observation_receipt_v1 WHERE observation_id = ?1",
                    params![&observation_id],
                )?;
                receipt.receipts_deleted = receipt.receipts_deleted.saturating_add(read_u32(
                    i64::try_from(receipts).unwrap_or(i64::MAX),
                    "receipts_deleted",
                )?);
                // The refused-terminal audit ages out with the attempt history
                // it belongs to. It is keyed by observation, not by the journal
                // row, so nothing cascades it: without this it would outlive
                // every row it describes and grow without bound.
                transaction.execute(
                    "DELETE FROM tdmem_observation_attempt_refusal_v1 WHERE observation_id = ?1",
                    params![&observation_id],
                )?;
                // The delivery row cascades with the journal row.
                let deleted = transaction.execute(
                    "DELETE FROM tdmem_observation_journal_v1 WHERE idempotency_key = ?1",
                    params![&key],
                )?;
                receipt.journal_rows_deleted =
                    receipt.journal_rows_deleted.saturating_add(read_u32(
                        i64::try_from(deleted).unwrap_or(i64::MAX),
                        "journal_rows_deleted",
                    )?);
            }

            // (4) Withheld audit rows age out on the same audit window. They
            // hold digests of refused — often secret-bearing — content, so
            // "never swept" would mean "kept forever".
            let withheld =
                transaction.execute(DELETE_AGED_WITHHELD, params![deletable_cutoff, batch])?;
            receipt.withheld_rows_deleted = read_u32(
                i64::try_from(withheld).unwrap_or(i64::MAX),
                "withheld_rows_deleted",
            )?;

            receipt.remaining_candidates =
                remaining_candidates(transaction, &policy, now_unix_micros, deletable_cutoff)?;
            Ok(receipt)
        })?;

        // Purged pages live on in the write-ahead log until it is checkpointed,
        // so a sweep that freed content is not done on disk until this lands.
        let mut receipt = receipt;
        receipt.wal_truncated = self.checkpoint_truncate()?;
        Ok(receipt)
    }

    fn forget_source(
        &self,
        request: &ForgetSourceRequestV1,
    ) -> Result<ForgetReceiptV1, ObservationJournalError> {
        request.validate()?;
        let mut receipt = self.with_transaction(|transaction| {
            let key = request.forget_source_key.as_str();
            let matched: i64 = transaction.query_row(
                "SELECT COUNT(*) FROM tdmem_observation_journal_v1 WHERE forget_source_key = ?1",
                params![key],
                |row| row.get(0),
            )?;
            let (with_content, with_binding): (i64, i64) = transaction.query_row(
                "SELECT COALESCE(SUM(payload_bytes IS NOT NULL), 0), \
                        COALESCE(SUM(sanitization_receipt_id IS NOT NULL), 0) \
                 FROM tdmem_observation_journal_v1 WHERE forget_source_key = ?1",
                params![key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;

            // Every unsettled delivery is terminalized, without a flag to opt
            // out of it: once the content is gone the row can never be
            // delivered, and leaving it queued would leave a dispatcher
            // repeatedly discovering an observation whose bytes no longer
            // exist.
            let deliveries_forgotten = terminalize_deliveries(
                transaction,
                &format!(
                    "SELECT d.idempotency_key, d.observation_id, d.attempt_number, d.state \
                     FROM tdmem_observation_delivery_v1 d \
                     JOIN tdmem_observation_journal_v1 j ON j.idempotency_key = d.idempotency_key \
                     WHERE j.forget_source_key = ?1 AND d.state IN {UNSETTLED_STATES} \
                     ORDER BY d.idempotency_key"
                ),
                params![key],
                DeliveryStateV1::Forgotten,
                ObservationOutcomeV1::Cancelled,
                &request.reason,
                request.requested_at_unix_micros,
            )?;

            transaction.execute(
                &format!(
                    "UPDATE tdmem_observation_journal_v1 SET {PURGE_COLUMNS}, \
                     content_forgotten_at_micros = COALESCE(content_forgotten_at_micros, ?1) \
                     WHERE forget_source_key = ?2"
                ),
                params![request.requested_at_unix_micros, key],
            )?;

            // The withheld audit answers to the same key: a refused event's
            // digests are exactly as much a record of the subject as an
            // admitted one's.
            let withheld_deleted = transaction.execute(
                "DELETE FROM tdmem_observation_withheld_v2 WHERE forget_source_key = ?1",
                params![key],
            )?;

            let retained: i64 = transaction.query_row(
                "SELECT COUNT(*) FROM tdmem_observation_receipt_v1 r \
                 JOIN tdmem_observation_journal_v1 j ON j.idempotency_key = r.idempotency_key \
                 WHERE j.forget_source_key = ?1",
                params![key],
                |row| row.get(0),
            )?;

            Ok(ForgetReceiptV1 {
                forget_source_key: request.forget_source_key.clone(),
                journal_rows_matched: read_u64(matched, "journal_rows_matched")?,
                payloads_zeroed: read_u64(with_content, "payloads_zeroed")?,
                sanitization_bindings_cleared: read_u64(
                    with_binding,
                    "sanitization_bindings_cleared",
                )?,
                deliveries_forgotten: u64::from(deliveries_forgotten),
                withheld_rows_deleted: read_u64(
                    i64::try_from(withheld_deleted).unwrap_or(i64::MAX),
                    "withheld_rows_deleted",
                )?,
                receipts_retained: read_u64(retained, "receipts_retained")?,
                wal_truncated: false,
                completed_at_unix_micros: request.requested_at_unix_micros,
            })
        })?;

        // The rows are purged, but the pre-purge page images are still in the
        // write-ahead log until it is checkpointed and truncated. Until that
        // lands the deletion is not complete on disk, and the receipt says so.
        receipt.wal_truncated = self.checkpoint_truncate()?;
        Ok(receipt)
    }

    fn verify_forgotten(
        &self,
        key: &ForgetSourceKeyV1,
    ) -> Result<ForgetVerificationV1, ObservationJournalError> {
        // Truncate first: verification must describe the state of the store
        // after every purged page has actually left the log, not before.
        let wal_truncated = self.checkpoint_truncate()?;
        self.with_connection(|connection| {
            let (matching, with_content, with_binding): (i64, i64, i64) = connection.query_row(
                "SELECT COUNT(*), COALESCE(SUM(payload_bytes IS NOT NULL), 0), \
                        COALESCE(SUM(sanitization_receipt_id IS NOT NULL), 0) \
                 FROM tdmem_observation_journal_v1 WHERE forget_source_key = ?1",
                params![key.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
            let undelivered: i64 = connection.query_row(
                &format!(
                    "SELECT COUNT(*) FROM tdmem_observation_delivery_v1 d \
                     JOIN tdmem_observation_journal_v1 j ON j.idempotency_key = d.idempotency_key \
                     WHERE j.forget_source_key = ?1 AND d.state IN {UNSETTLED_STATES}"
                ),
                params![key.as_str()],
                |row| row.get(0),
            )?;
            let withheld: i64 = connection.query_row(
                "SELECT COUNT(*) FROM tdmem_observation_withheld_v2 WHERE forget_source_key = ?1",
                params![key.as_str()],
                |row| row.get(0),
            )?;
            let rows_with_content_remaining =
                read_u64(with_content, "rows_with_content_remaining")?;
            let rows_with_binding_remaining =
                read_u64(with_binding, "rows_with_binding_remaining")?;
            let undelivered_remaining = read_u64(undelivered, "undelivered_remaining")?;
            let withheld_rows_remaining = read_u64(withheld, "withheld_rows_remaining")?;
            Ok(ForgetVerificationV1 {
                forget_source_key: key.clone(),
                journal_rows_matching: read_u64(matching, "journal_rows_matching")?,
                rows_with_content_remaining,
                rows_with_binding_remaining,
                undelivered_remaining,
                withheld_rows_remaining,
                wal_truncated,
                verified: rows_with_content_remaining == 0
                    && rows_with_binding_remaining == 0
                    && undelivered_remaining == 0
                    && withheld_rows_remaining == 0
                    && wal_truncated,
            })
        })
    }
}

/// Bounded deletion of aged withheld audit rows.
///
/// `DELETE ... LIMIT` needs a compile option the bundled library does not carry,
/// so the bound is applied by selecting the primary key of one batch.
const DELETE_AGED_WITHHELD: &str = r#"
DELETE FROM tdmem_observation_withheld_v2
WHERE (source_authority, exact_scope_sha256, source_stream, source_sequence, receipt_id) IN (
    SELECT source_authority, exact_scope_sha256, source_stream, source_sequence, receipt_id
    FROM tdmem_observation_withheld_v2
    WHERE withheld_at_micros <= ?1
    ORDER BY withheld_at_micros, source_sequence, receipt_id
    LIMIT ?2)
"#;

/// Every class of work a further sweep would still do.
///
/// Counting only what this sweep happened to purge would report zero while
/// stranded rows sat in the store forever, so each phase contributes its own
/// remainder: content still to purge, deliveries still to terminalize, rows
/// still to delete, and withheld records still to age out.
fn remaining_candidates(
    transaction: &Transaction<'_>,
    policy: &RetentionPolicyV1,
    now_unix_micros: i64,
    deletable_cutoff: i64,
) -> Result<u64, ObservationJournalError> {
    let purge_query = format!(
        "SELECT COUNT(*) FROM tdmem_observation_journal_v1 j \
         WHERE j.payload_bytes IS NOT NULL AND {EFFECTIVE_EXPIRY} <= ?5"
    );
    let purgeable: i64 = transaction.query_row(
        &purge_query,
        params![
            policy.ephemeral_max_age_micros,
            policy.session_max_age_micros,
            policy.project_max_age_micros,
            policy.profile_max_age_micros,
            now_unix_micros,
        ],
        |row| row.get(0),
    )?;
    let stranded: i64 = transaction.query_row(
        &format!(
            "SELECT COUNT(*) FROM tdmem_observation_journal_v1 j \
             JOIN tdmem_observation_delivery_v1 d ON d.idempotency_key = j.idempotency_key \
             WHERE j.payload_bytes IS NULL AND d.state IN {UNSETTLED_STATES}"
        ),
        [],
        |row| row.get(0),
    )?;
    let deletable: i64 = transaction.query_row(
        &format!(
            "SELECT COUNT(*) FROM tdmem_observation_journal_v1 j \
             JOIN tdmem_observation_delivery_v1 d ON d.idempotency_key = j.idempotency_key \
             WHERE j.content_forgotten_at_micros IS NOT NULL \
               AND j.content_forgotten_at_micros <= ?1 \
               AND d.state IN {SETTLED_STATES}"
        ),
        params![deletable_cutoff],
        |row| row.get(0),
    )?;
    let withheld: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM tdmem_observation_withheld_v2 WHERE withheld_at_micros <= ?1",
        params![deletable_cutoff],
        |row| row.get(0),
    )?;
    Ok(read_u64(purgeable, "purgeable")?
        .saturating_add(read_u64(stranded, "stranded")?)
        .saturating_add(read_u64(deletable, "deletable")?)
        .saturating_add(read_u64(withheld, "withheld")?))
}
