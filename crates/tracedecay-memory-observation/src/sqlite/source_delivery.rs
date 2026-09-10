//! One bounded read snapshot over caller-selected source keys.

use std::{collections::BTreeSet, time::Instant};

use rusqlite::{Connection, Row, params};
use tracedecay_memory_provider_api::{CancellationToken, OwnedExactScope};

use super::{
    SqliteObservationJournal,
    row::{
        LEASE_SELECT_COLUMNS, RECEIPT_SELECT_COLUMNS, StoredExactScopeV1, decode_admitted,
        decode_json, decode_receipt, read_u32, read_u64, sql_i64,
    },
};
use crate::{
    CanonicalSettlementReceiptV1, DeliveryStateV1, ExpectedSourceDeliveryV1,
    ObservationDeliveryReceiptV1, ObservationIdV1, ObservationIdempotencyKeyV1,
    ObservationJournalError, ObservationLaneKeyV1, RecoveryTimeBudgetV1, SourceDeliveryEvidenceV1,
    SourceStreamKeyV1,
};

const OPERATION: &str = "read_source_deliveries";

fn corrupt(field: &'static str) -> ObservationJournalError {
    ObservationJournalError::Corrupt {
        table: "tdmem_observation_delivery_v1",
        field,
    }
}

fn check_bound(
    started: Instant,
    budget: RecoveryTimeBudgetV1,
    cancellation: &CancellationToken,
) -> Result<(), ObservationJournalError> {
    if cancellation.is_cancelled() {
        return Err(ObservationJournalError::OperationCancelled {
            operation: OPERATION,
        });
    }
    if budget.remaining_micros <= i64::try_from(started.elapsed().as_micros()).unwrap_or(i64::MAX) {
        return Err(ObservationJournalError::BudgetExhausted {
            operation: OPERATION,
        });
    }
    Ok(())
}

impl SqliteObservationJournal {
    /// Reads at most 256 exact source keys in one deferred read transaction.
    ///
    /// This uses the existing source and receipt unique indexes, never a queue
    /// count or replay watermark. The caller's one budget includes lock wait,
    /// SQLite contention, decoding and all requested keys. No rows are changed.
    pub fn read_source_deliveries(
        &self,
        lane: &ObservationLaneKeyV1,
        exact_scope: &OwnedExactScope,
        stream: &SourceStreamKeyV1,
        sources: &[ExpectedSourceDeliveryV1],
        budget: RecoveryTimeBudgetV1,
        cancellation: &CancellationToken,
    ) -> Result<Vec<SourceDeliveryEvidenceV1>, ObservationJournalError> {
        let started = Instant::now();
        check_bound(started, budget, cancellation)?;
        lane.validate()?;
        exact_scope.validate()?;
        stream.validate()?;
        if sources.is_empty()
            || sources.len() > 256
            || stream.exact_scope_sha256 != exact_scope.exact_scope_sha256()
        {
            return Err(ObservationJournalError::ValueOutOfRange {
                field: "source_delivery_request",
            });
        }
        let mut unique = BTreeSet::new();
        let mut unique_events = BTreeSet::new();
        for source in sources {
            crate::identity::require_bounded(
                &source.source_event_id,
                "source_event_id",
                crate::SOURCE_EVENT_ID_MAX_BYTES,
            )?;
            if source.source_sequence.0 == 0
                || !unique.insert(source.source_sequence)
                || !unique_events.insert(source.source_event_id.as_str())
            {
                return Err(ObservationJournalError::ValueOutOfRange {
                    field: "duplicate_source_sequence",
                });
            }
        }
        let lock_budget = RecoveryTimeBudgetV1 {
            remaining_micros: budget
                .remaining_micros
                .saturating_sub(i64::try_from(started.elapsed().as_micros()).unwrap_or(i64::MAX)),
        };
        self.with_cancellable_bounded_connection(OPERATION, lock_budget, cancellation, |connection| {
            // unchecked_transaction is deferred and requires only &Connection;
            // the outer bounded helper owns the sole journal mutex guard.
            let transaction = connection.unchecked_transaction()?;
            let sql = format!(
                "SELECT {LEASE_SELECT_COLUMNS}, j.provenance_sha256, \
                 j.occurred_at_micros, j.admitted_at_micros, j.request_id, j.envelope_sha256, \
                 d.state, d.observation_id, d.provider_id, d.registration_revision, \
                 d.exact_scope_sha256, d.source_sequence, d.last_provider_instance_id, \
                 d.last_receipt_id, d.last_outcome, d.last_committed_effect, \
                 j.source_authority, j.source_stream, j.source_event_id, j.source_event_revision \
                 FROM tdmem_observation_journal_v1 j INDEXED BY tdmem_observation_journal_source_v1 \
                 LEFT JOIN tdmem_observation_delivery_v1 d ON d.idempotency_key = j.idempotency_key \
                 WHERE j.provider_id = ?1 AND j.registration_revision = ?2 \
                 AND j.source_authority = ?3 AND j.exact_scope_sha256 = ?4 \
                 AND j.source_stream = ?5 AND j.source_sequence = ?6 LIMIT 1"
            );
            let mut statement = transaction.prepare(&sql)?;
            let mut evidence = Vec::with_capacity(sources.len());
            for source in sources {
                check_bound(started, budget, cancellation)?;
                // Each statement inherits the remaining original allowance;
                // earlier keys never buy a fresh SQLite busy timeout.
                let left = budget.remaining_micros.saturating_sub(i64::try_from(started.elapsed().as_micros()).unwrap_or(i64::MAX));
                transaction.busy_timeout(std::time::Duration::from_micros(u64::try_from(left.max(0)).unwrap_or(0)))?;
                let mut rows = statement.query(params![
                    lane.provider_id.as_str(), sql_i64(lane.registration_revision, "registration_revision")?,
                    stream.source_authority.as_wire(), &stream.exact_scope_sha256,
                    stream.source_stream.as_str(), sql_i64(source.source_sequence.0, "source_sequence")?,
                ])?;
                evidence.push(match rows.next()? {
                    None => SourceDeliveryEvidenceV1::Missing,
                    Some(row) => decode_source_delivery(&transaction, row, lane, exact_scope, stream, source)?,
                });
                check_bound(started, budget, cancellation)?;
            }
            drop(statement);
            transaction.commit()?;
            check_bound(started, budget, cancellation)?;
            Ok(evidence)
        })
    }
}

fn decode_source_delivery(
    connection: &Connection,
    row: &Row<'_>,
    lane: &ObservationLaneKeyV1,
    exact_scope: &OwnedExactScope,
    stream: &SourceStreamKeyV1,
    expected: &ExpectedSourceDeliveryV1,
) -> Result<SourceDeliveryEvidenceV1, ObservationJournalError> {
    let key = ObservationIdempotencyKeyV1::parse(&row.get::<_, String>(0)?)?;
    let observation_id = ObservationIdV1::parse(&row.get::<_, String>(1)?)?;
    let state = DeliveryStateV1::from_wire(
        &row.get::<_, Option<String>>(34)?
            .ok_or_else(|| corrupt("missing_delivery"))?,
    )?;
    let scope = decode_json::<StoredExactScopeV1>(&row.get::<_, String>(27)?, "exact_scope_json")?
        .into_scope()?;
    let source = decode_json::<CanonicalSettlementReceiptV1>(
        &row.get::<_, String>(28)?,
        "settlement_receipt_json",
    )?;
    source.validate()?;
    if scope != *exact_scope
        || row.get::<_, String>(6)? != stream.exact_scope_sha256
        || row.get::<_, String>(2)? != lane.provider_id.as_str()
        || read_u64(row.get(4)?, "registration_revision")? != lane.registration_revision
        || row.get::<_, String>(35)? != observation_id.as_str()
        || row.get::<_, String>(36)? != lane.provider_id.as_str()
        || read_u64(row.get(37)?, "registration_revision")? != lane.registration_revision
        || row.get::<_, String>(38)? != stream.exact_scope_sha256
        || read_u64(row.get(39)?, "source_sequence")? != expected.source_sequence.0
        || read_u64(row.get(21)?, "source_sequence")? != expected.source_sequence.0
        || row.get::<_, String>(44)? != stream.source_authority.as_wire()
        || row.get::<_, String>(45)? != stream.source_stream.as_str()
        || row.get::<_, String>(46)? != expected.source_event_id
        || read_u64(row.get(47)?, "source_event_revision")? != source.source_event_revision
        || source.source_authority != stream.source_authority
        || source.source_stream != stream.source_stream
        || source.source_sequence != expected.source_sequence
        || source.source_event_id != expected.source_event_id
    {
        return Err(corrupt("source_delivery_identity"));
    }
    let attempt = read_u32(row.get(26)?, "attempt_number")?;
    let last_receipt: Option<String> = row.get(41)?;
    if last_receipt.is_none()
        && (row.get::<_, Option<String>>(42)?.is_some()
            || row.get::<_, Option<String>>(43)?.is_some())
    {
        return Err(corrupt("outcome_without_receipt"));
    }
    let receipt = last_receipt
        .as_deref()
        .map(|id| read_matching_receipt(connection, id, row, attempt, state))
        .transpose()?;
    if matches!(
        state,
        DeliveryStateV1::Acknowledged | DeliveryStateV1::DuplicateAcknowledged
    ) && receipt.is_none()
    {
        return Err(corrupt("acknowledged_without_receipt"));
    }
    if row.get_ref(10)?.data_type() == rusqlite::types::Type::Null {
        return Ok(SourceDeliveryEvidenceV1::Purged { state });
    }
    let admitted = decode_admitted(row)?;
    if admitted.idempotency_key != key
        || admitted.observation_id != observation_id
        || admitted.exact_scope != *exact_scope
        || admitted.source != source
    {
        return Err(corrupt("admitted_source_identity"));
    }
    Ok(SourceDeliveryEvidenceV1::Retained {
        admitted: Box::new(admitted),
        state,
        receipt,
    })
}

fn read_matching_receipt(
    connection: &Connection,
    receipt_id: &str,
    delivery: &Row<'_>,
    attempt: u32,
    state: DeliveryStateV1,
) -> Result<ObservationDeliveryReceiptV1, ObservationJournalError> {
    let mut statement = connection.prepare(&format!(
        "SELECT {RECEIPT_SELECT_COLUMNS} FROM tdmem_observation_receipt_v1 WHERE receipt_id = ?1 LIMIT 1"
    ))?;
    let mut rows = statement.query([receipt_id])?;
    let receipt = decode_receipt(
        rows.next()?
            .ok_or_else(|| corrupt("missing_last_receipt"))?,
    )?;
    let acknowledged = matches!(
        state,
        DeliveryStateV1::Acknowledged | DeliveryStateV1::DuplicateAcknowledged
    );
    if receipt.receipt_id.as_str() != receipt_id
        || receipt.idempotency_key.as_str() != delivery.get::<_, String>(0)?
        || receipt.observation_id.as_str() != delivery.get::<_, String>(1)?
        || receipt.payload_sha256 != delivery.get::<_, String>(9)?
        || receipt.extensions_digest != delivery.get::<_, String>(11)?
        || receipt.provider_id.as_str() != delivery.get::<_, String>(2)?
        || receipt.registration_revision != read_u64(delivery.get(4)?, "registration_revision")?
        || receipt.attempt_number > attempt
        || delivery.get::<_, Option<String>>(42)?.as_deref() != Some(receipt.outcome.as_wire())
        || delivery.get::<_, Option<String>>(43)?.as_deref()
            != Some(receipt.committed_effect.as_wire())
        || (acknowledged
            && (receipt.attempt_number != attempt
                || receipt.implied_state() != state
                || receipt.provider_instance_id.is_none()
                || receipt.provider_instance_id != delivery.get::<_, Option<String>>(40)?))
    {
        return Err(corrupt("last_receipt_identity"));
    }
    Ok(receipt)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_delivery_connection_wait_is_bounded_and_cancellable()
    -> Result<(), ObservationJournalError> {
        let journal = SqliteObservationJournal::open_in_memory(crate::RetentionPolicyV1 {
            ephemeral_max_age_micros: 1_000_000,
            session_max_age_micros: 1_000_000,
            project_max_age_micros: 1_000_000,
            profile_max_age_micros: 1_000_000,
            receipt_retention_micros: 1_000_000,
            max_queue_items: 256,
            max_queue_bytes: 1_000_000,
            max_attempts: 3,
            backoff_base_micros: 1_000,
            backoff_max_micros: 10_000,
            sweep_batch_rows: 256,
        })?;
        let _held = journal
            .connection
            .lock()
            .map_err(|_| ObservationJournalError::LockPoisoned)?;
        let started = Instant::now();
        let token = CancellationToken::new();
        let result = journal.with_cancellable_bounded_connection(
            OPERATION,
            RecoveryTimeBudgetV1 {
                remaining_micros: 10_000,
            },
            &token,
            |_| Ok(()),
        );
        assert!(matches!(
            result,
            Err(ObservationJournalError::BudgetExhausted { .. })
        ));
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        std::thread::scope(|scope| {
            scope.spawn(|| {
                std::thread::sleep(std::time::Duration::from_millis(5));
                token.cancel();
            });
            let started = Instant::now();
            let result = journal.with_cancellable_bounded_connection(
                OPERATION,
                RecoveryTimeBudgetV1 {
                    remaining_micros: 5_000_000,
                },
                &token,
                |_| Ok(()),
            );
            assert!(matches!(
                result,
                Err(ObservationJournalError::OperationCancelled { .. })
            ));
            assert!(started.elapsed() < std::time::Duration::from_secs(1));
        });
        Ok(())
    }
}
