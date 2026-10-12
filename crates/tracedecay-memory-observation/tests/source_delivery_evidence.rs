//! Exact source status requires validated durable receipt evidence.

mod support;

use support::{Builder, T0, TestResult, applied_receipt, journal, lease_request, receipt_for};
use tracedecay_memory_observation::{
    AdmittedObservationV1, DeliveryStateV1, ExpectedSourceDeliveryV1, ObservationCommittedEffectV1,
    ObservationDispatchPortV1, ObservationJournalError, ObservationJournalReaderV1,
    ObservationLaneKeyV1, ObservationOutcomeV1, RecoveryTimeBudgetV1, SourceDeliveryEvidenceV1,
    SourceSequenceV1, SqliteObservationJournal,
};
use tracedecay_memory_provider_api::CancellationToken;

fn read(
    store: &SqliteObservationJournal,
    admitted: &AdmittedObservationV1,
) -> Result<SourceDeliveryEvidenceV1, ObservationJournalError> {
    Ok(store
        .read_source_deliveries(
            &ObservationLaneKeyV1::of(&admitted.target),
            &admitted.exact_scope,
            &admitted.stream_key(),
            &[ExpectedSourceDeliveryV1 {
                source_sequence: admitted.source.source_sequence,
                source_event_id: admitted.source.source_event_id.clone(),
            }],
            RecoveryTimeBudgetV1 {
                remaining_micros: 1_000_000,
            },
            &CancellationToken::new(),
        )?
        .remove(0))
}

#[test]
fn exact_source_reads_distinguish_missing_pending_leased_unknown_and_durable_ack() -> TestResult {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("journal.sqlite3");
    let store = journal(&path)?;
    let admitted = Builder::at_sequence(7).build()?;
    assert_eq!(read(&store, &admitted)?, SourceDeliveryEvidenceV1::Missing);
    store.append_admitted(&admitted)?;
    assert!(matches!(
        read(&store, &admitted)?,
        SourceDeliveryEvidenceV1::Retained {
            state: DeliveryStateV1::Pending,
            receipt: None,
            ..
        }
    ));
    let leased = store.lease_pending(&lease_request(T0, 1))?.remove(0);
    assert!(matches!(
        read(&store, &admitted)?,
        SourceDeliveryEvidenceV1::Retained {
            state: DeliveryStateV1::Leased,
            receipt: None,
            ..
        }
    ));
    let unknown = receipt_for(
        &leased,
        ObservationOutcomeV1::EffectUnknown,
        ObservationCommittedEffectV1::Unknown,
        T0,
    );
    store.record_attempt(&unknown)?;
    assert!(
        matches!(read(&store, &admitted)?, SourceDeliveryEvidenceV1::Retained { state: DeliveryStateV1::EffectUnknown, receipt: Some(actual), .. } if actual == unknown)
    );
    let leased = store
        .lease_pending(&lease_request(T0 + support::MINUTE, 1))?
        .remove(0);
    let receipt = applied_receipt(&leased, T0 + support::MINUTE);
    store.record_attempt(&receipt)?;
    drop(store);
    let reopened = journal(&path)?;
    assert!(
        matches!(read(&reopened, &admitted)?, SourceDeliveryEvidenceV1::Retained { state: DeliveryStateV1::Acknowledged, admitted: actual, receipt: Some(actual_receipt) } if *actual == admitted && actual_receipt == receipt)
    );
    Ok(())
}

#[test]
fn exact_source_receipt_decoder_refuses_identity_digest_and_state_drift() -> TestResult {
    for (column, replacement) in [
        ("provider_id", "other.provider"),
        ("idempotency_key", "wrong-key"),
        (
            "payload_sha256",
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        ),
        ("provider_receipt_digest", "bad-digest"),
        ("provider_instance_id", "other-instance"),
    ] {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("journal.sqlite3");
        let store = journal(&path)?;
        let admitted = Builder::at_sequence(1).build()?;
        store.append_admitted(&admitted)?;
        let leased = store.lease_pending(&lease_request(T0, 1))?.remove(0);
        store.record_attempt(&applied_receipt(&leased, T0))?;
        rusqlite::Connection::open(&path)?.execute(
            &format!("UPDATE tdmem_observation_receipt_v1 SET {column} = ?1"),
            [replacement],
        )?;
        assert!(
            read(&store, &admitted).is_err(),
            "accepted corrupt {column}"
        );
    }
    Ok(())
}

#[test]
fn exact_source_key_checks_canonical_identity_and_never_decodes_unrequested_rows() -> TestResult {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("journal.sqlite3");
    let store = journal(&path)?;
    let admitted = Builder::at_sequence(1).build()?;
    let unrelated = Builder::at_sequence(900).build()?;
    store.append_admitted(&admitted)?;
    store.append_admitted(&unrelated)?;
    let connection = rusqlite::Connection::open(&path)?;
    connection.execute(
        "UPDATE tdmem_observation_journal_v1 SET payload_bytes = X'00' WHERE source_sequence = 900",
        [],
    )?;
    assert!(matches!(
        read(&store, &admitted)?,
        SourceDeliveryEvidenceV1::Retained { .. }
    ));
    assert!(read(&store, &unrelated).is_err());
    let mut wrong = admitted.clone();
    wrong.source.source_event_id = "wrong-canonical-observation".into();
    assert!(read(&store, &wrong).is_err());
    connection.execute(
        "UPDATE tdmem_observation_delivery_v1 SET state = 'acknowledged' WHERE source_sequence = 1",
        [],
    )?;
    assert!(
        read(&store, &admitted).is_err(),
        "bare acknowledged state has no provider receipt"
    );
    Ok(())
}

#[test]
fn exact_source_reads_enforce_caller_bounds_and_duplicate_key_limits() -> TestResult {
    let temp = tempfile::tempdir()?;
    let store = journal(&temp.path().join("journal.sqlite3"))?;
    let admitted = Builder::at_sequence(1).build()?;
    let sources = (1..=257)
        .map(|sequence| ExpectedSourceDeliveryV1 {
            source_sequence: SourceSequenceV1(sequence),
            source_event_id: format!("canonical-{sequence}"),
        })
        .collect::<Vec<_>>();
    let lane = ObservationLaneKeyV1::of(&admitted.target);
    let stream = admitted.stream_key();
    let token = CancellationToken::new();
    let run = |sources: &[ExpectedSourceDeliveryV1], budget| {
        store.read_source_deliveries(
            &lane,
            &admitted.exact_scope,
            &stream,
            sources,
            RecoveryTimeBudgetV1 {
                remaining_micros: budget,
            },
            &token,
        )
    };
    assert_eq!(run(&sources[..256], 1_000_000)?.len(), 256);
    assert!(run(&sources, 1_000_000).is_err());
    assert!(run(&[], 1_000_000).is_err());
    assert!(run(&[sources[0].clone(), sources[0].clone()], 1_000_000).is_err());
    assert!(matches!(
        run(&sources[..1], 0),
        Err(ObservationJournalError::BudgetExhausted { .. })
    ));
    token.cancel();
    assert!(matches!(
        run(&sources[..1], 1_000_000),
        Err(ObservationJournalError::OperationCancelled { .. })
    ));
    Ok(())
}

#[test]
fn exact_source_duplicate_receipts_survive_restart_and_purged_content_stays_unavailable()
-> TestResult {
    use tracedecay_memory_observation::{ForgetSourceRequestV1, ObservationRetentionPortV1};
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("journal.sqlite3");
    let store = journal(&path)?;
    let admitted = Builder::at_sequence(1).build()?;
    store.append_admitted(&admitted)?;
    let leased = store.lease_pending(&lease_request(T0, 1))?.remove(0);
    let receipt = receipt_for(
        &leased,
        ObservationOutcomeV1::DuplicateAcknowledged,
        ObservationCommittedEffectV1::Duplicate,
        T0,
    );
    store.record_attempt(&receipt)?;
    drop(store);
    let store = journal(&path)?;
    assert!(
        matches!(read(&store, &admitted)?, SourceDeliveryEvidenceV1::Retained { state: DeliveryStateV1::DuplicateAcknowledged, receipt: Some(actual), .. } if actual == receipt)
    );
    store.forget_source(&ForgetSourceRequestV1 {
        forget_source_key: admitted.privacy.forget_source_key.clone(),
        requested_at_unix_micros: T0 + support::SECOND,
        reason: "test deletion".into(),
    })?;
    // Privacy deletion purges content while preserving a completed attempt's
    // audit state; that retained acknowledgement still cannot authorize use.
    assert_eq!(
        read(&store, &admitted)?,
        SourceDeliveryEvidenceV1::Purged {
            state: DeliveryStateV1::DuplicateAcknowledged
        }
    );
    Ok(())
}
