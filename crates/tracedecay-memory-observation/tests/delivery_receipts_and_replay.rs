//! AC3 continued: the delivery state machine, immutable receipts, bounded
//! retries, lease recovery, and the withheld replay path.

mod support;

use support::{
    Builder, LEASE, PROVIDER_RECEIPT_DIGEST, SECOND, T0, TestResult, applied_receipt, journal,
    lease_request, policy, receipt_for, stream_key, unavailable_receipt,
};

use tracedecay_memory_provider_api::contract::TerminalCode;
use tracedecay_memory_provider_api::{
    CommittedEffectEvidence, FallbackDirective, ProviderOperation, TerminalRecord, WithheldReason,
    derive_withheld_receipt_id, empty_opaque_extensions_digest,
};

use tracedecay_memory_observation::{
    AppendOutcomeV1, AttemptOutcomeV1, DeliveryStateV1, ForgetSourceKeyV1,
    ObservationCommittedEffectV1, ObservationDeliveryReceiptV1, ObservationDispatchPortV1,
    ObservationJournalError, ObservationJournalReaderV1, ObservationOutcomeV1, ReplayDispositionV1,
    SourceSequenceV1, WithheldAdmissionV1,
};

/// Builds the terminal a provider returns when it recognises a redelivery of a
/// mutation it already committed. `duplicate_of_idempotency_key` is a
/// parameter so a test can hand back a key that is *not* the delivered
/// observation's.
fn duplicate_terminal(
    leased: &tracedecay_memory_observation::LeasedObservationV1,
    duplicate_of_idempotency_key: &str,
    duplicate_of_operation_id: &str,
) -> Result<TerminalRecord, Box<dyn std::error::Error>> {
    let effect = CommittedEffectEvidence::duplicate(
        1,
        duplicate_of_idempotency_key,
        duplicate_of_operation_id,
        PROVIDER_RECEIPT_DIGEST,
    )?;
    Ok(TerminalRecord::new(
        ProviderOperation::Observe,
        leased.target.provider_id.clone(),
        TerminalCode::Success,
        effect,
        FallbackDirective::forbidden(),
        format!("observe-{}", leased.observation_id.as_str()),
        leased.exact_scope_sha256.clone(),
        None,
    )?)
}

#[test]
fn a_provider_that_deduplicates_a_redelivery_mints_a_duplicate_receipt() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let admitted = Builder::at_sequence(1).build()?;
    store.append_admitted(&admitted)?;
    let leased = store.lease_pending(&lease_request(T0, 4))?;

    let terminal = duplicate_terminal(
        &leased[0],
        leased[0].idempotency_key.as_str(),
        "observe-original-operation",
    )?;
    let receipt =
        ObservationDeliveryReceiptV1::from_terminal(&terminal, &leased[0], T0, T0 + 1_000)?;

    // Both halves come from the provider's own typed evidence, not from a
    // guess about what a repeated attempt must mean.
    assert_eq!(receipt.outcome, ObservationOutcomeV1::DuplicateAcknowledged);
    assert_eq!(
        receipt.committed_effect,
        ObservationCommittedEffectV1::Duplicate
    );
    assert_eq!(
        receipt.implied_state(),
        DeliveryStateV1::DuplicateAcknowledged
    );
    assert!(!receipt.is_retryable());

    match store.record_attempt(&receipt)? {
        AttemptOutcomeV1::Recorded { state, .. } => {
            assert_eq!(state, DeliveryStateV1::DuplicateAcknowledged);
        }
        other => return Err(format!("unexpected attempt outcome: {other:?}").into()),
    }
    // Delivery is finished: a duplicate is an acknowledgement, not a retry.
    assert!(
        store
            .lease_pending(&lease_request(T0 + LEASE + SECOND, 4))?
            .is_empty()
    );
    Ok(())
}

#[test]
fn a_success_without_duplicate_evidence_is_still_applied() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    store.append_admitted(&Builder::at_sequence(1).build()?)?;
    let leased = store.lease_pending(&lease_request(T0, 4))?;

    let terminal = TerminalRecord::new(
        ProviderOperation::Observe,
        leased[0].target.provider_id.clone(),
        TerminalCode::Success,
        CommittedEffectEvidence::committed(
            1,
            2,
            Vec::new(),
            PROVIDER_RECEIPT_DIGEST,
            PROVIDER_RECEIPT_DIGEST,
        )?,
        FallbackDirective::forbidden(),
        format!("observe-{}", leased[0].observation_id.as_str()),
        leased[0].exact_scope_sha256.clone(),
        None,
    )?;
    let receipt =
        ObservationDeliveryReceiptV1::from_terminal(&terminal, &leased[0], T0, T0 + 1_000)?;
    assert_eq!(receipt.outcome, ObservationOutcomeV1::Applied);
    assert_eq!(
        receipt.committed_effect,
        ObservationCommittedEffectV1::Applied
    );
    Ok(())
}

#[test]
fn a_duplicate_claim_for_another_mutation_is_refused() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    store.append_admitted(&Builder::at_sequence(1).build()?)?;
    let leased = store.lease_pending(&lease_request(T0, 4))?;

    let foreign_key = "9".repeat(64);
    let terminal = duplicate_terminal(&leased[0], &foreign_key, "observe-original-operation")?;
    assert!(matches!(
        ObservationDeliveryReceiptV1::from_terminal(&terminal, &leased[0], T0, T0 + 1_000),
        Err(ObservationJournalError::DuplicateAcknowledgementKeyMismatch)
    ));
    Ok(())
}

/// The outcome and the committed effect are two readings of one piece of
/// provider evidence, so the journal refuses a receipt that carries only one of
/// them. This is the persistence-layer half of "a duplicate is never inferred":
/// `duplicate_acknowledged` cannot be recorded as a bare label with no
/// duplicate effect behind it, and a duplicate effect cannot be filed away
/// under an outcome that hides it.
#[test]
fn a_duplicate_outcome_without_a_duplicate_effect_is_refused() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    store.append_admitted(&Builder::at_sequence(1).build()?)?;
    let leased = store.lease_pending(&lease_request(T0, 4))?;

    for (outcome, committed_effect) in [
        (
            ObservationOutcomeV1::DuplicateAcknowledged,
            ObservationCommittedEffectV1::Applied,
        ),
        (
            ObservationOutcomeV1::Applied,
            ObservationCommittedEffectV1::Duplicate,
        ),
    ] {
        let receipt = receipt_for(&leased[0], outcome, committed_effect, T0);
        assert!(
            matches!(
                store.record_attempt(&receipt),
                Err(ObservationJournalError::DuplicateAcknowledgementIncoherent { .. })
            ),
            "outcome {outcome:?} with effect {committed_effect:?} was recorded"
        );
    }
    Ok(())
}

#[test]
fn every_attempt_writes_a_receipt_and_retries_end_in_exhausted() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let admitted = Builder::at_sequence(1).build()?;
    store.append_admitted(&admitted)?;

    let mut now = T0;
    let mut states = Vec::new();
    for attempt in 1..=policy().max_attempts {
        let leased = store.lease_pending(&lease_request(now, 4))?;
        assert_eq!(leased.len(), 1, "attempt {attempt} could not lease");
        assert_eq!(leased[0].attempt_number, attempt);
        match store.record_attempt(&unavailable_receipt(&leased[0], now))? {
            AttemptOutcomeV1::Recorded { state, .. } => states.push(state),
            other => return Err(format!("unexpected attempt outcome: {other:?}").into()),
        }
        now += policy().backoff_max_micros + SECOND;
    }
    assert_eq!(
        states,
        vec![
            DeliveryStateV1::Pending,
            DeliveryStateV1::Pending,
            DeliveryStateV1::Exhausted
        ]
    );

    let receipts = store.receipts_for(&admitted.observation_id)?;
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.attempt_number)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );

    // Exhausted work is still visible; it is never deleted silently.
    let page = store.inspect(&Default::default())?;
    assert_eq!(page.total_rows, 1);
    assert_eq!(page.rows[0].state, DeliveryStateV1::Exhausted);
    assert!(store.lease_pending(&lease_request(now, 4))?.is_empty());
    Ok(())
}

#[test]
fn receipts_are_immutable_and_a_resubmitted_attempt_is_a_duplicate() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let admitted = Builder::at_sequence(1).build()?;
    store.append_admitted(&admitted)?;
    let leased = store.lease_pending(&lease_request(T0, 4))?;
    let first = applied_receipt(&leased[0], T0);
    store.record_attempt(&first)?;

    let mut rewritten = first.clone();
    rewritten.outcome = ObservationOutcomeV1::RejectedPrivacyPolicy;
    rewritten.committed_effect = ObservationCommittedEffectV1::None;
    rewritten.provider_receipt_digest = None;
    assert_eq!(
        store.record_attempt(&rewritten)?,
        AttemptOutcomeV1::DuplicateReceipt {
            state: DeliveryStateV1::Acknowledged
        }
    );

    let receipts = store.receipts_for(&admitted.observation_id)?;
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].outcome, ObservationOutcomeV1::Applied);
    Ok(())
}

#[test]
fn acknowledgement_without_a_provider_receipt_digest_is_refused() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    store.append_admitted(&Builder::at_sequence(1).build()?)?;
    let leased = store.lease_pending(&lease_request(T0, 4))?;

    // Each outcome is paired with the effect its own provider evidence would
    // carry, so the refusal under test is the missing acknowledgement digest
    // and not an incoherent outcome/effect pair.
    for (outcome, committed_effect) in [
        (
            ObservationOutcomeV1::Applied,
            ObservationCommittedEffectV1::Applied,
        ),
        (
            ObservationOutcomeV1::DuplicateAcknowledged,
            ObservationCommittedEffectV1::Duplicate,
        ),
        (
            ObservationOutcomeV1::PartialEffect,
            ObservationCommittedEffectV1::Partial,
        ),
    ] {
        let mut receipt = receipt_for(&leased[0], outcome, committed_effect, T0);
        receipt.provider_receipt_digest = None;
        assert!(matches!(
            store.record_attempt(&receipt),
            Err(ObservationJournalError::AcknowledgementWithoutProviderReceipt { .. })
        ));
    }
    Ok(())
}

#[test]
fn a_receipt_describing_other_content_is_refused() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    store.append_admitted(&Builder::at_sequence(1).build()?)?;
    let leased = store.lease_pending(&lease_request(T0, 4))?;
    let mut receipt = applied_receipt(&leased[0], T0);
    receipt.payload_sha256 =
        "0000000000000000000000000000000000000000000000000000000000000000".to_owned();
    assert!(matches!(
        store.record_attempt(&receipt),
        Err(ObservationJournalError::ReceiptDigestMismatch {
            field: "payload_sha256"
        })
    ));
    Ok(())
}

#[test]
fn a_lost_lease_does_not_lose_the_receipt() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let admitted = Builder::at_sequence(1).build()?;
    store.append_admitted(&admitted)?;
    let leased = store.lease_pending(&lease_request(T0, 4))?;

    // The dispatcher stalled past its lease; another process reaped it.
    assert_eq!(store.reap_expired_leases(T0 + LEASE + SECOND, 8)?, 1);

    let outcome = store.record_attempt(&applied_receipt(&leased[0], T0 + LEASE + 2 * SECOND))?;
    assert!(matches!(outcome, AttemptOutcomeV1::LeaseLost { .. }));
    assert_eq!(store.receipts_for(&admitted.observation_id)?.len(), 1);
    Ok(())
}

#[test]
fn releasing_a_lease_reschedules_it_and_an_unknown_lease_is_typed() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    store.append_admitted(&Builder::at_sequence(1).build()?)?;
    let leased = store.lease_pending(&lease_request(T0, 4))?;
    store.release_lease(&leased[0].lease_id, T0 + 5 * SECOND)?;
    assert!(
        store
            .lease_pending(&lease_request(T0 + SECOND, 4))?
            .is_empty()
    );
    assert_eq!(
        store
            .lease_pending(&lease_request(T0 + 6 * SECOND, 4))?
            .len(),
        1
    );
    assert!(matches!(
        store.release_lease(&leased[0].lease_id, T0),
        Err(ObservationJournalError::UnknownLease { .. })
    ));
    Ok(())
}

#[test]
fn effect_unknown_stays_retryable_and_terminal_states_do_not() -> TestResult {
    assert!(!DeliveryStateV1::EffectUnknown.is_terminal());
    assert!(DeliveryStateV1::EffectUnknown.is_deliverable());
    assert!(DeliveryStateV1::Acknowledged.is_terminal());
    assert!(
        DeliveryStateV1::EffectUnknown.can_transition_to(DeliveryStateV1::Leased),
        "an unknown effect must stay reconcilable"
    );
    assert!(!DeliveryStateV1::Acknowledged.can_transition_to(DeliveryStateV1::Leased));
    assert!(DeliveryStateV1::Acknowledged.can_transition_to(DeliveryStateV1::Forgotten));
    assert!(!DeliveryStateV1::Forgotten.can_transition_to(DeliveryStateV1::Pending));

    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    store.append_admitted(&Builder::at_sequence(1).build()?)?;
    let leased = store.lease_pending(&lease_request(T0, 4))?;
    let outcome = store.record_attempt(&receipt_for(
        &leased[0],
        ObservationOutcomeV1::EffectUnknown,
        ObservationCommittedEffectV1::Unknown,
        T0,
    ))?;
    assert!(matches!(
        outcome,
        AttemptOutcomeV1::Recorded {
            state: DeliveryStateV1::EffectUnknown,
            ..
        }
    ));
    let backoff = policy().next_attempt_delay(1);
    assert_eq!(
        store
            .lease_pending(&lease_request(T0 + backoff + 2 * SECOND, 4))?
            .len(),
        1
    );
    Ok(())
}

#[test]
fn a_deadline_that_elapses_before_delivery_expires_with_a_terminal_receipt() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let admitted = Builder::at_sequence(1).build()?;
    store.append_admitted(&admitted)?;

    let past_deadline = admitted.deadline_unix_micros + SECOND;
    assert!(
        store
            .lease_pending(&lease_request(past_deadline, 4))?
            .is_empty()
    );

    let page = store.inspect(&Default::default())?;
    assert_eq!(page.rows[0].state, DeliveryStateV1::Expired);
    let receipts = store.receipts_for(&admitted.observation_id)?;
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].outcome, ObservationOutcomeV1::DeadlineExceeded);
    assert_eq!(
        receipts[0].committed_effect,
        ObservationCommittedEffectV1::None
    );
    Ok(())
}

#[test]
fn a_withheld_event_advances_the_cursor_without_creating_delivery_work() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    store.append_admitted(&Builder::at_sequence(4).build()?)?;

    let source_payload_sha256 = support::digest_hex(b"payload-holding-a-secret");
    let findings_digest = support::digest_hex(b"known-credential-prefix-at:$.message");
    let sanitizer_revision = support::SANITIZER_REVISION.to_owned();
    let extensions_digest = empty_opaque_extensions_digest();
    let receipt_id = derive_withheld_receipt_id(
        &sanitizer_revision,
        &source_payload_sha256,
        &extensions_digest,
        WithheldReason::SecretRejected,
        1,
        &findings_digest,
    );
    let withheld = WithheldAdmissionV1 {
        source_authority: "host_session".to_owned(),
        exact_scope_sha256: support::scope()?.exact_scope_sha256(),
        source_stream: "session-1".to_owned(),
        source_sequence: 5,
        source_event_id: "event-5".to_owned(),
        source_event_revision: "0".to_owned(),
        receipt_id,
        reason: WithheldReason::SecretRejected.as_str().to_owned(),
        source_payload_sha256,
        extensions_digest,
        sanitizer_revision,
        finding_count: 1,
        findings_digest,
        forget_source_key: ForgetSourceKeyV1::new("forget:session-1")?,
    };
    store.record_withheld_at(&withheld, T0 + SECOND)?;

    let cursor = store
        .replay_cursor(&stream_key("session-1")?)?
        .ok_or("cursor missing")?;
    assert_eq!(cursor.last_admitted_sequence, SourceSequenceV1(5));
    assert_eq!(cursor.last_disposition, ReplayDispositionV1::Withheld);
    assert!(cursor.last_settlement_proof_sha256.is_none());

    // No delivery work and no payload were created for it.
    assert_eq!(store.inspect(&Default::default())?.total_rows, 1);
    assert!(store.lease_pending(&lease_request(T0, 8))?.len() == 1);

    // The refused event is never re-emitted, and the refusal says *why*: the
    // source position was withheld, not merely overtaken by a later one.
    let replayed = Builder::at_sequence(5).build()?;
    assert!(matches!(
        store.append_admitted(&replayed)?,
        AppendOutcomeV1::RejectedWithheldSource {
            source_sequence: SourceSequenceV1(5)
        }
    ));

    // Digests only: the refused payload bytes were never stored anywhere.
    let connection = rusqlite::Connection::open(directory.path().join("journal.sqlite3"))?;
    let stored: String = connection.query_row(
        "SELECT source_payload_sha256 FROM tdmem_observation_withheld_v2 WHERE source_sequence = 5",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(stored, withheld.source_payload_sha256);
    let columns: i64 = connection.query_row(
        "SELECT COUNT(*) FROM pragma_table_info('tdmem_observation_withheld_v2') \
         WHERE name LIKE '%payload_bytes%' OR name LIKE '%body%'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(columns, 0, "the withheld audit must never hold content");
    Ok(())
}

#[test]
fn an_invalid_retention_policy_is_rejected_at_open() -> TestResult {
    let directory = tempfile::tempdir()?;
    for mutate in [
        (|policy: &mut tracedecay_memory_observation::RetentionPolicyV1| policy.max_attempts = 0)
            as fn(&mut tracedecay_memory_observation::RetentionPolicyV1),
        |policy| policy.sweep_batch_rows = 0,
        |policy| policy.backoff_max_micros = 1,
        |policy| policy.max_queue_bytes = 0,
    ] {
        let mut invalid = policy();
        mutate(&mut invalid);
        assert!(matches!(
            tracedecay_memory_observation::SqliteObservationJournal::open(
                directory.path().join("invalid.sqlite3"),
                invalid,
            ),
            Err(ObservationJournalError::InvalidRetentionPolicy { .. })
        ));
    }
    Ok(())
}
