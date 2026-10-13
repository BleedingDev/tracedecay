//! AC3: delivery state survives restart.
//!
//! Every test here uses a real file, drops the whole store to close the
//! connection, and opens a brand-new one on the same path. Nothing is carried
//! across the boundary in memory.

mod support;

use support::{
    Builder, LEASE, SECOND, T0, TestResult, applied_receipt, journal, lease_request, policy,
    stream_key, unavailable_receipt,
};

use tracedecay_memory_observation::{
    AppendOutcomeV1, DeliveryStateV1, ObservationDispatchPortV1, ObservationJournalReaderV1,
    ObservationOutcomeV1, SourceSequenceV1, SqliteObservationJournal,
};

#[test]
fn delivery_state_attempts_and_replay_position_survive_reopen() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("observation-journal.sqlite3");

    let applied_source = Builder::at_sequence(7).build()?;
    let retryable_source = Builder::at_sequence(8).build()?;
    let inflight_source = Builder::at_sequence(9).build()?;

    // ---- phase 1: a live dispatcher ----
    {
        let store = journal(&path)?;
        for admitted in [&applied_source, &retryable_source, &inflight_source] {
            assert!(matches!(
                store.append_admitted(admitted)?,
                AppendOutcomeV1::Appended { .. }
            ));
        }
        let leased = store.lease_pending(&lease_request(T0, 3))?;
        assert_eq!(leased.len(), 3);
        assert_eq!(leased[0].source_sequence, SourceSequenceV1(7));

        store.record_attempt(&applied_receipt(&leased[0], T0))?;
        store.record_attempt(&unavailable_receipt(&leased[1], T0))?;
        // leased[2] is deliberately left leased: the dispatcher "crashed".
    }

    // ---- phase 2: restart ----
    let store = journal(&path)?;

    // (a) the ingress replay position survived
    let cursor = store
        .replay_cursor(&stream_key("session-1")?)?
        .ok_or("replay cursor missing after restart")?;
    assert_eq!(cursor.last_admitted_sequence, SourceSequenceV1(9));

    // (b) the acknowledged item is terminal and is never re-delivered
    let ready = store.lease_pending(&lease_request(T0 + SECOND, 10))?;
    assert!(
        !ready
            .iter()
            .any(|item| item.observation_id == applied_source.observation_id)
    );

    // (c) the retryable item honours its persisted backoff
    assert!(
        !ready
            .iter()
            .any(|item| item.observation_id == retryable_source.observation_id)
    );
    let backoff = policy().next_attempt_delay(1);
    let later = store.lease_pending(&lease_request(T0 + backoff + 2 * SECOND, 10))?;
    assert!(
        later
            .iter()
            .any(|item| item.observation_id == retryable_source.observation_id)
    );

    // (d) the crashed lease is not re-leasable before expiry and is after
    assert!(
        !later
            .iter()
            .any(|item| item.observation_id == inflight_source.observation_id)
    );
    assert_eq!(store.reap_expired_leases(T0 + LEASE + SECOND, 64)?, 1);
    let after_reap = store.lease_pending(&lease_request(T0 + LEASE + 2 * SECOND, 10))?;
    assert!(
        after_reap
            .iter()
            .any(|item| item.observation_id == inflight_source.observation_id)
    );

    // (e) immutable receipts survived with their attempt numbers
    let receipts = store.receipts_for(&applied_source.observation_id)?;
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].attempt_number, 1);
    assert_eq!(receipts[0].outcome, ObservationOutcomeV1::Applied);

    // (f) idempotency is stable across the restart
    assert!(matches!(
        store.append_admitted(&applied_source)?,
        AppendOutcomeV1::DuplicateIdempotencyKey {
            state: DeliveryStateV1::Acknowledged,
            ..
        }
    ));
    let page = store.inspect(&Default::default())?;
    assert_eq!(page.total_rows, 3);
    Ok(())
}

#[test]
fn crash_between_canonical_commit_and_append_is_recovered_from_the_cursor() -> TestResult {
    // The executable proof of AC1's causal binding: the canonical authority is
    // ahead of the journal after a crash, so it re-emits from the cursor and
    // the content-derived key makes re-emission a no-op instead of a duplicate.
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("observation-journal.sqlite3");
    {
        let store = journal(&path)?;
        for sequence in 7..=9 {
            store.append_admitted(&Builder::at_sequence(sequence).build()?)?;
        }
    }

    let store = journal(&path)?;
    let cursor = store
        .replay_cursor(&stream_key("session-1")?)?
        .ok_or("replay cursor missing")?;
    assert_eq!(cursor.last_admitted_sequence, SourceSequenceV1(9));

    // The authority re-emits from its own durable position, which overlaps.
    let mut outcomes = Vec::new();
    for sequence in 8..=10 {
        outcomes.push(store.append_admitted(&Builder::at_sequence(sequence).build()?)?);
    }
    assert!(matches!(
        outcomes[0],
        AppendOutcomeV1::DuplicateIdempotencyKey { .. }
    ));
    assert!(matches!(
        outcomes[1],
        AppendOutcomeV1::DuplicateIdempotencyKey { .. }
    ));
    assert!(matches!(outcomes[2], AppendOutcomeV1::Appended { .. }));

    assert_eq!(store.inspect(&Default::default())?.total_rows, 4);
    assert_eq!(
        store
            .replay_cursor(&stream_key("session-1")?)?
            .ok_or("replay cursor missing")?
            .last_admitted_sequence,
        SourceSequenceV1(10)
    );
    Ok(())
}

#[test]
fn a_corrupted_row_is_an_error_not_a_wrong_delivery() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("observation-journal.sqlite3");
    {
        let store = journal(&path)?;
        store.append_admitted(&Builder::at_sequence(1).build()?)?;
    }
    {
        let connection = rusqlite::Connection::open(&path)?;
        connection.execute(
            "UPDATE tdmem_observation_journal_v1 \
             SET settlement_receipt_json = json_set(settlement_receipt_json, \
                 '$.settlement_proof_sha256', '')",
            [],
        )?;
    }
    let store = SqliteObservationJournal::open(&path, policy())?;
    let error = store
        .lease_pending(&lease_request(T0, 4))
        .err()
        .ok_or("a corrupt row was delivered")?;
    assert!(matches!(
        error,
        tracedecay_memory_observation::ObservationJournalError::Corrupt {
            field: "settlement_receipt_json",
            ..
        }
    ));
    Ok(())
}
