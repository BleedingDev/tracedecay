//! AC4: retention and privacy deletion rules are explicit, bounded, and have a
//! verifiable postcondition.

mod support;

use support::{
    Builder, DAY, HOUR, SECOND, T0, TestResult, applied_receipt, journal, lease_request, policy,
};

use tracedecay_memory_observation::{
    DeliveryStateV1, ForgetSourceKeyV1, ForgetSourceRequestV1, ObservationDispatchPortV1,
    ObservationJournalReaderV1, ObservationOutcomeV1, ObservationRetentionPortV1, RetentionClassV1,
    SqliteObservationJournal,
};

#[test]
fn retention_class_drives_the_effective_expiry() -> TestResult {
    for (class, age) in [
        (
            RetentionClassV1::Ephemeral,
            policy().ephemeral_max_age_micros,
        ),
        (RetentionClassV1::Session, policy().session_max_age_micros),
        (RetentionClassV1::Project, policy().project_max_age_micros),
    ] {
        let directory = tempfile::tempdir()?;
        let store = journal(&directory.path().join("journal.sqlite3"))?;
        let admitted = Builder {
            retention_class: class,
            // Far-future privacy expiry, so the class age is what binds.
            expires_at: T0 + 3650 * DAY,
            ..Builder::at_sequence(1)
        }
        .build()?;
        store.append_admitted(&admitted)?;

        let before = store.sweep_expired(T0 + age - SECOND, 64)?;
        assert_eq!(before.payloads_purged, 0, "{class:?} purged too early");

        let after = store.sweep_expired(T0 + age + SECOND, 64)?;
        assert_eq!(after.payloads_purged, 1, "{class:?} did not purge on time");
        assert_eq!(after.deliveries_expired, 1);
    }
    Ok(())
}

#[test]
fn the_admitted_privacy_expiry_wins_when_it_is_the_earlier_bound() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let admitted = Builder {
        retention_class: RetentionClassV1::Profile,
        expires_at: T0 + HOUR,
        ..Builder::at_sequence(1)
    }
    .build()?;
    store.append_admitted(&admitted)?;
    assert_eq!(
        store.sweep_expired(T0 + HOUR - SECOND, 64)?.payloads_purged,
        0
    );
    assert_eq!(
        store.sweep_expired(T0 + HOUR + SECOND, 64)?.payloads_purged,
        1
    );
    Ok(())
}

#[test]
fn a_provider_cannot_extend_expiry() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let admitted = Builder {
        retention_class: RetentionClassV1::Ephemeral,
        expires_at: T0 + 3650 * DAY,
        ..Builder::at_sequence(1)
    }
    .build()?;
    store.append_admitted(&admitted)?;

    // An acknowledgement carrying a much later generation and finish time does
    // not move the row's expiry: no receipt column feeds the expiry expression.
    let leased = store.lease_pending(&lease_request(T0, 4))?;
    let mut receipt = applied_receipt(&leased[0], T0);
    receipt.state_generation_after = Some(u64::MAX >> 1);
    receipt.finished_at_unix_micros = T0 + 3650 * DAY;
    store.record_attempt(&receipt)?;

    let sweep = store.sweep_expired(T0 + policy().ephemeral_max_age_micros + SECOND, 64)?;
    assert_eq!(sweep.payloads_purged, 1);
    Ok(())
}

#[test]
fn sweep_expires_undelivered_rows_before_purging_them() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let admitted = Builder {
        retention_class: RetentionClassV1::Ephemeral,
        ..Builder::at_sequence(1)
    }
    .build()?;
    store.append_admitted(&admitted)?;

    let sweep = store.sweep_expired(T0 + policy().ephemeral_max_age_micros + SECOND, 64)?;
    assert_eq!(sweep.deliveries_expired, 1);
    assert_eq!(sweep.payloads_purged, 1);
    assert_eq!(sweep.journal_rows_deleted, 0, "audit must outlive content");

    let page = store.inspect(&Default::default())?;
    assert_eq!(page.total_rows, 1);
    assert_eq!(page.rows[0].state, DeliveryStateV1::Expired);
    assert!(!page.rows[0].content_present);
    assert!(page.rows[0].content_forgotten_at_unix_micros.is_some());

    let receipts = store.receipts_for(&admitted.observation_id)?;
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].outcome, ObservationOutcomeV1::DeadlineExceeded);
    Ok(())
}

#[test]
fn a_fully_aged_row_is_deleted_only_after_its_audit_window_closes() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let admitted = Builder {
        retention_class: RetentionClassV1::Ephemeral,
        ..Builder::at_sequence(1)
    }
    .build()?;
    store.append_admitted(&admitted)?;
    let purge_at = T0 + policy().ephemeral_max_age_micros + SECOND;
    store.sweep_expired(purge_at, 64)?;

    assert_eq!(
        store
            .sweep_expired(purge_at + policy().receipt_retention_micros - SECOND, 64)?
            .journal_rows_deleted,
        0
    );
    let final_sweep =
        store.sweep_expired(purge_at + policy().receipt_retention_micros + SECOND, 64)?;
    assert_eq!(final_sweep.journal_rows_deleted, 1);
    assert_eq!(final_sweep.receipts_deleted, 1);
    assert_eq!(final_sweep.remaining_candidates, 0);
    assert_eq!(store.inspect(&Default::default())?.total_rows, 0);
    Ok(())
}

#[test]
fn sweep_is_bounded_and_converges() -> TestResult {
    let mut bounded = policy();
    bounded.sweep_batch_rows = 4;
    let directory = tempfile::tempdir()?;
    let store = SqliteObservationJournal::open(directory.path().join("journal.sqlite3"), bounded)?;
    for sequence in 1..=10 {
        store.append_admitted(
            &Builder {
                retention_class: RetentionClassV1::Ephemeral,
                ..Builder::at_sequence(sequence)
            }
            .build()?,
        )?;
    }
    let now = T0 + bounded.ephemeral_max_age_micros + SECOND;
    let first = store.sweep_expired(now, 64)?;
    assert_eq!(first.payloads_purged, 4);
    assert!(first.remaining_candidates > 0);

    let mut guard = 0;
    let mut receipt = first;
    while receipt.remaining_candidates > 0 && guard < 20 {
        receipt = store.sweep_expired(now, 64)?;
        guard += 1;
    }
    assert_eq!(receipt.remaining_candidates, 0, "sweep did not converge");
    Ok(())
}

#[test]
fn forget_source_zeroes_content_and_keeps_the_digest_only_audit() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    let store = journal(&path)?;
    let forgotten = Builder {
        forget_source_key: "forget:doomed".to_owned(),
        ..Builder::at_sequence(1)
    }
    .build()?;
    let sibling = Builder {
        forget_source_key: "forget:kept".to_owned(),
        ..Builder::at_sequence(2)
    }
    .build()?;
    store.append_admitted(&forgotten)?;
    store.append_admitted(&sibling)?;

    // Record one attempt first, so the surviving audit trail is real.
    let leased = store.lease_pending(&lease_request(T0, 1))?;
    store.record_attempt(&applied_receipt(&leased[0], T0))?;

    let key = ForgetSourceKeyV1::new("forget:doomed")?;
    let receipt = store.forget_source(&ForgetSourceRequestV1 {
        forget_source_key: key.clone(),
        reason: "subject_erasure_request".to_owned(),
        requested_at_unix_micros: T0 + SECOND,
    })?;
    assert_eq!(receipt.journal_rows_matched, 1);
    assert_eq!(receipt.payloads_zeroed, 1);
    assert_eq!(receipt.receipts_retained, 1);

    let verification = store.verify_forgotten(&key)?;
    assert!(verification.verified);
    assert_eq!(verification.rows_with_content_remaining, 0);
    assert_eq!(verification.undelivered_remaining, 0);

    // The sibling key is untouched and honestly reports itself unforgotten.
    let sibling_key = ForgetSourceKeyV1::new("forget:kept")?;
    let sibling_verification = store.verify_forgotten(&sibling_key)?;
    assert!(!sibling_verification.verified);
    assert_eq!(sibling_verification.rows_with_content_remaining, 1);

    // Raw SQL: no content bytes remain for the forgotten key.
    let connection = rusqlite::Connection::open(&path)?;
    let remaining: i64 = connection.query_row(
        "SELECT COUNT(*) FROM tdmem_observation_journal_v1 \
         WHERE forget_source_key = 'forget:doomed' AND payload_bytes IS NOT NULL",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(remaining, 0);
    let digests: i64 = connection.query_row(
        "SELECT COUNT(*) FROM tdmem_observation_receipt_v1",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(digests, 1, "the digest-only audit trail must survive");
    Ok(())
}

#[test]
fn forgotten_content_is_never_leased_and_survives_reopen() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    let key = ForgetSourceKeyV1::new("forget:session-1")?;
    {
        let store = journal(&path)?;
        store.append_admitted(&Builder::at_sequence(1).build()?)?;
        store.forget_source(&ForgetSourceRequestV1 {
            forget_source_key: key.clone(),
            reason: "retention_policy".to_owned(),
            requested_at_unix_micros: T0 + SECOND,
        })?;
    }
    let store = journal(&path)?;
    assert!(
        store
            .lease_pending(&lease_request(T0 + 2 * SECOND, 8))?
            .is_empty()
    );
    assert!(store.verify_forgotten(&key)?.verified);
    assert_eq!(
        store.inspect(&Default::default())?.rows[0].state,
        DeliveryStateV1::Forgotten
    );
    Ok(())
}

#[test]
fn the_store_persists_in_write_ahead_logging_mode() -> TestResult {
    // WAL is recorded in the database header, so a fresh connection observes
    // what the store chose. It is the durability half of "survives restart";
    // `secure_delete` is connection-scoped and is set on every connection the
    // store opens, so purged pages are zeroed rather than merely unlinked.
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    drop(journal(&path)?);
    let connection = rusqlite::Connection::open(&path)?;
    let journal_mode: String = connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    assert_eq!(journal_mode.to_lowercase(), "wal");
    Ok(())
}
