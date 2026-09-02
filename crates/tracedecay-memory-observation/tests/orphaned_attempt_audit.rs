//! Every attempt number a delivery row spends is accounted for by a durable
//! typed record.
//!
//! The lease claim consumes an attempt before any provider is contacted, and a
//! reaped lease never gives that number back. So a dispatcher that dies between
//! the claim and the answer used to leave a counter that says "an attempt
//! happened" and no record of what became of it — an audit hole exactly the
//! width of the crash window this milestone is about. The reaper now writes an
//! orphaned-attempt record for it, and these tests hold that closed:
//!
//! ```text
//! attempt_number == receipts + attempt refusals + orphaned attempts
//! ```

mod support;

use support::{
    Builder, LEASE, SECOND, T0, TestResult, applied_receipt, journal, lease_request, policy,
};

use tracedecay_memory_observation::{
    AttemptOrphanCauseV1, AttemptOrphanRecoveryV1, AttemptOutcomeV1, DeliveryStateV1,
    JournalInspectionFilterV1, ObservationDispatchPortV1, ObservationJournalReaderV1,
};

#[test]
fn a_lease_that_lapses_without_an_answer_leaves_one_orphaned_attempt() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let admitted = Builder::at_sequence(1).build()?;
    store.append_admitted(&admitted)?;

    let leased = store.lease_pending(&lease_request(T0, 4))?;
    let claim = leased.first().ok_or("nothing was leased")?;
    assert_eq!(claim.attempt_number, 1);

    // The dispatcher dies holding the lease. Nothing was recorded.
    let after_lapse = T0 + LEASE + SECOND;
    assert_eq!(store.reap_expired_leases(after_lapse, 8)?, 1);

    let orphans = store.attempt_orphans_for(&admitted.observation_id)?;
    assert_eq!(
        orphans.len(),
        1,
        "the reaped attempt left no durable explanation"
    );
    let orphan = orphans.first().ok_or("orphans were non-empty a line ago")?;
    assert_eq!(orphan.attempt_number, 1);
    assert_eq!(orphan.observation_id, admitted.observation_id);
    assert_eq!(orphan.idempotency_key, admitted.idempotency_key);
    // The record names the exact claim it came from and the exact content that
    // claim was going to deliver, so an operator can tie it to one lease and
    // one payload rather than to a row.
    assert_eq!(orphan.lease_id, claim.lease_id);
    assert_eq!(orphan.lease_owner, support::OWNER);
    assert_eq!(orphan.payload_sha256, admitted.payload.sha256);
    assert_eq!(orphan.exact_scope_sha256, claim.exact_scope_sha256);
    assert_eq!(
        orphan.provider_instance_id.as_deref(),
        Some(claim.target.provider_instance_id.as_str())
    );
    assert_eq!(
        orphan.cause,
        AttemptOrphanCauseV1::LeaseExpiredWithoutAnswer
    );
    assert_eq!(
        orphan.recovery,
        AttemptOrphanRecoveryV1::RedeliveryScheduled
    );
    assert_eq!(
        orphan.lease_expired_at_unix_micros,
        claim.lease_expires_at_unix_micros
    );
    assert_eq!(orphan.recorded_at_unix_micros, after_lapse);

    // No receipt and no refusal was invented: the record says an attempt was
    // spent, never that a provider effect happened.
    assert!(store.receipts_for(&admitted.observation_id)?.is_empty());
    assert!(
        store
            .attempt_refusals_for(&admitted.observation_id)?
            .is_empty()
    );
    Ok(())
}

#[test]
fn an_answered_attempt_and_a_released_lease_leave_no_orphan() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;

    // (1) An attempt the provider answered is already accounted for.
    let answered = Builder::at_sequence(1).build()?;
    store.append_admitted(&answered)?;
    let leased = store.lease_pending(&lease_request(T0, 4))?;
    let claim = leased.first().ok_or("nothing was leased")?;
    assert!(matches!(
        store.record_attempt(&applied_receipt(claim, T0))?,
        AttemptOutcomeV1::Recorded { .. }
    ));

    // (2) A dispatcher that hands the lease back rather than dying holds no
    // orphaned attempt either: the attempt is still spent, but the reaper never
    // reclaims a lease that is no longer held.
    let released = Builder::at_sequence(2).build()?;
    store.append_admitted(&released)?;
    let leased = store.lease_pending(&lease_request(T0, 4))?;
    let claim = leased.first().ok_or("nothing was leased")?;
    store.release_lease(&claim.lease_id, T0 + SECOND)?;

    assert_eq!(store.reap_expired_leases(T0 + LEASE + SECOND, 8)?, 0);
    assert!(
        store
            .attempt_orphans_for(&answered.observation_id)?
            .is_empty()
    );
    assert!(
        store
            .attempt_orphans_for(&released.observation_id)?
            .is_empty()
    );
    Ok(())
}

#[test]
fn the_attempt_that_exhausts_a_row_is_recorded_as_unrecoverable() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let admitted = Builder::at_sequence(1).build()?;
    store.append_admitted(&admitted)?;
    let max_attempts = policy().max_attempts;

    let mut now = T0;
    for attempt in 1..=max_attempts {
        let leased = store.lease_pending(&lease_request(now, 4))?;
        let claim = leased.first().ok_or("nothing was leased")?;
        assert_eq!(claim.attempt_number, attempt);
        now = now + LEASE + SECOND;
        assert_eq!(store.reap_expired_leases(now, 8)?, 1);
    }

    let orphans = store.attempt_orphans_for(&admitted.observation_id)?;
    assert_eq!(
        orphans
            .iter()
            .map(|orphan| orphan.attempt_number)
            .collect::<Vec<u32>>(),
        (1..=max_attempts).collect::<Vec<u32>>(),
        "every consumed attempt must carry its own record"
    );
    // The reaper distinguishes "this will be redelivered" from "there is
    // nothing left to redeliver with", instead of reporting one outcome for
    // both.
    for orphan in &orphans[..orphans.len() - 1] {
        assert_eq!(
            orphan.recovery,
            AttemptOrphanRecoveryV1::RedeliveryScheduled
        );
    }
    assert_eq!(
        orphans
            .last()
            .ok_or("orphans were non-empty a line ago")?
            .recovery,
        AttemptOrphanRecoveryV1::AttemptsExhausted
    );

    // And the store agrees: the next pass terminalizes the row rather than
    // leasing a fourth attempt nobody is allowed to make.
    assert!(store.lease_pending(&lease_request(now, 4))?.is_empty());
    let page = store.inspect(&JournalInspectionFilterV1 {
        limit: 8,
        ..JournalInspectionFilterV1::default()
    })?;
    let row = page.rows.first().ok_or("no delivery row")?;
    assert_eq!(row.state, DeliveryStateV1::Exhausted);
    Ok(())
}

#[test]
fn a_bounded_reap_only_accounts_for_the_leases_it_reclaimed() -> TestResult {
    // The reap budget bounds the audit exactly as it bounds the reclaim: a
    // round that reclaims two leases writes two records, never one for a lease
    // that is still held.
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let first = Builder::at_sequence(1).build()?;
    let second = Builder::at_sequence(2).build()?;
    let third = Builder::at_sequence(3).build()?;
    for admitted in [&first, &second, &third] {
        store.append_admitted(admitted)?;
    }
    assert_eq!(store.lease_pending(&lease_request(T0, 8))?.len(), 3);

    let after_lapse = T0 + LEASE + SECOND;
    assert_eq!(store.reap_expired_leases(after_lapse, 2)?, 2);
    let recorded: usize = [&first, &second, &third]
        .into_iter()
        .map(|admitted| {
            store
                .attempt_orphans_for(&admitted.observation_id)
                .map(|orphans| orphans.len())
        })
        .collect::<Result<Vec<usize>, _>>()?
        .into_iter()
        .sum();
    assert_eq!(
        recorded, 2,
        "the audit outran the reclaim, so a lease that is still held was declared orphaned"
    );

    assert_eq!(store.reap_expired_leases(after_lapse, 8)?, 1);
    for admitted in [&first, &second, &third] {
        assert_eq!(
            store.attempt_orphans_for(&admitted.observation_id)?.len(),
            1
        );
    }
    Ok(())
}
