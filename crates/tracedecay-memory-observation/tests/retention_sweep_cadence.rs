//! AC4: the retention rules are not only explicit, they are *driven*. The
//! sweeper is the component a delivery loop mounts to decide when the journal's
//! bounded sweep is due, so these tests exercise it against the real SQLite
//! journal rather than against a description of it.
#![allow(clippy::panic)]

mod support;

use support::{Builder, HOUR, MINUTE, SECOND, T0, TestResult, journal, lease_request, policy};

use tracedecay_memory_observation::{
    DeliveryStateV1, ForgetReceiptV1, ForgetSourceKeyV1, ForgetSourceRequestV1,
    ForgetVerificationV1, JournalInspectionFilterV1, ObservationDispatchPortV1,
    ObservationJournalError, ObservationJournalReaderV1, ObservationOutcomeV1,
    ObservationRetentionPortV1, ObservationRuntimeError, RetentionClassV1, RetentionPolicyV1,
    RetentionSweepReceiptV1, RetentionSweepScheduleV1, RetentionSweeperV1, RetentionTickV1,
    SqliteObservationJournal,
};

const INTERVAL: i64 = MINUTE;
const ERROR_BACKOFF: i64 = 5 * MINUTE;

fn schedule() -> Result<RetentionSweepScheduleV1, ObservationJournalError> {
    RetentionSweepScheduleV1::bounded(INTERVAL, ERROR_BACKOFF)
}

fn expired_by(now: i64, store: &SqliteObservationJournal, sequence: u64) -> TestResult {
    let admitted = Builder {
        retention_class: RetentionClassV1::Profile,
        // The admitted privacy expiry is the binding bound: one hour before
        // `now`, so the row is already aged when the sweeper first looks.
        expires_at: now - HOUR,
        ..Builder::at_sequence(sequence)
    }
    .build()?;
    store.append_admitted(&admitted)?;
    Ok(())
}

#[test]
fn the_first_tick_is_due_at_once_and_terminalizes_then_purges_expired_rows() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let now = T0 + 2 * HOUR;
    expired_by(now, &store, 1)?;

    let mut sweeper = RetentionSweeperV1::new(&store, schedule()?, now);
    assert_eq!(
        sweeper.next_due_unix_micros(),
        now,
        "first sweep is due at mount"
    );

    let RetentionTickV1::Swept {
        receipt,
        next_due_unix_micros,
    } = sweeper.tick(now)?
    else {
        panic!("a due sweep must run");
    };
    assert_eq!(receipt.deliveries_expired, 1);
    assert_eq!(receipt.payloads_purged, 1);
    assert_eq!(receipt.journal_rows_deleted, 0, "audit outlives content");
    assert_eq!(receipt.remaining_candidates, 0);
    assert_eq!(next_due_unix_micros, now + INTERVAL);

    // Terminalized with a typed receipt, content gone, and never leasable.
    let page = store.inspect(&JournalInspectionFilterV1::default())?;
    assert_eq!(page.total_rows, 1);
    assert_eq!(page.rows[0].state, DeliveryStateV1::Expired);
    assert!(!page.rows[0].content_present);
    assert!(page.rows[0].content_forgotten_at_unix_micros.is_some());
    let receipts = store.receipts_for(&page.rows[0].observation_id)?;
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].outcome, ObservationOutcomeV1::DeadlineExceeded);
    assert!(store.lease_pending(&lease_request(now, 8))?.is_empty());

    // Not due again before the interval elapses; due again once it has.
    assert_eq!(
        sweeper.tick(now + INTERVAL - SECOND)?,
        RetentionTickV1::NotDue {
            next_due_unix_micros: now + INTERVAL
        }
    );
    assert!(matches!(
        sweeper.tick(now + INTERVAL)?,
        RetentionTickV1::Swept { .. }
    ));
    Ok(())
}

#[test]
fn a_backlog_wider_than_one_batch_is_due_again_at_once_until_it_converges() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = SqliteObservationJournal::open(
        directory.path().join("journal.sqlite3"),
        RetentionPolicyV1 {
            sweep_batch_rows: 2,
            ..policy()
        },
    )?;
    let now = T0 + 2 * HOUR;
    for sequence in 1..=5 {
        expired_by(now, &store, sequence)?;
    }

    let mut sweeper = RetentionSweeperV1::new(&store, schedule()?, now);
    let mut purged = 0_u32;
    let mut passes = 0_u32;
    loop {
        passes += 1;
        assert!(passes <= 10, "sweep did not converge");
        let RetentionTickV1::Swept {
            receipt,
            next_due_unix_micros,
        } = sweeper.tick(now)?
        else {
            panic!("a due sweep must run on pass {passes}");
        };
        assert!(receipt.payloads_purged <= 2, "batch bound was not honoured");
        purged += receipt.payloads_purged;
        if receipt.remaining_candidates == 0 {
            assert_eq!(next_due_unix_micros, now + INTERVAL);
            break;
        }
        assert_eq!(
            next_due_unix_micros, now,
            "a backlog with progress is due on the next loop turn"
        );
    }
    assert_eq!(purged, 5);
    assert_eq!(passes, 3);
    assert!(store.lease_pending(&lease_request(now, 8))?.is_empty());
    Ok(())
}

#[test]
fn a_journal_with_nothing_expired_is_swept_at_the_interval_and_left_alone() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let admitted = Builder::at_sequence(1).build()?;
    store.append_admitted(&admitted)?;
    let now = T0 + MINUTE;

    let mut sweeper = RetentionSweeperV1::new(&store, schedule()?, now);
    let RetentionTickV1::Swept { receipt, .. } = sweeper.tick(now)? else {
        panic!("a due sweep must run");
    };
    assert_eq!(
        receipt,
        RetentionSweepReceiptV1 {
            wal_truncated: receipt.wal_truncated,
            ..RetentionSweepReceiptV1::default()
        }
    );
    let page = store.inspect(&JournalInspectionFilterV1::default())?;
    assert_eq!(page.rows[0].state, DeliveryStateV1::Pending);
    assert!(page.rows[0].content_present);
    assert_eq!(store.lease_pending(&lease_request(now, 8))?.len(), 1);
    Ok(())
}

/// A retention port whose sweep answers with whatever the test scripted, so
/// the cadence under failure and under non-actionable candidates can be pinned
/// without corrupting a real journal.
struct ScriptedRetentionPort {
    policy: RetentionPolicyV1,
    answer: Result<RetentionSweepReceiptV1, &'static str>,
}

impl ObservationRetentionPortV1 for ScriptedRetentionPort {
    fn retention_policy(&self) -> &RetentionPolicyV1 {
        &self.policy
    }

    fn sweep_expired(
        &self,
        _now_unix_micros: i64,
        budget: u32,
    ) -> Result<RetentionSweepReceiptV1, ObservationJournalError> {
        assert_eq!(
            budget, self.policy.sweep_batch_rows,
            "budget is the policy's batch"
        );
        self.answer
            .map_err(|field| ObservationJournalError::EmptyField { field })
    }

    fn forget_source(
        &self,
        _request: &ForgetSourceRequestV1,
    ) -> Result<ForgetReceiptV1, ObservationJournalError> {
        unreachable!("the sweeper never forgets")
    }

    fn verify_forgotten(
        &self,
        _key: &ForgetSourceKeyV1,
    ) -> Result<ForgetVerificationV1, ObservationJournalError> {
        unreachable!("the sweeper never verifies")
    }
}

#[test]
fn a_failed_sweep_is_returned_typed_and_is_not_retried_before_the_backoff() -> TestResult {
    let port = ScriptedRetentionPort {
        policy: policy(),
        answer: Err("sweep_failed"),
    };
    let now = T0;
    let mut sweeper = RetentionSweeperV1::new(&port, schedule()?, now);
    let error = sweeper.tick(now).err();
    assert!(
        matches!(
            error,
            Some(ObservationRuntimeError::Journal(
                ObservationJournalError::EmptyField {
                    field: "sweep_failed"
                }
            ))
        ),
        "scripted failure must surface typed, got {error:?}"
    );
    assert_eq!(sweeper.next_due_unix_micros(), now + ERROR_BACKOFF);
    assert_eq!(
        sweeper.tick(now + ERROR_BACKOFF - SECOND)?,
        RetentionTickV1::NotDue {
            next_due_unix_micros: now + ERROR_BACKOFF
        }
    );
    assert!(sweeper.tick(now + ERROR_BACKOFF).is_err());
    Ok(())
}

#[test]
fn remaining_candidates_without_progress_back_off_instead_of_running_hot() -> TestResult {
    let port = ScriptedRetentionPort {
        policy: policy(),
        answer: Ok(RetentionSweepReceiptV1 {
            remaining_candidates: 7,
            wal_truncated: true,
            ..RetentionSweepReceiptV1::default()
        }),
    };
    let now = T0;
    let mut sweeper = RetentionSweeperV1::new(&port, schedule()?, now);
    let RetentionTickV1::Swept {
        next_due_unix_micros,
        ..
    } = sweeper.tick(now)?
    else {
        panic!("a due sweep must run");
    };
    assert_eq!(next_due_unix_micros, now + ERROR_BACKOFF);
    assert_eq!(
        sweeper.tick(now + SECOND)?,
        RetentionTickV1::NotDue {
            next_due_unix_micros: now + ERROR_BACKOFF
        }
    );
    Ok(())
}

#[test]
fn a_schedule_that_cannot_bound_the_cadence_is_refused() -> TestResult {
    for ((interval, backoff), field) in [
        ((0, MINUTE), "interval_micros"),
        ((MINUTE, -1), "error_backoff_micros"),
    ] {
        let refused = RetentionSweepScheduleV1::bounded(interval, backoff).err();
        assert!(
            matches!(
                refused,
                Some(ObservationJournalError::InvalidSweepSchedule { field: refused_field })
                    if refused_field == field
            ),
            "{field}: {refused:?}"
        );
    }
    let bounded = schedule()?;
    assert_eq!(bounded.interval_micros(), INTERVAL);
    assert_eq!(bounded.error_backoff_micros(), ERROR_BACKOFF);
    Ok(())
}
