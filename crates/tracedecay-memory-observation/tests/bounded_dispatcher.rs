//! The bounded dispatcher: drain a journalled backlog under explicit round and
//! wall bounds, reschedule what produced no terminal on the policy's own curve,
//! record every terminal attempt exactly once, and never deliver a settled row
//! a second time.
//!
//! These tests exist because the two things a dispatcher gets wrong are
//! invisible in a single round: a backlog that only moves one batch per wake
//! (the wake edge is one collapsed boolean, and nothing raises it again for
//! rows that are already durable), and a retry that comes back at a flat
//! interval no matter how many attempts already failed.

mod support;

use std::cell::{Cell, RefCell};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use support::{
    Builder, LEASE, MINUTE, PROVENANCE_DIGEST, PROVIDER_RECEIPT_DIGEST, SECOND, T0, TestResult,
    journal, lease_request, policy,
};

use tracedecay_memory_observation::{
    AttemptRefusalCategoryV1, DeliveryAttemptV1, DeliveryControlV1, DeliveryRuntimeV1,
    DeliveryStateV1, DeliveryWakeV1, DispatchPolicyV1, DispatchRequestV1, DrainStopV1,
    JournalInspectionFilterV1, JournalInspectionRowV1, LeasedObservationV1,
    ObservationDispatchPortV1, ObservationJournalError, ObservationJournalReaderV1,
    ProviderDeliveryAdapterV1, RetentionPolicyV1, RetryBackoffV1, SourceSequenceV1,
    SqliteObservationJournal,
};
use tracedecay_memory_provider_api::contract::TerminalCode;
use tracedecay_memory_provider_api::{
    CommittedEffectEvidence, FallbackDirective, ProviderOperation, TerminalRecord,
};

const ATTEMPT_BUDGET: i64 = 5 * SECOND;
const BATCH: u32 = 8;

// ---------------------------------------------------------------- adapters --

#[derive(Debug)]
struct AdapterError(String);

impl Display for AdapterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for AdapterError {}

/// What one delivered row looked like from the provider's side.
#[derive(Clone, Debug, Eq, PartialEq)]
struct DeliveredV1 {
    idempotency_key: String,
    attempt_number: u32,
}

/// A provider that always applies, and remembers every key it saw.
struct ApplyingProvider {
    seen: RefCell<Vec<DeliveredV1>>,
    now: Cell<i64>,
    /// Optional shutdown to request from inside the first call, so a test can
    /// cancel a drain the way a daemon does: mid-flight, not between rounds.
    shutdown_after_first: Option<&'static DeliveryWakeV1>,
}

impl ApplyingProvider {
    fn new(now: i64) -> Self {
        Self {
            seen: RefCell::new(Vec::new()),
            now: Cell::new(now),
            shutdown_after_first: None,
        }
    }
}

impl ProviderDeliveryAdapterV1 for ApplyingProvider {
    type Error = AdapterError;

    fn deliver(
        &self,
        leased: &LeasedObservationV1,
        _control: &DeliveryControlV1,
    ) -> Result<DeliveryAttemptV1, Self::Error> {
        self.seen.borrow_mut().push(DeliveredV1 {
            idempotency_key: leased.idempotency_key.as_str().to_owned(),
            attempt_number: leased.attempt_number,
        });
        let started = self.now.get();
        let terminal = observe_success(leased).map_err(|error| AdapterError(error.to_string()))?;
        if let Some(wake) = self.shutdown_after_first {
            wake.request_shutdown();
        }
        Ok(DeliveryAttemptV1::Answered {
            terminal: Box::new(terminal),
            started_at_unix_micros: started,
            finished_at_unix_micros: started.saturating_add(1_000),
        })
    }
}

/// A provider whose transport never reaches the provider at all, so no attempt
/// ever produces a terminal and every lease comes back on the retry curve.
struct UnreachableProvider {
    attempts: Cell<u32>,
}

impl ProviderDeliveryAdapterV1 for UnreachableProvider {
    type Error = AdapterError;

    fn deliver(
        &self,
        leased: &LeasedObservationV1,
        _control: &DeliveryControlV1,
    ) -> Result<DeliveryAttemptV1, Self::Error> {
        self.attempts.set(self.attempts.get().saturating_add(1));
        Err(AdapterError(format!(
            "transport never reached the provider on attempt {}",
            leased.attempt_number
        )))
    }
}

fn observe_success(leased: &LeasedObservationV1) -> Result<TerminalRecord, Box<dyn Error>> {
    Ok(TerminalRecord::new(
        ProviderOperation::Observe,
        leased.target.provider_id.clone(),
        TerminalCode::Success,
        CommittedEffectEvidence::committed(
            1,
            2,
            Vec::new(),
            PROVIDER_RECEIPT_DIGEST,
            PROVENANCE_DIGEST,
        )?,
        FallbackDirective::forbidden(),
        format!("observe-{}", leased.observation_id.as_str()),
        leased.exact_scope_sha256.clone(),
        None,
    )?)
}

// ----------------------------------------------------------------- helpers --

fn dispatch_policy() -> DispatchPolicyV1 {
    DispatchPolicyV1 {
        lease_duration_micros: LEASE,
        batch_max_items: BATCH,
        batch_max_bytes: 1_048_576,
        attempt_budget_micros: ATTEMPT_BUDGET,
        reap_budget: 16,
        max_rounds_per_drain: 8,
        drain_budget_micros: 30 * SECOND,
    }
}

fn dispatch_at(now: i64) -> DispatchRequestV1 {
    DispatchRequestV1 {
        lease: lease_request(now, BATCH),
        retry_backoff: RetryBackoffV1::of(&policy()),
        attempt_budget_micros: ATTEMPT_BUDGET,
    }
}

fn seed(store: &SqliteObservationJournal, rows: u64) -> TestResult {
    for sequence in 1..=rows {
        store.append_admitted(&Builder::at_sequence(sequence).build()?)?;
    }
    Ok(())
}

fn rows(store: &SqliteObservationJournal) -> Result<Vec<JournalInspectionRowV1>, Box<dyn Error>> {
    Ok(store
        .inspect(&JournalInspectionFilterV1 {
            limit: 200,
            ..JournalInspectionFilterV1::default()
        })?
        .rows)
}

fn row_at(
    store: &SqliteObservationJournal,
    sequence: u64,
) -> Result<JournalInspectionRowV1, Box<dyn Error>> {
    rows(store)?
        .into_iter()
        .find(|row| row.source_sequence == SourceSequenceV1(sequence))
        .ok_or_else(|| format!("no delivery row at sequence {sequence}").into())
}

/// A caller clock that stands still unless a test advances it, so a drain's
/// per-round instants are observable rather than wall-clock noise.
struct FrozenClock(Cell<i64>);

impl FrozenClock {
    const fn new(now: i64) -> Self {
        Self(Cell::new(now))
    }

    fn now(&self) -> i64 {
        self.0.get()
    }

    fn advance(&self, by: i64) {
        self.0.set(self.0.get().saturating_add(by));
    }
}

// ------------------------------------------------------------------- tests --

/// The defect this catches: a worker that dispatches one batch per wake leaves
/// everything past `batch_max_items` sitting in a durable journal that nothing
/// will signal about again, so a restart backlog drains one batch per park
/// interval instead of converging.
#[test]
fn a_backlog_larger_than_one_batch_drains_in_one_turn() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    seed(&store, 20)?;
    let wake = DeliveryWakeV1::new();
    let provider = ApplyingProvider::new(T0);
    let delivery = DeliveryRuntimeV1::new(&store, &provider, &wake);
    let clock = FrozenClock::new(T0);

    // One round is exactly one batch: that is the stall.
    let single = delivery.dispatch_batch(&dispatch_at(clock.now()))?;
    assert_eq!(single.leased, BATCH);
    assert_eq!(single.settled_terminal, BATCH);

    // The drain keeps going while full batches keep coming.
    let report = delivery.drain(
        &dispatch_at(clock.now()),
        &dispatch_policy().drain_bounds(&policy(), clock.now())?,
        || clock.now(),
    )?;
    assert_eq!(
        report.rounds, 3,
        "8 + 4 rows, then the empty round that proves the journal is drained"
    );
    assert_eq!(report.totals.leased, 12);
    assert_eq!(report.totals.receipts_recorded, 12);
    assert_eq!(report.totals.settled_terminal, 12);
    assert_eq!(report.stop, DrainStopV1::Quiesced);
    assert!(!report.more_work_pending());

    for row in rows(&store)? {
        assert_eq!(
            row.state,
            DeliveryStateV1::Acknowledged,
            "row {:?} was left behind by the drain",
            row.source_sequence
        );
    }
    assert_eq!(provider.seen.borrow().len(), 20);
    Ok(())
}

/// Every terminal attempt is recorded exactly once, and a settled row is never
/// handed to the provider again — including by a later drain that finds the
/// journal already quiesced.
#[test]
fn every_terminal_attempt_is_recorded_once_and_never_redelivered() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    seed(&store, 12)?;
    let wake = DeliveryWakeV1::new();
    let provider = ApplyingProvider::new(T0);
    let delivery = DeliveryRuntimeV1::new(&store, &provider, &wake);

    let first = delivery.drain(
        &dispatch_at(T0),
        &dispatch_policy().drain_bounds(&policy(), T0)?,
        || T0,
    )?;
    assert_eq!(first.totals.receipts_recorded, 12);
    for sequence in 1..=12 {
        let row = row_at(&store, sequence)?;
        assert_eq!(row.state, DeliveryStateV1::Acknowledged);
        assert_eq!(
            store.receipts_for(&row.observation_id)?.len(),
            1,
            "sequence {sequence} recorded more than one receipt for one attempt"
        );
    }

    // A second drain over a settled journal must do nothing at all: a settled
    // row that got delivered again would be a duplicated provider effect.
    let delivered_before = provider.seen.borrow().len();
    let second = delivery.drain(
        &dispatch_at(T0 + SECOND),
        &dispatch_policy().drain_bounds(&policy(), T0 + SECOND)?,
        || T0 + SECOND,
    )?;
    assert_eq!(second.rounds, 1);
    assert_eq!(second.totals.leased, 0);
    assert_eq!(second.stop, DrainStopV1::Quiesced);
    assert_eq!(provider.seen.borrow().len(), delivered_before);
    Ok(())
}

/// The defect this catches: an attempt that produced no terminal at all was
/// rescheduled at a flat interval, so an unreachable provider was retried every
/// `backoff_base` until its attempt ceiling was consumed — while a provider
/// that politely answered `provider_unavailable` backed off exponentially. Both
/// must ride the same curve.
#[test]
fn an_attempt_that_produced_no_terminal_backs_off_on_the_journal_curve() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    store.append_admitted(&Builder::at_sequence(1).build()?)?;
    let wake = DeliveryWakeV1::new();
    let provider = UnreachableProvider {
        attempts: Cell::new(0),
    };
    let delivery = DeliveryRuntimeV1::new(&store, &provider, &wake);
    let retention = policy();

    let mut now = T0;
    for attempt in 1..=2_u32 {
        let report = delivery.dispatch_batch(&dispatch_at(now))?;
        assert_eq!(report.leased, 1, "attempt {attempt} never leased its row");
        assert_eq!(report.failures.len(), 1);
        assert_eq!(report.receipts_recorded, 0);

        let row = row_at(&store, 1)?;
        assert_eq!(row.state, DeliveryStateV1::Pending);
        let expected = now.saturating_add(retention.next_attempt_delay(attempt));
        assert_eq!(
            row.next_attempt_at_unix_micros, expected,
            "attempt {attempt} was not rescheduled on the journal's own curve"
        );
        // The curve is exponential, so attempt 2 waits strictly longer than 1.
        assert_eq!(
            retention.next_attempt_delay(attempt),
            retention.backoff_base_micros * i64::from(1_u32 << (attempt - 1))
        );
        now = expected;
    }
    assert_eq!(provider.attempts.get(), 2);
    Ok(())
}

/// The round bound is real: a drain over a backlog bigger than
/// `max_rounds_per_drain * batch` hands the loop back and says so, instead of
/// running until the journal is empty and starving reaping, retention, and
/// shutdown.
#[test]
fn a_drain_stops_at_its_round_bound_and_reports_work_still_pending() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    seed(&store, 30)?;
    let wake = DeliveryWakeV1::new();
    let provider = ApplyingProvider::new(T0);
    let delivery = DeliveryRuntimeV1::new(&store, &provider, &wake);
    let policy = DispatchPolicyV1 {
        max_rounds_per_drain: 2,
        ..dispatch_policy()
    };
    policy.validate_against(&support::policy())?;

    let report = delivery.drain(
        &dispatch_at(T0),
        &policy.drain_bounds(&support::policy(), T0)?,
        || T0,
    )?;
    assert_eq!(report.rounds, 2);
    assert_eq!(report.totals.leased, 2 * BATCH);
    assert_eq!(report.stop, DrainStopV1::RoundBudgetReached);
    assert!(report.more_work_pending());
    assert_eq!(provider.seen.borrow().len(), usize::try_from(2 * BATCH)?);
    assert_eq!(row_at(&store, 30)?.state, DeliveryStateV1::Pending);
    Ok(())
}

/// The wall bound is real too, and it is checked between rounds against the
/// caller's own clock — not against the instant the drain started.
#[test]
fn a_drain_stops_between_rounds_once_its_wall_budget_elapsed() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    seed(&store, 30)?;
    let wake = DeliveryWakeV1::new();
    let provider = ApplyingProvider::new(T0);
    let delivery = DeliveryRuntimeV1::new(&store, &provider, &wake);
    let policy = DispatchPolicyV1 {
        drain_budget_micros: 10 * SECOND,
        ..dispatch_policy()
    };
    let clock = FrozenClock::new(T0);
    let bounds = policy.drain_bounds(&support::policy(), clock.now())?;

    // Each round costs six seconds of the ten-second budget, so the second
    // round starts and the third does not.
    let report = delivery.drain(&dispatch_at(clock.now()), &bounds, || {
        let now = clock.now();
        clock.advance(6 * SECOND);
        now
    })?;
    assert_eq!(report.rounds, 2);
    assert_eq!(report.stop, DrainStopV1::BudgetElapsed);
    assert!(report.more_work_pending());
    assert_eq!(report.totals.settled_terminal, 2 * BATCH);
    assert_eq!(row_at(&store, 30)?.state, DeliveryStateV1::Pending);
    Ok(())
}

/// Shutdown requested from inside an attempt stops the drain at the end of that
/// round: the in-flight attempt keeps its own control, no further round starts,
/// and everything not yet delivered stays `Pending` with no receipt.
#[test]
fn a_drain_starts_no_round_after_shutdown_and_strands_nothing() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    seed(&store, 24)?;
    let wake: &'static DeliveryWakeV1 = Box::leak(Box::new(DeliveryWakeV1::new()));
    let provider = ApplyingProvider {
        seen: RefCell::new(Vec::new()),
        now: Cell::new(T0),
        shutdown_after_first: Some(wake),
    };
    let delivery = DeliveryRuntimeV1::new(&store, &provider, wake);

    let report = delivery.drain(
        &dispatch_at(T0),
        &dispatch_policy().drain_bounds(&policy(), T0)?,
        || T0,
    )?;
    assert_eq!(report.rounds, 1, "no round may start after shutdown");
    assert_eq!(report.stop, DrainStopV1::ShutdownRequested);
    assert_eq!(
        report.totals.leased, BATCH,
        "the interrupted round still leased its batch"
    );
    assert_eq!(
        report.totals.receipts_recorded, 1,
        "only the attempt already in flight may produce a receipt"
    );
    assert_eq!(report.totals.cancelled_before_dispatch, BATCH - 1);
    assert_eq!(report.totals.leases_released, BATCH - 1);

    // Rows the round never attempted carry no receipt and are eligible again.
    let mut pending = 0_u32;
    for row in rows(&store)? {
        if row.state == DeliveryStateV1::Pending {
            pending = pending.saturating_add(1);
            assert_eq!(store.receipts_for(&row.observation_id)?.len(), 0);
        }
    }
    assert_eq!(pending, 23);
    assert_eq!(store.lease_pending(&lease_request(T0, 64))?.len(), 23);
    Ok(())
}

/// A drain with no round bound is refused before it leases anything, and there
/// is no way to hand the runtime one at all.
///
/// The defect this catches: `DrainBoundsV1` used to expose both fields, so any
/// call site could pass `u32::MAX` rounds and an `i64::MAX` deadline past the
/// policy the journal validated. The only constructor is now
/// `DispatchPolicyV1::drain_bounds`, which revalidates against the retention
/// policy first, so a bound that bounds nothing never becomes a `DrainBoundsV1`
/// and never reaches a lease.
#[test]
fn a_drain_without_a_round_bound_is_refused_before_leasing() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    seed(&store, 2)?;
    let wake = DeliveryWakeV1::new();
    let provider = ApplyingProvider::new(T0);
    let delivery = DeliveryRuntimeV1::new(&store, &provider, &wake);

    // A bounded policy produces bounds and drains.
    let bounded = DispatchPolicyV1 {
        max_rounds_per_drain: 1,
        ..dispatch_policy()
    };
    let report = delivery.drain(
        &dispatch_at(T0),
        &bounded.drain_bounds(&policy(), T0)?,
        || T0,
    )?;
    assert_eq!(report.rounds, 1);

    // An unbounded one produces no bounds at all, so nothing can be drained
    // under it, and the store is untouched by the refusal.
    let unbounded = [
        DispatchPolicyV1 {
            max_rounds_per_drain: 0,
            ..dispatch_policy()
        },
        DispatchPolicyV1 {
            max_rounds_per_drain: u32::MAX,
            ..dispatch_policy()
        },
    ];
    for candidate in unbounded {
        match candidate.drain_bounds(&policy(), T0) {
            Err(ObservationJournalError::InvalidDispatchPolicy {
                field: "max_rounds_per_drain",
            }) => {}
            other => {
                return Err(format!("{candidate:?} produced usable bounds: {other:?}").into());
            }
        }
    }
    match (DispatchPolicyV1 {
        drain_budget_micros: i64::MAX,
        ..dispatch_policy()
    })
    .drain_bounds(&policy(), T0)
    {
        Err(ObservationJournalError::InvalidDispatchPolicy {
            field: "drain_budget_micros",
        }) => {}
        other => return Err(format!("an unbounded wall budget was admitted: {other:?}").into()),
    }
    Ok(())
}

/// Both drain bounds are part of the policy the journal validates, so a mount
/// cannot configure a dispatcher that never yields or that cannot fit a single
/// attempt.
#[test]
fn drain_bounds_are_configurable_and_refused_when_they_bound_nothing() -> TestResult {
    let retention = policy();
    dispatch_policy().validate_against(&retention)?;

    let cases = [
        (
            "max_rounds_per_drain",
            DispatchPolicyV1 {
                max_rounds_per_drain: 0,
                ..dispatch_policy()
            },
        ),
        (
            "drain_budget_micros",
            DispatchPolicyV1 {
                drain_budget_micros: ATTEMPT_BUDGET - 1,
                ..dispatch_policy()
            },
        ),
    ];
    for (expected, candidate) in cases {
        match candidate.validate_against(&retention) {
            Err(ObservationJournalError::InvalidDispatchPolicy { field }) => {
                assert_eq!(field, expected, "{candidate:?}");
            }
            other => {
                return Err(
                    format!("{candidate:?} was not refused on {expected}: {other:?}").into(),
                );
            }
        }
    }

    // The derived bounds are the policy's, not a second knob. They are also
    // read-only: `DrainBoundsV1` has no public fields and no other constructor,
    // so these two numbers are the widest a caller can ever ask for.
    let bounds = dispatch_policy().drain_bounds(&retention, T0)?;
    assert_eq!(bounds.max_rounds(), dispatch_policy().max_rounds_per_drain);
    assert_eq!(
        bounds.deadline_unix_micros(),
        T0 + dispatch_policy().drain_budget_micros
    );
    Ok(())
}

/// The dispatcher and the journal share one retry curve. A drift between them
/// would mean a provider that answers `provider_unavailable` and a provider
/// that cannot be reached at all are rescheduled differently for the same
/// attempt number.
#[test]
fn the_dispatcher_and_the_journal_reschedule_on_one_curve() -> TestResult {
    let retention = policy();
    let backoff = RetryBackoffV1::of(&retention);
    for attempt in 1..=40_u32 {
        assert_eq!(
            backoff.delay_for(attempt),
            retention.next_attempt_delay(attempt),
            "attempt {attempt} disagreed"
        );
    }
    assert_eq!(backoff.delay_for(1), retention.backoff_base_micros);
    assert_eq!(
        backoff.delay_for(40),
        retention.backoff_max_micros,
        "the curve must saturate at the ceiling rather than overflow"
    );
    assert_eq!(backoff.next_attempt_at(T0, 1), T0 + backoff.delay_for(1));

    backoff.validate()?;
    // The curve is not a caller-supplied struct any more: the fields are
    // private and `of` is the only constructor, so the only way to reach an
    // invalid curve is through an invalid retention policy — and that is
    // refused by `validate`, and again by the dispatcher before it leases.
    for (field, broken) in [
        (
            "backoff_base_micros",
            RetentionPolicyV1 {
                backoff_base_micros: 0,
                ..policy()
            },
        ),
        (
            "backoff_base_micros",
            RetentionPolicyV1 {
                backoff_base_micros: -SECOND,
                ..policy()
            },
        ),
        (
            "backoff_max_micros",
            RetentionPolicyV1 {
                backoff_base_micros: 2 * SECOND,
                backoff_max_micros: SECOND,
                ..policy()
            },
        ),
    ] {
        match RetryBackoffV1::of(&broken).validate() {
            Err(ObservationJournalError::InvalidDispatchPolicy { field: refused }) => {
                assert_eq!(refused, field);
            }
            other => return Err(format!("{field} was not refused: {other:?}").into()),
        }
    }
    Ok(())
}

/// A provider that answers about somebody else's exact coding scope, and
/// remembers how many times it did.
struct WrongScopeProvider {
    answers: Cell<u32>,
    now: i64,
}

impl ProviderDeliveryAdapterV1 for WrongScopeProvider {
    type Error = AdapterError;

    fn deliver(
        &self,
        leased: &LeasedObservationV1,
        _control: &DeliveryControlV1,
    ) -> Result<DeliveryAttemptV1, Self::Error> {
        self.answers.set(self.answers.get().saturating_add(1));
        let terminal = TerminalRecord::new(
            ProviderOperation::Observe,
            leased.target.provider_id.clone(),
            TerminalCode::Success,
            CommittedEffectEvidence::committed(
                1,
                2,
                Vec::new(),
                PROVIDER_RECEIPT_DIGEST,
                PROVENANCE_DIGEST,
            )
            .map_err(|error| AdapterError(error.to_string()))?,
            FallbackDirective::forbidden(),
            format!("observe-{}", leased.observation_id.as_str()),
            // Somebody else's scope: the host must refuse this as evidence.
            PROVENANCE_DIGEST.to_owned(),
            None,
        )
        .map_err(|error| AdapterError(error.to_string()))?;
        Ok(DeliveryAttemptV1::Answered {
            terminal: Box::new(terminal),
            started_at_unix_micros: self.now,
            finished_at_unix_micros: self.now.saturating_add(1_000),
        })
    }
}

/// A provider that answers nothing and asks — falsely — to be treated as
/// cancelled, which would buy it immediate eligibility if the runtime believed
/// adapters about cancellation.
struct ForgedCancellationProvider {
    attempts: Cell<u32>,
}

impl ProviderDeliveryAdapterV1 for ForgedCancellationProvider {
    type Error = AdapterError;

    fn deliver(
        &self,
        _leased: &LeasedObservationV1,
        _control: &DeliveryControlV1,
    ) -> Result<DeliveryAttemptV1, Self::Error> {
        self.attempts.set(self.attempts.get().saturating_add(1));
        Ok(DeliveryAttemptV1::CancelledByShutdown)
    }
}

/// The defect this catches: `lease_pending` stops at `max_bytes` long before it
/// runs out of eligible rows, so a drain that read "fewer rows than the item
/// bound" as quiescence reported `more_work_pending() == false` over a full
/// journal. The mounted worker then parked for its whole interval on a backlog
/// nothing raises the wake edge for again — the same liveness stall the drain
/// exists to remove, in byte-shaped clothing.
#[test]
fn a_backlog_bounded_by_bytes_rather_than_items_still_drains_in_one_turn() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    seed(&store, 5)?;

    // Every fixture payload is bigger than a fifth of this, so the byte bound
    // cuts each round short while the item bound stays wide open at eight.
    let byte_bound = 40_u64;
    let policy_under_test = DispatchPolicyV1 {
        batch_max_bytes: byte_bound,
        ..dispatch_policy()
    };
    let request = DispatchRequestV1 {
        lease: {
            let mut lease = lease_request(T0, BATCH);
            lease.max_bytes = byte_bound;
            lease
        },
        retry_backoff: RetryBackoffV1::of(&policy()),
        attempt_budget_micros: ATTEMPT_BUDGET,
    };

    // One round is byte-bound, not item-bound: fewer rows than `max_items`
    // came back even though four more rows are eligible right now.
    let wake = DeliveryWakeV1::new();
    let probe = ApplyingProvider::new(T0);
    let single = DeliveryRuntimeV1::new(&store, &probe, &wake).dispatch_batch(&request)?;
    assert!(
        single.leased > 0 && single.leased < BATCH,
        "the byte bound must cut the batch short: leased {}",
        single.leased
    );
    // Inspect rather than lease: the rows the byte bound left behind are still
    // pending and still eligible at this very instant, which is exactly why
    // reading a short batch as quiescence was wrong.
    let still_eligible = rows(&store)?
        .into_iter()
        .filter(|row| {
            row.state == DeliveryStateV1::Pending && row.next_attempt_at_unix_micros <= T0
        })
        .count();
    assert_eq!(still_eligible, 5 - usize::try_from(single.leased)?);

    let wake = DeliveryWakeV1::new();
    let provider = ApplyingProvider::new(T0);
    let delivery = DeliveryRuntimeV1::new(&store, &provider, &wake);
    let report = delivery.drain(
        &request,
        &policy_under_test.drain_bounds(&policy(), T0)?,
        || T0,
    )?;

    assert_eq!(report.stop, DrainStopV1::Quiesced);
    assert!(
        !report.more_work_pending(),
        "a genuinely empty journal must not report pending work"
    );
    for row in rows(&store)? {
        assert_eq!(
            row.state,
            DeliveryStateV1::Acknowledged,
            "row {:?} was left behind by a byte-bounded drain",
            row.source_sequence
        );
    }
    Ok(())
}

/// The defect this catches: a drain that stops at a bound while byte-bounded
/// rounds are still leasing work must say so, or the worker parks on it.
#[test]
fn a_byte_bounded_drain_that_hits_its_round_bound_reports_pending_work() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    seed(&store, 6)?;
    let byte_bound = 40_u64;
    let policy_under_test = DispatchPolicyV1 {
        batch_max_bytes: byte_bound,
        max_rounds_per_drain: 2,
        ..dispatch_policy()
    };
    let request = DispatchRequestV1 {
        lease: {
            let mut lease = lease_request(T0, BATCH);
            lease.max_bytes = byte_bound;
            lease
        },
        retry_backoff: RetryBackoffV1::of(&policy()),
        attempt_budget_micros: ATTEMPT_BUDGET,
    };
    let wake = DeliveryWakeV1::new();
    let provider = ApplyingProvider::new(T0);
    let delivery = DeliveryRuntimeV1::new(&store, &provider, &wake);

    let report = delivery.drain(
        &request,
        &policy_under_test.drain_bounds(&policy(), T0)?,
        || T0,
    )?;
    assert_eq!(report.rounds, 2);
    assert_eq!(report.stop, DrainStopV1::RoundBudgetReached);
    assert!(
        report.more_work_pending(),
        "a byte-bounded drain cut short by its round bound left work behind"
    );
    assert!(
        rows(&store)?.iter().any(|row| {
            row.state == DeliveryStateV1::Pending && row.next_attempt_at_unix_micros <= T0
        }),
        "the journal really does still hold eligible rows"
    );
    Ok(())
}

/// The defect this catches: an answered attempt whose terminal the host refused
/// left evidence only in the in-memory batch report, so a crash erased every
/// trace of a provider answer after the attempt number was already consumed.
#[test]
fn a_refused_provider_terminal_is_recorded_durably_and_never_rewritten() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    let observation_id = {
        let store = journal(&path)?;
        store.append_admitted(&Builder::at_sequence(1).build()?)?;
        let wake = DeliveryWakeV1::new();
        let provider = WrongScopeProvider {
            answers: Cell::new(0),
            now: T0,
        };
        let delivery = DeliveryRuntimeV1::new(&store, &provider, &wake);
        let report = delivery.dispatch_batch(&dispatch_at(T0))?;

        assert_eq!(report.leased, 1);
        assert_eq!(
            report.receipts_recorded, 0,
            "a refused terminal is no receipt"
        );
        assert_eq!(report.refusals_recorded, 1);
        assert_eq!(report.failures.len(), 1);
        let row = row_at(&store, 1)?;
        assert_eq!(row.state, DeliveryStateV1::Pending);
        row.observation_id
    };

    // Restart: the refusal is durable, the receipt still does not exist.
    let store = journal(&path)?;
    assert!(
        store.receipts_for(&observation_id)?.is_empty(),
        "a refused terminal must never become provider-effect evidence"
    );
    let refusals = store.attempt_refusals_for(&observation_id)?;
    assert_eq!(refusals.len(), 1, "the refusal did not survive restart");
    let refusal = refusals.first().ok_or("expected one refusal")?;
    assert_eq!(refusal.attempt_number, 1);
    assert_eq!(
        refusal.category,
        AttemptRefusalCategoryV1::TerminalIdentityMismatch
    );
    assert_eq!(refusal.refused_field, "exact_scope_sha256");
    assert_eq!(refusal.provided.as_deref(), Some(PROVENANCE_DIGEST));
    assert_eq!(refusal.terminal_code, "success");
    assert_eq!(
        refusal.provider_receipt_digest.as_deref(),
        Some(PROVIDER_RECEIPT_DIGEST),
        "safe terminal metadata is retained, without accepting the effect claim"
    );
    assert!(
        refusal.detail.contains("exact_scope_sha256"),
        "unexpected detail: {}",
        refusal.detail
    );

    // Immutable: a second refusal for the same attempt does not rewrite it.
    let wake = DeliveryWakeV1::new();
    let provider = WrongScopeProvider {
        answers: Cell::new(0),
        now: T0 + MINUTE,
    };
    let delivery = DeliveryRuntimeV1::new(&store, &provider, &wake);
    let mut retry = dispatch_at(T0 + MINUTE);
    retry.lease.now_unix_micros = T0 + MINUTE;
    let second = delivery.dispatch_batch(&retry)?;
    assert_eq!(second.leased, 1, "the row came back on the retry curve");
    assert_eq!(second.refusals_recorded, 1);

    let refusals = store.attempt_refusals_for(&observation_id)?;
    assert_eq!(refusals.len(), 2, "attempt 2 has its own refusal slot");
    let first = refusals.first().ok_or("expected attempt 1")?;
    assert_eq!(first.attempt_number, 1);
    assert_eq!(
        first.started_at_unix_micros, T0,
        "the standing refusal for attempt 1 was rewritten"
    );
    Ok(())
}

/// The defect this catches: an adapter could name its own retry instant — even
/// one already past — so a whole failed batch became eligible again inside the
/// same drain and marched through the attempt ceiling in one turn.
#[test]
fn an_adapter_cannot_buy_an_immediate_same_drain_retry() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    seed(&store, 3)?;
    let wake = DeliveryWakeV1::new();
    let provider = ForgedCancellationProvider {
        attempts: Cell::new(0),
    };
    let delivery = DeliveryRuntimeV1::new(&store, &provider, &wake);

    let report = delivery.drain(
        &dispatch_at(T0),
        &dispatch_policy().drain_bounds(&policy(), T0)?,
        || T0,
    )?;
    assert_eq!(report.stop, DrainStopV1::Quiesced);
    assert_eq!(
        report.totals.leased, 3,
        "each row must be handed to the provider exactly once in this drain"
    );
    assert_eq!(provider.attempts.get(), 3);
    assert_eq!(
        report.totals.cancelled_in_flight, 0,
        "nothing was cancelled"
    );

    // Every row is back on the journal's own curve, not eligible at once.
    let retention = policy();
    for sequence in 1..=3 {
        let row = row_at(&store, sequence)?;
        assert_eq!(row.state, DeliveryStateV1::Pending);
        assert_eq!(row.attempt_number, 1, "one attempt per row, not three");
        assert_eq!(
            row.next_attempt_at_unix_micros,
            T0 + retention.next_attempt_delay(1),
            "sequence {sequence} was made eligible off the shared curve"
        );
    }
    assert!(
        store.lease_pending(&lease_request(T0, 64))?.is_empty(),
        "no row may be leasable again at the instant its attempt failed"
    );
    Ok(())
}
