//! Delivery: lease, dispatch the exact stored bytes, record what came back,
//! return what did not, reap what lapsed, and stop within an explicit bound.
//!
//! # The one thing this runtime refuses to do
//!
//! It never invents a provider outcome. A provider that answered produces a
//! receipt derived from its own terminal record; an unanswered attempt is
//! released without a receipt, while a shutdown-cancelled attempt receives
//! host-owned cancellation evidence whose effect remains unknown. The
//! at-least-once story is unchanged: an acknowledgement lost between the
//! provider's commit and this process's write is redelivered, and the provider
//! recognises the content-derived idempotency key and answers
//! `duplicate_acknowledged`.
//!
//! Releasing a lease does not give the attempt number back — the claim consumed
//! it — so redelivery still walks towards the policy's `max_attempts` instead of
//! retrying forever.
//!
//! # How cancellation reaches an attempt
//!
//! Every attempt is handed a [`DeliveryControlV1`]: an absolute deadline that is
//! the tightest of the caller's per-attempt budget, the row's own delivery
//! deadline, and the lease expiry, plus the wake edge's shared cancellation
//! token. [`DeliveryWakeV1::request_shutdown`] cancels that token, so a provider
//! blocked inside a call sees shutdown through the same control the fabric
//! already preflights, and the round checks it again before each further item.
//! A cancelled attempt is recorded as host-owned evidence with an unknown
//! provider effect, then its lease is released and the row stays redeliverable.

use tracedecay_memory_provider_api::{CancellationToken, ProviderOperation, TerminalRecord};

use crate::error::ObservationJournalError;
use crate::identity::{DispatchLeaseIdV1, ObservationIdV1};
use crate::inspection::JournalInspectionFilterV1;
use crate::lease::{AttemptOutcomeV1, LeaseRequestV1, LeasedObservationV1};
use crate::port::ObservationJournalReaderV1;
use crate::receipt::ObservationDeliveryReceiptV1;
use crate::refusal::{AttemptRefusalCategoryV1, AttemptRefusalOutcomeV1, AttemptRefusalRecordV1};
use crate::state::DeliveryStateV1;

use super::dispatch_policy::RetryBackoffV1;
use super::error::{AdapterFailureV1, ObservationRuntimeError, TerminalIdentityMismatchV1};
use super::wake::{DeliveryWakeV1, WakeOutcomeV1};

/// What one delivery attempt produced.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeliveryAttemptV1 {
    /// The provider answered with a typed terminal record.
    Answered {
        /// The provider's own terminal, carried verbatim so the receipt is
        /// derived from it rather than from the adapter's opinion of it.
        terminal: Box<TerminalRecord>,
        /// Instant the attempt started.
        started_at_unix_micros: i64,
        /// Instant the attempt finished.
        finished_at_unix_micros: i64,
    },
    /// No provider answer was obtained, so nothing is known about the provider's
    /// effect and nothing is recorded as an outcome. The lease returns to
    /// `Pending` for a later attempt.
    ///
    /// It deliberately carries no retry instant. When it did, an adapter could
    /// name any instant it liked — including one already past — and a whole
    /// failed batch became eligible again inside the same drain, walking
    /// straight through the attempt ceiling. Rescheduling is runtime-owned
    /// instead: [`DispatchRequestV1::retry_backoff`], the journal's own capped
    /// exponential, applied to the attempt number this claim consumed.
    Unanswered,
    /// The attempt stopped because shutdown cancelled the control it was handed.
    ///
    /// Nothing was answered here either, but the row is deliberately eligible
    /// again at once: no provider work was refused, the process is going away,
    /// and the next life of the dispatcher should find the row waiting rather
    /// than serving a backoff for a shutdown it did not cause. The runtime
    /// honours this **only** when the control really is cancelled — an adapter
    /// that claims cancellation without it is treated as
    /// [`DeliveryAttemptV1::Unanswered`], so immediate eligibility cannot be
    /// bought by lying.
    CancelledByShutdown {
        /// Instant the cancelled attempt started.
        started_at_unix_micros: i64,
        /// Instant the host stopped waiting for the attempt.
        finished_at_unix_micros: i64,
    },
}

/// The bound one delivery attempt runs under.
///
/// Built by the runtime for every leased item and handed to the adapter, which
/// must propagate both halves into the provider call: the deadline as the
/// operation's absolute deadline, the token as its cancellation. The adapter
/// never extends either.
#[derive(Clone, Debug)]
pub struct DeliveryControlV1 {
    deadline_unix_micros: i64,
    cancellation: CancellationToken,
}

impl DeliveryControlV1 {
    /// Absolute instant after which the attempt must stop. Never later than the
    /// lease expiry or the row's own delivery deadline.
    #[must_use]
    pub const fn deadline_unix_micros(&self) -> i64 {
        self.deadline_unix_micros
    }

    /// Budget left at `now_unix_micros`, saturating at zero.
    #[must_use]
    pub fn remaining_micros(&self, now_unix_micros: i64) -> i64 {
        self.deadline_unix_micros
            .saturating_sub(now_unix_micros)
            .max(0)
    }

    /// The shared cancellation token. Cancelled exactly when shutdown was
    /// requested on the wake edge this attempt was dispatched under.
    #[must_use]
    pub fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// Whether shutdown has cancelled this attempt.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

/// The caller-supplied provider transport seam.
///
/// The adapter is handed the journal's own sanitized bytes in
/// [`LeasedObservationV1::payload`] and must send exactly those, so the
/// provider's `payload_sha256` comparison matches the receipt the journal
/// stores. It is also handed the attempt's [`DeliveryControlV1`] and must run
/// the provider call under it.
pub trait ProviderDeliveryAdapterV1 {
    /// The adapter's own failure type, preserved whole when it fails.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Delivers one leased observation under the attempt's bound.
    fn deliver(
        &self,
        leased: &LeasedObservationV1,
        control: &DeliveryControlV1,
    ) -> Result<DeliveryAttemptV1, Self::Error>;
}

/// One bounded dispatch round.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatchRequestV1 {
    /// The bounded lease to claim work with.
    pub lease: LeaseRequestV1,
    /// How a lease released because the *adapter itself* failed is
    /// rescheduled. The delay is the policy's capped exponential for the
    /// attempt number the claim consumed, measured from the round's own
    /// `lease.now_unix_micros`, so a provider that is simply unreachable backs
    /// off exactly like one that answers `provider_unavailable` instead of
    /// being retried at a flat interval until its attempt ceiling is gone.
    pub retry_backoff: RetryBackoffV1,
    /// How long one attempt may run from the lease instant. The attempt's
    /// deadline is this, tightened by the lease expiry and the row's own
    /// delivery deadline. Must be positive.
    pub attempt_budget_micros: i64,
}

/// One delivery that produced no receipt.
#[derive(Debug)]
pub struct DeliveryFailureV1 {
    /// Observation the attempt addressed.
    pub observation_id: ObservationIdV1,
    /// Attempt number the lease consumed. It is never handed out again.
    pub attempt_number: u32,
    /// Whether the lease was returned to `Pending`. `false` means the lease had
    /// already lapsed and been reaped, so a reaper already recovered the row.
    pub lease_released: bool,
    /// The adapter's own failure, or the reason its terminal could not describe
    /// this delivery.
    pub cause: AdapterFailureV1,
}

/// What one dispatch round did.
#[derive(Debug, Default)]
pub struct DeliveryBatchReportV1 {
    /// Rows leased this round.
    pub leased: u32,
    /// Immutable attempt receipts written.
    pub receipts_recorded: u32,
    /// Attempts whose receipt already existed; the standing receipt stands and
    /// still settles the row.
    pub duplicate_receipts: u32,
    /// Attempts whose lease had already been reaped or completed. The receipt is
    /// still written; the row is not disturbed.
    pub leases_lost: u32,
    /// Leases returned to `Pending` without a receipt.
    pub leases_released: u32,
    /// Deliveries that reached a terminal state this round.
    pub settled_terminal: u32,
    /// Deliveries scheduled for another attempt.
    pub retry_scheduled: u32,
    /// Rows this round leased but never handed to the adapter, because shutdown
    /// was requested between items. Their leases were released with immediate
    /// eligibility; no attempt was made and no receipt exists.
    pub cancelled_before_dispatch: u32,
    /// Attempts stopped after shutdown cancelled their control. Each has a
    /// host-owned cancellation receipt and its lease is released for replay.
    pub cancelled_in_flight: u32,
    /// Answered attempts whose terminal the host refused, recorded as durable
    /// refusal evidence beside an unknown-effect receipt for the attempt.
    pub refusals_recorded: u32,
    /// Answered attempts whose refusal was already durably recorded, so the
    /// standing record stands.
    pub duplicate_refusals: u32,
    /// Attempts that produced an adapter or terminal-validation failure.
    pub failures: Vec<DeliveryFailureV1>,
}

impl DeliveryBatchReportV1 {
    /// Folds one round's counters and failures into a running total.
    ///
    /// Nothing is dropped and nothing is rounded: a drain reports exactly what
    /// its rounds reported. The failure list stays bounded by construction —
    /// at most `max_rounds * batch_max_items` entries, both of which the
    /// dispatch policy validates.
    fn absorb(&mut self, round: Self) {
        self.leased = self.leased.saturating_add(round.leased);
        self.receipts_recorded = self
            .receipts_recorded
            .saturating_add(round.receipts_recorded);
        self.duplicate_receipts = self
            .duplicate_receipts
            .saturating_add(round.duplicate_receipts);
        self.leases_lost = self.leases_lost.saturating_add(round.leases_lost);
        self.leases_released = self.leases_released.saturating_add(round.leases_released);
        self.settled_terminal = self.settled_terminal.saturating_add(round.settled_terminal);
        self.retry_scheduled = self.retry_scheduled.saturating_add(round.retry_scheduled);
        self.cancelled_before_dispatch = self
            .cancelled_before_dispatch
            .saturating_add(round.cancelled_before_dispatch);
        self.cancelled_in_flight = self
            .cancelled_in_flight
            .saturating_add(round.cancelled_in_flight);
        self.refusals_recorded = self
            .refusals_recorded
            .saturating_add(round.refusals_recorded);
        self.duplicate_refusals = self
            .duplicate_refusals
            .saturating_add(round.duplicate_refusals);
        self.failures.extend(round.failures);
    }
}

/// The two bounds one drain runs under.
///
/// Built from the dispatch policy by [`DispatchPolicyV1::drain_bounds`], never
/// by hand at a call site, so a drain cannot be handed a longer budget than the
/// policy the journal validated.
///
/// [`DispatchPolicyV1::drain_bounds`]: super::DispatchPolicyV1::drain_bounds
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DrainBoundsV1 {
    max_rounds: u32,
    deadline_unix_micros: i64,
}

impl DrainBoundsV1 {
    /// The one constructor, reachable only from [`DispatchPolicyV1::drain_bounds`]
    /// after that policy has been revalidated against the journal's retention
    /// policy. Private to the runtime module on purpose: with no public
    /// constructor and no public fields, an external call site cannot forge
    /// `u32::MAX` rounds or an `i64::MAX` deadline, so the widest drain any
    /// caller can ask for is the one the validated policy already describes.
    ///
    /// [`DispatchPolicyV1::drain_bounds`]: super::DispatchPolicyV1::drain_bounds
    pub(super) const fn of_policy(max_rounds: u32, deadline_unix_micros: i64) -> Self {
        Self {
            max_rounds,
            deadline_unix_micros,
        }
    }

    /// Maximum rounds the drain may run.
    #[must_use]
    pub const fn max_rounds(&self) -> u32 {
        self.max_rounds
    }

    /// Absolute instant at or after which the drain stops between rounds.
    #[must_use]
    pub const fn deadline_unix_micros(&self) -> i64 {
        self.deadline_unix_micros
    }
}

/// Why a drain handed the loop back.
///
/// Every variant except [`DrainStopV1::Quiesced`] means deliverable work was
/// still eligible when the drain returned, which is exactly what
/// [`DrainReportV1::more_work_pending`] reports: a caller that parks on that
/// answer would sit on a backlog nothing is going to signal about again.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DrainStopV1 {
    /// A round leased no rows at all, which is the only authoritative proof
    /// that nothing more was eligible: a batch cut short by the byte bound is
    /// not, because the journal stops leasing at `max_bytes` long before it
    /// runs out of work.
    #[default]
    Quiesced,
    /// The round bound was reached with full batches still coming.
    RoundBudgetReached,
    /// The wall budget elapsed between rounds.
    BudgetElapsed,
    /// Shutdown was requested. In-flight attempts were already cancelled
    /// through their own control; the drain starts no further round.
    ShutdownRequested,
}

/// What one bounded drain did.
#[derive(Debug, Default)]
pub struct DrainReportV1 {
    /// Rounds actually dispatched.
    pub rounds: u32,
    /// Every round's counters and failures, folded together.
    pub totals: DeliveryBatchReportV1,
    /// Why the drain stopped.
    pub stop: DrainStopV1,
}

impl DrainReportV1 {
    /// Whether deliverable work was still eligible when the drain returned.
    ///
    /// A caller that parks on `true` re-introduces the very stall the drain
    /// exists to remove, because the wake edge is one collapsed signal and
    /// nothing raises it again for work that is already journalled.
    #[must_use]
    pub const fn more_work_pending(&self) -> bool {
        !matches!(self.stop, DrainStopV1::Quiesced)
    }
}

/// What one bounded shutdown pass did.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ShutdownReportV1 {
    /// Lapsed leases returned to `Pending` by this pass.
    pub leases_reaped: u32,
    /// Rows still held by a lease after the pass.
    pub leases_outstanding: u64,
    /// True only when no lease is outstanding. A `false` here is not a failure:
    /// a lease held by another live dispatcher is legitimate, and every lease
    /// lapses on its own without a coordinator.
    pub quiesced: bool,
}

/// One bounded shutdown pass.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShutdownRequestV1 {
    /// Provider whose leases this runtime is responsible for.
    pub provider_id: String,
    /// Current instant. Leases lapsed at or before it are reaped.
    pub now_unix_micros: i64,
    /// Maximum leases one pass may reap. The explicit bound.
    pub reap_budget: u32,
}

/// Drives leased observations out to a provider and their answers back in.
#[derive(Debug)]
pub struct DeliveryRuntimeV1<'a, R: ?Sized, A> {
    reader: &'a R,
    adapter: &'a A,
    wake: &'a DeliveryWakeV1,
}

impl<'a, R, A> DeliveryRuntimeV1<'a, R, A>
where
    R: ObservationJournalReaderV1 + ?Sized,
    A: ProviderDeliveryAdapterV1,
{
    /// Binds one journal reader, one provider adapter, and the wake edge.
    #[must_use]
    pub const fn new(reader: &'a R, adapter: &'a A, wake: &'a DeliveryWakeV1) -> Self {
        Self {
            reader,
            adapter,
            wake,
        }
    }

    /// Parks until admission signals work, shutdown is requested, or the
    /// explicit bound elapses.
    #[must_use]
    pub fn wait_for_work(&self, timeout: std::time::Duration) -> WakeOutcomeV1 {
        self.wake.wait(timeout)
    }

    /// Leases one bounded batch, dispatches it, and records what came back.
    ///
    /// A single row's adapter failure does not abandon the rest of the batch:
    /// its lease is released, the failure is reported, and the round continues.
    /// Nothing is swallowed — every failure reaches the caller in
    /// [`DeliveryBatchReportV1::failures`] with the adapter's own error intact.
    ///
    /// Shutdown is honoured at three points: a round that starts after shutdown
    /// leases nothing; a round interrupted between items releases the rest of
    /// its leases without an attempt; and an attempt that returns without a
    /// receipt after its control was cancelled is counted as cancelled, not
    /// failed. In every case the row stays `Pending` and no receipt is invented.
    pub fn dispatch_batch(
        &self,
        request: &DispatchRequestV1,
    ) -> Result<DeliveryBatchReportV1, ObservationRuntimeError> {
        if request.attempt_budget_micros <= 0 {
            return Err(ObservationRuntimeError::InvalidDispatchRequest {
                field: "attempt_budget_micros",
            });
        }
        // The retry curve is checked *before* a lease is taken, not after a
        // batch has already failed. A curve that cannot delay would make every
        // released row eligible again inside this same drain and burn the
        // attempt ceiling in one turn.
        request.retry_backoff.validate()?;
        let cancellation = self.wake.cancellation();
        if cancellation.is_cancelled() {
            return Ok(DeliveryBatchReportV1::default());
        }
        let leased = self.reader.lease_pending(&request.lease)?;
        let mut report = DeliveryBatchReportV1 {
            leased: u32::try_from(leased.len()).unwrap_or(u32::MAX),
            ..DeliveryBatchReportV1::default()
        };
        let budget_deadline = request
            .lease
            .now_unix_micros
            .saturating_add(request.attempt_budget_micros);

        for item in leased {
            if cancellation.is_cancelled() {
                // Nothing was attempted, so the row is eligible again at once.
                if self.release(&item.lease_id, request.lease.now_unix_micros)? {
                    report.leases_released = report.leases_released.saturating_add(1);
                } else {
                    report.leases_lost = report.leases_lost.saturating_add(1);
                }
                report.cancelled_before_dispatch =
                    report.cancelled_before_dispatch.saturating_add(1);
                continue;
            }
            let control = DeliveryControlV1 {
                deadline_unix_micros: budget_deadline
                    .min(item.deadline_unix_micros)
                    .min(item.lease_expires_at_unix_micros),
                cancellation: cancellation.clone(),
            };
            let attempt = match self.adapter.deliver(&item, &control) {
                Ok(attempt) => attempt,
                Err(cause) => {
                    // Cancellation is an explicit adapter outcome. A shared
                    // token may race with an unrelated transport or readiness
                    // error, so it cannot erase the typed cause or manufacture
                    // cancellation evidence for an attempt that returned Err.
                    self.fail_attempt(
                        &mut report,
                        &item,
                        AdapterFailureV1::new(cause),
                        request
                            .retry_backoff
                            .next_attempt_at(request.lease.now_unix_micros, item.attempt_number),
                    )?;
                    continue;
                }
            };

            match attempt {
                DeliveryAttemptV1::Unanswered => {
                    let retry_after_unix_micros = request
                        .retry_backoff
                        .next_attempt_at(request.lease.now_unix_micros, item.attempt_number);
                    if self.release(&item.lease_id, retry_after_unix_micros)? {
                        report.leases_released = report.leases_released.saturating_add(1);
                    } else {
                        report.leases_lost = report.leases_lost.saturating_add(1);
                    }
                }
                DeliveryAttemptV1::CancelledByShutdown {
                    started_at_unix_micros,
                    finished_at_unix_micros,
                } => {
                    // Immediate eligibility and cancellation evidence are
                    // reserved for a real shutdown. An adapter that claims
                    // cancellation without a cancelled control serves the
                    // ordinary retry curve, so no adapter can buy itself a
                    // same-drain retry or mint a false receipt.
                    if control.is_cancelled() {
                        report.cancelled_in_flight = report.cancelled_in_flight.saturating_add(1);
                        self.record_cancelled(
                            &mut report,
                            &item,
                            started_at_unix_micros,
                            finished_at_unix_micros,
                            request.lease.now_unix_micros,
                        )?;
                    } else {
                        let retry_after_unix_micros = request
                            .retry_backoff
                            .next_attempt_at(request.lease.now_unix_micros, item.attempt_number);
                        if self.release(&item.lease_id, retry_after_unix_micros)? {
                            report.leases_released = report.leases_released.saturating_add(1);
                        } else {
                            report.leases_lost = report.leases_lost.saturating_add(1);
                        }
                    }
                }
                DeliveryAttemptV1::Answered {
                    terminal,
                    started_at_unix_micros,
                    finished_at_unix_micros,
                } => {
                    // Shutdown owns an attempt as soon as its shared token is
                    // cancelled. An adapter may answer after that edge, but the
                    // answer is late evidence and must never become a second
                    // terminal outcome. Atomically release the lease with the
                    // host-owned cancellation receipt instead.
                    if control.is_cancelled() {
                        report.cancelled_in_flight = report.cancelled_in_flight.saturating_add(1);
                        self.record_cancelled(
                            &mut report,
                            &item,
                            started_at_unix_micros,
                            finished_at_unix_micros,
                            request.lease.now_unix_micros,
                        )?;
                        continue;
                    }
                    if let Err(mismatch) = verify_terminal(&item, &terminal) {
                        // The terminal is refused as delivery evidence, but the
                        // attempt number it consumed is gone. Record the refusal
                        // durably first, then release, so a crash here cannot
                        // erase the fact that the provider answered.
                        self.record_refusal(
                            &mut report,
                            &item,
                            &terminal,
                            AttemptRefusalCategoryV1::TerminalIdentityMismatch,
                            mismatch.field,
                            Some(mismatch.expected.clone()),
                            Some(mismatch.provided.clone()),
                            &mismatch.to_string(),
                            started_at_unix_micros,
                            finished_at_unix_micros,
                        )?;
                        let receipt = ObservationDeliveryReceiptV1::from_refused_terminal(
                            &item,
                            started_at_unix_micros,
                            finished_at_unix_micros,
                        )?;
                        let lease_released = self.record_unsettled(
                            &mut report,
                            &item,
                            &receipt,
                            request.retry_backoff.next_attempt_at(
                                request.lease.now_unix_micros,
                                item.attempt_number,
                            ),
                        )?;
                        report.failures.push(DeliveryFailureV1 {
                            observation_id: item.observation_id.clone(),
                            attempt_number: item.attempt_number,
                            lease_released,
                            cause: AdapterFailureV1::new(mismatch),
                        });
                        continue;
                    }
                    let receipt = match ObservationDeliveryReceiptV1::from_terminal(
                        &terminal,
                        &item,
                        started_at_unix_micros,
                        finished_at_unix_micros,
                    ) {
                        Ok(receipt) => receipt,
                        Err(cause) => {
                            self.record_refusal(
                                &mut report,
                                &item,
                                &terminal,
                                AttemptRefusalCategoryV1::ReceiptNotAdmissible,
                                "terminal_record",
                                None,
                                Some(terminal.terminal_code().as_wire().to_owned()),
                                &cause.to_string(),
                                started_at_unix_micros,
                                finished_at_unix_micros,
                            )?;
                            let receipt = ObservationDeliveryReceiptV1::from_refused_terminal(
                                &item,
                                started_at_unix_micros,
                                finished_at_unix_micros,
                            )?;
                            let lease_released = self.record_unsettled(
                                &mut report,
                                &item,
                                &receipt,
                                request.retry_backoff.next_attempt_at(
                                    request.lease.now_unix_micros,
                                    item.attempt_number,
                                ),
                            )?;
                            report.failures.push(DeliveryFailureV1 {
                                observation_id: item.observation_id.clone(),
                                attempt_number: item.attempt_number,
                                lease_released,
                                cause: AdapterFailureV1::new(cause),
                            });
                            continue;
                        }
                    };
                    self.record(&mut report, &receipt)?;
                }
            }
        }
        Ok(report)
    }

    /// Keeps dispatching bounded rounds while full batches keep coming, and
    /// stops on the first of: nothing more eligible, the round bound, the wall
    /// budget, or shutdown.
    ///
    /// # Why a single round is not enough
    ///
    /// The wake edge is one collapsed boolean, not a count. A worker that
    /// dispatches one batch per wake therefore drains a journalled backlog at
    /// `batch_max_items` per park interval, because nothing signals again for
    /// rows that were already admitted — the backlog is durable, and the only
    /// thing that was lost is the *edge*. A drain closes that gap without
    /// unbounding the loop: it runs at most `bounds.max_rounds` rounds, stops
    /// between rounds once `bounds.deadline_unix_micros` has passed so reaping,
    /// retention, and shutdown are never starved, and tells the caller through
    /// [`DrainReportV1::more_work_pending`] whether it left work behind.
    ///
    /// `now` is the caller's clock, read once per round. The runtime owns no
    /// clock: the same closure supplies the lease instant, the round's retry
    /// baseline, and the budget comparison, so a rescheduled row is never dated
    /// from an instant the round started minutes earlier.
    ///
    /// `template`'s own `lease.now_unix_micros` is replaced per round; every
    /// other field is used verbatim.
    ///
    /// A journal failure ends the drain with the error, exactly as a single
    /// round does. What earlier rounds already recorded is durable and is not
    /// unwound.
    pub fn drain<C>(
        &self,
        template: &DispatchRequestV1,
        bounds: &DrainBoundsV1,
        mut now: C,
    ) -> Result<DrainReportV1, ObservationRuntimeError>
    where
        C: FnMut() -> i64,
    {
        if bounds.max_rounds == 0 {
            return Err(ObservationRuntimeError::InvalidDispatchRequest {
                field: "max_rounds",
            });
        }
        let cancellation = self.wake.cancellation();
        let mut report = DrainReportV1::default();
        loop {
            if cancellation.is_cancelled() {
                report.stop = DrainStopV1::ShutdownRequested;
                break;
            }
            if report.rounds >= bounds.max_rounds {
                report.stop = DrainStopV1::RoundBudgetReached;
                break;
            }
            let started_at_unix_micros = now();
            if started_at_unix_micros >= bounds.deadline_unix_micros {
                report.stop = DrainStopV1::BudgetElapsed;
                break;
            }
            let mut request = template.clone();
            request.lease.now_unix_micros = started_at_unix_micros;
            let round = self.dispatch_batch(&request)?;
            report.rounds = report.rounds.saturating_add(1);
            let leased = round.leased;
            report.totals.absorb(round);
            if leased == 0 {
                // Zero rows is the only authoritative proof that nothing was
                // eligible. A short batch is not: `lease_pending` stops early
                // once `max_bytes` is reached, so a round that leased one 700
                // KiB row under a 1 MiB byte bound and an item bound of 16 has
                // left plenty behind. Reading a short batch as quiescence made
                // `more_work_pending()` false there and parked the worker on a
                // backlog nothing signals about again — the very stall this
                // drain exists to remove, wearing a byte-shaped disguise.
                report.stop = DrainStopV1::Quiesced;
                break;
            }
        }
        Ok(report)
    }

    /// Returns lapsed leases to `Pending`, bounded per call.
    pub fn reap(&self, now_unix_micros: i64, budget: u32) -> Result<u32, ObservationRuntimeError> {
        self.reader
            .reap_expired_leases(now_unix_micros, budget)
            .map_err(ObservationRuntimeError::from)
    }

    /// Signals shutdown, reaps one bounded round of lapsed leases, and reports
    /// truthfully whether anything is still held.
    ///
    /// One pass, one budget. A caller that wants to keep trying calls again; a
    /// caller that gives up still leaves nothing stranded, because every
    /// outstanding lease carries its own expiry and any process can reap it.
    pub fn shutdown(
        &self,
        request: &ShutdownRequestV1,
    ) -> Result<ShutdownReportV1, ObservationRuntimeError> {
        self.wake.request_shutdown();
        let leases_reaped = self
            .reader
            .reap_expired_leases(request.now_unix_micros, request.reap_budget)?;
        let outstanding = self.reader.inspect(&JournalInspectionFilterV1 {
            provider_id: Some(request.provider_id.clone()),
            states: vec![DeliveryStateV1::Leased],
            limit: 1,
            ..JournalInspectionFilterV1::default()
        })?;
        Ok(ShutdownReportV1 {
            leases_reaped,
            leases_outstanding: outstanding.total_rows,
            quiesced: outstanding.total_rows == 0,
        })
    }

    fn record(
        &self,
        report: &mut DeliveryBatchReportV1,
        receipt: &ObservationDeliveryReceiptV1,
    ) -> Result<(), ObservationRuntimeError> {
        match self.reader.record_attempt(receipt)? {
            AttemptOutcomeV1::Recorded {
                state,
                next_attempt_at_unix_micros,
            } => {
                report.receipts_recorded = report.receipts_recorded.saturating_add(1);
                if state.is_terminal() {
                    report.settled_terminal = report.settled_terminal.saturating_add(1);
                }
                if next_attempt_at_unix_micros.is_some() {
                    report.retry_scheduled = report.retry_scheduled.saturating_add(1);
                }
            }
            AttemptOutcomeV1::DuplicateReceipt { state } => {
                report.duplicate_receipts = report.duplicate_receipts.saturating_add(1);
                if state.is_terminal() {
                    report.settled_terminal = report.settled_terminal.saturating_add(1);
                }
            }
            AttemptOutcomeV1::LeaseLost { .. } => {
                report.receipts_recorded = report.receipts_recorded.saturating_add(1);
                report.leases_lost = report.leases_lost.saturating_add(1);
            }
        }
        Ok(())
    }

    /// Writes durable evidence that a provider answered this attempt and the
    /// host refused the answer.
    ///
    /// This is the detailed companion to the attempt's host-owned
    /// unknown-effect receipt: no provider effect is attributed, and the row
    /// still goes back on the retry curve. It preserves the thing the in-memory
    /// batch report cannot — that attempt number `n` of this observation was
    /// spent on a terminal the host rejected, and why. Without it a crash
    /// right after the refusal leaves a consumed attempt with no trace of what
    /// consumed it.
    ///
    /// The runtime owns no clock, so the refusal is stamped with the attempt's
    /// own finish instant rather than an invented one.
    #[allow(clippy::too_many_arguments)]
    fn record_refusal(
        &self,
        report: &mut DeliveryBatchReportV1,
        item: &LeasedObservationV1,
        terminal: &TerminalRecord,
        category: AttemptRefusalCategoryV1,
        refused_field: &str,
        expected: Option<String>,
        provided: Option<String>,
        detail: &str,
        started_at_unix_micros: i64,
        finished_at_unix_micros: i64,
    ) -> Result<(), ObservationRuntimeError> {
        // An adapter with a disagreeing clock must not turn a refusal into a
        // lost record, so the window is ordered rather than rejected.
        let finished_at_unix_micros = finished_at_unix_micros.max(started_at_unix_micros);
        let refusal = AttemptRefusalRecordV1 {
            observation_id: item.observation_id.clone(),
            attempt_number: item.attempt_number,
            idempotency_key: item.idempotency_key.clone(),
            provider_id: item.target.provider_id.as_str().to_owned(),
            provider_instance_id: AttemptRefusalRecordV1::bound_text(
                &item.target.provider_instance_id,
            ),
            registration_revision: item.target.registration_revision,
            exact_scope_sha256: item.exact_scope_sha256.clone(),
            category,
            refused_field: AttemptRefusalRecordV1::bound_text(refused_field),
            expected: expected.map(|value| AttemptRefusalRecordV1::bound_text(&value)),
            provided: provided.map(|value| AttemptRefusalRecordV1::bound_text(&value)),
            detail: AttemptRefusalRecordV1::bound_text(detail),
            terminal_operation: terminal.operation().as_wire().to_owned(),
            terminal_code: terminal.terminal_code().as_wire().to_owned(),
            terminal_operation_id: AttemptRefusalRecordV1::bound_text(terminal.operation_id()),
            provider_receipt_digest: AttemptRefusalRecordV1::bound_digest(
                terminal.provider_receipt_sha256(),
            ),
            started_at_unix_micros,
            finished_at_unix_micros,
            recorded_at_unix_micros: finished_at_unix_micros,
        };
        match self.reader.record_attempt_refusal(&refusal)? {
            AttemptRefusalOutcomeV1::Recorded => {
                report.refusals_recorded = report.refusals_recorded.saturating_add(1);
            }
            AttemptRefusalOutcomeV1::AlreadyRecorded => {
                report.duplicate_refusals = report.duplicate_refusals.saturating_add(1);
            }
        }
        Ok(())
    }

    fn record_cancelled(
        &self,
        report: &mut DeliveryBatchReportV1,
        item: &LeasedObservationV1,
        started_at_unix_micros: i64,
        finished_at_unix_micros: i64,
        retry_after_unix_micros: i64,
    ) -> Result<(), ObservationRuntimeError> {
        let receipt = ObservationDeliveryReceiptV1::from_cancelled(
            item,
            started_at_unix_micros,
            finished_at_unix_micros,
        )?;
        self.record_unsettled(report, item, &receipt, retry_after_unix_micros)?;
        Ok(())
    }

    fn record_unsettled(
        &self,
        report: &mut DeliveryBatchReportV1,
        item: &LeasedObservationV1,
        receipt: &ObservationDeliveryReceiptV1,
        retry_after_unix_micros: i64,
    ) -> Result<bool, ObservationRuntimeError> {
        match self.reader.record_unsettled_attempt(
            receipt,
            &item.lease_id,
            retry_after_unix_micros,
        )? {
            AttemptOutcomeV1::Recorded {
                state,
                next_attempt_at_unix_micros,
            } => {
                report.receipts_recorded = report.receipts_recorded.saturating_add(1);
                report.leases_released = report.leases_released.saturating_add(1);
                if state.is_terminal() {
                    report.settled_terminal = report.settled_terminal.saturating_add(1);
                }
                if next_attempt_at_unix_micros.is_some() {
                    report.retry_scheduled = report.retry_scheduled.saturating_add(1);
                }
                return Ok(true);
            }
            AttemptOutcomeV1::DuplicateReceipt { state } => {
                report.duplicate_receipts = report.duplicate_receipts.saturating_add(1);
                if state.is_terminal() {
                    report.settled_terminal = report.settled_terminal.saturating_add(1);
                }
            }
            AttemptOutcomeV1::LeaseLost { .. } => {
                report.leases_lost = report.leases_lost.saturating_add(1);
            }
        }
        Ok(false)
    }

    fn fail_attempt(
        &self,
        report: &mut DeliveryBatchReportV1,
        item: &LeasedObservationV1,
        cause: AdapterFailureV1,
        retry_after_unix_micros: i64,
    ) -> Result<(), ObservationRuntimeError> {
        let lease_released = self.release(&item.lease_id, retry_after_unix_micros)?;
        if lease_released {
            report.leases_released = report.leases_released.saturating_add(1);
        } else {
            report.leases_lost = report.leases_lost.saturating_add(1);
        }
        report.failures.push(DeliveryFailureV1 {
            observation_id: item.observation_id.clone(),
            attempt_number: item.attempt_number,
            lease_released,
            cause,
        });
        Ok(())
    }

    /// Returns `false` when the lease had already lapsed and been reaped, which
    /// is an expected race rather than a failure: some other process already
    /// recovered the row.
    fn release(
        &self,
        lease: &DispatchLeaseIdV1,
        retry_after_unix_micros: i64,
    ) -> Result<bool, ObservationRuntimeError> {
        match self.reader.release_lease(lease, retry_after_unix_micros) {
            Ok(()) => Ok(true),
            Err(ObservationJournalError::UnknownLease { .. }) => Ok(false),
            Err(other) => Err(other.into()),
        }
    }
}

/// Proves a provider terminal describes the delivery it answers.
fn verify_terminal(
    leased: &LeasedObservationV1,
    terminal: &TerminalRecord,
) -> Result<(), TerminalIdentityMismatchV1> {
    if terminal.operation() != ProviderOperation::Observe {
        return Err(TerminalIdentityMismatchV1 {
            field: "operation_kind",
            expected: ProviderOperation::Observe.as_wire().to_owned(),
            provided: terminal.operation().as_wire().to_owned(),
        });
    }
    if terminal.provider_id() != &leased.target.provider_id {
        return Err(TerminalIdentityMismatchV1 {
            field: "provider_id",
            expected: leased.target.provider_id.as_str().to_owned(),
            provided: terminal.provider_id().as_str().to_owned(),
        });
    }
    if terminal.exact_scope_sha256() != leased.exact_scope_sha256 {
        return Err(TerminalIdentityMismatchV1 {
            field: "exact_scope_sha256",
            expected: leased.exact_scope_sha256.clone(),
            provided: terminal.exact_scope_sha256().to_owned(),
        });
    }
    Ok(())
}
