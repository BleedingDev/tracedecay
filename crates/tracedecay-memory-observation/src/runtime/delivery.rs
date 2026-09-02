//! Delivery: lease, dispatch the exact stored bytes, record what came back,
//! return what did not, reap what lapsed, and stop within an explicit bound.
//!
//! # The one thing this runtime refuses to do
//!
//! It never invents an outcome. A provider that answered produces a receipt
//! derived from its own terminal record; a provider that did not answer
//! produces no receipt at all, only a released lease. That asymmetry is the
//! whole at-least-once story: an attempt whose acknowledgement was lost between
//! the provider's commit and this process's write is *not* recorded as failed —
//! it is redelivered, and the provider recognises the content-derived
//! idempotency key and answers `duplicate_acknowledged`.
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
//! A cancelled attempt is never recorded as an outcome: the provider did not
//! answer, so the lease is released and the row stays redeliverable.

use tracedecay_memory_provider_api::{CancellationToken, ProviderOperation, TerminalRecord};

use crate::error::ObservationJournalError;
use crate::identity::{DispatchLeaseIdV1, ObservationIdV1};
use crate::inspection::JournalInspectionFilterV1;
use crate::lease::{AttemptOutcomeV1, LeaseRequestV1, LeasedObservationV1};
use crate::port::ObservationJournalReaderV1;
use crate::receipt::ObservationDeliveryReceiptV1;
use crate::state::DeliveryStateV1;

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
    Unanswered {
        /// When the row becomes eligible again. Explicit: the runtime holds no
        /// clock and invents no backoff.
        retry_after_unix_micros: i64,
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
    /// When a lease released because the *adapter itself* failed becomes
    /// eligible again. Explicit for the same reason as
    /// [`DeliveryAttemptV1::Unanswered`]: nothing here owns a clock.
    pub retry_after_unix_micros: i64,
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
    /// Attempts that came back without a receipt after shutdown had cancelled
    /// their control. The provider did not answer, so nothing is recorded and
    /// the lease was released.
    pub cancelled_in_flight: u32,
    /// Attempts that produced no receipt because an adapter failed.
    pub failures: Vec<DeliveryFailureV1>,
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
                    if control.is_cancelled() {
                        report.cancelled_in_flight = report.cancelled_in_flight.saturating_add(1);
                    }
                    self.fail_attempt(
                        &mut report,
                        &item,
                        AdapterFailureV1::new(cause),
                        request.retry_after_unix_micros,
                    )?;
                    continue;
                }
            };

            match attempt {
                DeliveryAttemptV1::Unanswered {
                    retry_after_unix_micros,
                } => {
                    if control.is_cancelled() {
                        report.cancelled_in_flight = report.cancelled_in_flight.saturating_add(1);
                    }
                    if self.release(&item.lease_id, retry_after_unix_micros)? {
                        report.leases_released = report.leases_released.saturating_add(1);
                    } else {
                        report.leases_lost = report.leases_lost.saturating_add(1);
                    }
                }
                DeliveryAttemptV1::Answered {
                    terminal,
                    started_at_unix_micros,
                    finished_at_unix_micros,
                } => {
                    if let Err(mismatch) = verify_terminal(&item, &terminal) {
                        self.fail_attempt(
                            &mut report,
                            &item,
                            AdapterFailureV1::new(mismatch),
                            request.retry_after_unix_micros,
                        )?;
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
                            self.fail_attempt(
                                &mut report,
                                &item,
                                AdapterFailureV1::new(cause),
                                request.retry_after_unix_micros,
                            )?;
                            continue;
                        }
                    };
                    self.record(&mut report, &receipt)?;
                }
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
