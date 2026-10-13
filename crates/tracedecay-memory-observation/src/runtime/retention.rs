//! Retention: run the journal's bounded sweep on a cadence a caller's loop can
//! drive without owning a clock, a thread, or a tight loop.
//!
//! [`ObservationRetentionPortV1::sweep_expired`] is one bounded pass. Something
//! still has to decide *when* to call it, and that decision has three
//! failure shapes worth ruling out by construction: a sweep that never runs
//! (a mountless retention rule), a sweep that runs on every loop iteration
//! (a checkpoint-and-truncate on every wake), and a sweep that, having failed,
//! is retried immediately forever. [`RetentionSweeperV1`] owns exactly that
//! decision and nothing else.
//!
//! * The first call is due at the instant the sweeper is built, so a mounted
//!   journal is swept as soon as its owner's loop turns for the first time —
//!   including one that a restart found full of aged rows.
//! * A pass that reports remaining candidates *and* made progress is due again
//!   at once, so a backlog converges over consecutive loop turns, each bounded
//!   by the policy's batch size.
//! * A pass that made no progress, or that failed, is not due again before the
//!   schedule's backoff, so neither a store fault nor a candidate the sweep
//!   cannot act on can turn the caller's loop hot.
//!
//! [`ObservationRetentionPortV1::sweep_expired`]: crate::ObservationRetentionPortV1::sweep_expired

use crate::error::ObservationJournalError;
use crate::port::ObservationRetentionPortV1;
use crate::retention::RetentionSweepReceiptV1;

use super::error::ObservationRuntimeError;

/// Cadence bounds for the retention sweep. Explicit: nothing here defaults,
/// and a value of this type is valid by construction — it only exists through
/// [`RetentionSweepScheduleV1::bounded`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetentionSweepScheduleV1 {
    interval_micros: i64,
    error_backoff_micros: i64,
}

impl RetentionSweepScheduleV1 {
    /// Builds a schedule, refusing one that cannot bound anything.
    ///
    /// * `interval_micros` — time between two sweeps when the last one found
    ///   nothing left to do.
    /// * `error_backoff_micros` — time before the next attempt after a sweep
    ///   that failed or that reported remaining candidates without acting on
    ///   any.
    pub const fn bounded(
        interval_micros: i64,
        error_backoff_micros: i64,
    ) -> Result<Self, ObservationJournalError> {
        if interval_micros <= 0 {
            return Err(ObservationJournalError::InvalidSweepSchedule {
                field: "interval_micros",
            });
        }
        if error_backoff_micros <= 0 {
            return Err(ObservationJournalError::InvalidSweepSchedule {
                field: "error_backoff_micros",
            });
        }
        Ok(Self {
            interval_micros,
            error_backoff_micros,
        })
    }

    /// Time between two sweeps when the last one found nothing left to do.
    #[must_use]
    pub const fn interval_micros(&self) -> i64 {
        self.interval_micros
    }

    /// Time before the next attempt after a failed or non-actionable sweep.
    #[must_use]
    pub const fn error_backoff_micros(&self) -> i64 {
        self.error_backoff_micros
    }
}

/// Why a [`RetentionSweeperV1::tick`] did not sweep.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetentionTickV1 {
    /// The next sweep is not due before the carried instant.
    NotDue {
        /// Instant at which the next sweep becomes due.
        next_due_unix_micros: i64,
    },
    /// One bounded sweep ran and produced this receipt.
    Swept {
        /// What the sweep did.
        receipt: RetentionSweepReceiptV1,
        /// Instant at which the next sweep becomes due.
        next_due_unix_micros: i64,
    },
}

/// Drives one journal's bounded retention sweep on a caller-supplied clock.
#[derive(Debug)]
pub struct RetentionSweeperV1<'a, R: ?Sized> {
    port: &'a R,
    schedule: RetentionSweepScheduleV1,
    next_due_unix_micros: i64,
}

impl<'a, R> RetentionSweeperV1<'a, R>
where
    R: ObservationRetentionPortV1 + ?Sized,
{
    /// Binds one retention port and a schedule. The first sweep is due at
    /// `now_unix_micros`.
    #[must_use]
    pub const fn new(
        port: &'a R,
        schedule: RetentionSweepScheduleV1,
        now_unix_micros: i64,
    ) -> Self {
        Self {
            port,
            schedule,
            next_due_unix_micros: now_unix_micros,
        }
    }

    /// Instant at which the next sweep becomes due.
    #[must_use]
    pub const fn next_due_unix_micros(&self) -> i64 {
        self.next_due_unix_micros
    }

    /// Runs one bounded sweep if one is due, and reschedules from its receipt.
    ///
    /// A failed sweep is returned to the caller intact and is not due again
    /// before the schedule's backoff. Nothing is retried here.
    pub fn tick(
        &mut self,
        now_unix_micros: i64,
    ) -> Result<RetentionTickV1, ObservationRuntimeError> {
        if now_unix_micros < self.next_due_unix_micros {
            return Ok(RetentionTickV1::NotDue {
                next_due_unix_micros: self.next_due_unix_micros,
            });
        }
        let budget = self.port.retention_policy().sweep_batch_rows;
        let receipt = match self.port.sweep_expired(now_unix_micros, budget) {
            Ok(receipt) => receipt,
            Err(error) => {
                self.next_due_unix_micros =
                    now_unix_micros.saturating_add(self.schedule.error_backoff_micros);
                return Err(ObservationRuntimeError::Journal(error));
            }
        };
        self.next_due_unix_micros = if receipt.remaining_candidates == 0 {
            now_unix_micros.saturating_add(self.schedule.interval_micros)
        } else if made_progress(&receipt) {
            now_unix_micros
        } else {
            now_unix_micros.saturating_add(self.schedule.error_backoff_micros)
        };
        Ok(RetentionTickV1::Swept {
            receipt,
            next_due_unix_micros: self.next_due_unix_micros,
        })
    }
}

/// Whether a sweep acted on at least one row. A receipt that reports remaining
/// candidates but touched nothing describes work the sweep cannot do right now,
/// not work to be retried on the next loop turn.
const fn made_progress(receipt: &RetentionSweepReceiptV1) -> bool {
    receipt.payloads_purged > 0
        || receipt.deliveries_expired > 0
        || receipt.deliveries_forgotten > 0
        || receipt.journal_rows_deleted > 0
        || receipt.receipts_deleted > 0
        || receipt.withheld_rows_deleted > 0
}
