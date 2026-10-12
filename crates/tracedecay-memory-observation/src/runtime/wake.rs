//! The wake edge between admission and delivery.
//!
//! Admission and delivery are separate concerns with separate transactions, so
//! something has to tell a parked delivery worker that new work landed. This is
//! that something and nothing more: two booleans behind a condition variable.
//!
//! It is deliberately *not* a queue. The journal is the queue — the durable one
//! — and a lost or spurious wake costs a wasted poll, never a lost observation:
//! a worker that never hears the signal still finds the row on its next timed
//! wake, because [`DeliveryWakeV1::wait`] always takes an explicit bound.

use std::sync::{Condvar, Mutex, PoisonError};
use std::time::Duration;

use tracedecay_memory_provider_api::CancellationToken;

/// Why a parked delivery worker woke.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum WakeOutcomeV1 {
    /// Admission signalled new deliverable work.
    Work,
    /// The explicit wait bound elapsed. A worker should still poll: the signal
    /// is an optimisation, the journal is the authority.
    TimedOut,
    /// Shutdown was requested. This outranks pending work, and the pending flag
    /// is left standing so a later worker still sees it.
    ShutdownRequested,
}

#[derive(Debug, Default)]
struct WakeStateV1 {
    pending: bool,
    shutdown: bool,
}

/// The signal admission raises and delivery waits on.
///
/// Shared by reference across the ingress and delivery runtimes; a caller that
/// drives both inline can create one, ignore the waiting, and lose nothing.
///
/// It also carries the one cancellation token every in-flight provider attempt
/// is handed: [`Self::request_shutdown`] cancels it, so a provider blocked
/// inside a delivery observes shutdown through the same control it was given,
/// not only the worker parked between batches.
#[derive(Debug, Default)]
pub struct DeliveryWakeV1 {
    state: Mutex<WakeStateV1>,
    changed: Condvar,
    cancellation: CancellationToken,
}

impl DeliveryWakeV1 {
    /// Creates an unsignalled, running wake handle.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records that deliverable work exists and wakes every waiter.
    ///
    /// Idempotent: repeated signals before a worker wakes collapse into one,
    /// because the flag says "look at the journal", not "here are N items".
    pub fn signal(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.pending = true;
        drop(state);
        self.changed.notify_all();
    }

    /// Requests shutdown, cancels every in-flight attempt's control, and wakes
    /// every waiter immediately.
    pub fn request_shutdown(&self) {
        self.cancellation.cancel();
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.shutdown = true;
        drop(state);
        self.changed.notify_all();
    }

    /// The cancellation token shared by every attempt dispatched under this
    /// wake. Cancelled exactly when shutdown is requested; never before.
    #[must_use]
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Whether shutdown has been requested.
    #[must_use]
    pub fn is_shutdown(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .shutdown
    }

    /// Waits for work, shutdown, or the explicit bound — whichever comes first.
    ///
    /// The bound is mandatory. A delivery worker must come back on its own even
    /// when nothing signals it, because leases lapse and retries become
    /// eligible on a clock the wake handle knows nothing about.
    #[must_use]
    pub fn wait(&self, timeout: Duration) -> WakeOutcomeV1 {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let (mut state, _elapsed) = self
            .changed
            .wait_timeout_while(state, timeout, |state| !state.pending && !state.shutdown)
            .unwrap_or_else(PoisonError::into_inner);
        if state.shutdown {
            return WakeOutcomeV1::ShutdownRequested;
        }
        if state.pending {
            state.pending = false;
            return WakeOutcomeV1::Work;
        }
        WakeOutcomeV1::TimedOut
    }
}
