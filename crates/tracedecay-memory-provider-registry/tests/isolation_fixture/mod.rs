//! A host-side bounded-execution boundary for provider calls, shared by the
//! supervision tests.
//!
//! This is the same shape the composition root mounts
//! (`crates/tracedecay/src/daemon/retained_owner/observation_journey.rs`): the
//! provider's call runs on a worker the host can walk away from, the calling
//! thread waits only for the operation's own budget, and a call that outlives
//! it is **abandoned** rather than joined. Tests use it to drive a provider
//! that genuinely never returns.
#![allow(dead_code)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::Duration;

use tracedecay_memory_provider_registry::{
    BoundedCallRefusalV1, BoundedProviderCallV1, CancellationToken, CompositionLifecycleError,
    HandshakeResponse, ProviderHandshakeWorkV1,
};

/// How often the bounded wait re-checks the caller's cancellation.
const CANCELLATION_POLL_MILLIS: u64 = 5;

/// A thread-backed bounded-execution boundary with a finite abandonment
/// ceiling.
#[derive(Debug)]
pub struct ThreadBoundedProviderCallV1 {
    abandoned: AtomicUsize,
    max_abandoned: usize,
}

impl ThreadBoundedProviderCallV1 {
    #[must_use]
    pub const fn new(max_abandoned: usize) -> Self {
        Self {
            abandoned: AtomicUsize::new(0),
            max_abandoned,
        }
    }

    /// Calls still abandoned to a provider that never returned.
    #[must_use]
    pub fn abandoned(&self) -> usize {
        self.abandoned.load(Ordering::Acquire)
    }
}

impl Default for ThreadBoundedProviderCallV1 {
    fn default() -> Self {
        Self::new(8)
    }
}

impl BoundedProviderCallV1 for ThreadBoundedProviderCallV1 {
    fn handshake_within(
        &self,
        budget_millis: u64,
        cancellation: &CancellationToken,
        work: ProviderHandshakeWorkV1,
    ) -> Result<Result<HandshakeResponse, CompositionLifecycleError>, BoundedCallRefusalV1> {
        let abandoned = self.abandoned.load(Ordering::Acquire);
        if abandoned >= self.max_abandoned {
            return Err(BoundedCallRefusalV1::Exhausted {
                abandoned,
                maximum: self.max_abandoned,
            });
        }
        let (answers, inbox) = mpsc::sync_channel(1);
        thread::Builder::new()
            .name("test-bounded-provider-call".to_owned())
            .spawn(move || {
                // A send failure means the caller abandoned this call; the
                // answer is simply dropped.
                let _ = answers.send(work());
            })
            .map_err(|source| BoundedCallRefusalV1::Unavailable(source.to_string()))?;

        let mut remaining = Duration::from_millis(budget_millis);
        let slice = Duration::from_millis(CANCELLATION_POLL_MILLIS);
        loop {
            let wait = remaining.min(slice);
            match inbox.recv_timeout(wait) {
                Ok(answer) => return Ok(answer),
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(BoundedCallRefusalV1::Unavailable(
                        "bounded provider call ended without answering".to_owned(),
                    ));
                }
                Err(RecvTimeoutError::Timeout) => {
                    remaining = remaining.saturating_sub(wait);
                    if cancellation.is_cancelled() {
                        self.abandoned.fetch_add(1, Ordering::AcqRel);
                        return Err(BoundedCallRefusalV1::Cancelled);
                    }
                    if remaining.is_zero() {
                        self.abandoned.fetch_add(1, Ordering::AcqRel);
                        return Err(BoundedCallRefusalV1::Abandoned {
                            waited_millis: budget_millis,
                        });
                    }
                }
            }
        }
    }
}
