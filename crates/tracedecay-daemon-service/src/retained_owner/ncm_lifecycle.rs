//! Lifecycle authority over the daemon's existing NCM worker owner.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tracedecay_memory_provider_ncm::RustNcmWorkerOwner;
use tracedecay_memory_provider_registry::{
    ProviderLifecycleOwnerErrorV1, ProviderLifecycleOwnerV1,
};

/// Bounded lifecycle adapter over the same worker used by the NCM surface.
/// It controls the actual child process rather than its host wrapper thread.
pub struct NcmLifecycleOwner(Arc<RustNcmWorkerOwner>);

impl NcmLifecycleOwner {
    /// Retains the existing worker owner without starting another process.
    #[must_use]
    pub fn new(worker: Arc<RustNcmWorkerOwner>) -> Self {
        Self(worker)
    }
}

fn lifecycle_deadline(deadline_unix_micros: i64) -> Result<Instant, ProviderLifecycleOwnerErrorV1> {
    let remaining = deadline_unix_micros
        .checked_sub(tracedecay_contracts::now_micros().0)
        .filter(|remaining| *remaining > 0)
        .ok_or(ProviderLifecycleOwnerErrorV1::DeadlineElapsed)?;
    Instant::now()
        .checked_add(Duration::from_micros(remaining as u64))
        .ok_or(ProviderLifecycleOwnerErrorV1::DeadlineElapsed)
}

fn owner_result<T>(
    result: Result<T, impl std::fmt::Display>,
    deadline: Instant,
) -> Result<T, ProviderLifecycleOwnerErrorV1> {
    result.map_err(|error| {
        if Instant::now() >= deadline {
            ProviderLifecycleOwnerErrorV1::DeadlineElapsed
        } else {
            ProviderLifecycleOwnerErrorV1::Unavailable(error.to_string())
        }
    })
}

impl ProviderLifecycleOwnerV1 for NcmLifecycleOwner {
    fn start(&self, deadline_unix_micros: i64) -> Result<(), ProviderLifecycleOwnerErrorV1> {
        let deadline = lifecycle_deadline(deadline_unix_micros)?;
        owner_result(self.0.start(deadline), deadline)
    }

    fn request_stop(
        &self,
        deadline_unix_micros: i64,
    ) -> Result<bool, ProviderLifecycleOwnerErrorV1> {
        let deadline = lifecycle_deadline(deadline_unix_micros)?;
        owner_result(self.0.request_stop(deadline), deadline)
    }

    fn kill(&self, deadline_unix_micros: i64) -> Result<(), ProviderLifecycleOwnerErrorV1> {
        let deadline = lifecycle_deadline(deadline_unix_micros)?;
        owner_result(self.0.kill(deadline), deadline)
    }
}
