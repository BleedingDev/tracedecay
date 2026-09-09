//! Real NCM adapter construction at the daemon composition boundary.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tracedecay_domain::UserProfileId;

use tracedecay_memory_provider_ncm::{
    NcmCognitiveSurface, NcmProviderAdapter, RustNcmConfig, RustNcmSurface, RustNcmWorkerOwner,
    StateRoot, WorkerOptions,
};
use tracedecay_memory_provider_registry::{
    ObservationInstanceProofV1, ObservationProviderMountV1, ObservationStateNamespacePolicyV1,
    ObserverProviderRegistration,
};

/// Construction failed before an NCM registration could be mounted.
#[derive(Debug, thiserror::Error)]
pub(in crate::daemon) enum NcmObserverConstructionError {
    /// A daemon may not create another worker under conflicting profile/config authority.
    #[error(
        "NCM worker {field} conflicts with this daemon's admitted owner; restart with one profile/configuration"
    )]
    OwnerBindingConflict { field: &'static str },
    /// Poisoned daemon ownership cannot be replaced with another process.
    #[error("NCM worker owner slot is unavailable")]
    OwnerSlotUnavailable,
    /// Configured state root was not admitted.
    #[error("NCM observer state root is invalid: {0}")]
    StateRoot(String),
    /// The real worker could not be constructed or report coherent identity.
    #[error("NCM observer worker is unavailable: {0}")]
    Worker(#[from] tracedecay_memory_provider_ncm::RustNcmError),
    /// The real surface declared an invalid adapter identity.
    #[error("NCM observer adapter is invalid: {0}")]
    Adapter(#[from] tracedecay_memory_provider_ncm::NcmAdapterError),
}

/// One strong daemon-generation owner. The empty slot starts no worker thread,
/// child, or model; only explicitly enabled project composition acquires it.
#[derive(Default)]
pub(in crate::daemon) struct NcmWorkerOwnerSlot {
    binding: Mutex<Option<NcmWorkerOwnerBinding>>,
}

struct NcmWorkerOwnerBinding {
    profile_id: UserProfileId,
    worker_binary: PathBuf,
    state_root: PathBuf,
    worker: Arc<RustNcmWorkerOwner>,
}

impl NcmWorkerOwnerSlot {
    /// Retains exactly one profile/configured owner for this daemon generation.
    /// Paths are the admitted values, never resolved against ambient HOME/CWD.
    fn acquire(
        &self,
        profile_id: &UserProfileId,
        worker_binary: &Path,
        state_root: &Path,
    ) -> Result<Arc<RustNcmWorkerOwner>, NcmObserverConstructionError> {
        let mut slot = self
            .binding
            .lock()
            .map_err(|_| NcmObserverConstructionError::OwnerSlotUnavailable)?;
        if let Some(binding) = slot.as_ref() {
            let conflict = if &binding.profile_id != profile_id {
                Some("profile")
            } else if binding.worker_binary.as_os_str() != worker_binary.as_os_str() {
                Some("worker_binary")
            } else if binding.state_root.as_os_str() != state_root.as_os_str() {
                Some("state_root")
            } else {
                None
            };
            if let Some(field) = conflict {
                return Err(NcmObserverConstructionError::OwnerBindingConflict { field });
            }
            return Ok(Arc::clone(&binding.worker));
        }
        let admitted = StateRoot::new(state_root.to_path_buf())
            .map_err(NcmObserverConstructionError::StateRoot)?;
        // Only the lazy client owner is created under this lock. The journey's
        // delivery thread later proves the instance through its serialized mailbox.
        let worker = Arc::new(RustNcmWorkerOwner::new(RustNcmConfig {
            worker_binary: worker_binary.to_path_buf(),
            state_root: admitted,
            worker_options: WorkerOptions::default(),
        })?);
        *slot = Some(NcmWorkerOwnerBinding {
            profile_id: profile_id.clone(),
            worker_binary: worker_binary.to_path_buf(),
            state_root: state_root.to_path_buf(),
            worker: Arc::clone(&worker),
        });
        Ok(worker)
    }
}

struct NcmInstanceProof(Arc<RustNcmSurface>);

impl std::fmt::Debug for NcmInstanceProof {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("NcmInstanceProof")
    }
}

impl ObservationInstanceProofV1 for NcmInstanceProof {
    fn prove(
        &self,
        deadline: std::time::Instant,
        cancelled: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Result<Option<String>, tracedecay_memory_provider_registry::TerminalCode> {
        self.0.prove_provider_instance(deadline, cancelled)
    }
}

/// Builds only the real worker-backed NCM adapter, always as an observer.
///
/// Construction declares pinned identity without starting the worker. The
/// retained delivery thread proves it later; encoder unavailability remains
/// typed not-ready. No synthetic encoder or
/// active recall registration can be selected through this constructor.
pub(in crate::daemon) fn construct_ncm_observer(
    owners: &NcmWorkerOwnerSlot,
    profile_id: &UserProfileId,
    worker_binary: PathBuf,
    state_root: PathBuf,
    registration_revision: u64,
) -> Result<(ObserverProviderRegistration, ObservationProviderMountV1), NcmObserverConstructionError>
{
    let worker = owners.acquire(profile_id, &worker_binary, &state_root)?;
    let surface = Arc::new(RustNcmSurface::from_production_worker(worker)?);
    let provider_instance_id = surface.provider_instance_id()?;
    let descriptor = surface.descriptor();
    let instance_proof = Some(
        Arc::new(NcmInstanceProof(Arc::clone(&surface))) as Arc<dyn ObservationInstanceProofV1>
    );
    let provider = Arc::new(NcmProviderAdapter::new(surface)?);
    let mount = ObservationProviderMountV1 {
        provider_id: descriptor.provider_id,
        registration_revision,
        provider_instance_id,
        instance_proof,
        host_limits: descriptor.limits,
        state_root: state_root.join("namespaces"),
        journal_file_name: "memory-observation-ncm-journal-v1.sqlite3",
        state_namespace_policy: ObservationStateNamespacePolicyV1::AdapterAttestedExactScope,
    };
    Ok((
        ObserverProviderRegistration {
            provider,
            registration_revision,
        },
        mount,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::thread;
    use std::time::{Duration, Instant};

    use super::*;

    #[test]
    fn daemon_slot_reuses_one_strong_owner_and_refuses_conflicting_bindings() {
        let temp = tempfile::TempDir::new().unwrap();
        let worker = temp.path().join("absent-worker");
        let root = temp.path().join("state");
        let profile = UserProfileId::new("profile.shared-ncm-worker").unwrap();
        let slot = NcmWorkerOwnerSlot::default();
        assert!(
            slot.binding.lock().unwrap().is_none(),
            "disabled slot is empty"
        );
        let barrier = Barrier::new(4);
        let owners = thread::scope(|threads| {
            let calls = (0..4)
                .map(|_| {
                    threads.spawn(|| {
                        barrier.wait();
                        slot.acquire(&profile, &worker, &root).unwrap()
                    })
                })
                .collect::<Vec<_>>();
            calls
                .into_iter()
                .map(|call| call.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert!(owners.iter().all(|owner| Arc::ptr_eq(owner, &owners[0])));
        assert!(
            owners[0].worker_pid().is_none(),
            "acquisition alone is lazy"
        );
        for (other_profile, other_worker, other_root, expected_field) in [
            (
                UserProfileId::new("profile.other").unwrap(),
                worker.clone(),
                root.clone(),
                "profile",
            ),
            (
                profile.clone(),
                temp.path().join("other-worker"),
                root.clone(),
                "worker_binary",
            ),
            (
                profile.clone(),
                worker.clone(),
                temp.path().join("other-root"),
                "state_root",
            ),
            (
                profile.clone(),
                worker.clone(),
                root.join("."),
                "state_root",
            ),
        ] {
            assert!(
                matches!(slot.acquire(&other_profile, &other_worker, &other_root),
                Err(NcmObserverConstructionError::OwnerBindingConflict { field }) if field == expected_field)
            );
            assert!(Arc::ptr_eq(
                &slot.acquire(&profile, &worker, &root).unwrap(),
                &owners[0]
            ));
        }
        let witness = Arc::downgrade(&owners[0]);
        drop(owners);
        assert!(
            witness.upgrade().is_some(),
            "closing every project keeps the daemon owner"
        );
        let started = Instant::now();
        drop(slot);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "lazy owner teardown is bounded"
        );
        assert!(witness.upgrade().is_none());
    }

    #[test]
    fn lazy_observer_keeps_the_bound_owner_without_starting_a_child() {
        let temp = tempfile::TempDir::new().unwrap();
        let worker = temp.path().join("absent-worker");
        let root = temp.path().join("state");
        let profile = UserProfileId::new("profile.unavailable-shared-ncm").unwrap();
        let slot = NcmWorkerOwnerSlot::default();
        let (_, first) =
            construct_ncm_observer(&slot, &profile, worker.clone(), root.clone(), 1).unwrap();
        assert!(first.provider_instance_id.is_none());
        assert!(first.instance_proof.is_some());
        let owner = slot.acquire(&profile, &worker, &root).unwrap();
        assert!(owner.worker_pid().is_none());
        let (_, second) =
            construct_ncm_observer(&slot, &profile, worker.clone(), root.clone(), 1).unwrap();
        assert!(second.provider_instance_id.is_none());
        assert!(Arc::ptr_eq(
            &owner,
            &slot.acquire(&profile, &worker, &root).unwrap()
        ));
        assert!(matches!(
            construct_ncm_observer(
                &slot,
                &profile,
                temp.path().join("replacement-worker"),
                root,
                1
            ),
            Err(NcmObserverConstructionError::OwnerBindingConflict {
                field: "worker_binary"
            })
        ));
    }

    #[test]
    fn invocation_clones_share_the_daemon_ncm_owner_slot() {
        let invocation = crate::daemon::DaemonInvocationState::default();
        let cloned = invocation.clone();
        assert!(Arc::ptr_eq(
            &invocation.ncm_worker_owner,
            &cloned.ncm_worker_owner
        ));
        assert!(
            invocation
                .ncm_worker_owner
                .binding
                .lock()
                .unwrap()
                .is_none()
        );
    }
}
