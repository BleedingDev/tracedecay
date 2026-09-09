//! Provider identity and state authority supplied to a generic observation mount.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use crate::{OwnedProviderId, ProviderLimits, TerminalCode};

/// State namespace admission belongs to the registered provider boundary.
#[derive(Clone, Debug)]
pub enum ObservationStateNamespacePolicyV1 {
    /// Native state is contained beneath its admitted identity prefix.
    Prefix(String),
    /// The adapter validates the exact-scope namespace in its ready response.
    /// NCM uses its bare SHA-256 namespace under the worker's namespaces root.
    AdapterAttestedExactScope,
}

/// One bounded proof of a provider's global implementation instance.
/// This is not exact-scope readiness; delivery must still obtain that proof
/// for each admitted row through the provider supervisor.
pub trait ObservationInstanceProofV1: std::fmt::Debug + Send + Sync {
    /// Runs on the existing delivery worker, honoring the absolute deadline
    /// and a nonblocking cancellation probe. A refusal is not retried by the
    /// journey; daemon recreation reconstructs the unavailable mount.
    fn prove(
        &self,
        deadline: Instant,
        cancelled: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Result<Option<String>, TerminalCode>;
}

/// Identity and finite limits of one registered observation recipient.
#[derive(Clone, Debug)]
pub struct ObservationProviderMountV1 {
    /// Logical identity declared by the real adapter.
    pub provider_id: OwnedProviderId,
    /// Product-owned registration revision.
    pub registration_revision: u64,
    /// Real provider instance used by durable delivery leases; absent while unavailable.
    pub provider_instance_id: Option<String>,
    /// Optional one-shot proof for a lazily constructed observer.
    /// Native supplies its existing static proved instance instead.
    pub instance_proof: Option<Arc<dyn ObservationInstanceProofV1>>,
    /// Host handshake ceilings for this provider.
    pub host_limits: ProviderLimits,
    /// Host-admitted root immediately containing provider namespaces.
    pub state_root: PathBuf,
    /// Fixed provider-owned journal filename beneath the canonical store root.
    /// Each provider needs independent ingress watermarks and delivery receipts.
    pub journal_file_name: &'static str,
    /// Namespace validation performed before granting state capabilities.
    pub state_namespace_policy: ObservationStateNamespacePolicyV1,
}
