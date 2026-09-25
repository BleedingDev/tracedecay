//! Declaration fragment for the root-owned provider-control executor module.
//! Merge these declarations with its implementation; this is not an executor stub.

use std::path::PathBuf;
use std::sync::Arc;

use tracedecay_contracts::ResolvedScope;
use tracedecay_domain::{ManifestDigest, ProjectId, UserProfileId};
use tracedecay_global_db::RegisteredGlobalDbLeaseV1;
use tracedecay_memory_provider_registry::ProjectMemoryProviderComposition;
use tracedecay_runtime_core::db::Database;

use super::observation_journey::ProjectObservationJourneyV1;

pub(crate) mod authority;

/// Actual mounted project authorities, assembled once during full project open.
pub(crate) struct ProviderControlMountInputsV1 {
    pub(crate) authority: Option<Arc<authority::ProviderControlAuthorityV1>>,
    pub(crate) composition: Arc<ProjectMemoryProviderComposition>,
    pub(crate) journeys: Vec<Arc<ProjectObservationJourneyV1>>,
    pub(crate) profile_id: UserProfileId,
    pub(crate) mounted_scope: ResolvedScope,
    pub(crate) authoritative_project_id: ProjectId,
    pub(crate) project_root: PathBuf,
    pub(crate) configuration_digest: ManifestDigest,
    pub(crate) canonical_session_db: RegisteredGlobalDbLeaseV1,
    pub(crate) canonical_dispositions: Arc<Database>,
}

// Required root-owned constructor signature (implementation supplied separately):
// pub(crate) fn project_provider_control_port(
//     inputs: ProviderControlMountInputsV1,
// ) -> Arc<dyn tracedecay_contracts::retained_surfaces::RetainedProviderControlExecutionPortV1>
