//! Composition-root assembly of retained session, memory, LCM, and automation
//! owners. Implementations live in the owner crates; this module only selects
//! their native inputs and mounts the retained surface.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::DaemonInvocationService;
use tracedecay_contracts::retained_surfaces::{
    FactStoreCurateRequestV1, MemoryScopeV1, RetainedAutomationExecutionPortV1,
    RetainedProjectSelectorV1, RetainedSurfaceExecutionContextV1, RetainedSurfaceExecutionFutureV1,
};
use tracedecay_contracts::{
    RetainedMemoryExecutionPortV1, RetainedSurfaceExecutionErrorV1, RetainedSurfacePortsV1,
};
use tracedecay_domain::{FactOwnerV1, ManifestDigest, ProjectId};
use tracedecay_session_runtime::retained::{
    ProjectRetainedSessionAuthoritiesV1, RetainedSessionRefreshPortV1, map_execution_error,
};
use tracedecay_store_runtime::retained_memory::{
    MemoryTargetAccessV1, RetainedMemoryTargetAuthorityV1, RetainedMemoryTargetV1,
};

use tracedecay_project::project::TraceDecay;

mod retained_curator;
pub use retained_curator::execute_retained_memory_curator;

#[cfg(feature = "memory-provider-host")]
pub(crate) mod cognitive_recall;
#[cfg(feature = "memory-provider-host")]
pub use cognitive_recall::CognitiveRecallMountError;
#[cfg(all(feature = "memory-provider-host", feature = "test-helpers"))]
pub use cognitive_recall::test_context_evidence;
#[cfg(all(test, feature = "memory-provider-host"))]
#[path = "retained_owner/native_common_factory_tests.rs"]
mod native_common_factory_tests;
#[cfg(feature = "memory-provider-host")]
pub(crate) mod native_provider;
#[cfg(all(test, feature = "memory-provider-host"))]
#[path = "retained_owner/native_provider_parity_tests.rs"]
mod native_provider_parity_tests;
#[cfg(feature = "memory-provider-host")]
pub(crate) mod native_staged_observations;
#[cfg(feature = "memory-provider-host")]
pub(crate) mod observation_journey;
#[cfg(feature = "memory-provider-host")]
pub(crate) mod provider_control;
#[cfg(feature = "memory-provider-host")]
pub(crate) mod provider_history;

#[cfg(feature = "memory-provider-host")]
pub(crate) use tracedecay_session_memory::memory_mapping;

/// Builds the host-owned Native observation metadata used by the retained
/// observation journey and its provider fixtures. This small constructor used
/// to live in the binary composition root; keeping it beside the Native owner
/// means the daemon-service crate never has to reach back through the binary
/// composition root.
#[cfg(feature = "memory-provider-host")]
pub(crate) fn native_observation_mount(
    data_root: &Path,
    registration_revision: u64,
) -> tracedecay_domain::errors::Result<
    tracedecay_memory_provider_registry::ObservationProviderMountV1,
> {
    use tracedecay_domain::errors::TraceDecayError;
    use tracedecay_memory_provider_registry::{
        NATIVE_PROVIDER_ID, ObservationProviderMountV1, ObservationStateNamespacePolicyV1,
        OwnedProviderId,
    };

    Ok(ObservationProviderMountV1 {
        provider_id: OwnedProviderId::new(NATIVE_PROVIDER_ID).map_err(|error| {
            TraceDecayError::Config {
                message: format!("invalid Native observation identity: {error}"),
            }
        })?,
        registration_revision,
        provider_instance_id: Some(native_provider::PROVIDER_INSTANCE_ID.to_owned()),
        instance_proof: None,
        host_limits: native_provider::native_provider_limits(),
        state_root: data_root.join(observation_journey::PROVIDER_STATE_DIR_NAME),
        journal_file_name: "memory-observation-journal-v1.sqlite3",
        state_namespace_policy: ObservationStateNamespacePolicyV1::Prefix(
            NATIVE_PROVIDER_ID.to_owned(),
        ),
    })
}

/// Returns the registration revision declared by this daemon-service
/// composition for a provider. Test evidence uses this declaration only to
/// build a health request; the observed health reply remains authoritative.
#[cfg(all(feature = "memory-provider-host", feature = "test-helpers"))]
pub(crate) fn declared_project_provider_registration_revision_for_test(
    provider_id: &str,
) -> Option<u64> {
    match provider_id {
        tracedecay_memory_provider_registry::NATIVE_PROVIDER_ID => Some(1),
        tracedecay_memory_provider_ncm::NCM_PROVIDER_ID => Some(1),
        _ => None,
    }
}

#[cfg(test)]
mod memory_target_journeys;
#[cfg(test)]
mod profile_refresh_journeys;
#[cfg(test)]
mod session_retained_effect_tests;

/// Exact authorities used by independently mounted project retained families.
/// A missing session or LCM authority cannot prevent memory from registering.
#[derive(Clone)]
pub struct ProductionRetainedAuthoritiesV1 {
    pub cg: Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
    pub project_root: PathBuf,
    pub project_id: tracedecay_domain::ProjectId,
    pub mounted_profile_id: Option<tracedecay_domain::UserProfileId>,
    pub mounted_session_store_id: Option<tracedecay_session_memory::context::SessionStoreId>,
    pub mounted_session_root_id: Option<tracedecay_session_memory::context::SessionRootId>,
    pub registered_session_db: Option<tracedecay_global_db::RegisteredGlobalDbLeaseV1>,
    pub project_refresh: Option<Arc<dyn RetainedSessionRefreshPortV1>>,
    pub project_retrieval: Option<
        Arc<dyn tracedecay_session_runtime::session_retrieval::SessionApplicationRetrievalPortV1>,
    >,
    pub project_workflow_index: Option<Arc<dyn tracedecay_sessions::WorkflowIndexReadPort>>,
    pub project_lcm:
        Option<Arc<dyn tracedecay_session_runtime::lcm_authority::MountedLcmAuthorityPort>>,
    #[cfg(feature = "memory-provider-host")]
    pub provider_control: Option<
        Arc<dyn tracedecay_contracts::retained_surfaces::RetainedProviderControlExecutionPortV1>,
    >,
    pub configuration_digest: ManifestDigest,
    pub invocation_service: Option<DaemonInvocationService>,
}

fn served_store_identity(
    cg: &TraceDecay,
) -> Result<(PathBuf, ProjectId, bool), RetainedSurfaceExecutionErrorV1> {
    match cg.project_memory_owner() {
        Ok(FactOwnerV1::Project { project_id }) => Ok((
            cg.project_root().to_path_buf(),
            project_id,
            cg.is_read_only(),
        )),
        Ok(FactOwnerV1::Profile) => Err(RetainedSurfaceExecutionErrorV1::NotFoundOrNotAuthorized),
        Err(error) => Err(map_execution_error(error)),
    }
}

pub async fn live_retained_memory_authority(
    cg: &tokio::sync::RwLock<Arc<TraceDecay>>,
    mounted_project_id: &ProjectId,
    mounted_project_root: &Path,
) -> Result<RetainedMemoryTargetAuthorityV1, RetainedSurfaceExecutionErrorV1> {
    let graph = cg.read().await;
    let (served_project_root, store_layout_project_id, graph_read_only) =
        served_store_identity(graph.as_ref())?;
    Ok(RetainedMemoryTargetAuthorityV1 {
        registry: graph.retained_store_runtime_registry(),
        profile_database: graph.profile_database().clone(),
        project_root: mounted_project_root.to_path_buf(),
        project_id: mounted_project_id.clone(),
        store_layout_project_id,
        served_project_root,
        graph_read_only,
    })
}

struct AssembledRetainedMemory {
    cg: Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
    mounted_project_id: ProjectId,
    mounted_project_root: PathBuf,
    configuration_digest: ManifestDigest,
}

impl RetainedMemoryExecutionPortV1 for AssembledRetainedMemory {
    fn execute_memory<'a>(
        &'a self,
        context: RetainedSurfaceExecutionContextV1<'a>,
        request: tracedecay_contracts::RetainedMemoryRequestV1<'a>,
    ) -> RetainedSurfaceExecutionFutureV1<'a> {
        Box::pin(async move {
            let authority = live_retained_memory_authority(
                self.cg.as_ref(),
                &self.mounted_project_id,
                &self.mounted_project_root,
            )
            .await?;
            tracedecay_store_runtime::retained_memory::DirectRetainedMemoryPortV1::project(
                authority,
                self.configuration_digest.clone(),
            )
            .execute_request(context, request)
            .await
        })
    }
}

pub fn retained_surface_ports(
    authorities: ProductionRetainedAuthoritiesV1,
) -> Arc<RetainedSurfacePortsV1<'static>> {
    let mut ports = RetainedSurfacePortsV1::default();
    ports = ports.with_memory(Arc::new(AssembledRetainedMemory {
        cg: Arc::clone(&authorities.cg),
        mounted_project_id: authorities.project_id.clone(),
        mounted_project_root: authorities.project_root.clone(),
        configuration_digest: authorities.configuration_digest.clone(),
    }));
    #[cfg(feature = "memory-provider-host")]
    if let Some(provider_control) = authorities.provider_control {
        ports = ports.with_provider_control(provider_control);
    }
    if let Some(invocation_service) = authorities.invocation_service.clone() {
        ports = ports.with_automation(Arc::new(AssembledRetainedAutomation {
            cg: Arc::clone(&authorities.cg),
            invocation_service,
        }));
    }
    if let (
        Some(profile_id),
        Some(session_store_id),
        Some(session_root_id),
        Some(refresh),
        Some(retrieval),
        Some(session_database),
        Some(workflow_index),
    ) = (
        authorities.mounted_profile_id,
        authorities.mounted_session_store_id,
        authorities.mounted_session_root_id,
        authorities.project_refresh,
        authorities.project_retrieval.clone(),
        authorities.registered_session_db,
        authorities.project_workflow_index,
    ) {
        ports = ports.with_session(Arc::new(
            tracedecay_session_runtime::retained::DirectRetainedSessionPortV1::project(
                ProjectRetainedSessionAuthoritiesV1 {
                    project_root: authorities.project_root,
                    project_id: authorities.project_id,
                    profile_id,
                    session_store_id,
                    session_root_id,
                    configuration_digest: authorities.configuration_digest,
                    refresh,
                    retrieval,
                    session_database,
                    workflow_index,
                },
            ),
        ));
    }
    if let (Some(authority), Some(retrieval)) =
        (authorities.project_lcm, authorities.project_retrieval)
    {
        ports = ports.with_lcm(Arc::new(
            tracedecay_session_runtime::retained::DirectRetainedLcmPortV1::project(
                authority, retrieval,
            ),
        ));
    }
    Arc::new(ports)
}

/// Single `RetainedAutomationExecutionPortV1` impl at the composition root.
/// The curator still requires the selected `TraceDecay` lock inside
/// `dashboard_automation`; this type forwards that already-selected runtime
/// and the invocation service. It is not a compatibility rename of
/// `DirectRetainedAutomationPortV1`.
struct AssembledRetainedAutomation {
    cg: Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
    invocation_service: DaemonInvocationService,
}

impl RetainedAutomationExecutionPortV1 for AssembledRetainedAutomation {
    fn execute_fact_store_curate<'a>(
        &'a self,
        context: RetainedSurfaceExecutionContextV1<'a>,
        request: &'a FactStoreCurateRequestV1,
    ) -> RetainedSurfaceExecutionFutureV1<'a> {
        Box::pin(async move {
            let cg = self.cg.read().await.clone();
            hotpath::future!(
                execute_retained_memory_curator(
                    cg.as_ref(),
                    &self.invocation_service,
                    &context,
                    request
                ),
                label = "daemon.retained.automation.curate"
            )
            .await
        })
    }
}

pub async fn open_project_retained_memory_target(
    cg: &TraceDecay,
    registered_root: &Path,
    admitted_project_id: &ProjectId,
    memory_scope: Option<MemoryScopeV1>,
    selector: Option<&RetainedProjectSelectorV1>,
    access: MemoryTargetAccessV1,
) -> Result<RetainedMemoryTargetV1<'static>, RetainedSurfaceExecutionErrorV1> {
    let (served_project_root, store_layout_project_id, graph_read_only) =
        served_store_identity(cg)?;
    let authority = RetainedMemoryTargetAuthorityV1 {
        registry: cg.retained_store_runtime_registry(),
        profile_database: cg.profile_database().clone(),
        project_root: cg.project_root().to_path_buf(),
        project_id: admitted_project_id.clone(),
        store_layout_project_id,
        served_project_root,
        graph_read_only,
    };
    tracedecay_store_runtime::retained_memory::open_project_retained_memory_target(
        &authority,
        registered_root,
        admitted_project_id,
        memory_scope,
        selector,
        access,
    )
    .await
}

// ---------------------------------------------------------------------------
// Project memory-provider host composition
// ---------------------------------------------------------------------------
//
// The daemon binary owns the project-open state machine and the NCM worker
// owner.  The retained owner crate owns the provider-neutral assembly below.
// Keeping these handles opaque is deliberate: the binary can publish and
// retain the result without importing the child retained-owner modules, while
// the service crate never reaches back into the binary composition root.

#[cfg(feature = "memory-provider-host")]
pub type NcmRegistrationFactoryV1 = Arc<
    dyn Fn(
            tracedecay_domain::UserProfileId,
            PathBuf,
            PathBuf,
            u64,
            tracedecay_memory_provider_registry::EnabledProviderMode,
            Option<Arc<dyn tracedecay_memory_provider_registry::AdvisoryAdmissionAuthority>>,
        ) -> std::result::Result<
            (
                tracedecay_memory_provider_registry::ProviderRegistrationV1,
                tracedecay_memory_provider_registry::ObservationProviderMountV1,
            ),
            String,
        > + Send
        + Sync,
>;

/// Test-only decoration of the production Native application port.
#[cfg(feature = "memory-provider-host")]
pub type NativeApplicationPortInterpositionV1 = Arc<
    dyn Fn(
            Arc<dyn tracedecay_memory_provider_registry::NativeMemoryApplicationPort>,
        ) -> Arc<dyn tracedecay_memory_provider_registry::NativeMemoryApplicationPort>
        + Send
        + Sync,
>;

/// Inputs needed to compose one project's provider registrations.
#[cfg(feature = "memory-provider-host")]
pub struct ProjectMemoryProviderHostInputsV1 {
    /// Validated participation resolved from the pinned project configuration.
    pub activation: tracedecay_domain::configuration::MemoryProviderSelectionV1,
    /// The configured NCM worker, if NCM participation is enabled.
    pub ncm_observer: tracedecay_domain::configuration::MemoryProviderNcmObserverV1,
    /// The graph authority the Native application port reads.
    pub graph: Arc<TraceDecay>,
    /// Canonical checkout root served by this project route.
    pub canonical_project_path: PathBuf,
    /// Profile identity bound to the route.
    pub profile_id: tracedecay_domain::UserProfileId,
    /// Exact scope published by the code-index authority.
    pub scope: tracedecay_contracts::ResolvedScope,
    /// Authoritative project identity checked against `scope`.
    pub authoritative_project_id: tracedecay_domain::ProjectId,
    /// Store-owned data root used for provider journals and the recall ledger.
    pub store_data_root: PathBuf,
    /// Pinned routing gate used to build the active recall policy.
    pub recall_routing: tracedecay_domain::configuration::MemoryProviderRecallRoutingV1,
    /// NCM registration is supplied by the daemon composition root because it
    /// owns the worker-generation slot and the concrete worker topology.
    pub ncm_registration_factory: Option<NcmRegistrationFactoryV1>,
    /// Optional test decoration of the real Native port.
    #[cfg(feature = "memory-provider-host")]
    pub native_port_interposition: Option<NativeApplicationPortInterpositionV1>,
}

/// Opaque provider host retained by one project-server generation.
#[cfg(feature = "memory-provider-host")]
pub struct ProjectMemoryProviderHostMountV1 {
    composition: Arc<tracedecay_memory_provider_registry::ProjectMemoryProviderComposition>,
    observation_provider_mounts: Vec<(
        tracedecay_memory_provider_registry::ConfiguredObservationProviderMountV1,
        Arc<provider_history::ProviderHistoryAuthorityMountV1>,
    )>,
    cognitive_recall_mount: Option<Arc<ProjectCognitiveRecallMountV1>>,
    locator_key: cognitive_recall::control_attribution::RecallLocatorKeyV1,
}

#[cfg(feature = "memory-provider-host")]
impl ProjectMemoryProviderHostMountV1 {
    /// Returns the provider-neutral composition retained by this project.
    #[must_use]
    pub fn composition(
        &self,
    ) -> Arc<tracedecay_memory_provider_registry::ProjectMemoryProviderComposition> {
        Arc::clone(&self.composition)
    }

    /// Borrows the composed registry, or `None` for the inert disabled value.
    #[must_use]
    pub fn registry(
        &self,
    ) -> Option<&tracedecay_memory_provider_registry::ProjectMemoryProviderRegistry> {
        self.composition.registry()
    }

    /// The mounted recall route, when the configuration selected an active
    /// provider. The route remains owned by this same host generation.
    pub fn cognitive_recall_mount(&self) -> Option<Arc<ProjectCognitiveRecallMountV1>> {
        self.cognitive_recall_mount.as_ref().map(Arc::clone)
    }
}

/// Opaque observation journey retained by a full project server.
#[cfg(feature = "memory-provider-host")]
pub struct ProjectObservationJourneyMountV1 {
    inner: Arc<observation_journey::ProjectObservationJourneyV1>,
}

#[cfg(feature = "memory-provider-host")]
impl ProjectObservationJourneyMountV1 {
    /// Shuts down the already-owned observation worker inside the caller's
    /// deadline and returns typed failures as display-safe text.
    pub async fn shutdown(&self, deadline: tokio::time::Instant) -> Vec<String> {
        self.inner
            .shutdown(deadline)
            .await
            .into_iter()
            .map(|failure| failure.to_string())
            .collect()
    }
}

/// Opaque cognitive recall route retained by a project server.
#[cfg(feature = "memory-provider-host")]
pub struct ProjectCognitiveRecallMountV1 {
    inner: Arc<cognitive_recall::ProjectCognitiveRecallMountV1>,
}

#[cfg(feature = "memory-provider-host")]
impl ProjectCognitiveRecallMountV1 {
    /// Mints a recall port bound to this route's exact project scope.
    pub fn port_for_session(
        &self,
        canonical_session_id: &str,
    ) -> std::result::Result<
        tracedecay_memory_provider_registry::ProjectCognitiveRecallPortV1,
        CognitiveRecallMountError,
    > {
        self.inner.port_for_session(canonical_session_id)
    }

    pub(crate) fn inner(&self) -> Arc<cognitive_recall::ProjectCognitiveRecallMountV1> {
        Arc::clone(&self.inner)
    }
}

/// Inputs needed to mount the full observation and provider-control owners.
#[cfg(feature = "memory-provider-host")]
pub struct ProjectMemoryProviderFullMountInputsV1 {
    /// The same graph opened for the route.
    pub graph: Arc<TraceDecay>,
    /// Canonical checkout root served by the route.
    pub canonical_project_path: PathBuf,
    /// Profile identity bound to the route.
    pub profile_id: tracedecay_domain::UserProfileId,
    /// Brain identity used by the existing hook admission ledger.
    pub brain_id: tracedecay_domain::BrainId,
    /// Exact scope published by code-index admission.
    pub scope: tracedecay_contracts::ResolvedScope,
    /// Authoritative project identity checked against the scope.
    pub authoritative_project_id: tracedecay_domain::ProjectId,
    /// Canonical project session database lease.
    pub session_db: tracedecay_global_db::RegisteredGlobalDbLeaseV1,
    /// Pinned configuration digest retained by provider-control operations.
    pub configuration_digest: ManifestDigest,
}

/// Full provider-owner bundle retained after the core server is replaced.
#[cfg(feature = "memory-provider-host")]
pub struct ProjectMemoryProviderFullMountV1 {
    observation_journeys: Vec<Arc<ProjectObservationJourneyMountV1>>,
    deferred_observation_journeys: std::sync::Mutex<
        Vec<(
            Arc<observation_journey::ProjectObservationJourneyV1>,
            tracedecay_memory_provider_registry::ObservationMountRequirementV1,
        )>,
    >,
    provider_control_mount:
        Arc<dyn tracedecay_contracts::retained_surfaces::RetainedProviderControlExecutionPortV1>,
}

#[cfg(feature = "memory-provider-host")]
impl ProjectMemoryProviderFullMountV1 {
    /// Returns the retained observation journeys for the MCP server lifetime.
    #[must_use]
    pub fn observation_journeys(&self) -> Vec<Arc<ProjectObservationJourneyMountV1>> {
        self.observation_journeys.iter().map(Arc::clone).collect()
    }

    /// Returns the retained provider-control surface.
    #[must_use]
    pub fn provider_control_mount(
        &self,
    ) -> Arc<dyn tracedecay_contracts::retained_surfaces::RetainedProviderControlExecutionPortV1>
    {
        Arc::clone(&self.provider_control_mount)
    }

    /// Activates dormant journeys after the full MCP server is reachable.
    pub async fn activate_after_publication(
        &self,
        observation_store: tracedecay_global_db::GlobalDbObservationStore,
    ) -> std::result::Result<(), String> {
        let deferred = self
            .deferred_observation_journeys
            .lock()
            .map_err(|_| "provider observation activation state was poisoned".to_owned())?
            .drain(..)
            .collect::<Vec<_>>();
        for (journey, requirement) in deferred {
            if let Err(error) = journey.start_observer_with_live_replay(observation_store.clone()) {
                if requirement
                    == tracedecay_memory_provider_registry::ObservationMountRequirementV1::Required
                {
                    return Err(format!(
                        "required memory observation activation failed after full publication: {error}"
                    ));
                }
                tracing::warn!(
                    event = "memory_observation_optional_start_failed",
                    error = ?error,
                    journal = %journey.journal_path().display(),
                    "optional observer could not start after full host publication"
                );
            }
        }
        Ok(())
    }
}

/// Compose the provider registrations and the optional recall route for one
/// project. The concrete NCM constructor remains a closure supplied by the
/// daemon root, so service composition has no dependency cycle.
#[cfg(feature = "memory-provider-host")]
pub async fn mount_project_memory_provider_host(
    inputs: ProjectMemoryProviderHostInputsV1,
) -> std::result::Result<Arc<ProjectMemoryProviderHostMountV1>, String> {
    use tracedecay_domain::configuration::{MemoryProviderKindV1, MemoryProviderParticipationV1};
    use tracedecay_memory_provider_registry::{
        ConfiguredObservationProviderMountV1, EnabledProviderMode, FabricConfig,
        NATIVE_RECALL_SCOPE_BINDINGS, NativeProvider, ObservationMountActivationV1,
        ObservationMountRequirementV1, ProjectMemoryProviderComposition, ProviderExecutionShapeV1,
        ProviderRegistrationV1, RecallScopeBindingsV1, SelectedProviderActivationV1,
    };

    if inputs.activation.is_disabled() {
        let locator_key = new_recall_locator_key()?;
        return Ok(Arc::new(ProjectMemoryProviderHostMountV1 {
            composition: Arc::new(ProjectMemoryProviderComposition::Disabled),
            observation_provider_mounts: Vec::new(),
            cognitive_recall_mount: None,
            locator_key,
        }));
    }

    let mut selected = None;
    let mut observers = Vec::new();
    let mut observation_provider_mounts = Vec::new();
    for (kind, participation) in [
        (MemoryProviderKindV1::Native, inputs.activation.native),
        (MemoryProviderKindV1::Ncm, inputs.activation.ncm),
    ] {
        let mode = match participation {
            MemoryProviderParticipationV1::Disabled => continue,
            MemoryProviderParticipationV1::Observer => EnabledProviderMode::Observer,
            MemoryProviderParticipationV1::Active => EnabledProviderMode::Active,
        };
        let requirement = if mode == EnabledProviderMode::Active
            || (kind == MemoryProviderKindV1::Native
                && inputs.activation.active_provider().is_none())
        {
            ObservationMountRequirementV1::Required
        } else {
            ObservationMountRequirementV1::Optional
        };
        let history_mount = Arc::new(provider_history::ProviderHistoryAuthorityMountV1::default());
        let admission_authority: Arc<
            dyn tracedecay_memory_provider_registry::AdvisoryAdmissionAuthority,
        > = history_mount.clone();
        let constructed: std::result::Result<
            (ProviderRegistrationV1, ConfiguredObservationProviderMountV1),
            String,
        > = match kind {
            MemoryProviderKindV1::Native => async {
                let graph_cell = Arc::new(tokio::sync::RwLock::new(Arc::clone(&inputs.graph)));
                let provider_state_root = inputs
                    .graph
                    .store_layout()
                    .data_root
                    .join(observation_journey::PROVIDER_STATE_DIR_NAME);
                let port = native_provider::project_native_memory_application_port_with_authority_off_runtime(
                    graph_cell,
                    inputs.canonical_project_path.clone(),
                    inputs.profile_id.clone(),
                    provider_state_root,
                    admission_authority,
                )
                .await
                .map_err(|error| format!("could not construct project Native application port: {error}"))?;
                let port = match inputs.native_port_interposition.as_ref() {
                    Some(interpose) => interpose(port),
                    None => port,
                };
                let provider =
                    Arc::new(NativeProvider::new(port).map_err(|error| {
                        format!("could not construct Native provider: {error}")
                    })?);
                let provider_id =
                    tracedecay_memory_provider_registry::OwnedProviderId::new(kind.provider_id())
                        .map_err(|error| error.to_string())?;
                let registration = ProviderRegistrationV1 {
                    provider_id,
                    provider,
                    registration_revision: 1,
                    mode,
                    execution_shape: ProviderExecutionShapeV1::HostAuthoredInProcess,
                    recall_scope_bindings: RecallScopeBindingsV1::from_wire(
                        NATIVE_RECALL_SCOPE_BINDINGS.iter().copied(),
                    )
                    .map_err(|error| error.to_string())?,
                    lifecycle: ProviderLifecycleOwnershipV1::CompositionBound,
                };
                let mount = ConfiguredObservationProviderMountV1 {
                    mount: native_observation_mount(&inputs.graph.store_layout().data_root, 1)
                        .map_err(|error| error.to_string())?,
                    requirement,
                    activation: if requirement == ObservationMountRequirementV1::Required {
                        ObservationMountActivationV1::BeforePublication
                    } else {
                        ObservationMountActivationV1::AfterPublication
                    },
                };
                Ok((registration, mount))
            }
            .await,
            MemoryProviderKindV1::Ncm => {
                let factory = inputs
                    .ncm_registration_factory
                    .clone()
                    .ok_or_else(|| "NCM composition factory was not supplied".to_owned())?;
                let tracedecay_domain::configuration::MemoryProviderNcmObserverV1::Enabled {
                    worker_binary,
                    state_root,
                } = inputs.ncm_observer.clone()
                else {
                    return Err("selected NCM participation is disabled".to_owned());
                };
                let profile_id = inputs.profile_id.clone();
                let registration = tokio::task::spawn_blocking(move || {
                    factory(
                        profile_id,
                        worker_binary,
                        state_root,
                        1,
                        mode,
                        Some(admission_authority),
                    )
                })
                .await
                .map_err(|error| format!("NCM construction task unavailable: {error}"))??;
                Ok((
                    registration.0,
                    ConfiguredObservationProviderMountV1 {
                        mount: registration.1,
                        requirement,
                        activation: ObservationMountActivationV1::AfterPublication,
                    },
                ))
            }
        };
        match constructed {
            Ok((registration, mount)) => {
                if mode == EnabledProviderMode::Active {
                    selected = Some(registration);
                } else {
                    observers.push(registration);
                }
                observation_provider_mounts.push((mount, history_mount));
            }
            Err(error) if requirement == ObservationMountRequirementV1::Optional => {
                tracing::warn!(
                    provider = kind.provider_id(),
                    error = %error,
                    "optional memory observer unavailable before registration"
                );
            }
            Err(error) => return Err(error),
        }
    }

    let composition = Arc::new(
        ProjectMemoryProviderComposition::compose_registered(
            match selected {
                Some(registration) => SelectedProviderActivationV1::Injected {
                    fabric_config: FabricConfig {
                        max_registered_providers: observers.len() + 1,
                        max_in_flight: 1,
                    },
                    registration,
                },
                None => SelectedProviderActivationV1::ObserversOnly {
                    fabric_config: FabricConfig {
                        max_registered_providers: observers.len(),
                        max_in_flight: 1,
                    },
                },
            },
            observers,
        )
        .map_err(|error| format!("could not compose project memory-provider host: {error}"))?,
    );

    let locator_key = new_recall_locator_key()?;
    let cognitive_recall_mount = match composition
        .registry()
        .and_then(|registry| registry.selected_registration())
    {
        Some(registration)
            if registration.mode == EnabledProviderMode::Active
                && inputs.scope.project_id == inputs.authoritative_project_id =>
        {
            let routing = project_recall_routing_policy(
                Some((
                    &registration.provider_id,
                    registration.registration_revision,
                )),
                &inputs.recall_routing,
            )?;
            let mount = cognitive_recall::mount_project_cognitive_recall(
                cognitive_recall::CognitiveRecallMountInputsV1 {
                    composition: Arc::clone(&composition),
                    profile_id: inputs.profile_id.clone(),
                    scope: inputs.scope.clone(),
                    authoritative_project_id: inputs.authoritative_project_id.clone(),
                    store_data_root: inputs.store_data_root.clone(),
                    canonical_project_path: inputs.canonical_project_path.clone(),
                    graph: Arc::clone(&inputs.graph),
                    routing,
                    host_limits: registration.limits,
                    invocation_boundary: cognitive_recall::host_provider_invocation_boundary(1),
                    locator_key: locator_key.clone(),
                },
            )
            .map_err(|error| format!("could not mount project cognitive recall route: {error}"))?;
            Some(Arc::new(ProjectCognitiveRecallMountV1 { inner: mount }))
        }
        Some(_) => {
            return Err(
                "active memory provider composition disagrees with the authoritative project scope"
                    .to_owned(),
            );
        }
        None => None,
    };

    Ok(Arc::new(ProjectMemoryProviderHostMountV1 {
        composition,
        observation_provider_mounts,
        cognitive_recall_mount,
        locator_key,
    }))
}

/// Inputs and owner assembly for the full project server. This function is
/// called only after the project session database has been admitted.
#[cfg(feature = "memory-provider-host")]
pub async fn mount_project_memory_provider_full(
    host: &Arc<ProjectMemoryProviderHostMountV1>,
    inputs: ProjectMemoryProviderFullMountInputsV1,
    cancellation: &tracedecay_runtime_core::cancellation::CancellationToken,
) -> std::result::Result<Arc<ProjectMemoryProviderFullMountV1>, String> {
    use tracedecay_memory_provider_registry::ObservationMountRequirementV1;

    let hook_origin_reader = Arc::new(provider_history::HookOriginReaderV1::new(
        inputs.graph.hook_store_layout().data_root.clone(),
        inputs.brain_id.clone(),
        inputs.profile_id.clone(),
    ));
    let mut journeys = Vec::with_capacity(host.observation_provider_mounts.len());
    let mut deferred = Vec::new();
    let mut control_journals = Vec::new();
    for (configured, history_mount) in &host.observation_provider_mounts {
        if cancellation.is_cancelled() {
            return Err("project open was cancelled during provider observation mount".to_owned());
        }
        let provider = &configured.mount;
        let required = configured.requirement == ObservationMountRequirementV1::Required;
        let journey_inputs = observation_journey::ObservationJourneyMountInputsV1 {
            composition: Arc::clone(&host.composition),
            profile_id: inputs.profile_id.clone(),
            scope: inputs.scope.clone(),
            authoritative_project_id: inputs.authoritative_project_id.clone(),
            store_data_root: inputs.graph.store_layout().data_root.clone(),
            provider: provider.clone(),
            policy: observation_journey::ObservationJourneyPolicyV1::project_default(),
        };
        let journey =
            match observation_journey::mount_observer_dormant(journey_inputs, cancellation).await {
                Ok(journey) => journey,
                Err(error @ observation_journey::ObservationJourneyError::Cancelled { .. }) => {
                    return Err(error.to_string());
                }
                Err(error) if required => {
                    return Err(format!(
                        "could not mount project observation journey: {error}"
                    ));
                }
                Err(error) => {
                    tracing::warn!(
                        provider = provider.provider_id.as_str(),
                        error = %error,
                        "observer journey unavailable"
                    );
                    continue;
                }
            };
        let original_authority = match provider_history::HistoryIdentityBridgeV1::admit(
            &inputs.canonical_project_path,
            &inputs.profile_id,
            &inputs.scope,
            &inputs.session_db.binding().shard_id,
        ) {
            Ok(bridge) => Some(Arc::new(
                provider_history::MountedOriginalObservationAuthorityV1 {
                    reader: Arc::clone(&hook_origin_reader),
                    bridge: Arc::new(bridge),
                },
            )),
            Err(error)
                if matches!(
                    &error,
                    provider_history::ProviderHistoryErrorV1::Unavailable(
                        "repository marker"
                            | "current repository capture"
                            | "canonical repository identity"
                    )
                ) =>
            {
                tracing::debug!(
                    provider = provider.provider_id.as_str(),
                    error = %error,
                    "provider history unavailable without original repository evidence"
                );
                None
            }
            Err(error) => {
                return Err(format!("provider history mount refused: {error}"));
            }
        };
        let authority = Arc::new(provider_history::ProviderHistoryAuthorityV1 {
            mounted_scope: inputs.scope.clone(),
            profile_id: inputs.profile_id.clone(),
            registered_shard: inputs.session_db.binding().shard_id.clone(),
            observations: Arc::new(inputs.session_db.observation_store()),
            dispositions: Arc::new(inputs.graph.db().clone()),
            original_authority,
            journal: journey.history_journal(),
            provider_id: provider.provider_id.clone(),
            policy_revision: 1,
            runtime: tokio::runtime::Handle::current(),
        });
        authority
            .validate_mount()
            .map_err(|error| format!("provider history mount refused: {error}"))?;
        history_mount
            .bind(authority.clone())
            .map_err(|error| format!("provider history binding refused: {error}"))?;
        journey
            .bind_history_authority(authority.clone())
            .map_err(|error| format!("provider journey history binding refused: {error}"))?;
        if let Some(recall) = host
            .cognitive_recall_mount
            .as_ref()
            .filter(|recall| recall.inner.routing().active_provider() == &provider.provider_id)
        {
            recall
                .inner
                .bind_selected_history(authority, Arc::clone(&journey))
                .map_err(|error| format!("provider recall history binding refused: {error}"))?;
        }
        if configured.activation
            == tracedecay_memory_provider_registry::ObservationMountActivationV1::BeforePublication
        {
            observation_journey::activate_required_with_startup_replay(
                Arc::clone(&journey),
                inputs.session_db.observation_store(),
                cancellation,
            )
            .await
            .map_err(|error| format!("provider observation startup replay failed: {error}"))?;
        } else {
            deferred.push((Arc::clone(&journey), configured.requirement));
        }
        control_journals.push((provider.provider_id.clone(), journey.history_journal()));
        journeys.push(Arc::new(ProjectObservationJourneyMountV1 {
            inner: journey,
        }));
    }

    let now = tracedecay_contracts::now_micros().0;
    let control = tracedecay_memory_provider_registry::OperationControl::new(
        now.saturating_add(1_000_000),
        1_000,
        tracedecay_memory_provider_registry::CancellationToken::new(),
    );
    let control_inputs = provider_control::authority::ProviderControlAuthorityInputsV1 {
        canonical_project_path: inputs.canonical_project_path.clone(),
        profile_id: inputs.profile_id.clone(),
        mounted_scope: inputs.scope.clone(),
        session_db: inputs.session_db.clone(),
        dispositions: Arc::new(inputs.graph.db().clone()),
        hook_origin_reader,
        store_data_root: inputs.graph.store_layout().data_root.clone(),
        live_ledger: host
            .cognitive_recall_mount
            .as_ref()
            .map(|mount| mount.inner.control_ledger()),
        locator_key: host.locator_key.clone(),
        live_journals: control_journals,
        runtime: tokio::runtime::Handle::current(),
    };
    let blocking_control = control.clone();
    let mut work = tokio::task::spawn_blocking(move || {
        provider_control::authority::ProviderControlAuthorityV1::from_mounted_data(
            control_inputs,
            &blocking_control,
        )
    });
    let joined = tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            control.cancellation().cancel();
            let _ = work.await;
            return Err("project open was cancelled during provider control mount".to_owned());
        }
        joined = &mut work => joined,
    };
    let authority = if control.snapshot().is_err() {
        tracing::debug!(
            cause = "admission_budget",
            "provider source controls unavailable"
        );
        None
    } else {
        match joined {
            Ok(Ok(authority)) => Some(Arc::new(authority)),
            Ok(Err(error)) => {
                tracing::debug!(cause = "retained_authority", error = %error, "provider source controls unavailable");
                None
            }
            Err(error) => {
                tracing::debug!(cause = "mount_task", error = %error, "provider source controls unavailable");
                None
            }
        }
    };
    let provider_control_mount = provider_control::project_provider_control_port(
        provider_control::ProviderControlMountInputsV1 {
            authority,
            composition: Arc::clone(&host.composition),
            journeys: journeys
                .iter()
                .map(|journey| Arc::clone(&journey.inner))
                .collect(),
            profile_id: inputs.profile_id,
            mounted_scope: inputs.scope,
            authoritative_project_id: inputs.authoritative_project_id,
            project_root: inputs.canonical_project_path,
            configuration_digest: inputs.configuration_digest,
            canonical_session_db: inputs.session_db.clone(),
            canonical_dispositions: Arc::new(inputs.graph.db().clone()),
        },
    );
    Ok(Arc::new(ProjectMemoryProviderFullMountV1 {
        observation_journeys: journeys,
        deferred_observation_journeys: std::sync::Mutex::new(deferred),
        provider_control_mount,
    }))
}

#[cfg(feature = "memory-provider-host")]
fn new_recall_locator_key()
-> std::result::Result<cognitive_recall::control_attribution::RecallLocatorKeyV1, String> {
    let mut bytes = vec![0_u8; 32];
    getrandom::getrandom(&mut bytes).map_err(|error| {
        format!("OS entropy unavailable for the project recall locator key: {error}")
    })?;
    cognitive_recall::control_attribution::RecallLocatorKeyV1::from_material(bytes)
        .map_err(|error| format!("project recall locator key was rejected: {error}"))
}

#[cfg(feature = "memory-provider-host")]
fn project_recall_degradation_rule(
    routing: &tracedecay_domain::configuration::MemoryProviderRecallRoutingV1,
) -> std::result::Result<tracedecay_memory_provider_registry::DegradationRule, String> {
    use tracedecay_memory_provider_registry::{
        DegradationCause, DegradationRule, PinnedDegradationPolicy,
    };
    let Some(configured) = &routing.degradation else {
        return Ok(DegradationRule::DefaultContentFree);
    };
    let policy = PinnedDegradationPolicy::new(
        configured.policy_id.clone(),
        configured.policy_revision,
        configured
            .allowed_causes
            .iter()
            .copied()
            .map(|cause| match cause {
                tracedecay_domain::configuration::MemoryProviderRecallDegradationCauseV1::Unsupported => DegradationCause::Unsupported,
                tracedecay_domain::configuration::MemoryProviderRecallDegradationCauseV1::Unavailable => DegradationCause::Unavailable,
                tracedecay_domain::configuration::MemoryProviderRecallDegradationCauseV1::Cancelled => DegradationCause::Cancelled,
                tracedecay_domain::configuration::MemoryProviderRecallDegradationCauseV1::TimedOut => DegradationCause::TimedOut,
                tracedecay_domain::configuration::MemoryProviderRecallDegradationCauseV1::Partial => DegradationCause::Partial,
                tracedecay_domain::configuration::MemoryProviderRecallDegradationCauseV1::Stale => DegradationCause::Stale,
                tracedecay_domain::configuration::MemoryProviderRecallDegradationCauseV1::BudgetExhausted => DegradationCause::BudgetExhausted,
            }),
    )
    .map_err(|error| format!("memory provider recall degradation policy is invalid: {error}"))?;
    Ok(DegradationRule::ExplicitPinned(policy))
}

#[cfg(feature = "memory-provider-host")]
fn project_recall_routing_policy(
    selected: Option<(&tracedecay_memory_provider_registry::OwnedProviderId, u64)>,
    routing: &tracedecay_domain::configuration::MemoryProviderRecallRoutingV1,
) -> std::result::Result<tracedecay_memory_provider_registry::ActiveRoutingPolicy, String> {
    use tracedecay_memory_provider_registry::{ActiveRoutingPolicy, FallbackRule};
    let Some((provider_id, registration_revision)) = selected else {
        return Err("active recall route has no selected provider".to_owned());
    };
    let fallback = match &routing.fallback {
        None => FallbackRule::Forbidden,
        Some(rule) => {
            return Err(format!(
                "memory provider recall routing pins fallback policy '{}'@{} to target provider '{}', but this project composition registers only the selected provider; remove fallback",
                rule.policy_id, rule.policy_revision, rule.target_provider
            ));
        }
    };
    ActiveRoutingPolicy::new_with_degradation(
        provider_id.clone(),
        registration_revision,
        fallback,
        project_recall_degradation_rule(routing)?,
    )
    .map_err(|error| format!("memory provider recall routing policy is invalid: {error}"))
}
