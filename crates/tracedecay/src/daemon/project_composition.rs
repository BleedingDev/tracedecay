//! Production project composition: the wiring that builds one project's MCP
//! server from its store runtime, schedulers, and authority ports.
//!
//! `production_project_server` is the single composition root shared by the
//! Unix broker, the portable broker, and the in-process test harness.

use super::*;
use tracedecay_code_index_runtime::code_index_scheduler;
use tracedecay_daemon_identity::profile_identity;
use tracedecay_daemon_service::daemon_owned_project_source_access_at;
use tracedecay_runtime_core::logging::log_daemon_event;
use tracedecay_session_runtime::session_sync::DaemonSessionSyncConfig;
use tracedecay_session_runtime::session_temporal_refresh_scheduler::{
    ProfileSessionHistoricalIngestor, ProjectSessionHistoricalIngestor,
};

mod code_index_activation;
#[cfg(test)]
mod future_size_tests;
#[cfg(feature = "memory-provider-host")]
mod ncm_observer;
mod runtime;
mod session_database_admission;
use code_index_activation::{
    CodeIndexActivationMountInputs, code_index_activation_hint_sink, code_index_activation_mount,
    code_index_freshness_probe_sink, code_index_hook_sink, code_index_reconcile_sink,
};
#[cfg(feature = "memory-provider-host")]
pub(super) use ncm_observer::NcmWorkerOwnerSlot;
pub(in crate::daemon) use runtime::ProductionProjectCompositionRuntime;
use runtime::bind_verified_project_graph_runtime;
use session_database_admission::{join_independent_session_opens, log_session_database_admission};

/// Commit points in the full publication transaction. The test-only failure
/// injector can stop immediately after any one of these points and assert
/// that the candidate never becomes reachable and that retrying starts clean.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum ProjectOpenFailurePhase {
    SessionDatabases = 1,
    ProviderMount = 2,
    McpConstructed = 3,
    GitTransactions = 4,
    IndependentOwners = 5,
    DependentOwners = 6,
    ProviderActivated = 7,
    RuntimeReady = 8,
    RegistryPublished = 9,
    HttpMounted = 10,
}

#[cfg(test)]
static PROJECT_OPEN_FAILURE_AFTER_PHASE: std::sync::atomic::AtomicU8 =
    std::sync::atomic::AtomicU8::new(0);
#[cfg(test)]
static PROJECT_OPEN_REACHED_PHASE: std::sync::atomic::AtomicU8 =
    std::sync::atomic::AtomicU8::new(0);
#[cfg(test)]
static PROJECT_OPEN_PHASE_CHANGED: tokio::sync::Notify = tokio::sync::Notify::const_new();

/// Arm a deterministic full-publication failure for the next matching phase.
/// This is intentionally process-local and test-only: production behavior has
/// no injected branch and every phase checkpoint compiles to a no-op.
#[cfg(test)]
pub(super) fn fail_project_open_after(phase: ProjectOpenFailurePhase) {
    PROJECT_OPEN_REACHED_PHASE.store(0, Ordering::Release);
    PROJECT_OPEN_FAILURE_AFTER_PHASE.store(phase as u8, Ordering::Release);
}

#[cfg(test)]
pub(super) fn clear_project_open_failure() {
    PROJECT_OPEN_FAILURE_AFTER_PHASE.store(0, Ordering::Release);
    PROJECT_OPEN_REACHED_PHASE.store(0, Ordering::Release);
}

#[cfg(test)]
pub(super) async fn wait_for_project_open_phase(phase: ProjectOpenFailurePhase) {
    loop {
        if PROJECT_OPEN_REACHED_PHASE.load(Ordering::Acquire) == phase as u8 {
            return;
        }
        PROJECT_OPEN_PHASE_CHANGED.notified().await;
    }
}

/// Independent provider participation resolved from the pinned project
/// configuration. The daemon owns the decision; the service crate only
/// receives the validated value and assembles neutral retained handles.
#[cfg(feature = "memory-provider-host")]
pub(super) type ProjectMemoryProviderActivation =
    tracedecay_domain::configuration::MemoryProviderSelectionV1;

#[cfg(all(test, feature = "memory-provider-host"))]
pub(super) use tracedecay_daemon_service::retained_owner::NativeApplicationPortInterpositionV1;

/// How one project open obtains its provider participation.
#[cfg(feature = "memory-provider-host")]
#[derive(Clone)]
pub(super) enum ProjectMemoryProviderActivationSelector {
    /// Read the authoritative runtime configuration for this open.
    FromRuntimeConfiguration,
    /// Read the same configuration, then decorate the real Native port in a
    /// test harness before the service registers it.
    #[cfg(all(test, feature = "memory-provider-host"))]
    FromRuntimeConfigurationWithNativePortInterposition(NativeApplicationPortInterpositionV1),
}

#[cfg(feature = "memory-provider-host")]
impl std::fmt::Debug for ProjectMemoryProviderActivationSelector {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FromRuntimeConfiguration => formatter.write_str("FromRuntimeConfiguration"),
            #[cfg(all(test, feature = "memory-provider-host"))]
            Self::FromRuntimeConfigurationWithNativePortInterposition(_) => {
                formatter.write_str("FromRuntimeConfigurationWithNativePortInterposition(..)")
            }
        }
    }
}

#[cfg(feature = "memory-provider-host")]
impl ProjectMemoryProviderActivationSelector {
    fn resolve(
        self,
        runtime_configuration: &tracedecay_configuration::config::PinnedRuntimeConfiguration,
    ) -> Result<ProjectMemoryProviderActivation> {
        let config = runtime_configuration.config();
        tracedecay_domain::configuration::MemoryProviderSelectionV1::resolve(
            config.memory_provider_native_enabled,
            &config.memory_provider_ncm_observer,
            &config.memory_provider_recall_routing,
        )
        .map_err(|error| TraceDecayError::Config {
            message: error.to_string(),
        })
    }

    #[cfg(all(test, feature = "memory-provider-host"))]
    fn native_application_port_interposition(
        &self,
    ) -> Option<NativeApplicationPortInterpositionV1> {
        match self {
            Self::FromRuntimeConfiguration => None,
            Self::FromRuntimeConfigurationWithNativePortInterposition(interposition) => {
                Some(Arc::clone(interposition))
            }
        }
    }
}

pub(super) struct ProductionProjectComposition {
    #[cfg(unix)]
    pub(super) key: ProjectServerKey,
    pub(super) canonical_project_path: PathBuf,
    pub(super) server: Arc<crate::mcp::McpServer>,
    #[cfg(unix)]
    pub(super) inserted: bool,
}

pub(super) fn project_server_response_lifecycle_has_in_flight(
    lifecycle: &crate::mcp::server::ProjectServerResponseLifecycle,
) -> bool {
    Arc::clone(lifecycle.response_gate())
        .try_write_owned()
        .is_err()
}

fn project_server_has_in_flight_response(server: &Arc<crate::mcp::McpServer>) -> bool {
    let lifecycle = server.project_server_response_lifecycle();
    Arc::strong_count(server) > 1 || project_server_response_lifecycle_has_in_flight(&lifecycle)
}

#[hotpath::measure(label = "daemon.project.compose.release_idle", future = true)]
#[expect(
    clippy::too_many_lines,
    reason = "Idle-server release is one cache-evict-and-shutdown before the next project open."
)]
async fn release_one_idle_project_server_before_open(
    store_administration: &StoreAdministration,
    invocation: &DaemonInvocationState,
    capacity_gate: Arc<ProjectOpenGate>,
    capacity_admission: tokio::sync::OwnedMutexGuard<()>,
) -> Result<tokio::sync::OwnedMutexGuard<()>> {
    let runtime_registry = store_administration.session_runtime_registry().await?;
    // The route cache and invocation schedulers have independent bounds. Retire
    // the whole idle owner before either fills: evicting only its MCP server
    // leaves the code-index worker holding its scheduler slot.
    let project_server_cache_saturated = store_administration
        .project_servers()
        .lock()
        .await
        .servers
        .len()
        >= MAX_CACHED_PROJECT_SERVERS;
    let graph_admission_available = runtime_registry.has_project_graph_admission_capacity()?;
    if graph_admission_available && !project_server_cache_saturated {
        return Ok(capacity_admission);
    }
    if let Some(error) = store_administration
        .completed_capacity_retirement_failure()
        .await
    {
        return Err(TraceDecayError::Config {
            message: format!(
                "a prior project server retirement failed before capacity reuse: {error}"
            ),
        });
    }
    let profile_identity = store_administration.profile_identity()?.clone();
    let mut retirement_admission = store_administration
        .acquire_project_server_retirement_admission()
        .await;
    let victim = {
        let mut servers = store_administration.project_servers().lock().await;
        servers.retire_lru_ready_under_graph_pressure(project_server_has_in_flight_response)
    };
    let victim = match victim {
        Ok(victim) => victim,
        // Nothing is idle enough to retire. That is terminal only when graph
        // admission itself is exhausted; a merely saturated project-server
        // cache still has the bounded route-cache eviction (and the
        // already-cached-key fast path) behind it, so leave that decision to
        // the bind below rather than refusing an open this path was not
        // entered to refuse.
        Err(()) if graph_admission_available => return Ok(capacity_admission),
        Err(()) => return Err(project_server_capacity_error()),
    };
    let Some((retired_owner, retired_servers)) = victim else {
        return Ok(capacity_admission);
    };
    let prior_owner_retirements = retirement_admission.prior_completions_for_owner(&retired_owner);
    for (_, server) in &retired_servers {
        server.revoke_project_server_responses();
    }
    let project_roots = retired_servers
        .iter()
        .map(|(key, _)| key.project_root.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let mut hook_data_roots = std::collections::BTreeSet::new();
    for (_, server) in &retired_servers {
        let graph = server.cg().await;
        hook_data_roots.insert(graph.hook_store_layout().data_root.clone());
    }
    let retired_servers = retired_servers
        .into_iter()
        .map(|(_, server)| server)
        .collect::<Vec<_>>();
    let retired_server_count = retired_servers.len();
    let retirement_administration = store_administration.clone();
    let retirement_invocation = invocation.clone();
    let completion =
        retirement_admission.spawn_and_track_fallible(retired_owner.clone(), async move {
            let _capacity_admission = capacity_admission;
            retirement_administration
                .session_temporal_refresh_schedulers()
                .retire_project(&retired_owner)
                .await;
            super::project_server_lifecycle::retire_project_servers(retired_servers, None).await;
            for data_root in hook_data_roots {
                super::hook_v2_replay_consumer::shutdown_hook_v2_replay_consumer(&data_root).await;
            }
            for prior in prior_owner_retirements {
                prior.wait().await?;
            }
            let project_id =
                retired_owner
                    .project_id
                    .clone()
                    .ok_or_else(|| TraceDecayError::Config {
                        message:
                            "retired project server omitted its authoritative project identity"
                                .to_owned(),
                    })?;
            let project_id = tracedecay_domain::ProjectId::new(project_id).map_err(|error| {
                TraceDecayError::Config {
                    message: format!("retired project server identity is invalid: {error}"),
                }
            })?;
            super::branch_admin::retire_registered_context_scout_owner(
                &project_id,
                &retired_owner.graph_db_path,
            );
            let runtime_quiescence = retirement_invocation
                .quiesce_project_runtime_owners(
                    profile_identity.profile_id(),
                    &project_id,
                    &project_roots,
                )
                .await?;
            let project_sessions_path = retired_owner
                .store_root
                .join(tracedecay_runtime_core::storage::SESSIONS_DB_FILENAME);
            retirement_administration
                .git_index_transaction_services()
                .retire_project_database(&project_id, &project_sessions_path)
                .await
                .map_err(|error| TraceDecayError::Config {
                    message: format!(
                        "could not retire project Git transaction actors before capacity reuse: {error}"
                    ),
                })?;
            retirement_administration
                .native_integration_services()
                .retire_project_database(&project_id, &project_sessions_path)
                .await
                .map_err(|error| TraceDecayError::Config {
                    message: format!(
                        "could not retire project native integration actors before capacity reuse: {error}"
                    ),
                })?;
            retirement_administration
                .session_sync_service()
                .retire_project(profile_identity.profile_id(), &project_id)
                .await
                .map_err(|error| TraceDecayError::Config {
                    message: format!(
                        "could not retire project session sync before capacity reuse: {error}"
                    ),
                })?;
            let telemetry_sampling = retirement_administration.store_telemetry_sampling();
            telemetry_sampling.release_retained_handle(&project_sessions_path);
            telemetry_sampling.release_retained_handle(&retired_owner.graph_db_path);
            runtime_registry
                .retire_project_session_relation_graph(&project_id)
                .await?;
            runtime_registry
                .retire_project_memory_graph(&project_id)
                .await?;
            runtime_registry
                .drop_project_runtime_caches(&project_id)
                .await;
            drop(runtime_quiescence);
            Ok(())
        });
    hotpath::gauge!("project_servers").inc(-(retired_server_count as f64));
    drop(retirement_admission);
    completion.wait().await?;
    let capacity_admission = Arc::clone(&capacity_gate).lock_owned().await;
    if !store_administration
        .session_runtime_registry()
        .await?
        .has_project_graph_admission_capacity()?
    {
        return Err(project_server_capacity_error());
    }
    Ok(capacity_admission)
}

#[cfg(test)]
pub(super) fn daemon_transcript_source_home(profile_root: &Path) -> Option<PathBuf> {
    profile_root.parent().map(Path::to_path_buf)
}

#[cfg(not(test))]
pub(super) fn daemon_transcript_source_home(_profile_root: &Path) -> Option<PathBuf> {
    tracedecay_sessions::runtime::home_dir()
}

/// Borrowed handles every project-open phase reads. A phase future captures
/// one pointer to this record instead of the handles themselves.
struct ProjectOpenInputs<'a> {
    store_administration: &'a StoreAdministration,
    project_open_gates: &'a tokio::sync::Mutex<ProjectOpenGates>,
    invocation: &'a DaemonInvocationState,
    http_application_registry: &'a http_application::DaemonHttpApplicationRegistry,
    canonical_project_path: &'a Path,
    handshake: &'a DaemonHandshake,
    runtime: &'a ProductionProjectCompositionRuntime,
    cancellation: &'a CancellationToken,
    /// Start of this open attempt. `project_open_phase` events report elapsed
    /// time from here unless they name a narrower phase start.
    started: Instant,
    #[cfg(feature = "memory-provider-host")]
    activation: ProjectMemoryProviderActivationSelector,
    #[cfg(test)]
    project_open_attempts: Option<&'a Arc<AtomicUsize>>,
}

/// Compose one project's MCP server: admit the route, open its graph, publish
/// the graph/search core, then upgrade that core in place to the full
/// session-capable server.
///
/// Each phase is its own future that owns its temporaries, so this state
/// machine carries only the compact phase results across awaits. The phases
/// are boxed at these call sites: under `--features hotpath` every measured
/// async fn embeds its body by value, and boxing here keeps the measured
/// wrapper (and every instrumented caller) a few words wide instead of
/// inlining the whole open.
#[hotpath::measure(label = "daemon.project.compose.server", future = true)]
#[allow(
    clippy::too_many_arguments,
    reason = "This composition entry delegates to the configured provider selector and project-open state machine."
)]
pub(super) async fn production_project_server(
    store_administration: &StoreAdministration,
    project_open_gates: &tokio::sync::Mutex<ProjectOpenGates>,
    invocation: &DaemonInvocationState,
    http_application_registry: &http_application::DaemonHttpApplicationRegistry,
    canonical_project_path: &Path,
    handshake: &DaemonHandshake,
    runtime: ProductionProjectCompositionRuntime,
    cancellation: &CancellationToken,
    #[cfg(test)] project_open_attempts: Option<&Arc<AtomicUsize>>,
) -> Result<ProductionProjectComposition> {
    #[cfg(feature = "memory-provider-host")]
    {
        production_project_server_inner(
            store_administration,
            project_open_gates,
            invocation,
            http_application_registry,
            canonical_project_path,
            handshake,
            runtime,
            cancellation,
            ProjectMemoryProviderActivationSelector::FromRuntimeConfiguration,
            #[cfg(test)]
            project_open_attempts,
        )
        .await
    }
    #[cfg(not(feature = "memory-provider-host"))]
    {
        production_project_server_inner(
            store_administration,
            project_open_gates,
            invocation,
            http_application_registry,
            canonical_project_path,
            handshake,
            runtime,
            cancellation,
            #[cfg(test)]
            project_open_attempts,
        )
        .await
    }
}

/// Test harness entry that keeps the production configuration decision while
/// allowing a caller-owned decorator around the real Native application port.
#[cfg(feature = "memory-provider-host")]
#[allow(
    clippy::too_many_arguments,
    reason = "The explicit selector is a test-only composition seam matching the production open entry."
)]
pub(super) async fn production_project_server_with_activation(
    store_administration: &StoreAdministration,
    project_open_gates: &tokio::sync::Mutex<ProjectOpenGates>,
    invocation: &DaemonInvocationState,
    http_application_registry: &http_application::DaemonHttpApplicationRegistry,
    canonical_project_path: &Path,
    handshake: &DaemonHandshake,
    runtime: ProductionProjectCompositionRuntime,
    cancellation: &CancellationToken,
    activation: ProjectMemoryProviderActivationSelector,
    #[cfg(test)] project_open_attempts: Option<&Arc<AtomicUsize>>,
) -> Result<ProductionProjectComposition> {
    production_project_server_inner(
        store_administration,
        project_open_gates,
        invocation,
        http_application_registry,
        canonical_project_path,
        handshake,
        runtime,
        cancellation,
        activation,
        #[cfg(test)]
        project_open_attempts,
    )
    .await
}

#[hotpath::measure(label = "daemon.project.compose.server", future = true)]
#[allow(
    clippy::too_many_arguments,
    reason = "This composition entry binds route admission, store lifetime, invocation and HTTP owners before publishing a server."
)]
async fn production_project_server_inner(
    store_administration: &StoreAdministration,
    project_open_gates: &tokio::sync::Mutex<ProjectOpenGates>,
    invocation: &DaemonInvocationState,
    http_application_registry: &http_application::DaemonHttpApplicationRegistry,
    canonical_project_path: &Path,
    handshake: &DaemonHandshake,
    runtime: ProductionProjectCompositionRuntime,
    cancellation: &CancellationToken,
    #[cfg(feature = "memory-provider-host")] activation: ProjectMemoryProviderActivationSelector,
    #[cfg(test)] project_open_attempts: Option<&Arc<AtomicUsize>>,
) -> Result<ProductionProjectComposition> {
    let inputs = ProjectOpenInputs {
        store_administration,
        project_open_gates,
        invocation,
        http_application_registry,
        canonical_project_path,
        handshake,
        runtime: &runtime,
        cancellation,
        started: Instant::now(),
        #[cfg(feature = "memory-provider-host")]
        activation,
        #[cfg(test)]
        project_open_attempts,
    };
    let admitted = match Box::pin(inputs.admit_route()).await? {
        RouteAdmission::Cached(composition) => return Ok(composition),
        RouteAdmission::Admitted(admitted) => admitted,
    };
    let opened = match Box::pin(inputs.open_graph(&admitted.route)).await? {
        GraphOpen::Cached(composition) => return Ok(composition),
        GraphOpen::Opened(opened) => opened,
    };
    let (core_candidate, core) = Box::pin(inputs.compose_core_server(&opened)).await?;
    let CoreRouteBinding {
        mut resolved,
        inserted,
    } = Box::pin(inputs.bind_core_route(
        admitted.route,
        &opened.key,
        core_candidate,
        &core.route_registered,
    ))
    .await?;
    if inserted {
        if let Some(project_id) = opened.key.owner.project_id.as_deref() {
            inputs
                .http_application_registry
                .block_project_route(project_id)
                .await;
        }
        let activation = match Box::pin(inputs.activate_core_route(&opened, &core, &resolved)).await
        {
            Ok(activation) => activation,
            Err(error) => {
                // Core activation can fail after installing a source-edit
                // owner or beginning a runtime publication. Feed even a
                // partially built activation through the same exact-owner
                // rollback so a pending route never survives the error.
                let empty_activation = CoreRouteActivation {
                    publication_attempt: None,
                    core_source_edit_mutation: None,
                };
                let Err(error) = Box::pin(inputs.settle_failed_full_upgrade(
                    &opened,
                    &core,
                    &empty_activation,
                    &resolved,
                    None,
                    error,
                ))
                .await
                else {
                    unreachable!("failed project-open settlement cannot succeed");
                };
                return Err(error);
            }
        };
        let upgrade = match Box::pin(inputs.construct_full_server(&opened, &core)).await {
            Ok(mut full) => {
                let phase_failure = inputs
                    .phase_checkpoint(ProjectOpenFailurePhase::ProviderMount)
                    .and_then(|()| {
                        inputs.phase_checkpoint(ProjectOpenFailurePhase::McpConstructed)
                    });
                match phase_failure {
                    Ok(()) => {
                        match Box::pin(inputs.finish_full_server(
                            &opened,
                            &core,
                            &activation,
                            &resolved,
                            &mut full,
                        ))
                        .await
                        {
                            Ok(()) => Ok(Arc::clone(&full.server)),
                            // Keep the complete transaction alive until the
                            // failure funnel has retired every owner it
                            // mounted. In particular, dropping the provider
                            // bundle alone does not stop an already-started
                            // observation worker.
                            Err(error) => Err((error, Some(full))),
                        }
                    }
                    Err(error) => Err((error, Some(full))),
                }
            }
            Err(error) => Err((error, None)),
        };
        match upgrade {
            Ok(full_server) => resolved = full_server,
            Err((error, published_full_server)) => {
                Box::pin(inputs.settle_failed_full_upgrade(
                    &opened,
                    &core,
                    &activation,
                    &resolved,
                    published_full_server,
                    error,
                ))
                .await?;
            }
        }
    } else {
        drop(admitted.foreground_project_open);
        core.route_registered.store(false, Ordering::Release);
    }
    Ok(ProductionProjectComposition {
        #[cfg(unix)]
        key: opened.key,
        canonical_project_path: canonical_project_path.to_path_buf(),
        server: resolved,
        #[cfg(unix)]
        inserted,
    })
}

/// The primary checkout this route must be served from, when a linked worktree
/// is checked out on the *same* branch as its primary.
///
/// `ProjectServerKey` is root-bound so distinct linked worktrees keep exact
/// root-bound servers over one shared `StoreOwnerKey` — a worktree on another
/// branch (or detached) serves a different generation and must not be answered
/// from the primary's graph. A worktree on the same branch serves the same
/// branch content from the same store owner and the same graph database, so a
/// second physical server for it is a duplicate owner over one authority, not
/// an exact route: the route belongs in the alias map beside the primary's.
///
/// Returns `None` for a primary checkout, a non-worktree path, a detached or
/// unborn checkout, and any worktree whose branch differs from the primary's.
fn shared_primary_checkout_root(project_path: &std::path::Path) -> Option<PathBuf> {
    let primary = tracedecay_runtime_core::worktree::repository_identity_root(project_path)?;
    let branch = tracedecay_runtime_core::branch::current_branch(project_path)?;
    if tracedecay_runtime_core::branch::current_branch(&primary)? != branch {
        return None;
    }
    tracedecay_daemon_identity::authority::canonical_identity_path(&primary).ok()
}

/// One `project_open_phase` event. `detail` precedes the elapsed time so the
/// field order matches every existing consumer of these lines.
fn log_project_open_phase(
    project: &Path,
    phase: &str,
    detail: Option<(&str, String)>,
    since: Instant,
) {
    let mut fields = vec![
        ("project", project.display().to_string()),
        ("phase", phase.to_owned()),
    ];
    if let Some(detail) = detail {
        fields.push(detail);
    }
    fields.push(("elapsed_ms", since.elapsed().as_millis().to_string()));
    log_daemon_event("project_open_phase", &fields);
}

/// Outcome of route admission: a published server already answers this route,
/// or the caller now owns the open behind the route's single-flight gate.
enum RouteAdmission {
    Cached(ProductionProjectComposition),
    Admitted(AdmittedProjectRoute),
}

/// Guards an admitted opener holds until its route publishes or fails.
///
/// Fields drop in declaration order: the graph admission slot is released
/// first, then the foreground-open marker, then the single-flight gate that
/// lets the next opener of this route observe the published server.
struct AdmittedProjectRoute {
    _capacity_admission: tokio::sync::OwnedMutexGuard<()>,
    foreground_project_open: tracedecay_store_runtime::ForegroundProjectOpenAdmission,
    _singleflight: tokio::sync::OwnedMutexGuard<()>,
    route: ProjectRouteKey,
}

/// Outcome of the graph open: an owner for this key was already published (the
/// route is now bound to it), or this caller opened the graph and owns the
/// composition that follows.
enum GraphOpen {
    Cached(ProductionProjectComposition),
    /// Boxed so the root state machine holds a pointer, not the pinned
    /// configuration snapshot, across every later phase.
    Opened(Box<OpenedProjectGraph>),
}

/// The opened graph and the route-wide choices resolved from its configuration.
struct OpenedProjectGraph {
    cg: Arc<crate::project::TraceDecay>,
    key: ProjectServerKey,
    runtime_configuration: tracedecay_configuration::config::PinnedRuntimeConfiguration,
    project_database_is_read_only: bool,
    code_index_store_root: PathBuf,
}

/// Ports both of this route's MCP servers publish. Built once before the core
/// server; the full server that replaces the core publishes the same ports.
struct ProjectRoutePorts {
    code_index: ProjectCodeIndexAuthorities,
    dashboard_code_index_freshness_reader:
        tracedecay_contracts::code_index_freshness::CodeIndexFreshnessReader,
    dashboard_feedback_status_reader: tracedecay_dashboard_api::feedback_api::FeedbackStatusReader,
    dashboard_pr_autotrack_reader: tracedecay_dashboard_api::PrAutoTrackManagedSummaryReader,
    diagnostic_broker: Arc<tokio::sync::Mutex<tracedecay_lsp::analyzer::broker::DiagnosticBroker>>,
    code_index_hook_sink: crate::mcp::server::CodeIndexHookSink,
    code_index_reconcile_sink: crate::mcp::server::CodeIndexReconcileSink,
    code_index_freshness_probe_sink: crate::mcp::server::CodeIndexFreshnessProbeSink,
    application_invocation_executor: Arc<dyn tracedecay_daemon_protocol::DaemonInvocationExecutor>,
    retained_server_resolver: crate::mcp::server::RetainedProjectServerResolver,
    automation_scheduler_reconciler:
        Option<tracedecay_dashboard_api::AutomationSchedulerReconciler>,
}

/// Route-owned state shared by the core server, the full server that replaces
/// it, and the retirement funnel behind both.
struct ComposedCoreServer {
    /// Authoritative project identity from the route key; every owner
    /// registration below binds to it.
    project_id: String,
    /// The key the route currently serves. A rekey moves it, and every
    /// publication below compares against it first.
    current_key: Arc<tokio::sync::Mutex<ProjectServerKey>>,
    route_registered: Arc<AtomicBool>,
    database_owner_reconciler: crate::mcp::DatabaseOwnerReconciler,
    profile_identity: profile_identity::LocalProfileIdentityAuthorityV1,
    registered_profile_db: tracedecay_global_db::RegisteredGlobalDbLeaseV1,
    accounting_db: Option<tracedecay_global_db::RegisteredGlobalDbLeaseV1>,
    graph_runtime: Arc<tracedecay_store_runtime::DaemonSessionRuntimeRegistryV1>,
    transcript_source_home: Option<PathBuf>,
    code_index_activation: Arc<code_index_scheduler::CodeIndexActivationV1>,
    #[cfg(feature = "memory-provider-host")]
    memory_provider_host:
        Arc<tracedecay_daemon_service::retained_owner::ProjectMemoryProviderHostMountV1>,
    ports: ProjectRoutePorts,
}

impl ComposedCoreServer {
    /// Publish this route's ports on a server construction context. The core
    /// and the full server publish the same set.
    fn publish_route_ports(
        &self,
        context: crate::mcp::server::McpServerConstructionContext,
        cg: &Arc<crate::project::TraceDecay>,
        invocation: &DaemonInvocationState,
    ) -> crate::mcp::server::McpServerConstructionContext {
        let ports = &self.ports;
        let code_index = &ports.code_index;
        let mut context = context
            .with_dashboard_code_index_freshness_reader(Arc::clone(
                &ports.dashboard_code_index_freshness_reader,
            ))
            .with_dashboard_feedback_status_reader(Arc::clone(
                &ports.dashboard_feedback_status_reader,
            ))
            .with_dashboard_pr_autotrack_reader(Arc::clone(&ports.dashboard_pr_autotrack_reader))
            .with_diagnostics_lsp(Arc::clone(&ports.diagnostic_broker))
            .with_code_index_hook_sink(Arc::clone(&ports.code_index_hook_sink))
            .with_code_index_reconcile_sink(Arc::clone(&ports.code_index_reconcile_sink))
            .with_code_index_freshness_probe_sink(Arc::clone(
                &ports.code_index_freshness_probe_sink,
            ))
            .with_code_index_publication_identity(Arc::clone(&code_index.publication_identity))
            .with_code_index_search_executor(Arc::clone(&code_index.search_executor))
            .with_code_index_similar_executor(Arc::clone(&code_index.similar_executor))
            .with_code_index_redundancy_executor(Arc::clone(&code_index.redundancy_executor))
            .with_code_index_branch_diff_executor(Arc::clone(&code_index.branch_diff_executor))
            .with_code_graph_projection_read_port(Arc::clone(
                &code_index.graph_projection_read_port,
            ))
            .with_code_index_ignored_dependency_admission(Arc::clone(
                &code_index.ignored_dependency_admission,
            ))
            .with_code_graph_read_admission_port(Arc::clone(&code_index.graph_read_admission_port))
            .with_verified_graph_query_port(
                tracedecay_graph_query::admitted_verified_graph_query_port_with_source(
                    Arc::clone(&code_index.graph_read_admission_port),
                    Arc::clone(&code_index.graph_projection_read_port),
                    cg.source_read_context(),
                ),
            )
            .with_code_index_search_authority(code_index.search_authority.clone())
            .with_admitted_project_scope(code_index.scope.clone())
            .with_project_server_live(Arc::clone(&self.route_registered))
            .with_application_invocation_executor(Arc::clone(
                &ports.application_invocation_executor,
            ))
            .with_daemon_invocation_service(invocation.service.clone())
            .with_retained_project_server_resolver(Arc::clone(&ports.retained_server_resolver));
        #[cfg(feature = "memory-provider-host")]
        {
            context =
                context.with_memory_provider_host_mount(Arc::clone(&self.memory_provider_host));
            if let Some(recall) = self.memory_provider_host.cognitive_recall_mount() {
                context = context.with_cognitive_recall_mount(recall);
            }
        }
        if let Some(reconciler) = ports.automation_scheduler_reconciler.as_ref() {
            context = context.with_automation_scheduler_reconciler(Arc::clone(reconciler));
        }
        context
    }
}

/// The published core and whether this open inserted it. `inserted == false`
/// means the route bound to an owner published concurrently for the same key.
struct CoreRouteBinding {
    resolved: Arc<crate::mcp::McpServer>,
    inserted: bool,
}

/// Owners the published core carries into the full upgrade and its failure
/// funnel.
struct CoreRouteActivation {
    /// The mandatory-owner publication of the registered project runtime;
    /// `None` when no owner published a runtime for this root (a read-only
    /// database mounts no source-edit owner and nothing else in the core).
    publication_attempt: Option<tracedecay_daemon_service::ProjectRuntimePublicationAttemptV1>,
    /// The core's preview-only source-edit lane; `None` for a read-only database.
    core_source_edit_mutation:
        Option<Arc<tracedecay_daemon_service::project_owner_registration::SourceEditMutationGate>>,
}

/// Both session databases this route serves, admitted together.
struct AdmittedSessionDatabases {
    session_db: tracedecay_global_db::RegisteredGlobalDbLeaseV1,
    user_session_db: tracedecay_global_db::RegisteredGlobalDbLeaseV1,
}

/// The full server candidate and every retained mount created while it is
/// being prepared. It remains private until `finish_full_server` commits the
/// owner-registry cutover.
struct PublishedFullServer {
    server: Arc<crate::mcp::McpServer>,
    session_db: tracedecay_global_db::RegisteredGlobalDbLeaseV1,
    session_holder_database_paths: Vec<PathBuf>,
    #[cfg(feature = "memory-provider-host")]
    provider_full_mount:
        Arc<tracedecay_daemon_service::retained_owner::ProjectMemoryProviderFullMountV1>,
}

impl ProjectOpenInputs<'_> {
    fn log_phase(&self, phase: &str, detail: Option<(&str, String)>, since: Instant) {
        log_project_open_phase(self.canonical_project_path, phase, detail, since);
    }

    #[inline]
    fn phase_checkpoint(&self, phase: ProjectOpenFailurePhase) -> Result<()> {
        #[cfg(test)]
        {
            PROJECT_OPEN_REACHED_PHASE.store(phase as u8, Ordering::Release);
            PROJECT_OPEN_PHASE_CHANGED.notify_waiters();
            if PROJECT_OPEN_FAILURE_AFTER_PHASE.load(Ordering::Acquire) == phase as u8 {
                PROJECT_OPEN_FAILURE_AFTER_PHASE.store(0, Ordering::Release);
                return Err(TraceDecayError::Config {
                    message: format!("injected project-open failure after {phase:?}"),
                });
            }
        }
        Ok(())
    }

    /// Route admission: registry enrollment, the published-server cache, the
    /// route's single-flight gate, the foreground-open marker, and one graph
    /// admission slot (releasing an idle server when the daemon is at capacity).
    #[hotpath::measure(label = "daemon.project.compose.admit_route", future = true)]
    async fn admit_route(&self) -> Result<RouteAdmission> {
        project_open_cancellation_checkpoint(self.cancellation)?;
        self.invocation
            .configuration_runtime_registrar()
            .ensure_worker_plan()?;
        ensure_registered_project_route(
            self.store_administration,
            self.canonical_project_path,
            self.handshake.allow_init,
        )
        .await?;
        let route = ProjectRouteKey::from_handshake(self.canonical_project_path, self.handshake)?;
        if let Some((cached_key, cached_server)) =
            cached_route_server(self.store_administration, &route).await
        {
            return Ok(RouteAdmission::Cached(cached_project_composition(
                self.canonical_project_path,
                cached_key,
                cached_server,
            )));
        }

        let gate = project_open_gate(self.project_open_gates, &route).await;
        let singleflight = tokio::select! {
            biased;
            () = self.cancellation.cancelled() => return Err(project_open_cancellation_error()),
            singleflight = gate.lock_owned() => singleflight,
        };
        // Order-sensitive: the same lookup runs again behind the single-flight
        // gate so a concurrent open that published while this caller waited is
        // reused.
        if let Some((cached_key, cached_server)) =
            cached_route_server(self.store_administration, &route).await
        {
            return Ok(RouteAdmission::Cached(cached_project_composition(
                self.canonical_project_path,
                cached_key,
                cached_server,
            )));
        }
        let foreground_project_open = self
            .store_administration
            .session_runtime_registry()
            .await?
            .begin_foreground_project_open()?;
        let capacity_gate = project_open_capacity_gate(self.project_open_gates).await;
        let capacity_admission = tokio::select! {
            biased;
            () = self.cancellation.cancelled() => return Err(project_open_cancellation_error()),
            admission = Arc::clone(&capacity_gate).lock_owned() => admission,
        };
        let capacity_admission = release_one_idle_project_server_before_open(
            self.store_administration,
            self.invocation,
            capacity_gate,
            capacity_admission,
        )
        .await?;
        Ok(RouteAdmission::Admitted(AdmittedProjectRoute {
            _capacity_admission: capacity_admission,
            foreground_project_open,
            _singleflight: singleflight,
            route,
        }))
    }

    /// Open the project graph behind the admitted route, re-check the deletion
    /// fence and the owner registry, and resolve the route-wide configuration
    /// choices every later phase reads.
    #[hotpath::measure(label = "daemon.project.compose.open_graph", future = true)]
    async fn open_graph(&self, route: &ProjectRouteKey) -> Result<GraphOpen> {
        #[cfg(test)]
        if let Some(attempts) = self.project_open_attempts {
            attempts.fetch_add(1, Ordering::Relaxed);
        }
        let cg = Arc::new(
            open_project_for_handshake(
                self.canonical_project_path,
                self.handshake,
                self.store_administration,
            )
            .await?,
        );
        let mut key = ProjectServerKey::from_open_project(&cg, self.handshake)?;
        if let Some(shared_root) = shared_primary_checkout_root(self.canonical_project_path) {
            key.project_root = shared_root;
        }
        self.log_phase("graph_admitted", None, self.started);
        project_open_cancellation_checkpoint(self.cancellation)?;
        // A deletion may arrive while the graph opens. Recheck the durable
        // replay fence before this in-flight open can republish its registry
        // authority.
        ensure_registered_project_route(
            self.store_administration,
            self.canonical_project_path,
            false,
        )
        .await?;
        ensure_context_scout_owner_before_advertising(&cg)?;
        cg.register_project_store_in_global_registry().await?;
        let code_index_store_root = cg.store_layout().data_root.join("code-index-v1");
        let runtime_configuration = cg
            .configuration_runtime()
            .client()
            .current()
            .await
            .map_err(|error| TraceDecayError::Config {
                message: format!("authoritative runtime configuration unavailable: {error}"),
            })?;
        let project_database_is_read_only = !cg.db().is_writable();
        let existing = {
            let mut servers = self.store_administration.project_servers().lock().await;
            let existing = servers.get_ready(&key).cloned();
            if existing.is_some() {
                servers.bind_route(route.clone(), key.clone());
            }
            existing
        };
        if let Some(existing) = existing {
            return Ok(GraphOpen::Cached(cached_project_composition(
                self.canonical_project_path,
                key,
                existing,
            )));
        }
        Ok(GraphOpen::Opened(Box::new(OpenedProjectGraph {
            cg,
            key,
            runtime_configuration,
            project_database_is_read_only,
            code_index_store_root,
        })))
    }

    /// Build every route-owned port and construct the core (graph, search,
    /// diagnostics) server candidate. Nothing is published yet.
    #[hotpath::measure(label = "daemon.project.compose.core", future = true)]
    #[expect(
        clippy::too_many_lines,
        reason = "Core server composition wires one project's ports into a single McpServer."
    )]
    async fn compose_core_server(
        &self,
        opened: &OpenedProjectGraph,
    ) -> Result<(Arc<crate::mcp::McpServer>, ComposedCoreServer)> {
        let OpenedProjectGraph {
            cg,
            key,
            runtime_configuration,
            project_database_is_read_only,
            code_index_store_root,
        } = opened;
        let current_key = Arc::new(tokio::sync::Mutex::new(key.clone()));
        let current_project_path = Arc::new(tokio::sync::Mutex::new(
            self.canonical_project_path.to_path_buf(),
        ));
        let route_registered = Arc::new(AtomicBool::new(true));
        // Route-owned cancellation lifetime. Terminal route revocation (a
        // failed owner rekey) must end this route's activation and query
        // waiters without touching the caller's project-open token, so they
        // hang off a child: cancelling a child never propagates to its parent.
        let route_cancellation = self.cancellation.child_token();
        let database_owner_reconciler = self.runtime.database_owner_reconciler(
            self.store_administration,
            Arc::clone(&current_key),
            Arc::clone(&current_project_path),
            Arc::clone(&route_registered),
            route_cancellation.clone(),
            self.handshake.clone(),
        );
        let automation_scheduler_reconciler = self.runtime.automation_scheduler_reconciler(
            Arc::clone(&current_key),
            Arc::clone(&current_project_path),
            self.handshake.clone(),
        );
        let project_id = key
            .owner
            .project_id
            .clone()
            .ok_or_else(|| TraceDecayError::Config {
                message: "project session runtime requires an authoritative project identity"
                    .to_owned(),
            })?;
        let registered_profile_db = self
            .store_administration
            .registered_profile_database()
            .await?;
        let graph_runtime = self
            .store_administration
            .registered_runtime_registry()
            .await?;
        let profile_identity = self.store_administration.profile_identity()?.clone();
        let accounting_db = tracedecay_global_db::global_accounting_enabled()
            .then(|| registered_profile_db.clone());
        let code_index = project_code_index_authorities(
            self.invocation,
            cg,
            self.canonical_project_path,
            &project_id,
            &profile_identity,
            &route_registered,
            *project_database_is_read_only,
        )?;
        #[cfg(feature = "memory-provider-host")]
        let memory_provider_activation = self.activation.clone().resolve(runtime_configuration)?;
        let code_index_mount = code_index_activation_mount(CodeIndexActivationMountInputs {
            invocation: self.invocation.clone(),
            project_id: code_index.project_id.clone(),
            project_root: self.canonical_project_path.to_path_buf(),
            store_root: code_index_store_root.clone(),
            native_graph_activation: runtime_configuration.config().native_graph_activation,
            scope: code_index.scope.clone(),
            route_registered: Arc::clone(&route_registered),
            cancellation: route_cancellation.clone(),
            graph_runtime: Arc::clone(&graph_runtime),
            graph_publication_database: Arc::new(cg.db().clone()),
        });
        let code_index_hint_sink = code_index_activation_hint_sink(
            self.invocation.code_index_schedulers.clone(),
            self.canonical_project_path.to_path_buf(),
        );
        let code_index_automatic_admission =
            if tracedecay_runtime_core::worktree::is_linked_worktree(self.canonical_project_path)
                && !cg.get_config().sync.watch_linked_worktrees
            {
                code_index_scheduler::CodeIndexAutomaticAdmissionV1::LinkedWorktreeDisabled
            } else {
                code_index_scheduler::CodeIndexAutomaticAdmissionV1::Admitted
            };
        let code_index_activation = Arc::new(
            code_index_scheduler::CodeIndexActivationV1::new_with_admission(
                self.canonical_project_path,
                Arc::clone(&route_registered),
                route_cancellation.clone(),
                code_index_automatic_admission,
                code_index_mount,
                code_index_hint_sink,
            ),
        );
        let code_index_hook_sink = code_index_hook_sink(Arc::clone(&code_index_activation));
        let code_index_reconcile_sink = code_index_reconcile_sink(
            self.invocation.code_index_schedulers.clone(),
            Arc::clone(&code_index_activation),
        );
        let code_index_freshness_probe_sink = code_index_freshness_probe_sink(
            self.invocation.code_index_schedulers.clone(),
            Arc::clone(&code_index_activation),
        );
        // The daemon mounts the same broker the MCP server and the directly
        // served dashboard open: persisted analyzer settings (with a recorded
        // degradation for an unreadable file) plus the home-level OpenCode
        // analyzer-ownership registration adopted on top of the project-level
        // one.
        let diagnostic_broker =
            tracedecay_application::dashboard_diagnostics::open_diagnostic_broker(
                self.canonical_project_path.to_path_buf(),
                &cg.store_layout().dashboard_root,
            )
            .await;
        let application_invocation_executor: Arc<
            dyn tracedecay_daemon_protocol::DaemonInvocationExecutor,
        > = Arc::new(InProcessDaemonInvocationExecutor::new(
            self.invocation.clone(),
            self.store_administration.clone(),
            self.canonical_project_path.to_path_buf(),
            code_index.scope.clone(),
        ));
        #[cfg(feature = "memory-provider-host")]
        let ncm_registration_factory: tracedecay_daemon_service::retained_owner::NcmRegistrationFactoryV1 = {
            let owners = Arc::clone(&self.invocation.ncm_worker_owner);
            Arc::new(move |profile_id, worker_binary, state_root, registration_revision, mode, authority| {
                ncm_observer::construct_ncm_registration_with_authority(
                    &owners,
                    &profile_id,
                    worker_binary,
                    state_root,
                    registration_revision,
                    mode,
                    authority,
                )
                .map_err(|error| error.to_string())
            })
        };
        #[cfg(feature = "memory-provider-host")]
        let memory_provider_host =
            tracedecay_daemon_service::retained_owner::mount_project_memory_provider_host(
                tracedecay_daemon_service::retained_owner::ProjectMemoryProviderHostInputsV1 {
                    activation: memory_provider_activation,
                    ncm_observer: runtime_configuration
                        .config()
                        .memory_provider_ncm_observer
                        .clone(),
                    graph: Arc::clone(cg),
                    canonical_project_path: self.canonical_project_path.to_path_buf(),
                    profile_id: profile_identity.profile_id().clone(),
                    scope: code_index.scope.clone(),
                    authoritative_project_id: code_index.project_id.clone(),
                    store_data_root: cg.store_layout().data_root.clone(),
                    recall_routing: runtime_configuration
                        .config()
                        .memory_provider_recall_routing
                        .clone(),
                    ncm_registration_factory: Some(ncm_registration_factory),
                    #[cfg(all(test, feature = "memory-provider-host"))]
                    native_port_interposition: self
                        .activation
                        .native_application_port_interposition(),
                    #[cfg(all(not(test), feature = "memory-provider-host"))]
                    native_port_interposition: None,
                },
            )
            .await
            .map_err(|error| TraceDecayError::Config { message: error })?;
        let transcript_source_home = daemon_transcript_source_home(profile_identity.profile_root());
        let core = ComposedCoreServer {
            project_id,
            current_key,
            route_registered,
            database_owner_reconciler,
            profile_identity,
            registered_profile_db,
            accounting_db,
            graph_runtime,
            transcript_source_home,
            code_index_activation,
            #[cfg(feature = "memory-provider-host")]
            memory_provider_host,
            ports: ProjectRoutePorts {
                code_index,
                dashboard_code_index_freshness_reader: project_dashboard_freshness_reader(
                    self.invocation.code_index_schedulers.clone(),
                ),
                dashboard_feedback_status_reader:
                    tracedecay_dashboard_api::feedback_api::feedback_status_reader(
                        self.invocation.feedback_runtime_registrar(),
                    ),
                dashboard_pr_autotrack_reader: project_dashboard_pr_autotrack_reader(),
                diagnostic_broker,
                code_index_hook_sink,
                code_index_reconcile_sink,
                code_index_freshness_probe_sink,
                application_invocation_executor,
                retained_server_resolver: retained_project_server_resolver(
                    self.store_administration.clone(),
                ),
                automation_scheduler_reconciler,
            },
        };
        let core_context = core.publish_route_ports(
            crate::mcp::server::McpServerConstructionContext::daemon_owned_core(
                Arc::clone(cg),
                self.handshake.scope_prefix.clone(),
                crate::mcp::server::McpServerDaemonCoreAuthority {
                    profile_identity: core.profile_identity.clone(),
                    accounting: core.accounting_db.clone(),
                    registry: core.registered_profile_db.clone(),
                    database_owner_reconciler: Arc::clone(&core.database_owner_reconciler),
                    project_routes: self.store_administration.project_routes(),
                    writers: crate::mcp::server::McpServerWriters::daemon_owned(
                        coordinated_dashboard_automation_writer(self.store_administration.clone()),
                        coordinated_background_refresh_writer(self.store_administration.clone()),
                    ),
                },
            ),
            cg,
            self.invocation,
        );
        project_open_cancellation_checkpoint(self.cancellation)?;
        let mcp_construction_started = Instant::now();
        let core_candidate = crate::mcp::McpServer::new_with_context(core_context).await;
        core_candidate
            .install_generation_census_reader(Arc::clone(
                &core.ports.code_index.generation_census_reader,
            ))
            .map_err(|_| TraceDecayError::Config {
                message: "core MCP generation census authority was already installed".to_owned(),
            })?;
        self.log_phase("mcp_core_constructed", None, mcp_construction_started);
        Ok((core_candidate, core))
    }

    /// Bind the route to the core candidate inside the bounded owner registry,
    /// retiring whatever the bounded eviction displaced.
    #[hotpath::measure(label = "daemon.project.compose.bind_route", future = true)]
    async fn bind_core_route(
        &self,
        route: ProjectRouteKey,
        key: &ProjectServerKey,
        core_candidate: Arc<crate::mcp::McpServer>,
        route_registered: &AtomicBool,
    ) -> Result<CoreRouteBinding> {
        if self.cancellation.is_cancelled() {
            core_candidate.shutdown().await;
            return Err(project_open_cancellation_error());
        }
        // Retirement admission precedes the owner registry. Once bounded
        // eviction removes an idle server, its exact Arc crosses only the
        // synchronous `spawn_and_track` handoff below; no caller cancellation
        // can drop it between registry removal and canonical shutdown
        // ownership.
        let mut retirement_admission = self
            .store_administration
            .acquire_project_server_retirement_admission()
            .await;
        let resolution = {
            let mut servers = self.store_administration.project_servers().lock().await;
            servers.bind_or_insert_route_bounded(
                route,
                key.clone(),
                core_candidate,
                MAX_CACHED_PROJECT_SERVERS,
                project_server_has_in_flight_response,
            )
        };
        let Some((resolved, inserted, retired)) = resolution else {
            route_registered.store(false, Ordering::Release);
            return Err(project_server_capacity_error());
        };
        for (retired_key, retired_server) in retired {
            let owner = retired_key.owner;
            self.store_administration
                .session_temporal_refresh_schedulers()
                .retire_project(&owner)
                .await;
            retirement_admission.spawn_and_track(
                owner,
                super::project_server_lifecycle::retire_project_servers(vec![retired_server], None),
            );
            hotpath::gauge!("project_servers").inc(-1.0);
        }
        // The owner registry guard was dropped before the synchronous
        // retirement handoff. Release admission before the remaining
        // project-open awaits.
        drop(retirement_admission);
        if inserted {
            hotpath::gauge!("project_servers").inc(1.0);
        }
        Ok(CoreRouteBinding { resolved, inserted })
    }

    /// Publish the inserted core: register the code-index activation, install
    /// the preview-only source-edit lane, begin the runtime publication, and
    /// mark the route ready.
    #[hotpath::measure(label = "daemon.project.compose.publish_core", future = true)]
    async fn activate_core_route(
        &self,
        opened: &OpenedProjectGraph,
        core: &ComposedCoreServer,
        resolved: &Arc<crate::mcp::McpServer>,
    ) -> Result<CoreRouteActivation> {
        if self.cancellation.is_cancelled() {
            return Err(project_open_cancellation_error());
        }
        if !self
            .invocation
            .code_index_schedulers
            .register_activation(&core.ports.code_index.scope, &core.code_index_activation)
        {
            core.route_registered.store(false, Ordering::Release);
            return Err(TraceDecayError::Config {
                message: "code-index activation scope does not match the project route".to_owned(),
            });
        }
        // The core's own lane never opens: only the full server reaches a Git
        // transaction authority. Its gate is kept so a rolled-back publication
        // can report a terminal failure instead of warming forever.
        let core_source_edit_mutation = if opened.project_database_is_read_only {
            None
        } else {
            Some(
                project_open_owners::install_project_open_source_edit_preview_owner(
                    resolved.as_ref(),
                    Arc::clone(&opened.cg),
                    Arc::clone(&core.ports.code_index.graph_projection_read_port),
                    self.canonical_project_path,
                    &core.project_id,
                )
                .await?,
            )
        };
        // The source-edit owner is the core's only runtime component, so its
        // registration is what creates the registry slot this publication
        // attempt fences. A read-only database registers none.
        let publication_attempt = self
            .invocation
            .service
            .project_runtimes
            .begin_publication(self.canonical_project_path);
        if publication_attempt.is_none() && core_source_edit_mutation.is_some() {
            return Err(TraceDecayError::Config {
                message: "project runtime disappeared before its publication began".to_owned(),
            });
        }
        // Publish the graph/search/diagnostic core before session admission.
        // Source-edit previews are available, while mutations fail closed as
        // warming until the full server has its transaction authority.
        {
            let mut servers = self.store_administration.project_servers().lock().await;
            if !servers.mark_ready(&opened.key) {
                return Err(TraceDecayError::Config {
                    message: "project server disappeared before core publication completed"
                        .to_owned(),
                });
            }
        }
        self.log_phase("core_published", None, self.started);
        Ok(CoreRouteActivation {
            publication_attempt,
            core_source_edit_mutation,
        })
    }

    /// Admit the project and profile session databases together, settle the
    /// project session graph for serving, and bind the graph runtime.
    #[hotpath::measure(label = "daemon.project.compose.admit_sessions", future = true)]
    async fn admit_session_databases(
        &self,
        cg: &Arc<crate::project::TraceDecay>,
        project_id: &tracedecay_domain::ProjectId,
        project_database_is_read_only: bool,
    ) -> Result<AdmittedSessionDatabases> {
        let project_session_open = async {
            let started = Instant::now();
            let database = self
                .store_administration
                .registered_project_session_database(cg.project_root(), cg.store_layout())
                .await?;
            Ok((database, started.elapsed()))
        };
        let profile_session_open = async {
            let started = Instant::now();
            let database = self
                .store_administration
                .registered_profile_session_database()
                .await?;
            Ok((database, started.elapsed()))
        };
        let ((session_db, project_sessions_elapsed), (user_session_db, profile_sessions_elapsed)) =
            join_independent_session_opens(project_session_open, profile_session_open).await?;
        let session_runtime_registry = self.store_administration.session_runtime_registry().await?;
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => {
                return Err(project_open_cancellation_error());
            }
            settlement = session_runtime_registry
                .settle_project_session_graph_for_serving(project_id) => {
                settlement?;
            }
        }
        if !project_database_is_read_only {
            bind_verified_project_graph_runtime(cg.db(), session_db.as_ref()).await?;
        }
        log_session_database_admission(
            self.canonical_project_path,
            project_sessions_elapsed,
            profile_sessions_elapsed,
        );
        Ok(AdmittedSessionDatabases {
            session_db,
            user_session_db,
        })
    }

    /// Construct the full server over the admitted session databases and
    /// runtime owners. The owner-registry cutover is deliberately deferred to
    /// `finish_full_server`, after every required owner and publication fence
    /// has succeeded.
    ///
    /// The core is reachable from here on, so every step leaves this function
    /// with an error instead of returning behind a published route: the
    /// caller's funnel owns retiring the owner. Retired relational graph repair
    /// is deliberately absent; the bounded code-index activation owns
    /// background indexing.
    #[hotpath::measure(label = "daemon.project.compose.construct_full", future = true)]
    #[expect(
        clippy::too_many_lines,
        reason = "Full server construction is one owner-and-port assembly for a published project route."
    )]
    async fn construct_full_server(
        &self,
        opened: &OpenedProjectGraph,
        core: &ComposedCoreServer,
    ) -> Result<PublishedFullServer> {
        let OpenedProjectGraph {
            cg,
            key,
            runtime_configuration,
            project_database_is_read_only,
            ..
        } = opened;
        let code_index = &core.ports.code_index;
        if *core.current_key.lock().await != *key {
            return Err(TraceDecayError::Config {
                message: "project changed branch during core capability admission".to_owned(),
            });
        }
        project_open_cancellation_checkpoint(self.cancellation)?;
        let AdmittedSessionDatabases {
            session_db,
            user_session_db,
        } = self
            .admit_session_databases(
                cg,
                &code_index.scope.project_id,
                *project_database_is_read_only,
            )
            .await?;
        self.phase_checkpoint(ProjectOpenFailurePhase::SessionDatabases)?;
        #[cfg(feature = "memory-provider-host")]
        let provider_full_mount =
            tracedecay_daemon_service::retained_owner::mount_project_memory_provider_full(
                &core.memory_provider_host,
                tracedecay_daemon_service::retained_owner::ProjectMemoryProviderFullMountInputsV1 {
                    graph: Arc::clone(cg),
                    canonical_project_path: self.canonical_project_path.to_path_buf(),
                    profile_id: core.profile_identity.profile_id().clone(),
                    brain_id: core.profile_identity.brain_id().clone(),
                    scope: code_index.scope.clone(),
                    authoritative_project_id: code_index.project_id.clone(),
                    session_db: session_db.clone(),
                    configuration_digest: runtime_configuration
                        .snapshot()
                        .effective_behavior_digest
                        .clone(),
                },
                self.cancellation,
            )
            .await
            .map_err(|error| TraceDecayError::Config { message: error })?;
        let delivery_access = daemon_owned_project_source_access_at(
            &code_index.scope,
            self.canonical_project_path,
            runtime_configuration,
            tracedecay_contracts::now_micros(),
        )
        .map_err(|error| TraceDecayError::Config {
            message: format!("project delivery source access denied: {error}"),
        })?;
        project_delivery_mount::ensure_project_delivery_settlement(
            self.invocation,
            self.canonical_project_path,
            session_db.clone(),
            &code_index.scope,
            &delivery_access,
        )
        .await?;
        self.phase_checkpoint(ProjectOpenFailurePhase::GitTransactions)?;
        let host_admission_broker = Some(
            self.store_administration
                .host_admission_broker(&session_db)
                .await?,
        );
        let refresh_schedulers = self
            .store_administration
            .session_temporal_refresh_schedulers();
        // Session ingest prepares captures under the one process background
        // CPU authority the bootstrap worker plan installed and mounted
        // into the refresh schedulers; project open without it is a
        // composition defect, not a degraded mode.
        let background_cpu =
            refresh_schedulers
                .background_cpu()
                .ok_or_else(|| TraceDecayError::Config {
                    message:
                        "process background CPU authority was not mounted during daemon bootstrap"
                            .to_owned(),
                })?;
        let project_session_refresh_wake = refresh_schedulers
            .ensure_project_with_history(
                key.owner.clone(),
                session_db.clone(),
                Arc::new(ProjectSessionHistoricalIngestor::new(
                    session_db.clone(),
                    Arc::new(core.profile_identity.clone()),
                    self.canonical_project_path.to_path_buf(),
                    code_index.project_id.clone(),
                    core.transcript_source_home.clone(),
                    refresh_schedulers.codex_discovery(),
                    Arc::clone(&background_cpu),
                )),
            )
            .await;
        let user_session_refresh_wake = refresh_schedulers
            .ensure_profile_with_history(
                user_session_db.db_path().to_path_buf(),
                user_session_db.clone(),
                Arc::new(ProfileSessionHistoricalIngestor::new(
                    user_session_db.clone(),
                    core.registered_profile_db.clone(),
                    Arc::new(core.profile_identity.clone()),
                    core.transcript_source_home.clone(),
                    refresh_schedulers.codex_discovery(),
                    Arc::clone(&background_cpu),
                    crate::session_review_port(),
                )),
            )
            .await;
        let session_sync_owner = self.store_administration.session_sync_service();
        session_sync_owner
            .register_project(DaemonSessionSyncConfig {
                brain_id: core.profile_identity.brain_id().clone(),
                profile_id: core.profile_identity.profile_id().clone(),
                project_id: code_index.project_id.clone(),
                profile_root: core.profile_identity.profile_root().to_path_buf(),
                project_root: self.canonical_project_path.to_path_buf(),
                transcript_source_home: core.transcript_source_home.clone(),
                project_sessions: session_db.clone(),
                user_sessions: user_session_db.clone(),
                registry: core.registered_profile_db.clone(),
                background_cpu: Arc::clone(&background_cpu),
                startup_import: cg.get_config().sync.session_start_sync,
                project_refresh: project_session_refresh_wake.clone(),
                user_refresh: user_session_refresh_wake.clone(),
            })
            .await?;
        let session_sync_port: Arc<dyn tracedecay_contracts::session_sync::SessionSyncServicePort> =
            session_sync_owner;
        let session_sync_service = Arc::downgrade(&session_sync_port);
        let store_telemetry_sampling = self.store_administration.store_telemetry_sampling();
        register_route_store_telemetry(
            &store_telemetry_sampling,
            cg,
            &code_index.scope,
            [
                core.registered_profile_db.as_ref(),
                user_session_db.as_ref(),
                session_db.as_ref(),
            ],
        );
        // Live Remote Brain operational read composed from the mounted remote
        // credential/spool/recovery authorities. Every operator surface
        // (Doctor, MCP, dashboard) re-observes current listener, enrollment,
        // spool, replay, backup, and failover state through this one provider;
        // typed `Unavailable` remains only when the remote plane is genuinely
        // unreadable.
        let remote_operational_status: tracedecay_contracts::RemoteOperationalStatusReaderV1 = {
            let remote_credentials = core.graph_runtime.remote_credential_authority();
            Arc::new(move || remote_credentials.operational_status())
        };
        let remote_operational_read = {
            let remote_operational_status = Arc::clone(&remote_operational_status);
            Arc::new(move || remote_operational_status().doctor_read())
        };
        // Historical convergence runs in the background after admission, so a
        // large store can be mid-migration while the daemon serves. Doctor
        // re-reads that state on every report instead of a snapshot taken
        // before the migrations were scheduled.
        let schema_convergence = {
            let registry = self.store_administration.session_runtime_registry().await?;
            Arc::new(move || {
                let unconverged = registry.unconverged_registered_schemas();
                tracedecay_daemon_service::doctor_kernel::SchemaConvergenceDoctorReadV1 {
                    storage:
                        tracedecay_daemon_service::doctor_kernel::pending_schema_migration_read(
                            &unconverged,
                        ),
                    findings: registry.registered_schema_convergence_observations(),
                }
            })
        };
        let doctor_report_reader =
            tracedecay_daemon_service::doctor_kernel::production_doctor_report_reader(
                self.canonical_project_path.to_path_buf(),
                code_index.project_id.clone(),
                cg.store_layout().clone(),
                cg.db().clone(),
                core.registered_profile_db.clone(),
                user_session_db.clone(),
                session_db.clone(),
                core.profile_identity.profile_root().to_path_buf(),
                core.transcript_source_home.clone(),
                remote_operational_read,
                schema_convergence,
                cg.get_config().sync.retention.clone(),
                self.invocation.code_index_schedulers.clone(),
                Arc::clone(&core.ports.diagnostic_broker),
                self.invocation.feedback_runtime_registrar(),
                store_telemetry_sampling,
            );
        let (delivery_settlement_authority, delivery_settlement_recorder) =
            project_delivery_settlement_ports(self.invocation, self.canonical_project_path).await?;
        let profile_session_refresh = self
            .store_administration
            .profile_session_refresh_service(&user_session_db)
            .await;
        let full_context = core
            .publish_route_ports(
                crate::mcp::server::McpServerConstructionContext::daemon_owned(
                    Arc::clone(cg),
                    self.handshake.scope_prefix.clone(),
                    crate::mcp::server::McpServerDaemonAuthority {
                        profile_identity: core.profile_identity.clone(),
                        databases: crate::mcp::server::McpServerDaemonDatabases {
                            accounting: core.accounting_db.clone(),
                            registry: core.registered_profile_db.clone(),
                            project_sessions: session_db.clone(),
                            profile_sessions: user_session_db,
                        },
                        host_admission_broker,
                        background_cpu,
                        project_session_refresh_wake,
                        user_session_refresh_wake,
                        profile_session_refresh,
                        session_sync_service,
                        database_owner_reconciler: Arc::clone(&core.database_owner_reconciler),
                        project_routes: self.store_administration.project_routes(),
                        writers: crate::mcp::server::McpServerWriters::daemon_owned(
                            coordinated_dashboard_automation_writer(
                                self.store_administration.clone(),
                            ),
                            coordinated_background_refresh_writer(
                                self.store_administration.clone(),
                            ),
                        ),
                        delivery_settlement_authority,
                        delivery_settlement_recorder,
                    },
                ),
                cg,
                self.invocation,
            )
            .with_remote_operational_status(remote_operational_status)
            .with_dashboard_doctor_report_reader(doctor_report_reader)
            .with_startup_catch_up_enabled(self.runtime.startup_catch_up());
        #[cfg(feature = "memory-provider-host")]
        let full_context = provider_full_mount
            .observation_journeys()
            .into_iter()
            .fold(full_context, |context, journey| {
                context.with_observation_journey_mount(journey)
            })
            .with_provider_control_mount(provider_full_mount.provider_control_mount());
        project_open_cancellation_checkpoint(self.cancellation)?;
        let full_construction_started = Instant::now();
        let full_candidate = crate::mcp::McpServer::new_with_context(full_context).await;
        full_candidate
            .install_generation_census_reader(Arc::clone(&code_index.generation_census_reader))
            .map_err(|_| TraceDecayError::Config {
                message: "full MCP generation census authority was already installed".to_owned(),
            })?;
        self.log_phase("mcp_full_constructed", None, full_construction_started);
        if *core.current_key.lock().await != *key {
            full_candidate.shutdown().await;
            return Err(TraceDecayError::Config {
                message: "project changed branch during full capability admission".to_owned(),
            });
        }
        Ok(PublishedFullServer {
            server: full_candidate,
            session_db,
            session_holder_database_paths: Vec::new(),
            #[cfg(feature = "memory-provider-host")]
            provider_full_mount,
        })
    }

    /// Mount the full server's dependent owners: the source-edit lane, Git
    /// index transactions, and the production owners. The HTTP route is a
    /// publication surface and is committed only after the MCP owner swap.
    ///
    /// The widest project-open phase: the two owner registrations it awaits
    /// are the largest leaves of the open (each ~20 KB, ~80 KB when
    /// instrumented), so its caller boxes this phase rather than doubling it
    /// into its own state.
    #[hotpath::measure(label = "daemon.project.compose.full_owners", future = true)]
    async fn mount_full_server_owners(
        &self,
        opened: &OpenedProjectGraph,
        core: &ComposedCoreServer,
        full_server: &crate::mcp::McpServer,
        session_db: tracedecay_global_db::RegisteredGlobalDbLeaseV1,
        core_source_edit_mutation: Option<
            Arc<tracedecay_daemon_service::project_owner_registration::SourceEditMutationGate>,
        >,
    ) -> Result<()> {
        let full_setup_started = Instant::now();
        project_open_cancellation_checkpoint(self.cancellation)?;
        // The shared invocation registry admits one source-edit owner per
        // project root. Core publication already registered it; the full
        // upgrade reuses that owner and marks its mutation gate ready after
        // Git transaction authority exists.
        let source_edit_mutation_ready = if opened.project_database_is_read_only {
            None
        } else {
            Some(
                core_source_edit_mutation.ok_or_else(|| TraceDecayError::Config {
                    message: "writable project did not install source edit preview authority"
                        .to_owned(),
                })?,
            )
        };
        self.log_phase("source_edit_preview_ready", None, full_setup_started);
        ensure_git_index_transactions_for_mutation_owners(
            self.store_administration,
            session_db,
            self.canonical_project_path,
            opened.key.owner.project_id.as_deref(),
        )
        .await?;
        self.log_phase("git_transactions_ready", None, full_setup_started);
        self.phase_checkpoint(ProjectOpenFailurePhase::GitTransactions)?;
        let dependent_owners = if opened.project_database_is_read_only {
            None
        } else {
            let source_edit_mutation_ready =
                source_edit_mutation_ready.ok_or_else(|| TraceDecayError::Config {
                    message: "writable project did not install source edit preview authority"
                        .to_owned(),
                })?;
            let state = project_open_owners::register_project_open_production_owners(
                self.invocation,
                self.store_administration.git_index_transaction_services(),
                self.store_administration.native_integration_services(),
                self.canonical_project_path,
                &core.project_id,
                full_server,
                source_edit_mutation_ready,
            )
            .await?;
            self.log_phase("independent_owners_registered", None, full_setup_started);
            self.phase_checkpoint(ProjectOpenFailurePhase::IndependentOwners)?;
            Some(state)
        };
        project_open_cancellation_checkpoint(self.cancellation)?;
        if let Some(dependent_owners) = dependent_owners {
            project_open_owners::register_project_open_dependent_owners(
                self.invocation,
                self.canonical_project_path,
                full_server,
                dependent_owners,
            )
            .await?;
            self.log_phase("production_owners_registered", None, full_setup_started);
        }
        self.phase_checkpoint(ProjectOpenFailurePhase::DependentOwners)?;
        Ok(())
    }

    /// Mount full owners, then atomically commit runtime, MCP-registry, and
    /// HTTP reachability. The displaced core is retired only after that commit
    /// succeeds.
    #[hotpath::measure(label = "daemon.project.compose.publish_full", future = true)]
    async fn finish_full_server(
        &self,
        opened: &OpenedProjectGraph,
        core: &ComposedCoreServer,
        activation: &CoreRouteActivation,
        resolved: &Arc<crate::mcp::McpServer>,
        full: &mut PublishedFullServer,
    ) -> Result<()> {
        // Keep the core in the registry while this whole block runs. A full
        // candidate is only dispatchable after every owner mount, deferred
        // provider activation, and the runtime publication fence succeeds.
        let mut registry_published = false;
        let result = async {
            self.log_phase("session_capabilities_prepared", None, self.started);
            full.session_holder_database_paths = self
                .invocation
                .service
                .mount_session_holder_databases([
                    core.registered_profile_db.clone(),
                    full.session_db.clone(),
                ])
                .await;
            Box::pin(self.mount_full_server_owners(
                opened,
                core,
                full.server.as_ref(),
                full.session_db.clone(),
                activation.core_source_edit_mutation.clone(),
            ))
            .await?;
            #[cfg(feature = "memory-provider-host")]
            full.provider_full_mount
                .activate_after_publication(full.session_db.observation_store())
                .await
                .map_err(|error| TraceDecayError::Config { message: error })?;
            self.phase_checkpoint(ProjectOpenFailurePhase::ProviderActivated)?;
            if *core.current_key.lock().await != opened.key {
                return Err(TraceDecayError::Config {
                    message: "project changed branch during full capability admission".to_owned(),
                });
            }
            if let Some(attempt) = &activation.publication_attempt
                && !self
                    .invocation
                    .service
                    .project_runtimes
                    .mark_publication_ready(attempt)
            {
                return Err(TraceDecayError::Config {
                    message: "project runtime publication attempt was superseded".to_owned(),
                });
            }
            self.phase_checkpoint(ProjectOpenFailurePhase::RuntimeReady)?;
            // This is the single visibility transition for the full server.
            // Until it succeeds, all registry lookups continue to resolve the
            // core and the full candidate has no dispatch path.
            let upgraded = self
                .store_administration
                .project_servers()
                .lock()
                .await
                .replace_ready_if(&opened.key, Arc::clone(&full.server), |current| {
                    Arc::ptr_eq(current, resolved)
                });
            if !upgraded {
                return Err(TraceDecayError::Config {
                    message: "project server changed during session capability upgrade".to_owned(),
                });
            }
            registry_published = true;
            self.phase_checkpoint(ProjectOpenFailurePhase::RegistryPublished)?;
            // HTTP is a separate cache and therefore gets its route only after
            // the MCP cutover. Any failure below is rolled back to the core and
            // removed from the HTTP registry by the transaction funnel.
            mount_http_application_router(
                self.http_application_registry,
                &core.project_id,
                self.canonical_project_path,
            )
            .await?;
            self.log_phase("http_application_mounted", None, self.started);
            self.phase_checkpoint(ProjectOpenFailurePhase::HttpMounted)?;
            resolved.revoke_project_server_responses_after_drain().await;
            schedule_project_server_retirement(
                self.store_administration,
                opened.key.owner.clone(),
                vec![Arc::clone(resolved)],
                None,
            )
            .await;
            full.server.publish_doctor_report();
            let code_index_status = match core.code_index_activation.automatic_admission() {
                code_index_scheduler::CodeIndexAutomaticAdmissionV1::Admitted => {
                    if core.code_index_activation.activate() {
                        "warming"
                    } else {
                        "unavailable"
                    }
                }
                code_index_scheduler::CodeIndexAutomaticAdmissionV1::LinkedWorktreeDisabled => {
                    log_daemon_event(
                        "code_index_activation_skipped",
                        &[
                            ("project", self.canonical_project_path.display().to_string()),
                            ("reason", "linked_worktree_disabled".to_owned()),
                        ],
                    );
                    "linked_worktree_disabled"
                }
            };
            self.log_phase(
                "full_published",
                Some(("code_index", code_index_status.to_owned())),
                self.started,
            );
            Ok(())
        }
        .await;
        if result.is_err() {
            // Restore the core before returning to the common failure funnel;
            // this makes the candidate unreachable even when a later mount
            // fails after the registry swap.
            self.http_application_registry
                .remove_project_route(&core.project_id)
                .await;
            if registry_published {
                let restored = self
                    .store_administration
                    .project_servers()
                    .lock()
                    .await
                    .swap_ready_if(&opened.key, Arc::clone(resolved), |current| {
                        Arc::ptr_eq(current, &full.server)
                    });
                if restored.is_none() {
                    tracing::warn!(
                        project = %self.canonical_project_path.display(),
                        "full project publication rollback found a different registry owner"
                    );
                }
            }
            if let Some(attempt) = &activation.publication_attempt {
                self.invocation
                    .service
                    .project_runtimes
                    .mark_publication_failed(attempt);
            }
        }
        result
    }

    /// Unwind a failed full publication as one transaction. The core is only a
    /// provisional route for this attempt; keeping it after a failed owner
    /// mount would leave runtime owners, provider workers, and HTTP reachability
    /// behind for the next retry.
    #[hotpath::measure(label = "daemon.project.compose.settle_failed_upgrade", future = true)]
    async fn settle_failed_full_upgrade(
        &self,
        opened: &OpenedProjectGraph,
        core: &ComposedCoreServer,
        activation: &CoreRouteActivation,
        resolved: &Arc<crate::mcp::McpServer>,
        published_full_server: Option<PublishedFullServer>,
        error: TraceDecayError,
    ) -> Result<()> {
        let failed_key = core.current_key.lock().await.clone();
        if let Some(mutation) = &activation.core_source_edit_mutation {
            mutation.mark_failed();
        }
        if let Some(attempt) = &activation.publication_attempt {
            self.invocation
                .service
                .project_runtimes
                .mark_publication_failed(attempt);
        }
        retire_failed_project_open_owner(
            self.store_administration,
            self.invocation,
            self.http_application_registry,
            self.canonical_project_path,
            opened,
            &failed_key,
            resolved,
            published_full_server,
            &core.route_registered,
        )
        .await;
        Err(error)
    }
}

/// Look this route up in the published project-server cache, refreshing its
/// recency on a hit. Callers reuse the returned server instead of opening.
async fn cached_route_server(
    store_administration: &StoreAdministration,
    route: &ProjectRouteKey,
) -> Option<(ProjectServerKey, Arc<crate::mcp::McpServer>)> {
    let mut servers = store_administration.project_servers().lock().await;
    servers
        .get_route_and_touch(route)
        .map(|(key, server)| (key.clone(), Arc::clone(server)))
}

/// The composition returned by every cache hit. `inserted` is always false:
/// reusing a published server never publishes a route.
fn cached_project_composition(
    canonical_project_path: &Path,
    key: ProjectServerKey,
    server: Arc<crate::mcp::McpServer>,
) -> ProductionProjectComposition {
    #[cfg(not(unix))]
    let _ = key;
    ProductionProjectComposition {
        #[cfg(unix)]
        key,
        canonical_project_path: canonical_project_path.to_path_buf(),
        server,
        #[cfg(unix)]
        inserted: false,
    }
}

/// Every exact-scope code-index port this route publishes to its MCP servers.
struct ProjectCodeIndexAuthorities {
    publication_identity: crate::mcp::server::CodeIndexPublicationIdentityResolver,
    project_id: tracedecay_domain::ProjectId,
    scope: tracedecay_contracts::ResolvedScope,
    graph_projection_read_port: Arc<dyn tracedecay_graph_query::CodeGraphProjectionReadPort>,
    ignored_dependency_admission:
        Arc<dyn tracedecay_application::code_index::CodeIndexIgnoredDependencyAdmissionPortV1>,
    generation_census_reader: tracedecay_runtime_core::runtime_telemetry::GenerationCensusReader,
    graph_read_admission_port: crate::mcp::server::CodeGraphReadAdmissionPort,
    search_authority: tracedecay_query::code_search::CodeIndexSearchAuthorityV1,
    search_executor: crate::mcp::server::CodeIndexSearchExecutor,
    similar_executor: crate::mcp::server::CodeIndexSimilarExecutor,
    redundancy_executor: crate::mcp::server::CodeIndexRedundancyExecutor,
    branch_diff_executor: crate::mcp::server::CodeIndexBranchDiffExecutor,
}

/// Resolve the project's search identity and bind every code-index read port to
/// that one exact scope. Scope resolution reads the graph's own project root,
/// not the handshake path, so a relocated store still binds its own scope.
fn project_code_index_authorities(
    invocation: &DaemonInvocationState,
    cg: &Arc<crate::project::TraceDecay>,
    canonical_project_path: &Path,
    authoritative_project_id: &str,
    profile_identity: &profile_identity::LocalProfileIdentityAuthorityV1,
    route_registered: &Arc<AtomicBool>,
    project_database_is_read_only: bool,
) -> Result<ProjectCodeIndexAuthorities> {
    let publication_identity: crate::mcp::server::CodeIndexPublicationIdentityResolver =
        Arc::new(invocation.code_index_schedulers.clone());
    let project_id = tracedecay_domain::ProjectId::new(authoritative_project_id.to_owned())
        .map_err(|error| TraceDecayError::Config {
            message: format!("project search identity is invalid: {error}"),
        })?;
    let scope =
        tracedecay_code_index_runtime::resolved_scope_for_project(cg.project_root(), &project_id)
            .map_err(|error| TraceDecayError::Config {
            message: format!("project search scope is invalid: {error:?}"),
        })?;
    let graph_projection_read_port =
        tracedecay_code_index_runtime::project_reads::project_code_graph_projection_read_port(
            invocation.code_index_schedulers.clone(),
            canonical_project_path.to_path_buf(),
            scope.clone(),
        );
    let ignored_dependency_admission = tracedecay_code_index_runtime::project_reads::
        project_code_index_ignored_dependency_admission_port(
            invocation.code_index_schedulers.clone(),
            canonical_project_path.to_path_buf(),
            scope.clone(),
            !project_database_is_read_only,
        );
    let generation_census_reader =
        tracedecay_code_index_runtime::project_reads::project_code_index_generation_census_reader(
            invocation.code_index_schedulers.clone(),
            canonical_project_path.to_path_buf(),
            scope.clone(),
        );
    let graph_read_admission_port: crate::mcp::server::CodeGraphReadAdmissionPort = Arc::new(
        tracedecay_daemon_service::DaemonCodeGraphReadAdmission::production(
            canonical_project_path.to_path_buf(),
            scope.clone(),
            Arc::clone(cg.configuration_runtime()),
        ),
    );
    let search_admission = tracedecay_daemon_service::admit_query_mcp_read(
        Some(profile_identity),
        &project_id,
        &scope,
        Arc::clone(route_registered),
    )
    .map_err(|error| TraceDecayError::Config {
        message: format!("project search admission is unavailable: {error}"),
    })?;
    let search_authority = search_admission.search_authority();
    let read_admission_provider = tracedecay_daemon_service::QueryMcpReadAdmissionProviderV1::new(
        profile_identity.clone(),
        project_id.clone(),
        Arc::clone(route_registered),
    );
    let search_executor = code_index_search_executor(
        invocation.code_index_schedulers.clone(),
        project_id.clone(),
        read_admission_provider.clone(),
        tracedecay_code_index_runtime::mcp_admission::RegisteredProjectScopeResolverV1,
    );
    let similar_executor = code_index_similar_executor(
        invocation.code_index_schedulers.clone(),
        project_id.clone(),
        read_admission_provider.clone(),
        tracedecay_code_index_runtime::mcp_admission::RegisteredProjectScopeResolverV1,
    );
    let redundancy_executor = code_index_redundancy_executor(
        invocation.code_index_schedulers.clone(),
        project_id.clone(),
        read_admission_provider.clone(),
        tracedecay_code_index_runtime::mcp_admission::RegisteredProjectScopeResolverV1,
    );
    let branch_diff_executor = code_index_branch_diff_executor(
        invocation.code_index_schedulers.clone(),
        project_id.clone(),
        read_admission_provider,
        tracedecay_code_index_runtime::mcp_admission::RegisteredProjectScopeResolverV1,
    );
    Ok(ProjectCodeIndexAuthorities {
        publication_identity,
        project_id,
        scope,
        graph_projection_read_port,
        ignored_dependency_admission,
        generation_census_reader,
        graph_read_admission_port,
        search_authority,
        search_executor,
        similar_executor,
        redundancy_executor,
        branch_diff_executor,
    })
}

/// Dashboard-facing freshness reader for this route's code-index schedulers.
fn project_dashboard_freshness_reader(
    schedulers: code_index_scheduler::CodeIndexSchedulerRegistryV1,
) -> tracedecay_contracts::code_index_freshness::CodeIndexFreshnessReader {
    let reader: tracedecay_contracts::code_index_freshness::CodeIndexFreshnessReader =
        Arc::new(move |project_root| {
            let schedulers = schedulers.clone();
            Box::pin(async move { schedulers.dashboard_freshness(&project_root).await })
        });
    reader
}

fn project_dashboard_pr_autotrack_reader()
-> tracedecay_dashboard_api::PrAutoTrackManagedSummaryReader {
    Arc::new(|store_root| {
        tracedecay_application::pr_tracking::managed_summary(&store_root).map(|entries| {
            entries
                .into_iter()
                .map(
                    |entry| tracedecay_dashboard_api::PrAutoTrackManagedSummaryEntryV1 {
                        branch: entry.branch,
                        pr: entry.pr,
                        head_branch: entry.head_branch,
                    },
                )
                .collect()
        })
    })
}

/// Register the project graph and the session databases this route owns with
/// the sampling authority. An unavailable registration is recorded and skipped,
/// never fatal: telemetry must not fail an otherwise healthy project open.
fn register_route_store_telemetry(
    sampling: &tracedecay_maintenance::telemetry::StoreTelemetrySamplingRegistry,
    cg: &Arc<crate::project::TraceDecay>,
    scope: &tracedecay_contracts::ResolvedScope,
    session_databases: [&tracedecay_global_db::RegisteredGlobalDb; 3],
) {
    let record_telemetry_registration = |path: &Path, registered: bool| {
        if !registered {
            log_daemon_event(
                "store_telemetry_registration",
                &[
                    ("store", path.display().to_string()),
                    ("outcome", "unavailable".to_owned()),
                ],
            );
        }
    };
    record_telemetry_registration(
        cg.db().database_path(),
        sampling.register_port(cg.db().database_path(), scope, || {
            cg.storage_telemetry_handle()
        }),
    );
    for database in session_databases {
        record_telemetry_registration(
            database.db_path(),
            sampling.register_port(database.db_path(), scope, || {
                database.storage_telemetry_handle()
            }),
        );
    }
}

/// Read this project's mounted delivery settlement ports. Both are required:
/// the full server refuses to publish without a settlement authority and a
/// recorder, so an unmounted port fails the upgrade instead of degrading.
async fn project_delivery_settlement_ports(
    invocation: &DaemonInvocationState,
    canonical_project_path: &Path,
) -> Result<(
    Arc<tracedecay_application::observability::DeliverySettlementAuthorityV1>,
    Arc<tracedecay_application::observability::BoundedDeliverySettlementRecorderV1>,
)> {
    let authority = invocation
        .service
        .delivery_settlement_authority(Some(canonical_project_path))
        .await
        .map_err(|error| TraceDecayError::Config {
            message: format!("delivery settlement authority is invalid: {error}"),
        })?
        .ok_or_else(|| TraceDecayError::Config {
            message: "delivery settlement authority is not mounted".to_owned(),
        })?;
    let recorder = invocation
        .service
        .delivery_settlement_recorder(Some(canonical_project_path))
        .await
        .ok_or_else(|| TraceDecayError::Config {
            message: "delivery settlement recorder is not mounted".to_owned(),
        })?;
    Ok((authority, recorder))
}

/// Retire every resource this failed open attempt published. The owner
/// registry, HTTP cache, invocation runtime, refresh schedulers, provider
/// workers, and database leases all have independent retention, so rollback
/// must visit each one explicitly.
#[hotpath::measure(label = "daemon.project.compose.retire_failed", future = true)]
async fn retire_failed_project_open_owner(
    store_administration: &StoreAdministration,
    invocation: &DaemonInvocationState,
    http_application_registry: &http_application::DaemonHttpApplicationRegistry,
    canonical_project_path: &Path,
    opened: &OpenedProjectGraph,
    failed_key: &ProjectServerKey,
    resolved: &Arc<crate::mcp::McpServer>,
    published_full_server: Option<PublishedFullServer>,
    route_registered: &Arc<AtomicBool>,
) {
    let full_server = published_full_server
        .as_ref()
        .map(|full| Arc::clone(&full.server));
    let session_holder_database_paths = published_full_server
        .as_ref()
        .map(|full| full.session_holder_database_paths.clone())
        .unwrap_or_default();
    let removed = store_administration
        .project_servers()
        .lock()
        .await
        .remove_if(failed_key, |server| {
            Arc::ptr_eq(server, resolved)
                || full_server
                    .as_ref()
                    .is_some_and(|full| Arc::ptr_eq(server, full))
        })
        .into_iter()
        .collect::<Vec<_>>();
    route_registered.store(false, Ordering::Release);
    if let Some(project_id) = failed_key.owner.project_id.as_deref() {
        http_application_registry
            .remove_project_route(project_id)
            .await;
    }
    for server in &removed {
        server.revoke_project_server_responses();
    }
    let full_is_removed = full_server
        .as_ref()
        .is_some_and(|full| removed.iter().any(|server| Arc::ptr_eq(server, full)));
    if !removed.is_empty() {
        store_administration
            .session_temporal_refresh_schedulers()
            .retire_project(&failed_key.owner)
            .await;
        super::project_server_lifecycle::retire_project_servers(
            removed,
            Some(Arc::clone(route_registered)),
        )
        .await;
    }

    // A full candidate is deliberately retained through this function even
    // when it never entered the registry. Shut down its provider journeys and
    // server task graph before the last Arc drops.
    if let Some(full) = published_full_server {
        #[cfg(feature = "memory-provider-host")]
        for journey in full.provider_full_mount.observation_journeys() {
            let failures = journey
                .shutdown(
                    tokio::time::Instant::now() + tracedecay_daemon_service::TASK_ABORT_DEADLINE,
                )
                .await;
            for failure in failures {
                tracing::warn!(
                    project = %canonical_project_path.display(),
                    failure = %failure,
                    "failed project-open provider journey did not stop cleanly"
                );
            }
        }
        if !full_is_removed {
            full.server.revoke_project_server_responses();
            full.server.shutdown().await;
        }
    }

    let Some(project_id) = failed_key
        .owner
        .project_id
        .clone()
        .and_then(|project_id| tracedecay_domain::ProjectId::new(project_id).ok())
    else {
        tracing::warn!(
            project = %canonical_project_path.display(),
            "failed project-open owner omitted its authoritative project identity"
        );
        return;
    };
    let Ok(identity) = store_administration.profile_identity() else {
        tracing::warn!(
            project = %canonical_project_path.display(),
            "failed project-open owner profile identity was unavailable during rollback"
        );
        return;
    };
    let identity = identity.clone();
    let mut project_roots = std::collections::BTreeSet::new();
    project_roots.insert(canonical_project_path.to_path_buf());
    project_roots.insert(failed_key.project_root.clone());
    if let Err(error) = invocation
        .retire_project_runtime_owners(identity.profile_id(), &project_id, &project_roots)
        .await
    {
        tracing::warn!(
            project = %canonical_project_path.display(),
            %error,
            "failed project-open invocation owners did not retire cleanly"
        );
    }

    let project_sessions_path = failed_key
        .store_root
        .join(tracedecay_runtime_core::storage::SESSIONS_DB_FILENAME);
    if let Err(error) = store_administration
        .git_index_transaction_services()
        .retire_project_database(&project_id, &project_sessions_path)
        .await
    {
        tracing::warn!(
            project = %canonical_project_path.display(),
            %error,
            "failed project-open Git transaction owner did not retire cleanly"
        );
    }
    if let Err(error) = store_administration
        .native_integration_services()
        .retire_project_database(&project_id, &project_sessions_path)
        .await
    {
        tracing::warn!(
            project = %canonical_project_path.display(),
            %error,
            "failed project-open Native integration owner did not retire cleanly"
        );
    }
    if let Err(error) = store_administration
        .session_sync_service()
        .retire_project(identity.profile_id(), &project_id)
        .await
    {
        tracing::warn!(
            project = %canonical_project_path.display(),
            %error,
            "failed project-open session sync owner did not retire cleanly"
        );
    }
    super::branch_admin::retire_registered_context_scout_owner(
        &project_id,
        &failed_key.owner.graph_db_path,
    );
    super::hook_v2_replay_consumer::shutdown_hook_v2_replay_consumer(
        &opened.cg.hook_store_layout().data_root,
    )
    .await;
    let telemetry_sampling = store_administration.store_telemetry_sampling();
    telemetry_sampling.release_retained_handle(&project_sessions_path);
    telemetry_sampling.release_retained_handle(&failed_key.owner.graph_db_path);
    if let Ok(runtime_registry) = store_administration.session_runtime_registry().await {
        let _ = runtime_registry
            .retire_project_session_relation_graph(&project_id)
            .await;
        let _ = runtime_registry
            .retire_project_memory_graph(&project_id)
            .await;
        runtime_registry
            .drop_project_runtime_caches(&project_id)
            .await;
    }
    invocation
        .service
        .unmount_session_holder_databases(session_holder_database_paths)
        .await;
}
