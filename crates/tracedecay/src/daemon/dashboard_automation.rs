//! Daemon-retained automation execution for dashboard project states.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::Value;
use tracedecay_application::observability::BoundedObservabilityProducerV1;
use tracedecay_automation::managed_skills::validate_skill_id;
use tracedecay_automation_runtime::automation::AutomationRunControl;
use tracedecay_automation_runtime::automation::backend::CodexAppServerBackend;
use tracedecay_automation_runtime::automation::config::{
    AutomationConfig, from_configuration_snapshot,
};
use tracedecay_automation_runtime::automation::effect_runtime::{
    AutomationEffectAdmission, AutomationEffectAuthority, AutomationSettledTerminal,
    RetainedAutomationSettlementOutcome,
};
use tracedecay_automation_runtime::automation::host_io::HostIo;
use tracedecay_automation_runtime::automation::managed_skills::{
    ManagedSkill, apply_managed_skill_update, archive_managed_skill, disable_managed_skill,
    load_managed_skill, managed_skill_dir, preview_managed_skill_update, restore_managed_skill,
    save_managed_skill,
};
use tracedecay_automation_runtime::automation::run_ledger::AutomationTrigger;
use tracedecay_automation_runtime::automation::runner::{
    MemoryCuratorAutomationOptions, RetainedAutomationRun, SessionReflectorAutomationOptions,
    SkillWriterAutomationOptions, run_memory_curator_with_backend_for_retained_settlement,
    run_session_reflector_with_backend_for_retained_settlement,
    run_skill_writer_with_backend_for_retained_settlement,
};
use tracedecay_automation_runtime::automation::skill_writer::deploy_managed_skills_to_project;
use tracedecay_automation_runtime::ports::session_evidence::{LcmGrepSort, LcmScope};
use tracedecay_contracts::now_micros;
use tracedecay_contracts::retained_surfaces::{LcmGrepSortV1, LcmRoleV1, LcmSearchScopeV1};
#[cfg(feature = "test-transport")]
use tracedecay_daemon_identity::authority;
use tracedecay_daemon_service::DaemonInvocationService;
use tracedecay_dashboard_api::{
    DashboardAutomationAuthorityErrorV1, DashboardAutomationAuthorityV1,
    DashboardAutomationObservationRecorderV1, DashboardAutomationRunOutcomeV1,
    DashboardAutomationRunPortV1, DashboardAutomationRunRequestV1, DashboardAutomationWriter,
    DashboardHttpRequestControlV1, DashboardManagedSkillCommandOutcomeV1,
    DashboardManagedSkillCommandPortV1, DashboardManagedSkillCommandV1,
};
use tracedecay_domain::configuration::UserProfileId;

use crate::mcp::server::{RetainedProjectGraphRequest, RetainedProjectServerResolver};
use crate::project::TraceDecay;
use tracedecay_domain::errors::{Result, TraceDecayError};

type DashboardAutomationResult<T> = std::result::Result<T, DashboardAutomationAuthorityErrorV1>;
type DashboardAutomationProjectFuture = std::pin::Pin<
    Box<dyn Future<Output = DashboardAutomationResult<Arc<TraceDecay>>> + Send + 'static>,
>;
type DashboardAutomationProjectResolver =
    Arc<dyn Fn(PathBuf) -> DashboardAutomationProjectFuture + Send + Sync + 'static>;

const USER_JOB_REQUEST_TIMEOUT_SECS: u64 = 120;

struct DashboardAutomationRequestRuntime {
    config: AutomationConfig,
    backend: CodexAppServerBackend,
}

impl DashboardAutomationRequestRuntime {
    fn new(configured: &AutomationConfig) -> Self {
        let mut config = configured.clone();
        config.timeout_secs = config.timeout_secs.min(USER_JOB_REQUEST_TIMEOUT_SECS);
        let backend = CodexAppServerBackend::from_automation_config(&config);
        Self { config, backend }
    }

    fn execution(&self) -> (&AutomationConfig, &CodexAppServerBackend) {
        (&self.config, &self.backend)
    }
}

fn dashboard_memory_curator_options(
    fact_review_limit: Option<usize>,
    min_confidence: Option<f64>,
) -> MemoryCuratorAutomationOptions {
    let mut options = MemoryCuratorAutomationOptions {
        trigger: AutomationTrigger::Dashboard,
        ..MemoryCuratorAutomationOptions::default()
    };
    if let Some(fact_review_limit) = fact_review_limit {
        options.fact_review_limit = fact_review_limit;
    }
    if let Some(min_confidence) = min_confidence {
        options.min_confidence = min_confidence;
    }
    options
}

fn dashboard_session_reflector_options(
    provider: Option<String>,
    query: Option<String>,
    evidence_limit: Option<usize>,
    scope: Option<LcmSearchScopeV1>,
    session_id: Option<String>,
    include_summaries: Option<bool>,
    include_recent_sessions: Option<bool>,
    recent_sessions_limit: Option<usize>,
    sort: Option<LcmGrepSortV1>,
    source: Option<String>,
    role: Option<LcmRoleV1>,
    start_time: Option<i64>,
    end_time: Option<i64>,
) -> SessionReflectorAutomationOptions {
    let mut options = SessionReflectorAutomationOptions {
        trigger: AutomationTrigger::Dashboard,
        session_id,
        source,
        role: role.map(dashboard_lcm_role).map(str::to_owned),
        start_time,
        end_time,
        ..SessionReflectorAutomationOptions::default()
    };
    if let Some(provider) = provider {
        options.provider = provider;
    }
    if let Some(query) = query {
        options.query = query;
    }
    if let Some(evidence_limit) = evidence_limit {
        options.evidence_limit = evidence_limit;
    }
    if let Some(scope) = scope {
        options.scope = dashboard_lcm_scope(scope);
    }
    if let Some(include_summaries) = include_summaries {
        options.include_summaries = include_summaries;
    }
    if let Some(include_recent_sessions) = include_recent_sessions {
        options.include_recent_sessions = include_recent_sessions;
    }
    if let Some(recent_sessions_limit) = recent_sessions_limit {
        options.recent_sessions_limit = recent_sessions_limit;
    }
    if let Some(sort) = sort {
        options.sort = dashboard_lcm_sort(sort);
    }
    options
}

fn dashboard_skill_writer_options(
    provider: Option<String>,
    query: Option<String>,
    evidence_limit: Option<usize>,
    include_recent_sessions: Option<bool>,
    recent_sessions_limit: Option<usize>,
    profile_root: &Path,
) -> SkillWriterAutomationOptions {
    let mut options = SkillWriterAutomationOptions {
        trigger: AutomationTrigger::Dashboard,
        profile_root: Some(profile_root.to_path_buf()),
        ..SkillWriterAutomationOptions::default()
    };
    if let Some(provider) = provider {
        options.provider = provider;
    }
    if let Some(query) = query {
        options.query = query;
    }
    if let Some(evidence_limit) = evidence_limit {
        options.evidence_limit = evidence_limit;
    }
    if let Some(include_recent_sessions) = include_recent_sessions {
        options.include_recent_sessions = include_recent_sessions;
    }
    if let Some(recent_sessions_limit) = recent_sessions_limit {
        options.recent_sessions_limit = recent_sessions_limit;
    }
    options
}

fn dashboard_lcm_scope(scope: LcmSearchScopeV1) -> LcmScope {
    match scope {
        LcmSearchScopeV1::Current => LcmScope::Current,
        LcmSearchScopeV1::Session => LcmScope::Session,
        LcmSearchScopeV1::All => LcmScope::All,
    }
}

fn dashboard_lcm_sort(sort: LcmGrepSortV1) -> LcmGrepSort {
    match sort {
        LcmGrepSortV1::Recency => LcmGrepSort::Recency,
        LcmGrepSortV1::Relevance => LcmGrepSort::Relevance,
        LcmGrepSortV1::Hybrid => LcmGrepSort::Hybrid,
    }
}

fn dashboard_lcm_role(role: LcmRoleV1) -> &'static str {
    match role {
        LcmRoleV1::System => "system",
        LcmRoleV1::User => "user",
        LcmRoleV1::Assistant => "assistant",
        LcmRoleV1::Tool => "tool",
        LcmRoleV1::Unknown => "unknown",
    }
}

enum DashboardAutomationAdmission {
    Execute(Box<AutomationEffectAuthority>),
    Replay(Box<AutomationSettledTerminal>),
}

async fn prepare_dashboard_automation_effect(
    invocation_service: &DaemonInvocationService,
    cg: &TraceDecay,
    request_control: &DashboardHttpRequestControlV1,
    configuration_digest: tracedecay_domain::ManifestDigest,
    request: tracedecay_contracts::retained_surfaces::AutomationRunRequestV1,
) -> DashboardAutomationResult<DashboardAutomationAdmission> {
    let admission = tracedecay_daemon_service::automation_effect::prepare(
        invocation_service,
        cg,
        cg.project_root(),
        &cg.store_layout().dashboard_root,
        request_control.request_id(),
        request_control.deadline(),
        request_control.cancellation(),
        request_control.observed_at(),
        configuration_digest,
        request,
    )
    .await
    .map_err(automation_failed)?;
    match admission {
        AutomationEffectAdmission::Execute(effect) => {
            Ok(DashboardAutomationAdmission::Execute(effect))
        }
        AutomationEffectAdmission::Replay(terminal) => {
            Ok(DashboardAutomationAdmission::Replay(terminal))
        }
        AutomationEffectAdmission::PreAdmissionProblem(envelope) => Err(
            DashboardAutomationAuthorityErrorV1::ApplicationProblem(envelope),
        ),
        AutomationEffectAdmission::Conflict => Err(automation_admission_conflict()),
    }
}

async fn settle_dashboard_automation_run<T>(
    effect: Box<AutomationEffectAuthority>,
    retained_run: RetainedAutomationRun<T>,
    producer: &Arc<BoundedObservabilityProducerV1>,
    project_root: &Path,
    surface: &'static str,
) -> DashboardAutomationResult<DashboardAutomationRunOutcomeV1>
where
    T: Send + 'static,
{
    let observer = tracedecay_daemon_service::automation_observation::automation_run_observer(
        Arc::clone(producer),
        project_root.to_path_buf(),
        surface,
    );
    let waiter = effect.start_retained_automation_settlement(retained_run, Some(observer), |run| {
        (run.ledger_record, run.committed_receipt)
    });
    match waiter.wait().await.map_err(automation_failed)? {
        RetainedAutomationSettlementOutcome::Run {
            terminal,
            record: _record,
        } => automation_terminal_run(&terminal),
        RetainedAutomationSettlementOutcome::Problem {
            problem,
            record: _record,
        } => Err(automation_problem(problem)),
        RetainedAutomationSettlementOutcome::Reused { record: _record }
        | RetainedAutomationSettlementOutcome::AbandonedObserved { record: _record } => Err(
            automation_failed("dashboard automation cannot reuse a scheduler-only skip"),
        ),
    }
}

pub(crate) fn dashboard_automation_observation_port(
    invocation_service: DaemonInvocationService,
) -> Arc<
    dyn Fn(PathBuf) -> tracedecay_dashboard_api::DashboardAutomationObservationFuture
        + Send
        + Sync
        + 'static,
> {
    Arc::new(move |project_root| {
        let invocation_service = invocation_service.clone();
        Box::pin(async move {
            let producer = crate::daemon::project_automation_observation_producer(
                &invocation_service,
                &project_root,
            )
            .await
            .ok_or_else(|| {
                "dashboard automation observation authority is unavailable".to_owned()
            })?;
            Ok(Arc::new(move |record| {
                crate::daemon::record_project_automation_run(
                    producer.as_ref(),
                    &project_root,
                    &record,
                    "dashboard_user_job",
                );
            }) as DashboardAutomationObservationRecorderV1)
        })
    })
}

/// Builds the single exact-profile authority used by production dashboard
/// states and their host-admission integration journeys.
pub(crate) fn compose_dashboard_automation_authority(
    profile_root: PathBuf,
    daemon_user_profile_id: UserProfileId,
    retained_project_server_resolver: RetainedProjectServerResolver,
    writer: DashboardAutomationWriter,
    invocation_service: DaemonInvocationService,
) -> Result<DashboardAutomationAuthorityV1> {
    let project_resolver = dashboard_automation_project_resolver(
        daemon_user_profile_id,
        retained_project_server_resolver,
    );
    compose_dashboard_automation_authority_with_resolver(
        profile_root,
        project_resolver,
        writer,
        invocation_service,
    )
}

fn compose_dashboard_automation_authority_with_resolver(
    profile_root: PathBuf,
    project_resolver: DashboardAutomationProjectResolver,
    writer: DashboardAutomationWriter,
    invocation_service: DaemonInvocationService,
) -> Result<DashboardAutomationAuthorityV1> {
    let run_port = dashboard_automation_run_port(
        profile_root.clone(),
        Arc::clone(&project_resolver),
        invocation_service,
    );
    let skill_port =
        dashboard_managed_skill_command_port(profile_root.clone(), project_resolver, writer);
    DashboardAutomationAuthorityV1::new(profile_root, run_port, skill_port).map_err(|error| {
        TraceDecayError::Config {
            message: error.detail().to_owned(),
        }
    })
}

#[cfg(feature = "test-transport")]
pub(crate) fn compose_dashboard_automation_authority_for_test(
    profile_root: PathBuf,
    retained: Arc<TraceDecay>,
    writer: DashboardAutomationWriter,
    invocation_service: DaemonInvocationService,
) -> Result<DashboardAutomationAuthorityV1> {
    let retained_root = retained.project_root().to_path_buf();
    let project_resolver: DashboardAutomationProjectResolver =
        Arc::new(move |requested_project_root| {
            let retained = Arc::clone(&retained);
            let retained_root = retained_root.clone();
            Box::pin(async move {
                let requested = authority::canonical_identity_path(&requested_project_root)
                    .map_err(|error| DashboardAutomationAuthorityErrorV1::Unavailable {
                        detail: format!("dashboard automation project is unavailable: {error}"),
                    })?;
                let retained_identity = authority::canonical_identity_path(&retained_root)
                    .map_err(|error| DashboardAutomationAuthorityErrorV1::Unavailable {
                        detail: format!(
                            "retained dashboard automation project is unavailable: {error}"
                        ),
                    })?;
                if requested != retained_identity {
                    return Err(DashboardAutomationAuthorityErrorV1::Denied {
                        detail: "dashboard automation project authority resolved a different root"
                            .to_owned(),
                    });
                }
                validate_dashboard_automation_project(retained, &requested_project_root)
            })
        });
    compose_dashboard_automation_authority_with_resolver(
        profile_root,
        project_resolver,
        writer,
        invocation_service,
    )
}

fn dashboard_automation_run_port(
    profile_root: PathBuf,
    project_resolver: DashboardAutomationProjectResolver,
    invocation_service: DaemonInvocationService,
) -> DashboardAutomationRunPortV1 {
    Arc::new(move |invocation| {
        let profile_root = profile_root.clone();
        let project_resolver = Arc::clone(&project_resolver);
        let invocation_service = invocation_service.clone();
        Box::pin(async move {
            let cg = project_resolver(invocation.project_root.clone()).await?;
            let run_control = dashboard_automation_run_control(&invocation.control);
            // The canonical runner owns task locking, cooldowns, run-ledger
            // publication, and curation CAS. Holding the daemon's broad store
            // writer across a model turn would serialize unrelated projects.
            execute_dashboard_automation_run(
                cg.as_ref(),
                profile_root,
                invocation.request,
                invocation.control,
                &run_control,
                &invocation_service,
            )
            .await
        })
    })
}

fn dashboard_automation_run_control(
    control: &DashboardHttpRequestControlV1,
) -> AutomationRunControl {
    let cancellation = control.cancellation().clone();
    let deadline = control.deadline();
    AutomationRunControl::from_interrupted(Arc::new(move || {
        cancellation.is_cancelled() || deadline.is_elapsed_at(now_micros())
    }))
}

fn dashboard_managed_skill_command_port(
    profile_root: PathBuf,
    project_resolver: DashboardAutomationProjectResolver,
    writer: DashboardAutomationWriter,
) -> DashboardManagedSkillCommandPortV1 {
    Arc::new(move |invocation| {
        let profile_root = profile_root.clone();
        let project_resolver = Arc::clone(&project_resolver);
        let writer = Arc::clone(&writer);
        Box::pin(async move {
            execute_serialized_dashboard_automation(&writer, move || async move {
                let cg = project_resolver(invocation.project_root.clone()).await?;
                execute_dashboard_managed_skill_command(
                    &tracedecay_agent_hosts::host_io(),
                    &profile_root,
                    cg.project_root(),
                    invocation.command,
                )
                .await
            })
            .await
        })
    })
}

fn dashboard_automation_project_resolver(
    daemon_user_profile_id: UserProfileId,
    retained_project_server_resolver: RetainedProjectServerResolver,
) -> DashboardAutomationProjectResolver {
    Arc::new(move |requested_project_root| {
        let daemon_user_profile_id = daemon_user_profile_id.clone();
        let retained_project_server_resolver = Arc::clone(&retained_project_server_resolver);
        Box::pin(async move {
            let retained_server = retained_project_server_resolver(
                RetainedProjectGraphRequest::for_mounted_root(requested_project_root.clone()),
            )
            .await
            .map_err(|error| DashboardAutomationAuthorityErrorV1::Unavailable {
                detail: format!("dashboard automation project authority is unavailable: {error}"),
            })?
            .ok_or_else(|| DashboardAutomationAuthorityErrorV1::Unavailable {
                detail: format!(
                    "dashboard automation project '{}' is not retained by the daemon",
                    requested_project_root.display()
                ),
            })?;
            if retained_server
                .profile_identity()
                .is_none_or(|identity| identity.profile_id() != &daemon_user_profile_id)
            {
                return Err(DashboardAutomationAuthorityErrorV1::Denied {
                    detail: "dashboard automation project belongs to another profile".to_owned(),
                });
            }
            let retained = retained_server.cg_snapshot().await;
            validate_dashboard_automation_project(retained, &requested_project_root)
        })
    })
}

fn validate_dashboard_automation_project(
    retained: Arc<TraceDecay>,
    requested_project_root: &Path,
) -> DashboardAutomationResult<Arc<TraceDecay>> {
    let requested = requested_project_root.canonicalize().map_err(|error| {
        DashboardAutomationAuthorityErrorV1::Unavailable {
            detail: format!(
                "dashboard automation project '{}' cannot be resolved: {error}",
                requested_project_root.display()
            ),
        }
    })?;
    let retained_root = retained.project_root().canonicalize().map_err(|error| {
        DashboardAutomationAuthorityErrorV1::Unavailable {
            detail: format!(
                "retained dashboard automation project '{}' cannot be resolved: {error}",
                retained.project_root().display()
            ),
        }
    })?;
    if requested != retained_root {
        return Err(DashboardAutomationAuthorityErrorV1::Denied {
            detail: "dashboard automation project authority resolved a different root".to_owned(),
        });
    }
    Ok(retained)
}

async fn execute_serialized_dashboard_automation<T, Operation, OperationFuture>(
    writer: &DashboardAutomationWriter,
    operation: Operation,
) -> DashboardAutomationResult<T>
where
    T: Send + 'static,
    Operation: FnOnce() -> OperationFuture + Send + 'static,
    OperationFuture: Future<Output = DashboardAutomationResult<T>> + Send + 'static,
{
    let outcome = Arc::new(tokio::sync::Mutex::new(None));
    let written_outcome = Arc::clone(&outcome);
    let writer_result = writer(Box::new(move || {
        Box::pin(async move {
            let result = operation().await;
            let serialized_result = match result.as_ref() {
                Ok(_) => Ok(Value::Null),
                Err(error) => Err(error.detail().to_owned()),
            };
            *written_outcome.lock().await = Some(result);
            serialized_result
        })
    }))
    .await;
    let operation_result = outcome.lock().await.take();
    match operation_result {
        Some(result) => result,
        None => Err(DashboardAutomationAuthorityErrorV1::Failed {
            detail: match writer_result {
                Ok(_) => {
                    "dashboard automation writer returned without an operation outcome".to_owned()
                }
                Err(detail) => detail,
            },
        }),
    }
}

#[hotpath::measure(label = "daemon.dashboard.automation.execute", future = true)]
#[expect(
    clippy::too_many_lines,
    reason = "The observation producer and pinned configuration are admitted before any typed dashboard task or retained effect is reserved."
)]
async fn execute_dashboard_automation_run(
    cg: &TraceDecay,
    profile_root: PathBuf,
    request: DashboardAutomationRunRequestV1,
    request_control: DashboardHttpRequestControlV1,
    run_control: &AutomationRunControl,
    invocation_service: &DaemonInvocationService,
) -> DashboardAutomationResult<DashboardAutomationRunOutcomeV1> {
    let producer = crate::daemon::project_automation_observation_producer(
        invocation_service,
        cg.project_root(),
    )
    .await
    .ok_or_else(|| DashboardAutomationAuthorityErrorV1::Unavailable {
        detail: "dashboard automation observation authority is unavailable".to_owned(),
    })?;
    let pinned = cg
        .configuration_runtime()
        .client()
        .current()
        .await
        .map_err(|error| DashboardAutomationAuthorityErrorV1::Unavailable {
            detail: format!("automation configuration authority is unavailable: {error}"),
        })?;
    let config = from_configuration_snapshot(pinned.snapshot()).map_err(automation_failed)?;
    let configuration_digest =
        tracedecay_automation_runtime::automation::effect_runtime::pinned_automation_configuration_digest(
            pinned.revision_id(),
            &pinned.snapshot().effective_behavior_digest,
            &pinned.snapshot().resolution_provenance_digest,
        )
        .map_err(automation_failed)?;
    let runtime = DashboardAutomationRequestRuntime::new(&config);
    let (config, backend) = runtime.execution();
    let run = match request {
        DashboardAutomationRunRequestV1::MemoryCurator {
            fact_review_limit,
            min_confidence,
        } => {
            let mut options = dashboard_memory_curator_options(fact_review_limit, min_confidence);
            let run_id = request_control.request_id().as_str().to_owned();
            options.run_id = Some(run_id.clone());
            let automation_context = cg.automation_project_context().map_err(automation_failed)?;
            let admission = prepare_dashboard_automation_effect(
                invocation_service,
                cg,
                &request_control,
                configuration_digest,
                tracedecay_automation_runtime::automation::effect_runtime::memory_curator_run_request(
                    &run_id,
                    options.fact_review_limit,
                    options.min_confidence,
                )
                .map_err(automation_failed)?,
            )
            .await?;
            let effect = match admission {
                DashboardAutomationAdmission::Execute(effect) => effect,
                DashboardAutomationAdmission::Replay(terminal) => {
                    return automation_terminal_run(&terminal);
                }
            };
            let retained_run = run_memory_curator_with_backend_for_retained_settlement(
                &automation_context,
                config,
                pinned.revision_id(),
                backend,
                options,
                run_control,
            )
            .await;
            settle_dashboard_automation_run(
                effect,
                retained_run,
                &producer,
                cg.project_root(),
                "dashboard_memory_curator",
            )
            .await?
        }
        DashboardAutomationRunRequestV1::SessionReflector {
            provider,
            query,
            evidence_limit,
            scope,
            session_id,
            include_summaries,
            include_recent_sessions,
            recent_sessions_limit,
            sort,
            source,
            role,
            start_time,
            end_time,
        } => {
            let mut options = dashboard_session_reflector_options(
                provider,
                query,
                evidence_limit,
                scope,
                session_id,
                include_summaries,
                include_recent_sessions,
                recent_sessions_limit,
                sort,
                source,
                role,
                start_time,
                end_time,
            );
            let run_id = request_control.request_id().as_str().to_owned();
            options.run_id = Some(run_id.clone());
            let automation_context = cg.automation_project_context().map_err(automation_failed)?;
            let admission = prepare_dashboard_automation_effect(
                invocation_service,
                cg,
                &request_control,
                configuration_digest,
                tracedecay_automation_runtime::automation::effect_runtime::session_reflector_run_request(
                    &run_id,
                    &options,
                )
                .map_err(automation_failed)?,
            )
            .await?;
            let effect = match admission {
                DashboardAutomationAdmission::Execute(effect) => effect,
                DashboardAutomationAdmission::Replay(terminal) => {
                    return automation_terminal_run(&terminal);
                }
            };
            let retained_run = run_session_reflector_with_backend_for_retained_settlement(
                &automation_context,
                config,
                run_control,
                pinned.revision_id(),
                backend,
                options,
            )
            .await;
            settle_dashboard_automation_run(
                effect,
                retained_run,
                &producer,
                cg.project_root(),
                "dashboard_session_reflector",
            )
            .await?
        }
        DashboardAutomationRunRequestV1::SkillWriter {
            provider,
            query,
            evidence_limit,
            include_recent_sessions,
            recent_sessions_limit,
        } => {
            let mut options = dashboard_skill_writer_options(
                provider,
                query,
                evidence_limit,
                include_recent_sessions,
                recent_sessions_limit,
                &profile_root,
            );
            let run_id = request_control.request_id().as_str().to_owned();
            options.run_id = Some(run_id.clone());
            let automation_context = cg.automation_project_context().map_err(automation_failed)?;
            let admission = prepare_dashboard_automation_effect(
                invocation_service,
                cg,
                &request_control,
                configuration_digest,
                tracedecay_automation_runtime::automation::effect_runtime::skill_writer_run_request(
                    &run_id,
                    &options,
                )
                .map_err(automation_failed)?,
            )
            .await?;
            let effect = match admission {
                DashboardAutomationAdmission::Execute(effect) => effect,
                DashboardAutomationAdmission::Replay(terminal) => {
                    return automation_terminal_run(&terminal);
                }
            };
            let retained_run = run_skill_writer_with_backend_for_retained_settlement(
                &automation_context,
                config,
                pinned.revision_id(),
                backend,
                options,
            )
            .await;
            settle_dashboard_automation_run(
                effect,
                retained_run,
                &producer,
                cg.project_root(),
                "dashboard_skill_writer",
            )
            .await?
        }
        DashboardAutomationRunRequestV1::UserJob { job_id, run_id } => {
            let job = tracedecay_automation_runtime::automation::jobs::find_job(
                &cg.store_layout().dashboard_root,
                &job_id,
            )
            .await
            .map_err(automation_failed)?
            .ok_or_else(|| DashboardAutomationAuthorityErrorV1::NotFound {
                detail: format!("automation job '{job_id}' was not found"),
            })?;
            let admission = tracedecay_daemon_service::automation_effect::prepare(
                invocation_service,
                cg,
                cg.project_root(),
                &cg.store_layout().dashboard_root,
                request_control.request_id(),
                request_control.deadline(),
                request_control.cancellation(),
                request_control.observed_at(),
                configuration_digest,
                tracedecay_automation_runtime::automation::effect_runtime::user_job_run_request(
                    &run_id, &job_id,
                )
                .map_err(automation_failed)?,
            )
            .await
            .map_err(automation_failed)?;
            let effect = match admission {
                tracedecay_automation_runtime::automation::effect_runtime::AutomationEffectAdmission::Execute(effect) => effect,
                tracedecay_automation_runtime::automation::effect_runtime::AutomationEffectAdmission::Replay(terminal) => {
                    return automation_terminal_run(&terminal);
                }
                tracedecay_automation_runtime::automation::effect_runtime::AutomationEffectAdmission::PreAdmissionProblem(envelope) => {
                    return Err(DashboardAutomationAuthorityErrorV1::ApplicationProblem(envelope));
                }
                tracedecay_automation_runtime::automation::effect_runtime::AutomationEffectAdmission::Conflict => {
                    return Err(automation_admission_conflict());
                }
            };
            let observer =
                tracedecay_daemon_service::automation_observation::automation_run_observer(
                    Arc::clone(&producer),
                    cg.project_root().to_path_buf(),
                    "dashboard_user_job",
                );
            let retained_run = tracedecay_automation_runtime::automation::jobs::
                run_user_job_with_backend_for_retained_settlement(
                    &cg.store_layout().dashboard_root,
                    config,
                    backend,
                    &job,
                    tracedecay_automation_runtime::automation::jobs::UserJobRunOptions {
                        trigger: AutomationTrigger::Dashboard,
                        run_id: Some(run_id),
                        profile_root: Some(profile_root),
                        project_root: Some(cg.project_root().to_path_buf()),
                        // Dashboard triggers never reach the scheduler
                        // diagnostic append path.
                        occurrence_anchor_run_id: None,
                    },
                )
                .await;
            let waiter =
                effect.start_retained_automation_settlement(retained_run, Some(observer), |run| {
                    (run.ledger_record, run.committed_receipt)
                });
            match waiter.wait().await.map_err(automation_failed)? {
                tracedecay_automation_runtime::automation::effect_runtime::RetainedAutomationSettlementOutcome::Run {
                    terminal,
                    record: _record,
                } => automation_terminal_run(&terminal)?,
                tracedecay_automation_runtime::automation::effect_runtime::RetainedAutomationSettlementOutcome::Problem {
                    problem,
                    record: _record,
                } => return Err(automation_problem(problem)),
                tracedecay_automation_runtime::automation::effect_runtime::RetainedAutomationSettlementOutcome::Reused {
                    record: _record,
                }
                | tracedecay_automation_runtime::automation::effect_runtime::RetainedAutomationSettlementOutcome::AbandonedObserved {
                    record: _record,
                } => {
                    return Err(automation_failed(
                        "dashboard automation cannot reuse a scheduler-only skip",
                    ));
                }
            }
        }
    };
    Ok(run)
}

#[hotpath::measure(label = "daemon.dashboard.automation.skill", future = true)]
async fn execute_dashboard_managed_skill_command(
    host_io: &HostIo,
    profile_root: &Path,
    project_root: &Path,
    command: DashboardManagedSkillCommandV1,
) -> DashboardAutomationResult<DashboardManagedSkillCommandOutcomeV1> {
    let skill = match command {
        DashboardManagedSkillCommandV1::Create { draft, pinned } => {
            let mut skill = draft.materialize().map_err(automation_invalid)?;
            if let Some(pinned) = pinned {
                skill.set_pinned(pinned);
            }
            match save_managed_skill(profile_root, &skill).await {
                Ok(()) => skill,
                Err(error)
                    if managed_skill_dir(profile_root, &skill.metadata.id)
                        .is_ok_and(|path| path.join("skill.json").is_file()) =>
                {
                    return Err(DashboardAutomationAuthorityErrorV1::Conflict {
                        detail: error.to_string(),
                    });
                }
                Err(error) => return Err(automation_failed(error)),
            }
        }
        DashboardManagedSkillCommandV1::Update {
            id,
            base_checksum,
            update,
        } => {
            let current = load_exact_managed_skill(profile_root, &id).await?;
            if current.metadata.checksum != base_checksum {
                return Err(DashboardAutomationAuthorityErrorV1::Conflict {
                    detail: format!("base checksum for managed skill '{id}' is stale"),
                });
            }
            preview_managed_skill_update(&current, &update).map_err(automation_invalid)?;
            match apply_managed_skill_update(profile_root, &id, &base_checksum, update).await {
                Ok(skill) => skill,
                Err(error) => {
                    return match load_exact_managed_skill(profile_root, &id).await {
                        Ok(skill) if skill.metadata.checksum != base_checksum => {
                            Err(DashboardAutomationAuthorityErrorV1::Conflict {
                                detail: format!("base checksum for managed skill '{id}' is stale"),
                            })
                        }
                        Err(error @ DashboardAutomationAuthorityErrorV1::NotFound { .. }) => {
                            Err(error)
                        }
                        _ => Err(automation_failed(error)),
                    };
                }
            }
        }
        DashboardManagedSkillCommandV1::Disable { id } => {
            load_exact_managed_skill(profile_root, &id).await?;
            disable_managed_skill(profile_root, &id)
                .await
                .map_err(|error| managed_skill_lifecycle_error(profile_root, &id, error))?
        }
        DashboardManagedSkillCommandV1::Archive { id } => {
            load_exact_managed_skill(profile_root, &id).await?;
            archive_managed_skill(profile_root, &id)
                .await
                .map_err(|error| managed_skill_lifecycle_error(profile_root, &id, error))?
        }
        DashboardManagedSkillCommandV1::Restore { id } => {
            load_exact_managed_skill(profile_root, &id).await?;
            restore_managed_skill(profile_root, &id)
                .await
                .map_err(|error| managed_skill_lifecycle_error(profile_root, &id, error))?
        }
    };
    let deployment = deploy_managed_skills_to_project(host_io, profile_root, project_root);
    Ok(DashboardManagedSkillCommandOutcomeV1 { skill, deployment })
}

async fn load_exact_managed_skill(
    profile_root: &Path,
    id: &str,
) -> DashboardAutomationResult<ManagedSkill> {
    validate_skill_id(id).map_err(automation_invalid)?;
    let record_path = managed_skill_dir(profile_root, id)
        .map_err(automation_invalid)?
        .join("skill.json");
    if !record_path.is_file() {
        return Err(DashboardAutomationAuthorityErrorV1::NotFound {
            detail: format!("managed skill '{id}' was not found"),
        });
    }
    match load_managed_skill(profile_root, id).await {
        Ok(skill) => Ok(skill),
        Err(_) if !record_path.is_file() => Err(DashboardAutomationAuthorityErrorV1::NotFound {
            detail: format!("managed skill '{id}' was not found"),
        }),
        Err(error) => Err(automation_failed(error)),
    }
}

fn managed_skill_lifecycle_error(
    profile_root: &Path,
    id: &str,
    error: impl std::fmt::Display,
) -> DashboardAutomationAuthorityErrorV1 {
    if managed_skill_dir(profile_root, id).is_ok_and(|path| !path.join("skill.json").is_file()) {
        DashboardAutomationAuthorityErrorV1::NotFound {
            detail: format!("managed skill '{id}' was not found"),
        }
    } else {
        automation_failed(error)
    }
}

fn automation_invalid(error: impl std::fmt::Display) -> DashboardAutomationAuthorityErrorV1 {
    DashboardAutomationAuthorityErrorV1::Invalid {
        detail: error.to_string(),
    }
}

fn automation_failed(error: impl std::fmt::Display) -> DashboardAutomationAuthorityErrorV1 {
    DashboardAutomationAuthorityErrorV1::Failed {
        detail: error.to_string(),
    }
}

fn automation_admission_conflict() -> DashboardAutomationAuthorityErrorV1 {
    DashboardAutomationAuthorityErrorV1::Conflict {
        detail: "automation run identity conflicts with its durable admission".to_owned(),
    }
}

fn automation_terminal_run(
    terminal: &tracedecay_automation_runtime::automation::effect_runtime::AutomationSettledTerminal,
) -> DashboardAutomationResult<tracedecay_contracts::retained_surfaces::AutomationRunResultV1> {
    if let Some(run) = terminal.run_result() {
        return Ok(run.clone());
    }
    if let Some(problem) = terminal.problem() {
        return Err(automation_problem(Box::new(problem.clone())));
    }
    Err(automation_failed(
        "automation terminal has neither a run nor a problem",
    ))
}

fn automation_problem(
    problem: Box<
        tracedecay_automation_runtime::automation::effect_runtime::AutomationSettledProblem,
    >,
) -> DashboardAutomationAuthorityErrorV1 {
    DashboardAutomationAuthorityErrorV1::AutomationProblem(problem)
}

#[cfg(test)]
mod tests {
    #[cfg(all(unix, feature = "test-transport"))]
    use std::collections::BTreeMap;
    #[cfg(all(unix, feature = "test-transport"))]
    use std::ffi::{OsStr, OsString};
    #[cfg(all(unix, feature = "test-transport"))]
    use std::path::PathBuf;
    #[cfg(all(unix, feature = "test-transport"))]
    use std::sync::Arc;

    use super::{
        DashboardAutomationRequestRuntime, dashboard_memory_curator_options,
        dashboard_session_reflector_options, dashboard_skill_writer_options,
    };
    #[cfg(all(unix, feature = "test-transport"))]
    use super::{
        DashboardAutomationRunInvocationV1, DashboardAutomationRunRequestV1,
        DashboardHttpRequestControlV1, dashboard_automation_run_port,
    };
    #[cfg(all(unix, feature = "test-transport"))]
    use tracedecay_automation_runtime::automation::backend::AgentTaskKind;
    use tracedecay_automation_runtime::automation::config::AutomationConfig;
    #[cfg(all(unix, feature = "test-transport"))]
    use tracedecay_automation_runtime::automation::jobs::{AutomationJob, JobDelivery, save_jobs};
    use tracedecay_automation_runtime::automation::run_ledger::AutomationTrigger;
    use tracedecay_automation_runtime::ports::session_evidence::{LcmGrepSort, LcmScope};
    #[cfg(all(unix, feature = "test-transport"))]
    use tracedecay_contracts::retained_surfaces::AutomationRunTerminalV1;
    use tracedecay_contracts::retained_surfaces::{LcmGrepSortV1, LcmRoleV1, LcmSearchScopeV1};

    #[cfg(all(unix, feature = "test-transport"))]
    use std::os::unix::fs::PermissionsExt;

    #[cfg(all(unix, feature = "test-transport"))]
    static USER_JOB_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[cfg(all(unix, feature = "test-transport"))]
    struct EnvVarGuard {
        key: &'static str,
        previous: Option<OsString>,
    }

    #[cfg(all(unix, feature = "test-transport"))]
    impl EnvVarGuard {
        fn set(key: &'static str, value: impl AsRef<OsStr>) -> Self {
            let previous = std::env::var_os(key);
            // Rust 2024 makes process-environment mutation explicitly unsafe.
            // The test-wide lock keeps the fake app-server path stable while
            // this daemon execution journey is in flight.
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, previous }
        }
    }

    #[cfg(all(unix, feature = "test-transport"))]
    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            unsafe {
                if let Some(previous) = self.previous.take() {
                    std::env::set_var(self.key, previous);
                } else {
                    std::env::remove_var(self.key);
                }
            }
        }
    }

    #[cfg(all(unix, feature = "test-transport"))]
    fn install_user_job_codex(temp: &tempfile::TempDir) -> (PathBuf, PathBuf) {
        let script_path = temp.path().join("codex-user-job.py");
        let request_log = temp.path().join("user-job-request.json");
        let script = format!(
            r##"#!/usr/bin/env python3
import json
import pathlib
import sys

if len(sys.argv) != 2 or sys.argv[1] != "app-server":
    sys.exit(42)

request_log = pathlib.Path({request_log})
for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    if method == "initialize":
        print(json.dumps({{"id": message.get("id"), "result": {{}}}}), flush=True)
    elif method == "thread/start":
        print(json.dumps({{
            "id": message.get("id"),
            "result": {{"thread": {{"id": "thread-dashboard-user-job", "model": "dashboard-user-job-model"}}}}
        }}), flush=True)
    elif method == "turn/start":
        request_log.write_text(json.dumps(message), encoding="utf-8")
        print(json.dumps({{
            "method": "item/agentMessage/delta",
            "params": {{"delta": "dashboard user job output", "model": "dashboard-user-job-model"}}
        }}), flush=True)
        print(json.dumps({{"method": "turn/completed"}}), flush=True)
        break
"##,
            request_log = serde_json::to_string(&request_log.display().to_string())
                .expect("encode user-job request log path"),
        );
        std::fs::write(&script_path, script).expect("write user-job fake codex script");
        let mut permissions = std::fs::metadata(&script_path)
            .expect("user-job fake codex metadata")
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&script_path, permissions)
            .expect("make user-job fake codex executable");
        (script_path, request_log)
    }

    #[test]
    fn dashboard_user_job_caps_backend_calls_for_the_wall_budget() {
        let configured = AutomationConfig {
            timeout_secs: 300,
            ..AutomationConfig::default()
        };

        let runtime = DashboardAutomationRequestRuntime::new(&configured);

        assert_eq!(runtime.execution().0.timeout_secs, 120);
    }

    #[test]
    fn dashboard_typed_options_project_explicit_fields() {
        let memory = dashboard_memory_curator_options(Some(42), Some(0.81));
        assert_eq!(memory.trigger, AutomationTrigger::Dashboard);
        assert_eq!(memory.fact_review_limit, 42);
        assert_eq!(memory.min_confidence, 0.81);

        let session = dashboard_session_reflector_options(
            Some("cursor".to_owned()),
            Some("workflow correction".to_owned()),
            Some(7),
            Some(LcmSearchScopeV1::Session),
            Some("session-1".to_owned()),
            Some(true),
            Some(true),
            Some(3),
            Some(LcmGrepSortV1::Relevance),
            Some("codex".to_owned()),
            Some(LcmRoleV1::Assistant),
            Some(10),
            Some(20),
        );
        assert_eq!(session.trigger, AutomationTrigger::Dashboard);
        assert_eq!(session.provider, "cursor");
        assert_eq!(session.query, "workflow correction");
        assert_eq!(session.evidence_limit, 7);
        assert_eq!(session.scope, LcmScope::Session);
        assert_eq!(session.session_id.as_deref(), Some("session-1"));
        assert!(session.include_summaries);
        assert!(session.include_recent_sessions);
        assert_eq!(session.recent_sessions_limit, 3);
        assert_eq!(session.sort, LcmGrepSort::Relevance);
        assert_eq!(session.source.as_deref(), Some("codex"));
        assert_eq!(session.role.as_deref(), Some("assistant"));
        assert_eq!(session.start_time, Some(10));
        assert_eq!(session.end_time, Some(20));

        let skill = dashboard_skill_writer_options(
            Some("all".to_owned()),
            Some("repeated correction".to_owned()),
            Some(9),
            Some(false),
            Some(2),
            std::path::Path::new("/profile"),
        );
        assert_eq!(skill.trigger, AutomationTrigger::Dashboard);
        assert_eq!(skill.provider, "all");
        assert_eq!(skill.query, "repeated correction");
        assert_eq!(skill.evidence_limit, 9);
        assert!(!skill.include_recent_sessions);
        assert_eq!(skill.recent_sessions_limit, 2);
        assert_eq!(
            skill.profile_root.as_deref(),
            Some(std::path::Path::new("/profile"))
        );
    }

    #[cfg(all(unix, feature = "test-transport"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dashboard_user_job_runs_through_daemon_admission_runner_and_settlement() {
        let _env_lock = USER_JOB_ENV_LOCK.lock().await;
        let temp = tempfile::TempDir::new().expect("dashboard user-job fixture");
        let fixture_root = temp
            .path()
            .canonicalize()
            .expect("canonical dashboard user-job fixture root");
        let profile_root = fixture_root.join("profile");
        let project_root = fixture_root.join("project");
        std::fs::create_dir_all(project_root.join("src"))
            .expect("dashboard user-job source directory");
        std::fs::write(project_root.join("src/lib.rs"), "pub fn fixture() {}\n")
            .expect("dashboard user-job source");
        let (fake_codex, request_log) = install_user_job_codex(&temp);
        let _codex_bin = EnvVarGuard::set("TRACEDECAY_CODEX_BIN", &fake_codex);

        let graph = Arc::new(
            crate::project::TraceDecay::init_with_options_for_test(
                &project_root,
                crate::project::TraceDecayOpenOptions {
                    profile_root: Some(profile_root.clone()),
                    global_db_path: Some(profile_root.join("global.db")),
                },
            )
            .await
            .expect("initialize dashboard user-job project"),
        );
        let project_root = graph
            .project_root()
            .canonicalize()
            .expect("canonical dashboard user-job project");
        let dashboard_root = graph.store_layout().dashboard_root.clone();
        let job_id = "dashboard-user-job";
        let run_id = "dashboard_user_job_dashboard-user-job_1000000";
        save_jobs(
            &dashboard_root,
            &[AutomationJob {
                id: job_id.to_owned(),
                name: "Dashboard user job".to_owned(),
                prompt: "Produce a dashboard user-job fixture.".to_owned(),
                schedule: None,
                enabled: true,
                interval_secs: None,
                cooldown_secs: None,
                skill_ids: Vec::new(),
                pre_run_command: None,
                delivery: JobDelivery::default(),
                created_at: 0,
                updated_at: 0,
                extra: BTreeMap::new(),
            }],
        )
        .await
        .expect("persist dashboard user-job fixture");

        let configuration = graph
            .configuration_runtime()
            .client()
            .current()
            .await
            .expect("dashboard user-job configuration");
        let project_id = graph
            .configuration_runtime()
            .configuration_target()
            .project_id
            .clone();
        let scope =
            tracedecay_code_index_runtime::resolved_scope_for_project(&project_root, &project_id)
                .expect("dashboard user-job scope");
        let observed_at = tracedecay_contracts::now_micros();
        let access = tracedecay_daemon_service::daemon_owned_project_source_access_at(
            &scope,
            &project_root,
            &configuration,
            observed_at,
        )
        .expect("dashboard user-job retained access");
        let grant =
            crate::daemon::project_open_owners::project_open_retained_grant(&access, observed_at)
                .expect("dashboard user-job retained grant");
        let profile_id = graph
            .profile_database()
            .binding()
            .shard_id
            .profile_id
            .clone();
        let invocation_service = tracedecay_daemon_service::DaemonInvocationService::default();
        let project_sessions = graph
            .store_runtime_registry()
            .project_sessions(project_id.clone(), [project_root.clone()])
            .await
            .expect("dashboard user-job project sessions");
        let policy_digest = tracedecay_domain::canonical_sha256(&(
            "tracedecay.dashboard-user-job.observability-policy.v1",
            &project_id,
            &access.configuration_digest,
        ))
        .expect("dashboard user-job observability policy");
        invocation_service
            .mount_observability_producer(
                project_root.clone(),
                project_sessions,
                project_id.clone(),
                access.configuration_digest.clone(),
                policy_digest,
            )
            .await
            .expect("mount dashboard user-job observability");
        let retained_ports = tracedecay_daemon_service::retained_owner::retained_surface_ports(
            tracedecay_daemon_service::retained_owner::ProductionRetainedAuthoritiesV1 {
                cg: Arc::new(tokio::sync::RwLock::new(Arc::clone(&graph))),
                project_root: project_root.clone(),
                project_id: project_id.clone(),
                mounted_profile_id: None,
                mounted_session_store_id: None,
                mounted_session_root_id: None,
                registered_session_db: None,
                project_refresh: None,
                project_retrieval: None,
                project_workflow_index: None,
                project_lcm: None,
                #[cfg(feature = "memory-provider-host")]
                provider_control: None,
                configuration_digest: access.configuration_digest.clone(),
                invocation_service: Some(invocation_service.clone()),
            },
        );
        tracedecay_daemon_service::DaemonRetainedRuntimeRegistrar::new(&invocation_service)
            .register(
                profile_id,
                project_root.clone(),
                scope,
                access.requester.clone(),
                grant,
                retained_ports,
            )
            .await
            .expect("register dashboard user-job retained runtime");

        let retained_graph = Arc::clone(&graph);
        let project_resolver: super::DashboardAutomationProjectResolver =
            Arc::new(move |requested_project_root| {
                let retained_graph = Arc::clone(&retained_graph);
                Box::pin(async move {
                    super::validate_dashboard_automation_project(
                        retained_graph,
                        &requested_project_root,
                    )
                })
            });
        let run_port = dashboard_automation_run_port(
            profile_root.clone(),
            project_resolver,
            invocation_service,
        );
        let observed_at = tracedecay_contracts::now_micros();
        let request_id =
            tracedecay_contracts::RequestId::new("request.dashboard-user-job-execution")
                .expect("dashboard user-job request id");
        let cancellation =
            tracedecay_contracts::CancellationSignal::active("cancel.dashboard-user-job-execution")
                .expect("dashboard user-job cancellation");
        let deadline = tracedecay_contracts::Deadline::new(tracedecay_domain::UtcMicros(
            observed_at.0 + 300_000_000,
        ))
        .expect("dashboard user-job deadline");
        let outcome = run_port(DashboardAutomationRunInvocationV1 {
            project_root: project_root.clone(),
            request: DashboardAutomationRunRequestV1::UserJob {
                job_id: job_id.to_owned(),
                run_id: run_id.to_owned(),
            },
            control: DashboardHttpRequestControlV1::from_parts_for_test(
                request_id,
                deadline,
                cancellation,
                observed_at,
            ),
        })
        .await
        .expect("dashboard user-job daemon execution");

        assert_eq!(outcome.run_id.as_str(), run_id);
        assert_eq!(
            outcome.task,
            tracedecay_contracts::retained_surfaces::AutomationTaskV1::UserJob
        );
        assert!(matches!(
            outcome.terminal,
            AutomationRunTerminalV1::Completed { .. }
        ));
        assert!(
            request_log.is_file(),
            "runner must reach the fake app-server"
        );
        let request_text =
            std::fs::read_to_string(&request_log).expect("read dashboard user-job backend request");
        assert!(request_text.contains(job_id));
        assert!(request_text.contains(project_root.to_string_lossy().as_ref()));
        let output_path = dashboard_root
            .join("job-output")
            .join(job_id)
            .join(format!("{run_id}.md"));
        assert_eq!(
            std::fs::read_to_string(&output_path)
                .expect("read delivered dashboard user-job output"),
            "dashboard user job output"
        );
        let record =
            tracedecay_automation_runtime::automation::run_ledger::find_run_record_exact_bounded(
                &dashboard_root,
                run_id,
            )
            .await
            .expect("read settled dashboard user-job record")
            .expect("settled dashboard user-job record");
        assert_eq!(record.task, AgentTaskKind::UserJob);
        assert_eq!(
            record.task_key.as_deref(),
            Some("user_job:dashboard-user-job")
        );
        assert_eq!(record.trigger, AutomationTrigger::Dashboard);
        assert_eq!(
            record.status,
            tracedecay_automation_runtime::automation::run_ledger::AutomationRunStatus::Succeeded
        );
        assert_eq!(record.backend_attempt_count, 1);
        assert!(record.validation_report.as_ref().is_some_and(|report| {
            report["status"] == "delivered" && report["delivery"]["mode"] == "file"
        }));
    }
}
