use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tracedecay_automation_runtime::automation::AutomationRunControl;
use tracedecay_contracts::{CancellationSignal, Deadline, now_micros};
use tracedecay_domain::ProvenanceId;
use tracedecay_semantic_contracts::{
    ModelArtifactManifestV1, SemanticModelLifecycleStatusV1, SemanticRuntimeStatusProjectionV1,
};
use tracedecay_store::{ProjectMemoryAutomaticFactReceiptV1, ProjectMemoryAutomaticFactStateV1};

use tracedecay_domain::errors::{Result, TraceDecayError};
use tracedecay_global_db::RegisteredGlobalDb;
use tracedecay_project::project::TraceDecay;
use tracedecay_session_memory::fact_store::DatabaseFactStore;
use tracedecay_session_memory::memory::{MemoryApplication, MemoryApplicationError};

use super::json_result;
use crate::ToolResult;

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum AdminProjectAction {
    CounterGet,
    CounterReset,
    StatusAccounting,
    GitignoreStatus,
    Bench {
        queries_toml: Option<String>,
        json: bool,
        max_nodes: usize,
    },
    AutomaticFactReceiptList {
        state: Option<String>,
        limit: usize,
    },
    AutomaticFactReceiptView {
        id: String,
    },
    AutomationReconcile {
        scope: tracedecay_dashboard_api::AutomationReconcileScope,
    },
    SemanticStatus,
    SemanticAcquire,
    SemanticImport {
        manifest: ModelArtifactManifestV1,
        source: PathBuf,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SemanticAdminRequestV1 {
    Status,
    Acquire,
    Import {
        manifest: ModelArtifactManifestV1,
        source: PathBuf,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum SemanticAdminResponseV1 {
    Status {
        /// The runtime actually mounted for this route. `None` means the
        /// configured state has not produced a mounted runtime yet (for
        /// example, before the required daemon restart after enablement).
        lifecycle: Option<SemanticModelLifecycleStatusV1>,
        /// Index-generation scheduling and serving state observed from the
        /// same mounted runtime as `lifecycle`.
        runtime: Option<SemanticRuntimeStatusProjectionV1>,
    },
    Acquisition {
        queued: bool,
        lifecycle: SemanticModelLifecycleStatusV1,
    },
    Import {
        lifecycle: SemanticModelLifecycleStatusV1,
    },
}

pub type SemanticAdminFutureV1 =
    Pin<Box<dyn Future<Output = Result<SemanticAdminResponseV1>> + Send>>;
pub type SemanticAdminExecutorV1 = Arc<
    dyn Fn(SemanticAdminRequestV1, Deadline, CancellationSignal) -> SemanticAdminFutureV1
        + Send
        + Sync,
>;

fn project_memory_application<'a>(
    cg: &TraceDecay,
    db: &'a tracedecay_runtime_core::db::Database,
) -> Result<MemoryApplication<DatabaseFactStore<'a>>> {
    let owner = cg.project_memory_owner()?;
    MemoryApplication::new(owner, DatabaseFactStore::new(db))
        .map_err(|error| memory_application_error(&error))
}

fn memory_application_error(error: &MemoryApplicationError) -> TraceDecayError {
    TraceDecayError::Config {
        message: format!("project memory application failed: {error}"),
    }
}

fn admin_project_run_control(
    deadline: Deadline,
    cancellation: CancellationSignal,
) -> AutomationRunControl {
    AutomationRunControl::from_interrupted(Arc::new(move || {
        cancellation.is_cancelled() || deadline.is_elapsed_at(now_micros())
    }))
}

fn parse_automatic_fact_apply_id(value: String) -> Result<ProvenanceId> {
    ProvenanceId::new(value).map_err(|error| TraceDecayError::Config {
        message: format!("invalid automatic fact apply id: {error}"),
    })
}

fn parse_automatic_fact_state(value: &str) -> Result<ProjectMemoryAutomaticFactStateV1> {
    let normalized = value.trim().replace('-', "_");
    match normalized.as_str() {
        "applied" => Ok(ProjectMemoryAutomaticFactStateV1::Applied),
        "quarantined" => Ok(ProjectMemoryAutomaticFactStateV1::Quarantined),
        _ => Err(TraceDecayError::Config {
            message: format!(
                "invalid automatic fact state `{value}`; expected applied or quarantined"
            ),
        }),
    }
}

fn automatic_fact_state_name(state: ProjectMemoryAutomaticFactStateV1) -> &'static str {
    match state {
        ProjectMemoryAutomaticFactStateV1::Applied => "applied",
        ProjectMemoryAutomaticFactStateV1::Quarantined => "quarantined",
    }
}

fn automatic_fact_receipt_json(receipt: &ProjectMemoryAutomaticFactReceiptV1) -> Value {
    let request = receipt.request();
    let mut value = Map::from_iter([
        (
            "apply_id".to_owned(),
            Value::String(receipt.apply_id().as_str().to_owned()),
        ),
        (
            "state".to_owned(),
            Value::String(automatic_fact_state_name(receipt.state()).to_owned()),
        ),
        (
            "operation_id".to_owned(),
            Value::String(request.operation_id().as_str().to_owned()),
        ),
        (
            "add_fact_request".to_owned(),
            json!({
                "content": request.content(),
                "category": request.category(),
                "source_label": request.source_label(),
                "tags": request.tags(),
                "entities": request.entities(),
                "trust": request.default_trust(),
                "metadata": request.metadata(),
            }),
        ),
        ("evidence".to_owned(), json!(receipt.evidence())),
        (
            "recorded_at_micros".to_owned(),
            json!(receipt.recorded_at().0),
        ),
    ]);
    if let Some(fact_id) = receipt.applied_fact_id() {
        value.insert(
            "applied_fact_id".to_owned(),
            Value::String(fact_id.as_str().to_owned()),
        );
    }
    if let Some(reason) = receipt.quarantine_reason() {
        value.insert(
            "quarantine_reason".to_owned(),
            Value::String(reason.to_owned()),
        );
    }
    Value::Object(value)
}

#[hotpath::measure(future = true, label = "mcp.admin.project.total")]
#[cfg_attr(
    not(feature = "hotpath"),
    expect(
        clippy::too_many_lines,
        reason = "Admin project handling is one action match onto registry and store owners."
    )
)]
pub async fn handle_admin_project(
    cg: &TraceDecay,
    args: Value,
    global_db: Option<&RegisteredGlobalDb>,
    automation_scheduler_reconciler: Option<
        tracedecay_dashboard_api::AutomationSchedulerReconciler,
    >,
    semantic_admin: Option<SemanticAdminExecutorV1>,
    application_deadline: Deadline,
    application_cancellation: CancellationSignal,
) -> Result<ToolResult> {
    let run_control = admin_project_run_control(
        application_deadline.clone(),
        application_cancellation.clone(),
    );
    let action: AdminProjectAction =
        serde_json::from_value(args).map_err(|error| TraceDecayError::Config {
            message: format!("invalid tracedecay_admin_project arguments: {error}"),
        })?;
    let value = match action {
        AdminProjectAction::CounterGet => json!({ "counter": cg.get_local_counter().await? }),
        AdminProjectAction::CounterReset => {
            cg.reset_local_counter().await?;
            json!({ "reset": true })
        }
        AdminProjectAction::AutomationReconcile { scope } => {
            if scope != tracedecay_dashboard_api::AutomationReconcileScope::Project {
                return Err(TraceDecayError::Config {
                    message:
                        "profile automation reconciliation requires a projectless daemon request"
                            .to_string(),
                });
            }
            let outcome = match automation_scheduler_reconciler {
                Some(reconcile) => reconcile().await,
                None => {
                    tracedecay_dashboard_api::AutomationSchedulerReconcileOutcome::OwnerUnavailable
                }
            };
            json!({ "scope": "project", "outcome": outcome })
        }
        AdminProjectAction::StatusAccounting => {
            let global_db = global_db.ok_or_else(|| TraceDecayError::Config {
                message: "daemon global database is unavailable".to_string(),
            })?;
            let tokens_saved = cg.get_tokens_saved().await?;
            // An explicit accounting status action fails closed: a registry
            // it cannot write or read is an error, not a null total.
            global_db
                .try_upsert_project_tokens(cg.project_root(), tokens_saved)
                .await?;
            let global_tokens_saved = global_db
                .try_global_tokens_saved()
                .await
                .map_err(|message| TraceDecayError::Config { message })
                .map(|total| total.saturating_sub(tokens_saved))
                .map(|total| (total > 0).then_some(total))?;
            json!({
                "tokens_saved": tokens_saved,
                "global_tokens_saved": global_tokens_saved,
            })
        }
        AdminProjectAction::GitignoreStatus => {
            let configuration = cg
                .configuration_runtime()
                .client()
                .current()
                .await
                .map_err(|error| TraceDecayError::Config {
                    message: format!("configuration authority unavailable: {error}"),
                })?;
            json!({
                "git_ignore": configuration.config().git_ignore,
                "revision_id": configuration.revision_id().as_str(),
            })
        }
        AdminProjectAction::Bench {
            queries_toml,
            json,
            max_nodes,
        } => {
            let report = crate::bench::run_bench_with_toml(
                cg,
                queries_toml
                    .as_deref()
                    .unwrap_or(crate::bench::DEFAULT_QUERIES_TOML),
                crate::bench::BenchOptions {
                    format: crate::bench::OutputFormat::Json,
                    max_nodes,
                },
            )?;
            let output = if json {
                crate::bench::format_report_json(&report)
            } else {
                crate::bench::format_report_console(&report)
            };
            json!({ "output": output })
        }
        AdminProjectAction::AutomaticFactReceiptList { state, limit } => {
            let db = cg.open_project_store_db()?;
            let memory = project_memory_application(cg, &db)?;
            let state = state
                .as_deref()
                .map(parse_automatic_fact_state)
                .transpose()?;
            let page = memory
                .list_project_memory_automatic_fact_receipts(
                    state,
                    None,
                    limit,
                    run_control.read_control(),
                )
                .await
                .map_err(|error| memory_application_error(&error))?;
            let receipts = page
                .receipts()
                .iter()
                .map(automatic_fact_receipt_json)
                .collect::<Vec<_>>();
            json!({
                "availability": { "state": "available" },
                "count": receipts.len(),
                "receipts": receipts,
                "next_after_apply_id": page
                    .next_after_apply_id()
                    .map(ProvenanceId::as_str),
            })
        }
        AdminProjectAction::AutomaticFactReceiptView { id } => {
            let apply_id = parse_automatic_fact_apply_id(id)?;
            let db = cg.open_project_store_db()?;
            let memory = project_memory_application(cg, &db)?;
            let receipt = memory
                .get_project_memory_automatic_fact_receipt(apply_id, run_control.read_control())
                .await
                .map_err(|error| memory_application_error(&error))?
                .ok_or_else(|| TraceDecayError::Config {
                    message: "automatic fact receipt not found".to_string(),
                })?;
            json!({ "receipt": automatic_fact_receipt_json(&receipt) })
        }
        AdminProjectAction::SemanticStatus => {
            execute_semantic_admin(
                semantic_admin,
                SemanticAdminRequestV1::Status,
                application_deadline,
                application_cancellation,
            )
            .await?
        }
        AdminProjectAction::SemanticAcquire => {
            execute_semantic_admin(
                semantic_admin,
                SemanticAdminRequestV1::Acquire,
                application_deadline,
                application_cancellation,
            )
            .await?
        }
        AdminProjectAction::SemanticImport { manifest, source } => {
            validate_semantic_import_source(&source)?;
            execute_semantic_admin(
                semantic_admin,
                SemanticAdminRequestV1::Import { manifest, source },
                application_deadline,
                application_cancellation,
            )
            .await?
        }
    };
    Ok(json_result(&value))
}

async fn execute_semantic_admin(
    executor: Option<SemanticAdminExecutorV1>,
    request: SemanticAdminRequestV1,
    deadline: Deadline,
    cancellation: CancellationSignal,
) -> Result<Value> {
    let executor = executor.ok_or_else(|| TraceDecayError::Config {
        message: "semantic runtime is unavailable for this project".to_owned(),
    })?;
    let response = executor(request, deadline, cancellation).await?;
    serde_json::to_value(response).map_err(TraceDecayError::from)
}

fn validate_semantic_import_source(source: &Path) -> Result<()> {
    if !source.is_absolute() {
        return Err(TraceDecayError::Config {
            message: "semantic artifact source must be an absolute path".to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_application_control() -> (Deadline, CancellationSignal) {
        (
            Deadline::new(tracedecay_domain::UtcMicros(i64::MAX)).unwrap(),
            CancellationSignal::active("cancel.admin-project-test").unwrap(),
        )
    }

    fn tool_json(result: &ToolResult) -> Value {
        let text = result.value["content"][0]["text"]
            .as_str()
            .expect("admin project result should contain JSON text");
        serde_json::from_str(text).expect("admin project result should be valid JSON")
    }

    async fn seed_automatic_fact_receipt(
        cg: &TraceDecay,
        apply_id: &str,
        content: &str,
    ) -> ProjectMemoryAutomaticFactReceiptV1 {
        use tracedecay_domain::{ActorId, Confidence, FactCategoryV1};
        use tracedecay_session_memory::memory::ProjectMemoryFactAddRequest;

        let owner = cg.project_memory_owner().unwrap();
        let db = cg.open_project_store_db().unwrap();
        let memory = MemoryApplication::new(owner.clone(), DatabaseFactStore::new(&db)).unwrap();
        let actor = ActorId::new("automation.session-reflector".to_owned()).unwrap();
        let request = tracedecay_session_memory::memory::automatic_fact_add_command(
            owner,
            ProjectMemoryFactAddRequest {
                content: content.to_owned(),
                category: FactCategoryV1::Decision,
                source_label: Some("admin-project-test".to_owned()),
                tags: Vec::new(),
                entities: Vec::new(),
                trust: Some(Confidence::new(0.9).unwrap()),
                metadata: json!({}),
            },
            "run.admin-project-test",
            apply_id,
            Some(actor),
        )
        .unwrap();
        let run_control = AutomationRunControl::from_interrupted(Arc::new(|| false));
        let write_control = run_control.write_control();
        memory
            .apply_project_memory_automatic_fact(
                ProvenanceId::new(apply_id.to_owned()).unwrap(),
                request,
                tracedecay_store::ProjectMemoryAutomaticFactEvidenceV1::default(),
                &write_control,
            )
            .await
            .unwrap()
            .receipt()
            .clone()
    }

    #[tokio::test]
    async fn admin_project_handler_reads_terminal_automatic_fact_receipts() {
        let temp = tempfile::tempdir().unwrap();
        let project_root = temp.path().join("project");
        let profile_root = temp.path().join("profile");
        std::fs::create_dir_all(&project_root).unwrap();
        std::fs::create_dir_all(&profile_root).unwrap();
        let project_root = std::fs::canonicalize(project_root).unwrap();
        let profile_root = std::fs::canonicalize(profile_root).unwrap();
        let cg = TraceDecay::init_with_options_for_test(
            &project_root,
            tracedecay_project::project::TraceDecayOpenOptions {
                global_db_path: Some(profile_root.join("global.db")),
                profile_root: Some(profile_root),
            },
        )
        .await
        .unwrap();
        let owner_before =
            tracedecay_runtime_core::db::probe_writer_owner(&cg.store_layout().graph_db_path)
                .unwrap();

        let apply_id = "automatic-fact.rpc.read-only";
        seed_automatic_fact_receipt(
            &cg,
            apply_id,
            "Admin project RPC reads this terminal automatic fact receipt",
        )
        .await;

        let (deadline, cancellation) = test_application_control();
        let applied = tool_json(
            &handle_admin_project(
                &cg,
                json!({
                    "action": "automatic_fact_receipt_list",
                    "state": "applied",
                    "limit": 50,
                }),
                None,
                None,
                None,
                deadline,
                cancellation,
            )
            .await
            .unwrap(),
        );
        assert_eq!(applied["count"], 1);
        assert_eq!(applied["availability"]["state"], "available");
        assert!(
            applied["receipts"]
                .as_array()
                .unwrap()
                .iter()
                .all(|receipt| receipt["state"] == "applied")
        );

        let (deadline, cancellation) = test_application_control();
        let viewed = tool_json(
            &handle_admin_project(
                &cg,
                json!({ "action": "automatic_fact_receipt_view", "id": apply_id }),
                None,
                None,
                None,
                deadline,
                cancellation,
            )
            .await
            .unwrap(),
        );
        assert_eq!(viewed["receipt"]["apply_id"], apply_id);
        assert_eq!(viewed["receipt"]["state"], "applied");
        assert_eq!(
            viewed["receipt"]["add_fact_request"]["content"],
            "Admin project RPC reads this terminal automatic fact receipt"
        );
        assert_eq!(
            viewed["receipt"]["add_fact_request"]["category"],
            "decision"
        );
        assert_eq!(viewed["receipt"]["add_fact_request"]["trust"], json!(0.9));
        assert_eq!(
            viewed["receipt"]["add_fact_request"]["source_label"],
            "admin-project-test"
        );
        assert!(viewed["receipt"]["applied_fact_id"].is_string());

        for action in [
            json!({ "action": "fact_apply", "id": apply_id }),
            json!({
                "action": "fact_reject",
                "id": apply_id,
                "reason": "not durable",
            }),
        ] {
            let (deadline, cancellation) = test_application_control();
            assert!(
                handle_admin_project(&cg, action, None, None, None, deadline, cancellation,)
                    .await
                    .is_err(),
                "manual fact mutations must not be accepted"
            );
        }

        let owner_after =
            tracedecay_runtime_core::db::probe_writer_owner(&cg.store_layout().graph_db_path)
                .unwrap();
        assert_eq!(owner_after, owner_before);
    }

    #[test]
    fn admin_project_wire_contract_round_trips_typed_results_without_local_fallback() {
        assert!(matches!(
            serde_json::from_value::<AdminProjectAction>(json!({ "action": "gitignore_status" }))
                .unwrap(),
            AdminProjectAction::GitignoreStatus
        ));

        for retired_action in [
            json!({
                "action": "memory_curate",
                "apply": true,
                "llm": false,
                "llm_ops": null,
                "fact_review_limit": 12,
                "min_confidence": 0.75,
            }),
            json!({ "action": "fact_apply", "id": "fact_1" }),
            json!({
                "action": "fact_reject",
                "id": "fact_1",
                "reason": "not durable",
            }),
            json!({ "action": "fact_list", "state": "applied", "limit": 50 }),
            json!({ "action": "fact_view", "id": "fact_1" }),
            json!({
                "action": "automation_run",
                "task": "session_reflection",
                "options": {
                    "provider": "claude",
                    "query": "decisions",
                    "evidence_limit": 11,
                    "scope": "session",
                    "session_id": "session-3",
                    "include_summaries": false,
                    "sort": "hybrid",
                    "source": "assistant",
                    "role": "user",
                    "start_time": 10,
                    "end_time": 20
                }
            }),
            json!({
                "action": "automation_run",
                "task": "skill_writing",
                "options": {
                    "provider": "all",
                    "query": "repeated workflow",
                    "evidence_limit": 13
                }
            }),
        ] {
            assert!(serde_json::from_value::<AdminProjectAction>(retired_action).is_err());
        }

        let list = serde_json::from_value::<AdminProjectAction>(json!({
            "action": "automatic_fact_receipt_list",
            "state": "applied",
            "limit": 50,
        }))
        .unwrap();
        assert!(matches!(
            list,
            AdminProjectAction::AutomaticFactReceiptList { state: Some(state), limit: 50 }
                if state == "applied"
        ));
        let view = serde_json::from_value::<AdminProjectAction>(json!({
            "action": "automatic_fact_receipt_view",
            "id": "fact_1",
        }))
        .unwrap();
        assert!(matches!(
            view,
            AdminProjectAction::AutomaticFactReceiptView { id } if id == "fact_1"
        ));

        assert!(
            serde_json::from_value::<AdminProjectAction>(json!({
                "action": "automation_run",
                "task": "memory_curation",
                "options": { "fact_review_limit": 12, "min_confidence": 0.75 }
            }))
            .is_err()
        );
        assert!(matches!(
            serde_json::from_value::<AdminProjectAction>(json!({
                "action": "automation_reconcile",
                "scope": "project"
            }))
            .unwrap(),
            AdminProjectAction::AutomationReconcile {
                scope: tracedecay_dashboard_api::AutomationReconcileScope::Project
            }
        ));
        assert!(matches!(
            serde_json::from_value::<AdminProjectAction>(json!({
                "action": "semantic_status"
            }))
            .unwrap(),
            AdminProjectAction::SemanticStatus
        ));
        assert!(matches!(
            serde_json::from_value::<AdminProjectAction>(json!({
                "action": "semantic_acquire"
            }))
            .unwrap(),
            AdminProjectAction::SemanticAcquire
        ));
        assert!(
            serde_json::from_value::<AdminProjectAction>(json!({
                "action": "semantic_import",
                "manifest": { "payload": {} },
                "source": "/tmp/model"
            }))
            .is_err(),
            "semantic import must decode through the canonical manifest contract"
        );
    }

    #[tokio::test]
    async fn semantic_admin_fails_closed_without_the_mounted_runtime_executor() {
        let (deadline, cancellation) = test_application_control();
        let error =
            execute_semantic_admin(None, SemanticAdminRequestV1::Status, deadline, cancellation)
                .await
                .expect_err("missing mounted semantic runtime must fail");
        assert!(
            error
                .to_string()
                .contains("semantic runtime is unavailable")
        );
    }

    #[test]
    fn semantic_import_requires_an_absolute_source_before_executor_dispatch() {
        let error = validate_semantic_import_source(Path::new("relative/model"))
            .expect_err("relative semantic import source must be rejected");
        assert!(error.to_string().contains("must be an absolute path"));
        validate_semantic_import_source(&std::env::current_dir().expect("absolute current dir"))
            .expect("absolute source reaches the daemon executor for canonicalization");
    }

    #[test]
    fn semantic_status_serializes_unmounted_observations_explicitly() {
        assert_eq!(
            serde_json::to_value(SemanticAdminResponseV1::Status {
                lifecycle: None,
                runtime: None,
            })
            .unwrap(),
            json!({
                "outcome": "status",
                "lifecycle": null,
                "runtime": null,
            })
        );
    }

    #[test]
    fn automation_admin_actions_have_stable_strict_schemas() {
        assert!(
            serde_json::from_value::<AdminProjectAction>(json!({
                "action": "fact_apply",
                "id": "fact_1"
            }))
            .is_err()
        );
        for retired in ["pending_approval", "applying", "rejected_validation"] {
            assert!(parse_automatic_fact_state(retired).is_err());
        }
        assert_eq!(
            parse_automatic_fact_state("applied").unwrap(),
            ProjectMemoryAutomaticFactStateV1::Applied
        );
        assert_eq!(
            parse_automatic_fact_state(" quarantined ").unwrap(),
            ProjectMemoryAutomaticFactStateV1::Quarantined
        );
    }
}
