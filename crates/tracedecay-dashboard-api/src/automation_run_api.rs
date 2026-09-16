use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::Json;
use serde::Deserialize;
use serde_json::{Value, json};

use super::automation_authority::{
    DashboardAutomationRunRequestV1, automation_authority_error_response,
    exact_automation_authority,
};
use super::util::http_detail;
use super::{DashboardHttpRequestControlV1, DashboardState};
use tracedecay_automation_runtime::automation::run_ledger::{
    AutomationRunArtifact, AutomationRunArtifactKind, AutomationRunLedgerRecord, find_run_record,
    read_published_artifact_chain, read_run_artifact_payload,
};
use tracedecay_contracts::retained_surfaces::{LcmGrepSortV1, LcmRoleV1, LcmSearchScopeV1};

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryCuratorRunBody {
    fact_review_limit: Option<usize>,
    min_confidence: Option<f64>,
}

impl From<MemoryCuratorRunBody> for DashboardAutomationRunRequestV1 {
    fn from(body: MemoryCuratorRunBody) -> Self {
        Self::MemoryCurator {
            fact_review_limit: body.fact_review_limit,
            min_confidence: body.min_confidence,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionReflectorRunBody {
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
}

impl From<SessionReflectorRunBody> for DashboardAutomationRunRequestV1 {
    fn from(body: SessionReflectorRunBody) -> Self {
        Self::SessionReflector {
            provider: body.provider,
            query: body.query,
            evidence_limit: body.evidence_limit,
            scope: body.scope,
            session_id: body.session_id,
            include_summaries: body.include_summaries,
            include_recent_sessions: body.include_recent_sessions,
            recent_sessions_limit: body.recent_sessions_limit,
            sort: body.sort,
            source: body.source,
            role: body.role,
            start_time: body.start_time,
            end_time: body.end_time,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillWriterRunBody {
    provider: Option<String>,
    query: Option<String>,
    evidence_limit: Option<usize>,
    include_recent_sessions: Option<bool>,
    recent_sessions_limit: Option<usize>,
}

impl From<SkillWriterRunBody> for DashboardAutomationRunRequestV1 {
    fn from(body: SkillWriterRunBody) -> Self {
        Self::SkillWriter {
            provider: body.provider,
            query: body.query,
            evidence_limit: body.evidence_limit,
            include_recent_sessions: body.include_recent_sessions,
            recent_sessions_limit: body.recent_sessions_limit,
        }
    }
}

#[hotpath::measure(label = "dashboard_api.automation.memory_curator", future = true)]
pub async fn memory_curator(
    State(state): State<DashboardState>,
    axum::Extension(control): axum::Extension<DashboardHttpRequestControlV1>,
    body: Option<axum::extract::Json<MemoryCuratorRunBody>>,
) -> (StatusCode, Json<Value>) {
    let body = body.map(|body| body.0).unwrap_or_default();
    run_dashboard_task_endpoint(state, body.into(), control).await
}

#[hotpath::measure(label = "dashboard_api.automation.session_reflector", future = true)]
pub async fn session_reflection(
    State(state): State<DashboardState>,
    axum::Extension(control): axum::Extension<DashboardHttpRequestControlV1>,
    body: Option<axum::extract::Json<SessionReflectorRunBody>>,
) -> (StatusCode, Json<Value>) {
    let body = body.map(|body| body.0).unwrap_or_default();
    run_dashboard_task_endpoint(state, body.into(), control).await
}

#[hotpath::measure(label = "dashboard_api.automation.skill_writer", future = true)]
pub async fn skill_writing(
    State(state): State<DashboardState>,
    axum::Extension(control): axum::Extension<DashboardHttpRequestControlV1>,
    body: Option<axum::extract::Json<SkillWriterRunBody>>,
) -> (StatusCode, Json<Value>) {
    let body = body.map(|body| body.0).unwrap_or_default();
    run_dashboard_task_endpoint(state, body.into(), control).await
}

async fn run_dashboard_task_endpoint(
    state: DashboardState,
    request: DashboardAutomationRunRequestV1,
    control: DashboardHttpRequestControlV1,
) -> (StatusCode, Json<Value>) {
    let authority = match exact_automation_authority(&state) {
        Ok(authority) => authority,
        Err(error) => return automation_authority_error_response(error),
    };
    match authority.run(&state.project_root, request, control).await {
        Ok(payload) => (StatusCode::OK, Json(json!({ "run": payload }))),
        Err(error) => automation_authority_error_response(error),
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct RunListParams {
    limit: Option<i64>,
}

/// The newest automation runs from the ledger, projected to the fields the
/// run-history surface reads. Heavy per-run payloads (proposed/applied ops,
/// validation reports) stay behind the per-run artifact routes.
#[hotpath::measure(label = "dashboard_api.runs.list", future = true)]
pub async fn run_list(
    State(state): State<DashboardState>,
    axum::extract::Query(params): axum::extract::Query<RunListParams>,
) -> (StatusCode, Json<Value>) {
    let limit = super::util::coerce_limit(params.limit, 50, 200) as usize;
    // The locked ledger tail read is this route's only I/O; row projection
    // after it is linear in the (bounded) page.
    match hotpath::future!(
        tracedecay_automation_runtime::automation::run_ledger::load_run_records_page(
            &state.dashboard_root,
            limit,
        ),
        label = "dashboard_api.runs.ledger_read"
    )
    .await
    {
        Ok(page) => {
            let runs: Vec<Value> = page.records.iter().map(run_history_row).collect();
            let count = runs.len();
            let completeness = if page.is_complete() {
                "known"
            } else {
                "partial"
            };
            (
                StatusCode::OK,
                Json(json!({
                    "runs": runs,
                    "count": count,
                    "limit": limit,
                    "has_more": page.has_more,
                    "malformed_row_count": page.malformed_row_count,
                    "completeness": completeness,
                    "error": "",
                })),
            )
        }
        Err(err) => internal_error(&format!("Failed to read automation run ledger: {err}")),
    }
}

/// One ledger record as the run-history row: identity, outcome, review tallies,
/// and which artifacts exist — every field measured from the record itself.
fn run_history_row(record: &AutomationRunLedgerRecord) -> Value {
    json!({
        "run_id": record.run_id,
        "task": record.task,
        "trigger": record.trigger,
        "backend": record.backend,
        "model": record.model,
        "status": record.status,
        "reviewed_count": record.reviewed_count,
        "accepted_count": record.accepted_count,
        "rejected_count": record.rejected_count,
        "skipped_count": record.skipped_count,
        "error": record.error,
        "started_at": record.started_at,
        "completed_at": record.completed_at,
        "artifact_kinds": record
            .artifacts
            .iter()
            .map(|artifact| artifact.kind.clone())
            .collect::<Vec<_>>(),
    })
}

#[hotpath::measure(label = "dashboard_api.runs.artifacts", future = true)]
pub async fn artifact_list(
    State(state): State<DashboardState>,
    AxumPath(run_id): AxumPath<String>,
) -> (StatusCode, Json<Value>) {
    match find_run_record(&state.dashboard_root, &run_id).await {
        Ok(Some(record)) => {
            let count = record.artifacts.len();
            // Integrity verification re-reads the publication chain from disk
            // on every list call; measure it apart from the record lookup.
            let integrity = hotpath::future!(
                read_published_artifact_chain(&state.dashboard_root, &run_id, None),
                label = "dashboard_api.runs.chain_verify"
            )
            .await;
            let (integrity_status, integrity_verified) = match integrity {
                Ok(Some(published)) if published == record.artifacts => ("verified", true),
                Ok(Some(_)) => ("ledger_publication_mismatch", false),
                Ok(None) => ("publication_unavailable", false),
                Err(_) => ("verification_failed", false),
            };
            (
                StatusCode::OK,
                Json(json!({
                    "run_id": run_id,
                    "artifacts": record.artifacts,
                    "artifact_chain": artifact_chain_summary(
                        &record.artifacts,
                        integrity_status,
                        integrity_verified,
                    ),
                    "count": count,
                    "error": "",
                })),
            )
        }
        Ok(None) => not_found(&format!("automation run '{run_id}' not found")),
        Err(err) => internal_error(&format!("Failed to load automation run artifacts: {err}")),
    }
}

#[hotpath::measure(label = "dashboard_api.runs.artifact", future = true)]
pub async fn artifact_payload(
    State(state): State<DashboardState>,
    AxumPath((run_id, kind)): AxumPath<(String, String)>,
) -> (StatusCode, Json<Value>) {
    let record = match find_run_record(&state.dashboard_root, &run_id).await {
        Ok(Some(record)) => record,
        Ok(None) => {
            return not_found(&format!("automation run '{run_id}' not found"));
        }
        Err(err) => {
            return internal_error(&format!("Failed to load automation run artifact: {err}"));
        }
    };
    let Some(artifact) = find_artifact(&record.artifacts, &kind) else {
        return not_found(&format!(
            "automation run artifact '{kind}' not found for run '{run_id}'"
        ));
    };
    // Heavy per-run payloads (proposed/applied ops, validation reports) are
    // read and parsed here; this span scales with artifact size while the
    // surrounding handler phases stay fixed-price.
    match hotpath::future!(
        read_run_artifact_payload(&state.dashboard_root, &run_id, artifact),
        label = "dashboard_api.runs.artifact_read"
    )
    .await
    {
        Ok(payload) => (
            StatusCode::OK,
            Json(json!({
                "run_id": run_id,
                "artifact": artifact,
                "payload": payload,
                "error": "",
            })),
        ),
        Err(err) => internal_error(&format!("Failed to read automation run artifact: {err}")),
    }
}

fn not_found(message: &str) -> (StatusCode, Json<Value>) {
    (StatusCode::NOT_FOUND, Json(http_detail(message)))
}

fn internal_error(message: &str) -> (StatusCode, Json<Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(http_detail(message)),
    )
}

fn find_artifact<'a>(
    artifacts: &'a [AutomationRunArtifact],
    kind: &str,
) -> Option<&'a AutomationRunArtifact> {
    artifacts.iter().find(|artifact| artifact.kind == kind)
}

fn artifact_chain_summary(
    artifacts: &[AutomationRunArtifact],
    integrity_status: &str,
    integrity_verified: bool,
) -> Value {
    let expected_kinds = expected_artifact_chain_kinds();
    let present_kinds = artifacts
        .iter()
        .map(|artifact| artifact.kind.as_str())
        .collect::<Vec<_>>();
    let complete = expected_kinds
        .iter()
        .all(|expected| present_kinds.iter().any(|present| present == expected));
    json!({
        "expected_kinds": expected_kinds,
        "present_kinds": present_kinds,
        "metadata_complete": complete,
        "complete": complete && integrity_verified,
        "integrity_status": integrity_status,
    })
}

fn expected_artifact_chain_kinds() -> Vec<&'static str> {
    vec![
        AutomationRunArtifactKind::Traces.as_str(),
        AutomationRunArtifactKind::Feedback.as_str(),
        AutomationRunArtifactKind::GeneratedEvals.as_str(),
        AutomationRunArtifactKind::ValidationGate.as_str(),
        AutomationRunArtifactKind::OptimizerDiagnosis.as_str(),
        AutomationRunArtifactKind::CodexHandoff.as_str(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_run_bodies_project_current_typed_options() {
        let memory = serde_json::from_value::<MemoryCuratorRunBody>(json!({
            "fact_review_limit": 12,
            "min_confidence": 0.72,
        }))
        .expect("memory-curator body");
        assert_eq!(
            DashboardAutomationRunRequestV1::from(memory),
            DashboardAutomationRunRequestV1::MemoryCurator {
                fact_review_limit: Some(12),
                min_confidence: Some(0.72),
            }
        );

        let reflector = serde_json::from_value::<SessionReflectorRunBody>(json!({
            "provider": "cursor",
            "query": "workflow correction",
            "evidence_limit": 7,
            "scope": "all",
            "session_id": "session-1",
            "include_summaries": true,
            "include_recent_sessions": true,
            "recent_sessions_limit": 3,
            "sort": "hybrid",
            "source": "codex",
            "role": "assistant",
            "start_time": 10,
            "end_time": 20,
        }))
        .expect("session-reflection body");
        assert_eq!(
            DashboardAutomationRunRequestV1::from(reflector),
            DashboardAutomationRunRequestV1::SessionReflector {
                provider: Some("cursor".to_owned()),
                query: Some("workflow correction".to_owned()),
                evidence_limit: Some(7),
                scope: Some(LcmSearchScopeV1::All),
                session_id: Some("session-1".to_owned()),
                include_summaries: Some(true),
                include_recent_sessions: Some(true),
                recent_sessions_limit: Some(3),
                sort: Some(LcmGrepSortV1::Hybrid),
                source: Some("codex".to_owned()),
                role: Some(LcmRoleV1::Assistant),
                start_time: Some(10),
                end_time: Some(20),
            }
        );

        let writer = serde_json::from_value::<SkillWriterRunBody>(json!({
            "provider": "all",
            "query": "repeated correction",
            "evidence_limit": 9,
            "include_recent_sessions": false,
            "recent_sessions_limit": 2,
        }))
        .expect("skill-writing body");
        assert_eq!(
            DashboardAutomationRunRequestV1::from(writer),
            DashboardAutomationRunRequestV1::SkillWriter {
                provider: Some("all".to_owned()),
                query: Some("repeated correction".to_owned()),
                evidence_limit: Some(9),
                include_recent_sessions: Some(false),
                recent_sessions_limit: Some(2),
            }
        );
    }

    #[test]
    fn manual_run_bodies_reject_unregistered_storage_selectors() {
        for body in [
            json!({"hermes_home": "/tmp/hermes"}),
            json!({"storage_scope": "hermes_profile"}),
            json!({"unsupported_field": true}),
        ] {
            assert!(
                serde_json::from_value::<SkillWriterRunBody>(body).is_err(),
                "unregistered skill-writer body field must be rejected"
            );
        }
    }
}
