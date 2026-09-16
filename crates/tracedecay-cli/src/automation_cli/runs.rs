use super::{daemon_automation_run, daemon_project_dashboard_root};
use crate::cli::{AutomationRunAction, AutomationRunsAction};
use crate::resolve_cli_project_root;

pub(super) fn automation_run_rpc_request(
    action: AutomationRunAction,
) -> tracedecay_domain::errors::Result<(Option<String>, serde_json::Value)> {
    let (path, task, options) = match action {
        AutomationRunAction::MemoryCuration {
            fact_review_limit,
            min_confidence,
            path,
        } => {
            validate_fact_review_limit(fact_review_limit)?;
            validate_min_confidence(min_confidence)?;
            (
                path,
                "memory_curator",
                serde_json::json!({
                    "fact_review_limit": fact_review_limit,
                    "min_confidence": min_confidence,
                }),
            )
        }
        AutomationRunAction::SessionReflection {
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
            path,
        } => {
            validate_text_option("provider", &provider)?;
            validate_text_option("query", &query)?;
            validate_evidence_limit(evidence_limit)?;
            validate_scope(&scope)?;
            validate_sort(&sort)?;
            validate_recent_sessions_limit(recent_sessions_limit)?;
            if let Some(role) = role.as_deref() {
                validate_role(role)?;
            }
            if let (Some(start_time), Some(end_time)) = (start_time, end_time)
                && start_time > end_time
            {
                return Err(config_error(
                    "session-reflection --start-time must be no later than --end-time",
                ));
            }
            (
                path,
                "session_reflector",
                serde_json::json!({
                    "provider": provider,
                    "query": query,
                    "evidence_limit": evidence_limit,
                    "scope": scope,
                    "session_id": session_id,
                    "include_summaries": include_summaries,
                    "include_recent_sessions": include_recent_sessions,
                    "recent_sessions_limit": recent_sessions_limit,
                    "sort": sort,
                    "source": source,
                    "role": role,
                    "start_time": start_time,
                    "end_time": end_time,
                }),
            )
        }
        AutomationRunAction::SkillWriting {
            provider,
            query,
            evidence_limit,
            include_recent_sessions,
            recent_sessions_limit,
            path,
        } => {
            validate_text_option("provider", &provider)?;
            validate_text_option("query", &query)?;
            validate_evidence_limit(evidence_limit)?;
            validate_recent_sessions_limit(recent_sessions_limit)?;
            (
                path,
                "skill_writer",
                serde_json::json!({
                    "provider": provider,
                    "query": query,
                    "evidence_limit": evidence_limit,
                    "include_recent_sessions": include_recent_sessions,
                    "recent_sessions_limit": recent_sessions_limit,
                }),
            )
        }
    };

    Ok((
        path,
        serde_json::json!({
            "task": task,
            "options": options,
        }),
    ))
}

pub(super) async fn handle_automation_run_command(
    action: AutomationRunAction,
) -> tracedecay_domain::errors::Result<()> {
    let (path, args) = automation_run_rpc_request(action)?;
    let project_path = resolve_cli_project_root(path, None, None).await?;
    let payload = daemon_automation_run(&project_path, args).await?;
    let run = automation_run_result(&payload)?;
    println!("{}", serde_json::to_string_pretty(run)?);
    Ok(())
}

fn automation_run_result(
    payload: &serde_json::Value,
) -> tracedecay_domain::errors::Result<&serde_json::Value> {
    payload
        .get("run")
        .ok_or_else(|| config_error("daemon automation response omitted run"))
}

fn config_error(message: impl Into<String>) -> tracedecay_domain::errors::TraceDecayError {
    tracedecay_domain::errors::TraceDecayError::Config {
        message: message.into(),
    }
}

fn validate_fact_review_limit(limit: usize) -> tracedecay_domain::errors::Result<()> {
    if (1..=1_000).contains(&limit) {
        Ok(())
    } else {
        Err(config_error(
            "memory-curation --fact-review-limit must be between 1 and 1000",
        ))
    }
}

fn validate_min_confidence(confidence: f64) -> tracedecay_domain::errors::Result<()> {
    if confidence.is_finite() && (0.0..=1.0).contains(&confidence) {
        Ok(())
    } else {
        Err(config_error(
            "memory-curation --min-confidence must be between 0 and 1",
        ))
    }
}

fn validate_evidence_limit(limit: usize) -> tracedecay_domain::errors::Result<()> {
    if (1..=50).contains(&limit) {
        Ok(())
    } else {
        Err(config_error(
            "automation evidence limit must be between 1 and 50",
        ))
    }
}

fn validate_recent_sessions_limit(limit: usize) -> tracedecay_domain::errors::Result<()> {
    if (1..=10).contains(&limit) {
        Ok(())
    } else {
        Err(config_error(
            "automation recent-sessions limit must be between 1 and 10",
        ))
    }
}

fn validate_scope(scope: &str) -> tracedecay_domain::errors::Result<()> {
    if matches!(scope, "all" | "session" | "current") {
        Ok(())
    } else {
        Err(config_error(format!(
            "invalid session-reflection --scope '{scope}'; expected all, session, or current"
        )))
    }
}

fn validate_sort(sort: &str) -> tracedecay_domain::errors::Result<()> {
    if matches!(sort, "recency" | "relevance" | "hybrid") {
        Ok(())
    } else {
        Err(config_error(format!(
            "invalid session-reflection --sort '{sort}'; expected recency, relevance, or hybrid"
        )))
    }
}

fn validate_role(role: &str) -> tracedecay_domain::errors::Result<()> {
    if matches!(role, "system" | "user" | "assistant" | "tool" | "unknown") {
        Ok(())
    } else {
        Err(config_error(format!(
            "invalid session-reflection --role '{role}'; expected system, user, assistant, tool, or unknown"
        )))
    }
}

fn validate_text_option(name: &str, value: &str) -> tracedecay_domain::errors::Result<()> {
    let trimmed = value.trim();
    if !trimmed.is_empty() && trimmed.len() <= 4_096 && !trimmed.chars().any(char::is_control) {
        Ok(())
    } else {
        Err(config_error(format!(
            "automation --{name} must be non-empty text"
        )))
    }
}

pub(super) async fn handle_automation_runs_command(
    action: AutomationRunsAction,
) -> tracedecay_domain::errors::Result<()> {
    use tracedecay_automation_runtime::automation::run_ledger::{
        find_run_record, load_run_records, read_run_artifact_payload,
    };

    let path = match &action {
        AutomationRunsAction::List { path, .. }
        | AutomationRunsAction::View { path, .. }
        | AutomationRunsAction::Artifact { path, .. } => path.clone(),
    };
    let project_path = resolve_cli_project_root(path, None, None).await?;
    let dashboard_root = daemon_project_dashboard_root(&project_path).await?;

    match action {
        AutomationRunsAction::List { limit, json, .. } => {
            let limit = limit.min(200);
            let records = load_run_records(&dashboard_root, limit).await?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "dashboard_root": dashboard_root,
                        "count": records.len(),
                        "limit": limit,
                        "records": records,
                    }))?
                );
            } else {
                print_automation_run_list(&records);
            }
        }
        AutomationRunsAction::View { run_id, json, .. } => {
            let record = find_run_record(&dashboard_root, &run_id)
                .await?
                .ok_or_else(|| tracedecay_domain::errors::TraceDecayError::Config {
                    message: format!("automation run not found: {run_id}"),
                })?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "dashboard_root": dashboard_root,
                        "record": record,
                    }))?
                );
            } else {
                print_automation_run_record(&record);
            }
        }
        AutomationRunsAction::Artifact {
            run_id, kind, json, ..
        } => {
            let record = find_run_record(&dashboard_root, &run_id)
                .await?
                .ok_or_else(|| tracedecay_domain::errors::TraceDecayError::Config {
                    message: format!("automation run not found: {run_id}"),
                })?;
            let artifact = record
                .artifacts
                .iter()
                .find(|artifact| artifact.kind == kind)
                .ok_or_else(|| tracedecay_domain::errors::TraceDecayError::Config {
                    message: format!("automation run artifact not found: {run_id}/{kind}"),
                })?;
            let payload =
                read_run_artifact_payload(&dashboard_root, &record.run_id, artifact).await?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "dashboard_root": dashboard_root,
                        "run_id": record.run_id,
                        "artifact": artifact,
                        "payload": payload,
                    }))?
                );
            } else {
                print_automation_run_artifact(&record.run_id, artifact, &payload)?;
            }
        }
    }
    Ok(())
}

fn print_automation_run_list(
    records: &[tracedecay_automation_runtime::automation::run_ledger::AutomationRunLedgerRecord],
) {
    if records.is_empty() {
        println!("No automation runs.");
        return;
    }
    println!("RUN ID\tSTATUS\tTASK\tTRIGGER\tACCEPTED\tREJECTED\tCOMPLETED\tERROR");
    for record in records {
        println!(
            "{}\t{}\t{}\t{:?}\t{}\t{}\t{}\t{}",
            record.run_id,
            record.status.as_str(),
            record.task_key.as_deref().unwrap_or_else(|| {
                tracedecay_automation_runtime::automation::backend::task_key(record.task)
            }),
            record.trigger,
            record.accepted_count,
            record.rejected_count,
            record.completed_at,
            record.error.as_deref().unwrap_or("")
        );
    }
}

fn print_automation_run_record(
    record: &tracedecay_automation_runtime::automation::run_ledger::AutomationRunLedgerRecord,
) {
    println!("run_id: {}", record.run_id);
    println!("status: {}", record.status.as_str());
    println!(
        "task: {}",
        record.task_key.as_deref().unwrap_or_else(|| {
            tracedecay_automation_runtime::automation::backend::task_key(record.task)
        })
    );
    println!("trigger: {:?}", record.trigger);
    println!("backend: {}", record.backend);
    if let Some(model) = record.model.as_deref() {
        println!("model: {model}");
    }
    println!("accepted_count: {}", record.accepted_count);
    println!("rejected_count: {}", record.rejected_count);
    println!("reviewed_count: {}", record.reviewed_count);
    if let Some(error) = record.error.as_deref() {
        println!("error: {error}");
    }
    if !record.artifacts.is_empty() {
        println!("artifacts:");
        for artifact in &record.artifacts {
            println!(
                "- {}\t{}\t{}",
                artifact.kind,
                artifact.path,
                artifact.summary.as_deref().unwrap_or("")
            );
        }
    }
}

fn print_automation_run_artifact(
    run_id: &str,
    artifact: &tracedecay_automation_runtime::automation::run_ledger::AutomationRunArtifact,
    payload: &serde_json::Value,
) -> tracedecay_domain::errors::Result<()> {
    println!("run_id: {run_id}");
    println!("artifact: {}", artifact.kind);
    println!("path: {}", artifact.path);
    if let Some(summary) = artifact.summary.as_deref() {
        println!("summary: {summary}");
    }
    println!("{}", serde_json::to_string_pretty(payload)?);
    Ok(())
}

#[cfg(test)]
mod typed_request_tests {
    use super::*;

    #[test]
    fn memory_curator_request_uses_the_v2_task_and_public_bounds() {
        let (path, request) = automation_run_rpc_request(AutomationRunAction::MemoryCuration {
            fact_review_limit: 31,
            min_confidence: 0.81,
            path: Some("/project".to_owned()),
        })
        .expect("valid curation request");

        assert_eq!(path.as_deref(), Some("/project"));
        assert_eq!(
            request,
            serde_json::json!({
                "task": "memory_curator",
                "options": {
                    "fact_review_limit": 31,
                    "min_confidence": 0.81,
                },
            })
        );
    }

    #[test]
    fn session_reflector_request_preserves_all_v2_evidence_controls() {
        let (_, request) = automation_run_rpc_request(AutomationRunAction::SessionReflection {
            provider: "claude".to_owned(),
            query: "decision".to_owned(),
            evidence_limit: 11,
            scope: "session".to_owned(),
            session_id: Some("session-3".to_owned()),
            include_summaries: false,
            include_recent_sessions: true,
            recent_sessions_limit: 4,
            sort: "hybrid".to_owned(),
            source: Some("assistant".to_owned()),
            role: Some("user".to_owned()),
            start_time: Some(10),
            end_time: Some(20),
            path: None,
        })
        .expect("valid reflector request");

        assert_eq!(request["task"], "session_reflector");
        assert_eq!(request["options"]["provider"], "claude");
        assert_eq!(request["options"]["query"], "decision");
        assert_eq!(request["options"]["evidence_limit"], 11);
        assert_eq!(request["options"]["scope"], "session");
        assert_eq!(request["options"]["session_id"], "session-3");
        assert_eq!(request["options"]["include_summaries"], false);
        assert_eq!(request["options"]["include_recent_sessions"], true);
        assert_eq!(request["options"]["recent_sessions_limit"], 4);
        assert_eq!(request["options"]["sort"], "hybrid");
        assert_eq!(request["options"]["source"], "assistant");
        assert_eq!(request["options"]["role"], "user");
        assert_eq!(request["options"]["start_time"], 10);
        assert_eq!(request["options"]["end_time"], 20);
    }

    #[test]
    fn skill_writer_request_preserves_recent_session_controls() {
        let (_, request) = automation_run_rpc_request(AutomationRunAction::SkillWriting {
            provider: "all".to_owned(),
            query: "repeated workflow".to_owned(),
            evidence_limit: 13,
            include_recent_sessions: false,
            recent_sessions_limit: 5,
            path: None,
        })
        .expect("valid skill request");

        assert_eq!(
            request,
            serde_json::json!({
                "task": "skill_writer",
                "options": {
                    "provider": "all",
                    "query": "repeated workflow",
                    "evidence_limit": 13,
                    "include_recent_sessions": false,
                    "recent_sessions_limit": 5,
                },
            })
        );
    }

    #[test]
    fn typed_request_validation_rejects_unbounded_or_ambiguous_controls() {
        let invalid_limit = automation_run_rpc_request(AutomationRunAction::MemoryCuration {
            fact_review_limit: 0,
            min_confidence: 0.7,
            path: None,
        });
        assert!(invalid_limit.is_err());

        let invalid_scope = automation_run_rpc_request(AutomationRunAction::SessionReflection {
            provider: "cursor".to_owned(),
            query: "decision".to_owned(),
            evidence_limit: 10,
            scope: "project".to_owned(),
            session_id: None,
            include_summaries: true,
            include_recent_sessions: true,
            recent_sessions_limit: 3,
            sort: "recency".to_owned(),
            source: None,
            role: None,
            start_time: None,
            end_time: None,
            path: None,
        });
        assert!(invalid_scope.is_err());

        let invalid_window = automation_run_rpc_request(AutomationRunAction::SkillWriting {
            provider: "all".to_owned(),
            query: "workflow".to_owned(),
            evidence_limit: 10,
            include_recent_sessions: true,
            recent_sessions_limit: 0,
            path: None,
        });
        assert!(invalid_window.is_err());
    }
}
