use crate::mcp::tools::SessionAuthorities;
use serde_json::{Value, json};
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tracedecay_automation_runtime::automation::config_error;
use tracedecay_domain::errors::Result;

use super::hermes::user_review;
use super::ingest::{CodexStopSourceBound, ingest_transcript_with_stop_bound};
use super::required_str;

/// Admit a Codex terminal receipt by retaining its follow-up work in
/// the daemon. The hook only receives this acknowledgement; transcript ingest
/// and review are cancellable daemon-owned work for that exact session.
#[hotpath::measure(label = "mcp.hook_runtime.terminal")]
pub(super) fn retain_codex_stop(
    args: &Value,
    profile_root: &Path,
    session_runtime_registry: &Arc<tracedecay_store_runtime::DaemonSessionRuntimeRegistryV1>,
    session_authorities: SessionAuthorities<'_>,
    project_route: Option<crate::mcp::project_route::ResolvedProjectRoute>,
) -> Result<Value> {
    let session_id = required_str(args, "session_id")?.to_owned();
    let user_sessions = session_authorities
        .user
        .cloned()
        .ok_or_else(|| config_error("daemon user session database is unavailable"))?;
    let profile_identity = session_authorities
        .profile_identity
        .clone()
        .ok_or_else(|| config_error("daemon profile identity is unavailable"))?;
    let project = project_route
        .map(|route| {
            let server = route.retained_server()?;
            if server.project_route_live() == Some(false) {
                return Err(config_error("Codex Stop project route is no longer live"));
            }
            let sessions = server.project_session_db().ok_or_else(|| {
                config_error("Codex Stop project session authority is unavailable")
            })?;
            Ok((server, sessions))
        })
        .transpose()?;
    let transcript_home = tracedecay_sessions::runtime::home_dir()
        .ok_or_else(|| config_error("Codex transcript source home is unavailable"))?;
    let background_cpu = session_authorities.background_cpu.clone();
    let profile_root = profile_root.to_path_buf();
    let weak_registry = Arc::downgrade(session_runtime_registry);
    let task_session_id = session_id.clone();
    let accepted =
        session_runtime_registry.retain_hook_task("codex", &session_id, move |cancellation| {
            let followup = async move {
                if cancellation.is_cancelled() {
                    return;
                }
                let Some(session_runtime_registry) = weak_registry.upgrade() else {
                    return;
                };
                let Ok(global_db) = session_runtime_registry.profile_database().await else {
                    return;
                };
                let graph = match project.as_ref() {
                    Some((server, _)) => {
                        if server.project_route_live() == Some(false) {
                            return;
                        }
                        let Some(graph) =
                            await_terminal_operation(&cancellation, server.cg_snapshot()).await
                        else {
                            return;
                        };
                        Some(graph)
                    }
                    None => None,
                };
                let stop_bound = match (graph.as_deref(), project.as_ref()) {
                    (Some(cg), Some((_, sessions))) if cfg!(feature = "memory-provider-host") => {
                        Some(
                            retained_codex_source_bound(
                                cg,
                                sessions,
                                profile_identity.as_ref(),
                                &task_session_id,
                                &cancellation,
                            )
                            .await,
                        )
                    }
                    _ => None,
                };
                let ingest_args = json!({
                    "action": "ingest_transcript",
                    "provider": "codex",
                    "user_scope": project.is_none(),
                    "max_new_bytes": tracedecay_sessions::runtime::codex::CODEX_HOOK_MAX_NEW_BYTES,
                    "session_id": task_session_id,
                });
                let authorities = SessionAuthorities::new(
                    project.as_ref().map(|(_, sessions)| sessions),
                    Some(&user_sessions),
                )
                .with_profile_identity(Some(std::sync::Arc::clone(&profile_identity)))
                .with_background_cpu(background_cpu.clone());
                let ingested = ingest_transcript_with_stop_bound(
                    graph.as_deref(),
                    &ingest_args,
                    Some(&profile_root),
                    Some(global_db.as_ref()),
                    None,
                    authorities,
                    &cancellation,
                    stop_bound.as_ref(),
                )
                .await
                .map_err(|error| {
                    tracing::warn!(%error, "retained Codex Stop transcript ingestion failed");
                    error
                })
                .ok()
                .and_then(|result| result.get("messages_upserted").and_then(Value::as_u64))
                .is_some_and(|count| count > 0);
                if project.is_none()
                    && ingested
                    && !cancellation.is_cancelled()
                    && let Some(session_id) = ingest_args.get("session_id").cloned()
                {
                    let _ = await_terminal_operation(
                        &cancellation,
                        user_review(
                            &json!({
                                "action": "user_review",
                                "provider": "codex",
                                "session_id": session_id,
                            }),
                            &profile_root,
                            &session_runtime_registry,
                        ),
                    )
                    .await;
                }
            };
            hotpath::future!(
                tracedecay_sessions::runtime::with_transcript_source_home(
                    transcript_home,
                    followup
                ),
                label = "mcp.hook_runtime.terminal_followup"
            )
        });
    if !accepted {
        return Err(config_error(
            "daemon retained terminal-hook task is unavailable",
        ));
    }
    Ok(json!({
        "action": "codex_stop",
        "status": "accepted",
        "session_id": session_id,
    }))
}

/// Read only the existing validated live-origin ledger after the retained
/// project graph is available. This work never runs in the synchronous hook
/// acknowledgment budget and never derives an authority ceiling from live EOF.
async fn retained_codex_source_bound(
    cg: &crate::tracedecay::TraceDecay,
    sessions: &tracedecay_global_db::RegisteredGlobalDb,
    profile: &dyn tracedecay_contracts::ProfileIdentityReadPort,
    session_id: &str,
    cancellation: &tracedecay_application::observation::ObservationCancellation,
) -> CodexStopSourceBound {
    use tracedecay_domain::ObservationScopeV1;
    use tracedecay_hooks::{HookHostV1, admission_ledger::read_hook_live_origin_boundaries};
    use tracedecay_store::StoreShardScopeV1;
    let binding = &sessions.binding().shard_id;
    let StoreShardScopeV1::ProjectSessions { project_id } = &binding.scope else {
        return CodexStopSourceBound::Deferred;
    };
    if binding.brain_id != *profile.brain_id()
        || binding.profile_id != *profile.profile_id()
        || cg.store_layout().identity.project_id.as_deref() != Some(project_id.as_str())
        || cancellation.is_cancelled()
    {
        return CodexStopSourceBound::Deferred;
    }
    let Ok(Some(session)) = sessions.get_session_result("codex", session_id).await else {
        return CodexStopSourceBound::Deferred;
    };
    if session.provider != "codex" || session.session_id != session_id {
        return CodexStopSourceBound::Deferred;
    }
    let Some(transcript_path) = session.transcript_path else {
        return CodexStopSourceBound::Deferred;
    };
    let Ok(source) = tracedecay_sessions::runtime::codex::codex_observation_source_v2(session_id)
    else {
        return CodexStopSourceBound::Deferred;
    };
    let ledger_root = super::admission::hook_v2_admission_ledger_root(
        &cg.hook_store_layout().data_root,
        HookHostV1::Codex,
    );
    let project_root = cg.project_root().to_path_buf();
    let project_id = project_id.clone();
    let brain_id = profile.brain_id().clone();
    let profile_id = profile.profile_id().clone();
    let protected_session = tracedecay_agent_hosts::hooks::protected_native_session_id(session_id);
    let cancellation = cancellation.clone();
    tokio::task::spawn_blocking(move || {
        let resolve = || -> Option<CodexStopSourceBound> {
            if cancellation.is_cancelled() {
                return None;
            }
            let canonical_project = std::fs::canonicalize(project_root).ok()?;
            if std::fs::canonicalize(session.project_path).ok()? != canonical_project {
                return None;
            }
            let canonical_path = std::fs::canonicalize(transcript_path).ok()?;
            let boundaries = read_hook_live_origin_boundaries(
                &ledger_root,
                HookHostV1::Codex,
                super::envelope::hook_now(),
            )
            .ok()?;
            let mut matching = boundaries.into_iter().filter(|boundary| {
                let observation = &boundary.observation;
                boundary.admission.protected_session_id == protected_session
                    && observation.scope.brain_id == brain_id
                    && observation.scope.profile_id == profile_id
                    && observation.scope.repository.project_id() == Some(&project_id)
                    && observation.source == source
                    && observation.canonical_source_path == canonical_path
            });
            let boundary = matching.next()?;
            if matching.next().is_some() || cancellation.is_cancelled() {
                return None;
            }
            let checkpoint = boundary.observation.checkpoint;
            Some(CodexStopSourceBound::Sealed(
                tracedecay_sessions::runtime::codex::SealedJsonlSourceBound::new(
                    canonical_path,
                    source,
                    ObservationScopeV1::Project { project_id },
                    checkpoint.generation,
                    checkpoint.file_identity,
                    checkpoint.complete_frontier,
                    checkpoint.complete_prefix_fingerprint,
                ),
            ))
        };
        resolve().unwrap_or(CodexStopSourceBound::Deferred)
    })
    .await
    .unwrap_or(CodexStopSourceBound::Deferred)
}

pub(super) async fn await_terminal_operation<T>(
    cancellation: &tracedecay_application::observation::ObservationCancellation,
    operation: impl Future<Output = T>,
) -> Option<T> {
    tokio::pin!(operation);
    loop {
        if cancellation.is_cancelled() {
            return None;
        }
        tokio::select! {
            output = &mut operation => return Some(output),
            () = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }
}
