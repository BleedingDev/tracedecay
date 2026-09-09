//! Stop must commit its own canonical rows while historical ingestion is denied.
use super::ProductionProjectCompositionHarnessV1;
use super::journey_test_support::{git, tool_payload};
use crate::mcp::project_route::HookProjectRouteCache;
use serde_json::{Value, json};
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};
use tracedecay_daemon_protocol::DaemonClientIdentity;
use tracedecay_global_db::RegisteredGlobalDb;
use tracedecay_hooks::core_events::{HookAgent, HookRouteMetadata};
use tracedecay_mcp::hook_events::{HookEvent, HookEventKind};
use tracedecay_sessions::runtime::with_transcript_source_home;

// Same convergence bound as the shipped CLI host-memory journey.
const SETTLEMENT_BUDGET: Duration = Duration::from_secs(60);

#[tokio::test]
async fn registered_codex_stop_captures_only_its_session_without_historical_ingest() {
    assert_codex_stop_capture(true).await;
}

#[tokio::test]
async fn untethered_codex_stop_retains_profile_ingest_without_historical_ingest() {
    assert_codex_stop_capture(false).await;
}

async fn assert_codex_stop_capture(project_scoped: bool) {
    let isolation = tempfile::TempDir::new().expect("isolated Codex profile");
    let home = fs::canonicalize(isolation.path()).unwrap();
    let project = home.join("project");
    fs::create_dir_all(&project).unwrap();
    fs::write(project.join("lib.rs"), "pub fn codex_stop_marker() {}\n").unwrap();
    git(&project, &["init", "--quiet", "-b", "main"]);
    git(&project, &["add", "."]);
    git(
        &project,
        &[
            "-c",
            "user.name=TraceDecay Test",
            "-c",
            "user.email=tests@tracedecay.invalid",
            "commit",
            "--quiet",
            "-m",
            "fixture",
        ],
    );
    let sibling = home.join("sibling-worktree");
    git(
        &project,
        &[
            "worktree",
            "add",
            "--quiet",
            "-b",
            "sibling",
            sibling.to_str().unwrap(),
        ],
    );
    let untethered = home.join("untethered");
    fs::create_dir_all(&untethered).unwrap();
    let harness =
        ProductionProjectCompositionHarnessV1::open_for_session_retrieval(&home, [project.clone()])
            .await
            .unwrap();
    let resources = harness.resources.as_ref().unwrap();
    let administration = &resources.store_administration;
    let server = harness.server(&project).unwrap();
    let project_db = server.project_session_db().unwrap();
    let profile_db = administration
        .registered_profile_session_database()
        .await
        .unwrap();
    let session = if project_scoped {
        "codex-stop-project"
    } else {
        "codex-stop-profile"
    };
    if project_scoped {
        // This is the exact route publication called by native SessionStart.
        let event = HookEvent {
            agent: HookAgent::Codex,
            kind: HookEventKind::SessionStart,
            rel_paths: Vec::new(),
            had_command: false,
            cwd: Some(project.clone()),
            route: Some(HookRouteMetadata {
                session_id: Some(session.to_owned()),
                thread_id: None,
                cwd: Some(project.clone()),
                worktree: None,
                branch: None,
            }),
            receipt: None,
        };
        server
            .update_hook_workspace_route(&event, &mut HookProjectRouteCache::default())
            .await
            .unwrap();
    }
    let schedulers = administration.session_temporal_refresh_schedulers();
    let owners = administration
        .project_servers()
        .lock()
        .await
        .servers
        .keys()
        .map(|key| key.owner.clone())
        .collect::<std::collections::HashSet<_>>();
    let mut states = Vec::new();
    for owner in owners {
        states.push(
            schedulers
                .project_state(&owner)
                .await
                .expect("project history worker"),
        );
    }
    let deadline = Instant::now() + SETTLEMENT_BUDGET;
    loop {
        assert!(
            schedulers
                .wait_profile_idle(profile_db.db_path(), SETTLEMENT_BUDGET)
                .await
        );
        if states.iter().all(|state| state.is_idle()) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "initial history settles before writing the rollout"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Current-thread runtime: no worker runs between observing idle and taking
    // every permit. Keep these held until all positive and negative assertions.
    let admission = schedulers.historical_ingest_admission();
    let permits = u32::try_from(admission.available_permits()).unwrap();
    assert!(permits > 0);
    let held = admission.clone().try_acquire_many_owned(permits).unwrap();
    assert_eq!(project_db.session_message_count().await.unwrap(), 0);
    assert_eq!(profile_db.session_message_count().await.unwrap(), 0);
    let source_home = ProductionProjectCompositionHarnessV1::transcript_source_home(&home).unwrap();
    let cwd = if project_scoped {
        &project
    } else {
        &untethered
    };
    write_rollout(&source_home, session, cwd, 1);
    // Filenames match the target substring; metadata must exclude both peers.
    let same_project_session = format!("{session}-other-session");
    let sibling_session = format!("{session}-other-worktree");
    write_rollout(&source_home, &same_project_session, &project, 1);
    write_rollout(&source_home, &sibling_session, &sibling, 1);
    let client = DaemonClientIdentity::new(
        harness.profile_root().to_path_buf(),
        harness.profile_root().join("global.db"),
    );
    let mut arguments = json!({"action": "codex_stop", "session_id": session, "format": "json"});
    if project_scoped {
        arguments["project_root"] = json!(project);
        let mut stale = arguments.clone();
        stale["project_root"] = json!(sibling);
        let rejected = with_transcript_source_home(
            source_home.clone(),
            crate::daemon::projectless::projectless_tools_call_response(
                json!(1),
                Some(&json!({"name": "tracedecay_hook_runtime", "arguments": stale})),
                &client,
                administration,
            ),
        )
        .await;
        assert!(
            rejected.error.is_some()
                || rejected
                    .result
                    .as_ref()
                    .is_some_and(|result| result["isError"] == true),
            "sibling root must be refused: {rejected:?}"
        );
        assert_eq!(project_db.session_message_count().await.unwrap(), 0);
    }
    let response = with_transcript_source_home(
        source_home.clone(),
        crate::daemon::projectless::projectless_tools_call_response(
            json!(2),
            Some(&json!({"name": "tracedecay_hook_runtime", "arguments": arguments})),
            &client,
            administration,
        ),
    )
    .await;
    assert_eq!(tool_payload(&response)["status"], "accepted");
    let expected_db = if project_scoped {
        &project_db
    } else {
        &profile_db
    };
    let other_db = if project_scoped {
        &profile_db
    } else {
        &project_db
    };
    await_messages(expected_db.as_ref(), session, 2).await;
    assert_eq!(admission.available_permits(), 0);
    assert_eq!(expected_db.session_message_count().await.unwrap(), 2);
    assert!(
        expected_db
            .session_messages_after("codex", &same_project_session, 0, 16)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        expected_db
            .session_messages_after("codex", &sibling_session, 0, 16)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(other_db.session_message_count().await.unwrap(), 0);
    let first = expected_db
        .session_messages_after("codex", session, 0, 16)
        .await
        .unwrap();
    write_rollout(&source_home, session, cwd, 2);
    let response = with_transcript_source_home(
        source_home,
        crate::daemon::projectless::projectless_tools_call_response(
            json!(3),
            Some(&json!({"name": "tracedecay_hook_runtime", "arguments": arguments})),
            &client,
            administration,
        ),
    )
    .await;
    assert_eq!(tool_payload(&response)["status"], "accepted");
    await_messages(expected_db.as_ref(), session, 4).await;
    let after = expected_db
        .session_messages_after("codex", session, 0, 16)
        .await
        .unwrap();
    assert_eq!(
        &after[..2],
        first.as_slice(),
        "first turn projections survive replay unchanged"
    );
    assert_eq!(expected_db.session_message_count().await.unwrap(), 4);
    assert_eq!(other_db.session_message_count().await.unwrap(), 0);
    assert_eq!(admission.available_permits(), 0);
    drop(held);
    drop(project_db);
    drop(profile_db);
    drop(server);
    harness.shutdown().await;
}

fn write_rollout(home: &Path, session: &str, cwd: &Path, turns: usize) {
    let directory = home.join(".codex/sessions/2026/02/01");
    fs::create_dir_all(&directory).unwrap();
    let mut rows = vec![
        json!({"timestamp": "2026-02-01T00:00:00.000Z", "type": "session_meta", "payload": {"id": session, "cwd": cwd}}),
    ];
    for turn in 0..turns {
        rows.push(json!({"timestamp": format!("2026-02-01T00:00:{:02}.000Z", turn * 2 + 1), "type": "event_msg", "payload": {"type": "user_message", "message": format!("quicksilver question {turn}")}}));
        rows.push(json!({"timestamp": format!("2026-02-01T00:00:{:02}.000Z", turn * 2 + 2), "type": "event_msg", "payload": {"type": "agent_message", "message": format!("quicksilver answer {turn}")}}));
    }
    fs::write(
        directory.join(format!("rollout-{session}.jsonl")),
        rows.iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )
    .unwrap();
}

async fn await_messages(database: &RegisteredGlobalDb, session: &str, expected: usize) {
    let deadline = Instant::now() + SETTLEMENT_BUDGET;
    loop {
        let rows = database
            .session_messages_after("codex", session, 0, 16)
            .await
            .unwrap();
        if rows.len() >= expected {
            assert_eq!(rows.len(), expected);
            return;
        }
        assert!(
            Instant::now() < deadline,
            "Stop never committed {expected} canonical messages while history was denied; observed {}",
            rows.len()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
