//! Guards that on-demand summarization launches a host CLI only through the
//! configured `lcm.summarizer_executables.v1` binding.
//!
//! A trap `cursor-agent`/`codex` sits first on `PATH` and records every launch.
//! With the isolated profile and no configured executable the request must
//! settle on the typed unconfigured reason and the trap must stay silent.

use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use crate::lcm_effects::DaemonLcmEffectService;
use tracedecay_domain::ProjectId;
use tracedecay_domain::configuration::{LcmSummarizerExecutableV1, LcmSummarizerExecutablesV1};
use tracedecay_global_db::tests::harness::{
    RegisteredGlobalDbHarness, RegisteredGlobalDbTestRuntime,
};
use tracedecay_lcm::{
    LcmCompressionRequest, LcmError, LcmSummaryRequest, LcmSummarySourceMessage,
    LcmSummarySourceRange,
};
use tracedecay_runtime_core::db::engine::params;
use tracedecay_runtime_core::test_executable::write_executable_script;
use tracedecay_sessions::runtime::{SessionMessageRecord, SessionRecord};

use super::{
    CODEX_APP_SERVER_UNCONFIGURED, SUMMARIZER_CONFIGURATION_UNAVAILABLE, SummaryResolutionError,
    generate_provider_summary, resolve_authoritative_summary, summarizer_executables,
};

/// `PATH` is process-wide, so the guards below take turns owning it.
static PATH_OWNER: Mutex<()> = Mutex::new(());

/// Puts trap executables first on `PATH` for the guard's lifetime. Each trap
/// appends its name to `launches` so a spawn is observable after the fact.
struct TrapPath {
    previous: Option<OsString>,
    launches: PathBuf,
    _directory: tempfile::TempDir,
    _owner: MutexGuard<'static, ()>,
}

impl TrapPath {
    fn install(names: &[&str]) -> Self {
        let owner = PATH_OWNER
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let directory = tempfile::tempdir().unwrap();
        let launches = directory.path().join("launches.log");
        for name in names {
            let trap = directory.path().join(name);
            write_executable_script(
                &trap,
                format!(
                    "#!/bin/sh\nprintf '%s\\n' \"$0\" >> '{}'\nexit 0\n",
                    launches.display()
                ),
            )
            .unwrap();
        }
        let previous = std::env::var_os("PATH");
        let mut path = directory.path().as_os_str().to_owned();
        if let Some(rest) = &previous {
            path.push(":");
            path.push(rest);
        }
        // SAFETY: tests serialize process-environment access through the
        // shared TraceDecay environment lock; the guard restores PATH on drop.
        unsafe { std::env::set_var("PATH", &path) };
        Self {
            previous,
            launches,
            _directory: directory,
            _owner: owner,
        }
    }

    fn launches(&self) -> String {
        std::fs::read_to_string(&self.launches).unwrap_or_default()
    }
}

impl Drop for TrapPath {
    fn drop(&mut self) {
        // SAFETY: see `install`.
        unsafe {
            match self.previous.take() {
                Some(previous) => std::env::set_var("PATH", previous),
                None => std::env::remove_var("PATH"),
            }
        }
    }
}

fn summary_request(provider: &str) -> LcmSummaryRequest {
    LcmSummaryRequest {
        provider: provider.to_owned(),
        session_id: "guard-session".to_owned(),
        focus_topic: None,
        prompt: "summarize".to_owned(),
        source_range: LcmSummarySourceRange {
            from_store_id: 1,
            to_store_id: 2,
        },
        source_messages: vec![LcmSummarySourceMessage {
            store_id: 1,
            role: "user".to_owned(),
            content: "hello".to_owned(),
        }],
        extraction_request: None,
    }
}

fn unavailable_reason(error: SummaryResolutionError) -> &'static str {
    match error {
        SummaryResolutionError::Unavailable(reason) => reason,
        SummaryResolutionError::Storage(error) => {
            panic!("expected a typed unavailable reason, got storage error {error}")
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn unconfigured_summarizers_report_typed_state_and_spawn_nothing() {
    let trap = TrapPath::install(&["cursor-agent", "codex"]);
    let harness = RegisteredGlobalDbHarness::open("lcm-summarizer-executable-guard").await;
    let database = harness.registered.clone();

    // A fresh profile resolves its own explicit unconfigured Codex executor.
    assert_eq!(
        summarizer_executables(&database).await.ok(),
        Some(LcmSummarizerExecutablesV1::unconfigured())
    );

    let cursor = generate_provider_summary(
        &database,
        "cursor",
        &summary_request("cursor"),
        Duration::from_secs(5),
    )
    .await
    .err()
    .map(unavailable_reason);
    assert_eq!(cursor, Some(CODEX_APP_SERVER_UNCONFIGURED));

    let codex = generate_provider_summary(
        &database,
        "codex",
        &summary_request("codex"),
        Duration::from_secs(5),
    )
    .await
    .err()
    .map(unavailable_reason);
    assert_eq!(codex, Some(CODEX_APP_SERVER_UNCONFIGURED));

    assert_eq!(
        trap.launches(),
        "",
        "an unconfigured summarizer must never reach a PATH-resolved host CLI"
    );
    // The traps are reachable through PATH, so silence above is the setting
    // refusing, not a missing binary.
    let resolved = std::process::Command::new("cursor-agent").status().unwrap();
    assert!(resolved.success());
    assert!(trap.launches().contains("cursor-agent"));
}

#[tokio::test(flavor = "multi_thread")]
async fn project_shard_without_a_published_pin_is_unavailable_not_ambient() {
    let trap = TrapPath::install(&["cursor-agent"]);
    let root = tempfile::tempdir().unwrap();
    let project_id = ProjectId::new("project.lcm-summarizer-guard".to_owned()).unwrap();
    let runtime = RegisteredGlobalDbTestRuntime::project(
        root.path().join("profile"),
        root.path().join("project"),
        project_id,
    )
    .await
    .unwrap();
    let database = runtime.project_database_arc().unwrap();

    let reason = generate_provider_summary(
        &database,
        "cursor",
        &summary_request("cursor"),
        Duration::from_secs(5),
    )
    .await
    .err()
    .map(unavailable_reason);
    assert_eq!(reason, Some(SUMMARIZER_CONFIGURATION_UNAVAILABLE));
    assert_eq!(trap.launches(), "");
}

#[tokio::test(flavor = "multi_thread")]
async fn configured_executable_is_launched_instead_of_the_path_binary() {
    let trap = TrapPath::install(&["cursor-agent"]);
    let root = tempfile::tempdir().unwrap();
    let project_root = root.path().join("project");
    let project_id = ProjectId::new("project.lcm-summarizer-configured".to_owned()).unwrap();
    let runtime = RegisteredGlobalDbTestRuntime::project(
        root.path().join("profile"),
        &project_root,
        project_id,
    )
    .await
    .unwrap();
    let database = runtime.project_database_arc().unwrap();

    let configured = root.path().join("bin").join("cursor-agent");
    std::fs::create_dir_all(configured.parent().unwrap()).unwrap();
    write_executable_script(
        &configured,
        "#!/bin/sh\nprintf '%s\\n' 'configured summary text'\n",
    )
    .unwrap();
    tracedecay_configuration::test_support::pin_lcm_summarizer_executables(
        &database,
        &project_root,
        LcmSummarizerExecutablesV1 {
            cursor_agent: LcmSummarizerExecutableV1::configured(configured.clone()).unwrap(),
            codex: LcmSummarizerExecutableV1::Unconfigured,
        },
    )
    .unwrap();

    let summary = generate_provider_summary(
        &database,
        "cursor",
        &summary_request("cursor"),
        Duration::from_secs(5),
    )
    .await
    .ok()
    .map(|summary| (summary.text, summary.route));
    assert_eq!(
        summary,
        Some((
            "configured summary text".to_owned(),
            "cursor_agent".to_owned()
        ))
    );
    assert_eq!(
        trap.launches(),
        "",
        "the PATH binary must stay untouched while a configured executable exists"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn configured_model_reaches_the_summarizer_in_a_private_workspace() {
    let _trap = TrapPath::install(&[]);
    let root = tempfile::tempdir().unwrap();
    let project_root = root.path().join("project");
    let project_id = ProjectId::new("project.lcm-summarizer-tuning".to_owned()).unwrap();
    let runtime = RegisteredGlobalDbTestRuntime::project(
        root.path().join("profile"),
        &project_root,
        project_id,
    )
    .await
    .unwrap();
    let database = runtime.project_database_arc().unwrap();

    let configured = root.path().join("bin").join("cursor-agent");
    let argv = root.path().join("argv.log");
    std::fs::create_dir_all(configured.parent().unwrap()).unwrap();
    write_executable_script(
        &configured,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\nprintf '%s\\n' 'tuned summary text'\n",
            argv.display()
        ),
    )
    .unwrap();
    tracedecay_configuration::test_support::pin_lcm_summarizer_executables(
        &database,
        &project_root,
        LcmSummarizerExecutablesV1 {
            cursor_agent: LcmSummarizerExecutableV1::configured_with(
                configured.clone(),
                Some("configured-summary-model".to_owned()),
                Some(5),
            )
            .unwrap(),
            codex: LcmSummarizerExecutableV1::Unconfigured,
        },
    )
    .unwrap();

    let summary = generate_provider_summary(
        &database,
        "cursor",
        &summary_request("cursor"),
        Duration::from_secs(30),
    )
    .await
    .ok()
    .map(|summary| summary.text);
    assert_eq!(summary.as_deref(), Some("tuned summary text"));

    let argv = std::fs::read_to_string(&argv).unwrap();
    let args: Vec<&str> = argv.lines().collect();
    let value_after = |flag: &str| {
        args.iter()
            .position(|arg| *arg == flag)
            .and_then(|index| args.get(index + 1).copied())
    };
    assert_eq!(value_after("--model"), Some("configured-summary-model"));
    let workspace = PathBuf::from(value_after("--workspace").unwrap());
    assert_ne!(workspace, std::env::temp_dir());
    assert!(
        !workspace.exists(),
        "the per-run summary workspace must be removed after the run"
    );

    // Reuse this protocol fixture to prove future-drop cleanup for both
    // configured executors; no live model availability is inferred.
    let child_marker = root.path().join("summary-child.log");
    let actor_barrier = root.path().join("actor-barrier");
    let release_actor = root.path().join("release-actor");
    let actor_input = root.path().join("actor-input.json");
    write_executable_script(&configured, format!(r#"#!/bin/sh
mode="$1"
workspace="$PWD"
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--workspace" ]; then workspace="$2"; break; fi
  shift
done
stall() {{
  sleep 30 &
  descendant="$!"
  printf '%s\n' "$$" "$descendant" "$workspace" > '{}'
  if [ -f '{}' ]; then
    while [ ! -f '{}' ]; do sleep 0.01; done
    kill "$descendant"
    wait "$descendant" 2>/dev/null
  else
    wait "$descendant"
  fi
}}
if [ "$mode" = "app-server" ]; then
  while IFS= read -r line; do
    case "$line" in
      *'"id":0'*) printf '%s\n' '{{"id":0,"result":{{}}}}' ;;
      *'"id":3'*) printf '%s\n' '{{"id":3,"result":{{"config":{{"mcp_servers":{{}},"profiles":{{}}}}}}}}' ;;
      *'"id":4'*) printf '%s\n' '{{"id":4,"result":{{"data":[{{"skills":[],"errors":[]}}]}}}}' ;;
      *'"id":1'*) printf '%s\n' '{{"id":1,"result":{{"thread":{{"id":"thread-1"}}}}}}' ;;
      *'"id":2'*)
        printf '%s\n' "$line" > '{}'
        stall
        printf '%s\n' '{{"method":"item/completed","params":{{"item":{{"content":[{{"type":"output_text","text":"summary of the original selected sources"}}]}}}}}}'
        printf '%s\n' '{{"method":"turn/completed"}}'
        ;;
    esac
  done
else
  stall
fi
"#, child_marker.display(), actor_barrier.display(), release_actor.display(), actor_input.display())).unwrap();
    for provider in ["cursor", "codex"] {
        let executable =
            LcmSummarizerExecutableV1::configured_with(configured.clone(), None, Some(5)).unwrap();
        let executables = if provider == "cursor" {
            LcmSummarizerExecutablesV1 {
                cursor_agent: executable,
                codex: LcmSummarizerExecutableV1::Unconfigured,
            }
        } else {
            LcmSummarizerExecutablesV1 {
                cursor_agent: LcmSummarizerExecutableV1::Unconfigured,
                codex: executable,
            }
        };
        tracedecay_configuration::test_support::pin_lcm_summarizer_executables(
            &database,
            &project_root,
            executables,
        )
        .unwrap();
        let task_database = database.clone();
        let task = tokio::spawn(async move {
            generate_provider_summary(
                &task_database,
                provider,
                &summary_request(provider),
                Duration::from_secs(5),
            )
            .await
        });
        let marker = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(marker) = std::fs::read_to_string(&child_marker)
                    && marker.lines().count() == 3
                {
                    break marker;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("configured protocol fixture reached its stalled turn");
        let lines = marker.lines().collect::<Vec<_>>();
        let leader: i32 = lines[0].parse().unwrap();
        let descendant: i32 = lines[1].parse().unwrap();
        let workspace = PathBuf::from(lines[2]);
        assert!(workspace.exists());
        assert!(workspace.join("summary-input.txt").exists() || provider == "codex");
        task.abort();
        match task.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("aborted summary future unexpectedly completed"),
        }
        unsafe extern "C" {
            fn kill(pid: i32, signal: i32) -> i32;
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while workspace.exists()
                || unsafe { kill(leader, 0) } == 0
                || unsafe { kill(descendant, 0) } == 0
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("aborted summary reaped its owned process group and removed its workspace");
        std::fs::remove_file(&child_marker).unwrap();
    }

    // This phase uses the actual effect, including provider selection and its
    // planning-to-publication handoff. Interior retirement changes neither
    // endpoint nor raw revision, so only exact selected-input binding rejects it.
    let session_id = "summary-actor-retirement";
    assert!(
        database
            .upsert_session(&SessionRecord {
                provider: "codex".to_owned(),
                session_id: session_id.to_owned(),
                project_key: "project.lcm-summarizer-tuning".to_owned(),
                project_path: project_root.to_string_lossy().into_owned(),
                title: None,
                started_at: Some(1),
                ended_at: None,
                transcript_path: None,
                metadata_json: None,
                parent_session_id: None,
                is_subagent: false,
                agent_id: None,
                parent_tool_use_id: None,
            })
            .await
    );
    for (index, content) in [
        "Preserve the original release plan",
        "The original plan has four important steps",
        "The deployment requires review",
        "Protected fresh continuation tail",
    ]
    .into_iter()
    .enumerate()
    {
        let ordinal = i64::try_from(index + 1).unwrap();
        database
            .lcm_ingest_raw_message(
                database.db_path().parent().unwrap(),
                &SessionMessageRecord {
                    provider: "codex".to_owned(),
                    message_id: format!("actor-source-{ordinal}"),
                    session_id: session_id.to_owned(),
                    role: if index % 2 == 0 { "user" } else { "assistant" }.to_owned(),
                    timestamp: Some(ordinal),
                    ordinal,
                    text: content.to_owned(),
                    kind: Some("message".to_owned()),
                    model: None,
                    tool_names: None,
                    source_path: None,
                    source_offset: None,
                    metadata_json: None,
                },
            )
            .await
            .unwrap();
    }
    let request: LcmCompressionRequest = serde_json::from_value(serde_json::json!({
        "provider": "codex", "session_id": session_id,
        "messages": [],
        "current_tokens": 1000, "max_assembly_tokens": 1, "max_source_messages": 8,
        "fresh_tail_count": 1, "summarizer": {"mode": "hermes_auxiliary"}
    }))
    .unwrap();
    std::fs::write(&actor_barrier, b"hold the actor response").unwrap();
    let effect_database = database.clone();
    let effect = tokio::spawn(async move {
        DaemonLcmEffectService::new(effect_database, None, None)
            .compress(request)
            .await
    });
    let marker = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(marker) = std::fs::read_to_string(&child_marker)
                && marker.lines().count() == 3
            {
                break marker;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("actual effect reached the app-server turn barrier");
    let workspace = PathBuf::from(marker.lines().nth(2).unwrap());
    assert!(workspace.exists());
    let writer = database.begin_write_transaction().await.unwrap();
    let before =
        tracedecay_lcm::summary_convergence::session_raw_revision(&writer, "codex", session_id)
            .await
            .unwrap();
    let mut rows = writer.query("SELECT store_id FROM lcm_raw_messages WHERE provider = ?1 AND session_id = ?2 ORDER BY store_id", params!["codex", session_id]).await.unwrap();
    let mut source_ids = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        source_ids.push(row.get::<i64>(0).unwrap());
    }
    drop(rows);
    assert_eq!(source_ids.len(), 4);
    let turn: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&actor_input).unwrap()).unwrap();
    let prompt = turn["params"]["input"][0]["text"].as_str().unwrap();
    for source in &source_ids[..3] {
        assert!(prompt.contains(&format!("store_id={source}]")));
    }
    assert!(!prompt.contains(&format!("store_id={}]", source_ids[3])));
    assert_eq!(
        writer
            .execute(
                "DELETE FROM lcm_raw_messages WHERE store_id = ?1",
                params![source_ids[1]]
            )
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        tracedecay_lcm::summary_convergence::session_raw_revision(&writer, "codex", session_id)
            .await
            .unwrap(),
        before
    );
    writer.commit().await.unwrap();
    std::fs::write(&release_actor, b"release the original summary").unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), effect)
        .await
        .expect("released effect settles")
        .unwrap();
    assert_eq!(result.unwrap_err(), LcmError::StaleSummarySourceContent);
    assert_eq!(
        database
            .lcm_status("codex", Some(session_id))
            .await
            .unwrap()
            .summary_node_count,
        0
    );
    assert!(
        !workspace.exists(),
        "completed actor workspace is cleaned before failed publication settles"
    );
    unsafe extern "C" {
        fn kill(pid: i32, signal: i32) -> i32;
    }
    for pid in marker.lines().take(2) {
        assert_ne!(unsafe { kill(pid.parse().unwrap(), 0) }, 0);
    }
}

#[tokio::test]
async fn executor_selection_cannot_change_the_source_session_authority() {
    let harness = RegisteredGlobalDbHarness::open("lcm-summary-source-identity").await;
    let request = summary_request("claude");
    for (provider, session_id) in [
        ("codex", request.session_id.as_str()),
        ("claude", "another-session"),
    ] {
        assert!(matches!(
            resolve_authoritative_summary(
                &harness.registered,
                provider,
                session_id,
                &request,
                Duration::from_secs(5),
                None
            )
            .await,
            Err(SummaryResolutionError::Storage(
                tracedecay_lcm::LcmError::SummarySourceNotOwnedBySession
            ))
        ));
    }
}
