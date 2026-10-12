//! Codex context-compaction ingestion and daemon-owned summary publication.
//! PostCompact retains exact native evidence. The LCM owner publishes summaries
//! through the configured app-server authority.

#[cfg(unix)]
use std::io::Write;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
#[cfg(unix)]
use std::process::Stdio;
#[cfg(unix)]
use std::time::Duration;

use tempfile::TempDir;
#[cfg(unix)]
use tracedecay_domain::ObservationScopeV1;
use tracedecay_domain::ProjectId;
#[cfg(unix)]
use tracedecay_lcm::{
    LcmCompressionRequest, LcmContentSlice, LcmDescribeRequest, LcmDescribeTarget,
    LcmExpandRequest, LcmExpandTarget, LcmSummarizerMode,
};
#[cfg(unix)]
use tracedecay_project::project::TraceDecayOpenOptions;
use tracedecay_project::test_support::host_admission::HostAdmissionTestRuntimeV1;
use tracedecay_runtime_core::config::ProfileRoot;
#[cfg(unix)]
use tracedecay_runtime_core::test_executable::write_executable_script;
#[cfg(unix)]
use tracedecay_sessions::observation::ObservationCancellation;
#[cfg(unix)]
use tracedecay_sessions::runtime::hosts::codex::codex_observation_source_v2;
use tracedecay_sessions::runtime::{SessionProvider, with_transcript_source_profile};

#[cfg(unix)]
use crate::common::{TestChildProcess, spawn_tracedecay_daemon, tracedecay_command_with_home};
#[cfg(unix)]
use crate::restart_atomicity::mark_test_project;
use crate::support::setup;

async fn registered_runtime(
    home: &std::path::Path,
    project: &std::path::Path,
) -> HostAdmissionTestRuntimeV1 {
    let project_id = ProjectId::new("project.codex-compaction").unwrap();
    assert!(
        tracedecay_runtime_core::storage::write_repository_identity_marker(
            project,
            project_id.as_str()
        )
        .unwrap()
    );
    HostAdmissionTestRuntimeV1::project(home.join(".tracedecay"), project, project_id)
        .await
        .unwrap()
}

#[cfg(unix)]
fn write_codex_rollout_with_compaction(
    home: &std::path::Path,
    project: &std::path::Path,
    session: &str,
) -> std::path::PathBuf {
    let dir = home.join(".codex/sessions/2026/01/01");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("rollout-2026-01-01T00-00-20-{session}.jsonl"));
    let contents = format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n",
        serde_json::json!({
            "timestamp": "2026-01-01T00:00:20.000Z",
            "type": "session_meta",
            "payload": {"id": session, "cwd": project.to_string_lossy(), "model": "gpt-5.5"}
        }),
        serde_json::json!({
            "timestamp": "2026-01-01T00:00:21.000Z",
            "type": "event_msg",
            "payload": {"type": "user_message", "message": "Map the release automation state"}
        }),
        serde_json::json!({
            "timestamp": "2026-01-01T00:00:22.000Z",
            "type": "event_msg",
            "payload": {"type": "agent_message", "message": "Release automation is mapped."}
        }),
        serde_json::json!({
            "timestamp": "2026-01-01T00:00:23.000Z",
            "type": "compacted",
            "payload": {
                "message": "",
                "replacement_history": [
                    {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "Map the release automation state"}]},
                    {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "Release automation is mapped."}]},
                    {"type": "compaction", "encrypted_content": "encrypted-codex-summary"}
                ]
            }
        }),
        serde_json::json!({
            "timestamp": "2026-01-01T00:00:23.010Z",
            "type": "event_msg",
            "payload": {"type": "context_compacted"}
        }),
        serde_json::json!({
            "timestamp": "2026-01-01T00:00:24.000Z",
            "type": "event_msg",
            "payload": {"type": "user_message", "message": "Continue after compaction"}
        }),
    );
    std::fs::write(&path, contents).unwrap();
    path
}

/// Binds `codex_bin` as the project's `codex` summarizer through the CLI
/// configuration tools, the same route an operator uses.
#[cfg(unix)]
fn configure_codex_summarizer(
    home: &std::path::Path,
    project: &std::path::Path,
    project_id: &ProjectId,
    codex_bin: &std::path::Path,
) {
    let key = tracedecay_domain::configuration::LCM_SUMMARIZER_EXECUTABLES_SETTING_KEY;
    let run_tool = |name: &str, args: serde_json::Value| -> serde_json::Value {
        let output = tracedecay_command_with_home(home)
            .current_dir(project)
            .args(["tool", name, "--json", "--args"])
            .arg(args.to_string())
            .stdin(Stdio::null())
            .output()
            .unwrap_or_else(|error| panic!("run `tracedecay tool {name}`: {error}"));
        assert!(
            output.status.success(),
            "`tracedecay tool {name}` failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let printed: serde_json::Value = serde_json::from_slice(&output.stdout)
            .unwrap_or_else(|error| panic!("`tracedecay tool {name}` JSON: {error}"));
        printed["structuredContent"].clone()
    };
    let current = run_tool(
        "tracedecay_configuration_get",
        serde_json::json!({ "key": key }),
    );
    let expected_revision = current
        .pointer("/outcome/value/payload/revision_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| panic!("configuration revision: {current}"))
        .to_owned();
    let receipt = run_tool(
        "tracedecay_configuration_set",
        serde_json::json!({
            "layer": {"kind": "project", "project_id": project_id.as_str()},
            "key": key,
            "value": {
                "kind": "lcm_summarizer_executables",
                "value": {
                    "codex": {
                        "state": "configured",
                        "canonical_path": codex_bin,
                        "timeout_secs": 5,
                    },
                },
            },
            "expected_revision": expected_revision,
            "idempotency_key": "configuration.idempotency.codex-compaction-summarizer",
        }),
    );
    assert_eq!(
        receipt["outcome"]["outcome"], "effect",
        "codex summarizer binding must commit: {receipt}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn codex_post_compact_defers_summary_to_the_lcm_owner() {
    let tmp = TempDir::new().unwrap();
    let (home, project) = setup(&tmp);
    let profile = home.join(".tracedecay");
    let codex_bin = tmp.path().join("codex");
    write_executable_script(
        &codex_bin,
        r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"id":0'*) printf '%s\n' '{"id":0,"result":{}}' ;;
    *'"id":1'*) printf '%s\n' '{"id":1,"result":{"thread":{"id":"thread-1","model":"codex-hook-test"}}}' ;;
    *'"id":2'*)
      printf '%s\n' '{"method":"item/completed","params":{"model":"codex-hook-test","item":{"content":[{"type":"output_text","text":"Codex authoritative hook summary"}]}}}'
      printf '%s\n' '{"method":"turn/completed"}'
      ;;
  esac
done
"#,
    )
    .unwrap();
    let project_id = mark_test_project(&project);
    // The hook resolves the project root through the initialized-store gate,
    // exactly like production installs: `init` creates the project store
    // first, then daemon enrollment mounts the already-initialized layout
    // (the canonical enrollment composition itself creates the project graph
    // database, so init must come first, as it does in a real install).
    crate::common::initialize_tracedecay_cli_project(&home, &project);
    // The daemon launches a summarizer only through the project's
    // `lcm.summarizer_executables.v1` binding, so the fake `codex` is bound
    // through the production configuration surface while the managed daemon
    // from init is still serving the project.
    configure_codex_summarizer(&home, &project, &project_id, &codex_bin);
    crate::common::stop_managed_daemon(&home);
    let enrollment = HostAdmissionTestRuntimeV1::project(&profile, &project, project_id.clone())
        .await
        .unwrap();
    drop(enrollment);
    let rollout = write_codex_rollout_with_compaction(&home, &project, "codex-compact");

    let daemon = spawn_tracedecay_daemon(&home);
    let event = serde_json::json!({
        "hook_event_name": "PostCompact",
        "session_id": "codex-compact",
        "cwd": project,
        "context_tokens": 124_000,
        "context_window_size": 128_000
    });
    let mut hook = tracedecay_command_with_home(&home);
    let child = hook
        .arg("hook-codex-post-compact")
        .current_dir(&project)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .spawn()
        .unwrap();
    let mut child = TestChildProcess::new(child);
    writeln!(child.stdin_mut().unwrap(), "{event}").unwrap();
    let output = child
        .wait_with_output(Duration::from_secs(5))
        .expect("PostCompact must return within the host deadline");
    assert!(
        output.status.success(),
        "Codex PostCompact hook failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !String::from_utf8_lossy(&output.stderr).contains("PostCompact daemon call failed"),
        "the PostCompact hook must not fail open around canonical intake: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    drop(daemon);

    let runtime = HostAdmissionTestRuntimeV1::project(&profile, &project, project_id.clone())
        .await
        .unwrap();

    let source = codex_observation_source_v2("codex-compact").unwrap();
    let cursor = runtime
        .project_observation_source_cursor_for_test(&source)
        .await
        .unwrap()
        .expect("PostCompact must durably admit its exact transcript window");
    assert_eq!(
        cursor.position(),
        std::fs::metadata(&rollout).unwrap().len()
    );

    // PostCompact returns before projection settles. Drain its durable work
    // after stopping the daemon, without rereading or admitting the rollout.
    let drained = runtime
        .facade()
        .drain_projection_queue(
            "codex",
            &ObservationScopeV1::Project { project_id },
            &ObservationCancellation::default(),
            32,
        )
        .await
        .unwrap();
    assert!(!drained.deferred);

    let status = runtime
        .lcm_status_for_test("codex", Some("codex-compact"))
        .await
        .unwrap();
    assert_eq!(status.raw_message_count, 4);

    // The real background owner may settle before the daemon stops. Otherwise
    // drive its canonical LCM effect explicitly, without requiring the host
    // callback to wait for the configured app-server.
    if status.summary_node_count == 0 {
        let _configuration_project = runtime
            .open_project_graph_read_only_for_test(
                &project,
                TraceDecayOpenOptions::for_profile(&ProfileRoot::new(&profile).with_home(&home)),
            )
            .await
            .unwrap();
        let compressed = runtime
            .lcm_compress_for_test(LcmCompressionRequest {
                provider: "codex".to_string(),
                session_id: "codex-compact".to_string(),
                messages: Vec::new(),
                current_tokens: Some(124_000),
                focus_topic: None,
                ignore_session_patterns: Vec::new(),
                stateless_session_patterns: Vec::new(),
                ignore_message_patterns: Vec::new(),
                expected_current_frontier_store_id: None,
                threshold_tokens: None,
                max_assembly_tokens: None,
                leaf_chunk_tokens: None,
                max_source_messages: None,
                summary_fan_in: None,
                incremental_max_depth: None,
                fresh_tail_count: None,
                dynamic_leaf_chunk_enabled: None,
                dynamic_leaf_chunk_max: None,
                context_length: Some(128_000),
                reserve_tokens_floor: None,
                summarizer: LcmSummarizerMode::HermesAuxiliary,
            })
            .await
            .unwrap();
        assert_eq!(compressed.status, "ok");
        assert_eq!(compressed.summary_nodes_created, 1);
    }

    let status = runtime
        .lcm_status_for_test("codex", Some("codex-compact"))
        .await
        .unwrap();
    assert_eq!(status.raw_message_count, 4);
    assert_eq!(status.summary_node_count, 1);
    assert!(status.dag.depths.values().any(|depth| depth.count == 1));

    let description = runtime
        .lcm_describe_for_test(LcmDescribeRequest {
            provider: "codex".to_string(),
            session_id: "codex-compact".to_string(),
            target: LcmDescribeTarget::Session,
        })
        .await
        .unwrap();
    assert_eq!(description.summary_nodes.len(), 1);
    // The daemon compression publishes a leaf summary over the session's raw
    // backlog (depth 0); the retired host side channel used to publish its
    // replacement-history summary at depth 1.
    assert_eq!(description.summary_nodes[0].depth, 0);
    assert_eq!(description.summary_nodes[0].source_count, 2);

    let node_id = description.summary_nodes[0].node_id.clone();
    let expanded = runtime
        .lcm_describe_for_test(LcmDescribeRequest {
            provider: "codex".to_string(),
            session_id: "codex-compact".to_string(),
            target: LcmDescribeTarget::SummaryNode { node_id },
        })
        .await
        .unwrap();
    let summary = expanded.summary_node.expect("summary node should expand");
    assert_eq!(summary.source_count, 2);
    // Provenance metadata rides the node-level describe; the session-level
    // summary listing is a lightweight overview without metadata_json.
    let metadata: serde_json::Value = serde_json::from_str(
        summary
            .metadata_json
            .as_deref()
            .expect("summary provenance"),
    )
    .unwrap();
    assert_eq!(
        metadata["summary_route"], "codex_app_server:codex-hook-test",
        "summary provenance must retain the actual app-server model: {metadata}"
    );

    let expansion = runtime
        .lcm_expand_for_test(LcmExpandRequest {
            provider: "codex".to_string(),
            session_id: "codex-compact".to_string(),
            target: LcmExpandTarget::SummaryNode {
                node_id: summary.node_id.clone(),
            },
            content_slice: Some(LcmContentSlice {
                offset: 0,
                limit: 1024,
            }),
            source_offset: 0,
            source_limit: Some(10),
        })
        .await
        .unwrap();
    assert_eq!(
        expansion
            .summary_node
            .as_ref()
            .expect("expanded summary node should be present")
            .summary_text,
        "Codex authoritative hook summary"
    );
    // The summary expansion returns the summary text as its content and the
    // exact raw sources alongside; the encrypted native payload never lands.
    assert!(!expansion.content.contains("encrypted-codex-summary"));
    assert_eq!(expansion.summary_sources.len(), 2);
    let source_contents = expansion
        .summary_sources
        .iter()
        .map(|source| source.content.as_str())
        .collect::<Vec<_>>();
    assert!(source_contents.contains(&"Map the release automation state"));
    assert!(source_contents.contains(&"Release automation is mapped."));
    assert!(
        !source_contents
            .iter()
            .any(|content| content.contains("encrypted-codex-summary"))
    );
}

#[tokio::test]
async fn repeated_codex_compactions_remain_native_evidence_until_daemon_effect() {
    let tmp = TempDir::new().unwrap();
    let (home, project) = setup(&tmp);
    let dir = home.join(".codex/sessions/2026/01/01");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("rollout-2026-01-01T00-00-30-codex-repeat.jsonl");
    let cwd = project.to_string_lossy();
    let compact = |at: &str| {
        serde_json::json!({
            "timestamp": at,
            "type": "compacted",
            "payload": {
                "message": "",
                "replacement_history": [
                    {"type": "compaction", "encrypted_content": "encrypted"}
                ]
            }
        })
    };
    let lines = [
        serde_json::json!({
            "timestamp": "2026-01-01T00:00:30.000Z",
            "type": "session_meta",
            "payload": {"id": "codex-repeat", "cwd": cwd, "model": "gpt-5.5"}
        }),
        serde_json::json!({
            "timestamp": "2026-01-01T00:00:31.000Z",
            "type": "event_msg",
            "payload": {"type": "user_message", "message": "First compacted prompt"}
        }),
        serde_json::json!({
            "timestamp": "2026-01-01T00:00:32.000Z",
            "type": "event_msg",
            "payload": {"type": "agent_message", "message": "First compacted reply"}
        }),
        compact("2026-01-01T00:00:33.000Z"),
        serde_json::json!({
            "timestamp": "2026-01-01T00:00:34.000Z",
            "type": "event_msg",
            "payload": {"type": "user_message", "message": "Second compacted prompt"}
        }),
        serde_json::json!({
            "timestamp": "2026-01-01T00:00:35.000Z",
            "type": "event_msg",
            "payload": {"type": "agent_message", "message": "Second compacted reply"}
        }),
        compact("2026-01-01T00:00:36.000Z"),
    ];
    std::fs::write(
        &path,
        lines
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )
    .unwrap();

    let runtime = registered_runtime(&home, &project).await;
    with_transcript_source_profile(
        ProfileRoot::new(runtime.profile_root_for_test()).with_home(&home),
        runtime.ingest_project_provider_for_test(&project, Some(SessionProvider::Codex)),
    )
    .await
    .unwrap();

    let status = runtime
        .lcm_status_for_test("codex", Some("codex-repeat"))
        .await
        .unwrap();
    assert_eq!(status.raw_message_count, 6);
    assert_eq!(status.summary_node_count, 0);
}
