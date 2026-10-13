#![cfg(all(feature = "test-transport", feature = "memory-provider-host"))]

//! Production Claude transcript import and default Native memory retrieval.
//!
//! The root composition owns the daemon project server, canonical session
//! stores, and MCP transport. This journey uses that composition to import real
//! host records, replay the import, publish the Claude session route, and recall
//! canonical messages and facts through shipped tools.

use std::path::Path;
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::{Value, json};
use tempfile::TempDir;
use tracedecay::daemon::ProductionProjectCompositionHarnessV1;
use tracedecay_configuration::config::PinnedRuntimeConfiguration;
use tracedecay_contracts::retained_surfaces::{
    FactCommitOwnerV1, FactProjectionV1, FactSearchGraphCoverageV1, FactStoreAddCommitV1,
    FactStoreAddRequestV1, FactStoreAddResultV1, FactV1, LcmLoadSessionRequestV1,
    LcmLoadSessionResultV1, LcmMessageV1, MemoryScopeV1, MessageSearchRequestV1,
    MessageSearchResultV1, RetainedOutcomeStatusV1, RetainedProjectSelectorV1,
};
use tracedecay_contracts::retrieval::{
    AdminCliSessionSyncV1, AdminCliSurfaceRequestV1, ContextResultV1, ContextSurfaceRequestV1,
};
use tracedecay_contracts::session_sync::SessionSyncSourceV1;
use tracedecay_contracts::{
    ApplicationProblem, ApplicationProblemRecord, ApplicationUnavailableClassV1, LegalAction,
    OperationTermination, RetryScope,
};
use tracedecay_domain::configuration::{
    MemoryProviderKindV1, MemoryProviderNcmObserverV1, MemoryProviderParticipationV1,
    MemoryProviderSelectionV1,
};
use tracedecay_domain::{HostIntegrationIdV1, ProjectId, TemporalModeV1};

const CLAUDE_SESSION: &str = "claude-memory-journey-session";
const USER_MESSAGE_ID: &str = "journey-uuid-1";
const ASSISTANT_MESSAGE_ID: &str = "journey-uuid-2";
const USER_CONTENT: &str = "how does the quicksilver transport probe decide its retry budget?";
const ASSISTANT_CONTENT: &str =
    "the quicksilver transport probe reads its retry budget from the pinned deadline";
const JOURNEY_TERM: &str = "quicksilver";
const TAIL_SENTINEL: &str = "pinned deadline";
const RECALL_LIMIT: usize = 5;
const EXPECTED_MESSAGES: usize = 2;

fn git(root: &Path, arguments: &[&str]) {
    let program = tracedecay_runtime_core::git::try_git_program()
        .expect("absolute git executable should resolve");
    let status = std::process::Command::new(program)
        .current_dir(root)
        .args(arguments)
        .status()
        .expect("git command runs");
    assert!(status.success(), "git {arguments:?} failed");
}

fn initialize_project(project: &Path) {
    std::fs::create_dir_all(project).expect("project root");
    git(project, &["init", "-q", "-b", "main"]);
    git(project, &["config", "user.email", "journey@example.com"]);
    git(project, &["config", "user.name", "Journey"]);
    std::fs::create_dir_all(project.join("src")).expect("project source directory");
    std::fs::write(
        project.join("src/lib.rs"),
        "/// Quicksilver transport probe.\npub fn quicksilver_probe() -> u8 { 7 }\n",
    )
    .expect("project source file");
    git(project, &["add", "."]);
    git(project, &["commit", "-q", "-m", "initial"]);
}

fn write_claude_transcript(transcript_home: &Path, project: &Path) {
    let directory = transcript_home.join(".claude/projects/-claude-memory-journey");
    std::fs::create_dir_all(&directory).expect("transcript directory");
    let cwd = project.to_string_lossy().to_string();
    let rows = [
        json!({
            "type": "user",
            "cwd": cwd,
            "sessionId": CLAUDE_SESSION,
            "uuid": USER_MESSAGE_ID,
            "timestamp": "2026-02-01T00:00:00.000Z",
            "message": {
                "role": "user",
                "content": USER_CONTENT,
            },
        }),
        json!({
            "type": "assistant",
            "cwd": cwd,
            "sessionId": CLAUDE_SESSION,
            "uuid": ASSISTANT_MESSAGE_ID,
            "parentUuid": USER_MESSAGE_ID,
            "timestamp": "2026-02-01T00:00:01.000Z",
            "message": {
                "id": "msg_journey_2",
                "role": "assistant",
                "model": "claude-opus-4-8",
                "content": [{"type": "text", "text": ASSISTANT_CONTENT}],
            },
        }),
    ];
    let contents = rows
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(
        directory.join(format!("{CLAUDE_SESSION}.jsonl")),
        format!("{contents}\n"),
    )
    .expect("write Claude transcript");
}

async fn tool_document(
    harness: &ProductionProjectCompositionHarnessV1,
    project: &Path,
    tool: &str,
    arguments: Value,
) -> Value {
    let result = tool_result(harness, project, tool, arguments).await;
    parse_tool_document(&result, tool)
}

async fn tool_result(
    harness: &ProductionProjectCompositionHarnessV1,
    project: &Path,
    tool: &str,
    arguments: Value,
) -> Value {
    let response = harness
        .call_tool(project, tool, arguments)
        .await
        .unwrap_or_else(|error| panic!("{tool} invocation failed: {error}"));
    assert!(
        response.error.is_none(),
        "{tool} JSON-RPC error: {response:?}"
    );
    response.result.expect("MCP tool result")
}

fn parse_tool_document(result: &Value, tool: &str) -> Value {
    assert_ne!(result["isError"], true, "{tool} failed: {result}");
    let text = result["content"]
        .as_array()
        .and_then(|content| content.first())
        .and_then(|item| item["text"].as_str())
        .unwrap_or_else(|| panic!("{tool} produced no text content: {result}"));
    serde_json::from_str(text)
        .unwrap_or_else(|error| panic!("{tool} returned invalid JSON: {error}: {text}"))
}

async fn fresh_session_tool_document(
    harness: &ProductionProjectCompositionHarnessV1,
    project: &Path,
    tool: &str,
    arguments: Value,
    deadline: Instant,
) -> Value {
    let mut last_problem = None;
    loop {
        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            tool_result(harness, project, tool, arguments.clone()),
        )
        .await
        .unwrap_or_else(|_| {
            panic!("{tool} exhausted its fresh-read budget; last problem: {last_problem:?}")
        });
        if result["isError"] != true {
            return parse_tool_document(&result, tool);
        }
        let wire = tracedecay_mcp::tool_errors::tool_result_problem(&result)
            .unwrap_or_else(|| panic!("{tool} failed without a canonical problem: {result}"));
        let problem: ApplicationProblemRecord = serde_json::from_value(wire.clone())
            .expect("the fresh-read refusal is a valid canonical problem record");
        assert_eq!(
            problem.code, "application.retained.authority-unavailable",
            "{tool} refused: {problem:?}"
        );
        assert!(
            matches!(
                problem.source(),
                ApplicationProblem::Unavailable {
                    classification: ApplicationUnavailableClassV1::Authority,
                    ..
                }
            ),
            "{tool} refused: {problem:?}"
        );
        assert_eq!(problem.retry_scope, Some(RetryScope::SameRequest));
        assert_eq!(problem.legal_actions, [LegalAction::Retry]);
        let delay = problem
            .pre_admission_retry_delay()
            .unwrap_or_else(|| panic!("{tool} cannot retry this refusal: {problem:?}"));
        assert!(
            delay < deadline.saturating_duration_since(Instant::now()),
            "{tool} exhausted its fresh-read budget: {problem:?}"
        );
        last_problem = Some(problem);
        tokio::time::sleep(delay).await;
    }
}

fn tool_payload(document: Value) -> Value {
    document
        .pointer("/outcome/value/payload")
        .cloned()
        .unwrap_or(document)
}

fn json_tool_arguments(request: impl Serialize) -> Value {
    let mut arguments = serde_json::to_value(request).expect("canonical tool request serializes");
    arguments
        .as_object_mut()
        .expect("canonical tool request is an object")
        .insert("format".to_owned(), json!("json"));
    arguments
}

async fn assert_default_native_configuration(
    harness: &ProductionProjectCompositionHarnessV1,
    project: &Path,
) -> ProjectId {
    let graph = harness.server(project).expect("project server").cg().await;
    let runtime = graph.configuration_runtime();
    let observed = runtime
        .observed_runtime_configuration()
        .await
        .expect("durable runtime activation is readable")
        .expect("the production composition recorded its configuration pin");
    let pin = PinnedRuntimeConfiguration::new(
        runtime.configuration_target().clone(),
        observed.revision_id,
        observed.snapshot,
    )
    .expect("the observed configuration decodes through the production authority");
    let config = pin.config();
    assert!(config.memory_provider_native_enabled);
    assert_eq!(
        config.memory_provider_ncm_observer,
        MemoryProviderNcmObserverV1::Disabled {}
    );
    let selection = MemoryProviderSelectionV1::resolve(
        config.memory_provider_native_enabled,
        &config.memory_provider_ncm_observer,
        &config.memory_provider_recall_routing,
    )
    .expect("the observed provider selection is valid");
    assert_eq!(
        selection.active_provider(),
        Some(MemoryProviderKindV1::Native)
    );
    assert_eq!(selection.native, MemoryProviderParticipationV1::Active);
    assert_eq!(selection.ncm, MemoryProviderParticipationV1::Disabled);
    pin.target().project_id.clone()
}

async fn import_project_transcripts(
    harness: &ProductionProjectCompositionHarnessV1,
    project: &Path,
) -> AdminCliSessionSyncV1 {
    let document = tool_document(
        harness,
        project,
        "tracedecay_admin_cli",
        json_tool_arguments(AdminCliSurfaceRequestV1::SessionsImport {}),
    )
    .await;
    let mut outcome: AdminCliSessionSyncV1 =
        serde_json::from_value(tool_payload(document)).expect("typed transcript import admission");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let idempotency_key = match &outcome {
            AdminCliSessionSyncV1::Complete {
                source,
                termination,
                failure_codes,
                ..
            } => {
                assert_eq!(*source, SessionSyncSourceV1::ImportTranscripts);
                assert_eq!(*termination, OperationTermination::Partial);
                assert!(
                    failure_codes.is_empty(),
                    "transcript import failed: {outcome:?}"
                );
                return outcome;
            }
            AdminCliSessionSyncV1::Accepted {
                idempotency_key, ..
            }
            | AdminCliSessionSyncV1::Joined {
                idempotency_key, ..
            } => idempotency_key.clone(),
            _ => panic!("transcript import did not complete: {outcome:?}"),
        };
        assert!(
            Instant::now() < deadline,
            "transcript import did not settle: {outcome:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        let document = tool_document(
            harness,
            project,
            "tracedecay_admin_cli",
            json_tool_arguments(AdminCliSurfaceRequestV1::SessionsSyncStatus {
                idempotency_key: idempotency_key.as_str().to_owned(),
            }),
        )
        .await;
        outcome =
            serde_json::from_value(tool_payload(document)).expect("typed transcript import status");
    }
}

/// Send the real hook protocol through the maintained MCP connection and
/// require one correlated acknowledgement for this exact Claude session route.
async fn publish_claude_host_session_route(
    harness: &ProductionProjectCompositionHarnessV1,
    project: &Path,
) {
    let event = tracedecay_hooks::DaemonHookEvent::session_start(
        HostIntegrationIdV1::Claude,
        project.to_path_buf(),
    )
    .with_route(Some(tracedecay_hooks::HookRouteMetadata {
        session_id: Some(CLAUDE_SESSION.to_owned()),
        thread_id: None,
        cwd: Some(project.to_path_buf()),
        worktree: Some(project.to_path_buf()),
        branch: tracedecay_runtime_core::branch::current_branch(project),
    }));
    let request_id = json!("hook-event-route-v1");
    let request = tracedecay_mcp::JsonRpcRequest {
        jsonrpc: "2.0".to_owned(),
        id: Some(request_id.clone()),
        method: tracedecay_hooks::HOOK_EVENT_METHOD.to_owned(),
        params: Some(serde_json::to_value(event).expect("the hook event serializes")),
    };
    let (mut transport, sender, mut responses) = tracedecay_mcp::transport::ChannelTransport::new();
    sender
        .send(serde_json::to_string(&request).expect("the hook request serializes"))
        .expect("the hook request is queued");
    drop(sender);
    let server = harness.server(project).expect("project server");
    Box::pin(server.run_connection(&mut transport))
        .await
        .expect("the hook connection completes");
    drop(transport);
    let mut acknowledgement = None;
    while let Some(line) = responses.recv().await {
        let response: tracedecay_mcp::JsonRpcResponse =
            serde_json::from_str(line.trim()).expect("the hook response is valid JSON-RPC");
        if response.id == request_id {
            assert!(
                acknowledgement.replace(response).is_none(),
                "the hook request receives exactly one acknowledgement"
            );
        }
    }
    let response = acknowledgement.expect("the hook acknowledgement correlates to its request");
    assert_eq!(response.id, request_id);
    assert!(
        response.error.is_none(),
        "hook acknowledgement failed: {response:?}"
    );
    assert_eq!(response.result, Some(json!({})));
}

async fn search_claude_messages(
    harness: &ProductionProjectCompositionHarnessV1,
    project: &Path,
    project_id: &ProjectId,
    deadline: Instant,
) -> MessageSearchResultV1 {
    loop {
        let document = fresh_session_tool_document(
            harness,
            project,
            "tracedecay_message_search",
            json_tool_arguments(MessageSearchRequestV1 {
                query: Some(JOURNEY_TERM.to_owned()),
                provider: Some("claude".to_owned()),
                project_selector: Some(RetainedProjectSelectorV1 {
                    project_id: project_id.clone(),
                }),
                include_subagents: Some(false),
                require_fresh: Some(true),
                limit: Some(u64::try_from(RECALL_LIMIT).expect("fixture recall limit")),
                ..MessageSearchRequestV1::default()
            }),
            deadline,
        )
        .await;
        let result: MessageSearchResultV1 =
            serde_json::from_value(tool_payload(document)).expect("typed canonical message search");
        assert!(
            result.error.is_none(),
            "canonical message search failed: {result:?}"
        );
        if result.outcome == RetainedOutcomeStatusV1::Complete
            && result.count == Some(EXPECTED_MESSAGES)
        {
            assert_eq!(result.status, RetainedOutcomeStatusV1::Ok);
            assert!(result.require_fresh);
            assert!(!result.refresh_required);
            assert_eq!(result.provider, "claude");
            assert_eq!(result.requested_provider.as_deref(), Some("claude"));
            assert_eq!(result.store_scope.as_deref(), Some("project"));
            let hits = result
                .results
                .as_ref()
                .expect("complete message search has hits");
            assert_eq!(hits.len(), EXPECTED_MESSAGES);
            assert!(hits.len() <= RECALL_LIMIT);
            for (message_id, role, content) in [
                (USER_MESSAGE_ID, "user", USER_CONTENT),
                (ASSISTANT_MESSAGE_ID, "assistant", ASSISTANT_CONTENT),
            ] {
                let matches = hits
                    .iter()
                    .filter(|hit| hit.message.message_id == message_id)
                    .collect::<Vec<_>>();
                assert_eq!(
                    matches.len(),
                    1,
                    "each captured Claude record has one canonical identity"
                );
                let hit = matches[0];
                assert_eq!(hit.session.provider, "claude");
                assert_eq!(hit.session.session_id, CLAUDE_SESSION);
                assert_eq!(hit.message.provider, "claude");
                assert_eq!(hit.message.session_id, CLAUDE_SESSION);
                assert_eq!(hit.message.role, role);
                assert_eq!(hit.message.text, content);
                if let Some(owner) = &hit.project_id {
                    assert_eq!(owner, project_id.as_str());
                }
            }
            assert!(
                hits.iter()
                    .any(|hit| hit.message.text.ends_with(TAIL_SENTINEL))
            );
            return result;
        }
        assert!(
            matches!(
                result.outcome,
                RetainedOutcomeStatusV1::Complete
                    | RetainedOutcomeStatusV1::CompleteZero
                    | RetainedOutcomeStatusV1::Partial
                    | RetainedOutcomeStatusV1::Stale
            ),
            "canonical message search refused: {result:?}"
        );
        assert!(
            Instant::now() < deadline,
            "the captured Claude records did not become fresh: {result:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn load_claude_messages(
    harness: &ProductionProjectCompositionHarnessV1,
    project: &Path,
    deadline: Instant,
) -> Vec<LcmMessageV1> {
    let document = fresh_session_tool_document(
        harness,
        project,
        "tracedecay_lcm_load_session",
        json_tool_arguments(LcmLoadSessionRequestV1 {
            provider: Some("claude".to_owned()),
            session_id: CLAUDE_SESSION.to_owned(),
            cursor: None,
            temporal_mode: Some(TemporalModeV1::Current),
            limit: Some(u64::try_from(RECALL_LIMIT).expect("fixture recall limit")),
            role: None,
            roles: None,
            start_time: None,
            end_time: None,
            content_offset: None,
            content_limit: None,
        }),
        deadline,
    )
    .await;
    let result: LcmLoadSessionResultV1 =
        serde_json::from_value(tool_payload(document)).expect("typed canonical LCM retrieval");
    assert_eq!(result.status, RetainedOutcomeStatusV1::Ok);
    assert!(
        result.error.is_none(),
        "canonical LCM retrieval failed: {result:?}"
    );
    assert_eq!(result.provider.as_deref(), Some("claude"));
    assert_eq!(result.session_id.as_deref(), Some(CLAUDE_SESSION));
    assert_eq!(result.omitted, Some(0));
    assert!(
        result
            .temporal
            .as_ref()
            .expect("LCM temporal authority")
            .next_cursor
            .is_none()
    );
    let mut messages = result.messages;
    assert_eq!(messages.len(), EXPECTED_MESSAGES);
    assert!(messages.len() <= RECALL_LIMIT);
    messages.sort_by(|left, right| left.message_id.cmp(&right.message_id));
    for (message, (message_id, role, content)) in messages.iter().zip([
        (USER_MESSAGE_ID, "user", USER_CONTENT),
        (ASSISTANT_MESSAGE_ID, "assistant", ASSISTANT_CONTENT),
    ]) {
        assert_eq!(message.provider, "claude");
        assert_eq!(message.session_id, CLAUDE_SESSION);
        assert_eq!(message.message_id, message_id);
        assert_eq!(message.role, role);
        assert_eq!(message.content, content);
        assert!(!message.content_range.truncated);
        assert_eq!(message.content_range.offset, 0);
        let chars = u64::try_from(content.chars().count()).expect("fixture content length");
        assert_eq!(message.content_range.returned_chars, chars);
        assert_eq!(message.content_range.total_chars, chars);
    }
    assert!(
        messages
            .iter()
            .any(|message| message.content.ends_with(TAIL_SENTINEL))
    );
    messages
}

async fn add_project_fact(
    harness: &ProductionProjectCompositionHarnessV1,
    project: &Path,
    project_id: &ProjectId,
) -> FactV1 {
    let document = tool_document(
        harness,
        project,
        "tracedecay_fact_store_add",
        json_tool_arguments(FactStoreAddRequestV1 {
            content: ASSISTANT_CONTENT.to_owned(),
            memory_scope: Some(MemoryScopeV1::Project),
            category: None,
            tags: Vec::new(),
            entities: Vec::new(),
            trust: Some(0.91),
            source_label: Some("claude-memory-journey".to_owned()),
            metadata: None,
            project_selector: Some(RetainedProjectSelectorV1 {
                project_id: project_id.clone(),
            }),
        }),
    )
    .await;
    assert_eq!(document["outcome"]["outcome"], "effect");
    let effect = &document["outcome"]["value"];
    assert_eq!(effect["reconciliation"], "reconciled");
    assert_eq!(effect["execution"]["termination"], "completed");
    assert_eq!(effect["receipt"]["outcome"], "completed");
    let outcome: FactStoreAddResultV1 =
        serde_json::from_value(tool_payload(document)).expect("typed canonical fact commit");
    let FactStoreAddResultV1::Committed {
        result: FactStoreAddCommitV1::Added { fact, commit },
    } = outcome
    else {
        panic!("the isolated project must commit a new canonical fact: {outcome:?}");
    };
    let FactProjectionV1::Available { fact } = fact else {
        panic!("the committed canonical fact must be readable: {fact:?}");
    };
    let owner = FactCommitOwnerV1::Project {
        project_id: project_id.clone(),
    };
    assert_eq!(fact.owner, owner);
    assert_eq!(commit.owner, owner);
    assert_eq!(commit.fact_id, fact.fact_id);
    assert_eq!(fact.content, ASSISTANT_CONTENT);
    assert_eq!(fact.source_label.as_deref(), Some("claude-memory-journey"));
    assert!(!commit.committed_event_ids.is_empty());
    *fact
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_native_recalls_canonical_claude_messages_and_project_facts_after_import_replay() {
    let isolation = TempDir::new().expect("journey isolation");
    let project = isolation.path().join("project");
    initialize_project(&project);
    let transcript_home =
        ProductionProjectCompositionHarnessV1::transcript_source_home(isolation.path())
            .expect("the composition pins its own transcript source home");
    write_claude_transcript(&transcript_home, &project);
    let harness = ProductionProjectCompositionHarnessV1::open(isolation.path(), [project.clone()])
        .await
        .expect("the real production project composition opens");
    let project_id = assert_default_native_configuration(&harness, &project).await;
    import_project_transcripts(&harness, &project).await;
    publish_claude_host_session_route(&harness, &project).await;
    let capture_deadline = Instant::now() + Duration::from_secs(30);
    let searched = search_claude_messages(&harness, &project, &project_id, capture_deadline).await;
    let captured = load_claude_messages(&harness, &project, capture_deadline).await;

    import_project_transcripts(&harness, &project).await;
    let replay_deadline = Instant::now() + Duration::from_secs(30);
    assert_eq!(
        load_claude_messages(&harness, &project, replay_deadline).await,
        captured,
        "import replay preserves both canonical record identities and complete content"
    );
    let replayed_search =
        search_claude_messages(&harness, &project, &project_id, replay_deadline).await;
    assert_eq!(replayed_search.results, searched.results);

    let fact = add_project_fact(&harness, &project, &project_id).await;
    let document = tool_document(
        &harness,
        &project,
        "tracedecay_context",
        json_tool_arguments(ContextSurfaceRequestV1 {
            task: USER_CONTENT.to_owned(),
            max_nodes: None,
            include_code: None,
            max_code_blocks: None,
            mode: None,
            include_memory: None,
            memory_limit: Some(u32::try_from(RECALL_LIMIT).expect("fixture recall limit")),
            memory_min_trust: None,
            temporal_query: None,
            exclusions: None,
            lexical_anchors: None,
            prefer_symbol: None,
            budget_tokens: None,
        }),
    )
    .await;
    let payload = tool_payload(document);
    assert!(
        payload.get("advisory_provider_memory").is_none(),
        "default Native uses canonical memory_matches: {payload}"
    );
    let context: ContextResultV1 =
        serde_json::from_value(payload).expect("typed Native context result");
    assert!(
        context.memory_matches_error.is_none(),
        "Native recall failed: {context:?}"
    );
    assert!(!context.memory_matches.is_empty());
    assert!(context.memory_matches.len() <= RECALL_LIMIT);
    assert!(matches!(
        context.memory_graph_coverage,
        Some(FactSearchGraphCoverageV1::Complete { .. })
    ));
    let recalled = context
        .memory_matches
        .iter()
        .find(|hit| hit.fact.fact_id == fact.fact_id)
        .expect("default Native context recalls the actual committed project fact");
    assert_eq!(recalled.fact.owner, fact.owner);
    assert_eq!(recalled.fact.content, ASSISTANT_CONTENT);
    assert!(recalled.fact.content.ends_with(TAIL_SENTINEL));
    harness.shutdown().await;
}
