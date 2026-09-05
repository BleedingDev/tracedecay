//! The Claude Code host memory journey.
//!
//! Everything here runs against the *production* composition root
//! ([`ProductionProjectCompositionHarnessV1`] opens projects through
//! `production_project_server`, the same function the daemon calls), so what
//! is proved is the shipped path and not a hand-wired mount:
//!
//! 1. an operator turns the two project-scoped memory-provider gates on
//!    through the real `tracedecay_configuration_set` surface and the daemon
//!    is then restarted on that project -- the sequence those `DaemonRestart`
//!    settings require, since a composition that is already open keeps the
//!    mounts it opened with;
//! 2. Claude Code writes a real transcript into the composition's own pinned
//!    transcript home;
//! 3. the shipped project transcript import commits those Claude messages as
//!    canonical observations, and the mounted observation journey admits them
//!    into the durable journal and settles them against Native exactly once;
//! 4. the host publishes that session's workspace route through the shipped
//!    `tracedecay/hookEvent` notification -- the only writer of the daemon's
//!    private route cache, and the precondition for any call that names an
//!    explicit session identity;
//! 5. a later `tracedecay_context` call carrying the same host session id
//!    receives the advisory provider-memory lane, bounded and de-duplicated.
//!
//! What this module deliberately does **not** claim is hook *causality*. The
//! commit above is driven by the administrative transcript import, so it
//! proves settlement and recall, not that a hook invocation caused them. The
//! causal proof -- an empty journal, a transcript written only after the
//! project is mounted, and then the shipped `tracedecay
//! hook-claude-session-start` and `tracedecay hook-stop` binaries invoked as
//! subprocesses with Claude Code's own payloads on stdin -- lives in
//! `crates/tracedecay-cli/tests/product_memory_provider_claude_host_journey.rs`,
//! where a real daemon and a real binary are available to drive it.
//!
//! Two more journeys live beside it:
//! [`the_advisory_lane_is_additive_and_never_costs_the_claude_host_its_own_answer`]
//! holds the fail-open property -- a healthy route answers and a genuinely
//! failing provider degrades in a typed state that names its routed provider,
//! with the two answers taken from one composition at one source revision so
//! their host sections are compared for exact equality, and both still keep
//! every section a dormant composition produced -- and
//! [`the_shipped_claude_bundle_stages_hooks_and_registers_project_rules_without_disturbing_operator_state`]
//! holds the bundle-staging property. Deployed *registration* through the
//! Claude CLI -- install, re-install, update, deactivate, undo, and rollback
//! against a real `claude` executable -- is proved by
//! `crates/tracedecay-cli/tests/host_lifecycle_cli_acceptance.rs`.
//!
//! Exact project/worktree identity is compared against the authoritative
//! resolved scope rather than re-derived.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::{Value, json};
use tempfile::TempDir;
use tracedecay_domain::configuration::{
    ConfigurationIdempotencyKey, ConfigurationLayerIdV1, ConfigurationRevisionId,
    ConfigurationValueV1, MEMORY_PROVIDER_NATIVE_ENABLED_SETTING_KEY,
    MEMORY_PROVIDER_RECALL_ROUTING_SETTING_KEY, SettingKey,
};
use tracedecay_domain::{ProjectId, UserProfileId};
use tracedecay_memory_provider_registry::{
    HandshakeRequest, HandshakeResponse, NativeMemoryApplicationPort, NativeObservation,
    ProviderCall, ProviderDescriptor, ProviderReply, TerminalCode, TerminalRecord,
};

use super::journey_journal_inspection::{
    ACKNOWLEDGED_DELIVERY_STATE, journal_digest, journal_row_identities, open_journal,
};
use super::{JOURNAL_FILE_NAME, SESSION_MESSAGE_OBSERVATION_KIND, exact_scope_for_session};
use crate::daemon::production_harness::ProductionProjectCompositionHarnessV1;

/// The Claude Code session id the whole journey is bound to. It is the host's
/// own identity: the hook route publishes it, the transcript carries it, and
/// the later context call names it.
const CLAUDE_SESSION: &str = "claude-memory-journey-session";

/// A term that appears in every transcript record, so a recall that answers at
/// all has something to answer with.
const JOURNEY_TERM: &str = "quicksilver";

/// The assistant message's final phrase, proving recall returns the complete
/// committed content rather than only its matching prefix.
const TAIL_SENTINEL: &str = "pinned deadline";

/// How many deliveries this journey's transcript must produce: the observation
/// journey admits one `session.message_committed.v1` per committed session
/// message, and [`write_claude_transcript`] writes exactly one user record and
/// one assistant record.
const EXPECTED_SESSION_MESSAGE_ROWS: usize = 2;

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

/// A real git project: the composition refuses to resolve an exact scope
/// without a repository, a worktree, and a checked-out reference.
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

/// The source edit the Claude Code session itself makes while it runs: the
/// agent read the quicksilver probe, then changed it.
///
/// It is committed between the dormant composition and the restarted one for
/// a reason that is worth stating plainly. The upstream in-process
/// composition harness only observes a published code index on a *second*
/// `open` when the verified source changed in between: with a byte-identical
/// checkout the restarted scheduler neither republishes a complete generation
/// nor reports the typed generation-empty state, so
/// `wait_for_production_composition_code_index`
/// (`crates/tracedecay/src/daemon/production_harness.rs:833-870`) exhausts its
/// 20-second budget and `open` fails with `production-composition code index
/// did not publish`. That is an upstream defect, not a product one -- it also
/// fails two pre-existing upstream journeys
/// (`configuration_idempotency_journey_test::
/// user_profile_configuration_batch_has_cli_dashboard_parity_after_restart`
/// and `...::configuration_set_has_cli_mcp_http_sdk_parity_and_replays_after_restart`)
/// and it reproduces with no memory-provider gate and no transcript at all.
///
/// Committing a real edit here is therefore not a workaround bolted onto the
/// journey: it is what the host session under test actually does, and it is
/// the shape of restart the harness can observe.
fn commit_claude_session_source_edit(project: &Path) {
    std::fs::write(
        project.join("src/lib.rs"),
        "/// Quicksilver transport probe.\n\
         ///\n\
         /// The retry budget is read from the pinned deadline.\n\
         pub fn quicksilver_probe() -> u8 { 11 }\n",
    )
    .expect("the Claude session's own source edit");
    git(project, &["add", "."]);
    git(project, &["commit", "-q", "-m", "quicksilver retry budget"]);
}

/// Writes the transcript Claude Code itself writes, into the transcript home
/// the composition pins for this isolation root. A transcript written under
/// the ambient `$HOME` is invisible to the composed daemon.
fn write_claude_transcript(transcript_home: &Path, project: &Path) {
    let directory = transcript_home.join(".claude/projects/-claude-memory-journey");
    std::fs::create_dir_all(&directory).expect("transcript directory");
    let cwd = project.to_string_lossy().to_string();
    let rows = [
        json!({
            "type": "user",
            "cwd": cwd,
            "sessionId": CLAUDE_SESSION,
            "uuid": "journey-uuid-1",
            "timestamp": "2026-02-01T00:00:00.000Z",
            "message": {
                "role": "user",
                "content": format!("how does the {JOURNEY_TERM} transport probe decide its retry budget?"),
            },
        }),
        json!({
            "type": "assistant",
            "cwd": cwd,
            "sessionId": CLAUDE_SESSION,
            "uuid": "journey-uuid-2",
            "parentUuid": "journey-uuid-1",
            "timestamp": "2026-02-01T00:00:01.000Z",
            "message": {
                "id": "msg_journey_2",
                "role": "assistant",
                "model": "claude-opus-4-8",
                "content": [{
                    "type": "text",
                    "text": format!("the {JOURNEY_TERM} transport probe reads its retry budget from the pinned deadline"),
                }],
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

/// The tool payload of a successful MCP `tools/call`, panicking on a protocol
/// error so a refusal can never read as an empty answer.
fn tool_text(response: &tracedecay_mcp::transport::JsonRpcResponse, tool_name: &str) -> String {
    let result = response
        .result
        .as_ref()
        .unwrap_or_else(|| panic!("{tool_name} JSON-RPC error: {:?}", response.error));
    result["content"]
        .as_array()
        .and_then(|content| content.first())
        .and_then(|item| item["text"].as_str())
        .unwrap_or_else(|| panic!("{tool_name} produced no text content: {result}"))
        .to_owned()
}

async fn current_revision(
    harness: &ProductionProjectCompositionHarnessV1,
    project: &Path,
) -> ConfigurationRevisionId {
    harness
        .server(project)
        .expect("project server")
        .cg()
        .await
        .configuration_runtime()
        .client()
        .current()
        .await
        .expect("current configuration")
        .revision_id
}

async fn project_identity(
    harness: &ProductionProjectCompositionHarnessV1,
    project: &Path,
) -> (ProjectId, UserProfileId) {
    let graph = harness.server(project).expect("project server").cg().await;
    let project_id = graph
        .configuration_runtime()
        .configuration_target()
        .project_id
        .clone();
    let profile_id = graph
        .configuration_runtime()
        .registered_database()
        .binding()
        .shard_id
        .profile_id
        .clone();
    (project_id, profile_id)
}

/// Turns one canonical configuration setting on through the shipped
/// `tracedecay_configuration_set` MCP surface — the operator path, not a
/// direct store write.
async fn configuration_set(
    harness: &ProductionProjectCompositionHarnessV1,
    project: &Path,
    layer: ConfigurationLayerIdV1,
    key: &str,
    value: ConfigurationValueV1,
    idempotency: &str,
) {
    let expected_revision = current_revision(harness, project).await;
    let request = tracedecay_application::ConfigurationSetRequestV1 {
        layer,
        key: SettingKey::new(key).expect("canonical setting key"),
        value,
        expected_revision: expected_revision.clone(),
        idempotency_key: ConfigurationIdempotencyKey::new(idempotency)
            .expect("configuration idempotency key"),
    };
    // The request carries exactly the wire fields `ConfigurationSetRequestV1`
    // declares; it rejects unknown members, so no rendering hint may ride
    // along with it.
    let arguments = serde_json::to_value(&request).expect("configuration set request");
    let response = harness
        .call_tool(project, "tracedecay_configuration_set", arguments)
        .await
        .expect("configuration set tool call");
    assert!(
        response.error.is_none(),
        "the operator gate must reach the configuration surface: {response:?}"
    );
    let result = response
        .result
        .as_ref()
        .expect("configuration set tool result");
    assert_ne!(
        result["isError"], true,
        "the operator gate must settle as a durable configuration effect: {result}"
    );
    assert_ne!(
        current_revision(harness, project).await,
        expected_revision,
        "committing {key} must advance the canonical configuration revision"
    );
}

/// Runs the shipped project transcript import — the same
/// `SessionSyncCommandV1::ImportTranscripts` pass the daemon's session-sync
/// worker runs — and returns how many session messages it committed.
async fn import_project_transcripts(
    harness: &ProductionProjectCompositionHarnessV1,
    project: &Path,
) -> Value {
    let response = harness
        .call_tool(
            project,
            "tracedecay_admin_cli",
            json!({ "action": "sessions_import", "format": "json" }),
        )
        .await
        .expect("transcript import tool call");
    let text = tool_text(&response, "tracedecay_admin_cli");
    serde_json::from_str(&text).unwrap_or(Value::Null)
}

/// Publishes this host session's workspace route the shipped way: the
/// acknowledged `tracedecay/hookEvent` request a Claude Code lifecycle hook
/// sends the daemon, carrying the session's structural identity and workspace.
///
/// This is a *precondition* of the recall below, not an extra claim. A
/// registered-project reader whose arguments carry an explicit session or
/// thread identity fails closed when that identity has no registered private
/// project route (`crates/tracedecay/src/mcp/server/requests/tool_dispatch.rs`,
/// `route_tool_arguments`) -- deliberately, so one host's session can never
/// inherit the workspace another connection happened to open. The recall is
/// bound to `CLAUDE_SESSION` by exactly that explicit identity
/// (`cognitive_recall::advisory_session_binding`), so the route a real Claude
/// Code session publishes must exist before the call, and
/// `McpServer::update_hook_workspace_route` is the only thing that publishes
/// one.
///
/// It commits nothing: a `sessionStart` event plans a branch sync
/// (`hook_events::plan_session_start_hook_event`), so the journey's only
/// commit driver is still the administrative transcript import above, and this
/// module's "no hook causality" claim is untouched.
async fn publish_claude_host_session_route(
    harness: &ProductionProjectCompositionHarnessV1,
    project: &Path,
) {
    let event = tracedecay_hooks::DaemonHookEvent::session_start(
        tracedecay_hooks::HookAgent::Claude,
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
    let response = harness
        .server(project)
        .expect("project server")
        .handle_request(&request)
        .await
        .expect("the acknowledged hook request returns after route publication");
    assert_eq!(response.id, request_id);
    assert!(
        response.error.is_none(),
        "hook acknowledgement failed: {response:?}"
    );
    assert_eq!(response.result, Some(json!({ "processed": true })));
}

/// The advisory provider-memory lane of one `tracedecay_context` answer, or
/// `None` when the answer carries no lane at all.
async fn context_advisory_lane(
    harness: &ProductionProjectCompositionHarnessV1,
    project: &Path,
    task: &str,
    session: Option<&str>,
) -> (Value, Option<Value>) {
    let mut arguments = json!({ "task": task, "format": "json" });
    if let Some(session) = session {
        arguments["_meta"] = json!({ "session_id": session });
    }
    let response = harness
        .call_tool(project, "tracedecay_context", arguments)
        .await
        .expect("context tool call");
    let text = tool_text(&response, "tracedecay_context");
    let parsed: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
    let lane = parsed
        .get("advisory_provider_memory")
        .filter(|value| !value.is_null())
        .cloned();
    (parsed, lane)
}

/// Turns both memory-provider gates on for this project through the shipped
/// `tracedecay_configuration_set` surface. Both settings are project-scoped
/// and `DaemonRestart`: a composition that is already open keeps the mounts it
/// opened with, so the daemon must be restarted before they take effect.
async fn enable_memory_provider_host(
    harness: &ProductionProjectCompositionHarnessV1,
    project: &Path,
) {
    let (project_id, _) = project_identity(harness, project).await;
    let layer = ConfigurationLayerIdV1::Project { project_id };
    configuration_set(
        harness,
        project,
        layer.clone(),
        MEMORY_PROVIDER_NATIVE_ENABLED_SETTING_KEY,
        ConfigurationValueV1::Boolean(true),
        "configuration.idempotency.claude-journey-host",
    )
    .await;
    configuration_set(
        harness,
        project,
        layer,
        MEMORY_PROVIDER_RECALL_ROUTING_SETTING_KEY,
        ConfigurationValueV1::Text(
            json!({ "active_provider": tracedecay_memory_provider_registry::NATIVE_PROVIDER_ID })
                .to_string(),
        ),
        "configuration.idempotency.claude-journey-routing",
    )
    .await;
}

/// Restarts the daemon on this project with both provider gates committed: the
/// dormant composition commits them, stops, and the composition that comes
/// back has the provider host mounted.
async fn composition_with_memory_provider_host(
    isolation: &Path,
    project: &Path,
) -> ProductionProjectCompositionHarnessV1 {
    let dormant = ProductionProjectCompositionHarnessV1::open(isolation, [project.to_path_buf()])
        .await
        .expect("dormant production composition");
    enable_memory_provider_host(&dormant, project).await;
    dormant.shutdown().await;
    // The session's own source edit lands before the daemon comes back; see
    // `commit_claude_session_source_edit`.
    commit_claude_session_source_edit(project);

    ProductionProjectCompositionHarnessV1::open(isolation, [project.to_path_buf()])
        .await
        .expect("production composition with the memory provider host mounted")
}

/// The host's own sections of one `tracedecay_context` answer: every top-level
/// member except the advisory lane, in sorted order, paired with a shape
/// witness that a hollowed-out section cannot forge.
///
/// The witness is `(JSON type, element/character count)` rather than the bare
/// key name. That distinction is the whole point: a lane that emptied,
/// retyped or truncated a host section would keep every key and still be
/// caught here.
fn host_section_shapes(answer: &Value) -> Vec<(String, &'static str, usize)> {
    fn shape(value: &Value) -> (&'static str, usize) {
        match value {
            Value::Null => ("null", 0),
            Value::Bool(_) => ("bool", 1),
            Value::Number(_) => ("number", 1),
            Value::String(text) => ("string", text.chars().count()),
            Value::Array(items) => ("array", items.len()),
            Value::Object(members) => ("object", members.len()),
        }
    }
    let mut sections = answer
        .as_object()
        .map(|object| {
            object
                .iter()
                .filter(|(key, _)| key.as_str() != "advisory_provider_memory")
                .map(|(key, value)| {
                    let (kind, width) = shape(value);
                    (key.clone(), kind, width)
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    sections.sort();
    sections
}

/// The exact scope the journal must have bound this host session to, taken
/// from the authoritative resolved scope rather than re-derived from a path.
fn expected_exact_scope_sha256(
    project: &Path,
    project_id: &ProjectId,
    profile_id: &UserProfileId,
) -> String {
    let scope = tracedecay_code_index_runtime::resolved_scope_for_project(project, project_id)
        .expect("authoritative resolved scope");
    exact_scope_for_session(profile_id, &scope, CLAUDE_SESSION)
        .expect("exact scope for the Claude host session")
        .exact_scope_sha256()
        .to_owned()
}

/// Every Claude Code session message this project commits settles in the
/// durable observation journal against Native exactly once, under this
/// project's authoritative exact scope, and a later `tracedecay_context` call
/// naming the same host session recalls it inside the advisory lane's own
/// candidate budget, de-duplicated.
///
/// Real defect this catches: an observation journey that admits the same
/// committed message twice, binds it to a scope other than the resolved
/// worktree scope, retries an already-accepted delivery, or lets recall
/// answer with unbounded or duplicated candidates.
///
/// The commit here is driven by the shipped administrative transcript import,
/// so this test claims settlement and recall -- not hook causality. The
/// causal proof that real `tracedecay hook-claude-session-start` and
/// `tracedecay hook-stop` invocations are what put the messages in the journal
/// is the subprocess journey in
/// `crates/tracedecay-cli/tests/product_memory_provider_claude_host_journey.rs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_committed_claude_session_message_settles_once_and_a_later_context_call_recalls_it_bounded()
 {
    let isolation = TempDir::new().expect("journey isolation");
    let project: PathBuf = isolation.path().join("project");
    initialize_project(&project);

    let transcript_home =
        ProductionProjectCompositionHarnessV1::transcript_source_home(isolation.path())
            .expect("the composition pins its own transcript source home");
    write_claude_transcript(&transcript_home, &project);

    let harness = composition_with_memory_provider_host(isolation.path(), &project).await;
    let (project_id, profile_id) = project_identity(&harness, &project).await;
    let data_root = harness
        .project_data_root(&project)
        .await
        .expect("project data root");
    let journal_path = data_root.join(JOURNAL_FILE_NAME);
    assert!(
        journal_path.exists(),
        "an enabled composition must mount the durable observation journal at {}",
        journal_path.display()
    );
    let journal = open_journal(&journal_path);

    // 1. The shipped transcript import commits the host's session messages as
    //    canonical observations for this project. Its own reported outcome is
    //    the evidence, not a test-only counter: a route that refused would say
    //    so here rather than leaving an empty store to be mistaken for one
    //    that had nothing to import.
    let imported = import_project_transcripts(&harness, &project).await;
    assert_ne!(
        imported,
        Value::Null,
        "the shipped transcript import must report a typed outcome"
    );

    // 2. The mounted journey admits every committed message and settles it
    //    against Native exactly once.
    let rows = journal
        .await_settlement(EXPECTED_SESSION_MESSAGE_ROWS)
        .await;
    assert_eq!(
        rows.len(),
        EXPECTED_SESSION_MESSAGE_ROWS,
        "the transcript's two committed session messages must produce exactly two deliveries: \
         {:?}",
        journal_digest(&rows)
    );
    let expected_scope = expected_exact_scope_sha256(&project, &project_id, &profile_id);
    for row in &rows {
        assert_eq!(
            row.observation_kind, SESSION_MESSAGE_OBSERVATION_KIND,
            "the journey admits exactly the session-message observation kind"
        );
        assert_eq!(
            row.exact_scope_sha256, expected_scope,
            "every journal row must carry this project's exact worktree-bound scope"
        );
        assert_eq!(
            row.state, ACKNOWLEDGED_DELIVERY_STATE,
            "Native accepts session messages, so the row settles acknowledged"
        );
        assert_eq!(
            row.attempt_number, 1,
            "an accepted observation is delivered once, never retried"
        );
        assert!(
            row.content_present,
            "a settled delivery still holds its content until retention takes it: {:?}",
            journal_digest(&rows)
        );
    }
    // Two *distinct* messages, not the same one journalled twice: the source
    // positions the journey admitted must differ.
    let mut sequences = rows
        .iter()
        .map(|row| row.source_sequence)
        .collect::<Vec<_>>();
    sequences.sort_unstable();
    sequences.dedup();
    assert_eq!(
        sequences.len(),
        EXPECTED_SESSION_MESSAGE_ROWS,
        "each committed message occupies its own source position: {:?}",
        journal_digest(&rows)
    );

    // 3. Re-running the same import is idempotent: the journal holds the same
    //    rows, by identity, because the idempotency key is content-derived.
    //    The import call is synchronous, so anything it admitted is already in
    //    the journal by the time it returns.
    let settled = journal_row_identities(&rows);
    let _ = import_project_transcripts(&harness, &project).await;
    let replayed = journal
        .await_settlement(EXPECTED_SESSION_MESSAGE_ROWS)
        .await;
    assert_eq!(
        journal_row_identities(&replayed),
        settled,
        "replaying the same transcript must reproduce exactly the same deliveries, by \
         observation identity, payload digest and attempt count: {:?}",
        journal_digest(&replayed)
    );

    // 4. The host publishes this session's workspace route, exactly as a
    //    Claude Code lifecycle hook does. Without it the next call's explicit
    //    session identity has no registered private project route and the
    //    daemon refuses it, which is the guard working, not the journey.
    publish_claude_host_session_route(&harness, &project).await;

    // 5. A later context call naming the same host session receives the
    //    advisory provider-memory lane, bounded by the lane's own budget.
    let (answer, lane) = context_advisory_lane(
        &harness,
        &project,
        &format!("how does the {JOURNEY_TERM} transport probe decide its retry budget?"),
        Some(CLAUDE_SESSION),
    )
    .await;
    let lane = lane
        .unwrap_or_else(|| panic!("an active provider must contribute an advisory lane: {answer}"));
    assert_eq!(
        lane["state"], "answered",
        "the advisory lane must answer rather than report a refusal: {lane}"
    );
    let candidates = lane["candidates"].as_array().cloned().unwrap_or_default();
    assert!(
        !candidates.is_empty(),
        "the observed Claude session must be recallable: {lane}"
    );
    assert!(
        candidates.len() <= super::super::cognitive_recall::ADVISORY_RECALL_MAXIMUM_CANDIDATES,
        "the advisory lane is bounded by its own candidate budget: {lane}"
    );
    let mut provenance = candidates
        .iter()
        .map(|candidate| candidate["provenance"].to_string())
        .collect::<Vec<_>>();
    let total = provenance.len();
    provenance.sort();
    provenance.dedup();
    assert_eq!(
        provenance.len(),
        total,
        "recall candidates must be de-duplicated: {lane}"
    );
    assert!(
        candidates.iter().any(|candidate| {
            candidate["content"].as_str().is_some_and(|content| {
                content.contains(JOURNEY_TERM) && content.contains(TAIL_SENTINEL)
            })
        }),
        "the advisory lane must recall the assistant message whole, tail included: {lane}"
    );

    harness.shutdown().await;
}

/// The advisory lane is *additive*: mounting the provider host may add the
/// provider-memory lane to a Claude Code context answer and may never take a
/// host section away from it, and whatever the lane terminates as, it says so
/// in a typed state that names the provider its route pinned.
///
/// Both halves are exercised on **one** composition, at **one** source
/// revision, and are separated by nothing but a boolean the routed provider
/// itself reads: the composition mounts a caller-owned provider that delegates
/// every operation to the production Native port until it is asked to fail,
/// and then answers recall with a `ProviderUnavailable` terminal. Because the
/// healthy and degraded answers come from the same mount and the same
/// checkout, their host sections may be compared for *exact* equality rather
/// than for mere survival.
///
/// Real defect this catches: an advisory lane that rewrites, truncates, or
/// replaces the canonical answer -- the failure mode that makes provider
/// memory unsafe to turn on, because a broken or empty provider would then
/// silently degrade the answer the agent acts on. It equally catches the
/// quieter version of that defect: a provider failure that is reported as an
/// ordinary empty answer, so an operator cannot tell "the provider knew
/// nothing" from "the provider is broken". The dormant composition is the
/// control: it answers the same task on an identical repository with no
/// provider host at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_advisory_lane_is_additive_and_never_costs_the_claude_host_its_own_answer() {
    let isolation = TempDir::new().expect("journey isolation");
    let project: PathBuf = isolation.path().join("project");
    initialize_project(&project);
    let transcript_home =
        ProductionProjectCompositionHarnessV1::transcript_source_home(isolation.path())
            .expect("the composition pins its own transcript source home");
    write_claude_transcript(&transcript_home, &project);
    let task = format!("how does the {JOURNEY_TERM} transport probe decide its retry budget?");

    // Control: this same project, answered by a dormant composition. No gate is
    // on, so there is no advisory lane at all -- an absent lane, never an
    // empty one.
    let dormant = ProductionProjectCompositionHarnessV1::open(isolation.path(), [project.clone()])
        .await
        .expect("dormant production composition");
    let (baseline, dormant_lane) = context_advisory_lane(&dormant, &project, &task, None).await;
    assert!(
        dormant_lane.is_none(),
        "a dormant composition must contribute no advisory lane at all: {baseline}"
    );
    let baseline_shapes = host_section_shapes(&baseline);
    assert!(
        baseline_shapes.iter().any(|(_, _, width)| *width > 0),
        "the host answer must carry populated sections to compare against: {baseline}"
    );
    enable_memory_provider_host(&dormant, &project).await;
    dormant.shutdown().await;
    commit_claude_session_source_edit(&project);

    // The same task, put to a composition with the provider host mounted --
    // and with the caller's own provider behind the real Native adapter. The
    // configuration gates, the routing policy, the registration revision and
    // `NativeProvider::new`'s descriptor validation are all untouched: the only
    // substituted thing is the value of the application port this composition
    // root already injects.
    let recall_unavailable = Arc::new(AtomicBool::new(false));
    let harness = {
        let recall_unavailable = Arc::clone(&recall_unavailable);
        ProductionProjectCompositionHarnessV1::open_with_native_application_port_interposition(
            isolation.path(),
            [project.clone()],
            Arc::new(move |production_port| {
                Arc::new(AdversarialRecallPortV1 {
                    inner: production_port,
                    recall_unavailable: Arc::clone(&recall_unavailable),
                }) as Arc<dyn NativeMemoryApplicationPort>
            }),
        )
        .await
        .expect("production composition with the memory provider host mounted")
    };
    // The ordinary agent call: no structural session identity in `_meta`, which
    // is what every real `tracedecay_context` call from an agent looks like.
    // The lane binds it to the MCP connection the request identity was minted
    // on, so a mounted healthy route answers -- and the whole host answer must
    // still be there beside it.
    let (unbound_answer, unbound_lane) =
        context_advisory_lane(&harness, &project, &task, None).await;
    let unbound_lane = unbound_lane.unwrap_or_else(|| {
        panic!(
            "a mounted route must report its refusal as a lane, never by disappearing: \
             {unbound_answer}"
        )
    });
    assert_eq!(
        unbound_lane["state"], "answered",
        "the MCP connection this call was minted on is itself a session identity, so the \
         mounted route binds and answers: {unbound_lane}"
    );
    assert_eq!(
        unbound_lane["provider_id"],
        tracedecay_memory_provider_registry::NATIVE_PROVIDER_ID,
        "the lane must name the provider its routing policy pinned: {unbound_lane}"
    );
    assert_eq!(
        unbound_lane["registration_revision"], 1,
        "the lane must carry the registration revision the composition mounted: {unbound_lane}"
    );
    assert_eq!(
        unbound_lane["degradation"],
        Value::Null,
        "a healthy route reports no degradation; a degraded one must say which: {unbound_lane}"
    );
    // The dormant control proves the lane is absent when disabled. Exact
    // host-section preservation is compared below between two answers from
    // this same mounted composition and source revision; the committed edit
    // intentionally makes the dormant control a different host snapshot.
    assert!(
        !host_section_shapes(&unbound_answer).is_empty(),
        "the mounted host answer must retain host-owned sections: {unbound_answer}"
    );

    // Now the routed provider fails, and nothing else changes: the same
    // composition, the same mount, the same registration revision, the same
    // checkout, the same task, no restart and no source edit in between. The
    // provider answers recall with `TerminalCode::ProviderUnavailable`, which
    // the fabric carries as the reply's terminal, the Native adapter passes
    // through, and the recall port classifies as
    // `CognitiveRecallDegradation::Unavailable`.
    recall_unavailable.store(true, Ordering::SeqCst);

    let (degraded_answer, degraded_lane) =
        context_advisory_lane(&harness, &project, &task, None).await;
    let degraded_lane = degraded_lane.unwrap_or_else(|| {
        panic!(
            "a broken provider must degrade *inside* the lane, never by removing it: \
             {degraded_answer}"
        )
    });
    // The provider was genuinely contacted: a lane that never reached it could
    // not carry the provider's own identity and the revision it was
    // registered under.
    assert_eq!(
        degraded_lane["provider_id"],
        tracedecay_memory_provider_registry::NATIVE_PROVIDER_ID,
        "the degraded lane must still name the provider that failed: {degraded_lane}"
    );
    assert_eq!(
        degraded_lane["registration_revision"], 1,
        "the degraded lane must carry the registration revision the call was routed under: \
         {degraded_lane}"
    );
    assert_eq!(
        degraded_lane["state"], "answered",
        "a provider failure is a typed degradation of the lane, not an untyped refusal: \
         {degraded_lane}"
    );
    assert_eq!(
        degraded_lane["degradation"], "unavailable",
        "a provider that answers recall with `ProviderUnavailable` must surface as the typed \
         `unavailable` degradation, never as a silently empty healthy lane: {degraded_lane}"
    );
    assert_eq!(
        degraded_lane["candidates"].as_array().map(Vec::len),
        Some(0),
        "a failed recall must contribute no candidates: {degraded_lane}"
    );
    // The whole point, stated exactly. Same composition, same source revision:
    // every host section must be identical in type *and* width between the
    // healthy answer and the degraded one. A lane that hollowed out, retyped,
    // shortened or dropped any host section on the provider's failure is
    // caught here, and so is one that quietly added a section.
    assert_eq!(
        host_section_shapes(&degraded_answer),
        host_section_shapes(&unbound_answer),
        "a provider failure must cost the host answer nothing at all: healthy={unbound_answer} \
         degraded={degraded_answer}"
    );

    harness.shutdown().await;
}

/// A caller-owned Native application port that delegates every operation to
/// the production port the composition built, until it is told to fail recall.
///
/// This is the provider boundary the fail-open contract exists for. Nothing
/// about the mount is weakened to admit it: it declares the production
/// descriptor, so `NativeProvider::new` validates exactly what it validates
/// for the real port, the fabric registers it under the same revision, and the
/// recall route reaches it through the same policy. What it can do that the
/// real port cannot is fail *on demand*, at one operation, without touching
/// the provider's own durable state -- which is what makes the healthy and
/// degraded answers comparable for exact equality.
struct AdversarialRecallPortV1 {
    inner: Arc<dyn NativeMemoryApplicationPort>,
    recall_unavailable: Arc<AtomicBool>,
}

impl NativeMemoryApplicationPort for AdversarialRecallPortV1 {
    fn descriptor(&self) -> ProviderDescriptor {
        self.inner.descriptor()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        self.inner.handshake(request)
    }

    fn health(&self, call: &ProviderCall) -> ProviderReply {
        self.inner.health(call)
    }

    fn observe(&self, observation: NativeObservation<'_>) -> ProviderReply {
        self.inner.observe(observation)
    }

    fn recall(&self, call: &ProviderCall) -> ProviderReply {
        if !self.recall_unavailable.load(Ordering::SeqCst) {
            return self.inner.recall(call);
        }
        // A provider-owned terminal, built the way any provider builds one it
        // could not dispatch: no payload, no effect, no fallback authority.
        ProviderReply {
            terminal: TerminalRecord::failure_before_dispatch_for_call(
                TerminalCode::ProviderUnavailable,
                call,
                "journey.adversarial_recall_unavailable",
            ),
            payload: None,
            warnings: Vec::new(),
            extensions: Vec::new(),
            state_generation: call.expected_state_generation,
        }
    }

    fn feedback(&self, call: &ProviderCall) -> ProviderReply {
        self.inner.feedback(call)
    }

    fn maintenance(&self, call: &ProviderCall) -> ProviderReply {
        self.inner.maintenance(call)
    }

    fn inspection(&self, call: &ProviderCall) -> ProviderReply {
        self.inner.inspection(call)
    }

    fn correction(&self, call: &ProviderCall) -> ProviderReply {
        self.inner.correction(call)
    }

    fn delete_by_source(&self, call: &ProviderCall) -> ProviderReply {
        self.inner.delete_by_source(call)
    }

    fn snapshot_export(&self, call: &ProviderCall) -> ProviderReply {
        self.inner.snapshot_export(call)
    }

    fn snapshot_restore(&self, call: &ProviderCall) -> ProviderReply {
        self.inner.snapshot_restore(call)
    }

    fn replay(&self, call: &ProviderCall) -> ProviderReply {
        self.inner.replay(call)
    }
}

// ---------------------------------------------------------------------------
// The shipped Claude Code host bundle: the hooks this journey rides are the
// ones `tracedecay install --agent claude` deploys, and deploying them again,
// updating them, or undoing them must never disturb operator-owned state.
// ---------------------------------------------------------------------------

/// The operator's own Claude settings, which TraceDecay's staging lane has no
/// business touching.
const OPERATOR_CLAUDE_SETTINGS: &str =
    "{\n  \"model\": \"opus\",\n  \"permissions\": {\n    \"allow\": [\"Bash(ls:*)\"]\n  }\n}\n";

/// The operator's own project rules, which install must append to and undo
/// must give back.
const OPERATOR_PROJECT_RULES: &str = "# Team rules\n\nAlways run the linter before pushing.\n";

/// The heading of the TraceDecay-managed block inside a project `CLAUDE.md`.
const MANAGED_RULES_MARKER: &str = "## MANDATORY: No Explore Agents When Tracedecay Is Available";

/// Every file the deployed bundle holds, as `(relative path, bytes)`, sorted.
fn deployed_bundle(deploy_dir: &Path) -> Vec<(String, Vec<u8>)> {
    fn walk(base: &Path, directory: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        let Ok(entries) = std::fs::read_dir(directory) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(base, &path, out);
            } else {
                let relative = path
                    .strip_prefix(base)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .into_owned();
                out.push((relative, std::fs::read(&path).unwrap_or_default()));
            }
        }
    }
    let mut files = Vec::new();
    walk(deploy_dir, deploy_dir, &mut files);
    files.sort();
    files
}

/// The command and arguments the deployed hook manifest binds one Claude hook
/// event to, or `None` when the event is not declared at all.
fn deployed_hook_invocation(hooks: &Value, event: &str) -> Option<(String, Vec<String>)> {
    let handler = hooks
        .pointer(&format!("/hooks/{event}"))?
        .as_array()?
        .iter()
        .find_map(|entry| entry.pointer("/hooks/0"))?;
    let command = handler.get("command")?.as_str()?.to_owned();
    let arguments = handler
        .get("args")?
        .as_array()?
        .iter()
        .filter_map(|argument| argument.as_str().map(str::to_owned))
        .collect();
    Some((command, arguments))
}

/// Bundle *staging* and project-rules registration for the shipped Claude
/// Code host: staging, re-staging, and update converge on a byte-identical
/// deployed bundle, registration appends exactly one managed block to the
/// operator's own rules, and undo gives those rules back -- with the
/// operator's `settings.json` untouched throughout.
///
/// Scope, stated plainly: Claude Code owns its own activation, so this test
/// stops at the deferral boundary. It never calls
/// `activate_deployed_host_registration` or
/// `deactivate_deployed_host_registration`, and it therefore proves nothing
/// about the marketplace/plugin entries, the TraceDecay permission entry, or
/// rollback. Those are proved against a real `claude` executable by
/// `crates/tracedecay-cli/tests/host_lifecycle_cli_acceptance.rs`
/// (`claude_lifecycle_tracks_assets_only_after_native_activation`), which
/// drives install, repeated install, update, injected-failure rollback,
/// recovery, and idempotent uninstall against a real `claude` executable.
///
/// Real defect this catches: a memory integration that bolts its own hook or
/// settings entry onto the Claude host, so a second install or an undo leaves
/// the operator's `settings.json` or project rules changed behind their back.
/// The assertion that the memory journey's hooks are exactly the shipped
/// lifecycle hooks is what keeps that from being added later without notice.
#[test]
fn the_shipped_claude_bundle_stages_hooks_and_registers_project_rules_without_disturbing_operator_state()
 {
    use tracedecay_agent_hosts::agents::host_bundle_v2::HostBundleComponentV1;
    use tracedecay_agent_hosts::agents::{
        AgentIntegration, ClaudeIntegration, InstallContext, NonInteractiveInstallOutcome,
        UpdatePluginOutcome,
    };

    let home_dir = TempDir::new().expect("claude home");
    let project_dir = TempDir::new().expect("claude project");
    // Canonicalize: the host lifecycle refuses a project path that does not
    // resolve to itself, and a temp dir is a symlink on macOS.
    let home = std::fs::canonicalize(home_dir.path()).expect("canonical home");
    let project = std::fs::canonicalize(project_dir.path()).expect("canonical project");

    let settings = home.join(".claude/settings.json");
    std::fs::create_dir_all(settings.parent().expect("settings parent")).expect("claude dir");
    std::fs::write(&settings, OPERATOR_CLAUDE_SETTINGS).expect("operator settings");
    let project_rules = project.join(".claude/CLAUDE.md");
    std::fs::create_dir_all(project_rules.parent().expect("rules parent")).expect("project dir");
    std::fs::write(&project_rules, OPERATOR_PROJECT_RULES).expect("operator rules");

    let install = InstallContext {
        home: home.clone(),
        tracedecay_bin: "/opt/tracedecay/bin/tracedecay".to_owned(),
        tool_permissions: Vec::new(),
        project_root: None,
        dashboard: false,
    };
    let integration = ClaudeIntegration;
    let components: &[HostBundleComponentV1] = &[];

    // Install stages the bundle that carries the Claude lifecycle hooks.
    let NonInteractiveInstallOutcome::DeferredUserAction(staged) = integration
        .prepare_non_interactive_install(&install)
        .expect("staging the Claude bundle")
    else {
        panic!("Claude Code owns its own activation, so a fresh install defers to its CLI");
    };
    let deploy_dir = staged
        .staged_paths
        .first()
        .cloned()
        .expect("the deferral must name the staged bundle root");
    let installed = deployed_bundle(&deploy_dir);
    assert!(
        !installed.is_empty(),
        "staging must deploy the bundle to {}",
        deploy_dir.display()
    );

    // The hooks this memory journey rides are the shipped lifecycle hooks,
    // bound to the pinned binary -- and there is no memory-specific hook
    // beside them.
    let hooks: Value = serde_json::from_slice(
        &std::fs::read(deploy_dir.join("hooks/hooks.json")).expect("deployed hook manifest"),
    )
    .expect("the deployed hook manifest must be JSON");
    for (event, argument) in [
        ("SessionStart", "hook-claude-session-start"),
        ("Stop", "hook-stop"),
        ("PostToolUse", "hook-claude-post-tool-use"),
    ] {
        let (command, arguments) = deployed_hook_invocation(&hooks, event)
            .unwrap_or_else(|| panic!("the bundle must declare the {event} hook: {hooks}"));
        assert_eq!(
            command, install.tracedecay_bin,
            "the {event} hook must run the pinned binary: {hooks}"
        );
        assert_eq!(
            arguments,
            vec![argument.to_owned()],
            "the {event} hook must invoke the shipped handler: {hooks}"
        );
    }
    let declared_events: Vec<String> = hooks["hooks"]
        .as_object()
        .expect("hook manifest events")
        .keys()
        .cloned()
        .collect();
    assert!(
        declared_events
            .iter()
            .all(|event| !event.to_ascii_lowercase().contains("memory")),
        "the memory journey must ride the shipped lifecycle hooks, not its own: {declared_events:?}"
    );

    // Re-installing converges: not one byte of the deployed bundle changes.
    integration
        .prepare_non_interactive_install(&install)
        .expect("re-staging the Claude bundle");
    assert_eq!(
        deployed_bundle(&deploy_dir),
        installed,
        "re-installing must leave the deployed bundle byte-identical"
    );

    // So does an update against the same version.
    let UpdatePluginOutcome::DeferredUserAction(_) = integration
        .update_plugin(&install)
        .expect("updating the Claude bundle")
    else {
        panic!("Claude Code owns its own cache, so an update defers activation to its CLI");
    };
    assert_eq!(
        deployed_bundle(&deploy_dir),
        installed,
        "an update against the same version must leave the deployed bundle byte-identical"
    );
    assert_eq!(
        std::fs::read_to_string(&settings).expect("settings after staging"),
        OPERATOR_CLAUDE_SETTINGS,
        "staging the bundle must not touch the operator's own Claude settings"
    );

    // Project registration appends the managed block to operator rules, and
    // registering again converges instead of appending a second copy.
    integration
        .activate_project_host_component_registration(components, &install, &project)
        .expect("registering the project host component");
    let registered = std::fs::read_to_string(&project_rules).expect("registered rules");
    assert!(
        registered.starts_with(OPERATOR_PROJECT_RULES),
        "registration must append to the operator's own rules: {registered}"
    );
    assert_eq!(
        registered.matches(MANAGED_RULES_MARKER).count(),
        1,
        "registration must write exactly one managed block: {registered}"
    );
    integration
        .activate_project_host_component_registration(components, &install, &project)
        .expect("re-registering the project host component");
    assert_eq!(
        std::fs::read_to_string(&project_rules).expect("re-registered rules"),
        registered,
        "re-registering must converge on the same file"
    );

    // Undo gives the operator their own rules back, and is itself idempotent.
    integration
        .deactivate_project_host_component_registration(components, &install, &project)
        .expect("deregistering the project host component");
    let undone = std::fs::read_to_string(&project_rules).expect("rules after undo");
    assert!(
        !undone.contains(MANAGED_RULES_MARKER),
        "undo must remove the managed block: {undone}"
    );
    assert!(
        undone.contains(OPERATOR_PROJECT_RULES.trim()),
        "undo must preserve the operator's own rules verbatim: {undone}"
    );
    integration
        .deactivate_project_host_component_registration(components, &install, &project)
        .expect("deregistering twice");
    assert_eq!(
        std::fs::read_to_string(&project_rules).expect("rules after second undo"),
        undone,
        "a second undo must change nothing"
    );
    assert_eq!(
        std::fs::read_to_string(&settings).expect("settings after undo"),
        OPERATOR_CLAUDE_SETTINGS,
        "undo must not touch the operator's own Claude settings"
    );
}
