#![cfg(all(feature = "test-transport", feature = "memory-provider-host"))]

//! Root-boundary production Claude host memory journey.
//!
//! The production composition harness belongs to the root crate because it
//! assembles the daemon project server. Keeping this journey at that boundary
//! preserves the shipped transcript import, Native observation settlement,
//! Claude hook route publication, and later context recall without making the
//! daemon-service crate depend upward on its composition root.
//!
//! Hook causality remains covered by
//! crates/tracedecay-cli/tests/product_memory_provider_claude_host_journey.rs,
//! which invokes the shipped Claude lifecycle binaries with real host payloads.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::TempDir;
use tracedecay::daemon::ProductionProjectCompositionHarnessV1;
use tracedecay_domain::configuration::{
    ConfigurationIdempotencyKey, ConfigurationLayerIdV1, ConfigurationRevisionId,
    ConfigurationValueV1, MEMORY_PROVIDER_NATIVE_ENABLED_SETTING_KEY,
    MEMORY_PROVIDER_RECALL_ROUTING_SETTING_KEY, SettingKey,
};
use tracedecay_domain::{ProjectId, UserProfileId};
use tracedecay_memory_observation::{
    DeliveryStateV1, JournalInspectionFilterV1, JournalInspectionRowV1, ObservationJournalReaderV1,
    RetentionPolicyV1, SqliteObservationJournal,
};

const JOURNAL_FILE_NAME: &str = "memory-observation-journal-v1.sqlite3";
const SESSION_MESSAGE_OBSERVATION_KIND: &str = "session.message_committed.v1";
const ACKNOWLEDGED_DELIVERY_STATE: &str = DeliveryStateV1::Acknowledged.as_wire();
const ADVISORY_RECALL_MAXIMUM_CANDIDATES: usize = 5;

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
        .revision_id()
        .clone()
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
    let request = tracedecay_contracts::ConfigurationSetRequestV1 {
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct JourneyJournalRowV1 {
    observation_id: String,
    idempotency_key: String,
    payload_sha256: String,
    exact_scope_sha256: String,
    observation_kind: String,
    source_sequence: u64,
    attempt_number: u32,
    content_present: bool,
    state: &'static str,
    terminal: bool,
}

impl JourneyJournalRowV1 {
    fn of(row: &JournalInspectionRowV1) -> Self {
        Self {
            observation_id: row.observation_id.as_str().to_owned(),
            idempotency_key: row.idempotency_key.as_str().to_owned(),
            payload_sha256: row.payload_sha256.clone(),
            exact_scope_sha256: row.exact_scope_sha256.clone(),
            observation_kind: row.observation_kind.clone(),
            source_sequence: row.source_sequence.0,
            attempt_number: row.attempt_number,
            content_present: row.content_present,
            state: row.state.as_wire(),
            terminal: row.state.is_terminal(),
        }
    }
}

struct JourneyJournalV1(SqliteObservationJournal);

fn inspection_retention_policy() -> RetentionPolicyV1 {
    RetentionPolicyV1 {
        ephemeral_max_age_micros: 3_600_000_000,
        session_max_age_micros: 86_400_000_000,
        project_max_age_micros: 2_592_000_000_000,
        profile_max_age_micros: 2_592_000_000_000,
        receipt_retention_micros: 604_800_000_000,
        max_queue_items: 10_000,
        max_queue_bytes: 64 * 1_048_576,
        max_attempts: 8,
        backoff_base_micros: 1_000_000,
        backoff_max_micros: 300_000_000,
        sweep_batch_rows: 512,
    }
}

fn open_journal(journal_path: &Path) -> JourneyJournalV1 {
    JourneyJournalV1(
        SqliteObservationJournal::open(journal_path, inspection_retention_policy())
            .expect("the durable observation journal must open through its own store API"),
    )
}

impl JourneyJournalV1 {
    fn rows(&self) -> Vec<JourneyJournalRowV1> {
        let page = self
            .0
            .inspect(&JournalInspectionFilterV1 {
                limit: 100,
                ..JournalInspectionFilterV1::default()
            })
            .expect("the durable observation journal must answer an inspection");
        assert!(
            page.next_cursor.is_none(),
            "this Claude journey must fit in one bounded journal inspection page"
        );
        page.rows.iter().map(JourneyJournalRowV1::of).collect()
    }

    async fn await_settlement(&self, minimum_rows: usize) -> Vec<JourneyJournalRowV1> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let rows = self.rows();
            if rows.len() >= minimum_rows && rows.iter().all(|row| row.terminal) {
                return rows;
            }
            assert!(
                Instant::now() < deadline,
                "the durable observation journal never settled {minimum_rows} deliveries within 30s: {:?}",
                journal_digest(&rows)
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

fn journal_row_identities(rows: &[JourneyJournalRowV1]) -> Vec<(String, String, String, u32)> {
    let mut identities = rows
        .iter()
        .map(|row| {
            (
                row.idempotency_key.clone(),
                row.observation_id.clone(),
                row.payload_sha256.clone(),
                row.attempt_number,
            )
        })
        .collect::<Vec<_>>();
    identities.sort();
    identities
}

fn journal_digest(rows: &[JourneyJournalRowV1]) -> Vec<String> {
    let mut digest = rows
        .iter()
        .map(|row| {
            format!(
                "{}|{}|{}|attempt={}|seq={}|content_present={}",
                row.observation_kind,
                row.exact_scope_sha256,
                row.state,
                row.attempt_number,
                row.source_sequence,
                row.content_present,
            )
        })
        .collect::<Vec<_>>();
    digest.sort();
    digest
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
    let (_project_id, _profile_id) = project_identity(&harness, &project).await;
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
    let mut exact_scopes = rows
        .iter()
        .map(|row| row.exact_scope_sha256.clone())
        .collect::<Vec<_>>();
    exact_scopes.sort();
    exact_scopes.dedup();
    assert_eq!(
        exact_scopes.len(),
        1,
        "all deliveries from one Claude session must share one exact scope: {:?}",
        journal_digest(&rows)
    );
    for row in &rows {
        assert_eq!(
            row.observation_kind, SESSION_MESSAGE_OBSERVATION_KIND,
            "the journey admits exactly the session-message observation kind"
        );
        assert!(
            !row.exact_scope_sha256.is_empty(),
            "every journal row must carry a non-empty exact worktree-bound scope"
        );
        assert_eq!(
            row.exact_scope_sha256, exact_scopes[0],
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
        candidates.len() <= ADVISORY_RECALL_MAXIMUM_CANDIDATES,
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
