//! Production MCP coverage for the mounted cognitive-recall advisory lane.
//!
//! The recall owner lives in `tracedecay-daemon-service`, while the MCP server
//! lives in this crate. Keeping this journey at the root boundary preserves
//! the real request path without making the owner depend on the transport
//! crate again.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use serde_json::{Value, json};
use tempfile::TempDir;
use tracedecay_domain::{Confidence, FactCategoryV1, ProjectId};
use tracedecay_memory_provider_registry::NATIVE_PROVIDER_ID;
use tracedecay_session_memory::memory::{
    ProjectMemoryFactAddRequest, ProjectMemoryFactAddRequestOutcome,
};
use tracedecay_store::FactWriteControl;

use super::McpServer;
use crate::config::PinnedUserDataDir;
use crate::project::{TraceDecay, TraceDecayOpenOptions};
use tracedecay_daemon_service::retained_owner::test_context_evidence::native_cognitive_recall_mount_for_test;
use tracedecay_mcp::JsonRpcRequest;

const ADVISORY_RECALL_CONTEXT_TOOL: &str = "tracedecay_context";
const SEEDED_CONTENT: &str = "cognitive recall ledger durable retrieval";

/// One registered project server driven through the production MCP request
/// path, with all fixture authorities retained for the server lifetime.
struct McpJourneyFixture {
    _temporary: TempDir,
    _pin: PinnedUserDataDir,
    _provider_data: TempDir,
    server: Arc<McpServer>,
}

impl McpJourneyFixture {
    /// Issues one ordinary `tools/call` and returns the exact text delivered
    /// to the agent.
    async fn call_context(&self, task: &str) -> String {
        let mut connection = self
            .server
            .new_connection_route_state()
            .expect("connection route state");
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_owned(),
            id: Some(json!(1)),
            method: "tools/call".to_owned(),
            params: Some(json!({
                "name": ADVISORY_RECALL_CONTEXT_TOOL,
                "arguments": { "task": task },
            })),
        };
        let response = self
            .server
            .handle_request_for_connection(&request, false, &mut connection, false)
            .await
            .expect("the context request receives a response");
        assert!(
            response.error.is_none(),
            "the canonical context call must succeed: {:?}",
            response.error
        );
        response
            .result
            .as_ref()
            .and_then(|result| result.pointer("/content/0/text"))
            .and_then(Value::as_str)
            .expect("the context tool answers with rendered text")
            .to_owned()
    }
}

async fn mcp_journey_fixture(project: &str) -> McpJourneyFixture {
    mcp_journey_fixture_inner(project, true).await
}

async fn mcp_journey_fixture_unmounted(project: &str) -> McpJourneyFixture {
    mcp_journey_fixture_inner(project, false).await
}

/// Builds the same registered project context used by the daemon test
/// harness, then injects the service-owned Native mount when requested.
async fn mcp_journey_fixture_inner(project: &str, mounted: bool) -> McpJourneyFixture {
    let pin = PinnedUserDataDir::new();
    let temporary = TempDir::new().expect("journey fixture root");
    let project_root = temporary.path();
    super::writer_test_support::git(project_root, &["init", "-q", "-b", "main"]);
    super::writer_test_support::git(
        project_root,
        &["config", "user.email", "cognitive-recall@test.invalid"],
    );
    super::writer_test_support::git(project_root, &["config", "user.name", "Cognitive Recall"]);
    std::fs::write(project_root.join(".gitignore"), ".tracedecay/\n").expect("write gitignore");
    std::fs::create_dir_all(project_root.join("src")).expect("create project source");
    std::fs::write(project_root.join("src/a.rs"), "pub fn a() {}\n").expect("write project source");
    super::writer_test_support::git(project_root, &["add", "."]);
    super::writer_test_support::git(project_root, &["commit", "-q", "-m", "initial"]);

    let project_id = ProjectId::new(project).expect("project identity");
    let (cg, runtime) =
        TraceDecay::init_test_fixture_with_registered_runtime(project_root, project_id.as_str())
            .await
            .expect("registered project fixture");
    let runtime = Arc::new(runtime);

    // The mount and the server intentionally use separate graph handles, as
    // production does: both still resolve the same registered project owner.
    let mount_graph = Arc::new(
        runtime
            .open_project_graph_for_test(
                project_root,
                TraceDecayOpenOptions {
                    profile_root: Some(runtime.profile_root_for_test().to_path_buf()),
                    global_db_path: None,
                },
            )
            .await
            .expect("reopen registered project graph"),
    );
    seed_fixture(&mount_graph).await;

    let profile_id = tracedecay_daemon_identity::profile_identity::load_or_create(
        runtime.profile_root_for_test(),
    )
    .expect("fixture profile identity")
    .profile_id()
    .clone();
    let scope =
        tracedecay_code_index_runtime::resolved_scope_for_project(project_root, &project_id)
            .expect("resolve fixture scope");
    let provider_data = TempDir::new().expect("provider state root");
    let mount = mounted.then(|| {
        native_cognitive_recall_mount_for_test(
            mount_graph,
            profile_id,
            scope,
            provider_data.path().to_path_buf(),
        )
        .expect("mount Native cognitive recall route")
    });

    let mut context =
        crate::test_support::host_admission::mcp_server_context_for_test(runtime, cg, None)
            .expect("registered MCP server context");
    context.startup_catch_up_enabled = false;
    if let Some(mount) = mount {
        context = context.with_cognitive_recall_mount(mount);
    }
    let server = McpServer::new_with_registered_test_context(context, Vec::new())
        .await
        .expect("registered project server");

    McpJourneyFixture {
        _temporary: temporary,
        _pin: pin,
        _provider_data: provider_data,
        server,
    }
}

async fn seed_fixture(graph: &TraceDecay) {
    let memory = graph
        .project_memory_application()
        .expect("project memory application");
    let preflight = memory
        .preflight_project_memory_fact_add(
            ProjectMemoryFactAddRequest {
                content: SEEDED_CONTENT.to_owned(),
                category: FactCategoryV1::Project,
                source_label: Some("cognitive-recall-seed".to_owned()),
                tags: vec!["cognitive".to_owned(), "recall".to_owned()],
                entities: vec!["TraceDecay".to_owned()],
                trust: Some(Confidence::new(0.91).expect("fact trust")),
                metadata: json!({"fixture": "cognitive-recall"}),
            },
            None,
        )
        .expect("preflight seeded fact");
    let outcome = memory
        .add_preflighted_project_memory_fact(
            preflight,
            &FactWriteControl::new(Arc::new(|| false), Arc::new(|| true)),
        )
        .await
        .expect("commit seeded fact");
    assert!(matches!(
        outcome,
        ProjectMemoryFactAddRequestOutcome::Applied(_)
    ));
}

/// The production MCP journey: an ordinary `tools/call` for
/// `tracedecay_context` carries the routed provider's advisory lane.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ordinary_mcp_context_call_returns_the_routed_advisory_lane() {
    let journey = mcp_journey_fixture("project.advisory-journey").await;

    let answered = journey.call_context("cognitive recall ledger").await;
    assert!(
        answered.contains(&format!("Provider {NATIVE_PROVIDER_ID}")),
        "an ordinary MCP context call must carry the routed provider's lane: {answered}"
    );
    assert!(
        answered.contains(SEEDED_CONTENT),
        "the routed provider's admitted candidate must reach the agent: {answered}"
    );
}

/// The differential journey: the same MCP call on a server without a mounted
/// route renders no provider advisory lane.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_mcp_context_call_on_an_unmounted_server_renders_no_advisory_lane() {
    let journey = mcp_journey_fixture_unmounted("project.advisory-journey-dormant").await;

    let unmounted = journey.call_context("cognitive recall ledger").await;
    assert!(
        !unmounted.contains("Provider memory (advisory)"),
        "a server with no mounted route must render no advisory lane: {unmounted}"
    );
    assert!(
        !unmounted.contains(NATIVE_PROVIDER_ID),
        "a server with no mounted route must name no provider: {unmounted}"
    );
}
