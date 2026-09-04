use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use serde_json::{Value, json};
use tempfile::TempDir;
use tracedecay_domain::errors::TraceDecayError;
use tracedecay_hooks::core_events::{HookAgent, HookRouteMetadata};
use tracedecay_mcp::hook_events::{HookEvent, HookEventKind};

use super::*;
use crate::daemon::projectless::projectless_registered_project_reader_server;
use crate::mcp::project_route::{
    ProjectRouteFailure, ProjectRouteFailureKind, WorkspaceProjectRoute,
};
use crate::mcp::server::McpServer;

fn tool_request(tool_name: &str, arguments: Value) -> String {
    serde_json::to_string(&json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": tool_name,
            "arguments": arguments,
        },
    }))
    .expect("serialize projectless tool request")
}

fn hook_event(session_id: &str, thread_id: Option<&str>, cwd: PathBuf) -> HookEvent {
    HookEvent {
        agent: HookAgent::Claude,
        kind: HookEventKind::FileEdit,
        rel_paths: Vec::new(),
        had_command: false,
        cwd: Some(cwd.clone()),
        route: Some(HookRouteMetadata {
            session_id: Some(session_id.to_owned()),
            thread_id: thread_id.map(str::to_owned),
            cwd: Some(cwd),
            worktree: None,
            branch: None,
        }),
        receipt: None,
    }
}

fn expect_route_error(
    result: tracedecay_domain::errors::Result<Option<Arc<McpServer>>>,
    message: &str,
) -> TraceDecayError {
    match result {
        Err(error) => error,
        Ok(_) => panic!("{message}"),
    }
}

fn assert_route_error(error: &TraceDecayError, reason_code: &str, retryable: bool) {
    let context = error
        .project_route_context()
        .unwrap_or_else(|| panic!("expected project-route error, got {error}"));
    assert_eq!(context.0, reason_code);
    assert_eq!(context.1, retryable);
}

struct RoutedProjectFixture {
    _isolation: TempDir,
    harness: crate::daemon::ProductionProjectCompositionHarnessV1,
    server: Arc<McpServer>,
    event: HookEvent,
}

async fn routed_project_fixture(session_id: &str) -> RoutedProjectFixture {
    let isolation = TempDir::new().expect("projectless route isolation");
    let project = isolation.path().join("projectless-route-project");
    fs::create_dir_all(project.join("src")).expect("route source directory");
    fs::write(
        project.join("Cargo.toml"),
        "[package]\nname = \"projectless_route_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .expect("route manifest");
    fs::write(project.join("src/lib.rs"), "pub fn route_marker() {}\n").expect("route source");
    for args in [
        &["init", "--quiet"][..],
        &["add", "."][..],
        &[
            "-c",
            "user.name=TraceDecay Tests",
            "-c",
            "user.email=tests@tracedecay.invalid",
            "commit",
            "--quiet",
            "-m",
            "fixture",
        ][..],
    ] {
        assert!(
            Command::new(
                tracedecay_runtime_core::git::try_git_program()
                    .expect("absolute git executable should resolve"),
            )
            .args(args)
            .current_dir(&project)
            .status()
            .expect("git route fixture")
            .success()
        );
    }
    let harness = crate::daemon::ProductionProjectCompositionHarnessV1::open(
        isolation.path(),
        [project.clone()],
    )
    .await
    .expect("route composition");
    let server = harness.server(&project).expect("registered route server");
    let route_root = server.cg().await.project_root().to_path_buf();
    let event = hook_event(session_id, Some("thread.projectless-route"), route_root);
    RoutedProjectFixture {
        _isolation: isolation,
        harness,
        server,
        event,
    }
}

fn test_profile() -> (TempDir, StoreAdministration, DaemonClientIdentity) {
    let temp = TempDir::new().expect("projectless profile isolation");
    let profile_root = temp.path().join("profile");
    let administration = test_store_administration_for_profile(&profile_root);
    // The daemon rewrites every handshake identity through
    // `canonical_identity_path` before admission, so this fixture identity
    // carries the same canonical root the profile authority resolves (macOS
    // temp roots alias `/var` to `/private/var`).
    let profile_root =
        tracedecay_daemon_identity::authority::canonical_identity_path(&profile_root)
            .expect("canonical test profile root");
    let identity = test_client_identity_for(profile_root);
    (temp, administration, identity)
}

#[test]
fn projectless_preselection_requires_reader_and_explicit_matching_identity() {
    let (_profile, administration, identity) = test_profile();

    for request in [
        "not-json".to_owned(),
        tool_request("tracedecay_grep", json!({"pattern": "marker"})),
        tool_request(
            "tracedecay_status",
            json!({"_meta": {"session_id": "session.unrelated"}}),
        ),
        tool_request(
            "tracedecay_lcm_status",
            json!({
                "storage_scope": "user",
                "_meta": {"session_id": "session.user"}
            }),
        ),
    ] {
        assert!(
            projectless_registered_project_reader_server(&request, &identity, &administration)
                .expect("non-routed projectless request")
                .is_none(),
            "projectless preselection widened request: {request}"
        );
    }

    let error = expect_route_error(
        projectless_registered_project_reader_server(
            &tool_request(
                "tracedecay_grep",
                json!({
                    "pattern": "marker",
                    "_meta": {"session_id": "session.unknown"}
                }),
            ),
            &identity,
            &administration,
        ),
        "unknown explicit identity must fail closed",
    );
    assert_route_error(&error, "project_route_not_found", false);
}

#[test]
fn projectless_preselection_refuses_foreign_profile_before_route_lookup() {
    let (_profile, administration, _identity) = test_profile();
    let foreign = TempDir::new().expect("foreign profile isolation");
    let foreign_identity = test_client_identity_for(foreign.path().join("profile"));

    let error = expect_route_error(
        projectless_registered_project_reader_server(
            &tool_request(
                "tracedecay_grep",
                json!({
                    "pattern": "marker",
                    "_meta": {"session_id": "session.unknown"}
                }),
            ),
            &foreign_identity,
            &administration,
        ),
        "foreign profile must be refused",
    );
    assert!(
        error.project_route_context().is_none()
            && error
                .to_string()
                .contains("does not match its authenticated identity"),
        "unexpected admission error: {error}"
    );
}

#[tokio::test]
async fn projectless_preselection_uses_exact_route_then_rejects_dead_server() {
    let session_id = ["AKIA", "PROJECTLESS", "ROUTE", "5"].concat();
    let (_profile, administration, identity) = test_profile();
    let fixture = routed_project_fixture(&session_id).await;
    let routes = administration.project_routes();
    let mut snapshot = routes.snapshot().expect("project route snapshot");
    fixture
        .server
        .update_hook_workspace_route(&fixture.event, &mut snapshot)
        .await
        .expect("resolve projectless route");
    routes.store(&snapshot).expect("publish projectless route");

    let request = tool_request(
        "tracedecay_grep",
        json!({
            "pattern": "route_marker",
            "_meta": {"session_id": session_id}
        }),
    );
    let selected =
        projectless_registered_project_reader_server(&request, &identity, &administration)
            .expect("select exact projectless route")
            .expect("registered reader must select a server");
    assert!(Arc::ptr_eq(&selected, &fixture.server));
    drop(selected);

    let RoutedProjectFixture {
        _isolation,
        harness,
        server,
        event: _,
    } = fixture;
    drop(server);
    harness.shutdown().await;

    let error = expect_route_error(
        projectless_registered_project_reader_server(&request, &identity, &administration),
        "dead weak route must fail closed",
    );
    assert_route_error(&error, "project_route_unavailable", true);
}

#[test]
fn projectless_preselection_propagates_cached_route_failure() {
    let (_profile, administration, identity) = test_profile();
    let routes = administration.project_routes();
    let mut snapshot = routes.snapshot().expect("project route snapshot");
    let event = hook_event(
        "session.denied",
        Some("thread.denied"),
        PathBuf::from("/work/denied"),
    );
    snapshot.observe_workspace_route(
        &event,
        WorkspaceProjectRoute::Failed(ProjectRouteFailure {
            kind: ProjectRouteFailureKind::NotAuthorized,
            detail: "route belongs to another authenticated profile".to_owned(),
        }),
    );
    routes
        .store(&snapshot)
        .expect("publish failed project route");

    let error = expect_route_error(
        projectless_registered_project_reader_server(
            &tool_request(
                "tracedecay_context",
                json!({
                    "task": "inspect",
                    "_meta": {"thread_id": "thread.denied"}
                }),
            ),
            &identity,
            &administration,
        ),
        "cached route failure must propagate",
    );
    assert_route_error(&error, "project_route_not_authorized", false);
    assert!(error.to_string().contains("another authenticated profile"));
}
