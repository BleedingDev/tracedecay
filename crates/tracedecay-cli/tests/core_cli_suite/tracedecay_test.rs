//! CLI journeys for the product surfaces that replaced the retired direct
//! `TraceDecay` graph/index API.

use std::{
    fs,
    path::Path,
    process::Command,
    time::{Duration, Instant},
};

use serde_json::{Value, json};
use tempfile::TempDir;

use crate::common::{self, canonical_existing_path, tracedecay_command_with_home};

fn init_daemon_project(project: &Path, home: &Path, source: &str) {
    fs::create_dir_all(project.join("src")).unwrap();
    fs::write(project.join("src/lib.rs"), source).unwrap();
    let git = Command::new(common::git_program())
        .args(["init", "--quiet", "--initial-branch=main"])
        .current_dir(project)
        .output()
        .expect("initialize Git worktree");
    assert!(
        git.status.success(),
        "git init failed: {}",
        String::from_utf8_lossy(&git.stderr)
    );

    crate::common::initialize_tracedecay_cli_project(home, project);
}

fn run_tool(project: &Path, home: &Path, args: &[&str]) -> std::process::Output {
    tracedecay_command_with_home(home)
        .current_dir(project)
        .arg("tool")
        .args(args)
        .output()
        .expect("tracedecay tool should run")
}

fn try_search_payload(output: &std::process::Output) -> Option<Value> {
    let envelope: Value = serde_json::from_slice(&output.stdout).ok()?;
    let text = envelope
        .get("content")?
        .as_array()?
        .first()?
        .get("text")?
        .as_str()?;
    serde_json::from_str(text).ok()
}

fn search_payload(output: &std::process::Output) -> Value {
    try_search_payload(output).unwrap_or_else(|| {
        panic!(
            "expected a JSON search payload; status={:?}, stdout={}, stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

fn run_search(project: &Path, home: &Path, args: Value) -> std::process::Output {
    let project_arg = project.to_string_lossy().to_string();
    let args_json = serde_json::to_string(&args).expect("serialize search arguments");
    run_tool(
        project,
        home,
        &[
            "--project",
            project_arg.as_str(),
            "search",
            "--json",
            "--args",
            args_json.as_str(),
        ],
    )
}

fn assert_search_refused(output: &std::process::Output, description: &str) {
    let payload = search_payload(output);
    assert_eq!(
        payload["status"], "unavailable",
        "{description} should return an unavailable search payload: {payload}"
    );
    assert_eq!(
        payload["reason"], "search_failed",
        "{description} should be refused by cursor validation: {payload}"
    );
    assert_eq!(
        payload["results"].as_array().map(Vec::len),
        Some(0),
        "{description} should not return search results: {payload}"
    );
}

fn setup_daemon_project(
    source: &str,
) -> (TempDir, TempDir, std::path::PathBuf, std::path::PathBuf) {
    let home = TempDir::new().unwrap();
    let project = TempDir::new().unwrap();
    let home_path = canonical_existing_path(home.path());
    let project_path = canonical_existing_path(project.path());
    common::ensure_tracedecay_daemon(&home_path);
    init_daemon_project(&project_path, &home_path, source);
    (home, project, home_path, project_path)
}

#[test]
fn daemon_tool_searches_the_active_project() {
    let (_home, _project, home_path, project_path) =
        setup_daemon_project("pub fn findable_symbol() {}\n");
    let project_arg = project_path.to_string_lossy().to_string();
    let output = common::poll_until(
        Instant::now() + Duration::from_secs(30),
        Duration::from_millis(100),
        || {
            let output = run_tool(
                &project_path,
                &home_path,
                &[
                    "--project",
                    &project_arg,
                    "search",
                    "--json",
                    "--args",
                    r#"{"query":"findable_symbol","limit":10}"#,
                ],
            );
            (output.status.success()
                && String::from_utf8_lossy(&output.stdout).contains("findable_symbol"))
            .then_some(output)
        },
        || "daemon scheduler did not publish findable_symbol for search".to_owned(),
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("findable_symbol"),
        "daemon-owned search must return the indexed symbol"
    );
}

#[test]
fn daemon_tool_search_paginates_authenticated_cursor_over_cli_transport() {
    let source = r#"
        /// The alpha primary cursor fixture has repeated cursor and fixture evidence.
        pub fn alpha_cursor_fixture_primary() -> u32 {
            let alpha_cursor_fixture_primary_value = 1;
            alpha_cursor_fixture_primary_value
        }

        /// The secondary cursor fixture has alpha cursor and fixture evidence.
        pub fn fixture_secondary() -> u32 {
            let alpha_cursor_fixture_secondary_value = 2;
            alpha_cursor_fixture_secondary_value
        }

        /// This alpha cursor fixture is a lower-ranked distractor.
        pub fn fixture_distractor() -> u32 {
            3
        }
    "#;
    let (_home, _project, home_path, project_path) = setup_daemon_project(source);
    let query = "alpha cursor fixture";

    let first = common::poll_until(
        Instant::now() + Duration::from_secs(30),
        Duration::from_millis(100),
        || {
            let output = run_search(
                &project_path,
                &home_path,
                json!({"query": query, "limit": 1, "format": "json"}),
            );
            let Some(payload) = try_search_payload(&output) else {
                return None;
            };
            let has_one_result = payload["results"]
                .as_array()
                .is_some_and(|results| results.len() == 1);
            let has_cursor = payload["next_cursor"]
                .as_str()
                .is_some_and(|cursor| !cursor.is_empty());
            let is_ready = output.status.success()
                && payload["coverage"]["recall"] == "full"
                && has_one_result
                && has_cursor;
            is_ready.then_some(output)
        },
        || format!("search did not produce a paginated first page for {query}"),
    );
    let first_payload = search_payload(&first);
    let first_results = first_payload["results"]
        .as_array()
        .expect("first search page results");
    assert_eq!(first_results.len(), 1, "first page={first_payload}");
    let first_anchor = first_results[0]["candidate"]["anchor_id"]
        .as_str()
        .expect("first result anchor")
        .to_owned();
    let first_cursor = first_payload["next_cursor"]
        .as_str()
        .expect("first page next_cursor")
        .to_owned();
    let cursor_object: Value =
        serde_json::from_str(&first_cursor).expect("opaque cursor JSON object");
    assert!(
        cursor_object["signature"].as_str().is_some(),
        "cursor={first_cursor}"
    );
    assert_eq!(
        cursor_object["next_ordinal"],
        json!(1),
        "cursor={first_cursor}"
    );

    let second = run_search(
        &project_path,
        &home_path,
        json!({
            "query": query,
            "limit": 1,
            "cursor": first_cursor.clone(),
            "format": "json"
        }),
    );
    assert!(
        second.status.success(),
        "second search page failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&second.stdout),
        String::from_utf8_lossy(&second.stderr)
    );
    let second_payload = search_payload(&second);
    let second_results = second_payload["results"]
        .as_array()
        .expect("second search page results");
    assert_eq!(second_results.len(), 1, "second page={second_payload}");
    let second_anchor = second_results[0]["candidate"]["anchor_id"]
        .as_str()
        .expect("second result anchor");
    assert_ne!(
        first_anchor, second_anchor,
        "the continuation page repeated the first result: first={first_payload}, second={second_payload}"
    );

    let mut tampered_cursor = cursor_object;
    tampered_cursor["signature"] = json!(format!("hmac-sha256:{}", "0".repeat(64)));
    let tampered = run_search(
        &project_path,
        &home_path,
        json!({
            "query": query,
            "limit": 1,
            "cursor": serde_json::to_string(&tampered_cursor).expect("serialize tampered cursor"),
            "format": "json"
        }),
    );
    assert_search_refused(&tampered, "tampered cursor");

    let mismatched_query = run_search(
        &project_path,
        &home_path,
        json!({
            "query": "different cursor fixture",
            "limit": 1,
            "cursor": first_cursor,
            "format": "json"
        }),
    );
    assert_search_refused(&mismatched_query, "query-mismatched cursor");
}

#[test]
fn daemon_tool_search_discloses_configured_alias_recovery() {
    let (_home, _project, home_path, project_path) = setup_daemon_project("pub fn cache() {}\n");
    let project_arg = project_path.to_string_lossy().to_string();
    let output = common::poll_until(
        Instant::now() + Duration::from_secs(30),
        Duration::from_millis(100),
        || {
            let output = run_tool(
                &project_path,
                &home_path,
                &[
                    "--project",
                    &project_arg,
                    "search",
                    "--json",
                    "--args",
                    r#"{"query":"memoization","lexical_aliases":[{"strict_query":"memoization","alternative":"cache"}],"limit":10}"#,
                ],
            );
            let stdout = String::from_utf8_lossy(&output.stdout);
            (output.status.success()
                && stdout.contains("Strict query: `memoization`")
                && stdout.contains("Alternative tried: `cache`")
                && stdout.contains("Reason: configured vocabulary alias"))
            .then_some(output)
        },
        || "daemon scheduler did not expose alias recovery for cache".to_owned(),
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("cache"), "{stdout}");
}

/// A daemon-owned source edit is a preview-then-apply effect: an apply must
/// carry a fresh `idempotency_key` and the `expected_state` its own preview
/// returned, so the write is compare-and-set against the exact bytes the
/// preview was computed from. Drive that journey end to end from the CLI.
#[test]
fn daemon_tool_str_replace_updates_source() {
    let (_home, _project, home_path, project_path) =
        setup_daemon_project("pub fn answer() -> u32 { 1 }\n");
    let project_arg = project_path.to_string_lossy().to_string();
    let preview = run_tool(
        &project_path,
        &home_path,
        &[
            "--project",
            &project_arg,
            "str_replace",
            "--json",
            "--args",
            r#"{"path":"src/lib.rs","old_str":"pub fn answer() -> u32 { 1 }","new_str":"pub fn answer() -> u32 { 2 }","dry_run":true,"format":"json"}"#,
        ],
    );
    assert!(
        preview.status.success(),
        "daemon-owned source edit preview failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&preview.stdout),
        String::from_utf8_lossy(&preview.stderr),
    );
    assert_eq!(
        fs::read_to_string(project_path.join("src/lib.rs")).unwrap(),
        "pub fn answer() -> u32 { 1 }\n",
        "a preview must not write"
    );
    let expected_state = source_edit_expected_state(&preview.stdout);

    let apply_args = serde_json::json!({
        "path": "src/lib.rs",
        "old_str": "pub fn answer() -> u32 { 1 }",
        "new_str": "pub fn answer() -> u32 { 2 }",
        "idempotency_key": "core-cli-suite.source-edit.str-replace",
        "expected_state": expected_state,
    })
    .to_string();
    let output = run_tool(
        &project_path,
        &home_path,
        &[
            "--project",
            &project_arg,
            "str_replace",
            "--json",
            "--args",
            apply_args.as_str(),
        ],
    );

    assert!(
        output.status.success(),
        "daemon-owned source edit failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert_eq!(
        fs::read_to_string(project_path.join("src/lib.rs")).unwrap(),
        "pub fn answer() -> u32 { 2 }\n"
    );
}

/// Reads `expected_state` out of a `format: "json"` source-edit preview.
///
/// `tracedecay tool --json` prints the MCP tool-result envelope; the requested
/// JSON document travels inside the first content block's text.
fn source_edit_expected_state(stdout: &[u8]) -> String {
    let envelope: serde_json::Value =
        serde_json::from_slice(stdout).expect("source edit preview should print JSON");
    let text = envelope["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("preview returned no content text: {envelope}"));
    let document: serde_json::Value =
        serde_json::from_str(text).expect("preview content should carry the JSON document");
    document["expected_state"]
        .as_str()
        .unwrap_or_else(|| panic!("preview omitted expected_state: {document}"))
        .to_owned()
}
