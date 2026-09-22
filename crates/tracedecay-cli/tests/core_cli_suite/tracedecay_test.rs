//! CLI journeys for the product surfaces that replaced the retired direct
//! `TraceDecay` graph/index API.

use std::{
    collections::HashSet,
    fs,
    path::Path,
    process::Command,
    time::{Duration, Instant},
};

use serde_json::{Value, json};
use tempfile::TempDir;

use crate::common::{self, canonical_existing_path, tracedecay_command_with_home};

#[cfg(unix)]
mod rmcp_test_support;

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

fn cursor_fixture_source() -> &'static str {
    r#"
        /// The alpha primary cursor fixture has repeated cursor and fixture evidence.
        pub fn alpha_cursor_fixture_primary(input: u32) -> u32 {
            let one = input.wrapping_add(1);
            let two = one.wrapping_add(2);
            let three = two.wrapping_add(3);
            let four = three.wrapping_add(4);
            let five = four.wrapping_add(5);
            let six = five.wrapping_add(6);
            six.wrapping_add(one)
        }

        /// The secondary cursor fixture has alpha cursor and fixture evidence.
        pub fn fixture_secondary(input: u32) -> u32 {
            let one = input.wrapping_add(1);
            let two = one.wrapping_add(2);
            let three = two.wrapping_add(3);
            let four = three.wrapping_add(4);
            let five = four.wrapping_add(5);
            let six = five.wrapping_add(6);
            six.wrapping_add(one)
        }

        /// This alpha cursor fixture is a lower-ranked distractor.
        pub fn fixture_distractor(input: u32) -> u32 {
            let one = input.wrapping_add(1);
            let two = one.wrapping_add(2);
            let three = two.wrapping_add(3);
            let four = three.wrapping_add(4);
            let five = four.wrapping_add(5);
            let six = five.wrapping_add(6);
            six.wrapping_add(one)
        }

        /// This near cursor fixture has alpha cursor and fixture evidence with
        /// a changed tail so the similar walk must cross its near phase.
        pub fn fixture_near(input: u32) -> u32 {
            let one = input.wrapping_add(1);
            let two = one.wrapping_add(2);
            let three = two.wrapping_add(3);
            let four = three.wrapping_add(4);
            let five = four.wrapping_add(5);
            let six = five.wrapping_add(6);
            six.wrapping_add(one).wrapping_add(two)
        }
    "#
}

fn try_search_payload(output: &std::process::Output) -> Option<Value> {
    if !output.status.success() {
        return None;
    }
    let envelope: Value = serde_json::from_slice(&output.stdout).ok()?;
    if envelope.get("isError").and_then(Value::as_bool) == Some(true) {
        return None;
    }
    envelope["content"]
        .as_array()?
        .iter()
        .filter_map(|item| item.get("text").and_then(Value::as_str))
        .find_map(|text| serde_json::from_str(text).ok())
}

fn search_payload(output: &std::process::Output, description: &str) -> Value {
    try_search_payload(output).unwrap_or_else(|| {
        panic!(
            "{description} did not return a successful JSON tool payload; status={:?}, stdout={}, stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

fn run_json_tool(
    project: &Path,
    home: &Path,
    tool_name: &str,
    args: Value,
) -> std::process::Output {
    let project_arg = project.to_string_lossy().to_string();
    let args_json = serde_json::to_string(&args).expect("serialize tool arguments");
    run_tool(
        project,
        home,
        &[
            "--project",
            project_arg.as_str(),
            tool_name,
            "--json",
            "--args",
            args_json.as_str(),
        ],
    )
}

fn run_search(project: &Path, home: &Path, args: Value) -> std::process::Output {
    run_json_tool(project, home, "search", args)
}

fn search_result_sets(payload: &Value, description: &str) -> (HashSet<String>, HashSet<String>) {
    let results = payload["results"]
        .as_array()
        .unwrap_or_else(|| panic!("{description} omitted its results array: {payload}"));
    let mut anchors = HashSet::new();
    let mut names = HashSet::new();
    for result in results {
        let anchor = result["candidate"]["anchor_id"]
            .as_str()
            .unwrap_or_else(|| panic!("{description} result omitted its anchor: {result}"));
        let name = result["display"]["name"]
            .as_str()
            .unwrap_or_else(|| panic!("{description} result omitted its display name: {result}"));
        assert!(
            anchors.insert(anchor.to_owned()),
            "{description} repeated an anchor within one page: {anchor}; payload={payload}"
        );
        assert!(
            names.insert(name.to_owned()),
            "{description} repeated a display name within one page: {name}; payload={payload}"
        );
    }
    (anchors, names)
}

fn optional_cursor(value: &Value, description: &str) -> Option<String> {
    let next_cursor = value
        .get("next_cursor")
        .unwrap_or_else(|| panic!("{description} omitted its next_cursor field: {value}"));
    if next_cursor.is_null() {
        return None;
    }
    let cursor = next_cursor
        .as_str()
        .unwrap_or_else(|| panic!("{description} returned a non-string next_cursor: {value}"));
    assert!(
        !cursor.is_empty(),
        "{description} returned an empty next_cursor: {value}"
    );
    Some(cursor.to_owned())
}

fn similar_cursor_after(cursor: &str) -> Option<Value> {
    let encoded = cursor.strip_prefix("ccclone2.")?;
    let bytes = hex::decode(encoded).ok()?;
    let envelope: Value = serde_json::from_slice(&bytes).ok()?;
    envelope.get("payload")?.get("after").cloned()
}

fn assert_cli_similar_refused(
    output: &std::process::Output,
    reason_code: &str,
    retryable: bool,
    detail: &str,
    description: &str,
) {
    assert!(
        !output.status.success(),
        "{description} must fail with the typed similar refusal: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stdout.is_empty(),
        "{description} must not print a successful tool result: stdout={}",
        String::from_utf8_lossy(&output.stdout)
    );
    let expected = format!(
        "Error: daemon tool call failed: tool project route failed: reason_code={reason_code} retryable={retryable}: {detail}"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.lines().any(|line| line == expected.as_str()),
        "{description} must preserve its typed refusal in stderr: expected={expected}, stderr={stderr}"
    );
}

fn assert_search_unavailable_payload(payload: &Value, description: &str) {
    assert_eq!(
        payload["status"], "unavailable",
        "{description} should return an unavailable search payload: {payload}"
    );
    assert_eq!(
        payload["reason"], "search_failed",
        "{description} should preserve the typed cursor refusal reason: {payload}"
    );
    assert!(
        payload.get("code_generation").is_some_and(Value::is_null),
        "{description} must not claim a served generation: {payload}"
    );
    assert!(
        payload
            .get("query_fallback_digest")
            .is_some_and(Value::is_null),
        "{description} must not claim a fallback result set: {payload}"
    );
    assert_eq!(
        payload["results"].as_array().map(Vec::len),
        Some(0),
        "{description} must not return search results: {payload}"
    );
    assert_eq!(
        payload["coverage"]["recall"], "partial",
        "{description} must disclose degraded coverage: {payload}"
    );
    for lane in ["exact", "lexical", "graph"] {
        assert_eq!(
            payload["coverage"][lane]["status"], "unavailable",
            "{description} {lane} coverage must be unavailable: {payload}"
        );
        assert_eq!(
            payload["coverage"][lane]["reason"], "search_failed",
            "{description} {lane} coverage must preserve the refusal reason: {payload}"
        );
    }
    assert_eq!(
        payload["freshness"]["state"], "possibly_stale",
        "{description} must carry the unavailable freshness verdict: {payload}"
    );
    assert_eq!(
        payload["freshness"]["indexing"]["reason"], "search_failed",
        "{description} freshness must preserve the refusal reason: {payload}"
    );
}

fn assert_search_refused(output: &std::process::Output, description: &str) {
    assert!(
        !output.status.success(),
        "{description} must return its typed refusal envelope with a failure status: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let envelope: Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "{description} did not return a JSON tool envelope: {error}; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    assert_eq!(
        envelope["isError"],
        json!(true),
        "{description} must mark the typed search refusal as a semantic tool error: {envelope}"
    );
    let payload = envelope["content"]
        .as_array()
        .and_then(|content| {
            content
                .iter()
                .filter_map(|item| item.get("text"))
                .find_map(|text| {
                    text.as_str()
                        .and_then(|text| serde_json::from_str(text).ok())
                })
        })
        .unwrap_or_else(|| panic!("{description} omitted its typed refusal payload: {envelope}"));
    assert_search_unavailable_payload(&payload, description);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr
            .lines()
            .any(|line| line == "Error: tracedecay_search reported an application failure."),
        "{description} must report the semantic refusal at the CLI boundary: stderr={stderr}"
    );
}

#[test]
fn daemon_tool_search_paginates_authenticated_cursor_over_cli_transport() {
    let (_home, _project, home_path, project_path) = setup_daemon_project(cursor_fixture_source());
    let query = "alpha cursor fixture";

    let baseline_payload = common::poll_until(
        Instant::now() + Duration::from_secs(30),
        Duration::from_millis(100),
        || {
            let output = run_search(
                &project_path,
                &home_path,
                json!({"query": query, "limit": 500, "format": "json"}),
            );
            let Some(payload) = try_search_payload(&output) else {
                return None;
            };
            let ready = payload["coverage"]["recall"] == "full"
                && payload["results"]
                    .as_array()
                    .is_some_and(|results| !results.is_empty())
                && payload.get("next_cursor").is_some_and(Value::is_null);
            ready.then_some(payload)
        },
        || format!("search did not produce a complete unpaged baseline for {query}"),
    );
    let (baseline_anchors, baseline_names) =
        search_result_sets(&baseline_payload, "unpaged CLI search baseline");
    for expected_name in [
        "alpha_cursor_fixture_primary",
        "fixture_secondary",
        "fixture_distractor",
        "fixture_near",
    ] {
        assert!(
            baseline_names.contains(expected_name),
            "unpaged CLI baseline omitted fixture symbol {expected_name}: {baseline_payload}"
        );
    }

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
            let ready = payload["coverage"]["recall"] == "full"
                && payload["results"]
                    .as_array()
                    .is_some_and(|results| results.len() == 1)
                && payload["next_cursor"]
                    .as_str()
                    .is_some_and(|cursor| !cursor.is_empty());
            ready.then_some(output)
        },
        || format!("search did not produce a paginated first page for {query}"),
    );
    let first_payload = search_payload(&first, "first CLI search page");
    let first_results = first_payload["results"]
        .as_array()
        .expect("first CLI search page results");
    assert_eq!(first_results.len(), 1, "first page={first_payload}");
    assert_eq!(
        first_results[0]["final_ordinal"],
        json!(0),
        "the first CLI search result must have ordinal zero: {first_payload}"
    );

    let mut seen_anchors = HashSet::new();
    let mut seen_names = HashSet::new();
    let first_anchor = first_results[0]["candidate"]["anchor_id"]
        .as_str()
        .expect("first CLI result anchor")
        .to_owned();
    let first_name = first_results[0]["display"]["name"]
        .as_str()
        .expect("first CLI result display name")
        .to_owned();
    assert!(
        seen_anchors.insert(first_anchor.clone()),
        "first CLI result anchor must be unique: {first_anchor}"
    );
    assert!(
        seen_names.insert(first_name.clone()),
        "first CLI result display name must be unique: {first_name}"
    );

    let first_cursor = first_payload["next_cursor"]
        .as_str()
        .expect("first CLI page next_cursor")
        .to_owned();
    let cursor_object: Value =
        serde_json::from_str(&first_cursor).expect("authenticated CLI search cursor JSON object");
    assert_eq!(
        cursor_object["next_ordinal"],
        json!(1),
        "the first CLI continuation must begin at ordinal 1: cursor={first_cursor}"
    );
    assert!(
        cursor_object["signature"]
            .as_str()
            .is_some_and(|signature| signature.starts_with("hmac-sha256:")),
        "the CLI cursor must carry an HMAC signature: cursor={first_cursor}"
    );

    let mut seen_cursors = HashSet::new();
    assert!(seen_cursors.insert(first_cursor.clone()));
    let mut next_cursor = Some(first_cursor.clone());
    let mut page_count = 1_usize;
    while let Some(cursor) = next_cursor.take() {
        assert!(
            page_count < 64,
            "CLI search continuation did not terminate after 64 pages"
        );
        let output = run_search(
            &project_path,
            &home_path,
            json!({
                "query": query,
                "limit": 1,
                "cursor": cursor,
                "format": "json"
            }),
        );
        let payload = search_payload(&output, "CLI search continuation page");
        assert_eq!(
            payload["coverage"]["recall"], "full",
            "CLI continuation page must retain full coverage: {payload}"
        );
        let results = payload["results"]
            .as_array()
            .expect("CLI continuation page results");
        assert_eq!(results.len(), 1, "CLI continuation page={payload}");
        assert_eq!(
            results[0]["final_ordinal"],
            json!(page_count as u64),
            "CLI search result ordinals must advance without gaps: {payload}"
        );
        let anchor = results[0]["candidate"]["anchor_id"]
            .as_str()
            .expect("CLI continuation result anchor")
            .to_owned();
        let name = results[0]["display"]["name"]
            .as_str()
            .expect("CLI continuation result display name")
            .to_owned();
        assert!(
            seen_anchors.insert(anchor.clone()),
            "CLI search repeated a result across pages: anchor={anchor}, page={payload}"
        );
        assert!(
            seen_names.insert(name.clone()),
            "CLI search repeated a display name across pages: name={name}, page={payload}"
        );
        page_count += 1;

        next_cursor = optional_cursor(&payload, "CLI search continuation page");
        if let Some(cursor) = next_cursor.as_ref() {
            assert!(
                seen_cursors.insert(cursor.clone()),
                "CLI search repeated an authenticated continuation: {cursor}"
            );
            let cursor_object: Value =
                serde_json::from_str(cursor).expect("CLI continuation cursor JSON object");
            assert_eq!(
                cursor_object["next_ordinal"],
                json!(page_count as u64),
                "CLI continuation ordinal must advance with every page: cursor={cursor}"
            );
            assert!(
                cursor_object["signature"]
                    .as_str()
                    .is_some_and(|signature| signature.starts_with("hmac-sha256:")),
                "CLI continuation cursor must retain its HMAC signature: cursor={cursor}"
            );
        }
    }
    assert!(
        page_count >= 2,
        "the CLI fixture must exercise at least one authenticated continuation"
    );
    assert_eq!(
        seen_anchors.len(),
        page_count,
        "every completed CLI search page must contribute one globally unique result"
    );
    assert_eq!(
        seen_anchors, baseline_anchors,
        "CLI pagination must cover exactly the unpaged baseline anchors"
    );
    assert_eq!(
        seen_names, baseline_names,
        "CLI pagination must cover exactly the unpaged baseline names"
    );

    let mut tampered_cursor = cursor_object;
    tampered_cursor["signature"] = json!(format!("hmac-sha256:{}", "0".repeat(64)));
    let tampered = run_search(
        &project_path,
        &home_path,
        json!({
            "query": query,
            "limit": 1,
            "cursor": serde_json::to_string(&tampered_cursor)
                .expect("serialize tampered CLI search cursor"),
            "format": "json"
        }),
    );
    assert_search_refused(&tampered, "tampered CLI search cursor");

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
    assert_search_refused(&mismatched_query, "query-mismatched CLI search cursor");

    let active_project = run_json_tool(
        &project_path,
        &home_path,
        "active_project",
        json!({"format": "json"}),
    );
    let active_project_payload = search_payload(&active_project, "CLI active_project");
    let project_id = active_project_payload["project_id"]
        .as_str()
        .expect("CLI active_project project_id");
    let repository_id = active_project_payload["repository_id"]
        .as_str()
        .expect("CLI active_project repository_id");
    let symbol_occurrence_id = first_results[0]["node_id"]
        .as_str()
        .expect("CLI search result node_id for similar")
        .to_owned();
    let similar_args = json!({
        "project_id": project_id,
        "repository_id": repository_id,
        "target": {
            "kind": "symbol_occurrence",
            "symbol_occurrence_id": symbol_occurrence_id.clone(),
        },
        "match_classes": ["conservative_exact"],
        "result_limit": 1,
        "work_limit": 20,
        "format": "json"
    });
    let first = common::poll_until(
        Instant::now() + Duration::from_secs(30),
        Duration::from_millis(100),
        || {
            let output = run_json_tool(&project_path, &home_path, "similar", similar_args.clone());
            let Some(payload) = try_search_payload(&output) else {
                return None;
            };
            let has_source = payload["source"]["symbol_occurrence_id"] == symbol_occurrence_id;
            let has_paginated_family = payload["families"].as_array().is_some_and(|families| {
                families.iter().any(|family| {
                    family["match_class"] == "conservative_exact"
                        && family["members"]
                            .as_array()
                            .is_some_and(|members| members.len() == 1)
                        && family["next_cursor"]
                            .as_str()
                            .is_some_and(|cursor| !cursor.is_empty())
                })
            });
            (has_source && has_paginated_family).then_some((output, payload))
        },
        || {
            "similar did not publish a paginated verified clone family for the CLI search result"
                .to_owned()
        },
    );
    let first_cursor = first.1["families"]
        .as_array()
        .and_then(|families| {
            families.iter().find_map(|family| {
                (family["match_class"] == "conservative_exact")
                    .then(|| family["next_cursor"].as_str())
                    .flatten()
            })
        })
        .expect("first CLI similar family next_cursor")
        .to_owned();
    assert!(
        first_cursor.starts_with("ccclone2."),
        "CLI similar continuation must use the authenticated clone cursor: {first_cursor}"
    );

    // The first page is already in hand. Only cursors minted by a processed
    // page enter this queue, so every page is consumed exactly once.
    let mut pending = Some(first);
    let mut pending_cursors = Vec::new();
    let mut seen_cursors = HashSet::new();
    let mut seen_members = HashSet::new();
    let mut seen_near_members = HashSet::new();
    let mut page_count = 0_usize;
    let mut saw_terminal_exact_family = false;
    let mut saw_complete_coverage = false;
    let mut saw_near_cursor = false;
    let mut saw_near_phase_cursor = false;
    let mut saw_near_result = false;
    let mut saw_near_complete = false;
    while pending.is_some() || !pending_cursors.is_empty() {
        assert!(
            page_count < 64,
            "CLI similar continuation did not terminate after 64 pages"
        );
        let (output, payload) = match pending.take() {
            Some(page) => page,
            None => {
                let cursor = pending_cursors
                    .pop()
                    .expect("CLI similar continuation cursor");
                let mut args = similar_args.clone();
                args["cursor"] = json!(cursor);
                let output = run_json_tool(&project_path, &home_path, "similar", args);
                let payload = search_payload(&output, "CLI similar continuation page");
                (output, payload)
            }
        };
        assert!(
            output.status.success(),
            "CLI similar page must succeed: stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            payload["source"]["symbol_occurrence_id"], symbol_occurrence_id,
            "CLI similar continuation changed its source: {payload}"
        );
        saw_complete_coverage |= payload["coverage"]["status"] == json!("complete");
        let families = payload["families"]
            .as_array()
            .expect("CLI similar families");
        let conservative_families = families
            .iter()
            .filter(|family| family["match_class"] == json!("conservative_exact"))
            .collect::<Vec<_>>();
        assert!(
            conservative_families.len() <= 1,
            "the CLI exact-class request must carry at most one conservative family: {payload}"
        );
        let family_cursor = if let Some(family) = conservative_families.first() {
            let members = family["members"]
                .as_array()
                .expect("CLI similar family members");
            assert_eq!(
                family["member_count"],
                json!(members.len()),
                "CLI family member_count must match the wire page: {payload}"
            );
            for member in members {
                let member_id = member["symbol_occurrence_id"]
                    .as_str()
                    .expect("CLI similar member occurrence id")
                    .to_owned();
                assert!(
                    seen_members.insert(member_id.clone()),
                    "CLI similar repeated a member across pages: {member_id}; page={payload}"
                );
            }
            let family_cursor = optional_cursor(family, "CLI similar conservative family");
            if family_cursor.is_none() {
                assert_eq!(
                    family["complete"],
                    json!(true),
                    "terminal CLI similar family must be complete: {payload}"
                );
                saw_terminal_exact_family = true;
            }
            family_cursor
        } else {
            None
        };

        let near = payload["near"]
            .as_object()
            .expect("CLI similar near result");
        let near_matches = near["matches"]
            .as_array()
            .expect("CLI similar near matches");
        if !near_matches.is_empty() {
            saw_near_result = true;
        }
        for near_match in near_matches {
            let member_id = near_match["candidate"]["symbol_occurrence_id"]
                .as_str()
                .expect("CLI similar near member occurrence id")
                .to_owned();
            assert!(
                seen_near_members.insert(member_id.clone()),
                "CLI similar repeated a near member across pages: {member_id}; page={payload}"
            );
        }
        let near_cursor = optional_cursor(&payload["near"], "CLI similar near result");
        if let Some(cursor) = near_cursor.as_ref() {
            saw_near_cursor = true;
            saw_near_phase_cursor |= similar_cursor_after(cursor) == Some(json!("NearStart"));
        } else if near["coverage"]["status"] == json!("complete") {
            saw_near_complete = true;
        }
        assert!(
            conservative_families
                .first()
                .and_then(|family| family["members"].as_array())
                .is_some_and(|members| !members.is_empty())
                || !near_matches.is_empty(),
            "every CLI similar page must carry verified evidence: {payload}"
        );
        page_count += 1;

        for cursor in family_cursor.into_iter().chain(near_cursor) {
            assert!(
                cursor.starts_with("ccclone2."),
                "CLI similar continuation must remain an authenticated clone cursor: {cursor}"
            );
            assert!(
                seen_cursors.insert(cursor.clone()),
                "CLI similar repeated an authenticated continuation: {cursor}"
            );
            pending_cursors.push(cursor);
        }
    }
    assert!(
        saw_terminal_exact_family,
        "CLI similar pagination must reach a complete conservative family"
    );
    assert!(
        saw_complete_coverage,
        "CLI similar pagination must reach complete overall coverage"
    );
    assert!(
        saw_near_cursor,
        "CLI similar pagination must observe a near continuation cursor"
    );
    assert!(
        saw_near_phase_cursor,
        "CLI similar pagination must observe the authenticated NearStart phase cursor"
    );
    assert!(
        saw_near_result,
        "CLI similar pagination must return at least one verified near result"
    );
    assert!(
        saw_near_complete,
        "CLI similar pagination must reach complete near coverage"
    );
    assert!(
        page_count >= 2,
        "the CLI repeated clone fixture must exercise at least one similar continuation"
    );
    assert!(
        seen_members.len() >= 2,
        "CLI similar pagination must expose more than one unique family member"
    );

    let mut tampered_cursor = first_cursor.clone();
    let last = tampered_cursor
        .pop()
        .expect("CLI similar cursor must have an encoded body");
    tampered_cursor.push(if last == '0' { '1' } else { '0' });
    let mut tampered_args = similar_args.clone();
    tampered_args["cursor"] = json!(tampered_cursor);
    let tampered = run_json_tool(&project_path, &home_path, "similar", tampered_args);
    assert_cli_similar_refused(
        &tampered,
        "invalid_request",
        false,
        "the maintained clone similarity lane is unavailable: invalid_request",
        "tampered CLI similar cursor",
    );

    let mut mismatched_args = similar_args;
    mismatched_args["match_classes"] = json!(["rename_normalized_exact"]);
    mismatched_args["cursor"] = json!(first_cursor);
    let mismatched = run_json_tool(&project_path, &home_path, "similar", mismatched_args);
    assert_cli_similar_refused(
        &mismatched,
        "generation_unavailable",
        true,
        "the maintained clone similarity lane is unavailable: generation_unavailable",
        "query-mismatched CLI similar cursor",
    );
}

#[cfg(unix)]
fn assert_rmcp_success(response: &Value, description: &str) {
    assert_eq!(
        response["jsonrpc"],
        json!("2.0"),
        "{description}: {response}"
    );
    assert_eq!(response["id"], json!(2), "{description}: {response}");
    assert!(
        response.get("error").is_none_or(Value::is_null),
        "{description} must be a successful JSON-RPC response: {response}"
    );
    assert!(
        response["result"].is_object(),
        "{description} must carry a tool result: {response}"
    );
    assert_ne!(
        response["result"]["isError"],
        json!(true),
        "{description} must not be a semantic tool refusal: {response}"
    );
}

#[cfg(unix)]
fn assert_rmcp_search_refused(response: &Value, description: &str) {
    // Search exposes its typed unavailable state as a completed tool result,
    // marked as a semantic MCP tool error. Requiring that exact shape prevents
    // a disconnected or internally failed daemon from satisfying this
    // assertion with an unrelated JSON-RPC error.
    assert_eq!(
        response["jsonrpc"],
        json!("2.0"),
        "{description}: {response}"
    );
    assert_eq!(response["id"], json!(2), "{description}: {response}");
    assert!(
        response.get("error").is_none_or(Value::is_null),
        "{description} must return the typed search result instead of a JSON-RPC error: {response}"
    );
    assert!(
        response["result"].is_object(),
        "{description} must carry a tool result: {response}"
    );
    assert_eq!(
        response["result"]["isError"],
        json!(true),
        "{description} must mark the typed search refusal as a semantic tool error: {response}"
    );
    let payload = rmcp_test_support::tool_payload(response);
    assert_search_unavailable_payload(&payload, description);
}

#[cfg(unix)]
fn assert_rmcp_similar_refused(response: &Value, description: &str) {
    let expected = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "error": {
            "code": -32602,
            "message": "tool project route failed: reason_code=invalid_request retryable=false: the maintained clone similarity lane is unavailable: invalid_request",
            "data": {
            "tool": "tracedecay_similar",
            "reason_code": "invalid_request",
            "retryable": false,
            "detail": "the maintained clone similarity lane is unavailable: invalid_request",
            }
        }
    });
    assert_eq!(
        response, &expected,
        "{description} must return the exact typed refusal envelope"
    );
}

#[cfg(unix)]
fn assert_rmcp_similar_query_mismatch(response: &Value, description: &str) {
    let expected = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "error": {
            "code": -32603,
            "message": "tool project route failed: reason_code=generation_unavailable retryable=true: the maintained clone similarity lane is unavailable: generation_unavailable",
            "data": {
            "tool": "tracedecay_similar",
            "reason_code": "generation_unavailable",
            "retryable": true,
            "detail": "the maintained clone similarity lane is unavailable: generation_unavailable",
            }
        }
    });
    assert_eq!(
        response, &expected,
        "{description} must return the exact typed stale-cursor envelope"
    );
}

#[cfg(unix)]
#[test]
fn daemon_rmcp_search_paginates_authenticated_cursor_to_completion() {
    let (_home, _project, home_path, project_path) = setup_daemon_project(cursor_fixture_source());
    let query = "alpha cursor fixture";

    let (baseline_response, baseline_payload) = common::poll_until(
        Instant::now() + Duration::from_secs(30),
        Duration::from_millis(100),
        || {
            let response = rmcp_test_support::call_json_tool(
                &home_path,
                &project_path,
                "tracedecay_search",
                json!({"query": query, "limit": 500, "format": "json"}),
            );
            let Some(payload) = rmcp_test_support::try_tool_payload(&response) else {
                return None;
            };
            let ready = response["error"].is_null()
                && response["result"]["isError"] != json!(true)
                && payload["coverage"]["recall"] == "full"
                && payload["results"]
                    .as_array()
                    .is_some_and(|results| !results.is_empty())
                && payload.get("next_cursor").is_some_and(Value::is_null);
            ready.then_some((response, payload))
        },
        || format!("RMCP search did not produce a complete unpaged baseline for {query}"),
    );
    assert_rmcp_success(&baseline_response, "unpaged RMCP search baseline");
    let (baseline_anchors, baseline_names) =
        search_result_sets(&baseline_payload, "unpaged RMCP search baseline");

    let (first_response, first_payload) = common::poll_until(
        Instant::now() + Duration::from_secs(30),
        Duration::from_millis(100),
        || {
            let response = rmcp_test_support::call_json_tool(
                &home_path,
                &project_path,
                "tracedecay_search",
                json!({"query": query, "limit": 1, "format": "json"}),
            );
            let Some(payload) = rmcp_test_support::try_tool_payload(&response) else {
                return None;
            };
            let ready = response["error"].is_null()
                && response["result"]["isError"] != json!(true)
                && payload["coverage"]["recall"] == "full"
                && payload["results"]
                    .as_array()
                    .is_some_and(|results| results.len() == 1)
                && payload["next_cursor"]
                    .as_str()
                    .is_some_and(|cursor| !cursor.is_empty());
            ready.then_some((response, payload))
        },
        || format!("RMCP search did not produce a paginated first page for {query}"),
    );
    assert_rmcp_success(&first_response, "first RMCP search page");
    let first_results = first_payload["results"]
        .as_array()
        .expect("first RMCP search page results");
    assert_eq!(first_results.len(), 1, "first page={first_payload}");
    assert_eq!(
        first_results[0]["final_ordinal"],
        json!(0),
        "the first RMCP search result must have ordinal zero: {first_payload}"
    );

    let mut seen_anchors = HashSet::new();
    let mut seen_names = HashSet::new();
    let first_anchor = first_results[0]["candidate"]["anchor_id"]
        .as_str()
        .expect("first RMCP result anchor")
        .to_owned();
    let first_name = first_results[0]["display"]["name"]
        .as_str()
        .expect("first RMCP result display name")
        .to_owned();
    assert!(
        seen_anchors.insert(first_anchor.clone()),
        "first result anchor must be unique: {first_anchor}"
    );
    assert!(
        seen_names.insert(first_name.clone()),
        "first RMCP result display name must be unique: {first_name}"
    );

    let first_cursor = first_payload["next_cursor"]
        .as_str()
        .expect("first RMCP page next_cursor")
        .to_owned();
    let cursor_object: Value =
        serde_json::from_str(&first_cursor).expect("authenticated search cursor JSON object");
    assert!(
        cursor_object["signature"]
            .as_str()
            .is_some_and(|signature| signature.starts_with("hmac-sha256:")),
        "cursor={first_cursor}"
    );
    assert_eq!(
        cursor_object["next_ordinal"],
        json!(1),
        "the first continuation must begin at ordinal 1: cursor={first_cursor}"
    );

    let mut seen_cursors = HashSet::new();
    assert!(seen_cursors.insert(first_cursor.clone()));
    let mut next_cursor = Some(first_cursor.clone());
    let mut page_count = 1_usize;
    while let Some(cursor) = next_cursor.take() {
        assert!(
            page_count < 64,
            "RMCP search continuation did not terminate after 64 pages"
        );
        let response = rmcp_test_support::call_json_tool(
            &home_path,
            &project_path,
            "tracedecay_search",
            json!({
                "query": query,
                "limit": 1,
                "cursor": cursor,
                "format": "json"
            }),
        );
        assert_rmcp_success(&response, "RMCP search continuation page");
        let payload = rmcp_test_support::tool_payload(&response);
        assert_eq!(
            payload["coverage"]["recall"], "full",
            "continuation page must retain full coverage: {payload}"
        );
        let results = payload["results"]
            .as_array()
            .expect("RMCP continuation page results");
        assert_eq!(results.len(), 1, "continuation page={payload}");
        assert_eq!(
            results[0]["final_ordinal"],
            json!(page_count as u64),
            "RMCP search result ordinals must advance without gaps: {payload}"
        );
        let anchor = results[0]["candidate"]["anchor_id"]
            .as_str()
            .expect("RMCP continuation result anchor")
            .to_owned();
        let name = results[0]["display"]["name"]
            .as_str()
            .expect("RMCP continuation result display name")
            .to_owned();
        assert!(
            seen_anchors.insert(anchor.clone()),
            "RMCP search repeated a result across pages: anchor={anchor}, page={payload}"
        );
        assert!(
            seen_names.insert(name.clone()),
            "RMCP search repeated a display name across pages: name={name}, page={payload}"
        );
        page_count += 1;

        next_cursor = optional_cursor(&payload, "RMCP search continuation page");
        if let Some(cursor) = next_cursor.as_ref() {
            assert!(
                seen_cursors.insert(cursor.clone()),
                "RMCP search repeated an authenticated continuation: {cursor}"
            );
            let cursor_object: Value =
                serde_json::from_str(cursor).expect("continuation cursor JSON object");
            assert_eq!(
                cursor_object["next_ordinal"],
                json!(page_count as u64),
                "continuation ordinal must advance with every page: cursor={cursor}"
            );
            assert!(
                cursor_object["signature"]
                    .as_str()
                    .is_some_and(|signature| signature.starts_with("hmac-sha256:")),
                "continuation cursor must retain its authentication signature: {cursor}"
            );
        }
    }
    assert!(
        page_count >= 2,
        "the fixture must exercise at least one authenticated continuation"
    );
    assert_eq!(
        seen_anchors.len(),
        page_count,
        "every completed search page must contribute one globally unique result"
    );
    assert_eq!(
        seen_anchors, baseline_anchors,
        "RMCP pagination must cover exactly the unpaged baseline anchors"
    );
    assert_eq!(
        seen_names, baseline_names,
        "RMCP pagination must cover exactly the unpaged baseline names"
    );

    let mut tampered_cursor = cursor_object;
    tampered_cursor["signature"] = json!(format!("hmac-sha256:{}", "0".repeat(64)));
    let tampered_response = rmcp_test_support::call_json_tool(
        &home_path,
        &project_path,
        "tracedecay_search",
        json!({
            "query": query,
            "limit": 1,
            "cursor": serde_json::to_string(&tampered_cursor)
                .expect("serialize tampered RMCP search cursor"),
            "format": "json"
        }),
    );
    assert_rmcp_search_refused(&tampered_response, "tampered RMCP search cursor");

    let mismatched_response = rmcp_test_support::call_json_tool(
        &home_path,
        &project_path,
        "tracedecay_search",
        json!({
            "query": "different cursor fixture",
            "limit": 1,
            "cursor": first_cursor,
            "format": "json"
        }),
    );
    assert_rmcp_search_refused(&mismatched_response, "query-mismatched RMCP search cursor");
}

#[cfg(unix)]
#[test]
fn daemon_rmcp_similar_paginates_authenticated_cursor_to_completion() {
    let (_home, _project, home_path, project_path) = setup_daemon_project(cursor_fixture_source());
    let search_payload = common::poll_until(
        Instant::now() + Duration::from_secs(30),
        Duration::from_millis(100),
        || {
            let response = rmcp_test_support::call_json_tool(
                &home_path,
                &project_path,
                "tracedecay_search",
                json!({
                    "query": "alpha cursor fixture",
                    "limit": 10,
                    "format": "json"
                }),
            );
            let Some(payload) = rmcp_test_support::try_tool_payload(&response) else {
                return None;
            };
            let ready = response["error"].is_null()
                && response["result"]["isError"] != json!(true)
                && payload["coverage"]["recall"] == "full"
                && payload["results"]
                    .as_array()
                    .is_some_and(|results| !results.is_empty());
            ready.then_some(payload)
        },
        || "RMCP search did not publish a target for similar".to_owned(),
    );
    let search_results = search_payload["results"]
        .as_array()
        .expect("RMCP search results for similar");
    let target_result = search_results
        .iter()
        .find(|result| result["display"]["name"] == json!("alpha_cursor_fixture_primary"))
        .or_else(|| search_results.first())
        .expect("RMCP search target result");
    let symbol_occurrence_id = target_result["node_id"]
        .as_str()
        .expect("JSON RMCP search result node_id for similar")
        .to_owned();

    let active_project_payload = common::poll_until(
        Instant::now() + Duration::from_secs(30),
        Duration::from_millis(100),
        || {
            let response = rmcp_test_support::call_json_tool(
                &home_path,
                &project_path,
                "tracedecay_active_project",
                json!({"format": "json"}),
            );
            let Some(payload) = rmcp_test_support::try_tool_payload(&response) else {
                return None;
            };
            (response["error"].is_null()
                && response["result"]["isError"] != json!(true)
                && payload["project_id"].as_str().is_some()
                && payload["repository_id"].as_str().is_some())
            .then_some(payload)
        },
        || "RMCP active_project did not expose project and repository identities".to_owned(),
    );
    let project_id = active_project_payload["project_id"]
        .as_str()
        .expect("RMCP active_project project_id");
    let repository_id = active_project_payload["repository_id"]
        .as_str()
        .expect("RMCP active_project repository_id");
    let similar_args = json!({
        "project_id": project_id,
        "repository_id": repository_id,
        "target": {
            "kind": "symbol_occurrence",
            "symbol_occurrence_id": symbol_occurrence_id.clone(),
        },
        "match_classes": ["conservative_exact"],
        "result_limit": 1,
        "work_limit": 20,
        "format": "json"
    });

    let first = common::poll_until(
        Instant::now() + Duration::from_secs(30),
        Duration::from_millis(100),
        || {
            let response = rmcp_test_support::call_json_tool(
                &home_path,
                &project_path,
                "tracedecay_similar",
                similar_args.clone(),
            );
            let Some(payload) = rmcp_test_support::try_tool_payload(&response) else {
                return None;
            };
            let has_paginated_family = payload["families"].as_array().is_some_and(|families| {
                families.iter().any(|family| {
                    family["match_class"] == json!("conservative_exact")
                        && family["members"]
                            .as_array()
                            .is_some_and(|members| members.len() == 1)
                        && family["next_cursor"]
                            .as_str()
                            .is_some_and(|cursor| !cursor.is_empty())
                })
            });
            let ready = response["error"].is_null()
                && response["result"]["isError"] != json!(true)
                && payload["source"]["symbol_occurrence_id"] == symbol_occurrence_id
                && has_paginated_family;
            ready.then_some((response, payload))
        },
        || "RMCP similar did not publish a paginated verified clone family".to_owned(),
    );

    let first_cursor = first.1["families"]
        .as_array()
        .and_then(|families| {
            families.iter().find_map(|family| {
                (family["match_class"] == json!("conservative_exact"))
                    .then(|| family["next_cursor"].as_str())
                    .flatten()
            })
        })
        .expect("first RMCP similar family next_cursor")
        .to_owned();
    assert!(
        first_cursor.starts_with("ccclone2."),
        "similar continuation must use the authenticated clone cursor: {first_cursor}"
    );

    let mut pending = Some(first);
    let mut pending_cursors = Vec::new();
    let mut seen_cursors = HashSet::new();
    let mut seen_members = HashSet::new();
    let mut seen_near_members = HashSet::new();
    let mut page_count = 0_usize;
    let mut saw_terminal_exact_family = false;
    let mut saw_complete_coverage = false;
    let mut saw_near_cursor = false;
    let mut saw_near_phase_cursor = false;
    let mut saw_near_result = false;
    let mut saw_near_complete = false;
    while pending.is_some() || !pending_cursors.is_empty() {
        assert!(
            page_count < 64,
            "RMCP similar continuation did not terminate after 64 pages"
        );
        let (response, payload) = match pending.take() {
            Some(page) => page,
            None => {
                let cursor = pending_cursors
                    .pop()
                    .expect("RMCP similar continuation cursor");
                let mut args = similar_args.clone();
                args["cursor"] = json!(cursor);
                let response = rmcp_test_support::call_json_tool(
                    &home_path,
                    &project_path,
                    "tracedecay_similar",
                    args,
                );
                let payload = rmcp_test_support::tool_payload(&response);
                (response, payload)
            }
        };
        assert_rmcp_success(&response, "RMCP similar page");
        assert_eq!(
            payload["source"]["symbol_occurrence_id"], symbol_occurrence_id,
            "similar continuation changed its source: {payload}"
        );
        saw_complete_coverage |= payload["coverage"]["status"] == json!("complete");
        let families = payload["families"]
            .as_array()
            .expect("RMCP similar families");
        let conservative_families = families
            .iter()
            .filter(|family| family["match_class"] == json!("conservative_exact"))
            .collect::<Vec<_>>();
        assert!(
            conservative_families.len() <= 1,
            "the exact-class request must carry at most one conservative family: {payload}"
        );
        let family_cursor = if let Some(family) = conservative_families.first() {
            let members = family["members"]
                .as_array()
                .expect("RMCP similar family members");
            assert_eq!(
                family["member_count"],
                json!(members.len()),
                "family member_count must match the wire page: {payload}"
            );
            for member in members {
                let member_id = member["symbol_occurrence_id"]
                    .as_str()
                    .expect("RMCP similar member occurrence id")
                    .to_owned();
                assert!(
                    seen_members.insert(member_id.clone()),
                    "RMCP similar repeated a member across pages: {member_id}; page={payload}"
                );
            }
            let family_cursor = optional_cursor(family, "RMCP similar conservative family");
            if family_cursor.is_none() {
                assert_eq!(
                    family["complete"],
                    json!(true),
                    "terminal similar family must be complete: {payload}"
                );
                saw_terminal_exact_family = true;
            }
            family_cursor
        } else {
            None
        };
        let near = payload["near"]
            .as_object()
            .expect("RMCP similar near result");
        let near_matches = near["matches"]
            .as_array()
            .expect("RMCP similar near matches");
        if !near_matches.is_empty() {
            saw_near_result = true;
        }
        for near_match in near_matches {
            let member_id = near_match["candidate"]["symbol_occurrence_id"]
                .as_str()
                .expect("RMCP similar near member occurrence id")
                .to_owned();
            assert!(
                seen_near_members.insert(member_id.clone()),
                "RMCP similar repeated a near member across pages: {member_id}; page={payload}"
            );
        }
        let near_cursor = optional_cursor(&payload["near"], "RMCP similar near result");
        if let Some(cursor) = near_cursor.as_ref() {
            saw_near_cursor = true;
            saw_near_phase_cursor |= similar_cursor_after(cursor) == Some(json!("NearStart"));
        } else if near["coverage"]["status"] == json!("complete") {
            saw_near_complete = true;
        }
        assert!(
            conservative_families
                .first()
                .and_then(|family| family["members"].as_array())
                .is_some_and(|members| !members.is_empty())
                || !near_matches.is_empty(),
            "every RMCP similar page must carry verified evidence: {payload}"
        );
        page_count += 1;

        for cursor in [family_cursor, near_cursor].into_iter().flatten() {
            assert!(
                cursor.starts_with("ccclone2."),
                "RMCP similar continuation must remain an authenticated clone cursor: {cursor}"
            );
            assert!(
                seen_cursors.insert(cursor.clone()),
                "RMCP similar repeated an authenticated continuation: {cursor}"
            );
            pending_cursors.push(cursor);
        }
    }
    assert!(
        saw_terminal_exact_family,
        "RMCP similar pagination must reach a complete conservative family"
    );
    assert!(
        saw_complete_coverage,
        "RMCP similar pagination must reach complete overall coverage"
    );
    assert!(
        saw_near_cursor,
        "RMCP similar pagination must observe a near continuation cursor"
    );
    assert!(
        saw_near_phase_cursor,
        "RMCP similar pagination must observe the authenticated NearStart phase cursor"
    );
    assert!(
        saw_near_result,
        "RMCP similar pagination must return at least one verified near result"
    );
    assert!(
        saw_near_complete,
        "RMCP similar pagination must reach complete near coverage"
    );
    assert!(
        page_count >= 2,
        "the repeated clone fixture must exercise at least one similar continuation"
    );
    assert!(
        seen_members.len() >= 2,
        "similar pagination must expose more than one unique family member"
    );

    let mut tampered_cursor = first_cursor.clone();
    let last = tampered_cursor
        .pop()
        .expect("similar cursor must have an encoded body");
    tampered_cursor.push(if last == '0' { '1' } else { '0' });
    let mut tampered_args = similar_args.clone();
    tampered_args["cursor"] = json!(tampered_cursor);
    let tampered_response = rmcp_test_support::call_json_tool(
        &home_path,
        &project_path,
        "tracedecay_similar",
        tampered_args,
    );
    assert_rmcp_similar_refused(&tampered_response, "tampered RMCP similar cursor");

    let mut mismatched_args = similar_args;
    mismatched_args["match_classes"] = json!(["rename_normalized_exact"]);
    mismatched_args["cursor"] = json!(first_cursor);
    let mismatched_response = rmcp_test_support::call_json_tool(
        &home_path,
        &project_path,
        "tracedecay_similar",
        mismatched_args,
    );
    assert_rmcp_similar_query_mismatch(
        &mismatched_response,
        "query-mismatched RMCP similar cursor",
    );
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
