#![cfg(feature = "test-transport")]

//! Production-composition code-search journey.
//!
//! This deliberately enters through the harness JSON-RPC call boundary. The
//! response therefore crosses the MCP server, the installed code-index
//! authority/scheduler, and the generation-bound published display reader.

use crate::support::{
    ProductionCompositionFixture, production_composition_fixture_with_sources,
    warm_code_index_search,
};
use serde_json::{Value, json};
use std::fs;
use std::path::Path;

const QUERY: &str = "alpha cursor fixture";

fn write_cursor_fixture_sources(project: &Path) {
    fs::create_dir_all(project.join("src")).expect("code-search fixture src directory");
    fs::write(
        project.join("src/alpha_cursor_fixture_primary.rs"),
        r#"
/// The alpha primary cursor fixture has repeated cursor and fixture evidence.
pub fn alpha_cursor_fixture_primary() -> u32 {
    let alpha_cursor_fixture_primary_value = 1;
    alpha_cursor_fixture_primary_value
}
"#,
    )
    .expect("code-search primary fixture");
    fs::write(
        project.join("src/cursor_fixture_secondary.rs"),
        r#"
/// The secondary cursor fixture has alpha cursor and fixture evidence.
pub fn fixture_secondary() -> u32 {
    let alpha_cursor_fixture_secondary_value = 2;
    alpha_cursor_fixture_secondary_value
}
"#,
    )
    .expect("code-search secondary fixture");
    fs::write(
        project.join("src/cursor_fixture_distractor.rs"),
        r#"
/// This alpha cursor fixture is a lower-ranked distractor.
pub fn fixture_distractor() -> u32 {
    3
}
"#,
    )
    .expect("code-search distractor fixture");
}

async fn search_page(fixture: &ProductionCompositionFixture, arguments: Value) -> Value {
    let response = fixture
        .harness
        .call_tool(&fixture.project_root, "tracedecay_search", arguments)
        .await
        .expect("production MCP search invocation");
    assert!(
        response.error.is_none(),
        "production MCP search returned a protocol error: {:?}",
        response.error
    );
    let result = response
        .result
        .expect("production MCP search result envelope");
    let text = result["content"][0]["text"]
        .as_str()
        .expect("production MCP search text");
    serde_json::from_str(text).expect("production MCP search JSON payload")
}

fn result_name(result: &Value) -> &str {
    result["display"]["name"]
        .as_str()
        .or_else(|| result["candidate"]["anchor_id"].as_str())
        .expect("search result display name or anchor")
}

fn result_anchor(result: &Value) -> &str {
    result["candidate"]["anchor_id"]
        .as_str()
        .expect("search result candidate anchor")
}

fn results(payload: &Value) -> &[Value] {
    payload["results"]
        .as_array()
        .expect("search payload results array")
}

fn assert_serving_provenance(payload: &Value, generation: &str) {
    assert_eq!(
        payload["code_generation"].as_str(),
        Some(generation),
        "search page must report the generation that answered: {payload}"
    );
    assert!(
        payload["query_fallback_digest"].as_str().is_some(),
        "search page must retain the authenticated fallback provenance: {payload}"
    );
    assert_eq!(payload["coverage"]["recall"], json!("full"));
    assert!(
        !results(payload).is_empty(),
        "a warmed fixture search must return candidates: {payload}"
    );
    for result in results(payload) {
        assert!(
            result["display"]["path"].as_str().is_some(),
            "published display hydration must include a logical path: {result}"
        );
        assert!(
            result["display"]["name"].as_str().is_some(),
            "published display hydration must include a symbol name: {result}"
        );
        assert!(
            result["display"]["qualified_name"].as_str().is_some(),
            "published display hydration must include a qualified symbol name: {result}"
        );
        assert!(
            result["candidate"]["occurrences"]
                .as_array()
                .is_some_and(|occurrences| !occurrences.is_empty()),
            "ranked candidates must carry occurrence provenance: {result}"
        );
    }
}

fn assert_cursor_refusal(payload: &Value, expected_reason: &str) {
    assert_eq!(payload["status"], json!("unavailable"), "{payload}");
    assert_eq!(payload["reason"], json!(expected_reason), "{payload}");
    assert_eq!(payload["results"], json!([]), "{payload}");
    assert!(
        payload["coverage"]["recall"].as_str().is_some(),
        "typed refusal must still expose lane coverage: {payload}"
    );
}

#[tokio::test]
async fn production_search_pages_authenticated_union_and_refuses_invalid_scope() {
    let fixture = production_composition_fixture_with_sources(write_cursor_fixture_sources).await;
    let server = fixture
        .harness
        .server(&fixture.project_root)
        .expect("production code-search server");
    warm_code_index_search(server.as_ref(), QUERY).await;

    let status_response = fixture
        .harness
        .call_tool(
            &fixture.project_root,
            "tracedecay_status",
            json!({
                "include_branch_diagnostics": false,
                "include_storage_health": false,
                "include_session_ingest": false,
                "include_staleness": false,
                "format": "json",
            }),
        )
        .await
        .expect("production MCP status invocation");
    assert!(
        status_response.error.is_none(),
        "status response: {status_response:?}"
    );
    let status_result = status_response.result.expect("status result envelope");
    let status_text = status_result["content"][0]["text"]
        .as_str()
        .expect("status text");
    let status: Value = serde_json::from_str(status_text).expect("status JSON payload");
    let generation = status["code_index_freshness"]["worktree"]["latest_generation_id"]
        .as_str()
        .expect("status generation provenance")
        .to_owned();

    let first = search_page(
        &fixture,
        json!({"query": QUERY, "limit": 1, "format": "json"}),
    )
    .await;
    assert_serving_provenance(&first, &generation);
    assert_eq!(
        results(&first).len(),
        1,
        "the fixture must force one result per page"
    );
    let first_name = result_name(&results(&first)[0]).to_owned();
    let first_anchor = result_anchor(&results(&first)[0]).to_owned();
    assert_eq!(
        first_name, "alpha_cursor_fixture_primary",
        "primary lexical result should lead"
    );
    assert_eq!(
        results(&first)[0]["display"]["path"],
        json!("src/alpha_cursor_fixture_primary.rs"),
        "the first result must hydrate from its published declaration artifact"
    );

    let first_cursor = first["next_cursor"]
        .as_str()
        .expect("first page must expose an authenticated continuation")
        .to_owned();
    let cursor_object: Value =
        serde_json::from_str(&first_cursor).expect("next_cursor must be typed JSON");
    for field in [
        "key_id",
        "profile_id",
        "snapshot_digest",
        "freshness_digest",
        "candidate_set_digest",
        "authorization_revision",
        "ranking_revision",
        "signature",
    ] {
        assert!(
            cursor_object[field].is_string(),
            "authenticated cursor must expose typed {field} binding: {cursor_object}"
        );
    }
    assert!(cursor_object["query_digest"]["privacy_domain"].is_string());
    assert!(cursor_object["query_digest"]["mac"].is_string());
    assert_eq!(cursor_object["next_ordinal"], json!(1));
    let candidate_set_digest = cursor_object["candidate_set_digest"].clone();

    let second = search_page(
        &fixture,
        json!({
            "query": QUERY,
            "limit": 1,
            "cursor": first_cursor,
            "format": "json",
        }),
    )
    .await;
    assert_serving_provenance(&second, &generation);
    assert_eq!(
        results(&second).len(),
        1,
        "second page must also be bounded"
    );
    assert_ne!(
        result_name(&results(&second)[0]),
        first_name,
        "cursor continuation must advance through the frozen set"
    );
    let second_anchor = result_anchor(&results(&second)[0]).to_owned();
    assert_ne!(
        second_anchor, first_anchor,
        "cursor continuation must advance to a new candidate anchor"
    );
    if let Some(second_cursor) = second["next_cursor"].as_str() {
        let second_cursor: Value =
            serde_json::from_str(second_cursor).expect("second next_cursor must be typed JSON");
        assert_eq!(
            second_cursor["candidate_set_digest"], candidate_set_digest,
            "cursor pages must stay bound to one frozen candidate set"
        );
    }

    let mut all_names = vec![
        first_name.clone(),
        result_name(&results(&second)[0]).to_owned(),
    ];
    let mut all_anchors = vec![first_anchor, second_anchor];
    let mut cursor = second["next_cursor"].as_str().map(str::to_owned);
    while let Some(cursor_value) = cursor.take() {
        let page = search_page(
            &fixture,
            json!({
                "query": QUERY,
                "limit": 1,
                "cursor": cursor_value,
                "format": "json",
            }),
        )
        .await;
        assert_serving_provenance(&page, &generation);
        let name = result_name(&results(&page)[0]).to_owned();
        let anchor = result_anchor(&results(&page)[0]).to_owned();
        assert!(
            !all_anchors.contains(&anchor),
            "paged union must not duplicate a candidate anchor: {all_anchors:?} then {anchor}"
        );
        all_anchors.push(anchor);
        assert!(
            !all_names.contains(&name),
            "paged union must not duplicate a candidate: {all_names:?} then {name}"
        );
        all_names.push(name);
        cursor = page["next_cursor"].as_str().map(str::to_owned);
    }
    assert!(
        all_names.iter().any(|name| name == "fixture_secondary"),
        "paged union must include the secondary fixture: {all_names:?}"
    );
    assert!(
        all_names.iter().any(|name| name == "fixture_distractor"),
        "paged union must include the distractor fixture: {all_names:?}"
    );
    assert_eq!(
        all_names
            .iter()
            .position(|name| name == "fixture_distractor"),
        Some(all_names.len() - 1),
        "the lower-ranked distractor must remain after the focused symbols: {all_names:?}"
    );

    let mut tampered_cursor = cursor_object;
    tampered_cursor["signature"] =
        json!("hmac-sha256:0000000000000000000000000000000000000000000000000000000000000000");
    let tampered = search_page(
        &fixture,
        json!({
            "query": QUERY,
            "limit": 1,
            "cursor": serde_json::to_string(&tampered_cursor).expect("tampered cursor JSON"),
            "format": "json",
        }),
    )
    .await;
    assert_cursor_refusal(&tampered, "search_failed");

    let binding_mismatch = search_page(
        &fixture,
        json!({
            "query": "different fixture query",
            "limit": 1,
            "cursor": first["next_cursor"].clone(),
            "format": "json",
        }),
    )
    .await;
    assert_cursor_refusal(&binding_mismatch, "search_failed");

    let wrong_scope = fixture
        .harness
        .call_tool(
            &fixture.project_root,
            "tracedecay_search",
            json!({
                "query": QUERY,
                "project_selector": {"project_id": "project.foreign-code-search"},
                "format": "json",
            }),
        )
        .await
        .expect("wrong-scope MCP invocation");
    let error = wrong_scope.error.expect("wrong-scope search refusal");
    assert!(
        wrong_scope.result.is_none(),
        "wrong-scope search must not answer"
    );
    assert!(
        error.message.contains("does not accept project selectors"),
        "wrong-scope search must refuse before retrieval: {error:?}"
    );

    fixture.harness.shutdown().await;
}
