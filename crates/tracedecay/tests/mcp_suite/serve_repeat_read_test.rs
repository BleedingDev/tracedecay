//! Repeated identical reads in one `tracedecay serve` session: a pointer for
//! an unchanged answer, the full new content after an edit, and full answers
//! when the host has not marked the connection as one agent's.

use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::TempDir;

use crate::common;
use crate::serve_harness::init_project_with_file;
use crate::serve_tool_surface_test::PlainMcpClient;

const POINTER: &str = "tracedecay_repeat_read:";
const REPEAT_READ_POINTERS_ENV: &str = "TRACEDECAY_MCP_REPEAT_READ_POINTERS";

/// One function whose body alone is well over the pointer threshold, beside
/// enough small functions that the file outline is too.
fn source(marker: &str) -> String {
    let body = (0..60)
        .map(|index| format!("    let value_{index} = {index};\n"))
        .collect::<String>();
    let helpers = (0..6)
        .map(|index| format!("pub fn repeat_read_helper_{index}() {{}}\n"))
        .collect::<String>();
    format!("pub fn {marker}() -> usize {{\n{body}    0\n}}\n{helpers}")
}

fn call(id: i64, name: &str, arguments: &Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "tools/call",
        "params": { "name": name, "arguments": arguments }
    })
}

fn text(response: &Value) -> String {
    let content = response["result"]["content"]
        .as_array()
        .unwrap_or_else(|| panic!("tools/call carried no content: {response}"));
    content
        .iter()
        .filter_map(|block| block["text"].as_str())
        .collect()
}

fn is_pointer(response: &Value) -> bool {
    response["result"]["content"][0]["text"]
        .as_str()
        .is_some_and(|text| text.starts_with(POINTER))
}

fn json_body(response: &Value) -> Value {
    response["result"]["content"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|block| block["text"].as_str())
        .find_map(|text| serde_json::from_str::<Value>(text).ok())
        .unwrap_or_else(|| panic!("no JSON body in {response}"))
}

fn start(client: &mut PlainMcpClient) {
    let initialize = client.request(json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": { "name": "repeat-read-test", "version": "1" }
        }
    }));
    assert!(initialize.get("error").is_none(), "{initialize}");
    client.send(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }));
}

fn wait_ready(client: &mut PlainMcpClient, id: i64) {
    let status = client.request(call(
        id,
        "tracedecay_status",
        &json!({
            "format": "json",
            "wait_for": { "state": "ready", "timeout_ms": 110_000 },
        }),
    ));
    let status = json_body(&status);
    assert_eq!(
        status["wait"],
        json!({ "outcome": "reached" }),
        "code index never became ready: {status}"
    );
}

fn exact_symbol(client: &mut PlainMcpClient, id: i64, name: &str) -> Value {
    json_body(&client.request(call(
        id,
        "tracedecay_find_exact_symbol",
        &json!({ "name": name, "format": "json" }),
    )))
}

/// Polls an exact-symbol read, a different call from any the test repeats,
/// until the daemon serves `name` from a raw worktree write.
fn wait_until_indexed(client: &mut PlainMcpClient, name: &str) {
    let deadline = Instant::now() + Duration::from_secs(90);
    for id in 1_000.. {
        let found = exact_symbol(client, id, name);
        if found["matches"]
            .as_array()
            .is_some_and(|matches| !matches.is_empty())
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the daemon never indexed {name}: {found}"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}

fn provenance(response: &Value) -> String {
    text(response)
        .lines()
        .find(|line| line.contains("Provenance:"))
        .unwrap_or_else(|| panic!("source_body must render provenance: {response}"))
        .to_owned()
}

#[tokio::test]
async fn serve_points_repeat_reads_at_the_earlier_result_until_content_changes() {
    let home = TempDir::new().unwrap();
    let project = init_project_with_file(home.path(), &source("repeat_read_marker")).await;
    let _daemon = common::spawn_tracedecay_daemon(home.path());
    let mut client = PlainMcpClient::start_with_env(
        home.path(),
        project.path(),
        &[(REPEAT_READ_POINTERS_ENV, "1")],
    );
    start(&mut client);
    wait_ready(&mut client, 2);

    let lookup = exact_symbol(&mut client, 3, "repeat_read_marker");
    let node_id = lookup["matches"][0]["id"]
        .as_str()
        .unwrap_or_else(|| panic!("exact lookup must return a node id: {lookup}"))
        .to_owned();
    let body = json!({ "node_id": node_id });

    let first = client.request(call(10, "tracedecay_source_body", &body));
    assert!(
        !is_pointer(&first) && text(&first).contains("let value_59 = 59;"),
        "the first read is the full body: {first}"
    );
    let second = client.request(call(11, "tracedecay_source_body", &body));
    assert!(
        is_pointer(&second),
        "an identical unchanged read must answer with a pointer: {second}"
    );
    assert!(
        text(&second).contains("tracedecay_source_body"),
        "the pointer names the tool: {second}"
    );
    let third = client.request(call(12, "tracedecay_source_body", &body));
    assert!(
        !is_pointer(&third) && text(&third).contains("let value_59 = 59;"),
        "the call after a pointer must serve the full body again: {third}"
    );
    assert_ne!(
        provenance(&first),
        provenance(&third),
        "identical answers still differ in per-call provenance, so the pointer must not \
         depend on rendered text"
    );

    let outline = json!({ "file": "src/lib.rs" });
    let before_edit = client.request(call(20, "tracedecay_source_outline", &outline));
    assert!(
        !is_pointer(&before_edit) && text(&before_edit).contains("repeat_read_marker"),
        "{before_edit}"
    );
    std::fs::write(
        project.path().join("src/lib.rs"),
        source("repeat_read_edited"),
    )
    .unwrap();
    wait_until_indexed(&mut client, "repeat_read_edited");
    let after_edit = client.request(call(22, "tracedecay_source_outline", &outline));
    assert!(
        !is_pointer(&after_edit),
        "a read after an edit must never point at the stale answer: {after_edit}"
    );
    assert!(
        text(&after_edit).contains("repeat_read_edited")
            && !text(&after_edit).contains("repeat_read_marker"),
        "the read after an edit must carry the new content: {after_edit}"
    );
    let repeat = client.request(call(23, "tracedecay_source_outline", &outline));
    assert!(
        is_pointer(&repeat),
        "the unchanged repeat after the edit must answer with a pointer: {repeat}"
    );
}

#[tokio::test]
async fn serve_without_the_pointer_env_answers_every_repeat_in_full() {
    let home = TempDir::new().unwrap();
    let project = init_project_with_file(home.path(), &source("repeat_read_default")).await;
    let _daemon = common::spawn_tracedecay_daemon(home.path());
    let mut client = PlainMcpClient::start_with_env(home.path(), project.path(), &[]);
    start(&mut client);
    wait_ready(&mut client, 2);
    let outline = json!({ "file": "src/lib.rs" });
    let first = client.request(call(3, "tracedecay_source_outline", &outline));
    let repeat = client.request(call(4, "tracedecay_source_outline", &outline));
    assert!(
        text(&first).contains("repeat_read_default"),
        "the outline must answer: {first}"
    );
    assert!(
        !is_pointer(&repeat),
        "a host that did not mark its connection as one agent's must always get full answers, \
         since it may share the connection across agents: {repeat}"
    );
}
