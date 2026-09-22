//! Small authenticated RMCP client used by the real-daemon CLI journeys.
//!
//! The CLI command path deliberately starts with `tools/call`, which exercises
//! the daemon's legacy replay transport. These tests need a second client that
//! performs the MCP initialize handshake first so the daemon routes the same
//! call through its production RMCP adapter.

use std::{
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use serde_json::{Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tracedecay_daemon_protocol::{
    DaemonClientIdentity, DaemonHandshake, MovedStoreAdoption, connect_to_daemon_connection,
    write_daemon_preamble,
};

static NEXT_CLIENT_INSTANCE: AtomicU64 = AtomicU64::new(1);

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(120);

/// Invoke one JSON-formatted MCP tool over a fresh, authenticated RMCP route.
pub fn call_json_tool(home: &Path, project: &Path, tool_name: &str, arguments: Value) -> Value {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build RMCP test runtime");
    runtime.block_on(call_json_tool_async(home, project, tool_name, arguments))
}

async fn call_json_tool_async(
    home: &Path,
    project: &Path,
    tool_name: &str,
    arguments: Value,
) -> Value {
    let socket_path = crate::common::daemon_socket_path(home);
    let resolved = tracedecay_daemon_identity::connection_for_socket_path(&socket_path);
    assert!(
        resolved.auth_token.is_some(),
        "real-daemon RMCP tests must resolve the daemon's authenticated authority"
    );
    let connection = resolved.into_protocol();
    let instance = NEXT_CLIENT_INSTANCE.fetch_add(1, Ordering::Relaxed);
    let handshake = DaemonHandshake {
        project_path: Some(project.to_path_buf()),
        scope_prefix: None,
        timings: false,
        allow_init: false,
        allow_initialize_root_routing: false,
        client_identity: DaemonClientIdentity::new(
            home.join(".tracedecay"),
            home.join(".tracedecay/global.db"),
        ),
        client_version: env!("CARGO_PKG_VERSION").to_owned(),
        client_instance_id: format!("core-cli-rmcp-{}-{instance}", std::process::id()),
        tool_list_changed_capable: false,
        catalog_version: String::new(),
        moved_store_adoption: MovedStoreAdoption::Never,
    };

    let stream = connect_to_daemon_connection(&connection)
        .await
        .expect("connect authenticated RMCP test client");
    let (reader, mut writer) = stream.into_split();
    write_daemon_preamble(&mut writer, &connection, &handshake)
        .await
        .expect("write authenticated RMCP test preamble");

    write_json_line(
        &mut writer,
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {
                    "name": "tracedecay-core-cli-rmcp-test",
                    "version": env!("CARGO_PKG_VERSION"),
                },
            },
        }),
    )
    .await;
    let mut reader = BufReader::new(reader);
    let initialized = read_response(&mut reader, 1, "RMCP initialize response").await;
    assert_eq!(initialized["jsonrpc"], json!("2.0"), "{initialized}");
    assert_eq!(initialized["id"], json!(1), "{initialized}");
    assert!(
        initialized.get("error").is_none_or(Value::is_null),
        "RMCP initialize must succeed: {initialized}"
    );
    assert!(
        initialized["result"].is_object(),
        "RMCP initialize must return a result: {initialized}"
    );
    assert!(
        initialized["result"]["protocolVersion"].is_string(),
        "RMCP initialize must negotiate a protocol version: {initialized}"
    );
    assert!(
        initialized["result"]["serverInfo"].is_object(),
        "RMCP initialize must identify the daemon server: {initialized}"
    );

    // A standards-compliant client acknowledges initialization before issuing
    // tools/call. This notification has no response id and read_response below
    // intentionally skips such asynchronous frames.
    write_json_line(
        &mut writer,
        &json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
    )
    .await;
    write_json_line(
        &mut writer,
        &json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": arguments,
            },
        }),
    )
    .await;
    let response = read_response(&mut reader, 2, "RMCP tools/call response").await;
    writer.shutdown().await.expect("close RMCP test client");
    response
}

async fn write_json_line<W>(writer: &mut W, value: &Value)
where
    W: AsyncWrite + Unpin,
{
    writer
        .write_all(value.to_string().as_bytes())
        .await
        .expect("write RMCP JSON request");
    writer
        .write_all(b"\n")
        .await
        .expect("write RMCP JSON newline");
    writer.flush().await.expect("flush RMCP JSON request");
}

async fn read_response<R>(reader: &mut R, request_id: u64, description: &str) -> Value
where
    R: AsyncBufRead + Unpin,
{
    loop {
        let mut line = String::new();
        let bytes = tokio::time::timeout(RESPONSE_TIMEOUT, reader.read_line(&mut line))
            .await
            .unwrap_or_else(|_| panic!("{description} timed out"))
            .unwrap_or_else(|error| panic!("{description} failed: {error}"));
        assert_ne!(bytes, 0, "{description}: daemon closed the RMCP stream");
        let value: Value = serde_json::from_str(line.trim())
            .unwrap_or_else(|error| panic!("{description} was not JSON: {error}; line={line:?}"));
        if value["id"].as_u64() == Some(request_id) {
            return value;
        }
    }
}

/// Extract the JSON document carried in a successful MCP tool result.
pub fn tool_payload(response: &Value) -> Value {
    try_tool_payload(response)
        .unwrap_or_else(|| panic!("RMCP tool result omitted a JSON text payload: {response}"))
}

/// Extract a JSON tool payload when a poll should retry a transient refusal.
pub fn try_tool_payload(response: &Value) -> Option<Value> {
    if response.get("error").is_some_and(|error| !error.is_null()) {
        return None;
    }
    response["result"]["content"]
        .as_array()?
        .iter()
        .filter_map(|item| item.get("text").and_then(Value::as_str))
        .find_map(|text| serde_json::from_str(text).ok())
}
