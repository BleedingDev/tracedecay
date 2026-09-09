//! Host hook events: daemon notification over the broker connection.
//!
//! The wire metadata and event constructors are pure data and live in
//! [`tracedecay_hooks::core_events`]. Only delivery — which needs the daemon
//! connection, handshake, and preamble — remains root-coupled.

use std::path::Path;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::time::{Duration, Instant, timeout_at};
use tracedecay_hooks::core_events::{DaemonHookEvent, HOOK_EVENT_METHOD, HookEventNotifyOutcomeV1};

#[cfg(unix)]
use tracedecay_daemon_identity::connection_for_socket_path;
use tracedecay_daemon_identity::{DaemonConnection, current_daemon_connection};
#[cfg(unix)]
use tracedecay_daemon_protocol::SOCKET_ENV;

use super::{BrokerStream, JsonRpcRequest, write_daemon_preamble};

pub(crate) const HOOK_EVENT_NOTIFY_TIMEOUT: Duration = Duration::from_millis(750);

#[hotpath::measure(label = "daemon.engine.hooks.notify", future = true)]
pub async fn notify_hook_event(
    project_path: &Path,
    event: DaemonHookEvent,
) -> HookEventNotifyOutcomeV1 {
    let connection = {
        #[cfg(unix)]
        {
            std::env::var_os(SOCKET_ENV)
                .filter(|path| !path.is_empty())
                .map(|path| connection_for_socket_path(Path::new(&path)))
                .map_or_else(current_daemon_connection, Ok)
        }
        #[cfg(not(unix))]
        {
            current_daemon_connection()
        }
    };
    let Ok(connection) = connection else {
        return HookEventNotifyOutcomeV1::Unavailable;
    };
    notify_hook_event_to_connection(project_path, event, connection).await
}

#[hotpath::measure(label = "daemon.engine.hooks.deliver", future = true)]
async fn notify_hook_event_to_connection(
    project_path: &Path,
    event: DaemonHookEvent,
    connection: DaemonConnection,
) -> HookEventNotifyOutcomeV1 {
    let deadline = Instant::now() + HOOK_EVENT_NOTIFY_TIMEOUT;
    match timeout_at(
        deadline,
        deliver_hook_event_until(project_path, event, connection, deadline),
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(_) => HookEventNotifyOutcomeV1::TimedOut,
    }
}

async fn deliver_hook_event_until(
    project_path: &Path,
    event: DaemonHookEvent,
    connection: DaemonConnection,
    deadline: Instant,
) -> HookEventNotifyOutcomeV1 {
    let Ok(handshake) = crate::daemon::handshake_for_current_client(
        Some(project_path.to_path_buf()),
        None,
        false,
        false,
    ) else {
        return HookEventNotifyOutcomeV1::Malformed;
    };
    let Ok(params) = serde_json::to_value(event) else {
        return HookEventNotifyOutcomeV1::Malformed;
    };
    // Route publication is causally required by later session-bound tool calls.
    // Carry an id and wait for the daemon's post-processing acknowledgement;
    // closing a notification-only socket immediately after flush lets the
    // broker's peer-disconnect branch win before project binding and route store.
    let request_id = serde_json::json!("hook-event-route-v1");
    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: Some(request_id.clone()),
        method: HOOK_EVENT_METHOD.to_string(),
        params: Some(params),
    };
    let Ok(line) = serde_json::to_string(&request) else {
        return HookEventNotifyOutcomeV1::Malformed;
    };
    loop {
        if Instant::now() >= deadline {
            return HookEventNotifyOutcomeV1::TimedOut;
        }
        let Ok(stream) = BrokerStream::connect(&connection.endpoint).await else {
            return HookEventNotifyOutcomeV1::Unavailable;
        };
        let (reader, mut writer) = stream.into_owned_split();
        if write_daemon_preamble(&mut writer, &connection, &handshake)
            .await
            .is_err()
        {
            return HookEventNotifyOutcomeV1::Unavailable;
        }
        if writer.write_all(line.as_bytes()).await.is_err() {
            return HookEventNotifyOutcomeV1::Unavailable;
        }
        if writer.write_all(b"\n").await.is_err() || writer.flush().await.is_err() {
            return HookEventNotifyOutcomeV1::Unavailable;
        }

        let mut reader = BufReader::new(reader);
        let mut response_line = String::new();
        if reader.read_line(&mut response_line).await.is_err() || response_line.is_empty() {
            return HookEventNotifyOutcomeV1::Unavailable;
        }
        let Ok(response) = serde_json::from_str::<serde_json::Value>(&response_line) else {
            return HookEventNotifyOutcomeV1::Malformed;
        };
        if response.get("id") != Some(&request_id) {
            return HookEventNotifyOutcomeV1::Malformed;
        }
        if let Some(error) = response.get("error") {
            if response["jsonrpc"] != serde_json::json!("2.0")
                || response
                    .get("result")
                    .is_some_and(|result| !result.is_null())
                || error["code"] != serde_json::json!(-32603)
                || !error
                    .get("message")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(super::error_message_is_project_warming)
            {
                return HookEventNotifyOutcomeV1::Malformed;
            }
            // This correlated refusal is sent before hook dispatch. Reconnect
            // with identical bytes while the existing background open progresses;
            // transport errors or an uncertain acknowledgement are never retried.
            let Some(remaining) = deadline
                .checked_duration_since(Instant::now())
                .filter(|remaining| !remaining.is_zero())
            else {
                return HookEventNotifyOutcomeV1::TimedOut;
            };
            drop(reader);
            drop(writer);
            tokio::time::sleep(remaining.min(super::PROJECT_OPEN_RETRY_INTERVAL)).await;
            continue;
        }
        if response["result"]["processed"] != serde_json::json!(true) {
            return HookEventNotifyOutcomeV1::Malformed;
        }
        if writer.shutdown().await.is_err() {
            return HookEventNotifyOutcomeV1::Unavailable;
        }
        return HookEventNotifyOutcomeV1::Delivered;
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::time::Instant;

    use super::*;

    #[cfg(unix)]
    #[tokio::test]
    async fn missing_hook_socket_returns_typed_unavailable_without_retry_delay() {
        // Delivery builds the client handshake first, and that handshake reads
        // the registered product runtime for its truthful binary version. Every
        // real hook-notifying process registers one at its entry point; a
        // per-test process that does not would classify the socket outcome as
        // Malformed (no advertisable version) before it ever reaches the
        // connect this test covers.
        crate::product_runtime::register_fixture_product_runtime();
        let socket_dir = tempfile::tempdir().unwrap();
        let missing_socket = socket_dir.path().join("missing.sock");
        let connection = connection_for_socket_path(&missing_socket);
        let started = Instant::now();

        let outcome = notify_hook_event_to_connection(
            socket_dir.path(),
            DaemonHookEvent::cursor_after_shell_execution(socket_dir.path().to_path_buf()),
            connection,
        )
        .await;

        assert_eq!(outcome, HookEventNotifyOutcomeV1::Unavailable);
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "a missing socket must not consume the outer hook timeout"
        );
    }

    async fn notify_with_scripted_responses(
        replies: Vec<Option<serde_json::Value>>,
    ) -> (HookEventNotifyOutcomeV1, Vec<String>) {
        crate::product_runtime::register_fixture_product_runtime();
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("hook.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let (stop, mut stopping) = tokio::sync::oneshot::channel::<()>();
        let broker = tokio::spawn(async move {
            let mut requests = Vec::new();
            loop {
                let stream = tokio::select! {
                    biased;
                    _ = &mut stopping => break,
                    accepted = listener.accept() => accepted.unwrap().0,
                };
                let (reader, mut writer) = stream.into_split();
                let mut reader = BufReader::new(reader);
                let mut handshake = String::new();
                if reader.read_line(&mut handshake).await.unwrap() == 0 {
                    continue;
                }
                crate::daemon::DaemonHandshake::from_line(handshake.trim()).unwrap();
                let mut request = String::new();
                if reader.read_line(&mut request).await.unwrap() == 0 {
                    continue;
                }
                requests.push(request);
                let reply = &replies[(requests.len() - 1).min(replies.len() - 1)];
                let Some(reply) = reply else {
                    continue;
                };
                let mut bytes = serde_json::to_vec(reply).unwrap();
                bytes.push(b'\n');
                let _ = writer.write_all(&bytes).await;
                let _ = writer.shutdown().await;
                // A processed reply leaves the peer read half alive until the
                // client's shutdown completes; otherwise the fake broker can
                // manufacture BrokenPipe after a successfully received ACK.
                if reply["result"]["processed"] == serde_json::json!(true) {
                    let mut eof = String::new();
                    tokio::select! {
                        biased;
                        _ = &mut stopping => break,
                        _ = reader.read_line(&mut eof) => {},
                    }
                }
            }
            requests
        });
        let connection = connection_for_socket_path(&socket);
        assert!(
            connection.auth_token.is_none(),
            "fixture preamble has no auth"
        );
        let outcome = notify_hook_event_to_connection(
            directory.path(),
            DaemonHookEvent::cursor_after_shell_execution(directory.path().to_path_buf()),
            connection,
        )
        .await;
        let _ = stop.send(());
        let requests = broker.await.unwrap();
        (outcome, requests)
    }

    fn warming_response() -> serde_json::Value {
        serde_json::json!({
            "jsonrpc": "2.0", "id": "hook-event-route-v1",
            "error": { "code": -32603, "message": format!(
                "TraceDecay project fixture {}", super::super::PROJECT_WARMING_RETRY_HINT
            ) },
        })
    }

    #[tokio::test]
    async fn correlated_warming_retries_identical_hook_until_processed() {
        let (outcome, requests) = notify_with_scripted_responses(vec![
            Some(warming_response()),
            Some(serde_json::json!({
                "jsonrpc": "2.0", "id": "hook-event-route-v1",
                "result": { "processed": true },
            })),
        ])
        .await;
        assert_eq!(outcome, HookEventNotifyOutcomeV1::Delivered);
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0], requests[1],
            "warming retry must preserve event identity"
        );
    }

    #[tokio::test]
    async fn repeated_warming_spends_only_the_original_hook_deadline() {
        let started = Instant::now();
        let (outcome, requests) = tokio::time::timeout(
            Duration::from_secs(2),
            notify_with_scripted_responses(vec![Some(warming_response())]),
        )
        .await
        .expect("warming must remain bounded by the hook deadline");
        assert_eq!(outcome, HookEventNotifyOutcomeV1::TimedOut);
        assert!(
            requests.len() > 1,
            "warming should retry before the deadline"
        );
        assert!(started.elapsed() >= HOOK_EVENT_NOTIFY_TIMEOUT);
        assert!(requests.windows(2).all(|pair| pair[0] == pair[1]));
    }

    #[tokio::test]
    async fn lost_hook_acknowledgement_never_retries() {
        let (outcome, requests) = notify_with_scripted_responses(vec![None]).await;
        assert_eq!(outcome, HookEventNotifyOutcomeV1::Unavailable);
        assert_eq!(requests.len(), 1);
    }

    #[tokio::test]
    async fn fatal_or_mismatched_hook_response_never_retries() {
        let mut mismatched = warming_response();
        mismatched["id"] = serde_json::json!("another-request");
        let mut dual_result = warming_response();
        dual_result["result"] = serde_json::json!({ "processed": true });
        let mut wrong_code = warming_response();
        wrong_code["error"]["code"] = serde_json::json!(-32600);
        let mut wrong_version = warming_response();
        wrong_version["jsonrpc"] = serde_json::json!("1.0");
        for response in [
            mismatched,
            dual_result,
            wrong_code,
            wrong_version,
            serde_json::json!({
                "jsonrpc": "2.0", "id": "hook-event-route-v1",
                "error": { "code": -32603, "message": "project identity refused" },
            }),
            serde_json::json!({
                "jsonrpc": "2.0", "id": "hook-event-route-v1",
                "result": { "processed": false },
            }),
        ] {
            let (outcome, requests) = notify_with_scripted_responses(vec![Some(response)]).await;
            assert_eq!(outcome, HookEventNotifyOutcomeV1::Malformed);
            assert_eq!(requests.len(), 1);
        }
    }
}
