#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
#![doc = "Process-level acceptance tests for the bounded NCM worker transport."]

use serde_json::{Value, json};
use std::io::Write;
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tracedecay_memory_ncm_core::types::SourceId;
use tracedecay_memory_ncm_runtime::client::{ClientError, WorkerClient, WorkerOptions};
use tracedecay_memory_ncm_runtime::engine::{ObserveRequest, Outcome};
use tracedecay_memory_ncm_runtime::ports::Deadline;
use tracedecay_memory_ncm_runtime::wire::{
    self, MAX_REQUEST_BYTES, Operation, PROTOCOL_VERSION, Reply, Request,
};

const BINARY: &str = env!("CARGO_BIN_EXE_tracedecay-ncm-worker");
const CALL_DEADLINE: Duration = Duration::from_secs(5);

fn namespace(index: u8) -> String {
    format!("{index:02x}{}", "0".repeat(62))
}

fn options() -> WorkerOptions {
    WorkerOptions {
        test_double: true,
        reconciliation_deadline: CALL_DEADLINE,
        ..WorkerOptions::default()
    }
}

fn client(root: &TempDir) -> WorkerClient {
    WorkerClient::spawn(BINARY, root.path(), options()).expect("worker client starts")
}

fn observe_payload(idempotency_key: &str, key: &str, value: &str) -> Value {
    let mut request = ObserveRequest {
        idempotency_key: idempotency_key.to_owned(),
        payload_sha256: String::new(),
        source: SourceId(format!("source-{idempotency_key}")),
        key_text: key.to_owned(),
        value_text: value.to_owned(),
        affect: None,
        surprise: 0.4,
        intensity: 1.0,
        provenance: json!({"origin": "worker-test"}),
        deadline: Deadline {
            remaining_ms: u64::MAX,
        },
    };
    request.payload_sha256 = request
        .canonical_payload_sha256()
        .expect("canonical observe payload serializes");
    json!({
        "idempotency_key": request.idempotency_key,
        "payload_sha256": request.payload_sha256,
        "source": request.source.0,
        "key_text": request.key_text,
        "value_text": request.value_text,
        "affect": request.affect,
        "surprise": request.surprise,
        "intensity": request.intensity,
        "provenance": request.provenance
    })
}

fn raw_worker(root: &Path) -> (Child, ChildStdin, ChildStdout) {
    let mut child = Command::new(BINARY)
        .arg("--state-root")
        .arg(root)
        .arg("--test-double")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("raw worker starts");
    let stdin = child.stdin.take().expect("worker stdin is piped");
    let stdout = child.stdout.take().expect("worker stdout is piped");
    (child, stdin, stdout)
}

fn write_request(stdin: &mut ChildStdin, request: &Request) {
    stdin
        .write_all(&wire::encode_request(request).expect("request encodes"))
        .expect("request writes");
    stdin.flush().expect("request flushes");
}

fn read_reply(stdout: &mut ChildStdout) -> Reply {
    wire::read_reply(stdout)
        .expect("reply frame is valid")
        .expect("reply exists")
}

fn process_exists(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

#[test]
fn handshake_observe_and_recall_round_trip() {
    let root = TempDir::new().expect("temp root");
    let client = client(&root);
    let ns = namespace(1);

    let handshake = client
        .call(
            Request::new(1, 0, Operation::Handshake, &ns, json!({})),
            CALL_DEADLINE,
        )
        .expect("handshake succeeds");
    assert_eq!(handshake.outcome, Outcome::Success);
    assert_eq!(handshake.payload.as_ref().unwrap()["ready"], true);
    let health = client
        .call(
            Request::new(4, 0, Operation::Health, "", json!({})),
            CALL_DEADLINE,
        )
        .expect("health succeeds");
    assert_eq!(health.payload.as_ref().unwrap()["process_alive"], true);
    assert_eq!(health.payload.as_ref().unwrap()["encoder_ready"], true);

    let observe = client
        .call(
            Request::new(
                2,
                0,
                Operation::Observe,
                &ns,
                observe_payload("round-trip", "garden bird", "a robin arrived"),
            ),
            CALL_DEADLINE,
        )
        .expect("observe succeeds");
    assert_eq!(observe.outcome, Outcome::Success);
    assert_eq!(observe.payload.as_ref().unwrap()["replayed"], false);

    let recall = client
        .call(
            Request::new(
                3,
                0,
                Operation::Recall,
                &ns,
                json!({"query_text": "garden bird", "top_k": 4}),
            ),
            CALL_DEADLINE,
        )
        .expect("recall succeeds");
    assert_eq!(recall.outcome, Outcome::Success);
    assert!(recall.payload.is_some());
}

#[test]
fn oversized_header_is_rejected_before_body_allocation_and_worker_continues() {
    let root = TempDir::new().expect("temp root");
    let (mut child, mut stdin, mut stdout) = raw_worker(root.path());
    let oversized = u32::try_from(MAX_REQUEST_BYTES + 1).expect("bound fits u32");
    stdin
        .write_all(&oversized.to_be_bytes())
        .expect("oversized header writes");
    stdin.flush().expect("header flushes");

    let rejected = read_reply(&mut stdout);
    assert_eq!(rejected.id, 0);
    assert_eq!(rejected.error.as_ref().unwrap().kind, "oversized_frame");

    write_request(
        &mut stdin,
        &Request::new(9, 1000, Operation::Health, "", json!({})),
    );
    let health = read_reply(&mut stdout);
    assert_eq!(health.id, 9);
    assert_eq!(health.outcome, Outcome::Success);
    assert!(child.try_wait().expect("worker status").is_none());
    drop(stdin);
    assert!(child.wait().expect("worker exits").success());
}

#[test]
fn malformed_json_gets_typed_error_and_next_request_works() {
    let root = TempDir::new().expect("temp root");
    let (mut child, mut stdin, mut stdout) = raw_worker(root.path());
    let malformed = b"{not-json";
    stdin
        .write_all(&(malformed.len() as u32).to_be_bytes())
        .expect("malformed length writes");
    stdin.write_all(malformed).expect("malformed body writes");
    stdin.flush().expect("malformed frame flushes");
    let rejected = read_reply(&mut stdout);
    assert_eq!(rejected.error.as_ref().unwrap().kind, "malformed_json");

    write_request(
        &mut stdin,
        &Request::new(10, 1000, Operation::Health, "", json!({})),
    );
    assert_eq!(read_reply(&mut stdout).outcome, Outcome::Success);
    drop(stdin);
    assert!(child.wait().expect("worker exits").success());
}

#[test]
fn deadline_kills_mutating_worker_and_next_call_respawns() {
    let root = TempDir::new().expect("temp root");
    let client = client(&root);
    let ns = namespace(2);
    let mut payload = json!({
        "idempotency_key": "slow-maintenance",
        "kind": {"advance": {"ticks": 10_000}}
    });
    payload["test_sleep_before_ms"] = json!(1000);
    let started = Instant::now();
    let result = client.call(
        Request::new(20, 0, Operation::Maintenance, &ns, payload),
        Duration::from_millis(30),
    );
    assert_eq!(result, Err(ClientError::EffectUnknown { op_id: 20 }));
    assert!(started.elapsed() <= Duration::from_millis(300));
    assert_eq!(client.pid(), None);

    let health = client
        .call(
            Request::new(21, 0, Operation::Health, "", json!({})),
            CALL_DEADLINE,
        )
        .expect("next call respawns");
    assert_eq!(health.outcome, Outcome::Success);
    assert!(client.pid().is_some());
}

#[test]
fn observe_killed_after_commit_reconciles_without_second_record() {
    let root = TempDir::new().expect("temp root");
    let client = client(&root);
    let ns = namespace(3);
    let mut payload = observe_payload("reconcile-observe", "stable key", "stable value");
    payload["test_sleep_after_commit_ms"] = json!(1000);
    let result = client.call(
        Request::new(30, 0, Operation::Observe, &ns, payload),
        Duration::from_millis(150),
    );
    assert_eq!(result, Err(ClientError::EffectUnknown { op_id: 30 }));

    let replay = client
        .reconcile_unknown("reconcile-observe")
        .expect("receipt replay succeeds after restart");
    assert_eq!(replay.outcome, Outcome::Success);
    assert_eq!(replay.payload.as_ref().unwrap()["replayed"], true);

    let inspection = client
        .call(
            Request::new(31, 0, Operation::Inspection, &ns, json!({})),
            CALL_DEADLINE,
        )
        .expect("inspection succeeds");
    assert_eq!(inspection.payload.as_ref().unwrap()["records"], 1);
}

#[test]
fn bounded_mailbox_returns_busy_under_concurrent_backpressure() {
    let root = TempDir::new().expect("temp root");
    let client = Arc::new(client(&root));
    let barrier = Arc::new(Barrier::new(42));
    let mut handles = Vec::new();
    for index in 0..41_u64 {
        let client = Arc::clone(&client);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut payload = json!({});
            if index == 0 {
                payload["test_sleep_before_ms"] = json!(500);
            }
            client.call(
                Request::new(100 + index, 0, Operation::Health, "", payload),
                Duration::from_secs(2),
            )
        }));
    }
    barrier.wait();
    let results: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().expect("call thread joins"))
        .collect();
    assert!(results.contains(&Err(ClientError::Busy)));
    assert!(results.iter().any(Result::is_ok));
}

#[test]
fn protocol_and_handshake_identity_mismatches_fail_closed() {
    let root = TempDir::new().expect("temp root");
    let client = client(&root);
    let ns = namespace(4);
    let mut wrong_protocol = Request::new(200, 0, Operation::Health, "", json!({}));
    wrong_protocol.protocol_version = PROTOCOL_VERSION + 1;
    let reply = client
        .call(wrong_protocol, CALL_DEADLINE)
        .expect("typed incompatibility reply");
    assert_eq!(reply.outcome, Outcome::Incompatible);

    let reply = client
        .call(
            Request::new(
                201,
                0,
                Operation::Handshake,
                ns,
                json!({"algorithm_profile": "wrong-profile"}),
            ),
            CALL_DEADLINE,
        )
        .expect("typed handshake reply");
    assert_eq!(reply.outcome, Outcome::Incompatible);

    let model = client
        .call(
            Request::new(
                202,
                0,
                Operation::Handshake,
                namespace(5),
                json!({"model": "wrong-model"}),
            ),
            CALL_DEADLINE,
        )
        .expect("typed model mismatch reply");
    assert_eq!(model.outcome, Outcome::Incompatible);

    let epoch = client
        .call(
            Request::new(
                203,
                0,
                Operation::Handshake,
                namespace(6),
                json!({"epoch": 99}),
            ),
            CALL_DEADLINE,
        )
        .expect("typed epoch mismatch reply");
    assert_eq!(epoch.outcome, Outcome::Incompatible);
}

#[test]
fn disabled_provider_never_launches_and_spawn_failures_exhaust_budget() {
    let root = TempDir::new().expect("temp root");
    let disabled = WorkerOptions {
        enabled: false,
        ..options()
    };
    assert!(matches!(
        WorkerClient::spawn(BINARY, root.path(), disabled),
        Err(ClientError::Disabled)
    ));

    let missing = WorkerOptions {
        max_restart_attempts: 1,
        ..options()
    };
    let client = WorkerClient::spawn(
        root.path().join("missing-worker-binary"),
        root.path(),
        missing,
    )
    .expect("owner thread starts lazily");
    let first = client.call(
        Request::new(210, 0, Operation::Health, "", json!({})),
        Duration::from_millis(100),
    );
    assert!(matches!(first, Err(ClientError::Spawn(_))));
    let second = client.call(
        Request::new(211, 0, Operation::Health, "", json!({})),
        Duration::from_millis(100),
    );
    assert_eq!(second, Err(ClientError::RestartExhausted));
    assert_eq!(client.pid(), None);
}

#[test]
fn process_death_is_detected_then_lazily_restarted() {
    let root = TempDir::new().expect("temp root");
    let client = client(&root);
    client
        .call(
            Request::new(220, 0, Operation::Health, "", json!({})),
            CALL_DEADLINE,
        )
        .expect("initial worker starts");
    let first_pid = client.pid().expect("worker pid exists");
    assert!(
        Command::new("kill")
            .arg("-9")
            .arg(first_pid.to_string())
            .status()
            .expect("kill command runs")
            .success()
    );
    let failed = client.call(
        Request::new(221, 0, Operation::Health, "", json!({})),
        Duration::from_millis(500),
    );
    assert!(matches!(
        failed,
        Err(ClientError::MalformedReply(_))
            | Err(ClientError::WorkerExited)
            | Err(ClientError::Transport(_))
    ));
    let restarted = client
        .call(
            Request::new(222, 0, Operation::Health, "", json!({})),
            CALL_DEADLINE,
        )
        .expect("worker restarts on following call");
    assert_eq!(restarted.outcome, Outcome::Success);
    assert_ne!(client.pid(), Some(first_pid));
}

#[test]
fn malformed_worker_reply_is_detected_and_process_is_reaped() {
    let root = TempDir::new().expect("temp root");
    let client =
        WorkerClient::spawn("/bin/echo", root.path(), options()).expect("client owner starts");
    let result = client.call(
        Request::new(240, 0, Operation::Health, "", json!({})),
        Duration::from_millis(500),
    );
    assert!(matches!(result, Err(ClientError::MalformedReply(_))));
    assert_eq!(client.pid(), None);
}

#[cfg(unix)]
#[test]
fn full_pipe_backpressure_is_hard_cancelled_without_orphan() {
    use std::os::unix::fs::PermissionsExt;

    let root = TempDir::new().expect("temp root");
    let sleeper = root.path().join("blocked-worker.sh");
    std::fs::write(&sleeper, "#!/bin/sh\nexec sleep 5\n").expect("write sleeper");
    let mut permissions = std::fs::metadata(&sleeper)
        .expect("sleeper metadata")
        .permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&sleeper, permissions).expect("make sleeper executable");
    let client = WorkerClient::spawn(&sleeper, root.path(), options()).expect("client starts");
    let payload = json!({"padding": "x".repeat(240 * 1024)});
    let started = Instant::now();
    let result = client.call(
        Request::new(241, 0, Operation::Health, "", payload),
        Duration::from_millis(30),
    );
    assert_eq!(result, Err(ClientError::Cancelled));
    assert!(started.elapsed() <= Duration::from_millis(300));
    assert_eq!(client.pid(), None);
}

#[test]
fn client_drop_reaps_worker_and_stdin_eof_exits_zero() {
    let root = TempDir::new().expect("temp root");
    let client = client(&root);
    client
        .call(
            Request::new(230, 0, Operation::Health, "", json!({})),
            CALL_DEADLINE,
        )
        .expect("worker starts");
    let pid = client.pid().expect("worker pid exists");
    drop(client);
    let deadline = Instant::now() + Duration::from_millis(300);
    while process_exists(pid) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    assert!(!process_exists(pid));

    let (mut child, stdin, _stdout) = raw_worker(root.path());
    drop(stdin);
    assert!(child.wait().expect("EOF worker exits").success());
}
