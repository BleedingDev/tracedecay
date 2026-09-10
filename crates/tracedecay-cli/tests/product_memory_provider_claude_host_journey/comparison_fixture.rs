//! Parent-driven extraction of the real CLI host journey.
//!
//! The socket carries the complete scheduled invocation and durable capture
//! acknowledgements. It supplies no provider or source authority. Raw command
//! output is retained before a decoded observation copy is inspected.

#[path = "comparison_fixture/controlled_rpc.rs"]
mod controlled_rpc;

use super::*;
use sha2::{Digest, Sha256};
use std::io;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use tracedecay::daemon::test_context_evidence::{
    COMMON_ADVISORY_PROFILE_ID, COMMON_ADVISORY_REQUIRED_CAPABILITIES, OperationControl,
    OwnedExactScope, ProviderDeclaredLimitsForTestV1, ProviderNumericDeclarationForTestV1,
    production_provider_numeric_declaration_for_test, project_delivered_context_for_test,
    read_retained_context_trace_for_test,
};
use tracedecay_contracts::retained_surfaces::{
    ProviderControlHealthCheckV1, ProviderControlOperationResultV1, ProviderControlReadinessV1,
    ProviderControlRequestV1, ProviderControlResultV1, ProviderControlScopeV1,
    ProviderControlStateSelectorV1, ProviderControlTerminalV1, ProviderHealthRequestV1,
    RetainedSurfaceResultV1,
};
use tracedecay_contracts::{
    ApplicationOutcome, CancellationSignal, CoverageCompleteness, Deadline, OperationReceipt,
    OperationTermination, RequestId, ResolvedScope,
};
use tracedecay_daemon_protocol::{DaemonInvocationOutcome, DaemonInvocationResponse};
use tracedecay_domain::UtcMicros;

const PROTOCOL: &str = "tracedecay.host-comparison.v1";
// A valid 16 MiB host response may grow when carried as JSON. The durable
// Python event sink keeps its independent, unchanged 16 MiB limit.
const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
const CHANNEL_DEADLINE: Duration = Duration::from_secs(60);
const COMMAND_DEADLINE: Duration = Duration::from_secs(180);
const CLEANUP_DEADLINE: Duration = Duration::from_secs(15);
const PROCESS_POLL: Duration = Duration::from_millis(20);
// A separate read-only evidence operation, begun after the scheduled host call.
// This must never replace or extend the provider's original request control.
const TRACE_EVIDENCE_READ_MILLIS: u64 = 5_000;
const SETUP_HEALTH_MILLIS: u64 = 5_000;

type FixtureResult<T> = Result<T, String>;

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn io_result<T>(value: io::Result<T>, operation: &str) -> FixtureResult<T> {
    value.map_err(|error| format!("{operation}: {error}"))
}

fn before_deadline(deadline: Instant, operation: &str) -> FixtureResult<()> {
    if Instant::now() >= deadline {
        return Err(format!("{operation} exceeded its original deadline"));
    }
    Ok(())
}

fn string<'a>(value: &'a Value, key: &str) -> FixtureResult<&'a str> {
    value[key]
        .as_str()
        .filter(|text| !text.is_empty())
        .ok_or_else(|| format!("missing nonempty {key}"))
}

fn sha256_spelling(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn context_payload_index(result: &Value) -> FixtureResult<(usize, Value)> {
    let payload = tracedecay::daemon::tool_json_payload(result, "tracedecay_context")
        .map_err(|error| format!("actual context payload: {error}"))?;
    // Use the production unique-JSON-payload rule to identify its original
    // content position. Warnings may have been prepended after the pack ran.
    let matches: Vec<_> = result["content"]
        .as_array()
        .ok_or("context result lacks content array")?
        .iter()
        .enumerate()
        .filter_map(|(index, block)| {
            let text = block["text"].as_str()?;
            serde_json::from_str::<Value>(text)
                .ok()
                .map(|value| (index, value))
        })
        .collect();
    let [(index, observed)] = matches.as_slice() else {
        return Err("context payload position is absent or ambiguous".into());
    };
    if observed != &payload || result["content"][*index]["type"] != "text" {
        return Err(
            "context payload position does not identify the production text payload".into(),
        );
    }
    Ok((*index, payload))
}

fn observed_delivery_scope(advisory: &Value) -> FixtureResult<OwnedExactScope> {
    let replay = &advisory["canonical_history_replay"];
    let revision = advisory["registration_revision"]
        .as_u64()
        .filter(|revision| *revision > 0)
        .ok_or("actual advisory registration revision is unavailable")?;
    if string(replay, "provider_id")? != string(advisory, "provider_id")?
        || replay["registration_revision"].as_u64() != Some(revision)
    {
        return Err("actual replay carrier and advisory provider registration disagree".into());
    }
    let scope = &replay["delivery_scope"];
    OwnedExactScope::new(
        string(scope, "profile_id")?,
        string(scope, "project_id")?,
        string(scope, "repository_identity")?,
        string(scope, "worktree_identity")?,
        string(scope, "branch_identity")?,
        string(scope, "agent_session_id")?,
        string(scope, "resolved_scope_digest")?,
    )
    .map_err(|error| format!("actual delivered destination scope: {error}"))
}

fn complete_limits_digest(limits: &ProviderDeclaredLimitsForTestV1) -> String {
    let mut digest = Sha256::new();
    for value in [
        limits.request_bytes,
        limits.response_bytes,
        limits.observation_batch_items,
        limits.recall_candidates,
        limits.concurrent_operations,
        limits.operation_millis,
        limits.snapshot_bytes,
        limits.inspection_items,
    ] {
        digest.update(value.to_be_bytes());
    }
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn health_scope_from_actual_application(
    application: &ResolvedScope,
    profile_id: &str,
    expected_project_id: &str,
    native_session: &str,
) -> FixtureResult<ProviderControlScopeV1> {
    application
        .validate()
        .map_err(|error| format!("actual Health application scope: {error}"))?;
    if application.project_id.as_str() != expected_project_id {
        return Err(
            "actual Health application scope belongs to a different registered project".into(),
        );
    }
    let reference = application
        .reference
        .as_ref()
        .ok_or("actual Health application scope has no exact reference")?;
    // These fields come from the actual daemon response, persisted profile
    // authority, and the native session whose real Start preceded this call.
    // The production destination bridge compares exactly these same fields.
    let scope = ProviderControlScopeV1 {
        profile_id: profile_id.to_owned(),
        project_id: application.project_id.as_str().to_owned(),
        repository_identity: application.repository_id.as_str().to_owned(),
        worktree_identity: application.worktree_id.as_str().to_owned(),
        branch_identity: reference.as_str().to_owned(),
        agent_session_id: native_session.to_owned(),
        resolved_scope_digest: application.scope_digest.as_str().to_owned(),
    };
    scope
        .validate()
        .map_err(|error| format!("actual Health destination scope: {error}"))?;
    Ok(scope)
}

fn accept_health_numeric_evidence(
    request: &ProviderControlRequestV1,
    expected_scope: &ProviderControlScopeV1,
    declaration: &ProviderNumericDeclarationForTestV1,
    result: &ProviderControlResultV1,
) -> FixtureResult<Value> {
    result
        .validate_for(request)
        .map_err(|error| format!("actual Health result contract: {error}"))?;
    if !matches!(request, ProviderControlRequestV1::Health(_))
        || result.terminal != ProviderControlTerminalV1::Success
        || result.provider_id != declaration.provider_id
        || result.registration_revision != declaration.declared_registration_revision
        || &result.scope != expected_scope
    {
        return Err("actual Health success, provider, registration, or complete destination scope does not match".into());
    }
    let ProviderControlOperationResultV1::Health(Some(health)) = &result.result else {
        return Err("actual successful Health data is unavailable".into());
    };
    if health.readiness != ProviderControlReadinessV1::Ready
        || health.provider_instance_id != declaration.declared_provider_instance_id
        || health.implementation_identity_digest
            != declaration.declared_implementation_identity_digest
        || health.scope_digest != expected_scope.sha256()
        || health.effective_limits_digest != declaration.declared_limits_digest
        || complete_limits_digest(&declaration.declared_limits)
            != declaration.declared_limits_digest
    {
        return Err("actual Health readiness, instance, implementation, scope, or complete eight-field limits digest does not match".into());
    }
    for required in std::iter::once(COMMON_ADVISORY_PROFILE_ID)
        .chain(COMMON_ADVISORY_REQUIRED_CAPABILITIES.iter().copied())
    {
        if !health.capability_states.iter().any(|capability| {
            capability.capability_id == required && capability.state == "available"
        }) {
            return Err(format!(
                "actual Health does not report required common capability {required} available"
            ));
        }
    }
    Ok(
        json!({"status": "observed", "provider_id": result.provider_id,
            "registration_revision": result.registration_revision, "scope": result.scope,
            "provider_instance_id": health.provider_instance_id,
            "implementation_identity_digest": health.implementation_identity_digest,
            "state_identity_digest": health.state_identity_digest, "state_generation": health.state_generation,
            "readiness": health.readiness, "capability_states": health.capability_states,
            "effective_limits": declaration.declared_limits,
            "effective_limits_digest": health.effective_limits_digest,
            "numeric_evidence_source": "complete production eight-field declaration matched to this fresh actual Health result",
        }),
    )
}

fn validate_fresh_health_execution(
    receipt: &OperationReceipt,
    observed_at: UtcMicros,
    deadline: &Deadline,
) -> FixtureResult<()> {
    receipt
        .validate()
        .map_err(|error| format!("actual Health execution receipt: {error}"))?;
    if receipt.termination != OperationTermination::Completed
        || receipt.cancellation.is_some()
        || receipt.started_at < observed_at
        || receipt.ended_at > deadline.expires_at
        || receipt.effective_deadline.expires_at > deadline.expires_at
        || receipt.ended_at > receipt.effective_deadline.expires_at
    {
        return Err(
            "actual Health execution is interrupted or outside its original fresh request interval"
                .into(),
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn accept_fresh_setup_health_response(
    response: &DaemonInvocationResponse,
    request_id: &RequestId,
    request: &ProviderControlRequestV1,
    observed_at: UtcMicros,
    deadline: &Deadline,
    profile_id: &str,
    project_id: &str,
    native_session: &str,
    declaration: &ProviderNumericDeclarationForTestV1,
) -> FixtureResult<Value> {
    if response.request_id != request_id.as_str() {
        return Err("actual Health response does not match its caller-owned request ID".into());
    }
    let DaemonInvocationOutcome::RetainedApplication { scope, outcome } = &response.outcome else {
        return Err("Health invocation did not return retained application evidence".into());
    };
    let ApplicationOutcome::Evidence(packet) = outcome else {
        return Err("Health invocation did not return read-only evidence".into());
    };
    validate_fresh_health_execution(&packet.execution, observed_at, deadline)?;
    if packet.coverage.completeness != CoverageCompleteness::Complete
        || !packet.omissions.is_empty()
    {
        return Err("actual Health evidence is incomplete".into());
    }
    let Some(RetainedSurfaceResultV1::ProviderControl(result)) = &packet.payload else {
        return Err("Health invocation lacks actual typed provider-control evidence".into());
    };
    let expected_scope =
        health_scope_from_actual_application(scope, profile_id, project_id, native_session)?;
    let mut accepted =
        accept_health_numeric_evidence(request, &expected_scope, declaration, result)?;
    accepted["actual_application_scope"] =
        serde_json::to_value(scope).map_err(|error| error.to_string())?;
    accepted["actual_execution_receipt"] =
        serde_json::to_value(&packet.execution).map_err(|error| error.to_string())?;
    Ok(accepted)
}

struct Channel {
    stream: UnixStream,
    nonce: String,
}

fn frame_remaining(deadline: Instant) -> FixtureResult<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| "comparison frame exceeded its original transfer deadline".into())
}

fn read_frame_bytes(
    stream: &mut UnixStream,
    bytes: &mut [u8],
    deadline: Instant,
) -> FixtureResult<()> {
    let mut offset = 0;
    while offset < bytes.len() {
        io_result(
            stream.set_read_timeout(Some(frame_remaining(deadline)?)),
            "set remaining frame read deadline",
        )?;
        match stream.read(&mut bytes[offset..]) {
            Ok(0) => {
                return Err(format!(
                    "comparison frame ended after {offset} of {} bytes",
                    bytes.len()
                ));
            }
            Ok(count) => offset += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                ) =>
            {
                return Err("comparison frame exceeded its original transfer deadline".into());
            }
            Err(error) => return Err(format!("read comparison frame: {error}")),
        }
    }
    frame_remaining(deadline)?;
    Ok(())
}

fn write_frame_bytes(
    stream: &mut UnixStream,
    bytes: &[u8],
    deadline: Instant,
) -> FixtureResult<()> {
    let mut offset = 0;
    while offset < bytes.len() {
        io_result(
            stream.set_write_timeout(Some(frame_remaining(deadline)?)),
            "set remaining frame write deadline",
        )?;
        match stream.write(&bytes[offset..]) {
            Ok(0) => {
                return Err(format!(
                    "comparison frame wrote zero bytes at offset {offset}"
                ));
            }
            Ok(count) => offset += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                ) =>
            {
                return Err("comparison frame exceeded its original transfer deadline".into());
            }
            Err(error) => return Err(format!("write comparison frame: {error}")),
        }
    }
    frame_remaining(deadline)?;
    Ok(())
}

impl Channel {
    fn connect() -> FixtureResult<Self> {
        let path = PathBuf::from(
            std::env::var_os("TRACEDECAY_HOST_COMPARISON_SOCKET")
                .ok_or("missing TRACEDECAY_HOST_COMPARISON_SOCKET")?,
        );
        if !path.is_absolute() {
            return Err("comparison socket must be absolute".into());
        }
        let nonce = std::env::var("TRACEDECAY_HOST_COMPARISON_NONCE")
            .map_err(|error| format!("missing comparison nonce: {error}"))?;
        if !sha256_spelling(&nonce) {
            return Err("comparison nonce must be 64 lowercase hexadecimal characters".into());
        }
        let stream = io_result(
            UnixStream::connect(path),
            "connect parent comparison socket",
        )?;
        io_result(
            stream.set_read_timeout(Some(CHANNEL_DEADLINE)),
            "channel read deadline",
        )?;
        io_result(
            stream.set_write_timeout(Some(CHANNEL_DEADLINE)),
            "channel write deadline",
        )?;
        let mut channel = Self { stream, nonce };
        // Authenticate the transport before receiving a scheduled invocation.
        channel.receive_kind("hello")?;
        channel.send("ready", None, Value::Null)?;
        Ok(channel)
    }

    fn receive(&mut self) -> FixtureResult<Value> {
        self.receive_until(Instant::now() + CHANNEL_DEADLINE)
    }

    fn receive_until(&mut self, deadline: Instant) -> FixtureResult<Value> {
        let mut header = [0u8; 8];
        read_frame_bytes(&mut self.stream, &mut header, deadline)?;
        let declared = u64::from_be_bytes(header);
        if declared == 0 || declared > MAX_FRAME_BYTES as u64 {
            return Err(format!(
                "comparison frame length {declared} outside 1..={MAX_FRAME_BYTES}"
            ));
        }
        // Check the untrusted declaration before allocating any payload bytes.
        let mut bytes = vec![0u8; declared as usize];
        // Header and every partial payload read share this one deadline.
        read_frame_bytes(&mut self.stream, &mut bytes, deadline)?;
        let value: Value = serde_json::from_slice(&bytes)
            .map_err(|error| format!("comparison frame is not UTF-8 JSON: {error}"))?;
        if value["protocol"] != PROTOCOL || value["nonce"] != self.nonce {
            return Err("comparison protocol or nonce mismatch".into());
        }
        Ok(value)
    }

    fn receive_kind(&mut self, expected: &str) -> FixtureResult<Value> {
        let value = self.receive()?;
        if value["kind"] != expected {
            return Err(format!("expected comparison {expected} frame"));
        }
        Ok(value)
    }

    fn send(&mut self, kind: &str, index: Option<usize>, body: Value) -> FixtureResult<String> {
        let deadline = Instant::now() + CHANNEL_DEADLINE;
        let bytes = serde_json::to_vec(&json!({
            "protocol": PROTOCOL, "nonce": self.nonce, "kind": kind,
            "action_index": index, "body": body,
        }))
        .map_err(|error| format!("serialize comparison {kind}: {error}"))?;
        if bytes.len() > MAX_FRAME_BYTES {
            // Do not truncate raw evidence to make an oversized event fit.
            return Err(format!(
                "comparison {kind} frame is {} bytes, cap {MAX_FRAME_BYTES}; raw artifacts retained",
                bytes.len()
            ));
        }
        write_frame_bytes(
            &mut self.stream,
            &(bytes.len() as u64).to_be_bytes(),
            deadline,
        )?;
        write_frame_bytes(&mut self.stream, &bytes, deadline)?;
        io_result(
            self.stream
                .set_write_timeout(Some(frame_remaining(deadline)?)),
            "set remaining frame flush deadline",
        )?;
        io_result(self.stream.flush(), "flush comparison frame")?;
        frame_remaining(deadline)?;
        Ok(sha256(&bytes))
    }

    fn durable(&mut self, kind: &str, index: Option<usize>, body: Value) -> FixtureResult<()> {
        let digest = self.send(kind, index, body)?;
        let ack = self.receive_kind("ack")?;
        if ack["ack_kind"] != kind
            || ack["action_index"] != json!(index)
            || ack["frame_sha256"] != digest
        {
            return Err(format!(
                "stale, skipped, or mismatched durable {kind} acknowledgement"
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProcessIdentity {
    pid: u32,
    parent: u32,
    group: u32,
    start: String,
    state: String,
}

impl ProcessIdentity {
    fn public(&self) -> Value {
        json!({"pid": self.pid, "start_identity": self.start})
    }

    fn key(&self) -> (u32, String) {
        (self.pid, self.start.clone())
    }
}

// A process-inspection helper is also an owned direct child. Its guard exists
// before the first fallible poll and preserves cleanup evidence on every error
// path, including an unwind, without selecting any external PID.
struct InspectionChild {
    child: Child,
    parent: PathBuf,
    exit: Option<std::process::ExitStatus>,
    cleanup_attempted: bool,
}

impl InspectionChild {
    fn poll(&mut self) -> FixtureResult<Option<std::process::ExitStatus>> {
        if self.exit.is_none() {
            self.exit = io_result(self.child.try_wait(), "join process inspection command")?;
        }
        Ok(self.exit)
    }

    fn cleanup(&mut self, primary: &str) -> Value {
        self.cleanup_attempted = true;
        let mut errors = BTreeSet::new();
        match self.poll() {
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => {
                if let Err(error) = self.child.kill() {
                    errors.insert(format!("kill owned inspection child: {error}"));
                }
            }
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while self.exit.is_none() && Instant::now() < deadline {
            if let Err(error) = self.poll() {
                errors.insert(error);
            }
            if self.exit.is_none() {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        use std::os::unix::process::ExitStatusExt;
        let evidence = json!({"scope": "owned process inspection helper", "pid": self.child.id(),
            "status": if self.exit.is_some() { "completed" } else { "unmeasured" },
            "joined": self.exit.is_some(), "primary_failure": primary,
            "exit_code": self.exit.and_then(|status| status.code()),
            "exit_signal": self.exit.and_then(|status| status.signal()), "cleanup_errors": errors});
        static FAILURE_SEQUENCE: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);
        let sequence = FAILURE_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = self.parent.join(format!(
            "host-inspection-cleanup-{}-{sequence}.json",
            self.child.id()
        ));
        match serde_json::to_vec(&evidence)
            .map_err(|error| error.to_string())
            .and_then(|bytes| write_durable(&path, &bytes))
        {
            Ok(artifact) => json!({"evidence": evidence, "artifact": artifact}),
            Err(error) => json!({"evidence": evidence, "artifact_error": error}),
        }
    }
}

impl Drop for InspectionChild {
    fn drop(&mut self) {
        if self.exit.is_none() && !self.cleanup_attempted {
            let _ = self.cleanup("process inspection exited without a joined child");
        }
    }
}

// Same identity boundary used by the reviewed Python process capture: pid and
// OS-reported lstart. Actual ancestry/group membership is observed; a daemon
// wait alone is never evidence that its worker children disappeared.
fn inspect_process_command(command: &mut Command) -> FixtureResult<(u32, Output)> {
    let parent = PathBuf::from(
        std::env::var_os("TRACEDECAY_HOST_COMPARISON_ARTIFACT_ROOT")
            .ok_or("missing artifact parent for bounded process inspection")?,
    );
    let mut stdout = io_result(
        tempfile::tempfile_in(&parent),
        "allocate owned process inspection output",
    )?;
    let mut stderr = io_result(
        tempfile::tempfile_in(&parent),
        "allocate owned process inspection error",
    )?;
    command
        .stdin(Stdio::null())
        .stdout(Stdio::from(io_result(
            stdout.try_clone(),
            "clone process inspection output",
        )?))
        .stderr(Stdio::from(io_result(
            stderr.try_clone(),
            "clone process inspection error",
        )?));
    let child = io_result(command.spawn(), "spawn bounded process inspection command")?;
    let mut owner = InspectionChild {
        child,
        parent,
        exit: None,
        cleanup_attempted: false,
    };
    let pid = owner.child.id();
    let result: FixtureResult<(u32, Output)> = (|| {
        let deadline = Instant::now() + Duration::from_secs(2);
        let status = loop {
            if let Some(status) = owner.poll()? {
                break status;
            }
            if Instant::now() >= deadline {
                return Err("process inspection exceeded its original deadline".into());
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        let read = |file: &mut fs::File| -> FixtureResult<Vec<u8>> {
            if io_result(file.metadata(), "process inspection output size")?.len()
                > MAX_FRAME_BYTES as u64
            {
                return Err("process inspection output exceeded its allocation bound".into());
            }
            io_result(
                std::io::Seek::seek(file, std::io::SeekFrom::Start(0)),
                "rewind process inspection output",
            )?;
            let mut bytes = Vec::new();
            io_result(
                file.read_to_end(&mut bytes),
                "read complete process inspection output",
            )?;
            Ok(bytes)
        };
        Ok((
            pid,
            Output {
                status,
                stdout: read(&mut stdout)?,
                stderr: read(&mut stderr)?,
            },
        ))
    })();
    match result {
        Ok(output) => Ok(output),
        Err(primary) => {
            let cleanup = owner.cleanup(&primary);
            Err(format!("{primary}; inspection cleanup: {cleanup}"))
        }
    }
}

fn process_snapshot() -> FixtureResult<BTreeMap<u32, ProcessIdentity>> {
    let mut command = Command::new("ps");
    command
        .args(["-A", "-o", "pid=,ppid=,pgid=,lstart=,stat="])
        .env("LC_ALL", "C");
    let (inspection_pid, output) = inspect_process_command(&mut command)?;
    if !output.status.success() {
        return Err(format!(
            "process identity inspection failed: {}",
            output.status
        ));
    }
    let text = std::str::from_utf8(&output.stdout)
        .map_err(|error| format!("process identity inspection UTF-8: {error}"))?;
    let mut rows = BTreeMap::new();
    for line in text.lines() {
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() != 9 {
            return Err("process identity inspection returned an unrecognized row".into());
        }
        let number = |index: usize| {
            fields[index]
                .parse::<u32>()
                .map_err(|error| format!("process identity number: {error}"))
        };
        let row = ProcessIdentity {
            pid: number(0)?,
            parent: number(1)?,
            group: number(2)?,
            start: fields[3..8].join(" "),
            state: fields[8].to_owned(),
        };
        // The helper was synchronously joined above; it is never a remaining
        // child merely because ps observed itself before exiting.
        if row.pid != inspection_pid {
            rows.insert(row.pid, row);
        }
    }
    Ok(rows)
}

struct OwnedChild {
    label: String,
    child: Child,
    identity: Option<ProcessIdentity>,
    exit: Option<Value>,
}

struct OwnedProcesses {
    root: ProcessIdentity,
    known: BTreeMap<(u32, String), ProcessIdentity>,
    children: Vec<OwnedChild>,
    signals: Vec<Value>,
    closed: bool,
}

impl OwnedProcesses {
    fn new() -> FixtureResult<Self> {
        let root = process_snapshot()?
            .remove(&std::process::id())
            .ok_or("fixture process has no readable start identity")?;
        if root.pid != root.group {
            return Err("fixture must be launched in its own session/process group".into());
        }
        Ok(Self {
            root,
            known: BTreeMap::new(),
            children: Vec::new(),
            signals: Vec::new(),
            closed: false,
        })
    }

    fn sample(&mut self) -> FixtureResult<BTreeMap<u32, ProcessIdentity>> {
        let all = process_snapshot()?;
        if all.get(&self.root.pid).map(ProcessIdentity::key) != Some(self.root.key()) {
            return Err("fixture root process identity changed during capture".into());
        }
        let mut owned_pids = BTreeSet::from([self.root.pid]);
        for row in all.values() {
            if row.pid != self.root.pid
                && (row.group == self.root.group || self.known.contains_key(&row.key()))
            {
                owned_pids.insert(row.pid);
            }
        }
        loop {
            let before = owned_pids.len();
            for row in all.values() {
                if owned_pids.contains(&row.parent) {
                    owned_pids.insert(row.pid);
                }
            }
            if before == owned_pids.len() {
                break;
            }
        }
        let owned: BTreeMap<_, _> = all
            .into_iter()
            .filter(|(pid, _)| *pid != self.root.pid && owned_pids.contains(pid))
            .collect();
        for row in owned.values() {
            self.known.insert(row.key(), row.clone());
        }
        for child in &mut self.children {
            if child.identity.is_none() {
                child.identity = owned.get(&child.child.id()).cloned();
            }
        }
        Ok(owned)
    }

    fn spawn(&mut self, command: &mut Command, label: &str) -> FixtureResult<usize> {
        command.process_group(self.root.group as i32);
        let child = io_result(command.spawn(), &format!("spawn {label}"))?;
        let slot = self.children.len();
        // Store the Child before fallible identity collection or startup waits.
        self.children.push(OwnedChild {
            label: label.to_owned(),
            child,
            identity: None,
            exit: None,
        });
        self.sample()?;
        Ok(slot)
    }

    fn poll(&mut self, slot: usize) -> FixtureResult<Option<Value>> {
        let owned = &mut self.children[slot];
        if let Some(exit) = &owned.exit {
            return Ok(Some(exit.clone()));
        }
        let status = io_result(owned.child.try_wait(), &format!("poll {}", owned.label))?;
        if let Some(status) = status {
            use std::os::unix::process::ExitStatusExt;
            let exit = json!({
                "label": owned.label, "pid": owned.child.id(),
                "identity": owned.identity.as_ref().map(ProcessIdentity::public),
                "joined": true, "success": status.success(),
                "code": status.code(), "signal": status.signal(),
            });
            owned.exit = Some(exit.clone());
            return Ok(Some(exit));
        }
        Ok(None)
    }

    fn wait(&mut self, slot: usize, deadline: Instant) -> FixtureResult<Value> {
        loop {
            self.sample()?;
            if let Some(exit) = self.poll(slot)? {
                return Ok(exit);
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "{} exceeded its original command deadline",
                    self.children[slot].label
                ));
            }
            std::thread::sleep(PROCESS_POLL);
        }
    }

    fn signal(&mut self, identity: &ProcessIdentity, signal: &str) -> FixtureResult<()> {
        let current = process_snapshot()?;
        if current.get(&identity.pid).map(ProcessIdentity::key) != Some(identity.key()) {
            return Ok(());
        }
        if !self.known.contains_key(&identity.key()) || identity.pid == self.root.pid {
            return Err("refusing a signal outside the recorded owned process set".into());
        }
        let (_, output) = inspect_process_command(
            Command::new("/bin/kill").args([signal, &identity.pid.to_string()]),
        )?;
        self.signals.push(json!({"identity": identity.public(), "signal": signal,
            "command_joined": true, "success": output.status.success(), "code": output.status.code()}));
        Ok(())
    }

    fn stop(&mut self, slot: usize) -> FixtureResult<Value> {
        if let Some(exit) = self.poll(slot)? {
            return Ok(exit);
        }
        self.sample()?;
        let identity = self.children[slot]
            .identity
            .clone()
            .ok_or("live direct child has no verified process identity")?;
        self.signal(&identity, "-TERM")?;
        let polite = Instant::now() + Duration::from_secs(3);
        loop {
            self.sample()?;
            if let Some(exit) = self.poll(slot)? {
                return Ok(exit);
            }
            if Instant::now() >= polite {
                break;
            }
            std::thread::sleep(PROCESS_POLL);
        }
        self.signal(&identity, "-KILL")?;
        self.wait(slot, Instant::now() + CLEANUP_DEADLINE)
    }

    fn close(&mut self) -> Value {
        let result = self.close_inner();
        self.closed = result.is_ok();
        match result {
            Ok(observations) => json!({"status": "completed", "remaining_owned_children": 0,
                "root": self.root.public(), "descendant_observations": observations,
                "direct_child_exits": self.children.iter().filter_map(|child| child.exit.clone()).collect::<Vec<_>>(),
                "signals": self.signals, "scope": "verified root ancestry and process group"}),
            Err(error) => json!({"status": "unmeasured", "remaining_owned_children": null,
                "reason": error, "root": self.root.public(), "signals": self.signals,
                "direct_child_exits": self.children.iter().filter_map(|child| child.exit.clone()).collect::<Vec<_>>()}),
        }
    }

    fn close_inner(&mut self) -> FixtureResult<Vec<Value>> {
        let deadline = Instant::now() + CLEANUP_DEADLINE;
        let mut signalled = BTreeSet::new();
        loop {
            let remaining = self.sample()?;
            for slot in 0..self.children.len() {
                self.poll(slot)?;
            }
            if remaining.is_empty() && self.children.iter().all(|child| child.exit.is_some()) {
                // A second actual snapshot catches children that completed
                // reparenting while their direct parent was being joined.
                if self.sample()?.is_empty() {
                    return Ok(self
                        .known
                        .values()
                        .map(|row| {
                            json!({
                                "identity": row.public(), "parent_pid": row.parent,
                                "process_group": row.group, "last_observed_state": row.state,
                                "absence_verified_at_close": true,
                            })
                        })
                        .collect());
                }
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "owned child cleanup deadline; {} currently observed descendants",
                    remaining.len()
                ));
            }
            let signal =
                if deadline.saturating_duration_since(Instant::now()) > Duration::from_secs(10) {
                    "-TERM"
                } else {
                    "-KILL"
                };
            for identity in remaining.values().rev() {
                if signalled.insert((identity.key(), signal)) {
                    self.signal(identity, signal)?;
                }
            }
            std::thread::sleep(PROCESS_POLL);
        }
    }
}

impl Drop for OwnedProcesses {
    fn drop(&mut self) {
        if !self.closed {
            // Best-effort fallback on panic/protocol failure. Explicit close
            // retains the actual result and is the only success evidence.
            let _ = self.close_inner();
        }
    }
}

fn write_durable(path: &Path, bytes: &[u8]) -> FixtureResult<Value> {
    let mut file = io_result(
        fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path),
        "create owned raw artifact",
    )?;
    io_result(file.write_all(bytes), "write complete owned raw artifact")?;
    io_result(file.sync_all(), "fsync owned raw artifact")?;
    if let Some(parent) = path.parent() {
        io_result(
            io_result(fs::File::open(parent), "open owned raw artifact directory")?.sync_all(),
            "fsync owned raw artifact directory",
        )?;
    }
    Ok(json!({"path": path, "bytes": bytes.len(), "sha256": sha256(bytes)}))
}

fn existing_artifact(path: &Path) -> FixtureResult<Value> {
    let mut file = io_result(
        fs::OpenOptions::new().read(true).write(true).open(path),
        "open completed raw artifact",
    )?;
    io_result(file.sync_all(), "fsync completed raw artifact")?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 65536];
    let mut bytes = 0u64;
    loop {
        let count = io_result(file.read(&mut buffer), "hash complete raw artifact")?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
        bytes += count as u64;
    }
    Ok(
        json!({"path": path, "bytes": bytes, "sha256": digest.finalize().iter().map(|byte| format!("{byte:02x}")).collect::<String>()}),
    )
}

struct RawCommand {
    record: Value,
    stdout: PathBuf,
}

impl RawCommand {
    fn decode(&self) -> FixtureResult<Value> {
        let size = io_result(fs::metadata(&self.stdout), "raw command stdout metadata")?.len();
        if size > MAX_FRAME_BYTES as u64 {
            return Err(
                "raw stdout exceeds observation-copy bound; complete artifact retained".into(),
            );
        }
        let bytes = io_result(
            fs::read(&self.stdout),
            "read preserved stdout observation copy",
        )?;
        serde_json::from_slice(&bytes)
            .map_err(|error| format!("preserved stdout is not complete JSON: {error}"))
    }

    fn success(&self) -> bool {
        self.record["exit"]["success"] == true && self.record["cutoff"].is_null()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Lane {
    Native,
    Ncm,
    NoMemory,
    Documentation,
}

impl Lane {
    fn parse(value: &str) -> FixtureResult<Self> {
        match value {
            "provider:tracedecay.native" => Ok(Self::Native),
            "provider:ncm" => Ok(Self::Ncm),
            "no_memory" => Ok(Self::NoMemory),
            "explicit_documentation" => Ok(Self::Documentation),
            _ => Err("unknown scheduled comparison lane".into()),
        }
    }

    fn provider(self) -> Option<ActiveProvider> {
        match self {
            Self::Native => Some(ActiveProvider::Native),
            Self::Ncm => Some(ActiveProvider::RustNcm),
            Self::NoMemory | Self::Documentation => None,
        }
    }
}

/// A mapping from supplied native bytes to their scheduled input. Canonical
/// identities are added only after reading the host's admitted record.
struct ProjectedSource {
    source: Value,
    session: String,
    path: PathBuf,
    start: u64,
    end: u64,
    native_bytes_sha256: String,
}

struct HostFixture<'owner> {
    journey: ClaudeHostJourney,
    processes: &'owner mut OwnedProcesses,
    invocation: Value,
    lane: Lane,
    archive: PathBuf,
    clock: Instant,
    daemon_slot: Option<usize>,
    command_sequence: usize,
    commands: Vec<Value>,
    project_id: String,
    initial_origin: Option<Value>,
    started_sessions: BTreeMap<String, Value>,
    projected: Vec<ProjectedSource>,
    initial_evidence: Value,
}

impl<'owner> HostFixture<'owner> {
    fn allocate(invocation: Value, processes: &'owner mut OwnedProcesses) -> FixtureResult<Self> {
        validate_invocation(&invocation)?;
        let lane = Lane::parse(string(&invocation, "lane")?)?;
        let codex = match string(&invocation, "host")? {
            "claude" => false,
            "codex" => true,
            _ => return Err("unknown scheduled native host".into()),
        };
        let parent = PathBuf::from(
            std::env::var_os("TRACEDECAY_HOST_COMPARISON_ARTIFACT_ROOT")
                .ok_or("missing TRACEDECAY_HOST_COMPARISON_ARTIFACT_ROOT")?,
        );
        if !parent.is_absolute() || !parent.is_dir() {
            return Err("comparison artifact parent must be an existing absolute directory".into());
        }
        // Preserve from creation, including failures during project allocation.
        // The caller owns parent; this fixture never removes it or its socket.
        let home = io_result(
            tempfile::Builder::new()
                .prefix("host-fixture-")
                .disable_cleanup(true)
                .tempdir_in(&parent),
            "allocate persistent isolated fixture",
        )?;
        let archive = home.path().join("comparison-artifacts");
        io_result(
            fs::create_dir(&archive),
            "create fixture raw artifact directory",
        )?;
        let bytes = serde_json::to_vec(&invocation).map_err(|error| error.to_string())?;
        write_durable(&archive.join("invocation.json"), &bytes)?;
        let journey = ClaudeHostJourney::allocate(
            home,
            codex,
            lane.provider().unwrap_or(ActiveProvider::Native),
            false,
        );
        Ok(Self {
            journey,
            processes,
            invocation,
            lane,
            archive,
            clock: Instant::now(),
            daemon_slot: None,
            command_sequence: 0,
            commands: Vec::new(),
            project_id: String::new(),
            initial_origin: None,
            started_sessions: BTreeMap::new(),
            projected: Vec::new(),
            initial_evidence: Value::Null,
        })
    }

    fn clock_ns(&self) -> u128 {
        self.clock.elapsed().as_nanos()
    }

    fn run(
        &mut self,
        mut command: Command,
        label: &str,
        input: Option<&[u8]>,
        bound: Duration,
    ) -> FixtureResult<RawCommand> {
        self.command_sequence += 1;
        let prefix = format!("command-{:06}", self.command_sequence);
        let stdout = self.archive.join(format!("{prefix}.stdout"));
        let stderr = self.archive.join(format!("{prefix}.stderr"));
        let create = |path: &Path| {
            io_result(
                fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(path),
                "allocate subprocess raw output",
            )
        };
        command
            .stdout(Stdio::from(create(&stdout)?))
            .stderr(Stdio::from(create(&stderr)?));
        let stdin_record = if let Some(bytes) = input {
            let path = self.archive.join(format!("{prefix}.stdin"));
            let record = write_durable(&path, bytes)?;
            command.stdin(Stdio::from(io_result(
                fs::File::open(path),
                "open retained subprocess input",
            )?));
            record
        } else {
            command.stdin(Stdio::null());
            Value::Null
        };
        let args: Vec<_> = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        let program = command.get_program().to_string_lossy().into_owned();
        let started = self.clock_ns();
        let deadline = Instant::now() + bound;
        let slot = self.processes.spawn(&mut command, label)?;
        let wait = self.processes.wait(slot, deadline);
        let ended = self.clock_ns();
        let (exit, cutoff) = match wait {
            Ok(exit) => (exit, None),
            Err(error) => {
                let cleanup = self.processes.stop(slot);
                let exit = cleanup.as_ref().ok().cloned().unwrap_or(Value::Null);
                (
                    exit,
                    Some(json!({"reason": error, "cleanup": cleanup.err(),
                    "elapsed_at_cutoff_ns": ended.saturating_sub(started)})),
                )
            }
        };
        let record = json!({
            "label": label, "program": program, "args": args, "stdin": stdin_record,
            "stdout": existing_artifact(&stdout)?, "stderr": existing_artifact(&stderr)?,
            "exit": exit, "cutoff": cutoff, "process_group": self.processes.root.public(),
            "timing": {"status": "measured", "clock": "fixture_process_monotonic_elapsed",
                "start_monotonic_ns": started, "end_monotonic_ns": ended, "elapsed_ns": ended.saturating_sub(started)},
        });
        self.commands.push(record.clone());
        Ok(RawCommand { record, stdout })
    }

    fn tool(
        &mut self,
        name: &str,
        arguments: &Value,
        deadline_ms: Option<u64>,
    ) -> FixtureResult<RawCommand> {
        let mut command = self.journey.cli(&["tool", "--project"]);
        command
            .arg(&self.journey.project)
            .args([name, "--args", "-", "--json"]);
        if let Some(millis) = deadline_ms {
            command.env(
                tracedecay_daemon_protocol::TOOL_REQUEST_DEADLINE_ENV,
                millis.to_string(),
            );
        }
        let bytes = serde_json::to_vec(arguments).map_err(|error| error.to_string())?;
        // This outer bound only collects the CLI's typed deadline result. It
        // does not extend the producer deadline passed in the environment.
        let bound = deadline_ms
            .map(|ms| Duration::from_millis(ms).saturating_add(Duration::from_secs(15)))
            .unwrap_or(COMMAND_DEADLINE);
        self.run(command, name, Some(&bytes), bound)
    }

    fn payload(raw: &RawCommand, name: &str) -> FixtureResult<Value> {
        let value = raw.decode()?;
        tracedecay::daemon::tool_json_payload(&value, name)
            .map_err(|error| format!("{name} decoded observation copy: {error}"))
    }

    fn configuration_revision(&mut self) -> FixtureResult<String> {
        let raw = self.tool("tracedecay_configuration_observed_state", &json!({}), None)?;
        if !raw.success() {
            return Err(
                "configuration observed-state command failed; raw evidence retained".into(),
            );
        }
        let value = raw.decode()?;
        let mut revisions = Vec::new();
        collect_string_field(&value, "desired_revision_id", &mut revisions);
        revisions.sort();
        revisions.dedup();
        if revisions.len() != 1 {
            return Err(
                "configuration components do not report one actual desired revision".into(),
            );
        }
        Ok(revisions.remove(0))
    }

    fn setting(&mut self, key: &str, value: Value) -> FixtureResult<()> {
        let expected = self.configuration_revision()?;
        let raw = self.tool(
            "tracedecay_configuration_set",
            &json!({
                "layer": {"kind": "project", "project_id": self.project_id},
                "key": key, "value": value, "expected_revision": expected,
                "idempotency_key": format!("comparison.configuration.{}", self.command_sequence),
            }),
            None,
        )?;
        if !raw.success() {
            return Err(format!(
                "configuration write {key} failed; raw evidence retained"
            ));
        }
        Ok(())
    }

    fn authority(&self) -> FixtureResult<DaemonAuthorityRecord> {
        let bytes = io_result(
            fs::read(daemon_authority_path(&self.journey.profile)),
            "read isolated daemon authority",
        )?;
        let authority: DaemonAuthorityRecord =
            serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
        let slot = self.daemon_slot.ok_or("no owned daemon")?;
        if authority.pid != self.processes.children[slot].child.id()
            || io_result(
                fs::canonicalize(&authority.profile_root),
                "canonical authority profile",
            )? != io_result(
                fs::canonicalize(&self.journey.profile),
                "canonical fixture profile",
            )?
        {
            return Err("authority does not name this fixture daemon and profile".into());
        }
        Ok(authority)
    }

    fn start_daemon(&mut self) -> FixtureResult<Value> {
        if self.daemon_slot.is_some() {
            return Err("fixture daemon already owned".into());
        }
        self.command_sequence += 1;
        let log = self
            .archive
            .join(format!("daemon-{:06}.stderr", self.command_sequence));
        let file = io_result(
            fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&log),
            "create daemon stderr artifact",
        )?;
        let mut command = self.journey.cli(&["daemon", "run"]);
        command
            .env("TRACEDECAY_TEST_HOST_HISTORY_RECALL_DIAGNOSTICS", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::from(file));
        let start = self.clock_ns();
        let deadline = Instant::now() + COMMAND_DEADLINE;
        let slot = self.processes.spawn(&mut command, "isolated daemon")?;
        self.daemon_slot = Some(slot);
        loop {
            self.processes.sample()?;
            if self.processes.poll(slot)?.is_some() {
                return Err(format!(
                    "daemon exited before readiness; stderr preserved at {}",
                    log.display()
                ));
            }
            if let Ok(authority) = self.authority() {
                let end = self.clock_ns();
                return Ok(
                    json!({"pid": authority.pid, "process_run_id": authority.process_run_id,
                    "profile_root": authority.profile_root, "version": authority.version,
                    "stderr_path": log, "publication_timing": {
                        "phase": "cold_startup", "status": "measured",
                        "clock": "fixture_process_monotonic_elapsed", "start_monotonic_ns": start,
                        "end_monotonic_ns": end, "elapsed_ns": end.saturating_sub(start),
                        "boundary": "spawn to actual authority publication; project readiness measured separately"}}),
                );
            }
            if Instant::now() >= deadline {
                return Err("isolated daemon authority publication deadline".into());
            }
            std::thread::sleep(PROCESS_POLL);
        }
    }

    fn stop_daemon(&mut self) -> FixtureResult<Value> {
        let slot = self.daemon_slot.take().ok_or("no daemon to restart")?;
        let exit = self.processes.stop(slot)?;
        // There is no in-flight CLI action at this serialized lifecycle
        // boundary. Join/verify every worker before opening the state again.
        let descendants = self.processes.close_inner()?;
        Ok(json!({"daemon_exit": exit, "remaining_owned_children": 0,
            "descendant_observations": descendants}))
    }

    fn startup_barrier(&mut self) -> FixtureResult<Value> {
        let authority = self.authority()?;
        let key = format!(
            "session-sync.startup.{}.{}",
            authority.process_run_id, self.project_id
        );
        let deadline = Instant::now() + SETTLEMENT_BUDGET;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?;
        let mut observations = Vec::new();
        loop {
            before_deadline(deadline, "startup history")?;
            let outcome = self.journey.startup_sync_status_for_daemon(
                &runtime,
                &key,
                deadline,
                authority.pid,
            );
            before_deadline(deadline, "startup history")?;
            observations.push(outcome.clone());
            match outcome["status"].as_str() {
                Some("complete") => {
                    if outcome["termination"] != "completed" {
                        return Err("startup history completed with non-success terminal".into());
                    }
                    let coverage = outcome["coverage"]
                        .as_array()
                        .filter(|entries| !entries.is_empty())
                        .ok_or("missing startup source coverage")?;
                    let remaining = coverage
                        .iter()
                        .try_fold(0u64, |sum, entry| {
                            let coverage = entry.get("coverage")?;
                            let value = match coverage["outcome"].as_str()? {
                                "complete" => 0,
                                "partial" => coverage["deferred_units"].as_u64()?,
                                "backpressured" => coverage["rejected_units"].as_u64()?,
                                _ => return None,
                            };
                            Some(sum.saturating_add(value))
                        })
                        .ok_or("unrecognized startup remaining-work coverage")?;
                    if remaining != 0 {
                        return Err("startup history left deferred or rejected work".into());
                    }
                    break;
                }
                Some("accepted" | "joined") => {}
                Some("unavailable")
                    if matches!(
                        outcome["reason_code"].as_str(),
                        Some(
                            "session_sync_authority_unavailable"
                                | "session_sync_operation_not_found"
                        )
                    ) => {}
                _ => return Err("startup history refused; actual status retained".into()),
            }
            self.processes.sample()?;
            if Instant::now() >= deadline {
                return Err("startup history original settlement deadline".into());
            }
            std::thread::sleep(JOURNAL_POLL_INTERVAL);
        }
        let status_bytes = serde_json::to_vec(&observations).map_err(|error| error.to_string())?;
        self.command_sequence += 1;
        let statuses = write_durable(
            &self
                .archive
                .join(format!("startup-status-{:06}.json", self.command_sequence)),
            &status_bytes,
        )?;
        loop {
            before_deadline(deadline, "startup projection")?;
            let remaining = deadline.saturating_duration_since(Instant::now());
            let raw = self.tool(
                "tracedecay_lcm_doctor",
                &json!({"format": "json"}),
                Some(
                    u64::try_from(remaining.as_millis())
                        .unwrap_or(u64::MAX)
                        .max(1),
                ),
            )?;
            before_deadline(deadline, "startup projection")?;
            let doctor = Self::payload(&raw, "tracedecay_lcm_doctor")?;
            let projection = doctor
                .pointer("/outcome/value/payload/projection")
                .ok_or("doctor omitted retained projection")?;
            match projection["state"].as_str() {
                Some("current") => {
                    return Ok(
                        json!({"startup_status": statuses, "projection_command": raw.record, "projection": projection}),
                    );
                }
                Some("stale") => {}
                _ => return Err("retained startup projection cannot converge".into()),
            }
            if Instant::now() >= deadline {
                return Err("retained startup projection original settlement deadline".into());
            }
            std::thread::sleep(JOURNAL_POLL_INTERVAL);
        }
    }

    fn setup(&mut self) -> FixtureResult<()> {
        // The stock isolated profile starts with both advisory providers
        // disabled. Commit the selected gates before its first advisory mount.
        let initial_daemon = self.start_daemon()?;
        let deadline = Instant::now() + COMMAND_DEADLINE;
        loop {
            let raw = self.run(
                self.journey.cli(&["init"]),
                "register isolated project",
                None,
                deadline.saturating_duration_since(Instant::now()),
            )?;
            if raw.success() {
                break;
            }
            let stderr_path = raw.record["stderr"]["path"]
                .as_str()
                .ok_or("missing init stderr artifact")?;
            let stderr = io_result(
                fs::read_to_string(stderr_path),
                "read init error observation",
            )?;
            if !stderr.contains("code_index_scheduler_unavailable") || Instant::now() >= deadline {
                return Err(
                    "isolated project initialization failed; exact outputs retained".into(),
                );
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let mut command = self.journey.cli(&["projects", "context"]);
        command.arg(&self.journey.project).arg("--json");
        let context = self.run(
            command,
            "read registered project identity",
            None,
            COMMAND_DEADLINE,
        )?;
        let context_value = context.decode()?;
        self.project_id = string(&context_value["project"], "project_id")?.to_owned();
        self.setting(
            MEMORY_PROVIDER_NATIVE_ENABLED_SETTING_KEY,
            json!({"kind": "boolean", "value": self.lane == Lane::Native}),
        )?;
        if let Some(provider) = self.lane.provider() {
            self.setting(
                MEMORY_PROVIDER_RECALL_ROUTING_SETTING_KEY,
                json!({"kind": "text", "value": json!({
                "active_provider": provider.id(), "degradation": {
                    "policy_id": "policy.host-comparison.history.v1", "policy_revision": 1,
                    "allowed_causes": ["partial", "stale"],
                },
            }).to_string()}),
            )?;
        }
        if self.lane == Lane::Ncm {
            let worker = PathBuf::from(
                std::env::var_os("TRACEDECAY_NCM_WORKER")
                    .ok_or("missing actual NCM worker binary")?,
            );
            let installed = PathBuf::from(
                std::env::var_os("TRACEDECAY_NCM_REAL_MODEL_ROOT")
                    .ok_or("missing installed NCM model root")?,
            );
            if !worker.is_absolute() || !worker.is_file() || !installed.is_absolute() {
                return Err("real NCM requires absolute existing worker/model paths".into());
            }
            let models = io_result(
                fs::canonicalize(installed.join("models")),
                "canonical installed model directory",
            )?;
            if !models.is_dir() {
                return Err("NCM model directory missing".into());
            }
            let state = self.journey.home.path().join("ncm-observer");
            io_result(
                fs::create_dir(&state),
                "allocate isolated NCM mutable state",
            )?;
            io_result(
                std::os::unix::fs::symlink(&models, state.join("models")),
                "link installed read-only model artifacts",
            )?;
            self.setting("memory.provider_ncm_observer.v1", json!({"kind": "text", "value": json!({
                "mode": "enabled", "worker_binary": io_result(fs::canonicalize(worker), "canonical NCM worker")?,
                "state_root": io_result(fs::canonicalize(state), "canonical isolated NCM state")?,
            }).to_string()}))?;
        }
        if self.lane == Lane::Documentation {
            // The explicit lane's files remain separate from common canonical
            // fact evidence. Actual documentation delivery is still unresolved.
            io_result(
                fs::create_dir(self.journey.project.join("docs")),
                "allocate explicit documentation directory",
            )?;
        }
        let stopped = self.stop_daemon()?;
        let selected_daemon = self.start_daemon()?;
        // Configuration observed-state mounts the selected project without an
        // extra recall query. The same canonical barrier runs for every lane.
        let revision = self.configuration_revision()?;
        let canonical_start = self.startup_barrier()?;
        self.initial_evidence = json!({"initial_daemon": initial_daemon, "stopped_initial_daemon": stopped,
            "selected_daemon": selected_daemon, "configuration_revision": revision,
            "registered_project": context_value, "canonical_start": canonical_start});
        // Establish the real destination before metadata acknowledgement and
        // before any scheduled query. Its transcript contains no query text.
        let destination_session = self.destination_session()?;
        let destination_start = self.ensure_native_session_started(&destination_session)?;
        self.initial_evidence["destination_native_start"] = destination_start;
        if self.lane.provider().is_some() {
            // Health is a separate setup evidence operation. Control lanes
            // never dispatch it, and it does not consume a scheduled recall.
            let host = if self.journey.codex {
                "codex"
            } else {
                "claude"
            };
            self.initial_evidence["provider_setup_health"] =
                self.capture_setup_health(host, &destination_session);
        }
        Ok(())
    }

    fn metadata(&self, setup_error: Option<&str>) -> Value {
        json!({
            "case_trial_id": self.invocation["case_trial_id"], "consumed_case": self.invocation["case"],
            "budgets": self.invocation["budgets"], "mode": "production_host",
            "namespace_identity": self.journey.home.path(),
            "physical_paths": {"home": self.journey.home.path(), "profile": self.journey.profile,
                "project": self.journey.project, "raw_artifacts": self.archive},
            "process_group": self.processes.root.public(), "observer_enabled": false,
            "selected_provider": null,
            "selected_provider_evidence": {"status": "unavailable",
                "reason": "full actual selected build/model/effective-limit binding is not exposed by setup output",
                "actual_setup": self.initial_evidence},
            "projection_sha256": null, "canonical_start_sha256": null,
            "canonical_evidence_status": {"status": "unavailable",
                "reason": "common canonical source/start projection helper pending; requested case digest is not host state evidence"},
            "setup_error": setup_error, "setup_commands": self.commands,
        })
    }

    fn hook(&mut self, subcommand: &str, payload: &Value) -> FixtureResult<RawCommand> {
        let bytes = serde_json::to_vec(payload).map_err(|error| error.to_string())?;
        self.run(
            self.journey.cli(&[subcommand]),
            subcommand,
            Some(&bytes),
            COMMAND_DEADLINE,
        )
    }

    fn destination_session(&self) -> FixtureResult<String> {
        Ok(format!(
            "comparison-request-{}",
            &sha256(string(&self.invocation["case"], "id")?.as_bytes())[..32]
        ))
    }

    fn ensure_native_session_started(&mut self, session: &str) -> FixtureResult<Value> {
        if let Some(evidence) = self.started_sessions.get(session) {
            return Ok(json!({"reused": true, "evidence": evidence}));
        }
        let path = self.journey.transcript_path_for_session(session);
        match fs::symlink_metadata(&path) {
            Ok(_) => {
                return Err(
                    "unstarted native transcript already exists; refusing to replace its bytes"
                        .into(),
                );
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "inspect native transcript before initialization: {error}"
                ));
            }
        }
        // Reuse the production journey's empty Claude / metadata-only Codex
        // initializer. No scheduled message is present at this live baseline.
        self.journey.initialize_session_transcript(session);
        let initial_file = io_result(
            fs::OpenOptions::new().read(true).write(true).open(&path),
            "open initialized native transcript for durability",
        )?;
        io_result(
            initial_file.sync_all(),
            "fsync initialized native transcript",
        )?;
        let parent = path.parent().ok_or("native transcript parent missing")?;
        io_result(
            io_result(fs::File::open(parent), "open native transcript directory")?.sync_all(),
            "fsync initialized native transcript directory",
        )?;
        let initial_bytes = io_result(fs::read(&path), "read exact native session baseline")?;
        let initial_artifact = write_durable(
            &self.archive.join(format!(
                "session-{}.initial.jsonl",
                sha256(session.as_bytes())
            )),
            &initial_bytes,
        )?;
        let command = if self.journey.codex {
            "hook-codex-session-start"
        } else {
            "hook-claude-session-start"
        };
        let mut payload = json!({"session_id": session, "cwd": self.journey.project,
            "hook_event_name": "SessionStart", "source": "startup"});
        if !self.journey.codex {
            payload["transcript_path"] = json!(path);
        }
        // Codex supplies the session identity; its metadata-only rollout is
        // discovered through the actual native session reader.
        let raw = self.hook(command, &payload)?;
        if !raw.success() {
            return Err("native SessionStart failed; exact baseline/input/output retained".into());
        }
        let evidence = json!({
            "native_host": if self.journey.codex { "codex" } else { "claude" },
            "native_session_id": session, "transcript_path": path,
            "initial_transcript": initial_artifact, "hook": raw.record,
            "canonical_bootstrap_evidence": {"status": "unavailable",
                "reason": "successful native hook and retained baseline do not alone prove the canonical session row or live-origin boundary"},
        });
        self.started_sessions
            .insert(session.to_owned(), evidence.clone());
        Ok(json!({"reused": false, "evidence": evidence}))
    }

    fn source_session_projection(&self, source: &Value) -> FixtureResult<(Value, String)> {
        let origin = source
            .get("origin")
            .filter(|value| value.is_object())
            .ok_or("source lacks original origin")?;
        let scope = json!({"project": origin["project"], "repository": origin["repository"],
            "worktree": origin["worktree"], "branch": origin["branch"]});
        if scope
            .as_object()
            .is_none_or(|fields| fields.values().any(Value::is_null))
        {
            return Err(
                "source origin lacks an exact project/repository/worktree/branch mapping".into(),
            );
        }
        if self
            .initial_origin
            .as_ref()
            .is_some_and(|initial| *initial != scope)
        {
            return Err("scheduled source requires a distinct physical origin; multi-origin fixture mapping is unresolved".into());
        }
        let session = format!(
            "comparison-session-{}",
            &sha256(string(origin, "session")?.as_bytes())[..32]
        );
        Ok((scope, session))
    }

    fn project_source(&mut self, source: &Value, index: usize) -> FixtureResult<usize> {
        let (scope, session) = self.source_session_projection(source)?;
        if !self.started_sessions.contains_key(&session) {
            return Err("scheduled source requires its native SessionStart before append".into());
        }
        let source_id = string(source, "id")?;
        let revision = string(source, "revision")?;
        let content = string(source, "content")?;
        let native_id = format!(
            "comparison-source-{}",
            &sha256(format!("{source_id}\0{revision}").as_bytes())[..32]
        );
        let path = self.journey.transcript_path_for_session(&session);
        let mut file = io_result(
            fs::OpenOptions::new().append(true).open(&path),
            "open already initialized native transcript",
        )?;
        // This stable native timestamp represents scheduled order only; it is
        // never reused as canonical revision or host receipt time.
        let timestamp = format!("2026-02-01T00:{:02}:{:02}.000Z", index / 60, index % 60);
        if index >= 3600 {
            return Err("native fixture timestamp projection exceeds its explicit bound".into());
        }
        let start = io_result(file.metadata(), "native transcript start offset")?.len();
        let record = if self.journey.codex {
            json!({"timestamp": timestamp, "type": "event_msg",
                "payload": {"type": "agent_message", "message": content}})
        } else {
            json!({"type": "assistant", "cwd": self.journey.project,
                "sessionId": session, "uuid": native_id, "timestamp": timestamp,
                "message": {"id": native_id, "role": "assistant", "model": "host-comparison-fixture",
                    "content": [{"type": "text", "text": content}]}})
        };
        let mut bytes = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
        bytes.push(b'\n');
        let native_bytes_sha256 = sha256(&bytes);
        write_durable(
            &self.archive.join(format!("source-{index:06}.native.jsonl")),
            &bytes,
        )?;
        io_result(
            file.write_all(&bytes),
            "append scheduled native source exactly once",
        )?;
        io_result(file.sync_all(), "fsync scheduled native transcript")?;
        let end = io_result(file.metadata(), "native transcript end offset")?.len();
        if end.saturating_sub(start) != bytes.len() as u64 {
            return Err("native transcript append length mismatch".into());
        }
        if self.lane == Lane::Documentation {
            // Files carry the exact source content. Revision lineage chooses
            // the current owned file, while every prior input stays archived.
            let lineage = string(source, "lineage_id")?;
            let document = self
                .journey
                .project
                .join("docs")
                .join(format!("{}.md", &sha256(lineage.as_bytes())[..32]));
            let mut document_file = io_result(
                fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&document),
                "write current explicit documentation",
            )?;
            io_result(
                document_file.write_all(content.as_bytes()),
                "write exact documentation source content",
            )?;
            io_result(
                document_file.sync_all(),
                "fsync current explicit documentation",
            )?;
            write_durable(
                &self
                    .archive
                    .join(format!("source-{index:06}.documentation")),
                content.as_bytes(),
            )?;
        }
        self.initial_origin = Some(scope);
        let slot = self.projected.len();
        self.projected.push(ProjectedSource {
            source: source.clone(),
            session,
            path,
            start,
            end,
            native_bytes_sha256,
        });
        Ok(slot)
    }

    fn observe(&mut self, action: &Value, index: usize) -> FixtureResult<Value> {
        let (_, session) = self.source_session_projection(&action["source"])?;
        let session_start = self.ensure_native_session_started(&session)?;
        let slot = self.project_source(&action["source"], index)?;
        let projected = &self.projected[slot];
        let path = projected.path.clone();
        // Every appended source has a real Stop, including the first Claude
        // source after its now content-free SessionStart baseline.
        let (command, payload) = if self.journey.codex {
            (
                "hook-codex-stop",
                json!({"session_id": session,
                "turn_id": action["action_id"], "transcript_path": path, "cwd": self.journey.project,
                "hook_event_name": "Stop", "model": "host-comparison-fixture", "permission_mode": "default",
                "stop_hook_active": false, "last_assistant_message": null}),
            )
        } else {
            (
                "hook-stop",
                json!({"session_id": session, "transcript_path": path,
                "cwd": self.journey.project, "hook_event_name": "Stop", "stop_hook_active": false}),
            )
        };
        let raw = self.hook(command, &payload)?;
        if !raw.success() {
            return Err("native Stop failed; exact input/output retained".into());
        }
        let admitted = if self.lane.provider().is_some() {
            self.await_source(slot)?
        } else {
            // A disabled advisory lane must not contact a provider to fill a
            // measurement gap. The canonical-store reader remains separate.
            json!({"status": "unavailable", "reason": "canonical-store source read helper pending for control lane"})
        };
        let source = &self.projected[slot];
        Ok(
            json!({"native_session_start": session_start, "hooks": [raw.record], "admitted": admitted,
                "source_projection": {"scheduled_source": source.source, "native_session_id": source.session,
                    "path": source.path, "byte_start": source.start, "byte_end": source.end,
                    "native_bytes_sha256": source.native_bytes_sha256},
                "provider_contacted": admitted.get("provider_contacted").cloned().unwrap_or_else(||
                    if self.lane.provider().is_none() { json!(false) } else { Value::Null }),
                "terminal": admitted.get("terminal").cloned().unwrap_or(json!("native_hook_completed")),
                "successful_completion": admitted["successful_completion"] == true,
            }),
        )
    }

    fn await_source(&mut self, slot: usize) -> FixtureResult<Value> {
        let deadline = Instant::now() + SETTLEMENT_BUDGET;
        loop {
            before_deadline(deadline, "native source settlement")?;
            self.processes.sample()?;
            let source = &self.projected[slot];
            if let Some(journal) = self.journey.journal_for(self.lane == Lane::Ncm) {
                let mut cursor = None;
                let mut scanned = 0usize;
                loop {
                    before_deadline(deadline, "native source evidence page")?;
                    let page = journal
                        .inspect(&JournalInspectionFilterV1 {
                            limit: JOURNAL_INSPECTION_PAGE_LIMIT,
                            after_cursor: cursor,
                            ..JournalInspectionFilterV1::default()
                        })
                        .map_err(|error| format!("inspect actual observation journal: {error}"))?;
                    before_deadline(deadline, "native source evidence page")?;
                    scanned += page.rows.len();
                    if scanned > 100_000 {
                        return Err(
                            "canonical source evidence inspection exceeded bounded row limit"
                                .into(),
                        );
                    }
                    for row in &page.rows {
                        before_deadline(deadline, "native source evidence row")?;
                        if row.source_stream != "session_observation_store" {
                            continue;
                        }
                        let Some(admitted) = journal
                            .read_admitted_observation_by_idempotency(&row.idempotency_key)
                            .map_err(|error| error.to_string())?
                        else {
                            continue;
                        };
                        let payload: Value = serde_json::from_slice(&admitted.payload.bytes)
                            .map_err(|error| error.to_string())?;
                        let canonical: tracedecay_domain::observation::CanonicalObservationEnvelopeV1 =
                            serde_json::from_value(payload["canonical_payload"].clone()).map_err(|error| error.to_string())?;
                        canonical.validate().map_err(|error| error.to_string())?;
                        before_deadline(deadline, "native source evidence row")?;
                        let range = canonical.evidence().range();
                        if canonical.provider().as_str()
                            != if self.journey.codex {
                                "codex"
                            } else {
                                "claude"
                            }
                            || canonical.relations().session_id().as_str() != source.session
                            || range.start() != source.start
                            || range.end() > source.end
                            || range.end() <= source.start
                        {
                            continue;
                        }
                        if !row.state.is_terminal() {
                            continue;
                        }
                        let receipts = journal
                            .receipts_for(&row.observation_id)
                            .map_err(|error| error.to_string())?;
                        before_deadline(deadline, "native source receipt evidence")?;
                        let receipt_rows: Vec<_> = receipts.iter().map(|receipt| json!({
                            "receipt_id": receipt.receipt_id.as_str(), "observation_id": receipt.observation_id.as_str(),
                            "idempotency_key": receipt.idempotency_key.as_str(), "payload_sha256": receipt.payload_sha256,
                            "extensions_digest": receipt.extensions_digest, "provider_id": receipt.provider_id.as_str(),
                            "provider_instance_id": receipt.provider_instance_id, "registration_revision": receipt.registration_revision,
                            "state_generation_before": receipt.state_generation_before, "state_generation_after": receipt.state_generation_after,
                            "attempt_number": receipt.attempt_number, "outcome": receipt.outcome.as_wire(),
                            "committed_effect": receipt.committed_effect.as_wire(), "provider_effect_summary": receipt.provider_effect_summary,
                            "provider_receipt_digest": receipt.provider_receipt_digest,
                            "started_at_unix_micros": receipt.started_at_unix_micros,
                            "finished_at_unix_micros": receipt.finished_at_unix_micros, "warnings": receipt.warnings,
                        })).collect();
                        let bytes_ref = write_durable(
                            &self.archive.join(format!("source-{slot:06}.admitted.json")),
                            &admitted.payload.bytes,
                        )?;
                        before_deadline(deadline, "native source evidence capture")?;
                        let latest = receipts.last();
                        return Ok(json!({"status": "observed", "admitted_payload": bytes_ref,
                            "canonical_payload": payload["canonical_payload"], "original_scope": admitted_scope(&admitted),
                            "canonical_source_event_id": admitted.source.source_event_id,
                            "canonical_provider_id": canonical.provider().as_str(),
                            "canonical_session_id": canonical.relations().session_id().as_str(),
                            "stable_record_id": canonical.stable_record_id().as_str(),
                            "canonical_source_revision": canonical.evidence().revision(),
                            "source_sequence": admitted.source.source_sequence.0,
                            "journal_state": row.state.as_wire(), "receipts": receipt_rows,
                            "provider_contacted": receipts.iter().any(|receipt| receipt.provider_instance_id.is_some()),
                            "terminal": latest.map(|receipt| receipt.outcome.as_wire()).unwrap_or(row.state.as_wire()),
                            "successful_completion": latest.is_some_and(|receipt| matches!(receipt.outcome,
                                ObservationOutcomeV1::Applied | ObservationOutcomeV1::DuplicateAcknowledged)),
                        }));
                    }
                    cursor = page.next_cursor;
                    if cursor.is_none() {
                        break;
                    }
                }
            }
            if Instant::now() >= deadline {
                return Err("native source did not acquire terminal admitted evidence within original settlement deadline".into());
            }
            std::thread::sleep(JOURNAL_POLL_INTERVAL);
        }
    }

    /// Setup metadata only. The caller must first establish this real native
    /// session through the shipped Start hook. The host then authorizes that
    /// canonical row and live boundary again for this actual Health request.
    fn capture_setup_health(&mut self, canonical_provider_id: &str, native_session: &str) -> Value {
        let started = self.clock_ns();
        let captured = (|| -> FixtureResult<Value> {
            let selected = self
                .lane
                .provider()
                .ok_or("control lane cannot dispatch provider Health")?;
            if canonical_provider_id
                != if self.journey.codex {
                    "codex"
                } else {
                    "claude"
                }
                || !self.started_sessions.contains_key(native_session)
            {
                return Err(
                    "setup Health requires this fixture's already started real native session"
                        .into(),
                );
            }
            let declaration = production_provider_numeric_declaration_for_test(selected.id())
                .map_err(|error| format!("production provider declaration: {error}"))?
                .ok_or("production provider declaration is unsupported")?;
            if declaration.provider_id != selected.id()
                || declaration.declared_registration_revision == 0
            {
                return Err(
                    "production provider declaration does not bind selected operator lane".into(),
                );
            }
            let profile =
                tracedecay_daemon_identity::profile_identity::load_existing(&self.journey.profile)
                    .map_err(|error| format!("read existing Health profile authority: {error}"))?;
            let request = ProviderControlRequestV1::Health(ProviderHealthRequestV1 {
                state: ProviderControlStateSelectorV1::CanonicalSession {
                    provider_id: declaration.provider_id.clone(),
                    registration_revision: declaration.declared_registration_revision,
                    canonical_provider_id: canonical_provider_id.to_owned(),
                    session_id: native_session.to_owned(),
                },
                requested_checks: vec![
                    ProviderControlHealthCheckV1::Protocol,
                    ProviderControlHealthCheckV1::State,
                    ProviderControlHealthCheckV1::Scope,
                    ProviderControlHealthCheckV1::Capacity,
                    ProviderControlHealthCheckV1::Persistence,
                    ProviderControlHealthCheckV1::Recovery,
                    ProviderControlHealthCheckV1::Privacy,
                ],
            });
            // This is the caller's real metadata operation ID, never a guessed
            // host-minted recall request ID, operation UUID, or state generation.
            let request_id = RequestId::new(format!(
                "comparison.setup.health.{}",
                sha256(string(&self.invocation, "case_trial_id")?.as_bytes())
            ))
            .map_err(|error| format!("setup Health caller request ID: {error}"))?;
            let cancellation =
                CancellationSignal::active(format!("{}.cancel", request_id.as_str()))
                    .map_err(|error| format!("setup Health cancellation identity: {error}"))?;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|error| format!("setup Health operation clock: {error}"))?;
            let observed_at = UtcMicros(
                i64::try_from(now.as_micros())
                    .map_err(|_| "setup Health clock exceeds supported range")?,
            );
            request
                .validate_at(observed_at)
                .map_err(|error| format!("setup Health request: {error}"))?;
            let deadline = Deadline::new(UtcMicros(
                observed_at
                    .0
                    .checked_add((SETUP_HEALTH_MILLIS * 1_000) as i64)
                    .ok_or("setup Health deadline exceeds supported range")?,
            ))
            .map_err(|error| format!("setup Health deadline: {error}"))?;
            let rpc = self.controlled_provider_rpc(
                request_id.clone(),
                request.clone(),
                observed_at,
                deadline.clone(),
                cancellation,
            )?;
            // From here onward, every capture/refusal keeps the dispatched typed
            // result and its separate durable RPC record. Nothing is retried.
            let accepted = if let Some(error) = &rpc.capture_error {
                Err(format!(
                    "actual setup Health RPC evidence could not be retained: {error}"
                ))
            } else {
                rpc.result
                    .as_ref()
                    .map_err(|error| format!("actual setup Health RPC client outcome: {error:?}"))
                    .and_then(|response| {
                        accept_fresh_setup_health_response(
                            response,
                            &request_id,
                            &request,
                            observed_at,
                            &deadline,
                            profile.profile_id().as_str(),
                            &self.project_id,
                            native_session,
                            &declaration,
                        )
                    })
            };
            Ok(json!({"status": "captured", "rpc": rpc.record,
                "declared_policy": declaration,
                "declaration_scope": "production request expectations; none of these fields alone establishes a live provider",
                "actual_health_binding": accepted.unwrap_or_else(|reason| json!({"status": "unavailable", "reason": reason})),
                "complete_pin_status": {"status": "unavailable",
                    "reason": "actual executable/model/state-schema pin binding remains separate from Health numeric acceptance"},
            }))
        })();
        let ended = self.clock_ns();
        let mut evidence =
            captured.unwrap_or_else(|reason| json!({"status": "unavailable", "reason": reason}));
        evidence["timing"] = json!({"phase": "provider_setup_health_evidence", "status": "measured",
            "clock": "fixture_process_monotonic_elapsed", "start_monotonic_ns": started,
            "end_monotonic_ns": ended, "elapsed_ns": ended.saturating_sub(started)});
        evidence
    }

    fn actual_context_store(&self, scope: &OwnedExactScope) -> FixtureResult<(PathBuf, Value)> {
        let profile =
            tracedecay_daemon_identity::profile_identity::load_existing(&self.journey.profile)
                .map_err(|error| format!("read existing isolated profile authority: {error}"))?;
        if scope.profile_id != profile.profile_id().as_str() || scope.project_id != self.project_id
        {
            return Err(
                "actual delivery scope does not match isolated profile/project authority".into(),
            );
        }
        let registered = &self.initial_evidence["registered_project"];
        let project = &registered["project"];
        let root = io_result(
            fs::canonicalize(&self.journey.project),
            "canonical fixture project",
        )?;
        if string(project, "project_id")? != self.project_id
            || Path::new(string(project, "canonical_root")?) != root
        {
            return Err(
                "registered project evidence does not match the actual fixture project".into(),
            );
        }
        let matches: Vec<_> = registered["stores"]
            .as_array()
            .ok_or("registered project lacks actual store records")?
            .iter()
            .filter(|entry| {
                entry["store"]["store_kind"] == "code_project"
                    && entry["store"]["project_id"] == self.project_id
            })
            .collect();
        let [selected] = matches.as_slice() else {
            return Err("registered canonical project store is absent or ambiguous".into());
        };
        let store = &selected["store"];
        if store["storage_mode"] != "profile_sharded" {
            return Err(
                "actual project store does not use the supported profile-sharded layout".into(),
            );
        }
        let relative = Path::new(string(store, "store_relpath")?);
        if relative
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
        {
            return Err("actual registry store path is not a contained relative path".into());
        }
        let observed_path = profile.profile_root().join(relative);
        let production_path = tracedecay_runtime_core::storage::profile_sharded_data_root(
            profile.profile_root(),
            &self.project_id,
        );
        if observed_path != production_path {
            return Err(
                "actual registry store path and production layout resolver disagree".into(),
            );
        }
        let data_root = io_result(
            fs::canonicalize(&observed_path),
            "canonical registered data root",
        )?;
        if !data_root.is_dir()
            || !data_root.starts_with(profile.profile_root())
            || data_root != observed_path
        {
            return Err(
                "actual canonical project store escapes or redirects its registered path".into(),
            );
        }
        Ok((
            data_root.clone(),
            json!({
                "registered_project": project, "registered_store": selected,
                "profile_root": profile.profile_root(), "profile_id": profile.profile_id().as_str(),
                "canonical_store_data_root": data_root,
            }),
        ))
    }

    fn capture_retained_context_trace(&mut self, advisory: &Value) -> Value {
        let started = self.clock_ns();
        let captured = (|| -> FixtureResult<Value> {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|error| format!("trace evidence operation clock: {error}"))?;
            let now_micros = i64::try_from(now.as_micros())
                .map_err(|_| "trace evidence operation clock exceeds supported range")?;
            let deadline_micros = now_micros
                .checked_add((TRACE_EVIDENCE_READ_MILLIS * 1_000) as i64)
                .ok_or("trace evidence deadline exceeds supported range")?;
            let control = OperationControl::new(
                deadline_micros,
                TRACE_EVIDENCE_READ_MILLIS,
                Default::default(),
            );
            let provider_id = string(advisory, "provider_id")?;
            let expected = self
                .lane
                .provider()
                .ok_or("control lane has no advisory provider")?;
            if provider_id != expected.id() {
                return Err(
                    "delivered advisory provider does not match selected operator lane".into(),
                );
            }
            let scope = observed_delivery_scope(advisory)?;
            let revision = advisory["registration_revision"]
                .as_u64()
                .ok_or("actual advisory registration revision is unavailable")?;
            let correlation = &advisory["recall_trace"];
            let request_id = string(correlation, "request_id")?;
            let trace_ref = string(correlation, "trace_ref")?;
            let authority = self.authority()?;
            let (data_root, store_evidence) = self.actual_context_store(&scope)?;
            control
                .snapshot()
                .map_err(|code| format!("trace evidence pre-read stopped: {code:?}"))?;
            // This synchronous fixture thread is outside an async executor. The
            // reader opens one read-only WAL transaction with this separate
            // evidence operation's original control, never a provider call.
            let trace = read_retained_context_trace_for_test(
                &data_root,
                trace_ref,
                request_id,
                provider_id,
                revision,
                &scope,
                &control,
            )
            .map_err(|error| format!("actual retained recall trace: {error}"))?;
            control
                .snapshot()
                .map_err(|code| format!("trace evidence capture stopped: {code:?}"))?;
            let bytes = serde_json::to_vec(&trace).map_err(|error| error.to_string())?;
            self.command_sequence += 1;
            let artifact = write_durable(
                &self
                    .archive
                    .join(format!("context-trace-{:06}.json", self.command_sequence)),
                &bytes,
            )?;
            control
                .snapshot()
                .map_err(|code| format!("trace evidence capture stopped: {code:?}"))?;
            Ok(json!({"status": "observed", "artifact": artifact,
                "correlation": correlation,
                "provider_id": trace.provider_id, "registration_revision": trace.registration_revision,
                "delivery_scope": advisory["canonical_history_replay"]["delivery_scope"],
                "store_evidence": store_evidence,
                "daemon_identity": {"pid": authority.pid, "process_run_id": authority.process_run_id},
                "token_summary_scope": "retained production context pack; excludes enclosing CLI serialization and all other final blocks",
                "candidate_trace_scope": "actual retained trace items; comparison stage flags and delivery claims have not been inferred",
            }))
        })();
        let ended = self.clock_ns();
        let mut evidence =
            captured.unwrap_or_else(|reason| json!({"status": "unavailable", "reason": reason}));
        evidence["timing"] = json!({"phase": "retained_trace_evidence_read", "status": "measured",
            "clock": "fixture_process_monotonic_elapsed", "start_monotonic_ns": started,
            "end_monotonic_ns": ended, "elapsed_ns": ended.saturating_sub(started)});
        evidence["operation_budget_ms"] = json!(TRACE_EVIDENCE_READ_MILLIS);
        evidence
    }

    fn capture_context_projection(
        &mut self,
        result: &Value,
        payload_content_index: usize,
        payload: &Value,
    ) -> FixtureResult<Value> {
        let projection = project_delivered_context_for_test(result, payload_content_index)
            .map_err(|error| format!("actual final context projector: {error}"))?;
        let bytes = serde_json::to_vec(&projection).map_err(|error| error.to_string())?;
        self.command_sequence += 1;
        let artifact = write_durable(
            &self.archive.join(format!(
                "context-projection-{:06}.json",
                self.command_sequence
            )),
            &bytes,
        )?;
        let trace = if let Some(advisory) = payload.get("advisory_provider_memory") {
            self.capture_retained_context_trace(advisory)
        } else {
            json!({"status": "unavailable", "reason": "actual payload carries no advisory recall correlation"})
        };
        Ok(json!({"status": "observed", "artifact": artifact,
            "payload_content_index": payload_content_index, "retained_trace": trace,
            "projection_scope": "decoded original result and production semantic member classifications; raw stdout remains the exact CLI byte boundary",
        }))
    }

    fn recall(&mut self, action: &Value) -> FixtureResult<Value> {
        let task = string(&action["query"], "text")?;
        let session = self.destination_session()?;
        let session_start = self
            .started_sessions
            .get(&session)
            .cloned()
            .ok_or("scheduled recall destination lacks its completed native SessionStart")?;
        let raw = self.tool("tracedecay_context", &json!({
            "task": task, "format": "json", "memory_limit": self.invocation["budgets"]["requested_candidates"],
            // Canonical explicit facts remain common to all four lanes. The
            // operator composition, not include_memory=false, gates providers.
            "include_memory": true, "_meta": {"session_id": session},
        }), self.invocation["budgets"]["provider_deadline_ms"].as_u64())?;
        if !raw.record["cutoff"].is_null() {
            return Err("context command exceeded capture bound; cutoff and complete raw artifacts retained".into());
        }
        let capture_started = self.clock_ns();
        let value = raw.decode()?;
        // Inspect an independent decoded copy; never strip, join, re-render,
        // or use its serialization as the captured stdout/token boundary.
        let blocks: Vec<_> = value["content"]
            .as_array()
            .ok_or("context result lacks final content array")?
            .iter()
            .enumerate()
            .filter_map(|(index, block)| {
                block["text"].as_str().map(|text| {
                    json!({
                        "content_index": index, "type": block["type"], "text": text,
                        "utf8_bytes": text.len(), "sha256": sha256(text.as_bytes()),
                    })
                })
            })
            .collect();
        let blocks_bytes = serde_json::to_vec(&blocks).map_err(|error| error.to_string())?;
        self.command_sequence += 1;
        let blocks_ref = write_durable(
            &self
                .archive
                .join(format!("context-blocks-{:06}.json", self.command_sequence)),
            &blocks_bytes,
        )?;
        let payload = context_payload_index(&value);
        if self.lane.provider().is_none()
            && payload
                .as_ref()
                .is_ok_and(|(_, payload)| payload.get("advisory_provider_memory").is_some())
        {
            return Err(
                "control lane unexpectedly received an advisory provider lane; raw result retained"
                    .into(),
            );
        }
        let projection = payload
            .and_then(|(index, payload)| self.capture_context_projection(&value, index, &payload))
            .unwrap_or_else(|reason| json!({"status": "unavailable", "reason": reason}));
        let capture_ended = self.clock_ns();
        Ok(
            json!({"raw_command": raw.record, "final_text_blocks": blocks_ref,
                "context_projection": projection,
                "evidence_capture_timing": {"phase": "host_evidence_capture", "status": "measured",
                    "clock": "fixture_process_monotonic_elapsed", "start_monotonic_ns": capture_started,
                    "end_monotonic_ns": capture_ended, "elapsed_ns": capture_ended.saturating_sub(capture_started)},
                "request_session_projection": session,
                "request_session_start": session_start,
                "terminal": if value["isError"] == true { "tool_result_error" } else { "tool_result_completed" },
                "terminal_scope": "complete CLI ToolResult; provider terminal unavailable without retained host trace",
                "provider_contacted": if self.lane.provider().is_none() { json!(false) } else { Value::Null },
                "successful_completion": false,
                "delivery_status": {"status": "unavailable",
                    "reason": "complete CLI wrapper token accounting and comparison candidate-stage mapping remain unavailable; raw stdout and actual semantic projection are retained separately"},
                "documentation_delivery_status": if self.lane == Lane::Documentation {
                    json!({"status": "unavailable", "reason": "actual explicit documentation read/delivery route pending"})
                } else { Value::Null },
            }),
        )
    }

    fn restart(&mut self) -> FixtureResult<Value> {
        let before = self.authority()?;
        let stopped = self.stop_daemon()?;
        let started = self.start_daemon()?;
        let after = self.authority()?;
        if before.pid == after.pid || before.process_run_id == after.process_run_id {
            return Err("restart did not establish a new actual daemon identity".into());
        }
        self.configuration_revision()?;
        let readiness = self.startup_barrier()?;
        Ok(
            json!({"terminal": "daemon_restart_completed", "provider_contacted":
            if self.lane.provider().is_none() { json!(false) } else { Value::Null },
            "successful_completion": true, "before": {"pid": before.pid, "process_run_id": before.process_run_id},
            "stopped": stopped, "after": started, "readiness": readiness}),
        )
    }

    fn action(&mut self, action: &Value, index: usize) -> Value {
        let start = self.clock_ns();
        let command_start = self.commands.len();
        let projected_start = self.projected.len();
        let child_start = self.processes.children.len();
        let signal_start = self.processes.signals.len();
        let operation = action["step"]["action"].as_str().unwrap_or("");
        let phase = match operation {
            "observe" => "observation",
            "restart" => "restart",
            _ => "host_request",
        };
        let outcome = match operation {
            "observe" => self.observe(action, index),
            "recall" => self.recall(action),
            "restart" => self.restart(),
            _ => Err(format!(
                "scheduled {operation} has no reviewed concrete fixture control binding"
            )),
        };
        let end = self.clock_ns();
        let fixture_timing = json!({"phase": "fixture_action", "status": "measured",
            "clock": "fixture_process_monotonic_elapsed", "start_monotonic_ns": start,
            "end_monotonic_ns": end, "elapsed_ns": end.saturating_sub(start)});
        let mut timing = if operation == "recall" {
            // The read-only trace capture runs after the only scheduled query.
            // Its SQL, decoding, and fsync time must not inflate host latency.
            self.commands[command_start..]
                .iter()
                .find(|command| command["label"] == "tracedecay_context")
                .map(|command| command["timing"].clone())
                .unwrap_or_else(|| json!({"status": "unavailable",
                    "reason": "scheduled context command did not acquire a measured process interval"}))
        } else {
            fixture_timing.clone()
        };
        timing["phase"] = json!(phase);
        let mut result = json!({"input": action, "action": operation, "request_id": action["step"]["query_id"],
            "selected_provider": null, "phase": phase, "delivery": null,
            "process_group": self.processes.root.public(), "expectation": "indeterminate",
            "timing": timing, "fixture_action_timing": fixture_timing,
            "raw_commands": &self.commands[command_start..]});
        match outcome {
            Ok(evidence) => {
                result["status"] = json!("completed");
                result["terminal"] = evidence["terminal"].clone();
                result["provider_contacted"] = evidence["provider_contacted"].clone();
                result["successful_completion"] = evidence["successful_completion"].clone();
                result["host_evidence"] = evidence;
                result["comparison_evidence_status"] = json!({"status": "incomplete",
                    "reason": "actual selected pin and complete host delivery attribution not yet bound"});
            }
            Err(error) => {
                let started = self.commands.len() > command_start
                    || self.projected.len() > projected_start
                    || self.processes.children.len() > child_start
                    || self.processes.signals.len() > signal_start;
                result["status"] = json!(if started { "censored" } else { "unexecuted" });
                result["terminal"] = Value::Null;
                result["provider_contacted"] = if self.lane.provider().is_none() {
                    json!(false)
                } else {
                    Value::Null
                };
                result["successful_completion"] = json!(false);
                result["reason"] = json!(error);
                if started && result["timing"]["status"] == "measured" {
                    result["timing"]["elapsed_at_cutoff_ns"] =
                        result["timing"]["elapsed_ns"].clone();
                }
            }
        }
        result
    }
}

fn validate_invocation(invocation: &Value) -> FixtureResult<()> {
    let case = &invocation["case"];
    let case_id = string(case, "id")?;
    if invocation["case_id"] != case_id {
        return Err("invocation case ID mismatch".into());
    }
    let sources = case["sources"]
        .as_array()
        .ok_or("missing complete source array")?;
    let queries = case["queries"]
        .as_array()
        .ok_or("missing complete query array")?;
    let steps = case["steps"]
        .as_array()
        .ok_or("missing complete ordered steps")?;
    let actions = invocation["actions"]
        .as_array()
        .ok_or("missing ordered actions")?;
    if actions.len() != steps.len() {
        return Err("invocation action denominator mismatch".into());
    }
    for (index, (action, step)) in actions.iter().zip(steps).enumerate() {
        let referenced = |rows: &[Value], key: &str| {
            step[key]
                .as_str()
                .and_then(|id| rows.iter().find(|row| row["id"] == id))
                .cloned()
                .unwrap_or(Value::Null)
        };
        if *action
            != json!({"action_id": format!("{case_id}/{index}"), "step": step,
            "source": referenced(sources, "source_id"), "query": referenced(queries, "query_id")})
        {
            return Err(format!("scheduled action {index} was changed or reordered"));
        }
    }
    // These are requested limits, checked before execution rather than adjusted
    // after seeing a response. Actual effective limits require host evidence.
    if invocation["budgets"]
        != json!({"requested_candidates": 8, "effective_candidates": 5,
        "advisory_tokens": 8192, "total_context_tokens": 128000,
        "provider_deadline_ms": 5000, "advisory_slice_ms": 2000})
        || invocation["tokenizer"]
            != json!({"identity": "tiktoken.o200k_base", "revision": "tiktoken-rs-0.12"})
    {
        return Err("scheduled budgets or tokenizer differ from frozen comparison contract".into());
    }
    Ok(())
}

fn unexecuted_action(action: &Value, reason: &str) -> Value {
    json!({"input": action, "action": action["step"]["action"], "request_id": action["step"]["query_id"],
        "status": "unexecuted", "terminal": null, "provider_contacted": null,
        "selected_provider": null, "phase": "host_request", "delivery": null,
        "successful_completion": false, "expectation": "indeterminate", "reason": reason,
        "timing": {"status": "unmeasured", "reason": reason}})
}

fn replay_invocation(
    invocation: Value,
    processes: &mut OwnedProcesses,
    channel: &mut Channel,
) -> FixtureResult<Value> {
    let actions = invocation["actions"]
        .as_array()
        .ok_or("missing scheduled action array")?
        .clone();
    let mut fixture = HostFixture::allocate(invocation, processes)?;
    let setup_error = fixture.setup().err();
    let metadata = fixture.metadata(setup_error.as_deref());
    channel.durable("metadata", None, metadata.clone())?;
    let mut recorded = Vec::with_capacity(actions.len());
    let mut stop = setup_error;
    for (index, action) in actions.iter().enumerate() {
        let result = if let Some(reason) = &stop {
            unexecuted_action(action, reason)
        } else {
            // No action retry. One native logical action can own its required
            // SessionStart and Stop subprocesses and their durable receipts.
            fixture.action(action, index)
        };
        if result["status"] != "completed" && stop.is_none() {
            stop = Some(
                result["reason"]
                    .as_str()
                    .unwrap_or("scheduled action did not complete")
                    .to_owned(),
            );
        }
        let mut bytes = serde_json::to_vec(&result).map_err(|error| error.to_string())?;
        bytes.push(b'\n');
        write_durable(
            &fixture.archive.join(format!("action-{index:06}.json")),
            &bytes,
        )?;
        // The next scheduled operation cannot start until the parent has
        // fsynced exactly this action payload and acknowledged its digest.
        channel.durable("action", Some(index), result.clone())?;
        recorded.push(result);
    }
    let mut result = metadata;
    result["status"] = json!(if stop.is_some() {
        "unexecuted"
    } else {
        "completed"
    });
    result["reason"] = json!(stop);
    result["actions"] = json!(recorded);
    result["capture_completeness"] = json!({"status": "incomplete",
        "reason": "full selected-pin, canonical-state, and rendered delivery bindings require reviewed evidence helpers"});
    Ok(result)
}

/// Run only from the reviewed parent connector, which owns the nonce socket,
/// exclusive artifact parent and test executable process group. There is no
/// built-in case selection, held-out input reader, or assertion journey here.
#[test]
#[ignore = "parent-driven real host comparison; requires nonce socket and isolated artifact parent"]
fn host_comparison_fixture_entry() -> FixtureResult<()> {
    let mut channel = Channel::connect()?;
    let create = channel.receive_kind("create")?;
    let invocation = create
        .get("body")
        .filter(|body| body.is_object())
        .ok_or("create frame lacks whole invocation")?
        .clone();
    let mut processes = OwnedProcesses::new()?;
    let replay = catch_unwind(AssertUnwindSafe(|| {
        replay_invocation(invocation.clone(), &mut processes, &mut channel)
    }));
    let result = match replay {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => {
            json!({"case_trial_id": invocation["case_trial_id"], "status": "unexecuted",
            "reason": error, "process_group": processes.root.public(), "actions": [],
            "stop_remaining_reason": "fixture failed; previously acknowledged events and raw artifacts remain authoritative"})
        }
        Err(panic) => {
            let reason = panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|text| (*text).to_owned()))
                .unwrap_or_else(|| "non-string fixture panic".into());
            json!({"case_trial_id": invocation["case_trial_id"], "status": "unexecuted", "reason": reason,
                "process_group": processes.root.public(), "actions": [],
                "stop_remaining_reason": "fixture panicked; previously acknowledged events and raw artifacts remain authoritative"})
        }
    };
    // Finish is a replay boundary. The parent explicitly requests close; only
    // then do the actual joins produce the closed record used for next-case
    // admission. On channel failure, cleanup still runs before returning.
    let finish = channel.send("finish", None, result);
    let close_request = finish.and_then(|_| channel.receive_kind("close"));
    let cleanup = processes.close();
    let artifact_parent = PathBuf::from(
        std::env::var_os("TRACEDECAY_HOST_COMPARISON_ARTIFACT_ROOT")
            .ok_or("comparison artifact parent disappeared")?,
    );
    let cleanup_bytes = serde_json::to_vec(&cleanup).map_err(|error| error.to_string())?;
    let cleanup_artifact = write_durable(
        &artifact_parent.join(format!("host-fixture-cleanup-{}.json", std::process::id())),
        &cleanup_bytes,
    );
    let closed = channel.send("closed", None, cleanup.clone());
    close_request?;
    cleanup_artifact?;
    closed?;
    if cleanup["status"] != "completed" {
        return Err("owned child cleanup could not prove completion; artifacts preserved".into());
    }
    Ok(())
}

#[cfg(test)]
mod health_evidence_tests {
    use super::*;

    #[test]
    fn fresh_execution_rejects_stale_widened_and_interrupted_receipts() {
        let observed_at = UtcMicros(100);
        let original_deadline = Deadline::new(UtcMicros(200)).unwrap();
        let complete = OperationReceipt::completed(
            UtcMicros(110),
            UtcMicros(150),
            Deadline::new(UtcMicros(180)).unwrap(),
            Default::default(),
        )
        .unwrap();
        validate_fresh_health_execution(&complete, observed_at, &original_deadline)
            .expect("fresh complete receipt within a narrower deadline");
        let variants = [
            OperationReceipt {
                started_at: UtcMicros(99),
                ..complete.clone()
            },
            OperationReceipt {
                ended_at: UtcMicros(201),
                ..complete.clone()
            },
            OperationReceipt {
                effective_deadline: Deadline::new(UtcMicros(201)).unwrap(),
                ..complete.clone()
            },
            OperationReceipt {
                ended_at: UtcMicros(181),
                ..complete.clone()
            },
            OperationReceipt {
                termination: OperationTermination::Partial,
                ..complete.clone()
            },
            OperationReceipt {
                cancellation: Some(tracedecay_contracts::CancellationObservation {
                    stage: tracedecay_contracts::CancellationStage::DuringRead,
                    observed_at: UtcMicros(130),
                }),
                ..complete
            },
        ];
        for receipt in variants {
            assert!(
                validate_fresh_health_execution(&receipt, observed_at, &original_deadline).is_err()
            );
        }
    }

    fn declaration(provider: &str) -> ProviderNumericDeclarationForTestV1 {
        production_provider_numeric_declaration_for_test(provider)
            .expect("pure production declaration")
            .expect("supported test provider")
    }

    // Synthetic wire evidence exercises only the acceptance gate. No fixture
    // below is dispatched or reported as an actual provider observation.
    fn gate_fixture(
        declaration: &ProviderNumericDeclarationForTestV1,
    ) -> (
        ProviderControlRequestV1,
        ProviderControlScopeV1,
        ProviderControlResultV1,
    ) {
        let scope = ProviderControlScopeV1 {
            profile_id: "profile.health-gate".into(),
            project_id: "project.health-gate".into(),
            repository_identity: "repository.health-gate".into(),
            worktree_identity: "worktree.health-gate".into(),
            branch_identity: "ref.health-gate".into(),
            agent_session_id: "session.health-gate".into(),
            resolved_scope_digest: format!("sha256:{}", "a".repeat(64)),
        };
        let request = ProviderControlRequestV1::Health(ProviderHealthRequestV1 {
            state: ProviderControlStateSelectorV1::CanonicalSession {
                provider_id: declaration.provider_id.clone(),
                registration_revision: declaration.declared_registration_revision,
                canonical_provider_id: "claude".into(),
                session_id: scope.agent_session_id.clone(),
            },
            requested_checks: vec![ProviderControlHealthCheckV1::Protocol],
        });
        let capabilities: Vec<_> = std::iter::once(COMMON_ADVISORY_PROFILE_ID)
            .chain(COMMON_ADVISORY_REQUIRED_CAPABILITIES.iter().copied())
            .map(|id| json!({"capability_id": id, "state": "available"}))
            .collect();
        let result = serde_json::from_value(json!({
            "provider_id": declaration.provider_id,
            "registration_revision": declaration.declared_registration_revision,
            "scope": scope, "operation_id": "01988c58-0000-7000-8000-000000000001",
            "idempotency_key": null, "terminal": "success", "diagnostic_id": null,
            "domain_detail": null, "warnings": [],
            "effect": {"state": "none", "committed_boundary": null,
                "state_generation_before": null, "state_generation_after": null,
                "committed_item_refs": [], "uncommitted_item_refs": [],
                "provider_receipt_digest": null, "reconciliation_action": null,
                "verification_digest": null, "duplicate_of_idempotency_key": null,
                "duplicate_of_operation_id": null},
            "result": {"operation": "health", "data": {
                "provider_instance_id": declaration.declared_provider_instance_id,
                "implementation_identity_digest": declaration.declared_implementation_identity_digest,
                "state_identity_digest": "b".repeat(64), "state_generation": 17,
                "scope_digest": scope.sha256(), "readiness": "ready",
                "capability_states": capabilities,
                "effective_limits_digest": declaration.declared_limits_digest,
                "backlog": 0, "recovery_state": "ready",
            }},
        })).expect("typed synthetic Health evidence");
        (request, scope, result)
    }

    #[test]
    fn all_eight_production_fields_match_the_production_health_digest() {
        for provider in [CONFIGURED_PROVIDER_ID, NCM_OBSERVER_PROVIDER_ID] {
            let declared = declaration(provider);
            let limits = &declared.declared_limits;
            let bytes: Vec<u8> = [
                limits.request_bytes,
                limits.response_bytes,
                limits.observation_batch_items,
                limits.recall_candidates,
                limits.concurrent_operations,
                limits.operation_millis,
                limits.snapshot_bytes,
                limits.inspection_items,
            ]
            .into_iter()
            .flat_map(u64::to_be_bytes)
            .collect();
            assert_eq!(sha256(&bytes), declared.declared_limits_digest);
            let (request, scope, response) = gate_fixture(&declared);
            let accepted = accept_health_numeric_evidence(&request, &scope, &declared, &response)
                .expect("complete synthetic binding admits declared numbers");
            assert_eq!(
                accepted["effective_limits"],
                serde_json::to_value(limits).unwrap()
            );
            assert_eq!(accepted["state_generation"], 17);
        }
    }

    fn lower_one_limit(limits: &mut ProviderDeclaredLimitsForTestV1, index: usize) {
        let field = match index {
            0 => &mut limits.request_bytes,
            1 => &mut limits.response_bytes,
            2 => &mut limits.observation_batch_items,
            3 => &mut limits.recall_candidates,
            4 => &mut limits.concurrent_operations,
            5 => &mut limits.operation_millis,
            6 => &mut limits.snapshot_bytes,
            7 => &mut limits.inspection_items,
            _ => panic!("all eight fields are enumerated"),
        };
        assert!(
            *field > 0,
            "production declaration has a finite positive ceiling"
        );
        *field -= 1;
    }

    #[test]
    fn every_lower_negotiated_limit_remains_unavailable() {
        for provider in [CONFIGURED_PROVIDER_ID, NCM_OBSERVER_PROVIDER_ID] {
            let declared = declaration(provider);
            for index in 0..8 {
                let mut lower = declared.declared_limits.clone();
                lower_one_limit(&mut lower, index);
                let (request, scope, mut response) = gate_fixture(&declared);
                let ProviderControlOperationResultV1::Health(Some(health)) = &mut response.result
                else {
                    panic!("Health fixture");
                };
                health.effective_limits_digest = complete_limits_digest(&lower);
                assert!(
                    accept_health_numeric_evidence(&request, &scope, &declared, &response).is_err(),
                    "provider {provider}, changed field {index}"
                );
            }
        }
    }

    #[test]
    fn every_destination_dimension_must_match_independent_application_evidence() {
        let declared = declaration(CONFIGURED_PROVIDER_ID);
        for field in [
            "profile_id",
            "project_id",
            "repository_identity",
            "worktree_identity",
            "branch_identity",
            "agent_session_id",
            "resolved_scope_digest",
        ] {
            let (request, scope, mut response) = gate_fixture(&declared);
            let mut altered = serde_json::to_value(&response.scope).unwrap();
            altered[field] = if field == "resolved_scope_digest" {
                json!(format!("sha256:{}", "c".repeat(64)))
            } else {
                json!("different.valid-identity")
            };
            response.scope = serde_json::from_value(altered).unwrap();
            let ProviderControlOperationResultV1::Health(Some(health)) = &mut response.result
            else {
                panic!("Health fixture");
            };
            // Internal self-consistency cannot replace the independently bound
            // application/profile/native-session expectation.
            health.scope_digest = response.scope.sha256();
            assert!(
                accept_health_numeric_evidence(&request, &scope, &declared, &response).is_err(),
                "changed {field}"
            );
        }
    }

    #[test]
    fn missing_or_mismatched_health_never_admits_declared_numbers() {
        let declared = declaration(CONFIGURED_PROVIDER_ID);
        for (pointer, replacement) in [
            ("/provider_id", json!(NCM_OBSERVER_PROVIDER_ID)),
            (
                "/registration_revision",
                json!(declared.declared_registration_revision + 1),
            ),
            ("/terminal", json!("partial")),
            ("/result/data", Value::Null),
            ("/result/data/readiness", json!("degraded")),
            (
                "/result/data/provider_instance_id",
                json!("different-instance"),
            ),
            (
                "/result/data/implementation_identity_digest",
                json!("c".repeat(64)),
            ),
            (
                "/result/data/effective_limits_digest",
                json!("c".repeat(64)),
            ),
        ] {
            let (request, scope, response) = gate_fixture(&declared);
            let mut wire = serde_json::to_value(response).unwrap();
            *wire.pointer_mut(pointer).unwrap() = replacement;
            let response = serde_json::from_value(wire).expect("typed mismatch fixture");
            assert!(
                accept_health_numeric_evidence(&request, &scope, &declared, &response).is_err(),
                "changed {pointer}"
            );
        }
        assert!(
            production_provider_numeric_declaration_for_test("unsupported.provider")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn every_common_capability_must_be_available_in_the_actual_response() {
        let declared = declaration(CONFIGURED_PROVIDER_ID);
        for required in std::iter::once(COMMON_ADVISORY_PROFILE_ID)
            .chain(COMMON_ADVISORY_REQUIRED_CAPABILITIES.iter().copied())
        {
            for remove in [true, false] {
                let (request, scope, mut response) = gate_fixture(&declared);
                let ProviderControlOperationResultV1::Health(Some(health)) = &mut response.result
                else {
                    panic!("Health fixture");
                };
                if remove {
                    health
                        .capability_states
                        .retain(|capability| capability.capability_id != required);
                } else {
                    health
                        .capability_states
                        .iter_mut()
                        .find(|capability| capability.capability_id == required)
                        .unwrap()
                        .state = "unavailable".into();
                }
                assert!(
                    accept_health_numeric_evidence(&request, &scope, &declared, &response).is_err(),
                    "capability {required}, removed={remove}"
                );
            }
        }
    }
}

#[cfg(test)]
mod context_evidence_tests {
    use super::*;

    #[test]
    fn actual_payload_position_survives_warning_and_metrics_blocks() {
        let payload = json!({"host": "evidence", "future_field": [1, 2, 3]});
        let result = json!({"content": [
            {"type": "text", "text": "Warning: production diagnostic"},
            {"type": "text", "text": payload.to_string()},
            {"type": "text", "text": "tracedecay_metrics: before=99 after=42"},
        ], "isError": false, "unknown_result_field": "preserve"});
        let original = result.clone();
        let (index, observed) = context_payload_index(&result).expect("one actual JSON payload");
        assert_eq!(index, 1);
        assert_eq!(observed, payload);
        let projection =
            project_delivered_context_for_test(&result, index).expect("production projection");
        assert_eq!(projection.result, original);
        assert_eq!(projection.text_blocks.len(), 3);
        assert_eq!(projection.text_blocks[0].content_index, 0);
        assert_eq!(
            projection.text_blocks[2].text,
            "tracedecay_metrics: before=99 after=42"
        );
    }

    #[test]
    fn duplicate_or_absent_payloads_never_choose_a_position() {
        for blocks in [
            json!([]),
            json!([{"type": "text", "text": "warning only"}]),
            json!([{"type": "text", "text": "{}"}, {"type": "text", "text": "{}"}]),
            json!([{"type": "text", "text": "null"}, {"type": "text", "text": "{}"}]),
            json!([{"type": "resource", "text": "{}"}]),
        ] {
            assert!(context_payload_index(&json!({"content": blocks})).is_err());
        }
    }

    fn observed_advisory() -> Value {
        json!({"provider_id": "tracedecay.native", "registration_revision": 7,
            "canonical_history_replay": {
                "provider_id": "tracedecay.native", "registration_revision": 7,
                "batches": [], "delivery_scope": {
                    "profile_id": "actual-profile", "project_id": "actual-project",
                    "repository_identity": "actual-repository", "worktree_identity": "actual-worktree",
                    "branch_identity": "actual-branch", "agent_session_id": "actual-destination-session",
                    "resolved_scope_digest": format!("sha256:{}", "a".repeat(64)),
                },
            },
        })
    }

    #[test]
    fn empty_history_still_requires_the_complete_actual_destination_scope() {
        let advisory = observed_advisory();
        let scope = observed_delivery_scope(&advisory).expect("complete actual carrier");
        assert_eq!(scope.agent_session_id, "actual-destination-session");
        for field in [
            "profile_id",
            "project_id",
            "repository_identity",
            "worktree_identity",
            "branch_identity",
            "agent_session_id",
            "resolved_scope_digest",
        ] {
            let mut missing = advisory.clone();
            missing["canonical_history_replay"]["delivery_scope"]
                .as_object_mut()
                .unwrap()
                .remove(field);
            assert!(
                observed_delivery_scope(&missing).is_err(),
                "missing {field}"
            );
        }
    }

    #[test]
    fn replay_and_advisory_registration_must_reconcile() {
        for (pointer, replacement) in [
            ("/provider_id", json!("ncm")),
            ("/registration_revision", json!(8)),
            ("/registration_revision", json!(0)),
            ("/canonical_history_replay/provider_id", Value::Null),
            (
                "/canonical_history_replay/registration_revision",
                Value::Null,
            ),
            (
                "/canonical_history_replay/delivery_scope/resolved_scope_digest",
                json!("unvalidated"),
            ),
        ] {
            let mut advisory = observed_advisory();
            *advisory.pointer_mut(pointer).unwrap() = replacement;
            assert!(
                observed_delivery_scope(&advisory).is_err(),
                "mismatched {pointer}"
            );
        }
    }
}

#[cfg(test)]
mod transport_tests {
    use super::*;

    fn pair() -> (Channel, UnixStream) {
        let (stream, parent) = UnixStream::pair().expect("local test socket pair");
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("test read bound");
        stream
            .set_write_timeout(Some(Duration::from_secs(2)))
            .expect("test write bound");
        parent
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("peer read bound");
        parent
            .set_write_timeout(Some(Duration::from_secs(2)))
            .expect("peer write bound");
        (
            Channel {
                stream,
                nonce: "a".repeat(64),
            },
            parent,
        )
    }

    fn frame(parent: &mut UnixStream, value: &Value) {
        let bytes = serde_json::to_vec(value).expect("test frame");
        parent
            .write_all(&(bytes.len() as u64).to_be_bytes())
            .expect("test header");
        parent.write_all(&bytes).expect("test payload");
    }

    #[test]
    fn rejects_oversized_declaration_without_waiting_for_a_payload() {
        let (mut channel, mut parent) = pair();
        parent
            .write_all(&(MAX_FRAME_BYTES as u64 + 1).to_be_bytes())
            .expect("oversized header");
        assert!(channel.receive().unwrap_err().contains("outside"));
    }

    #[test]
    fn rejects_wrong_nonce_before_accepting_invocation() {
        let (mut channel, mut parent) = pair();
        frame(
            &mut parent,
            &json!({"protocol": PROTOCOL, "nonce": "b".repeat(64), "kind": "hello"}),
        );
        assert!(channel.receive_kind("hello").unwrap_err().contains("nonce"));
    }

    #[test]
    fn accepts_fragmented_header_and_payload_within_one_deadline() {
        let (mut channel, mut parent) = pair();
        let value = json!({"protocol": PROTOCOL, "nonce": "a".repeat(64), "kind": "hello"});
        let expected = value.clone();
        let peer = std::thread::spawn(move || {
            let payload = serde_json::to_vec(&value).expect("fragmented frame");
            let bytes = [
                (payload.len() as u64).to_be_bytes().as_slice(),
                payload.as_slice(),
            ]
            .concat();
            for chunk in bytes.chunks(3) {
                parent.write_all(chunk).expect("write fragment");
            }
        });
        assert_eq!(
            channel
                .receive_until(Instant::now() + Duration::from_secs(2))
                .expect("bounded fragmented frame"),
            expected
        );
        peer.join().expect("joined fragmented-frame peer");
    }

    #[test]
    fn partial_payload_progress_does_not_restart_the_original_frame_deadline() {
        let (mut channel, mut parent) = pair();
        let (ready_sender, ready_receiver) = std::sync::mpsc::channel();
        let (go_sender, go_receiver) = std::sync::mpsc::channel();
        let peer = std::thread::spawn(move || {
            let payload = serde_json::to_vec(&json!({"protocol": PROTOCOL,
                "nonce": "a".repeat(64), "kind": "hello", "padding": "x".repeat(128)}))
            .expect("drip frame");
            parent
                .write_all(&(payload.len() as u64).to_be_bytes())
                .expect("drip header");
            ready_sender.send(()).expect("header ready");
            go_receiver
                .recv_timeout(Duration::from_secs(2))
                .expect("begin bounded transfer");
            // Each individual read progresses before a fresh 80 ms timeout.
            // The complete frame nevertheless exceeds the original bound.
            for chunk in payload.chunks(16) {
                if parent.write_all(chunk).is_err() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(15));
            }
        });
        ready_receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("peer header readiness");
        let deadline = Instant::now() + Duration::from_millis(80);
        go_sender.send(()).expect("start original deadline");
        let error = channel.receive_until(deadline).unwrap_err();
        assert!(error.contains("original transfer deadline"), "{error}");
        drop(channel);
        peer.join().expect("joined drip-frame peer");
    }

    #[test]
    fn acknowledgement_binds_exact_payload_kind_and_index() {
        for wrong_field in ["ack_kind", "action_index", "frame_sha256"] {
            let (mut channel, mut parent) = pair();
            let peer = std::thread::spawn(move || {
                let mut header = [0; 8];
                parent.read_exact(&mut header).expect("action header");
                let mut bytes = vec![0; u64::from_be_bytes(header) as usize];
                parent.read_exact(&mut bytes).expect("action bytes");
                let mut ack = json!({"protocol": PROTOCOL, "nonce": "a".repeat(64), "kind": "ack",
                    "ack_kind": "action", "action_index": 3, "frame_sha256": sha256(&bytes)});
                ack[wrong_field] = if wrong_field == "action_index" {
                    json!(2)
                } else {
                    json!("wrong")
                };
                frame(&mut parent, &ack);
            });
            assert!(
                channel
                    .durable("action", Some(3), json!({"original": "bytes\n"}))
                    .unwrap_err()
                    .contains("acknowledgement")
            );
            peer.join().expect("joined test peer");
        }
    }
}
