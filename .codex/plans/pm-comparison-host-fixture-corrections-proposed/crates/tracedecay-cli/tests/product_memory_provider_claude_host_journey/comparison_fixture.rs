//! Parent-driven extraction of the real CLI host journey.
//!
//! The socket carries the complete scheduled invocation and durable capture
//! acknowledgements. It supplies no provider or source authority. Raw command
//! output is retained before a decoded observation copy is inspected.

use super::*;
use sha2::{Digest, Sha256};
use std::io;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::panic::{AssertUnwindSafe, catch_unwind};

const PROTOCOL: &str = "tracedecay.host-comparison.v1";
// A valid 16 MiB host response may grow when carried as JSON. The durable
// Python event sink keeps its independent, unchanged 16 MiB limit.
const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
const CHANNEL_DEADLINE: Duration = Duration::from_secs(60);
const COMMAND_DEADLINE: Duration = Duration::from_secs(180);
const CLEANUP_DEADLINE: Duration = Duration::from_secs(15);
const PROCESS_POLL: Duration = Duration::from_millis(20);

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
    started_sessions: BTreeSet<String>,
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
            started_sessions: BTreeSet::new(),
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

    fn project_source(&mut self, source: &Value, index: usize) -> FixtureResult<usize> {
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
        let source_id = string(source, "id")?;
        let revision = string(source, "revision")?;
        let content = string(source, "content")?;
        let native_id = format!(
            "comparison-source-{}",
            &sha256(format!("{source_id}\0{revision}").as_bytes())[..32]
        );
        let path = self.journey.transcript_path_for_session(&session);
        let first = !path.exists();
        io_result(
            fs::create_dir_all(path.parent().ok_or("native transcript parent missing")?),
            "create native transcript directory",
        )?;
        let mut file = io_result(
            fs::OpenOptions::new().create(true).append(true).open(&path),
            "open owned native transcript",
        )?;
        // This stable native timestamp represents scheduled order only; it is
        // never reused as canonical revision or host receipt time.
        let timestamp = format!("2026-02-01T00:{:02}:{:02}.000Z", index / 60, index % 60);
        if index >= 3600 {
            return Err("native fixture timestamp projection exceeds its explicit bound".into());
        }
        if self.journey.codex && first {
            let metadata = json!({"timestamp": timestamp, "type": "session_meta",
                "payload": {"id": session, "cwd": self.journey.project}});
            io_result(
                writeln!(file, "{metadata}"),
                "write native Codex session metadata",
            )?;
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
        let slot = self.project_source(&action["source"], index)?;
        let projected = &self.projected[slot];
        let session = projected.session.clone();
        let path = projected.path.clone();
        let first = self.started_sessions.insert(session.clone());
        let mut hooks = Vec::new();
        if first {
            let command = if self.journey.codex {
                "hook-codex-session-start"
            } else {
                "hook-claude-session-start"
            };
            let raw = self.hook(command, &json!({"session_id": session, "transcript_path": path,
                "cwd": self.journey.project, "hook_event_name": "SessionStart", "source": "startup"}))?;
            let success = raw.success();
            hooks.push(raw.record);
            if !success {
                return Err("native SessionStart failed; exact input/output retained".into());
            }
        }
        if self.journey.codex || !first {
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
            let success = raw.success();
            hooks.push(raw.record);
            if !success {
                return Err("native Stop failed; exact input/output retained".into());
            }
        }
        let admitted = if self.lane.provider().is_some() {
            self.await_source(slot)?
        } else {
            // A disabled advisory lane must not contact a provider to fill a
            // measurement gap. The canonical-store reader remains separate.
            json!({"status": "unavailable", "reason": "canonical-store source read helper pending for control lane"})
        };
        let source = &self.projected[slot];
        Ok(json!({"hooks": hooks, "admitted": admitted,
            "source_projection": {"scheduled_source": source.source, "native_session_id": source.session,
                "path": source.path, "byte_start": source.start, "byte_end": source.end,
                "native_bytes_sha256": source.native_bytes_sha256},
            "provider_contacted": admitted.get("provider_contacted").cloned().unwrap_or_else(||
                if self.lane.provider().is_none() { json!(false) } else { Value::Null }),
            "terminal": admitted.get("terminal").cloned().unwrap_or(json!("native_hook_completed")),
            "successful_completion": admitted["successful_completion"] == true,
        }))
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

    fn recall(&mut self, action: &Value) -> FixtureResult<Value> {
        let task = string(&action["query"], "text")?;
        let session = format!(
            "comparison-request-{}",
            &sha256(string(&self.invocation["case"], "id")?.as_bytes())[..32]
        );
        let raw = self.tool("tracedecay_context", &json!({
            "task": task, "format": "json", "memory_limit": self.invocation["budgets"]["requested_candidates"],
            // Canonical explicit facts remain common to all four lanes. The
            // operator composition, not include_memory=false, gates providers.
            "include_memory": true, "_meta": {"session_id": session},
        }), self.invocation["budgets"]["provider_deadline_ms"].as_u64())?;
        if !raw.record["cutoff"].is_null() {
            return Err("context command exceeded capture bound; cutoff and complete raw artifacts retained".into());
        }
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
        let payload = tracedecay::daemon::tool_json_payload(&value, "tracedecay_context").ok();
        let advisory = payload
            .as_ref()
            .and_then(|payload| payload.get("advisory_provider_memory"))
            .cloned();
        if self.lane.provider().is_none() && advisory.is_some() {
            return Err(
                "control lane unexpectedly received an advisory provider lane; raw result retained"
                    .into(),
            );
        }
        Ok(
            json!({"raw_command": raw.record, "final_text_blocks": blocks_ref,
                "advisory_observation": advisory,
                "request_session_projection": session,
                "terminal": if value["isError"] == true { "tool_result_error" } else { "tool_result_completed" },
                "terminal_scope": "complete CLI ToolResult; provider terminal unavailable without retained host trace",
                "provider_contacted": if self.lane.provider().is_none() { json!(false) } else { Value::Null },
                "successful_completion": false,
                "delivery_status": {"status": "unavailable",
                    "reason": "exact renderer section/token/stage projector pending; complete stdout and block boundaries retained"},
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
        let mut result = json!({"input": action, "action": operation, "request_id": action["step"]["query_id"],
            "selected_provider": null, "phase": phase, "delivery": null,
            "process_group": self.processes.root.public(), "expectation": "indeterminate",
            "timing": {"phase": phase, "status": "measured", "clock": "fixture_process_monotonic_elapsed",
                "start_monotonic_ns": start, "end_monotonic_ns": end, "elapsed_ns": end.saturating_sub(start)},
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
                if started {
                    result["timing"]["elapsed_at_cutoff_ns"] = json!(end.saturating_sub(start));
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
        "advisory_tokens": 1024, "total_context_tokens": 128000,
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
