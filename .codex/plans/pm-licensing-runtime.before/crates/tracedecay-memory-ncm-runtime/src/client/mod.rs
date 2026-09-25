//! Bounded single-owner client for the supervised NCM worker process.

use crate::wire::{self, Operation, Reply, Request};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Maximum queued calls per client.
pub const MAX_QUEUED_REQUESTS: usize = 32;
/// Maximum aggregate encoded bytes in the client mailbox.
pub const MAX_QUEUED_BYTES: usize = 8 * 1024 * 1024;
/// Hard worker termination escalation budget after a deadline.
pub const KILL_ESCALATION: Duration = Duration::from_millis(250);
const MAX_SNAPSHOT_TRANSPORT_BYTES: usize = 256 * 1024 * 1024;
static NEXT_SNAPSHOT_FILE: AtomicU64 = AtomicU64::new(1);

/// Process launch and restart controls.
#[derive(Clone, Debug)]
pub struct WorkerOptions {
    /// Whether the provider is admitted to launch a worker.
    pub enabled: bool,
    /// Launch the named hash-encoder test double.
    pub test_double: bool,
    /// Permit startup without a locally loaded production encoder.
    pub no_encoder_required: bool,
    /// Consecutive spawn failures allowed before reporting restart exhaustion.
    pub max_restart_attempts: u32,
    /// Budget used by [`WorkerClient::reconcile_unknown`].
    pub reconciliation_deadline: Duration,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            test_double: false,
            no_encoder_required: false,
            max_restart_attempts: 3,
            reconciliation_deadline: Duration::from_secs(5),
        }
    }
}

/// Client-side process, framing, deadline, or admission failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientError {
    /// The provider is disabled and must not launch a process.
    Disabled,
    /// The bounded request-count or byte mailbox is full.
    Busy,
    /// The request expired before it could start or complete without a possible effect.
    Cancelled,
    /// A mutating call lost transport after it may have committed.
    EffectUnknown {
        /// Request identifier whose effect must be reconciled.
        op_id: u64,
    },
    /// Request JSON exceeded the 256 KiB wire bound.
    RequestTooLarge,
    /// Worker process creation failed.
    Spawn(String),
    /// Consecutive process creation failures exhausted the configured budget.
    RestartExhausted,
    /// Pipe I/O failed.
    Transport(String),
    /// Worker stdout was not a valid bounded reply stream.
    MalformedReply(String),
    /// The worker exited before acknowledging a request.
    WorkerExited,
    /// No killed request is retained under this idempotency key.
    UnknownIdempotencyKey,
    /// The client owner thread stopped unexpectedly.
    OwnerStopped,
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disabled => formatter.write_str("worker provider is disabled"),
            Self::Busy => formatter.write_str("worker mailbox is full"),
            Self::Cancelled => formatter.write_str("worker request cancelled"),
            Self::EffectUnknown { op_id } => {
                write!(formatter, "effect unknown for request {op_id}")
            }
            Self::RequestTooLarge => formatter.write_str("worker request exceeds 256 KiB"),
            Self::Spawn(detail) => write!(formatter, "spawn worker: {detail}"),
            Self::RestartExhausted => formatter.write_str("worker restart budget exhausted"),
            Self::Transport(detail) => write!(formatter, "worker transport: {detail}"),
            Self::MalformedReply(detail) => write!(formatter, "malformed worker reply: {detail}"),
            Self::WorkerExited => formatter.write_str("worker exited before reply"),
            Self::UnknownIdempotencyKey => formatter.write_str("unknown idempotency key"),
            Self::OwnerStopped => formatter.write_str("worker owner stopped"),
        }
    }
}

impl std::error::Error for ClientError {}

/// One supervised worker process and its bounded owner mailbox.
pub struct WorkerClient {
    calls: SyncSender<OwnerCommand>,
    queued_bytes: Arc<AtomicUsize>,
    unknown: Arc<Mutex<BTreeMap<String, Request>>>,
    shutdown: Arc<AtomicBool>,
    owner: Mutex<Option<JoinHandle<()>>>,
    pid: Arc<AtomicU32>,
    reconciliation_deadline: Duration,
    root: PathBuf,
    lifecycle: Arc<LifecycleState>,
}

#[derive(Default)]
struct LifecycleState {
    gate: Mutex<()>,
    stopped: AtomicBool,
    requested: AtomicU64,
    completed: AtomicU64,
    force: AtomicBool,
}

impl WorkerClient {
    /// Creates a client owner. The worker itself starts lazily on the first call.
    pub fn spawn(
        binary_path: impl AsRef<Path>,
        root: impl AsRef<Path>,
        options: WorkerOptions,
    ) -> Result<Self, ClientError> {
        if !options.enabled {
            return Err(ClientError::Disabled);
        }
        if !root.as_ref().is_absolute() {
            return Err(ClientError::Spawn("state root must be absolute".to_owned()));
        }
        let (calls, receiver) = mpsc::sync_channel(MAX_QUEUED_REQUESTS);
        let queued_bytes = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let pid = Arc::new(AtomicU32::new(0));
        let owner_shutdown = Arc::clone(&shutdown);
        let owner_pid = Arc::clone(&pid);
        let owner_queued_bytes = Arc::clone(&queued_bytes);
        let lifecycle = Arc::new(LifecycleState::default());
        let owner_lifecycle = Arc::clone(&lifecycle);
        let launch = Launch {
            binary: binary_path.as_ref().to_path_buf(),
            root: root.as_ref().to_path_buf(),
            test_double: options.test_double,
            no_encoder_required: options.no_encoder_required,
            max_restart_attempts: options.max_restart_attempts,
        };
        let owner = thread::Builder::new()
            .name("ncm-worker-owner".to_owned())
            .spawn(move || {
                owner_loop(
                    receiver,
                    owner_shutdown,
                    owner_pid,
                    owner_queued_bytes,
                    launch,
                    owner_lifecycle,
                )
            })
            .map_err(|error| ClientError::Spawn(error.to_string()))?;
        Ok(Self {
            calls,
            queued_bytes,
            unknown: Arc::new(Mutex::new(BTreeMap::new())),
            shutdown,
            owner: Mutex::new(Some(owner)),
            pid,
            reconciliation_deadline: options.reconciliation_deadline,
            root: root.as_ref().to_path_buf(),
            lifecycle,
        })
    }

    /// Sends one bounded call and waits through the hard-kill escalation budget.
    pub fn call(&self, request: Request, deadline: Duration) -> Result<Reply, ClientError> {
        self.call_cancellable(request, deadline, Arc::new(|| false))
    }

    /// Sends one bounded request with a caller-owned cancellation probe.
    /// Cancellation retires only this request, using the existing kill/reap
    /// path if it reached the child. The shared owner remains available.
    pub fn call_cancellable(
        &self,
        mut request: Request,
        deadline: Duration,
        cancelled: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Result<Reply, ClientError> {
        if cancelled() || deadline.is_zero() {
            return Err(ClientError::Cancelled);
        }
        if self.lifecycle.stopped.load(Ordering::Acquire) {
            return Err(ClientError::Disabled);
        }
        let lifecycle = Arc::clone(&self.lifecycle);
        let epoch = lifecycle.requested.load(Ordering::Acquire);
        let cancelled: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(move || {
            cancelled()
                || lifecycle.stopped.load(Ordering::Acquire)
                || lifecycle.requested.load(Ordering::Acquire) != epoch
        });
        let deadline_ms = u64::try_from(deadline.as_millis()).unwrap_or(u64::MAX);
        request.deadline_ms = deadline_ms;
        let logical_request = request.clone();
        let mut restore_file = None;
        let frame_bytes = match wire::encode_request(&request) {
            Ok(frame) => frame.len(),
            Err(wire::FrameError::Oversized { .. }) if request.op == Operation::SnapshotRestore => {
                let path = self.externalize_snapshot_restore(&mut request)?;
                restore_file = Some(path);
                match wire::encode_request(&request) {
                    Ok(frame) => frame.len(),
                    Err(wire::FrameError::Oversized { .. }) => {
                        remove_transport_file(restore_file.as_deref());
                        return Err(ClientError::RequestTooLarge);
                    }
                    Err(error) => {
                        remove_transport_file(restore_file.as_deref());
                        return Err(ClientError::Transport(error.to_string()));
                    }
                }
            }
            Err(wire::FrameError::Oversized { .. }) => return Err(ClientError::RequestTooLarge),
            Err(error) => return Err(ClientError::Transport(error.to_string())),
        };
        if let Err(error) = self.reserve_bytes(frame_bytes) {
            remove_transport_file(restore_file.as_deref());
            return Err(error);
        }
        let (response_tx, response_rx) = mpsc::sync_channel(1);
        let expires = Instant::now()
            .checked_add(deadline)
            .unwrap_or_else(Instant::now);
        let command = OwnerCommand {
            request: request.clone(),
            expires,
            queued_bytes: frame_bytes,
            response: response_tx,
            cancelled: Arc::clone(&cancelled),
        };
        match self.calls.try_send(command) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.queued_bytes.fetch_sub(frame_bytes, Ordering::AcqRel);
                remove_transport_file(restore_file.as_deref());
                return Err(ClientError::Busy);
            }
            Err(TrySendError::Disconnected(_)) => {
                self.queued_bytes.fetch_sub(frame_bytes, Ordering::AcqRel);
                remove_transport_file(restore_file.as_deref());
                return Err(ClientError::OwnerStopped);
            }
        }
        let wait = deadline
            .saturating_add(KILL_ESCALATION)
            .saturating_add(Duration::from_millis(100));
        let mut wait_until = Instant::now()
            .checked_add(wait)
            .unwrap_or_else(Instant::now);
        let mut cancellation_seen = false;
        let mut result = loop {
            if !cancellation_seen && cancelled() {
                cancellation_seen = true;
                wait_until =
                    wait_until.min(Instant::now() + KILL_ESCALATION + Duration::from_millis(100));
            }
            let Some(remaining) = wait_until.checked_duration_since(Instant::now()) else {
                break Err(if cancellation_seen {
                    indeterminate_or(&request, ClientError::Cancelled)
                } else {
                    ClientError::Cancelled
                });
            };
            match response_rx.recv_timeout(remaining.min(Duration::from_millis(10))) {
                Ok(result) => {
                    break if (cancellation_seen || cancelled()) && result.is_ok() {
                        Err(indeterminate_or(&request, ClientError::Cancelled))
                    } else {
                        result
                    };
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break Err(ClientError::OwnerStopped),
            }
        };
        if let Ok(reply) = result {
            result = self.hydrate_snapshot_export(&request, reply);
        }
        self.update_unknown(&logical_request, &result);
        remove_transport_file(restore_file.as_deref());
        result
    }

    /// Replays the retained mutating request for an indeterminate idempotency key.
    pub fn reconcile_unknown(&self, idempotency_key: &str) -> Result<Reply, ClientError> {
        let request = self
            .unknown
            .lock()
            .map_err(|_| ClientError::OwnerStopped)?
            .get(idempotency_key)
            .cloned()
            .ok_or(ClientError::UnknownIdempotencyKey)?;
        self.call(request, self.reconciliation_deadline)
    }

    /// Current worker process identifier, or `None` while stopped/lazy.
    #[must_use]
    pub fn pid(&self) -> Option<u32> {
        match self.pid.load(Ordering::Acquire) {
            0 => None,
            pid => Some(pid),
        }
    }

    /// Starts or resumes this owner's actual worker and proves its handshake
    /// before the supplied deadline. No additional supervisor is created.
    pub fn start(&self, deadline: Instant) -> Result<(), ClientError> {
        let _gate = self.lifecycle_gate(deadline)?;
        if self.lifecycle.completed.load(Ordering::Acquire)
            < self.lifecycle.requested.load(Ordering::Acquire)
        {
            return Err(ClientError::Busy);
        }
        self.lifecycle.stopped.store(false, Ordering::Release);
        let remaining = remaining(deadline).ok_or(ClientError::Cancelled)?;
        let request = Request::new(
            0,
            remaining.as_millis().try_into().unwrap_or(u64::MAX),
            Operation::Handshake,
            "0000000000000000000000000000000000000000000000000000000000000000",
            json!({"algorithm_profile": "ncm-biomem-rs.v1"}),
        );
        let reply = self.call(request, remaining)?;
        if reply.outcome == crate::engine::Outcome::Success && self.pid().is_some() {
            Ok(())
        } else {
            Err(ClientError::Transport(
                "worker did not establish readiness".to_owned(),
            ))
        }
    }

    /// Requests graceful termination and waits for the owner to confirm child
    /// reap and pipe-thread completion. False leaves termination unconfirmed.
    pub fn request_stop(&self, deadline: Instant) -> Result<bool, ClientError> {
        self.stop_owned_worker(deadline, false)
    }

    /// Forces termination through this existing owner. Success proves the child
    /// was reaped; callers must not treat a deadline error as confirmed erasure.
    pub fn kill(&self, deadline: Instant) -> Result<(), ClientError> {
        if self.stop_owned_worker(deadline, true)? {
            Ok(())
        } else {
            Err(ClientError::Cancelled)
        }
    }

    fn lifecycle_gate(
        &self,
        deadline: Instant,
    ) -> Result<std::sync::MutexGuard<'_, ()>, ClientError> {
        loop {
            if Instant::now() >= deadline {
                return Err(ClientError::Cancelled);
            }
            match self.lifecycle.gate.try_lock() {
                Ok(gate) => return Ok(gate),
                Err(std::sync::TryLockError::Poisoned(_)) => return Err(ClientError::OwnerStopped),
                Err(std::sync::TryLockError::WouldBlock) => thread::sleep(Duration::from_millis(2)),
            }
        }
    }

    fn stop_owned_worker(&self, deadline: Instant, force: bool) -> Result<bool, ClientError> {
        let _gate = self.lifecycle_gate(deadline)?;
        self.lifecycle.stopped.store(true, Ordering::Release);
        self.lifecycle.force.store(force, Ordering::Release);
        let requested = self
            .lifecycle
            .requested
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        loop {
            if self.lifecycle.completed.load(Ordering::Acquire) >= requested {
                return Ok(true);
            }
            if Instant::now() >= deadline {
                return Ok(false);
            }
            if self.shutdown.load(Ordering::Acquire) {
                return Err(ClientError::OwnerStopped);
            }
            thread::sleep(Duration::from_millis(2));
        }
    }

    fn externalize_snapshot_restore(&self, request: &mut Request) -> Result<PathBuf, ClientError> {
        let common = request
            .payload
            .pointer("/common_portability/action")
            .is_some_and(|value| value == "snapshot_restore");
        let snapshot = if common {
            request.payload.pointer("/common_portability/bytes")
        } else {
            request.payload.get("snapshot")
        }
        .cloned()
        .ok_or(ClientError::RequestTooLarge)?;
        let bytes: Vec<u8> = serde_json::from_value(snapshot)
            .map_err(|error| ClientError::Transport(format!("snapshot payload: {error}")))?;
        if bytes.is_empty() || bytes.len() > MAX_SNAPSHOT_TRANSPORT_BYTES {
            return Err(ClientError::RequestTooLarge);
        }
        let directory =
            snapshot_directory(&self.root, &request.namespace).map_err(ClientError::Transport)?;
        fs::create_dir_all(&directory).map_err(|error| {
            ClientError::Transport(format!("create snapshot directory: {error}"))
        })?;
        let serial = NEXT_SNAPSHOT_FILE.fetch_add(1, Ordering::Relaxed);
        let file_name = format!(
            "restore-{}-{}-{serial}.json",
            std::process::id(),
            request.id
        );
        let snapshot_file = directory.join(file_name);
        let temporary_file = directory.join(format!(
            ".restore-{}-{}-{serial}.tmp",
            std::process::id(),
            request.id
        ));
        atomic_write(&temporary_file, &snapshot_file, &bytes).map_err(ClientError::Transport)?;
        let byte_length = u64::try_from(bytes.len()).map_err(|_| ClientError::RequestTooLarge)?;
        let content_sha256 = sha256_hex(&bytes);
        let object = (if common {
            request
                .payload
                .get_mut("common_portability")
                .and_then(Value::as_object_mut)
        } else {
            request.payload.as_object_mut()
        })
        .ok_or_else(|| {
            ClientError::Transport("snapshot restore payload must be an object".to_owned())
        })?;
        object.remove(if common { "bytes" } else { "snapshot" });
        object.insert(
            "snapshot_file".to_owned(),
            Value::String(snapshot_file.to_string_lossy().into_owned()),
        );
        object.insert("byte_length".to_owned(), Value::from(byte_length));
        object.insert("content_sha256".to_owned(), Value::String(content_sha256));
        Ok(snapshot_file)
    }

    fn hydrate_snapshot_export(
        &self,
        request: &Request,
        mut reply: Reply,
    ) -> Result<Reply, ClientError> {
        if request.op != Operation::SnapshotExport
            || !matches!(reply.outcome, crate::engine::Outcome::Success)
        {
            return Ok(reply);
        }
        let payload = reply.payload.as_ref().ok_or_else(|| {
            ClientError::MalformedReply("snapshot export omitted file metadata".to_owned())
        })?;
        let snapshot_file = payload
            .get("snapshot_file")
            .and_then(Value::as_str)
            .map(PathBuf::from)
            .ok_or_else(|| {
                ClientError::MalformedReply("snapshot export omitted snapshot_file".to_owned())
            })?;
        let byte_length = payload
            .get("byte_length")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                ClientError::MalformedReply("snapshot export omitted byte_length".to_owned())
            })?;
        let content_sha256 = payload
            .get("content_sha256")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ClientError::MalformedReply("snapshot export omitted content_sha256".to_owned())
            })?;
        let payload_generation = payload
            .get("state_generation")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                ClientError::MalformedReply("snapshot export omitted state_generation".to_owned())
            })?;
        if payload_generation != reply.state_generation {
            return Err(ClientError::MalformedReply(
                "snapshot export generation mismatch".to_owned(),
            ));
        }
        let bytes = read_verified_snapshot_file(
            &self.root,
            &request.namespace,
            &snapshot_file,
            byte_length,
            content_sha256,
        )?;
        let common = request
            .payload
            .pointer("/common_portability/action")
            .is_some_and(|value| value == "snapshot_export");
        if common && payload["common_portability"] != "snapshot_export" {
            return Err(ClientError::MalformedReply(
                "common snapshot marker differs".to_owned(),
            ));
        }
        reply.payload = Some(json!({
            "format": "ncm-snapshot.v1",
            "bytes": bytes,
            "byte_length": byte_length,
            "content_sha256": content_sha256,
            "state_generation": reply.state_generation
        }));
        if common {
            reply.payload = Some(
                json!({"common_portability": "snapshot_export", "bytes": reply.payload.as_ref().and_then(|payload| payload.get("bytes")), "warnings": []}),
            );
        }
        Ok(reply)
    }

    fn reserve_bytes(&self, bytes: usize) -> Result<(), ClientError> {
        self.queued_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(bytes)
                    .filter(|next| *next <= MAX_QUEUED_BYTES)
            })
            .map(|_| ())
            .map_err(|_| ClientError::Busy)
    }

    fn update_unknown(&self, request: &Request, result: &Result<Reply, ClientError>) {
        let Some(key) = idempotency_key(request) else {
            return;
        };
        let Ok(mut unknown) = self.unknown.lock() else {
            return;
        };
        if matches!(result, Err(ClientError::EffectUnknown { .. })) {
            unknown.insert(key.to_owned(), request.clone());
        } else if result.is_ok() {
            unknown.remove(key);
        }
    }
}

fn snapshot_directory(root: &Path, namespace: &str) -> Result<PathBuf, String> {
    if namespace.len() != 64
        || !namespace
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err("snapshot namespace must be lowercase sha256 hex".to_owned());
    }
    Ok(root.join("namespaces").join(namespace).join("snapshots"))
}

fn atomic_write(temporary_file: &Path, destination: &Path, bytes: &[u8]) -> Result<(), String> {
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(temporary_file)
            .map_err(|error| format!("create snapshot transport file: {error}"))?;
        file.write_all(bytes)
            .map_err(|error| format!("write snapshot transport file: {error}"))?;
        file.sync_all()
            .map_err(|error| format!("sync snapshot transport file: {error}"))?;
        fs::rename(temporary_file, destination)
            .map_err(|error| format!("publish snapshot transport file: {error}"))
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary_file);
    }
    result
}

fn read_verified_snapshot_file(
    root: &Path,
    namespace: &str,
    snapshot_file: &Path,
    byte_length: u64,
    content_sha256: &str,
) -> Result<Vec<u8>, ClientError> {
    if !snapshot_file.is_absolute() || !is_sha256_hex(content_sha256) {
        return Err(ClientError::MalformedReply(
            "invalid snapshot export file metadata".to_owned(),
        ));
    }
    let expected_length = usize::try_from(byte_length)
        .map_err(|_| ClientError::MalformedReply("snapshot export length overflow".to_owned()))?;
    if expected_length == 0 || expected_length > MAX_SNAPSHOT_TRANSPORT_BYTES {
        return Err(ClientError::MalformedReply(
            "snapshot export length is outside the snapshot budget".to_owned(),
        ));
    }
    let directory = snapshot_directory(root, namespace).map_err(ClientError::MalformedReply)?;
    let canonical_directory = fs::canonicalize(&directory).map_err(|error| {
        ClientError::MalformedReply(format!("open snapshot export directory: {error}"))
    })?;
    let canonical_file = fs::canonicalize(snapshot_file).map_err(|error| {
        ClientError::MalformedReply(format!("open snapshot export file: {error}"))
    })?;
    if !canonical_file.starts_with(&canonical_directory)
        || canonical_file.parent() != Some(canonical_directory.as_path())
    {
        return Err(ClientError::MalformedReply(
            "snapshot export file escapes namespace directory".to_owned(),
        ));
    }
    let _guard = SnapshotFileGuard {
        path: canonical_file.clone(),
    };
    let file = File::open(&canonical_file).map_err(|error| {
        ClientError::MalformedReply(format!("open snapshot export file: {error}"))
    })?;
    let actual_length = file
        .metadata()
        .map_err(|error| {
            ClientError::MalformedReply(format!("inspect snapshot export file: {error}"))
        })?
        .len();
    if actual_length != byte_length {
        return Err(ClientError::MalformedReply(
            "snapshot export byte length mismatch".to_owned(),
        ));
    }
    let read_limit = byte_length.checked_add(1).ok_or_else(|| {
        ClientError::MalformedReply("snapshot export read limit overflow".to_owned())
    })?;
    let mut bytes = Vec::with_capacity(expected_length);
    file.take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            ClientError::MalformedReply(format!("read snapshot export file: {error}"))
        })?;
    if bytes.len() != expected_length || sha256_hex(&bytes) != content_sha256 {
        return Err(ClientError::MalformedReply(
            "snapshot export content verification failed".to_owned(),
        ));
    }
    Ok(bytes)
}

fn remove_transport_file(path: Option<&Path>) {
    if let Some(path) = path {
        let _ = fs::remove_file(path);
    }
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

struct SnapshotFileGuard {
    path: PathBuf,
}

impl Drop for SnapshotFileGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

impl Drop for WorkerClient {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Ok(owner) = self.owner.get_mut()
            && let Some(owner) = owner.take()
        {
            let _ = owner.join();
        }
    }
}

fn idempotency_key(request: &Request) -> Option<&str> {
    if request.op.is_mutating() {
        request
            .payload
            .get("idempotency_key")
            .or_else(|| {
                matches!(request.op, Operation::SnapshotRestore | Operation::Replay)
                    .then(|| {
                        request
                            .payload
                            .pointer("/common_portability/idempotency_key")
                    })
                    .flatten()
            })?
            .as_str()
    } else {
        None
    }
}

struct OwnerCommand {
    request: Request,
    expires: Instant,
    queued_bytes: usize,
    response: SyncSender<Result<Reply, ClientError>>,
    cancelled: Arc<dyn Fn() -> bool + Send + Sync>,
}

#[derive(Clone)]
struct Launch {
    binary: PathBuf,
    root: PathBuf,
    test_double: bool,
    no_encoder_required: bool,
    max_restart_attempts: u32,
}

fn owner_loop(
    calls: Receiver<OwnerCommand>,
    shutdown: Arc<AtomicBool>,
    pid: Arc<AtomicU32>,
    queued_bytes: Arc<AtomicUsize>,
    launch: Launch,
    lifecycle: Arc<LifecycleState>,
) {
    let mut process: Option<WorkerProcess> = None;
    let mut restart_failures = 0_u32;
    while !shutdown.load(Ordering::Acquire) {
        let requested = lifecycle.requested.load(Ordering::Acquire);
        if requested > lifecycle.completed.load(Ordering::Acquire) {
            if let Some(worker) = process.take() {
                worker.terminate(!lifecycle.force.load(Ordering::Acquire));
            }
            restart_failures = 0;
            lifecycle.completed.store(requested, Ordering::Release);
        }
        let command = match calls.recv_timeout(Duration::from_millis(10)) {
            Ok(command) => command,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        };
        queued_bytes.fetch_sub(command.queued_bytes, Ordering::AcqRel);
        if (command.cancelled)() || Instant::now() >= command.expires {
            let _ = command.response.send(Err(ClientError::Cancelled));
            continue;
        }
        if process.is_none() {
            if restart_failures > launch.max_restart_attempts {
                let _ = command.response.send(Err(ClientError::RestartExhausted));
                continue;
            }
            match WorkerProcess::spawn(&launch, Arc::clone(&pid)) {
                Ok(worker) => process = Some(worker),
                Err(error) => {
                    restart_failures = restart_failures.saturating_add(1);
                    let reported = if restart_failures > launch.max_restart_attempts {
                        ClientError::RestartExhausted
                    } else {
                        error
                    };
                    let _ = command.response.send(Err(reported));
                    continue;
                }
            }
        }
        let result = match process.as_mut() {
            Some(worker) => worker.execute(
                &command.request,
                command.expires,
                &shutdown,
                command.cancelled.as_ref(),
            ),
            None => Err(ClientError::OwnerStopped),
        };
        let abnormal = matches!(
            result,
            Err(ClientError::Cancelled)
                | Err(ClientError::EffectUnknown { .. })
                | Err(ClientError::Transport(_))
                | Err(ClientError::MalformedReply(_))
                | Err(ClientError::WorkerExited)
        );
        if abnormal && let Some(worker) = process.take() {
            worker.terminate(false);
        }
        if abnormal && !(command.cancelled)() {
            restart_failures = restart_failures.saturating_add(1);
        } else if result.is_ok() {
            restart_failures = 0;
        }
        let _ = command.response.send(result);
    }
    if let Some(worker) = process {
        worker.terminate(true);
    }
    pid.store(0, Ordering::Release);
}

enum WriterCommand {
    Frame(Vec<u8>, SyncSender<Result<(), String>>),
    Shutdown,
}

struct WorkerProcess {
    child: Child,
    writer: SyncSender<WriterCommand>,
    replies: Receiver<Result<Reply, String>>,
    writer_thread: JoinHandle<()>,
    reader_thread: JoinHandle<()>,
    pid: Arc<AtomicU32>,
}

impl WorkerProcess {
    fn spawn(launch: &Launch, pid: Arc<AtomicU32>) -> Result<Self, ClientError> {
        let mut command = ProcessCommand::new(&launch.binary);
        command
            .arg("--state-root")
            .arg(&launch.root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        if launch.test_double {
            command.arg("--test-double");
        }
        if launch.no_encoder_required {
            command.arg("--no-encoder-required");
        }
        let mut child = command
            .spawn()
            .map_err(|error| ClientError::Spawn(error.to_string()))?;
        let stdin = match child.stdin.take() {
            Some(stdin) => stdin,
            None => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(ClientError::Spawn("worker stdin was not piped".to_owned()));
            }
        };
        let stdout = match child.stdout.take() {
            Some(stdout) => stdout,
            None => {
                drop(stdin);
                let _ = child.kill();
                let _ = child.wait();
                return Err(ClientError::Spawn("worker stdout was not piped".to_owned()));
            }
        };

        let (writer, writer_rx) = mpsc::sync_channel::<WriterCommand>(1);
        let writer_thread = match thread::Builder::new()
            .name("ncm-worker-writer".to_owned())
            .spawn(move || writer_loop(stdin, writer_rx))
        {
            Ok(handle) => handle,
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(ClientError::Spawn(error.to_string()));
            }
        };
        let (reply_tx, replies) = mpsc::sync_channel(1);
        let reader_thread = match thread::Builder::new()
            .name("ncm-worker-reader".to_owned())
            .spawn(move || reader_loop(stdout, reply_tx))
        {
            Ok(handle) => handle,
            Err(error) => {
                drop(writer);
                let _ = child.kill();
                let _ = child.wait();
                let _ = writer_thread.join();
                return Err(ClientError::Spawn(error.to_string()));
            }
        };
        pid.store(child.id(), Ordering::Release);
        Ok(Self {
            child,
            writer,
            replies,
            writer_thread,
            reader_thread,
            pid,
        })
    }

    fn execute(
        &mut self,
        request: &Request,
        expires: Instant,
        shutdown: &AtomicBool,
        cancelled: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<Reply, ClientError> {
        if cancelled() {
            return Err(ClientError::Cancelled);
        }
        let remaining = remaining(expires).ok_or_else(|| timeout_error(request))?;
        let mut outbound = request.clone();
        outbound.deadline_ms = u64::try_from(remaining.as_millis())
            .unwrap_or(u64::MAX)
            .max(1);
        let frame = wire::encode_request(&outbound).map_err(|error| match error {
            wire::FrameError::Oversized { .. } => ClientError::RequestTooLarge,
            other => ClientError::Transport(other.to_string()),
        })?;
        let (ack_tx, ack_rx) = mpsc::sync_channel(1);
        self.writer
            .send(WriterCommand::Frame(frame, ack_tx))
            .map_err(|_| ClientError::WorkerExited)?;
        match wait_channel(&ack_rx, expires, shutdown, cancelled) {
            Ok(Ok(())) => {}
            Ok(Err(detail)) => return Err(ClientError::Transport(detail)),
            Err(WaitError::Deadline) => return Err(timeout_error(request)),
            // The writer may have sent the frame before cancellation was
            // observed. A mutating operation is indeterminate until reconciled.
            Err(WaitError::Shutdown) => {
                return Err(indeterminate_or(request, ClientError::Cancelled));
            }
            Err(WaitError::Disconnected) => return Err(ClientError::WorkerExited),
        }
        let reply = match wait_channel(&self.replies, expires, shutdown, cancelled) {
            Ok(Ok(reply)) => reply,
            Ok(Err(detail)) => {
                return Err(indeterminate_or(
                    request,
                    ClientError::MalformedReply(detail),
                ));
            }
            Err(WaitError::Deadline) => return Err(timeout_error(request)),
            Err(WaitError::Shutdown) => {
                return Err(indeterminate_or(request, ClientError::Cancelled));
            }
            Err(WaitError::Disconnected) => {
                return Err(indeterminate_or(request, ClientError::WorkerExited));
            }
        };
        if reply.id != request.id {
            return Err(indeterminate_or(
                request,
                ClientError::MalformedReply(format!(
                    "reply id {} does not match request {}",
                    reply.id, request.id
                )),
            ));
        }
        Ok(reply)
    }

    fn terminate(mut self, graceful: bool) {
        if graceful {
            let _ = self.writer.try_send(WriterCommand::Shutdown);
            if wait_for_exit(&mut self.child, KILL_ESCALATION) {
                self.finish_threads();
                return;
            }
        }
        let _ = self.child.kill();
        let _ = wait_for_exit(&mut self.child, KILL_ESCALATION);
        let _ = self.child.wait();
        self.finish_threads();
    }

    fn finish_threads(self) {
        self.pid.store(0, Ordering::Release);
        drop(self.writer);
        let _ = self.writer_thread.join();
        let _ = self.reader_thread.join();
    }
}

fn writer_loop(mut stdin: std::process::ChildStdin, commands: Receiver<WriterCommand>) {
    while let Ok(command) = commands.recv() {
        match command {
            WriterCommand::Frame(frame, acknowledgement) => {
                let result = stdin
                    .write_all(&frame)
                    .and_then(|()| stdin.flush())
                    .map_err(|error| error.to_string());
                let failed = result.is_err();
                let _ = acknowledgement.send(result);
                if failed {
                    break;
                }
            }
            WriterCommand::Shutdown => break,
        }
    }
}

fn reader_loop(mut stdout: std::process::ChildStdout, replies: SyncSender<Result<Reply, String>>) {
    loop {
        match wire::read_reply(&mut stdout) {
            Ok(Some(reply)) => {
                if replies.send(Ok(reply)).is_err() {
                    break;
                }
            }
            Ok(None) => {
                let _ = replies.send(Err("worker stdout reached EOF".to_owned()));
                break;
            }
            Err(error) => {
                let _ = replies.send(Err(error.to_string()));
                break;
            }
        }
    }
}

fn remaining(expires: Instant) -> Option<Duration> {
    expires.checked_duration_since(Instant::now())
}

fn timeout_error(request: &Request) -> ClientError {
    if request.op.is_mutating() {
        ClientError::EffectUnknown { op_id: request.id }
    } else {
        ClientError::Cancelled
    }
}

fn indeterminate_or(request: &Request, otherwise: ClientError) -> ClientError {
    if request.op.is_mutating() {
        ClientError::EffectUnknown { op_id: request.id }
    } else {
        otherwise
    }
}

enum WaitError {
    Deadline,
    Shutdown,
    Disconnected,
}

fn wait_channel<T>(
    receiver: &Receiver<T>,
    expires: Instant,
    shutdown: &AtomicBool,
    cancelled: &(dyn Fn() -> bool + Send + Sync),
) -> Result<T, WaitError> {
    loop {
        if shutdown.load(Ordering::Acquire) || cancelled() {
            return Err(WaitError::Shutdown);
        }
        let remaining = remaining(expires).ok_or(WaitError::Deadline)?;
        let slice = remaining.min(Duration::from_millis(10));
        match receiver.recv_timeout(slice) {
            Ok(value) => return Ok(value),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return Err(WaitError::Disconnected),
        }
    }
}

fn wait_for_exit(child: &mut Child, budget: Duration) -> bool {
    let deadline = Instant::now()
        .checked_add(budget)
        .unwrap_or_else(Instant::now);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) => {}
            Err(_) => return false,
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(5));
    }
}
