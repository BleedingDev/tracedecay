//! Bounded single-owner client for the supervised NCM worker process.

use crate::wire::{self, Reply, Request};
use std::collections::BTreeMap;
use std::fmt;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
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
        })
    }

    /// Sends one bounded call and waits through the hard-kill escalation budget.
    pub fn call(&self, mut request: Request, deadline: Duration) -> Result<Reply, ClientError> {
        let deadline_ms = u64::try_from(deadline.as_millis()).unwrap_or(u64::MAX);
        request.deadline_ms = deadline_ms;
        let frame_bytes = match wire::encode_request(&request) {
            Ok(frame) => frame.len(),
            Err(wire::FrameError::Oversized { .. }) => return Err(ClientError::RequestTooLarge),
            Err(error) => return Err(ClientError::Transport(error.to_string())),
        };
        self.reserve_bytes(frame_bytes)?;
        let (response_tx, response_rx) = mpsc::sync_channel(1);
        let expires = Instant::now()
            .checked_add(deadline)
            .unwrap_or_else(Instant::now);
        let command = OwnerCommand {
            request: request.clone(),
            expires,
            queued_bytes: frame_bytes,
            response: response_tx,
        };
        match self.calls.try_send(command) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.queued_bytes.fetch_sub(frame_bytes, Ordering::AcqRel);
                return Err(ClientError::Busy);
            }
            Err(TrySendError::Disconnected(_)) => {
                self.queued_bytes.fetch_sub(frame_bytes, Ordering::AcqRel);
                return Err(ClientError::OwnerStopped);
            }
        }
        let wait = deadline
            .saturating_add(KILL_ESCALATION)
            .saturating_add(Duration::from_millis(100));
        let result = match response_rx.recv_timeout(wait) {
            Ok(result) => result,
            Err(RecvTimeoutError::Timeout) => Err(ClientError::Cancelled),
            Err(RecvTimeoutError::Disconnected) => Err(ClientError::OwnerStopped),
        };
        self.update_unknown(&request, &result);
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
        request.payload.get("idempotency_key")?.as_str()
    } else {
        None
    }
}

struct OwnerCommand {
    request: Request,
    expires: Instant,
    queued_bytes: usize,
    response: SyncSender<Result<Reply, ClientError>>,
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
) {
    let mut process: Option<WorkerProcess> = None;
    let mut restart_failures = 0_u32;
    while !shutdown.load(Ordering::Acquire) {
        let command = match calls.recv_timeout(Duration::from_millis(10)) {
            Ok(command) => command,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        };
        queued_bytes.fetch_sub(command.queued_bytes, Ordering::AcqRel);
        if Instant::now() >= command.expires {
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
            Some(worker) => worker.execute(&command.request, command.expires, &shutdown),
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
        if abnormal {
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
    ) -> Result<Reply, ClientError> {
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
        match wait_channel(&ack_rx, expires, shutdown) {
            Ok(Ok(())) => {}
            Ok(Err(detail)) => return Err(ClientError::Transport(detail)),
            Err(WaitError::Deadline) => return Err(timeout_error(request)),
            Err(WaitError::Shutdown) => return Err(ClientError::Cancelled),
            Err(WaitError::Disconnected) => return Err(ClientError::WorkerExited),
        }
        let reply = match wait_channel(&self.replies, expires, shutdown) {
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
) -> Result<T, WaitError> {
    loop {
        if shutdown.load(Ordering::Acquire) {
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
