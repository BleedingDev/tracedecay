//! The unchanged common advisory suite over the production NCM worker and model.
#![cfg(all(feature = "rust-backend", unix))]
#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::fd::OwnedFd;
use std::os::unix::fs::{MetadataExt, symlink};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tracedecay_memory_conformance::compatibility::{
    CommonAdvisoryFixture, CommonAdvisoryFixtureFactory, CompatibilityScenario,
    CompatibilityVerdict, FixtureEnvironmentAction, FixtureEnvironmentEvidence, FixtureUnavailable,
    LegacyDurableRecordEvidence, LegacyIdentityEvidence, LegacyRecordEvidence,
    run_common_advisory_suite,
};
use tracedecay_memory_conformance::real_fixture::{FixtureAuthority, FixtureAuthoritySnapshot};
use tracedecay_memory_provider_api::contract::{
    CommittedEffectState, SourceDisposition, TerminalCode,
};
use tracedecay_memory_provider_api::{
    CancellationToken, CanonicalPayload, HandshakeRequest, HandshakeRequestParts,
    HandshakeResponse, MemoryProvider, OperationControl, OwnedExactScope, OwnedVersionedId,
    PayloadSanitizationReceipt, PayloadSanitizationReceiptParts, ProviderCall, ProviderCallParts,
    ProviderDescriptor, ProviderOperation, ProviderReply, TerminalRecord,
    observation_extensions_digest,
};
use tracedecay_memory_provider_ncm::{
    NcmCognitiveSurface, NcmNamespace, NcmProviderAdapter, NcmSurfaceCall,
    NcmSurfaceHandshakeRequest, NcmSurfaceHandshakeResponse, RustNcmConfig, RustNcmSurface,
    RustNcmWorkerOwner, StateRoot, WorkerOptions,
};

const ACTIVE: u8 = 1;
const OBSERVE: u8 = 2;
const DISABLED: u8 = 3;
const QUARANTINED: u8 = 4;
const REGISTRATION_REVISION: u64 = 1;
const LIFECYCLE_BUDGET: Duration = Duration::from_secs(30);
const HELPER_OUTPUT_LIMIT: usize = 8 * 1024 * 1024;
const REAP_RESERVE: Duration = Duration::from_millis(100);
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Same post-commit cancellation seam as the direct Rust backend regression.
/// The actual worker response must prove a durable mutation before this hook
/// cancels the original call. The adapter produces the unknown-effect terminal.
struct WithholdCommittedReply {
    inner: Arc<RustNcmSurface>,
    armed: Arc<AtomicBool>,
    worker: Arc<TrackedWorker>,
    cleanup: Arc<CleanupCollector>,
}

impl NcmCognitiveSurface for WithholdCommittedReply {
    fn descriptor(&self) -> ProviderDescriptor {
        self.inner.descriptor()
    }

    fn handshake(&self, request: &NcmSurfaceHandshakeRequest) -> NcmSurfaceHandshakeResponse {
        let deadline = inherited_deadline(&request.control);
        let reply = self.inner.handshake(request);
        self.worker.observe_process(deadline, &self.cleanup);
        reply
    }

    fn invoke(&self, call: &NcmSurfaceCall) -> ProviderReply {
        let deadline = inherited_deadline(&call.control);
        let reply = self.inner.invoke(call);
        self.worker.observe_process(deadline, &self.cleanup);
        if call.operation.mutates_provider_state() && self.armed.swap(false, Ordering::SeqCst) {
            assert_eq!(reply.terminal.terminal_code(), TerminalCode::Success);
            assert_eq!(
                reply.terminal.committed_effect().state(),
                CommittedEffectState::Committed,
                "the transport hook requires an actual durable commit"
            );
            assert!(
                reply
                    .terminal
                    .committed_effect()
                    .provider_receipt_sha256()
                    .is_some()
            );
            call.control.cancellation().cancel();
        }
        reply
    }
}

/// Observes the existing legacy projection and real replies without rewriting
/// either. The captured opaque key is checked against the durable journal.
struct LegacyWriterSurface {
    inner: Arc<RustNcmSurface>,
    dispatched: Arc<Mutex<Vec<(NcmSurfaceCall, ProviderReply)>>>,
    worker: Arc<TrackedWorker>,
    cleanup: Arc<CleanupCollector>,
}

impl NcmCognitiveSurface for LegacyWriterSurface {
    fn descriptor(&self) -> ProviderDescriptor {
        self.inner.descriptor()
    }

    fn handshake(&self, request: &NcmSurfaceHandshakeRequest) -> NcmSurfaceHandshakeResponse {
        let deadline = inherited_deadline(&request.control);
        let reply = self.inner.handshake(request);
        self.worker.observe_process(deadline, &self.cleanup);
        reply
    }

    fn invoke(&self, call: &NcmSurfaceCall) -> ProviderReply {
        let deadline = inherited_deadline(&call.control);
        let reply = self.inner.invoke(call);
        self.worker.observe_process(deadline, &self.cleanup);
        if call.operation == ProviderOperation::Observe {
            self.dispatched
                .lock()
                .expect("legacy capture lock")
                .push((call.clone(), reply.clone()));
        }
        reply
    }
}

/// Fixture host admission. Admitted calls and all their bytes go unchanged to
/// the real adapter; mode evidence is read from this same dispatch gate.
struct FixtureHost {
    adapter: NcmProviderAdapter,
    mode: Arc<AtomicU8>,
}

impl FixtureHost {
    fn admits(&self, operation: ProviderOperation) -> bool {
        match self.mode.load(Ordering::SeqCst) {
            ACTIVE => true,
            OBSERVE => operation != ProviderOperation::Recall,
            DISABLED | QUARANTINED => false,
            _ => false,
        }
    }
}

impl MemoryProvider for FixtureHost {
    fn descriptor(&self) -> ProviderDescriptor {
        self.adapter.descriptor()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        self.adapter.handshake(request)
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        let refusal = if !self.admits(call.operation) {
            Some((
                TerminalCode::ProviderUnavailable,
                "fixture.host.mode_withholds_dispatch",
            ))
        } else {
            None
        };
        if let Some((code, diagnostic)) = refusal {
            return ProviderReply {
                terminal: TerminalRecord::failure_before_dispatch(
                    call.operation,
                    call.provider_id.clone(),
                    code,
                    &call.operation_id,
                    call.exact_scope.exact_scope_sha256(),
                    Some(call.expected_state_generation),
                    diagnostic,
                ),
                payload: None,
                warnings: Vec::new(),
                extensions: Vec::new(),
                state_generation: call.expected_state_generation,
            };
        }
        self.adapter.invoke(call)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProcessIdentity {
    pid: u32,
    parent_pid: u32,
    started: String,
}

impl ProcessIdentity {
    fn read(
        pid: u32,
        deadline: Instant,
        cleanup: &CleanupCollector,
    ) -> Result<Option<Self>, String> {
        let mut command = Command::new("ps");
        command
            .args([
                "-p",
                &pid.to_string(),
                "-o",
                "pid=",
                "-o",
                "ppid=",
                "-o",
                "lstart=",
            ])
            .env("LC_ALL", "C");
        let output = bounded_output(&mut command, deadline, cleanup)?;
        if output.status.code() == Some(1) && output.stdout.is_empty() {
            return Ok(None);
        }
        if !output.status.success() {
            return Err(format!(
                "cannot read owned process start identity: {}",
                output.status
            ));
        }
        let text = String::from_utf8(output.stdout).map_err(err)?;
        let fields: Vec<_> = text.split_whitespace().collect();
        if fields.len() != 7 {
            return Err("process start identity was not fully reported by ps".into());
        }
        let identity = Self {
            pid: fields[0].parse().map_err(err)?,
            parent_pid: fields[1].parse().map_err(err)?,
            started: fields[2..].join(" "),
        };
        if identity.pid != pid {
            return Err("process identity names a different PID".into());
        }
        Ok(Some(identity))
    }

    fn evidence(&self) -> String {
        format!(
            "pid={};ppid={};start={}",
            self.pid, self.parent_pid, self.started
        )
    }

    fn confirm_exited(&self, deadline: Instant, cleanup: &CleanupCollector) -> Result<(), String> {
        if Self::read(self.pid, deadline, cleanup)?.as_ref() == Some(self) {
            return Err("owned worker still exists after the bounded reap".into());
        }
        Ok(())
    }
}

/// Keeps all actual owners alive until the explicit factory cleanup pass. Drop
/// may attempt cleanup, but only this later pass can establish final success.
#[derive(Default)]
struct CleanupCollector {
    workers: Mutex<Vec<Arc<TrackedWorker>>>,
    helper_events: Mutex<Vec<Value>>,
    unconfirmed_helpers: Mutex<Vec<Child>>,
}

struct TrackedWorker {
    owner: Arc<RustNcmWorkerOwner>,
    root: PathBuf,
    state: Mutex<WorkerCleanupState>,
}

#[derive(Default)]
struct WorkerCleanupState {
    identity: Option<ProcessIdentity>,
    active: bool,
    identity_missing: bool,
    preserve_state: bool,
    retain_failed_legacy_evidence: bool,
    final_confirmed: bool,
    events: Vec<Value>,
}

impl TrackedWorker {
    fn observe_process(&self, deadline: Instant, cleanup: &CleanupCollector) {
        let Some(pid) = self.owner.worker_pid() else {
            return;
        };
        let mut state = self.state.lock().expect("worker cleanup state");
        if state.active && state.identity.as_ref().is_some_and(|old| old.pid == pid) {
            return;
        }
        state.active = true;
        state.final_confirmed = false;
        // Record ownership before the bounded identity helper can fail.
        state
            .events
            .push(json!({"action": "observed_owned_pid", "pid": pid}));
        match ProcessIdentity::read(pid, deadline, cleanup) {
            Ok(Some(identity)) if identity.parent_pid == std::process::id() => {
                state.events.push(
                    json!({"action": "captured_start_identity", "identity": identity.evidence()}),
                );
                state.identity = Some(identity);
            }
            result => {
                state.identity_missing = true;
                state.preserve_state = true;
                state.events.push(json!({"action": "identity_unconfirmed", "pid": pid, "outcome": format!("{result:?}")}));
            }
        }
    }

    fn stop(
        &self,
        deadline: Instant,
        cleanup: &CleanupCollector,
        action: &str,
    ) -> Result<Option<ProcessIdentity>, String> {
        // Never run ps before stop: the real owner must first attempt its
        // bounded child reap and pipe-thread joins using the inherited deadline.
        let pid_before_stop = self.owner.worker_pid();
        let stopped = self.owner.request_stop(deadline).map_err(err);
        let mut outcome = match &stopped {
            Ok(true) if self.owner.worker_pid().is_none() => Ok(()),
            value => Err(format!(
                "owned worker stop did not confirm reap and pipe joins: {value:?}"
            )),
        };
        let stop_failed = outcome.is_err();
        let mut killed = None;
        if stop_failed {
            let result = self.owner.kill(deadline).map_err(err);
            killed = Some(format!("{result:?}"));
            outcome = result.and_then(|()| {
                if self.owner.worker_pid().is_none() {
                    Ok(())
                } else {
                    Err("owned worker remains after kill".into())
                }
            });
        }
        let mut state = self.state.lock().expect("worker cleanup state");
        if let Some(pid) = pid_before_stop
            && !state
                .identity
                .as_ref()
                .is_some_and(|identity| identity.pid == pid)
        {
            state.identity_missing = true;
            state
                .events
                .push(json!({"action": "uncaptured_owned_pid_at_stop", "pid": pid}));
        }
        if stop_failed {
            state.preserve_state = true;
        }
        if outcome.is_ok()
            && let Some(identity) = &state.identity
        {
            outcome = identity.confirm_exited(deadline, cleanup);
        }
        if outcome.is_ok() && state.identity_missing {
            outcome = Err("worker start identity was not captured; state retained".into());
        }
        let identity = state.identity.as_ref().map(ProcessIdentity::evidence);
        state.events.push(json!({"action": action, "identity": identity,
            "stop_result": format!("{stopped:?}"), "kill_result": killed, "outcome": format!("{outcome:?}"),
            "remaining_owned_pid": self.owner.worker_pid()}));
        if outcome.is_ok() {
            state.active = false;
        } else {
            state.preserve_state = true;
        }
        state.final_confirmed = action == "factory_final_cleanup" && outcome.is_ok();
        outcome.map(|()| state.identity.clone())
    }
}

impl CleanupCollector {
    fn finish(&self, deadline: Instant) -> Value {
        let workers = self.workers.lock().expect("factory workers").clone();
        for worker in &workers {
            let _recorded = worker.stop(deadline, self, "factory_final_cleanup");
        }
        let mut pending = self.unconfirmed_helpers.lock().expect("helper ownership");
        pending.retain_mut(|child| {
            let outcome = terminate_and_reap(child, deadline);
            self.helper_events.lock().expect("helper events").push(json!({
                "pid": child.id(), "action": "factory_final_cleanup", "outcome": format!("{outcome:?}"), "reaped": outcome.is_ok(),
            }));
            outcome.is_err()
        });
        let unconfirmed_workers = workers
            .iter()
            .filter(|worker| {
                !worker
                    .state
                    .lock()
                    .expect("worker cleanup state")
                    .final_confirmed
            })
            .count();
        let confirmed = unconfirmed_workers == 0 && pending.is_empty();
        let mut preserve_roots = BTreeMap::<PathBuf, bool>::new();
        for worker in &workers {
            let state = worker.state.lock().expect("worker cleanup state");
            *preserve_roots.entry(worker.root.clone()).or_default() |=
                state.preserve_state || state.retain_failed_legacy_evidence || !confirmed;
        }
        // Aggregate every physical owner of a scenario before removing its
        // root, including replaced owners from FreshNamespace.
        let roots: Vec<_> = preserve_roots.iter().map(|(root, preserve)| {
            let removal = if !preserve && root.exists() { Some(fs::remove_dir_all(root).map_err(err)) } else { None };
            json!({"root": root, "state_preserved": root.exists(), "removal": format!("{removal:?}")})
        }).collect();
        let worker_rows: Vec<_> = workers
            .iter()
            .map(|worker| {
                let state = worker.state.lock().expect("worker cleanup state");
                json!({"root": worker.root, "final_confirmed": state.final_confirmed,
                "state_preserved": worker.root.exists(),
                "intentional_failed_legacy_evidence": state.retain_failed_legacy_evidence,
                "events": state.events})
            })
            .collect();
        json!({"confirmed": confirmed, "unconfirmed_workers": unconfirmed_workers,
            "unconfirmed_helpers": pending.len(), "workers": worker_rows, "roots": roots,
            "helpers": *self.helper_events.lock().expect("helper events")})
    }
}

fn inherited_deadline(control: &OperationControl) -> Instant {
    let now = Instant::now();
    let remaining = control
        .snapshot()
        .map_or(0, |snapshot| snapshot.remaining_millis);
    now + Duration::from_millis(remaining)
}

fn remaining_control(deadline: Instant) -> Result<OperationControl, String> {
    let millis = u64::try_from(
        deadline
            .saturating_duration_since(Instant::now())
            .as_millis(),
    )
    .map_err(err)?;
    if millis == 0 {
        return Err("fixture action deadline elapsed".into());
    }
    Ok(OperationControl::new(
        i64::MAX,
        millis,
        CancellationToken::new(),
    ))
}

/// One owned-child runner for ps and sqlite3, with no blocking reader threads.
/// Nonblocking socket readers bound both output and waiting under the original
/// action deadline; the final 100 ms of that same budget is reserved for reap.
fn bounded_output(
    command: &mut Command,
    deadline: Instant,
    cleanup: &CleanupCollector,
) -> Result<Output, String> {
    let work_deadline = deadline
        .checked_sub(REAP_RESERVE)
        .ok_or("helper deadline underflow")?;
    if Instant::now() >= work_deadline {
        return Err("helper has no inherited work budget".into());
    }
    let (mut stdout, stdout_child) = UnixStream::pair().map_err(err)?;
    let (mut stderr, stderr_child) = UnixStream::pair().map_err(err)?;
    stdout.set_nonblocking(true).map_err(err)?;
    stderr.set_nonblocking(true).map_err(err)?;
    command
        .stdin(Stdio::null())
        .stdout(Stdio::from(OwnedFd::from(stdout_child)))
        .stderr(Stdio::from(OwnedFd::from(stderr_child)));
    let mut child = command.spawn().map_err(err)?;
    // Command retains its configured descriptors after spawn. Drop the parent
    // copies of the writer ends so EOF can prove the helper closed both streams.
    command.stdout(Stdio::null()).stderr(Stdio::null());
    let pid = child.id();
    let mut out = Vec::new();
    let mut errors = Vec::new();
    let mut closed = [false; 2];
    let mut total_bytes = 0_usize;
    let result = (|| {
        loop {
            for (index, stream, bytes) in
                [(0, &mut stdout, &mut out), (1, &mut stderr, &mut errors)]
            {
                let mut buffer = [0_u8; 8192];
                // One read per stream per poll prevents a prolific writer from
                // starving the original deadline or the other output stream.
                match stream.read(&mut buffer) {
                    Ok(0) => closed[index] = true,
                    Ok(count) => {
                        total_bytes += count;
                        if total_bytes > HELPER_OUTPUT_LIMIT {
                            return Err("helper output limit exceeded".into());
                        }
                        bytes.extend_from_slice(&buffer[..count]);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(error) => return Err(err(error)),
                }
            }
            if let Some(status) = child.try_wait().map_err(err)?
                && closed.iter().all(|value| *value)
            {
                return Ok(Output {
                    status,
                    stdout: out,
                    stderr: errors,
                });
            }
            if Instant::now() >= work_deadline {
                return Err("helper inherited deadline elapsed".into());
            }
            std::thread::sleep(
                Duration::from_millis(2)
                    .min(work_deadline.saturating_duration_since(Instant::now())),
            );
        }
    })();
    let reaped = if result.is_ok() {
        Ok(())
    } else {
        terminate_and_reap(&mut child, deadline)
    };
    cleanup.helper_events.lock().expect("helper events").push(json!({"pid": pid,
        "program": command.get_program().to_string_lossy(), "reaped": reaped.is_ok(),
        "outcome": result.as_ref().map(|output| format!("exit={}", output.status)).unwrap_or_else(|error| error.clone()),
        "reap_outcome": format!("{reaped:?}")}));
    if let Err(reason) = reaped {
        cleanup
            .unconfirmed_helpers
            .lock()
            .expect("helper ownership")
            .push(child);
        return Err(format!("helper termination unconfirmed: {reason}"));
    }
    result
}

fn terminate_and_reap(child: &mut Child, deadline: Instant) -> Result<(), String> {
    if child.try_wait().map_err(err)?.is_some() {
        return Ok(());
    }
    let killed = child.kill().map_err(err);
    loop {
        if child.try_wait().map_err(err)?.is_some() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "owned helper reap deadline elapsed; kill={killed:?}"
            ));
        }
        std::thread::sleep(
            Duration::from_millis(2).min(deadline.saturating_duration_since(Instant::now())),
        );
    }
}

struct RealNcmFactory {
    cleanup: Arc<CleanupCollector>,
    run_root: PathBuf,
    worker_binary: PathBuf,
    models: PathBuf,
}

impl CommonAdvisoryFixtureFactory for RealNcmFactory {
    fn create(
        &self,
        scenario: &CompatibilityScenario,
        exact_scope: &OwnedExactScope,
    ) -> Result<Box<dyn CommonAdvisoryFixture>, FixtureUnavailable> {
        RealNcmFixture::new(self, scenario, exact_scope)
            .map(|fixture| Box::new(fixture) as Box<dyn CommonAdvisoryFixture>)
            .map_err(|reason| FixtureUnavailable { reason })
    }
}

struct RealNcmFixture {
    cleanup: Arc<CleanupCollector>,
    worker: Option<Arc<TrackedWorker>>,
    root: PathBuf,
    provider_root: PathBuf,
    worker_binary: PathBuf,
    models: PathBuf,
    authority: FixtureAuthority,
    scope: OwnedExactScope,
    mode: Arc<AtomicU8>,
    lose_reply: Arc<AtomicBool>,
    owner: Option<Arc<RustNcmWorkerOwner>>,
    host: Option<FixtureHost>,
    stopped_process: Option<ProcessIdentity>,
    namespace_number: u64,
    legacy_references: Vec<u64>,
}

impl RealNcmFixture {
    fn new(
        factory: &RealNcmFactory,
        scenario: &CompatibilityScenario,
        scope: &OwnedExactScope,
    ) -> Result<Self, String> {
        let root = factory.run_root.join(format!(
            "scenario-{}-{}",
            sha256(scenario.case_id.as_bytes()),
            NEXT_ID.fetch_add(1, Ordering::Relaxed),
        ));
        let authority = FixtureAuthority::from_scenario(scenario, scope)?;
        fs::create_dir(&root).map_err(err)?;
        let mut fixture = Self {
            cleanup: Arc::clone(&factory.cleanup),
            worker: None,
            provider_root: root.join("provider-0"),
            root,
            worker_binary: factory.worker_binary.clone(),
            models: factory.models.clone(),
            authority,
            scope: scope.clone(),
            mode: Arc::new(AtomicU8::new(ACTIVE)),
            lose_reply: Arc::new(AtomicBool::new(false)),
            owner: None,
            host: None,
            stopped_process: None,
            namespace_number: 0,
            legacy_references: Vec::new(),
        };
        fs::create_dir(fixture.root.join("host")).map_err(err)?;
        fixture.persist_authority()?;
        write_synced(&fixture.root.join("host/mode"), b"active")?;
        fixture.open_physical_namespace()?;
        Ok(fixture)
    }

    fn host(&self) -> Result<&FixtureHost, String> {
        self.host
            .as_ref()
            .ok_or_else(|| "fixture host was shut down".into())
    }

    fn owner(&self) -> Result<&Arc<RustNcmWorkerOwner>, String> {
        self.owner
            .as_ref()
            .ok_or_else(|| "fixture worker owner was shut down".into())
    }

    fn build_identity(&self) -> Result<(String, String), String> {
        let descriptor = self.host()?.descriptor();
        Ok((
            descriptor.provider_id.as_str().into(),
            descriptor.implementation_identity_sha256,
        ))
    }

    fn namespace_database(&self) -> PathBuf {
        self.provider_root
            .join("namespaces")
            .join(NcmNamespace::from_exact_scope(&self.scope).as_str())
            .join("ncm.sqlite")
    }

    fn namespace_evidence(&self) -> String {
        self.namespace_database().display().to_string()
    }

    fn make_host(&self, owner: &Arc<RustNcmWorkerOwner>) -> Result<FixtureHost, String> {
        let inner =
            Arc::new(RustNcmSurface::from_production_worker(Arc::clone(owner)).map_err(err)?);
        let surface = Arc::new(WithholdCommittedReply {
            inner,
            armed: Arc::clone(&self.lose_reply),
            worker: Arc::clone(self.worker.as_ref().ok_or("missing worker cleanup owner")?),
            cleanup: Arc::clone(&self.cleanup),
        });
        let adapter = NcmProviderAdapter::new(surface)
            .map_err(err)?
            .with_admission_authority(Arc::new(self.authority.clone()));
        Ok(FixtureHost {
            adapter,
            mode: Arc::clone(&self.mode),
        })
    }

    fn open_physical_namespace(&mut self) -> Result<(), String> {
        fs::create_dir(&self.provider_root).map_err(err)?;
        symlink(&self.models, self.provider_root.join("models")).map_err(err)?;
        let owner = Arc::new(
            RustNcmWorkerOwner::new(RustNcmConfig {
                worker_binary: self.worker_binary.clone(),
                state_root: StateRoot::new(self.provider_root.clone())?,
                worker_options: WorkerOptions::default(),
            })
            .map_err(err)?,
        );
        let worker = Arc::new(TrackedWorker {
            owner: Arc::clone(&owner),
            root: self.root.clone(),
            state: Mutex::new(WorkerCleanupState::default()),
        });
        self.cleanup
            .workers
            .lock()
            .expect("factory workers")
            .push(Arc::clone(&worker));
        self.worker = Some(worker);
        self.owner = Some(Arc::clone(&owner));
        self.host = Some(self.make_host(&owner)?);
        if self.namespace_database().exists() {
            return Err("new physical namespace was unexpectedly populated".into());
        }
        Ok(())
    }

    fn start_current(&mut self, deadline: Instant) -> Result<ProcessIdentity, String> {
        let started = self.owner()?.start(deadline).map_err(err);
        let worker = self.worker.as_ref().ok_or("missing worker cleanup owner")?;
        worker.observe_process(deadline, &self.cleanup);
        started?;
        let state = worker.state.lock().expect("worker cleanup state");
        let identity = state
            .identity
            .clone()
            .filter(|identity| {
                Some(identity.pid) == self.owner().ok().and_then(|owner| owner.worker_pid())
            })
            .ok_or("started child has no captured process identity")?;
        self.stopped_process = None;
        Ok(identity)
    }

    fn stop_current(&mut self, deadline: Instant) -> Result<Option<ProcessIdentity>, String> {
        let Some(worker) = self.worker.as_ref() else {
            return Ok(self.stopped_process.clone());
        };
        let previous = worker.stop(deadline, &self.cleanup, "fixture_explicit_stop")?;
        self.stopped_process.clone_from(&previous);
        Ok(previous)
    }

    fn persist_authority(&self) -> Result<(), String> {
        let bytes = serde_json::to_vec(&self.authority.snapshot()?).map_err(err)?;
        write_synced(&self.root.join("host/authority.json"), &bytes)
    }

    fn revalidate_current_authority(&self) -> Result<(), String> {
        let current = serde_json::to_value(self.authority.snapshot()?).map_err(err)?;
        let retained: FixtureAuthoritySnapshot =
            serde_json::from_slice(&fs::read(self.root.join("host/authority.json")).map_err(err)?)
                .map_err(err)?;
        let reopened = FixtureAuthority::from_snapshot(retained)?;
        let retained = serde_json::to_value(reopened.snapshot()?).map_err(err)?;
        if current != retained || current.get("available").and_then(Value::as_bool) != Some(true) {
            return Err(
                "current fixture authority is unavailable or differs from its durable state".into(),
            );
        }
        Ok(())
    }

    /// Opens the actual persisted namespace during an environment restart.
    /// A corrupt/incompatible state still proves that the worker addressed the
    /// original namespace; the suite makes the subsequent readiness verdict.
    fn reopen_persisted_namespace(&self, deadline: Instant) -> Result<(), String> {
        let descriptor = self.host()?.descriptor();
        let request = HandshakeRequest::new(HandshakeRequestParts {
            provider_id: descriptor.provider_id,
            registration_revision: REGISTRATION_REVISION,
            exact_scope: self.scope.clone(),
            request_id: format!(
                "fixture.physical-reopen.{}",
                NEXT_ID.fetch_add(1, Ordering::Relaxed)
            ),
            required_capabilities: descriptor.capabilities.into_iter().collect(),
            host_limits: descriptor.limits,
            control: remaining_control(deadline)?,
            challenge_nonce: [0x6e; 32],
        })
        .map_err(err)?;
        let mut response = self.host()?.handshake(&request);
        // The existing adapter requires fresh readiness after a worker identity
        // change. Retry only that explicit transition, never an arbitrary failure.
        if response.terminal.terminal_code() == TerminalCode::StaleIdentity {
            response = self.host()?.handshake(&request);
        }
        match response.terminal.terminal_code() {
            TerminalCode::Success if response.accepted_scope.as_ref() == Some(&self.scope) => {
                Ok(())
            }
            TerminalCode::ResetRequired | TerminalCode::StateIncompatible
                if response.terminal.committed_effect().state() == CommittedEffectState::None =>
            {
                Ok(())
            }
            _ => Err(format!(
                "worker could not reopen the original namespace: {:?}",
                response.terminal
            )),
        }
    }

    fn install_legacy(
        &mut self,
        observations: &[Value],
        deadline: Instant,
    ) -> Result<FixtureEnvironmentEvidence, String> {
        if observations.is_empty()
            || observations.len() as u64 > self.host()?.descriptor().limits.observation_batch_items
            || self.namespace_database().exists()
            || self.owner()?.worker_pid().is_some()
        {
            return Err(
                "legacy installation requires bounded sources and an unopened empty namespace"
                    .into(),
            );
        }
        let build = self.build_identity()?;
        let owner = Arc::clone(self.owner()?);
        self.host.take();
        let dispatched = Arc::new(Mutex::new(Vec::new()));
        let surface = Arc::new(LegacyWriterSurface {
            inner: Arc::new(
                RustNcmSurface::from_production_worker(Arc::clone(&owner)).map_err(err)?,
            ),
            dispatched: Arc::clone(&dispatched),
            worker: Arc::clone(self.worker.as_ref().ok_or("missing legacy cleanup owner")?),
            cleanup: Arc::clone(&self.cleanup),
        });
        // This adapter intentionally has no common admission authority. Only
        // the existing legacy Observe payload is submitted to its real worker.
        let writer = NcmProviderAdapter::new(surface.clone()).map_err(err)?;
        let mut observed = Vec::new();
        for observation in observations {
            let call = legacy_observation_call(&writer, &self.scope, observation, deadline)?;
            let reply = writer.invoke(&call);
            if reply.terminal.terminal_code() != TerminalCode::Success
                || reply.terminal.committed_effect().state() != CommittedEffectState::Committed
                || reply.terminal.operation_id() != call.operation_id
                || reply
                    .terminal
                    .committed_effect()
                    .provider_receipt_sha256()
                    .is_none()
            {
                return Err(format!(
                    "real legacy Observe did not establish its original commit: {:?}",
                    reply.terminal
                ));
            }
            observed.push((call, reply));
        }
        let writer_process = self
            .stop_current(deadline)?
            .ok_or("legacy writer did not start a real process")?;
        if writer_process.parent_pid != std::process::id() {
            return Err("legacy writer process was not owned by this fixture".into());
        }
        drop(writer);
        drop(surface);
        let dispatched = std::mem::take(
            &mut *dispatched
                .lock()
                .map_err(|_| "legacy capture lock poisoned")?,
        );
        if dispatched.len() != observed.len() {
            return Err(
                "legacy capture count differs from the actual submitted observations".into(),
            );
        }
        let database = self.namespace_database();
        let mut sqlite_evidence = Vec::new();
        let metadata = self.legacy_sqlite_json(
            &database,
            "schema_version",
            "SELECT CAST(value AS TEXT) AS schema_version FROM meta WHERE key = 'schema_version';",
            deadline,
            &mut sqlite_evidence,
        )?;
        if metadata.len() != 1 || metadata[0]["schema_version"] != "1" {
            return Err("actual legacy writer did not retain its version 1 storage schema".into());
        }
        let stored_version: u64 = metadata[0]["schema_version"]
            .as_str()
            .ok_or("stored schema version is not text")?
            .parse()
            .map_err(err)?;
        let rows = self.legacy_sqlite_json(
            &database,
            "capsules_and_journal",
            &format!("{LEGACY_DURABLE_SELECT} ORDER BY c.record_id;"),
            deadline,
            &mut sqlite_evidence,
        )?;
        let counts = self.legacy_sqlite_json(
            &database,
            "record_counts",
            "SELECT (SELECT count(*) FROM capsules) AS capsules, (SELECT count(*) FROM events) AS events;",
            deadline,
            &mut sqlite_evidence,
        )?;
        if rows.len() != observations.len()
            || counts.len() != 1
            || counts[0]["capsules"].as_u64() != Some(rows.len() as u64)
            || counts[0]["events"].as_u64() != Some(rows.len() as u64)
        {
            return Err(
                "actual legacy writer did not populate exactly the requested source records".into(),
            );
        }
        let mut records = Vec::new();
        let mut durability = Vec::new();
        for (((call, reply), (opaque_call, surface_reply)), row) in
            observed.iter().zip(dispatched.iter()).zip(rows.iter())
        {
            let public_reply: Value = serde_json::from_slice(
                &reply
                    .payload
                    .as_ref()
                    .ok_or("legacy Observe returned no payload")?
                    .bytes,
            )
            .map_err(err)?;
            let record_id = public_reply["record_id"]
                .as_u64()
                .filter(|id| *id > 0)
                .ok_or("legacy worker returned no positive record reference")?;
            let original_key = call
                .idempotency_key
                .as_deref()
                .ok_or("legacy caller key missing")?;
            let original_receipt = reply
                .terminal
                .committed_effect()
                .provider_receipt_sha256()
                .ok_or("legacy original commit receipt missing")?;
            let opaque_key = opaque_call
                .idempotency_key
                .as_deref()
                .ok_or("legacy dispatch key missing")?;
            let public_input: Value = serde_json::from_slice(&call.payload.bytes).map_err(err)?;
            let opaque_input: Value =
                serde_json::from_slice(&opaque_call.payload.bytes).map_err(err)?;
            let provenance: Value = serde_json::from_str(
                row["provenance"]
                    .as_str()
                    .ok_or("legacy capsule provenance missing")?,
            )
            .map_err(err)?;
            let expected_provenance = json!({
                "observation_kind": public_input["observation_kind"],
                "payload_contract": public_input["payload_contract"],
            });
            let receipt_text = row["receipt"]
                .as_str()
                .ok_or("legacy journal receipt missing")?;
            let receipt: Value = serde_json::from_str(receipt_text).map_err(err)?;
            let content = public_input["canonical_payload"]["content"]
                .as_str()
                .ok_or("legacy content missing")?;
            let role = public_input["canonical_payload"]["role"]
                .as_str()
                .ok_or("legacy role missing")?;
            if provenance != expected_provenance
                || provenance.get("common_capsule").is_some()
                || provenance.get("delivery_capsule").is_some()
                || row["record_id"].as_u64() != Some(record_id)
                || row["key_text"] != content
                || row["value_text"] != format!("{role}: {content}")
                || row["source_id"] != opaque_input["canonical_payload"]["forget_source_key"]
                || row["idempotency_key"].as_str() != Some(opaque_key)
                || row["kind"] != "observe"
                || row["status"] != "valid"
                || row["seq"].as_u64() != Some(reply.state_generation)
                || row["commit_seq"].as_u64() != Some(reply.state_generation)
                || receipt["reply"]["state_generation"].as_u64() != Some(reply.state_generation)
                || receipt["reply"]["payload"] != public_reply
                || receipt["operation"]["observe"]["record_id"].as_u64() != Some(record_id)
                || surface_reply.payload != reply.payload
                || legacy_receipt_from_durable_basis(&receipt)? != original_receipt
            {
                return Err(
                    "legacy capsule or journal does not match the real dispatched commit".into(),
                );
            }
            let stored_record = serde_json::to_string(row).map_err(err)?;
            if stored_record.contains(&call.operation_id)
                || stored_record.contains(original_key)
                || stored_record.contains(original_receipt)
            {
                return Err("legacy durable identity assumptions differ from the actual stored representation".into());
            }
            records.push(LegacyRecordEvidence {
                // The old API returns a numeric record_id. Its exact decimal
                // representation preserves that original provider reference.
                stable_memory_ref: record_id.to_string(),
                original_operation_id: LegacyIdentityEvidence::CallerObservedOnly {
                    value: reply.terminal.operation_id().to_owned(),
                },
                original_idempotency_key: LegacyIdentityEvidence::CallerObservedOnly {
                    value: original_key.to_owned(),
                },
                original_receipt_sha256: original_receipt.to_owned(),
                immutable_record_sha256: ncm_legacy_immutable_record_sha256(row)?,
                stored_receipt_bytes_sha256: sha256(receipt_text.as_bytes()),
                retained_source_fields: BTreeMap::new(),
            });
            durability.push(json!({
                "provider_reference": {"observed_record_id": record_id,
                    "durable_capsule_record_id": row["record_id"], "durable_journal_record_id": receipt["operation"]["observe"]["record_id"],
                    "legacy_reference_text": record_id.to_string()},
                "caller_observed_only": {"original_operation_id": reply.terminal.operation_id(),
                    "original_public_idempotency_key": original_key,
                    "operation_id_retained_in_capsule_or_event": false,
                    "public_idempotency_key_retained_in_capsule_or_event": false},
                "provider_receipt": {"observed_original_sha256": original_receipt,
                    "digest_stored_verbatim": false, "durable_basis_verified": true,
                    "durable_basis": "events.receipt.reply.state_generation and payload, excluding replayed"},
                "observed_dispatch": {"opaque_operation_id": opaque_call.operation_id,
                    "opaque_idempotency_key": opaque_key,
                    "opaque_source": opaque_input["canonical_payload"]["forget_source_key"]},
                "actually_durable": {"record": row, "parsed_receipt": receipt},
                "retained_canonical_source_fields": {},
                "common_capsule_present": false, "delivery_capsule_present": false,
            }));
        }
        // Preserve this audit beside the scenario directory, so fixture cleanup
        // cannot erase the distinction between retained and caller-only identity.
        write_synced(
            &self.root.with_extension("legacy-writer-evidence.json"),
            &serde_json::to_vec_pretty(&json!({
                "representation": "production legacy Observe without common capsules",
                "stored_schema_version": stored_version, "numeric_schema_migration": false,
                "writer_process": writer_process.evidence(), "writer_process_exited": true,
                "namespace": self.namespace_evidence(), "records": durability,
                "common_suite_records": records,
            }))
            .map_err(err)?,
        )?;
        self.legacy_references = rows
            .iter()
            .map(|row| {
                row["record_id"]
                    .as_u64()
                    .ok_or_else(|| "legacy stored reference missing".to_owned())
            })
            .collect::<Result<Vec<_>, String>>()?;
        self.host = Some(self.make_host(&owner)?);
        if self.build_identity()? != build {
            return Err(
                "legacy installation changed the selected production implementation".into(),
            );
        }
        Ok(FixtureEnvironmentEvidence::LegacyV1Installed {
            stored_version,
            source_count: rows.len() as u64,
            records,
        })
    }

    fn legacy_sqlite_json(
        &self,
        database: &Path,
        stage: &str,
        sql: &str,
        deadline: Instant,
        evidence: &mut Vec<Value>,
    ) -> Result<Vec<Value>, String> {
        // Exactly three fixed audit queries call this wrapper. Store paths and
        // metadata only in local evidence; never include record content.
        let mut files = Vec::new();
        for suffix in ["", "-wal", "-shm"] {
            let mut path = database.as_os_str().to_os_string();
            path.push(suffix);
            files.push(sqlite_file_metadata(&PathBuf::from(path)));
        }
        evidence.push(json!({"stage": stage, "before_action": files, "outcome": "pending"}));
        let audit_path = self.root.with_extension("legacy-sqlite-evidence.json");
        write_synced(
            &audit_path,
            &serde_json::to_vec_pretty(evidence).map_err(err)?,
        )?;
        let result = sqlite_json(database, sql, true, deadline, &self.cleanup);
        evidence.last_mut().expect("legacy SQLite stage")["outcome"] = match &result {
            Ok(rows) => json!({"state": "success", "returned_rows": rows.len()}),
            Err(reason) => json!({"state": "failed", "failure_stage": stage,
                "reason": reason.chars().take(2048).collect::<String>()}),
        };
        write_synced(
            &audit_path,
            &serde_json::to_vec_pretty(evidence).map_err(err)?,
        )?;
        result.map_err(|_| {
            format!("legacy SQLite audit failed at {stage}; exact error retained in local evidence")
        })
    }

    fn corrupt_payload(&mut self, deadline: Instant) -> Result<FixtureEnvironmentEvidence, String> {
        self.stop_current(deadline)?
            .ok_or("cannot corrupt an unstarted provider")?;
        let database = self.namespace_database();
        let before = read_capsule(&database, deadline, &self.cleanup)?;
        let record_id = before["record_id"]
            .as_u64()
            .ok_or("capsule record ID is invalid")?;
        let mut provenance: Value = serde_json::from_str(
            before["provenance"]
                .as_str()
                .ok_or("capsule provenance is absent")?,
        )
        .map_err(err)?;
        let original = capsule_bytes(&provenance)?;
        let claimed = provenance["common_capsule"]["sha256"]
            .as_str()
            .ok_or("stored capsule digest is absent")?
            .to_owned();
        let before_actual = sha256(&original);
        if before_actual != claimed {
            return Err("fixture requires an intact real capsule before corruption".into());
        }
        let mut payload: Value = serde_json::from_slice(&original).map_err(err)?;
        let content = payload["canonical_payload"]["content"]
            .as_str()
            .ok_or("stored common observation has no actual content payload")?;
        payload["canonical_payload"]["content"] =
            json!(format!("{content} [physical fixture corruption]"));
        let changed = serde_json::to_vec(&payload).map_err(err)?;
        provenance["common_capsule"]["bytes"] = json!(changed);
        let encoded = serde_json::to_string(&provenance).map_err(err)?;
        // This SQL only addresses a positively read record in the fixture's
        // stopped, isolated database. It preserves schema, digest and all other rows.
        let changed_rows = sqlite_json(
            &database,
            &format!(
                "UPDATE capsules SET provenance = '{}' WHERE record_id = {record_id}; SELECT changes() AS changed;",
                encoded.replace('\'', "''"),
            ),
            false,
            deadline,
            &self.cleanup,
        )?;
        if changed_rows.as_slice() != [json!({"changed": 1})] {
            return Err("physical corruption did not update exactly the selected capsule".into());
        }
        let after = read_capsule(&database, deadline, &self.cleanup)?;
        if after["record_id"] != before["record_id"] {
            return Err("physical corruption selected a different retained record".into());
        }
        let after: Value = serde_json::from_str(
            after["provenance"]
                .as_str()
                .ok_or("changed provenance missing")?,
        )
        .map_err(err)?;
        let after_claimed = after["common_capsule"]["sha256"]
            .as_str()
            .ok_or("claimed digest disappeared after physical mutation")?
            .to_owned();
        let after_actual = sha256(&capsule_bytes(&after)?);
        if claimed != after_claimed || before_actual == after_actual {
            return Err("physical corruption failed to preserve only the claimed digest".into());
        }
        Ok(FixtureEnvironmentEvidence::PayloadCorrupted {
            before_actual_sha256: before_actual,
            after_actual_sha256: after_actual,
            before_claimed_sha256: claimed,
            after_claimed_sha256: after_claimed,
        })
    }

    fn environment(
        &mut self,
        action: &FixtureEnvironmentAction,
    ) -> Result<FixtureEnvironmentEvidence, String> {
        let deadline = Instant::now() + LIFECYCLE_BUDGET;
        match action {
            FixtureEnvironmentAction::Restart => {
                let identity = self.build_identity()?;
                let database = self.namespace_database();
                let metadata = fs::metadata(&database).map_err(err)?;
                let physical_file = (metadata.dev(), metadata.ino());
                let previous = self
                    .stop_current(deadline)?
                    .ok_or("restart has no previous real process")?;
                let current = self.start_current(deadline)?;
                if previous == current {
                    return Err("restart reused the previous process start identity".into());
                }
                self.reopen_persisted_namespace(deadline)?;
                let metadata = fs::metadata(&database).map_err(err)?;
                if physical_file != (metadata.dev(), metadata.ino())
                    || identity != self.build_identity()?
                {
                    return Err(
                        "restart changed the physical store or selected implementation".into(),
                    );
                }
                previous.confirm_exited(deadline, &self.cleanup)?;
                Ok(FixtureEnvironmentEvidence::Restarted {
                    previous_process: previous.evidence(),
                    current_process: current.evidence(),
                    previous_process_exited: true,
                    reopened_persisted_namespace: true,
                })
            }
            FixtureEnvironmentAction::FreshNamespace => {
                let previous_namespace = self.namespace_evidence();
                let identity = self.build_identity()?;
                self.revalidate_current_authority()?;
                self.stop_current(deadline)?;
                self.host.take();
                self.owner.take();
                self.namespace_number = self
                    .namespace_number
                    .checked_add(1)
                    .ok_or("namespace counter overflow")?;
                self.provider_root = self
                    .root
                    .join(format!("provider-{}", self.namespace_number));
                self.open_physical_namespace()?;
                self.start_current(deadline)?;
                if self.namespace_database().exists() || self.build_identity()? != identity {
                    return Err(
                        "fresh namespace did not preserve selection and empty physical state"
                            .into(),
                    );
                }
                Ok(FixtureEnvironmentEvidence::FreshNamespace {
                    previous_namespace,
                    current_namespace: self.namespace_evidence(),
                    current_dispositions_revalidated: true,
                })
            }
            FixtureEnvironmentAction::OpenSession { destination_scope } => {
                destination_scope.validate().map_err(err)?;
                let identity = self.build_identity()?;
                self.scope = destination_scope.clone();
                self.host = Some(self.make_host(self.owner()?)?);
                // Reopening a populated session must refresh the new surface's
                // declared generation before the suite checks its next handshake.
                self.reopen_persisted_namespace(deadline)?;
                Ok(FixtureEnvironmentEvidence::SessionOpened {
                    destination_scope: self.scope.clone(),
                    selected_provider_unchanged: self.build_identity()? == identity,
                })
            }
            FixtureEnvironmentAction::InstallLegacyV1 { observations } => {
                let result = self.install_legacy(observations, deadline);
                if result.is_err()
                    && let Some(worker) = &self.worker
                {
                    worker
                        .state
                        .lock()
                        .expect("worker cleanup state")
                        .retain_failed_legacy_evidence = true;
                }
                result
            }
            FixtureEnvironmentAction::InspectLegacyDurableState { .. } => {
                // Use actual old stored numeric references only. Keep the reopened
                // worker live; inability to read within the budget remains Unknown.
                if self.legacy_references.is_empty() || self.legacy_references.len() > 64 {
                    return Err("legacy physical audit has no bounded stored references".into());
                }
                let mut records = Vec::new();
                for reference in &self.legacy_references {
                    let rows = sqlite_json(
                        &self.namespace_database(),
                        &format!(
                            "{LEGACY_DURABLE_SELECT} WHERE c.record_id = {reference} LIMIT 2;"
                        ),
                        true,
                        deadline,
                        &self.cleanup,
                    )?;
                    for row in rows {
                        records.push(ncm_legacy_durable_record(&row));
                    }
                }
                Ok(FixtureEnvironmentEvidence::LegacyDurableState { records })
            }
            FixtureEnvironmentAction::CorruptPayloadPreservingDigest => {
                self.corrupt_payload(deadline)
            }
            FixtureEnvironmentAction::RecordSourceDisposition {
                source_key,
                disposition,
            } => {
                let state = SourceDisposition::from_wire(disposition)
                    .ok_or("unknown source disposition")?;
                let authority_ref = self.authority.record_disposition(source_key, state)?;
                self.persist_authority()?;
                Ok(FixtureEnvironmentEvidence::SourceDispositionRecorded {
                    source_key: source_key.clone(),
                    disposition: disposition.clone(),
                    authority_ref,
                })
            }
            FixtureEnvironmentAction::LoseNextReplyAfterCommit => {
                if self.lose_reply.swap(true, Ordering::SeqCst) {
                    return Err("previous committed-reply loss hook remains armed".into());
                }
                Ok(FixtureEnvironmentEvidence::LostReplyArmed)
            }
            FixtureEnvironmentAction::SetAdmissionAuthorityAvailable { available } => {
                self.authority.set_available(*available)?;
                self.persist_authority()?;
                Ok(FixtureEnvironmentEvidence::AuthorityAvailability {
                    available: *available,
                })
            }
            FixtureEnvironmentAction::SetMode { mode } => {
                let identity = self.build_identity()?;
                let next = match mode.as_str() {
                    "active" => ACTIVE,
                    "observe" => OBSERVE,
                    "disabled" => DISABLED,
                    "quarantined" => QUARANTINED,
                    _ => return Err("unknown fixture host mode".into()),
                };
                write_synced(&self.root.join("host/mode"), mode.as_bytes())?;
                self.mode.store(next, Ordering::SeqCst);
                Ok(FixtureEnvironmentEvidence::ModeChanged {
                    mode: mode.clone(),
                    selected_provider_unchanged: self.build_identity()? == identity,
                    recall_admitted: self.host()?.admits(ProviderOperation::Recall),
                    observation_admitted: self.host()?.admits(ProviderOperation::Observe),
                })
            }
            FixtureEnvironmentAction::Shutdown => {
                let previous = self.stop_current(deadline)?;
                self.host.take();
                self.owner.take();
                if let Some(previous) = previous {
                    previous.confirm_exited(deadline, &self.cleanup)?;
                }
                Ok(FixtureEnvironmentEvidence::Shutdown {
                    remaining_workers: 0,
                })
            }
        }
    }
}

impl CommonAdvisoryFixture for RealNcmFixture {
    fn provider(&self) -> &dyn MemoryProvider {
        self.host
            .as_ref()
            .expect("suite requested a provider after explicit shutdown")
    }

    fn apply_environment(
        &mut self,
        action: &FixtureEnvironmentAction,
    ) -> Result<FixtureEnvironmentEvidence, FixtureUnavailable> {
        self.environment(action)
            .map_err(|reason| FixtureUnavailable { reason })
    }
}

impl Drop for RealNcmFixture {
    fn drop(&mut self) {
        if let Some(worker) = &self.worker {
            let _recorded = worker.stop(
                Instant::now() + LIFECYCLE_BUDGET,
                &self.cleanup,
                "fixture_drop_provisional",
            );
        }
        // Factory retains every actual owner and state directory until its
        // explicit final cleanup; this best-effort Drop is never a pass signal.
        self.host.take();
        self.owner.take();
    }
}

fn legacy_observation_call(
    writer: &NcmProviderAdapter,
    scope: &OwnedExactScope,
    observation: &Value,
    deadline: Instant,
) -> Result<ProviderCall, String> {
    if observation["observation_kind"] != "session.message_committed.v1"
        || observation["payload_contract"] != "tracedecay.memory.observation.session-message.v1"
    {
        return Err(
            "legacy fixture writer requires the supplied session message observation".into(),
        );
    }
    let content = observation["canonical_payload"]["content"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or("legacy fixture source text is absent")?;
    let role = observation["canonical_payload"]["role"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or("legacy fixture message role is absent")?;
    let forget_source_key = observation
        .pointer("/source_identity/original_source/source/source_key")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or("legacy routing key is absent")?;
    let legacy = json!({
        "observation_kind": "session.message_committed.v1",
        "payload_contract": "tracedecay.memory.observation.session-message.v1",
        "canonical_payload": {"role": role, "content": content, "forget_source_key": forget_source_key},
    });
    let descriptor = writer.descriptor();
    let identity = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let ready_request = HandshakeRequest::new(HandshakeRequestParts {
        provider_id: descriptor.provider_id.clone(),
        registration_revision: REGISTRATION_REVISION,
        exact_scope: scope.clone(),
        request_id: format!("fixture.legacy.ready.{identity}"),
        required_capabilities: vec![
            OwnedVersionedId::new(ProviderOperation::Observe.capability_id()).map_err(err)?,
        ],
        host_limits: descriptor.limits,
        control: remaining_control(deadline)?,
        challenge_nonce: [0x76; 32],
    })
    .map_err(err)?;
    let mut ready = writer.handshake(&ready_request);
    if ready.terminal.terminal_code() == TerminalCode::StaleIdentity {
        ready = writer.handshake(&ready_request);
    }
    if ready.terminal.terminal_code() != TerminalCode::Success
        || ready.accepted_scope.as_ref() != Some(scope)
    {
        return Err(format!(
            "real legacy worker could not become ready: {:?}",
            ready.terminal
        ));
    }
    let bytes = serde_json::to_vec(&legacy).map_err(err)?;
    let digest = sha256(&bytes);
    let call = ProviderCall::new(ProviderCallParts {
        operation: ProviderOperation::Observe,
        provider_id: descriptor.provider_id,
        registration_revision: REGISTRATION_REVISION,
        ready_receipt_sha256: ready
            .ready_receipt_sha256
            .ok_or("legacy readiness has no receipt")?,
        exact_scope: scope.clone(),
        request_id: format!("fixture.legacy.request.{identity}"),
        operation_id: format!("fixture.legacy.original-operation.{identity}"),
        expected_state_generation: ready
            .descriptor
            .ok_or("legacy readiness has no descriptor")?
            .state_generation,
        idempotency_key: Some(format!("fixture.legacy.original-delivery.{identity}")),
        control: remaining_control(deadline)?,
        payload: CanonicalPayload::new(
            OwnedVersionedId::new("tracedecay.memory.provider.observation.v1").map_err(err)?,
            bytes,
            digest,
        )
        .map_err(err)?,
        required_capabilities: vec![
            OwnedVersionedId::new(ProviderOperation::Observe.capability_id()).map_err(err)?,
        ],
        extensions: Vec::new(),
    })
    .map_err(err)?;
    // This is the same explicit fixture hygiene admission as the existing
    // legacy adapter tests; it creates no common source authorization capsule.
    let receipt = PayloadSanitizationReceipt::new(
        PayloadSanitizationReceiptParts::accepted_unmodified_with_extensions(
            "tracedecay.memory.observation.hygiene.v1+ncm-real-legacy-fixture",
            call.payload.sha256.clone(),
            observation_extensions_digest(&call.extensions).map_err(err)?,
        ),
    )
    .map_err(err)?;
    Ok(call.with_sanitization(receipt))
}

// The existing old-writer audit projection, shared by baseline and both live
// point reads. These fields identify retained content and its original event.
// Mutable model/recall counters, center state and global generation are excluded.
const LEGACY_DURABLE_SELECT: &str = "SELECT c.record_id, c.source_id, c.key_text, c.value_text, c.provenance, c.status, c.commit_seq, e.seq, e.kind, e.idempotency_key, e.payload_sha256, e.receipt FROM capsules AS c JOIN events AS e ON e.seq = c.commit_seq";
fn ncm_legacy_immutable_record_sha256(row: &Value) -> Result<String, String> {
    let mut projection = serde_json::Map::new();
    for field in [
        "record_id",
        "source_id",
        "key_text",
        "value_text",
        "provenance",
        "status",
        "commit_seq",
        "seq",
        "kind",
        "idempotency_key",
        "payload_sha256",
    ] {
        projection.insert(
            field.into(),
            row.get(field)
                .ok_or("legacy immutable stored field missing")?
                .clone(),
        );
    }
    Ok(sha256(&serde_json::to_vec(&projection).map_err(err)?))
}
fn ncm_legacy_durable_record(row: &Value) -> LegacyDurableRecordEvidence {
    let receipt_bytes = row["receipt"]
        .as_str()
        .map(str::as_bytes)
        .unwrap_or_default();
    let parsed = serde_json::from_slice::<Value>(receipt_bytes).ok();
    // The fresh immutable projection includes the exact old capsule-free
    // provenance bytes. A newly synthesized mapping changes that fingerprint.
    // This old representation has no stored original public operation/key.
    LegacyDurableRecordEvidence {
        stable_memory_ref: row["record_id"]
            .as_u64()
            .map(|value| value.to_string())
            .unwrap_or_default(),
        immutable_record_sha256: ncm_legacy_immutable_record_sha256(row).unwrap_or_default(),
        stored_receipt_bytes_sha256: sha256(receipt_bytes),
        original_receipt_sha256: parsed
            .as_ref()
            .and_then(|value| legacy_receipt_from_durable_basis(value).ok())
            .unwrap_or_default(),
        retained_operation_id: None,
        retained_idempotency_key: None,
    }
}

/// Verify the observed old receipt against its actual durable basis. This never
/// supplies a substitute receipt or invents an original operation identity.
fn legacy_receipt_from_durable_basis(receipt: &Value) -> Result<String, String> {
    let generation = receipt["reply"]["state_generation"]
        .as_u64()
        .ok_or("legacy durable reply generation missing")?;
    let mut payload = receipt["reply"]["payload"].clone();
    let object = payload
        .as_object_mut()
        .ok_or("legacy durable reply payload missing")?;
    object.remove("replayed");
    object.remove("common_observation");
    let bytes = serde_json::to_vec(&payload).map_err(err)?;
    let mut digest = Sha256::new();
    digest.update(b"tracedecay.ncm.rust-worker-receipt.v1\0");
    digest.update((b"observe".len() as u64).to_be_bytes());
    digest.update(b"observe");
    digest.update(generation.to_be_bytes());
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(&bytes);
    Ok(digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn capsule_bytes(provenance: &Value) -> Result<Vec<u8>, String> {
    let bytes = provenance["common_capsule"]["bytes"]
        .as_array()
        .ok_or("capsule bytes are absent")?;
    if bytes.is_empty() || bytes.len() > 131_072 {
        return Err("capsule bytes exceed the existing common bound".into());
    }
    bytes
        .iter()
        .map(|value| {
            value
                .as_u64()
                .and_then(|value| u8::try_from(value).ok())
                .ok_or_else(|| "capsule contains a non-byte value".into())
        })
        .collect()
}

fn read_capsule(
    database: &Path,
    deadline: Instant,
    cleanup: &CleanupCollector,
) -> Result<Value, String> {
    let mut rows = sqlite_json(
        database,
        "SELECT record_id, provenance FROM capsules WHERE status = 'valid' ORDER BY record_id LIMIT 1;",
        true,
        deadline,
        cleanup,
    )?;
    if rows.len() != 1 {
        return Err("physical fixture has no single retained capsule".into());
    }
    Ok(rows.remove(0))
}

/// Records existence, type, size and the regular-file open stage without
/// reading database content. Nonregular paths are never opened.
fn sqlite_file_metadata(path: &Path) -> Value {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            let file_type = metadata.file_type();
            let kind = if file_type.is_file() {
                "regular_file"
            } else if file_type.is_dir() {
                "directory"
            } else if file_type.is_symlink() {
                "symlink"
            } else {
                "other"
            };
            let open_error = file_type
                .is_file()
                .then(|| {
                    File::open(path).map(drop).map_err(|error| {
                    json!({"stage": "read_only_file_open", "kind": format!("{:?}", error.kind()),
                        "os_error": error.raw_os_error()})
                }).err()
                })
                .flatten();
            json!({"path": path, "exists": true, "type": kind, "bytes": metadata.len(),
                "mode": metadata.mode(), "read_only_open_attempted": file_type.is_file(),
                "open_error": open_error})
        }
        Err(error) => json!({"path": path,
            "exists": if error.kind() == std::io::ErrorKind::NotFound { Some(false) } else { None },
            "metadata_error": {"stage": "symlink_metadata", "kind": format!("{:?}", error.kind()),
                "os_error": error.raw_os_error()}}),
    }
}

fn sqlite_json(
    database: &Path,
    sql: &str,
    read_only: bool,
    deadline: Instant,
    cleanup: &CleanupCollector,
) -> Result<Vec<Value>, String> {
    if !database.is_file() {
        return Err("fixture database does not exist".into());
    }
    let mut command = Command::new("sqlite3");
    command.args(["-batch", "-json"]);
    if read_only {
        // A closed WAL database can have no sidecars. Permit SQLite to open
        // its WAL infrastructure while prohibiting SQL writes to retained data.
        command.args(["-cmd", "PRAGMA query_only=ON;"]);
    }
    command.arg(database).arg(sql);
    let output = bounded_output(&mut command, deadline, cleanup)?;
    if !output.status.success() {
        return Err(format!(
            "fixture SQLite action failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    serde_json::from_slice(&output.stdout).map_err(err)
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let temporary = path.with_extension(format!(
        "pending-{}",
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let mut file = File::create_new(&temporary).map_err(err)?;
    file.write_all(bytes).map_err(err)?;
    file.sync_all().map_err(err)?;
    drop(file);
    fs::rename(&temporary, path).map_err(err)?;
    File::open(path.parent().ok_or("durable fixture file has no parent")?)
        .map_err(err)?
        .sync_all()
        .map_err(err)
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("NCM crate is under the repository crates directory")
        .to_path_buf()
}

fn target_dir() -> PathBuf {
    match std::env::var_os("CARGO_TARGET_DIR") {
        Some(path) => {
            let path = PathBuf::from(path);
            if path.is_absolute() {
                path
            } else {
                repository_root().join(path)
            }
        }
        None => repository_root().join("target"),
    }
}

#[test]
#[ignore = "requires TRACEDECAY_NCM_WORKER, TRACEDECAY_NCM_REAL_MODEL_ROOT, pinned offline model, ps and sqlite3"]
fn common_advisory_factory_runs_unchanged_suite_with_production_ncm() {
    let worker_binary = PathBuf::from(
        std::env::var_os("TRACEDECAY_NCM_WORKER")
            .expect("set the already-built real NCM worker path; this test never invokes Cargo"),
    );
    assert!(worker_binary.is_absolute() && worker_binary.is_file());
    let model_root = PathBuf::from(
        std::env::var_os("TRACEDECAY_NCM_REAL_MODEL_ROOT")
            .expect("set the installed pinned offline model root"),
    );
    assert!(model_root.is_absolute());
    let models = model_root.join("models").canonicalize().unwrap();
    assert!(models.is_dir());
    let run_root = target_dir().join("test-profile").join(format!(
        "ncm-common-real-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    fs::create_dir_all(&run_root).unwrap();
    let factory = RealNcmFactory {
        cleanup: Arc::new(CleanupCollector::default()),
        run_root: run_root.clone(),
        worker_binary,
        models,
    };
    let scope = OwnedExactScope::new(
        "profile.common-advisory",
        "project.common-advisory",
        "repository.common-advisory",
        "worktree.common-advisory",
        "refs/heads/common-advisory",
        "session.common-advisory",
        format!(
            "sha256:{}",
            sha256(b"fixed exact common advisory fixture scope")
        ),
    )
    .unwrap();
    let report = run_common_advisory_suite(&factory, &scope, REGISTRATION_REVISION);
    let cleanup = factory.cleanup.finish(Instant::now() + LIFECYCLE_BUDGET);
    write_synced(
        &run_root.join("cleanup.json"),
        &serde_json::to_vec_pretty(&cleanup).unwrap(),
    )
    .unwrap();
    let report = report.unwrap();
    let rows: Vec<_> = report.results.iter().map(|row| json!({
        "case_id": row.case_id, "step_id": row.step_id,
        "verdict": format!("{:?}", row.verdict), "details": row.details,
        "environment_evidence": row.environment_evidence.as_ref().map(|value| format!("{value:?}")),
        "operation_report_present": row.operation_report.is_some(),
    })).collect();
    let d = report.denominators;
    let summary = json!({
        "cleanup": cleanup,
        "common_program_matched": report.common_program_matched,
        "compatible": report.compatible(),
        "legacy_delivery_identity": report.legacy_delivery_identity,
        "denominators": {"planned": d.planned, "passed": d.passed, "failed": d.failed,
            "degraded": d.degraded, "unknown": d.unknown, "unknown_effects": d.unknown_effects,
            "reconciled_effects": d.reconciled_effects},
        "reached_operations": report.reached_operations,
        "missing_operations": report.missing_operations, "rows": rows,
    });
    write_synced(
        &run_root.join("summary.json"),
        &serde_json::to_vec_pretty(&summary).unwrap(),
    )
    .unwrap();
    let mut evidence =
        std::io::BufWriter::new(File::create_new(run_root.join("actual-report.txt")).unwrap());
    writeln!(evidence, "{report:?}").unwrap();
    evidence.flush().unwrap();
    evidence.get_ref().sync_all().unwrap();
    let failures: Vec<_> = report
        .results
        .iter()
        .filter(|row| {
            matches!(
                row.verdict,
                CompatibilityVerdict::Failed | CompatibilityVerdict::Unknown
            )
        })
        .take(20)
        .map(|row| {
            let details = row
                .details
                .iter()
                .take(2)
                .map(|detail| detail.chars().take(256).collect::<String>())
                .collect::<Vec<_>>()
                .join("; ");
            format!(
                "{} / {}: {:?}: {}",
                row.case_id.chars().take(128).collect::<String>(),
                row.step_id.chars().take(128).collect::<String>(),
                row.verdict,
                details
            )
        })
        .collect();
    assert!(
        cleanup["confirmed"] == true,
        "real fixture cleanup unconfirmed; evidence at {}",
        run_root.display()
    );
    assert!(
        report.compatible(),
        "real NCM common suite is not compatible; actual report at {}; denominators: {}; \
         first 20 failed/unknown steps (at most 2 details of 256 characters each):\n{}",
        run_root.display(),
        summary["denominators"],
        failures.join("\n")
    );
}
