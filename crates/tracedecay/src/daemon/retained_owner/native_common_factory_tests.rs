//! The unchanged common suite over real Native actors in owned test processes.
//!
//! RPC carries the actual provider API values. Lifecycle metadata may retain the
//! last verbatim descriptor after a witnessed exit; readiness and operation
//! replies are never synthesized. Canonical graph storage and provider-local
//! namespaces are separate, and every child is reaped before its namespace moves.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tracedecay_domain::canonical_text::sha256_hex;
use tracedecay_domain::{FactOwnerV1, UserProfileId};
use tracedecay_memory_conformance::compatibility::{
    CommonAdvisoryFixture, CommonAdvisoryFixtureFactory, CompatibilityScenario, CompatibilityStep,
    FixtureEnvironmentAction, FixtureEnvironmentEvidence, FixtureUnavailable,
    LegacyDurableRecordEvidence, LegacyIdentityEvidence, LegacyRecordEvidence,
    common_advisory_scenarios, run_common_advisory_suite, run_compatibility_program, scope_json,
};
use tracedecay_memory_conformance::real_fixture::{
    DescriptorDto, FixtureAuthority, FixtureAuthoritySnapshot, HandshakeRequestDto,
    HandshakeResponseDto, ProviderCallDto, ProviderReplyDto,
};
use tracedecay_memory_conformance::{
    ExpectedCommittedEffect, GenerationExpectation, PayloadExpectation, ProductStepOutput,
    TerminalExpectation,
};
use tracedecay_memory_provider_registry::{
    AdvisoryAdmissionAuthority, AdvisoryAdmissionError, COMMON_ADVISORY_PROFILE_ID,
    CancellationToken, CanonicalPayload, CommittedEffectState, CurrentAdvisoryAdmission,
    DegradationCauseV1, EnabledProviderMode, FabricConfig, FabricError, HandshakeRequest,
    HandshakeRequestParts, HandshakeResponse, MemoryProviderV1, NATIVE_PROVIDER_ID,
    NATIVE_RECALL_SCOPE_BINDINGS, NativeProvider, OperationControl, OwnedExactScope,
    OwnedProviderId, OwnedVersionedId, ProjectMemoryProviderComposition, ProviderCall,
    ProviderCallParts, ProviderDescriptor, ProviderExecutionShapeV1, ProviderLifecycleAdapterV1,
    ProviderLifecycleOwnershipV1, ProviderOperation, ProviderRegistrationV1, ProviderReply,
    ProviderSupervisorV1, QuarantinePolicyV1, RecallScopeBindingsV1, RestartBudgetV1,
    SelectedProviderActivationV1, ShutdownBudgetV1, SourceDisposition, SupervisedScopeV1,
    SupervisorOutcomeV1, TerminalCode,
};

use super::native_provider::{
    IMPLEMENTATION_IDENTITY_SHA256, ProjectNativeMemoryApplicationPort, STATE_SCHEMA_VERSION,
    native_provider_limits,
};
use super::native_staged_observations::{
    StagedObservationRecord, StagedObservationStore, StagedOutcome, exact_scope_from_value,
    staged_store_path,
};
use crate::tracedecay::{TraceDecay, TraceDecayOpenOptions};

const CHILD_TEST: &str =
    "daemon::retained_owner::native_common_factory_tests::native_common_provider_child";
const CHILD_CONFIG_ENV: &str = "TRACEDECAY_NATIVE_COMMON_CHILD_CONFIG";
const FRAME_MAXIMUM: usize = 20 * 1024 * 1024;
const ENVIRONMENT_BUDGET: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(2);
const CLEANUP_BUDGET: Duration = Duration::from_secs(5);
const PROBE_WITNESS_MAXIMUM: usize = 4096;

fn failure(error: impl std::fmt::Display) -> String {
    error.to_string()
}
fn unavailable(error: impl std::fmt::Display) -> FixtureUnavailable {
    FixtureUnavailable {
        reason: error.to_string(),
    }
}
fn now_micros() -> Result<i64, String> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(failure)?
            .as_micros(),
    )
    .map_err(failure)
}
fn deadline_after(duration: Duration) -> Result<i64, String> {
    now_micros()?
        .checked_add(i64::try_from(duration.as_micros()).map_err(failure)?)
        .ok_or_else(|| "fixture deadline overflow".into())
}
fn before_deadline(deadline: i64) -> Result<(), String> {
    if now_micros()? < deadline {
        Ok(())
    } else {
        Err("owned fixture deadline elapsed".into())
    }
}
fn scope(value: &Value) -> Result<OwnedExactScope, String> {
    exact_scope_from_value(value).map_err(failure)
}

/// Dispatch and ordinary response waiting share the original call control.
/// A separate finite cleanup bound may only drain an already dispatched reply
/// or terminate an unusable transport; it never extends provider execution.
struct WaitBound<'a> {
    deadline: Instant,
    control: Option<&'a OperationControl>,
}
impl<'a> WaitBound<'a> {
    fn environment() -> Self {
        Self {
            deadline: Instant::now() + ENVIRONMENT_BUDGET,
            control: None,
        }
    }
    fn cleanup() -> Self {
        Self {
            deadline: Instant::now() + CLEANUP_BUDGET,
            control: None,
        }
    }
    fn until(deadline: i64) -> Result<Self, String> {
        let remaining = deadline
            .checked_sub(now_micros()?)
            .ok_or("fixture deadline overflow")?;
        let duration = Duration::from_micros(u64::try_from(remaining).map_err(failure)?);
        Ok(Self {
            deadline: Instant::now() + duration,
            control: None,
        })
    }
    fn operation(control: &'a OperationControl) -> Self {
        Self {
            deadline: Instant::now() + ENVIRONMENT_BUDGET,
            control: Some(control),
        }
    }
    fn check(&self) -> Result<(), String> {
        if let Some(control) = self.control {
            control.snapshot().map_err(|error| format!("{error:?}"))?;
        }
        if Instant::now() >= self.deadline {
            return Err("fixture transport bound elapsed".into());
        }
        Ok(())
    }
}
fn transfer(
    mut action: impl FnMut(&mut [u8]) -> io::Result<usize>,
    bytes: &mut [u8],
    bound: &WaitBound<'_>,
) -> Result<(), String> {
    let mut offset = 0;
    while offset < bytes.len() {
        bound.check()?;
        match action(&mut bytes[offset..]) {
            Ok(0) => return Err("fixture transport closed mid-frame".into()),
            Ok(count) => offset += count,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) =>
            {
                thread::sleep(POLL)
            }
            Err(error) => return Err(failure(error)),
        }
    }
    Ok(())
}
fn encoded_frame<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
    let payload = serde_json::to_vec(value).map_err(failure)?;
    if payload.is_empty() || payload.len() > FRAME_MAXIMUM {
        return Err("fixture frame length exceeded".into());
    }
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&u32::try_from(payload.len()).map_err(failure)?.to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}
fn write_frame<T: Serialize>(
    stream: &mut TcpStream,
    value: &T,
    bound: &WaitBound<'_>,
) -> Result<(), String> {
    let mut frame = encoded_frame(value)?;
    transfer(|bytes| stream.write(bytes), &mut frame, bound)
}

/// The sole owner of one response frame. Both offsets survive a control-ended
/// return, so cleanup resumes the exact same header/body without a second reader.
#[derive(Default)]
struct FrameReader {
    header: [u8; 4],
    header_read: usize,
    body: Vec<u8>,
    body_read: usize,
}
impl FrameReader {
    fn read<T: DeserializeOwned>(
        &mut self,
        stream: &mut TcpStream,
        bound: &WaitBound<'_>,
        mut probe: Option<&mut ParentProbe>,
    ) -> Result<T, String> {
        loop {
            if let Some(probe) = probe.as_deref_mut() {
                probe.poll(self, bound.control)?;
            }
            bound.check()?;
            if self.header_read == self.header.len() && self.body.is_empty() {
                let length = usize::try_from(u32::from_be_bytes(self.header)).map_err(failure)?;
                if length == 0 || length > FRAME_MAXIMUM {
                    return Err("fixture frame length exceeded".into());
                }
                self.body.resize(length, 0);
            }
            if !self.body.is_empty() && self.body_read == self.body.len() {
                return serde_json::from_slice(&self.body).map_err(failure);
            }
            let reading_header = self.header_read < self.header.len();
            let target = if reading_header {
                &mut self.header[self.header_read..]
            } else {
                &mut self.body[self.body_read..]
            };
            match stream.read(target) {
                Ok(0) => return Err("fixture transport closed mid-frame".into()),
                Ok(count) => {
                    if reading_header {
                        self.header_read += count;
                    } else {
                        self.body_read += count;
                    }
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) =>
                {
                    thread::sleep(POLL)
                }
                Err(error) => return Err(failure(error)),
            }
        }
    }
}
fn read_frame<T: DeserializeOwned>(
    stream: &mut TcpStream,
    bound: &WaitBound<'_>,
) -> Result<T, String> {
    FrameReader::default().read(stream, bound, None)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CleanupEntry {
    pid: u32,
    launch_identity: String,
    child_identity: Option<String>,
    provider_root: PathBuf,
    confirmed_reaped: bool,
    exit_status: Option<String>,
    failures: Vec<String>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CleanupSnapshot {
    children: BTreeMap<String, CleanupEntry>,
    fixture_failures: Vec<String>,
    preserved_directories: Vec<PathBuf>,
}
impl CleanupSnapshot {
    fn all_reaped(&self) -> bool {
        self.children.values().all(|child| child.confirmed_reaped)
    }
}
#[derive(Clone, Default)]
struct CleanupLedger(Arc<Mutex<CleanupSnapshot>>);
impl CleanupLedger {
    fn snapshot(&self) -> CleanupSnapshot {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }
    fn register(&self, pid: u32, launch: &str, provider_root: &Path) {
        let mut state = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        if state.children.contains_key(launch) {
            state
                .fixture_failures
                .push(format!("duplicate child launch {launch}"));
            return;
        }
        state.children.insert(
            launch.into(),
            CleanupEntry {
                pid,
                launch_identity: launch.into(),
                child_identity: None,
                provider_root: provider_root.into(),
                confirmed_reaped: false,
                exit_status: None,
                failures: Vec::new(),
            },
        );
    }
    fn identity(&self, launch: &str, identity: &str) {
        let mut state = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        if let Some(child) = state.children.get_mut(launch) {
            child.child_identity = Some(identity.into());
        } else {
            state
                .fixture_failures
                .push(format!("identity for unregistered child {launch}"));
        }
    }
    fn reaped(&self, launch: &str, pid: u32, status: ExitStatus) {
        let mut state = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        match state.children.get_mut(launch) {
            Some(child) if child.pid == pid => {
                child.confirmed_reaped = true;
                child.exit_status = Some(status.to_string());
            }
            _ => state
                .fixture_failures
                .push(format!("reap witness did not match child {launch}:{pid}")),
        }
    }
    fn error(&self, launch: &str, error: impl Into<String>) {
        let error = error.into();
        let mut state = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        if let Some(child) = state.children.get_mut(launch) {
            child.failures.push(error);
        } else {
            state.fixture_failures.push(error);
        }
    }
    fn preserve(&self, path: PathBuf) {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .preserved_directories
            .push(path);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum ProbeKind {
    CancelInAuthority,
    SplitResponsePrefix,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TransportProbe {
    kind: ProbeKind,
    operation_id: String,
    witness_path: PathBuf,
    release_path: PathBuf,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProbeWitness {
    pid: u32,
    rpc_id: u64,
    operation_id: String,
    original_deadline_utc_micros: i64,
    original_remaining_millis: u64,
    provider_invocations: u64,
    authority_entries: u64,
    terminal_code: Option<String>,
}
#[derive(Clone, Debug, Default)]
struct ParentProbeEvidence {
    pid: Option<u32>,
    rpc_id: Option<u64>,
    original_deadline_utc_micros: Option<i64>,
    cancelled_after_witness: bool,
    drained_actual_response: bool,
    retained_header_bytes: usize,
    retained_body_bytes: usize,
    witness: Option<ProbeWitness>,
    child: Option<ProbeWitness>,
    failures: Vec<String>,
}
#[derive(Default)]
struct ParentProbeState {
    pending: Option<TransportProbe>,
    last: Option<ParentProbeEvidence>,
}
struct ParentProbe {
    config: TransportProbe,
    evidence: ParentProbeEvidence,
}
impl ParentProbe {
    fn poll(
        &mut self,
        reader: &FrameReader,
        control: Option<&OperationControl>,
    ) -> Result<(), String> {
        if self.evidence.cancelled_after_witness {
            return Ok(());
        }
        if self.config.kind == ProbeKind::SplitResponsePrefix && reader.header_read != 2 {
            return Ok(());
        }
        let bytes = match fs::read(&self.config.witness_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(failure(error)),
        };
        if bytes.len() > PROBE_WITNESS_MAXIMUM {
            return Err("child probe witness exceeded bound".into());
        }
        let witness: ProbeWitness = serde_json::from_slice(&bytes).map_err(failure)?;
        let control = control.ok_or("probe has no original call control")?;
        control.snapshot().map_err(|error| format!("{error:?}"))?;
        if witness.operation_id != self.config.operation_id
            || Some(witness.pid) != self.evidence.pid
            || Some(witness.rpc_id) != self.evidence.rpc_id
            || witness.original_deadline_utc_micros != control.deadline_utc_micros()
            || witness.provider_invocations != 1
            || (self.config.kind == ProbeKind::CancelInAuthority && witness.authority_entries != 1)
        {
            return Err("actual child probe witness differs from dispatched call".into());
        }
        self.evidence.original_deadline_utc_micros = Some(control.deadline_utc_micros());
        self.evidence.witness = Some(witness);
        self.evidence.retained_header_bytes = reader.header_read;
        self.evidence.retained_body_bytes = reader.body_read;
        control.cancellation().cancel();
        self.evidence.cancelled_after_witness = true;
        if self.config.kind == ProbeKind::SplitResponsePrefix {
            // The child emitted only two header bytes. Release the remaining
            // actual frame after cancellation, forcing this reader to resume.
            fs::write(&self.config.release_path, b"release actual response").map_err(failure)?;
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChildConfig {
    project_root: PathBuf,
    profile_root: PathBuf,
    provider_root: PathBuf,
    exact_scope: Value,
    authority: FixtureAuthoritySnapshot,
    address: String,
    launch_identity: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChildHello {
    pid: u32,
    entry_unix_nanos: u128,
    launch_identity: String,
    channel: String,
}
#[derive(Serialize, Deserialize)]
enum ChildRequest {
    Descriptor,
    Handshake {
        value: HandshakeRequestDto,
    },
    Invoke {
        value: ProviderCallDto,
    },
    RecordDisposition {
        source_key: String,
        disposition: String,
    },
    SetAuthorityAvailable(bool),
    LoseNextReply,
    ArmProbe(TransportProbe),
    ProbeEvidence,
    Shutdown,
}
#[derive(Serialize, Deserialize)]
enum ChildResponse {
    Ready {
        authority: FixtureAuthoritySnapshot,
    },
    Descriptor(DescriptorDto),
    Handshake(HandshakeResponseDto),
    Reply(ProviderReplyDto),
    Authority {
        snapshot: FixtureAuthoritySnapshot,
        reference: Option<String>,
    },
    LostReplyArmed,
    ProbeArmed,
    ProbeEvidence(ProbeWitness),
    Stopped,
    Error(String),
}
#[derive(Serialize)]
struct OutgoingRpc<'a> {
    request_id: u64,
    request: &'a ChildRequest,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IncomingRpc {
    request_id: u64,
    request: ChildRequest,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseFrame {
    request_id: u64,
    response: ChildResponse,
}

struct OwnedChild {
    process: Child,
    rpc: TcpStream,
    cancellation: TcpStream,
    identity: String,
    launch_identity: String,
    cleanup: CleanupLedger,
    request_sequence: u64,
    usable: bool,
}
impl OwnedChild {
    fn exchange(
        &mut self,
        request: &ChildRequest,
        bound: &WaitBound<'_>,
    ) -> Result<ChildResponse, String> {
        self.exchange_with_probe(request, bound, None)
    }
    fn exchange_with_probe(
        &mut self,
        request: &ChildRequest,
        bound: &WaitBound<'_>,
        mut probe: Option<&mut ParentProbe>,
    ) -> Result<ChildResponse, String> {
        if !self.usable {
            return Err("Native RPC channel is permanently unusable".into());
        }
        bound.check()?;
        self.request_sequence = self
            .request_sequence
            .checked_add(1)
            .ok_or("fixture RPC sequence overflow")?;
        let id = self.request_sequence;
        if let Some(probe) = probe.as_deref_mut() {
            probe.evidence.pid = Some(self.process.id());
            probe.evidence.rpc_id = Some(id);
        }
        if let Err(error) = write_frame(
            &mut self.rpc,
            &OutgoingRpc {
                request_id: id,
                request,
            },
            bound,
        ) {
            // Never complete or retry a partially written request after its
            // dispatch bound ends. Only teardown may follow this point.
            return Err(self.invalidate(format!("RPC request write failed: {error}"), bound));
        }
        let mut reader = FrameReader::default();
        let first: Result<ResponseFrame, String> =
            reader.read(&mut self.rpc, bound, probe.as_deref_mut());
        let response = match first {
            Ok(response) => response,
            Err(error)
                if bound
                    .control
                    .is_some_and(|control| control.snapshot().is_err()) =>
            {
                // The operation remains under its original control in both
                // processes. This independent finite budget only forwards the
                // live cancel and drains the already dispatched actual reply.
                let drain = WaitBound::cleanup();
                let cancel_bound = WaitBound {
                    deadline: drain
                        .deadline
                        .min(Instant::now() + Duration::from_millis(20)),
                    control: None,
                };
                let mut cancellation = id.to_be_bytes();
                if let Err(cancel_error) = transfer(
                    |bytes| self.cancellation.write(bytes),
                    &mut cancellation,
                    &cancel_bound,
                ) {
                    return Err(self.invalidate(
                        format!("RPC ended ({error}); cancellation channel failed: {cancel_error}"),
                        bound,
                    ));
                }
                if let Some(probe) = probe.as_deref_mut() {
                    probe.evidence.retained_header_bytes = reader.header_read;
                    probe.evidence.retained_body_bytes = reader.body_read;
                }
                match reader.read::<ResponseFrame>(&mut self.rpc, &drain, None) {
                    Ok(response) => {
                        if let Some(probe) = probe.as_deref_mut() {
                            probe.evidence.drained_actual_response = true;
                        }
                        response
                    }
                    Err(drain_error) => {
                        return Err(self.invalidate(
                            format!(
                                "RPC ended ({error}); actual response drain failed: {drain_error}"
                            ),
                            bound,
                        ));
                    }
                }
            }
            Err(error) => {
                return Err(self.invalidate(format!("RPC response failed: {error}"), bound));
            }
        };
        if response.request_id != id {
            return Err(self.invalidate(
                format!(
                    "RPC response id {} did not match dispatched id {id}",
                    response.request_id
                ),
                bound,
            ));
        }
        if !matches!(&response.response, ChildResponse::Error(_))
            && !matches!(
                (request, &response.response),
                (ChildRequest::Descriptor, ChildResponse::Descriptor(_))
                    | (ChildRequest::Handshake { .. }, ChildResponse::Handshake(_))
                    | (ChildRequest::Invoke { .. }, ChildResponse::Reply(_))
                    | (
                        ChildRequest::RecordDisposition { .. },
                        ChildResponse::Authority { .. }
                    )
                    | (
                        ChildRequest::SetAuthorityAvailable(_),
                        ChildResponse::Authority { .. }
                    )
                    | (ChildRequest::LoseNextReply, ChildResponse::LostReplyArmed)
                    | (ChildRequest::ArmProbe(_), ChildResponse::ProbeArmed)
                    | (ChildRequest::ProbeEvidence, ChildResponse::ProbeEvidence(_))
                    | (ChildRequest::Shutdown, ChildResponse::Stopped)
            )
        {
            return Err(self.invalidate("RPC response kind differs from request".into(), bound));
        }
        match response.response {
            ChildResponse::Error(error) => {
                Err(self.invalidate(format!("child transport failure: {error}"), bound))
            }
            response => Ok(response),
        }
    }
    fn invalidate(&mut self, error: String, bound: &WaitBound<'_>) -> String {
        self.usable = false;
        self.rpc.shutdown(Shutdown::Both).ok();
        self.cancellation.shutdown(Shutdown::Both).ok();
        self.cleanup.error(&self.launch_identity, error.clone());
        // Operation teardown has its separately approved finite bound.
        // Lifecycle/environment calls retain their existing absolute bound;
        // a failed startup leaves its owned handle for supervisor cleanup.
        let remaining = if bound.control.is_some() {
            CLEANUP_BUDGET
        } else {
            bound.deadline.saturating_duration_since(Instant::now())
        };
        let cleanup = deadline_after(remaining).and_then(|deadline| self.kill_and_reap(deadline));
        match cleanup {
            Ok(()) => format!("{error}; owned child killed and reaped"),
            Err(cleanup) => {
                self.cleanup.error(&self.launch_identity, cleanup.clone());
                format!("{error}; owned child cleanup unconfirmed: {cleanup}")
            }
        }
    }
    fn witnessed_exit(&mut self) -> Result<bool, String> {
        match self.process.try_wait().map_err(failure)? {
            Some(status) => {
                self.cleanup
                    .reaped(&self.launch_identity, self.process.id(), status);
                Ok(true)
            }
            None => Ok(false),
        }
    }
    fn confirm_exit(&mut self, deadline: i64) -> Result<bool, String> {
        loop {
            if self.witnessed_exit()? {
                return Ok(true);
            }
            if before_deadline(deadline).is_err() {
                return Ok(false);
            }
            thread::sleep(POLL);
        }
    }
    fn kill_and_reap(&mut self, deadline: i64) -> Result<(), String> {
        self.usable = false;
        if !self.witnessed_exit()? {
            self.process.kill().map_err(failure)?;
        }
        self.rpc.shutdown(Shutdown::Both).ok();
        self.cancellation.shutdown(Shutdown::Both).ok();
        if !self.confirm_exit(deadline)? {
            return Err("owned Native child death unconfirmed".into());
        }
        Ok(())
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        // Never panic while another fixture failure is unwinding. The final
        // factory-owned ledger gate makes an unconfirmed reap a test failure.
        let result =
            deadline_after(CLEANUP_BUDGET).and_then(|deadline| self.kill_and_reap(deadline));
        if let Err(error) = result {
            self.cleanup.error(&self.launch_identity, error);
        }
    }
}

struct ProcessState {
    config: ChildConfig,
    control_root: PathBuf,
    launches: u64,
    cleanup: CleanupLedger,
    child: Option<OwnedChild>,
    starting_child: Option<Child>,
    last_exited_process: Option<String>,
    last_descriptor: Option<ProviderDescriptor>,
}
impl ProcessState {
    fn spawn(&mut self, deadline: i64) -> Result<(), String> {
        before_deadline(deadline)?;
        if self.child.is_some() || self.starting_child.is_some() {
            return Err("cannot replace an unreaped Native child".into());
        }
        self.launches = self
            .launches
            .checked_add(1)
            .ok_or("fixture launch sequence overflow")?;
        let listener = TcpListener::bind(("127.0.0.1", 0)).map_err(failure)?;
        listener.set_nonblocking(true).map_err(failure)?;
        self.config.address = listener.local_addr().map_err(failure)?.to_string();
        self.config.launch_identity = format!("{}:{}", self.control_root.display(), self.launches);
        let config_path = self
            .control_root
            .join(format!("child-{}.json", self.launches));
        let bytes = serde_json::to_vec(&self.config).map_err(failure)?;
        if bytes.len() > FRAME_MAXIMUM {
            return Err("fixture child config exceeded bound".into());
        }
        fs::write(&config_path, bytes).map_err(failure)?;
        let log = File::create(
            self.control_root
                .join(format!("child-{}.log", self.launches)),
        )
        .map_err(failure)?;
        let process = Command::new(std::env::current_exe().map_err(failure)?)
            .args([
                "--exact",
                CHILD_TEST,
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD_CONFIG_ENV, &config_path)
            .stdin(Stdio::null())
            .stdout(log.try_clone().map_err(failure)?)
            .stderr(log)
            .spawn()
            .map_err(failure)?;
        // Retain the handle immediately: failed startup remains owned so the
        // supervisor can kill/reap it even before either socket exists.
        self.cleanup.register(
            process.id(),
            &self.config.launch_identity,
            &self.config.provider_root,
        );
        self.starting_child = Some(process);
        let startup = (|| {
            let mut rpc = None;
            let mut cancellation = None;
            let mut identity = None;
            while rpc.is_none() || cancellation.is_none() {
                before_deadline(deadline)?;
                let process = self
                    .starting_child
                    .as_mut()
                    .ok_or("missing starting child")?;
                if let Some(status) = process.try_wait().map_err(failure)? {
                    self.cleanup
                        .reaped(&self.config.launch_identity, process.id(), status);
                    return Err("Native fixture exited before connecting".into());
                }
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream.set_nonblocking(true).map_err(failure)?;
                        let hello: ChildHello =
                            read_frame(&mut stream, &WaitBound::until(deadline)?)?;
                        if hello.pid
                            != self
                                .starting_child
                                .as_ref()
                                .ok_or("missing starting child")?
                                .id()
                            || hello.launch_identity != self.config.launch_identity
                        {
                            return Err("fixture child identity mismatch".into());
                        }
                        let actual = format!(
                            "pid:{}:child-entry-unix-nanos:{}:launch:{}",
                            hello.pid, hello.entry_unix_nanos, hello.launch_identity
                        );
                        if identity
                            .as_ref()
                            .is_some_and(|previous| previous != &actual)
                        {
                            return Err("fixture channels name different incarnations".into());
                        }
                        identity = Some(actual);
                        match hello.channel.as_str() {
                            "rpc" if rpc.is_none() => rpc = Some(stream),
                            "cancellation" if cancellation.is_none() => cancellation = Some(stream),
                            _ => return Err("unexpected fixture child channel".into()),
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => thread::sleep(POLL),
                    Err(error) => return Err(failure(error)),
                }
            }
            Ok((
                rpc.ok_or("missing RPC channel")?,
                cancellation.ok_or("missing cancellation channel")?,
                identity.ok_or("missing process identity")?,
            ))
        })();
        let (rpc, cancellation, identity) = startup?;
        let process = self.starting_child.take().ok_or("missing starting child")?;
        self.cleanup
            .identity(&self.config.launch_identity, &identity);
        self.child = Some(OwnedChild {
            process,
            rpc,
            cancellation,
            identity,
            launch_identity: self.config.launch_identity.clone(),
            cleanup: self.cleanup.clone(),
            request_sequence: 0,
            usable: true,
        });
        // Failed initialization retains the owned child for the supervisor's
        // separately bounded shutdown; start does not spend cleanup time past
        // its own deadline.
        let child = self
            .child
            .as_mut()
            .ok_or("missing initialized child owner")?;
        let ready: ChildResponse = read_frame(&mut child.rpc, &WaitBound::until(deadline)?)?;
        match ready {
            ChildResponse::Ready { authority } => {
                // This snapshot came from the independent fixture authority
                // opened in the child before Native construction/readiness.
                FixtureAuthority::from_snapshot(authority.clone())?;
                self.config.authority = authority;
            }
            ChildResponse::Error(error) => return Err(error),
            _ => return Err("Native child did not finish real initialization".into()),
        }
        let ChildResponse::Descriptor(value) =
            child.exchange(&ChildRequest::Descriptor, &WaitBound::until(deadline)?)?
        else {
            return Err("missing actual Native descriptor".into());
        };
        self.last_descriptor = Some(value.restore()?);
        Ok(())
    }
    fn stop(&mut self, deadline: i64, force: bool) -> Result<bool, String> {
        if let Some(process) = self.starting_child.as_mut() {
            let mut status = process.try_wait().map_err(failure)?;
            if status.is_none() {
                process.kill().map_err(failure)?;
            }
            loop {
                if let Some(status) = status {
                    self.cleanup
                        .reaped(&self.config.launch_identity, process.id(), status);
                    break;
                }
                if before_deadline(deadline).is_err() {
                    return Ok(false);
                }
                thread::sleep(POLL);
                status = process.try_wait().map_err(failure)?;
            }
            self.starting_child.take();
        }
        let Some(child) = self.child.as_mut() else {
            return Ok(true);
        };
        if !child.witnessed_exit()? {
            if force || !child.usable {
                child.kill_and_reap(deadline)?;
            } else {
                let bound = WaitBound::until(deadline)?;
                if !matches!(
                    child.exchange(&ChildRequest::Shutdown, &bound),
                    Ok(ChildResponse::Stopped)
                ) {
                    return Ok(false);
                }
                if !child.confirm_exit(deadline)? {
                    return Ok(false);
                }
            }
        }
        self.last_exited_process = Some(child.identity.clone());
        self.child.take();
        Ok(true)
    }
}
impl Drop for ProcessState {
    fn drop(&mut self) {
        let result = deadline_after(CLEANUP_BUDGET).and_then(|deadline| {
            if self.stop(deadline, true)? {
                Ok(())
            } else {
                Err("Native final owner drop did not confirm reap".into())
            }
        });
        if let Err(error) = result {
            self.cleanup.error(&self.config.launch_identity, error);
        }
    }
}

#[derive(Clone)]
struct NativeProcessProxy {
    state: Arc<Mutex<ProcessState>>,
    probes: Arc<Mutex<ParentProbeState>>,
}
impl NativeProcessProxy {
    fn lock(&self, bound: &WaitBound<'_>) -> Result<MutexGuard<'_, ProcessState>, String> {
        loop {
            bound.check()?;
            match self.state.try_lock() {
                Ok(state) => return Ok(state),
                Err(TryLockError::WouldBlock) => thread::sleep(POLL),
                Err(TryLockError::Poisoned(_)) => {
                    return Err("Native fixture owner lock poisoned".into());
                }
            }
        }
    }
    fn cleanup_lock(&self, bound: &WaitBound<'_>) -> Result<MutexGuard<'_, ProcessState>, String> {
        loop {
            bound.check()?;
            match self.state.try_lock() {
                Ok(state) => return Ok(state),
                Err(TryLockError::Poisoned(poison)) => return Ok(poison.into_inner()),
                Err(TryLockError::WouldBlock) => thread::sleep(POLL),
            }
        }
    }
    fn force_cleanup(&self, deadline: i64) -> Result<(), String> {
        let bound = WaitBound::until(deadline)?;
        let mut state = self.cleanup_lock(&bound)?;
        if state.stop(deadline, true)? {
            Ok(())
        } else {
            Err("Native forced cleanup did not confirm child reap".into())
        }
    }
    fn request(
        &self,
        request: ChildRequest,
        control: Option<&OperationControl>,
    ) -> Result<ChildResponse, String> {
        self.request_with_probe(request, control, None)
    }
    fn request_with_probe(
        &self,
        request: ChildRequest,
        control: Option<&OperationControl>,
        probe: Option<&mut ParentProbe>,
    ) -> Result<ChildResponse, String> {
        let bound = control.map_or_else(WaitBound::environment, WaitBound::operation);
        let mut state = self.lock(&bound)?;
        state
            .child
            .as_mut()
            .ok_or("Native child is not running; no provider reply can be fabricated")?
            .exchange_with_probe(&request, &bound, probe)
    }
    fn take_probe(&self, call: &ProviderCall) -> Result<Option<ParentProbe>, String> {
        let mut probes = self
            .probes
            .lock()
            .map_err(|_| "parent probe state poisoned")?;
        if probes
            .pending
            .as_ref()
            .is_some_and(|probe| probe.operation_id == call.operation_id)
        {
            Ok(probes.pending.take().map(|config| ParentProbe {
                config,
                evidence: ParentProbeEvidence::default(),
            }))
        } else {
            Ok(None)
        }
    }
    fn collect_probe_evidence(&self) -> Result<(), String> {
        if self
            .probes
            .lock()
            .map_err(|_| "parent probe state poisoned")?
            .last
            .is_none()
        {
            return Ok(());
        }
        let bound = WaitBound::cleanup();
        let mut state = self.cleanup_lock(&bound)?;
        let response = state
            .child
            .as_mut()
            .ok_or("probe child is absent")?
            .exchange(&ChildRequest::ProbeEvidence, &bound)?;
        let ChildResponse::ProbeEvidence(evidence) = response else {
            return Err("missing actual child probe evidence".into());
        };
        let mut probes = self
            .probes
            .lock()
            .map_err(|_| "parent probe state poisoned")?;
        let parent = probes
            .last
            .as_mut()
            .ok_or("missing parent probe evidence")?;
        parent.child = Some(evidence);
        Ok(())
    }
    fn descriptor_result(&self) -> Result<ProviderDescriptor, String> {
        let bound = WaitBound::environment();
        let mut state = self.lock(&bound)?;
        if let Some(child) = state.child.as_mut() {
            let ChildResponse::Descriptor(value) =
                child.exchange(&ChildRequest::Descriptor, &bound)?
            else {
                return Err("wrong descriptor response".into());
            };
            let descriptor = value.restore()?;
            state.last_descriptor = Some(descriptor.clone());
            Ok(descriptor)
        } else {
            state
                .last_descriptor
                .clone()
                .ok_or_else(|| "no actual child descriptor has been captured".into())
        }
    }
}
impl MemoryProviderV1 for NativeProcessProxy {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor_result()
            .expect("actual Native descriptor transport")
    }
    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        let value = HandshakeRequestDto::capture(request).expect("lossless handshake request");
        let ChildResponse::Handshake(value) = self
            .request(ChildRequest::Handshake { value }, Some(&request.control))
            .expect("actual Native handshake transport")
        else {
            panic!("wrong handshake response");
        };
        value.restore().expect("lossless actual handshake response")
    }
    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        let value = ProviderCallDto::capture(call).expect("lossless original provider call");
        let mut probe = self.take_probe(call).expect("test probe state");
        let result = self.request_with_probe(
            ChildRequest::Invoke { value },
            Some(&call.control),
            probe.as_mut(),
        );
        if let Some(mut probe) = probe {
            if let Err(error) = &result {
                probe.evidence.failures.push(error.clone());
            }
            self.probes
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .last = Some(probe.evidence);
        }
        let ChildResponse::Reply(value) = result.expect("actual Native invoke transport") else {
            panic!("wrong invoke response");
        };
        // Even after caller cancellation, this is the actual correlated child
        // terminal. The common runner retains/evaluates it without replacement.
        value.restore().expect("lossless actual Native reply")
    }
}

#[derive(Clone)]
struct NativeLifecycle {
    proxy: NativeProcessProxy,
}
impl ProviderLifecycleAdapterV1 for NativeLifecycle {
    type Error = io::Error;
    fn start(&self, deadline: i64) -> Result<(), Self::Error> {
        let bound = WaitBound::until(deadline).map_err(io::Error::other)?;
        self.proxy
            .lock(&bound)
            .map_err(io::Error::other)?
            .spawn(deadline)
            .map_err(io::Error::other)
    }
    fn handshake(
        &self,
        request: &HandshakeRequest,
        deadline: i64,
    ) -> Result<HandshakeResponse, Self::Error> {
        let mut bound = WaitBound::until(deadline).map_err(io::Error::other)?;
        bound.control = Some(&request.control);
        let value = HandshakeRequestDto::capture(request).map_err(io::Error::other)?;
        let mut state = self.proxy.lock(&bound).map_err(io::Error::other)?;
        let child = state
            .child
            .as_mut()
            .ok_or_else(|| io::Error::other("Native child is absent"))?;
        let ChildResponse::Handshake(response) = child
            .exchange(&ChildRequest::Handshake { value }, &bound)
            .map_err(io::Error::other)?
        else {
            return Err(io::Error::other("wrong actual handshake response"));
        };
        response.restore().map_err(io::Error::other)
    }
    fn request_stop(&self, deadline: i64) -> Result<bool, Self::Error> {
        let bound = WaitBound::until(deadline).map_err(io::Error::other)?;
        self.proxy
            .lock(&bound)
            .map_err(io::Error::other)?
            .stop(deadline, false)
            .map_err(io::Error::other)
    }
    fn kill(&self, deadline: i64) -> Result<(), Self::Error> {
        let bound = WaitBound::until(deadline).map_err(io::Error::other)?;
        if self
            .proxy
            .lock(&bound)
            .map_err(io::Error::other)?
            .stop(deadline, true)
            .map_err(io::Error::other)?
        {
            Ok(())
        } else {
            Err(io::Error::other("Native child not reaped"))
        }
    }
}

fn handshake_request(scope: &OwnedExactScope, label: &str) -> Result<HandshakeRequest, String> {
    HandshakeRequest::new(HandshakeRequestParts {
        provider_id: OwnedProviderId::new(NATIVE_PROVIDER_ID).map_err(failure)?,
        registration_revision: 1,
        exact_scope: scope.clone(),
        request_id: label.into(),
        required_capabilities: vec![
            OwnedVersionedId::new(COMMON_ADVISORY_PROFILE_ID).map_err(failure)?,
        ],
        host_limits: native_provider_limits(),
        control: OperationControl::new(
            deadline_after(ENVIRONMENT_BUDGET)?,
            30_000,
            CancellationToken::new(),
        ),
        challenge_nonce: [13; 32],
    })
    .map_err(failure)
}

struct NativeCommonFactory {
    project_root: PathBuf,
    profile_root: PathBuf,
    scenario_root: PathBuf,
    sequence: AtomicU64,
    cleanup: CleanupLedger,
    owners: Mutex<Vec<Arc<Mutex<ProcessState>>>>,
}
struct NativeCommonFixture {
    proxy: Arc<NativeProcessProxy>,
    supervisor: ProviderSupervisorV1<NativeLifecycle>,
    base_scope: OwnedExactScope,
    current_scope: OwnedExactScope,
    namespace: u64,
    root: Option<tempfile::TempDir>,
    legacy_references: Vec<String>,
    cleanup: CleanupLedger,
}
impl NativeCommonFactory {
    fn create_owned(
        &self,
        scenario: &CompatibilityScenario,
        exact_scope: &OwnedExactScope,
    ) -> Result<NativeCommonFixture, FixtureUnavailable> {
        let ordinal = self.sequence.fetch_add(1, Ordering::Relaxed);
        let root = tempfile::Builder::new()
            .prefix(&format!("native-{ordinal}-"))
            .tempdir_in(&self.scenario_root)
            .map_err(unavailable)?;
        let control_root = root.path().join("control");
        let provider_root = root.path().join("namespace-0");
        fs::create_dir_all(&control_root).map_err(unavailable)?;
        fs::create_dir_all(&provider_root).map_err(unavailable)?;
        let authority =
            FixtureAuthority::from_scenario(scenario, exact_scope).map_err(unavailable)?;
        let proxy = Arc::new(NativeProcessProxy {
            state: Arc::new(Mutex::new(ProcessState {
                config: ChildConfig {
                    project_root: self.project_root.clone(),
                    profile_root: self.profile_root.clone(),
                    provider_root,
                    exact_scope: scope_json(exact_scope),
                    authority: authority.snapshot().map_err(unavailable)?,
                    address: String::new(),
                    launch_identity: String::new(),
                },
                control_root,
                launches: 0,
                cleanup: self.cleanup.clone(),
                child: None,
                starting_child: None,
                last_exited_process: None,
                last_descriptor: None,
            })),
            probes: Arc::new(Mutex::new(ParentProbeState::default())),
        });
        // Retain every actual owner before startup, including a fixture whose
        // construction later fails. Final collection rechecks these owners.
        self.owners
            .lock()
            .map_err(|_| unavailable("factory owner list poisoned"))?
            .push(proxy.state.clone());
        let binding = SupervisedScopeV1::new(
            OwnedProviderId::new(NATIVE_PROVIDER_ID).map_err(unavailable)?,
            1,
            exact_scope.clone(),
            native_provider_limits(),
        )
        .map_err(unavailable)?
        .with_pinned_identity(
            Some(IMPLEMENTATION_IDENTITY_SHA256.into()),
            Some(STATE_SCHEMA_VERSION.into()),
        );
        let supervisor = ProviderSupervisorV1::new(
            NativeLifecycle {
                proxy: proxy.as_ref().clone(),
            },
            binding,
            RestartBudgetV1 {
                max_attempts_per_window: 32,
                window_micros: 120_000_000,
                backoff_base_micros: 1,
                backoff_max_micros: 1,
            },
            ShutdownBudgetV1 {
                grace_micros: 5_000_000,
                kill_micros: 5_000_000,
            },
        )
        .map_err(unavailable)?
        .with_quarantine_policy(QuarantinePolicyV1 {
            max_provider_violations: 1,
        })
        .map_err(unavailable)?;
        let mut fixture = NativeCommonFixture {
            proxy,
            supervisor,
            base_scope: exact_scope.clone(),
            current_scope: exact_scope.clone(),
            namespace: 0,
            root: Some(root),
            legacy_references: Vec::new(),
            cleanup: self.cleanup.clone(),
        };
        fixture.start().map_err(unavailable)?;
        Ok(fixture)
    }
    fn finish(&self, deadline: i64) -> Result<CleanupSnapshot, String> {
        let owners = self
            .owners
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        for owner in owners {
            let proxy = NativeProcessProxy {
                state: owner,
                probes: Arc::new(Mutex::new(ParentProbeState::default())),
            };
            if let Err(error) = proxy.force_cleanup(deadline) {
                self.cleanup.error("factory final owner recheck", error);
            }
        }
        let snapshot = self.cleanup.snapshot();
        if !snapshot.all_reaped() || !snapshot.fixture_failures.is_empty() {
            return Err(format!("Native final cleanup unconfirmed: {snapshot:#?}"));
        }
        Ok(snapshot)
    }
}
impl CommonAdvisoryFixtureFactory for NativeCommonFactory {
    fn create(
        &self,
        scenario: &CompatibilityScenario,
        exact_scope: &OwnedExactScope,
    ) -> Result<Box<dyn CommonAdvisoryFixture>, FixtureUnavailable> {
        self.create_owned(scenario, exact_scope)
            .map(|fixture| Box::new(fixture) as Box<dyn CommonAdvisoryFixture>)
    }
}
impl NativeCommonFixture {
    fn start(&mut self) -> Result<(), String> {
        let request = handshake_request(&self.base_scope, "native-fixture.start")?;
        let now = now_micros()?;
        let deadline = deadline_after(ENVIRONMENT_BUDGET)?;
        match self
            .supervisor
            .start_or_restart(&request, now, deadline, deadline)
        {
            SupervisorOutcomeV1::Ready(_) => Ok(()),
            SupervisorOutcomeV1::Unavailable(cause) => {
                Err(format!("real Native readiness failed: {cause}"))
            }
        }
    }
    fn stop(&mut self) -> Result<(), String> {
        let report = self.supervisor.shutdown(now_micros()?).map_err(failure)?;
        let state = self.proxy.lock(&WaitBound::environment())?;
        if !report.confirmed_dead || state.child.is_some() || state.starting_child.is_some() {
            return Err("Native shutdown did not reap the child".into());
        }
        Ok(())
    }
    fn process_identity(&self) -> Result<String, String> {
        self.proxy
            .lock(&WaitBound::environment())?
            .child
            .as_ref()
            .map(|child| child.identity.clone())
            .ok_or_else(|| "no live Native child".into())
    }
    fn provider_root(&self) -> Result<PathBuf, String> {
        Ok(self
            .proxy
            .lock(&WaitBound::environment())?
            .config
            .provider_root
            .clone())
    }
    fn record_authority(&self, response: ChildResponse) -> Result<Option<String>, String> {
        let ChildResponse::Authority {
            snapshot,
            reference,
        } = response
        else {
            return Err("missing actual authority update".into());
        };
        FixtureAuthority::from_snapshot(snapshot.clone())?;
        let mut state = self.proxy.lock(&WaitBound::environment())?;
        state.config.authority = snapshot;
        // Persist the actual fixture-owned authority snapshot independently of
        // provider state so neither rollback nor a fresh namespace revives it.
        let path = state.control_root.join("authority-current.json");
        fs::write(
            &path,
            serde_json::to_vec(&state.config.authority).map_err(failure)?,
        )
        .map_err(failure)?;
        File::open(&path)
            .map_err(failure)?
            .sync_all()
            .map_err(failure)?;
        Ok(reference)
    }
    /// These fixture probes cover actual registration, fabric admission and
    /// supervisor quarantine. Shipped Claude/Codex host-journey coverage belongs
    /// to the separate real CLI journey tests.
    fn mode(&mut self, mode: &str) -> Result<FixtureEnvironmentEvidence, String> {
        let before = self.proxy.descriptor_result()?;
        if mode == "quarantined" {
            let old_identity = self.process_identity()?;
            let launches = self.proxy.lock(&WaitBound::environment())?.launches;
            self.supervisor
                .adapter()
                .kill(deadline_after(Duration::from_secs(5))?)
                .map_err(failure)?;
            if self.proxy.lock(&WaitBound::environment())?.child.is_some() {
                return Err("crash was not physically reaped".into());
            }
            self.supervisor.report_crash();
            if !self.supervisor.is_quarantined() {
                return Err("witnessed crash did not engage real supervisor quarantine".into());
            }
            let request = handshake_request(&self.base_scope, "native-fixture.quarantined")?;
            let now = now_micros()?;
            let deadline = deadline_after(ENVIRONMENT_BUDGET)?;
            if !matches!(
                self.supervisor
                    .start_or_restart(&request, now, deadline, deadline),
                SupervisorOutcomeV1::Unavailable(DegradationCauseV1::Quarantined { .. })
            ) {
                return Err("quarantined supervisor did not refuse readiness".into());
            }
            let state = self.proxy.lock(&WaitBound::environment())?;
            if state.child.is_some() || state.launches != launches || old_identity.is_empty() {
                return Err("quarantine spawned a new child".into());
            }
            drop(state);
            return Ok(FixtureEnvironmentEvidence::ModeChanged {
                mode: mode.into(),
                selected_provider_unchanged: before == self.proxy.descriptor_result()?,
                recall_admitted: false,
                observation_admitted: false,
            });
        }
        if self.supervisor.is_quarantined() {
            self.stop()?;
            self.supervisor.release_quarantine().map_err(failure)?;
            self.start()?;
        }
        let selection = match mode {
            "disabled" => SelectedProviderActivationV1::Disabled,
            "observe" | "active" => SelectedProviderActivationV1::Injected {
                fabric_config: FabricConfig::new(1, 1).map_err(failure)?,
                registration: ProviderRegistrationV1 {
                    provider_id: before.provider_id.clone(),
                    provider: self.proxy.clone(),
                    registration_revision: 1,
                    mode: if mode == "observe" {
                        EnabledProviderMode::Observer
                    } else {
                        EnabledProviderMode::Active
                    },
                    execution_shape: ProviderExecutionShapeV1::HostAuthoredInProcess,
                    recall_scope_bindings: RecallScopeBindingsV1::from_wire(
                        NATIVE_RECALL_SCOPE_BINDINGS.iter().copied(),
                    )
                    .map_err(failure)?,
                    // Mode probes borrow the already-supervised test runtime;
                    // composition must not create an independent process owner.
                    lifecycle: ProviderLifecycleOwnershipV1::CompositionBound,
                },
            },
            _ => return Err("unknown Native fixture mode".into()),
        };
        let composition = ProjectMemoryProviderComposition::compose_registered(selection, vec![])
            .map_err(failure)?;
        let (recall_admitted, observation_admitted) = if let Some(registry) = composition.registry()
        {
            let readiness = registry
                .handshake(&handshake_request(
                    &self.current_scope,
                    "native-fixture.mode-readiness",
                )?)
                .map_err(failure)?;
            if readiness.terminal.terminal_code() != TerminalCode::Success {
                return Err("mode probe did not have real readiness".into());
            }
            let generation = readiness
                .descriptor
                .as_ref()
                .ok_or("mode probe descriptor missing")?
                .state_generation;
            let receipt = readiness
                .ready_receipt_sha256
                .as_deref()
                .ok_or("mode probe receipt missing")?;
            // Deliberately malformed *new environment probes* prove that the
            // fabric admitted dispatch without adding provider state. Ordinary
            // suite requests and replies remain untouched.
            let probe = |operation: ProviderOperation| -> Result<ProviderCall, String> {
                let contract = if operation == ProviderOperation::Recall {
                    "tracedecay.memory.provider.recall.v1"
                } else {
                    "tracedecay.memory.provider.observation.v1"
                };
                let bytes = b"{}".to_vec();
                ProviderCall::new(ProviderCallParts {
                    operation,
                    provider_id: before.provider_id.clone(),
                    registration_revision: 1,
                    ready_receipt_sha256: receipt.into(),
                    exact_scope: self.current_scope.clone(),
                    request_id: format!("native-mode-probe-{}", operation.as_wire()),
                    operation_id: format!("native-mode-probe-{}", operation.as_wire()),
                    expected_state_generation: generation,
                    idempotency_key: operation
                        .mutates_provider_state()
                        .then(|| sha256_hex(b"native-fixture-mode-probe")),
                    control: OperationControl::new(
                        deadline_after(ENVIRONMENT_BUDGET)?,
                        30_000,
                        CancellationToken::new(),
                    ),
                    payload: CanonicalPayload::new(
                        OwnedVersionedId::new(contract).map_err(failure)?,
                        bytes.clone(),
                        sha256_hex(&bytes),
                    )
                    .map_err(failure)?,
                    required_capabilities: vec![
                        OwnedVersionedId::new(operation.capability_id()).map_err(failure)?,
                    ],
                    extensions: vec![],
                })
                .map_err(failure)
            };
            let recall = match registry.invoke_active(&probe(ProviderOperation::Recall)?) {
                Ok(reply)
                    if reply.terminal.committed_effect().state() == CommittedEffectState::None
                        && reply.terminal.terminal_code() == TerminalCode::InvalidRequest =>
                {
                    true
                }
                Err(FabricError::ProviderObserverOnly(_)) if mode == "observe" => false,
                result => return Err(format!("unexpected real recall mode admission: {result:?}")),
            };
            let observation = match registry
                .deliver_observation_result(&probe(ProviderOperation::Observe)?)
            {
                Ok(tracedecay_memory_provider_registry::ObserverDeliveryResult::Accepted(
                    receipt,
                )) if receipt.terminal.committed_effect().state() == CommittedEffectState::None
                    && receipt.terminal.terminal_code() == TerminalCode::InvalidRequest =>
                {
                    true
                }
                result => {
                    return Err(format!(
                        "unexpected real observation mode admission: {result:?}"
                    ));
                }
            };
            (recall, observation)
        } else {
            (false, false)
        };
        let after = self.proxy.descriptor_result()?;
        if after.state_generation != before.state_generation {
            return Err("effect-free mode probes changed Native state".into());
        }
        Ok(FixtureEnvironmentEvidence::ModeChanged {
            mode: mode.into(),
            selected_provider_unchanged: before.provider_id == after.provider_id
                && before.implementation_identity_sha256 == after.implementation_identity_sha256,
            recall_admitted,
            observation_admitted,
        })
    }
}
impl CommonAdvisoryFixture for NativeCommonFixture {
    fn provider(&self) -> &dyn MemoryProviderV1 {
        self.proxy.as_ref()
    }
    fn apply_environment(
        &mut self,
        action: &FixtureEnvironmentAction,
    ) -> Result<FixtureEnvironmentEvidence, FixtureUnavailable> {
        self.apply(action).map_err(unavailable)
    }
}
impl NativeCommonFixture {
    fn apply(
        &mut self,
        action: &FixtureEnvironmentAction,
    ) -> Result<FixtureEnvironmentEvidence, String> {
        match action {
            FixtureEnvironmentAction::Restart => {
                let previous_process = {
                    let state = self.proxy.lock(&WaitBound::environment())?;
                    state
                        .child
                        .as_ref()
                        .map(|child| child.identity.clone())
                        .or_else(|| state.last_exited_process.clone())
                        .ok_or("restart has no prior owned process")?
                };
                let previous_namespace = self.provider_root()?;
                self.proxy.descriptor_result()?;
                self.start()?;
                let current_process = self.process_identity()?;
                if current_process == previous_process
                    || self
                        .proxy
                        .lock(&WaitBound::environment())?
                        .last_exited_process
                        .as_ref()
                        != Some(&previous_process)
                {
                    return Err("restart did not replace and reap the prior process".into());
                }
                Ok(FixtureEnvironmentEvidence::Restarted {
                    previous_process,
                    current_process,
                    previous_process_exited: true,
                    reopened_persisted_namespace: previous_namespace == self.provider_root()?,
                })
            }
            FixtureEnvironmentAction::FreshNamespace => {
                let previous_namespace = self.provider_root()?;
                self.proxy.descriptor_result()?;
                self.stop()?;
                self.namespace = self
                    .namespace
                    .checked_add(1)
                    .ok_or("namespace sequence overflow")?;
                let current_namespace = self
                    .root
                    .as_ref()
                    .ok_or("Native fixture directory is absent")?
                    .path()
                    .join(format!("namespace-{}", self.namespace));
                fs::create_dir(&current_namespace).map_err(failure)?;
                self.proxy
                    .lock(&WaitBound::environment())?
                    .config
                    .provider_root = current_namespace.clone();
                self.start()?;
                Ok(FixtureEnvironmentEvidence::FreshNamespace {
                    previous_namespace: previous_namespace.display().to_string(),
                    current_namespace: current_namespace.display().to_string(),
                    current_dispositions_revalidated: true,
                })
            }
            FixtureEnvironmentAction::OpenSession { destination_scope } => {
                destination_scope.validate().map_err(failure)?;
                let before = self.proxy.descriptor_result()?;
                // Native accepts a per-call exact session; the independently
                // installed fixture authority still decides historical access.
                self.current_scope = destination_scope.clone();
                let after = self.proxy.descriptor_result()?;
                Ok(FixtureEnvironmentEvidence::SessionOpened {
                    destination_scope: self.current_scope.clone(),
                    selected_provider_unchanged: before.provider_id == after.provider_id
                        && before.implementation_identity_sha256
                            == after.implementation_identity_sha256,
                })
            }
            FixtureEnvironmentAction::InstallLegacyV1 { observations } => {
                self.proxy.descriptor_result()?;
                self.stop()?;
                let records =
                    install_legacy(&self.provider_root()?, &self.current_scope, observations)?;
                self.legacy_references = records
                    .iter()
                    .map(|record| record.stable_memory_ref.clone())
                    .collect();
                // The next Restart action, not this writer, opens/migrates v1.
                Ok(FixtureEnvironmentEvidence::LegacyV1Installed {
                    stored_version: 1,
                    source_count: u64::try_from(records.len()).map_err(failure)?,
                    records,
                })
            }
            FixtureEnvironmentAction::InspectLegacyDurableState { .. } => {
                // Point-read the owned namespace while the restarted child remains live.
                self.proxy.descriptor_result()?;
                if self.legacy_references.is_empty() || self.legacy_references.len() > 64 {
                    return Err("legacy physical audit has no bounded stored references".into());
                }
                let connection = Connection::open_with_flags(
                    staged_store_path(&self.provider_root()?),
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                )
                .map_err(failure)?;
                connection
                    .busy_timeout(Duration::from_millis(250))
                    .map_err(failure)?;
                let mut statement = connection.prepare(&format!("SELECT {LEGACY_COLUMNS} FROM tdmem_native_staged_observation_v1 WHERE provider_reference=?1 LIMIT 2")).map_err(failure)?;
                let mut records = Vec::new();
                for reference in &self.legacy_references {
                    let rows = statement
                        .query_map([reference], native_legacy_durable_record)
                        .map_err(failure)?;
                    for record in rows {
                        records.push(record.map_err(failure)?);
                    }
                }
                Ok(FixtureEnvironmentEvidence::LegacyDurableState { records })
            }
            FixtureEnvironmentAction::CorruptPayloadPreservingDigest => {
                self.proxy.descriptor_result()?;
                self.stop()?;
                corrupt_payload(&self.provider_root()?)
            }
            FixtureEnvironmentAction::RecordSourceDisposition {
                source_key,
                disposition,
            } => {
                let response = self.proxy.request(
                    ChildRequest::RecordDisposition {
                        source_key: source_key.clone(),
                        disposition: disposition.clone(),
                    },
                    None,
                )?;
                let authority_ref = self
                    .record_authority(response)?
                    .ok_or("disposition update omitted journal reference")?;
                Ok(FixtureEnvironmentEvidence::SourceDispositionRecorded {
                    source_key: source_key.clone(),
                    disposition: disposition.clone(),
                    authority_ref,
                })
            }
            FixtureEnvironmentAction::SetAdmissionAuthorityAvailable { available } => {
                let response = self
                    .proxy
                    .request(ChildRequest::SetAuthorityAvailable(*available), None)?;
                self.record_authority(response)?;
                Ok(FixtureEnvironmentEvidence::AuthorityAvailability {
                    available: *available,
                })
            }
            FixtureEnvironmentAction::LoseNextReplyAfterCommit => {
                if !matches!(
                    self.proxy.request(ChildRequest::LoseNextReply, None)?,
                    ChildResponse::LostReplyArmed
                ) {
                    return Err("Native commit-reply hook was not armed".into());
                }
                Ok(FixtureEnvironmentEvidence::LostReplyArmed)
            }
            FixtureEnvironmentAction::SetMode { mode } => self.mode(mode),
            FixtureEnvironmentAction::Shutdown => {
                self.proxy.descriptor_result()?;
                self.stop()?;
                Ok(FixtureEnvironmentEvidence::Shutdown {
                    remaining_workers: 0,
                })
            }
        }
    }
}
impl NativeCommonFixture {
    fn arm_probe(&self, kind: ProbeKind, operation_id: &str) -> Result<(), String> {
        let control_root = self
            .proxy
            .lock(&WaitBound::environment())?
            .control_root
            .clone();
        let probe = TransportProbe {
            kind,
            operation_id: operation_id.into(),
            witness_path: control_root.join("transport-probe-witness.json"),
            release_path: control_root.join("transport-probe-release"),
        };
        if !matches!(
            self.proxy
                .request(ChildRequest::ArmProbe(probe.clone()), None)?,
            ChildResponse::ProbeArmed
        ) {
            return Err("child did not arm the real transport probe".into());
        }
        self.proxy
            .probes
            .lock()
            .map_err(|_| "parent probe state poisoned")?
            .pending = Some(probe);
        Ok(())
    }
}
impl Drop for NativeCommonFixture {
    fn drop(&mut self) {
        if let Err(error) = self.proxy.collect_probe_evidence() {
            let mut probes = self
                .proxy
                .probes
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if let Some(last) = probes.last.as_mut() {
                last.failures.push(error);
            }
        }
        // Graceful cleanup is provisional. Escalation and final collection
        // inspect the actual retained owner even after a poisoned RPC mutex.
        let graceful = self.stop();
        let settled = match graceful {
            Ok(()) => Ok(()),
            Err(error) => {
                let launch = self
                    .proxy
                    .state
                    .try_lock()
                    .map(|state| state.config.launch_identity.clone())
                    .unwrap_or_else(|_| "fixture graceful cleanup".into());
                self.cleanup.error(&launch, error);
                deadline_after(CLEANUP_BUDGET)
                    .and_then(|deadline| self.proxy.force_cleanup(deadline))
            }
        };
        if let Err(error) = settled {
            self.cleanup.error("fixture forced cleanup", error);
        }
        // The common runner drops scenario fixtures before validating its
        // report. Keep nested evidence until the outer test has confirmed both
        // behavioral success and child cleanup; the outer root owns deletion.
        if let Some(root) = self.root.take() {
            self.cleanup.preserve(root.keep());
        }
    }
}

#[derive(Default)]
struct CancellationBridge {
    active: Option<(u64, CancellationToken)>,
    pending: Option<u64>,
}
impl CancellationBridge {
    fn start(&mut self, id: u64) -> CancellationToken {
        let token = CancellationToken::new();
        if self.pending == Some(id) {
            token.cancel();
            self.pending = None;
        }
        self.active = Some((id, token.clone()));
        token
    }
    fn cancel(&mut self, id: u64) {
        if let Some((active, token)) = &self.active {
            if *active == id {
                token.cancel();
                return;
            }
            if *active > id {
                return;
            }
        }
        self.pending = Some(id);
    }
}

#[derive(Default)]
struct ChildProbeState {
    config: Option<TransportProbe>,
    witness: Option<ProbeWitness>,
}
impl ChildProbeState {
    fn arm(&mut self, config: TransportProbe) -> Result<(), String> {
        if self.config.is_some() || config.operation_id.is_empty() {
            return Err("child transport probe was already armed or invalid".into());
        }
        self.config = Some(config);
        Ok(())
    }
    fn invoked(&mut self, id: u64, call: &ProviderCall) -> Result<(), String> {
        if !self
            .config
            .as_ref()
            .is_some_and(|probe| probe.operation_id == call.operation_id)
        {
            return Ok(());
        }
        let count = self
            .witness
            .as_ref()
            .map_or(0, |witness| witness.provider_invocations)
            .checked_add(1)
            .ok_or("probe invocation count overflow")?;
        self.witness = Some(ProbeWitness {
            pid: std::process::id(),
            rpc_id: id,
            operation_id: call.operation_id.clone(),
            original_deadline_utc_micros: call.control.deadline_utc_micros(),
            original_remaining_millis: call.control.remaining_millis(),
            provider_invocations: count,
            authority_entries: 0,
            terminal_code: None,
        });
        Ok(())
    }
    fn completed(
        &mut self,
        call: &ProviderCall,
        reply: &ProviderReply,
    ) -> Result<Option<TransportProbe>, String> {
        let Some(config) = self
            .config
            .as_ref()
            .filter(|probe| probe.operation_id == call.operation_id)
        else {
            return Ok(None);
        };
        let witness = self
            .witness
            .as_mut()
            .ok_or("probe completed without invocation")?;
        witness.terminal_code = Some(reply.terminal.terminal_code().as_wire().into());
        if config.kind == ProbeKind::SplitResponsePrefix {
            write_probe_witness(&config.witness_path, witness)?;
            Ok(Some(config.clone()))
        } else {
            Ok(None)
        }
    }
}
fn write_probe_witness(path: &Path, witness: &ProbeWitness) -> Result<(), String> {
    let bytes = serde_json::to_vec(witness).map_err(failure)?;
    if bytes.len() > PROBE_WITNESS_MAXIMUM {
        return Err("child probe witness exceeded bound".into());
    }
    let pending = path.with_extension("pending");
    fs::write(&pending, bytes).map_err(failure)?;
    fs::rename(pending, path).map_err(failure)
}
struct ProbedAuthority {
    actual: FixtureAuthority,
    probe: Arc<Mutex<ChildProbeState>>,
}
impl AdvisoryAdmissionAuthority for ProbedAuthority {
    fn admit(
        &self,
        call: &ProviderCall,
    ) -> Result<CurrentAdvisoryAdmission, AdvisoryAdmissionError> {
        let gate = {
            let mut probe = self
                .probe
                .lock()
                .map_err(|_| AdvisoryAdmissionError::Unavailable("probe authority lock"))?;
            let config = probe
                .config
                .as_ref()
                .filter(|config| {
                    config.kind == ProbeKind::CancelInAuthority
                        && config.operation_id == call.operation_id
                })
                .cloned();
            if let Some(config) = config {
                let witness = probe
                    .witness
                    .as_mut()
                    .ok_or(AdvisoryAdmissionError::Unavailable(
                        "probe invocation witness",
                    ))?;
                witness.authority_entries = witness
                    .authority_entries
                    .checked_add(1)
                    .ok_or(AdvisoryAdmissionError::Unavailable("probe authority count"))?;
                write_probe_witness(&config.witness_path, witness)
                    .map_err(|_| AdvisoryAdmissionError::Unavailable("probe authority witness"))?;
                true
            } else {
                false
            }
        };
        if gate {
            // This gate exists only in the explicitly armed cancellation test.
            // Native's actor has entered the real admission port; the same
            // original child control determines when the delegate resumes.
            while call.control.snapshot().is_ok() {
                thread::sleep(POLL);
            }
        }
        // No admission or provider terminal is synthesized by the probe.
        self.actual.admit(call)
    }
}
fn write_response(
    stream: &mut TcpStream,
    response: &ResponseFrame,
    split: Option<&TransportProbe>,
) -> Result<(), String> {
    let Some(probe) = split else {
        return write_frame(stream, response, &WaitBound::environment());
    };
    let mut bytes = encoded_frame(response)?;
    let bound = WaitBound::cleanup();
    transfer(|bytes| stream.write(bytes), &mut bytes[..2], &bound)?;
    while !probe.release_path.exists() {
        bound.check()?;
        thread::sleep(POLL);
    }
    transfer(|bytes| stream.write(bytes), &mut bytes[2..], &bound)
}

/// Self-exec harness entry only. Broad ignored-test runs without the launch
/// variable do not count this entry as behavioral evidence.
#[test]
#[ignore = "owned Native common-suite process entry"]
fn native_common_provider_child() {
    let Some(path) = std::env::var_os(CHILD_CONFIG_ENV) else {
        return;
    };
    run_child(Path::new(&path)).expect("real Native test-child runtime");
}
fn run_child(path: &Path) -> Result<(), String> {
    if fs::metadata(path).map_err(failure)?.len() > u64::try_from(FRAME_MAXIMUM).map_err(failure)? {
        return Err("fixture child config exceeded bound".into());
    }
    let config: ChildConfig =
        serde_json::from_slice(&fs::read(path).map_err(failure)?).map_err(failure)?;
    let entry_unix_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(failure)?
        .as_nanos();
    let connect = |channel: &str| -> Result<TcpStream, String> {
        let mut stream = TcpStream::connect(&config.address).map_err(failure)?;
        stream.set_nonblocking(true).map_err(failure)?;
        write_frame(
            &mut stream,
            &ChildHello {
                pid: std::process::id(),
                entry_unix_nanos,
                launch_identity: config.launch_identity.clone(),
                channel: channel.into(),
            },
            &WaitBound::environment(),
        )?;
        Ok(stream)
    };
    let mut rpc = connect("rpc")?;
    let cancellation = connect("cancellation")?;
    let cancellation_shutdown = cancellation.try_clone().map_err(failure)?;
    let bridge = Arc::new(Mutex::new(CancellationBridge::default()));
    let stopping = Arc::new(AtomicBool::new(false));
    let cancellation_worker = {
        let bridge = bridge.clone();
        let stopping = stopping.clone();
        thread::spawn(move || {
            let mut stream = cancellation;
            let mut frame = [0; 8];
            let mut offset = 0;
            while !stopping.load(Ordering::Acquire) {
                match stream.read(&mut frame[offset..]) {
                    Ok(0) => break,
                    Ok(count) => {
                        offset += count;
                        if offset == frame.len() {
                            bridge
                                .lock()
                                .expect("child cancellation lock")
                                .cancel(u64::from_be_bytes(frame));
                            offset = 0;
                        }
                    }
                    Err(error)
                        if matches!(
                            error.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                        ) =>
                    {
                        thread::sleep(POLL)
                    }
                    Err(_) => break,
                }
            }
        })
    };
    let mut active_request_id = None;
    let result: Result<(), String> = (|| {
        let authority = FixtureAuthority::from_snapshot(config.authority.clone())?;
        let probe = Arc::new(Mutex::new(ChildProbeState::default()));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(failure)?;
        let graph = Arc::new(
            runtime
                .block_on(TraceDecay::init_with_options(
                    &config.project_root,
                    TraceDecayOpenOptions {
                        global_db_path: Some(config.profile_root.join("global.db")),
                        profile_root: Some(config.profile_root.clone()),
                    },
                ))
                .map_err(failure)?,
        );
        let exact_scope = scope(&config.exact_scope)?;
        let port = Arc::new(
            ProjectNativeMemoryApplicationPort::new(
                Arc::new(tokio::sync::RwLock::new(graph.clone())),
                config.project_root.clone(),
                UserProfileId::new(exact_scope.profile_id).map_err(failure)?,
                &config.provider_root,
            )
            .map_err(failure)?
            .with_admission_authority(Arc::new(ProbedAuthority {
                actual: authority.clone(),
                probe: probe.clone(),
            })),
        );
        let provider = NativeProvider::new(port.clone()).map_err(failure)?;
        write_frame(
            &mut rpc,
            &ChildResponse::Ready {
                authority: authority.snapshot()?,
            },
            &WaitBound::environment(),
        )?;
        let mut last_request_id = 0_u64;
        let shutdown_request_id = loop {
            let incoming: IncomingRpc = read_frame(&mut rpc, &WaitBound::environment())?;
            let received = Instant::now();
            let id = incoming.request_id;
            if id
                != last_request_id
                    .checked_add(1)
                    .ok_or("child RPC sequence overflow")?
            {
                return Err("child received an out-of-order or duplicate RPC request".into());
            }
            last_request_id = id;
            active_request_id = Some(id);
            let mut split = None;
            let response = match incoming.request {
                ChildRequest::Descriptor => {
                    ChildResponse::Descriptor(DescriptorDto::capture(&provider.descriptor())?)
                }
                ChildRequest::Handshake { value } => {
                    let token = bridge
                        .lock()
                        .map_err(|_| "child cancellation lock poisoned")?
                        .start(id);
                    let request = value.restore(token, received.elapsed())?;
                    let response = provider.handshake(&request);
                    bridge
                        .lock()
                        .map_err(|_| "child cancellation lock poisoned")?
                        .active = None;
                    ChildResponse::Handshake(HandshakeResponseDto::capture(&response)?)
                }
                ChildRequest::Invoke { value } => {
                    let token = bridge
                        .lock()
                        .map_err(|_| "child cancellation lock poisoned")?
                        .start(id);
                    let call = value.restore(token, received.elapsed())?;
                    probe
                        .lock()
                        .map_err(|_| "child probe lock poisoned")?
                        .invoked(id, &call)?;
                    let response = provider.invoke(&call);
                    bridge
                        .lock()
                        .map_err(|_| "child cancellation lock poisoned")?
                        .active = None;
                    split = probe
                        .lock()
                        .map_err(|_| "child probe lock poisoned")?
                        .completed(&call, &response)?;
                    ChildResponse::Reply(ProviderReplyDto::capture(&response)?)
                }
                ChildRequest::RecordDisposition {
                    source_key,
                    disposition,
                } => {
                    let disposition = SourceDisposition::from_wire(&disposition)
                        .ok_or("invalid fixture disposition")?;
                    let reference = authority.record_disposition(&source_key, disposition)?;
                    ChildResponse::Authority {
                        snapshot: authority.snapshot()?,
                        reference: Some(reference),
                    }
                }
                ChildRequest::SetAuthorityAvailable(available) => {
                    authority.set_available(available)?;
                    ChildResponse::Authority {
                        snapshot: authority.snapshot()?,
                        reference: None,
                    }
                }
                ChildRequest::LoseNextReply => {
                    port.staged_store().lose_next_reply();
                    ChildResponse::LostReplyArmed
                }
                ChildRequest::ArmProbe(config) => {
                    probe
                        .lock()
                        .map_err(|_| "child probe lock poisoned")?
                        .arm(config)?;
                    ChildResponse::ProbeArmed
                }
                ChildRequest::ProbeEvidence => ChildResponse::ProbeEvidence(
                    probe
                        .lock()
                        .map_err(|_| "child probe lock poisoned")?
                        .witness
                        .clone()
                        .ok_or("probe never reached actual Native invoke")?,
                ),
                ChildRequest::Shutdown => break id,
            };
            write_response(
                &mut rpc,
                &ResponseFrame {
                    request_id: id,
                    response,
                },
                split.as_ref(),
            )?;
            active_request_id = None;
        };
        drop(provider);
        drop(port); // The production actor Drop joins its thread.
        drop(graph);
        runtime.shutdown_timeout(Duration::from_secs(5));
        write_frame(
            &mut rpc,
            &ResponseFrame {
                request_id: shutdown_request_id,
                response: ChildResponse::Stopped,
            },
            &WaitBound::environment(),
        )?;
        active_request_id = None;
        Ok(())
    })();
    if let Err(error) = &result {
        if let Some(request_id) = active_request_id {
            let _ = write_frame(
                &mut rpc,
                &ResponseFrame {
                    request_id,
                    response: ChildResponse::Error(error.clone()),
                },
                &WaitBound::cleanup(),
            );
        } else {
            let _ = write_frame(
                &mut rpc,
                &ChildResponse::Error(error.clone()),
                &WaitBound::cleanup(),
            );
        }
    }
    stopping.store(true, Ordering::Release);
    cancellation_shutdown.shutdown(Shutdown::Both).ok();
    cancellation_worker
        .join()
        .map_err(|_| "child cancellation worker panicked")?;
    result
}

// This is the original 24-column v1 table definition also used by the existing
// native_staged_observations::tests::downgrade_fixture_to_real_v1 regression.
// The populated v2 writer supplies real receipt/reference derivation; copying
// only these columns produces genuine old storage before migration opens it.
const LEGACY_TABLE_DDL: &str = "
CREATE TABLE tdmem_native_staged_observation_v1 (
 exact_scope_sha256 TEXT NOT NULL, idempotency_key TEXT NOT NULL,
 profile_id TEXT NOT NULL, project_id TEXT NOT NULL, repository_identity TEXT NOT NULL,
 worktree_identity TEXT NOT NULL, branch_identity TEXT NOT NULL, agent_session_id TEXT NOT NULL,
 resolved_scope_digest TEXT NOT NULL, source_authority TEXT NOT NULL, source_event_id TEXT NOT NULL,
 source_revision INTEGER NOT NULL CHECK(source_revision >= 0), observation_kind TEXT NOT NULL,
 payload_contract TEXT NOT NULL, sanitized_payload BLOB, payload_sha256 TEXT NOT NULL,
 operation_id TEXT NOT NULL, request_identity TEXT NOT NULL, provider_reference TEXT NOT NULL,
 receipt TEXT NOT NULL, effect_digest TEXT NOT NULL,
 admitted_sequence INTEGER NOT NULL CHECK(admitted_sequence > 0), admitted_at_unix_ms INTEGER NOT NULL,
 tombstone INTEGER NOT NULL CHECK(tombstone IN (0,1)),
 CHECK((sanitized_payload IS NULL) = (tombstone = 1)),
 PRIMARY KEY(exact_scope_sha256,idempotency_key)
) STRICT;";
const LEGACY_COLUMNS: &str = "exact_scope_sha256,idempotency_key,profile_id,project_id,repository_identity,worktree_identity,branch_identity,agent_session_id,resolved_scope_digest,source_authority,source_event_id,source_revision,observation_kind,payload_contract,sanitized_payload,payload_sha256,operation_id,request_identity,provider_reference,receipt,effect_digest,admitted_sequence,admitted_at_unix_ms,tombstone";
fn install_legacy(
    root: &Path,
    exact_scope: &OwnedExactScope,
    observations: &[Value],
) -> Result<Vec<LegacyRecordEvidence>, String> {
    if observations.is_empty() || observations.len() > 64 {
        return Err("legacy writer source bound".into());
    }
    let staged = StagedObservationStore::open(root).map_err(failure)?;
    for (index, observation) in observations.iter().enumerate() {
        let event = observation["observation_id"]
            .as_str()
            .ok_or("legacy source identity missing")?;
        let content = observation["canonical_payload"]["content"]
            .as_str()
            .ok_or("legacy message content missing")?;
        // A v1 payload had no common original_source. Envelope version 1 and
        // its old numeric source revision do not become opaque source revisions.
        let payload = json!({
            "observation_kind": "session.message_committed.v1",
            "payload_contract": "tracedecay.memory.observation.session-message.v1",
            "canonical_payload": { "version": 1, "stable_record_id": event,
                "facts": [{ "kind": "message", "role": "user", "content": { "text": content } }] },
        });
        let offered = StagedObservationRecord {
            scope: exact_scope.clone(),
            idempotency_key: sha256_hex(format!("native-legacy-delivery-{index}").as_bytes()),
            source_authority: "host_session".into(),
            source_event_id: event.into(),
            source_revision: Some("99".into()),
            observation_kind: "session.message_committed.v1".into(),
            payload_contract: "tracedecay.memory.observation.session-message.v1".into(),
            sanitized_payload: serde_json::to_vec(&payload).map_err(failure)?,
            operation_id: format!("native-legacy-operation-{index}"),
            request_identity: format!("native-legacy-request-{index}"),
            admitted_at_unix_ms: now_micros()? / 1000,
        };
        if !matches!(
            staged.stage_or_duplicate(offered).map_err(failure)?,
            StagedOutcome::Committed(_)
        ) {
            return Err("legacy writer did not commit a fresh row".into());
        }
    }
    let path = staged.path().to_path_buf();
    drop(staged);
    let mut connection = Connection::open(&path).map_err(failure)?;
    let transaction = connection.transaction().map_err(failure)?;
    transaction
        .execute_batch("ALTER TABLE tdmem_native_staged_observation_v1 RENAME TO fixture_v2_copy;")
        .map_err(failure)?;
    transaction
        .execute_batch(LEGACY_TABLE_DDL)
        .map_err(failure)?;
    transaction.execute(&format!("INSERT INTO tdmem_native_staged_observation_v1 ({LEGACY_COLUMNS}) SELECT {LEGACY_COLUMNS} FROM fixture_v2_copy"), []).map_err(failure)?;
    transaction.execute_batch("DROP TABLE fixture_v2_copy;
        DROP TABLE tdmem_native_state_v2; DROP TABLE tdmem_native_operation_v2;
        DROP TABLE tdmem_native_deleted_source_v2; DROP TABLE tdmem_native_replay_v2;
        UPDATE tdmem_native_staged_observation_v1 SET source_revision=99;
        CREATE UNIQUE INDEX tdmem_native_staged_observation_sequence_v1 ON tdmem_native_staged_observation_v1(admitted_sequence);
        CREATE INDEX tdmem_native_staged_observation_recall_v1 ON tdmem_native_staged_observation_v1(exact_scope_sha256,tombstone,admitted_sequence);
        CREATE INDEX tdmem_native_staged_observation_checkout_recall_v1 ON tdmem_native_staged_observation_v1(profile_id,project_id,repository_identity,worktree_identity,branch_identity,tombstone,admitted_sequence);
        PRAGMA user_version=1;").map_err(failure)?;
    transaction.commit().map_err(failure)?;
    let version = connection
        .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
        .map_err(failure)?;
    let columns = connection
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('tdmem_native_staged_observation_v1')",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(failure)?;
    if version != 1 || columns != 24 {
        return Err("legacy writer did not produce genuine v1 storage".into());
    }
    let mut statement = connection.prepare(&format!("SELECT {LEGACY_COLUMNS} FROM tdmem_native_staged_observation_v1 ORDER BY admitted_sequence")).map_err(failure)?;
    let retained = statement
        .query_map([], native_legacy_durable_record)
        .map_err(failure)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(failure)?;
    let records = retained
        .into_iter()
        .map(|record| {
            Ok(LegacyRecordEvidence {
                stable_memory_ref: record.stable_memory_ref,
                original_operation_id: LegacyIdentityEvidence::Retained {
                    value: record
                        .retained_operation_id
                        .ok_or("legacy stored operation missing")?,
                },
                original_idempotency_key: LegacyIdentityEvidence::Retained {
                    value: record
                        .retained_idempotency_key
                        .ok_or("legacy stored key missing")?,
                },
                original_receipt_sha256: record.original_receipt_sha256,
                immutable_record_sha256: record.immutable_record_sha256,
                stored_receipt_bytes_sha256: record.stored_receipt_bytes_sha256,
                retained_source_fields: BTreeMap::new(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    if records.len() != observations.len() {
        return Err("legacy writer lost a source".into());
    }
    Ok(records)
}
// Exact 24-column v1 projection in LEGACY_COLUMNS order. Each SQLite value
// includes its type and exact bytes; added v2 attribution, feedback, validity,
// projected digest and global generation tables are outside this immutable audit.
fn native_legacy_durable_record(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<LegacyDurableRecordEvidence> {
    use rusqlite::types::ValueRef;
    let mut projection = Vec::new();
    for index in 0..24 {
        let value = match row.get_ref(index)? {
            ValueRef::Null => json!(["null"]),
            ValueRef::Integer(value) => json!(["integer", value]),
            ValueRef::Real(value) => json!(["real_bits", value.to_bits()]),
            ValueRef::Text(bytes) => json!(["text", bytes]),
            ValueRef::Blob(bytes) => json!(["blob", bytes]),
        };
        projection.push(value);
    }
    let receipt_bytes = match row.get_ref(19)? {
        ValueRef::Text(bytes) | ValueRef::Blob(bytes) => bytes,
        _ => &[],
    };
    Ok(LegacyDurableRecordEvidence {
        stable_memory_ref: row.get::<_, String>(18).unwrap_or_default(),
        immutable_record_sha256: sha256_hex(
            &serde_json::to_vec(&projection).expect("SQLite value projection is serializable"),
        ),
        stored_receipt_bytes_sha256: sha256_hex(receipt_bytes),
        original_receipt_sha256: row.get::<_, String>(19).unwrap_or_default(),
        retained_operation_id: row.get(16).ok(),
        retained_idempotency_key: row.get(1).ok(),
    })
}
fn corrupt_payload(root: &Path) -> Result<FixtureEnvironmentEvidence, String> {
    let mut connection = Connection::open(staged_store_path(root)).map_err(failure)?;
    let transaction = connection.transaction().map_err(failure)?;
    let (reference, before, before_claimed): (String, Vec<u8>, String) = transaction.query_row(
        "SELECT provider_reference,sanitized_payload,payload_sha256 FROM tdmem_native_staged_observation_v1 WHERE tombstone=0 ORDER BY admitted_sequence LIMIT 1",
        [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ).map_err(failure)?;
    if before.is_empty() {
        return Err("corruption requires a populated physical row".into());
    }
    // Legal JSON whitespace changes the physical payload while retaining every
    // source value. Integrity must reject it against the unchanged claimed hash.
    let mut after = before.clone();
    after.push(b' ');
    let changed = transaction.execute("UPDATE tdmem_native_staged_observation_v1 SET sanitized_payload=?1 WHERE provider_reference=?2", params![after, reference]).map_err(failure)?;
    if changed != 1 {
        return Err("corruption did not change exactly one row".into());
    }
    let (stored_after, after_claimed): (Vec<u8>, String) = transaction.query_row("SELECT sanitized_payload,payload_sha256 FROM tdmem_native_staged_observation_v1 WHERE provider_reference=?1", params![reference], |row| Ok((row.get(0)?, row.get(1)?))).map_err(failure)?;
    transaction.commit().map_err(failure)?;
    Ok(FixtureEnvironmentEvidence::PayloadCorrupted {
        before_actual_sha256: sha256_hex(&before),
        after_actual_sha256: sha256_hex(&stored_after),
        before_claimed_sha256: before_claimed,
        after_claimed_sha256: after_claimed,
    })
}

struct NativeTestEnvironment {
    root: Option<tempfile::TempDir>,
    factory: NativeCommonFactory,
    exact_scope: OwnedExactScope,
    finished: bool,
    behavior_passed: bool,
}
impl NativeTestEnvironment {
    fn new() -> Self {
        let temporary = tempfile::tempdir().expect("Native common canonical fixture");
        let project_root = temporary.path().join("canonical-project");
        let profile_root = temporary.path().join("canonical-profile");
        let scenario_root = temporary.path().join("provider-scenarios");
        for path in [&project_root, &profile_root, &scenario_root] {
            fs::create_dir_all(path).expect("fixture directory");
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("canonical initialization runtime");
        let graph = runtime
            .block_on(TraceDecay::init_with_options(
                &project_root,
                TraceDecayOpenOptions {
                    global_db_path: Some(profile_root.join("global.db")),
                    profile_root: Some(profile_root.clone()),
                },
            ))
            .expect("initialize real canonical owner");
        let FactOwnerV1::Project { project_id } =
            graph.project_memory_owner().expect("canonical owner")
        else {
            panic!("expected project owner");
        };
        let exact_scope = OwnedExactScope::new(
            "profile.native-common-factory",
            project_id.as_str(),
            "repository.native-common-factory",
            "worktree.native-common-factory",
            "branch.native-common-factory",
            "session.native-common-factory",
            format!("sha256:{}", "9".repeat(64)),
        )
        .expect("real-owner common scope");
        drop(graph);
        runtime.shutdown_timeout(Duration::from_secs(5));
        let factory = NativeCommonFactory {
            project_root,
            profile_root,
            scenario_root,
            sequence: AtomicU64::new(0),
            cleanup: CleanupLedger::default(),
            owners: Mutex::new(Vec::new()),
        };
        Self {
            root: Some(temporary),
            factory,
            exact_scope,
            finished: false,
            behavior_passed: false,
        }
    }
    fn finish(&mut self) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        let result = self.factory.finish(deadline_after(ENVIRONMENT_BUDGET)?);
        let snapshot = self.factory.cleanup.snapshot();
        let root = self.root.as_ref().ok_or("Native test root is absent")?;
        fs::write(
            root.path().join("cleanup.json"),
            serde_json::to_vec_pretty(&snapshot).map_err(failure)?,
        )
        .map_err(failure)?;
        result?;
        self.finished = true;
        Ok(())
    }
}
impl Drop for NativeTestEnvironment {
    fn drop(&mut self) {
        let cleanup_failed = self.finish().is_err();
        let snapshot = self.factory.cleanup.snapshot();
        if cleanup_failed
            || !self.behavior_passed
            || thread::panicking()
            || !snapshot.fixture_failures.is_empty()
            || snapshot
                .children
                .values()
                .any(|child| !child.failures.is_empty())
        {
            if let Some(root) = self.root.take() {
                let path = root.keep();
                eprintln!("Native fixture evidence retained at {}", path.display());
            }
        }
    }
}

struct ProbeFactory<'a> {
    actual: &'a NativeCommonFactory,
    kind: ProbeKind,
    operation_id: String,
    evidence: Mutex<Vec<Arc<Mutex<ParentProbeState>>>>,
}
impl CommonAdvisoryFixtureFactory for ProbeFactory<'_> {
    fn create(
        &self,
        scenario: &CompatibilityScenario,
        exact_scope: &OwnedExactScope,
    ) -> Result<Box<dyn CommonAdvisoryFixture>, FixtureUnavailable> {
        let fixture = self.actual.create_owned(scenario, exact_scope)?;
        fixture
            .arm_probe(self.kind, &self.operation_id)
            .map_err(unavailable)?;
        self.evidence
            .lock()
            .map_err(|_| unavailable("probe evidence list poisoned"))?
            .push(fixture.proxy.probes.clone());
        Ok(Box::new(fixture))
    }
}
impl ProbeFactory<'_> {
    fn evidence(&self) -> ParentProbeEvidence {
        let states = self.evidence.lock().expect("probe evidence list");
        assert_eq!(states.len(), 1, "one actual scenario child owns the probe");
        let state = states[0].lock().expect("probe evidence state");
        assert!(state.pending.is_none(), "probe must reach actual invoke");
        state.last.clone().expect("actual completed parent probe")
    }
}
fn probe_reply<'a>(
    report: &'a tracedecay_memory_conformance::compatibility::CommonAdvisoryReport,
    step_id: &str,
) -> &'a ProviderReply {
    let row = report
        .results
        .iter()
        .find(|row| row.step_id == step_id)
        .expect("probe result row");
    assert!(
        row.details.is_empty(),
        "probe common-runner violations: {:?}",
        row.details
    );
    let step = row
        .operation_report
        .as_ref()
        .expect("actual common operation report")
        .steps()
        .iter()
        .find(|step| matches!(step.output(), ProductStepOutput::Operation(_)))
        .expect("actual provider operation output");
    assert!(
        step.provider_contacted(),
        "pre-dispatch host interception is not evidence"
    );
    let ProductStepOutput::Operation(reply) = step.output() else {
        unreachable!()
    };
    reply
}
fn health_probe_step(scenarios: &[CompatibilityScenario]) -> CompatibilityStep {
    scenarios.iter().flat_map(|scenario| &scenario.steps).find(|step| {
        matches!(step, CompatibilityStep::Call { fixture, .. } if fixture.operation == ProviderOperation::Health)
    }).expect("unchanged suite health fixture").clone()
}

#[test]
fn native_common_factory_cancellation_after_actual_authority_entry_drains_real_terminal() {
    let mut environment = NativeTestEnvironment::new();
    let scenarios =
        common_advisory_scenarios(&environment.exact_scope, 1).expect("common scenarios");
    let step = |id: &str| {
        scenarios
            .iter()
            .flat_map(|scenario| &scenario.steps)
            .find(|step| step.step_id() == id)
            .expect("existing common source-authority step")
            .clone()
    };
    let mut cancelled = step("authority.original_attribution_retained");
    let CompatibilityStep::Call {
        fixture,
        assertions,
        ..
    } = &mut cancelled
    else {
        unreachable!()
    };
    let cancelled_id = fixture.step_id.clone();
    let operation_id = fixture.operation_id.clone();
    assert!(!fixture.control.cancel_before_dispatch);
    fixture.expectation.terminal = TerminalExpectation::exactly(TerminalCode::Cancelled);
    fixture.expectation.committed_effect = ExpectedCommittedEffect::none();
    fixture.expectation.state_generation = GenerationExpectation::Unchanged;
    fixture.expectation.payload = PayloadExpectation::Absent;
    assertions.clear();
    let next = health_probe_step(&scenarios);
    let next_id = next.step_id().to_owned();
    let scenario = CompatibilityScenario {
        case_id: "native.transport.cancel_inside_actual_authority".into(),
        steps: vec![
            step("authority.observe_original"),
            step("authority.open_destination"),
            cancelled,
            next,
        ],
    };
    let factory = ProbeFactory {
        actual: &environment.factory,
        kind: ProbeKind::CancelInAuthority,
        operation_id,
        evidence: Mutex::new(Vec::new()),
    };
    let report = run_compatibility_program(&factory, &environment.exact_scope, 1, &[scenario]);
    let evidence = factory.evidence();
    let cancelled = probe_reply(&report, &cancelled_id);
    assert_eq!(cancelled.terminal.terminal_code(), TerminalCode::Cancelled);
    assert_eq!(
        cancelled.terminal.committed_effect().state(),
        CommittedEffectState::None
    );
    assert_eq!(
        probe_reply(&report, &next_id).terminal.terminal_code(),
        TerminalCode::Success
    );
    assert_eq!(report.denominators.failed, 0, "{:#?}", report.results);
    assert_eq!(report.denominators.unknown, 0, "{:#?}", report.results);
    assert!(evidence.failures.is_empty(), "{evidence:#?}");
    assert!(evidence.cancelled_after_witness && evidence.drained_actual_response);
    let witness = evidence
        .witness
        .as_ref()
        .expect("Native actor authority-entry witness");
    let child = evidence
        .child
        .as_ref()
        .expect("actual child probe accounting");
    assert_eq!(witness.authority_entries, 1);
    assert_eq!(child.authority_entries, 1);
    assert_eq!(
        child.provider_invocations, 1,
        "the operation must never be retried"
    );
    assert_eq!(
        child.rpc_id,
        evidence.rpc_id.expect("correlated response id")
    );
    assert_eq!(
        child.original_deadline_utc_micros,
        evidence.original_deadline_utc_micros.unwrap()
    );
    assert!(child.original_remaining_millis > 0 && child.original_remaining_millis <= 30_000);
    assert_eq!(child.terminal_code.as_deref(), Some("cancelled"));
    drop(factory);
    environment
        .finish()
        .expect("all actual probe children reaped");
    let cleanup = environment.factory.cleanup.snapshot();
    assert_eq!(cleanup.children.len(), 1);
    let reaped = cleanup
        .children
        .values()
        .next()
        .expect("actual child cleanup record");
    assert!(reaped.confirmed_reaped && reaped.child_identity.is_some());
    assert_eq!(reaped.pid, child.pid);
    environment.behavior_passed = true;
}

#[test]
fn native_common_factory_cancel_with_two_response_prefix_bytes_resumes_same_frame() {
    let mut environment = NativeTestEnvironment::new();
    let scenarios =
        common_advisory_scenarios(&environment.exact_scope, 1).expect("common scenarios");
    let first = health_probe_step(&scenarios);
    let first_id = first.step_id().to_owned();
    let CompatibilityStep::Call { fixture, .. } = &first else {
        unreachable!()
    };
    let operation_id = fixture.operation_id.clone();
    let mut next = first.clone();
    let CompatibilityStep::Call { fixture, .. } = &mut next else {
        unreachable!()
    };
    fixture.step_id = "native.transport.next_actual_health".into();
    fixture.request_id = "native.transport.next_actual_health.request".into();
    fixture.operation_id = "019467f0-0000-7000-8000-0000000000fe".into();
    let next_id = fixture.step_id.clone();
    let next_operation_id = fixture.operation_id.clone();
    let scenario = CompatibilityScenario {
        case_id: "native.transport.cancel_after_partial_actual_response".into(),
        steps: vec![first, next],
    };
    let factory = ProbeFactory {
        actual: &environment.factory,
        kind: ProbeKind::SplitResponsePrefix,
        operation_id,
        evidence: Mutex::new(Vec::new()),
    };
    let report = run_compatibility_program(&factory, &environment.exact_scope, 1, &[scenario]);
    let evidence = factory.evidence();
    // Native completed the first Health before its response was split. Caller
    // cancellation must retain that actual success, not invent Cancelled.
    assert_eq!(
        probe_reply(&report, &first_id).terminal.terminal_code(),
        TerminalCode::Success
    );
    let next = probe_reply(&report, &next_id);
    assert_eq!(next.terminal.terminal_code(), TerminalCode::Success);
    assert_eq!(next.terminal.operation_id(), next_operation_id);
    assert_eq!(report.denominators.failed, 0, "{:#?}", report.results);
    assert_eq!(report.denominators.unknown, 0, "{:#?}", report.results);
    assert!(evidence.failures.is_empty(), "{evidence:#?}");
    assert!(evidence.cancelled_after_witness && evidence.drained_actual_response);
    assert_eq!(
        evidence.retained_header_bytes, 2,
        "must exercise the interrupted prefix"
    );
    assert_eq!(evidence.retained_body_bytes, 0);
    let child = evidence
        .child
        .as_ref()
        .expect("actual child probe accounting");
    assert_eq!(
        child.provider_invocations, 1,
        "draining must never redispatch the operation"
    );
    assert_eq!(
        child.rpc_id,
        evidence.rpc_id.expect("correlated actual response")
    );
    assert_eq!(
        child.original_deadline_utc_micros,
        evidence.original_deadline_utc_micros.unwrap()
    );
    assert_eq!(child.terminal_code.as_deref(), Some("success"));
    drop(factory);
    environment
        .finish()
        .expect("all actual partial-frame probe children reaped");
    let cleanup = environment.factory.cleanup.snapshot();
    assert_eq!(cleanup.children.len(), 1);
    let reaped = cleanup
        .children
        .values()
        .next()
        .expect("actual child cleanup record");
    assert!(reaped.confirmed_reaped && reaped.child_identity.is_some());
    assert_eq!(reaped.pid, child.pid);
    environment.behavior_passed = true;
}

#[test]
fn native_common_advisory_factory_compatibility() {
    let mut environment = NativeTestEnvironment::new();
    let report = run_common_advisory_suite(&environment.factory, &environment.exact_scope, 1);
    environment
        .finish()
        .expect("all Native common-suite child owners confirmed reaped");
    let report = report.expect("unchanged common program");
    eprintln!(
        "real Native common suite denominators: {:?}; legacy delivery identity: {:?}",
        report.denominators, report.legacy_delivery_identity
    );
    let failures: Vec<_> = report
        .results
        .iter()
        .filter(|row| !row.details.is_empty())
        .map(|row| (&row.case_id, &row.step_id, &row.verdict, &row.details))
        .collect();
    assert!(
        report.compatible(),
        "real Native common suite: {:?}; missing operations={:?}; failures={failures:#?}",
        report.denominators,
        report.missing_operations
    );
    assert_eq!(
        report.denominators.unknown, 0,
        "environment capabilities must not remain unresolved"
    );
    assert_eq!(
        report.denominators.unknown_effects, report.denominators.reconciled_effects,
        "every lost effect must be reconciled"
    );
    assert_eq!(
        report.denominators.planned,
        report.denominators.passed + report.denominators.degraded
    );
    environment.behavior_passed = true;
}
