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
use std::process::{Child, Command, Stdio};
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
    CommonAdvisoryFixture, CommonAdvisoryFixtureFactory, CompatibilityScenario,
    FixtureEnvironmentAction, FixtureEnvironmentEvidence, FixtureUnavailable, LegacyRecordEvidence,
    run_common_advisory_suite, scope_json,
};
use tracedecay_memory_conformance::real_fixture::{
    DescriptorDto, FixtureAuthority, FixtureAuthoritySnapshot, HandshakeRequestDto,
    HandshakeResponseDto, ProviderCallDto, ProviderReplyDto,
};
use tracedecay_memory_provider_registry::{
    COMMON_ADVISORY_PROFILE_ID, CancellationToken, CanonicalPayload, CommittedEffectState,
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

/// No blocking socket read, write, or mutex acquisition outlives the original
/// call control. Transport failure is a fixture failure, never a provider reply.
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
            control.snapshot().map_err(failure)?;
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
fn write_frame<T: Serialize>(
    stream: &mut TcpStream,
    value: &T,
    bound: &WaitBound<'_>,
) -> Result<(), String> {
    let mut bytes = serde_json::to_vec(value).map_err(failure)?;
    if bytes.is_empty() || bytes.len() > FRAME_MAXIMUM {
        return Err("fixture frame length exceeded".into());
    }
    let mut header = u32::try_from(bytes.len()).map_err(failure)?.to_be_bytes();
    transfer(|bytes| stream.write(bytes), &mut header, bound)?;
    transfer(|bytes| stream.write(bytes), &mut bytes, bound)
}
fn read_frame<T: DeserializeOwned>(
    stream: &mut TcpStream,
    bound: &WaitBound<'_>,
) -> Result<T, String> {
    let mut header = [0; 4];
    transfer(|bytes| stream.read(bytes), &mut header, bound)?;
    let length = usize::try_from(u32::from_be_bytes(header)).map_err(failure)?;
    if length == 0 || length > FRAME_MAXIMUM {
        return Err("fixture frame length exceeded".into());
    }
    let mut bytes = vec![0; length];
    transfer(|bytes| stream.read(bytes), &mut bytes, bound)?;
    serde_json::from_slice(&bytes).map_err(failure)
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
        id: u64,
        value: HandshakeRequestDto,
    },
    Invoke {
        id: u64,
        value: ProviderCallDto,
    },
    RecordDisposition {
        source_key: String,
        disposition: String,
    },
    SetAuthorityAvailable(bool),
    LoseNextReply,
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
    Stopped,
    Error(String),
}

struct OwnedChild {
    process: Child,
    rpc: TcpStream,
    cancellation: TcpStream,
    identity: String,
}
impl OwnedChild {
    fn exchange(
        &mut self,
        request: &ChildRequest,
        bound: &WaitBound<'_>,
    ) -> Result<ChildResponse, String> {
        let result = write_frame(&mut self.rpc, request, bound)
            .and_then(|()| read_frame(&mut self.rpc, bound));
        if result.is_err()
            && bound
                .control
                .is_some_and(|control| control.snapshot().is_err())
        {
            let id = match request {
                ChildRequest::Handshake { id, .. } | ChildRequest::Invoke { id, .. } => Some(*id),
                _ => None,
            };
            if let Some(id) = id {
                // The independent stream remains readable while the child is in
                // Native invoke. Never reset or extend the ended caller control.
                let cancel_bound = WaitBound {
                    deadline: Instant::now() + Duration::from_millis(20),
                    control: None,
                };
                let mut bytes = id.to_be_bytes();
                let _ = transfer(
                    |bytes| self.cancellation.write(bytes),
                    &mut bytes,
                    &cancel_bound,
                );
            }
        }
        match result? {
            ChildResponse::Error(error) => Err(error),
            response => Ok(response),
        }
    }
    fn confirm_exit(&mut self, deadline: i64) -> Result<bool, String> {
        loop {
            if self.process.try_wait().map_err(failure)?.is_some() {
                return Ok(true);
            }
            if before_deadline(deadline).is_err() {
                return Ok(false);
            }
            thread::sleep(POLL);
        }
    }
    fn kill_and_reap(&mut self, deadline: i64) -> Result<(), String> {
        if self.process.try_wait().map_err(failure)?.is_none() {
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
        if self.process.try_wait().ok().flatten().is_none() {
            let deadline = deadline_after(Duration::from_secs(5)).expect("cleanup clock");
            self.kill_and_reap(deadline)
                .expect("owned Native child must be reaped during cleanup");
        }
    }
}

struct ProcessState {
    config: ChildConfig,
    control_root: PathBuf,
    launches: u64,
    request_sequence: u64,
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
        self.starting_child = Some(process);
        let startup = (|| {
            let mut rpc = None;
            let mut cancellation = None;
            let mut identity = None;
            while rpc.is_none() || cancellation.is_none() {
                before_deadline(deadline)?;
                if self
                    .starting_child
                    .as_mut()
                    .ok_or("missing starting child")?
                    .try_wait()
                    .map_err(failure)?
                    .is_some()
                {
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
        self.child = Some(OwnedChild {
            process,
            rpc,
            cancellation,
            identity,
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
            if process.try_wait().map_err(failure)?.is_none() {
                process.kill().map_err(failure)?;
            }
            while process.try_wait().map_err(failure)?.is_none() {
                if before_deadline(deadline).is_err() {
                    return Ok(false);
                }
                thread::sleep(POLL);
            }
            self.starting_child.take();
        }
        let Some(child) = self.child.as_mut() else {
            return Ok(true);
        };
        if child.process.try_wait().map_err(failure)?.is_none() {
            if force {
                child.kill_and_reap(deadline)?;
            } else {
                let budget = Duration::from_micros(
                    u64::try_from(
                        deadline
                            .checked_sub(now_micros()?)
                            .ok_or("shutdown deadline overflow")?,
                    )
                    .map_err(failure)?,
                );
                let bound = WaitBound {
                    deadline: Instant::now() + budget,
                    control: None,
                };
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
        if self.starting_child.is_some() {
            let deadline = deadline_after(Duration::from_secs(5)).expect("startup cleanup clock");
            assert!(
                self.stop(deadline, true).expect("startup child cleanup"),
                "starting Native child was not reaped"
            );
        }
    }
}

#[derive(Clone)]
struct NativeProcessProxy {
    state: Arc<Mutex<ProcessState>>,
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
    fn request(
        &self,
        mut request: ChildRequest,
        control: Option<&OperationControl>,
    ) -> Result<ChildResponse, String> {
        let bound = control.map_or_else(WaitBound::environment, WaitBound::operation);
        let mut state = self.lock(&bound)?;
        if let ChildRequest::Handshake { id, .. } | ChildRequest::Invoke { id, .. } = &mut request {
            state.request_sequence = state
                .request_sequence
                .checked_add(1)
                .ok_or("RPC sequence overflow")?;
            *id = state.request_sequence;
        }
        state
            .child
            .as_mut()
            .ok_or("Native child is not running; no provider reply can be fabricated")?
            .exchange(&request, &bound)
    }
    fn descriptor_result(&self) -> Result<ProviderDescriptor, String> {
        let mut state = self.lock(&WaitBound::environment())?;
        if let Some(child) = state.child.as_mut() {
            let ChildResponse::Descriptor(value) =
                child.exchange(&ChildRequest::Descriptor, &WaitBound::environment())?
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
            .request(
                ChildRequest::Handshake { id: 0, value },
                Some(&request.control),
            )
            .expect("actual Native handshake transport")
        else {
            panic!("wrong handshake response");
        };
        value.restore().expect("lossless actual handshake response")
    }
    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        let value = ProviderCallDto::capture(call).expect("lossless original provider call");
        let ChildResponse::Reply(value) = self
            .request(ChildRequest::Invoke { id: 0, value }, Some(&call.control))
            .expect("actual Native invoke transport")
        else {
            panic!("wrong invoke response");
        };
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
        state.request_sequence = state
            .request_sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("RPC sequence overflow"))?;
        let id = state.request_sequence;
        let child = state
            .child
            .as_mut()
            .ok_or_else(|| io::Error::other("Native child is absent"))?;
        let ChildResponse::Handshake(response) = child
            .exchange(&ChildRequest::Handshake { id, value }, &bound)
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
}
struct NativeCommonFixture {
    proxy: Arc<NativeProcessProxy>,
    supervisor: ProviderSupervisorV1<NativeLifecycle>,
    base_scope: OwnedExactScope,
    current_scope: OwnedExactScope,
    namespace: u64,
    root: tempfile::TempDir,
}
impl CommonAdvisoryFixtureFactory for NativeCommonFactory {
    fn create(
        &self,
        scenario: &CompatibilityScenario,
        exact_scope: &OwnedExactScope,
    ) -> Result<Box<dyn CommonAdvisoryFixture>, FixtureUnavailable> {
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
                request_sequence: 0,
                child: None,
                starting_child: None,
                last_exited_process: None,
                last_descriptor: None,
            })),
        });
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
            root,
        };
        fixture.start().map_err(unavailable)?;
        Ok(Box::new(fixture))
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
        if !report.confirmed_dead || self.proxy.lock(&WaitBound::environment())?.child.is_some() {
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
                // The next Restart action, not this writer, opens/migrates v1.
                Ok(FixtureEnvironmentEvidence::LegacyV1Installed {
                    stored_version: 1,
                    source_count: u64::try_from(records.len()).map_err(failure)?,
                    records,
                })
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
impl Drop for NativeCommonFixture {
    fn drop(&mut self) {
        // Also runs when the common program fails a prerequisite or panics.
        // ProcessState/OwnedChild is a second kill/reap guard on failure.
        if let Err(error) = self.stop() {
            eprintln!("Native fixture graceful cleanup failed; owned kill guard follows: {error}");
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
    let result = (|| {
        let authority = FixtureAuthority::from_snapshot(config.authority.clone())?;
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
            .with_admission_authority(Arc::new(authority.clone())),
        );
        let provider = NativeProvider::new(port.clone()).map_err(failure)?;
        write_frame(
            &mut rpc,
            &ChildResponse::Ready {
                authority: authority.snapshot()?,
            },
            &WaitBound::environment(),
        )?;
        loop {
            let request: ChildRequest = read_frame(&mut rpc, &WaitBound::environment())?;
            let received = Instant::now();
            let response = match request {
                ChildRequest::Descriptor => {
                    ChildResponse::Descriptor(DescriptorDto::capture(&provider.descriptor())?)
                }
                ChildRequest::Handshake { id, value } => {
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
                ChildRequest::Invoke { id, value } => {
                    let token = bridge
                        .lock()
                        .map_err(|_| "child cancellation lock poisoned")?
                        .start(id);
                    let call = value.restore(token, received.elapsed())?;
                    let response = provider.invoke(&call);
                    bridge
                        .lock()
                        .map_err(|_| "child cancellation lock poisoned")?
                        .active = None;
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
                ChildRequest::Shutdown => break,
            };
            write_frame(&mut rpc, &response, &WaitBound::environment())?;
        }
        drop(provider);
        drop(port); // The production actor Drop joins its thread.
        drop(graph);
        runtime.shutdown_timeout(Duration::from_secs(5));
        write_frame(&mut rpc, &ChildResponse::Stopped, &WaitBound::environment())?;
        Ok(())
    })();
    if let Err(error) = &result {
        let _ = write_frame(
            &mut rpc,
            &ChildResponse::Error(error.clone()),
            &WaitBound::environment(),
        );
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
    let mut statement = connection.prepare("SELECT provider_reference,operation_id,idempotency_key,receipt FROM tdmem_native_staged_observation_v1 ORDER BY admitted_sequence").map_err(failure)?;
    let records = statement
        .query_map([], |row| {
            Ok(LegacyRecordEvidence {
                stable_memory_ref: row.get(0)?,
                original_operation_id: row.get(1)?,
                original_idempotency_key: row.get(2)?,
                original_receipt_sha256: row.get(3)?,
                retained_source_fields: BTreeMap::new(),
            })
        })
        .map_err(failure)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(failure)?;
    if records.len() != observations.len() {
        return Err("legacy writer lost a source".into());
    }
    Ok(records)
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

#[test]
fn native_common_advisory_factory_compatibility() {
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
    };
    let report =
        run_common_advisory_suite(&factory, &exact_scope, 1).expect("unchanged common program");
    eprintln!(
        "real Native common suite denominators: {:?}",
        report.denominators
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
}
