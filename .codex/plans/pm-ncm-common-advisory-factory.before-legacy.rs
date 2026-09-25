//! The unchanged common advisory suite over the production NCM worker and model.
#![cfg(all(feature = "rust-backend", unix))]
#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::{MetadataExt, symlink};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tracedecay_memory_conformance::compatibility::{
    CommonAdvisoryFixture, CommonAdvisoryFixtureFactory, CompatibilityScenario,
    CompatibilityVerdict, FixtureEnvironmentAction, FixtureEnvironmentEvidence, FixtureUnavailable,
    run_common_advisory_suite,
};
use tracedecay_memory_conformance::real_fixture::{FixtureAuthority, FixtureAuthoritySnapshot};
use tracedecay_memory_provider_api::contract::{
    CommittedEffectState, SourceDisposition, TerminalCode,
};
use tracedecay_memory_provider_api::{
    CancellationToken, HandshakeRequest, HandshakeRequestParts, HandshakeResponse, MemoryProvider,
    OperationControl, OwnedExactScope, ProviderCall, ProviderDescriptor, ProviderOperation,
    ProviderReply, TerminalRecord,
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
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Same post-commit cancellation seam as the direct Rust backend regression.
/// The actual worker response must prove a durable mutation before this hook
/// cancels the original call. The adapter produces the unknown-effect terminal.
struct WithholdCommittedReply {
    inner: Arc<RustNcmSurface>,
    armed: Arc<AtomicBool>,
}

impl NcmCognitiveSurface for WithholdCommittedReply {
    fn descriptor(&self) -> ProviderDescriptor {
        self.inner.descriptor()
    }

    fn handshake(&self, request: &NcmSurfaceHandshakeRequest) -> NcmSurfaceHandshakeResponse {
        self.inner.handshake(request)
    }

    fn invoke(&self, call: &NcmSurfaceCall) -> ProviderReply {
        let reply = self.inner.invoke(call);
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

/// Fixture host admission. Admitted calls and all their bytes go unchanged to
/// the real adapter; mode evidence is read from this same dispatch gate.
struct FixtureHost {
    adapter: NcmProviderAdapter,
    scope: OwnedExactScope,
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
        if request.exact_scope != self.scope {
            return HandshakeResponse {
                terminal: TerminalRecord::failure_before_dispatch(
                    ProviderOperation::Handshake,
                    request.provider_id.clone(),
                    TerminalCode::ScopeMismatch,
                    &request.request_id,
                    request.exact_scope.exact_scope_sha256(),
                    None,
                    "fixture.host.mounted_scope_differs",
                ),
                descriptor: None,
                provider_instance_id: None,
                state_namespace: None,
                accepted_scope: None,
                effective_limits: None,
                ready_receipt_sha256: None,
                warnings: Vec::new(),
            };
        }
        self.adapter.handshake(request)
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        let refusal = if call.exact_scope != self.scope {
            Some((
                TerminalCode::ScopeMismatch,
                "fixture.host.mounted_scope_differs",
            ))
        } else if !self.admits(call.operation) {
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
    fn read(pid: u32) -> Result<Option<Self>, String> {
        let output = Command::new("ps")
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
            .env("LC_ALL", "C")
            .output()
            .map_err(err)?;
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

    fn confirm_exited(&self) -> Result<(), String> {
        if Self::read(self.pid)?.as_ref() == Some(self) {
            return Err("owned worker still exists after the bounded reap".into());
        }
        Ok(())
    }
}

struct RealNcmFactory {
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
        });
        let adapter = NcmProviderAdapter::new(surface)
            .map_err(err)?
            .with_admission_authority(Arc::new(self.authority.clone()));
        Ok(FixtureHost {
            adapter,
            scope: self.scope.clone(),
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
        self.host = Some(self.make_host(&owner)?);
        self.owner = Some(owner);
        if self.namespace_database().exists() {
            return Err("new physical namespace was unexpectedly populated".into());
        }
        Ok(())
    }

    fn start_current(&mut self) -> Result<ProcessIdentity, String> {
        self.owner()?
            .start(Instant::now() + LIFECYCLE_BUDGET)
            .map_err(err)?;
        let pid = self
            .owner()?
            .worker_pid()
            .ok_or("start returned without an owned child")?;
        let identity =
            ProcessIdentity::read(pid)?.ok_or("started child exited before identity capture")?;
        if identity.parent_pid != std::process::id() {
            return Err("worker process is not owned by this fixture process".into());
        }
        self.stopped_process = None;
        Ok(identity)
    }

    fn stop_current(&mut self) -> Result<Option<ProcessIdentity>, String> {
        let Some(owner) = self.owner.as_ref() else {
            return Ok(self.stopped_process.clone());
        };
        let previous = match owner.worker_pid() {
            Some(pid) => {
                Some(ProcessIdentity::read(pid)?.ok_or("owned child vanished before stop")?)
            }
            None => self.stopped_process.clone(),
        };
        if !owner
            .request_stop(Instant::now() + LIFECYCLE_BUDGET)
            .map_err(err)?
            || owner.worker_pid().is_some()
        {
            return Err("worker stop did not confirm process reap and pipe-thread joins".into());
        }
        if let Some(previous) = &previous {
            previous.confirm_exited()?;
        }
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
    fn reopen_persisted_namespace(&self) -> Result<(), String> {
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
            control: OperationControl::new(i64::MAX, 30_000, CancellationToken::new()),
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

    fn corrupt_payload(&mut self) -> Result<FixtureEnvironmentEvidence, String> {
        self.stop_current()?
            .ok_or("cannot corrupt an unstarted provider")?;
        let database = self.namespace_database();
        let before = read_capsule(&database)?;
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
        )?;
        if changed_rows.as_slice() != [json!({"changed": 1})] {
            return Err("physical corruption did not update exactly the selected capsule".into());
        }
        let after = read_capsule(&database)?;
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
        match action {
            FixtureEnvironmentAction::Restart => {
                let identity = self.build_identity()?;
                let database = self.namespace_database();
                let metadata = fs::metadata(&database).map_err(err)?;
                let physical_file = (metadata.dev(), metadata.ino());
                let previous = self.stop_current()?.ok_or("restart has no previous real process")?;
                let current = self.start_current()?;
                if previous == current {
                    return Err("restart reused the previous process start identity".into());
                }
                self.reopen_persisted_namespace()?;
                let metadata = fs::metadata(&database).map_err(err)?;
                if physical_file != (metadata.dev(), metadata.ino()) || identity != self.build_identity()? {
                    return Err("restart changed the physical store or selected implementation".into());
                }
                previous.confirm_exited()?;
                Ok(FixtureEnvironmentEvidence::Restarted {
                    previous_process: previous.evidence(), current_process: current.evidence(),
                    previous_process_exited: true, reopened_persisted_namespace: true,
                })
            }
            FixtureEnvironmentAction::FreshNamespace => {
                let previous_namespace = self.namespace_evidence();
                let identity = self.build_identity()?;
                self.revalidate_current_authority()?;
                self.stop_current()?;
                self.host.take();
                self.owner.take();
                self.namespace_number = self.namespace_number.checked_add(1).ok_or("namespace counter overflow")?;
                self.provider_root = self.root.join(format!("provider-{}", self.namespace_number));
                self.open_physical_namespace()?;
                self.start_current()?;
                if self.namespace_database().exists() || self.build_identity()? != identity {
                    return Err("fresh namespace did not preserve selection and empty physical state".into());
                }
                Ok(FixtureEnvironmentEvidence::FreshNamespace {
                    previous_namespace, current_namespace: self.namespace_evidence(),
                    current_dispositions_revalidated: true,
                })
            }
            FixtureEnvironmentAction::OpenSession { destination_scope } => {
                destination_scope.validate().map_err(err)?;
                let identity = self.build_identity()?;
                self.scope = destination_scope.clone();
                self.host = Some(self.make_host(self.owner()?)?);
                Ok(FixtureEnvironmentEvidence::SessionOpened {
                    destination_scope: self.scope.clone(),
                    selected_provider_unchanged: self.build_identity()? == identity,
                })
            }
            FixtureEnvironmentAction::InstallLegacyV1 { .. } => Err(
                "No genuine pre-common NCM writer retains the common public stable reference and original delivery receipt required by LegacyRecordEvidence. The actual pre-common schema is already version 1; its key/value/source records lack common_capsule and delivery_capsule. Current observations cannot be relabeled as legacy, and a production migration/inspection seam has not been supplied.".into(),
            ),
            FixtureEnvironmentAction::CorruptPayloadPreservingDigest => self.corrupt_payload(),
            FixtureEnvironmentAction::RecordSourceDisposition { source_key, disposition } => {
                let state = SourceDisposition::from_wire(disposition).ok_or("unknown source disposition")?;
                let authority_ref = self.authority.record_disposition(source_key, state)?;
                self.persist_authority()?;
                Ok(FixtureEnvironmentEvidence::SourceDispositionRecorded {
                    source_key: source_key.clone(), disposition: disposition.clone(), authority_ref,
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
                Ok(FixtureEnvironmentEvidence::AuthorityAvailability { available: *available })
            }
            FixtureEnvironmentAction::SetMode { mode } => {
                let identity = self.build_identity()?;
                let next = match mode.as_str() {
                    "active" => ACTIVE, "observe" => OBSERVE,
                    "disabled" => DISABLED, "quarantined" => QUARANTINED,
                    _ => return Err("unknown fixture host mode".into()),
                };
                write_synced(&self.root.join("host/mode"), mode.as_bytes())?;
                self.mode.store(next, Ordering::SeqCst);
                Ok(FixtureEnvironmentEvidence::ModeChanged {
                    mode: mode.clone(), selected_provider_unchanged: self.build_identity()? == identity,
                    recall_admitted: self.host()?.admits(ProviderOperation::Recall),
                    observation_admitted: self.host()?.admits(ProviderOperation::Observe),
                })
            }
            FixtureEnvironmentAction::Shutdown => {
                let previous = self.stop_current()?;
                self.host.take();
                self.owner.take();
                if let Some(previous) = previous { previous.confirm_exited()?; }
                Ok(FixtureEnvironmentEvidence::Shutdown { remaining_workers: 0 })
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
        let mut confirmed = self.stop_current().is_ok();
        if !confirmed && let Some(owner) = self.owner.as_ref() {
            confirmed = owner.kill(Instant::now() + LIFECYCLE_BUDGET).is_ok()
                && owner.worker_pid().is_none();
        }
        self.host.take();
        self.owner.take();
        // Never unlink mutable state while a fixture child may still own it.
        if confirmed {
            let _ = fs::remove_dir_all(&self.root);
        }
    }
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

fn read_capsule(database: &Path) -> Result<Value, String> {
    let mut rows = sqlite_json(
        database,
        "SELECT record_id, provenance FROM capsules WHERE status = 'valid' ORDER BY record_id LIMIT 1;",
        true,
    )?;
    if rows.len() != 1 {
        return Err("physical fixture has no single retained capsule".into());
    }
    Ok(rows.remove(0))
}

fn sqlite_json(database: &Path, sql: &str, read_only: bool) -> Result<Vec<Value>, String> {
    if !database.is_file() {
        return Err("fixture database does not exist".into());
    }
    let mut command = Command::new("sqlite3");
    command.args(["-batch", "-json"]);
    if read_only {
        command.arg("-readonly");
    }
    let output = command.arg(database).arg(sql).output().map_err(err)?;
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
    let report = run_common_advisory_suite(&factory, &scope, REGISTRATION_REVISION).unwrap();
    let rows: Vec<_> = report.results.iter().map(|row| json!({
        "case_id": row.case_id, "step_id": row.step_id,
        "verdict": format!("{:?}", row.verdict), "details": row.details,
        "environment_evidence": row.environment_evidence.as_ref().map(|value| format!("{value:?}")),
        "operation_report_present": row.operation_report.is_some(),
    })).collect();
    let d = report.denominators;
    let summary = json!({
        "common_program_matched": report.common_program_matched,
        "compatible": report.compatible(),
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
    let mut evidence = File::create_new(run_root.join("actual-report.txt")).unwrap();
    writeln!(evidence, "{report:#?}").unwrap();
    evidence.sync_all().unwrap();
    let failures: Vec<_> = report
        .results
        .iter()
        .filter(|row| {
            matches!(
                row.verdict,
                CompatibilityVerdict::Failed | CompatibilityVerdict::Unknown
            )
        })
        .map(|row| {
            format!(
                "{} / {}: {}",
                row.case_id,
                row.step_id,
                row.details.join("; ")
            )
        })
        .collect();
    assert!(
        report.compatible(),
        "real NCM common suite is not compatible; actual report at {}\n{}",
        run_root.display(),
        failures.join("\n")
    );
}
