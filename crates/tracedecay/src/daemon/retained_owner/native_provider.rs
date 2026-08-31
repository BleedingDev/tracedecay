//! Project-owned Native application port.
//!
//! The provider-neutral Native adapter is synchronous, while the retained
//! project-memory authority is asynchronous. This module keeps that seam
//! narrow: one bounded actor owns a current-thread Tokio runtime and performs
//! only the read needed to verify an already-settled Native fact promotion.
//! No provider operation in this module writes Native memory.

// This implementation is intentionally constructible before product
// composition mounts it. Keep the dormant constructor/actor surface warning-
// free until the composition owner wires the explicit activation path.
#![allow(dead_code)]

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tracedecay_application::RetainedSurfaceExecutionErrorV1;
use tracedecay_application::retained_surfaces::{
    FactCommitOwnerV1, FactCommitReceiptV1, FactProjectionV1, FactV1, MemoryScopeV1,
};
use tracedecay_domain::{FactOwnerV1, ProjectId};
use tracedecay_memory_provider_registry::{
    ApiError, CommittedEffectEvidence, FallbackDirective, HandshakeRequest, HandshakeResponse,
    NATIVE_FACT_PROMOTION_OBSERVATION_KIND, NATIVE_FACT_PROMOTION_PAYLOAD_CONTRACT_ID,
    NATIVE_PROVIDER_ID, NativeMemoryApplicationPort, NativeObservation, OBSERVATION_CONTRACT_ID,
    OperationControl, OwnedProviderId, OwnedVersionedId, ProviderCall, ProviderDescriptor,
    ProviderLimits, ProviderOperation, ProviderReply, TerminalCode, TerminalRecord,
};
use tracedecay_store::{
    FactReadControl, ProjectMemoryFactHistoryQueryV1, ProjectMemoryFactHistoryV1,
    ProjectMemoryFactIdV1,
};

use super::memory::memory_application;
use super::memory_mapping;
use super::memory_target::{MemoryTargetAccessV1, open_project_retained_memory_target};
use crate::tracedecay::TraceDecay;

#[cfg(test)]
#[path = "native_provider_tests.rs"]
mod tests;

const IMPLEMENTATION_IDENTITY_SHA256: &str =
    "7fe6923361d4caa6c213e0760d438c9f3b9bda60d4c1195812130bfe66c2fa16";
const STATE_SCHEMA_VERSION: &str = "native-application-port-v1";
const PROVIDER_INSTANCE_ID: &str = "tracedecay.native.project";
const STATE_NAMESPACE: &str = "tracedecay.native.project";
const READY_RECEIPT_DOMAIN: &[u8] = b"tracedecay.native.application-ready.v1\0";
const ACTOR_THREAD_NAME: &str = "tracedecay-native-memory-read";
const ACTOR_POLL_MILLIS: u64 = 10;
const NATIVE_OPERATION_MILLIS: u64 = 1_000;

const INVALID_PAYLOAD_DIAGNOSTIC: &str = "native.fact_promotion_payload_invalid";
const PROMOTION_MISMATCH_DIAGNOSTIC: &str = "native.fact_promotion_verification_mismatch";
const SCOPE_UNAVAILABLE_DIAGNOSTIC: &str = "native.fact_promotion_scope_unavailable";
const PROVIDER_UNAVAILABLE_DIAGNOSTIC: &str = "native.application_port_unavailable";
const CANCELLED_DIAGNOSTIC: &str = "native.fact_promotion_cancelled";
const DEADLINE_DIAGNOSTIC: &str = "native.fact_promotion_deadline_exceeded";

/// Construction failures for the project-owned Native application port.
#[derive(Debug)]
pub(crate) enum NativeMemoryApplicationPortBuildError {
    /// The fixed provider descriptor could not be assembled or validated.
    Descriptor(ApiError),
    /// The bounded actor runtime could not be constructed.
    Runtime(std::io::Error),
    /// The bounded actor thread could not be started.
    ActorThread(std::io::Error),
}

impl fmt::Display for NativeMemoryApplicationPortBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Descriptor(error) => {
                write!(
                    formatter,
                    "Native application-port descriptor is invalid: {error}"
                )
            }
            Self::Runtime(error) => {
                write!(
                    formatter,
                    "Native application-port actor runtime could not start: {error}"
                )
            }
            Self::ActorThread(error) => {
                write!(
                    formatter,
                    "Native application-port actor could not start: {error}"
                )
            }
        }
    }
}

impl Error for NativeMemoryApplicationPortBuildError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Descriptor(error) => Some(error),
            Self::Runtime(error) => Some(error),
            Self::ActorThread(error) => Some(error),
        }
    }
}

/// The project-owned Native application port used by product composition.
pub(crate) struct ProjectNativeMemoryApplicationPort {
    descriptor: ProviderDescriptor,
    actor: NativeReadActor,
}

/// Builds the project-owned Native application port behind the provider
/// registry's neutral trait object.
pub(crate) fn project_native_memory_application_port(
    cg: Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
    project_root: PathBuf,
) -> Result<Arc<dyn NativeMemoryApplicationPort>, NativeMemoryApplicationPortBuildError> {
    Ok(Arc::new(ProjectNativeMemoryApplicationPort::new(
        cg,
        project_root,
    )?))
}

impl ProjectNativeMemoryApplicationPort {
    /// Creates one bounded actor-backed port over the live project graph cell.
    pub(crate) fn new(
        cg: Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
        project_root: PathBuf,
    ) -> Result<Self, NativeMemoryApplicationPortBuildError> {
        let descriptor =
            native_descriptor().map_err(NativeMemoryApplicationPortBuildError::Descriptor)?;
        let actor = NativeReadActor::new(cg, project_root)?;
        Ok(Self { descriptor, actor })
    }

    fn success_reply(&self, call: &ProviderCall) -> ProviderReply {
        ProviderReply {
            terminal: terminal_for_call(call, TerminalCode::Success, None),
            payload: Some(call.payload.clone()),
            warnings: Vec::new(),
            extensions: call.extensions.clone(),
            state_generation: call.expected_state_generation,
        }
    }

    fn unavailable_reply(&self, call: &ProviderCall, diagnostic: &'static str) -> ProviderReply {
        ProviderReply {
            terminal: terminal_for_call(call, TerminalCode::ProviderUnavailable, Some(diagnostic)),
            payload: None,
            warnings: Vec::new(),
            extensions: Vec::new(),
            state_generation: call.expected_state_generation,
        }
    }

    fn handshake_failure(
        &self,
        request: &HandshakeRequest,
        code: TerminalCode,
        diagnostic: &'static str,
    ) -> HandshakeResponse {
        HandshakeResponse {
            terminal: TerminalRecord::failure_before_dispatch(
                ProviderOperation::Handshake,
                self.descriptor.provider_id.clone(),
                code,
                &request.request_id,
                request_scope_digest(request),
                None,
                diagnostic,
            ),
            descriptor: None,
            provider_instance_id: None,
            state_namespace: None,
            accepted_scope: None,
            effective_limits: None,
            ready_receipt_sha256: None,
            warnings: Vec::new(),
        }
    }

    fn observe_invalid(&self, call: &ProviderCall) -> ProviderReply {
        ProviderReply {
            terminal: terminal_for_call(
                call,
                TerminalCode::InvalidRequest,
                Some(INVALID_PAYLOAD_DIAGNOSTIC),
            ),
            payload: None,
            warnings: Vec::new(),
            extensions: Vec::new(),
            state_generation: call.expected_state_generation,
        }
    }

    fn observe_failure(&self, call: &ProviderCall, failure: NativeReadFailure) -> ProviderReply {
        let (code, diagnostic) = failure.terminal();
        ProviderReply {
            terminal: terminal_for_call(call, code, Some(diagnostic)),
            payload: None,
            warnings: Vec::new(),
            extensions: Vec::new(),
            state_generation: call.expected_state_generation,
        }
    }
}

impl NativeMemoryApplicationPort for ProjectNativeMemoryApplicationPort {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        if request.validate().is_err() {
            return self.handshake_failure(
                request,
                TerminalCode::InvalidRequest,
                "native.handshake_request_invalid",
            );
        }
        if request.provider_id.as_str() != NATIVE_PROVIDER_ID {
            return self.handshake_failure(
                request,
                TerminalCode::InvalidRequest,
                "native.provider_id_mismatch",
            );
        }
        if request
            .required_capabilities
            .iter()
            .any(|capability| !self.descriptor.supports(capability.as_str()))
        {
            return self.handshake_failure(
                request,
                TerminalCode::CapabilityUnsupported,
                "native.required_capability_missing",
            );
        }
        let effective_limits = request.host_limits.minimum(self.descriptor.limits);
        if let Err(code) = request.control.snapshot() {
            return self.handshake_failure(
                request,
                code,
                "native.handshake_request_control_terminal",
            );
        }
        let terminal = match TerminalRecord::new(
            ProviderOperation::Handshake,
            self.descriptor.provider_id.clone(),
            TerminalCode::Success,
            CommittedEffectEvidence::none(Some(self.descriptor.state_generation)),
            FallbackDirective::forbidden(),
            request.request_id.clone(),
            request.exact_scope.exact_scope_sha256(),
            None,
        ) {
            Ok(terminal) => terminal,
            Err(_) => {
                return self.handshake_failure(
                    request,
                    TerminalCode::ContractViolation,
                    "native.handshake_terminal_invalid",
                );
            }
        };
        HandshakeResponse {
            terminal,
            descriptor: Some(self.descriptor.clone()),
            provider_instance_id: Some(PROVIDER_INSTANCE_ID.to_owned()),
            state_namespace: Some(STATE_NAMESPACE.to_owned()),
            accepted_scope: Some(request.exact_scope.clone()),
            effective_limits: Some(effective_limits),
            ready_receipt_sha256: Some(ready_receipt(request, effective_limits)),
            warnings: Vec::new(),
        }
    }

    fn health(&self, call: &ProviderCall) -> ProviderReply {
        if let Err(failure) = control_failure(&call.control) {
            return self.observe_failure(call, failure);
        }
        self.success_reply(call)
    }

    fn observe(&self, observation: NativeObservation<'_>) -> ProviderReply {
        let call = observation.call;
        if let Err(failure) = control_failure(&call.control) {
            return self.observe_failure(call, failure);
        }
        if call.validate().is_err() || !observation_matches_call(&observation) {
            return self.observe_invalid(call);
        }
        let payload = match parse_settled_native_fact(&observation) {
            Ok(payload) => payload,
            Err(failure) => return self.observe_failure(call, failure),
        };
        let outcome = self
            .actor
            .dispatch(call.clone(), payload.fact, payload.commit);
        match outcome {
            NativeReadOutcome::Verified => self.success_reply(call),
            NativeReadOutcome::Failed(failure) => self.observe_failure(call, failure),
        }
    }

    fn recall(&self, call: &ProviderCall) -> ProviderReply {
        self.unavailable_reply(call, "native.recall_unimplemented")
    }

    fn feedback(&self, call: &ProviderCall) -> ProviderReply {
        self.unavailable_reply(call, "native.feedback_unimplemented")
    }

    fn maintenance(&self, call: &ProviderCall) -> ProviderReply {
        self.unavailable_reply(call, "native.maintenance_unimplemented")
    }

    fn inspection(&self, call: &ProviderCall) -> ProviderReply {
        self.unavailable_reply(call, "native.inspection_unimplemented")
    }

    fn correction(&self, call: &ProviderCall) -> ProviderReply {
        self.unavailable_reply(call, "native.correction_unimplemented")
    }

    fn delete_by_source(&self, call: &ProviderCall) -> ProviderReply {
        self.unavailable_reply(call, "native.delete_by_source_unimplemented")
    }

    fn snapshot_export(&self, call: &ProviderCall) -> ProviderReply {
        self.unavailable_reply(call, "native.snapshot_export_unimplemented")
    }

    fn snapshot_restore(&self, call: &ProviderCall) -> ProviderReply {
        self.unavailable_reply(call, "native.snapshot_restore_unimplemented")
    }

    fn replay(&self, call: &ProviderCall) -> ProviderReply {
        self.unavailable_reply(call, "native.replay_unimplemented")
    }
}

fn native_descriptor() -> Result<ProviderDescriptor, ApiError> {
    let provider_id = OwnedProviderId::new(NATIVE_PROVIDER_ID)?;
    // `ProviderDescriptor` requires the mandatory recall capability even
    // while the 0401 application port deliberately keeps the recall method
    // unavailable for the later 0402 slice. No optional capability is
    // advertised here.
    let capabilities = [
        OwnedVersionedId::new("provider.health.v1")?,
        OwnedVersionedId::new("observation.accept.v1")?,
        OwnedVersionedId::new("recall.query.v1")?,
    ];
    ProviderDescriptor::new(
        provider_id,
        IMPLEMENTATION_IDENTITY_SHA256,
        STATE_SCHEMA_VERSION,
        0,
        capabilities,
        ProviderLimits {
            request_bytes: 4_096,
            response_bytes: 8_192,
            observation_batch_items: 16,
            recall_candidates: 32,
            concurrent_operations: 4,
            operation_millis: NATIVE_OPERATION_MILLIS,
            snapshot_bytes: 65_536,
            inspection_items: 64,
        },
    )
}

fn request_scope_digest(request: &HandshakeRequest) -> String {
    if request.exact_scope.validate().is_ok() {
        request.exact_scope.exact_scope_sha256()
    } else {
        String::new()
    }
}

fn call_scope_digest(call: &ProviderCall) -> String {
    if call.exact_scope.validate().is_ok() {
        call.exact_scope.exact_scope_sha256()
    } else {
        String::new()
    }
}

fn terminal_for_call(
    call: &ProviderCall,
    code: TerminalCode,
    diagnostic: Option<&'static str>,
) -> TerminalRecord {
    if code == TerminalCode::Success {
        if let Ok(terminal) = TerminalRecord::new(
            call.operation,
            call.provider_id.clone(),
            code,
            CommittedEffectEvidence::none(Some(call.expected_state_generation)),
            FallbackDirective::forbidden(),
            call.operation_id.clone(),
            call_scope_digest(call),
            None,
        ) {
            return terminal;
        }
        return TerminalRecord::failure_before_dispatch(
            call.operation,
            call.provider_id.clone(),
            TerminalCode::InternalFailure,
            &call.operation_id,
            call_scope_digest(call),
            Some(call.expected_state_generation),
            "native.success_terminal_invalid",
        );
    }
    TerminalRecord::failure_before_dispatch(
        call.operation,
        call.provider_id.clone(),
        code,
        &call.operation_id,
        call_scope_digest(call),
        Some(call.expected_state_generation),
        diagnostic.unwrap_or(PROVIDER_UNAVAILABLE_DIAGNOSTIC),
    )
}

fn ready_receipt(request: &HandshakeRequest, effective_limits: ProviderLimits) -> String {
    let mut digest = Sha256::new();
    digest.update(READY_RECEIPT_DOMAIN);
    digest.update(request.challenge_nonce);
    digest.update(request.registration_revision.to_be_bytes());
    digest.update(request.exact_scope.exact_scope_sha256().as_bytes());
    digest.update(request.request_id.as_bytes());
    digest.update(self_descriptor_identity());
    digest.update(effective_limits.request_bytes.to_be_bytes());
    digest.update(effective_limits.response_bytes.to_be_bytes());
    digest.update(effective_limits.observation_batch_items.to_be_bytes());
    digest.update(effective_limits.recall_candidates.to_be_bytes());
    digest.update(effective_limits.concurrent_operations.to_be_bytes());
    digest.update(effective_limits.operation_millis.to_be_bytes());
    digest.update(effective_limits.snapshot_bytes.to_be_bytes());
    digest.update(effective_limits.inspection_items.to_be_bytes());
    hex::encode(digest.finalize())
}

fn self_descriptor_identity() -> &'static [u8] {
    IMPLEMENTATION_IDENTITY_SHA256.as_bytes()
}

fn observation_matches_call(observation: &NativeObservation<'_>) -> bool {
    let call = observation.call;
    if call.operation != ProviderOperation::Observe
        || call.provider_id.as_str() != NATIVE_PROVIDER_ID
        || call.payload.contract_id.as_str() != OBSERVATION_CONTRACT_ID
        || observation.observation_kind != NATIVE_FACT_PROMOTION_OBSERVATION_KIND
        || observation.payload_contract != NATIVE_FACT_PROMOTION_PAYLOAD_CONTRACT_ID
    {
        return false;
    }
    let Ok(envelope) = serde_json::from_slice::<Value>(&call.payload.bytes) else {
        return false;
    };
    let Some(object) = envelope.as_object() else {
        return false;
    };
    object.len() == 3
        && object.get("observation_kind")
            == Some(&Value::String(observation.observation_kind.clone()))
        && object.get("payload_contract")
            == Some(&Value::String(observation.payload_contract.clone()))
        && object.get("canonical_payload") == Some(&observation.canonical_payload)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SettledNativeFactWriteV1 {
    kind: String,
    fact: FactV1,
    commit: FactCommitReceiptV1,
}

fn parse_settled_native_fact(
    observation: &NativeObservation<'_>,
) -> Result<SettledNativeFactWriteV1, NativeReadFailure> {
    let payload =
        serde_json::from_value::<SettledNativeFactWriteV1>(observation.canonical_payload.clone())
            .map_err(|_| NativeReadFailure::InvalidPayload)?;
    if payload.kind != "settled_native_fact_write" {
        return Err(NativeReadFailure::InvalidPayload);
    }
    validate_settled_native_fact(observation.call, &payload)?;
    Ok(payload)
}

fn validate_settled_native_fact(
    call: &ProviderCall,
    payload: &SettledNativeFactWriteV1,
) -> Result<(), NativeReadFailure> {
    let project_id = ProjectId::new(call.exact_scope.project_id.clone())
        .map_err(|_| NativeReadFailure::ScopeUnavailable)?;
    let domain_owner = FactOwnerV1::Project {
        project_id: project_id.clone(),
    };
    let public_owner = FactCommitOwnerV1::Project { project_id };
    if payload.fact.owner != public_owner || payload.commit.owner != public_owner {
        return Err(NativeReadFailure::ScopeUnavailable);
    }
    if payload.fact.fact_id.validate_owner(&domain_owner).is_err()
        || payload.fact.fact_id != payload.commit.fact_id
        || payload.commit.committed_event_ids.is_empty()
        || payload.commit.committed_event_ids.last() != Some(&payload.commit.last_event_id)
        || payload.commit.committed_event_ids.last() != Some(&payload.fact.last_event_id)
        || payload.fact.last_event_id != payload.commit.last_event_id
        || payload.commit.active_assertion_id.as_ref() != Some(&payload.fact.active_assertion_id)
        || payload.fact.telemetry.updated_at != payload.fact.projected_as_of
    {
        return Err(NativeReadFailure::PromotionMismatch);
    }
    let mut event_ids = BTreeSet::new();
    if payload
        .commit
        .committed_event_ids
        .iter()
        .any(|event_id| !event_ids.insert(event_id))
    {
        return Err(NativeReadFailure::PromotionMismatch);
    }
    Ok(())
}

fn control_failure(control: &OperationControl) -> Result<(), NativeReadFailure> {
    control.snapshot().map(|_| ()).map_err(|code| match code {
        TerminalCode::Cancelled => NativeReadFailure::Cancelled,
        TerminalCode::DeadlineExceeded => NativeReadFailure::DeadlineExceeded,
        _ => NativeReadFailure::ProviderUnavailable,
    })
}

#[derive(Clone, Copy, Debug)]
enum NativeReadFailure {
    InvalidPayload,
    PromotionMismatch,
    ScopeUnavailable,
    ProviderUnavailable,
    Cancelled,
    DeadlineExceeded,
}

impl NativeReadFailure {
    fn terminal(self) -> (TerminalCode, &'static str) {
        match self {
            Self::InvalidPayload => (TerminalCode::InvalidRequest, INVALID_PAYLOAD_DIAGNOSTIC),
            Self::PromotionMismatch => (
                TerminalCode::ContractViolation,
                PROMOTION_MISMATCH_DIAGNOSTIC,
            ),
            Self::ScopeUnavailable => {
                (TerminalCode::ScopeUnavailable, SCOPE_UNAVAILABLE_DIAGNOSTIC)
            }
            Self::ProviderUnavailable => (
                TerminalCode::ProviderUnavailable,
                PROVIDER_UNAVAILABLE_DIAGNOSTIC,
            ),
            Self::Cancelled => (TerminalCode::Cancelled, CANCELLED_DIAGNOSTIC),
            Self::DeadlineExceeded => (TerminalCode::DeadlineExceeded, DEADLINE_DIAGNOSTIC),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum NativeReadOutcome {
    Verified,
    Failed(NativeReadFailure),
}

struct NativeReadCommand {
    call: ProviderCall,
    fact: FactV1,
    commit: FactCommitReceiptV1,
    reply: SyncSender<NativeReadOutcome>,
}

struct NativeReadActor {
    sender: Mutex<Option<SyncSender<NativeReadCommand>>>,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl NativeReadActor {
    fn new(
        cg: Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
        project_root: PathBuf,
    ) -> Result<Self, NativeMemoryApplicationPortBuildError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(NativeMemoryApplicationPortBuildError::Runtime)?;
        let (sender, receiver) = mpsc::sync_channel(1);
        let join = thread::Builder::new()
            .name(ACTOR_THREAD_NAME.to_owned())
            .spawn(move || native_read_actor_main(receiver, cg, project_root, runtime))
            .map_err(NativeMemoryApplicationPortBuildError::ActorThread)?;
        Ok(Self {
            sender: Mutex::new(Some(sender)),
            join: Mutex::new(Some(join)),
        })
    }

    fn dispatch(
        &self,
        call: ProviderCall,
        fact: FactV1,
        commit: FactCommitReceiptV1,
    ) -> NativeReadOutcome {
        let (reply, receiver) = mpsc::sync_channel(1);
        let control = call.control.clone();
        let command = NativeReadCommand {
            call,
            fact,
            commit,
            reply,
        };
        let sender = match self.sender.lock() {
            Ok(sender) => sender.as_ref().cloned(),
            Err(_) => None,
        };
        let Some(sender) = sender else {
            return NativeReadOutcome::Failed(NativeReadFailure::ProviderUnavailable);
        };
        match sender.try_send(command) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                return NativeReadOutcome::Failed(NativeReadFailure::ProviderUnavailable);
            }
        }
        loop {
            let snapshot = match control.snapshot() {
                Ok(snapshot) => snapshot,
                Err(code) => {
                    return NativeReadOutcome::Failed(match code {
                        TerminalCode::Cancelled => NativeReadFailure::Cancelled,
                        TerminalCode::DeadlineExceeded => NativeReadFailure::DeadlineExceeded,
                        _ => NativeReadFailure::ProviderUnavailable,
                    });
                }
            };
            let wait_millis = snapshot.remaining_millis.min(ACTOR_POLL_MILLIS).max(1);
            match receiver.recv_timeout(Duration::from_millis(wait_millis)) {
                Ok(outcome) => return outcome,
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    return NativeReadOutcome::Failed(NativeReadFailure::ProviderUnavailable);
                }
            }
        }
    }
}

impl Drop for NativeReadActor {
    fn drop(&mut self) {
        let sender = match self.sender.lock() {
            Ok(mut sender) => sender.take(),
            Err(error) => error.into_inner().take(),
        };
        drop(sender);
        let join = match self.join.lock() {
            Ok(mut join) => join.take(),
            Err(error) => error.into_inner().take(),
        };
        if let Some(join) = join {
            let _ = join.join();
        }
    }
}

fn native_read_actor_main(
    receiver: mpsc::Receiver<NativeReadCommand>,
    cg: Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
    project_root: PathBuf,
    runtime: tokio::runtime::Runtime,
) {
    while let Ok(command) = receiver.recv() {
        let NativeReadCommand {
            call,
            fact,
            commit,
            reply,
        } = command;
        let outcome = verify_with_runtime(&runtime, &cg, &project_root, call, fact, commit);
        let _ = reply.send(outcome);
    }
}

fn verify_with_runtime(
    runtime: &tokio::runtime::Runtime,
    cg: &Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
    project_root: &Path,
    call: ProviderCall,
    fact: FactV1,
    commit: FactCommitReceiptV1,
) -> NativeReadOutcome {
    let snapshot = match call.control.snapshot() {
        Ok(snapshot) => snapshot,
        Err(code) => {
            return NativeReadOutcome::Failed(match code {
                TerminalCode::Cancelled => NativeReadFailure::Cancelled,
                TerminalCode::DeadlineExceeded => NativeReadFailure::DeadlineExceeded,
                _ => NativeReadFailure::ProviderUnavailable,
            });
        }
    };
    let timeout_millis = snapshot.remaining_millis.min(NATIVE_OPERATION_MILLIS);
    match runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_millis(timeout_millis),
            verify_current_fact(cg, project_root, &call, &fact, &commit),
        )
        .await
    }) {
        Ok(outcome) => outcome,
        Err(_) => NativeReadOutcome::Failed(NativeReadFailure::DeadlineExceeded),
    }
}

async fn verify_current_fact(
    cg: &Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
    project_root: &Path,
    call: &ProviderCall,
    expected_fact: &FactV1,
    commit: &FactCommitReceiptV1,
) -> NativeReadOutcome {
    if let Err(failure) = control_failure(&call.control) {
        return NativeReadOutcome::Failed(failure);
    }
    let project_id = match ProjectId::new(call.exact_scope.project_id.clone()) {
        Ok(project_id) => project_id,
        Err(_) => return NativeReadOutcome::Failed(NativeReadFailure::ScopeUnavailable),
    };
    let current = Arc::clone(&*cg.read().await);
    if let Err(failure) = control_failure(&call.control) {
        return NativeReadOutcome::Failed(failure);
    }
    let target = match open_project_retained_memory_target(
        &current,
        project_root,
        &project_id,
        Some(MemoryScopeV1::Project),
        None,
        MemoryTargetAccessV1::Read,
    )
    .await
    {
        Ok(target) => target,
        Err(error) => return NativeReadOutcome::Failed(map_retained_error(error)),
    };
    let memory = match memory_application(target.database(), target.owner().clone()) {
        Ok(memory) => memory,
        Err(_) => return NativeReadOutcome::Failed(NativeReadFailure::ProviderUnavailable),
    };
    let fact_id =
        match ProjectMemoryFactIdV1::new(target.owner().clone(), expected_fact.fact_id.clone()) {
            Ok(fact_id) => fact_id,
            Err(_) => return NativeReadOutcome::Failed(NativeReadFailure::PromotionMismatch),
        };
    let read_control = native_fact_read_control(&call.control);
    let history_query = match ProjectMemoryFactHistoryQueryV1::new(
        fact_id.clone(),
        None,
        MAX_NATIVE_FACT_LINEAGE,
    ) {
        Ok(query) => query,
        Err(_) => return NativeReadOutcome::Failed(NativeReadFailure::ProviderUnavailable),
    };
    let history = match memory
        .get_project_memory_history(history_query, &read_control)
        .await
    {
        Ok(history) => history,
        Err(error) => {
            return NativeReadOutcome::Failed(map_retained_error(
                memory_mapping::map_memory_error(error),
            ));
        }
    };
    if !receipt_matches_authoritative_history(&history, target.owner(), expected_fact, commit) {
        return NativeReadOutcome::Failed(NativeReadFailure::PromotionMismatch);
    }
    let projection = match memory.get_project_memory_fact(fact_id, &read_control).await {
        Ok(Some(projection)) => projection,
        Ok(None) => return NativeReadOutcome::Failed(NativeReadFailure::PromotionMismatch),
        Err(error) => {
            return NativeReadOutcome::Failed(map_retained_error(
                memory_mapping::map_memory_error(error),
            ));
        }
    };
    let public = match memory_mapping::projection(&projection) {
        Ok(public) => public,
        Err(error) => return NativeReadOutcome::Failed(map_retained_error(error)),
    };
    let FactProjectionV1::Available { fact } = public else {
        return NativeReadOutcome::Failed(NativeReadFailure::PromotionMismatch);
    };
    if *fact == *expected_fact {
        if let Err(failure) = control_failure(&call.control) {
            return NativeReadOutcome::Failed(failure);
        }
        NativeReadOutcome::Verified
    } else {
        NativeReadOutcome::Failed(NativeReadFailure::PromotionMismatch)
    }
}

const MAX_NATIVE_FACT_LINEAGE: usize = 1_000;

fn receipt_matches_authoritative_history(
    history: &ProjectMemoryFactHistoryV1,
    authoritative_owner: &FactOwnerV1,
    expected_fact: &FactV1,
    expected_commit: &FactCommitReceiptV1,
) -> bool {
    if history.owner() != authoritative_owner
        || !public_owner_matches(authoritative_owner, &expected_commit.owner)
        || history.fact_id() != &expected_commit.fact_id
        || expected_commit.committed_event_ids.is_empty()
        || expected_commit.committed_event_ids.last() != Some(&expected_commit.last_event_id)
        || expected_commit.committed_event_ids.last() != Some(&expected_fact.last_event_id)
        || expected_commit.active_assertion_id.as_ref() != Some(&expected_fact.active_assertion_id)
    {
        return false;
    }
    let history_event_ids = history
        .events()
        .iter()
        .map(|event| event.event_id())
        .collect::<Vec<_>>();
    let Some(start) = history_event_ids
        .len()
        .checked_sub(expected_commit.committed_event_ids.len())
    else {
        return false;
    };
    history_event_ids[start..]
        .iter()
        .copied()
        .eq(expected_commit.committed_event_ids.iter())
        && history
            .events()
            .last()
            .is_some_and(|event| event.event_id() == &expected_fact.last_event_id)
        && history.next_after().is_none()
}

fn public_owner_matches(
    authoritative_owner: &FactOwnerV1,
    public_owner: &FactCommitOwnerV1,
) -> bool {
    match (authoritative_owner, public_owner) {
        (FactOwnerV1::Profile, FactCommitOwnerV1::Profile) => true,
        (
            FactOwnerV1::Project {
                project_id: authoritative_project_id,
            },
            FactCommitOwnerV1::Project { project_id },
        ) => authoritative_project_id == project_id,
        _ => false,
    }
}

fn native_fact_read_control(control: &OperationControl) -> FactReadControl {
    let control = control.clone();
    FactReadControl::new(Arc::new(move || control.snapshot().is_err()))
}

fn map_retained_error(error: RetainedSurfaceExecutionErrorV1) -> NativeReadFailure {
    match error {
        RetainedSurfaceExecutionErrorV1::Cancelled(_) => NativeReadFailure::Cancelled,
        RetainedSurfaceExecutionErrorV1::TimedOut(_) => NativeReadFailure::DeadlineExceeded,
        RetainedSurfaceExecutionErrorV1::NotFoundOrNotAuthorized => {
            NativeReadFailure::ScopeUnavailable
        }
        RetainedSurfaceExecutionErrorV1::Conflict => NativeReadFailure::PromotionMismatch,
        RetainedSurfaceExecutionErrorV1::InvalidRequest => NativeReadFailure::InvalidPayload,
        RetainedSurfaceExecutionErrorV1::ApplicationProblem(_)
        | RetainedSurfaceExecutionErrorV1::StructuralRefusal(_)
        | RetainedSurfaceExecutionErrorV1::PartialEffect { .. }
        | RetainedSurfaceExecutionErrorV1::Stale
        | RetainedSurfaceExecutionErrorV1::Unsupported
        | RetainedSurfaceExecutionErrorV1::Saturated
        | RetainedSurfaceExecutionErrorV1::Unavailable
        | RetainedSurfaceExecutionErrorV1::ProfileResetRequired
        | RetainedSurfaceExecutionErrorV1::ProjectResetRequired => {
            NativeReadFailure::ProviderUnavailable
        }
    }
}
