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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tracedecay_contracts::retained_surfaces::{
    FactCommitOwnerV1, FactCommitReceiptV1, FactIdentitySourceResultV1, FactProjectionV1,
    FactSearchGraphCoverageV1, FactV1, MemoryScopeV1,
};
use tracedecay_contracts::{
    CancellationContext, CancellationSignal, CapabilityGrantId, CapabilityGrantSnapshot, Deadline,
    DisclosureClass, RequestContext, RequestId, RetainedSurfaceExecutionErrorV1, now_micros,
};
use tracedecay_domain::{
    ActorId, FactOwnerV1, ProjectId, RefId, RepositoryId, RetrievalGrainV1, SessionId,
    TemporalModeV1, UtcMicros, WorktreeId, canonical_sha256,
};
use tracedecay_memory_provider_registry::{
    ApiError, CanonicalPayload, CommittedEffectEvidence, FallbackDirective, HandshakeRequest,
    HandshakeResponse, NATIVE_FACT_PROMOTION_OBSERVATION_KIND,
    NATIVE_FACT_PROMOTION_PAYLOAD_CONTRACT_ID, NATIVE_PROVIDER_ID,
    NATIVE_STAGED_SESSION_OBSERVATION_KIND, NATIVE_STAGED_SESSION_PAYLOAD_CONTRACT_ID,
    NativeMemoryApplicationPort, NativeObservation, OBSERVATION_CONTRACT_ID, OperationControl,
    OwnedProviderId, OwnedVersionedId, ProviderCall, ProviderDescriptor, ProviderLimits,
    ProviderOperation, ProviderReply, TerminalCode, TerminalRecord,
};
use tracedecay_store::{
    FactReadControl, ProjectMemoryFactHistoryQueryV1, ProjectMemoryFactHistoryV1,
    ProjectMemoryFactIdV1, ProjectMemoryFactSearchKindV1, ProjectMemoryFactSearchPageV1,
    ProjectMemoryFactSearchQuery,
};
use tracedecay_temporal_query::context::ContextBudget;
use tracedecay_temporal_query::ranking::DiversityLimits;

use super::memory_mapping;
use super::native_authority::NativeSessionRetrievalMountV1;
use super::native_session_recall::{
    NativeSessionRecallBatch, NativeSessionRecallBatchStatus, NativeSessionRecallLimits,
    NativeSessionRecallOptions, NativeSessionRecallTemporal, NativeSessionRecallUnavailable,
    retrieve_native_session_recall_with_cancellation,
};
use super::open_project_retained_memory_target;
use tracedecay_project::project::TraceDecay;
use tracedecay_session_memory::fact_store::DatabaseFactStore;
use tracedecay_session_memory::memory::MemoryApplication;
use tracedecay_store_runtime::retained_memory::MemoryTargetAccessV1;

#[cfg(test)]
#[path = "native_baseline_tests.rs"]
mod baseline_tests;
#[cfg(test)]
#[path = "native_provider_tests.rs"]
mod tests;

pub(super) const IMPLEMENTATION_IDENTITY_SHA256: &str =
    "7fe6923361d4caa6c213e0760d438c9f3b9bda60d4c1195812130bfe66c2fa16";
pub(super) const STATE_SCHEMA_VERSION: &str = "native-application-port-v1";
pub(crate) const PROVIDER_INSTANCE_ID: &str = "tracedecay.native.project";
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
const RECALL_INVALID_DIAGNOSTIC: &str = "native.recall_request_invalid";
const RECALL_UNSUPPORTED_DIAGNOSTIC: &str = "native.recall_semantics_unsupported";
const RECALL_SCOPE_MISMATCH_DIAGNOSTIC: &str = "native.recall_scope_mismatch";
const RECALL_EXTENSION_DIAGNOSTIC: &str = "native.recall_extension_unsupported";
const RECALL_PROJECTION_DIAGNOSTIC: &str = "native.recall_projection_invalid";
const RECALL_SCORE_DOMAIN: &str = "tracedecay.native.project-memory.search.v1";
const RECALL_SCORE_DOMAIN_VERSION: u32 = 1;
const RECALL_CONTRACT_ID: &str = "tracedecay.memory.provider.recall.v1";

/// Construction failures for the project-owned Native application port.
#[derive(Debug)]
pub(crate) enum NativeMemoryApplicationPortBuildError {
    /// The fixed provider descriptor could not be assembled or validated.
    Descriptor(ApiError),
    /// The bounded actor runtime could not be constructed.
    Runtime(std::io::Error),
    /// The bounded actor thread could not be started.
    ActorThread(std::io::Error),
    /// The bounded actor construction task could not be joined.
    BlockingJoin(String),
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
            Self::BlockingJoin(error) => {
                write!(
                    formatter,
                    "Native application-port construction task could not be joined: {error}"
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
            Self::BlockingJoin(_) => None,
        }
    }
}

/// The project-owned Native application port used by product composition.
pub(crate) struct ProjectNativeMemoryApplicationPort {
    descriptor: ProviderDescriptor,
    actor: NativeReadActor,
    session_retrieval: Arc<NativeSessionRetrievalMountV1>,
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

/// Builds a Native port with the late-bound canonical session authority that
/// project composition installs after the session database is admitted.
pub(crate) fn project_native_memory_application_port_with_session_retrieval(
    cg: Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
    project_root: PathBuf,
    session_retrieval: Arc<NativeSessionRetrievalMountV1>,
) -> Result<Arc<dyn NativeMemoryApplicationPort>, NativeMemoryApplicationPortBuildError> {
    Ok(Arc::new(
        ProjectNativeMemoryApplicationPort::new_with_session_retrieval(
            cg,
            project_root,
            session_retrieval,
        )?,
    ))
}

/// Builds the Native port from an async composition context without retaining
/// a provider-owned state root. Construction only starts the bounded read
/// actor; all durable observation and recall state remains host canonical.
pub(crate) async fn project_native_memory_application_port_off_runtime(
    cg: Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
    project_root: PathBuf,
) -> Result<Arc<dyn NativeMemoryApplicationPort>, NativeMemoryApplicationPortBuildError> {
    tokio::task::spawn_blocking(move || project_native_memory_application_port(cg, project_root))
        .await
        .map_err(|error| NativeMemoryApplicationPortBuildError::BlockingJoin(error.to_string()))?
}

/// Async composition variant retaining the host's canonical session mount.
pub(crate) async fn project_native_memory_application_port_off_runtime_with_session_retrieval(
    cg: Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
    project_root: PathBuf,
    session_retrieval: Arc<NativeSessionRetrievalMountV1>,
) -> Result<Arc<dyn NativeMemoryApplicationPort>, NativeMemoryApplicationPortBuildError> {
    tokio::task::spawn_blocking(move || {
        project_native_memory_application_port_with_session_retrieval(
            cg,
            project_root,
            session_retrieval,
        )
    })
    .await
    .map_err(|error| NativeMemoryApplicationPortBuildError::BlockingJoin(error.to_string()))?
}

impl ProjectNativeMemoryApplicationPort {
    /// Creates one bounded actor-backed port over the live project graph cell.
    pub(crate) fn new(
        cg: Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
        project_root: PathBuf,
    ) -> Result<Self, NativeMemoryApplicationPortBuildError> {
        Self::new_with_session_retrieval(
            cg,
            project_root,
            Arc::new(NativeSessionRetrievalMountV1::default()),
        )
    }

    /// Creates one actor-backed Native port over a late-bound canonical
    /// session retrieval mount.
    pub(crate) fn new_with_session_retrieval(
        cg: Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
        project_root: PathBuf,
        session_retrieval: Arc<NativeSessionRetrievalMountV1>,
    ) -> Result<Self, NativeMemoryApplicationPortBuildError> {
        let descriptor =
            native_descriptor().map_err(NativeMemoryApplicationPortBuildError::Descriptor)?;
        let actor = NativeReadActor::new(cg, project_root, Arc::clone(&session_retrieval))?;
        Ok(Self {
            descriptor,
            actor,
            session_retrieval,
        })
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
        let call = observation.call();
        if let Err(failure) = control_failure(&call.control) {
            return self.observe_failure(call, failure);
        }
        if call.validate().is_err() || !observation_matches_call(&observation) {
            return self.observe_invalid(call);
        }
        match observation {
            // Fact promotion remains a separate, explicitly authorized
            // consequence. It is the only observation kind that enters the
            // Native fact verification actor.
            NativeObservation::FactPromotion(_) => {
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
            // Session messages are already admitted and sanitized by the host
            // observation journey. Native acknowledges that canonical
            // delivery statelessly; it owns no staging table, journal, or
            // receipt. The host records the resulting acknowledgement in its
            // own delivery journal.
            NativeObservation::StagedSession(_) => self.success_reply(call),
        }
    }

    fn recall(&self, call: &ProviderCall) -> ProviderReply {
        if let Err(failure) = control_failure(&call.control) {
            return self.observe_failure(call, failure);
        }
        if call.validate().is_err()
            || call.operation != ProviderOperation::Recall
            || call.provider_id.as_str() != NATIVE_PROVIDER_ID
            || call.payload.contract_id.as_str() != RECALL_CONTRACT_ID
        {
            return self.observe_failure(call, NativeReadFailure::RecallInvalidRequest);
        }
        let request = match parse_native_recall_request(call) {
            Ok(request) => request,
            Err(failure) => return self.observe_failure(call, failure),
        };
        match self.actor.dispatch_recall(call.clone(), request) {
            NativeRecallOutcome::Reply(reply) => reply,
            NativeRecallOutcome::Failed(failure) => self.observe_failure(call, failure),
        }
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
    // `ProviderDescriptor` requires the mandatory recall capability. The
    // Native implementation maps it to the owner-bound project-memory read
    // authority below; no optional capability is advertised here.
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
        native_provider_limits(),
    )
}

pub(crate) fn native_provider_limits() -> ProviderLimits {
    ProviderLimits {
        request_bytes: 4_096,
        response_bytes: 8_192,
        observation_batch_items: 16,
        recall_candidates: 32,
        concurrent_operations: 4,
        operation_millis: NATIVE_OPERATION_MILLIS,
        snapshot_bytes: 65_536,
        inspection_items: 64,
    }
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
    if matches!(
        code,
        TerminalCode::Success | TerminalCode::SuccessZeroResults | TerminalCode::Partial
    ) {
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
    let call = observation.call();
    if call.operation != ProviderOperation::Observe
        || call.provider_id.as_str() != NATIVE_PROVIDER_ID
        || call.payload.contract_id.as_str() != OBSERVATION_CONTRACT_ID
        || !matches!(
            (
                observation.observation_kind(),
                observation.payload_contract()
            ),
            (
                NATIVE_FACT_PROMOTION_OBSERVATION_KIND,
                NATIVE_FACT_PROMOTION_PAYLOAD_CONTRACT_ID
            ) | (
                NATIVE_STAGED_SESSION_OBSERVATION_KIND,
                NATIVE_STAGED_SESSION_PAYLOAD_CONTRACT_ID
            )
        )
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
            == Some(&Value::String(observation.observation_kind().to_owned()))
        && object.get("payload_contract")
            == Some(&Value::String(observation.payload_contract().to_owned()))
        && object.get("canonical_payload") == Some(observation.canonical_payload())
}

/// The strict, provider-neutral request envelope understood by the Native
/// application port.  The contract deliberately keeps this wire value
/// provider-neutral; the Native mapping below only accepts the current,
/// owner-bound projection that the retained-memory authority can prove.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeRecallRequestV1 {
    provider_id: String,
    registration_revision: u64,
    ready_receipt_digest: String,
    exact_scope_identity: NativeRecallScopeV1,
    request_identity: String,
    objective: String,
    query: String,
    temporal_query: NativeRecallTemporalQueryV1,
    budgets: NativeRecallBudgetsV1,
    exclusions: NativeRecallExclusionsV1,
    /// The host-admitted history claim is carried through the provider call
    /// so a canonical session projection cannot silently lose its grant.
    #[serde(default)]
    history_grant: Option<Value>,
    required_capabilities: Vec<String>,
    policy_revision: u64,
    extensions: Vec<NativeRecallExtensionV1>,
    deadline: Value,
    cancellation: Value,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct NativeRecallScopeV1 {
    profile_id: String,
    project_id: String,
    repository_identity: String,
    worktree_identity: String,
    branch_identity: String,
    agent_session_id: String,
    resolved_scope_digest: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeRecallTemporalQueryV1 {
    mode: String,
    evaluation_time: String,
    as_of: Value,
    interval_start: Value,
    interval_end: Value,
    include_superseded: bool,
    include_revoked: bool,
    unknown_validity_policy: String,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeRecallBudgetsV1 {
    maximum_candidates: u64,
    maximum_candidate_content_bytes: u64,
    maximum_total_content_bytes: u64,
    maximum_source_refs_per_candidate: u64,
    maximum_trace_refs_per_candidate: u64,
    maximum_warnings: u64,
    maximum_extensions_per_candidate: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeRecallExclusionsV1 {
    stable_memory_refs: Vec<String>,
    candidate_ids: Vec<String>,
    source_refs: Vec<String>,
    trace_refs: Vec<String>,
    observation_ids: Vec<String>,
    content_sha256: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeRecallExtensionV1 {
    extension_id: String,
    extension_version: u32,
    criticality: String,
    canonical_payload: Value,
    payload_sha256: String,
}

fn parse_native_recall_request(
    call: &ProviderCall,
) -> Result<NativeRecallRequestV1, NativeReadFailure> {
    let request = serde_json::from_slice::<NativeRecallRequestV1>(&call.payload.bytes)
        .map_err(|_| NativeReadFailure::RecallInvalidRequest)?;
    if request.provider_id != NATIVE_PROVIDER_ID
        || request.registration_revision != call.registration_revision
        || request.ready_receipt_digest != call.ready_receipt_sha256
        || request.request_identity != call.request_id
        || request.required_capabilities.len() != 1
        || request.required_capabilities[0] != "recall.query.v1"
        || request.policy_revision == 0
    {
        return Err(NativeReadFailure::RecallInvalidRequest);
    }
    if request.exact_scope_identity != native_recall_scope(call) {
        return Err(NativeReadFailure::RecallScopeMismatch);
    }
    validate_recall_text(&request.objective, 8_192)
        .map_err(|_| NativeReadFailure::RecallInvalidRequest)?;
    validate_recall_text(&request.query, 32_768)
        .map_err(|_| NativeReadFailure::RecallInvalidRequest)?;
    validate_recall_temporal(&request.temporal_query)?;
    validate_recall_budgets(&request.budgets)?;
    validate_recall_exclusions(&request.exclusions)?;
    validate_recall_history(call, &request)?;
    validate_recall_extensions(&request.extensions)?;
    validate_recall_control(call, &request.deadline, &request.cancellation)?;
    Ok(request)
}

fn native_recall_scope(call: &ProviderCall) -> NativeRecallScopeV1 {
    NativeRecallScopeV1 {
        profile_id: call.exact_scope.profile_id.clone(),
        project_id: call.exact_scope.project_id.clone(),
        repository_identity: call.exact_scope.repository_identity.clone(),
        worktree_identity: call.exact_scope.worktree_identity.clone(),
        branch_identity: call.exact_scope.branch_identity.clone(),
        agent_session_id: call.exact_scope.agent_session_id.clone(),
        resolved_scope_digest: call.exact_scope.resolved_scope_digest.clone(),
    }
}

fn validate_recall_text(value: &str, maximum_bytes: usize) -> Result<(), ()> {
    if value.is_empty()
        || value.len() > maximum_bytes
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(());
    }
    Ok(())
}

fn validate_recall_temporal(
    temporal: &NativeRecallTemporalQueryV1,
) -> Result<(), NativeReadFailure> {
    let Some(evaluation_micros) = parse_rfc3339_micros(&temporal.evaluation_time) else {
        return Err(NativeReadFailure::RecallInvalidRequest);
    };
    let now_micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_micros()).ok())
        .unwrap_or(i64::MAX);
    if evaluation_micros > now_micros {
        return Err(NativeReadFailure::RecallInvalidRequest);
    }
    match temporal.mode.as_str() {
        "current" => {
            if !temporal.as_of.is_null()
                || !temporal.interval_start.is_null()
                || !temporal.interval_end.is_null()
            {
                return Err(NativeReadFailure::RecallInvalidRequest);
            }
        }
        "as_of" => {
            let Some(as_of) = temporal.as_of.as_str().and_then(parse_rfc3339_micros) else {
                return Err(NativeReadFailure::RecallInvalidRequest);
            };
            if as_of > evaluation_micros
                || !temporal.interval_start.is_null()
                || !temporal.interval_end.is_null()
            {
                return Err(NativeReadFailure::RecallInvalidRequest);
            }
        }
        "interval" => {
            let Some(start) = temporal
                .interval_start
                .as_str()
                .and_then(parse_rfc3339_micros)
            else {
                return Err(NativeReadFailure::RecallInvalidRequest);
            };
            let Some(end) = temporal
                .interval_end
                .as_str()
                .and_then(parse_rfc3339_micros)
            else {
                return Err(NativeReadFailure::RecallInvalidRequest);
            };
            if start >= end || !temporal.as_of.is_null() {
                return Err(NativeReadFailure::RecallInvalidRequest);
            }
        }
        "history" => {
            if !temporal.as_of.is_null()
                || !temporal.interval_start.is_null()
                || !temporal.interval_end.is_null()
            {
                return Err(NativeReadFailure::RecallInvalidRequest);
            }
        }
        _ => return Err(NativeReadFailure::RecallInvalidRequest),
    }
    if !matches!(
        temporal.unknown_validity_policy.as_str(),
        "exclude" | "degrade" | "allow_with_warning"
    ) {
        return Err(NativeReadFailure::RecallInvalidRequest);
    }
    Ok(())
}

fn validate_recall_history(
    call: &ProviderCall,
    request: &NativeRecallRequestV1,
) -> Result<(), NativeReadFailure> {
    let Some(value) = request.history_grant.as_ref() else {
        return if call.history_grant().is_some() {
            Err(NativeReadFailure::RecallScopeMismatch)
        } else {
            Ok(())
        };
    };
    let grant = super::provider_history::history_grant_from_json(value)
        .map_err(|_| NativeReadFailure::RecallInvalidRequest)?;
    if grant.destination_scope != call.exact_scope
        || grant.policy_revision != request.policy_revision
    {
        return Err(NativeReadFailure::RecallScopeMismatch);
    }
    if let Some(private) = call.history_grant()
        && private != &grant
    {
        return Err(NativeReadFailure::RecallScopeMismatch);
    }
    Ok(())
}

fn parse_rfc3339_micros(value: &str) -> Option<i64> {
    let bytes = value.as_bytes();
    if bytes.len() < 20
        || bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || bytes.get(10) != Some(&b'T')
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
    {
        return None;
    }
    let year = parse_digits(&bytes[0..4])?;
    let month = parse_digits(&bytes[5..7])?;
    let day = parse_digits(&bytes[8..10])?;
    let hour = parse_digits(&bytes[11..13])?;
    let minute = parse_digits(&bytes[14..16])?;
    let second = parse_digits(&bytes[17..19])?;
    if !(1..=12).contains(&month)
        || hour > 23
        || minute > 59
        || second > 59
        || day == 0
        || day > days_in_month(year, month)
    {
        return None;
    }
    let mut index = 19;
    let mut micros = 0_i64;
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        let start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        let fraction = bytes.get(start..index)?;
        if fraction.is_empty() || fraction.len() > 9 {
            return None;
        }
        let mut value = parse_digits(fraction)?;
        for _ in fraction.len()..6 {
            value = value.checked_mul(10)?;
        }
        for _ in 6..fraction.len() {
            value /= 10;
        }
        micros = i64::from(value);
    }
    let offset_minutes = match bytes.get(index..) {
        Some([b'Z']) => 0_i64,
        Some(
            [
                sign,
                hour_tz_tens,
                hour_tz_ones,
                b':',
                minute_tz_tens,
                minute_tz_ones,
            ],
        ) if *sign == b'+' || *sign == b'-' => {
            if !hour_tz_tens.is_ascii_digit()
                || !hour_tz_ones.is_ascii_digit()
                || !minute_tz_tens.is_ascii_digit()
                || !minute_tz_ones.is_ascii_digit()
            {
                return None;
            }
            let hour_tz = i64::from(*hour_tz_tens - b'0') * 10 + i64::from(*hour_tz_ones - b'0');
            let minute_tz =
                i64::from(*minute_tz_tens - b'0') * 10 + i64::from(*minute_tz_ones - b'0');
            if hour_tz > 23 || minute_tz > 59 {
                return None;
            }
            let signed = hour_tz.checked_mul(60)?.checked_add(minute_tz)?;
            if *sign == b'+' { signed } else { -signed }
        }
        _ => return None,
    };
    let days = days_from_civil(year, month, day)?;
    let seconds = days
        .checked_mul(86_400)?
        .checked_add(i64::from(hour).checked_mul(3_600)?)?
        .checked_add(i64::from(minute).checked_mul(60)?)?
        .checked_add(i64::from(second))?
        .checked_sub(offset_minutes.checked_mul(60)?)?;
    seconds.checked_mul(1_000_000)?.checked_add(micros)
}

fn parse_digits(value: &[u8]) -> Option<u32> {
    if value.is_empty() || value.iter().any(|byte| !byte.is_ascii_digit()) {
        return None;
    }
    value.iter().try_fold(0_u32, |accumulator, byte| {
        accumulator
            .checked_mul(10)?
            .checked_add(u32::from(*byte - b'0'))
    })
}

fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        2 if year % 400 == 0 || year % 4 == 0 && year % 100 != 0 => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

fn days_from_civil(year: u32, month: u32, day: u32) -> Option<i64> {
    let year = i64::from(year) - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month = i64::from(month);
    let day = i64::from(day);
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era.checked_mul(146_097)?
        .checked_add(day_of_era)?
        .checked_sub(719_468)
}

fn validate_recall_budgets(budgets: &NativeRecallBudgetsV1) -> Result<(), NativeReadFailure> {
    if [
        budgets.maximum_candidates,
        budgets.maximum_candidate_content_bytes,
        budgets.maximum_total_content_bytes,
        budgets.maximum_source_refs_per_candidate,
        budgets.maximum_trace_refs_per_candidate,
        budgets.maximum_warnings,
        budgets.maximum_extensions_per_candidate,
    ]
    .into_iter()
    .any(|value| value == 0)
    {
        return Err(NativeReadFailure::RecallInvalidRequest);
    }
    Ok(())
}

fn validate_recall_exclusions(
    exclusions: &NativeRecallExclusionsV1,
) -> Result<(), NativeReadFailure> {
    let groups = [
        (&exclusions.stable_memory_refs, "stable_memory_refs"),
        (&exclusions.candidate_ids, "candidate_ids"),
        (&exclusions.source_refs, "source_refs"),
        (&exclusions.trace_refs, "trace_refs"),
        (&exclusions.observation_ids, "observation_ids"),
        (&exclusions.content_sha256, "content_sha256"),
    ];
    for (values, _) in groups {
        if values.len() > 1_024 {
            return Err(NativeReadFailure::RecallInvalidRequest);
        }
        let mut unique = BTreeSet::new();
        if values.iter().any(|value| !unique.insert(value)) {
            return Err(NativeReadFailure::RecallInvalidRequest);
        }
    }
    Ok(())
}

fn validate_recall_extensions(
    extensions: &[NativeRecallExtensionV1],
) -> Result<(), NativeReadFailure> {
    if extensions.len() > 16 {
        return Err(NativeReadFailure::RecallInvalidRequest);
    }
    let mut ids = BTreeSet::new();
    for extension in extensions {
        if extension.extension_version == 0
            || extension.extension_id.is_empty()
            || extension.criticality != "optional" && extension.criticality != "required"
            || !ids.insert((&extension.extension_id, extension.extension_version))
        {
            return Err(NativeReadFailure::RecallInvalidRequest);
        }
        let bytes = serde_json::to_vec(&extension.canonical_payload)
            .map_err(|_| NativeReadFailure::RecallInvalidRequest)?;
        if bytes.is_empty()
            || bytes.len() > 131_072
            || sha256_hex(&bytes) != extension.payload_sha256
        {
            return Err(NativeReadFailure::RecallInvalidRequest);
        }
        if extension.criticality == "required" {
            return Err(NativeReadFailure::RecallExtensionUnsupported);
        }
    }
    Ok(())
}

fn validate_recall_control(
    call: &ProviderCall,
    deadline: &Value,
    cancellation: &Value,
) -> Result<(), NativeReadFailure> {
    let deadline = deadline
        .as_object()
        .ok_or(NativeReadFailure::RecallInvalidRequest)?;
    if deadline.len() != 2
        || deadline
            .keys()
            .any(|key| key != "deadline_utc_micros" && key != "remaining_millis")
    {
        return Err(NativeReadFailure::RecallInvalidRequest);
    }
    let deadline_utc_micros = deadline
        .get("deadline_utc_micros")
        .and_then(Value::as_i64)
        .ok_or(NativeReadFailure::RecallInvalidRequest)?;
    let remaining_millis = deadline
        .get("remaining_millis")
        .and_then(Value::as_u64)
        .ok_or(NativeReadFailure::RecallInvalidRequest)?;
    if deadline_utc_micros != call.control.deadline_utc_micros()
        || remaining_millis > call.control.remaining_millis()
    {
        return Err(NativeReadFailure::RecallInvalidRequest);
    }
    match cancellation {
        Value::String(state) if state == "live" => Ok(()),
        Value::Object(state)
            if state.len() == 1
                && state.get("state") == Some(&Value::String("live".to_owned())) =>
        {
            Ok(())
        }
        _ => Err(NativeReadFailure::RecallInvalidRequest),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
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
        serde_json::from_value::<SettledNativeFactWriteV1>(observation.canonical_payload().clone())
            .map_err(|_| NativeReadFailure::InvalidPayload)?;
    if payload.kind != "settled_native_fact_write" {
        return Err(NativeReadFailure::InvalidPayload);
    }
    validate_settled_native_fact(observation.call(), &payload)?;
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
    RecallInvalidRequest,
    RecallUnsupported,
    RecallScopeMismatch,
    RecallExtensionUnsupported,
    RecallProjectionInvalid,
    RecallBudgetExhausted,
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
            Self::RecallInvalidRequest => (TerminalCode::InvalidRequest, RECALL_INVALID_DIAGNOSTIC),
            Self::RecallUnsupported => (
                TerminalCode::CapabilityUnsupported,
                RECALL_UNSUPPORTED_DIAGNOSTIC,
            ),
            Self::RecallScopeMismatch => (
                TerminalCode::ScopeMismatch,
                RECALL_SCOPE_MISMATCH_DIAGNOSTIC,
            ),
            Self::RecallExtensionUnsupported => (
                TerminalCode::CapabilityUnsupported,
                RECALL_EXTENSION_DIAGNOSTIC,
            ),
            Self::RecallProjectionInvalid => (
                TerminalCode::ContractViolation,
                RECALL_PROJECTION_DIAGNOSTIC,
            ),
            Self::RecallBudgetExhausted => {
                (TerminalCode::CapacityExceeded, RECALL_INVALID_DIAGNOSTIC)
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum NativeReadOutcome {
    Verified,
    Failed(NativeReadFailure),
}

enum NativeRecallOutcome {
    Reply(ProviderReply),
    Failed(NativeReadFailure),
}

enum NativeReadCommand {
    Verify {
        call: ProviderCall,
        fact: FactV1,
        commit: FactCommitReceiptV1,
        reply: SyncSender<NativeReadOutcome>,
    },
    Recall {
        call: ProviderCall,
        request: NativeRecallRequestV1,
        reply: SyncSender<NativeRecallOutcome>,
    },
}

struct NativeReadActor {
    sender: Mutex<Option<SyncSender<NativeReadCommand>>>,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl NativeReadActor {
    fn new(
        cg: Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
        project_root: PathBuf,
        session_retrieval: Arc<NativeSessionRetrievalMountV1>,
    ) -> Result<Self, NativeMemoryApplicationPortBuildError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(NativeMemoryApplicationPortBuildError::Runtime)?;
        let (sender, receiver) = mpsc::sync_channel(1);
        let join = thread::Builder::new()
            .name(ACTOR_THREAD_NAME.to_owned())
            .spawn(move || {
                native_read_actor_main(receiver, cg, project_root, runtime, session_retrieval)
            })
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
        let command = NativeReadCommand::Verify {
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
        match receive_actor_reply(&control, receiver) {
            Ok(outcome) => outcome,
            Err(failure) => NativeReadOutcome::Failed(failure),
        }
    }

    fn dispatch_recall(
        &self,
        call: ProviderCall,
        request: NativeRecallRequestV1,
    ) -> NativeRecallOutcome {
        let (reply, receiver) = mpsc::sync_channel(1);
        let control = call.control.clone();
        let command = NativeReadCommand::Recall {
            call,
            request,
            reply,
        };
        let sender = match self.sender.lock() {
            Ok(sender) => sender.as_ref().cloned(),
            Err(_) => None,
        };
        let Some(sender) = sender else {
            return NativeRecallOutcome::Failed(NativeReadFailure::ProviderUnavailable);
        };
        match sender.try_send(command) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                return NativeRecallOutcome::Failed(NativeReadFailure::ProviderUnavailable);
            }
        }
        match receive_actor_reply(&control, receiver) {
            Ok(outcome) => outcome,
            Err(failure) => NativeRecallOutcome::Failed(failure),
        }
    }
}

fn receive_actor_reply<T>(
    control: &OperationControl,
    receiver: mpsc::Receiver<T>,
) -> Result<T, NativeReadFailure> {
    loop {
        let snapshot = match control.snapshot() {
            Ok(snapshot) => snapshot,
            Err(code) => {
                return Err(match code {
                    TerminalCode::Cancelled => NativeReadFailure::Cancelled,
                    TerminalCode::DeadlineExceeded => NativeReadFailure::DeadlineExceeded,
                    _ => NativeReadFailure::ProviderUnavailable,
                });
            }
        };
        let wait_millis = snapshot.remaining_millis.min(ACTOR_POLL_MILLIS).max(1);
        match receiver.recv_timeout(Duration::from_millis(wait_millis)) {
            Ok(outcome) => return Ok(outcome),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                return Err(NativeReadFailure::ProviderUnavailable);
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
    session_retrieval: Arc<NativeSessionRetrievalMountV1>,
) {
    while let Ok(command) = receiver.recv() {
        match command {
            NativeReadCommand::Verify {
                call,
                fact,
                commit,
                reply,
            } => {
                let outcome = verify_with_runtime(&runtime, &cg, &project_root, call, fact, commit);
                let _ = reply.send(outcome);
            }
            NativeReadCommand::Recall {
                call,
                request,
                reply,
            } => {
                let outcome = recall_with_runtime(
                    &runtime,
                    &cg,
                    &project_root,
                    &session_retrieval,
                    call,
                    request,
                );
                let _ = reply.send(outcome);
            }
        }
    }
}

fn recall_with_runtime(
    runtime: &tokio::runtime::Runtime,
    cg: &Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
    project_root: &Path,
    session_retrieval: &Arc<NativeSessionRetrievalMountV1>,
    call: ProviderCall,
    request: NativeRecallRequestV1,
) -> NativeRecallOutcome {
    let snapshot = match call.control.snapshot() {
        Ok(snapshot) => snapshot,
        Err(code) => {
            return NativeRecallOutcome::Failed(match code {
                TerminalCode::Cancelled => NativeReadFailure::Cancelled,
                TerminalCode::DeadlineExceeded => NativeReadFailure::DeadlineExceeded,
                _ => NativeReadFailure::ProviderUnavailable,
            });
        }
    };
    let timeout_millis = snapshot.remaining_millis.min(NATIVE_OPERATION_MILLIS);
    let session_retrieval = Arc::clone(session_retrieval);
    match runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_millis(timeout_millis),
            recall_canonical_session(cg, project_root, &session_retrieval, &call, &request),
        )
        .await
    }) {
        Ok(outcome) => outcome,
        Err(_) => NativeRecallOutcome::Failed(NativeReadFailure::DeadlineExceeded),
    }
}

/// Reads the host-admitted session projection through the canonical application
/// retrieval service. Native owns only this bounded response projection; it
/// never opens a provider facts database for recall.
async fn recall_canonical_session(
    _cg: &Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
    _project_root: &Path,
    session_retrieval: &NativeSessionRetrievalMountV1,
    call: &ProviderCall,
    request: &NativeRecallRequestV1,
) -> NativeRecallOutcome {
    if let Err(failure) = control_failure(&call.control) {
        return NativeRecallOutcome::Failed(failure);
    }
    if !matches!(
        request.objective.as_str(),
        "search" | "probe" | "related" | "reason"
    ) {
        return NativeRecallOutcome::Failed(NativeReadFailure::RecallUnsupported);
    }

    let (temporal_mode, native_temporal, evaluation_time_micros) =
        match native_session_temporal(&request.temporal_query) {
            Ok(value) => value,
            Err(failure) => return NativeRecallOutcome::Failed(failure),
        };
    let limit = match usize::try_from(request.budgets.maximum_candidates.min(32)) {
        Ok(limit) if limit > 0 => limit,
        _ => return NativeRecallOutcome::Failed(NativeReadFailure::RecallInvalidRequest),
    };
    let context_bytes = request.budgets.maximum_total_content_bytes;
    let session_id = match SessionId::new(format!(
        "native-recall.{}",
        sha256_hex(call.request_id.as_bytes())
    )) {
        Ok(session_id) => session_id,
        Err(_) => return NativeRecallOutcome::Failed(NativeReadFailure::RecallInvalidRequest),
    };
    let query = match SessionTemporalQuery::new(
        session_id,
        None,
        request.query.clone(),
        None,
        temporal_mode,
        RetrievalGrainV1::Occurrence,
        limit,
        DiversityLimits::default(),
        ContextBudget {
            max_bytes: context_bytes,
            max_tokens: context_bytes,
            estimator_version: "words-v1".to_owned(),
        },
    ) {
        Ok(query) => query
            .with_retrieval_scope(SessionRetrievalScope::AllSessionsInAuthorizedRoot)
            .with_execution_limits(
                tracedecay_session_runtime::session_retrieval::admitted_execution_limits(limit),
            ),
        Err(failure) => return NativeRecallOutcome::Failed(failure),
    };
    let options = match native_session_recall_options(
        request,
        evaluation_time_micros,
        native_temporal,
    ) {
        Ok(options) => options,
        Err(failure) => return NativeRecallOutcome::Failed(failure),
    };

    let scope = match native_recall_scope_for_mount(session_retrieval, &call.exact_scope) {
        Ok(scope) => scope,
        Err(failure) => return NativeRecallOutcome::Failed(failure),
    };
    let cancellation = match CancellationSignal::active(format!(
        "native-session-recall.{}",
        sha256_hex(call.request_id.as_bytes())
    )) {
        Ok(signal) => signal,
        Err(_) => return NativeRecallOutcome::Failed(NativeReadFailure::RecallInvalidRequest),
    };
    let context = match native_recall_context(call, &scope, &cancellation) {
        Ok(context) => context,
        Err(failure) => return NativeRecallOutcome::Failed(failure),
    };
    let _cancellation_bridge = NativeCancellationBridge::start(&call.control, &cancellation);
    let batch = match retrieve_native_session_recall_with_cancellation(
        session_retrieval,
        &context,
        &cancellation,
        query,
        &options,
    )
    .await
    {
        Ok(batch) => batch,
        Err(error) => return NativeRecallOutcome::Failed(map_session_recall_error(error)),
    };
    if let Err(failure) = control_failure(&call.control) {
        return NativeRecallOutcome::Failed(failure);
    }
    match build_native_session_recall_reply(call, request, &batch) {
        Ok(reply) => NativeRecallOutcome::Reply(reply),
        Err(failure) => NativeRecallOutcome::Failed(failure),
    }
}

fn native_session_temporal(
    temporal: &NativeRecallTemporalQueryV1,
) -> Result<(TemporalModeV1, NativeSessionRecallTemporal, i64), NativeReadFailure> {
    let evaluation_time_micros = parse_rfc3339_micros(&temporal.evaluation_time)
        .ok_or(NativeReadFailure::RecallInvalidRequest)?;
    match temporal.mode.as_str() {
        "current" => Ok((
            TemporalModeV1::Current,
            NativeSessionRecallTemporal::Current,
            evaluation_time_micros,
        )),
        "as_of" => {
            let cutoff_micros = temporal
                .as_of
                .as_str()
                .and_then(parse_rfc3339_micros)
                .ok_or(NativeReadFailure::RecallInvalidRequest)?;
            Ok((
                TemporalModeV1::AsOf {
                    cutoff: UtcMicros(cutoff_micros),
                },
                NativeSessionRecallTemporal::AsOf { cutoff_micros },
                evaluation_time_micros,
            ))
        }
        "interval" | "history" => Err(NativeReadFailure::RecallUnsupported),
        _ => Err(NativeReadFailure::RecallInvalidRequest),
    }
}

fn native_session_recall_options(
    request: &NativeRecallRequestV1,
    evaluation_time_micros: i64,
    temporal: NativeSessionRecallTemporal,
) -> Result<NativeSessionRecallOptions, NativeReadFailure> {
    let unknown_validity_policy = match request.temporal_query.unknown_validity_policy.as_str() {
        "exclude" => tracedecay_contracts::memory::CognitiveRecallUnknownValidityPolicy::Exclude,
        "degrade" => tracedecay_contracts::memory::CognitiveRecallUnknownValidityPolicy::Degrade,
        "allow_with_warning" => {
            tracedecay_contracts::memory::CognitiveRecallUnknownValidityPolicy::AllowWithWarning
        }
        _ => return Err(NativeReadFailure::RecallInvalidRequest),
    };
    let maximum_candidates = usize::try_from(request.budgets.maximum_candidates.min(32))
        .map_err(|_| NativeReadFailure::RecallInvalidRequest)?;
    let maximum_candidate_content_bytes =
        usize::try_from(request.budgets.maximum_candidate_content_bytes)
            .map_err(|_| NativeReadFailure::RecallInvalidRequest)?;
    let maximum_total_content_bytes =
        usize::try_from(request.budgets.maximum_total_content_bytes)
            .map_err(|_| NativeReadFailure::RecallInvalidRequest)?;
    Ok(NativeSessionRecallOptions {
        temporal,
        evaluation_time_micros: Some(evaluation_time_micros),
        include_superseded: request.temporal_query.include_superseded,
        include_revoked: request.temporal_query.include_revoked,
        unknown_validity_policy,
        exclusions: tracedecay_contracts::memory::CognitiveRecallExclusions {
            stable_memory_refs: request.exclusions.stable_memory_refs.clone(),
            candidate_ids: request.exclusions.candidate_ids.clone(),
            source_refs: request.exclusions.source_refs.clone(),
            trace_refs: request.exclusions.trace_refs.clone(),
            observation_ids: request.exclusions.observation_ids.clone(),
            content_sha256: request.exclusions.content_sha256.clone(),
        },
        limits: NativeSessionRecallLimits {
            maximum_candidates,
            maximum_candidate_content_bytes,
            maximum_total_content_bytes,
            maximum_work_units: 100_000,
        },
    })
}

fn native_recall_scope_for_mount(
    session_retrieval: &NativeSessionRetrievalMountV1,
    exact_scope: &OwnedExactScope,
) -> Result<tracedecay_contracts::ResolvedScope, NativeReadFailure> {
    exact_scope
        .validate()
        .map_err(|_| NativeReadFailure::RecallScopeMismatch)?;
    if let (Some(profile_id), Some(scope)) = (
        session_retrieval.host_profile_id(),
        session_retrieval.host_scope(),
    ) {
        let reference_matches = scope
            .reference
            .as_ref()
            .is_some_and(|reference| reference.as_str() == exact_scope.branch_identity);
        if profile_id.as_str() != exact_scope.profile_id
            || scope.project_id.as_str() != exact_scope.project_id
            || scope.repository_id.as_str() != exact_scope.repository_identity
            || scope.worktree_id.as_str() != exact_scope.worktree_identity
            || !reference_matches
            || scope.scope_digest.as_str() != exact_scope.resolved_scope_digest
        {
            return Err(NativeReadFailure::RecallScopeMismatch);
        }
        return Ok(scope.clone());
    }

    let project_id = ProjectId::new(exact_scope.project_id.clone())
        .map_err(|_| NativeReadFailure::RecallScopeMismatch)?;
    let repository_id = RepositoryId::new(exact_scope.repository_identity.clone())
        .map_err(|_| NativeReadFailure::RecallScopeMismatch)?;
    let worktree_id = WorktreeId::new(exact_scope.worktree_identity.clone())
        .map_err(|_| NativeReadFailure::RecallScopeMismatch)?;
    let reference = RefId::new(exact_scope.branch_identity.clone())
        .map_err(|_| NativeReadFailure::RecallScopeMismatch)?;
    let scope = tracedecay_contracts::ResolvedScope::new(
        project_id,
        repository_id,
        worktree_id,
        Some(reference),
    )
    .map_err(|_| NativeReadFailure::RecallScopeMismatch)?;
    if scope.scope_digest.as_str() != exact_scope.resolved_scope_digest {
        return Err(NativeReadFailure::RecallScopeMismatch);
    }
    Ok(scope)
}

fn native_recall_context(
    call: &ProviderCall,
    scope: &tracedecay_contracts::ResolvedScope,
    cancellation: &CancellationSignal,
) -> Result<RequestContext, NativeReadFailure> {
    let observed_at = now_micros();
    let expires_at = UtcMicros(call.control.deadline_utc_micros());
    if expires_at <= observed_at {
        return Err(NativeReadFailure::DeadlineExceeded);
    }
    let actor = ActorId::new("actor.tracedecay.native.recall")
        .map_err(|_| NativeReadFailure::RecallInvalidRequest)?;
    let capability = tracedecay_tool_catalog::CapabilityId::new("recall.query.v1")
        .map_err(|_| NativeReadFailure::RecallInvalidRequest)?;
    let use_case = tracedecay_tool_catalog::UseCaseId::new("memory.session-recall.v1")
        .map_err(|_| NativeReadFailure::RecallInvalidRequest)?;
    let grant_digest = canonical_sha256(&(
        "tracedecay.native.session-recall-grant.v1",
        call.exact_scope.exact_scope_sha256(),
        call.request_id.as_str(),
        expires_at.0,
    ))
    .map_err(|_| NativeReadFailure::RecallInvalidRequest)?;
    let grant = CapabilityGrantSnapshot::new(
        CapabilityGrantId::new("grant.tracedecay.native.session-recall")
            .map_err(|_| NativeReadFailure::RecallInvalidRequest)?,
        1,
        grant_digest,
        actor.clone(),
        observed_at,
        expires_at,
        scope.clone(),
        std::collections::BTreeSet::from([capability]),
        std::collections::BTreeSet::from([use_case]),
        DisclosureClass::Sensitive,
    )
    .map_err(|_| NativeReadFailure::RecallInvalidRequest)?;
    let request_id =
        RequestId::new(call.request_id.clone()).map_err(|_| NativeReadFailure::RecallInvalidRequest)?;
    RequestContext::new(
        actor,
        scope.clone(),
        grant,
        request_id,
        Deadline::new(expires_at).map_err(|_| NativeReadFailure::RecallInvalidRequest)?,
        cancellation.context(),
    )
    .map_err(|_| NativeReadFailure::RecallInvalidRequest)
}

struct NativeCancellationBridge {
    task: Option<tokio::task::JoinHandle<()>>,
}

impl NativeCancellationBridge {
    fn start(control: &OperationControl, cancellation: &CancellationSignal) -> Self {
        let provider_cancellation = control.cancellation();
        let cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            loop {
                if provider_cancellation.is_cancelled() {
                    cancellation.cancel(now_micros());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
        Self { task: Some(task) }
    }
}

impl Drop for NativeCancellationBridge {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn map_session_recall_error(
    error: super::native_session_recall::NativeSessionRecallAdapterError,
) -> NativeReadFailure {
    use super::native_session_recall::NativeSessionRecallAdapterError;
    match error {
        NativeSessionRecallAdapterError::Unsupported(_) => NativeReadFailure::RecallUnsupported,
        NativeSessionRecallAdapterError::InvalidLimits(_)
        | NativeSessionRecallAdapterError::InvalidPage(_) => {
            NativeReadFailure::RecallProjectionInvalid
        }
        NativeSessionRecallAdapterError::Unavailable(unavailable) => match unavailable {
            NativeSessionRecallUnavailable::Cancelled => NativeReadFailure::Cancelled,
            NativeSessionRecallUnavailable::TimedOut => NativeReadFailure::DeadlineExceeded,
            NativeSessionRecallUnavailable::WrongScope
            | NativeSessionRecallUnavailable::Denied => NativeReadFailure::RecallScopeMismatch,
            NativeSessionRecallUnavailable::BudgetExhausted => {
                NativeReadFailure::RecallBudgetExhausted
            }
            NativeSessionRecallUnavailable::Retrieval(_)
            | NativeSessionRecallUnavailable::CursorStale
            | NativeSessionRecallUnavailable::Locked
            | NativeSessionRecallUnavailable::Redacted
            | NativeSessionRecallUnavailable::Deleted
            | NativeSessionRecallUnavailable::ResetRequired(_)
            | NativeSessionRecallUnavailable::CursorManifestLimitExceeded => {
                NativeReadFailure::ProviderUnavailable
            }
        },
    }
}

fn build_native_session_recall_reply(
    call: &ProviderCall,
    request: &NativeRecallRequestV1,
    batch: &NativeSessionRecallBatch,
) -> Result<ProviderReply, NativeReadFailure> {
    let mut candidates = batch
        .candidates
        .iter()
        .map(|candidate| native_session_recall_candidate(call, candidate))
        .collect::<Vec<_>>();
    let matched_items = batch.admitted_items;
    let mut excluded_items = batch.excluded_items;
    let mut truncated_items = match batch.status {
        NativeSessionRecallBatchStatus::Partial { omitted } => omitted,
        NativeSessionRecallBatchStatus::Complete | NativeSessionRecallBatchStatus::Stale => 0,
    };
    let mut reasons = Vec::new();
    if matches!(batch.status, NativeSessionRecallBatchStatus::Partial { .. }) {
        reasons.push("session_projection_partial".to_owned());
    }
    if matches!(batch.status, NativeSessionRecallBatchStatus::Stale) {
        reasons.push("session_projection_stale".to_owned());
    }
    if batch.degraded {
        reasons.push("session_validity_degraded".to_owned());
    }
    let mut response = native_session_recall_response_value(
        call,
        request,
        batch,
        &candidates,
        matched_items,
        excluded_items,
        truncated_items,
        &reasons,
    );
    let mut response_bytes =
        serde_json::to_vec(&response).map_err(|_| NativeReadFailure::RecallProjectionInvalid)?;
    while u64::try_from(response_bytes.len()).unwrap_or(u64::MAX) > NATIVE_RESPONSE_BYTES
        && candidates.pop().is_some()
    {
        excluded_items = excluded_items.saturating_add(1);
        truncated_items = truncated_items.saturating_add(1);
        if !reasons.iter().any(|reason| reason == "response_byte_budget") {
            reasons.push("response_byte_budget".to_owned());
        }
        response = native_session_recall_response_value(
            call,
            request,
            batch,
            &candidates,
            matched_items,
            excluded_items,
            truncated_items,
            &reasons,
        );
        response_bytes = serde_json::to_vec(&response)
            .map_err(|_| NativeReadFailure::RecallProjectionInvalid)?;
    }
    if u64::try_from(response_bytes.len()).unwrap_or(u64::MAX) > NATIVE_RESPONSE_BYTES {
        return Err(NativeReadFailure::RecallBudgetExhausted);
    }
    let terminal_code = recall_terminal_code(
        matched_items,
        candidates.len(),
        excluded_items,
        truncated_items,
        &reasons,
    );
    response["terminal"] = serde_json::json!({
        "terminal_code": terminal_code.as_wire(),
        "diagnostic_id": Value::Null,
    });
    let response_bytes =
        serde_json::to_vec(&response).map_err(|_| NativeReadFailure::RecallProjectionInvalid)?;
    if u64::try_from(response_bytes.len()).unwrap_or(u64::MAX) > NATIVE_RESPONSE_BYTES {
        return Err(NativeReadFailure::RecallBudgetExhausted);
    }
    let payload = CanonicalPayload::new(
        OwnedVersionedId::new(RECALL_CONTRACT_ID)
            .map_err(|_| NativeReadFailure::RecallProjectionInvalid)?,
        response_bytes.clone(),
        sha256_hex(&response_bytes),
    )
    .map_err(|_| NativeReadFailure::RecallProjectionInvalid)?;
    Ok(ProviderReply {
        terminal: terminal_for_call(call, terminal_code, None),
        payload: Some(payload),
        warnings: Vec::new(),
        extensions: call.extensions.clone(),
        state_generation: call.expected_state_generation,
    })
}

fn native_session_recall_candidate(
    call: &ProviderCall,
    candidate: &super::native_session_recall::NativeSessionRecallCandidate,
) -> Result<Value, NativeReadFailure> {
    let observed_at = candidate
        .validity
        .observed_at_micros
        .and_then(tracedecay_memory_provider_registry::recall_admission::rfc3339_utc_micros);
    let valid_from = candidate
        .validity
        .valid_from_micros
        .and_then(tracedecay_memory_provider_registry::recall_admission::rfc3339_utc_micros);
    let valid_until = candidate
        .validity
        .valid_until_micros
        .and_then(tracedecay_memory_provider_registry::recall_admission::rfc3339_utc_micros);
    let superseded_at = candidate
        .validity
        .superseded_at_micros
        .and_then(tracedecay_memory_provider_registry::recall_admission::rfc3339_utc_micros);
    let revoked_at = candidate
        .validity
        .revoked_at_micros
        .and_then(tracedecay_memory_provider_registry::recall_admission::rfc3339_utc_micros);
    let observation_refs = candidate
        .provenance
        .source_observation_id
        .as_ref()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    Ok(serde_json::json!({
        "candidate_id": candidate.candidate_id,
        "stable_memory_ref": candidate.stable_memory_ref,
        "content": candidate.content,
        "content_ref": Value::Null,
        "content_sha256": candidate.content_sha256,
        "native_score": {
            "score_domain_id": super::native_session_recall::NATIVE_SESSION_RECALL_SCORE_DOMAIN,
            "score_domain_version": 1,
            "raw_value": native_score_decimal(candidate.score_millionths),
            "direction": "higher_is_better",
            "declared_minimum": "0.000000",
            "declared_maximum": "1.000000",
            "calibration_state": "provider_calibrated",
            "semantics": "canonical session retrieval relevance score",
            "components": {
                "score_millionths": candidate.score_millionths,
            },
        },
        "confidence": Value::Null,
        "exact_scope_identity": checkout_observation_scope_value(call),
        "validity": {
            "observed_at": observed_at,
            "valid_from": valid_from,
            "valid_until": valid_until,
            "superseded_at": superseded_at,
            "superseded_by": Value::Null,
            "revoked_at": revoked_at,
            "source_revision": candidate.validity.source_revision,
            "temporal_state": native_session_validity_state(candidate.validity.state),
        },
        "provenance": {
            "state": "available",
            "origin_refs": candidate.source_refs,
            "observation_refs": observation_refs,
            "source_refs": candidate.source_refs,
            "transform_chain": [],
            "provider_trace_refs": candidate.trace_refs,
            "redaction_reason": Value::Null,
            "session_anchor": candidate.session_anchor,
            "observation_anchor": candidate.observation_anchor,
            "source_anchor": candidate.source_anchor,
            "provider": candidate.provider,
            "session_id": candidate.session_id,
            "message_id": candidate.message_id,
            "ordinal": candidate.ordinal,
            "role": candidate.role,
            "kind": candidate.kind,
        },
        "explanation": {
            "summary": "canonical session message retrieved by the host-admitted Native route",
            "matched_features": [],
            "activation_trace_refs": candidate.trace_refs,
            "limitations": candidate.warnings,
        },
        "source_refs": candidate.source_refs,
        "trace_refs": candidate.trace_refs,
        "sensitivity": "unknown",
        "memory_class": super::native_session_recall::NATIVE_SESSION_RECALL_MEMORY_CLASS,
        "warnings": candidate.warnings,
        "extensions": [],
    }))
}

fn checkout_observation_scope_value(call: &ProviderCall) -> Value {
    serde_json::json!({
        "scope_binding": "checkout_observations",
        "profile_id": call.exact_scope.profile_id,
        "project_id": call.exact_scope.project_id,
        "repository_identity": call.exact_scope.repository_identity,
        "worktree_identity": call.exact_scope.worktree_identity,
        "branch_identity": call.exact_scope.branch_identity,
        "agent_session_id": "",
        "resolved_scope_digest": "",
    })
}

fn native_session_validity_state(
    state: super::native_session_recall::NativeSessionRecallValidityState,
) -> &'static str {
    match state {
        super::native_session_recall::NativeSessionRecallValidityState::Current => "current",
        super::native_session_recall::NativeSessionRecallValidityState::Expired => "expired",
        super::native_session_recall::NativeSessionRecallValidityState::Future => "future",
        super::native_session_recall::NativeSessionRecallValidityState::Superseded => "superseded",
        super::native_session_recall::NativeSessionRecallValidityState::Revoked => "revoked",
        super::native_session_recall::NativeSessionRecallValidityState::Unknown => "unknown",
    }
}

fn native_session_recall_response_value(
    call: &ProviderCall,
    request: &NativeRecallRequestV1,
    batch: &NativeSessionRecallBatch,
    candidates: &[Value],
    matched_items: u64,
    excluded_items: u64,
    truncated_items: u64,
    reasons: &[String],
) -> Value {
    let state = match batch.status {
        NativeSessionRecallBatchStatus::Stale => "stale",
        NativeSessionRecallBatchStatus::Partial { .. } => "partial",
        NativeSessionRecallBatchStatus::Complete if candidates.is_empty() => "zero_results",
        NativeSessionRecallBatchStatus::Complete => "complete",
    };
    let next_cursor = batch.temporal.cursor.clone();
    serde_json::json!({
        "provider_id": NATIVE_PROVIDER_ID,
        "provider_instance_id": PROVIDER_INSTANCE_ID,
        "registration_revision": request.registration_revision,
        "ready_receipt_digest": request.ready_receipt_digest,
        "request_identity": request.request_identity,
        "exact_scope_identity": exact_scope_value(call),
        "provider_state_generation": call.expected_state_generation,
        "candidates": candidates,
        "coverage": {
            "state": state,
            "searched_scope_digest": call.exact_scope.exact_scope_sha256(),
            "searched_temporal_digest": recall_temporal_digest(&request.temporal_query),
            "scanned_items": batch.scanned_items,
            "matched_items": matched_items,
            "returned_items": candidates.len(),
            "excluded_items": excluded_items,
            "truncated_items": truncated_items,
            "next_cursor": next_cursor,
            "reasons": reasons,
            "canonical_temporal": batch.temporal,
            "history_grant_present": request.history_grant.is_some(),
        },
        "ordering": {
            "score_domain_id": super::native_session_recall::NATIVE_SESSION_RECALL_SCORE_DOMAIN,
            "direction": "higher_is_better",
            "tie_breaker": "candidate_id_lexicographic_utf8",
        },
        "terminal": {
            "terminal_code": "success",
            "diagnostic_id": Value::Null,
        },
        "warnings": [],
    })
}

fn recall_temporal_digest(temporal: &NativeRecallTemporalQueryV1) -> String {
    let value = serde_json::json!({
        "mode": temporal.mode,
        "evaluation_time": temporal.evaluation_time,
        "as_of": temporal.as_of,
        "interval_start": temporal.interval_start,
        "interval_end": temporal.interval_end,
        "include_superseded": temporal.include_superseded,
        "include_revoked": temporal.include_revoked,
        "unknown_validity_policy": temporal.unknown_validity_policy,
    });
    serde_json::to_vec(&value)
        .map(|bytes| sha256_hex(&bytes))
        .unwrap_or_default()
}

fn recall_terminal_code(
    matched_items: u64,
    returned_items: usize,
    excluded_items: u64,
    truncated_items: u64,
    reasons: &[String],
) -> TerminalCode {
    if matched_items == 0 && returned_items == 0 && excluded_items == 0 && reasons.is_empty() {
        TerminalCode::SuccessZeroResults
    } else if excluded_items > 0 || truncated_items > 0 || !reasons.is_empty() {
        TerminalCode::Partial
    } else {
        TerminalCode::Success
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
    let memory = match MemoryApplication::new(
        target.owner().clone(),
        DatabaseFactStore::new(target.database()),
    ) {
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
        | RetainedSurfaceExecutionErrorV1::Unavailable { detail: _ }
        | RetainedSurfaceExecutionErrorV1::ProfileResetRequired
        | RetainedSurfaceExecutionErrorV1::ProjectResetRequired => {
            NativeReadFailure::ProviderUnavailable
        }
    }
}
