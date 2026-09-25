//! Project-owned Native application port.
//!
//! The provider-neutral Native adapter is synchronous, while the retained
//! project-memory authority is asynchronous. This module keeps that seam
//! narrow: one bounded actor owns a current-thread Tokio runtime and the
//! staged advisory lifecycle. Canonical fact operations verify already-settled
//! facts through their existing owner-bound memory application port.

// This implementation is intentionally constructible before product
// composition mounts it. Keep the dormant constructor/actor surface warning-
// free until the composition owner wires the explicit activation path.
#![allow(dead_code)]

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tracedecay_contracts::RetainedSurfaceExecutionErrorV1;
use tracedecay_contracts::retained_surfaces::{
    FactCommitOwnerV1, FactCommitReceiptV1, FactIdentitySourceResultV1, FactProjectionV1,
    FactSearchGraphCoverageV1, FactV1, MemoryScopeV1,
};
use tracedecay_domain::{FactOwnerV1, ProjectId, UserProfileId};
use tracedecay_memory_provider_registry::{
    ApiError, CanonicalPayload, CommittedEffectEvidence, FallbackDirective, HandshakeRequest,
    HandshakeResponse, NATIVE_FACT_PROMOTION_OBSERVATION_KIND,
    NATIVE_FACT_PROMOTION_PAYLOAD_CONTRACT_ID, NATIVE_PROVIDER_ID,
    NATIVE_STAGED_SESSION_OBSERVATION_KIND, NATIVE_STAGED_SESSION_PAYLOAD_CONTRACT_ID,
    NativeMemoryApplicationPort, NativeObservation, NativeObservationEnvelope,
    OBSERVATION_CONTRACT_ID, OperationControl, OwnedProviderId, OwnedVersionedId, ProviderCall,
    ProviderDescriptor, ProviderLimits, ProviderOperation, ProviderReply, TerminalCode,
    TerminalRecord, rfc3339_utc_micros,
};
use tracedecay_session_memory::fact_store::DatabaseFactStore;
use tracedecay_session_memory::memory::MemoryApplication;
use tracedecay_store::{
    FactReadControl, ProjectMemoryFactHistoryQueryV1, ProjectMemoryFactHistoryV1,
    ProjectMemoryFactIdV1, ProjectMemoryFactSearchKindV1, ProjectMemoryFactSearchPageV1,
    ProjectMemoryFactSearchQuery,
};
use tracedecay_store_runtime::retained_memory::MemoryTargetAccessV1;

use super::memory_mapping;
use super::native_staged_observations::{
    StagedControlOutcome, StagedEffectEvidence, StagedObservationRecord, StagedObservationStore,
    StagedOutcome, StagedRow, StagedStoreError, recorded_validity,
};
use super::open_project_retained_memory_target;
use crate::tracedecay::TraceDecay;

#[cfg(test)]
#[path = "native_baseline_tests.rs"]
mod baseline_tests;
#[cfg(test)]
#[path = "native_provider_tests.rs"]
mod tests;

pub(super) const IMPLEMENTATION_IDENTITY_SHA256: &str =
    "7fe6923361d4caa6c213e0760d438c9f3b9bda60d4c1195812130bfe66c2fa16";
pub(super) const STATE_SCHEMA_VERSION: &str = "native-staged-v2";
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
const RECALL_BUDGET_DIAGNOSTIC: &str = "native.recall_budget_exhausted";
const RECALL_SCORE_DOMAIN: &str = "tracedecay.native.project-memory.search.v1";
const RECALL_SCORE_DOMAIN_VERSION: u32 = 1;
const RECALL_CONTRACT_ID: &str = "tracedecay.memory.provider.recall.v1";
const RECALL_HISTORY_UNAVAILABLE_REASON: &str = "native.recall_history_unsupported";

/// The `source_authority` the accepted observation contract declares for
/// `session.message_committed.v1`
/// (`product/contracts/memory-provider-v1/provider-observation-contract.json`).
/// It is a contract constant, never a value copied out of a payload.
const STAGED_SESSION_SOURCE_AUTHORITY: &str = "host_session";

const STAGED_SOURCE_IDENTITY_DIAGNOSTIC: &str = "native.staged_source_identity_unavailable";
const STAGED_CONFLICT_DIAGNOSTIC: &str = "native.staged_observation_conflict";
const STAGED_STORE_DIAGNOSTIC: &str = "native.staged_observation_store_unavailable";

/// Score domain of a staged advisory candidate. Deliberately distinct from the
/// project-memory fact domain: the two are not the same measurement, and the
/// host normalizes each against the domain the candidate names.
const STAGED_RECALL_SCORE_DOMAIN: &str = "tracedecay.native.staged-observation.recall.v1";

/// Hard per-candidate ceiling on staged message text, applied under whatever
/// `request.budgets.maximum_candidate_content_bytes` the host asked for. A
/// staged row carries agent-authored text, so its share of the fixed
/// `NATIVE_RESPONSE_BYTES` envelope is bounded here rather than by the request.
const STAGED_CANDIDATE_CONTENT_MAX_BYTES: u64 = 2_048;

/// Construction failures for the project-owned Native application port.
#[derive(Debug)]
pub(crate) enum NativeMemoryApplicationPortBuildError {
    /// The fixed provider descriptor could not be assembled or validated.
    Descriptor(ApiError),
    /// The bounded actor runtime could not be constructed.
    Runtime(std::io::Error),
    /// The bounded actor thread could not be started.
    ActorThread(std::io::Error),
    /// The provider-local staged-observation store under the host-granted
    /// provider-state root could not be opened. Project open fails here rather
    /// than mounting a Native port that would silently refuse every session
    /// observation.
    StagedStore(StagedStoreError),
    /// The blocking task the composition root builds the port on could not be
    /// run to completion (the runtime is shutting down, or the task panicked).
    BlockingJoin {
        /// Bounded description of the join failure.
        detail: String,
    },
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
            Self::StagedStore(error) => {
                write!(
                    formatter,
                    "Native staged-observation store could not be opened: {error}"
                )
            }
            Self::BlockingJoin { detail } => {
                write!(
                    formatter,
                    "Native application port could not be built off the async runtime: {detail}"
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
            Self::StagedStore(error) => Some(error),
            Self::BlockingJoin { .. } => None,
        }
    }
}

/// The project-owned Native application port used by product composition.
pub(crate) struct ProjectNativeMemoryApplicationPort {
    descriptor: ProviderDescriptor,
    actor: NativeReadActor,
    /// Product-owned staged-observation store. Actor writes and recalls share
    /// one committed state; descriptor reads expose its durable generation.
    staged: Arc<StagedObservationStore>,
    common_scopes: Mutex<BTreeSet<(String, u64)>>,
    accepted_readiness: Mutex<Option<NativeAcceptedReadiness>>,
    admission_authority:
        Option<Arc<dyn tracedecay_memory_provider_registry::AdvisoryAdmissionAuthority>>,
}

/// Only the latest successful handshake can supply descriptor evidence. The
/// immutable implementation/capability descriptor remains owned by the port.
#[derive(Clone)]
struct NativeAcceptedReadiness {
    registration_revision: u64,
    exact_scope_sha256: String,
    ready_receipt_sha256: String,
    provider_instance_id: String,
    state_namespace: String,
    effective_limits: ProviderLimits,
}

/// Builds the project-owned Native application port behind the provider
/// registry's neutral trait object.
///
/// `profile_id` is the daemon's own profile identity, supplied by the
/// composition root at mount time. It is the only profile the adapter ever
/// attests on a recall candidate; the profile named in a call's exact scope
/// is never copied into an attestation.
///
/// `provider_state_root` is the host-granted root every supervised provider's
/// state is contained under (`<store data root>/provider-state`). The Native
/// staged-observation store is opened beneath it; a placement that cannot be
/// opened fails project open instead of degrading to a port that refuses every
/// session observation.
pub(crate) fn project_native_memory_application_port(
    cg: Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
    project_root: PathBuf,
    profile_id: UserProfileId,
    provider_state_root: &Path,
) -> Result<Arc<dyn NativeMemoryApplicationPort>, NativeMemoryApplicationPortBuildError> {
    Ok(Arc::new(ProjectNativeMemoryApplicationPort::new(
        cg,
        project_root,
        profile_id,
        provider_state_root,
    )?))
}

/// [`project_native_memory_application_port`], moved off the async runtime's
/// worker threads.
///
/// Building the port is blocking work with an unbounded tail: `create_dir_all`,
/// a `SQLite` open, a journal-mode change, `BEGIN IMMEDIATE`, schema DDL, and a
/// durable commit — on a contended database or a slow `fsync` that is a stall,
/// not a pause. `StagedObservationStore` declares the same blocking discipline
/// `SqliteObservationJournal` does, so the composition root must honour it at
/// *construction* too and not only on the delivery path. Every input is owned,
/// so the closure carries no borrow across the await.
///
/// # Errors
///
/// Returns [`NativeMemoryApplicationPortBuildError`] for every build failure,
/// including [`NativeMemoryApplicationPortBuildError::BlockingJoin`] when the
/// blocking task itself could not be run to completion.
pub(crate) async fn project_native_memory_application_port_off_runtime(
    cg: Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
    project_root: PathBuf,
    profile_id: UserProfileId,
    provider_state_root: PathBuf,
) -> Result<Arc<dyn NativeMemoryApplicationPort>, NativeMemoryApplicationPortBuildError> {
    tokio::task::spawn_blocking(move || {
        project_native_memory_application_port(cg, project_root, profile_id, &provider_state_root)
    })
    .await
    .map_err(
        |error| NativeMemoryApplicationPortBuildError::BlockingJoin {
            detail: error.to_string(),
        },
    )?
}

/// Builds the Native actor off runtime and installs the live host authority before exposure.
pub(crate) async fn project_native_memory_application_port_with_authority_off_runtime(
    cg: Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
    project_root: PathBuf,
    profile_id: UserProfileId,
    provider_state_root: PathBuf,
    authority: Arc<dyn tracedecay_memory_provider_registry::AdvisoryAdmissionAuthority>,
) -> Result<Arc<dyn NativeMemoryApplicationPort>, NativeMemoryApplicationPortBuildError> {
    tokio::task::spawn_blocking(move || {
        ProjectNativeMemoryApplicationPort::new(cg, project_root, profile_id, &provider_state_root)
            .map(|port| {
                Arc::new(port.with_admission_authority(authority))
                    as Arc<dyn NativeMemoryApplicationPort>
            })
    })
    .await
    .map_err(
        |error| NativeMemoryApplicationPortBuildError::BlockingJoin {
            detail: error.to_string(),
        },
    )?
}

impl ProjectNativeMemoryApplicationPort {
    /// Creates one bounded actor-backed port over the live project graph cell
    /// for the daemon profile `profile_id`.
    pub(crate) fn new(
        cg: Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
        project_root: PathBuf,
        profile_id: UserProfileId,
        provider_state_root: &Path,
    ) -> Result<Self, NativeMemoryApplicationPortBuildError> {
        let descriptor =
            native_descriptor().map_err(NativeMemoryApplicationPortBuildError::Descriptor)?;
        let staged = Arc::new(
            StagedObservationStore::open(provider_state_root)
                .map_err(NativeMemoryApplicationPortBuildError::StagedStore)?,
        );
        let actor = NativeReadActor::new(cg, project_root, profile_id, Arc::clone(&staged))?;
        Ok(Self {
            descriptor,
            actor,
            staged,
            common_scopes: Mutex::new(BTreeSet::new()),
            accepted_readiness: Mutex::new(None),
            admission_authority: None,
        })
    }

    /// Installs the existing host authority for cross-origin history and restore admission.
    pub(crate) fn with_admission_authority(
        mut self,
        authority: Arc<dyn tracedecay_memory_provider_registry::AdvisoryAdmissionAuthority>,
    ) -> Self {
        self.admission_authority = Some(authority);
        self
    }

    /// Test-only handle on the provider-local staged store, so a durability
    /// fault can be injected between the staged insert and its commit and the
    /// answered terminal checked against the row that did (not) survive.
    #[cfg(test)]
    pub(crate) fn staged_store(&self) -> &StagedObservationStore {
        &self.staged
    }

    /// Durably stages one admitted session message and answers with the
    /// evidence of the row that actually committed.
    ///
    /// The scope written on the row is the host-attested `call.exact_scope`;
    /// no payload field ever contributes scope identity. The payload bytes are
    /// the ones the admission hygiene receipt binds, stored verbatim and never
    /// re-sanitized. `stage_or_duplicate` commits its transaction *before* it
    /// returns [`StagedOutcome::Committed`], so a `Success` answered here can
    /// never outlive a rolled-back row: a failure leaves no row and the
    /// journal item stays redeliverable.
    ///
    /// Nothing on this path writes a canonical fact. A staged row becomes an
    /// advisory recall candidate only, bound to its five origin checkout fields.
    fn observe_staged_session(&self, envelope: &NativeObservationEnvelope<'_>) -> ProviderReply {
        let call = envelope.call;
        let Some(idempotency_key) = call.idempotency_key.clone() else {
            return self.observe_failure(call, NativeReadFailure::StagedSourceIdentityUnavailable);
        };
        let payload: Value = match serde_json::from_slice(&call.payload.bytes) {
            Ok(value) => value,
            Err(_) => return self.observe_invalid(call),
        };
        let Some(source) = staged_source_identity(&payload, &envelope.canonical_payload) else {
            return self.observe_failure(call, NativeReadFailure::StagedSourceIdentityUnavailable);
        };
        let record = StagedObservationRecord {
            scope: call.exact_scope.clone(),
            idempotency_key: idempotency_key.clone(),
            source_authority: STAGED_SESSION_SOURCE_AUTHORITY.to_owned(),
            source_event_id: source.source_event_id,
            source_revision: source.source_revision,
            observation_kind: envelope.observation_kind.clone(),
            payload_contract: envelope.payload_contract.clone(),
            sanitized_payload: call.payload.bytes.clone(),
            operation_id: call.operation_id.clone(),
            request_identity: call.request_id.clone(),
            admitted_at_unix_ms: unix_millis_now(),
        };
        match self
            .actor
            .dispatch_staged(call.clone(), record, self.admission_authority.clone())
        {
            Ok(StagedOutcome::Committed(evidence)) => self.staged_committed_reply(call, &evidence),
            Ok(StagedOutcome::Duplicate(evidence)) => {
                self.staged_duplicate_reply(call, &idempotency_key, &evidence)
            }
            // A contradiction is never answered as a silent duplicate: the
            // stored row stands and the delivery is refused with the
            // idempotency-conflict terminal.
            Ok(StagedOutcome::Conflict { reason }) => {
                tracing::warn!(
                    event = "memory_native_staged_observation_conflict",
                    operation_id = %call.operation_id,
                    reason = %reason,
                    "staged session observation contradicts the stored row"
                );
                self.observe_failure(call, NativeReadFailure::StagedConflict)
            }
            Err(error) => {
                tracing::warn!(
                    event = "memory_native_staged_observation_failed",
                    operation_id = %call.operation_id,
                    error = ?error,
                    "staged session observation could not be committed"
                );
                self.observe_failure(call, error)
            }
        }
    }

    /// Success carrying the committed-effect evidence stored on the new row.
    ///
    /// The store allocates a durable generation inside the same transaction as
    /// the row. The returned evidence and every later descriptor read agree.
    fn staged_committed_reply(
        &self,
        call: &ProviderCall,
        evidence: &StagedEffectEvidence,
    ) -> ProviderReply {
        match CommittedEffectEvidence::committed(
            call.expected_state_generation,
            evidence.admitted_sequence,
            vec![evidence.provider_reference.clone()],
            evidence.receipt.clone(),
            evidence.effect_digest.clone(),
        ) {
            Ok(effect) => staged_effect_reply(call, effect, evidence.admitted_sequence),
            Err(_) => self.observe_failure(call, NativeReadFailure::StagedStoreUnavailable),
        }
    }

    /// Success carrying the *stored* evidence of the earlier row.
    ///
    /// Exactly what the duplicate committed-effect state may carry, and no
    /// more. `CommittedEffectEvidence::duplicate` is validated by
    /// `validate_duplicate_effect`
    /// (`crates/tracedecay-memory-provider-api/src/lib.rs`), which *refuses*
    /// a duplicate that carries `committed_item_refs` or a
    /// `verification_sha256`: "duplicate requires the deduplicated key, the
    /// committing operation, and the prior receipt, and claims no new
    /// partition". That is a deliberate cross-provider rule — a duplicate
    /// commits nothing, so it may not describe a committed partition — and
    /// this provider does not widen a shared contract for its own
    /// convenience. So the honest statement of what a redelivery reproduces
    /// is:
    ///
    /// * `provider_receipt_sha256` — the receipt stored on the committing
    ///   row, byte-for-byte the one the first reply carried.
    /// * `duplicate_of_operation_id` — the operation that actually committed,
    ///   read from the row, not this request's operation.
    /// * `duplicate_of_idempotency_key` — the key *this* request carried; the
    ///   journal proves it against the delivery it answers.
    ///
    /// The row's `provider_reference` and `effect_digest` are durable and
    /// unchanged, and they are what the committed reply carried, but they do
    /// not ride on the duplicate envelope. A caller that needs them for the
    /// deduplicated row reads them from the staged store, which is why they
    /// are stored as columns and survive content eviction.
    fn staged_duplicate_reply(
        &self,
        call: &ProviderCall,
        idempotency_key: &str,
        evidence: &StagedEffectEvidence,
    ) -> ProviderReply {
        match CommittedEffectEvidence::duplicate(
            self.staged
                .generation()
                .unwrap_or(call.expected_state_generation),
            idempotency_key,
            evidence.operation_id.clone(),
            evidence.receipt.clone(),
        ) {
            Ok(effect) => staged_effect_reply(
                call,
                effect,
                self.staged
                    .generation()
                    .unwrap_or(call.expected_state_generation),
            ),
            Err(_) => self.observe_failure(call, NativeReadFailure::StagedStoreUnavailable),
        }
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
        if matches!(failure, NativeReadFailure::StagedEffectUnknown) {
            return unknown_store_reply(call);
        }
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
        let mut descriptor = self.descriptor.clone();
        if let Ok(generation) = self.staged.generation() {
            descriptor.state_generation = generation;
        }
        descriptor
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
        let descriptor = self.descriptor();
        let effective_limits = request.host_limits.minimum(descriptor.limits);
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
            CommittedEffectEvidence::none(Some(descriptor.state_generation)),
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
        let mut latest = match self.accepted_readiness.lock() {
            Ok(latest) => latest,
            Err(_) => {
                return self.handshake_failure(
                    request,
                    TerminalCode::ProviderUnavailable,
                    "native.readiness_unavailable",
                );
            }
        };
        let accepted = NativeAcceptedReadiness {
            registration_revision: request.registration_revision,
            exact_scope_sha256: request.exact_scope.exact_scope_sha256(),
            ready_receipt_sha256: ready_receipt(request, effective_limits),
            provider_instance_id: PROVIDER_INSTANCE_ID.to_owned(),
            state_namespace: STATE_NAMESPACE.to_owned(),
            effective_limits,
        };
        if request
            .required_capabilities
            .iter()
            .any(|capability| capability.as_str() == "memory.advisory_common.v1")
        {
            match self.common_scopes.lock() {
                Ok(mut scopes) => {
                    scopes.insert((
                        request.exact_scope.exact_scope_sha256(),
                        request.registration_revision,
                    ));
                }
                Err(_) => {
                    return self.handshake_failure(
                        request,
                        TerminalCode::ProviderUnavailable,
                        "native.profile_negotiation_unavailable",
                    );
                }
            }
        }
        *latest = Some(accepted.clone());
        HandshakeResponse {
            terminal,
            descriptor: Some(descriptor),
            provider_instance_id: Some(accepted.provider_instance_id),
            state_namespace: Some(accepted.state_namespace),
            accepted_scope: Some(request.exact_scope.clone()),
            effective_limits: Some(accepted.effective_limits),
            ready_receipt_sha256: Some(accepted.ready_receipt_sha256),
            warnings: Vec::new(),
        }
    }

    fn health(&self, call: &ProviderCall) -> ProviderReply {
        if let Err(failure) = control_failure(&call.control) {
            return self.observe_failure(call, failure);
        }
        self.lifecycle_call(call)
    }

    fn observe(&self, observation: NativeObservation<'_>) -> ProviderReply {
        let call = observation.call();
        if let Err(failure) = control_failure(&call.control) {
            return self.observe_failure(call, failure);
        }
        if call.validate().is_err() || !observation_matches_call(&observation) {
            return self.observe_invalid(call);
        }
        match &observation {
            // Unchanged path. A settled canonical fact promotion is verified
            // against the retained project-memory authority and writes
            // nothing — neither a canonical fact nor a staged row.
            NativeObservation::FactPromotion(envelope) => {
                let payload = match parse_settled_native_fact(envelope) {
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
            // Staged path. The admitted session message is durably committed
            // to the provider-local staged store before this returns.
            NativeObservation::StagedSession(envelope) => self.observe_staged_session(envelope),
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
        let mut request = match parse_native_recall_request(call) {
            Ok(request) => request,
            Err(failure) => return self.observe_failure(call, failure),
        };
        request.common_profile = self.common_scopes.lock().ok().is_some_and(|scopes| {
            scopes.contains(&(
                call.exact_scope.exact_scope_sha256(),
                call.registration_revision,
            ))
        });
        match self
            .actor
            .dispatch_recall(call.clone(), request, self.admission_authority.clone())
        {
            NativeRecallOutcome::Reply(reply) => reply,
            NativeRecallOutcome::Failed(failure) => self.observe_failure(call, failure),
        }
    }

    fn feedback(&self, call: &ProviderCall) -> ProviderReply {
        self.lifecycle_call(call)
    }

    fn maintenance(&self, call: &ProviderCall) -> ProviderReply {
        self.lifecycle_call(call)
    }

    fn inspection(&self, call: &ProviderCall) -> ProviderReply {
        self.lifecycle_call(call)
    }

    fn correction(&self, call: &ProviderCall) -> ProviderReply {
        self.lifecycle_call(call)
    }

    fn delete_by_source(&self, call: &ProviderCall) -> ProviderReply {
        self.lifecycle_call(call)
    }

    fn snapshot_export(&self, call: &ProviderCall) -> ProviderReply {
        self.lifecycle_call(call)
    }

    fn snapshot_restore(&self, call: &ProviderCall) -> ProviderReply {
        self.lifecycle_call(call)
    }

    fn replay(&self, call: &ProviderCall) -> ProviderReply {
        self.lifecycle_call(call)
    }
}

/// Test-only: the host-granted provider-state root a fixture-mounted port is
/// given.
///
/// Production composition passes `<store data root>/provider-state`. A test
/// derives it from its own fixture root so every constructed port owns a
/// separate staged store, exactly as two projects would.
#[cfg(test)]
pub(crate) fn test_provider_state_root(project_root: &Path) -> PathBuf {
    project_root
        .parent()
        .unwrap_or(project_root)
        .join(super::observation_journey::PROVIDER_STATE_DIR_NAME)
}

pub(crate) const fn native_provider_limits() -> ProviderLimits {
    ProviderLimits {
        request_bytes: 65_536,
        response_bytes: NATIVE_RESPONSE_BYTES,
        observation_batch_items: 16,
        recall_candidates: 32,
        concurrent_operations: 4,
        operation_millis: NATIVE_OPERATION_MILLIS,
        snapshot_bytes: 65_536,
        inspection_items: 64,
    }
}

fn native_descriptor() -> Result<ProviderDescriptor, ApiError> {
    let provider_id = OwnedProviderId::new(NATIVE_PROVIDER_ID)?;
    // Common advisory operations use the provider-local staged store. Legacy
    // explicit-fact callers retain their owner-bound canonical memory adapter.
    let capabilities = [
        "memory.advisory_common.v1",
        "provider.health.v1",
        "observation.accept.v1",
        "recall.query.v1",
        "recall.temporal.v1",
        "feedback.record.v1",
        "maintenance.run.v1",
        "inspection.read.v1",
        "correction.apply.v1",
        "deletion.by_source.v1",
        "snapshot.export.v1",
        "snapshot.restore.v1",
        "replay.apply.v1",
        "facts.explicit.v1",
    ]
    .into_iter()
    .map(OwnedVersionedId::new)
    .collect::<Result<Vec<_>, _>>()?;
    ProviderDescriptor::new(
        provider_id,
        IMPLEMENTATION_IDENTITY_SHA256,
        STATE_SCHEMA_VERSION,
        0,
        capabilities,
        native_provider_limits(),
    )
}

/// Copies the production declaration without constructing a provider or state.
/// The limits remain declared ceilings until a caller matches actual health.
#[cfg(feature = "test-helpers")]
pub(crate) fn production_provider_declaration_for_test()
-> Result<(ProviderDescriptor, String, String), ApiError> {
    let descriptor = native_descriptor()?;
    let mut digest = Sha256::new();
    digest_native_limits(&mut digest, descriptor.limits);
    let limits_digest = hex::encode(digest.finalize());
    Ok((descriptor, PROVIDER_INSTANCE_ID.to_owned(), limits_digest))
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
    digest_native_limits(&mut digest, effective_limits);
    hex::encode(digest.finalize())
}

fn digest_native_limits(digest: &mut Sha256, limits: ProviderLimits) {
    for limit in [
        limits.request_bytes,
        limits.response_bytes,
        limits.observation_batch_items,
        limits.recall_candidates,
        limits.concurrent_operations,
        limits.operation_millis,
        limits.snapshot_bytes,
        limits.inspection_items,
    ] {
        digest.update(limit.to_be_bytes());
    }
}

fn self_descriptor_identity() -> &'static [u8] {
    IMPLEMENTATION_IDENTITY_SHA256.as_bytes()
}

/// Whether the classified observation still agrees, field by field, with the
/// sanitized envelope the call actually carried.
///
/// The kind/contract pair is checked against the pair the *variant* stands
/// for, so a session message can never be answered on the fact-promotion path
/// or the other way round.
fn observation_matches_call(observation: &NativeObservation<'_>) -> bool {
    let (expected_kind, expected_contract) = match observation {
        NativeObservation::FactPromotion(_) => (
            NATIVE_FACT_PROMOTION_OBSERVATION_KIND,
            NATIVE_FACT_PROMOTION_PAYLOAD_CONTRACT_ID,
        ),
        NativeObservation::StagedSession(envelope) => match envelope.observation_kind.as_str() {
            NATIVE_STAGED_SESSION_OBSERVATION_KIND => (
                NATIVE_STAGED_SESSION_OBSERVATION_KIND,
                NATIVE_STAGED_SESSION_PAYLOAD_CONTRACT_ID,
            ),
            "source.edit_settled.v1" => (
                "source.edit_settled.v1",
                "tracedecay.memory.observation.source-edit.v1",
            ),
            "test.execution_settled.v1" => (
                "test.execution_settled.v1",
                "tracedecay.memory.observation.test-execution.v1",
            ),
            "feedback.outcome_settled.v1" => (
                "feedback.outcome_settled.v1",
                "tracedecay.memory.observation.feedback-outcome.v1",
            ),
            _ => return false,
        },
    };
    let observed = observation.envelope();
    let call = observed.call;
    if call.operation != ProviderOperation::Observe
        || call.provider_id.as_str() != NATIVE_PROVIDER_ID
        || call.payload.contract_id.as_str() != OBSERVATION_CONTRACT_ID
        || observed.observation_kind != expected_kind
        || observed.payload_contract != expected_contract
    {
        return false;
    }
    let Ok(envelope) = serde_json::from_slice::<Value>(&call.payload.bytes) else {
        return false;
    };
    let Some(object) = envelope.as_object() else {
        return false;
    };
    object.keys().all(|key| {
        matches!(
            key.as_str(),
            "observation_kind"
                | "payload_contract"
                | "canonical_payload"
                | "source_identity"
                | "history_grant"
                | "observation_id"
                | "idempotency_key"
                | "provider_id"
                | "registration_revision"
                | "ready_receipt_digest"
                | "exact_scope_identity"
                | "payload_sha256"
                | "extensions"
                | "provenance"
                | "privacy"
                | "occurred_at"
                | "admitted_at"
                | "source_sequence"
                | "request_identity"
                | "deadline"
                | "cancellation"
        )
    }) && object.get("observation_kind") == Some(&Value::String(observed.observation_kind.clone()))
        && object.get("payload_contract") == Some(&Value::String(observed.payload_contract.clone()))
        && object.get("canonical_payload") == Some(&observed.canonical_payload)
}

/// Stable canonical source identity of one settled session message, read out
/// of the canonical payload the admission hygiene receipt binds.
///
/// The canonical observation envelope names the record it settled
/// (`stable_record_id`) and the envelope revision that shaped it (`version`).
/// That pair survives both a registration-revision change and a journal
/// reconstruction, neither of which the delivery idempotency key survives
/// (the key mixes in `target.registration_revision`), which is why the staged
/// store's lifetime exactly-once index is built on it. A payload that does not
/// name it is refused rather than staged under a substitute identity.
struct StagedSourceIdentityV1 {
    source_event_id: String,
    source_revision: Option<String>,
}

fn staged_source_identity(
    envelope: &Value,
    canonical_payload: &Value,
) -> Option<StagedSourceIdentityV1> {
    if let Some(source) = envelope.pointer("/source_identity/original_source/source") {
        let source_event_id = source.get("observation_id")?.as_str()?.to_owned();
        let source_revision = match source.get("source_revision")? {
            Value::Null => None,
            Value::String(text) if !text.is_empty() => Some(text.clone()),
            _ => return None,
        };
        return Some(StagedSourceIdentityV1 {
            source_event_id,
            source_revision,
        });
    }
    let source_event_id = canonical_payload.get("stable_record_id")?.as_str()?;
    (!source_event_id.is_empty()).then(|| StagedSourceIdentityV1 {
        source_event_id: source_event_id.to_owned(),
        source_revision: None,
    })
}

/// Host wall clock in milliseconds, recorded on the staged row for audit only.
/// Staged recall recency is the row's admission sequence, never this value.
pub(crate) fn unix_millis_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

/// A success terminal carrying real committed-effect evidence.
///
/// [`terminal_for_call`] answers `CommittedEffectEvidence::none`, which is the
/// truth for every read-only Native operation. The staged path did change
/// durable provider-local state, so it needs the committed (or duplicate)
/// evidence instead; the state generation reported is the one the effect
/// evidence declares, which the fabric checks against the call.
pub(super) fn staged_response_fits(
    call: &ProviderCall,
    evidence: &StagedEffectEvidence,
    duplicate: bool,
    generation: u64,
) -> bool {
    let effect = if duplicate {
        CommittedEffectEvidence::duplicate(
            generation,
            evidence.idempotency_key.clone(),
            evidence.operation_id.clone(),
            evidence.receipt.clone(),
        )
    } else {
        CommittedEffectEvidence::committed(
            call.expected_state_generation,
            generation,
            vec![evidence.provider_reference.clone()],
            evidence.receipt.clone(),
            evidence.effect_digest.clone(),
        )
    };
    effect.is_ok_and(|effect| {
        staged_effect_reply(call, effect, generation)
            .validate(native_provider_limits().response_bytes)
            .is_ok()
    })
}

fn staged_effect_reply(
    call: &ProviderCall,
    effect: CommittedEffectEvidence,
    state_generation: u64,
) -> ProviderReply {
    match TerminalRecord::new(
        call.operation,
        call.provider_id.clone(),
        TerminalCode::Success,
        effect,
        FallbackDirective::forbidden(),
        call.operation_id.clone(),
        call_scope_digest(call),
        None,
    ) {
        Ok(terminal) => ProviderReply {
            terminal,
            payload: Some(call.payload.clone()),
            warnings: Vec::new(),
            extensions: call.extensions.clone(),
            state_generation,
        },
        Err(_) => ProviderReply {
            terminal: TerminalRecord::failure_before_dispatch(
                call.operation,
                call.provider_id.clone(),
                TerminalCode::InternalFailure,
                &call.operation_id,
                call_scope_digest(call),
                Some(call.expected_state_generation),
                "native.staged_terminal_invalid",
            ),
            payload: None,
            warnings: Vec::new(),
            extensions: Vec::new(),
            state_generation: call.expected_state_generation,
        },
    }
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
    required_capabilities: Vec<String>,
    policy_revision: u64,
    extensions: Vec<NativeRecallExtensionV1>,
    deadline: Value,
    cancellation: Value,
    #[serde(default)]
    history_grant: Option<Value>,
    #[serde(skip)]
    common_profile: bool,
    #[serde(skip)]
    unknown_validity_withheld: bool,
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
        || !request
            .required_capabilities
            .iter()
            .any(|capability| capability == "recall.query.v1")
        || request.required_capabilities.iter().any(|capability| {
            !matches!(
                capability.as_str(),
                "recall.query.v1"
                    | "recall.temporal.v1"
                    | "memory.advisory_common.v1"
                    | "facts.explicit.v1"
            )
        })
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
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_nanos()).ok())
        .unwrap_or(i64::MAX);
    owned_temporal_query(temporal)?
        .validate_at(now_nanos)
        .map_err(|_| NativeReadFailure::RecallInvalidRequest)?;

    Ok(())
}

pub(crate) fn parse_rfc3339_micros(value: &str) -> Option<i64> {
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
    for value in groups.into_iter().flat_map(|(values, _)| values) {
        if value.is_empty() || value.len() > 1024 || value.chars().any(char::is_control) {
            return Err(NativeReadFailure::RecallInvalidRequest);
        }
    }
    for digest in &exclusions.content_sha256 {
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
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
    // The payload records an earlier wire snapshot. Transport may consume
    // budget before restoring the live control, which remains authoritative
    // for execution; its remaining budget must never be rebuilt from payload.
    if deadline_utc_micros != call.control.deadline_utc_micros() || remaining_millis == 0 {
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
    envelope: &NativeObservationEnvelope<'_>,
) -> Result<SettledNativeFactWriteV1, NativeReadFailure> {
    let payload =
        serde_json::from_value::<SettledNativeFactWriteV1>(envelope.canonical_payload.clone())
            .map_err(|_| NativeReadFailure::InvalidPayload)?;
    if payload.kind != "settled_native_fact_write" {
        return Err(NativeReadFailure::InvalidPayload);
    }
    validate_settled_native_fact(envelope.call, &payload)?;
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
    Unauthorized,
    Cancelled,
    DeadlineExceeded,
    RecallInvalidRequest,
    RecallUnsupported,
    RecallScopeMismatch,
    RecallExtensionUnsupported,
    RecallProjectionInvalid,
    RecallBudgetExhausted,
    /// The canonical payload does not name the stable source record identity
    /// the staged store's lifetime exactly-once index requires.
    StagedSourceIdentityUnavailable,
    /// The key or the settled source event is already staged under
    /// contradicting content. Never answered as a silent duplicate.
    StagedConflict,
    /// The provider-local staged store could not be read or written.
    StagedStoreUnavailable,
    StagedEffectUnknown,
}

impl NativeReadFailure {
    fn terminal(self) -> (TerminalCode, &'static str) {
        match self {
            Self::StagedEffectUnknown => (
                TerminalCode::EffectUnknown,
                "native.operation_reconciliation_required",
            ),
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
            Self::Unauthorized => (
                TerminalCode::Unauthorized,
                "native.advisory_authority_denied",
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
                (TerminalCode::CapacityExceeded, RECALL_BUDGET_DIAGNOSTIC)
            }
            Self::StagedSourceIdentityUnavailable => (
                TerminalCode::InvalidRequest,
                STAGED_SOURCE_IDENTITY_DIAGNOSTIC,
            ),
            Self::StagedConflict => (TerminalCode::Conflict, STAGED_CONFLICT_DIAGNOSTIC),
            Self::StagedStoreUnavailable => {
                (TerminalCode::ProviderUnavailable, STAGED_STORE_DIAGNOSTIC)
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
    Stage {
        call: ProviderCall,
        record: StagedObservationRecord,
        authority: Option<Arc<dyn tracedecay_memory_provider_registry::AdvisoryAdmissionAuthority>>,
        reply: SyncSender<Result<StagedOutcome, NativeReadFailure>>,
    },
    Control {
        call: ProviderCall,
        authority: Option<Arc<dyn tracedecay_memory_provider_registry::AdvisoryAdmissionAuthority>>,
        reply: SyncSender<Result<StagedControlOutcome, NativeReadFailure>>,
    },
    Verify {
        call: ProviderCall,
        fact: FactV1,
        commit: FactCommitReceiptV1,
        reply: SyncSender<NativeReadOutcome>,
    },
    Recall {
        call: ProviderCall,
        request: NativeRecallRequestV1,
        authority: Option<Arc<dyn tracedecay_memory_provider_registry::AdvisoryAdmissionAuthority>>,
        reply: SyncSender<NativeRecallOutcome>,
    },
}

struct NativeQueuedRequest(Arc<AtomicU64>);

impl Drop for NativeQueuedRequest {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

struct QueuedNativeReadCommand {
    command: NativeReadCommand,
    queued: NativeQueuedRequest,
}

struct NativeReadActor {
    sender: Mutex<Option<SyncSender<QueuedNativeReadCommand>>>,
    queued_requests: Arc<AtomicU64>,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl NativeReadActor {
    fn new(
        cg: Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
        project_root: PathBuf,
        profile_id: UserProfileId,
        staged: Arc<StagedObservationStore>,
    ) -> Result<Self, NativeMemoryApplicationPortBuildError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(NativeMemoryApplicationPortBuildError::Runtime)?;
        let (sender, receiver) = mpsc::sync_channel(1);
        let join = thread::Builder::new()
            .name(ACTOR_THREAD_NAME.to_owned())
            .spawn(move || {
                native_read_actor_main(receiver, cg, project_root, profile_id, staged, runtime);
            })
            .map_err(NativeMemoryApplicationPortBuildError::ActorThread)?;
        Ok(Self {
            sender: Mutex::new(Some(sender)),
            queued_requests: Arc::new(AtomicU64::new(0)),
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
        if let Err(failure) = self.enqueue_store(command) {
            return NativeReadOutcome::Failed(failure);
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
        authority: Option<Arc<dyn tracedecay_memory_provider_registry::AdvisoryAdmissionAuthority>>,
    ) -> NativeRecallOutcome {
        let (reply, receiver) = mpsc::sync_channel(1);
        let control = call.control.clone();
        let command = NativeReadCommand::Recall {
            call,
            request,
            authority,
            reply,
        };
        if let Err(failure) = self.enqueue_store(command) {
            return NativeRecallOutcome::Failed(failure);
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
    receiver: mpsc::Receiver<QueuedNativeReadCommand>,
    cg: Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
    project_root: PathBuf,
    profile_id: UserProfileId,
    staged: Arc<StagedObservationStore>,
    runtime: tokio::runtime::Runtime,
) {
    while let Ok(QueuedNativeReadCommand { command, queued }) = receiver.recv() {
        // Cancellation and authority refusals still consumed this queue slot.
        drop(queued);
        match command {
            NativeReadCommand::Stage {
                call,
                record,
                authority,
                reply,
            } => {
                let outcome = admit_native_call(authority.as_deref(), &call)
                    .and_then(|_| control_failure(&call.control))
                    .and_then(|()| {
                        staged
                            .stage_controlled(record, &call)
                            .map_err(store_failure)
                    });
                #[cfg(test)]
                if matches!(outcome, Ok(StagedOutcome::Committed(_))) && staged.take_lost_reply() {
                    continue;
                }
                let _ = reply.send(outcome);
            }
            NativeReadCommand::Control {
                call,
                authority,
                reply,
            } => {
                let outcome =
                    admit_native_call(authority.as_deref(), &call).and_then(|admission| {
                        control_failure(&call.control)?;
                        staged
                            .control(&call, admission.as_ref())
                            .map_err(store_failure)
                    });
                #[cfg(test)]
                if outcome.as_ref().is_ok_and(|outcome| outcome.changed) && staged.take_lost_reply()
                {
                    continue;
                }
                let _ = reply.send(outcome);
            }
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
                authority,
                reply,
            } => {
                let admission = match admit_native_call(authority.as_deref(), &call) {
                    Ok(admission) => admission,
                    Err(failure) => {
                        let _ = reply.send(NativeRecallOutcome::Failed(failure));
                        continue;
                    }
                };
                let mut request = request;
                if let Some(admission) = admission {
                    let mut projected = serde_json::json!({"history_grant":request.history_grant});
                    apply_current_admission(&mut projected, &admission);
                    request.history_grant = projected
                        .get("history_grant")
                        .filter(|value| !value.is_null())
                        .cloned();
                }
                let outcome = recall_with_runtime(
                    &runtime,
                    &cg,
                    &project_root,
                    &profile_id,
                    &staged,
                    call,
                    request,
                );
                let _ = reply.send(outcome);
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn recall_with_runtime(
    runtime: &tokio::runtime::Runtime,
    cg: &Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
    project_root: &Path,
    profile_id: &UserProfileId,
    staged: &StagedObservationStore,
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
    match runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_millis(timeout_millis),
            recall_project_memory(cg, project_root, profile_id, staged, &call, &request),
        )
        .await
    }) {
        Ok(outcome) => outcome,
        Err(_) => NativeRecallOutcome::Failed(NativeReadFailure::DeadlineExceeded),
    }
}

async fn recall_project_memory(
    cg: &Arc<tokio::sync::RwLock<Arc<TraceDecay>>>,
    project_root: &Path,
    profile_id: &UserProfileId,
    staged: &StagedObservationStore,
    call: &ProviderCall,
    request: &NativeRecallRequestV1,
) -> NativeRecallOutcome {
    if let Err(failure) = control_failure(&call.control) {
        return NativeRecallOutcome::Failed(failure);
    }
    let project_id = match ProjectId::new(call.exact_scope.project_id.clone()) {
        Ok(project_id) => project_id,
        Err(_) => return NativeRecallOutcome::Failed(NativeReadFailure::RecallScopeMismatch),
    };
    let current = Arc::clone(&*cg.read().await);
    if let Err(failure) = control_failure(&call.control) {
        return NativeRecallOutcome::Failed(failure);
    }
    // A recall scoped to a project other than the one this Native instance
    // owns is a scope mismatch (a different authority), not an unavailable
    // scope: the owning project is present, it is simply not the requested
    // one. Decide this before opening the target so the memory-target
    // authorization denial is reserved for a missing or unauthorized owner.
    let expected_owner = FactOwnerV1::Project {
        project_id: project_id.clone(),
    };
    match current.project_memory_owner() {
        Ok(owner) if owner == expected_owner => {}
        Ok(_) => return NativeRecallOutcome::Failed(NativeReadFailure::RecallScopeMismatch),
        Err(_) => return NativeRecallOutcome::Failed(NativeReadFailure::ProviderUnavailable),
    }
    if common_recall(request) {
        let mut live_call = call.clone();
        live_call.expected_state_generation = match staged.generation() {
            Ok(value) => value,
            Err(_) => {
                return NativeRecallOutcome::Failed(NativeReadFailure::StagedStoreUnavailable);
            }
        };
        let page = match ProjectMemoryFactSearchPageV1::new(
            expected_owner,
            Vec::new(),
            None,
            tracedecay_store::ProjectMemoryFactSearchGraphCoverageV1::NotApplicable,
        ) {
            Ok(page) => page,
            Err(_) => {
                return NativeRecallOutcome::Failed(NativeReadFailure::RecallProjectionInvalid);
            }
        };
        let temporal = match owned_temporal_query(&request.temporal_query) {
            Ok(query) => query,
            Err(failure) => return NativeRecallOutcome::Failed(failure),
        };
        let rows = match staged.recall_temporal(
            &call.exact_scope,
            &request.query,
            &temporal,
            request.history_grant.as_ref(),
            &owned_exclusions(&request.exclusions),
            &call.request_id,
        ) {
            Ok(rows) => rows,
            Err(_) => {
                return NativeRecallOutcome::Failed(NativeReadFailure::StagedStoreUnavailable);
            }
        };
        let mut request = request.clone();
        request.unknown_validity_withheld = staged
            .has_unknown_validity(&call.exact_scope)
            .unwrap_or(true);
        return match build_native_recall_reply(&live_call, &request, profile_id, &page, &rows) {
            Ok(reply) => NativeRecallOutcome::Reply(reply),
            Err(failure) => NativeRecallOutcome::Failed(failure),
        };
    }
    if request.temporal_query.mode != "current" {
        return NativeRecallOutcome::Failed(NativeReadFailure::RecallUnsupported);
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
        Err(error) => return NativeRecallOutcome::Failed(map_retained_error(error)),
    };
    let owner = target.owner().clone();
    if owner != expected_owner {
        return NativeRecallOutcome::Failed(NativeReadFailure::RecallScopeMismatch);
    }
    let memory = match MemoryApplication::new(owner.clone(), DatabaseFactStore::new(target.database()))
    {
        Ok(memory) => memory,
        Err(_) => return NativeRecallOutcome::Failed(NativeReadFailure::ProviderUnavailable),
    };
    let search_query = match native_recall_search_query(request, owner) {
        Ok(query) => query,
        Err(failure) => return NativeRecallOutcome::Failed(failure),
    };
    let read_control = native_fact_read_control(&call.control);
    let page_result = match request.objective.as_str() {
        "search" => {
            memory
                .search_project_memory_facts(search_query, &read_control)
                .await
        }
        "probe" => {
            memory
                .probe_project_memory_facts(search_query, &read_control)
                .await
        }
        "related" => {
            memory
                .related_project_memory_facts(search_query, &read_control)
                .await
        }
        "reason" => {
            memory
                .reason_project_memory_facts(search_query, &read_control)
                .await
        }
        _ => return NativeRecallOutcome::Failed(NativeReadFailure::RecallUnsupported),
    };
    let page = match page_result {
        Ok(page) => page,
        Err(error) => {
            return NativeRecallOutcome::Failed(map_retained_error(
                memory_mapping::map_memory_error(error),
            ));
        }
    };
    if let Err(failure) = control_failure(&call.control) {
        return NativeRecallOutcome::Failed(failure);
    }
    // Staged advisory rows of *this* exact scope. The store returns only rows
    // whose seven stored scope fields re-derive the call's own exact-scope
    // digest, so no other checkout and no other agent session is reachable
    // from here. A store that cannot be read fails the recall rather than
    // silently answering with facts alone.
    let staged_rows = match staged.recall(
        &call.exact_scope,
        &request.query,
        staged.retention().maximum_content_rows_per_scope,
    ) {
        Ok(rows) => rows,
        Err(error) => {
            tracing::warn!(
                event = "memory_native_staged_recall_failed",
                operation_id = %call.operation_id,
                error = %error,
                "staged observation recall failed"
            );
            return NativeRecallOutcome::Failed(NativeReadFailure::StagedStoreUnavailable);
        }
    };
    match build_native_recall_reply(call, request, profile_id, &page, &staged_rows) {
        Ok(reply) => NativeRecallOutcome::Reply(reply),
        Err(failure) => NativeRecallOutcome::Failed(failure),
    }
}

/// The single candidate ceiling both classes are budgeted under: the host's
/// requested maximum, never above the provider's own
/// `ProviderLimits.recall_candidates`.
fn recall_candidate_ceiling(request: &NativeRecallRequestV1) -> usize {
    let ceiling = request
        .budgets
        .maximum_candidates
        .min(native_provider_limits().recall_candidates);
    usize::try_from(ceiling).unwrap_or(usize::MAX)
}

fn native_recall_search_query(
    request: &NativeRecallRequestV1,
    owner: FactOwnerV1,
) -> Result<ProjectMemoryFactSearchQuery, NativeReadFailure> {
    let limit = usize::try_from(request.budgets.maximum_candidates.min(32))
        .map_err(|_| NativeReadFailure::RecallInvalidRequest)?;
    let (kind, query) = match request.objective.as_str() {
        "search" => (
            ProjectMemoryFactSearchKindV1::Search,
            Some(request.query.clone()),
        ),
        "probe" => (
            ProjectMemoryFactSearchKindV1::Probe,
            Some(request.query.clone()),
        ),
        "related" => (
            ProjectMemoryFactSearchKindV1::Related {
                entity: request.query.clone(),
            },
            None,
        ),
        "reason" => {
            let entities = serde_json::from_str::<Vec<String>>(&request.query)
                .map_err(|_| NativeReadFailure::RecallUnsupported)?;
            (ProjectMemoryFactSearchKindV1::Reason { entities }, None)
        }
        _ => return Err(NativeReadFailure::RecallUnsupported),
    };
    ProjectMemoryFactSearchQuery::new(owner, kind, query, None, limit)
        .map_err(|_| NativeReadFailure::RecallInvalidRequest)
}

/// One candidate of either class, carrying the key the two classes are merged
/// on.
///
/// The merge rule is fixed here rather than left to the caller: score
/// descending, and at equal score a canonical fact is ordered ahead of a
/// staged observation. Because the sort is stable, facts keep the store's own
/// order among themselves and staged rows keep the store's
/// `(score desc, admitted_sequence desc, idempotency_key asc)` order among
/// themselves, so the whole ordering is total and reproducible.
struct RankedRecallCandidateV1 {
    /// Score in millionths, comparable across both classes.
    score_millionths: u64,
    /// `false` for a canonical fact, `true` for a staged observation.
    staged: bool,
    /// The rendered candidate.
    value: Value,
}

/// Builds the recall reply from the canonical fact page and the staged
/// advisory rows of the same exact scope, budgeted under one ceiling.
///
/// Staged rows can never starve facts: every fact hit is offered first and
/// sorts ahead of a staged row at equal score, and the single
/// [`recall_candidate_ceiling`] applies to the merged list.
fn build_native_recall_reply(
    call: &ProviderCall,
    request: &NativeRecallRequestV1,
    profile_id: &UserProfileId,
    page: &ProjectMemoryFactSearchPageV1,
    staged_rows: &[StagedRow],
) -> Result<ProviderReply, NativeReadFailure> {
    build_native_recall_reply_with_response_bytes(
        call,
        request,
        profile_id,
        page,
        staged_rows,
        NATIVE_RESPONSE_BYTES,
    )
}

fn build_native_recall_reply_with_response_bytes(
    call: &ProviderCall,
    request: &NativeRecallRequestV1,
    profile_id: &UserProfileId,
    page: &ProjectMemoryFactSearchPageV1,
    staged_rows: &[StagedRow],
    maximum_response_bytes: u64,
) -> Result<ProviderReply, NativeReadFailure> {
    let mapped = memory_mapping::search_page(page)
        .map_err(|_| NativeReadFailure::RecallProjectionInvalid)?;
    let expected_owner = FactCommitOwnerV1::Project {
        project_id: ProjectId::new(call.exact_scope.project_id.clone())
            .map_err(|_| NativeReadFailure::RecallScopeMismatch)?,
    };
    if mapped.owner != expected_owner
        || mapped
            .hits
            .iter()
            .any(|hit| hit.fact.owner != expected_owner)
    {
        return Err(NativeReadFailure::RecallScopeMismatch);
    }

    let mut ranked: Vec<RankedRecallCandidateV1> = Vec::new();
    let mut total_content_bytes = 0_u64;
    let mut excluded_items = 0_u64;
    let mut reasons = graph_coverage_reasons(mapped.graph_coverage);
    if request.unknown_validity_withheld {
        push_reason(&mut reasons, "unknown_validity_withheld");
    }
    let evaluation_micros = parse_rfc3339_micros(&request.temporal_query.evaluation_time)
        .ok_or(NativeReadFailure::RecallInvalidRequest)?;
    for hit in &mapped.hits {
        if common_recall(request) {
            continue;
        }
        let exclusions = &request.exclusions;
        if exclusions
            .stable_memory_refs
            .contains(&hit.fact.fact_id.to_string())
            || exclusions
                .candidate_ids
                .contains(&format!("{}:{}", call.request_id, hit.fact.fact_id))
            || exclusions
                .content_sha256
                .contains(&sha256_hex(hit.fact.content.as_bytes()))
            || fact_source_refs(&hit.fact)
                .iter()
                .any(|source| exclusions.source_refs.contains(source))
        {
            excluded_items += 1;
            push_reason(&mut reasons, "request_exclusion");
            continue;
        }
        // The current-fact search has no historical projection to consult. Do
        // not relabel a newer authoritative projection as if it existed at
        // the requested evaluation time; exclude it and report the partial
        // temporal coverage instead.
        if hit.fact.projected_as_of.0 > evaluation_micros {
            excluded_items = excluded_items.saturating_add(1);
            push_reason(&mut reasons, "projected_as_of_after_evaluation_time");
            continue;
        }
        let content_bytes = u64::try_from(hit.fact.content.len()).unwrap_or(u64::MAX);
        let source_refs = fact_source_refs(&hit.fact);
        let source_ref_count = u64::try_from(source_refs.len()).unwrap_or(u64::MAX);
        if content_bytes > request.budgets.maximum_candidate_content_bytes {
            excluded_items = excluded_items.saturating_add(1);
            push_reason(&mut reasons, "candidate_content_budget");
            continue;
        }
        if total_content_bytes.saturating_add(content_bytes)
            > request.budgets.maximum_total_content_bytes
        {
            excluded_items = excluded_items.saturating_add(1);
            push_reason(&mut reasons, "total_content_budget");
            continue;
        }
        if source_ref_count > request.budgets.maximum_source_refs_per_candidate {
            excluded_items = excluded_items.saturating_add(1);
            push_reason(&mut reasons, "source_ref_budget");
            continue;
        }
        if request.budgets.maximum_trace_refs_per_candidate == 0 {
            return Err(NativeReadFailure::RecallInvalidRequest);
        }
        total_content_bytes = total_content_bytes.saturating_add(content_bytes);
        ranked.push(RankedRecallCandidateV1 {
            score_millionths: u64::from(hit.scores.score_millionths),
            staged: false,
            value: native_recall_candidate(call, profile_id, &hit.fact, hit)?,
        });
    }

    let temporal = owned_temporal_query(&request.temporal_query)?;
    for row in staged_rows {
        if staged_row_excluded(row, call, &request.exclusions) {
            excluded_items += 1;
            push_reason(&mut reasons, "request_exclusion");
            continue;
        }
        if common_recall(request) {
            if row.original_source.is_none() {
                excluded_items += 1;
                push_reason(&mut reasons, "source_attribution_unavailable");
                continue;
            }
            if !history_permits(row, call, request) {
                excluded_items += 1;
                push_reason(&mut reasons, "source_history_not_granted");
                continue;
            }
            use tracedecay_memory_provider_registry::TemporalEligibility;
            match row
                .validity
                .eligibility(&temporal, current_row_disposition(row, request), false)
                .map_err(|_| NativeReadFailure::RecallProjectionInvalid)?
            {
                TemporalEligibility::Excluded => {
                    excluded_items += 1;
                    continue;
                }
                TemporalEligibility::WithheldUnknown => {
                    excluded_items += 1;
                    push_reason(&mut reasons, "unknown_validity_withheld");
                    continue;
                }
                TemporalEligibility::IncludedUnknown => {
                    push_reason(&mut reasons, "unknown_validity_admitted")
                }
                TemporalEligibility::Eligible => {}
            }
            if row.source_revision.is_none() {
                push_reason(&mut reasons, "unknown_source_revision");
            }
        }
        // The same temporal rule the fact path applies: a row admitted after
        // the requested evaluation time is not evidence that existed then.
        let observed_micros = row.admitted_at_unix_ms.saturating_mul(1_000);
        if !common_recall(request) && observed_micros > evaluation_micros {
            excluded_items = excluded_items.saturating_add(1);
            push_reason(&mut reasons, "projected_as_of_after_evaluation_time");
            continue;
        }
        let (content, truncated) = staged_candidate_content(row, request);
        if content.is_empty() {
            excluded_items = excluded_items.saturating_add(1);
            push_reason(&mut reasons, "candidate_content_budget");
            continue;
        }
        let content_bytes = u64::try_from(content.len()).unwrap_or(u64::MAX);
        if total_content_bytes.saturating_add(content_bytes)
            > request.budgets.maximum_total_content_bytes
        {
            excluded_items = excluded_items.saturating_add(1);
            push_reason(&mut reasons, "total_content_budget");
            continue;
        }
        total_content_bytes = total_content_bytes.saturating_add(content_bytes);
        ranked.push(RankedRecallCandidateV1 {
            score_millionths: staged_score_millionths(row.score),
            staged: true,
            value: native_staged_recall_candidate(
                call,
                row,
                &content,
                truncated,
                observed_micros,
                request,
            )?,
        });
    }

    // Stable, so equal-score members of one class keep the order their own
    // store produced; `staged` breaks a cross-class tie in the facts' favour.
    let merged_order = |left: &RankedRecallCandidateV1, right: &RankedRecallCandidateV1| {
        right
            .score_millionths
            .cmp(&left.score_millionths)
            .then(left.staged.cmp(&right.staged))
    };
    ranked.sort_by(merged_order);
    let candidate_ceiling = recall_candidate_ceiling(request);
    if ranked.len() > candidate_ceiling {
        excluded_items = excluded_items
            .saturating_add(u64::try_from(ranked.len() - candidate_ceiling).unwrap_or(u64::MAX));
        // Non-starvation, enforced rather than hoped for. Staged scores and
        // fact scores come from two different domains: a freshly staged
        // message scores near the staged maximum, so with enough staged rows
        // the merged prefix can be entirely staged and every canonical fact —
        // the only class the host can actually cite — falls off the end. One
        // slot of a non-zero ceiling is therefore reserved for the
        // highest-ranked eligible fact whenever the prefix holds none. It is a
        // reservation, not a re-ranking: the remaining slots keep the merged
        // order exactly, and the selection is re-sorted by the same comparator
        // so repeated identical requests answer identical bytes.
        let reserved_fact = (candidate_ceiling > 0
            && !ranked[..candidate_ceiling]
                .iter()
                .any(|entry| !entry.staged))
        .then(|| ranked.iter().position(|entry| !entry.staged))
        .flatten();
        match reserved_fact {
            Some(index) => {
                let fact = ranked.remove(index);
                ranked.truncate(candidate_ceiling.saturating_sub(1));
                ranked.push(fact);
                ranked.sort_by(merged_order);
            }
            None => ranked.truncate(candidate_ceiling),
        }
        push_reason(&mut reasons, "candidate_limit");
    }

    if mapped.next_after.is_some() {
        push_reason(&mut reasons, "candidate_limit");
    }
    let matched_items =
        u64::try_from(mapped.hits.len().saturating_add(staged_rows.len())).unwrap_or(u64::MAX);
    let truncated_items = u64::from(mapped.next_after.is_some());
    // The fact page cannot be trimmed here — the store cursor points after the
    // whole page, so dropping a fact would make it unreachable. A staged row
    // has no cursor, so the complete provider reply is brought under the fixed
    // byte ceiling by dropping the lowest-ranked staged rows and saying so.
    // Measuring only the canonical payload is insufficient: the boundary also
    // charges the terminal, payload framing, digest, warnings, and extensions.
    loop {
        let candidates: Vec<Value> = ranked.iter().map(|entry| entry.value.clone()).collect();
        let terminal_code = recall_terminal_code(
            matched_items,
            candidates.len(),
            excluded_items,
            truncated_items,
            &reasons,
        );
        let mut response = native_recall_response_value(
            call,
            request,
            &candidates,
            matched_items,
            excluded_items,
            truncated_items,
            &reasons,
            mapped.next_after.as_ref(),
        );
        response["terminal"] = serde_json::json!({
            "terminal_code": terminal_code.as_wire(),
            "diagnostic_id": Value::Null,
        });
        let response_bytes = serde_json::to_vec(&response)
            .map_err(|_| NativeReadFailure::RecallProjectionInvalid)?;
        let payload = CanonicalPayload::new(
            OwnedVersionedId::new(RECALL_CONTRACT_ID)
                .map_err(|_| NativeReadFailure::RecallProjectionInvalid)?,
            response_bytes.clone(),
            sha256_hex(&response_bytes),
        )
        .map_err(|_| NativeReadFailure::RecallProjectionInvalid)?;
        let reply = ProviderReply {
            terminal: terminal_for_call(call, terminal_code, None),
            payload: Some(payload),
            warnings: Vec::new(),
            extensions: call.extensions.clone(),
            state_generation: call.expected_state_generation,
        };
        match reply.validate(maximum_response_bytes) {
            Ok(()) => return Ok(reply),
            Err(ApiError::BoundaryBytesExceeded { .. }) => {
                let Some(index) = ranked.iter().rposition(|entry| entry.staged) else {
                    return Err(NativeReadFailure::RecallBudgetExhausted);
                };
                ranked.remove(index);
                excluded_items = excluded_items.saturating_add(1);
                push_reason(&mut reasons, "response_byte_budget");
            }
            Err(_) => return Err(NativeReadFailure::RecallProjectionInvalid),
        }
    }
}

/// Complete provider-reply ceiling, derived from the product recall shape.
///
/// The application asks for at most eight candidates and 8 KiB of aggregate
/// content. Measured staged-candidate metadata is below 3 KiB per candidate,
/// while terminal/payload framing is below 2 KiB. The old 8 KiB value predated
/// recall and could not contain even the content budget plus mandatory framing;
/// 64 KiB covers the measured `2 KiB + 8 * 3 KiB + 8 KiB` shape with room for
/// JSON escaping and ordinary identity growth. Aggregate validation below still
/// returns a truthful partial reply when an exceptional encoding exceeds it.
const NATIVE_RESPONSE_BYTES: u64 = 65_536;

fn native_recall_candidate(
    call: &ProviderCall,
    profile_id: &UserProfileId,
    fact: &FactV1,
    hit: &tracedecay_contracts::retained_surfaces::FactSearchHitV1,
) -> Result<Value, NativeReadFailure> {
    // The canonical record this candidate *is*, named in the host's own
    // canonical-record reference form so host provenance hydration can read
    // it back through the retained project-memory authority instead of
    // taking the adapter's word for it. It leads `origin_refs` because it is
    // the strongest origin the adapter can offer; the evidence anchors that
    // produced the fact follow it.
    let mut origin_refs = vec![format!("record:{}", fact.fact_id)];
    origin_refs.extend(fact_source_refs(fact));
    let summary = hit
        .why
        .clone()
        .unwrap_or_else(|| "native project-memory match".to_owned());
    if summary.len() > 8_192 || summary.chars().any(char::is_control) {
        return Err(NativeReadFailure::RecallProjectionInvalid);
    }
    let scores = hit.scores;
    let category = serde_json::to_value(&fact.category)
        .map_err(|_| NativeReadFailure::RecallProjectionInvalid)?;
    let score_components = serde_json::json!({
        "score_millionths": scores.score_millionths,
        "fts_score_millionths": scores.fts_score_millionths,
        "jaccard_score_millionths": scores.jaccard_score_millionths,
        "holographic_score_millionths": scores.holographic_score_millionths,
        "trust_score_millionths": scores.trust_score_millionths,
    });
    let observed_at = rfc3339_utc_micros(fact.projected_as_of.0)
        .ok_or(NativeReadFailure::RecallProjectionInvalid)?;
    let valid_from = rfc3339_utc_micros(fact.telemetry.created_at.0)
        .ok_or(NativeReadFailure::RecallProjectionInvalid)?;
    let full_lineage_unavailable = serde_json::json!({
        "state": "unavailable",
        "reason": RECALL_HISTORY_UNAVAILABLE_REASON,
        "refs": [],
    });
    let native_linkage = serde_json::json!({
        "outcome_history": {
            "state": "partial",
            "active_assertion_id": fact.active_assertion_id.to_string(),
            "last_event_id": fact.last_event_id.to_string(),
            "full_lineage": full_lineage_unavailable,
        },
    });
    Ok(serde_json::json!({
        "candidate_id": format!("{}:{}", call.request_id, fact.fact_id),
        "stable_memory_ref": fact.fact_id.to_string(),
        "content": fact.content,
        "content_ref": Value::Null,
        "content_sha256": sha256_hex(fact.content.as_bytes()),
        "native_score": {
            "score_domain_id": RECALL_SCORE_DOMAIN,
            "score_domain_version": RECALL_SCORE_DOMAIN_VERSION,
            "raw_value": native_score_decimal(scores.score_millionths),
            "direction": "higher_is_better",
            "declared_minimum": "0.000000",
            "declared_maximum": "1.500000",
            "calibration_state": "provider_calibrated",
            "semantics": "project-memory combined score; fixed-point millionths",
            "components": score_components,
        },
        "confidence": Value::Null,
        "exact_scope_identity": native_fact_scope_attestation(fact, profile_id),
        // The contract fixes validity instants as utc_rfc3339_nanos; the
        // host admission authority denies any other representation as an
        // invalid validity record, so the Native micros are projected here.
        "validity": {
            "observed_at": observed_at,
            "valid_from": valid_from,
            "valid_until": Value::Null,
            "superseded_at": Value::Null,
            "superseded_by": Value::Null,
            "revoked_at": Value::Null,
            "source_revision": fact.last_event_id.to_string(),
            "temporal_state": "current",
        },
        "provenance": {
            "state": "available",
            "origin_refs": origin_refs,
            "observation_refs": [],
            "source_refs": fact_source_refs(fact),
            "native_linkage": native_linkage,
            "transform_chain": [],
            "provider_trace_refs": [],
            "redaction_reason": Value::Null,
        },
        "explanation": {
            "summary": summary,
            "matched_features": [],
            "activation_trace_refs": [],
            "native_linkage_ref": "provenance.native_linkage",
            "native_score_ref": "native_score",
            "limitations": ["native score is not host-normalized"],
        },
        "source_refs": fact_source_refs(fact),
        "trace_refs": [],
        "sensitivity": "unknown",
        "memory_class": category,
        "warnings": [],
        "extensions": [],
    }))
}

/// The staged message text this candidate may carry, and whether it was cut.
///
/// The cap is the smaller of the host's requested per-candidate ceiling and
/// the adapter's own [`STAGED_CANDIDATE_CONTENT_MAX_BYTES`], and the cut lands
/// on a UTF-8 boundary. An empty result means no whole character fits, and the
/// caller excludes the row rather than emitting empty content.
fn staged_candidate_content(row: &StagedRow, request: &NativeRecallRequestV1) -> (String, bool) {
    let cap = usize::try_from(
        request
            .budgets
            .maximum_candidate_content_bytes
            .min(STAGED_CANDIDATE_CONTENT_MAX_BYTES),
    )
    .unwrap_or(usize::MAX);
    if row.message_text.len() <= cap {
        return (row.message_text.clone(), false);
    }
    let mut boundary = cap;
    while boundary > 0 && !row.message_text.is_char_boundary(boundary) {
        boundary -= 1;
    }
    (row.message_text[..boundary].to_owned(), true)
}

/// The staged score in the same fixed-point millionths the fact scores use, so
/// one comparison orders both classes.
fn staged_score_millionths(score: f64) -> u64 {
    if score.is_nan() {
        return 0;
    }
    let clamped = score.clamp(0.0, 1.0) * 1_000_000.0;
    // `clamped` is finite and within `[0, 1_000_000]`, so the cast is exact.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    {
        clamped.round() as u64
    }
}

/// One staged session observation as an advisory recall candidate.
///
/// The candidate attests the row's five checkout fields; its own original
/// session and scope digests remain in origin provenance. The store already
/// proved the seven origin fields re-derive their stored digest. Nothing about a staged row is shaped like a host evidence
/// reference: `origin_refs` names the provider-local row, the operation that
/// committed it, and the host request identity of that delivery, so host
/// provenance hydration classifies it as provider-attested rather than
/// host-confirmed. The content is the contract-aware extracted message text —
/// never envelope JSON and never the raw payload — and the host's own
/// untrusted-recall gate is what hardens it downstream.
fn native_staged_recall_candidate(
    call: &ProviderCall,
    row: &StagedRow,
    content: &str,
    truncated: bool,
    observed_micros: i64,
    request: &NativeRecallRequestV1,
) -> Result<Value, NativeReadFailure> {
    let observed_at =
        rfc3339_utc_micros(observed_micros).ok_or(NativeReadFailure::RecallProjectionInvalid)?;
    let score_millionths = staged_score_millionths(row.score);
    let raw_value = format!(
        "{}.{:06}",
        score_millionths / 1_000_000,
        score_millionths % 1_000_000
    );
    let mut limitations = vec![
        "staged session observation is advisory provider state, not a canonical fact".to_owned(),
        "native score is not host-normalized".to_owned(),
    ];
    if truncated {
        limitations.push("content truncated to the staged candidate byte cap".to_owned());
    }
    let exact_scope_identity = if common_recall(request) {
        let mut scope = exact_scope_value(call);
        scope["scope_binding"] = serde_json::json!("exact_coding_scope");
        scope
    } else {
        staged_scope_attestation(row)
    };
    let mut candidate = serde_json::json!({
        "candidate_id": format!("{}:{}", call.request_id, row.provider_reference),
        "stable_memory_ref": row.provider_reference,
        "content": content,
        "content_ref": Value::Null,
        "content_sha256": sha256_hex(content.as_bytes()),
        "native_score": {
            "score_domain_id": STAGED_RECALL_SCORE_DOMAIN,
            "score_domain_version": RECALL_SCORE_DOMAIN_VERSION,
            "raw_value": raw_value,
            "direction": "higher_is_better",
            "declared_minimum": "0.000000",
            "declared_maximum": "1.000000",
            "calibration_state": "provider_calibrated",
            "semantics": "staged session observation: admission recency and query lexical \
                          overlap; fixed-point millionths",
            "components": {
                "score_millionths": score_millionths,
                "admitted_sequence": row.admitted_sequence,
            },
        },
        "confidence": Value::Null,
        "exact_scope_identity": exact_scope_identity,
        "validity": {
            "observed_at": observed_at,
            "valid_from": row.validity.valid_from_utc_nanos.and_then(format_rfc3339_nanos),
            "valid_until": row.validity.valid_until_utc_nanos.and_then(format_rfc3339_nanos),
            "superseded_at": row.validity.superseded_at_utc_nanos.and_then(format_rfc3339_nanos),
            "superseded_by": row.validity.superseded_by,
            "revoked_at": row.validity.revoked_at_utc_nanos.and_then(format_rfc3339_nanos),
            "source_revision": row.source_revision,
            "temporal_state": staged_temporal_state(row,request)?,
        },
        "provenance": {
            "state": "available",
            // Provider-attested only. `provider_reference` names the staged
            // row, `operation:` the provider operation that committed it, and
            // `request:` the host request identity of the same delivery.
            "origin_refs": [
                row.provider_reference,
                format!("operation:{}", row.operation_id),
                format!("request:{}", row.request_identity),
            ],
            "observation_refs": staged_source_values(row,"observation_id"),
            "source_refs": staged_source_values(row,"source_key"),
            "native_linkage": {
                "staged_observation": {
                    "state": "available",
                    "observation_kind": row.observation_kind,
                    "payload_contract": row.payload_contract,
                    "payload_sha256": row.payload_sha256,
                    "source_authority": row.source_authority,
                    "source_event_id": row.source_event_id,
                    "receipt": row.receipt,
                    "effect_digest": row.effect_digest,
                    "origin_scope": if common_recall(request) {
                        row.original_source.as_ref().and_then(|source|source.get("origin_scope")).cloned().unwrap_or_else(||serde_json::json!({"state":"unavailable"}))
                    } else {serde_json::json!({
                        "agent_session_id": row.scope.agent_session_id,
                        "resolved_scope_digest": row.scope.resolved_scope_digest,
                        "exact_scope_sha256": row.exact_scope_sha256,
                    })},
                    "delivery_scope": super::native_staged_observations::scope_json(&row.scope),
                },
            },
            "transform_chain": [],
            "provider_trace_refs": if common_recall(request) {vec![format!("operation:{}",row.operation_id)]} else {Vec::<String>::new()},
            "redaction_reason": Value::Null,
        },
        "explanation": {
            "summary": "staged session observation of this checkout",
            "matched_features": [],
            "activation_trace_refs": [],
            "native_linkage_ref": "provenance.native_linkage",
            "native_score_ref": "native_score",
            "limitations": limitations,
        },
        "source_refs": staged_source_values(row,"source_key"),
        "trace_refs": if common_recall(request) {vec![format!("operation:{}",row.operation_id)]} else {Vec::<String>::new()},
        "sensitivity": "unknown",
        "memory_class": "session_observation",
        "warnings": [],
        "extensions": [],
    });
    if let Some(original_source) = &row.original_source {
        candidate["provenance"]["original_sources"] = serde_json::json!([original_source]);
    }
    Ok(candidate)
}

/// The checkout claim binds the stored origin's five checkout fields.
/// Session and resolved-scope fields must be present and empty in the claim;
/// their original values remain only in staged-observation origin provenance.
fn staged_scope_attestation(row: &StagedRow) -> Value {
    serde_json::json!({
        "scope_binding": "checkout_observations",
        "profile_id": row.scope.profile_id,
        "project_id": row.scope.project_id,
        "repository_identity": row.scope.repository_identity,
        "worktree_identity": row.scope.worktree_identity,
        "branch_identity": row.scope.branch_identity,
        "agent_session_id": "",
        "resolved_scope_digest": "",
    })
}

fn native_score_decimal(millionths: u32) -> String {
    format!("{}.{:06}", millionths / 1_000_000, millionths % 1_000_000)
}

fn fact_source_refs(fact: &FactV1) -> Vec<String> {
    match &fact.source {
        FactIdentitySourceResultV1::Evidence {
            anchor_id,
            stable_key,
        } => vec![anchor_id.to_string(), stable_key.to_string()],
        FactIdentitySourceResultV1::Application { operation_id } => vec![operation_id.to_string()],
    }
}

/// Outcome-envelope scope binding: the request scope this reply answers,
/// which the adapter verified byte-for-byte against the call before
/// searching. It is a binding to the request, never an attestation about any
/// candidate; per-candidate scope comes from
/// [`native_fact_scope_attestation`].
fn exact_scope_value(call: &ProviderCall) -> Value {
    serde_json::json!({
        "profile_id": call.exact_scope.profile_id,
        "project_id": call.exact_scope.project_id,
        "repository_identity": call.exact_scope.repository_identity,
        "worktree_identity": call.exact_scope.worktree_identity,
        "branch_identity": call.exact_scope.branch_identity,
        "agent_session_id": call.exact_scope.agent_session_id,
        "resolved_scope_digest": call.exact_scope.resolved_scope_digest,
    })
}

/// Scope identity the adapter attests for one Native fact, under the
/// binding that names exactly which fields it vouches for.
///
/// A Native fact record carries only its owner; the retained project store
/// has no repository, worktree, branch, session, or resolved-scope dimension,
/// and the current-fact search returns every fact of the project regardless
/// of which checkout or session committed it. A project-owned fact is
/// therefore attested as `project_facts`: the project identity proven by the
/// fact owner and the profile identity of the daemon that mounted this
/// adapter (never the profile named in the call), with the optional checkout
/// fields left empty and the forbidden session and digest fields empty. A
/// profile-owned fact is attested as `profile_facts` with only the mount
/// profile. The host admission authority applies the binding's rules, so a
/// candidate can never be admitted wearing the requester's worktree, branch,
/// or session identity.
fn native_fact_scope_attestation(fact: &FactV1, profile_id: &UserProfileId) -> Value {
    let (scope_binding, project_id) = match &fact.owner {
        FactCommitOwnerV1::Project { project_id } => ("project_facts", project_id.as_str()),
        FactCommitOwnerV1::Profile => ("profile_facts", ""),
    };
    serde_json::json!({
        "scope_binding": scope_binding,
        "profile_id": profile_id.as_str(),
        "project_id": project_id,
        "repository_identity": "",
        "worktree_identity": "",
        "branch_identity": "",
        "agent_session_id": "",
        "resolved_scope_digest": "",
    })
}

fn native_recall_response_value(
    call: &ProviderCall,
    request: &NativeRecallRequestV1,
    candidates: &[Value],
    matched_items: u64,
    excluded_items: u64,
    truncated_items: u64,
    reasons: &[String],
    next_after: Option<&tracedecay_contracts::retained_surfaces::FactSearchCursorV1>,
) -> Value {
    let state = if !reasons.is_empty() || truncated_items > 0 {
        "partial"
    } else if candidates.is_empty() {
        "zero_results"
    } else {
        "complete"
    };
    let next_cursor = next_after.map(|cursor| {
        format!(
            "score:{}:updated:{}:fact:{}",
            cursor.score_millionths, cursor.updated_at.0, cursor.fact_id
        )
    });
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
            "scanned_items": matched_items,
            "matched_items": matched_items,
            "returned_items": candidates.len(),
            "excluded_items": excluded_items,
            "truncated_items": truncated_items,
            "next_cursor": next_cursor,
            "reasons": reasons,
        },
        "ordering": {
            "score_domain_id": RECALL_SCORE_DOMAIN,
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

fn graph_coverage_reasons(coverage: FactSearchGraphCoverageV1) -> Vec<String> {
    match coverage {
        FactSearchGraphCoverageV1::NotApplicable | FactSearchGraphCoverageV1::NotMounted => {
            Vec::new()
        }
        FactSearchGraphCoverageV1::Complete { .. } => Vec::new(),
        FactSearchGraphCoverageV1::Degraded { reason } => vec![match reason {
            tracedecay_contracts::retained_surfaces::FactSearchGraphDegradationV1::Conflict => {
                "graph_conflict"
            }
            tracedecay_contracts::retained_surfaces::FactSearchGraphDegradationV1::Unavailable => {
                "graph_unavailable"
            }
            tracedecay_contracts::retained_surfaces::FactSearchGraphDegradationV1::BudgetExhausted => {
                "graph_budget_exhausted"
            }
            tracedecay_contracts::retained_surfaces::FactSearchGraphDegradationV1::DeadlineExceeded => {
                "graph_deadline_exceeded"
            }
        }
        .to_owned()],
    }
}

fn push_reason(reasons: &mut Vec<String>, reason: &str) {
    if !reasons.iter().any(|value| value == reason) {
        reasons.push(reason.to_owned());
    }
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
        | RetainedSurfaceExecutionErrorV1::Unavailable { .. }
        | RetainedSurfaceExecutionErrorV1::ProfileResetRequired
        | RetainedSurfaceExecutionErrorV1::ProjectResetRequired => {
            NativeReadFailure::ProviderUnavailable
        }
    }
}

impl ProjectNativeMemoryApplicationPort {
    fn actual_capability_states(&self) -> Vec<Value> {
        self.descriptor.capabilities.iter().map(|capability| {
            serde_json::json!({"capability_id":capability.as_str(),"state":"available"})
        }).collect()
    }

    fn complete_health_response(
        &self,
        outcome: &mut StagedControlOutcome,
        accepted: &NativeAcceptedReadiness,
    ) -> Result<(), NativeReadFailure> {
        // Namespace/schema/scope/revision identity, not a checksum of stored content.
        let state_identity = serde_json::json!({
            "state_namespace": accepted.state_namespace,
            "state_schema_version": self.descriptor.state_schema_version,
            "scope_digest": accepted.exact_scope_sha256,
            "state_generation": outcome.generation_after,
        });
        let state_bytes = serde_json::to_vec(&state_identity)
            .map_err(|_| NativeReadFailure::ProviderUnavailable)?;
        let mut limits_digest = Sha256::new();
        digest_native_limits(&mut limits_digest, accepted.effective_limits);
        let Value::Object(response) = &mut outcome.response else {
            return Err(NativeReadFailure::ProviderUnavailable);
        };
        response.extend([
            (
                "provider_id".to_owned(),
                self.descriptor.provider_id.as_str().into(),
            ),
            (
                "provider_instance_id".to_owned(),
                accepted.provider_instance_id.clone().into(),
            ),
            (
                "implementation_identity_digest".to_owned(),
                self.descriptor
                    .implementation_identity_sha256
                    .clone()
                    .into(),
            ),
            (
                "state_identity_digest".to_owned(),
                sha256_hex(&state_bytes).into(),
            ),
            (
                "state_generation".to_owned(),
                outcome.generation_after.into(),
            ),
            (
                "scope_digest".to_owned(),
                accepted.exact_scope_sha256.clone().into(),
            ),
            (
                "capability_states".to_owned(),
                self.actual_capability_states().into(),
            ),
            (
                "effective_limits_digest".to_owned(),
                hex::encode(limits_digest.finalize()).into(),
            ),
            (
                "backlog".to_owned(),
                self.actor.queued_requests.load(Ordering::Acquire).into(),
            ),
        ]);
        Ok(())
    }

    fn complete_capability_response(
        &self,
        outcome: &mut StagedControlOutcome,
        call: &ProviderCall,
        request: &Value,
        accepted: &NativeAcceptedReadiness,
    ) -> Result<(), NativeReadFailure> {
        let Value::Object(response) = &mut outcome.response else {
            return Err(NativeReadFailure::ProviderUnavailable);
        };
        response.extend([
            ("items".to_owned(), self.actual_capability_states().into()),
            (
                "state_generation".to_owned(),
                outcome.generation_after.into(),
            ),
            ("coverage".to_owned(), "complete".into()),
            ("next_cursor".to_owned(), Value::Null),
            ("redactions".to_owned(), serde_json::json!([])),
            ("warnings".to_owned(), serde_json::json!([])),
        ]);
        super::native_staged_observations::paginate_inspection_evidence(
            outcome,
            call,
            request,
            accepted.effective_limits.inspection_items,
            accepted.effective_limits.response_bytes,
        )
        .map_err(store_failure)
    }

    fn lifecycle_call(&self, call: &ProviderCall) -> ProviderReply {
        if call.validate().is_err() || call.provider_id.as_str() != NATIVE_PROVIDER_ID {
            return self.observe_invalid(call);
        }
        if let Err(failure) = control_failure(&call.control) {
            return self.observe_failure(call, failure);
        }
        let inspection_request = if call.operation == ProviderOperation::Inspection {
            match serde_json::from_slice::<Value>(&call.payload.bytes) {
                Ok(request) => Some(request),
                Err(_) => return self.observe_invalid(call),
            }
        } else {
            None
        };
        let capability_status = inspection_request
            .as_ref()
            .is_some_and(|request| request["view"] == "capability_status");
        let descriptor_readiness =
            if call.operation == ProviderOperation::Health || capability_status {
                let latest = match self.accepted_readiness.lock() {
                    Ok(latest) => latest,
                    Err(_) => {
                        return self.observe_failure(call, NativeReadFailure::ProviderUnavailable);
                    }
                };
                match latest.as_ref() {
                    Some(accepted)
                        if accepted.registration_revision == call.registration_revision
                            && accepted.exact_scope_sha256 == call_scope_digest(call)
                            && accepted.ready_receipt_sha256 == call.ready_receipt_sha256 =>
                    {
                        Some(accepted.clone())
                    }
                    _ => return self.observe_failure(call, NativeReadFailure::ProviderUnavailable),
                }
            } else {
                None
            };
        match self
            .actor
            .dispatch_control(call.clone(), self.admission_authority.clone())
        {
            Ok(mut outcome) => {
                let (generation, maximum_response_bytes) = if let Some(accepted) =
                    descriptor_readiness
                {
                    let generation = outcome.generation_after;
                    let completed = if let Some(request) = inspection_request.as_ref() {
                        self.complete_capability_response(&mut outcome, call, request, &accepted)
                    } else {
                        self.complete_health_response(&mut outcome, &accepted)
                    };
                    if let Err(failure) = completed {
                        return self.observe_failure(call, failure);
                    }
                    (generation, accepted.effective_limits.response_bytes)
                } else if inspection_request
                    .as_ref()
                    .is_some_and(|request| request["view"] == "maintenance_receipt")
                {
                    (
                        outcome.generation_after,
                        native_provider_limits().response_bytes,
                    )
                } else {
                    (
                        self.staged
                            .generation()
                            .unwrap_or(call.expected_state_generation),
                        native_provider_limits().response_bytes,
                    )
                };
                native_control_reply(call, outcome, generation, maximum_response_bytes)
            }
            Err(failure) => self.observe_failure(call, failure),
        }
    }
}

pub(super) fn native_control_reply(
    call: &ProviderCall,
    outcome: StagedControlOutcome,
    live_generation: u64,
    maximum_response_bytes: u64,
) -> ProviderReply {
    let effect = if outcome.duplicate {
        CommittedEffectEvidence::duplicate(
            live_generation,
            call.idempotency_key.clone().unwrap_or_default(),
            outcome.operation_id,
            outcome.receipt.clone(),
        )
    } else if outcome.changed {
        CommittedEffectEvidence::committed(
            outcome.generation_before,
            outcome.generation_after,
            vec![format!("native-operation:{}", outcome.receipt)],
            outcome.receipt.clone(),
            outcome.receipt.clone(),
        )
    } else {
        Ok(CommittedEffectEvidence::none(Some(live_generation)))
    };
    let reply = effect.and_then(|effect| {
        let terminal = TerminalRecord::new(
            call.operation,
            call.provider_id.clone(),
            TerminalCode::Success,
            effect,
            FallbackDirective::forbidden(),
            call.operation_id.clone(),
            call_scope_digest(call),
            None,
        )?;
        let bytes = serde_json::to_vec(&outcome.response).unwrap_or_default();
        let payload = CanonicalPayload::new(
            call.payload.contract_id.clone(),
            bytes.clone(),
            sha256_hex(&bytes),
        )?;
        Ok(ProviderReply {
            terminal,
            payload: Some(payload),
            warnings: Vec::new(),
            extensions: call.extensions.clone(),
            state_generation: live_generation,
        })
    });
    match reply {
        Ok(reply) if reply.validate(maximum_response_bytes).is_ok() => reply,
        _ if call.operation.mutates_provider_state() => unknown_store_reply(call),
        _ => ProviderReply {
            terminal: TerminalRecord::failure_before_dispatch(
                call.operation,
                call.provider_id.clone(),
                TerminalCode::CapacityExceeded,
                &call.operation_id,
                call_scope_digest(call),
                Some(live_generation),
                "native.lifecycle_response_budget",
            ),
            payload: None,
            warnings: Vec::new(),
            extensions: Vec::new(),
            state_generation: live_generation,
        },
    }
}

fn unknown_store_reply(call: &ProviderCall) -> ProviderReply {
    let digest: [u8; 32] = Sha256::digest(
        format!(
            "native-reconcile:{}:{}",
            call.operation_id,
            call.idempotency_key.as_deref().unwrap_or_default()
        )
        .as_bytes(),
    )
    .into();
    ProviderReply {
        terminal: TerminalRecord::effect_unknown_for_call(
            call,
            digest,
            "native.operation_reconciliation_required",
        ),
        payload: None,
        warnings: Vec::new(),
        extensions: Vec::new(),
        state_generation: call.expected_state_generation,
    }
}

fn store_failure(error: StagedStoreError) -> NativeReadFailure {
    match error {
        StagedStoreError::CommitUnknown(_) => NativeReadFailure::StagedEffectUnknown,
        StagedStoreError::ControlEnded(TerminalCode::Cancelled) => NativeReadFailure::Cancelled,
        StagedStoreError::ControlEnded(_) => NativeReadFailure::DeadlineExceeded,
        StagedStoreError::ValueOutOfRange { .. } => NativeReadFailure::RecallBudgetExhausted,
        StagedStoreError::InvalidAdvisory(_) | StagedStoreError::EmptyField { .. } => {
            NativeReadFailure::InvalidPayload
        }
        StagedStoreError::PrivacyDeleted | StagedStoreError::LifecycleConflict(_) => {
            NativeReadFailure::StagedConflict
        }
        _ => NativeReadFailure::StagedStoreUnavailable,
    }
}

impl NativeReadActor {
    fn enqueue_store(&self, command: NativeReadCommand) -> Result<(), NativeReadFailure> {
        let sender = self
            .sender
            .lock()
            .ok()
            .and_then(|sender| sender.as_ref().cloned())
            .ok_or(NativeReadFailure::ProviderUnavailable)?;
        self.queued_requests.fetch_add(1, Ordering::AcqRel);
        // The reservation is released on dequeue, failed send, or receiver drop.
        sender
            .try_send(QueuedNativeReadCommand {
                command,
                queued: NativeQueuedRequest(Arc::clone(&self.queued_requests)),
            })
            .map_err(|_| NativeReadFailure::ProviderUnavailable)
    }

    fn dispatch_staged(
        &self,
        call: ProviderCall,
        record: StagedObservationRecord,
        authority: Option<Arc<dyn tracedecay_memory_provider_registry::AdvisoryAdmissionAuthority>>,
    ) -> Result<StagedOutcome, NativeReadFailure> {
        let (reply, receiver) = mpsc::sync_channel(1);
        let control = call.control.clone();
        self.enqueue_store(NativeReadCommand::Stage {
            call,
            record,
            authority,
            reply,
        })?;
        receive_actor_reply(&control, receiver)
            .map_err(|_| NativeReadFailure::StagedEffectUnknown)?
    }

    fn dispatch_control(
        &self,
        call: ProviderCall,
        authority: Option<Arc<dyn tracedecay_memory_provider_registry::AdvisoryAdmissionAuthority>>,
    ) -> Result<StagedControlOutcome, NativeReadFailure> {
        let (reply, receiver) = mpsc::sync_channel(1);
        let control = call.control.clone();
        let mutation = !matches!(
            call.operation,
            ProviderOperation::Health
                | ProviderOperation::Inspection
                | ProviderOperation::SnapshotExport
        );
        self.enqueue_store(NativeReadCommand::Control {
            call,
            authority,
            reply,
        })?;
        receive_actor_reply(&control, receiver).map_err(|failure| {
            if mutation {
                NativeReadFailure::StagedEffectUnknown
            } else {
                failure
            }
        })?
    }
}

fn common_recall(request: &NativeRecallRequestV1) -> bool {
    request.common_profile
        || request
            .required_capabilities
            .iter()
            .any(|capability| capability == "memory.advisory_common.v1")
}

/// Parses the common wire without discarding nanosecond source evidence.
pub(crate) fn parse_rfc3339_nanos(value: &str) -> Option<i64> {
    if !value.ends_with('Z') {
        return None;
    }
    let micros = parse_rfc3339_micros(value)?;
    let fraction = value
        .get(19..)?
        .strip_prefix('.')
        .map(|fraction| fraction.trim_end_matches('Z'))
        .unwrap_or("");
    let remainder = if fraction.len() > 6 {
        let mut tail = fraction[6..].parse::<i64>().ok()?;
        for _ in fraction.len()..9 {
            tail = tail.checked_mul(10)?;
        }
        tail
    } else {
        0
    };
    micros.checked_mul(1000)?.checked_add(remainder)
}

pub(crate) fn format_rfc3339_nanos(value: i64) -> Option<String> {
    let seconds = value.div_euclid(1_000_000_000);
    let base = rfc3339_utc_micros(seconds.checked_mul(1_000_000)?)?;
    Some(format!(
        "{}.{:09}Z",
        base.get(..19)?,
        value.rem_euclid(1_000_000_000)
    ))
}

fn owned_temporal_query(
    temporal: &NativeRecallTemporalQueryV1,
) -> Result<tracedecay_memory_provider_registry::OwnedTemporalQuery, NativeReadFailure> {
    use tracedecay_memory_provider_registry::{
        CommonUnknownValidityPolicy, OwnedTemporalQuery, TemporalMode,
    };
    let invalid = NativeReadFailure::RecallInvalidRequest;
    let timestamp = |value: &Value| -> Result<Option<i64>, NativeReadFailure> {
        match value {
            Value::Null => Ok(None),
            Value::String(text) => parse_rfc3339_nanos(text).map(Some).ok_or(invalid),
            _ => Err(invalid),
        }
    };
    Ok(OwnedTemporalQuery {
        mode: match temporal.mode.as_str() {
            "current" => TemporalMode::Current,
            "as_of" => TemporalMode::AsOf,
            "interval" => TemporalMode::Interval,
            "history" => TemporalMode::History,
            _ => return Err(invalid),
        },
        evaluation_time_utc_nanos: parse_rfc3339_nanos(&temporal.evaluation_time).ok_or(invalid)?,
        as_of_utc_nanos: timestamp(&temporal.as_of)?,
        interval_start_utc_nanos: timestamp(&temporal.interval_start)?,
        interval_end_utc_nanos: timestamp(&temporal.interval_end)?,
        include_superseded: temporal.include_superseded,
        include_revoked: temporal.include_revoked,
        unknown_validity_policy: match temporal.unknown_validity_policy.as_str() {
            "exclude" => CommonUnknownValidityPolicy::Exclude,
            "degrade" => CommonUnknownValidityPolicy::Degrade,
            "allow_with_warning" => CommonUnknownValidityPolicy::AllowWithWarning,
            _ => return Err(invalid),
        },
    })
}

fn staged_row_excluded(
    row: &StagedRow,
    call: &ProviderCall,
    exclusions: &NativeRecallExclusionsV1,
) -> bool {
    let observation = row
        .original_source
        .as_ref()
        .and_then(|value| value.pointer("/source/observation_id"))
        .and_then(Value::as_str);
    let source_key = row
        .original_source
        .as_ref()
        .and_then(|value| value.pointer("/source/source_key"))
        .and_then(Value::as_str);
    exclusions
        .stable_memory_refs
        .contains(&row.provider_reference)
        || exclusions
            .candidate_ids
            .contains(&format!("{}:{}", call.request_id, row.provider_reference))
        || exclusions
            .content_sha256
            .contains(&sha256_hex(row.message_text.as_bytes()))
        || exclusions
            .observation_ids
            .iter()
            .any(|value| Some(value.as_str()) == observation || value == &row.source_event_id)
        || exclusions
            .source_refs
            .iter()
            .any(|value| Some(value.as_str()) == source_key)
        || exclusions
            .trace_refs
            .iter()
            .any(|value| value == &format!("operation:{}", row.operation_id))
}

fn current_row_disposition(
    row: &StagedRow,
    request: &NativeRecallRequestV1,
) -> tracedecay_memory_provider_registry::SourceDisposition {
    use tracedecay_memory_provider_registry::SourceDisposition;
    let state = request
        .history_grant
        .as_ref()
        .and_then(|grant| grant.get("sources"))
        .and_then(Value::as_array)
        .and_then(|sources| {
            sources
                .iter()
                .find(|source| row.original_source.as_ref() == source.get("attribution"))
        })
        .and_then(|source| source.pointer("/current_disposition/state"))
        .and_then(Value::as_str);
    match state {
        None | Some("available") => SourceDisposition::Available,
        Some("superseded") => SourceDisposition::Superseded,
        Some("revoked") => SourceDisposition::Revoked,
        Some("deleted") => SourceDisposition::Deleted,
        Some("redacted") => SourceDisposition::Redacted,
        Some("expired") => SourceDisposition::Expired,
        _ => SourceDisposition::Unknown,
    }
}

fn history_permits(row: &StagedRow, call: &ProviderCall, request: &NativeRecallRequestV1) -> bool {
    if row.scope == call.exact_scope {
        return true;
    }
    let Some(grant) = &request.history_grant else {
        return false;
    };
    if super::native_staged_observations::exact_scope_from_value(&grant["destination_scope"])
        .ok()
        .as_ref()
        != Some(&call.exact_scope)
        || grant["relation"] != "same_checkout"
    {
        return false;
    }
    grant
        .get("sources")
        .and_then(Value::as_array)
        .is_some_and(|sources| {
            sources.iter().any(|source| {
                row.original_source.as_ref() == source.get("attribution")
                    && matches!(
                        source
                            .pointer("/current_disposition/state")
                            .and_then(Value::as_str),
                        Some("available" | "superseded" | "revoked")
                    )
            })
        })
}

fn staged_source_values(row: &StagedRow, field: &str) -> Vec<String> {
    row.original_source
        .as_ref()
        .and_then(|source| source.get("source"))
        .and_then(|source| source.get(field))
        .and_then(Value::as_str)
        .map(|value| vec![value.to_owned()])
        .unwrap_or_default()
}

fn staged_temporal_state(
    row: &StagedRow,
    request: &NativeRecallRequestV1,
) -> Result<&'static str, NativeReadFailure> {
    let query = owned_temporal_query(&request.temporal_query)?;
    let at = query
        .as_of_utc_nanos
        .unwrap_or(query.evaluation_time_utc_nanos);
    let validity = &row.validity;
    if validity.valid_from_utc_nanos.is_none() {
        return Ok("unknown");
    }
    if validity
        .revoked_at_utc_nanos
        .is_some_and(|event| event <= at)
    {
        return Ok("revoked");
    }
    if validity
        .superseded_at_utc_nanos
        .is_some_and(|event| event <= at)
    {
        return Ok("superseded");
    }
    if validity
        .valid_until_utc_nanos
        .is_some_and(|until| until <= at)
    {
        return Ok("expired");
    }
    if validity.valid_from_utc_nanos.is_some_and(|from| from > at) {
        return Ok("future");
    }
    Ok("current")
}

fn admission_failure(
    error: tracedecay_memory_provider_registry::AdvisoryAdmissionError,
) -> NativeReadFailure {
    use tracedecay_memory_provider_registry::AdvisoryAdmissionError;
    match error {
        AdvisoryAdmissionError::Control(TerminalCode::Cancelled) => NativeReadFailure::Cancelled,
        AdvisoryAdmissionError::Control(_) => NativeReadFailure::DeadlineExceeded,
        AdvisoryAdmissionError::Unavailable(_) => NativeReadFailure::ProviderUnavailable,
        AdvisoryAdmissionError::Denied(_) | AdvisoryAdmissionError::BindingMismatch => {
            NativeReadFailure::Unauthorized
        }
        _ => NativeReadFailure::InvalidPayload,
    }
}

fn admit_native_call(
    authority: Option<&dyn tracedecay_memory_provider_registry::AdvisoryAdmissionAuthority>,
    call: &ProviderCall,
) -> Result<Option<tracedecay_memory_provider_registry::CurrentAdvisoryAdmission>, NativeReadFailure>
{
    use tracedecay_memory_provider_registry::SourceDisposition;
    let payload: Value = serde_json::from_slice(&call.payload.bytes)
        .map_err(|_| NativeReadFailure::InvalidPayload)?;
    let original = payload.pointer("/source_identity/original_source");
    let cross_origin = original
        .and_then(|source| source.pointer("/origin_scope/exact_scope_identity"))
        .is_some_and(|scope| {
            super::native_staged_observations::exact_scope_from_value(scope)
                .ok()
                .as_ref()
                != Some(&call.exact_scope)
        });
    let required = matches!(
        call.operation,
        ProviderOperation::Replay | ProviderOperation::SnapshotRestore
    ) || call.history_grant().is_some()
        || payload
            .get("history_grant")
            .is_some_and(|grant| !grant.is_null())
        || (call.operation == ProviderOperation::Observe && cross_origin);
    if !required {
        return Ok(None);
    }
    let authority = authority.ok_or(NativeReadFailure::ProviderUnavailable)?;
    let admission = authority.admit(call).map_err(admission_failure)?;
    admission.verify_for(call).map_err(admission_failure)?;
    if let Some(grant) = call.history_grant() {
        if grant.sources.len() != admission.history_sources.len() {
            return Err(NativeReadFailure::RecallScopeMismatch);
        }
        let mut covered = std::collections::BTreeSet::new();
        for claimed in &grant.sources {
            let index = admission
                .history_sources
                .iter()
                .position(|trusted| claimed.attribution == trusted.attribution)
                .ok_or(NativeReadFailure::RecallScopeMismatch)?;
            if !covered.insert(index) {
                return Err(NativeReadFailure::RecallScopeMismatch);
            }
        }
    }
    if let Some(sources) = payload
        .pointer("/history_grant/sources")
        .and_then(Value::as_array)
    {
        if sources.len() != admission.history_sources.len() {
            return Err(NativeReadFailure::RecallScopeMismatch);
        }
        let mut covered = std::collections::BTreeSet::new();
        for source in sources {
            let index = admission
                .history_sources
                .iter()
                .position(|trusted| {
                    attribution_matches(&source["attribution"], &trusted.attribution)
                })
                .ok_or(NativeReadFailure::RecallScopeMismatch)?;
            if !covered.insert(index) {
                return Err(NativeReadFailure::RecallScopeMismatch);
            }
        }
    }
    if cross_origin {
        let trusted = admission
            .history_sources
            .iter()
            .find(|trusted| {
                original.is_some_and(|source| attribution_matches(source, &trusted.attribution))
            })
            .ok_or(NativeReadFailure::RecallScopeMismatch)?;
        if !matches!(
            trusted.current_disposition.state,
            SourceDisposition::Available
                | SourceDisposition::Superseded
                | SourceDisposition::Revoked
        ) {
            return Err(NativeReadFailure::RecallScopeMismatch);
        }
    }
    if call.operation == ProviderOperation::Replay {
        let items = payload
            .get("resolved_observations")
            .and_then(Value::as_array)
            .ok_or(NativeReadFailure::InvalidPayload)?;
        for item in items {
            let source = item
                .pointer("/observation/source_identity/original_source")
                .ok_or(NativeReadFailure::InvalidPayload)?;
            if !admission
                .history_sources
                .iter()
                .any(|trusted| attribution_matches(source, &trusted.attribution))
            {
                return Err(NativeReadFailure::RecallScopeMismatch);
            }
        }
    }
    if call.operation == ProviderOperation::SnapshotRestore {
        let restored = admission
            .restore
            .as_ref()
            .ok_or(NativeReadFailure::RecallScopeMismatch)?;
        restored
            .validate_for(&call.exact_scope)
            .map_err(|_| NativeReadFailure::RecallScopeMismatch)?;
        let sources = payload
            .get("source_dispositions")
            .and_then(Value::as_array)
            .ok_or(NativeReadFailure::InvalidPayload)?;
        if sources.len() != restored.sources.len() {
            return Err(NativeReadFailure::RecallScopeMismatch);
        }
        for source in sources {
            if !restored
                .sources
                .iter()
                .any(|(identity, _)| source_identity_matches(&source["source"], identity))
            {
                return Err(NativeReadFailure::RecallScopeMismatch);
            }
        }
    }
    control_failure(&call.control)?;
    Ok(Some(admission))
}

// Only the actor's fresh, call-bound host result is projected into local processing.
// The original payload remains unchanged for binding and idempotency evidence.
pub(super) fn apply_current_admission(
    request: &mut Value,
    admission: &tracedecay_memory_provider_registry::CurrentAdvisoryAdmission,
) {
    if let Some(sources) = request
        .pointer_mut("/history_grant/sources")
        .and_then(Value::as_array_mut)
    {
        for source in sources {
            if let Some(trusted) = admission
                .history_sources
                .iter()
                .find(|trusted| attribution_matches(&source["attribution"], &trusted.attribution))
            {
                source["current_disposition"] =
                    current_disposition_value(&trusted.current_disposition);
            }
        }
    }
    if let Some(restore) = &admission.restore {
        request["disposition_checkpoint"] = serde_json::json!({
            "exact_scope":super::native_staged_observations::scope_json(&restore.checkpoint.exact_scope),
            "authority_ref":restore.checkpoint.authority_ref,
            "authority_revision":restore.checkpoint.authority_revision,
            "checked_at":format_rfc3339_nanos(restore.checkpoint.checked_at_utc_nanos)
        });
        if let Some(sources) = request
            .get_mut("source_dispositions")
            .and_then(Value::as_array_mut)
        {
            for source in sources {
                if let Some((_, disposition)) = restore
                    .sources
                    .iter()
                    .find(|(identity, _)| source_identity_matches(&source["source"], identity))
                {
                    source["current_disposition"] = current_disposition_value(disposition);
                }
            }
        }
    }
}

fn current_disposition_value(
    disposition: &tracedecay_memory_provider_registry::CurrentSourceDisposition,
) -> Value {
    serde_json::json!({"state":disposition.state.as_wire(),"authority_ref":disposition.authority_ref,
        "authority_revision":disposition.authority_revision,"checked_at":format_rfc3339_nanos(disposition.checked_at_utc_nanos)})
}

pub(super) fn source_identity_matches(
    value: &Value,
    source: &tracedecay_memory_provider_registry::OriginalSourceIdentity,
) -> bool {
    value["canonical_provider_id"].as_str() == Some(source.canonical_provider_id.as_str())
        && value["canonical_session_id"].as_str() == Some(source.canonical_session_id.as_str())
        && value["source_key"].as_str() == Some(source.source_key.as_str())
        && value.get("stable_record_id").is_some()
        && value["stable_record_id"].as_str() == source.stable_record_id.as_deref()
        && value["observation_id"].as_str() == Some(source.observation_id.as_str())
        && value.get("source_revision").is_some()
        && value["source_revision"].as_str() == source.source_revision.as_deref()
        && value["content_sha256"].as_str() == Some(source.content_sha256.as_str())
}

pub(super) fn attribution_matches(
    value: &Value,
    source: &tracedecay_memory_provider_registry::SourceAttribution,
) -> bool {
    use tracedecay_memory_provider_registry::OriginScopeEvidence;
    let origin = &value["origin_scope"];
    let origin_matches = match &source.origin_scope {
        OriginScopeEvidence::Unavailable => origin["state"] == "unavailable",
        OriginScopeEvidence::IngestionOnly => origin["state"] == "ingestion_only",
        OriginScopeEvidence::Recorded {
            scope,
            authority_ref,
        } => {
            origin["state"] == "recorded"
                && origin["authority_ref"].as_str() == Some(authority_ref.as_str())
                && super::native_staged_observations::exact_scope_from_value(
                    &origin["exact_scope_identity"],
                )
                .ok()
                .as_ref()
                    == Some(scope)
        }
    };
    source_identity_matches(&value["source"], &source.source)
        && origin_matches
        && value["source_sequence"].as_u64() == Some(source.source_sequence)
        && value.get("occurred_at").is_some()
        && value["occurred_at"].as_str().and_then(parse_rfc3339_nanos)
            == source.occurred_at_utc_nanos
        && value["ingested_at"].as_str().and_then(parse_rfc3339_nanos)
            == Some(source.ingested_at_utc_nanos)
        && recorded_validity(Some(value)).ok().as_ref() == Some(&source.validity)
}

fn owned_exclusions(
    value: &NativeRecallExclusionsV1,
) -> tracedecay_memory_provider_registry::OwnedRecallExclusions {
    tracedecay_memory_provider_registry::OwnedRecallExclusions {
        stable_memory_refs: value.stable_memory_refs.clone(),
        candidate_ids: value.candidate_ids.clone(),
        source_refs: value.source_refs.clone(),
        trace_refs: value.trace_refs.clone(),
        observation_ids: value.observation_ids.clone(),
        content_sha256: value.content_sha256.clone(),
    }
}
