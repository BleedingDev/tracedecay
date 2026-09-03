#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(warnings)]
#![deny(clippy::dbg_macro)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(clippy::print_stderr)]
#![deny(clippy::print_stdout)]
#![deny(clippy::todo)]
#![deny(clippy::unimplemented)]
#![deny(clippy::unwrap_used)]
//! TraceDecay Native memory behind the provider-neutral runtime boundary.
//!
//! This crate is deliberately an adapter, not a second memory implementation.
//! It owns no database, index, scoring, curation, privacy, graph, or persistence
//! state. A future composition mount supplies the existing owner-bound Native
//! application port. The adapter validates the stable Native provider identity,
//! routes provider operations to narrow port methods, preserves canonical call
//! bytes and exact scope unchanged, and rejects undeclared optional operations
//! locally before contacting Native operation authority.
//!
//! Observation classification happens here — an admitted envelope is parsed
//! into one typed [`NativeObservation`] variant — but the durable consequence
//! of an accepted observation belongs entirely to the application port behind
//! this boundary. Staging a session message opens no store in this crate.

use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use serde_json::Value;
use tracedecay_memory_provider_api::contract::TerminalCode;
use tracedecay_memory_provider_api::{
    ApiError, HandshakeRequest, HandshakeResponse, MemoryProvider, ProviderCall,
    ProviderDescriptor, ProviderOperation, ProviderReply, TerminalRecord,
};

/// Stable logical provider identity for TraceDecay Native memory.
pub const NATIVE_PROVIDER_ID: &str = "tracedecay.native";

/// Recall candidate scope bindings the host authorizes Native to attest, in
/// the wire vocabulary of `tracedecay.memory.provider.recall.v1`
/// `candidate_scope_binding.bindings`.
///
/// Native facts carry only their owner — a project or the profile — and are
/// attested as `project_facts` or `profile_facts`. Native additionally
/// attests `exact_coding_scope`, because the Native application port also
/// answers recall from the session observations it staged as provider-local
/// advisory state: a staged row is recorded under the whole admitted exact
/// scope, so it is attested under that scope and nothing weaker.
///
/// `exact_coding_scope` admission compares every exact-scope field
/// byte-for-byte, `agent_session_id` and `resolved_scope_digest` included, so
/// a staged row is recallable only inside the session that produced it. That
/// is a deliberate limitation of this slice, not an oversight; a durable
/// cross-session binding for staged observations is tracked as `tdmem-b8q`.
///
/// The registry records this declaration at registration and passes it to
/// admission with the admitted call; a provider reply can never widen it.
pub const NATIVE_RECALL_SCOPE_BINDINGS: &[&str] =
    &["exact_coding_scope", "project_facts", "profile_facts"];

/// Provider-neutral contract carried by an admitted observation call.
pub const OBSERVATION_CONTRACT_ID: &str = "tracedecay.memory.provider.observation.v1";

/// Observation kind reserved for an explicitly authorized Native promotion
/// event.
pub const NATIVE_FACT_PROMOTION_OBSERVATION_KIND: &str = "native.fact_promoted.v1";

/// Payload contract paired with [`NATIVE_FACT_PROMOTION_OBSERVATION_KIND`].
pub const NATIVE_FACT_PROMOTION_PAYLOAD_CONTRACT_ID: &str =
    "tracedecay.memory.observation.native-fact-promotion.v1";

/// The one host observation kind Native stages as provider-local advisory
/// state, from `tracedecay.memory.provider.observation.v1`
/// `observation_kinds`.
///
/// Accepting a kind is a capability commitment: every accepted kind needs its
/// own candidate projection, retention behaviour, and containment tests. Only
/// this kind and [`NATIVE_FACT_PROMOTION_OBSERVATION_KIND`] are accepted;
/// every other contract-known kind stays on the unsupported path.
pub const NATIVE_STAGED_SESSION_OBSERVATION_KIND: &str = "session.message_committed.v1";

/// Payload contract paired with [`NATIVE_STAGED_SESSION_OBSERVATION_KIND`].
pub const NATIVE_STAGED_SESSION_PAYLOAD_CONTRACT_ID: &str =
    "tracedecay.memory.observation.session-message.v1";

const HANDSHAKE_CONTRACT_ID: &str = "tracedecay.memory.provider.handshake.v1";
const HEALTH_CONTRACT_ID: &str = "tracedecay.memory.provider.health.v1";
const RECALL_CONTRACT_ID: &str = "tracedecay.memory.provider.recall.v1";
const FEEDBACK_CONTRACT_ID: &str = "tracedecay.memory.provider.feedback.v1";
const MAINTENANCE_CONTRACT_ID: &str = "tracedecay.memory.provider.maintenance.v1";
const INSPECTION_CONTRACT_ID: &str = "tracedecay.memory.provider.inspection.v1";
const CORRECTION_CONTRACT_ID: &str = "tracedecay.memory.provider.correction.v1";
const DELETE_BY_SOURCE_CONTRACT_ID: &str = "tracedecay.memory.provider.deletion-by-source.v1";
const SNAPSHOT_EXPORT_CONTRACT_ID: &str = "tracedecay.memory.provider.snapshot-export.v1";
const SNAPSHOT_RESTORE_CONTRACT_ID: &str = "tracedecay.memory.provider.snapshot-restore.v1";
const REPLAY_CONTRACT_ID: &str = "tracedecay.memory.provider.replay.v1";

/// Construction failure before a Native adapter can be registered.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NativeAdapterError {
    /// The application port exposed an invalid or incomplete descriptor.
    InvalidDescriptor(ApiError),
    /// The supplied application port did not expose the stable Native identity.
    ProviderIdMismatch {
        /// Required stable identity.
        expected: &'static str,
        /// Identity declared by the supplied port.
        declared: String,
    },
}

impl fmt::Display for NativeAdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDescriptor(error) => {
                write!(
                    formatter,
                    "Native application port descriptor is invalid: {error}"
                )
            }
            Self::ProviderIdMismatch { expected, declared } => write!(
                formatter,
                "Native application port declared provider {declared}, expected {expected}"
            ),
        }
    }
}

impl Error for NativeAdapterError {}

/// The parsed view of an admitted observation envelope.
///
/// `call` is the original provider call, so its exact scope, request and
/// operation identities, idempotency key, control token, and opaque extensions
/// remain unchanged. The remaining fields are copied from the canonical JSON
/// envelope without semantic rewriting: the adapter never re-sanitizes,
/// reshapes, or re-derives what admission already sanitized and bound to a
/// receipt.
#[derive(Clone, Debug)]
pub struct NativeObservationEnvelope<'call> {
    /// The original admitted provider call.
    pub call: &'call ProviderCall,
    /// Exact `observation_kind` from the canonical envelope.
    pub observation_kind: String,
    /// Exact `payload_contract` from the canonical envelope.
    pub payload_contract: String,
    /// Parsed `canonical_payload` from the canonical envelope.
    pub canonical_payload: Value,
}

/// One admitted observation envelope, classified into the exact Native
/// consequence its kind authorizes.
///
/// The classification is the authorization: the adapter accepts exactly two
/// kinds and the application port branches on this enum rather than
/// re-reading `observation_kind`, so a kind can never acquire a consequence
/// it was not admitted for. Every other kind is refused before dispatch.
#[derive(Clone, Debug)]
pub enum NativeObservation<'call> {
    /// [`NATIVE_FACT_PROMOTION_OBSERVATION_KIND`]: an explicitly authorized
    /// Native promotion event.
    ///
    /// Receiving this variant is verification-only and does not by itself
    /// authorize a fact write; the port re-runs Native validation and owns
    /// the durable receipt.
    FactPromotion(NativeObservationEnvelope<'call>),
    /// [`NATIVE_STAGED_SESSION_OBSERVATION_KIND`]: a canonically settled host
    /// session message the port stages as provider-local advisory state.
    ///
    /// Staging writes no canonical Native fact. A staged row can become an
    /// accepted fact only through the separate, explicitly authorized
    /// promotion path.
    StagedSession(NativeObservationEnvelope<'call>),
}

impl<'call> NativeObservation<'call> {
    /// The canonical envelope carried by whichever variant this is.
    #[must_use]
    pub const fn envelope(&self) -> &NativeObservationEnvelope<'call> {
        match self {
            Self::FactPromotion(envelope) | Self::StagedSession(envelope) => envelope,
        }
    }

    /// The original admitted provider call.
    #[must_use]
    pub const fn call(&self) -> &'call ProviderCall {
        self.envelope().call
    }

    /// Exact `observation_kind` from the canonical envelope.
    #[must_use]
    pub fn observation_kind(&self) -> &str {
        self.envelope().observation_kind.as_str()
    }

    /// Exact `payload_contract` from the canonical envelope.
    #[must_use]
    pub fn payload_contract(&self) -> &str {
        self.envelope().payload_contract.as_str()
    }

    /// Parsed `canonical_payload` from the canonical envelope.
    #[must_use]
    pub const fn canonical_payload(&self) -> &Value {
        &self.envelope().canonical_payload
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObservationParseError {
    Malformed,
    UnknownKind,
    KindContractMismatch,
    UnsupportedKind,
}

impl ObservationParseError {
    const fn terminal_code(self) -> TerminalCode {
        match self {
            Self::UnsupportedKind => TerminalCode::CapabilityUnsupported,
            Self::Malformed | Self::UnknownKind | Self::KindContractMismatch => {
                TerminalCode::InvalidRequest
            }
        }
    }

    const fn diagnostic_id(self) -> &'static str {
        match self {
            Self::Malformed => "native.observation_envelope_invalid",
            Self::UnknownKind => "native.observation_kind_unknown",
            Self::KindContractMismatch => "native.observation_kind_contract_mismatch",
            Self::UnsupportedKind => "native.observation_unsupported",
        }
    }
}

/// Narrow application boundary implemented by the existing TraceDecay Native
/// memory composition in M3.
///
/// The port owns Native authority and therefore constructs all Native terminal
/// records, provenance, receipts, and exact-scope digests after dispatch. The
/// adapter constructs only typed pre-dispatch rejections, with unknown effect
/// generation and no fallback authority, and never opens or mutates Native
/// persistence.
pub trait NativeMemoryApplicationPort: Send + Sync + 'static {
    /// Returns the current real Native descriptor and capability set.
    fn descriptor(&self) -> ProviderDescriptor;

    /// Performs the existing read-only Native compatibility handshake.
    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse;

    /// Executes mandatory Native health without changing state.
    fn health(&self, call: &ProviderCall) -> ProviderReply;

    /// Handles one admitted Native observation under Native authority.
    ///
    /// The adapter parses and classifies the provider-neutral envelope before
    /// this method is called, so the implementation branches on the
    /// [`NativeObservation`] variant rather than on a kind string. The
    /// trusted application implementation must preserve owner, provenance,
    /// trust, temporal state, idempotency, and receipts.
    ///
    /// [`NativeObservation::FactPromotion`] is verification-only and must not
    /// imply a fact write; a separate authorized operation owns any canonical
    /// Native mutation. [`NativeObservation::StagedSession`] does have a
    /// durable consequence, but only in the port's own provider-local staged
    /// store, and it must be committed before a success terminal is returned.
    /// Neither variant writes a canonical Native fact from this path, and the
    /// adapter itself still opens no persistence of any kind.
    fn observe(&self, observation: NativeObservation<'_>) -> ProviderReply;

    /// Executes existing Native recall and preserves Native ordering, scores,
    /// evidence, temporal state, and provenance in the canonical payload.
    fn recall(&self, call: &ProviderCall) -> ProviderReply;

    /// Records one declared optional Native feedback operation.
    fn feedback(&self, call: &ProviderCall) -> ProviderReply;

    /// Runs one declared optional Native maintenance operation.
    fn maintenance(&self, call: &ProviderCall) -> ProviderReply;

    /// Performs one declared optional redacted Native inspection.
    fn inspection(&self, call: &ProviderCall) -> ProviderReply;

    /// Applies one declared optional Native correction.
    fn correction(&self, call: &ProviderCall) -> ProviderReply;

    /// Deletes Native memory admitted under one declared source identity.
    fn delete_by_source(&self, call: &ProviderCall) -> ProviderReply;

    /// Exports one declared optional Native snapshot.
    fn snapshot_export(&self, call: &ProviderCall) -> ProviderReply;

    /// Restores one declared optional Native snapshot.
    fn snapshot_restore(&self, call: &ProviderCall) -> ProviderReply;

    /// Applies one declared optional deterministic Native replay.
    fn replay(&self, call: &ProviderCall) -> ProviderReply;
}

/// Provider-neutral TraceDecay Native adapter over one existing application
/// port.
pub struct NativeProvider {
    port: Arc<dyn NativeMemoryApplicationPort>,
    descriptor: ProviderDescriptor,
    state_generation: AtomicU64,
    descriptor_drifted: AtomicBool,
}

impl NativeProvider {
    /// Constructs a Native provider only when the supplied port declares the
    /// stable Native identity and the mandatory provider capabilities.
    pub fn new(port: Arc<dyn NativeMemoryApplicationPort>) -> Result<Self, NativeAdapterError> {
        let descriptor = port.descriptor();
        descriptor
            .validate()
            .map_err(NativeAdapterError::InvalidDescriptor)?;
        if descriptor.provider_id.as_str() != NATIVE_PROVIDER_ID {
            return Err(NativeAdapterError::ProviderIdMismatch {
                expected: NATIVE_PROVIDER_ID,
                declared: descriptor.provider_id.as_str().to_owned(),
            });
        }
        let state_generation = AtomicU64::new(descriptor.state_generation);
        Ok(Self {
            port,
            descriptor,
            state_generation,
            descriptor_drifted: AtomicBool::new(false),
        })
    }

    fn descriptor_snapshot(&self) -> ProviderDescriptor {
        let mut descriptor = self.descriptor.clone();
        descriptor.state_generation = self.state_generation.load(Ordering::Acquire);
        descriptor
    }

    fn refresh_descriptor(&self) -> Option<ProviderDescriptor> {
        if self.descriptor_drifted.load(Ordering::Acquire) {
            return None;
        }
        let candidate = self.port.descriptor();
        if candidate.validate().is_err() || !same_immutable_descriptor(&self.descriptor, &candidate)
        {
            self.descriptor_drifted.store(true, Ordering::Release);
            return None;
        }

        let previous_generation = self.state_generation.load(Ordering::Acquire);
        if candidate.state_generation < previous_generation {
            self.descriptor_drifted.store(true, Ordering::Release);
            return None;
        }
        self.state_generation
            .fetch_max(candidate.state_generation, Ordering::AcqRel);
        if self.descriptor_drifted.load(Ordering::Acquire) {
            None
        } else {
            Some(self.descriptor_snapshot())
        }
    }

    fn reject(
        &self,
        call: &ProviderCall,
        terminal_code: TerminalCode,
        diagnostic_id: &'static str,
    ) -> ProviderReply {
        let exact_scope_sha256 = if call.exact_scope.validate().is_ok() {
            call.exact_scope.exact_scope_sha256()
        } else {
            String::new()
        };
        let terminal = TerminalRecord::failure_before_dispatch(
            call.operation,
            self.descriptor.provider_id.clone(),
            terminal_code,
            if call.operation_id.is_empty() {
                "native.invalid-operation-id"
            } else {
                call.operation_id.as_str()
            },
            exact_scope_sha256,
            // A pre-dispatch refusal touches no state, so the generation the
            // call was addressed to is exactly the generation observed. The
            // fabric requires that evidence on every non-handshake reply;
            // omitting it turns a typed refusal into a protocol violation the
            // host would retry until exhaustion.
            Some(call.expected_state_generation),
            diagnostic_id,
        );
        ProviderReply {
            terminal,
            payload: None,
            warnings: Vec::new(),
            extensions: Vec::new(),
            state_generation: call.expected_state_generation,
        }
    }

    fn validate_payload_contract(&self, call: &ProviderCall) -> Option<ProviderReply> {
        if call.payload.contract_id.as_str() != canonical_payload_contract_id(call.operation) {
            return Some(self.reject(
                call,
                TerminalCode::InvalidRequest,
                if call.operation == ProviderOperation::Observe {
                    "native.observation_contract_invalid"
                } else {
                    "native.payload_contract_invalid"
                },
            ));
        }
        None
    }

    fn parse_observation<'call>(
        call: &'call ProviderCall,
    ) -> Result<NativeObservation<'call>, ObservationParseError> {
        let envelope = serde_json::from_slice::<Value>(&call.payload.bytes)
            .map_err(|_| ObservationParseError::Malformed)?;
        let object = envelope
            .as_object()
            .ok_or(ObservationParseError::Malformed)?;
        let observation_kind = object
            .get("observation_kind")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or(ObservationParseError::Malformed)?
            .to_owned();
        let payload_contract = object
            .get("payload_contract")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or(ObservationParseError::Malformed)?
            .to_owned();
        let canonical_payload = object
            .get("canonical_payload")
            .filter(|value| !value.is_null())
            .cloned()
            .ok_or(ObservationParseError::Malformed)?;

        let expected_payload_contract = match observation_kind.as_str() {
            NATIVE_STAGED_SESSION_OBSERVATION_KIND => NATIVE_STAGED_SESSION_PAYLOAD_CONTRACT_ID,
            "tool.execution_settled.v1" => "tracedecay.memory.observation.tool-execution.v1",
            "source.edit_settled.v1" => "tracedecay.memory.observation.source-edit.v1",
            "test.execution_settled.v1" => "tracedecay.memory.observation.test-execution.v1",
            "diagnostic.observed.v1" => "tracedecay.memory.observation.diagnostic.v1",
            "git.evidence_observed.v1" => "tracedecay.memory.observation.git-evidence.v1",
            NATIVE_FACT_PROMOTION_OBSERVATION_KIND => NATIVE_FACT_PROMOTION_PAYLOAD_CONTRACT_ID,
            "feedback.outcome_settled.v1" => "tracedecay.memory.observation.feedback-outcome.v1",
            "automation.outcome_settled.v1" => {
                "tracedecay.memory.observation.automation-outcome.v1"
            }
            _ => return Err(ObservationParseError::UnknownKind),
        };
        if payload_contract != expected_payload_contract {
            return Err(ObservationParseError::KindContractMismatch);
        }
        // Classification is the authorization boundary: exactly two kinds are
        // accepted, and each is handed to the port as its own variant. Every
        // other contract-known kind is refused here, before the port is
        // reached, because accepting it would commit Native to a projection,
        // retention rule, and containment story it does not have.
        let staged = match observation_kind.as_str() {
            NATIVE_FACT_PROMOTION_OBSERVATION_KIND => false,
            NATIVE_STAGED_SESSION_OBSERVATION_KIND => true,
            _ => return Err(ObservationParseError::UnsupportedKind),
        };
        let envelope = NativeObservationEnvelope {
            call,
            observation_kind,
            payload_contract,
            canonical_payload,
        };

        Ok(if staged {
            NativeObservation::StagedSession(envelope)
        } else {
            NativeObservation::FactPromotion(envelope)
        })
    }

    fn reject_handshake(
        &self,
        request: &HandshakeRequest,
        terminal_code: TerminalCode,
        diagnostic_id: &'static str,
    ) -> HandshakeResponse {
        let exact_scope_sha256 = if request.exact_scope.validate().is_ok() {
            request.exact_scope.exact_scope_sha256()
        } else {
            String::new()
        };
        let terminal = TerminalRecord::failure_before_dispatch(
            ProviderOperation::Handshake,
            self.descriptor.provider_id.clone(),
            terminal_code,
            &request.request_id,
            exact_scope_sha256,
            None,
            diagnostic_id,
        );
        HandshakeResponse {
            terminal,
            descriptor: None,
            provider_instance_id: None,
            state_namespace: None,
            accepted_scope: None,
            effective_limits: None,
            ready_receipt_sha256: None,
            warnings: Vec::new(),
        }
    }
}

impl MemoryProvider for NativeProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        match self.refresh_descriptor() {
            Some(descriptor) => descriptor,
            None => self.descriptor_snapshot(),
        }
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        if request.validate().is_err() {
            return self.reject_handshake(
                request,
                TerminalCode::InvalidRequest,
                "native.handshake_request_invalid",
            );
        }
        if request.provider_id.as_str() != self.descriptor.provider_id.as_str() {
            return self.reject_handshake(
                request,
                TerminalCode::InvalidRequest,
                "native.provider_id_mismatch",
            );
        }
        if self.refresh_descriptor().is_none() {
            return self.reject_handshake(
                request,
                TerminalCode::ContractViolation,
                "native.descriptor_drift",
            );
        }
        self.port.handshake(request)
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        if call.validate().is_err() {
            return self.reject(
                call,
                TerminalCode::InvalidRequest,
                "native.provider_call_invalid",
            );
        }
        if call.provider_id.as_str() != self.descriptor.provider_id.as_str() {
            return self.reject(
                call,
                TerminalCode::InvalidRequest,
                "native.provider_id_mismatch",
            );
        }
        if call.operation == ProviderOperation::Handshake {
            return self.reject(
                call,
                TerminalCode::InvalidRequest,
                "native.handshake_requires_handshake_port",
            );
        }
        if !self.descriptor.supports(call.operation.capability_id()) {
            return self.reject(
                call,
                TerminalCode::CapabilityUnsupported,
                "native.capability_unsupported",
            );
        }
        if let Some(rejection) = self.validate_payload_contract(call) {
            return rejection;
        }
        let observation = if call.operation == ProviderOperation::Observe {
            match Self::parse_observation(call) {
                Ok(observation) => Some(observation),
                Err(error) => {
                    return self.reject(call, error.terminal_code(), error.diagnostic_id());
                }
            }
        } else {
            None
        };
        match self.refresh_descriptor() {
            Some(_) => {}
            None => {
                return self.reject(
                    call,
                    TerminalCode::ContractViolation,
                    "native.descriptor_drift",
                );
            }
        }
        match call.operation {
            ProviderOperation::Health => self.port.health(call),
            ProviderOperation::Observe => match observation {
                Some(observation) => self.port.observe(observation),
                None => self.reject(
                    call,
                    TerminalCode::ContractViolation,
                    "native.observation_dispatch_missing",
                ),
            },
            ProviderOperation::Recall => self.port.recall(call),
            ProviderOperation::Feedback => self.port.feedback(call),
            ProviderOperation::Maintenance => self.port.maintenance(call),
            ProviderOperation::Inspection => self.port.inspection(call),
            ProviderOperation::Correction => self.port.correction(call),
            ProviderOperation::DeleteBySource => self.port.delete_by_source(call),
            ProviderOperation::SnapshotExport => self.port.snapshot_export(call),
            ProviderOperation::SnapshotRestore => self.port.snapshot_restore(call),
            ProviderOperation::Replay => self.port.replay(call),
            ProviderOperation::Handshake => self.reject(
                call,
                TerminalCode::InvalidRequest,
                "native.operation_dispatch_unreachable",
            ),
        }
    }
}

fn same_immutable_descriptor(left: &ProviderDescriptor, right: &ProviderDescriptor) -> bool {
    left.provider_id == right.provider_id
        && left.implementation_identity_sha256 == right.implementation_identity_sha256
        && left.state_schema_version == right.state_schema_version
        && left.protocol_major == right.protocol_major
        && left.protocol_minor == right.protocol_minor
        && left.capabilities == right.capabilities
        && left.limits == right.limits
}

const fn canonical_payload_contract_id(operation: ProviderOperation) -> &'static str {
    match operation {
        ProviderOperation::Handshake => HANDSHAKE_CONTRACT_ID,
        ProviderOperation::Health => HEALTH_CONTRACT_ID,
        ProviderOperation::Observe => OBSERVATION_CONTRACT_ID,
        ProviderOperation::Recall => RECALL_CONTRACT_ID,
        ProviderOperation::Feedback => FEEDBACK_CONTRACT_ID,
        ProviderOperation::Maintenance => MAINTENANCE_CONTRACT_ID,
        ProviderOperation::Inspection => INSPECTION_CONTRACT_ID,
        ProviderOperation::Correction => CORRECTION_CONTRACT_ID,
        ProviderOperation::DeleteBySource => DELETE_BY_SOURCE_CONTRACT_ID,
        ProviderOperation::SnapshotExport => SNAPSHOT_EXPORT_CONTRACT_ID,
        ProviderOperation::SnapshotRestore => SNAPSHOT_RESTORE_CONTRACT_ID,
        ProviderOperation::Replay => REPLAY_CONTRACT_ID,
    }
}
