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
//! projects the port's descriptor to the capabilities it can map losslessly,
//! preserves canonical call bytes and exact scope unchanged, and rejects
//! unsupported operations locally before contacting Native operation authority.
//!
//! Observation classification happens here — an admitted envelope is parsed
//! into one typed [`NativeObservation`] variant — but the durable consequence
//! of an accepted observation belongs entirely to the application port behind
//! this boundary. Staging a session message opens no store in this crate.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::Value;
use tracedecay_memory_provider_api::contract::TerminalCode;
use tracedecay_memory_provider_api::{
    ApiError, HandshakeRequest, HandshakeResponse, MemoryProvider, OperationControl,
    OwnedVersionedId, ProviderCall, ProviderDescriptor, ProviderOperation, ProviderReply,
    TerminalRecord,
};

/// Stable logical provider identity for TraceDecay Native memory.
pub const NATIVE_PROVIDER_ID: &str = "tracedecay.native";

/// Capability IDs the generic Native adapter can currently map without
/// fabricating a provider-local authority.
///
/// The application port may expose additional typed Native routes, but those
/// routes are not generic provider capabilities. In particular, the adapter
/// does not advertise temporal recall, lifecycle controls, snapshots, replay,
/// or canonical fact writes until each has an exact provider-local mapping.
pub const NATIVE_PROVIDER_CAPABILITY_IDS: &[&str] = &[
    "provider.health.v1",
    "observation.accept.v1",
    "recall.query.v1",
];

/// Recall candidate scope bindings the host authorizes Native to attest, in
/// the wire vocabulary of `tracedecay.memory.provider.recall.v1`
/// `candidate_scope_binding.bindings`.
///
/// Native facts attest their project/profile owner. Staged observations are
/// stored under all seven exact origin fields, but may be recalled in another
/// agent session on the same profile, project, repository, worktree and branch
/// under `checkout_observations`. Candidate session and resolved-scope fields
/// are empty; the immutable origin fields remain in provenance. The fully exact
/// binding remains authorized and still compares all seven fields.
///
/// The registry records this declaration at registration and passes it to
/// admission with the admitted call; a provider reply can never widen it.
pub const NATIVE_RECALL_SCOPE_BINDINGS: &[&str] = &[
    "exact_coding_scope",
    "checkout_observations",
    "project_facts",
    "profile_facts",
];

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

const OBSERVATION_ENVELOPE_FIELDS: [&str; 3] =
    ["canonical_payload", "observation_kind", "payload_contract"];

const HANDSHAKE_CONTRACT_ID: &str = "tracedecay.memory.provider.handshake.v1";
const HEALTH_CONTRACT_ID: &str = "tracedecay.memory.provider.health.v1";
const RECALL_CONTRACT_ID: &str = "tracedecay.memory.provider.recall.v1";
const RECALL_RESULT_CONTRACT_ID: &str = "tracedecay.memory.recall.query.outcome.v1";
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

    /// Records one typed Native feedback operation.
    ///
    /// This port method is intentionally broader than the current generic
    /// adapter projection. The adapter does not invoke it until a lossless
    /// provider-local mapping is declared.
    fn feedback(&self, call: &ProviderCall) -> ProviderReply;

    /// Runs one typed Native maintenance operation.
    fn maintenance(&self, call: &ProviderCall) -> ProviderReply;

    /// Performs one typed redacted Native inspection.
    fn inspection(&self, call: &ProviderCall) -> ProviderReply;

    /// Applies one typed Native correction.
    fn correction(&self, call: &ProviderCall) -> ProviderReply;

    /// Deletes Native memory admitted under one typed source identity.
    fn delete_by_source(&self, call: &ProviderCall) -> ProviderReply;

    /// Exports one typed Native snapshot.
    fn snapshot_export(&self, call: &ProviderCall) -> ProviderReply;

    /// Restores one typed Native snapshot.
    fn snapshot_restore(&self, call: &ProviderCall) -> ProviderReply;

    /// Applies one typed deterministic Native replay.
    fn replay(&self, call: &ProviderCall) -> ProviderReply;
}

/// Provider-neutral TraceDecay Native adapter over one existing application
/// port.
pub struct NativeProvider {
    port: Arc<dyn NativeMemoryApplicationPort>,
    descriptor: ProviderDescriptor,
    state_generation: AtomicU64,
    descriptor_drifted: AtomicBool,
    /// Serializes live descriptor refresh, identity validation, and the
    /// application-port contact that follows it. Without one gate, a caller
    /// can validate generation N and contact the port after another caller
    /// has projected generation N+1.
    dispatch_lock: Mutex<()>,
}

impl NativeProvider {
    /// Constructs a Native provider only when the supplied port declares the
    /// stable Native identity and the mandatory provider capabilities.
    pub fn new(port: Arc<dyn NativeMemoryApplicationPort>) -> Result<Self, NativeAdapterError> {
        let port_descriptor = port.descriptor();
        port_descriptor
            .validate()
            .map_err(NativeAdapterError::InvalidDescriptor)?;
        if port_descriptor.provider_id.as_str() != NATIVE_PROVIDER_ID {
            return Err(NativeAdapterError::ProviderIdMismatch {
                expected: NATIVE_PROVIDER_ID,
                declared: port_descriptor.provider_id.as_str().to_owned(),
            });
        }
        let descriptor = project_descriptor(port_descriptor);
        descriptor
            .validate()
            .map_err(NativeAdapterError::InvalidDescriptor)?;
        let state_generation = AtomicU64::new(descriptor.state_generation);
        Ok(Self {
            port,
            descriptor,
            state_generation,
            descriptor_drifted: AtomicBool::new(false),
            dispatch_lock: Mutex::new(()),
        })
    }

    fn descriptor_snapshot(&self) -> ProviderDescriptor {
        let mut descriptor = self.descriptor.clone();
        descriptor.state_generation = self.state_generation.load(Ordering::Acquire);
        descriptor
    }

    fn refresh_descriptor(&self) -> Option<ProviderDescriptor> {
        self.refresh_descriptor_with_control(None).ok().flatten()
    }

    fn refresh_descriptor_with_control(
        &self,
        control: Option<&OperationControl>,
    ) -> Result<Option<ProviderDescriptor>, TerminalCode> {
        if self.descriptor_drifted.load(Ordering::Acquire) {
            return Ok(None);
        }
        if let Some(control) = control {
            control.snapshot()?;
        }
        let port_candidate = self.port.descriptor();
        // The descriptor call is the first contact with the application port.
        // Sample live control again at this boundary so a cancellation racing
        // that contact cannot proceed to an operation call.
        if let Some(control) = control {
            control.snapshot()?;
        }
        if port_candidate.validate().is_err() {
            self.descriptor_drifted.store(true, Ordering::Release);
            return Ok(None);
        }
        let candidate = project_descriptor(port_candidate);
        if !same_immutable_descriptor(&self.descriptor, &candidate) {
            self.descriptor_drifted.store(true, Ordering::Release);
            return Ok(None);
        }

        let previous_generation = self.state_generation.load(Ordering::Acquire);
        if candidate.state_generation < previous_generation {
            self.descriptor_drifted.store(true, Ordering::Release);
            return Ok(None);
        }
        self.state_generation
            .fetch_max(candidate.state_generation, Ordering::AcqRel);
        if self.descriptor_drifted.load(Ordering::Acquire) {
            Ok(None)
        } else {
            Ok(Some(self.descriptor_snapshot()))
        }
    }

    fn supports_required_capabilities(
        descriptor: &ProviderDescriptor,
        required: &BTreeSet<OwnedVersionedId>,
    ) -> bool {
        required
            .iter()
            .all(|capability| descriptor.supports(capability.as_str()))
    }

    fn valid_terminal_identity(
        &self,
        operation: ProviderOperation,
        operation_id: &str,
        exact_scope_sha256: &str,
        terminal: &TerminalRecord,
    ) -> bool {
        terminal.operation() == operation
            && terminal.provider_id() == &self.descriptor.provider_id
            && terminal.operation_id() == operation_id
            && terminal.exact_scope_sha256() == exact_scope_sha256
            && terminal.fallback().eligibility()
                == tracedecay_memory_provider_api::contract::FallbackEligibility::Forbidden
            && TerminalRecord::new(
                terminal.operation(),
                terminal.provider_id().clone(),
                terminal.terminal_code(),
                terminal.committed_effect().clone(),
                terminal.fallback().clone(),
                terminal.operation_id().to_owned(),
                terminal.exact_scope_sha256().to_owned(),
                terminal.diagnostic_id().map(str::to_owned),
            )
            .is_ok()
    }

    fn validate_handshake_response(
        &self,
        request: &HandshakeRequest,
        response: &HandshakeResponse,
    ) -> Result<Option<ProviderDescriptor>, ()> {
        let exact_scope_sha256 = request.exact_scope.exact_scope_sha256();
        if response.warnings.len() > 32
            || !self.valid_terminal_identity(
                ProviderOperation::Handshake,
                &request.request_id,
                &exact_scope_sha256,
                &response.terminal,
            )
            || response.terminal.committed_effect().state()
                != tracedecay_memory_provider_api::contract::CommittedEffectState::None
            || response
                .terminal
                .committed_effect()
                .provider_receipt_sha256()
                .is_some()
        {
            return Err(());
        }

        if response.terminal.terminal_code() != TerminalCode::Success {
            if matches!(
                response.terminal.terminal_code(),
                TerminalCode::SuccessZeroResults | TerminalCode::Partial
            ) {
                return Err(());
            }
            if response.descriptor.is_some()
                || response.provider_instance_id.is_some()
                || response.state_namespace.is_some()
                || response.accepted_scope.is_some()
                || response.effective_limits.is_some()
                || response.ready_receipt_sha256.is_some()
            {
                return Err(());
            }
            return Ok(None);
        }

        let descriptor = response.descriptor.as_ref().ok_or(())?;
        descriptor.validate().map_err(|_| ())?;
        if descriptor.provider_id.as_str() != NATIVE_PROVIDER_ID {
            return Err(());
        }
        let projected = project_descriptor(descriptor.clone());
        projected.validate().map_err(|_| ())?;
        if !same_immutable_descriptor(&self.descriptor, &projected)
            || projected.state_generation < self.state_generation.load(Ordering::Acquire)
            || response.accepted_scope.as_ref() != Some(&request.exact_scope)
            || !response
                .provider_instance_id
                .as_deref()
                .is_some_and(|value| valid_canonical_text(value, None))
            || !response
                .state_namespace
                .as_deref()
                .is_some_and(|value| valid_canonical_text(value, Some(128)))
            || response.effective_limits
                != Some(request.host_limits.minimum(self.descriptor.limits))
            || response
                .effective_limits
                .is_none_or(|limits| limits.validate().is_err())
            || !response
                .ready_receipt_sha256
                .as_deref()
                .is_some_and(is_lowercase_sha256)
        {
            return Err(());
        }

        let effect = response.terminal.committed_effect();
        if effect.state_generation_before() != Some(projected.state_generation)
            || effect.state_generation_after() != Some(projected.state_generation)
        {
            return Err(());
        }
        Ok(Some(projected))
    }

    fn validate_application_reply(&self, call: &ProviderCall, reply: &ProviderReply) -> bool {
        let exact_scope_sha256 = call.exact_scope.exact_scope_sha256();
        if !self.valid_terminal_identity(
            call.operation,
            &call.operation_id,
            &exact_scope_sha256,
            &reply.terminal,
        ) || reply
            .validate(self.descriptor.limits.response_bytes)
            .is_err()
        {
            return false;
        }

        if !valid_effect_for_operation(call, reply)
            || !valid_effect_generations(call, reply)
            || reply
                .terminal
                .validate_duplicate_binding_for_call(call)
                .is_err()
        {
            return false;
        }

        match reply.terminal.terminal_code() {
            TerminalCode::Success | TerminalCode::SuccessZeroResults | TerminalCode::Partial => {
                reply.payload.as_ref().is_some_and(|payload| {
                    payload.contract_id.as_str() == canonical_result_contract_id(call.operation)
                        && serde_json::from_slice::<Value>(&payload.bytes)
                            .is_ok_and(|value| value.is_object())
                })
            }
            _ => reply.payload.is_none(),
        }
    }

    fn validated_application_reply(
        &self,
        call: &ProviderCall,
        reply: ProviderReply,
    ) -> ProviderReply {
        if self.validate_application_reply(call, &reply) {
            reply
        } else {
            self.reject(
                call,
                TerminalCode::ContractViolation,
                "native.application_reply_contract_violation",
            )
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
        let envelope = parse_canonical_observation(&call.payload.bytes)?;
        let object = envelope
            .as_object()
            .ok_or(ObservationParseError::Malformed)?;
        if object.len() != 3
            || object
                .keys()
                .any(|key| !OBSERVATION_ENVELOPE_FIELDS.contains(&key.as_str()))
        {
            return Err(ObservationParseError::Malformed);
        }
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
            .filter(|value| value.as_object().is_some_and(|payload| !payload.is_empty()))
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
        // Only the session message has a provider-local staged projection.
        // Fact promotion remains verification-only. Structured common
        // observations need a distinct Native authority and stay unsupported
        // until that mapping is implemented.
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
        // A port callback may ask for the current descriptor while the
        // adapter is contacting that same port. Do not wait recursively in
        // that case: the in-flight operation's projected snapshot is the
        // only descriptor that is safe to expose until the gate is released.
        let dispatch = match self.dispatch_lock.try_lock() {
            Ok(guard) => Some(guard),
            Err(std::sync::TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
            Err(std::sync::TryLockError::WouldBlock) => None,
        };
        match dispatch {
            Some(_dispatch) => match self.refresh_descriptor() {
                Some(descriptor) => descriptor,
                None => self.descriptor_snapshot(),
            },
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
        if !Self::supports_required_capabilities(&self.descriptor, &request.required_capabilities) {
            return self.reject_handshake(
                request,
                TerminalCode::CapabilityUnsupported,
                "native.required_capability_missing",
            );
        }
        if let Err(code) = request.control.snapshot() {
            return self.reject_handshake(
                request,
                code,
                "native.handshake_request_control_terminal",
            );
        }
        let _dispatch = self
            .dispatch_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match self.refresh_descriptor_with_control(Some(&request.control)) {
            Ok(Some(_)) => {}
            Ok(None) => {
                return self.reject_handshake(
                    request,
                    TerminalCode::ContractViolation,
                    "native.descriptor_drift",
                );
            }
            Err(code) => {
                return self.reject_handshake(
                    request,
                    code,
                    "native.handshake_request_control_terminal",
                );
            }
        }
        if let Err(code) = request.control.snapshot() {
            return self.reject_handshake(
                request,
                code,
                "native.handshake_request_control_terminal",
            );
        }
        let mut response = self.port.handshake(request);
        let projected_descriptor = match self.validate_handshake_response(request, &response) {
            Ok(projected_descriptor) => projected_descriptor,
            Err(()) => {
                return self.reject_handshake(
                    request,
                    TerminalCode::ContractViolation,
                    "native.handshake_response_contract_violation",
                );
            }
        };
        if let Some(projected_descriptor) = projected_descriptor {
            self.state_generation
                .fetch_max(projected_descriptor.state_generation, Ordering::AcqRel);
            response.descriptor = Some(projected_descriptor);
        }
        response
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
        if !Self::supports_required_capabilities(&self.descriptor, &call.required_capabilities) {
            return self.reject(
                call,
                TerminalCode::CapabilityUnsupported,
                "native.required_capability_missing",
            );
        }
        if let Err(code) = call.control.snapshot() {
            return self.reject(call, code, "native.request_control_terminal");
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
        let _dispatch = self
            .dispatch_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match self.refresh_descriptor_with_control(Some(&call.control)) {
            Ok(Some(_)) => {}
            Ok(None) => {
                return self.reject(
                    call,
                    TerminalCode::ContractViolation,
                    "native.descriptor_drift",
                );
            }
            Err(code) => return self.reject(call, code, "native.request_control_terminal"),
        }
        if let Err(code) = call.control.snapshot() {
            return self.reject(call, code, "native.request_control_terminal");
        }
        let current_generation = self.state_generation.load(Ordering::Acquire);
        if call.expected_state_generation != current_generation {
            return self.reject(
                call,
                TerminalCode::StaleIdentity,
                "native.state_generation_mismatch",
            );
        }
        match call.operation {
            ProviderOperation::Health => {
                self.validated_application_reply(call, self.port.health(call))
            }
            ProviderOperation::Observe => match observation {
                Some(observation) => {
                    self.validated_application_reply(call, self.port.observe(observation))
                }
                None => self.reject(
                    call,
                    TerminalCode::ContractViolation,
                    "native.observation_dispatch_missing",
                ),
            },
            ProviderOperation::Recall => {
                self.validated_application_reply(call, self.port.recall(call))
            }
            ProviderOperation::Feedback => {
                self.validated_application_reply(call, self.port.feedback(call))
            }
            ProviderOperation::Maintenance => {
                self.validated_application_reply(call, self.port.maintenance(call))
            }
            ProviderOperation::Inspection => {
                self.validated_application_reply(call, self.port.inspection(call))
            }
            ProviderOperation::Correction => {
                self.validated_application_reply(call, self.port.correction(call))
            }
            ProviderOperation::DeleteBySource => {
                self.validated_application_reply(call, self.port.delete_by_source(call))
            }
            ProviderOperation::SnapshotExport => {
                self.validated_application_reply(call, self.port.snapshot_export(call))
            }
            ProviderOperation::SnapshotRestore => {
                self.validated_application_reply(call, self.port.snapshot_restore(call))
            }
            ProviderOperation::Replay => {
                self.validated_application_reply(call, self.port.replay(call))
            }
            ProviderOperation::Handshake => self.reject(
                call,
                TerminalCode::InvalidRequest,
                "native.operation_dispatch_unreachable",
            ),
        }
    }
}

fn valid_effect_for_operation(call: &ProviderCall, reply: &ProviderReply) -> bool {
    let state = reply.terminal.committed_effect().state();
    if !call.operation.mutates_provider_state() {
        return state == tracedecay_memory_provider_api::contract::CommittedEffectState::None;
    }

    match reply.terminal.terminal_code() {
        TerminalCode::Success => matches!(
            state,
            tracedecay_memory_provider_api::contract::CommittedEffectState::None
                | tracedecay_memory_provider_api::contract::CommittedEffectState::Committed
                | tracedecay_memory_provider_api::contract::CommittedEffectState::Duplicate
        ),
        // A mutating operation may complete with no effect, a committed
        // effect, or a duplicate acknowledgement under `success`, but these
        // read/query terminals cannot stand in for that operation-specific
        // settlement.
        TerminalCode::SuccessZeroResults | TerminalCode::Partial => false,
        TerminalCode::PartialEffect => {
            state == tracedecay_memory_provider_api::contract::CommittedEffectState::Partial
        }
        TerminalCode::EffectUnknown => {
            state == tracedecay_memory_provider_api::contract::CommittedEffectState::Unknown
        }
        TerminalCode::DeadlineExceeded | TerminalCode::Cancelled => matches!(
            state,
            tracedecay_memory_provider_api::contract::CommittedEffectState::None
                | tracedecay_memory_provider_api::contract::CommittedEffectState::Partial
                | tracedecay_memory_provider_api::contract::CommittedEffectState::Unknown
        ),
        TerminalCode::ProviderUnavailable => matches!(
            state,
            tracedecay_memory_provider_api::contract::CommittedEffectState::None
                | tracedecay_memory_provider_api::contract::CommittedEffectState::Unknown
        ),
        TerminalCode::ContractViolation | TerminalCode::InternalFailure => matches!(
            state,
            tracedecay_memory_provider_api::contract::CommittedEffectState::None
                | tracedecay_memory_provider_api::contract::CommittedEffectState::Partial
                | tracedecay_memory_provider_api::contract::CommittedEffectState::Unknown
        ),
        _ => state == tracedecay_memory_provider_api::contract::CommittedEffectState::None,
    }
}

fn valid_effect_generations(call: &ProviderCall, reply: &ProviderReply) -> bool {
    let effect = reply.terminal.committed_effect();
    if effect.state() == tracedecay_memory_provider_api::contract::CommittedEffectState::Unknown {
        // Unknown evidence intentionally carries no generation claim. Its
        // receipt and reconciliation action are the witness retained for
        // later inspection, so requiring `Some` here would erase the only
        // truthful effect state the provider can report after uncertainty.
        return true;
    }
    effect.state_generation_before() == Some(call.expected_state_generation)
        && effect.state_generation_after() == Some(reply.state_generation)
}

fn parse_canonical_observation(bytes: &[u8]) -> Result<Value, ObservationParseError> {
    let envelope =
        serde_json::from_slice::<Value>(bytes).map_err(|_| ObservationParseError::Malformed)?;
    if json_has_duplicate_object_keys(bytes).map_err(|_| ObservationParseError::Malformed)? {
        return Err(ObservationParseError::Malformed);
    }
    let canonical = serde_json::to_vec(&envelope).map_err(|_| ObservationParseError::Malformed)?;
    if canonical.as_slice() != bytes || contains_floating_number(&envelope) {
        return Err(ObservationParseError::Malformed);
    }
    Ok(envelope)
}

fn contains_floating_number(value: &Value) -> bool {
    match value {
        Value::Number(number) => number.as_i64().is_none() && number.as_u64().is_none(),
        Value::Array(values) => values.iter().any(contains_floating_number),
        Value::Object(values) => values.values().any(contains_floating_number),
        Value::Null | Value::Bool(_) | Value::String(_) => false,
    }
}

fn json_has_duplicate_object_keys(bytes: &[u8]) -> Result<bool, ()> {
    let mut scanner = JsonKeyScanner {
        bytes,
        index: 0,
        duplicated: false,
    };
    scanner.parse_value()?;
    scanner.skip_whitespace();
    if scanner.index != bytes.len() {
        return Err(());
    }
    Ok(scanner.duplicated)
}

struct JsonKeyScanner<'bytes> {
    bytes: &'bytes [u8],
    index: usize,
    duplicated: bool,
}

impl JsonKeyScanner<'_> {
    fn parse_value(&mut self) -> Result<(), ()> {
        self.skip_whitespace();
        match self.bytes.get(self.index).copied() {
            Some(b'{') => self.parse_object(),
            Some(b'[') => self.parse_array(),
            Some(b'"') => self.parse_string().map(|_| ()),
            Some(b'-' | b'0'..=b'9' | b't' | b'f' | b'n') => self.parse_atom(),
            _ => Err(()),
        }
    }

    fn parse_object(&mut self) -> Result<(), ()> {
        self.consume(b'{')?;
        self.skip_whitespace();
        if self.consume_if(b'}') {
            return Ok(());
        }

        let mut keys = BTreeSet::new();
        loop {
            self.skip_whitespace();
            let key_literal = self.parse_string()?;
            let key = serde_json::from_slice::<String>(key_literal).map_err(|_| ())?;
            if !keys.insert(key) {
                self.duplicated = true;
            }
            self.skip_whitespace();
            self.consume(b':')?;
            self.parse_value()?;
            self.skip_whitespace();
            if self.consume_if(b'}') {
                return Ok(());
            }
            self.consume(b',')?;
        }
    }

    fn parse_array(&mut self) -> Result<(), ()> {
        self.consume(b'[')?;
        self.skip_whitespace();
        if self.consume_if(b']') {
            return Ok(());
        }
        loop {
            self.parse_value()?;
            self.skip_whitespace();
            if self.consume_if(b']') {
                return Ok(());
            }
            self.consume(b',')?;
        }
    }

    fn parse_atom(&mut self) -> Result<(), ()> {
        let start = self.index;
        while let Some(byte) = self.bytes.get(self.index).copied() {
            if matches!(
                byte,
                b' ' | b'\t' | b'\n' | b'\r' | b',' | b']' | b'}' | b':'
            ) {
                break;
            }
            self.index = self.index.saturating_add(1);
        }
        (self.index > start).then_some(()).ok_or(())
    }

    fn parse_string(&mut self) -> Result<&[u8], ()> {
        let start = self.index;
        self.consume(b'"')?;
        loop {
            match self.bytes.get(self.index).copied() {
                Some(b'"') => {
                    self.index = self.index.saturating_add(1);
                    return Ok(&self.bytes[start..self.index]);
                }
                Some(b'\\') => {
                    self.index = self.index.saturating_add(2);
                    if self.index > self.bytes.len() {
                        return Err(());
                    }
                }
                Some(byte) if byte < 0x20 => return Err(()),
                Some(_) => self.index = self.index.saturating_add(1),
                None => return Err(()),
            }
        }
    }

    fn skip_whitespace(&mut self) {
        while matches!(
            self.bytes.get(self.index),
            Some(b' ' | b'\t' | b'\n' | b'\r')
        ) {
            self.index = self.index.saturating_add(1);
        }
    }

    fn consume(&mut self, expected: u8) -> Result<(), ()> {
        if self.consume_if(expected) {
            Ok(())
        } else {
            Err(())
        }
    }

    fn consume_if(&mut self, expected: u8) -> bool {
        if self.bytes.get(self.index) == Some(&expected) {
            self.index = self.index.saturating_add(1);
            true
        } else {
            false
        }
    }
}

fn project_descriptor(mut descriptor: ProviderDescriptor) -> ProviderDescriptor {
    descriptor.capabilities.retain(|capability| {
        NATIVE_PROVIDER_CAPABILITY_IDS
            .iter()
            .any(|supported| *supported == capability.as_str())
    });
    descriptor
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

fn valid_canonical_text(value: &str, maximum: Option<usize>) -> bool {
    !value.is_empty()
        && value.trim() == value
        && !value.chars().any(char::is_control)
        && maximum.is_none_or(|maximum| value.len() <= maximum)
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
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

const fn canonical_result_contract_id(operation: ProviderOperation) -> &'static str {
    match operation {
        ProviderOperation::Handshake => HANDSHAKE_CONTRACT_ID,
        ProviderOperation::Health => HEALTH_CONTRACT_ID,
        ProviderOperation::Observe => OBSERVATION_CONTRACT_ID,
        ProviderOperation::Recall => RECALL_RESULT_CONTRACT_ID,
        ProviderOperation::Feedback => "tracedecay.memory.feedback.record.outcome.v1",
        ProviderOperation::Maintenance => "tracedecay.memory.maintenance.run.outcome.v1",
        ProviderOperation::Inspection => "tracedecay.memory.inspection.read.outcome.v1",
        ProviderOperation::Correction => "tracedecay.memory.correction.apply.outcome.v1",
        ProviderOperation::DeleteBySource => "tracedecay.memory.deletion.by_source.outcome.v1",
        ProviderOperation::SnapshotExport => "tracedecay.memory.snapshot.export.outcome.v1",
        ProviderOperation::SnapshotRestore => "tracedecay.memory.snapshot.restore.outcome.v1",
        ProviderOperation::Replay => "tracedecay.memory.replay.apply.outcome.v1",
    }
}
