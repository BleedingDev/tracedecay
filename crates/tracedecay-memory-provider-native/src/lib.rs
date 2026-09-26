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
//! It owns no database, index, scoring, curation, privacy, graph, staging, or
//! persistence state. The composition mount supplies the owner-bound Native
//! application port, which answers from upstream TraceDecay authorities only:
//! canonical facts through the owner-bound memory application and session
//! history through the `tracedecay_message_search` kernel. The adapter
//! validates the stable Native provider identity, projects the port's
//! descriptor to the capabilities it can map losslessly, preserves canonical
//! call bytes and exact scope unchanged, and rejects unsupported operations
//! locally before contacting Native operation authority.
//!
//! Native observes nothing. Upstream has exactly one capture authority (host
//! admission, projection, and the raw LCM message store), so no observation
//! kind is a Native capability.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::Value;
use tracedecay_memory_provider_api::contract::{CommittedEffectState, TerminalCode};
use tracedecay_memory_provider_api::{
    ApiError, HandshakeRequest, HandshakeResponse, MemoryProvider, OperationControl,
    OwnedVersionedId, ProviderCall, ProviderDescriptor, ProviderOperation, ProviderReply,
    TerminalRecord,
};

/// Stable logical provider identity for TraceDecay Native memory.
pub const NATIVE_PROVIDER_ID: &str = "tracedecay.native";

/// Capability IDs the Native adapter maps losslessly onto upstream
/// authorities: health, and recall over canonical facts and session history.
///
/// Native declares no observation, feedback, maintenance, inspection,
/// correction, deletion, snapshot, or replay capability. Upstream owns one
/// capture path and one curation path; a provider copy of either would be a
/// shadow authority.
pub const NATIVE_PROVIDER_CAPABILITY_IDS: &[&str] = &["provider.health.v1", "recall.query.v1"];

/// Recall candidate scope bindings the host authorizes Native to attest, in
/// the wire vocabulary of `tracedecay.memory.provider.recall.v1`
/// `candidate_scope_binding.bindings`.
///
/// Native produces exactly two kinds of candidate. Upstream facts are owned by
/// the project (`project_facts`) or by the profile (`profile_facts`). Upstream
/// `tracedecay_message_search` hits come from every session in the
/// authorized project root, so they are project-wide as well and attest the
/// same project binding with the checkout, session, and resolved-scope
/// fields left empty. No candidate attests a single checkout.
///
/// The registry records this declaration at registration and passes it to
/// admission with the admitted call; a provider reply can never widen it.
pub const NATIVE_RECALL_SCOPE_BINDINGS: &[&str] = &["project_facts", "profile_facts"];

const HEALTH_CONTRACT_ID: &str = "tracedecay.memory.provider.health.v1";
const RECALL_CONTRACT_ID: &str = "tracedecay.memory.provider.recall.v1";
/// Canonical payload contract identity returned by a successful recall operation.
pub const RECALL_RESULT_CONTRACT_ID: &str = "tracedecay.memory.recall.query.outcome.v1";

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

/// Narrow application boundary implemented by the TraceDecay Native memory
/// composition.
///
/// The port answers only from upstream authorities and constructs all Native
/// terminal records and exact-scope digests after dispatch. The adapter
/// constructs only typed pre-dispatch rejections, with no fallback authority,
/// and never opens or mutates Native persistence.
pub trait NativeMemoryApplicationPort: Send + Sync + 'static {
    /// Returns the current real Native descriptor and capability set.
    fn descriptor(&self) -> ProviderDescriptor;

    /// Performs the read-only Native compatibility handshake.
    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse;

    /// Executes mandatory Native health without changing state.
    fn health(&self, call: &ProviderCall) -> ProviderReply;

    /// Executes Native recall: an upstream canonical fact read, or an
    /// explicitly requested upstream message search. Upstream ordering,
    /// scores, and continuation are preserved in the canonical payload.
    fn recall(&self, call: &ProviderCall) -> ProviderReply;
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

        if !valid_effect_for_operation(reply)
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
                    Some(payload.contract_id.as_str()) == canonical_result_contract_id(call.operation)
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
        if Some(call.payload.contract_id.as_str()) != canonical_payload_contract_id(call.operation)
        {
            return Some(self.reject(
                call,
                TerminalCode::InvalidRequest,
                "native.payload_contract_invalid",
            ));
        }
        None
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
            ProviderOperation::Recall => {
                self.validated_application_reply(call, self.port.recall(call))
            }
            // The projected descriptor declares no other capability, so the
            // capability gate above refuses every other operation first.
            _ => self.reject(
                call,
                TerminalCode::CapabilityUnsupported,
                "native.capability_unsupported",
            ),
        }
    }
}

/// Native operations are reads: no dispatched reply may claim an effect.
fn valid_effect_for_operation(reply: &ProviderReply) -> bool {
    reply.terminal.committed_effect().state() == CommittedEffectState::None
}

fn valid_effect_generations(call: &ProviderCall, reply: &ProviderReply) -> bool {
    let effect = reply.terminal.committed_effect();
    effect.state_generation_before() == Some(call.expected_state_generation)
        && effect.state_generation_after() == Some(reply.state_generation)
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

const fn canonical_payload_contract_id(operation: ProviderOperation) -> Option<&'static str> {
    match operation {
        ProviderOperation::Health => Some(HEALTH_CONTRACT_ID),
        ProviderOperation::Recall => Some(RECALL_CONTRACT_ID),
        _ => None,
    }
}

const fn canonical_result_contract_id(operation: ProviderOperation) -> Option<&'static str> {
    match operation {
        ProviderOperation::Health => Some(HEALTH_CONTRACT_ID),
        ProviderOperation::Recall => Some(RECALL_RESULT_CONTRACT_ID),
        _ => None,
    }
}
