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
//! Topology-neutral TraceDecay adapter boundary for licensed NCM memory.
//!
//! This crate does not implement NCM, select an in-process or local-process
//! topology, open NCM state, or modify licensed behavior. It translates the
//! provider-neutral runtime contract into an opaque NCM surface contract. Raw
//! coding identities never cross that surface: TraceDecay derives one stable
//! namespace digest from the exact admitted scope, while the adapter retains
//! responsibility for reattaching the original scope to public responses.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use sha2::{Digest, Sha256};
use tracedecay_memory_provider_api::contract::{
    CommittedEffectState, FallbackEligibility, TerminalCode,
};
use tracedecay_memory_provider_api::{
    CanonicalPayload, HandshakeRequest, HandshakeResponse, MemoryProvider, OperationControl,
    OwnedExactScope, OwnedOpaqueExtension, OwnedVersionedId, ProviderCall, ProviderDescriptor,
    ProviderLimits, ProviderOperation, ProviderReply, TerminalRecord,
};

/// Stable logical provider identity reserved for NCM.
pub const NCM_PROVIDER_ID: &str = "ncm";
const NAMESPACE_DOMAIN: &[u8] = b"tracedecay.ncm.scope.v1\0";

/// Construction failure before an NCM surface can be registered.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NcmAdapterError {
    /// The supplied surface did not expose the reserved NCM provider identity.
    ProviderIdMismatch {
        /// Required stable identity.
        expected: &'static str,
        /// Identity declared by the supplied surface.
        declared: String,
    },
}

impl fmt::Display for NcmAdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProviderIdMismatch { expected, declared } => write!(
                formatter,
                "NCM surface declared provider {declared}, expected {expected}"
            ),
        }
    }
}

impl Error for NcmAdapterError {}

/// Opaque provider-local namespace derived from a complete TraceDecay coding
/// scope.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NcmNamespace(String);

impl NcmNamespace {
    /// Derives a stable namespace without exposing raw profile, project,
    /// repository, worktree, branch, or agent-session identifiers to NCM.
    #[must_use]
    pub fn from_exact_scope(scope: &OwnedExactScope) -> Self {
        let mut digest = Sha256::new();
        digest.update(NAMESPACE_DOMAIN);
        for value in [
            scope.profile_id.as_bytes(),
            scope.project_id.as_bytes(),
            scope.repository_identity.as_bytes(),
            scope.worktree_identity.as_bytes(),
            scope.branch_identity.as_bytes(),
            scope.agent_session_id.as_bytes(),
        ] {
            digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
            digest.update(value);
        }
        digest.update(scope.scope_revision.to_be_bytes());
        Self(hex_digest(&digest.finalize()))
    }

    /// Returns the lowercase SHA-256 namespace digest.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Handshake request visible to the licensed NCM surface.
#[derive(Clone, Debug)]
pub struct NcmSurfaceHandshakeRequest {
    /// Accepted TraceDecay registration revision.
    pub registration_revision: u64,
    /// Opaque provider-local state namespace.
    pub namespace: NcmNamespace,
    /// Stable request identity.
    pub request_id: String,
    /// Required provider-neutral capabilities.
    pub required_capabilities: BTreeSet<OwnedVersionedId>,
    /// Finite host ceilings.
    pub host_limits: ProviderLimits,
    /// Deadline and live cancellation.
    pub control: OperationControl,
    /// Challenge nonce for this handshake.
    pub challenge_nonce: [u8; 32],
}

/// Handshake response returned by the licensed NCM surface without raw coding
/// scope.
#[derive(Clone, Debug)]
pub struct NcmSurfaceHandshakeResponse {
    /// Typed provider terminal.
    pub terminal: TerminalRecord,
    /// Real NCM descriptor when ready.
    pub descriptor: Option<ProviderDescriptor>,
    /// Opaque runtime instance identity.
    pub provider_instance_id: Option<String>,
    /// Accepted opaque namespace.
    pub namespace: Option<NcmNamespace>,
    /// Effective finite limits.
    pub effective_limits: Option<ProviderLimits>,
    /// Scoped readiness receipt digest.
    pub ready_receipt_sha256: Option<String>,
    /// Bounded non-secret warnings.
    pub warnings: Vec<String>,
}

/// Provider operation visible to the licensed NCM surface.
#[derive(Clone, Debug)]
pub struct NcmSurfaceCall {
    /// Provider-neutral operation identity.
    pub operation: ProviderOperation,
    /// Opaque provider-local namespace.
    pub namespace: NcmNamespace,
    /// Accepted registration revision.
    pub registration_revision: u64,
    /// Compatible readiness receipt digest.
    pub ready_receipt_sha256: String,
    /// Stable request identity.
    pub request_id: String,
    /// Stable effect identity.
    pub operation_id: String,
    /// Provider state generation expected by the caller.
    pub expected_state_generation: u64,
    /// Deterministic key for mutating operations.
    pub idempotency_key: Option<String>,
    /// Deadline and live cancellation.
    pub control: OperationControl,
    /// Canonical provider-neutral payload.
    pub payload: CanonicalPayload,
    /// Required capabilities for this call.
    pub required_capabilities: BTreeSet<OwnedVersionedId>,
    /// Opaque optional extensions.
    pub extensions: Vec<OwnedOpaqueExtension>,
}

impl NcmSurfaceCall {
    fn from_provider_call(call: &ProviderCall) -> Self {
        Self {
            operation: call.operation,
            namespace: NcmNamespace::from_exact_scope(&call.exact_scope),
            registration_revision: call.registration_revision,
            ready_receipt_sha256: call.ready_receipt_sha256.clone(),
            request_id: call.request_id.clone(),
            operation_id: call.operation_id.clone(),
            expected_state_generation: call.expected_state_generation,
            idempotency_key: call.idempotency_key.clone(),
            control: call.control.clone(),
            payload: call.payload.clone(),
            required_capabilities: call.required_capabilities.clone(),
            extensions: call.extensions.clone(),
        }
    }
}

/// Licensed NCM behavior surface supplied after the M6 surface audit and
/// topology decision.
///
/// This trait is intentionally topology-neutral. An implementation may call a
/// Rust library or a supervised local process, but callers observe the same
/// bounded provider contract and opaque namespace.
pub trait NcmCognitiveSurface: Send + Sync + 'static {
    /// Returns the real NCM implementation/capability descriptor.
    fn descriptor(&self) -> ProviderDescriptor;

    /// Performs a read-only compatibility handshake for one opaque namespace.
    fn handshake(&self, request: &NcmSurfaceHandshakeRequest) -> NcmSurfaceHandshakeResponse;

    /// Executes one provider-local operation using canonical provider-neutral
    /// bytes and no raw coding identities.
    fn invoke(&self, call: &NcmSurfaceCall) -> ProviderReply;
}

/// Provider-neutral adapter over one audited licensed NCM surface.
pub struct NcmProviderAdapter {
    surface: Arc<dyn NcmCognitiveSurface>,
}

impl NcmProviderAdapter {
    /// Constructs an adapter only for a surface declaring the reserved NCM
    /// identity. This does not select an execution topology or open state.
    pub fn new(surface: Arc<dyn NcmCognitiveSurface>) -> Result<Self, NcmAdapterError> {
        let descriptor = surface.descriptor();
        if descriptor.provider_id.as_str() != NCM_PROVIDER_ID {
            return Err(NcmAdapterError::ProviderIdMismatch {
                expected: NCM_PROVIDER_ID,
                declared: descriptor.provider_id.as_str().to_owned(),
            });
        }
        Ok(Self { surface })
    }

    fn handshake_failure(
        request: &HandshakeRequest,
        code: TerminalCode,
        diagnostic_id: &'static str,
    ) -> HandshakeResponse {
        let scope = NcmNamespace::from_exact_scope(&request.exact_scope);
        let terminal = TerminalRecord::new(
            code,
            CommittedEffectState::None,
            FallbackEligibility::Forbidden,
            request.request_id.clone(),
            scope.as_str(),
            None,
            Some(diagnostic_id.to_owned()),
        );
        let terminal = match terminal {
            Ok(value) => value,
            Err(_) => return Self::unreachable_handshake_failure(request, scope),
        };
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

    fn unreachable_handshake_failure(
        request: &HandshakeRequest,
        scope: NcmNamespace,
    ) -> HandshakeResponse {
        let terminal = TerminalRecord {
            terminal_code: TerminalCode::InternalFailure,
            committed_effect: CommittedEffectState::None,
            fallback: FallbackEligibility::Forbidden,
            operation_id: request.request_id.clone(),
            exact_scope_sha256: scope.0,
            provider_receipt_sha256: None,
            diagnostic_id: Some("ncm.adapter_terminal_construction_failed".to_owned()),
        };
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

    fn invoke_failure(
        call: &ProviderCall,
        code: TerminalCode,
        diagnostic_id: &'static str,
    ) -> ProviderReply {
        let scope = NcmNamespace::from_exact_scope(&call.exact_scope);
        let terminal = match TerminalRecord::new(
            code,
            CommittedEffectState::None,
            FallbackEligibility::Forbidden,
            call.operation_id.clone(),
            scope.as_str(),
            None,
            Some(diagnostic_id.to_owned()),
        ) {
            Ok(value) => value,
            Err(_) => TerminalRecord {
                terminal_code: TerminalCode::InternalFailure,
                committed_effect: CommittedEffectState::None,
                fallback: FallbackEligibility::Forbidden,
                operation_id: call.operation_id.clone(),
                exact_scope_sha256: scope.0,
                provider_receipt_sha256: None,
                diagnostic_id: Some("ncm.adapter_terminal_construction_failed".to_owned()),
            },
        };
        ProviderReply {
            terminal,
            payload: None,
            warnings: Vec::new(),
            extensions: Vec::new(),
            state_generation: call.expected_state_generation,
        }
    }

    fn surface_contract_failure(call: &ProviderCall, reply: &ProviderReply) -> ProviderReply {
        let terminal = if call.operation.mutates_provider_state() {
            TerminalRecord::new(
                TerminalCode::EffectUnknown,
                CommittedEffectState::Unknown,
                FallbackEligibility::Forbidden,
                call.operation_id.clone(),
                NcmNamespace::from_exact_scope(&call.exact_scope).as_str(),
                reply.terminal.provider_receipt_sha256.clone(),
                Some("ncm.surface_contract_violation_after_effect".to_owned()),
            )
        } else {
            TerminalRecord::new(
                TerminalCode::ContractViolation,
                CommittedEffectState::None,
                FallbackEligibility::Forbidden,
                call.operation_id.clone(),
                NcmNamespace::from_exact_scope(&call.exact_scope).as_str(),
                None,
                Some("ncm.surface_contract_violation".to_owned()),
            )
        };
        let terminal = match terminal {
            Ok(value) => value,
            Err(_) => {
                return Self::invoke_failure(
                    call,
                    TerminalCode::InternalFailure,
                    "ncm.adapter_terminal_construction_failed",
                );
            }
        };
        ProviderReply {
            terminal,
            payload: None,
            warnings: Vec::new(),
            extensions: Vec::new(),
            state_generation: reply.state_generation,
        }
    }

    fn valid_surface_reply(call: &ProviderCall, reply: &ProviderReply) -> bool {
        reply.terminal.operation_id == call.operation_id
            && reply.terminal.exact_scope_sha256
                == NcmNamespace::from_exact_scope(&call.exact_scope).as_str()
            && reply.terminal.fallback == FallbackEligibility::Forbidden
    }
}

impl MemoryProvider for NcmProviderAdapter {
    fn descriptor(&self) -> ProviderDescriptor {
        self.surface.descriptor()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        if request.provider_id.as_str() != NCM_PROVIDER_ID {
            return Self::handshake_failure(
                request,
                TerminalCode::InvalidRequest,
                "ncm.provider_id_mismatch",
            );
        }
        if let Err(code) = request.control.snapshot() {
            return Self::handshake_failure(request, code, "ncm.request_control_terminal");
        }
        let descriptor = self.surface.descriptor();
        if request
            .required_capabilities
            .iter()
            .any(|capability| !descriptor.supports(capability.as_str()))
        {
            return Self::handshake_failure(
                request,
                TerminalCode::CapabilityUnsupported,
                "ncm.required_capability_missing",
            );
        }
        let namespace = NcmNamespace::from_exact_scope(&request.exact_scope);
        let surface_request = NcmSurfaceHandshakeRequest {
            registration_revision: request.registration_revision,
            namespace: namespace.clone(),
            request_id: request.request_id.clone(),
            required_capabilities: request.required_capabilities.clone(),
            host_limits: request.host_limits,
            control: request.control.clone(),
            challenge_nonce: request.challenge_nonce,
        };
        let surface_response = self.surface.handshake(&surface_request);
        if surface_response.terminal.operation_id != request.request_id
            || surface_response.terminal.exact_scope_sha256 != namespace.as_str()
            || surface_response.terminal.fallback != FallbackEligibility::Forbidden
        {
            return Self::handshake_failure(
                request,
                TerminalCode::ContractViolation,
                "ncm.surface_handshake_contract_violation",
            );
        }
        if surface_response.terminal.terminal_code != TerminalCode::Success {
            return HandshakeResponse {
                terminal: surface_response.terminal,
                descriptor: None,
                provider_instance_id: None,
                state_namespace: None,
                accepted_scope: None,
                effective_limits: None,
                ready_receipt_sha256: None,
                warnings: surface_response.warnings,
            };
        }
        let Some(response_descriptor) = surface_response.descriptor else {
            return Self::handshake_failure(
                request,
                TerminalCode::ContractViolation,
                "ncm.surface_missing_descriptor",
            );
        };
        if response_descriptor.provider_id.as_str() != NCM_PROVIDER_ID
            || surface_response.namespace.as_ref() != Some(&namespace)
            || surface_response.provider_instance_id.is_none()
            || surface_response.effective_limits.is_none()
            || surface_response.ready_receipt_sha256.is_none()
        {
            return Self::handshake_failure(
                request,
                TerminalCode::ContractViolation,
                "ncm.surface_incomplete_ready_response",
            );
        }
        HandshakeResponse {
            terminal: surface_response.terminal,
            descriptor: Some(response_descriptor),
            provider_instance_id: surface_response.provider_instance_id,
            state_namespace: Some(namespace.0),
            accepted_scope: Some(request.exact_scope.clone()),
            effective_limits: surface_response.effective_limits,
            ready_receipt_sha256: surface_response.ready_receipt_sha256,
            warnings: surface_response.warnings,
        }
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        if call.provider_id.as_str() != NCM_PROVIDER_ID {
            return Self::invoke_failure(
                call,
                TerminalCode::InvalidRequest,
                "ncm.provider_id_mismatch",
            );
        }
        if call.operation == ProviderOperation::Handshake {
            return Self::invoke_failure(
                call,
                TerminalCode::InvalidRequest,
                "ncm.handshake_requires_handshake_port",
            );
        }
        if let Err(code) = call.control.snapshot() {
            return Self::invoke_failure(call, code, "ncm.request_control_terminal");
        }
        let descriptor = self.surface.descriptor();
        if !descriptor.supports(call.operation.capability_id()) {
            return Self::invoke_failure(
                call,
                TerminalCode::CapabilityUnsupported,
                "ncm.capability_unsupported",
            );
        }
        let surface_call = NcmSurfaceCall::from_provider_call(call);
        let reply = self.surface.invoke(&surface_call);
        if Self::valid_surface_reply(call, &reply) {
            reply
        } else {
            Self::surface_contract_failure(call, &reply)
        }
    }
}

fn hex_digest(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len().saturating_mul(2));
    for byte in value {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}
