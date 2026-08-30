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
//! Topology-neutral NCM adapter behind TraceDecay's memory-provider API.
//!
//! This crate deliberately contains no NCM algorithm, persistence, Python
//! binding, socket client, process supervisor, or TraceDecay storage access.
//! It validates the configured provider identity and forwards complete
//! provider-neutral requests to a narrow runtime port. The licensed NCM audit
//! and execution-topology ADR decide how that port is implemented later.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use tracedecay_memory_provider_api::contract::TerminalCode;
use tracedecay_memory_provider_api::{
    HandshakeRequest, HandshakeResponse, MemoryProvider, OwnedProviderId, ProviderCall,
    ProviderDescriptor, ProviderOperation, ProviderReply,
};

/// Construction failure before an NCM adapter can be registered.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NcmAdapterError {
    /// The supplied runtime declared a different logical provider identity.
    ProviderIdMismatch {
        /// Provider identity selected by product configuration.
        expected: OwnedProviderId,
        /// Provider identity declared by the supplied runtime port.
        declared: OwnedProviderId,
    },
}

impl fmt::Display for NcmAdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProviderIdMismatch { expected, declared } => write!(
                formatter,
                "NCM runtime declared provider {}, expected {}",
                declared.as_str(),
                expected.as_str()
            ),
        }
    }
}

impl Error for NcmAdapterError {}

/// Topology-neutral boundary implemented by the selected NCM integration.
///
/// The port owns all NCM-specific capability mapping, state, provenance,
/// receipts, cancellation propagation, and terminal truth. Implementations may
/// eventually be in-process or isolated, but this trait exposes neither choice
/// and therefore cannot select a transport before the evidence-backed topology
/// gate closes.
pub trait NcmRuntimePort: Send + Sync + 'static {
    /// Returns the runtime's current real descriptor and capability set.
    fn descriptor(&self) -> ProviderDescriptor;

    /// Performs the read-only compatibility handshake for one exact scope.
    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse;

    /// Executes one already-admitted provider-neutral call without changing
    /// its payload, scope, identities, deadline, cancellation, or extensions.
    fn invoke(&self, call: &ProviderCall) -> ProviderReply;

    /// Produces the authoritative NCM handshake rejection for an adapter-level
    /// validation failure.
    fn reject_handshake(
        &self,
        request: &HandshakeRequest,
        terminal_code: TerminalCode,
        diagnostic_id: &'static str,
    ) -> HandshakeResponse;

    /// Produces the authoritative NCM call rejection for an adapter-level
    /// validation failure.
    fn reject_call(
        &self,
        call: &ProviderCall,
        terminal_code: TerminalCode,
        diagnostic_id: &'static str,
    ) -> ProviderReply;
}

/// Provider-neutral adapter over one configured NCM runtime port.
pub struct NcmProviderAdapter {
    provider_id: OwnedProviderId,
    runtime: Arc<dyn NcmRuntimePort>,
}

impl NcmProviderAdapter {
    /// Constructs an adapter only when configuration and runtime agree on the
    /// stable logical provider identity.
    pub fn new(
        provider_id: OwnedProviderId,
        runtime: Arc<dyn NcmRuntimePort>,
    ) -> Result<Self, NcmAdapterError> {
        let declared = runtime.descriptor().provider_id;
        if declared != provider_id {
            return Err(NcmAdapterError::ProviderIdMismatch {
                expected: provider_id,
                declared,
            });
        }
        Ok(Self {
            provider_id,
            runtime,
        })
    }

    /// Returns the stable configured NCM provider identity.
    #[must_use]
    pub const fn provider_id(&self) -> &OwnedProviderId {
        &self.provider_id
    }
}

impl MemoryProvider for NcmProviderAdapter {
    fn descriptor(&self) -> ProviderDescriptor {
        self.runtime.descriptor()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        if request.provider_id != self.provider_id {
            return self.runtime.reject_handshake(
                request,
                TerminalCode::InvalidRequest,
                "ncm.provider_id_mismatch",
            );
        }
        self.runtime.handshake(request)
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        if call.provider_id != self.provider_id {
            return self.runtime.reject_call(
                call,
                TerminalCode::InvalidRequest,
                "ncm.provider_id_mismatch",
            );
        }
        if call.operation == ProviderOperation::Handshake {
            return self.runtime.reject_call(
                call,
                TerminalCode::InvalidRequest,
                "ncm.handshake_requires_handshake_port",
            );
        }
        if !self
            .runtime
            .descriptor()
            .supports(call.operation.capability_id())
        {
            return self.runtime.reject_call(
                call,
                TerminalCode::CapabilityUnsupported,
                "ncm.capability_unsupported",
            );
        }
        self.runtime.invoke(call)
    }
}
