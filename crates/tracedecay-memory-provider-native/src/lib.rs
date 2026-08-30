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

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use tracedecay_memory_provider_api::contract::TerminalCode;
use tracedecay_memory_provider_api::{
    HandshakeRequest, HandshakeResponse, MemoryProvider, OwnedProviderId, OwnedVersionedId,
    ProviderCall, ProviderDescriptor, ProviderOperation, ProviderReply, TerminalRecord,
};

/// Stable logical provider identity for TraceDecay Native memory.
pub const NATIVE_PROVIDER_ID: &str = "tracedecay.native";

/// Construction failure before a Native adapter can be registered.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NativeAdapterError {
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
            Self::ProviderIdMismatch { expected, declared } => write!(
                formatter,
                "Native application port declared provider {declared}, expected {expected}"
            ),
        }
    }
}

impl Error for NativeAdapterError {}

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

    /// Admits a provider observation through the existing Native application
    /// policy. Arbitrary observations must not be converted into facts by the
    /// adapter itself.
    fn observe(&self, call: &ProviderCall) -> ProviderReply;

    /// Executes existing Native recall and preserves Native ordering, scores,
    /// evidence, temporal state, and provenance in the canonical payload.
    fn recall(&self, call: &ProviderCall) -> ProviderReply;

    /// Executes one declared optional Native lifecycle or inspection operation.
    fn lifecycle(&self, call: &ProviderCall) -> ProviderReply;

    /// Returns a typed Native rejection with the authoritative exact-scope
    /// digest and diagnostic identity.
    fn reject(
        &self,
        call: &ProviderCall,
        terminal_code: TerminalCode,
        diagnostic_id: &'static str,
    ) -> ProviderReply;
}

/// Provider-neutral TraceDecay Native adapter over one existing application
/// port.
pub struct NativeProvider {
    port: Arc<dyn NativeMemoryApplicationPort>,
    provider_id: OwnedProviderId,
    declared_capabilities: Vec<OwnedVersionedId>,
}

impl NativeProvider {
    /// Constructs a Native provider only when the supplied port declares the
    /// stable Native identity and the mandatory provider capabilities.
    pub fn new(port: Arc<dyn NativeMemoryApplicationPort>) -> Result<Self, NativeAdapterError> {
        let descriptor = port.descriptor();
        if descriptor.provider_id.as_str() != NATIVE_PROVIDER_ID {
            return Err(NativeAdapterError::ProviderIdMismatch {
                expected: NATIVE_PROVIDER_ID,
                declared: descriptor.provider_id.as_str().to_owned(),
            });
        }
        Ok(Self {
            port,
            provider_id: descriptor.provider_id,
            declared_capabilities: descriptor.capabilities.into_iter().collect(),
        })
    }

    fn reject(
        &self,
        call: &ProviderCall,
        terminal_code: TerminalCode,
        diagnostic_id: &'static str,
    ) -> ProviderReply {
        let terminal = TerminalRecord::failure_before_dispatch(
            call.operation,
            self.provider_id.clone(),
            terminal_code,
            if call.operation_id.is_empty() {
                "native.invalid-operation-id"
            } else {
                call.operation_id.as_str()
            },
            call.exact_scope.exact_scope_sha256(),
            None,
            diagnostic_id,
        );
        ProviderReply {
            terminal,
            payload: None,
            warnings: Vec::new(),
            extensions: Vec::new(),
            // ProviderReply still requires a scalar even when structured
            // evidence truthfully records that no generation was observed.
            state_generation: call.expected_state_generation,
        }
    }
}

impl MemoryProvider for NativeProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        self.port.descriptor()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        self.port.handshake(request)
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        if call.provider_id.as_str() != self.provider_id.as_str() {
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
        if !self
            .declared_capabilities
            .iter()
            .any(|capability| capability.as_str() == call.operation.capability_id())
        {
            return self.reject(
                call,
                TerminalCode::CapabilityUnsupported,
                "native.capability_unsupported",
            );
        }
        match call.operation {
            ProviderOperation::Health => self.port.health(call),
            ProviderOperation::Observe => self.port.observe(call),
            ProviderOperation::Recall => self.port.recall(call),
            ProviderOperation::Feedback
            | ProviderOperation::Maintenance
            | ProviderOperation::Inspection
            | ProviderOperation::Correction
            | ProviderOperation::DeleteBySource
            | ProviderOperation::SnapshotExport
            | ProviderOperation::SnapshotRestore
            | ProviderOperation::Replay => self.port.lifecycle(call),
            ProviderOperation::Handshake => self.reject(
                call,
                TerminalCode::InvalidRequest,
                "native.handshake_requires_handshake_port",
            ),
        }
    }
}
