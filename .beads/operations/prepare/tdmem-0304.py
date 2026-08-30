#!/usr/bin/env python3
"""Materialize the topology-neutral NCM provider boundary for tdmem-0304."""

from __future__ import annotations

import fnmatch
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
CRATE = ROOT / "crates/tracedecay-memory-provider-ncm"
FLOOR = "08fbe33a7c7f403191fd5d6e356c7b6681b96403"

CARGO = '''[package]
name = "tracedecay-memory-provider-ncm"
version.workspace = true
edition.workspace = true
publish = false
license = "MIT"
description = "Topology-neutral adapter boundary for licensed NCM cognitive memory"
repository = "https://github.com/BleedingDev/tracedecay"

[dependencies]
sha2 = "0.11"
tracedecay-memory-provider-api = { path = "../tracedecay-memory-provider-api" }

[lints.rust]
missing_docs = "deny"
unsafe_code = "forbid"
warnings = "deny"

[lints.clippy]
dbg_macro = "deny"
expect_used = "deny"
panic = "deny"
print_stderr = "deny"
print_stdout = "deny"
todo = "deny"
unimplemented = "deny"
unwrap_used = "deny"
'''

LIB = r'''#![forbid(unsafe_code)]
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
    OwnedExactScope, OwnedOpaqueExtension, OwnedProviderId, OwnedVersionedId, ProviderCall,
    ProviderDescriptor, ProviderLimits, ProviderOperation, ProviderReply, TerminalRecord,
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
        let mut reply = ProviderReply::failure(call, code);
        reply.terminal.diagnostic_id = Some(diagnostic_id.to_owned());
        reply
    }

    fn surface_contract_failure(call: &ProviderCall, reply: &ProviderReply) -> ProviderReply {
        let mut terminal = if call.operation.mutates_provider_state() {
            TerminalRecord::new(
                TerminalCode::EffectUnknown,
                CommittedEffectState::Unknown,
                FallbackEligibility::Forbidden,
                call.operation_id.clone(),
                call.exact_scope_sha256(),
                reply.terminal.provider_receipt_sha256.clone(),
                Some("ncm.surface_contract_violation_after_effect".to_owned()),
            )
        } else {
            TerminalRecord::new(
                TerminalCode::ContractViolation,
                CommittedEffectState::None,
                FallbackEligibility::Forbidden,
                call.operation_id.clone(),
                call.exact_scope_sha256(),
                None,
                Some("ncm.surface_contract_violation".to_owned()),
            )
        };
        let terminal = match terminal.take() {
            Ok(value) => value,
            Err(_) => return Self::invoke_failure(call, TerminalCode::InternalFailure, "ncm.adapter_terminal_construction_failed"),
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
'''

TESTS = r'''use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tracedecay_memory_provider_api::contract::{
    CommittedEffectState, FallbackEligibility, TerminalCode,
};
use tracedecay_memory_provider_api::{
    CancellationToken, CanonicalPayload, HandshakeRequest, HandshakeRequestParts,
    MemoryProvider, OperationControl, OwnedExactScope, OwnedProviderId, OwnedVersionedId,
    ProviderCall, ProviderCallParts, ProviderDescriptor, ProviderLimits, ProviderOperation,
    ProviderReply, TerminalRecord,
};
use tracedecay_memory_provider_ncm::{
    NCM_PROVIDER_ID, NcmAdapterError, NcmCognitiveSurface, NcmNamespace,
    NcmProviderAdapter, NcmSurfaceCall, NcmSurfaceHandshakeRequest,
    NcmSurfaceHandshakeResponse,
};

const ZERO_SHA: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const ONE_SHA: &str = "1111111111111111111111111111111111111111111111111111111111111111";

struct MockSurface {
    descriptor: ProviderDescriptor,
    handshake_calls: AtomicUsize,
    invoke_calls: AtomicUsize,
    last_handshake: Mutex<Option<NcmSurfaceHandshakeRequest>>,
    last_call: Mutex<Option<NcmSurfaceCall>>,
    malformed_reply_scope: bool,
}

impl MockSurface {
    fn new(provider_id: &str, optional: &[&str], malformed_reply_scope: bool) -> Self {
        let mut capabilities = vec![
            OwnedVersionedId::new("provider.health.v1").expect("health"),
            OwnedVersionedId::new("observation.accept.v1").expect("observe"),
            OwnedVersionedId::new("recall.query.v1").expect("recall"),
        ];
        capabilities.extend(
            optional
                .iter()
                .map(|value| OwnedVersionedId::new(*value).expect("optional")),
        );
        Self {
            descriptor: ProviderDescriptor::new(
                OwnedProviderId::new(provider_id).expect("provider id"),
                ZERO_SHA,
                "ncm-state-v1",
                4,
                capabilities,
                limits(),
            )
            .expect("descriptor"),
            handshake_calls: AtomicUsize::new(0),
            invoke_calls: AtomicUsize::new(0),
            last_handshake: Mutex::new(None),
            last_call: Mutex::new(None),
            malformed_reply_scope,
        }
    }

    fn terminal(
        &self,
        operation_id: &str,
        namespace: &NcmNamespace,
        operation: ProviderOperation,
        code: TerminalCode,
    ) -> TerminalRecord {
        let effect = if code == TerminalCode::Success && operation.mutates_provider_state() {
            CommittedEffectState::Committed
        } else {
            CommittedEffectState::None
        };
        let scope = if self.malformed_reply_scope {
            ONE_SHA
        } else {
            namespace.as_str()
        };
        TerminalRecord::new(
            code,
            effect,
            FallbackEligibility::Forbidden,
            operation_id.to_owned(),
            scope,
            (effect == CommittedEffectState::Committed).then(|| ONE_SHA.to_owned()),
            (code != TerminalCode::Success).then(|| format!("ncm.{}", code.as_wire())),
        )
        .expect("terminal")
    }
}

impl NcmCognitiveSurface for MockSurface {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }

    fn handshake(&self, request: &NcmSurfaceHandshakeRequest) -> NcmSurfaceHandshakeResponse {
        self.handshake_calls.fetch_add(1, Ordering::Relaxed);
        *self.last_handshake.lock().expect("handshake lock") = Some(request.clone());
        NcmSurfaceHandshakeResponse {
            terminal: self.terminal(
                &request.request_id,
                &request.namespace,
                ProviderOperation::Handshake,
                TerminalCode::Success,
            ),
            descriptor: Some(self.descriptor.clone()),
            provider_instance_id: Some("ncm.instance-1".to_owned()),
            namespace: Some(request.namespace.clone()),
            effective_limits: Some(request.host_limits.minimum(self.descriptor.limits)),
            ready_receipt_sha256: Some(ONE_SHA.to_owned()),
            warnings: Vec::new(),
        }
    }

    fn invoke(&self, call: &NcmSurfaceCall) -> ProviderReply {
        self.invoke_calls.fetch_add(1, Ordering::Relaxed);
        *self.last_call.lock().expect("call lock") = Some(call.clone());
        ProviderReply {
            terminal: self.terminal(
                &call.operation_id,
                &call.namespace,
                call.operation,
                TerminalCode::Success,
            ),
            payload: Some(call.payload.clone()),
            warnings: Vec::new(),
            extensions: call.extensions.clone(),
            state_generation: if call.operation.mutates_provider_state() {
                call.expected_state_generation.saturating_add(1)
            } else {
                call.expected_state_generation
            },
        }
    }
}

fn limits() -> ProviderLimits {
    ProviderLimits {
        request_bytes: 4096,
        response_bytes: 8192,
        observation_batch_items: 16,
        recall_candidates: 32,
        concurrent_operations: 4,
        operation_millis: 1000,
        snapshot_bytes: 65536,
        inspection_items: 64,
    }
}

fn scope() -> OwnedExactScope {
    OwnedExactScope::new(
        "profile-a",
        "project-a",
        "repository-a",
        "worktree-a",
        "refs/heads/main",
        "session-a",
        3,
    )
    .expect("scope")
}

fn handshake(provider_id: &str) -> HandshakeRequest {
    HandshakeRequest::new(HandshakeRequestParts {
        provider_id: OwnedProviderId::new(provider_id).expect("provider id"),
        registration_revision: 1,
        exact_scope: scope(),
        request_id: "handshake-a".to_owned(),
        required_capabilities: [
            "provider.health.v1",
            "observation.accept.v1",
            "recall.query.v1",
        ]
        .into_iter()
        .map(|value| OwnedVersionedId::new(value).expect("capability"))
        .collect::<BTreeSet<_>>(),
        host_limits: limits(),
        control: OperationControl::new(1000, 500, CancellationToken::new()),
        challenge_nonce: [7; 32],
    })
    .expect("handshake")
}

fn call(provider_id: &str, operation: ProviderOperation) -> ProviderCall {
    ProviderCall::new(ProviderCallParts {
        operation,
        provider_id: OwnedProviderId::new(provider_id).expect("provider id"),
        registration_revision: 1,
        ready_receipt_sha256: ZERO_SHA.to_owned(),
        exact_scope: scope(),
        request_id: "request-a".to_owned(),
        operation_id: format!("operation-{}", operation.capability_id()),
        expected_state_generation: 4,
        idempotency_key: operation
            .mutates_provider_state()
            .then(|| "idempotency-a".to_owned()),
        control: OperationControl::new(1000, 500, CancellationToken::new()),
        payload: CanonicalPayload::new(
            OwnedVersionedId::new("tracedecay.memory.test-payload.v1")
                .expect("payload contract"),
            b"{\"fixture\":true}".to_vec(),
            ONE_SHA,
        )
        .expect("payload"),
        required_capabilities: [OwnedVersionedId::new(operation.capability_id())
            .expect("operation capability")]
        .into_iter()
        .collect::<BTreeSet<_>>(),
        extensions: Vec::new(),
    })
    .expect("call")
}

#[test]
fn constructor_rejects_a_non_ncm_surface() {
    let surface = Arc::new(MockSurface::new("vendor.memory", &[], false));
    let result = NcmProviderAdapter::new(surface);
    assert_eq!(
        result.err(),
        Some(NcmAdapterError::ProviderIdMismatch {
            expected: NCM_PROVIDER_ID,
            declared: "vendor.memory".to_owned(),
        })
    );
}

#[test]
fn namespace_is_deterministic_and_opaque() {
    let first = NcmNamespace::from_exact_scope(&scope());
    let second = NcmNamespace::from_exact_scope(&scope());
    assert_eq!(first, second);
    assert_eq!(first.as_str().len(), 64);
    assert!(
        first
            .as_str()
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
    assert!(!first.as_str().contains("project-a"));
    assert!(!first.as_str().contains("worktree-a"));
}

#[test]
fn namespace_isolates_worktree_branch_and_session() {
    let original = scope();
    let mut changed_worktree = original.clone();
    changed_worktree.worktree_identity = "worktree-b".to_owned();
    let mut changed_branch = original.clone();
    changed_branch.branch_identity = "refs/heads/feature".to_owned();
    let mut changed_session = original.clone();
    changed_session.agent_session_id = "session-b".to_owned();
    let values = [
        NcmNamespace::from_exact_scope(&original),
        NcmNamespace::from_exact_scope(&changed_worktree),
        NcmNamespace::from_exact_scope(&changed_branch),
        NcmNamespace::from_exact_scope(&changed_session),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    assert_eq!(values.len(), 4);
}

#[test]
fn handshake_exposes_only_namespace_to_surface_and_reattaches_scope() {
    let surface = Arc::new(MockSurface::new(NCM_PROVIDER_ID, &[], false));
    let provider = NcmProviderAdapter::new(surface.clone()).expect("adapter");
    let request = handshake(NCM_PROVIDER_ID);
    let expected_namespace = NcmNamespace::from_exact_scope(&request.exact_scope);
    let response = provider.handshake(&request);
    assert_eq!(response.terminal.terminal_code, TerminalCode::Success);
    assert_eq!(response.accepted_scope, Some(request.exact_scope.clone()));
    assert_eq!(response.state_namespace.as_deref(), Some(expected_namespace.as_str()));
    let mapped = surface
        .last_handshake
        .lock()
        .expect("handshake lock")
        .clone()
        .expect("mapped handshake");
    assert_eq!(mapped.namespace, expected_namespace);
    assert_eq!(mapped.request_id, request.request_id);
    assert_eq!(mapped.control.snapshot(), request.control.snapshot());
}

#[test]
fn mandatory_operation_preserves_canonical_call_values() {
    let surface = Arc::new(MockSurface::new(NCM_PROVIDER_ID, &[], false));
    let provider = NcmProviderAdapter::new(surface.clone()).expect("adapter");
    let request = call(NCM_PROVIDER_ID, ProviderOperation::Observe);
    let reply = provider.invoke(&request);
    assert_eq!(reply.terminal.terminal_code, TerminalCode::Success);
    assert_eq!(reply.payload, Some(request.payload.clone()));
    let mapped = surface
        .last_call
        .lock()
        .expect("call lock")
        .clone()
        .expect("mapped call");
    assert_eq!(mapped.namespace, NcmNamespace::from_exact_scope(&request.exact_scope));
    assert_eq!(mapped.registration_revision, request.registration_revision);
    assert_eq!(mapped.ready_receipt_sha256, request.ready_receipt_sha256);
    assert_eq!(mapped.idempotency_key, request.idempotency_key);
    assert_eq!(mapped.payload, request.payload);
    assert_eq!(mapped.control.snapshot(), request.control.snapshot());
}

#[test]
fn undeclared_optional_capability_never_reaches_surface() {
    let surface = Arc::new(MockSurface::new(NCM_PROVIDER_ID, &[], false));
    let provider = NcmProviderAdapter::new(surface.clone()).expect("adapter");
    let request = call(NCM_PROVIDER_ID, ProviderOperation::Maintenance);
    let reply = provider.invoke(&request);
    assert_eq!(reply.terminal.terminal_code, TerminalCode::CapabilityUnsupported);
    assert_eq!(surface.invoke_calls.load(Ordering::Relaxed), 0);
}

#[test]
fn wrong_target_never_reaches_surface() {
    let surface = Arc::new(MockSurface::new(NCM_PROVIDER_ID, &[], false));
    let provider = NcmProviderAdapter::new(surface.clone()).expect("adapter");
    let request = call("vendor.memory", ProviderOperation::Recall);
    let reply = provider.invoke(&request);
    assert_eq!(reply.terminal.terminal_code, TerminalCode::InvalidRequest);
    assert_eq!(surface.invoke_calls.load(Ordering::Relaxed), 0);
}

#[test]
fn invoke_rejects_handshake_operation() {
    let surface = Arc::new(MockSurface::new(NCM_PROVIDER_ID, &[], false));
    let provider = NcmProviderAdapter::new(surface.clone()).expect("adapter");
    let request = call(NCM_PROVIDER_ID, ProviderOperation::Handshake);
    let reply = provider.invoke(&request);
    assert_eq!(reply.terminal.terminal_code, TerminalCode::InvalidRequest);
    assert_eq!(surface.invoke_calls.load(Ordering::Relaxed), 0);
}

#[test]
fn cancelled_request_never_reaches_surface() {
    let surface = Arc::new(MockSurface::new(NCM_PROVIDER_ID, &[], false));
    let provider = NcmProviderAdapter::new(surface.clone()).expect("adapter");
    let mut request = call(NCM_PROVIDER_ID, ProviderOperation::Recall);
    request.control.cancellation().cancel();
    let reply = provider.invoke(&request);
    assert_eq!(reply.terminal.terminal_code, TerminalCode::Cancelled);
    assert_eq!(surface.invoke_calls.load(Ordering::Relaxed), 0);
}

#[test]
fn malformed_read_reply_becomes_contract_violation() {
    let surface = Arc::new(MockSurface::new(NCM_PROVIDER_ID, &[], true));
    let provider = NcmProviderAdapter::new(surface).expect("adapter");
    let request = call(NCM_PROVIDER_ID, ProviderOperation::Recall);
    let reply = provider.invoke(&request);
    assert_eq!(reply.terminal.terminal_code, TerminalCode::ContractViolation);
    assert_eq!(reply.terminal.committed_effect, CommittedEffectState::None);
    assert!(reply.payload.is_none());
}

#[test]
fn malformed_mutating_reply_reports_unknown_effect() {
    let surface = Arc::new(MockSurface::new(NCM_PROVIDER_ID, &[], true));
    let provider = NcmProviderAdapter::new(surface).expect("adapter");
    let request = call(NCM_PROVIDER_ID, ProviderOperation::Observe);
    let reply = provider.invoke(&request);
    assert_eq!(reply.terminal.terminal_code, TerminalCode::EffectUnknown);
    assert_eq!(reply.terminal.committed_effect, CommittedEffectState::Unknown);
    assert!(reply.payload.is_none());
}

#[test]
fn handshake_surface_contract_mismatch_is_fail_closed() {
    let surface = Arc::new(MockSurface::new(NCM_PROVIDER_ID, &[], true));
    let provider = NcmProviderAdapter::new(surface).expect("adapter");
    let response = provider.handshake(&handshake(NCM_PROVIDER_ID));
    assert_eq!(response.terminal.terminal_code, TerminalCode::ContractViolation);
    assert!(response.descriptor.is_none());
    assert!(response.accepted_scope.is_none());
}
'''

README = '''# NCM Provider Boundary

This product-owned crate is the topology-neutral boundary for licensed NCM/Biomem integration. It does **not** contain NCM, copy NCM behavior, choose a process model, open state, or claim a usable provider.

The boundary:

- accepts only a surface declaring the reserved `ncm` provider identity;
- derives a stable SHA-256 namespace from the complete exact TraceDecay coding scope;
- exposes only that opaque namespace—not profile, project, repository, worktree, branch, or agent-session identifiers—to the licensed surface;
- preserves canonical payload bytes, idempotency, expected generation, required capabilities, deadlines, cancellation, and readiness identity;
- reattaches the original TraceDecay scope only after a compatible surface response;
- rejects wrong identities, undeclared capabilities, malformed scope/operation terminals, and fake ready responses before product use;
- converts malformed post-effect replies into `effect_unknown` rather than pretending no provider effect occurred.

The crate depends only on `tracedecay-memory-provider-api` and `sha2`. It has no TraceDecay store, database, code-index, daemon, host, dashboard, Native adapter, OCEAN, socket, process, or NCM implementation dependency. The licensed surface audit and execution-topology decision remain owned by `tdmem-0701` and `tdmem-0702`.
'''


def write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def add_workspace_member() -> None:
    path = ROOT / "Cargo.toml"
    text = path.read_text(encoding="utf-8")
    member = '    "crates/tracedecay-memory-provider-ncm",\n'
    if member in text:
        return
    marker = '    "crates/tracedecay-memory-provider-native",\n'
    if marker not in text:
        raise SystemExit("workspace insertion marker is missing")
    path.write_text(text.replace(marker, marker + member, 1), encoding="utf-8")


def git_changed(path: str) -> bool:
    return subprocess.run(
        ["git", "diff", "--quiet", "--", path], cwd=ROOT, check=False
    ).returncode != 0


def product_patterns() -> list[str]:
    policy = json.loads(
        (ROOT / "product/upstream/patch-footprint-policy.json").read_text(encoding="utf-8")
    )
    return [value for value in policy["product_owned_paths"] if isinstance(value, str)]


def is_product_owned(path: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)


def diff_stats() -> tuple[int, int, int]:
    result = subprocess.run(
        ["git", "diff", "--no-renames", "--numstat", FLOOR, "--"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    patterns = product_patterns()
    production = 0
    tests = 0
    lines = 0
    for raw in result.stdout.splitlines():
        if not raw:
            continue
        added, deleted, path = raw.split("\t", 2)
        if added == "-" or deleted == "-" or is_product_owned(path, patterns):
            continue
        changed = int(added) + int(deleted)
        lines += changed
        name = Path(path).name.lower()
        if path.startswith("tests/") or "/tests/" in path or "fixture" in name or name.endswith("_test.rs"):
            tests += 1
        else:
            production += 1
    return production, tests, lines


def update_convergence_map() -> None:
    path = ROOT / "product/upstream/convergence-map.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    entries = {entry["path"]: entry for entry in document["entries"]}
    for upstream_path in ("Cargo.toml", "Cargo.lock"):
        entry = entries.get(upstream_path)
        if entry is None:
            if upstream_path == "Cargo.lock" and not git_changed("Cargo.lock"):
                continue
            raise SystemExit(f"missing convergence entry for {upstream_path}")
        if "tdmem-0304" not in entry["bead_ids"]:
            entry["bead_ids"].append("tdmem-0304")
        for command in (
            "cargo clippy -p tracedecay-memory-provider-ncm --all-targets --locked -- -D warnings",
            "cargo test -p tracedecay-memory-provider-ncm --locked",
        ):
            if command not in entry["verification"]:
                entry["verification"].append(command)
    production, tests, lines = diff_stats()
    document["snapshot"] = {
        "upstream_existing_production_files": production,
        "upstream_existing_test_or_fixture_files": tests,
        "total_upstream_changed_lines": lines,
        "composition_root_files": 0,
        "exception_zone_files": 0,
        "observed_state": "The product branch changes only additive workspace membership and generated path-package lock entries; provider API, fabric, Native adapter, and topology-neutral NCM boundary remain product-owned.",
    }
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")


write(CRATE / "Cargo.toml", CARGO)
write(CRATE / "src/lib.rs", LIB)
write(CRATE / "tests/ncm_adapter.rs", TESTS)
write(CRATE / "README.md", README)
add_workspace_member()
subprocess.run(
    ["cargo", "check", "-p", "tracedecay-memory-provider-ncm", "--all-targets"],
    cwd=ROOT,
    check=True,
)
subprocess.run(
    ["cargo", "fmt", "--package", "tracedecay-memory-provider-ncm"],
    cwd=ROOT,
    check=True,
)
update_convergence_map()

manifest = [
    {
        "path": "crates/tracedecay-memory-provider-ncm",
        "message": "feat(memory): add topology-neutral NCM provider boundary",
    }
]
for path, message in (
    ("Cargo.toml", "build(memory): register NCM provider workspace member"),
    ("Cargo.lock", "build(memory): lock NCM provider path package"),
    (
        "product/upstream/convergence-map.json",
        "docs(upstream): map NCM provider workspace wiring",
    ),
):
    status = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all", "--", path],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if status.strip():
        manifest.append({"path": path, "message": message})
write(
    ROOT / ".beads/operations/prepared-files.json",
    json.dumps(manifest, indent=2) + "\n",
)
Path(__file__).unlink()
