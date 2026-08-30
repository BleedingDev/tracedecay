//! Focused integration tests for the topology-neutral NCM adapter boundary.
#![allow(clippy::expect_used)]

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tracedecay_memory_provider_api::contract::{
    CommittedEffectState, FallbackEligibility, TerminalCode,
};
use tracedecay_memory_provider_api::{
    CancellationToken, CanonicalPayload, HandshakeRequest, HandshakeRequestParts, MemoryProvider,
    OperationControl, OwnedExactScope, OwnedProviderId, OwnedVersionedId, ProviderCall,
    ProviderCallParts, ProviderDescriptor, ProviderLimits, ProviderOperation, ProviderReply,
    TerminalRecord,
};
use tracedecay_memory_provider_ncm::{
    NCM_PROVIDER_ID, NcmAdapterError, NcmCognitiveSurface, NcmNamespace, NcmProviderAdapter,
    NcmSurfaceCall, NcmSurfaceHandshakeRequest, NcmSurfaceHandshakeResponse,
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
        .collect::<Vec<_>>(),
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
            OwnedVersionedId::new("tracedecay.memory.test-payload.v1").expect("payload contract"),
            b"{\"fixture\":true}".to_vec(),
            ONE_SHA,
        )
        .expect("payload"),
        required_capabilities: [
            OwnedVersionedId::new(operation.capability_id()).expect("operation capability")
        ]
        .into_iter()
        .collect::<Vec<_>>(),
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
    assert_eq!(
        response.state_namespace.as_deref(),
        Some(expected_namespace.as_str())
    );
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
    assert_eq!(
        mapped.namespace,
        NcmNamespace::from_exact_scope(&request.exact_scope)
    );
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
    assert_eq!(
        reply.terminal.terminal_code,
        TerminalCode::CapabilityUnsupported
    );
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
    let request = call(NCM_PROVIDER_ID, ProviderOperation::Recall);
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
    assert_eq!(
        reply.terminal.terminal_code,
        TerminalCode::ContractViolation
    );
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
    assert_eq!(
        reply.terminal.committed_effect,
        CommittedEffectState::Unknown
    );
    assert!(reply.payload.is_none());
}

#[test]
fn handshake_surface_contract_mismatch_is_fail_closed() {
    let surface = Arc::new(MockSurface::new(NCM_PROVIDER_ID, &[], true));
    let provider = NcmProviderAdapter::new(surface).expect("adapter");
    let response = provider.handshake(&handshake(NCM_PROVIDER_ID));
    assert_eq!(
        response.terminal.terminal_code,
        TerminalCode::ContractViolation
    );
    assert!(response.descriptor.is_none());
    assert!(response.accepted_scope.is_none());
}
