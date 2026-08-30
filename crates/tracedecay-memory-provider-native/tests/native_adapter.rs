//! Integration journeys for the Native provider adapter boundary.
#![allow(clippy::expect_used)]

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tracedecay_memory_provider_api::contract::{
    CommittedEffectState, FallbackEligibility, TerminalCode,
};
use tracedecay_memory_provider_api::{
    CancellationToken, CanonicalPayload, CommittedEffectEvidence, FallbackDirective,
    HandshakeRequest, HandshakeRequestParts, HandshakeResponse, MemoryProvider, OperationControl,
    OwnedExactScope, OwnedProviderId, OwnedVersionedId, ProviderCall, ProviderCallParts,
    ProviderDescriptor, ProviderLimits, ProviderOperation, ProviderReply, TerminalRecord,
};
use tracedecay_memory_provider_native::{
    NATIVE_PROVIDER_ID, NativeAdapterError, NativeMemoryApplicationPort, NativeProvider,
};

const ZERO_SHA: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const ONE_SHA: &str = "1111111111111111111111111111111111111111111111111111111111111111";

#[derive(Default)]
struct Counters {
    descriptor: AtomicUsize,
    handshake: AtomicUsize,
    health: AtomicUsize,
    observe: AtomicUsize,
    recall: AtomicUsize,
    lifecycle: AtomicUsize,
    reject: AtomicUsize,
}

struct MockNativePort {
    descriptor: ProviderDescriptor,
    counters: Counters,
    last_call: Mutex<Option<ProviderCall>>,
    last_handshake: Mutex<Option<HandshakeRequest>>,
}

impl MockNativePort {
    fn new(provider_id: &str, optional: &[&str]) -> Self {
        let mut capabilities = vec![
            OwnedVersionedId::new("provider.health.v1").expect("health capability"),
            OwnedVersionedId::new("observation.accept.v1").expect("observe capability"),
            OwnedVersionedId::new("recall.query.v1").expect("recall capability"),
        ];
        capabilities.extend(
            optional
                .iter()
                .map(|value| OwnedVersionedId::new(*value).expect("optional capability")),
        );
        Self {
            descriptor: ProviderDescriptor::new(
                OwnedProviderId::new(provider_id).expect("provider id"),
                ZERO_SHA,
                "native-state-v1",
                7,
                capabilities,
                limits(),
            )
            .expect("descriptor"),
            counters: Counters::default(),
            last_call: Mutex::new(None),
            last_handshake: Mutex::new(None),
        }
    }

    fn terminal(&self, call: &ProviderCall, code: TerminalCode) -> ProviderReply {
        let (effect, state_generation) =
            if code == TerminalCode::Success && call.operation.mutates_provider_state() {
                let state_generation = call.expected_state_generation.saturating_add(1);
                (
                    CommittedEffectEvidence::committed(
                        call.expected_state_generation,
                        state_generation,
                        vec![call.operation_id.clone()],
                        ONE_SHA,
                        ONE_SHA,
                    )
                    .expect("committed effect evidence"),
                    state_generation,
                )
            } else {
                (
                    CommittedEffectEvidence::none(Some(call.expected_state_generation)),
                    call.expected_state_generation,
                )
            };
        ProviderReply {
            terminal: TerminalRecord::new(
                call.operation,
                self.descriptor.provider_id.clone(),
                code,
                effect,
                FallbackDirective::forbidden(),
                call.operation_id.clone(),
                call.exact_scope.exact_scope_sha256(),
                (code != TerminalCode::Success).then(|| format!("native.{}", code.as_wire())),
            )
            .expect("terminal"),
            payload: (code == TerminalCode::Success).then(|| call.payload.clone()),
            warnings: Vec::new(),
            extensions: call.extensions.clone(),
            state_generation,
        }
    }

    fn record(&self, call: &ProviderCall) {
        *self.last_call.lock().expect("last call lock") = Some(call.clone());
    }
}

impl NativeMemoryApplicationPort for MockNativePort {
    fn descriptor(&self) -> ProviderDescriptor {
        self.counters.descriptor.fetch_add(1, Ordering::Relaxed);
        self.descriptor.clone()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        self.counters.handshake.fetch_add(1, Ordering::Relaxed);
        *self.last_handshake.lock().expect("handshake lock") = Some(request.clone());
        HandshakeResponse {
            terminal: TerminalRecord::new(
                ProviderOperation::Handshake,
                self.descriptor.provider_id.clone(),
                TerminalCode::Success,
                CommittedEffectEvidence::none(Some(self.descriptor.state_generation)),
                FallbackDirective::forbidden(),
                request.request_id.clone(),
                request.exact_scope.exact_scope_sha256(),
                None,
            )
            .expect("handshake terminal"),
            descriptor: Some(self.descriptor.clone()),
            provider_instance_id: Some("native.instance-1".to_owned()),
            state_namespace: Some("native.project".to_owned()),
            accepted_scope: Some(request.exact_scope.clone()),
            effective_limits: Some(request.host_limits.minimum(self.descriptor.limits)),
            ready_receipt_sha256: Some(ONE_SHA.to_owned()),
            warnings: Vec::new(),
        }
    }

    fn health(&self, call: &ProviderCall) -> ProviderReply {
        self.counters.health.fetch_add(1, Ordering::Relaxed);
        self.record(call);
        self.terminal(call, TerminalCode::Success)
    }

    fn observe(&self, call: &ProviderCall) -> ProviderReply {
        self.counters.observe.fetch_add(1, Ordering::Relaxed);
        self.record(call);
        self.terminal(call, TerminalCode::Success)
    }

    fn recall(&self, call: &ProviderCall) -> ProviderReply {
        self.counters.recall.fetch_add(1, Ordering::Relaxed);
        self.record(call);
        self.terminal(call, TerminalCode::Success)
    }

    fn lifecycle(&self, call: &ProviderCall) -> ProviderReply {
        self.counters.lifecycle.fetch_add(1, Ordering::Relaxed);
        self.record(call);
        self.terminal(call, TerminalCode::Success)
    }

    fn reject(
        &self,
        call: &ProviderCall,
        terminal_code: TerminalCode,
        _diagnostic_id: &'static str,
    ) -> ProviderReply {
        self.counters.reject.fetch_add(1, Ordering::Relaxed);
        self.terminal(call, terminal_code)
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

fn call(provider_id: &str, operation: ProviderOperation) -> ProviderCall {
    ProviderCall::new(ProviderCallParts {
        operation,
        provider_id: OwnedProviderId::new(provider_id).expect("provider id"),
        registration_revision: 1,
        ready_receipt_sha256: ZERO_SHA.to_owned(),
        exact_scope: scope(),
        request_id: "request-a".to_owned(),
        operation_id: format!("operation-{}", operation.capability_id()),
        expected_state_generation: 7,
        idempotency_key: operation
            .mutates_provider_state()
            .then(|| "idempotency-a".to_owned()),
        control: OperationControl::new(i64::MAX, 500, CancellationToken::new()),
        payload: CanonicalPayload::new(
            OwnedVersionedId::new("tracedecay.memory.test-payload.v1").expect("payload contract"),
            b"{\"fixture\":true}".to_vec(),
            ONE_SHA,
        )
        .expect("payload"),
        required_capabilities: vec![
            OwnedVersionedId::new(operation.capability_id()).expect("operation capability"),
        ],
        extensions: Vec::new(),
    })
    .expect("call")
}

fn handshake(provider_id: &str) -> HandshakeRequest {
    HandshakeRequest::new(HandshakeRequestParts {
        provider_id: OwnedProviderId::new(provider_id).expect("provider id"),
        registration_revision: 1,
        exact_scope: scope(),
        request_id: "handshake-a".to_owned(),
        required_capabilities: vec![
            OwnedVersionedId::new("provider.health.v1").expect("health"),
            OwnedVersionedId::new("observation.accept.v1").expect("observe"),
            OwnedVersionedId::new("recall.query.v1").expect("recall"),
        ],
        host_limits: limits(),
        control: OperationControl::new(i64::MAX, 500, CancellationToken::new()),
        challenge_nonce: [9; 32],
    })
    .expect("handshake")
}

#[test]
fn constructor_rejects_non_native_identity() {
    let port = Arc::new(MockNativePort::new("vendor.memory", &[]));
    let result = NativeProvider::new(port);
    assert_eq!(
        result.err(),
        Some(NativeAdapterError::ProviderIdMismatch {
            expected: NATIVE_PROVIDER_ID,
            declared: "vendor.memory".to_owned(),
        })
    );
}

#[test]
fn descriptor_is_owned_by_the_application_port() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port).expect("adapter");
    assert_eq!(
        provider.descriptor().provider_id.as_str(),
        NATIVE_PROVIDER_ID
    );
    assert!(provider.descriptor().supports("provider.health.v1"));
}

#[test]
fn handshake_preserves_exact_scope_and_request_identity() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let request = handshake(NATIVE_PROVIDER_ID);
    let response = provider.handshake(&request);
    assert_eq!(response.terminal.operation(), ProviderOperation::Handshake);
    assert_eq!(response.terminal.provider_id().as_str(), NATIVE_PROVIDER_ID);
    assert_eq!(response.terminal.terminal_code(), TerminalCode::Success);
    assert_eq!(
        response.terminal.committed_effect().state(),
        CommittedEffectState::None
    );
    assert_eq!(
        response
            .terminal
            .committed_effect()
            .state_generation_before(),
        Some(7)
    );
    assert_eq!(
        response
            .terminal
            .committed_effect()
            .state_generation_after(),
        Some(7)
    );
    assert_eq!(
        response.terminal.fallback().eligibility(),
        FallbackEligibility::Forbidden
    );
    assert_eq!(
        response.terminal.exact_scope_sha256(),
        request.exact_scope.exact_scope_sha256()
    );
    assert_eq!(response.accepted_scope, Some(request.exact_scope.clone()));
    assert_eq!(port.counters.handshake.load(Ordering::Relaxed), 1);
    let recorded = port
        .last_handshake
        .lock()
        .expect("handshake lock")
        .clone()
        .expect("recorded handshake");
    assert_eq!(recorded.request_id, request.request_id);
    assert_eq!(recorded.exact_scope, request.exact_scope);
}

#[test]
fn mandatory_operations_route_without_payload_transformation() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    for operation in [
        ProviderOperation::Health,
        ProviderOperation::Observe,
        ProviderOperation::Recall,
    ] {
        let request = call(NATIVE_PROVIDER_ID, operation);
        let expected_payload = request.payload.clone();
        let reply = provider.invoke(&request);
        assert_eq!(reply.terminal.operation(), operation);
        assert_eq!(reply.terminal.provider_id().as_str(), NATIVE_PROVIDER_ID);
        assert_eq!(reply.terminal.terminal_code(), TerminalCode::Success);
        assert_eq!(reply.payload, Some(expected_payload));
        if operation.mutates_provider_state() {
            assert_eq!(
                reply.terminal.committed_effect().state(),
                CommittedEffectState::Committed
            );
            assert_eq!(
                reply.terminal.committed_effect().state_generation_before(),
                Some(request.expected_state_generation)
            );
            assert_eq!(
                reply.terminal.committed_effect().state_generation_after(),
                Some(reply.state_generation)
            );
            assert_eq!(reply.terminal.provider_receipt_sha256(), Some(ONE_SHA));
        } else {
            assert_eq!(
                reply.terminal.committed_effect().state(),
                CommittedEffectState::None
            );
            assert_eq!(
                reply.terminal.committed_effect().state_generation_before(),
                Some(request.expected_state_generation)
            );
            assert_eq!(
                reply.terminal.committed_effect().state_generation_after(),
                Some(request.expected_state_generation)
            );
            assert_eq!(reply.terminal.provider_receipt_sha256(), None);
        }
        assert_eq!(
            reply.terminal.fallback().eligibility(),
            FallbackEligibility::Forbidden
        );
        let recorded = port
            .last_call
            .lock()
            .expect("last call lock")
            .clone()
            .expect("recorded call");
        assert_eq!(recorded.exact_scope, request.exact_scope);
        assert_eq!(recorded.payload, request.payload);
        let recorded_control = recorded.control.snapshot().expect("recorded control");
        let request_control = request.control.snapshot().expect("request control");
        assert_eq!(
            recorded_control.deadline_utc_micros,
            request_control.deadline_utc_micros
        );
        assert!(recorded_control.remaining_millis >= request_control.remaining_millis);
    }
    assert_eq!(port.counters.health.load(Ordering::Relaxed), 1);
    assert_eq!(port.counters.observe.load(Ordering::Relaxed), 1);
    assert_eq!(port.counters.recall.load(Ordering::Relaxed), 1);
}

#[test]
fn declared_optional_operation_routes_to_lifecycle_port() {
    let port = Arc::new(MockNativePort::new(
        NATIVE_PROVIDER_ID,
        &["feedback.record.v1"],
    ));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let request = call(NATIVE_PROVIDER_ID, ProviderOperation::Feedback);
    let reply = provider.invoke(&request);
    assert_eq!(reply.terminal.operation(), request.operation);
    assert_eq!(reply.terminal.provider_id().as_str(), NATIVE_PROVIDER_ID);
    assert_eq!(reply.terminal.terminal_code(), TerminalCode::Success);
    assert_eq!(
        reply.terminal.committed_effect().state(),
        CommittedEffectState::Committed
    );
    assert_eq!(
        reply.terminal.committed_effect().state_generation_before(),
        Some(request.expected_state_generation)
    );
    assert_eq!(
        reply.terminal.committed_effect().state_generation_after(),
        Some(request.expected_state_generation + 1)
    );
    assert_eq!(
        reply.terminal.committed_effect().committed_item_refs(),
        std::slice::from_ref(&request.operation_id)
    );
    assert_eq!(reply.terminal.provider_receipt_sha256(), Some(ONE_SHA));
    assert_eq!(
        reply.terminal.committed_effect().verification_sha256(),
        Some(ONE_SHA)
    );
    assert_eq!(
        reply.state_generation,
        request.expected_state_generation + 1
    );
    assert_eq!(
        reply.terminal.fallback().eligibility(),
        FallbackEligibility::Forbidden
    );
    assert_eq!(port.counters.lifecycle.load(Ordering::Relaxed), 1);
    assert_eq!(port.counters.reject.load(Ordering::Relaxed), 0);
}

#[test]
fn undeclared_optional_operation_is_explicitly_unsupported() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let request = call(NATIVE_PROVIDER_ID, ProviderOperation::Maintenance);
    let descriptor_calls = port.counters.descriptor.load(Ordering::Relaxed);
    let reply = provider.invoke(&request);
    assert_eq!(reply.terminal.operation(), request.operation);
    assert_eq!(reply.terminal.provider_id().as_str(), NATIVE_PROVIDER_ID);
    assert_eq!(
        reply.terminal.terminal_code(),
        TerminalCode::CapabilityUnsupported
    );
    assert_eq!(
        reply.terminal.committed_effect().state(),
        CommittedEffectState::None
    );
    assert_eq!(
        reply.terminal.committed_effect().state_generation_before(),
        None
    );
    assert_eq!(
        reply.terminal.committed_effect().state_generation_after(),
        None
    );
    assert_eq!(
        reply.terminal.fallback().eligibility(),
        FallbackEligibility::Forbidden
    );
    assert_eq!(reply.terminal.fallback().policy(), None);
    assert_eq!(reply.terminal.fallback().reason(), None);
    assert_eq!(
        reply.terminal.diagnostic_id(),
        Some("native.capability_unsupported")
    );
    assert_eq!(
        reply.terminal.exact_scope_sha256(),
        request.exact_scope.exact_scope_sha256()
    );
    assert_eq!(reply.state_generation, request.expected_state_generation);
    assert_eq!(port.counters.lifecycle.load(Ordering::Relaxed), 0);
    assert_eq!(port.counters.reject.load(Ordering::Relaxed), 0);
    assert_eq!(
        port.counters.descriptor.load(Ordering::Relaxed),
        descriptor_calls
    );
}

#[test]
fn wrong_target_identity_is_rejected_before_native_operation() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let request = call("vendor.memory", ProviderOperation::Recall);
    let reply = provider.invoke(&request);
    assert_eq!(reply.terminal.operation(), request.operation);
    assert_eq!(reply.terminal.provider_id().as_str(), NATIVE_PROVIDER_ID);
    assert_eq!(reply.terminal.terminal_code(), TerminalCode::InvalidRequest);
    assert_eq!(
        reply.terminal.diagnostic_id(),
        Some("native.provider_id_mismatch")
    );
    assert_eq!(
        reply.terminal.committed_effect().state(),
        CommittedEffectState::None
    );
    assert_eq!(
        reply.terminal.fallback().eligibility(),
        FallbackEligibility::Forbidden
    );
    assert_eq!(port.counters.recall.load(Ordering::Relaxed), 0);
    assert_eq!(port.counters.reject.load(Ordering::Relaxed), 0);
}

#[test]
fn handshake_operation_must_use_the_handshake_method() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let request = call(NATIVE_PROVIDER_ID, ProviderOperation::Handshake);
    let reply = provider.invoke(&request);
    assert_eq!(reply.terminal.operation(), ProviderOperation::Handshake);
    assert_eq!(reply.terminal.provider_id().as_str(), NATIVE_PROVIDER_ID);
    assert_eq!(reply.terminal.terminal_code(), TerminalCode::InvalidRequest);
    assert_eq!(
        reply.terminal.diagnostic_id(),
        Some("native.handshake_requires_handshake_port")
    );
    assert_eq!(
        reply.terminal.committed_effect().state(),
        CommittedEffectState::None
    );
    assert_eq!(
        reply.terminal.fallback().eligibility(),
        FallbackEligibility::Forbidden
    );
    assert_eq!(port.counters.handshake.load(Ordering::Relaxed), 0);
    assert_eq!(port.counters.reject.load(Ordering::Relaxed), 0);
}

#[test]
fn provider_has_no_internal_memory_state() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let before = provider.descriptor();
    let request = call(NATIVE_PROVIDER_ID, ProviderOperation::Observe);
    let reply = provider.invoke(&request);
    assert_eq!(
        reply.terminal.committed_effect().state(),
        CommittedEffectState::Committed
    );
    assert_eq!(
        reply.terminal.committed_effect().state_generation_before(),
        Some(request.expected_state_generation)
    );
    assert_eq!(
        reply.terminal.committed_effect().state_generation_after(),
        Some(reply.state_generation)
    );
    assert_eq!(reply.terminal.provider_receipt_sha256(), Some(ONE_SHA));
    assert_eq!(provider.descriptor(), before);
    assert_eq!(port.counters.observe.load(Ordering::Relaxed), 1);
}

#[test]
fn descriptor_capabilities_are_deterministically_ordered() {
    let port = Arc::new(MockNativePort::new(
        NATIVE_PROVIDER_ID,
        &["snapshot.export.v1", "feedback.record.v1"],
    ));
    let provider = NativeProvider::new(port).expect("adapter");
    let descriptor = provider.descriptor();
    let capabilities = descriptor
        .capabilities
        .iter()
        .map(|value| value.as_str())
        .collect::<BTreeSet<_>>();
    assert!(capabilities.contains("feedback.record.v1"));
    assert!(capabilities.contains("snapshot.export.v1"));
}
