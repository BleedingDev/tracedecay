//! Integration journeys for the Native provider adapter boundary.
#![allow(clippy::expect_used)]

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::Value;
use tracedecay_memory_provider_api::contract::{
    CommittedEffectState, FallbackEligibility, TerminalCode,
};
use tracedecay_memory_provider_api::{
    ApiError, CancellationToken, CanonicalPayload, CommittedEffectEvidence, FallbackDirective,
    HandshakeRequest, HandshakeRequestParts, HandshakeResponse, MemoryProvider, OperationControl,
    OwnedExactScope, OwnedProviderId, OwnedVersionedId, PayloadSanitizationReceipt,
    PayloadSanitizationReceiptParts, ProviderCall, ProviderCallParts, ProviderDescriptor,
    ProviderLimits, ProviderOperation, ProviderReply, TerminalRecord,
};
use tracedecay_memory_provider_native::{
    NATIVE_FACT_PROMOTION_OBSERVATION_KIND, NATIVE_FACT_PROMOTION_PAYLOAD_CONTRACT_ID,
    NATIVE_PROVIDER_ID, NATIVE_STAGED_SESSION_OBSERVATION_KIND,
    NATIVE_STAGED_SESSION_PAYLOAD_CONTRACT_ID, NativeAdapterError, NativeMemoryApplicationPort,
    NativeObservation, NativeProvider, OBSERVATION_CONTRACT_ID,
};

const ZERO_SHA: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const ONE_SHA: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const RESOLVED_SCOPE_DIGEST: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const FIXTURE_SHA: &str = "ffbc2dfc402782325da71132100e74ff511d1585dd80e4ea196ed4bcace3fef2";
const FACT_SHAPED_OBSERVATION_SHA: &str =
    "345f350426c8d55ebaa4b10c41862eaeae8b910cd3dceb939ef3d256885c5c6d";
const FACT_SHAPED_OBSERVATION: &[u8] = b"{\"native_fact\":{\"owner\":\"project\",\"trust\":0.9,\"temporal\":\"current\",\"receipt\":\"receipt\"}}";
const PROMOTED_OBSERVATION: &[u8] = b"{\"canonical_payload\":{\"fact\":\"promote\"},\"observation_kind\":\"native.fact_promoted.v1\",\"payload_contract\":\"tracedecay.memory.observation.native-fact-promotion.v1\"}";
const PROMOTED_OBSERVATION_SHA: &str =
    "e5e7fdeecb1a62f0ecd1f5330b50cd96bb4e90dc26c37e114d7860ffdcf0e9a2";
const SESSION_MESSAGE_OBSERVATION: &str = "{\"canonical_payload\":{\"event\":\"staged\"},\"observation_kind\":\"session.message_committed.v1\",\"payload_contract\":\"tracedecay.memory.observation.session-message.v1\"}";
const SESSION_MESSAGE_OBSERVATION_SHA: &str =
    "9944b6c6a88edd3d3518110ebcba9566de1b077ff3ae1f0c19a21c47be76b291";
/// The session kind carrying the fact-promotion payload contract: a kind the
/// adapter now accepts must still be refused when its declared contract pair
/// is broken.
const SESSION_KIND_WRONG_CONTRACT: &str = "{\"canonical_payload\":{\"event\":\"staged\"},\"observation_kind\":\"session.message_committed.v1\",\"payload_contract\":\"tracedecay.memory.observation.native-fact-promotion.v1\"}";
const SESSION_KIND_WRONG_CONTRACT_SHA: &str =
    "c4573bb3d11015734ec9a19f8e1294635f65ba8d6b96d77a5e64b7773210f733";

/// What the adapter handed the port: the classified variant plus the exact
/// envelope fields it carried.
#[derive(Clone, Debug, PartialEq)]
struct ObservedVariant {
    variant: &'static str,
    observation_kind: String,
    payload_contract: String,
    canonical_payload: Value,
}

impl ObservedVariant {
    fn capture(observation: &NativeObservation<'_>) -> Self {
        let variant = match observation {
            NativeObservation::FactPromotion(_) => "fact_promotion",
            NativeObservation::StagedSession(_) => "staged_session",
        };
        Self {
            variant,
            observation_kind: observation.observation_kind().to_owned(),
            payload_contract: observation.payload_contract().to_owned(),
            canonical_payload: observation.canonical_payload().clone(),
        }
    }
}

#[derive(Default)]
struct Counters {
    descriptor: AtomicUsize,
    handshake: AtomicUsize,
    health: AtomicUsize,
    observe: AtomicUsize,
    recall: AtomicUsize,
    feedback: AtomicUsize,
    maintenance: AtomicUsize,
    inspection: AtomicUsize,
    correction: AtomicUsize,
    delete_by_source: AtomicUsize,
    snapshot_export: AtomicUsize,
    snapshot_restore: AtomicUsize,
    replay: AtomicUsize,
}

impl Counters {
    fn operation_calls(&self, operation: ProviderOperation) -> usize {
        match operation {
            ProviderOperation::Handshake => self.handshake.load(Ordering::Relaxed),
            ProviderOperation::Health => self.health.load(Ordering::Relaxed),
            ProviderOperation::Observe => self.observe.load(Ordering::Relaxed),
            ProviderOperation::Recall => self.recall.load(Ordering::Relaxed),
            ProviderOperation::Feedback => self.feedback.load(Ordering::Relaxed),
            ProviderOperation::Maintenance => self.maintenance.load(Ordering::Relaxed),
            ProviderOperation::Inspection => self.inspection.load(Ordering::Relaxed),
            ProviderOperation::Correction => self.correction.load(Ordering::Relaxed),
            ProviderOperation::DeleteBySource => self.delete_by_source.load(Ordering::Relaxed),
            ProviderOperation::SnapshotExport => self.snapshot_export.load(Ordering::Relaxed),
            ProviderOperation::SnapshotRestore => self.snapshot_restore.load(Ordering::Relaxed),
            ProviderOperation::Replay => self.replay.load(Ordering::Relaxed),
        }
    }
}

struct MockNativePort {
    descriptor: ProviderDescriptor,
    followup_descriptor: Mutex<Option<ProviderDescriptor>>,
    observation_code: TerminalCode,
    counters: Counters,
    last_call: Mutex<Option<ProviderCall>>,
    last_observation: Mutex<Option<ObservedVariant>>,
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
            followup_descriptor: Mutex::new(None),
            observation_code: TerminalCode::Success,
            counters: Counters::default(),
            last_call: Mutex::new(None),
            last_observation: Mutex::new(None),
            last_handshake: Mutex::new(None),
        }
    }

    fn with_followup_descriptor(
        provider_id: &str,
        optional: &[&str],
        followup_descriptor: ProviderDescriptor,
    ) -> Self {
        let port = Self::new(provider_id, optional);
        *port
            .followup_descriptor
            .lock()
            .expect("followup descriptor lock") = Some(followup_descriptor);
        port
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
        let call_index = self.counters.descriptor.fetch_add(1, Ordering::Relaxed);
        if call_index > 0
            && let Some(descriptor) = self
                .followup_descriptor
                .lock()
                .expect("followup descriptor lock")
                .as_ref()
        {
            return descriptor.clone();
        }
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

    fn observe(&self, observation: NativeObservation<'_>) -> ProviderReply {
        self.counters.observe.fetch_add(1, Ordering::Relaxed);
        *self.last_observation.lock().expect("last observation lock") =
            Some(ObservedVariant::capture(&observation));
        self.record(observation.call());
        self.terminal(observation.call(), self.observation_code)
    }

    fn recall(&self, call: &ProviderCall) -> ProviderReply {
        self.counters.recall.fetch_add(1, Ordering::Relaxed);
        self.record(call);
        self.terminal(call, TerminalCode::Success)
    }

    fn feedback(&self, call: &ProviderCall) -> ProviderReply {
        self.counters.feedback.fetch_add(1, Ordering::Relaxed);
        self.record(call);
        self.terminal(call, TerminalCode::Success)
    }

    fn maintenance(&self, call: &ProviderCall) -> ProviderReply {
        self.counters.maintenance.fetch_add(1, Ordering::Relaxed);
        self.record(call);
        self.terminal(call, TerminalCode::Success)
    }

    fn inspection(&self, call: &ProviderCall) -> ProviderReply {
        self.counters.inspection.fetch_add(1, Ordering::Relaxed);
        self.record(call);
        self.terminal(call, TerminalCode::Success)
    }

    fn correction(&self, call: &ProviderCall) -> ProviderReply {
        self.counters.correction.fetch_add(1, Ordering::Relaxed);
        self.record(call);
        self.terminal(call, TerminalCode::Success)
    }

    fn delete_by_source(&self, call: &ProviderCall) -> ProviderReply {
        self.counters
            .delete_by_source
            .fetch_add(1, Ordering::Relaxed);
        self.record(call);
        self.terminal(call, TerminalCode::Success)
    }

    fn snapshot_export(&self, call: &ProviderCall) -> ProviderReply {
        self.counters
            .snapshot_export
            .fetch_add(1, Ordering::Relaxed);
        self.record(call);
        self.terminal(call, TerminalCode::Success)
    }

    fn snapshot_restore(&self, call: &ProviderCall) -> ProviderReply {
        self.counters
            .snapshot_restore
            .fetch_add(1, Ordering::Relaxed);
        self.record(call);
        self.terminal(call, TerminalCode::Success)
    }

    fn replay(&self, call: &ProviderCall) -> ProviderReply {
        self.counters.replay.fetch_add(1, Ordering::Relaxed);
        self.record(call);
        self.terminal(call, TerminalCode::Success)
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
        RESOLVED_SCOPE_DIGEST,
    )
    .expect("scope")
}

fn operation_contract_id(operation: ProviderOperation) -> &'static str {
    match operation {
        ProviderOperation::Handshake => "tracedecay.memory.provider.handshake.v1",
        ProviderOperation::Health => "tracedecay.memory.provider.health.v1",
        ProviderOperation::Observe => OBSERVATION_CONTRACT_ID,
        ProviderOperation::Recall => "tracedecay.memory.provider.recall.v1",
        ProviderOperation::Feedback => "tracedecay.memory.provider.feedback.v1",
        ProviderOperation::Maintenance => "tracedecay.memory.provider.maintenance.v1",
        ProviderOperation::Inspection => "tracedecay.memory.provider.inspection.v1",
        ProviderOperation::Correction => "tracedecay.memory.provider.correction.v1",
        ProviderOperation::DeleteBySource => "tracedecay.memory.provider.deletion-by-source.v1",
        ProviderOperation::SnapshotExport => "tracedecay.memory.provider.snapshot-export.v1",
        ProviderOperation::SnapshotRestore => "tracedecay.memory.provider.snapshot-restore.v1",
        ProviderOperation::Replay => "tracedecay.memory.provider.replay.v1",
    }
}

fn optional_provider_operations() -> [(ProviderOperation, &'static str); 8] {
    [
        (ProviderOperation::Feedback, "feedback.record.v1"),
        (ProviderOperation::Maintenance, "maintenance.run.v1"),
        (ProviderOperation::Inspection, "inspection.read.v1"),
        (ProviderOperation::Correction, "correction.apply.v1"),
        (ProviderOperation::DeleteBySource, "deletion.by_source.v1"),
        (ProviderOperation::SnapshotExport, "snapshot.export.v1"),
        (ProviderOperation::SnapshotRestore, "snapshot.restore.v1"),
        (ProviderOperation::Replay, "replay.apply.v1"),
    ]
}

fn all_provider_operations() -> [ProviderOperation; 12] {
    [
        ProviderOperation::Handshake,
        ProviderOperation::Health,
        ProviderOperation::Observe,
        ProviderOperation::Recall,
        ProviderOperation::Feedback,
        ProviderOperation::Maintenance,
        ProviderOperation::Inspection,
        ProviderOperation::Correction,
        ProviderOperation::DeleteBySource,
        ProviderOperation::SnapshotExport,
        ProviderOperation::SnapshotRestore,
        ProviderOperation::Replay,
    ]
}

fn call(provider_id: &str, operation: ProviderOperation) -> ProviderCall {
    let (payload_bytes, payload_sha256) = if operation == ProviderOperation::Observe {
        (PROMOTED_OBSERVATION.to_vec(), PROMOTED_OBSERVATION_SHA)
    } else {
        (b"{\"fixture\":true}".to_vec(), FIXTURE_SHA)
    };
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
            OwnedVersionedId::new(operation_contract_id(operation)).expect("payload contract"),
            payload_bytes,
            payload_sha256,
        )
        .expect("payload"),
        required_capabilities: vec![
            OwnedVersionedId::new(operation.capability_id()).expect("operation capability"),
        ],
        extensions: Vec::new(),
    })
    .map(admitted)
    .expect("call")
}

/// Sanitizer revision this harness stands in for. The real revision is derived
/// by `tracedecay-memory-hygiene` from the canonical policy document.
const TEST_SANITIZER_REVISION: &str = "tracedecay.memory.observation.hygiene.v1+native-test";

/// Attaches the receipt the admitted hygiene pipeline mints for a payload it
/// read and left byte-identical. Observation dispatch fails closed without one.
fn admitted(call: ProviderCall) -> ProviderCall {
    if call.operation != ProviderOperation::Observe {
        return call;
    }
    let receipt =
        PayloadSanitizationReceipt::new(PayloadSanitizationReceiptParts::accepted_unmodified(
            TEST_SANITIZER_REVISION,
            call.payload.sha256.clone(),
        ))
        .expect("accepted sanitization receipt");
    call.with_sanitization(receipt)
}

fn observation_call(json: &str, payload_sha256: &str) -> ProviderCall {
    let mut request = call(NATIVE_PROVIDER_ID, ProviderOperation::Observe);
    request.payload = CanonicalPayload::new(
        OwnedVersionedId::new(OBSERVATION_CONTRACT_ID).expect("observation contract"),
        json.as_bytes().to_vec(),
        payload_sha256,
    )
    .expect("observation payload");
    // The receipt binds the payload digest, so replacing the payload requires
    // re-admitting the call rather than carrying a receipt for other bytes.
    admitted(request)
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
fn constructor_rejects_a_mutated_invalid_descriptor() {
    let mut port = MockNativePort::new(NATIVE_PROVIDER_ID, &[]);
    port.descriptor
        .capabilities
        .retain(|capability| capability.as_str() != "recall.query.v1");
    let result = NativeProvider::new(Arc::new(port));
    assert_eq!(
        result.err(),
        Some(NativeAdapterError::InvalidDescriptor(
            ApiError::MandatoryCapabilityMissing("recall.query.v1")
        ))
    );
}

#[test]
fn descriptor_generation_advances_without_changing_immutable_fields() {
    let initial = MockNativePort::new(NATIVE_PROVIDER_ID, &["feedback.record.v1"]);
    let mut advanced = initial.descriptor.clone();
    advanced.state_generation = 8;
    let port = Arc::new(MockNativePort::with_followup_descriptor(
        NATIVE_PROVIDER_ID,
        &["feedback.record.v1"],
        advanced,
    ));
    let provider = NativeProvider::new(port.clone()).expect("adapter");

    let descriptor = provider.descriptor();
    assert_eq!(descriptor.state_generation, 8);
    assert!(descriptor.supports("feedback.record.v1"));
    assert_eq!(port.counters.descriptor.load(Ordering::Relaxed), 2);

    let request = call(NATIVE_PROVIDER_ID, ProviderOperation::Feedback);
    let reply = provider.invoke(&request);
    assert_eq!(reply.terminal.terminal_code(), TerminalCode::Success);
    assert_eq!(
        port.counters.operation_calls(ProviderOperation::Feedback),
        1
    );
    assert_eq!(port.counters.descriptor.load(Ordering::Relaxed), 3);
}

#[test]
fn descriptor_immutable_drift_is_blocked_before_operation_dispatch() {
    for drift in ["capability", "identity"] {
        let initial = MockNativePort::new(NATIVE_PROVIDER_ID, &["feedback.record.v1"]);
        let mut drifted = initial.descriptor.clone();
        drifted.state_generation = 8;
        if drift == "capability" {
            drifted
                .capabilities
                .retain(|capability| capability.as_str() != "feedback.record.v1");
        } else {
            drifted.provider_id = OwnedProviderId::new("vendor.memory").expect("drifted id");
        }
        let port = Arc::new(MockNativePort::with_followup_descriptor(
            NATIVE_PROVIDER_ID,
            &["feedback.record.v1"],
            drifted,
        ));
        let provider = NativeProvider::new(port.clone()).expect("adapter");

        let descriptor = provider.descriptor();
        assert_eq!(descriptor.provider_id.as_str(), NATIVE_PROVIDER_ID);
        assert_eq!(descriptor.state_generation, 7);
        assert!(descriptor.supports("feedback.record.v1"));

        let request = call(NATIVE_PROVIDER_ID, ProviderOperation::Feedback);
        let reply = provider.invoke(&request);
        assert_eq!(
            reply.terminal.terminal_code(),
            TerminalCode::ContractViolation
        );
        assert_eq!(
            reply.terminal.diagnostic_id(),
            Some("native.descriptor_drift")
        );
        assert_eq!(
            port.counters.operation_calls(ProviderOperation::Feedback),
            0
        );
    }
}

#[test]
fn descriptor_generation_regression_is_blocked_before_operation_dispatch() {
    let initial = MockNativePort::new(NATIVE_PROVIDER_ID, &[]);
    let mut regressed = initial.descriptor.clone();
    regressed.state_generation = 6;
    let port = Arc::new(MockNativePort::with_followup_descriptor(
        NATIVE_PROVIDER_ID,
        &[],
        regressed,
    ));
    let provider = NativeProvider::new(port.clone()).expect("adapter");

    assert_eq!(provider.descriptor().state_generation, 7);
    let request = call(NATIVE_PROVIDER_ID, ProviderOperation::Health);
    let reply = provider.invoke(&request);
    assert_eq!(
        reply.terminal.terminal_code(),
        TerminalCode::ContractViolation
    );
    assert_eq!(
        reply.terminal.diagnostic_id(),
        Some("native.descriptor_drift")
    );
    assert_eq!(port.counters.health.load(Ordering::Relaxed), 0);
}

#[test]
fn descriptor_drift_remains_latched_after_the_port_recovers() {
    let initial = MockNativePort::new(NATIVE_PROVIDER_ID, &[]);
    let mut drifted = initial.descriptor.clone();
    drifted.state_schema_version = "native-state-v2".to_owned();
    let port = Arc::new(MockNativePort::with_followup_descriptor(
        NATIVE_PROVIDER_ID,
        &[],
        drifted,
    ));
    let provider = NativeProvider::new(port.clone()).expect("adapter");

    assert_eq!(
        provider.descriptor().state_schema_version,
        "native-state-v1"
    );
    *port
        .followup_descriptor
        .lock()
        .expect("followup descriptor lock") = None;
    let descriptor_calls = port.counters.descriptor.load(Ordering::Relaxed);

    let request = call(NATIVE_PROVIDER_ID, ProviderOperation::Health);
    let reply = provider.invoke(&request);
    assert_eq!(
        reply.terminal.terminal_code(),
        TerminalCode::ContractViolation
    );
    assert_eq!(
        reply.terminal.diagnostic_id(),
        Some("native.descriptor_drift")
    );
    assert_eq!(port.counters.health.load(Ordering::Relaxed), 0);
    assert_eq!(
        port.counters.descriptor.load(Ordering::Relaxed),
        descriptor_calls
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
fn invalid_handshake_envelopes_fail_before_native_contact() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let base = handshake(NATIVE_PROVIDER_ID);

    let mut zero_revision = base.clone();
    zero_revision.registration_revision = 0;
    let mut invalid_scope = base.clone();
    invalid_scope.exact_scope.profile_id.clear();
    let mut malformed_request_id = base.clone();
    malformed_request_id.request_id = "\n".to_owned();
    let mut invalid_limits = base;
    invalid_limits.host_limits.request_bytes = 0;

    for request in [
        zero_revision,
        invalid_scope,
        malformed_request_id,
        invalid_limits,
    ] {
        let response = provider.handshake(&request);
        assert_eq!(
            response.terminal.terminal_code(),
            TerminalCode::InvalidRequest
        );
        assert_eq!(
            response.terminal.diagnostic_id(),
            Some("native.handshake_request_invalid")
        );
        assert_eq!(response.terminal.provider_id().as_str(), NATIVE_PROVIDER_ID);
        assert_eq!(
            response.terminal.committed_effect().state(),
            CommittedEffectState::None
        );
        assert!(response.descriptor.is_none());
        assert!(response.provider_instance_id.is_none());
        assert!(response.state_namespace.is_none());
        assert!(response.accepted_scope.is_none());
        assert!(response.effective_limits.is_none());
        assert!(response.ready_receipt_sha256.is_none());
        assert!(response.warnings.is_empty());
    }

    let wrong_target = handshake("vendor.memory");
    let wrong_target_response = provider.handshake(&wrong_target);
    assert_eq!(
        wrong_target_response.terminal.terminal_code(),
        TerminalCode::InvalidRequest
    );
    assert_eq!(
        wrong_target_response.terminal.diagnostic_id(),
        Some("native.provider_id_mismatch")
    );
    assert_eq!(
        wrong_target_response.terminal.provider_id().as_str(),
        NATIVE_PROVIDER_ID
    );
    assert_eq!(port.counters.descriptor.load(Ordering::Relaxed), 1);
    assert_eq!(port.counters.handshake.load(Ordering::Relaxed), 0);
    assert!(
        port.last_handshake
            .lock()
            .expect("last handshake lock")
            .is_none()
    );
}

#[test]
fn mutated_operation_envelopes_fail_before_all_native_contact() {
    let port = Arc::new(MockNativePort::new(
        NATIVE_PROVIDER_ID,
        &["feedback.record.v1"],
    ));
    let provider = NativeProvider::new(port.clone()).expect("adapter");

    let mut stale_payload_digest = call(NATIVE_PROVIDER_ID, ProviderOperation::Health);
    stale_payload_digest.payload.sha256 = ZERO_SHA.to_owned();
    let mut invalid_scope = call(NATIVE_PROVIDER_ID, ProviderOperation::Recall);
    invalid_scope.exact_scope.repository_identity.clear();
    let mut missing_idempotency = call(NATIVE_PROVIDER_ID, ProviderOperation::Feedback);
    missing_idempotency.idempotency_key = None;
    let mut malformed_receipt = call(NATIVE_PROVIDER_ID, ProviderOperation::Recall);
    malformed_receipt.ready_receipt_sha256 = "invalid".to_owned();
    let mut malformed_request_id = call(NATIVE_PROVIDER_ID, ProviderOperation::Health);
    malformed_request_id.request_id = "\n".to_owned();
    let mut malformed_operation_id = call(NATIVE_PROVIDER_ID, ProviderOperation::Feedback);
    malformed_operation_id.operation_id = "\n".to_owned();

    for request in [
        stale_payload_digest,
        invalid_scope,
        missing_idempotency,
        malformed_receipt,
        malformed_request_id,
        malformed_operation_id,
    ] {
        let reply = provider.invoke(&request);
        assert_eq!(reply.terminal.terminal_code(), TerminalCode::InvalidRequest);
        assert_eq!(
            reply.terminal.diagnostic_id(),
            Some("native.provider_call_invalid")
        );
        assert_eq!(
            reply.terminal.committed_effect().state(),
            CommittedEffectState::None
        );
        assert_eq!(reply.terminal.provider_receipt_sha256(), None);
        assert_eq!(reply.payload, None);
    }
    assert_eq!(port.counters.descriptor.load(Ordering::Relaxed), 1);
    assert_eq!(port.counters.health.load(Ordering::Relaxed), 0);
    assert_eq!(port.counters.observe.load(Ordering::Relaxed), 0);
    assert_eq!(port.counters.recall.load(Ordering::Relaxed), 0);
    for operation in all_provider_operations() {
        assert_eq!(port.counters.operation_calls(operation), 0);
    }
    assert!(port.last_call.lock().expect("last call lock").is_none());
}

#[test]
fn wrong_payload_contract_for_every_invokable_operation_is_invalid_without_port_contact() {
    let optional_operations = optional_provider_operations();
    let capabilities = optional_operations
        .iter()
        .map(|(_, capability)| *capability)
        .collect::<Vec<_>>();
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &capabilities));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let descriptor_calls = port.counters.descriptor.load(Ordering::Relaxed);

    for operation in all_provider_operations()
        .into_iter()
        .filter(|operation| *operation != ProviderOperation::Handshake)
    {
        let mut request = call(NATIVE_PROVIDER_ID, operation);
        let wrong_contract_id =
            if operation_contract_id(operation) == "tracedecay.memory.provider.recall.v1" {
                "tracedecay.memory.provider.health.v1"
            } else {
                "tracedecay.memory.provider.recall.v1"
            };
        request.payload.contract_id =
            OwnedVersionedId::new(wrong_contract_id).expect("wrong payload contract");

        let reply = provider.invoke(&request);
        assert_eq!(reply.terminal.operation(), operation);
        assert_eq!(reply.terminal.terminal_code(), TerminalCode::InvalidRequest);
        assert_eq!(
            reply.terminal.diagnostic_id(),
            Some(if operation == ProviderOperation::Observe {
                "native.observation_contract_invalid"
            } else {
                "native.payload_contract_invalid"
            })
        );
        assert_eq!(
            reply.terminal.committed_effect().state(),
            CommittedEffectState::None
        );
        assert_eq!(reply.terminal.provider_receipt_sha256(), None);
        assert_eq!(reply.payload, None);
    }

    for operation in all_provider_operations() {
        assert_eq!(port.counters.operation_calls(operation), 0);
    }
    assert_eq!(
        port.counters.descriptor.load(Ordering::Relaxed),
        descriptor_calls
    );
    assert!(port.last_call.lock().expect("last call lock").is_none());
    assert!(
        port.last_handshake
            .lock()
            .expect("last handshake lock")
            .is_none()
    );
}

#[test]
fn supported_mandatory_operations_route_without_payload_transformation() {
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
fn declared_optional_operations_route_only_to_their_dedicated_ports() {
    let optional_operations = optional_provider_operations();
    let capabilities = optional_operations
        .iter()
        .map(|(_, capability)| *capability)
        .collect::<Vec<_>>();
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &capabilities));
    let provider = NativeProvider::new(port.clone()).expect("adapter");

    for (operation_index, (operation, _)) in optional_operations.into_iter().enumerate() {
        let request = call(NATIVE_PROVIDER_ID, operation);
        let reply = provider.invoke(&request);
        assert_eq!(reply.terminal.operation(), request.operation);
        assert_eq!(reply.terminal.provider_id().as_str(), NATIVE_PROVIDER_ID);
        assert_eq!(reply.terminal.terminal_code(), TerminalCode::Success);
        if operation.mutates_provider_state() {
            assert_eq!(
                reply.terminal.committed_effect().state(),
                CommittedEffectState::Committed
            );
            assert_eq!(
                reply.terminal.committed_effect().state_generation_after(),
                Some(request.expected_state_generation + 1)
            );
            assert_eq!(reply.terminal.provider_receipt_sha256(), Some(ONE_SHA));
        } else {
            assert_eq!(
                reply.terminal.committed_effect().state(),
                CommittedEffectState::None
            );
            assert_eq!(
                reply.terminal.committed_effect().state_generation_after(),
                Some(request.expected_state_generation)
            );
            assert_eq!(reply.terminal.provider_receipt_sha256(), None);
        }
        assert_eq!(
            reply.terminal.committed_effect().state_generation_before(),
            Some(request.expected_state_generation)
        );
        assert_eq!(
            reply.terminal.fallback().eligibility(),
            FallbackEligibility::Forbidden
        );

        for (counter_index, (routed_operation, _)) in
            optional_provider_operations().into_iter().enumerate()
        {
            assert_eq!(
                port.counters.operation_calls(routed_operation),
                usize::from(counter_index <= operation_index),
                "{operation:?} reached the wrong application-port method"
            );
        }
    }
}

#[test]
fn undeclared_optional_operations_are_unsupported_without_port_contact() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let descriptor_calls = port.counters.descriptor.load(Ordering::Relaxed);

    for (operation, _) in optional_provider_operations() {
        let request = call(NATIVE_PROVIDER_ID, operation);
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
        // A pre-dispatch refusal observes exactly the generation the call was
        // addressed to; the fabric refuses replies that omit that evidence.
        assert_eq!(
            reply.terminal.committed_effect().state_generation_before(),
            Some(request.expected_state_generation)
        );
        assert_eq!(
            reply.terminal.committed_effect().state_generation_after(),
            Some(request.expected_state_generation)
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

        let mut wrong_contract = request;
        wrong_contract.payload.contract_id =
            OwnedVersionedId::new("tracedecay.memory.provider.health.v1")
                .expect("wrong payload contract");
        let wrong_contract_reply = provider.invoke(&wrong_contract);
        assert_eq!(
            wrong_contract_reply.terminal.terminal_code(),
            TerminalCode::CapabilityUnsupported
        );
        assert_eq!(
            wrong_contract_reply.terminal.diagnostic_id(),
            Some("native.capability_unsupported")
        );
    }

    for operation in all_provider_operations() {
        assert_eq!(port.counters.operation_calls(operation), 0);
    }
    assert_eq!(
        port.counters.descriptor.load(Ordering::Relaxed),
        descriptor_calls
    );
    assert!(port.last_call.lock().expect("last call lock").is_none());
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
}

#[test]
fn handshake_operation_must_use_the_handshake_method() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let mut request = call(NATIVE_PROVIDER_ID, ProviderOperation::Handshake);
    request.payload.contract_id =
        OwnedVersionedId::new("tracedecay.memory.provider.recall.v1").expect("wrong contract");
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
}

#[test]
fn promoted_observation_is_typed_and_preserves_the_original_call() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let request = call(NATIVE_PROVIDER_ID, ProviderOperation::Observe);
    let reply = provider.invoke(&request);

    assert_eq!(reply.terminal.terminal_code(), TerminalCode::Success);
    assert_eq!(port.counters.observe.load(Ordering::Relaxed), 1);
    let parsed = port
        .last_observation
        .lock()
        .expect("last observation lock")
        .clone()
        .expect("typed observation");
    assert_eq!(parsed.variant, "fact_promotion");
    assert_eq!(
        parsed.observation_kind,
        NATIVE_FACT_PROMOTION_OBSERVATION_KIND
    );
    assert_eq!(
        parsed.payload_contract,
        NATIVE_FACT_PROMOTION_PAYLOAD_CONTRACT_ID
    );
    assert_eq!(
        parsed.canonical_payload,
        serde_json::json!({"fact": "promote"})
    );

    let recorded = port
        .last_call
        .lock()
        .expect("last call lock")
        .clone()
        .expect("recorded observation");
    assert_eq!(recorded.operation, request.operation);
    assert_eq!(recorded.provider_id, request.provider_id);
    assert_eq!(
        recorded.registration_revision,
        request.registration_revision
    );
    assert_eq!(recorded.ready_receipt_sha256, request.ready_receipt_sha256);
    assert_eq!(recorded.exact_scope, request.exact_scope);
    assert_eq!(recorded.request_id, request.request_id);
    assert_eq!(recorded.operation_id, request.operation_id);
    assert_eq!(
        recorded.expected_state_generation,
        request.expected_state_generation
    );
    assert_eq!(recorded.idempotency_key, request.idempotency_key);
    assert_eq!(
        recorded.control.snapshot().expect("recorded control"),
        request.control.snapshot().expect("request control")
    );
    assert_eq!(recorded.payload, request.payload);
    assert_eq!(
        recorded.required_capabilities,
        request.required_capabilities
    );
    assert_eq!(recorded.extensions, request.extensions);
}

#[test]
fn known_unaccepted_observation_kinds_are_refused_without_native_contact() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let descriptor_calls = port.counters.descriptor.load(Ordering::Relaxed);
    let cases = [
        (
            "tool.execution_settled.v1",
            "tracedecay.memory.observation.tool-execution.v1",
            "1ce3001fd5f5c006e3a9f40699c872c3d8c04128bbb59d93bdf969daf439a5e6",
        ),
        (
            "source.edit_settled.v1",
            "tracedecay.memory.observation.source-edit.v1",
            "e89eeb143ab42fbd4d1c6af64581bf4081fec37445c631abb2285ddede317fea",
        ),
        (
            "test.execution_settled.v1",
            "tracedecay.memory.observation.test-execution.v1",
            "66c7831fb471b1e0e1cf4c3023bbc4cf3c109e1cf70de78b94743df1346a020e",
        ),
        (
            "diagnostic.observed.v1",
            "tracedecay.memory.observation.diagnostic.v1",
            "53ff926555cadd5217484297e319b26e1be9e9a3de92d8590fb232598cf631a8",
        ),
        (
            "git.evidence_observed.v1",
            "tracedecay.memory.observation.git-evidence.v1",
            "ee1d9b254a58459febf76c6b2f9df45c603bcf501071f546a816f8f2e8e953d4",
        ),
        (
            "feedback.outcome_settled.v1",
            "tracedecay.memory.observation.feedback-outcome.v1",
            "ecee2a6d7d0a9cfa40c11e5a5c3fce6a6dcdc5a003f17a54cb083f0e48e39c85",
        ),
        (
            "automation.outcome_settled.v1",
            "tracedecay.memory.observation.automation-outcome.v1",
            "8dd095ed652ad8695f84a4bbac24df917d0a3a9b49b92235476716d6f5f362f7",
        ),
    ];

    for (kind, payload_contract, payload_sha256) in cases {
        let json = format!(
            "{{\"canonical_payload\":{{\"event\":\"staged\"}},\"observation_kind\":\"{kind}\",\"payload_contract\":\"{payload_contract}\"}}"
        );
        let reply = provider.invoke(&observation_call(&json, payload_sha256));
        assert_eq!(
            reply.terminal.terminal_code(),
            TerminalCode::CapabilityUnsupported
        );
        assert_eq!(
            reply.terminal.diagnostic_id(),
            Some("native.observation_unsupported")
        );
    }

    assert_eq!(port.counters.observe.load(Ordering::Relaxed), 0);
    assert_eq!(
        port.counters.descriptor.load(Ordering::Relaxed),
        descriptor_calls
    );
    assert!(port.last_call.lock().expect("last call lock").is_none());
}

/// The one host kind Native accepts reaches the port as its own typed
/// variant, with the admitted call and canonical bytes unchanged.
#[test]
fn session_message_observation_is_typed_as_staged_and_preserves_the_original_call() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let request = observation_call(SESSION_MESSAGE_OBSERVATION, SESSION_MESSAGE_OBSERVATION_SHA);
    let reply = provider.invoke(&request);

    assert_eq!(reply.terminal.terminal_code(), TerminalCode::Success);
    assert_eq!(port.counters.observe.load(Ordering::Relaxed), 1);
    let parsed = port
        .last_observation
        .lock()
        .expect("last observation lock")
        .clone()
        .expect("typed observation");
    assert_eq!(
        parsed,
        ObservedVariant {
            variant: "staged_session",
            observation_kind: NATIVE_STAGED_SESSION_OBSERVATION_KIND.to_owned(),
            payload_contract: NATIVE_STAGED_SESSION_PAYLOAD_CONTRACT_ID.to_owned(),
            canonical_payload: serde_json::json!({"event": "staged"}),
        }
    );

    // The port receives the admitted call untouched: same sanitized bytes,
    // same exact scope, same idempotency and operation identity. Staging must
    // store what admission sanitized, not a re-derived payload.
    let recorded = port
        .last_call
        .lock()
        .expect("last call lock")
        .clone()
        .expect("recorded observation");
    assert_eq!(recorded.payload, request.payload);
    assert_eq!(
        recorded.payload.bytes,
        SESSION_MESSAGE_OBSERVATION.as_bytes()
    );
    assert_eq!(recorded.exact_scope, request.exact_scope);
    assert_eq!(recorded.idempotency_key, request.idempotency_key);
    assert_eq!(recorded.operation_id, request.operation_id);
    assert_eq!(recorded.request_id, request.request_id);
    assert_eq!(
        recorded.registration_revision,
        request.registration_revision
    );
    assert_eq!(recorded.extensions, request.extensions);
}

/// Accepting the session kind does not loosen its contract pairing: the kind
/// is accepted only with the payload contract the observation contract
/// declares for it.
#[test]
fn session_message_kind_with_a_foreign_payload_contract_is_refused_before_native_contact() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let reply = provider.invoke(&observation_call(
        SESSION_KIND_WRONG_CONTRACT,
        SESSION_KIND_WRONG_CONTRACT_SHA,
    ));

    assert_eq!(reply.terminal.terminal_code(), TerminalCode::InvalidRequest);
    assert_eq!(
        reply.terminal.diagnostic_id(),
        Some("native.observation_kind_contract_mismatch")
    );
    assert_eq!(port.counters.observe.load(Ordering::Relaxed), 0);
    assert!(port.last_observation.lock().expect("lock").is_none());
}

#[test]
fn unknown_mismatched_and_malformed_observations_fail_before_native_contact() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let descriptor_calls = port.counters.descriptor.load(Ordering::Relaxed);
    let cases = [
        (
            "not-json",
            "0c21a879c732a67910d80988df4919d794f6a070aab610ef865032a28046b021",
            TerminalCode::InvalidRequest,
            "native.observation_envelope_invalid",
        ),
        (
            "{\"canonical_payload\":{\"event\":\"unknown\"},\"observation_kind\":\"vendor.future.v1\",\"payload_contract\":\"vendor.future-payload.v1\"}",
            "a6c8e823e4920e40530c7fa0c3626c85d4b642f43a12268e425f967dbf82982c",
            TerminalCode::InvalidRequest,
            "native.observation_kind_unknown",
        ),
        (
            "{\"canonical_payload\":{\"event\":\"mismatch\"},\"observation_kind\":\"native.fact_promoted.v1\",\"payload_contract\":\"tracedecay.memory.observation.session-message.v1\"}",
            "e987fbc09093731507ba0f0a7a3c51718ad163687fbe06fc50576f7de52527f4",
            TerminalCode::InvalidRequest,
            "native.observation_kind_contract_mismatch",
        ),
        (
            "{\"observation_kind\":\"native.fact_promoted.v1\",\"payload_contract\":\"tracedecay.memory.observation.native-fact-promotion.v1\"}",
            "286469241bc8446189bdf53846a8b618e9392b8e6e6a0dd5c2d149e58ae4d8f9",
            TerminalCode::InvalidRequest,
            "native.observation_envelope_invalid",
        ),
    ];

    for (json, payload_sha256, terminal_code, diagnostic_id) in cases {
        let reply = provider.invoke(&observation_call(json, payload_sha256));
        assert_eq!(reply.terminal.terminal_code(), terminal_code);
        assert_eq!(reply.terminal.diagnostic_id(), Some(diagnostic_id));
    }

    assert_eq!(port.counters.observe.load(Ordering::Relaxed), 0);
    assert_eq!(
        port.counters.descriptor.load(Ordering::Relaxed),
        descriptor_calls
    );
    assert!(port.last_call.lock().expect("last call lock").is_none());
}

#[test]
fn fact_shaped_generic_observation_is_rejected_by_native_authority() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let descriptor_calls = port.counters.descriptor.load(Ordering::Relaxed);
    let mut request = call(NATIVE_PROVIDER_ID, ProviderOperation::Observe);
    request.payload = CanonicalPayload::new(
        OwnedVersionedId::new(OBSERVATION_CONTRACT_ID).expect("observation contract"),
        FACT_SHAPED_OBSERVATION.to_vec(),
        FACT_SHAPED_OBSERVATION_SHA,
    )
    .expect("fact-shaped observation");
    // Re-admit: the receipt binds the payload digest, so a replaced payload
    // needs a receipt for the bytes actually dispatched.
    let request = admitted(request);
    let reply = provider.invoke(&request);
    assert_eq!(reply.terminal.terminal_code(), TerminalCode::InvalidRequest);
    assert_eq!(
        reply.terminal.diagnostic_id(),
        Some("native.observation_envelope_invalid")
    );
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
    assert_eq!(reply.payload, None);
    assert_eq!(reply.state_generation, request.expected_state_generation);
    assert_eq!(
        port.counters.descriptor.load(Ordering::Relaxed),
        descriptor_calls
    );
    assert_eq!(port.counters.observe.load(Ordering::Relaxed), 0);
    assert!(port.last_call.lock().expect("last call lock").is_none());
}

#[test]
fn invalid_generic_observations_fail_before_native_contact() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let base = call(NATIVE_PROVIDER_ID, ProviderOperation::Observe);

    let mut wrong_contract = base.clone();
    wrong_contract.payload.contract_id =
        OwnedVersionedId::new("tracedecay.memory.provider.recall.v1").expect("recall contract");
    let wrong_contract_reply = provider.invoke(&wrong_contract);
    assert_eq!(
        wrong_contract_reply.terminal.terminal_code(),
        TerminalCode::InvalidRequest
    );

    let mut stale_digest = base.clone();
    stale_digest.payload.sha256 = ZERO_SHA.to_owned();
    let stale_digest_reply = provider.invoke(&stale_digest);
    assert_eq!(
        stale_digest_reply.terminal.terminal_code(),
        TerminalCode::InvalidRequest
    );

    let mut missing_idempotency = base;
    missing_idempotency.idempotency_key = None;
    let missing_idempotency_reply = provider.invoke(&missing_idempotency);
    assert_eq!(
        missing_idempotency_reply.terminal.terminal_code(),
        TerminalCode::InvalidRequest
    );

    assert_eq!(
        wrong_contract_reply.terminal.diagnostic_id(),
        Some("native.observation_contract_invalid")
    );
    for reply in [&stale_digest_reply, &missing_idempotency_reply] {
        assert_eq!(
            reply.terminal.diagnostic_id(),
            Some("native.provider_call_invalid")
        );
    }

    for reply in [
        wrong_contract_reply,
        stale_digest_reply,
        missing_idempotency_reply,
    ] {
        assert_eq!(
            reply.terminal.committed_effect().state(),
            CommittedEffectState::None
        );
        assert_eq!(reply.terminal.provider_receipt_sha256(), None);
        assert_eq!(reply.payload, None);
    }
    assert_eq!(port.counters.observe.load(Ordering::Relaxed), 0);
    assert_eq!(port.counters.descriptor.load(Ordering::Relaxed), 1);
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
