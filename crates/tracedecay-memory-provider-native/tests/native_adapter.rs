//! Integration journeys for the Native provider adapter boundary.
//!
//! Native maps exactly two capabilities, health and recall, onto upstream
//! TraceDecay authorities. Every other operation, observation included, is
//! refused before the application port is contacted.
#![allow(clippy::expect_used)]

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};

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
    NATIVE_PROVIDER_CAPABILITY_IDS, NATIVE_PROVIDER_ID, NativeAdapterError,
    NativeMemoryApplicationPort, NativeProvider,
};

/// Provider-neutral observation contract. Native declares no observation
/// capability; the contract is used only to prove that refusal.
const OBSERVATION_CONTRACT_ID: &str = "tracedecay.memory.provider.observation.v1";

const ZERO_SHA: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const ONE_SHA: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const RESOLVED_SCOPE_DIGEST: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const FIXTURE_SHA: &str = "ffbc2dfc402782325da71132100e74ff511d1585dd80e4ea196ed4bcace3fef2";

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
    descriptor_control: Mutex<Option<CancellationToken>>,
    health_gate: Mutex<Option<(Sender<()>, Receiver<()>)>>,
    handshake_override: Mutex<Option<HandshakeResponse>>,
    reply_override: Mutex<Option<ProviderReply>>,
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
            followup_descriptor: Mutex::new(None),
            descriptor_control: Mutex::new(None),
            health_gate: Mutex::new(None),
            handshake_override: Mutex::new(None),
            reply_override: Mutex::new(None),
            counters: Counters::default(),
            last_call: Mutex::new(None),
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
        let payload = if matches!(
            code,
            TerminalCode::Success | TerminalCode::SuccessZeroResults | TerminalCode::Partial
        ) {
            Some(
                CanonicalPayload::new(
                    OwnedVersionedId::new(result_contract_id(call.operation))
                        .expect("result contract"),
                    call.payload.bytes.clone(),
                    call.payload.sha256.clone(),
                )
                .expect("result payload"),
            )
        } else {
            None
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
            payload,
            warnings: Vec::new(),
            extensions: call.extensions.clone(),
            state_generation,
        }
    }

    fn record(&self, call: &ProviderCall) {
        *self.last_call.lock().expect("last call lock") = Some(call.clone());
    }

    fn reply(&self, call: &ProviderCall, code: TerminalCode) -> ProviderReply {
        let override_reply = self
            .reply_override
            .lock()
            .expect("reply override lock")
            .take();
        override_reply.unwrap_or_else(|| self.terminal(call, code))
    }

    fn default_handshake_response(&self, request: &HandshakeRequest) -> HandshakeResponse {
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
}

impl NativeMemoryApplicationPort for MockNativePort {
    fn descriptor(&self) -> ProviderDescriptor {
        let call_index = self.counters.descriptor.fetch_add(1, Ordering::Relaxed);
        if let Some(control) = self
            .descriptor_control
            .lock()
            .expect("descriptor control lock")
            .take()
        {
            control.cancel();
        }
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
        let override_response = self
            .handshake_override
            .lock()
            .expect("handshake override lock")
            .take();
        override_response.unwrap_or_else(|| self.default_handshake_response(request))
    }

    fn health(&self, call: &ProviderCall) -> ProviderReply {
        self.counters.health.fetch_add(1, Ordering::Relaxed);
        self.record(call);
        if let Some((entered, release)) = self.health_gate.lock().expect("health gate lock").take()
        {
            entered.send(()).expect("health entered receiver");
            release.recv().expect("health release sender");
        }
        self.reply(call, TerminalCode::Success)
    }

    fn recall(&self, call: &ProviderCall) -> ProviderReply {
        self.counters.recall.fetch_add(1, Ordering::Relaxed);
        self.record(call);
        self.reply(call, TerminalCode::Success)
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

fn result_contract_id(operation: ProviderOperation) -> &'static str {
    match operation {
        ProviderOperation::Handshake => "tracedecay.memory.provider.handshake.v1",
        ProviderOperation::Health => "tracedecay.memory.provider.health.v1",
        ProviderOperation::Observe => OBSERVATION_CONTRACT_ID,
        ProviderOperation::Recall => "tracedecay.memory.recall.query.outcome.v1",
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
    let (payload_bytes, payload_sha256) = (b"{\"fixture\":true}".to_vec(), FIXTURE_SHA);
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

fn handshake(provider_id: &str) -> HandshakeRequest {
    HandshakeRequest::new(HandshakeRequestParts {
        provider_id: OwnedProviderId::new(provider_id).expect("provider id"),
        registration_revision: 1,
        exact_scope: scope(),
        request_id: "handshake-a".to_owned(),
        required_capabilities: vec![
            OwnedVersionedId::new("provider.health.v1").expect("health"),
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
    assert!(!descriptor.supports("feedback.record.v1"));
    assert_eq!(port.counters.descriptor.load(Ordering::Relaxed), 2);

    let mut request = call(NATIVE_PROVIDER_ID, ProviderOperation::Health);
    request.expected_state_generation = 8;
    let reply = provider.invoke(&request);
    assert_eq!(reply.terminal.terminal_code(), TerminalCode::Success);
    assert_eq!(port.counters.operation_calls(ProviderOperation::Health), 1);
    assert_eq!(port.counters.descriptor.load(Ordering::Relaxed), 3);
}

#[test]
fn stale_and_future_generations_are_refused_after_descriptor_refresh() {
    for expected_generation in [6, 8] {
        let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
        let provider = NativeProvider::new(port.clone()).expect("adapter");
        let mut request = call(NATIVE_PROVIDER_ID, ProviderOperation::Health);
        request.expected_state_generation = expected_generation;
        let descriptor_calls = port.counters.descriptor.load(Ordering::Relaxed);

        let reply = provider.invoke(&request);

        assert_eq!(reply.terminal.terminal_code(), TerminalCode::StaleIdentity);
        assert_eq!(
            reply.terminal.diagnostic_id(),
            Some("native.state_generation_mismatch")
        );
        assert_eq!(
            reply.terminal.committed_effect().state_generation_before(),
            Some(expected_generation)
        );
        assert_eq!(port.counters.health.load(Ordering::Relaxed), 0);
        assert_eq!(
            port.counters.descriptor.load(Ordering::Relaxed),
            descriptor_calls + 1
        );
    }
}

#[test]
fn cancellation_racing_descriptor_contact_blocks_operation_dispatch() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let cancellation = CancellationToken::new();
    let mut request = call(NATIVE_PROVIDER_ID, ProviderOperation::Health);
    request.control = OperationControl::new(i64::MAX, 500, cancellation.clone());
    *port
        .descriptor_control
        .lock()
        .expect("descriptor control lock") = Some(cancellation);
    let descriptor_calls = port.counters.descriptor.load(Ordering::Relaxed);

    let reply = provider.invoke(&request);

    assert_eq!(reply.terminal.terminal_code(), TerminalCode::Cancelled);
    assert_eq!(
        reply.terminal.diagnostic_id(),
        Some("native.request_control_terminal")
    );
    assert_eq!(
        port.counters.descriptor.load(Ordering::Relaxed),
        descriptor_calls + 1
    );
    assert_eq!(port.counters.health.load(Ordering::Relaxed), 0);
}

#[test]
fn concurrent_stale_call_never_reaches_application_port() {
    let initial = MockNativePort::new(NATIVE_PROVIDER_ID, &[]);
    let mut advanced = initial.descriptor.clone();
    advanced.state_generation = 8;
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = Arc::new(NativeProvider::new(port.clone()).expect("adapter"));
    let (health_entered_tx, health_entered_rx) = mpsc::channel();
    let (health_release_tx, health_release_rx) = mpsc::channel();
    *port.health_gate.lock().expect("health gate lock") =
        Some((health_entered_tx, health_release_rx));

    let first = call(NATIVE_PROVIDER_ID, ProviderOperation::Health);
    let mut second = call(NATIVE_PROVIDER_ID, ProviderOperation::Health);
    second.operation_id = "operation-b".to_owned();
    let first_provider = Arc::clone(&provider);
    let first_thread = std::thread::spawn(move || first_provider.invoke(&first));
    health_entered_rx.recv().expect("first health contact");
    *port
        .followup_descriptor
        .lock()
        .expect("followup descriptor lock") = Some(advanced);

    let (descriptor_returned_tx, descriptor_returned_rx) = mpsc::channel();
    let second_provider = Arc::clone(&provider);
    let second_thread = std::thread::spawn(move || {
        let projected = second_provider.descriptor();
        descriptor_returned_tx
            .send(projected.state_generation)
            .expect("descriptor observer");
        second_provider.invoke(&second)
    });

    // The descriptor refresh from the concurrent caller must not contact the
    // port while the first caller is inside its application operation. It
    // receives the in-flight generation snapshot and waits for the provider
    // gate before attempting its own refresh.
    assert_eq!(
        descriptor_returned_rx
            .recv()
            .expect("concurrent descriptor"),
        7
    );
    health_release_tx.send(()).expect("release first health");

    let first_reply = first_thread.join().expect("first operation");
    let second_reply = second_thread.join().expect("second operation");
    assert_eq!(first_reply.terminal.terminal_code(), TerminalCode::Success);
    assert_eq!(
        second_reply.terminal.terminal_code(),
        TerminalCode::StaleIdentity
    );
    assert_eq!(
        second_reply.terminal.diagnostic_id(),
        Some("native.state_generation_mismatch")
    );
    assert_eq!(port.counters.health.load(Ordering::Relaxed), 1);
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
                .retain(|capability| capability.as_str() != "recall.query.v1");
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
        assert!(!descriptor.supports("feedback.record.v1"));

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
        assert_eq!(port.counters.operation_calls(ProviderOperation::Health), 0);
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
    let descriptor = provider.descriptor();
    assert_eq!(descriptor.provider_id.as_str(), NATIVE_PROVIDER_ID);
    assert_eq!(descriptor.implementation_identity_sha256, ZERO_SHA);
    assert!(descriptor.supports("provider.health.v1"));
    assert_eq!(
        descriptor
            .capabilities
            .iter()
            .map(|value| value.as_str())
            .collect::<BTreeSet<_>>(),
        NATIVE_PROVIDER_CAPABILITY_IDS.iter().copied().collect()
    );
}

#[test]
fn handshake_preserves_exact_scope_and_request_identity() {
    let optional_capabilities = optional_provider_operations()
        .iter()
        .map(|(_, capability)| *capability)
        .collect::<Vec<_>>();
    let port = Arc::new(MockNativePort::new(
        NATIVE_PROVIDER_ID,
        &optional_capabilities,
    ));
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
    let response_descriptor = response.descriptor.expect("projected descriptor");
    let response_capabilities = response_descriptor
        .capabilities
        .iter()
        .map(|value| value.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        response_capabilities,
        NATIVE_PROVIDER_CAPABILITY_IDS.iter().copied().collect()
    );
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
fn handshake_rejects_unknown_and_undeclared_required_capabilities_before_port_contact() {
    for (required_capability, declared_capabilities) in [
        ("feedback.record.v1", &[][..]),
        (
            "vendor.future-capability.v7",
            &["vendor.future-capability.v7"][..],
        ),
    ] {
        let port = Arc::new(MockNativePort::new(
            NATIVE_PROVIDER_ID,
            declared_capabilities,
        ));
        let provider = NativeProvider::new(port.clone()).expect("adapter");
        let mut request = handshake(NATIVE_PROVIDER_ID);
        request
            .required_capabilities
            .insert(OwnedVersionedId::new(required_capability).expect("required capability"));
        let descriptor_calls = port.counters.descriptor.load(Ordering::Relaxed);

        let response = provider.handshake(&request);

        assert_eq!(
            response.terminal.terminal_code(),
            TerminalCode::CapabilityUnsupported
        );
        assert_eq!(
            response.terminal.diagnostic_id(),
            Some("native.required_capability_missing")
        );
        assert_eq!(
            response.terminal.committed_effect().state(),
            CommittedEffectState::None
        );
        assert_eq!(
            port.counters.descriptor.load(Ordering::Relaxed),
            descriptor_calls
        );
        assert_eq!(port.counters.handshake.load(Ordering::Relaxed), 0);
        assert_eq!(port.counters.operation_calls(ProviderOperation::Health), 0);
        assert_eq!(port.descriptor.state_generation, 7);
        assert!(response.descriptor.is_none());
        assert!(
            port.last_handshake
                .lock()
                .expect("handshake lock")
                .is_none()
        );
    }
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
fn cancelled_or_expired_handshake_is_refused_before_descriptor_and_port_contact() {
    let cases = [
        (TerminalCode::Cancelled, {
            let cancellation = CancellationToken::new();
            cancellation.cancel();
            OperationControl::new(i64::MAX, 500, cancellation)
        }),
        (
            TerminalCode::DeadlineExceeded,
            OperationControl::new(0, 500, CancellationToken::new()),
        ),
    ];

    for (expected_code, control) in cases {
        let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
        let provider = NativeProvider::new(port.clone()).expect("adapter");
        let mut request = handshake(NATIVE_PROVIDER_ID);
        request.control = control;
        let descriptor_calls = port.counters.descriptor.load(Ordering::Relaxed);

        let response = provider.handshake(&request);

        assert_eq!(response.terminal.terminal_code(), expected_code);
        assert_eq!(
            response.terminal.diagnostic_id(),
            Some("native.handshake_request_control_terminal")
        );
        assert!(response.descriptor.is_none());
        assert_eq!(
            port.counters.descriptor.load(Ordering::Relaxed),
            descriptor_calls
        );
        assert_eq!(port.counters.handshake.load(Ordering::Relaxed), 0);
    }
}

#[test]
fn cancelled_or_expired_operation_is_refused_before_descriptor_and_port_contact() {
    let cases = [
        (TerminalCode::Cancelled, {
            let cancellation = CancellationToken::new();
            cancellation.cancel();
            OperationControl::new(i64::MAX, 500, cancellation)
        }),
        (
            TerminalCode::DeadlineExceeded,
            OperationControl::new(0, 500, CancellationToken::new()),
        ),
    ];

    for (expected_code, control) in cases {
        let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
        let provider = NativeProvider::new(port.clone()).expect("adapter");
        let mut request = call(NATIVE_PROVIDER_ID, ProviderOperation::Health);
        request.control = control;
        let descriptor_calls = port.counters.descriptor.load(Ordering::Relaxed);

        let reply = provider.invoke(&request);

        assert_eq!(reply.terminal.terminal_code(), expected_code);
        assert_eq!(
            reply.terminal.diagnostic_id(),
            Some("native.request_control_terminal")
        );
        assert_eq!(reply.payload, None);
        assert_eq!(reply.state_generation, request.expected_state_generation);
        assert_eq!(
            port.counters.descriptor.load(Ordering::Relaxed),
            descriptor_calls
        );
        assert_eq!(port.counters.health.load(Ordering::Relaxed), 0);
    }
}

#[test]
fn malformed_handshake_reply_is_converted_to_contract_violation() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let request = handshake(NATIVE_PROVIDER_ID);
    let mut malformed = port.default_handshake_response(&request);
    let mut foreign_scope = scope();
    foreign_scope.project_id = "project-b".to_owned();
    malformed.accepted_scope = Some(foreign_scope);
    *port
        .handshake_override
        .lock()
        .expect("handshake override lock") = Some(malformed);

    let response = provider.handshake(&request);

    assert_eq!(
        response.terminal.terminal_code(),
        TerminalCode::ContractViolation
    );
    assert_eq!(
        response.terminal.diagnostic_id(),
        Some("native.handshake_response_contract_violation")
    );
    assert!(response.descriptor.is_none());
    assert!(response.provider_instance_id.is_none());
    assert_eq!(port.counters.handshake.load(Ordering::Relaxed), 1);
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
    let capabilities = optional_provider_operations()
        .iter()
        .map(|(_, capability)| *capability)
        .collect::<Vec<_>>();
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &capabilities));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let descriptor_calls = port.counters.descriptor.load(Ordering::Relaxed);

    for operation in [ProviderOperation::Health, ProviderOperation::Recall] {
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
            Some("native.payload_contract_invalid")
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
    for operation in [ProviderOperation::Health, ProviderOperation::Recall] {
        let request = call(NATIVE_PROVIDER_ID, operation);
        let expected_payload = request.payload.clone();
        let reply = provider.invoke(&request);
        assert_eq!(reply.terminal.operation(), operation);
        assert_eq!(reply.terminal.provider_id().as_str(), NATIVE_PROVIDER_ID);
        assert_eq!(reply.terminal.terminal_code(), TerminalCode::Success);
        let result_payload = reply.payload.as_ref().expect("result payload");
        assert_eq!(result_payload.bytes, expected_payload.bytes);
        assert_eq!(result_payload.sha256, expected_payload.sha256);
        assert_eq!(
            result_payload.contract_id.as_str(),
            result_contract_id(operation)
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
    assert_eq!(port.counters.recall.load(Ordering::Relaxed), 1);
}

#[test]
fn malformed_application_result_contract_is_converted_to_contract_violation() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let request = call(NATIVE_PROVIDER_ID, ProviderOperation::Recall);
    let mut malformed = port.terminal(&request, TerminalCode::Success);
    malformed
        .payload
        .as_mut()
        .expect("result payload")
        .contract_id = OwnedVersionedId::new("tracedecay.memory.provider.health.v1")
        .expect("foreign result contract");
    *port.reply_override.lock().expect("reply override lock") = Some(malformed);

    let reply = provider.invoke(&request);

    assert_eq!(
        reply.terminal.terminal_code(),
        TerminalCode::ContractViolation
    );
    assert_eq!(
        reply.terminal.diagnostic_id(),
        Some("native.application_reply_contract_violation")
    );
    assert_eq!(reply.payload, None);
    assert_eq!(reply.state_generation, request.expected_state_generation);
    assert_eq!(port.counters.recall.load(Ordering::Relaxed), 1);
}

#[test]
fn malformed_application_reply_digest_is_converted_to_contract_violation() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let request = call(NATIVE_PROVIDER_ID, ProviderOperation::Health);
    let mut malformed = port.terminal(&request, TerminalCode::Success);
    malformed.payload.as_mut().expect("result payload").sha256 = ZERO_SHA.to_owned();
    *port.reply_override.lock().expect("reply override lock") = Some(malformed);

    let reply = provider.invoke(&request);

    assert_eq!(
        reply.terminal.terminal_code(),
        TerminalCode::ContractViolation
    );
    assert_eq!(reply.payload, None);
    assert_eq!(port.counters.health.load(Ordering::Relaxed), 1);
}

#[test]
fn a_dispatched_read_claiming_any_effect_is_a_contract_violation() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    let request = call(NATIVE_PROVIDER_ID, ProviderOperation::Recall);
    let committed = CommittedEffectEvidence::committed(
        request.expected_state_generation,
        request.expected_state_generation.saturating_add(1),
        vec![request.operation_id.clone()],
        ONE_SHA,
        ONE_SHA,
    )
    .expect("committed effect evidence");
    let mut reply = port.terminal(&request, TerminalCode::Success);
    reply.terminal = TerminalRecord::new(
        request.operation,
        OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("provider id"),
        TerminalCode::Success,
        committed,
        FallbackDirective::forbidden(),
        request.operation_id.clone(),
        request.exact_scope.exact_scope_sha256(),
        None,
    )
    .expect("effect-claiming terminal");
    reply.state_generation = request.expected_state_generation.saturating_add(1);
    *port.reply_override.lock().expect("reply override lock") = Some(reply);

    let reply = provider.invoke(&request);

    assert_eq!(
        reply.terminal.terminal_code(),
        TerminalCode::ContractViolation
    );
    assert_eq!(
        reply.terminal.diagnostic_id(),
        Some("native.application_reply_contract_violation")
    );
    assert_eq!(port.counters.recall.load(Ordering::Relaxed), 1);
}

#[test]
fn observation_is_never_a_native_capability() {
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &[]));
    assert!(
        port.descriptor.supports("observation.accept.v1"),
        "the port fixture declares observation so the projection is what refuses it"
    );
    let provider = NativeProvider::new(port.clone()).expect("adapter");
    assert!(!provider.descriptor().supports("observation.accept.v1"));
    let request = call(NATIVE_PROVIDER_ID, ProviderOperation::Observe);

    let reply = provider.invoke(&request);

    assert_eq!(
        reply.terminal.terminal_code(),
        TerminalCode::CapabilityUnsupported
    );
    assert_eq!(
        reply.terminal.diagnostic_id(),
        Some("native.capability_unsupported")
    );
    assert_eq!(
        reply.terminal.committed_effect().state(),
        CommittedEffectState::None
    );
    for operation in all_provider_operations() {
        assert_eq!(port.counters.operation_calls(operation), 0);
    }
    assert!(port.last_call.lock().expect("last call lock").is_none());
}

#[test]
fn port_declared_optional_operations_remain_hidden_without_lossless_mapping() {
    let optional_operations = optional_provider_operations();
    let capabilities = optional_operations
        .iter()
        .map(|(_, capability)| *capability)
        .collect::<Vec<_>>();
    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, &capabilities));
    let provider = NativeProvider::new(port.clone()).expect("adapter");

    for (operation, capability) in optional_operations {
        let request = call(NATIVE_PROVIDER_ID, operation);
        let reply = provider.invoke(&request);
        assert_eq!(reply.terminal.operation(), request.operation);
        assert_eq!(reply.terminal.provider_id().as_str(), NATIVE_PROVIDER_ID);
        assert_eq!(
            reply.terminal.terminal_code(),
            TerminalCode::CapabilityUnsupported
        );
        assert!(!provider.descriptor().supports(capability));
        assert_eq!(
            reply.terminal.committed_effect().state_generation_before(),
            Some(request.expected_state_generation)
        );
        assert_eq!(
            reply.terminal.fallback().eligibility(),
            FallbackEligibility::Forbidden
        );
        assert_eq!(
            reply.terminal.diagnostic_id(),
            Some("native.capability_unsupported")
        );
    }

    for operation in optional_provider_operations().map(|(operation, _)| operation) {
        assert_eq!(port.counters.operation_calls(operation), 0);
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
fn invoke_rejects_unknown_and_undeclared_required_capabilities_before_port_contact() {
    for (required_capability, declared_capabilities) in [
        ("feedback.record.v1", &[][..]),
        (
            "vendor.future-capability.v7",
            &["vendor.future-capability.v7"][..],
        ),
    ] {
        let port = Arc::new(MockNativePort::new(
            NATIVE_PROVIDER_ID,
            declared_capabilities,
        ));
        let provider = NativeProvider::new(port.clone()).expect("adapter");
        let mut request = call(NATIVE_PROVIDER_ID, ProviderOperation::Recall);
        request
            .required_capabilities
            .insert(OwnedVersionedId::new(required_capability).expect("required capability"));
        let descriptor_calls = port.counters.descriptor.load(Ordering::Relaxed);

        let reply = provider.invoke(&request);

        assert_eq!(
            reply.terminal.terminal_code(),
            TerminalCode::CapabilityUnsupported
        );
        assert_eq!(
            reply.terminal.diagnostic_id(),
            Some("native.required_capability_missing")
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
        assert_eq!(reply.state_generation, request.expected_state_generation);
        assert_eq!(
            port.counters.descriptor.load(Ordering::Relaxed),
            descriptor_calls
        );
        for operation in all_provider_operations() {
            assert_eq!(port.counters.operation_calls(operation), 0);
        }
        assert!(port.last_call.lock().expect("last call lock").is_none());
    }
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
    assert_eq!(
        capabilities,
        NATIVE_PROVIDER_CAPABILITY_IDS.iter().copied().collect()
    );
}
