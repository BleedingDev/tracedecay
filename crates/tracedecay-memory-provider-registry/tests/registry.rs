//! Focused composition tests for the product-owned provider registry.
#![allow(clippy::expect_used, clippy::panic)]

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tracedecay_memory_provider_api::contract::{
    CommittedEffectState, FallbackEligibility, TerminalCode,
};
use tracedecay_memory_provider_api::{
    CancellationToken, CanonicalPayload, CommittedEffectEvidence, FallbackDirective,
    HandshakeRequest, HandshakeRequestParts, HandshakeResponse, OperationControl, OwnedExactScope,
    OwnedOpaqueExtension, OwnedProviderId, OwnedVersionedId, PayloadSanitizationReceipt,
    PayloadSanitizationReceiptParts, PinnedFallbackPolicy, ProviderCall, ProviderCallParts,
    ProviderDescriptor, ProviderLimits, ProviderOperation, ProviderReply, TerminalRecord,
    observation_extensions_digest,
};
use tracedecay_memory_provider_native::{
    NATIVE_PROVIDER_ID, NativeAdapterError, NativeMemoryApplicationPort, NativeObservation,
};
use tracedecay_memory_provider_registry::{
    EnabledProviderMode, FabricConfig, FabricError, NativeProviderActivation, ObserverReceipt,
    ProjectMemoryProviderComposition, ProviderCapabilityAvailability, ProviderMode,
    ProviderReadiness, ReadinessTargetError, RegistryError,
};

const ZERO_SHA: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const ONE_SHA: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const TWO_SHA: &str = "2222222222222222222222222222222222222222222222222222222222222222";
const REGISTRY_PAYLOAD_SHA: &str =
    "2bc217171d7030f82de2853ea3e4914b803d3504a3409433d62bf426a613d7ee";
const OPAQUE_PAYLOAD_SHA: &str = "a4ebe309c7d7eaf1b08aec54feea5668a4b10a564770d162dbd7a131990d0de8";
const OBSERVATION_PAYLOAD_SHA: &str =
    "b6c0cb54c14eb8485ba9f86925c370bbcb8e464687f875b7fa1085a1d1b9b1fe";
const RESOLVED_SCOPE_DIGEST: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const OBSERVATION_PAYLOAD: &[u8] = br#"{"canonical_payload":{"kind":"settled_native_fact_write","fact":{"fixture":true},"commit":{"fixture":true}},"observation_kind":"native.fact_promoted.v1","payload_contract":"tracedecay.memory.observation.native-fact-promotion.v1"}"#;

struct MockNativePort {
    descriptor: ProviderDescriptor,
    descriptor_calls: AtomicUsize,
}

impl MockNativePort {
    fn new(provider_id: &str, limits: ProviderLimits) -> Self {
        Self {
            descriptor: descriptor(provider_id, limits),
            descriptor_calls: AtomicUsize::new(0),
        }
    }
}

impl NativeMemoryApplicationPort for MockNativePort {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor_calls.fetch_add(1, Ordering::Relaxed);
        self.descriptor.clone()
    }

    fn handshake(&self, _request: &HandshakeRequest) -> HandshakeResponse {
        unexpected_provider_contact()
    }

    fn health(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected_provider_contact()
    }

    fn observe(&self, _observation: NativeObservation<'_>) -> ProviderReply {
        unexpected_provider_contact()
    }

    fn recall(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected_provider_contact()
    }

    fn feedback(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected_provider_contact()
    }

    fn maintenance(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected_provider_contact()
    }

    fn inspection(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected_provider_contact()
    }

    fn correction(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected_provider_contact()
    }

    fn delete_by_source(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected_provider_contact()
    }

    fn snapshot_export(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected_provider_contact()
    }

    fn snapshot_restore(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected_provider_contact()
    }

    fn replay(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected_provider_contact()
    }
}

fn unexpected_provider_contact<T>() -> T {
    panic!("composition tests must not execute provider operations")
}

struct EvidenceNativePort {
    descriptor: ProviderDescriptor,
    handshake_terminal_operation: ProviderOperation,
    handshake_terminal_provider_id: OwnedProviderId,
    handshake_accepted_scope: Option<OwnedExactScope>,
    handshake_calls: AtomicUsize,
    health_calls: AtomicUsize,
    observe_calls: AtomicUsize,
}

impl EvidenceNativePort {
    fn new() -> Self {
        Self {
            descriptor: descriptor(NATIVE_PROVIDER_ID, limits()),
            handshake_terminal_operation: ProviderOperation::Handshake,
            handshake_terminal_provider_id: OwnedProviderId::new(NATIVE_PROVIDER_ID)
                .expect("native provider"),
            handshake_accepted_scope: None,
            handshake_calls: AtomicUsize::new(0),
            health_calls: AtomicUsize::new(0),
            observe_calls: AtomicUsize::new(0),
        }
    }

    fn with_handshake_terminal(mut self, operation: ProviderOperation, provider_id: &str) -> Self {
        self.handshake_terminal_operation = operation;
        self.handshake_terminal_provider_id =
            OwnedProviderId::new(provider_id).expect("terminal provider");
        self
    }

    /// Makes the mock lie about the accepted exact scope, standing in for a
    /// provider that echoes a foreign or stale coding-scope identity instead
    /// of the one the request actually carried.
    fn with_handshake_accepted_scope(mut self, scope: OwnedExactScope) -> Self {
        self.handshake_accepted_scope = Some(scope);
        self
    }

    fn unavailable_reply(&self, call: &ProviderCall) -> ProviderReply {
        let current_provider = OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("native provider");
        let policy = PinnedFallbackPolicy::new(
            "memory.fallback.policy",
            7,
            OwnedProviderId::new("vendor.backup").expect("fallback provider"),
        )
        .expect("fallback policy");
        let fallback = FallbackDirective::explicit_policy_only(
            &current_provider,
            policy,
            "explicit host policy may select the pinned provider",
        )
        .expect("fallback directive");
        ProviderReply {
            terminal: TerminalRecord::new(
                call.operation,
                call.provider_id.clone(),
                TerminalCode::ProviderUnavailable,
                CommittedEffectEvidence::none(Some(call.expected_state_generation)),
                fallback,
                call.operation_id.clone(),
                call.exact_scope.exact_scope_sha256(),
                Some("native.provider_unavailable".to_owned()),
            )
            .expect("unavailable terminal"),
            payload: None,
            warnings: vec!["native provider unavailable".to_owned()],
            extensions: Vec::new(),
            state_generation: call.expected_state_generation,
        }
    }

    fn committed_observation_reply(&self, call: &ProviderCall) -> ProviderReply {
        let state_generation = call.expected_state_generation.saturating_add(1);
        ProviderReply {
            terminal: TerminalRecord::new(
                call.operation,
                call.provider_id.clone(),
                TerminalCode::Success,
                CommittedEffectEvidence::committed(
                    call.expected_state_generation,
                    state_generation,
                    vec!["observation:item-1".to_owned()],
                    ONE_SHA,
                    TWO_SHA,
                )
                .expect("committed observation evidence"),
                FallbackDirective::forbidden(),
                call.operation_id.clone(),
                call.exact_scope.exact_scope_sha256(),
                None,
            )
            .expect("committed observation terminal"),
            payload: Some(call.payload.clone()),
            warnings: vec!["observation accepted".to_owned()],
            extensions: call.extensions.clone(),
            state_generation,
        }
    }
}

impl NativeMemoryApplicationPort for EvidenceNativePort {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        self.handshake_calls.fetch_add(1, Ordering::Relaxed);
        HandshakeResponse {
            terminal: TerminalRecord::new(
                self.handshake_terminal_operation,
                self.handshake_terminal_provider_id.clone(),
                TerminalCode::Success,
                CommittedEffectEvidence::none(Some(self.descriptor.state_generation)),
                FallbackDirective::forbidden(),
                request.request_id.clone(),
                request.exact_scope.exact_scope_sha256(),
                None,
            )
            .expect("handshake terminal"),
            descriptor: Some(self.descriptor.clone()),
            provider_instance_id: Some("native.registry-instance".to_owned()),
            state_namespace: Some("native.registry-scope".to_owned()),
            accepted_scope: Some(
                self.handshake_accepted_scope
                    .clone()
                    .unwrap_or_else(|| request.exact_scope.clone()),
            ),
            effective_limits: Some(request.host_limits.minimum(self.descriptor.limits)),
            ready_receipt_sha256: Some(ONE_SHA.to_owned()),
            warnings: Vec::new(),
        }
    }

    fn health(&self, call: &ProviderCall) -> ProviderReply {
        self.health_calls.fetch_add(1, Ordering::Relaxed);
        self.unavailable_reply(call)
    }

    fn observe(&self, observation: NativeObservation<'_>) -> ProviderReply {
        self.observe_calls.fetch_add(1, Ordering::Relaxed);
        self.committed_observation_reply(observation.call())
    }

    fn recall(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected_provider_contact()
    }

    fn feedback(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected_provider_contact()
    }

    fn maintenance(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected_provider_contact()
    }

    fn inspection(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected_provider_contact()
    }

    fn correction(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected_provider_contact()
    }

    fn delete_by_source(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected_provider_contact()
    }

    fn snapshot_export(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected_provider_contact()
    }

    fn snapshot_restore(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected_provider_contact()
    }

    fn replay(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected_provider_contact()
    }
}

fn limits() -> ProviderLimits {
    ProviderLimits {
        request_bytes: 4_096,
        response_bytes: 8_192,
        observation_batch_items: 16,
        recall_candidates: 32,
        concurrent_operations: 4,
        operation_millis: 1_000,
        snapshot_bytes: 65_536,
        inspection_items: 64,
    }
}

fn descriptor(provider_id: &str, limits: ProviderLimits) -> ProviderDescriptor {
    ProviderDescriptor::new(
        OwnedProviderId::new(provider_id).expect("provider id"),
        ZERO_SHA,
        "registry-test-v1",
        5,
        [
            OwnedVersionedId::new("provider.health.v1").expect("health capability"),
            OwnedVersionedId::new("observation.accept.v1").expect("observe capability"),
            OwnedVersionedId::new("recall.query.v1").expect("recall capability"),
        ],
        limits,
    )
    .expect("descriptor")
}

fn config(max_registered_providers: usize, max_in_flight: usize) -> FabricConfig {
    FabricConfig {
        max_registered_providers,
        max_in_flight,
    }
}

fn exact_scope() -> OwnedExactScope {
    OwnedExactScope::new(
        "profile-registry",
        "project-registry",
        "repository-registry",
        "worktree-registry",
        "refs/heads/registry",
        "session-registry",
        RESOLVED_SCOPE_DIGEST,
    )
    .expect("exact scope")
}

fn foreign_exact_scope() -> OwnedExactScope {
    OwnedExactScope::new(
        "profile-foreign",
        "project-foreign",
        "repository-foreign",
        "worktree-foreign",
        "refs/heads/foreign",
        "session-foreign",
        RESOLVED_SCOPE_DIGEST,
    )
    .expect("foreign exact scope")
}

fn call_after_handshake(
    operation: ProviderOperation,
    response: &HandshakeResponse,
) -> ProviderCall {
    assert_eq!(response.terminal.terminal_code(), TerminalCode::Success);
    let ready_receipt_sha256 = response
        .ready_receipt_sha256
        .clone()
        .expect("successful handshake ready receipt");
    let expected_state_generation = response
        .descriptor
        .as_ref()
        .expect("successful handshake descriptor")
        .state_generation;
    let payload_contract = match operation {
        ProviderOperation::Handshake => "tracedecay.memory.provider.handshake.v1",
        ProviderOperation::Health => "tracedecay.memory.provider.health.v1",
        ProviderOperation::Observe => "tracedecay.memory.provider.observation.v1",
        ProviderOperation::Recall => "tracedecay.memory.provider.recall.v1",
        ProviderOperation::Feedback => "tracedecay.memory.provider.feedback.v1",
        ProviderOperation::Maintenance => "tracedecay.memory.provider.maintenance.v1",
        ProviderOperation::Inspection => "tracedecay.memory.provider.inspection.v1",
        ProviderOperation::Correction => "tracedecay.memory.provider.correction.v1",
        ProviderOperation::DeleteBySource => "tracedecay.memory.provider.deletion-by-source.v1",
        ProviderOperation::SnapshotExport => "tracedecay.memory.provider.snapshot-export.v1",
        ProviderOperation::SnapshotRestore => "tracedecay.memory.provider.snapshot-restore.v1",
        ProviderOperation::Replay => "tracedecay.memory.provider.replay.v1",
    };

    let (payload_bytes, payload_sha256) = match operation {
        ProviderOperation::Observe => (OBSERVATION_PAYLOAD.to_vec(), OBSERVATION_PAYLOAD_SHA),
        _ => (br#"{"registry":true}"#.to_vec(), REGISTRY_PAYLOAD_SHA),
    };

    ProviderCall::new(ProviderCallParts {
        operation,
        provider_id: OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("native provider"),
        registration_revision: 31,
        ready_receipt_sha256,
        exact_scope: exact_scope(),
        request_id: format!("request-{}", operation.capability_id()),
        operation_id: format!("operation-{}", operation.capability_id()),
        expected_state_generation,
        idempotency_key: operation
            .mutates_provider_state()
            .then(|| "registry-idempotency-key".to_owned()),
        control: OperationControl::new(i64::MAX, 1_000, CancellationToken::new()),
        payload: CanonicalPayload::new(
            OwnedVersionedId::new(payload_contract).expect("payload contract"),
            payload_bytes,
            payload_sha256,
        )
        .expect("payload"),
        required_capabilities: vec![
            OwnedVersionedId::new(operation.capability_id()).expect("operation capability"),
        ],
        extensions: vec![
            OwnedOpaqueExtension::new(
                OwnedVersionedId::new("vendor.registry-test.optional.v1").expect("extension id"),
                1,
                false,
                OPAQUE_PAYLOAD_SHA,
                br#"{"opaque":true}"#.to_vec(),
            )
            .expect("optional extension"),
        ],
    })
    .map(admitted)
    .expect("provider call")
}

/// Sanitizer revision this harness stands in for. The real revision is derived
/// by `tracedecay-memory-hygiene` from the canonical policy document.
const TEST_SANITIZER_REVISION: &str = "tracedecay.memory.observation.hygiene.v1+registry-test";

/// Attaches the receipt the admitted hygiene pipeline mints for a payload it
/// read and left byte-identical. Observation dispatch fails closed without one.
fn admitted(call: ProviderCall) -> ProviderCall {
    if call.operation != ProviderOperation::Observe {
        return call;
    }
    // The receipt binds the sanitized payload *and* the exact opaque
    // extensions dispatched with it; a receipt over the empty extension set
    // would be rejected as unbound for the optional extension this fixture
    // carries.
    let extensions_digest =
        observation_extensions_digest(&call.extensions).expect("observation extensions digest");
    let receipt = PayloadSanitizationReceipt::new(
        PayloadSanitizationReceiptParts::accepted_unmodified_with_extensions(
            TEST_SANITIZER_REVISION,
            call.payload.sha256.clone(),
            extensions_digest,
        ),
    )
    .expect("accepted sanitization receipt");
    call.with_sanitization(receipt)
}

fn handshake() -> HandshakeRequest {
    HandshakeRequest::new(HandshakeRequestParts {
        provider_id: OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("native provider"),
        registration_revision: 31,
        exact_scope: exact_scope(),
        request_id: "registry-handshake".to_owned(),
        required_capabilities: vec![
            OwnedVersionedId::new("provider.health.v1").expect("health capability"),
            OwnedVersionedId::new("observation.accept.v1").expect("observation capability"),
            OwnedVersionedId::new("recall.query.v1").expect("recall capability"),
        ],
        host_limits: limits(),
        control: OperationControl::new(i64::MAX, 1_000, CancellationToken::new()),
        challenge_nonce: [7; 32],
    })
    .expect("handshake request")
}

#[test]
fn disabled_mode_has_no_port_or_provider_registration() -> Result<(), Box<dyn Error>> {
    let composition =
        ProjectMemoryProviderComposition::compose(NativeProviderActivation::Disabled)?;

    assert!(matches!(
        &composition,
        ProjectMemoryProviderComposition::Disabled
    ));
    assert!(composition.registry().is_none());
    Ok(())
}

#[test]
fn enabled_mode_injects_native_with_configured_revision_mode_and_limits()
-> Result<(), Box<dyn Error>> {
    for (mode, expected_mode, registration_revision) in [
        (EnabledProviderMode::Observer, ProviderMode::Observer, 17),
        (EnabledProviderMode::Active, ProviderMode::Active, 18),
    ] {
        let native_limits = limits();
        let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, native_limits));
        let composition =
            ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
                fabric_config: config(1, 2),
                port: port.clone(),
                registration_revision,
                mode,
            })?;
        let registry = composition.registry().expect("enabled registry");

        let statuses = registry.statuses()?;
        assert_eq!(statuses, registry.statuses()?);
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].provider_id.as_str(), NATIVE_PROVIDER_ID);
        assert_eq!(statuses[0].registration_revision, registration_revision);
        assert_eq!(statuses[0].mode, expected_mode);
        assert_eq!(statuses[0].descriptor.limits, native_limits);
        assert!(port.descriptor_calls.load(Ordering::Relaxed) >= 2);
    }
    Ok(())
}

#[test]
fn statuses_project_readiness_capabilities_and_effective_limits() -> Result<(), Box<dyn Error>> {
    let port = Arc::new(EvidenceNativePort::new());
    let composition =
        ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
            fabric_config: config(1, 2),
            port,
            registration_revision: 31,
            mode: EnabledProviderMode::Active,
        })?;
    let registry = composition.registry().expect("enabled registry");

    let before_handshake = registry.statuses()?;
    assert_eq!(before_handshake.len(), 1);
    let before_handshake = &before_handshake[0];
    assert_eq!(before_handshake.readiness, ProviderReadiness::NotReady);
    assert_eq!(before_handshake.effective_limits, None);
    assert_eq!(
        before_handshake.capability_availability("provider.health.v1"),
        ProviderCapabilityAvailability::SupportedNotReady
    );
    assert_eq!(
        before_handshake.capability_availability("recall.query.v1"),
        ProviderCapabilityAvailability::SupportedNotReady
    );

    let handshake_response = registry.handshake(&handshake())?;
    let after_handshake = registry.statuses()?;
    assert_eq!(after_handshake.len(), 1);
    let after_handshake = &after_handshake[0];
    assert_eq!(after_handshake.readiness, ProviderReadiness::Ready);
    assert_eq!(after_handshake.effective_limits, Some(limits()));
    assert_eq!(
        after_handshake.effective_limits,
        handshake_response.effective_limits
    );
    assert_eq!(
        after_handshake.capability_availability("provider.health.v1"),
        ProviderCapabilityAvailability::SupportedReady
    );
    assert_eq!(
        after_handshake.capability_availability("recall.query.v1"),
        ProviderCapabilityAvailability::SupportedReady
    );
    assert_eq!(
        after_handshake.ready_receipt_sha256.as_deref(),
        Some(ONE_SHA)
    );
    Ok(())
}

#[test]
fn finite_fabric_config_is_validated_before_native_registration() -> Result<(), Box<dyn Error>> {
    let invalid = ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
        fabric_config: config(0, 1),
        port: Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, limits())),
        registration_revision: 1,
        mode: EnabledProviderMode::Active,
    });
    assert!(matches!(
        invalid,
        Err(RegistryError::Fabric(FabricError::InvalidConfig(
            "max_registered_providers must be positive"
        )))
    ));

    let invalid = ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
        fabric_config: config(1, 0),
        port: Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, limits())),
        registration_revision: 1,
        mode: EnabledProviderMode::Active,
    });
    assert!(matches!(
        invalid,
        Err(RegistryError::Fabric(FabricError::InvalidConfig(
            "max_in_flight must be positive"
        )))
    ));

    let port = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, limits()));
    let composition =
        ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
            fabric_config: config(1, 2),
            port,
            registration_revision: 1,
            mode: EnabledProviderMode::Active,
        })?;
    let statuses = composition
        .registry()
        .expect("enabled registry")
        .statuses()?;
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].provider_id.as_str(), NATIVE_PROVIDER_ID);
    Ok(())
}

#[test]
fn adapter_identity_and_registration_revision_failures_remain_typed() {
    let foreign = Arc::new(MockNativePort::new("vendor.foreign", limits()));
    let invalid_adapter =
        ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
            fabric_config: config(1, 1),
            port: foreign,
            registration_revision: 1,
            mode: EnabledProviderMode::Active,
        });
    assert_eq!(
        invalid_adapter.err(),
        Some(RegistryError::NativeAdapter(
            NativeAdapterError::ProviderIdMismatch {
                expected: NATIVE_PROVIDER_ID,
                declared: "vendor.foreign".to_owned(),
            }
        ))
    );

    let native = Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID, limits()));
    let invalid_revision =
        ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
            fabric_config: config(1, 1),
            port: native,
            registration_revision: 0,
            mode: EnabledProviderMode::Active,
        });
    assert_eq!(
        invalid_revision.err(),
        Some(RegistryError::Fabric(FabricError::InvalidConfig(
            "registration_revision must be positive"
        )))
    );
}

#[test]
fn active_route_preserves_structured_fallback_evidence() -> Result<(), Box<dyn Error>> {
    let port = Arc::new(EvidenceNativePort::new());
    let composition =
        ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
            fabric_config: config(1, 2),
            port: port.clone(),
            registration_revision: 31,
            mode: EnabledProviderMode::Active,
        })?;
    let registry = composition.registry().expect("enabled registry");

    let handshake_response = registry.handshake(&handshake())?;
    let call = call_after_handshake(ProviderOperation::Health, &handshake_response);
    let reply = registry.invoke_active(&call)?;

    assert_eq!(
        reply.terminal.terminal_code(),
        TerminalCode::ProviderUnavailable
    );
    assert_eq!(reply.terminal.operation(), ProviderOperation::Health);
    assert_eq!(reply.terminal.provider_id().as_str(), NATIVE_PROVIDER_ID);
    assert_eq!(reply.terminal.operation_id(), call.operation_id);
    assert_eq!(
        reply.terminal.exact_scope_sha256(),
        call.exact_scope.exact_scope_sha256()
    );
    assert_eq!(
        reply.terminal.diagnostic_id(),
        Some("native.provider_unavailable")
    );
    assert_eq!(
        reply.terminal.committed_effect().state(),
        CommittedEffectState::None
    );
    assert_eq!(
        reply.terminal.fallback().eligibility(),
        FallbackEligibility::ExplicitPolicyOnly
    );
    assert_eq!(
        reply
            .terminal
            .fallback()
            .source_provider_id()
            .map(OwnedProviderId::as_str),
        Some(NATIVE_PROVIDER_ID)
    );
    let policy = reply
        .terminal
        .fallback()
        .policy()
        .expect("pinned fallback policy");
    assert_eq!(policy.policy_id(), "memory.fallback.policy");
    assert_eq!(policy.policy_revision(), 7);
    assert_eq!(policy.target_provider_id().as_str(), "vendor.backup");
    assert_eq!(
        reply.terminal.fallback().reason(),
        Some("explicit host policy may select the pinned provider")
    );
    assert_eq!(reply.state_generation, 5);
    assert_eq!(port.health_calls.load(Ordering::Relaxed), 1);
    Ok(())
}

#[test]
fn handshake_route_preserves_complete_provider_neutral_terminal() -> Result<(), Box<dyn Error>> {
    let port = Arc::new(EvidenceNativePort::new());
    let composition =
        ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
            fabric_config: config(1, 2),
            port: port.clone(),
            registration_revision: 31,
            mode: EnabledProviderMode::Active,
        })?;
    let registry = composition.registry().expect("enabled registry");
    let request = handshake();

    let response = registry.handshake(&request)?;

    assert_eq!(port.handshake_calls.load(Ordering::Relaxed), 1);
    assert_eq!(response.terminal.operation(), ProviderOperation::Handshake);
    assert_eq!(response.terminal.provider_id(), &request.provider_id);
    assert_eq!(response.terminal.operation_id(), request.request_id);
    assert_eq!(
        response.terminal.exact_scope_sha256(),
        request.exact_scope.exact_scope_sha256()
    );
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
        Some(5)
    );
    assert_eq!(
        response.terminal.fallback().eligibility(),
        FallbackEligibility::Forbidden
    );
    assert_eq!(response.accepted_scope, Some(request.exact_scope));
    assert_eq!(response.ready_receipt_sha256.as_deref(), Some(ONE_SHA));
    Ok(())
}

#[test]
fn handshake_route_rejects_wrong_terminal_operation_and_provider() -> Result<(), Box<dyn Error>> {
    let wrong_operation = Arc::new(
        EvidenceNativePort::new()
            .with_handshake_terminal(ProviderOperation::Health, NATIVE_PROVIDER_ID),
    );
    let composition =
        ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
            fabric_config: config(1, 2),
            port: wrong_operation.clone(),
            registration_revision: 31,
            mode: EnabledProviderMode::Active,
        })?;
    assert_eq!(
        composition
            .registry()
            .expect("enabled registry")
            .handshake(&handshake()),
        Err(FabricError::ResponseOperationKindMismatch {
            expected: ProviderOperation::Handshake,
            returned: ProviderOperation::Health,
        })
    );
    assert_eq!(wrong_operation.handshake_calls.load(Ordering::Relaxed), 1);

    let wrong_provider = Arc::new(
        EvidenceNativePort::new()
            .with_handshake_terminal(ProviderOperation::Handshake, "vendor.foreign"),
    );
    let composition =
        ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
            fabric_config: config(1, 2),
            port: wrong_provider.clone(),
            registration_revision: 31,
            mode: EnabledProviderMode::Active,
        })?;
    assert_eq!(
        composition
            .registry()
            .expect("enabled registry")
            .handshake(&handshake()),
        Err(FabricError::ResponseProviderMismatch {
            expected: NATIVE_PROVIDER_ID.to_owned(),
            returned: "vendor.foreign".to_owned(),
        })
    );
    assert_eq!(wrong_provider.handshake_calls.load(Ordering::Relaxed), 1);
    Ok(())
}

#[test]
fn observer_route_strips_output_but_preserves_structured_effect_evidence()
-> Result<(), Box<dyn Error>> {
    let port = Arc::new(EvidenceNativePort::new());
    let composition =
        ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
            fabric_config: config(1, 2),
            port: port.clone(),
            registration_revision: 31,
            mode: EnabledProviderMode::Observer,
        })?;
    let registry = composition.registry().expect("enabled registry");
    let handshake_response = registry.handshake(&handshake())?;
    let call = call_after_handshake(ProviderOperation::Observe, &handshake_response);

    assert_eq!(
        registry.invoke_active(&call),
        Err(FabricError::ProviderObserverOnly(
            NATIVE_PROVIDER_ID.to_owned()
        ))
    );
    assert_eq!(port.observe_calls.load(Ordering::Relaxed), 0);

    let receipt = registry.deliver_observation(&call)?;

    assert_eq!(port.observe_calls.load(Ordering::Relaxed), 1);
    let ObserverReceipt {
        provider_id,
        registration_revision,
        terminal,
    } = receipt;
    assert_eq!(provider_id.as_str(), NATIVE_PROVIDER_ID);
    assert_eq!(registration_revision, 31);
    assert_eq!(terminal.operation_id(), call.operation_id);
    assert_eq!(terminal.operation(), ProviderOperation::Observe);
    assert_eq!(terminal.provider_id().as_str(), NATIVE_PROVIDER_ID);
    assert_eq!(
        terminal.exact_scope_sha256(),
        call.exact_scope.exact_scope_sha256()
    );
    assert_eq!(terminal.terminal_code(), TerminalCode::Success);
    assert_eq!(terminal.diagnostic_id(), None);
    let effect = terminal.committed_effect();
    assert_eq!(effect.state(), CommittedEffectState::Committed);
    assert_eq!(effect.committed_boundary(), None);
    assert_eq!(effect.state_generation_before(), Some(5));
    assert_eq!(effect.state_generation_after(), Some(6));
    assert_eq!(
        effect.committed_item_refs(),
        &["observation:item-1".to_owned()]
    );
    assert_eq!(effect.uncommitted_item_refs(), &[] as &[String]);
    assert_eq!(effect.provider_receipt_sha256(), Some(ONE_SHA));
    assert_eq!(effect.reconciliation_action(), None);
    assert_eq!(effect.verification_sha256(), Some(TWO_SHA));
    assert_eq!(
        terminal.fallback().eligibility(),
        FallbackEligibility::Forbidden
    );
    assert_eq!(terminal.fallback().source_provider_id(), None);
    assert_eq!(terminal.fallback().policy(), None);
    assert_eq!(terminal.fallback().reason(), None);
    Ok(())
}

#[test]
fn readiness_target_derives_only_from_validated_handshake_evidence() -> Result<(), Box<dyn Error>> {
    let port = Arc::new(EvidenceNativePort::new());
    let composition =
        ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
            fabric_config: config(1, 2),
            port,
            registration_revision: 31,
            mode: EnabledProviderMode::Active,
        })?;
    let registry = composition.registry().expect("enabled registry");

    let target = registry.readiness_target(&handshake())?;

    assert_eq!(target.provider_id().as_str(), NATIVE_PROVIDER_ID);
    assert_eq!(target.provider_instance_id(), "native.registry-instance");
    assert_eq!(target.registration_revision(), 31);
    assert_eq!(target.ready_receipt_sha256(), ONE_SHA);
    Ok(())
}

#[test]
fn readiness_target_rejects_stale_registration_revision() -> Result<(), Box<dyn Error>> {
    let port = Arc::new(EvidenceNativePort::new());
    let composition =
        ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
            fabric_config: config(1, 2),
            port,
            registration_revision: 31,
            mode: EnabledProviderMode::Active,
        })?;
    let registry = composition.registry().expect("enabled registry");

    let mut stale_request_parts = HandshakeRequestParts {
        provider_id: OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("native provider"),
        registration_revision: 30,
        exact_scope: exact_scope(),
        request_id: "registry-handshake-stale-revision".to_owned(),
        required_capabilities: vec![
            OwnedVersionedId::new("provider.health.v1").expect("health capability"),
            OwnedVersionedId::new("observation.accept.v1").expect("observation capability"),
            OwnedVersionedId::new("recall.query.v1").expect("recall capability"),
        ],
        host_limits: limits(),
        control: OperationControl::new(i64::MAX, 1_000, CancellationToken::new()),
        challenge_nonce: [7; 32],
    };
    let stale_request =
        HandshakeRequest::new(stale_request_parts.clone()).expect("stale handshake request");

    let result = registry.readiness_target(&stale_request);
    assert_eq!(
        result,
        Err(ReadinessTargetError::Fabric(
            FabricError::RegistrationRevisionMismatch {
                accepted: 31,
                requested: 30,
            }
        ))
    );

    // A stale revision never derives a target even once corrected on a later
    // call: readiness_target re-validates every call independently instead
    // of trusting a previously rejected request's cached shape.
    stale_request_parts.registration_revision = 31;
    stale_request_parts.request_id = "registry-handshake-corrected-revision".to_owned();
    let corrected_request =
        HandshakeRequest::new(stale_request_parts).expect("corrected handshake request");
    let target = registry.readiness_target(&corrected_request)?;
    assert_eq!(target.registration_revision(), 31);
    assert_eq!(target.ready_receipt_sha256(), ONE_SHA);
    Ok(())
}

#[test]
fn readiness_target_rejects_foreign_accepted_scope() -> Result<(), Box<dyn Error>> {
    let port =
        Arc::new(EvidenceNativePort::new().with_handshake_accepted_scope(foreign_exact_scope()));
    let composition =
        ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
            fabric_config: config(1, 2),
            port,
            registration_revision: 31,
            mode: EnabledProviderMode::Active,
        })?;
    let registry = composition.registry().expect("enabled registry");

    let result = registry.readiness_target(&handshake());
    assert_eq!(
        result,
        Err(ReadinessTargetError::Fabric(
            FabricError::SuccessfulHandshakeScopeMismatch
        ))
    );

    // The registry retains no readiness from the rejected attempt: status
    // still reports NotReady, so no stale scope could leak into a later
    // target derivation either.
    let statuses = registry.statuses()?;
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].readiness, ProviderReadiness::NotReady);
    Ok(())
}

#[test]
fn disabled_composition_has_no_receiver_for_readiness_target() -> Result<(), Box<dyn Error>> {
    let composition =
        ProjectMemoryProviderComposition::compose(NativeProviderActivation::Disabled)?;

    // Disabled composition constructs no fabric, adapter, or registration,
    // so there is no `ProjectMemoryProviderRegistry` value to call
    // `readiness_target` on: the type system itself keeps disabled
    // composition from ever activating a readiness target.
    assert!(composition.registry().is_none());
    Ok(())
}
