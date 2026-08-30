//! Behavioral contract tests for the provider-neutral runtime API.

use std::collections::BTreeSet;

use tracedecay_memory_provider_api::contract::{
    CommittedEffectState, FallbackEligibility, TerminalCode,
};
use tracedecay_memory_provider_api::{
    ApiError, CancellationToken, CanonicalPayload, HandshakeRequest, HandshakeRequestParts,
    HandshakeResponse, MemoryProvider, OperationControl, OwnedExactScope, OwnedProviderId,
    OwnedVersionedId, ProviderCall, ProviderCallParts, ProviderDescriptor, ProviderLimits,
    ProviderOperation, ProviderReply, TerminalRecord,
};

const DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn capability(value: &str) -> Result<OwnedVersionedId, ApiError> {
    OwnedVersionedId::new(value)
}

fn provider_id() -> Result<OwnedProviderId, ApiError> {
    OwnedProviderId::new("test.provider")
}

fn scope() -> Result<OwnedExactScope, ApiError> {
    OwnedExactScope::new(
        "profile-1",
        "project-1",
        "repo-1",
        "worktree-1",
        "refs/heads/main",
        "session-1",
        7,
    )
}

fn limits() -> ProviderLimits {
    ProviderLimits {
        request_bytes: 1024,
        response_bytes: 2048,
        observation_batch_items: 8,
        recall_candidates: 16,
        concurrent_operations: 2,
        operation_millis: 500,
        snapshot_bytes: 4096,
        inspection_items: 64,
    }
}

fn descriptor() -> Result<ProviderDescriptor, ApiError> {
    ProviderDescriptor::new(
        provider_id()?,
        DIGEST,
        "state.v1",
        0,
        [
            capability("provider.health.v1")?,
            capability("observation.accept.v1")?,
            capability("recall.query.v1")?,
        ],
        limits(),
    )
}

fn payload() -> Result<CanonicalPayload, ApiError> {
    CanonicalPayload::new(
        capability("tracedecay.memory.test-request.v1")?,
        br#"{}"#.to_vec(),
        DIGEST,
    )
}

#[derive(Clone)]
struct TestProvider {
    descriptor: ProviderDescriptor,
}

impl MemoryProvider for TestProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        HandshakeResponse {
            terminal: TerminalRecord {
                terminal_code: TerminalCode::Success,
                committed_effect: CommittedEffectState::None,
                fallback: FallbackEligibility::Forbidden,
                operation_id: request.request_id.clone(),
                exact_scope_sha256: DIGEST.to_owned(),
                provider_receipt_sha256: None,
                diagnostic_id: None,
            },
            descriptor: Some(self.descriptor.clone()),
            provider_instance_id: Some("test.provider.instance-1".to_owned()),
            state_namespace: Some("scope-1".to_owned()),
            accepted_scope: Some(request.exact_scope.clone()),
            effective_limits: Some(request.host_limits.minimum(self.descriptor.limits)),
            ready_receipt_sha256: Some(DIGEST.to_owned()),
            warnings: Vec::new(),
        }
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        ProviderReply {
            terminal: TerminalRecord {
                terminal_code: TerminalCode::SuccessZeroResults,
                committed_effect: CommittedEffectState::None,
                fallback: FallbackEligibility::Forbidden,
                operation_id: call.operation_id.clone(),
                exact_scope_sha256: DIGEST.to_owned(),
                provider_receipt_sha256: None,
                diagnostic_id: None,
            },
            payload: Some(call.payload.clone()),
            warnings: Vec::new(),
            extensions: Vec::new(),
            state_generation: self.descriptor.state_generation,
        }
    }
}

#[test]
fn provider_and_capability_identifiers_use_generated_validation() {
    assert!(OwnedProviderId::new("tracedecay.native").is_ok());
    assert!(OwnedProviderId::new("TraceDecay").is_err());
    assert!(OwnedVersionedId::new("recall.query.v1").is_ok());
    assert!(OwnedVersionedId::new("recall.query").is_err());
}

#[test]
fn exact_scope_is_complete_and_borrowable() -> Result<(), ApiError> {
    let scope = scope()?;
    let borrowed = scope.borrowed();
    assert_eq!(borrowed.project_id, "project-1");
    assert_eq!(borrowed.worktree_identity, "worktree-1");
    assert_eq!(borrowed.scope_revision, 7);
    Ok(())
}

#[test]
fn request_control_distinguishes_cancellation_and_deadline() {
    let cancellation = CancellationToken::new();
    let control = OperationControl::new(123, 10, cancellation.clone());
    assert!(control.snapshot().is_ok());
    cancellation.cancel();
    assert_eq!(control.snapshot(), Err(TerminalCode::Cancelled));

    let expired = OperationControl::new(123, 0, CancellationToken::new());
    assert_eq!(expired.snapshot(), Err(TerminalCode::DeadlineExceeded));
}

#[test]
fn descriptor_requires_all_mandatory_capabilities() -> Result<(), ApiError> {
    let result = ProviderDescriptor::new(
        provider_id()?,
        DIGEST,
        "state.v1",
        0,
        [capability("provider.health.v1")?],
        limits(),
    );
    assert!(matches!(
        result,
        Err(ApiError::MandatoryCapabilityMissing(
            "observation.accept.v1"
        ))
    ));
    Ok(())
}

#[test]
fn duplicate_capabilities_are_rejected() -> Result<(), ApiError> {
    let duplicate = capability("provider.health.v1")?;
    let result = ProviderDescriptor::new(
        provider_id()?,
        DIGEST,
        "state.v1",
        0,
        [
            duplicate.clone(),
            duplicate,
            capability("observation.accept.v1")?,
            capability("recall.query.v1")?,
        ],
        limits(),
    );
    assert!(matches!(result, Err(ApiError::DuplicateCapability(_))));
    Ok(())
}

#[test]
fn mutating_calls_require_idempotency_and_operation_capability() -> Result<(), ApiError> {
    let parts = ProviderCallParts {
        operation: ProviderOperation::Observe,
        provider_id: provider_id()?,
        registration_revision: 1,
        ready_receipt_sha256: DIGEST.to_owned(),
        exact_scope: scope()?,
        request_id: "request-1".to_owned(),
        operation_id: "operation-1".to_owned(),
        expected_state_generation: 0,
        idempotency_key: None,
        control: OperationControl::new(123, 10, CancellationToken::new()),
        payload: payload()?,
        required_capabilities: vec![capability("observation.accept.v1")?],
        extensions: Vec::new(),
    };
    assert!(matches!(
        ProviderCall::new(parts),
        Err(ApiError::MissingIdempotencyKey)
    ));

    let missing_capability = ProviderCallParts {
        operation: ProviderOperation::Recall,
        provider_id: provider_id()?,
        registration_revision: 1,
        ready_receipt_sha256: DIGEST.to_owned(),
        exact_scope: scope()?,
        request_id: "request-2".to_owned(),
        operation_id: "operation-2".to_owned(),
        expected_state_generation: 0,
        idempotency_key: None,
        control: OperationControl::new(123, 10, CancellationToken::new()),
        payload: payload()?,
        required_capabilities: vec![capability("provider.health.v1")?],
        extensions: Vec::new(),
    };
    assert!(matches!(
        ProviderCall::new(missing_capability),
        Err(ApiError::MissingOperationCapability("recall.query.v1"))
    ));
    Ok(())
}

#[test]
fn trait_object_executes_typed_handshake_and_call() -> Result<(), ApiError> {
    let provider: Box<dyn MemoryProvider> = Box::new(TestProvider {
        descriptor: descriptor()?,
    });
    let handshake = HandshakeRequest::new(HandshakeRequestParts {
        provider_id: provider_id()?,
        registration_revision: 1,
        exact_scope: scope()?,
        request_id: "handshake-request".to_owned(),
        required_capabilities: vec![
            capability("provider.health.v1")?,
            capability("observation.accept.v1")?,
            capability("recall.query.v1")?,
        ],
        host_limits: limits(),
        control: OperationControl::new(123, 10, CancellationToken::new()),
        challenge_nonce: [7; 32],
    })?;
    let response = provider.handshake(&handshake);
    assert_eq!(response.terminal.terminal_code, TerminalCode::Success);
    let ready_descriptor = match response.descriptor.as_ref() {
        Some(value) => value,
        None => return Err(ApiError::EmptyField("ready_descriptor")),
    };
    assert_eq!(ready_descriptor.provider_id.as_str(), "test.provider");

    let call = ProviderCall::new(ProviderCallParts {
        operation: ProviderOperation::Recall,
        provider_id: provider_id()?,
        registration_revision: 1,
        ready_receipt_sha256: DIGEST.to_owned(),
        exact_scope: scope()?,
        request_id: "request-3".to_owned(),
        operation_id: "operation-3".to_owned(),
        expected_state_generation: 0,
        idempotency_key: None,
        control: OperationControl::new(123, 10, CancellationToken::new()),
        payload: payload()?,
        required_capabilities: vec![capability("recall.query.v1")?],
        extensions: Vec::new(),
    })?;
    let reply = provider.invoke(&call);
    assert_eq!(
        reply.terminal.terminal_code,
        TerminalCode::SuccessZeroResults
    );
    assert_eq!(reply.terminal.fallback, FallbackEligibility::Forbidden);
    Ok(())
}

#[test]
fn capability_set_is_deterministic() -> Result<(), ApiError> {
    let descriptor = descriptor()?;
    let actual = descriptor
        .capabilities
        .iter()
        .map(OwnedVersionedId::as_str)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        actual,
        BTreeSet::from([
            "observation.accept.v1",
            "provider.health.v1",
            "recall.query.v1",
        ])
    );
    Ok(())
}
