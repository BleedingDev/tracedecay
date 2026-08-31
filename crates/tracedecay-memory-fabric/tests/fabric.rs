//! Behavioral tests for capability-driven memory-fabric orchestration.

use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use tracedecay_memory_fabric::{
    FabricConfig, FabricError, MemoryFabric, ObserverReceipt, ProviderMode,
};
use tracedecay_memory_provider_api::contract::{
    CommittedEffectState, FallbackEligibility, TerminalCode,
};
use tracedecay_memory_provider_api::{
    ApiError, CancellationToken, CanonicalPayload, CommittedEffectEvidence, FallbackDirective,
    HandshakeRequest, HandshakeRequestParts, HandshakeResponse, MemoryProvider, OperationControl,
    OwnedExactScope, OwnedOpaqueExtension, OwnedProviderId, OwnedVersionedId, PinnedFallbackPolicy,
    ProviderCall, ProviderCallParts, ProviderDescriptor, ProviderLimits, ProviderOperation,
    ProviderReply, TerminalRecord,
};

const DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SECOND_DIGEST: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const PAYLOAD_DIGEST: &str = "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a";

fn provider_id(value: &str) -> Result<OwnedProviderId, ApiError> {
    OwnedProviderId::new(value)
}

fn capability(value: &str) -> Result<OwnedVersionedId, ApiError> {
    OwnedVersionedId::new(value)
}

fn scope() -> Result<OwnedExactScope, ApiError> {
    OwnedExactScope::new(
        "profile-1",
        "project-1",
        "repo-1",
        "worktree-1",
        "refs/heads/main",
        "session-1",
        9,
    )
}

fn limits() -> ProviderLimits {
    ProviderLimits {
        request_bytes: 4096,
        response_bytes: 8192,
        observation_batch_items: 8,
        recall_candidates: 16,
        concurrent_operations: 2,
        operation_millis: 1000,
        snapshot_bytes: 16384,
        inspection_items: 32,
    }
}

fn payload() -> Result<CanonicalPayload, ApiError> {
    CanonicalPayload::new(
        capability("tracedecay.memory.test-request.v1")?,
        br#"{}"#.to_vec(),
        PAYLOAD_DIGEST,
    )
}

#[derive(Clone, Copy)]
enum HandshakeMutation {
    None,
    MissingProviderInstance,
    MissingStateNamespace,
    OversizedStateNamespace,
    MissingEffectiveLimits,
    MissingReadyReceipt,
    MalformedReadyReceipt,
    TooManyWarnings,
    MissingHandshakeGenerationEvidence,
    InvalidDescriptor,
    ChangedDescriptor,
    InflatedEffectiveLimits,
    DeflatedEffectiveLimits,
    RotateReadyReceipt,
}

#[allow(clippy::too_many_arguments)]
fn terminal(
    operation: ProviderOperation,
    provider: &str,
    code: TerminalCode,
    effect: CommittedEffectEvidence,
    fallback: FallbackDirective,
    operation_id: &str,
    exact_scope_sha256: &str,
    diagnostic_id: Option<&str>,
) -> Result<TerminalRecord, ApiError> {
    TerminalRecord::new(
        operation,
        provider_id(provider)?,
        code,
        effect,
        fallback,
        operation_id,
        exact_scope_sha256,
        diagnostic_id.map(str::to_owned),
    )
}

struct TestProvider {
    descriptor: ProviderDescriptor,
    state_generation: AtomicUsize,
    handshakes: AtomicUsize,
    invocations: AtomicUsize,
    handshake_terminal: TerminalRecord,
    default_observe_reply: ProviderReply,
    default_recall_reply: ProviderReply,
    scripted_reply: Option<ProviderReply>,
    scripted_handshake_terminal: bool,
    handshake_mutation: HandshakeMutation,
}

impl TestProvider {
    fn new(provider: &str, extra: &[&str]) -> Result<Self, ApiError> {
        Self::build(provider, extra, None)
    }

    fn scripted(provider: &str, reply: ProviderReply) -> Result<Self, ApiError> {
        Self::build(provider, &[], Some(reply))
    }

    fn scripted_handshake(
        provider: &str,
        handshake_terminal: TerminalRecord,
    ) -> Result<Self, ApiError> {
        let mut scripted = Self::build(provider, &[], None)?;
        scripted.handshake_terminal = handshake_terminal;
        scripted.scripted_handshake_terminal = true;
        Ok(scripted)
    }

    fn mutated_handshake(
        provider: &str,
        handshake_mutation: HandshakeMutation,
    ) -> Result<Self, ApiError> {
        let mut provider = Self::build(provider, &[], None)?;
        provider.handshake_mutation = handshake_mutation;
        Ok(provider)
    }

    fn build(
        provider: &str,
        extra: &[&str],
        scripted_reply: Option<ProviderReply>,
    ) -> Result<Self, ApiError> {
        let mut capabilities = vec![
            capability("provider.health.v1")?,
            capability("observation.accept.v1")?,
            capability("recall.query.v1")?,
        ];
        for value in extra {
            capabilities.push(capability(value)?);
        }
        let exact_scope_sha256 = scope()?.exact_scope_sha256();
        let handshake_terminal = terminal(
            ProviderOperation::Handshake,
            provider,
            TerminalCode::Success,
            CommittedEffectEvidence::none(Some(0)),
            FallbackDirective::forbidden(),
            "handshake-1",
            &exact_scope_sha256,
            None,
        )?;
        let default_observe_reply = ProviderReply {
            terminal: terminal(
                ProviderOperation::Observe,
                provider,
                TerminalCode::Success,
                CommittedEffectEvidence::committed(
                    0,
                    1,
                    vec!["operation-observation.accept.v1".to_owned()],
                    DIGEST,
                    DIGEST,
                )?,
                FallbackDirective::forbidden(),
                "operation-observation.accept.v1",
                &exact_scope_sha256,
                None,
            )?,
            payload: Some(payload()?),
            warnings: vec!["test-warning".to_owned()],
            extensions: Vec::new(),
            state_generation: 1,
        };
        let default_recall_reply = ProviderReply {
            terminal: terminal(
                ProviderOperation::Recall,
                provider,
                TerminalCode::SuccessZeroResults,
                CommittedEffectEvidence::none(Some(0)),
                FallbackDirective::forbidden(),
                "operation-recall.query.v1",
                &exact_scope_sha256,
                None,
            )?,
            payload: Some(payload()?),
            warnings: vec!["test-warning".to_owned()],
            extensions: Vec::new(),
            state_generation: 0,
        };
        Ok(Self {
            descriptor: ProviderDescriptor::new(
                provider_id(provider)?,
                DIGEST,
                "state.v1",
                0,
                capabilities,
                limits(),
            )?,
            state_generation: AtomicUsize::new(0),
            handshakes: AtomicUsize::new(0),
            invocations: AtomicUsize::new(0),
            handshake_terminal,
            default_observe_reply,
            default_recall_reply,
            scripted_reply,
            scripted_handshake_terminal: false,
            handshake_mutation: HandshakeMutation::None,
        })
    }

    fn invocation_count(&self) -> usize {
        self.invocations.load(Ordering::Acquire)
    }

    fn handshake_count(&self) -> usize {
        self.handshakes.load(Ordering::Acquire)
    }
}

impl MemoryProvider for TestProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        let mut descriptor = self.descriptor.clone();
        descriptor.state_generation =
            u64::try_from(self.state_generation.load(Ordering::Acquire)).unwrap_or(u64::MAX);
        descriptor
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        let handshake_index = self.handshakes.fetch_add(1, Ordering::AcqRel);
        let descriptor = self.descriptor();
        let handshake_terminal = if self.scripted_handshake_terminal {
            self.handshake_terminal.clone()
        } else {
            match terminal(
                ProviderOperation::Handshake,
                descriptor.provider_id.as_str(),
                TerminalCode::Success,
                CommittedEffectEvidence::none(Some(descriptor.state_generation)),
                FallbackDirective::forbidden(),
                &request.request_id,
                &request.exact_scope.exact_scope_sha256(),
                None,
            ) {
                Ok(terminal) => terminal,
                Err(_) => self.handshake_terminal.clone(),
            }
        };
        let mut response = HandshakeResponse {
            terminal: handshake_terminal,
            descriptor: Some(descriptor.clone()),
            provider_instance_id: Some("test.provider.instance-1".to_owned()),
            state_namespace: Some("scope-1".to_owned()),
            accepted_scope: Some(request.exact_scope.clone()),
            effective_limits: Some(request.host_limits.minimum(descriptor.limits)),
            ready_receipt_sha256: Some(DIGEST.to_owned()),
            warnings: Vec::new(),
        };
        match self.handshake_mutation {
            HandshakeMutation::None => {}
            HandshakeMutation::MissingProviderInstance => response.provider_instance_id = None,
            HandshakeMutation::MissingStateNamespace => response.state_namespace = None,
            HandshakeMutation::OversizedStateNamespace => {
                response.state_namespace = Some("n".repeat(129));
            }
            HandshakeMutation::MissingEffectiveLimits => response.effective_limits = None,
            HandshakeMutation::MissingReadyReceipt => response.ready_receipt_sha256 = None,
            HandshakeMutation::MalformedReadyReceipt => {
                response.ready_receipt_sha256 = Some("not-a-digest".to_owned());
            }
            HandshakeMutation::TooManyWarnings => {
                response.warnings = vec!["warning".to_owned(); 33];
            }
            HandshakeMutation::MissingHandshakeGenerationEvidence => {
                if let Ok(terminal) = terminal(
                    ProviderOperation::Handshake,
                    self.descriptor.provider_id.as_str(),
                    TerminalCode::Success,
                    CommittedEffectEvidence::none(None),
                    FallbackDirective::forbidden(),
                    &request.request_id,
                    &request.exact_scope.exact_scope_sha256(),
                    None,
                ) {
                    response.terminal = terminal;
                }
            }
            HandshakeMutation::InvalidDescriptor => {
                if let Some(descriptor) = &mut response.descriptor {
                    descriptor.protocol_major = 2;
                }
            }
            HandshakeMutation::ChangedDescriptor => {
                if let Some(descriptor) = &mut response.descriptor {
                    descriptor.state_schema_version.push_str(".changed");
                }
            }
            HandshakeMutation::InflatedEffectiveLimits => {
                response.effective_limits = Some(request.host_limits);
            }
            HandshakeMutation::DeflatedEffectiveLimits => {
                if let Some(limits) = &mut response.effective_limits {
                    limits.response_bytes /= 2;
                }
            }
            HandshakeMutation::RotateReadyReceipt if handshake_index > 0 => {
                response.ready_receipt_sha256 = Some(SECOND_DIGEST.to_owned());
            }
            HandshakeMutation::RotateReadyReceipt => {}
        }
        response
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        self.invocations.fetch_add(1, Ordering::AcqRel);
        let reply = if let Some(reply) = &self.scripted_reply {
            reply.clone()
        } else if call.operation == ProviderOperation::Observe {
            self.default_observe_reply.clone()
        } else {
            let state_generation =
                u64::try_from(self.state_generation.load(Ordering::Acquire)).unwrap_or(u64::MAX);
            match terminal(
                call.operation,
                self.descriptor.provider_id.as_str(),
                TerminalCode::SuccessZeroResults,
                CommittedEffectEvidence::none(Some(state_generation)),
                FallbackDirective::forbidden(),
                &call.operation_id,
                &call.exact_scope.exact_scope_sha256(),
                None,
            ) {
                Ok(reply_terminal) => ProviderReply {
                    terminal: reply_terminal,
                    payload: self.default_recall_reply.payload.clone(),
                    warnings: self.default_recall_reply.warnings.clone(),
                    extensions: self.default_recall_reply.extensions.clone(),
                    state_generation,
                },
                Err(_) => self.default_recall_reply.clone(),
            }
        };
        self.state_generation.store(
            usize::try_from(reply.state_generation).unwrap_or(usize::MAX),
            Ordering::Release,
        );
        reply
    }
}

struct BlockingProvider {
    inner: TestProvider,
    dispatch_barrier: Arc<Barrier>,
}

impl MemoryProvider for BlockingProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        self.inner.descriptor()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        self.inner.handshake(request)
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        self.dispatch_barrier.wait();
        self.dispatch_barrier.wait();
        self.inner.invoke(call)
    }
}

fn handshake_request(provider: &str) -> Result<HandshakeRequest, ApiError> {
    HandshakeRequest::new(HandshakeRequestParts {
        provider_id: provider_id(provider)?,
        registration_revision: 1,
        exact_scope: scope()?,
        request_id: "handshake-1".to_owned(),
        required_capabilities: vec![
            capability("provider.health.v1")?,
            capability("observation.accept.v1")?,
            capability("recall.query.v1")?,
        ],
        host_limits: limits(),
        control: OperationControl::new(i64::MAX, 100, CancellationToken::new()),
        challenge_nonce: [3; 32],
    })
}

fn call(
    provider: &str,
    operation: ProviderOperation,
    idempotency_key: Option<&str>,
    required: &[&str],
    control: OperationControl,
) -> Result<ProviderCall, ApiError> {
    let capabilities = required
        .iter()
        .map(|value| capability(value))
        .collect::<Result<Vec<_>, _>>()?;
    ProviderCall::new(ProviderCallParts {
        operation,
        provider_id: provider_id(provider)?,
        registration_revision: 1,
        ready_receipt_sha256: DIGEST.to_owned(),
        exact_scope: scope()?,
        request_id: format!("request-{}", operation.capability_id()),
        operation_id: format!("operation-{}", operation.capability_id()),
        expected_state_generation: 0,
        idempotency_key: idempotency_key.map(str::to_owned),
        control,
        payload: payload()?,
        required_capabilities: capabilities,
        extensions: Vec::new(),
    })
}

#[test]
fn registry_is_bounded_and_rejects_duplicate_or_mismatched_identity() -> Result<(), Box<dyn Error>>
{
    let fabric = MemoryFabric::new(FabricConfig::new(1, 1)?)?;
    let provider = Arc::new(TestProvider::new("provider.one", &[])?);
    fabric.register(
        provider_id("provider.one")?,
        1,
        ProviderMode::Disabled,
        provider.clone(),
    )?;
    assert!(matches!(
        fabric.register(
            provider_id("provider.one")?,
            1,
            ProviderMode::Disabled,
            provider.clone(),
        ),
        Err(FabricError::DuplicateProvider(_))
    ));
    let second = Arc::new(TestProvider::new("provider.two", &[])?);
    assert!(matches!(
        fabric.register(
            provider_id("provider.two")?,
            1,
            ProviderMode::Disabled,
            second,
        ),
        Err(FabricError::RegistryCapacityExhausted)
    ));

    let other_fabric = MemoryFabric::new(FabricConfig::new(2, 1)?)?;
    assert!(matches!(
        other_fabric.register(
            provider_id("selected.provider")?,
            1,
            ProviderMode::Disabled,
            provider,
        ),
        Err(FabricError::ProviderDescriptorMismatch { .. })
    ));

    let invalid_fabric = MemoryFabric::new(FabricConfig::new(1, 1)?)?;
    let mut invalid_provider = TestProvider::new("provider.invalid-descriptor", &[])?;
    invalid_provider.descriptor.protocol_major = 2;
    assert_eq!(
        invalid_fabric.register(
            provider_id("provider.invalid-descriptor")?,
            1,
            ProviderMode::Disabled,
            Arc::new(invalid_provider),
        ),
        Err(FabricError::Api(ApiError::IncompatibleProtocol {
            major: 2,
            minor: 0,
        }))
    );
    Ok(())
}

#[test]
fn successful_handshake_revalidates_every_readiness_field() -> Result<(), Box<dyn Error>> {
    let scenarios = [
        (
            "provider.missing-instance",
            HandshakeMutation::MissingProviderInstance,
            FabricError::Api(ApiError::EmptyField("provider_instance_id")),
        ),
        (
            "provider.missing-namespace",
            HandshakeMutation::MissingStateNamespace,
            FabricError::Api(ApiError::EmptyField("state_namespace")),
        ),
        (
            "provider.oversized-namespace",
            HandshakeMutation::OversizedStateNamespace,
            FabricError::Api(ApiError::TerminalTextTooLong {
                field: "state_namespace",
                maximum: 128,
            }),
        ),
        (
            "provider.missing-limits",
            HandshakeMutation::MissingEffectiveLimits,
            FabricError::Api(ApiError::EmptyField("effective_limits")),
        ),
        (
            "provider.missing-receipt",
            HandshakeMutation::MissingReadyReceipt,
            FabricError::Api(ApiError::EmptyField("ready_receipt_sha256")),
        ),
        (
            "provider.malformed-receipt",
            HandshakeMutation::MalformedReadyReceipt,
            FabricError::Api(ApiError::InvalidSha256("ready_receipt_sha256")),
        ),
        (
            "provider.too-many-handshake-warnings",
            HandshakeMutation::TooManyWarnings,
            FabricError::Api(ApiError::TooManyBoundaryItems {
                field: "warnings",
                maximum: 32,
            }),
        ),
        (
            "provider.missing-handshake-generation",
            HandshakeMutation::MissingHandshakeGenerationEvidence,
            FabricError::ResponseStateGenerationAfterMissing { reported: 0 },
        ),
        (
            "provider.invalid-handshake-descriptor",
            HandshakeMutation::InvalidDescriptor,
            FabricError::Api(ApiError::IncompatibleProtocol { major: 2, minor: 0 }),
        ),
        (
            "provider.changed-descriptor",
            HandshakeMutation::ChangedDescriptor,
            FabricError::SuccessfulHandshakeDescriptorMismatch,
        ),
    ];

    for (provider_name, mutation, expected) in scenarios {
        let fabric = MemoryFabric::new(FabricConfig::new(1, 1)?)?;
        fabric.register(
            provider_id(provider_name)?,
            1,
            ProviderMode::Active,
            Arc::new(TestProvider::mutated_handshake(provider_name, mutation)?),
        )?;
        assert_eq!(
            fabric.handshake(&handshake_request(provider_name)?),
            Err(expected)
        );
    }

    let provider_name = "provider.inflated-limits";
    let fabric = MemoryFabric::new(FabricConfig::new(1, 1)?)?;
    let mut provider =
        TestProvider::mutated_handshake(provider_name, HandshakeMutation::InflatedEffectiveLimits)?;
    provider.descriptor.limits.response_bytes /= 2;
    fabric.register(
        provider_id(provider_name)?,
        1,
        ProviderMode::Active,
        Arc::new(provider),
    )?;
    assert_eq!(
        fabric.handshake(&handshake_request(provider_name)?),
        Err(FabricError::SuccessfulHandshakeEffectiveLimitsMismatch)
    );

    let provider_name = "provider.deflated-limits";
    let fabric = MemoryFabric::new(FabricConfig::new(1, 1)?)?;
    fabric.register(
        provider_id(provider_name)?,
        1,
        ProviderMode::Active,
        Arc::new(TestProvider::mutated_handshake(
            provider_name,
            HandshakeMutation::DeflatedEffectiveLimits,
        )?),
    )?;
    assert_eq!(
        fabric.handshake(&handshake_request(provider_name)?),
        Err(FabricError::SuccessfulHandshakeEffectiveLimitsMismatch)
    );
    Ok(())
}

#[test]
fn failed_handshake_rejects_readiness_metadata_and_bounds_warnings() -> Result<(), Box<dyn Error>> {
    let provider_name = "provider.failed-handshake-metadata";
    let request = handshake_request(provider_name)?;
    let failed_terminal = terminal(
        ProviderOperation::Handshake,
        provider_name,
        TerminalCode::ProviderUnavailable,
        CommittedEffectEvidence::none(Some(0)),
        FallbackDirective::forbidden(),
        &request.request_id,
        &request.exact_scope.exact_scope_sha256(),
        Some("diagnostic.handshake-unavailable"),
    )?;
    let fabric = MemoryFabric::new(FabricConfig::new(1, 1)?)?;
    let provider = Arc::new(TestProvider::scripted_handshake(
        provider_name,
        failed_terminal,
    )?);
    fabric.register(
        provider_id(provider_name)?,
        1,
        ProviderMode::Active,
        provider.clone(),
    )?;
    assert_eq!(
        fabric.handshake(&request),
        Err(FabricError::FailedHandshakeCarriedReadiness)
    );
    assert_eq!(provider.handshake_count(), 1);

    let warning_provider_name = "provider.failed-handshake-warnings";
    let warning_request = handshake_request(warning_provider_name)?;
    let warning_terminal = terminal(
        ProviderOperation::Handshake,
        warning_provider_name,
        TerminalCode::ProviderUnavailable,
        CommittedEffectEvidence::none(Some(0)),
        FallbackDirective::forbidden(),
        &warning_request.request_id,
        &warning_request.exact_scope.exact_scope_sha256(),
        Some("diagnostic.handshake-unavailable"),
    )?;
    let warning_fabric = MemoryFabric::new(FabricConfig::new(1, 1)?)?;
    let mut warning_provider =
        TestProvider::mutated_handshake(warning_provider_name, HandshakeMutation::TooManyWarnings)?;
    warning_provider.handshake_terminal = warning_terminal;
    warning_provider.scripted_handshake_terminal = true;
    warning_fabric.register(
        provider_id(warning_provider_name)?,
        1,
        ProviderMode::Active,
        Arc::new(warning_provider),
    )?;
    assert_eq!(
        warning_fabric.handshake(&warning_request),
        Err(FabricError::Api(ApiError::TooManyBoundaryItems {
            field: "warnings",
            maximum: 32,
        }))
    );
    Ok(())
}

#[test]
fn mutated_handshake_request_fails_before_provider_contact() -> Result<(), Box<dyn Error>> {
    let fabric = MemoryFabric::new(FabricConfig::new(1, 1)?)?;
    let provider = Arc::new(TestProvider::new("provider.handshake-envelope", &[])?);
    fabric.register(
        provider_id("provider.handshake-envelope")?,
        1,
        ProviderMode::Active,
        provider.clone(),
    )?;
    let mut request = handshake_request("provider.handshake-envelope")?;
    request.request_id.clear();
    assert_eq!(
        fabric.handshake(&request),
        Err(FabricError::Api(ApiError::EmptyField("request_id")))
    );
    assert_eq!(provider.handshake_count(), 0);
    Ok(())
}

#[test]
fn active_provider_is_selected_by_identity_and_capability() -> Result<(), Box<dyn Error>> {
    let fabric = MemoryFabric::new(FabricConfig::new(4, 2)?)?;
    let provider = Arc::new(TestProvider::new("provider.active", &[])?);
    fabric.register(
        provider_id("provider.active")?,
        1,
        ProviderMode::Active,
        provider.clone(),
    )?;
    let response = fabric.handshake(&handshake_request("provider.active")?)?;
    assert_eq!(response.terminal.operation(), ProviderOperation::Handshake);
    assert_eq!(response.terminal.provider_id().as_str(), "provider.active");
    assert_eq!(response.terminal.terminal_code(), TerminalCode::Success);

    let recall = call(
        "provider.active",
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    let reply = fabric.invoke_active(&recall)?;
    assert_eq!(
        reply.terminal.terminal_code(),
        TerminalCode::SuccessZeroResults
    );
    assert_eq!(reply.terminal.operation(), ProviderOperation::Recall);
    assert_eq!(reply.terminal.provider_id().as_str(), "provider.active");
    assert_eq!(provider.invocation_count(), 1);

    let feedback = call(
        "provider.active",
        ProviderOperation::Feedback,
        Some(DIGEST),
        &["feedback.record.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    assert!(matches!(
        fabric.invoke_active(&feedback),
        Err(FabricError::MissingCapability(capability))
            if capability == "feedback.record.v1"
    ));
    assert_eq!(provider.invocation_count(), 1);
    Ok(())
}

#[test]
fn readiness_is_required_scoped_and_rotated_after_generation_change() -> Result<(), Box<dyn Error>>
{
    let provider_name = "provider.readiness";
    let fabric = MemoryFabric::new(FabricConfig::new(1, 1)?)?;
    let provider = Arc::new(TestProvider::mutated_handshake(
        provider_name,
        HandshakeMutation::RotateReadyReceipt,
    )?);
    fabric.register(
        provider_id(provider_name)?,
        1,
        ProviderMode::Active,
        provider.clone(),
    )?;

    let recall_before_handshake = call(
        provider_name,
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    assert!(matches!(
        fabric.invoke_active(&recall_before_handshake),
        Err(FabricError::ProviderNotReady(provider)) if provider == provider_name
    ));
    assert_eq!(provider.invocation_count(), 0);

    let first_handshake = fabric.handshake(&handshake_request(provider_name)?)?;
    assert_eq!(
        first_handshake.ready_receipt_sha256.as_deref(),
        Some(DIGEST)
    );

    let mut wrong_receipt = recall_before_handshake.clone();
    wrong_receipt.ready_receipt_sha256 = SECOND_DIGEST.to_owned();
    assert_eq!(
        fabric.invoke_active(&wrong_receipt),
        Err(FabricError::ReadyReceiptMismatch)
    );

    let mut wrong_scope = recall_before_handshake.clone();
    wrong_scope.exact_scope.worktree_identity = "another-worktree".to_owned();
    assert_eq!(
        fabric.invoke_active(&wrong_scope),
        Err(FabricError::ReadyScopeMismatch)
    );

    let mut wrong_revision = recall_before_handshake.clone();
    wrong_revision.registration_revision = 2;
    assert!(matches!(
        fabric.invoke_active(&wrong_revision),
        Err(FabricError::RegistrationRevisionMismatch {
            accepted: 1,
            requested: 2,
        })
    ));
    assert_eq!(provider.invocation_count(), 0);

    let observe = call(
        provider_name,
        ProviderOperation::Observe,
        Some(DIGEST),
        &["observation.accept.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    fabric.deliver_observation(&observe)?;
    assert_eq!(provider.invocation_count(), 1);

    let mut stale_generation_receipt = recall_before_handshake.clone();
    stale_generation_receipt.expected_state_generation = 1;
    assert!(matches!(
        fabric.invoke_active(&stale_generation_receipt),
        Err(FabricError::ProviderNotReady(provider)) if provider == provider_name
    ));
    assert_eq!(provider.invocation_count(), 1);

    let second_handshake = fabric.handshake(&handshake_request(provider_name)?)?;
    assert_eq!(
        second_handshake.ready_receipt_sha256.as_deref(),
        Some(SECOND_DIGEST)
    );
    assert_eq!(
        second_handshake
            .descriptor
            .as_ref()
            .map(|descriptor| descriptor.state_generation),
        Some(1)
    );

    let mut replayed_receipt = stale_generation_receipt.clone();
    replayed_receipt.ready_receipt_sha256 = DIGEST.to_owned();
    assert_eq!(
        fabric.invoke_active(&replayed_receipt),
        Err(FabricError::ReadyReceiptMismatch)
    );

    let mut current_recall = stale_generation_receipt;
    current_recall.ready_receipt_sha256 = SECOND_DIGEST.to_owned();
    fabric.invoke_active(&current_recall)?;
    assert_eq!(provider.invocation_count(), 2);

    provider.state_generation.store(0, Ordering::Release);
    assert_eq!(
        fabric.handshake(&handshake_request(provider_name)?),
        Err(FabricError::SuccessfulHandshakeStateGenerationRegressed {
            accepted: 1,
            returned: 0,
        })
    );

    fabric.set_mode(&provider_id(provider_name)?, 1, ProviderMode::Observer)?;
    fabric.set_mode(&provider_id(provider_name)?, 1, ProviderMode::Active)?;
    assert!(matches!(
        fabric.invoke_active(&current_recall),
        Err(FabricError::ProviderNotReady(provider)) if provider == provider_name
    ));
    assert_eq!(provider.invocation_count(), 2);
    Ok(())
}

#[test]
fn capacity_rejection_preserves_previously_accepted_readiness() -> Result<(), Box<dyn Error>> {
    let fabric = Arc::new(MemoryFabric::new(FabricConfig::new(2, 1)?)?);
    let dispatch_barrier = Arc::new(Barrier::new(2));
    let blocking_provider = Arc::new(BlockingProvider {
        inner: TestProvider::new("provider.capacity-holder", &[])?,
        dispatch_barrier: Arc::clone(&dispatch_barrier),
    });
    let retained_provider = Arc::new(TestProvider::new("provider.retained-readiness", &[])?);
    fabric.register(
        provider_id("provider.capacity-holder")?,
        1,
        ProviderMode::Active,
        blocking_provider,
    )?;
    fabric.register(
        provider_id("provider.retained-readiness")?,
        1,
        ProviderMode::Active,
        retained_provider.clone(),
    )?;
    fabric.handshake(&handshake_request("provider.capacity-holder")?)?;
    fabric.handshake(&handshake_request("provider.retained-readiness")?)?;

    let blocking_call = call(
        "provider.capacity-holder",
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    let in_flight_fabric = Arc::clone(&fabric);
    let in_flight = thread::spawn(move || in_flight_fabric.invoke_active(&blocking_call));
    dispatch_barrier.wait();
    let rejected_handshake = fabric.handshake(&handshake_request("provider.retained-readiness")?);
    dispatch_barrier.wait();
    let in_flight_result = in_flight.join();
    assert!(in_flight_result.is_ok());
    if let Ok(result) = in_flight_result {
        result?;
    }

    assert_eq!(rejected_handshake, Err(FabricError::CapacityExhausted));
    assert_eq!(retained_provider.handshake_count(), 1);
    let retained_call = call(
        "provider.retained-readiness",
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    fabric.invoke_active(&retained_call)?;
    assert_eq!(retained_provider.invocation_count(), 1);
    Ok(())
}

#[test]
fn mutated_calls_fail_before_active_or_observer_provider_contact() -> Result<(), Box<dyn Error>> {
    let fabric = MemoryFabric::new(FabricConfig::new(1, 1)?)?;
    let provider = Arc::new(TestProvider::new("provider.call-envelope", &[])?);
    fabric.register(
        provider_id("provider.call-envelope")?,
        1,
        ProviderMode::Active,
        provider.clone(),
    )?;

    let mut recall = call(
        "provider.call-envelope",
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    recall.ready_receipt_sha256 = "not-a-digest".to_owned();
    assert_eq!(
        fabric.invoke_active(&recall),
        Err(FabricError::Api(ApiError::InvalidSha256(
            "ready_receipt_sha256"
        )))
    );

    let mut observe = call(
        "provider.call-envelope",
        ProviderOperation::Observe,
        Some(DIGEST),
        &["observation.accept.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    observe.payload.bytes.push(b' ');
    assert!(matches!(
        fabric.deliver_observation(&observe),
        Err(FabricError::Api(_))
    ));
    assert_eq!(provider.invocation_count(), 0);

    let mut bounded_handshake = handshake_request("provider.call-envelope")?;
    bounded_handshake.host_limits.request_bytes = 128;
    fabric.handshake(&bounded_handshake)?;
    let bounded_recall = call(
        "provider.call-envelope",
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    assert_eq!(
        fabric.invoke_active(&bounded_recall),
        Err(FabricError::Api(ApiError::BoundaryBytesExceeded {
            field: "request",
            maximum: 128,
        }))
    );
    assert_eq!(provider.invocation_count(), 0);
    Ok(())
}

#[test]
fn mutable_reply_envelopes_are_revalidated_after_provider_contact() -> Result<(), Box<dyn Error>> {
    let fabric = MemoryFabric::new(FabricConfig::new(4, 2)?)?;

    let stale_provider_name = "provider.stale-reply-payload";
    let stale_call = call(
        stale_provider_name,
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    let mut stale_payload = payload()?;
    stale_payload.bytes.push(b' ');
    let stale_reply = ProviderReply {
        terminal: terminal(
            ProviderOperation::Recall,
            stale_provider_name,
            TerminalCode::SuccessZeroResults,
            CommittedEffectEvidence::none(Some(0)),
            FallbackDirective::forbidden(),
            &stale_call.operation_id,
            &stale_call.exact_scope.exact_scope_sha256(),
            None,
        )?,
        payload: Some(stale_payload),
        warnings: Vec::new(),
        extensions: Vec::new(),
        state_generation: 0,
    };
    let stale_provider = Arc::new(TestProvider::scripted(stale_provider_name, stale_reply)?);
    fabric.register(
        provider_id(stale_provider_name)?,
        1,
        ProviderMode::Active,
        stale_provider.clone(),
    )?;
    fabric.handshake(&handshake_request(stale_provider_name)?)?;
    assert!(matches!(
        fabric.invoke_active(&stale_call),
        Err(FabricError::Api(_))
    ));
    assert_eq!(stale_provider.invocation_count(), 1);

    let failure_provider_name = "provider.failure-payload";
    let failure_call = call(
        failure_provider_name,
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    let failure_reply = ProviderReply {
        terminal: terminal(
            ProviderOperation::Recall,
            failure_provider_name,
            TerminalCode::ProviderUnavailable,
            CommittedEffectEvidence::none(Some(0)),
            FallbackDirective::forbidden(),
            &failure_call.operation_id,
            &failure_call.exact_scope.exact_scope_sha256(),
            Some("provider.unavailable"),
        )?,
        payload: Some(payload()?),
        warnings: Vec::new(),
        extensions: Vec::new(),
        state_generation: 0,
    };
    fabric.register(
        provider_id(failure_provider_name)?,
        1,
        ProviderMode::Active,
        Arc::new(TestProvider::scripted(
            failure_provider_name,
            failure_reply,
        )?),
    )?;
    fabric.handshake(&handshake_request(failure_provider_name)?)?;
    assert!(matches!(
        fabric.invoke_active(&failure_call),
        Err(FabricError::Api(_))
    ));

    let oversized_provider_name = "provider.oversized-reply";
    let oversized_call = call(
        oversized_provider_name,
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    let oversized_reply = ProviderReply {
        terminal: terminal(
            ProviderOperation::Recall,
            oversized_provider_name,
            TerminalCode::SuccessZeroResults,
            CommittedEffectEvidence::none(Some(0)),
            FallbackDirective::forbidden(),
            &oversized_call.operation_id,
            &oversized_call.exact_scope.exact_scope_sha256(),
            None,
        )?,
        payload: None,
        warnings: vec!["w".repeat(500)],
        extensions: Vec::new(),
        state_generation: 0,
    };
    fabric.register(
        provider_id(oversized_provider_name)?,
        1,
        ProviderMode::Active,
        Arc::new(TestProvider::scripted(
            oversized_provider_name,
            oversized_reply,
        )?),
    )?;
    let mut bounded_handshake = handshake_request(oversized_provider_name)?;
    bounded_handshake.host_limits.response_bytes = 256;
    fabric.handshake(&bounded_handshake)?;
    assert!(matches!(
        fabric.invoke_active(&oversized_call),
        Err(FabricError::Api(_))
    ));

    let extensions_provider_name = "provider.too-many-extensions";
    let extensions_call = call(
        extensions_provider_name,
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    let extensions = (0..17)
        .map(|index| {
            OwnedOpaqueExtension::new(
                capability(&format!("vendor.extension-{index}.v1"))?,
                1,
                false,
                PAYLOAD_DIGEST,
                br#"{}"#.to_vec(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let extensions_reply = ProviderReply {
        terminal: terminal(
            ProviderOperation::Recall,
            extensions_provider_name,
            TerminalCode::SuccessZeroResults,
            CommittedEffectEvidence::none(Some(0)),
            FallbackDirective::forbidden(),
            &extensions_call.operation_id,
            &extensions_call.exact_scope.exact_scope_sha256(),
            None,
        )?,
        payload: None,
        warnings: Vec::new(),
        extensions,
        state_generation: 0,
    };
    fabric.register(
        provider_id(extensions_provider_name)?,
        1,
        ProviderMode::Active,
        Arc::new(TestProvider::scripted(
            extensions_provider_name,
            extensions_reply,
        )?),
    )?;
    fabric.handshake(&handshake_request(extensions_provider_name)?)?;
    assert!(matches!(
        fabric.invoke_active(&extensions_call),
        Err(FabricError::Api(_))
    ));
    Ok(())
}

#[test]
fn observer_delivery_is_structurally_isolated_from_active_output() -> Result<(), Box<dyn Error>> {
    let fabric = MemoryFabric::new(FabricConfig::new(4, 2)?)?;
    let provider = Arc::new(TestProvider::new("provider.observer", &[])?);
    fabric.register(
        provider_id("provider.observer")?,
        1,
        ProviderMode::Observer,
        provider.clone(),
    )?;
    fabric.handshake(&handshake_request("provider.observer")?)?;
    let observe = call(
        "provider.observer",
        ProviderOperation::Observe,
        Some(DIGEST),
        &["observation.accept.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    let receipt = fabric.deliver_observation(&observe)?;
    let ObserverReceipt {
        provider_id: receipt_provider_id,
        registration_revision,
        terminal: receipt_terminal,
    } = receipt;
    assert_eq!(receipt_provider_id.as_str(), "provider.observer");
    assert_eq!(registration_revision, 1);
    assert_eq!(receipt_terminal.operation(), ProviderOperation::Observe);
    assert_eq!(receipt_terminal.provider_id().as_str(), "provider.observer");
    assert_eq!(receipt_terminal.terminal_code(), TerminalCode::Success);
    assert_eq!(
        receipt_terminal.committed_effect().state(),
        CommittedEffectState::Committed
    );
    assert_eq!(provider.invocation_count(), 1);

    let recall = call(
        "provider.observer",
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    assert!(matches!(
        fabric.invoke_active(&recall),
        Err(FabricError::ProviderObserverOnly(_))
    ));
    assert_eq!(provider.invocation_count(), 1);
    Ok(())
}

#[test]
fn observer_receipt_preserves_generation_bound_committed_effect_shapes()
-> Result<(), Box<dyn Error>> {
    let fabric = MemoryFabric::new(FabricConfig::new(8, 2)?)?;
    let scenarios = vec![
        (
            "provider.effect-none",
            TerminalCode::Success,
            CommittedEffectEvidence::none(Some(0)),
            0,
            None,
        ),
        (
            "provider.effect-committed",
            TerminalCode::Success,
            CommittedEffectEvidence::committed(
                0,
                1,
                vec!["item-a".to_owned(), "item-b".to_owned()],
                DIGEST,
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            )?,
            1,
            None,
        ),
        (
            "provider.effect-partial",
            TerminalCode::PartialEffect,
            CommittedEffectEvidence::partial(
                "items[0..1)",
                0,
                1,
                vec!["item-a".to_owned()],
                vec!["item-b".to_owned()],
                DIGEST,
                "resume-from:item-b",
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            )?,
            1,
            Some("diagnostic.partial-effect"),
        ),
    ];
    let mut receipts = Vec::new();

    for (provider_name, code, effect, state_generation, diagnostic_id) in scenarios {
        let mut observe = call(
            provider_name,
            ProviderOperation::Observe,
            Some(DIGEST),
            &["observation.accept.v1"],
            OperationControl::new(i64::MAX, 100, CancellationToken::new()),
        )?;
        observe.expected_state_generation = effect.state_generation_before().unwrap_or_default();
        let expected_effect = effect.clone();
        let reply = ProviderReply {
            terminal: terminal(
                ProviderOperation::Observe,
                provider_name,
                code,
                effect,
                FallbackDirective::forbidden(),
                &observe.operation_id,
                &observe.exact_scope.exact_scope_sha256(),
                diagnostic_id,
            )?,
            payload: None,
            warnings: vec!["retained-warning".to_owned()],
            extensions: Vec::new(),
            state_generation,
        };
        let provider = Arc::new(TestProvider::scripted(provider_name, reply)?);
        fabric.register(
            provider_id(provider_name)?,
            1,
            ProviderMode::Observer,
            provider,
        )?;
        fabric.handshake(&handshake_request(provider_name)?)?;
        let receipt = fabric.deliver_observation(&observe)?;
        assert_eq!(receipt.terminal.committed_effect(), &expected_effect);
        receipts.push(receipt);
    }

    let none = receipts[0].terminal.committed_effect();
    assert_eq!(none.state(), CommittedEffectState::None);
    assert_eq!(none.state_generation_before(), Some(0));
    assert_eq!(none.state_generation_after(), Some(0));

    let committed = receipts[1].terminal.committed_effect();
    assert_eq!(committed.state(), CommittedEffectState::Committed);
    assert_eq!(committed.state_generation_before(), Some(0));
    assert_eq!(committed.state_generation_after(), Some(1));
    assert_eq!(
        committed.committed_item_refs(),
        &["item-a".to_owned(), "item-b".to_owned()]
    );
    assert_eq!(committed.provider_receipt_sha256(), Some(DIGEST));
    assert_eq!(
        committed.verification_sha256(),
        Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
    );

    let partial = receipts[2].terminal.committed_effect();
    assert_eq!(partial.state(), CommittedEffectState::Partial);
    assert_eq!(partial.committed_boundary(), Some("items[0..1)"));
    assert_eq!(partial.committed_item_refs(), &["item-a".to_owned()]);
    assert_eq!(partial.uncommitted_item_refs(), &["item-b".to_owned()]);
    assert_eq!(partial.reconciliation_action(), Some("resume-from:item-b"));

    Ok(())
}

#[test]
fn fallback_is_preserved_but_never_automatically_dispatched() -> Result<(), Box<dyn Error>> {
    let fabric = MemoryFabric::new(FabricConfig::new(4, 2)?)?;
    let target = Arc::new(TestProvider::new("provider.fallback-target", &[])?);
    fabric.register(
        provider_id("provider.fallback-target")?,
        1,
        ProviderMode::Active,
        target.clone(),
    )?;

    let forbidden_call = call(
        "provider.forbidden",
        ProviderOperation::Observe,
        Some(DIGEST),
        &["observation.accept.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    let forbidden_reply = ProviderReply {
        terminal: terminal(
            ProviderOperation::Observe,
            "provider.forbidden",
            TerminalCode::ProviderUnavailable,
            CommittedEffectEvidence::none(Some(0)),
            FallbackDirective::forbidden(),
            &forbidden_call.operation_id,
            &forbidden_call.exact_scope.exact_scope_sha256(),
            Some("diagnostic.provider-unavailable"),
        )?,
        payload: None,
        warnings: Vec::new(),
        extensions: Vec::new(),
        state_generation: 0,
    };
    let forbidden_provider = Arc::new(TestProvider::scripted(
        "provider.forbidden",
        forbidden_reply,
    )?);
    fabric.register(
        provider_id("provider.forbidden")?,
        1,
        ProviderMode::Observer,
        forbidden_provider.clone(),
    )?;
    fabric.handshake(&handshake_request("provider.forbidden")?)?;
    let forbidden_receipt = fabric.deliver_observation(&forbidden_call)?;
    assert_eq!(
        forbidden_receipt.terminal.fallback().eligibility(),
        FallbackEligibility::Forbidden
    );

    let explicit_provider_id = provider_id("provider.explicit")?;
    let explicit_call = call(
        explicit_provider_id.as_str(),
        ProviderOperation::Observe,
        Some(DIGEST),
        &["observation.accept.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    let policy = PinnedFallbackPolicy::new(
        "policy.memory-failover",
        7,
        provider_id("provider.fallback-target")?,
    )?;
    let explicit_reply = ProviderReply {
        terminal: terminal(
            ProviderOperation::Observe,
            explicit_provider_id.as_str(),
            TerminalCode::ProviderUnavailable,
            CommittedEffectEvidence::none(Some(0)),
            FallbackDirective::explicit_policy_only(
                &explicit_provider_id,
                policy,
                "operator-approved provider outage policy",
            )?,
            &explicit_call.operation_id,
            &explicit_call.exact_scope.exact_scope_sha256(),
            Some("diagnostic.provider-unavailable"),
        )?,
        payload: None,
        warnings: Vec::new(),
        extensions: Vec::new(),
        state_generation: 0,
    };
    let explicit_provider = Arc::new(TestProvider::scripted(
        explicit_provider_id.as_str(),
        explicit_reply,
    )?);
    fabric.register(
        explicit_provider_id,
        1,
        ProviderMode::Observer,
        explicit_provider.clone(),
    )?;
    fabric.handshake(&handshake_request("provider.explicit")?)?;
    let explicit_receipt = fabric.deliver_observation(&explicit_call)?;
    let fallback = explicit_receipt.terminal.fallback();
    assert_eq!(
        fallback.eligibility(),
        FallbackEligibility::ExplicitPolicyOnly
    );
    assert_eq!(
        fallback.reason(),
        Some("operator-approved provider outage policy")
    );
    let retained_policy = fallback
        .policy()
        .ok_or(ApiError::EmptyField("fallback_policy"))?;
    assert_eq!(retained_policy.policy_id(), "policy.memory-failover");
    assert_eq!(retained_policy.policy_revision(), 7);
    assert_eq!(
        retained_policy.target_provider_id().as_str(),
        "provider.fallback-target"
    );
    assert_eq!(forbidden_provider.invocation_count(), 1);
    assert_eq!(explicit_provider.invocation_count(), 1);
    assert_eq!(target.invocation_count(), 0);
    Ok(())
}

#[test]
fn boundary_rejects_operation_provider_scope_and_generation_mismatches()
-> Result<(), Box<dyn Error>> {
    let fabric = MemoryFabric::new(FabricConfig::new(8, 2)?)?;

    let wrong_handshake_request = handshake_request("provider.wrong-handshake-operation")?;
    let wrong_handshake_terminal = terminal(
        ProviderOperation::Health,
        "provider.wrong-handshake-operation",
        TerminalCode::Success,
        CommittedEffectEvidence::none(Some(0)),
        FallbackDirective::forbidden(),
        &wrong_handshake_request.request_id,
        &wrong_handshake_request.exact_scope.exact_scope_sha256(),
        None,
    )?;
    fabric.register(
        provider_id("provider.wrong-handshake-operation")?,
        1,
        ProviderMode::Observer,
        Arc::new(TestProvider::scripted_handshake(
            "provider.wrong-handshake-operation",
            wrong_handshake_terminal,
        )?),
    )?;
    assert_eq!(
        fabric.handshake(&wrong_handshake_request),
        Err(FabricError::ResponseOperationKindMismatch {
            expected: ProviderOperation::Handshake,
            returned: ProviderOperation::Health,
        })
    );

    let wrong_operation_call = call(
        "provider.wrong-operation",
        ProviderOperation::Observe,
        Some(DIGEST),
        &["observation.accept.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    let wrong_operation_reply = ProviderReply {
        terminal: terminal(
            ProviderOperation::Recall,
            "provider.wrong-operation",
            TerminalCode::SuccessZeroResults,
            CommittedEffectEvidence::none(Some(0)),
            FallbackDirective::forbidden(),
            &wrong_operation_call.operation_id,
            &wrong_operation_call.exact_scope.exact_scope_sha256(),
            None,
        )?,
        payload: None,
        warnings: Vec::new(),
        extensions: Vec::new(),
        state_generation: 0,
    };
    fabric.register(
        provider_id("provider.wrong-operation")?,
        1,
        ProviderMode::Observer,
        Arc::new(TestProvider::scripted(
            "provider.wrong-operation",
            wrong_operation_reply,
        )?),
    )?;
    fabric.handshake(&handshake_request("provider.wrong-operation")?)?;
    assert_eq!(
        fabric.deliver_observation(&wrong_operation_call),
        Err(FabricError::ResponseOperationKindMismatch {
            expected: ProviderOperation::Observe,
            returned: ProviderOperation::Recall,
        })
    );

    let wrong_provider_call = call(
        "provider.wrong-attribution",
        ProviderOperation::Observe,
        Some(DIGEST),
        &["observation.accept.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    let wrong_provider_reply = ProviderReply {
        terminal: terminal(
            ProviderOperation::Observe,
            "provider.other-attribution",
            TerminalCode::Success,
            CommittedEffectEvidence::none(Some(0)),
            FallbackDirective::forbidden(),
            &wrong_provider_call.operation_id,
            &wrong_provider_call.exact_scope.exact_scope_sha256(),
            None,
        )?,
        payload: None,
        warnings: Vec::new(),
        extensions: Vec::new(),
        state_generation: 0,
    };
    fabric.register(
        provider_id("provider.wrong-attribution")?,
        1,
        ProviderMode::Observer,
        Arc::new(TestProvider::scripted(
            "provider.wrong-attribution",
            wrong_provider_reply,
        )?),
    )?;
    fabric.handshake(&handshake_request("provider.wrong-attribution")?)?;
    assert!(matches!(
        fabric.deliver_observation(&wrong_provider_call),
        Err(FabricError::ResponseProviderMismatch { expected, returned })
            if expected == "provider.wrong-attribution"
                && returned == "provider.other-attribution"
    ));

    let wrong_scope_call = call(
        "provider.wrong-scope",
        ProviderOperation::Observe,
        Some(DIGEST),
        &["observation.accept.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    assert_ne!(wrong_scope_call.exact_scope.exact_scope_sha256(), DIGEST);
    let wrong_scope_reply = ProviderReply {
        terminal: terminal(
            ProviderOperation::Observe,
            "provider.wrong-scope",
            TerminalCode::Success,
            CommittedEffectEvidence::none(Some(0)),
            FallbackDirective::forbidden(),
            &wrong_scope_call.operation_id,
            DIGEST,
            None,
        )?,
        payload: None,
        warnings: Vec::new(),
        extensions: Vec::new(),
        state_generation: 0,
    };
    fabric.register(
        provider_id("provider.wrong-scope")?,
        1,
        ProviderMode::Observer,
        Arc::new(TestProvider::scripted(
            "provider.wrong-scope",
            wrong_scope_reply,
        )?),
    )?;
    fabric.handshake(&handshake_request("provider.wrong-scope")?)?;
    assert!(matches!(
        fabric.deliver_observation(&wrong_scope_call),
        Err(FabricError::ResponseScopeMismatch { .. })
    ));

    let wrong_generation_call = call(
        "provider.wrong-generation",
        ProviderOperation::Observe,
        Some(DIGEST),
        &["observation.accept.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    let wrong_generation_reply = ProviderReply {
        terminal: terminal(
            ProviderOperation::Observe,
            "provider.wrong-generation",
            TerminalCode::Success,
            CommittedEffectEvidence::committed(0, 1, vec!["item-a".to_owned()], DIGEST, DIGEST)?,
            FallbackDirective::forbidden(),
            &wrong_generation_call.operation_id,
            &wrong_generation_call.exact_scope.exact_scope_sha256(),
            None,
        )?,
        payload: None,
        warnings: Vec::new(),
        extensions: Vec::new(),
        state_generation: 2,
    };
    fabric.register(
        provider_id("provider.wrong-generation")?,
        1,
        ProviderMode::Observer,
        Arc::new(TestProvider::scripted(
            "provider.wrong-generation",
            wrong_generation_reply,
        )?),
    )?;
    fabric.handshake(&handshake_request("provider.wrong-generation")?)?;
    assert_eq!(
        fabric.deliver_observation(&wrong_generation_call),
        Err(FabricError::ResponseStateGenerationMismatch {
            evidence: 1,
            reported: 2,
        })
    );

    let wrong_generation_before_call = call(
        "provider.wrong-generation-before",
        ProviderOperation::Observe,
        Some(DIGEST),
        &["observation.accept.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    let wrong_generation_before_reply = ProviderReply {
        terminal: terminal(
            ProviderOperation::Observe,
            "provider.wrong-generation-before",
            TerminalCode::Success,
            CommittedEffectEvidence::committed(1, 2, vec!["item-a".to_owned()], DIGEST, DIGEST)?,
            FallbackDirective::forbidden(),
            &wrong_generation_before_call.operation_id,
            &wrong_generation_before_call
                .exact_scope
                .exact_scope_sha256(),
            None,
        )?,
        payload: None,
        warnings: Vec::new(),
        extensions: Vec::new(),
        state_generation: 2,
    };
    fabric.register(
        provider_id("provider.wrong-generation-before")?,
        1,
        ProviderMode::Observer,
        Arc::new(TestProvider::scripted(
            "provider.wrong-generation-before",
            wrong_generation_before_reply,
        )?),
    )?;
    fabric.handshake(&handshake_request("provider.wrong-generation-before")?)?;
    assert_eq!(
        fabric.deliver_observation(&wrong_generation_before_call),
        Err(FabricError::ResponseStateGenerationMismatch {
            evidence: 1,
            reported: 0,
        })
    );

    let missing_generation_before_call = call(
        "provider.missing-generation-before",
        ProviderOperation::Observe,
        Some(DIGEST),
        &["observation.accept.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    let missing_generation_before_reply = ProviderReply {
        terminal: terminal(
            ProviderOperation::Observe,
            "provider.missing-generation-before",
            TerminalCode::EffectUnknown,
            CommittedEffectEvidence::unknown(DIGEST, "inspect-provider-journal")?,
            FallbackDirective::forbidden(),
            &missing_generation_before_call.operation_id,
            &missing_generation_before_call
                .exact_scope
                .exact_scope_sha256(),
            Some("diagnostic.effect-unknown"),
        )?,
        payload: None,
        warnings: Vec::new(),
        extensions: Vec::new(),
        state_generation: 0,
    };
    fabric.register(
        provider_id("provider.missing-generation-before")?,
        1,
        ProviderMode::Observer,
        Arc::new(TestProvider::scripted(
            "provider.missing-generation-before",
            missing_generation_before_reply,
        )?),
    )?;
    fabric.handshake(&handshake_request("provider.missing-generation-before")?)?;
    assert_eq!(
        fabric.deliver_observation(&missing_generation_before_call),
        Err(FabricError::ResponseStateGenerationBeforeMissing { expected: 0 })
    );

    Ok(())
}

#[test]
fn disabled_provider_and_wrong_revision_fail_before_provider_contact() -> Result<(), Box<dyn Error>>
{
    let fabric = MemoryFabric::new(FabricConfig::new(4, 2)?)?;
    let provider = Arc::new(TestProvider::new("provider.disabled", &[])?);
    fabric.register(
        provider_id("provider.disabled")?,
        1,
        ProviderMode::Disabled,
        provider.clone(),
    )?;
    assert!(matches!(
        fabric.handshake(&handshake_request("provider.disabled")?),
        Err(FabricError::ProviderDisabled(_))
    ));
    assert_eq!(provider.handshake_count(), 0);
    fabric.set_mode(&provider_id("provider.disabled")?, 1, ProviderMode::Active)?;
    let mut recall = call(
        "provider.disabled",
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    recall.registration_revision = 2;
    assert!(matches!(
        fabric.invoke_active(&recall),
        Err(FabricError::RegistrationRevisionMismatch {
            accepted: 1,
            requested: 2
        })
    ));
    assert_eq!(provider.invocation_count(), 0);
    Ok(())
}

#[test]
fn cancellation_and_deadline_are_terminal_before_provider_contact() -> Result<(), Box<dyn Error>> {
    let fabric = MemoryFabric::new(FabricConfig::new(4, 2)?)?;
    let provider = Arc::new(TestProvider::new("provider.controlled", &[])?);
    fabric.register(
        provider_id("provider.controlled")?,
        1,
        ProviderMode::Active,
        provider.clone(),
    )?;

    let handshake_cancellation = CancellationToken::new();
    handshake_cancellation.cancel();
    let mut cancelled_handshake = handshake_request("provider.controlled")?;
    cancelled_handshake.control = OperationControl::new(i64::MAX, 100, handshake_cancellation);
    assert_eq!(
        fabric.handshake(&cancelled_handshake),
        Err(FabricError::Cancelled)
    );
    let mut expired_handshake = handshake_request("provider.controlled")?;
    expired_handshake.control = OperationControl::new(i64::MAX, 0, CancellationToken::new());
    assert_eq!(
        fabric.handshake(&expired_handshake),
        Err(FabricError::DeadlineExceeded)
    );
    assert_eq!(provider.handshake_count(), 0);
    fabric.handshake(&handshake_request("provider.controlled")?)?;

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = call(
        "provider.controlled",
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(i64::MAX, 100, cancellation),
    )?;
    assert_eq!(
        fabric.invoke_active(&cancelled),
        Err(FabricError::Cancelled)
    );

    let expired = call(
        "provider.controlled",
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(i64::MAX, 0, CancellationToken::new()),
    )?;
    assert_eq!(
        fabric.invoke_active(&expired),
        Err(FabricError::DeadlineExceeded)
    );
    assert_eq!(provider.invocation_count(), 0);
    Ok(())
}

#[test]
fn statuses_are_deterministic_and_mode_changes_require_revision() -> Result<(), Box<dyn Error>> {
    let fabric = MemoryFabric::new(FabricConfig::new(4, 2)?)?;
    let provider_b = Arc::new(TestProvider::new("provider.b", &[])?);
    let provider_a = Arc::new(TestProvider::new("provider.a", &[])?);
    fabric.register(
        provider_id("provider.b")?,
        1,
        ProviderMode::Observer,
        provider_b,
    )?;
    fabric.register(
        provider_id("provider.a")?,
        1,
        ProviderMode::Disabled,
        provider_a,
    )?;
    assert!(matches!(
        fabric.set_mode(&provider_id("provider.a")?, 2, ProviderMode::Active),
        Err(FabricError::RegistrationRevisionMismatch { .. })
    ));
    fabric.set_mode(&provider_id("provider.a")?, 1, ProviderMode::Active)?;
    let statuses = fabric.statuses()?;
    assert_eq!(statuses.len(), 2);
    assert_eq!(statuses[0].provider_id.as_str(), "provider.a");
    assert_eq!(statuses[0].mode, ProviderMode::Active);
    assert_eq!(statuses[1].provider_id.as_str(), "provider.b");
    Ok(())
}
