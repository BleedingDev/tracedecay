//! Behavioral tests for capability-driven memory-fabric orchestration.

use std::error::Error;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use tracedecay_memory_fabric::{
    ActiveCallPlan, ActiveRoutingPolicy, DegradationCause, DegradationDecision,
    DegradationDeclinedReason, DegradationRule, FabricConfig, FabricError, FallbackDecision,
    FallbackDeclinedReason, FallbackRule, MemoryFabric, ObserverReceipt, PinnedDegradationPolicy,
    ProviderCapabilityAvailability, ProviderMode, ProviderReadiness, ReadyRouteTarget, RouteTarget,
    RoutedProviderIdentity, RoutingError,
};
use tracedecay_memory_provider_api::contract::{
    CommittedEffectState, FallbackEligibility, TerminalCode,
};
use tracedecay_memory_provider_api::{
    ApiError, CancellationToken, CanonicalPayload, CommittedEffectEvidence, FallbackDirective,
    HandshakeRequest, HandshakeRequestParts, HandshakeResponse, MemoryProvider, OperationControl,
    OwnedExactScope, OwnedOpaqueExtension, OwnedProviderId, OwnedVersionedId,
    PayloadSanitizationReceipt, PayloadSanitizationReceiptParts, PinnedFallbackPolicy,
    ProviderCall, ProviderCallParts, ProviderDescriptor, ProviderLimits, ProviderOperation,
    ProviderReply, SanitizationDisposition, TerminalRecord,
};

const DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SECOND_DIGEST: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const PAYLOAD_DIGEST: &str = "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a";
const RESOLVED_SCOPE_DIGEST: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";

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
        RESOLVED_SCOPE_DIGEST,
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
    .and_then(admitted)
}

/// Sanitizer revision these tests stand in for. The real revision is derived by
/// `tracedecay-memory-hygiene` from the canonical policy document; the fabric
/// only cares that a self-consistent receipt binds the dispatched payload.
const TEST_SANITIZER_REVISION: &str = "tracedecay.memory.observation.hygiene.v1+fabric-test";

/// Attaches the receipt the admitted hygiene pipeline mints for a payload it
/// read and left byte-identical. Observation dispatch fails closed without one.
fn admitted(call: ProviderCall) -> Result<ProviderCall, ApiError> {
    if call.operation != ProviderOperation::Observe {
        return Ok(call);
    }
    let receipt =
        PayloadSanitizationReceipt::new(PayloadSanitizationReceiptParts::accepted_unmodified(
            TEST_SANITIZER_REVISION,
            call.payload.sha256.clone(),
        ))?;
    Ok(call.with_sanitization(receipt))
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

#[test]
fn status_reports_declared_capabilities_as_not_ready_before_handshake() -> Result<(), Box<dyn Error>>
{
    let provider_name = "provider.status-before-handshake";
    let fabric = MemoryFabric::new(FabricConfig::new(1, 1)?)?;
    let provider = TestProvider::new(
        provider_name,
        &["feedback.record.v1", "vendor.extension.v1"],
    )?;
    fabric.register(
        provider_id(provider_name)?,
        1,
        ProviderMode::Active,
        Arc::new(provider),
    )?;

    let status = &fabric.statuses()?[0];
    assert_eq!(status.readiness, ProviderReadiness::NotReady);
    assert_eq!(status.effective_limits, None);
    assert_eq!(status.ready_receipt_sha256, None);
    assert_eq!(
        status.capability_availability("provider.health.v1"),
        ProviderCapabilityAvailability::SupportedNotReady
    );
    assert_eq!(
        status.capability_availability("feedback.record.v1"),
        ProviderCapabilityAvailability::SupportedNotReady
    );
    assert_eq!(
        status.capability_availability("correction.apply.v1"),
        ProviderCapabilityAvailability::Undeclared
    );
    assert_eq!(
        status.capability_availability("vendor.extension.v1"),
        ProviderCapabilityAvailability::DataUnavailable
    );
    Ok(())
}

#[test]
fn status_reflects_retained_handshake_limits_receipt_and_readiness() -> Result<(), Box<dyn Error>> {
    let provider_name = "provider.status-after-handshake";
    let fabric = MemoryFabric::new(FabricConfig::new(1, 1)?)?;
    let mut provider = TestProvider::new(provider_name, &["feedback.record.v1"])?;
    let mut provider_limits = limits();
    provider_limits.request_bytes = 2048;
    provider_limits.response_bytes = 4096;
    provider_limits.observation_batch_items = 4;
    provider_limits.recall_candidates = 8;
    provider.descriptor.limits = provider_limits;
    fabric.register(
        provider_id(provider_name)?,
        1,
        ProviderMode::Active,
        Arc::new(provider),
    )?;

    fabric.handshake(&handshake_request(provider_name)?)?;
    let status = &fabric.statuses()?[0];
    assert_eq!(status.readiness, ProviderReadiness::Ready);
    assert_eq!(status.effective_limits, Some(provider_limits));
    assert_eq!(status.ready_receipt_sha256.as_deref(), Some(DIGEST));
    assert_eq!(
        status.capability_availability("provider.health.v1"),
        ProviderCapabilityAvailability::SupportedReady
    );
    assert_eq!(
        status.capability_availability("feedback.record.v1"),
        ProviderCapabilityAvailability::SupportedReady
    );

    fabric.set_mode(&provider_id(provider_name)?, 1, ProviderMode::Disabled)?;
    let status = &fabric.statuses()?[0];
    assert_eq!(status.readiness, ProviderReadiness::NotReady);
    assert_eq!(status.effective_limits, None);
    assert_eq!(status.ready_receipt_sha256, None);
    assert_eq!(
        status.capability_availability("provider.health.v1"),
        ProviderCapabilityAvailability::SupportedNotReady
    );

    fabric.set_mode(&provider_id(provider_name)?, 1, ProviderMode::Active)?;
    let status = &fabric.statuses()?[0];
    assert_eq!(status.readiness, ProviderReadiness::NotReady);
    assert_eq!(status.effective_limits, None);
    Ok(())
}

#[test]
fn observation_delivery_fails_closed_without_a_bound_sanitization_receipt()
-> Result<(), Box<dyn Error>> {
    let fabric = MemoryFabric::new(FabricConfig::new(4, 1)?)?;
    let provider = Arc::new(TestProvider::new("provider.hygiene", &[])?);
    fabric.register(
        provider_id("provider.hygiene")?,
        1,
        ProviderMode::Observer,
        provider.clone(),
    )?;
    fabric.handshake(&handshake_request("provider.hygiene")?)?;

    let admitted_call = call(
        "provider.hygiene",
        ProviderOperation::Observe,
        Some(DIGEST),
        &["observation.accept.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    let receipt = admitted_call
        .sanitization()
        .ok_or_else(|| std::io::Error::other("the test helper must admit every observation"))?
        .clone();
    assert_eq!(receipt.disposition(), SanitizationDisposition::Accepted);

    // Strip the receipt by rebuilding the same envelope without one.
    let unsanitized = ProviderCall::new(ProviderCallParts {
        operation: admitted_call.operation,
        provider_id: admitted_call.provider_id.clone(),
        registration_revision: admitted_call.registration_revision,
        ready_receipt_sha256: admitted_call.ready_receipt_sha256.clone(),
        exact_scope: admitted_call.exact_scope.clone(),
        request_id: admitted_call.request_id.clone(),
        operation_id: admitted_call.operation_id.clone(),
        expected_state_generation: admitted_call.expected_state_generation,
        idempotency_key: admitted_call.idempotency_key.clone(),
        control: OperationControl::new(i64::MAX, 100, CancellationToken::new()),
        payload: admitted_call.payload.clone(),
        required_capabilities: admitted_call
            .required_capabilities
            .iter()
            .cloned()
            .collect(),
        extensions: admitted_call.extensions.clone(),
    })?;
    assert!(unsanitized.sanitization().is_none());
    assert!(matches!(
        fabric.deliver_observation(&unsanitized),
        Err(FabricError::Api(ApiError::UnsanitizedObservation))
    ));
    assert_eq!(
        provider.invocation_count(),
        0,
        "an unsanitized observation must never reach the provider"
    );

    // A well-formed receipt that describes different bytes is refused too.
    let unbound = unsanitized
        .clone()
        .with_sanitization(PayloadSanitizationReceipt::new(
            PayloadSanitizationReceiptParts::accepted_unmodified(
                TEST_SANITIZER_REVISION,
                SECOND_DIGEST,
            ),
        )?);
    assert!(matches!(
        fabric.deliver_observation(&unbound),
        Err(FabricError::Api(ApiError::SanitizationReceiptUnbound))
    ));
    assert_eq!(provider.invocation_count(), 0);

    // The gate runs before the concurrency permit is taken, so the single
    // permit this fabric owns is still available to a legitimate delivery.
    fabric.deliver_observation(&admitted_call)?;
    assert_eq!(provider.invocation_count(), 1);
    Ok(())
}

// --- Explicit routing and fallback policy -----------------------------------

/// Host plan standing in for the composition root: it builds the readiness
/// handshake and the recall call for whichever target the router names, so a
/// fallback target receives its own fresh handshake and a call bound to that
/// target's identity, receipt, and state generation.
struct RecallRoutePlan;

impl ActiveCallPlan for RecallRoutePlan {
    type Error = ApiError;

    fn handshake_request(&self, target: &RouteTarget) -> Result<HandshakeRequest, ApiError> {
        HandshakeRequest::new(HandshakeRequestParts {
            provider_id: target.provider_id.clone(),
            registration_revision: target.registration_revision,
            exact_scope: scope()?,
            request_id: "handshake-route".to_owned(),
            required_capabilities: vec![capability("recall.query.v1")?],
            host_limits: limits(),
            control: OperationControl::new(i64::MAX, 100, CancellationToken::new()),
            challenge_nonce: [5; 32],
        })
    }

    fn provider_call(&self, target: &ReadyRouteTarget) -> Result<ProviderCall, ApiError> {
        ProviderCall::new(ProviderCallParts {
            operation: ProviderOperation::Recall,
            provider_id: target.provider_id.clone(),
            registration_revision: target.registration_revision,
            ready_receipt_sha256: target.ready_receipt_sha256.clone(),
            exact_scope: scope()?,
            request_id: "request-recall.query.v1".to_owned(),
            operation_id: "operation-recall.query.v1".to_owned(),
            expected_state_generation: target.descriptor.state_generation,
            idempotency_key: None,
            control: OperationControl::new(i64::MAX, 100, CancellationToken::new()),
            payload: payload()?,
            required_capabilities: vec![capability("recall.query.v1")?],
            extensions: Vec::new(),
        })
    }
}

fn routing_policy(
    provider: &str,
    revision: u64,
    fallback: FallbackRule,
) -> Result<ActiveRoutingPolicy, Box<dyn Error>> {
    Ok(ActiveRoutingPolicy::new(
        provider_id(provider)?,
        revision,
        fallback,
    )?)
}

fn unavailable_recall_reply(
    provider: &str,
    fallback: FallbackDirective,
) -> Result<ProviderReply, ApiError> {
    Ok(ProviderReply {
        terminal: terminal(
            ProviderOperation::Recall,
            provider,
            TerminalCode::ProviderUnavailable,
            CommittedEffectEvidence::none(Some(0)),
            fallback,
            "operation-recall.query.v1",
            &scope()?.exact_scope_sha256(),
            Some("diagnostic.provider-unavailable"),
        )?,
        payload: None,
        warnings: Vec::new(),
        extensions: Vec::new(),
        state_generation: 0,
    })
}

#[test]
fn routing_policy_rejects_zero_revision_and_self_targeting_fallback() -> Result<(), Box<dyn Error>>
{
    assert!(matches!(
        ActiveRoutingPolicy::new(provider_id("provider.a")?, 0, FallbackRule::Forbidden),
        Err(tracedecay_memory_fabric::RoutingPolicyError::RegistrationRevisionZero)
    ));
    let self_target = PinnedFallbackPolicy::new("policy.loop", 1, provider_id("provider.a")?)?;
    assert!(matches!(
        ActiveRoutingPolicy::new(
            provider_id("provider.a")?,
            1,
            FallbackRule::ExplicitPinned(self_target)
        ),
        Err(tracedecay_memory_fabric::RoutingPolicyError::FallbackTargetMatchesActiveProvider)
    ));
    Ok(())
}

#[test]
fn degradation_requires_an_explicit_pinned_allowlist() -> Result<(), Box<dyn Error>> {
    let forbidden = routing_policy("provider.active", 1, FallbackRule::Forbidden)?;
    assert_eq!(
        forbidden.decide_degradation(DegradationCause::Unavailable),
        DegradationDecision::Declined(DegradationDeclinedReason::HostRuleForbidden)
    );

    let pinned = PinnedDegradationPolicy::new(
        "policy.recall.degradation",
        7,
        [DegradationCause::Unavailable],
    )?;
    let configured = ActiveRoutingPolicy::new_with_degradation(
        provider_id("provider.active")?,
        1,
        FallbackRule::Forbidden,
        DegradationRule::ExplicitPinned(pinned.clone()),
    )?;
    assert_eq!(
        configured.decide_degradation(DegradationCause::Unavailable),
        DegradationDecision::Allowed {
            policy: pinned.clone(),
        }
    );
    assert_eq!(
        configured.decide_degradation(DegradationCause::TimedOut),
        DegradationDecision::Declined(DegradationDeclinedReason::CauseNotAllowed {
            cause: DegradationCause::TimedOut,
            policy: pinned,
        })
    );
    assert!(matches!(
        PinnedDegradationPolicy::new(
            "policy.recall.degradation",
            7,
            [DegradationCause::Unavailable, DegradationCause::Unavailable],
        ),
        Err(
            tracedecay_memory_fabric::RoutingPolicyError::DuplicateDegradationCause(
                DegradationCause::Unavailable
            )
        )
    ));
    Ok(())
}

#[test]
fn routing_refuses_non_active_or_mismatched_registrations_before_any_contact()
-> Result<(), Box<dyn Error>> {
    let fabric = MemoryFabric::new(FabricConfig::new(4, 2)?)?;
    let observer = Arc::new(TestProvider::new("provider.observer", &[])?);
    fabric.register(
        provider_id("provider.observer")?,
        1,
        ProviderMode::Observer,
        observer.clone(),
    )?;
    let disabled = Arc::new(TestProvider::new("provider.disabled", &[])?);
    fabric.register(
        provider_id("provider.disabled")?,
        1,
        ProviderMode::Disabled,
        disabled.clone(),
    )?;
    let active = Arc::new(TestProvider::new("provider.active", &[])?);
    fabric.register(
        provider_id("provider.active")?,
        2,
        ProviderMode::Active,
        active.clone(),
    )?;

    // An observer registration is never selectable for product output, and
    // the refusal happens before any handshake or call reaches it.
    let Err(RoutingError::ProviderNotActive { provider_id, mode }) = fabric.route_active(
        &routing_policy("provider.observer", 1, FallbackRule::Forbidden)?,
        "recall.query.v1",
        &RecallRoutePlan,
    ) else {
        return Err("observer must be refused before contact".into());
    };
    assert_eq!(provider_id.as_str(), "provider.observer");
    assert_eq!(mode, ProviderMode::Observer);
    let Err(RoutingError::ProviderNotActive { mode, .. }) = fabric.route_active(
        &routing_policy("provider.disabled", 1, FallbackRule::Forbidden)?,
        "recall.query.v1",
        &RecallRoutePlan,
    ) else {
        return Err("disabled must be refused before contact".into());
    };
    assert_eq!(mode, ProviderMode::Disabled);
    assert!(matches!(
        fabric.route_active(
            &routing_policy("provider.missing", 1, FallbackRule::Forbidden)?,
            "recall.query.v1",
            &RecallRoutePlan,
        ),
        Err(RoutingError::ProviderNotRegistered { .. })
    ));
    let Err(RoutingError::RegistrationRevisionMismatch {
        configured,
        registered,
        ..
    }) = fabric.route_active(
        &routing_policy("provider.active", 1, FallbackRule::Forbidden)?,
        "recall.query.v1",
        &RecallRoutePlan,
    )
    else {
        return Err("stale pinned revision must be refused".into());
    };
    assert_eq!((configured, registered), (1, 2));
    let Err(RoutingError::CapabilityUndeclared { capability, .. }) = fabric.route_active(
        &routing_policy("provider.active", 2, FallbackRule::Forbidden)?,
        "recall.missing.v1",
        &RecallRoutePlan,
    ) else {
        return Err("undeclared capability must be refused".into());
    };
    assert_eq!(capability, "recall.missing.v1");
    for provider in [&observer, &disabled, &active] {
        assert_eq!(provider.handshake_count(), 0);
        assert_eq!(provider.invocation_count(), 0);
    }
    Ok(())
}

#[test]
fn routed_replies_name_their_provider_and_keep_zero_results_distinct_from_unavailable()
-> Result<(), Box<dyn Error>> {
    let fabric = MemoryFabric::new(FabricConfig::new(4, 2)?)?;
    let active = Arc::new(TestProvider::new("provider.active", &[])?);
    fabric.register(
        provider_id("provider.active")?,
        1,
        ProviderMode::Active,
        active.clone(),
    )?;
    let down = Arc::new(TestProvider::scripted(
        "provider.down",
        unavailable_recall_reply("provider.down", FallbackDirective::forbidden())?,
    )?);
    fabric.register(
        provider_id("provider.down")?,
        1,
        ProviderMode::Active,
        down.clone(),
    )?;

    let zero = fabric.route_active(
        &routing_policy("provider.active", 1, FallbackRule::Forbidden)?,
        "recall.query.v1",
        &RecallRoutePlan,
    )?;
    assert_eq!(zero.terminal_code(), TerminalCode::SuccessZeroResults);
    assert_eq!(
        zero.identity,
        RoutedProviderIdentity {
            provider_id: provider_id("provider.active")?,
            registration_revision: 1,
            provider_instance_id: "test.provider.instance-1".to_owned(),
        }
    );
    assert_eq!(zero.call.provider_id.as_str(), "provider.active");
    assert_eq!(zero.call.ready_receipt_sha256, DIGEST);
    assert_eq!(zero.fallback, FallbackDecision::NotApplicable);
    assert_eq!(active.handshake_count(), 1);
    assert_eq!(active.invocation_count(), 1);

    let unavailable = fabric.route_active(
        &routing_policy("provider.down", 1, FallbackRule::Forbidden)?,
        "recall.query.v1",
        &RecallRoutePlan,
    )?;
    assert_eq!(
        unavailable.terminal_code(),
        TerminalCode::ProviderUnavailable
    );
    assert_eq!(unavailable.identity.provider_id.as_str(), "provider.down");
    assert!(unavailable.reply.payload.is_none());
    assert_eq!(
        unavailable.fallback,
        FallbackDecision::Declined(FallbackDeclinedReason::DirectiveForbidden)
    );
    // The unavailable route never touched the healthy provider.
    assert_eq!(active.handshake_count(), 1);
    assert_eq!(active.invocation_count(), 1);
    Ok(())
}

#[test]
fn fallback_dispatches_only_under_the_matching_pinned_rule_with_a_fresh_target_handshake()
-> Result<(), Box<dyn Error>> {
    let fabric = MemoryFabric::new(FabricConfig::new(4, 2)?)?;
    let target = Arc::new(TestProvider::new("provider.fallback-target", &[])?);
    fabric.register(
        provider_id("provider.fallback-target")?,
        1,
        ProviderMode::Active,
        target.clone(),
    )?;
    let policy = PinnedFallbackPolicy::new(
        "policy.memory-failover",
        7,
        provider_id("provider.fallback-target")?,
    )?;
    let explicit_id = provider_id("provider.explicit")?;
    let explicit = Arc::new(TestProvider::scripted(
        "provider.explicit",
        unavailable_recall_reply(
            "provider.explicit",
            FallbackDirective::explicit_policy_only(
                &explicit_id,
                policy.clone(),
                "operator-approved provider outage policy",
            )?,
        )?,
    )?);
    fabric.register(explicit_id, 1, ProviderMode::Active, explicit.clone())?;

    // Default host rule: the provider's directive alone authorises nothing.
    let declined = fabric.route_active(
        &routing_policy("provider.explicit", 1, FallbackRule::Forbidden)?,
        "recall.query.v1",
        &RecallRoutePlan,
    )?;
    assert_eq!(declined.terminal_code(), TerminalCode::ProviderUnavailable);
    assert_eq!(declined.identity.provider_id.as_str(), "provider.explicit");
    assert_eq!(
        declined.fallback,
        FallbackDecision::Declined(FallbackDeclinedReason::HostRuleForbidden)
    );
    assert_eq!(explicit.invocation_count(), 1);
    assert_eq!(target.handshake_count(), 0);
    assert_eq!(target.invocation_count(), 0);

    // A pinned rule at another revision is a mismatch, not a near-enough.
    let stale_rule = PinnedFallbackPolicy::new(
        "policy.memory-failover",
        8,
        provider_id("provider.fallback-target")?,
    )?;
    let mismatched = fabric.route_active(
        &routing_policy(
            "provider.explicit",
            1,
            FallbackRule::ExplicitPinned(stale_rule.clone()),
        )?,
        "recall.query.v1",
        &RecallRoutePlan,
    )?;
    assert_eq!(
        mismatched.identity.provider_id.as_str(),
        "provider.explicit"
    );
    assert_eq!(
        mismatched.fallback,
        FallbackDecision::Declined(FallbackDeclinedReason::PolicyMismatch {
            directive: policy.clone(),
            configured: stale_rule,
        })
    );
    assert_eq!(explicit.invocation_count(), 2);
    assert_eq!(target.handshake_count(), 0);
    assert_eq!(target.invocation_count(), 0);

    // The identical pin dispatches exactly one fresh handshake and one call
    // against the target, and the reply is attributed to the target.
    let dispatched = fabric.route_active(
        &routing_policy(
            "provider.explicit",
            1,
            FallbackRule::ExplicitPinned(policy.clone()),
        )?,
        "recall.query.v1",
        &RecallRoutePlan,
    )?;
    assert_eq!(dispatched.terminal_code(), TerminalCode::SuccessZeroResults);
    assert_eq!(
        dispatched.identity.provider_id.as_str(),
        "provider.fallback-target"
    );
    assert_eq!(
        dispatched.call.provider_id.as_str(),
        "provider.fallback-target"
    );
    assert_eq!(
        dispatched.fallback,
        FallbackDecision::Dispatched {
            from: RoutedProviderIdentity {
                provider_id: provider_id("provider.explicit")?,
                registration_revision: 1,
                provider_instance_id: "test.provider.instance-1".to_owned(),
            },
            from_terminal_code: TerminalCode::ProviderUnavailable,
            policy: policy.clone(),
        }
    );
    assert_eq!(explicit.invocation_count(), 3);
    assert_eq!(target.handshake_count(), 1);
    assert_eq!(target.invocation_count(), 1);

    // A target demoted to observer is declined even under the matching pin.
    fabric.set_mode(
        &provider_id("provider.fallback-target")?,
        1,
        ProviderMode::Observer,
    )?;
    let demoted = fabric.route_active(
        &routing_policy("provider.explicit", 1, FallbackRule::ExplicitPinned(policy))?,
        "recall.query.v1",
        &RecallRoutePlan,
    )?;
    assert_eq!(demoted.identity.provider_id.as_str(), "provider.explicit");
    assert_eq!(
        demoted.fallback,
        FallbackDecision::Declined(FallbackDeclinedReason::TargetNotActive {
            target: provider_id("provider.fallback-target")?,
            mode: ProviderMode::Observer,
        })
    );
    assert_eq!(target.handshake_count(), 1);
    assert_eq!(target.invocation_count(), 1);
    Ok(())
}

/// Deterministic **FNV-1a** digest (not SHA-256) over an active reply's
/// complete `Debug` representation.
///
/// Scope, stated honestly: [`ProviderReply`] is the *upstream provider
/// envelope* this router returns, not TraceDecay's final product-visible
/// output. This digest is therefore a focused **router invariant** - the
/// bytes the fabric hands its caller do not change when an observer is
/// registered alongside - and nothing more. The product-output differential
/// (the final admitted recall result a request actually consumes, hashed
/// with SHA-256 with the observer enabled, disabled, and failing) lives one
/// layer up, in
/// `tracedecay-memory-provider-registry/tests/recall_port.rs`, where the
/// real recall port and its outcome type exist.
///
/// FNV-1a rather than SHA-256 because this crate's dependency-direction gate
/// fixes the exact allowlist of dev-dependencies it may declare, and a
/// router-invariant check does not need collision resistance: full
/// `assert_eq!` equality on the reply value is asserted alongside it.
fn product_output_digest(reply: &ProviderReply) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01B3;
    format!("{reply:?}")
        .as_bytes()
        .iter()
        .fold(FNV_OFFSET_BASIS, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
        })
}

/// tdmem-0903: product output is bit-identical whether or not an observer
/// registration exists alongside the active provider.
///
/// Two independent fabrics run the identical active call plan against
/// behaviorally identical active providers; one fabric additionally carries a
/// registered, actively-delivering observer. Active registrations are keyed
/// by provider ID in disjoint map entries with their own dispatch gate and
/// readiness state (`MemoryFabric::register`), so an observer's registration
/// and delivery cannot reach the active provider's entry through any typed
/// path this crate exposes. This test proves that structural claim against
/// real dispatch rather than asserting it from the type shape alone: the
/// [`product_output_digest`] of the active reply, and the full reply value,
/// match exactly between the two fabrics. This is a router invariant over
/// the provider envelope, not the product-output differential; see
/// [`product_output_digest`] for where that proof lives.
#[test]
fn active_provider_output_hash_is_identical_with_or_without_observer() -> Result<(), Box<dyn Error>>
{
    let solo_fabric = MemoryFabric::new(FabricConfig::new(4, 2)?)?;
    let solo_active = Arc::new(TestProvider::new("provider.active", &[])?);
    solo_fabric.register(
        provider_id("provider.active")?,
        1,
        ProviderMode::Active,
        solo_active.clone(),
    )?;
    solo_fabric.handshake(&handshake_request("provider.active")?)?;
    let solo_reply = solo_fabric.invoke_active(&call(
        "provider.active",
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?)?;

    let accompanied_fabric = MemoryFabric::new(FabricConfig::new(4, 2)?)?;
    let accompanied_active = Arc::new(TestProvider::new("provider.active", &[])?);
    accompanied_fabric.register(
        provider_id("provider.active")?,
        1,
        ProviderMode::Active,
        accompanied_active.clone(),
    )?;
    let observer = Arc::new(TestProvider::new("provider.observer", &[])?);
    accompanied_fabric.register(
        provider_id("provider.observer")?,
        1,
        ProviderMode::Observer,
        observer.clone(),
    )?;
    accompanied_fabric.handshake(&handshake_request("provider.active")?)?;
    accompanied_fabric.handshake(&handshake_request("provider.observer")?)?;
    // The observer actively delivers observations before the active call, so
    // this is not merely "an idle observer changed nothing" but "an observer
    // that did real work changed nothing".
    accompanied_fabric.deliver_observation(&call(
        "provider.observer",
        ProviderOperation::Observe,
        Some(DIGEST),
        &["observation.accept.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?)?;
    let accompanied_reply = accompanied_fabric.invoke_active(&call(
        "provider.active",
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?)?;

    assert_eq!(solo_reply, accompanied_reply);
    assert_eq!(
        product_output_digest(&solo_reply),
        product_output_digest(&accompanied_reply)
    );
    assert_eq!(solo_active.invocation_count(), 1);
    assert_eq!(accompanied_active.invocation_count(), 1);
    assert_eq!(observer.invocation_count(), 1);
    Ok(())
}

/// tdmem-0903: an observer's delivery failure never alters active provider
/// output, and an observer can never write canonical (active) state.
///
/// The observer's scripted reply misattributes its terminal to a different
/// operation, so `deliver_observation` fails closed with a typed
/// `FabricError` before any terminal is retained for the observer. The active
/// provider, registered under a disjoint provider ID in the same fabric, is
/// then invoked and its output hash is compared against a baseline fabric
/// that never registered an observer at all. An unchanged hash proves the
/// failed observer call left no trace reachable from the active path.
#[test]
fn observer_delivery_failure_does_not_alter_active_provider_output() -> Result<(), Box<dyn Error>> {
    let baseline_fabric = MemoryFabric::new(FabricConfig::new(4, 2)?)?;
    let baseline_active = Arc::new(TestProvider::new("provider.active", &[])?);
    baseline_fabric.register(
        provider_id("provider.active")?,
        1,
        ProviderMode::Active,
        baseline_active.clone(),
    )?;
    baseline_fabric.handshake(&handshake_request("provider.active")?)?;
    let baseline_reply = baseline_fabric.invoke_active(&call(
        "provider.active",
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?)?;

    let fabric = MemoryFabric::new(FabricConfig::new(4, 2)?)?;
    let active = Arc::new(TestProvider::new("provider.active", &[])?);
    fabric.register(
        provider_id("provider.active")?,
        1,
        ProviderMode::Active,
        active.clone(),
    )?;
    let failing_observe_call = call(
        "provider.failing-observer",
        ProviderOperation::Observe,
        Some(DIGEST),
        &["observation.accept.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    // Misattributed operation on the reply's own terminal: a provider defect
    // that must fail closed rather than be silently accepted as an observation.
    let failing_reply = ProviderReply {
        terminal: terminal(
            ProviderOperation::Recall,
            "provider.failing-observer",
            TerminalCode::SuccessZeroResults,
            CommittedEffectEvidence::none(Some(0)),
            FallbackDirective::forbidden(),
            &failing_observe_call.operation_id,
            &failing_observe_call.exact_scope.exact_scope_sha256(),
            None,
        )?,
        payload: None,
        warnings: Vec::new(),
        extensions: Vec::new(),
        state_generation: 0,
    };
    let failing_observer = Arc::new(TestProvider::scripted(
        "provider.failing-observer",
        failing_reply,
    )?);
    fabric.register(
        provider_id("provider.failing-observer")?,
        1,
        ProviderMode::Observer,
        failing_observer.clone(),
    )?;
    fabric.handshake(&handshake_request("provider.active")?)?;
    fabric.handshake(&handshake_request("provider.failing-observer")?)?;

    assert_eq!(
        fabric.deliver_observation(&failing_observe_call),
        Err(FabricError::ResponseOperationKindMismatch {
            expected: ProviderOperation::Observe,
            returned: ProviderOperation::Recall,
        })
    );
    // The failed observer delivery cannot write canonical active output: the
    // fabric never even offers an active-call path for an observer-mode
    // registration in the first place.
    assert!(matches!(
        fabric.invoke_active(&call(
            "provider.failing-observer",
            ProviderOperation::Recall,
            None,
            &["recall.query.v1"],
            OperationControl::new(i64::MAX, 100, CancellationToken::new()),
        )?),
        Err(FabricError::ProviderObserverOnly(provider)) if provider == "provider.failing-observer"
    ));

    let reply_after_observer_failure = fabric.invoke_active(&call(
        "provider.active",
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?)?;

    assert_eq!(baseline_reply, reply_after_observer_failure);
    assert_eq!(
        product_output_digest(&baseline_reply),
        product_output_digest(&reply_after_observer_failure)
    );
    assert_eq!(baseline_active.invocation_count(), 1);
    assert_eq!(active.invocation_count(), 1);
    assert_eq!(failing_observer.invocation_count(), 1);
    Ok(())
}

/// tdmem-0903: a blocking observer that has exhausted the observer admission
/// lane cannot make a valid active call fail.
///
/// Before the fix this test guards, `MemoryFabric` held one global
/// `PermitCounter` that `invoke_active` and `deliver_observation` both drew
/// from. With `max_in_flight = 1`, one observer sitting inside
/// `MemoryProvider::invoke` held the only permit, and an otherwise valid
/// active invocation returned `FabricError::CapacityExhausted` *before the
/// active provider was ever contacted* — the exact refutation of "observer
/// failures do not alter active provider behavior".
///
/// The proof is concurrent, not sequential: a real observer is parked inside
/// its provider call while the active invocation runs. The second observer's
/// rejection is what proves the observer lane really is exhausted at that
/// instant, so the active call's success cannot be explained by the lane
/// having quietly drained.
#[test]
fn blocked_observer_lane_cannot_starve_the_active_provider() -> Result<(), Box<dyn Error>> {
    // Baseline: the same active call with no observer registered anywhere.
    let baseline_fabric = MemoryFabric::new(FabricConfig::new(1, 1)?)?;
    let baseline_active = Arc::new(TestProvider::new("provider.active", &[])?);
    baseline_fabric.register(
        provider_id("provider.active")?,
        1,
        ProviderMode::Active,
        baseline_active.clone(),
    )?;
    baseline_fabric.handshake(&handshake_request("provider.active")?)?;
    let baseline_reply = baseline_fabric.invoke_active(&call(
        "provider.active",
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?)?;

    // One permit per lane: the tightest configuration production uses.
    let fabric = Arc::new(MemoryFabric::new(FabricConfig::new(4, 1)?)?);
    let active = Arc::new(TestProvider::new("provider.active", &[])?);
    let dispatch_barrier = Arc::new(Barrier::new(2));
    let blocking_observer = Arc::new(BlockingProvider {
        inner: TestProvider::new("provider.blocking-observer", &[])?,
        dispatch_barrier: Arc::clone(&dispatch_barrier),
    });
    let second_observer = Arc::new(TestProvider::new("provider.second-observer", &[])?);
    fabric.register(
        provider_id("provider.active")?,
        1,
        ProviderMode::Active,
        active.clone(),
    )?;
    fabric.register(
        provider_id("provider.blocking-observer")?,
        1,
        ProviderMode::Observer,
        blocking_observer,
    )?;
    fabric.register(
        provider_id("provider.second-observer")?,
        1,
        ProviderMode::Observer,
        second_observer.clone(),
    )?;
    fabric.handshake(&handshake_request("provider.active")?)?;
    fabric.handshake(&handshake_request("provider.blocking-observer")?)?;
    fabric.handshake(&handshake_request("provider.second-observer")?)?;

    let blocking_call = call(
        "provider.blocking-observer",
        ProviderOperation::Observe,
        Some(DIGEST),
        &["observation.accept.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?;
    let observer_fabric = Arc::clone(&fabric);
    let parked_observer =
        thread::spawn(move || observer_fabric.deliver_observation(&blocking_call));
    // The observer is now inside its provider call, holding an observer permit.
    dispatch_barrier.wait();

    // The observer lane is genuinely exhausted at this instant: a second,
    // independently gated observer is refused.
    let starved_observer = fabric.deliver_observation(&call(
        "provider.second-observer",
        ProviderOperation::Observe,
        Some(DIGEST),
        &["observation.accept.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?);

    // ...and the active provider is nevertheless contacted exactly once and
    // returns byte-identical baseline output.
    let reply_under_observer_pressure = fabric.invoke_active(&call(
        "provider.active",
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?)?;

    dispatch_barrier.wait();
    let parked_result = parked_observer.join();
    assert!(parked_result.is_ok());
    if let Ok(result) = parked_result {
        result?;
    }

    assert_eq!(starved_observer, Err(FabricError::CapacityExhausted));
    assert_eq!(second_observer.invocation_count(), 0);
    assert_eq!(baseline_reply, reply_under_observer_pressure);
    assert_eq!(
        product_output_digest(&baseline_reply),
        product_output_digest(&reply_under_observer_pressure)
    );
    assert_eq!(active.invocation_count(), 1);
    assert_eq!(baseline_active.invocation_count(), 1);
    Ok(())
}

/// A counted canonical-state write port.
///
/// `writes` is incremented, and the rolling state digest advanced, only by
/// [`Self::apply_write`] — the single path through which anything in this
/// test can change canonical state. An assertion that the count is zero and
/// the digest unchanged is therefore a mechanical statement that no write was
/// attempted, not an inference from a return value.
struct CanonicalStateAuthority {
    writes: AtomicUsize,
    state_digest: AtomicU64,
}

impl CanonicalStateAuthority {
    const EMPTY_DIGEST: u64 = 0xcbf2_9ce4_8422_2325;
    const DIGEST_PRIME: u64 = 0x0000_0100_0000_01B3;

    fn new() -> Self {
        Self {
            writes: AtomicUsize::new(0),
            state_digest: AtomicU64::new(Self::EMPTY_DIGEST),
        }
    }

    /// The only mutation path. Advances the canonical generation and folds the
    /// mutation's identity into the canonical state digest.
    fn apply_write(&self, operation: ProviderOperation, operation_id: &str) {
        self.writes.fetch_add(1, Ordering::AcqRel);
        let mut digest = self.state_digest.load(Ordering::Acquire);
        for byte in operation.as_wire().bytes().chain(operation_id.bytes()) {
            digest = (digest ^ u64::from(byte)).wrapping_mul(Self::DIGEST_PRIME);
        }
        self.state_digest.store(digest, Ordering::Release);
    }

    fn write_count(&self) -> usize {
        self.writes.load(Ordering::Acquire)
    }

    fn state_digest(&self) -> u64 {
        self.state_digest.load(Ordering::Acquire)
    }
}

/// Whether an operation would write canonical state if it reached a provider.
const fn writes_canonical_state(operation: ProviderOperation) -> bool {
    matches!(
        operation,
        ProviderOperation::Correction
            | ProviderOperation::DeleteBySource
            | ProviderOperation::SnapshotRestore
            | ProviderOperation::Replay
    )
}

/// A provider whose canonical-authority write port is contacted by every
/// mutating operation that actually reaches it, and by nothing else.
struct CanonicalWritingProvider {
    inner: TestProvider,
    authority: Arc<CanonicalStateAuthority>,
}

impl CanonicalWritingProvider {
    fn new(provider: &str, authority: Arc<CanonicalStateAuthority>) -> Result<Self, ApiError> {
        Ok(Self {
            inner: TestProvider::new(
                provider,
                &[
                    "correction.apply.v1",
                    "deletion.by_source.v1",
                    "snapshot.restore.v1",
                    "replay.apply.v1",
                ],
            )?,
            authority,
        })
    }
}

impl MemoryProvider for CanonicalWritingProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        self.inner.descriptor()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        self.inner.handshake(request)
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        if !writes_canonical_state(call.operation) {
            return self.inner.invoke(call);
        }
        self.authority
            .apply_write(call.operation, &call.operation_id);
        let committed = CommittedEffectEvidence::committed(
            0,
            1,
            vec![call.operation_id.clone()],
            DIGEST,
            DIGEST,
        );
        match committed.and_then(|effect| {
            terminal(
                call.operation,
                self.inner.descriptor().provider_id.as_str(),
                TerminalCode::Success,
                effect,
                FallbackDirective::forbidden(),
                &call.operation_id,
                &call.exact_scope.exact_scope_sha256(),
                None,
            )
        }) {
            Ok(reply_terminal) => ProviderReply {
                terminal: reply_terminal,
                payload: None,
                warnings: Vec::new(),
                extensions: Vec::new(),
                state_generation: 1,
            },
            Err(_) => self.inner.invoke(call),
        }
    }
}

/// tdmem-0903: an observer registration cannot reach a canonical-state write
/// port, and canonical state is provably unchanged after every attempt.
///
/// The previous proof for this acceptance criterion only attempted
/// [`ProviderOperation::Recall`], which the API classifies as advisory and
/// read-only, so it demonstrated nothing about writes. This test attempts
/// every operation the provider API defines as mutating canonical state
/// (`correction`, `deletion_by_source`, `snapshot_restore`, `replay`) from an
/// Observer registration, over both routes the fabric exposes:
///
/// * `invoke_active` — refused with `ProviderObserverOnly`, the observer-mode
///   gate, before provider contact;
/// * `deliver_observation` — refused with `OperationNotObservation`, because
///   the observation route accepts only `observation.accept.v1`, so a
///   mutating operation cannot be smuggled through the one route an observer
///   *is* allowed to use.
///
/// Both the write counter and the canonical state digest are asserted
/// unchanged after every attempt. The final section is the positive control
/// that keeps those assertions from passing vacuously: the identical provider
/// registered as Active writes exactly once and moves the digest, proving the
/// write port is genuinely reachable when the mode gate permits it.
#[test]
fn observer_cannot_reach_the_canonical_state_write_port() -> Result<(), Box<dyn Error>> {
    const MUTATIONS: [ProviderOperation; 4] = [
        ProviderOperation::Correction,
        ProviderOperation::DeleteBySource,
        ProviderOperation::SnapshotRestore,
        ProviderOperation::Replay,
    ];

    let observer_authority = Arc::new(CanonicalStateAuthority::new());
    let quiescent_digest = observer_authority.state_digest();
    let fabric = MemoryFabric::new(FabricConfig::new(2, 2)?)?;
    let observer = Arc::new(CanonicalWritingProvider::new(
        "provider.observer",
        Arc::clone(&observer_authority),
    )?);
    fabric.register(
        provider_id("provider.observer")?,
        1,
        ProviderMode::Observer,
        observer,
    )?;
    fabric.handshake(&handshake_request("provider.observer")?)?;

    for operation in MUTATIONS {
        let mutation = call(
            "provider.observer",
            operation,
            Some(DIGEST),
            &[operation.capability_id()],
            OperationControl::new(i64::MAX, 100, CancellationToken::new()),
        )?;
        assert_eq!(
            fabric.invoke_active(&mutation),
            Err(FabricError::ProviderObserverOnly(
                "provider.observer".to_owned()
            )),
            "active route admitted {} from an observer",
            operation.as_wire()
        );
        assert_eq!(
            fabric.deliver_observation(&mutation),
            Err(FabricError::OperationNotObservation),
            "observation route admitted {} from an observer",
            operation.as_wire()
        );
        assert_eq!(
            observer_authority.write_count(),
            0,
            "canonical write port was contacted for {}",
            operation.as_wire()
        );
        assert_eq!(
            observer_authority.state_digest(),
            quiescent_digest,
            "canonical state digest moved for {}",
            operation.as_wire()
        );
    }

    // Positive control: the same provider, the same call, an Active
    // registration. The write port is reached exactly once and canonical
    // state moves, so the zero-write assertions above are load-bearing.
    let active_authority = Arc::new(CanonicalStateAuthority::new());
    let active_fabric = MemoryFabric::new(FabricConfig::new(2, 2)?)?;
    let active = Arc::new(CanonicalWritingProvider::new(
        "provider.observer",
        Arc::clone(&active_authority),
    )?);
    active_fabric.register(
        provider_id("provider.observer")?,
        1,
        ProviderMode::Active,
        active,
    )?;
    active_fabric.handshake(&handshake_request("provider.observer")?)?;
    active_fabric.invoke_active(&call(
        "provider.observer",
        ProviderOperation::Correction,
        Some(DIGEST),
        &["correction.apply.v1"],
        OperationControl::new(i64::MAX, 100, CancellationToken::new()),
    )?)?;
    assert_eq!(active_authority.write_count(), 1);
    assert_ne!(active_authority.state_digest(), quiescent_digest);
    Ok(())
}
