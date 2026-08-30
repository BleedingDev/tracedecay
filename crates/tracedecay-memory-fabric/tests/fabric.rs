//! Behavioral tests for capability-driven memory-fabric orchestration.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tracedecay_memory_fabric::{
    FabricConfig, FabricError, MemoryFabric, ObserverReceipt, ProviderMode,
};
use tracedecay_memory_provider_api::contract::{
    CommittedEffectState, FallbackEligibility, TerminalCode,
};
use tracedecay_memory_provider_api::{
    ApiError, CancellationToken, CanonicalPayload, CommittedEffectEvidence, FallbackDirective,
    HandshakeRequest, HandshakeRequestParts, HandshakeResponse, MemoryProvider, OperationControl,
    OwnedExactScope, OwnedProviderId, OwnedVersionedId, PinnedFallbackPolicy, ProviderCall,
    ProviderCallParts, ProviderDescriptor, ProviderLimits, ProviderOperation, ProviderReply,
    TerminalRecord,
};

const DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

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
        DIGEST,
    )
}

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
    invocations: AtomicUsize,
    handshake_terminal: TerminalRecord,
    default_observe_reply: ProviderReply,
    default_recall_reply: ProviderReply,
    scripted_reply: Option<ProviderReply>,
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
        Ok(scripted)
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
            invocations: AtomicUsize::new(0),
            handshake_terminal,
            default_observe_reply,
            default_recall_reply,
            scripted_reply,
        })
    }

    fn invocation_count(&self) -> usize {
        self.invocations.load(Ordering::Acquire)
    }
}

impl MemoryProvider for TestProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        HandshakeResponse {
            terminal: self.handshake_terminal.clone(),
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
        self.invocations.fetch_add(1, Ordering::AcqRel);
        if let Some(reply) = &self.scripted_reply {
            return reply.clone();
        }
        if call.operation == ProviderOperation::Observe {
            self.default_observe_reply.clone()
        } else {
            self.default_recall_reply.clone()
        }
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
fn observer_delivery_is_structurally_isolated_from_active_output() -> Result<(), Box<dyn Error>> {
    let fabric = MemoryFabric::new(FabricConfig::new(4, 2)?)?;
    let provider = Arc::new(TestProvider::new("provider.observer", &[])?);
    fabric.register(
        provider_id("provider.observer")?,
        1,
        ProviderMode::Observer,
        provider.clone(),
    )?;
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
fn observer_receipt_preserves_all_committed_effect_shapes() -> Result<(), Box<dyn Error>> {
    let fabric = MemoryFabric::new(FabricConfig::new(8, 2)?)?;
    let scenarios = vec![
        (
            "provider.effect-none",
            TerminalCode::Success,
            CommittedEffectEvidence::none(Some(2)),
            2,
            None,
        ),
        (
            "provider.effect-committed",
            TerminalCode::Success,
            CommittedEffectEvidence::committed(
                3,
                4,
                vec!["item-a".to_owned(), "item-b".to_owned()],
                DIGEST,
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            )?,
            4,
            None,
        ),
        (
            "provider.effect-partial",
            TerminalCode::PartialEffect,
            CommittedEffectEvidence::partial(
                "items[0..1)",
                4,
                5,
                vec!["item-a".to_owned()],
                vec!["item-b".to_owned()],
                DIGEST,
                "resume-from:item-b",
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            )?,
            5,
            Some("diagnostic.partial-effect"),
        ),
        (
            "provider.effect-unknown",
            TerminalCode::EffectUnknown,
            CommittedEffectEvidence::unknown(DIGEST, "inspect-provider-journal")?,
            7,
            Some("diagnostic.effect-unknown"),
        ),
    ];
    let mut receipts = Vec::new();

    for (provider_name, code, effect, state_generation, diagnostic_id) in scenarios {
        let observe = call(
            provider_name,
            ProviderOperation::Observe,
            Some(DIGEST),
            &["observation.accept.v1"],
            OperationControl::new(i64::MAX, 100, CancellationToken::new()),
        )?;
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
        let receipt = fabric.deliver_observation(&observe)?;
        assert_eq!(receipt.terminal.committed_effect(), &expected_effect);
        receipts.push(receipt);
    }

    let none = receipts[0].terminal.committed_effect();
    assert_eq!(none.state(), CommittedEffectState::None);
    assert_eq!(none.state_generation_before(), Some(2));
    assert_eq!(none.state_generation_after(), Some(2));

    let committed = receipts[1].terminal.committed_effect();
    assert_eq!(committed.state(), CommittedEffectState::Committed);
    assert_eq!(committed.state_generation_before(), Some(3));
    assert_eq!(committed.state_generation_after(), Some(4));
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

    let unknown = receipts[3].terminal.committed_effect();
    assert_eq!(unknown.state(), CommittedEffectState::Unknown);
    assert_eq!(unknown.provider_receipt_sha256(), Some(DIGEST));
    assert_eq!(
        unknown.reconciliation_action(),
        Some("inspect-provider-journal")
    );
    assert_eq!(unknown.state_generation_after(), None);
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
            CommittedEffectEvidence::unknown(DIGEST, "reconcile-before-fallback")?,
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
    assert_eq!(
        fabric.deliver_observation(&wrong_generation_call),
        Err(FabricError::ResponseStateGenerationMismatch {
            evidence: 1,
            reported: 2,
        })
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
