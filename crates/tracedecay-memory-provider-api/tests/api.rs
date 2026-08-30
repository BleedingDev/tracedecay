//! Behavioral contract tests for the provider-neutral runtime API.

use std::collections::BTreeSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracedecay_memory_provider_api::contract::{
    CommittedEffectExpectation, CommittedEffectState, FallbackEligibility, PROVIDER_LIMITS,
    TERMINAL_CODE_POLICIES, TerminalCode,
};
use tracedecay_memory_provider_api::{
    ApiError, CancellationToken, CanonicalPayload, CommittedEffectEvidence,
    CommittedEffectEvidenceParts, FallbackDirective, HandshakeRequest, HandshakeRequestParts,
    HandshakeResponse, MAX_COMMITTED_EFFECT_ITEM_REF_BYTES, MAX_COMMITTED_EFFECT_ITEM_REFS,
    MemoryProvider, OperationControl, OwnedExactScope, OwnedProviderId, OwnedVersionedId,
    PinnedFallbackPolicy, ProviderCall, ProviderCallParts, ProviderDescriptor, ProviderLimits,
    ProviderOperation, ProviderReply, TerminalRecord, UNKNOWN_EFFECT_RECONCILIATION_ACTION,
};

const DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const ALT_DIGEST: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

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

fn effect_for_state(state: CommittedEffectState) -> Result<CommittedEffectEvidence, ApiError> {
    match state {
        CommittedEffectState::None => Ok(CommittedEffectEvidence::none(Some(7))),
        CommittedEffectState::Committed => CommittedEffectEvidence::committed(
            7,
            8,
            vec!["item-committed".to_owned()],
            DIGEST,
            DIGEST,
        ),
        CommittedEffectState::Partial => CommittedEffectEvidence::partial(
            "after:item-committed",
            7,
            8,
            vec!["item-committed".to_owned()],
            vec!["item-uncommitted".to_owned()],
            DIGEST,
            "resume:item-uncommitted",
            DIGEST,
        ),
        CommittedEffectState::Unknown => {
            CommittedEffectEvidence::unknown(DIGEST, "reconcile:operation")
        }
    }
}

fn effect_is_allowed(expectation: CommittedEffectExpectation, state: CommittedEffectState) -> bool {
    match expectation {
        CommittedEffectExpectation::OperationSpecific
        | CommittedEffectExpectation::NoneOrOperationSpecific => {
            matches!(
                state,
                CommittedEffectState::None | CommittedEffectState::Committed
            )
        }
        CommittedEffectExpectation::None => state == CommittedEffectState::None,
        CommittedEffectExpectation::NonePartialOrUnknown => {
            state != CommittedEffectState::Committed
        }
        CommittedEffectExpectation::NoneOrUnknown => {
            matches!(
                state,
                CommittedEffectState::None | CommittedEffectState::Unknown
            )
        }
        CommittedEffectExpectation::Partial => state == CommittedEffectState::Partial,
        CommittedEffectExpectation::Unknown => state == CommittedEffectState::Unknown,
    }
}

fn diagnostic_for(code: TerminalCode) -> Option<String> {
    if matches!(
        code,
        TerminalCode::Success | TerminalCode::SuccessZeroResults | TerminalCode::Partial
    ) {
        None
    } else {
        Some("diagnostic.provider.v1".to_owned())
    }
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
            terminal: TerminalRecord::new(
                ProviderOperation::Handshake,
                self.descriptor.provider_id.clone(),
                TerminalCode::Success,
                CommittedEffectEvidence::none(Some(self.descriptor.state_generation)),
                FallbackDirective::forbidden(),
                request.request_id.clone(),
                DIGEST,
                None,
            )
            .expect("valid test handshake terminal"),
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
            terminal: TerminalRecord::new(
                call.operation,
                call.provider_id.clone(),
                TerminalCode::SuccessZeroResults,
                CommittedEffectEvidence::none(Some(self.descriptor.state_generation)),
                FallbackDirective::forbidden(),
                call.operation_id.clone(),
                DIGEST,
                None,
            )
            .expect("valid test invocation terminal"),
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
    assert_eq!(scope.validate(), Ok(()));
    let borrowed = scope.borrowed();
    assert_eq!(borrowed.project_id, "project-1");
    assert_eq!(borrowed.worktree_identity, "worktree-1");
    assert_eq!(borrowed.scope_revision, 7);
    let digest = scope.exact_scope_sha256();
    assert_eq!(
        digest,
        "aa2f1ac9c33a448fb824abf783a6d40ab52050d91bcc580d907e6b0a3303938e"
    );
    assert_eq!(digest, scope.clone().exact_scope_sha256());
    assert!(
        digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );

    let mut variants = Vec::new();
    let mut changed = scope.clone();
    changed.profile_id.push_str("-changed");
    variants.push(changed);
    let mut changed = scope.clone();
    changed.project_id.push_str("-changed");
    variants.push(changed);
    let mut changed = scope.clone();
    changed.repository_identity.push_str("-changed");
    variants.push(changed);
    let mut changed = scope.clone();
    changed.worktree_identity.push_str("-changed");
    variants.push(changed);
    let mut changed = scope.clone();
    changed.branch_identity.push_str("-changed");
    variants.push(changed);
    let mut changed = scope.clone();
    changed.agent_session_id.push_str("-changed");
    variants.push(changed);
    let mut changed = scope.clone();
    changed.scope_revision = 8;
    variants.push(changed);
    assert!(
        variants
            .iter()
            .all(|variant| variant.exact_scope_sha256() != digest)
    );

    let left = OwnedExactScope::new("ab", "c", "repo", "worktree", "branch", "session", 1)?;
    let right = OwnedExactScope::new("a", "bc", "repo", "worktree", "branch", "session", 1)?;
    assert_ne!(left.exact_scope_sha256(), right.exact_scope_sha256());
    Ok(())
}

#[test]
fn request_control_distinguishes_cancellation_and_deadline() {
    let cancellation = CancellationToken::new();
    let control = OperationControl::new(i64::MAX, 10, cancellation.clone());
    assert!(control.snapshot().is_ok());
    cancellation.cancel();
    assert_eq!(control.snapshot(), Err(TerminalCode::Cancelled));

    let expired = OperationControl::new(i64::MAX, 0, CancellationToken::new());
    assert_eq!(expired.snapshot(), Err(TerminalCode::DeadlineExceeded));

    let wall_clock_expired = OperationControl::new(1, 10, CancellationToken::new());
    assert_eq!(
        wall_clock_expired.snapshot(),
        Err(TerminalCode::DeadlineExceeded)
    );

    let decaying = OperationControl::new(i64::MAX, 10_000, CancellationToken::new());
    std::thread::sleep(Duration::from_millis(2));
    let snapshot = decaying.snapshot().expect("live decaying budget");
    assert!(snapshot.remaining_millis > 0);
    assert!(snapshot.remaining_millis < 10_000);

    let now_micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_micros()).unwrap_or(i64::MAX))
        .unwrap_or(i64::MAX);
    let short_wall_deadline = now_micros.saturating_add(20_000);
    let wall_capped = OperationControl::new(short_wall_deadline, 10_000, CancellationToken::new());
    let snapshot = wall_capped.snapshot().expect("live wall-capped budget");
    assert!(snapshot.remaining_millis > 0);
    assert!(snapshot.remaining_millis <= 20);
    assert!(wall_capped.remaining_millis() <= 20);
    std::thread::sleep(Duration::from_millis(25));
    assert_eq!(wall_capped.snapshot(), Err(TerminalCode::DeadlineExceeded));

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let both_terminal = OperationControl::new(1, 0, cancelled);
    assert_eq!(both_terminal.snapshot(), Err(TerminalCode::Cancelled));
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
fn descriptor_revalidates_public_boundary_fields() -> Result<(), ApiError> {
    let mut invalid = descriptor()?;
    invalid.implementation_identity_sha256.clear();
    assert_eq!(
        invalid.validate(),
        Err(ApiError::InvalidSha256("implementation_identity_sha256"))
    );

    let mut invalid = descriptor()?;
    invalid.state_schema_version.clear();
    assert_eq!(
        invalid.validate(),
        Err(ApiError::EmptyField("state_schema_version"))
    );

    let mut invalid = descriptor()?;
    invalid.protocol_minor = 1;
    assert_eq!(
        invalid.validate(),
        Err(ApiError::IncompatibleProtocol { major: 1, minor: 1 })
    );

    let mut invalid = descriptor()?;
    invalid.capabilities.remove(&capability("recall.query.v1")?);
    assert_eq!(
        invalid.validate(),
        Err(ApiError::MandatoryCapabilityMissing("recall.query.v1"))
    );

    let mut invalid = descriptor()?;
    invalid.limits.request_bytes = 0;
    assert_eq!(
        invalid.validate(),
        Err(ApiError::ZeroLimit("request_bytes"))
    );
    Ok(())
}

#[test]
fn provider_limits_enforce_every_canonical_bound() {
    let maximum = ProviderLimits {
        request_bytes: 16_777_216,
        response_bytes: 33_554_432,
        observation_batch_items: 4_096,
        recall_candidates: 10_000,
        concurrent_operations: 1_024,
        operation_millis: 3_600_000,
        snapshot_bytes: 1_073_741_824,
        inspection_items: 100_000,
    };
    assert_eq!(maximum.validate(), Ok(maximum));

    for catalog in PROVIDER_LIMITS {
        let mut candidate = maximum;
        let value = catalog.maximum.saturating_add(1);
        match catalog.limit_id {
            "request_bytes" => candidate.request_bytes = value,
            "response_bytes" => candidate.response_bytes = value,
            "observation_batch_items" => candidate.observation_batch_items = value,
            "recall_candidates" => candidate.recall_candidates = value,
            "concurrent_operations" => candidate.concurrent_operations = value,
            "operation_millis" => candidate.operation_millis = value,
            "snapshot_bytes" => candidate.snapshot_bytes = value,
            "inspection_items" => candidate.inspection_items = value,
            _ => continue,
        }
        assert_eq!(
            candidate.validate(),
            Err(ApiError::LimitExceedsMaximum {
                limit: catalog.limit_id,
                maximum: catalog.maximum,
            })
        );
    }
}

#[test]
fn provider_limits_reject_zero_for_every_catalog_row() {
    for catalog in PROVIDER_LIMITS {
        let mut candidate = limits();
        match catalog.limit_id {
            "request_bytes" => candidate.request_bytes = 0,
            "response_bytes" => candidate.response_bytes = 0,
            "observation_batch_items" => candidate.observation_batch_items = 0,
            "recall_candidates" => candidate.recall_candidates = 0,
            "concurrent_operations" => candidate.concurrent_operations = 0,
            "operation_millis" => candidate.operation_millis = 0,
            "snapshot_bytes" => candidate.snapshot_bytes = 0,
            "inspection_items" => candidate.inspection_items = 0,
            _ => continue,
        }
        assert_eq!(
            candidate.validate(),
            Err(ApiError::ZeroLimit(catalog.limit_id))
        );
    }
}

#[test]
fn handshake_and_call_revalidate_every_exact_scope_identity() -> Result<(), ApiError> {
    for field in [
        "profile_id",
        "project_id",
        "repository_identity",
        "worktree_identity",
        "branch_identity",
        "agent_session_id",
    ] {
        let mut invalid_scope = scope()?;
        match field {
            "profile_id" => invalid_scope.profile_id.clear(),
            "project_id" => invalid_scope.project_id.clear(),
            "repository_identity" => invalid_scope.repository_identity.clear(),
            "worktree_identity" => invalid_scope.worktree_identity.clear(),
            "branch_identity" => invalid_scope.branch_identity.clear(),
            "agent_session_id" => invalid_scope.agent_session_id.clear(),
            _ => continue,
        }
        assert_eq!(invalid_scope.validate(), Err(ApiError::EmptyField(field)));
        let handshake = HandshakeRequest::new(HandshakeRequestParts {
            provider_id: provider_id()?,
            registration_revision: 1,
            exact_scope: invalid_scope.clone(),
            request_id: "handshake-request".to_owned(),
            required_capabilities: vec![capability("provider.health.v1")?],
            host_limits: limits(),
            control: OperationControl::new(i64::MAX, 10, CancellationToken::new()),
            challenge_nonce: [7; 32],
        });
        assert!(matches!(handshake, Err(ApiError::EmptyField(actual)) if actual == field));

        let call = ProviderCall::new(ProviderCallParts {
            operation: ProviderOperation::Recall,
            provider_id: provider_id()?,
            registration_revision: 1,
            ready_receipt_sha256: DIGEST.to_owned(),
            exact_scope: invalid_scope,
            request_id: "request-invalid-scope".to_owned(),
            operation_id: "operation-invalid-scope".to_owned(),
            expected_state_generation: 0,
            idempotency_key: None,
            control: OperationControl::new(i64::MAX, 10, CancellationToken::new()),
            payload: payload()?,
            required_capabilities: vec![capability("recall.query.v1")?],
            extensions: Vec::new(),
        });
        assert!(matches!(call, Err(ApiError::EmptyField(actual)) if actual == field));
    }
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
        control: OperationControl::new(i64::MAX, 10, CancellationToken::new()),
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
        control: OperationControl::new(i64::MAX, 10, CancellationToken::new()),
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
        control: OperationControl::new(i64::MAX, 10, CancellationToken::new()),
        challenge_nonce: [7; 32],
    })?;
    let response = provider.handshake(&handshake);
    assert_eq!(response.terminal.terminal_code(), TerminalCode::Success);
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
        control: OperationControl::new(i64::MAX, 10, CancellationToken::new()),
        payload: payload()?,
        required_capabilities: vec![capability("recall.query.v1")?],
        extensions: Vec::new(),
    })?;
    let reply = provider.invoke(&call);
    assert_eq!(
        reply.terminal.terminal_code(),
        TerminalCode::SuccessZeroResults
    );
    assert_eq!(
        reply.terminal.fallback().eligibility(),
        FallbackEligibility::Forbidden
    );
    Ok(())
}

#[test]
fn committed_effect_factories_retain_borrowed_structured_evidence() -> Result<(), ApiError> {
    let none = CommittedEffectEvidence::none(None);
    assert_eq!(none.state(), CommittedEffectState::None);
    assert_eq!(none.state_generation_before(), None);
    assert_eq!(none.provider_receipt_sha256(), None);

    let committed = effect_for_state(CommittedEffectState::Committed)?;
    assert_eq!(committed.state_generation_before(), Some(7));
    assert_eq!(committed.state_generation_after(), Some(8));
    assert_eq!(committed.committed_item_refs(), &["item-committed"]);
    assert!(committed.uncommitted_item_refs().is_empty());
    assert_eq!(committed.provider_receipt_sha256(), Some(DIGEST));
    assert_eq!(committed.verification_sha256(), Some(DIGEST));

    let partial = effect_for_state(CommittedEffectState::Partial)?;
    assert_eq!(partial.committed_boundary(), Some("after:item-committed"));
    assert_eq!(
        partial.reconciliation_action(),
        Some("resume:item-uncommitted")
    );
    assert_eq!(
        partial.borrowed().uncommitted_item_refs,
        &["item-uncommitted"]
    );

    let unknown = effect_for_state(CommittedEffectState::Unknown)?;
    assert_eq!(unknown.state_generation_before(), None);
    assert_eq!(unknown.state_generation_after(), None);
    assert_eq!(unknown.committed_boundary(), None);
    assert_eq!(unknown.reconciliation_action(), Some("reconcile:operation"));
    assert_eq!(unknown.verification_sha256(), None);
    Ok(())
}

#[test]
fn committed_effect_rejects_generation_and_state_field_contradictions() {
    let none_with_one_generation =
        CommittedEffectEvidence::from_parts(CommittedEffectEvidenceParts {
            state: CommittedEffectState::None,
            committed_boundary: None,
            state_generation_before: Some(1),
            state_generation_after: None,
            committed_item_refs: Vec::new(),
            uncommitted_item_refs: Vec::new(),
            provider_receipt_sha256: None,
            reconciliation_action: None,
            verification_sha256: None,
        });
    assert_eq!(
        none_with_one_generation,
        Err(ApiError::InvalidEffectGenerations)
    );

    let none_with_receipt = CommittedEffectEvidence::from_parts(CommittedEffectEvidenceParts {
        state: CommittedEffectState::None,
        committed_boundary: None,
        state_generation_before: Some(1),
        state_generation_after: Some(1),
        committed_item_refs: Vec::new(),
        uncommitted_item_refs: Vec::new(),
        provider_receipt_sha256: Some(DIGEST.to_owned()),
        reconciliation_action: None,
        verification_sha256: None,
    });
    assert!(matches!(
        none_with_receipt,
        Err(ApiError::InvalidCommittedEffect(_))
    ));

    assert_eq!(
        CommittedEffectEvidence::committed(2, 1, Vec::new(), DIGEST, DIGEST),
        Err(ApiError::InvalidEffectGenerations)
    );

    let committed_with_uncommitted =
        CommittedEffectEvidence::from_parts(CommittedEffectEvidenceParts {
            state: CommittedEffectState::Committed,
            committed_boundary: None,
            state_generation_before: Some(1),
            state_generation_after: Some(2),
            committed_item_refs: vec!["done".to_owned()],
            uncommitted_item_refs: vec!["pending".to_owned()],
            provider_receipt_sha256: Some(DIGEST.to_owned()),
            reconciliation_action: None,
            verification_sha256: Some(DIGEST.to_owned()),
        });
    assert!(matches!(
        committed_with_uncommitted,
        Err(ApiError::InvalidCommittedEffect(_))
    ));

    let unknown_with_generation =
        CommittedEffectEvidence::from_parts(CommittedEffectEvidenceParts {
            state: CommittedEffectState::Unknown,
            committed_boundary: None,
            state_generation_before: Some(1),
            state_generation_after: None,
            committed_item_refs: Vec::new(),
            uncommitted_item_refs: Vec::new(),
            provider_receipt_sha256: Some(DIGEST.to_owned()),
            reconciliation_action: Some("reconcile".to_owned()),
            verification_sha256: None,
        });
    assert!(matches!(
        unknown_with_generation,
        Err(ApiError::InvalidCommittedEffect(_))
    ));
}

#[test]
fn partial_effect_requires_bounded_unique_disjoint_nonempty_partitions() {
    let overlap = CommittedEffectEvidence::partial(
        "boundary",
        1,
        2,
        vec!["same".to_owned()],
        vec!["same".to_owned()],
        DIGEST,
        "reconcile",
        DIGEST,
    );
    assert_eq!(
        overlap,
        Err(ApiError::OverlappingEffectItemRef("same".to_owned()))
    );

    let duplicate = CommittedEffectEvidence::committed(
        1,
        2,
        vec!["same".to_owned(), "same".to_owned()],
        DIGEST,
        DIGEST,
    );
    assert_eq!(
        duplicate,
        Err(ApiError::DuplicateEffectItemRef("same".to_owned()))
    );

    let empty_item = CommittedEffectEvidence::committed(1, 2, vec![String::new()], DIGEST, DIGEST);
    assert_eq!(
        empty_item,
        Err(ApiError::NonCanonicalTerminalText("effect_item_ref"))
    );

    let too_long = CommittedEffectEvidence::committed(
        1,
        2,
        vec!["x".repeat(MAX_COMMITTED_EFFECT_ITEM_REF_BYTES + 1)],
        DIGEST,
        DIGEST,
    );
    assert_eq!(
        too_long,
        Err(ApiError::TerminalTextTooLong {
            field: "effect_item_ref",
            maximum: MAX_COMMITTED_EFFECT_ITEM_REF_BYTES,
        })
    );

    let too_many = CommittedEffectEvidence::committed(
        1,
        2,
        (0..=MAX_COMMITTED_EFFECT_ITEM_REFS)
            .map(|index| format!("item-{index}"))
            .collect(),
        DIGEST,
        DIGEST,
    );
    assert_eq!(
        too_many,
        Err(ApiError::TooManyEffectItemRefs {
            maximum: MAX_COMMITTED_EFFECT_ITEM_REFS,
        })
    );

    let empty_partition = CommittedEffectEvidence::partial(
        "boundary",
        1,
        2,
        Vec::new(),
        vec!["pending".to_owned()],
        DIGEST,
        "resume",
        DIGEST,
    );
    assert!(matches!(
        empty_partition,
        Err(ApiError::InvalidCommittedEffect(_))
    ));
}

#[test]
fn fallback_requires_complete_distinct_pinned_host_policy() -> Result<(), ApiError> {
    let current = provider_id()?;
    assert_eq!(
        PinnedFallbackPolicy::new("memory.fallback.policy", 0, OwnedProviderId::new("other")?),
        Err(ApiError::InvalidFallbackPolicyRevision)
    );
    let same_target = PinnedFallbackPolicy::new("memory.fallback.policy", 1, current.clone())?;
    assert_eq!(
        FallbackDirective::explicit_policy_only(&current, same_target, "provider unavailable"),
        Err(ApiError::FallbackTargetMatchesCurrentProvider)
    );

    let pin = PinnedFallbackPolicy::new(
        "memory.fallback.policy",
        7,
        OwnedProviderId::new("alternate.provider")?,
    )?;
    assert_eq!(
        FallbackDirective::explicit_policy_only(&current, pin.clone(), ""),
        Err(ApiError::NonCanonicalTerminalText("fallback_reason"))
    );
    let directive = FallbackDirective::explicit_policy_only(
        &current,
        pin,
        "explicit host policy selected an alternate",
    )?;
    assert_eq!(
        directive.eligibility(),
        FallbackEligibility::ExplicitPolicyOnly
    );
    let borrowed = directive.borrowed();
    assert_eq!(
        borrowed.reason,
        Some("explicit host policy selected an alternate")
    );
    assert_eq!(
        borrowed.policy.map(|policy| policy.policy_revision),
        Some(7)
    );
    assert_eq!(
        borrowed.policy.map(|policy| policy.target_provider_id),
        Some("alternate.provider")
    );

    let forbidden = FallbackDirective::forbidden();
    assert_eq!(forbidden.policy(), None);
    assert_eq!(forbidden.reason(), None);
    Ok(())
}

#[test]
fn every_terminal_code_enforces_generated_effect_and_fallback_matrix() -> Result<(), ApiError> {
    let current = provider_id()?;
    for policy in TERMINAL_CODE_POLICIES {
        for state in [
            CommittedEffectState::None,
            CommittedEffectState::Committed,
            CommittedEffectState::Partial,
            CommittedEffectState::Unknown,
        ] {
            let result = TerminalRecord::new(
                ProviderOperation::Observe,
                current.clone(),
                policy.terminal_code,
                effect_for_state(state)?,
                FallbackDirective::forbidden(),
                "operation-matrix",
                DIGEST,
                diagnostic_for(policy.terminal_code),
            );
            assert_eq!(
                result.is_ok(),
                effect_is_allowed(policy.effect_expectation, state),
                "effect matrix drift for {} and {}",
                policy.terminal_code.as_wire(),
                state.as_wire()
            );
        }

        let pin = PinnedFallbackPolicy::new(
            "memory.fallback.policy",
            1,
            OwnedProviderId::new("alternate.provider")?,
        )?;
        let explicit =
            FallbackDirective::explicit_policy_only(&current, pin, "explicit test host policy")?;
        let explicit_result = TerminalRecord::new(
            ProviderOperation::Observe,
            current.clone(),
            policy.terminal_code,
            effect_for_state(CommittedEffectState::None)?,
            explicit,
            "operation-fallback-matrix",
            DIGEST,
            diagnostic_for(policy.terminal_code),
        );
        let expected = policy.maximum_fallback_eligibility
            == FallbackEligibility::ExplicitPolicyOnly
            && effect_is_allowed(policy.effect_expectation, CommittedEffectState::None);
        assert_eq!(
            explicit_result.is_ok(),
            expected,
            "fallback matrix drift for {}",
            policy.terminal_code.as_wire()
        );
    }
    Ok(())
}

#[test]
fn terminal_record_requires_failure_diagnostic_and_owns_receipt_once() -> Result<(), ApiError> {
    let no_diagnostic = TerminalRecord::new(
        ProviderOperation::Recall,
        provider_id()?,
        TerminalCode::InvalidRequest,
        CommittedEffectEvidence::none(None),
        FallbackDirective::forbidden(),
        "operation-invalid",
        DIGEST,
        None,
    );
    assert_eq!(no_diagnostic, Err(ApiError::MissingFailureDiagnostic));

    let record = TerminalRecord::new(
        ProviderOperation::Observe,
        provider_id()?,
        TerminalCode::Success,
        effect_for_state(CommittedEffectState::Committed)?,
        FallbackDirective::forbidden(),
        "operation-success",
        DIGEST,
        None,
    )?;
    assert_eq!(
        record.committed_effect().state(),
        CommittedEffectState::Committed
    );
    assert_eq!(
        record.fallback().eligibility(),
        FallbackEligibility::Forbidden
    );
    assert_eq!(record.provider_receipt_sha256(), Some(DIGEST));
    assert_eq!(
        record.borrowed().committed_effect.provider_receipt_digest,
        Some(DIGEST)
    );
    assert_eq!(record.operation(), ProviderOperation::Observe);
    assert_eq!(record.provider_id().as_str(), "test.provider");
    assert_eq!(record.borrowed().operation_kind, "observe");
    assert_eq!(record.borrowed().provider_id, "test.provider");
    Ok(())
}

#[test]
fn read_only_operations_reject_every_non_none_effect() -> Result<(), ApiError> {
    for operation in [
        ProviderOperation::Handshake,
        ProviderOperation::Health,
        ProviderOperation::Recall,
        ProviderOperation::Inspection,
        ProviderOperation::SnapshotExport,
    ] {
        let result = TerminalRecord::new(
            operation,
            provider_id()?,
            TerminalCode::Success,
            effect_for_state(CommittedEffectState::Committed)?,
            FallbackDirective::forbidden(),
            "operation-read-only",
            DIGEST,
            None,
        );
        assert_eq!(
            result,
            Err(ApiError::ReadOnlyOperationEffect {
                operation,
                effect_state: CommittedEffectState::Committed,
            })
        );
    }
    Ok(())
}

#[test]
fn explicit_fallback_is_bound_to_the_exact_terminal_provider() -> Result<(), ApiError> {
    let source = OwnedProviderId::new("source.provider")?;
    let target = OwnedProviderId::new("target.provider")?;
    let policy = PinnedFallbackPolicy::new("memory.fallback.policy", 1, target.clone())?;
    let fallback = FallbackDirective::explicit_policy_only(&source, policy, "host policy")?;
    assert_eq!(fallback.source_provider_id(), Some(&source));

    let laundered = TerminalRecord::new(
        ProviderOperation::Observe,
        target,
        TerminalCode::CapabilityUnsupported,
        CommittedEffectEvidence::none(Some(1)),
        fallback,
        "operation-fallback",
        DIGEST,
        Some("diagnostic.fallback.v1".to_owned()),
    );
    assert_eq!(laundered, Err(ApiError::FallbackSourceProviderMismatch));
    Ok(())
}

#[test]
fn terminal_text_is_bounded_and_rejects_whitespace_or_controls() -> Result<(), ApiError> {
    assert_eq!(
        PinnedFallbackPolicy::new("   ", 1, OwnedProviderId::new("target.provider")?),
        Err(ApiError::NonCanonicalTerminalText("fallback_policy_id"))
    );
    assert_eq!(
        PinnedFallbackPolicy::new(
            "x".repeat(
                tracedecay_memory_provider_api::contract::TERMINAL_FALLBACK_POLICY_ID_MAX_BYTES + 1
            ),
            1,
            OwnedProviderId::new("target.provider")?,
        ),
        Err(ApiError::TerminalTextTooLong {
            field: "fallback_policy_id",
            maximum:
                tracedecay_memory_provider_api::contract::TERMINAL_FALLBACK_POLICY_ID_MAX_BYTES,
        })
    );

    let invalid_boundary = CommittedEffectEvidence::partial(
        " boundary ",
        1,
        2,
        vec!["done".to_owned()],
        vec!["pending".to_owned()],
        DIGEST,
        "resume",
        DIGEST,
    );
    assert_eq!(
        invalid_boundary,
        Err(ApiError::NonCanonicalTerminalText("committed_boundary"))
    );

    let invalid_diagnostic = TerminalRecord::new(
        ProviderOperation::Recall,
        provider_id()?,
        TerminalCode::InvalidRequest,
        CommittedEffectEvidence::none(None),
        FallbackDirective::forbidden(),
        "operation-invalid",
        DIGEST,
        Some("\n".to_owned()),
    );
    assert_eq!(
        invalid_diagnostic,
        Err(ApiError::NonCanonicalTerminalText("diagnostic_id"))
    );
    Ok(())
}

#[test]
fn pre_dispatch_failure_factory_is_infallible_and_normalizes_mutated_call() -> Result<(), ApiError>
{
    let mut call = ProviderCall::new(ProviderCallParts {
        operation: ProviderOperation::Recall,
        provider_id: provider_id()?,
        registration_revision: 1,
        ready_receipt_sha256: DIGEST.to_owned(),
        exact_scope: scope()?,
        request_id: "request-factory".to_owned(),
        operation_id: "operation-factory".to_owned(),
        expected_state_generation: 7,
        idempotency_key: None,
        control: OperationControl::new(i64::MAX, 10, CancellationToken::new()),
        payload: payload()?,
        required_capabilities: vec![capability("recall.query.v1")?],
        extensions: Vec::new(),
    })?;
    let expected_scope = call.exact_scope.exact_scope_sha256();
    let normal = TerminalRecord::failure_before_dispatch_for_call(
        TerminalCode::ProviderUnavailable,
        &call,
        "diagnostic.provider-unavailable.v1",
    );
    assert_eq!(normal.terminal_code(), TerminalCode::ProviderUnavailable);
    assert_eq!(normal.operation(), ProviderOperation::Recall);
    assert_eq!(normal.provider_id().as_str(), "test.provider");
    assert_eq!(normal.operation_id(), "operation-factory");
    assert_eq!(normal.exact_scope_sha256(), expected_scope);
    assert_eq!(normal.committed_effect().state_generation_before(), Some(7));

    call.operation_id = " mutated\nsecret ".to_owned();
    call.exact_scope.project_id.clear();
    let malformed =
        TerminalRecord::failure_before_dispatch_for_call(TerminalCode::PartialEffect, &call, "   ");
    assert_eq!(malformed.terminal_code(), TerminalCode::InternalFailure);
    assert_eq!(
        malformed.operation_id(),
        "tracedecay.internal-failure.operation.v1"
    );
    assert_eq!(
        malformed.diagnostic_id(),
        Some("tracedecay.memory.provider.internal-failure.v1")
    );
    assert_ne!(malformed.exact_scope_sha256(), expected_scope);
    assert!(
        malformed
            .exact_scope_sha256()
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
    assert_eq!(
        malformed.committed_effect().state(),
        CommittedEffectState::None
    );
    Ok(())
}

#[test]
fn post_dispatch_internal_failure_preserves_validated_unknown_effect() -> Result<(), ApiError> {
    let call = ProviderCall::new(ProviderCallParts {
        operation: ProviderOperation::Observe,
        provider_id: provider_id()?,
        registration_revision: 1,
        ready_receipt_sha256: DIGEST.to_owned(),
        exact_scope: scope()?,
        request_id: "request-post-dispatch".to_owned(),
        operation_id: "operation-post-dispatch".to_owned(),
        expected_state_generation: 7,
        idempotency_key: Some("idempotency-post-dispatch".to_owned()),
        control: OperationControl::new(i64::MAX, 10, CancellationToken::new()),
        payload: payload()?,
        required_capabilities: vec![capability("observation.accept.v1")?],
        extensions: Vec::new(),
    })?;
    let terminal = TerminalRecord::internal_failure_for_call(
        &call,
        CommittedEffectEvidence::unknown(DIGEST, "reconcile-provider-effect")?,
        "diagnostic.internal.v1",
    );
    assert_eq!(terminal.terminal_code(), TerminalCode::InternalFailure);
    assert_eq!(
        terminal.committed_effect().state(),
        CommittedEffectState::Unknown
    );
    assert_eq!(terminal.provider_receipt_sha256(), Some(DIGEST));
    assert_eq!(
        terminal.committed_effect().reconciliation_action(),
        Some("reconcile-provider-effect")
    );

    let rebound = terminal
        .clone()
        .try_with_identity("operation-public", ALT_DIGEST)?;
    assert_eq!(rebound.operation_id(), "operation-public");
    assert_eq!(rebound.exact_scope_sha256(), ALT_DIGEST);
    assert_eq!(rebound.operation(), terminal.operation());
    assert_eq!(rebound.provider_id(), terminal.provider_id());
    assert_eq!(rebound.terminal_code(), terminal.terminal_code());
    assert_eq!(rebound.committed_effect(), terminal.committed_effect());
    assert_eq!(
        terminal.clone().try_with_identity(" ", ALT_DIGEST),
        Err(ApiError::NonCanonicalTerminalText("operation_id"))
    );
    assert_eq!(
        terminal.try_with_identity("operation-public", "not-a-digest"),
        Err(ApiError::InvalidSha256("exact_scope_sha256"))
    );
    Ok(())
}

#[test]
fn typed_reconciliation_digest_builds_equivalent_unknown_effect() -> Result<(), ApiError> {
    let typed = CommittedEffectEvidence::unknown_from_reconciliation_digest([0xab; 32]);
    let encoded = "ab".repeat(32);
    let general =
        CommittedEffectEvidence::unknown(encoded.clone(), UNKNOWN_EFFECT_RECONCILIATION_ACTION)?;
    assert_eq!(typed, general);
    assert_eq!(typed.provider_receipt_sha256(), Some(encoded.as_str()));
    assert_eq!(
        typed.reconciliation_action(),
        Some("reconcile.provider-effect.v1")
    );
    Ok(())
}

#[test]
fn effect_unknown_factory_preserves_mutating_uncertainty_and_rejects_false_read_effects()
-> Result<(), ApiError> {
    let mut call = ProviderCall::new(ProviderCallParts {
        operation: ProviderOperation::Observe,
        provider_id: provider_id()?,
        registration_revision: 1,
        ready_receipt_sha256: DIGEST.to_owned(),
        exact_scope: scope()?,
        request_id: "request-effect-unknown".to_owned(),
        operation_id: "operation-effect-unknown".to_owned(),
        expected_state_generation: 17,
        idempotency_key: Some("idempotency-effect-unknown".to_owned()),
        control: OperationControl::new(i64::MAX, 10, CancellationToken::new()),
        payload: payload()?,
        required_capabilities: vec![capability("observation.accept.v1")?],
        extensions: Vec::new(),
    })?;
    let terminal =
        TerminalRecord::effect_unknown_for_call(&call, [0xcd; 32], "diagnostic.effect-unknown.v1");
    assert_eq!(terminal.operation(), ProviderOperation::Observe);
    assert_eq!(terminal.provider_id(), &call.provider_id);
    assert_eq!(terminal.terminal_code(), TerminalCode::EffectUnknown);
    assert_eq!(terminal.operation_id(), call.operation_id);
    assert_eq!(
        terminal.exact_scope_sha256(),
        call.exact_scope.exact_scope_sha256()
    );
    assert_eq!(
        terminal.committed_effect(),
        &CommittedEffectEvidence::unknown_from_reconciliation_digest([0xcd; 32])
    );
    assert_eq!(
        terminal.fallback().eligibility(),
        FallbackEligibility::Forbidden
    );

    call.operation = ProviderOperation::Recall;
    call.idempotency_key = None;
    call.required_capabilities = BTreeSet::from([capability("recall.query.v1")?]);
    let read_only =
        TerminalRecord::effect_unknown_for_call(&call, [0xef; 32], "diagnostic.read-only.v1");
    assert_eq!(read_only.terminal_code(), TerminalCode::InternalFailure);
    assert_eq!(
        read_only.committed_effect(),
        &CommittedEffectEvidence::none(Some(17))
    );

    call.operation = ProviderOperation::Observe;
    call.idempotency_key = Some("idempotency-effect-unknown".to_owned());
    call.required_capabilities = BTreeSet::from([capability("observation.accept.v1")?]);
    call.exact_scope.project_id.clear();
    let malformed = TerminalRecord::effect_unknown_for_call(&call, [0xaa; 32], "   ");
    assert_eq!(malformed.terminal_code(), TerminalCode::InternalFailure);
    assert_eq!(
        malformed.committed_effect(),
        &CommittedEffectEvidence::none(Some(17))
    );
    assert_eq!(
        malformed.diagnostic_id(),
        Some("tracedecay.memory.provider.internal-failure.v1")
    );
    Ok(())
}

#[test]
fn post_dispatch_factory_handles_malformed_and_read_only_calls_without_losing_truth()
-> Result<(), ApiError> {
    let mut call = ProviderCall::new(ProviderCallParts {
        operation: ProviderOperation::Observe,
        provider_id: provider_id()?,
        registration_revision: 1,
        ready_receipt_sha256: DIGEST.to_owned(),
        exact_scope: scope()?,
        request_id: "request-post-dispatch-malformed".to_owned(),
        operation_id: "operation-post-dispatch-malformed".to_owned(),
        expected_state_generation: 11,
        idempotency_key: Some("idempotency-post-dispatch-malformed".to_owned()),
        control: OperationControl::new(i64::MAX, 10, CancellationToken::new()),
        payload: payload()?,
        required_capabilities: vec![capability("observation.accept.v1")?],
        extensions: Vec::new(),
    })?;
    call.operation_id = " malformed\nidentity ".to_owned();
    call.exact_scope.project_id.clear();
    let unknown = CommittedEffectEvidence::unknown(DIGEST, "reconcile-provider-effect")?;
    let malformed = TerminalRecord::internal_failure_for_call(&call, unknown.clone(), "   ");
    assert_eq!(malformed.terminal_code(), TerminalCode::InternalFailure);
    assert_eq!(malformed.committed_effect(), &unknown);
    assert_eq!(
        malformed.operation_id(),
        "tracedecay.internal-failure.operation.v1"
    );
    assert_eq!(
        malformed.diagnostic_id(),
        Some("tracedecay.memory.provider.internal-failure.v1")
    );

    call.operation = ProviderOperation::Recall;
    let read_only = TerminalRecord::internal_failure_for_call(&call, unknown, "diagnostic.read.v1");
    assert_eq!(read_only.operation(), ProviderOperation::Recall);
    assert_eq!(read_only.terminal_code(), TerminalCode::InternalFailure);
    assert_eq!(
        read_only.committed_effect(),
        &CommittedEffectEvidence::none(Some(11))
    );

    call.operation = ProviderOperation::Observe;
    let committed = CommittedEffectEvidence::committed(
        11,
        12,
        vec!["committed-item".to_owned()],
        DIGEST,
        ALT_DIGEST,
    )?;
    let degraded =
        TerminalRecord::internal_failure_for_call(&call, committed.clone(), "diagnostic.commit.v1");
    assert_eq!(degraded.terminal_code(), TerminalCode::Partial);
    assert_eq!(degraded.committed_effect(), &committed);
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
