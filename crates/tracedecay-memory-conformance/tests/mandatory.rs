//! End-to-end mandatory conformance journeys against a deterministic dummy provider.
#![allow(clippy::expect_used)]

use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};

use tracedecay_memory_conformance::{
    ConformanceError, DifferentialField, FixtureIdentity, MANDATORY_FIXTURE_BUILD_SHA256,
    MANDATORY_FIXTURE_ID, MandatoryConformanceHarness, MandatoryFixture, MandatoryScenario,
};
use tracedecay_memory_provider_api::contract::{
    CONTRACT_SET_ID, CONTRACT_SET_SHA256, CommittedEffectState, FallbackEligibility, TerminalCode,
};
use tracedecay_memory_provider_api::{
    ApiError, CancellationToken, CanonicalPayload, HandshakeRequest, HandshakeRequestParts,
    HandshakeResponse, MemoryProvider, OperationControl, OwnedExactScope, OwnedProviderId,
    OwnedVersionedId, ProviderCall, ProviderCallParts, ProviderDescriptor, ProviderLimits,
    ProviderOperation, ProviderReply, TerminalRecord,
};

const DUMMY_PROVIDER_ID: &str = "test.dummy";
const SCOPE_SHA: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const READY_SHA: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const IMPLEMENTATION_SHA: &str =
    "2222222222222222222222222222222222222222222222222222222222222222";
const RECEIPT_SHA: &str =
    "3333333333333333333333333333333333333333333333333333333333333333";
const HEALTH_PAYLOAD_SHA: &str =
    "4444444444444444444444444444444444444444444444444444444444444444";
const OBSERVE_PAYLOAD_SHA: &str =
    "5555555555555555555555555555555555555555555555555555555555555555";
const RECALL_PAYLOAD_SHA: &str =
    "6666666666666666666666666666666666666666666666666666666666666666";
const OTHER_IMPLEMENTATION_SHA: &str =
    "7777777777777777777777777777777777777777777777777777777777777777";

struct DummyProvider {
    descriptor: ProviderDescriptor,
    state_generation: AtomicU64,
}

impl DummyProvider {
    fn new() -> Result<Self, ApiError> {
        let capabilities = [
            "provider.health.v1",
            "observation.accept.v1",
            "recall.query.v1",
        ]
        .into_iter()
        .map(OwnedVersionedId::new)
        .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            descriptor: ProviderDescriptor::new(
                OwnedProviderId::new(DUMMY_PROVIDER_ID)?,
                IMPLEMENTATION_SHA,
                "dummy-state-v1",
                0,
                capabilities,
                limits(),
            )?,
            state_generation: AtomicU64::new(0),
        })
    }

    fn reply(&self, call: &ProviderCall) -> ProviderReply {
        let committed = call.operation == ProviderOperation::Observe;
        let state_generation = if committed {
            self.state_generation
                .fetch_add(1, Ordering::AcqRel)
                .saturating_add(1)
        } else {
            self.state_generation.load(Ordering::Acquire)
        };
        let committed_effect = if committed {
            CommittedEffectState::Committed
        } else {
            CommittedEffectState::None
        };
        ProviderReply {
            terminal: TerminalRecord::new(
                TerminalCode::Success,
                committed_effect,
                FallbackEligibility::Forbidden,
                call.operation_id.clone(),
                SCOPE_SHA,
                committed.then(|| RECEIPT_SHA.to_owned()),
                None,
            )
            .expect("dummy terminal"),
            payload: Some(call.payload.clone()),
            warnings: Vec::new(),
            extensions: Vec::new(),
            state_generation,
        }
    }
}

impl MemoryProvider for DummyProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        HandshakeResponse {
            terminal: TerminalRecord::new(
                TerminalCode::Success,
                CommittedEffectState::None,
                FallbackEligibility::Forbidden,
                request.request_id.clone(),
                SCOPE_SHA,
                None,
                None,
            )
            .expect("dummy handshake terminal"),
            descriptor: Some(self.descriptor.clone()),
            provider_instance_id: Some("dummy.instance-1".to_owned()),
            state_namespace: Some("dummy.scope-1".to_owned()),
            accepted_scope: Some(request.exact_scope.clone()),
            effective_limits: Some(request.host_limits.minimum(self.descriptor.limits)),
            ready_receipt_sha256: Some(READY_SHA.to_owned()),
            warnings: Vec::new(),
        }
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        self.reply(call)
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

fn scope() -> Result<OwnedExactScope, ApiError> {
    OwnedExactScope::new(
        "profile-a",
        "project-a",
        "repository-a",
        "worktree-a",
        "refs/heads/main",
        "session-a",
        3,
    )
}

fn call(
    provider_id: &str,
    exact_scope: OwnedExactScope,
    operation: ProviderOperation,
    expected_state_generation: u64,
    payload_sha256: &str,
    payload: &[u8],
) -> Result<ProviderCall, ApiError> {
    ProviderCall::new(ProviderCallParts {
        operation,
        provider_id: OwnedProviderId::new(provider_id)?,
        registration_revision: 1,
        ready_receipt_sha256: READY_SHA.to_owned(),
        exact_scope,
        request_id: format!("request-{}", operation.capability_id()),
        operation_id: format!("operation-{}", operation.capability_id()),
        expected_state_generation,
        idempotency_key: operation
            .mutates_provider_state()
            .then(|| "dummy-observe-idempotency".to_owned()),
        control: OperationControl::new(1_000, 500, CancellationToken::new()),
        payload: CanonicalPayload::new(
            OwnedVersionedId::new("tracedecay.memory.test-payload.v1")?,
            payload.to_vec(),
            payload_sha256,
        )?,
        required_capabilities: vec![OwnedVersionedId::new(operation.capability_id())?],
        extensions: Vec::new(),
    })
}

fn fixture_with_identity(
    identity: FixtureIdentity,
) -> Result<MandatoryFixture, Box<dyn Error>> {
    let exact_scope = scope()?;
    let handshake = HandshakeRequest::new(HandshakeRequestParts {
        provider_id: OwnedProviderId::new(DUMMY_PROVIDER_ID)?,
        registration_revision: 1,
        exact_scope: exact_scope.clone(),
        request_id: "dummy-handshake".to_owned(),
        required_capabilities: [
            "provider.health.v1",
            "observation.accept.v1",
            "recall.query.v1",
        ]
        .into_iter()
        .map(OwnedVersionedId::new)
        .collect::<Result<Vec<_>, _>>()?,
        host_limits: limits(),
        control: OperationControl::new(1_000, 500, CancellationToken::new()),
        challenge_nonce: [9; 32],
    })?;
    let scenarios = vec![
        MandatoryScenario::new(call(
            DUMMY_PROVIDER_ID,
            exact_scope.clone(),
            ProviderOperation::Health,
            0,
            HEALTH_PAYLOAD_SHA,
            b"{\"provider-secret-output\":\"health\"}",
        )?)?,
        MandatoryScenario::new(call(
            DUMMY_PROVIDER_ID,
            exact_scope.clone(),
            ProviderOperation::Observe,
            0,
            OBSERVE_PAYLOAD_SHA,
            b"{\"provider-secret-output\":\"observe\"}",
        )?)?,
        MandatoryScenario::new(call(
            DUMMY_PROVIDER_ID,
            exact_scope,
            ProviderOperation::Recall,
            1,
            RECALL_PAYLOAD_SHA,
            b"{\"provider-secret-output\":\"recall\"}",
        )?)?,
    ];
    Ok(MandatoryFixture::new(
        identity, SCOPE_SHA, handshake, scenarios,
    )?)
}

fn fixture() -> Result<MandatoryFixture, Box<dyn Error>> {
    fixture_with_identity(FixtureIdentity::mandatory(
        DUMMY_PROVIDER_ID,
        IMPLEMENTATION_SHA,
    )?)
}

#[test]
fn mandatory_conformance_runs_against_dummy_provider() -> Result<(), Box<dyn Error>> {
    let provider = DummyProvider::new()?;
    let fixture = fixture()?;
    let report = MandatoryConformanceHarness::new(&provider).run_product(&fixture)?;
    assert_eq!(report.identity.contract_set_id, CONTRACT_SET_ID);
    assert_eq!(report.identity.contract_set_sha256, CONTRACT_SET_SHA256);
    assert_eq!(report.identity.fixture_id, MANDATORY_FIXTURE_ID);
    assert_eq!(
        report.identity.fixture_build_sha256,
        MANDATORY_FIXTURE_BUILD_SHA256
    );
    assert_eq!(report.scenarios.len(), 3);
    assert_eq!(
        report.scenarios[0].operation,
        ProviderOperation::Health
    );
    assert_eq!(
        report.scenarios[1].reply.terminal.committed_effect,
        CommittedEffectState::Committed
    );
    assert_eq!(
        report.scenarios[2].operation,
        ProviderOperation::Recall
    );
    Ok(())
}

#[test]
fn exact_contract_and_fixture_build_identities_are_enforced() -> Result<(), Box<dyn Error>> {
    let wrong_contract = FixtureIdentity::new(
        "tracedecay.memory.provider.contract-set.v2",
        CONTRACT_SET_SHA256,
        MANDATORY_FIXTURE_ID,
        MANDATORY_FIXTURE_BUILD_SHA256,
        DUMMY_PROVIDER_ID,
        IMPLEMENTATION_SHA,
    );
    assert!(matches!(
        wrong_contract,
        Err(ConformanceError::ContractSetMismatch {
            field: "contract_set_id",
            ..
        })
    ));

    let valid = fixture()?;
    let wrong_build = FixtureIdentity::new(
        CONTRACT_SET_ID,
        CONTRACT_SET_SHA256,
        MANDATORY_FIXTURE_ID,
        SCOPE_SHA,
        DUMMY_PROVIDER_ID,
        IMPLEMENTATION_SHA,
    )?;
    let result = MandatoryFixture::new(
        wrong_build,
        valid.exact_scope_sha256,
        valid.handshake,
        valid.scenarios,
    );
    assert!(matches!(
        result,
        Err(ConformanceError::FixtureIdentityMismatch {
            field: "fixture_build_sha256",
            ..
        })
    ));
    Ok(())
}

#[test]
fn provider_and_implementation_identities_are_pinned() -> Result<(), Box<dyn Error>> {
    let mismatched_provider = FixtureIdentity::mandatory("vendor.memory", IMPLEMENTATION_SHA)?;
    let result = fixture_with_identity(mismatched_provider);
    assert!(matches!(
        result,
        Err(error) if error.downcast_ref::<ConformanceError>().is_some_and(|value| matches!(value, ConformanceError::ProviderIdentityMismatch { .. }))
    ));

    let provider = DummyProvider::new()?;
    let wrong_build_fixture = fixture_with_identity(FixtureIdentity::mandatory(
        DUMMY_PROVIDER_ID,
        OTHER_IMPLEMENTATION_SHA,
    )?)?;
    let result = MandatoryConformanceHarness::new(&provider).run_product(&wrong_build_fixture);
    assert!(matches!(
        result,
        Err(ConformanceError::ProviderImplementationMismatch { .. })
    ));
    Ok(())
}

#[test]
fn observer_report_has_no_provider_output_channel() -> Result<(), Box<dyn Error>> {
    let provider = DummyProvider::new()?;
    let observer = MandatoryConformanceHarness::new(&provider).run_observer(&fixture()?)?;
    assert_eq!(observer.scenarios.len(), 3);
    let rendered = format!("{observer:?}");
    for forbidden in [
        HEALTH_PAYLOAD_SHA,
        OBSERVE_PAYLOAD_SHA,
        RECALL_PAYLOAD_SHA,
        RECEIPT_SHA,
        "provider-secret-output",
        "dummy.scope-1",
    ] {
        assert!(!rendered.contains(forbidden));
    }
    Ok(())
}

#[test]
fn differential_report_is_typed_and_deterministic() -> Result<(), Box<dyn Error>> {
    let provider = DummyProvider::new()?;
    let left = MandatoryConformanceHarness::new(&provider).run_product(&fixture()?)?;
    let mut right = left.clone();
    right.scenarios[0].reply.state_generation = right.scenarios[0]
        .reply
        .state_generation
        .saturating_add(1);
    let differential = left.compare(&right);
    assert_eq!(differential.findings.len(), 1);
    assert_eq!(
        differential.findings[0].field,
        DifferentialField::StateGeneration
    );
    assert_eq!(
        differential.findings[0].operation,
        Some(ProviderOperation::Health)
    );
    Ok(())
}
