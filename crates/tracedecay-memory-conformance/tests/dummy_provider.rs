//! Mandatory conformance, scenario, differential, and observer-isolation journeys.

use std::error::Error;

use tracedecay_memory_conformance::{
    ConformanceHarness, DifferentialReport, ExpectedCall, FixtureIdentity,
    MandatoryConformanceFixture, ObserverConformanceResult, ProductOutputDigest, ProviderScenario,
    ScenarioRunner,
};
use tracedecay_memory_provider_api::contract::{
    CommittedEffectState, FallbackEligibility, TerminalCode,
};
use tracedecay_memory_provider_api::{
    ApiError, CancellationToken, CanonicalPayload, HandshakeRequest, HandshakeRequestParts,
    HandshakeResponse, MemoryProvider, OperationControl, OwnedExactScope, OwnedProviderId,
    OwnedVersionedId, ProviderCall, ProviderCallParts, ProviderDescriptor, ProviderLimits,
    ProviderOperation, ProviderReply, TerminalRecord,
};

type TestResult = Result<(), Box<dyn Error>>;

const ZERO_SHA: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const ONE_SHA: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const PROVIDER_ID: &str = "dummy.memory";
const BUILD_ID: &str = "dummy-memory-build-2026-08-30";

#[derive(Clone)]
struct DummyProvider {
    descriptor: ProviderDescriptor,
    recall_terminal: TerminalCode,
}

impl DummyProvider {
    fn new(recall_terminal: TerminalCode) -> Result<Self, ApiError> {
        Ok(Self {
            descriptor: ProviderDescriptor::new(
                OwnedProviderId::new(PROVIDER_ID)?,
                ONE_SHA,
                "dummy-state-v1",
                1,
                [
                    OwnedVersionedId::new("provider.health.v1")?,
                    OwnedVersionedId::new("observation.accept.v1")?,
                    OwnedVersionedId::new("recall.query.v1")?,
                ],
                limits(),
            )?,
            recall_terminal,
        })
    }

    fn terminal(&self, call: &ProviderCall, code: TerminalCode) -> TerminalRecord {
        let committed_effect =
            if code == TerminalCode::Success && call.operation.mutates_provider_state() {
                CommittedEffectState::Committed
            } else {
                CommittedEffectState::None
            };
        TerminalRecord {
            terminal_code: code,
            committed_effect,
            fallback: FallbackEligibility::Forbidden,
            operation_id: call.operation_id.clone(),
            exact_scope_sha256: ZERO_SHA.to_owned(),
            provider_receipt_sha256: (committed_effect == CommittedEffectState::Committed)
                .then(|| ONE_SHA.to_owned()),
            diagnostic_id: (code != TerminalCode::Success)
                .then(|| "dummy.recall_failure".to_owned()),
        }
    }
}

impl MemoryProvider for DummyProvider {
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
                exact_scope_sha256: ZERO_SHA.to_owned(),
                provider_receipt_sha256: None,
                diagnostic_id: None,
            },
            descriptor: Some(self.descriptor.clone()),
            provider_instance_id: Some("dummy-instance-1".to_owned()),
            state_namespace: Some("dummy-namespace-1".to_owned()),
            accepted_scope: Some(request.exact_scope.clone()),
            effective_limits: Some(self.descriptor.limits),
            ready_receipt_sha256: Some(ZERO_SHA.to_owned()),
            warnings: Vec::new(),
        }
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        let terminal_code = if call.operation == ProviderOperation::Recall {
            self.recall_terminal
        } else {
            TerminalCode::Success
        };
        ProviderReply {
            terminal: self.terminal(call, terminal_code),
            payload: (terminal_code == TerminalCode::Success).then(|| call.payload.clone()),
            warnings: Vec::new(),
            extensions: Vec::new(),
            state_generation: 1,
        }
    }
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
        inspection_items: 32,
    }
}

fn exact_scope() -> Result<OwnedExactScope, ApiError> {
    OwnedExactScope::new(
        "profile-a",
        "project-a",
        "repo-a",
        "worktree-a",
        "refs/heads/main",
        "agent-session-a",
        1,
    )
}

fn call(operation: ProviderOperation) -> Result<ProviderCall, ApiError> {
    ProviderCall::new(ProviderCallParts {
        operation,
        provider_id: OwnedProviderId::new(PROVIDER_ID)?,
        registration_revision: 1,
        ready_receipt_sha256: ZERO_SHA.to_owned(),
        exact_scope: exact_scope()?,
        request_id: format!("request-{}", operation.capability_id()),
        operation_id: format!("operation-{}", operation.capability_id()),
        expected_state_generation: 1,
        idempotency_key: operation
            .mutates_provider_state()
            .then(|| format!("idempotency-{}", operation.capability_id())),
        control: OperationControl::new(1_000_000, 500, CancellationToken::new()),
        payload: CanonicalPayload::new(
            OwnedVersionedId::new("tracedecay.memory.test-payload.v1")?,
            format!("{{\"operation\":\"{}\"}}", operation.capability_id()).into_bytes(),
            ONE_SHA,
        )?,
        required_capabilities: vec![OwnedVersionedId::new(operation.capability_id())?],
        extensions: Vec::new(),
    })
}

fn fixture() -> Result<MandatoryConformanceFixture, Box<dyn Error>> {
    let identity = FixtureIdentity::new(
        OwnedVersionedId::new("tracedecay.memory-provider.contract-set.v1")?,
        ZERO_SHA,
        OwnedProviderId::new(PROVIDER_ID)?,
        BUILD_ID,
        ONE_SHA,
    )?;
    let handshake = HandshakeRequest::new(HandshakeRequestParts {
        provider_id: OwnedProviderId::new(PROVIDER_ID)?,
        registration_revision: 1,
        exact_scope: exact_scope()?,
        request_id: "handshake-dummy".to_owned(),
        required_capabilities: vec![
            OwnedVersionedId::new("provider.health.v1")?,
            OwnedVersionedId::new("observation.accept.v1")?,
            OwnedVersionedId::new("recall.query.v1")?,
        ],
        host_limits: limits(),
        control: OperationControl::new(1_000_000, 500, CancellationToken::new()),
        challenge_nonce: [7; 32],
    })?;
    Ok(MandatoryConformanceFixture::new(
        identity,
        handshake,
        TerminalCode::Success,
        ZERO_SHA,
        ExpectedCall::new(
            call(ProviderOperation::Health)?,
            TerminalCode::Success,
            ZERO_SHA,
        )?,
        ExpectedCall::new(
            call(ProviderOperation::Observe)?,
            TerminalCode::Success,
            ZERO_SHA,
        )?,
        ExpectedCall::new(
            call(ProviderOperation::Recall)?,
            TerminalCode::Success,
            ZERO_SHA,
        )?,
    )?)
}

fn check(condition: bool, message: &'static str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

#[test]
fn mandatory_suite_passes_against_dummy_provider() -> TestResult {
    let provider = DummyProvider::new(TerminalCode::Success)?;
    let fixture = fixture()?;
    let report = ConformanceHarness::run(&provider, &fixture);
    check(report.passed(), "mandatory dummy-provider suite failed")?;
    check(
        report.fixture_identity.provider_build_id == BUILD_ID,
        "fixture lost exact provider build identity",
    )?;
    check(
        report.cases.len() == 6,
        "mandatory suite did not emit all six cases",
    )
}

#[test]
fn differential_report_exposes_provider_outcome_mismatch() -> TestResult {
    let fixture = fixture()?;
    let scenario = ProviderScenario::new(
        "mandatory-round-trip",
        fixture.identity.clone(),
        vec![
            fixture.health.call.clone(),
            fixture.observation.call.clone(),
            fixture.recall.call.clone(),
        ],
    )?;
    let passing = DummyProvider::new(TerminalCode::Success)?;
    let failing = DummyProvider::new(TerminalCode::InternalFailure)?;
    let left = ScenarioRunner::run(&passing, &scenario);
    let right = ScenarioRunner::run(&failing, &scenario);
    let differential = DifferentialReport::compare(&left, &right);
    check(
        !differential.equivalent(),
        "differential report hid terminal mismatch",
    )?;
    check(
        differential.cases.iter().any(|case| !case.same_terminal),
        "differential report emitted no mismatching case",
    )
}

#[test]
fn observer_result_retains_only_product_digest_and_report() -> TestResult {
    let provider = DummyProvider::new(TerminalCode::Success)?;
    let fixture = fixture()?;
    let report = ConformanceHarness::run(&provider, &fixture);
    let observer = ObserverConformanceResult::new(ProductOutputDigest::new(ZERO_SHA)?, report);
    check(
        observer.product_output_digest().as_str() == ZERO_SHA,
        "observer result changed baseline product digest",
    )?;
    check(
        observer.conformance().passed(),
        "observer result changed conformance report",
    )
}
