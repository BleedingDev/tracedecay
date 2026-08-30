//! Executable conformance against the canonical deterministic M1 dummy provider.

use std::error::Error;
use std::io;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

use sha2::{Digest, Sha256};
use tracedecay_memory_conformance::{
    ConformanceStatus, ContractIdentity, DifferentialReport, EvaluationError, FixtureIdentity,
    ProviderBuildIdentity, ProviderHarness, RequestControlFixture, ScenarioFixture,
    mandatory_conformance_fixture,
};
use tracedecay_memory_provider_api::contract::{
    CommittedEffectState, FallbackEligibility, TerminalCode,
};

#[allow(dead_code)]
#[path = "../../../product/conformance/dummy-provider/src/lib.rs"]
mod canonical_dummy;
use tracedecay_memory_provider_api::{
    ApiError, CanonicalPayload, CommittedEffectEvidence, FallbackDirective, HandshakeRequest,
    HandshakeResponse, MemoryProvider, OwnedExactScope, OwnedProviderId, OwnedVersionedId,
    PinnedFallbackPolicy, ProviderCall, ProviderDescriptor, ProviderLimits, ProviderOperation,
    ProviderReply, TerminalRecord,
};

const BUILD_DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const READY_RECEIPT: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const EFFECT_RECEIPT: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
const OBSERVATION_PAYLOAD_SHA256: &str =
    "e2cb333c1f9ac5b0285bd10fd6844c1a578b9b4701c77a438cb332a50c0142c1";
const REGISTRATION_REVISION: u64 = 41;

struct DummyMemoryProviderAdapter {
    inner: Mutex<canonical_dummy::DummyProvider>,
    descriptor: ProviderDescriptor,
    limits: ProviderLimits,
    scope_digest: String,
    handshake_calls: AtomicUsize,
    invoke_calls: AtomicUsize,
    last_registration_revision: AtomicU64,
}

impl DummyMemoryProviderAdapter {
    fn new() -> Result<Self, Box<dyn Error>> {
        Self::new_for_scope(&exact_scope()?)
    }

    fn new_for_scope(exact_scope: &OwnedExactScope) -> Result<Self, Box<dyn Error>> {
        let provider_id = OwnedProviderId::new("test.canonical-dummy")?;
        let scope_digest = exact_scope.exact_scope_sha256();
        let inner = canonical_dummy::DummyProvider::new(provider_id.as_str(), &scope_digest)
            .map_err(io::Error::other)?;
        let limits = limits();
        let descriptor = ProviderDescriptor::new(
            provider_id,
            BUILD_DIGEST,
            "canonical-dummy.v1",
            0,
            mandatory_capabilities()?,
            limits,
        )?;
        Ok(Self {
            inner: Mutex::new(inner),
            descriptor,
            limits,
            scope_digest,
            handshake_calls: AtomicUsize::new(0),
            invoke_calls: AtomicUsize::new(0),
            last_registration_revision: AtomicU64::new(0),
        })
    }

    fn handshake_calls(&self) -> usize {
        self.handshake_calls.load(Ordering::SeqCst)
    }

    fn invoke_calls(&self) -> usize {
        self.invoke_calls.load(Ordering::SeqCst)
    }

    fn last_registration_revision(&self) -> u64 {
        self.last_registration_revision.load(Ordering::SeqCst)
    }

    fn lock(&self) -> MutexGuard<'_, canonical_dummy::DummyProvider> {
        match self.inner.lock() {
            Ok(provider) => provider,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn terminal(
        &self,
        operation: ProviderOperation,
        terminal_code: TerminalCode,
        committed_effect: CommittedEffectEvidence,
        operation_id: &str,
        diagnostic_id: Option<String>,
    ) -> TerminalRecord {
        validated_terminal(TerminalRecord::new(
            operation,
            self.descriptor.provider_id.clone(),
            terminal_code,
            committed_effect,
            FallbackDirective::forbidden(),
            operation_id,
            &self.scope_digest,
            diagnostic_id,
        ))
    }

    fn control(
        control: &tracedecay_memory_provider_api::OperationControl,
    ) -> Result<canonical_dummy::contract::RequestControl, TerminalCode> {
        let snapshot = control.snapshot()?;
        Ok(canonical_dummy::contract::RequestControl {
            deadline_utc_micros: snapshot.deadline_utc_micros,
            remaining_millis: snapshot.remaining_millis,
            cancellation: canonical_dummy::contract::CancellationState::Live,
        })
    }

    fn context(
        call: &ProviderCall,
        control: canonical_dummy::contract::RequestControl,
    ) -> canonical_dummy::OperationContext {
        canonical_dummy::OperationContext {
            exact_scope_digest: call.exact_scope.exact_scope_sha256(),
            operation_id: call.operation_id.clone(),
            idempotency_key: call.idempotency_key.clone().unwrap_or_default(),
            expected_state_generation: call.expected_state_generation,
            request_control: control,
        }
    }

    fn preflight_reply(&self, call: &ProviderCall, code: TerminalCode) -> ProviderReply {
        ProviderReply {
            terminal: self.terminal(
                call.operation,
                code,
                CommittedEffectEvidence::none(Some(self.lock().state_generation())),
                &call.operation_id,
                Some(format!("dummy.{}", code.as_wire())),
            ),
            payload: None,
            warnings: Vec::new(),
            extensions: Vec::new(),
            state_generation: self.lock().state_generation(),
        }
    }

    fn adapt_terminal<T>(
        &self,
        call: &ProviderCall,
        terminal: canonical_dummy::Terminal<T>,
        mapped_payload: Option<CanonicalPayload>,
    ) -> ProviderReply {
        let terminal_code = convert_terminal(terminal.terminal_code);
        let committed_effect = convert_effect(terminal.committed_effect);
        let fallback = convert_fallback(terminal.fallback);
        if fallback != FallbackEligibility::Forbidden {
            return self.preflight_reply(call, TerminalCode::ContractViolation);
        }
        let committed_effect = match committed_effect {
            CommittedEffectState::None => {
                CommittedEffectEvidence::none(Some(terminal.state_generation))
            }
            CommittedEffectState::Committed => {
                let committed_item_refs = call
                    .idempotency_key
                    .clone()
                    .into_iter()
                    .collect::<Vec<_>>();
                match CommittedEffectEvidence::committed(
                    call.expected_state_generation,
                    terminal.state_generation,
                    committed_item_refs,
                    EFFECT_RECEIPT,
                    &call.payload.sha256,
                ) {
                    Ok(effect) => effect,
                    Err(_) => {
                        return self.preflight_reply(call, TerminalCode::ContractViolation);
                    }
                }
            }
            CommittedEffectState::Partial | CommittedEffectState::Unknown => {
                return self.preflight_reply(call, TerminalCode::ContractViolation);
            }
        };
        let payload = if terminal.payload.is_some() {
            mapped_payload
        } else {
            None
        };
        ProviderReply {
            terminal: self.terminal(
                call.operation,
                terminal_code,
                committed_effect,
                &call.operation_id,
                terminal.diagnostic_id,
            ),
            payload,
            warnings: Vec::new(),
            extensions: Vec::new(),
            state_generation: terminal.state_generation,
        }
    }
}

impl MemoryProvider for DummyMemoryProviderAdapter {
    fn descriptor(&self) -> ProviderDescriptor {
        let generation = self.lock().state_generation();
        let mut descriptor = self.descriptor.clone();
        descriptor.state_generation = generation;
        descriptor
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        self.handshake_calls.fetch_add(1, Ordering::SeqCst);
        self.last_registration_revision
            .store(request.registration_revision, Ordering::SeqCst);
        let control = match Self::control(&request.control) {
            Ok(control) => control,
            Err(code) => {
                return HandshakeResponse {
                    terminal: self.terminal(
                        ProviderOperation::Handshake,
                        code,
                        CommittedEffectEvidence::none(Some(self.lock().state_generation())),
                        &request.request_id,
                        Some(format!("dummy.{}", code.as_wire())),
                    ),
                    descriptor: None,
                    provider_instance_id: None,
                    state_namespace: None,
                    accepted_scope: None,
                    effective_limits: None,
                    ready_receipt_sha256: None,
                    warnings: Vec::new(),
                };
            }
        };
        let terminal = self.lock().handshake(
            request.provider_id.as_str(),
            &request.exact_scope.exact_scope_sha256(),
            control,
        );
        let terminal_code = convert_terminal(terminal.terminal_code);
        let committed_effect = convert_effect(terminal.committed_effect);
        let fallback = convert_fallback(terminal.fallback);
        let admitted_terminal = committed_effect == CommittedEffectState::None
            && fallback == FallbackEligibility::Forbidden;
        let payload = terminal.payload;
        let success = terminal_code == TerminalCode::Success && admitted_terminal;
        let terminal_code = if admitted_terminal {
            terminal_code
        } else {
            TerminalCode::ContractViolation
        };
        HandshakeResponse {
            terminal: self.terminal(
                ProviderOperation::Handshake,
                terminal_code,
                CommittedEffectEvidence::none(Some(terminal.state_generation)),
                &request.request_id,
                terminal.diagnostic_id.or_else(|| {
                    (!admitted_terminal).then(|| "dummy.contract_violation".to_owned())
                }),
            ),
            descriptor: success.then(|| self.descriptor()),
            provider_instance_id: payload
                .as_ref()
                .map(|result| result.provider_instance_id.clone()),
            state_namespace: success.then(|| "canonical-dummy-scope".to_owned()),
            accepted_scope: success.then(|| request.exact_scope.clone()),
            effective_limits: success.then(|| request.host_limits.minimum(self.limits)),
            ready_receipt_sha256: success.then(|| READY_RECEIPT.to_owned()),
            warnings: Vec::new(),
        }
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        self.invoke_calls.fetch_add(1, Ordering::SeqCst);
        let control = match Self::control(&call.control) {
            Ok(control) => control,
            Err(code) => return self.preflight_reply(call, code),
        };
        let context = Self::context(call, control);
        match call.operation {
            ProviderOperation::Health => {
                let terminal = self.lock().health(&context);
                let payload = match terminal.payload.as_ref().map(health_payload).transpose() {
                    Ok(payload) => payload,
                    Err(_) => return self.preflight_reply(call, TerminalCode::ContractViolation),
                };
                self.adapt_terminal(call, terminal, payload)
            }
            ProviderOperation::Observe => {
                let content = match String::from_utf8(call.payload.bytes.clone()) {
                    Ok(content) => content,
                    Err(_) => return self.preflight_reply(call, TerminalCode::ContractViolation),
                };
                let observation = canonical_dummy::Observation {
                    observation_id: context.idempotency_key.clone(),
                    source_sequence: 1,
                    canonical_content: content,
                    payload_sha256: call.payload.sha256.clone(),
                    extensions: call
                        .extensions
                        .iter()
                        .map(|extension| canonical_dummy::OwnedOpaqueExtension {
                            extension_id: extension.extension_id.as_str().to_owned(),
                            extension_version: extension.extension_version,
                            required: extension.required,
                            canonical_payload: extension.canonical_payload.clone(),
                            payload_sha256: extension.payload_sha256.clone(),
                        })
                        .collect(),
                };
                let terminal = self.lock().observe(&context, observation);
                let payload = match terminal
                    .payload
                    .as_ref()
                    .map(observation_payload)
                    .transpose()
                {
                    Ok(payload) => payload,
                    Err(_) => return self.preflight_reply(call, TerminalCode::ContractViolation),
                };
                self.adapt_terminal(call, terminal, payload)
            }
            ProviderOperation::Recall => {
                let (query, maximum_candidates) = match decode_recall_request(&call.payload) {
                    Ok(request) => request,
                    Err(()) => {
                        return self.preflight_reply(call, TerminalCode::ContractViolation);
                    }
                };
                let request = canonical_dummy::RecallRequest {
                    context,
                    query,
                    maximum_candidates,
                };
                let terminal = self.lock().recall(&request);
                let payload = match terminal.payload.as_ref().map(recall_payload).transpose() {
                    Ok(payload) => payload,
                    Err(_) => return self.preflight_reply(call, TerminalCode::ContractViolation),
                };
                self.adapt_terminal(call, terminal, payload)
            }
            _ => self.preflight_reply(call, TerminalCode::CapabilityUnsupported),
        }
    }
}

fn decode_recall_request(payload: &CanonicalPayload) -> Result<(String, usize), ()> {
    if payload.contract_id.as_str() != "tracedecay.memory.provider.recall.v1" {
        return Err(());
    }
    let text = std::str::from_utf8(&payload.bytes).map_err(|_| ())?;
    let mut lines = text.lines();
    let query = lines
        .next()
        .and_then(|line| line.strip_prefix("query="))
        .filter(|query| !query.is_empty())
        .ok_or(())?;
    let maximum_candidates = lines
        .next()
        .and_then(|line| line.strip_prefix("maximum_candidates="))
        .ok_or(())?
        .parse::<usize>()
        .map_err(|_| ())?;
    if maximum_candidates == 0 || lines.next().is_some() {
        return Err(());
    }
    Ok((query.to_owned(), maximum_candidates))
}

struct PayloadVariantProvider {
    inner: DummyMemoryProviderAdapter,
    replacement: CanonicalPayload,
}

struct EffectEvidenceVariantProvider {
    inner: DummyMemoryProviderAdapter,
}

struct FallbackPolicyVariantProvider {
    inner: DummyMemoryProviderAdapter,
    fallback: FallbackDirective,
}

impl FallbackPolicyVariantProvider {
    fn new(target_provider_id: &str) -> Result<Self, Box<dyn Error>> {
        let inner = DummyMemoryProviderAdapter::new()?;
        let current_provider_id = inner.descriptor.provider_id.clone();
        let policy = PinnedFallbackPolicy::new(
            "test.explicit-fallback-policy",
            7,
            OwnedProviderId::new(target_provider_id)?,
        )?;
        let fallback = FallbackDirective::explicit_policy_only(
            &current_provider_id,
            policy,
            "test provider unavailable",
        )?;
        Ok(Self { inner, fallback })
    }
}

impl MemoryProvider for FallbackPolicyVariantProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        self.inner.descriptor()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        self.inner.handshake(request)
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        let mut reply = self.inner.invoke(call);
        if call.operation == ProviderOperation::Health {
            reply.payload = None;
            reply.terminal = validated_terminal(TerminalRecord::new(
                call.operation,
                call.provider_id.clone(),
                TerminalCode::ProviderUnavailable,
                CommittedEffectEvidence::none(Some(reply.state_generation)),
                self.fallback.clone(),
                &call.operation_id,
                call.exact_scope.exact_scope_sha256(),
                Some("test.provider_unavailable".to_owned()),
            ));
        }
        reply
    }
}

impl EffectEvidenceVariantProvider {
    fn new() -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            inner: DummyMemoryProviderAdapter::new()?,
        })
    }
}

impl MemoryProvider for EffectEvidenceVariantProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        self.inner.descriptor()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        self.inner.handshake(request)
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        let mut reply = self.inner.invoke(call);
        if call.operation == ProviderOperation::Observe
            && reply.terminal.committed_effect().state() == CommittedEffectState::Committed
        {
            let current = reply.terminal.committed_effect();
            let variant_effect = match (
                current.state_generation_before(),
                current.state_generation_after(),
            ) {
                (Some(before), Some(after)) => CommittedEffectEvidence::committed(
                    before,
                    after,
                    current.committed_item_refs().to_vec(),
                    "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                    "0000000000000000000000000000000000000000000000000000000000000000",
                ),
                _ => return self.inner.preflight_reply(call, TerminalCode::ContractViolation),
            };
            let variant_effect = match variant_effect {
                Ok(effect) => effect,
                Err(_) => return self.inner.preflight_reply(call, TerminalCode::ContractViolation),
            };
            reply.terminal = validated_terminal(TerminalRecord::new(
                reply.terminal.operation(),
                reply.terminal.provider_id().clone(),
                reply.terminal.terminal_code(),
                variant_effect,
                reply.terminal.fallback().clone(),
                reply.terminal.operation_id(),
                reply.terminal.exact_scope_sha256(),
                reply.terminal.diagnostic_id().map(str::to_owned),
            ));
        }
        reply
    }
}

impl PayloadVariantProvider {
    fn new() -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            inner: DummyMemoryProviderAdapter::new()?,
            replacement: CanonicalPayload::new(
                OwnedVersionedId::new("tracedecay.memory.provider.terminal.v1")?,
                b"[]".to_vec(),
                "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e7b66bd8c2ad4ce7b16fc33",
            )?,
        })
    }
}

impl MemoryProvider for PayloadVariantProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        self.inner.descriptor()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        self.inner.handshake(request)
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        let mut reply = self.inner.invoke(call);
        if reply.payload.is_some() {
            reply.payload = Some(self.replacement.clone());
        }
        reply
    }
}

struct HealthFailureProvider {
    inner: DummyMemoryProviderAdapter,
}

#[derive(Clone, Copy)]
enum HandshakeMutation {
    AcceptedScope,
    ReadyReceipt,
    DescriptorLimits,
    DescriptorProtocol,
}

struct HandshakeMutationProvider {
    inner: DummyMemoryProviderAdapter,
    mutation: HandshakeMutation,
    wrong_scope: Option<OwnedExactScope>,
}

impl HandshakeMutationProvider {
    fn new(mutation: HandshakeMutation) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            inner: DummyMemoryProviderAdapter::new()?,
            mutation,
            wrong_scope: matches!(mutation, HandshakeMutation::AcceptedScope)
                .then(alternate_scope)
                .transpose()?,
        })
    }
}

impl MemoryProvider for HandshakeMutationProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        self.inner.descriptor()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        let mut response = self.inner.handshake(request);
        match self.mutation {
            HandshakeMutation::AcceptedScope => {
                response.accepted_scope = self.wrong_scope.clone();
            }
            HandshakeMutation::ReadyReceipt => {
                response.ready_receipt_sha256 = Some("malformed-receipt".to_owned());
            }
            HandshakeMutation::DescriptorLimits => {
                if let Some(descriptor) = response.descriptor.as_mut() {
                    descriptor.limits.request_bytes =
                        descriptor.limits.request_bytes.saturating_sub(1);
                }
            }
            HandshakeMutation::DescriptorProtocol => {
                if let Some(descriptor) = response.descriptor.as_mut() {
                    descriptor.protocol_minor = descriptor.protocol_minor.saturating_add(1);
                }
            }
        }
        response
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        self.inner.invoke(call)
    }
}

#[derive(Clone, Copy)]
enum TerminalIdentityMutation {
    OperationId,
    ExactScope,
}

struct TerminalIdentityMutationProvider {
    inner: DummyMemoryProviderAdapter,
    mutation: TerminalIdentityMutation,
}

impl TerminalIdentityMutationProvider {
    fn new(mutation: TerminalIdentityMutation) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            inner: DummyMemoryProviderAdapter::new()?,
            mutation,
        })
    }
}

impl MemoryProvider for TerminalIdentityMutationProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        self.inner.descriptor()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        self.inner.handshake(request)
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        let mut reply = self.inner.invoke(call);
        if call.operation == ProviderOperation::Health {
            let operation_id = match self.mutation {
                TerminalIdentityMutation::OperationId => "wrong-operation-id",
                TerminalIdentityMutation::ExactScope => reply.terminal.operation_id(),
            };
            let exact_scope_sha256 = match self.mutation {
                TerminalIdentityMutation::OperationId => reply.terminal.exact_scope_sha256(),
                TerminalIdentityMutation::ExactScope => {
                    "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                }
            };
            reply.terminal = validated_terminal(TerminalRecord::new(
                reply.terminal.operation(),
                reply.terminal.provider_id().clone(),
                reply.terminal.terminal_code(),
                reply.terminal.committed_effect().clone(),
                reply.terminal.fallback().clone(),
                operation_id,
                exact_scope_sha256,
                reply.terminal.diagnostic_id().map(str::to_owned),
            ));
        }
        reply
    }
}

impl HealthFailureProvider {
    fn new() -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            inner: DummyMemoryProviderAdapter::new()?,
        })
    }
}

impl MemoryProvider for HealthFailureProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        self.inner.descriptor()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        self.inner.handshake(request)
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        let mut reply = self.inner.invoke(call);
        if call.operation == ProviderOperation::Health {
            reply.terminal = validated_terminal(TerminalRecord::new(
                reply.terminal.operation(),
                reply.terminal.provider_id().clone(),
                TerminalCode::InternalFailure,
                reply.terminal.committed_effect().clone(),
                reply.terminal.fallback().clone(),
                reply.terminal.operation_id(),
                reply.terminal.exact_scope_sha256(),
                Some("dummy.forced_internal_failure".to_owned()),
            ));
        }
        reply
    }
}

fn validated_terminal(result: Result<TerminalRecord, ApiError>) -> TerminalRecord {
    match result {
        Ok(terminal) => terminal,
        Err(_) => std::process::abort(),
    }
}

#[test]
fn mandatory_suite_passes_against_canonical_dummy_provider() -> Result<(), Box<dyn Error>> {
    let provider = DummyMemoryProviderAdapter::new()?;
    let harness = ProviderHarness::new(&provider)?;
    let fixture = mandatory_conformance_fixture(
        harness.fixture_identity(),
        exact_scope()?,
        REGISTRATION_REVISION,
    )?;

    let report = harness.run_product(&fixture)?;

    assert!(report.passed());
    assert_eq!(report.summary().status, ConformanceStatus::Pass);
    assert_eq!(report.summary().planned_steps, 7);
    assert_eq!(report.summary().executed_steps, 7);
    assert_eq!(report.summary().provider_contacted_steps, 5);
    assert_eq!(report.summary().host_preflight_steps, 2);
    assert_eq!(report.summary().passed_steps, 7);
    assert_eq!(report.summary().failed_steps, 0);
    assert_eq!(report.summary().not_run_steps, 0);
    assert_eq!(report.identity(), fixture.identity());
    assert_eq!(fixture.registration_revision(), REGISTRATION_REVISION);
    assert_eq!(provider.handshake_calls(), 1);
    assert_eq!(provider.invoke_calls(), 4);
    assert!(
        report.steps()[..5]
            .iter()
            .all(|step| step.provider_contacted())
    );
    let observe = match report.steps()[2].output() {
        tracedecay_memory_conformance::ProductStepOutput::Operation(reply) => reply,
        tracedecay_memory_conformance::ProductStepOutput::Handshake(_) => {
            return Err(io::Error::other("mandatory.observe returned a handshake output").into());
        }
    };
    assert_eq!(observe.terminal.committed_effect().state(), CommittedEffectState::Committed);
    assert_eq!(
        observe.terminal.committed_effect().state_generation_before(),
        Some(0)
    );
    assert_eq!(
        observe.terminal.committed_effect().state_generation_after(),
        Some(1)
    );
    assert_eq!(
        observe.terminal.committed_effect().committed_item_refs(),
        ["mandatory-observation-key"]
    );
    assert_eq!(observe.terminal.provider_receipt_sha256(), Some(EFFECT_RECEIPT));
    assert_eq!(
        observe.terminal.committed_effect().verification_sha256(),
        Some(OBSERVATION_PAYLOAD_SHA256)
    );
    for (index, step_id, terminal_code) in [
        (5, "mandatory.cancelled_recall", TerminalCode::Cancelled),
        (
            6,
            "mandatory.expired_recall",
            TerminalCode::DeadlineExceeded,
        ),
    ] {
        let step = &report.steps()[index];
        assert_eq!(step.evaluation().step_id(), step_id);
        assert!(!step.provider_contacted());
        assert!(matches!(
            step.output(),
            tracedecay_memory_conformance::ProductStepOutput::Operation(reply)
                if reply.terminal.terminal_code() == terminal_code
        ));
        if let tracedecay_memory_conformance::ProductStepOutput::Operation(reply) = step.output() {
            assert_eq!(
                reply.terminal.committed_effect(),
                &CommittedEffectEvidence::none(Some(1))
            );
        }
    }
    assert_eq!(provider.last_registration_revision(), REGISTRATION_REVISION);
    assert_eq!(
        report.identity().contract().contract_set_id(),
        tracedecay_memory_provider_api::contract::CONTRACT_SET_ID
    );
    assert_eq!(
        report.identity().contract().contract_set_sha256(),
        tracedecay_memory_provider_api::contract::CONTRACT_SET_SHA256
    );
    assert_eq!(
        report.identity().provider().provider_id().as_str(),
        "test.canonical-dummy"
    );
    assert_eq!(
        report.identity().provider().build_identity_sha256(),
        BUILD_DIGEST
    );
    Ok(())
}

#[test]
fn cancelled_and_expired_handshakes_do_not_contact_provider_code() -> Result<(), Box<dyn Error>> {
    for (fixture_id, control, terminal_code) in [
        (
            "cancelled-handshake",
            RequestControlFixture::cancelled(4_102_444_800_000_000, 5_000),
            TerminalCode::Cancelled,
        ),
        (
            "expired-handshake",
            RequestControlFixture::expired(4_102_444_800_000_000),
            TerminalCode::DeadlineExceeded,
        ),
    ] {
        let provider = DummyMemoryProviderAdapter::new()?;
        let harness = ProviderHarness::new(&provider)?;
        let source = mandatory_conformance_fixture(
            harness.fixture_identity(),
            exact_scope()?,
            REGISTRATION_REVISION,
        )?;
        let mut handshake = source.handshake().clone();
        handshake.control = control;
        handshake.expectation.terminal_code = terminal_code;
        handshake.expectation.require_descriptor = false;
        handshake.expectation.require_accepted_scope = false;
        handshake.expectation.require_ready_receipt = false;
        let fixture = ScenarioFixture::new(
            fixture_id,
            source.identity().clone(),
            source.exact_scope().clone(),
            source.registration_revision(),
            handshake,
            Vec::new(),
        )?;

        let report = harness.run_product(&fixture)?;

        assert!(report.passed());
        assert_eq!(report.summary().planned_steps, 1);
        assert_eq!(report.summary().executed_steps, 1);
        assert_eq!(report.summary().provider_contacted_steps, 0);
        assert_eq!(report.summary().host_preflight_steps, 1);
        assert_eq!(provider.handshake_calls(), 0);
        let handshake_step = &report.steps()[0];
        assert_eq!(
            handshake_step.evaluation().step_id(),
            "mandatory.handshake"
        );
        assert!(!handshake_step.provider_contacted());
        assert!(matches!(
            handshake_step.output(),
            tracedecay_memory_conformance::ProductStepOutput::Handshake(response)
                if response.terminal.terminal_code() == terminal_code
        ));
    }
    Ok(())
}

#[test]
fn observer_report_is_sanitized_and_differentially_comparable() -> Result<(), Box<dyn Error>> {
    let product_provider = DummyMemoryProviderAdapter::new()?;
    let observer_provider = DummyMemoryProviderAdapter::new()?;
    let product_harness = ProviderHarness::new(&product_provider)?;
    let observer_harness = ProviderHarness::new(&observer_provider)?;
    let fixture = mandatory_conformance_fixture(
        product_harness.fixture_identity(),
        exact_scope()?,
        REGISTRATION_REVISION,
    )?;

    let product = product_harness.run_product(&fixture)?;
    let observer = observer_harness.run_observer(&fixture)?;

    assert!(observer.passed());
    assert_eq!(observer.identity(), fixture.identity());
    assert_eq!(observer.steps().len(), fixture.planned_steps());
    assert!(
        observer
            .steps()
            .iter()
            .all(|step| step.evaluation().passed())
    );
    let differential = DifferentialReport::compare(product, observer)?;
    assert!(!differential.differs());
    assert_eq!(differential.steps().len(), fixture.planned_steps());
    assert_eq!(product_provider.handshake_calls(), 1);
    assert_eq!(product_provider.invoke_calls(), 4);
    assert_eq!(observer_provider.handshake_calls(), 1);
    assert_eq!(observer_provider.invoke_calls(), 4);
    Ok(())
}

#[test]
fn differential_retains_structured_effect_evidence_beyond_coarse_state()
-> Result<(), Box<dyn Error>> {
    let product_provider = DummyMemoryProviderAdapter::new()?;
    let observer_provider = EffectEvidenceVariantProvider::new()?;
    let product_harness = ProviderHarness::new(&product_provider)?;
    let observer_harness = ProviderHarness::new(&observer_provider)?;
    let fixture = mandatory_conformance_fixture(
        product_harness.fixture_identity(),
        exact_scope()?,
        REGISTRATION_REVISION,
    )?;

    let product = product_harness.run_product(&fixture)?;
    let observer = observer_harness.run_observer(&fixture)?;
    assert!(product.passed());
    assert!(observer.passed());

    let differential = DifferentialReport::compare(product, observer)?;
    let observe = &differential.steps()[2];
    let product_terminal = match observe.product_observed.as_ref() {
        Some(observed) => &observed.terminal,
        None => return Err(io::Error::other("missing product terminal summary").into()),
    };
    let observer_terminal = match observe.observer_observed.as_ref() {
        Some(observed) => &observed.terminal,
        None => return Err(io::Error::other("missing observer terminal summary").into()),
    };

    assert_eq!(
        product_terminal.committed_effect().state(),
        observer_terminal.committed_effect().state()
    );
    assert_eq!(
        product_terminal
            .committed_effect()
            .state_generation_after(),
        observer_terminal
            .committed_effect()
            .state_generation_after()
    );
    assert_ne!(
        product_terminal.provider_receipt_sha256(),
        observer_terminal.provider_receipt_sha256()
    );
    assert!(observe.product_failed_fields.is_empty());
    assert!(observe.observer_failed_fields.is_empty());
    assert!(observe.differs());
    assert!(differential.differs());
    Ok(())
}

#[test]
fn differential_retains_complete_fallback_policy_beyond_eligibility()
-> Result<(), Box<dyn Error>> {
    let product_provider = FallbackPolicyVariantProvider::new("test.fallback-a")?;
    let observer_provider = FallbackPolicyVariantProvider::new("test.fallback-b")?;
    let product_harness = ProviderHarness::new(&product_provider)?;
    let observer_harness = ProviderHarness::new(&observer_provider)?;
    let fixture = mandatory_conformance_fixture(
        product_harness.fixture_identity(),
        exact_scope()?,
        REGISTRATION_REVISION,
    )?;

    let product = product_harness.run_product(&fixture)?;
    let observer = observer_harness.run_observer(&fixture)?;
    assert!(!product.passed());
    assert!(!observer.passed());

    let differential = DifferentialReport::compare(product, observer)?;
    let health = &differential.steps()[1];
    let product_terminal = match health.product_observed.as_ref() {
        Some(observed) => &observed.terminal,
        None => return Err(io::Error::other("missing product fallback terminal").into()),
    };
    let observer_terminal = match health.observer_observed.as_ref() {
        Some(observed) => &observed.terminal,
        None => return Err(io::Error::other("missing observer fallback terminal").into()),
    };
    let product_policy = match product_terminal.fallback().policy() {
        Some(policy) => policy,
        None => return Err(io::Error::other("missing product fallback policy").into()),
    };
    let observer_policy = match observer_terminal.fallback().policy() {
        Some(policy) => policy,
        None => return Err(io::Error::other("missing observer fallback policy").into()),
    };

    assert_eq!(
        product_terminal.fallback().eligibility(),
        observer_terminal.fallback().eligibility()
    );
    assert_ne!(
        product_policy.target_provider_id(),
        observer_policy.target_provider_id()
    );
    assert_eq!(health.product_failed_fields, health.observer_failed_fields);
    assert!(health.differs());
    assert!(differential.differs());
    Ok(())
}

#[test]
fn terminal_payload_substitution_fails_without_leaking_bytes_to_observer_reports()
-> Result<(), Box<dyn Error>> {
    let base_product_provider = DummyMemoryProviderAdapter::new()?;
    let variant_product_provider = PayloadVariantProvider::new()?;
    let base_observer_provider = DummyMemoryProviderAdapter::new()?;
    let variant_observer_provider = PayloadVariantProvider::new()?;
    let base_product_harness = ProviderHarness::new(&base_product_provider)?;
    let variant_product_harness = ProviderHarness::new(&variant_product_provider)?;
    let base_observer_harness = ProviderHarness::new(&base_observer_provider)?;
    let variant_observer_harness = ProviderHarness::new(&variant_observer_provider)?;
    let fixture = mandatory_conformance_fixture(
        base_product_harness.fixture_identity(),
        exact_scope()?,
        REGISTRATION_REVISION,
    )?;

    let base_product = base_product_harness.run_product(&fixture)?;
    let variant_product = variant_product_harness.run_product(&fixture)?;
    let base_observer = base_observer_harness.run_observer(&fixture)?;
    let variant_observer = variant_observer_harness.run_observer(&fixture)?;

    assert!(base_product.passed());
    assert!(!variant_product.passed());
    assert_ne!(base_product, variant_product);
    assert!(!variant_observer.passed());
    assert_eq!(variant_product.summary().failed_steps, 4);
    assert_eq!(variant_observer.summary().failed_steps, 4);
    let base_observed_projection = base_observer
        .steps()
        .iter()
            .map(|step| step.observed().clone())
        .collect::<Vec<_>>();
    let variant_observed_projection = variant_observer
        .steps()
        .iter()
            .map(|step| step.observed().clone())
        .collect::<Vec<_>>();
    assert_eq!(base_observed_projection, variant_observed_projection);
    assert!(
        variant_observer.steps()[1..5]
            .iter()
            .all(|step| { step.evaluation().failed_fields() == ["payload"] })
    );
    assert_eq!(base_observer_provider.handshake_calls(), 1);
    assert_eq!(base_observer_provider.invoke_calls(), 4);
    assert_eq!(variant_observer_provider.inner.handshake_calls(), 1);
    assert_eq!(variant_observer_provider.inner.invoke_calls(), 4);
    let payload_only_differential = DifferentialReport::compare(base_product, variant_observer)?;
    assert!(payload_only_differential.differs());
    assert!(
        payload_only_differential.steps()[1..5]
            .iter()
            .all(|step| step.differs())
    );
    Ok(())
}

#[test]
fn differential_report_detects_observer_visible_behavior() -> Result<(), Box<dyn Error>> {
    let product_provider = DummyMemoryProviderAdapter::new()?;
    let observer_provider = HealthFailureProvider::new()?;
    let product_harness = ProviderHarness::new(&product_provider)?;
    let observer_harness = ProviderHarness::new(&observer_provider)?;
    let fixture = mandatory_conformance_fixture(
        product_harness.fixture_identity(),
        exact_scope()?,
        REGISTRATION_REVISION,
    )?;

    let product = product_harness.run_product(&fixture)?;
    let observer = observer_harness.run_observer(&fixture)?;
    let differential = DifferentialReport::compare(product, observer)?;

    assert!(differential.differs());
    assert!(!differential.steps()[0].differs());
    assert!(differential.steps()[1].differs());
    assert_eq!(
        differential.steps()[1]
            .observer_observed
            .as_ref()
            .map(|observed| observed.terminal.terminal_code()),
        Some(TerminalCode::InternalFailure)
    );
    assert_eq!(observer_provider.inner.handshake_calls(), 1);
    assert_eq!(observer_provider.inner.invoke_calls(), 4);
    Ok(())
}

#[test]
fn differential_rejects_same_named_scenarios_with_different_semantic_inputs()
-> Result<(), Box<dyn Error>> {
    let product_provider = DummyMemoryProviderAdapter::new()?;
    let product_harness = ProviderHarness::new(&product_provider)?;
    let source = mandatory_conformance_fixture(
        product_harness.fixture_identity(),
        exact_scope()?,
        REGISTRATION_REVISION,
    )?;
    let product = product_harness.run_product(&source)?;

    let different_scope = alternate_scope()?;
    let scope_provider = DummyMemoryProviderAdapter::new_for_scope(&different_scope)?;
    let scope_harness = ProviderHarness::new(&scope_provider)?;
    let scope_fixture = mandatory_conformance_fixture(
        scope_harness.fixture_identity(),
        different_scope,
        REGISTRATION_REVISION,
    )?;
    let scope_observer = scope_harness.run_observer(&scope_fixture)?;
    assert!(matches!(
        DifferentialReport::compare(product.clone(), scope_observer),
        Err(EvaluationError::DifferentialScenarioMismatch { .. })
    ));

    let revision_provider = DummyMemoryProviderAdapter::new()?;
    let revision_harness = ProviderHarness::new(&revision_provider)?;
    let revision_fixture = mandatory_conformance_fixture(
        revision_harness.fixture_identity(),
        exact_scope()?,
        REGISTRATION_REVISION + 1,
    )?;
    let revision_observer = revision_harness.run_observer(&revision_fixture)?;
    assert!(matches!(
        DifferentialReport::compare(product.clone(), revision_observer),
        Err(EvaluationError::DifferentialScenarioMismatch { .. })
    ));

    let mut control_operations = source.operations().to_vec();
    control_operations[0].control = RequestControlFixture::live(4_102_444_800_000_000, 4_999)?;
    let control_fixture = rebuild_fixture(
        &source,
        source.fixture_id(),
        source.handshake().clone(),
        control_operations,
    )?;
    assert_ne!(
        source.scenario_identity(),
        control_fixture.scenario_identity()
    );
    let control_provider = DummyMemoryProviderAdapter::new()?;
    let control_harness = ProviderHarness::new(&control_provider)?;
    let control_observer = control_harness.run_observer(&control_fixture)?;
    assert!(matches!(
        DifferentialReport::compare(product.clone(), control_observer),
        Err(EvaluationError::DifferentialScenarioMismatch { .. })
    ));

    let mut expectation_operations = source.operations().to_vec();
    expectation_operations[0].expectation.payload =
        tracedecay_memory_conformance::PayloadExpectation::Present;
    let expectation_fixture = rebuild_fixture(
        &source,
        source.fixture_id(),
        source.handshake().clone(),
        expectation_operations,
    )?;
    assert_ne!(
        source.scenario_identity(),
        expectation_fixture.scenario_identity()
    );
    let expectation_provider = DummyMemoryProviderAdapter::new()?;
    let expectation_harness = ProviderHarness::new(&expectation_provider)?;
    let expectation_observer = expectation_harness.run_observer(&expectation_fixture)?;
    assert!(matches!(
        DifferentialReport::compare(product.clone(), expectation_observer),
        Err(EvaluationError::DifferentialScenarioMismatch { .. })
    ));

    let mut kind_operations = source.operations().to_vec();
    kind_operations[0].operation = ProviderOperation::Recall;
    kind_operations[0].required_capabilities = vec![OwnedVersionedId::new(
        ProviderOperation::Recall.capability_id(),
    )?];
    kind_operations[0].payload = recall_request_payload("no-match", 16)?;
    kind_operations[0].expectation.terminal =
        tracedecay_memory_conformance::TerminalExpectation::exactly(
            TerminalCode::SuccessZeroResults,
        );
    kind_operations[0].expectation.payload =
        tracedecay_memory_conformance::PayloadExpectation::Exact(canonical_payload(
            "tracedecay.memory.provider.recall.v1",
            "{\"candidate_count\":0,\"coverage_complete\":true}".to_owned(),
        )?);
    let kind_fixture = rebuild_fixture(
        &source,
        source.fixture_id(),
        source.handshake().clone(),
        kind_operations,
    )?;
    assert_eq!(
        source.planned_step_ids().collect::<Vec<_>>(),
        kind_fixture.planned_step_ids().collect::<Vec<_>>()
    );
    assert_ne!(source.scenario_identity(), kind_fixture.scenario_identity());
    let kind_provider = DummyMemoryProviderAdapter::new()?;
    let kind_harness = ProviderHarness::new(&kind_provider)?;
    let kind_observer = kind_harness.run_observer(&kind_fixture)?;
    assert!(matches!(
        DifferentialReport::compare(product, kind_observer),
        Err(EvaluationError::DifferentialScenarioMismatch { .. })
    ));
    Ok(())
}

#[test]
fn differential_compares_failed_invariant_field_sets() -> Result<(), Box<dyn Error>> {
    let product_provider =
        TerminalIdentityMutationProvider::new(TerminalIdentityMutation::OperationId)?;
    let observer_provider =
        TerminalIdentityMutationProvider::new(TerminalIdentityMutation::ExactScope)?;
    let product_harness = ProviderHarness::new(&product_provider)?;
    let observer_harness = ProviderHarness::new(&observer_provider)?;
    let fixture = mandatory_conformance_fixture(
        product_harness.fixture_identity(),
        exact_scope()?,
        REGISTRATION_REVISION,
    )?;

    let product = product_harness.run_product(&fixture)?;
    let observer = observer_harness.run_observer(&fixture)?;
    let differential = DifferentialReport::compare(product, observer)?;
    let health = &differential.steps()[1];

    assert_eq!(health.product_status, Some(ConformanceStatus::Fail));
    assert_eq!(health.observer_status, Some(ConformanceStatus::Fail));
    assert_eq!(health.product_observed, health.observer_observed);
    assert_eq!(health.product_failed_fields, ["terminal.operation_id"]);
    assert_eq!(
        health.observer_failed_fields,
        ["terminal.exact_scope_sha256"]
    );
    assert!(health.differs());
    assert!(differential.differs());
    Ok(())
}

#[test]
fn recall_evaluation_consumes_the_actual_fixture_query() -> Result<(), Box<dyn Error>> {
    let provider = DummyMemoryProviderAdapter::new()?;
    let harness = ProviderHarness::new(&provider)?;
    let source = mandatory_conformance_fixture(
        harness.fixture_identity(),
        exact_scope()?,
        REGISTRATION_REVISION,
    )?;
    let mut operations = source.operations().to_vec();
    operations[3].payload = recall_request_payload("no-match", 16)?;
    let fixture = rebuild_fixture(
        &source,
        "actual-recall-query",
        source.handshake().clone(),
        operations,
    )?;

    let report = harness.run_product(&fixture)?;

    assert_eq!(report.summary().failed_steps, 1);
    assert_eq!(report.steps()[4].evaluation().step_id(), "mandatory.recall");
    assert_eq!(
        report.steps()[4]
            .evaluation()
            .violations()
            .iter()
            .map(|violation| violation.field)
            .collect::<Vec<_>>(),
        ["terminal_code", "payload"]
    );
    Ok(())
}

#[test]
fn handshake_rejects_wrong_scope_receipt_limits_and_protocol() -> Result<(), Box<dyn Error>> {
    for (mutation, expected_field) in [
        (HandshakeMutation::AcceptedScope, "accepted_scope"),
        (HandshakeMutation::ReadyReceipt, "ready_receipt_sha256"),
        (HandshakeMutation::DescriptorLimits, "descriptor.limits"),
        (
            HandshakeMutation::DescriptorProtocol,
            "descriptor.protocol_minor",
        ),
    ] {
        let provider = HandshakeMutationProvider::new(mutation)?;
        let harness = ProviderHarness::new(&provider)?;
        let fixture = mandatory_conformance_fixture(
            harness.fixture_identity(),
            exact_scope()?,
            REGISTRATION_REVISION,
        )?;

        let report = harness.run_product(&fixture)?;

        assert!(!report.passed());
        assert_eq!(report.summary().executed_steps, 1);
        assert_eq!(provider.inner.invoke_calls(), 0);
        assert!(
            report.steps()[0]
                .evaluation()
                .violations()
                .iter()
                .any(|violation| violation.field == expected_field)
        );
    }
    Ok(())
}

#[test]
fn typed_mismatch_fails_without_hiding_provider_output() -> Result<(), Box<dyn Error>> {
    let provider = DummyMemoryProviderAdapter::new()?;
    let harness = ProviderHarness::new(&provider)?;
    let source = mandatory_conformance_fixture(
        harness.fixture_identity(),
        exact_scope()?,
        REGISTRATION_REVISION,
    )?;
    let mut operations = source.operations().to_vec();
    operations[0].expectation.terminal =
        tracedecay_memory_conformance::TerminalExpectation::exactly(TerminalCode::InternalFailure);
    let fixture = ScenarioFixture::new(
        "intentional-terminal-mismatch",
        source.identity().clone(),
        source.exact_scope().clone(),
        source.registration_revision(),
        source.handshake().clone(),
        operations,
    )?;

    let report = harness.run_product(&fixture)?;

    assert!(!report.passed());
    assert_eq!(report.summary().failed_steps, 1);
    let health = &report.steps()[1];
    assert_eq!(health.evaluation().violations().len(), 1);
    assert_eq!(health.evaluation().violations()[0].field, "terminal_code");
    assert!(matches!(
        health.output(),
        tracedecay_memory_conformance::ProductStepOutput::Operation(_)
    ));
    Ok(())
}

#[test]
fn stale_contract_identity_is_rejected_before_execution() -> Result<(), Box<dyn Error>> {
    let provider = DummyMemoryProviderAdapter::new()?;
    let harness = ProviderHarness::new(&provider)?;
    let source = mandatory_conformance_fixture(
        harness.fixture_identity(),
        exact_scope()?,
        REGISTRATION_REVISION,
    )?;
    let stale_identity = FixtureIdentity::new(
        ContractIdentity::new("tracedecay.memory.provider.contract-set.v9", BUILD_DIGEST)?,
        source.identity().provider().clone(),
    );
    let fixture = ScenarioFixture::new(
        "stale-contract",
        stale_identity,
        source.exact_scope().clone(),
        source.registration_revision(),
        source.handshake().clone(),
        source.operations().to_vec(),
    )?;

    let error = harness.run_product(&fixture);

    assert!(matches!(
        error,
        Err(EvaluationError::ContractIdentityMismatch { .. })
    ));
    assert_eq!(provider.lock().state_generation(), 0);
    Ok(())
}

#[test]
fn provider_and_build_identity_mismatches_are_rejected_before_both_run_modes()
-> Result<(), Box<dyn Error>> {
    let mut malformed_provider = DummyMemoryProviderAdapter::new()?;
    malformed_provider.descriptor.implementation_identity_sha256 = "not-a-sha256".to_owned();
    assert!(matches!(
        ProviderHarness::new(&malformed_provider),
        Err(EvaluationError::InvalidProviderBuildIdentitySha256(actual))
            if actual == "not-a-sha256"
    ));
    assert_eq!(malformed_provider.handshake_calls(), 0);

    let provider = DummyMemoryProviderAdapter::new()?;
    let harness = ProviderHarness::new(&provider)?;
    let wrong_provider = fixture_identity_for(
        "test.other-dummy",
        BUILD_DIGEST,
        ContractIdentity::current(),
    )?;
    let wrong_build = fixture_identity_for(
        "test.canonical-dummy",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        ContractIdentity::current(),
    )?;

    for (identity, expected_field) in [
        (wrong_provider, "provider_id"),
        (wrong_build, "provider_build_identity_sha256"),
    ] {
        let fixture =
            mandatory_conformance_fixture(identity, exact_scope()?, REGISTRATION_REVISION)?;
        for result in [
            harness.run_product(&fixture).map(|_| ()),
            harness.run_observer(&fixture).map(|_| ()),
        ] {
            assert!(matches!(
                result,
                Err(EvaluationError::ProviderIdentityMismatch { field, .. })
                    if field == expected_field
            ));
        }
    }
    assert_eq!(provider.handshake_calls(), 0);
    assert_eq!(provider.invoke_calls(), 0);
    Ok(())
}

#[test]
fn fixture_rejects_zero_live_budget_and_unreachable_operations() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        RequestControlFixture::live(4_102_444_800_000_000, 0),
        Err(EvaluationError::ZeroLiveRequestBudget)
    );

    let provider = DummyMemoryProviderAdapter::new()?;
    let harness = ProviderHarness::new(&provider)?;
    let source = mandatory_conformance_fixture(
        harness.fixture_identity(),
        exact_scope()?,
        REGISTRATION_REVISION,
    )?;
    let mut handshake = source.handshake().clone();
    handshake.expectation.terminal_code = TerminalCode::Cancelled;
    let result = ScenarioFixture::new(
        "unreachable-operations",
        source.identity().clone(),
        source.exact_scope().clone(),
        source.registration_revision(),
        handshake,
        source.operations().to_vec(),
    );
    assert!(matches!(
        result,
        Err(EvaluationError::OperationsRequireSuccessfulHandshake)
    ));

    let mut operations = source.operations().to_vec();
    operations[1].idempotency_key = None;
    let result = ScenarioFixture::new(
        "missing-idempotency-key",
        source.identity().clone(),
        source.exact_scope().clone(),
        source.registration_revision(),
        source.handshake().clone(),
        operations,
    );
    assert!(matches!(
        result,
        Err(EvaluationError::MissingFixtureIdempotencyKey { step_id })
            if step_id == "mandatory.observe"
    ));
    Ok(())
}

#[test]
fn fixture_rejects_duplicate_ids_and_capability_inconsistencies() -> Result<(), Box<dyn Error>> {
    let provider = DummyMemoryProviderAdapter::new()?;
    let harness = ProviderHarness::new(&provider)?;
    let source = mandatory_conformance_fixture(
        harness.fixture_identity(),
        exact_scope()?,
        REGISTRATION_REVISION,
    )?;

    let mut handshake = source.handshake().clone();
    handshake.expectation.require_ready_receipt = false;
    let result = rebuild_fixture(
        &source,
        "missing-ready-receipt",
        handshake,
        source.operations().to_vec(),
    );
    assert!(matches!(
        result,
        Err(EvaluationError::OperationsRequireReadyReceipt)
    ));

    let mut handshake = source.handshake().clone();
    handshake.expectation.require_descriptor = false;
    let result = rebuild_fixture(
        &source,
        "missing-handshake-descriptor",
        handshake,
        source.operations().to_vec(),
    );
    assert!(matches!(
        result,
        Err(EvaluationError::OperationsRequireHandshakeDescriptor)
    ));

    let mut handshake = source.handshake().clone();
    handshake.expectation.require_accepted_scope = false;
    let result = rebuild_fixture(
        &source,
        "missing-accepted-scope",
        handshake,
        source.operations().to_vec(),
    );
    assert!(matches!(
        result,
        Err(EvaluationError::OperationsRequireAcceptedScope)
    ));

    let mut operations = source.operations().to_vec();
    operations[0].step_id = source.handshake().step_id.clone();
    let result = rebuild_fixture(
        &source,
        "duplicate-step",
        source.handshake().clone(),
        operations,
    );
    assert!(matches!(result, Err(EvaluationError::DuplicateStepId(_))));

    let mut operations = source.operations().to_vec();
    let duplicate_operation_id = operations[0].operation_id.clone();
    operations[1].operation_id = duplicate_operation_id;
    let result = rebuild_fixture(
        &source,
        "duplicate-operation",
        source.handshake().clone(),
        operations,
    );
    assert!(matches!(
        result,
        Err(EvaluationError::DuplicateOperationId(_))
    ));

    let mut operations = source.operations().to_vec();
    operations[0].request_id = source.handshake().request_id.clone();
    let result = rebuild_fixture(
        &source,
        "duplicate-request",
        source.handshake().clone(),
        operations,
    );
    assert!(matches!(
        result,
        Err(EvaluationError::DuplicateRequestId(_))
    ));

    let mut operations = source.operations().to_vec();
    operations[0].required_capabilities.clear();
    let result = rebuild_fixture(
        &source,
        "missing-operation-capability",
        source.handshake().clone(),
        operations,
    );
    assert!(matches!(
        result,
        Err(EvaluationError::MissingFixtureOperationCapability { .. })
    ));

    let mut operations = source.operations().to_vec();
    operations[0]
        .required_capabilities
        .push(OwnedVersionedId::new("inspection.read.v1")?);
    let result = rebuild_fixture(
        &source,
        "unnegotiated-capability",
        source.handshake().clone(),
        operations,
    );
    assert!(matches!(
        result,
        Err(EvaluationError::OperationCapabilityNotNegotiated {
            capability_id,
            ..
        }) if capability_id == "inspection.read.v1"
    ));
    Ok(())
}

fn rebuild_fixture(
    source: &ScenarioFixture,
    fixture_id: &str,
    handshake: tracedecay_memory_conformance::HandshakeFixture,
    operations: Vec<tracedecay_memory_conformance::OperationFixture>,
) -> Result<ScenarioFixture, EvaluationError> {
    ScenarioFixture::new(
        fixture_id,
        source.identity().clone(),
        source.exact_scope().clone(),
        source.registration_revision(),
        handshake,
        operations,
    )
}

fn fixture_identity_for(
    provider_id: &str,
    build_digest: &str,
    contract: ContractIdentity,
) -> Result<FixtureIdentity, EvaluationError> {
    let descriptor = ProviderDescriptor::new(
        OwnedProviderId::new(provider_id)?,
        build_digest,
        "canonical-dummy.v1",
        0,
        mandatory_capabilities()?,
        limits(),
    )?;
    Ok(FixtureIdentity::new(
        contract,
        ProviderBuildIdentity::from_descriptor(&descriptor)?,
    ))
}

fn exact_scope() -> Result<OwnedExactScope, ApiError> {
    OwnedExactScope::new(
        "profile-conformance",
        "project-conformance",
        "repository-conformance",
        "worktree-conformance",
        "refs/heads/conformance",
        "session-conformance",
        1,
    )
}

fn alternate_scope() -> Result<OwnedExactScope, ApiError> {
    OwnedExactScope::new(
        "profile-conformance",
        "project-conformance",
        "repository-conformance",
        "worktree-conformance-alternate",
        "refs/heads/conformance",
        "session-conformance",
        1,
    )
}

fn limits() -> ProviderLimits {
    ProviderLimits {
        request_bytes: 1_048_576,
        response_bytes: 1_048_576,
        observation_batch_items: 1_024,
        recall_candidates: 1_024,
        concurrent_operations: 8,
        operation_millis: 5_000,
        snapshot_bytes: 16_777_216,
        inspection_items: 1_024,
    }
}

fn mandatory_capabilities() -> Result<Vec<OwnedVersionedId>, ApiError> {
    [
        "provider.health.v1",
        "observation.accept.v1",
        "recall.query.v1",
    ]
    .into_iter()
    .map(OwnedVersionedId::new)
    .collect()
}

fn health_payload(result: &canonical_dummy::HealthResult) -> Result<CanonicalPayload, ApiError> {
    canonical_payload(
        "tracedecay.memory.provider.health.v1",
        format!(
            "{{\"state_generation\":{},\"stored_observations\":{}}}",
            result.state_generation, result.stored_observations
        ),
    )
}

fn observation_payload(
    result: &canonical_dummy::ObservationResult,
) -> Result<CanonicalPayload, ApiError> {
    let acceptance = match result.acceptance {
        canonical_dummy::ObservationAcceptance::Applied => "applied",
        canonical_dummy::ObservationAcceptance::DuplicateAcknowledged => "duplicate_acknowledged",
    };
    canonical_payload(
        "tracedecay.memory.provider.observation.v1",
        format!(
            "{{\"acceptance\":\"{acceptance}\",\"acknowledged_sequence\":{}}}",
            result.acknowledged_sequence
        ),
    )
}

fn recall_payload(result: &canonical_dummy::RecallResult) -> Result<CanonicalPayload, ApiError> {
    canonical_payload(
        "tracedecay.memory.provider.recall.v1",
        format!(
            "{{\"candidate_count\":{},\"coverage_complete\":{}}}",
            result.candidates.len(),
            result.coverage_complete
        ),
    )
}

fn canonical_payload(contract_id: &str, value: String) -> Result<CanonicalPayload, ApiError> {
    let bytes = value.into_bytes();
    let sha256 = hex_digest(&Sha256::digest(&bytes));
    CanonicalPayload::new(OwnedVersionedId::new(contract_id)?, bytes, sha256)
}

fn recall_request_payload(
    query: &str,
    maximum_candidates: usize,
) -> Result<CanonicalPayload, ApiError> {
    canonical_payload(
        "tracedecay.memory.provider.recall.v1",
        format!("query={query}\nmaximum_candidates={maximum_candidates}\n"),
    )
}

fn hex_digest(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len().saturating_mul(2));
    for byte in value {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn convert_terminal(value: canonical_dummy::contract::TerminalCode) -> TerminalCode {
    match TerminalCode::from_wire(value.as_wire()) {
        Some(value) => value,
        None => TerminalCode::InternalFailure,
    }
}

fn convert_effect(value: canonical_dummy::contract::CommittedEffectState) -> CommittedEffectState {
    match CommittedEffectState::from_wire(value.as_wire()) {
        Some(value) => value,
        None => CommittedEffectState::Unknown,
    }
}

fn convert_fallback(value: canonical_dummy::contract::FallbackEligibility) -> FallbackEligibility {
    match FallbackEligibility::from_wire(value.as_wire()) {
        Some(value) => value,
        None => FallbackEligibility::Forbidden,
    }
}
