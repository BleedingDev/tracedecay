use std::collections::BTreeSet;

use tracedecay_memory_provider_api::contract::TerminalCode;
use tracedecay_memory_provider_api::{
    ApiError, CommittedEffectEvidence, FallbackDirective, HandshakeRequest, HandshakeRequestParts,
    HandshakeResponse, MemoryProvider, PayloadSanitizationReceipt, PayloadSanitizationReceiptParts,
    ProviderCall, ProviderCallParts, ProviderDescriptor, ProviderOperation, ProviderReply,
    TerminalRecord,
};

use crate::fixture::{
    EffectGenerationExpectation, ExpectedCommittedEffect, GenerationExpectation, HandshakeFixture,
    ItemRefsExpectation, OperationFixture, OptionalTextExpectation, PayloadExpectation,
    is_lowercase_sha256,
};
use crate::{
    ConformanceViolation, ContractIdentity, EvaluationError, FixtureIdentity, ObserverRunReport,
    ObserverStepResult, ProductRunReport, ProductStepOutput, ProductStepResult,
    ProviderBuildIdentity, ScenarioFixture, StepEvaluation,
};

/// Provider-neutral scenario runner bound to one real provider implementation.
pub struct ProviderHarness<'provider> {
    provider: &'provider dyn MemoryProvider,
    provider_identity: ProviderBuildIdentity,
}

impl<'provider> ProviderHarness<'provider> {
    /// Binds a harness to the exact provider and immutable build identity in its descriptor.
    pub fn new(provider: &'provider dyn MemoryProvider) -> Result<Self, EvaluationError> {
        let descriptor = provider.descriptor();
        Ok(Self {
            provider,
            provider_identity: ProviderBuildIdentity::from_descriptor(&descriptor)?,
        })
    }

    /// Returns the exact identity every fixture for this harness must carry.
    #[must_use]
    pub fn fixture_identity(&self) -> FixtureIdentity {
        FixtureIdentity::new(ContractIdentity::current(), self.provider_identity.clone())
    }

    /// Runs a scenario as an active/product provider and retains its typed outputs.
    pub fn run_product(
        &self,
        fixture: &ScenarioFixture,
    ) -> Result<ProductRunReport, EvaluationError> {
        let execution = self.execute(fixture)?;
        let steps = execution
            .steps
            .into_iter()
            .map(|step| {
                ProductStepResult::new(step.evaluation, step.output, step.provider_contacted)
            })
            .collect();
        Ok(ProductRunReport::new(
            fixture.scenario_identity(),
            fixture.planned_step_ids().map(str::to_owned).collect(),
            steps,
        ))
    }

    /// Runs a scenario in observer mode and irreversibly drops operation payload output.
    pub fn run_observer(
        &self,
        fixture: &ScenarioFixture,
    ) -> Result<ObserverRunReport, EvaluationError> {
        let execution = self.execute(fixture)?;
        let steps = execution
            .steps
            .into_iter()
            .map(|step| {
                let observed = step
                    .output
                    .summary()
                    .with_fixture_controlled_terminal_identity(step.evaluation.violations());
                ObserverStepResult::new(step.evaluation, observed, step.provider_contacted)
            })
            .collect();
        Ok(ObserverRunReport::new(
            fixture.scenario_identity(),
            fixture.planned_step_ids().map(str::to_owned).collect(),
            steps,
        ))
    }

    fn execute(&self, fixture: &ScenarioFixture) -> Result<Execution, EvaluationError> {
        self.validate_fixture_identity(fixture)?;
        let initial_descriptor = self.provider.descriptor();
        self.provider_identity
            .require_match(&ProviderBuildIdentity::from_descriptor(
                &initial_descriptor,
            )?)?;

        let handshake_request = materialize_handshake(fixture)?;
        let (handshake_response, handshake_provider_contacted) =
            match handshake_request.control.snapshot() {
                Ok(_) => (self.provider.handshake(&handshake_request), true),
                Err(terminal_code) => (
                    host_handshake_control_response(
                        &handshake_request,
                        terminal_code,
                        initial_descriptor.state_generation,
                    )?,
                    false,
                ),
            };
        let handshake_violations = evaluate_handshake(
            fixture.handshake(),
            &handshake_request,
            &handshake_response,
            &initial_descriptor,
            &self.provider_identity,
            fixture.exact_scope(),
        );
        let handshake_evaluation =
            StepEvaluation::new(&fixture.handshake().step_id, handshake_violations);
        let may_invoke = handshake_evaluation.passed()
            && handshake_response.terminal.terminal_code() == TerminalCode::Success;
        let ready_receipt = handshake_response.ready_receipt_sha256.clone();
        let mut state_generation = handshake_response
            .descriptor
            .as_ref()
            .map_or(initial_descriptor.state_generation, |descriptor| {
                descriptor.state_generation
            });
        let mut steps = vec![ExecutedStep {
            evaluation: handshake_evaluation,
            output: ProductStepOutput::Handshake(Box::new(handshake_response)),
            provider_contacted: handshake_provider_contacted,
        }];

        let Some(ready_receipt) = ready_receipt.filter(|_| may_invoke) else {
            return Ok(Execution { steps });
        };

        for operation in fixture.operations() {
            let state_generation_before = state_generation;
            let call =
                materialize_operation(fixture, operation, &ready_receipt, state_generation_before)?;
            let (reply, provider_contacted) = match call.control.snapshot() {
                Ok(_) => (self.provider.invoke(&call), true),
                Err(terminal_code) => (
                    host_control_reply(&call, terminal_code, state_generation_before)?,
                    false,
                ),
            };
            let violations = evaluate_operation(operation, &call, &reply, state_generation_before);
            let next_state_generation =
                trusted_state_generation(&reply, &call, state_generation_before);
            steps.push(ExecutedStep {
                evaluation: StepEvaluation::new(&operation.step_id, violations),
                output: ProductStepOutput::Operation(Box::new(reply)),
                provider_contacted,
            });
            let Some(next_state_generation) = next_state_generation else {
                break;
            };
            state_generation = next_state_generation;
        }
        Ok(Execution { steps })
    }

    fn validate_fixture_identity(&self, fixture: &ScenarioFixture) -> Result<(), EvaluationError> {
        fixture.identity().contract().validate_current()?;
        fixture
            .identity()
            .provider()
            .require_match(&self.provider_identity)
    }
}

struct Execution {
    steps: Vec<ExecutedStep>,
}

struct ExecutedStep {
    evaluation: StepEvaluation,
    output: ProductStepOutput,
    provider_contacted: bool,
}

fn host_handshake_control_response(
    request: &HandshakeRequest,
    terminal_code: TerminalCode,
    state_generation: u64,
) -> Result<HandshakeResponse, EvaluationError> {
    Ok(HandshakeResponse {
        terminal: TerminalRecord::new(
            tracedecay_memory_provider_api::ProviderOperation::Handshake,
            request.provider_id.clone(),
            terminal_code,
            CommittedEffectEvidence::none(Some(state_generation)),
            FallbackDirective::forbidden(),
            request.request_id.clone(),
            request.exact_scope.exact_scope_sha256(),
            Some(format!("host.control.{}", terminal_code.as_wire())),
        )?,
        descriptor: None,
        provider_instance_id: None,
        state_namespace: None,
        accepted_scope: None,
        effective_limits: None,
        ready_receipt_sha256: None,
        warnings: Vec::new(),
    })
}

fn host_control_reply(
    call: &ProviderCall,
    terminal_code: TerminalCode,
    state_generation: u64,
) -> Result<ProviderReply, EvaluationError> {
    Ok(ProviderReply {
        terminal: TerminalRecord::new(
            call.operation,
            call.provider_id.clone(),
            terminal_code,
            CommittedEffectEvidence::none(Some(state_generation)),
            FallbackDirective::forbidden(),
            call.operation_id.clone(),
            call.exact_scope.exact_scope_sha256(),
            Some(format!("host.control.{}", terminal_code.as_wire())),
        )?,
        payload: None,
        warnings: Vec::new(),
        extensions: Vec::new(),
        state_generation,
    })
}

fn materialize_handshake(fixture: &ScenarioFixture) -> Result<HandshakeRequest, EvaluationError> {
    let handshake = fixture.handshake();
    Ok(HandshakeRequest::new(HandshakeRequestParts {
        provider_id: fixture.identity().provider().provider_id().clone(),
        registration_revision: fixture.registration_revision(),
        exact_scope: fixture.exact_scope().clone(),
        request_id: handshake.request_id.clone(),
        required_capabilities: handshake.required_capabilities.clone(),
        host_limits: handshake.host_limits,
        control: handshake.control.materialize(),
        challenge_nonce: handshake.challenge_nonce,
    })?)
}

/// Sanitizer revision the harness stamps on the receipts it mints.
///
/// Conformance is provider-neutral and deliberately does not depend on
/// `tracedecay-memory-hygiene`; it exercises the boundary the real pipeline
/// mints into, not the pipeline itself. Fixture payloads are literal contract
/// documents that carry no credential material, so the harness admits them as
/// read-and-unchanged.
pub(crate) const CONFORMANCE_SANITIZER_REVISION: &str =
    "tracedecay.memory.observation.hygiene.v1+conformance-harness";

/// Attaches the receipt an admitted observation must carry.
///
/// `ProviderCall::validate` fails closed for observations without one, so every
/// scenario step that observes is admitted here before dispatch.
pub(crate) fn admitted(call: ProviderCall) -> Result<ProviderCall, ApiError> {
    if call.operation != ProviderOperation::Observe {
        return Ok(call);
    }
    let receipt =
        PayloadSanitizationReceipt::new(PayloadSanitizationReceiptParts::accepted_unmodified(
            CONFORMANCE_SANITIZER_REVISION,
            call.payload.sha256.clone(),
        ))?;
    Ok(call.with_sanitization(receipt))
}

fn materialize_operation(
    fixture: &ScenarioFixture,
    operation: &OperationFixture,
    ready_receipt_sha256: &str,
    state_generation: u64,
) -> Result<ProviderCall, EvaluationError> {
    Ok(admitted(ProviderCall::new(ProviderCallParts {
        operation: operation.operation,
        provider_id: fixture.identity().provider().provider_id().clone(),
        registration_revision: fixture.registration_revision(),
        ready_receipt_sha256: ready_receipt_sha256.to_owned(),
        exact_scope: fixture.exact_scope().clone(),
        request_id: operation.request_id.clone(),
        operation_id: operation.operation_id.clone(),
        expected_state_generation: state_generation,
        idempotency_key: operation.idempotency_key.clone(),
        control: operation.control.materialize(),
        payload: operation.payload.clone(),
        required_capabilities: operation.required_capabilities.clone(),
        extensions: operation.extensions.clone(),
    })?)?)
}

fn evaluate_handshake(
    fixture: &HandshakeFixture,
    request: &HandshakeRequest,
    response: &HandshakeResponse,
    initial_descriptor: &ProviderDescriptor,
    expected_identity: &ProviderBuildIdentity,
    exact_scope: &tracedecay_memory_provider_api::OwnedExactScope,
) -> Vec<ConformanceViolation> {
    let step_id = &fixture.step_id;
    let mut violations = Vec::new();
    compare_terminal_code(
        &mut violations,
        step_id,
        fixture.expectation.terminal_code,
        response.terminal.terminal_code(),
    );
    evaluate_committed_effect(
        &mut violations,
        step_id,
        &fixture.expectation.committed_effect,
        response.terminal.committed_effect(),
        initial_descriptor.state_generation,
        response
            .descriptor
            .as_ref()
            .map_or(initial_descriptor.state_generation, |descriptor| {
                descriptor.state_generation
            }),
    );
    evaluate_fallback(
        &mut violations,
        step_id,
        &fixture.expectation.fallback,
        response.terminal.fallback(),
    );
    compare_string(
        &mut violations,
        step_id,
        "terminal.operation_id",
        &request.request_id,
        response.terminal.operation_id(),
    );
    validate_terminal_identity(
        &mut violations,
        step_id,
        &response.terminal,
        tracedecay_memory_provider_api::ProviderOperation::Handshake,
        &request.provider_id,
        &request.exact_scope.exact_scope_sha256(),
    );

    match &response.descriptor {
        Some(descriptor) => {
            if let Err(error) = descriptor.validate() {
                violations.push(ConformanceViolation::new(
                    step_id,
                    "descriptor.validity",
                    "valid provider descriptor",
                    error.to_string(),
                ));
            }
            compare_string(
                &mut violations,
                step_id,
                "descriptor.provider_id",
                expected_identity.provider_id().as_str(),
                descriptor.provider_id.as_str(),
            );
            compare_string(
                &mut violations,
                step_id,
                "descriptor.build_identity_sha256",
                expected_identity.build_identity_sha256(),
                &descriptor.implementation_identity_sha256,
            );
            compare_string(
                &mut violations,
                step_id,
                "descriptor.state_schema_version",
                initial_descriptor.state_schema_version.as_str(),
                descriptor.state_schema_version.as_str(),
            );
            compare_debug(
                &mut violations,
                step_id,
                "descriptor.state_generation",
                &initial_descriptor.state_generation,
                &descriptor.state_generation,
            );
            compare_debug(
                &mut violations,
                step_id,
                "descriptor.protocol_major",
                &initial_descriptor.protocol_major,
                &descriptor.protocol_major,
            );
            compare_debug(
                &mut violations,
                step_id,
                "descriptor.protocol_minor",
                &initial_descriptor.protocol_minor,
                &descriptor.protocol_minor,
            );
            let expected_capabilities = initial_descriptor
                .capabilities
                .iter()
                .map(|capability| capability.as_str())
                .collect::<BTreeSet<_>>();
            let actual_capabilities = descriptor
                .capabilities
                .iter()
                .map(|capability| capability.as_str())
                .collect::<BTreeSet<_>>();
            compare_debug(
                &mut violations,
                step_id,
                "descriptor.capabilities",
                &expected_capabilities,
                &actual_capabilities,
            );
            compare_debug(
                &mut violations,
                step_id,
                "descriptor.limits",
                &initial_descriptor.limits,
                &descriptor.limits,
            );
            for capability in &request.required_capabilities {
                if !descriptor.supports(capability.as_str()) {
                    violations.push(ConformanceViolation::new(
                        step_id,
                        "descriptor.required_capability",
                        capability.as_str(),
                        "missing",
                    ));
                }
            }
            let expected_limits = request.host_limits.minimum(initial_descriptor.limits);
            if response.effective_limits != Some(expected_limits) {
                violations.push(ConformanceViolation::new(
                    step_id,
                    "effective_limits",
                    format!("{expected_limits:?}"),
                    format!("{:?}", response.effective_limits),
                ));
            }
        }
        None if fixture.expectation.require_descriptor => {
            violations.push(ConformanceViolation::new(
                step_id,
                "descriptor",
                "present",
                "absent",
            ));
        }
        None => {}
    }

    match response.accepted_scope.as_ref() {
        Some(actual) if actual != exact_scope => violations.push(ConformanceViolation::new(
            step_id,
            "accepted_scope",
            "exact fixture scope",
            "different scope",
        )),
        None if fixture.expectation.require_accepted_scope => {
            violations.push(ConformanceViolation::new(
                step_id,
                "accepted_scope",
                "exact fixture scope",
                "absent",
            ));
        }
        Some(_) | None => {}
    }
    if response.terminal.terminal_code() == TerminalCode::Success {
        require_nonempty_option(
            &mut violations,
            step_id,
            "provider_instance_id",
            response.provider_instance_id.as_deref(),
        );
        require_nonempty_option(
            &mut violations,
            step_id,
            "state_namespace",
            response.state_namespace.as_deref(),
        );
    }
    match response.ready_receipt_sha256.as_deref() {
        Some(receipt) if is_lowercase_sha256(receipt) => {}
        Some(receipt) => violations.push(ConformanceViolation::new(
            step_id,
            "ready_receipt_sha256",
            "lowercase SHA-256",
            receipt,
        )),
        None if fixture.expectation.require_ready_receipt => {
            violations.push(ConformanceViolation::new(
                step_id,
                "ready_receipt_sha256",
                "present",
                "absent",
            ));
        }
        None => {}
    }
    violations
}

fn evaluate_operation(
    fixture: &OperationFixture,
    call: &ProviderCall,
    reply: &ProviderReply,
    state_generation_before: u64,
) -> Vec<ConformanceViolation> {
    let step_id = &fixture.step_id;
    let mut violations = Vec::new();
    if !fixture
        .expectation
        .terminal
        .accepts(reply.terminal.terminal_code())
    {
        violations.push(ConformanceViolation::new(
            step_id,
            "terminal_code",
            fixture
                .expectation
                .terminal
                .iter()
                .map(TerminalCode::as_wire)
                .collect::<Vec<_>>()
                .join("|"),
            reply.terminal.terminal_code().as_wire(),
        ));
    }
    evaluate_committed_effect(
        &mut violations,
        step_id,
        &fixture.expectation.committed_effect,
        reply.terminal.committed_effect(),
        state_generation_before,
        reply.state_generation,
    );
    evaluate_duplicate_binding(
        &mut violations,
        step_id,
        call,
        reply.terminal.committed_effect(),
    );
    evaluate_fallback(
        &mut violations,
        step_id,
        &fixture.expectation.fallback,
        reply.terminal.fallback(),
    );
    compare_string(
        &mut violations,
        step_id,
        "terminal.operation_id",
        &call.operation_id,
        reply.terminal.operation_id(),
    );
    validate_terminal_identity(
        &mut violations,
        step_id,
        &reply.terminal,
        call.operation,
        &call.provider_id,
        &call.exact_scope.exact_scope_sha256(),
    );
    evaluate_generation(
        &mut violations,
        step_id,
        fixture.expectation.state_generation,
        state_generation_before,
        reply.state_generation,
    );
    evaluate_payload(
        &mut violations,
        step_id,
        &fixture.expectation.payload,
        reply.payload.as_ref(),
    );
    violations
}

fn compare_terminal_code(
    violations: &mut Vec<ConformanceViolation>,
    step_id: &str,
    expected: TerminalCode,
    actual: TerminalCode,
) {
    if expected != actual {
        violations.push(ConformanceViolation::new(
            step_id,
            "terminal_code",
            expected.as_wire(),
            actual.as_wire(),
        ));
    }
}

/// A duplicate acknowledgement is evidence about the mutation the caller
/// actually sent. A provider that returns one naming a different key — or one
/// for a call that never carried a key — is claiming credit for someone else's
/// prior work, so the binding is checked against the call rather than against
/// the fixture's own expectation.
fn evaluate_duplicate_binding(
    violations: &mut Vec<ConformanceViolation>,
    step_id: &str,
    call: &ProviderCall,
    actual: &CommittedEffectEvidence,
) {
    if actual.state() != tracedecay_memory_provider_api::contract::CommittedEffectState::Duplicate {
        return;
    }
    let request_key = call.idempotency_key.as_deref().unwrap_or_default();
    let claimed_key = actual.duplicate_of_idempotency_key().unwrap_or_default();
    if request_key.is_empty() || claimed_key != request_key {
        violations.push(ConformanceViolation::new(
            step_id,
            "terminal.committed_effect.duplicate_of_idempotency_key_binding",
            if request_key.is_empty() {
                "request idempotency key".to_owned()
            } else {
                request_key.to_owned()
            },
            claimed_key.to_owned(),
        ));
    }
    if actual.duplicate_of_operation_id().is_none_or(str::is_empty) {
        violations.push(ConformanceViolation::new(
            step_id,
            "terminal.committed_effect.duplicate_of_operation_id_binding",
            "present",
            "absent",
        ));
    }
}

fn evaluate_committed_effect(
    violations: &mut Vec<ConformanceViolation>,
    step_id: &str,
    expected: &ExpectedCommittedEffect,
    actual: &CommittedEffectEvidence,
    state_generation_before: u64,
    state_generation_after: u64,
) {
    if expected.state != actual.state() {
        violations.push(ConformanceViolation::new(
            step_id,
            "terminal.committed_effect.state",
            expected.state.as_wire(),
            actual.state().as_wire(),
        ));
    }
    evaluate_optional_text(
        violations,
        step_id,
        "terminal.committed_effect.committed_boundary",
        &expected.committed_boundary,
        actual.committed_boundary(),
        false,
    );
    evaluate_effect_generation(
        violations,
        step_id,
        "terminal.committed_effect.state_generation_before",
        expected.state_generation_before,
        actual.state_generation_before(),
        state_generation_before,
        state_generation_after,
    );
    evaluate_effect_generation(
        violations,
        step_id,
        "terminal.committed_effect.state_generation_after",
        expected.state_generation_after,
        actual.state_generation_after(),
        state_generation_before,
        state_generation_after,
    );
    evaluate_item_refs(
        violations,
        step_id,
        "terminal.committed_effect.committed_item_refs",
        &expected.committed_item_refs,
        actual.committed_item_refs(),
    );
    evaluate_item_refs(
        violations,
        step_id,
        "terminal.committed_effect.uncommitted_item_refs",
        &expected.uncommitted_item_refs,
        actual.uncommitted_item_refs(),
    );
    evaluate_optional_text(
        violations,
        step_id,
        "terminal.committed_effect.provider_receipt_sha256",
        &expected.provider_receipt_sha256,
        actual.provider_receipt_sha256(),
        true,
    );
    let generations_explicitly_unknown =
        actual.state_generation_before().is_none() && actual.state_generation_after().is_none();
    let requires_envelope_generation_binding = match actual.state() {
        tracedecay_memory_provider_api::contract::CommittedEffectState::None => {
            !generations_explicitly_unknown
        }
        // A duplicate must name the generation too: it asserts the generation
        // did not move, which is only checkable against the envelope.
        tracedecay_memory_provider_api::contract::CommittedEffectState::Committed
        | tracedecay_memory_provider_api::contract::CommittedEffectState::Duplicate
        | tracedecay_memory_provider_api::contract::CommittedEffectState::Partial => true,
        tracedecay_memory_provider_api::contract::CommittedEffectState::Unknown => false,
    };
    if requires_envelope_generation_binding {
        compare_debug(
            violations,
            step_id,
            "terminal.committed_effect.envelope_generation_before",
            &Some(state_generation_before),
            &actual.state_generation_before(),
        );
        compare_debug(
            violations,
            step_id,
            "terminal.committed_effect.envelope_generation_after",
            &Some(state_generation_after),
            &actual.state_generation_after(),
        );
    }
    evaluate_optional_text(
        violations,
        step_id,
        "terminal.committed_effect.reconciliation_action",
        &expected.reconciliation_action,
        actual.reconciliation_action(),
        false,
    );
    evaluate_optional_text(
        violations,
        step_id,
        "terminal.committed_effect.verification_sha256",
        &expected.verification_sha256,
        actual.verification_sha256(),
        true,
    );
    evaluate_optional_text(
        violations,
        step_id,
        "terminal.committed_effect.duplicate_of_idempotency_key",
        &expected.duplicate_of_idempotency_key,
        actual.duplicate_of_idempotency_key(),
        // Not shape-checked here: this field echoes the key the *caller* sent,
        // whose encoding is the observation contract's business, and a scenario
        // may drive a provider with a readable key. The binding that matters —
        // that the echo is this call's own key — is checked against the call in
        // `evaluate_duplicate_binding`, which no key encoding can satisfy by
        // accident.
        false,
    );
    evaluate_optional_text(
        violations,
        step_id,
        "terminal.committed_effect.duplicate_of_operation_id",
        &expected.duplicate_of_operation_id,
        actual.duplicate_of_operation_id(),
        false,
    );
}

fn evaluate_effect_generation(
    violations: &mut Vec<ConformanceViolation>,
    step_id: &str,
    field: &'static str,
    expected: EffectGenerationExpectation,
    actual: Option<u64>,
    state_generation_before: u64,
    state_generation_after: u64,
) {
    let expected = match expected {
        EffectGenerationExpectation::Unknown => None,
        EffectGenerationExpectation::OperationBefore => Some(state_generation_before),
        EffectGenerationExpectation::OperationAfter => Some(state_generation_after),
        EffectGenerationExpectation::Exact(value) => Some(value),
    };
    compare_debug(violations, step_id, field, &expected, &actual);
}

fn evaluate_optional_text(
    violations: &mut Vec<ConformanceViolation>,
    step_id: &str,
    field: &'static str,
    expected: &OptionalTextExpectation,
    actual: Option<&str>,
    require_sha256: bool,
) {
    let matches = match expected {
        OptionalTextExpectation::Absent => actual.is_none(),
        OptionalTextExpectation::Present => actual.is_some(),
        OptionalTextExpectation::Exact(expected) => actual == Some(expected),
    };
    if !matches {
        let expected = match expected {
            OptionalTextExpectation::Absent => "absent".to_owned(),
            OptionalTextExpectation::Present => "present".to_owned(),
            OptionalTextExpectation::Exact(expected) => expected.clone(),
        };
        violations.push(ConformanceViolation::new(
            step_id,
            field,
            expected,
            actual.unwrap_or("absent"),
        ));
    }
    if require_sha256
        && let Some(actual) = actual
        && !is_lowercase_sha256(actual)
    {
        violations.push(ConformanceViolation::new(
            step_id,
            field,
            "lowercase SHA-256",
            actual,
        ));
    }
}

fn evaluate_item_refs(
    violations: &mut Vec<ConformanceViolation>,
    step_id: &str,
    field: &'static str,
    expected: &ItemRefsExpectation,
    actual: &[String],
) {
    let matches = match expected {
        ItemRefsExpectation::Empty => actual.is_empty(),
        ItemRefsExpectation::Any => true,
        ItemRefsExpectation::NonEmpty => !actual.is_empty(),
        ItemRefsExpectation::Exact(expected) => actual == expected,
    };
    if !matches {
        let expected = match expected {
            ItemRefsExpectation::Empty => "empty".to_owned(),
            ItemRefsExpectation::Any => "any bounded partition".to_owned(),
            ItemRefsExpectation::NonEmpty => "nonempty bounded partition".to_owned(),
            ItemRefsExpectation::Exact(expected) => format!("{expected:?}"),
        };
        violations.push(ConformanceViolation::new(
            step_id,
            field,
            expected,
            format!("{actual:?}"),
        ));
    }
}

fn evaluate_fallback(
    violations: &mut Vec<ConformanceViolation>,
    step_id: &str,
    expected: &FallbackDirective,
    actual: &FallbackDirective,
) {
    if expected.eligibility() != actual.eligibility() {
        violations.push(ConformanceViolation::new(
            step_id,
            "terminal.fallback.eligibility",
            expected.eligibility().as_wire(),
            actual.eligibility().as_wire(),
        ));
    }
    compare_debug(
        violations,
        step_id,
        "terminal.fallback.source_provider_id",
        &expected
            .source_provider_id()
            .map(tracedecay_memory_provider_api::OwnedProviderId::as_str),
        &actual
            .source_provider_id()
            .map(tracedecay_memory_provider_api::OwnedProviderId::as_str),
    );
    match (expected.policy(), actual.policy()) {
        (Some(expected), Some(actual)) => {
            compare_string(
                violations,
                step_id,
                "terminal.fallback.policy_id",
                expected.policy_id(),
                actual.policy_id(),
            );
            compare_debug(
                violations,
                step_id,
                "terminal.fallback.policy_revision",
                &expected.policy_revision(),
                &actual.policy_revision(),
            );
            compare_string(
                violations,
                step_id,
                "terminal.fallback.target_provider_id",
                expected.target_provider_id().as_str(),
                actual.target_provider_id().as_str(),
            );
        }
        (Some(_), None) => violations.push(ConformanceViolation::new(
            step_id,
            "terminal.fallback.policy",
            "present",
            "absent",
        )),
        (None, Some(_)) => violations.push(ConformanceViolation::new(
            step_id,
            "terminal.fallback.policy",
            "absent",
            "present",
        )),
        (None, None) => {}
    }
    compare_debug(
        violations,
        step_id,
        "terminal.fallback.reason",
        &expected.reason(),
        &actual.reason(),
    );
}

fn compare_string(
    violations: &mut Vec<ConformanceViolation>,
    step_id: &str,
    field: &'static str,
    expected: &str,
    actual: &str,
) {
    if expected != actual {
        violations.push(ConformanceViolation::new(step_id, field, expected, actual));
    }
}

fn compare_debug<T: std::fmt::Debug + PartialEq>(
    violations: &mut Vec<ConformanceViolation>,
    step_id: &str,
    field: &'static str,
    expected: &T,
    actual: &T,
) {
    if expected != actual {
        violations.push(ConformanceViolation::new(
            step_id,
            field,
            format!("{expected:?}"),
            format!("{actual:?}"),
        ));
    }
}

fn evaluate_generation(
    violations: &mut Vec<ConformanceViolation>,
    step_id: &str,
    expectation: GenerationExpectation,
    before: u64,
    actual: u64,
) {
    let expected = match expectation {
        GenerationExpectation::Any => return,
        GenerationExpectation::Unchanged => Some(before),
        GenerationExpectation::IncreasedBy(delta) => before.checked_add(delta),
    };
    match expected {
        Some(expected) if expected == actual => {}
        Some(expected) => violations.push(ConformanceViolation::new(
            step_id,
            "state_generation",
            expected.to_string(),
            actual.to_string(),
        )),
        None => violations.push(ConformanceViolation::new(
            step_id,
            "state_generation",
            "non-overflowing expected generation",
            actual.to_string(),
        )),
    }
}

fn evaluate_payload(
    violations: &mut Vec<ConformanceViolation>,
    step_id: &str,
    expectation: &PayloadExpectation,
    actual: Option<&tracedecay_memory_provider_api::CanonicalPayload>,
) {
    let matches = match expectation {
        PayloadExpectation::Any => true,
        PayloadExpectation::Present => actual.is_some(),
        PayloadExpectation::Absent => actual.is_none(),
        PayloadExpectation::Exact(expected) => actual == Some(expected),
    };
    if !matches {
        let expected = match expectation {
            PayloadExpectation::Any => "any",
            PayloadExpectation::Present => "present",
            PayloadExpectation::Absent => "absent",
            PayloadExpectation::Exact(_) => "exact canonical payload",
        };
        violations.push(ConformanceViolation::new(
            step_id,
            "payload",
            expected,
            if actual.is_some() {
                "present"
            } else {
                "absent"
            },
        ));
    }
}

fn validate_terminal_identity(
    violations: &mut Vec<ConformanceViolation>,
    step_id: &str,
    terminal: &tracedecay_memory_provider_api::TerminalRecord,
    expected_operation: tracedecay_memory_provider_api::ProviderOperation,
    expected_provider_id: &tracedecay_memory_provider_api::OwnedProviderId,
    expected_scope_sha256: &str,
) {
    compare_debug(
        violations,
        step_id,
        "terminal.operation",
        &expected_operation,
        &terminal.operation(),
    );
    compare_string(
        violations,
        step_id,
        "terminal.provider_id",
        expected_provider_id.as_str(),
        terminal.provider_id().as_str(),
    );
    if terminal.exact_scope_sha256() != expected_scope_sha256 {
        violations.push(ConformanceViolation::new(
            step_id,
            "terminal.exact_scope_sha256",
            expected_scope_sha256,
            terminal.exact_scope_sha256(),
        ));
    }
    if let Some(receipt) = terminal.provider_receipt_sha256()
        && !is_lowercase_sha256(receipt)
    {
        violations.push(ConformanceViolation::new(
            step_id,
            "terminal.provider_receipt_sha256",
            "lowercase SHA-256",
            receipt,
        ));
    }
}

fn trusted_state_generation(
    reply: &ProviderReply,
    call: &ProviderCall,
    state_generation_before: u64,
) -> Option<u64> {
    if reply.terminal.operation() != call.operation
        || reply.terminal.provider_id() != &call.provider_id
        || reply.terminal.operation_id() != call.operation_id
        || reply.terminal.exact_scope_sha256() != call.exact_scope.exact_scope_sha256()
    {
        return None;
    }
    let effect = reply.terminal.committed_effect();
    if effect.state() == tracedecay_memory_provider_api::contract::CommittedEffectState::Unknown
        || effect.state_generation_before() != Some(state_generation_before)
        || effect.state_generation_after() != Some(reply.state_generation)
    {
        return None;
    }
    Some(reply.state_generation)
}

fn require_nonempty_option(
    violations: &mut Vec<ConformanceViolation>,
    step_id: &str,
    field: &'static str,
    value: Option<&str>,
) {
    if value.is_none_or(str::is_empty) {
        violations.push(ConformanceViolation::new(
            step_id,
            field,
            "nonempty",
            value.unwrap_or("absent"),
        ));
    }
}
