use std::collections::{BTreeMap, BTreeSet};

use tracedecay_memory_provider_api::{
    HandshakeResponse, OwnedExactScope, ProviderDescriptor, ProviderLimits, ProviderReply,
    TerminalRecord,
};

use crate::{EvaluationError, FixtureIdentity, ScenarioIdentity};

/// Overall result of a complete conformance run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConformanceStatus {
    /// Every planned step executed and satisfied its typed expectation.
    Pass,
    /// At least one step failed or could not execute after a failed prerequisite.
    Fail,
}

/// Kind of provider call represented by a result row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StepKind {
    /// Provider readiness handshake.
    Handshake,
    /// Capability-routed provider operation.
    Operation,
}

/// One deterministic expected-versus-actual conformance difference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConformanceViolation {
    /// Stable scenario step identity.
    pub step_id: String,
    /// Stable field or invariant name.
    pub field: &'static str,
    /// Deterministic expected value.
    pub expected: String,
    /// Deterministic actual value.
    pub actual: String,
}

impl ConformanceViolation {
    pub(crate) fn new(
        step_id: &str,
        field: &'static str,
        expected: impl Into<String>,
        actual: impl Into<String>,
    ) -> Self {
        Self {
            step_id: step_id.to_owned(),
            field,
            expected: expected.into(),
            actual: actual.into(),
        }
    }
}

/// Evaluation attached to either a retained product output or a payload-isolated observer summary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StepEvaluation {
    step_id: String,
    status: ConformanceStatus,
    violations: Vec<ConformanceViolation>,
}

impl StepEvaluation {
    pub(crate) fn new(step_id: &str, violations: Vec<ConformanceViolation>) -> Self {
        let status = if violations.is_empty() {
            ConformanceStatus::Pass
        } else {
            ConformanceStatus::Fail
        };
        Self {
            step_id: step_id.to_owned(),
            status,
            violations,
        }
    }

    /// Returns the stable scenario step identity.
    #[must_use]
    pub fn step_id(&self) -> &str {
        &self.step_id
    }

    /// Returns the step conformance status.
    #[must_use]
    pub const fn status(&self) -> ConformanceStatus {
        self.status
    }

    /// Returns deterministic conformance differences.
    #[must_use]
    pub fn violations(&self) -> &[ConformanceViolation] {
        &self.violations
    }

    /// Returns whether this executed step satisfied every expectation.
    #[must_use]
    pub const fn passed(&self) -> bool {
        matches!(self.status, ConformanceStatus::Pass)
    }
}

/// Typed provider output retained only by an active/product run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProductStepOutput {
    /// Full typed handshake response.
    Handshake(Box<HandshakeResponse>),
    /// Full typed operation reply, including any canonical provider payload.
    Operation(Box<ProviderReply>),
}

impl ProductStepOutput {
    pub(crate) fn summary(&self) -> ObservedStepSummary {
        match self {
            Self::Handshake(response) => ObservedStepSummary::from_handshake(response),
            Self::Operation(reply) => ObservedStepSummary::from_reply(reply),
        }
    }
}

/// One active/product step with its full typed provider output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProductStepResult {
    evaluation: StepEvaluation,
    output: ProductStepOutput,
    provider_contacted: bool,
}

impl ProductStepResult {
    pub(crate) const fn new(
        evaluation: StepEvaluation,
        output: ProductStepOutput,
        provider_contacted: bool,
    ) -> Self {
        Self {
            evaluation,
            output,
            provider_contacted,
        }
    }

    /// Returns the expectation evaluation.
    #[must_use]
    pub const fn evaluation(&self) -> &StepEvaluation {
        &self.evaluation
    }

    /// Returns the full typed provider output for product-path tests.
    #[must_use]
    pub const fn output(&self) -> &ProductStepOutput {
        &self.output
    }

    /// Returns whether evaluation of this step contacted provider code.
    #[must_use]
    pub const fn provider_contacted(&self) -> bool {
        self.provider_contacted
    }
}

/// Sanitized provider observation safe to retain from observer-mode execution.
///
/// This type intentionally has no payload bytes, canonical payload, handshake
/// response, provider reply, or product-output field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedStepSummary {
    /// Kind of provider call observed.
    pub kind: StepKind,
    /// Complete validated terminal consequence, excluding provider payload bytes.
    pub terminal: TerminalRecord,
    /// Provider-local state generation, when one was exposed.
    pub state_generation: Option<u64>,
    /// Exact readiness receipt returned by a handshake, when present.
    pub ready_receipt_sha256: Option<String>,
    /// Complete validated handshake descriptor, when present.
    pub descriptor: Option<ProviderDescriptor>,
    /// Provider runtime instance identity returned by a handshake.
    pub provider_instance_id: Option<String>,
    /// Provider-local state namespace returned by a handshake.
    pub state_namespace: Option<String>,
    /// Exact scope accepted by a handshake.
    pub accepted_scope: Option<OwnedExactScope>,
    /// Effective limits negotiated by a handshake.
    pub effective_limits: Option<ProviderLimits>,
    /// Whether an operation payload was present, without retaining its bytes.
    pub payload_present: bool,
}

impl ObservedStepSummary {
    pub(crate) fn from_handshake(response: &HandshakeResponse) -> Self {
        Self {
            kind: StepKind::Handshake,
            terminal: response.terminal.clone(),
            state_generation: response
                .descriptor
                .as_ref()
                .map(|descriptor| descriptor.state_generation),
            ready_receipt_sha256: response.ready_receipt_sha256.clone(),
            descriptor: response.descriptor.clone(),
            provider_instance_id: response.provider_instance_id.clone(),
            state_namespace: response.state_namespace.clone(),
            accepted_scope: response.accepted_scope.clone(),
            effective_limits: response.effective_limits,
            payload_present: false,
        }
    }

    pub(crate) fn from_reply(reply: &ProviderReply) -> Self {
        Self {
            kind: StepKind::Operation,
            terminal: reply.terminal.clone(),
            state_generation: Some(reply.state_generation),
            ready_receipt_sha256: None,
            descriptor: None,
            provider_instance_id: None,
            state_namespace: None,
            accepted_scope: None,
            effective_limits: None,
            payload_present: reply.payload.is_some(),
        }
    }

    /// Replaces failed fixture-controlled terminal-identity strings with their
    /// expected values so provider-invented identities never persist in
    /// retained summaries. The deviation itself stays visible through the
    /// step's failed invariant fields; provider-attested evidence such as
    /// committed-effect receipts and fallback policies is retained raw.
    pub(crate) fn with_fixture_controlled_terminal_identity(
        mut self,
        violations: &[ConformanceViolation],
    ) -> Self {
        let expected_for = |field: &str| {
            violations
                .iter()
                .find(|violation| violation.field == field)
                .map(|violation| violation.expected.clone())
        };
        let operation_id = expected_for("terminal.operation_id");
        let exact_scope_sha256 = expected_for("terminal.exact_scope_sha256");
        if operation_id.is_none() && exact_scope_sha256.is_none() {
            return self;
        }
        if let Ok(terminal) = TerminalRecord::new(
            self.terminal.operation(),
            self.terminal.provider_id().clone(),
            self.terminal.terminal_code(),
            self.terminal.committed_effect().clone(),
            self.terminal.fallback().clone(),
            operation_id.unwrap_or_else(|| self.terminal.operation_id().to_owned()),
            exact_scope_sha256.unwrap_or_else(|| self.terminal.exact_scope_sha256().to_owned()),
            self.terminal.diagnostic_id().map(str::to_owned),
        ) {
            self.terminal = terminal;
        }
        self
    }
}

/// One observer step containing terminal consequences but no operation payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObserverStepResult {
    evaluation: ObserverStepEvaluation,
    observed: ObservedStepSummary,
    provider_contacted: bool,
}

impl ObserverStepResult {
    pub(crate) fn new(
        evaluation: StepEvaluation,
        observed: ObservedStepSummary,
        provider_contacted: bool,
    ) -> Self {
        Self {
            evaluation: ObserverStepEvaluation::from_product_evaluation(evaluation),
            observed,
            provider_contacted,
        }
    }

    /// Returns the expectation evaluation.
    #[must_use]
    pub const fn evaluation(&self) -> &ObserverStepEvaluation {
        &self.evaluation
    }

    /// Returns the payload-isolated provider observation.
    #[must_use]
    pub const fn observed(&self) -> &ObservedStepSummary {
        &self.observed
    }

    /// Returns whether evaluation of this step contacted provider code.
    #[must_use]
    pub const fn provider_contacted(&self) -> bool {
        self.provider_contacted
    }
}

/// Payload-isolated observer evaluation retaining only fixture-controlled field identities.
///
/// Expected and actual provider strings are deliberately discarded before an
/// observer report is constructed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObserverStepEvaluation {
    step_id: String,
    status: ConformanceStatus,
    failed_fields: Vec<&'static str>,
}

impl ObserverStepEvaluation {
    fn from_product_evaluation(evaluation: StepEvaluation) -> Self {
        Self {
            step_id: evaluation.step_id,
            status: evaluation.status,
            failed_fields: evaluation
                .violations
                .into_iter()
                .map(|violation| violation.field)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        }
    }

    /// Returns the stable fixture-controlled scenario step identity.
    #[must_use]
    pub fn step_id(&self) -> &str {
        &self.step_id
    }

    /// Returns the observer step conformance status.
    #[must_use]
    pub const fn status(&self) -> ConformanceStatus {
        self.status
    }

    /// Returns fixture-controlled names of fields that failed conformance.
    #[must_use]
    pub fn failed_fields(&self) -> &[&'static str] {
        &self.failed_fields
    }

    /// Returns whether the observer step satisfied every expectation.
    #[must_use]
    pub const fn passed(&self) -> bool {
        matches!(self.status, ConformanceStatus::Pass)
    }
}

/// Exact execution and conformance counts for one run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RunSummary {
    /// Planned handshake plus operation count.
    pub planned_steps: usize,
    /// Planned steps that were evaluated, including host-side control preflights.
    pub executed_steps: usize,
    /// Evaluated steps that crossed into provider code.
    pub provider_contacted_steps: usize,
    /// Evaluated operations completed by host control preflight without provider contact.
    pub host_preflight_steps: usize,
    /// Executed steps with no conformance differences.
    pub passed_steps: usize,
    /// Executed steps with at least one conformance difference.
    pub failed_steps: usize,
    /// Planned steps not run after an unsatisfied handshake prerequisite.
    pub not_run_steps: usize,
    /// Overall run status.
    pub status: ConformanceStatus,
}

impl RunSummary {
    fn from_evaluations(
        planned_steps: usize,
        evaluations: impl IntoIterator<Item = (ConformanceStatus, bool)>,
    ) -> Self {
        let mut executed_steps = 0usize;
        let mut provider_contacted_steps = 0usize;
        let mut passed_steps = 0usize;
        let mut failed_steps = 0usize;
        for (status, provider_contacted) in evaluations {
            executed_steps = executed_steps.saturating_add(1);
            if provider_contacted {
                provider_contacted_steps = provider_contacted_steps.saturating_add(1);
            }
            if matches!(status, ConformanceStatus::Pass) {
                passed_steps = passed_steps.saturating_add(1);
            } else {
                failed_steps = failed_steps.saturating_add(1);
            }
        }
        let not_run_steps = planned_steps.saturating_sub(executed_steps);
        let host_preflight_steps = executed_steps.saturating_sub(provider_contacted_steps);
        let status = if failed_steps == 0 && not_run_steps == 0 {
            ConformanceStatus::Pass
        } else {
            ConformanceStatus::Fail
        };
        Self {
            planned_steps,
            executed_steps,
            provider_contacted_steps,
            host_preflight_steps,
            passed_steps,
            failed_steps,
            not_run_steps,
            status,
        }
    }
}

/// Active/product execution report with exact identities and full typed provider outputs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProductRunReport {
    scenario_identity: ScenarioIdentity,
    planned_step_ids: Vec<String>,
    summary: RunSummary,
    steps: Vec<ProductStepResult>,
}

impl ProductRunReport {
    pub(crate) fn new(
        scenario_identity: ScenarioIdentity,
        planned_step_ids: Vec<String>,
        steps: Vec<ProductStepResult>,
    ) -> Self {
        let summary = RunSummary::from_evaluations(
            planned_step_ids.len(),
            steps
                .iter()
                .map(|step| (step.evaluation().status(), step.provider_contacted())),
        );
        Self {
            scenario_identity,
            planned_step_ids,
            summary,
            steps,
        }
    }

    /// Returns the stable fixture identity.
    #[must_use]
    pub fn fixture_id(&self) -> &str {
        self.scenario_identity.fixture_id()
    }

    /// Returns exact contract, provider, and build identities.
    #[must_use]
    pub const fn identity(&self) -> &FixtureIdentity {
        self.scenario_identity.fixture_identity()
    }

    /// Returns every immutable semantic input that defined this run.
    #[must_use]
    pub const fn scenario_identity(&self) -> &ScenarioIdentity {
        &self.scenario_identity
    }

    /// Returns planned step identities in fixture execution order.
    #[must_use]
    pub fn planned_step_ids(&self) -> &[String] {
        &self.planned_step_ids
    }

    /// Returns exact execution and conformance counts.
    #[must_use]
    pub const fn summary(&self) -> RunSummary {
        self.summary
    }

    /// Returns active results in fixture order.
    #[must_use]
    pub fn steps(&self) -> &[ProductStepResult] {
        &self.steps
    }

    /// Returns whether the complete planned scenario passed.
    #[must_use]
    pub const fn passed(&self) -> bool {
        matches!(self.summary.status, ConformanceStatus::Pass)
    }
}

/// Observer execution report whose shape cannot retain provider payloads or product outputs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObserverRunReport {
    scenario_identity: ScenarioIdentity,
    planned_step_ids: Vec<String>,
    summary: RunSummary,
    steps: Vec<ObserverStepResult>,
}

impl ObserverRunReport {
    pub(crate) fn new(
        scenario_identity: ScenarioIdentity,
        planned_step_ids: Vec<String>,
        steps: Vec<ObserverStepResult>,
    ) -> Self {
        let summary = RunSummary::from_evaluations(
            planned_step_ids.len(),
            steps
                .iter()
                .map(|step| (step.evaluation().status(), step.provider_contacted())),
        );
        Self {
            scenario_identity,
            planned_step_ids,
            summary,
            steps,
        }
    }

    /// Returns the stable fixture identity.
    #[must_use]
    pub fn fixture_id(&self) -> &str {
        self.scenario_identity.fixture_id()
    }

    /// Returns exact contract, provider, and build identities.
    #[must_use]
    pub const fn identity(&self) -> &FixtureIdentity {
        self.scenario_identity.fixture_identity()
    }

    /// Returns every immutable fixture-controlled semantic input that defined this run.
    #[must_use]
    pub const fn scenario_identity(&self) -> &ScenarioIdentity {
        &self.scenario_identity
    }

    /// Returns planned step identities in fixture execution order.
    #[must_use]
    pub fn planned_step_ids(&self) -> &[String] {
        &self.planned_step_ids
    }

    /// Returns exact execution and conformance counts.
    #[must_use]
    pub const fn summary(&self) -> RunSummary {
        self.summary
    }

    /// Returns payload-isolated observer results in fixture order.
    #[must_use]
    pub fn steps(&self) -> &[ObserverStepResult] {
        &self.steps
    }

    /// Returns whether the complete planned scenario passed.
    #[must_use]
    pub const fn passed(&self) -> bool {
        matches!(self.summary.status, ConformanceStatus::Pass)
    }
}

/// One stable product-versus-observer comparison row without observer payload material.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DifferentialStep {
    /// Stable scenario step identity.
    pub step_id: String,
    /// Product conformance status, or `None` when the product step did not execute.
    pub product_status: Option<ConformanceStatus>,
    /// Observer conformance status, or `None` when the observer step did not execute.
    pub observer_status: Option<ConformanceStatus>,
    /// Fixture-controlled invariant fields failed by the product evaluation.
    pub product_failed_fields: Vec<&'static str>,
    /// Fixture-controlled invariant fields failed by the observer evaluation.
    pub observer_failed_fields: Vec<&'static str>,
    /// Whether the product evaluation contacted provider code.
    pub product_provider_contacted: Option<bool>,
    /// Whether the observer evaluation contacted provider code.
    pub observer_provider_contacted: Option<bool>,
    /// Sanitized product behavior, or `None` when the product step did not execute.
    pub product_observed: Option<ObservedStepSummary>,
    /// Sanitized observer behavior, or `None` when the observer step did not execute.
    pub observer_observed: Option<ObservedStepSummary>,
}

impl DifferentialStep {
    /// Returns whether execution, status, or terminal behavior differs.
    #[must_use]
    pub fn differs(&self) -> bool {
        self.product_status != self.observer_status
            || self.product_failed_fields != self.observer_failed_fields
            || self.product_provider_contacted != self.observer_provider_contacted
            || self.product_observed != self.observer_observed
    }
}

/// Product-versus-observer report retaining the two result domains as distinct Rust types.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DifferentialReport {
    product: ProductRunReport,
    observer: ObserverRunReport,
    steps: Vec<DifferentialStep>,
}

impl DifferentialReport {
    /// Compares reports from the same fixture while excluding operation payloads from observer data.
    pub fn compare(
        product: ProductRunReport,
        observer: ObserverRunReport,
    ) -> Result<Self, EvaluationError> {
        if product.fixture_id() != observer.fixture_id() {
            return Err(EvaluationError::DifferentialFixtureMismatch {
                product_fixture_id: product.fixture_id().to_owned(),
                observer_fixture_id: observer.fixture_id().to_owned(),
            });
        }
        if product.identity() != observer.identity() {
            return Err(EvaluationError::DifferentialIdentityMismatch {
                fixture_id: product.fixture_id().to_owned(),
            });
        }
        if product.planned_step_ids != observer.planned_step_ids {
            return Err(EvaluationError::DifferentialShapeMismatch {
                fixture_id: product.fixture_id().to_owned(),
            });
        }
        if product.scenario_identity != observer.scenario_identity {
            return Err(EvaluationError::DifferentialScenarioMismatch {
                fixture_id: product.fixture_id().to_owned(),
            });
        }
        let product_steps = product
            .steps
            .iter()
            .map(|step| (step.evaluation.step_id().to_owned(), step))
            .collect::<BTreeMap<_, _>>();
        let observer_steps = observer
            .steps
            .iter()
            .map(|step| (step.evaluation.step_id().to_owned(), step))
            .collect::<BTreeMap<_, _>>();
        let mut step_ids = product
            .steps
            .iter()
            .map(|step| step.evaluation.step_id().to_owned())
            .collect::<Vec<_>>();
        step_ids.extend(
            observer
                .steps
                .iter()
                .map(|step| step.evaluation.step_id())
                .filter(|step_id| !product_steps.contains_key(*step_id))
                .map(str::to_owned),
        );
        let steps = step_ids
            .into_iter()
            .map(|step_id| {
                let product_step = product_steps.get(&step_id);
                let observer_step = observer_steps.get(&step_id);
                DifferentialStep {
                    step_id,
                    product_status: product_step.map(|step| step.evaluation.status()),
                    observer_status: observer_step.map(|step| step.evaluation.status()),
                    product_failed_fields: product_step
                        .map(|step| {
                            step.evaluation
                                .violations()
                                .iter()
                                .map(|violation| violation.field)
                                .collect::<BTreeSet<_>>()
                                .into_iter()
                                .collect()
                        })
                        .unwrap_or_default(),
                    observer_failed_fields: observer_step
                        .map(|step| step.evaluation.failed_fields().to_vec())
                        .unwrap_or_default(),
                    product_provider_contacted: product_step.map(|step| step.provider_contacted()),
                    observer_provider_contacted: observer_step
                        .map(|step| step.provider_contacted()),
                    product_observed: product_step.map(|step| {
                        step.output
                            .summary()
                            .with_fixture_controlled_terminal_identity(step.evaluation.violations())
                    }),
                    observer_observed: observer_step.map(|step| step.observed.clone()),
                }
            })
            .collect();
        Ok(Self {
            product,
            observer,
            steps,
        })
    }

    /// Returns the active/product report, including its typed outputs.
    #[must_use]
    pub const fn product(&self) -> &ProductRunReport {
        &self.product
    }

    /// Returns the structurally payload-isolated observer report.
    #[must_use]
    pub const fn observer(&self) -> &ObserverRunReport {
        &self.observer
    }

    /// Returns deterministic comparison rows in fixture execution order.
    #[must_use]
    pub fn steps(&self) -> &[DifferentialStep] {
        &self.steps
    }

    /// Returns whether any step differs in execution, status, or terminal code.
    #[must_use]
    pub fn differs(&self) -> bool {
        self.steps.iter().any(DifferentialStep::differs)
    }
}
