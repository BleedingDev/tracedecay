#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(warnings)]
#![deny(clippy::dbg_macro)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(clippy::print_stderr)]
#![deny(clippy::print_stdout)]
#![deny(clippy::todo)]
#![deny(clippy::unimplemented)]
#![deny(clippy::unwrap_used)]
//! Provider-neutral conformance and differential evaluation for memory providers.
//!
//! The crate depends only on the canonical provider API. It can exercise any
//! provider without the dashboard or a concrete adapter dependency. Fixtures
//! pin contract, provider, implementation, and build identities. Observer
//! results retain only an opaque product-output digest, never product output
//! bytes or a route capable of replacing active-provider results.

use std::error::Error;
use std::fmt;

use tracedecay_memory_provider_api::contract::TerminalCode;
use tracedecay_memory_provider_api::{
    HandshakeRequest, MemoryProvider, OwnedProviderId, OwnedVersionedId, ProviderCall,
    ProviderDescriptor, ProviderOperation,
};

/// Fixture or scenario validation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConformanceError {
    /// A required string was empty.
    EmptyField(&'static str),
    /// A required digest was not lowercase SHA-256.
    InvalidSha256(&'static str),
    /// A request target did not match the fixture provider.
    ProviderIdentityMismatch {
        /// Validation context.
        context: &'static str,
        /// Required provider identity.
        expected: String,
        /// Actual provider identity.
        actual: String,
    },
    /// A mandatory fixture slot contained the wrong operation.
    OperationMismatch {
        /// Fixture slot.
        slot: &'static str,
        /// Required operation.
        expected: ProviderOperation,
        /// Actual operation.
        actual: ProviderOperation,
    },
    /// A provider scenario contained no calls.
    EmptyScenario,
}

impl fmt::Display for ConformanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyField(field) => write!(formatter, "required field {field} is empty"),
            Self::InvalidSha256(field) => {
                write!(formatter, "field {field} is not lowercase SHA-256 hex")
            }
            Self::ProviderIdentityMismatch {
                context,
                expected,
                actual,
            } => write!(
                formatter,
                "{context} targets provider {actual}, expected {expected}"
            ),
            Self::OperationMismatch {
                slot,
                expected,
                actual,
            } => write!(
                formatter,
                "fixture slot {slot} contains {actual:?}, expected {expected:?}"
            ),
            Self::EmptyScenario => formatter.write_str("provider scenario contains no calls"),
        }
    }
}

impl Error for ConformanceError {}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn require_sha256(value: &str, field: &'static str) -> Result<(), ConformanceError> {
    if is_lowercase_sha256(value) {
        Ok(())
    } else {
        Err(ConformanceError::InvalidSha256(field))
    }
}

fn require_non_empty(value: &str, field: &'static str) -> Result<(), ConformanceError> {
    if value.is_empty() {
        Err(ConformanceError::EmptyField(field))
    } else {
        Ok(())
    }
}

fn require_provider(
    context: &'static str,
    expected: &OwnedProviderId,
    actual: &OwnedProviderId,
) -> Result<(), ConformanceError> {
    if expected == actual {
        Ok(())
    } else {
        Err(ConformanceError::ProviderIdentityMismatch {
            context,
            expected: expected.as_str().to_owned(),
            actual: actual.as_str().to_owned(),
        })
    }
}

fn require_operation(
    slot: &'static str,
    expected: ProviderOperation,
    actual: ProviderOperation,
) -> Result<(), ConformanceError> {
    if expected == actual {
        Ok(())
    } else {
        Err(ConformanceError::OperationMismatch {
            slot,
            expected,
            actual,
        })
    }
}

/// Immutable identities pinned by a conformance fixture.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureIdentity {
    /// Versioned canonical contract-set identity.
    pub contract_id: OwnedVersionedId,
    /// Digest of the exact canonical contract-set bytes used by the fixture.
    pub contract_set_sha256: String,
    /// Stable logical provider identity.
    pub provider_id: OwnedProviderId,
    /// Exact provider build or artifact identity.
    pub provider_build_id: String,
    /// Digest of the immutable provider implementation identity.
    pub implementation_identity_sha256: String,
}

impl FixtureIdentity {
    /// Creates a fully pinned fixture identity.
    pub fn new(
        contract_id: OwnedVersionedId,
        contract_set_sha256: impl Into<String>,
        provider_id: OwnedProviderId,
        provider_build_id: impl Into<String>,
        implementation_identity_sha256: impl Into<String>,
    ) -> Result<Self, ConformanceError> {
        let contract_set_sha256 = contract_set_sha256.into();
        let provider_build_id = provider_build_id.into();
        let implementation_identity_sha256 = implementation_identity_sha256.into();
        require_sha256(&contract_set_sha256, "contract_set_sha256")?;
        require_non_empty(&provider_build_id, "provider_build_id")?;
        require_sha256(
            &implementation_identity_sha256,
            "implementation_identity_sha256",
        )?;
        Ok(Self {
            contract_id,
            contract_set_sha256,
            provider_id,
            provider_build_id,
            implementation_identity_sha256,
        })
    }
}

/// One mandatory provider call and its exact terminal expectation.
#[derive(Clone, Debug)]
pub struct ExpectedCall {
    /// Complete canonical provider call.
    pub call: ProviderCall,
    /// Required terminal code.
    pub terminal_code: TerminalCode,
    /// Required exact-scope digest in the terminal record.
    pub exact_scope_sha256: String,
}

impl ExpectedCall {
    /// Creates a validated expected call.
    pub fn new(
        call: ProviderCall,
        terminal_code: TerminalCode,
        exact_scope_sha256: impl Into<String>,
    ) -> Result<Self, ConformanceError> {
        let exact_scope_sha256 = exact_scope_sha256.into();
        require_sha256(&exact_scope_sha256, "expected_exact_scope_sha256")?;
        Ok(Self {
            call,
            terminal_code,
            exact_scope_sha256,
        })
    }
}

/// Canonical mandatory provider-conformance fixture.
#[derive(Clone, Debug)]
pub struct MandatoryConformanceFixture {
    /// Exact contract, provider, implementation, and build identities.
    pub identity: FixtureIdentity,
    /// Complete read-only provider handshake request.
    pub handshake: HandshakeRequest,
    /// Required handshake terminal code.
    pub handshake_terminal_code: TerminalCode,
    /// Required handshake exact-scope digest.
    pub handshake_exact_scope_sha256: String,
    /// Mandatory health call.
    pub health: ExpectedCall,
    /// Mandatory observation-acceptance call.
    pub observation: ExpectedCall,
    /// Mandatory recall call.
    pub recall: ExpectedCall,
}

impl MandatoryConformanceFixture {
    /// Creates a fixture only when all targets and mandatory operation slots are exact.
    pub fn new(
        identity: FixtureIdentity,
        handshake: HandshakeRequest,
        handshake_terminal_code: TerminalCode,
        handshake_exact_scope_sha256: impl Into<String>,
        health: ExpectedCall,
        observation: ExpectedCall,
        recall: ExpectedCall,
    ) -> Result<Self, ConformanceError> {
        let handshake_exact_scope_sha256 = handshake_exact_scope_sha256.into();
        require_sha256(
            &handshake_exact_scope_sha256,
            "handshake_exact_scope_sha256",
        )?;
        require_provider("handshake", &identity.provider_id, &handshake.provider_id)?;
        require_provider("health", &identity.provider_id, &health.call.provider_id)?;
        require_provider(
            "observation",
            &identity.provider_id,
            &observation.call.provider_id,
        )?;
        require_provider("recall", &identity.provider_id, &recall.call.provider_id)?;
        require_operation("health", ProviderOperation::Health, health.call.operation)?;
        require_operation(
            "observation",
            ProviderOperation::Observe,
            observation.call.operation,
        )?;
        require_operation("recall", ProviderOperation::Recall, recall.call.operation)?;
        Ok(Self {
            identity,
            handshake,
            handshake_terminal_code,
            handshake_exact_scope_sha256,
            health,
            observation,
            recall,
        })
    }
}

/// Stable case identity in the mandatory suite.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ConformanceCase {
    /// Runtime descriptor matches pinned provider and implementation identity.
    DescriptorIdentity,
    /// Runtime descriptor declares all mandatory capabilities.
    MandatoryCapabilities,
    /// Compatible read-only handshake.
    Handshake,
    /// Mandatory provider health.
    Health,
    /// Mandatory observation acceptance.
    Observation,
    /// Mandatory recall.
    Recall,
}

/// Outcome of one mandatory conformance case.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaseOutcome {
    /// Stable case identity.
    pub case: ConformanceCase,
    /// Expected terminal when the case invokes a provider operation.
    pub expected_terminal: Option<TerminalCode>,
    /// Observed terminal when the case invokes a provider operation.
    pub observed_terminal: Option<TerminalCode>,
    /// Whether every case invariant held.
    pub passed: bool,
    /// Bounded mismatch explanation when the case failed.
    pub diagnostic: Option<String>,
}

struct TerminalShape<'a> {
    code: TerminalCode,
    operation_id: &'a str,
    exact_scope_sha256: &'a str,
}

impl CaseOutcome {
    fn predicate(case: ConformanceCase, passed: bool, failure: String) -> Self {
        Self {
            case,
            expected_terminal: None,
            observed_terminal: None,
            passed,
            diagnostic: if passed { None } else { Some(failure) },
        }
    }

    fn terminal(
        case: ConformanceCase,
        expected: TerminalShape<'_>,
        observed: TerminalShape<'_>,
    ) -> Self {
        let passed = expected.code == observed.code
            && expected.operation_id == observed.operation_id
            && expected.exact_scope_sha256 == observed.exact_scope_sha256;
        let diagnostic = if passed {
            None
        } else {
            Some(format!(
                "expected {:?}/{}/{}, observed {:?}/{}/{}",
                expected.code,
                expected.operation_id,
                expected.exact_scope_sha256,
                observed.code,
                observed.operation_id,
                observed.exact_scope_sha256
            ))
        };
        Self {
            case,
            expected_terminal: Some(expected.code),
            observed_terminal: Some(observed.code),
            passed,
            diagnostic,
        }
    }
}

/// Runtime identity observed directly from a provider descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeIdentity {
    /// Stable logical provider identity.
    pub provider_id: OwnedProviderId,
    /// Immutable implementation identity digest.
    pub implementation_identity_sha256: String,
    /// Provider-local state schema identity.
    pub state_schema_version: String,
    /// Provider-local state generation.
    pub state_generation: u64,
}

impl From<&ProviderDescriptor> for RuntimeIdentity {
    fn from(descriptor: &ProviderDescriptor) -> Self {
        Self {
            provider_id: descriptor.provider_id.clone(),
            implementation_identity_sha256: descriptor.implementation_identity_sha256.clone(),
            state_schema_version: descriptor.state_schema_version.clone(),
            state_generation: descriptor.state_generation,
        }
    }
}

/// Complete mandatory conformance report for one pinned fixture.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConformanceReport {
    /// Exact fixture identities.
    pub fixture_identity: FixtureIdentity,
    /// Runtime identity observed from the provider.
    pub runtime_identity: RuntimeIdentity,
    /// Deterministically ordered mandatory case outcomes.
    pub cases: Vec<CaseOutcome>,
}

impl ConformanceReport {
    /// Returns true only when every mandatory case passed.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.cases.iter().all(|case| case.passed)
    }

    /// Returns one case outcome by stable identity.
    #[must_use]
    pub fn case(&self, wanted: ConformanceCase) -> Option<&CaseOutcome> {
        self.cases.iter().find(|case| case.case == wanted)
    }
}

/// Provider-neutral mandatory conformance harness.
pub struct ConformanceHarness;

impl ConformanceHarness {
    /// Runs descriptor, capability, handshake, health, observation, and recall checks.
    #[must_use]
    pub fn run(
        provider: &dyn MemoryProvider,
        fixture: &MandatoryConformanceFixture,
    ) -> ConformanceReport {
        let descriptor = provider.descriptor();
        let runtime_identity = RuntimeIdentity::from(&descriptor);
        let descriptor_matches = descriptor.provider_id.as_str()
            == fixture.identity.provider_id.as_str()
            && descriptor.implementation_identity_sha256
                == fixture.identity.implementation_identity_sha256;
        let descriptor_failure = format!(
            "expected provider {}/implementation {}, observed {}/{}",
            fixture.identity.provider_id.as_str(),
            fixture.identity.implementation_identity_sha256,
            descriptor.provider_id.as_str(),
            descriptor.implementation_identity_sha256
        );
        let mut cases = vec![CaseOutcome::predicate(
            ConformanceCase::DescriptorIdentity,
            descriptor_matches,
            descriptor_failure,
        )];

        let missing = [
            "provider.health.v1",
            "observation.accept.v1",
            "recall.query.v1",
        ]
        .into_iter()
        .filter(|capability| !descriptor.supports(capability))
        .collect::<Vec<_>>();
        cases.push(CaseOutcome::predicate(
            ConformanceCase::MandatoryCapabilities,
            missing.is_empty(),
            format!("missing mandatory capabilities: {}", missing.join(", ")),
        ));

        let handshake = provider.handshake(&fixture.handshake);
        cases.push(CaseOutcome::terminal(
            ConformanceCase::Handshake,
            TerminalShape {
                code: fixture.handshake_terminal_code,
                operation_id: &fixture.handshake.request_id,
                exact_scope_sha256: &fixture.handshake_exact_scope_sha256,
            },
            TerminalShape {
                code: handshake.terminal.terminal_code,
                operation_id: &handshake.terminal.operation_id,
                exact_scope_sha256: &handshake.terminal.exact_scope_sha256,
            },
        ));
        cases.push(run_expected_call(
            provider,
            ConformanceCase::Health,
            &fixture.health,
        ));
        cases.push(run_expected_call(
            provider,
            ConformanceCase::Observation,
            &fixture.observation,
        ));
        cases.push(run_expected_call(
            provider,
            ConformanceCase::Recall,
            &fixture.recall,
        ));

        ConformanceReport {
            fixture_identity: fixture.identity.clone(),
            runtime_identity,
            cases,
        }
    }
}

fn run_expected_call(
    provider: &dyn MemoryProvider,
    case: ConformanceCase,
    expected: &ExpectedCall,
) -> CaseOutcome {
    let reply = provider.invoke(&expected.call);
    CaseOutcome::terminal(
        case,
        TerminalShape {
            code: expected.terminal_code,
            operation_id: &expected.call.operation_id,
            exact_scope_sha256: &expected.exact_scope_sha256,
        },
        TerminalShape {
            code: reply.terminal.terminal_code,
            operation_id: &reply.terminal.operation_id,
            exact_scope_sha256: &reply.terminal.exact_scope_sha256,
        },
    )
}

/// Neutral ordered provider scenario for differential evaluation.
#[derive(Clone, Debug)]
pub struct ProviderScenario {
    /// Stable scenario identity.
    pub scenario_id: String,
    /// Exact fixture identities for the scenario.
    pub identity: FixtureIdentity,
    /// Ordered complete provider calls.
    pub calls: Vec<ProviderCall>,
}

impl ProviderScenario {
    /// Creates a non-empty scenario whose calls all target the pinned provider.
    pub fn new(
        scenario_id: impl Into<String>,
        identity: FixtureIdentity,
        calls: Vec<ProviderCall>,
    ) -> Result<Self, ConformanceError> {
        let scenario_id = scenario_id.into();
        require_non_empty(&scenario_id, "scenario_id")?;
        if calls.is_empty() {
            return Err(ConformanceError::EmptyScenario);
        }
        for call in &calls {
            require_provider("scenario call", &identity.provider_id, &call.provider_id)?;
        }
        Ok(Self {
            scenario_id,
            identity,
            calls,
        })
    }
}

/// One provider-neutral scenario step result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScenarioStepResult {
    /// Requested operation.
    pub operation: ProviderOperation,
    /// Requested stable operation identity.
    pub operation_id: String,
    /// Observed terminal code.
    pub terminal_code: TerminalCode,
    /// Operation identity echoed by the provider terminal.
    pub terminal_operation_id: String,
    /// Exact-scope digest reported by the provider terminal.
    pub exact_scope_sha256: String,
    /// Successful canonical payload digest, without payload bytes.
    pub payload_sha256: Option<String>,
}

/// Provider-neutral scenario report.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScenarioReport {
    /// Stable scenario identity.
    pub scenario_id: String,
    /// Exact fixture identities.
    pub fixture_identity: FixtureIdentity,
    /// Runtime identity observed from the provider.
    pub runtime_identity: RuntimeIdentity,
    /// Deterministically ordered step results.
    pub steps: Vec<ScenarioStepResult>,
}

/// Provider-neutral scenario runner.
pub struct ScenarioRunner;

impl ScenarioRunner {
    /// Runs one ordered scenario and retains only comparable terminal metadata and digests.
    #[must_use]
    pub fn run(provider: &dyn MemoryProvider, scenario: &ProviderScenario) -> ScenarioReport {
        let descriptor = provider.descriptor();
        let runtime_identity = RuntimeIdentity::from(&descriptor);
        let steps = scenario
            .calls
            .iter()
            .map(|call| {
                let reply = provider.invoke(call);
                let payload_sha256 = reply.payload.as_ref().map(|payload| payload.sha256.clone());
                ScenarioStepResult {
                    operation: call.operation,
                    operation_id: call.operation_id.clone(),
                    terminal_code: reply.terminal.terminal_code,
                    terminal_operation_id: reply.terminal.operation_id,
                    exact_scope_sha256: reply.terminal.exact_scope_sha256,
                    payload_sha256,
                }
            })
            .collect();
        ScenarioReport {
            scenario_id: scenario.scenario_id.clone(),
            fixture_identity: scenario.identity.clone(),
            runtime_identity,
            steps,
        }
    }
}

/// One index-aligned differential comparison case.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DifferentialCase {
    /// Zero-based scenario-step index.
    pub index: usize,
    /// Left provider operation when present.
    pub left_operation: Option<ProviderOperation>,
    /// Right provider operation when present.
    pub right_operation: Option<ProviderOperation>,
    /// Left terminal when present.
    pub left_terminal: Option<TerminalCode>,
    /// Right terminal when present.
    pub right_terminal: Option<TerminalCode>,
    /// Whether operation and terminal identities match.
    pub same_terminal: bool,
    /// Whether successful payload digests match.
    pub same_payload: bool,
}

/// Differential report that compares provider-visible outcomes, never internal state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DifferentialReport {
    /// Left fixture identity.
    pub left_identity: FixtureIdentity,
    /// Right fixture identity.
    pub right_identity: FixtureIdentity,
    /// Index-aligned outcome comparisons.
    pub cases: Vec<DifferentialCase>,
}

impl DifferentialReport {
    /// Compares two scenario reports without requiring provider-state equivalence.
    #[must_use]
    pub fn compare(left: &ScenarioReport, right: &ScenarioReport) -> Self {
        let count = left.steps.len().max(right.steps.len());
        let cases = (0..count)
            .map(|index| {
                let left_step = left.steps.get(index);
                let right_step = right.steps.get(index);
                let same_terminal = match (left_step, right_step) {
                    (Some(left_value), Some(right_value)) => {
                        left_value.operation == right_value.operation
                            && left_value.terminal_code == right_value.terminal_code
                    }
                    _ => false,
                };
                let same_payload = left_step.and_then(|step| step.payload_sha256.as_deref())
                    == right_step.and_then(|step| step.payload_sha256.as_deref());
                DifferentialCase {
                    index,
                    left_operation: left_step.map(|step| step.operation),
                    right_operation: right_step.map(|step| step.operation),
                    left_terminal: left_step.map(|step| step.terminal_code),
                    right_terminal: right_step.map(|step| step.terminal_code),
                    same_terminal,
                    same_payload,
                }
            })
            .collect();
        Self {
            left_identity: left.fixture_identity.clone(),
            right_identity: right.fixture_identity.clone(),
            cases,
        }
    }

    /// Returns true only when every index-aligned terminal and payload digest matches.
    #[must_use]
    pub fn equivalent(&self) -> bool {
        self.cases
            .iter()
            .all(|case| case.same_terminal && case.same_payload)
    }
}

/// Opaque digest of an externally produced product output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProductOutputDigest(String);

impl ProductOutputDigest {
    /// Creates a validated product-output digest without retaining output bytes.
    pub fn new(value: impl Into<String>) -> Result<Self, ConformanceError> {
        let value = value.into();
        require_sha256(&value, "product_output_sha256")?;
        Ok(Self(value))
    }

    /// Returns the lowercase SHA-256 digest.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Observer-only conformance result structurally separated from product output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObserverConformanceResult {
    product_output_digest: ProductOutputDigest,
    conformance: ConformanceReport,
}

impl ObserverConformanceResult {
    /// Binds an observer report to an immutable baseline-output digest.
    #[must_use]
    pub const fn new(
        product_output_digest: ProductOutputDigest,
        conformance: ConformanceReport,
    ) -> Self {
        Self {
            product_output_digest,
            conformance,
        }
    }

    /// Returns the baseline product-output digest; no output bytes are retained.
    #[must_use]
    pub const fn product_output_digest(&self) -> &ProductOutputDigest {
        &self.product_output_digest
    }

    /// Returns the isolated observer conformance report.
    #[must_use]
    pub const fn conformance(&self) -> &ConformanceReport {
        &self.conformance
    }
}
