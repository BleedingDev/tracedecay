use std::error::Error;
use std::fmt;

use tracedecay_memory_provider_api::ApiError;

/// Stable fixture-construction or execution-precondition failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EvaluationError {
    /// A required evaluation field was empty.
    EmptyField(&'static str),
    /// A scenario declared the same step identity more than once.
    DuplicateStepId(String),
    /// A scenario declared the same provider operation identity more than once.
    DuplicateOperationId(String),
    /// A scenario declared the same request identity more than once.
    DuplicateRequestId(String),
    /// An operation fixture omitted its capability from its required set.
    MissingFixtureOperationCapability {
        /// Stable step identity containing the malformed requirement.
        step_id: String,
        /// Canonical capability required by the operation.
        capability_id: &'static str,
    },
    /// An operation required a capability that the handshake did not negotiate.
    OperationCapabilityNotNegotiated {
        /// Stable step identity containing the extra requirement.
        step_id: String,
        /// Capability absent from handshake requirements.
        capability_id: String,
    },
    /// A mutating operation fixture omitted a nonempty idempotency key.
    MissingFixtureIdempotencyKey {
        /// Stable step identity containing the malformed mutation.
        step_id: String,
    },
    /// A terminal expectation did not contain any accepted value.
    EmptyTerminalExpectation,
    /// Operations were declared without requiring a handshake ready receipt.
    OperationsRequireReadyReceipt,
    /// Operations were declared without requiring the provider descriptor.
    OperationsRequireHandshakeDescriptor,
    /// Operations were declared without requiring the exact accepted scope.
    OperationsRequireAcceptedScope,
    /// Operations were declared after a handshake expected not to succeed.
    OperationsRequireSuccessfulHandshake,
    /// Live request control was declared with an already-expired zero budget.
    ZeroLiveRequestBudget,
    /// A fixture targeted a different contract set than this build exposes.
    ContractIdentityMismatch {
        /// Contract-set identity required by this evaluator build.
        expected_id: &'static str,
        /// Contract-set digest required by this evaluator build.
        expected_sha256: &'static str,
        /// Contract-set identity carried by the fixture.
        actual_id: String,
        /// Contract-set digest carried by the fixture.
        actual_sha256: String,
    },
    /// A fixture provider or build identity did not match the harness target.
    ProviderIdentityMismatch {
        /// Identity component that differed.
        field: &'static str,
        /// Exact identity expected by the fixture.
        expected: String,
        /// Exact identity exposed by the harness.
        actual: String,
    },
    /// A provider descriptor exposed a malformed immutable build digest.
    InvalidProviderBuildIdentitySha256(String),
    /// Product and observer reports came from different fixtures.
    DifferentialFixtureMismatch {
        /// Product-run fixture identity.
        product_fixture_id: String,
        /// Observer-run fixture identity.
        observer_fixture_id: String,
    },
    /// Product and observer reports declared different planned step shapes.
    DifferentialShapeMismatch {
        /// Shared textual fixture identity whose shapes differed.
        fixture_id: String,
    },
    /// Product and observer reports carried different exact fixture identities.
    DifferentialIdentityMismatch {
        /// Shared textual fixture identity whose exact identities differed.
        fixture_id: String,
    },
    /// Product and observer reports carried different semantic scenario inputs.
    DifferentialScenarioMismatch {
        /// Shared textual fixture identity whose semantic inputs differed.
        fixture_id: String,
    },
    /// The provider API rejected a materialized request or call.
    ProviderApi(ApiError),
}

impl fmt::Display for EvaluationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyField(field) => {
                write!(formatter, "required evaluation field {field} is empty")
            }
            Self::DuplicateStepId(step_id) => {
                write!(formatter, "scenario step id {step_id} is duplicated")
            }
            Self::DuplicateOperationId(operation_id) => {
                write!(
                    formatter,
                    "provider operation id {operation_id} is duplicated"
                )
            }
            Self::DuplicateRequestId(request_id) => {
                write!(formatter, "provider request id {request_id} is duplicated")
            }
            Self::MissingFixtureOperationCapability {
                step_id,
                capability_id,
            } => write!(
                formatter,
                "scenario step {step_id} does not require its operation capability {capability_id}"
            ),
            Self::OperationCapabilityNotNegotiated {
                step_id,
                capability_id,
            } => write!(
                formatter,
                "scenario step {step_id} requires capability {capability_id} absent from the handshake"
            ),
            Self::MissingFixtureIdempotencyKey { step_id } => write!(
                formatter,
                "mutating scenario step {step_id} requires a nonempty idempotency key"
            ),
            Self::EmptyTerminalExpectation => {
                formatter.write_str("terminal expectation has no accepted value")
            }
            Self::OperationsRequireReadyReceipt => formatter
                .write_str("a scenario with operations must require a handshake ready receipt"),
            Self::OperationsRequireHandshakeDescriptor => formatter
                .write_str("a scenario with operations must require a handshake descriptor"),
            Self::OperationsRequireAcceptedScope => formatter
                .write_str("a scenario with operations must require the exact accepted scope"),
            Self::OperationsRequireSuccessfulHandshake => {
                formatter.write_str("a scenario with operations must expect a successful handshake")
            }
            Self::ZeroLiveRequestBudget => {
                formatter.write_str("live request control must have a nonzero remaining budget")
            }
            Self::ContractIdentityMismatch {
                expected_id,
                expected_sha256,
                actual_id,
                actual_sha256,
            } => write!(
                formatter,
                "fixture contract identity {actual_id}@{actual_sha256} does not match {expected_id}@{expected_sha256}"
            ),
            Self::ProviderIdentityMismatch {
                field,
                expected,
                actual,
            } => write!(
                formatter,
                "fixture {field} identity {expected} does not match harness identity {actual}"
            ),
            Self::InvalidProviderBuildIdentitySha256(actual) => write!(
                formatter,
                "provider build identity {actual} is not lowercase SHA-256"
            ),
            Self::DifferentialFixtureMismatch {
                product_fixture_id,
                observer_fixture_id,
            } => write!(
                formatter,
                "product fixture {product_fixture_id} does not match observer fixture {observer_fixture_id}"
            ),
            Self::DifferentialShapeMismatch { fixture_id } => write!(
                formatter,
                "product and observer reports for fixture {fixture_id} have different planned steps"
            ),
            Self::DifferentialIdentityMismatch { fixture_id } => write!(
                formatter,
                "product and observer reports for fixture {fixture_id} have different exact identities"
            ),
            Self::DifferentialScenarioMismatch { fixture_id } => write!(
                formatter,
                "product and observer reports for fixture {fixture_id} have different semantic inputs"
            ),
            Self::ProviderApi(error) => write!(formatter, "provider API rejected fixture: {error}"),
        }
    }
}

impl Error for EvaluationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ProviderApi(error) => Some(error),
            _ => None,
        }
    }
}

impl From<ApiError> for EvaluationError {
    fn from(error: ApiError) -> Self {
        Self::ProviderApi(error)
    }
}
