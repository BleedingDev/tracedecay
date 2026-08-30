use std::error::Error;
use std::fmt;

use tracedecay_memory_provider_api::contract::{
    CONTRACT_SET_ID, CONTRACT_SET_SHA256, CommittedEffectState, TerminalCode,
};
use tracedecay_memory_provider_api::{
    HandshakeRequest, OwnedProviderId, OwnedVersionedId, ProviderCall, ProviderOperation,
};

/// Stable identity of the mandatory provider conformance fixture.
pub const MANDATORY_FIXTURE_ID: &str = "tracedecay.memory.conformance.mandatory.v1";
/// SHA-256 of the mandatory fixture identity and revision.
pub const MANDATORY_FIXTURE_BUILD_SHA256: &str =
    "2c605992efb09d23cc7bbfdc8d4ce55459a8090668bec3769b04d1b1cd2142bc";

/// Typed failure produced while validating or running provider conformance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConformanceError {
    /// One identity or digest field is malformed.
    InvalidIdentity(&'static str),
    /// The fixture references a different canonical contract-set identity.
    ContractSetMismatch {
        /// Mismatched field name.
        field: &'static str,
        /// Canonical expected value.
        expected: &'static str,
        /// Supplied value.
        actual: String,
    },
    /// The mandatory fixture identity or build digest is not the pinned value.
    FixtureIdentityMismatch {
        /// Mismatched field name.
        field: &'static str,
        /// Pinned expected value.
        expected: &'static str,
        /// Supplied value.
        actual: String,
    },
    /// A fixture, descriptor, or response names the wrong provider.
    ProviderIdentityMismatch {
        /// Provider identity pinned by the fixture.
        expected: String,
        /// Provider identity actually observed.
        actual: String,
    },
    /// A descriptor or response names the wrong provider implementation build.
    ProviderImplementationMismatch {
        /// Implementation digest pinned by the fixture.
        expected: String,
        /// Implementation digest actually observed.
        actual: String,
    },
    /// A mandatory provider capability is absent.
    MissingMandatoryCapability(&'static str),
    /// The fixture shape violates the mandatory scenario contract.
    InvalidFixture(&'static str),
    /// The provider rejected the mandatory handshake.
    HandshakeRejected(TerminalCode),
    /// The provider returned a malformed successful handshake.
    HandshakeContractViolation(&'static str),
    /// The provider rejected one mandatory operation.
    OperationRejected {
        /// Rejected mandatory operation.
        operation: ProviderOperation,
        /// Typed provider terminal.
        terminal_code: TerminalCode,
    },
    /// The provider returned a malformed successful operation reply.
    OperationContractViolation {
        /// Malformed mandatory operation.
        operation: ProviderOperation,
        /// Stable validation reason.
        reason: &'static str,
    },
}

impl fmt::Display for ConformanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIdentity(field) => write!(formatter, "invalid conformance identity field {field}"),
            Self::ContractSetMismatch {
                field,
                expected,
                actual,
            } => write!(
                formatter,
                "contract-set field {field} expected {expected}, found {actual}"
            ),
            Self::FixtureIdentityMismatch {
                field,
                expected,
                actual,
            } => write!(
                formatter,
                "mandatory fixture field {field} expected {expected}, found {actual}"
            ),
            Self::ProviderIdentityMismatch { expected, actual } => write!(
                formatter,
                "provider identity expected {expected}, found {actual}"
            ),
            Self::ProviderImplementationMismatch { expected, actual } => write!(
                formatter,
                "provider implementation expected {expected}, found {actual}"
            ),
            Self::MissingMandatoryCapability(capability) => {
                write!(formatter, "mandatory capability {capability} is absent")
            }
            Self::InvalidFixture(reason) => write!(formatter, "invalid mandatory fixture: {reason}"),
            Self::HandshakeRejected(code) => {
                write!(formatter, "mandatory handshake returned {}", code.as_wire())
            }
            Self::HandshakeContractViolation(reason) => {
                write!(formatter, "mandatory handshake violated the contract: {reason}")
            }
            Self::OperationRejected {
                operation,
                terminal_code,
            } => write!(
                formatter,
                "mandatory operation {} returned {}",
                operation.capability_id(),
                terminal_code.as_wire()
            ),
            Self::OperationContractViolation { operation, reason } => write!(
                formatter,
                "mandatory operation {} violated the contract: {reason}",
                operation.capability_id()
            ),
        }
    }
}

impl Error for ConformanceError {}

/// Exact contract, fixture, provider, and implementation identities for one run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureIdentity {
    /// Canonical Memory Provider contract-set identity.
    pub contract_set_id: String,
    /// Canonical Memory Provider contract-set source digest.
    pub contract_set_sha256: String,
    /// Versioned conformance fixture identity.
    pub fixture_id: String,
    /// Immutable fixture build digest.
    pub fixture_build_sha256: String,
    /// Stable logical provider identity.
    pub provider_id: String,
    /// Immutable provider implementation digest.
    pub provider_implementation_sha256: String,
}

impl FixtureIdentity {
    /// Validates and owns one exact conformance identity tuple.
    pub fn new(
        contract_set_id: impl Into<String>,
        contract_set_sha256: impl Into<String>,
        fixture_id: impl Into<String>,
        fixture_build_sha256: impl Into<String>,
        provider_id: impl Into<String>,
        provider_implementation_sha256: impl Into<String>,
    ) -> Result<Self, ConformanceError> {
        let identity = Self {
            contract_set_id: contract_set_id.into(),
            contract_set_sha256: contract_set_sha256.into(),
            fixture_id: fixture_id.into(),
            fixture_build_sha256: fixture_build_sha256.into(),
            provider_id: provider_id.into(),
            provider_implementation_sha256: provider_implementation_sha256.into(),
        };
        if identity.contract_set_id != CONTRACT_SET_ID {
            return Err(ConformanceError::ContractSetMismatch {
                field: "contract_set_id",
                expected: CONTRACT_SET_ID,
                actual: identity.contract_set_id,
            });
        }
        if identity.contract_set_sha256 != CONTRACT_SET_SHA256 {
            return Err(ConformanceError::ContractSetMismatch {
                field: "contract_set_sha256",
                expected: CONTRACT_SET_SHA256,
                actual: identity.contract_set_sha256,
            });
        }
        OwnedVersionedId::new(identity.fixture_id.clone())
            .map_err(|_| ConformanceError::InvalidIdentity("fixture_id"))?;
        OwnedProviderId::new(identity.provider_id.clone())
            .map_err(|_| ConformanceError::InvalidIdentity("provider_id"))?;
        require_sha256(&identity.fixture_build_sha256, "fixture_build_sha256")?;
        require_sha256(
            &identity.provider_implementation_sha256,
            "provider_implementation_sha256",
        )?;
        Ok(identity)
    }

    /// Builds the pinned mandatory fixture identity for one provider build.
    pub fn mandatory(
        provider_id: impl Into<String>,
        provider_implementation_sha256: impl Into<String>,
    ) -> Result<Self, ConformanceError> {
        Self::new(
            CONTRACT_SET_ID,
            CONTRACT_SET_SHA256,
            MANDATORY_FIXTURE_ID,
            MANDATORY_FIXTURE_BUILD_SHA256,
            provider_id,
            provider_implementation_sha256,
        )
    }
}

/// One mandatory capability scenario and its expected terminal semantics.
#[derive(Clone, Debug)]
pub struct MandatoryScenario {
    /// Complete canonical provider call.
    pub call: ProviderCall,
    /// Required committed-effect classification on success.
    pub expected_committed_effect: CommittedEffectState,
    /// Whether a successful reply must contain a canonical payload.
    pub payload_required: bool,
}

impl MandatoryScenario {
    /// Creates one health, observation, or recall scenario.
    pub fn new(call: ProviderCall) -> Result<Self, ConformanceError> {
        let (expected_committed_effect, payload_required) = match call.operation {
            ProviderOperation::Health => (CommittedEffectState::None, false),
            ProviderOperation::Observe => (CommittedEffectState::Committed, false),
            ProviderOperation::Recall => (CommittedEffectState::None, true),
            _ => {
                return Err(ConformanceError::InvalidFixture(
                    "scenario operation is not mandatory",
                ));
            }
        };
        Ok(Self {
            call,
            expected_committed_effect,
            payload_required,
        })
    }

    /// Returns the routed mandatory operation.
    #[must_use]
    pub const fn operation(&self) -> ProviderOperation {
        self.call.operation
    }
}

/// Pinned handshake plus deterministic health, observation, and recall calls.
#[derive(Clone, Debug)]
pub struct MandatoryFixture {
    /// Exact contract, fixture, provider, and implementation identities.
    pub identity: FixtureIdentity,
    /// Expected exact-scope digest on every provider terminal.
    pub exact_scope_sha256: String,
    /// Mandatory compatible handshake request.
    pub handshake: HandshakeRequest,
    /// Mandatory scenarios in health, observation, recall order.
    pub scenarios: Vec<MandatoryScenario>,
}

impl MandatoryFixture {
    /// Validates one complete mandatory conformance fixture.
    pub fn new(
        identity: FixtureIdentity,
        exact_scope_sha256: impl Into<String>,
        handshake: HandshakeRequest,
        scenarios: Vec<MandatoryScenario>,
    ) -> Result<Self, ConformanceError> {
        if identity.fixture_id != MANDATORY_FIXTURE_ID {
            return Err(ConformanceError::FixtureIdentityMismatch {
                field: "fixture_id",
                expected: MANDATORY_FIXTURE_ID,
                actual: identity.fixture_id,
            });
        }
        if identity.fixture_build_sha256 != MANDATORY_FIXTURE_BUILD_SHA256 {
            return Err(ConformanceError::FixtureIdentityMismatch {
                field: "fixture_build_sha256",
                expected: MANDATORY_FIXTURE_BUILD_SHA256,
                actual: identity.fixture_build_sha256,
            });
        }
        let exact_scope_sha256 = exact_scope_sha256.into();
        require_sha256(&exact_scope_sha256, "exact_scope_sha256")?;
        ensure_provider(&identity.provider_id, handshake.provider_id.as_str())?;
        for capability in [
            "provider.health.v1",
            "observation.accept.v1",
            "recall.query.v1",
        ] {
            if !handshake
                .required_capabilities
                .iter()
                .any(|required| required.as_str() == capability)
            {
                return Err(ConformanceError::MissingMandatoryCapability(capability));
            }
        }
        let expected_operations = [
            ProviderOperation::Health,
            ProviderOperation::Observe,
            ProviderOperation::Recall,
        ];
        if scenarios.len() != expected_operations.len() {
            return Err(ConformanceError::InvalidFixture(
                "exactly three mandatory scenarios are required",
            ));
        }
        for (scenario, expected_operation) in scenarios.iter().zip(expected_operations) {
            if scenario.operation() != expected_operation {
                return Err(ConformanceError::InvalidFixture(
                    "mandatory scenarios must be ordered health, observation, recall",
                ));
            }
            ensure_provider(&identity.provider_id, scenario.call.provider_id.as_str())?;
            if scenario.call.registration_revision != handshake.registration_revision {
                return Err(ConformanceError::InvalidFixture(
                    "scenario registration revision differs from handshake",
                ));
            }
            if scenario.call.exact_scope != handshake.exact_scope {
                return Err(ConformanceError::InvalidFixture(
                    "scenario exact scope differs from handshake",
                ));
            }
        }
        Ok(Self {
            identity,
            exact_scope_sha256,
            handshake,
            scenarios,
        })
    }
}

fn ensure_provider(expected: &str, actual: &str) -> Result<(), ConformanceError> {
    if expected == actual {
        Ok(())
    } else {
        Err(ConformanceError::ProviderIdentityMismatch {
            expected: expected.to_owned(),
            actual: actual.to_owned(),
        })
    }
}

pub(crate) fn require_sha256(
    value: &str,
    field: &'static str,
) -> Result<(), ConformanceError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(ConformanceError::InvalidIdentity(field))
    }
}
