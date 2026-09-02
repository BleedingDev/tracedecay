use std::collections::BTreeSet;

use tracedecay_memory_provider_api::contract::{
    CONTRACT_SET_ID, CONTRACT_SET_SHA256, CommittedEffectState, TerminalCode,
};
use tracedecay_memory_provider_api::{
    ApiError, CancellationToken, CanonicalPayload, CommittedEffectEvidence,
    CommittedEffectEvidenceParts, FallbackDirective, MAX_COMMITTED_EFFECT_ITEM_REFS,
    OperationControl, OwnedExactScope, OwnedOpaqueExtension, OwnedProviderId, OwnedVersionedId,
    ProviderDescriptor, ProviderLimits, ProviderOperation, TerminalRecord,
};

use crate::EvaluationError;

const EMPTY_OBJECT_SHA256: &str =
    "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a";
const LIVE_DEADLINE_UTC_MICROS: i64 = 4_102_444_800_000_000; // 2100-01-01T00:00:00Z.

/// Exact canonical contract-set identity carried by every fixture and report.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContractIdentity {
    contract_set_id: String,
    contract_set_sha256: String,
}

impl ContractIdentity {
    /// Creates an explicitly versioned contract identity, including identities loaded from fixtures.
    pub fn new(
        contract_set_id: impl Into<String>,
        contract_set_sha256: impl Into<String>,
    ) -> Result<Self, EvaluationError> {
        let identity = Self {
            contract_set_id: contract_set_id.into(),
            contract_set_sha256: contract_set_sha256.into(),
        };
        require_non_empty(&identity.contract_set_id, "contract_set_id")?;
        require_non_empty(&identity.contract_set_sha256, "contract_set_sha256")?;
        Ok(identity)
    }

    /// Returns the exact contract identity compiled into the provider API.
    #[must_use]
    pub fn current() -> Self {
        Self {
            contract_set_id: CONTRACT_SET_ID.to_owned(),
            contract_set_sha256: CONTRACT_SET_SHA256.to_owned(),
        }
    }

    /// Returns the stable contract-set identifier.
    #[must_use]
    pub fn contract_set_id(&self) -> &str {
        &self.contract_set_id
    }

    /// Returns the canonical contract-set digest.
    #[must_use]
    pub fn contract_set_sha256(&self) -> &str {
        &self.contract_set_sha256
    }

    pub(crate) fn validate_current(&self) -> Result<(), EvaluationError> {
        if self.contract_set_id == CONTRACT_SET_ID
            && self.contract_set_sha256 == CONTRACT_SET_SHA256
        {
            Ok(())
        } else {
            Err(EvaluationError::ContractIdentityMismatch {
                expected_id: CONTRACT_SET_ID,
                expected_sha256: CONTRACT_SET_SHA256,
                actual_id: self.contract_set_id.clone(),
                actual_sha256: self.contract_set_sha256.clone(),
            })
        }
    }
}

/// Exact logical-provider, immutable-implementation, and build identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderBuildIdentity {
    provider_id: OwnedProviderId,
    build_identity_sha256: String,
}

impl ProviderBuildIdentity {
    /// Captures the logical provider and immutable build identity exposed by its descriptor.
    pub fn from_descriptor(descriptor: &ProviderDescriptor) -> Result<Self, EvaluationError> {
        if !is_lowercase_sha256(&descriptor.implementation_identity_sha256) {
            return Err(EvaluationError::InvalidProviderBuildIdentitySha256(
                descriptor.implementation_identity_sha256.clone(),
            ));
        }
        descriptor.validate()?;
        Ok(Self {
            provider_id: descriptor.provider_id.clone(),
            build_identity_sha256: descriptor.implementation_identity_sha256.clone(),
        })
    }

    /// Returns the stable logical provider ID.
    #[must_use]
    pub fn provider_id(&self) -> &OwnedProviderId {
        &self.provider_id
    }

    /// Returns the immutable provider build/implementation digest.
    #[must_use]
    pub fn build_identity_sha256(&self) -> &str {
        &self.build_identity_sha256
    }

    pub(crate) fn require_match(&self, actual: &Self) -> Result<(), EvaluationError> {
        require_identity_match(
            "provider_id",
            self.provider_id.as_str(),
            actual.provider_id.as_str(),
        )?;
        require_identity_match(
            "provider_build_identity_sha256",
            &self.build_identity_sha256,
            &actual.build_identity_sha256,
        )
    }
}

/// Complete identity pinned by a fixture and copied into every resulting report.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureIdentity {
    contract: ContractIdentity,
    provider: ProviderBuildIdentity,
}

impl FixtureIdentity {
    /// Combines exact contract and provider-build identities.
    #[must_use]
    pub const fn new(contract: ContractIdentity, provider: ProviderBuildIdentity) -> Self {
        Self { contract, provider }
    }

    /// Returns the exact contract identity.
    #[must_use]
    pub const fn contract(&self) -> &ContractIdentity {
        &self.contract
    }

    /// Returns the exact provider and build identity.
    #[must_use]
    pub const fn provider(&self) -> &ProviderBuildIdentity {
        &self.provider
    }
}

/// Deadline and cancellation behavior materialized independently for one step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestControlFixture {
    /// Absolute UTC deadline recorded in the request envelope.
    pub deadline_utc_micros: i64,
    /// Finite monotonic budget remaining at dispatch.
    pub remaining_millis: u64,
    /// Whether cancellation is requested before the provider receives the call.
    pub cancel_before_dispatch: bool,
}

impl RequestControlFixture {
    /// Creates live request control with a finite, nonzero remaining budget.
    pub const fn live(
        deadline_utc_micros: i64,
        remaining_millis: u64,
    ) -> Result<Self, EvaluationError> {
        if remaining_millis == 0 {
            return Err(EvaluationError::ZeroLiveRequestBudget);
        }
        Ok(Self {
            deadline_utc_micros,
            remaining_millis,
            cancel_before_dispatch: false,
        })
    }

    /// Creates already-cancelled request control; cancellation wins when the budget is also zero.
    #[must_use]
    pub const fn cancelled(deadline_utc_micros: i64, remaining_millis: u64) -> Self {
        Self {
            deadline_utc_micros,
            remaining_millis,
            cancel_before_dispatch: true,
        }
    }

    /// Creates expired request control with no remaining budget.
    #[must_use]
    pub const fn expired(deadline_utc_micros: i64) -> Self {
        Self {
            deadline_utc_micros,
            remaining_millis: 0,
            cancel_before_dispatch: false,
        }
    }

    pub(crate) fn materialize(self) -> OperationControl {
        let cancellation = CancellationToken::new();
        if self.cancel_before_dispatch {
            cancellation.cancel();
        }
        OperationControl::new(
            self.deadline_utc_micros,
            self.remaining_millis,
            cancellation,
        )
    }
}

/// Expected behavior of the fixture's provider handshake.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HandshakeExpectation {
    /// Exact terminal code required from the handshake.
    pub terminal_code: TerminalCode,
    /// Complete structured committed-effect expectation.
    pub committed_effect: ExpectedCommittedEffect,
    /// Exact fallback decision, including any policy pin and reason.
    pub fallback: FallbackDirective,
    /// Whether a successful response must contain a descriptor.
    pub require_descriptor: bool,
    /// Whether a successful response must echo the exact accepted scope.
    pub require_accepted_scope: bool,
    /// Whether subsequent operations require a real ready-receipt digest.
    pub require_ready_receipt: bool,
}

/// Provider-neutral handshake template.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HandshakeFixture {
    /// Stable scenario step identity.
    pub step_id: String,
    /// Stable request identity.
    pub request_id: String,
    /// Mandatory capabilities required by the scenario.
    pub required_capabilities: Vec<OwnedVersionedId>,
    /// Host ceilings offered to every provider.
    pub host_limits: ProviderLimits,
    /// Deadline and cancellation behavior.
    pub control: RequestControlFixture,
    /// Deterministic challenge nonce.
    pub challenge_nonce: [u8; 32],
    /// Expected handshake behavior.
    pub expectation: HandshakeExpectation,
}

/// Nonempty set of accepted terminal outcomes for one provider operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TerminalExpectation(BTreeSet<TerminalCode>);

impl TerminalExpectation {
    /// Accepts exactly one terminal outcome.
    #[must_use]
    pub fn exactly(terminal_code: TerminalCode) -> Self {
        Self(BTreeSet::from([terminal_code]))
    }

    /// Accepts any terminal outcome in a nonempty set.
    pub fn one_of(
        terminal_codes: impl IntoIterator<Item = TerminalCode>,
    ) -> Result<Self, EvaluationError> {
        let terminal_codes = terminal_codes.into_iter().collect::<BTreeSet<_>>();
        if terminal_codes.is_empty() {
            Err(EvaluationError::EmptyTerminalExpectation)
        } else {
            Ok(Self(terminal_codes))
        }
    }

    /// Returns whether one terminal code satisfies the expectation.
    #[must_use]
    pub fn accepts(&self, terminal_code: TerminalCode) -> bool {
        self.0.contains(&terminal_code)
    }

    /// Iterates accepted terminal codes in deterministic wire order.
    pub fn iter(&self) -> impl Iterator<Item = TerminalCode> + '_ {
        self.0.iter().copied()
    }
}

/// Expected relationship between provider state generation before and after a step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GenerationExpectation {
    /// Do not constrain provider-local state generation.
    Any,
    /// Require the provider-local generation to remain unchanged.
    Unchanged,
    /// Require an exact positive or zero increment.
    IncreasedBy(u64),
}

/// Expected provider payload shape without interpreting provider-specific bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PayloadExpectation {
    /// Accept either payload presence or absence.
    Any,
    /// Require a payload but do not interpret its provider-specific bytes.
    Present,
    /// Require no payload.
    Absent,
    /// Require one exact canonical payload envelope.
    Exact(CanonicalPayload),
}

/// Expected relationship between an effect-evidence generation and the operation envelope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectGenerationExpectation {
    /// The evidence must explicitly declare the generation unknown.
    Unknown,
    /// The evidence must equal the generation immediately before dispatch.
    OperationBefore,
    /// The evidence must equal the generation returned by the operation.
    OperationAfter,
    /// The evidence must equal one fixed provider-local generation.
    Exact(u64),
}

/// Presence or exact-value expectation for one optional effect-evidence string.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OptionalTextExpectation {
    /// The field must be absent.
    Absent,
    /// The field must be present; the provider chooses its nonempty value.
    Present,
    /// The field must contain this exact value.
    Exact(String),
}

/// Cardinality or exact-value expectation for a committed-effect item partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ItemRefsExpectation {
    /// The partition must be empty.
    Empty,
    /// Any valid bounded partition, including an empty one, is accepted.
    Any,
    /// The partition must contain at least one valid item reference.
    NonEmpty,
    /// The partition must equal these ordered item references exactly.
    Exact(Vec<String>),
}

/// Complete nine-field committed-effect expectation.
///
/// Provider-neutral fixtures can require presence and generation anchoring
/// without inventing provider-local receipts or item references. Provider-
/// specific fixtures can use exact values for every field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpectedCommittedEffect {
    /// Truthful committed-effect state.
    pub state: CommittedEffectState,
    /// Exact-boundary presence or value.
    pub committed_boundary: OptionalTextExpectation,
    /// Generation before the effect.
    pub state_generation_before: EffectGenerationExpectation,
    /// Generation after settlement or reconciliation.
    pub state_generation_after: EffectGenerationExpectation,
    /// Committed provider-local item partition.
    pub committed_item_refs: ItemRefsExpectation,
    /// Uncommitted provider-local item partition.
    pub uncommitted_item_refs: ItemRefsExpectation,
    /// Effect-receipt presence or exact digest.
    pub provider_receipt_sha256: OptionalTextExpectation,
    /// Reconciliation-action presence or exact value.
    pub reconciliation_action: OptionalTextExpectation,
    /// Verification-digest presence or exact value.
    pub verification_sha256: OptionalTextExpectation,
    /// Deduplicated request idempotency key presence or exact value.
    pub duplicate_of_idempotency_key: OptionalTextExpectation,
    /// Original committing operation presence or exact value.
    pub duplicate_of_operation_id: OptionalTextExpectation,
}

impl ExpectedCommittedEffect {
    /// Requires effect-free evidence bound to the unchanged pre-dispatch generation.
    #[must_use]
    pub fn none() -> Self {
        Self {
            state: CommittedEffectState::None,
            committed_boundary: OptionalTextExpectation::Absent,
            state_generation_before: EffectGenerationExpectation::OperationBefore,
            state_generation_after: EffectGenerationExpectation::OperationBefore,
            committed_item_refs: ItemRefsExpectation::Empty,
            uncommitted_item_refs: ItemRefsExpectation::Empty,
            provider_receipt_sha256: OptionalTextExpectation::Absent,
            reconciliation_action: OptionalTextExpectation::Absent,
            verification_sha256: OptionalTextExpectation::Absent,
            duplicate_of_idempotency_key: OptionalTextExpectation::Absent,
            duplicate_of_operation_id: OptionalTextExpectation::Absent,
        }
    }

    /// Requires a fully committed effect with provider-chosen receipt, references, and verification.
    #[must_use]
    pub fn committed() -> Self {
        Self {
            state: CommittedEffectState::Committed,
            committed_boundary: OptionalTextExpectation::Absent,
            state_generation_before: EffectGenerationExpectation::OperationBefore,
            state_generation_after: EffectGenerationExpectation::OperationAfter,
            committed_item_refs: ItemRefsExpectation::Any,
            uncommitted_item_refs: ItemRefsExpectation::Empty,
            provider_receipt_sha256: OptionalTextExpectation::Present,
            reconciliation_action: OptionalTextExpectation::Absent,
            verification_sha256: OptionalTextExpectation::Present,
            duplicate_of_idempotency_key: OptionalTextExpectation::Absent,
            duplicate_of_operation_id: OptionalTextExpectation::Absent,
        }
    }

    /// Requires a duplicate acknowledgement bound to the exact request key and
    /// the operation whose earlier delivery actually committed.
    ///
    /// The generation must not move and no new partition may be claimed: a
    /// duplicate reports an effect that already existed, so a provider that
    /// advances state here is applying, not deduplicating.
    #[must_use]
    pub fn duplicate(
        duplicate_of_idempotency_key: OptionalTextExpectation,
        duplicate_of_operation_id: OptionalTextExpectation,
    ) -> Self {
        Self {
            state: CommittedEffectState::Duplicate,
            committed_boundary: OptionalTextExpectation::Absent,
            state_generation_before: EffectGenerationExpectation::OperationBefore,
            state_generation_after: EffectGenerationExpectation::OperationBefore,
            committed_item_refs: ItemRefsExpectation::Empty,
            uncommitted_item_refs: ItemRefsExpectation::Empty,
            provider_receipt_sha256: OptionalTextExpectation::Present,
            reconciliation_action: OptionalTextExpectation::Absent,
            verification_sha256: OptionalTextExpectation::Absent,
            duplicate_of_idempotency_key,
            duplicate_of_operation_id,
        }
    }

    /// Requires a partitioned partial effect with all reconciliation evidence present.
    #[must_use]
    pub fn partial() -> Self {
        Self {
            state: CommittedEffectState::Partial,
            committed_boundary: OptionalTextExpectation::Present,
            state_generation_before: EffectGenerationExpectation::OperationBefore,
            state_generation_after: EffectGenerationExpectation::OperationAfter,
            committed_item_refs: ItemRefsExpectation::NonEmpty,
            uncommitted_item_refs: ItemRefsExpectation::NonEmpty,
            provider_receipt_sha256: OptionalTextExpectation::Present,
            reconciliation_action: OptionalTextExpectation::Present,
            verification_sha256: OptionalTextExpectation::Present,
            duplicate_of_idempotency_key: OptionalTextExpectation::Absent,
            duplicate_of_operation_id: OptionalTextExpectation::Absent,
        }
    }

    /// Requires an explicitly unknown effect without a fabricated partition or generation.
    #[must_use]
    pub fn unknown() -> Self {
        Self {
            state: CommittedEffectState::Unknown,
            committed_boundary: OptionalTextExpectation::Absent,
            state_generation_before: EffectGenerationExpectation::Unknown,
            state_generation_after: EffectGenerationExpectation::Unknown,
            committed_item_refs: ItemRefsExpectation::Empty,
            uncommitted_item_refs: ItemRefsExpectation::Empty,
            provider_receipt_sha256: OptionalTextExpectation::Present,
            reconciliation_action: OptionalTextExpectation::Present,
            verification_sha256: OptionalTextExpectation::Absent,
            duplicate_of_idempotency_key: OptionalTextExpectation::Absent,
            duplicate_of_operation_id: OptionalTextExpectation::Absent,
        }
    }

    fn materialize_for_validation(&self) -> Result<CommittedEffectEvidence, ApiError> {
        const VALIDATION_DIGEST: &str =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let generations = match self.state {
            CommittedEffectState::None => {
                let before_unknown =
                    self.state_generation_before == EffectGenerationExpectation::Unknown;
                let after_unknown =
                    self.state_generation_after == EffectGenerationExpectation::Unknown;
                if before_unknown != after_unknown {
                    return Err(ApiError::InvalidEffectGenerations);
                }
                if let (
                    EffectGenerationExpectation::Exact(before),
                    EffectGenerationExpectation::Exact(after),
                ) = (self.state_generation_before, self.state_generation_after)
                    && before != after
                {
                    return Err(ApiError::InvalidEffectGenerations);
                }
                if before_unknown {
                    (None, None)
                } else {
                    (Some(1), Some(1))
                }
            }
            CommittedEffectState::Committed | CommittedEffectState::Partial => {
                if self.state_generation_before == EffectGenerationExpectation::Unknown
                    || self.state_generation_after == EffectGenerationExpectation::Unknown
                {
                    return Err(ApiError::InvalidEffectGenerations);
                }
                if let (
                    EffectGenerationExpectation::Exact(before),
                    EffectGenerationExpectation::Exact(after),
                ) = (self.state_generation_before, self.state_generation_after)
                    && after < before
                {
                    return Err(ApiError::InvalidEffectGenerations);
                }
                (Some(1), Some(2))
            }
            CommittedEffectState::Duplicate => {
                if self.state_generation_before == EffectGenerationExpectation::Unknown
                    || self.state_generation_after == EffectGenerationExpectation::Unknown
                    || self.state_generation_before != self.state_generation_after
                {
                    return Err(ApiError::InvalidEffectGenerations);
                }
                (Some(1), Some(1))
            }
            CommittedEffectState::Unknown => {
                if self.state_generation_before != EffectGenerationExpectation::Unknown
                    || self.state_generation_after != EffectGenerationExpectation::Unknown
                {
                    return Err(ApiError::InvalidEffectGenerations);
                }
                (None, None)
            }
        };
        let text = |expectation: &OptionalTextExpectation, present: &str| match expectation {
            OptionalTextExpectation::Absent => None,
            OptionalTextExpectation::Present => Some(present.to_owned()),
            OptionalTextExpectation::Exact(value) => Some(value.clone()),
        };
        let mut occupied_refs = self
            .committed_item_refs
            .exact_values()
            .iter()
            .chain(self.uncommitted_item_refs.exact_values())
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut fresh_ref = |prefix: &str| {
            for index in 0..=MAX_COMMITTED_EFFECT_ITEM_REFS {
                let candidate = format!("{prefix}-{index}");
                if occupied_refs.insert(candidate.clone()) {
                    return Ok(candidate);
                }
            }
            Err(ApiError::TooManyEffectItemRefs {
                maximum: MAX_COMMITTED_EFFECT_ITEM_REFS,
            })
        };
        let committed_witness = fresh_ref("validation-committed-item")?;
        let uncommitted_witness = fresh_ref("validation-uncommitted-item")?;
        let refs = |expectation: &ItemRefsExpectation, present: &str| match expectation {
            ItemRefsExpectation::Empty => Vec::new(),
            ItemRefsExpectation::Any if self.state == CommittedEffectState::Partial => {
                vec![present.to_owned()]
            }
            ItemRefsExpectation::Any => Vec::new(),
            ItemRefsExpectation::NonEmpty => vec![present.to_owned()],
            ItemRefsExpectation::Exact(values) => values.clone(),
        };
        CommittedEffectEvidence::from_parts(CommittedEffectEvidenceParts {
            state: self.state,
            committed_boundary: text(&self.committed_boundary, "validation-boundary"),
            state_generation_before: generations.0,
            state_generation_after: generations.1,
            committed_item_refs: refs(&self.committed_item_refs, &committed_witness),
            uncommitted_item_refs: refs(&self.uncommitted_item_refs, &uncommitted_witness),
            provider_receipt_sha256: text(&self.provider_receipt_sha256, VALIDATION_DIGEST),
            reconciliation_action: text(
                &self.reconciliation_action,
                "validation-reconciliation-action",
            ),
            verification_sha256: text(&self.verification_sha256, VALIDATION_DIGEST),
            duplicate_of_idempotency_key: text(
                &self.duplicate_of_idempotency_key,
                VALIDATION_DIGEST,
            ),
            duplicate_of_operation_id: text(
                &self.duplicate_of_operation_id,
                "validation-duplicate-of-operation",
            ),
        })
    }
}

impl ItemRefsExpectation {
    fn exact_values(&self) -> &[String] {
        match self {
            Self::Exact(values) => values,
            Self::Empty | Self::Any | Self::NonEmpty => &[],
        }
    }
}

/// Provider-neutral expected outcome of one operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationExpectation {
    /// Accepted typed terminal outcomes.
    pub terminal: TerminalExpectation,
    /// Complete structured committed-effect expectation.
    pub committed_effect: ExpectedCommittedEffect,
    /// Exact fallback decision, including any policy pin and reason.
    pub fallback: FallbackDirective,
    /// Provider-local state-generation relationship.
    pub state_generation: GenerationExpectation,
    /// Provider payload shape or exact envelope.
    pub payload: PayloadExpectation,
}

/// Provider-neutral operation template materialized after a successful handshake.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationFixture {
    /// Stable scenario step identity.
    pub step_id: String,
    /// Capability-routed provider operation.
    pub operation: ProviderOperation,
    /// Stable request identity.
    pub request_id: String,
    /// Stable operation identity.
    pub operation_id: String,
    /// Deterministic idempotency key for mutating operations.
    pub idempotency_key: Option<String>,
    /// Canonical provider-neutral input payload.
    pub payload: CanonicalPayload,
    /// Required capabilities, including the operation capability.
    pub required_capabilities: Vec<OwnedVersionedId>,
    /// Opaque extensions passed through without activating evaluator behavior.
    pub extensions: Vec<OwnedOpaqueExtension>,
    /// Deadline and cancellation behavior.
    pub control: RequestControlFixture,
    /// Expected typed outcome.
    pub expectation: OperationExpectation,
}

/// Exact, typed, provider-neutral scenario fixture.
#[derive(Clone, Debug)]
pub struct ScenarioFixture {
    fixture_id: String,
    identity: FixtureIdentity,
    exact_scope: OwnedExactScope,
    registration_revision: u64,
    handshake: HandshakeFixture,
    operations: Vec<OperationFixture>,
}

/// Immutable identity of every semantic input that can affect one scenario run.
///
/// This snapshot is fixture-controlled. It deliberately retains the complete
/// handshake and ordered operation templates so reports cannot compare equal
/// after a scope, registration revision, operation kind, request control,
/// payload, extension, capability, or expectation changes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScenarioIdentity {
    fixture_id: String,
    fixture_identity: FixtureIdentity,
    exact_scope_sha256: String,
    registration_revision: u64,
    handshake: HandshakeFixture,
    operations: Vec<OperationFixture>,
}

impl ScenarioIdentity {
    fn from_fixture(fixture: &ScenarioFixture) -> Self {
        Self {
            fixture_id: fixture.fixture_id.clone(),
            fixture_identity: fixture.identity.clone(),
            exact_scope_sha256: fixture.exact_scope.exact_scope_sha256(),
            registration_revision: fixture.registration_revision,
            handshake: fixture.handshake.clone(),
            operations: fixture.operations.clone(),
        }
    }

    /// Returns the stable textual fixture identifier.
    #[must_use]
    pub fn fixture_id(&self) -> &str {
        &self.fixture_id
    }

    /// Returns the exact contract, provider, and build identity.
    #[must_use]
    pub const fn fixture_identity(&self) -> &FixtureIdentity {
        &self.fixture_identity
    }

    /// Returns the canonical digest of the exact TraceDecay coding scope.
    #[must_use]
    pub fn exact_scope_sha256(&self) -> &str {
        &self.exact_scope_sha256
    }

    /// Returns the accepted provider registration revision.
    #[must_use]
    pub const fn registration_revision(&self) -> u64 {
        self.registration_revision
    }

    /// Returns the exact handshake input and expectation.
    #[must_use]
    pub const fn handshake(&self) -> &HandshakeFixture {
        &self.handshake
    }

    /// Returns exact operation inputs and expectations in execution order.
    #[must_use]
    pub fn operations(&self) -> &[OperationFixture] {
        &self.operations
    }
}

impl ScenarioFixture {
    /// Builds a scenario and rejects empty or duplicate stable step identities.
    pub fn new(
        fixture_id: impl Into<String>,
        identity: FixtureIdentity,
        exact_scope: OwnedExactScope,
        registration_revision: u64,
        handshake: HandshakeFixture,
        operations: Vec<OperationFixture>,
    ) -> Result<Self, EvaluationError> {
        let fixture_id = fixture_id.into();
        require_non_empty(&fixture_id, "fixture_id")?;
        require_non_empty(&handshake.step_id, "handshake_step_id")?;
        require_non_empty(&handshake.request_id, "handshake_request_id")?;
        validate_terminal_expectation(
            ProviderOperation::Handshake,
            identity.provider().provider_id(),
            handshake.expectation.terminal_code,
            &handshake.expectation.committed_effect,
            &handshake.expectation.fallback,
        )?;
        if !operations.is_empty() && !handshake.expectation.require_ready_receipt {
            return Err(EvaluationError::OperationsRequireReadyReceipt);
        }
        if !operations.is_empty() && !handshake.expectation.require_descriptor {
            return Err(EvaluationError::OperationsRequireHandshakeDescriptor);
        }
        if !operations.is_empty() && !handshake.expectation.require_accepted_scope {
            return Err(EvaluationError::OperationsRequireAcceptedScope);
        }
        if !operations.is_empty() && handshake.expectation.terminal_code != TerminalCode::Success {
            return Err(EvaluationError::OperationsRequireSuccessfulHandshake);
        }
        let handshake_capabilities = handshake
            .required_capabilities
            .iter()
            .map(OwnedVersionedId::as_str)
            .collect::<BTreeSet<_>>();
        let mut step_ids = BTreeSet::from([handshake.step_id.clone()]);
        let mut request_ids = BTreeSet::from([handshake.request_id.clone()]);
        let mut operation_ids = BTreeSet::new();
        for operation in &operations {
            require_non_empty(&operation.step_id, "operation_step_id")?;
            require_non_empty(&operation.request_id, "operation_request_id")?;
            require_non_empty(&operation.operation_id, "operation_id")?;
            for terminal_code in operation.expectation.terminal.iter() {
                validate_terminal_expectation(
                    operation.operation,
                    identity.provider().provider_id(),
                    terminal_code,
                    &operation.expectation.committed_effect,
                    &operation.expectation.fallback,
                )?;
            }
            if !step_ids.insert(operation.step_id.clone()) {
                return Err(EvaluationError::DuplicateStepId(operation.step_id.clone()));
            }
            if !operation_ids.insert(operation.operation_id.clone()) {
                return Err(EvaluationError::DuplicateOperationId(
                    operation.operation_id.clone(),
                ));
            }
            if !request_ids.insert(operation.request_id.clone()) {
                return Err(EvaluationError::DuplicateRequestId(
                    operation.request_id.clone(),
                ));
            }
            if operation.operation.mutates_provider_state()
                && operation
                    .idempotency_key
                    .as_deref()
                    .is_none_or(str::is_empty)
            {
                return Err(EvaluationError::MissingFixtureIdempotencyKey {
                    step_id: operation.step_id.clone(),
                });
            }
            let operation_capability = operation.operation.capability_id();
            if !operation
                .required_capabilities
                .iter()
                .any(|capability| capability.as_str() == operation_capability)
            {
                return Err(EvaluationError::MissingFixtureOperationCapability {
                    step_id: operation.step_id.clone(),
                    capability_id: operation_capability,
                });
            }
            for capability in &operation.required_capabilities {
                if !handshake_capabilities.contains(capability.as_str()) {
                    return Err(EvaluationError::OperationCapabilityNotNegotiated {
                        step_id: operation.step_id.clone(),
                        capability_id: capability.as_str().to_owned(),
                    });
                }
            }
        }
        Ok(Self {
            fixture_id,
            identity,
            exact_scope,
            registration_revision,
            handshake,
            operations,
        })
    }

    /// Returns the stable fixture identity.
    #[must_use]
    pub fn fixture_id(&self) -> &str {
        &self.fixture_id
    }

    /// Returns exact contract, provider, and build identities.
    #[must_use]
    pub const fn identity(&self) -> &FixtureIdentity {
        &self.identity
    }

    /// Returns the exact TraceDecay-owned coding scope.
    #[must_use]
    pub const fn exact_scope(&self) -> &OwnedExactScope {
        &self.exact_scope
    }

    /// Returns the accepted provider registration revision.
    #[must_use]
    pub const fn registration_revision(&self) -> u64 {
        self.registration_revision
    }

    /// Returns the handshake template.
    #[must_use]
    pub const fn handshake(&self) -> &HandshakeFixture {
        &self.handshake
    }

    /// Returns operation templates in deterministic execution order.
    #[must_use]
    pub fn operations(&self) -> &[OperationFixture] {
        &self.operations
    }

    /// Returns the exact number of planned provider calls, including handshake.
    #[must_use]
    pub fn planned_steps(&self) -> usize {
        self.operations.len().saturating_add(1)
    }

    /// Iterates all planned step identities in deterministic execution order.
    pub fn planned_step_ids(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.handshake.step_id.as_str()).chain(
            self.operations
                .iter()
                .map(|operation| operation.step_id.as_str()),
        )
    }

    /// Captures every semantic scenario input as one immutable report identity.
    #[must_use]
    pub fn scenario_identity(&self) -> ScenarioIdentity {
        ScenarioIdentity::from_fixture(self)
    }
}

fn validate_terminal_expectation(
    operation: ProviderOperation,
    provider_id: &OwnedProviderId,
    terminal_code: TerminalCode,
    committed_effect: &ExpectedCommittedEffect,
    fallback: &FallbackDirective,
) -> Result<(), ApiError> {
    TerminalRecord::new(
        operation,
        provider_id.clone(),
        terminal_code,
        committed_effect.materialize_for_validation()?,
        fallback.clone(),
        "fixture-validation-operation",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some("fixture.validation".to_owned()),
    )?;
    Ok(())
}

/// Builds the mandatory provider-health, observe/idempotency, recall, cancellation, and deadline suite.
pub fn mandatory_conformance_fixture(
    identity: FixtureIdentity,
    exact_scope: OwnedExactScope,
    registration_revision: u64,
) -> Result<ScenarioFixture, EvaluationError> {
    let live_control = RequestControlFixture::live(LIVE_DEADLINE_UTC_MICROS, 5_000)?;
    let handshake = HandshakeFixture {
        step_id: "mandatory.handshake".to_owned(),
        request_id: "mandatory-handshake-request".to_owned(),
        required_capabilities: mandatory_capabilities()?,
        host_limits: conformance_limits(),
        control: live_control,
        challenge_nonce: [0x5a; 32],
        expectation: HandshakeExpectation {
            terminal_code: TerminalCode::Success,
            committed_effect: ExpectedCommittedEffect::none(),
            fallback: FallbackDirective::forbidden(),
            require_descriptor: true,
            require_accepted_scope: true,
            require_ready_receipt: true,
        },
    };

    let operations = vec![
        operation_fixture(
            "mandatory.health",
            ProviderOperation::Health,
            "mandatory-health-request",
            "mandatory-health-operation",
            None,
            live_control,
            OperationExpectation {
                terminal: TerminalExpectation::exactly(TerminalCode::Success),
                committed_effect: ExpectedCommittedEffect::none(),
                fallback: FallbackDirective::forbidden(),
                state_generation: GenerationExpectation::Unchanged,
                payload: PayloadExpectation::Exact(canonical_payload(
                    "tracedecay.memory.provider.health.v1",
                    b"{\"state_generation\":0,\"stored_observations\":0}",
                    "79dad4076e4f26e52d0fe0f42b11c1e6d067bf86df1ad4a27a6257b95df6d5a8",
                )?),
            },
        )?,
        operation_fixture(
            "mandatory.observe",
            ProviderOperation::Observe,
            "mandatory-observe-request",
            "mandatory-observe-operation",
            Some("mandatory-observation-key"),
            live_control,
            OperationExpectation {
                terminal: TerminalExpectation::exactly(TerminalCode::Success),
                committed_effect: ExpectedCommittedEffect::committed(),
                fallback: FallbackDirective::forbidden(),
                state_generation: GenerationExpectation::IncreasedBy(1),
                payload: PayloadExpectation::Exact(canonical_payload(
                    "tracedecay.memory.provider.observation.v1",
                    b"{\"acceptance\":\"applied\",\"acknowledged_sequence\":1}",
                    "079290f0c05aa81319b58c467e568318dbda0bc798d25e58bd0070ac54045b2b",
                )?),
            },
        )?,
        operation_fixture(
            "mandatory.observe_duplicate",
            ProviderOperation::Observe,
            "mandatory-observe-duplicate-request",
            "mandatory-observe-duplicate-operation",
            Some("mandatory-observation-key"),
            live_control,
            OperationExpectation {
                terminal: TerminalExpectation::exactly(TerminalCode::Success),
                // The redelivery carries the first observe's idempotency key
                // under its own operation id, so the acknowledgement has to name
                // the key it deduplicated and the operation that committed.
                committed_effect: ExpectedCommittedEffect::duplicate(
                    OptionalTextExpectation::Exact("mandatory-observation-key".to_owned()),
                    OptionalTextExpectation::Exact("mandatory-observe-operation".to_owned()),
                ),
                fallback: FallbackDirective::forbidden(),
                state_generation: GenerationExpectation::Unchanged,
                payload: PayloadExpectation::Exact(canonical_payload(
                    "tracedecay.memory.provider.observation.v1",
                    b"{\"acceptance\":\"duplicate_acknowledged\",\"acknowledged_sequence\":1}",
                    "b3cf4f2813b9341a1131597e91af0d507b4b13ae9a921645f419e9b9de4946e3",
                )?),
            },
        )?,
        operation_fixture(
            "mandatory.recall",
            ProviderOperation::Recall,
            "mandatory-recall-request",
            "mandatory-recall-operation",
            None,
            live_control,
            OperationExpectation {
                terminal: TerminalExpectation::exactly(TerminalCode::Success),
                committed_effect: ExpectedCommittedEffect::none(),
                fallback: FallbackDirective::forbidden(),
                state_generation: GenerationExpectation::Unchanged,
                payload: PayloadExpectation::Exact(canonical_payload(
                    "tracedecay.memory.provider.recall.v1",
                    b"{\"candidate_count\":1,\"coverage_complete\":true}",
                    "91c216d20632851156ab0f89e9738d4443c08e624a9b6c58ff7c0f1dd20e3b1c",
                )?),
            },
        )?,
        operation_fixture(
            "mandatory.cancelled_recall",
            ProviderOperation::Recall,
            "mandatory-cancelled-recall-request",
            "mandatory-cancelled-recall-operation",
            None,
            RequestControlFixture::cancelled(LIVE_DEADLINE_UTC_MICROS, 5_000),
            OperationExpectation {
                terminal: TerminalExpectation::exactly(TerminalCode::Cancelled),
                committed_effect: ExpectedCommittedEffect::none(),
                fallback: FallbackDirective::forbidden(),
                state_generation: GenerationExpectation::Unchanged,
                payload: PayloadExpectation::Absent,
            },
        )?,
        operation_fixture(
            "mandatory.expired_recall",
            ProviderOperation::Recall,
            "mandatory-expired-recall-request",
            "mandatory-expired-recall-operation",
            None,
            RequestControlFixture::expired(LIVE_DEADLINE_UTC_MICROS),
            OperationExpectation {
                terminal: TerminalExpectation::exactly(TerminalCode::DeadlineExceeded),
                committed_effect: ExpectedCommittedEffect::none(),
                fallback: FallbackDirective::forbidden(),
                state_generation: GenerationExpectation::Unchanged,
                payload: PayloadExpectation::Absent,
            },
        )?,
    ];

    ScenarioFixture::new(
        "tracedecay.memory.mandatory-conformance.v1",
        identity,
        exact_scope,
        registration_revision,
        handshake,
        operations,
    )
}

fn operation_fixture(
    step_id: &str,
    operation: ProviderOperation,
    request_id: &str,
    operation_id: &str,
    idempotency_key: Option<&str>,
    control: RequestControlFixture,
    expectation: OperationExpectation,
) -> Result<OperationFixture, ApiError> {
    Ok(OperationFixture {
        step_id: step_id.to_owned(),
        operation,
        request_id: request_id.to_owned(),
        operation_id: operation_id.to_owned(),
        idempotency_key: idempotency_key.map(str::to_owned),
        payload: conformance_payload(operation)?,
        required_capabilities: vec![OwnedVersionedId::new(operation.capability_id())?],
        extensions: Vec::new(),
        control,
        expectation,
    })
}

fn conformance_payload(operation: ProviderOperation) -> Result<CanonicalPayload, ApiError> {
    match operation {
        ProviderOperation::Health => canonical_payload(
            "tracedecay.memory.provider.health.v1",
            b"{}",
            EMPTY_OBJECT_SHA256,
        ),
        ProviderOperation::Observe => canonical_payload(
            "tracedecay.memory.provider.observation.v1",
            b"mandatory-memory-content",
            "e2cb333c1f9ac5b0285bd10fd6844c1a578b9b4701c77a438cb332a50c0142c1",
        ),
        ProviderOperation::Recall => canonical_payload(
            "tracedecay.memory.provider.recall.v1",
            b"query=mandatory-memory\nmaximum_candidates=16\n",
            "d53d74df75a7552258e6c0bbd0ab0ba3967d3991fad1beed4f5e6d603f13160c",
        ),
        _ => canonical_payload(
            "tracedecay.memory.provider.terminal.v1",
            b"{}",
            EMPTY_OBJECT_SHA256,
        ),
    }
}

fn canonical_payload(
    contract_id: &str,
    bytes: &[u8],
    sha256: &str,
) -> Result<CanonicalPayload, ApiError> {
    CanonicalPayload::new(OwnedVersionedId::new(contract_id)?, bytes.to_vec(), sha256)
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

const fn conformance_limits() -> ProviderLimits {
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

fn require_non_empty(value: &str, field: &'static str) -> Result<(), EvaluationError> {
    if value.is_empty() {
        Err(EvaluationError::EmptyField(field))
    } else {
        Ok(())
    }
}

fn require_identity_match(
    field: &'static str,
    expected: &str,
    actual: &str,
) -> Result<(), EvaluationError> {
    if expected == actual {
        Ok(())
    } else {
        Err(EvaluationError::ProviderIdentityMismatch {
            field,
            expected: expected.to_owned(),
            actual: actual.to_owned(),
        })
    }
}

pub(crate) fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
