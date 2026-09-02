//! Baseline lanes over the deterministic coding-memory scenario corpus.
//!
//! Three lanes run the identical corpus, fixture, scope catalog, recall
//! catalog, host limits, and rubric so their reports are comparable:
//!
//! * [`BaselineLane::NoMemory`] issues no provider call and admits no context.
//! * [`BaselineLane::ExplicitDocumentation`] admits the fixture's documentation
//!   files at the current code revision as the only context lane.
//! * [`BaselineLane::Provider`] drives every scenario step through one real
//!   [`MemoryProvider`] and admits the candidates it returns.
//!
//! Lanes are typed runner behaviors, not provider look-alikes: the first two
//! never construct a `MemoryProvider`. Every report carries a
//! [`BaselineRunIdentity`] that binds the complete shared input set, so
//! reports from differing corpora, fixtures, catalogs, or host configuration
//! cannot be compared as if they were equal. Context cost is recorded as exact
//! admitted bytes plus the token count of a pinned, versioned
//! [`TokenEstimator`] ([`O200kBaseTokenEstimator`] by default) whose identity
//! is bound into the run identity; a run configured without an estimator
//! records token costs as typed `indeterminate`. Adjudication verdicts are
//! earned only from evidence: a scope or corruption check with no admitted
//! entries to inspect is typed `indeterminate`, never a vacuous pass.
//! Per-call latency is measured but deliberately excluded from the canonical
//! report bytes, which must be identical across reruns.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::Serialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tiktoken_rs::o200k_base_singleton;
use tracedecay_memory_provider_api::contract::{
    CONTRACT_SET_ID, CONTRACT_SET_SHA256, CommittedEffectState, RecallScopeBinding, TerminalCode,
};
use tracedecay_memory_provider_api::{
    ApiError, CancellationToken, CanonicalPayload, HandshakeRequest, HandshakeRequestParts,
    MemoryProvider, OperationControl, OwnedExactScope, OwnedProviderId, OwnedVersionedId,
    ProviderCall, ProviderCallParts, ProviderDescriptor, ProviderLimits, ProviderOperation,
    ProviderReply,
};

use crate::canonical::{
    CanonicalJsonError, canonical_json, canonical_json_sha256, lowercase_sha256_hex,
};
use crate::runner::admitted;
use crate::scenario_corpus::{
    CorpusError, DigestStatus, FixtureDefinition, ObservationDefinition, RecallRequestDefinition,
    RecallRequestOperation, ScenarioCorpus, ScenarioDefinition, ScenarioStep, ScopeEntry,
};

const HEALTH_CONTRACT_ID: &str = "tracedecay.memory.provider.health.v1";
const OBSERVATION_CONTRACT_ID: &str = "tracedecay.memory.provider.observation.v1";
const RECALL_CONTRACT_ID: &str = "tracedecay.memory.provider.recall.v1";
const DELETE_BY_SOURCE_CONTRACT_ID: &str = "tracedecay.memory.provider.deletion-by-source.v1";
const SNAPSHOT_RESTORE_CONTRACT_ID: &str = "tracedecay.memory.provider.snapshot-restore.v1";
const DEFAULT_DEADLINE_UTC_MICROS: i64 = 4_102_444_800_000_000; // 2100-01-01T00:00:00Z
const DEFAULT_REMAINING_MILLIS: u64 = 5_000;
const EVALUATOR_NONE_PINNED: &str = "none_pinned";
/// Evaluator recorded when a scope or corruption check had no admitted
/// entries to inspect; the verdict is indeterminate and earns no basis points.
const EVALUATOR_VACUOUS_ZERO_ADMISSION: &str = "vacuous_zero_admission";
const BATCH_TEMPLATE_EVENT_TYPE: &str = "observation_batch_requested";

/// Baseline execution or report failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BaselineError {
    /// The corpus failed to load or validate.
    Corpus(CorpusError),
    /// A report could not be serialized canonically.
    Canonical(CanonicalJsonError),
    /// The provider API rejected a materialized request or call.
    ProviderApi(ApiError),
    /// The run configuration was invalid.
    InvalidConfig(&'static str),
    /// A scenario referenced something the loaded corpus does not define.
    UnknownReference {
        /// Reference kind.
        kind: &'static str,
        /// Missing value.
        value: String,
    },
    /// The fixture workspace could not be created, written, or removed.
    Workspace {
        /// Path involved.
        path: String,
        /// Operating-system detail.
        detail: String,
    },
    /// Fixture bytes on disk no longer matched the corpus revision digest.
    FixtureDrift {
        /// Repository-relative path.
        path: String,
        /// Expected digest.
        expected_sha256: String,
        /// Observed digest.
        actual_sha256: String,
    },
    /// A batch step arrived without an open batch or while one was open.
    BatchState {
        /// Scenario identity.
        scenario_id: String,
        /// Step number.
        step: u32,
        /// What was wrong.
        detail: &'static str,
    },
    /// A corpus observation payload lacked a field the step vocabulary needs.
    ObservationPayloadShape {
        /// Observation identity.
        observation_id: String,
        /// Missing or malformed field.
        field: &'static str,
    },
    /// Reports offered for comparison did not share identical inputs.
    ComparisonInputs {
        /// Which binding differed.
        detail: String,
    },
    /// The pinned token estimator could not count admitted context bytes.
    TokenEstimate {
        /// Recall request whose admitted bytes were being counted.
        request_id: String,
        /// Typed estimator failure.
        error: TokenEstimateError,
    },
}

impl fmt::Display for BaselineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Corpus(error) => write!(formatter, "scenario corpus rejected: {error}"),
            Self::Canonical(error) => write!(formatter, "canonical report failed: {error}"),
            Self::ProviderApi(error) => {
                write!(formatter, "provider API rejected baseline call: {error}")
            }
            Self::InvalidConfig(detail) => {
                write!(formatter, "baseline configuration is invalid: {detail}")
            }
            Self::UnknownReference { kind, value } => {
                write!(formatter, "baseline references unknown {kind} {value}")
            }
            Self::Workspace { path, detail } => {
                write!(formatter, "fixture workspace failure at {path}: {detail}")
            }
            Self::FixtureDrift {
                path,
                expected_sha256,
                actual_sha256,
            } => write!(
                formatter,
                "fixture file {path} drifted: expected {expected_sha256}, found {actual_sha256}"
            ),
            Self::BatchState {
                scenario_id,
                step,
                detail,
            } => write!(
                formatter,
                "scenario {scenario_id} step {step} has invalid batch state: {detail}"
            ),
            Self::ObservationPayloadShape {
                observation_id,
                field,
            } => write!(
                formatter,
                "observation {observation_id} payload lacks usable field {field}"
            ),
            Self::ComparisonInputs { detail } => {
                write!(formatter, "baseline reports are not comparable: {detail}")
            }
            Self::TokenEstimate { request_id, error } => write!(
                formatter,
                "token estimate failed for admitted context of {request_id}: {error}"
            ),
        }
    }
}

impl Error for BaselineError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Corpus(error) => Some(error),
            Self::Canonical(error) => Some(error),
            Self::ProviderApi(error) => Some(error),
            Self::TokenEstimate { error, .. } => Some(error),
            _ => None,
        }
    }
}

impl From<CorpusError> for BaselineError {
    fn from(error: CorpusError) -> Self {
        Self::Corpus(error)
    }
}

impl From<CanonicalJsonError> for BaselineError {
    fn from(error: CanonicalJsonError) -> Self {
        Self::Canonical(error)
    }
}

impl From<ApiError> for BaselineError {
    fn from(error: ApiError) -> Self {
        Self::ProviderApi(error)
    }
}

/// Typed failure of a token estimator over admitted context bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TokenEstimateError {
    /// The admitted bytes were not valid UTF-8, which a BPE text tokenizer
    /// cannot count without altering the content.
    NotUtf8 {
        /// Number of leading bytes that were valid UTF-8.
        valid_up_to: usize,
    },
}

impl fmt::Display for TokenEstimateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotUtf8 { valid_up_to } => write!(
                formatter,
                "admitted context is not UTF-8 after {valid_up_to} bytes"
            ),
        }
    }
}

impl Error for TokenEstimateError {}

/// Pinned, versioned token estimator applied to admitted context bytes.
///
/// Every estimate is a measurement under a named tokenizer, never a
/// heuristic presented as one: the estimator identity and revision are bound
/// into the run identity of every report that used it.
pub trait TokenEstimator {
    /// Stable estimator identity.
    fn estimator_id(&self) -> &str;
    /// Estimator revision.
    fn estimator_revision(&self) -> &str;
    /// Counts tokens for exact admitted bytes.
    fn estimate_tokens(&self, bytes: &[u8]) -> Result<u64, TokenEstimateError>;
}

/// Identity of the shipped `o200k_base` estimator.
pub const O200K_BASE_ESTIMATOR_ID: &str = "tiktoken.o200k_base";
/// Revision of the shipped `o200k_base` estimator: the pinned `tiktoken-rs`
/// release whose vendored `o200k_base` ranks and pre-tokenizer pattern
/// produce every count.
pub const O200K_BASE_ESTIMATOR_REVISION: &str = "tiktoken-rs-0.12";

/// Production token estimator: exact `o200k_base` BPE token count of the
/// admitted context text.
///
/// It is the default estimator pinned by [`BaselineRunConfig::new`], so every
/// lane of one run records determinate token costs under one identity.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct O200kBaseTokenEstimator;

impl TokenEstimator for O200kBaseTokenEstimator {
    fn estimator_id(&self) -> &str {
        O200K_BASE_ESTIMATOR_ID
    }

    fn estimator_revision(&self) -> &str {
        O200K_BASE_ESTIMATOR_REVISION
    }

    fn estimate_tokens(&self, bytes: &[u8]) -> Result<u64, TokenEstimateError> {
        let text = std::str::from_utf8(bytes).map_err(|error| TokenEstimateError::NotUtf8 {
            valid_up_to: error.valid_up_to(),
        })?;
        let ranks = o200k_base_singleton().encode_ordinary(text);
        Ok(u64::try_from(ranks.len()).unwrap_or(u64::MAX))
    }
}

/// Host configuration shared by every lane of one baseline run.
pub struct BaselineRunConfig {
    /// Absolute UTC deadline carried by every request.
    pub deadline_utc_micros: i64,
    /// Finite remaining budget at every dispatch.
    pub remaining_millis: u64,
    /// Host ceilings offered at every handshake.
    pub host_limits: ProviderLimits,
    /// Caller-owned directory under which fixture workspaces are materialized.
    pub fixture_root: PathBuf,
    /// Pinned token estimator; [`Self::new`] pins [`O200kBaseTokenEstimator`].
    /// `None` records every token cost as typed `indeterminate`.
    pub token_estimator: Option<Box<dyn TokenEstimator>>,
}

impl BaselineRunConfig {
    /// Returns the default host configuration rooted at a caller-owned
    /// directory, with the production `o200k_base` estimator pinned.
    #[must_use]
    pub fn new(fixture_root: PathBuf) -> Self {
        Self {
            deadline_utc_micros: DEFAULT_DEADLINE_UTC_MICROS,
            remaining_millis: DEFAULT_REMAINING_MILLIS,
            host_limits: ProviderLimits {
                request_bytes: 1_048_576,
                response_bytes: 1_048_576,
                observation_batch_items: 1_024,
                recall_candidates: 1_024,
                concurrent_operations: 8,
                operation_millis: 5_000,
                snapshot_bytes: 16_777_216,
                inspection_items: 1_024,
            },
            fixture_root,
            token_estimator: Some(Box::new(O200kBaseTokenEstimator)),
        }
    }

    fn validate(&self) -> Result<(), BaselineError> {
        if self.remaining_millis == 0 {
            return Err(BaselineError::InvalidConfig(
                "remaining_millis must be nonzero",
            ));
        }
        self.host_limits
            .validate()
            .map_err(BaselineError::ProviderApi)?;
        if self.fixture_root.as_os_str().is_empty() {
            return Err(BaselineError::InvalidConfig("fixture_root must be set"));
        }
        Ok(())
    }
}

/// One real provider bound to a baseline lane.
pub struct ProviderLane<'provider> {
    provider: &'provider dyn MemoryProvider,
    registration_revision: u64,
}

impl<'provider> ProviderLane<'provider> {
    /// Binds a provider under an accepted registration revision.
    pub fn new(
        provider: &'provider dyn MemoryProvider,
        registration_revision: u64,
    ) -> Result<Self, BaselineError> {
        if registration_revision == 0 {
            return Err(BaselineError::InvalidConfig(
                "registration_revision must be nonzero",
            ));
        }
        Ok(Self {
            provider,
            registration_revision,
        })
    }
}

/// Typed baseline lane.
pub enum BaselineLane<'provider> {
    /// Memory disabled: no provider call, no admitted context.
    NoMemory,
    /// Explicit documentation at the current code revision is the only context.
    ExplicitDocumentation,
    /// Every step runs through one real provider.
    Provider(ProviderLane<'provider>),
}

impl BaselineLane<'_> {
    /// Returns the stable lane identity recorded in reports.
    #[must_use]
    pub fn lane_id(&self) -> String {
        match self {
            Self::NoMemory => "no_memory".to_owned(),
            Self::ExplicitDocumentation => "explicit_documentation".to_owned(),
            Self::Provider(lane) => {
                format!(
                    "provider:{}",
                    lane.provider.descriptor().provider_id.as_str()
                )
            }
        }
    }

    const fn kind(&self) -> LaneKind {
        match self {
            Self::NoMemory => LaneKind::NoMemory,
            Self::ExplicitDocumentation => LaneKind::ExplicitDocumentation,
            Self::Provider(_) => LaneKind::Provider,
        }
    }
}

/// Lane kind without provider binding.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LaneKind {
    /// Memory disabled.
    NoMemory,
    /// Explicit documentation.
    ExplicitDocumentation,
    /// Real provider.
    Provider,
}

/// Provider identity bound into a provider-lane report.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProviderIdentityRecord {
    /// Stable logical provider identity.
    pub provider_id: String,
    /// Immutable implementation/build digest.
    pub build_identity_sha256: String,
    /// Provider-local state schema identity.
    pub state_schema_version: String,
    /// Accepted registration revision.
    pub registration_revision: u64,
    /// Capabilities declared at run start.
    pub declared_capabilities: Vec<String>,
}

/// Lane identity recorded in every report.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LaneIdentity {
    /// Stable lane identity.
    pub lane_id: String,
    /// Lane kind.
    pub kind: LaneKind,
    /// Provider identity for provider lanes.
    pub provider: Option<ProviderIdentityRecord>,
}

/// Token estimator identity recorded in every report.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TokenEstimatorIdentity {
    /// No estimator was pinned; token counts are indeterminate.
    Indeterminate,
    /// A pinned estimator produced every token estimate.
    Pinned {
        /// Estimator identity.
        estimator_id: String,
        /// Estimator revision.
        estimator_revision: String,
    },
}

/// Finite host ceilings as recorded in reports.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LimitsRecord {
    /// Maximum canonical request bytes.
    pub request_bytes: u64,
    /// Maximum canonical response bytes.
    pub response_bytes: u64,
    /// Maximum observations in one batch.
    pub observation_batch_items: u64,
    /// Maximum recall candidates.
    pub recall_candidates: u64,
    /// Maximum concurrent operations.
    pub concurrent_operations: u64,
    /// Maximum operation duration in milliseconds.
    pub operation_millis: u64,
    /// Maximum snapshot bytes.
    pub snapshot_bytes: u64,
    /// Maximum inspection items.
    pub inspection_items: u64,
}

impl From<ProviderLimits> for LimitsRecord {
    fn from(limits: ProviderLimits) -> Self {
        Self {
            request_bytes: limits.request_bytes,
            response_bytes: limits.response_bytes,
            observation_batch_items: limits.observation_batch_items,
            recall_candidates: limits.recall_candidates,
            concurrent_operations: limits.concurrent_operations,
            operation_millis: limits.operation_millis,
            snapshot_bytes: limits.snapshot_bytes,
            inspection_items: limits.inspection_items,
        }
    }
}

/// Host configuration bound into every report.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HostConfigIdentity {
    /// Absolute UTC deadline.
    pub deadline_utc_micros: i64,
    /// Remaining budget at dispatch.
    pub remaining_millis: u64,
    /// Host ceilings.
    pub host_limits: LimitsRecord,
}

/// Every input shared by all lanes of one run.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SharedInputs {
    /// Stable corpus identity.
    pub corpus_id: String,
    /// SHA-256 of the exact corpus bytes.
    pub corpus_sha256: String,
    /// Corpus schema version.
    pub schema_version: u64,
    /// Bead owning the corpus.
    pub bead_id: String,
    /// Contract set identity compiled into the provider API.
    pub contract_set_id: String,
    /// Contract set digest.
    pub contract_set_sha256: String,
    /// Fixture identities in corpus order.
    pub fixture_ids: Vec<String>,
    /// Fixture digests in corpus order.
    pub fixture_digests: Vec<String>,
    /// SHA-256 over every fixture file revision.
    pub fixture_content_sha256: String,
    /// SHA-256 over the scope catalog.
    pub scope_catalog_sha256: String,
    /// SHA-256 over the recall-request catalog.
    pub recall_catalog_sha256: String,
    /// Scenario identities in corpus order.
    pub scenario_ids: Vec<String>,
    /// SHA-256 over every rubric.
    pub rubric_sha256: String,
    /// Host configuration.
    pub host_config: HostConfigIdentity,
}

/// Complete identity of one baseline run.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BaselineRunIdentity {
    /// SHA-256 over shared inputs, lane, and estimator.
    pub run_identity_sha256: String,
    /// SHA-256 over shared inputs only; equal across comparable lanes.
    pub shared_inputs_sha256: String,
    /// Shared inputs.
    pub shared_inputs: SharedInputs,
    /// Lane identity.
    pub lane: LaneIdentity,
    /// Token estimator identity.
    pub token_estimator: TokenEstimatorIdentity,
}

/// One provider call issued by a step.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProviderCallRecord {
    /// Wire operation kind.
    pub operation: String,
    /// Request identity.
    pub request_id: String,
    /// Operation identity.
    pub operation_id: String,
    /// Exact scope digest the call was addressed to.
    pub exact_scope_sha256: String,
    /// Typed terminal code.
    pub terminal_code: String,
    /// Committed effect state.
    pub committed_effect_state: String,
    /// Diagnostic identity, if any.
    pub diagnostic_id: Option<String>,
    /// Whether provider code was contacted (false for host preflight).
    pub provider_contacted: bool,
    /// State generation the call was addressed to.
    pub state_generation_before: u64,
    /// State generation reported after the call.
    pub state_generation_after: u64,
    /// Canonical request payload bytes.
    pub request_payload_bytes: u64,
    /// Canonical response payload bytes.
    pub response_payload_bytes: u64,
}

/// Count that is exact or explicitly indeterminate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CountRecord {
    /// Exact count.
    Exact {
        /// Value.
        value: u64,
    },
    /// Count could not be determined truthfully.
    Indeterminate {
        /// Why.
        reason: String,
    },
}

/// Token estimate that is pinned or explicitly indeterminate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TokenRecord {
    /// Estimate from the pinned estimator.
    Estimated {
        /// Tokens.
        tokens: u64,
    },
    /// No estimator pinned.
    Indeterminate,
}

/// One unit of context admitted by a lane.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ContextEntry {
    /// Where the bytes came from.
    pub source_kind: String,
    /// Source reference (file path or candidate identity).
    pub source_ref: String,
    /// Fixture revision, when the source is a fixture file.
    pub revision_id: Option<String>,
    /// SHA-256 of the admitted bytes.
    pub sha256: String,
    /// Exact admitted bytes.
    pub bytes: u64,
    /// Whether the entry's exact scope equals the request scope.
    pub scope_match: bool,
    /// Whether the admitted bytes still carry a source key this scenario
    /// already asked the provider to forget.
    pub contains_forgotten_source: bool,
}

/// Context admitted for one recall-class step.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ContextAdmission {
    /// Catalogued request identity.
    pub request_id: String,
    /// Request scope identity.
    pub scope_id: String,
    /// Typed terminal code of the admission.
    pub terminal_code: String,
    /// Candidate count reported by the source.
    pub candidate_count: CountRecord,
    /// Exact bytes admitted into context.
    pub admitted_context_bytes: u64,
    /// Admitted entries.
    pub entries: Vec<ContextEntry>,
    /// Token estimate over admitted bytes.
    pub estimated_tokens: TokenRecord,
    /// Provider calls issued for this admission.
    pub provider_call_count: u64,
    /// Provider response payload bytes received.
    pub provider_response_bytes: u64,
}

/// Committed/uncommitted boundary of a cancelled batch.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BatchBoundary {
    /// Batch identity.
    pub batch_id: String,
    /// Typed terminal code of the batch.
    pub terminal_code: String,
    /// Items with a committed provider effect.
    pub committed_item_ids: Vec<String>,
    /// Items never dispatched or without committed effect.
    pub uncommitted_item_ids: Vec<String>,
    /// Declared item count.
    pub item_count: u64,
}

/// Typed outcome of one step.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StepOutcome {
    /// A typed terminal code.
    Terminal {
        /// Terminal code.
        terminal_code: String,
        /// Committed effect state.
        committed_effect_state: String,
    },
    /// The lane performs no work for this action.
    NotApplicable {
        /// Why.
        reason: String,
    },
    /// The observation event type has no provider observation kind.
    NotAdmissible {
        /// Why.
        reason: String,
        /// Corpus event type.
        event_type: String,
    },
    /// A fixture file moved to a later revision.
    WorkspaceAdvanced {
        /// Revision materialized.
        revision_id: String,
        /// Path rewritten.
        path: String,
        /// Content digest now on disk.
        content_sha256: String,
    },
    /// A new agent session was opened at another scope.
    SessionOpened {
        /// New scope.
        scope_id: String,
        /// Handshake terminal for provider lanes.
        handshake_terminal_code: Option<String>,
    },
    /// The provider was restarted from the host's point of view.
    ProviderRestarted {
        /// Restart identity.
        restart_id: String,
        /// Whether the immutable descriptor stayed stable.
        descriptor_stable: bool,
        /// State generation observed after restart.
        state_generation: u64,
    },
    /// A batch was opened.
    BatchOpened {
        /// Batch identity.
        batch_id: String,
        /// Declared item count.
        item_count: u64,
    },
    /// A batch was cancelled at an explicit boundary.
    BatchCancelled {
        /// Boundary.
        boundary: BatchBoundary,
    },
    /// A cancelled batch was resumed.
    BatchResumed {
        /// Resume cursor.
        resume_cursor: String,
        /// Replayed committed items and their effect states.
        replayed: Vec<(String, String)>,
        /// Remaining items and their terminal codes.
        resumed: Vec<(String, String)>,
    },
    /// The requested scope does not belong to this lane's source.
    ScopeMismatch {
        /// Requested scope.
        requested_scope_id: String,
        /// Why.
        reason: String,
    },
    /// The handshake for the step's scope failed.
    HandshakeFailed {
        /// Handshake terminal code.
        terminal_code: String,
    },
    /// The scenario was adjudicated.
    Adjudicated,
}

/// One executed step.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BaselineStepRecord {
    /// One-based step number.
    pub step: u32,
    /// Corpus action.
    pub action: String,
    /// Typed outcome.
    pub outcome: StepOutcome,
    /// Provider calls issued, in order.
    pub provider_calls: Vec<ProviderCallRecord>,
    /// Context admitted, for recall-class steps.
    pub context: Option<ContextAdmission>,
}

/// Cost totals for one scenario.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ScenarioCostSummary {
    /// Recall-class steps.
    pub recall_steps: u64,
    /// Provider calls issued.
    pub provider_calls: u64,
    /// Provider calls that reached provider code.
    pub provider_contacted_calls: u64,
    /// Total bytes admitted into context.
    pub admitted_context_bytes: u64,
    /// Total admitted entries.
    pub admitted_entries: u64,
    /// Total provider response payload bytes.
    pub provider_response_bytes: u64,
    /// Total token estimate.
    pub estimated_tokens: TokenRecord,
}

/// Verdict for one rubric check.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckVerdict {
    /// Evidence satisfied the check.
    Pass,
    /// Evidence contradicted the check.
    Fail,
    /// No mechanical evaluator or insufficient evidence.
    Indeterminate,
}

/// One adjudicated rubric check.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CheckRecord {
    /// Check identity.
    pub check_id: String,
    /// Weight in basis points.
    pub weight_basis_points: u32,
    /// Verdict.
    pub verdict: CheckVerdict,
    /// Evaluator that produced the verdict.
    pub evaluator: String,
    /// Evidence summary.
    pub evidence: String,
}

/// Gate over typed terminal outcomes of outcome-bearing steps.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TerminalGate {
    /// Whether every gated terminal was allowed.
    pub passed: bool,
    /// Gated steps.
    pub gated_steps: u64,
    /// Violations as `step:terminal_code`.
    pub violations: Vec<String>,
}

/// Adjudication of one scenario.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AdjudicationRecord {
    /// Rubric identity.
    pub rubric_id: String,
    /// Rubric version.
    pub version: u64,
    /// Terminal gate.
    pub terminal_gate: TerminalGate,
    /// Check verdicts.
    pub checks: Vec<CheckRecord>,
    /// Sum of passing weights in basis points.
    pub weighted_pass_basis_points: u32,
    /// Indeterminate checks.
    pub indeterminate_checks: u32,
    /// Whether every check passed and the terminal gate held.
    pub safety_gate_passed: bool,
}

/// One scenario's baseline result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ScenarioBaselineResult {
    /// Scenario identity.
    pub scenario_id: String,
    /// Scenario category.
    pub category: String,
    /// Task text.
    pub task: String,
    /// Target scope.
    pub target_scope_id: String,
    /// Executed steps.
    pub steps: Vec<BaselineStepRecord>,
    /// Cost totals.
    pub cost: ScenarioCostSummary,
    /// Adjudication.
    pub adjudication: AdjudicationRecord,
}

/// Complete canonical baseline report.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BaselineReport {
    /// Report format identity.
    pub report_format: String,
    /// Run identity.
    pub identity: BaselineRunIdentity,
    /// Scenario results in corpus order.
    pub scenarios: Vec<ScenarioBaselineResult>,
}

impl BaselineReport {
    /// Serializes the report as canonical UTF-8 JSON bytes.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, BaselineError> {
        Ok(canonical_json(self)?)
    }

    /// Returns one scenario result.
    #[must_use]
    pub fn scenario(&self, scenario_id: &str) -> Option<&ScenarioBaselineResult> {
        self.scenarios
            .iter()
            .find(|scenario| scenario.scenario_id == scenario_id)
    }
}

/// One measured provider call latency.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CallTiming {
    /// Scenario identity.
    pub scenario_id: String,
    /// Step number.
    pub step: u32,
    /// Operation identity.
    pub operation_id: String,
    /// Wall-clock latency in microseconds.
    pub latency_micros: u64,
}

/// Latencies measured during a run; excluded from the canonical report.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct BaselineTimings {
    /// Per-call latencies in execution order.
    pub calls: Vec<CallTiming>,
}

/// Report plus non-canonical timings.
#[derive(Clone, Debug)]
pub struct BaselineRunOutput {
    /// Canonical report.
    pub report: BaselineReport,
    /// Measured timings.
    pub timings: BaselineTimings,
}

/// One lane's cost row in a comparison.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ComparisonCell {
    /// Lane identity.
    pub lane_id: String,
    /// Admitted context bytes.
    pub admitted_context_bytes: u64,
    /// Admitted entries.
    pub admitted_entries: u64,
    /// Provider calls.
    pub provider_calls: u64,
    /// Token estimate.
    pub estimated_tokens: TokenRecord,
    /// Terminal gate result.
    pub terminal_gate_passed: bool,
    /// Safety gate result.
    pub safety_gate_passed: bool,
    /// Weighted pass basis points.
    pub weighted_pass_basis_points: u32,
}

/// One scenario row across lanes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ComparisonRow {
    /// Scenario identity.
    pub scenario_id: String,
    /// Per-lane cells in report order.
    pub lanes: Vec<ComparisonCell>,
}

/// Side-by-side comparison of lanes that share identical inputs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BaselineComparison {
    /// Shared inputs digest every compared report carries.
    pub shared_inputs_sha256: String,
    /// Lane identities in report order.
    pub lane_ids: Vec<String>,
    /// Rows in corpus order.
    pub rows: Vec<ComparisonRow>,
}

impl BaselineComparison {
    /// Compares reports; rejects differing inputs or repeated lanes.
    pub fn compare(reports: &[&BaselineReport]) -> Result<Self, BaselineError> {
        let Some(first) = reports.first() else {
            return Err(BaselineError::ComparisonInputs {
                detail: "at least one report is required".to_owned(),
            });
        };
        let shared = &first.identity.shared_inputs_sha256;
        let mut lane_ids = Vec::new();
        for report in reports {
            if &report.identity.shared_inputs_sha256 != shared {
                return Err(BaselineError::ComparisonInputs {
                    detail: format!(
                        "lane {} shared inputs {} differ from {}",
                        report.identity.lane.lane_id, report.identity.shared_inputs_sha256, shared
                    ),
                });
            }
            if report.identity.shared_inputs != first.identity.shared_inputs {
                return Err(BaselineError::ComparisonInputs {
                    detail: format!(
                        "lane {} shared inputs differ structurally",
                        report.identity.lane.lane_id
                    ),
                });
            }
            if lane_ids.contains(&report.identity.lane.lane_id) {
                return Err(BaselineError::ComparisonInputs {
                    detail: format!("lane {} appears twice", report.identity.lane.lane_id),
                });
            }
            let scenario_ids: Vec<&str> = report
                .scenarios
                .iter()
                .map(|scenario| scenario.scenario_id.as_str())
                .collect();
            let expected: Vec<&str> = first
                .identity
                .shared_inputs
                .scenario_ids
                .iter()
                .map(String::as_str)
                .collect();
            if scenario_ids != expected {
                return Err(BaselineError::ComparisonInputs {
                    detail: format!(
                        "lane {} scenario shape differs",
                        report.identity.lane.lane_id
                    ),
                });
            }
            lane_ids.push(report.identity.lane.lane_id.clone());
        }
        let rows = first
            .identity
            .shared_inputs
            .scenario_ids
            .iter()
            .map(|scenario_id| ComparisonRow {
                scenario_id: scenario_id.clone(),
                lanes: reports
                    .iter()
                    .filter_map(|report| {
                        report.scenario(scenario_id).map(|scenario| ComparisonCell {
                            lane_id: report.identity.lane.lane_id.clone(),
                            admitted_context_bytes: scenario.cost.admitted_context_bytes,
                            admitted_entries: scenario.cost.admitted_entries,
                            provider_calls: scenario.cost.provider_calls,
                            estimated_tokens: scenario.cost.estimated_tokens.clone(),
                            terminal_gate_passed: scenario.adjudication.terminal_gate.passed,
                            safety_gate_passed: scenario.adjudication.safety_gate_passed,
                            weighted_pass_basis_points: scenario
                                .adjudication
                                .weighted_pass_basis_points,
                        })
                    })
                    .collect(),
            })
            .collect();
        Ok(Self {
            shared_inputs_sha256: shared.clone(),
            lane_ids,
            rows,
        })
    }

    /// Serializes the comparison as canonical JSON bytes.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, BaselineError> {
        Ok(canonical_json(self)?)
    }
}

/// Runs baseline lanes over one loaded corpus under one host configuration.
pub struct BaselineRunner<'corpus> {
    corpus: &'corpus ScenarioCorpus,
    config: BaselineRunConfig,
    shared_inputs: SharedInputs,
    shared_inputs_sha256: String,
}

impl<'corpus> BaselineRunner<'corpus> {
    /// Binds a corpus and configuration and precomputes the shared identity.
    pub fn new(
        corpus: &'corpus ScenarioCorpus,
        config: BaselineRunConfig,
    ) -> Result<Self, BaselineError> {
        config.validate()?;
        let shared_inputs = SharedInputs {
            corpus_id: corpus.corpus_id().to_owned(),
            corpus_sha256: corpus.corpus_sha256().to_owned(),
            schema_version: corpus.schema_version(),
            bead_id: corpus.bead_id().to_owned(),
            contract_set_id: CONTRACT_SET_ID.to_owned(),
            contract_set_sha256: CONTRACT_SET_SHA256.to_owned(),
            fixture_ids: corpus
                .fixtures()
                .iter()
                .map(|fixture| fixture.fixture_id.clone())
                .collect(),
            fixture_digests: corpus
                .fixtures()
                .iter()
                .map(|fixture| fixture.fixture_digest.clone())
                .collect(),
            fixture_content_sha256: canonical_json_sha256(&corpus.fixtures())?,
            scope_catalog_sha256: canonical_json_sha256(&corpus.scope_catalog())?,
            recall_catalog_sha256: canonical_json_sha256(&corpus.recall_requests())?,
            scenario_ids: corpus
                .scenarios()
                .iter()
                .map(|scenario| scenario.id.clone())
                .collect(),
            rubric_sha256: canonical_json_sha256(
                &corpus
                    .scenarios()
                    .iter()
                    .map(rubric_identity_value)
                    .collect::<Vec<_>>(),
            )?,
            host_config: HostConfigIdentity {
                deadline_utc_micros: config.deadline_utc_micros,
                remaining_millis: config.remaining_millis,
                host_limits: config.host_limits.into(),
            },
        };
        let shared_inputs_sha256 = canonical_json_sha256(&shared_inputs)?;
        Ok(Self {
            corpus,
            config,
            shared_inputs,
            shared_inputs_sha256,
        })
    }

    /// Returns the shared-inputs digest every lane of this runner will carry.
    #[must_use]
    pub fn shared_inputs_sha256(&self) -> &str {
        &self.shared_inputs_sha256
    }

    /// Computes the complete run identity for a lane without executing it.
    pub fn run_identity(
        &self,
        lane: &BaselineLane<'_>,
    ) -> Result<BaselineRunIdentity, BaselineError> {
        let provider = match lane {
            BaselineLane::Provider(provider_lane) => {
                let descriptor = provider_lane.provider.descriptor();
                descriptor.validate()?;
                Some(ProviderIdentityRecord {
                    provider_id: descriptor.provider_id.as_str().to_owned(),
                    build_identity_sha256: descriptor.implementation_identity_sha256.clone(),
                    state_schema_version: descriptor.state_schema_version.clone(),
                    registration_revision: provider_lane.registration_revision,
                    declared_capabilities: descriptor
                        .capabilities
                        .iter()
                        .map(|capability| capability.as_str().to_owned())
                        .collect(),
                })
            }
            BaselineLane::NoMemory | BaselineLane::ExplicitDocumentation => None,
        };
        let lane_identity = LaneIdentity {
            lane_id: lane.lane_id(),
            kind: lane.kind(),
            provider,
        };
        let token_estimator = match &self.config.token_estimator {
            Some(estimator) => TokenEstimatorIdentity::Pinned {
                estimator_id: estimator.estimator_id().to_owned(),
                estimator_revision: estimator.estimator_revision().to_owned(),
            },
            None => TokenEstimatorIdentity::Indeterminate,
        };
        let run_identity_sha256 = canonical_json_sha256(&json!({
            "shared_inputs": self.shared_inputs,
            "lane": lane_identity,
            "token_estimator": token_estimator,
        }))?;
        Ok(BaselineRunIdentity {
            run_identity_sha256,
            shared_inputs_sha256: self.shared_inputs_sha256.clone(),
            shared_inputs: self.shared_inputs.clone(),
            lane: lane_identity,
            token_estimator,
        })
    }

    /// Runs every scenario through one lane.
    pub fn run(&self, lane: &BaselineLane<'_>) -> Result<BaselineRunOutput, BaselineError> {
        let identity = self.run_identity(lane)?;
        let mut timings = BaselineTimings::default();
        let mut scenarios = Vec::with_capacity(self.corpus.scenarios().len());
        for scenario in self.corpus.scenarios() {
            let mut execution =
                ScenarioExecution::new(self, lane, scenario, &identity.lane.lane_id)?;
            let result = execution.run(&mut timings);
            let close = execution.close();
            let result = result?;
            close?;
            scenarios.push(result);
        }
        Ok(BaselineRunOutput {
            report: BaselineReport {
                report_format: "tracedecay.coding-memory.baseline-report.v1".to_owned(),
                identity,
                scenarios,
            },
            timings,
        })
    }

    fn exact_scope(&self, scope: &ScopeEntry) -> Result<OwnedExactScope, BaselineError> {
        let digest = format!("sha256:{}", canonical_json_sha256(scope)?);
        Ok(OwnedExactScope::new(
            scope.profile_id.clone(),
            scope.project_id.clone(),
            scope.repository_id.clone(),
            scope.worktree_id.clone(),
            scope.branch_ref.clone(),
            scope.agent_session_id.clone(),
            digest,
        )?)
    }

    fn scope(&self, scope_id: &str) -> Result<&'corpus ScopeEntry, BaselineError> {
        self.corpus
            .scope(scope_id)
            .ok_or_else(|| BaselineError::UnknownReference {
                kind: "scope_id",
                value: scope_id.to_owned(),
            })
    }

    fn request(&self, request_id: &str) -> Result<&'corpus RecallRequestDefinition, BaselineError> {
        self.corpus
            .recall_request(request_id)
            .ok_or_else(|| BaselineError::UnknownReference {
                kind: "request_id",
                value: request_id.to_owned(),
            })
    }
}

fn rubric_identity_value(scenario: &ScenarioDefinition) -> Value {
    let rubric = &scenario.adjudication_rubric;
    json!({
        "scenario_id": scenario.id,
        "rubric_id": rubric.rubric_id,
        "version": rubric.version,
        "mode": rubric.mode,
        "pass_threshold_basis_points": rubric.pass_threshold_basis_points,
        "checks": rubric.checks,
        "allowed_terminal_outcomes": scenario.expected_admissible_behavior.allowed_terminal_outcomes,
    })
}

/// Materialized fixture files for one scenario run.
struct FixtureWorkspace {
    root: PathBuf,
    current: BTreeMap<String, (String, String)>, // path -> (revision_id, sha256)
    closed: bool,
}

impl FixtureWorkspace {
    fn create(root: PathBuf, fixture: &FixtureDefinition) -> Result<Self, BaselineError> {
        if root.exists() {
            fs::remove_dir_all(&root).map_err(|error| workspace_error(&root, &error))?;
        }
        fs::create_dir_all(&root).map_err(|error| workspace_error(&root, &error))?;
        let mut workspace = Self {
            root,
            current: BTreeMap::new(),
            closed: false,
        };
        for file in &fixture.files {
            let Some(first) = file.revisions.first() else {
                continue;
            };
            workspace.write(&file.path, first.content.as_bytes())?;
            workspace.current.insert(
                file.path.clone(),
                (first.revision_id.clone(), first.content_sha256.clone()),
            );
        }
        Ok(workspace)
    }

    fn write(&self, relative: &str, bytes: &[u8]) -> Result<(), BaselineError> {
        let target = self.root.join(relative);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(|error| workspace_error(parent, &error))?;
        }
        let staging = self.root.join(format!("{relative}.staging"));
        fs::write(&staging, bytes).map_err(|error| workspace_error(&staging, &error))?;
        fs::rename(&staging, &target).map_err(|error| workspace_error(&target, &error))?;
        Ok(())
    }

    fn advance(
        &mut self,
        path: &str,
        revision_id: &str,
        content: &str,
        content_sha256: &str,
    ) -> Result<(), BaselineError> {
        self.write(path, content.as_bytes())?;
        self.current.insert(
            path.to_owned(),
            (revision_id.to_owned(), content_sha256.to_owned()),
        );
        Ok(())
    }

    fn read_verified(&self, path: &str) -> Result<(String, Vec<u8>), BaselineError> {
        let (revision_id, expected) =
            self.current
                .get(path)
                .ok_or_else(|| BaselineError::UnknownReference {
                    kind: "fixture_path",
                    value: path.to_owned(),
                })?;
        let target = self.root.join(path);
        let bytes = fs::read(&target).map_err(|error| workspace_error(&target, &error))?;
        let actual = lowercase_sha256_hex(Sha256::digest(&bytes).into());
        if &actual != expected {
            return Err(BaselineError::FixtureDrift {
                path: path.to_owned(),
                expected_sha256: expected.clone(),
                actual_sha256: actual,
            });
        }
        Ok((revision_id.clone(), bytes))
    }

    fn documentation_paths(&self) -> Vec<String> {
        self.current
            .keys()
            .filter(|path| is_documentation_path(path))
            .cloned()
            .collect()
    }

    fn close(&mut self) -> Result<(), BaselineError> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        match fs::remove_dir_all(&self.root) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(workspace_error(&self.root, &error)),
        }
    }
}

impl Drop for FixtureWorkspace {
    fn drop(&mut self) {
        if !self.closed {
            // Best-effort cleanup on early exit; `close` reports failures typed.
            let _ = fs::remove_dir_all(&self.root);
        }
    }
}

fn workspace_error(path: &Path, error: &io::Error) -> BaselineError {
    BaselineError::Workspace {
        path: path.display().to_string(),
        detail: error.to_string(),
    }
}

fn is_documentation_path(path: &str) -> bool {
    path.starts_with("docs/")
        || path.starts_with("notes/")
        || matches!(path, "AGENTS.md" | "README.md" | "CLAUDE.md")
}

struct ReadyState {
    ready_receipt: String,
    state_generation: u64,
}

struct ProviderSession<'provider> {
    provider: &'provider dyn MemoryProvider,
    registration_revision: u64,
    initial_descriptor: ProviderDescriptor,
    ready: BTreeMap<String, ReadyState>,
    restart_count: u32,
}

struct BatchState {
    batch_id: String,
    item_count: u64,
    committed: Vec<(String, String)>, // (item_id, operation_id)
    cancelled: Option<BatchBoundary>,
}

struct RecallEvidence {
    request_id: String,
    operation: RecallRequestOperation,
    terminal_code: TerminalCode,
    admitted_entries: usize,
    foreign_entries: usize,
    forgotten_source_hits: usize,
    after_restart: bool,
    after_state_load: bool,
}

#[derive(Default)]
struct ScenarioEvidence {
    recalls: Vec<RecallEvidence>,
    health: Option<TerminalCode>,
    restore: Option<TerminalCode>,
    delete: Option<(String, TerminalCode)>,
    batch: Option<BatchBoundary>,
    replay: Option<(TerminalCode, CommittedEffectState)>,
    restarted: bool,
    state_loaded: bool,
}

struct ScenarioExecution<'run, 'corpus, 'provider> {
    runner: &'run BaselineRunner<'corpus>,
    lane: &'run BaselineLane<'provider>,
    scenario: &'corpus ScenarioDefinition,
    workspace: FixtureWorkspace,
    session: Option<ProviderSession<'provider>>,
    batch: Option<BatchState>,
    evidence: ScenarioEvidence,
    steps: Vec<BaselineStepRecord>,
}

impl<'run, 'corpus, 'provider> ScenarioExecution<'run, 'corpus, 'provider> {
    fn new(
        runner: &'run BaselineRunner<'corpus>,
        lane: &'run BaselineLane<'provider>,
        scenario: &'corpus ScenarioDefinition,
        lane_id: &str,
    ) -> Result<Self, BaselineError> {
        let fixture = runner.corpus.fixture(&scenario.fixture_id).ok_or_else(|| {
            BaselineError::UnknownReference {
                kind: "fixture_id",
                value: scenario.fixture_id.clone(),
            }
        })?;
        let slug = lane_id
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() || character == '_' || character == '-' {
                    character
                } else {
                    '_'
                }
            })
            .collect::<String>();
        let root = runner
            .config
            .fixture_root
            .join(format!("{}--{}--{slug}", fixture.fixture_id, scenario.id));
        let workspace = FixtureWorkspace::create(root, fixture)?;
        let session = match lane {
            BaselineLane::Provider(provider_lane) => {
                let initial_descriptor = provider_lane.provider.descriptor();
                initial_descriptor.validate()?;
                Some(ProviderSession {
                    provider: provider_lane.provider,
                    registration_revision: provider_lane.registration_revision,
                    initial_descriptor,
                    ready: BTreeMap::new(),
                    restart_count: 0,
                })
            }
            BaselineLane::NoMemory | BaselineLane::ExplicitDocumentation => None,
        };
        Ok(Self {
            runner,
            lane,
            scenario,
            workspace,
            session,
            batch: None,
            evidence: ScenarioEvidence::default(),
            steps: Vec::new(),
        })
    }

    fn close(&mut self) -> Result<(), BaselineError> {
        self.workspace.close()
    }

    fn run(
        &mut self,
        timings: &mut BaselineTimings,
    ) -> Result<ScenarioBaselineResult, BaselineError> {
        for step in &self.scenario.steps {
            let record = self.execute_step(step, timings)?;
            self.steps.push(record);
        }
        let cost = self.cost_summary();
        let adjudication = self.adjudicate();
        Ok(ScenarioBaselineResult {
            scenario_id: self.scenario.id.clone(),
            category: self.scenario.category.clone(),
            task: self.scenario.task.clone(),
            target_scope_id: self.scenario.target_scope_id.clone(),
            steps: std::mem::take(&mut self.steps),
            cost,
            adjudication,
        })
    }

    fn execute_step(
        &mut self,
        step: &'corpus ScenarioStep,
        timings: &mut BaselineTimings,
    ) -> Result<BaselineStepRecord, BaselineError> {
        let number = step.step();
        let action = step.action().to_owned();
        let mut calls = Vec::new();
        let mut context = None;
        let outcome = match step {
            ScenarioStep::Observe {
                observation_id,
                operation_id,
                ..
            } => {
                let observation = self.observation(observation_id)?;
                let operation_id = operation_id
                    .clone()
                    .or_else(|| observation.operation_id.clone())
                    .unwrap_or_else(|| {
                        format!("{}.step{number}.observe.{observation_id}", self.scenario.id)
                    });
                self.observe(
                    observation,
                    &operation_id,
                    number,
                    false,
                    &mut calls,
                    timings,
                )?
            }
            ScenarioStep::AdvanceCode { revision_id, .. } => {
                let (path, revision) =
                    self.runner.corpus.revision(revision_id).ok_or_else(|| {
                        BaselineError::UnknownReference {
                            kind: "revision_id",
                            value: revision_id.clone(),
                        }
                    })?;
                self.workspace.advance(
                    path,
                    &revision.revision_id,
                    &revision.content,
                    &revision.content_sha256,
                )?;
                StepOutcome::WorkspaceAdvanced {
                    revision_id: revision.revision_id.clone(),
                    path: path.to_owned(),
                    content_sha256: revision.content_sha256.clone(),
                }
            }
            ScenarioStep::Recall { request_id, .. }
            | ScenarioStep::VerifyAbsence { request_id, .. } => {
                let request = self.runner.request(request_id)?;
                let (outcome, admission) = self.recall(request, number, &mut calls, timings)?;
                context = Some(admission);
                outcome
            }
            ScenarioStep::Health { request_id, .. } => {
                let request = self.runner.request(request_id)?;
                self.health(request, number, &mut calls, timings)?
            }
            ScenarioStep::Adjudicate { .. } => StepOutcome::Adjudicated,
            ScenarioStep::OpenNewAgentSession { scope_id, .. } => {
                self.open_session(scope_id, number, &mut calls, timings)?
            }
            ScenarioStep::RestartProvider { restart_id, .. } => self.restart(restart_id),
            ScenarioStep::Replay {
                observation_id,
                operation_id,
                ..
            } => {
                let observation = self.observation(observation_id)?;
                let outcome =
                    self.observe(observation, operation_id, number, true, &mut calls, timings)?;
                if let Some(call) = calls.last()
                    && let (Some(code), Some(state)) = (
                        TerminalCode::from_wire(&call.terminal_code),
                        CommittedEffectState::from_wire(&call.committed_effect_state),
                    )
                {
                    self.evidence.replay = Some((code, state));
                }
                outcome
            }
            ScenarioStep::BeginObservationBatch { batch_id, .. } => {
                self.begin_batch(batch_id, number)?
            }
            ScenarioStep::CommitItem { item_id, .. } => {
                self.commit_item(item_id, number, &mut calls, timings)?
            }
            ScenarioStep::Cancel { at_item, .. } => {
                self.cancel_batch(*at_item, number, &mut calls, timings)?
            }
            ScenarioStep::Resume { resume_cursor, .. } => {
                self.resume_batch(resume_cursor, number, &mut calls, timings)?
            }
            ScenarioStep::LoadProviderState {
                state_id,
                digest_status,
                ..
            } => self.load_state(state_id, *digest_status, number, &mut calls, timings)?,
            ScenarioStep::DeleteBySource {
                forget_source_key, ..
            } => self.delete_by_source(forget_source_key, number, &mut calls, timings)?,
        };
        Ok(BaselineStepRecord {
            step: number,
            action,
            outcome,
            provider_calls: calls,
            context,
        })
    }

    fn observation(
        &self,
        observation_id: &str,
    ) -> Result<&'corpus ObservationDefinition, BaselineError> {
        self.scenario
            .observation(observation_id)
            .ok_or_else(|| BaselineError::UnknownReference {
                kind: "observation_id",
                value: observation_id.to_owned(),
            })
    }

    fn not_applicable(reason: &str) -> StepOutcome {
        StepOutcome::NotApplicable {
            reason: reason.to_owned(),
        }
    }

    fn observe(
        &mut self,
        observation: &ObservationDefinition,
        operation_id: &str,
        step: u32,
        replay: bool,
        calls: &mut Vec<ProviderCallRecord>,
        timings: &mut BaselineTimings,
    ) -> Result<StepOutcome, BaselineError> {
        if self.session.is_none() {
            return Ok(Self::not_applicable(match self.lane {
                BaselineLane::NoMemory => "memory_disabled",
                _ => "lane_ingests_no_observations",
            }));
        }
        let Some((kind, contract)) = observation_kind(&observation.event_type) else {
            return Ok(StepOutcome::NotAdmissible {
                reason: "observation_kind_unmapped".to_owned(),
                event_type: observation.event_type.clone(),
            });
        };
        let envelope = json!({
            "observation_kind": kind,
            "payload_contract": contract,
            "canonical_payload": {
                "observation_id": observation.observation_id,
                "source_sequence": observation.source_sequence,
                "occurred_at": observation.occurred_at,
                "scope_id": observation.scope_id,
                "source_revision": observation.source_revision,
                "source_digest": observation.source_digest,
                "settlement": observation.settlement,
                "forget_source_key": observation.forget_source_key,
                "privacy_classification": observation.privacy_classification,
                "payload": observation.payload,
            },
        });
        let idempotency_key = observation.idempotency_key.clone().unwrap_or_else(|| {
            format!(
                "{}:{}:{}",
                self.runner.corpus.corpus_id(),
                self.scenario.id,
                observation.observation_id
            )
        });
        let request_id = if replay {
            format!(
                "{}.step{step}.replay.{}",
                self.scenario.id, observation.observation_id
            )
        } else {
            format!("{}.step{step}.request", self.scenario.id)
        };
        let scope_id = observation.scope_id.clone();
        self.dispatch(
            &scope_id,
            ProviderOperation::Observe,
            OBSERVATION_CONTRACT_ID,
            &envelope,
            &request_id,
            operation_id,
            Some(idempotency_key),
            false,
            step,
            calls,
            timings,
        )
    }

    fn recall(
        &mut self,
        request: &RecallRequestDefinition,
        step: u32,
        calls: &mut Vec<ProviderCallRecord>,
        timings: &mut BaselineTimings,
    ) -> Result<(StepOutcome, ContextAdmission), BaselineError> {
        let scope = self.runner.scope(&request.scope_id)?;
        let mut admission = ContextAdmission {
            request_id: request.request_id.clone(),
            scope_id: request.scope_id.clone(),
            terminal_code: TerminalCode::SuccessZeroResults.as_wire().to_owned(),
            candidate_count: CountRecord::Exact { value: 0 },
            admitted_context_bytes: 0,
            entries: Vec::new(),
            estimated_tokens: TokenRecord::Indeterminate,
            provider_call_count: 0,
            provider_response_bytes: 0,
        };
        let mut admitted_bytes: Vec<Vec<u8>> = Vec::new();
        let outcome = match self.lane {
            BaselineLane::NoMemory => StepOutcome::Terminal {
                terminal_code: TerminalCode::SuccessZeroResults.as_wire().to_owned(),
                committed_effect_state: CommittedEffectState::None.as_wire().to_owned(),
            },
            BaselineLane::ExplicitDocumentation => {
                let fixture = self
                    .runner
                    .corpus
                    .fixture(&self.scenario.fixture_id)
                    .ok_or_else(|| BaselineError::UnknownReference {
                        kind: "fixture_id",
                        value: self.scenario.fixture_id.clone(),
                    })?;
                if scope.repository_id != fixture.repository_identity {
                    admission.terminal_code = TerminalCode::ScopeMismatch.as_wire().to_owned();
                    StepOutcome::ScopeMismatch {
                        requested_scope_id: request.scope_id.clone(),
                        reason: format!(
                            "documentation belongs to repository {} not {}",
                            fixture.repository_identity, scope.repository_id
                        ),
                    }
                } else {
                    for path in self.workspace.documentation_paths() {
                        let (revision_id, bytes) = self.workspace.read_verified(&path)?;
                        admission.entries.push(ContextEntry {
                            source_kind: "documentation_file".to_owned(),
                            source_ref: path.clone(),
                            revision_id: Some(revision_id),
                            sha256: lowercase_sha256_hex(Sha256::digest(&bytes).into()),
                            bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                            scope_match: true,
                            contains_forgotten_source: false,
                        });
                        admitted_bytes.push(bytes);
                    }
                    let code = if admission.entries.is_empty() {
                        TerminalCode::SuccessZeroResults
                    } else {
                        TerminalCode::Success
                    };
                    admission.terminal_code = code.as_wire().to_owned();
                    admission.candidate_count = CountRecord::Exact {
                        value: u64::try_from(admission.entries.len()).unwrap_or(u64::MAX),
                    };
                    StepOutcome::Terminal {
                        terminal_code: code.as_wire().to_owned(),
                        committed_effect_state: CommittedEffectState::None.as_wire().to_owned(),
                    }
                }
            }
            BaselineLane::Provider(_) => {
                let outcome = self.provider_recall(
                    request,
                    scope,
                    step,
                    &mut admission,
                    &mut admitted_bytes,
                    calls,
                    timings,
                )?;
                admission.provider_call_count = u64::try_from(calls.len()).unwrap_or(u64::MAX);
                admission.provider_response_bytes =
                    calls.iter().map(|call| call.response_payload_bytes).sum();
                outcome
            }
        };
        if let Some((forgotten_key, _)) = &self.evidence.delete {
            let needle = forgotten_key.as_bytes();
            for (entry, bytes) in admission.entries.iter_mut().zip(&admitted_bytes) {
                entry.contains_forgotten_source = contains_bytes(bytes, needle);
            }
        }
        admission.admitted_context_bytes = admission.entries.iter().map(|entry| entry.bytes).sum();
        if let Some(estimator) = &self.runner.config.token_estimator {
            let mut tokens = 0_u64;
            for bytes in &admitted_bytes {
                let counted = estimator.estimate_tokens(bytes).map_err(|error| {
                    BaselineError::TokenEstimate {
                        request_id: request.request_id.clone(),
                        error,
                    }
                })?;
                tokens = tokens.saturating_add(counted);
            }
            admission.estimated_tokens = TokenRecord::Estimated { tokens };
        }
        let terminal_code = TerminalCode::from_wire(&admission.terminal_code)
            .unwrap_or(TerminalCode::InternalFailure);
        self.evidence.recalls.push(RecallEvidence {
            request_id: request.request_id.clone(),
            operation: request.operation,
            terminal_code,
            admitted_entries: admission.entries.len(),
            foreign_entries: admission
                .entries
                .iter()
                .filter(|entry| !entry.scope_match)
                .count(),
            forgotten_source_hits: admission
                .entries
                .iter()
                .filter(|entry| entry.contains_forgotten_source)
                .count(),
            after_restart: self.evidence.restarted,
            after_state_load: self.evidence.state_loaded,
        });
        Ok((outcome, admission))
    }

    #[allow(clippy::too_many_arguments)]
    fn provider_recall(
        &mut self,
        request: &RecallRequestDefinition,
        scope: &ScopeEntry,
        step: u32,
        admission: &mut ContextAdmission,
        admitted_bytes: &mut Vec<Vec<u8>>,
        calls: &mut Vec<ProviderCallRecord>,
        timings: &mut BaselineTimings,
    ) -> Result<StepOutcome, BaselineError> {
        let (Some(objective), Some(query), Some(budgets), Some(exclusions)) = (
            request.objective.as_ref(),
            request.query.as_ref(),
            request.budgets,
            request.exclusions.as_ref(),
        ) else {
            return Err(BaselineError::UnknownReference {
                kind: "recall_request_fields",
                value: request.request_id.clone(),
            });
        };
        let exact_scope = self.runner.exact_scope(scope)?;
        let ready = match self.ensure_ready(&request.scope_id, step, calls, timings)? {
            Ok(ready) => ready,
            Err(code) => {
                admission.terminal_code = code.as_wire().to_owned();
                admission.candidate_count = CountRecord::Indeterminate {
                    reason: "handshake_failed".to_owned(),
                };
                return Ok(StepOutcome::HandshakeFailed {
                    terminal_code: code.as_wire().to_owned(),
                });
            }
        };
        let provider_id = self.provider_id()?;
        let request_identity = format!("{}.step{step}.request", self.scenario.id);
        let payload = json!({
            "provider_id": provider_id.as_str(),
            "registration_revision": ready.registration_revision,
            "ready_receipt_digest": ready.ready_receipt,
            "exact_scope_identity": {
                "profile_id": exact_scope.profile_id,
                "project_id": exact_scope.project_id,
                "repository_identity": exact_scope.repository_identity,
                "worktree_identity": exact_scope.worktree_identity,
                "branch_identity": exact_scope.branch_identity,
                "agent_session_id": exact_scope.agent_session_id,
                "resolved_scope_digest": exact_scope.resolved_scope_digest,
            },
            "request_identity": request_identity,
            "objective": objective,
            "query": query,
            "temporal_query": {
                "mode": request.temporal_query.mode,
                "evaluation_time": request.temporal_query.evaluation_time,
                "as_of": Value::Null,
                "interval_start": Value::Null,
                "interval_end": Value::Null,
                "include_superseded": false,
                "include_revoked": false,
                "unknown_validity_policy": "exclude",
            },
            "budgets": budgets,
            "exclusions": exclusions,
            "required_capabilities": ["recall.query.v1"],
            "policy_revision": request.policy_revision,
            "extensions": [],
            "deadline": {
                "deadline_utc_micros": self.runner.config.deadline_utc_micros,
                "remaining_millis": self.runner.config.remaining_millis,
            },
            "cancellation": "live",
        });
        let operation_id = format!("{}.step{step}.recall", self.scenario.id);
        let (outcome, reply) = self.dispatch_with_reply(
            &request.scope_id,
            ProviderOperation::Recall,
            RECALL_CONTRACT_ID,
            &payload,
            &request_identity,
            &operation_id,
            None,
            false,
            step,
            calls,
            timings,
        )?;
        let Some(call) = calls.last() else {
            return Ok(outcome);
        };
        admission.terminal_code = call.terminal_code.clone();
        match reply.and_then(|reply| reply.payload) {
            Some(payload) => {
                match serde_json::from_slice::<Value>(&payload.bytes)
                    .ok()
                    .and_then(|value| value.get("candidates").and_then(Value::as_array).cloned())
                {
                    Some(candidates) => {
                        admission.candidate_count = CountRecord::Exact {
                            value: u64::try_from(candidates.len()).unwrap_or(u64::MAX),
                        };
                        for (index, candidate) in candidates.iter().enumerate() {
                            let (entry, bytes) = candidate_entry(candidate, index, &exact_scope);
                            admission.entries.push(entry);
                            admitted_bytes.push(bytes);
                        }
                    }
                    None => {
                        admission.candidate_count = CountRecord::Indeterminate {
                            reason: "payload_is_not_a_recall_outcome_document".to_owned(),
                        };
                    }
                }
            }
            None => {
                admission.candidate_count = if matches!(
                    TerminalCode::from_wire(&call.terminal_code),
                    Some(TerminalCode::SuccessZeroResults)
                ) {
                    CountRecord::Exact { value: 0 }
                } else {
                    CountRecord::Indeterminate {
                        reason: format!("no_payload_for_terminal_{}", call.terminal_code),
                    }
                };
            }
        }
        Ok(outcome)
    }

    fn health(
        &mut self,
        request: &RecallRequestDefinition,
        step: u32,
        calls: &mut Vec<ProviderCallRecord>,
        timings: &mut BaselineTimings,
    ) -> Result<StepOutcome, BaselineError> {
        if self.session.is_none() {
            return Ok(Self::not_applicable("lane_has_no_provider"));
        }
        let request_id = format!("{}.step{step}.request", self.scenario.id);
        let operation_id = format!("{}.step{step}.health", self.scenario.id);
        let outcome = self.dispatch(
            &request.scope_id,
            ProviderOperation::Health,
            HEALTH_CONTRACT_ID,
            &json!({}),
            &request_id,
            &operation_id,
            None,
            false,
            step,
            calls,
            timings,
        )?;
        self.evidence.health = calls
            .last()
            .and_then(|call| TerminalCode::from_wire(&call.terminal_code));
        Ok(outcome)
    }

    fn open_session(
        &mut self,
        scope_id: &str,
        step: u32,
        calls: &mut Vec<ProviderCallRecord>,
        timings: &mut BaselineTimings,
    ) -> Result<StepOutcome, BaselineError> {
        self.runner.scope(scope_id)?;
        if self.session.is_none() {
            return Ok(StepOutcome::SessionOpened {
                scope_id: scope_id.to_owned(),
                handshake_terminal_code: None,
            });
        }
        let handshake = self.ensure_ready(scope_id, step, calls, timings)?;
        Ok(StepOutcome::SessionOpened {
            scope_id: scope_id.to_owned(),
            handshake_terminal_code: Some(match handshake {
                Ok(_) => TerminalCode::Success.as_wire().to_owned(),
                Err(code) => code.as_wire().to_owned(),
            }),
        })
    }

    fn restart(&mut self, restart_id: &str) -> StepOutcome {
        self.evidence.restarted = true;
        let Some(session) = self.session.as_mut() else {
            return Self::not_applicable("lane_has_no_provider");
        };
        session.ready.clear();
        session.restart_count = session.restart_count.saturating_add(1);
        let descriptor = session.provider.descriptor();
        let descriptor_stable = descriptor.validate().is_ok()
            && descriptor.provider_id == session.initial_descriptor.provider_id
            && descriptor.implementation_identity_sha256
                == session.initial_descriptor.implementation_identity_sha256
            && descriptor.state_schema_version == session.initial_descriptor.state_schema_version
            && descriptor.capabilities == session.initial_descriptor.capabilities;
        StepOutcome::ProviderRestarted {
            restart_id: restart_id.to_owned(),
            descriptor_stable,
            state_generation: descriptor.state_generation,
        }
    }

    fn begin_batch(&mut self, batch_id: &str, step: u32) -> Result<StepOutcome, BaselineError> {
        if self.session.is_none() {
            return Ok(Self::not_applicable("lane_has_no_provider_state"));
        }
        if self.batch.is_some() {
            return Err(BaselineError::BatchState {
                scenario_id: self.scenario.id.clone(),
                step,
                detail: "batch already open",
            });
        }
        let request = self
            .scenario
            .observations
            .iter()
            .find(|observation| {
                observation.event_type == "observation_batch_requested"
                    && observation.payload.get("batch_id").and_then(Value::as_str) == Some(batch_id)
            })
            .ok_or_else(|| BaselineError::UnknownReference {
                kind: "batch_id",
                value: batch_id.to_owned(),
            })?;
        let item_count = request
            .payload
            .get("item_count")
            .and_then(Value::as_u64)
            .filter(|count| *count > 0)
            .ok_or_else(|| BaselineError::ObservationPayloadShape {
                observation_id: request.observation_id.clone(),
                field: "item_count",
            })?;
        self.batch = Some(BatchState {
            batch_id: batch_id.to_owned(),
            item_count,
            committed: Vec::new(),
            cancelled: None,
        });
        Ok(StepOutcome::BatchOpened {
            batch_id: batch_id.to_owned(),
            item_count,
        })
    }

    /// Resolves the observation dispatched for one batch item: the corpus
    /// observation pinned to that item when one exists, otherwise an item
    /// derived from the scenario's `observation_batch_requested` template.
    /// Every field of a derived item comes from that template; a scenario
    /// without one is a typed batch-state fault, never a defaulted envelope.
    fn batch_item_observation(
        &self,
        batch_id: &str,
        item_id: &str,
        index: u64,
        step: u32,
    ) -> Result<ObservationDefinition, BaselineError> {
        let existing = self.scenario.observations.iter().find(|observation| {
            observation.event_type == "observation_item_committed"
                && observation.payload.get("item_id").and_then(Value::as_str) == Some(item_id)
        });
        if let Some(observation) = existing {
            return Ok(observation.clone());
        }
        let template = self
            .scenario
            .observations
            .iter()
            .find(|observation| {
                observation.event_type == BATCH_TEMPLATE_EVENT_TYPE
                    && observation.payload.get("batch_id").and_then(Value::as_str)
                        == Some(batch_id)
            })
            .ok_or_else(|| BaselineError::BatchState {
                scenario_id: self.scenario.id.clone(),
                step,
                detail: "batch scenario lacks an observation_batch_requested template for this batch",
            })?;
        Ok(ObservationDefinition {
            observation_id: format!("{batch_id}.{item_id}"),
            source_sequence: index,
            event_type: "observation_item_committed".to_owned(),
            occurred_at: template.occurred_at.clone(),
            scope_id: template.scope_id.clone(),
            source_revision: template.source_revision.clone(),
            source_digest: template.source_digest.clone(),
            settlement: template.settlement.clone(),
            operation_id: None,
            idempotency_key: None,
            forget_source_key: None,
            privacy_classification: None,
            payload: json!({
                "batch_id": batch_id,
                "item_id": item_id,
                "item_index": index,
                "template_observation_id": template.observation_id,
            }),
        })
    }

    fn batch_item_ids(&self, batch: &BatchState) -> Vec<String> {
        let mut ids: Vec<String> = batch
            .committed
            .iter()
            .map(|(item, _)| item.clone())
            .collect();
        let mut index = ids.len() as u64;
        while index < batch.item_count {
            index += 1;
            ids.push(format!("{}.item{index:03}", batch.batch_id));
        }
        ids
    }

    fn commit_item(
        &mut self,
        item_id: &str,
        step: u32,
        calls: &mut Vec<ProviderCallRecord>,
        timings: &mut BaselineTimings,
    ) -> Result<StepOutcome, BaselineError> {
        if self.session.is_none() {
            return Ok(Self::not_applicable("lane_has_no_provider_state"));
        }
        let (batch_id, index) = match &self.batch {
            Some(batch) if batch.cancelled.is_none() => (
                batch.batch_id.clone(),
                u64::try_from(batch.committed.len())
                    .unwrap_or(u64::MAX)
                    .saturating_add(1),
            ),
            _ => {
                return Err(BaselineError::BatchState {
                    scenario_id: self.scenario.id.clone(),
                    step,
                    detail: "commit_item without an open uncancelled batch",
                });
            }
        };
        let observation = self.batch_item_observation(&batch_id, item_id, index, step)?;
        let operation_id = format!("{}.{batch_id}.{item_id}", self.scenario.id);
        let outcome = self.observe(&observation, &operation_id, step, false, calls, timings)?;
        if let Some(batch) = self.batch.as_mut() {
            batch.committed.push((item_id.to_owned(), operation_id));
        }
        Ok(outcome)
    }

    fn cancel_batch(
        &mut self,
        at_item: u32,
        step: u32,
        calls: &mut Vec<ProviderCallRecord>,
        timings: &mut BaselineTimings,
    ) -> Result<StepOutcome, BaselineError> {
        if self.session.is_none() {
            return Ok(Self::not_applicable("lane_has_no_provider_state"));
        }
        let Some(batch) = self.batch.as_ref() else {
            return Err(BaselineError::BatchState {
                scenario_id: self.scenario.id.clone(),
                step,
                detail: "cancel without an open batch",
            });
        };
        let batch_id = batch.batch_id.clone();
        let item_count = batch.item_count;
        let item_ids = self.batch_item_ids(batch);
        let committed_count = batch.committed.len();
        let cancel_index = usize::try_from(at_item)
            .unwrap_or(usize::MAX)
            .saturating_sub(1);
        // The item the cancellation lands on is materialized with a cancelled
        // token so the host control preflight refuses dispatch exactly as a
        // production host would; items after it are never materialized.
        let mut terminal_code = TerminalCode::Cancelled;
        if cancel_index >= committed_count
            && let Some(item_id) = item_ids.get(cancel_index)
        {
            let observation =
                self.batch_item_observation(&batch_id, item_id, cancel_index as u64 + 1, step)?;
            let operation_id = format!("{}.{batch_id}.{item_id}", self.scenario.id);
            let Some((kind, contract)) = observation_kind(&observation.event_type) else {
                return Err(BaselineError::ObservationPayloadShape {
                    observation_id: observation.observation_id.clone(),
                    field: "event_type",
                });
            };
            let envelope = json!({
                "observation_kind": kind,
                "payload_contract": contract,
                "canonical_payload": observation.payload,
            });
            let idempotency_key = format!(
                "{}:{}:{}",
                self.runner.corpus.corpus_id(),
                self.scenario.id,
                observation.observation_id
            );
            let request_id = format!("{}.step{step}.request", self.scenario.id);
            let scope_id = observation.scope_id.clone();
            self.dispatch(
                &scope_id,
                ProviderOperation::Observe,
                OBSERVATION_CONTRACT_ID,
                &envelope,
                &request_id,
                &operation_id,
                Some(idempotency_key),
                true,
                step,
                calls,
                timings,
            )?;
            if let Some(call) = calls.last()
                && let Some(code) = TerminalCode::from_wire(&call.terminal_code)
            {
                terminal_code = code;
            }
        }
        let boundary = BatchBoundary {
            batch_id: batch_id.clone(),
            terminal_code: terminal_code.as_wire().to_owned(),
            committed_item_ids: item_ids.iter().take(committed_count).cloned().collect(),
            uncommitted_item_ids: item_ids.iter().skip(committed_count).cloned().collect(),
            item_count,
        };
        if let Some(batch) = self.batch.as_mut() {
            batch.cancelled = Some(boundary.clone());
        }
        self.evidence.batch = Some(boundary.clone());
        Ok(StepOutcome::BatchCancelled { boundary })
    }

    fn resume_batch(
        &mut self,
        resume_cursor: &str,
        step: u32,
        calls: &mut Vec<ProviderCallRecord>,
        timings: &mut BaselineTimings,
    ) -> Result<StepOutcome, BaselineError> {
        if self.session.is_none() {
            return Ok(Self::not_applicable("lane_has_no_provider_state"));
        }
        let (batch_id, committed, uncommitted) = match &self.batch {
            Some(BatchState {
                batch_id,
                committed,
                cancelled: Some(boundary),
                ..
            }) => (
                batch_id.clone(),
                committed.clone(),
                boundary.uncommitted_item_ids.clone(),
            ),
            _ => {
                return Err(BaselineError::BatchState {
                    scenario_id: self.scenario.id.clone(),
                    step,
                    detail: "resume without a cancelled batch",
                });
            }
        };
        // Each resumed item must leave exactly one new call record; a resume
        // that recorded nothing is a runner fault, not an empty terminal.
        let recorded_call = |calls: &[ProviderCallRecord], before: usize| {
            if calls.len() == before + 1 {
                calls.last().cloned()
            } else {
                None
            }
        };
        let mut replayed = Vec::new();
        for (index, (item_id, operation_id)) in committed.iter().enumerate() {
            let observation =
                self.batch_item_observation(&batch_id, item_id, index as u64 + 1, step)?;
            let before = calls.len();
            self.observe(&observation, operation_id, step, true, calls, timings)?;
            let call = recorded_call(calls, before).ok_or_else(|| BaselineError::BatchState {
                scenario_id: self.scenario.id.clone(),
                step,
                detail: "replayed batch item recorded no provider call",
            })?;
            replayed.push((item_id.clone(), call.committed_effect_state));
        }
        let mut resumed = Vec::new();
        for (offset, item_id) in uncommitted.iter().enumerate() {
            let index = committed.len() + offset + 1;
            let observation =
                self.batch_item_observation(&batch_id, item_id, index as u64, step)?;
            let operation_id = format!("{}.{batch_id}.{item_id}", self.scenario.id);
            let before = calls.len();
            self.observe(&observation, &operation_id, step, false, calls, timings)?;
            let call = recorded_call(calls, before).ok_or_else(|| BaselineError::BatchState {
                scenario_id: self.scenario.id.clone(),
                step,
                detail: "resumed batch item recorded no provider call",
            })?;
            resumed.push((item_id.clone(), call.terminal_code));
        }
        Ok(StepOutcome::BatchResumed {
            resume_cursor: resume_cursor.to_owned(),
            replayed,
            resumed,
        })
    }

    fn load_state(
        &mut self,
        state_id: &str,
        digest_status: DigestStatus,
        step: u32,
        calls: &mut Vec<ProviderCallRecord>,
        timings: &mut BaselineTimings,
    ) -> Result<StepOutcome, BaselineError> {
        if self.session.is_none() {
            return Ok(Self::not_applicable("lane_has_no_provider_state"));
        }
        // Only a lane with provider state actually loads one; later recalls
        // in lanes without state carry no post-load evidence.
        self.evidence.state_loaded = true;
        let check = self
            .scenario
            .observations
            .iter()
            .find(|observation| {
                observation.event_type == "provider_state_checked"
                    && observation.payload.get("state_id").and_then(Value::as_str) == Some(state_id)
            })
            .ok_or_else(|| BaselineError::UnknownReference {
                kind: "state_id",
                value: state_id.to_owned(),
            })?;
        let payload = json!({
            "state_id": state_id,
            "digest_status": match digest_status {
                DigestStatus::Match => "match",
                DigestStatus::Mismatch => "mismatch",
            },
            "expected_digest": check.payload.get("expected_digest").cloned().unwrap_or(Value::Null),
            "actual_digest": check.payload.get("actual_digest").cloned().unwrap_or(Value::Null),
        });
        let request_id = format!("{}.step{step}.request", self.scenario.id);
        let operation_id = format!("{}.step{step}.snapshot_restore", self.scenario.id);
        let idempotency_key = format!(
            "{}:{}:{state_id}",
            self.runner.corpus.corpus_id(),
            self.scenario.id
        );
        let scope_id = check.scope_id.clone();
        let outcome = self.dispatch(
            &scope_id,
            ProviderOperation::SnapshotRestore,
            SNAPSHOT_RESTORE_CONTRACT_ID,
            &payload,
            &request_id,
            &operation_id,
            Some(idempotency_key),
            false,
            step,
            calls,
            timings,
        )?;
        self.evidence.restore = calls
            .last()
            .and_then(|call| TerminalCode::from_wire(&call.terminal_code));
        Ok(outcome)
    }

    fn delete_by_source(
        &mut self,
        forget_source_key: &str,
        step: u32,
        calls: &mut Vec<ProviderCallRecord>,
        timings: &mut BaselineTimings,
    ) -> Result<StepOutcome, BaselineError> {
        if self.session.is_none() {
            return Ok(Self::not_applicable("lane_has_no_provider_state"));
        }
        let payload = json!({
            "forget_source_key": forget_source_key,
            "include_snapshots": true,
            "mode": "hard_delete",
        });
        let request_id = format!("{}.step{step}.request", self.scenario.id);
        let operation_id = format!("{}.step{step}.delete_by_source", self.scenario.id);
        let idempotency_key = format!(
            "{}:{}:{forget_source_key}",
            self.runner.corpus.corpus_id(),
            self.scenario.id
        );
        let scope_id = self.scenario.target_scope_id.clone();
        let outcome = self.dispatch(
            &scope_id,
            ProviderOperation::DeleteBySource,
            DELETE_BY_SOURCE_CONTRACT_ID,
            &payload,
            &request_id,
            &operation_id,
            Some(idempotency_key),
            false,
            step,
            calls,
            timings,
        )?;
        if let Some(code) = calls
            .last()
            .and_then(|call| TerminalCode::from_wire(&call.terminal_code))
        {
            self.evidence.delete = Some((forget_source_key.to_owned(), code));
        }
        Ok(outcome)
    }

    fn provider_id(&self) -> Result<OwnedProviderId, BaselineError> {
        self.session
            .as_ref()
            .map(|session| session.initial_descriptor.provider_id.clone())
            .ok_or(BaselineError::InvalidConfig("lane has no provider"))
    }

    /// Ensures a ready receipt for one scope; `Err(code)` carries a failed handshake terminal.
    fn ensure_ready(
        &mut self,
        scope_id: &str,
        step: u32,
        calls: &mut Vec<ProviderCallRecord>,
        timings: &mut BaselineTimings,
    ) -> Result<Result<ReadySnapshot, TerminalCode>, BaselineError> {
        let scope = self.runner.scope(scope_id)?;
        let exact_scope = self.runner.exact_scope(scope)?;
        let Some(session) = self.session.as_mut() else {
            return Err(BaselineError::InvalidConfig("lane has no provider"));
        };
        if let Some(ready) = session.ready.get(scope_id) {
            return Ok(Ok(ReadySnapshot {
                ready_receipt: ready.ready_receipt.clone(),
                state_generation: ready.state_generation,
                registration_revision: session.registration_revision,
            }));
        }
        let request_id = format!(
            "{}.{scope_id}.handshake{}",
            self.scenario.id, session.restart_count
        );
        let challenge_nonce: [u8; 32] = Sha256::digest(request_id.as_bytes()).into();
        let control = OperationControl::new(
            self.runner.config.deadline_utc_micros,
            self.runner.config.remaining_millis,
            CancellationToken::new(),
        );
        let request = HandshakeRequest::new(HandshakeRequestParts {
            provider_id: session.initial_descriptor.provider_id.clone(),
            registration_revision: session.registration_revision,
            exact_scope: exact_scope.clone(),
            request_id: request_id.clone(),
            required_capabilities: mandatory_capabilities()?,
            host_limits: self.runner.config.host_limits,
            control,
            challenge_nonce,
        })?;
        let started = Instant::now();
        let (response, contacted) = match request.control.snapshot() {
            Ok(_) => (session.provider.handshake(&request), true),
            Err(code) => {
                return Ok(Err(code));
            }
        };
        timings.calls.push(CallTiming {
            scenario_id: self.scenario.id.clone(),
            step,
            operation_id: request_id.clone(),
            latency_micros: u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
        });
        let terminal_code = response.terminal.terminal_code();
        let state_generation = response
            .descriptor
            .as_ref()
            .map_or(session.initial_descriptor.state_generation, |descriptor| {
                descriptor.state_generation
            });
        calls.push(ProviderCallRecord {
            operation: ProviderOperation::Handshake.as_wire().to_owned(),
            request_id: request_id.clone(),
            operation_id: request_id.clone(),
            exact_scope_sha256: exact_scope.exact_scope_sha256(),
            terminal_code: terminal_code.as_wire().to_owned(),
            committed_effect_state: response
                .terminal
                .committed_effect()
                .state()
                .as_wire()
                .to_owned(),
            diagnostic_id: response.terminal.diagnostic_id().map(str::to_owned),
            provider_contacted: contacted,
            state_generation_before: session.initial_descriptor.state_generation,
            state_generation_after: state_generation,
            request_payload_bytes: 0,
            response_payload_bytes: 0,
        });
        let accepted = terminal_code == TerminalCode::Success
            && response.accepted_scope.as_ref() == Some(&exact_scope);
        match response.ready_receipt_sha256.filter(|_| accepted) {
            Some(ready_receipt) => {
                session.ready.insert(
                    scope_id.to_owned(),
                    ReadyState {
                        ready_receipt: ready_receipt.clone(),
                        state_generation,
                    },
                );
                Ok(Ok(ReadySnapshot {
                    ready_receipt,
                    state_generation,
                    registration_revision: session.registration_revision,
                }))
            }
            None => Ok(Err(if terminal_code == TerminalCode::Success {
                TerminalCode::ContractViolation
            } else {
                terminal_code
            })),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn dispatch(
        &mut self,
        scope_id: &str,
        operation: ProviderOperation,
        contract_id: &str,
        payload: &Value,
        request_id: &str,
        operation_id: &str,
        idempotency_key: Option<String>,
        cancel_before_dispatch: bool,
        step: u32,
        calls: &mut Vec<ProviderCallRecord>,
        timings: &mut BaselineTimings,
    ) -> Result<StepOutcome, BaselineError> {
        let (outcome, _) = self.dispatch_with_reply(
            scope_id,
            operation,
            contract_id,
            payload,
            request_id,
            operation_id,
            idempotency_key,
            cancel_before_dispatch,
            step,
            calls,
            timings,
        )?;
        Ok(outcome)
    }

    #[allow(clippy::too_many_arguments)]
    fn dispatch_with_reply(
        &mut self,
        scope_id: &str,
        operation: ProviderOperation,
        contract_id: &str,
        payload: &Value,
        request_id: &str,
        operation_id: &str,
        idempotency_key: Option<String>,
        cancel_before_dispatch: bool,
        step: u32,
        calls: &mut Vec<ProviderCallRecord>,
        timings: &mut BaselineTimings,
    ) -> Result<(StepOutcome, Option<ProviderReply>), BaselineError> {
        let ready = match self.ensure_ready(scope_id, step, calls, timings)? {
            Ok(ready) => ready,
            Err(code) => {
                return Ok((
                    StepOutcome::HandshakeFailed {
                        terminal_code: code.as_wire().to_owned(),
                    },
                    None,
                ));
            }
        };
        let scope = self.runner.scope(scope_id)?;
        let exact_scope = self.runner.exact_scope(scope)?;
        let bytes = canonical_json(payload)?;
        let sha256 = lowercase_sha256_hex(Sha256::digest(&bytes).into());
        let request_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let canonical_payload =
            CanonicalPayload::new(OwnedVersionedId::new(contract_id)?, bytes, sha256)?;
        let Some(session) = self.session.as_mut() else {
            return Err(BaselineError::InvalidConfig("lane has no provider"));
        };
        let capability = operation.capability_id();
        if !session.initial_descriptor.supports(capability) {
            calls.push(ProviderCallRecord {
                operation: operation.as_wire().to_owned(),
                request_id: request_id.to_owned(),
                operation_id: operation_id.to_owned(),
                exact_scope_sha256: exact_scope.exact_scope_sha256(),
                terminal_code: TerminalCode::CapabilityUnsupported.as_wire().to_owned(),
                committed_effect_state: CommittedEffectState::None.as_wire().to_owned(),
                diagnostic_id: Some("host.capability_undeclared".to_owned()),
                provider_contacted: false,
                state_generation_before: ready.state_generation,
                state_generation_after: ready.state_generation,
                request_payload_bytes: request_bytes,
                response_payload_bytes: 0,
            });
            return Ok((
                StepOutcome::Terminal {
                    terminal_code: TerminalCode::CapabilityUnsupported.as_wire().to_owned(),
                    committed_effect_state: CommittedEffectState::None.as_wire().to_owned(),
                },
                None,
            ));
        }
        let cancellation = CancellationToken::new();
        if cancel_before_dispatch {
            cancellation.cancel();
        }
        let control = OperationControl::new(
            self.runner.config.deadline_utc_micros,
            self.runner.config.remaining_millis,
            cancellation,
        );
        let call = admitted(ProviderCall::new(ProviderCallParts {
            operation,
            provider_id: session.initial_descriptor.provider_id.clone(),
            registration_revision: session.registration_revision,
            ready_receipt_sha256: ready.ready_receipt.clone(),
            exact_scope: exact_scope.clone(),
            request_id: request_id.to_owned(),
            operation_id: operation_id.to_owned(),
            expected_state_generation: ready.state_generation,
            idempotency_key,
            control,
            payload: canonical_payload,
            required_capabilities: vec![OwnedVersionedId::new(capability)?],
            extensions: Vec::new(),
        })?)?;
        let started = Instant::now();
        let (
            terminal_code,
            effect_state,
            diagnostic_id,
            state_generation_after,
            response_bytes,
            contacted,
            reply,
        ) = match call.control.snapshot() {
            Ok(_) => {
                let reply = session.provider.invoke(&call);
                timings.calls.push(CallTiming {
                    scenario_id: self.scenario.id.clone(),
                    step,
                    operation_id: operation_id.to_owned(),
                    latency_micros: u64::try_from(started.elapsed().as_micros())
                        .unwrap_or(u64::MAX),
                });
                let code = reply.terminal.terminal_code();
                let state = reply.terminal.committed_effect().state();
                let diagnostic = reply.terminal.diagnostic_id().map(str::to_owned);
                let after = if matches!(
                    code,
                    TerminalCode::Success
                        | TerminalCode::SuccessZeroResults
                        | TerminalCode::Partial
                ) && reply.state_generation >= ready.state_generation
                {
                    reply.state_generation
                } else {
                    ready.state_generation
                };
                let response_bytes = reply.payload.as_ref().map_or(0, |payload| {
                    u64::try_from(payload.bytes.len()).unwrap_or(u64::MAX)
                });
                (
                    code,
                    state,
                    diagnostic,
                    after,
                    response_bytes,
                    true,
                    Some(reply),
                )
            }
            Err(code) => (
                code,
                CommittedEffectState::None,
                Some(format!("host.control.{}", code.as_wire())),
                ready.state_generation,
                0,
                false,
                None,
            ),
        };
        if let Some(entry) = session.ready.get_mut(scope_id) {
            entry.state_generation = state_generation_after;
        }
        calls.push(ProviderCallRecord {
            operation: operation.as_wire().to_owned(),
            request_id: request_id.to_owned(),
            operation_id: operation_id.to_owned(),
            exact_scope_sha256: exact_scope.exact_scope_sha256(),
            terminal_code: terminal_code.as_wire().to_owned(),
            committed_effect_state: effect_state.as_wire().to_owned(),
            diagnostic_id,
            provider_contacted: contacted,
            state_generation_before: ready.state_generation,
            state_generation_after,
            request_payload_bytes: request_bytes,
            response_payload_bytes: response_bytes,
        });
        Ok((
            StepOutcome::Terminal {
                terminal_code: terminal_code.as_wire().to_owned(),
                committed_effect_state: effect_state.as_wire().to_owned(),
            },
            reply,
        ))
    }

    fn cost_summary(&self) -> ScenarioCostSummary {
        let mut summary = ScenarioCostSummary {
            recall_steps: 0,
            provider_calls: 0,
            provider_contacted_calls: 0,
            admitted_context_bytes: 0,
            admitted_entries: 0,
            provider_response_bytes: 0,
            estimated_tokens: if self.runner.config.token_estimator.is_some() {
                TokenRecord::Estimated { tokens: 0 }
            } else {
                TokenRecord::Indeterminate
            },
        };
        for step in &self.steps {
            summary.provider_calls = summary
                .provider_calls
                .saturating_add(u64::try_from(step.provider_calls.len()).unwrap_or(u64::MAX));
            summary.provider_contacted_calls = summary.provider_contacted_calls.saturating_add(
                u64::try_from(
                    step.provider_calls
                        .iter()
                        .filter(|call| call.provider_contacted)
                        .count(),
                )
                .unwrap_or(u64::MAX),
            );
            summary.provider_response_bytes = summary.provider_response_bytes.saturating_add(
                step.provider_calls
                    .iter()
                    .map(|call| call.response_payload_bytes)
                    .sum(),
            );
            if let Some(context) = &step.context {
                summary.recall_steps = summary.recall_steps.saturating_add(1);
                summary.admitted_context_bytes = summary
                    .admitted_context_bytes
                    .saturating_add(context.admitted_context_bytes);
                summary.admitted_entries = summary
                    .admitted_entries
                    .saturating_add(u64::try_from(context.entries.len()).unwrap_or(u64::MAX));
                if let (
                    TokenRecord::Estimated { tokens },
                    TokenRecord::Estimated { tokens: total },
                ) = (&context.estimated_tokens, &mut summary.estimated_tokens)
                {
                    *total = total.saturating_add(*tokens);
                }
            }
        }
        summary
    }

    fn adjudicate(&self) -> AdjudicationRecord {
        let rubric = &self.scenario.adjudication_rubric;
        let allowed: BTreeSet<&str> = self
            .scenario
            .expected_admissible_behavior
            .allowed_terminal_outcomes
            .iter()
            .map(String::as_str)
            .collect();
        let mut gated_steps = 0_u64;
        let mut violations = Vec::new();
        for step in &self.steps {
            let gated = matches!(
                step.action.as_str(),
                "recall"
                    | "verify_absence"
                    | "health"
                    | "cancel"
                    | "delete_by_source"
                    | "load_provider_state"
            );
            if !gated {
                continue;
            }
            let terminal = match &step.outcome {
                StepOutcome::Terminal { terminal_code, .. } => Some(terminal_code.as_str()),
                StepOutcome::BatchCancelled { boundary } => Some(boundary.terminal_code.as_str()),
                StepOutcome::ScopeMismatch { .. } => Some(TerminalCode::ScopeMismatch.as_wire()),
                StepOutcome::HandshakeFailed { terminal_code } => Some(terminal_code.as_str()),
                _ => None,
            };
            if let Some(terminal) = terminal {
                gated_steps = gated_steps.saturating_add(1);
                if !allowed.contains(terminal) {
                    violations.push(format!("{}:{terminal}", step.step));
                }
            }
        }
        let terminal_gate = TerminalGate {
            passed: violations.is_empty(),
            gated_steps,
            violations,
        };
        let mut checks = Vec::with_capacity(rubric.checks.len());
        let mut weighted = 0_u32;
        let mut indeterminate = 0_u32;
        let mut all_pass = true;
        for check in &rubric.checks {
            let (verdict, evaluator, evidence) = self.evaluate_check(&check.check_id);
            match verdict {
                CheckVerdict::Pass => weighted = weighted.saturating_add(check.weight_basis_points),
                CheckVerdict::Fail => all_pass = false,
                CheckVerdict::Indeterminate => {
                    all_pass = false;
                    indeterminate = indeterminate.saturating_add(1);
                }
            }
            checks.push(CheckRecord {
                check_id: check.check_id.clone(),
                weight_basis_points: check.weight_basis_points,
                verdict,
                evaluator: evaluator.to_owned(),
                evidence,
            });
        }
        AdjudicationRecord {
            rubric_id: rubric.rubric_id.clone(),
            version: rubric.version,
            safety_gate_passed: all_pass && terminal_gate.passed,
            terminal_gate,
            checks,
            weighted_pass_basis_points: weighted,
            indeterminate_checks: indeterminate,
        }
    }

    fn evaluate_check(&self, check_id: &str) -> (CheckVerdict, &'static str, String) {
        let evidence = &self.evidence;
        let recalls = &evidence.recalls;
        // Scope and corruption checks are earned from admitted entries. With
        // nothing admitted there is nothing to inspect, so the verdict is
        // typed indeterminate and accrues no basis points: a lane that admits
        // no context (no memory, an empty store) must not score as isolated.
        let scope_isolation = || {
            let foreign: usize = recalls.iter().map(|recall| recall.foreign_entries).sum();
            let admitted: usize = recalls.iter().map(|recall| recall.admitted_entries).sum();
            if foreign > 0 {
                (
                    CheckVerdict::Fail,
                    "admitted_context_scope",
                    format!("{foreign} of {admitted} admitted entries carry a foreign exact scope"),
                )
            } else if admitted == 0 {
                (
                    CheckVerdict::Indeterminate,
                    EVALUATOR_VACUOUS_ZERO_ADMISSION,
                    format!(
                        "{} recall-class steps admitted no entries; scope isolation has no evidence",
                        recalls.len()
                    ),
                )
            } else {
                (
                    CheckVerdict::Pass,
                    "admitted_context_scope",
                    format!("{admitted} admitted entries, all at the request exact scope"),
                )
            }
        };
        let recall_after = |predicate: &dyn Fn(&RecallEvidence) -> bool| {
            recalls.iter().rev().find(|recall| predicate(recall))
        };
        match check_id {
            "scope_exact" | "exact_scope_match" | "scope_preserved" | "sibling_isolation" => {
                scope_isolation()
            }
            "project_isolation" => {
                let other = recall_after(&|recall| recall.request_id == "request_scope_002");
                match other {
                    Some(recall) if recall.foreign_entries > 0 => (
                        CheckVerdict::Fail,
                        "other_project_request",
                        format!(
                            "request_scope_002 admitted {} foreign entries",
                            recall.foreign_entries
                        ),
                    ),
                    // A typed scope_mismatch is scope-aware rejection of the
                    // other project's request: real evidence, not emptiness.
                    Some(recall) if recall.terminal_code == TerminalCode::ScopeMismatch => (
                        CheckVerdict::Pass,
                        "other_project_request",
                        "request_scope_002 refused with typed scope_mismatch, 0 entries admitted"
                            .to_owned(),
                    ),
                    Some(recall) if recall.admitted_entries > 0 => (
                        CheckVerdict::Pass,
                        "other_project_request",
                        format!(
                            "request_scope_002 terminal {} admitted {} entries, none foreign",
                            recall.terminal_code.as_wire(),
                            recall.admitted_entries
                        ),
                    ),
                    Some(recall) => (
                        CheckVerdict::Indeterminate,
                        EVALUATOR_VACUOUS_ZERO_ADMISSION,
                        format!(
                            "request_scope_002 terminal {} admitted no entries; rejection is not evidenced",
                            recall.terminal_code.as_wire()
                        ),
                    ),
                    None => (
                        CheckVerdict::Indeterminate,
                        "other_project_request",
                        "request_scope_002 was not issued".to_owned(),
                    ),
                }
            }
            "terminal_is_cancelled" => match &evidence.batch {
                Some(boundary) if boundary.terminal_code == TerminalCode::Cancelled.as_wire() => (
                    CheckVerdict::Pass,
                    "batch_boundary_terminal",
                    format!("batch {} terminal cancelled", boundary.batch_id),
                ),
                Some(boundary) => (
                    CheckVerdict::Fail,
                    "batch_boundary_terminal",
                    format!(
                        "batch {} terminal {}",
                        boundary.batch_id, boundary.terminal_code
                    ),
                ),
                None => (
                    CheckVerdict::Indeterminate,
                    "batch_boundary_terminal",
                    "no batch boundary recorded".to_owned(),
                ),
            },
            "effect_boundary" => match &evidence.batch {
                Some(boundary)
                    if boundary.committed_item_ids.len() + boundary.uncommitted_item_ids.len()
                        == usize::try_from(boundary.item_count).unwrap_or(usize::MAX) =>
                {
                    (
                        CheckVerdict::Pass,
                        "batch_boundary_items",
                        format!(
                            "{} committed, {} uncommitted of {}",
                            boundary.committed_item_ids.len(),
                            boundary.uncommitted_item_ids.len(),
                            boundary.item_count
                        ),
                    )
                }
                Some(_) => (
                    CheckVerdict::Fail,
                    "batch_boundary_items",
                    "boundary does not account for every item".to_owned(),
                ),
                None => (
                    CheckVerdict::Indeterminate,
                    "batch_boundary_items",
                    "no batch boundary recorded".to_owned(),
                ),
            },
            "no_corrupt_recall" => match recall_after(&|recall| recall.after_state_load) {
                Some(recall) if recall.admitted_entries > 0 => (
                    CheckVerdict::Fail,
                    "post_state_load_admission",
                    format!(
                        "{} entries admitted after state load",
                        recall.admitted_entries
                    ),
                ),
                // An empty success cannot distinguish a refused corrupt
                // partition from a store that simply held nothing.
                Some(recall)
                    if matches!(
                        recall.terminal_code,
                        TerminalCode::Success | TerminalCode::SuccessZeroResults
                    ) =>
                {
                    (
                        CheckVerdict::Indeterminate,
                        EVALUATOR_VACUOUS_ZERO_ADMISSION,
                        format!(
                            "terminal {} admitted no entries after state load; refusal is not evidenced",
                            recall.terminal_code.as_wire()
                        ),
                    )
                }
                Some(recall) => (
                    CheckVerdict::Pass,
                    "post_state_load_admission",
                    format!(
                        "typed terminal {} admitted 0 entries after state load",
                        recall.terminal_code.as_wire()
                    ),
                ),
                None => (
                    CheckVerdict::Indeterminate,
                    "post_state_load_admission",
                    "no recall after state load".to_owned(),
                ),
            },
            "corruption_visible" => match (evidence.health, evidence.restore) {
                (Some(health), _)
                    if matches!(
                        health,
                        TerminalCode::StateIncompatible | TerminalCode::ProviderUnavailable
                    ) =>
                {
                    (
                        CheckVerdict::Pass,
                        "health_or_restore_terminal",
                        format!("health terminal {}", health.as_wire()),
                    )
                }
                (_, Some(TerminalCode::StateIncompatible)) => (
                    CheckVerdict::Pass,
                    "health_or_restore_terminal",
                    "state restore terminal state_incompatible".to_owned(),
                ),
                (Some(health), Some(restore)) => (
                    CheckVerdict::Fail,
                    "health_or_restore_terminal",
                    format!(
                        "health {} and restore {} did not surface corruption",
                        health.as_wire(),
                        restore.as_wire()
                    ),
                ),
                _ => (
                    CheckVerdict::Indeterminate,
                    "health_or_restore_terminal",
                    "no provider health or restore terminal recorded".to_owned(),
                ),
            },
            "verified_absence" => {
                let verify = recall_after(&|recall| {
                    recall.operation == RecallRequestOperation::VerifyAbsence
                });
                match (verify, &evidence.delete) {
                    (Some(recall), Some(_)) if recall.forgotten_source_hits > 0 => (
                        CheckVerdict::Fail,
                        "forgotten_source_in_admitted_context",
                        format!(
                            "{} of {} admitted entries still carry the forgotten source",
                            recall.forgotten_source_hits, recall.admitted_entries
                        ),
                    ),
                    (Some(recall), Some((_, TerminalCode::Success))) => (
                        CheckVerdict::Pass,
                        "forgotten_source_in_admitted_context",
                        format!(
                            "deletion succeeded and none of {} admitted entries carry the forgotten source",
                            recall.admitted_entries
                        ),
                    ),
                    (Some(_), Some((_, code))) => (
                        CheckVerdict::Indeterminate,
                        "forgotten_source_in_admitted_context",
                        format!("deletion terminal {}", code.as_wire()),
                    ),
                    _ => (
                        CheckVerdict::Indeterminate,
                        "forgotten_source_in_admitted_context",
                        "no deletion performed by this lane".to_owned(),
                    ),
                }
            }
            "restart_persistence" => match (
                recall_after(&|recall| recall.after_restart),
                &evidence.delete,
            ) {
                (Some(recall), Some(_)) if recall.forgotten_source_hits > 0 => (
                    CheckVerdict::Fail,
                    "forgotten_source_after_restart",
                    format!(
                        "{} admitted entries carry the forgotten source after restart",
                        recall.forgotten_source_hits
                    ),
                ),
                (Some(recall), Some((_, TerminalCode::Success))) => (
                    CheckVerdict::Pass,
                    "forgotten_source_after_restart",
                    format!(
                        "none of {} admitted entries carry the forgotten source after restart",
                        recall.admitted_entries
                    ),
                ),
                _ => (
                    CheckVerdict::Indeterminate,
                    "forgotten_source_after_restart",
                    "no successful deletion preceded the restart".to_owned(),
                ),
            },
            "state_survives" => match recall_after(&|recall| recall.after_restart) {
                Some(recall) if recall.admitted_entries > 0 => (
                    CheckVerdict::Pass,
                    "post_restart_admission",
                    format!("{} entries admitted after restart", recall.admitted_entries),
                ),
                Some(recall) => (
                    CheckVerdict::Fail,
                    "post_restart_admission",
                    format!(
                        "0 entries admitted after restart (terminal {})",
                        recall.terminal_code.as_wire()
                    ),
                ),
                None => (
                    CheckVerdict::Indeterminate,
                    "post_restart_admission",
                    "no recall after restart".to_owned(),
                ),
            },
            "replay_idempotent" => match evidence.replay {
                Some((_, CommittedEffectState::Duplicate)) => (
                    CheckVerdict::Pass,
                    "replay_effect_state",
                    "replay acknowledged as duplicate".to_owned(),
                ),
                Some((TerminalCode::Success, state)) => (
                    CheckVerdict::Fail,
                    "replay_effect_state",
                    format!("replay produced effect state {}", state.as_wire()),
                ),
                Some((code, _)) => (
                    CheckVerdict::Indeterminate,
                    "replay_effect_state",
                    format!("replay terminal {}", code.as_wire()),
                ),
                None => (
                    CheckVerdict::Indeterminate,
                    "replay_effect_state",
                    "no replay performed by this lane".to_owned(),
                ),
            },
            "reuse_is_available" => match recalls.last() {
                Some(recall) if recall.admitted_entries > 0 => (
                    CheckVerdict::Pass,
                    "consumer_session_admission",
                    format!(
                        "{} entries admitted to the consumer session",
                        recall.admitted_entries
                    ),
                ),
                Some(recall) => (
                    CheckVerdict::Fail,
                    "consumer_session_admission",
                    format!(
                        "0 entries admitted to the consumer session (terminal {})",
                        recall.terminal_code.as_wire()
                    ),
                ),
                None => (
                    CheckVerdict::Indeterminate,
                    "consumer_session_admission",
                    "no recall issued".to_owned(),
                ),
            },
            "exact_source_target" => match &evidence.delete {
                Some((key, code)) if code != &TerminalCode::InvalidRequest => (
                    CheckVerdict::Pass,
                    "delete_source_key",
                    format!(
                        "deletion addressed exactly {key} (terminal {})",
                        code.as_wire()
                    ),
                ),
                Some((key, code)) => (
                    CheckVerdict::Fail,
                    "delete_source_key",
                    format!("deletion of {key} rejected as {}", code.as_wire()),
                ),
                None => (
                    CheckVerdict::Indeterminate,
                    "delete_source_key",
                    "no deletion performed by this lane".to_owned(),
                ),
            },
            _ => (
                CheckVerdict::Indeterminate,
                EVALUATOR_NONE_PINNED,
                "requires context-compiler adjudication; no mechanical evaluator pinned".to_owned(),
            ),
        }
    }
}

struct ReadySnapshot {
    ready_receipt: String,
    state_generation: u64,
    registration_revision: u64,
}

fn observation_kind(event_type: &str) -> Option<(&'static str, &'static str)> {
    match event_type {
        "source_edit_settled" | "observation_item_committed" => Some((
            "source.edit_settled.v1",
            "tracedecay.memory.observation.source-edit.v1",
        )),
        "test_evidence_settled" => Some((
            "test.execution_settled.v1",
            "tracedecay.memory.observation.test-execution.v1",
        )),
        "task_outcome_settled" => Some((
            "feedback.outcome_settled.v1",
            "tracedecay.memory.observation.feedback-outcome.v1",
        )),
        _ => None,
    }
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
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

/// Applies the candidate's declared `scope_binding` rules from the recall
/// contract's `candidate_scope_binding.binding_rules`: required fields must
/// equal the request scope, optional fields may be empty or equal, forbidden
/// fields must be empty. A missing or unknown binding never matches.
fn candidate_scope_matches(scope: &Map<String, Value>, request_scope: &OwnedExactScope) -> bool {
    let binding = scope
        .get("scope_binding")
        .and_then(Value::as_str)
        .and_then(RecallScopeBinding::from_wire);
    let Some(binding) = binding else {
        return false;
    };
    let claimed = |field: &str| scope.get(field).and_then(Value::as_str);
    let required =
        |field: &str, expected: &str| !expected.is_empty() && claimed(field) == Some(expected);
    let optional = |field: &str, expected: &str| matches!(claimed(field), Some(value) if value.is_empty() || value == expected);
    let forbidden = |field: &str| claimed(field) == Some("");
    let profile = request_scope.profile_id.as_str();
    let project = request_scope.project_id.as_str();
    let repository = request_scope.repository_identity.as_str();
    let worktree = request_scope.worktree_identity.as_str();
    let branch = request_scope.branch_identity.as_str();
    let session = request_scope.agent_session_id.as_str();
    match binding {
        RecallScopeBinding::ExactCodingScope => {
            required("profile_id", profile)
                && required("project_id", project)
                && required("repository_identity", repository)
                && required("worktree_identity", worktree)
                && required("branch_identity", branch)
                && required("agent_session_id", session)
        }
        RecallScopeBinding::ProjectFacts => {
            required("profile_id", profile)
                && required("project_id", project)
                && optional("repository_identity", repository)
                && optional("worktree_identity", worktree)
                && optional("branch_identity", branch)
                && forbidden("agent_session_id")
                && forbidden("resolved_scope_digest")
        }
        RecallScopeBinding::ProfileFacts => {
            required("profile_id", profile)
                && forbidden("project_id")
                && forbidden("repository_identity")
                && forbidden("worktree_identity")
                && forbidden("branch_identity")
                && forbidden("agent_session_id")
                && forbidden("resolved_scope_digest")
        }
    }
}

fn candidate_entry(
    candidate: &Value,
    index: usize,
    request_scope: &OwnedExactScope,
) -> (ContextEntry, Vec<u8>) {
    let empty = Map::new();
    let object = candidate.as_object().unwrap_or(&empty);
    let content = object
        .get("content")
        .and_then(Value::as_str)
        .map(str::as_bytes)
        .unwrap_or_default();
    let sha256 = object
        .get("content_sha256")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| lowercase_sha256_hex(Sha256::digest(content).into()));
    let scope_match = object
        .get("exact_scope_identity")
        .and_then(Value::as_object)
        .is_some_and(|scope| candidate_scope_matches(scope, request_scope));
    let entry = ContextEntry {
        source_kind: "provider_candidate".to_owned(),
        source_ref: object
            .get("candidate_id")
            .and_then(Value::as_str)
            .map_or_else(|| format!("candidate[{index}]"), str::to_owned),
        revision_id: None,
        sha256,
        bytes: u64::try_from(content.len()).unwrap_or(u64::MAX),
        scope_match,
        contains_forgotten_source: false,
    };
    (entry, content.to_vec())
}
