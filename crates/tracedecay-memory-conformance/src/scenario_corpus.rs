//! Typed loader for the deterministic coding-memory scenario corpus.
//!
//! The corpus is a versioned, provider-neutral JSON document under
//! `product/evaluation/`. Loading verifies every digest and cross-reference
//! the corpus promises so a baseline run can never start from an
//! under-specified scenario: every `recall`, `verify_absence`, and `health`
//! step must resolve to exactly one recall-request catalog entry, every
//! revision and scope must exist, and every rubric must weigh to one.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tracedecay_memory_provider_api::contract::TerminalCode;

use crate::canonical::lowercase_sha256_hex;

/// Rubric weights are stored as basis points so reports carry no floats.
pub const RUBRIC_WEIGHT_BASIS_POINTS: u32 = 10_000;

/// Corpus load or validation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CorpusError {
    /// The document was not valid JSON for the corpus schema.
    Json(String),
    /// The corpus declared an unsupported schema version.
    UnsupportedSchemaVersion(u64),
    /// A fixture digest did not match its fixture identity.
    FixtureDigestMismatch {
        /// Fixture whose digest differed.
        fixture_id: String,
    },
    /// A fixture file revision's bytes did not hash to its declared digest.
    RevisionDigestMismatch {
        /// Revision whose content differed.
        revision_id: String,
    },
    /// Fixture file revisions were not numbered contiguously from one.
    RevisionOrder {
        /// Path whose revisions were misnumbered.
        path: String,
    },
    /// An identifier was declared more than once.
    DuplicateIdentifier {
        /// Identifier kind.
        kind: &'static str,
        /// Duplicated value.
        value: String,
    },
    /// A reference named something the corpus does not define.
    UnknownReference {
        /// Reference kind.
        kind: &'static str,
        /// Missing value.
        value: String,
        /// Scenario that made the reference, when applicable.
        scenario_id: String,
    },
    /// Scenario steps were not numbered contiguously from one.
    StepOrder {
        /// Scenario whose steps were misnumbered.
        scenario_id: String,
    },
    /// A scenario did not end with its own adjudication step.
    MissingAdjudication {
        /// Scenario missing the final adjudication.
        scenario_id: String,
    },
    /// A recall-request catalog entry did not agree with the step using it.
    RecallRequestMismatch {
        /// Request identity in disagreement.
        request_id: String,
        /// Field that disagreed.
        field: &'static str,
    },
    /// A recall-request catalog entry was referenced other than exactly once.
    RecallRequestReferenceCount {
        /// Request identity.
        request_id: String,
        /// Observed references.
        references: usize,
    },
    /// A rubric's weights did not sum to one.
    RubricWeightSum {
        /// Rubric identity.
        rubric_id: String,
        /// Observed sum in basis points.
        basis_points: u32,
    },
    /// A rubric weight could not be represented exactly in basis points.
    RubricWeightPrecision {
        /// Rubric identity.
        rubric_id: String,
        /// Check whose weight was imprecise.
        check_id: String,
    },
    /// An allowed terminal outcome was not a canonical wire value.
    UnknownTerminalOutcome {
        /// Scenario naming the outcome.
        scenario_id: String,
        /// Non-canonical value.
        value: String,
    },
    /// A scenario opened an observation batch without an
    /// `observation_batch_requested` observation for that batch, which is the
    /// only source of occurrence, scope, revision, and digest for its items.
    MissingBatchTemplate {
        /// Scenario opening the batch.
        scenario_id: String,
        /// Batch identity lacking a template.
        batch_id: String,
    },
}

impl fmt::Display for CorpusError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(detail) => write!(formatter, "scenario corpus is not valid JSON: {detail}"),
            Self::UnsupportedSchemaVersion(version) => {
                write!(
                    formatter,
                    "scenario corpus schema version {version} is unsupported"
                )
            }
            Self::FixtureDigestMismatch { fixture_id } => {
                write!(
                    formatter,
                    "fixture {fixture_id} digest does not match its identity"
                )
            }
            Self::RevisionDigestMismatch { revision_id } => {
                write!(
                    formatter,
                    "revision {revision_id} content does not match its digest"
                )
            }
            Self::RevisionOrder { path } => {
                write!(
                    formatter,
                    "fixture file {path} revisions are not contiguous from 1"
                )
            }
            Self::DuplicateIdentifier { kind, value } => {
                write!(formatter, "{kind} {value} is declared more than once")
            }
            Self::UnknownReference {
                kind,
                value,
                scenario_id,
            } => write!(
                formatter,
                "scenario {scenario_id} references unknown {kind} {value}"
            ),
            Self::StepOrder { scenario_id } => {
                write!(
                    formatter,
                    "scenario {scenario_id} steps are not contiguous from 1"
                )
            }
            Self::MissingAdjudication { scenario_id } => write!(
                formatter,
                "scenario {scenario_id} does not end with its own adjudication step"
            ),
            Self::RecallRequestMismatch { request_id, field } => write!(
                formatter,
                "recall request {request_id} disagrees with its step on {field}"
            ),
            Self::RecallRequestReferenceCount {
                request_id,
                references,
            } => write!(
                formatter,
                "recall request {request_id} is referenced {references} times instead of once"
            ),
            Self::RubricWeightSum {
                rubric_id,
                basis_points,
            } => write!(
                formatter,
                "rubric {rubric_id} weights sum to {basis_points} basis points instead of {RUBRIC_WEIGHT_BASIS_POINTS}"
            ),
            Self::RubricWeightPrecision {
                rubric_id,
                check_id,
            } => write!(
                formatter,
                "rubric {rubric_id} check {check_id} weight is not an exact basis-point value"
            ),
            Self::UnknownTerminalOutcome { scenario_id, value } => write!(
                formatter,
                "scenario {scenario_id} allows non-canonical terminal outcome {value}"
            ),
            Self::MissingBatchTemplate {
                scenario_id,
                batch_id,
            } => write!(
                formatter,
                "scenario {scenario_id} opens batch {batch_id} without an observation_batch_requested observation"
            ),
        }
    }
}

impl Error for CorpusError {}

/// One exact coding scope from the corpus scope catalog.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeEntry {
    /// Stable scope identity used by scenarios.
    pub scope_id: String,
    /// Profile authority identity.
    pub profile_id: String,
    /// Project authority identity.
    pub project_id: String,
    /// Repository authority identity.
    pub repository_id: String,
    /// Exact linked-worktree identity.
    pub worktree_id: String,
    /// Exact branch reference.
    pub branch_ref: String,
    /// Exact coding-agent session identity.
    pub agent_session_id: String,
    /// Scope revision.
    pub scope_revision: u64,
}

/// One versioned revision of a fixture file.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FileRevision {
    /// Stable revision identity.
    pub revision_id: String,
    /// One-based revision number within its file.
    pub revision: u64,
    /// Exact UTF-8 file content.
    pub content: String,
    /// Lowercase SHA-256 of the content bytes.
    pub content_sha256: String,
}

/// One fixture file with its ordered revisions.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureFile {
    /// Repository-relative POSIX path.
    pub path: String,
    /// Ordered revisions starting at revision 1.
    pub revisions: Vec<FileRevision>,
}

/// One synthetic repository fixture.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureDefinition {
    /// Stable fixture identity.
    pub fixture_id: String,
    /// Repository identity the fixture stands for.
    pub repository_identity: String,
    /// SHA-256 of the fixture identity.
    pub fixture_digest: String,
    /// Fixture root kind.
    pub root_kind: String,
    /// Files with versioned content.
    pub files: Vec<FixtureFile>,
}

/// Operation a recall-request catalog entry drives.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecallRequestOperation {
    /// Advisory recall.
    Recall,
    /// Recall that must admit nothing.
    VerifyAbsence,
    /// Provider health.
    Health,
}

impl RecallRequestOperation {
    /// Returns the canonical corpus action name.
    #[must_use]
    pub const fn as_action(self) -> &'static str {
        match self {
            Self::Recall => "recall",
            Self::VerifyAbsence => "verify_absence",
            Self::Health => "health",
        }
    }
}

/// Temporal window of one recall request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecallTemporalQuery {
    /// Temporal mode; the corpus pins `current`.
    pub mode: String,
    /// Fixed RFC 3339 evaluation instant.
    pub evaluation_time: String,
}

/// Finite recall budgets.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecallBudgets {
    /// Maximum candidates.
    pub maximum_candidates: u64,
    /// Maximum bytes per candidate.
    pub maximum_candidate_content_bytes: u64,
    /// Maximum total content bytes.
    pub maximum_total_content_bytes: u64,
    /// Maximum source references per candidate.
    pub maximum_source_refs_per_candidate: u64,
    /// Maximum trace references per candidate.
    pub maximum_trace_refs_per_candidate: u64,
    /// Maximum warnings.
    pub maximum_warnings: u64,
    /// Maximum extensions per candidate.
    pub maximum_extensions_per_candidate: u64,
}

/// Explicit recall exclusions.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecallExclusions {
    /// Excluded stable memory references.
    pub stable_memory_refs: Vec<String>,
    /// Excluded candidate identities.
    pub candidate_ids: Vec<String>,
    /// Excluded source references.
    pub source_refs: Vec<String>,
    /// Excluded trace references.
    pub trace_refs: Vec<String>,
    /// Excluded observation identities.
    pub observation_ids: Vec<String>,
    /// Excluded content digests.
    pub content_sha256: Vec<String>,
}

/// One fully specified recall, verify-absence, or health request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecallRequestDefinition {
    /// Stable request identity referenced by exactly one step.
    pub request_id: String,
    /// Scenario that owns the request.
    pub scenario_id: String,
    /// Operation the request drives.
    pub operation: RecallRequestOperation,
    /// Exact scope the request is issued for.
    pub scope_id: String,
    /// Recall objective; absent for health.
    #[serde(default)]
    pub objective: Option<String>,
    /// Recall query; absent for health.
    #[serde(default)]
    pub query: Option<String>,
    /// Temporal window.
    pub temporal_query: RecallTemporalQuery,
    /// Finite budgets; absent for health.
    #[serde(default)]
    pub budgets: Option<RecallBudgets>,
    /// Explicit exclusions; absent for health.
    #[serde(default)]
    pub exclusions: Option<RecallExclusions>,
    /// Recall policy revision.
    pub policy_revision: u64,
}

/// Digest verification result a corrupt-state step simulates.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DigestStatus {
    /// State bytes match their digest.
    Match,
    /// State bytes do not match their digest.
    Mismatch,
}

/// One deterministic scenario step.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScenarioStep {
    /// Deliver one settled observation.
    Observe {
        /// One-based step number.
        step: u32,
        /// Observation to deliver.
        observation_id: String,
        /// Fixed operation identity when the corpus pins one.
        #[serde(default)]
        operation_id: Option<String>,
    },
    /// Move one fixture file to a later revision.
    AdvanceCode {
        /// One-based step number.
        step: u32,
        /// Revision to materialize.
        revision_id: String,
    },
    /// Issue one catalogued recall.
    Recall {
        /// One-based step number.
        step: u32,
        /// Catalogued request.
        request_id: String,
        /// Explicit request scope when it differs from the target scope.
        #[serde(default)]
        scope_id: Option<String>,
    },
    /// Adjudicate the scenario.
    Adjudicate {
        /// One-based step number.
        step: u32,
        /// Rubric identity, always the scenario identity.
        rubric: String,
    },
    /// Open a new agent session at another scope.
    OpenNewAgentSession {
        /// One-based step number.
        step: u32,
        /// Scope of the new session.
        scope_id: String,
    },
    /// Restart the provider.
    RestartProvider {
        /// One-based step number.
        step: u32,
        /// Restart identity.
        restart_id: String,
    },
    /// Replay one already-delivered observation.
    Replay {
        /// One-based step number.
        step: u32,
        /// Observation to replay.
        observation_id: String,
        /// Original operation identity.
        operation_id: String,
    },
    /// Begin one observation batch.
    BeginObservationBatch {
        /// One-based step number.
        step: u32,
        /// Batch identity.
        batch_id: String,
    },
    /// Commit one batch item.
    CommitItem {
        /// One-based step number.
        step: u32,
        /// Item identity.
        item_id: String,
    },
    /// Cancel the open batch before one item.
    Cancel {
        /// One-based step number.
        step: u32,
        /// Cancellation identity.
        cancel_id: String,
        /// One-based item index at which cancellation lands.
        at_item: u32,
    },
    /// Resume the cancelled batch from a cursor.
    Resume {
        /// One-based step number.
        step: u32,
        /// Resume cursor identity.
        resume_cursor: String,
    },
    /// Load provider state with a known digest status.
    LoadProviderState {
        /// One-based step number.
        step: u32,
        /// State identity.
        state_id: String,
        /// Digest verification result.
        digest_status: DigestStatus,
    },
    /// Issue one catalogued health request.
    Health {
        /// One-based step number.
        step: u32,
        /// Catalogued request.
        request_id: String,
    },
    /// Delete provider memory by admitted source key.
    DeleteBySource {
        /// One-based step number.
        step: u32,
        /// Exact source key to forget.
        forget_source_key: String,
    },
    /// Verify that a catalogued recall admits nothing.
    VerifyAbsence {
        /// One-based step number.
        step: u32,
        /// Catalogued request.
        request_id: String,
    },
}

impl ScenarioStep {
    /// Returns the one-based step number.
    #[must_use]
    pub const fn step(&self) -> u32 {
        match self {
            Self::Observe { step, .. }
            | Self::AdvanceCode { step, .. }
            | Self::Recall { step, .. }
            | Self::Adjudicate { step, .. }
            | Self::OpenNewAgentSession { step, .. }
            | Self::RestartProvider { step, .. }
            | Self::Replay { step, .. }
            | Self::BeginObservationBatch { step, .. }
            | Self::CommitItem { step, .. }
            | Self::Cancel { step, .. }
            | Self::Resume { step, .. }
            | Self::LoadProviderState { step, .. }
            | Self::Health { step, .. }
            | Self::DeleteBySource { step, .. }
            | Self::VerifyAbsence { step, .. } => *step,
        }
    }

    /// Returns the canonical corpus action name.
    #[must_use]
    pub const fn action(&self) -> &'static str {
        match self {
            Self::Observe { .. } => "observe",
            Self::AdvanceCode { .. } => "advance_code",
            Self::Recall { .. } => "recall",
            Self::Adjudicate { .. } => "adjudicate",
            Self::OpenNewAgentSession { .. } => "open_new_agent_session",
            Self::RestartProvider { .. } => "restart_provider",
            Self::Replay { .. } => "replay",
            Self::BeginObservationBatch { .. } => "begin_observation_batch",
            Self::CommitItem { .. } => "commit_item",
            Self::Cancel { .. } => "cancel",
            Self::Resume { .. } => "resume",
            Self::LoadProviderState { .. } => "load_provider_state",
            Self::Health { .. } => "health",
            Self::DeleteBySource { .. } => "delete_by_source",
            Self::VerifyAbsence { .. } => "verify_absence",
        }
    }

    /// Returns the catalogued request identity for request-bearing steps.
    #[must_use]
    pub fn request_id(&self) -> Option<(&str, RecallRequestOperation)> {
        match self {
            Self::Recall { request_id, .. } => Some((request_id, RecallRequestOperation::Recall)),
            Self::VerifyAbsence { request_id, .. } => {
                Some((request_id, RecallRequestOperation::VerifyAbsence))
            }
            Self::Health { request_id, .. } => Some((request_id, RecallRequestOperation::Health)),
            _ => None,
        }
    }
}

/// One settled source observation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationDefinition {
    /// Stable observation identity.
    pub observation_id: String,
    /// Per-scope source sequence.
    pub source_sequence: u64,
    /// Corpus event type.
    pub event_type: String,
    /// Fixed RFC 3339 occurrence instant.
    pub occurred_at: String,
    /// Scope the observation belongs to.
    pub scope_id: String,
    /// Fixture revision the observation is bound to.
    pub source_revision: String,
    /// Digest of the bound source.
    pub source_digest: String,
    /// Settlement state.
    pub settlement: String,
    /// Fixed operation identity when the corpus pins one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    /// Fixed idempotency key when the corpus pins one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Source key a later deletion may address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forget_source_key: Option<String>,
    /// Privacy classification when the corpus pins one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub privacy_classification: Option<String>,
    /// Synthetic payload.
    pub payload: Value,
}

/// One piece of evidence attached to a code revision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceDefinition {
    /// Stable evidence identity.
    pub evidence_id: String,
    /// Evidence kind.
    pub kind: String,
    /// Evidence status.
    pub status: String,
    /// Assertion the evidence supports.
    pub assertion: String,
    /// Evidence digest.
    pub digest: String,
}

/// One code or evidence revision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CodeEvidenceRevision {
    /// Fixture revision identity.
    pub revision_id: String,
    /// Revision number.
    pub revision: u64,
    /// Parent revision, if any.
    pub parent_revision_id: Option<String>,
    /// Scope of the revision.
    pub scope_id: String,
    /// Files changed by the revision.
    pub changed_files: Vec<String>,
    /// Attached evidence.
    pub evidence: Vec<EvidenceDefinition>,
}

/// What may enter final context and which terminal outcomes are permitted.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectedAdmissibleBehavior {
    /// Admission summary.
    pub admission: String,
    /// Authority order when documented.
    #[serde(default)]
    pub authority_order: Option<Vec<String>>,
    /// Required behaviors.
    pub must: Vec<String>,
    /// Forbidden behaviors.
    pub must_not: Vec<String>,
    /// Permitted typed terminal outcomes for outcome-bearing steps.
    pub allowed_terminal_outcomes: Vec<String>,
    /// Provider-effect boundary.
    pub provider_effect: String,
}

/// One weighted rubric check.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RubricCheck {
    /// Stable check identity.
    pub check_id: String,
    /// Weight in basis points of the whole rubric.
    #[serde(deserialize_with = "deserialize_basis_points", rename = "weight")]
    pub weight_basis_points: u32,
    /// Human pass criterion.
    pub pass_if: String,
    /// Human fail criterion.
    pub fail_if: String,
}

fn deserialize_basis_points<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let weight = f64::deserialize(deserializer)?;
    basis_points(weight).ok_or_else(|| {
        serde::de::Error::custom(format!(
            "rubric weight {weight} is not an exact basis-point value"
        ))
    })
}

/// Converts a rubric weight in `[0, 1]` to exact basis points.
#[must_use]
pub fn basis_points(weight: f64) -> Option<u32> {
    if !(0.0..=1.0).contains(&weight) {
        return None;
    }
    let scaled = weight * f64::from(RUBRIC_WEIGHT_BASIS_POINTS);
    let rounded = scaled.round();
    if (scaled - rounded).abs() > 1e-6 {
        return None;
    }
    // `rounded` is within [0, 10_000] here, so the cast is exact.
    Some(rounded as u32)
}

/// Weighted adjudication rubric.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdjudicationRubric {
    /// Rubric identity, always the scenario identity.
    pub rubric_id: String,
    /// Rubric version.
    pub version: u64,
    /// Rubric mode.
    pub mode: String,
    /// Pass threshold; the corpus pins one.
    #[serde(
        deserialize_with = "deserialize_basis_points",
        rename = "pass_threshold"
    )]
    pub pass_threshold_basis_points: u32,
    /// Weighted checks.
    pub checks: Vec<RubricCheck>,
}

/// One deterministic scenario.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioDefinition {
    /// Stable scenario identity.
    pub id: String,
    /// Scenario category.
    pub category: String,
    /// Human title.
    pub title: String,
    /// Fixture the scenario runs against.
    pub fixture_id: String,
    /// Task the agent is asked to complete.
    pub task: String,
    /// Scope producing the observations.
    pub source_scope_id: String,
    /// Scope receiving the final answer.
    pub target_scope_id: String,
    /// Ordered steps.
    pub steps: Vec<ScenarioStep>,
    /// Settled observations.
    pub observations: Vec<ObservationDefinition>,
    /// Code and evidence revisions.
    pub code_evidence_revisions: Vec<CodeEvidenceRevision>,
    /// Admission expectations.
    pub expected_admissible_behavior: ExpectedAdmissibleBehavior,
    /// Adjudication rubric.
    pub adjudication_rubric: AdjudicationRubric,
}

impl ScenarioDefinition {
    /// Returns one observation by identity.
    #[must_use]
    pub fn observation(&self, observation_id: &str) -> Option<&ObservationDefinition> {
        self.observations
            .iter()
            .find(|observation| observation.observation_id == observation_id)
    }
}

/// Runner-supplied provider selection policy.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderSelectionPolicy {
    /// Selection mode.
    pub mode: String,
    /// Whether every provider sees the same fixture and task.
    pub same_fixture_and_task_for_each_provider: bool,
    /// Whether provider identity is run metadata only.
    pub provider_identity_is_run_metadata: bool,
    /// Whether provider output is advisory.
    pub provider_output_is_advisory: bool,
    /// Whether observer output participates in adjudication.
    pub observer_output_participates_in_adjudication: bool,
}

/// Fixture execution policy.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FixturePolicy {
    /// Clock policy.
    pub clock: String,
    /// Randomness policy.
    pub randomness: String,
    /// Network policy.
    pub network: String,
    /// Filesystem policy.
    pub filesystem: String,
    /// External process policy.
    pub external_processes: String,
    /// Credential policy.
    pub credentials: String,
    /// Source material policy.
    pub source_material: String,
}

/// Adjudication policy.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdjudicationPolicy {
    /// Score range.
    pub score_range: Vec<u64>,
    /// Safety gate rule.
    pub safety_gate: String,
    /// Indeterminate handling.
    pub indeterminate_policy: String,
    /// Provider failure handling.
    pub provider_failure_policy: String,
    /// Missing evidence handling.
    pub missing_evidence_policy: String,
    /// Owner of final context assembly.
    pub context_owner: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CorpusDocument {
    schema_version: u64,
    corpus_id: String,
    bead_id: String,
    title: String,
    canonical_encoding: String,
    provider_neutral: bool,
    provider_selection: ProviderSelectionPolicy,
    fixture_policy: FixturePolicy,
    adjudication_policy: AdjudicationPolicy,
    scope_catalog: Vec<ScopeEntry>,
    fixtures: Vec<FixtureDefinition>,
    recall_requests: Vec<RecallRequestDefinition>,
    scenarios: Vec<ScenarioDefinition>,
}

/// Validated, digest-bound scenario corpus.
#[derive(Clone, Debug)]
pub struct ScenarioCorpus {
    corpus_sha256: String,
    schema_version: u64,
    corpus_id: String,
    bead_id: String,
    title: String,
    canonical_encoding: String,
    provider_neutral: bool,
    provider_selection: ProviderSelectionPolicy,
    fixture_policy: FixturePolicy,
    adjudication_policy: AdjudicationPolicy,
    scope_catalog: Vec<ScopeEntry>,
    fixtures: Vec<FixtureDefinition>,
    recall_requests: Vec<RecallRequestDefinition>,
    scenarios: Vec<ScenarioDefinition>,
    revision_paths: BTreeMap<String, String>,
}

impl ScenarioCorpus {
    /// Parses and fully validates corpus bytes.
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self, CorpusError> {
        let document: CorpusDocument =
            serde_json::from_slice(bytes).map_err(|error| CorpusError::Json(error.to_string()))?;
        if document.schema_version != 1 {
            return Err(CorpusError::UnsupportedSchemaVersion(
                document.schema_version,
            ));
        }
        let corpus = Self {
            corpus_sha256: lowercase_sha256_hex(Sha256::digest(bytes).into()),
            schema_version: document.schema_version,
            corpus_id: document.corpus_id,
            bead_id: document.bead_id,
            title: document.title,
            canonical_encoding: document.canonical_encoding,
            provider_neutral: document.provider_neutral,
            provider_selection: document.provider_selection,
            fixture_policy: document.fixture_policy,
            adjudication_policy: document.adjudication_policy,
            scope_catalog: document.scope_catalog,
            fixtures: document.fixtures,
            recall_requests: document.recall_requests,
            scenarios: document.scenarios,
            revision_paths: BTreeMap::new(),
        };
        corpus.validated()
    }

    fn validated(mut self) -> Result<Self, CorpusError> {
        let mut scope_ids = BTreeSet::new();
        for scope in &self.scope_catalog {
            if !scope_ids.insert(scope.scope_id.clone()) {
                return Err(CorpusError::DuplicateIdentifier {
                    kind: "scope_id",
                    value: scope.scope_id.clone(),
                });
            }
        }

        let mut fixture_ids = BTreeSet::new();
        let mut revision_paths = BTreeMap::new();
        for fixture in &self.fixtures {
            if !fixture_ids.insert(fixture.fixture_id.clone()) {
                return Err(CorpusError::DuplicateIdentifier {
                    kind: "fixture_id",
                    value: fixture.fixture_id.clone(),
                });
            }
            let expected_digest =
                lowercase_sha256_hex(Sha256::digest(fixture.fixture_id.as_bytes()).into());
            if fixture.fixture_digest != expected_digest {
                return Err(CorpusError::FixtureDigestMismatch {
                    fixture_id: fixture.fixture_id.clone(),
                });
            }
            let mut paths = BTreeSet::new();
            for file in &fixture.files {
                if !paths.insert(file.path.clone()) {
                    return Err(CorpusError::DuplicateIdentifier {
                        kind: "fixture_path",
                        value: file.path.clone(),
                    });
                }
                for (index, revision) in file.revisions.iter().enumerate() {
                    let expected_number =
                        u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1);
                    if revision.revision != expected_number {
                        return Err(CorpusError::RevisionOrder {
                            path: file.path.clone(),
                        });
                    }
                    let actual =
                        lowercase_sha256_hex(Sha256::digest(revision.content.as_bytes()).into());
                    if actual != revision.content_sha256 {
                        return Err(CorpusError::RevisionDigestMismatch {
                            revision_id: revision.revision_id.clone(),
                        });
                    }
                    if revision_paths
                        .insert(revision.revision_id.clone(), file.path.clone())
                        .is_some()
                    {
                        return Err(CorpusError::DuplicateIdentifier {
                            kind: "revision_id",
                            value: revision.revision_id.clone(),
                        });
                    }
                }
            }
        }
        self.revision_paths = revision_paths;

        let mut request_ids = BTreeMap::new();
        for request in &self.recall_requests {
            if request_ids
                .insert(request.request_id.clone(), request)
                .is_some()
            {
                return Err(CorpusError::DuplicateIdentifier {
                    kind: "request_id",
                    value: request.request_id.clone(),
                });
            }
            if !scope_ids.contains(&request.scope_id) {
                return Err(CorpusError::UnknownReference {
                    kind: "scope_id",
                    value: request.scope_id.clone(),
                    scenario_id: request.scenario_id.clone(),
                });
            }
            let recall_fields_present = request.objective.is_some()
                && request.query.is_some()
                && request.budgets.is_some()
                && request.exclusions.is_some();
            let recall_fields_absent = request.objective.is_none()
                && request.query.is_none()
                && request.budgets.is_none()
                && request.exclusions.is_none();
            let well_formed = match request.operation {
                RecallRequestOperation::Recall | RecallRequestOperation::VerifyAbsence => {
                    recall_fields_present
                        && request
                            .query
                            .as_deref()
                            .is_some_and(|query| !query.is_empty() && query.trim() == query)
                        && request.budgets.is_some_and(|budgets| {
                            [
                                budgets.maximum_candidates,
                                budgets.maximum_candidate_content_bytes,
                                budgets.maximum_total_content_bytes,
                                budgets.maximum_source_refs_per_candidate,
                                budgets.maximum_trace_refs_per_candidate,
                                budgets.maximum_warnings,
                                budgets.maximum_extensions_per_candidate,
                            ]
                            .iter()
                            .all(|value| *value > 0)
                        })
                }
                RecallRequestOperation::Health => recall_fields_absent,
            };
            if !well_formed
                || request.policy_revision == 0
                || request.temporal_query.mode != "current"
            {
                return Err(CorpusError::RecallRequestMismatch {
                    request_id: request.request_id.clone(),
                    field: "operation_shape",
                });
            }
        }

        let mut references: BTreeMap<String, usize> = BTreeMap::new();
        let mut scenario_ids = BTreeSet::new();
        for scenario in &self.scenarios {
            let scenario_id = scenario.id.clone();
            if !scenario_ids.insert(scenario_id.clone()) {
                return Err(CorpusError::DuplicateIdentifier {
                    kind: "scenario_id",
                    value: scenario_id,
                });
            }
            if !fixture_ids.contains(&scenario.fixture_id) {
                return Err(CorpusError::UnknownReference {
                    kind: "fixture_id",
                    value: scenario.fixture_id.clone(),
                    scenario_id,
                });
            }
            for scope_id in [&scenario.source_scope_id, &scenario.target_scope_id] {
                if !scope_ids.contains(scope_id) {
                    return Err(CorpusError::UnknownReference {
                        kind: "scope_id",
                        value: scope_id.clone(),
                        scenario_id: scenario_id.clone(),
                    });
                }
            }
            let mut observation_ids = BTreeSet::new();
            for observation in &scenario.observations {
                if !observation_ids.insert(observation.observation_id.clone()) {
                    return Err(CorpusError::DuplicateIdentifier {
                        kind: "observation_id",
                        value: observation.observation_id.clone(),
                    });
                }
                if !scope_ids.contains(&observation.scope_id) {
                    return Err(CorpusError::UnknownReference {
                        kind: "scope_id",
                        value: observation.scope_id.clone(),
                        scenario_id: scenario_id.clone(),
                    });
                }
                if !self
                    .revision_paths
                    .contains_key(&observation.source_revision)
                {
                    return Err(CorpusError::UnknownReference {
                        kind: "revision_id",
                        value: observation.source_revision.clone(),
                        scenario_id: scenario_id.clone(),
                    });
                }
            }
            for revision in &scenario.code_evidence_revisions {
                if !self.revision_paths.contains_key(&revision.revision_id) {
                    return Err(CorpusError::UnknownReference {
                        kind: "revision_id",
                        value: revision.revision_id.clone(),
                        scenario_id: scenario_id.clone(),
                    });
                }
            }
            for outcome in &scenario
                .expected_admissible_behavior
                .allowed_terminal_outcomes
            {
                if TerminalCode::from_wire(outcome).is_none() {
                    return Err(CorpusError::UnknownTerminalOutcome {
                        scenario_id: scenario_id.clone(),
                        value: outcome.clone(),
                    });
                }
            }
            let rubric = &scenario.adjudication_rubric;
            let mut check_ids = BTreeSet::new();
            let mut weight_sum = 0_u32;
            for check in &rubric.checks {
                if !check_ids.insert(check.check_id.clone()) {
                    return Err(CorpusError::DuplicateIdentifier {
                        kind: "check_id",
                        value: check.check_id.clone(),
                    });
                }
                if check.weight_basis_points == 0 {
                    return Err(CorpusError::RubricWeightPrecision {
                        rubric_id: rubric.rubric_id.clone(),
                        check_id: check.check_id.clone(),
                    });
                }
                weight_sum = weight_sum.saturating_add(check.weight_basis_points);
            }
            if weight_sum != RUBRIC_WEIGHT_BASIS_POINTS {
                return Err(CorpusError::RubricWeightSum {
                    rubric_id: rubric.rubric_id.clone(),
                    basis_points: weight_sum,
                });
            }

            for (index, step) in scenario.steps.iter().enumerate() {
                let expected_number = u32::try_from(index).unwrap_or(u32::MAX).saturating_add(1);
                if step.step() != expected_number {
                    return Err(CorpusError::StepOrder {
                        scenario_id: scenario_id.clone(),
                    });
                }
                self.validate_step_references(scenario, step, &scope_ids, &observation_ids)?;
                if let Some((request_id, operation)) = step.request_id() {
                    let Some(request) = request_ids.get(request_id) else {
                        return Err(CorpusError::UnknownReference {
                            kind: "request_id",
                            value: request_id.to_owned(),
                            scenario_id: scenario_id.clone(),
                        });
                    };
                    if request.scenario_id != scenario.id {
                        return Err(CorpusError::RecallRequestMismatch {
                            request_id: request_id.to_owned(),
                            field: "scenario_id",
                        });
                    }
                    if request.operation != operation {
                        return Err(CorpusError::RecallRequestMismatch {
                            request_id: request_id.to_owned(),
                            field: "operation",
                        });
                    }
                    let expected_scope = match step {
                        ScenarioStep::Recall {
                            scope_id: Some(scope_id),
                            ..
                        } => scope_id.as_str(),
                        _ => scenario.target_scope_id.as_str(),
                    };
                    if request.scope_id != expected_scope {
                        return Err(CorpusError::RecallRequestMismatch {
                            request_id: request_id.to_owned(),
                            field: "scope_id",
                        });
                    }
                    *references.entry(request_id.to_owned()).or_insert(0) += 1;
                }
            }
            match scenario.steps.last() {
                Some(ScenarioStep::Adjudicate { rubric, .. })
                    if rubric == &scenario.id
                        && rubric == &scenario.adjudication_rubric.rubric_id => {}
                _ => {
                    return Err(CorpusError::MissingAdjudication { scenario_id });
                }
            }
        }
        for request in &self.recall_requests {
            let count = references.get(&request.request_id).copied().unwrap_or(0);
            if count != 1 {
                return Err(CorpusError::RecallRequestReferenceCount {
                    request_id: request.request_id.clone(),
                    references: count,
                });
            }
        }
        Ok(self)
    }

    fn validate_step_references(
        &self,
        scenario: &ScenarioDefinition,
        step: &ScenarioStep,
        scope_ids: &BTreeSet<String>,
        observation_ids: &BTreeSet<String>,
    ) -> Result<(), CorpusError> {
        let unknown = |kind: &'static str, value: &str| CorpusError::UnknownReference {
            kind,
            value: value.to_owned(),
            scenario_id: scenario.id.clone(),
        };
        match step {
            ScenarioStep::Observe { observation_id, .. }
            | ScenarioStep::Replay { observation_id, .. } => {
                if !observation_ids.contains(observation_id) {
                    return Err(unknown("observation_id", observation_id));
                }
            }
            ScenarioStep::AdvanceCode { revision_id, .. } => {
                if !self.revision_paths.contains_key(revision_id) {
                    return Err(unknown("revision_id", revision_id));
                }
            }
            ScenarioStep::Recall {
                scope_id: Some(scope_id),
                ..
            }
            | ScenarioStep::OpenNewAgentSession { scope_id, .. } => {
                if !scope_ids.contains(scope_id) {
                    return Err(unknown("scope_id", scope_id));
                }
            }
            ScenarioStep::Cancel { at_item, .. } if *at_item == 0 => {
                return Err(CorpusError::StepOrder {
                    scenario_id: scenario.id.clone(),
                });
            }
            ScenarioStep::BeginObservationBatch { batch_id, .. } => {
                let has_template = scenario.observations.iter().any(|observation| {
                    observation.event_type == "observation_batch_requested"
                        && observation.payload.get("batch_id").and_then(Value::as_str)
                            == Some(batch_id.as_str())
                });
                if !has_template {
                    return Err(CorpusError::MissingBatchTemplate {
                        scenario_id: scenario.id.clone(),
                        batch_id: batch_id.clone(),
                    });
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Returns the lowercase SHA-256 of the exact corpus bytes that were loaded.
    #[must_use]
    pub fn corpus_sha256(&self) -> &str {
        &self.corpus_sha256
    }

    /// Returns the corpus schema version.
    #[must_use]
    pub const fn schema_version(&self) -> u64 {
        self.schema_version
    }

    /// Returns the stable corpus identity.
    #[must_use]
    pub fn corpus_id(&self) -> &str {
        &self.corpus_id
    }

    /// Returns the bead that owns the corpus.
    #[must_use]
    pub fn bead_id(&self) -> &str {
        &self.bead_id
    }

    /// Returns the corpus title.
    #[must_use]
    pub fn title(&self) -> &str {
        &self.title
    }

    /// Returns the declared canonical encoding.
    #[must_use]
    pub fn canonical_encoding(&self) -> &str {
        &self.canonical_encoding
    }

    /// Returns whether the corpus declares itself provider-neutral.
    #[must_use]
    pub const fn provider_neutral(&self) -> bool {
        self.provider_neutral
    }

    /// Returns the provider selection policy.
    #[must_use]
    pub const fn provider_selection(&self) -> &ProviderSelectionPolicy {
        &self.provider_selection
    }

    /// Returns the fixture execution policy.
    #[must_use]
    pub const fn fixture_policy(&self) -> &FixturePolicy {
        &self.fixture_policy
    }

    /// Returns the adjudication policy.
    #[must_use]
    pub const fn adjudication_policy(&self) -> &AdjudicationPolicy {
        &self.adjudication_policy
    }

    /// Returns the scope catalog in corpus order.
    #[must_use]
    pub fn scope_catalog(&self) -> &[ScopeEntry] {
        &self.scope_catalog
    }

    /// Returns one scope by identity.
    #[must_use]
    pub fn scope(&self, scope_id: &str) -> Option<&ScopeEntry> {
        self.scope_catalog
            .iter()
            .find(|scope| scope.scope_id == scope_id)
    }

    /// Returns every fixture in corpus order.
    #[must_use]
    pub fn fixtures(&self) -> &[FixtureDefinition] {
        &self.fixtures
    }

    /// Returns one fixture by identity.
    #[must_use]
    pub fn fixture(&self, fixture_id: &str) -> Option<&FixtureDefinition> {
        self.fixtures
            .iter()
            .find(|fixture| fixture.fixture_id == fixture_id)
    }

    /// Returns the recall-request catalog in corpus order.
    #[must_use]
    pub fn recall_requests(&self) -> &[RecallRequestDefinition] {
        &self.recall_requests
    }

    /// Returns one catalogued request by identity.
    #[must_use]
    pub fn recall_request(&self, request_id: &str) -> Option<&RecallRequestDefinition> {
        self.recall_requests
            .iter()
            .find(|request| request.request_id == request_id)
    }

    /// Returns every scenario in corpus order.
    #[must_use]
    pub fn scenarios(&self) -> &[ScenarioDefinition] {
        &self.scenarios
    }

    /// Returns one scenario by identity.
    #[must_use]
    pub fn scenario(&self, scenario_id: &str) -> Option<&ScenarioDefinition> {
        self.scenarios
            .iter()
            .find(|scenario| scenario.id == scenario_id)
    }

    /// Resolves a revision identity to its fixture file path and revision.
    #[must_use]
    pub fn revision(&self, revision_id: &str) -> Option<(&str, &FileRevision)> {
        let path = self.revision_paths.get(revision_id)?;
        let revision = self
            .fixtures
            .iter()
            .flat_map(|fixture| fixture.files.iter())
            .filter(|file| &file.path == path)
            .flat_map(|file| file.revisions.iter())
            .find(|revision| revision.revision_id == revision_id)?;
        Some((path.as_str(), revision))
    }
}
