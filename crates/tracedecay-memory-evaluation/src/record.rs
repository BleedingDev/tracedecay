//! Typed run records the evaluator consumes.
//!
//! A record never infers a value the runner did not measure: every optional
//! measurement is an explicit enum with a reason, and every candidate carries
//! a label from the pinned vocabulary, `missing` included.

use serde::{Deserialize, Serialize};

/// Candidate label from the catalog vocabulary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateLabel {
    /// The candidate helped the task.
    Useful,
    /// The candidate hurt the task or contradicted current truth.
    Harmful,
    /// The candidate was true once and is superseded.
    Stale,
    /// The candidate was neither useful nor harmful.
    Irrelevant,
    /// The candidate could not be verified against evidence.
    Unverifiable,
    /// A labeler looked and could not decide.
    Indeterminate,
    /// Nobody labeled the candidate.
    Missing,
}

impl CandidateLabel {
    /// Every label, in vocabulary order.
    pub const ALL: [Self; 7] = [
        Self::Useful,
        Self::Harmful,
        Self::Stale,
        Self::Irrelevant,
        Self::Unverifiable,
        Self::Indeterminate,
        Self::Missing,
    ];

    /// Wire value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Useful => "useful",
            Self::Harmful => "harmful",
            Self::Stale => "stale",
            Self::Irrelevant => "irrelevant",
            Self::Unverifiable => "unverifiable",
            Self::Indeterminate => "indeterminate",
            Self::Missing => "missing",
        }
    }

    /// Whether the label resolves the candidate for label-based denominators.
    #[must_use]
    pub const fn is_resolved(self) -> bool {
        !matches!(self, Self::Indeterminate | Self::Missing)
    }
}

/// Provenance state of an admitted candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProvenanceState {
    /// Provenance is present.
    Available,
    /// Provenance exists and was redacted by policy.
    Redacted,
    /// The provider declared it has no provenance.
    Unavailable,
    /// Nobody recorded a provenance state.
    Missing,
}

impl ProvenanceState {
    /// Whether the state counts as explicit provenance.
    #[must_use]
    pub const fn is_complete(self) -> bool {
        matches!(self, Self::Available | Self::Redacted)
    }
}

/// One candidate admitted into context by a recall-class step.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AdmittedCandidate {
    /// Catalogued request the candidate answered.
    pub request_id: String,
    /// Stable candidate reference (source path or candidate identity).
    pub candidate_ref: String,
    /// Whether the candidate's exact scope equals the request scope.
    pub scope_match: bool,
    /// Provenance state.
    pub provenance: ProvenanceState,
    /// Label.
    pub label: CandidateLabel,
    /// Whether the candidate still carries a source key the scenario asked to forget.
    pub contains_forgotten_source: bool,
}

/// Resolved or unresolved task outcome of one scenario.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TaskOutcome {
    /// Rubric and terminal gate passed.
    Pass,
    /// A check failed or the terminal gate failed.
    Fail,
    /// Evidence was insufficient to decide.
    Indeterminate {
        /// Why.
        reason: String,
    },
}

/// Outcome of one rubric check.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckOutcome {
    /// Evidence satisfied the check.
    Pass,
    /// Evidence contradicted the check.
    Fail,
    /// No evaluator or insufficient evidence.
    Indeterminate,
}

/// One rubric check result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RubricCheckResult {
    /// Check id from the corpus rubric.
    pub check_id: String,
    /// Outcome.
    pub outcome: CheckOutcome,
}

/// Terminal gate evidence over outcome-bearing steps.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TerminalGateEvidence {
    /// Whether every gated terminal code was allowed by the scenario.
    pub passed: bool,
    /// Terminal codes observed, as canonical wire values.
    pub observed_terminal_codes: Vec<String>,
    /// Violations as `step:terminal_code`.
    pub violations: Vec<String>,
}

/// A measurement the runner either took or explicitly did not take.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Measured<T> {
    /// Measured value.
    Value {
        /// The measurement.
        value: T,
    },
    /// Not measured; never inferred.
    Unmeasured {
        /// Why.
        reason: String,
    },
}

/// Correction-latency evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CorrectionEvidence {
    /// The scenario has no stale-then-corrected claim.
    NotApplicable {
        /// Why.
        reason: String,
    },
    /// Measured latency from correcting evidence to corrected admission.
    Measured {
        /// Microseconds.
        latency_micros: u64,
    },
    /// The scenario has a correction but the runner did not measure it.
    Unmeasured {
        /// Why.
        reason: String,
    },
}

/// Repeated-discovery evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DiscoveryEvidence {
    /// The runner did not enumerate required facts.
    NotEnumerated {
        /// Why.
        reason: String,
    },
    /// Required facts and how many were rediscovered from source.
    Enumerated {
        /// Facts the task required.
        required_facts: u64,
        /// Required facts rediscovered from source despite memory.
        rediscovered_facts: u64,
    },
}

/// Corrupt-state recall evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CorruptStateEvidence {
    /// The scenario never loads corrupt provider state.
    NotExercised,
    /// Admissions sourced from corrupt state were counted.
    Enumerated {
        /// Count.
        admitted_from_corrupt_state: u64,
    },
    /// The scenario loads corrupt state but the count is unknown.
    NotEnumerable {
        /// Why.
        reason: String,
    },
}

/// Everything the evaluator needs about one scenario run.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScenarioRunRecord {
    /// Scenario id from the corpus.
    pub scenario_id: String,
    /// Terminal gate evidence.
    pub terminal_gate: TerminalGateEvidence,
    /// Task outcome.
    pub task_outcome: TaskOutcome,
    /// Rubric check results.
    pub rubric_checks: Vec<RubricCheckResult>,
    /// Admitted candidates across every recall-class step.
    pub candidates: Vec<AdmittedCandidate>,
    /// Recall wall-clock samples in microseconds.
    pub recall_latency_micros: Vec<u64>,
    /// Tokens of admitted context under the pinned estimator.
    pub context_tokens: Measured<u64>,
    /// Seconds of human curation.
    pub curation_seconds: Measured<u64>,
    /// Correction evidence.
    pub correction: CorrectionEvidence,
    /// Discovery evidence.
    pub discovery: DiscoveryEvidence,
    /// Corrupt-state evidence.
    pub corrupt_state: CorruptStateEvidence,
}

/// Provider identity carried as run metadata, never as an input to a metric.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProviderRunIdentity {
    /// Lane id (`no_memory`, `explicit_documentation`, `provider:<id>`).
    pub lane_id: String,
    /// Provider id when the lane wraps a provider.
    pub provider_id: Option<String>,
    /// Run identity digest when the runner produced one.
    pub run_identity_sha256: Option<String>,
}

/// One provider's run over the corpus.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProviderRunRecord {
    /// Provider identity.
    pub provider: ProviderRunIdentity,
    /// Scenario records.
    pub scenarios: Vec<ScenarioRunRecord>,
}
