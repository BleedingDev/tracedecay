//! Typed failures of catalog loading and metric evaluation.

use thiserror::Error;

/// A defect in the metric catalog bytes or their binding to the code.
#[derive(Debug, Error)]
pub enum CatalogError {
    /// The catalog is not valid JSON of the expected shape.
    #[error("metric catalog is not valid JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// The catalog declares a schema version this code does not compute.
    #[error("metric catalog schema version {found} is not supported (expected {expected})")]
    UnsupportedSchemaVersion {
        /// Declared schema version.
        found: u64,
        /// Supported schema version.
        expected: u64,
    },
    /// The catalog identity differs from the one this code is bound to.
    #[error("metric catalog id {found:?} is not {expected:?}")]
    CatalogIdMismatch {
        /// Declared catalog id.
        found: String,
        /// Expected catalog id.
        expected: String,
    },
    /// A metric id appears more than once.
    #[error("metric {metric_id} is defined more than once")]
    DuplicateMetric {
        /// Offending metric id.
        metric_id: String,
    },
    /// A metric the code computes has no catalog definition.
    #[error("metric {metric_id} is computed by the code but missing from the catalog")]
    MissingMetric {
        /// Missing metric id.
        metric_id: String,
    },
    /// A metric definition contradicts the catalog invariants.
    #[error("metric {metric_id} is invalid: {reason}")]
    InvalidMetric {
        /// Offending metric id.
        metric_id: String,
        /// Why the definition is rejected.
        reason: String,
    },
    /// The label vocabulary does not partition into resolved and unresolved labels.
    #[error("label vocabulary is invalid: {0}")]
    InvalidLabelVocabulary(String),
    /// A safety-critical check list is malformed.
    #[error("safety_critical_checks entry for scenario {scenario_id} is invalid: {reason}")]
    InvalidSafetyCriticalChecks {
        /// Scenario id.
        scenario_id: String,
        /// Why the entry is rejected.
        reason: String,
    },
}

/// A defect in the run records handed to the evaluator.
#[derive(Debug, Error)]
pub enum EvaluationError {
    /// The catalog could not be loaded or validated.
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    /// No scenario was recorded; an empty run never yields a verdict.
    #[error("no scenario run records were supplied")]
    NoScenarios,
    /// A scenario id is not bound by the catalog.
    #[error("scenario {scenario_id} is not part of the bound corpus")]
    UnknownScenario {
        /// Offending scenario id.
        scenario_id: String,
    },
    /// The same scenario appears twice in one provider run.
    #[error("scenario {scenario_id} was recorded more than once")]
    DuplicateScenario {
        /// Offending scenario id.
        scenario_id: String,
    },
    /// A terminal code in a run record is not a canonical wire value.
    #[error("scenario {scenario_id} carries non-canonical terminal code {terminal_code:?}")]
    UnknownTerminalCode {
        /// Scenario id.
        scenario_id: String,
        /// Offending wire value.
        terminal_code: String,
    },
    /// A rubric check appears twice in one scenario record.
    #[error("scenario {scenario_id} records check {check_id} more than once")]
    DuplicateCheck {
        /// Scenario id.
        scenario_id: String,
        /// Check id.
        check_id: String,
    },
    /// A discovery record claims more rediscovered facts than required facts.
    #[error(
        "scenario {scenario_id} rediscovered {rediscovered} facts but only {required} were required"
    )]
    DiscoveryExceedsRequired {
        /// Scenario id.
        scenario_id: String,
        /// Required facts.
        required: u64,
        /// Rediscovered facts.
        rediscovered: u64,
    },
    /// An annotation targets a candidate the baseline never admitted.
    #[error(
        "annotation targets scenario {scenario_id} request {request_id} candidate {candidate_ref}, which was not admitted"
    )]
    UnmatchedAnnotation {
        /// Scenario id.
        scenario_id: String,
        /// Request id.
        request_id: String,
        /// Candidate reference.
        candidate_ref: String,
    },
    /// An annotation targets a scenario the baseline never ran.
    #[error("annotation targets scenario {scenario_id}, which was not run")]
    UnmatchedScenarioAnnotation {
        /// Scenario id.
        scenario_id: String,
    },
}
