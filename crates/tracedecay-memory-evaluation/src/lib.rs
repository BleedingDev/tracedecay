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
//! Versioned memory-quality, safety, cost, and latency metrics for coding-memory
//! providers (`tdmem-0904`).
//!
//! The catalog `product/evaluation/coding-memory-metrics.v1.json` is compiled
//! into this crate ([`CATALOG_JSON`]) and validated against the metrics the code
//! computes ([`MetricId::ALL`]) so definitions and computation cannot drift.
//! [`evaluate`] turns typed [`ProviderRunRecord`]s into a [`MetricReport`] whose
//! aggregate task score, safety gate, and verdict are separate fields: the
//! verdict is `Fail` whenever any safety-class metric exceeds its ceiling or is
//! indeterminate, any safety-critical rubric check is not a pass, or a terminal
//! gate failed, regardless of the aggregate. Unresolved labels are never coerced
//! to zero or to pass; the report carries labeled, unlabeled, and indeterminate
//! counts beside every label-based value.
//!
//! Caller status: the conformance baseline runner (`tdmem-0902`) produces a
//! [`tracedecay_memory_conformance::BaselineRunOutput`]; it does not call this
//! crate. [`provider_run_from_baseline`] converts that output plus
//! [`BaselineAnnotations`] into run records without inferring anything the
//! runner did not measure. The intended consumer is the Native versus NCM
//! differential runner (`tdmem-0905`), which does not exist yet; today the only
//! callers are this crate's integration tests, which drive the real
//! `BaselineRunner` over the `NoMemory` and `ExplicitDocumentation` lanes. The
//! root-crate Native lane (`native_baseline`) is not yet evaluated here.

mod baseline;
mod catalog;
mod error;
mod evaluate;
mod record;
mod report;

pub use baseline::{
    BaselineAnnotations, CORRUPT_RECALL_CHECK_ID, CandidateAnnotation, provider_run_from_baseline,
};
pub use catalog::{
    AggregateTaskScoreDefinition, AggregationDefinition, CATALOG_ID, CATALOG_JSON, CheckBinding,
    CorpusBinding, DenominatorDefinition, Determinism, Direction, LabelVocabulary, MetricCatalog,
    MetricClass, MetricDefinition, MetricId, PercentileMethod, ProvenanceStateVocabulary,
    SUPPORTED_SCHEMA_VERSION, SafetyGatePolicy, TerminalContractBinding, UnresolvedLabelPolicy,
    ZeroPopulationPolicy,
};
pub use error::{CatalogError, EvaluationError};
pub use evaluate::{evaluate, nearest_rank_percentile};
pub use record::{
    AdmittedCandidate, CandidateLabel, CheckOutcome, CorrectionEvidence, CorruptStateEvidence,
    DiscoveryEvidence, Measured, ProvenanceState, ProviderRunIdentity, ProviderRunRecord,
    RubricCheckResult, ScenarioRunRecord, TaskOutcome, TerminalGateEvidence,
};
pub use report::{
    AggregateTaskScore, LabelCounts, MetricReport, MetricResult, MetricValue, REPORT_FORMAT,
    SafetyFailure, SafetyGate, ScenarioMetricReport, Verdict,
};

use tracedecay_memory_conformance::BaselineRunOutput;

impl MetricReport {
    /// Evaluates a baseline run under the embedded catalog.
    pub fn from_baseline_run(
        output: &BaselineRunOutput,
        annotations: &BaselineAnnotations,
    ) -> Result<Self, EvaluationError> {
        let catalog = MetricCatalog::embedded()?;
        let run = provider_run_from_baseline(&catalog, output, annotations)?;
        evaluate(&catalog, &run)
    }
}
