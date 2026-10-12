//! Metric report types.
//!
//! The aggregate task score, the safety gate, and the verdict are three
//! separate fields of one report; no surface returns the aggregate alone.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::catalog::{Determinism, MetricClass, MetricId};
use crate::record::{CheckOutcome, ProviderRunIdentity};

/// Report format identity.
pub const REPORT_FORMAT: &str = "tracedecay.coding-memory.metric-report.v1";

/// Counts of candidate labels behind a label-based metric.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct LabelCounts {
    /// Candidates with a resolved label.
    pub labeled: u64,
    /// Candidates nobody labeled (`missing`).
    pub unlabeled: u64,
    /// Candidates a labeler could not decide (`indeterminate`).
    pub indeterminate: u64,
}

impl LabelCounts {
    /// Total candidates counted.
    #[must_use]
    pub const fn total(self) -> u64 {
        self.labeled + self.unlabeled + self.indeterminate
    }

    /// Candidates without a resolved label.
    #[must_use]
    pub const fn unresolved(self) -> u64 {
        self.unlabeled + self.indeterminate
    }

    pub(crate) fn add(&mut self, other: Self) {
        self.labeled += other.labeled;
        self.unlabeled += other.unlabeled;
        self.indeterminate += other.indeterminate;
    }
}

/// A computed metric value, or the honest reason there is none.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MetricValue {
    /// Numerator over an explicit denominator population.
    Ratio {
        /// Numerator count.
        numerator: u64,
        /// Denominator count; never zero.
        denominator: u64,
        /// `numerator / denominator`.
        value: f64,
    },
    /// A measured quantity in the metric unit.
    Quantity {
        /// Value.
        value: f64,
        /// Samples behind the value.
        samples: u64,
    },
    /// The metric could not be computed truthfully.
    Indeterminate {
        /// Why.
        reason: String,
    },
    /// The population is empty or the scenario does not exercise the metric.
    NotApplicable {
        /// Why.
        reason: String,
    },
}

impl MetricValue {
    /// Numeric value when the metric was computed.
    #[must_use]
    pub const fn numeric(&self) -> Option<f64> {
        match self {
            Self::Ratio { value, .. } | Self::Quantity { value, .. } => Some(*value),
            Self::Indeterminate { .. } | Self::NotApplicable { .. } => None,
        }
    }

    /// Whether the value is `Indeterminate`.
    #[must_use]
    pub const fn is_indeterminate(&self) -> bool {
        matches!(self, Self::Indeterminate { .. })
    }

    pub(crate) fn ratio(numerator: u64, denominator: u64) -> Self {
        // Callers guarantee a non-zero denominator; a zero denominator is
        // reported through the zero-population policy before reaching here.
        let value = if denominator == 0 {
            0.0
        } else {
            numerator as f64 / denominator as f64
        };
        Self::Ratio {
            numerator,
            denominator,
            value,
        }
    }
}

/// One computed metric with its catalog binding.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricResult {
    /// Metric id.
    pub metric_id: MetricId,
    /// Catalog definition version.
    pub version: u32,
    /// Class.
    pub class: MetricClass,
    /// Determinism.
    pub determinism: Determinism,
    /// Whether the metric gates safety.
    pub safety_gating: bool,
    /// Ceiling for gating metrics.
    pub ceiling: Option<f64>,
    /// Value.
    pub value: MetricValue,
    /// Label counts for label-based metrics.
    pub label_counts: Option<LabelCounts>,
}

/// One reason the safety gate failed.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SafetyFailure {
    /// A gating metric exceeded its ceiling.
    MetricExceededCeiling {
        /// Scenario id.
        scenario_id: String,
        /// Metric id.
        metric_id: MetricId,
        /// Computed value.
        value: f64,
        /// Ceiling.
        ceiling: f64,
    },
    /// A gating metric was indeterminate.
    MetricIndeterminate {
        /// Scenario id.
        scenario_id: String,
        /// Metric id.
        metric_id: MetricId,
        /// Why.
        reason: String,
    },
    /// A safety-critical rubric check did not pass.
    SafetyCriticalCheckNotPassed {
        /// Scenario id.
        scenario_id: String,
        /// Check id.
        check_id: String,
        /// Recorded outcome.
        outcome: CheckOutcome,
    },
    /// A safety-critical rubric check was never recorded.
    SafetyCriticalCheckMissing {
        /// Scenario id.
        scenario_id: String,
        /// Check id.
        check_id: String,
    },
    /// The scenario's terminal gate failed.
    TerminalGateFailed {
        /// Scenario id.
        scenario_id: String,
        /// Violations as `step:terminal_code`.
        violations: Vec<String>,
    },
}

impl SafetyFailure {
    /// Scenario the failure belongs to.
    #[must_use]
    pub fn scenario_id(&self) -> &str {
        match self {
            Self::MetricExceededCeiling { scenario_id, .. }
            | Self::MetricIndeterminate { scenario_id, .. }
            | Self::SafetyCriticalCheckNotPassed { scenario_id, .. }
            | Self::SafetyCriticalCheckMissing { scenario_id, .. }
            | Self::TerminalGateFailed { scenario_id, .. } => scenario_id,
        }
    }

    /// Metric the failure names, when it names one.
    #[must_use]
    pub const fn metric_id(&self) -> Option<MetricId> {
        match self {
            Self::MetricExceededCeiling { metric_id, .. }
            | Self::MetricIndeterminate { metric_id, .. } => Some(*metric_id),
            Self::SafetyCriticalCheckNotPassed { .. }
            | Self::SafetyCriticalCheckMissing { .. }
            | Self::TerminalGateFailed { .. } => None,
        }
    }
}

/// The safety gate over every scenario.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SafetyGate {
    /// Whether no failure was recorded.
    pub passed: bool,
    /// Every failure, in scenario order.
    pub failures: Vec<SafetyFailure>,
}

/// Per-scenario metric report.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScenarioMetricReport {
    /// Scenario id.
    pub scenario_id: String,
    /// Every metric of the catalog.
    pub metrics: BTreeMap<MetricId, MetricResult>,
    /// Safety failures of this scenario.
    pub safety_failures: Vec<SafetyFailure>,
}

/// Aggregate task score with its coverage.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AggregateTaskScore {
    /// Mean task outcome over resolved scenarios, in `[0, 1]`.
    pub value: f64,
    /// Scenarios whose task outcome resolved to pass or fail.
    pub resolved_scenarios: u64,
    /// Scenarios whose task outcome was indeterminate.
    pub indeterminate_scenarios: u64,
}

/// Report verdict.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// The safety gate passed.
    Pass,
    /// The safety gate failed, regardless of the aggregate.
    Fail,
}

/// Complete metric report for one provider run.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricReport {
    /// Report format identity.
    pub report_format: String,
    /// Catalog id.
    pub catalog_id: String,
    /// Catalog version.
    pub catalog_version: u64,
    /// Label vocabulary version.
    pub label_vocabulary_version: u32,
    /// Corpus id the catalog binds.
    pub corpus_id: String,
    /// Provider identity (metadata only).
    pub provider: ProviderRunIdentity,
    /// Per-scenario reports in input order.
    pub scenarios: Vec<ScenarioMetricReport>,
    /// Per-provider metrics pooled over scenarios.
    pub provider_metrics: BTreeMap<MetricId, MetricResult>,
    /// Aggregate task score, absent when no scenario resolved.
    pub aggregate_task_score: Option<AggregateTaskScore>,
    /// Safety gate.
    pub safety_gate: SafetyGate,
    /// Verdict: `Fail` whenever the safety gate failed.
    pub verdict: Verdict,
}

impl MetricReport {
    /// Whether the verdict is `Pass`.
    #[must_use]
    pub const fn passed(&self) -> bool {
        matches!(self.verdict, Verdict::Pass)
    }

    /// One scenario report.
    #[must_use]
    pub fn scenario(&self, scenario_id: &str) -> Option<&ScenarioMetricReport> {
        self.scenarios
            .iter()
            .find(|scenario| scenario.scenario_id == scenario_id)
    }
}
