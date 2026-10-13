//! The versioned metric catalog and its binding to the metrics the code computes.

use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::CatalogError;

/// Raw bytes of the checked-in catalog; the code and the definitions ship together.
pub const CATALOG_JSON: &str =
    include_str!("../../../product/evaluation/coding-memory-metrics.v1.json");

/// Catalog schema version this code computes.
pub const SUPPORTED_SCHEMA_VERSION: u64 = 1;

/// Catalog identity this code is bound to.
pub const CATALOG_ID: &str = "tracedecay.coding-memory.metrics.v1";

/// Identity of every metric the evaluator computes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricId {
    /// Whether the scenario task passed its rubric and terminal gate.
    TaskOutcome,
    /// Share of labeled admitted candidates that were useful.
    UsefulRecallPrecision,
    /// Share of admitted candidates that were harmful or stale.
    HarmfulStaleRecallRate,
    /// Time from correcting evidence to admission of the corrected claim.
    CorrectionLatency,
    /// Share of required facts rediscovered from source despite memory.
    RepeatedDiscoveryRate,
    /// Tokens of admitted context.
    ContextTokens,
    /// Nearest-rank 50th percentile of recall latency.
    RecallLatencyP50,
    /// Nearest-rank 95th percentile of recall latency.
    RecallLatencyP95,
    /// Seconds of human curation.
    HumanCurationTime,
    /// Share of admitted candidates with explicit provenance.
    ProvenanceCompleteness,
    /// Share of admitted candidates outside the request scope.
    ScopeLeakage,
    /// Admitted candidates sourced from corrupt provider state.
    CorruptStateRecall,
    /// Share of admitted candidates carrying a forgotten source key.
    DeletedSourceRecall,
}

impl MetricId {
    /// Every metric, in catalog order.
    pub const ALL: [Self; 13] = [
        Self::TaskOutcome,
        Self::UsefulRecallPrecision,
        Self::HarmfulStaleRecallRate,
        Self::CorrectionLatency,
        Self::RepeatedDiscoveryRate,
        Self::ContextTokens,
        Self::RecallLatencyP50,
        Self::RecallLatencyP95,
        Self::HumanCurationTime,
        Self::ProvenanceCompleteness,
        Self::ScopeLeakage,
        Self::CorruptStateRecall,
        Self::DeletedSourceRecall,
    ];

    /// Stable wire identity, equal to the catalog `metric_id`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TaskOutcome => "task_outcome",
            Self::UsefulRecallPrecision => "useful_recall_precision",
            Self::HarmfulStaleRecallRate => "harmful_stale_recall_rate",
            Self::CorrectionLatency => "correction_latency",
            Self::RepeatedDiscoveryRate => "repeated_discovery_rate",
            Self::ContextTokens => "context_tokens",
            Self::RecallLatencyP50 => "recall_latency_p50",
            Self::RecallLatencyP95 => "recall_latency_p95",
            Self::HumanCurationTime => "human_curation_time",
            Self::ProvenanceCompleteness => "provenance_completeness",
            Self::ScopeLeakage => "scope_leakage",
            Self::CorruptStateRecall => "corrupt_state_recall",
            Self::DeletedSourceRecall => "deleted_source_recall",
        }
    }
}

impl MetricId {
    /// Per-scenario and per-provider aggregation modes the code implements.
    #[must_use]
    pub const fn aggregation_modes(self) -> (&'static str, &'static str) {
        match self {
            Self::TaskOutcome => ("single_value", "pooled_ratio"),
            Self::UsefulRecallPrecision
            | Self::HarmfulStaleRecallRate
            | Self::RepeatedDiscoveryRate
            | Self::ProvenanceCompleteness
            | Self::ScopeLeakage
            | Self::DeletedSourceRecall => ("ratio", "pooled_ratio"),
            Self::CorrectionLatency => ("single_value", "mean"),
            Self::ContextTokens | Self::HumanCurationTime | Self::CorruptStateRecall => {
                ("single_value", "sum")
            }
            Self::RecallLatencyP50 | Self::RecallLatencyP95 => {
                ("nearest_rank_percentile", "pooled_nearest_rank_percentile")
            }
        }
    }
}

impl fmt::Display for MetricId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for MetricId {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|metric| metric.as_str() == value)
            .ok_or_else(|| format!("unknown metric id {value:?}"))
    }
}

/// Metric class.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricClass {
    /// Usefulness of recall for the task.
    Quality,
    /// Harm to the task or to canonical truth.
    Safety,
    /// Resource or human cost.
    Cost,
    /// Wall-clock latency.
    Latency,
}

/// Whether a metric is byte-stable across reruns of the same inputs.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Determinism {
    /// Same inputs, same value.
    Deterministic,
    /// Depends on wall clock, estimator, or a human.
    Nondeterministic,
}

/// How unresolved candidate labels enter a denominator.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnresolvedLabelPolicy {
    /// Unresolved candidates leave the denominator; their counts are reported.
    ExcludeAndReport,
    /// One unresolved candidate makes the metric indeterminate.
    IndeterminateIfAny,
    /// The metric ignores labels.
    NotLabelBased,
}

/// What an empty population yields.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ZeroPopulationPolicy {
    /// Reported as not applicable; neither passes nor fails.
    NotApplicable,
    /// Reported as indeterminate.
    Indeterminate,
}

/// Which direction is better.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// Larger values are better.
    HigherIsBetter,
    /// Smaller values are better.
    LowerIsBetter,
}

/// Denominator definition of one metric.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DenominatorDefinition {
    /// Human description of the population.
    pub population: String,
    /// Unresolved-label policy.
    pub unresolved_label_policy: UnresolvedLabelPolicy,
    /// Zero-population policy.
    pub zero_population_policy: ZeroPopulationPolicy,
}

/// Aggregation modes of one metric.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregationDefinition {
    /// Per-scenario mode.
    pub per_scenario: String,
    /// Per-provider mode.
    pub per_provider: String,
}

/// Rubric checks of one scenario bound to a metric or to the safety gate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckBinding {
    /// Scenario id.
    pub scenario_id: String,
    /// Check ids within the scenario rubric.
    pub check_ids: Vec<String>,
}

/// One versioned metric definition.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricDefinition {
    /// Metric identity.
    pub metric_id: MetricId,
    /// Definition version.
    pub version: u32,
    /// Metric class.
    pub class: MetricClass,
    /// Title.
    pub title: String,
    /// Description.
    pub description: String,
    /// Numerator definition.
    pub numerator: String,
    /// Denominator definition.
    pub denominator: DenominatorDefinition,
    /// Unit.
    pub unit: String,
    /// Direction.
    pub direction: Direction,
    /// Aggregation modes.
    pub aggregation: AggregationDefinition,
    /// Determinism.
    pub determinism: Determinism,
    /// Whether the metric participates in the safety gate.
    pub safety_gating: bool,
    /// Ceiling a safety-gating metric must not exceed.
    pub ceiling: Option<f64>,
    /// Runner inputs the metric consumes.
    pub inputs: Vec<String>,
    /// Scenarios that exercise the metric; `None` means every scenario.
    pub applicable_scenarios: Option<Vec<String>>,
    /// Corpus rubric checks the metric explains.
    pub rubric_check_bindings: Vec<CheckBinding>,
}

/// Corpus the catalog is bound to.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CorpusBinding {
    /// Corpus id.
    pub corpus_id: String,
    /// Corpus schema version.
    pub schema_version: u64,
    /// Corpus bead id.
    pub bead_id: String,
}

/// Terminal contract the catalog is bound to.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalContractBinding {
    /// Contract id.
    pub contract_id: String,
    /// Contract schema version.
    pub schema_version: u64,
}

/// Pinned candidate label vocabulary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LabelVocabulary {
    /// Vocabulary version.
    pub version: u32,
    /// Every label.
    pub labels: Vec<String>,
    /// Labels that resolve a candidate.
    pub resolved_labels: Vec<String>,
    /// Labels that leave a candidate unresolved.
    pub unresolved_labels: Vec<String>,
    /// Reconciliation note against the feedback capability.
    pub reconciliation: String,
}

/// Provenance states considered complete or incomplete.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProvenanceStateVocabulary {
    /// Explicit states.
    pub complete: Vec<String>,
    /// Non-explicit states.
    pub incomplete: Vec<String>,
    /// Why.
    pub rationale: String,
}

/// Percentile method.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PercentileMethod {
    /// Method name.
    pub name: String,
    /// Definition.
    pub definition: String,
}

/// Safety-gate policy.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SafetyGatePolicy {
    /// Human rule.
    pub rule: String,
    /// Indeterminate policy inherited from the corpus.
    pub indeterminate_policy: String,
    /// Missing-evidence policy inherited from the corpus.
    pub missing_evidence_policy: String,
    /// Provider-failure policy inherited from the corpus.
    pub provider_failure_policy: String,
    /// Always false: the aggregate can never hide the gate.
    pub aggregate_task_score_can_hide_safety: bool,
}

/// Aggregate task score definition.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateTaskScoreDefinition {
    /// Definition.
    pub definition: String,
    /// Coverage fields every report carries beside the aggregate.
    pub coverage_fields: Vec<String>,
}

/// The whole versioned catalog.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricCatalog {
    /// Catalog schema version.
    pub schema_version: u64,
    /// Catalog id.
    pub catalog_id: String,
    /// Catalog version.
    pub catalog_version: u64,
    /// Owning bead.
    pub bead_id: String,
    /// Title.
    pub title: String,
    /// Corpus binding.
    pub corpus_binding: CorpusBinding,
    /// Terminal contract binding.
    pub terminal_contract_binding: TerminalContractBinding,
    /// Label vocabulary.
    pub label_vocabulary: LabelVocabulary,
    /// Task outcome vocabulary.
    pub task_outcome_vocabulary: Vec<String>,
    /// Provenance state vocabulary.
    pub provenance_state_vocabulary: ProvenanceStateVocabulary,
    /// Percentile method.
    pub percentile_method: PercentileMethod,
    /// Unresolved-label policy descriptions.
    pub unresolved_label_policies: serde_json::Map<String, serde_json::Value>,
    /// Zero-population policy descriptions.
    pub zero_population_policies: serde_json::Map<String, serde_json::Value>,
    /// Safety gate policy.
    pub safety_gate: SafetyGatePolicy,
    /// Aggregate task score definition.
    pub aggregate_task_score: AggregateTaskScoreDefinition,
    /// Verdict rule.
    pub verdict_rule: String,
    /// Safety-critical rubric checks per scenario.
    pub safety_critical_checks: Vec<CheckBinding>,
    /// Metric definitions.
    pub metrics: Vec<MetricDefinition>,
}

impl MetricCatalog {
    /// Parses and validates the catalog compiled into this crate.
    pub fn embedded() -> Result<Self, CatalogError> {
        Self::from_json_str(CATALOG_JSON)
    }

    /// Parses and validates catalog JSON.
    pub fn from_json_str(json: &str) -> Result<Self, CatalogError> {
        let catalog: Self = serde_json::from_str(json)?;
        catalog.validate()?;
        Ok(catalog)
    }

    fn validate(&self) -> Result<(), CatalogError> {
        if self.schema_version != SUPPORTED_SCHEMA_VERSION {
            return Err(CatalogError::UnsupportedSchemaVersion {
                found: self.schema_version,
                expected: SUPPORTED_SCHEMA_VERSION,
            });
        }
        if self.catalog_id != CATALOG_ID {
            return Err(CatalogError::CatalogIdMismatch {
                found: self.catalog_id.clone(),
                expected: CATALOG_ID.to_owned(),
            });
        }
        if self.safety_gate.aggregate_task_score_can_hide_safety {
            return Err(CatalogError::InvalidMetric {
                metric_id: "safety_gate".to_owned(),
                reason: "aggregate_task_score_can_hide_safety must be false".to_owned(),
            });
        }
        self.validate_labels()?;
        self.validate_safety_critical_checks()?;
        let mut seen = BTreeSet::new();
        for metric in &self.metrics {
            if !seen.insert(metric.metric_id) {
                return Err(CatalogError::DuplicateMetric {
                    metric_id: metric.metric_id.to_string(),
                });
            }
            metric.validate()?;
        }
        for metric_id in MetricId::ALL {
            if !seen.contains(&metric_id) {
                return Err(CatalogError::MissingMetric {
                    metric_id: metric_id.to_string(),
                });
            }
        }
        Ok(())
    }

    fn validate_labels(&self) -> Result<(), CatalogError> {
        let vocabulary = &self.label_vocabulary;
        let labels: BTreeSet<&str> = vocabulary.labels.iter().map(String::as_str).collect();
        if labels.len() != vocabulary.labels.len() {
            return Err(CatalogError::InvalidLabelVocabulary(
                "labels repeat".to_owned(),
            ));
        }
        let resolved: BTreeSet<&str> = vocabulary
            .resolved_labels
            .iter()
            .map(String::as_str)
            .collect();
        let unresolved: BTreeSet<&str> = vocabulary
            .unresolved_labels
            .iter()
            .map(String::as_str)
            .collect();
        if !resolved.is_disjoint(&unresolved) {
            return Err(CatalogError::InvalidLabelVocabulary(
                "resolved and unresolved labels overlap".to_owned(),
            ));
        }
        let union: BTreeSet<&str> = resolved.union(&unresolved).copied().collect();
        if union != labels {
            return Err(CatalogError::InvalidLabelVocabulary(
                "resolved and unresolved labels must partition the label set".to_owned(),
            ));
        }
        let expected: BTreeSet<&str> = crate::record::CandidateLabel::ALL
            .iter()
            .map(|label| label.as_str())
            .collect();
        if labels != expected {
            return Err(CatalogError::InvalidLabelVocabulary(format!(
                "catalog labels {labels:?} differ from the typed labels {expected:?}"
            )));
        }
        for label in crate::record::CandidateLabel::ALL {
            let catalog_resolved = resolved.contains(label.as_str());
            if catalog_resolved != label.is_resolved() {
                return Err(CatalogError::InvalidLabelVocabulary(format!(
                    "label {} resolution differs between catalog and code",
                    label.as_str()
                )));
            }
        }
        Ok(())
    }

    fn validate_safety_critical_checks(&self) -> Result<(), CatalogError> {
        let mut scenarios = BTreeSet::new();
        for binding in &self.safety_critical_checks {
            if !scenarios.insert(binding.scenario_id.as_str()) {
                return Err(CatalogError::InvalidSafetyCriticalChecks {
                    scenario_id: binding.scenario_id.clone(),
                    reason: "scenario listed more than once".to_owned(),
                });
            }
            let unique: BTreeSet<&str> = binding.check_ids.iter().map(String::as_str).collect();
            if unique.len() != binding.check_ids.len() || binding.check_ids.is_empty() {
                return Err(CatalogError::InvalidSafetyCriticalChecks {
                    scenario_id: binding.scenario_id.clone(),
                    reason: "check ids must be non-empty and unique".to_owned(),
                });
            }
        }
        Ok(())
    }

    /// Definition of one metric.
    #[must_use]
    pub fn metric(&self, metric_id: MetricId) -> Option<&MetricDefinition> {
        self.metrics
            .iter()
            .find(|metric| metric.metric_id == metric_id)
    }

    /// Scenario ids the catalog binds.
    #[must_use]
    pub fn scenario_ids(&self) -> BTreeSet<&str> {
        self.safety_critical_checks
            .iter()
            .map(|binding| binding.scenario_id.as_str())
            .collect()
    }

    /// Safety-critical check ids of one scenario.
    #[must_use]
    pub fn safety_critical_check_ids(&self, scenario_id: &str) -> &[String] {
        self.safety_critical_checks
            .iter()
            .find(|binding| binding.scenario_id == scenario_id)
            .map_or(&[], |binding| binding.check_ids.as_slice())
    }

    /// Scenario ids bound to a metric through its rubric checks.
    #[must_use]
    pub fn scenarios_bound_to(&self, metric_id: MetricId) -> BTreeSet<&str> {
        self.metric(metric_id)
            .map(|metric| {
                metric
                    .rubric_check_bindings
                    .iter()
                    .map(|binding| binding.scenario_id.as_str())
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl MetricDefinition {
    fn validate(&self) -> Result<(), CatalogError> {
        let reject = |reason: &str| CatalogError::InvalidMetric {
            metric_id: self.metric_id.to_string(),
            reason: reason.to_owned(),
        };
        if self.version == 0 {
            return Err(reject("version must be at least 1"));
        }
        if self.numerator.trim().is_empty() || self.denominator.population.trim().is_empty() {
            return Err(reject(
                "numerator and denominator population must be explicit",
            ));
        }
        if (self.class == MetricClass::Safety) != self.safety_gating {
            return Err(reject(
                "safety-class metrics gate and only safety-class metrics gate",
            ));
        }
        match (self.safety_gating, self.ceiling) {
            (true, Some(ceiling)) if ceiling.is_finite() && ceiling >= 0.0 => {}
            (true, _) => return Err(reject("safety-gating metrics need a finite ceiling")),
            (false, Some(_)) => return Err(reject("non-gating metrics carry no ceiling")),
            (false, None) => {}
        }
        if self.safety_gating && self.direction != Direction::LowerIsBetter {
            return Err(reject("safety-gating metrics are lower-is-better ceilings"));
        }
        if self.inputs.is_empty() {
            return Err(reject("inputs must name at least one runner field"));
        }
        let expected_policy = match self.metric_id {
            MetricId::UsefulRecallPrecision => UnresolvedLabelPolicy::ExcludeAndReport,
            MetricId::HarmfulStaleRecallRate => UnresolvedLabelPolicy::IndeterminateIfAny,
            _ => UnresolvedLabelPolicy::NotLabelBased,
        };
        if self.denominator.unresolved_label_policy != expected_policy {
            return Err(reject(
                "unresolved_label_policy differs from what the code computes",
            ));
        }
        let (per_scenario, per_provider) = self.metric_id.aggregation_modes();
        if self.aggregation.per_scenario != per_scenario
            || self.aggregation.per_provider != per_provider
        {
            return Err(reject(
                "aggregation modes differ from what the code computes",
            ));
        }
        if let Some(applicable) = &self.applicable_scenarios {
            let unique: BTreeSet<&str> = applicable.iter().map(String::as_str).collect();
            if applicable.is_empty() || unique.len() != applicable.len() {
                return Err(reject("applicable_scenarios must be non-empty and unique"));
            }
        }
        let mut scenarios = BTreeSet::new();
        for binding in &self.rubric_check_bindings {
            if !scenarios.insert(binding.scenario_id.as_str()) {
                return Err(reject("rubric_check_bindings list a scenario twice"));
            }
            if binding.check_ids.is_empty() {
                return Err(reject("rubric_check_bindings entries need check ids"));
            }
        }
        Ok(())
    }
}
