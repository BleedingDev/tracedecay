//! Metric computation over typed run records.

use std::collections::{BTreeMap, BTreeSet};

use tracedecay_memory_provider_api::contract::TerminalCode;

use crate::catalog::{MetricCatalog, MetricDefinition, MetricId, UnresolvedLabelPolicy};
use crate::error::EvaluationError;
use crate::record::{
    AdmittedCandidate, CandidateLabel, CheckOutcome, CorrectionEvidence, CorruptStateEvidence,
    DiscoveryEvidence, Measured, ProviderRunRecord, ScenarioRunRecord, TaskOutcome,
};
use crate::report::{
    AggregateTaskScore, LabelCounts, MetricReport, MetricResult, MetricValue, REPORT_FORMAT,
    SafetyFailure, SafetyGate, ScenarioMetricReport, Verdict,
};

/// Nearest-rank percentile: the sample at one-based rank `ceil(p / 100 * n)`.
///
/// Returns `None` for zero samples or a percentile outside `1..=100`; timings
/// are never fabricated.
#[must_use]
pub fn nearest_rank_percentile(samples: &[u64], percentile: u64) -> Option<u64> {
    if samples.is_empty() || percentile == 0 || percentile > 100 {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let count = sorted.len() as u64;
    let rank = (percentile * count).div_ceil(100).max(1);
    sorted.get((rank - 1) as usize).copied()
}

/// Computes the metric report for one provider run under a catalog.
pub fn evaluate(
    catalog: &MetricCatalog,
    run: &ProviderRunRecord,
) -> Result<MetricReport, EvaluationError> {
    if run.scenarios.is_empty() {
        return Err(EvaluationError::NoScenarios);
    }
    let bound = catalog.scenario_ids();
    let mut seen = BTreeSet::new();
    for scenario in &run.scenarios {
        validate_scenario(scenario, &bound, &mut seen)?;
    }

    let mut scenario_reports = Vec::with_capacity(run.scenarios.len());
    let mut failures = Vec::new();
    for scenario in &run.scenarios {
        let report = evaluate_scenario(catalog, scenario);
        failures.extend(report.safety_failures.iter().cloned());
        scenario_reports.push(report);
    }

    let mut provider_metrics = BTreeMap::new();
    for definition in &catalog.metrics {
        provider_metrics.insert(
            definition.metric_id,
            pool_metric(definition, run, &scenario_reports),
        );
    }
    let aggregate_task_score = aggregate_task_score(&run.scenarios);
    let safety_gate = SafetyGate {
        passed: failures.is_empty(),
        failures,
    };
    let verdict = if safety_gate.passed {
        Verdict::Pass
    } else {
        Verdict::Fail
    };
    Ok(MetricReport {
        report_format: REPORT_FORMAT.to_owned(),
        catalog_id: catalog.catalog_id.clone(),
        catalog_version: catalog.catalog_version,
        label_vocabulary_version: catalog.label_vocabulary.version,
        corpus_id: catalog.corpus_binding.corpus_id.clone(),
        provider: run.provider.clone(),
        scenarios: scenario_reports,
        provider_metrics,
        aggregate_task_score,
        safety_gate,
        verdict,
    })
}

fn validate_scenario(
    scenario: &ScenarioRunRecord,
    bound: &BTreeSet<&str>,
    seen: &mut BTreeSet<String>,
) -> Result<(), EvaluationError> {
    let scenario_id = scenario.scenario_id.clone();
    if !bound.contains(scenario_id.as_str()) {
        return Err(EvaluationError::UnknownScenario { scenario_id });
    }
    if !seen.insert(scenario_id.clone()) {
        return Err(EvaluationError::DuplicateScenario { scenario_id });
    }
    for code in &scenario.terminal_gate.observed_terminal_codes {
        if TerminalCode::from_wire(code).is_none() {
            return Err(EvaluationError::UnknownTerminalCode {
                scenario_id,
                terminal_code: code.clone(),
            });
        }
    }
    let mut checks = BTreeSet::new();
    for check in &scenario.rubric_checks {
        if !checks.insert(check.check_id.as_str()) {
            return Err(EvaluationError::DuplicateCheck {
                scenario_id,
                check_id: check.check_id.clone(),
            });
        }
    }
    if let DiscoveryEvidence::Enumerated {
        required_facts,
        rediscovered_facts,
    } = scenario.discovery
        && rediscovered_facts > required_facts
    {
        return Err(EvaluationError::DiscoveryExceedsRequired {
            scenario_id,
            required: required_facts,
            rediscovered: rediscovered_facts,
        });
    }
    Ok(())
}

fn evaluate_scenario(
    catalog: &MetricCatalog,
    scenario: &ScenarioRunRecord,
) -> ScenarioMetricReport {
    let mut metrics = BTreeMap::new();
    let mut safety_failures = Vec::new();
    let scenario_id = scenario.scenario_id.as_str();

    if !scenario.terminal_gate.passed {
        safety_failures.push(SafetyFailure::TerminalGateFailed {
            scenario_id: scenario_id.to_owned(),
            violations: scenario.terminal_gate.violations.clone(),
        });
    }
    for check_id in catalog.safety_critical_check_ids(scenario_id) {
        match scenario
            .rubric_checks
            .iter()
            .find(|check| &check.check_id == check_id)
        {
            Some(check) if check.outcome == CheckOutcome::Pass => {}
            Some(check) => safety_failures.push(SafetyFailure::SafetyCriticalCheckNotPassed {
                scenario_id: scenario_id.to_owned(),
                check_id: check_id.clone(),
                outcome: check.outcome,
            }),
            None => safety_failures.push(SafetyFailure::SafetyCriticalCheckMissing {
                scenario_id: scenario_id.to_owned(),
                check_id: check_id.clone(),
            }),
        }
    }

    for definition in &catalog.metrics {
        let (value, label_counts) = compute_metric(definition, scenario);
        if definition.safety_gating {
            match (&value, definition.ceiling) {
                (MetricValue::Indeterminate { reason }, _) => {
                    safety_failures.push(SafetyFailure::MetricIndeterminate {
                        scenario_id: scenario_id.to_owned(),
                        metric_id: definition.metric_id,
                        reason: reason.clone(),
                    });
                }
                (value, Some(ceiling)) => {
                    if let Some(numeric) = value.numeric()
                        && numeric > ceiling
                    {
                        safety_failures.push(SafetyFailure::MetricExceededCeiling {
                            scenario_id: scenario_id.to_owned(),
                            metric_id: definition.metric_id,
                            value: numeric,
                            ceiling,
                        });
                    }
                }
                (_, None) => {}
            }
        }
        metrics.insert(
            definition.metric_id,
            MetricResult {
                metric_id: definition.metric_id,
                version: definition.version,
                class: definition.class,
                determinism: definition.determinism,
                safety_gating: definition.safety_gating,
                ceiling: definition.ceiling,
                value,
                label_counts,
            },
        );
    }

    ScenarioMetricReport {
        scenario_id: scenario_id.to_owned(),
        metrics,
        safety_failures,
    }
}

fn label_counts(candidates: &[AdmittedCandidate]) -> LabelCounts {
    let mut counts = LabelCounts::default();
    for candidate in candidates {
        match candidate.label {
            CandidateLabel::Missing => counts.unlabeled += 1,
            CandidateLabel::Indeterminate => counts.indeterminate += 1,
            _ => counts.labeled += 1,
        }
    }
    counts
}

fn not_applicable_or_indeterminate(definition: &MetricDefinition, reason: &str) -> MetricValue {
    match definition.denominator.zero_population_policy {
        crate::catalog::ZeroPopulationPolicy::NotApplicable => MetricValue::NotApplicable {
            reason: reason.to_owned(),
        },
        crate::catalog::ZeroPopulationPolicy::Indeterminate => MetricValue::Indeterminate {
            reason: reason.to_owned(),
        },
    }
}

fn label_ratio(
    definition: &MetricDefinition,
    candidates: &[AdmittedCandidate],
    counts_as_numerator: fn(CandidateLabel) -> bool,
) -> (MetricValue, Option<LabelCounts>) {
    let counts = label_counts(candidates);
    if candidates.is_empty() {
        return (
            not_applicable_or_indeterminate(definition, "no admitted candidates"),
            Some(counts),
        );
    }
    let numerator = candidates
        .iter()
        .filter(|candidate| counts_as_numerator(candidate.label))
        .count() as u64;
    let value = match definition.denominator.unresolved_label_policy {
        UnresolvedLabelPolicy::ExcludeAndReport => {
            if counts.labeled == 0 {
                MetricValue::Indeterminate {
                    reason: format!(
                        "all {} admitted candidates are unresolved ({} unlabeled, {} indeterminate)",
                        counts.total(),
                        counts.unlabeled,
                        counts.indeterminate
                    ),
                }
            } else {
                MetricValue::ratio(numerator, counts.labeled)
            }
        }
        UnresolvedLabelPolicy::IndeterminateIfAny => {
            if counts.unresolved() > 0 {
                MetricValue::Indeterminate {
                    reason: format!(
                        "{} of {} admitted candidates are unresolved ({} unlabeled, {} indeterminate)",
                        counts.unresolved(),
                        counts.total(),
                        counts.unlabeled,
                        counts.indeterminate
                    ),
                }
            } else {
                MetricValue::ratio(numerator, counts.total())
            }
        }
        UnresolvedLabelPolicy::NotLabelBased => MetricValue::ratio(numerator, counts.total()),
    };
    (value, Some(counts))
}

fn mechanical_ratio(
    definition: &MetricDefinition,
    candidates: &[AdmittedCandidate],
    counts_as_numerator: fn(&AdmittedCandidate) -> bool,
) -> MetricValue {
    if candidates.is_empty() {
        return not_applicable_or_indeterminate(definition, "no admitted candidates");
    }
    let numerator = candidates.iter().filter(|c| counts_as_numerator(c)).count() as u64;
    MetricValue::ratio(numerator, candidates.len() as u64)
}

fn measured_quantity(measured: &Measured<u64>) -> MetricValue {
    match measured {
        Measured::Value { value } => MetricValue::Quantity {
            value: *value as f64,
            samples: 1,
        },
        Measured::Unmeasured { reason } => MetricValue::Indeterminate {
            reason: reason.clone(),
        },
    }
}

fn percentile_value(samples: &[u64], percentile: u64) -> MetricValue {
    match nearest_rank_percentile(samples, percentile) {
        Some(value) => MetricValue::Quantity {
            value: value as f64,
            samples: samples.len() as u64,
        },
        None => MetricValue::Indeterminate {
            reason: "no_samples".to_owned(),
        },
    }
}

fn compute_metric(
    definition: &MetricDefinition,
    scenario: &ScenarioRunRecord,
) -> (MetricValue, Option<LabelCounts>) {
    let candidates = scenario.candidates.as_slice();
    let applicable = definition
        .applicable_scenarios
        .as_ref()
        .is_none_or(|scenarios| scenarios.iter().any(|id| id == &scenario.scenario_id));
    if !applicable {
        return (
            MetricValue::NotApplicable {
                reason: "scenario does not exercise this metric".to_owned(),
            },
            None,
        );
    }
    match definition.metric_id {
        MetricId::TaskOutcome => {
            let value = match &scenario.task_outcome {
                TaskOutcome::Pass => MetricValue::ratio(1, 1),
                TaskOutcome::Fail => MetricValue::ratio(0, 1),
                TaskOutcome::Indeterminate { reason } => MetricValue::Indeterminate {
                    reason: reason.clone(),
                },
            };
            (value, None)
        }
        MetricId::UsefulRecallPrecision => label_ratio(definition, candidates, |label| {
            label == CandidateLabel::Useful
        }),
        MetricId::HarmfulStaleRecallRate => label_ratio(definition, candidates, |label| {
            matches!(label, CandidateLabel::Harmful | CandidateLabel::Stale)
        }),
        MetricId::CorrectionLatency => {
            let value = match &scenario.correction {
                CorrectionEvidence::NotApplicable { reason } => MetricValue::NotApplicable {
                    reason: reason.clone(),
                },
                CorrectionEvidence::Measured { latency_micros } => MetricValue::Quantity {
                    value: *latency_micros as f64,
                    samples: 1,
                },
                CorrectionEvidence::Unmeasured { reason } => MetricValue::Indeterminate {
                    reason: reason.clone(),
                },
            };
            (value, None)
        }
        MetricId::RepeatedDiscoveryRate => {
            let value = match &scenario.discovery {
                DiscoveryEvidence::NotEnumerated { reason } => MetricValue::Indeterminate {
                    reason: reason.clone(),
                },
                DiscoveryEvidence::Enumerated {
                    required_facts: 0, ..
                } => not_applicable_or_indeterminate(definition, "no required facts enumerated"),
                DiscoveryEvidence::Enumerated {
                    required_facts,
                    rediscovered_facts,
                } => MetricValue::ratio(*rediscovered_facts, *required_facts),
            };
            (value, None)
        }
        MetricId::ContextTokens => (measured_quantity(&scenario.context_tokens), None),
        MetricId::RecallLatencyP50 => (percentile_value(&scenario.recall_latency_micros, 50), None),
        MetricId::RecallLatencyP95 => (percentile_value(&scenario.recall_latency_micros, 95), None),
        MetricId::HumanCurationTime => (measured_quantity(&scenario.curation_seconds), None),
        MetricId::ProvenanceCompleteness => (
            mechanical_ratio(definition, candidates, |c| c.provenance.is_complete()),
            None,
        ),
        MetricId::ScopeLeakage => (
            mechanical_ratio(definition, candidates, |c| !c.scope_match),
            None,
        ),
        MetricId::CorruptStateRecall => {
            let value = match &scenario.corrupt_state {
                CorruptStateEvidence::NotExercised => MetricValue::NotApplicable {
                    reason: "scenario does not load corrupt provider state".to_owned(),
                },
                CorruptStateEvidence::Enumerated {
                    admitted_from_corrupt_state,
                } => MetricValue::Quantity {
                    value: *admitted_from_corrupt_state as f64,
                    samples: 1,
                },
                CorruptStateEvidence::NotEnumerable { reason } => MetricValue::Indeterminate {
                    reason: reason.clone(),
                },
            };
            (value, None)
        }
        MetricId::DeletedSourceRecall => (
            mechanical_ratio(definition, candidates, |c| c.contains_forgotten_source),
            None,
        ),
    }
}

fn aggregate_task_score(scenarios: &[ScenarioRunRecord]) -> Option<AggregateTaskScore> {
    let mut passes = 0u64;
    let mut resolved = 0u64;
    let mut indeterminate = 0u64;
    for scenario in scenarios {
        match scenario.task_outcome {
            TaskOutcome::Pass => {
                passes += 1;
                resolved += 1;
            }
            TaskOutcome::Fail => resolved += 1,
            TaskOutcome::Indeterminate { .. } => indeterminate += 1,
        }
    }
    (resolved > 0).then(|| AggregateTaskScore {
        value: passes as f64 / resolved as f64,
        resolved_scenarios: resolved,
        indeterminate_scenarios: indeterminate,
    })
}

/// Per-provider pooling of one metric over the scenario reports.
fn pool_metric(
    definition: &MetricDefinition,
    run: &ProviderRunRecord,
    scenarios: &[ScenarioMetricReport],
) -> MetricResult {
    let results: Vec<&MetricResult> = scenarios
        .iter()
        .filter_map(|scenario| scenario.metrics.get(&definition.metric_id))
        .collect();
    let mut counts = LabelCounts::default();
    let mut has_counts = false;
    for result in &results {
        if let Some(label_counts) = result.label_counts {
            counts.add(label_counts);
            has_counts = true;
        }
    }
    let indeterminate_scenarios: Vec<&str> = results
        .iter()
        .zip(scenarios)
        .filter(|(result, _)| result.value.is_indeterminate())
        .map(|(_, scenario)| scenario.scenario_id.as_str())
        .collect();
    let tolerate_indeterminate = matches!(
        definition.metric_id,
        MetricId::TaskOutcome | MetricId::RecallLatencyP50 | MetricId::RecallLatencyP95
    ) || definition.denominator.unresolved_label_policy
        == UnresolvedLabelPolicy::ExcludeAndReport;

    let value = if !indeterminate_scenarios.is_empty() && !tolerate_indeterminate {
        MetricValue::Indeterminate {
            reason: format!(
                "indeterminate in scenarios: {}",
                indeterminate_scenarios.join(", ")
            ),
        }
    } else {
        match definition.aggregation.per_provider.as_str() {
            "pooled_ratio" => pool_ratio(&results, &indeterminate_scenarios),
            "sum" => pool_quantities(&results, |values, samples| MetricValue::Quantity {
                value: values.iter().sum(),
                samples,
            }),
            "mean" => pool_quantities(&results, |values, samples| MetricValue::Quantity {
                value: values.iter().sum::<f64>() / values.len() as f64,
                samples,
            }),
            "pooled_nearest_rank_percentile" => match definition.metric_id {
                MetricId::RecallLatencyP95 => pooled_latency(run, 95),
                _ => pooled_latency(run, 50),
            },
            other => MetricValue::Indeterminate {
                reason: format!("unsupported per_provider aggregation {other:?}"),
            },
        }
    };
    MetricResult {
        metric_id: definition.metric_id,
        version: definition.version,
        class: definition.class,
        determinism: definition.determinism,
        safety_gating: definition.safety_gating,
        ceiling: definition.ceiling,
        value,
        label_counts: has_counts.then_some(counts),
    }
}

fn pool_ratio(results: &[&MetricResult], indeterminate_scenarios: &[&str]) -> MetricValue {
    let mut numerator = 0u64;
    let mut denominator = 0u64;
    for result in results {
        if let MetricValue::Ratio {
            numerator: n,
            denominator: d,
            ..
        } = result.value
        {
            numerator += n;
            denominator += d;
        }
    }
    if denominator == 0 {
        if indeterminate_scenarios.is_empty() {
            MetricValue::NotApplicable {
                reason: "no scenario had a population".to_owned(),
            }
        } else {
            MetricValue::Indeterminate {
                reason: format!(
                    "no resolved population; indeterminate in scenarios: {}",
                    indeterminate_scenarios.join(", ")
                ),
            }
        }
    } else {
        MetricValue::ratio(numerator, denominator)
    }
}

fn pool_quantities(results: &[&MetricResult], fold: fn(&[f64], u64) -> MetricValue) -> MetricValue {
    let mut values = Vec::new();
    let mut samples = 0u64;
    for result in results {
        if let MetricValue::Quantity { value, samples: s } = result.value {
            values.push(value);
            samples += s;
        }
    }
    if values.is_empty() {
        MetricValue::NotApplicable {
            reason: "no scenario measured this metric".to_owned(),
        }
    } else {
        fold(&values, samples)
    }
}

/// Pools raw recall latency samples of every scenario into one percentile.
fn pooled_latency(run: &ProviderRunRecord, percentile: u64) -> MetricValue {
    let samples: Vec<u64> = run
        .scenarios
        .iter()
        .flat_map(|scenario| scenario.recall_latency_micros.iter().copied())
        .collect();
    percentile_value(&samples, percentile)
}
