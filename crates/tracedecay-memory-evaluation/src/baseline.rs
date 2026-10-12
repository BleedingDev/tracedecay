//! Conversion of a conformance [`BaselineRunOutput`] into metric run records.
//!
//! The baseline runner records mechanical evidence (scope match, forgotten
//! source keys, terminal codes, rubric verdicts, token estimates, timings).
//! It records neither candidate labels nor provenance states nor human
//! measurements; those arrive as [`BaselineAnnotations`] and default to the
//! honest unresolved values (`missing`, unmeasured, not enumerated) when
//! absent. Nothing here infers a value the runner did not produce.

use std::collections::{BTreeMap, BTreeSet};

use tracedecay_memory_conformance::{
    BaselineRunOutput, CheckVerdict, ScenarioBaselineResult, StepOutcome, TokenRecord,
};
use tracedecay_memory_provider_api::ProviderOperation;

use crate::catalog::{MetricCatalog, MetricId};
use crate::error::EvaluationError;
use crate::record::{
    AdmittedCandidate, CandidateLabel, CheckOutcome, CorrectionEvidence, CorruptStateEvidence,
    DiscoveryEvidence, Measured, ProvenanceState, ProviderRunIdentity, ProviderRunRecord,
    RubricCheckResult, ScenarioRunRecord, TaskOutcome, TerminalGateEvidence,
};

/// Rubric check whose pass verdict proves that no admitted candidate came
/// from corrupt provider state. The check is bound to
/// [`MetricId::CorruptStateRecall`] in the catalog.
pub const CORRUPT_RECALL_CHECK_ID: &str = "no_corrupt_recall";

/// Label and provenance of one admitted candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CandidateAnnotation {
    /// Label.
    pub label: CandidateLabel,
    /// Provenance state.
    pub provenance: ProvenanceState,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ScenarioAnnotations {
    candidates: BTreeMap<(String, String), CandidateAnnotation>,
    curation_seconds: Option<u64>,
    correction_latency_micros: Option<u64>,
    discovery: Option<(u64, u64)>,
}

/// Evidence a baseline run cannot record on its own.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BaselineAnnotations {
    scenarios: BTreeMap<String, ScenarioAnnotations>,
}

impl BaselineAnnotations {
    /// No annotations: every candidate is `missing`/`missing`, nothing is measured.
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    /// Labels one admitted candidate of one request.
    pub fn annotate_candidate(
        &mut self,
        scenario_id: &str,
        request_id: &str,
        candidate_ref: &str,
        annotation: CandidateAnnotation,
    ) -> &mut Self {
        self.scenarios
            .entry(scenario_id.to_owned())
            .or_default()
            .candidates
            .insert(
                (request_id.to_owned(), candidate_ref.to_owned()),
                annotation,
            );
        self
    }

    /// Records measured human curation seconds for a scenario.
    pub fn record_curation_seconds(&mut self, scenario_id: &str, seconds: u64) -> &mut Self {
        self.scenarios
            .entry(scenario_id.to_owned())
            .or_default()
            .curation_seconds = Some(seconds);
        self
    }

    /// Records a measured correction latency for a scenario.
    pub fn record_correction_latency_micros(
        &mut self,
        scenario_id: &str,
        micros: u64,
    ) -> &mut Self {
        self.scenarios
            .entry(scenario_id.to_owned())
            .or_default()
            .correction_latency_micros = Some(micros);
        self
    }

    /// Records the required and rediscovered fact counts for a scenario.
    pub fn record_discovery(
        &mut self,
        scenario_id: &str,
        required_facts: u64,
        rediscovered_facts: u64,
    ) -> &mut Self {
        self.scenarios
            .entry(scenario_id.to_owned())
            .or_default()
            .discovery = Some((required_facts, rediscovered_facts));
        self
    }
}

/// Builds the typed provider run record from a baseline run and annotations.
///
/// Every annotation must target a scenario the run executed and a candidate
/// the run admitted; a stray annotation is a typed error, never ignored.
pub fn provider_run_from_baseline(
    catalog: &MetricCatalog,
    output: &BaselineRunOutput,
    annotations: &BaselineAnnotations,
) -> Result<ProviderRunRecord, EvaluationError> {
    let bound = catalog.scenario_ids();
    let ran: BTreeSet<&str> = output
        .report
        .scenarios
        .iter()
        .map(|scenario| scenario.scenario_id.as_str())
        .collect();
    for scenario_id in annotations.scenarios.keys() {
        if !ran.contains(scenario_id.as_str()) {
            return Err(EvaluationError::UnmatchedScenarioAnnotation {
                scenario_id: scenario_id.clone(),
            });
        }
    }

    let corruption_scenarios = catalog
        .metric(MetricId::CorruptStateRecall)
        .and_then(|metric| metric.applicable_scenarios.clone())
        .unwrap_or_default();
    let correction_scenarios = catalog
        .metric(MetricId::CorrectionLatency)
        .and_then(|metric| metric.applicable_scenarios.clone())
        .unwrap_or_default();

    let mut scenarios = Vec::with_capacity(output.report.scenarios.len());
    for scenario in &output.report.scenarios {
        if !bound.contains(scenario.scenario_id.as_str()) {
            return Err(EvaluationError::UnknownScenario {
                scenario_id: scenario.scenario_id.clone(),
            });
        }
        let empty = ScenarioAnnotations::default();
        let scenario_annotations = annotations
            .scenarios
            .get(&scenario.scenario_id)
            .unwrap_or(&empty);
        scenarios.push(scenario_record(
            scenario,
            output,
            scenario_annotations,
            corruption_scenarios
                .iter()
                .any(|id| id == &scenario.scenario_id),
            correction_scenarios
                .iter()
                .any(|id| id == &scenario.scenario_id),
        )?);
    }

    let identity = &output.report.identity;
    Ok(ProviderRunRecord {
        provider: ProviderRunIdentity {
            lane_id: identity.lane.lane_id.clone(),
            provider_id: identity
                .lane
                .provider
                .as_ref()
                .map(|provider| provider.provider_id.clone()),
            run_identity_sha256: Some(identity.run_identity_sha256.clone()),
        },
        scenarios,
    })
}

fn scenario_record(
    scenario: &ScenarioBaselineResult,
    output: &BaselineRunOutput,
    annotations: &ScenarioAnnotations,
    exercises_corruption: bool,
    exercises_correction: bool,
) -> Result<ScenarioRunRecord, EvaluationError> {
    let scenario_id = scenario.scenario_id.as_str();
    let adjudication = &scenario.adjudication;

    let mut candidates = Vec::new();
    let mut admitted_keys = BTreeSet::new();
    let mut observed_terminal_codes = Vec::new();
    let mut recall_latency_micros = Vec::new();
    let recall_wire = ProviderOperation::Recall.as_wire();
    for step in &scenario.steps {
        if let StepOutcome::Terminal { terminal_code, .. } = &step.outcome {
            observed_terminal_codes.push(terminal_code.clone());
        }
        for call in &step.provider_calls {
            observed_terminal_codes.push(call.terminal_code.clone());
            if call.operation == recall_wire && call.provider_contacted {
                recall_latency_micros.extend(
                    output
                        .timings
                        .calls
                        .iter()
                        .filter(|timing| {
                            timing.scenario_id == scenario_id
                                && timing.step == step.step
                                && timing.operation_id == call.operation_id
                        })
                        .map(|timing| timing.latency_micros),
                );
            }
        }
        let Some(admission) = &step.context else {
            continue;
        };
        for entry in &admission.entries {
            let key = (admission.request_id.clone(), entry.source_ref.clone());
            let annotation = annotations.candidates.get(&key).copied();
            admitted_keys.insert(key);
            candidates.push(AdmittedCandidate {
                request_id: admission.request_id.clone(),
                candidate_ref: entry.source_ref.clone(),
                scope_match: entry.scope_match,
                provenance: annotation.map_or(ProvenanceState::Missing, |a| a.provenance),
                label: annotation.map_or(CandidateLabel::Missing, |a| a.label),
                contains_forgotten_source: entry.contains_forgotten_source,
            });
        }
    }
    observed_terminal_codes.sort();
    observed_terminal_codes.dedup();
    for (request_id, candidate_ref) in annotations.candidates.keys() {
        if !admitted_keys.contains(&(request_id.clone(), candidate_ref.clone())) {
            return Err(EvaluationError::UnmatchedAnnotation {
                scenario_id: scenario_id.to_owned(),
                request_id: request_id.clone(),
                candidate_ref: candidate_ref.clone(),
            });
        }
    }

    let rubric_checks: Vec<RubricCheckResult> = adjudication
        .checks
        .iter()
        .map(|check| RubricCheckResult {
            check_id: check.check_id.clone(),
            outcome: match check.verdict {
                CheckVerdict::Pass => CheckOutcome::Pass,
                CheckVerdict::Fail => CheckOutcome::Fail,
                CheckVerdict::Indeterminate => CheckOutcome::Indeterminate,
            },
        })
        .collect();
    let failed: Vec<&str> = rubric_checks
        .iter()
        .filter(|check| check.outcome == CheckOutcome::Fail)
        .map(|check| check.check_id.as_str())
        .collect();
    let indeterminate: Vec<&str> = rubric_checks
        .iter()
        .filter(|check| check.outcome == CheckOutcome::Indeterminate)
        .map(|check| check.check_id.as_str())
        .collect();
    let task_outcome = if !adjudication.terminal_gate.passed || !failed.is_empty() {
        TaskOutcome::Fail
    } else if indeterminate.is_empty() {
        TaskOutcome::Pass
    } else {
        TaskOutcome::Indeterminate {
            reason: format!("indeterminate checks: {}", indeterminate.join(", ")),
        }
    };

    let corrupt_state = if exercises_corruption {
        match adjudication
            .checks
            .iter()
            .find(|check| check.check_id == CORRUPT_RECALL_CHECK_ID)
        {
            Some(check) if check.verdict == CheckVerdict::Pass => {
                CorruptStateEvidence::Enumerated {
                    admitted_from_corrupt_state: 0,
                }
            }
            Some(check) => CorruptStateEvidence::NotEnumerable {
                reason: format!(
                    "check {CORRUPT_RECALL_CHECK_ID} verdict {:?} by evaluator {}: {}",
                    check.verdict, check.evaluator, check.evidence
                ),
            },
            None => CorruptStateEvidence::NotEnumerable {
                reason: format!("check {CORRUPT_RECALL_CHECK_ID} was not adjudicated"),
            },
        }
    } else {
        CorruptStateEvidence::NotExercised
    };

    let correction = if exercises_correction {
        match annotations.correction_latency_micros {
            Some(latency_micros) => CorrectionEvidence::Measured { latency_micros },
            None => CorrectionEvidence::Unmeasured {
                reason: "runner_did_not_measure_correction_latency".to_owned(),
            },
        }
    } else {
        CorrectionEvidence::NotApplicable {
            reason: "scenario has no stale-then-corrected claim".to_owned(),
        }
    };

    Ok(ScenarioRunRecord {
        scenario_id: scenario_id.to_owned(),
        terminal_gate: TerminalGateEvidence {
            passed: adjudication.terminal_gate.passed,
            observed_terminal_codes,
            violations: adjudication.terminal_gate.violations.clone(),
        },
        task_outcome,
        rubric_checks,
        candidates,
        recall_latency_micros,
        context_tokens: match scenario.cost.estimated_tokens {
            TokenRecord::Estimated { tokens } => Measured::Value { value: tokens },
            TokenRecord::Indeterminate => Measured::Unmeasured {
                reason: "no_token_estimator_pinned".to_owned(),
            },
        },
        curation_seconds: match annotations.curation_seconds {
            Some(value) => Measured::Value { value },
            None => Measured::Unmeasured {
                reason: "human_curation_not_measured".to_owned(),
            },
        },
        correction,
        discovery: match annotations.discovery {
            Some((required_facts, rediscovered_facts)) => DiscoveryEvidence::Enumerated {
                required_facts,
                rediscovered_facts,
            },
            None => DiscoveryEvidence::NotEnumerated {
                reason: "runner_did_not_enumerate_required_facts".to_owned(),
            },
        },
        corrupt_state,
    })
}
