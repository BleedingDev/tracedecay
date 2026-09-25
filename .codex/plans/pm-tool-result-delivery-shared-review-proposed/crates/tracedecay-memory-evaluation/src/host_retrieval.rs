//! Source-grounded host retrieval joins over the existing metric records.
//!
//! A caller first validates the matched schedule and host capture ledger. This
//! boundary independently verifies the delivered-byte/candidate/annotation join
//! before calling the existing evaluator. It never performs host admission and
//! never interprets a retrieval-rubric outcome as downstream agent success.

use std::collections::{BTreeMap, BTreeSet};

mod tool_result;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracedecay_memory_conformance::{O200kBaseTokenEstimator, TokenEstimator};

use crate::{
    AdmittedCandidate, CandidateLabel, CheckOutcome, EvaluationError, Measured, MetricCatalog,
    MetricReport, ProviderRunRecord, TaskOutcome, evaluate,
};

/// A defect in a host delivery/metric join.
#[derive(Debug, thiserror::Error)]
pub enum HostRetrievalError {
    /// The capture and metric records disagree or required evidence is absent.
    #[error("invalid host retrieval evidence: {0}")]
    InvalidEvidence(String),
    /// The existing metric evaluator rejected the record/catalog binding.
    #[error(transparent)]
    Evaluation(#[from] EvaluationError),
}

/// One exact delivered section, including its framing and attribution.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HostContextSection {
    /// `canonical`, `advisory`, or ASCII-whitespace-only `framing`.
    pub kind: String,
    /// Exact delivered text; concatenated sections reconstruct the response.
    pub text: String,
    /// Candidate identity for advisory sections.
    pub candidate_ref: Option<String>,
}

/// A source-grounded annotation for a finally delivered candidate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HostCandidateEvidence {
    /// Existing evaluator candidate record.
    pub candidate: AdmittedCandidate,
    /// Exact emitted body, without lossy byte conversion.
    pub content: String,
    /// SHA-256 of the exact emitted UTF-8 body.
    pub content_sha256: String,
    /// Host ledger stages, in returned/admitted/selected/packed/delivered order.
    pub stages: [bool; 5],
    /// Withholding reason required for a candidate not delivered.
    pub withholding_reason: Option<String>,
    /// Frozen source IDs used by the independent retrieval annotation.
    pub source_ids: Vec<String>,
    /// Frozen required-fact evidence references; required for a useful label.
    pub required_fact_refs: Vec<String>,
    /// Reviewer reason, including explicit missing/indeterminate reasons.
    pub annotation_reason: String,
}

/// Exact final rendered context and its complete candidate stage ledger.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HostDeliveredContext {
    /// Absent/`rendered_text_v1` for legacy text; `tool_result_v1` for raw blocks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub representation: Option<String>,
    /// Versioned raw carrier, lexical projections, and observed final joins.
    /// This branch requires empty legacy text/sections/candidates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_result: Option<serde_json::Value>,
    /// Actual host response text, after the final merge.
    #[serde(default)]
    pub final_text: String,
    /// SHA-256 of the final response UTF-8 bytes.
    #[serde(default)]
    pub final_sha256: String,
    /// Must be `tiktoken.o200k_base`.
    pub tokenizer_identity: String,
    /// Must be `tiktoken-rs-0.12`.
    pub tokenizer_revision: String,
    /// Exact rendered-text count, or sum of independently counted decoded blocks.
    pub final_tokens: u64,
    /// Exact canonical-section count, separate from joined-context count.
    pub canonical_tokens: u64,
    /// Exact count of the joined, completely annotated advisory sections.
    pub advisory_tokens: u64,
    /// Sum of individual exact candidate-body counts, separate from joined text.
    pub candidate_body_tokens: u64,
    /// Ordered sections which reconstruct the final bytes.
    #[serde(default)]
    pub sections: Vec<HostContextSection>,
    /// Complete returned-to-delivered ledger, including withheld candidates.
    pub candidates: Vec<HostCandidateEvidence>,
}

/// One scheduled recall; missing delivery remains represented by `None`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HostRecallEvidence {
    /// Frozen scenario ID.
    pub scenario_id: String,
    /// Frozen request ID; unique within its scenario and paired trial.
    pub request_id: String,
    /// Actual final delivered response, never the adapter result.
    pub delivery: Option<HostDeliveredContext>,
    /// Direct host-request span only; kernel/IPC/test-duration spans do not join.
    pub host_latency_micros: Measured<u64>,
}

/// A single host/lane/trial, evaluated separately to preserve lane uniqueness.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HostRetrievalRun {
    /// Existing evaluator record. Its task fields mean retrieval rubric only.
    pub record: ProviderRunRecord,
    /// Every scheduled recall, including unexecuted and censored attempts.
    pub recalls: Vec<HostRecallEvidence>,
}

/// Existing metric report with explicit limits on its interpretation.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct HostRetrievalReport {
    /// Always `host_retrieval_assessment`.
    pub assessment: &'static str,
    /// Always `unmeasured`; no task runner is authorized by this interface.
    pub task_benefit: &'static str,
    /// Meaning of the inherited task/rubric score.
    pub task_outcome_semantics: &'static str,
    /// Token metric boundary; no claim of complete model input is made.
    pub context_token_semantics: &'static str,
    /// The unchanged metric framework's report.
    pub metrics: MetricReport,
}

fn invalid(reason: impl Into<String>) -> HostRetrievalError {
    HostRetrievalError::InvalidEvidence(reason.into())
}

fn bytes_digest(text: &str) -> String {
    Sha256::digest(text.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn exact_tokens(text: &str) -> Result<u64, HostRetrievalError> {
    O200kBaseTokenEstimator
        .estimate_tokens(text.as_bytes())
        .map_err(|error| invalid(error.to_string()))
}

fn delivered_candidates(
    recall: &HostRecallEvidence,
    delivery: &HostDeliveredContext,
    lane: &str,
) -> Result<Vec<AdmittedCandidate>, HostRetrievalError> {
    if delivery.representation.as_deref() == Some("tool_result_v1") {
        return tool_result::validate(recall, delivery, lane);
    }
    if !matches!(
        delivery.representation.as_deref(),
        None | Some("rendered_text_v1")
    ) || delivery.tool_result.is_some()
    {
        return Err(invalid("unknown or mixed delivery representation"));
    }
    let section_text = |kind: &str| {
        delivery
            .sections
            .iter()
            .filter(|section| section.kind == kind)
            .map(|section| section.text.as_str())
            .collect::<String>()
    };
    let body_tokens = delivery
        .candidates
        .iter()
        .filter(|e| e.stages[4])
        .try_fold(0_u64, |sum, e| {
            sum.checked_add(exact_tokens(&e.content)?)
                .ok_or_else(|| invalid("candidate token count overflow"))
        })?;
    if delivery.tokenizer_identity != "tiktoken.o200k_base"
        || delivery.tokenizer_revision != "tiktoken-rs-0.12"
        || bytes_digest(&delivery.final_text) != delivery.final_sha256
        || delivery
            .sections
            .iter()
            .map(|s| s.text.as_str())
            .collect::<String>()
            != delivery.final_text
        || delivery.final_tokens > 128_000
        || delivery.advisory_tokens > 1_024
        || exact_tokens(&delivery.final_text)? != delivery.final_tokens
        || exact_tokens(&section_text("canonical"))? != delivery.canonical_tokens
        || exact_tokens(&section_text("advisory"))? != delivery.advisory_tokens
        || body_tokens != delivery.candidate_body_tokens
    {
        return Err(invalid("final bytes, tokenizer, or context quota mismatch"));
    }
    let mut sections = BTreeMap::new();
    for section in &delivery.sections {
        match section.kind.as_str() {
            "advisory" => {
                let reference = section
                    .candidate_ref
                    .as_deref()
                    .ok_or_else(|| invalid("advisory section lacks candidate"))?;
                if sections.insert(reference, section.text.as_str()).is_some() {
                    return Err(invalid("duplicate advisory section"));
                }
            }
            "canonical" if section.candidate_ref.is_none() => {}
            "framing"
                if section.candidate_ref.is_none()
                    && section.text.bytes().all(|byte| byte.is_ascii_whitespace()) => {}
            _ => return Err(invalid("invalid section attribution")),
        }
    }
    let mut seen = BTreeSet::new();
    let mut result = Vec::new();
    for evidence in &delivery.candidates {
        let candidate = &evidence.candidate;
        if candidate.request_id != recall.request_id
            || !seen.insert(candidate.candidate_ref.as_str())
            || evidence.stages.windows(2).any(|pair| pair[1] && !pair[0])
        {
            return Err(invalid(
                "candidate request, uniqueness, or host-stage mismatch",
            ));
        }
        if !evidence.stages[4] {
            if evidence
                .withholding_reason
                .as_deref()
                .is_none_or(str::is_empty)
            {
                return Err(invalid("withheld candidate lacks host reason"));
            }
            continue;
        }
        let section = sections
            .remove(candidate.candidate_ref.as_str())
            .ok_or_else(|| invalid("metric candidate was not finally delivered"))?;
        if evidence.content.is_empty()
            || section != evidence.content
            || bytes_digest(&evidence.content) != evidence.content_sha256
            || evidence.annotation_reason.is_empty()
            || (candidate.label == CandidateLabel::Useful
                && (evidence.source_ids.is_empty() || evidence.required_fact_refs.is_empty()))
        {
            return Err(invalid(
                "candidate body/digest or source-grounded annotation missing",
            ));
        }
        result.push(candidate.clone());
    }
    if !sections.is_empty() {
        return Err(invalid("delivered section missing from candidate ledger"));
    }
    Ok(result)
}

/// Joins verified post-delivery evidence and evaluates the existing catalog.
///
/// This check cannot authenticate a fixture or assign human relevance labels.
/// Production-host connection evidence and frozen-source adjudication remain
/// prerequisites. A caller cannot supply an adapter candidate as an admission,
/// discard a scheduled recall, or silently substitute backend timing here.
pub fn evaluate_host_retrieval(
    catalog: &MetricCatalog,
    run: &HostRetrievalRun,
) -> Result<HostRetrievalReport, HostRetrievalError> {
    let mut recalls: BTreeMap<&str, Vec<&HostRecallEvidence>> = BTreeMap::new();
    let mut keys = BTreeSet::new();
    let scenario_ids: BTreeSet<_> = run
        .record
        .scenarios
        .iter()
        .map(|s| s.scenario_id.as_str())
        .collect();
    for recall in &run.recalls {
        if !scenario_ids.contains(recall.scenario_id.as_str())
            || !keys.insert((recall.scenario_id.as_str(), recall.request_id.as_str()))
        {
            return Err(invalid("unknown scenario or duplicate scheduled recall"));
        }
        recalls.entry(&recall.scenario_id).or_default().push(recall);
    }
    for scenario in &run.record.scenarios {
        let evidence = recalls
            .get(scenario.scenario_id.as_str())
            .ok_or_else(|| invalid("scenario has no scheduled recalls"))?;
        let mut candidates = Vec::new();
        let mut tokens = Some(0_u64);
        let mut latency = Vec::new();
        let mut missing_delivery = false;
        let mut unresolved_delivery = false;
        for recall in evidence {
            if let Measured::Value { value } = recall.host_latency_micros {
                latency.push(value);
            }
            if let Some(delivery) = &recall.delivery {
                candidates.extend(delivered_candidates(
                    recall,
                    delivery,
                    &run.record.provider.lane_id,
                )?);
                tokens = tokens.and_then(|value| value.checked_add(delivery.final_tokens));
                unresolved_delivery |= tool_result::unresolved(delivery);
            } else {
                missing_delivery = true;
                tokens = None;
            }
        }
        if candidates != scenario.candidates || latency != scenario.recall_latency_micros {
            return Err(invalid(
                "metric candidates/latencies do not equal post-delivery evidence",
            ));
        }
        match (&scenario.context_tokens, tokens) {
            (Measured::Value { value }, Some(actual)) if *value == actual => {}
            (Measured::Unmeasured { reason }, None) if !reason.is_empty() => {}
            _ => {
                return Err(invalid(
                    "metric token count is not the exact final-context count",
                ));
            }
        }
        if (missing_delivery || unresolved_delivery) && scenario.task_outcome == TaskOutcome::Pass {
            return Err(invalid(
                "missing delivery or unresolved final join cannot pass retrieval rubric",
            ));
        }
        if run.record.provider.lane_id == "no_memory" && !candidates.is_empty() {
            return Err(invalid("no-memory lane delivered advisory candidates"));
        }
        if run.record.provider.provider_id.is_some()
            && candidates.is_empty()
            && scenario.rubric_checks.iter().any(|check| {
                check.check_id == "nonvacuous_safety" && check.outcome == CheckOutcome::Pass
            })
        {
            return Err(invalid(
                "zero-admission provider cannot prove nonvacuous safety",
            ));
        }
    }
    Ok(HostRetrievalReport {
        assessment: "host_retrieval_assessment",
        task_benefit: "unmeasured",
        task_outcome_semantics: "frozen source-grounded retrieval rubric; downstream agent success unmeasured",
        context_token_semantics: "rendered_text_v1: exact joined text; tool_result_v1: sum of independently counted ordered decoded text blocks; complete model input unmeasured",
        metrics: evaluate(catalog, &run.record)?,
    })
}
