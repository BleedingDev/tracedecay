//! Behavioral tests for host-owned normalization of admitted recall
//! candidates: a deterministic declared-range projection that keeps the
//! provider's native score visible, orders by the host relevance rather than
//! by provider rank, supports absent stable references and uncalibrated
//! domains, and denies non-finite or malformed scores at admission so they
//! never reach advisory content.
#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

mod recall_fixture;

use std::collections::BTreeMap;
use std::error::Error;
use std::sync::{Arc, Mutex};

use recall_fixture::*;
use serde_json::{Value, json};
use tracedecay_application::memory::{CognitiveRecallRequest, CognitiveRecallResult};
use tracedecay_application::{
    CancellationContext, CancellationSignal, Deadline, RequestId, ResolvedScope, now_micros,
};
use tracedecay_domain::{ProjectId, RefId, RepositoryId, UtcMicros, WorktreeId};
use tracedecay_memory_provider_api::{OwnedExactScope, OwnedProviderId};
use tracedecay_memory_provider_registry::{
    ActiveRoutingPolicy, CognitiveRecallPortInputsV1, EnabledProviderMode, ExactScopeBinding,
    ExactScopeBindingError, FabricConfig, FallbackRule, HOST_NORMALIZATION_POLICY_ID,
    NATIVE_PROVIDER_ID, NativeProviderActivation, NativeScoreDefect, NativeScoreV1,
    NormalizationUnavailableReason, ProjectCognitiveRecallPortV1, ProjectMemoryProviderComposition,
    ProviderLimits, RecallAdmissionAuditError, RecallAdmissionObserver, RecallAdmissionReport,
    RecallCandidateV1, RecallDenialReason, RecallNormalizationPolicyV1, RecallRelevanceV1,
    ScoreCalibrationEvidence, admit_recall_candidates, normalize_admitted_candidates,
};

// --- score and candidate builders ----------------------------------------

/// A well-formed provider score over the declared range `minimum..=maximum`.
fn score(raw: &str, minimum: &str, maximum: &str, direction: &str, calibration: &str) -> Value {
    json!({
        "score_domain_id": "fixture.relevance",
        "score_domain_version": 1,
        "raw_value": raw,
        "direction": direction,
        "declared_minimum": minimum,
        "declared_maximum": maximum,
        "calibration_state": calibration,
        "semantics": "fixture relevance",
        "components": {},
    })
}

/// A calibrated, higher-is-better score on the unit range.
fn unit_score(raw: &str) -> Value {
    score(raw, "0", "1", "higher_is_better", "provider_calibrated")
}

fn scored_candidate(id: &str, native_score: Value) -> RecallCandidateV1 {
    let mut value = candidate_value(
        id,
        &format!("content of {id}"),
        scope_value(&admitted_scope()),
        current_validity(),
    );
    value["native_score"] = native_score;
    decode(value)
}

fn admit(candidates: Vec<RecallCandidateV1>) -> Result<(Vec<String>, Vec<String>), Box<dyn Error>> {
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_exact(),
        candidates,
    )?;
    let normalization =
        normalize_admitted_candidates(RecallNormalizationPolicyV1::default(), &admission.admitted)?;
    Ok((
        normalization
            .candidates
            .iter()
            .map(|candidate| candidate.candidate_id.clone())
            .collect(),
        admission
            .report
            .denied
            .iter()
            .map(|denied| denied.reason.label().to_owned())
            .collect(),
    ))
}

// --- deterministic projection --------------------------------------------

/// The projection is exact over the provider's own declared range: a value at
/// a quarter of a unit range, the same fraction expressed on a shifted range,
/// mixed decimal scales, and half-up rounding of a repeating fraction all
/// produce the pinned six-decimal value. Repeating the whole normalization
/// yields byte-identical output, which is what "deterministic for fixed
/// config" means.
#[test]
fn normalization_projects_the_declared_range_exactly_and_deterministically()
-> Result<(), Box<dyn Error>> {
    let cases = [
        ("quarter-unit", unit_score("0.25"), "0.250000"),
        (
            "shifted-range",
            score("3", "1", "5", "higher_is_better", "provider_calibrated"),
            "0.500000",
        ),
        (
            "mixed-scales",
            score("0.1", "0", "0.4", "higher_is_better", "provider_calibrated"),
            "0.250000",
        ),
        (
            "rounds-down",
            score("1", "0", "3", "higher_is_better", "provider_calibrated"),
            "0.333333",
        ),
        (
            "rounds-up",
            score("2", "0", "3", "higher_is_better", "provider_calibrated"),
            "0.666667",
        ),
        (
            "inverted-direction",
            score("0.25", "0", "1", "lower_is_better", "provider_calibrated"),
            "0.750000",
        ),
        ("range-floor", unit_score("0"), "0.000000"),
        ("range-ceiling", unit_score("1"), "1.000000"),
    ];

    let candidates: Vec<_> = cases
        .iter()
        .map(|(id, native_score, _)| scored_candidate(id, native_score.clone()))
        .collect();
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_exact(),
        candidates,
    )?;
    assert_eq!(admission.admitted.len(), cases.len());

    let policy = RecallNormalizationPolicyV1::default();
    let first = normalize_admitted_candidates(policy, &admission.admitted)?;
    let second = normalize_admitted_candidates(policy, &admission.admitted)?;
    assert_eq!(
        serde_json::to_string(&first)?,
        serde_json::to_string(&second)?,
        "a fixed policy over a fixed reply must produce byte-identical output"
    );
    assert_eq!(first.normalization_policy_id, HOST_NORMALIZATION_POLICY_ID);
    assert!(first.cross_provider_ordering_admissible);

    for (id, _, expected) in cases {
        let candidate = first.candidate(id).expect("normalized candidate");
        let normalized = candidate
            .relevance
            .normalized()
            .expect("normalized relevance");
        assert_eq!(normalized.normalized_value, expected, "{id}");
        assert_eq!(
            normalized.normalization_policy_id,
            HOST_NORMALIZATION_POLICY_ID
        );
        assert_eq!(normalized.normalization_policy_revision, 1);
    }
    Ok(())
}

/// A pinned policy revision travels onto every value it produced, so a stored
/// normalized score can always be traced to the configuration that made it.
#[test]
fn every_normalized_value_carries_the_policy_revision_that_produced_it()
-> Result<(), Box<dyn Error>> {
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_exact(),
        vec![scored_candidate("only", unit_score("0.5"))],
    )?;
    let normalization = normalize_admitted_candidates(
        RecallNormalizationPolicyV1::declared_range_linear(7),
        &admission.admitted,
    )?;
    assert_eq!(normalization.normalization_policy_revision, 7);
    let normalized = normalization.candidates[0]
        .relevance
        .normalized()
        .expect("normalized relevance");
    assert_eq!(normalized.normalization_policy_revision, 7);
    Ok(())
}

// --- native semantics are never erased -----------------------------------

/// Normalization is additive: the provider's score is carried verbatim beside
/// a separately labelled host value, and the host value names the digest of
/// the exact declaration it was derived from. Two candidates that differ only
/// in their raw value must therefore carry different input digests — a
/// normalizer that digested something other than the score it used would pass
/// the value assertions and fail here.
#[test]
fn the_native_score_and_explanation_stay_visible_beside_the_host_value()
-> Result<(), Box<dyn Error>> {
    let low = unit_score("0.25");
    let high = unit_score("0.75");
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_exact(),
        vec![
            scored_candidate("low", low.clone()),
            scored_candidate("high", high.clone()),
        ],
    )?;
    let normalization =
        normalize_admitted_candidates(RecallNormalizationPolicyV1::default(), &admission.admitted)?;

    let declared_low: NativeScoreV1 = serde_json::from_value(low)?;
    let declared_high: NativeScoreV1 = serde_json::from_value(high)?;
    let normalized_low = normalization.candidate("low").expect("low candidate");
    let normalized_high = normalization.candidate("high").expect("high candidate");

    assert_eq!(normalized_low.native_score, declared_low);
    assert_eq!(normalized_high.native_score, declared_high);
    assert_eq!(normalized_low.native_score.raw_value, "0.25");
    assert_eq!(
        normalized_low.explanation_summary.as_deref(),
        Some("fixture match")
    );
    assert_eq!(
        normalized_low.stable_memory_ref.as_deref(),
        Some("memory:low")
    );

    let digest_low = normalized_low.relevance.input_native_score_digest();
    let digest_high = normalized_high.relevance.input_native_score_digest();
    assert_eq!(digest_low.len(), 64);
    assert_ne!(
        digest_low, digest_high,
        "the input digest must bind to the score that was actually normalized"
    );
    Ok(())
}

// --- host ordering -------------------------------------------------------

/// Host order is the normalized relevance, not the provider's rank, and the
/// provider's declared direction decides which raw value is better. The
/// provider hands the candidates over in ascending raw order on a
/// lower-is-better domain, so a normalizer that ignored `direction` — or that
/// simply preserved provider order — would produce the opposite sequence.
/// Provider rank is retained so the reordering stays explainable.
#[test]
fn host_order_follows_normalized_relevance_and_the_declared_direction() -> Result<(), Box<dyn Error>>
{
    let lower_is_better =
        |raw: &str| score(raw, "0", "1", "lower_is_better", "provider_calibrated");
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_exact(),
        vec![
            scored_candidate("worst", lower_is_better("0.9")),
            scored_candidate("middle", lower_is_better("0.5")),
            scored_candidate("best", lower_is_better("0.1")),
        ],
    )?;
    let normalization =
        normalize_admitted_candidates(RecallNormalizationPolicyV1::default(), &admission.admitted)?;

    let order: Vec<_> = normalization
        .candidates
        .iter()
        .map(|candidate| candidate.candidate_id.as_str())
        .collect();
    assert_eq!(order, vec!["best", "middle", "worst"]);
    let ranks: Vec<_> = normalization.host_order().collect();
    assert_eq!(ranks, vec![2, 1, 0], "provider rank must stay recoverable");
    Ok(())
}

/// Equal relevance is broken by candidate id in UTF-8 byte order, never by
/// arrival order, so the same set of ties always packs the same way.
#[test]
fn ties_are_broken_by_candidate_id_not_by_provider_arrival_order() -> Result<(), Box<dyn Error>> {
    let (order, denied) = admit(vec![
        scored_candidate("zulu", unit_score("0.5")),
        scored_candidate("alpha", unit_score("0.5")),
        scored_candidate("mike", unit_score("0.5")),
    ])?;
    assert!(denied.is_empty());
    assert_eq!(order, vec!["alpha", "mike", "zulu"]);
    Ok(())
}

// --- malformed and non-finite scores -------------------------------------

/// Non-finite, exponent, padded, and otherwise non-canonical raw values, an
/// inverted declared range, a raw value outside the provider's own range, and
/// structurally broken score records are all denied at admission with a typed
/// defect. Nothing here is repaired into a neutral relevance, and the denial
/// ledger row carries identity and reason without content.
#[test]
fn non_finite_and_malformed_native_scores_are_denied_at_admission() -> Result<(), Box<dyn Error>> {
    let cases: Vec<(&str, Value, NativeScoreDefect)> = vec![
        (
            "nan",
            unit_score("NaN"),
            NativeScoreDefect::RawValueNotCanonicalDecimal,
        ),
        (
            "infinity",
            unit_score("Infinity"),
            NativeScoreDefect::RawValueNotCanonicalDecimal,
        ),
        (
            "negative-infinity",
            unit_score("-Infinity"),
            NativeScoreDefect::RawValueNotCanonicalDecimal,
        ),
        (
            "exponent",
            unit_score("1e-3"),
            NativeScoreDefect::RawValueNotCanonicalDecimal,
        ),
        (
            "empty",
            unit_score(""),
            NativeScoreDefect::RawValueNotCanonicalDecimal,
        ),
        (
            "padded",
            unit_score(" 0.5"),
            NativeScoreDefect::RawValueNotCanonicalDecimal,
        ),
        (
            "signed",
            unit_score("+0.5"),
            NativeScoreDefect::RawValueNotCanonicalDecimal,
        ),
        (
            "bare-fraction",
            unit_score(".5"),
            NativeScoreDefect::RawValueNotCanonicalDecimal,
        ),
        (
            "trailing-point",
            unit_score("0."),
            NativeScoreDefect::RawValueNotCanonicalDecimal,
        ),
        (
            "leading-zeros",
            unit_score("00.5"),
            NativeScoreDefect::RawValueNotCanonicalDecimal,
        ),
        (
            "nan-minimum",
            score("0.5", "NaN", "1", "higher_is_better", "provider_calibrated"),
            NativeScoreDefect::DeclaredMinimumNotCanonicalDecimal,
        ),
        (
            "nan-maximum",
            score("0.5", "0", "NaN", "higher_is_better", "provider_calibrated"),
            NativeScoreDefect::DeclaredMaximumNotCanonicalDecimal,
        ),
        (
            "inverted-range",
            score("0.5", "1", "0", "higher_is_better", "provider_calibrated"),
            NativeScoreDefect::DeclaredRangeInverted,
        ),
        (
            "above-range",
            score("1.5", "0", "1", "higher_is_better", "provider_calibrated"),
            NativeScoreDefect::RawValueOutOfDeclaredRange,
        ),
        (
            "below-range",
            score("-0.5", "0", "1", "higher_is_better", "provider_calibrated"),
            NativeScoreDefect::RawValueOutOfDeclaredRange,
        ),
    ];

    for (id, native_score, expected) in cases {
        let admission = admit_recall_candidates(
            &admitted_scope(),
            "request",
            &current_query(),
            &authorized_exact(),
            vec![scored_candidate(id, native_score)],
        )?;
        assert!(admission.admitted.is_empty(), "{id} must not be admitted");
        assert_eq!(admission.report.denied.len(), 1, "{id}");
        assert_eq!(
            admission.report.denied[0].reason,
            RecallDenialReason::NativeScoreMalformed {
                defect: expected.clone()
            },
            "{id}"
        );
        assert_eq!(
            admission.report.denied[0].reason.label(),
            "native_score_malformed",
            "{id}"
        );
        assert_eq!(admission.report.denied[0].candidate_id, id);
        let serialized = serde_json::to_string(&admission.report)?;
        assert!(
            !serialized.contains(&format!("content of {id}")),
            "{id} denial ledger must never carry content"
        );
    }
    Ok(())
}

/// Structurally broken score records — an absent score, a missing required
/// field, an unknown field, an unknown enum value, an over-long component
/// map, and an over-precise decimal — are denied too, so a provider cannot
/// smuggle an uninterpretable relevance past the host.
#[test]
fn structurally_invalid_native_scores_are_denied_at_admission() -> Result<(), Box<dyn Error>> {
    let mut over_precise = unit_score("0.1234567890123");
    over_precise["declared_maximum"] = json!("1");
    let mut too_many_components = unit_score("0.5");
    let components: BTreeMap<String, Value> = (0..33)
        .map(|index| (format!("component-{index}"), json!(index)))
        .collect();
    too_many_components["components"] = json!(components);
    let mut unknown_field = unit_score("0.5");
    unknown_field["provider_normalized_value"] = json!("1.0");
    let mut missing_field = unit_score("0.5");
    missing_field
        .as_object_mut()
        .expect("score object")
        .remove("calibration_state");
    let mut unknown_direction = unit_score("0.5");
    unknown_direction["direction"] = json!("closest_is_better");
    let mut blank_domain = unit_score("0.5");
    blank_domain["score_domain_id"] = json!("");

    let cases: Vec<(&str, Value, &str)> = vec![
        ("absent", Value::Null, "undecodable"),
        ("not-an-object", json!("0.5"), "undecodable"),
        ("missing-field", missing_field, "undecodable"),
        ("unknown-field", unknown_field, "undecodable"),
        ("unknown-direction", unknown_direction, "undecodable"),
        ("blank-domain", blank_domain, "score_domain_id_invalid"),
        (
            "too-many-components",
            too_many_components,
            "too_many_components",
        ),
        (
            "over-precise",
            over_precise,
            "raw_value_not_canonical_decimal",
        ),
    ];

    for (id, native_score, expected_label) in cases {
        let admission = admit_recall_candidates(
            &admitted_scope(),
            "request",
            &current_query(),
            &authorized_exact(),
            vec![scored_candidate(id, native_score)],
        )?;
        assert!(admission.admitted.is_empty(), "{id} must not be admitted");
        assert_eq!(admission.report.denied.len(), 1, "{id}");
        let RecallDenialReason::NativeScoreMalformed { defect } =
            &admission.report.denied[0].reason
        else {
            panic!("{id} must be denied as a malformed native score");
        };
        assert_eq!(defect.label(), expected_label, "{id}");
    }
    Ok(())
}

// --- absent optional inputs ----------------------------------------------

/// A candidate without a stable provider reference, without an explanation
/// summary, and without any calibration claim is fully supported: it is
/// admitted, it is normalized, and the missing calibration is recorded as
/// explicit evidence that forbids cross-provider ordering instead of being
/// silently treated as calibrated relevance.
#[test]
fn absent_stable_reference_and_absent_calibration_are_supported() -> Result<(), Box<dyn Error>> {
    let mut value = candidate_value(
        "sparse",
        "content of sparse",
        scope_value(&admitted_scope()),
        current_validity(),
    );
    value["stable_memory_ref"] = Value::Null;
    value["native_score"] = score("0.5", "0", "1", "higher_is_better", "uncalibrated");
    value["explanation"] = json!({
        "summary": "",
        "matched_features": [],
        "activation_trace_refs": [],
        "limitations": [],
    });

    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_exact(),
        vec![decode(value)],
    )?;
    assert_eq!(admission.admitted.len(), 1);
    let normalization =
        normalize_admitted_candidates(RecallNormalizationPolicyV1::default(), &admission.admitted)?;

    let candidate = normalization.candidate("sparse").expect("sparse candidate");
    assert!(candidate.stable_memory_ref.is_none());
    assert!(candidate.explanation_summary.is_none());
    let normalized = candidate
        .relevance
        .normalized()
        .expect("uncalibrated scores are still normalized");
    assert_eq!(normalized.normalized_value, "0.500000");
    assert_eq!(
        normalized.calibration_evidence,
        ScoreCalibrationEvidence::DeclaredRangeOnly
    );
    assert!(
        !normalized.warnings.is_empty(),
        "an uncalibrated projection must say so"
    );
    assert!(
        !normalization.cross_provider_ordering_admissible,
        "an uncalibrated input must not license cross-provider ordering"
    );
    assert!(!normalization.warnings.is_empty());
    Ok(())
}

/// A single-point declared range leaves no relative relevance. The candidate
/// keeps its native score, is explicitly marked unavailable rather than
/// scored as a neutral value, holds provider order behind every normalized
/// candidate, and takes cross-provider ordering off the table for the set.
#[test]
fn a_degenerate_declared_range_retains_the_native_score_without_a_value()
-> Result<(), Box<dyn Error>> {
    let flat = score("2", "2", "2", "higher_is_better", "provider_calibrated");
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_exact(),
        vec![
            scored_candidate("flat", flat),
            scored_candidate("ranked", unit_score("0.1")),
        ],
    )?;
    assert_eq!(admission.admitted.len(), 2);
    let normalization =
        normalize_admitted_candidates(RecallNormalizationPolicyV1::default(), &admission.admitted)?;

    let order: Vec<_> = normalization
        .candidates
        .iter()
        .map(|candidate| candidate.candidate_id.as_str())
        .collect();
    assert_eq!(
        order,
        vec!["ranked", "flat"],
        "an unnormalizable candidate must never outrank a normalized one"
    );
    let flat = normalization.candidate("flat").expect("flat candidate");
    assert_eq!(flat.native_score.raw_value, "2");
    let RecallRelevanceV1::Unavailable {
        reason, warnings, ..
    } = &flat.relevance
    else {
        panic!("a degenerate range has no normalized value");
    };
    assert_eq!(
        *reason,
        NormalizationUnavailableReason::DegenerateDeclaredRange
    );
    assert!(!warnings.is_empty());
    assert!(!flat.relevance.input_native_score_digest().is_empty());
    assert!(!normalization.cross_provider_ordering_admissible);
    Ok(())
}

// --- mounted port path ---------------------------------------------------

struct TestScopeBinding;

impl ExactScopeBinding for TestScopeBinding {
    fn bind_exact_scope(
        &self,
        scope: &ResolvedScope,
    ) -> Result<OwnedExactScope, ExactScopeBindingError> {
        let reference = scope.reference.as_ref().ok_or_else(|| {
            ExactScopeBindingError::ReferenceUnavailable {
                project_id: scope.project_id.as_str().to_owned(),
            }
        })?;
        Ok(OwnedExactScope::new(
            "profile-recall",
            scope.project_id.as_str(),
            scope.repository_id.as_str(),
            scope.worktree_id.as_str(),
            reference.as_str(),
            "session-recall",
            scope.scope_digest.as_str(),
        )?)
    }
}

#[derive(Default)]
struct LedgerObserver(Mutex<Vec<RecallAdmissionReport>>);

impl RecallAdmissionObserver for LedgerObserver {
    fn observe_admission(
        &self,
        report: &RecallAdmissionReport,
    ) -> Result<(), RecallAdmissionAuditError> {
        self.0.lock().expect("ledger lock").push(report.clone());
        Ok(())
    }
}

fn mount_port(
    provider: Arc<RecallFixturePort>,
    observer: Arc<LedgerObserver>,
) -> ProjectCognitiveRecallPortV1 {
    let composition = Arc::new(
        ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
            fabric_config: FabricConfig {
                max_registered_providers: 1,
                max_in_flight: 2,
            },
            port: provider,
            registration_revision: 31,
            mode: EnabledProviderMode::Active,
        })
        .expect("enabled composition"),
    );
    ProjectCognitiveRecallPortV1::mount(CognitiveRecallPortInputsV1 {
        invocation_boundary: test_invocation_boundary(),
        composition,
        scope_binding: Arc::new(TestScopeBinding),
        admission_observer: observer,
        routing: ActiveRoutingPolicy::new(
            OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("provider id"),
            31,
            FallbackRule::Forbidden,
        )
        .expect("routing policy"),
        host_limits: port_limits(),
        policy_revision: 1,
        budgets: budgets(),
    })
    .expect("mounted port")
}

fn port_limits() -> ProviderLimits {
    limits()
}

const RECALL_TOKEN: &str = "token.recall-normalization";

fn cancellation_signal() -> CancellationSignal {
    CancellationSignal::active(RECALL_TOKEN).expect("active signal")
}

fn port_request() -> CognitiveRecallRequest {
    let now = now_micros();
    CognitiveRecallRequest::new(
        ResolvedScope::new(
            ProjectId::new("project.recall-port").expect("project id"),
            RepositoryId::new("repository.recall-port").expect("repository id"),
            WorktreeId::new("worktree.recall-port").expect("worktree id"),
            Some(RefId::new("refs/heads/recall-port").expect("reference id")),
        )
        .expect("resolved scope"),
        RequestId::new("request.recall-normalization").expect("request id"),
        Deadline::new(UtcMicros(now.0.saturating_add(60_000_000))).expect("deadline"),
        CancellationContext::active(RECALL_TOKEN).expect("active context"),
        "recall normalization",
        8,
    )
    .expect("recall request")
}

fn delivered_ids(result: &CognitiveRecallResult) -> Vec<String> {
    result
        .candidates()
        .iter()
        .map(|candidate| candidate.candidate_id().to_owned())
        .collect()
}

/// The mounted port delivers admitted candidates in host-normalized order,
/// not in provider order. The fixture provider returns `in-scope-1` before
/// `in-scope-2` on a lower-is-better domain where `in-scope-2` is the better
/// answer, so a port that forwarded provider order — or that ignored the
/// declared direction — would deliver the worse candidate first through the
/// real fabric, adapter, admission, and application result.
#[tokio::test]
async fn the_mounted_port_delivers_candidates_in_host_normalized_order() {
    let mut provider = RecallFixturePort::new();
    provider.native_score_overrides.insert(
        "in-scope-1".to_owned(),
        score("0.9", "0", "1", "lower_is_better", "provider_calibrated"),
    );
    provider.native_score_overrides.insert(
        "in-scope-2".to_owned(),
        score("0.1", "0", "1", "lower_is_better", "provider_calibrated"),
    );
    let observer = Arc::new(LedgerObserver::default());
    let port = mount_port(Arc::new(provider), observer.clone());

    let cancellation = cancellation_signal();
    let outcome = port
        .recall_admitted(port_request(), &cancellation)
        .await
        .expect("bridged recall");

    assert_eq!(
        delivered_ids(&outcome.result),
        vec!["in-scope-2", "in-scope-1"]
    );
    let normalization = outcome.normalization.expect("normalization evidence");
    assert_eq!(
        normalization
            .candidate("in-scope-2")
            .expect("better candidate")
            .relevance
            .normalized()
            .expect("normalized relevance")
            .normalized_value,
        "0.900000"
    );
    assert_eq!(
        normalization
            .candidate("in-scope-1")
            .expect("worse candidate")
            .relevance
            .normalized()
            .expect("normalized relevance")
            .normalized_value,
        "0.100000"
    );
    assert_eq!(
        normalization
            .candidate("in-scope-1")
            .expect("worse candidate")
            .native_score
            .raw_value,
        "0.9",
        "the provider's own score must survive the reordering"
    );
    assert_eq!(normalization.host_order().collect::<Vec<_>>(), vec![1, 0]);
}

/// A non-finite provider score is denied on the mounted path: the candidate
/// never reaches the application result, its denial is retained by the audit
/// observer with a typed reason, and the surviving candidate is still
/// delivered.
#[tokio::test]
async fn the_mounted_port_denies_a_non_finite_provider_score() {
    let mut provider = RecallFixturePort::new();
    provider
        .native_score_overrides
        .insert("in-scope-1".to_owned(), unit_score("NaN"));
    let observer = Arc::new(LedgerObserver::default());
    let port = mount_port(Arc::new(provider), observer.clone());

    let cancellation = cancellation_signal();
    let outcome = port
        .recall_admitted(port_request(), &cancellation)
        .await
        .expect("bridged recall");

    assert_eq!(delivered_ids(&outcome.result), vec!["in-scope-2"]);
    let report = outcome.report.expect("admission report");
    let denied: Vec<_> = report
        .denied
        .iter()
        .filter(|denied| denied.candidate_id == "in-scope-1")
        .collect();
    assert_eq!(denied.len(), 1);
    assert_eq!(denied[0].reason.label(), "native_score_malformed");
    let retained = observer.0.lock().expect("ledger lock");
    assert_eq!(retained.len(), 1);
    assert!(
        retained[0]
            .denied
            .iter()
            .any(|denied| denied.candidate_id == "in-scope-1"),
        "the denial must be audit-visible"
    );
}
