//! Behavioral tests for host-owned exact deduplication and budget selection over
//! normalized recall candidates: duplicate candidates never consume repeated
//! budget, distinct positive/negative evidence is never collapsed on wording
//! alone, selection is deterministic and fully explainable, and no
//! provider-specific ID shape is assumed.
#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

mod recall_fixture;

use std::error::Error;

use recall_fixture::*;
use serde_json::{Value, json};
use tracedecay_memory_provider_registry::{
    BudgetExclusionReason, DuplicateReason, RecallCandidateV1, RecallSelectionError,
    RecallSelectionPolicyError, RecallSelectionPolicyV1, RecallSelectionV1,
    admit_recall_candidates, normalize_admitted_candidates, select_recall_candidates,
};

/// Builds one candidate with explicit content and an optional stable
/// reference override (`None` keeps the fixture's per-candidate default,
/// which is always unique).
fn candidate_with(id: &str, content: &str, stable_memory_ref: Option<&str>) -> RecallCandidateV1 {
    let mut value = candidate_value(
        id,
        content,
        scope_value(&admitted_scope()),
        current_validity(),
    );
    if let Some(reference) = stable_memory_ref {
        value["stable_memory_ref"] = json!(reference);
    }
    decode(value)
}

/// Admits, normalizes, and selects over `candidates` with a selection budget
/// of `maximum_selected`, returning selected ids in final order alongside the
/// deduplication ledger.
#[allow(clippy::type_complexity)]
fn select(
    candidates: Vec<RecallCandidateV1>,
    maximum_selected: usize,
) -> Result<(Vec<String>, Vec<(String, String, DuplicateReason)>), Box<dyn Error>> {
    let (selection, all_candidate_ids) = selection_of(candidates, maximum_selected)?;
    assert_complete_accounting(&selection, &all_candidate_ids);
    Ok((
        selection
            .selected_candidate_ids()
            .map(str::to_owned)
            .collect(),
        selection
            .deduplicated
            .into_iter()
            .map(|entry| {
                (
                    entry.candidate_id,
                    entry.duplicate_of_candidate_id,
                    entry.reason,
                )
            })
            .collect(),
    ))
}

/// Admits, normalizes, and selects over `candidates`, returning the whole
/// selection receipt alongside every normalized candidate id in host order.
fn selection_of(
    candidates: Vec<RecallCandidateV1>,
    maximum_selected: usize,
) -> Result<(RecallSelectionV1, Vec<String>), Box<dyn Error>> {
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_exact(),
        candidates,
    )?;
    let normalization = normalize_admitted_candidates(Default::default(), &admission.admitted)?;
    let all_candidate_ids: Vec<String> = normalization
        .candidates
        .iter()
        .map(|candidate| candidate.candidate_id.clone())
        .collect();
    let policy = RecallSelectionPolicyV1::new(maximum_selected)?;
    let selection = select_recall_candidates(policy, &normalization, &admission.admitted)?;
    Ok((selection, all_candidate_ids))
}

/// Every input candidate must appear in exactly one of the three ledgers: a
/// selection whose ledgers do not partition its input cannot be reconciled,
/// and a candidate that vanished silently is exactly the accounting hole this
/// asserts against.
fn assert_complete_accounting(selection: &RecallSelectionV1, all_candidate_ids: &[String]) {
    let mut accounted: Vec<&str> = selection.accounted_candidate_ids().collect();
    let accounted_rows = accounted.len();
    accounted.sort_unstable();
    accounted.dedup();
    assert_eq!(
        accounted.len(),
        accounted_rows,
        "a candidate is accounted for in more than one ledger: {accounted:?}"
    );
    let mut expected: Vec<&str> = all_candidate_ids.iter().map(String::as_str).collect();
    expected.sort_unstable();
    assert_eq!(
        accounted, expected,
        "selection ledgers do not account for every input candidate"
    );
}

// --- stable-reference and content-digest deduplication -------------------

/// Two candidates that declare the same non-empty stable memory reference are
/// the same underlying memory even though their inline text differs (e.g. a
/// provider re-surfacing the same record with a refreshed summary); only the
/// host-ordered first survives and consumes budget.
#[test]
fn same_stable_memory_ref_collapses_regardless_of_wording() -> Result<(), Box<dyn Error>> {
    let candidates = vec![
        candidate_with("a", "first phrasing of the record", Some("memory:shared-1")),
        candidate_with(
            "b",
            "second phrasing of the same record",
            Some("memory:shared-1"),
        ),
    ];
    let (selected, deduplicated) = select(candidates, 8)?;
    assert_eq!(selected, vec!["a".to_owned()]);
    assert_eq!(deduplicated.len(), 1);
    assert_eq!(deduplicated[0].0, "b");
    assert_eq!(deduplicated[0].1, "a");
    assert_eq!(deduplicated[0].2, DuplicateReason::StableMemoryRef);
    Ok(())
}

/// Two candidates with byte-identical content but different candidate ids
/// and no stable references still collapse: identical bytes cannot
/// express different evidence no matter the source.
#[test]
fn identical_content_collapses_by_digest_without_a_stable_reference() -> Result<(), Box<dyn Error>>
{
    let candidates = ["a", "b"]
        .into_iter()
        .map(|id| {
            let mut value = candidate_value(
                id,
                "the exact same words, byte for byte",
                scope_value(&admitted_scope()),
                current_validity(),
            );
            value["stable_memory_ref"] = Value::Null;
            decode(value)
        })
        .collect();
    let (selected, deduplicated) = select(candidates, 8)?;
    assert_eq!(selected, vec!["a".to_owned()]);
    assert_eq!(deduplicated.len(), 1);
    assert_eq!(deduplicated[0].0, "b");
    assert_eq!(deduplicated[0].2, DuplicateReason::ContentDigest);
    Ok(())
}

/// A duplicate never consumes a second unit of the selection budget: with a
/// budget of two, a distinct third candidate is still selected
/// once the exact duplicate is folded away rather than the budget being
/// exhausted by the duplicate pair.
#[test]
fn duplicate_candidates_do_not_consume_repeated_budget() -> Result<(), Box<dyn Error>> {
    let candidates = vec![
        candidate_with("a", "identical wording here", None),
        candidate_with("b", "identical wording here", None),
        candidate_with(
            "c",
            "a completely unrelated topic about database indexing",
            None,
        ),
    ];
    let (selected, deduplicated) = select(candidates, 2)?;
    assert_eq!(selected, vec!["a".to_owned(), "c".to_owned()]);
    assert_eq!(deduplicated.len(), 1);
    assert_eq!(deduplicated[0].0, "b");
    Ok(())
}

/// Similar wording can express distinct claims; only exact identity deduplicates.
#[test]
fn near_wording_and_negations_remain_distinct() -> Result<(), Box<dyn Error>> {
    for suffix in [
        "shipping",
        "not release",
        "can't release",
        "can’t release",
        "never release",
    ] {
        let base = "the deploy pipeline requires a database migration before";
        let (selected, deduplicated) = select(
            vec![
                candidate_with("a", &format!("{base} release"), None),
                candidate_with("b", &format!("{base} {suffix}"), None),
            ],
            8,
        )?;
        assert_eq!(selected, ["a", "b"], "{suffix}");
        assert!(deduplicated.is_empty(), "{suffix}");
    }
    Ok(())
}

// --- complete accounting of every candidate --------------------------------

/// A distinct candidate the selection budget cannot admit is recorded as a
/// budget exclusion instead of disappearing: `selected`, `deduplicated`,
/// and `budget_excluded` together account for every
/// input candidate exactly once.
#[test]
fn candidates_past_the_budget_are_recorded_not_dropped() -> Result<(), Box<dyn Error>> {
    let candidates = vec![
        candidate_with(
            "a",
            "the release checklist covers database migrations",
            None,
        ),
        candidate_with(
            "b",
            "the release checklist covers database migrations",
            None,
        ),
        candidate_with(
            "c",
            "frontend bundling is handled by a separate config",
            None,
        ),
        candidate_with(
            "d",
            "telemetry sampling runs at one percent in staging",
            None,
        ),
    ];
    let (selection, all_ids) = selection_of(candidates, 2)?;
    assert_complete_accounting(&selection, &all_ids);

    let selected: Vec<&str> = selection.selected_candidate_ids().collect();
    assert_eq!(selected, vec!["a", "c"]);
    assert_eq!(selection.deduplicated.len(), 1);
    assert_eq!(selection.deduplicated[0].candidate_id, "b");
    assert_eq!(selection.budget_excluded.len(), 1);
    assert_eq!(selection.budget_excluded[0].candidate_id, "d");
    assert_eq!(
        selection.budget_excluded[0].reason,
        BudgetExclusionReason::SelectionBudgetExhausted {
            maximum_selected: 2
        }
    );
    assert!(
        selection
            .warnings
            .iter()
            .any(|warning| warning.contains("did not fit the selection budget")),
        "{:?}",
        selection.warnings
    );
    Ok(())
}

/// Duplicates are still classified after the budget fills; the later distinct
/// candidate receives an explicit budget exclusion.
#[test]
fn classification_continues_after_the_budget_is_full() -> Result<(), Box<dyn Error>> {
    let content = "deployment requires validation";
    let candidates = vec![
        candidate_with("a", content, None),
        candidate_with(
            "b",
            "telemetry sampling runs at one percent in staging",
            None,
        ),
        candidate_with("c", content, None),
        candidate_with(
            "d",
            "release notes are generated from the changelog file",
            None,
        ),
    ];
    let (selection, all_ids) = selection_of(candidates, 2)?;
    assert_complete_accounting(&selection, &all_ids);

    let selected: Vec<&str> = selection.selected_candidate_ids().collect();
    assert_eq!(selected, vec!["a", "b"]);
    assert_eq!(selection.deduplicated.len(), 1);
    assert_eq!(selection.deduplicated[0].candidate_id, "c");
    assert_eq!(selection.deduplicated[0].duplicate_of_candidate_id, "a");
    assert_eq!(selection.budget_excluded.len(), 1);
    assert_eq!(selection.budget_excluded[0].candidate_id, "d");
    Ok(())
}

// --- typed refusal of a mismatched admitted slice ---------------------------

/// The admitted slice must be the one the normalization was built from. A
/// same-sized but reordered slice would pair one candidate's words with
/// another candidate's identity and permit an incorrect collapse, so it is a
/// typed error rather than a quietly content-free comparison.
#[test]
fn reordered_admitted_slice_is_refused() -> Result<(), Box<dyn Error>> {
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_exact(),
        vec![
            candidate_with("a", "the deploy pipeline requires a migration", None),
            candidate_with("b", "the frontend build uses a separate bundler", None),
        ],
    )?;
    let normalization = normalize_admitted_candidates(Default::default(), &admission.admitted)?;
    let mut reordered = admission.admitted.clone();
    reordered.reverse();

    let error =
        select_recall_candidates(RecallSelectionPolicyV1::new(8)?, &normalization, &reordered)
            .expect_err("a reordered admitted slice is not the normalized slice");
    assert_eq!(error, RecallSelectionError::NormalizationMismatch);
    Ok(())
}

/// A `provider_rank` that does not index the admitted slice is refused
/// instead of being treated as a candidate with no inline content, which
/// would silently disable deduplication for it.
#[test]
fn out_of_range_provider_rank_is_refused() -> Result<(), Box<dyn Error>> {
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_exact(),
        vec![
            candidate_with("a", "the deploy pipeline requires a migration", None),
            candidate_with("b", "the frontend build uses a separate bundler", None),
        ],
    )?;
    let normalization = normalize_admitted_candidates(Default::default(), &admission.admitted)?;

    let error = select_recall_candidates(
        RecallSelectionPolicyV1::new(8)?,
        &normalization,
        &admission.admitted[..1],
    )
    .expect_err("a truncated admitted slice cannot describe the normalization");
    assert_eq!(error, RecallSelectionError::NormalizationMismatch);
    Ok(())
}

/// A normalized candidate whose canonical content digest disagrees with the
/// admitted entry is refused before any selection decision.
#[test]
fn content_digest_disagreement_is_refused() -> Result<(), Box<dyn Error>> {
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_exact(),
        vec![candidate_with(
            "a",
            "the deploy pipeline requires a migration",
            None,
        )],
    )?;
    let mut normalization = normalize_admitted_candidates(Default::default(), &admission.admitted)?;
    let foreign_digest = "f".repeat(64);
    normalization.candidates[0].content_sha256 = foreign_digest;

    let error = select_recall_candidates(
        RecallSelectionPolicyV1::new(8)?,
        &normalization,
        &admission.admitted,
    )
    .expect_err("a foreign content digest is not the admitted candidate's");
    assert_eq!(error, RecallSelectionError::NormalizationMismatch);
    Ok(())
}

// --- determinism and explainability ---------------------------------------

/// Selecting twice over the same normalized set under the same policy
/// produces byte-identical selected order and identical ledgers.
#[test]
fn selection_is_deterministic_for_a_fixed_policy() -> Result<(), Box<dyn Error>> {
    let candidates = || {
        vec![
            candidate_with("a", "topic one about caching strategy", None),
            candidate_with("b", "topic one about caching strategy exactly", None),
            candidate_with("c", "topic two about test isolation", None),
        ]
    };
    let (first, first_dedup) = select(candidates(), 8)?;
    let (second, second_dedup) = select(candidates(), 8)?;
    assert_eq!(first, second);
    assert_eq!(first_dedup, second_dedup);
    Ok(())
}

/// A candidate whose content is a reference rather than inline text can
/// still be deduplicated by stable reference or content digest. Distinct
/// references with distinct digests remain independent candidates.
#[test]
fn distinct_reference_only_content_is_retained() -> Result<(), Box<dyn Error>> {
    let mut a = candidate_value(
        "a",
        "irrelevant since content_ref takes precedence in the fixture builder",
        scope_value(&admitted_scope()),
        current_validity(),
    );
    // Same digest as a real reference target would declare, but the
    // candidate itself carries a reference, not inline content.
    a["content"] = Value::Null;
    a["content_ref"] = json!({"kind": "test-ref", "id": "ref-a"});
    let b = candidate_with("b", "a totally different sentence about networking", None);
    let candidates = vec![decode(a), b];
    let (selected, deduplicated) = select(candidates, 8)?;
    assert_eq!(selected.len(), 2);
    assert!(deduplicated.is_empty());
    Ok(())
}

// --- policy construction ---------------------------------------------------

/// A zero-candidate selection budget is refused rather than silently
/// producing an always-empty selection.
#[test]
fn zero_budget_policy_is_refused() {
    assert_eq!(
        RecallSelectionPolicyV1::new(0),
        Err(RecallSelectionPolicyError::ZeroBudget)
    );
}

#[test]
fn narrowing_never_expands_the_pinned_budget() -> Result<(), Box<dyn Error>> {
    let policy = RecallSelectionPolicyV1::new(4)?;
    assert_eq!(policy.narrowed_to(2)?.maximum_selected(), 2);
    assert_eq!(policy.narrowed_to(9)?.maximum_selected(), 4);
    assert_eq!(
        policy.narrowed_to(0),
        Err(RecallSelectionPolicyError::ZeroBudget)
    );
    Ok(())
}

// --- no provider-specific ID assumption ------------------------------------

/// Dedup by stable reference works on arbitrary opaque strings — a UUID-like
/// value and a short slug both collapse correctly when repeated, with no
/// assumption about ID shape or length.
#[test]
fn stable_reference_dedup_makes_no_assumption_about_id_shape() -> Result<(), Box<dyn Error>> {
    let odd_ref = "urn:x-ncm:9f86d081-884c-4d90:chunk#7!";
    let candidates = vec![
        candidate_with("a", "first mention", Some(odd_ref)),
        candidate_with(
            "b",
            "second mention with different wording entirely",
            Some(odd_ref),
        ),
    ];
    let (selected, deduplicated) = select(candidates, 8)?;
    assert_eq!(selected, vec!["a".to_owned()]);
    assert_eq!(deduplicated[0].2, DuplicateReason::StableMemoryRef);
    Ok(())
}

#[test]
fn distinct_meaning_is_preserved_without_a_wording_heuristic() -> Result<(), Box<dyn Error>> {
    for (left, right) in [
        ("safe", "unsafe"),
        ("enabled", "disabled"),
        ("10", "100"),
        ("x == y", "x != y"),
        ("x > y", "x < y"),
    ] {
        let base = "the deployment rule after validation and review of production configuration is";
        let (selection, ids) = selection_of(
            vec![
                candidate_with("a", &format!("{base} {left}"), None),
                candidate_with("b", &format!("{base} {right}"), None),
            ],
            8,
        )?;
        assert_complete_accounting(&selection, &ids);
        assert_eq!(
            selection.selected_candidate_ids().collect::<Vec<_>>(),
            ["a", "b"],
            "{left} versus {right}"
        );
    }
    Ok(())
}

#[test]
fn differences_beyond_the_old_sample_prefix_are_preserved() -> Result<(), Box<dyn Error>> {
    let prefix = "shared deployment validation context ".repeat(130);
    assert!(prefix.len() > 4096);
    let (selection, ids) = selection_of(
        vec![
            candidate_with("a", &format!("{prefix}allow production rollout"), None),
            candidate_with("b", &format!("{prefix}deny production rollout"), None),
        ],
        8,
    )?;
    assert_complete_accounting(&selection, &ids);
    assert_eq!(
        selection.selected_candidate_ids().collect::<Vec<_>>(),
        ["a", "b"]
    );
    Ok(())
}

#[test]
fn altered_normalization_cannot_change_selection() -> Result<(), Box<dyn Error>> {
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_exact(),
        vec![
            candidate_with("a", "deployment allowed", None),
            candidate_with("b", "deployment denied", None),
        ],
    )?;
    let canonical = normalize_admitted_candidates(Default::default(), &admission.admitted)?;
    let mut variants = Vec::new();
    let mut omitted = canonical.clone();
    omitted.candidates.clear();
    variants.push(("empty candidates", omitted));
    let mut truncated = canonical.clone();
    truncated.candidates.pop();
    variants.push(("omitted candidate", truncated));
    let mut duplicated = canonical.clone();
    duplicated.candidates.push(duplicated.candidates[0].clone());
    variants.push(("duplicated candidate", duplicated));
    let mut stable_ref = canonical.clone();
    stable_ref.candidates[1].stable_memory_ref = stable_ref.candidates[0].stable_memory_ref.clone();
    variants.push(("forged stable reference", stable_ref));
    let mut reordered = canonical.clone();
    reordered.candidates.reverse();
    variants.push(("forged host order", reordered));
    let mut metadata = canonical.clone();
    metadata.candidates[0].explanation_summary = Some("forged evidence".to_owned());
    variants.push(("forged candidate metadata", metadata));
    let mut policy = canonical.clone();
    policy.normalization_policy_revision += 1;
    variants.push(("forged policy", policy));
    let mut warnings = canonical.clone();
    warnings.warnings.push("forged set evidence".to_owned());
    variants.push(("forged set warnings", warnings));
    let mut ordering = canonical;
    ordering.cross_provider_ordering_admissible = !ordering.cross_provider_ordering_admissible;
    variants.push(("forged cross-provider ordering", ordering));
    for (label, altered) in variants {
        assert_eq!(
            select_recall_candidates(
                RecallSelectionPolicyV1::new(8)?,
                &altered,
                &admission.admitted
            ),
            Err(RecallSelectionError::NormalizationMismatch),
            "incorrect refusal for {label}"
        );
    }
    Ok(())
}
