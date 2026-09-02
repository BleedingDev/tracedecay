//! Behavioral tests for host-owned deduplication and diversity selection over
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
    BudgetExclusionReason, DuplicateReason, NEGATION_MARKERS, RecallCandidateV1,
    RecallSelectionError, RecallSelectionPolicyError, RecallSelectionPolicyV1, RecallSelectionV1,
    admit_recall_candidates, normalize_admitted_candidates, select_recall_candidates,
};

/// An 18-word sentence with no negation marker in it, used to build pairs
/// that differ by exactly one inserted word. Inserting one word at position
/// two leaves 14 of the 19 trigrams in the union shared — 736,842 ppm, above
/// the 700,000 ppm duplicate bar — so any such pair collapses unless
/// something other than wording keeps it apart.
const DUPLICATE_BAR_BASE: &str = "the migration can safely proceed after validation checks confirm \
                                  schema compatibility and rollback readiness for production \
                                  deployment today";

/// Measured similarity of a [`DUPLICATE_BAR_BASE`] pair, in parts per
/// million: 14 shared trigrams out of a 19-trigram union.
const DUPLICATE_BAR_SIMILARITY_PPM: u32 = 736_842;

/// A 10-word sentence with no negation marker in it. One inserted word leaves
/// 6 of the 11 trigrams in the union shared — 545,454 ppm, above the 400,000
/// ppm diversity bar but below the 700,000 ppm duplicate bar — so such a pair
/// is diversity-excluded, not deduplicated, unless something other than
/// wording keeps it apart.
const DIVERSITY_BAR_BASE: &str =
    "orbit harbor delta canyon meridian cobalt lantern summit quartz falcon";

/// Measured similarity of a [`DIVERSITY_BAR_BASE`] pair, in parts per
/// million: 6 shared trigrams out of an 11-trigram union.
const DIVERSITY_BAR_SIMILARITY_PPM: u32 = 545_454;

/// `base` with `word` inserted as the third word.
fn with_inserted_word(base: &str, word: &str) -> String {
    let mut words: Vec<&str> = base.split_whitespace().collect();
    words.insert(2, word);
    words.join(" ")
}

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
/// dedup and diversity ledgers' candidate ids.
#[allow(clippy::type_complexity)]
fn select(
    candidates: Vec<RecallCandidateV1>,
    maximum_selected: usize,
) -> Result<
    (
        Vec<String>,
        Vec<(String, String, DuplicateReason)>,
        Vec<(String, String)>,
    ),
    Box<dyn Error>,
> {
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
        selection
            .diversity_excluded
            .into_iter()
            .map(|entry| (entry.candidate_id, entry.similar_to_candidate_id))
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

/// Every input candidate must appear in exactly one of the four ledgers: a
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
    let (selected, deduplicated, diversity_excluded) = select(candidates, 8)?;
    assert_eq!(selected, vec!["a".to_owned()]);
    assert_eq!(deduplicated.len(), 1);
    assert_eq!(deduplicated[0].0, "b");
    assert_eq!(deduplicated[0].1, "a");
    assert_eq!(deduplicated[0].2, DuplicateReason::StableMemoryRef);
    assert!(diversity_excluded.is_empty());
    Ok(())
}

/// Two candidates with byte-identical content but different candidate ids
/// and different stable references still collapse: identical bytes cannot
/// express different evidence no matter the source.
#[test]
fn identical_content_collapses_by_digest_without_a_stable_reference() -> Result<(), Box<dyn Error>>
{
    let candidates = vec![
        candidate_with("a", "the exact same words, byte for byte", None),
        candidate_with("b", "the exact same words, byte for byte", None),
    ];
    let (selected, deduplicated, _) = select(candidates, 8)?;
    assert_eq!(selected, vec!["a".to_owned()]);
    assert_eq!(deduplicated.len(), 1);
    assert_eq!(deduplicated[0].0, "b");
    assert_eq!(deduplicated[0].2, DuplicateReason::ContentDigest);
    Ok(())
}

/// A duplicate never consumes a second unit of the selection budget: with a
/// budget of one, a genuinely distinct third candidate is still selected
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
    let (selected, deduplicated, _) = select(candidates, 2)?;
    assert_eq!(selected, vec!["a".to_owned(), "c".to_owned()]);
    assert_eq!(deduplicated.len(), 1);
    assert_eq!(deduplicated[0].0, "b");
    Ok(())
}

// --- bounded content-similarity near-duplicate collapse -------------------

/// Two candidates with no shared identity but near-identical wording (one
/// word changed out of many) collapse as a near-content duplicate.
#[test]
fn near_identical_wording_without_shared_identity_collapses() -> Result<(), Box<dyn Error>> {
    let candidates = vec![
        candidate_with(
            "a",
            "the deploy pipeline requires a database migration before release",
            None,
        ),
        candidate_with(
            "b",
            "the deploy pipeline requires a database migration before shipping",
            None,
        ),
    ];
    let (selected, deduplicated, _) = select(candidates, 8)?;
    assert_eq!(selected, vec!["a".to_owned()]);
    assert_eq!(deduplicated.len(), 1);
    assert_eq!(deduplicated[0].0, "b");
    assert!(matches!(
        deduplicated[0].2,
        DuplicateReason::NearContent { .. }
    ));
    Ok(())
}

/// Candidates about unrelated topics never collapse, however short.
#[test]
fn unrelated_content_never_collapses() -> Result<(), Box<dyn Error>> {
    let candidates = vec![
        candidate_with(
            "a",
            "the deploy pipeline requires a database migration",
            None,
        ),
        candidate_with(
            "b",
            "the frontend build uses a separate bundler config",
            None,
        ),
    ];
    let (selected, deduplicated, _) = select(candidates, 8)?;
    assert_eq!(selected.len(), 2);
    assert!(deduplicated.is_empty());
    Ok(())
}

// --- negation guard: distinct polarity is never collapsed -----------------

/// Two candidates that share almost every word but disagree on negation
/// assert opposite things and must never be collapsed, however high their
/// wording overlap would otherwise score.
#[test]
fn negated_and_unnegated_claims_are_not_collapsed() -> Result<(), Box<dyn Error>> {
    let candidates = vec![
        candidate_with("a", "the migration is safe to run in production", None),
        candidate_with("b", "the migration is not safe to run in production", None),
    ];
    let (selected, deduplicated, diversity_excluded) = select(candidates, 8)?;
    assert_eq!(selected.len(), 2);
    assert!(deduplicated.is_empty());
    assert!(diversity_excluded.is_empty());
    Ok(())
}

/// Control for the diversity guard below: with a *non*-negating word
/// inserted, the pair's measured similarity clears the diversity bar (but not
/// the duplicate bar) and the second candidate really is excluded. Without
/// this the guard test could pass on a pair the metric would never have
/// excluded anyway.
#[test]
fn one_inserted_neutral_word_clears_the_diversity_bar() -> Result<(), Box<dyn Error>> {
    let variant = with_inserted_word(DIVERSITY_BAR_BASE, "clearly");
    let (selection, all_ids) = selection_of(
        vec![
            candidate_with("a", DIVERSITY_BAR_BASE, None),
            candidate_with("b", &variant, None),
        ],
        8,
    )?;
    assert_complete_accounting(&selection, &all_ids);
    assert!(selection.deduplicated.is_empty(), "below the duplicate bar");
    assert_eq!(selection.diversity_excluded.len(), 1);
    assert_eq!(selection.diversity_excluded[0].candidate_id, "b");
    assert_eq!(selection.diversity_excluded[0].similar_to_candidate_id, "a");
    assert_eq!(
        selection.diversity_excluded[0].similarity_ppm,
        DIVERSITY_BAR_SIMILARITY_PPM
    );
    Ok(())
}

/// The negation guard stops diversity selection from discarding the negative
/// half of contradictory evidence: the identical insertion the control above
/// shows is diversity-excluded is kept when the inserted word negates. Run
/// for every marker of the vocabulary, so a marker the tokenizer cannot
/// actually match fails here.
#[test]
fn every_negation_marker_protects_diversity_selection() -> Result<(), Box<dyn Error>> {
    for marker in NEGATION_MARKERS {
        let variant = with_inserted_word(DIVERSITY_BAR_BASE, marker);
        let (selection, all_ids) = selection_of(
            vec![
                candidate_with("a", DIVERSITY_BAR_BASE, None),
                candidate_with("b", &variant, None),
            ],
            8,
        )?;
        assert_complete_accounting(&selection, &all_ids);
        let selected: Vec<&str> = selection.selected_candidate_ids().collect();
        assert_eq!(selected, vec!["a", "b"], "marker {marker:?}");
        assert!(
            selection.diversity_excluded.is_empty(),
            "marker {marker:?} was diversity-excluded: {:?}",
            selection.diversity_excluded
        );
    }
    Ok(())
}

/// Control for the duplicate-bar guard below: a single inserted neutral word
/// leaves the pair above the duplicate bar, so it collapses as a near-content
/// duplicate. Every negation test that follows uses the same insertion, which
/// is what makes those tests non-vacuous.
#[test]
fn one_inserted_neutral_word_clears_the_duplicate_bar() -> Result<(), Box<dyn Error>> {
    let variant = with_inserted_word(DUPLICATE_BAR_BASE, "clearly");
    let (selected, deduplicated, _) = select(
        vec![
            candidate_with("a", DUPLICATE_BAR_BASE, None),
            candidate_with("b", &variant, None),
        ],
        8,
    )?;
    assert_eq!(selected, vec!["a".to_owned()]);
    assert_eq!(deduplicated.len(), 1);
    assert_eq!(
        deduplicated[0].2,
        DuplicateReason::NearContent {
            similarity_ppm: DUPLICATE_BAR_SIMILARITY_PPM
        }
    );
    Ok(())
}

/// Every marker of the negation vocabulary — the contractions included —
/// blocks a collapse that the control above proves the metric would
/// otherwise make. Splitting tokens on every non-alphanumeric character turns
/// "can't" into "can" and "t", which match nothing, so a pair differing only
/// by a contraction would be collapsed as a duplicate and the negative
/// evidence lost; that regression fails this test on the first contraction.
#[test]
fn every_negation_marker_blocks_a_duplicate_collapse() -> Result<(), Box<dyn Error>> {
    for marker in NEGATION_MARKERS {
        let variant = with_inserted_word(DUPLICATE_BAR_BASE, marker);
        let (selection, all_ids) = selection_of(
            vec![
                candidate_with("a", DUPLICATE_BAR_BASE, None),
                candidate_with("b", &variant, None),
            ],
            8,
        )?;
        assert_complete_accounting(&selection, &all_ids);
        let selected: Vec<&str> = selection.selected_candidate_ids().collect();
        assert_eq!(selected, vec!["a", "b"], "marker {marker:?}");
        assert!(
            selection.deduplicated.is_empty(),
            "marker {marker:?} was collapsed: {:?}",
            selection.deduplicated
        );
        assert!(
            selection.diversity_excluded.is_empty(),
            "marker {marker:?} was diversity-excluded: {:?}",
            selection.diversity_excluded
        );
    }
    Ok(())
}

/// Real content writes contractions with a typographic apostrophe. A marker
/// written that way is the same marker, so it must block the same collapse
/// the ASCII form does.
#[test]
fn typographic_apostrophe_contractions_block_a_duplicate_collapse() -> Result<(), Box<dyn Error>> {
    for marker in ["can\u{2019}t", "won\u{2019}t", "doesn\u{2019}t"] {
        let variant = with_inserted_word(DUPLICATE_BAR_BASE, marker);
        let (selected, deduplicated, diversity_excluded) = select(
            vec![
                candidate_with("a", DUPLICATE_BAR_BASE, None),
                candidate_with("b", &variant, None),
            ],
            8,
        )?;
        assert_eq!(selected.len(), 2, "marker {marker:?}");
        assert!(deduplicated.is_empty(), "marker {marker:?}");
        assert!(diversity_excluded.is_empty(), "marker {marker:?}");
    }
    Ok(())
}

// --- complete accounting of every candidate --------------------------------

/// A distinct candidate the selection budget cannot admit is recorded as a
/// budget exclusion instead of disappearing: `selected`, `deduplicated`,
/// `diversity_excluded`, and `budget_excluded` together account for every
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
    assert!(selection.diversity_excluded.is_empty());
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

/// A candidate that is redundant with an already-selected one is still
/// classified as redundant after the budget is full, rather than being
/// reported as a budget exclusion: the ledger says why each candidate was
/// dropped, not merely that it was.
#[test]
fn classification_continues_after_the_budget_is_full() -> Result<(), Box<dyn Error>> {
    let redundant = with_inserted_word(DIVERSITY_BAR_BASE, "clearly");
    let candidates = vec![
        candidate_with("a", DIVERSITY_BAR_BASE, None),
        candidate_with(
            "b",
            "telemetry sampling runs at one percent in staging",
            None,
        ),
        candidate_with("c", &redundant, None),
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
    assert_eq!(selection.diversity_excluded.len(), 1);
    assert_eq!(selection.diversity_excluded[0].candidate_id, "c");
    assert_eq!(selection.diversity_excluded[0].similar_to_candidate_id, "a");
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
    assert_eq!(
        error,
        RecallSelectionError::CandidateIdentityMismatch {
            provider_rank: 0,
            expected: "a".to_owned(),
            admitted: "b".to_owned(),
        }
    );
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
    assert_eq!(
        error,
        RecallSelectionError::ProviderRankOutOfRange {
            candidate_id: "b".to_owned(),
            provider_rank: 1,
            admitted_len: 1,
        }
    );
    Ok(())
}

/// A normalized candidate whose canonical content digest disagrees with the
/// admitted entry is refused: the metric would otherwise sample bytes the
/// candidate was not admitted with.
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
    let admitted_digest = normalization.candidates[0].content_sha256.clone();
    let foreign_digest = "f".repeat(64);
    normalization.candidates[0].content_sha256 = foreign_digest.clone();

    let error = select_recall_candidates(
        RecallSelectionPolicyV1::new(8)?,
        &normalization,
        &admission.admitted,
    )
    .expect_err("a foreign content digest is not the admitted candidate's");
    assert_eq!(
        error,
        RecallSelectionError::ContentDigestMismatch {
            candidate_id: "a".to_owned(),
            expected: foreign_digest,
            admitted: admitted_digest,
        }
    );
    Ok(())
}

// --- diversity selection ----------------------------------------------------

/// Below the duplicate bar but clearly redundant wording is excluded by
/// diversity selection so it does not crowd out a distinct third candidate
/// under a constrained budget.
#[test]
fn redundant_wording_is_diversity_excluded_under_budget() -> Result<(), Box<dyn Error>> {
    let candidates = vec![
        candidate_with(
            "a",
            "orbit harbor delta canyon meridian cobalt lantern summit quartz falcon",
            None,
        ),
        // Single interior word changed (cobalt -> topaz): shares 5 of 8
        // trigrams with `a`, a similarity clearly above the diversity bar but
        // below the stricter duplicate bar.
        candidate_with(
            "b",
            "orbit harbor delta canyon meridian topaz lantern summit quartz falcon",
            None,
        ),
        candidate_with(
            "c",
            "vector nimbus thistle bramble cinder holloway pewter marlin ochre wisteria",
            None,
        ),
    ];
    let (selected, deduplicated, diversity_excluded) = select(candidates, 2)?;
    assert!(deduplicated.is_empty());
    assert_eq!(selected, vec!["a".to_owned(), "c".to_owned()]);
    assert_eq!(diversity_excluded.len(), 1);
    assert_eq!(diversity_excluded[0].0, "b");
    assert_eq!(diversity_excluded[0].1, "a");
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
    let (first, first_dedup, first_diversity) = select(candidates(), 8)?;
    let (second, second_dedup, second_diversity) = select(candidates(), 8)?;
    assert_eq!(first, second);
    assert_eq!(first_dedup, second_dedup);
    assert_eq!(first_diversity, second_diversity);
    Ok(())
}

/// A candidate whose content is a reference rather than inline text can
/// still be deduplicated by stable reference or content digest, but is never
/// treated as similar to anything on wording it never carried inline.
#[test]
fn reference_only_content_is_never_near_content_collapsed() -> Result<(), Box<dyn Error>> {
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
    let (selected, deduplicated, diversity_excluded) = select(candidates, 8)?;
    assert_eq!(selected.len(), 2);
    assert!(deduplicated.is_empty());
    assert!(diversity_excluded.is_empty());
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

/// A diversity bar stricter than the duplicate bar is refused: diversity
/// would then exclude candidates dedup itself would already have collapsed,
/// making the two passes' evidence contradictory.
#[test]
fn inverted_thresholds_are_refused() {
    let result = RecallSelectionPolicyV1::with_thresholds(4, 500_000, 900_000);
    assert_eq!(
        result,
        Err(RecallSelectionPolicyError::ThresholdOrderInverted {
            duplicate: 500_000,
            diversity: 900_000,
        })
    );
}

/// A threshold above the unit scale cannot be a similarity fraction.
#[test]
fn out_of_range_threshold_is_refused() {
    let result = RecallSelectionPolicyV1::with_thresholds(4, 1_000_001, 100);
    assert_eq!(
        result,
        Err(RecallSelectionPolicyError::ThresholdOutOfRange {
            value: 1_000_001,
            unit: 1_000_000,
        })
    );
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
    let (selected, deduplicated, _) = select(candidates, 8)?;
    assert_eq!(selected, vec!["a".to_owned()]);
    assert_eq!(deduplicated[0].2, DuplicateReason::StableMemoryRef);
    Ok(())
}
