//! Host-owned deduplication and diversity selection over normalized recall
//! candidates.
//!
//! Normalization ([`crate::recall_normalization`]) converts admitted
//! candidates into one common candidate space, ordered by host relevance.
//! This module is the next stage: it decides which of those already-admitted
//! candidates actually consume advisory budget, and it decides nothing about
//! admission or relevance — a candidate this module drops was already
//! admitted and normalized; it is excluded only because another surviving
//! candidate already carries the same, or too similar, evidence.
//!
//! Two passes run in host order (the order [`RecallNormalizationV1`] already
//! established):
//!
//! * **Deduplication** collapses candidates that are the *same* memory. A
//!   provider-declared [`stable_memory_ref`](NormalizedRecallCandidateV1::stable_memory_ref)
//!   match is the strongest signal when both sides declare one; an exact
//!   [`content_sha256`](NormalizedRecallCandidateV1::content_sha256) match is
//!   next, since identical bytes cannot express different evidence no matter
//!   the source; a bounded content-similarity metric over inline text is the
//!   fallback when neither identity is available. Every collapse is recorded
//!   as a [`DeduplicatedCandidateV1`] row naming which surviving candidate it
//!   was folded into and why.
//! * **Diversity selection** then walks the deduplicated set once more and
//!   drops a candidate that is merely *redundant* with one already selected
//!   — similar wording repeated from a different angle — so a fixed budget
//!   is not consumed by near-restatements of the same point. Every drop is
//!   recorded as a [`DiversityExcludedCandidateV1`] row.
//!
//! * **Budget exclusion** is the last classification: once the policy's
//!   `maximum_selected` slots are full, every remaining survivor is still
//!   classified and recorded as a [`BudgetExcludedCandidateV1`] row rather
//!   than silently disappearing. Selection therefore returns a *complete*
//!   decision receipt: every candidate of the input normalization appears
//!   exactly once across `selected`, `deduplicated`, `diversity_excluded`,
//!   and `budget_excluded`, so the output can always be reconciled against
//!   its input.
//!
//! **Distinct evidence is never collapsed on wording alone.** The bounded
//! similarity metric records, for both dedup and diversity, a deterministic
//! negation signature per candidate (which of a fixed marker vocabulary —
//! "not", "never", "cannot", "can't", ... — appears in its content). Two
//! candidates whose negation signatures differ are never treated as similar,
//! however high their shingle overlap: "the migration is safe" and "the
//! migration is not safe" share almost every word and assert opposite things,
//! and a wording-only metric that could not tell them apart would be exactly
//! the polarity loss this stage must not introduce. Contraction markers are
//! matched as whole words: the apostrophe is canonicalized (typographic to
//! ASCII) and kept inside the token, so "can't" is one token that matches the
//! vocabulary rather than two tokens ("can", "t") that match nothing.
//!
//! **A normalization that does not describe the admitted slice is a typed
//! error.** [`select_recall_candidates`] validates every normalized
//! candidate's `provider_rank`, identity, and content digest against the
//! admitted entry it indexes before selecting anything. A reordered,
//! foreign, or wrong-sized admitted slice is refused with
//! [`RecallSelectionError`]; it is never degraded into "no inline content",
//! which would attach one candidate's words to another candidate's identity
//! and permit incorrect deduplication.
//!
//! **Selection is deterministic for a fixed policy.** Both passes iterate the
//! normalization's own host order, ties are already broken there by
//! candidate id, and the bounded similarity metric is exact integer
//! arithmetic (a Jaccard ratio over word shingles, expressed in parts per
//! [`SIMILARITY_UNIT`]) with no floating point. The same normalized set under
//! the same policy always yields the same selection, in the same order.
//!
//! **No provider-specific identity format is assumed.** `stable_memory_ref`
//! and `content_sha256` are compared as opaque strings; nothing here branches
//! on provider identity, ID shape, or ID length.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::recall_admission::{AdmittedRecallCandidate, RecallCandidateContent};
use crate::recall_normalization::{NormalizedRecallCandidateV1, RecallNormalizationV1};

/// Identity of the host selection policy implemented here.
pub const HOST_SELECTION_POLICY_ID: &str = "tracedecay.host.recall.selection.dedup_diversity.v1";

/// Revision of [`HOST_SELECTION_POLICY_ID`]. Any change to the dedup keys,
/// the similarity metric, the negation vocabulary, or the ordering rules must
/// increment this.
///
/// Revision 2 canonicalizes apostrophes and keeps them inside word tokens, so
/// the contraction markers of [`NEGATION_MARKERS`] are matchable; revision 1
/// split them apart and could never match them.
pub const HOST_SELECTION_POLICY_REVISION: u64 = 2;

/// Similarity scale: every similarity this module emits is parts per this
/// unit, where the unit itself means "identical".
pub const SIMILARITY_UNIT: u32 = 1_000_000;

/// Default bounded-content-similarity bar at or above which two candidates
/// with no matching stable reference or content digest are still treated as
/// the same memory and collapsed at deduplication.
pub const DEFAULT_DUPLICATE_SIMILARITY_THRESHOLD_PPM: u32 = 700_000;

/// Default bounded-content-similarity bar at or above which a candidate is
/// treated as redundant with an already-selected candidate and excluded by
/// diversity selection. Deliberately below
/// [`DEFAULT_DUPLICATE_SIMILARITY_THRESHOLD_PPM`]: diversity trims
/// near-restatements that dedup's stricter bar does not consider the same
/// memory.
pub const DEFAULT_DIVERSITY_SIMILARITY_THRESHOLD_PPM: u32 = 400_000;

/// Word-token n-gram size the bounded similarity metric shingles over.
const SHINGLE_SIZE: usize = 3;

/// Maximum bytes of candidate content the similarity metric inspects. Bounds
/// the cost of selection independent of provider-declared content size; it
/// never changes what is delivered, only what the metric samples.
const MAX_SIMILARITY_CONTENT_BYTES: usize = 4_096;

/// The apostrophe every contraction marker is written with.
const APOSTROPHE: char = '\'';

/// Text form of [`APOSTROPHE`].
const ASCII_APOSTROPHE: &str = "'";

/// Apostrophe forms real content carries that are canonicalized to
/// [`APOSTROPHE`] before tokenization: the typographic right single quote,
/// the modifier letter apostrophe, and the fullwidth apostrophe.
const APOSTROPHE_VARIANTS: [char; 3] = ['\u{2019}', '\u{02BC}', '\u{FF07}'];

/// Deterministic negation vocabulary. Two candidates whose presence of these
/// markers differs are never treated as duplicates or diversity-redundant of
/// each other, whatever their shingle overlap.
///
/// Every entry is matched as one whole token of the canonicalized content, so
/// the contraction entries are only matchable because tokenization keeps a
/// canonicalized apostrophe inside the word. The vocabulary is public so the
/// guard it provides can be exercised entry by entry rather than asserted for
/// one sample marker.
pub const NEGATION_MARKERS: &[&str] = &[
    "not",
    "no",
    "never",
    "cannot",
    "can't",
    "won't",
    "don't",
    "doesn't",
    "isn't",
    "aren't",
    "wasn't",
    "weren't",
    "shouldn't",
    "wouldn't",
    "couldn't",
    "without",
    "neither",
    "nor",
];

/// Pinned host selection configuration.
///
/// Determinism is a property of this value: the same normalized candidate set
/// under the same policy always produces the same selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecallSelectionPolicyV1 {
    policy_id: &'static str,
    policy_revision: u64,
    maximum_selected: usize,
    duplicate_similarity_threshold_ppm: u32,
    diversity_similarity_threshold_ppm: u32,
}

/// Why a [`RecallSelectionPolicyV1`] could not be constructed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RecallSelectionPolicyError {
    /// A policy that can select nothing is not a usable budget.
    #[error("selection budget must admit at least one candidate")]
    ZeroBudget,
    /// A threshold outside `0..=SIMILARITY_UNIT` cannot be a similarity
    /// fraction.
    #[error("similarity threshold {value} exceeds the unit scale {unit}")]
    ThresholdOutOfRange {
        /// The out-of-range threshold.
        value: u32,
        /// The unit it was compared against.
        unit: u32,
    },
    /// The duplicate bar must be at least as strict as the diversity bar, or
    /// diversity selection would exclude candidates dedup itself would have
    /// collapsed, making the two passes' evidence contradictory.
    #[error(
        "duplicate similarity threshold {duplicate} must be at or above the diversity threshold \
         {diversity}"
    )]
    ThresholdOrderInverted {
        /// The configured duplicate threshold.
        duplicate: u32,
        /// The configured diversity threshold.
        diversity: u32,
    },
}

impl RecallSelectionPolicyV1 {
    /// The pinned policy with the default similarity thresholds and the
    /// given output budget.
    ///
    /// # Errors
    ///
    /// Returns [`RecallSelectionPolicyError::ZeroBudget`] when
    /// `maximum_selected` is zero.
    pub fn new(maximum_selected: usize) -> Result<Self, RecallSelectionPolicyError> {
        Self::with_thresholds(
            maximum_selected,
            DEFAULT_DUPLICATE_SIMILARITY_THRESHOLD_PPM,
            DEFAULT_DIVERSITY_SIMILARITY_THRESHOLD_PPM,
        )
    }

    /// The pinned policy with explicit similarity thresholds.
    ///
    /// # Errors
    ///
    /// Returns [`RecallSelectionPolicyError::ZeroBudget`],
    /// [`RecallSelectionPolicyError::ThresholdOutOfRange`], or
    /// [`RecallSelectionPolicyError::ThresholdOrderInverted`] for an invalid
    /// configuration; nothing here clamps or repairs an invalid value.
    pub fn with_thresholds(
        maximum_selected: usize,
        duplicate_similarity_threshold_ppm: u32,
        diversity_similarity_threshold_ppm: u32,
    ) -> Result<Self, RecallSelectionPolicyError> {
        if maximum_selected == 0 {
            return Err(RecallSelectionPolicyError::ZeroBudget);
        }
        for value in [
            duplicate_similarity_threshold_ppm,
            diversity_similarity_threshold_ppm,
        ] {
            if value > SIMILARITY_UNIT {
                return Err(RecallSelectionPolicyError::ThresholdOutOfRange {
                    value,
                    unit: SIMILARITY_UNIT,
                });
            }
        }
        if duplicate_similarity_threshold_ppm < diversity_similarity_threshold_ppm {
            return Err(RecallSelectionPolicyError::ThresholdOrderInverted {
                duplicate: duplicate_similarity_threshold_ppm,
                diversity: diversity_similarity_threshold_ppm,
            });
        }
        Ok(Self {
            policy_id: HOST_SELECTION_POLICY_ID,
            policy_revision: HOST_SELECTION_POLICY_REVISION,
            maximum_selected,
            duplicate_similarity_threshold_ppm,
            diversity_similarity_threshold_ppm,
        })
    }

    /// Policy identity carried on every selection it produces.
    #[must_use]
    pub const fn policy_id(&self) -> &'static str {
        self.policy_id
    }

    /// Pinned policy revision.
    #[must_use]
    pub const fn policy_revision(&self) -> u64 {
        self.policy_revision
    }

    /// Maximum candidates a selection under this policy may retain.
    #[must_use]
    pub const fn maximum_selected(&self) -> usize {
        self.maximum_selected
    }

    /// Bounded-similarity bar at or above which two candidates are the same
    /// memory.
    #[must_use]
    pub const fn duplicate_similarity_threshold_ppm(&self) -> u32 {
        self.duplicate_similarity_threshold_ppm
    }

    /// Bounded-similarity bar at or above which a candidate is redundant with
    /// an already-selected one.
    #[must_use]
    pub const fn diversity_similarity_threshold_ppm(&self) -> u32 {
        self.diversity_similarity_threshold_ppm
    }

    /// The same pinned thresholds under a budget no larger than this one's.
    ///
    /// A caller may only tighten the pinned budget: `maximum_selected` above
    /// the policy's own budget is clamped down to it, never up.
    ///
    /// # Errors
    ///
    /// Returns [`RecallSelectionPolicyError::ZeroBudget`] when
    /// `maximum_selected` is zero.
    pub fn narrowed_to(self, maximum_selected: usize) -> Result<Self, RecallSelectionPolicyError> {
        Self::with_thresholds(
            maximum_selected.min(self.maximum_selected),
            self.duplicate_similarity_threshold_ppm,
            self.diversity_similarity_threshold_ppm,
        )
    }
}

/// Why one candidate was collapsed into an earlier, surviving candidate at
/// deduplication.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum DuplicateReason {
    /// Both candidates declared the same non-empty stable provider memory
    /// reference.
    StableMemoryRef,
    /// Both candidates carry the same canonical content digest.
    ContentDigest,
    /// Bounded content similarity met or exceeded the policy's duplicate
    /// threshold and the two candidates' negation signatures agreed.
    NearContent {
        /// The measured similarity, in parts per [`SIMILARITY_UNIT`].
        similarity_ppm: u32,
    },
}

/// One candidate removed at deduplication.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct DeduplicatedCandidateV1 {
    /// The removed candidate's request-scoped identity.
    pub candidate_id: String,
    /// The surviving candidate it was collapsed into.
    pub duplicate_of_candidate_id: String,
    /// Why the two were treated as the same memory.
    pub reason: DuplicateReason,
}

/// One candidate removed at diversity selection.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct DiversityExcludedCandidateV1 {
    /// The excluded candidate's request-scoped identity.
    pub candidate_id: String,
    /// The already-selected candidate it was too similar to.
    pub similar_to_candidate_id: String,
    /// The measured similarity, in parts per [`SIMILARITY_UNIT`].
    pub similarity_ppm: u32,
}

/// Why one candidate that survived both deduplication and diversity was
/// still not selected.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum BudgetExclusionReason {
    /// The policy's selection budget was already full when this candidate
    /// was reached in host order.
    SelectionBudgetExhausted {
        /// The budget that was exhausted.
        maximum_selected: usize,
    },
}

/// One candidate that was neither a duplicate nor redundant but did not fit
/// the selection budget.
///
/// This row exists so a selection is reconcilable against its input: a
/// candidate that simply vanished past the budget would make the output
/// impossible to account for.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct BudgetExcludedCandidateV1 {
    /// The excluded candidate's request-scoped identity.
    pub candidate_id: String,
    /// Zero-based position of the candidate in the input host order.
    pub host_order_position: usize,
    /// Why it was excluded.
    pub reason: BudgetExclusionReason,
}

/// Why one normalized candidate set could not be selected over at all.
///
/// Every variant is an integrity violation between the normalization and the
/// admitted slice it claims to describe. None is repaired or degraded: a
/// selection over mismatched inputs could attach one candidate's content to
/// another candidate's identity and collapse distinct evidence.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RecallSelectionError {
    /// A normalized candidate's `provider_rank` does not index the admitted
    /// slice.
    #[error(
        "normalized candidate {candidate_id} carries provider rank {provider_rank}, outside the \
         {admitted_len} admitted candidate(s)"
    )]
    ProviderRankOutOfRange {
        /// The normalized candidate's identity.
        candidate_id: String,
        /// The rank it declared.
        provider_rank: usize,
        /// Length of the admitted slice supplied.
        admitted_len: usize,
    },
    /// The admitted entry at a normalized candidate's `provider_rank` is a
    /// different candidate, so the slice is not the one normalization ran
    /// over.
    #[error(
        "admitted candidate at provider rank {provider_rank} is {admitted}, not the normalized \
         candidate {expected}"
    )]
    CandidateIdentityMismatch {
        /// Rank whose entry disagreed.
        provider_rank: usize,
        /// Identity the normalization recorded.
        expected: String,
        /// Identity the admitted slice carries.
        admitted: String,
    },
    /// The admitted entry's canonical content digest disagrees with the
    /// normalized candidate's, so the content the metric would sample is not
    /// the content the candidate was admitted with.
    #[error(
        "admitted candidate {candidate_id} carries content digest {admitted}, not the normalized \
         digest {expected}"
    )]
    ContentDigestMismatch {
        /// The candidate whose digests disagreed.
        candidate_id: String,
        /// Digest the normalization recorded.
        expected: String,
        /// Digest the admitted slice carries.
        admitted: String,
    },
}

/// The result of deduplication and diversity selection over one
/// [`RecallNormalizationV1`].
///
/// The four candidate ledgers partition the input exactly: every candidate of
/// the normalization appears once in [`Self::selected`],
/// [`Self::deduplicated`], [`Self::diversity_excluded`], or
/// [`Self::budget_excluded`], and in no two of them.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct RecallSelectionV1 {
    /// Policy identity that produced this selection.
    pub selection_policy_id: String,
    /// Revision of that policy.
    pub selection_policy_revision: u64,
    /// Candidates retained for context, in host order — a strict
    /// subsequence of [`RecallNormalizationV1::candidates`].
    pub selected: Vec<NormalizedRecallCandidateV1>,
    /// Candidates removed as duplicates, in the order they were evaluated.
    pub deduplicated: Vec<DeduplicatedCandidateV1>,
    /// Candidates removed for content diversity, in the order they were
    /// evaluated.
    pub diversity_excluded: Vec<DiversityExcludedCandidateV1>,
    /// Distinct candidates that did not fit the selection budget, in the
    /// order they were evaluated.
    pub budget_excluded: Vec<BudgetExcludedCandidateV1>,
    /// Bounded set-level host warnings.
    pub warnings: Vec<String>,
}

impl RecallSelectionV1 {
    /// Request-scoped identities of the selected candidates, in selection
    /// order.
    pub fn selected_candidate_ids(&self) -> impl Iterator<Item = &str> {
        self.selected
            .iter()
            .map(|candidate| candidate.candidate_id.as_str())
    }

    /// Every candidate identity this selection accounts for, across all four
    /// ledgers, in ledger order.
    ///
    /// The host uses this to reconcile a selection against the normalization
    /// it was produced from: the two sets are always equal.
    pub fn accounted_candidate_ids(&self) -> impl Iterator<Item = &str> {
        self.selected_candidate_ids()
            .chain(
                self.deduplicated
                    .iter()
                    .map(|entry| entry.candidate_id.as_str()),
            )
            .chain(
                self.diversity_excluded
                    .iter()
                    .map(|entry| entry.candidate_id.as_str()),
            )
            .chain(
                self.budget_excluded
                    .iter()
                    .map(|entry| entry.candidate_id.as_str()),
            )
    }
}

/// A deterministic, bounded profile of one candidate's inline content used
/// only to compare candidates against each other. Never retained in the
/// selection output and never derived from a content reference: a candidate
/// whose content was not hydrated inline carries no profile and is compared
/// only by stable reference and content digest.
struct ContentProfile {
    shingles: BTreeSet<String>,
    negation_signature: Vec<bool>,
}

/// Applies host deduplication and diversity selection to one normalized
/// candidate set.
///
/// `admitted` must be the exact admitted slice `normalization` was built from
/// ([`NormalizedRecallCandidateV1::provider_rank`] indexes into it). That is
/// verified, not assumed: each rank must be in range and the entry it names
/// must carry the same candidate identity and canonical content digest as the
/// normalized candidate. Anything else is a
/// [`RecallSelectionError`] — a wrong-but-same-sized slice would otherwise
/// pair one candidate's words with another candidate's identity.
///
/// # Errors
///
/// Returns [`RecallSelectionError`] when the normalization does not describe
/// `admitted`.
pub fn select_recall_candidates(
    policy: RecallSelectionPolicyV1,
    normalization: &RecallNormalizationV1,
    admitted: &[AdmittedRecallCandidate],
) -> Result<RecallSelectionV1, RecallSelectionError> {
    let mut profiles: Vec<Option<ContentProfile>> =
        Vec::with_capacity(normalization.candidates.len());
    for candidate in &normalization.candidates {
        let entry = admitted.get(candidate.provider_rank).ok_or_else(|| {
            RecallSelectionError::ProviderRankOutOfRange {
                candidate_id: candidate.candidate_id.clone(),
                provider_rank: candidate.provider_rank,
                admitted_len: admitted.len(),
            }
        })?;
        let source = entry.candidate();
        if source.candidate_id != candidate.candidate_id {
            return Err(RecallSelectionError::CandidateIdentityMismatch {
                provider_rank: candidate.provider_rank,
                expected: candidate.candidate_id.clone(),
                admitted: source.candidate_id.clone(),
            });
        }
        if source.content_sha256 != candidate.content_sha256 {
            return Err(RecallSelectionError::ContentDigestMismatch {
                candidate_id: candidate.candidate_id.clone(),
                expected: candidate.content_sha256.clone(),
                admitted: source.content_sha256.clone(),
            });
        }
        profiles.push(content_profile(entry));
    }

    let mut deduplicated = Vec::new();
    let mut representatives: Vec<usize> = Vec::new();

    'candidates: for (index, candidate) in normalization.candidates.iter().enumerate() {
        for &representative_index in &representatives {
            let representative = &normalization.candidates[representative_index];
            if let Some(reason) = duplicate_reason(
                policy,
                candidate,
                profiles[index].as_ref(),
                representative,
                profiles[representative_index].as_ref(),
            ) {
                deduplicated.push(DeduplicatedCandidateV1 {
                    candidate_id: candidate.candidate_id.clone(),
                    duplicate_of_candidate_id: representative.candidate_id.clone(),
                    reason,
                });
                continue 'candidates;
            }
        }
        representatives.push(index);
    }

    let mut selected: Vec<NormalizedRecallCandidateV1> = Vec::new();
    let mut selected_profiles: Vec<&Option<ContentProfile>> = Vec::new();
    let mut diversity_excluded = Vec::new();
    let mut budget_excluded = Vec::new();

    // Every survivor is classified, including the ones the budget cannot
    // admit: a candidate that fell off the end without a row would leave the
    // selection unreconcilable against its input.
    for representative_index in representatives {
        let candidate = &normalization.candidates[representative_index];
        let profile = &profiles[representative_index];
        let redundant = selected.iter().zip(selected_profiles.iter()).find_map(
            |(already_selected, already_profile)| {
                let similarity_ppm =
                    bounded_similarity(profile.as_ref(), already_profile.as_ref())?;
                (similarity_ppm >= policy.diversity_similarity_threshold_ppm)
                    .then_some((already_selected.candidate_id.clone(), similarity_ppm))
            },
        );
        match redundant {
            Some((similar_to_candidate_id, similarity_ppm)) => {
                diversity_excluded.push(DiversityExcludedCandidateV1 {
                    candidate_id: candidate.candidate_id.clone(),
                    similar_to_candidate_id,
                    similarity_ppm,
                });
            }
            None if selected.len() >= policy.maximum_selected => {
                budget_excluded.push(BudgetExcludedCandidateV1 {
                    candidate_id: candidate.candidate_id.clone(),
                    host_order_position: representative_index,
                    reason: BudgetExclusionReason::SelectionBudgetExhausted {
                        maximum_selected: policy.maximum_selected,
                    },
                });
            }
            None => {
                selected.push(candidate.clone());
                selected_profiles.push(profile);
            }
        }
    }

    let mut warnings = Vec::new();
    if !deduplicated.is_empty() {
        warnings.push(format!(
            "{} candidate(s) removed as duplicates of an earlier candidate",
            deduplicated.len()
        ));
    }
    if !diversity_excluded.is_empty() {
        warnings.push(format!(
            "{} candidate(s) excluded for content diversity",
            diversity_excluded.len()
        ));
    }
    if !budget_excluded.is_empty() {
        warnings.push(format!(
            "{} distinct candidate(s) did not fit the selection budget of {}",
            budget_excluded.len(),
            policy.maximum_selected
        ));
    }

    Ok(RecallSelectionV1 {
        selection_policy_id: policy.policy_id().to_owned(),
        selection_policy_revision: policy.policy_revision(),
        selected,
        deduplicated,
        diversity_excluded,
        budget_excluded,
        warnings,
    })
}

fn duplicate_reason(
    policy: RecallSelectionPolicyV1,
    candidate: &NormalizedRecallCandidateV1,
    profile: Option<&ContentProfile>,
    representative: &NormalizedRecallCandidateV1,
    representative_profile: Option<&ContentProfile>,
) -> Option<DuplicateReason> {
    if let (Some(candidate_ref), Some(representative_ref)) = (
        non_empty(candidate.stable_memory_ref.as_deref()),
        non_empty(representative.stable_memory_ref.as_deref()),
    ) && candidate_ref == representative_ref
    {
        return Some(DuplicateReason::StableMemoryRef);
    }
    if candidate.content_sha256 == representative.content_sha256 {
        return Some(DuplicateReason::ContentDigest);
    }
    let similarity_ppm = bounded_similarity(profile, representative_profile)?;
    (similarity_ppm >= policy.duplicate_similarity_threshold_ppm)
        .then_some(DuplicateReason::NearContent { similarity_ppm })
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.is_empty())
}

/// Bounded content similarity in parts per [`SIMILARITY_UNIT`], or `None`
/// when a similarity cannot honestly be established: either side lacks
/// inline content, both sides have no shingled tokens, or the two
/// candidates' negation signatures disagree. `None` is never treated as
/// "maximally similar" or "not similar" by its caller — it means the metric
/// abstains, and an abstained comparison never collapses or excludes a
/// candidate.
fn bounded_similarity(a: Option<&ContentProfile>, b: Option<&ContentProfile>) -> Option<u32> {
    let (a, b) = (a?, b?);
    if a.negation_signature != b.negation_signature {
        return None;
    }
    if a.shingles.is_empty() && b.shingles.is_empty() {
        return None;
    }
    let intersection = a.shingles.intersection(&b.shingles).count() as u64;
    let union = a.shingles.union(&b.shingles).count() as u64;
    if union == 0 {
        return None;
    }
    let scaled = intersection.saturating_mul(u64::from(SIMILARITY_UNIT)) / union;
    Some(u32::try_from(scaled).unwrap_or(SIMILARITY_UNIT))
}

fn content_profile(entry: &AdmittedRecallCandidate) -> Option<ContentProfile> {
    match entry.content() {
        RecallCandidateContent::Inline(content) => Some(profile_text(content)),
        RecallCandidateContent::Reference(_) => None,
    }
}

fn profile_text(content: &str) -> ContentProfile {
    let bounded = bounded_prefix(content, MAX_SIMILARITY_CONTENT_BYTES);
    // Canonicalize the apostrophe before splitting and keep it inside the
    // word, so a contraction is one token. Splitting on every non-alphanumeric
    // character would turn "can't" into "can" and "t", and the contraction
    // markers of NEGATION_MARKERS could then never match — the negation guard
    // would silently pass a polarity-opposite pair as similar.
    let canonical = bounded
        .to_lowercase()
        .replace(APOSTROPHE_VARIANTS, ASCII_APOSTROPHE);
    let tokens: Vec<&str> = canonical
        .split(|character: char| !character.is_alphanumeric() && character != APOSTROPHE)
        .map(|token| token.trim_matches(APOSTROPHE))
        .filter(|token| !token.is_empty())
        .collect();
    let negation_signature = NEGATION_MARKERS
        .iter()
        .map(|marker| tokens.contains(marker))
        .collect();
    let shingles = if tokens.len() < SHINGLE_SIZE {
        tokens.iter().map(|token| (*token).to_owned()).collect()
    } else {
        tokens
            .windows(SHINGLE_SIZE)
            .map(|window| window.join(" "))
            .collect()
    };
    ContentProfile {
        shingles,
        negation_signature,
    }
}

/// The longest UTF-8-safe prefix of `text` no longer than `max_bytes`.
fn bounded_prefix(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}
