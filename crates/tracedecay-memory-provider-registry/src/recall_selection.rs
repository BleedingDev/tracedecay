//! Host-owned exact deduplication and budget selection over normalized recall.
//!
//! Candidates are the same memory only when their non-empty stable memory
//! references or exact content digests match. Similar wording alone never
//! establishes identity: small changes can reverse a claim or change code.
//!
//! The first representative in deterministic host relevance order survives.
//! Every input appears exactly once as selected, deduplicated, or excluded by
//! budget. Deduplication continues after the budget fills, preserving reasons
//! for all candidates. Provider references are opaque strings.
//!
//! Selection recomputes canonical normalization from the admitted slice and
//! requires full equality before making decisions. This authenticates the
//! complete candidate set, metadata, ordering, and normalization policy.

use serde::{Deserialize, Serialize};

use crate::recall_admission::AdmittedRecallCandidate;
use crate::recall_normalization::{
    NormalizedRecallCandidateV1, RecallNormalizationError, RecallNormalizationV1,
    normalize_admitted_candidates,
};

/// Identity of the host selection policy implemented here.
pub const HOST_SELECTION_POLICY_ID: &str = "tracedecay.host.recall.selection.dedup_diversity.v1";

/// Revision 3 removes wording heuristics and validates the full normalization.
pub const HOST_SELECTION_POLICY_REVISION: u64 = 3;

/// Pinned host selection configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecallSelectionPolicyV1 {
    maximum_selected: usize,
}

/// Why a selection policy could not be constructed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RecallSelectionPolicyError {
    /// A policy that can select nothing is not a usable budget.
    #[error("selection budget must admit at least one candidate")]
    ZeroBudget,
}

impl RecallSelectionPolicyV1 {
    /// Constructs the pinned policy with the given output budget.
    ///
    /// # Errors
    /// Returns [`RecallSelectionPolicyError::ZeroBudget`] for a zero budget.
    pub fn new(maximum_selected: usize) -> Result<Self, RecallSelectionPolicyError> {
        if maximum_selected == 0 {
            return Err(RecallSelectionPolicyError::ZeroBudget);
        }
        Ok(Self { maximum_selected })
    }

    /// Policy identity carried on every selection.
    #[must_use]
    pub const fn policy_id(&self) -> &'static str {
        HOST_SELECTION_POLICY_ID
    }

    /// Pinned policy revision.
    #[must_use]
    pub const fn policy_revision(&self) -> u64 {
        HOST_SELECTION_POLICY_REVISION
    }

    /// Maximum retained candidates.
    #[must_use]
    pub const fn maximum_selected(&self) -> usize {
        self.maximum_selected
    }

    /// Tightens the budget, clamping requests above the existing limit.
    ///
    /// # Errors
    /// Returns [`RecallSelectionPolicyError::ZeroBudget`] for a zero budget.
    pub fn narrowed_to(self, maximum_selected: usize) -> Result<Self, RecallSelectionPolicyError> {
        Self::new(maximum_selected.min(self.maximum_selected))
    }
}

/// Why a candidate was collapsed into an earlier representative.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum DuplicateReason {
    /// Both candidates declared the same non-empty stable memory reference.
    StableMemoryRef,
    /// Both candidates carry the same exact content digest.
    ContentDigest,
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

/// Why one distinct candidate did not fit the selection budget.
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

/// One distinct candidate that did not fit the selection budget.
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

/// Why selection or contribution validation failed.
///
/// Every variant is an integrity violation between the normalization and the
/// admitted slice it claims to describe. None is repaired or degraded: a
/// selection over mismatched inputs could attach one candidate's content to
/// another candidate's identity and collapse distinct evidence.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RecallSelectionError {
    /// Recomputing normalization from the admitted slice failed.
    #[error("admitted candidates could not be normalized: {0}")]
    Normalization(#[from] RecallNormalizationError),
    /// The supplied normalization differs from the canonical admitted result.
    #[error("normalization does not match the admitted candidates under the host policy")]
    NormalizationMismatch,
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
    /// normalized candidate's.
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

/// The result of exact deduplication and budget selection over one
/// [`RecallNormalizationV1`].
///
/// The three candidate ledgers partition the input exactly: every candidate of
/// the normalization appears once in [`Self::selected`],
/// [`Self::deduplicated`], or [`Self::budget_excluded`], and in no two of them.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct RecallSelectionV1 {
    /// Policy identity that produced this selection.
    pub selection_policy_id: String,
    /// Revision of that policy.
    pub selection_policy_revision: u64,
    /// Candidates retained for context, in host order — a
    /// subsequence of [`RecallNormalizationV1::candidates`].
    pub selected: Vec<NormalizedRecallCandidateV1>,
    /// Candidates removed as duplicates, in the order they were evaluated.
    pub deduplicated: Vec<DeduplicatedCandidateV1>,
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

    /// Every candidate identity this selection accounts for, across all three
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
                self.budget_excluded
                    .iter()
                    .map(|entry| entry.candidate_id.as_str()),
            )
    }
}

/// Deduplicates in host order, then selects representatives within budget.
///
/// # Errors
/// Returns [`RecallSelectionError::Normalization`] if canonical normalization
/// fails, or [`RecallSelectionError::NormalizationMismatch`] if any supplied
/// normalization field differs from the canonical result for `admitted`.
pub fn select_recall_candidates(
    policy: RecallSelectionPolicyV1,
    normalization: &RecallNormalizationV1,
    admitted: &[AdmittedRecallCandidate],
) -> Result<RecallSelectionV1, RecallSelectionError> {
    let canonical = normalize_admitted_candidates(Default::default(), admitted)?;
    if normalization != &canonical {
        return Err(RecallSelectionError::NormalizationMismatch);
    }

    let mut deduplicated = Vec::new();
    let mut representatives: Vec<(usize, &NormalizedRecallCandidateV1)> = Vec::new();
    for (index, candidate) in normalization.candidates.iter().enumerate() {
        if let Some((representative, reason)) =
            representatives.iter().find_map(|(_, representative)| {
                duplicate_reason(candidate, representative).map(|reason| (*representative, reason))
            })
        {
            deduplicated.push(DeduplicatedCandidateV1 {
                candidate_id: candidate.candidate_id.clone(),
                duplicate_of_candidate_id: representative.candidate_id.clone(),
                reason,
            });
        } else {
            representatives.push((index, candidate));
        }
    }

    let mut selected = Vec::new();
    let mut budget_excluded = Vec::new();
    for (host_order_position, candidate) in representatives {
        if selected.len() < policy.maximum_selected {
            selected.push(candidate.clone());
        } else {
            budget_excluded.push(BudgetExcludedCandidateV1 {
                candidate_id: candidate.candidate_id.clone(),
                host_order_position,
                reason: BudgetExclusionReason::SelectionBudgetExhausted {
                    maximum_selected: policy.maximum_selected,
                },
            });
        }
    }

    let mut warnings = Vec::new();
    if !deduplicated.is_empty() {
        warnings.push(format!(
            "{} candidate(s) removed as duplicates of an earlier candidate",
            deduplicated.len()
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
        budget_excluded,
        warnings,
    })
}

fn duplicate_reason(
    candidate: &NormalizedRecallCandidateV1,
    representative: &NormalizedRecallCandidateV1,
) -> Option<DuplicateReason> {
    if let (Some(candidate_ref), Some(representative_ref)) = (
        candidate.stable_memory_ref.as_deref(),
        representative.stable_memory_ref.as_deref(),
    ) && !candidate_ref.is_empty()
        && candidate_ref == representative_ref
    {
        return Some(DuplicateReason::StableMemoryRef);
    }
    (candidate.content_sha256 == representative.content_sha256)
        .then_some(DuplicateReason::ContentDigest)
}
