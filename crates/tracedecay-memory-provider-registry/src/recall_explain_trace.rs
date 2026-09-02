//! Correlatable, bounded explain trace over one recall's full pipeline.
//!
//! [`RecallAdmissionReport`], [`RecallSelectionV1`], and [`ContextPackV1`]
//! each carry their own stage's reasons: the admission ledger names why a
//! candidate was refused, the selection ledgers name why an admitted
//! candidate was collapsed, excluded for diversity, or did not fit the
//! selection budget, and the pack's exclusion ledger names why a selected
//! candidate never reached the rendered text. Read separately they cannot
//! answer "what happened to candidate X end to end", because each stage only
//! knows the candidates it itself received.
//!
//! [`build_recall_explain_trace`] reconciles all of them into one trace that
//! is a **complete partition in provider order**: exactly one row per
//! candidate the provider returned, at the provider's own rank, and the
//! builder refuses rather than returns a trace it could not account for.
//! Stages that never ran are named explicitly
//! ([`RecallExplainStageV1::NormalizationUnavailable`],
//! [`RecallExplainStageV1::SelectionUnavailable`]) instead of leaving the
//! candidate out, so "absent from the trace" is never a possible answer.
//!
//! Every row carries two independently labelled reasons that must never be
//! conflated: a **host decision** — the typed, stable-coded refusal,
//! exclusion, or selection status the host itself computed, retained as the
//! source stage's own reason value so no token, quota, or budget number is
//! flattened away — and the **provider's own explanation**, which is
//! evidence of what the provider claimed, never proof of why the host acted.
//!
//! Provider explanation text is provider-controlled and therefore never
//! copied into a trace verbatim. It reaches a row only through a
//! [`RecallExplanationRedactorV1`], and its state in the row is always
//! explicit: not provided, withheld with a typed reason, or retained in the
//! exact bounded form the redactor returned. Production injects the host's
//! own untrusted-memory gate; [`ContainedExplanationRedactorV1`] is the
//! containment-only floor for callers that have no such gate.
//!
//! The trace also carries a [`RecallExplainTraceV1::trace_id`] deterministic
//! over the request identity, the provider identity, and the registration
//! revision, so a later outcome record (feedback, a maintenance run, an
//! audit query) can cite the exact trace a recalled item came from without
//! re-deriving it from the source reports.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::recall_admission::{RecallAdmissionReport, RecallDenialReason};
use crate::recall_context_pack::{
    ContextItemProvenanceV1, ContextPackV1, ProviderExclusionReason, uncontained_item_identity,
};
use crate::recall_normalization::RecallNormalizationV1;
use crate::recall_selection::{BudgetExclusionReason, DuplicateReason, RecallSelectionV1};

/// The host's own untrusted-memory boundary label.
///
/// Re-derived here rather than imported: this crate sits below the hygiene
/// pipeline, and the label's containment predicate is deliberately cheap and
/// total so a downstream boundary can refuse a forged copy without taking a
/// dependency on the gate that mints it.
pub const EXPLAIN_TRACE_BOUNDARY_LABEL: &str = "[untrusted-memory]";

/// Character ceiling [`ContainedExplanationRedactorV1`] admits for one
/// provider explanation. A trace is an audit artefact, not a second copy of
/// the pack: an explanation longer than this is withheld, never truncated
/// into something that reads like the provider's whole claim.
pub const MAX_EXPLAIN_EXPLANATION_CHARS: usize = 256;

/// Which pipeline stage one candidate's trace item stopped at, in pipeline
/// order.
///
/// Every candidate the provider returned reaches exactly one variant. This is
/// what makes a trace a partition rather than a sample: nothing the pipeline
/// touched is missing from it, and a stage that never ran is named rather
/// than silently dropping the candidates that were waiting for it.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecallExplainStageV1 {
    /// Refused at admission; never reached normalization or selection.
    Denied,
    /// Admitted, but the normalization stage did not run for this recall.
    NormalizationUnavailable,
    /// Admitted and normalized, but the selection stage did not run for this
    /// recall.
    SelectionUnavailable,
    /// Collapsed into an earlier, surviving candidate at deduplication.
    Deduplicated,
    /// Excluded for content redundancy with an already-selected candidate.
    DiversityExcluded,
    /// Survived deduplication and diversity but did not fit the selection
    /// budget.
    BudgetExcluded,
    /// Selected, then withheld by a host stage that runs between selection
    /// and pack compilation — unhydratable provenance, an unhydrated content
    /// reference — and named by the host's own stable reason code.
    HostWithheld,
    /// Selected by the host; no pack was compiled for this trace, so the
    /// pack stage has no verdict to report.
    Selected,
    /// Selected, then excluded from the compiled pack by the pack's own
    /// budgets or containment checks.
    PackExcluded,
    /// Compiled into the rendered pack the agent received.
    Injected,
}

impl RecallExplainStageV1 {
    /// Stable snake_case label, identical to the wire tag.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Denied => "denied",
            Self::NormalizationUnavailable => "normalization_unavailable",
            Self::SelectionUnavailable => "selection_unavailable",
            Self::Deduplicated => "deduplicated",
            Self::DiversityExcluded => "diversity_excluded",
            Self::BudgetExcluded => "budget_excluded",
            Self::HostWithheld => "host_withheld",
            Self::Selected => "selected",
            Self::PackExcluded => "pack_excluded",
            Self::Injected => "injected",
        }
    }
}

/// The host-computed decision for one candidate, retained as the source
/// stage's own typed reason.
///
/// Nothing here is flattened into a string: an advisory-quota exclusion still
/// carries the quota, the tokens the section had already spent, and the
/// item's measured cost; a selection-budget exclusion still carries the
/// maximum the policy allowed. A caller explaining a truncation reads the
/// numbers the host actually decided on rather than re-deriving them.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum RecallExplainHostDecisionV1 {
    /// Refused at admission.
    Denied {
        /// The admission ledger's own typed refusal.
        reason: RecallDenialReason,
    },
    /// Admitted; the normalization stage did not run.
    NormalizationUnavailable,
    /// Admitted and normalized; the selection stage did not run.
    SelectionUnavailable,
    /// Collapsed into a surviving candidate.
    Deduplicated {
        /// The surviving candidate it was collapsed into.
        duplicate_of_candidate_id: String,
        /// The deduplication ledger's own typed reason.
        reason: DuplicateReason,
    },
    /// Excluded as redundant with an already-selected candidate.
    DiversityExcluded {
        /// The already-selected candidate it was too similar to.
        similar_to_candidate_id: String,
        /// Measured similarity in parts per million.
        similarity_ppm: u32,
    },
    /// Did not fit the selection budget.
    BudgetExcluded {
        /// Zero-based position of the candidate in the input host order.
        host_order_position: usize,
        /// The selection ledger's own typed reason, carrying the budget.
        reason: BudgetExclusionReason,
    },
    /// Withheld by a host stage between selection and pack compilation.
    HostWithheld {
        /// Stable snake_case code the host stage supplied.
        reason_code: String,
        /// Bounded host-authored detail, when the stage supplied one.
        detail: Option<String>,
    },
    /// Selected; no pack was compiled for this trace.
    Selected,
    /// Selected, then excluded by the pack.
    PackExcluded {
        /// The pack's own typed exclusion, carrying every budget, quota, and
        /// token value the decision was measured against.
        reason: ProviderExclusionReason,
    },
    /// Compiled into the rendered pack.
    Injected {
        /// Section the item was compiled into.
        section: String,
        /// Exact token cost of the compiled item.
        tokens: u64,
    },
}

impl RecallExplainHostDecisionV1 {
    /// The stage this decision places a candidate at.
    #[must_use]
    pub const fn stage(&self) -> RecallExplainStageV1 {
        match self {
            Self::Denied { .. } => RecallExplainStageV1::Denied,
            Self::NormalizationUnavailable => RecallExplainStageV1::NormalizationUnavailable,
            Self::SelectionUnavailable => RecallExplainStageV1::SelectionUnavailable,
            Self::Deduplicated { .. } => RecallExplainStageV1::Deduplicated,
            Self::DiversityExcluded { .. } => RecallExplainStageV1::DiversityExcluded,
            Self::BudgetExcluded { .. } => RecallExplainStageV1::BudgetExcluded,
            Self::HostWithheld { .. } => RecallExplainStageV1::HostWithheld,
            Self::Selected => RecallExplainStageV1::Selected,
            Self::PackExcluded { .. } => RecallExplainStageV1::PackExcluded,
            Self::Injected { .. } => RecallExplainStageV1::Injected,
        }
    }

    /// Stable snake_case host reason code.
    ///
    /// This is host authority, never the provider's own words: it is the same
    /// code family a caller would get from matching the source reason enum,
    /// surfaced as one field so every stage's reasons are comparable
    /// regardless of which enum produced them.
    #[must_use]
    pub fn code(&self) -> &str {
        match self {
            Self::Denied { reason } => reason.label(),
            Self::NormalizationUnavailable => "normalization_unavailable",
            Self::SelectionUnavailable => "selection_unavailable",
            Self::Deduplicated { reason, .. } => duplicate_reason_code(reason),
            Self::DiversityExcluded { .. } => "near_content",
            Self::BudgetExcluded { reason, .. } => budget_exclusion_reason_code(reason),
            Self::HostWithheld { reason_code, .. } => reason_code.as_str(),
            Self::Selected => "selected_pack_not_compiled",
            Self::PackExcluded { reason } => provider_exclusion_reason_code(reason),
            Self::Injected { .. } => "compiled_into_pack",
        }
    }

    /// Bounded, host-authored detail for the decision, when it carries one.
    #[must_use]
    pub fn detail(&self) -> Option<String> {
        match self {
            Self::Denied { reason } => denial_reason_detail(reason),
            Self::NormalizationUnavailable
            | Self::SelectionUnavailable
            | Self::Selected
            | Self::Injected { .. } => None,
            Self::Deduplicated {
                duplicate_of_candidate_id,
                ..
            } => Some(format!("duplicate_of={duplicate_of_candidate_id}")),
            Self::DiversityExcluded {
                similar_to_candidate_id,
                similarity_ppm,
            } => Some(format!(
                "similar_to={similar_to_candidate_id} similarity_ppm={similarity_ppm}"
            )),
            Self::BudgetExcluded {
                host_order_position,
                reason,
            } => match reason {
                BudgetExclusionReason::SelectionBudgetExhausted { maximum_selected } => {
                    Some(format!(
                        "host_order_position={host_order_position} \
                         maximum_selected={maximum_selected}"
                    ))
                }
            },
            Self::HostWithheld { detail, .. } => detail.clone(),
            Self::PackExcluded { reason } => provider_exclusion_reason_detail(reason),
        }
    }
}

fn duplicate_reason_code(reason: &DuplicateReason) -> &'static str {
    match reason {
        DuplicateReason::StableMemoryRef => "stable_memory_ref",
        DuplicateReason::ContentDigest => "content_digest",
        DuplicateReason::NearContent { .. } => "near_content",
    }
}

const fn budget_exclusion_reason_code(reason: &BudgetExclusionReason) -> &'static str {
    match reason {
        BudgetExclusionReason::SelectionBudgetExhausted { .. } => "selection_budget_exhausted",
    }
}

const fn provider_exclusion_reason_code(reason: &ProviderExclusionReason) -> &'static str {
    match reason {
        ProviderExclusionReason::MetadataNotContained { .. } => "metadata_not_contained",
        ProviderExclusionReason::AdvisoryQuotaExhausted { .. } => "advisory_quota_exhausted",
        ProviderExclusionReason::TotalBudgetExhausted { .. } => "total_budget_exhausted",
        ProviderExclusionReason::ContentNotInline => "content_not_inline",
        ProviderExclusionReason::AdvisoryFramingDoesNotFit { .. } => {
            "advisory_framing_does_not_fit"
        }
        ProviderExclusionReason::RenderedPackOverBudget { .. } => "rendered_pack_over_budget",
    }
}

/// Bounded host-authored detail naming the exact numbers a pack exclusion
/// turned on, so an operator reading a truncation does not have to reopen the
/// pack to learn which budget bit.
fn provider_exclusion_reason_detail(reason: &ProviderExclusionReason) -> Option<String> {
    match reason {
        ProviderExclusionReason::MetadataNotContained { field } => {
            Some(format!("field={}", field.label()))
        }
        ProviderExclusionReason::AdvisoryQuotaExhausted {
            advisory_token_quota,
            section_tokens_used,
            item_tokens,
        } => Some(format!(
            "advisory_token_quota={advisory_token_quota} \
             section_tokens_used={section_tokens_used} item_tokens={item_tokens}"
        )),
        ProviderExclusionReason::TotalBudgetExhausted {
            total_token_budget,
            remaining_tokens,
            item_tokens,
        } => Some(format!(
            "total_token_budget={total_token_budget} remaining_tokens={remaining_tokens} \
             item_tokens={item_tokens}"
        )),
        ProviderExclusionReason::ContentNotInline => None,
        ProviderExclusionReason::AdvisoryFramingDoesNotFit {
            advisory_token_quota,
            framing_tokens,
        } => Some(format!(
            "advisory_token_quota={advisory_token_quota} framing_tokens={framing_tokens}"
        )),
        ProviderExclusionReason::RenderedPackOverBudget {
            token_budget,
            rendered_tokens,
        } => Some(format!(
            "token_budget={token_budget} rendered_tokens={rendered_tokens}"
        )),
    }
}

fn denial_reason_detail(reason: &RecallDenialReason) -> Option<String> {
    match reason {
        RecallDenialReason::ScopeBindingUnauthorized { binding } => {
            Some(format!("binding={binding:?}"))
        }
        RecallDenialReason::ScopeMismatch { field }
        | RecallDenialReason::UnknownIdentity { field }
        | RecallDenialReason::ForbiddenIdentity { field } => Some(format!("field={field:?}")),
        RecallDenialReason::InvalidValidityRecord { detail } => Some(detail.clone()),
        RecallDenialReason::NativeScoreMalformed { defect } => Some(format!("{defect:?}")),
        RecallDenialReason::StaleIdentity
        | RecallDenialReason::NotYetValid
        | RecallDenialReason::Expired
        | RecallDenialReason::Revoked
        | RecallDenialReason::Superseded
        | RecallDenialReason::UnknownValidity
        | RecallDenialReason::ContentDigestMismatch
        | RecallDenialReason::ContentSelectionInvalid => None,
    }
}

/// What the trace retains of one candidate's provider-authored explanation.
///
/// The state is always explicit. A selected candidate whose provider said
/// nothing reads as [`Self::NotProvided`] — a typed answer the contract
/// allows — rather than as an absent field indistinguishable from a bug, and
/// a candidate whose explanation the host refused reads as [`Self::Withheld`]
/// with the host's reason, never as silence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RecallExplainProviderExplanationV1 {
    /// The provider supplied no explanation for this candidate.
    NotProvided,
    /// The provider supplied one and the host's redaction gate refused it.
    /// The refused bytes are never retained; the digest still binds the row
    /// to what was refused.
    Withheld {
        /// Stable snake_case code for the rule that fired.
        reason_code: String,
        /// Digest of the provider's original explanation.
        source_sha256: String,
    },
    /// The provider supplied one and the host's redaction gate returned this
    /// exact bounded form. Evidence of what the provider claimed, never proof
    /// of why the host acted.
    Retained {
        /// The hardened, bounded text.
        text: String,
        /// Digest of the provider's original explanation.
        source_sha256: String,
    },
}

impl RecallExplainProviderExplanationV1 {
    /// Stable snake_case state label.
    #[must_use]
    pub const fn state_code(&self) -> &'static str {
        match self {
            Self::NotProvided => "not_provided",
            Self::Withheld { .. } => "withheld",
            Self::Retained { .. } => "retained",
        }
    }

    /// The retained text, when the host admitted one.
    #[must_use]
    pub fn text(&self) -> Option<&str> {
        match self {
            Self::NotProvided | Self::Withheld { .. } => None,
            Self::Retained { text, .. } => Some(text),
        }
    }
}

/// Digest of one provider-authored explanation, for rows that must bind to
/// text they deliberately do not keep.
#[must_use]
pub fn explanation_source_sha256(explanation: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"recall.explain.provider_explanation\0");
    hasher.update(explanation.as_bytes());
    hex::encode(hasher.finalize())
}

/// The host gate that decides what, if anything, of one provider explanation
/// a trace may retain.
///
/// The trace builder never reads provider bytes directly. Production injects
/// the host's own untrusted-memory gate — the same admitted-secret pipeline,
/// neutralization, and containment an agent-visible line passes — so a trace
/// can never become the one artefact that leaked what the pack withheld.
pub trait RecallExplanationRedactorV1 {
    /// Decides one candidate's explanation.
    ///
    /// `candidate_id` is supplied so a host implementation can answer from a
    /// value it already hardened for the same candidate rather than hardening
    /// twice.
    fn redact(&self, candidate_id: &str, explanation: &str) -> RecallExplainProviderExplanationV1;
}

/// The containment-only floor for callers with no host gate.
///
/// It refuses an explanation that is empty, longer than its ceiling, not a
/// single control-free line, or that forges the host's own untrusted-memory
/// boundary label. It is **not** a secret scanner: a host that can classify
/// credential material must inject its own gate instead of relying on this.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContainedExplanationRedactorV1 {
    max_chars: usize,
}

impl Default for ContainedExplanationRedactorV1 {
    fn default() -> Self {
        Self {
            max_chars: MAX_EXPLAIN_EXPLANATION_CHARS,
        }
    }
}

impl ContainedExplanationRedactorV1 {
    /// A redactor with an explicit character ceiling.
    #[must_use]
    pub const fn new(max_chars: usize) -> Self {
        Self { max_chars }
    }
}

/// Whether one already-rendered string is contained: non-empty, exactly one
/// line, no control or hidden characters, and no forged host boundary label.
#[must_use]
pub fn is_contained_explanation(text: &str) -> bool {
    !text.is_empty()
        && !text.contains(EXPLAIN_TRACE_BOUNDARY_LABEL)
        && !text.chars().any(|character| {
            character.is_control()
                || character.is_whitespace() && character != ' '
                || matches!(character, '\u{200b}'..='\u{200f}' | '\u{202a}'..='\u{202e}'
                    | '\u{2066}'..='\u{2069}' | '\u{feff}')
        })
}

impl RecallExplanationRedactorV1 for ContainedExplanationRedactorV1 {
    fn redact(&self, _candidate_id: &str, explanation: &str) -> RecallExplainProviderExplanationV1 {
        let source_sha256 = explanation_source_sha256(explanation);
        let withheld = |reason_code: &str| RecallExplainProviderExplanationV1::Withheld {
            reason_code: reason_code.to_owned(),
            source_sha256: source_sha256.clone(),
        };
        if explanation.trim().is_empty() {
            return withheld("empty_explanation");
        }
        if explanation.chars().count() > self.max_chars {
            return withheld("oversized_explanation");
        }
        if !is_contained_explanation(explanation) {
            return withheld("explanation_not_contained");
        }
        RecallExplainProviderExplanationV1::Retained {
            text: explanation.to_owned(),
            source_sha256,
        }
    }
}

/// One candidate a host stage between selection and pack compilation
/// withheld, supplied by that stage so the trace stays a complete partition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecallExplainHostWithholdingV1 {
    /// Request-scoped candidate identity.
    pub candidate_id: String,
    /// Stable snake_case code for the host rule that withheld it.
    pub reason_code: String,
    /// Bounded host-authored detail, when the stage has one.
    pub detail: Option<String>,
}

/// One candidate's place in the explain trace.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecallExplainItemV1 {
    /// Request-scoped candidate identity.
    pub candidate_id: String,
    /// Zero-based index of the candidate in the provider's own order.
    pub provider_rank: usize,
    /// Which stage this candidate's journey stopped at.
    pub stage: RecallExplainStageV1,
    /// Stable snake_case host reason code, always present.
    pub host_reason_code: String,
    /// Bounded, host-authored detail for the reason, when it carries one.
    pub host_reason_detail: Option<String>,
    /// The source stage's own typed decision, with every quota, budget, and
    /// token value it was measured against.
    pub host_decision: RecallExplainHostDecisionV1,
    /// What the trace retains of the provider's own explanation, always as an
    /// explicit state.
    pub provider_explanation: RecallExplainProviderExplanationV1,
    /// Section the candidate was compiled into, when it reached the pack.
    pub section: Option<String>,
    /// Exact token cost of the compiled item, when it reached the pack.
    pub tokens: Option<u64>,
}

/// Bounded, deterministic summary of the pack's token and section budgets,
/// carried in the trace so token decisions are visible without re-reading
/// the full [`ContextPackV1`].
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecallExplainTokenSummaryV1 {
    /// The pack's total token budget.
    pub total_token_budget: u64,
    /// Advisory quota inside that budget.
    pub advisory_token_quota: u64,
    /// Tokens reserved for the pack's own deterministic framing.
    pub framing_tokens: u64,
    /// Accounted tokens of framing plus every admitted item.
    pub total_tokens: u64,
    /// Accounted tokens the advisory provider lane consumed.
    pub advisory_tokens: u64,
    /// Measured token cost of the exact rendered text.
    pub rendered_tokens: u64,
}

/// One recall's complete, correlatable explain trace.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecallExplainTraceV1 {
    /// Deterministic identity of this trace, derived from the request
    /// identity, the provider identity, and the registration revision.
    pub trace_id: String,
    /// Request identity the admission ran under.
    pub request_id: String,
    /// Provider the routing policy pinned for this recall.
    pub provider_id: String,
    /// Registration revision the reply was admitted under.
    pub registration_revision: u64,
    /// Candidates the provider returned.
    pub requested_count: usize,
    /// Whether the lane was reported degraded at any stage.
    pub degraded: bool,
    /// Every returned candidate's trace item, one row per received
    /// candidate, in provider order.
    pub items: Vec<RecallExplainItemV1>,
    /// Token and section decisions, when a pack was compiled for this trace.
    pub token_summary: Option<RecallExplainTokenSummaryV1>,
}

impl RecallExplainTraceV1 {
    /// Counts items per stage, in stage-label order.
    #[must_use]
    pub fn stage_counts(&self) -> Vec<(&'static str, usize)> {
        let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();
        for item in &self.items {
            *counts.entry(item.stage.label()).or_insert(0) += 1;
        }
        counts.into_iter().collect()
    }

    /// Counts denied items per host reason code, in code order.
    #[must_use]
    pub fn denial_reason_counts(&self) -> Vec<(&str, usize)> {
        let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
        for item in &self.items {
            if item.stage != RecallExplainStageV1::Denied {
                continue;
            }
            *counts.entry(item.host_reason_code.as_str()).or_insert(0) += 1;
        }
        counts.into_iter().collect()
    }

    /// Looks one candidate's trace item up by its request-scoped identity.
    #[must_use]
    pub fn item(&self, candidate_id: &str) -> Option<&RecallExplainItemV1> {
        self.items
            .iter()
            .find(|item| item.candidate_id == candidate_id)
    }
}

/// Why one recall could not be reconciled into a complete trace.
///
/// Every variant is an integrity violation between the reports the builder
/// was handed. None is repaired: a trace that quietly dropped a candidate, or
/// invented a stage for one, would be worse than no trace at all, because it
/// would read as a complete account of the recall.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RecallExplainTraceError {
    /// The admission report's received count and its received-identity ledger
    /// disagree.
    #[error(
        "admission report declares {received_count} received candidates but lists {listed} \
         candidate identities"
    )]
    ReceivedLedgerMismatch {
        /// Count the report declared.
        received_count: usize,
        /// Identities the report listed.
        listed: usize,
    },
    /// The received-identity ledger names one candidate twice.
    #[error("admission report lists candidate {candidate_id} more than once")]
    DuplicateCandidate {
        /// The repeated identity.
        candidate_id: String,
    },
    /// A later stage's ledger names a candidate the provider never returned.
    #[error(
        "the {stage} ledger names candidate {candidate_id}, which the admission report never \
         received"
    )]
    UnknownCandidate {
        /// The unknown identity.
        candidate_id: String,
        /// Stage whose ledger named it.
        stage: &'static str,
    },
    /// Two stages both claim the same candidate.
    #[error(
        "candidate {candidate_id} is accounted for at stage {existing} and again at stage \
         {conflicting}"
    )]
    ConflictingStages {
        /// The contested identity.
        candidate_id: String,
        /// Stage that claimed it first.
        existing: &'static str,
        /// Stage that claimed it again.
        conflicting: &'static str,
    },
    /// An admitted candidate reached no stage at all.
    #[error("candidate {candidate_id} was admitted but no stage accounts for it")]
    CandidateUnaccounted {
        /// The unaccounted identity.
        candidate_id: String,
    },
}

/// Everything one explain trace is reconciled from.
///
/// Each later-stage report is optional independently because a lane can
/// degrade at any point in the pipeline — the builder never invents a later
/// stage's outcome for a candidate that never reached it, and names the
/// missing stage instead.
pub struct RecallExplainTraceInputsV1<'inputs> {
    /// Provider the routing policy pinned for this recall.
    pub provider_id: &'inputs str,
    /// Registration revision the reply was admitted under.
    pub registration_revision: u64,
    /// Rank-final admission ledger.
    pub report: &'inputs RecallAdmissionReport,
    /// Normalization receipt, when the stage ran.
    pub normalization: Option<&'inputs RecallNormalizationV1>,
    /// Selection receipt, when the stage ran.
    pub selection: Option<&'inputs RecallSelectionV1>,
    /// Compiled pack, when the stage ran.
    pub pack: Option<&'inputs ContextPackV1>,
    /// Candidates a host stage between selection and pack compilation
    /// withheld. Empty when no such stage withheld anything.
    pub host_withheld: &'inputs [RecallExplainHostWithholdingV1],
    /// Identities the host substituted between selection and pack
    /// compilation, as `provider candidate id -> identity the pack recorded`.
    ///
    /// A host that refuses a provider's own candidate identity renders the
    /// item under a minted stand-in, so the pack's rows no longer carry the
    /// identity selection used. Without this mapping the reconciliation would
    /// lose exactly the rows a hostile provider produced. Empty when the host
    /// substituted nothing.
    pub pack_identity_aliases: &'inputs BTreeMap<String, String>,
    /// The gate that decides what of each provider explanation is retained.
    pub redactor: &'inputs dyn RecallExplanationRedactorV1,
}

/// Derives the deterministic trace identity for one recall.
fn trace_id(request_id: &str, provider_id: &str, registration_revision: u64) -> String {
    let mut hasher = Sha256::new();
    hasher.update(request_id.as_bytes());
    hasher.update([0]);
    hasher.update(provider_id.as_bytes());
    hasher.update([0]);
    hasher.update(registration_revision.to_be_bytes());
    hex::encode(hasher.finalize())
}

fn place(
    slots: &mut [Option<RecallExplainItemV1>],
    rank_of: &BTreeMap<&str, usize>,
    candidate_id: &str,
    decision: RecallExplainHostDecisionV1,
    provider_explanation: RecallExplainProviderExplanationV1,
) -> Result<(), RecallExplainTraceError> {
    let stage = decision.stage();
    let Some(rank) = rank_of.get(candidate_id).copied() else {
        return Err(RecallExplainTraceError::UnknownCandidate {
            candidate_id: candidate_id.to_owned(),
            stage: stage.label(),
        });
    };
    let Some(slot) = slots.get_mut(rank) else {
        return Err(RecallExplainTraceError::UnknownCandidate {
            candidate_id: candidate_id.to_owned(),
            stage: stage.label(),
        });
    };
    if let Some(existing) = slot.as_ref() {
        return Err(RecallExplainTraceError::ConflictingStages {
            candidate_id: candidate_id.to_owned(),
            existing: existing.stage.label(),
            conflicting: stage.label(),
        });
    }
    let (section, tokens) = match &decision {
        RecallExplainHostDecisionV1::Injected { section, tokens } => {
            (Some(section.clone()), Some(*tokens))
        }
        _ => (None, None),
    };
    *slot = Some(RecallExplainItemV1 {
        candidate_id: candidate_id.to_owned(),
        provider_rank: rank,
        stage,
        host_reason_code: decision.code().to_owned(),
        host_reason_detail: decision.detail(),
        host_decision: decision,
        provider_explanation,
        section,
        tokens,
    });
    Ok(())
}

fn explanation_for(
    normalization: Option<&RecallNormalizationV1>,
    redactor: &dyn RecallExplanationRedactorV1,
    candidate_id: &str,
) -> RecallExplainProviderExplanationV1 {
    match normalization
        .and_then(|normalization| normalization.candidate(candidate_id))
        .and_then(|candidate| candidate.explanation_summary.as_deref())
    {
        None => RecallExplainProviderExplanationV1::NotProvided,
        Some(explanation) => redactor.redact(candidate_id, explanation),
    }
}

/// Builds one recall's explain trace from its admission report and whatever
/// later stages actually ran.
///
/// The returned trace holds exactly one row per candidate the provider
/// returned, at that candidate's provider rank.
///
/// # Errors
///
/// Returns [`RecallExplainTraceError`] when the supplied reports cannot be
/// reconciled into that complete partition — an unknown candidate in a stage
/// ledger, two stages claiming one candidate, or an admitted candidate no
/// stage accounts for. The builder reports the inconsistency rather than
/// emitting a trace that would read as a complete account of the recall.
pub fn build_recall_explain_trace(
    inputs: RecallExplainTraceInputsV1<'_>,
) -> Result<RecallExplainTraceV1, RecallExplainTraceError> {
    let RecallExplainTraceInputsV1 {
        provider_id,
        registration_revision,
        report,
        normalization,
        selection,
        pack,
        host_withheld,
        pack_identity_aliases,
        redactor,
    } = inputs;

    if report.received_candidate_ids.len() != report.received_count {
        return Err(RecallExplainTraceError::ReceivedLedgerMismatch {
            received_count: report.received_count,
            listed: report.received_candidate_ids.len(),
        });
    }
    let mut rank_of: BTreeMap<&str, usize> = BTreeMap::new();
    for (rank, candidate_id) in report.received_candidate_ids.iter().enumerate() {
        if rank_of.insert(candidate_id.as_str(), rank).is_some() {
            return Err(RecallExplainTraceError::DuplicateCandidate {
                candidate_id: candidate_id.clone(),
            });
        }
    }
    let mut slots: Vec<Option<RecallExplainItemV1>> =
        vec![None; report.received_candidate_ids.len()];

    for denied in &report.denied {
        place(
            &mut slots,
            &rank_of,
            &denied.candidate_id,
            RecallExplainHostDecisionV1::Denied {
                reason: denied.reason.clone(),
            },
            // A denied candidate never reached normalization, so the host
            // never retained its explanation. This is a typed absence, not a
            // withholding the host performed.
            RecallExplainProviderExplanationV1::NotProvided,
        )?;
    }

    if let Some(selection) = selection {
        for dedup in &selection.deduplicated {
            place(
                &mut slots,
                &rank_of,
                &dedup.candidate_id,
                RecallExplainHostDecisionV1::Deduplicated {
                    duplicate_of_candidate_id: dedup.duplicate_of_candidate_id.clone(),
                    reason: dedup.reason,
                },
                explanation_for(normalization, redactor, &dedup.candidate_id),
            )?;
        }
        for excluded in &selection.diversity_excluded {
            place(
                &mut slots,
                &rank_of,
                &excluded.candidate_id,
                RecallExplainHostDecisionV1::DiversityExcluded {
                    similar_to_candidate_id: excluded.similar_to_candidate_id.clone(),
                    similarity_ppm: excluded.similarity_ppm,
                },
                explanation_for(normalization, redactor, &excluded.candidate_id),
            )?;
        }
        for excluded in &selection.budget_excluded {
            place(
                &mut slots,
                &rank_of,
                &excluded.candidate_id,
                RecallExplainHostDecisionV1::BudgetExcluded {
                    host_order_position: excluded.host_order_position,
                    reason: excluded.reason,
                },
                explanation_for(normalization, redactor, &excluded.candidate_id),
            )?;
        }
    }

    let mut withheld_ids: BTreeSet<&str> = BTreeSet::new();
    for withholding in host_withheld {
        withheld_ids.insert(withholding.candidate_id.as_str());
        place(
            &mut slots,
            &rank_of,
            &withholding.candidate_id,
            RecallExplainHostDecisionV1::HostWithheld {
                reason_code: withholding.reason_code.clone(),
                detail: withholding.detail.clone(),
            },
            explanation_for(normalization, redactor, &withholding.candidate_id),
        )?;
    }

    if let Some(selection) = selection {
        for selected in &selection.selected {
            let candidate_id = selected.candidate_id.as_str();
            if withheld_ids.contains(candidate_id) {
                continue;
            }
            let decision = match pack {
                None => RecallExplainHostDecisionV1::Selected,
                Some(pack) => pack_decision(
                    pack,
                    candidate_id,
                    pack_identity_aliases.get(candidate_id).map(String::as_str),
                )
                .ok_or_else(|| RecallExplainTraceError::CandidateUnaccounted {
                    candidate_id: candidate_id.to_owned(),
                })?,
            };
            place(
                &mut slots,
                &rank_of,
                candidate_id,
                decision,
                explanation_for(normalization, redactor, candidate_id),
            )?;
        }
    }

    // Whatever is still unplaced was admitted but never reached the stage
    // that would have decided it. The stage that did not run is named; the
    // candidate is never dropped.
    for (rank, slot) in slots.iter_mut().enumerate() {
        if slot.is_some() {
            continue;
        }
        let Some(candidate_id) = report.received_candidate_ids.get(rank) else {
            continue;
        };
        let decision = match (normalization, selection) {
            (_, Some(_)) => {
                return Err(RecallExplainTraceError::CandidateUnaccounted {
                    candidate_id: candidate_id.clone(),
                });
            }
            (Some(_), None) => RecallExplainHostDecisionV1::SelectionUnavailable,
            (None, None) => RecallExplainHostDecisionV1::NormalizationUnavailable,
        };
        let provider_explanation = explanation_for(normalization, redactor, candidate_id);
        *slot = Some(RecallExplainItemV1 {
            candidate_id: candidate_id.clone(),
            provider_rank: rank,
            stage: decision.stage(),
            host_reason_code: decision.code().to_owned(),
            host_reason_detail: decision.detail(),
            host_decision: decision,
            provider_explanation,
            section: None,
            tokens: None,
        });
    }

    let mut items = Vec::with_capacity(slots.len());
    for (rank, slot) in slots.into_iter().enumerate() {
        match slot {
            Some(item) => items.push(item),
            None => {
                return Err(RecallExplainTraceError::CandidateUnaccounted {
                    candidate_id: report
                        .received_candidate_ids
                        .get(rank)
                        .cloned()
                        .unwrap_or_default(),
                });
            }
        }
    }

    let token_summary = pack.map(|pack| RecallExplainTokenSummaryV1 {
        total_token_budget: pack.total_token_budget,
        advisory_token_quota: pack.advisory_token_quota,
        framing_tokens: pack.framing_tokens,
        total_tokens: pack.total_tokens,
        advisory_tokens: pack.advisory_tokens(),
        rendered_tokens: pack.rendered_tokens,
    });

    Ok(RecallExplainTraceV1 {
        trace_id: trace_id(&report.request_id, provider_id, registration_revision),
        request_id: report.request_id.clone(),
        provider_id: provider_id.to_owned(),
        registration_revision,
        requested_count: report.received_count,
        degraded: report.degraded,
        items,
        token_summary,
    })
}

/// The pack's verdict for one selected candidate.
///
/// A containment refusal is recorded under a host-minted stand-in identity —
/// the exclusion ledger must not reprint the uncontained bytes — so the
/// stand-in is re-derived here rather than the row being lost.
fn pack_decision(
    pack: &ContextPackV1,
    candidate_id: &str,
    pack_identity: Option<&str>,
) -> Option<RecallExplainHostDecisionV1> {
    let pack_identity = pack_identity.unwrap_or(candidate_id);
    let injected = pack.items().find_map(|item| match &item.provenance {
        ContextItemProvenanceV1::Provider {
            candidate_id: item_candidate_id,
            ..
        } if item_candidate_id == candidate_id || item_candidate_id == pack_identity => {
            Some((item.section, item.tokens))
        }
        _ => None,
    });
    if let Some((section, tokens)) = injected {
        return Some(RecallExplainHostDecisionV1::Injected {
            section: section.label().to_owned(),
            tokens,
        });
    }
    let stand_in = uncontained_item_identity(candidate_id);
    let aliased_stand_in = uncontained_item_identity(pack_identity);
    pack.excluded_provider_items
        .iter()
        .find(|excluded| {
            excluded.candidate_id == candidate_id
                || excluded.candidate_id == pack_identity
                || excluded.candidate_id == stand_in
                || excluded.candidate_id == aliased_stand_in
        })
        .map(|excluded| RecallExplainHostDecisionV1::PackExcluded {
            reason: excluded.reason,
        })
}
