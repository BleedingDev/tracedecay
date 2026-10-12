//! Host-owned normalization of admitted provider recall candidates.
//!
//! Admission ([`crate::recall_admission`]) is rank-final: it decides which
//! provider candidates may be seen at all. Normalization is the next stage and
//! it decides nothing about admission — it converts the candidates that
//! already passed admission into one common candidate space that host policy
//! (deduplication, diversity, budgets, context packing) can order.
//!
//! Three rules shape this module:
//!
//! * **The native score is never rewritten.** Every normalized candidate
//!   carries the provider's [`NativeScoreV1`] verbatim, together with the
//!   digest of the exact bytes the normalized value was derived from. The
//!   host-normalized value is a *separately labelled* field
//!   ([`HostNormalizedScoreV1`]) that names the policy and revision that
//!   produced it; it never replaces, rescales, or hides the provider's own
//!   score, explanation, or ordering evidence.
//! * **Raw scores are never compared across providers or domains.** Ordering
//!   uses the normalized value only, and the set records whether the inputs
//!   were calibrated well enough for that order to be admissible evidence
//!   across providers ([`RecallNormalizationV1::cross_provider_ordering_admissible`]).
//!   A score domain that declares a degenerate range cannot be projected at
//!   all; those candidates keep their native score, are marked
//!   [`RecallRelevanceV1::Unavailable`], and hold provider order behind every
//!   normalized candidate rather than being silently blended in.
//! * **Normalization is deterministic for a fixed policy.** The projection is
//!   exact fixed-point arithmetic over the provider's own declared range with
//!   half-up rounding at a pinned scale, and ties are broken by candidate id
//!   in UTF-8 byte order. The same reply under the same policy always yields
//!   the same values in the same order, on every platform.
//!
//! Malformed native scores — a missing field, an unknown field, `NaN`,
//! `Infinity`, an exponent form, an inverted declared range, or a raw value
//! outside the provider's own declared range — are **not** normalized to some
//! neutral value. They are denied at admission
//! ([`crate::recall_admission::RecallDenialReason::NativeScoreMalformed`]), so
//! a candidate whose relevance cannot be established honestly can never reach
//! advisory content. A provider's missing confidence is represented separately
//! in the normalized common candidate space and never manufactured from a
//! native score.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use sha2::{Digest, Sha256};

use crate::recall_admission::{AdmittedRecallCandidate, ScopeBinding, TemporalState};

/// Identity of the host normalization policy implemented here: a linear
/// projection of the provider's raw value onto its own declared range,
/// oriented by the provider's declared direction.
pub const HOST_NORMALIZATION_POLICY_ID: &str =
    "tracedecay.host.recall.normalization.declared_range_linear.v1";

/// Revision of [`HOST_NORMALIZATION_POLICY_ID`]. Any change to the projection,
/// rounding, ordering, or evidence rules must increment this.
pub const HOST_NORMALIZATION_POLICY_REVISION: u64 = 3;

/// Decimal places of every emitted normalized value.
const NORMALIZED_SCALE: u32 = 6;

/// Scale factor of [`NORMALIZED_SCALE`].
const NORMALIZED_UNIT: i128 = 1_000_000;

/// Maximum integer digits accepted in a canonical decimal string.
const MAX_DECIMAL_INTEGER_DIGITS: usize = 18;

/// Maximum fraction digits accepted in a canonical decimal string.
const MAX_DECIMAL_FRACTION_DIGITS: usize = 12;

/// Maximum bytes of a canonical decimal string.
const MAX_DECIMAL_TEXT_BYTES: usize = 40;

/// Maximum bytes of a score domain identity.
const MAX_SCORE_DOMAIN_ID_BYTES: usize = 128;

/// Maximum bytes of the provider's score semantics description.
const MAX_SCORE_SEMANTICS_BYTES: usize = 512;

/// Maximum score components the recall contract allows.
pub const MAX_SCORE_COMPONENTS: usize = 32;

/// Maximum bytes of a decode diagnostic retained in a typed defect.
const MAX_DEFECT_DETAIL_BYTES: usize = 256;

/// Framing domain of the native-score digest.
const NATIVE_SCORE_DIGEST_DOMAIN: &[u8] = b"tracedecay.memory.provider.recall.native_score.v1";

/// Direction of a provider score domain.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScoreDirection {
    /// Larger raw values are more relevant.
    HigherIsBetter,
    /// Smaller raw values are more relevant.
    LowerIsBetter,
}

impl ScoreDirection {
    /// Stable wire value.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::HigherIsBetter => "higher_is_better",
            Self::LowerIsBetter => "lower_is_better",
        }
    }
}

/// Calibration the provider claims for its own score domain.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScoreCalibrationState {
    /// The provider makes no calibration claim.
    Uncalibrated,
    /// The provider calibrated the domain itself.
    ProviderCalibrated,
    /// The domain was calibrated against an external reference.
    ExternallyCalibrated,
}

impl ScoreCalibrationState {
    /// Stable wire value.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Uncalibrated => "uncalibrated",
            Self::ProviderCalibrated => "provider_calibrated",
            Self::ExternallyCalibrated => "externally_calibrated",
        }
    }
}

/// A provider-native score exactly as the provider declared it.
///
/// The host parses it to establish that a relevance can be computed at all; it
/// never rewrites any field.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NativeScoreV1 {
    /// Provider-defined score domain identity.
    pub score_domain_id: String,
    /// Version of that score domain.
    pub score_domain_version: u64,
    /// Raw score as a canonical decimal string.
    pub raw_value: String,
    /// Whether larger or smaller raw values are more relevant.
    pub direction: ScoreDirection,
    /// Inclusive lower bound the provider declares for the domain.
    pub declared_minimum: String,
    /// Inclusive upper bound the provider declares for the domain.
    pub declared_maximum: String,
    /// Calibration the provider claims.
    pub calibration_state: ScoreCalibrationState,
    /// Bounded human description of what the score means.
    pub semantics: String,
    /// Opaque provider score components, retained but never interpreted.
    pub components: BTreeMap<String, Value>,
}

/// Why one provider-native score cannot be accepted as a relevance input.
///
/// Every variant is a contract violation by the provider, so the candidate is
/// denied at admission rather than normalized to a neutral value.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(tag = "defect", rename_all = "snake_case")]
pub enum NativeScoreDefect {
    /// The score is absent, is not an object, misses a required field, adds an
    /// unknown field, or names an unknown enum value.
    Undecodable {
        /// Bounded, content-free decoder diagnostic.
        detail: String,
    },
    /// The score domain identity is empty, untrimmed, over-long, or carries
    /// control characters.
    ScoreDomainIdInvalid,
    /// The semantics description is empty, over-long, or carries control
    /// characters.
    SemanticsInvalid,
    /// The score declares more components than the contract allows.
    TooManyComponents {
        /// Components the provider declared.
        components: usize,
    },
    /// `raw_value` is not a canonical decimal string. `NaN`, `Infinity`,
    /// `-Infinity`, exponent forms, and empty values all land here.
    RawValueNotCanonicalDecimal,
    /// `declared_minimum` is not a canonical decimal string.
    DeclaredMinimumNotCanonicalDecimal,
    /// `declared_maximum` is not a canonical decimal string.
    DeclaredMaximumNotCanonicalDecimal,
    /// `declared_maximum` is below `declared_minimum`.
    DeclaredRangeInverted,
    /// `raw_value` lies outside the provider's own declared range.
    RawValueOutOfDeclaredRange,
    /// The declared values cannot be aligned to a common scale within the
    /// host's fixed-point bounds.
    ScaleOverflow,
}

impl NativeScoreDefect {
    /// Stable snake_case label for metrics and log fields.
    #[must_use]
    pub const fn label(&self) -> &'static str {
        match self {
            Self::Undecodable { .. } => "undecodable",
            Self::ScoreDomainIdInvalid => "score_domain_id_invalid",
            Self::SemanticsInvalid => "semantics_invalid",
            Self::TooManyComponents { .. } => "too_many_components",
            Self::RawValueNotCanonicalDecimal => "raw_value_not_canonical_decimal",
            Self::DeclaredMinimumNotCanonicalDecimal => "declared_minimum_not_canonical_decimal",
            Self::DeclaredMaximumNotCanonicalDecimal => "declared_maximum_not_canonical_decimal",
            Self::DeclaredRangeInverted => "declared_range_inverted",
            Self::RawValueOutOfDeclaredRange => "raw_value_out_of_declared_range",
            Self::ScaleOverflow => "scale_overflow",
        }
    }
}

/// A native score the host established it can reason about, together with the
/// digest of the exact declaration the relevance is derived from.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ValidatedNativeScoreV1 {
    score: NativeScoreV1,
    native_score_sha256: String,
}

impl ValidatedNativeScoreV1 {
    /// The provider's score, unchanged.
    #[must_use]
    pub const fn score(&self) -> &NativeScoreV1 {
        &self.score
    }

    /// Framed digest of the declared score.
    #[must_use]
    pub fn native_score_sha256(&self) -> &str {
        &self.native_score_sha256
    }

    /// Consumes the wrapper, yielding the provider's score.
    #[must_use]
    pub fn into_score(self) -> NativeScoreV1 {
        self.score
    }
}

/// Validates one provider-native score declaration.
///
/// Returns the score unchanged on success. Every failure is a typed defect the
/// caller records as an admission denial; nothing here repairs, clamps, or
/// substitutes a provider value.
///
/// # Errors
///
/// Returns the first [`NativeScoreDefect`] in contract field order.
pub fn validate_native_score(value: &Value) -> Result<ValidatedNativeScoreV1, NativeScoreDefect> {
    let score: NativeScoreV1 =
        serde_json::from_value(value.clone()).map_err(|error| NativeScoreDefect::Undecodable {
            detail: bounded_detail(&error.to_string()),
        })?;
    if !is_bounded_label(&score.score_domain_id, MAX_SCORE_DOMAIN_ID_BYTES) {
        return Err(NativeScoreDefect::ScoreDomainIdInvalid);
    }
    if !is_bounded_label(&score.semantics, MAX_SCORE_SEMANTICS_BYTES) {
        return Err(NativeScoreDefect::SemanticsInvalid);
    }
    if score.components.len() > MAX_SCORE_COMPONENTS {
        return Err(NativeScoreDefect::TooManyComponents {
            components: score.components.len(),
        });
    }
    // Establish here — once — that a relevance is derivable at all. The
    // projection itself is recomputed by `normalize_native_score` from the
    // same strings, so no derived number is carried across the admission
    // boundary where it could drift from the provider's own declaration.
    let _ = projected_units(&score)?;
    let native_score_sha256 = native_score_digest(&score)?;
    Ok(ValidatedNativeScoreV1 {
        score,
        native_score_sha256,
    })
}

/// Evidence recorded with a normalized value about the calibration of its
/// input.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScoreCalibrationEvidence {
    /// The provider calibrated the domain itself.
    ProviderCalibrated,
    /// The domain was calibrated against an external reference.
    ExternallyCalibrated,
    /// No calibration was claimed: the normalized value is a projection onto
    /// the provider's declared range and nothing more.
    DeclaredRangeOnly,
}

impl ScoreCalibrationEvidence {
    /// Stable wire value.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::ProviderCalibrated => "provider_calibrated",
            Self::ExternallyCalibrated => "externally_calibrated",
            Self::DeclaredRangeOnly => "declared_range_only",
        }
    }

    /// Whether this evidence supports ordering the value against another
    /// provider's normalized value.
    #[must_use]
    pub const fn supports_cross_provider_ordering(self) -> bool {
        matches!(self, Self::ProviderCalibrated | Self::ExternallyCalibrated)
    }

    const fn from_state(state: ScoreCalibrationState) -> Self {
        match state {
            ScoreCalibrationState::Uncalibrated => Self::DeclaredRangeOnly,
            ScoreCalibrationState::ProviderCalibrated => Self::ProviderCalibrated,
            ScoreCalibrationState::ExternallyCalibrated => Self::ExternallyCalibrated,
        }
    }
}

/// The host-owned normalized relevance of one candidate.
///
/// The provider may never supply this record; it is produced here and labelled
/// with the policy that produced it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HostNormalizedScoreV1 {
    /// Policy identity that produced the value.
    pub normalization_policy_id: String,
    /// Revision of that policy.
    pub normalization_policy_revision: u64,
    /// Normalized relevance in the closed range `0.000000`..=`1.000000`.
    pub normalized_value: String,
    /// Framed digest of the native score the value was derived from.
    pub input_native_score_digest: String,
    /// Calibration evidence of the input.
    pub calibration_evidence: ScoreCalibrationEvidence,
    /// Bounded host warnings about the value's interpretation.
    pub warnings: Vec<String>,
}

/// Why a normalized relevance could not be produced for an admitted
/// candidate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NormalizationUnavailableReason {
    /// The provider declares a single-point range, so no candidate in the
    /// domain can be ranked relative to another. The native score is retained
    /// and cross-provider ordering is forbidden.
    DegenerateDeclaredRange,
}

/// Relevance of one admitted candidate under the host policy.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RecallRelevanceV1 {
    /// A normalized relevance was produced.
    Normalized {
        /// The host-owned normalized score.
        score: HostNormalizedScoreV1,
    },
    /// No normalized relevance exists; the native score is retained and the
    /// candidate may not be cross-provider ordered.
    Unavailable {
        /// Why normalization is unavailable.
        reason: NormalizationUnavailableReason,
        /// Framed digest of the retained native score.
        input_native_score_digest: String,
        /// Bounded host warnings.
        warnings: Vec<String>,
    },
}

impl RecallRelevanceV1 {
    /// The normalized score, when one exists.
    #[must_use]
    pub const fn normalized(&self) -> Option<&HostNormalizedScoreV1> {
        match self {
            Self::Normalized { score } => Some(score),
            Self::Unavailable { .. } => None,
        }
    }

    /// Digest of the native score the relevance was derived from, in both
    /// states.
    #[must_use]
    pub fn input_native_score_digest(&self) -> &str {
        match self {
            Self::Normalized { score } => &score.input_native_score_digest,
            Self::Unavailable {
                input_native_score_digest,
                ..
            } => input_native_score_digest,
        }
    }
}

/// Pinned host normalization configuration.
///
/// Determinism is a property of this value: two runs with the same policy and
/// the same provider reply produce byte-identical normalized values and the
/// same host order.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RecallNormalizationPolicyV1 {
    policy_id: &'static str,
    policy_revision: u64,
}

impl Default for RecallNormalizationPolicyV1 {
    fn default() -> Self {
        Self {
            policy_id: HOST_NORMALIZATION_POLICY_ID,
            policy_revision: HOST_NORMALIZATION_POLICY_REVISION,
        }
    }
}

impl RecallNormalizationPolicyV1 {
    /// The declared-range linear policy pinned at `revision`.
    ///
    /// # Errors
    ///
    /// Returns [`RecallNormalizationError::UnsupportedPolicyRevision`] when
    /// `revision` does not identify the evidence rules implemented here.
    pub const fn declared_range_linear(revision: u64) -> Result<Self, RecallNormalizationError> {
        if revision != HOST_NORMALIZATION_POLICY_REVISION {
            return Err(RecallNormalizationError::UnsupportedPolicyRevision {
                policy_id: HOST_NORMALIZATION_POLICY_ID,
                requested_revision: revision,
                supported_revision: HOST_NORMALIZATION_POLICY_REVISION,
            });
        }
        Ok(Self {
            policy_id: HOST_NORMALIZATION_POLICY_ID,
            policy_revision: revision,
        })
    }

    /// Policy identity carried on every value it produces.
    #[must_use]
    pub const fn policy_id(&self) -> &'static str {
        self.policy_id
    }

    /// Pinned policy revision.
    #[must_use]
    pub const fn policy_revision(&self) -> u64 {
        self.policy_revision
    }
}

/// Evidence that the host can or cannot make a confidence claim about a
/// normalized relevance.
///
/// Confidence is candidate evidence, not a value inferred from score
/// calibration or declared-range projection. A supplied provider value is
/// preserved exactly; an explicit `null` remains explicit absence.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecallConfidenceUnavailableReason {
    /// The provider candidate carried no explicit confidence datum.
    NotProvided,
}

/// Host confidence evidence for one normalized candidate.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RecallConfidenceV1 {
    /// The provider supplied an admitted confidence value.
    Available {
        /// The exact finite value supplied by the provider in `0.0..=1.0`.
        value: Number,
    },
    /// The normalized candidate has no confidence claim.
    Unavailable {
        /// Why confidence is unavailable.
        reason: RecallConfidenceUnavailableReason,
    },
}

/// One admitted candidate in the host's common candidate space.
///
/// The native score and the provider explanation are retained verbatim
/// alongside the separately labelled host relevance and confidence evidence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NormalizedRecallCandidateV1 {
    /// Request-scoped candidate identity.
    pub candidate_id: String,
    /// Optional stable provider memory reference. Absence is supported and is
    /// never treated as a defect.
    pub stable_memory_ref: Option<String>,
    /// Canonical content digest of the candidate.
    pub content_sha256: String,
    /// Zero-based index of the candidate in the provider's own order.
    pub provider_rank: usize,
    /// The provider's score, unchanged.
    pub native_score: NativeScoreV1,
    /// Separately labelled host relevance.
    pub relevance: RecallRelevanceV1,
    /// Explicit candidate confidence state. Score calibration and range span
    /// never manufacture confidence when the provider supplied no datum.
    pub confidence: RecallConfidenceV1,
    /// Provider explanation summary, retained as evidence. Absence is
    /// supported.
    pub explanation_summary: Option<String>,
    /// Scope binding the candidate was admitted under.
    pub scope_binding: ScopeBinding,
    /// Temporal state the host computed at admission.
    pub host_temporal_state: TemporalState,
    /// Host warnings attached at admission.
    pub host_warnings: Vec<String>,
}

/// The admitted candidates of one recall in the host's common candidate space,
/// in host order.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecallNormalizationV1 {
    /// Policy identity that produced every value in the set.
    pub normalization_policy_id: String,
    /// Revision of that policy.
    pub normalization_policy_revision: u64,
    /// Candidates in deterministic host order.
    pub candidates: Vec<NormalizedRecallCandidateV1>,
    /// Whether the normalized values in this set may be ordered against
    /// another provider's normalized values. False when any input was
    /// uncalibrated or could not be normalized at all.
    pub cross_provider_ordering_admissible: bool,
    /// Bounded set-level host warnings.
    pub warnings: Vec<String>,
}

impl RecallNormalizationV1 {
    /// Provider ranks in host order.
    pub fn host_order(&self) -> impl Iterator<Item = usize> + '_ {
        self.candidates
            .iter()
            .map(|candidate| candidate.provider_rank)
    }

    /// Looks one candidate up by its request-scoped identity.
    #[must_use]
    pub fn candidate(&self, candidate_id: &str) -> Option<&NormalizedRecallCandidateV1> {
        self.candidates
            .iter()
            .find(|candidate| candidate.candidate_id == candidate_id)
    }
}

/// Failure of normalizing one admitted recall reply.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RecallNormalizationError {
    /// The caller requested a policy revision whose evidence rules are not
    /// implemented by this host.
    #[error(
        "normalization policy {policy_id} revision {requested_revision} is unsupported; supported revision is {supported_revision}"
    )]
    UnsupportedPolicyRevision {
        /// Policy whose revision was requested.
        policy_id: &'static str,
        /// Revision requested by the caller.
        requested_revision: u64,
        /// Revision whose behavior this host implements.
        supported_revision: u64,
    },
    /// An admitted candidate carried a native score the host cannot project.
    /// Admission rejects such candidates, so reaching this is a host-internal
    /// inconsistency rather than a provider fault; it is reported instead of
    /// being repaired.
    #[error("admitted candidate {candidate_id} carries a native score defect: {}", .defect.label())]
    NativeScore {
        /// Candidate whose score could not be projected.
        candidate_id: String,
        /// The defect found.
        defect: NativeScoreDefect,
    },
}

/// Converts admitted candidates into the host's common candidate space.
///
/// The returned set is ordered by host relevance, not by provider order:
/// normalized candidates come first in descending normalized value with
/// candidate id as the UTF-8 byte tie-breaker, and candidates whose
/// relevance could not be normalized follow in provider order. Provider order
/// is preserved as
/// [`NormalizedRecallCandidateV1::provider_rank`] so the reordering is always
/// explainable.
///
/// # Errors
///
/// Returns [`RecallNormalizationError::NativeScore`] when an admitted
/// candidate's native score cannot be projected.
pub fn normalize_admitted_candidates(
    policy: RecallNormalizationPolicyV1,
    admitted: &[AdmittedRecallCandidate],
) -> Result<RecallNormalizationV1, RecallNormalizationError> {
    let mut ordered: Vec<(SortKey, NormalizedRecallCandidateV1)> =
        Vec::with_capacity(admitted.len());
    let mut cross_provider_ordering_admissible = true;
    let mut uncalibrated_domains: Vec<String> = Vec::new();
    let mut unnormalizable_domains: Vec<String> = Vec::new();

    for (provider_rank, entry) in admitted.iter().enumerate() {
        let candidate = entry.candidate();
        let score = entry.native_score();
        let native_score_sha256 = entry.native_score_sha256();
        let units =
            projected_units(score).map_err(|defect| RecallNormalizationError::NativeScore {
                candidate_id: candidate.candidate_id.clone(),
                defect,
            })?;
        let relevance = relevance_from_units(policy, score, native_score_sha256, units);
        let confidence = match entry.confidence() {
            Some(value) => RecallConfidenceV1::Available {
                value: value.clone(),
            },
            None => RecallConfidenceV1::Unavailable {
                reason: RecallConfidenceUnavailableReason::NotProvided,
            },
        };
        let key = match units {
            Some(units) => {
                if !ScoreCalibrationEvidence::from_state(score.calibration_state)
                    .supports_cross_provider_ordering()
                {
                    cross_provider_ordering_admissible = false;
                    push_domain(&mut uncalibrated_domains, &score.score_domain_id);
                }
                SortKey::Normalized {
                    descending_units: -units,
                    candidate_id: candidate.candidate_id.clone(),
                }
            }
            None => {
                cross_provider_ordering_admissible = false;
                push_domain(&mut unnormalizable_domains, &score.score_domain_id);
                SortKey::Unavailable { provider_rank }
            }
        };
        ordered.push((
            key,
            NormalizedRecallCandidateV1 {
                candidate_id: candidate.candidate_id.clone(),
                stable_memory_ref: candidate.stable_memory_ref.clone(),
                content_sha256: candidate.content_sha256.clone(),
                provider_rank,
                native_score: score.clone(),
                relevance,
                confidence,
                explanation_summary: explanation_summary(&candidate.explanation),
                scope_binding: entry.scope_binding(),
                host_temporal_state: entry.host_temporal_state(),
                host_warnings: entry.warnings().to_vec(),
            },
        ));
    }

    ordered.sort_by(|left, right| left.0.cmp(&right.0));

    let mut warnings = Vec::new();
    if !uncalibrated_domains.is_empty() {
        warnings.push(format!(
            "normalized values for uncalibrated score domains [{}] are declared-range projections \
             and are not admissible cross-provider ordering evidence",
            uncalibrated_domains.join(", ")
        ));
    }
    if !unnormalizable_domains.is_empty() {
        warnings.push(format!(
            "score domains [{}] declare a degenerate range; their candidates retain the native \
             score, hold provider order, and are not cross-provider orderable",
            unnormalizable_domains.join(", ")
        ));
    }
    Ok(RecallNormalizationV1 {
        normalization_policy_id: policy.policy_id().to_owned(),
        normalization_policy_revision: policy.policy_revision(),
        candidates: ordered
            .into_iter()
            .map(|(_, candidate)| candidate)
            .collect(),
        cross_provider_ordering_admissible,
        warnings,
    })
}

/// Produces the host relevance of one native score under `policy`.
///
/// # Errors
///
/// Returns the [`NativeScoreDefect`] that prevents any projection.
pub fn normalize_native_score(
    policy: RecallNormalizationPolicyV1,
    score: &NativeScoreV1,
    native_score_sha256: &str,
) -> Result<RecallRelevanceV1, NativeScoreDefect> {
    let units = projected_units(score)?;
    Ok(relevance_from_units(
        policy,
        score,
        native_score_sha256,
        units,
    ))
}

fn relevance_from_units(
    policy: RecallNormalizationPolicyV1,
    score: &NativeScoreV1,
    native_score_sha256: &str,
    units: Option<i128>,
) -> RecallRelevanceV1 {
    let Some(units) = units else {
        return RecallRelevanceV1::Unavailable {
            reason: NormalizationUnavailableReason::DegenerateDeclaredRange,
            input_native_score_digest: native_score_sha256.to_owned(),
            warnings: vec![format!(
                "score domain {} declares declared_minimum == declared_maximum; no relative \
                 relevance exists and the native score is retained unchanged",
                score.score_domain_id
            )],
        };
    };
    let calibration_evidence = ScoreCalibrationEvidence::from_state(score.calibration_state);
    let mut warnings = Vec::new();
    if !calibration_evidence.supports_cross_provider_ordering() {
        warnings.push(format!(
            "score domain {} is uncalibrated; the normalized value is a projection onto the \
             provider's declared range and is not calibrated relevance",
            score.score_domain_id
        ));
    }
    RecallRelevanceV1::Normalized {
        score: HostNormalizedScoreV1 {
            normalization_policy_id: policy.policy_id().to_owned(),
            normalization_policy_revision: policy.policy_revision(),
            normalized_value: format_normalized(units),
            input_native_score_digest: native_score_sha256.to_owned(),
            calibration_evidence,
            warnings,
        },
    }
}

/// Deterministic host ordering key.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum SortKey {
    /// Normalized candidates first: descending relevance, then candidate id in
    /// UTF-8 byte order.
    Normalized {
        descending_units: i128,
        candidate_id: String,
    },
    /// Then candidates without a normalized relevance, in provider order.
    Unavailable { provider_rank: usize },
}

fn push_domain(domains: &mut Vec<String>, score_domain_id: &str) {
    if !domains.iter().any(|domain| domain == score_domain_id) {
        domains.push(score_domain_id.to_owned());
    }
}

fn explanation_summary(explanation: &Value) -> Option<String> {
    explanation
        .get("summary")
        .and_then(Value::as_str)
        .filter(|summary| !summary.is_empty())
        .map(str::to_owned)
}

/// Projects `score` onto `0..=NORMALIZED_UNIT`.
///
/// `Ok(None)` means the provider declared a single-point range, which is not a
/// defect but leaves no relative relevance to compute.
fn projected_units(score: &NativeScoreV1) -> Result<Option<i128>, NativeScoreDefect> {
    let raw = parse_canonical_decimal(&score.raw_value)
        .ok_or(NativeScoreDefect::RawValueNotCanonicalDecimal)?;
    let minimum = parse_canonical_decimal(&score.declared_minimum)
        .ok_or(NativeScoreDefect::DeclaredMinimumNotCanonicalDecimal)?;
    let maximum = parse_canonical_decimal(&score.declared_maximum)
        .ok_or(NativeScoreDefect::DeclaredMaximumNotCanonicalDecimal)?;

    let scale = raw.scale.max(minimum.scale).max(maximum.scale);
    let raw = rescale(raw, scale).ok_or(NativeScoreDefect::ScaleOverflow)?;
    let minimum = rescale(minimum, scale).ok_or(NativeScoreDefect::ScaleOverflow)?;
    let maximum = rescale(maximum, scale).ok_or(NativeScoreDefect::ScaleOverflow)?;

    let span = maximum
        .checked_sub(minimum)
        .ok_or(NativeScoreDefect::ScaleOverflow)?;
    if span < 0 {
        return Err(NativeScoreDefect::DeclaredRangeInverted);
    }
    if raw < minimum || raw > maximum {
        return Err(NativeScoreDefect::RawValueOutOfDeclaredRange);
    }
    if span == 0 {
        return Ok(None);
    }

    let offset = match score.direction {
        ScoreDirection::HigherIsBetter => raw.checked_sub(minimum),
        ScoreDirection::LowerIsBetter => maximum.checked_sub(raw),
    }
    .ok_or(NativeScoreDefect::ScaleOverflow)?;
    // Half-up rounding on an exact non-negative rational: (2*offset*UNIT +
    // span) / (2*span). Every operand is checked, so a domain wide enough to
    // overflow the host's fixed point is a typed defect rather than a wrapped
    // number.
    let numerator = offset
        .checked_mul(NORMALIZED_UNIT)
        .and_then(|scaled| scaled.checked_mul(2))
        .and_then(|scaled| scaled.checked_add(span))
        .ok_or(NativeScoreDefect::ScaleOverflow)?;
    let denominator = span
        .checked_mul(2)
        .ok_or(NativeScoreDefect::ScaleOverflow)?;
    let units = numerator
        .checked_div(denominator)
        .ok_or(NativeScoreDefect::ScaleOverflow)?;
    Ok(Some(units.clamp(0, NORMALIZED_UNIT)))
}

fn format_normalized(units: i128) -> String {
    let integer = units / NORMALIZED_UNIT;
    let fraction = (units % NORMALIZED_UNIT).abs();
    let width = NORMALIZED_SCALE as usize;
    format!("{integer}.{fraction:0width$}")
}

/// A canonical decimal string parsed to exact fixed point.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FixedDecimal {
    units: i128,
    scale: u32,
}

fn rescale(value: FixedDecimal, scale: u32) -> Option<i128> {
    let difference = scale.checked_sub(value.scale)?;
    let factor = 10i128.checked_pow(difference)?;
    value.units.checked_mul(factor)
}

/// Parses a canonical decimal string.
///
/// Canonical means: optional `-`, an integer part without redundant leading
/// zeros, an optional fraction of at least one digit, ASCII digits only, and
/// no negative zero. `NaN`, `nan`, `Infinity`, `-Infinity`, `inf`, `1e5`,
/// `+1`, `1.`, `.5`, and whitespace-padded values are all rejected, which is
/// exactly what makes non-finite provider scores impossible to admit.
fn parse_canonical_decimal(text: &str) -> Option<FixedDecimal> {
    if text.is_empty() || text.len() > MAX_DECIMAL_TEXT_BYTES || !text.is_ascii() {
        return None;
    }
    let (negative, magnitude) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let mut parts = magnitude.splitn(2, '.');
    let integer = parts.next()?;
    let fraction = parts.next().unwrap_or("");
    if integer.is_empty()
        || integer.len() > MAX_DECIMAL_INTEGER_DIGITS
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || (integer.len() > 1 && integer.starts_with('0'))
    {
        return None;
    }
    if magnitude.contains('.')
        && (fraction.is_empty()
            || fraction.len() > MAX_DECIMAL_FRACTION_DIGITS
            || !fraction.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return None;
    }
    let scale = u32::try_from(fraction.len()).ok()?;
    let digits = format!("{integer}{fraction}");
    let magnitude: i128 = digits.parse().ok()?;
    if negative && magnitude == 0 {
        return None;
    }
    let units = if negative { -magnitude } else { magnitude };
    Some(FixedDecimal { units, scale })
}

fn is_bounded_label(value: &str, maximum_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum_bytes
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn bounded_detail(detail: &str) -> String {
    let mut bounded = String::new();
    for character in detail.chars().filter(|character| !character.is_control()) {
        if bounded.len() + character.len_utf8() > MAX_DEFECT_DETAIL_BYTES {
            break;
        }
        bounded.push(character);
    }
    bounded
}

/// Length-framed digest of one native score declaration.
fn native_score_digest(score: &NativeScoreV1) -> Result<String, NativeScoreDefect> {
    let components =
        serde_json::to_vec(&score.components).map_err(|error| NativeScoreDefect::Undecodable {
            detail: bounded_detail(&error.to_string()),
        })?;
    let version = score.score_domain_version.to_string();
    let mut hasher = Sha256::new();
    for part in [
        NATIVE_SCORE_DIGEST_DOMAIN,
        score.score_domain_id.as_bytes(),
        version.as_bytes(),
        score.raw_value.as_bytes(),
        score.direction.as_wire().as_bytes(),
        score.declared_minimum.as_bytes(),
        score.declared_maximum.as_bytes(),
        score.calibration_state.as_wire().as_bytes(),
        score.semantics.as_bytes(),
        components.as_slice(),
    ] {
        let length = u64::try_from(part.len()).unwrap_or(u64::MAX);
        hasher.update(length.to_be_bytes());
        hasher.update(part);
    }
    Ok(hex::encode(hasher.finalize()))
}
