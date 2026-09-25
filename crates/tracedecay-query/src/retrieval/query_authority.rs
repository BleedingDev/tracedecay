//! Authenticated production authority for the canonical query fallback.
//!
//! Construction requires an immutable fallback policy and a daemon-owned
//! query/cursor keyring. The policy may be the checked-in core fallback or an
//! evaluated replacement; this module does not choose weights, mint
//! calibration identities, or generate key material.

use std::collections::BTreeSet;
use std::sync::Arc;

use thiserror::Error;
use tracedecay_domain::{
    CalibrationProfileId, ComponentRevision, DiversityPolicy, EphemeralSanitizedQueryViewV1,
    FusionProfile, FusionProfileId, ManifestDigest, PrivacyDomainId, QueryDigest,
    QueryFallbackSubpayload, RetrievalAnchorId, RetrievalContractError, RetrievalCursor,
    RetrievalCursorKeyId, RetrievalError, RetrievalRequest, RetrieverBatch, RetrieverKind,
    RetrieverOutcome, ScoreDomainCalibrationV1, ScoreDomainId, canonical_sha256,
};

use super::evidence_lanes::{TaskSessionCandidateSelectionV1, TaskSessionLaneEvidenceV1};
use super::fusion::{
    CompositionKernel, CompositionLaneInput, CompositionOutputV1, CompositionPageV1,
    FusionStageError, FusionStageInput, QueryDigestAuthenticationError, RetrievalCursorKeyringV1,
};
use super::semantic::SemanticCompositionExecutionAuthorityV1;

/// Immutable comparator/ranking revision shared by the query evaluator,
/// production authority, and cursor validation.
pub const QUERY_RANKING_REVISION_V1: &str = "ranking.candidate.v1";
/// Versioned request-local cursor lifetime for the canonical query authority.
pub const QUERY_CURSOR_TTL_MICROS_V1: u64 = 15 * 60 * 1_000_000;

/// Checked-in semantic composition profile layered over the mounted fallback.
pub const CANONICAL_SEMANTIC_COMPOSITION_PROFILE_ID_V1: &str =
    "profile.query-semantic.jina-cosine-exact-flat.v1";
/// Versioned calibration identity for Jina cosine distance on the CPU exact-flat lane.
pub(crate) const CANONICAL_SEMANTIC_CALIBRATION_PROFILE_ID_V1: &str =
    "calibration.semantic.jina-cosine-exact-flat.v1";
/// The fixed semantic contribution accepted by the canonical composition policy.
pub const CANONICAL_SEMANTIC_WEIGHT_MICROS_V1: u32 = 250_000;
/// Highest descending raw score, representing zero cosine distance.
pub const CANONICAL_SEMANTIC_RAW_MAX_MICROS_V1: u64 = i64::MAX as u64;
/// Lowest descending raw score, representing cosine distance `2.0` at scale `1e9`.
pub const CANONICAL_SEMANTIC_RAW_MIN_MICROS_V1: u64 =
    CANONICAL_SEMANTIC_RAW_MAX_MICROS_V1 - 2_000_000_000;

const CANONICAL_SEMANTIC_POLICY_ID_V1: &str = "policy.query-semantic.jina-cosine-exact-flat.v1";
const CANONICAL_SEMANTIC_POLICY_DIGEST_DOMAIN_V1: &str =
    "tracedecay.query.semantic-composition-policy.v1";
const CANONICAL_SEMANTIC_PROFILE_DIGEST_DOMAIN_V1: &str =
    "tracedecay.query.semantic-composition-profile.v1";

/// Canonical semantic composition policy derived from one mounted fallback authority.
///
/// The application retains ownership of immutable generation, projection, model,
/// vector, and source bindings. This value owns only deterministic composition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanonicalSemanticCompositionAuthorityV1 {
    /// Existing execution authority over the derived four-lane profile.
    pub execution: SemanticCompositionExecutionAuthorityV1,
    /// Canonical digest of the final profile, diversity policy, and ranking revision.
    pub profile_digest: ManifestDigest,
}

/// Complete authenticated query composition retained for server-side audit.
/// The fallback payload is the canonical exact/lexical/graph result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorizedQueryFallbackV1 {
    pub query_digest: QueryDigest,
    pub fallback: Arc<QueryFallbackSubpayload>,
    pub composition: CompositionOutputV1,
    /// Exact compact lane inputs the composition consumed, retained for
    /// observation. Already-ranked fallback candidates must never be treated
    /// as a lane.
    pub fallback_lanes: Vec<CompositionLaneInput>,
    pub page_size: usize,
    /// Authenticated client continuation supplied for this page. It remains
    /// outside the canonical fallback subpayload.
    pub request_cursor: Option<RetrievalCursor>,
}

/// Complete authenticated composition for every canonical retrieval lane.
///
/// Unlike [`AuthorizedQueryFallbackV1`], this result has no fallback
/// projection. The immutable composition and page retain typed lane outcomes,
/// checkpoints, score contributions, and comparator provenance for later
/// authoritative hydration and explanation rendering.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorizedFederatedRetrievalV1 {
    pub query_digest: QueryDigest,
    pub composition: CompositionOutputV1,
    pub page: CompositionPageV1,
    pub page_size: usize,
    pub request_cursor: Option<RetrievalCursor>,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum QueryAuthorityErrorV1 {
    #[error("query authority is unavailable for the admitted scope")]
    AuthorityUnavailable,
    #[error("query authority rejected its immutable profile: {0}")]
    InvalidAuthority(String),
    #[error("query does not match the accepted profile or budget")]
    RequestProfileMismatch,
    #[error("query authority method does not match the mounted authority mode")]
    AuthorityModeMismatch,
    #[error("query composition does not contain its required lanes exactly once")]
    LaneSetMismatch,
    #[error(transparent)]
    QueryAuthentication(#[from] QueryDigestAuthenticationError),
    #[error(transparent)]
    Composition(#[from] FusionStageError),
    #[error(transparent)]
    Retrieval(#[from] RetrievalError),
    #[error(transparent)]
    Contract(#[from] RetrievalContractError),
}

/// One production query profile/key authority.
///
/// The configuration owner mounts this from either the checked-in fallback
/// policy or an accepted evaluation. The durable provider owns key lifecycle;
/// only an authenticated [`QueryDigest`] and signed cursor leave this owner.
pub struct QueryAuthorityV1 {
    mode: QueryAuthorityModeV1,
    profile: FusionProfile,
    diversity: DiversityPolicy,
    kernel: CompositionKernel,
    keyring: RetrievalCursorKeyringV1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueryAuthorityModeV1 {
    Fallback,
    Federated,
}

impl QueryAuthorityV1 {
    pub fn new(
        profile: FusionProfile,
        diversity: DiversityPolicy,
        ranking_revision: ComponentRevision,
        keyring: RetrievalCursorKeyringV1,
    ) -> Result<Self, QueryAuthorityErrorV1> {
        Self::new_with_mode(
            profile,
            diversity,
            ranking_revision,
            keyring,
            QueryAuthorityModeV1::Fallback,
        )
    }

    /// Mount the same canonical composition authority for all retrieval lanes.
    ///
    /// The caller must provide an already evaluated profile covering every
    /// canonical lane. This constructor never manufactures calibrations or
    /// weights.
    pub fn new_federated(
        profile: FusionProfile,
        diversity: DiversityPolicy,
        ranking_revision: ComponentRevision,
        keyring: RetrievalCursorKeyringV1,
    ) -> Result<Self, QueryAuthorityErrorV1> {
        Self::new_with_mode(
            profile,
            diversity,
            ranking_revision,
            keyring,
            QueryAuthorityModeV1::Federated,
        )
    }

    fn new_with_mode(
        profile: FusionProfile,
        diversity: DiversityPolicy,
        ranking_revision: ComponentRevision,
        keyring: RetrievalCursorKeyringV1,
        mode: QueryAuthorityModeV1,
    ) -> Result<Self, QueryAuthorityErrorV1> {
        profile.retrieval_budget.validate()?;
        let expected_lanes = match mode {
            QueryAuthorityModeV1::Fallback => BTreeSet::from(RetrieverKind::QUERY_FALLBACK_LANES),
            QueryAuthorityModeV1::Federated => BTreeSet::from(RetrieverKind::ALL_LANES),
        };
        let calibration_lanes = profile
            .calibrations
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();
        let weight_lanes = profile
            .weights_micros
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();
        let thresholds_are_valid = profile
            .minimum_calibrated_feature_micros
            .iter()
            .all(|(lane, threshold)| expected_lanes.contains(lane) && *threshold <= 1_000_000);
        if calibration_lanes != expected_lanes
            || weight_lanes != expected_lanes
            || !thresholds_are_valid
            || profile.rerank_policy_id.is_some()
        {
            return Err(QueryAuthorityErrorV1::InvalidAuthority(
                "profile lane set does not match the mounted query authority".to_owned(),
            ));
        }
        if diversity.policy_id != profile.diversity_policy_id
            || diversity.evaluation_result_anchor.as_ref()
                != Some(&profile.evaluation_result_anchor)
        {
            return Err(QueryAuthorityErrorV1::InvalidAuthority(
                "diversity policy is not bound to the immutable profile authority".to_owned(),
            ));
        }
        Ok(Self {
            mode,
            profile,
            diversity,
            kernel: CompositionKernel::new(ranking_revision),
            keyring,
        })
    }

    pub fn profile(&self) -> &FusionProfile {
        &self.profile
    }

    pub fn privacy_domain(&self) -> &PrivacyDomainId {
        self.keyring.privacy_domain()
    }

    pub fn ranking_revision(&self) -> &ComponentRevision {
        self.kernel.ranking_revision()
    }

    /// Derive the checked-in Jina cosine semantic composition from this fallback.
    ///
    /// Baseline exact, lexical, and graph calibrations, weights, thresholds,
    /// budget, diversity caps, key authority, and ranking revision remain
    /// unchanged. The derived authority adds exactly one semantic lane and no
    /// reranker. Its policy anchor and digest are content-bound; they are not an
    /// accepted evaluation receipt or a substitute for an application-owned
    /// immutable ready binding.
    pub fn canonical_semantic_composition_authority(
        &self,
    ) -> Result<CanonicalSemanticCompositionAuthorityV1, QueryAuthorityErrorV1> {
        if self.mode != QueryAuthorityModeV1::Fallback {
            return Err(QueryAuthorityErrorV1::AuthorityModeMismatch);
        }

        let mut profile = self.profile.clone();
        let mut diversity = self.diversity.clone();
        let semantic_calibration =
            policy_identity::<CalibrationProfileId>(CANONICAL_SEMANTIC_CALIBRATION_PROFILE_ID_V1)?;
        let semantic_score_domain =
            policy_identity::<ScoreDomainId>(super::QUERY_SEMANTIC_SCORE_DOMAIN_V1)?;
        if profile
            .calibrations
            .insert(RetrieverKind::Semantic, semantic_calibration.clone())
            .is_some()
            || profile
                .score_domain_calibrations
                .insert(
                    semantic_score_domain.clone(),
                    ScoreDomainCalibrationV1 {
                        calibration_profile_id: semantic_calibration,
                        score_domain: semantic_score_domain,
                        raw_min_micros: CANONICAL_SEMANTIC_RAW_MIN_MICROS_V1,
                        raw_max_micros: CANONICAL_SEMANTIC_RAW_MAX_MICROS_V1,
                    },
                )
                .is_some()
            || profile
                .weights_micros
                .insert(RetrieverKind::Semantic, CANONICAL_SEMANTIC_WEIGHT_MICROS_V1)
                .is_some()
            || profile
                .minimum_calibrated_feature_micros
                .insert(RetrieverKind::Semantic, 0)
                .is_some()
        {
            return Err(QueryAuthorityErrorV1::InvalidAuthority(
                "fallback authority already contains semantic policy material".to_owned(),
            ));
        }
        profile.profile_id =
            policy_identity::<FusionProfileId>(CANONICAL_SEMANTIC_COMPOSITION_PROFILE_ID_V1)?;
        profile.rerank_policy_id = None;

        // Follow the checked-in core-query policy: hash a versioned provisional
        // policy, then publish that digest as the immutable policy anchor.
        let provisional_anchor =
            policy_identity::<RetrievalAnchorId>(CANONICAL_SEMANTIC_POLICY_ID_V1)?;
        profile.evaluation_result_anchor = provisional_anchor.clone();
        diversity.evaluation_result_anchor = Some(provisional_anchor);
        let policy_digest = canonical_sha256(&(
            CANONICAL_SEMANTIC_POLICY_DIGEST_DOMAIN_V1,
            &profile,
            &diversity,
            self.ranking_revision(),
        ))
        .map_err(|error| QueryAuthorityErrorV1::InvalidAuthority(error.to_string()))?;
        let policy_anchor = policy_identity::<RetrievalAnchorId>(&format!(
            "{CANONICAL_SEMANTIC_POLICY_ID_V1}.{}",
            policy_digest.as_str()
        ))?;
        profile.evaluation_result_anchor = policy_anchor.clone();
        diversity.evaluation_result_anchor = Some(policy_anchor);

        let profile_digest = canonical_sha256(&(
            CANONICAL_SEMANTIC_PROFILE_DIGEST_DOMAIN_V1,
            &profile,
            &diversity,
            self.ranking_revision(),
        ))
        .map_err(|error| QueryAuthorityErrorV1::InvalidAuthority(error.to_string()))?;
        let execution = SemanticCompositionExecutionAuthorityV1::new(
            profile,
            diversity,
            None,
            self.ranking_revision().clone(),
        )
        .map_err(|error| QueryAuthorityErrorV1::InvalidAuthority(error.to_string()))?;
        Ok(CanonicalSemanticCompositionAuthorityV1 {
            execution,
            profile_digest,
        })
    }

    pub fn task_session_score_domain(&self) -> Result<ScoreDomainId, QueryAuthorityErrorV1> {
        if self.mode != QueryAuthorityModeV1::Federated {
            return Err(QueryAuthorityErrorV1::AuthorityModeMismatch);
        }
        let calibration = self
            .profile
            .calibrations
            .get(&RetrieverKind::TaskSession)
            .ok_or_else(|| {
                QueryAuthorityErrorV1::InvalidAuthority(
                    "federated profile omits TaskSession calibration".to_owned(),
                )
            })?;
        let mut domains = self
            .profile
            .score_domain_calibrations
            .iter()
            .filter(|(_, candidate)| &candidate.calibration_profile_id == calibration)
            .map(|(domain, _)| domain.clone());
        let domain = domains.next().ok_or_else(|| {
            QueryAuthorityErrorV1::InvalidAuthority(
                "federated profile omits the TaskSession score domain".to_owned(),
            )
        })?;
        if domains.next().is_some() {
            return Err(QueryAuthorityErrorV1::InvalidAuthority(
                "federated profile has ambiguous TaskSession score domains".to_owned(),
            ));
        }
        Ok(domain)
    }

    /// Rank one exact TaskSession expansion with the active evaluated
    /// federated profile. Unrelated lanes are not represented as successful
    /// empty batches; this projection retains only the accepted TaskSession
    /// calibration, weight, score-domain mapping, diversity, cursor key, and
    /// comparator revision.
    pub fn select_task_session(
        &self,
        request: &RetrievalRequest,
        query_view: &EphemeralSanitizedQueryViewV1,
        outcome: RetrieverOutcome<RetrieverBatch<TaskSessionLaneEvidenceV1>>,
        page_size: usize,
        cursor: Option<&RetrievalCursor>,
    ) -> Result<TaskSessionCandidateSelectionV1, QueryAuthorityErrorV1> {
        if self.mode != QueryAuthorityModeV1::Federated {
            return Err(QueryAuthorityErrorV1::AuthorityModeMismatch);
        }
        self.validate_request(request)?;
        let mut profile = self.profile.clone();
        profile
            .calibrations
            .retain(|lane, _| *lane == RetrieverKind::TaskSession);
        profile
            .weights_micros
            .retain(|lane, _| *lane == RetrieverKind::TaskSession);
        let lane = CompositionLaneInput::new(RetrieverKind::TaskSession, outcome)?;
        let composition = self.kernel.compose_selected_lane(
            &FusionStageInput {
                profile,
                lanes: vec![lane],
            },
            &self.diversity,
            RetrieverKind::TaskSession,
        )?;
        let page = self.kernel.paginate(
            request,
            query_view,
            &self.keyring,
            &composition,
            page_size,
            cursor,
        )?;
        TaskSessionCandidateSelectionV1::new(page.ranked_candidates, page.cursor)
            .map_err(|error| QueryAuthorityErrorV1::InvalidAuthority(error.to_string()))
    }

    /// Authenticate one request-local sanitized query with the daemon-owned
    /// key. Only the privacy-bound digest leaves the query authority.
    pub fn authenticate_query(
        &self,
        request: &RetrievalRequest,
        query_view: &EphemeralSanitizedQueryViewV1,
    ) -> Result<QueryDigest, QueryAuthorityErrorV1> {
        self.validate_request(request)?;
        Ok(self.keyring.digest_active_query(request, query_view)?)
    }

    /// Authenticate one prepared-query cursor payload with the daemon-owned key.
    pub fn authenticate_prepared_cursor_payload(
        &self,
        request: &RetrievalRequest,
        payload_bytes: &[u8],
    ) -> Result<QueryDigest, QueryAuthorityErrorV1> {
        self.validate_request(request)?;
        Ok(self
            .keyring
            .digest_active_prepared_cursor_payload(request, payload_bytes)?)
    }

    pub fn active_query_key_id(&self) -> RetrievalCursorKeyId {
        self.keyring.active_query_key_id()
    }

    pub fn verify_authenticated_query(
        &self,
        key_id: &RetrievalCursorKeyId,
        request: &RetrievalRequest,
        query_view: &EphemeralSanitizedQueryViewV1,
        digest: &QueryDigest,
    ) -> Result<(), QueryAuthorityErrorV1> {
        self.validate_request(request)?;
        self.keyring
            .verify_query_digest_for(key_id, request, query_view, digest)?;
        Ok(())
    }

    pub fn verify_prepared_cursor_payload(
        &self,
        key_id: &RetrievalCursorKeyId,
        request: &RetrievalRequest,
        payload_bytes: &[u8],
        digest: &QueryDigest,
    ) -> Result<(), QueryAuthorityErrorV1> {
        self.validate_request(request)?;
        self.keyring
            .verify_prepared_cursor_payload_for(key_id, request, payload_bytes, digest)?;
        Ok(())
    }

    /// Compose and page the exact query lanes under the accepted immutable
    /// profile, returning the authenticated query identity and canonical
    /// fallback subpayload together.
    #[hotpath::measure(label = "query.authority.compose")]
    pub fn compose(
        &self,
        request: &RetrievalRequest,
        query_view: &EphemeralSanitizedQueryViewV1,
        lanes: Vec<CompositionLaneInput>,
        page_size: usize,
        cursor: Option<&RetrievalCursor>,
    ) -> Result<AuthorizedQueryFallbackV1, QueryAuthorityErrorV1> {
        if self.mode != QueryAuthorityModeV1::Fallback {
            return Err(QueryAuthorityErrorV1::AuthorityModeMismatch);
        }
        self.validate_request(request)?;
        validate_lane_set(&lanes, &RetrieverKind::QUERY_FALLBACK_LANES)?;
        let query_digest = self.keyring.digest_active_query(request, query_view)?;
        let fallback_lanes = lanes.clone();
        let composition = self.kernel.compose(
            &FusionStageInput {
                profile: self.profile.clone(),
                lanes,
            },
            &self.diversity,
        )?;
        let mut page = self.kernel.paginate(
            request,
            query_view,
            &self.keyring,
            &composition,
            page_size,
            cursor,
        )?;
        hotpath::gauge!("query.fusion.results").set(page.ranked_candidates.len());
        for (ordinal, candidate) in page.ranked_candidates.iter_mut().enumerate() {
            candidate.final_ordinal = ordinal as u32;
        }
        let fallback = QueryFallbackSubpayload::new(
            composition.profile_id.clone(),
            page.ranked_candidates,
            composition
                .public_lane_statuses
                .iter()
                .filter(|(lane, _)| lane.is_query_fallback_lane())
                .map(|(lane, status)| (*lane, *status))
                .collect(),
            composition.freshness.clone(),
            page.cursor,
        )?;
        Ok(AuthorizedQueryFallbackV1 {
            query_digest,
            fallback: Arc::new(fallback),
            composition,
            fallback_lanes,
            page_size,
            request_cursor: cursor.cloned(),
        })
    }

    /// Compose and page every canonical retrieval lane under the accepted
    /// immutable profile. Candidate payloads remain unhydrated; the returned
    /// page is an authenticated slice of the frozen compact candidate set.
    #[hotpath::measure(label = "query.authority.compose_federated")]
    pub fn compose_federated(
        &self,
        request: &RetrievalRequest,
        query_view: &EphemeralSanitizedQueryViewV1,
        lanes: Vec<CompositionLaneInput>,
        page_size: usize,
        cursor: Option<&RetrievalCursor>,
    ) -> Result<AuthorizedFederatedRetrievalV1, QueryAuthorityErrorV1> {
        if self.mode != QueryAuthorityModeV1::Federated {
            return Err(QueryAuthorityErrorV1::AuthorityModeMismatch);
        }
        self.validate_request(request)?;
        validate_lane_set(&lanes, &RetrieverKind::ALL_LANES)?;
        let query_digest = self.keyring.digest_active_query(request, query_view)?;
        let composition = self.kernel.compose(
            &FusionStageInput {
                profile: self.profile.clone(),
                lanes,
            },
            &self.diversity,
        )?;
        let page = self.kernel.paginate(
            request,
            query_view,
            &self.keyring,
            &composition,
            page_size,
            cursor,
        )?;
        hotpath::gauge!("query.fusion.results").set(page.ranked_candidates.len());
        Ok(AuthorizedFederatedRetrievalV1 {
            query_digest,
            composition,
            page,
            page_size,
            request_cursor: cursor.cloned(),
        })
    }

    /// Build an authenticated cursor for a frozen composition at an arbitrary
    /// ordinal. Optional semantic execution binds its continuation after this
    /// base cursor has been created.
    pub fn continuation_cursor_at(
        &self,
        request: &RetrievalRequest,
        query_view: &EphemeralSanitizedQueryViewV1,
        composition: &CompositionOutputV1,
        next_ordinal: usize,
    ) -> Result<RetrievalCursor, QueryAuthorityErrorV1> {
        self.validate_request(request)?;
        Ok(self.kernel.cursor(
            request,
            query_view,
            &self.keyring,
            composition,
            next_ordinal,
        )?)
    }

    /// Attach and re-sign an optional semantic continuation. The continuation
    /// remains nested in the authenticated cursor envelope and is never
    /// serialized into the canonical fallback subpayload when absent.
    pub fn bind_semantic_continuation(
        &self,
        cursor: &mut RetrievalCursor,
        semantic: tracedecay_domain::SemanticRetrievalContinuationV1,
    ) -> Result<(), QueryAuthorityErrorV1> {
        semantic.validate()?;
        let mut bound = cursor.clone();
        semantic.validate_for_cursor(&bound)?;
        bound.semantic = Some(semantic);
        self.keyring.resign_cursor(&mut bound)?;
        bound.validate()?;
        *cursor = bound;
        Ok(())
    }

    pub fn bind_code_source_cursor(
        &self,
        cursor: &mut RetrievalCursor,
        binding: tracedecay_domain::CodeSourceCursorBindingV1,
    ) -> Result<(), QueryAuthorityErrorV1> {
        binding.validate()?;
        cursor.code_source = Some(binding);
        cursor.validate()?;
        self.keyring.resign_cursor(cursor)?;
        Ok(())
    }

    pub fn verify_code_source_cursor(
        &self,
        cursor: &RetrievalCursor,
        expected: &tracedecay_domain::CodeSourceCursorBindingV1,
    ) -> Result<(), QueryAuthorityErrorV1> {
        expected.validate()?;
        self.keyring.verify_cursor(cursor)?;
        if cursor.code_source.as_ref() != Some(expected) {
            return Err(QueryAuthorityErrorV1::Retrieval(
                RetrievalError::CursorSetMismatch,
            ));
        }
        Ok(())
    }

    fn validate_request(&self, request: &RetrievalRequest) -> Result<(), QueryAuthorityErrorV1> {
        request.budget.validate()?;
        if request.profile_id != self.profile.profile_id
            || !budget_is_profile_compatible(&request.budget, &self.profile.retrieval_budget)
        {
            return Err(QueryAuthorityErrorV1::RequestProfileMismatch);
        }
        Ok(())
    }
}

fn policy_identity<T>(value: &str) -> Result<T, QueryAuthorityErrorV1>
where
    T: TryFrom<String>,
    T::Error: std::fmt::Display,
{
    T::try_from(value.to_owned())
        .map_err(|error| QueryAuthorityErrorV1::InvalidAuthority(error.to_string()))
}

/// Caller deadlines narrow an evaluated profile budget without changing the
/// profile's resource ceilings or identity. A request may therefore carry a
/// deadline projected by the scheduler while retaining the same accepted
/// fallback/profile binding.
fn budget_is_profile_compatible(
    request: &tracedecay_domain::RetrievalBudget,
    profile: &tracedecay_domain::RetrievalBudget,
) -> bool {
    request.max_candidates_per_lane == profile.max_candidates_per_lane
        && request.max_fused_candidates == profile.max_fused_candidates
        && request.max_hydrated_results == profile.max_hydrated_results
        && request.max_hydration_bytes == profile.max_hydration_bytes
        && match (profile.deadline_micros, request.deadline_micros) {
            (Some(profile_deadline), Some(request_deadline)) => {
                request_deadline <= profile_deadline
            }
            (Some(_), None) => false,
            (None, _) => true,
        }
}

fn validate_lane_set(
    lanes: &[CompositionLaneInput],
    required_lanes: &[RetrieverKind],
) -> Result<(), QueryAuthorityErrorV1> {
    let actual = lanes.iter().map(|lane| lane.lane).collect::<BTreeSet<_>>();
    let expected = required_lanes.iter().copied().collect::<BTreeSet<_>>();
    if lanes.len() != expected.len() || actual != expected {
        return Err(QueryAuthorityErrorV1::LaneSetMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tracedecay_domain::{
        CalibrationProfileId, DiversityPolicyId, FusionProfileId, RetrievalAnchorId,
        RetrievalBudget, ScoreDomainCalibrationV1,
    };

    use super::*;

    fn id<T>(value: &str) -> T
    where
        T: TryFrom<String>,
        T::Error: std::fmt::Debug,
    {
        T::try_from(value.to_owned()).expect("valid test identity")
    }

    fn fallback_policy() -> (FusionProfile, DiversityPolicy) {
        let evaluation_anchor = id::<RetrievalAnchorId>(
            "policy.query-fallback.v1.sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );
        let diversity_policy_id = id::<DiversityPolicyId>("diversity.candidate.v1");
        let mut calibrations = BTreeMap::new();
        let mut score_domain_calibrations = BTreeMap::new();
        let mut weights_micros = BTreeMap::new();
        for (lane, score_domain, weight) in [
            (
                RetrieverKind::ExactLiteral,
                "score.exact.fixture.v1",
                1_000_000,
            ),
            (
                RetrieverKind::Lexical,
                "score.lexical.fixture.v1",
                1_000_000,
            ),
            (RetrieverKind::Graph, "score.graph.fixture.v1", 250_000),
        ] {
            let calibration =
                id::<CalibrationProfileId>(&format!("calibration.{}.fixture.v1", lane.as_str()));
            let score_domain = id::<ScoreDomainId>(score_domain);
            calibrations.insert(lane, calibration.clone());
            score_domain_calibrations.insert(
                score_domain.clone(),
                ScoreDomainCalibrationV1 {
                    calibration_profile_id: calibration,
                    score_domain,
                    raw_min_micros: 0,
                    raw_max_micros: 1_000_000,
                },
            );
            weights_micros.insert(lane, weight);
        }
        (
            FusionProfile {
                profile_id: id::<FusionProfileId>("profile.query-fallback.fixture.v1"),
                evaluation_result_anchor: evaluation_anchor.clone(),
                calibrations,
                score_domain_calibrations,
                minimum_calibrated_feature_micros: BTreeMap::from([(
                    RetrieverKind::Lexical,
                    125_000,
                )]),
                weights_micros,
                diversity_policy_id: diversity_policy_id.clone(),
                rerank_policy_id: None,
                retrieval_budget: RetrievalBudget {
                    max_candidates_per_lane: 32,
                    max_fused_candidates: 32,
                    max_hydrated_results: 16,
                    max_hydration_bytes: 65_536,
                    deadline_micros: None,
                },
            },
            DiversityPolicy {
                policy_id: diversity_policy_id,
                evaluation_result_anchor: Some(evaluation_anchor),
                per_source_namespace: None,
                per_source_instance: None,
                per_repository: None,
                per_file: Some(2),
                per_session_or_thread: None,
                per_copy_cluster: None,
                per_evidence_role: None,
            },
        )
    }

    fn authority_with_policy(
        profile: FusionProfile,
        diversity: DiversityPolicy,
    ) -> QueryAuthorityV1 {
        let keyring = RetrievalCursorKeyringV1::new(
            id::<PrivacyDomainId>("privacy.semantic-composition.fixture"),
            id::<RetrievalCursorKeyId>("retrieval-key.semantic-composition.fixture"),
            1,
            vec![7_u8; 32],
            1_000_000,
        )
        .expect("valid keyring");
        QueryAuthorityV1::new(
            profile,
            diversity,
            id::<ComponentRevision>(QUERY_RANKING_REVISION_V1),
            keyring,
        )
        .expect("valid fallback authority")
    }

    #[test]
    fn canonical_semantic_factory_preserves_fallback_policy_and_adds_fixed_jina_lane() {
        let (profile, diversity) = fallback_policy();
        let baseline_profile = profile.clone();
        let baseline_diversity = diversity.clone();
        let authority = authority_with_policy(profile, diversity);

        let semantic = authority
            .canonical_semantic_composition_authority()
            .expect("canonical semantic authority");
        let semantic_profile = semantic.execution.profile();
        let semantic_diversity = semantic.execution.diversity();

        assert_eq!(
            semantic_profile.profile_id.as_str(),
            CANONICAL_SEMANTIC_COMPOSITION_PROFILE_ID_V1
        );
        for lane in RetrieverKind::QUERY_FALLBACK_LANES {
            assert_eq!(
                semantic_profile.calibrations.get(&lane),
                baseline_profile.calibrations.get(&lane)
            );
            assert_eq!(
                semantic_profile.weights_micros.get(&lane),
                baseline_profile.weights_micros.get(&lane)
            );
        }
        assert_eq!(
            semantic_profile
                .minimum_calibrated_feature_micros
                .get(&RetrieverKind::Lexical),
            Some(&125_000)
        );
        assert_eq!(
            semantic_profile
                .minimum_calibrated_feature_micros
                .get(&RetrieverKind::Semantic),
            Some(&0)
        );
        assert_eq!(
            semantic_profile.weights_micros[&RetrieverKind::Semantic],
            CANONICAL_SEMANTIC_WEIGHT_MICROS_V1
        );
        let semantic_score_domain =
            id::<ScoreDomainId>(super::super::QUERY_SEMANTIC_SCORE_DOMAIN_V1);
        let calibration = &semantic_profile.score_domain_calibrations[&semantic_score_domain];
        assert_eq!(
            calibration.calibration_profile_id.as_str(),
            CANONICAL_SEMANTIC_CALIBRATION_PROFILE_ID_V1
        );
        assert_eq!(calibration.raw_max_micros, 9_223_372_036_854_775_807);
        assert_eq!(calibration.raw_min_micros, 9_223_372_034_854_775_807);
        assert_eq!(
            semantic_profile.retrieval_budget,
            baseline_profile.retrieval_budget
        );
        assert!(semantic_profile.rerank_policy_id.is_none());
        assert_eq!(semantic.execution.rerank_policy(), None);
        assert_eq!(semantic_diversity.policy_id, baseline_diversity.policy_id);
        assert_eq!(semantic_diversity.per_file, baseline_diversity.per_file);
        assert_eq!(
            semantic_diversity.evaluation_result_anchor.as_ref(),
            Some(&semantic_profile.evaluation_result_anchor)
        );
        assert_ne!(
            semantic_profile.evaluation_result_anchor,
            baseline_profile.evaluation_result_anchor
        );
        assert!(
            semantic_profile
                .evaluation_result_anchor
                .as_str()
                .starts_with("policy.query-semantic.jina-cosine-exact-flat.v1.sha256:")
        );
    }

    #[test]
    fn canonical_semantic_factory_is_deterministic_and_binds_baseline_policy_changes() {
        let (profile, diversity) = fallback_policy();
        let authority = authority_with_policy(profile.clone(), diversity.clone());
        let first = authority
            .canonical_semantic_composition_authority()
            .expect("first canonical authority");
        let repeated = authority
            .canonical_semantic_composition_authority()
            .expect("repeated canonical authority");
        assert_eq!(first, repeated);

        let mut changed = profile;
        changed.weights_micros.insert(RetrieverKind::Graph, 249_999);
        let changed = authority_with_policy(changed, diversity)
            .canonical_semantic_composition_authority()
            .expect("changed canonical authority");
        assert_ne!(first.profile_digest, changed.profile_digest);
        assert_ne!(
            first.execution.profile().evaluation_result_anchor,
            changed.execution.profile().evaluation_result_anchor
        );
        assert_eq!(
            changed.execution.profile().weights_micros[&RetrieverKind::Graph],
            249_999
        );
    }

    #[test]
    fn canonical_semantic_factory_rejects_a_non_fallback_authority_mode() {
        let (profile, diversity) = fallback_policy();
        let mut authority = authority_with_policy(profile, diversity);
        authority.mode = QueryAuthorityModeV1::Federated;

        assert!(matches!(
            authority.canonical_semantic_composition_authority(),
            Err(QueryAuthorityErrorV1::AuthorityModeMismatch)
        ));
    }
}
