//! Optional semantic augmentation over the canonical exact/lexical/graph query.
//!
//! The mounted application runtime owns model, projection, vector, and source
//! readiness. This module owns only scheduler routing, canonical composition,
//! and authenticated continuation pagination.

use std::sync::Arc;

use thiserror::Error;
use tracedecay_contracts::ResolvedScope;
use tracedecay_domain::{
    EphemeralSanitizedQueryViewV1, OptionalStagePublicStatus, RetrievalRequest,
    SemanticRetrievalContinuationV1,
};
use tracedecay_query::retrieval::fusion::{CompositionOutputV1, digest_candidate_set};
use tracedecay_query::retrieval::ports::RetrievalExecutionControl;
use tracedecay_query::retrieval::semantic::{
    SemanticAbstentionDispositionV1, SemanticAbstentionV1, SemanticCompositionExecutionOutcomeV1,
    SemanticQueryModeV1, SemanticQueryServiceError,
};
use tracedecay_query::retrieval::{AuthorizedQueryFallbackV1, QueryAuthorityV1};

use super::CodeIndexSchedulerRegistryV1;
use super::query_runtime::{
    ExecutedQuerySearchV1, QuerySearchExecutionErrorV1, QuerySearchExecutionRequestV1,
};

pub struct ExecutedQuerySemanticSearchV1 {
    pub query: ExecutedQuerySearchV1,
    pub semantic: SemanticAugmentationOutcomeV1,
}

pub struct SemanticAugmentedCompositionV1 {
    pub composition: CompositionOutputV1,
    pub cursor: Option<tracedecay_domain::RetrievalCursor>,
    pub hydration_budget: tracedecay_domain::RetrievalBudget,
    /// Exact canonical fallback object that authorized semantic composition.
    pub fallback: Arc<tracedecay_domain::QueryFallbackSubpayload>,
}

pub enum SemanticAugmentationOutcomeV1 {
    Augmented(Box<SemanticAugmentedCompositionV1>),
    Fallback {
        abstention: SemanticAbstentionV1,
        fallback: Arc<tracedecay_domain::QueryFallbackSubpayload>,
    },
    StrictUnavailable {
        abstention: SemanticAbstentionV1,
        fallback: Arc<tracedecay_domain::QueryFallbackSubpayload>,
    },
}

#[derive(Debug, Error)]
pub enum QuerySemanticSearchExecutionErrorV1 {
    #[error(transparent)]
    Query(#[from] QuerySearchExecutionErrorV1),
    #[error(transparent)]
    Semantic(#[from] SemanticQueryServiceError),
}

pub const fn semantic_abstention_reason(abstention: &SemanticAbstentionV1) -> &'static str {
    match abstention {
        SemanticAbstentionV1::IndexUnavailable => "semantic_index_unavailable",
        SemanticAbstentionV1::Indexing => "semantic_indexing",
        SemanticAbstentionV1::IndexDegraded => "semantic_degraded",
        SemanticAbstentionV1::IndexFailed => "semantic_failed",
        SemanticAbstentionV1::IndexStale => "semantic_generation_stale",
        SemanticAbstentionV1::IndexIncompatible => "semantic_generation_incompatible",
        SemanticAbstentionV1::CalibrationUnavailable => "calibration_unavailable",
        SemanticAbstentionV1::CalibrationInvalid => "calibration_invalid",
        SemanticAbstentionV1::CalibrationShifted => "calibration_shifted",
        SemanticAbstentionV1::NoCandidates => "semantic_no_candidates",
        SemanticAbstentionV1::BelowAcceptanceThreshold => "semantic_below_threshold",
        SemanticAbstentionV1::AmbiguousTopCandidates => "semantic_ambiguous",
        SemanticAbstentionV1::PartialCoverage => "semantic_partial",
        SemanticAbstentionV1::SemanticUnavailable => "semantic_unavailable",
        SemanticAbstentionV1::Cancelled => "semantic_cancelled",
        SemanticAbstentionV1::TimedOut => "semantic_timed_out",
        SemanticAbstentionV1::BudgetExceeded => "semantic_budget_exceeded",
        SemanticAbstentionV1::Denied => "semantic_denied",
        SemanticAbstentionV1::Stale => "semantic_stale",
        SemanticAbstentionV1::LaneFailure => "semantic_lane_failed",
    }
}

impl CodeIndexSchedulerRegistryV1 {
    /// Execute the canonical query exactly once, then optionally add semantic
    /// influence against that query's exact immutable code generation.
    #[hotpath::measure(future = true)]
    pub async fn execute_query_with_semantic<C>(
        &self,
        scope: &ResolvedScope,
        input: QuerySearchExecutionRequestV1,
        control: Arc<C>,
        mode: SemanticQueryModeV1,
    ) -> Result<ExecutedQuerySemanticSearchV1, QuerySemanticSearchExecutionErrorV1>
    where
        C: RetrievalExecutionControl + 'static,
    {
        let query = hotpath::future!(
            self.execute_controlled_query(scope, input, control.clone()),
            label = "daemon.query.semantic.canonical"
        )
        .await?;
        self.augment_executed_query(scope, query, control.as_ref(), mode)
            .await
    }

    /// Historical generation variant. The immutable generation selected by
    /// exact-source admission is the only source the semantic runtime may use.
    #[hotpath::measure(future = true)]
    pub async fn execute_query_on_generation_with_semantic<C>(
        &self,
        scope: &ResolvedScope,
        input: QuerySearchExecutionRequestV1,
        latest: super::LatestCompleteCodeIndexV1,
        control: Arc<C>,
        mode: SemanticQueryModeV1,
    ) -> Result<ExecutedQuerySemanticSearchV1, QuerySemanticSearchExecutionErrorV1>
    where
        C: RetrievalExecutionControl + 'static,
    {
        let generation = latest.generation_handle();
        let query = self
            .execute_query_search_on_generation(scope, input, latest, control.clone())
            .await?;
        self.augment_executed_query_with_generation(
            scope,
            query,
            generation,
            control.as_ref(),
            mode,
        )
        .await
    }

    async fn augment_executed_query<C>(
        &self,
        scope: &ResolvedScope,
        query: ExecutedQuerySearchV1,
        control: &C,
        mode: SemanticQueryModeV1,
    ) -> Result<ExecutedQuerySemanticSearchV1, QuerySemanticSearchExecutionErrorV1>
    where
        C: RetrievalExecutionControl + Sync,
    {
        let latest = match hotpath::future!(
            self.generation_for(scope, &query.generation),
            label = "daemon.query.semantic.generation_lookup"
        )
        .await
        {
            Ok(Some(latest)) => latest,
            Ok(None) => {
                let semantic = semantic_abstention_outcome(
                    mode,
                    SemanticAbstentionV1::IndexStale,
                    query
                        .authorized
                        .request_cursor
                        .as_ref()
                        .is_some_and(|cursor| cursor.semantic.is_some()),
                    Arc::clone(&query.authorized.fallback),
                );
                return Ok(ExecutedQuerySemanticSearchV1 { query, semantic });
            }
            Err(reason) => {
                return Err(QuerySemanticSearchExecutionErrorV1::Query(
                    QuerySearchExecutionErrorV1::ExactGenerationUnavailable(reason),
                ));
            }
        };
        self.augment_executed_query_with_generation(
            scope,
            query,
            latest.generation_handle(),
            control,
            mode,
        )
        .await
    }

    async fn augment_executed_query_with_generation<C>(
        &self,
        scope: &ResolvedScope,
        query: ExecutedQuerySearchV1,
        code_generation: Arc<tracedecay_code_index::production::CodeIndexPublishedGenerationV1>,
        control: &C,
        mode: SemanticQueryModeV1,
    ) -> Result<ExecutedQuerySemanticSearchV1, QuerySemanticSearchExecutionErrorV1>
    where
        C: RetrievalExecutionControl + Sync,
    {
        let semantic = match self
            .execute_semantic_after_query(
                scope,
                code_generation,
                query.sanitized.request(),
                query.sanitized.query_view(),
                &query.authorized,
                control,
                mode,
            )
            .await
        {
            Ok(semantic) => semantic,
            Err(SemanticQueryServiceError::StrictUnavailable(abstention)) => {
                SemanticAugmentationOutcomeV1::StrictUnavailable {
                    abstention,
                    fallback: Arc::clone(&query.authorized.fallback),
                }
            }
            Err(error) => return Err(QuerySemanticSearchExecutionErrorV1::Semantic(error)),
        };
        Ok(ExecutedQuerySemanticSearchV1 { query, semantic })
    }

    /// Compose one optional semantic lane over an already authenticated query.
    /// Model loading, vector publication, and generation preparation never
    /// start here; a missing runtime is a typed abstention.
    #[allow(clippy::too_many_arguments)]
    #[hotpath::measure(future = true)]
    pub async fn execute_semantic_after_query<C>(
        &self,
        scope: &ResolvedScope,
        code_generation: Arc<tracedecay_code_index::production::CodeIndexPublishedGenerationV1>,
        base: &RetrievalRequest,
        query_view: &EphemeralSanitizedQueryViewV1,
        authorized_query: &AuthorizedQueryFallbackV1,
        control: &C,
        mode: SemanticQueryModeV1,
    ) -> Result<SemanticAugmentationOutcomeV1, SemanticQueryServiceError>
    where
        C: RetrievalExecutionControl + Sync,
    {
        let semantic_continuation = authorized_query
            .request_cursor
            .as_ref()
            .is_some_and(|cursor| cursor.semantic.is_some());
        if let Some(cursor) = authorized_query.request_cursor.as_ref() {
            match cursor.semantic.as_ref() {
                Some(semantic) if semantic.mode != mode => {
                    return Err(SemanticQueryServiceError::InvalidCursor);
                }
                // A fallback continuation has frozen the baseline candidate
                // set. Never switch it to semantic ranking on a later page.
                None => {
                    return semantic_abstention(
                        mode,
                        SemanticAbstentionV1::SemanticUnavailable,
                        false,
                        Arc::clone(&authorized_query.fallback),
                    );
                }
                Some(_) => {}
            }
        }

        let Some(runtime) = hotpath::future!(
            self.semantic_runtime_for_scope(scope),
            label = "daemon.query.semantic.runtime_lookup"
        )
        .await
        else {
            return semantic_abstention(
                mode,
                SemanticAbstentionV1::SemanticUnavailable,
                semantic_continuation,
                Arc::clone(&authorized_query.fallback),
            );
        };
        let Some(query_authority) = hotpath::future!(
            self.query_authority_for_scope(scope),
            label = "daemon.query.semantic.composition_authority"
        )
        .await
        else {
            return semantic_abstention(
                mode,
                SemanticAbstentionV1::CalibrationUnavailable,
                semantic_continuation,
                Arc::clone(&authorized_query.fallback),
            );
        };
        let semantic_authority = query_authority
            .canonical_semantic_composition_authority()
            .map_err(|_| SemanticQueryServiceError::InvalidPolicyDecision)?;
        let execution = runtime.execute_query(
            code_generation,
            base,
            query_view,
            authorized_query,
            control,
            mode,
        )?;
        let binding = execution.binding;
        let outcome = semantic_authority.execution.execute(
            authorized_query,
            execution.outcome,
            semantic_abstention_disposition(mode),
        )?;
        match outcome {
            SemanticCompositionExecutionOutcomeV1::Fallback {
                abstention,
                fallback,
            } => Ok(semantic_abstention_outcome(
                mode,
                abstention,
                semantic_continuation,
                fallback,
            )),
            SemanticCompositionExecutionOutcomeV1::Augmented(executed) => {
                let binding = binding.ok_or(SemanticQueryServiceError::InvalidPolicyDecision)?;
                let mut composition = executed.composition;
                let cursor = paginate_semantic_composition(
                    query_authority.as_ref(),
                    base,
                    query_view,
                    authorized_query,
                    mode,
                    &semantic_authority.profile_digest,
                    query_authority.ranking_revision(),
                    &binding,
                    &executed.rerank,
                    &mut composition,
                )?;
                Ok(SemanticAugmentationOutcomeV1::Augmented(Box::new(
                    SemanticAugmentedCompositionV1 {
                        composition,
                        cursor,
                        hydration_budget: binding.semantic_budget,
                        fallback: executed.fallback,
                    },
                )))
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn paginate_semantic_composition(
    query_authority: &QueryAuthorityV1,
    request: &RetrievalRequest,
    query_view: &EphemeralSanitizedQueryViewV1,
    authorized_query: &AuthorizedQueryFallbackV1,
    mode: SemanticQueryModeV1,
    profile_digest: &tracedecay_domain::ManifestDigest,
    semantic_ranking_revision: &tracedecay_domain::ComponentRevision,
    binding: &tracedecay_application::semantic_runtime::ProjectSemanticQueryBindingV1,
    rerank: &OptionalStagePublicStatus,
    composition: &mut CompositionOutputV1,
) -> Result<Option<tracedecay_domain::RetrievalCursor>, SemanticQueryServiceError> {
    let candidate_set_digest = digest_candidate_set(&composition.ranked_candidates)
        .map_err(|_| SemanticQueryServiceError::InvalidCursor)?;
    let ranking_revision =
        tracedecay_domain::RankingRevision::new(semantic_ranking_revision.as_str().to_owned())
            .map_err(|_| SemanticQueryServiceError::InvalidCursor)?;
    let supplied_semantic = authorized_query
        .request_cursor
        .as_ref()
        .and_then(|cursor| cursor.semantic.as_ref());
    let projection_key = binding.projection.projection_key();
    if supplied_semantic.is_some_and(|cursor| {
        cursor.mode != mode
            || cursor.profile_id != composition.profile_id
            || cursor.profile_digest != *profile_digest
            || cursor.code_generation != binding.source_generation
            || cursor.vector_generation != binding.vector_generation
            || cursor.model_artifact_digest != binding.model_artifact_digest
            || cursor.execution_provider != binding.execution_provider
            || cursor.projection_key != *projection_key
            || cursor.search_index_key != binding.search_index_key
            || cursor.capability_manifest_digest != binding.capability_manifest_digest
            || cursor.privacy_domain != binding.privacy_domain
            || cursor.privacy_key_epoch != binding.privacy_key_epoch
            || cursor.source_scope != binding.source_scope
            || cursor.candidate_set_digest != candidate_set_digest
            || cursor.public_lane_statuses != composition.public_lane_statuses
            || cursor.lane_checkpoints != composition.lane_checkpoints
            || cursor.ranking_revision != ranking_revision
    }) {
        return Err(SemanticQueryServiceError::InvalidCursor);
    }
    if composition.ranked_candidates.is_empty() {
        return if authorized_query.request_cursor.is_none() {
            Ok(None)
        } else {
            Err(SemanticQueryServiceError::InvalidCursor)
        };
    }
    let semantic_start = supplied_semantic.map_or(0, |cursor| cursor.next_ordinal as usize);
    if semantic_start >= composition.ranked_candidates.len() {
        return Err(SemanticQueryServiceError::InvalidCursor);
    }
    let semantic_page_size = usize::try_from(binding.semantic_budget.max_hydrated_results)
        .map_err(|_| SemanticQueryServiceError::InvalidCursor)?
        .min(authorized_query.page_size);
    if semantic_page_size == 0 {
        return Err(SemanticQueryServiceError::InvalidCursor);
    }
    let semantic_end = semantic_start
        .saturating_add(semantic_page_size)
        .min(composition.ranked_candidates.len());
    let page_len = semantic_end.saturating_sub(semantic_start);
    let query_start = authorized_query
        .request_cursor
        .as_ref()
        .map_or(0, |cursor| cursor.next_ordinal as usize);
    if query_start > authorized_query.composition.ranked_candidates.len() {
        return Err(SemanticQueryServiceError::InvalidCursor);
    }
    let query_end = query_start
        .saturating_add(page_len)
        .min(authorized_query.composition.ranked_candidates.len());
    let cursor = if semantic_end < composition.ranked_candidates.len() {
        let mut cursor = query_authority
            .continuation_cursor_at(
                request,
                query_view,
                &authorized_query.composition,
                query_end,
            )
            .map_err(|_| SemanticQueryServiceError::InvalidCursor)?;
        query_authority
            .bind_semantic_continuation(
                &mut cursor,
                SemanticRetrievalContinuationV1 {
                    mode,
                    profile_id: composition.profile_id.clone(),
                    profile_digest: profile_digest.clone(),
                    code_generation: binding.source_generation.clone(),
                    vector_generation: binding.vector_generation.clone(),
                    model_artifact_digest: binding.model_artifact_digest.clone(),
                    execution_provider: binding.execution_provider,
                    projection_key: projection_key.clone(),
                    search_index_key: binding.search_index_key.clone(),
                    capability_manifest_digest: binding.capability_manifest_digest.clone(),
                    privacy_domain: binding.privacy_domain.clone(),
                    privacy_key_epoch: binding.privacy_key_epoch,
                    source_scope: binding.source_scope.clone(),
                    candidate_set_digest,
                    public_lane_statuses: composition.public_lane_statuses.clone(),
                    lane_checkpoints: composition.lane_checkpoints.clone(),
                    ranking_revision,
                    rerank: rerank.clone(),
                    candidate_count: u32::try_from(composition.ranked_candidates.len())
                        .map_err(|_| SemanticQueryServiceError::InvalidCursor)?,
                    next_ordinal: u32::try_from(semantic_end)
                        .map_err(|_| SemanticQueryServiceError::InvalidCursor)?,
                },
            )
            .map_err(|_| SemanticQueryServiceError::InvalidCursor)?;
        Some(cursor)
    } else {
        None
    };
    let mut ranked = std::mem::take(&mut composition.ranked_candidates);
    composition.ranked_candidates = if semantic_start == 0 {
        ranked.truncate(semantic_end);
        ranked
    } else {
        ranked.drain(semantic_start..semantic_end).collect()
    };
    Ok(cursor)
}

fn semantic_abstention(
    mode: SemanticQueryModeV1,
    abstention: SemanticAbstentionV1,
    semantic_continuation: bool,
    fallback: Arc<tracedecay_domain::QueryFallbackSubpayload>,
) -> Result<SemanticAugmentationOutcomeV1, SemanticQueryServiceError> {
    if semantic_continuation {
        return Ok(SemanticAugmentationOutcomeV1::StrictUnavailable {
            abstention,
            fallback,
        });
    }
    match mode {
        SemanticQueryModeV1::FallbackAllowed => Ok(SemanticAugmentationOutcomeV1::Fallback {
            abstention,
            fallback,
        }),
        SemanticQueryModeV1::StrictSemantic => {
            Err(SemanticQueryServiceError::StrictUnavailable(abstention))
        }
    }
}

fn semantic_abstention_outcome(
    mode: SemanticQueryModeV1,
    abstention: SemanticAbstentionV1,
    semantic_continuation: bool,
    fallback: Arc<tracedecay_domain::QueryFallbackSubpayload>,
) -> SemanticAugmentationOutcomeV1 {
    if semantic_continuation {
        return SemanticAugmentationOutcomeV1::StrictUnavailable {
            abstention,
            fallback,
        };
    }
    match mode {
        SemanticQueryModeV1::FallbackAllowed => SemanticAugmentationOutcomeV1::Fallback {
            abstention,
            fallback,
        },
        SemanticQueryModeV1::StrictSemantic => SemanticAugmentationOutcomeV1::StrictUnavailable {
            abstention,
            fallback,
        },
    }
}

fn semantic_abstention_disposition(mode: SemanticQueryModeV1) -> SemanticAbstentionDispositionV1 {
    match mode {
        SemanticQueryModeV1::FallbackAllowed => SemanticAbstentionDispositionV1::UseFallback,
        SemanticQueryModeV1::StrictSemantic => SemanticAbstentionDispositionV1::RejectUnavailable,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tracedecay_application::semantic_runtime::ProjectSemanticQueryBindingV1;
    use tracedecay_domain::{
        AdmittedEmbeddingProjectionKeyV1, AuthorizationRevision, CalibrationProfileId,
        ChunkerRevision, CompactCandidate, ComponentRevision, DiversityPolicy,
        EmbeddingDeviceClassV1, EmbeddingDocumentCompositionV1, EmbeddingExecutionProviderV1,
        EmbeddingMetricV1, EmbeddingNormalizationV1, EmbeddingPoolingV1, EmbeddingPrecisionV1,
        EmbeddingProjectionKeyV1, EmbeddingTruncationSideV1, EvidenceRole, ExactClass,
        FixedPointScore, FreshnessCompatibilityV1, FreshnessVectorDigest, FusedCandidate,
        FusionProfile, LogicalEvidenceId, ManifestDigest, PrincipalId, PublicRetrieverStatus,
        QueryNormalizationRevision, RankedCandidate, RetrievalAnchorId, RetrievalBudget,
        RetrievalCursorKeyId, RetrievalRequest, RetrievalScope, RetrievalSnapshot, RetrieverBatch,
        RetrieverCoverage, RetrieverKind, RetrieverOutcome, SanitizerRevision,
        ScoreDomainCalibrationV1, ScoreDomainId, SemanticSearchIndexProfileV1,
        SemanticSourceScopeV1, SingleRootScopeV1, SourceFreshness, SourceNamespace,
        SourceOccurrenceId, TemporalModeV1, UtcMicros, VectorGenerationIdV1, VectorWatermark,
    };
    use tracedecay_query::retrieval::fusion::{
        CompositionLaneInput, CompositionOutputV1, RetrievalCursorKeyringV1,
    };
    use tracedecay_query::retrieval::semantic::SemanticCalibrationProfileV1;

    use super::*;

    fn id<T>(value: &str) -> T
    where
        T: TryFrom<String>,
        T::Error: std::fmt::Debug,
    {
        T::try_from(value.to_owned()).expect("fixture identity")
    }

    fn digest<T>(byte: char) -> T
    where
        T: TryFrom<String>,
        T::Error: std::fmt::Debug,
    {
        id(&format!("sha256:{}", byte.to_string().repeat(64)))
    }

    fn budget() -> RetrievalBudget {
        RetrievalBudget {
            max_candidates_per_lane: 16,
            max_fused_candidates: 16,
            max_hydrated_results: 2,
            max_hydration_bytes: 65_536,
            deadline_micros: None,
        }
    }

    fn fallback_profile() -> FusionProfile {
        let lanes = RetrieverKind::QUERY_FALLBACK_LANES;
        let lexical_score_domain = id::<ScoreDomainId>("score.lexical.semantic-continuation.v1");
        let lexical_calibration =
            id::<CalibrationProfileId>("calibration.lexical.semantic-continuation.v1");
        FusionProfile {
            profile_id: id("profile.query.semantic-continuation.v1"),
            evaluation_result_anchor: id("evaluation.query.semantic-continuation.v1"),
            calibrations: lanes
                .into_iter()
                .map(|lane| {
                    (
                        lane,
                        id::<CalibrationProfileId>(&format!(
                            "calibration.{}.semantic-continuation.v1",
                            lane.as_str()
                        )),
                    )
                })
                .collect(),
            score_domain_calibrations: BTreeMap::from([(
                lexical_score_domain.clone(),
                ScoreDomainCalibrationV1 {
                    calibration_profile_id: lexical_calibration,
                    score_domain: lexical_score_domain,
                    raw_min_micros: 0,
                    raw_max_micros: 1_000_000,
                },
            )]),
            minimum_calibrated_feature_micros: BTreeMap::new(),
            weights_micros: [
                (RetrieverKind::ExactLiteral, 1_000_000),
                (RetrieverKind::Lexical, 500_000),
                (RetrieverKind::Graph, 250_000),
            ]
            .into_iter()
            .collect(),
            diversity_policy_id: id("diversity.query.semantic-continuation.v1"),
            rerank_policy_id: None,
            retrieval_budget: budget(),
        }
    }

    fn request() -> RetrievalRequest {
        RetrievalRequest {
            principal: id::<PrincipalId>("principal.semantic-continuation"),
            scope: RetrievalScope {
                privacy_domain: id("privacy.semantic-continuation"),
                root: SingleRootScopeV1 {
                    repository: id("repository.semantic-continuation"),
                    worktree: None,
                    reference: None,
                },
            },
            temporal_mode: TemporalModeV1::Current,
            snapshot: RetrievalSnapshot {
                watermarks: VectorWatermark::default(),
                freshness_digest: digest::<FreshnessVectorDigest>('f'),
                authorization_revision: id::<AuthorizationRevision>(
                    "authorization.semantic-continuation.v1",
                ),
                captured_at: UtcMicros(7),
            },
            profile_id: fallback_profile().profile_id,
            budget: budget(),
        }
    }

    fn authority(request: &RetrievalRequest) -> QueryAuthorityV1 {
        let profile = fallback_profile();
        QueryAuthorityV1::new(
            profile.clone(),
            DiversityPolicy {
                policy_id: profile.diversity_policy_id,
                evaluation_result_anchor: Some(profile.evaluation_result_anchor),
                per_source_namespace: None,
                per_source_instance: None,
                per_repository: None,
                per_file: None,
                per_session_or_thread: None,
                per_copy_cluster: None,
                per_evidence_role: None,
            },
            id::<ComponentRevision>("ranking.semantic-continuation.v1"),
            RetrievalCursorKeyringV1::new(
                request.scope.privacy_domain.clone(),
                id::<RetrievalCursorKeyId>("cursor-key.semantic-continuation.v1"),
                7,
                vec![7_u8; 32],
                1_000_000,
            )
            .expect("cursor keyring"),
        )
        .expect("query authority")
    }

    fn freshness(source: &str) -> SourceFreshness {
        SourceFreshness {
            source_namespace: id::<SourceNamespace>("namespace.semantic-continuation"),
            source_instance: id(source),
            source_watermark: Some(7),
            projection_watermark: Some(7),
            observed_at: UtcMicros(7),
            source_generation: Some(1),
            generation_lag: Some(0),
            compatibility: FreshnessCompatibilityV1::Current,
            policy_revision: id::<ComponentRevision>("policy.semantic-continuation.v1"),
        }
    }

    fn lexical_candidate(name: &str, ordinal: u32) -> CompactCandidate {
        CompactCandidate {
            anchor_id: id::<RetrievalAnchorId>(&format!("anchor.semantic-continuation.{name}")),
            logical_evidence_id: id::<LogicalEvidenceId>(&format!(
                "logical.semantic-continuation.{name}"
            )),
            source_occurrence_id: id::<SourceOccurrenceId>(&format!(
                "occurrence.semantic-continuation.{name}"
            )),
            file_occurrence_id: None,
            source_namespace: id("namespace.semantic-continuation"),
            repository_id: Some(id("repository.semantic-continuation")),
            session_or_thread_id: None,
            logical_copy_cluster_id: None,
            logical_copy_evidence_anchor: None,
            evidence_role: EvidenceRole::Primary,
            retriever: RetrieverKind::Lexical,
            retriever_revision: id("retriever.lexical.semantic-continuation.v1"),
            score_domain: id("score.lexical.semantic-continuation.v1"),
            raw_score: FixedPointScore(900_000_u64.saturating_sub(u64::from(ordinal) * 100_000)),
            ordinal_rank: ordinal,
            exact_admission_proof: None,
            retriever_evidence_anchor: id(&format!("evidence.semantic-continuation.{name}")),
            freshness: freshness(&format!("source.semantic-continuation.{name}")),
        }
    }

    fn fallback_lanes() -> Vec<CompositionLaneInput> {
        RetrieverKind::QUERY_FALLBACK_LANES
            .into_iter()
            .map(|lane| {
                let candidates = if lane == RetrieverKind::Lexical {
                    ["a", "b", "c"]
                        .into_iter()
                        .enumerate()
                        .map(|(ordinal, name)| lexical_candidate(name, ordinal as u32))
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                let evidence_by_occurrence = candidates
                    .iter()
                    .map(|candidate| (candidate.source_occurrence_id.clone(), ()))
                    .collect();
                CompositionLaneInput::new(
                    lane,
                    RetrieverOutcome::Complete(RetrieverBatch {
                        candidates,
                        evidence_by_occurrence,
                        coverage: RetrieverCoverage::default(),
                        continuation: None,
                    }),
                )
                .expect("fallback lane")
            })
            .collect()
    }

    fn ranked(name: &str, ordinal: u32) -> RankedCandidate {
        RankedCandidate {
            candidate: FusedCandidate {
                anchor_id: id::<RetrievalAnchorId>(&format!("anchor.semantic-continuation.{name}")),
                logical_evidence_id: id::<LogicalEvidenceId>(&format!(
                    "logical.semantic-continuation.{name}"
                )),
                occurrences: Vec::new(),
                exact_class: ExactClass::Approximate,
                utility_micros: u64::from(100 - ordinal),
                contributions: Vec::new(),
                freshness: Vec::new(),
                decisions: Vec::new(),
            },
            final_ordinal: ordinal,
        }
    }

    fn semantic_composition(profile_id: tracedecay_domain::FusionProfileId) -> CompositionOutputV1 {
        CompositionOutputV1 {
            profile_id,
            ranked_candidates: ["x", "a", "b", "c"]
                .into_iter()
                .enumerate()
                .map(|(ordinal, name)| ranked(name, ordinal as u32))
                .collect(),
            comparator_records: Vec::new(),
            internal_lane_outcomes: BTreeMap::new(),
            public_lane_statuses: [
                RetrieverKind::ExactLiteral,
                RetrieverKind::Lexical,
                RetrieverKind::Graph,
                RetrieverKind::Semantic,
            ]
            .into_iter()
            .map(|lane| (lane, PublicRetrieverStatus::Complete))
            .collect(),
            freshness: Vec::new(),
            lane_checkpoints: Vec::new(),
            dedupe_decisions: Vec::new(),
            diversity_decisions: Vec::new(),
        }
    }

    fn projection() -> AdmittedEmbeddingProjectionKeyV1 {
        EmbeddingProjectionKeyV1 {
            model_artifact_digest: digest('a'),
            tokenizer_digest: digest('b'),
            config_digest: digest('c'),
            query_instruction_digest: Some(digest('d')),
            document_instruction_digest: Some(digest('e')),
            document_composition: EmbeddingDocumentCompositionV1::SanitizedText,
            pooling: EmbeddingPoolingV1::Mean,
            truncation_side: EmbeddingTruncationSideV1::Right,
            truncation_length: 128,
            inference_batch_size: 8,
            inference_batch_bytes: 4 * 1024,
            runtime_backend: "onnx.cpu".to_owned(),
            runtime_build_revision: "runtime.v1".to_owned(),
            device_class: EmbeddingDeviceClassV1::Cpu,
            execution_provider: EmbeddingExecutionProviderV1::Cpu,
            dimensions: 2,
            metric: EmbeddingMetricV1::Cosine,
            normalization: EmbeddingNormalizationV1::L2,
            precision: EmbeddingPrecisionV1::Fp32,
            chunk_schema_revision: "chunk.v1".to_owned(),
            chunker_revision: id::<ChunkerRevision>("chunker.v1"),
            privacy_domain: id("privacy.semantic-continuation"),
            privacy_key_epoch: 7,
        }
        .admit()
        .expect("admitted projection")
    }

    fn binding() -> ProjectSemanticQueryBindingV1 {
        let projection = projection();
        let vector_generation = VectorGenerationIdV1::new(digest('1'));
        let capability_manifest_digest = digest::<ManifestDigest>('2');
        ProjectSemanticQueryBindingV1 {
            source_generation: id("code-generation.semantic-continuation.v1"),
            vector_generation: vector_generation.clone(),
            search_index_key: SemanticSearchIndexProfileV1::exact_flat_v1()
                .and_then(|profile| profile.index_key())
                .expect("search index key"),
            model_artifact_digest: projection.embedding_key().model_artifact_digest.clone(),
            execution_provider: EmbeddingExecutionProviderV1::Cpu,
            capability_manifest_digest: capability_manifest_digest.clone(),
            privacy_domain: id("privacy.semantic-continuation"),
            privacy_key_epoch: 7,
            source_scope: SemanticSourceScopeV1 {
                project_id: id("project.semantic-continuation"),
                repository_id: id("repository.semantic-continuation"),
                worktree_id: id("worktree.semantic-continuation"),
                reference: None,
            },
            semantic_budget: budget(),
            calibration: SemanticCalibrationProfileV1::jina_exact_flat_v1(
                projection.projection_key().clone(),
                vector_generation,
                capability_manifest_digest,
            )
            .expect("semantic calibration"),
            projection,
        }
    }

    fn anchors(composition: &CompositionOutputV1) -> Vec<String> {
        composition
            .ranked_candidates
            .iter()
            .map(|candidate| candidate.candidate.anchor_id.as_str().to_owned())
            .collect()
    }

    #[test]
    fn semantic_second_page_round_trips_distinct_order_and_refuses_baseline_fallback() {
        let request = request();
        let query_view = EphemeralSanitizedQueryViewV1::sanitize(
            "semantic continuation",
            id::<SanitizerRevision>("sanitizer.semantic-continuation.v1"),
            id::<QueryNormalizationRevision>("normalization.semantic-continuation.v1"),
        )
        .expect("query view");
        let authority = authority(&request);
        let authorized = authority
            .compose(&request, &query_view, fallback_lanes(), 2, None)
            .expect("first baseline page");
        assert_eq!(
            authorized
                .fallback
                .ordered_candidates
                .iter()
                .map(|candidate| candidate.candidate.anchor_id.as_str())
                .collect::<Vec<_>>(),
            vec![
                "anchor.semantic-continuation.a",
                "anchor.semantic-continuation.b"
            ]
        );
        let fallback = Arc::clone(&authorized.fallback);
        let binding = binding();
        let mut semantic =
            semantic_composition(id("profile.query-semantic.jina-cosine-exact-flat.v1"));
        let cursor = paginate_semantic_composition(
            &authority,
            &request,
            &query_view,
            &authorized,
            SemanticQueryModeV1::FallbackAllowed,
            &digest('3'),
            authority.ranking_revision(),
            &binding,
            &OptionalStagePublicStatus::NotRequested,
            &mut semantic,
        )
        .expect("first semantic page")
        .expect("semantic continuation");
        assert_eq!(
            anchors(&semantic),
            vec![
                "anchor.semantic-continuation.x",
                "anchor.semantic-continuation.a"
            ]
        );
        assert!(cursor.semantic.is_some());
        let encoded_cursor = serde_json::to_string(&cursor).expect("encode semantic cursor");
        assert!(
            encoded_cursor.len() <= tracedecay_domain::RETRIEVAL_CURSOR_MAX_ENVELOPE_BYTES,
            "producer emitted a cursor rejected by the bounded decoder"
        );
        let cursor: tracedecay_domain::RetrievalCursor =
            serde_json::from_str(&encoded_cursor).expect("decode semantic cursor");

        let mut tampered_cursor = cursor.clone();
        tampered_cursor
            .semantic
            .as_mut()
            .expect("semantic continuation")
            .candidate_set_digest = digest('9');
        assert!(matches!(
            authority.compose(
                &request,
                &query_view,
                fallback_lanes(),
                2,
                Some(&tampered_cursor),
            ),
            Err(
                tracedecay_query::retrieval::QueryAuthorityErrorV1::Retrieval(
                    tracedecay_domain::RetrievalError::CursorAuthenticationFailed
                )
            )
        ));

        let second_authorized = authority
            .compose(&request, &query_view, fallback_lanes(), 2, Some(&cursor))
            .expect("authenticated second baseline page");
        let mut second_semantic =
            semantic_composition(id("profile.query-semantic.jina-cosine-exact-flat.v1"));
        let exhausted = paginate_semantic_composition(
            &authority,
            &request,
            &query_view,
            &second_authorized,
            SemanticQueryModeV1::FallbackAllowed,
            &digest('3'),
            authority.ranking_revision(),
            &binding,
            &OptionalStagePublicStatus::NotRequested,
            &mut second_semantic,
        )
        .expect("second semantic page");
        assert!(exhausted.is_none());
        assert_eq!(
            anchors(&second_semantic),
            vec![
                "anchor.semantic-continuation.b",
                "anchor.semantic-continuation.c"
            ]
        );

        for abstention in [
            SemanticAbstentionV1::SemanticUnavailable,
            SemanticAbstentionV1::Indexing,
        ] {
            let outcome = semantic_abstention_outcome(
                SemanticQueryModeV1::FallbackAllowed,
                abstention.clone(),
                cursor.semantic.is_some(),
                Arc::clone(&fallback),
            );
            assert!(matches!(
                outcome,
                SemanticAugmentationOutcomeV1::StrictUnavailable {
                    abstention: observed,
                    ..
                } if observed == abstention
            ));
        }
    }
}
