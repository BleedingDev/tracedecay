use std::sync::Arc;

use tracedecay_code_index::production::CodeIndexPublishedGenerationV1;
use tracedecay_domain::{
    AdmittedEmbeddingProjectionKeyV1, CodeGenerationId, EmbeddingExecutionProviderV1,
    EphemeralSanitizedQueryViewV1, ManifestDigest, PrivacyDomainId, QueryFallbackSubpayload,
    RetrievalBudget, RetrievalRequest, SemanticSearchIndexKeyV1, SemanticSourceScopeV1,
    VectorGenerationIdV1,
};
use tracedecay_query::retrieval::AuthorizedQueryFallbackV1;
use tracedecay_query::retrieval::ports::RetrievalExecutionControl;
use tracedecay_query::retrieval::semantic::{
    CalibratedSemanticQueryService, CompleteSemanticGenerationV1, SemanticCalibrationProfileV1,
    SemanticCodeRetriever, SemanticIndexStateV1, SemanticLaneReadinessV1, SemanticLaneRetriever,
    SemanticQueryModeV1, SemanticQueryServiceError, SemanticQueryServiceOutcomeV1,
    SemanticRetrievalRequestV1, SemanticVectorReadPort,
};
use tracedecay_semantic::{DaemonSemanticRuntimeHandleV1, SemanticEvaluationQueryFactoryV1};
use tracedecay_semantic_contracts::{
    SemanticRuntimeScheduleFailureV1, SemanticRuntimeScheduleStatusV1,
};

use super::runtime::ProjectSemanticRuntimeV1;

/// Exact serving identities used for both semantic execution and cursor
/// sealing. Callers must use this binding instead of rereading current runtime
/// state after a query, because activation may advance concurrently.
#[derive(Clone, Debug)]
pub struct ProjectSemanticQueryBindingV1 {
    pub source_generation: CodeGenerationId,
    pub vector_generation: VectorGenerationIdV1,
    pub projection: AdmittedEmbeddingProjectionKeyV1,
    pub search_index_key: SemanticSearchIndexKeyV1,
    pub model_artifact_digest: ManifestDigest,
    pub execution_provider: EmbeddingExecutionProviderV1,
    pub capability_manifest_digest: ManifestDigest,
    pub privacy_domain: PrivacyDomainId,
    pub privacy_key_epoch: u64,
    pub source_scope: SemanticSourceScopeV1,
    pub semantic_budget: RetrievalBudget,
    pub calibration: SemanticCalibrationProfileV1,
}

pub struct ProjectSemanticQueryExecutionV1 {
    pub outcome: SemanticQueryServiceOutcomeV1,
    /// Present only when execution was pinned to a complete immutable semantic
    /// publication. Typed fallback outcomes have no semantic cursor binding.
    pub binding: Option<ProjectSemanticQueryBindingV1>,
}

impl ProjectSemanticRuntimeV1 {
    #[allow(clippy::too_many_arguments)]
    #[hotpath::measure(label = "semantic.query.project")]
    pub fn execute_query<C>(
        &self,
        code_generation: Arc<CodeIndexPublishedGenerationV1>,
        base: &RetrievalRequest,
        query_view: &EphemeralSanitizedQueryViewV1,
        authorized_query: &AuthorizedQueryFallbackV1,
        control: &C,
        mode: SemanticQueryModeV1,
    ) -> Result<ProjectSemanticQueryExecutionV1, SemanticQueryServiceError>
    where
        C: RetrievalExecutionControl + Sync,
    {
        if base.profile_id != authorized_query.composition.profile_id {
            return unavailable_project_execution(
                SemanticIndexStateV1::Stale,
                mode,
                Arc::clone(&authorized_query.fallback),
            );
        }
        let serving = match self.query_snapshot() {
            Ok(Some(serving)) => serving,
            Ok(None) => {
                return unavailable_project_execution(
                    status_index_state(&self.handle),
                    mode,
                    Arc::clone(&authorized_query.fallback),
                );
            }
            Err(state) => {
                return unavailable_project_execution(
                    state,
                    mode,
                    Arc::clone(&authorized_query.fallback),
                );
            }
        };
        if code_generation.manifest().generation_id != serving.pointer.source_generation
            || code_generation.capability().manifest_digest != serving.capability_manifest_digest
        {
            return unavailable_project_execution(
                SemanticIndexStateV1::Stale,
                mode,
                Arc::clone(&authorized_query.fallback),
            );
        }
        let Some(factory) = self.handle.query_factory(
            &serving.pointer.source_generation,
            &serving.pointer.generation,
            &serving.pointer.projection_key,
        ) else {
            let state = match self.handle.current() {
                Some(current) if current.projection_key != serving.pointer.projection_key => {
                    SemanticIndexStateV1::Incompatible
                }
                Some(current) if current != serving.pointer.clone() => SemanticIndexStateV1::Stale,
                _ => status_index_state(&self.handle),
            };
            return unavailable_project_execution(
                state,
                mode,
                Arc::clone(&authorized_query.fallback),
            );
        };
        let request = SemanticRetrievalRequestV1 {
            base: base.clone(),
            source_scope: serving.vectors.source_scope().clone(),
            query_digest: authorized_query.query_digest.clone(),
            query_view,
            projection: &serving.projection,
            search_index_key: &serving.search_index_key,
            capability_manifest_digest: serving.capability_manifest_digest.clone(),
            vector_generation: serving.pointer.generation.clone(),
            code_generation: serving.pointer.source_generation.clone(),
            budget: base.budget,
        };
        let outcome = execute_ready_semantic_query(
            factory,
            &request,
            &serving.generation,
            Some(&serving.calibration),
            serving.vectors.as_ref(),
            control,
            mode,
            Arc::clone(&authorized_query.fallback),
        )?;
        let binding =
            matches!(&outcome, SemanticQueryServiceOutcomeV1::Augmented { .. }).then(|| {
                let embedding_key = serving.projection.embedding_key();
                ProjectSemanticQueryBindingV1 {
                    source_generation: serving.pointer.source_generation.clone(),
                    vector_generation: serving.pointer.generation.clone(),
                    projection: serving.projection.clone(),
                    search_index_key: serving.search_index_key.clone(),
                    model_artifact_digest: embedding_key.model_artifact_digest.clone(),
                    execution_provider: embedding_key.execution_provider.clone(),
                    capability_manifest_digest: serving.capability_manifest_digest.clone(),
                    privacy_domain: embedding_key.privacy_domain.clone(),
                    privacy_key_epoch: embedding_key.privacy_key_epoch,
                    source_scope: serving.vectors.source_scope().clone(),
                    semantic_budget: base.budget,
                    calibration: serving.calibration.clone(),
                }
            });
        Ok(ProjectSemanticQueryExecutionV1 { outcome, binding })
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_ready_semantic_query<V, C>(
    factory: SemanticEvaluationQueryFactoryV1,
    request: &SemanticRetrievalRequestV1<'_>,
    generation: &CompleteSemanticGenerationV1,
    calibration: Option<&SemanticCalibrationProfileV1>,
    vectors: &V,
    control: &C,
    mode: SemanticQueryModeV1,
    fallback: Arc<QueryFallbackSubpayload>,
) -> Result<SemanticQueryServiceOutcomeV1, SemanticQueryServiceError>
where
    V: SemanticVectorReadPort,
    C: RetrievalExecutionControl + Sync,
{
    let embedder = factory.create(control, request.budget.deadline_micros);
    let lane = SemanticCodeRetriever::new(&embedder, vectors, control);
    execute_calibrated_query(
        &lane,
        SemanticLaneReadinessV1::Ready {
            request,
            generation,
            calibration,
        },
        mode,
        fallback,
    )
}

fn unavailable_project_execution(
    state: SemanticIndexStateV1,
    mode: SemanticQueryModeV1,
    fallback: Arc<QueryFallbackSubpayload>,
) -> Result<ProjectSemanticQueryExecutionV1, SemanticQueryServiceError> {
    let outcome = execute_calibrated_query(
        &UnavailableSemanticLane,
        SemanticLaneReadinessV1::Unavailable(state),
        mode,
        fallback,
    )?;
    Ok(ProjectSemanticQueryExecutionV1 {
        outcome,
        binding: None,
    })
}

fn execute_calibrated_query<'a, L>(
    lane: &'a L,
    readiness: SemanticLaneReadinessV1<'a>,
    mode: SemanticQueryModeV1,
    fallback: Arc<QueryFallbackSubpayload>,
) -> Result<SemanticQueryServiceOutcomeV1, SemanticQueryServiceError>
where
    L: SemanticLaneRetriever,
{
    CalibratedSemanticQueryService::new(lane).execute_mode(readiness, mode, fallback)
}

fn status_index_state(handle: &DaemonSemanticRuntimeHandleV1) -> SemanticIndexStateV1 {
    match handle.status() {
        SemanticRuntimeScheduleStatusV1::Unavailable => SemanticIndexStateV1::Unavailable,
        SemanticRuntimeScheduleStatusV1::Indexing { .. } => SemanticIndexStateV1::Indexing,
        SemanticRuntimeScheduleStatusV1::Failed { reason, .. } => match reason {
            SemanticRuntimeScheduleFailureV1::Cancelled
            | SemanticRuntimeScheduleFailureV1::DeadlineExceeded => {
                SemanticIndexStateV1::Unavailable
            }
            SemanticRuntimeScheduleFailureV1::Artifact
            | SemanticRuntimeScheduleFailureV1::ArtifactDetail(_)
            | SemanticRuntimeScheduleFailureV1::Runtime
            | SemanticRuntimeScheduleFailureV1::Projection
            | SemanticRuntimeScheduleFailureV1::ProjectionDetail(_)
            | SemanticRuntimeScheduleFailureV1::Publication
            | SemanticRuntimeScheduleFailureV1::PublicationDetail(_) => {
                SemanticIndexStateV1::Failed
            }
        },
        SemanticRuntimeScheduleStatusV1::Current { .. } => SemanticIndexStateV1::Unavailable,
    }
}

struct UnavailableSemanticLane;

impl SemanticLaneRetriever for UnavailableSemanticLane {
    fn retrieve_semantic(
        &self,
        _request: &SemanticRetrievalRequestV1<'_>,
    ) -> Result<
        tracedecay_domain::RetrieverOutcome<
            tracedecay_domain::RetrieverBatch<
                tracedecay_query::retrieval::semantic::CodeSemanticEvidenceV1,
            >,
        >,
        tracedecay_query::retrieval::ports::RetrievalPortError,
    > {
        Err(
            tracedecay_query::retrieval::ports::RetrievalPortError::Contract(
                "unavailable semantic lane was executed".to_owned(),
            ),
        )
    }
}
