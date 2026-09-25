#![cfg(all(feature = "semantic-fastembed", not(windows)))]

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, Mutex};

use sha2::{Digest, Sha256};
use tracedecay_code_index::chunks::content_digest;
use tracedecay_code_index::embedding_document::{
    EmbeddingDocumentComposerV1, EmbeddingSymbolContextIndexV1,
};
use tracedecay_code_index::production::{
    CodeIndexAtomicPublicationPort, CodeIndexBuildRequestV1, CodeIndexCapturedFileV1,
    CodeIndexExecutionControlV1, CodeIndexGenerationScopeV1, CodeIndexProductionConfigV1,
    CodeIndexProductionOwnerV1, CodeIndexPublicationStoreErrorV1, CodeIndexPublishedGenerationV1,
    CodeIndexRepositoryParseIdentityV1, DAEMON_CODE_INDEX_CHUNKER_REVISION,
};
use tracedecay_code_index::projection::{
    ChunkProjectionDecisionV1, CodeChunkProjectionSink, ProjectionReceiptBuilderV1,
    ProjectionSinkErrorV1, ProjectionSinkReceiptV1, expected_request_digest,
};
use tracedecay_domain::{
    AdmittedEmbeddingProjectionKeyV1, ChunkerRevision, CodeGenerationId, CodeSearchChunkId,
    CommitId, EmbeddingDeviceClassV1, EmbeddingDocumentCompositionV1, EmbeddingExecutionProviderV1,
    EmbeddingMetricV1, EmbeddingNormalizationV1, EmbeddingPoolingV1, EmbeddingPrecisionV1,
    EmbeddingProjectionKeyV1, EmbeddingTruncationSideV1, FusionProfileId, LanguageId,
    ManifestDigest, PolicyRevisionId, PrincipalId, PrivacyDomainId, ProjectId,
    ProjectionBatchRequestV1, ProjectionKeyV1, ProjectionKindV1, ProjectionOperationV1,
    ProjectionOutcomeV1, ProjectionReplayReasonV1, PublicRetrieverStatus, QueryDigest,
    QueryFallbackSubpayload, QueryMac, QueryNormalizationRevision, RefId, RepositoryDirtyStateV1,
    RepositoryId, RetrievalBudget, RetrievalRequest, RetrievalScope, RetrievalSnapshot,
    RetrieverKind, RetrieverOutcome, SanitizationReceiptId, SanitizedCodeFileV1,
    SanitizedCodeSnapshotV1, SanitizerRevision, SemanticSearchIndexKeyV1,
    SemanticSearchIndexProfileV1, SemanticSourceScopeV1, SensitivityLevelV1, SingleRootScopeV1,
    SnapshotFileDispositionV1, TemporalModeV1, TreeId, UtcMicros, VectorWatermark, WorktreeId,
};
use tracedecay_query::retrieval::ports::RetrievalExecutionControl;
use tracedecay_query::retrieval::semantic::{
    CalibratedSemanticQueryService, CompleteSemanticGenerationV1, SemanticCalibrationProfileV1,
    SemanticCodeRetriever, SemanticLaneReadinessV1, SemanticLaneRetriever, SemanticQueryDecisionV1,
    SemanticQueryServiceOutcomeV1, SemanticRetrievalRequestV1,
};
use tracedecay_query::search_quality::packaged::{load_workload, packaged_evaluator_files};
use tracedecay_runtime_core::db::{
    Database, DatabaseAuthority, TestDatabaseRuntimeMode, TestDatabaseRuntimeScope,
};
use tracedecay_semantic::projector::{
    CanonicalChunkTokenLengthsV1, CanonicalChunkVectorEncoderV1, prepare_vector_generation_async,
};
use tracedecay_semantic::{
    CatalogedFastEmbedModelV1, DaemonSemanticRuntimeHandleV1, LoadedSemanticArtifactV1,
    ModelLifecycleErrorV1, ModelMemberSourceV1, SemanticEvaluationCancellationV1,
    SemanticEvaluationProjectionBatchCachePolicyV1, SemanticEvaluationProjectionBatchStoreV1,
    SemanticEvaluationProjectionResourcesV1, SemanticEvaluationQueryFactoryV1,
    SemanticExecutionAuthority, SemanticExecutionInterruptionV1, SemanticModelLifecycleOwnerV1,
    SemanticProjectionResumeOutcomeV1, open_local_semantic_evaluation_lifecycle,
    prepare_semantic_evaluation_projection, production_fastembed_catalog,
};
use tracedecay_semantic_contracts::SemanticResourceCeilings;
use tracedecay_store::runtime::DurableVectorAuthorityStoreV1;
use tracedecay_vector_authority::{VectorGenerationAuthority, VectorGenerationPlanV1};

use super::projection::{
    projection_request as runtime_projection_request, resume_projection_checkpoint,
};
use super::runtime::restore_current;
use super::vector_read::PublishedVectorReadPortV1;

const SOURCE_COMMIT: &str = "8312618fee8109b16be09e65f45118b4e550fa14";
const SOURCE_TREE: &str = "587aed48f162f74765da3a35fc41850523ea377d";
const PRIVACY_KEY_EPOCH: u64 = 7;

#[derive(Clone, Default)]
struct InMemoryPublicationStore {
    active: Arc<Mutex<BTreeMap<CodeIndexGenerationScopeV1, Arc<CodeIndexPublishedGenerationV1>>>>,
}

impl CodeIndexAtomicPublicationPort for InMemoryPublicationStore {
    fn load_active(
        &self,
        scope: &CodeIndexGenerationScopeV1,
    ) -> Result<Option<Arc<CodeIndexPublishedGenerationV1>>, CodeIndexPublicationStoreErrorV1> {
        Ok(self
            .active
            .lock()
            .expect("publication lock")
            .get(scope)
            .map(Arc::clone))
    }

    fn publish_atomically(
        &mut self,
        scope: &CodeIndexGenerationScopeV1,
        expected_active_generation: Option<&CodeGenerationId>,
        generation: Arc<CodeIndexPublishedGenerationV1>,
    ) -> Result<(), CodeIndexPublicationStoreErrorV1> {
        let mut active = self.active.lock().expect("publication lock");
        if active
            .get(scope)
            .map(|current| &current.manifest().generation_id)
            != expected_active_generation
        {
            return Err(CodeIndexPublicationStoreErrorV1::CompareAndSwap);
        }
        active.insert(scope.clone(), generation);
        Ok(())
    }
}

struct ApplyingProjectionSink;

impl CodeChunkProjectionSink for ApplyingProjectionSink {
    fn project_changed_chunks(
        &mut self,
        request: &ProjectionBatchRequestV1,
        receipt_builder: ProjectionReceiptBuilderV1<'_>,
    ) -> Result<ProjectionSinkReceiptV1, ProjectionSinkErrorV1> {
        let mut decisions = request
            .changes
            .added_or_changed
            .iter()
            .map(|change| ChunkProjectionDecisionV1 {
                chunk_id: change.chunk_id.clone(),
                prior_chunk_digest: change.prior_digest.clone(),
                current_chunk_digest: change.current_digest.clone(),
                operation: if change.prior_digest.is_some() {
                    ProjectionOperationV1::Updated
                } else {
                    ProjectionOperationV1::Added
                },
                outcome: ProjectionOutcomeV1::Applied,
                output_digest: change.current_digest.clone(),
            })
            .collect::<Vec<_>>();
        decisions.extend(
            request
                .changes
                .deleted
                .iter()
                .map(|change| ChunkProjectionDecisionV1 {
                    chunk_id: change.chunk_id.clone(),
                    prior_chunk_digest: change.prior_digest.clone(),
                    current_chunk_digest: None,
                    operation: ProjectionOperationV1::Deleted,
                    outcome: ProjectionOutcomeV1::Applied,
                    output_digest: None,
                }),
        );
        receipt_builder
            .build(&decisions)
            .map_err(|error| ProjectionSinkErrorV1::Rejected(error.to_string()))
    }
}

struct ActiveControl;

impl CodeIndexExecutionControlV1 for ActiveControl {
    fn is_cancelled(&self) -> bool {
        false
    }

    fn is_deadline_exceeded(&self) -> bool {
        false
    }
}

impl RetrievalExecutionControl for ActiveControl {
    fn is_cancelled(&self) -> bool {
        false
    }

    fn elapsed_micros(&self) -> u64 {
        0
    }
}

struct ActiveProjection;

impl SemanticExecutionAuthority for ActiveProjection {
    fn interruption(&self) -> Option<SemanticExecutionInterruptionV1> {
        None
    }
}

impl SemanticEvaluationCancellationV1 for ActiveProjection {}

struct FixtureModelSource {
    members: BTreeMap<String, Vec<u8>>,
}

impl ModelMemberSourceV1 for FixtureModelSource {
    fn fetch_member(
        &self,
        _model: &CatalogedFastEmbedModelV1,
        upstream_path: &str,
        destination: &Path,
    ) -> Result<(), ModelLifecycleErrorV1> {
        let bytes = self
            .members
            .get(upstream_path)
            .ok_or(ModelLifecycleErrorV1::DownloadFailed)?;
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent).map_err(|_| ModelLifecycleErrorV1::DownloadFailed)?;
        }
        std::fs::write(destination, bytes).map_err(|_| ModelLifecycleErrorV1::DownloadFailed)
    }
}

fn id<T>(value: &str) -> T
where
    T: TryFrom<String>,
    T::Error: std::fmt::Debug,
{
    T::try_from(value.to_owned()).expect("canonical fixture identity")
}

fn digest(seed: char) -> ManifestDigest {
    ManifestDigest::new(format!("sha256:{}", seed.to_string().repeat(64)))
        .expect("canonical digest")
}

fn resources() -> SemanticResourceCeilings {
    SemanticResourceCeilings {
        max_model_bytes: 1024 * 1024 * 1024,
        max_tokenizer_bytes: 64 * 1024 * 1024,
        max_resident_bytes: Some(4 * 1024 * 1024 * 1024),
        max_threads: 1,
        max_concurrent_sessions: 1,
        max_batch_size: 4,
        max_sequence_length: 8192,
        load_deadline_ms: 180_000,
    }
}

fn source_files() -> Vec<(String, &'static [u8])> {
    const DOCUMENTS: [&str; 6] = [
        "canonical",
        "coverage",
        "error",
        "repository",
        "time",
        "watermark",
    ];
    let workload = load_workload().expect("pinned search-quality workload");
    assert_eq!(workload.source_repository_commit, SOURCE_COMMIT);
    assert_eq!(workload.source_repository_tree, SOURCE_TREE);
    let packaged = packaged_evaluator_files()
        .iter()
        .copied()
        .collect::<BTreeMap<_, _>>();
    let mut sources = DOCUMENTS
        .iter()
        .map(|document_id| {
            let document = workload
                .corpus
                .iter()
                .find(|document| document.document_id == *document_id)
                .expect("selected workload document");
            let bytes = packaged
                .get(document.path.as_str())
                .copied()
                .expect("byte-exact packaged corpus source");
            (document.source_path.clone(), bytes)
        })
        .collect::<Vec<_>>();
    sources.sort_by(|left, right| left.0.cmp(&right.0));
    sources
}

fn production_generations(count: usize) -> Vec<Arc<CodeIndexPublishedGenerationV1>> {
    assert!(count > 0);
    let sources = source_files();
    let snapshot_files = sources
        .iter()
        .map(|(path, bytes)| SanitizedCodeFileV1 {
            file_occurrence_id: id(&format!("file.semantic.{}", document_id(path))),
            logical_path: path.clone(),
            language: Some(id::<LanguageId>("rust")),
            content_digest: content_digest(bytes),
            disposition: SnapshotFileDispositionV1::Present,
        })
        .collect::<Vec<_>>();
    let captured_files = snapshot_files
        .iter()
        .zip(&sources)
        .map(|(file, (_, bytes))| CodeIndexCapturedFileV1 {
            file_occurrence_id: file.file_occurrence_id.clone(),
            sanitized_bytes: Arc::from(*bytes),
            sensitivity_level: SensitivityLevelV1::Public,
        })
        .collect::<Vec<_>>();
    let joined_bytes = sources
        .iter()
        .flat_map(|(_, bytes)| bytes.iter().copied())
        .collect::<Vec<_>>();
    let config = CodeIndexProductionConfigV1 {
        project_id: id::<ProjectId>("project.semantic-real-model"),
        repository: id::<RepositoryId>("repository.tracedecay"),
        sanitizer_revision: id::<SanitizerRevision>("sanitizer.semantic-real-model.v1"),
        policy_revision: id::<PolicyRevisionId>("policy.semantic-real-model.v1"),
        chunker_revision: id::<ChunkerRevision>(DAEMON_CODE_INDEX_CHUNKER_REVISION),
        privacy_domain: id::<PrivacyDomainId>("privacy.semantic-real-model"),
        privacy_key_epoch: PRIVACY_KEY_EPOCH,
        max_snapshot_age_micros: None,
    };
    let mut owner = CodeIndexProductionOwnerV1::new(
        config.clone(),
        InMemoryPublicationStore::default(),
        ApplyingProjectionSink,
    )
    .expect("production code-index owner");
    (0..count)
        .map(|ordinal| {
            let timestamp =
                1_784_500_000_000_000_i64 + i64::try_from(ordinal).expect("generation ordinal");
            owner
                .build_and_publish(
                    CodeIndexBuildRequestV1 {
                        snapshot: SanitizedCodeSnapshotV1 {
                            repository: config.repository.clone(),
                            worktree: Some(id::<WorktreeId>("worktree.semantic-real-model")),
                            reference: Some(id::<RefId>("refs/heads/semantic-real-model")),
                            source_revision: Some(id::<CommitId>(SOURCE_COMMIT)),
                            sanitizer_revision: config.sanitizer_revision.clone(),
                            sanitization_receipts: vec![id::<SanitizationReceiptId>(
                                "receipt.semantic-real-model",
                            )],
                            content_identity: content_digest(&joined_bytes),
                            captured_at: UtcMicros(timestamp),
                            files: snapshot_files.clone(),
                        },
                        captured_files: captured_files.clone(),
                        changed_files: BTreeSet::new(),
                        invalidations: BTreeSet::new(),
                        ignored_source_admissions: Vec::new(),
                        repository_parse_identity: CodeIndexRepositoryParseIdentityV1 {
                            tree: Some(id::<TreeId>(SOURCE_TREE)),
                            dirty: RepositoryDirtyStateV1::Clean,
                        },
                        sealed_at: UtcMicros(timestamp),
                        target_projection_key: ProjectionKeyV1 {
                            kind: ProjectionKindV1::Lexical,
                            schema_revision: "lexical.semantic-real-model.v1".to_owned(),
                            profile_digest: digest('e'),
                        },
                    },
                    &ActiveControl,
                )
                .expect("production-extracted search-quality generation")
        })
        .collect()
}

fn production_generation() -> Arc<CodeIndexPublishedGenerationV1> {
    production_generations(1)
        .pop()
        .expect("one production generation")
}

fn document_id(path: &str) -> &str {
    path.rsplit('/')
        .next()
        .and_then(|name| name.strip_suffix(".rs"))
        .expect("Rust corpus path")
}

fn projection_request(
    code: &CodeIndexPublishedGenerationV1,
    projection: &AdmittedEmbeddingProjectionKeyV1,
) -> ProjectionBatchRequestV1 {
    let mut request = ProjectionBatchRequestV1 {
        request_digest: digest('0'),
        changes: code.projection().request().changes.clone(),
        previous_projection_key: None,
        target_projection_key: projection.projection_key().clone(),
        replay_reason: ProjectionReplayReasonV1::InitialProjection,
    };
    request.request_digest = expected_request_digest(&request).expect("projection request digest");
    request
}

fn published_vectors(
    code: &Arc<CodeIndexPublishedGenerationV1>,
    fixture: &std::path::Path,
) -> (
    SemanticEvaluationQueryFactoryV1,
    AdmittedEmbeddingProjectionKeyV1,
    Arc<tracedecay_vector_authority::PublishedVectorReadSnapshotV1>,
) {
    let lifecycle_root = tempfile::tempdir().expect("isolated lifecycle root");
    let lifecycle = open_local_semantic_evaluation_lifecycle(
        lifecycle_root.path(),
        fixture,
        resources(),
        1_784_500_000,
    )
    .expect("catalog-pinned local Jina lifecycle import");
    let artifact = LoadedSemanticArtifactV1::from_lifecycle(
        &lifecycle,
        code.manifest(),
        resources(),
        EmbeddingDocumentCompositionV1::SymbolContextHeader,
    )
    .expect("verified lifecycle artifact");
    let projection = artifact.projection().clone();
    let request = projection_request(code, &projection);
    let documents = Arc::new(EmbeddingDocumentComposerV1::new(
        EmbeddingSymbolContextIndexV1::from_generation_symbols(code.symbols()),
    ));
    let cache = SemanticEvaluationProjectionBatchStoreV1::new();
    let cache_request = cache.request_cache();
    let prepared = prepare_semantic_evaluation_projection(
        artifact,
        None,
        request,
        code.chunks().chunks(),
        documents,
        SemanticEvaluationProjectionResourcesV1 {
            memory_ceiling_bytes: resources().max_resident_bytes.expect("resident ceiling"),
        },
        &cache_request,
        SemanticEvaluationProjectionBatchCachePolicyV1::Bypass,
        Arc::new(ActiveProjection),
    )
    .expect("real Jina document projection");
    let query_factory = prepared.query_factory.clone();
    let expected_chunks = code
        .chunks()
        .chunks()
        .iter()
        .map(|chunk| chunk.id.clone())
        .collect::<Vec<_>>();
    let plan = VectorGenerationPlanV1::new(
        &projection,
        code.manifest().generation_id.clone(),
        prepared.prepared.receipt.source_manifest_digest.clone(),
        expected_chunks,
        None,
    )
    .expect("vector generation plan");
    let mut vectors = VectorGenerationAuthority::new();
    let build = vectors.begin_generation(plan).expect("vector build");
    vectors
        .commit_batch(&build, None, prepared.prepared)
        .expect("commit real vectors");
    let publication = vectors
        .publish_generation(&build)
        .expect("publish real vectors");
    let snapshot = vectors
        .generation_read_snapshot(&publication.generation_id)
        .expect("vector snapshot read")
        .expect("published vector snapshot");
    (query_factory, projection, snapshot)
}

fn search_index_key() -> SemanticSearchIndexKeyV1 {
    SemanticSearchIndexProfileV1::exact_flat_v1()
        .and_then(|profile| profile.index_key())
        .expect("exact-flat search profile")
}

fn query_digest(query: &str) -> QueryDigest {
    QueryDigest::new(
        id("privacy.semantic-real-model"),
        PRIVACY_KEY_EPOCH,
        QueryMac::new(format!(
            "hmac-sha256:{}",
            hex::encode(Sha256::digest(query.as_bytes()))
        ))
        .expect("query MAC shape"),
    )
}

fn retrieval_request<'a>(
    query: &'a tracedecay_domain::EphemeralSanitizedQueryViewV1,
    query_digest: QueryDigest,
    code: &CodeIndexPublishedGenerationV1,
    vectors: &tracedecay_vector_authority::PublishedVectorReadSnapshotV1,
    projection: &'a AdmittedEmbeddingProjectionKeyV1,
    search_index_key: &'a SemanticSearchIndexKeyV1,
    source_scope: SemanticSourceScopeV1,
) -> SemanticRetrievalRequestV1<'a> {
    let budget = RetrievalBudget {
        max_candidates_per_lane: u32::try_from(code.chunks().chunks().len())
            .expect("bounded corpus size"),
        max_fused_candidates: 64,
        max_hydrated_results: 16,
        max_hydration_bytes: 1024 * 1024,
        deadline_micros: None,
    };
    SemanticRetrievalRequestV1 {
        base: RetrievalRequest {
            principal: id::<PrincipalId>("principal.semantic-real-model"),
            scope: RetrievalScope {
                privacy_domain: id("privacy.semantic-real-model"),
                root: SingleRootScopeV1 {
                    repository: source_scope.repository_id.clone(),
                    worktree: Some(source_scope.worktree_id.clone()),
                    reference: source_scope.reference.clone(),
                },
            },
            temporal_mode: TemporalModeV1::Current,
            snapshot: RetrievalSnapshot {
                watermarks: VectorWatermark::default(),
                freshness_digest: id(&format!("sha256:{}", "f".repeat(64))),
                authorization_revision: id("authorization.semantic-real-model.v1"),
                captured_at: UtcMicros(1_784_500_000_000_000),
            },
            profile_id: id::<FusionProfileId>("profile.semantic-real-model.v1"),
            budget,
        },
        source_scope,
        query_digest,
        query_view: query,
        projection,
        search_index_key,
        capability_manifest_digest: code.capability().manifest_digest.clone(),
        vector_generation: vectors.generation_id().clone(),
        code_generation: code.manifest().generation_id.clone(),
        budget,
    }
}

fn fallback() -> Arc<QueryFallbackSubpayload> {
    Arc::new(
        QueryFallbackSubpayload::new(
            id("profile.semantic-real-model.v1"),
            Vec::new(),
            BTreeMap::from([
                (RetrieverKind::ExactLiteral, PublicRetrieverStatus::Complete),
                (RetrieverKind::Lexical, PublicRetrieverStatus::Complete),
                (RetrieverKind::Graph, PublicRetrieverStatus::Complete),
            ]),
            Vec::new(),
            None,
        )
        .expect("valid fallback"),
    )
}

fn ranked_matches(
    code: &CodeIndexPublishedGenerationV1,
    outcome: RetrieverOutcome<
        tracedecay_domain::RetrieverBatch<
            tracedecay_query::retrieval::semantic::CodeSemanticEvidenceV1,
        >,
    >,
) -> Vec<(CodeSearchChunkId, String, Option<String>, i64)> {
    let RetrieverOutcome::Complete(batch) = outcome else {
        panic!("real semantic query did not complete")
    };
    batch
        .candidates
        .iter()
        .map(|candidate| {
            let evidence = batch
                .evidence_by_occurrence
                .get(&candidate.source_occurrence_id)
                .expect("candidate semantic evidence");
            let chunk = code
                .chunks()
                .chunk(&evidence.chunk_id)
                .expect("semantic result must name a production-extracted chunk");
            let symbol = chunk
                .anchor
                .symbol_occurrence_id
                .as_ref()
                .and_then(|occurrence| {
                    code.symbols()
                        .symbols
                        .iter()
                        .find(|symbol| symbol.occurrence == *occurrence)
                })
                .map(|symbol| symbol.qualified_name.clone());
            (
                evidence.chunk_id.clone(),
                chunk.anchor.file_occurrence_id.as_str().to_owned(),
                symbol,
                evidence.distance.micros(),
            )
        })
        .collect()
}

fn top_match(
    code: &CodeIndexPublishedGenerationV1,
    outcome: RetrieverOutcome<
        tracedecay_domain::RetrieverBatch<
            tracedecay_query::retrieval::semantic::CodeSemanticEvidenceV1,
        >,
    >,
) -> (CodeSearchChunkId, i64) {
    let ranked = ranked_matches(code, outcome);
    let (chunk_id, _, _, distance) = ranked.first().expect("semantic candidate");
    (chunk_id.clone(), *distance)
}

fn deterministic_projection() -> AdmittedEmbeddingProjectionKeyV1 {
    EmbeddingProjectionKeyV1 {
        model_artifact_digest: digest('1'),
        tokenizer_digest: digest('2'),
        config_digest: digest('3'),
        query_instruction_digest: None,
        document_instruction_digest: None,
        document_composition: EmbeddingDocumentCompositionV1::SanitizedText,
        pooling: EmbeddingPoolingV1::Mean,
        truncation_side: EmbeddingTruncationSideV1::Right,
        truncation_length: 8192,
        inference_batch_size: 64,
        inference_batch_bytes: 8 * 1024 * 1024,
        runtime_backend: "test-deterministic".to_owned(),
        runtime_build_revision: "test-deterministic.v1".to_owned(),
        device_class: EmbeddingDeviceClassV1::Cpu,
        execution_provider: EmbeddingExecutionProviderV1::Cpu,
        dimensions: 2,
        metric: EmbeddingMetricV1::Cosine,
        normalization: EmbeddingNormalizationV1::L2,
        precision: EmbeddingPrecisionV1::Fp32,
        chunk_schema_revision: "chunk.semantic-real-model.v1".to_owned(),
        chunker_revision: id::<ChunkerRevision>(DAEMON_CODE_INDEX_CHUNKER_REVISION),
        privacy_domain: id::<PrivacyDomainId>("privacy.semantic-real-model"),
        privacy_key_epoch: PRIVACY_KEY_EPOCH,
    }
    .admit()
    .expect("deterministic admitted projection")
}

struct DeterministicEncoder;

impl CanonicalChunkTokenLengthsV1 for DeterministicEncoder {
    fn document_token_lengths(
        &mut self,
        _key: &EmbeddingProjectionKeyV1,
        chunks: &[&tracedecay_domain::CodeSearchChunkV1],
    ) -> Result<Vec<usize>, String> {
        Ok(vec![1; chunks.len()])
    }
}

impl CanonicalChunkVectorEncoderV1 for DeterministicEncoder {
    fn encode(
        &mut self,
        key: &EmbeddingProjectionKeyV1,
        _chunk: &tracedecay_domain::CodeSearchChunkV1,
    ) -> Result<Vec<f32>, String> {
        let dimensions = usize::try_from(key.dimensions)
            .map_err(|_| "projection dimensions do not fit this target".to_owned())?;
        let mut vector = vec![0.0; dimensions];
        let first = vector
            .first_mut()
            .ok_or_else(|| "projection has no dimensions".to_owned())?;
        *first = 1.0;
        Ok(vector)
    }
}

#[tokio::test]
async fn full_semantic_rebuild_of_incremental_code_generation_is_readable() {
    let mut generations = production_generations(2);
    let incremental = generations.pop().expect("incremental generation");
    let initial = generations.pop().expect("initial generation");
    assert_eq!(
        incremental.manifest().parent_generation.as_ref(),
        Some(&initial.manifest().generation_id)
    );
    let incremental_changes = &incremental.projection().request().changes;
    assert_eq!(
        incremental_changes.from_generation.as_ref(),
        Some(&initial.manifest().generation_id)
    );
    assert!(incremental_changes.reused_count > 0);

    let projection = deterministic_projection();
    let full_request = runtime_projection_request(&incremental, &projection, None)
        .expect("full semantic rebuild request");
    assert_eq!(
        full_request.replay_reason,
        ProjectionReplayReasonV1::FullRebuildIncompatible
    );
    assert!(full_request.changes.from_generation.is_none());
    assert_eq!(
        full_request.changes.added_or_changed.len(),
        incremental.chunks().chunks().len()
    );
    assert_ne!(
        full_request.changes.manifest_digest, incremental_changes.manifest_digest,
        "a full vector replay has a different delta seal from its incremental code generation"
    );

    let prepared = prepare_vector_generation_async(
        projection.clone(),
        full_request,
        incremental.chunks().chunks().to_vec(),
        DeterministicEncoder,
    )
    .await
    .expect("deterministic full projection");
    let expected_chunks = incremental
        .chunks()
        .chunks()
        .iter()
        .map(|chunk| chunk.id.clone())
        .collect::<Vec<_>>();
    let plan = VectorGenerationPlanV1::new(
        &projection,
        incremental.manifest().generation_id.clone(),
        prepared.receipt.source_manifest_digest.clone(),
        expected_chunks,
        None,
    )
    .expect("full vector plan");
    let mut authority = VectorGenerationAuthority::new();
    let build = authority.begin_generation(plan).expect("vector build");
    authority
        .commit_batch(&build, None, prepared)
        .expect("commit deterministic vectors");
    let publication = authority
        .publish_generation(&build)
        .expect("publish deterministic vectors");
    let snapshot = authority
        .generation_read_snapshot(&publication.generation_id)
        .expect("vector snapshot read")
        .expect("published vector snapshot");

    PublishedVectorReadPortV1::new(snapshot, incremental, search_index_key())
        .expect("complete vector membership binds the incremental code generation");
}

#[tokio::test]
async fn restore_rejects_vectors_for_a_stale_document_composition() {
    let code = production_generation();
    let lifecycle_root = tempfile::tempdir().expect("isolated lifecycle root");
    let mut catalog = production_fastembed_catalog();
    let model = catalog.models.first_mut().expect("default semantic model");
    let model_id = model.model_id.clone();
    let mut members = BTreeMap::new();
    for (role, member) in &mut model.members {
        let bytes = format!("semantic-restore-fixture:{role}").into_bytes();
        member.length = u64::try_from(bytes.len()).expect("fixture member length");
        member.sha256 = hex::encode(Sha256::digest(&bytes));
        members.insert(member.upstream_path.clone(), bytes);
    }
    let lifecycle = Arc::new(
        SemanticModelLifecycleOwnerV1::open(
            lifecycle_root.path(),
            catalog,
            Arc::new(FixtureModelSource { members }),
        )
        .expect("fixture lifecycle owner"),
    );
    lifecycle
        .select_model(Some(&model_id), false)
        .expect("select fixture model");
    lifecycle
        .acquire_blocking_for_tests()
        .expect("install fixture model");

    let persisted_projection = LoadedSemanticArtifactV1::lifecycle_projection(
        &lifecycle,
        code.manifest(),
        resources(),
        EmbeddingDocumentCompositionV1::SanitizedText,
    )
    .expect("persisted sanitized projection");
    let request = runtime_projection_request(&code, &persisted_projection, None)
        .expect("full persisted projection request");
    let prepared = prepare_vector_generation_async(
        persisted_projection.clone(),
        request,
        code.chunks().chunks().to_vec(),
        DeterministicEncoder,
    )
    .await
    .expect("deterministic persisted projection");
    let plan = VectorGenerationPlanV1::new(
        &persisted_projection,
        code.manifest().generation_id.clone(),
        prepared.receipt.source_manifest_digest.clone(),
        code.chunks()
            .chunks()
            .iter()
            .map(|chunk| chunk.id.clone())
            .collect(),
        None,
    )
    .expect("persisted vector plan");

    let database_root = tempfile::tempdir().expect("isolated vector database root");
    let database_path = database_root.path().join("semantic-restore.db");
    crate::register_test_schema_installer();
    let database_authority =
        DatabaseAuthority::acquire_test(&database_path, "semantic restore regression")
            .expect("vector database authority");
    let (database, _) = Database::publish_registered_test_runtime(
        &database_path,
        &database_authority,
        TestDatabaseRuntimeMode::Initialize,
        TestDatabaseRuntimeScope::Project {
            project_id: id("project.semantic-restore"),
        },
    )
    .await
    .expect("project vector database");
    let vector_store: Arc<dyn DurableVectorAuthorityStoreV1> = Arc::new(
        database
            .open_vector_authority("semantic-restore")
            .expect("durable vector authority"),
    );
    let build = vector_store
        .begin_generation(plan)
        .expect("begin persisted vector generation");
    let initial_checkpoint = vector_store
        .snapshot()
        .expect("fresh vector snapshot")
        .staged_checkpoint(&build)
        .cloned();
    let (expected_checkpoint, resume_outcome) =
        resume_projection_checkpoint(initial_checkpoint).expect("fresh projection resume state");
    assert_eq!(
        resume_outcome,
        SemanticProjectionResumeOutcomeV1::ReplayFromStart
    );
    assert!(
        expected_checkpoint.is_none(),
        "a zero-batch stage must commit without a prior checkpoint"
    );
    let committed_checkpoint = vector_store
        .commit_batch(&build, expected_checkpoint.as_ref(), prepared)
        .expect("commit persisted vector generation");
    let (expected_checkpoint, resume_outcome) =
        resume_projection_checkpoint(Some(committed_checkpoint.clone()))
            .expect("committed projection resume state");
    assert_eq!(
        resume_outcome,
        SemanticProjectionResumeOutcomeV1::CompletedBatches(1)
    );
    assert_eq!(expected_checkpoint.as_ref(), Some(&committed_checkpoint));
    let publication = vector_store
        .publish_generation_if_current(&build, None)
        .expect("publish persisted vector generation");

    let handle = DaemonSemanticRuntimeHandleV1::new(
        1,
        code.chunks().chunks().len(),
        resources()
            .max_resident_bytes
            .expect("bounded resident ceiling"),
    )
    .expect("semantic runtime handle");
    let query_snapshot = Arc::new(Mutex::new(None));
    let restored = restore_current(
        handle.clone(),
        lifecycle,
        Arc::clone(&vector_store),
        Arc::clone(&query_snapshot),
        code,
        resources(),
        EmbeddingDocumentCompositionV1::SymbolContextHeader,
    )
    .expect("stale projection is a rebuild signal");

    assert!(!restored);
    assert!(handle.current().is_none());
    assert!(
        query_snapshot
            .lock()
            .expect("query snapshot lock")
            .is_none()
    );
    assert_eq!(
        vector_store
            .snapshot()
            .expect("persisted vector snapshot")
            .active_generation(),
        Some(&publication.generation_id),
        "restore rejection must leave the saved generation available for replacement"
    );
}

#[test]
#[ignore = "requires the pinned 644 MB Jina package in TRACEDECAY_DISTRIBUTION_FASTEMBED_FIXTURE"]
fn cpu_jina_ranks_repository_concepts_and_reports_unrepresented_nearest_neighbor() {
    let fixture = std::env::var_os("TRACEDECAY_DISTRIBUTION_FASTEMBED_FIXTURE")
        .map(std::path::PathBuf::from)
        .filter(|path| path.is_dir())
        .expect("TRACEDECAY_DISTRIBUTION_FASTEMBED_FIXTURE must name the pinned Jina package");
    let code = production_generation();
    assert!(
        code.chunks().chunks().len() > 6,
        "the oracle must exercise production chunk extraction, not one row per document"
    );
    let (query_factory, projection, snapshot) = published_vectors(&code, &fixture);
    let search_index_key = search_index_key();
    let vectors = PublishedVectorReadPortV1::new(
        Arc::clone(&snapshot),
        Arc::clone(&code),
        search_index_key.clone(),
    )
    .expect("production vector read port");
    let control = ActiveControl;
    let calibration = SemanticCalibrationProfileV1::jina_exact_flat_v1(
        projection.projection_key().clone(),
        snapshot.generation_id().clone(),
        code.capability().manifest_digest.clone(),
    )
    .expect("versioned Jina distance policy");
    let complete = CompleteSemanticGenerationV1::new(
        projection.projection_key().clone(),
        search_index_key.clone(),
        snapshot.generation_id().clone(),
        code.manifest().generation_id.clone(),
        code.capability().manifest_digest.clone(),
    )
    .expect("complete semantic generation");

    let positives = [
        (
            "train-007",
            "how are canonical digests computed for manifests",
            "canonical",
        ),
        (
            "train-016",
            "what proves an offline replica is still permitted to answer",
            "coverage",
        ),
        (
            "validation-006",
            "repository dirty state and remote identity evidence",
            "repository",
        ),
    ];
    let workload = load_workload().expect("pinned query labels");
    let mut positive_results = Vec::new();
    for (query_id, raw_query, expected_document) in positives {
        let labelled = workload
            .queries
            .iter()
            .find(|query| query.query_id == query_id)
            .expect("pinned conceptual query");
        assert_eq!(labelled.query, raw_query);
        assert_eq!(
            labelled
                .label
                .as_ref()
                .and_then(|label| label.get("document_id"))
                .and_then(serde_json::Value::as_str),
            Some(expected_document)
        );
        let query = tracedecay_domain::EphemeralSanitizedQueryViewV1::sanitize(
            raw_query,
            id::<SanitizerRevision>("sanitizer.semantic-real-model.v1"),
            id::<QueryNormalizationRevision>("normalizer.semantic-real-model.v1"),
        )
        .expect("sanitized conceptual query");
        let digest = query_digest(raw_query);
        let request = retrieval_request(
            &query,
            digest,
            &code,
            &snapshot,
            &projection,
            &search_index_key,
            vectors.source_scope().clone(),
        );
        let embedder = query_factory.create(&control, None);
        let ranked = ranked_matches(
            &code,
            SemanticCodeRetriever::new(&embedder, &vectors, &control)
                .retrieve_semantic(&request)
                .expect("real semantic retrieval"),
        );
        let (_, actual_document, _, distance) = ranked.first().expect("semantic candidate");
        let expected_document_id = format!("file.semantic.{expected_document}");
        let expected_rank = ranked
            .iter()
            .position(|(_, document, _, _)| document == &expected_document_id)
            .expect("labeled document remains in the complete semantic result");
        let mut ranking_evidence = ranked.iter().take(8).cloned().collect::<Vec<_>>();
        if expected_rank >= ranking_evidence.len() {
            ranking_evidence.push(ranked[expected_rank].clone());
        }
        if query_id == "train-007" {
            let strict_fallback = fallback();
            let strict_embedder = query_factory.create(&control, None);
            let strict_lane = SemanticCodeRetriever::new(&strict_embedder, &vectors, &control);
            let strict = CalibratedSemanticQueryService::new(&strict_lane)
                .execute(
                    SemanticLaneReadinessV1::Ready {
                        request: &request,
                        generation: &complete,
                        calibration: Some(&calibration),
                    },
                    SemanticQueryDecisionV1::EXECUTE_STRICT,
                    Arc::clone(&strict_fallback),
                )
                .expect("represented code concept is available in strict semantic mode");
            let SemanticQueryServiceOutcomeV1::Augmented {
                semantic_lane,
                fallback: returned_fallback,
                ..
            } = strict
            else {
                panic!("represented code concept must reach canonical semantic composition")
            };
            assert_eq!(semantic_lane.lane, RetrieverKind::Semantic);
            assert!(matches!(
                semantic_lane.outcome,
                RetrieverOutcome::Complete(_)
            ));
            assert!(Arc::ptr_eq(&returned_fallback, &strict_fallback));
        }
        positive_results.push((
            query_id,
            *distance,
            expected_document,
            actual_document.clone(),
            expected_rank + 1,
            ranking_evidence,
        ));
    }

    let negative_text = "where does the websocket reconnect loop schedule heartbeat ping frames";
    let negative_query = tracedecay_domain::EphemeralSanitizedQueryViewV1::sanitize(
        negative_text,
        id::<SanitizerRevision>("sanitizer.semantic-real-model.v1"),
        id::<QueryNormalizationRevision>("normalizer.semantic-real-model.v1"),
    )
    .expect("sanitized negative query");
    let negative_digest = query_digest(negative_text);
    let negative_request = retrieval_request(
        &negative_query,
        negative_digest,
        &code,
        &snapshot,
        &projection,
        &search_index_key,
        vectors.source_scope().clone(),
    );
    let negative_embedder = query_factory.create(&control, None);
    let (_, negative_distance) = top_match(
        &code,
        SemanticCodeRetriever::new(&negative_embedder, &vectors, &control)
            .retrieve_semantic(&negative_request)
            .expect("real negative semantic retrieval"),
    );

    let service_embedder = query_factory.create(&control, None);
    let lane = SemanticCodeRetriever::new(&service_embedder, &vectors, &control);
    let decision = CalibratedSemanticQueryService::new(&lane)
        .execute(
            SemanticLaneReadinessV1::Ready {
                request: &negative_request,
                generation: &complete,
                calibration: Some(&calibration),
            },
            SemanticQueryDecisionV1::EXECUTE_WITH_FALLBACK,
            fallback(),
        )
        .expect("negative query returns typed fallback");
    let positive_distances = positive_results
        .iter()
        .map(|(query_id, distance, ..)| (*query_id, *distance))
        .collect::<Vec<_>>();
    for (query_id, distance, expected_document, actual_document, expected_rank, ranked) in
        &positive_results
    {
        assert_eq!(
            actual_document,
            &format!("file.semantic.{expected_document}"),
            "{query_id} distance={distance} ranked the wrong document; labeled document rank={expected_rank}; ranking={ranked:?}; positives={positive_distances:?}, negative={negative_distance}"
        );
    }
    assert!(
        positive_distances
            .iter()
            .all(|(_, distance)| negative_distance > *distance),
        "the unrepresented concept must remain farther than every labeled concept; positives={positive_distances:?}, negative={negative_distance}"
    );
    assert!(
        matches!(decision, SemanticQueryServiceOutcomeV1::Augmented { .. }),
        "the explicitly unfiltered nearest-neighbor baseline unexpectedly abstained; positives={positive_distances:?}, negative={negative_distance}, policy_distance={}, policy_margin={}",
        calibration.maximum_distance_micros,
        calibration.minimum_margin_micros,
    );
}
