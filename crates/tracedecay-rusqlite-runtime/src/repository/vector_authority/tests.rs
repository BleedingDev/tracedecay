use rusqlite::Savepoint;
use tempfile::TempDir;
use tracedecay_domain::{
    AdmittedEmbeddingProjectionKeyV1, BrainId, ChangedCodeChunkSetV1, ChangedCodeChunkV1,
    ChunkerRevision, CodeChunkProjectionReceiptV1, CodeGenerationId, CodeSearchChunkId,
    ContentDigest, EmbeddingDeviceClassV1, EmbeddingDocumentCompositionV1,
    EmbeddingExecutionProviderV1, EmbeddingMetricV1, EmbeddingNormalizationV1, EmbeddingPoolingV1,
    EmbeddingPrecisionV1, EmbeddingProjectionKeyV1, EmbeddingTruncationSideV1, LocatorDigest,
    ManifestDigest, PrivacyDomainId, ProjectId, ProjectionBatchReceiptV1, ProjectionBatchRequestV1,
    ProjectionOperationV1, ProjectionOutcomeV1, ProjectionReplayReasonV1, UserProfileId,
    VectorGenerationIdV1,
};
use tracedecay_store::runtime::{
    PreparedVectorGenerationV1, VectorAuthorityStoreErrorV1, VectorGenerationPlanV1,
    VectorGenerationPublicationV1, prepare_vector,
};
use tracedecay_store::{
    AdmissionConfigV1, RepositoryWritePayloadV1, RuntimeReadOutcomeV1, RuntimeReadRequestV1,
    StorageRuntimeErrorV1, StoreAuthorityEpochV1, StoreIncarnationV1, StoreRuntimeBindingV1,
    VerifiedStoreLocatorV1,
};

use crate::exact_sql::ExactSqlHandle;
use crate::reader::{ExistingReaderLocator, ReaderPool, ReaderQueryExecutor};
use crate::{ExistingWriterLocator, PersistentWriter, StorageOperationExecutor};

use super::{VECTOR_AUTHORITY_SCHEMA_V1, VectorAuthoritySqliteStorage};

struct NoWrites;

impl StorageOperationExecutor for NoWrites {
    fn execute(
        &mut self,
        _savepoint: &Savepoint<'_>,
        _payload: &RepositoryWritePayloadV1,
    ) -> rusqlite::Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
struct NoReads;

impl ReaderQueryExecutor for NoReads {
    fn execute_read(
        &mut self,
        _snapshot: &rusqlite::Transaction<'_>,
        _request: &RuntimeReadRequestV1,
    ) -> Result<RuntimeReadOutcomeV1, StorageRuntimeErrorV1> {
        unreachable!("exact SQL queries bypass the product read executor")
    }
}

struct Fixture {
    _directory: TempDir,
    _writer: PersistentWriter,
    _readers: ReaderPool<NoReads>,
    storage: VectorAuthoritySqliteStorage,
}

impl Fixture {
    fn new() -> Self {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("vector-authority.sqlite3");
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(VECTOR_AUTHORITY_SCHEMA_V1)
            .unwrap();
        drop(connection);
        let path = path.canonicalize().unwrap();
        let binding = StoreRuntimeBindingV1::new(
            tracedecay_store::StoreShardIdV1::project(
                BrainId::new("brain.vector-authority").unwrap(),
                UserProfileId::new("profile.vector-authority").unwrap(),
                ProjectId::new("project.vector-authority").unwrap(),
            ),
            StoreIncarnationV1::new(3).unwrap(),
            StoreAuthorityEpochV1::new(11).unwrap(),
        );
        let locator = VerifiedStoreLocatorV1::new(
            binding.shard_id.clone(),
            StoreIncarnationV1::new(3).unwrap(),
            LocatorDigest::new(format!("sha256:{}", "a".repeat(64))).unwrap(),
        );
        let writer = PersistentWriter::start(
            ExistingWriterLocator::new(binding.clone(), locator.clone(), path.clone()).unwrap(),
            AdmissionConfigV1::default(),
            NoWrites,
        )
        .unwrap();
        let readers = ReaderPool::start(
            ExistingReaderLocator::new(binding, locator, path).unwrap(),
            AdmissionConfigV1::default().readers,
            NoReads,
        )
        .unwrap();
        let handle = ExactSqlHandle::attach(&writer, &readers).unwrap();
        let storage = VectorAuthoritySqliteStorage::from_authorized_handle(handle).unwrap();
        Self {
            _directory: directory,
            _writer: writer,
            _readers: readers,
            storage,
        }
    }
}

fn digest(seed: u8) -> ManifestDigest {
    ManifestDigest::new(format!("sha256:{}", format!("{seed:02x}").repeat(32))).unwrap()
}

fn content(seed: u8) -> ContentDigest {
    ContentDigest::new(format!("sha256:{}", format!("{seed:02x}").repeat(32))).unwrap()
}

fn generation(value: &str) -> CodeGenerationId {
    CodeGenerationId::new(value).unwrap()
}

fn chunk(value: &str) -> CodeSearchChunkId {
    CodeSearchChunkId::new(value).unwrap()
}

fn embedding_key(seed: u8) -> AdmittedEmbeddingProjectionKeyV1 {
    EmbeddingProjectionKeyV1 {
        model_artifact_digest: digest(seed),
        tokenizer_digest: digest(seed.wrapping_add(1)),
        config_digest: digest(seed.wrapping_add(2)),
        query_instruction_digest: None,
        document_instruction_digest: None,
        document_composition: EmbeddingDocumentCompositionV1::SanitizedText,
        pooling: EmbeddingPoolingV1::Mean,
        truncation_side: EmbeddingTruncationSideV1::Right,
        truncation_length: 128,
        inference_batch_size: 8,
        inference_batch_bytes: 16_384,
        runtime_backend: "test-runtime".to_owned(),
        runtime_build_revision: "test-runtime-v1".to_owned(),
        device_class: EmbeddingDeviceClassV1::Cpu,
        execution_provider: EmbeddingExecutionProviderV1::Cpu,
        dimensions: 2,
        metric: EmbeddingMetricV1::Cosine,
        normalization: EmbeddingNormalizationV1::L2,
        precision: EmbeddingPrecisionV1::Fp32,
        chunk_schema_revision: "chunk-v1".to_owned(),
        chunker_revision: ChunkerRevision::new("chunker-v1").unwrap(),
        privacy_domain: PrivacyDomainId::new("project.vector-authority").unwrap(),
        privacy_key_epoch: 1,
    }
    .admit()
    .unwrap()
}

#[derive(Clone)]
struct PreparedCase {
    plan: VectorGenerationPlanV1,
    prepared: PreparedVectorGenerationV1,
    chunk_id: CodeSearchChunkId,
}

fn prepared_case(seed: u8, source: &str, chunk_name: &str, vector: Vec<f32>) -> PreparedCase {
    let admitted = embedding_key(seed);
    let chunk_id = chunk(chunk_name);
    let chunk_digest = content(seed.wrapping_add(20));
    let plan = VectorGenerationPlanV1::new(
        &admitted,
        generation(source),
        digest(seed.wrapping_add(40)),
        vec![chunk_id.clone()],
        None,
    )
    .unwrap();
    let mut changes = ChangedCodeChunkSetV1 {
        from_generation: None,
        to_generation: generation(source),
        manifest_digest: ManifestDigest::zero().unwrap(),
        added_or_changed: vec![ChangedCodeChunkV1 {
            chunk_id: chunk_id.clone(),
            prior_digest: None,
            current_digest: Some(chunk_digest.clone()),
        }],
        deleted: Vec::new(),
        reused_count: 0,
        reused_digest: ChangedCodeChunkSetV1::seal_reused_partition(&[]).unwrap().1,
    };
    changes.manifest_digest = changes.compute_digest().unwrap();
    let mut request = ProjectionBatchRequestV1 {
        request_digest: ManifestDigest::zero().unwrap(),
        changes,
        previous_projection_key: None,
        target_projection_key: admitted.projection_key().clone(),
        replay_reason: ProjectionReplayReasonV1::SourceEdit,
    };
    request.request_digest = tracedecay_domain::canonical_sha256(&(
        "tracedecay.projection-batch-request.v1",
        &request.changes,
        &request.previous_projection_key,
        &request.target_projection_key,
        request.replay_reason,
    ))
    .unwrap();
    let projected = prepare_vector(
        &admitted,
        generation(source),
        request.changes.manifest_digest.clone(),
        chunk_id.clone(),
        chunk_digest,
        vector,
    )
    .unwrap();
    let row_receipt = CodeChunkProjectionReceiptV1 {
        projection_key: admitted.projection_key().clone(),
        request_digest: request.request_digest.clone(),
        prior_generation: None,
        source_generation: generation(source),
        source_manifest_digest: request.changes.manifest_digest.clone(),
        chunk_id: chunk_id.clone(),
        prior_chunk_digest: None,
        current_chunk_digest: Some(projected.chunk_digest.clone()),
        operation: ProjectionOperationV1::Added,
        outcome: ProjectionOutcomeV1::Applied,
        output_digest: Some(projected.output_digest.clone()),
    };
    let mut receipt = ProjectionBatchReceiptV1 {
        target_projection_key: admitted.projection_key().clone(),
        request_digest: request.request_digest.clone(),
        source_generation: generation(source),
        source_manifest_digest: request.changes.manifest_digest.clone(),
        receipts: vec![row_receipt],
        reused_count: request.changes.reused_count,
        publication_digest: ManifestDigest::zero().unwrap(),
    };
    receipt.publication_digest =
        tracedecay_domain::projection_batch_publication_digest(&receipt).unwrap();
    PreparedCase {
        plan,
        prepared: PreparedVectorGenerationV1 {
            embedding_key: admitted,
            request,
            receipt,
            vectors: vec![projected],
            tombstones: Vec::new(),
        },
        chunk_id,
    }
}

fn publish_case(
    handle: &super::ProjectVectorAuthorityHandleV1,
    case: PreparedCase,
    expected_active: Option<&VectorGenerationIdV1>,
) -> VectorGenerationPublicationV1 {
    let build_id = handle.begin_generation(case.plan).unwrap();
    handle.commit_batch(&build_id, None, case.prepared).unwrap();
    handle
        .publish_generation_if_current(&build_id, expected_active)
        .unwrap()
}

#[test]
fn publication_query_and_project_singleton_rollback_survive_reopen() {
    let fixture = Fixture::new();
    let handle = fixture.storage.open("repo/worktree-a").unwrap();
    let first = prepared_case(1, "code-1", "src/first.rs#0", vec![1.0, 0.0]);
    let first_chunk = first.chunk_id.clone();
    let first_publication = publish_case(&handle, first, None);

    let reopened = fixture.storage.open("repo/worktree-a").unwrap();
    let first_snapshot = reopened.snapshot().unwrap();
    let first_hits = first_snapshot.search_active(vec![1.0, 0.0], 5).unwrap();
    assert_eq!(first_hits.len(), 1);
    assert_eq!(first_hits[0].chunk_id, first_chunk);

    let second = prepared_case(7, "code-2", "src/second.rs#0", vec![0.0, 1.0]);
    let second_publication =
        publish_case(&reopened, second, Some(&first_publication.generation_id));
    assert_ne!(
        first_publication.generation_id,
        second_publication.generation_id
    );
    reopened
        .rollback_generation_if_current(Some(&second_publication.generation_id))
        .unwrap();

    let after_rollback = fixture
        .storage
        .open("repo/worktree-a")
        .unwrap()
        .snapshot()
        .unwrap();
    assert_eq!(
        after_rollback.active_generation(),
        Some(&first_publication.generation_id)
    );
    let hits = after_rollback.search_active(vec![1.0, 0.0], 5).unwrap();
    assert_eq!(hits[0].chunk_id, first_chunk);
}

#[test]
fn stale_batch_cas_never_advances_memory_and_durable_checkpoint_reopens() {
    let fixture = Fixture::new();
    let writer = fixture.storage.open("repo/worktree-a").unwrap();
    let case = prepared_case(3, "code-cas", "src/lib.rs#0", vec![1.0, 0.0]);
    let build_id = writer.begin_generation(case.plan).unwrap();
    let stale = fixture.storage.open("repo/worktree-a").unwrap();

    let checkpoint = writer
        .commit_batch(&build_id, None, case.prepared.clone())
        .unwrap();
    let committed_revision = writer.revision().unwrap();
    let error = stale
        .commit_batch(&build_id, None, case.prepared)
        .unwrap_err();
    assert!(matches!(
        error,
        VectorAuthorityStoreErrorV1::Conflict { .. }
    ));
    assert_eq!(
        stale
            .snapshot()
            .unwrap()
            .staged_checkpoint(&build_id)
            .unwrap()
            .completed_batches,
        0
    );

    let reopened = fixture.storage.open("repo/worktree-a").unwrap();
    assert_eq!(
        reopened.revision().unwrap(),
        committed_revision,
        "failed CAS must not advance the durable head revision"
    );
    let reopened_snapshot = reopened.snapshot().unwrap();
    assert_eq!(
        reopened_snapshot.staged_checkpoint(&build_id),
        Some(&checkpoint)
    );
}

#[test]
fn new_build_atomically_replaces_abandoned_stage_and_batches() {
    let fixture = Fixture::new();
    let handle = fixture.storage.open("repo/worktree-a").unwrap();
    let abandoned = prepared_case(5, "abandoned", "src/abandoned.rs#0", vec![1.0, 0.0]);
    let abandoned_build = handle.begin_generation(abandoned.plan).unwrap();
    handle
        .commit_batch(&abandoned_build, None, abandoned.prepared)
        .unwrap();

    let replacement = prepared_case(6, "replacement", "src/replacement.rs#0", vec![0.0, 1.0]);
    let replacement_build = handle.begin_generation(replacement.plan).unwrap();
    let snapshot = handle.snapshot().unwrap();
    assert!(snapshot.staged_checkpoint(&abandoned_build).is_none());
    assert_eq!(
        snapshot
            .staged_checkpoint(&replacement_build)
            .unwrap()
            .completed_batches,
        0
    );
    drop(snapshot);

    let reopened = fixture.storage.open("repo/worktree-a").unwrap();
    let reopened_snapshot = reopened.snapshot().unwrap();
    assert!(
        reopened_snapshot
            .staged_checkpoint(&abandoned_build)
            .is_none(),
        "reopen must not replay the replaced stage or its committed batches"
    );
    assert!(
        reopened_snapshot
            .staged_checkpoint(&replacement_build)
            .is_some()
    );
}

#[test]
fn linked_worktree_namespaces_keep_independent_heads_in_one_database() {
    let fixture = Fixture::new();
    let worktree_a = fixture.storage.open("repo/worktree-a").unwrap();
    let worktree_b = fixture.storage.open("repo/worktree-b").unwrap();

    // The same deterministic build may exist in two linked worktrees. Its
    // stage and batch rows are namespace-local even though the completed
    // immutable generation and vector blob are safely shared.
    let shared = prepared_case(11, "shared-1", "src/shared.rs#0", vec![1.0, 0.0]);
    let shared_build = worktree_a.begin_generation(shared.plan.clone()).unwrap();
    assert!(
        worktree_b
            .snapshot()
            .unwrap()
            .staged_checkpoint(&shared_build)
            .is_none(),
        "worktree B must not see worktree A's staged build"
    );
    worktree_a
        .commit_batch(&shared_build, None, shared.prepared.clone())
        .unwrap();
    let a_first = worktree_a
        .publish_generation_if_current(&shared_build, None)
        .unwrap();
    let b_first = publish_case(&worktree_b, shared, None);
    assert_eq!(a_first.generation_id, b_first.generation_id);
    let a_second = publish_case(
        &worktree_a,
        prepared_case(12, "a-2", "src/a2.rs#0", vec![0.5, 0.5]),
        Some(&a_first.generation_id),
    );
    assert_eq!(
        worktree_b.snapshot().unwrap().active_generation(),
        Some(&b_first.generation_id),
        "worktree A publication must not move worktree B's head"
    );
    worktree_a
        .rollback_generation_if_current(Some(&a_second.generation_id))
        .unwrap();

    let reopened_a = fixture.storage.open("repo/worktree-a").unwrap();
    let reopened_b = fixture.storage.open("repo/worktree-b").unwrap();
    assert_eq!(
        reopened_a.snapshot().unwrap().active_generation(),
        Some(&a_first.generation_id)
    );
    assert_eq!(
        reopened_b.snapshot().unwrap().active_generation(),
        Some(&b_first.generation_id)
    );
    assert_eq!(
        reopened_a
            .snapshot()
            .unwrap()
            .search_active(vec![1.0, 0.0], 1)
            .unwrap()[0]
            .chunk_id,
        chunk("src/shared.rs#0")
    );
    assert_eq!(
        reopened_b
            .snapshot()
            .unwrap()
            .search_active(vec![1.0, 0.0], 1)
            .unwrap()[0]
            .chunk_id,
        chunk("src/shared.rs#0")
    );
}

#[test]
fn install_failure_compensation_and_stale_restore_are_durable() {
    let fixture = Fixture::new();
    let handle = fixture.storage.open("repo/worktree-a").unwrap();
    let first = publish_case(
        &handle,
        prepared_case(31, "install-a", "src/a.rs#0", vec![1.0, 0.0]),
        None,
    );
    let second = publish_case(
        &handle,
        prepared_case(32, "install-b", "src/b.rs#0", vec![0.0, 1.0]),
        Some(&first.generation_id),
    );
    handle
        .restore_active_generation_if_current(&second.generation_id, Some(&first.generation_id))
        .unwrap();

    let reopened = fixture.storage.open("repo/worktree-a").unwrap();
    let restored = reopened.snapshot().unwrap();
    assert_eq!(restored.active_generation(), Some(&first.generation_id));
    assert_eq!(restored.rollback_generation(), Some(&second.generation_id));
    assert_eq!(
        restored.search_active(vec![1.0, 0.0], 1).unwrap()[0].chunk_id,
        chunk("src/a.rs#0")
    );
    drop(restored);

    let third = publish_case(
        &reopened,
        prepared_case(33, "install-c", "src/c.rs#0", vec![0.7, 0.3]),
        Some(&first.generation_id),
    );
    assert!(matches!(
        reopened.restore_active_generation_if_current(
            &second.generation_id,
            Some(&first.generation_id)
        ),
        Err(VectorAuthorityStoreErrorV1::Authority(
            tracedecay_store::runtime::VectorAuthorityError::ActivePointerMismatch { .. }
        ))
    ));
    assert_eq!(
        fixture
            .storage
            .open("repo/worktree-a")
            .unwrap()
            .snapshot()
            .unwrap()
            .active_generation(),
        Some(&third.generation_id)
    );
}
