use tracedecay_vector_authority::*;

use serde_json::Value;
use sha2::{Digest as Sha2Digest, Sha256};

fn digest(seed: u8) -> ManifestDigest {
    ManifestDigest::new(format!("sha256:{}", format!("{seed:02x}").repeat(32))).unwrap()
}

fn content(seed: u8) -> ContentDigest {
    ContentDigest::new(format!("sha256:{}", format!("{seed:02x}").repeat(32))).unwrap()
}

fn snapshot_checksum(payload: &[u8]) -> ManifestDigest {
    let mut hasher = Sha256::new();
    hasher.update(VECTOR_SNAPSHOT_DIGEST_DOMAIN_V1.as_bytes());
    hasher.update([0]);
    hasher.update(payload);
    let digest = hasher.finalize();
    let hex = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    ManifestDigest::new(format!("sha256:{hex}")).unwrap()
}

fn generation(value: &str) -> CodeGenerationId {
    CodeGenerationId::new(value).unwrap()
}

fn chunk(value: &str) -> CodeSearchChunkId {
    CodeSearchChunkId::new(value).unwrap()
}

fn embedding_key(model_seed: u8, privacy_epoch: u64) -> AdmittedEmbeddingProjectionKeyV1 {
    EmbeddingProjectionKeyV1 {
        model_artifact_digest: digest(model_seed),
        tokenizer_digest: digest(model_seed.wrapping_add(1)),
        config_digest: digest(model_seed.wrapping_add(2)),
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
        privacy_domain: PrivacyDomainId::new("project-a").unwrap(),
        privacy_key_epoch: privacy_epoch,
    }
    .admit()
    .unwrap()
}

fn changes(
    from_generation: Option<CodeGenerationId>,
    to_generation: CodeGenerationId,
    added_or_changed: Vec<ChangedCodeChunkV1>,
    deleted: Vec<ChangedCodeChunkV1>,
    reused: Vec<ChangedCodeChunkV1>,
) -> ChangedCodeChunkSetV1 {
    let mut changes = ChangedCodeChunkSetV1 {
        from_generation,
        to_generation,
        manifest_digest: ManifestDigest::zero(),
        added_or_changed,
        deleted,
        reused,
    };
    changes.manifest_digest = changes.compute_digest().unwrap();
    changes
}

fn request(
    changes: ChangedCodeChunkSetV1,
    target_projection_key: ProjectionKeyV1,
    previous_projection_key: Option<ProjectionKeyV1>,
    replay_reason: ProjectionReplayReasonV1,
) -> ProjectionBatchRequestV1 {
    let mut request = ProjectionBatchRequestV1 {
        request_digest: ManifestDigest::zero(),
        changes,
        previous_projection_key,
        target_projection_key,
        replay_reason,
    };
    request.request_digest = request.compute_digest().unwrap();
    request
}

#[allow(clippy::too_many_arguments)]
fn receipt(
    admitted: &AdmittedEmbeddingProjectionKeyV1,
    request: &ProjectionBatchRequestV1,
    source_generation: CodeGenerationId,
    chunk_id: CodeSearchChunkId,
    prior_generation: Option<CodeGenerationId>,
    prior_chunk_digest: Option<ContentDigest>,
    current_chunk_digest: Option<ContentDigest>,
    operation: ProjectionOperationV1,
    outcome: ProjectionOutcomeV1,
    output_digest: Option<ContentDigest>,
) -> CodeChunkProjectionReceiptV1 {
    CodeChunkProjectionReceiptV1 {
        projection_key: admitted.projection_key().clone(),
        request_digest: request.request_digest.clone(),
        prior_generation,
        source_generation,
        source_manifest_digest: request.changes.manifest_digest.clone(),
        chunk_id,
        prior_chunk_digest,
        current_chunk_digest,
        operation,
        outcome,
        output_digest,
    }
}

fn batch_receipt(
    admitted: &AdmittedEmbeddingProjectionKeyV1,
    request: &ProjectionBatchRequestV1,
    receipts: Vec<CodeChunkProjectionReceiptV1>,
) -> ProjectionBatchReceiptV1 {
    let reused_count = receipts
        .iter()
        .filter(|receipt| receipt.operation == ProjectionOperationV1::Reused)
        .count() as u64;
    let mut receipt = ProjectionBatchReceiptV1 {
        target_projection_key: admitted.projection_key().clone(),
        request_digest: request.request_digest.clone(),
        source_generation: request.changes.to_generation.clone(),
        source_manifest_digest: request.changes.manifest_digest.clone(),
        receipts,
        reused_count,
        publication_digest: ManifestDigest::zero(),
    };
    receipt.publication_digest = receipt.expected_publication_digest().unwrap();
    receipt
}

fn prepared(
    admitted: &AdmittedEmbeddingProjectionKeyV1,
    request: ProjectionBatchRequestV1,
    receipts: Vec<CodeChunkProjectionReceiptV1>,
    vectors: Vec<ProjectedChunkVectorV1>,
    tombstones: Vec<VectorTombstoneV1>,
) -> PreparedVectorGenerationV1 {
    PreparedVectorGenerationV1 {
        embedding_key: admitted.clone(),
        receipt: batch_receipt(admitted, &request, receipts),
        request,
        vectors,
        tombstones,
    }
}

fn plan(
    admitted: &AdmittedEmbeddingProjectionKeyV1,
    source_generation: &str,
    source_manifest_seed: u8,
    expected_chunk_ids: Vec<CodeSearchChunkId>,
    base_generation: Option<VectorGenerationIdV1>,
) -> VectorGenerationPlanV1 {
    VectorGenerationPlanV1::new(
        admitted,
        generation(source_generation),
        digest(source_manifest_seed),
        expected_chunk_ids,
        base_generation,
    )
    .unwrap()
}

fn publish_single_added(
    authority: &mut VectorGenerationAuthority,
    admitted: &AdmittedEmbeddingProjectionKeyV1,
    source_generation: &str,
    source_manifest_seed: u8,
    chunk_id: CodeSearchChunkId,
    chunk_digest: ContentDigest,
    values: Vec<f32>,
) -> VectorGenerationPublicationV1 {
    let plan = plan(
        admitted,
        source_generation,
        source_manifest_seed,
        vec![chunk_id.clone()],
        None,
    );
    let build_id = authority.begin_generation(plan).unwrap();
    let changes = changes(
        None,
        generation(source_generation),
        vec![ChangedCodeChunkV1 {
            chunk_id: chunk_id.clone(),
            prior_digest: None,
            current_digest: Some(chunk_digest.clone()),
        }],
        Vec::new(),
        Vec::new(),
    );
    let request = request(
        changes,
        admitted.projection_key().clone(),
        None,
        ProjectionReplayReasonV1::SourceEdit,
    );
    let vector = prepare_vector(
        admitted,
        generation(source_generation),
        request.changes.manifest_digest.clone(),
        chunk_id.clone(),
        chunk_digest,
        values,
    )
    .unwrap();
    let receipt = receipt(
        admitted,
        &request,
        generation(source_generation),
        chunk_id,
        None,
        None,
        Some(vector.chunk_digest.clone()),
        ProjectionOperationV1::Added,
        ProjectionOutcomeV1::Applied,
        Some(vector.output_digest.clone()),
    );
    authority
        .commit_batch(
            &build_id,
            None,
            prepared(admitted, request, vec![receipt], vec![vector], Vec::new()),
        )
        .unwrap();
    authority.publish_generation(&build_id).unwrap()
}

#[test]
fn staged_generation_is_invisible_until_complete_publication_and_search_is_fenced() {
    let admitted = embedding_key(1, 1);
    let chunk_id = chunk("src/main.rs#0");
    let chunk_digest = content(11);
    let plan = plan(&admitted, "code-1", 21, vec![chunk_id.clone()], None);
    let build_id = plan.build_id().unwrap();
    let mut authority = VectorGenerationAuthority::new();
    assert_eq!(authority.begin_generation(plan.clone()).unwrap(), build_id);

    let changes = changes(
        None,
        generation("code-1"),
        vec![ChangedCodeChunkV1 {
            chunk_id: chunk_id.clone(),
            prior_digest: None,
            current_digest: Some(chunk_digest.clone()),
        }],
        Vec::new(),
        Vec::new(),
    );
    let request = request(
        changes,
        admitted.projection_key().clone(),
        None,
        ProjectionReplayReasonV1::SourceEdit,
    );
    let vector = prepare_vector(
        &admitted,
        generation("code-1"),
        request.changes.manifest_digest.clone(),
        chunk_id.clone(),
        chunk_digest,
        vec![1.0, 0.0],
    )
    .unwrap();
    let receipt = receipt(
        &admitted,
        &request,
        generation("code-1"),
        chunk_id.clone(),
        None,
        None,
        Some(vector.chunk_digest.clone()),
        ProjectionOperationV1::Add,
        ProjectionOutcomeV1::Applied,
        Some(vector.output_digest.clone()),
    );
    let prepared = prepared(&admitted, request, vec![receipt], vec![vector], Vec::new());

    assert!(matches!(
        authority.publish_generation(&build_id),
        Err(VectorAuthorityError::IncompleteGeneration)
    ));
    authority.commit_batch(&build_id, None, prepared).unwrap();
    assert!(matches!(
        authority.search_active(vec![1.0, 0.0], 5),
        Err(VectorAuthorityError::UnknownGeneration)
    ));
    let publication = authority.publish_generation(&build_id).unwrap();
    assert_eq!(
        authority.active_generation(),
        Some(&publication.generation_id)
    );
    let hits = authority.search_active(vec![1.0, 0.0], 5).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].chunk_id, chunk_id);
    assert_eq!(hits[0].score, 1.0);
}

#[test]
fn validated_batch_survives_crash_before_apply_and_resume_checkpoint() {
    let admitted = embedding_key(2, 1);
    let first_id = chunk("src/lib.rs#0");
    let second_id = chunk("src/lib.rs#1");
    let first_digest = content(31);
    let second_digest = content(32);
    let plan = plan(
        &admitted,
        "code-resume",
        41,
        vec![first_id.clone(), second_id.clone()],
        None,
    );
    let build_id = plan.build_id().unwrap();
    let mut authority = VectorGenerationAuthority::new();
    authority.begin_generation(plan).unwrap();

    let first_changes = changes(
        None,
        generation("code-resume"),
        vec![ChangedCodeChunkV1 {
            chunk_id: first_id.clone(),
            prior_digest: None,
            current_digest: Some(first_digest.clone()),
        }],
        Vec::new(),
        Vec::new(),
    );
    let first_request = request(
        first_changes,
        admitted.projection_key().clone(),
        None,
        ProjectionReplayReasonV1::SourceEdit,
    );
    let first_vector = prepare_vector(
        &admitted,
        generation("code-resume"),
        first_request.changes.manifest_digest.clone(),
        first_id.clone(),
        first_digest,
        vec![1.0, 0.0],
    )
    .unwrap();
    let first_receipt = receipt(
        &admitted,
        &first_request,
        generation("code-resume"),
        first_id.clone(),
        None,
        None,
        Some(first_vector.chunk_digest.clone()),
        ProjectionOperationV1::Added,
        ProjectionOutcomeV1::Applied,
        Some(first_vector.output_digest.clone()),
    );
    let first_prepared = prepared(
        &admitted,
        first_request,
        vec![first_receipt],
        vec![first_vector],
        Vec::new(),
    );
    let decision = match authority
        .validate_batch(&build_id, None, &first_prepared)
        .unwrap()
    {
        BatchCommitDecisionV1::Commit(decision) => decision,
        BatchCommitDecisionV1::Replay(_) => panic!("first batch cannot replay"),
    };
    let sealed_before_apply = authority.persist_sealed().unwrap();
    let mut restarted = VectorGenerationAuthority::reopen_sealed(&sealed_before_apply).unwrap();
    assert_eq!(
        restarted
            .staged_checkpoint(&build_id)
            .unwrap()
            .completed_batches,
        0
    );
    restarted.apply_batch(&build_id, decision).unwrap();
    let checkpoint = restarted.staged_checkpoint(&build_id).unwrap().clone();

    let second_changes = changes(
        None,
        generation("code-resume"),
        vec![ChangedCodeChunkV1 {
            chunk_id: second_id.clone(),
            prior_digest: None,
            current_digest: Some(second_digest.clone()),
        }],
        Vec::new(),
        Vec::new(),
    );
    let second_request = request(
        second_changes,
        admitted.projection_key().clone(),
        None,
        ProjectionReplayReasonV1::SourceEdit,
    );
    let second_vector = prepare_vector(
        &admitted,
        generation("code-resume"),
        second_request.changes.manifest_digest.clone(),
        second_id.clone(),
        second_digest,
        vec![0.0, 1.0],
    )
    .unwrap();
    let second_receipt = receipt(
        &admitted,
        &second_request,
        generation("code-resume"),
        second_id.clone(),
        None,
        None,
        Some(second_vector.chunk_digest.clone()),
        ProjectionOperationV1::Added,
        ProjectionOutcomeV1::Applied,
        Some(second_vector.output_digest.clone()),
    );
    restarted
        .commit_batch(
            &build_id,
            Some(&checkpoint),
            prepared(
                &admitted,
                second_request,
                vec![second_receipt],
                vec![second_vector],
                Vec::new(),
            ),
        )
        .unwrap();
    let publication = restarted.publish_generation(&build_id).unwrap();
    assert_eq!(publication.previous_active_generation, None);
    assert_eq!(
        restarted
            .search_exact_flat(&publication.generation_id, vec![1.0, 0.0], 5)
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn update_reuse_delete_share_base_bytes_and_rollback_is_atomic() {
    let admitted = embedding_key(3, 1);
    let a = chunk("src/a.rs#0");
    let b = chunk("src/b.rs#0");
    let c = chunk("src/c.rs#0");
    let d = chunk("src/d.rs#0");
    let a_digest = content(51);
    let b_digest = content(52);
    let c_digest = content(53);
    let d_digest = content(54);
    let updated_a_digest = content(55);
    let mut authority = VectorGenerationAuthority::new();

    let base_plan = plan(
        &admitted,
        "code-base",
        61,
        vec![a.clone(), b.clone(), d.clone()],
        None,
    );
    let base_build = authority.begin_generation(base_plan).unwrap();
    let base_changes = changes(
        None,
        generation("code-base"),
        vec![
            ChangedCodeChunkV1 {
                chunk_id: a.clone(),
                prior_digest: None,
                current_digest: Some(a_digest.clone()),
            },
            ChangedCodeChunkV1 {
                chunk_id: b.clone(),
                prior_digest: None,
                current_digest: Some(b_digest.clone()),
            },
            ChangedCodeChunkV1 {
                chunk_id: d.clone(),
                prior_digest: None,
                current_digest: Some(d_digest.clone()),
            },
        ],
        Vec::new(),
        Vec::new(),
    );
    let base_request = request(
        base_changes,
        admitted.projection_key().clone(),
        None,
        ProjectionReplayReasonV1::SourceEdit,
    );
    let base_a = prepare_vector(
        &admitted,
        generation("code-base"),
        base_request.changes.manifest_digest.clone(),
        a.clone(),
        a_digest.clone(),
        vec![1.0, 0.0],
    )
    .unwrap();
    let base_b = prepare_vector(
        &admitted,
        generation("code-base"),
        base_request.changes.manifest_digest.clone(),
        b.clone(),
        b_digest.clone(),
        vec![0.0, 1.0],
    )
    .unwrap();
    let base_d = prepare_vector(
        &admitted,
        generation("code-base"),
        base_request.changes.manifest_digest.clone(),
        d.clone(),
        d_digest.clone(),
        vec![0.5, 0.5],
    )
    .unwrap();
    let base_receipts = vec![
        receipt(
            &admitted,
            &base_request,
            generation("code-base"),
            a.clone(),
            None,
            None,
            Some(a_digest.clone()),
            ProjectionOperationV1::Add,
            ProjectionOutcomeV1::Applied,
            Some(base_a.output_digest.clone()),
        ),
        receipt(
            &admitted,
            &base_request,
            generation("code-base"),
            b.clone(),
            None,
            None,
            Some(b_digest.clone()),
            ProjectionOperationV1::Add,
            ProjectionOutcomeV1::Applied,
            Some(base_b.output_digest.clone()),
        ),
        receipt(
            &admitted,
            &base_request,
            generation("code-base"),
            d.clone(),
            None,
            None,
            Some(d_digest.clone()),
            ProjectionOperationV1::Add,
            ProjectionOutcomeV1::Applied,
            Some(base_d.output_digest.clone()),
        ),
    ];
    authority
        .commit_batch(
            &base_build,
            None,
            prepared(
                &admitted,
                base_request,
                base_receipts,
                vec![base_a.clone(), base_b.clone(), base_d.clone()],
                Vec::new(),
            ),
        )
        .unwrap();
    let base_publication = authority.publish_generation(&base_build).unwrap();
    let pool_after_base = authority.vector_pool_len();

    let target_plan = plan(
        &admitted,
        "code-target",
        62,
        vec![a.clone(), c.clone(), d.clone()],
        Some(base_publication.generation_id.clone()),
    );
    let target_build = authority.begin_generation(target_plan).unwrap();
    let target_changes = changes(
        Some(generation("code-base")),
        generation("code-target"),
        vec![
            ChangedCodeChunkV1 {
                chunk_id: a.clone(),
                prior_digest: Some(a_digest.clone()),
                current_digest: Some(updated_a_digest.clone()),
            },
            ChangedCodeChunkV1 {
                chunk_id: c.clone(),
                prior_digest: None,
                current_digest: Some(c_digest.clone()),
            },
        ],
        vec![ChangedCodeChunkV1 {
            chunk_id: b.clone(),
            prior_digest: Some(b_digest.clone()),
            current_digest: None,
        }],
        vec![ChangedCodeChunkV1 {
            chunk_id: d.clone(),
            prior_digest: Some(d_digest.clone()),
            current_digest: Some(d_digest.clone()),
        }],
    );
    let target_request = request(
        target_changes,
        admitted.projection_key().clone(),
        Some(admitted.projection_key().clone()),
        ProjectionReplayReasonV1::SourceEdit,
    );
    let target_c = prepare_vector(
        &admitted,
        generation("code-target"),
        target_request.changes.manifest_digest.clone(),
        c.clone(),
        c_digest,
        vec![0.7, 0.7],
    )
    .unwrap();
    let target_a = prepare_vector(
        &admitted,
        generation("code-target"),
        target_request.changes.manifest_digest.clone(),
        a.clone(),
        updated_a_digest.clone(),
        vec![0.8, 0.2],
    )
    .unwrap();
    let target_receipts = vec![
        receipt(
            &admitted,
            &target_request,
            generation("code-target"),
            a.clone(),
            Some(generation("code-base")),
            Some(a_digest.clone()),
            Some(updated_a_digest.clone()),
            ProjectionOperationV1::Update,
            ProjectionOutcomeV1::Applied,
            Some(target_a.output_digest.clone()),
        ),
        receipt(
            &admitted,
            &target_request,
            generation("code-target"),
            b.clone(),
            Some(generation("code-base")),
            Some(b_digest.clone()),
            None,
            ProjectionOperationV1::Delete,
            ProjectionOutcomeV1::Tombstoned,
            None,
        ),
        receipt(
            &admitted,
            &target_request,
            generation("code-target"),
            c.clone(),
            Some(generation("code-base")),
            None,
            Some(target_c.chunk_digest.clone()),
            ProjectionOperationV1::Add,
            ProjectionOutcomeV1::Applied,
            Some(target_c.output_digest.clone()),
        ),
        receipt(
            &admitted,
            &target_request,
            generation("code-target"),
            d.clone(),
            Some(generation("code-base")),
            Some(d_digest.clone()),
            Some(d_digest.clone()),
            ProjectionOperationV1::Reuse,
            ProjectionOutcomeV1::Reused,
            None,
        ),
    ];
    authority
        .commit_batch(
            &target_build,
            None,
            prepared(
                &admitted,
                target_request,
                target_receipts,
                vec![target_a.clone(), target_c.clone()],
                vec![VectorTombstoneV1 {
                    chunk_id: b.clone(),
                    prior_chunk_digest: b_digest,
                }],
            ),
        )
        .unwrap();

    let wrong_expected = Some(&base_publication.generation_id);
    let wrong = VectorGenerationIdV1::new(digest(99));
    assert!(matches!(
        authority.publish_generation_if_current(&target_build, Some(&wrong)),
        Err(VectorAuthorityError::ActivePointerMismatch { .. })
    ));
    assert_eq!(authority.active_generation(), wrong_expected);
    let target_publication = authority
        .publish_generation_if_current(&target_build, wrong_expected)
        .unwrap();
    assert_eq!(
        target_publication.previous_active_generation,
        wrong_expected.cloned()
    );
    assert_eq!(authority.vector_pool_len(), pool_after_base + 2);
    let target_hits = authority
        .search_exact_flat(&target_publication.generation_id, vec![1.0, 1.0], 3)
        .unwrap();
    assert_eq!(
        target_hits
            .iter()
            .map(|hit| hit.chunk_id.clone())
            .collect::<Vec<_>>(),
        vec![c.clone(), d.clone(), a.clone()]
    );
    assert!(
        authority
            .read_vector(&target_publication.generation_id, &a)
            .is_some()
    );
    assert!(
        authority
            .read_vector(&target_publication.generation_id, &b)
            .is_none()
    );
    assert_eq!(
        authority
            .generation(&target_publication.generation_id)
            .unwrap()
            .tombstones(),
        vec![b.clone()]
    );
    assert_eq!(
        authority.rollback().unwrap(),
        base_publication.generation_id
    );
    assert_eq!(
        authority.active_generation(),
        Some(&base_publication.generation_id)
    );
}

#[test]
fn corrupted_snapshot_and_incompatible_search_context_are_rejected() {
    let admitted = embedding_key(4, 7);
    let id = chunk("src/main.rs#0");
    let chunk_digest = content(71);
    let plan = plan(&admitted, "code-corrupt", 81, vec![id.clone()], None);
    let build_id = plan.build_id().unwrap();
    let mut authority = VectorGenerationAuthority::new();
    authority.begin_generation(plan).unwrap();
    let changes = changes(
        None,
        generation("code-corrupt"),
        vec![ChangedCodeChunkV1 {
            chunk_id: id.clone(),
            prior_digest: None,
            current_digest: Some(chunk_digest.clone()),
        }],
        Vec::new(),
        Vec::new(),
    );
    let request = request(
        changes,
        admitted.projection_key().clone(),
        None,
        ProjectionReplayReasonV1::SourceEdit,
    );
    let vector = prepare_vector(
        &admitted,
        generation("code-corrupt"),
        request.changes.manifest_digest.clone(),
        id.clone(),
        chunk_digest,
        vec![1.0, 0.0],
    )
    .unwrap();
    let receipt = receipt(
        &admitted,
        &request,
        generation("code-corrupt"),
        id,
        None,
        None,
        Some(vector.chunk_digest.clone()),
        ProjectionOperationV1::Added,
        ProjectionOutcomeV1::Applied,
        Some(vector.output_digest.clone()),
    );
    authority
        .commit_batch(
            &build_id,
            None,
            prepared(&admitted, request, vec![receipt], vec![vector], Vec::new()),
        )
        .unwrap();
    let publication = authority.publish_generation(&build_id).unwrap();
    let generation = authority.generation(&publication.generation_id).unwrap();
    let mut compatibility = SearchCompatibilityV1::from_generation(generation);
    compatibility.privacy_key_epoch += 1;
    let search_request = VectorSearchRequestV1 {
        generation_id: publication.generation_id.clone(),
        query: vec![1.0, 0.0],
        compatibility,
        limit: 1,
    };
    assert!(matches!(
        authority.search(&search_request),
        Err(VectorAuthorityError::SearchContextMismatch(_))
    ));

    let mut corrupted = authority.persist_sealed().unwrap();
    let midpoint = corrupted.len() / 2;
    corrupted[midpoint] ^= 0x01;
    assert!(matches!(
        VectorGenerationAuthority::reopen_sealed(&corrupted),
        Err(VectorAuthorityError::Corrupt(_))
    ));
}

#[test]
fn tampered_batch_digest_and_foreign_chunk_scope_are_rejected_without_mutation() {
    let admitted = embedding_key(5, 3);
    let id = chunk("src/tamper.rs#0");
    let source_generation = generation("code-tamper");
    let source_manifest = digest(121);
    let plan = plan(
        &admitted,
        source_generation.as_str(),
        121,
        vec![id.clone()],
        None,
    );
    let build_id = plan.build_id().unwrap();
    let mut authority = VectorGenerationAuthority::new();
    authority.begin_generation(plan.clone()).unwrap();

    let canonical_chunk = CodeSearchChunkV1::from_text(
        id.clone(),
        source_generation.clone(),
        source_manifest.clone(),
        admitted.privacy_domain().clone(),
        admitted.privacy_key_epoch(),
        "fn tamper() {}",
    )
    .unwrap();
    canonical_chunk
        .validate_for_generation(&plan, &admitted)
        .unwrap();
    let mut foreign_chunk = canonical_chunk.clone();
    foreign_chunk.source_generation = generation("other-scope");
    assert!(matches!(
        foreign_chunk.validate_for_generation(&plan, &admitted),
        Err(VectorAuthorityError::BatchIdentityMismatch(_))
    ));

    let changes = changes(
        None,
        source_generation.clone(),
        vec![ChangedCodeChunkV1 {
            chunk_id: id.clone(),
            prior_digest: None,
            current_digest: Some(content(122)),
        }],
        Vec::new(),
        Vec::new(),
    );
    let request = request(
        changes,
        admitted.projection_key().clone(),
        None,
        ProjectionReplayReasonV1::SourceEdit,
    );
    let vector = prepare_vector(
        &admitted,
        source_generation.clone(),
        request.changes.manifest_digest.clone(),
        id.clone(),
        content(122),
        vec![1.0, 0.0],
    )
    .unwrap();
    let receipt = receipt(
        &admitted,
        &request,
        source_generation,
        id,
        None,
        None,
        Some(vector.chunk_digest.clone()),
        ProjectionOperationV1::Add,
        ProjectionOutcomeV1::Applied,
        Some(vector.output_digest.clone()),
    );
    let valid_prepared = prepared(&admitted, request, vec![receipt], vec![vector], Vec::new());
    let mut tampered = valid_prepared.clone();
    tampered.vectors[0].output_digest = ContentDigest::zero();
    assert!(matches!(
        authority.commit_batch(&build_id, None, tampered),
        Err(VectorAuthorityError::BatchIdentityMismatch(_))
            | Err(VectorAuthorityError::InvalidVector(_))
    ));
    assert_eq!(
        authority
            .staged_checkpoint(&build_id)
            .unwrap()
            .completed_batches,
        0
    );
    assert_eq!(authority.vector_pool_len(), 0);
    assert!(authority.active_generation().is_none());

    let mut tampered_receipt = valid_prepared;
    tampered_receipt.receipt.publication_digest = ManifestDigest::zero();
    assert!(matches!(
        authority.commit_batch(&build_id, None, tampered_receipt),
        Err(VectorAuthorityError::BatchIdentityMismatch(_))
    ));
    assert_eq!(authority.vector_pool_len(), 0);
}

#[test]
fn recomputed_checksum_cannot_admit_swapped_persisted_rows_and_receipts() {
    let admitted = embedding_key(6, 4);
    let first_id = chunk("src/swap-a.rs#0");
    let second_id = chunk("src/swap-b.rs#0");
    let first_digest = content(131);
    let second_digest = content(132);
    let updated_first_digest = content(133);
    let mut authority = VectorGenerationAuthority::new();
    let base = publish_single_added(
        &mut authority,
        &admitted,
        "code-swap-base",
        134,
        first_id.clone(),
        first_digest.clone(),
        vec![1.0, 0.0],
    );

    let target_plan = plan(
        &admitted,
        "code-swap-target",
        135,
        vec![first_id.clone(), second_id.clone()],
        Some(base.generation_id.clone()),
    );
    let target_build = authority.begin_generation(target_plan).unwrap();
    let target_changes = changes(
        Some(generation("code-swap-base")),
        generation("code-swap-target"),
        vec![
            ChangedCodeChunkV1 {
                chunk_id: first_id.clone(),
                prior_digest: Some(first_digest.clone()),
                current_digest: Some(updated_first_digest.clone()),
            },
            ChangedCodeChunkV1 {
                chunk_id: second_id.clone(),
                prior_digest: None,
                current_digest: Some(second_digest.clone()),
            },
        ],
        Vec::new(),
        Vec::new(),
    );
    let target_request = request(
        target_changes,
        admitted.projection_key().clone(),
        Some(admitted.projection_key().clone()),
        ProjectionReplayReasonV1::SourceEdit,
    );
    let first_vector = prepare_vector(
        &admitted,
        generation("code-swap-target"),
        target_request.changes.manifest_digest.clone(),
        first_id.clone(),
        updated_first_digest.clone(),
        vec![0.8, 0.2],
    )
    .unwrap();
    let second_vector = prepare_vector(
        &admitted,
        generation("code-swap-target"),
        target_request.changes.manifest_digest.clone(),
        second_id.clone(),
        second_digest.clone(),
        vec![0.2, 0.8],
    )
    .unwrap();
    let target_receipts = vec![
        receipt(
            &admitted,
            &target_request,
            generation("code-swap-target"),
            first_id,
            Some(generation("code-swap-base")),
            Some(first_digest),
            Some(updated_first_digest),
            ProjectionOperationV1::Update,
            ProjectionOutcomeV1::Applied,
            Some(first_vector.output_digest.clone()),
        ),
        receipt(
            &admitted,
            &target_request,
            generation("code-swap-target"),
            second_id,
            Some(generation("code-swap-base")),
            None,
            Some(second_vector.chunk_digest.clone()),
            ProjectionOperationV1::Add,
            ProjectionOutcomeV1::Applied,
            Some(second_vector.output_digest.clone()),
        ),
    ];
    authority
        .commit_batch(
            &target_build,
            None,
            prepared(
                &admitted,
                target_request,
                target_receipts,
                vec![first_vector, second_vector],
                Vec::new(),
            ),
        )
        .unwrap();
    let publication = authority.publish_generation(&target_build).unwrap();

    let sealed = authority.persist_sealed().unwrap();
    let mut envelope: Value = serde_json::from_slice(&sealed).unwrap();
    let payload = envelope["payload"]
        .as_array()
        .unwrap()
        .iter()
        .map(|byte| byte.as_u64().unwrap() as u8)
        .collect::<Vec<_>>();
    let mut state: Value = serde_json::from_slice(&payload).unwrap();
    let generation_value = state["published"]
        .get_mut(publication.generation_id.as_str())
        .unwrap();
    let rows = generation_value["rows"].as_object_mut().unwrap();
    let row_keys = rows.keys().cloned().collect::<Vec<_>>();
    assert_eq!(row_keys.len(), 2);
    let first_row = rows.get(&row_keys[0]).unwrap().clone();
    let second_row = rows.get(&row_keys[1]).unwrap().clone();
    rows.insert(row_keys[0].clone(), second_row);
    rows.insert(row_keys[1].clone(), first_row);

    // Keep the receipt list canonical while moving each receipt's content
    // evidence with the swapped row.  This leaves map-key identity as the
    // only invalid boundary in the recomputed snapshot.
    let batch_value = generation_value["receipts"]
        .as_array_mut()
        .unwrap()
        .first_mut()
        .unwrap();
    let receipts = batch_value["receipts"].as_array_mut().unwrap();
    let first_current = receipts[0]["current_chunk_digest"].clone();
    let first_output = receipts[0]["output_digest"].clone();
    receipts[0]["current_chunk_digest"] = receipts[1]["current_chunk_digest"].clone();
    receipts[0]["output_digest"] = receipts[1]["output_digest"].clone();
    receipts[1]["current_chunk_digest"] = first_current;
    receipts[1]["output_digest"] = first_output;
    let mut batch: ProjectionBatchReceiptV1 = serde_json::from_value(batch_value.clone()).unwrap();
    batch.publication_digest = batch.expected_publication_digest().unwrap();
    *batch_value = serde_json::to_value(&batch).unwrap();
    generation_value["checkpoint"]["last_publication_digest"] =
        serde_json::to_value(batch.publication_digest).unwrap();

    let tampered_payload = serde_json::to_vec(&state).unwrap();
    envelope["payload"] = serde_json::to_value(&tampered_payload).unwrap();
    envelope["checksum"] = serde_json::to_value(snapshot_checksum(&tampered_payload)).unwrap();
    let tampered = serde_json::to_vec(&envelope).unwrap();
    assert!(matches!(
        VectorGenerationAuthority::reopen_sealed(&tampered),
        Err(VectorAuthorityError::Corrupt(_))
    ));
}

#[test]
fn generation_identity_binds_source_model_and_membership_but_not_base_lineage() {
    let admitted = embedding_key(8, 1);
    let id = chunk("src/identity.rs#0");
    let base = plan(&admitted, "code-identity", 91, vec![id.clone()], None);
    let rebuilt_from_other_base = plan(
        &admitted,
        "code-identity",
        91,
        vec![id.clone()],
        Some(VectorGenerationIdV1::new(digest(92))),
    );
    assert_eq!(
        base.generation_id().unwrap(),
        rebuilt_from_other_base.generation_id().unwrap()
    );
    assert_ne!(
        base.build_id().unwrap(),
        rebuilt_from_other_base.build_id().unwrap()
    );

    let other_model = plan(
        &embedding_key(9, 1),
        "code-identity",
        91,
        vec![id.clone()],
        None,
    );
    assert_ne!(
        base.generation_id().unwrap(),
        other_model.generation_id().unwrap()
    );

    let other_source = plan(&admitted, "code-identity-next", 91, vec![id.clone()], None);
    assert_ne!(
        base.generation_id().unwrap(),
        other_source.generation_id().unwrap()
    );

    let other_membership = plan(
        &admitted,
        "code-identity",
        91,
        vec![id, chunk("src/identity.rs#1")],
        None,
    );
    assert_ne!(
        base.generation_id().unwrap(),
        other_membership.generation_id().unwrap()
    );
}

#[test]
fn projection_profile_change_reembeds_a_reused_partition_from_the_base() {
    let old_key = embedding_key(10, 1);
    let new_key = embedding_key(11, 1);
    let chunk_id = chunk("src/profile-change.rs#0");
    let prior_digest = content(101);
    let mut authority = VectorGenerationAuthority::new();
    let base = publish_single_added(
        &mut authority,
        &old_key,
        "code-profile-base",
        102,
        chunk_id.clone(),
        prior_digest.clone(),
        vec![1.0, 0.0],
    );

    let target_plan = plan(
        &new_key,
        "code-profile-target",
        103,
        vec![chunk_id.clone()],
        Some(base.generation_id.clone()),
    );
    let target_generation_id = target_plan.generation_id().unwrap();
    let target_build = authority.begin_generation(target_plan).unwrap();
    let target_changes = changes(
        Some(generation("code-profile-base")),
        generation("code-profile-target"),
        Vec::new(),
        Vec::new(),
        vec![ChangedCodeChunkV1 {
            chunk_id: chunk_id.clone(),
            prior_digest: Some(prior_digest.clone()),
            current_digest: Some(prior_digest.clone()),
        }],
    );
    let target_request = request(
        target_changes,
        new_key.projection_key().clone(),
        Some(old_key.projection_key().clone()),
        ProjectionReplayReasonV1::ProjectionProfileChange,
    );
    let target_vector = prepare_vector(
        &new_key,
        generation("code-profile-target"),
        target_request.changes.manifest_digest.clone(),
        chunk_id.clone(),
        prior_digest.clone(),
        vec![0.0, 1.0],
    )
    .unwrap();
    let target_receipt = receipt(
        &new_key,
        &target_request,
        generation("code-profile-target"),
        chunk_id.clone(),
        Some(generation("code-profile-base")),
        Some(prior_digest.clone()),
        Some(prior_digest),
        ProjectionOperationV1::Update,
        ProjectionOutcomeV1::Applied,
        Some(target_vector.output_digest.clone()),
    );
    authority
        .commit_batch(
            &target_build,
            None,
            prepared(
                &new_key,
                target_request,
                vec![target_receipt],
                vec![target_vector],
                Vec::new(),
            ),
        )
        .unwrap();
    let publication = authority.publish_generation(&target_build).unwrap();
    assert_eq!(publication.generation_id, target_generation_id);
    assert_eq!(
        authority.read_vector(&publication.generation_id, &chunk_id),
        Some(vec![0.0, 1.0])
    );
}
