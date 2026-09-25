use std::collections::BTreeSet;
use std::sync::Arc;

use tracedecay_code_index::production::CodeIndexPublishedGenerationV1;
use tracedecay_domain::{
    CodeGenerationId, CodeSearchChunkV1, CompactCandidate, ComponentRevision,
    EmbeddingNormalizationV1, EvidenceRole, FixedPointScore, LogicalEvidenceId, ManifestDigest,
    PrivacyDomainId, ProjectionKeyV1, RetrievalAnchorId, RetrieverKind, ScoreDomainId,
    SemanticSearchIndexKeyV1, SemanticSourceScopeV1, SourceOccurrenceId, VectorGenerationIdV1,
};
use tracedecay_query::retrieval::graph::production_code_index_freshness;
use tracedecay_query::retrieval::ports::{
    CodeCandidateBindingV1, CodeOccurrenceRefV1, RetrievalPortError,
};
use tracedecay_query::retrieval::semantic::{
    SemanticSearchKindV1, SemanticVectorReadPort, SemanticVectorReadRequestV1,
    SemanticVectorRecordV1, SemanticVectorScanSummaryV1,
};
use tracedecay_vector_authority::PublishedVectorReadSnapshotV1;

/// Request-facing view of one immutable vector publication and the exact code
/// generation that supplies its candidate evidence.
#[derive(Debug)]
pub struct PublishedVectorReadPortV1 {
    generation: VectorGenerationIdV1,
    projection_key: ProjectionKeyV1,
    model_artifact_digest: ManifestDigest,
    search_index_key: SemanticSearchIndexKeyV1,
    source_generation: CodeGenerationId,
    capability_manifest_digest: ManifestDigest,
    source_scope: SemanticSourceScopeV1,
    privacy_domain: PrivacyDomainId,
    privacy_key_epoch: u64,
    rows: Arc<[SemanticVectorRecordV1]>,
}

impl PublishedVectorReadPortV1 {
    pub fn new(
        snapshot: Arc<PublishedVectorReadSnapshotV1>,
        code: Arc<CodeIndexPublishedGenerationV1>,
        search_index_key: SemanticSearchIndexKeyV1,
    ) -> Result<Self, RetrievalPortError> {
        let compatibility = snapshot.compatibility();
        let embedding_key = snapshot.embedding_key().embedding_key();
        let manifest = code.manifest();

        if compatibility.source_generation != manifest.generation_id {
            return Err(RetrievalPortError::GenerationMismatch);
        }
        if compatibility.projection_key != *snapshot.embedding_key().projection_key()
            || embedding_key.normalization != EmbeddingNormalizationV1::L2
            || compatibility.privacy_domain != manifest.privacy_domain
            || compatibility.privacy_key_epoch != manifest.privacy_key_epoch
            || compatibility.privacy_domain != *snapshot.embedding_key().privacy_domain()
            || compatibility.privacy_key_epoch != snapshot.embedding_key().privacy_key_epoch()
        {
            return Err(RetrievalPortError::IncompatibleProjection);
        }
        search_index_key
            .validate()
            .map_err(|error| RetrievalPortError::Contract(error.to_string()))?;

        let source_scope = semantic_source_scope(&code)?;
        let freshness = production_code_index_freshness(
            manifest.seal.sealed_at,
            ComponentRevision::new("policy.semantic.daemon.v1")
                .map_err(|error| RetrievalPortError::Contract(error.to_string()))?,
        )?;
        let score_domain =
            ScoreDomainId::new(tracedecay_query::retrieval::QUERY_SEMANTIC_SCORE_DOMAIN_V1)
                .map_err(|error| RetrievalPortError::Contract(error.to_string()))?;
        let mut seen = BTreeSet::new();
        let mut rows = Vec::with_capacity(snapshot.rows().len());
        for (ordinal, row) in snapshot.rows().iter().enumerate() {
            if !seen.insert(row.chunk_id().clone()) {
                return Err(RetrievalPortError::GenerationMismatch);
            }
            let chunk = code
                .chunks()
                .chunk(row.chunk_id())
                .ok_or(RetrievalPortError::GenerationMismatch)?;
            if row.chunk_digest() != &chunk.content_digest {
                return Err(RetrievalPortError::StaleEvidence);
            }
            let (anchor_id, logical_evidence_id, source_occurrence) =
                semantic_candidate_identity(chunk)?;
            let ordinal_rank = u32::try_from(ordinal).map_err(|_| {
                RetrievalPortError::Contract("semantic vector row ordinal exceeds u32".to_owned())
            })?;
            rows.push(SemanticVectorRecordV1 {
                vector_generation: snapshot.generation_id().clone(),
                projection_key: compatibility.projection_key.clone(),
                source_generation: manifest.generation_id.clone(),
                chunk_id: row.chunk_id().clone(),
                candidate: CompactCandidate {
                    anchor_id: anchor_id.clone(),
                    logical_evidence_id,
                    source_occurrence_id: source_occurrence.clone(),
                    file_occurrence_id: Some(chunk.anchor.file_occurrence_id.clone()),
                    source_namespace: freshness.source_namespace.clone(),
                    repository_id: Some(code.snapshot().repository.clone()),
                    session_or_thread_id: None,
                    logical_copy_cluster_id: None,
                    logical_copy_evidence_anchor: None,
                    evidence_role: EvidenceRole::Primary,
                    retriever: RetrieverKind::Semantic,
                    retriever_revision: ComponentRevision::new("retriever.semantic-flat.daemon.v1")
                        .map_err(|error| RetrievalPortError::Contract(error.to_string()))?,
                    score_domain: score_domain.clone(),
                    raw_score: FixedPointScore::ZERO,
                    ordinal_rank,
                    exact_admission_proof: None,
                    retriever_evidence_anchor: RetrievalAnchorId::new(format!(
                        "code-semantic:{}",
                        row.chunk_id().as_str()
                    ))
                    .map_err(|error| RetrievalPortError::Contract(error.to_string()))?,
                    freshness: freshness.clone(),
                },
                binding: CodeCandidateBindingV1 {
                    candidate_anchor: anchor_id,
                    occurrence: CodeOccurrenceRefV1 {
                        generation: chunk.anchor.generation_id.clone(),
                        file: chunk.anchor.file_occurrence_id.clone(),
                        symbol: chunk.anchor.symbol_occurrence_id.clone(),
                        chunk: Some(row.chunk_id().clone()),
                    },
                    language_descriptor_revision: chunk.language_descriptor_revision.clone(),
                    matched_term_kinds: Vec::new(),
                    source_occurrence,
                },
                values: Arc::clone(row.values()),
            });
        }
        if seen.len() != code.chunks().chunks().len() {
            return Err(RetrievalPortError::GenerationMismatch);
        }

        Ok(Self {
            generation: snapshot.generation_id().clone(),
            projection_key: compatibility.projection_key.clone(),
            model_artifact_digest: embedding_key.model_artifact_digest.clone(),
            search_index_key,
            source_generation: compatibility.source_generation.clone(),
            capability_manifest_digest: code.capability().manifest_digest.clone(),
            source_scope,
            privacy_domain: compatibility.privacy_domain.clone(),
            privacy_key_epoch: compatibility.privacy_key_epoch,
            rows: rows.into(),
        })
    }

    pub fn source_scope(&self) -> &SemanticSourceScopeV1 {
        &self.source_scope
    }

    fn validate_request(
        &self,
        request: &SemanticVectorReadRequestV1<'_>,
    ) -> Result<(), RetrievalPortError> {
        if request.vector_generation != &self.generation
            || request.source_generation != &self.source_generation
            || request.source_scope != &self.source_scope
        {
            return Err(RetrievalPortError::StaleEvidence);
        }
        if request.search_kind != SemanticSearchKindV1::ExactFlat
            || request.projection_key != &self.projection_key
            || request.model_artifact_digest != &self.model_artifact_digest
            || request.search_index_key != &self.search_index_key
            || request.capability_manifest_digest != &self.capability_manifest_digest
            || request.privacy_domain != &self.privacy_domain
            || request.privacy_key_epoch != self.privacy_key_epoch
        {
            return Err(RetrievalPortError::IncompatibleProjection);
        }
        Ok(())
    }
}

impl SemanticVectorReadPort for PublishedVectorReadPortV1 {
    fn scan_exact_flat(
        &self,
        request: SemanticVectorReadRequestV1<'_>,
        examine: &mut dyn FnMut() -> Result<(), RetrievalPortError>,
        visit: &mut dyn FnMut(&SemanticVectorRecordV1) -> Result<(), RetrievalPortError>,
    ) -> Result<SemanticVectorScanSummaryV1, RetrievalPortError> {
        self.validate_request(&request)?;
        hotpath::gauge!("semantic_exact_flat_scan_rows").set(self.rows.len());
        hotpath::gauge!("semantic_exact_flat_scan_dimensions")
            .set(self.rows.first().map_or(0, |row| row.values.len()));
        hotpath::measure_block!("semantic.vector.scan_exact_flat", {
            for row in self.rows.iter() {
                examine()?;
                visit(row)?;
            }
            Ok::<(), RetrievalPortError>(())
        })?;
        let count = u64::try_from(self.rows.len()).map_err(|_| {
            RetrievalPortError::Contract("semantic vector row count exceeds u64".to_owned())
        })?;
        Ok(SemanticVectorScanSummaryV1 {
            examined: count,
            eligible: count,
            excluded: 0,
            unknown: 0,
        })
    }
}

fn semantic_source_scope(
    code: &CodeIndexPublishedGenerationV1,
) -> Result<SemanticSourceScopeV1, RetrievalPortError> {
    Ok(SemanticSourceScopeV1 {
        project_id: code.manifest().project_id.clone(),
        repository_id: code.snapshot().repository.clone(),
        worktree_id: code
            .snapshot()
            .worktree
            .clone()
            .ok_or(RetrievalPortError::GenerationMismatch)?,
        reference: code.snapshot().reference.clone(),
    })
}

fn semantic_candidate_identity(
    chunk: &CodeSearchChunkV1,
) -> Result<(RetrievalAnchorId, LogicalEvidenceId, SourceOccurrenceId), RetrievalPortError> {
    let chunk_id = chunk.id.as_str();
    let evidence_id = chunk.anchor.symbol_occurrence_id.as_ref().map_or_else(
        || format!("code-chunk:{chunk_id}"),
        |symbol| format!("code-symbol:{}", symbol.as_str()),
    );
    Ok((
        RetrievalAnchorId::new(evidence_id.clone())
            .map_err(|error| RetrievalPortError::Contract(error.to_string()))?,
        LogicalEvidenceId::new(evidence_id)
            .map_err(|error| RetrievalPortError::Contract(error.to_string()))?,
        SourceOccurrenceId::new(format!("code-chunk:{chunk_id}"))
            .map_err(|error| RetrievalPortError::Contract(error.to_string()))?,
    ))
}
