//! Canonical contracts for immutable dense vector generations.

use std::{collections::BTreeMap, fmt, sync::Arc};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use tracedecay_domain::{
    AdmittedEmbeddingProjectionKeyV1, ChangedCodeChunkSetV1, ChangedCodeChunkV1, ChunkerRevision,
    CodeChunkProjectionReceiptV1, CodeGenerationId, CodeSearchChunkId, ContentDigest,
    EmbeddingDeviceClassV1, EmbeddingDocumentCompositionV1, EmbeddingExecutionProviderV1,
    EmbeddingMetricV1, EmbeddingNormalizationV1, EmbeddingPoolingV1, EmbeddingPrecisionV1,
    EmbeddingProjectionKeyV1, EmbeddingTruncationSideV1, ManifestDigest, PrivacyDomainId,
    ProjectionBatchReceiptV1, ProjectionBatchRequestV1, ProjectionKeyV1, ProjectionOperationV1,
    ProjectionOutcomeV1, ProjectionReplayReasonV1, VectorGenerationIdV1,
};
use tracedecay_domain::{DomainError, canonical_sha256, semantic_vector_output_digest};

pub const VECTOR_AUTHORITY_SCHEMA_V1: &str = "tracedecay.vector-authority.v1";
pub const VECTOR_GENERATION_PLAN_DIGEST_DOMAIN_V1: &str =
    "tracedecay.vector-generation-manifest.v1";
pub const VECTOR_GENERATION_BUILD_DIGEST_DOMAIN_V1: &str = "tracedecay.vector-generation-build.v1";
pub const VECTOR_BATCH_DIGEST_DOMAIN_V1: &str = "tracedecay.vector-committed-batch.v1";
pub const VECTOR_SNAPSHOT_DIGEST_DOMAIN_V1: &str = "tracedecay.vector-authority-snapshot.v1";

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum VectorAuthorityError {
    #[error("invalid vector-generation plan: {0}")]
    InvalidPlan(String),
    #[error("unknown vector-generation build")]
    UnknownBuild,
    #[error("unknown published vector generation")]
    UnknownGeneration,
    #[error("the supplied staging checkpoint is stale")]
    StaleCheckpoint,
    #[error("the active-generation compare-and-swap expected {expected:?}, found {actual:?}")]
    ActivePointerMismatch {
        expected: Option<String>,
        actual: Option<String>,
    },
    #[error("the rollback pointer is empty")]
    NoRollbackGeneration,
    #[error("projection batch is incompatible with its generation plan: {0}")]
    BatchIdentityMismatch(String),
    #[error("projection batch replay has conflicting content")]
    ConflictingBatchReplay,
    #[error("chunk {0} appears in more than one committed batch")]
    DuplicateChunkEffect(String),
    #[error("base generation is incompatible: {0}")]
    IncompatibleBaseGeneration(BaseGenerationIncompatibilityV1),
    #[error("reused chunk {0} has no matching immutable base vector")]
    MissingBaseVector(String),
    #[error("vector generation membership is incomplete")]
    IncompleteGeneration,
    #[error("immutable vector generation identity already has different content")]
    ImmutableGenerationConflict,
    #[error("content-addressed vector already has different bytes")]
    ContentAddressConflict,
    #[error("vector authority state is corrupt: {0}")]
    Corrupt(String),
    #[error("vector authority state cannot be serialized: {0}")]
    Serialization(String),
    #[error("vector search context is incompatible: {0}")]
    SearchContextMismatch(String),
    #[error("invalid vector: {0}")]
    InvalidVector(String),
    #[error("canonical domain contract violation: {0}")]
    Domain(String),
}

impl From<DomainError> for VectorAuthorityError {
    fn from(error: DomainError) -> Self {
        Self::Domain(error.to_string())
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum BaseGenerationIncompatibilityV1 {
    #[error("missing published base")]
    MissingPublished,
    #[error("base projection/model identity differs")]
    ProjectionMismatch,
    #[error("base source generation differs from the change watermark")]
    IdentityMismatch,
    #[error("base privacy identity differs")]
    PrivacyMismatch,
}

pub(crate) fn digest_json<T: Serialize>(
    domain: &str,
    value: &T,
) -> Result<ManifestDigest, VectorAuthorityError> {
    canonical_sha256(&(domain, value)).map_err(Into::into)
}

pub(crate) fn digest_bytes(
    domain: &str,
    bytes: &[u8],
) -> Result<ManifestDigest, VectorAuthorityError> {
    canonical_sha256(&(domain, bytes)).map_err(Into::into)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ProjectedChunkVectorV1 {
    pub projection_key: ProjectionKeyV1,
    pub source_generation: CodeGenerationId,
    pub source_manifest_digest: ManifestDigest,
    pub chunk_id: CodeSearchChunkId,
    pub chunk_digest: ContentDigest,
    pub values: Vec<f32>,
    pub output_digest: ContentDigest,
}

impl ProjectedChunkVectorV1 {
    pub fn new(
        admitted: &AdmittedEmbeddingProjectionKeyV1,
        source_generation: CodeGenerationId,
        source_manifest_digest: ManifestDigest,
        chunk_id: CodeSearchChunkId,
        chunk_digest: ContentDigest,
        values: Vec<f32>,
    ) -> Result<Self, VectorAuthorityError> {
        source_generation.validate()?;
        source_manifest_digest.validate()?;
        chunk_id.validate()?;
        chunk_digest.validate()?;
        validate_values(&values, admitted.embedding_key().dimensions as usize)?;
        let output_digest = semantic_vector_output_digest(
            admitted.projection_key(),
            &chunk_id,
            &chunk_digest,
            &values,
        )?;
        Ok(Self {
            projection_key: admitted.projection_key().clone(),
            source_generation,
            source_manifest_digest,
            chunk_id,
            chunk_digest,
            values,
            output_digest,
        })
    }

    pub fn validate(
        &self,
        admitted: &AdmittedEmbeddingProjectionKeyV1,
    ) -> Result<(), VectorAuthorityError> {
        self.source_generation.validate()?;
        self.source_manifest_digest.validate()?;
        self.chunk_id.validate()?;
        self.chunk_digest.validate()?;
        if self.projection_key != *admitted.projection_key() {
            return Err(VectorAuthorityError::BatchIdentityMismatch(
                "vector projection key differs from admitted model".to_owned(),
            ));
        }
        validate_values(&self.values, admitted.embedding_key().dimensions as usize)?;
        if semantic_vector_output_digest(
            &self.projection_key,
            &self.chunk_id,
            &self.chunk_digest,
            &self.values,
        )? != self.output_digest
        {
            return Err(VectorAuthorityError::BatchIdentityMismatch(format!(
                "vector output digest mismatch for {}",
                self.chunk_id
            )));
        }
        Ok(())
    }
}

pub fn validate_values(values: &[f32], dimensions: usize) -> Result<(), VectorAuthorityError> {
    if values.len() != dimensions {
        return Err(VectorAuthorityError::InvalidVector(format!(
            "dimension {} does not equal admitted dimension {dimensions}",
            values.len()
        )));
    }
    if !values.iter().all(|value| value.is_finite()) {
        return Err(VectorAuthorityError::InvalidVector(
            "vector contains a non-finite value".to_owned(),
        ));
    }
    Ok(())
}

pub fn vector_output_digest(
    projection_key: &ProjectionKeyV1,
    chunk_id: &CodeSearchChunkId,
    chunk_digest: &ContentDigest,
    values: &[f32],
) -> Result<ContentDigest, VectorAuthorityError> {
    semantic_vector_output_digest(projection_key, chunk_id, chunk_digest, values)
        .map_err(Into::into)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct VectorTombstoneV1 {
    pub chunk_id: CodeSearchChunkId,
    pub prior_chunk_digest: ContentDigest,
}

impl VectorTombstoneV1 {
    pub fn validate(&self) -> Result<(), VectorAuthorityError> {
        self.chunk_id.validate()?;
        self.prior_chunk_digest.validate()?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PreparedVectorGenerationV1 {
    pub embedding_key: AdmittedEmbeddingProjectionKeyV1,
    pub request: ProjectionBatchRequestV1,
    pub receipt: ProjectionBatchReceiptV1,
    pub vectors: Vec<ProjectedChunkVectorV1>,
    pub tombstones: Vec<VectorTombstoneV1>,
}

impl PreparedVectorGenerationV1 {
    pub fn prepared_digest(&self) -> Result<ManifestDigest, VectorAuthorityError> {
        digest_json(VECTOR_BATCH_DIGEST_DOMAIN_V1, self)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct VectorGenerationPlanV1 {
    pub target_projection_key: ProjectionKeyV1,
    pub source_generation: CodeGenerationId,
    pub source_manifest_digest: ManifestDigest,
    pub expected_chunk_ids: Vec<CodeSearchChunkId>,
    pub base_generation: Option<VectorGenerationIdV1>,
}

impl VectorGenerationPlanV1 {
    pub fn new(
        admitted: &AdmittedEmbeddingProjectionKeyV1,
        source_generation: CodeGenerationId,
        source_manifest_digest: ManifestDigest,
        expected_chunk_ids: Vec<CodeSearchChunkId>,
        base_generation: Option<VectorGenerationIdV1>,
    ) -> Result<Self, VectorAuthorityError> {
        let plan = Self {
            target_projection_key: admitted.projection_key().clone(),
            source_generation,
            source_manifest_digest,
            expected_chunk_ids,
            base_generation,
        };
        plan.validate()?;
        Ok(plan)
    }

    pub fn validate(&self) -> Result<(), VectorAuthorityError> {
        self.source_generation.validate()?;
        self.source_manifest_digest.validate()?;
        if self
            .expected_chunk_ids
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(VectorAuthorityError::InvalidPlan(
                "expected chunk membership must be sorted and unique".to_owned(),
            ));
        }
        for chunk_id in &self.expected_chunk_ids {
            chunk_id.validate()?;
        }
        if let Some(base) = &self.base_generation {
            base.validate()?;
        }
        Ok(())
    }

    pub fn identity_digest(&self) -> Result<ManifestDigest, VectorAuthorityError> {
        self.validate()?;
        digest_json(
            VECTOR_GENERATION_PLAN_DIGEST_DOMAIN_V1,
            &(
                &self.target_projection_key,
                &self.source_generation,
                &self.source_manifest_digest,
                &self.expected_chunk_ids,
            ),
        )
    }

    pub fn generation_id(&self) -> Result<VectorGenerationIdV1, VectorAuthorityError> {
        Ok(VectorGenerationIdV1::new(self.identity_digest()?))
    }

    pub fn build_id(&self) -> Result<VectorGenerationBuildIdV1, VectorAuthorityError> {
        Ok(VectorGenerationBuildIdV1::new(digest_json(
            VECTOR_GENERATION_BUILD_DIGEST_DOMAIN_V1,
            self,
        )?))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct VectorGenerationBuildIdV1(ManifestDigest);

impl VectorGenerationBuildIdV1 {
    pub fn new(digest: ManifestDigest) -> Self {
        Self(digest)
    }
    pub fn as_digest(&self) -> &ManifestDigest {
        &self.0
    }
    pub fn validate(&self) -> Result<(), VectorAuthorityError> {
        self.0.validate().map_err(Into::into)
    }
}

impl fmt::Display for VectorGenerationBuildIdV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct VectorProjectionCheckpointV1 {
    pub target_projection_key: ProjectionKeyV1,
    pub source_generation: CodeGenerationId,
    pub source_manifest_digest: ManifestDigest,
    pub completed_batches: u64,
    pub last_request_digest: Option<ManifestDigest>,
    pub last_publication_digest: Option<ManifestDigest>,
}

impl VectorProjectionCheckpointV1 {
    pub(crate) fn for_plan(plan: &VectorGenerationPlanV1) -> Self {
        Self {
            target_projection_key: plan.target_projection_key.clone(),
            source_generation: plan.source_generation.clone(),
            source_manifest_digest: plan.source_manifest_digest.clone(),
            completed_batches: 0,
            last_request_digest: None,
            last_publication_digest: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PublishedVectorRowV1 {
    pub projection_key: ProjectionKeyV1,
    pub source_generation: CodeGenerationId,
    pub source_manifest_digest: ManifestDigest,
    pub chunk_id: CodeSearchChunkId,
    pub chunk_digest: ContentDigest,
    pub output_digest: ContentDigest,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct VectorGenerationPublicationV1 {
    pub generation_id: VectorGenerationIdV1,
    pub manifest_digest: ManifestDigest,
    pub checkpoint: VectorProjectionCheckpointV1,
    pub previous_active_generation: Option<VectorGenerationIdV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PublishedVectorGenerationV1 {
    pub(crate) generation_id: VectorGenerationIdV1,
    pub(crate) plan: VectorGenerationPlanV1,
    pub(crate) embedding_key: AdmittedEmbeddingProjectionKeyV1,
    pub(crate) rows: BTreeMap<CodeSearchChunkId, PublishedVectorRowV1>,
    pub(crate) tombstone_digests: BTreeMap<CodeSearchChunkId, ContentDigest>,
    pub(crate) receipts: Vec<ProjectionBatchReceiptV1>,
    pub(crate) reused_digests: Vec<ManifestDigest>,
    pub(crate) checkpoint: VectorProjectionCheckpointV1,
    pub(crate) manifest_digest: ManifestDigest,
}

impl PublishedVectorGenerationV1 {
    pub fn generation_id(&self) -> &VectorGenerationIdV1 {
        &self.generation_id
    }
    pub fn projection_key(&self) -> &ProjectionKeyV1 {
        &self.plan.target_projection_key
    }
    pub fn source_generation(&self) -> &CodeGenerationId {
        &self.plan.source_generation
    }
    pub fn source_manifest_digest(&self) -> &ManifestDigest {
        &self.plan.source_manifest_digest
    }
    pub fn base_generation(&self) -> Option<&VectorGenerationIdV1> {
        self.plan.base_generation.as_ref()
    }
    pub fn embedding_key(&self) -> &AdmittedEmbeddingProjectionKeyV1 {
        &self.embedding_key
    }
    pub fn rows(&self) -> &BTreeMap<CodeSearchChunkId, PublishedVectorRowV1> {
        &self.rows
    }
    pub fn vectors(&self) -> &BTreeMap<CodeSearchChunkId, PublishedVectorRowV1> {
        &self.rows
    }
    pub fn tombstone_digests(&self) -> &BTreeMap<CodeSearchChunkId, ContentDigest> {
        &self.tombstone_digests
    }
    pub fn tombstones(&self) -> Vec<CodeSearchChunkId> {
        self.tombstone_digests.keys().cloned().collect()
    }
    pub fn receipts(&self) -> &[ProjectionBatchReceiptV1] {
        &self.receipts
    }
    pub fn checkpoint(&self) -> &VectorProjectionCheckpointV1 {
        &self.checkpoint
    }
    pub fn manifest_digest(&self) -> &ManifestDigest {
        &self.manifest_digest
    }
    pub fn expected_chunk_ids(&self) -> &[CodeSearchChunkId] {
        &self.plan.expected_chunk_ids
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SearchCompatibilityV1 {
    pub source_generation: CodeGenerationId,
    pub source_manifest_digest: ManifestDigest,
    pub projection_key: ProjectionKeyV1,
    pub privacy_domain: PrivacyDomainId,
    pub privacy_key_epoch: u64,
}

impl SearchCompatibilityV1 {
    pub fn from_generation(generation: &PublishedVectorGenerationV1) -> Self {
        Self {
            source_generation: generation.plan.source_generation.clone(),
            source_manifest_digest: generation.plan.source_manifest_digest.clone(),
            projection_key: generation.plan.target_projection_key.clone(),
            privacy_domain: generation.embedding_key.privacy_domain().clone(),
            privacy_key_epoch: generation.embedding_key.privacy_key_epoch(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PublishedVectorReadRowV1 {
    chunk_id: CodeSearchChunkId,
    chunk_digest: ContentDigest,
    vector_digest: ContentDigest,
    values: Arc<[f32]>,
}

impl PublishedVectorReadRowV1 {
    pub(crate) fn new(
        chunk_id: CodeSearchChunkId,
        chunk_digest: ContentDigest,
        vector_digest: ContentDigest,
        values: Arc<[f32]>,
    ) -> Self {
        Self {
            chunk_id,
            chunk_digest,
            vector_digest,
            values,
        }
    }
    pub fn chunk_id(&self) -> &CodeSearchChunkId {
        &self.chunk_id
    }
    pub fn chunk_digest(&self) -> &ContentDigest {
        &self.chunk_digest
    }
    pub fn vector_digest(&self) -> &ContentDigest {
        &self.vector_digest
    }
    pub fn values(&self) -> &Arc<[f32]> {
        &self.values
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PublishedVectorReadSnapshotV1 {
    generation_id: VectorGenerationIdV1,
    compatibility: SearchCompatibilityV1,
    embedding_key: AdmittedEmbeddingProjectionKeyV1,
    rows: Arc<[PublishedVectorReadRowV1]>,
}

impl PublishedVectorReadSnapshotV1 {
    pub(crate) fn new(
        generation_id: VectorGenerationIdV1,
        compatibility: SearchCompatibilityV1,
        embedding_key: AdmittedEmbeddingProjectionKeyV1,
        rows: Vec<PublishedVectorReadRowV1>,
    ) -> Self {
        Self {
            generation_id,
            compatibility,
            embedding_key,
            rows: rows.into(),
        }
    }
    pub fn generation_id(&self) -> &VectorGenerationIdV1 {
        &self.generation_id
    }
    pub fn compatibility(&self) -> &SearchCompatibilityV1 {
        &self.compatibility
    }
    pub fn embedding_key(&self) -> &AdmittedEmbeddingProjectionKeyV1 {
        &self.embedding_key
    }
    pub fn rows(&self) -> &[PublishedVectorReadRowV1] {
        &self.rows
    }
    pub fn search(
        &self,
        query: &[f32],
        limit: usize,
    ) -> Result<Vec<SearchHitV1>, VectorAuthorityError> {
        validate_values(
            query,
            self.embedding_key.embedding_key().dimensions as usize,
        )?;
        let mut hits = self
            .rows
            .iter()
            .map(|row| {
                Ok(SearchHitV1 {
                    generation_id: self.generation_id.clone(),
                    chunk_id: row.chunk_id.clone(),
                    score: cosine_similarity(query, &row.values)?,
                    vector_digest: row.vector_digest.clone(),
                })
            })
            .collect::<Result<Vec<_>, VectorAuthorityError>>()?;
        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.chunk_id.cmp(&right.chunk_id))
        });
        hits.truncate(limit);
        Ok(hits)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SearchHitV1 {
    pub generation_id: VectorGenerationIdV1,
    pub chunk_id: CodeSearchChunkId,
    pub score: f32,
    pub vector_digest: ContentDigest,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VectorSearchRequestV1 {
    pub generation_id: VectorGenerationIdV1,
    pub query: Vec<f32>,
    pub compatibility: SearchCompatibilityV1,
    pub limit: usize,
}

pub fn cosine_similarity(left: &[f32], right: &[f32]) -> Result<f32, VectorAuthorityError> {
    if left.len() != right.len() {
        return Err(VectorAuthorityError::InvalidVector(
            "cosine vectors have different dimensions".to_owned(),
        ));
    }
    validate_values(left, left.len())?;
    validate_values(right, right.len())?;
    let mut dot = 0.0_f64;
    let mut left_norm = 0.0_f64;
    let mut right_norm = 0.0_f64;
    for (&left, &right) in left.iter().zip(right) {
        let left = f64::from(left);
        let right = f64::from(right);
        dot += left * right;
        left_norm += left * left;
        right_norm += right * right;
    }
    if left_norm == 0.0 || right_norm == 0.0 {
        return Err(VectorAuthorityError::InvalidVector(
            "cosine vectors must have non-zero norm".to_owned(),
        ));
    }
    Ok((dot / (left_norm.sqrt() * right_norm.sqrt())) as f32)
}
