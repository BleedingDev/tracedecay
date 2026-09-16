//! The immutable generation state machine and its exact-flat read path.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::types::{
    AdmittedEmbeddingProjectionKeyV1, BaseGenerationIncompatibilityV1,
    CodeChunkProjectionReceiptV1, CodeGenerationId, CodeSearchChunkId, ContentDigest,
    ManifestDigest, PreparedVectorGenerationV1, ProjectedChunkVectorV1, ProjectionBatchReceiptV1,
    ProjectionOperationV1, ProjectionOutcomeV1, ProjectionReplayReasonV1,
    PublishedVectorGenerationV1, PublishedVectorRowV1, SearchCompatibilityV1, SearchHitV1,
    VECTOR_AUTHORITY_SCHEMA_V1, VECTOR_SNAPSHOT_DIGEST_DOMAIN_V1, VectorAuthorityError,
    VectorGenerationBuildIdV1, VectorGenerationIdV1, VectorGenerationPlanV1,
    VectorGenerationPublicationV1, VectorProjectionCheckpointV1, VectorSearchRequestV1,
    VectorTombstoneV1, cosine_similarity, digest_bytes, validate_values, vector_output_digest,
};

/// Whether a staging driver keeps a second copy of prepared values.  The
/// authority itself always stores values once in its content-addressed pool;
/// the enum is retained for compatibility with the historical in-memory and
/// graph drivers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StagedVectorValueRetentionV1 {
    #[default]
    Retained,
    Elided,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct StagedVectorRowV1 {
    projection_key: crate::types::ProjectionKeyV1,
    source_generation: CodeGenerationId,
    source_manifest_digest: ManifestDigest,
    chunk_id: CodeSearchChunkId,
    chunk_digest: ContentDigest,
    output_digest: ContentDigest,
}

impl StagedVectorRowV1 {
    fn as_published(&self) -> PublishedVectorRowV1 {
        PublishedVectorRowV1 {
            projection_key: self.projection_key.clone(),
            source_generation: self.source_generation.clone(),
            source_manifest_digest: self.source_manifest_digest.clone(),
            chunk_id: self.chunk_id.clone(),
            chunk_digest: self.chunk_digest.clone(),
            output_digest: self.output_digest.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct CommittedBatchV1 {
    request_digest: ManifestDigest,
    prepared_digest: ManifestDigest,
    receipt: ProjectionBatchReceiptV1,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct StagedVectorGenerationV1 {
    plan: VectorGenerationPlanV1,
    embedding_key: Option<AdmittedEmbeddingProjectionKeyV1>,
    rows: BTreeMap<CodeSearchChunkId, StagedVectorRowV1>,
    tombstone_digests: BTreeMap<CodeSearchChunkId, ContentDigest>,
    committed_batches: Vec<CommittedBatchV1>,
    committed_chunk_effects: BTreeSet<CodeSearchChunkId>,
    checkpoint: VectorProjectionCheckpointV1,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct VectorBlobV1 {
    values: Vec<f32>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct AuthorityStateV1 {
    staged: BTreeMap<VectorGenerationBuildIdV1, StagedVectorGenerationV1>,
    published: BTreeMap<VectorGenerationIdV1, PublishedVectorGenerationV1>,
    vector_pool: BTreeMap<ContentDigest, VectorBlobV1>,
    active_generation: Option<VectorGenerationIdV1>,
    rollback_generation: Option<VectorGenerationIdV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SnapshotEnvelopeV1 {
    schema: String,
    payload: Vec<u8>,
    checksum: ManifestDigest,
}

/// A validated batch decision. Validation is read-only; the persistent driver
/// can write this value durably and call [`VectorGenerationAuthority::apply_batch`]
/// afterwards without re-running inference or copying accumulated rows.
#[derive(Clone, Debug)]
pub struct PreparedBatchCommitV1 {
    embedding_key: AdmittedEmbeddingProjectionKeyV1,
    checkpoint: VectorProjectionCheckpointV1,
    receipt: ProjectionBatchReceiptV1,
    prepared_digest: ManifestDigest,
    effects: Vec<StagedEffectV1>,
    pool_additions: Vec<(ContentDigest, Vec<f32>)>,
    row_count_after: u64,
    tombstone_count_after: u64,
    receipt_count_after: u64,
}

impl PreparedBatchCommitV1 {
    pub fn checkpoint(&self) -> &VectorProjectionCheckpointV1 {
        &self.checkpoint
    }

    pub fn receipt(&self) -> &ProjectionBatchReceiptV1 {
        &self.receipt
    }

    pub fn prepared_digest(&self) -> &ManifestDigest {
        &self.prepared_digest
    }

    pub fn batch_ordinal(&self) -> u64 {
        self.receipt_count_after.saturating_sub(1)
    }

    pub fn row_count_after(&self) -> u64 {
        self.row_count_after
    }

    pub fn tombstone_count_after(&self) -> u64 {
        self.tombstone_count_after
    }

    pub fn receipt_count_after(&self) -> u64 {
        self.receipt_count_after
    }
}

#[derive(Clone, Debug)]
enum StagedEffectV1 {
    Vector(StagedVectorRowV1),
    Tombstone {
        chunk_id: CodeSearchChunkId,
        prior_chunk_digest: ContentDigest,
    },
}

/// The two idempotency outcomes of batch admission.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum BatchCommitDecisionV1 {
    Replay(VectorProjectionCheckpointV1),
    Commit(PreparedBatchCommitV1),
}

/// In-memory reference authority for immutable vector generations.
///
/// All writes are serialized through this type. A staged build is invisible to
/// search until publication validates its complete expected membership. The
/// float pool is keyed by the output digest, so reused source content shares
/// bytes while each logical generation still retains its own row identity.
#[derive(Clone, Debug, Default)]
pub struct VectorGenerationAuthority {
    state: AuthorityStateV1,
    staged_value_retention: StagedVectorValueRetentionV1,
}

pub type VectorGenerationStateMachineV1 = VectorGenerationAuthority;
pub type FakeVectorGenerationStoreV1 = VectorGenerationAuthority;
pub type VectorGenerationStoreErrorV1 = VectorAuthorityError;

impl VectorGenerationAuthority {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_staged_value_retention(retention: StagedVectorValueRetentionV1) -> Self {
        Self {
            state: AuthorityStateV1::default(),
            staged_value_retention: retention,
        }
    }

    pub fn staged_value_retention(&self) -> StagedVectorValueRetentionV1 {
        self.staged_value_retention
    }

    /// Start or resume the deterministic build identified by `plan`.
    pub fn begin_generation(
        &mut self,
        plan: VectorGenerationPlanV1,
    ) -> Result<VectorGenerationBuildIdV1, VectorAuthorityError> {
        plan.validate()?;
        let build_id = plan.build_id()?;
        if let Some(existing) = self.state.staged.get(&build_id) {
            if existing.plan == plan {
                return Ok(build_id);
            }
            return Err(VectorAuthorityError::InvalidPlan(
                "vector-generation build identity collision".to_owned(),
            ));
        }
        if let Some(base_id) = &plan.base_generation {
            self.state.published.get(base_id).ok_or(
                VectorAuthorityError::IncompatibleBaseGeneration(
                    BaseGenerationIncompatibilityV1::MissingPublished,
                ),
            )?;
        }
        self.state.staged.insert(
            build_id.clone(),
            StagedVectorGenerationV1 {
                checkpoint: VectorProjectionCheckpointV1::for_plan(&plan),
                plan,
                embedding_key: None,
                rows: BTreeMap::new(),
                tombstone_digests: BTreeMap::new(),
                committed_batches: Vec::new(),
                committed_chunk_effects: BTreeSet::new(),
            },
        );
        Ok(build_id)
    }

    /// Drop a staged attempt for the same deterministic build identity while
    /// leaving every previously published generation untouched.
    pub fn rebuild_generation(
        &mut self,
        plan: VectorGenerationPlanV1,
    ) -> Result<VectorGenerationBuildIdV1, VectorAuthorityError> {
        plan.validate()?;
        let build_id = plan.build_id()?;
        self.state.staged.remove(&build_id);
        self.begin_generation(plan)
    }

    pub fn cancel_generation(&mut self, build_id: &VectorGenerationBuildIdV1) -> bool {
        self.state.staged.remove(build_id).is_some()
    }

    pub fn staged_checkpoint(
        &self,
        build_id: &VectorGenerationBuildIdV1,
    ) -> Option<&VectorProjectionCheckpointV1> {
        self.state
            .staged
            .get(build_id)
            .map(|staged| &staged.checkpoint)
    }

    pub fn generation(
        &self,
        generation_id: &VectorGenerationIdV1,
    ) -> Option<&PublishedVectorGenerationV1> {
        self.state.published.get(generation_id)
    }

    pub fn generation_ids(&self) -> impl Iterator<Item = &VectorGenerationIdV1> {
        self.state.published.keys()
    }

    pub fn active_generation(&self) -> Option<&VectorGenerationIdV1> {
        self.state.active_generation.as_ref()
    }

    pub fn rollback_generation(&self) -> Option<&VectorGenerationIdV1> {
        self.state.rollback_generation.as_ref()
    }

    pub fn active_pointer(&self) -> Option<&VectorGenerationIdV1> {
        self.active_generation()
    }

    pub fn previous_generation(&self) -> Option<&VectorGenerationIdV1> {
        self.rollback_generation()
    }

    pub fn validate_batch(
        &self,
        build_id: &VectorGenerationBuildIdV1,
        expected_checkpoint: Option<&VectorProjectionCheckpointV1>,
        prepared: &PreparedVectorGenerationV1,
    ) -> Result<BatchCommitDecisionV1, VectorAuthorityError> {
        let staged = self
            .state
            .staged
            .get(build_id)
            .ok_or(VectorAuthorityError::UnknownBuild)?;
        let prepared_digest = prepared.prepared_digest()?;

        if let Some(existing) = staged
            .committed_batches
            .iter()
            .find(|batch| batch.request_digest == prepared.request.request_digest)
        {
            if existing.prepared_digest == prepared_digest {
                return Ok(BatchCommitDecisionV1::Replay(staged.checkpoint.clone()));
            }
            return Err(VectorAuthorityError::ConflictingBatchReplay);
        }
        if staged.checkpoint.completed_batches == 0 {
            if expected_checkpoint.is_some() {
                return Err(VectorAuthorityError::StaleCheckpoint);
            }
        } else if expected_checkpoint != Some(&staged.checkpoint) {
            return Err(VectorAuthorityError::StaleCheckpoint);
        }

        validate_batch_identity(&staged.plan, prepared)?;
        let embedding_key = &prepared.embedding_key;
        embedding_key.embedding_key().validate()?;
        if embedding_key.projection_key() != &staged.plan.target_projection_key {
            return Err(VectorAuthorityError::BatchIdentityMismatch(
                "admitted embedding key does not match the plan projection".to_owned(),
            ));
        }
        if let Some(existing) = &staged.embedding_key
            && existing != embedding_key
        {
            return Err(VectorAuthorityError::BatchIdentityMismatch(
                "one staged build contains multiple model/privacy identities".to_owned(),
            ));
        }
        if prepared.request.target_projection_key != *embedding_key.projection_key() {
            return Err(VectorAuthorityError::BatchIdentityMismatch(
                "request target projection differs from admitted model".to_owned(),
            ));
        }
        if prepared.request.request_digest != prepared.request.compute_digest()? {
            return Err(VectorAuthorityError::BatchIdentityMismatch(
                "projection request digest mismatch".to_owned(),
            ));
        }
        prepared.receipt.validate_digest()?;
        validate_receipt_identity(&staged.plan, prepared)?;

        let vectors_by_chunk = unique_vectors(&prepared.vectors)?;
        let tombstones_by_chunk = unique_tombstones(&prepared.tombstones)?;
        let base = self.base_for_batch(staged, prepared)?;
        let mut batch_chunks = BTreeSet::new();
        let mut effects = Vec::with_capacity(prepared.receipt.receipts.len());
        let mut pool_additions = Vec::new();
        let mut vector_effects = 0_u64;
        let mut tombstone_effects = 0_u64;

        for receipt in &prepared.receipt.receipts {
            if !batch_chunks.insert(receipt.chunk_id.clone())
                || staged.committed_chunk_effects.contains(&receipt.chunk_id)
            {
                return Err(VectorAuthorityError::DuplicateChunkEffect(
                    receipt.chunk_id.to_string(),
                ));
            }
            let expected_change = change_for_receipt(&prepared.request.changes, receipt)?;
            if expected_change.chunk_id != receipt.chunk_id {
                return Err(VectorAuthorityError::BatchIdentityMismatch(
                    "receipt chunk differs from source change".to_owned(),
                ));
            }
            match receipt.operation {
                ProjectionOperationV1::Added | ProjectionOperationV1::Updated => {
                    let vector = vectors_by_chunk.get(&receipt.chunk_id).ok_or_else(|| {
                        VectorAuthorityError::BatchIdentityMismatch(format!(
                            "missing vector for {}",
                            receipt.chunk_id
                        ))
                    })?;
                    validate_prepared_vector(
                        vector,
                        embedding_key,
                        &staged.plan,
                        &prepared.request.changes.manifest_digest,
                    )?;
                    if receipt.outcome != ProjectionOutcomeV1::Applied
                        || receipt.output_digest.as_ref() != Some(&vector.output_digest)
                        || receipt.current_chunk_digest.as_ref() != Some(&vector.chunk_digest)
                    {
                        return Err(VectorAuthorityError::BatchIdentityMismatch(
                            "add/update receipt does not describe its vector".to_owned(),
                        ));
                    }
                    if receipt.operation == ProjectionOperationV1::Added {
                        if receipt.prior_chunk_digest.is_some()
                            || expected_change.prior_digest.is_some()
                            || receipt.current_chunk_digest.as_ref()
                                != expected_change.current_digest.as_ref()
                            || expected_change.current_digest.as_ref() != Some(&vector.chunk_digest)
                        {
                            return Err(VectorAuthorityError::BatchIdentityMismatch(
                                "add receipt has prior content".to_owned(),
                            ));
                        }
                    } else {
                        if expected_change.prior_digest.is_none()
                            || receipt.prior_chunk_digest != expected_change.prior_digest
                            || receipt.current_chunk_digest.as_ref()
                                != expected_change.current_digest.as_ref()
                            || expected_change.current_digest.as_ref() != Some(&vector.chunk_digest)
                        {
                            return Err(VectorAuthorityError::BatchIdentityMismatch(
                                "update receipt content differs from the source change".to_owned(),
                            ));
                        }
                        if let Some(base) = base {
                            let prior = base.rows.get(&receipt.chunk_id).ok_or_else(|| {
                                VectorAuthorityError::MissingBaseVector(
                                    receipt.chunk_id.to_string(),
                                )
                            })?;
                            if receipt.prior_chunk_digest.as_ref() != Some(&prior.chunk_digest) {
                                return Err(VectorAuthorityError::MissingBaseVector(
                                    receipt.chunk_id.to_string(),
                                ));
                            }
                        }
                    }
                    ensure_pool_compatible(
                        &self.state.vector_pool,
                        &mut pool_additions,
                        &vector.output_digest,
                        &vector.values,
                    )?;
                    effects.push(StagedEffectV1::Vector(StagedVectorRowV1 {
                        projection_key: vector.projection_key.clone(),
                        source_generation: staged.plan.source_generation.clone(),
                        source_manifest_digest: staged.plan.source_manifest_digest.clone(),
                        chunk_id: vector.chunk_id.clone(),
                        chunk_digest: vector.chunk_digest.clone(),
                        output_digest: vector.output_digest.clone(),
                    }));
                    vector_effects = vector_effects.checked_add(1).ok_or_else(|| {
                        VectorAuthorityError::Corrupt("vector row census overflow".to_owned())
                    })?;
                }
                ProjectionOperationV1::Reused => {
                    let base = base.ok_or({
                        VectorAuthorityError::IncompatibleBaseGeneration(
                            BaseGenerationIncompatibilityV1::MissingPublished,
                        )
                    })?;
                    if base.projection_key() != &staged.plan.target_projection_key {
                        return Err(VectorAuthorityError::IncompatibleBaseGeneration(
                            BaseGenerationIncompatibilityV1::ProjectionMismatch,
                        ));
                    }
                    let prior = base.rows.get(&receipt.chunk_id).ok_or_else(|| {
                        VectorAuthorityError::MissingBaseVector(receipt.chunk_id.to_string())
                    })?;
                    if expected_change.prior_digest.as_ref() != Some(&prior.chunk_digest)
                        || expected_change.current_digest.as_ref() != Some(&prior.chunk_digest)
                        || receipt.prior_chunk_digest.as_ref() != Some(&prior.chunk_digest)
                        || receipt.current_chunk_digest.as_ref() != Some(&prior.chunk_digest)
                        || receipt.outcome != ProjectionOutcomeV1::Reused
                        || receipt.output_digest.is_some()
                    {
                        return Err(VectorAuthorityError::MissingBaseVector(
                            receipt.chunk_id.to_string(),
                        ));
                    }
                    effects.push(StagedEffectV1::Vector(StagedVectorRowV1 {
                        projection_key: staged.plan.target_projection_key.clone(),
                        source_generation: staged.plan.source_generation.clone(),
                        source_manifest_digest: staged.plan.source_manifest_digest.clone(),
                        chunk_id: prior.chunk_id.clone(),
                        chunk_digest: prior.chunk_digest.clone(),
                        output_digest: prior.output_digest.clone(),
                    }));
                    vector_effects = vector_effects.checked_add(1).ok_or_else(|| {
                        VectorAuthorityError::Corrupt("vector row census overflow".to_owned())
                    })?;
                }
                ProjectionOperationV1::Deleted => {
                    let tombstone =
                        tombstones_by_chunk.get(&receipt.chunk_id).ok_or_else(|| {
                            VectorAuthorityError::BatchIdentityMismatch(format!(
                                "missing tombstone for {}",
                                receipt.chunk_id
                            ))
                        })?;
                    let base = base.ok_or({
                        VectorAuthorityError::IncompatibleBaseGeneration(
                            BaseGenerationIncompatibilityV1::MissingPublished,
                        )
                    })?;
                    let prior = base.rows.get(&receipt.chunk_id).ok_or_else(|| {
                        VectorAuthorityError::MissingBaseVector(receipt.chunk_id.to_string())
                    })?;
                    if tombstone.prior_chunk_digest != prior.chunk_digest
                        || expected_change.prior_digest.as_ref() != Some(&prior.chunk_digest)
                        || receipt.prior_chunk_digest.as_ref() != Some(&prior.chunk_digest)
                        || receipt.current_chunk_digest.is_some()
                        || receipt.output_digest.is_some()
                        || !matches!(
                            receipt.outcome,
                            ProjectionOutcomeV1::Applied | ProjectionOutcomeV1::Tombstoned
                        )
                    {
                        return Err(VectorAuthorityError::MissingBaseVector(
                            receipt.chunk_id.to_string(),
                        ));
                    }
                    effects.push(StagedEffectV1::Tombstone {
                        chunk_id: receipt.chunk_id.clone(),
                        prior_chunk_digest: prior.chunk_digest.clone(),
                    });
                    tombstone_effects = tombstone_effects.checked_add(1).ok_or_else(|| {
                        VectorAuthorityError::Corrupt("tombstone census overflow".to_owned())
                    })?;
                }
            }
        }
        let vector_receipts = prepared
            .receipt
            .receipts
            .iter()
            .filter(|receipt| receipt.operation.produces_vector())
            .count();
        let tombstone_receipts = prepared
            .receipt
            .receipts
            .iter()
            .filter(|receipt| receipt.operation == ProjectionOperationV1::Deleted)
            .count();
        if vectors_by_chunk.len() != vector_receipts
            || tombstones_by_chunk.len() != tombstone_receipts
        {
            return Err(VectorAuthorityError::BatchIdentityMismatch(
                "prepared rows and receipt operations have different cardinality".to_owned(),
            ));
        }
        let reused_count = prepared
            .receipt
            .receipts
            .iter()
            .filter(|receipt| receipt.operation == ProjectionOperationV1::Reused)
            .count() as u64;
        if prepared.receipt.reused_count != reused_count {
            return Err(VectorAuthorityError::BatchIdentityMismatch(
                "reused receipt count mismatch".to_owned(),
            ));
        }

        let row_count_after = (staged.rows.len() as u64)
            .checked_add(vector_effects)
            .ok_or_else(|| {
                VectorAuthorityError::Corrupt("vector row census overflow".to_owned())
            })?;
        let tombstone_count_after = (staged.tombstone_digests.len() as u64)
            .checked_add(tombstone_effects)
            .ok_or_else(|| VectorAuthorityError::Corrupt("tombstone census overflow".to_owned()))?;
        let receipt_count_after = (staged.committed_batches.len() as u64)
            .checked_add(1)
            .ok_or_else(|| VectorAuthorityError::Corrupt("receipt census overflow".to_owned()))?;
        let mut checkpoint = staged.checkpoint.clone();
        checkpoint.completed_batches = checkpoint
            .completed_batches
            .checked_add(1)
            .ok_or_else(|| VectorAuthorityError::Corrupt("checkpoint overflow".to_owned()))?;
        checkpoint.last_request_digest = Some(prepared.request.request_digest.clone());
        checkpoint.last_publication_digest = Some(prepared.receipt.publication_digest.clone());

        Ok(BatchCommitDecisionV1::Commit(PreparedBatchCommitV1 {
            embedding_key: embedding_key.clone(),
            checkpoint,
            receipt: prepared.receipt.clone(),
            prepared_digest,
            effects,
            pool_additions,
            row_count_after,
            tombstone_count_after,
            receipt_count_after,
        }))
    }

    pub fn commit_batch(
        &mut self,
        build_id: &VectorGenerationBuildIdV1,
        expected_checkpoint: Option<&VectorProjectionCheckpointV1>,
        prepared: PreparedVectorGenerationV1,
    ) -> Result<VectorProjectionCheckpointV1, VectorAuthorityError> {
        match self.validate_batch(build_id, expected_checkpoint, &prepared)? {
            BatchCommitDecisionV1::Replay(checkpoint) => Ok(checkpoint),
            BatchCommitDecisionV1::Commit(decision) => self.apply_batch(build_id, decision),
        }
    }

    pub fn apply_batch(
        &mut self,
        build_id: &VectorGenerationBuildIdV1,
        decision: PreparedBatchCommitV1,
    ) -> Result<VectorProjectionCheckpointV1, VectorAuthorityError> {
        let PreparedBatchCommitV1 {
            embedding_key,
            checkpoint,
            receipt,
            prepared_digest,
            effects,
            pool_additions,
            row_count_after,
            tombstone_count_after,
            receipt_count_after,
        } = decision;

        // Check the checkpoint while the decision is still read-only.  This
        // keeps an accidentally stale apply from mutating the content pool.
        {
            let staged = self
                .state
                .staged
                .get(build_id)
                .ok_or(VectorAuthorityError::UnknownBuild)?;
            if staged.checkpoint.completed_batches.checked_add(1)
                != Some(checkpoint.completed_batches)
                || staged.committed_batches.len() as u64 + 1 != receipt_count_after
            {
                return Err(VectorAuthorityError::StaleCheckpoint);
            }
            if let Some(existing) = &staged.embedding_key
                && existing != &embedding_key
            {
                return Err(VectorAuthorityError::BatchIdentityMismatch(
                    "validated batch model identity differs from staged identity".to_owned(),
                ));
            }
            if checkpoint.target_projection_key != staged.plan.target_projection_key
                || checkpoint.source_generation != staged.plan.source_generation
                || checkpoint.source_manifest_digest != staged.plan.source_manifest_digest
                || checkpoint.last_request_digest != Some(receipt.request_digest.clone())
                || checkpoint.last_publication_digest != Some(receipt.publication_digest.clone())
            {
                return Err(VectorAuthorityError::BatchIdentityMismatch(
                    "validated batch checkpoint differs from the staged plan".to_owned(),
                ));
            }
            let mut effect_ids = BTreeSet::new();
            let mut vector_effects = 0_u64;
            let mut tombstone_effects = 0_u64;
            for effect in &effects {
                match effect {
                    StagedEffectV1::Vector(row) => {
                        if !effect_ids.insert(row.chunk_id.clone())
                            || staged.committed_chunk_effects.contains(&row.chunk_id)
                            || staged.rows.contains_key(&row.chunk_id)
                            || staged.tombstone_digests.contains_key(&row.chunk_id)
                        {
                            return Err(VectorAuthorityError::DuplicateChunkEffect(
                                row.chunk_id.to_string(),
                            ));
                        }
                        if row.projection_key != staged.plan.target_projection_key
                            || row.source_generation != staged.plan.source_generation
                            || row.source_manifest_digest != staged.plan.source_manifest_digest
                        {
                            return Err(VectorAuthorityError::BatchIdentityMismatch(
                                "validated vector row differs from the staged plan".to_owned(),
                            ));
                        }
                        let values = pool_additions
                            .iter()
                            .find(|(digest, _)| digest == &row.output_digest)
                            .map(|(_, values)| values.as_slice())
                            .or_else(|| {
                                self.state
                                    .vector_pool
                                    .get(&row.output_digest)
                                    .map(|blob| blob.values.as_slice())
                            })
                            .ok_or_else(|| {
                                VectorAuthorityError::Corrupt(format!(
                                    "validated vector {} has no content bytes",
                                    row.chunk_id
                                ))
                            })?;
                        if vector_output_digest(
                            &row.projection_key,
                            &row.chunk_id,
                            &row.chunk_digest,
                            values,
                        )? != row.output_digest
                        {
                            return Err(VectorAuthorityError::ContentAddressConflict);
                        }
                        vector_effects = vector_effects.checked_add(1).ok_or_else(|| {
                            VectorAuthorityError::Corrupt("vector row census overflow".to_owned())
                        })?;
                    }
                    StagedEffectV1::Tombstone {
                        chunk_id,
                        prior_chunk_digest: _,
                    } => {
                        if !effect_ids.insert(chunk_id.clone())
                            || staged.committed_chunk_effects.contains(chunk_id)
                            || staged.rows.contains_key(chunk_id)
                            || staged.tombstone_digests.contains_key(chunk_id)
                        {
                            return Err(VectorAuthorityError::DuplicateChunkEffect(
                                chunk_id.to_string(),
                            ));
                        }
                        tombstone_effects = tombstone_effects.checked_add(1).ok_or_else(|| {
                            VectorAuthorityError::Corrupt("tombstone census overflow".to_owned())
                        })?;
                    }
                }
            }
            if staged.rows.len() as u64 + vector_effects != row_count_after
                || staged.tombstone_digests.len() as u64 + tombstone_effects
                    != tombstone_count_after
            {
                return Err(VectorAuthorityError::Corrupt(
                    "validated batch census differs from staged state".to_owned(),
                ));
            }
        }

        // Validate all content-address collisions before inserting any new
        // blob. The authority has no concurrent writer while borrowed mutably,
        // so the second pass is atomic with respect to this operation.
        for (digest, values) in &pool_additions {
            if let Some(existing) = self.state.vector_pool.get(digest)
                && existing.values != *values
            {
                return Err(VectorAuthorityError::ContentAddressConflict);
            }
        }
        for (digest, values) in pool_additions {
            self.state
                .vector_pool
                .entry(digest)
                .or_insert(VectorBlobV1 { values });
        }

        let staged = self
            .state
            .staged
            .get_mut(build_id)
            .ok_or(VectorAuthorityError::UnknownBuild)?;
        if staged.embedding_key.is_none() {
            staged.embedding_key = Some(embedding_key);
        }
        for effect in effects {
            match effect {
                StagedEffectV1::Vector(row) => {
                    staged.committed_chunk_effects.insert(row.chunk_id.clone());
                    staged.tombstone_digests.remove(&row.chunk_id);
                    staged.rows.insert(row.chunk_id.clone(), row);
                }
                StagedEffectV1::Tombstone {
                    chunk_id,
                    prior_chunk_digest,
                } => {
                    staged.committed_chunk_effects.insert(chunk_id.clone());
                    staged.rows.remove(&chunk_id);
                    staged
                        .tombstone_digests
                        .insert(chunk_id, prior_chunk_digest);
                }
            }
        }
        if staged.rows.len() as u64 != row_count_after
            || staged.tombstone_digests.len() as u64 != tombstone_count_after
        {
            return Err(VectorAuthorityError::Corrupt(
                "staged census diverged from validated batch".to_owned(),
            ));
        }
        staged.checkpoint = checkpoint;
        staged.committed_batches.push(CommittedBatchV1 {
            request_digest: receipt.request_digest.clone(),
            prepared_digest,
            receipt,
        });
        Ok(staged.checkpoint.clone())
    }

    /// Validate and publish a complete generation, atomically advancing the
    /// active pointer. `expected_active` is a compare-and-swap fence for
    /// concurrent refreshes; `None` is the expected value for first publish.
    pub fn publish_generation_if_current(
        &mut self,
        build_id: &VectorGenerationBuildIdV1,
        expected_active: Option<&VectorGenerationIdV1>,
    ) -> Result<VectorGenerationPublicationV1, VectorAuthorityError> {
        let actual = self.state.active_generation.as_ref();
        if actual != expected_active {
            return Err(VectorAuthorityError::ActivePointerMismatch {
                expected: expected_active.map(ToString::to_string),
                actual: actual.map(ToString::to_string),
            });
        }
        let staged = self
            .state
            .staged
            .get(build_id)
            .ok_or(VectorAuthorityError::UnknownBuild)?;
        self.validate_staged(staged)?;
        if staged.committed_batches.is_empty()
            || staged.rows.len() != staged.plan.expected_chunk_ids.len()
            || staged
                .plan
                .expected_chunk_ids
                .iter()
                .any(|chunk_id| !staged.rows.contains_key(chunk_id))
        {
            return Err(VectorAuthorityError::IncompleteGeneration);
        }
        let generation_id = staged.plan.generation_id()?;
        let manifest_digest = generation_id.as_digest().clone();
        let generation = PublishedVectorGenerationV1 {
            generation_id: generation_id.clone(),
            plan: staged.plan.clone(),
            embedding_key: staged
                .embedding_key
                .clone()
                .ok_or(VectorAuthorityError::IncompleteGeneration)?,
            rows: staged
                .rows
                .iter()
                .map(|(chunk_id, row)| (chunk_id.clone(), row.as_published()))
                .collect(),
            tombstone_digests: staged.tombstone_digests.clone(),
            receipts: staged
                .committed_batches
                .iter()
                .map(|batch| batch.receipt.clone())
                .collect(),
            checkpoint: staged.checkpoint.clone(),
            manifest_digest: manifest_digest.clone(),
        };
        self.validate_published_generation(&generation)?;
        if let Some(existing) = self.state.published.get(&generation_id) {
            // `base_generation` is lineage evidence, not immutable vector
            // content. The generation identity intentionally omits it, so a
            // deterministic rebuild from another compatible base can replay
            // the same published bytes.
            if existing.embedding_key != generation.embedding_key
                || existing.rows != generation.rows
                || existing.tombstone_digests != generation.tombstone_digests
            {
                return Err(VectorAuthorityError::ImmutableGenerationConflict);
            }
        } else {
            self.state
                .published
                .insert(generation_id.clone(), generation);
        }
        self.state.staged.remove(build_id);
        let previous_active = self.state.active_generation.clone();
        let pointer_changed = previous_active.as_ref() != Some(&generation_id);
        if pointer_changed {
            self.state.rollback_generation = previous_active.clone();
            self.state.active_generation = Some(generation_id.clone());
        }
        let checkpoint = self
            .state
            .published
            .get(&generation_id)
            .ok_or(VectorAuthorityError::Corrupt(
                "published generation disappeared during pointer swap".to_owned(),
            ))?
            .checkpoint()
            .clone();
        Ok(VectorGenerationPublicationV1 {
            generation_id,
            manifest_digest,
            checkpoint,
            previous_active_generation: pointer_changed.then_some(previous_active).flatten(),
        })
    }

    /// Publish against the authority's current active pointer.
    pub fn publish_generation(
        &mut self,
        build_id: &VectorGenerationBuildIdV1,
    ) -> Result<VectorGenerationPublicationV1, VectorAuthorityError> {
        let expected = self.state.active_generation.clone();
        self.publish_generation_if_current(build_id, expected.as_ref())
    }

    pub fn publish_staged(
        &mut self,
        build_id: &VectorGenerationBuildIdV1,
    ) -> Result<VectorGenerationPublicationV1, VectorAuthorityError> {
        self.publish_generation(build_id)
    }

    /// Atomically move the active pointer to an already complete generation.
    pub fn activate_generation_if_current(
        &mut self,
        generation_id: &VectorGenerationIdV1,
        expected_active: Option<&VectorGenerationIdV1>,
    ) -> Result<Option<VectorGenerationIdV1>, VectorAuthorityError> {
        if self.state.active_generation.as_ref() != expected_active {
            return Err(VectorAuthorityError::ActivePointerMismatch {
                expected: expected_active.map(ToString::to_string),
                actual: self
                    .state
                    .active_generation
                    .as_ref()
                    .map(ToString::to_string),
            });
        }
        if !self.state.published.contains_key(generation_id) {
            return Err(VectorAuthorityError::UnknownGeneration);
        }
        if self.state.active_generation.as_ref() == Some(generation_id) {
            return Ok(None);
        }
        let previous = self.state.active_generation.replace(generation_id.clone());
        self.state.rollback_generation = previous.clone();
        Ok(previous)
    }

    pub fn activate_generation(
        &mut self,
        generation_id: &VectorGenerationIdV1,
    ) -> Result<Option<VectorGenerationIdV1>, VectorAuthorityError> {
        let expected = self.state.active_generation.clone();
        self.activate_generation_if_current(generation_id, expected.as_ref())
    }

    /// Swap the active and previous pointers. The old active remains the new
    /// rollback target, so a second rollback safely returns to the prior head.
    pub fn rollback_generation_if_current(
        &mut self,
        expected_active: Option<&VectorGenerationIdV1>,
    ) -> Result<VectorGenerationIdV1, VectorAuthorityError> {
        if self.state.active_generation.as_ref() != expected_active {
            return Err(VectorAuthorityError::ActivePointerMismatch {
                expected: expected_active.map(ToString::to_string),
                actual: self
                    .state
                    .active_generation
                    .as_ref()
                    .map(ToString::to_string),
            });
        }
        let rollback = self
            .state
            .rollback_generation
            .clone()
            .ok_or(VectorAuthorityError::NoRollbackGeneration)?;
        if !self.state.published.contains_key(&rollback) {
            return Err(VectorAuthorityError::Corrupt(
                "rollback pointer names a missing generation".to_owned(),
            ));
        }
        let previous = self.state.active_generation.replace(rollback.clone());
        self.state.rollback_generation = previous;
        Ok(rollback)
    }

    pub fn rollback(&mut self) -> Result<VectorGenerationIdV1, VectorAuthorityError> {
        let expected = self.state.active_generation.clone();
        self.rollback_generation_if_current(expected.as_ref())
    }

    pub fn read_vector(
        &self,
        generation_id: &VectorGenerationIdV1,
        chunk_id: &CodeSearchChunkId,
    ) -> Option<Vec<f32>> {
        let row = self
            .state
            .published
            .get(generation_id)?
            .rows()
            .get(chunk_id)?;
        self.state
            .vector_pool
            .get(&row.output_digest)
            .map(|blob| blob.values.clone())
    }

    pub fn vector_content(&self, digest: &ContentDigest) -> Option<&[f32]> {
        self.state
            .vector_pool
            .get(digest)
            .map(|blob| blob.values.as_slice())
    }

    pub fn vector_pool_len(&self) -> usize {
        self.state.vector_pool.len()
    }

    /// Search one generation with an explicit source/model/privacy fence.
    pub fn search(
        &self,
        request: &VectorSearchRequestV1,
    ) -> Result<Vec<SearchHitV1>, VectorAuthorityError> {
        let generation = self
            .state
            .published
            .get(&request.generation_id)
            .ok_or(VectorAuthorityError::UnknownGeneration)?;
        let expected = SearchCompatibilityV1::from_generation(generation);
        if request.compatibility != expected {
            return Err(VectorAuthorityError::SearchContextMismatch(
                "source, chunk-generation manifest, projection, or privacy epoch differs"
                    .to_owned(),
            ));
        }
        if generation.embedding_key.embedding_key().metric
            != crate::types::EmbeddingMetricV1::Cosine
        {
            return Err(VectorAuthorityError::SearchContextMismatch(
                "exact-flat search currently admits cosine projections only".to_owned(),
            ));
        }
        validate_values(
            &request.query,
            generation.embedding_key().dimensions() as usize,
        )?;
        let mut hits = Vec::with_capacity(generation.rows().len());
        for (chunk_id, row) in generation.rows() {
            let values = self
                .state
                .vector_pool
                .get(&row.output_digest)
                .ok_or_else(|| {
                    VectorAuthorityError::Corrupt(format!("missing vector {}", row.output_digest))
                })?;
            let score = cosine_similarity(&request.query, &values.values)?;
            hits.push(SearchHitV1 {
                generation_id: generation.generation_id().clone(),
                chunk_id: chunk_id.clone(),
                score,
                vector_digest: row.output_digest.clone(),
            });
        }
        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.chunk_id.cmp(&right.chunk_id))
        });
        hits.truncate(request.limit);
        Ok(hits)
    }

    pub fn search_exact_flat(
        &self,
        generation_id: &VectorGenerationIdV1,
        query: Vec<f32>,
        limit: usize,
    ) -> Result<Vec<SearchHitV1>, VectorAuthorityError> {
        let generation = self
            .state
            .published
            .get(generation_id)
            .ok_or(VectorAuthorityError::UnknownGeneration)?;
        self.search(&VectorSearchRequestV1 {
            generation_id: generation_id.clone(),
            query,
            compatibility: SearchCompatibilityV1::from_generation(generation),
            limit,
        })
    }

    pub fn search_active(
        &self,
        query: Vec<f32>,
        limit: usize,
    ) -> Result<Vec<SearchHitV1>, VectorAuthorityError> {
        let generation_id = self
            .state
            .active_generation
            .as_ref()
            .ok_or(VectorAuthorityError::UnknownGeneration)?;
        self.search_exact_flat(generation_id, query, limit)
    }

    /// Serialize staged checkpoints, published generations, active/rollback
    /// pointers, and the content-addressed pool under one checksum envelope.
    /// This is the crash/restart boundary: a reopened authority either has a
    /// complete validated state or returns a typed corruption error.
    pub fn persist_sealed(&self) -> Result<Vec<u8>, VectorAuthorityError> {
        self.validate_state()?;
        let payload = serde_json::to_vec(&self.state)
            .map_err(|error| VectorAuthorityError::Serialization(error.to_string()))?;
        let envelope = SnapshotEnvelopeV1 {
            schema: VECTOR_AUTHORITY_SCHEMA_V1.to_owned(),
            checksum: digest_bytes(VECTOR_SNAPSHOT_DIGEST_DOMAIN_V1, &payload),
            payload,
        };
        serde_json::to_vec(&envelope)
            .map_err(|error| VectorAuthorityError::Serialization(error.to_string()))
    }

    pub fn snapshot(&self) -> Result<Vec<u8>, VectorAuthorityError> {
        self.persist_sealed()
    }

    pub fn reopen_sealed(bytes: &[u8]) -> Result<Self, VectorAuthorityError> {
        let envelope: SnapshotEnvelopeV1 = serde_json::from_slice(bytes).map_err(|error| {
            VectorAuthorityError::Corrupt(format!("invalid snapshot envelope: {error}"))
        })?;
        if envelope.schema != VECTOR_AUTHORITY_SCHEMA_V1 {
            return Err(VectorAuthorityError::Corrupt(
                "unknown vector authority snapshot schema".to_owned(),
            ));
        }
        let expected = digest_bytes(VECTOR_SNAPSHOT_DIGEST_DOMAIN_V1, &envelope.payload);
        if envelope.checksum != expected {
            return Err(VectorAuthorityError::Corrupt(
                "vector authority snapshot checksum mismatch".to_owned(),
            ));
        }
        let state: AuthorityStateV1 =
            serde_json::from_slice(&envelope.payload).map_err(|error| {
                VectorAuthorityError::Corrupt(format!("invalid snapshot state: {error}"))
            })?;
        let authority = Self {
            state,
            staged_value_retention: StagedVectorValueRetentionV1::Retained,
        };
        authority
            .validate_state()
            .map_err(|error| VectorAuthorityError::Corrupt(error.to_string()))?;
        Ok(authority)
    }

    pub fn restore(bytes: &[u8]) -> Result<Self, VectorAuthorityError> {
        Self::reopen_sealed(bytes)
    }

    pub fn validate_state(&self) -> Result<(), VectorAuthorityError> {
        if let (Some(active), Some(rollback)) = (
            self.state.active_generation.as_ref(),
            self.state.rollback_generation.as_ref(),
        ) && active == rollback
        {
            return Err(VectorAuthorityError::Corrupt(
                "active and rollback pointers name the same generation".to_owned(),
            ));
        }
        if self.state.active_generation.is_none() && self.state.rollback_generation.is_some() {
            return Err(VectorAuthorityError::Corrupt(
                "rollback pointer exists without an active generation".to_owned(),
            ));
        }
        for pointer in [
            self.state.active_generation.as_ref(),
            self.state.rollback_generation.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            if !self.state.published.contains_key(pointer) {
                return Err(VectorAuthorityError::Corrupt(
                    "active/rollback pointer names a missing generation".to_owned(),
                ));
            }
        }
        for (build_id, staged) in &self.state.staged {
            if staged.plan.build_id()? != *build_id {
                return Err(VectorAuthorityError::Corrupt(
                    "staged build id does not match its plan".to_owned(),
                ));
            }
            self.validate_staged(staged)?;
        }
        for (generation_id, generation) in &self.state.published {
            if generation.generation_id() != generation_id {
                return Err(VectorAuthorityError::Corrupt(
                    "published map key does not match generation identity".to_owned(),
                ));
            }
            self.validate_published_generation(generation)?;
        }
        for (digest, blob) in &self.state.vector_pool {
            if blob.values.is_empty() {
                return Err(VectorAuthorityError::Corrupt(format!(
                    "content-addressed vector {digest} has no values"
                )));
            }
            validate_values(blob.values.as_slice(), blob.values.len())?;
            if digest.as_str().is_empty() {
                return Err(VectorAuthorityError::Corrupt(
                    "empty content-addressed vector digest".to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn base_for_batch<'a>(
        &'a self,
        staged: &StagedVectorGenerationV1,
        prepared: &PreparedVectorGenerationV1,
    ) -> Result<Option<&'a PublishedVectorGenerationV1>, VectorAuthorityError> {
        let Some(base_id) = &staged.plan.base_generation else {
            if prepared.receipt.receipts.iter().any(|receipt| {
                matches!(
                    receipt.operation,
                    ProjectionOperationV1::Reused | ProjectionOperationV1::Deleted
                ) || receipt.prior_chunk_digest.is_some()
            }) {
                return Err(VectorAuthorityError::IncompatibleBaseGeneration(
                    BaseGenerationIncompatibilityV1::MissingPublished,
                ));
            }
            return Ok(None);
        };
        let base = self.state.published.get(base_id).ok_or(
            VectorAuthorityError::IncompatibleBaseGeneration(
                BaseGenerationIncompatibilityV1::MissingPublished,
            ),
        )?;
        if let Some(from_generation) = &prepared.request.changes.from_generation {
            if from_generation != base.source_generation() {
                return Err(VectorAuthorityError::IncompatibleBaseGeneration(
                    BaseGenerationIncompatibilityV1::IdentityMismatch,
                ));
            }
        } else {
            return Err(VectorAuthorityError::IncompatibleBaseGeneration(
                BaseGenerationIncompatibilityV1::IdentityMismatch,
            ));
        }
        if prepared.request.previous_projection_key.as_ref() != Some(base.projection_key()) {
            return Err(VectorAuthorityError::IncompatibleBaseGeneration(
                BaseGenerationIncompatibilityV1::ProjectionMismatch,
            ));
        }
        if prepared.request.target_projection_key == *base.projection_key()
            && prepared.embedding_key != *base.embedding_key()
        {
            return Err(VectorAuthorityError::IncompatibleBaseGeneration(
                BaseGenerationIncompatibilityV1::PrivacyMismatch,
            ));
        }
        if let Some(existing) = &staged.embedding_key
            && existing != base.embedding_key()
            && prepared.request.target_projection_key == *base.projection_key()
        {
            return Err(VectorAuthorityError::IncompatibleBaseGeneration(
                BaseGenerationIncompatibilityV1::PrivacyMismatch,
            ));
        }
        Ok(Some(base))
    }

    fn validate_staged(
        &self,
        staged: &StagedVectorGenerationV1,
    ) -> Result<(), VectorAuthorityError> {
        staged.plan.validate()?;
        if staged.checkpoint.target_projection_key != staged.plan.target_projection_key
            || staged.checkpoint.source_generation != staged.plan.source_generation
            || staged.checkpoint.source_manifest_digest != staged.plan.source_manifest_digest
            || staged.checkpoint.completed_batches != staged.committed_batches.len() as u64
        {
            return Err(VectorAuthorityError::Corrupt(
                "staged checkpoint does not describe its plan or receipts".to_owned(),
            ));
        }
        if staged
            .rows
            .keys()
            .any(|chunk_id| !staged.plan.expected_chunk_ids.contains(chunk_id))
        {
            return Err(VectorAuthorityError::IncompleteGeneration);
        }
        if staged
            .rows
            .keys()
            .any(|chunk_id| staged.tombstone_digests.contains_key(chunk_id))
        {
            return Err(VectorAuthorityError::Corrupt(
                "staged row and tombstone overlap".to_owned(),
            ));
        }
        if let Some(key) = &staged.embedding_key {
            if key.projection_key() != &staged.plan.target_projection_key {
                return Err(VectorAuthorityError::Corrupt(
                    "staged model identity differs from plan projection".to_owned(),
                ));
            }
            key.embedding_key().validate()?;
            for (map_chunk_id, row) in &staged.rows {
                if map_chunk_id != &row.chunk_id {
                    return Err(VectorAuthorityError::Corrupt(
                        "staged row map key differs from its chunk identity".to_owned(),
                    ));
                }
                validate_staged_row(row, key, &staged.plan)?;
                let blob = self
                    .state
                    .vector_pool
                    .get(&row.output_digest)
                    .ok_or_else(|| {
                        VectorAuthorityError::Corrupt(format!(
                            "staged row {} is missing its content-addressed bytes",
                            row.chunk_id
                        ))
                    })?;
                validate_values(&blob.values, key.dimensions() as usize)?;
                if vector_output_digest(
                    &row.projection_key,
                    &row.chunk_id,
                    &row.chunk_digest,
                    &blob.values,
                )? != row.output_digest
                {
                    return Err(VectorAuthorityError::Corrupt(format!(
                        "content-addressed bytes do not match staged row {}",
                        row.chunk_id
                    )));
                }
            }
        } else if !staged.rows.is_empty() || !staged.tombstone_digests.is_empty() {
            return Err(VectorAuthorityError::Corrupt(
                "staged effects exist without an admitted model identity".to_owned(),
            ));
        }
        let expected_prior_generation = staged
            .plan
            .base_generation
            .as_ref()
            .map(|base_id| {
                self.state
                    .published
                    .get(base_id)
                    .ok_or_else(|| {
                        VectorAuthorityError::Corrupt(
                            "staged plan names a missing base generation".to_owned(),
                        )
                    })
                    .map(|base| base.source_generation())
            })
            .transpose()?;
        validate_receipts_and_effects(
            &staged.plan,
            &staged.committed_batches,
            &staged.rows,
            &staged.tombstone_digests,
            Some(&staged.committed_chunk_effects),
            expected_prior_generation,
        )?;
        if staged.checkpoint.completed_batches > 0 {
            let last = staged
                .committed_batches
                .last()
                .ok_or(VectorAuthorityError::Corrupt(
                    "staged checkpoint names no committed receipt".to_owned(),
                ))?;
            if staged.checkpoint.last_request_digest.as_ref() != Some(&last.request_digest)
                || staged.checkpoint.last_publication_digest.as_ref()
                    != Some(&last.receipt.publication_digest)
            {
                return Err(VectorAuthorityError::Corrupt(
                    "staged checkpoint does not name its last receipt".to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn validate_published_generation(
        &self,
        generation: &PublishedVectorGenerationV1,
    ) -> Result<(), VectorAuthorityError> {
        generation.plan.validate()?;
        if generation.manifest_digest != *generation.generation_id.as_digest()
            || generation.plan.identity_digest()? != generation.manifest_digest
        {
            return Err(VectorAuthorityError::Corrupt(
                "published generation identity does not match its plan".to_owned(),
            ));
        }
        if generation.checkpoint.target_projection_key != generation.plan.target_projection_key
            || generation.checkpoint.source_generation != generation.plan.source_generation
            || generation.checkpoint.source_manifest_digest
                != generation.plan.source_manifest_digest
            || generation.checkpoint.completed_batches != generation.receipts.len() as u64
            || generation.receipts.is_empty()
        {
            return Err(VectorAuthorityError::Corrupt(
                "published checkpoint is incomplete or incompatible".to_owned(),
            ));
        }
        if generation.embedding_key.projection_key() != &generation.plan.target_projection_key {
            return Err(VectorAuthorityError::Corrupt(
                "published model identity differs from plan projection".to_owned(),
            ));
        }
        generation.embedding_key.embedding_key().validate()?;
        if generation.rows.len() != generation.plan.expected_chunk_ids.len()
            || generation
                .plan
                .expected_chunk_ids
                .iter()
                .any(|chunk_id| !generation.rows.contains_key(chunk_id))
            || generation
                .rows
                .keys()
                .any(|chunk_id| generation.tombstone_digests.contains_key(chunk_id))
        {
            return Err(VectorAuthorityError::IncompleteGeneration);
        }
        for (map_chunk_id, row) in &generation.rows {
            if map_chunk_id != &row.chunk_id {
                return Err(VectorAuthorityError::Corrupt(
                    "published row map key differs from its chunk identity".to_owned(),
                ));
            }
            validate_published_row(row, &generation.embedding_key, &generation.plan)?;
            let blob = self
                .state
                .vector_pool
                .get(&row.output_digest)
                .ok_or_else(|| {
                    VectorAuthorityError::Corrupt(format!(
                        "published row {} is missing bytes",
                        row.chunk_id
                    ))
                })?;
            validate_values(&blob.values, generation.embedding_key.dimensions() as usize)?;
            if vector_output_digest(
                &row.projection_key,
                &row.chunk_id,
                &row.chunk_digest,
                &blob.values,
            )? != row.output_digest
            {
                return Err(VectorAuthorityError::Corrupt(format!(
                    "content-addressed bytes do not match row {}",
                    row.chunk_id
                )));
            }
        }
        if generation.checkpoint.last_request_digest
            != generation
                .receipts
                .last()
                .map(|receipt| receipt.request_digest.clone())
            || generation.checkpoint.last_publication_digest
                != generation
                    .receipts
                    .last()
                    .map(|receipt| receipt.publication_digest.clone())
        {
            return Err(VectorAuthorityError::Corrupt(
                "published checkpoint does not name its last receipt".to_owned(),
            ));
        }
        let expected_prior_generation = generation
            .plan
            .base_generation
            .as_ref()
            .map(|base_id| {
                self.state
                    .published
                    .get(base_id)
                    .ok_or_else(|| {
                        VectorAuthorityError::Corrupt(
                            "published plan names a missing base generation".to_owned(),
                        )
                    })
                    .map(|base| base.source_generation())
            })
            .transpose()?;
        validate_receipts_and_effects(
            &generation.plan,
            &generation
                .receipts
                .iter()
                .cloned()
                .map(|receipt| CommittedBatchV1 {
                    request_digest: receipt.request_digest.clone(),
                    prepared_digest: ManifestDigest::zero(),
                    receipt,
                })
                .collect::<Vec<_>>(),
            &generation
                .rows
                .iter()
                .map(|(chunk_id, row)| {
                    (
                        chunk_id.clone(),
                        StagedVectorRowV1 {
                            projection_key: row.projection_key.clone(),
                            source_generation: row.source_generation.clone(),
                            source_manifest_digest: row.source_manifest_digest.clone(),
                            chunk_id: row.chunk_id.clone(),
                            chunk_digest: row.chunk_digest.clone(),
                            output_digest: row.output_digest.clone(),
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
            &generation.tombstone_digests,
            None,
            expected_prior_generation,
        )?;
        Ok(())
    }
}

/// Re-check durable receipt evidence against the row census.  This is kept
/// separate from batch admission because a sealed/reopened authority no
/// longer has the original request payload or prepared float values.
fn validate_receipts_and_effects(
    plan: &VectorGenerationPlanV1,
    batches: &[CommittedBatchV1],
    rows: &BTreeMap<CodeSearchChunkId, StagedVectorRowV1>,
    tombstones: &BTreeMap<CodeSearchChunkId, ContentDigest>,
    expected_effects: Option<&BTreeSet<CodeSearchChunkId>>,
    expected_prior_generation: Option<&CodeGenerationId>,
) -> Result<(), VectorAuthorityError> {
    let mut request_digests = BTreeSet::new();
    let mut effect_ids = BTreeSet::new();
    for batch in batches {
        if batch.request_digest != batch.receipt.request_digest
            || !request_digests.insert(batch.request_digest.clone())
        {
            return Err(VectorAuthorityError::Corrupt(
                "committed batch request identity is duplicate or inconsistent".to_owned(),
            ));
        }
        batch.receipt.validate_digest()?;
        if batch.receipt.target_projection_key != plan.target_projection_key
            || batch.receipt.source_generation != plan.source_generation
        {
            return Err(VectorAuthorityError::Corrupt(
                "committed receipt escaped its generation".to_owned(),
            ));
        }
        if batch
            .receipt
            .receipts
            .windows(2)
            .any(|pair| pair[0].chunk_id >= pair[1].chunk_id)
        {
            return Err(VectorAuthorityError::Corrupt(
                "durable chunk receipts are not in canonical order".to_owned(),
            ));
        }
        batch.receipt.source_manifest_digest.validate()?;
        let expected_reused = batch
            .receipt
            .receipts
            .iter()
            .filter(|receipt| receipt.operation == ProjectionOperationV1::Reused)
            .count() as u64;
        if batch.receipt.reused_count != expected_reused {
            return Err(VectorAuthorityError::Corrupt(
                "committed receipt reused census is inconsistent".to_owned(),
            ));
        }
        for receipt in &batch.receipt.receipts {
            if !effect_ids.insert(receipt.chunk_id.clone()) {
                return Err(VectorAuthorityError::Corrupt(format!(
                    "chunk {} appears in more than one committed receipt",
                    receipt.chunk_id
                )));
            }
            if receipt.source_manifest_digest != batch.receipt.source_manifest_digest
                || receipt.request_digest != batch.receipt.request_digest
                || receipt.prior_generation.as_ref() != expected_prior_generation
            {
                return Err(VectorAuthorityError::Corrupt(
                    "durable chunk receipt differs from its batch watermark".to_owned(),
                ));
            }
            validate_persisted_receipt_row(plan, receipt, rows, tombstones)?;
        }
    }
    if let Some(expected_effects) = expected_effects
        && expected_effects != &effect_ids
    {
        return Err(VectorAuthorityError::Corrupt(
            "committed effect index differs from receipts".to_owned(),
        ));
    }
    if rows.keys().any(|chunk_id| !effect_ids.contains(chunk_id))
        || tombstones
            .keys()
            .any(|chunk_id| !effect_ids.contains(chunk_id))
    {
        return Err(VectorAuthorityError::Corrupt(
            "durable row census contains an unreceipted chunk".to_owned(),
        ));
    }
    Ok(())
}

fn validate_persisted_receipt_row(
    plan: &VectorGenerationPlanV1,
    receipt: &CodeChunkProjectionReceiptV1,
    rows: &BTreeMap<CodeSearchChunkId, StagedVectorRowV1>,
    tombstones: &BTreeMap<CodeSearchChunkId, ContentDigest>,
) -> Result<(), VectorAuthorityError> {
    receipt.chunk_id.validate()?;
    receipt.projection_key.validate()?;
    receipt.source_manifest_digest.validate()?;
    if let Some(digest) = &receipt.prior_chunk_digest {
        digest.validate()?;
    }
    if let Some(digest) = &receipt.current_chunk_digest {
        digest.validate()?;
    }
    if let Some(digest) = &receipt.output_digest {
        digest.validate()?;
    }
    if receipt.projection_key != plan.target_projection_key
        || receipt.source_generation != plan.source_generation
    {
        return Err(VectorAuthorityError::Corrupt(
            "durable chunk receipt escaped its generation".to_owned(),
        ));
    }
    match receipt.operation {
        ProjectionOperationV1::Added | ProjectionOperationV1::Updated => {
            if receipt.outcome != ProjectionOutcomeV1::Applied
                || receipt.current_chunk_digest.is_none()
                || receipt.output_digest.is_none()
                || (receipt.operation == ProjectionOperationV1::Added
                    && receipt.prior_chunk_digest.is_some())
                || (receipt.operation == ProjectionOperationV1::Updated
                    && receipt.prior_chunk_digest.is_none())
            {
                return Err(VectorAuthorityError::Corrupt(
                    "durable add/update receipt has invalid evidence".to_owned(),
                ));
            }
            let row = rows.get(&receipt.chunk_id).ok_or_else(|| {
                VectorAuthorityError::Corrupt(format!(
                    "durable receipt {} has no row",
                    receipt.chunk_id
                ))
            })?;
            if receipt.current_chunk_digest.as_ref() != Some(&row.chunk_digest)
                || receipt.output_digest.as_ref() != Some(&row.output_digest)
            {
                return Err(VectorAuthorityError::Corrupt(format!(
                    "durable receipt {} differs from its row",
                    receipt.chunk_id
                )));
            }
        }
        ProjectionOperationV1::Reused => {
            if receipt.outcome != ProjectionOutcomeV1::Reused
                || receipt.prior_chunk_digest.is_none()
                || receipt.current_chunk_digest != receipt.prior_chunk_digest
                || receipt.output_digest.is_some()
            {
                return Err(VectorAuthorityError::Corrupt(
                    "durable reuse receipt has invalid evidence".to_owned(),
                ));
            }
            let row = rows.get(&receipt.chunk_id).ok_or_else(|| {
                VectorAuthorityError::Corrupt(format!(
                    "durable reuse receipt {} has no row",
                    receipt.chunk_id
                ))
            })?;
            if receipt.current_chunk_digest.as_ref() != Some(&row.chunk_digest) {
                return Err(VectorAuthorityError::Corrupt(format!(
                    "durable reuse receipt {} differs from its row",
                    receipt.chunk_id
                )));
            }
        }
        ProjectionOperationV1::Deleted => {
            if receipt.prior_chunk_digest.is_none()
                || receipt.current_chunk_digest.is_some()
                || receipt.output_digest.is_some()
                || !matches!(
                    receipt.outcome,
                    ProjectionOutcomeV1::Applied | ProjectionOutcomeV1::Tombstoned
                )
            {
                return Err(VectorAuthorityError::Corrupt(
                    "durable delete receipt has invalid evidence".to_owned(),
                ));
            }
            if tombstones.get(&receipt.chunk_id) != receipt.prior_chunk_digest.as_ref() {
                return Err(VectorAuthorityError::Corrupt(format!(
                    "durable delete receipt {} differs from its tombstone",
                    receipt.chunk_id
                )));
            }
        }
    }
    Ok(())
}

fn validate_batch_identity(
    plan: &VectorGenerationPlanV1,
    prepared: &PreparedVectorGenerationV1,
) -> Result<(), VectorAuthorityError> {
    let request = &prepared.request;
    request.changes.validate()?;
    if request.target_projection_key != plan.target_projection_key
        || request.changes.to_generation != plan.source_generation
    {
        return Err(VectorAuthorityError::BatchIdentityMismatch(
            "request target/source identity differs from plan".to_owned(),
        ));
    }
    if request.previous_projection_key.as_ref() != Some(&request.target_projection_key)
        && !request.changes.reused.is_empty()
        && request.replay_reason != ProjectionReplayReasonV1::ProjectionProfileChange
    {
        return Err(VectorAuthorityError::BatchIdentityMismatch(
            "projection-key replay requires explicit re-embedding".to_owned(),
        ));
    }
    Ok(())
}

fn validate_receipt_identity(
    plan: &VectorGenerationPlanV1,
    prepared: &PreparedVectorGenerationV1,
) -> Result<(), VectorAuthorityError> {
    let receipt = &prepared.receipt;
    let request = &prepared.request;
    if receipt.target_projection_key != plan.target_projection_key
        || receipt.request_digest != request.request_digest
        || receipt.source_generation != plan.source_generation
        || receipt.source_manifest_digest != request.changes.manifest_digest
    {
        return Err(VectorAuthorityError::BatchIdentityMismatch(
            "projection receipt escaped its request or source generation".to_owned(),
        ));
    }
    for row in &prepared.vectors {
        if row.source_generation != plan.source_generation {
            return Err(VectorAuthorityError::BatchIdentityMismatch(
                "vector row belongs to a foreign source generation".to_owned(),
            ));
        }
    }
    if receipt
        .receipts
        .windows(2)
        .any(|pair| pair[0].chunk_id >= pair[1].chunk_id)
    {
        return Err(VectorAuthorityError::BatchIdentityMismatch(
            "chunk receipts must be sorted and unique".to_owned(),
        ));
    }
    let mut receipt_ids = BTreeSet::new();
    for row in &receipt.receipts {
        if !receipt_ids.insert(row.chunk_id.clone())
            || row.prior_generation != request.changes.from_generation
        {
            return Err(VectorAuthorityError::BatchIdentityMismatch(
                "chunk receipt has duplicate or foreign source lineage".to_owned(),
            ));
        }
        if row.projection_key != plan.target_projection_key
            || row.request_digest != request.request_digest
            || row.source_generation != plan.source_generation
            || row.source_manifest_digest != receipt.source_manifest_digest
        {
            return Err(VectorAuthorityError::BatchIdentityMismatch(
                "chunk receipt belongs to a foreign projection batch".to_owned(),
            ));
        }
        row.projection_key.validate()?;
        row.source_manifest_digest.validate()?;
        if let Some(digest) = &row.prior_chunk_digest {
            digest.validate()?;
        }
        if let Some(digest) = &row.current_chunk_digest {
            digest.validate()?;
        }
        if let Some(digest) = &row.output_digest {
            digest.validate()?;
        }
    }
    let expected_ids = request
        .changes
        .added_or_changed
        .iter()
        .chain(request.changes.deleted.iter())
        .chain(request.changes.reused.iter())
        .map(|change| change.chunk_id.clone())
        .collect::<BTreeSet<_>>();
    if receipt_ids != expected_ids {
        return Err(VectorAuthorityError::BatchIdentityMismatch(
            "chunk receipts do not cover the complete projection request".to_owned(),
        ));
    }
    Ok(())
}

fn unique_vectors(
    vectors: &[ProjectedChunkVectorV1],
) -> Result<BTreeMap<CodeSearchChunkId, &ProjectedChunkVectorV1>, VectorAuthorityError> {
    let mut by_chunk = BTreeMap::new();
    for vector in vectors {
        if by_chunk.insert(vector.chunk_id.clone(), vector).is_some() {
            return Err(VectorAuthorityError::DuplicateChunkEffect(
                vector.chunk_id.to_string(),
            ));
        }
    }
    Ok(by_chunk)
}

fn unique_tombstones(
    tombstones: &[VectorTombstoneV1],
) -> Result<BTreeMap<CodeSearchChunkId, &VectorTombstoneV1>, VectorAuthorityError> {
    let mut by_chunk = BTreeMap::new();
    for tombstone in tombstones {
        tombstone.validate()?;
        if by_chunk
            .insert(tombstone.chunk_id.clone(), tombstone)
            .is_some()
        {
            return Err(VectorAuthorityError::DuplicateChunkEffect(
                tombstone.chunk_id.to_string(),
            ));
        }
    }
    Ok(by_chunk)
}

fn change_for_receipt<'a>(
    changes: &'a crate::types::ChangedCodeChunkSetV1,
    receipt: &CodeChunkProjectionReceiptV1,
) -> Result<&'a crate::types::ChangedCodeChunkV1, VectorAuthorityError> {
    let change = match receipt.operation {
        ProjectionOperationV1::Added => changes
            .added_or_changed
            .iter()
            .find(|change| change.chunk_id == receipt.chunk_id && change.prior_digest.is_none()),
        ProjectionOperationV1::Updated => changes
            .added_or_changed
            .iter()
            .find(|change| change.chunk_id == receipt.chunk_id)
            .or_else(|| {
                // A projection-profile change re-embeds the unchanged
                // `reused` partition, so its receipt operation is Updated
                // even though its source change remains in that partition.
                changes
                    .reused
                    .iter()
                    .find(|change| change.chunk_id == receipt.chunk_id)
            }),
        ProjectionOperationV1::Reused => changes
            .reused
            .iter()
            .find(|change| change.chunk_id == receipt.chunk_id),
        ProjectionOperationV1::Deleted => changes
            .deleted
            .iter()
            .find(|change| change.chunk_id == receipt.chunk_id),
    };
    change.ok_or_else(|| {
        VectorAuthorityError::BatchIdentityMismatch(format!(
            "receipt {} is absent from its change partition",
            receipt.chunk_id
        ))
    })
}

fn validate_prepared_vector(
    vector: &ProjectedChunkVectorV1,
    admitted: &AdmittedEmbeddingProjectionKeyV1,
    plan: &VectorGenerationPlanV1,
    request_manifest_digest: &ManifestDigest,
) -> Result<(), VectorAuthorityError> {
    vector.validate(admitted)?;
    if vector.source_generation != plan.source_generation
        || vector.source_manifest_digest != *request_manifest_digest
    {
        return Err(VectorAuthorityError::BatchIdentityMismatch(
            "prepared vector source identity differs from its projection request".to_owned(),
        ));
    }
    Ok(())
}

fn validate_staged_row(
    row: &StagedVectorRowV1,
    admitted: &AdmittedEmbeddingProjectionKeyV1,
    plan: &VectorGenerationPlanV1,
) -> Result<(), VectorAuthorityError> {
    if row.projection_key != *admitted.projection_key()
        || row.source_generation != plan.source_generation
        || row.source_manifest_digest != plan.source_manifest_digest
    {
        return Err(VectorAuthorityError::Corrupt(
            "staged row source/model identity differs from plan".to_owned(),
        ));
    }
    Ok(())
}

fn validate_published_row(
    row: &PublishedVectorRowV1,
    admitted: &AdmittedEmbeddingProjectionKeyV1,
    plan: &VectorGenerationPlanV1,
) -> Result<(), VectorAuthorityError> {
    if row.projection_key != *admitted.projection_key()
        || row.source_generation != plan.source_generation
        || row.source_manifest_digest != plan.source_manifest_digest
    {
        return Err(VectorAuthorityError::Corrupt(
            "published row source/model identity differs from plan".to_owned(),
        ));
    }
    Ok(())
}

fn ensure_pool_compatible(
    pool: &BTreeMap<ContentDigest, VectorBlobV1>,
    additions: &mut Vec<(ContentDigest, Vec<f32>)>,
    digest: &ContentDigest,
    values: &[f32],
) -> Result<(), VectorAuthorityError> {
    if let Some(existing) = pool.get(digest) {
        if existing.values != values {
            return Err(VectorAuthorityError::ContentAddressConflict);
        }
        return Ok(());
    }
    if let Some((_, existing)) = additions
        .iter()
        .find(|(existing_digest, _)| existing_digest == digest)
    {
        if existing != values {
            return Err(VectorAuthorityError::ContentAddressConflict);
        }
        return Ok(());
    }
    additions.push((digest.clone(), values.to_vec()));
    Ok(())
}

pub fn prepare_vector(
    admitted: &AdmittedEmbeddingProjectionKeyV1,
    source_generation: CodeGenerationId,
    source_manifest_digest: ManifestDigest,
    chunk_id: CodeSearchChunkId,
    chunk_digest: ContentDigest,
    values: Vec<f32>,
) -> Result<ProjectedChunkVectorV1, VectorAuthorityError> {
    ProjectedChunkVectorV1::new(
        admitted,
        source_generation,
        source_manifest_digest,
        chunk_id,
        chunk_digest,
        values,
    )
}
