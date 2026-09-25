use std::sync::Arc;

use thiserror::Error;

pub use tracedecay_vector_authority::{
    BatchCommitDecisionV1, ContentDigest, PreparedVectorGenerationV1, ProjectedChunkVectorV1,
    ProjectionKeyV1, PublishedVectorGenerationV1, VectorAuthorityError, VectorGenerationAuthority,
    VectorGenerationBuildIdV1, VectorGenerationIdV1, VectorGenerationPlanV1,
    VectorGenerationPublicationV1, VectorProjectionCheckpointV1, prepare_vector,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct VectorAuthorityRevisionV1(u64);

impl VectorAuthorityRevisionV1 {
    pub const INITIAL: Self = Self(0);

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn next(self) -> Result<Self, VectorAuthorityStoreErrorV1> {
        self.0.checked_add(1).map(Self).ok_or_else(|| {
            VectorAuthorityStoreErrorV1::Corrupt("vector-authority revision overflow".to_owned())
        })
    }

    pub fn from_stored(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Debug, Error)]
pub enum VectorAuthorityStoreErrorV1 {
    #[error(
        "vector authority changed concurrently (expected revision {expected}, actual {actual})"
    )]
    Conflict { expected: u64, actual: u64 },
    #[error("vector authority storage is unavailable: {0}")]
    Unavailable(String),
    #[error("vector authority storage is corrupt: {0}")]
    Corrupt(String),
    #[error(transparent)]
    Authority(#[from] VectorAuthorityError),
}

/// Durable project-scoped vector lifecycle. Implementations acknowledge a
/// mutation only after its checkpoint or pointer swap commits atomically.
pub trait DurableVectorAuthorityStoreV1: Send + Sync {
    fn revision(&self) -> Result<VectorAuthorityRevisionV1, VectorAuthorityStoreErrorV1>;

    fn snapshot(&self) -> Result<Arc<VectorGenerationAuthority>, VectorAuthorityStoreErrorV1>;

    fn begin_generation(
        &self,
        plan: VectorGenerationPlanV1,
    ) -> Result<VectorGenerationBuildIdV1, VectorAuthorityStoreErrorV1>;

    fn commit_batch(
        &self,
        build_id: &VectorGenerationBuildIdV1,
        expected_checkpoint: Option<&VectorProjectionCheckpointV1>,
        prepared: PreparedVectorGenerationV1,
    ) -> Result<VectorProjectionCheckpointV1, VectorAuthorityStoreErrorV1>;

    fn publish_generation_if_current(
        &self,
        build_id: &VectorGenerationBuildIdV1,
        expected_active: Option<&VectorGenerationIdV1>,
    ) -> Result<VectorGenerationPublicationV1, VectorAuthorityStoreErrorV1>;

    fn activate_generation_if_current(
        &self,
        generation_id: &VectorGenerationIdV1,
        expected_active: Option<&VectorGenerationIdV1>,
    ) -> Result<Option<VectorGenerationIdV1>, VectorAuthorityStoreErrorV1>;

    fn rollback_generation_if_current(
        &self,
        expected_active: Option<&VectorGenerationIdV1>,
    ) -> Result<VectorGenerationIdV1, VectorAuthorityStoreErrorV1>;

    fn restore_active_generation_if_current(
        &self,
        expected_active: &VectorGenerationIdV1,
        replacement: Option<&VectorGenerationIdV1>,
    ) -> Result<(), VectorAuthorityStoreErrorV1>;
}
