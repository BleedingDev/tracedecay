//! Runtime-family dispatch for the CPU FastEmbed backend.
//!
//! The family and build revision are persisted in projection identities. A
//! runtime can therefore admit only the implementation that produced the
//! identity, while unsupported platforms retain the same typed unavailable
//! behavior as any other runtime incompatibility.
use std::sync::Arc;

use super::artifact_store::{FASTEMBED_RUNTIME_BUILD_REVISION_V1, FASTEMBED_RUNTIME_FAMILY_V1};
use super::fastembed_adapter::{
    AdmittedProjectionArtifactV1, BoundedSanitizedTextBatchV1, EmbedError, EmbeddingRuntime,
    EmbeddingSession, EmbeddingVectorV1, FastEmbedEmbeddingRuntime, SemanticExecutionAuthority,
};

/// Runtime family recorded as `EmbeddingProjectionKeyV1::runtime_backend`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EmbeddingRuntimeFamilyV1 {
    FastEmbedOrt,
}

impl EmbeddingRuntimeFamilyV1 {
    /// Whether this binary can execute the backend, independent of stored
    /// artifacts. Windows intentionally exposes a typed unavailable runtime.
    pub const fn is_compiled(self) -> bool {
        match self {
            Self::FastEmbedOrt => cfg!(all(feature = "semantic-fastembed", not(windows))),
        }
    }

    pub const fn runtime_family(self) -> &'static str {
        FASTEMBED_RUNTIME_FAMILY_V1
    }

    pub const fn build_revision(self) -> &'static str {
        FASTEMBED_RUNTIME_BUILD_REVISION_V1
    }

    pub fn from_runtime_family(name: &str) -> Option<Self> {
        (name == FASTEMBED_RUNTIME_FAMILY_V1).then_some(Self::FastEmbedOrt)
    }
}

/// Production embedding runtime for the single supported backend.
#[derive(Default)]
pub struct ProductionEmbeddingRuntime {
    fastembed: FastEmbedEmbeddingRuntime,
}

/// One warmed FastEmbed session.
pub enum ProductionEmbeddingSession {
    FastEmbed(Box<<FastEmbedEmbeddingRuntime as EmbeddingRuntime>::Session>),
}

impl EmbeddingSession for ProductionEmbeddingSession {
    fn authority(&self) -> &AdmittedProjectionArtifactV1 {
        match self {
            Self::FastEmbed(session) => session.authority(),
        }
    }

    fn resident_bytes_estimate(&self) -> u64 {
        match self {
            Self::FastEmbed(session) => session.resident_bytes_estimate(),
        }
    }

    fn embed_batch(
        &mut self,
        batch: &BoundedSanitizedTextBatchV1,
        authority: &dyn SemanticExecutionAuthority,
    ) -> Result<Vec<EmbeddingVectorV1>, EmbedError> {
        match self {
            Self::FastEmbed(session) => session.embed_batch(batch, authority),
        }
    }

    fn encoded_token_lengths(&mut self, texts: &[String]) -> Result<Vec<usize>, EmbedError> {
        match self {
            Self::FastEmbed(session) => session.encoded_token_lengths(texts),
        }
    }
}

impl EmbeddingRuntime for ProductionEmbeddingRuntime {
    type Session = ProductionEmbeddingSession;

    fn resident_bytes_reservation(&self, authority: &AdmittedProjectionArtifactV1) -> u64 {
        self.fastembed.resident_bytes_reservation(authority)
    }

    fn verify_artifact_compatibility(
        &self,
        authority: &AdmittedProjectionArtifactV1,
    ) -> Result<(), EmbedError> {
        self.fastembed.verify_artifact_compatibility(authority)
    }

    fn open_session(
        &self,
        authority: &AdmittedProjectionArtifactV1,
        interruption: &dyn SemanticExecutionAuthority,
    ) -> Result<Self::Session, EmbedError> {
        self.fastembed
            .open_session(authority, interruption)
            .map(|session| ProductionEmbeddingSession::FastEmbed(Box::new(session)))
    }
}

/// Factory used by integration code to create a fresh production runtime.
pub fn production_embedding_runtime_factory()
-> Arc<dyn Fn() -> Result<ProductionEmbeddingRuntime, EmbedError> + Send + Sync> {
    Arc::new(|| Ok(ProductionEmbeddingRuntime::default()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_family_identity_is_stable_and_exhaustive() {
        let family = EmbeddingRuntimeFamilyV1::FastEmbedOrt;
        assert_eq!(family.runtime_family(), "fastembed-ort");
        assert_eq!(
            EmbeddingRuntimeFamilyV1::from_runtime_family(family.runtime_family()),
            Some(family)
        );
        assert!(!family.build_revision().is_empty());
        assert!(EmbeddingRuntimeFamilyV1::from_runtime_family("other").is_none());
    }
}
