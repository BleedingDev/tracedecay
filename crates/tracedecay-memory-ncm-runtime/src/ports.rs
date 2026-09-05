//! Runtime ports: the seams between real implementations and named test doubles.

use std::path::{Path, PathBuf};
use tracedecay_memory_ncm_core::types::EMBEDDING_DIM;

/// Typed encoder failures (contract §9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EncoderError {
    /// Verified local artifacts are absent; explicit install is required.
    ArtifactsMissing(String),
    /// Artifact digest or shape did not match the pinned manifest.
    ArtifactMismatch(String),
    /// Inference failed.
    Inference(String),
    /// Deadline elapsed or cancellation observed before completion.
    Cancelled,
    /// Input exceeded the record budget.
    InputTooLarge,
}

impl std::fmt::Display for EncoderError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ArtifactsMissing(detail) => write!(formatter, "encoder artifacts missing: {detail}"),
            Self::ArtifactMismatch(detail) => write!(formatter, "encoder artifact mismatch: {detail}"),
            Self::Inference(detail) => write!(formatter, "encoder inference failed: {detail}"),
            Self::Cancelled => formatter.write_str("encoder cancelled"),
            Self::InputTooLarge => formatter.write_str("encoder input too large"),
        }
    }
}

impl std::error::Error for EncoderError {}

/// A normalized 384-D sentence embedding.
#[derive(Clone, Debug, PartialEq)]
pub struct Embedding(pub Vec<f32>);

impl Embedding {
    /// Validates dimension and finiteness.
    pub fn validated(values: Vec<f32>) -> Result<Self, EncoderError> {
        if values.len() != EMBEDDING_DIM {
            return Err(EncoderError::ArtifactMismatch(format!(
                "embedding dimension {} != {EMBEDDING_DIM}",
                values.len()
            )));
        }
        if !values.iter().all(|value| value.is_finite()) {
            return Err(EncoderError::Inference("non-finite embedding".to_owned()));
        }
        Ok(Self(values))
    }
}

/// Identity of the encoder that produced embeddings; part of the ready receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncoderIdentity {
    /// Model name, e.g. `paraphrase-multilingual-MiniLM-L12-v2`.
    pub model: String,
    /// sha256 of the ONNX artifact.
    pub artifact_sha256: String,
    /// Max sequence length applied (128).
    pub max_length: usize,
}

/// Text encoder port. The production implementation is `embedding::MiniLmEncoder`;
/// `embedding::doubles::HashEncoder` is a named test double and cannot satisfy real-model gates.
pub trait TextEncoder: Send + Sync {
    /// Identity bound into readiness.
    fn identity(&self) -> EncoderIdentity;
    /// Encodes a batch (≤ 32 texts) honoring the deadline.
    fn encode(&self, texts: &[&str], deadline: Deadline) -> Result<Vec<Embedding>, EncoderError>;
}

/// Remaining-time budget passed through every runtime call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Deadline {
    /// Remaining milliseconds; `0` means already expired.
    pub remaining_ms: u64,
}

/// Admitted provider state root. Namespace paths never escape it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateRoot(PathBuf);

impl StateRoot {
    /// Wraps an admitted absolute root directory.
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, String> {
        let path: PathBuf = path.into();
        if !path.is_absolute() {
            return Err("state root must be absolute".to_owned());
        }
        Ok(Self(path))
    }

    /// Root path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.0
    }

    /// Per-namespace directory; the namespace must be a lowercase sha256 hex.
    pub fn namespace_dir(&self, namespace: &str) -> Result<PathBuf, String> {
        if namespace.len() != 64 || !namespace.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f')) {
            return Err("namespace must be lowercase sha256 hex".to_owned());
        }
        Ok(self.0.join("namespaces").join(namespace))
    }

    /// Model artifact directory.
    #[must_use]
    pub fn models_dir(&self) -> PathBuf {
        self.0.join("models")
    }
}
