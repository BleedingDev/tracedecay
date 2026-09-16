//! Storage-neutral contracts for the isolated dense code-search authority.
//!
//! The types in this module deliberately carry all identities that can change
//! a vector.  A vector is useful only when its source generation, projection,
//! model, and privacy epoch are still the values the caller authorized.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const VECTOR_AUTHORITY_SCHEMA_V1: &str = "tracedecay.vector-authority.v1";
pub const EMBEDDING_PROJECTION_SCHEMA_V1: &str = "tracedecay.embedding-projection.v1";
pub const VECTOR_OUTPUT_DIGEST_DOMAIN_V1: &str = "tracedecay.semantic-vector-output.v1";
pub const VECTOR_GENERATION_PLAN_DIGEST_DOMAIN_V1: &str =
    "tracedecay.vector-generation-manifest.v1";
pub const VECTOR_GENERATION_BUILD_DIGEST_DOMAIN_V1: &str = "tracedecay.vector-generation-build.v1";
pub const VECTOR_BATCH_DIGEST_DOMAIN_V1: &str = "tracedecay.vector-committed-batch.v1";
pub const VECTOR_SNAPSHOT_DIGEST_DOMAIN_V1: &str = "tracedecay.vector-authority-snapshot.v1";

/// Errors are intentionally typed by lifecycle boundary.  Callers can
/// distinguish a stale retry from corruption without inspecting an error
/// string, while the detail stays useful in logs and tests.
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
    #[error("invalid identity: {0}")]
    InvalidIdentity(String),
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

/// A typed identity used for source and chunk names.  Unlike integrity
/// digests, these values are human-readable names, but they still reject
/// whitespace/control data so their canonical ordering is unambiguous.
macro_rules! identity_id {
    ($name:ident) => {
        #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, VectorAuthorityError> {
                let value = value.into();
                if value.is_empty() || value.trim() != value || value.chars().any(char::is_control)
                {
                    return Err(VectorAuthorityError::InvalidIdentity(format!(
                        "{} is empty or non-canonical",
                        stringify!($name)
                    )));
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn validate(&self) -> Result<(), VectorAuthorityError> {
                Self::new(self.0.clone()).map(|_| ())
            }
        }

        impl TryFrom<String> for $name {
            type Error = VectorAuthorityError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = VectorAuthorityError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl FromStr for $name {
            type Err = VectorAuthorityError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

macro_rules! digest_id {
    ($name:ident) => {
        #[derive(Clone, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, VectorAuthorityError> {
                let value = value.into();
                if !is_integrity_digest(&value) {
                    return Err(VectorAuthorityError::InvalidIdentity(format!(
                        "{} must be an algorithm-tagged lowercase digest",
                        stringify!($name)
                    )));
                }
                Ok(Self(value))
            }

            pub fn from_bytes(bytes: &[u8]) -> Self {
                Self(format!("sha256:{}", hex_encode(&sha256(bytes))))
            }

            pub fn zero() -> Self {
                Self(format!("sha256:{}", "0".repeat(64)))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn validate(&self) -> Result<(), VectorAuthorityError> {
                Self::new(self.0.clone()).map(|_| ())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
            }
        }

        impl TryFrom<String> for $name {
            type Error = VectorAuthorityError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = VectorAuthorityError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

identity_id!(CodeGenerationId);
identity_id!(CodeSearchChunkId);
identity_id!(PrivacyDomainId);
identity_id!(ChunkerRevision);

digest_id!(ManifestDigest);
digest_id!(ContentDigest);

pub type VectorDigest = ContentDigest;
pub type RequestDigest = ManifestDigest;

fn is_integrity_digest(value: &str) -> bool {
    let Some((algorithm, body)) = value.split_once(':') else {
        return false;
    };
    let expected = match algorithm {
        "sha256" | "blake3" => 64,
        "sha512" => 128,
        _ => return false,
    };
    body.len() == expected
        && body
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

pub(crate) fn digest_json<T: Serialize>(
    domain: &str,
    value: &T,
) -> Result<ManifestDigest, VectorAuthorityError> {
    digest_value(&(domain, value))
}

/// Serialize a digest input in the same canonical shape used by the
/// historical domain contracts. `serde_json::Value` uses ordered object keys
/// in this crate, while retaining tuple/array shape and finite float bits.
pub(crate) fn digest_value<T: Serialize>(
    value: &T,
) -> Result<ManifestDigest, VectorAuthorityError> {
    let value = serde_json::to_value(value)
        .map_err(|error| VectorAuthorityError::Serialization(error.to_string()))?;
    let payload = serde_json::to_vec(&value)
        .map_err(|error| VectorAuthorityError::Serialization(error.to_string()))?;
    Ok(ManifestDigest::from_bytes(&payload))
}

pub(crate) fn digest_bytes(domain: &str, bytes: &[u8]) -> ManifestDigest {
    let mut payload = Vec::with_capacity(domain.len() + 1 + bytes.len());
    payload.extend_from_slice(domain.as_bytes());
    payload.push(0);
    payload.extend_from_slice(bytes);
    ManifestDigest::from_bytes(&payload)
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionKindV1 {
    Lexical,
    Graph,
    Embedding,
}

#[derive(
    Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingPoolingV1 {
    #[default]
    Mean,
    Cls,
    LastToken,
    MeanSqrtLength,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingTruncationSideV1 {
    Left,
    Right,
}

#[derive(
    Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingDeviceClassV1 {
    #[default]
    Cpu,
    Gpu,
}

#[derive(
    Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingExecutionProviderV1 {
    #[default]
    Cpu,
    Cuda,
    WebGpu,
}

impl EmbeddingExecutionProviderV1 {
    pub const fn is_cpu(&self) -> bool {
        matches!(self, Self::Cpu)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingMetricV1 {
    Cosine,
    DotProduct,
    EuclideanL2,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingNormalizationV1 {
    None,
    L2,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingPrecisionV1 {
    Fp32,
    Fp16,
    Bf16,
    Int8,
}

#[derive(
    Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingDocumentCompositionV1 {
    #[default]
    SanitizedText,
    SymbolContextHeader,
}

impl EmbeddingDocumentCompositionV1 {
    pub const fn is_sanitized_text(&self) -> bool {
        matches!(self, Self::SanitizedText)
    }
}

/// Full model/runtime identity.  It is part of the projection key digest, so
/// changing a model, tokenizer, runtime, dimension, or numeric policy creates
/// a new immutable vector generation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(deny_unknown_fields)]
pub struct EmbeddingProjectionKeyV1 {
    pub model_artifact_digest: ManifestDigest,
    pub tokenizer_digest: ManifestDigest,
    pub config_digest: ManifestDigest,
    pub query_instruction_digest: Option<ManifestDigest>,
    pub document_instruction_digest: Option<ManifestDigest>,
    #[serde(
        default,
        skip_serializing_if = "EmbeddingDocumentCompositionV1::is_sanitized_text"
    )]
    pub document_composition: EmbeddingDocumentCompositionV1,
    pub pooling: EmbeddingPoolingV1,
    pub truncation_side: EmbeddingTruncationSideV1,
    pub truncation_length: u32,
    pub inference_batch_size: u32,
    pub inference_batch_bytes: u32,
    pub runtime_backend: String,
    pub runtime_build_revision: String,
    #[serde(default)]
    pub device_class: EmbeddingDeviceClassV1,
    #[serde(default, skip_serializing_if = "EmbeddingExecutionProviderV1::is_cpu")]
    pub execution_provider: EmbeddingExecutionProviderV1,
    pub dimensions: u32,
    pub metric: EmbeddingMetricV1,
    pub normalization: EmbeddingNormalizationV1,
    pub precision: EmbeddingPrecisionV1,
    pub chunk_schema_revision: String,
    pub chunker_revision: ChunkerRevision,
    pub privacy_domain: PrivacyDomainId,
    pub privacy_key_epoch: u64,
}

impl EmbeddingProjectionKeyV1 {
    pub fn validate(&self) -> Result<(), VectorAuthorityError> {
        self.model_artifact_digest.validate()?;
        self.tokenizer_digest.validate()?;
        self.config_digest.validate()?;
        if let Some(digest) = &self.query_instruction_digest {
            digest.validate()?;
        }
        if let Some(digest) = &self.document_instruction_digest {
            digest.validate()?;
        }
        if self.truncation_length == 0 {
            return Err(VectorAuthorityError::InvalidPlan(
                "embedding truncation length must be non-zero".to_owned(),
            ));
        }
        if self.inference_batch_size == 0 || self.inference_batch_bytes == 0 {
            return Err(VectorAuthorityError::InvalidPlan(
                "embedding batch limits must be non-zero".to_owned(),
            ));
        }
        if self.dimensions == 0 {
            return Err(VectorAuthorityError::InvalidPlan(
                "embedding dimensions must be non-zero".to_owned(),
            ));
        }
        validate_text_revision(&self.runtime_backend, "embedding runtime backend")?;
        validate_text_revision(&self.runtime_build_revision, "embedding runtime revision")?;
        validate_text_revision(&self.chunk_schema_revision, "embedding chunk schema")?;
        self.chunker_revision.validate()?;
        self.privacy_domain.validate()?;
        if self.privacy_key_epoch == 0 {
            return Err(VectorAuthorityError::InvalidPlan(
                "privacy key epoch must be non-zero".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<ManifestDigest, VectorAuthorityError> {
        self.validate()?;
        digest_json("tracedecay.embedding-projection-key.v1", self)
    }

    pub fn admit(&self) -> Result<AdmittedEmbeddingProjectionKeyV1, VectorAuthorityError> {
        let profile_digest = self.canonical_digest()?;
        Ok(AdmittedEmbeddingProjectionKeyV1 {
            embedding_key: self.clone(),
            projection_key: ProjectionKeyV1 {
                kind: ProjectionKindV1::Embedding,
                schema_revision: EMBEDDING_PROJECTION_SCHEMA_V1.to_owned(),
                profile_digest,
            },
        })
    }

    pub fn document_byte_budget(&self) -> Result<usize, VectorAuthorityError> {
        self.validate()?;
        let budget = self.inference_batch_bytes / self.inference_batch_size;
        if budget == 0 {
            return Err(VectorAuthorityError::InvalidPlan(
                "embedding document byte budget must be non-zero".to_owned(),
            ));
        }
        Ok(budget as usize)
    }
}

fn validate_text_revision(value: &str, field: &str) -> Result<(), VectorAuthorityError> {
    if value.is_empty() || value.trim() != value || value.chars().any(char::is_control) {
        return Err(VectorAuthorityError::InvalidPlan(format!(
            "{field} is empty or non-canonical"
        )));
    }
    Ok(())
}

/// Projection key plus the model/privacy admission proof.  The private fields
/// prevent a caller from fabricating a compatible-looking projection digest.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AdmittedEmbeddingProjectionKeyV1 {
    embedding_key: EmbeddingProjectionKeyV1,
    projection_key: ProjectionKeyV1,
}

impl Serialize for AdmittedEmbeddingProjectionKeyV1 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct AdmittedProjectionRef<'a> {
            embedding_key: &'a EmbeddingProjectionKeyV1,
            projection_key: &'a ProjectionKeyV1,
        }

        AdmittedProjectionRef {
            embedding_key: &self.embedding_key,
            projection_key: &self.projection_key,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AdmittedEmbeddingProjectionKeyV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct AdmittedProjectionRepr {
            embedding_key: EmbeddingProjectionKeyV1,
            projection_key: ProjectionKeyV1,
        }

        let repr = AdmittedProjectionRepr::deserialize(deserializer)?;
        let admitted = repr
            .embedding_key
            .admit()
            .map_err(serde::de::Error::custom)?;
        if admitted.projection_key != repr.projection_key {
            return Err(serde::de::Error::custom(
                "admitted embedding projection key digest mismatch",
            ));
        }
        Ok(admitted)
    }
}

impl AdmittedEmbeddingProjectionKeyV1 {
    pub fn embedding_key(&self) -> &EmbeddingProjectionKeyV1 {
        &self.embedding_key
    }

    pub fn projection_key(&self) -> &ProjectionKeyV1 {
        &self.projection_key
    }

    pub fn privacy_domain(&self) -> &PrivacyDomainId {
        &self.embedding_key.privacy_domain
    }

    pub fn privacy_key_epoch(&self) -> u64 {
        self.embedding_key.privacy_key_epoch
    }

    pub fn dimensions(&self) -> u32 {
        self.embedding_key.dimensions
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(deny_unknown_fields)]
pub struct ProjectionKeyV1 {
    pub kind: ProjectionKindV1,
    pub schema_revision: String,
    pub profile_digest: ManifestDigest,
}

impl ProjectionKeyV1 {
    pub fn validate(&self) -> Result<(), VectorAuthorityError> {
        if self.kind != ProjectionKindV1::Embedding {
            return Err(VectorAuthorityError::InvalidPlan(
                "vector authority accepts embedding projections only".to_owned(),
            ));
        }
        if self.schema_revision != EMBEDDING_PROJECTION_SCHEMA_V1 {
            return Err(VectorAuthorityError::InvalidPlan(
                "unknown embedding projection schema".to_owned(),
            ));
        }
        self.profile_digest.validate()
    }
}

/// A canonical source chunk descriptor used by convenience constructors and
/// tests. The authority stores only these identities and vector bytes; source
/// text never enters the durable vector snapshot.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct CodeSearchChunkV1 {
    pub id: CodeSearchChunkId,
    pub source_generation: CodeGenerationId,
    pub source_manifest_digest: ManifestDigest,
    pub content_digest: ContentDigest,
    pub privacy_domain: PrivacyDomainId,
    pub privacy_key_epoch: u64,
    #[serde(default)]
    pub sanitized_text: String,
}

impl CodeSearchChunkV1 {
    pub fn validate(&self) -> Result<(), VectorAuthorityError> {
        self.id.validate()?;
        self.source_generation.validate()?;
        self.source_manifest_digest.validate()?;
        self.content_digest.validate()?;
        self.privacy_domain.validate()?;
        if self.privacy_key_epoch == 0 {
            return Err(VectorAuthorityError::InvalidPlan(
                "chunk privacy key epoch must be non-zero".to_owned(),
            ));
        }
        Ok(())
    }

    /// Check the source chunk's privacy fence before a projector hands its
    /// bytes to the authority.  The authority repeats the same fence through
    /// the admitted embedding key and search compatibility context.
    pub fn validate_for_embedding(
        &self,
        admitted: &AdmittedEmbeddingProjectionKeyV1,
    ) -> Result<(), VectorAuthorityError> {
        self.validate()?;
        if self.privacy_domain != *admitted.privacy_domain()
            || self.privacy_key_epoch != admitted.privacy_key_epoch()
        {
            return Err(VectorAuthorityError::SearchContextMismatch(
                "chunk privacy domain or key epoch differs from the admitted embedding".to_owned(),
            ));
        }
        Ok(())
    }

    /// Check that this canonical chunk belongs to one planned source
    /// generation before a projector turns it into a vector.  The authority
    /// repeats the same source and membership fence from the opaque row
    /// identities, so a caller cannot accidentally mix chunks from another
    /// checkout into a resumable build.
    pub fn validate_for_generation(
        &self,
        plan: &VectorGenerationPlanV1,
        admitted: &AdmittedEmbeddingProjectionKeyV1,
    ) -> Result<(), VectorAuthorityError> {
        self.validate_for_embedding(admitted)?;
        if self.source_generation != plan.source_generation
            || self.source_manifest_digest != plan.source_manifest_digest
        {
            return Err(VectorAuthorityError::BatchIdentityMismatch(
                "chunk source generation or manifest differs from the plan".to_owned(),
            ));
        }
        if plan.expected_chunk_ids.binary_search(&self.id).is_err() {
            return Err(VectorAuthorityError::BatchIdentityMismatch(
                "chunk is outside the planned generation membership".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn from_text(
        id: CodeSearchChunkId,
        source_generation: CodeGenerationId,
        source_manifest_digest: ManifestDigest,
        privacy_domain: PrivacyDomainId,
        privacy_key_epoch: u64,
        sanitized_text: impl Into<String>,
    ) -> Result<Self, VectorAuthorityError> {
        let sanitized_text = sanitized_text.into();
        if sanitized_text.is_empty() || sanitized_text.chars().any(char::is_control) {
            return Err(VectorAuthorityError::InvalidPlan(
                "chunk text is empty or contains control data".to_owned(),
            ));
        }
        let content_digest = ContentDigest::from_bytes(sanitized_text.as_bytes());
        let chunk = Self {
            id,
            source_generation,
            source_manifest_digest,
            content_digest,
            privacy_domain,
            privacy_key_epoch,
            sanitized_text,
        };
        chunk.validate()?;
        Ok(chunk)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChangedCodeChunkV1 {
    pub chunk_id: CodeSearchChunkId,
    pub prior_digest: Option<ContentDigest>,
    pub current_digest: Option<ContentDigest>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChangedCodeChunkSetV1 {
    pub from_generation: Option<CodeGenerationId>,
    pub to_generation: CodeGenerationId,
    pub manifest_digest: ManifestDigest,
    pub added_or_changed: Vec<ChangedCodeChunkV1>,
    pub deleted: Vec<ChangedCodeChunkV1>,
    pub reused: Vec<ChangedCodeChunkV1>,
}

impl ChangedCodeChunkSetV1 {
    pub fn compute_digest(&self) -> Result<ManifestDigest, VectorAuthorityError> {
        digest_value(&ChangedCodeChunkSetDigestInput {
            domain: "tracedecay.changed-code-chunks.v1",
            from_generation: &self.from_generation,
            to_generation: &self.to_generation,
            added_or_changed: &self.added_or_changed,
            deleted: &self.deleted,
            reused: &self.reused,
        })
    }

    pub fn validate(&self) -> Result<(), VectorAuthorityError> {
        self.to_generation.validate()?;
        if let Some(from) = &self.from_generation {
            from.validate()?;
            if from == &self.to_generation {
                return Err(VectorAuthorityError::BatchIdentityMismatch(
                    "source change generations must differ".to_owned(),
                ));
            }
        }
        validate_change_partition(&self.added_or_changed, ChangePartition::AddedOrChanged)?;
        validate_change_partition(&self.deleted, ChangePartition::Deleted)?;
        validate_change_partition(&self.reused, ChangePartition::Reused)?;
        let mut ids = std::collections::BTreeSet::new();
        for change in self
            .added_or_changed
            .iter()
            .chain(self.deleted.iter())
            .chain(self.reused.iter())
        {
            if !ids.insert(&change.chunk_id) {
                return Err(VectorAuthorityError::BatchIdentityMismatch(
                    "chunk appears in more than one change partition".to_owned(),
                ));
            }
        }
        self.manifest_digest.validate()?;
        if self.compute_digest()? != self.manifest_digest {
            return Err(VectorAuthorityError::BatchIdentityMismatch(
                "changed chunk manifest digest mismatch".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct ChangedCodeChunkSetDigestInput<'a> {
    domain: &'static str,
    from_generation: &'a Option<CodeGenerationId>,
    to_generation: &'a CodeGenerationId,
    added_or_changed: &'a [ChangedCodeChunkV1],
    deleted: &'a [ChangedCodeChunkV1],
    reused: &'a [ChangedCodeChunkV1],
}

#[derive(Clone, Copy)]
enum ChangePartition {
    AddedOrChanged,
    Deleted,
    Reused,
}

fn validate_change_partition(
    changes: &[ChangedCodeChunkV1],
    partition: ChangePartition,
) -> Result<(), VectorAuthorityError> {
    for change in changes {
        change.chunk_id.validate()?;
        if let Some(digest) = &change.prior_digest {
            digest.validate()?;
        }
        if let Some(digest) = &change.current_digest {
            digest.validate()?;
        }
        let valid = match partition {
            ChangePartition::AddedOrChanged => {
                change.current_digest.is_some()
                    && change.prior_digest.as_ref() != change.current_digest.as_ref()
            }
            ChangePartition::Deleted => {
                change.prior_digest.is_some() && change.current_digest.is_none()
            }
            ChangePartition::Reused => {
                change.prior_digest.is_some() && change.prior_digest == change.current_digest
            }
        };
        if !valid {
            return Err(VectorAuthorityError::BatchIdentityMismatch(
                "invalid chunk change digest shape".to_owned(),
            ));
        }
    }
    if changes
        .windows(2)
        .any(|pair| pair[0].chunk_id >= pair[1].chunk_id)
    {
        return Err(VectorAuthorityError::BatchIdentityMismatch(
            "chunk changes must be sorted and unique".to_owned(),
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionReplayReasonV1 {
    InitialProjection,
    SourceEdit,
    ProjectionProfileChange,
    FullRebuild,
    FullRebuildIncompatible,
    QuarantinedCorruption,
    VerificationReplay,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProjectionBatchRequestV1 {
    pub request_digest: ManifestDigest,
    pub changes: ChangedCodeChunkSetV1,
    pub previous_projection_key: Option<ProjectionKeyV1>,
    pub target_projection_key: ProjectionKeyV1,
    pub replay_reason: ProjectionReplayReasonV1,
}

impl ProjectionBatchRequestV1 {
    pub fn compute_digest(&self) -> Result<ManifestDigest, VectorAuthorityError> {
        digest_value(&(
            "tracedecay.projection-batch-request.v1",
            &self.changes,
            &self.previous_projection_key,
            &self.target_projection_key,
            self.replay_reason,
        ))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "outcome", content = "reason", rename_all = "snake_case")]
pub enum ProjectionOutcomeV1 {
    Applied,
    Reused,
    Skipped { reason: String },
    Tombstoned,
    Failed { reason: String },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionOperationV1 {
    Added,
    Updated,
    Reused,
    Deleted,
}

impl ProjectionOperationV1 {
    // Historical dca4de9de callers used Added/Updated/Reused/Deleted.  The
    // short associated constants keep Add/Update/Reuse/Delete source wording
    // available without creating duplicate wire operations.
    #[allow(non_upper_case_globals)]
    pub const Add: Self = Self::Added;
    #[allow(non_upper_case_globals)]
    pub const Update: Self = Self::Updated;
    #[allow(non_upper_case_globals)]
    pub const Reuse: Self = Self::Reused;
    #[allow(non_upper_case_globals)]
    pub const Delete: Self = Self::Deleted;

    pub fn produces_vector(self) -> bool {
        matches!(self, Self::Added | Self::Updated)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CodeChunkProjectionReceiptV1 {
    pub projection_key: ProjectionKeyV1,
    pub request_digest: ManifestDigest,
    pub prior_generation: Option<CodeGenerationId>,
    pub source_generation: CodeGenerationId,
    pub source_manifest_digest: ManifestDigest,
    pub chunk_id: CodeSearchChunkId,
    pub prior_chunk_digest: Option<ContentDigest>,
    pub current_chunk_digest: Option<ContentDigest>,
    pub operation: ProjectionOperationV1,
    pub outcome: ProjectionOutcomeV1,
    pub output_digest: Option<ContentDigest>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProjectionBatchReceiptV1 {
    pub target_projection_key: ProjectionKeyV1,
    pub request_digest: ManifestDigest,
    pub source_generation: CodeGenerationId,
    pub source_manifest_digest: ManifestDigest,
    pub receipts: Vec<CodeChunkProjectionReceiptV1>,
    pub reused_count: u64,
    pub publication_digest: ManifestDigest,
}

impl ProjectionBatchReceiptV1 {
    pub fn expected_publication_digest(&self) -> Result<ManifestDigest, VectorAuthorityError> {
        digest_value(&(
            "tracedecay.projection-batch-receipt.v1",
            &self.target_projection_key,
            &self.request_digest,
            &self.source_generation,
            &self.source_manifest_digest,
            &self.receipts,
            self.reused_count,
        ))
    }

    pub fn validate_digest(&self) -> Result<(), VectorAuthorityError> {
        self.target_projection_key.validate()?;
        self.request_digest.validate()?;
        self.source_generation.validate()?;
        self.source_manifest_digest.validate()?;
        if self
            .receipts
            .windows(2)
            .any(|pair| pair[0].chunk_id >= pair[1].chunk_id)
        {
            return Err(VectorAuthorityError::BatchIdentityMismatch(
                "projection receipts must be sorted and unique".to_owned(),
            ));
        }
        if self.expected_publication_digest()? != self.publication_digest {
            return Err(VectorAuthorityError::BatchIdentityMismatch(
                "projection batch publication digest mismatch".to_owned(),
            ));
        }
        Ok(())
    }
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
        validate_values(&values, admitted.dimensions() as usize)?;
        let output_digest =
            vector_output_digest(admitted.projection_key(), &chunk_id, &chunk_digest, &values)?;
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
        validate_values(&self.values, admitted.dimensions() as usize)?;
        if vector_output_digest(
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
    let bits: Vec<u32> = values.iter().map(|value| value.to_bits()).collect();
    let digest = digest_value(&(
        VECTOR_OUTPUT_DIGEST_DOMAIN_V1,
        projection_key,
        chunk_id,
        chunk_digest,
        bits,
    ))?;
    ContentDigest::new(digest.as_str())
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
        self.prior_chunk_digest.validate()
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
        self.target_projection_key.validate()?;
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
        Ok(())
    }

    pub fn identity_digest(&self) -> Result<ManifestDigest, VectorAuthorityError> {
        self.validate()?;
        // Base lineage is execution history. The immutable generation
        // identity is known before projection starts and stays stable when
        // the same source/projection/membership plan is rebuilt from another
        // compatible base.
        digest_value(&(
            VECTOR_GENERATION_PLAN_DIGEST_DOMAIN_V1,
            &self.target_projection_key,
            &self.source_generation,
            &self.source_manifest_digest,
            &self.expected_chunk_ids,
        ))
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
pub struct VectorGenerationIdV1(ManifestDigest);

impl VectorGenerationIdV1 {
    pub fn new(digest: ManifestDigest) -> Self {
        Self(digest)
    }

    pub fn as_digest(&self) -> &ManifestDigest {
        &self.0
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn validate(&self) -> Result<(), VectorAuthorityError> {
        self.0.validate()
    }
}

impl fmt::Display for VectorGenerationIdV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
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

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn validate(&self) -> Result<(), VectorAuthorityError> {
        self.0.validate()
    }
}

impl fmt::Display for VectorGenerationBuildIdV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
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

/// A published generation is immutable from the authority's public API. Its
/// rows contain content addresses; float payloads live in the authority's
/// deduplicated content-addressed pool.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PublishedVectorGenerationV1 {
    pub(crate) generation_id: VectorGenerationIdV1,
    pub(crate) plan: VectorGenerationPlanV1,
    pub(crate) embedding_key: AdmittedEmbeddingProjectionKeyV1,
    pub(crate) rows: std::collections::BTreeMap<CodeSearchChunkId, PublishedVectorRowV1>,
    pub(crate) tombstone_digests: std::collections::BTreeMap<CodeSearchChunkId, ContentDigest>,
    pub(crate) receipts: Vec<ProjectionBatchReceiptV1>,
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

    pub fn vectors(&self) -> &std::collections::BTreeMap<CodeSearchChunkId, PublishedVectorRowV1> {
        &self.rows
    }

    /// Alias retained for storage-oriented callers that refer to published
    /// vector rows as `rows`.
    pub fn rows(&self) -> &std::collections::BTreeMap<CodeSearchChunkId, PublishedVectorRowV1> {
        self.vectors()
    }

    pub fn tombstone_digests(
        &self,
    ) -> &std::collections::BTreeMap<CodeSearchChunkId, ContentDigest> {
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

/// Read-only exact-flat index identity. No ANN or mutable query index is part
/// of this crate's authority.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SearchIndexKindV1 {
    #[default]
    ExactFlat,
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
    for (&l, &r) in left.iter().zip(right.iter()) {
        let l = f64::from(l);
        let r = f64::from(r);
        dot += l * r;
        left_norm += l * l;
        right_norm += r * r;
    }
    if left_norm == 0.0 || right_norm == 0.0 {
        return Err(VectorAuthorityError::InvalidVector(
            "cosine vectors must have non-zero norm".to_owned(),
        ));
    }
    let score = (dot / (left_norm.sqrt() * right_norm.sqrt())) as f32;
    if !score.is_finite() {
        return Err(VectorAuthorityError::InvalidVector(
            "cosine score is non-finite".to_owned(),
        ));
    }
    Ok(score)
}

pub fn decode_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, VectorAuthorityError> {
    serde_json::from_slice(bytes)
        .map_err(|error| VectorAuthorityError::Serialization(error.to_string()))
}
