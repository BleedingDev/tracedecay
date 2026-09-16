//! Native multilingual text embeddings for the NCM runtime.
//!
//! The production encoder is a small, synchronous wrapper around the pinned
//! `fastembed` ONNX backend. Installation is deliberately separate from
//! opening and inference: [`install`] is the only operation in this module
//! that may fetch model files. [`MiniLmEncoder::open`] validates every local
//! artifact before asking `fastembed` to construct its model, so ordinary
//! runtime operations never use an online fallback.
//!
//! [`doubles::HashEncoder`] is intentionally a named test double. It is useful
//! for exercising non-inference runtime paths, but its identity cannot satisfy
//! the production model gate.

use crate::ports::{Deadline, Embedding, EncoderError, EncoderIdentity, StateRoot, TextEncoder};
use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt};
use cap_std::{
    ambient_authority,
    fs::{Dir, OpenOptions},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::ffi::{OsStr, OsString};
#[cfg(feature = "real-encoder")]
use std::fs;
use std::io::Read;
#[cfg(feature = "real-encoder")]
use std::io::Write;
use std::path::{Component, Path, PathBuf};
#[cfg(feature = "real-encoder")]
use std::sync::Mutex;
use tracedecay_memory_ncm_core::types::AffectVector;

/// Short model identity used by the runtime and readiness receipts.
pub const MODEL_NAME: &str = "paraphrase-multilingual-MiniLM-L12-v2";
/// Hugging Face repository used by the pinned fastembed model definition.
pub const MODEL_REPOSITORY: &str = "Xenova/paraphrase-multilingual-MiniLM-L12-v2";
/// Immutable Xenova snapshot verified by the tracked backend acceptance receipt.
pub const MODEL_REVISION: &str = "2c4055b12046f11709e9df2c122e59ffbdc2f900";
/// Tracked evidence for the immutable model revision.
pub const MODEL_REVISION_PROVENANCE: &str = "product/ncm/receipts/backend/2fc72f1d81f543224d8e7d8ef19195b026ba855f.json#/identities/model/revision";
/// Primary model input limit, including the model's special tokens.
pub const MAX_LENGTH: usize = 128;
/// Maximum UTF-8 byte length accepted for one encoder input.
pub const MAX_INPUT_BYTES: usize = 16 * 1024;
/// Runtime manifest filename inside the admitted model directory.
pub const MANIFEST_FILENAME: &str = "ncm-encoder-manifest.json";
/// Version identity for the optional Czech/English keyword affect heuristic.
pub const KEYWORD_AFFECT_SIGNAL_SOURCE: &str = "keyword-heuristic.v1";

/// Applies Biomem's optional Czech/English keyword affect heuristic.
///
/// This is a deterministic text signal, not a measurement of a person's
/// emotional state. Callers that have an explicit four-channel affect vector
/// should preserve it; callers that select no signal should use
/// [`AffectVector::neutral`] instead.
#[must_use]
pub fn extract_keyword_affect(text: &str) -> AffectVector {
    const DOPAMINE: &[&str] = &[
        "radost",
        "úspěch",
        "skvělé",
        "výborně",
        "super",
        "hurá",
        "joy",
        "success",
        "great",
        "excellent",
        "amazing",
        "win",
    ];
    const SEROTONIN_POSITIVE: &[&str] = &[
        "klid",
        "pohoda",
        "spokojenost",
        "mír",
        "harmonie",
        "calm",
        "peace",
        "satisfied",
        "content",
        "balanced",
    ];
    const SEROTONIN_NEGATIVE: &[&str] = &["strach", "úzkost", "panika", "fear", "anxiety", "panic"];
    const CORTISOL: &[&str] = &[
        "chyba",
        "problém",
        "strach",
        "nemoc",
        "špatně",
        "nebezpečí",
        "error",
        "problem",
        "fear",
        "illness",
        "bad",
        "danger",
    ];
    const OXYTOCIN: &[&str] = &[
        "vztah",
        "člověk",
        "pomoc",
        "děkuji",
        "přátelství",
        "láska",
        "relation",
        "help",
        "thank",
        "friendship",
        "love",
        "together",
    ];

    let lowercase = text.to_lowercase();
    AffectVector([
        keyword_channel(&lowercase, DOPAMINE, 0.3, &[], 0.0),
        keyword_channel(
            &lowercase,
            SEROTONIN_POSITIVE,
            0.2,
            SEROTONIN_NEGATIVE,
            -0.15,
        ),
        keyword_channel(&lowercase, CORTISOL, 0.3, &[], 0.0),
        keyword_channel(&lowercase, OXYTOCIN, 0.25, &[], 0.0),
    ])
}

fn keyword_channel(
    lowercase: &str,
    positive: &[&str],
    boost: f32,
    negative: &[&str],
    penalty: f32,
) -> f32 {
    let positive_matches = positive
        .iter()
        .filter(|keyword| lowercase.contains(**keyword))
        .count() as f32;
    let negative_matches = negative
        .iter()
        .filter(|keyword| lowercase.contains(**keyword))
        .count() as f32;
    (1.0 + positive_matches * boost + negative_matches * penalty).clamp(0.5, 2.0)
}

const CACHE_REPOSITORY_DIR: &str = "models--Xenova--paraphrase-multilingual-MiniLM-L12-v2";
const MODEL_OVERRIDE_ENV_VARS: [&str; 3] = ["HF_HOME", "HF_ENDPOINT", "FASTEMBED_CACHE_DIR"];
const REQUIRED_FILES: [&str; 5] = [
    "onnx/model.onnx",
    "tokenizer.json",
    "config.json",
    "special_tokens_map.json",
    "tokenizer_config.json",
];
const REFERENCE_MANIFEST: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../product/ncm/reference/embedding-manifest.json"
));

/// One verified file in an encoder artifact manifest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncoderFile {
    /// Path relative to the model repository snapshot.
    pub path: String,
    /// Lowercase SHA-256 digest of the file bytes.
    pub sha256: String,
    /// Exact file length in bytes.
    pub bytes: u64,
}

/// Pinned model metadata and artifact digests used when opening the encoder.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PinnedEncoder {
    /// Short runtime model identity.
    pub model: String,
    /// Exact model repository used for the runtime artifact.
    pub repository: String,
    /// Immutable repository snapshot revision.
    pub revision: String,
    /// Repository-local evidence binding the revision to a verified receipt.
    pub revision_provenance: String,
    /// Files required to construct and tokenize the model.
    pub files: Vec<EncoderFile>,
    /// Maximum tokenizer sequence length.
    pub max_length: usize,
    /// Sentence pooling mode.
    pub pooling: String,
    /// Whether output vectors are L2 normalized.
    pub normalize: bool,
}

impl PinnedEncoder {
    /// Builds a pinned encoder description.
    #[must_use]
    pub fn new(
        model: impl Into<String>,
        files: Vec<EncoderFile>,
        max_length: usize,
        pooling: impl Into<String>,
        normalize: bool,
    ) -> Self {
        Self {
            model: model.into(),
            repository: MODEL_REPOSITORY.to_owned(),
            revision: MODEL_REVISION.to_owned(),
            revision_provenance: MODEL_REVISION_PROVENANCE.to_owned(),
            files,
            max_length,
            pooling: pooling.into(),
            normalize,
        }
    }

    /// Reads the repository's checked-in reference manifest.
    pub fn reference() -> Result<Self, EncoderError> {
        parse_manifest(REFERENCE_MANIFEST, "checked-in reference manifest")
    }

    /// Alias for [`PinnedEncoder::reference`] for callers that prefer a loader name.
    pub fn load_reference() -> Result<Self, EncoderError> {
        Self::reference()
    }

    /// Reads a manifest from an explicit path.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, EncoderError> {
        read_manifest(path.as_ref(), false)
    }

    /// Returns the pinned ONNX digest, if the manifest contains the model file.
    #[must_use]
    pub fn artifact_sha256(&self) -> Option<&str> {
        self.files
            .iter()
            .find(|file| file.path == REQUIRED_FILES[0])
            .map(|file| file.sha256.as_str())
    }
}

impl Default for PinnedEncoder {
    fn default() -> Self {
        match Self::reference() {
            Ok(manifest) => manifest,
            Err(_) => Self::new(MODEL_NAME, Vec::new(), MAX_LENGTH, "mean", true),
        }
    }
}

/// Returns whether the complete fastembed repository snapshot is locally cached.
///
/// This is the Rust equivalent of Biomem's `_is_model_cached` probe, narrowed
/// to the exact Xenova repository and all files required by this encoder. It
/// only reads the cache and never creates directories or contacts the network.
#[must_use]
pub fn is_model_cached(root: &StateRoot) -> bool {
    if ensure_authoritative_environment().is_err() {
        return false;
    }
    let expected = match PinnedEncoder::reference() {
        Ok(expected) => expected,
        Err(_) => return false,
    };
    let models_dir = root.models_dir();
    verify_cached_state(&models_dir, &expected).is_ok()
}

/// Alias for [`is_model_cached`] that makes the offline nature explicit.
#[must_use]
pub fn offline_probe(root: &StateRoot) -> bool {
    is_model_cached(root)
}

/// Downloads and verifies the pinned model into an admitted state root.
///
/// This is the only function in the embedding module permitted to invoke the
/// fastembed download-capable constructor. The fixed deadline can prevent a
/// download from starting when it is already expired; ONNX and HTTP work are
/// synchronous and cannot be interrupted once started.
pub fn install(root: &StateRoot, deadline: Deadline) -> Result<EncoderIdentity, EncoderError> {
    if deadline.remaining_ms == 0 {
        return Err(EncoderError::Cancelled);
    }
    ensure_authoritative_environment()?;
    let reference = PinnedEncoder::reference()?;
    ensure_manifest_is_pinned(&reference)?;

    #[cfg(feature = "real-encoder")]
    {
        let models_dir = root.models_dir();
        fs::create_dir_all(&models_dir)
            .map_err(|error| EncoderError::Inference(format!("create model directory: {error}")))?;

        let options =
            fastembed::TextInitOptions::new(fastembed::EmbeddingModel::ParaphraseMLMiniLML12V2)
                .with_cache_dir(models_dir.clone())
                .with_max_length(MAX_LENGTH)
                .with_show_download_progress(false);
        let _model = fastembed::TextEmbedding::try_new(options)
            .map_err(|error| EncoderError::Inference(format!("install encoder: {error}")))?;

        if deadline.remaining_ms == 0 {
            return Err(EncoderError::Cancelled);
        }

        // The manifest is derived from the cache after fastembed has proved
        // that it can construct the model. Release the ORT session before
        // reading the large ONNX buffer for the digest.
        drop(_model);
        let manifest = materialize_verified_snapshot(&models_dir, &reference)?;
        ensure_pinned_metadata(&manifest, &reference)?;
        write_manifest(&models_dir, &manifest)?;
        let model = manifest.model.clone();
        let artifact_sha256 = manifest.artifact_sha256().ok_or_else(|| {
            EncoderError::ArtifactMismatch("manifest has no ONNX digest".to_owned())
        })?;
        Ok(EncoderIdentity {
            model,
            artifact_sha256: artifact_sha256.to_owned(),
            max_length: manifest.max_length,
        })
    }

    #[cfg(not(feature = "real-encoder"))]
    {
        let _ = root;
        Err(EncoderError::ArtifactsMissing(
            "real-encoder feature is disabled".to_owned(),
        ))
    }
}

/// Production ONNX encoder for Xenova's multilingual MiniLM export.
#[cfg(feature = "real-encoder")]
pub struct MiniLmEncoder {
    model: Mutex<fastembed::TextEmbedding>,
    identity: EncoderIdentity,
}

/// Offline-only encoder placeholder when real inference is disabled.
#[cfg(not(feature = "real-encoder"))]
pub struct MiniLmEncoder {
    identity: EncoderIdentity,
}

impl MiniLmEncoder {
    /// Opens a verified local encoder without downloading any artifact.
    ///
    /// The `fastembed` constructor itself is synchronous and ONNX Runtime
    /// cannot be interrupted during a batch. The worker's kill-and-respawn
    /// path is the hard deadline for a hung inference; this method only
    /// performs the preflight checks needed to keep opening offline.
    pub fn open(root: &StateRoot, expected: &PinnedEncoder) -> Result<Self, EncoderError> {
        ensure_authoritative_environment()?;
        let reference = PinnedEncoder::reference()?;
        ensure_manifest_is_pinned(&reference)?;
        ensure_pinned_metadata(expected, &reference)?;
        ensure_pinned_metadata(&reference, expected)?;

        let models_dir = root.models_dir();
        let local = read_manifest(&models_dir.join(MANIFEST_FILENAME), true)?;
        ensure_pinned_metadata(&local, expected)?;
        ensure_pinned_metadata(expected, &local)?;
        let artifact_sha256 = local.artifact_sha256().ok_or_else(|| {
            EncoderError::ArtifactMismatch("manifest has no ONNX digest".to_owned())
        })?;
        let identity = EncoderIdentity {
            model: local.model.clone(),
            artifact_sha256: artifact_sha256.to_owned(),
            max_length: local.max_length,
        };

        #[cfg(feature = "real-encoder")]
        {
            // `load_verified_model` reads and verifies each artifact once and
            // passes those exact buffers to fastembed. Keeping verification
            // and construction in one operation closes the verify-then-reopen
            // window that could otherwise load bytes different from the ones
            // that were hashed.
            let model = load_verified_model(&models_dir, &local)?;
            let model = fastembed::TextEmbedding::try_new_from_user_defined(
                model,
                fastembed::InitOptionsUserDefined::new().with_max_length(MAX_LENGTH),
            )
            .map_err(|error| EncoderError::Inference(format!("open verified encoder: {error}")))?;
            Ok(Self {
                model: Mutex::new(model),
                identity,
            })
        }

        #[cfg(not(feature = "real-encoder"))]
        {
            verify_local_artifacts(&models_dir, &local, Some(expected.revision.as_str()))?;
            let _ = identity;
            Err(EncoderError::ArtifactsMissing(
                "real-encoder feature is disabled".to_owned(),
            ))
        }
    }

    /// Returns the identity bound to this verified encoder.
    #[must_use]
    pub fn identity(&self) -> EncoderIdentity {
        self.identity.clone()
    }

    /// Returns the shortest UTF-8 prefix that produces the tokenizer's first
    /// truncated sequence of at most [`MAX_LENGTH`] tokens.
    ///
    /// This helper is useful for proving the truncation boundary without
    /// reimplementing the tokenizer. It is not used to preprocess production
    /// inputs; fastembed owns tokenization and truncation during inference.
    #[cfg(feature = "real-encoder")]
    pub fn truncate_to_model_limit(&self, text: &str) -> Result<String, EncoderError> {
        validate_input(text)?;
        let model = self
            .model
            .lock()
            .map_err(|_| EncoderError::Inference("encoder mutex poisoned".to_owned()))?;
        let encoding = model
            .tokenizer
            .encode(text, true)
            .map_err(|error| EncoderError::Inference(format!("tokenize input: {error}")))?;
        let offsets = encoding.get_offsets();
        let special_tokens = encoding.get_special_tokens_mask();
        let mut end = 0_usize;
        for (offset, special) in offsets.iter().zip(special_tokens.iter()) {
            if *special == 0 && offset.1 > offset.0 {
                end = end.max(offset.1);
            }
        }
        text.get(..end).map(ToOwned::to_owned).ok_or_else(|| {
            EncoderError::Inference("tokenizer offset is not UTF-8 aligned".to_owned())
        })
    }
}

#[cfg(feature = "real-encoder")]
impl TextEncoder for MiniLmEncoder {
    fn identity(&self) -> EncoderIdentity {
        self.identity()
    }

    fn encode(&self, texts: &[&str], deadline: Deadline) -> Result<Vec<Embedding>, EncoderError> {
        if deadline.remaining_ms == 0 {
            return Err(EncoderError::Cancelled);
        }
        for text in texts {
            validate_input(text)?;
        }
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let mut model = self
            .model
            .lock()
            .map_err(|_| EncoderError::Inference("encoder mutex poisoned".to_owned()))?;
        let mut output = Vec::with_capacity(texts.len());
        for batch in texts.chunks(32) {
            if deadline.remaining_ms == 0 {
                return Err(EncoderError::Cancelled);
            }
            let embeddings = model
                .embed(batch, Some(32))
                .map_err(|error| EncoderError::Inference(format!("encode batch: {error}")))?;
            for embedding in embeddings {
                output.push(Embedding::validated(embedding)?);
            }
        }
        Ok(output)
    }
}

#[cfg(not(feature = "real-encoder"))]
impl TextEncoder for MiniLmEncoder {
    fn identity(&self) -> EncoderIdentity {
        self.identity()
    }

    fn encode(&self, _texts: &[&str], _deadline: Deadline) -> Result<Vec<Embedding>, EncoderError> {
        Err(EncoderError::ArtifactsMissing(
            "real-encoder feature is disabled".to_owned(),
        ))
    }
}

/// Named deterministic test doubles for runtime tests that do not need ORT.
pub mod doubles {
    use super::{
        Deadline, Embedding, EncoderError, EncoderIdentity, Sha256, TextEncoder, validate_input,
    };
    use sha2::Digest;
    use tracedecay_memory_ncm_core::types::EMBEDDING_DIM;

    /// Deterministic hash-based 384-D unit-vector test double.
    ///
    /// This type is intentionally not a production fallback. Its identity is
    /// `test-double/hash`, so real-model gates can reject it explicitly.
    #[derive(Clone, Copy, Debug, Default)]
    pub struct HashEncoder;

    impl HashEncoder {
        /// Creates the hash encoder test double.
        #[must_use]
        pub const fn new() -> Self {
            Self
        }

        /// Returns the test-double identity.
        #[must_use]
        pub fn identity(&self) -> EncoderIdentity {
            EncoderIdentity {
                model: "test-double/hash".to_owned(),
                artifact_sha256: "test-double/hash-v1".to_owned(),
                max_length: super::MAX_LENGTH,
            }
        }

        fn encode_one(text: &str) -> Embedding {
            let mut values = Vec::with_capacity(EMBEDDING_DIM);
            for index in 0..EMBEDDING_DIM {
                let mut hasher = Sha256::new();
                hasher.update(b"ncm-hash-encoder-v1\0");
                hasher.update(text.as_bytes());
                hasher.update((index as u64).to_le_bytes());
                let digest = hasher.finalize();
                let raw = u32::from_le_bytes([digest[0], digest[1], digest[2], digest[3]]);
                let unit = (raw as f32 / u32::MAX as f32) * 2.0 - 1.0;
                values.push(unit);
            }
            let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
            if norm == 0.0 {
                values[0] = 1.0;
                return Embedding(values);
            }
            for value in &mut values {
                *value /= norm;
            }
            Embedding(values)
        }
    }

    impl TextEncoder for HashEncoder {
        fn identity(&self) -> EncoderIdentity {
            self.identity()
        }

        fn encode(
            &self,
            texts: &[&str],
            deadline: Deadline,
        ) -> Result<Vec<Embedding>, EncoderError> {
            if deadline.remaining_ms == 0 {
                return Err(EncoderError::Cancelled);
            }
            for text in texts {
                validate_input(text)?;
            }
            Ok(texts.iter().map(|text| Self::encode_one(text)).collect())
        }
    }
}

fn validate_input(text: &str) -> Result<(), EncoderError> {
    if text.len() > MAX_INPUT_BYTES {
        Err(EncoderError::InputTooLarge)
    } else {
        Ok(())
    }
}

fn parse_manifest(contents: &str, source: &str) -> Result<PinnedEncoder, EncoderError> {
    serde_json::from_str(contents)
        .map_err(|error| EncoderError::ArtifactMismatch(format!("parse {source}: {error}")))
}

fn read_manifest(path: &Path, local: bool) -> Result<PinnedEncoder, EncoderError> {
    let bytes = read_path_file(path, local, 16 * 1024 * 1024)?;
    serde_json::from_slice(&bytes).map_err(|error| {
        EncoderError::ArtifactMismatch(format!("parse manifest {}: {error}", path.display()))
    })
}

fn ensure_authoritative_environment() -> Result<(), EncoderError> {
    for variable in MODEL_OVERRIDE_ENV_VARS {
        if std::env::var_os(variable).is_some() {
            return Err(EncoderError::ArtifactMismatch(format!(
                "ambient {variable} override is forbidden; use the admitted state root"
            )));
        }
    }
    Ok(())
}

fn ensure_manifest_is_pinned(manifest: &PinnedEncoder) -> Result<(), EncoderError> {
    validate_manifest_shape(manifest)
}

fn ensure_pinned_metadata(
    actual: &PinnedEncoder,
    expected: &PinnedEncoder,
) -> Result<(), EncoderError> {
    validate_manifest_shape(actual)?;
    validate_manifest_shape(expected)?;
    if actual.model != expected.model
        || actual.repository != expected.repository
        || actual.revision != expected.revision
        || actual.revision_provenance != expected.revision_provenance
        || actual.max_length != expected.max_length
        || actual.pooling != expected.pooling
        || actual.normalize != expected.normalize
        || actual.files.len() != expected.files.len()
    {
        return Err(EncoderError::ArtifactMismatch(
            "encoder manifest metadata differs from pinned manifest".to_owned(),
        ));
    }
    for expected_file in &expected.files {
        let actual_file = actual
            .files
            .iter()
            .find(|file| file.path == expected_file.path)
            .ok_or_else(|| {
                EncoderError::ArtifactMismatch(format!(
                    "manifest is missing pinned file {}",
                    expected_file.path
                ))
            })?;
        if actual_file.sha256 != expected_file.sha256 || actual_file.bytes != expected_file.bytes {
            return Err(EncoderError::ArtifactMismatch(format!(
                "digest or size differs for {}",
                expected_file.path
            )));
        }
    }
    Ok(())
}

fn verify_cached_state(models_dir: &Path, expected: &PinnedEncoder) -> Result<(), EncoderError> {
    ensure_manifest_is_pinned(expected)?;
    let local = read_manifest(&models_dir.join(MANIFEST_FILENAME), true)?;
    ensure_pinned_metadata(&local, expected)?;
    ensure_pinned_metadata(expected, &local)?;
    verify_local_artifacts(models_dir, &local, Some(expected.revision.as_str()))
}

fn validate_manifest_shape(manifest: &PinnedEncoder) -> Result<(), EncoderError> {
    if manifest.model != MODEL_NAME
        || manifest.repository != MODEL_REPOSITORY
        || manifest.revision != MODEL_REVISION
        || manifest.revision_provenance != MODEL_REVISION_PROVENANCE
        || manifest.max_length != MAX_LENGTH
        || manifest.pooling != "mean"
        || !manifest.normalize
        || manifest.files.len() != REQUIRED_FILES.len()
    {
        return Err(EncoderError::ArtifactMismatch(
            "unsupported encoder source, model, pooling, normalization, or sequence length"
                .to_owned(),
        ));
    }
    if !is_immutable_revision(&manifest.revision) {
        return Err(EncoderError::ArtifactMismatch(
            "encoder manifest lacks an immutable Xenova revision".to_owned(),
        ));
    }
    if !is_safe_provenance(&manifest.revision_provenance) {
        return Err(EncoderError::ArtifactMismatch(
            "encoder manifest has invalid revision provenance".to_owned(),
        ));
    }
    for required in REQUIRED_FILES {
        let file = manifest
            .files
            .iter()
            .find(|file| file.path == required)
            .ok_or_else(|| EncoderError::ArtifactMismatch(format!("manifest lacks {required}")))?;
        if !is_safe_relative_path(&file.path)
            || file.bytes == 0
            || file.sha256.len() != 64
            || !file.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
            || file.sha256.bytes().any(|byte| byte.is_ascii_uppercase())
        {
            return Err(EncoderError::ArtifactMismatch(format!(
                "invalid digest metadata for {}",
                file.path
            )));
        }
    }
    Ok(())
}

fn is_safe_relative_path(path: &str) -> bool {
    let candidate = Path::new(path);
    !candidate.is_absolute()
        && candidate
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn is_immutable_revision(revision: &str) -> bool {
    revision.len() == 40
        && revision
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn is_safe_provenance(provenance: &str) -> bool {
    let candidate = Path::new(provenance);
    !provenance.is_empty()
        && !candidate.is_absolute()
        && candidate
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

#[derive(Debug)]
struct CacheSnapshot {
    path: PathBuf,
    directory: Dir,
}

fn cache_snapshot(
    models_dir: &Path,
    expected_revision: Option<&str>,
) -> Result<CacheSnapshot, EncoderError> {
    let model_directory = open_model_directory(models_dir)?;
    let repository_path = models_dir.join(CACHE_REPOSITORY_DIR);
    let repository = open_directory_nofollow(
        &model_directory,
        OsStr::new(CACHE_REPOSITORY_DIR),
        &repository_path,
    )?;
    let refs_path = repository_path.join("refs");
    let refs = open_directory_nofollow(&repository, OsStr::new("refs"), &refs_path)?;
    let revision_path = refs_path.join("main");
    let revision_bytes = read_file_nofollow(&refs, OsStr::new("main"), &revision_path, 256)?;
    let revision = std::str::from_utf8(&revision_bytes).map_err(|error| {
        EncoderError::ArtifactMismatch(format!(
            "read cache revision {}: {error}",
            revision_path.display()
        ))
    })?;
    let revision = revision.trim();
    if !is_immutable_revision(revision) {
        return Err(EncoderError::ArtifactMismatch(
            "invalid cached model revision".to_owned(),
        ));
    }
    if let Some(expected_revision) = expected_revision {
        if revision != expected_revision {
            return Err(EncoderError::ArtifactMismatch(format!(
                "cached model revision {revision} differs from pinned Xenova revision"
            )));
        }
    }
    let snapshots_path = repository_path.join("snapshots");
    let snapshots = open_directory_nofollow(&repository, OsStr::new("snapshots"), &snapshots_path)?;
    let snapshot_path = snapshots_path.join(revision);
    let directory = open_directory_nofollow(&snapshots, OsStr::new(revision), &snapshot_path)?;
    Ok(CacheSnapshot {
        path: snapshot_path,
        directory,
    })
}

#[cfg(feature = "real-encoder")]
fn manifest_from_snapshot(
    snapshot: &CacheSnapshot,
    expected: &PinnedEncoder,
) -> Result<PinnedEncoder, EncoderError> {
    let mut files = Vec::with_capacity(REQUIRED_FILES.len());
    for relative in REQUIRED_FILES {
        let expected_file = expected
            .files
            .iter()
            .find(|file| file.path == relative)
            .ok_or_else(|| {
                EncoderError::ArtifactMismatch(format!("pinned manifest lacks {relative}"))
            })?;
        let bytes = read_snapshot_file(&snapshot, relative, expected_file.bytes)?;
        files.push(EncoderFile {
            path: relative.to_owned(),
            sha256: digest_bytes(&bytes),
            bytes: bytes.len() as u64,
        });
    }
    Ok(PinnedEncoder::new(
        MODEL_NAME, files, MAX_LENGTH, "mean", true,
    ))
}

#[cfg(feature = "real-encoder")]
fn materialize_verified_snapshot(
    models_dir: &Path,
    expected: &PinnedEncoder,
) -> Result<PinnedEncoder, EncoderError> {
    ensure_manifest_is_pinned(expected)?;
    let snapshot = cache_snapshot(models_dir, Some(expected.revision.as_str()))?;
    for file in &expected.files {
        let bytes = read_snapshot_file_following(&snapshot, &file.path, file.bytes)?;
        if bytes.len() as u64 != file.bytes {
            return Err(EncoderError::ArtifactMismatch(format!(
                "size differs for {}",
                file.path
            )));
        }
        if digest_bytes(&bytes) != file.sha256 {
            return Err(EncoderError::ArtifactMismatch(format!(
                "sha256 differs for {}",
                file.path
            )));
        }
        atomically_replace_snapshot_file(&snapshot, &file.path, &bytes)?;
    }
    let manifest = manifest_from_snapshot(&snapshot, expected)?;
    ensure_pinned_metadata(&manifest, expected)?;
    Ok(manifest)
}

fn verify_local_artifacts(
    models_dir: &Path,
    manifest: &PinnedEncoder,
    expected_revision: Option<&str>,
) -> Result<(), EncoderError> {
    let snapshot = cache_snapshot(models_dir, expected_revision)?;
    for file in &manifest.files {
        let _ = read_verified_artifact(&snapshot, file)?;
    }
    Ok(())
}

#[cfg(feature = "real-encoder")]
fn load_verified_model(
    models_dir: &Path,
    manifest: &PinnedEncoder,
) -> Result<fastembed::UserDefinedEmbeddingModel, EncoderError> {
    let snapshot = cache_snapshot(models_dir, Some(manifest.revision.as_str()))?;
    let read_artifact = |relative: &str| {
        let file = manifest
            .files
            .iter()
            .find(|file| file.path == relative)
            .ok_or_else(|| EncoderError::ArtifactMismatch(format!("manifest lacks {relative}")))?;
        read_verified_artifact(&snapshot, file)
    };
    let tokenizer_files = fastembed::TokenizerFiles {
        tokenizer_file: read_artifact("tokenizer.json")?,
        config_file: read_artifact("config.json")?,
        special_tokens_map_file: read_artifact("special_tokens_map.json")?,
        tokenizer_config_file: read_artifact("tokenizer_config.json")?,
    };
    Ok(fastembed::UserDefinedEmbeddingModel::new(
        read_artifact("onnx/model.onnx")?,
        tokenizer_files,
    )
    .with_pooling(fastembed::Pooling::Mean))
}

fn open_model_directory(models_dir: &Path) -> Result<Dir, EncoderError> {
    let parent = models_dir
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let name = models_dir.file_name().ok_or_else(|| {
        EncoderError::ArtifactMismatch(format!(
            "model cache has no directory name: {}",
            models_dir.display()
        ))
    })?;
    let parent_directory = Dir::open_ambient_dir(parent, ambient_authority()).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            EncoderError::ArtifactsMissing(models_dir.display().to_string())
        } else {
            EncoderError::ArtifactMismatch(format!(
                "open model cache parent {}: {error}",
                parent.display()
            ))
        }
    })?;
    open_directory_nofollow(&parent_directory, name, models_dir)
}

fn open_directory_nofollow(parent: &Dir, name: &OsStr, path: &Path) -> Result<Dir, EncoderError> {
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    let file = parent.open_with(name, &options).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            EncoderError::ArtifactsMissing(path.display().to_string())
        } else {
            EncoderError::ArtifactMismatch(format!(
                "open directory {} without following symlinks: {error}",
                path.display()
            ))
        }
    })?;
    let metadata = file.metadata().map_err(|error| {
        EncoderError::ArtifactMismatch(format!("inspect directory {}: {error}", path.display()))
    })?;
    if !metadata.file_type().is_dir() {
        return Err(EncoderError::ArtifactMismatch(format!(
            "cache component {} is not a directory",
            path.display()
        )));
    }
    Ok(Dir::from_std_file(file.into_std()))
}

fn open_file_nofollow(
    parent: &Dir,
    name: &OsStr,
    path: &Path,
) -> Result<cap_std::fs::File, EncoderError> {
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    let file = parent.open_with(name, &options).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            EncoderError::ArtifactsMissing(path.display().to_string())
        } else {
            EncoderError::ArtifactMismatch(format!(
                "open artifact {} without following symlinks: {error}",
                path.display()
            ))
        }
    })?;
    let metadata = file.metadata().map_err(|error| {
        EncoderError::ArtifactMismatch(format!("inspect artifact {}: {error}", path.display()))
    })?;
    if !metadata.file_type().is_file() {
        return Err(EncoderError::ArtifactMismatch(format!(
            "artifact {} is not a regular file",
            path.display()
        )));
    }
    Ok(file)
}

fn read_file_nofollow(
    parent: &Dir,
    name: &OsStr,
    path: &Path,
    maximum_bytes: u64,
) -> Result<Vec<u8>, EncoderError> {
    let file = open_file_nofollow(parent, name, path)?;
    read_open_file(file, path, maximum_bytes)
}

fn read_open_file(
    file: cap_std::fs::File,
    path: &Path,
    maximum_bytes: u64,
) -> Result<Vec<u8>, EncoderError> {
    let mut bytes = Vec::new();
    file.take(maximum_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| {
            EncoderError::ArtifactMismatch(format!("read {}: {error}", path.display()))
        })?;
    if bytes.len() as u64 > maximum_bytes {
        return Err(EncoderError::ArtifactMismatch(format!(
            "artifact {} exceeds the admitted byte bound",
            path.display()
        )));
    }
    Ok(bytes)
}

#[cfg(feature = "real-encoder")]
fn open_file_following(
    parent: &Dir,
    name: &OsStr,
    path: &Path,
) -> Result<cap_std::fs::File, EncoderError> {
    let mut options = OpenOptions::new();
    options.read(true);
    let file = parent.open_with(name, &options).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            EncoderError::ArtifactsMissing(path.display().to_string())
        } else {
            EncoderError::ArtifactMismatch(format!(
                "open installer source artifact {}: {error}",
                path.display()
            ))
        }
    })?;
    let metadata = file.metadata().map_err(|error| {
        EncoderError::ArtifactMismatch(format!(
            "inspect installer source artifact {}: {error}",
            path.display()
        ))
    })?;
    if !metadata.file_type().is_file() {
        return Err(EncoderError::ArtifactMismatch(format!(
            "installer source {} is not a regular file",
            path.display()
        )));
    }
    Ok(file)
}

fn read_path_file(path: &Path, local: bool, maximum_bytes: u64) -> Result<Vec<u8>, EncoderError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let name = path.file_name().ok_or_else(|| {
        EncoderError::ArtifactMismatch(format!("path has no file name: {}", path.display()))
    })?;
    let parent_directory = Dir::open_ambient_dir(parent, ambient_authority()).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound && local {
            EncoderError::ArtifactsMissing(path.display().to_string())
        } else {
            EncoderError::ArtifactMismatch(format!(
                "open manifest parent {}: {error}",
                parent.display()
            ))
        }
    })?;
    match open_file_nofollow(&parent_directory, name, path) {
        Ok(file) => {
            let mut bytes = Vec::new();
            file.take(maximum_bytes.saturating_add(1))
                .read_to_end(&mut bytes)
                .map_err(|error| {
                    EncoderError::ArtifactMismatch(format!("read {}: {error}", path.display()))
                })?;
            if bytes.len() as u64 > maximum_bytes {
                return Err(EncoderError::ArtifactMismatch(format!(
                    "file {} exceeds the admitted byte bound",
                    path.display()
                )));
            }
            Ok(bytes)
        }
        Err(EncoderError::ArtifactsMissing(_)) if local => {
            Err(EncoderError::ArtifactsMissing(path.display().to_string()))
        }
        Err(error) => Err(error),
    }
}

fn read_snapshot_file(
    snapshot: &CacheSnapshot,
    relative: &str,
    maximum_bytes: u64,
) -> Result<Vec<u8>, EncoderError> {
    let (directory, file_name, path) = snapshot_file_parent(snapshot, relative)?;
    read_file_nofollow(&directory, &file_name, &path, maximum_bytes)
}

#[cfg(feature = "real-encoder")]
fn read_snapshot_file_following(
    snapshot: &CacheSnapshot,
    relative: &str,
    maximum_bytes: u64,
) -> Result<Vec<u8>, EncoderError> {
    let (directory, file_name, path) = snapshot_file_parent(snapshot, relative)?;
    let file = open_file_following(&directory, &file_name, &path)?;
    read_open_file(file, &path, maximum_bytes)
}

fn snapshot_file_parent(
    snapshot: &CacheSnapshot,
    relative: &str,
) -> Result<(Dir, OsString, PathBuf), EncoderError> {
    let mut components = Path::new(relative).components().peekable();
    let mut directory = snapshot.directory.try_clone().map_err(|error| {
        EncoderError::ArtifactMismatch(format!(
            "clone snapshot directory {}: {error}",
            snapshot.path.display()
        ))
    })?;
    let mut path = snapshot.path.clone();
    loop {
        let component = components.next().ok_or_else(|| {
            EncoderError::ArtifactMismatch(format!(
                "artifact path is empty in {}",
                snapshot.path.display()
            ))
        })?;
        let Component::Normal(name) = component else {
            return Err(EncoderError::ArtifactMismatch(format!(
                "artifact path {relative} is not safely relative"
            )));
        };
        path.push(name);
        if components.peek().is_some() {
            directory = open_directory_nofollow(&directory, name, &path)?;
        } else {
            return Ok((directory, name.to_os_string(), path));
        }
    }
}

#[cfg(feature = "real-encoder")]
fn atomically_replace_snapshot_file(
    snapshot: &CacheSnapshot,
    relative: &str,
    bytes: &[u8],
) -> Result<(), EncoderError> {
    let (directory, file_name, path) = snapshot_file_parent(snapshot, relative)?;
    let file_stem = file_name.to_string_lossy();
    for attempt in 0..32_u32 {
        let temporary_name = OsString::from(format!(
            ".ncm-materialize-{file_stem}-{}-{attempt}.tmp",
            std::process::id()
        ));
        let mut options = OpenOptions::new();
        options
            .write(true)
            .create_new(true)
            .follow(FollowSymlinks::No);
        let mut temporary = match directory.open_with(&temporary_name, &options) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(EncoderError::Inference(format!(
                    "create materialized artifact temporary file for {}: {error}",
                    path.display()
                )));
            }
        };
        let write_result = temporary
            .write_all(bytes)
            .and_then(|()| temporary.sync_all());
        drop(temporary);
        if let Err(error) = write_result {
            let _ = directory.remove_file(&temporary_name);
            return Err(EncoderError::Inference(format!(
                "write materialized artifact {}: {error}",
                path.display()
            )));
        }
        return directory
            .rename(&temporary_name, &directory, &file_name)
            .map_err(|error| {
                let _ = directory.remove_file(&temporary_name);
                EncoderError::Inference(format!(
                    "publish materialized artifact {}: {error}",
                    path.display()
                ))
            });
    }
    Err(EncoderError::Inference(format!(
        "unable to allocate materialized artifact temporary file for {}",
        path.display()
    )))
}

fn read_verified_artifact(
    snapshot: &CacheSnapshot,
    file: &EncoderFile,
) -> Result<Vec<u8>, EncoderError> {
    let bytes = read_snapshot_file(snapshot, &file.path, file.bytes)?;
    if bytes.len() as u64 != file.bytes {
        return Err(EncoderError::ArtifactMismatch(format!(
            "size differs for {}",
            file.path
        )));
    }
    if digest_bytes(&bytes) != file.sha256 {
        return Err(EncoderError::ArtifactMismatch(format!(
            "sha256 differs for {}",
            file.path
        )));
    }
    Ok(bytes)
}

fn digest_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        output.push(char::from(b"0123456789abcdef"[(byte >> 4) as usize]));
        output.push(char::from(b"0123456789abcdef"[(byte & 0x0f) as usize]));
    }
    output
}

#[cfg(all(test, unix))]
mod tests {
    use super::{
        CACHE_REPOSITORY_DIR, EncoderFile, MODEL_REVISION, cache_snapshot, digest_bytes,
        read_snapshot_file, read_verified_artifact,
    };
    #[cfg(feature = "real-encoder")]
    use super::{MAX_LENGTH, MODEL_NAME};
    use std::fs;
    use std::os::unix::fs::symlink;

    fn cache_root() -> tempfile::TempDir {
        let root = tempfile::tempdir().expect("create model cache root");
        let snapshot = root
            .path()
            .join("models")
            .join(CACHE_REPOSITORY_DIR)
            .join("snapshots")
            .join(MODEL_REVISION);
        fs::create_dir_all(&snapshot).expect("create model snapshot");
        let refs = root
            .path()
            .join("models")
            .join(CACHE_REPOSITORY_DIR)
            .join("refs");
        fs::create_dir_all(&refs).expect("create model refs");
        fs::write(refs.join("main"), MODEL_REVISION).expect("write model revision");
        root
    }

    #[test]
    fn cache_snapshot_refuses_symlinked_repository_components() {
        let root = tempfile::tempdir().expect("create model cache root");
        let models = root.path().join("models");
        let outside = root.path().join("outside");
        fs::create_dir_all(&models).expect("create models directory");
        fs::create_dir_all(outside.join("refs")).expect("create outside refs");
        fs::write(outside.join("refs/main"), MODEL_REVISION).expect("write outside revision");
        symlink(&outside, models.join(CACHE_REPOSITORY_DIR)).expect("create repository symlink");

        let error = cache_snapshot(&models, Some(MODEL_REVISION))
            .expect_err("a symlinked repository must be rejected");
        assert!(matches!(error, super::EncoderError::ArtifactMismatch(_)));
    }

    #[test]
    fn snapshot_file_refuses_symlinked_artifacts() {
        let root = cache_root();
        let models = root.path().join("models");
        let snapshot = cache_snapshot(&models, Some(MODEL_REVISION)).expect("open cache snapshot");
        let outside = root.path().join("outside-tokenizer.json");
        fs::write(&outside, b"verified bytes").expect("write outside artifact");
        symlink(&outside, snapshot.path.join("tokenizer.json")).expect("create artifact symlink");

        let error = read_snapshot_file(&snapshot, "tokenizer.json", 64)
            .expect_err("a symlinked artifact must be rejected");
        assert!(matches!(error, super::EncoderError::ArtifactMismatch(_)));
    }

    #[test]
    fn verified_artifact_hashes_the_same_bytes_that_are_returned() {
        let root = cache_root();
        let models = root.path().join("models");
        let snapshot = cache_snapshot(&models, Some(MODEL_REVISION)).expect("open cache snapshot");
        let bytes = b"verified bytes";
        let path = snapshot.path.join("tokenizer.json");
        fs::write(&path, bytes).expect("write artifact");
        let file = EncoderFile {
            path: "tokenizer.json".to_owned(),
            sha256: digest_bytes(bytes),
            bytes: bytes.len() as u64,
        };

        assert_eq!(
            read_verified_artifact(&snapshot, &file).expect("verify artifact"),
            bytes
        );
    }

    #[cfg(feature = "real-encoder")]
    #[test]
    fn user_defined_model_contains_the_verified_bytes() {
        let root = cache_root();
        let models = root.path().join("models");
        let snapshot = cache_snapshot(&models, Some(MODEL_REVISION)).expect("open cache snapshot");
        fs::create_dir(snapshot.path.join("onnx")).expect("create onnx directory");

        let artifacts = [
            ("onnx/model.onnx", b"verified onnx bytes".as_slice()),
            ("tokenizer.json", b"verified tokenizer bytes".as_slice()),
            ("config.json", b"verified config bytes".as_slice()),
            (
                "special_tokens_map.json",
                b"verified special-token bytes".as_slice(),
            ),
            (
                "tokenizer_config.json",
                b"verified tokenizer-config bytes".as_slice(),
            ),
        ];
        let files = artifacts
            .iter()
            .map(|(relative, bytes)| {
                let path = snapshot.path.join(relative);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent).expect("create artifact parent");
                }
                fs::write(&path, bytes).expect("write artifact");
                EncoderFile {
                    path: (*relative).to_owned(),
                    sha256: digest_bytes(bytes),
                    bytes: bytes.len() as u64,
                }
            })
            .collect::<Vec<_>>();
        let manifest = super::PinnedEncoder::new(MODEL_NAME, files, MAX_LENGTH, "mean", true);
        let model = super::load_verified_model(&models, &manifest).expect("load verified model");

        assert_eq!(model.onnx_file, artifacts[0].1);
        assert_eq!(model.tokenizer_files.tokenizer_file, artifacts[1].1);
        assert_eq!(model.tokenizer_files.config_file, artifacts[2].1);
        assert_eq!(
            model.tokenizer_files.special_tokens_map_file,
            artifacts[3].1
        );
        assert_eq!(model.tokenizer_files.tokenizer_config_file, artifacts[4].1);
    }

    #[cfg(feature = "real-encoder")]
    #[test]
    fn installer_materializes_fastembed_symlinks_into_regular_files() {
        let root = cache_root();
        let models = root.path().join("models");
        let snapshot = cache_snapshot(&models, Some(MODEL_REVISION)).expect("open cache snapshot");
        let blobs = models.join(CACHE_REPOSITORY_DIR).join("blobs");
        fs::create_dir_all(&blobs).expect("create fastembed blob directory");

        let artifacts = [
            ("onnx/model.onnx", b"installer onnx bytes".as_slice()),
            ("tokenizer.json", b"installer tokenizer bytes".as_slice()),
            ("config.json", b"installer config bytes".as_slice()),
            (
                "special_tokens_map.json",
                b"installer special-token bytes".as_slice(),
            ),
            (
                "tokenizer_config.json",
                b"installer tokenizer-config bytes".as_slice(),
            ),
        ];
        let files = artifacts
            .iter()
            .map(|(relative, bytes)| {
                let digest = digest_bytes(bytes);
                fs::write(blobs.join(&digest), bytes).expect("write fastembed blob");
                let path = snapshot.path.join(relative);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent).expect("create snapshot artifact parent");
                }
                let target = if relative.starts_with("onnx/") {
                    format!("../../../blobs/{digest}")
                } else {
                    format!("../../blobs/{digest}")
                };
                symlink(target, &path).expect("create fastembed snapshot symlink");
                EncoderFile {
                    path: (*relative).to_owned(),
                    sha256: digest,
                    bytes: bytes.len() as u64,
                }
            })
            .collect::<Vec<_>>();
        let expected = super::PinnedEncoder::new(MODEL_NAME, files, MAX_LENGTH, "mean", true);
        let manifest = super::materialize_verified_snapshot(&models, &expected)
            .expect("materialize and verify installer snapshot");
        assert_eq!(manifest, expected);

        for (relative, bytes) in artifacts {
            let path = snapshot.path.join(relative);
            let metadata = fs::symlink_metadata(&path).expect("inspect materialized artifact");
            assert!(metadata.file_type().is_file());
            assert!(!metadata.file_type().is_symlink());
            assert_eq!(fs::read(path).expect("read materialized artifact"), bytes);
        }
    }
}

#[cfg(feature = "real-encoder")]
fn write_manifest(models_dir: &Path, manifest: &PinnedEncoder) -> Result<(), EncoderError> {
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|error| EncoderError::Inference(format!("serialize encoder manifest: {error}")))?;
    let directory = open_model_directory(models_dir)?;
    let file_name = OsStr::new(MANIFEST_FILENAME);
    for attempt in 0..32_u32 {
        let temporary_name = OsString::from(format!(
            ".{MANIFEST_FILENAME}.{}-{attempt}.tmp",
            std::process::id()
        ));
        let mut options = OpenOptions::new();
        options
            .write(true)
            .create_new(true)
            .follow(FollowSymlinks::No);
        let mut temporary = match directory.open_with(&temporary_name, &options) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(EncoderError::Inference(format!(
                    "create encoder manifest temporary file: {error}"
                )));
            }
        };
        let write_result = temporary
            .write_all(&bytes)
            .and_then(|()| temporary.sync_all());
        drop(temporary);
        if let Err(error) = write_result {
            let _ = directory.remove_file(&temporary_name);
            return Err(EncoderError::Inference(format!(
                "write encoder manifest: {error}"
            )));
        }
        return directory
            .rename(&temporary_name, &directory, file_name)
            .map_err(|error| {
                let _ = directory.remove_file(&temporary_name);
                EncoderError::Inference(format!("publish encoder manifest: {error}"))
            });
    }
    Err(EncoderError::Inference(
        "unable to allocate encoder manifest temporary file".to_owned(),
    ))
}
