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
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
#[cfg(feature = "real-encoder")]
use std::sync::Mutex;
use tracedecay_memory_ncm_core::types::AffectVector;

/// Short model identity used by the runtime and readiness receipts.
pub const MODEL_NAME: &str = "paraphrase-multilingual-MiniLM-L12-v2";
/// Hugging Face repository used by the pinned fastembed model definition.
pub const MODEL_REPOSITORY: &str = "Xenova/paraphrase-multilingual-MiniLM-L12-v2";
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
    let models_dir = root.models_dir();
    let snapshot = match cache_snapshot(&models_dir) {
        Ok(path) => path,
        Err(_) => return false,
    };
    REQUIRED_FILES
        .iter()
        .all(|relative| snapshot.join(relative).is_file())
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

        let manifest = manifest_from_cache(&models_dir)?;
        let reference = PinnedEncoder::reference()?;
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
        let reference = PinnedEncoder::reference()?;
        ensure_pinned_metadata(expected, &reference)?;
        ensure_pinned_metadata(&reference, expected)?;

        let models_dir = root.models_dir();
        let local = read_manifest(&models_dir.join(MANIFEST_FILENAME), true)?;
        ensure_pinned_metadata(&local, expected)?;
        ensure_pinned_metadata(expected, &local)?;
        verify_local_artifacts(&models_dir, &local)?;
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
            let options =
                fastembed::TextInitOptions::new(fastembed::EmbeddingModel::ParaphraseMLMiniLML12V2)
                    .with_cache_dir(models_dir)
                    .with_max_length(MAX_LENGTH)
                    .with_show_download_progress(false);
            let model = fastembed::TextEmbedding::try_new(options).map_err(|error| {
                EncoderError::Inference(format!("open verified encoder: {error}"))
            })?;
            Ok(Self {
                model: Mutex::new(model),
                identity,
            })
        }

        #[cfg(not(feature = "real-encoder"))]
        {
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
    let bytes = fs::read(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound && local {
            EncoderError::ArtifactsMissing(path.display().to_string())
        } else {
            EncoderError::ArtifactMismatch(format!("read manifest {}: {error}", path.display()))
        }
    })?;
    serde_json::from_slice(&bytes).map_err(|error| {
        EncoderError::ArtifactMismatch(format!("parse manifest {}: {error}", path.display()))
    })
}

fn ensure_pinned_metadata(
    actual: &PinnedEncoder,
    expected: &PinnedEncoder,
) -> Result<(), EncoderError> {
    validate_manifest_shape(actual)?;
    validate_manifest_shape(expected)?;
    if actual.model != expected.model
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

fn validate_manifest_shape(manifest: &PinnedEncoder) -> Result<(), EncoderError> {
    if manifest.model != MODEL_NAME
        || manifest.max_length != MAX_LENGTH
        || manifest.pooling != "mean"
        || !manifest.normalize
        || manifest.files.len() != REQUIRED_FILES.len()
    {
        return Err(EncoderError::ArtifactMismatch(
            "unsupported encoder model, pooling, normalization, or sequence length".to_owned(),
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

fn cache_snapshot(models_dir: &Path) -> Result<PathBuf, EncoderError> {
    let repository = models_dir.join(CACHE_REPOSITORY_DIR);
    let revision_path = repository.join("refs").join("main");
    let revision = fs::read_to_string(&revision_path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            EncoderError::ArtifactsMissing(revision_path.display().to_string())
        } else {
            EncoderError::ArtifactMismatch(format!("read cache revision: {error}"))
        }
    })?;
    let revision = revision.trim();
    if revision.is_empty()
        || Path::new(revision)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(EncoderError::ArtifactMismatch(
            "invalid cached model revision".to_owned(),
        ));
    }
    let snapshot = repository.join("snapshots").join(revision);
    if !snapshot.is_dir() {
        return Err(EncoderError::ArtifactsMissing(
            snapshot.display().to_string(),
        ));
    }
    Ok(snapshot)
}

#[cfg(feature = "real-encoder")]
fn manifest_from_cache(models_dir: &Path) -> Result<PinnedEncoder, EncoderError> {
    let snapshot = cache_snapshot(models_dir)?;
    let mut files = Vec::with_capacity(REQUIRED_FILES.len());
    for relative in REQUIRED_FILES {
        let path = snapshot.join(relative);
        let metadata = fs::metadata(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                EncoderError::ArtifactsMissing(path.display().to_string())
            } else {
                EncoderError::Inference(format!("inspect downloaded artifact: {error}"))
            }
        })?;
        let digest = digest_file(&path)?;
        files.push(EncoderFile {
            path: relative.to_owned(),
            sha256: digest,
            bytes: metadata.len(),
        });
    }
    Ok(PinnedEncoder::new(
        MODEL_NAME, files, MAX_LENGTH, "mean", true,
    ))
}

fn verify_local_artifacts(models_dir: &Path, manifest: &PinnedEncoder) -> Result<(), EncoderError> {
    let snapshot = cache_snapshot(models_dir)?;
    for file in &manifest.files {
        let path = snapshot.join(&file.path);
        let metadata = fs::metadata(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                EncoderError::ArtifactsMissing(path.display().to_string())
            } else {
                EncoderError::ArtifactMismatch(format!("inspect {}: {error}", file.path))
            }
        })?;
        if metadata.len() != file.bytes {
            return Err(EncoderError::ArtifactMismatch(format!(
                "size differs for {}",
                file.path
            )));
        }
        let digest = digest_file(&path)?;
        if digest != file.sha256 {
            return Err(EncoderError::ArtifactMismatch(format!(
                "sha256 differs for {}",
                file.path
            )));
        }
    }
    Ok(())
}

fn digest_file(path: &Path) -> Result<String, EncoderError> {
    let mut file = fs::File::open(path).map_err(|error| {
        EncoderError::ArtifactMismatch(format!("open {}: {error}", path.display()))
    })?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).map_err(|error| {
            EncoderError::ArtifactMismatch(format!("read {}: {error}", path.display()))
        })?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    let digest = hasher.finalize();
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        output.push(char::from(b"0123456789abcdef"[(byte >> 4) as usize]));
        output.push(char::from(b"0123456789abcdef"[(byte & 0x0f) as usize]));
    }
    Ok(output)
}

#[cfg(feature = "real-encoder")]
fn write_manifest(models_dir: &Path, manifest: &PinnedEncoder) -> Result<(), EncoderError> {
    let path = models_dir.join(MANIFEST_FILENAME);
    let temporary = models_dir.join(format!(".{MANIFEST_FILENAME}.{}.tmp", std::process::id()));
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|error| EncoderError::Inference(format!("serialize encoder manifest: {error}")))?;
    fs::write(&temporary, bytes)
        .map_err(|error| EncoderError::Inference(format!("write encoder manifest: {error}")))?;
    fs::rename(&temporary, &path)
        .map_err(|error| EncoderError::Inference(format!("publish encoder manifest: {error}")))
}
