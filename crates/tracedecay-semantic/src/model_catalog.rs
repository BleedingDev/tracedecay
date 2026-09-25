//! Immutable model pins for the CPU FastEmbed code-search runtime.
//!
//! The catalog is the one production declaration for a model: it pins the
//! upstream revision, license, member paths, lengths, and digests. The
//! lifecycle verifies those members before publication, and the runtime only
//! accepts the resulting local artifact.
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracedecay_domain::EmbeddingPrecisionV1;
use tracedecay_domain::canonical_text::encode_lowercase_hex;
use tracedecay_semantic_contracts::{ArtifactMemberRoleV1, DEFAULT_FASTEMBED_MODEL_ID};

use super::embedding_backend::EmbeddingRuntimeFamilyV1;

const CATALOG_SCHEMA_V1: &str = "tracedecay.fastembed.model-catalog.v1";

/// One immutable package member pin.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogMemberPinV1 {
    pub path: String,
    pub upstream_path: String,
    pub length: u64,
    pub sha256: String,
}

/// Provenance for a cataloged model.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogSourceV1 {
    pub upstream: String,
    pub revision: String,
    pub license: String,
    pub license_url: String,
    pub provenance: String,
}

/// The only supported embedding backend in this crate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "runtime", rename_all = "snake_case", deny_unknown_fields)]
pub enum CatalogedEmbeddingBackendV1 {
    /// `FastEmbed` over the CPU ONNX Runtime provider. `fastembed_enum`
    /// names the upstream `EmbeddingModel` variant for the pinned package.
    FastEmbedOrt { fastembed_enum: String },
}

impl CatalogedEmbeddingBackendV1 {
    pub fn runtime_family(&self) -> EmbeddingRuntimeFamilyV1 {
        match self {
            Self::FastEmbedOrt { .. } => EmbeddingRuntimeFamilyV1::FastEmbedOrt,
        }
    }

    pub fn precision(&self) -> EmbeddingPrecisionV1 {
        EmbeddingPrecisionV1::Fp32
    }

    /// Member roles required before the CPU runtime may open a session.
    pub fn required_member_roles(&self) -> &'static [&'static str] {
        match self {
            Self::FastEmbedOrt { .. } => &[
                "model",
                "tokenizer",
                "config",
                "special_tokens_map",
                "tokenizer_config",
            ],
        }
    }
}

/// One supported embedding model that settings may select.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogedFastEmbedModelV1 {
    pub model_id: String,
    pub backend: CatalogedEmbeddingBackendV1,
    pub model_code: String,
    pub source: CatalogSourceV1,
    pub expected_dimensions: u32,
    pub max_length: u32,
    pub members: BTreeMap<String, CatalogMemberPinV1>,
}

/// Versioned catalog of supported embedding models.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FastEmbedModelCatalogV1 {
    pub schema: String,
    pub models: Vec<CatalogedFastEmbedModelV1>,
}

/// Production catalog used by settings validation and daemon acquisition.
pub fn production_fastembed_catalog() -> FastEmbedModelCatalogV1 {
    FastEmbedModelCatalogV1::production()
}

/// Return the pinned embedding width for one production catalog model.
///
/// Runtime composition uses this narrow lookup to size projection admission
/// without copying catalog identity or accepting an uncataloged model.
pub fn production_model_dimensions(model_id: &str) -> Result<u32, CatalogErrorV1> {
    FastEmbedModelCatalogV1::production()
        .get(model_id)
        .map(|model| model.expected_dimensions)
        .ok_or(CatalogErrorV1::UnknownModel)
}

/// Admit a configured `selected_model` against the production catalog.
pub fn admit_production_model_selection(
    selected_model: Option<&str>,
) -> Result<(), CatalogErrorV1> {
    FastEmbedModelCatalogV1::production().admit_selected_model(selected_model)
}

impl FastEmbedModelCatalogV1 {
    pub fn production() -> Self {
        Self {
            schema: CATALOG_SCHEMA_V1.to_owned(),
            models: vec![jina_embeddings_v2_base_code()],
        }
    }

    pub fn get(&self, model_id: &str) -> Option<&CatalogedFastEmbedModelV1> {
        self.models.iter().find(|model| model.model_id == model_id)
    }

    pub fn model_ids(&self) -> impl Iterator<Item = &str> {
        self.models.iter().map(|model| model.model_id.as_str())
    }

    pub fn admit_selected_model(&self, selected_model: Option<&str>) -> Result<(), CatalogErrorV1> {
        match selected_model {
            Some(model_id) if self.get(model_id).is_none() => Err(CatalogErrorV1::UnknownModel),
            _ => Ok(()),
        }
    }

    pub fn validate(&self) -> Result<(), CatalogErrorV1> {
        if self.schema != CATALOG_SCHEMA_V1 {
            return Err(CatalogErrorV1::InvalidSchema);
        }
        if self.models.is_empty() {
            return Err(CatalogErrorV1::Empty);
        }
        let mut seen = BTreeSet::new();
        for model in &self.models {
            validate_model(model)?;
            if !seen.insert(model.model_id.as_str()) {
                return Err(CatalogErrorV1::DuplicateModelId);
            }
        }
        if self.get(DEFAULT_FASTEMBED_MODEL_ID).is_none() {
            return Err(CatalogErrorV1::MissingDefault);
        }
        Ok(())
    }
}

/// Catalog construction and lookup failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum CatalogErrorV1 {
    #[error("semantic model catalog schema is invalid")]
    InvalidSchema,
    #[error("semantic model catalog is empty")]
    Empty,
    #[error("semantic model catalog has a duplicate model id")]
    DuplicateModelId,
    #[error("semantic model catalog omits the default model")]
    MissingDefault,
    #[error("semantic model catalog entry is invalid")]
    InvalidEntry,
    #[error("selected semantic model is not in the catalog")]
    UnknownModel,
}

fn validate_model(model: &CatalogedFastEmbedModelV1) -> Result<(), CatalogErrorV1> {
    if !is_safe_catalog_component(&model.model_id, 128)
        || model.model_code.trim().is_empty()
        || model.expected_dimensions == 0
        || model.max_length == 0
        || model.members.is_empty()
        || model.source.upstream.trim().is_empty()
        || model.source.revision.trim().is_empty()
        || model.source.license.trim().is_empty()
        || model.source.license_url.trim().is_empty()
        || model.source.provenance.trim().is_empty()
    {
        return Err(CatalogErrorV1::InvalidEntry);
    }
    let CatalogedEmbeddingBackendV1::FastEmbedOrt { fastembed_enum } = &model.backend;
    if fastembed_enum.trim().is_empty() {
        return Err(CatalogErrorV1::InvalidEntry);
    }
    if model
        .backend
        .required_member_roles()
        .iter()
        .any(|role| !model.members.contains_key(*role))
    {
        return Err(CatalogErrorV1::InvalidEntry);
    }
    if model.source.revision.len() != 40
        || !model
            .source
            .revision
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(CatalogErrorV1::InvalidEntry);
    }
    for (role, member) in &model.members {
        if catalog_member_role(role).is_none()
            || !is_portable_catalog_path(&member.path)
            || !is_portable_catalog_path(&member.upstream_path)
            || member.length == 0
            || member.sha256.len() != 64
            || !member
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(CatalogErrorV1::InvalidEntry);
        }
    }
    Ok(())
}

/// Catalog model IDs become directory components during lifecycle staging and
/// installation. Keep them a single portable component so a catalog cannot
/// redirect those writes outside the daemon's artifact root.
fn is_safe_catalog_component(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value != "."
        && value != ".."
        && !value.starts_with('.')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// Catalog member paths are copied into private installs and later opened
/// relative to that install. Use the same portable path vocabulary as the
/// persisted manifest and reject platform-specific separators.
fn is_portable_catalog_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 512
        && !path.starts_with('/')
        && !path.contains('\\')
        && path.split('/').all(|segment| {
            !segment.is_empty()
                && segment != "."
                && segment != ".."
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        })
}

/// Production pin for FastEmbed's `JinaEmbeddingsV2BaseCode` model.
///
/// These lengths and digests are the immutable distribution fixture pins.
fn jina_embeddings_v2_base_code() -> CatalogedFastEmbedModelV1 {
    let mut members = BTreeMap::new();
    members.insert(
        "model".to_owned(),
        CatalogMemberPinV1 {
            path: "model.onnx".to_owned(),
            upstream_path: "onnx/model.onnx".to_owned(),
            length: 641_517_466,
            sha256: "63363fc178428b74620c6f3780cbc7191883fa5c7f84c0945c45eb5c4256733b".to_owned(),
        },
    );
    members.insert(
        "tokenizer".to_owned(),
        CatalogMemberPinV1 {
            path: "tokenizer.json".to_owned(),
            upstream_path: "tokenizer.json".to_owned(),
            length: 2_561_316,
            sha256: "b01c78a902aa4facb2f47f95449f48e2f7bbfea5d2472ee2f6ce92323c6f86e5".to_owned(),
        },
    );
    members.insert(
        "config".to_owned(),
        CatalogMemberPinV1 {
            path: "config.json".to_owned(),
            upstream_path: "config.json".to_owned(),
            length: 1_216,
            sha256: "e426aa684c7f9a95c5f020aa855faf93a24f065f5fad0c9e17b124670cabdea6".to_owned(),
        },
    );
    members.insert(
        "special_tokens_map".to_owned(),
        CatalogMemberPinV1 {
            path: "special_tokens_map.json".to_owned(),
            upstream_path: "special_tokens_map.json".to_owned(),
            length: 280,
            sha256: "06e405a36dfe4b9604f484f6a1e619af1a7f7d09e34a8555eb0b77b66318067f".to_owned(),
        },
    );
    members.insert(
        "tokenizer_config".to_owned(),
        CatalogMemberPinV1 {
            path: "tokenizer_config.json".to_owned(),
            upstream_path: "tokenizer_config.json".to_owned(),
            length: 493,
            sha256: "f477aeb15ff9f78d3c1ddf2361d2b0b8b20cf55220f839f29a37f3a18efddd89".to_owned(),
        },
    );
    CatalogedFastEmbedModelV1 {
        model_id: DEFAULT_FASTEMBED_MODEL_ID.to_owned(),
        backend: CatalogedEmbeddingBackendV1::FastEmbedOrt {
            fastembed_enum: "JinaEmbeddingsV2BaseCode".to_owned(),
        },
        model_code: "jinaai/jina-embeddings-v2-base-code".to_owned(),
        source: CatalogSourceV1 {
            upstream: "https://huggingface.co/jinaai/jina-embeddings-v2-base-code".to_owned(),
            revision: "516f4baf13dec4ddddda8631e019b5737c8bc250".to_owned(),
            license: "Apache-2.0".to_owned(),
            license_url: "https://www.apache.org/licenses/LICENSE-2.0".to_owned(),
            provenance:
                "https://huggingface.co/jinaai/jina-embeddings-v2-base-code/tree/516f4baf13dec4ddddda8631e019b5737c8bc250"
                    .to_owned(),
        },
        expected_dimensions: 768,
        max_length: 8192,
        members,
    }
}

/// Map a catalog member role name onto the manifest member vocabulary.
pub fn catalog_member_role(name: &str) -> Option<ArtifactMemberRoleV1> {
    match name {
        "model" => Some(ArtifactMemberRoleV1::Model),
        "tokenizer" => Some(ArtifactMemberRoleV1::Tokenizer),
        "config" => Some(ArtifactMemberRoleV1::Config),
        "special_tokens_map" => Some(ArtifactMemberRoleV1::SpecialTokensMap),
        "tokenizer_config" => Some(ArtifactMemberRoleV1::TokenizerConfig),
        _ => None,
    }
}

/// Package digest identity over the complete catalog identity.
///
/// Backend selection, model metadata, and source provenance are part of the
/// serving contract just as much as the member bytes. Keeping them in this
/// digest prevents two catalogs that happen to point at the same files from
/// sharing a lifecycle install or a vector projection when their runtime
/// semantics or provenance differ.
pub fn catalog_package_digest(model: &CatalogedFastEmbedModelV1) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"tracedecay.fastembed.catalog-package.v2\0");
    hasher.update(model.model_id.as_bytes());
    hasher.update(b"\0");
    match &model.backend {
        CatalogedEmbeddingBackendV1::FastEmbedOrt { fastembed_enum } => {
            hasher.update(b"fastembed_ort\0");
            hasher.update(fastembed_enum.as_bytes());
            hasher.update(b"\0");
        }
    }
    hasher.update(model.model_code.as_bytes());
    hasher.update(b"\0");
    hasher.update(model.source.upstream.as_bytes());
    hasher.update(b"\0");
    hasher.update(model.source.revision.as_bytes());
    hasher.update(b"\0");
    hasher.update(model.source.license.as_bytes());
    hasher.update(b"\0");
    hasher.update(model.source.license_url.as_bytes());
    hasher.update(b"\0");
    hasher.update(model.source.provenance.as_bytes());
    hasher.update(b"\0");
    hasher.update(model.expected_dimensions.to_le_bytes());
    hasher.update(model.max_length.to_le_bytes());
    hasher.update(b"\0");
    for (role, member) in &model.members {
        hasher.update(role.as_bytes());
        hasher.update(b"\0");
        hasher.update(member.path.as_bytes());
        hasher.update(b"\0");
        hasher.update(member.upstream_path.as_bytes());
        hasher.update(b"\0");
        hasher.update(member.length.to_le_bytes());
        hasher.update(member.sha256.as_bytes());
        hasher.update(b"\0");
    }
    encode_lowercase_hex(&hasher.finalize())
}

#[cfg(test)]
mod tests {
    use tracedecay_semantic_contracts::SemanticConfig;

    use super::*;

    #[test]
    fn production_catalog_pins_default_jina_code_model() {
        let catalog = FastEmbedModelCatalogV1::production();
        catalog.validate().expect("production catalog");
        let model = catalog
            .get(DEFAULT_FASTEMBED_MODEL_ID)
            .expect("default model");
        assert_eq!(model.expected_dimensions, 768);
        assert_eq!(model.max_length, 8192);
        assert_eq!(model.members.len(), 5);
        assert_eq!(
            model.backend.runtime_family(),
            EmbeddingRuntimeFamilyV1::FastEmbedOrt
        );
        assert_eq!(model.backend.precision(), EmbeddingPrecisionV1::Fp32);
        assert_eq!(model.members["model"].length, 641_517_466);
        assert_eq!(model.members["model"].sha256.len(), 64);
        assert_eq!(catalog_package_digest(model), catalog_package_digest(model));
    }

    #[test]
    fn catalog_identity_includes_backend_metadata_and_provenance() {
        let catalog = FastEmbedModelCatalogV1::production();
        let model = catalog
            .get(DEFAULT_FASTEMBED_MODEL_ID)
            .expect("default model");
        let identity = catalog_package_digest(model);

        let mut backend = model.clone();
        backend.backend = CatalogedEmbeddingBackendV1::FastEmbedOrt {
            fastembed_enum: "DifferentEmbeddingModel".to_owned(),
        };
        assert_ne!(catalog_package_digest(&backend), identity);

        let mut metadata = model.clone();
        metadata.model_code.push_str("-metadata-change");
        metadata.expected_dimensions += 1;
        metadata.max_length += 1;
        assert_ne!(catalog_package_digest(&metadata), identity);

        let mut provenance = model.clone();
        provenance.source.provenance.push_str("?provenance-change");
        assert_ne!(catalog_package_digest(&provenance), identity);
    }

    #[test]
    fn production_catalog_admits_exactly_its_own_model_ids() {
        let catalog = FastEmbedModelCatalogV1::production();
        for model_id in catalog.model_ids() {
            let config = SemanticConfig {
                enabled: true,
                ..SemanticConfig::default()
            };
            config.validate().expect("semantic configuration is valid");
            assert_eq!(config.effective_model_id(), Some(model_id));
            admit_production_model_selection(config.effective_model_id())
                .expect("catalog id is admitted");
        }
        assert_eq!(
            admit_production_model_selection(Some("NotARealModel")),
            Err(CatalogErrorV1::UnknownModel)
        );
        assert_eq!(admit_production_model_selection(None), Ok(()));
    }

    #[test]
    fn production_dimensions_come_only_from_the_catalog_entry() {
        let expected = FastEmbedModelCatalogV1::production()
            .get(DEFAULT_FASTEMBED_MODEL_ID)
            .expect("default model")
            .expected_dimensions;
        assert_eq!(
            production_model_dimensions(DEFAULT_FASTEMBED_MODEL_ID),
            Ok(expected)
        );
        assert_eq!(
            production_model_dimensions("NotARealModel"),
            Err(CatalogErrorV1::UnknownModel)
        );
    }

    #[test]
    fn validation_rejects_missing_members_and_unsafe_paths() {
        let mut catalog = FastEmbedModelCatalogV1::production();
        let model = catalog.models.first_mut().expect("model");
        model.members.remove("special_tokens_map");
        assert_eq!(catalog.validate(), Err(CatalogErrorV1::InvalidEntry));

        let mut catalog = FastEmbedModelCatalogV1::production();
        let model = catalog.models.first_mut().expect("model");
        model.members.get_mut("model").expect("model pin").path = "../model.onnx".to_owned();
        assert_eq!(catalog.validate(), Err(CatalogErrorV1::InvalidEntry));
    }

    #[test]
    fn unknown_model_is_rejected_by_lookup() {
        assert!(
            FastEmbedModelCatalogV1::production()
                .get("NotARealModel")
                .is_none()
        );
    }
}
