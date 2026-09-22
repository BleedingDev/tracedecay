//! Daemon-owned `FastEmbed` model acquisition lifecycle.
//!
//! Settings select a cataloged model (default [`DEFAULT_FASTEMBED_MODEL_ID`]).
//! Installation stays offline-safe. Project open only records the catalog
//! selection; strict semantic demand may queue acquisition of the immutable
//! revision in the background. The request never waits for model bytes and no
//! ambient hub or cache becomes serving authority.
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::watch;
use tracedecay_domain::canonical_text::encode_lowercase_hex;
use tracedecay_semantic_contracts::{
    ArtifactMemberPinV1, ArtifactMemberRoleV1, ArtifactPackageMemberV1, ArtifactProfileKindV1,
    DEFAULT_FASTEMBED_MODEL_ID, MODEL_ARTIFACT_MANIFEST_SCHEMA_V1, ModelArtifactManifestPayloadV1,
    ModelArtifactManifestV1, PlatformTargetV1, RuntimeCompatibilityV1,
    SemanticLifecycleVerifiedReadyEventV1, SemanticModelLifecycleStateV1,
    SemanticModelLifecycleStatusV1, SemanticModelRemediationV1, SemanticResourceCeilings,
    Sha256DigestHex, TruncationPolicyV1, UpstreamSourceV1,
};

#[cfg(feature = "semantic-fastembed")]
use hf_hub::{Cache, Repo, RepoType, api::sync::ApiBuilder};

use super::artifact_store::{
    ArtifactImportErrorV1, ArtifactInventoryRecordV1, ArtifactInventoryStateV1,
    ArtifactLeaseKindV1, ArtifactLeaseV1, GcReceiptV1, ModelArtifactStore, RetentionPolicyV1,
};
use super::model_catalog::{
    CatalogErrorV1, CatalogedFastEmbedModelV1, FastEmbedModelCatalogV1, catalog_package_digest,
};

const LIFECYCLE_SCHEMA_V1: &str = "tracedecay.fastembed.model-lifecycle.v1";
const INSTALL_META_SCHEMA_V1: &str = "tracedecay.fastembed.model-install.v1";
const ARTIFACT_GC_LEASE_SECONDS: u64 = 5 * 60;
const HF_HUB_CACHE_DIRECTORY_V1: &str = "hf-hub-cache";
const EMBEDDING_ACTIVE_LEASE_ID_V1: &str = "embedding:active:v1";
const EMBEDDING_ROLLBACK_LEASE_ID_V1: &str = "embedding:rollback:v1";
static LIFECYCLE_PRIVATE_PATH_NONCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableLifecycleV1 {
    schema: String,
    selected_model: Option<String>,
    auto_download: bool,
    state: Option<SemanticModelLifecycleStateV1>,
    previous_ready: Option<SemanticModelLifecycleStateV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstallMetaV1 {
    schema: String,
    model_id: String,
    revision: String,
    artifact_digest: String,
}

/// Errors from lifecycle ownership operations.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ModelLifecycleErrorV1 {
    #[error(transparent)]
    Catalog(#[from] CatalogErrorV1),
    #[error("semantic model lifecycle store is unavailable")]
    StoreUnavailable,
    #[error("semantic model lifecycle operation rejected")]
    Rejected,
    #[error("semantic model download failed")]
    DownloadFailed,
    #[error("semantic model download failed: {0}")]
    DownloadFailedWithReason(String),
    #[error("semantic model verification failed")]
    VerificationFailed,
    #[error("semantic reranker is unavailable")]
    RerankerUnavailable,
    #[error("semantic model install failed")]
    InstallFailed,
    #[error("semantic model acquisition worker failed while joining")]
    WorkerJoinFailed,
    #[error("semantic model acquisition was cancelled")]
    Cancelled,
    #[error("cancelled semantic model acquisition was quarantined at {0}")]
    CancellationCleanupQuarantined(PathBuf),
    #[error("cancelled semantic model acquisition cleanup failed for {0}")]
    CancellationCleanupFailed(PathBuf),
    #[error(transparent)]
    ArtifactImport(#[from] ArtifactImportErrorV1),
}

#[derive(Default)]
struct AcquisitionControlV1 {
    state: Mutex<AcquisitionControlStateV1>,
}

#[derive(Default)]
struct AcquisitionControlStateV1 {
    epoch: u64,
    cancelled: bool,
}

#[derive(Clone)]
struct AcquisitionEpochV1 {
    control: Arc<AcquisitionControlV1>,
    epoch: u64,
}

impl AcquisitionControlV1 {
    fn begin_epoch(self: &Arc<Self>) -> AcquisitionEpochV1 {
        let epoch = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.epoch = state.epoch.wrapping_add(1);
            state.cancelled = false;
            state.epoch
        };
        AcquisitionEpochV1 {
            control: Arc::clone(self),
            epoch,
        }
    }

    fn cancel_current(&self) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cancelled = true;
    }
}

impl AcquisitionEpochV1 {
    fn ensure_active(&self) -> Result<(), ModelLifecycleErrorV1> {
        let state = self
            .control
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if state.epoch == self.epoch && !state.cancelled {
            Ok(())
        } else {
            Err(ModelLifecycleErrorV1::Cancelled)
        }
    }

    fn while_active<T>(
        &self,
        operation: impl FnOnce() -> Result<T, ModelLifecycleErrorV1>,
    ) -> Result<T, ModelLifecycleErrorV1> {
        let state = self
            .control
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if state.epoch != self.epoch || state.cancelled {
            return Err(ModelLifecycleErrorV1::Cancelled);
        }
        operation()
    }

    fn while_current<T>(
        &self,
        operation: impl FnOnce() -> Result<T, ModelLifecycleErrorV1>,
    ) -> Result<T, ModelLifecycleErrorV1> {
        let state = self
            .control
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if state.epoch != self.epoch {
            return Err(ModelLifecycleErrorV1::Cancelled);
        }
        operation()
    }
}

/// Supplies package member bytes for a cataloged model.
///
/// Production uses the daemon-owned hub source against the catalog's immutable
/// repository revision. Tests may inject a fixture source through this port.
pub trait ModelMemberSourceV1: Send + Sync {
    fn fetch_member(
        &self,
        model: &CatalogedFastEmbedModelV1,
        upstream_path: &str,
        destination: &Path,
    ) -> Result<(), ModelLifecycleErrorV1>;
}

/// Daemon-owned Hugging Face source scoped to the lifecycle root.
///
/// The client never uses `FastEmbed`'s ambient cache discovery: it resolves the
/// cataloged repository and immutable revision into this explicit cache, then
/// the lifecycle independently checks every member's length and SHA-256 before
/// atomically publishing an install.
#[derive(Debug)]
pub struct HfHubModelMemberSourceV1 {
    cache_dir: PathBuf,
    endpoint: Option<String>,
    offline: bool,
}

impl HfHubModelMemberSourceV1 {
    fn new(cache_dir: PathBuf) -> Self {
        Self {
            cache_dir,
            endpoint: None,
            offline: hf_hub_offline(),
        }
    }

    #[cfg(all(test, feature = "semantic-fastembed", not(windows)))]
    fn new_for_tests(cache_dir: PathBuf, endpoint: Option<String>, offline: bool) -> Self {
        Self {
            cache_dir,
            endpoint,
            offline,
        }
    }
}

impl ModelMemberSourceV1 for HfHubModelMemberSourceV1 {
    #[hotpath::measure(label = "semantic.model_lifecycle.fetch_member")]
    fn fetch_member(
        &self,
        model: &CatalogedFastEmbedModelV1,
        upstream_path: &str,
        destination: &Path,
    ) -> Result<(), ModelLifecycleErrorV1> {
        fetch_hf_hub_member(
            &self.cache_dir,
            self.endpoint.as_deref(),
            self.offline,
            model,
            upstream_path,
            destination,
        )
    }
}

// The hub source is lifecycle-owned and compiles only with the optional
// FastEmbed acquisition feature. Runtime session opening never calls it.
#[cfg(feature = "semantic-fastembed")]
fn fetch_hf_hub_member(
    cache_dir: &Path,
    endpoint: Option<&str>,
    offline: bool,
    model: &CatalogedFastEmbedModelV1,
    upstream_path: &str,
    destination: &Path,
) -> Result<(), ModelLifecycleErrorV1> {
    ensure_lifecycle_directory(cache_dir).map_err(|_| ModelLifecycleErrorV1::StoreUnavailable)?;
    let cache = Cache::new(cache_dir.to_path_buf());
    let repository = Repo::with_revision(
        model.model_code.clone(),
        RepoType::Model,
        model.source.revision.clone(),
    );
    let cached = cache.repo(repository.clone()).get(upstream_path);
    let source = match cached {
        Some(path) => {
            crate::hotpath_observe::record_artifact_cache(true);
            path
        }
        None if offline => {
            crate::hotpath_observe::record_artifact_cache(false);
            crate::hotpath_observe::record_remote_failure("offline_cache_miss");
            return Err(ModelLifecycleErrorV1::DownloadFailedWithReason(format!(
                "member '{upstream_path}' is absent from the private cache while offline mode is enabled"
            )));
        }
        None => {
            crate::hotpath_observe::record_artifact_cache(false);
            let mut builder = ApiBuilder::from_cache(cache)
                .with_token(None)
                .with_progress(false)
                .with_retries(3);
            if let Some(endpoint) = endpoint {
                builder = builder.with_endpoint(endpoint.to_owned());
            }
            hotpath::measure_block!("semantic.hf_hub.download", {
                builder
                    .build()
                    .map_err(|error| {
                        crate::hotpath_observe::record_remote_failure("download_failed");
                        ModelLifecycleErrorV1::DownloadFailedWithReason(format!(
                            "cannot initialize the Hugging Face client for '{}': {error}",
                            model.model_code
                        ))
                    })?
                    .repo(repository)
                    .get(upstream_path)
                    .map_err(|error| {
                        crate::hotpath_observe::record_remote_failure("download_failed");
                        ModelLifecycleErrorV1::DownloadFailedWithReason(format!(
                            "cannot acquire '{}@{}/{}': {error}",
                            model.model_code, model.source.revision, upstream_path
                        ))
                    })?
            })
        }
    };
    if let Some(parent) = destination.parent() {
        ensure_lifecycle_directory(parent).map_err(|_| ModelLifecycleErrorV1::StoreUnavailable)?;
    }
    hotpath::measure_block!("semantic.hf_hub.decode", {
        let mut source = File::open(source).map_err(|error| {
            crate::hotpath_observe::record_remote_failure("download_failed");
            ModelLifecycleErrorV1::DownloadFailedWithReason(format!(
                "cannot open cached member '{upstream_path}' for staging: {error}"
            ))
        })?;
        let mut destination = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(destination)
            .map_err(|error| {
                crate::hotpath_observe::record_remote_failure("download_failed");
                ModelLifecycleErrorV1::DownloadFailedWithReason(format!(
                    "cannot create staging member '{upstream_path}': {error}"
                ))
            })?;
        io::copy(&mut source, &mut destination)
            .and_then(|_| destination.sync_all())
            .map_err(|error| {
                crate::hotpath_observe::record_remote_failure("download_failed");
                ModelLifecycleErrorV1::DownloadFailedWithReason(format!(
                    "cannot copy cached member '{upstream_path}' into staging: {error}"
                ))
            })
    })
}

fn hf_hub_offline() -> bool {
    std::env::var("HF_HUB_OFFLINE")
        .is_ok_and(|value| !value.is_empty() && !matches!(value.as_str(), "0" | "false" | "FALSE"))
}

#[cfg(not(feature = "semantic-fastembed"))]
fn fetch_hf_hub_member(
    cache_dir: &Path,
    endpoint: Option<&str>,
    offline: bool,
    model: &CatalogedFastEmbedModelV1,
    upstream_path: &str,
    destination: &Path,
) -> Result<(), ModelLifecycleErrorV1> {
    let _ = (
        cache_dir,
        endpoint,
        offline,
        model,
        upstream_path,
        destination,
    );
    crate::hotpath_observe::record_remote_failure("rejected");
    Err(ModelLifecycleErrorV1::Rejected)
}

include!("model_lifecycle/owner.rs");
include!("model_lifecycle/reconciliation.rs");
include!("model_lifecycle/acquisition.rs");
include!("model_lifecycle/persistence.rs");
include!("model_lifecycle/local_evaluation.rs");

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use sha2::{Digest, Sha256};

    use super::*;

    #[test]
    fn lifecycle_state_helpers_preserve_identity_fields() {
        let state = SemanticModelLifecycleStateV1::Ready {
            model_id: "JinaEmbeddingsV2BaseCode".to_owned(),
            revision: "516f4baf13dec4ddddda8631e019b5737c8bc250".to_owned(),
            artifact_digest: "sha256:artifact".to_owned(),
            install_path: PathBuf::from("/var/lib/tracedecay/semantic-models/install"),
        };
        assert_eq!(state.model_id(), "JinaEmbeddingsV2BaseCode");
        assert_eq!(state.artifact_digest(), "sha256:artifact");
        assert!(!state.omits_semantics());
        assert!(state.remediation().rollback);
    }

    #[test]
    fn private_install_is_reverified_before_ready_on_restart_and_rollback() {
        let root = tempfile::tempdir().expect("lifecycle root");
        let mut catalog = FastEmbedModelCatalogV1::production();
        let model = catalog.models.first_mut().expect("default model");
        let mut fixture_members = Vec::new();
        for (role, member) in &mut model.members {
            let bytes = format!("private-install-fixture:{role}").into_bytes();
            member.length = bytes.len() as u64;
            member.sha256 = encode_lowercase_hex(&Sha256::digest(&bytes));
            fixture_members.push((member.path.clone(), bytes));
        }
        let model_id = model.model_id.clone();
        let revision = model.source.revision.clone();
        let artifact_digest = catalog_package_digest(model);
        let source = Arc::new(HfHubModelMemberSourceV1::new(
            root.path().join("hf-cache"),
        ));
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog.clone(),
            source.clone(),
        )
        .expect("open lifecycle owner");

        owner
            .select_model(Some(&model_id), false)
            .expect("select fixture model");
        let install_path = install_path_for(root.path(), &model_id, &revision, &artifact_digest);
        fs::create_dir_all(&install_path).expect("private install directory");
        for (path, bytes) in fixture_members {
            fs::write(install_path.join(path), bytes).expect("private install member");
        }
        let metadata = InstallMetaV1 {
            schema: INSTALL_META_SCHEMA_V1.to_owned(),
            model_id: model_id.clone(),
            revision: revision.clone(),
            artifact_digest: artifact_digest.clone(),
        };
        write_json_atomic(&install_path.join("install.json"), &metadata)
            .expect("private install metadata");

        let installed = owner
            .select_model(Some(&model_id), false)
            .expect("re-admit private install");
        assert!(matches!(
            installed.state,
            Some(SemanticModelLifecycleStateV1::Installed { .. })
        ));
        owner.mark_ready().expect("mark verified install ready");
        {
            let mut guard = owner.inner.writer();
            guard.durable.previous_ready = guard.durable.state.clone();
            persist_durable(&owner.root, &guard.durable).expect("persist rollback pointer");
        }

        let model_path = install_path.join("model.onnx");
        let mut corrupted = fs::read(&model_path).expect("read model fixture");
        corrupted[0] ^= 0xff;
        fs::write(&model_path, corrupted).expect("tamper private install");

        assert_eq!(
            owner.rollback_to_previous(),
            Err(ModelLifecycleErrorV1::VerificationFailed),
            "rollback must reverify private bytes before activating Ready"
        );
        drop(owner);

        let reopened = SemanticModelLifecycleOwnerV1::open(root.path(), catalog, source)
            .expect("reopen lifecycle owner");
        assert!(matches!(
            reopened.status().state,
            Some(SemanticModelLifecycleStateV1::SelectedNotDownloaded { .. })
        ));
        assert!(!reopened.status().remediation.rollback);
        assert_eq!(
            reopened.verified_ready_events().borrow().artifact_digest,
            None,
            "restart must not expose a tampered private install as Ready"
        );
    }
}
