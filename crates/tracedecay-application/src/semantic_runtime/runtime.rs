//! Per-project ownership for the optional semantic retrieval runtime.

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use thiserror::Error;
use tracedecay_code_index::production::CodeIndexPublishedGenerationV1;
use tracedecay_domain::{
    AdmittedEmbeddingProjectionKeyV1, CodeGenerationId, EmbeddingDocumentCompositionV1,
    ManifestDigest, SemanticSearchIndexKeyV1, SemanticSearchIndexProfileV1,
};
use tracedecay_query::retrieval::semantic::{
    CompleteSemanticGenerationV1, SemanticCalibrationProfileV1, SemanticIndexStateV1,
};
use tracedecay_runtime_core::cancellation::CancellationToken;
use tracedecay_runtime_core::db::Database;
use tracedecay_semantic::{
    DaemonSemanticRuntimeHandleV1, LoadedSemanticArtifactV1, ModelLifecycleErrorV1,
    SemanticModelLifecycleOwnerV1, SemanticRuntimeRestoreRollbackV1,
    SemanticRuntimeShutdownReceiptV1, production_model_dimensions,
};
use tracedecay_semantic_contracts::{
    ModelArtifactManifestV1, SemanticConfig, SemanticGenerationPointerV1,
    SemanticLifecycleVerifiedReadyEventV1, SemanticModelLifecycleStatusV1,
    SemanticRuntimeScheduleFailureV1, SemanticRuntimeStatusProjectionV1,
};
use tracedecay_store::runtime::{DurableVectorAuthorityStoreV1, VectorAuthorityStoreErrorV1};

use super::vector_read::PublishedVectorReadPortV1;

#[derive(Debug, Error)]
pub enum ProjectSemanticRuntimeErrorV1 {
    #[error("invalid semantic runtime configuration: {0}")]
    Configuration(String),
    #[error(transparent)]
    Lifecycle(#[from] ModelLifecycleErrorV1),
    #[error("semantic runtime operation failed: {0}")]
    Runtime(SemanticRuntimeScheduleFailureV1),
    #[error(transparent)]
    VectorStore(#[from] VectorAuthorityStoreErrorV1),
    #[error(transparent)]
    VectorAuthority(#[from] tracedecay_vector_authority::VectorAuthorityError),
    #[error(transparent)]
    Database(#[from] tracedecay_domain::errors::TraceDecayError),
    #[error("semantic runtime blocking task failed")]
    TaskJoin,
}

impl From<SemanticRuntimeScheduleFailureV1> for ProjectSemanticRuntimeErrorV1 {
    fn from(error: SemanticRuntimeScheduleFailureV1) -> Self {
        Self::Runtime(error)
    }
}

#[derive(Debug)]
pub struct ProjectSemanticRuntimeShutdownReceiptV1 {
    pub runtime: SemanticRuntimeShutdownReceiptV1,
    pub model_acquisition_joined: bool,
}

#[derive(Clone, Debug)]
pub struct SemanticModelAcquisitionV1 {
    pub queued: bool,
    pub lifecycle: SemanticModelLifecycleStatusV1,
}

#[derive(Clone)]
pub struct ProjectSemanticRuntimeV1 {
    pub(super) config: SemanticConfig,
    pub(super) lifecycle: Arc<SemanticModelLifecycleOwnerV1>,
    pub(super) handle: DaemonSemanticRuntimeHandleV1,
    pub(super) vector_store: Arc<dyn DurableVectorAuthorityStoreV1>,
    pub(super) query_snapshot: Arc<Mutex<Option<Arc<ProjectSemanticQuerySnapshotV1>>>>,
    pub(super) cancellation: CancellationToken,
}

#[derive(Clone)]
pub(super) struct ProjectSemanticQuerySnapshotV1 {
    pub pointer: SemanticGenerationPointerV1,
    pub vectors: Arc<PublishedVectorReadPortV1>,
    pub projection: AdmittedEmbeddingProjectionKeyV1,
    pub search_index_key: SemanticSearchIndexKeyV1,
    pub calibration: SemanticCalibrationProfileV1,
    pub generation: CompleteSemanticGenerationV1,
    pub capability_manifest_digest: ManifestDigest,
}

impl ProjectSemanticRuntimeV1 {
    pub async fn open(
        config: SemanticConfig,
        semantic_selection_root: impl Into<PathBuf>,
        shared_semantic_artifact_root: impl Into<PathBuf>,
        lease_namespace: &str,
        database: Arc<Database>,
        cancellation: CancellationToken,
    ) -> Result<Self, ProjectSemanticRuntimeErrorV1> {
        config
            .validate()
            .map_err(|error| ProjectSemanticRuntimeErrorV1::Configuration(error.to_string()))?;
        let model_id = config.effective_model_id().ok_or_else(|| {
            ProjectSemanticRuntimeErrorV1::Configuration(
                "cannot open a disabled semantic runtime".to_owned(),
            )
        })?;
        let memory_ceiling_bytes = config
            .resources
            .resolved_max_resident_bytes()
            .map_err(|error| ProjectSemanticRuntimeErrorV1::Configuration(error.to_string()))?;
        let max_sessions =
            usize::try_from(config.resources.max_concurrent_sessions).map_err(|_| {
                ProjectSemanticRuntimeErrorV1::Configuration(
                    "semantic max_concurrent_sessions does not fit this target".to_owned(),
                )
            })?;
        let dimensions = production_model_dimensions(model_id)
            .map_err(|error| ProjectSemanticRuntimeErrorV1::Configuration(error.to_string()))?;
        let scalar_bytes = u64::try_from(std::mem::size_of::<f32>()).map_err(|_| {
            ProjectSemanticRuntimeErrorV1::Configuration(
                "semantic vector scalar width does not fit this target".to_owned(),
            )
        })?;
        let dense_vector_bytes =
            u64::from(dimensions)
                .checked_mul(scalar_bytes)
                .ok_or_else(|| {
                    ProjectSemanticRuntimeErrorV1::Configuration(
                        "semantic dense-vector byte width overflowed".to_owned(),
                    )
                })?;
        // This is a necessary upper bound from the admitted resident ceiling:
        // every indexed chunk owns at least one dense f32 vector. The runtime
        // and vector authority enforce their additional costs independently.
        let max_projection_units = memory_ceiling_bytes / dense_vector_bytes;
        if max_projection_units == 0 {
            return Err(ProjectSemanticRuntimeErrorV1::Configuration(
                "semantic resident ceiling cannot hold one dense vector".to_owned(),
            ));
        }
        let max_projection_units = usize::try_from(max_projection_units).map_err(|_| {
            ProjectSemanticRuntimeErrorV1::Configuration(
                "semantic projection bound does not fit this target".to_owned(),
            )
        })?;
        let vector_database = Arc::clone(&database);
        let vector_authority_namespace = lease_namespace.to_owned();
        let vector_store = tokio::task::spawn_blocking(move || {
            open_vector_store(&vector_database, &vector_authority_namespace)
        })
        .await
        .map_err(|_| ProjectSemanticRuntimeErrorV1::TaskJoin)??;
        let selection_root: PathBuf = semantic_selection_root.into();
        let artifact_root: PathBuf = shared_semantic_artifact_root.into();
        let lifecycle_namespace = lease_namespace.to_owned();
        let lifecycle = Arc::new(
            tokio::task::spawn_blocking(move || {
                SemanticModelLifecycleOwnerV1::open_scoped_default(
                    selection_root,
                    artifact_root,
                    &lifecycle_namespace,
                )
            })
            .await
            .map_err(|_| ProjectSemanticRuntimeErrorV1::TaskJoin)??,
        );
        let selection = lifecycle.configuration_selection_guard().await;
        let lifecycle_configuration = Arc::clone(&lifecycle);
        let selected_model = model_id.to_owned();
        let auto_download = config.auto_download;
        tokio::task::spawn_blocking(move || {
            let _selection = selection;
            lifecycle_configuration.select_model(Some(&selected_model), auto_download)?;
            if auto_download {
                lifecycle_configuration.enqueue_demand_acquisition_if_needed()?;
            }
            Ok::<(), ModelLifecycleErrorV1>(())
        })
        .await
        .map_err(|_| ProjectSemanticRuntimeErrorV1::TaskJoin)??;
        let handle = DaemonSemanticRuntimeHandleV1::new(
            max_sessions,
            max_projection_units,
            memory_ceiling_bytes,
        )?;
        Ok(Self {
            config,
            lifecycle,
            handle,
            vector_store,
            query_snapshot: Arc::new(Mutex::new(None)),
            cancellation,
        })
    }

    pub fn status(&self) -> SemanticRuntimeStatusProjectionV1 {
        self.handle.status_projection()
    }

    pub fn model_status(&self) -> SemanticModelLifecycleStatusV1 {
        self.lifecycle.status()
    }

    pub fn model_ready_events(
        &self,
    ) -> tokio::sync::watch::Receiver<SemanticLifecycleVerifiedReadyEventV1> {
        self.lifecycle.verified_ready_events()
    }

    /// Observe completion of the scheduler's serialized publication commit.
    /// A generation refused during that short commit window can be retried
    /// after this receiver advances without polling projection failures.
    pub fn schedule_commit_completions(&self) -> tokio::sync::watch::Receiver<u64> {
        self.handle.schedule_commit_completions()
    }

    pub fn current_source(&self) -> Option<CodeGenerationId> {
        self.handle
            .current()
            .map(|pointer| pointer.source_generation)
    }

    pub(super) fn query_snapshot(
        &self,
    ) -> Result<Option<Arc<ProjectSemanticQuerySnapshotV1>>, SemanticIndexStateV1> {
        self.query_snapshot
            .lock()
            .map(|snapshot| snapshot.clone())
            .map_err(|_| SemanticIndexStateV1::Failed)
    }

    pub fn acquire_model(&self) -> Result<SemanticModelAcquisitionV1, ModelLifecycleErrorV1> {
        let queued = self.lifecycle.begin_explicit_acquisition()?;
        Ok(SemanticModelAcquisitionV1 {
            queued,
            lifecycle: self.lifecycle.status(),
        })
    }

    pub fn import_model(
        &self,
        manifest: &ModelArtifactManifestV1,
        source: &Path,
        now_unix: u64,
    ) -> Result<SemanticModelLifecycleStatusV1, ModelLifecycleErrorV1> {
        let model_id = self
            .config
            .effective_model_id()
            .ok_or(ModelLifecycleErrorV1::Rejected)?;
        self.lifecycle
            .import_local_artifact(model_id, manifest, source, now_unix)
    }

    pub async fn restore(
        &self,
        generation: Arc<CodeIndexPublishedGenerationV1>,
    ) -> Result<bool, ProjectSemanticRuntimeErrorV1> {
        if !self.config.enabled {
            return Ok(false);
        }
        if self.cancellation.is_cancelled() {
            return Err(SemanticRuntimeScheduleFailureV1::Cancelled.into());
        }
        let handle = self.handle.clone();
        let lifecycle = Arc::clone(&self.lifecycle);
        let vector_store = self.vector_store.clone();
        let query_snapshot = Arc::clone(&self.query_snapshot);
        let resources = self.config.resources;
        let document_composition = self.config.document_composition;
        tokio::task::spawn_blocking(move || {
            restore_current(
                handle,
                lifecycle,
                vector_store,
                query_snapshot,
                generation,
                resources,
                document_composition,
            )
        })
        .await
        .map_err(|_| ProjectSemanticRuntimeErrorV1::TaskJoin)?
    }

    pub async fn shutdown(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<ProjectSemanticRuntimeShutdownReceiptV1, ProjectSemanticRuntimeErrorV1> {
        self.cancellation.cancel();
        self.handle.begin_shutdown();
        let lifecycle = Arc::clone(&self.lifecycle);
        let lifecycle_deadline = deadline.into_std();
        let lifecycle_join = tokio::task::spawn_blocking(move || {
            lifecycle.cancel_and_join_background_acquisition_until(lifecycle_deadline)
        });
        let runtime = self.handle.cancel_and_join_until(deadline).await;
        let model_acquisition_joined = lifecycle_join
            .await
            .map_err(|_| ProjectSemanticRuntimeErrorV1::TaskJoin)??;
        Ok(ProjectSemanticRuntimeShutdownReceiptV1 {
            runtime,
            model_acquisition_joined,
        })
    }
}

pub(super) fn restore_current(
    handle: DaemonSemanticRuntimeHandleV1,
    lifecycle: Arc<SemanticModelLifecycleOwnerV1>,
    vector_store: Arc<dyn DurableVectorAuthorityStoreV1>,
    query_snapshot: Arc<Mutex<Option<Arc<ProjectSemanticQuerySnapshotV1>>>>,
    code_generation: Arc<CodeIndexPublishedGenerationV1>,
    resources: tracedecay_semantic_contracts::SemanticResourceCeilings,
    document_composition: EmbeddingDocumentCompositionV1,
) -> Result<bool, ProjectSemanticRuntimeErrorV1> {
    let snapshot = vector_store.snapshot()?;
    let Some(generation_id) = snapshot.active_generation().cloned() else {
        return Ok(false);
    };
    let Some(generation) = snapshot.generation(&generation_id) else {
        return Err(VectorAuthorityStoreErrorV1::Corrupt(
            "active semantic vector generation is missing".to_owned(),
        )
        .into());
    };
    let projection = LoadedSemanticArtifactV1::lifecycle_projection(
        &lifecycle,
        code_generation.manifest(),
        resources,
        document_composition,
    )?;
    if generation.embedding_key() != &projection {
        return Ok(false);
    }
    let target = lifecycle
        .lifecycle_mutation_target_for_projection(&projection)
        .ok_or(SemanticRuntimeScheduleFailureV1::Artifact)?;
    let artifact =
        LoadedSemanticArtifactV1::from_lifecycle_projection(&lifecycle, &projection, resources)?;
    let pointer = SemanticGenerationPointerV1 {
        generation: generation_id.clone(),
        source_generation: generation.source_generation().clone(),
        projection_key: generation.projection_key().clone(),
    };
    let prepared_query = prepare_query_snapshot(&snapshot, &pointer, code_generation)?;
    let prepared_pointer = pointer.clone();
    let prepared = handle.prepare_restore(pointer, artifact)?;
    let current_snapshot = vector_store.snapshot()?;
    if current_snapshot.active_generation() != Some(&generation_id) {
        return Ok(false);
    }
    let rollback = RefCell::new(None::<SemanticRuntimeRestoreRollbackV1>);
    let committed = lifecycle.commit_runtime_ready_with_rollback(
        &target,
        || {
            *rollback.borrow_mut() = handle.commit_restore_with_rollback(prepared);
            rollback.borrow().is_some()
        },
        || {
            if let Some(rollback) = rollback.borrow_mut().take() {
                rollback.rollback();
            }
        },
    )?;
    if committed && handle.current().as_ref() == Some(&prepared_pointer) {
        let mut query_snapshot = query_snapshot.lock().map_err(|_| {
            ProjectSemanticRuntimeErrorV1::Runtime(SemanticRuntimeScheduleFailureV1::Runtime)
        })?;
        *query_snapshot = Some(prepared_query);
    }
    Ok(committed)
}

pub(super) fn prepare_query_snapshot(
    authority: &tracedecay_store::runtime::VectorGenerationAuthority,
    pointer: &SemanticGenerationPointerV1,
    code_generation: Arc<CodeIndexPublishedGenerationV1>,
) -> Result<Arc<ProjectSemanticQuerySnapshotV1>, SemanticRuntimeScheduleFailureV1> {
    let vectors = authority
        .generation_read_snapshot(&pointer.generation)
        .map_err(SemanticRuntimeScheduleFailureV1::publication)?
        .ok_or(SemanticRuntimeScheduleFailureV1::Publication)?;
    if vectors.generation_id() != &pointer.generation
        || vectors.compatibility().source_generation != pointer.source_generation
        || vectors.compatibility().projection_key != pointer.projection_key
    {
        return Err(SemanticRuntimeScheduleFailureV1::Publication);
    }
    let projection = vectors.embedding_key().clone();
    let search_index_key = SemanticSearchIndexProfileV1::exact_flat_v1()
        .and_then(|profile| profile.index_key())
        .map_err(SemanticRuntimeScheduleFailureV1::publication)?;
    let capability_manifest_digest = code_generation.capability().manifest_digest.clone();
    let vectors = Arc::new(
        PublishedVectorReadPortV1::new(vectors, code_generation, search_index_key.clone())
            .map_err(SemanticRuntimeScheduleFailureV1::publication)?,
    );
    let generation = CompleteSemanticGenerationV1::new(
        pointer.projection_key.clone(),
        search_index_key.clone(),
        pointer.generation.clone(),
        pointer.source_generation.clone(),
        capability_manifest_digest.clone(),
    )
    .map_err(|_| SemanticRuntimeScheduleFailureV1::Publication)?;
    let calibration = SemanticCalibrationProfileV1::jina_exact_flat_v1(
        pointer.projection_key.clone(),
        pointer.generation.clone(),
        capability_manifest_digest.clone(),
    )
    .map_err(|_| SemanticRuntimeScheduleFailureV1::Publication)?;
    Ok(Arc::new(ProjectSemanticQuerySnapshotV1 {
        pointer: pointer.clone(),
        vectors,
        projection,
        search_index_key,
        calibration,
        generation,
        capability_manifest_digest,
    }))
}

fn open_vector_store(
    database: &Database,
    authority_namespace: &str,
) -> Result<Arc<dyn DurableVectorAuthorityStoreV1>, ProjectSemanticRuntimeErrorV1> {
    let handle = database.open_vector_authority(authority_namespace)?;
    Ok(Arc::new(handle))
}
