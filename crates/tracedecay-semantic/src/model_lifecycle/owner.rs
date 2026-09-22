/// Owns selection, background acquisition, and remediation for one data root.
pub struct SemanticModelLifecycleOwnerV1 {
    root: PathBuf,
    catalog: FastEmbedModelCatalogV1,
    source: Arc<dyn ModelMemberSourceV1>,
    artifact_store: Arc<ModelArtifactStore>,
    lease_namespace: Option<String>,
    configuration_selection: Arc<tokio::sync::Mutex<()>>,
    inner: Arc<LifecyclePublicationGateV1>,
    worker: Mutex<AcquisitionWorkerStateV1>,
    acquisition: Arc<AcquisitionControlV1>,
    verified_ready: watch::Sender<SemanticLifecycleVerifiedReadyEventV1>,
}
struct LifecycleInner {
    durable: DurableLifecycleV1,
    evaluation_publication_readers: usize,
    evaluation_publication_writers_waiting: usize,
}

/// Exact lifecycle identity that a semantic evaluation may pin through its
/// durable publication. The owner alone mints it from the lifecycle state and
/// verified-ready event, preventing callers from constructing a lookalike.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SemanticModelLifecyclePublicationIdentityV1 {
    state: SemanticModelLifecycleStateV1,
    verified_ready_epoch: u64,
    verified_ready_artifact_digest: String,
}

/// Opaque owner-issued identity for one runtime lifecycle mutation.
///
/// A projection may finish after selection or artifact remediation has moved
/// on. The selection generation and exact artifact digest fence every later
/// `Ready` write against that stale work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SemanticModelLifecycleMutationTargetV1 {
    model_id: String,
    selection_generation: u64,
    artifact_digest: String,
}

impl SemanticModelLifecycleMutationTargetV1 {
    pub fn model_id(&self) -> &str {
        &self.model_id
    }

    pub const fn selection_generation(&self) -> u64 {
        self.selection_generation
    }

    pub fn artifact_digest(&self) -> &str {
        &self.artifact_digest
    }
}

impl SemanticModelLifecyclePublicationIdentityV1 {
    pub fn state(&self) -> &SemanticModelLifecycleStateV1 {
        &self.state
    }
}

/// A Send-safe read lease over the canonical lifecycle state. Dropping the
/// lease releases state-changing selection, install, and remediation work.
pub struct SemanticModelLifecycleEvaluationPublicationLeaseV1 {
    gate: Arc<LifecyclePublicationGateV1>,
}

impl Drop for SemanticModelLifecycleEvaluationPublicationLeaseV1 {
    fn drop(&mut self) {
        let mut guard = self.gate.read();
        let Some(readers) = guard.evaluation_publication_readers.checked_sub(1) else {
            return;
        };
        guard.evaluation_publication_readers = readers;
        if guard.evaluation_publication_readers == 0 {
            self.gate.readers_released.notify_all();
        }
    }
}

/// One lifecycle mutex also serves as the reader/writer publication gate. A
/// lease owns only an `Arc`, never a thread-affine mutex guard, so it remains
/// safe to carry through asynchronous daemon publication.
struct LifecyclePublicationGateV1 {
    inner: Mutex<LifecycleInner>,
    readers_released: Condvar,
}

impl LifecyclePublicationGateV1 {
    fn new(durable: DurableLifecycleV1) -> Self {
        Self {
            inner: Mutex::new(LifecycleInner {
                durable,
                evaluation_publication_readers: 0,
                evaluation_publication_writers_waiting: 0,
            }),
            readers_released: Condvar::new(),
        }
    }

    fn read(&self) -> MutexGuard<'_, LifecycleInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn writer(&self) -> LifecycleWriteGuardV1<'_> {
        let mut guard = self.read();
        guard.evaluation_publication_writers_waiting = guard
            .evaluation_publication_writers_waiting
            .saturating_add(1);
        while guard.evaluation_publication_readers != 0 {
            guard = self
                .readers_released
                .wait(guard)
                .unwrap_or_else(PoisonError::into_inner);
        }
        guard.evaluation_publication_writers_waiting = guard
            .evaluation_publication_writers_waiting
            .saturating_sub(1);
        LifecycleWriteGuardV1 { guard }
    }

    fn try_acquire_reader(
        self: &Arc<Self>,
        expected: &SemanticModelLifecyclePublicationIdentityV1,
        verified_ready: &watch::Sender<SemanticLifecycleVerifiedReadyEventV1>,
    ) -> Result<SemanticModelLifecycleEvaluationPublicationLeaseV1, ModelLifecycleErrorV1> {
        let mut guard = self.read();
        if guard.evaluation_publication_writers_waiting != 0
            || lifecycle_publication_identity(&guard, verified_ready)? != expected.clone()
        {
            return Err(ModelLifecycleErrorV1::Rejected);
        }
        guard.evaluation_publication_readers = guard
            .evaluation_publication_readers
            .checked_add(1)
            .ok_or(ModelLifecycleErrorV1::Rejected)?;
        Ok(SemanticModelLifecycleEvaluationPublicationLeaseV1 {
            gate: Arc::clone(self),
        })
    }
}

struct LifecycleWriteGuardV1<'a> {
    guard: MutexGuard<'a, LifecycleInner>,
}

impl std::ops::Deref for LifecycleWriteGuardV1<'_> {
    type Target = LifecycleInner;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl std::ops::DerefMut for LifecycleWriteGuardV1<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

fn lifecycle_publication_identity(
    guard: &LifecycleInner,
    verified_ready: &watch::Sender<SemanticLifecycleVerifiedReadyEventV1>,
) -> Result<SemanticModelLifecyclePublicationIdentityV1, ModelLifecycleErrorV1> {
    let state = guard
        .durable
        .state
        .clone()
        .ok_or(ModelLifecycleErrorV1::Rejected)?;
    let ready = verified_ready.borrow().clone();
    let artifact_digest = ready
        .artifact_digest
        .ok_or(ModelLifecycleErrorV1::Rejected)?;
    if artifact_digest != state.artifact_digest() {
        return Err(ModelLifecycleErrorV1::Rejected);
    }
    Ok(SemanticModelLifecyclePublicationIdentityV1 {
        state,
        verified_ready_epoch: ready.epoch,
        verified_ready_artifact_digest: artifact_digest,
    })
}

fn verified_ready_artifact_digest(state: &SemanticModelLifecycleStateV1) -> Option<String> {
    match state {
        SemanticModelLifecycleStateV1::Installed {
            artifact_digest, ..
        }
        | SemanticModelLifecycleStateV1::Ready {
            artifact_digest, ..
        } => Some(artifact_digest.clone()),
        _ => None,
    }
}

fn lifecycle_state_revision(state: &SemanticModelLifecycleStateV1) -> &str {
    match state {
        SemanticModelLifecycleStateV1::SelectedNotDownloaded { revision, .. }
        | SemanticModelLifecycleStateV1::Downloading { revision, .. }
        | SemanticModelLifecycleStateV1::Verifying { revision, .. }
        | SemanticModelLifecycleStateV1::Installed { revision, .. }
        | SemanticModelLifecycleStateV1::Loading { revision, .. }
        | SemanticModelLifecycleStateV1::Indexing { revision, .. }
        | SemanticModelLifecycleStateV1::Ready { revision, .. }
        | SemanticModelLifecycleStateV1::Failed { revision, .. } => revision,
    }
}

fn is_safe_install_component(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && !value.starts_with('.')
        && !value.contains(['/', '\\'])
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn is_lowercase_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Create a lifecycle directory one component at a time and reject every
/// pre-existing symlink or non-directory. `create_dir_all` follows a
/// pre-existing symlink, so validating after that call would be too late to
/// keep private lifecycle bytes below the configured root.
fn ensure_lifecycle_directory(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "lifecycle directory is not a real directory",
                ));
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty() && *parent != path)
            {
                ensure_lifecycle_directory(parent)?;
            }
            match fs::create_dir(path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
            let metadata = fs::symlink_metadata(path)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "lifecycle directory was replaced by a symlink",
                ));
            }
        }
        Err(error) => return Err(error),
    }
    Ok(())
}

fn ensure_lifecycle_private_directories(root: &Path) -> io::Result<()> {
    ensure_lifecycle_directory(root)?;
    for name in ["staging", "installs", "quarantine"] {
        ensure_lifecycle_directory(&root.join(name))?;
    }
    Ok(())
}

/// Verify that a private install path is exactly the path derived from its
/// persisted lifecycle identity, and that every component currently resolves
/// to a real directory beneath the lifecycle root. This preflight keeps a
/// corrupt or edited lifecycle file from turning remediation into arbitrary
/// recursive deletion.
fn private_install_path_is_safe(
    root: &Path,
    state: &SemanticModelLifecycleStateV1,
    path: &Path,
) -> bool {
    let model_id = state.model_id();
    let revision = lifecycle_state_revision(state);
    let digest = state.artifact_digest();
    if !is_safe_install_component(model_id)
        || !is_lowercase_hex(revision, 40)
        || !is_lowercase_hex(digest, 64)
    {
        return false;
    }
    let expected = install_path_for(root, model_id, revision, digest);
    if expected != path {
        return false;
    }
    let Ok(relative) = path.strip_prefix(root) else {
        return false;
    };
    let components = relative.components().collect::<Vec<_>>();
    if components.len() != 4
        || !components
            .iter()
            .all(|component| matches!(component, std::path::Component::Normal(_)))
    {
        return false;
    }
    let Ok(root_metadata) = fs::symlink_metadata(root) else {
        return false;
    };
    if !root_metadata.is_dir() || root_metadata.file_type().is_symlink() {
        return false;
    }
    let mut current = root.to_path_buf();
    for component in components {
        current.push(component.as_os_str());
        let Ok(metadata) = fs::symlink_metadata(&current) else {
            return false;
        };
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return false;
        }
    }
    true
}

fn remove_private_install_path(root: &Path, path: &Path) -> io::Result<()> {
    remove_private_path(root, path)
}

fn remove_private_path(root: &Path, path: &Path) -> io::Result<()> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "install path escaped root"))?;
    // Resolve the root once and remove through a capability. The capability
    // implementation uses no-follow traversal, so a parent swapped after
    // preflight cannot redirect recursive deletion through a symlink.
    cap_std::fs::Dir::open_ambient_dir(root, cap_fs_ext::ambient_authority())?
        .remove_dir_all(relative)
}

fn publish_verified_ready_event(
    events: &watch::Sender<SemanticLifecycleVerifiedReadyEventV1>,
    guard: &LifecycleInner,
) {
    let artifact_digest = guard
        .durable
        .state
        .as_ref()
        .and_then(verified_ready_artifact_digest);
    let Some(artifact_digest) = artifact_digest else {
        return;
    };
    events.send_modify(|current| {
        current.epoch = current.epoch.saturating_add(1);
        current.artifact_digest = Some(artifact_digest);
    });
}

#[derive(Default)]
struct AcquisitionWorkerStateV1 {
    handle: Option<JoinHandle<Result<(), ModelLifecycleErrorV1>>>,
    outcome: Option<ModelLifecycleErrorV1>,
    selection_generation: u64,
}

impl AcquisitionWorkerStateV1 {
    fn join_and_retain(
        &mut self,
        worker: JoinHandle<Result<(), ModelLifecycleErrorV1>>,
    ) -> Result<(), ModelLifecycleErrorV1> {
        let result = join_acquisition_worker(worker);
        if let Err(error) = &result {
            self.outcome = Some(error.clone());
        }
        result
    }

    fn reap_finished(&mut self) -> Result<(), ModelLifecycleErrorV1> {
        let worker = match self.handle.as_ref() {
            Some(worker) if worker.is_finished() => self.handle.take(),
            _ => None,
        };
        match worker {
            Some(worker) => self.join_and_retain(worker),
            None => Ok(()),
        }
    }
}

impl SemanticModelLifecycleOwnerV1 {
    pub fn open(
        root: impl Into<PathBuf>,
        catalog: FastEmbedModelCatalogV1,
        source: Arc<dyn ModelMemberSourceV1>,
    ) -> Result<Self, ModelLifecycleErrorV1> {
        let root = root.into();
        let artifact_root = root.join("verified-artifacts");
        Self::open_storage(root, artifact_root, None, catalog, source)
    }

    /// Keep selection and acquisition control private to one logical owner while
    /// sharing only the verified immutable artifact inventory.
    pub fn open_scoped(
        selection_root: impl Into<PathBuf>,
        shared_artifact_root: impl Into<PathBuf>,
        lease_namespace: &str,
        catalog: FastEmbedModelCatalogV1,
        source: Arc<dyn ModelMemberSourceV1>,
    ) -> Result<Self, ModelLifecycleErrorV1> {
        if lease_namespace.is_empty() {
            return Err(ModelLifecycleErrorV1::Rejected);
        }
        Self::open_storage(
            selection_root.into(),
            shared_artifact_root.into(),
            Some(encode_lowercase_hex(&Sha256::digest(
                lease_namespace.as_bytes(),
            ))),
            catalog,
            source,
        )
    }

    pub fn open_scoped_default(
        selection_root: impl Into<PathBuf>,
        shared_artifact_root: impl Into<PathBuf>,
        lease_namespace: &str,
    ) -> Result<Self, ModelLifecycleErrorV1> {
        let root = selection_root.into();
        let source = Arc::new(HfHubModelMemberSourceV1::new(
            root.join(HF_HUB_CACHE_DIRECTORY_V1),
        ));
        Self::open_scoped(
            root,
            shared_artifact_root,
            lease_namespace,
            FastEmbedModelCatalogV1::production(),
            source,
        )
    }

    fn open_storage(
        root: PathBuf,
        artifact_root: PathBuf,
        lease_namespace: Option<String>,
        catalog: FastEmbedModelCatalogV1,
        source: Arc<dyn ModelMemberSourceV1>,
    ) -> Result<Self, ModelLifecycleErrorV1> {
        catalog.validate()?;
        ensure_lifecycle_private_directories(&root)
            .map_err(|_| ModelLifecycleErrorV1::StoreUnavailable)?;
        let artifact_store = Arc::new(ModelArtifactStore::open(
            &artifact_root,
            RetentionPolicyV1 {
                grace_seconds: 7 * 24 * 60 * 60,
            },
        )?);
        let mut durable = load_or_default_durable(&root, &catalog)?;
        normalize_orphaned_acquisition_state(&root, &mut durable)?;
        // Start with an empty event and publish only after the durable state
        // has been reverified below. This prevents a corrupt private install
        // from being observed as Ready during startup.
        let (verified_ready, _) = watch::channel(SemanticLifecycleVerifiedReadyEventV1::default());
        let owner = Self {
            root,
            catalog,
            source,
            artifact_store,
            lease_namespace,
            configuration_selection: Arc::new(tokio::sync::Mutex::new(())),
            inner: Arc::new(LifecyclePublicationGateV1::new(durable)),
            worker: Mutex::new(AcquisitionWorkerStateV1::default()),
            acquisition: Arc::new(AcquisitionControlV1::default()),
            verified_ready,
        };
        owner.reverify_durable_install_state()?;
        let durable = owner.inner.read().durable.clone();
        owner.verified_ready.send_modify(|current| {
            current.epoch = 0;
            current.artifact_digest = durable
                .state
                .as_ref()
                .and_then(verified_ready_artifact_digest);
        });
        owner.reconcile_embedding_artifact_leases(&durable, current_unix_seconds()?)?;
        Ok(owner)
    }

    pub fn open_default(root: impl Into<PathBuf>) -> Result<Self, ModelLifecycleErrorV1> {
        let root = root.into();
        let source = Arc::new(HfHubModelMemberSourceV1::new(
            root.join(HF_HUB_CACHE_DIRECTORY_V1),
        ));
        Self::open(root, FastEmbedModelCatalogV1::production(), source)
    }

    fn lease_id(&self, slot: &str) -> String {
        match &self.lease_namespace {
            Some(owner) => format!("owner:{owner}:{slot}"),
            None => slot.to_owned(),
        }
    }

    /// Serialize canonical configuration read-and-select for startup requests
    /// that share this logical owner, including linked worktrees.
    pub async fn configuration_selection_guard(&self) -> tokio::sync::OwnedMutexGuard<()> {
        Arc::clone(&self.configuration_selection).lock_owned().await
    }

    pub fn catalog(&self) -> &FastEmbedModelCatalogV1 {
        &self.catalog
    }

    pub fn verified_ready_events(&self) -> watch::Receiver<SemanticLifecycleVerifiedReadyEventV1> {
        self.verified_ready.subscribe()
    }

    /// Mint the owner-bound identity used by a runtime projection commit.
    /// Every loadable lifecycle state must still point at independently
    /// verified bytes when this identity is issued.
    pub fn lifecycle_mutation_target(&self) -> Option<SemanticModelLifecycleMutationTargetV1> {
        let worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        let guard = self.inner.read();
        let model_id = guard.durable.selected_model.clone()?;
        let state = guard.durable.state.as_ref()?;
        if state.model_id() != model_id || !self.lifecycle_state_path_is_admissible(state) {
            return None;
        }
        Some(SemanticModelLifecycleMutationTargetV1 {
            model_id,
            selection_generation: worker.selection_generation,
            artifact_digest: state.artifact_digest().to_owned(),
        })
    }

    /// Bind a persisted projection to the currently selected lifecycle
    /// artifact. Legacy catalog projections are accepted only while the
    /// catalog package itself is still the selected artifact; imported and
    /// rolled-back artifacts require the owner-issued identity metadata.
    pub fn lifecycle_mutation_target_for_projection(
        &self,
        projection: &tracedecay_domain::AdmittedEmbeddingProjectionKeyV1,
    ) -> Option<SemanticModelLifecycleMutationTargetV1> {
        let target = self.lifecycle_mutation_target()?;
        if let Some(identity) = projection.lifecycle_artifact_identity() {
            return (identity == target.artifact_digest()).then_some(target);
        }
        let model = self.catalog.get(target.model_id())?;
        let expected = format!("sha256:{}", catalog_package_digest(model));
        (target.artifact_digest() == catalog_package_digest(model)
            && projection.embedding_key().model_artifact_digest.as_str() == expected)
            .then_some(target)
    }

    /// Check that a persisted lifecycle state still owns verified bytes. The
    /// private install path is rehashed in full; shared installs are admitted
    /// through the artifact store's own content-addressed verifier.
    fn lifecycle_state_path_is_admissible(
        &self,
        state: &SemanticModelLifecycleStateV1,
    ) -> bool {
        let Some(path) = install_path_of(state) else {
            return true;
        };
        let Some(model) = self.catalog.get(state.model_id()) else {
            return false;
        };
        let catalog_digest = catalog_package_digest(model);
        // A private install is addressed by the catalog identity below the
        // lifecycle root. Scoped owners instead persist that same catalog
        // identity in their lifecycle state while pointing at the shared
        // artifact store's content-addressed directory. Distinguish those
        // cases by the path itself; using only the lifecycle digest would
        // demote every valid shared install on restart.
        if path == install_path_for(&self.root, &model.model_id, &model.source.revision, &catalog_digest)
        {
            // `existing_install_path` validates the derived path, install
            // metadata, every declared member length, and every SHA-256 pin.
            // Keep this full read/hash on every readiness check so a private
            // model edited while the daemon was stopped can never be exposed
            // through a stale lifecycle state.
            return existing_install_path(&self.root, model, &catalog_digest)
                .is_some_and(|owned| owned == path);
        }
        let Some(inventory_digest) = self.artifact_store.installed_digest(path) else {
            return false;
        };
        if self.artifact_store.installed_directory(&inventory_digest) != path {
            return false;
        }
        let Ok(record) = self
            .artifact_store
            .verified_installed_record(&inventory_digest)
        else {
            return false;
        };
        let Some(manifest) = record.manifest.as_ref() else {
            return false;
        };
        verify_catalog_manifest(model, manifest).is_ok()
            && (state.artifact_digest() == catalog_digest
                || state.artifact_digest() == inventory_digest.to_string())
    }

    fn lifecycle_mutation_target_matches(
        &self,
        worker: &AcquisitionWorkerStateV1,
        guard: &LifecycleInner,
        target: &SemanticModelLifecycleMutationTargetV1,
    ) -> bool {
        worker.selection_generation == target.selection_generation
            && guard.durable.selected_model.as_deref() == Some(target.model_id.as_str())
            && guard.durable.state.as_ref().is_some_and(|state| {
                state.model_id() == target.model_id
                    && state.artifact_digest() == target.artifact_digest
                    && self.lifecycle_state_path_is_admissible(state)
            })
    }

    fn advance_selection_generation(&self) {
        let mut worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        worker.selection_generation = worker.selection_generation.saturating_add(1);
    }

    /// Revalidate every persisted install before startup can expose its Ready
    /// event. A private install is checked against all catalog member lengths
    /// and SHA-256 pins; a shared install is checked through the artifact
    /// store's content-addressed verifier. Invalid current state is demoted
    /// to an explicit not-downloaded selection so callers cannot serve bytes
    /// that were edited while the process was stopped.
    fn reverify_durable_install_state(&self) -> Result<(), ModelLifecycleErrorV1> {
        let mut guard = self.inner.writer();
        let invalid_current = guard.durable.state.as_ref().is_some_and(|state| {
            install_path_of(state).is_some() && !self.lifecycle_state_path_is_admissible(state)
        });
        let invalid_previous = guard
            .durable
            .previous_ready
            .as_ref()
            .is_some_and(|state| !self.lifecycle_state_path_is_admissible(state));
        if !invalid_current && !invalid_previous {
            return Ok(());
        }
        let prior = guard.durable.clone();
        if invalid_current {
            let state = guard
                .durable
                .state
                .as_ref()
                .ok_or(ModelLifecycleErrorV1::Rejected)?;
            let model = self
                .catalog
                .get(state.model_id())
                .ok_or(ModelLifecycleErrorV1::VerificationFailed)?;
            guard.durable.state = Some(SemanticModelLifecycleStateV1::SelectedNotDownloaded {
                model_id: model.model_id.clone(),
                revision: model.source.revision.clone(),
                artifact_digest: catalog_package_digest(model),
            });
        }
        if invalid_previous {
            guard.durable.previous_ready = None;
        }
        if let Err(error) = persist_durable(&self.root, &guard.durable) {
            guard.durable = prior;
            return Err(error);
        }
        Ok(())
    }

    /// Mint the exact lifecycle identity that a semantic evaluation may retain
    /// through its final configuration publication.
    pub fn verified_evaluation_publication_identity(
        &self,
    ) -> Result<SemanticModelLifecyclePublicationIdentityV1, ModelLifecycleErrorV1> {
        let guard = self.inner.read();
        lifecycle_publication_identity(&guard, &self.verified_ready)
    }

    /// Acquire the canonical lifecycle read side after checking the same
    /// identity that was observed for evaluation. State-changing lifecycle
    /// operations take the matching writer side and therefore cannot pass this
    /// point until the returned lease is dropped.
    pub async fn acquire_verified_evaluation_publication_lease(
        &self,
        expected: &SemanticModelLifecyclePublicationIdentityV1,
        cancellation: Arc<dyn crate::SemanticEvaluationCancellationV1>,
    ) -> Result<SemanticModelLifecycleEvaluationPublicationLeaseV1, ModelLifecycleErrorV1> {
        if crate::SemanticExecutionAuthority::interruption(cancellation.as_ref()).is_some() {
            return Err(ModelLifecycleErrorV1::Cancelled);
        }
        let lease = self
            .inner
            .try_acquire_reader(expected, &self.verified_ready)?;
        if crate::SemanticExecutionAuthority::interruption(cancellation.as_ref()).is_some() {
            drop(lease);
            return Err(ModelLifecycleErrorV1::Cancelled);
        }
        Ok(lease)
    }

    pub fn run_daemon_artifact_gc(
        &self,
        now_unix: u64,
    ) -> Result<Vec<GcReceiptV1>, ModelLifecycleErrorV1> {
        let expires_at_unix = now_unix
            .checked_add(ARTIFACT_GC_LEASE_SECONDS)
            .ok_or(ModelLifecycleErrorV1::Rejected)?;
        let lease = self.artifact_store.acquire_daemon_gc_lease(
            format!("daemon:{}:{now_unix}", std::process::id()),
            expires_at_unix,
            now_unix,
        )?;
        self.artifact_store
            .gc_with_daemon_lease(&lease, now_unix)
            .map_err(Into::into)
    }

    /// Explicitly import a complete local package through the verified store.
    /// Selection alone never invokes this operation.
    pub fn import_local_artifact(
        &self,
        model_id: &str,
        manifest: &ModelArtifactManifestV1,
        source: &Path,
        now_unix: u64,
    ) -> Result<SemanticModelLifecycleStatusV1, ModelLifecycleErrorV1> {
        let model = self
            .catalog
            .get(model_id)
            .ok_or(CatalogErrorV1::UnknownModel)?;
        verify_catalog_manifest(model, manifest)?;
        let record = self
            .artifact_store
            .import_local_directory(manifest, source, now_unix)?;
        self.publish_explicit_import(model, record, now_unix)
    }

    fn publish_explicit_import(
        &self,
        model: &CatalogedFastEmbedModelV1,
        record: ArtifactInventoryRecordV1,
        now_unix: u64,
    ) -> Result<SemanticModelLifecycleStatusV1, ModelLifecycleErrorV1> {
        self.advance_selection_generation();
        let mut guard = self.inner.writer();
        let prior_durable = guard.durable.clone();
        self.artifact_store.activate_artifact_with_rollback(
            &record.artifact_digest,
            &self.lease_id(EMBEDDING_ACTIVE_LEASE_ID_V1),
            &self.lease_id(EMBEDDING_ROLLBACK_LEASE_ID_V1),
            now_unix,
        )?;
        let install_path = self
            .artifact_store
            .installed_directory(&record.artifact_digest);
        if let Some(previous @ SemanticModelLifecycleStateV1::Ready { .. }) =
            guard.durable.state.clone()
        {
            guard.durable.previous_ready = Some(previous);
        }
        guard.durable.selected_model = Some(model.model_id.clone());
        guard.durable.state = Some(SemanticModelLifecycleStateV1::Installed {
            model_id: model.model_id.clone(),
            revision: model.source.revision.clone(),
            artifact_digest: record.artifact_digest.to_string(),
            install_path,
        });
        if let Err(error) = persist_durable(&self.root, &guard.durable) {
            guard.durable = prior_durable.clone();
            self.reconcile_embedding_artifact_leases(&prior_durable, now_unix)?;
            return Err(error);
        }
        publish_verified_ready_event(&self.verified_ready, &guard);
        drop(guard);
        Ok(self.status())
    }

    pub fn status(&self) -> SemanticModelLifecycleStatusV1 {
        let guard = self.inner.read();
        let mut remediation = guard.durable.state.as_ref().map_or(
            SemanticModelRemediationV1 {
                retry: false,
                remove: false,
                rollback: false,
            },
            SemanticModelLifecycleStateV1::remediation,
        );
        if matches!(
            guard.durable.previous_ready.as_ref(),
            Some(SemanticModelLifecycleStateV1::Ready { .. })
        ) {
            remediation.rollback = true;
        }
        // Durable readiness describes verified artifacts. Executability belongs
        // to this binary and must never retire another runtime's valid install.
        let runtime_available = guard
            .durable
            .selected_model
            .as_deref()
            .and_then(|id| self.catalog.get(id))
            .is_some_and(|model| model.backend.runtime_family().is_compiled());
        let semantics_omitted = !runtime_available
            || guard
                .durable
                .state
                .as_ref()
                .is_none_or(SemanticModelLifecycleStateV1::omits_semantics);
        SemanticModelLifecycleStatusV1 {
            selected_model: guard.durable.selected_model.clone(),
            auto_download: guard.durable.auto_download,
            catalog_model_ids: self.catalog.model_ids().map(str::to_owned).collect(),
            state: guard.durable.state.clone(),
            remediation,
            semantics_omitted,
        }
    }

    /// Apply a settings selection. `None` disables semantics without download.
    #[hotpath::measure(label = "semantic.model_lifecycle.select_model")]
    pub fn select_model(
        &self,
        model_id: Option<&str>,
        auto_download: bool,
    ) -> Result<SemanticModelLifecycleStatusV1, ModelLifecycleErrorV1> {
        let selected = match model_id {
            Some(model_id) => {
                let model = match self.catalog.get(model_id) {
                    Some(model) => model,
                    None => {
                        crate::hotpath_observe::record_model_failure("catalog_unknown");
                        return Err(CatalogErrorV1::UnknownModel.into());
                    }
                };
                let installed = match self.re_admit_durable_selection(model)? {
                    Some(state) => Some(state),
                    None => self.discover_shared_selection(model)?,
                };
                Some((model, installed))
            }
            None => None,
        };
        let mut worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        self.cancel_background_acquisition();
        worker.reap_finished()?;
        worker.selection_generation = worker.selection_generation.saturating_add(1);
        let mut guard = self.inner.writer();
        guard.durable.auto_download = auto_download;
        match selected {
            None => {
                guard.durable.selected_model = None;
                guard.durable.state = None;
            }
            Some((model, durable_selection)) => {
                let digest = catalog_package_digest(model);
                guard.durable.selected_model = Some(model.model_id.clone());
                if let Some(state) = durable_selection {
                    guard.durable.state = Some(state);
                } else if let Some(path) = existing_install_path(&self.root, model, &digest) {
                    guard.durable.state = Some(SemanticModelLifecycleStateV1::Installed {
                        model_id: model.model_id.clone(),
                        revision: model.source.revision.clone(),
                        artifact_digest: digest,
                        install_path: path,
                    });
                } else {
                    guard.durable.state =
                        Some(SemanticModelLifecycleStateV1::SelectedNotDownloaded {
                            model_id: model.model_id.clone(),
                            revision: model.source.revision.clone(),
                            artifact_digest: digest,
                        });
                }
            }
        }
        persist_durable(&self.root, &guard.durable)?;
        self.reconcile_embedding_artifact_leases(&guard.durable, current_unix_seconds()?)?;
        match guard.durable.state.as_ref() {
            Some(state) => {
                crate::hotpath_observe::record_lifecycle_state(state);
            }
            None => crate::hotpath_observe::record_model_state("disabled"),
        }
        publish_verified_ready_event(&self.verified_ready, &guard);
        drop(guard);
        drop(worker);
        Ok(self.status())
    }

    fn discover_shared_selection(
        &self,
        model: &CatalogedFastEmbedModelV1,
    ) -> Result<Option<SemanticModelLifecycleStateV1>, ModelLifecycleErrorV1> {
        if self.lease_namespace.is_none() {
            return Ok(None);
        }
        for record in self.artifact_store.inventory()?.records.values() {
            let Some(manifest) = record.manifest.as_ref() else {
                continue;
            };
            if verify_catalog_manifest(model, manifest).is_err()
                || !matches!(
                    record.state,
                    ArtifactInventoryStateV1::Installed
                        | ArtifactInventoryStateV1::RetainedForRollback
                )
            {
                continue;
            }
            self.artifact_store
                .verified_installed_record(&record.artifact_digest)?;
            return Ok(Some(SemanticModelLifecycleStateV1::Installed {
                model_id: model.model_id.clone(),
                revision: model.source.revision.clone(),
                artifact_digest: catalog_package_digest(model),
                install_path: self
                    .artifact_store
                    .installed_directory(&record.artifact_digest),
            }));
        }
        Ok(None)
    }

    fn re_admit_durable_selection(
        &self,
        model: &CatalogedFastEmbedModelV1,
    ) -> Result<Option<SemanticModelLifecycleStateV1>, ModelLifecycleErrorV1> {
        let state = {
            let guard = self.inner.read();
            guard.durable.state.clone()
        };
        let (was_ready, artifact_digest, durable_install_path) = match state {
            Some(SemanticModelLifecycleStateV1::Installed {
                model_id,
                revision,
                artifact_digest,
                install_path,
            }) if model_id == model.model_id && revision == model.source.revision => {
                (false, artifact_digest, install_path)
            }
            Some(SemanticModelLifecycleStateV1::Ready {
                model_id,
                revision,
                artifact_digest,
                install_path,
            }) if model_id == model.model_id && revision == model.source.revision => {
                (true, artifact_digest, install_path)
            }
            _ => return Ok(None),
        };
        let catalog_digest = catalog_package_digest(model);
        if artifact_digest == catalog_digest
            && let Some(install_path) = existing_install_path(&self.root, model, &catalog_digest)
        {
            return Ok(Some(if was_ready {
                SemanticModelLifecycleStateV1::Ready {
                    model_id: model.model_id.clone(),
                    revision: model.source.revision.clone(),
                    artifact_digest: catalog_digest,
                    install_path,
                }
            } else {
                SemanticModelLifecycleStateV1::Installed {
                    model_id: model.model_id.clone(),
                    revision: model.source.revision.clone(),
                    artifact_digest: catalog_digest,
                    install_path,
                }
            }));
        }
        // Every other verified install lives in the artifact inventory, whose
        // content address is the install directory's name — not the lifecycle
        // digest, which names the catalog package for a scoped acquisition.
        let digest = self
            .artifact_store
            .installed_digest(&durable_install_path)
            .ok_or(ModelLifecycleErrorV1::VerificationFailed)?;
        let record = self.artifact_store.verified_installed_record(&digest)?;
        let manifest = record
            .manifest
            .as_ref()
            .ok_or(ModelLifecycleErrorV1::VerificationFailed)?;
        verify_catalog_manifest(model, manifest)?;
        let install_path = self.artifact_store.installed_directory(&digest);
        Ok(Some(if was_ready {
            SemanticModelLifecycleStateV1::Ready {
                model_id: model.model_id.clone(),
                revision: model.source.revision.clone(),
                artifact_digest,
                install_path,
            }
        } else {
            SemanticModelLifecycleStateV1::Installed {
                model_id: model.model_id.clone(),
                revision: model.source.revision.clone(),
                artifact_digest,
                install_path,
            }
        }))
    }

    /// Queue background acquisition after semantic retrieval is demanded.
    pub fn enqueue_demand_acquisition_if_needed(
        &self,
    ) -> Result<bool, ModelLifecycleErrorV1> {
        let status = self.status();
        let selected_model = status.selected_model.clone();
        if !status.auto_download {
            return Ok(false);
        }
        let Some(state) = status.state else {
            return Ok(false);
        };
        if !matches!(
            state,
            SemanticModelLifecycleStateV1::SelectedNotDownloaded { .. }
                | SemanticModelLifecycleStateV1::Failed {
                    retryable: true,
                    ..
                }
        ) {
            return Ok(false);
        }
        self.spawn_acquire(true, selected_model.as_deref())
    }

    pub fn retry(&self) -> Result<SemanticModelLifecycleStatusV1, ModelLifecycleErrorV1> {
        let status = self.status();
        if !status.remediation.retry {
            return Err(ModelLifecycleErrorV1::Rejected);
        }
        let model_id = status
            .selected_model
            .clone()
            .ok_or(ModelLifecycleErrorV1::Rejected)?;
        let selected = self.select_model(Some(&model_id), status.auto_download)?;
        if selected
            .state
            .as_ref()
            .and_then(verified_ready_artifact_digest)
            .is_none()
        {
            self.spawn_acquire(false, Some(&model_id))?;
        }
        Ok(self.status())
    }

    pub fn remove_install(&self) -> Result<SemanticModelLifecycleStatusV1, ModelLifecycleErrorV1> {
        let status = self.status();
        if !status.remediation.remove {
            return Err(ModelLifecycleErrorV1::Rejected);
        }
        self.cancel_and_join_background_acquisition()?;
        let (model_id, auto_download, removal_path) = {
            let mut guard = self.inner.writer();
            let can_remove = guard
                .durable
                .state
                .as_ref()
                .is_some_and(|state| state.remediation().remove);
            if !can_remove {
                return Err(ModelLifecycleErrorV1::Rejected);
            }
            let removal_path = guard
                .durable
                .state
                .as_ref()
                .map(|state| self.removal_path_for_state(state))
                .transpose()?
                .flatten();
            let prior = guard.durable.clone();
            guard.durable.state = None;
            if let Err(error) = persist_durable(&self.root, &guard.durable) {
                guard.durable = prior;
                return Err(error);
            }
            self.reconcile_embedding_artifact_leases(&guard.durable, current_unix_seconds()?)?;
            (
                guard.durable.selected_model.clone(),
                guard.durable.auto_download,
                removal_path,
            )
        };
        if let Some(path) = removal_path {
            remove_private_install_path(&self.root, &path)
                .map_err(|_| ModelLifecycleErrorV1::InstallFailed)?;
        }
        self.select_model(model_id.as_deref(), auto_download)
    }

    fn removal_path_for_state(
        &self,
        state: &SemanticModelLifecycleStateV1,
    ) -> Result<Option<PathBuf>, ModelLifecycleErrorV1> {
        let path = match state {
            SemanticModelLifecycleStateV1::Failed {
                model_id,
                revision,
                artifact_digest,
                ..
            } => {
                let path = install_path_for(&self.root, model_id, revision, artifact_digest);
                if fs::symlink_metadata(&path).is_err() {
                    return Ok(None);
                }
                path
            }
            _ => {
                let Some(path) = install_path_of(state) else {
                    return Ok(None);
                };
                path.to_path_buf()
            }
        };
        if let Some(digest) = self.artifact_store.installed_digest(&path)
            && self.artifact_store.installed_directory(&digest) == path
        {
            // Shared artifact-store bytes remain inventory-owned and become
            // eligible only under a later daemon GC lease.
            return Ok(None);
        }
        if !private_install_path_is_safe(&self.root, state, &path) {
            return Err(ModelLifecycleErrorV1::Rejected);
        }
        Ok(Some(path))
    }

    /// Signal the daemon-owned acquisition worker without blocking shutdown.
    pub fn cancel_background_acquisition(&self) {
        self.acquisition.cancel_current();
    }

    /// Clear and return a terminal worker join or cancellation-cleanup outcome
    /// after its quarantine or cleanup state has been explicitly resolved.
    pub fn resolve_background_acquisition_outcome(&self) -> Option<ModelLifecycleErrorV1> {
        self.worker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .outcome
            .take()
    }

    /// Cancel and join the daemon-owned acquisition worker before its model
    /// state or staged files are mutated by another lifecycle operation.
    pub fn cancel_and_join_background_acquisition(&self) -> Result<(), ModelLifecycleErrorV1> {
        let mut worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        self.cancel_background_acquisition();
        if let Some(error) = worker.outcome.clone() {
            return Err(error);
        }
        if let Some(handle) = worker.handle.take() {
            worker.join_and_retain(handle)?;
        }
        Ok(())
    }

    /// Cancel acquisition and join only within the caller's shutdown budget.
    ///
    /// A worker that is still blocked in its source remains retained for a
    /// later join; cancellation checkpoints fence verified-install publication.
    pub fn cancel_and_join_background_acquisition_until(
        &self,
        deadline: std::time::Instant,
    ) -> Result<bool, ModelLifecycleErrorV1> {
        let mut worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        self.cancel_background_acquisition();
        if let Some(error) = worker.outcome.clone() {
            return Err(error);
        }
        loop {
            let finished = match worker.handle.as_ref() {
                None => return Ok(true),
                Some(handle) if handle.is_finished() => worker.handle.take(),
                Some(_) => None,
            };
            if let Some(handle) = finished {
                worker.join_and_retain(handle)?;
                return Ok(true);
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                return Ok(false);
            }
            std::thread::sleep(
                deadline
                    .saturating_duration_since(now)
                    .min(std::time::Duration::from_millis(1)),
            );
        }
    }

    pub fn rollback_to_previous(
        &self,
    ) -> Result<SemanticModelLifecycleStatusV1, ModelLifecycleErrorV1> {
        self.advance_selection_generation();
        let mut guard = self.inner.writer();
        let previous = guard
            .durable
            .previous_ready
            .clone()
            .ok_or(ModelLifecycleErrorV1::Rejected)?;
        if !matches!(previous, SemanticModelLifecycleStateV1::Ready { .. }) {
            return Err(ModelLifecycleErrorV1::Rejected);
        }
        if !self.lifecycle_state_path_is_admissible(&previous) {
            return Err(ModelLifecycleErrorV1::VerificationFailed);
        }
        let prior_durable = guard.durable.clone();
        if let Some(digest) =
            install_path_of(&previous).and_then(|path| self.artifact_store.installed_digest(path))
        {
            self.artifact_store.activate_artifact_with_rollback(
                &digest,
                &self.lease_id(EMBEDDING_ACTIVE_LEASE_ID_V1),
                &self.lease_id(EMBEDDING_ROLLBACK_LEASE_ID_V1),
                current_unix_seconds()?,
            )?;
        }
        if let Some(SemanticModelLifecycleStateV1::Ready { .. }) = &guard.durable.state {
            let ready_state = guard.durable.state.clone();
            guard.durable.previous_ready = ready_state;
        }
        guard.durable.selected_model = Some(previous.model_id().to_owned());
        guard.durable.state = Some(previous);
        if let Err(error) = persist_durable(&self.root, &guard.durable) {
            guard.durable = prior_durable.clone();
            self.reconcile_embedding_artifact_leases(&prior_durable, current_unix_seconds()?)?;
            return Err(error);
        }
        publish_verified_ready_event(&self.verified_ready, &guard);
        drop(guard);
        Ok(self.status())
    }

    pub fn mark_loading(&self) -> Result<(), ModelLifecycleErrorV1> {
        self.transition_installed_like(|model_id, revision, digest, path| {
            SemanticModelLifecycleStateV1::Loading {
                model_id,
                revision,
                artifact_digest: digest,
                install_path: path,
            }
        })
    }

    pub fn mark_indexing(
        &self,
        completed_units: u64,
        total_units: u64,
    ) -> Result<(), ModelLifecycleErrorV1> {
        let mut guard = self.inner.writer();
        let Some(state) = guard.durable.state.clone() else {
            return Err(ModelLifecycleErrorV1::Rejected);
        };
        let (SemanticModelLifecycleStateV1::Installed {
            model_id,
            revision,
            artifact_digest: digest,
            install_path,
        }
        | SemanticModelLifecycleStateV1::Loading {
            model_id,
            revision,
            artifact_digest: digest,
            install_path,
        }
        | SemanticModelLifecycleStateV1::Indexing {
            model_id,
            revision,
            artifact_digest: digest,
            install_path,
            ..
        }
        | SemanticModelLifecycleStateV1::Ready {
            model_id,
            revision,
            artifact_digest: digest,
            install_path,
        }) = state
        else {
            return Err(ModelLifecycleErrorV1::Rejected);
        };
        if total_units == 0 || completed_units > total_units {
            return Err(ModelLifecycleErrorV1::Rejected);
        }
        guard.durable.state = Some(SemanticModelLifecycleStateV1::Indexing {
            model_id,
            revision,
            artifact_digest: digest,
            install_path,
            completed_units,
            total_units,
        });
        persist_durable(&self.root, &guard.durable)
    }

    pub fn mark_ready(&self) -> Result<(), ModelLifecycleErrorV1> {
        let mut guard = self.inner.writer();
        let Some(state) = guard.durable.state.clone() else {
            return Err(ModelLifecycleErrorV1::Rejected);
        };
        if !self.lifecycle_state_path_is_admissible(&state) {
            return Err(ModelLifecycleErrorV1::VerificationFailed);
        }
        let ready = match state {
            SemanticModelLifecycleStateV1::Installed {
                model_id,
                revision,
                artifact_digest,
                install_path,
            }
            | SemanticModelLifecycleStateV1::Loading {
                model_id,
                revision,
                artifact_digest,
                install_path,
            }
            | SemanticModelLifecycleStateV1::Indexing {
                model_id,
                revision,
                artifact_digest,
                install_path,
                ..
            } => SemanticModelLifecycleStateV1::Ready {
                model_id,
                revision,
                artifact_digest,
                install_path,
            },
            SemanticModelLifecycleStateV1::Ready { .. } => state,
            _ => return Err(ModelLifecycleErrorV1::Rejected),
        };
        if let Some(previous) = guard.durable.state.clone()
            && matches!(previous, SemanticModelLifecycleStateV1::Ready { .. })
            && previous.artifact_digest() != ready.artifact_digest()
        {
            guard.durable.previous_ready = Some(previous);
        }
        guard.durable.state = Some(ready);
        persist_durable(&self.root, &guard.durable)?;
        publish_verified_ready_event(&self.verified_ready, &guard);
        Ok(())
    }

    /// Commit a process-local runtime and the durable Ready transition under
    /// one owner-issued mutation target. A stale projection is rejected before
    /// its callback runs; a persistence failure invokes the supplied rollback
    /// while the target is still fenced.
    pub fn commit_runtime_ready_with_rollback(
        &self,
        target: &SemanticModelLifecycleMutationTargetV1,
        commit: impl FnOnce() -> bool,
        rollback: impl FnOnce(),
    ) -> Result<bool, ModelLifecycleErrorV1> {
        let worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        let mut guard = self.inner.writer();
        if !self.lifecycle_mutation_target_matches(&worker, &guard, target) {
            return Err(ModelLifecycleErrorV1::Rejected);
        }
        if !commit() {
            return Ok(false);
        }
        let prior = guard.durable.clone();
        let Some(state) = guard.durable.state.clone() else {
            rollback();
            return Err(ModelLifecycleErrorV1::Rejected);
        };
        if !self.lifecycle_state_path_is_admissible(&state) {
            rollback();
            return Err(ModelLifecycleErrorV1::VerificationFailed);
        }
        let ready = match state {
            SemanticModelLifecycleStateV1::Installed {
                model_id,
                revision,
                artifact_digest,
                install_path,
            }
            | SemanticModelLifecycleStateV1::Loading {
                model_id,
                revision,
                artifact_digest,
                install_path,
            }
            | SemanticModelLifecycleStateV1::Indexing {
                model_id,
                revision,
                artifact_digest,
                install_path,
                ..
            }
            | SemanticModelLifecycleStateV1::Ready {
                model_id,
                revision,
                artifact_digest,
                install_path,
            } => SemanticModelLifecycleStateV1::Ready {
                model_id,
                revision,
                artifact_digest,
                install_path,
            },
            _ => {
                rollback();
                return Err(ModelLifecycleErrorV1::Rejected);
            }
        };
        if let Some(previous) = guard.durable.state.clone()
            && matches!(previous, SemanticModelLifecycleStateV1::Ready { .. })
            && previous.artifact_digest() != ready.artifact_digest()
        {
            guard.durable.previous_ready = Some(previous);
        }
        guard.durable.state = Some(ready);
        if let Err(error) = persist_durable(&self.root, &guard.durable) {
            guard.durable = prior;
            rollback();
            return Err(error);
        }
        publish_verified_ready_event(&self.verified_ready, &guard);
        Ok(true)
    }

    pub fn mark_runtime_failed(
        &self,
        detail: impl Into<String>,
        retryable: bool,
    ) -> Result<(), ModelLifecycleErrorV1> {
        let mut guard = self.inner.writer();
        let Some(state) = guard.durable.state.clone() else {
            return Err(ModelLifecycleErrorV1::Rejected);
        };
        let (SemanticModelLifecycleStateV1::Installed {
            model_id,
            revision,
            artifact_digest,
            ..
        }
        | SemanticModelLifecycleStateV1::Loading {
            model_id,
            revision,
            artifact_digest,
            ..
        }
        | SemanticModelLifecycleStateV1::Indexing {
            model_id,
            revision,
            artifact_digest,
            ..
        }
        | SemanticModelLifecycleStateV1::Ready {
            model_id,
            revision,
            artifact_digest,
            ..
        }) = state
        else {
            return Err(ModelLifecycleErrorV1::Rejected);
        };
        guard.durable.state = Some(SemanticModelLifecycleStateV1::Failed {
            model_id,
            revision,
            artifact_digest,
            detail: detail.into(),
            retryable,
        });
        persist_durable(&self.root, &guard.durable)
    }

    fn transition_installed_like(
        &self,
        build: impl FnOnce(String, String, String, PathBuf) -> SemanticModelLifecycleStateV1,
    ) -> Result<(), ModelLifecycleErrorV1> {
        let mut guard = self.inner.writer();
        let Some(state) = guard.durable.state.clone() else {
            return Err(ModelLifecycleErrorV1::Rejected);
        };
        if matches!(state, SemanticModelLifecycleStateV1::Ready { .. }) {
            guard.durable.previous_ready = Some(state.clone());
        }
        let next = match state {
            SemanticModelLifecycleStateV1::Installed {
                model_id,
                revision,
                artifact_digest,
                install_path,
            }
            | SemanticModelLifecycleStateV1::Loading {
                model_id,
                revision,
                artifact_digest,
                install_path,
            }
            | SemanticModelLifecycleStateV1::Ready {
                model_id,
                revision,
                artifact_digest,
                install_path,
            } => build(model_id, revision, artifact_digest, install_path),
            _ => return Err(ModelLifecycleErrorV1::Rejected),
        };
        guard.durable.state = Some(next);
        persist_durable(&self.root, &guard.durable)
    }

    fn spawn_acquire(
        &self,
        require_auto_download: bool,
        expected_model_id: Option<&str>,
    ) -> Result<bool, ModelLifecycleErrorV1> {
        let mut worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(error) = worker.outcome.as_ref() {
            return Err(error.clone());
        }
        worker.reap_finished()?;
        if worker.handle.is_some() {
            return Ok(false);
        }
        let epoch = self.acquisition.begin_epoch();
        let root = self.root.clone();
        let catalog = self.catalog.clone();
        let source = Arc::clone(&self.source);
        let inner = Arc::clone(&self.inner);
        let selected = {
            let guard = inner.read();
            if require_auto_download && !guard.durable.auto_download {
                return Ok(false);
            }
            if !matches!(
                guard.durable.state.as_ref(),
                Some(SemanticModelLifecycleStateV1::SelectedNotDownloaded { .. })
                    | Some(SemanticModelLifecycleStateV1::Failed {
                        retryable: true,
                        ..
                    })
            ) {
                return Ok(false);
            }
            let selected = guard.durable.selected_model.clone();
            if expected_model_id.is_some_and(|expected| selected.as_deref() != Some(expected)) {
                return Ok(false);
            }
            selected
        };
        let Some(model_id) = selected else {
            return Ok(false);
        };
        let worker_root = root.clone();
        let worker_catalog = catalog.clone();
        let worker_model_id = model_id.clone();
        let worker_inner = Arc::clone(&inner);
        let verified_ready = self.verified_ready.clone();
        let shared_store = self
            .lease_namespace
            .as_ref()
            .map(|_| Arc::clone(&self.artifact_store));
        let active_lease = self.lease_id(EMBEDDING_ACTIVE_LEASE_ID_V1);
        let rollback_lease = self.lease_id(EMBEDDING_ROLLBACK_LEASE_ID_V1);
        let handle = thread::Builder::new()
            .name("tracedecay-fastembed-acquire".to_owned())
            .spawn(move || {
                run_acquisition(
                    AcquisitionTargetV1 {
                        root: &worker_root,
                        catalog: &worker_catalog,
                        source: source.as_ref(),
                        model_id: &worker_model_id,
                    },
                    &epoch,
                    &worker_inner,
                    &verified_ready,
                    shared_store
                        .as_deref()
                        .map(|store| (store, active_lease.as_str(), rollback_lease.as_str())),
                )
            });
        match handle {
            Ok(join) => {
                worker.handle = Some(join);
                Ok(true)
            }
            Err(error) => {
                let lifecycle_error = ModelLifecycleErrorV1::DownloadFailedWithReason(format!(
                    "cannot start background acquisition worker: {error}"
                ));
                if let Some(model) = catalog.get(&model_id) {
                    set_failed_state(
                        &root,
                        &inner,
                        model,
                        &catalog_package_digest(model),
                        &lifecycle_error.to_string(),
                        true,
                    )?;
                }
                Err(lifecycle_error)
            }
        }
    }

    /// Synchronously acquire for tests and focused integration.
    pub fn acquire_blocking_for_tests(&self) -> Result<(), ModelLifecycleErrorV1> {
        let model_id = self
            .status()
            .selected_model
            .ok_or(ModelLifecycleErrorV1::Rejected)?;
        let epoch = self.acquisition.begin_epoch();
        run_acquisition(
            AcquisitionTargetV1 {
                root: &self.root,
                catalog: &self.catalog,
                source: self.source.as_ref(),
                model_id: &model_id,
            },
            &epoch,
            &self.inner,
            &self.verified_ready,
            self.lease_namespace
                .as_ref()
                .map(|_| {
                    (
                        self.artifact_store.as_ref(),
                        self.lease_id(EMBEDDING_ACTIVE_LEASE_ID_V1),
                        self.lease_id(EMBEDDING_ROLLBACK_LEASE_ID_V1),
                    )
                })
                .as_ref()
                .map(|(store, active, rollback)| (*store, active.as_str(), rollback.as_str())),
        )
    }
}

fn join_acquisition_worker(
    worker: JoinHandle<Result<(), ModelLifecycleErrorV1>>,
) -> Result<(), ModelLifecycleErrorV1> {
    match worker
        .join()
        .map_err(|_| ModelLifecycleErrorV1::WorkerJoinFailed)?
    {
        Ok(()) | Err(ModelLifecycleErrorV1::Cancelled) => Ok(()),
        Err(error) => Err(error),
    }
}
