type ResidentRerankerSlotV1 =
    Arc<Mutex<Option<Arc<super::rerank_adapter::FastEmbedRerankExecutorV1>>>>;

#[cfg(test)]
struct MutationPauseAfterJoinV1 {
    entered: Arc<std::sync::Barrier>,
    release: Arc<std::sync::Barrier>,
}

/// Owns selection, background acquisition, and remediation for one data root.
pub struct SemanticModelLifecycleOwnerV1 {
    root: PathBuf,
    catalog: FastEmbedModelCatalogV1,
    source: Arc<dyn ModelMemberSourceV1>,
    artifact_store: Arc<ModelArtifactStore>,
    lease_namespace: Option<String>,
    configuration_selection: Arc<tokio::sync::Mutex<()>>,
    mutation_reservation: Mutex<()>,
    #[cfg(test)]
    mutation_pause_after_join: Mutex<Option<MutationPauseAfterJoinV1>>,
    #[cfg(test)]
    mutation_pause_after_preflight_scan: Mutex<Option<MutationPauseAfterJoinV1>>,
    inner: Arc<LifecyclePublicationGateV1>,
    worker: Mutex<AcquisitionWorkerStateV1>,
    acquisition: Arc<AcquisitionControlV1>,
    verified_ready: watch::Sender<SemanticLifecycleVerifiedReadyEventV1>,
    resident_rerankers: Mutex<HashMap<Sha256DigestHex, ResidentRerankerSlotV1>>,
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

/// Opaque owner-issued identity for one lifecycle projection target.
///
/// Runtime projection work may outlive the operation that admitted it. The
/// selection generation binds every later state projection to the selection
/// that created the work, so a stale poller cannot move a newer selection
/// through `Loading`, `Indexing`, `Ready`, or `Failed`.
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

    /// Exact owner artifact identity bound to this lifecycle target. This is
    /// intentionally independent of the catalog package digest: imports and
    /// rollbacks may keep the same model while replacing its artifact.
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

/// Retain one owner-private install as durable cleanup debt.
///
/// The current install slot describes the artifact backing the lifecycle
/// state. It cannot also describe every older path whose retirement failed, so
/// those paths are kept separately until a later cleanup succeeds. Dedupe by
/// path: the path is the ownership authority and metadata may be reconstructed
/// from the install manifest on restart.
fn retain_private_install_debt(
    durable: &mut DurableLifecycleV1,
    private_install: DurablePrivateInstallV1,
) {
    if durable
        .private_install
        .as_ref()
        .is_some_and(|current| current.install_path == private_install.install_path)
        || durable
            .private_install_debts
            .iter()
            .any(|debt| debt.install_path == private_install.install_path)
    {
        return;
    }
    durable.private_install_debts.push(private_install);
}

fn remove_private_install_debt(durable: &mut DurableLifecycleV1, path: &Path) {
    durable
        .private_install_debts
        .retain(|debt| debt.install_path != path);
}

/// Replace the current private owner while preserving every displaced path.
/// Passing `None` intentionally moves the current owner into debt, which is
/// required when a shared artifact or a disabled selection becomes current.
fn replace_private_install(
    durable: &mut DurableLifecycleV1,
    next: Option<DurablePrivateInstallV1>,
) {
    let current = durable.private_install.take();
    if let Some(current) = current
        && next
            .as_ref()
            .is_none_or(|replacement| replacement.install_path != current.install_path)
    {
        retain_private_install_debt(durable, current);
    }
    if let Some(replacement) = next.as_ref() {
        remove_private_install_debt(durable, &replacement.install_path);
    }
    durable.private_install = next;
}

fn private_install_metadata_at_root(
    root: &Path,
    state: &SemanticModelLifecycleStateV1,
) -> Option<DurablePrivateInstallV1> {
    let install_path = install_path_of(state)?;
    if !private_cleanup_path_allowed(root, install_path) {
        return None;
    }
    Some(DurablePrivateInstallV1 {
        model_id: state.model_id().to_owned(),
        revision: state_revision(state).to_owned(),
        artifact_digest: state.artifact_digest().to_owned(),
        install_path: install_path.to_path_buf(),
    })
}

/// Create a lifecycle-owned directory only when the path itself is a real
/// directory. `create_dir_all` follows a pre-existing symlink, so validating
/// after that call is too late: an attacker could redirect all private
/// lifecycle bytes outside the owner root before the check runs.
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
            // Create one component at a time so a pre-existing symlink in an
            // ancestor cannot redirect `create_dir_all` outside the owner
            // root. The recursive parent check is also fail-closed for a
            // concurrently replaced parent.
            if let Some(parent) = path.parent().filter(|parent| {
                !parent.as_os_str().is_empty() && *parent != path
            }) {
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

fn is_private_install_layout_path(root: &Path, path: &Path) -> bool {
    let Ok(relative) = path.strip_prefix(root) else {
        return false;
    };
    matches!(
        relative.components().next(),
        Some(std::path::Component::Normal(name)) if name.to_str() == Some("installs")
    )
}

/// Validate every existing component of a private path without following a
/// symlink. Missing components are allowed so callers can validate a fresh
/// install before creating its model/revision directories, but every existing
/// component must be a real directory. The canonical comparison is retained
/// as a second fence for paths reached through a symlink introduced between
/// component checks.
fn private_path_within_base(root: &Path, base: &Path, path: &Path) -> bool {
    let Ok(relative) = path.strip_prefix(base) else {
        return false;
    };
    let components: Vec<_> = relative.components().collect();
    if components.is_empty()
        || components.iter().any(|component| {
            !matches!(component, std::path::Component::Normal(_))
        })
    {
        return false;
    }
    let Ok(root_metadata) = fs::symlink_metadata(root) else {
        return false;
    };
    let Ok(base_metadata) = fs::symlink_metadata(base) else {
        return false;
    };
    if root_metadata.file_type().is_symlink()
        || !root_metadata.is_dir()
        || base_metadata.file_type().is_symlink()
        || !base_metadata.is_dir()
    {
        return false;
    }
    let Ok(canonical_root) = fs::canonicalize(root) else {
        return false;
    };
    let Ok(canonical_base) = fs::canonicalize(base) else {
        return false;
    };
    if !canonical_base.starts_with(&canonical_root) {
        return false;
    }

    let mut current = base.to_path_buf();
    for (index, component) in components.iter().enumerate() {
        let std::path::Component::Normal(name) = component else {
            return false;
        };
        current.push(name);
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink()
                    || (index + 1 < components.len() && !metadata.is_dir())
                {
                    return false;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // The remaining components are not present yet. They are
                // still safe to create because the lexical component check
                // rejected `ParentDir` and every component already on disk
                // was checked above. Callers that create them re-run this
                // validator after creation.
                return true;
            }
            Err(_) => return false,
        }
    }
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return false;
    };
    if metadata.file_type().is_symlink() {
        return false;
    }
    fs::canonicalize(path)
        .ok()
        .is_some_and(|canonical_path| canonical_path.starts_with(&canonical_base))
}

/// Verify the artifact store's own path components without rejecting benign
/// platform aliases in the ambient parent (for example macOS /var). The
/// store opens its root and artifacts directory without following symlinks;
/// mirror that boundary for the path retained in a lifecycle target.
fn artifact_store_path_is_real(path: &Path) -> bool {
    let Some(artifacts_root) = path.parent() else {
        return false;
    };
    let Some(store_root) = artifacts_root.parent() else {
        return false;
    };
    let Ok(store_metadata) = fs::symlink_metadata(store_root) else {
        return false;
    };
    let Ok(artifacts_metadata) = fs::symlink_metadata(artifacts_root) else {
        return false;
    };
    let Ok(path_metadata) = fs::symlink_metadata(path) else {
        return false;
    };
    if store_metadata.file_type().is_symlink()
        || !store_metadata.is_dir()
        || artifacts_metadata.file_type().is_symlink()
        || !artifacts_metadata.is_dir()
        || path_metadata.file_type().is_symlink()
        || !path_metadata.is_dir()
    {
        return false;
    }
    let Ok(canonical_store_root) = fs::canonicalize(store_root) else {
        return false;
    };
    let Ok(canonical_artifacts_root) = fs::canonicalize(artifacts_root) else {
        return false;
    };
    let Ok(canonical_path) = fs::canonicalize(path) else {
        return false;
    };
    canonical_artifacts_root.starts_with(&canonical_store_root)
        && canonical_path.starts_with(&canonical_artifacts_root)
}

fn private_cleanup_path_allowed(root: &Path, path: &Path) -> bool {
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return false;
    }
    let relative = match path.strip_prefix(root) {
        Ok(relative) => relative,
        Err(_) => return false,
    };
    let components: Vec<_> = relative.components().collect();
    let allowed_base = match components.first() {
        Some(std::path::Component::Normal(name)) if name.to_str() == Some("installs") => {
            let valid_layout = components.len() == 4
                && components[1..3].iter().all(|component| {
                    matches!(component, std::path::Component::Normal(name) if !name
                        .is_empty())
                })
                && matches!(components.get(3), Some(std::path::Component::Normal(name)) if is_hex_digest_prefix(name));
            valid_layout.then(|| root.join("installs"))
        }
        Some(std::path::Component::Normal(name)) if name.to_str() == Some("staging") => {
            let valid_leaf = components.len() == 2
                && components
                    .get(1)
                    .and_then(|component| match component {
                        std::path::Component::Normal(name) => name.to_str(),
                        _ => None,
                    })
                    .is_some_and(|name| {
                        is_generated_private_backup_name(name)
                            || is_generated_acquisition_staging_name(name)
                    });
            valid_leaf.then(|| root.join("staging"))
        }
        Some(std::path::Component::Normal(name)) if name.to_str() == Some("quarantine") => {
            let valid_leaf = components.len() == 2
                && components
                    .get(1)
                    .and_then(|component| match component {
                        std::path::Component::Normal(name) => name.to_str(),
                        _ => None,
                    })
                    .is_some_and(is_generated_quarantine_name);
            valid_leaf.then(|| root.join("quarantine"))
        }
        _ => None,
    };
    let Some(allowed_base) = allowed_base else {
        return false;
    };
    private_path_within_base(root, &allowed_base, path)
}

fn is_hex_digest_prefix(name: &std::ffi::OsStr) -> bool {
    let Some(name) = name.to_str() else {
        return false;
    };
    name.len() == 16 && name.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Acquisition staging names are `<model-id>-<first-16-digest-hex>`. The
/// digest suffix makes a random user directory such as `notes-with-dash`
/// ineligible for recursive cleanup while still accepting model ids that
/// themselves contain dashes.
fn is_generated_acquisition_staging_name(name: &str) -> bool {
    let parts: Vec<_> = name.split('-').collect();
    if parts.len() < 2 || parts.first().is_some_and(|part| part.is_empty()) {
        return false;
    }
    // The deterministic base is `<model-id>-<digest-prefix>`. A collision
    // path appends `<pid>-<timestamp>-<nonce>` and, if necessary, one numeric
    // collision counter. Parse from the right so model ids may contain '-'.
    for tail_len in [0_usize, 3, 4, 5] {
        if parts.len() <= tail_len + 1 {
            continue;
        }
        let digest_index = parts.len() - tail_len - 1;
        let model_parts = &parts[..digest_index];
        let digest = parts[digest_index];
        if model_parts.iter().any(|part| part.is_empty())
            || model_parts
                .first()
                .is_some_and(|part| part.starts_with('.'))
            || digest.len() != 16
            || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            || parts[digest_index + 1..]
                .iter()
                .any(|part| part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()))
        {
            continue;
        }
        return true;
    }
    false
}

/// Cleanup quarantine only when its name was emitted by `cleanup_owned_path`.
/// Current names are `acquisition-<epoch>-<pid>-<timestamp>-<nonce>-<leaf>`;
/// accept the old epoch/leaf form for existing durable cleanup debt. This
/// prevents a user directory with an arbitrary dash from becoming recursively
/// deletable merely by placing it below the lifecycle root.
fn is_generated_quarantine_name(name: &str) -> bool {
    let Some(rest) = name.strip_prefix("acquisition-") else {
        return false;
    };
    let parts: Vec<_> = rest.split('-').collect();
    if parts.len() >= 5
        && parts[..4]
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
    {
        let leaf = parts[4..].join("-");
        if is_generated_quarantine_leaf(&leaf) {
            return true;
        }
    }
    if parts.len() >= 2
        && !parts[0].is_empty()
        && parts[0].bytes().all(|byte| byte.is_ascii_digit())
    {
        return is_generated_quarantine_leaf(&parts[1..].join("-"));
    }
    false
}

fn is_generated_quarantine_leaf(leaf: &str) -> bool {
    is_hex_digest_prefix(std::ffi::OsStr::new(leaf))
        || is_generated_acquisition_staging_name(leaf)
        || is_generated_private_backup_name(leaf)
}

/// Private replacement backups carry enough entropy to remain unique across
/// restarts: `<model-id>-<digest>-<pid>-<timestamp>-<epoch>-<nonce>` and an
/// optional numeric collision suffix. Parse the numeric tail from the right
/// so model ids may contain dashes without broadening the allowlist.
fn is_generated_private_backup_name(name: &str) -> bool {
    let Some(rest) = name.strip_prefix(".previous-install-") else {
        return false;
    };
    let parts: Vec<_> = rest.split('-').collect();
    if parts.len() < 6 {
        return false;
    }
    for tail_len in [4_usize, 5] {
        if parts.len() <= tail_len + 1 {
            continue;
        }
        let digest_index = parts.len() - tail_len - 1;
        let model_parts = &parts[..digest_index];
        let digest = parts[digest_index];
        if model_parts.is_empty()
            || model_parts.iter().any(|part| part.is_empty())
            || digest.len() != 16
            || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            || parts[digest_index + 1..]
                .iter()
                .any(|part| part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()))
        {
            continue;
        }
        return true;
    }
    false
}

/// Recover a private state path into the current owner slot before a caller
/// changes that state. This covers older lifecycle files written before the
/// explicit ownership field was introduced.
fn retain_private_install_from_state(root: &Path, durable: &mut DurableLifecycleV1) {
    if durable.private_install.is_none()
        && let Some(state) = durable.state.as_ref()
        && let Some(private_install) = private_install_metadata_at_root(root, state)
    {
        replace_private_install(durable, Some(private_install));
    }
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

fn normalize_orphaned_acquisition_state(
    root: &Path,
    durable: &mut DurableLifecycleV1,
) -> Result<(), ModelLifecycleErrorV1> {
    let next = match durable.state.clone() {
        Some(SemanticModelLifecycleStateV1::Downloading {
            model_id,
            revision,
            artifact_digest,
            ..
        })
        | Some(SemanticModelLifecycleStateV1::Verifying {
            model_id,
            revision,
            artifact_digest,
        }) => Some(SemanticModelLifecycleStateV1::SelectedNotDownloaded {
            model_id,
            revision,
            artifact_digest,
        }),
        _ => None,
    };
    let Some(next) = next else {
        return Ok(());
    };
    let prior = durable.clone();
    durable.state = Some(next);
    durable.failed_current = None;
    if let Err(error) = persist_durable(root, durable) {
        *durable = prior;
        return Err(error);
    }
    Ok(())
}

#[derive(Default)]
struct AcquisitionWorkerStateV1 {
    handle: Option<JoinHandle<Result<(), ModelLifecycleErrorV1>>>,
    // Keep terminal outcomes tied to the selection that spawned them. A
    // cancellation or panic from an older selection must not gate a newer
    // demand, especially when it selects the same model again.
    active_token: Option<AcquisitionWorkerTokenV1>,
    outcome: Option<ModelLifecycleErrorV1>,
    pending_outcomes:
        std::collections::VecDeque<(ModelLifecycleErrorV1, Option<String>, Option<u64>)>,
    outcome_model_id: Option<String>,
    outcome_selection_generation: Option<u64>,
    selection_generation: u64,
}

#[derive(Clone)]
struct AcquisitionWorkerTokenV1 {
    model_id: String,
    revision: String,
    artifact_digest: String,
    selection_generation: u64,
    epoch: AcquisitionEpochV1,
    staging_path: PathBuf,
    install_path: PathBuf,
    ownership: Arc<Mutex<AcquisitionWorkerPathOwnershipV1>>,
}

#[derive(Default)]
struct AcquisitionWorkerPathOwnershipV1 {
    staging: bool,
    private_install: bool,
}

#[derive(Clone, Copy)]
enum JoinedWorkerCleanupModeV1 {
    PreserveReferencedInstall,
    RetirePublishedInstall,
}

impl AcquisitionWorkerStateV1 {
    fn clear_outcome(&mut self) {
        if let Some((next, model_id, selection_generation)) = self.pending_outcomes.pop_front() {
            self.outcome = Some(next);
            self.outcome_model_id = model_id;
            self.outcome_selection_generation = selection_generation;
        } else {
            self.outcome = None;
            self.outcome_model_id = None;
            self.outcome_selection_generation = None;
        }
    }

    fn retain_outcome(
        &mut self,
        error: ModelLifecycleErrorV1,
        model_id: Option<String>,
        selection_generation: u64,
    ) {
        if let Some(previous) = self.outcome.replace(error) {
            self.pending_outcomes.push_back((
                previous,
                self.outcome_model_id.take(),
                self.outcome_selection_generation.take(),
            ));
        }
        self.outcome_model_id = model_id;
        self.outcome_selection_generation = Some(selection_generation);
    }

    fn retain_cleanup_errors(
        &mut self,
        errors: Vec<ModelLifecycleErrorV1>,
        token: &AcquisitionWorkerTokenV1,
    ) {
        let mut errors = errors.into_iter();
        let Some(first) = errors.next() else {
            return;
        };
        if let Some(previous) = self.outcome.take() {
            self.pending_outcomes.push_back((
                previous,
                self.outcome_model_id.take(),
                self.outcome_selection_generation.take(),
            ));
        }
        self.outcome = Some(first);
        self.pending_outcomes.extend(errors.map(|error| {
            (
                error,
                Some(token.model_id.clone()),
                Some(token.selection_generation),
            )
        }));
        self.outcome_model_id = Some(token.model_id.clone());
        self.outcome_selection_generation = Some(token.selection_generation);
    }

    fn outcome_is_stale(&self) -> bool {
        self.outcome.is_some()
            && self
                .outcome_selection_generation
                .is_some_and(|generation| generation != self.selection_generation)
    }

    fn outcome_is_retryable(&self) -> bool {
        matches!(
            self.outcome.as_ref(),
            Some(
                ModelLifecycleErrorV1::WorkerJoinFailed
                    | ModelLifecycleErrorV1::DownloadFailed
                    | ModelLifecycleErrorV1::DownloadFailedWithReason(_)
                    | ModelLifecycleErrorV1::InstallFailed
                    | ModelLifecycleErrorV1::ArtifactImport(
                        ArtifactImportErrorV1::StagingUnavailable
                    )
            )
        )
    }

    fn discard_stale_outcome(&mut self) {
        if self.outcome_is_stale() && self.outcome_is_retryable() {
            self.clear_outcome();
        }
    }

    fn join_and_retain(
        &mut self,
        worker: JoinHandle<Result<(), ModelLifecycleErrorV1>>,
    ) -> (
        Result<(), ModelLifecycleErrorV1>,
        Option<AcquisitionWorkerTokenV1>,
    ) {
        let active_token = self.active_token.take();
        let active_model_id = active_token.as_ref().map(|token| token.model_id.clone());
        let active_selection_generation = active_token
            .as_ref()
            .map_or(self.selection_generation, |token| {
                token.selection_generation
            });
        let result = join_acquisition_worker(worker);
        if let Err(error) = &result {
            self.retain_outcome(error.clone(), active_model_id, active_selection_generation);
        }
        (result, active_token)
    }

    fn take_for_join(
        &mut self,
    ) -> Option<(
        JoinHandle<Result<(), ModelLifecycleErrorV1>>,
        Option<AcquisitionWorkerTokenV1>,
    )> {
        Some((self.handle.take()?, self.active_token.take()))
    }

    fn retain_join_result(
        &mut self,
        result: &Result<(), ModelLifecycleErrorV1>,
        active_token: Option<&AcquisitionWorkerTokenV1>,
    ) {
        if let Err(error) = result {
            let model_id = active_token.map(|token| token.model_id.clone());
            let selection_generation = active_token.map_or(self.selection_generation, |token| {
                token.selection_generation
            });
            self.retain_outcome(error.clone(), model_id, selection_generation);
        }
    }

    fn reap_finished(
        &mut self,
    ) -> (
        Result<(), ModelLifecycleErrorV1>,
        Option<AcquisitionWorkerTokenV1>,
    ) {
        let worker = match self.handle.as_ref() {
            Some(worker) if worker.is_finished() => self.handle.take(),
            _ => None,
        };
        match worker {
            Some(worker) => self.join_and_retain(worker),
            None => (Ok(()), None),
        }
    }
}

impl SemanticModelLifecycleOwnerV1 {
    fn mutation_guard(&self) -> MutexGuard<'_, ()> {
        self.mutation_reservation
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    #[cfg(test)]
    fn set_mutation_pause_after_join_for_tests(
        &self,
        entered: Arc<std::sync::Barrier>,
        release: Arc<std::sync::Barrier>,
    ) {
        *self
            .mutation_pause_after_join
            .lock()
            .unwrap_or_else(PoisonError::into_inner) =
            Some(MutationPauseAfterJoinV1 { entered, release });
    }

    #[cfg(test)]
    fn pause_after_join_for_tests(&self) {
        let pause = self
            .mutation_pause_after_join
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(pause) = pause {
            let _ = pause.entered.wait();
            let _ = pause.release.wait();
        }
    }

    #[cfg(test)]
    fn set_mutation_pause_after_preflight_scan_for_tests(
        &self,
        entered: Arc<std::sync::Barrier>,
        release: Arc<std::sync::Barrier>,
    ) {
        *self
            .mutation_pause_after_preflight_scan
            .lock()
            .unwrap_or_else(PoisonError::into_inner) =
            Some(MutationPauseAfterJoinV1 { entered, release });
    }

    #[cfg(test)]
    fn pause_after_preflight_scan_for_tests(&self) {
        let pause = self
            .mutation_pause_after_preflight_scan
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(pause) = pause {
            let _ = pause.entered.wait();
            let _ = pause.release.wait();
        }
    }

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
        if recover_private_install_metadata(&root, &catalog, &mut durable) {
            persist_durable(&root, &durable)?;
        }
        // A process restart cannot have a live worker for a persisted
        // Downloading/Verifying state. Reclassify that interrupted attempt as
        // demand-retryable before publishing status to callers.
        normalize_orphaned_acquisition_state(&root, &mut durable)?;
        let initial_ready = SemanticLifecycleVerifiedReadyEventV1 {
            epoch: 0,
            artifact_digest: durable
                .state
                .as_ref()
                .and_then(verified_ready_artifact_digest),
        };
        let (verified_ready, _) = watch::channel(initial_ready);
        let owner = Self {
            root,
            catalog,
            source,
            artifact_store,
            lease_namespace,
            configuration_selection: Arc::new(tokio::sync::Mutex::new(())),
            mutation_reservation: Mutex::new(()),
            #[cfg(test)]
            mutation_pause_after_join: Mutex::new(None),
            #[cfg(test)]
            mutation_pause_after_preflight_scan: Mutex::new(None),
            inner: Arc::new(LifecyclePublicationGateV1::new(durable)),
            worker: Mutex::new(AcquisitionWorkerStateV1::default()),
            acquisition: Arc::new(AcquisitionControlV1::default()),
            verified_ready,
            resident_rerankers: Mutex::new(HashMap::new()),
        };
        let durable = owner.inner.read().durable.clone();
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

    /// Mint an owner-bound target for a runtime lifecycle projection. The
    /// target remains valid only while the selected model and generation stay
    /// unchanged.
    pub fn lifecycle_mutation_target(&self) -> Option<SemanticModelLifecycleMutationTargetV1> {
        let worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        let guard = self.inner.read();
        let model_id = guard.durable.selected_model.clone()?;
        let state = guard.durable.state.as_ref()?;
        if state.model_id() != model_id {
            return None;
        }
        if !self.lifecycle_state_path_is_admissible(state) {
            return None;
        }
        Some(SemanticModelLifecycleMutationTargetV1 {
            model_id,
            selection_generation: worker.selection_generation,
            artifact_digest: state.artifact_digest().to_owned(),
        })
    }

    /// Bind a runtime projection to the exact artifact selected by this owner
    /// before callers prepare process-local work. New lifecycle projections
    /// carry the owner-issued artifact identity as metadata. Old persisted
    /// catalog projections remain admissible only for the catalog package
    /// itself; they cannot be paired with an imported or rolled-back artifact
    /// whose lifecycle identity is different.
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

    /// Admit a lifecycle state as a runtime mutation target only when its
    /// backing bytes belong to the owner that minted the target. A lexical
    /// path check is insufficient here: a stale projection could otherwise
    /// provide a staging/quarantine path, an external path, or a private path
    /// whose parent was redirected by a symlink.
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
        if is_private_install_layout_path(&self.root, path) {
            return state.artifact_digest() == catalog_digest
                && existing_install_path(&self.root, model, &catalog_digest)
                    .is_some_and(|owned| owned == path);
        }

        // Shared artifacts are addressed by the store's content digest. The
        // lifecycle digest may be the catalog package digest (selection) or
        // the imported inventory digest (explicit import), so validate both
        // identities while requiring the exact inventory directory.
        let Some(inventory_digest) = self.artifact_store.installed_digest(path) else {
            return false;
        };
        if self.artifact_store.installed_directory(&inventory_digest) != path
            || !artifact_store_path_is_real(path)
        {
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
        if verify_catalog_manifest(model, manifest).is_err() {
            return false;
        }
        state.artifact_digest() == catalog_digest
            || state.artifact_digest() == inventory_digest.to_string()
    }

    fn advance_selection_generation(&self) {
        let mut worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        worker.selection_generation = worker.selection_generation.saturating_add(1);
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

    /// Re-admit the independently evaluated reranker selected by exact
    /// compatibility pins. No catalog lookup, network access, or ambient
    /// cache participates in this mount.
    pub fn mount_reranker(
        &self,
        pins: RerankCompatibilityPinsV1,
    ) -> Result<super::rerank_adapter::ProductionCodeRerankAuthorityV1, ModelLifecycleErrorV1> {
        let digest = sha256_hex_suffix(pins.artifact_manifest_digest.as_str())
            .ok_or(ModelLifecycleErrorV1::VerificationFailed)
            .and_then(|digest| {
                Sha256DigestHex::new(digest.to_owned())
                    .map_err(|_| ModelLifecycleErrorV1::VerificationFailed)
            })?;
        let artifact = self
            .artifact_store
            .admit_leased_for_runtime_by_digest(
                &digest,
                &RuntimeEnvironmentV1::detect_fastembed_process()
                    .map_err(|_| ModelLifecycleErrorV1::VerificationFailed)?,
                &self.lease_id(RERANKER_ACTIVE_LEASE_ID_V1),
                ArtifactLeaseKindV1::Active,
                current_unix_seconds()?,
            )
            .map_err(|_| ModelLifecycleErrorV1::VerificationFailed)?;
        let slot = {
            let mut residents = self
                .resident_rerankers
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            Arc::clone(
                residents
                    .entry(digest)
                    .or_insert_with(|| Arc::new(Mutex::new(None))),
            )
        };
        let mut resident = slot.lock().unwrap_or_else(PoisonError::into_inner);
        let executor = match resident.as_ref() {
            Some(executor) => Arc::clone(executor),
            None => {
                let executor =
                    super::rerank_adapter::warm_reranker_executor(artifact, pins.clone())
                        .map_err(map_reranker_admission_error)?;
                *resident = Some(Arc::clone(&executor));
                executor
            }
        };
        Ok(super::rerank_adapter::ProductionCodeRerankAuthorityV1::from_warmed(pins, executor))
    }

    pub fn import_local_reranker_artifact(
        &self,
        pins: RerankCompatibilityPinsV1,
        manifest: &ModelArtifactManifestV1,
        source: &Path,
        now_unix: u64,
    ) -> Result<RerankerArtifactLifecycleStatusV1, ModelLifecycleErrorV1> {
        super::rerank_adapter::validate_reranker_manifest_pins(manifest, &pins)
            .map_err(|_| ModelLifecycleErrorV1::VerificationFailed)?;
        let record = self
            .artifact_store
            .import_local_directory(manifest, source, now_unix)?;
        self.publish_reranker_artifact(pins, record, now_unix)
    }

    pub fn import_configured_https_reranker_artifact(
        &self,
        pins: RerankCompatibilityPinsV1,
        manifest: &ModelArtifactManifestV1,
        source: &ConfiguredHttpsArtifactSourceV1,
        transport: &dyn ExplicitHttpsArtifactTransportV1,
        resume_staging_id: Option<&str>,
        now_unix: u64,
    ) -> Result<RerankerArtifactLifecycleStatusV1, ModelLifecycleErrorV1> {
        super::rerank_adapter::validate_reranker_manifest_pins(manifest, &pins)
            .map_err(|_| ModelLifecycleErrorV1::VerificationFailed)?;
        let record = self.artifact_store.import_configured_https(
            manifest,
            source,
            transport,
            resume_staging_id,
            now_unix,
        )?;
        self.publish_reranker_artifact(pins, record, now_unix)
    }

    fn publish_reranker_artifact(
        &self,
        pins: RerankCompatibilityPinsV1,
        record: ArtifactInventoryRecordV1,
        now_unix: u64,
    ) -> Result<RerankerArtifactLifecycleStatusV1, ModelLifecycleErrorV1> {
        let environment = RuntimeEnvironmentV1::detect_fastembed_process()
            .map_err(|_| ModelLifecycleErrorV1::VerificationFailed)?;
        let admitted = self
            .artifact_store
            .admit_for_runtime_by_digest(&record.artifact_digest, &environment)
            .map_err(|_| ModelLifecycleErrorV1::VerificationFailed)?;
        super::rerank_adapter::admit_reranker_artifact(admitted, pins)
            .map_err(map_reranker_admission_error)?;
        self.artifact_store.activate_artifact_with_rollback(
            &record.artifact_digest,
            &self.lease_id(RERANKER_ACTIVE_LEASE_ID_V1),
            &self.lease_id(RERANKER_ROLLBACK_LEASE_ID_V1),
            now_unix,
        )?;
        self.retain_active_reranker(&record.artifact_digest);
        self.reranker_artifact_status()
    }

    pub fn reranker_artifact_status(
        &self,
    ) -> Result<RerankerArtifactLifecycleStatusV1, ModelLifecycleErrorV1> {
        let now_unix = current_unix_seconds()?;
        Ok(RerankerArtifactLifecycleStatusV1 {
            active_artifact_digest: self.artifact_store.artifact_digest_for_lease(
                &self.lease_id(RERANKER_ACTIVE_LEASE_ID_V1),
                ArtifactLeaseKindV1::Active,
                now_unix,
            )?,
            rollback_artifact_digest: self.artifact_store.artifact_digest_for_lease(
                &self.lease_id(RERANKER_ROLLBACK_LEASE_ID_V1),
                ArtifactLeaseKindV1::Rollback,
                now_unix,
            )?,
        })
    }

    pub fn rollback_reranker_artifact(
        &self,
        now_unix: u64,
    ) -> Result<RerankerArtifactLifecycleStatusV1, ModelLifecycleErrorV1> {
        let rollback = self
            .reranker_artifact_status()?
            .rollback_artifact_digest
            .ok_or(ModelLifecycleErrorV1::Rejected)?;
        self.artifact_store.activate_artifact_with_rollback(
            &rollback,
            &self.lease_id(RERANKER_ACTIVE_LEASE_ID_V1),
            &self.lease_id(RERANKER_ROLLBACK_LEASE_ID_V1),
            now_unix,
        )?;
        self.retain_active_reranker(&rollback);
        self.reranker_artifact_status()
    }

    fn retain_active_reranker(&self, active_digest: &Sha256DigestHex) {
        self.resident_rerankers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|digest, _| digest == active_digest);
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
        let _mutation = self.mutation_guard();
        let model = self
            .catalog
            .get(model_id)
            .ok_or(CatalogErrorV1::UnknownModel)?;
        verify_catalog_manifest(model, manifest)?;
        // Import replaces the selected artifact even when the model id stays
        // the same. Invalidate every runtime projection target before the
        // worker is cancelled so a stale poller cannot publish into the new
        // artifact generation during the join/publication window.
        self.advance_selection_generation();
        self.cancel_and_join_background_acquisition_locked_with_cleanup(
            JoinedWorkerCleanupModeV1::RetirePublishedInstall,
        )?;
        #[cfg(test)]
        self.pause_after_join_for_tests();
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
        let mut guard = self.inner.writer();
        let prior_durable = guard.durable.clone();
        let prior_private_install = prior_durable.private_install.clone().or_else(|| {
            prior_durable
                .state
                .as_ref()
                .and_then(|state| self.private_install_metadata_for_state(state))
        });
        replace_private_install(&mut guard.durable, prior_private_install.clone());
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
        guard.durable.failed_current = None;
        if let Err(error) = persist_durable(&self.root, &guard.durable) {
            guard.durable = prior_durable.clone();
            self.reconcile_embedding_artifact_leases(&prior_durable, now_unix)?;
            return Err(error);
        }
        // The imported artifact is already the durable selection. Wake the
        // activation coordinator before attempting to retire a private path,
        // whose cleanup may fail independently of this committed import.
        publish_verified_ready_event(&self.verified_ready, &guard);
        let previous_path = guard
            .durable
            .previous_ready
            .as_ref()
            .and_then(install_path_of);
        if prior_private_install.is_some()
            && !previous_path.is_some_and(|path| {
                prior_private_install
                    .as_ref()
                    .is_some_and(|private| private.install_path == path)
            })
        {
            if let Err(error) = self.retire_durable_private_install(&mut guard, None) {
                return Err(error);
            }
        }
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
        // A concurrent import may publish an Installed artifact immediately
        // before a rollback captures it as the displaced artifact. It is
        // still a verified, admissible rollback target even though it has not
        // reached Ready; rejecting it would make the result depend on which
        // side of the publication/rollback race acquired the reservation.
        if matches!(
            guard.durable.previous_ready.as_ref(),
            Some(
                SemanticModelLifecycleStateV1::Ready { .. }
                    | SemanticModelLifecycleStateV1::Installed { .. }
            )
        ) {
            remediation.rollback = true;
        }
        // A failed destructive cleanup can leave only durable ownership debt
        // after the public state has been cleared. Keep removal available so
        // callers can retry that debt instead of reporting a clean lifecycle
        // with an orphaned private install.
        if guard.durable.private_install.is_some()
            || !guard.durable.private_install_debts.is_empty()
        {
            remediation.remove = true;
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
        let _mutation = self.mutation_guard();
        self.select_model_locked(model_id, auto_download)
    }

    fn select_model_locked(
        &self,
        model_id: Option<&str>,
        auto_download: bool,
    ) -> Result<SemanticModelLifecycleStatusV1, ModelLifecycleErrorV1> {
        let selected_model = match model_id {
            Some(model_id) => {
                let model = match self.catalog.get(model_id) {
                    Some(model) => model,
                    None => {
                        crate::hotpath_observe::record_model_failure("catalog_unknown");
                        return Err(CatalogErrorV1::UnknownModel.into());
                    }
                };
                Some(model)
            }
            None => None,
        };
        let preserve_completed_install = selected_model.is_some_and(|model| {
            self.inner.read().durable.selected_model.as_deref() == Some(model.model_id.as_str())
        });
        // Capture the preflight view before joining the worker. A shared
        // acquisition may publish its inventory record in this window; the
        // post-join scan below must then be the source of the final state.
        if let Some(model) = selected_model {
            let _ = self.re_admit_durable_selection(model)?;
            let _ = self.discover_shared_selection(model)?;
        }
        #[cfg(test)]
        self.pause_after_preflight_scan_for_tests();
        let cleanup_mode = if preserve_completed_install {
            JoinedWorkerCleanupModeV1::PreserveReferencedInstall
        } else {
            JoinedWorkerCleanupModeV1::RetirePublishedInstall
        };
        let mut worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        worker.selection_generation = worker.selection_generation.saturating_add(1);
        // Advance the epoch before cancellation so every operation guarded by
        // the old epoch is fenced, including terminal failure publication.
        let _ = self.acquisition.begin_epoch();
        self.cancel_background_acquisition();
        // Cancellation is only a request. A source may be blocked until its
        // own checkpoint, so joining only already-finished handles leaves the
        // old handle resident and makes the next demand look single-flight
        // forever. Join the requested worker before publishing the selection;
        // the mutation reservation stays held across that wait and prevents a
        // new demand from entering the gap.
        drop(worker);
        self.cancel_and_join_background_acquisition_for_selection_locked(cleanup_mode)?;
        let mut worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        worker.discard_stale_outcome();

        // The first shared-store scan can race a worker that imports its
        // verified bytes while this selection is waiting to join it. Re-read
        // durable state and inventory after the join, immediately before
        // publication, so a newly imported shared artifact is admitted.
        let after_join = self.inner.read().durable.clone();
        let preserve_failed_install = selected_model.is_some_and(|model| {
            after_join.selected_model.as_deref() == Some(model.model_id.as_str())
                && matches!(
                    after_join.state,
                    Some(SemanticModelLifecycleStateV1::Failed { .. })
                )
        });
        let selected = match selected_model {
            Some(model) => {
                let durable_selection = if preserve_failed_install {
                    None
                } else {
                    match self.re_admit_durable_selection(model)? {
                        Some(state) => Some(state),
                        None => self.discover_shared_selection(model)?,
                    }
                };
                Some((model, durable_selection))
            }
            None => None,
        };
        let mut guard = self.inner.writer();
        let prior_durable = guard.durable.clone();
        let prior_private_install = prior_durable.private_install.clone().or_else(|| {
            prior_durable
                .state
                .as_ref()
                .and_then(|state| self.private_install_metadata_for_state(state))
        });
        // Keep the previous private path durably attached until the new
        // selection has been persisted. A failed persistence must leave the
        // old artifact and its ownership metadata intact.
        replace_private_install(&mut guard.durable, prior_private_install.clone());
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
                } else if !preserve_failed_install
                    && let Some(path) = existing_install_path(&self.root, model, &digest)
                {
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
        guard.durable.failed_current = None;
        if let Err(error) = persist_durable(&self.root, &guard.durable) {
            guard.durable = prior_durable;
            return Err(error);
        }
        // Publish the committed selection before any fallible retirement work.
        // A cleanup failure must still wake the production activation owner;
        // durable state and the event then describe the same replacement.
        publish_verified_ready_event(&self.verified_ready, &guard);
        self.reconcile_embedding_artifact_leases(&guard.durable, current_unix_seconds()?)?;

        let replacement_keeps_private_install = preserve_failed_install
            || guard
                .durable
                .selected_model
                .as_deref()
                .zip(guard.durable.state.as_ref().and_then(install_path_of))
                .is_some_and(|(model_id, path)| {
                    prior_private_install.as_ref().is_some_and(|private| {
                        private.model_id == model_id && private.install_path == path
                    })
                });
        if prior_private_install.is_some() && !replacement_keeps_private_install {
            self.retire_durable_private_install(&mut guard, None)?;
        }
        match guard.durable.state.as_ref() {
            Some(state) => {
                crate::hotpath_observe::record_lifecycle_state(state);
            }
            None => crate::hotpath_observe::record_model_state("disabled"),
        }
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
        if is_private_install_layout_path(&self.root, &durable_install_path)
            && !private_cleanup_path_allowed(&self.root, &durable_install_path)
        {
            return Err(ModelLifecycleErrorV1::VerificationFailed);
        }
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
    pub fn enqueue_demand_acquisition_if_needed(&self) -> bool {
        let status = self.status();
        let selected_model = status.selected_model.clone();
        if !status.auto_download {
            return false;
        }
        let Some(state) = status.state else {
            return false;
        };
        if !matches!(
            state,
            SemanticModelLifecycleStateV1::SelectedNotDownloaded { .. }
                | SemanticModelLifecycleStateV1::Failed {
                    retryable: true,
                    ..
                }
                | SemanticModelLifecycleStateV1::Downloading { .. }
                | SemanticModelLifecycleStateV1::Verifying { .. }
        ) {
            return false;
        }
        self.spawn_acquire(true, selected_model.as_deref())
    }

    pub fn retry(&self) -> Result<SemanticModelLifecycleStatusV1, ModelLifecycleErrorV1> {
        let _mutation = self.mutation_guard();
        self.reap_finished_for_retry_locked()?;
        let status = self.status();
        if !status.remediation.retry {
            return Err(ModelLifecycleErrorV1::Rejected);
        }
        let retained_cleanup_error = {
            let worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
            worker
                .outcome
                .as_ref()
                .filter(|_| !worker.outcome_is_retryable())
                .cloned()
        };
        if let Some(error) = retained_cleanup_error {
            return Err(error);
        }
        let model_id = status
            .selected_model
            .clone()
            .ok_or(ModelLifecycleErrorV1::Rejected)?;
        #[cfg(test)]
        self.pause_after_join_for_tests();
        let selected = self.select_model_locked(Some(&model_id), status.auto_download)?;
        if selected
            .state
            .as_ref()
            .and_then(verified_ready_artifact_digest)
            .is_none()
        {
            let spawned = self.spawn_acquire_locked(false, Some(&model_id));
            if !spawned {
                let retained_cleanup_error = {
                    let worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
                    worker
                        .outcome
                        .as_ref()
                        .filter(|_| !worker.outcome_is_retryable())
                        .cloned()
                };
                if let Some(error) = retained_cleanup_error {
                    return Err(error);
                }
                let current = self.status();
                if matches!(
                    current.state,
                    Some(
                        SemanticModelLifecycleStateV1::SelectedNotDownloaded { .. }
                            | SemanticModelLifecycleStateV1::Downloading { .. }
                            | SemanticModelLifecycleStateV1::Verifying { .. }
                            | SemanticModelLifecycleStateV1::Failed {
                                retryable: true,
                                ..
                            }
                    )
                ) {
                    return Err(ModelLifecycleErrorV1::Rejected);
                }
            }
        }
        Ok(self.status())
    }

    pub fn remove_install(&self) -> Result<SemanticModelLifecycleStatusV1, ModelLifecycleErrorV1> {
        let _mutation = self.mutation_guard();
        let status = self.status();
        if !status.remediation.remove {
            return Err(ModelLifecycleErrorV1::Rejected);
        }
        self.cancel_and_join_background_acquisition_locked()?;
        #[cfg(test)]
        self.pause_after_join_for_tests();

        // Cancellation can reap a successful worker and publish Installed
        // before this operation reaches its destructive step. Re-read under
        // the publication writer so the path being removed is the current
        // owner-private install, including a Failed state whose public shape
        // intentionally omits its install path.
        let mut guard = self.inner.writer();
        let prior = guard.durable.clone();
        let model_id = guard.durable.selected_model.clone();
        let auto_download = guard.durable.auto_download;
        // Preserve a private state path written by an older lifecycle file
        // before clearing the public state. Existing debt entries remain
        // untouched and are retired in the same destructive operation.
        retain_private_install_from_state(&self.root, &mut guard.durable);
        let previous_ready = guard.durable.previous_ready.clone();
        guard.durable.state = None;
        guard.durable.failed_current = None;
        if let Some(private_install) = guard.durable.private_install.take() {
            retain_private_install_debt(&mut guard.durable, private_install);
        }
        let private_debts = guard.durable.private_install_debts.clone();
        // Keep every ownership record attached while the destructive operation
        // runs. If removal fails, the same records remain durable for
        // retry/recovery and later lifecycle progress cannot overwrite them.
        let clears_previous_ready = guard.durable.private_install_debts.iter().any(|debt| {
            previous_ready
                .as_ref()
                .and_then(install_path_of)
                .is_some_and(|path| path == debt.install_path.as_path())
        });
        if clears_previous_ready {
            guard.durable.previous_ready = None;
        }
        if let Err(error) = persist_durable(&self.root, &guard.durable) {
            guard.durable = prior;
            return Err(error);
        }
        if let Err(error) =
            self.reconcile_embedding_artifact_leases(&guard.durable, current_unix_seconds()?)
        {
            // The ownership record was already committed before lease
            // reconciliation. Keep the in-memory view aligned with that
            // durable state so a reconciliation failure cannot hide the
            // private cleanup debt behind a best-effort rollback write.
            return Err(error);
        }
        for debt in &private_debts {
            if !private_cleanup_path_allowed(&self.root, &debt.install_path) {
                return Err(ModelLifecycleErrorV1::Rejected);
            }
            if let Err(error) = cleanup_private_owned_path(&self.root, &debt.install_path) {
                // The ownership record was persisted before cleanup. Retain
                // every path in memory as well; no fallible follow-up write is
                // needed to make this debt recoverable after restart.
                return Err(error);
            }
        }
        guard.durable.private_install_debts.clear();
        if let Err(error) = persist_durable(&self.root, &guard.durable) {
            // The durable record still names the removed paths. Keep the
            // in-memory view aligned with that evidence for retry/restart.
            guard.durable.private_install_debts = private_debts;
            return Err(error);
        }
        drop(guard);
        self.select_model_locked(model_id.as_deref(), auto_download)
    }

    /// Signal the daemon-owned acquisition worker without blocking shutdown.
    pub fn cancel_background_acquisition(&self) {
        self.acquisition.cancel_current();
    }

    fn normalize_orphaned_acquisition_state(&self) -> Result<(), ModelLifecycleErrorV1> {
        let mut guard = self.inner.writer();
        normalize_orphaned_acquisition_state(&self.root, &mut guard.durable)
    }

    fn reap_finished_for_retry_locked(&self) -> Result<(), ModelLifecycleErrorV1> {
        let mut worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        let (joined_result, token) = worker.reap_finished();
        let cleanup_result = self.cleanup_joined_worker(
            &mut worker,
            token,
            &joined_result,
            JoinedWorkerCleanupModeV1::PreserveReferencedInstall,
        );
        if matches!(&joined_result, Err(ModelLifecycleErrorV1::WorkerJoinFailed))
            || matches!(
                worker.outcome.as_ref(),
                Some(ModelLifecycleErrorV1::WorkerJoinFailed)
            )
        {
            // A panic leaves the durable state at Downloading/Verifying. Make
            // the retained join outcome retryable before public retry checks
            // remediation, even when cleanup also reports debt.
            self.normalize_orphaned_acquisition_state()?;
        }
        cleanup_result
    }

    fn cleanup_joined_worker(
        &self,
        worker: &mut AcquisitionWorkerStateV1,
        token: Option<AcquisitionWorkerTokenV1>,
        _result: &Result<(), ModelLifecycleErrorV1>,
        cleanup_mode: JoinedWorkerCleanupModeV1,
    ) -> Result<(), ModelLifecycleErrorV1> {
        let Some(token) = token else {
            return Ok(());
        };
        // Hold the publication writer while deciding whether this worker's
        // private install is still live and while deleting any owned paths.
        // Readers therefore cannot retain the old identity while its bytes
        // disappear, and a new reader cannot enter between the decision and
        // removal. Retire mode defers a referenced install to the enclosing
        // replacement operation; unreferenced worker paths are cleaned now.
        let mut publication = self.inner.writer();
        let preserve_private_install = self
            .current_selection_references_install_locked(&publication, &token)
            && matches!(
                cleanup_mode,
                JoinedWorkerCleanupModeV1::PreserveReferencedInstall
                    | JoinedWorkerCleanupModeV1::RetirePublishedInstall
            );
        let owns_private_install = token
            .ownership
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .private_install;
        let records_private_cleanup = owns_private_install && !preserve_private_install;
        // A worker can publish its private directory and then be cancelled or
        // lose the race with selection before the enclosing operation has a
        // chance to rotate durable ownership. Record that path before trying
        // to remove it. This makes cleanup failure recoverable after restart,
        // including when a later lifecycle write is unavailable.
        if records_private_cleanup {
            retain_private_install_debt(
                &mut publication.durable,
                DurablePrivateInstallV1 {
                    model_id: token.model_id.clone(),
                    revision: token.revision.clone(),
                    artifact_digest: token.artifact_digest.clone(),
                    install_path: token.install_path.clone(),
                },
            );
            if let Err(error) = persist_durable(&self.root, &publication.durable) {
                drop(publication);
                worker.retain_cleanup_errors(
                    vec![ModelLifecycleErrorV1::CancellationCleanupFailed(
                        token.install_path.clone(),
                    )],
                    &token,
                );
                return Err(error);
            }
        }
        let errors = cleanup_worker_owned_paths(&self.root, &token, preserve_private_install);
        if errors.is_empty() && records_private_cleanup {
            let debt_snapshot = publication.durable.private_install_debts.clone();
            remove_private_install_debt(&mut publication.durable, &token.install_path);
            if let Err(error) = persist_durable(&self.root, &publication.durable) {
                // The path was removed, but the durable debt remains the
                // recoverable evidence on disk. Restore the in-memory list to
                // match that evidence until the next successful write.
                publication.durable.private_install_debts = debt_snapshot;
                drop(publication);
                worker.retain_cleanup_errors(vec![error.clone()], &token);
                return Err(error);
            }
        }
        drop(publication);
        if let Some(error) = errors.first().cloned() {
            worker.retain_cleanup_errors(errors, &token);
            return Err(error);
        }
        Ok(())
    }

    fn current_selection_references_install_locked(
        &self,
        guard: &LifecycleInner,
        token: &AcquisitionWorkerTokenV1,
    ) -> bool {
        guard.durable.selected_model.as_deref() == Some(token.model_id.as_str())
            && (guard
                .durable
                .state
                .as_ref()
                .and_then(install_path_of)
                .is_some_and(|path| path == token.install_path.as_path())
                || guard
                    .durable
                    .private_install
                    .as_ref()
                    .is_some_and(|private| {
                        private.model_id == token.model_id
                            && private.install_path == token.install_path
                    }))
    }

    fn private_install_metadata_for_state(
        &self,
        state: &SemanticModelLifecycleStateV1,
    ) -> Option<DurablePrivateInstallV1> {
        private_install_metadata_at_root(&self.root, state)
    }

    fn install_is_admissible(&self, state: &SemanticModelLifecycleStateV1) -> bool {
        self.lifecycle_state_path_is_admissible(state)
    }

    fn retire_durable_private_install(
        &self,
        guard: &mut LifecycleWriteGuardV1<'_>,
        preserve_path: Option<&Path>,
    ) -> Result<(), ModelLifecycleErrorV1> {
        let Some(private_install) = guard.durable.private_install.clone() else {
            return Ok(());
        };
        if preserve_path.is_some_and(|path| path == private_install.install_path.as_path()) {
            return Ok(());
        }

        let before = guard.durable.clone();
        // Move the old owner into the durable debt collection before removal.
        // Later state progress can therefore update the current slot without
        // orphaning this path when cleanup fails.
        let previous_ready = guard.durable.previous_ready.clone();
        let clears_previous_ready = previous_ready
            .as_ref()
            .and_then(install_path_of)
            .is_some_and(|path| path == private_install.install_path.as_path());
        if clears_previous_ready {
            guard.durable.previous_ready = None;
        }
        replace_private_install(&mut guard.durable, None);
        if let Err(error) = persist_durable(&self.root, &guard.durable) {
            guard.durable = before;
            return Err(error);
        }
        match cleanup_private_owned_path(&self.root, &private_install.install_path) {
            Ok(()) => {
                let debts = guard.durable.private_install_debts.clone();
                remove_private_install_debt(&mut guard.durable, &private_install.install_path);
                if let Err(error) = persist_durable(&self.root, &guard.durable) {
                    // The on-disk record still names the removed path. Keep
                    // the in-memory view aligned with that durable evidence;
                    // restart recovery will clear the now-absent ownership
                    // record before any rollback can use it.
                    guard.durable.private_install_debts = debts;
                    return Err(error);
                }
                Ok(())
            }
            Err(error) => {
                // The ownership metadata was persisted before cleanup, so the
                // exact path remains durable without another fallible write.
                Err(error)
            }
        }
    }

    /// Retire a private install after a replacement durable state has already
    /// been committed. Keep the replacement as the current owner and queue the
    /// old path as debt before attempting cleanup.
    fn retire_replaced_private_install(
        &self,
        guard: &mut LifecycleWriteGuardV1<'_>,
        private_install: DurablePrivateInstallV1,
        replacement_private_install: Option<DurablePrivateInstallV1>,
        preserve_path: Option<&Path>,
    ) -> Result<(), ModelLifecycleErrorV1> {
        if preserve_path.is_some_and(|path| path == private_install.install_path.as_path()) {
            return Ok(());
        }
        let before = guard.durable.clone();
        let previous_ready = guard.durable.previous_ready.clone();
        let clears_previous_ready = previous_ready
            .as_ref()
            .and_then(install_path_of)
            .is_some_and(|path| path == private_install.install_path.as_path());
        if clears_previous_ready {
            guard.durable.previous_ready = None;
        }
        replace_private_install(&mut guard.durable, replacement_private_install);
        if let Err(error) = persist_durable(&self.root, &guard.durable) {
            guard.durable = before;
            return Err(error);
        }
        match cleanup_private_owned_path(&self.root, &private_install.install_path) {
            Ok(()) => {
                let debts = guard.durable.private_install_debts.clone();
                remove_private_install_debt(&mut guard.durable, &private_install.install_path);
                if let Err(error) = persist_durable(&self.root, &guard.durable) {
                    // The old path has already been retired, but its durable
                    // ownership record remains the recoverable evidence on
                    // disk. Keep memory aligned with that record.
                    guard.durable.private_install_debts = debts;
                    return Err(error);
                }
                Ok(())
            }
            Err(error) => {
                // The old path is already durable in the debt collection.
                Err(error)
            }
        }
    }

    /// Clear and return a terminal worker join or cancellation-cleanup outcome
    /// after its quarantine or cleanup state has been explicitly resolved.
    pub fn resolve_background_acquisition_outcome(&self) -> Option<ModelLifecycleErrorV1> {
        let _mutation = self.mutation_guard();
        let mut worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        let outcome = worker.outcome.clone();
        if let Some(
            ModelLifecycleErrorV1::CancellationCleanupQuarantined(path)
            | ModelLifecycleErrorV1::CancellationCleanupFailed(path),
        ) = outcome.as_ref()
            && (!private_cleanup_path_allowed(&self.root, path)
                || resolve_cleanup_path(path).is_err())
        {
            return outcome;
        }
        worker.clear_outcome();
        outcome
    }

    /// Cancel and join the daemon-owned acquisition worker before its model
    /// state or staged files are mutated by another lifecycle operation.
    pub fn cancel_and_join_background_acquisition(&self) -> Result<(), ModelLifecycleErrorV1> {
        let _mutation = self.mutation_guard();
        self.cancel_and_join_background_acquisition_locked()
    }

    fn cancel_and_join_background_acquisition_locked(&self) -> Result<(), ModelLifecycleErrorV1> {
        self.cancel_and_join_background_acquisition_locked_with_cleanup(
            JoinedWorkerCleanupModeV1::PreserveReferencedInstall,
        )
    }

    fn cancel_and_join_background_acquisition_locked_with_cleanup(
        &self,
        cleanup_mode: JoinedWorkerCleanupModeV1,
    ) -> Result<(), ModelLifecycleErrorV1> {
        let mut worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        let _ = self.acquisition.begin_epoch();
        self.cancel_background_acquisition();
        worker.discard_stale_outcome();
        if let Some(error) = worker.outcome.clone() {
            drop(worker);
            if matches!(&error, ModelLifecycleErrorV1::WorkerJoinFailed) {
                self.normalize_orphaned_acquisition_state()?;
            }
            return Err(error);
        }
        let Some((handle, token)) = worker.take_for_join() else {
            drop(worker);
            return self.normalize_orphaned_acquisition_state();
        };
        // Do not retain the owner mutex while joining. A source is allowed to
        // perform lifecycle checkpoints as it unwinds cancellation; holding
        // this lock across the join turns that cooperative path into a
        // deadlock and leaves a blocked handle mounted forever.
        drop(worker);
        let result = join_acquisition_worker(handle);
        let mut worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        worker.retain_join_result(&result, token.as_ref());
        let cleanup_result = self.cleanup_joined_worker(&mut worker, token, &result, cleanup_mode);
        let worker_join_failed = matches!(&result, Err(ModelLifecycleErrorV1::WorkerJoinFailed));
        if worker_join_failed {
            drop(worker);
            self.normalize_orphaned_acquisition_state()?;
            worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        }
        cleanup_result?;
        if worker.outcome_is_stale() && worker.outcome_is_retryable() {
            worker.clear_outcome();
            drop(worker);
            return self.normalize_orphaned_acquisition_state();
        }
        drop(worker);
        result?;
        self.normalize_orphaned_acquisition_state()
    }

    /// Selection changes may advance the lifecycle generation even when the
    /// prior worker left a path-bearing cleanup outcome. Join that worker
    /// before publishing the new selection, but carry the old cleanup debt
    /// forward instead of making an unrelated selection impossible.
    fn cancel_and_join_background_acquisition_for_selection_locked(
        &self,
        cleanup_mode: JoinedWorkerCleanupModeV1,
    ) -> Result<(), ModelLifecycleErrorV1> {
        let mut worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        let _ = self.acquisition.begin_epoch();
        self.cancel_background_acquisition();
        worker.discard_stale_outcome();
        let Some((handle, token)) = worker.take_for_join() else {
            drop(worker);
            return self.normalize_orphaned_acquisition_state();
        };
        // Join outside the worker mutex. The acquisition thread may need to
        // finish its cancellation checkpoint before this operation can
        // publish the replacement selection.
        drop(worker);
        let result = join_acquisition_worker(handle);
        let mut worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        worker.retain_join_result(&result, token.as_ref());
        let cleanup_result = self.cleanup_joined_worker(&mut worker, token, &result, cleanup_mode);
        let cleanup_failed = cleanup_result.is_err();
        if matches!(&result, Err(ModelLifecycleErrorV1::WorkerJoinFailed)) {
            drop(worker);
            self.normalize_orphaned_acquisition_state()?;
            worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        }
        // The selection generation was advanced before the join. Retryable
        // worker outcomes can therefore be dropped, while cleanup failures
        // remain attached to their old generation for explicit resolution.
        let stale_retryable_outcome = worker.outcome_is_stale() && worker.outcome_is_retryable();
        if stale_retryable_outcome {
            worker.clear_outcome();
        }
        if !cleanup_failed {
            if let Err(error) = result
                && !matches!(error, ModelLifecycleErrorV1::Cancelled)
                && !stale_retryable_outcome
            {
                drop(worker);
                return Err(error);
            }
        }
        drop(worker);
        self.normalize_orphaned_acquisition_state()
    }

    /// Cancel acquisition and join only within the caller's shutdown budget.
    ///
    /// A worker that is still blocked in its source remains retained for a
    /// later join; cancellation checkpoints fence verified-install publication.
    pub fn cancel_and_join_background_acquisition_until(
        &self,
        deadline: std::time::Instant,
    ) -> Result<bool, ModelLifecycleErrorV1> {
        let _mutation = self.mutation_guard();
        self.cancel_and_join_background_acquisition_until_locked(deadline)
    }

    fn cancel_and_join_background_acquisition_until_locked(
        &self,
        deadline: std::time::Instant,
    ) -> Result<bool, ModelLifecycleErrorV1> {
        let mut worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        let _ = self.acquisition.begin_epoch();
        self.cancel_background_acquisition();
        worker.discard_stale_outcome();
        if let Some(error) = worker.outcome.clone() {
            if matches!(&error, ModelLifecycleErrorV1::WorkerJoinFailed) {
                self.normalize_orphaned_acquisition_state()?;
            }
            return Err(error);
        }
        loop {
            let finished = match worker.handle.as_ref() {
                None => return self.normalize_orphaned_acquisition_state().map(|()| true),
                Some(handle) if handle.is_finished() => worker.handle.take(),
                Some(_) => None,
            };
            if let Some(handle) = finished {
                let (result, token) = worker.join_and_retain(handle);
                let cleanup_result = self.cleanup_joined_worker(
                    &mut worker,
                    token,
                    &result,
                    JoinedWorkerCleanupModeV1::PreserveReferencedInstall,
                );
                if matches!(&result, Err(ModelLifecycleErrorV1::WorkerJoinFailed)) {
                    self.normalize_orphaned_acquisition_state()?;
                }
                cleanup_result?;
                if worker.outcome_is_stale() && worker.outcome_is_retryable() {
                    worker.clear_outcome();
                    return self.normalize_orphaned_acquisition_state().map(|()| true);
                }
                result?;
                return self.normalize_orphaned_acquisition_state().map(|()| true);
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
        let _mutation = self.mutation_guard();
        if !self.status().remediation.rollback {
            return Err(ModelLifecycleErrorV1::Rejected);
        }
        // Rollback is an artifact replacement as well as a model selection
        // mutation. Fence pollers that were admitted for the artifact being
        // retired before cancellation and publication begin.
        self.advance_selection_generation();
        self.cancel_and_join_background_acquisition_locked_with_cleanup(
            JoinedWorkerCleanupModeV1::RetirePublishedInstall,
        )?;
        #[cfg(test)]
        self.pause_after_join_for_tests();
        let mut guard = self.inner.writer();
        let prior_durable = guard.durable.clone();
        let prior_private_install = prior_durable.private_install.clone().or_else(|| {
            prior_durable
                .state
                .as_ref()
                .and_then(|state| self.private_install_metadata_for_state(state))
        });
        replace_private_install(&mut guard.durable, prior_private_install.clone());
        let previous = guard
            .durable
            .previous_ready
            .clone()
            .ok_or(ModelLifecycleErrorV1::Rejected)?;
        if !matches!(
            previous,
            SemanticModelLifecycleStateV1::Ready { .. }
                | SemanticModelLifecycleStateV1::Installed { .. }
        ) {
            return Err(ModelLifecycleErrorV1::Rejected);
        }
        if !self.install_is_admissible(&previous) {
            // A stale rollback pointer must never resurrect a path that was
            // already retired. Leave the durable state untouched so callers
            // can report the rejection and select a fresh artifact.
            return Err(ModelLifecycleErrorV1::Rejected);
        }
        let prior_durable = guard.durable.clone();
        let now_unix = current_unix_seconds()?;
        // The current state may be Failed after its install was successfully
        // published. In that shape the public state intentionally omits the
        // path, but rollback still needs to retain the artifact that is being
        // displaced. Reconstruct an Installed rollback pointer from the
        // durable private owner or the active shared-store lease before
        // rotating the active lease.
        let current_state = guard.durable.state.clone();
        let current_rollback = self.rollback_state_for_current_artifact(
            current_state.as_ref(),
            prior_private_install.as_ref(),
            prior_durable.failed_current.as_ref(),
            now_unix,
        );
        if let Some(digest) =
            install_path_of(&previous).and_then(|path| self.artifact_store.installed_digest(path))
        {
            self.artifact_store.activate_artifact_with_rollback(
                &digest,
                &self.lease_id(EMBEDDING_ACTIVE_LEASE_ID_V1),
                &self.lease_id(EMBEDDING_ROLLBACK_LEASE_ID_V1),
                now_unix,
            )?;
        }
        guard.durable.previous_ready = current_rollback;
        guard.durable.selected_model = Some(previous.model_id().to_owned());
        guard.durable.state = Some(previous);
        guard.durable.failed_current = None;
        let replacement_private_install = guard
            .durable
            .state
            .as_ref()
            .and_then(|state| self.private_install_metadata_for_state(state));
        let previous_path = guard
            .durable
            .previous_ready
            .as_ref()
            .and_then(install_path_of);
        let preserves_prior_private_install = previous_path.is_some_and(|path| {
            prior_private_install
                .as_ref()
                .is_some_and(|private| private.install_path == path)
        });
        // Publish the replacement's ownership metadata in the same durable
        // commit as the rolled-back state. When the prior private install is
        // being retired, retain its ownership metadata through this commit so
        // a cleanup failure remains recoverable after a restart.
        replace_private_install(&mut guard.durable, replacement_private_install.clone());
        if let Err(error) = persist_durable(&self.root, &guard.durable) {
            guard.durable = prior_durable.clone();
            self.reconcile_embedding_artifact_leases(&prior_durable, now_unix)?;
            return Err(error);
        }
        // The durable pointer is the lease authority. Reconcile after the
        // rollback commit so a failed state cannot leave the process serving Q
        // while the rollback slot still points at an unrelated artifact.
        if let Err(error) = self.reconcile_embedding_artifact_leases(&guard.durable, now_unix) {
            // Durable rollback is committed even when lease reconciliation is
            // unavailable. Notify activation so it can re-read the state and
            // report the same degraded condition instead of waiting on a
            // stale verified-ready event forever.
            publish_verified_ready_event(&self.verified_ready, &guard);
            return Err(error);
        }
        publish_verified_ready_event(&self.verified_ready, &guard);
        if !preserves_prior_private_install
            && let Some(prior_private_install) = prior_private_install
        {
            self.retire_replaced_private_install(
                &mut guard,
                prior_private_install,
                replacement_private_install,
                None,
            )?;
        }
        drop(guard);
        Ok(self.status())
    }

    fn rollback_state_for_current_artifact(
        &self,
        state: Option<&SemanticModelLifecycleStateV1>,
        private_install: Option<&DurablePrivateInstallV1>,
        failed_current: Option<&SemanticModelLifecycleStateV1>,
        now_unix: u64,
    ) -> Option<SemanticModelLifecycleStateV1> {
        let state = state?;
        if matches!(state, SemanticModelLifecycleStateV1::Ready { .. }) {
            return self
                .lifecycle_state_path_is_admissible(state)
                .then(|| state.clone());
        }

        if let Some(private) = private_install.filter(|private| {
            private.model_id == state.model_id()
                && private.artifact_digest == state.artifact_digest()
        }) {
            let candidate = SemanticModelLifecycleStateV1::Installed {
                model_id: private.model_id.clone(),
                revision: private.revision.clone(),
                artifact_digest: private.artifact_digest.clone(),
                install_path: private.install_path.clone(),
            };
            if self.lifecycle_state_path_is_admissible(&candidate) {
                return Some(candidate);
            }
        }

        if let Some(failed) = failed_current.filter(|failed| {
            self.lifecycle_state_path_is_admissible(failed)
        }) {
            let install_path = install_path_of(failed)?.to_path_buf();
            return Some(SemanticModelLifecycleStateV1::Installed {
                model_id: state.model_id().to_owned(),
                revision: state_revision(state).to_owned(),
                artifact_digest: state.artifact_digest().to_owned(),
                install_path,
            });
        }

        let active_digest = self
            .artifact_store
            .artifact_digest_for_lease(
                &self.lease_id(EMBEDDING_ACTIVE_LEASE_ID_V1),
                ArtifactLeaseKindV1::Active,
                now_unix,
            )
            .ok()
            .flatten()?;
        let install_path = self.artifact_store.installed_directory(&active_digest);
        let candidate = SemanticModelLifecycleStateV1::Installed {
            model_id: state.model_id().to_owned(),
            revision: state_revision(state).to_owned(),
            artifact_digest: state.artifact_digest().to_owned(),
            install_path,
        };
        self.lifecycle_state_path_is_admissible(&candidate)
            .then_some(candidate)
    }

    pub fn mark_loading(
        &self,
        target: &SemanticModelLifecycleMutationTargetV1,
    ) -> Result<(), ModelLifecycleErrorV1> {
        self.transition_installed_like(target, |model_id, revision, digest, path| {
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
        target: &SemanticModelLifecycleMutationTargetV1,
        completed_units: u64,
        total_units: u64,
    ) -> Result<(), ModelLifecycleErrorV1> {
        let _mutation = self.mutation_guard();
        let worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        let mut guard = self.inner.writer();
        if !self.lifecycle_mutation_target_matches(&worker, &guard, target) {
            return Err(ModelLifecycleErrorV1::Rejected);
        }
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
        guard.durable.failed_current = None;
        let private_install = guard
            .durable
            .state
            .as_ref()
            .and_then(|state| self.private_install_metadata_for_state(state));
        replace_private_install(&mut guard.durable, private_install);
        persist_durable(&self.root, &guard.durable)
    }

    pub fn mark_ready(
        &self,
        target: &SemanticModelLifecycleMutationTargetV1,
    ) -> Result<(), ModelLifecycleErrorV1> {
        let _mutation = self.mutation_guard();
        let worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        let mut guard = self.inner.writer();
        self.mark_ready_locked(&worker, &mut guard, target)
    }

    /// Commit one already-prepared runtime observation or restore while the
    /// lifecycle target is reserved. Selection, import, rollback, and runtime
    /// projection pollers all use the same mutation reservation, so the
    /// callback cannot publish an old runtime after a same-model replacement
    /// wins the race. A rejected lifecycle publication is surfaced to the
    /// caller instead of being reported as a successful runtime commit.
    pub fn commit_runtime_ready(
        &self,
        target: &SemanticModelLifecycleMutationTargetV1,
        commit: impl FnOnce() -> bool,
    ) -> Result<bool, ModelLifecycleErrorV1> {
        self.commit_runtime_ready_with_rollback(target, commit, || {})
    }

    /// Commit a process-local runtime and its durable `Ready` state as one
    /// owner transaction. If the durable write fails after `commit` returns
    /// success, invoke `rollback` while the lifecycle mutation reservation is
    /// still held so the external runtime cannot outlive the state it claims.
    pub fn commit_runtime_ready_with_rollback(
        &self,
        target: &SemanticModelLifecycleMutationTargetV1,
        commit: impl FnOnce() -> bool,
        rollback: impl FnOnce(),
    ) -> Result<bool, ModelLifecycleErrorV1> {
        let _mutation = self.mutation_guard();
        let worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        let mut guard = self.inner.writer();
        if !self.lifecycle_mutation_target_matches(&worker, &guard, target) {
            return Err(ModelLifecycleErrorV1::Rejected);
        }
        if !commit() {
            return Ok(false);
        }
        if let Err(error) = self.mark_ready_locked(&worker, &mut guard, target) {
            rollback();
            return Err(error);
        }
        Ok(true)
    }

    fn mark_ready_locked(
        &self,
        worker: &AcquisitionWorkerStateV1,
        guard: &mut LifecycleWriteGuardV1<'_>,
        target: &SemanticModelLifecycleMutationTargetV1,
    ) -> Result<(), ModelLifecycleErrorV1> {
        if !self.lifecycle_mutation_target_matches(worker, guard, target) {
            return Err(ModelLifecycleErrorV1::Rejected);
        }
        let Some(state) = guard.durable.state.clone() else {
            return Err(ModelLifecycleErrorV1::Rejected);
        };
        let prior_durable = guard.durable.clone();
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
        guard.durable.failed_current = None;
        let private_install = guard
            .durable
            .state
            .as_ref()
            .and_then(|state| self.private_install_metadata_for_state(state));
        replace_private_install(&mut guard.durable, private_install);
        if let Err(error) = persist_durable(&self.root, &guard.durable) {
            guard.durable = prior_durable;
            return Err(error);
        }
        publish_verified_ready_event(&self.verified_ready, &guard);
        Ok(())
    }

    pub fn mark_runtime_failed(
        &self,
        target: &SemanticModelLifecycleMutationTargetV1,
        detail: impl Into<String>,
        retryable: bool,
    ) -> Result<(), ModelLifecycleErrorV1> {
        let _mutation = self.mutation_guard();
        let worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        let mut guard = self.inner.writer();
        if !self.lifecycle_mutation_target_matches(&worker, &guard, target) {
            return Err(ModelLifecycleErrorV1::Rejected);
        }
        let Some(state) = guard.durable.state.clone() else {
            return Err(ModelLifecycleErrorV1::Rejected);
        };
        let prior_durable = guard.durable.clone();
        let private_install = self.private_install_metadata_for_state(&state);
        let failed_current = install_path_of(&state).is_some().then_some(state.clone());
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
        guard.durable.failed_current = failed_current;
        replace_private_install(&mut guard.durable, private_install);
        if let Err(error) = persist_durable(&self.root, &guard.durable) {
            // A failed runtime projection must not leave the in-memory
            // lifecycle claiming a failure that never reached disk.
            // Restore the previous state and ownership atomically.
            guard.durable = prior_durable;
            return Err(error);
        }
        Ok(())
    }

    fn transition_installed_like(
        &self,
        target: &SemanticModelLifecycleMutationTargetV1,
        build: impl FnOnce(String, String, String, PathBuf) -> SemanticModelLifecycleStateV1,
    ) -> Result<(), ModelLifecycleErrorV1> {
        let _mutation = self.mutation_guard();
        let worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        let mut guard = self.inner.writer();
        if !self.lifecycle_mutation_target_matches(&worker, &guard, target) {
            return Err(ModelLifecycleErrorV1::Rejected);
        }
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
        guard.durable.failed_current = None;
        let private_install = guard
            .durable
            .state
            .as_ref()
            .and_then(|state| self.private_install_metadata_for_state(state));
        replace_private_install(&mut guard.durable, private_install);
        persist_durable(&self.root, &guard.durable)
    }

    fn spawn_acquire(&self, require_auto_download: bool, expected_model_id: Option<&str>) -> bool {
        let _mutation = self.mutation_guard();
        self.spawn_acquire_locked(require_auto_download, expected_model_id)
    }

    fn spawn_acquire_locked(
        &self,
        require_auto_download: bool,
        expected_model_id: Option<&str>,
    ) -> bool {
        let mut worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
        // Reaping is the point at which a finished worker's typed terminal
        // outcome becomes visible. The outcome may be consumed below only
        // after the demand and selection fences have been checked.
        let (joined_result, token) = worker.reap_finished();
        if self
            .cleanup_joined_worker(
                &mut worker,
                token,
                &joined_result,
                JoinedWorkerCleanupModeV1::PreserveReferencedInstall,
            )
            .is_err()
        {
            return false;
        }
        worker.discard_stale_outcome();
        if worker.handle.is_some() {
            return false;
        }
        // Cleanup failures retain their path-bearing evidence until an
        // explicit resolver or retirement operation handles it. They may not
        // be hidden by a later demand, even when their selection is stale.
        if worker.outcome.is_some() && !worker.outcome_is_retryable() {
            return false;
        }
        let root = self.root.clone();
        let catalog = self.catalog.clone();
        let source = Arc::clone(&self.source);
        let inner = Arc::clone(&self.inner);
        let (selected, reset_interrupted_state) = {
            let guard = inner.read();
            if require_auto_download && !guard.durable.auto_download {
                return false;
            }
            let state_is_retryable = matches!(
                guard.durable.state.as_ref(),
                Some(SemanticModelLifecycleStateV1::SelectedNotDownloaded { .. })
                    | Some(SemanticModelLifecycleStateV1::Failed {
                        retryable: true,
                        ..
                    })
            );
            let state_is_interrupted = matches!(
                guard.durable.state.as_ref(),
                Some(
                    SemanticModelLifecycleStateV1::Downloading { .. }
                        | SemanticModelLifecycleStateV1::Verifying { .. }
                )
            );
            let outcome_matches_selection = worker
                .outcome_model_id
                .as_deref()
                .zip(worker.outcome_selection_generation)
                .is_some_and(|(model_id, generation)| {
                    guard.durable.selected_model.as_deref() == Some(model_id)
                        && generation == worker.selection_generation
                });
            let outcome_is_retryable = worker.outcome_is_retryable();
            if let Some(_outcome) = worker.outcome.as_ref() {
                if outcome_matches_selection && !outcome_is_retryable {
                    return false;
                }
                if !state_is_retryable
                    && !(state_is_interrupted && outcome_matches_selection && outcome_is_retryable)
                {
                    return false;
                }
            } else if !state_is_retryable && !state_is_interrupted {
                return false;
            }
            let selected = guard.durable.selected_model.clone();
            if expected_model_id.is_some_and(|expected| selected.as_deref() != Some(expected)) {
                return false;
            }
            (selected, state_is_interrupted)
        };
        let Some(model_id) = selected else {
            return false;
        };
        if reset_interrupted_state {
            let mut guard = inner.writer();
            let Some(state) = guard.durable.state.clone() else {
                return false;
            };
            let next = match state {
                SemanticModelLifecycleStateV1::Downloading {
                    model_id: state_model_id,
                    revision,
                    artifact_digest,
                    ..
                }
                | SemanticModelLifecycleStateV1::Verifying {
                    model_id: state_model_id,
                    revision,
                    artifact_digest,
                } if state_model_id == model_id
                    && guard.durable.selected_model.as_deref() == Some(model_id.as_str()) =>
                {
                    SemanticModelLifecycleStateV1::SelectedNotDownloaded {
                        model_id: state_model_id,
                        revision,
                        artifact_digest,
                    }
                }
                _ => return false,
            };
            let prior = guard.durable.clone();
            guard.durable.state = Some(next);
            if persist_durable(&self.root, &guard.durable).is_err() {
                guard.durable = prior;
                return false;
            }
        }
        if worker.outcome.is_some() {
            worker.clear_outcome();
            // A retryable outcome may have a retained cleanup or join outcome
            // behind it. Promotion happens when the retry consumes the
            // current outcome, so re-check the promoted value before starting
            // another worker; otherwise a cleanup debt could be hidden by the
            // new demand.
            if worker.outcome.is_some() && !worker.outcome_is_retryable() {
                return false;
            }
        }
        let epoch = self.acquisition.begin_epoch();
        let model = match catalog.get(&model_id) {
            Some(model) => model,
            None => return false,
        };
        let digest = catalog_package_digest(model);
        let ownership = Arc::new(Mutex::new(AcquisitionWorkerPathOwnershipV1::default()));
        // Claim the collision-safe staging path before the worker is mounted.
        // The token and worker then share the exact directory that this
        // owner created; a pre-existing directory or dangling symlink can
        // never be mistaken for this worker's staging.
        let staging_path = match claim_staging_path_for_acquisition(&root, &model_id, &digest) {
            Ok(path) => path,
            Err(error) => {
                let _ = epoch.while_active(|| {
                    set_failed_state(
                        &root,
                        &inner,
                        model,
                        &digest,
                        &format!("cannot claim acquisition staging directory: {error}"),
                        true,
                    )
                });
                return false;
            }
        };
        // The claim is ownership immediately, even before the worker has a
        // chance to persist its first Downloading projection. If that write
        // or thread creation fails, the same ownership flag drives cleanup
        // (or durable debt retention) instead of leaking the claimed path.
        ownership
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .staging = true;
        let token = AcquisitionWorkerTokenV1 {
            model_id: model_id.clone(),
            revision: model.source.revision.clone(),
            artifact_digest: digest.clone(),
            selection_generation: worker.selection_generation,
            epoch: epoch.clone(),
            staging_path: staging_path.clone(),
            install_path: install_path_for(&root, &model_id, &model.source.revision, &digest),
            ownership: Arc::clone(&ownership),
        };
        let worker_root = root.clone();
        let worker_catalog = catalog.clone();
        let worker_model_id = model_id.clone();
        let worker_epoch = epoch.clone();
        let worker_inner = Arc::clone(&inner);
        let verified_ready = self.verified_ready.clone();
        let shared_store = self
            .lease_namespace
            .as_ref()
            .map(|_| Arc::clone(&self.artifact_store));
        let active_lease = self.lease_id(EMBEDDING_ACTIVE_LEASE_ID_V1);
        let rollback_lease = self.lease_id(EMBEDDING_ROLLBACK_LEASE_ID_V1);
        let worker_staging_path = staging_path.clone();
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
                    &worker_epoch,
                    &worker_inner,
                    &verified_ready,
                    shared_store
                        .as_deref()
                        .map(|store| (store, active_lease.as_str(), rollback_lease.as_str())),
                    worker_staging_path,
                    ownership,
                )
            });
        match handle {
            Ok(join) => {
                worker.handle = Some(join);
                worker.active_token = Some(token);
                true
            }
            Err(error) => {
                // `claim_staging_path_for_acquisition` ran before thread
                // creation, so a spawn failure must release that ownership
                // even though no worker token was installed in `worker`.
                // Persist the exact path if cleanup itself fails; otherwise
                // the failed spawn would leave an unowned private directory
                // for a later process to discover or delete accidentally.
                if let Err(cleanup_error) = cleanup_private_owned_path(&root, &staging_path) {
                    let mut guard = inner.writer();
                    retain_private_install_debt(
                        &mut guard.durable,
                        DurablePrivateInstallV1 {
                            model_id: model.model_id.clone(),
                            revision: model.source.revision.clone(),
                            artifact_digest: digest.clone(),
                            install_path: staging_path.clone(),
                        },
                    );
                    let _ = persist_durable(&root, &guard.durable);
                    crate::hotpath_observe::record_lifecycle_error(&cleanup_error);
                }
                let _ = epoch.while_active(|| {
                    set_failed_state(
                        &root,
                        &inner,
                        model,
                        &digest,
                        &format!("cannot start background acquisition worker: {error}"),
                        true,
                    )
                });
                false
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
        let model = self
            .catalog
            .get(&model_id)
            .ok_or(ModelLifecycleErrorV1::Rejected)?;
        let digest = catalog_package_digest(model);
        let ownership = Arc::new(Mutex::new(AcquisitionWorkerPathOwnershipV1::default()));
        let staging_path = match claim_staging_path_for_acquisition(&self.root, &model_id, &digest) {
            Ok(path) => path,
            Err(_) => {
                let _ = epoch.while_active(|| {
                    set_failed_state(
                        &self.root,
                        &self.inner,
                        model,
                        &digest,
                        &ModelLifecycleErrorV1::StoreUnavailable.to_string(),
                        true,
                    )
                });
                return Err(ModelLifecycleErrorV1::StoreUnavailable);
            }
        };
        ownership
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .staging = true;
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
            staging_path,
            ownership,
        )
    }
}

fn map_reranker_admission_error(
    error: super::rerank_adapter::RerankArtifactAdmissionErrorV1,
) -> ModelLifecycleErrorV1 {
    match error {
        super::rerank_adapter::RerankArtifactAdmissionErrorV1::IncompatiblePins
        | super::rerank_adapter::RerankArtifactAdmissionErrorV1::IncompatibleArtifact => {
            ModelLifecycleErrorV1::VerificationFailed
        }
        super::rerank_adapter::RerankArtifactAdmissionErrorV1::Unavailable => {
            ModelLifecycleErrorV1::RerankerUnavailable
        }
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
        Err(error @ ModelLifecycleErrorV1::CancellationCleanupQuarantined(_))
        | Err(error @ ModelLifecycleErrorV1::CancellationCleanupFailed(_)) => Err(error),
        Err(_) => Ok(()),
    }
}
