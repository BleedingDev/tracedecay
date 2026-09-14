fn current_unix_seconds() -> Result<u64, ModelLifecycleErrorV1> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| ModelLifecycleErrorV1::StoreUnavailable)
}

fn staging_path_for(root: &Path, model_id: &str, digest: &str) -> PathBuf {
    root.join("staging")
        .join(format!("{}-{}", model_id, &digest[..16.min(digest.len())]))
}

static PRIVATE_BACKUP_NONCE: AtomicU64 = AtomicU64::new(0);

/// Pick an acquisition staging path without taking ownership of a path that
/// survived a prior process or belongs to another user. The historical base
/// name remains deterministic when unused (which keeps resume diagnostics
/// readable); a collision gets a process/restart-unique suffix and the old
/// directory is left for startup reconciliation.
fn staging_path_for_acquisition(root: &Path, model_id: &str, digest: &str) -> PathBuf {
    let base = staging_path_for(root, model_id, digest);
    if fs::symlink_metadata(&base).is_err() {
        return base;
    }
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let nonce = PRIVATE_BACKUP_NONCE.fetch_add(1, AtomicOrdering::Relaxed);
    let leaf = format!(
        "{model_id}-{}-{}-{timestamp}-{nonce}",
        &digest[..16.min(digest.len())],
        std::process::id(),
    );
    let staging_root = root.join("staging");
    let mut candidate = staging_root.join(&leaf);
    let mut collision = 0_u64;
    while fs::symlink_metadata(&candidate).is_ok() {
        collision = collision.saturating_add(1);
        candidate = staging_root.join(format!("{leaf}-{collision}"));
    }
    candidate
}

/// Claim a final staging directory before starting an acquisition worker.
///
/// Choosing a path and creating it are one operation from the point of view
/// of ownership: `create_dir` is the atomic claim. In particular,
/// `Path::exists` cannot be used here because it reports false for a dangling
/// symlink, while `create_dir` correctly reports that symlink as occupied.
/// Every `AlreadyExists` result is treated as a collision and retried with a
/// fresh process/restart-unique suffix.
fn claim_staging_path_for_acquisition(
    root: &Path,
    model_id: &str,
    digest: &str,
) -> io::Result<PathBuf> {
    let staging_root = root.join("staging");
    ensure_lifecycle_directory(&staging_root)?;

    let base = staging_path_for(root, model_id, digest);
    match fs::create_dir(&base) {
        Ok(()) => return Ok(base),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }

    let mut candidate = staging_path_for_acquisition(root, model_id, digest);
    loop {
        match fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                // Re-run the naming helper after a raced collision. It emits
                // fresh process/restart entropy and still checks dangling
                // symlinks, while `create_dir` below remains the authority.
                candidate = staging_path_for_acquisition(root, model_id, digest);
            }
            Err(error) => return Err(error),
        }
    }
}

/// Serializes the selection of a quarantine destination with the rename that
/// claims it. The lock is a small per-root OS lock, so independent lifecycle
/// owners and processes using the same private root cannot both select the
/// same destination and then race through `rename` (whose replacement
/// behavior differs across platforms).
struct LifecycleCleanupFilesystemLockV1(File);

impl LifecycleCleanupFilesystemLockV1 {
    fn acquire(root: &Path) -> io::Result<Self> {
        let path = root.join(".lifecycle-cleanup.lock");
        // Do not follow a pre-existing symlink or non-file at the lock path.
        // This keeps the lock itself inside the lifecycle root, matching the
        // private path allowlist used for every cleanup target.
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "lifecycle cleanup lock is not a regular file",
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
            Ok(_) => {}
        }
        let file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "lifecycle cleanup lock was replaced by a non-file",
            ));
        }
        fs2::FileExt::lock_exclusive(&file)?;
        Ok(Self(file))
    }
}

impl Drop for LifecycleCleanupFilesystemLockV1 {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.0);
    }
}

/// Pick a backup name that remains unique across worker epochs and process
/// restarts. The existence check is deliberately a refusal loop: a recorded
/// `.previous-install-*` path is cleanup evidence and must never be deleted to
/// make room for a later acquisition.
fn private_backup_path_for(root: &Path, model_id: &str, digest: &str, epoch: u64) -> PathBuf {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let nonce = PRIVATE_BACKUP_NONCE.fetch_add(1, AtomicOrdering::Relaxed);
    private_backup_path_for_seed(root, model_id, digest, epoch, timestamp, nonce)
}

/// Resolve a backup name without ever treating an existing filesystem entry
/// as available. The seed-taking form keeps collision handling deterministic
/// in lifecycle tests, including for dangling symlinks that exists() would
/// incorrectly report as absent.
fn private_backup_path_for_seed(
    root: &Path,
    model_id: &str,
    digest: &str,
    epoch: u64,
    timestamp: u128,
    mut nonce: u64,
) -> PathBuf {
    let leaf = format!(
        ".previous-install-{model_id}-{}-{}-{timestamp}-{epoch}-{nonce}",
        &digest[..16.min(digest.len())],
        std::process::id(),
    );
    let staging_root = root.join("staging");
    let mut candidate = staging_root.join(&leaf);
    while fs::symlink_metadata(&candidate).is_ok() {
        nonce = nonce.saturating_add(1);
        let next_leaf = format!(
            ".previous-install-{model_id}-{}-{}-{timestamp}-{epoch}-{nonce}",
            &digest[..16.min(digest.len())],
            std::process::id(),
        );
        candidate = staging_root.join(next_leaf);
    }
    candidate
}

fn quarantine_path_for(
    root: &Path,
    epoch: u64,
    leaf: &str,
) -> io::Result<PathBuf> {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let nonce = PRIVATE_BACKUP_NONCE.fetch_add(1, AtomicOrdering::Relaxed);
    quarantine_path_for_seed(root, epoch, leaf, timestamp, nonce)
}

/// Resolve a quarantine name without ever replacing an existing path. The
/// seed-taking form keeps collision handling deterministic in lifecycle tests
/// while the production wrapper supplies process/restart-unique entropy.
fn quarantine_path_for_seed(
    root: &Path,
    epoch: u64,
    leaf: &str,
    timestamp: u128,
    mut nonce: u64,
) -> io::Result<PathBuf> {
    let quarantine_root = root.join("quarantine");
    loop {
        let candidate = quarantine_root.join(format!(
            "acquisition-{epoch}-{}-{timestamp}-{nonce}-{leaf}",
            std::process::id(),
        ));
        match fs::symlink_metadata(&candidate) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(candidate);
            }
            Ok(_) => {
                nonce = nonce.saturating_add(1);
            }
            Err(error) => return Err(error),
        }
    }
}

/// Move a cleanup path into quarantine without ever replacing a destination.
/// Linux and macOS expose the required atomic primitive; other targets fail
/// closed rather than falling back to replacement-capable `rename`.
fn rename_quarantine_noreplace(source: &Path, destination: &Path) -> io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        use std::ffi::CString;
        use std::os::fd::AsRawFd;
        use std::os::unix::ffi::OsStrExt;

        let source_parent = source
            .parent()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "source has no parent"))?;
        let destination_parent = destination.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "destination has no parent")
        })?;
        let source_name = source
            .file_name()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "source has no name"))?;
        let destination_name = destination.file_name().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "destination has no name")
        })?;
        let source_name = CString::new(source_name.as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "source name contains NUL"))?;
        let destination_name = CString::new(destination_name.as_bytes()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "destination name contains NUL")
        })?;
        let source_parent = File::open(source_parent)?;
        let destination_parent = File::open(destination_parent)?;

        #[cfg(target_os = "linux")]
        {
            unsafe extern "C" {
                fn renameat2(
                    olddirfd: i32,
                    oldpath: *const std::ffi::c_char,
                    newdirfd: i32,
                    newpath: *const std::ffi::c_char,
                    flags: u32,
                ) -> i32;
            }
            const RENAME_NOREPLACE: u32 = 1;
            let result = unsafe {
                renameat2(
                    source_parent.as_raw_fd(),
                    source_name.as_ptr(),
                    destination_parent.as_raw_fd(),
                    destination_name.as_ptr(),
                    RENAME_NOREPLACE,
                )
            };
            return if result == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            };
        }

        #[cfg(target_os = "macos")]
        {
            unsafe extern "C" {
                fn renameatx_np(
                    olddirfd: i32,
                    oldpath: *const std::ffi::c_char,
                    newdirfd: i32,
                    newpath: *const std::ffi::c_char,
                    flags: u32,
                ) -> i32;
            }
            const RENAME_EXCL: u32 = 0x0000_0004;
            let result = unsafe {
                renameatx_np(
                    source_parent.as_raw_fd(),
                    source_name.as_ptr(),
                    destination_parent.as_raw_fd(),
                    destination_name.as_ptr(),
                    RENAME_EXCL,
                )
            };
            return if result == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            };
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (source, destination);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "atomic no-replace quarantine is unavailable on this target",
        ))
    }
}

fn cleanup_worker_owned_paths(
    root: &Path,
    token: &AcquisitionWorkerTokenV1,
    preserve_private_install: bool,
) -> Vec<ModelLifecycleErrorV1> {
    let ownership = token
        .ownership
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let owned_staging = ownership.staging;
    let owned_private_install = ownership.private_install && !preserve_private_install;
    drop(ownership);

    let mut errors = Vec::new();
    if owned_staging
        && let Err(error) = cleanup_owned_path(root, &token.staging_path, token.epoch.epoch)
    {
        errors.push(error);
    }
    if owned_private_install
        && let Err(error) = cleanup_owned_path(root, &token.install_path, token.epoch.epoch)
    {
        errors.push(error);
    }
    errors
}

fn cleanup_owned_path(root: &Path, path: &Path, epoch: u64) -> Result<(), ModelLifecycleErrorV1> {
    if !private_cleanup_path_allowed(root, path) {
        return Err(ModelLifecycleErrorV1::CancellationCleanupFailed(
            path.to_path_buf(),
        ));
    }
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(_) => {
            return Err(ModelLifecycleErrorV1::CancellationCleanupFailed(
                path.to_path_buf(),
            ));
        }
        Ok(_) => {}
    }
    if fs::remove_dir_all(path).is_ok() {
        return Ok(());
    }
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(_) => {
            return Err(ModelLifecycleErrorV1::CancellationCleanupFailed(
                path.to_path_buf(),
            ));
        }
        Ok(_) => {}
    }
    let quarantine_root = root.join("quarantine");
    ensure_lifecycle_directory(&quarantine_root)
        .map_err(|_| ModelLifecycleErrorV1::CancellationCleanupFailed(path.to_path_buf()))?;
    let _filesystem_lock = LifecycleCleanupFilesystemLockV1::acquire(root)
        .map_err(|_| ModelLifecycleErrorV1::CancellationCleanupFailed(path.to_path_buf()))?;
    // Another owner may have completed the cleanup while this caller was
    // waiting for the root lock. Recheck under the same lock used for
    // destination selection and rename before reporting a spurious failure.
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(_) => {
            return Err(ModelLifecycleErrorV1::CancellationCleanupFailed(
                path.to_path_buf(),
            ));
        }
        Ok(_) => {}
    }
    let leaf = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("unknown");
    let quarantine_path = quarantine_path_for(root, epoch, leaf)
        .map_err(|_| ModelLifecycleErrorV1::CancellationCleanupFailed(path.to_path_buf()))?;
    if !private_cleanup_path_allowed(root, &quarantine_path) {
        return Err(ModelLifecycleErrorV1::CancellationCleanupFailed(
            path.to_path_buf(),
        ));
    }
    rename_quarantine_noreplace(path, &quarantine_path)
        .map_err(|_| ModelLifecycleErrorV1::CancellationCleanupFailed(path.to_path_buf()))?;
    match fs::remove_dir_all(&quarantine_path) {
        Ok(()) if !quarantine_path.exists() => Ok(()),
        Ok(()) => Err(ModelLifecycleErrorV1::CancellationCleanupQuarantined(
            quarantine_path,
        )),
        Err(_) => Err(ModelLifecycleErrorV1::CancellationCleanupQuarantined(
            quarantine_path,
        )),
    }
}

/// Retire an already-durable private install without moving it to quarantine.
/// The caller persists this exact ownership path before attempting removal;
/// keeping the path stable on failure lets restart recover the debt even when
/// a second lifecycle write is unavailable.
fn cleanup_private_owned_path(root: &Path, path: &Path) -> Result<(), ModelLifecycleErrorV1> {
    if !private_cleanup_path_allowed(root, path) {
        return Err(ModelLifecycleErrorV1::Rejected);
    }
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(_) => {
            return Err(ModelLifecycleErrorV1::CancellationCleanupFailed(
                path.to_path_buf(),
            ));
        }
        Ok(_) => {}
    }
    remove_path_if_present(path).map_err(|_| {
        ModelLifecycleErrorV1::CancellationCleanupFailed(path.to_path_buf())
    })?;
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Ok(_) | Err(_) => Err(ModelLifecycleErrorV1::CancellationCleanupFailed(
            path.to_path_buf(),
        )),
    }
}

fn remove_path_if_present(path: &Path) -> io::Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    #[cfg(test)]
    if path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with(".previous-install-"))
        && path.exists()
        && path
            .parent()
            .and_then(Path::parent)
            .map(|root| root.join(".fail-private-backup-cleanup"))
            .is_some_and(|marker| {
                if marker.is_file() {
                    let _ = fs::remove_file(marker);
                    true
                } else {
                    false
                }
            })
    {
        return Err(io::Error::other("injected private backup cleanup failure"));
    }
    #[cfg(test)]
    if path
        .ancestors()
        .find_map(|ancestor| {
            let marker = ancestor.join(".fail-private-owned-cleanup");
            marker.is_file().then(|| {
                let _ = fs::remove_file(marker);
            })
        })
        .is_some()
    {
        return Err(io::Error::other("injected private-owned cleanup failure"));
    }
    if metadata.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
}

fn resolve_cleanup_path(path: &Path) -> io::Result<()> {
    remove_path_if_present(path)?;
    match fs::symlink_metadata(path) {
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::Other,
            "cleanup path remains after removal",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// The model one acquisition run installs and the root it installs into.
#[derive(Clone, Copy)]
struct AcquisitionTargetV1<'a> {
    root: &'a Path,
    catalog: &'a FastEmbedModelCatalogV1,
    source: &'a dyn ModelMemberSourceV1,
    model_id: &'a str,
}

#[hotpath::measure(label = "semantic.model_lifecycle.acquire")]
fn run_acquisition(
    target: AcquisitionTargetV1<'_>,
    epoch: &AcquisitionEpochV1,
    inner: &LifecyclePublicationGateV1,
    verified_ready: &watch::Sender<SemanticLifecycleVerifiedReadyEventV1>,
    shared_store: Option<(&ModelArtifactStore, &str, &str)>,
    staging: PathBuf,
    ownership: Arc<Mutex<AcquisitionWorkerPathOwnershipV1>>,
) -> Result<(), ModelLifecycleErrorV1> {
    let AcquisitionTargetV1 {
        root,
        catalog,
        model_id,
        ..
    } = target;
    let result = run_acquisition_inner(
        target,
        epoch,
        inner,
        verified_ready,
        shared_store,
        staging,
        ownership,
    );
    match &result {
        Ok(()) => crate::hotpath_observe::record_model_state("installed"),
        Err(error) => {
            crate::hotpath_observe::record_lifecycle_error(error);
        }
    }
    if let Err(error) = &result
        && !matches!(error, ModelLifecycleErrorV1::Cancelled)
        && let Some(model) = catalog.get(model_id)
    {
        let (already_failed, already_published) = {
            let guard = inner.read();
            let already_failed = matches!(
                guard.durable.state.as_ref(),
                Some(SemanticModelLifecycleStateV1::Failed { .. })
            );
            // A private replacement commits `Installed` and wakes the
            // verified-ready event before retiring the old backup. If backup
            // cleanup then fails, the acquisition reports InstallFailed so
            // callers can observe the debt, but the newly published artifact
            // remains the current loadable lifecycle state. Do not project
            // that post-publication cleanup error back into unusable Failed.
            let already_published = matches!(
                guard.durable.state.as_ref(),
                Some(SemanticModelLifecycleStateV1::Installed {
                    model_id: state_model_id,
                    artifact_digest,
                    install_path,
                    ..
                }) if state_model_id == model_id
                    && artifact_digest == &catalog_package_digest(model)
                    && private_cleanup_path_allowed(root, install_path)
                    && install_path.exists()
            );
            (already_failed, already_published)
        };
        if !already_failed && !already_published {
            let retryable = matches!(
                error,
                ModelLifecycleErrorV1::StoreUnavailable
                    | ModelLifecycleErrorV1::DownloadFailed
                    | ModelLifecycleErrorV1::DownloadFailedWithReason(_)
                    | ModelLifecycleErrorV1::InstallFailed
                    | ModelLifecycleErrorV1::ArtifactImport(
                        ArtifactImportErrorV1::StagingUnavailable
                    )
            );
            let _ = epoch.while_active(|| {
                set_failed_state(
                    root,
                    inner,
                    model,
                    &catalog_package_digest(model),
                    &error.to_string(),
                    retryable,
                )
            });
        }
    }
    result
}
fn run_acquisition_inner(
    target: AcquisitionTargetV1<'_>,
    epoch: &AcquisitionEpochV1,
    inner: &LifecyclePublicationGateV1,
    verified_ready: &watch::Sender<SemanticLifecycleVerifiedReadyEventV1>,
    shared_store: Option<(&ModelArtifactStore, &str, &str)>,
    staging: PathBuf,
    ownership: Arc<Mutex<AcquisitionWorkerPathOwnershipV1>>,
) -> Result<(), ModelLifecycleErrorV1> {
    let AcquisitionTargetV1 {
        root,
        catalog,
        source,
        model_id,
    } = target;
    let model = catalog
        .get(model_id)
        .ok_or(CatalogErrorV1::UnknownModel)?
        .clone();
    let digest = catalog_package_digest(&model);
    let bytes_total: u64 = model.members.values().map(|member| member.length).sum();

    epoch.while_active(|| {
        let mut guard = inner.writer();
        guard.durable.selected_model = Some(model.model_id.clone());
        guard.durable.state = Some(SemanticModelLifecycleStateV1::Downloading {
            model_id: model.model_id.clone(),
            revision: model.source.revision.clone(),
            artifact_digest: digest.clone(),
            bytes_received: 0,
            bytes_total,
        });
        guard.durable.failed_current = None;
        persist_durable(root, &guard.durable)
    })?;
    crate::hotpath_observe::record_model_state("downloading");

    if !private_cleanup_path_allowed(root, &staging) {
        return Err(ModelLifecycleErrorV1::StoreUnavailable);
    }
    ensure_lifecycle_directory(&staging)
        .map_err(|_| ModelLifecycleErrorV1::StoreUnavailable)?;
    if !private_cleanup_path_allowed(root, &staging) {
        return Err(ModelLifecycleErrorV1::StoreUnavailable);
    }
    ownership
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .staging = true;

    let mut bytes_received = 0_u64;
    hotpath::gauge!("semantic_acquire_bytes_total").set(bytes_total);
    hotpath::gauge!("semantic_acquire_bytes_received").set(bytes_received);
    // Network + staging-copy phase. Early cancellation/failure returns drop
    // the span guard, so aborted downloads still record their duration.
    hotpath::measure_block!("semantic.acquire.download", {
        for member in model.members.values() {
            if epoch.ensure_active().is_err() {
                cleanup_cancelled_path(root, &staging, epoch)?;
                return Err(ModelLifecycleErrorV1::Cancelled);
            }
            let destination = staging.join(&member.path);
            let fetch = source.fetch_member(&model, &member.upstream_path, &destination);
            if epoch.ensure_active().is_err() {
                cleanup_cancelled_path(root, &staging, epoch)?;
                return Err(ModelLifecycleErrorV1::Cancelled);
            }
            if let Err(error) = fetch {
                return fail_state(
                    root,
                    inner,
                    &model,
                    &digest,
                    &error.to_string(),
                    true,
                    epoch,
                );
            }
            bytes_received = bytes_received.saturating_add(member.length);
            hotpath::gauge!("semantic_acquire_bytes_received").set(bytes_received);
            let progress = epoch.while_active(|| {
                let mut guard = inner.writer();
                guard.durable.state = Some(SemanticModelLifecycleStateV1::Downloading {
                    model_id: model.model_id.clone(),
                    revision: model.source.revision.clone(),
                    artifact_digest: digest.clone(),
                    bytes_received,
                    bytes_total,
                });
                guard.durable.failed_current = None;
                persist_durable(root, &guard.durable)
            });
            if matches!(&progress, Err(ModelLifecycleErrorV1::Cancelled)) {
                cleanup_cancelled_path(root, &staging, epoch)?;
            }
            progress?;
        }
    });

    if epoch.ensure_active().is_err() {
        cleanup_cancelled_path(root, &staging, epoch)?;
        return Err(ModelLifecycleErrorV1::Cancelled);
    }
    let verifying = epoch.while_active(|| {
        let mut guard = inner.writer();
        guard.durable.state = Some(SemanticModelLifecycleStateV1::Verifying {
            model_id: model.model_id.clone(),
            revision: model.source.revision.clone(),
            artifact_digest: digest.clone(),
        });
        guard.durable.failed_current = None;
        persist_durable(root, &guard.durable)
    });
    if matches!(&verifying, Err(ModelLifecycleErrorV1::Cancelled)) {
        cleanup_cancelled_path(root, &staging, epoch)?;
    }
    verifying?;
    crate::hotpath_observe::record_model_state("verifying");

    // Disk read + SHA-256 digest verification of every staged member,
    // separate from the download above and the install rename below.
    hotpath::measure_block!("semantic.acquire.verify", {
        for member in model.members.values() {
            let path = staging.join(&member.path);
            if !verify_member_file(&path, member.length, &member.sha256) {
                fs::remove_dir_all(&staging)
                    .map_err(|_| ModelLifecycleErrorV1::VerificationFailed)?;
                return fail_state(
                    root,
                    inner,
                    &model,
                    &digest,
                    "member length or sha256 mismatch",
                    true,
                    epoch,
                );
            }
            if epoch.ensure_active().is_err() {
                cleanup_cancelled_path(root, &staging, epoch)?;
                return Err(ModelLifecycleErrorV1::Cancelled);
            }
        }
    });

    if epoch.ensure_active().is_err() {
        cleanup_cancelled_path(root, &staging, epoch)?;
        return Err(ModelLifecycleErrorV1::Cancelled);
    }
    if let Some((store, active_lease, rollback_lease)) = shared_store {
        let resources = SemanticResourceCeilings {
            max_sequence_length: model.max_length,
            // Acquisition writes a durable artifact manifest, and its resource ceiling
            // records the shipped capability — not this host's share. Neither
            // operator configuration nor the process memory authority is in scope
            // here; the composition root resolves the live ceiling against the host
            // when it opens a session over the installed artifact.
            max_resident_bytes: Some(
                tracedecay_semantic_contracts::DEFAULT_SEMANTIC_RESIDENT_BYTES,
            ),
            ..SemanticResourceCeilings::default()
        };
        let manifest = catalog_artifact_manifest(&model, resources)?;
        let now_unix = current_unix_seconds()?;
        let record = store.import_local_directory(&manifest, &staging, now_unix)?;
        // Imported bytes belong to inventory even when cancellation races the
        // import. Only owner-private staging may be removed by this worker.
        fs::remove_dir_all(&staging).map_err(|_| ModelLifecycleErrorV1::InstallFailed)?;
        ownership
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .staging = false;
        return epoch.while_active(|| {
            let mut guard = inner.writer();
            let prior = guard.durable.clone();
            retain_private_install_from_state(root, &mut guard.durable);
            store.activate_artifact_with_rollback(
                &record.artifact_digest,
                active_lease,
                rollback_lease,
                now_unix,
            )?;
            // The lifecycle names the catalog package it installed, exactly as
            // the download/verify states before it and the private-root path
            // below do: that digest is the projection identity every vector
            // generation and compatibility pin carries. The inventory's
            // content address (which also hashes host-derived resource
            // ceilings) stays private to the store and is recovered from the
            // install directory when a lease or rollback needs it.
            guard.durable.state = Some(SemanticModelLifecycleStateV1::Installed {
                model_id: model.model_id.clone(),
                revision: model.source.revision.clone(),
                artifact_digest: digest.clone(),
                install_path: store.installed_directory(&record.artifact_digest),
            });
            guard.durable.failed_current = None;
            replace_private_install(&mut guard.durable, None);
            if let Err(error) = persist_durable(root, &guard.durable) {
                guard.durable = prior;
                reconcile_embedding_artifact_leases(
                    store,
                    active_lease,
                    rollback_lease,
                    &guard.durable,
                    now_unix,
                )?;
                return Err(error);
            }
            publish_verified_ready_event(verified_ready, &guard);
            Ok(())
        });
    }
    let install_path = install_path_for(root, &model.model_id, &model.source.revision, &digest);
    // Install-publication disk phase: prior-install removal, atomic rename,
    // durable install.json, and lifecycle publication all share one writer.
    // This prevents an evaluation reader from entering while the old private
    // path is being replaced or while the new path is not yet durable.
    let publication = epoch.while_active(|| {
        let mut guard = inner.writer();
        if !private_cleanup_path_allowed(root, &install_path) {
            return Err(ModelLifecycleErrorV1::InstallFailed);
        }
        let prior_durable = guard.durable.clone();
        // Older lifecycle files may have persisted only the private install
        // path in their state. Recover that ownership before the path is moved
        // aside so a replacement cleanup failure can retain exact metadata.
        retain_private_install_from_state(root, &mut guard.durable);
        let prior_private_install = guard.durable.private_install.clone();
        let backup_path = if install_path.exists() {
            let backup_path = private_backup_path_for(root, &model.model_id, &digest, epoch.epoch);
            if !private_cleanup_path_allowed(root, &backup_path) {
                return Err(ModelLifecycleErrorV1::InstallFailed);
            }
            // A backup path is durable cleanup evidence. Never let a stale
            // marker (including a dangling symlink) be replaced by the old
            // install at this move boundary.
            rename_quarantine_noreplace(&install_path, &backup_path)
                .map_err(|_| ModelLifecycleErrorV1::InstallFailed)?;
            Some(backup_path)
        } else {
            None
        };
        hotpath::measure_block!("semantic.acquire.install", {
            if let Some(parent) = install_path.parent() {
                if let Err(error) = fs::create_dir_all(parent) {
                    restore_private_install(root, &install_path, backup_path.as_deref())?;
                    let _ = error;
                    return Err(ModelLifecycleErrorV1::InstallFailed);
                }
            }
            if !private_cleanup_path_allowed(root, &install_path) {
                restore_private_install(root, &install_path, backup_path.as_deref())?;
                return Err(ModelLifecycleErrorV1::InstallFailed);
            }
            // Atomic publish: rename fully verified staging directory into place.
            if fs::rename(&staging, &install_path).is_err() {
                restore_private_install(root, &install_path, backup_path.as_deref())?;
                return Err(ModelLifecycleErrorV1::InstallFailed);
            }
            {
                let mut owned = ownership.lock().unwrap_or_else(PoisonError::into_inner);
                owned.staging = false;
                owned.private_install = true;
            }
            let meta = InstallMetaV1 {
                schema: INSTALL_META_SCHEMA_V1.to_owned(),
                model_id: model.model_id.clone(),
                revision: model.source.revision.clone(),
                artifact_digest: digest.clone(),
            };
            if write_json_atomic(&install_path.join("install.json"), &meta).is_err() {
                restore_private_install(root, &install_path, backup_path.as_deref())?;
                ownership
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .private_install = false;
                return Err(ModelLifecycleErrorV1::InstallFailed);
            }
        });
        guard.durable.state = Some(SemanticModelLifecycleStateV1::Installed {
            model_id: model.model_id.clone(),
            revision: model.source.revision.clone(),
            artifact_digest: digest.clone(),
            install_path: install_path.clone(),
        });
        guard.durable.failed_current = None;
        replace_private_install(
            &mut guard.durable,
            Some(DurablePrivateInstallV1 {
                model_id: model.model_id.clone(),
                revision: model.source.revision.clone(),
                artifact_digest: digest.clone(),
                install_path: install_path.clone(),
            }),
        );
        if let Err(error) = persist_durable(root, &guard.durable) {
            guard.durable = prior_durable;
            restore_private_install(root, &install_path, backup_path.as_deref())?;
            ownership
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .private_install = false;
            return Err(error);
        }
        // The replacement is now durable and its install path is published.
        // Wake activation before any fallible retirement of the old private
        // path so a cleanup or follow-up persistence error cannot leave the
        // process waiting on an event for a state that already committed.
        publish_verified_ready_event(verified_ready, &guard);
        if let Some(backup_path) = backup_path {
            if remove_path_if_present(&backup_path).is_err() && backup_path.exists() {
                // The newly published install is durable. The old path now
                // lives under a worker-owned backup name, so the original
                // install path is no longer sufficient cleanup evidence.
                // Queue the exact backup path before reporting failure; later
                // lifecycle progress and restart recovery then retain it.
                let mut backup_debt =
                    prior_private_install.unwrap_or_else(|| DurablePrivateInstallV1 {
                        model_id: model.model_id.clone(),
                        revision: model.source.revision.clone(),
                        artifact_digest: digest.clone(),
                        install_path: install_path.clone(),
                    });
                backup_debt.install_path = backup_path;
                retain_private_install_debt(&mut guard.durable, backup_debt);
                if let Err(error) = persist_durable(root, &guard.durable) {
                    return Err(error);
                }
                // Failure to remove the backup must not roll back the new
                // artifact after its durable commit.
                return Err(ModelLifecycleErrorV1::InstallFailed);
            }
            // The old path has been retired successfully. Remove the stale
            // pre-rename ownership entry; if this follow-up persistence fails,
            // restoring the entry below keeps memory and disk consistent until
            // restart filters the now-absent path.
            let prior_debts = guard.durable.private_install_debts.clone();
            remove_private_install_debt(&mut guard.durable, &install_path);
            if let Err(error) = persist_durable(root, &guard.durable) {
                guard.durable.private_install_debts = prior_debts;
                return Err(error);
            }
        }
        Ok(())
    });
    if matches!(&publication, Err(ModelLifecycleErrorV1::Cancelled)) {
        cleanup_cancelled_path(root, &staging, epoch)?;
        // Cancellation before the publication closure begins means this
        // worker never moved its staging directory into the canonical
        // install path. The canonical path may still belong to a prior
        // successful acquisition; only remove it after this worker has
        // atomically published it and recorded that ownership.
        let owns_private_install = ownership
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .private_install;
        if owns_private_install {
            cleanup_owned_path(root, &install_path, epoch.epoch)?;
        }
    }
    publication
}

fn restore_private_install(
    root: &Path,
    install_path: &Path,
    backup_path: Option<&Path>,
) -> Result<(), ModelLifecycleErrorV1> {
    if !private_cleanup_path_allowed(root, install_path)
        || backup_path.is_some_and(|path| !private_cleanup_path_allowed(root, path))
    {
        return Err(ModelLifecycleErrorV1::InstallFailed);
    }
    remove_path_if_present(install_path).map_err(|_| ModelLifecycleErrorV1::InstallFailed)?;
    if let Some(backup_path) = backup_path
        && backup_path.exists()
    {
        fs::rename(backup_path, install_path).map_err(|_| ModelLifecycleErrorV1::InstallFailed)?;
    }
    Ok(())
}

fn cleanup_cancelled_path(
    root: &Path,
    path: &Path,
    epoch: &AcquisitionEpochV1,
) -> Result<(), ModelLifecycleErrorV1> {
    epoch.while_current(|| cleanup_owned_path(root, path, epoch.epoch))
}

fn fail_state(
    root: &Path,
    inner: &LifecyclePublicationGateV1,
    model: &CatalogedFastEmbedModelV1,
    digest: &str,
    detail: &str,
    retryable: bool,
    epoch: &AcquisitionEpochV1,
) -> Result<(), ModelLifecycleErrorV1> {
    // Failure publication is a durable lifecycle mutation. Keep the control
    // mutex held through the write so cancellation or a newer acquisition
    // epoch cannot leave an old worker's Failed state behind.
    epoch.while_active(|| set_failed_state(root, inner, model, digest, detail, retryable))?;
    Err(if retryable {
        ModelLifecycleErrorV1::DownloadFailed
    } else {
        ModelLifecycleErrorV1::VerificationFailed
    })
}

#[cfg(test)]
mod acquisition_race_tests {
    use super::*;

    #[test]
    fn begin_epoch_selection_change_fences_failure_after_worker_preflight() {
        let fixture = tempfile::tempdir().expect("lifecycle fixture");
        let catalog = FastEmbedModelCatalogV1::production();
        let model = catalog
            .get(DEFAULT_FASTEMBED_MODEL_ID)
            .expect("production model")
            .clone();
        let digest = catalog_package_digest(&model);
        let inner = std::sync::Arc::new(LifecyclePublicationGateV1::new(DurableLifecycleV1 {
            schema: LIFECYCLE_SCHEMA_V1.to_owned(),
            selected_model: Some(model.model_id.clone()),
            auto_download: true,
            state: Some(SemanticModelLifecycleStateV1::SelectedNotDownloaded {
                model_id: model.model_id.clone(),
                revision: model.source.revision.clone(),
                artifact_digest: digest.clone(),
            }),
            previous_ready: None,
            failed_current: None,
            private_install: None,
            private_install_debts: Vec::new(),
        }));
        let control = std::sync::Arc::new(AcquisitionControlV1::default());
        let epoch = control.begin_epoch();
        let (checked_tx, checked_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let worker_inner = std::sync::Arc::clone(&inner);
        let worker_root = fixture.path().to_path_buf();
        let worker_model = model.clone();
        let worker_digest = digest.clone();
        let worker = std::thread::spawn(move || {
            epoch.ensure_active().expect("worker preflight");
            checked_tx.send(()).expect("worker checked receiver");
            release_rx.recv().expect("worker release sender");
            fail_state(
                &worker_root,
                &worker_inner,
                &worker_model,
                &worker_digest,
                "stale worker failure",
                true,
                &epoch,
            )
        });

        checked_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("worker passed final preflight");
        // Model selection advances the acquisition epoch. A cancellation bit
        // alone would leave this stale worker in the current generation and
        // permit its delayed failure publication.
        let _new_epoch = control.begin_epoch();
        release_tx.send(()).expect("release worker");

        assert_eq!(
            worker.join().expect("worker join"),
            Err(ModelLifecycleErrorV1::Cancelled),
        );
        let state = inner.read().durable.state.clone();
        assert!(matches!(
            state,
            Some(SemanticModelLifecycleStateV1::SelectedNotDownloaded { model_id, .. })
                if model_id == model.model_id
        ));
    }
}
