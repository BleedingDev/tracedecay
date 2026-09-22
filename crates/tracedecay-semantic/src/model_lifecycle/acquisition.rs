fn current_unix_seconds() -> Result<u64, ModelLifecycleErrorV1> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| ModelLifecycleErrorV1::StoreUnavailable)
}
/// The model one acquisition run installs and the root it installs into.
#[derive(Clone, Copy)]
struct AcquisitionTargetV1<'a> {
    root: &'a Path,
    catalog: &'a FastEmbedModelCatalogV1,
    source: &'a dyn ModelMemberSourceV1,
    model_id: &'a str,
}

fn staging_path_for(root: &Path, model_id: &str, digest: &str) -> PathBuf {
    root.join("staging")
        .join(format!("{model_id}-{}", &digest[..16.min(digest.len())]))
}

/// Claim a fresh staging directory without adopting or deleting a path left
/// by another worker or an earlier process. `create_dir` is the ownership
/// boundary, so dangling symlinks and concurrent claims are both treated as
/// occupied paths.
fn claim_staging_path_for_acquisition(
    root: &Path,
    model_id: &str,
    digest: &str,
) -> io::Result<PathBuf> {
    let staging_root = root.join("staging");
    ensure_lifecycle_directory(&staging_root)?;
    let base = staging_path_for(root, model_id, digest);
    let mut candidate = base.clone();
    let mut suffix = 0_u64;
    loop {
        match fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                suffix = suffix.saturating_add(1);
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |duration| duration.as_nanos());
                let nonce = LIFECYCLE_PRIVATE_PATH_NONCE.fetch_add(1, Ordering::Relaxed);
                candidate = staging_root.join(format!(
                    "{model_id}-{}-{}-{}-{nonce}-{suffix}",
                    &digest[..16.min(digest.len())],
                    std::process::id(),
                    timestamp,
                ));
            }
            Err(error) => return Err(error),
        }
    }
}

#[hotpath::measure(label = "semantic.model_lifecycle.acquire")]
fn run_acquisition(
    target: AcquisitionTargetV1<'_>,
    epoch: &AcquisitionEpochV1,
    inner: &LifecyclePublicationGateV1,
    verified_ready: &watch::Sender<SemanticLifecycleVerifiedReadyEventV1>,
    shared_store: Option<(&ModelArtifactStore, &str, &str)>,
) -> Result<(), ModelLifecycleErrorV1> {
    let AcquisitionTargetV1 {
        root,
        catalog,
        model_id,
        ..
    } = target;
    let mut result = run_acquisition_inner(target, epoch, inner, verified_ready, shared_store);
    match &result {
        Ok(()) => crate::hotpath_observe::record_model_state("installed"),
        Err(error) => {
            crate::hotpath_observe::record_lifecycle_error(error);
        }
    }
    let failure = result.as_ref().err().cloned();
    if let Some(error) = failure
        && !matches!(&error, ModelLifecycleErrorV1::Cancelled)
        && let Some(model) = catalog.get(model_id)
    {
        let already_failed = {
            let guard = inner.read();
            matches!(
                guard.durable.state.as_ref(),
                Some(SemanticModelLifecycleStateV1::Failed { .. })
            )
        };
        if !already_failed {
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
            // A terminal acquisition failure must remain observable even when
            // recording the durable Failed state also fails. Returning the
            // latter preserves the storage error instead of silently reducing
            // it to the original download/verification reason.
            if let Err(state_error) = epoch.while_current(|| {
                set_failed_state(
                    root,
                    inner,
                    model,
                    &catalog_package_digest(model),
                    &error.to_string(),
                    retryable,
                )
            }) && !matches!(state_error, ModelLifecycleErrorV1::Cancelled)
            {
                result = Err(state_error);
            }
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
    let bytes_total = model
        .members
        .values()
        .try_fold(0_u64, |total, member| total.checked_add(member.length))
        .ok_or(ModelLifecycleErrorV1::Rejected)?;

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
        persist_durable(root, &guard.durable)
    })?;
    crate::hotpath_observe::record_model_state("downloading");

    let staging = claim_staging_path_for_acquisition(root, &model.model_id, &digest)
        .map_err(|_| ModelLifecycleErrorV1::StoreUnavailable)?;

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
                return fail_state(root, inner, &model, &digest, &error.to_string(), true);
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
                if !private_cleanup_path_allowed(root, &staging) {
                    return Err(ModelLifecycleErrorV1::VerificationFailed);
                }
                remove_private_path(root, &staging)
                    .map_err(|_| ModelLifecycleErrorV1::VerificationFailed)?;
                return fail_state(
                    root,
                    inner,
                    &model,
                    &digest,
                    "member length or sha256 mismatch",
                    true,
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
        if !private_cleanup_path_allowed(root, &staging) {
            return Err(ModelLifecycleErrorV1::InstallFailed);
        }
        remove_private_path(root, &staging).map_err(|_| ModelLifecycleErrorV1::InstallFailed)?;
        return epoch.while_active(|| {
            let mut guard = inner.writer();
            let prior = guard.durable.clone();
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
    // and the durable install.json write.
    hotpath::measure_block!("semantic.acquire.install", {
        if let Some(parent) = install_path.parent() {
            ensure_lifecycle_directory(parent).map_err(|_| ModelLifecycleErrorV1::InstallFailed)?;
        }
        if fs::symlink_metadata(&install_path).is_ok() {
            let prior_install = SemanticModelLifecycleStateV1::Installed {
                model_id: model.model_id.clone(),
                revision: model.source.revision.clone(),
                artifact_digest: digest.clone(),
                install_path: install_path.clone(),
            };
            if !private_install_path_is_safe(root, &prior_install, &install_path) {
                return Err(ModelLifecycleErrorV1::InstallFailed);
            }
            remove_private_install_path(root, &install_path)
                .map_err(|_| ModelLifecycleErrorV1::InstallFailed)?;
        }
        // Atomic publish: rename fully verified staging directory into place.
        fs::rename(&staging, &install_path).map_err(|_| ModelLifecycleErrorV1::InstallFailed)?;
        if epoch.ensure_active().is_err() {
            cleanup_cancelled_path(root, &install_path, epoch)?;
            return Err(ModelLifecycleErrorV1::Cancelled);
        }
        let meta = InstallMetaV1 {
            schema: INSTALL_META_SCHEMA_V1.to_owned(),
            model_id: model.model_id.clone(),
            revision: model.source.revision.clone(),
            artifact_digest: digest.clone(),
        };
        let metadata = write_json_atomic(&install_path.join("install.json"), &meta)
            .map_err(|_| ModelLifecycleErrorV1::InstallFailed);
        if epoch.ensure_active().is_err() {
            cleanup_cancelled_path(root, &install_path, epoch)?;
            return Err(ModelLifecycleErrorV1::Cancelled);
        }
        metadata?;
    });

    let publication = epoch.while_active(|| {
        let mut guard = inner.writer();
        guard.durable.state = Some(SemanticModelLifecycleStateV1::Installed {
            model_id: model.model_id.clone(),
            revision: model.source.revision.clone(),
            artifact_digest: digest,
            install_path: install_path.clone(),
        });
        persist_durable(root, &guard.durable)?;
        publish_verified_ready_event(verified_ready, &guard);
        Ok(())
    });
    if matches!(&publication, Err(ModelLifecycleErrorV1::Cancelled)) {
        cleanup_cancelled_path(root, &install_path, epoch)?;
    }
    publication
}

fn cleanup_cancelled_path(
    root: &Path,
    path: &Path,
    epoch: &AcquisitionEpochV1,
) -> Result<(), ModelLifecycleErrorV1> {
    epoch.while_current(|| {
        match fs::symlink_metadata(path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(_) => {
                return Err(ModelLifecycleErrorV1::CancellationCleanupFailed(
                    path.to_path_buf(),
                ));
            }
            Ok(metadata)
                if metadata.file_type().is_symlink()
                    || !metadata.is_dir()
                    || !private_cleanup_path_allowed(root, path) =>
            {
                return Err(ModelLifecycleErrorV1::CancellationCleanupFailed(
                    path.to_path_buf(),
                ));
            }
            Ok(_) => {}
        }
        remove_private_path(root, path)
            .map_err(|_| ModelLifecycleErrorV1::CancellationCleanupFailed(path.to_path_buf()))
    })
}

fn private_cleanup_path_allowed(root: &Path, path: &Path) -> bool {
    let Ok(relative) = path.strip_prefix(root) else {
        return false;
    };
    let components = relative.components().collect::<Vec<_>>();
    if components.is_empty()
        || !components
            .iter()
            .all(|component| matches!(component, std::path::Component::Normal(_)))
    {
        return false;
    }
    let Some(std::path::Component::Normal(base)) = components.first() else {
        return false;
    };
    if !matches!(base.to_str(), Some("staging" | "installs" | "quarantine")) {
        return false;
    }
    let Ok(root_metadata) = fs::symlink_metadata(root) else {
        return false;
    };
    if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
        return false;
    }
    let mut current = root.to_path_buf();
    for component in components {
        current.push(component.as_os_str());
        let Ok(metadata) = fs::symlink_metadata(&current) else {
            return false;
        };
        if metadata.file_type().is_symlink() {
            return false;
        }
        if current != path && !metadata.is_dir() {
            return false;
        }
    }
    true
}

fn fail_state(
    root: &Path,
    inner: &LifecyclePublicationGateV1,
    model: &CatalogedFastEmbedModelV1,
    digest: &str,
    detail: &str,
    retryable: bool,
) -> Result<(), ModelLifecycleErrorV1> {
    set_failed_state(root, inner, model, digest, detail, retryable)?;
    Err(if retryable {
        ModelLifecycleErrorV1::DownloadFailed
    } else {
        ModelLifecycleErrorV1::VerificationFailed
    })
}
