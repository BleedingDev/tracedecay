fn set_failed_state(
    root: &Path,
    inner: &LifecyclePublicationGateV1,
    model: &CatalogedFastEmbedModelV1,
    digest: &str,
    detail: &str,
    retryable: bool,
) -> Result<(), ModelLifecycleErrorV1> {
    let mut guard = inner.writer();
    let prior = guard.durable.clone();
    let failed_current = guard
        .durable
        .state
        .clone()
        .filter(|state| install_path_of(state).is_some());
    guard.durable.state = Some(SemanticModelLifecycleStateV1::Failed {
        model_id: model.model_id.clone(),
        revision: model.source.revision.clone(),
        artifact_digest: digest.to_owned(),
        detail: detail.to_owned(),
        retryable,
    });
    guard.durable.failed_current = failed_current;
    if let Err(error) = persist_durable(root, &guard.durable) {
        // Keep the in-memory projection coherent with the last durable write;
        // callers may still inspect/retry the prior state after an injected or
        // transient lifecycle-store failure.
        guard.durable = prior;
        return Err(error);
    }
    Ok(())
}
fn verify_catalog_manifest(
    model: &CatalogedFastEmbedModelV1,
    manifest: &ModelArtifactManifestV1,
) -> Result<(), ModelLifecycleErrorV1> {
    manifest
        .validate()
        .map_err(|_| ModelLifecycleErrorV1::VerificationFailed)?;
    if manifest.payload.artifact_id != model.model_id
        || manifest.payload.dimensions != model.expected_dimensions
        || manifest.payload.truncation.max_length != model.max_length
        || manifest.payload.spdx_license != model.source.license
        || manifest.payload.upstream.revision != model.source.revision
        || manifest.payload.runtime.runtime != model.backend.runtime_family().runtime_family()
        || manifest.payload.runtime.build_revision
            != model.backend.runtime_family().build_revision()
        || manifest.payload.precision != model.backend.precision()
    {
        return Err(ModelLifecycleErrorV1::VerificationFailed);
    }
    for (role_name, catalog_member) in &model.members {
        let role = match role_name.as_str() {
            "model" => ArtifactMemberRoleV1::Model,
            "tokenizer" => ArtifactMemberRoleV1::Tokenizer,
            "config" => ArtifactMemberRoleV1::Config,
            "special_tokens_map" => ArtifactMemberRoleV1::SpecialTokensMap,
            "tokenizer_config" => ArtifactMemberRoleV1::TokenizerConfig,
            _ => return Err(ModelLifecycleErrorV1::VerificationFailed),
        };
        let member = manifest
            .package_member(role)
            .ok_or(ModelLifecycleErrorV1::VerificationFailed)?;
        if member.path != catalog_member.path
            || member.byte_length != catalog_member.length
            || member.digest.as_str() != catalog_member.sha256
        {
            return Err(ModelLifecycleErrorV1::VerificationFailed);
        }
    }
    Ok(())
}
fn load_or_default_durable(
    root: &Path,
    catalog: &FastEmbedModelCatalogV1,
) -> Result<DurableLifecycleV1, ModelLifecycleErrorV1> {
    let path = root.join("lifecycle.json");
    if path.is_file() {
        let bytes = fs::read(&path).map_err(|_| ModelLifecycleErrorV1::StoreUnavailable)?;
        let durable = serde_json::from_slice::<DurableLifecycleV1>(&bytes)
            .map_err(|_| ModelLifecycleErrorV1::VerificationFailed)?;
        if durable.schema != LIFECYCLE_SCHEMA_V1 {
            return Err(ModelLifecycleErrorV1::VerificationFailed);
        }
        return Ok(durable);
    }
    let model = catalog
        .get(DEFAULT_FASTEMBED_MODEL_ID)
        .ok_or(CatalogErrorV1::MissingDefault)?;
    let digest = catalog_package_digest(model);
    let state = if let Some(path) = existing_install_path(root, model, &digest) {
        Some(SemanticModelLifecycleStateV1::Installed {
            model_id: model.model_id.clone(),
            revision: model.source.revision.clone(),
            artifact_digest: digest,
            install_path: path,
        })
    } else {
        Some(SemanticModelLifecycleStateV1::SelectedNotDownloaded {
            model_id: model.model_id.clone(),
            revision: model.source.revision.clone(),
            artifact_digest: digest,
        })
    };
    let durable = DurableLifecycleV1 {
        schema: LIFECYCLE_SCHEMA_V1.to_owned(),
        selected_model: Some(DEFAULT_FASTEMBED_MODEL_ID.to_owned()),
        auto_download: false,
        state,
        previous_ready: None,
        failed_current: None,
        private_install: None,
        private_install_debts: Vec::new(),
    };
    persist_durable(root, &durable)?;
    Ok(durable)
}

fn recover_private_install_metadata(
    root: &Path,
    catalog: &FastEmbedModelCatalogV1,
    durable: &mut DurableLifecycleV1,
) -> bool {
    let mut changed = false;
    changed |= recover_orphaned_private_paths(root, catalog, durable);
    if let Some(failed_current) = durable.failed_current.as_ref()
        && (!matches!(
            durable.state.as_ref(),
            Some(SemanticModelLifecycleStateV1::Failed { .. })
        ) || install_path_of(failed_current).is_none_or(|path| {
            !path.exists()
                || (is_private_install_layout_path(root, path)
                    && !private_cleanup_path_allowed(root, path))
        }))
    {
        // The private recovery pointer is only meaningful alongside the
        // Failed state it explains. Never retain a malformed or missing path
        // merely because it was present in an older lifecycle file.
        durable.failed_current = None;
        changed = true;
    }
    if let Some(private_install) = durable.private_install.clone() {
        if !private_cleanup_path_allowed(root, &private_install.install_path)
            || !private_install.install_path.exists()
        {
            durable.private_install = None;
            changed = true;
            if durable
                .previous_ready
                .as_ref()
                .and_then(install_path_of)
                .is_some_and(|path| path == private_install.install_path.as_path())
            {
                durable.previous_ready = None;
            }
        }
    }
    let current_path = durable
        .private_install
        .as_ref()
        .map(|private| private.install_path.clone());
    let prior_debt_count = durable.private_install_debts.len();
    let mut seen_paths = std::collections::HashSet::new();
    durable.private_install_debts.retain(|debt| {
        private_cleanup_path_allowed(root, &debt.install_path)
            && debt.install_path.exists()
            && current_path
                .as_ref()
                .is_none_or(|current| current != &debt.install_path)
            && seen_paths.insert(debt.install_path.clone())
    });
    if durable.private_install_debts.len() != prior_debt_count {
        changed = true;
    }
    // A lifecycle file may retain a valid private `previous_ready` path even
    // after the current state has moved on. Treat that path as ownership debt
    // before any later progress can replace the current private slot.
    if let Some(previous) = durable.previous_ready.as_ref()
        && let Some(private_install) = private_install_metadata_at_root(root, previous)
        && private_install.install_path.exists()
        && durable
            .private_install
            .as_ref()
            .is_none_or(|current| current.install_path != private_install.install_path)
        && !durable
            .private_install_debts
            .iter()
            .any(|debt| debt.install_path == private_install.install_path)
    {
        durable.private_install_debts.push(private_install);
        changed = true;
    }
    // Older lifecycle readers expect the primary private owner slot to carry
    // a cleanup-only install after a disabled selection. Promote one retained
    // debt into that slot when there is no current state, while leaving every
    // other debt in the collection. This keeps the legacy owner visible after
    // restart without dropping the multi-debt evidence needed for cleanup.
    if durable.private_install.is_none()
        && durable.state.is_none()
        && let Some(private_install) = durable.private_install_debts.first().cloned()
    {
        durable.private_install = Some(private_install.clone());
        remove_private_install_debt(durable, &private_install.install_path);
        changed = true;
    }
    if let Some(state) = durable.state.clone()
        && let Some(path) = install_path_of(&state)
        && is_private_install_layout_path(root, path)
        && (!private_cleanup_path_allowed(root, path) || !path.exists())
    {
        durable.state = Some(SemanticModelLifecycleStateV1::SelectedNotDownloaded {
            model_id: state.model_id().to_owned(),
            revision: state_revision(&state).to_owned(),
            artifact_digest: state.artifact_digest().to_owned(),
        });
        changed = true;
    }
    if durable
        .previous_ready
        .as_ref()
        .and_then(install_path_of)
        .is_some_and(|path| {
            is_private_install_layout_path(root, path)
                && (!private_cleanup_path_allowed(root, path) || !path.exists())
        })
    {
        durable.previous_ready = None;
        changed = true;
    }
    if durable.private_install.is_some() {
        return changed;
    }
    let candidate = match durable.state.as_ref() {
        Some(
            state @ (SemanticModelLifecycleStateV1::Installed { install_path, .. }
            | SemanticModelLifecycleStateV1::Loading { install_path, .. }
            | SemanticModelLifecycleStateV1::Indexing { install_path, .. }
            | SemanticModelLifecycleStateV1::Ready { install_path, .. }),
        ) if is_private_install_layout_path(root, install_path)
            && private_cleanup_path_allowed(root, install_path)
            && install_path.exists() => Some((
            state.model_id().to_owned(),
            state_revision(state).to_owned(),
            state.artifact_digest().to_owned(),
            install_path.clone(),
        )),
        Some(SemanticModelLifecycleStateV1::Failed {
            model_id,
            revision,
            artifact_digest,
            ..
        }) => {
            let path = install_path_for(root, model_id, revision, artifact_digest);
            (private_cleanup_path_allowed(root, &path) && path.exists()).then_some((
                model_id.clone(),
                revision.clone(),
                artifact_digest.clone(),
                path,
            ))
        }
        _ => None,
    };
    let Some((model_id, revision, artifact_digest, install_path)) = candidate else {
        return false;
    };
    let private_install = DurablePrivateInstallV1 {
        model_id,
        revision,
        artifact_digest,
        install_path,
    };
    let private_install_path = private_install.install_path.clone();
    durable.private_install = Some(private_install);
    remove_private_install_debt(durable, &private_install_path);
    true
}

/// Recover staging and quarantine paths left by a worker that was cancelled,
/// panicked, or lost between an atomic rename and lifecycle persistence. The
/// install manifest, when present, restores exact metadata; partial staging
/// only needs an ownership record so cleanup cannot silently forget the path.
fn recover_orphaned_private_paths(
    root: &Path,
    catalog: &FastEmbedModelCatalogV1,
    durable: &mut DurableLifecycleV1,
) -> bool {
    let mut changed = false;
    let mut known_paths: std::collections::HashSet<PathBuf> = durable
        .private_install
        .iter()
        .map(|private| private.install_path.clone())
        .chain(
            durable
                .private_install_debts
                .iter()
                .map(|debt| debt.install_path.clone()),
        )
        .collect();
    // A private install may have been published after the worker's durable
    // state write and before its ownership write. Scan only the cataloged
    // model/revision/digest layout so a crash cannot leave those bytes
    // unowned, while arbitrary files below the owner root are never adopted
    // for deletion.
    for model in &catalog.models {
        let digest = catalog_package_digest(model);
        let path = install_path_for(root, &model.model_id, &model.source.revision, &digest);
        if !path.exists()
            || !private_cleanup_path_allowed(root, &path)
            || known_paths.contains(&path)
        {
            continue;
        }
        let debt = DurablePrivateInstallV1 {
            model_id: model.model_id.clone(),
            revision: model.source.revision.clone(),
            artifact_digest: digest,
            install_path: path.clone(),
        };
        known_paths.insert(path);
        durable.private_install_debts.push(debt);
        changed = true;
    }
    for directory in [root.join("staging"), root.join("quarantine")] {
        let Ok(entries) = fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.exists()
                || !private_cleanup_path_allowed(root, &path)
                || known_paths.contains(&path)
            {
                continue;
            }
            let Some(debt) = orphaned_private_install_for_path(root, catalog, &path) else {
                continue;
            };
            known_paths.insert(path);
            durable.private_install_debts.push(debt);
            changed = true;
        }
    }
    changed
}

fn orphaned_private_install_for_path(
    root: &Path,
    catalog: &FastEmbedModelCatalogV1,
    path: &Path,
) -> Option<DurablePrivateInstallV1> {
    let relative = path.strip_prefix(root).ok()?;
    let mut components = relative.components();
    let directory = components.next()?.as_os_str().to_str()?;
    let name = components.next()?.as_os_str().to_str()?;
    if components.next().is_some() {
        return None;
    }
    match directory {
        "staging" if name.starts_with(".previous-install-") => {}
        "staging"
            if catalog
                .model_ids()
                .any(|model_id| name.starts_with(&format!("{model_id}-"))) => {}
        "quarantine" if name.starts_with("acquisition-") => {}
        _ => return None,
    }
    let metadata = path.join("install.json");
    if let Ok(bytes) = fs::read(metadata)
        && let Ok(meta) = serde_json::from_slice::<InstallMetaV1>(&bytes)
        && meta.schema == INSTALL_META_SCHEMA_V1
    {
        return Some(DurablePrivateInstallV1 {
            model_id: meta.model_id,
            revision: meta.revision,
            artifact_digest: meta.artifact_digest,
            install_path: path.to_path_buf(),
        });
    }
    Some(DurablePrivateInstallV1 {
        model_id: "orphaned".to_owned(),
        revision: "unknown".to_owned(),
        artifact_digest: "unknown".to_owned(),
        install_path: path.to_path_buf(),
    })
}

fn state_revision(state: &SemanticModelLifecycleStateV1) -> &str {
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

fn persist_durable(root: &Path, durable: &DurableLifecycleV1) -> Result<(), ModelLifecycleErrorV1> {
    #[cfg(test)]
    if matches!(
        durable.state.as_ref(),
        Some(SemanticModelLifecycleStateV1::Installed { .. })
    ) {
        let fail_marker = root.join(".fail-installed-lifecycle-persist");
        if fail_marker.is_file() {
            let _ = fs::remove_file(fail_marker);
            return Err(ModelLifecycleErrorV1::StoreUnavailable);
        }
    }
    write_json_atomic(&root.join("lifecycle.json"), durable)
        .map_err(|_| ModelLifecycleErrorV1::StoreUnavailable)
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    {
        let mut file = File::create(&tmp)?;
        serde_json::to_writer_pretty(&mut file, value)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        file.flush()?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

fn install_path_for(root: &Path, model_id: &str, revision: &str, digest: &str) -> PathBuf {
    root.join("installs")
        .join(model_id)
        .join(revision)
        .join(&digest[..16.min(digest.len())])
}

fn existing_install_path(
    root: &Path,
    model: &CatalogedFastEmbedModelV1,
    digest: &str,
) -> Option<PathBuf> {
    let path = install_path_for(root, &model.model_id, &model.source.revision, digest);
    if !private_cleanup_path_allowed(root, &path) {
        return None;
    }
    let meta_path = path.join("install.json");
    if !meta_path.is_file() {
        return None;
    }
    let bytes = fs::read(&meta_path).ok()?;
    let meta: InstallMetaV1 = serde_json::from_slice(&bytes).ok()?;
    if meta.schema != INSTALL_META_SCHEMA_V1
        || meta.model_id != model.model_id
        || meta.revision != model.source.revision
        || meta.artifact_digest != digest
    {
        return None;
    }
    for member in model.members.values() {
        if !verify_member_file(&path.join(&member.path), member.length, &member.sha256) {
            return None;
        }
    }
    Some(path)
}

fn install_path_of(state: &SemanticModelLifecycleStateV1) -> Option<&Path> {
    match state {
        SemanticModelLifecycleStateV1::Installed { install_path, .. }
        | SemanticModelLifecycleStateV1::Loading { install_path, .. }
        | SemanticModelLifecycleStateV1::Indexing { install_path, .. }
        | SemanticModelLifecycleStateV1::Ready { install_path, .. } => Some(install_path),
        _ => None,
    }
}

fn verify_member_file(path: &Path, length: u64, sha256: &str) -> bool {
    let Ok(meta) = fs::metadata(path) else {
        return false;
    };
    if !meta.is_file() || meta.len() != length {
        return false;
    }
    let Ok(mut file) = File::open(path) else {
        return false;
    };
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let Ok(read) = file.read(&mut buffer) else {
            return false;
        };
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    encode_lowercase_hex(&hasher.finalize()) == sha256
}
