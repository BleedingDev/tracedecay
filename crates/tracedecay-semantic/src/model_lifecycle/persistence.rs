fn set_failed_state(
    root: &Path,
    inner: &LifecyclePublicationGateV1,
    model: &CatalogedFastEmbedModelV1,
    digest: &str,
    detail: &str,
    retryable: bool,
) -> Result<(), ModelLifecycleErrorV1> {
    let mut guard = inner.writer();
    guard.durable.state = Some(SemanticModelLifecycleStateV1::Failed {
        model_id: model.model_id.clone(),
        revision: model.source.revision.clone(),
        artifact_digest: digest.to_owned(),
        detail: detail.to_owned(),
        retryable,
    });
    persist_durable(root, &guard.durable)
}
fn verify_catalog_manifest(
    model: &CatalogedFastEmbedModelV1,
    manifest: &ModelArtifactManifestV1,
) -> Result<(), ModelLifecycleErrorV1> {
    manifest
        .validate()
        .map_err(|_| ModelLifecycleErrorV1::VerificationFailed)?;
    if manifest.payload.artifact_id != model.model_id
        || manifest.payload.profile_kind != ArtifactProfileKindV1::Embedding
        || manifest.payload.dimensions != model.expected_dimensions
        || manifest.payload.metric != SemanticMetricV1::Cosine
        || manifest.payload.normalization != EmbeddingNormalizationV1::L2
        || manifest.payload.pooling != EmbeddingPoolingV1::Mean
        || manifest.payload.truncation.side != TruncationSideV1::Right
        || manifest.payload.truncation.max_length != model.max_length
        || manifest.payload.spdx_license != model.source.license
        || manifest.payload.upstream.name != model.model_code
        || manifest.payload.upstream.version != model.source.revision
        || manifest.payload.upstream.revision != model.source.revision
        || manifest.payload.runtime.runtime != model.backend.runtime_family().runtime_family()
        || manifest.payload.runtime.build_revision
            != model.backend.runtime_family().build_revision()
        || manifest.payload.precision != model.backend.precision()
        || manifest.payload.device != DeviceClassV1::Cpu
        || manifest.payload.members.len() != model.members.len()
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
    match fs::symlink_metadata(&path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(ModelLifecycleErrorV1::VerificationFailed);
            }
            let bytes = fs::read(&path).map_err(|_| ModelLifecycleErrorV1::StoreUnavailable)?;
            let durable = serde_json::from_slice::<DurableLifecycleV1>(&bytes)
                .map_err(|_| ModelLifecycleErrorV1::VerificationFailed)?;
            if durable.schema != LIFECYCLE_SCHEMA_V1 {
                return Err(ModelLifecycleErrorV1::VerificationFailed);
            }
            return Ok(durable);
        }
        Err(error) if error.kind() != io::ErrorKind::NotFound => {
            return Err(ModelLifecycleErrorV1::StoreUnavailable);
        }
        Err(_) => {}
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
    };
    persist_durable(root, &durable)?;
    Ok(durable)
}

/// A process restart cannot have a live acquisition worker. Convert an
/// interrupted download/verification phase back to an explicit retryable
/// selection before publishing status, so a crash never leaves semantics
/// permanently stuck in a non-remediable state.
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
    durable.state = Some(next);
    persist_durable(root, durable)
}

fn persist_durable(root: &Path, durable: &DurableLifecycleV1) -> Result<(), ModelLifecycleErrorV1> {
    write_json_atomic(&root.join("lifecycle.json"), durable)
        .map_err(|_| ModelLifecycleErrorV1::StoreUnavailable)
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        ensure_lifecycle_directory(parent)?;
    }
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no parent"))?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no file name"))?;
    let nonce = LIFECYCLE_PRIVATE_PATH_NONCE.fetch_add(1, Ordering::Relaxed);
    let tmp = parent.join(format!(".{name}.tmp-{}-{nonce}", std::process::id()));
    let result = (|| {
        let mut file = OpenOptions::new().create_new(true).write(true).open(&tmp)?;
        serde_json::to_writer_pretty(&mut file, value)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        file.flush()?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, path)?;
        #[cfg(unix)]
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
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
    let mut current = root.to_path_buf();
    for component in [
        "installs",
        model.model_id.as_str(),
        model.source.revision.as_str(),
        &digest[..16.min(digest.len())],
    ] {
        current.push(component);
        let metadata = fs::symlink_metadata(&current).ok()?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return None;
        }
    }
    let meta_path = path.join("install.json");
    let metadata = fs::symlink_metadata(&meta_path).ok()?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
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
    let Ok(meta) = fs::symlink_metadata(path) else {
        return false;
    };
    if !meta.is_file() || meta.file_type().is_symlink() || meta.len() != length {
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
