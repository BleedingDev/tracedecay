impl SemanticModelLifecycleOwnerV1 {
    /// Reconcile this owner's slots from committed selection state after restart
    /// or compensation; foreign owners' inventory leases are never touched.
    fn reconcile_embedding_artifact_leases(
        &self,
        durable: &DurableLifecycleV1,
        now_unix: u64,
    ) -> Result<(), ModelLifecycleErrorV1> {
        reconcile_embedding_artifact_leases(
            &self.artifact_store,
            &self.lease_id(EMBEDDING_ACTIVE_LEASE_ID_V1),
            &self.lease_id(EMBEDDING_ROLLBACK_LEASE_ID_V1),
            durable,
            now_unix,
        )
    }
}

fn reconcile_embedding_artifact_leases(
    store: &ModelArtifactStore,
    active_lease: &str,
    rollback_lease: &str,
    durable: &DurableLifecycleV1,
    now_unix: u64,
) -> Result<(), ModelLifecycleErrorV1> {
    // Only inventory installs hold leases; the install directory is the
    // inventory's content address, while the lifecycle digest names the catalog
    // package for a scoped acquisition and a legacy private-root install.
    let digest_for = |state: Option<&SemanticModelLifecycleStateV1>| {
        state
            .and_then(install_path_of)
            .and_then(|path| store.installed_digest(path))
    };
    let current_active =
        store.artifact_digest_for_lease(active_lease, ArtifactLeaseKindV1::Active, now_unix)?;
    let failed_current_active = durable
        .failed_current
        .as_ref()
        .and_then(install_path_of)
        .filter(|path| path.exists())
        .and_then(|path| store.installed_digest(path));
    // A runtime failure intentionally omits its install path from the public
    // state. For a shared inventory artifact the active lease is the durable
    // path authority, so preserve that lease across restart until rollback or
    // a replacement explicitly changes it. Private installs have an explicit
    // owner slot and must not accidentally retain a prior shared lease.
    let preserve_failed_shared_active = matches!(
        durable.state,
        Some(SemanticModelLifecycleStateV1::Failed { .. })
    ) && durable.private_install.is_none();
    let desired_active = digest_for(durable.state.as_ref()).or_else(|| {
        preserve_failed_shared_active
            .then(|| failed_current_active.clone().or(current_active.clone()))
            .flatten()
    });
    let desired_rollback = digest_for(durable.previous_ready.as_ref())
        .filter(|digest| Some(digest) != desired_active.as_ref());
    match desired_active.as_ref() {
        Some(digest) => {
            store.activate_artifact_with_rollback(digest, active_lease, rollback_lease, now_unix)?
        }
        None => {
            if let Some(digest) = current_active {
                store.release_artifact_lease(&digest, active_lease, ArtifactLeaseKindV1::Active)?;
            }
        }
    }
    let current_rollback =
        store.artifact_digest_for_lease(rollback_lease, ArtifactLeaseKindV1::Rollback, now_unix)?;
    if current_rollback != desired_rollback {
        if let Some(digest) = current_rollback {
            store.release_artifact_lease(&digest, rollback_lease, ArtifactLeaseKindV1::Rollback)?;
        }
        if let Some(digest) = desired_rollback {
            store.acquire_artifact_lease(
                &digest,
                ArtifactLeaseV1 {
                    lease_id: rollback_lease.to_owned(),
                    kind: ArtifactLeaseKindV1::Rollback,
                    expires_at_unix: u64::MAX,
                },
                now_unix,
            )?;
        }
    }
    Ok(())
}
