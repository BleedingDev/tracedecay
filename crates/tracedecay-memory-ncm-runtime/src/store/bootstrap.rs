//! First creation publishes a closed, checkpointed database as one directory entry.
//! Unpublished stages live outside the namespace catalog and never authorize repair
//! of an existing namespace. In particular, an empty destination is still occupied.

use super::{
    NamespaceStore, StoreError, StoreIdentity, io_error, map_sqlite_error, namespace_paths,
};
use crate::ports::StateRoot;
use cap_std::{ambient_authority, fs::Dir};
use std::ffi::OsStr;
use std::fs;
use tempfile::{Builder, TempDir};
use tracedecay_private_fs::capability_dir::{rename_noreplace, sync_directory};
use tracedecay_private_fs::framed_log::sync_file_at;

pub(super) fn create(
    root: &StateRoot,
    namespace: &str,
    identity: StoreIdentity,
) -> Result<NamespaceStore, StoreError> {
    let (destination, _) = namespace_paths(root, namespace)?;
    match destination.symlink_metadata() {
        Ok(_) => return Err(StoreError::AlreadyExists),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(io_error(error)),
    }
    super::validate_identity(&identity)?;
    let stage = new_stage(root)?;
    if let Err(error) = initialize(&stage, namespace, identity.clone()) {
        return Err(discard(stage, error));
    }
    publish(root, namespace, stage)?;
    // Reopening at the final path acquires the ordinary writer lock and runs
    // exactly the same validation as every later open. No SQLite handle crosses
    // the rename: journal paths must never continue pointing into staging.
    NamespaceStore::open(root, namespace, &identity)
}

fn new_stage(root: &StateRoot) -> Result<TempDir, StoreError> {
    fs::create_dir_all(root.path().join("namespaces")).map_err(io_error)?;
    Builder::new()
        .prefix(".ncm-bootstrap-")
        .tempdir_in(root.path())
        .map_err(io_error)
}

fn initialize(stage: &TempDir, namespace: &str, identity: StoreIdentity) -> Result<(), StoreError> {
    let store =
        NamespaceStore::create_in_directory(namespace, stage.path().join("ncm.sqlite"), identity)?;
    let (busy, _, _): (i64, i64, i64) = store
        .conn
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(map_sqlite_error)?;
    if busy != 0 {
        return Err(StoreError::Busy);
    }
    store
        .conn
        .close()
        .map_err(|(_, error)| map_sqlite_error(error))?;
    sync_file_at(&stage.path().join("ncm.sqlite")).map_err(io_error)?;
    let directory = Dir::open_ambient_dir(stage.path(), ambient_authority()).map_err(io_error)?;
    sync_directory(&directory).map_err(io_error)
}

fn publish(root: &StateRoot, namespace: &str, stage: TempDir) -> Result<(), StoreError> {
    let result = (|| {
        let source = Dir::open_ambient_dir(root.path(), ambient_authority()).map_err(io_error)?;
        let catalog = source.open_dir("namespaces").map_err(io_error)?;
        let name = stage.path().file_name().ok_or_else(|| {
            StoreError::InvalidInput("bootstrap stage has no directory name".to_owned())
        })?;
        rename_noreplace(&source, name, &catalog, OsStr::new(namespace)).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                StoreError::AlreadyExists
            } else {
                io_error(error)
            }
        })?;
        Ok((source, catalog))
    })();
    let (source, catalog) = match result {
        Ok(parents) => parents,
        Err(error) => return Err(discard(stage, error)),
    };
    // The stage's old path is no longer owned. Even if a durability barrier or
    // the subsequent open fails, never remove the published namespace.
    let _ = stage.keep();
    sync_directory(&catalog).map_err(io_error)?;
    sync_directory(&source).map_err(io_error)
}

fn discard(stage: TempDir, error: StoreError) -> StoreError {
    match stage.close() {
        Ok(()) => error,
        Err(cleanup) => StoreError::Io(format!("{error}; remove bootstrap stage: {cleanup}")),
    }
}

#[cfg(test)]
mod tests;
