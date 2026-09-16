//! Crash-safe lifecycle for the NCM encoder model directory.
//!
//! The encoder's model cache is a single admitted filesystem object.  A
//! download is performed below a private, same-filesystem staging directory;
//! the complete candidate is verified before the live `models` directory is
//! changed.  Publication is a pair of directory-entry renames guarded by a
//! durable journal.  That gives readers either the old complete tree or the
//! new complete tree, while recovery can repair the short interval between
//! the two renames after a process or machine failure.

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt};
use cap_std::{
    ambient_authority,
    fs::{Dir, OpenOptions},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::ffi::OsStr;
use std::fs;
use std::io::Read;
#[cfg(feature = "real-encoder")]
use std::path::PathBuf;
use std::path::{Component, Path};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "real-encoder")]
use super::{
    CACHE_REPOSITORY_DIR, MANIFEST_FILENAME, MODEL_REVISION, REQUIRED_FILES,
    validate_manifest_shape,
};
use super::{PinnedEncoder, ensure_manifest_is_pinned};
use crate::ports::{Deadline, EncoderError, EncoderIdentity, StateRoot};

const JOURNAL_SCHEMA_VERSION: u16 = 1;
/// Durable journal kept beside the live model directory.
pub const JOURNAL_FILENAME: &str = "ncm-model-lifecycle-v1.json";
/// Prefix used for private same-filesystem staging roots.
pub const STAGING_PREFIX: &str = ".ncm-model-staging-";
/// Prefix used for rollback directories retained by an interrupted operation.
pub const BACKUP_PREFIX: &str = ".ncm-model-backup-";
const ROLLBACK_PREFIX: &str = ".ncm-model-rollback-";
#[cfg(feature = "real-encoder")]
const CANDIDATE_DIRECTORY: &str = "candidate";
#[cfg(feature = "real-encoder")]
const DOWNLOAD_DIRECTORY: &str = "download";

/// Installs the pinned model if it is absent and returns its verified identity.
pub fn install(root: &StateRoot, deadline: Deadline) -> Result<EncoderIdentity, EncoderError> {
    install_or_update(root, deadline, Operation::Install)
}

/// Replaces the live model tree with a freshly acquired pinned model.
pub fn update(root: &StateRoot, deadline: Deadline) -> Result<EncoderIdentity, EncoderError> {
    install_or_update(root, deadline, Operation::Update)
}

/// Repairs one interrupted model lifecycle transaction.
///
/// A missing journal is a successful no-op.  A present journal is accepted
/// only when every path and digest still belongs to the transaction that
/// created it; an unexpected external edit remains in place for inspection.
pub fn recover(root: &StateRoot) -> Result<(), EncoderError> {
    let root_directory = open_root_directory(root)?;
    let journal_path = root.path().join(JOURNAL_FILENAME);
    let Some(journal) = read_journal(&journal_path)? else {
        return Ok(());
    };
    validate_journal(&journal)?;
    recover_journal(root, &root_directory, &journal)
}

/// Removes the complete model tree transactionally.
///
/// The provider namespace and every other state-root entry remain untouched.
/// If the operation is interrupted after the live directory is moved, the
/// journal lets [`recover`] restore the exact pre-uninstall tree.
pub fn uninstall(root: &StateRoot) -> Result<(), EncoderError> {
    let root_directory = open_root_directory(root)?;
    let journal_path = root.path().join(JOURNAL_FILENAME);
    refuse_pending_journal(&journal_path)?;
    let Some(before_digest) =
        digest_optional_entry(&root_directory, OsStr::new("models"), &root.models_dir())?
    else {
        return Ok(());
    };

    let operation_id = operation_id(Operation::Uninstall);
    let backup_name = format!("{BACKUP_PREFIX}{operation_id}");
    if path_entry_exists(
        &root_directory,
        OsStr::new(&backup_name),
        &root.path().join(&backup_name),
    )? {
        return Err(EncoderError::ArtifactMismatch(
            "uninstall backup entry already exists".to_owned(),
        ));
    }
    let journal = LifecycleJournal {
        schema_version: JOURNAL_SCHEMA_VERSION,
        operation_id,
        operation: Operation::Uninstall,
        phase: Phase::Prepared,
        staging_name: None,
        backup_name: Some(backup_name.clone()),
        before_digest: Some(before_digest),
        after_digest: None,
    };
    write_journal(&journal_path, &journal)?;

    if let Err(error) = tracedecay_private_fs::capability_dir::rename_noreplace(
        &root_directory,
        OsStr::new("models"),
        &root_directory,
        OsStr::new(&backup_name),
    ) {
        let _ = remove_journal(&journal_path);
        return Err(io_error(
            "move model tree to uninstall backup",
            &root.models_dir(),
            error,
        ));
    }
    sync_directory(&root_directory, root.path())?;

    let mut journal = journal;
    journal.phase = Phase::BackedUp;
    if let Err(error) = write_journal(&journal_path, &journal) {
        return Err(error);
    }

    journal.phase = Phase::Published;
    write_journal(&journal_path, &journal)?;
    let backup_path = root.path().join(&backup_name);
    remove_directory_entry(&root_directory, OsStr::new(&backup_name), &backup_path)?;
    sync_directory(&root_directory, root.path())?;
    remove_journal(&journal_path)
}

fn install_or_update(
    root: &StateRoot,
    deadline: Deadline,
    operation: Operation,
) -> Result<EncoderIdentity, EncoderError> {
    if deadline.remaining_ms == 0 {
        return Err(EncoderError::Cancelled);
    }
    let root_directory = open_root_directory(root)?;
    let journal_path = root.path().join(JOURNAL_FILENAME);
    refuse_pending_journal(&journal_path)?;
    super::ensure_authoritative_environment()?;
    let expected = PinnedEncoder::reference()?;
    ensure_manifest_is_pinned(&expected)?;

    #[cfg(feature = "real-encoder")]
    {
        if matches!(operation, Operation::Install)
            && has_exact_verified_model(&root_directory, root, &expected)?
        {
            return super::encoder_identity(&expected);
        }

        let (staging_name, staging_path) = create_private_staging(&root_directory, root.path())?;
        let result = (|| {
            let download_path = staging_path.join(DOWNLOAD_DIRECTORY);
            create_private_directory(&download_path)?;
            let options =
                fastembed::TextInitOptions::new(fastembed::EmbeddingModel::ParaphraseMLMiniLML12V2)
                    .with_cache_dir(download_path.clone())
                    .with_max_length(super::MAX_LENGTH)
                    .with_show_download_progress(false);
            let model = fastembed::TextEmbedding::try_new(options)
                .map_err(|error| EncoderError::Inference(format!("acquire encoder: {error}")))?;
            if deadline.remaining_ms == 0 {
                return Err(EncoderError::Cancelled);
            }
            drop(model);

            let manifest = super::materialize_verified_snapshot(&download_path, &expected)?;
            super::ensure_pinned_metadata(&manifest, &expected)?;
            super::validate_materialized_encoder(&download_path, &manifest)?;

            let candidate_root = StateRoot::new(staging_path.clone()).map_err(|error| {
                EncoderError::ArtifactMismatch(format!("stage root is invalid: {error}"))
            })?;
            let (candidate_models, candidate_directory) =
                super::prepare_model_cache(&candidate_root)?;
            super::publish_materialized_snapshot(
                &download_path,
                &candidate_models,
                &candidate_directory,
                &manifest,
            )?;
            super::write_manifest(&candidate_models, &manifest)?;
            validate_candidate(&candidate_models, &manifest)?;
            sync_tree_path(&candidate_models)?;

            let before_digest =
                digest_optional_entry(&root_directory, OsStr::new("models"), &root.models_dir())?;
            let candidate_digest = digest_directory_path(&candidate_models)?;
            let operation_id = operation_id(operation);
            let backup_name = format!("{BACKUP_PREFIX}{operation_id}");
            let journal = LifecycleJournal {
                schema_version: JOURNAL_SCHEMA_VERSION,
                operation_id,
                operation,
                phase: Phase::Prepared,
                staging_name: Some(staging_name.clone()),
                backup_name: Some(backup_name.clone()),
                before_digest,
                after_digest: Some(candidate_digest),
            };
            let journal_path = root.path().join(JOURNAL_FILENAME);
            write_journal(&journal_path, &journal)?;
            publish_candidate(
                root,
                &root_directory,
                &journal_path,
                journal,
                &staging_path,
                &candidate_models,
                &backup_name,
                &manifest,
            )?;
            super::encoder_identity(&manifest)
        })();

        if result.is_err() {
            let _ = cleanup_staging(&root_directory, &staging_name, root.path());
        }
        result
    }

    #[cfg(not(feature = "real-encoder"))]
    {
        let _ = (root, root_directory, journal_path, expected, operation);
        Err(EncoderError::ArtifactsMissing(
            "real-encoder feature is disabled".to_owned(),
        ))
    }
}

#[cfg(feature = "real-encoder")]
fn publish_candidate(
    root: &StateRoot,
    root_directory: &Dir,
    journal_path: &Path,
    mut journal: LifecycleJournal,
    staging_path: &Path,
    candidate_models: &Path,
    backup_name: &str,
    manifest: &PinnedEncoder,
) -> Result<(), EncoderError> {
    let staging_name = staging_path.file_name().ok_or_else(|| {
        EncoderError::ArtifactMismatch("staged model root has no directory name".to_owned())
    })?;
    let models_path = root.models_dir();
    let backup_path = root.path().join(backup_name);

    if path_entry_exists(root_directory, OsStr::new(backup_name), &backup_path)? {
        return Err(EncoderError::ArtifactMismatch(
            "model backup entry already exists".to_owned(),
        ));
    }
    let current_before = digest_optional_entry(root_directory, OsStr::new("models"), &models_path)?;
    if current_before != journal.before_digest {
        let _ = remove_journal(journal_path);
        return Err(EncoderError::ArtifactMismatch(
            "live model tree changed before publication".to_owned(),
        ));
    }

    if let Err(error) = tracedecay_private_fs::capability_dir::rename_noreplace(
        root_directory,
        OsStr::new("models"),
        root_directory,
        OsStr::new(backup_name),
    ) {
        if error.kind() != std::io::ErrorKind::NotFound {
            return rollback_after_publish_failure(
                root,
                root_directory,
                journal_path,
                &journal,
                staging_name,
                &models_path,
                io_error("move previous model tree to backup", &models_path, error),
            );
        }
    }
    sync_directory(root_directory, root.path())?;
    journal.phase = Phase::BackedUp;
    write_journal(journal_path, &journal)?;

    // The candidate is below the staging root.  Rename it through the open
    // staging capability so a swapped path cannot redirect publication.
    let staging_directory = open_directory_path(staging_path)?;
    if let Err(error) = tracedecay_private_fs::capability_dir::rename_noreplace(
        &staging_directory,
        OsStr::new(CANDIDATE_DIRECTORY),
        root_directory,
        OsStr::new("models"),
    ) {
        let detail = io_error("publish verified model tree", &models_path, error);
        return rollback_after_publish_failure(
            root,
            root_directory,
            journal_path,
            &journal,
            staging_name,
            &models_path,
            detail,
        );
    }
    sync_directory(root_directory, root.path())?;
    journal.phase = Phase::Published;
    write_journal(journal_path, &journal)?;

    let published = root.models_dir();
    if validate_candidate(&published, manifest).is_err()
        || journal.after_digest.as_deref() != Some(digest_directory_path(&published)?.as_str())
    {
        return rollback_after_publish_failure(
            root,
            root_directory,
            journal_path,
            &journal,
            staging_name,
            &models_path,
            EncoderError::ArtifactMismatch("published model tree failed verification".to_owned()),
        );
    }

    if path_entry_exists(root_directory, OsStr::new(backup_name), &backup_path)? {
        verify_backup_digest(root, &journal, backup_name)?;
        remove_directory_entry(root_directory, OsStr::new(backup_name), &backup_path)?;
    }
    if path_entry_exists(root_directory, staging_name, staging_path)? {
        remove_directory_entry(root_directory, staging_name, staging_path)?;
    }
    sync_directory(root_directory, root.path())?;
    remove_journal(journal_path)
}

#[cfg(feature = "real-encoder")]
fn rollback_after_publish_failure(
    root: &StateRoot,
    root_directory: &Dir,
    journal_path: &Path,
    journal: &LifecycleJournal,
    staging_name: &OsStr,
    models_path: &Path,
    fallback: EncoderError,
) -> Result<(), EncoderError> {
    let rollback_result = rollback_to_before(
        root,
        root_directory,
        journal,
        Some(staging_name),
        models_path,
    );
    match rollback_result {
        Ok(()) => {
            let _ = remove_journal(journal_path);
            Err(fallback)
        }
        Err(error) => Err(EncoderError::Inference(format!(
            "model publication failed ({fallback}); rollback failed: {error}"
        ))),
    }
}

fn rollback_to_before(
    root: &StateRoot,
    root_directory: &Dir,
    journal: &LifecycleJournal,
    staging_name: Option<&OsStr>,
    models_path: &Path,
) -> Result<(), EncoderError> {
    let backup_name = journal.backup_name.as_deref().ok_or_else(|| {
        EncoderError::ArtifactMismatch("journal has no rollback backup".to_owned())
    })?;
    let backup_path = root.path().join(backup_name);
    let models_exists = path_entry_exists(root_directory, OsStr::new("models"), models_path)?;
    let backup_exists = path_entry_exists(root_directory, OsStr::new(backup_name), &backup_path)?;
    if models_exists
        && journal.phase == Phase::Published
        && !matches!(journal.operation, Operation::Uninstall)
    {
        let expected = journal.after_digest.as_deref().ok_or_else(|| {
            EncoderError::ArtifactMismatch("published journal has no candidate digest".to_owned())
        })?;
        let actual = digest_directory_path(models_path)?;
        if actual != expected {
            return Err(EncoderError::ArtifactMismatch(
                "published model tree changed outside the model transaction".to_owned(),
            ));
        }
    }
    if backup_exists {
        verify_backup_digest(root, journal, backup_name)?;
        if models_exists {
            let rollback_name = format!("{ROLLBACK_PREFIX}{}", journal.operation_id);
            let rollback_path = root.path().join(&rollback_name);
            if path_entry_exists(root_directory, OsStr::new(&rollback_name), &rollback_path)? {
                return Err(EncoderError::ArtifactMismatch(
                    "model rollback entry already exists".to_owned(),
                ));
            }
            tracedecay_private_fs::capability_dir::rename_noreplace(
                root_directory,
                OsStr::new("models"),
                root_directory,
                OsStr::new(&rollback_name),
            )
            .map_err(|error| io_error("move failed model tree aside", models_path, error))?;
            tracedecay_private_fs::capability_dir::rename_noreplace(
                root_directory,
                OsStr::new(backup_name),
                root_directory,
                OsStr::new("models"),
            )
            .map_err(|error| io_error("restore model rollback backup", models_path, error))?;
            remove_directory_entry(root_directory, OsStr::new(&rollback_name), &rollback_path)?;
        } else {
            tracedecay_private_fs::capability_dir::rename_noreplace(
                root_directory,
                OsStr::new(backup_name),
                root_directory,
                OsStr::new("models"),
            )
            .map_err(|error| io_error("restore model rollback backup", models_path, error))?;
        }
    } else if models_exists && journal.before_digest.is_none() {
        remove_directory_entry(root_directory, OsStr::new("models"), models_path)?;
    } else if journal.before_digest.is_some() {
        let current_digest =
            digest_optional_entry(root_directory, OsStr::new("models"), models_path)?;
        if current_digest != journal.before_digest {
            return Err(EncoderError::ArtifactsMissing(
                "model rollback backup is missing".to_owned(),
            ));
        }
    }
    if let Some(staging_name) = staging_name {
        let staging_path = root.path().join(staging_name);
        if path_entry_exists(root_directory, staging_name, &staging_path)? {
            remove_directory_entry(root_directory, staging_name, &staging_path)?;
        }
    }
    sync_directory(root_directory, root.path())
}

fn recover_journal(
    root: &StateRoot,
    root_directory: &Dir,
    journal: &LifecycleJournal,
) -> Result<(), EncoderError> {
    let models_path = root.models_dir();
    match journal.phase {
        Phase::Prepared | Phase::BackedUp => {
            rollback_to_before(
                root,
                root_directory,
                journal,
                journal.staging_name.as_deref().map(OsStr::new),
                &models_path,
            )?;
        }
        Phase::Published => match journal.operation {
            Operation::Uninstall => {
                if path_entry_exists(root_directory, OsStr::new("models"), &models_path)? {
                    return Err(EncoderError::ArtifactMismatch(
                        "uninstall journal found a live model tree".to_owned(),
                    ));
                }
                if let Some(name) = journal.backup_name.as_deref()
                    && path_entry_exists(root_directory, OsStr::new(name), &root.path().join(name))?
                {
                    verify_backup_digest(root, journal, name)?;
                    remove_directory_entry(
                        root_directory,
                        OsStr::new(name),
                        &root.path().join(name),
                    )?;
                }
            }
            Operation::Install | Operation::Update => {
                let Some(expected_digest) = journal.after_digest.as_deref() else {
                    return Err(EncoderError::ArtifactMismatch(
                        "publish journal has no candidate digest".to_owned(),
                    ));
                };
                let Some(actual) =
                    digest_optional_entry(root_directory, OsStr::new("models"), &models_path)?
                else {
                    rollback_to_before(
                        root,
                        root_directory,
                        journal,
                        journal.staging_name.as_deref().map(OsStr::new),
                        &models_path,
                    )?;
                    remove_journal(&root.path().join(JOURNAL_FILENAME))?;
                    return Ok(());
                };
                if actual != expected_digest {
                    return Err(EncoderError::ArtifactMismatch(
                        "published model tree changed outside the model transaction".to_owned(),
                    ));
                }
                #[cfg(feature = "real-encoder")]
                {
                    let expected = PinnedEncoder::reference()?;
                    validate_candidate(&models_path, &expected)?;
                }
                if let Some(name) = journal.backup_name.as_deref()
                    && path_entry_exists(root_directory, OsStr::new(name), &root.path().join(name))?
                {
                    verify_backup_digest(root, journal, name)?;
                    remove_directory_entry(
                        root_directory,
                        OsStr::new(name),
                        &root.path().join(name),
                    )?;
                }
            }
        },
    }
    if let Some(name) = journal.staging_name.as_deref()
        && path_entry_exists(root_directory, OsStr::new(name), &root.path().join(name))?
    {
        remove_directory_entry(root_directory, OsStr::new(name), &root.path().join(name))?;
    }
    sync_directory(root_directory, root.path())?;
    remove_journal(&root.path().join(JOURNAL_FILENAME))
}

#[cfg(feature = "real-encoder")]
fn has_exact_verified_model(
    root_directory: &Dir,
    root: &StateRoot,
    expected: &PinnedEncoder,
) -> Result<bool, EncoderError> {
    let Some(models) =
        open_optional_directory(root_directory, OsStr::new("models"), &root.models_dir())?
    else {
        return Ok(false);
    };
    let _ = models;
    validate_candidate(&root.models_dir(), expected)
        .map(|()| true)
        .or_else(|error| {
            if matches!(error, EncoderError::ArtifactsMissing(_)) {
                Ok(false)
            } else {
                Err(error)
            }
        })
}

#[cfg(feature = "real-encoder")]
fn validate_candidate(path: &Path, expected: &PinnedEncoder) -> Result<(), EncoderError> {
    validate_manifest_shape(expected)?;
    super::verify_cached_state(path, expected)?;
    exact_layout(path)?;
    super::validate_materialized_encoder(path, expected)
}

#[cfg(feature = "real-encoder")]
fn exact_layout(models_path: &Path) -> Result<(), EncoderError> {
    let models = open_directory_path(models_path)?;
    expect_entries(
        &models,
        models_path,
        &[CACHE_REPOSITORY_DIR],
        &[MANIFEST_FILENAME],
    )?;
    let repository_path = models_path.join(CACHE_REPOSITORY_DIR);
    let repository =
        open_directory_nofollow(&models, OsStr::new(CACHE_REPOSITORY_DIR), &repository_path)?;
    expect_entries(&repository, &repository_path, &["refs", "snapshots"], &[])?;
    let refs_path = repository_path.join("refs");
    let refs = open_directory_nofollow(&repository, OsStr::new("refs"), &refs_path)?;
    expect_entries(&refs, &refs_path, &[], &["main"])?;
    let snapshots_path = repository_path.join("snapshots");
    let snapshots = open_directory_nofollow(&repository, OsStr::new("snapshots"), &snapshots_path)?;
    expect_entries(&snapshots, &snapshots_path, &[MODEL_REVISION], &[])?;
    let snapshot_path = snapshots_path.join(MODEL_REVISION);
    let snapshot = open_directory_nofollow(&snapshots, OsStr::new(MODEL_REVISION), &snapshot_path)?;
    expect_entries(&snapshot, &snapshot_path, &["onnx"], &REQUIRED_FILES[1..])?;
    let onnx_path = snapshot_path.join("onnx");
    let onnx = open_directory_nofollow(&snapshot, OsStr::new("onnx"), &onnx_path)?;
    expect_entries(&onnx, &onnx_path, &[], &["model.onnx"])
}

#[cfg(feature = "real-encoder")]
fn expect_entries(
    directory: &Dir,
    path: &Path,
    expected_directories: &[&str],
    expected_files: &[&str],
) -> Result<(), EncoderError> {
    let mut seen = Vec::new();
    for entry in directory
        .read_dir(".")
        .map_err(|error| io_error("list model directory", path, error))?
    {
        let entry = entry.map_err(|error| io_error("read model directory", path, error))?;
        let name = entry.file_name();
        let file_type = entry
            .file_type()
            .map_err(|error| io_error("inspect model directory entry", path, error))?;
        if file_type.is_symlink() {
            return Err(EncoderError::ArtifactMismatch(format!(
                "model lifecycle path is a symlink: {}",
                path.join(&name).display()
            )));
        }
        let name_text = name.to_string_lossy();
        if file_type.is_dir() {
            if !expected_directories
                .iter()
                .any(|expected| *expected == name_text)
            {
                return Err(EncoderError::ArtifactMismatch(format!(
                    "unexpected model directory entry {}",
                    path.join(&name).display()
                )));
            }
        } else if file_type.is_file() {
            if !expected_files.iter().any(|expected| *expected == name_text) {
                return Err(EncoderError::ArtifactMismatch(format!(
                    "unexpected model file {}",
                    path.join(&name).display()
                )));
            }
        } else {
            return Err(EncoderError::ArtifactMismatch(format!(
                "model directory entry is not a regular object: {}",
                path.join(&name).display()
            )));
        }
        seen.push((name_text.into_owned(), file_type.is_dir()));
    }
    for expected in expected_directories {
        if !seen
            .iter()
            .any(|(name, directory)| name == expected && *directory)
        {
            return Err(EncoderError::ArtifactsMissing(
                path.join(expected).display().to_string(),
            ));
        }
    }
    for expected in expected_files {
        if !seen
            .iter()
            .any(|(name, directory)| name == expected && !*directory)
        {
            return Err(EncoderError::ArtifactsMissing(
                path.join(expected).display().to_string(),
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Operation {
    Install,
    Update,
    Uninstall,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Prepared,
    BackedUp,
    Published,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LifecycleJournal {
    schema_version: u16,
    operation_id: String,
    operation: Operation,
    phase: Phase,
    staging_name: Option<String>,
    backup_name: Option<String>,
    before_digest: Option<String>,
    after_digest: Option<String>,
}

fn operation_id(operation: Operation) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let digest = Sha256::digest(
        format!(
            "ncm-model-lifecycle.v1|{operation:?}|{now}|{}",
            std::process::id()
        )
        .as_bytes(),
    );
    digest
        .iter()
        .take(16)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(feature = "real-encoder")]
fn create_private_staging(
    root_directory: &Dir,
    root: &Path,
) -> Result<(String, PathBuf), EncoderError> {
    for attempt in 0..32_u32 {
        let name = format!(
            "{STAGING_PREFIX}{}-{attempt}",
            operation_id(Operation::Update)
        );
        let path = root.join(&name);
        match root_directory.create_dir(OsStr::new(&name)) {
            Ok(()) => {
                tracedecay_private_fs::make_private_directory(&path)
                    .map_err(|error| io_error("secure model staging directory", &path, error))?;
                return Ok((name, path));
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(io_error("create model staging directory", &path, error)),
        }
    }
    Err(EncoderError::Inference(
        "unable to allocate private model staging directory".to_owned(),
    ))
}

#[cfg(feature = "real-encoder")]
fn create_private_directory(path: &Path) -> Result<(), EncoderError> {
    fs::create_dir(path).map_err(|error| io_error("create model staging child", path, error))?;
    tracedecay_private_fs::make_private_directory(path)
        .map(|_| ())
        .map_err(|error| io_error("secure model staging child", path, error))
}

fn read_journal(path: &Path) -> Result<Option<LifecycleJournal>, EncoderError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error("inspect model lifecycle journal", path, error)),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(EncoderError::ArtifactMismatch(format!(
            "model lifecycle journal is not a regular file: {}",
            path.display()
        )));
    }
    let bytes = tracedecay_private_fs::framed_log::read_bounded(path, 1024 * 1024)
        .map_err(|error| io_error("read model lifecycle journal", path, error))?
        .ok_or_else(|| EncoderError::ArtifactsMissing(path.display().to_string()))?;
    serde_json::from_slice(&bytes).map(Some).map_err(|error| {
        EncoderError::ArtifactMismatch(format!("parse model lifecycle journal: {error}"))
    })
}

fn write_journal(path: &Path, journal: &LifecycleJournal) -> Result<(), EncoderError> {
    validate_journal(journal)?;
    let bytes = serde_json::to_vec_pretty(journal).map_err(|error| {
        EncoderError::Inference(format!("encode model lifecycle journal: {error}"))
    })?;
    tracedecay_private_fs::framed_log::atomic_write(
        path,
        "ncm-model-lifecycle-journal",
        &bytes,
        directory_sync_policy(),
    )
    .map_err(|error| io_error("write model lifecycle journal", path, error))
}

fn validate_journal(journal: &LifecycleJournal) -> Result<(), EncoderError> {
    if journal.schema_version != JOURNAL_SCHEMA_VERSION
        || !is_safe_entry_name(&journal.operation_id)
        || !journal
            .operation_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(EncoderError::ArtifactMismatch(
            "unsupported model lifecycle journal schema".to_owned(),
        ));
    }
    for name in journal
        .staging_name
        .iter()
        .chain(journal.backup_name.iter())
    {
        if !is_safe_entry_name(name)
            || (!name.starts_with(STAGING_PREFIX) && !name.starts_with(BACKUP_PREFIX))
        {
            return Err(EncoderError::ArtifactMismatch(
                "model lifecycle journal contains an unsafe private path".to_owned(),
            ));
        }
    }
    for digest in journal
        .before_digest
        .iter()
        .chain(journal.after_digest.iter())
    {
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(EncoderError::ArtifactMismatch(
                "model lifecycle journal contains an invalid digest".to_owned(),
            ));
        }
    }
    if matches!(journal.operation, Operation::Uninstall) && journal.staging_name.is_some() {
        return Err(EncoderError::ArtifactMismatch(
            "uninstall journal unexpectedly names staging".to_owned(),
        ));
    }
    if !matches!(journal.operation, Operation::Uninstall) && journal.staging_name.is_none() {
        return Err(EncoderError::ArtifactMismatch(
            "model publication journal has no staging directory".to_owned(),
        ));
    }
    let expected_backup = format!("{BACKUP_PREFIX}{}", journal.operation_id);
    if journal.backup_name.as_deref() != Some(expected_backup.as_str()) {
        return Err(EncoderError::ArtifactMismatch(
            "model lifecycle journal backup does not match its operation".to_owned(),
        ));
    }
    match journal.operation {
        Operation::Uninstall
            if journal.before_digest.is_none() || journal.after_digest.is_some() =>
        {
            return Err(EncoderError::ArtifactMismatch(
                "uninstall journal has invalid transaction digests".to_owned(),
            ));
        }
        Operation::Install | Operation::Update if journal.after_digest.is_none() => {
            return Err(EncoderError::ArtifactMismatch(
                "model publication journal has no candidate digest".to_owned(),
            ));
        }
        _ => {}
    }
    Ok(())
}

fn refuse_pending_journal(path: &Path) -> Result<(), EncoderError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(EncoderError::ArtifactMismatch(format!(
            "model lifecycle has a pending journal at {}; run recover first",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error("inspect model lifecycle journal", path, error)),
    }
}

fn remove_journal(path: &Path) -> Result<(), EncoderError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => {
            fs::remove_file(path)
                .map_err(|error| io_error("remove model lifecycle journal", path, error))?;
            tracedecay_private_fs::framed_log::sync_parent_directory(path, directory_sync_policy())
                .map_err(|error| io_error("sync model lifecycle journal directory", path, error))
        }
        Ok(_) => Err(EncoderError::ArtifactMismatch(
            "model lifecycle journal is not a regular file".to_owned(),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error("inspect model lifecycle journal", path, error)),
    }
}

fn open_root_directory(root: &StateRoot) -> Result<Dir, EncoderError> {
    validate_root_path(root.path())?;
    let parent = root
        .path()
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let name = root.path().file_name().ok_or_else(|| {
        EncoderError::ArtifactMismatch("state root has no directory name".to_owned())
    })?;
    let parent_directory = Dir::open_ambient_dir(parent, ambient_authority()).map_err(|error| {
        EncoderError::ArtifactMismatch(format!(
            "open state root parent {}: {error}",
            parent.display()
        ))
    })?;
    open_directory_nofollow(&parent_directory, name, root.path())
}

fn open_directory_path(path: &Path) -> Result<Dir, EncoderError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let name = path.file_name().ok_or_else(|| {
        EncoderError::ArtifactMismatch(format!("directory has no name: {}", path.display()))
    })?;
    let parent_directory = Dir::open_ambient_dir(parent, ambient_authority()).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            EncoderError::ArtifactsMissing(path.display().to_string())
        } else {
            io_error("open model directory parent", parent, error)
        }
    })?;
    open_directory_nofollow(&parent_directory, name, path)
}

fn open_directory_nofollow(parent: &Dir, name: &OsStr, path: &Path) -> Result<Dir, EncoderError> {
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    let file = parent.open_with(name, &options).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            EncoderError::ArtifactsMissing(path.display().to_string())
        } else {
            io_error(
                "open model directory without following symlinks",
                path,
                error,
            )
        }
    })?;
    let metadata = file
        .metadata()
        .map_err(|error| io_error("inspect model directory", path, error))?;
    if !metadata.file_type().is_dir() {
        return Err(EncoderError::ArtifactMismatch(format!(
            "model lifecycle path is not a directory: {}",
            path.display()
        )));
    }
    Ok(Dir::from_std_file(file.into_std()))
}

#[cfg(feature = "real-encoder")]
fn open_optional_directory(
    parent: &Dir,
    name: &OsStr,
    path: &Path,
) -> Result<Option<Dir>, EncoderError> {
    match open_directory_nofollow(parent, name, path) {
        Ok(directory) => Ok(Some(directory)),
        Err(EncoderError::ArtifactsMissing(_)) => Ok(None),
        Err(error) => Err(error),
    }
}

fn path_entry_exists(parent: &Dir, name: &OsStr, path: &Path) -> Result<bool, EncoderError> {
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    match parent.open_with(name, &options) {
        Ok(file) => {
            let metadata = file
                .metadata()
                .map_err(|error| io_error("inspect model lifecycle entry", path, error))?;
            if !metadata.file_type().is_dir() {
                return Err(EncoderError::ArtifactMismatch(format!(
                    "model lifecycle entry is not a directory: {}",
                    path.display()
                )));
            }
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error("inspect model lifecycle entry", path, error)),
    }
}

fn digest_optional_entry(
    parent: &Dir,
    name: &OsStr,
    path: &Path,
) -> Result<Option<String>, EncoderError> {
    if !path_entry_exists(parent, name, path)? {
        return Ok(None);
    }
    digest_directory_path(path).map(Some)
}

fn verify_backup_digest(
    root: &StateRoot,
    journal: &LifecycleJournal,
    backup_name: &str,
) -> Result<(), EncoderError> {
    let Some(expected) = journal.before_digest.as_deref() else {
        return Ok(());
    };
    let backup_path = root.path().join(backup_name);
    let actual = digest_directory_path(&backup_path)?;
    if actual != expected {
        return Err(EncoderError::ArtifactMismatch(
            "rollback backup changed outside the model transaction".to_owned(),
        ));
    }
    Ok(())
}

fn digest_directory_path(path: &Path) -> Result<String, EncoderError> {
    let directory = open_directory_path(path)?;
    let mut hasher = Sha256::new();
    digest_directory(&directory, path, Path::new(""), &mut hasher)?;
    Ok(hex_digest(hasher.finalize()))
}

fn digest_directory(
    directory: &Dir,
    path: &Path,
    relative: &Path,
    hasher: &mut Sha256,
) -> Result<(), EncoderError> {
    let mut entries = Vec::new();
    for entry in directory
        .read_dir(".")
        .map_err(|error| io_error("list model lifecycle tree", path, error))?
    {
        let entry = entry.map_err(|error| io_error("read model lifecycle tree", path, error))?;
        entries.push(entry);
    }
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let name = entry.file_name();
        let file_type = entry
            .file_type()
            .map_err(|error| io_error("inspect model lifecycle tree", path, error))?;
        let child_relative = relative.join(&name);
        hasher.update(child_relative.to_string_lossy().as_bytes());
        hasher.update([0]);
        if file_type.is_symlink() {
            return Err(EncoderError::ArtifactMismatch(format!(
                "model lifecycle tree contains a symlink: {}",
                path.join(&name).display()
            )));
        }
        if file_type.is_dir() {
            let child_path = path.join(&name);
            let child = open_directory_nofollow(directory, &name, &child_path)?;
            hasher.update([b'd']);
            digest_directory(&child, &child_path, &child_relative, hasher)?;
        } else if file_type.is_file() {
            let child_path = path.join(&name);
            let file = open_file_nofollow(directory, &name, &child_path)?;
            hasher.update([b'f']);
            let mut reader = file;
            let mut buffer = [0_u8; 1024 * 1024];
            loop {
                let read = reader
                    .read(&mut buffer)
                    .map_err(|error| io_error("read model lifecycle tree", &child_path, error))?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
            }
        } else {
            return Err(EncoderError::ArtifactMismatch(format!(
                "model lifecycle tree contains a non-regular entry: {}",
                path.join(&name).display()
            )));
        }
    }
    Ok(())
}

fn open_file_nofollow(
    parent: &Dir,
    name: &OsStr,
    path: &Path,
) -> Result<cap_std::fs::File, EncoderError> {
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    let file = parent.open_with(name, &options).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            EncoderError::ArtifactsMissing(path.display().to_string())
        } else {
            io_error("open model lifecycle file", path, error)
        }
    })?;
    let metadata = file
        .metadata()
        .map_err(|error| io_error("inspect model lifecycle file", path, error))?;
    if !metadata.file_type().is_file() {
        return Err(EncoderError::ArtifactMismatch(format!(
            "model lifecycle entry is not a regular file: {}",
            path.display()
        )));
    }
    Ok(file)
}

fn remove_directory_entry(parent: &Dir, name: &OsStr, path: &Path) -> Result<(), EncoderError> {
    let directory = open_directory_nofollow(parent, name, path)?;
    let mut interrupt = || Ok(());
    tracedecay_private_fs::capability_dir::remove_open_dir_all_nofollow(directory, &mut interrupt)
        .map_err(|error| io_error("remove model lifecycle directory", path, error))?;
    tracedecay_private_fs::capability_dir::sync_directory(parent)
        .map_err(|error| io_error("sync model lifecycle directory removal", path, error))
}

#[cfg(feature = "real-encoder")]
fn cleanup_staging(root_directory: &Dir, name: &str, root: &Path) -> Result<(), EncoderError> {
    let path = root.join(name);
    if path_entry_exists(root_directory, OsStr::new(name), &path)? {
        remove_directory_entry(root_directory, OsStr::new(name), &path)?;
    }
    Ok(())
}

#[cfg(feature = "real-encoder")]
fn sync_tree_path(path: &Path) -> Result<(), EncoderError> {
    let directory = open_directory_path(path)?;
    sync_tree(&directory, path)
}

#[cfg(feature = "real-encoder")]
fn sync_tree(directory: &Dir, path: &Path) -> Result<(), EncoderError> {
    let mut entries = Vec::new();
    for entry in directory
        .read_dir(".")
        .map_err(|error| io_error("list model lifecycle tree for sync", path, error))?
    {
        let entry =
            entry.map_err(|error| io_error("read model lifecycle tree for sync", path, error))?;
        entries.push(entry);
    }
    for entry in entries {
        let name = entry.file_name();
        let child_path = path.join(&name);
        let file_type = entry.file_type().map_err(|error| {
            io_error("inspect model lifecycle tree for sync", &child_path, error)
        })?;
        if file_type.is_symlink() {
            return Err(EncoderError::ArtifactMismatch(format!(
                "model lifecycle tree contains a symlink: {}",
                child_path.display()
            )));
        }
        if file_type.is_dir() {
            let child = open_directory_nofollow(directory, &name, &child_path)?;
            sync_tree(&child, &child_path)?;
        } else if file_type.is_file() {
            open_file_nofollow(directory, &name, &child_path)?
                .sync_all()
                .map_err(|error| io_error("sync model lifecycle file", &child_path, error))?;
        } else {
            return Err(EncoderError::ArtifactMismatch(format!(
                "model lifecycle tree contains a non-regular entry: {}",
                child_path.display()
            )));
        }
    }
    tracedecay_private_fs::capability_dir::sync_directory(directory)
        .map_err(|error| io_error("sync model lifecycle directory", path, error))
}

fn sync_directory(directory: &Dir, path: &Path) -> Result<(), EncoderError> {
    tracedecay_private_fs::capability_dir::sync_directory(directory)
        .map_err(|error| io_error("sync model lifecycle directory", path, error))
}

fn directory_sync_policy() -> tracedecay_private_fs::framed_log::DirectorySyncPolicy {
    tracedecay_private_fs::framed_log::DirectorySyncPolicy::Strict
}

fn validate_root_path(path: &Path) -> Result<(), EncoderError> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(EncoderError::ArtifactMismatch(
            "state root must be absolute and free of parent traversal".to_owned(),
        ));
    }
    Ok(())
}

fn is_safe_entry_name(name: &str) -> bool {
    let path = Path::new(name);
    !name.is_empty()
        && path.components().count() == 1
        && matches!(path.components().next(), Some(Component::Normal(_)))
}

fn hex_digest(digest: impl AsRef<[u8]>) -> String {
    digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn io_error(action: &str, path: &Path, error: std::io::Error) -> EncoderError {
    EncoderError::Inference(format!("{action} {}: {error}", path.display()))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    fn root() -> (tempfile::TempDir, StateRoot) {
        let temp = tempfile::tempdir().expect("create model lifecycle root");
        let root = StateRoot::new(temp.path()).expect("absolute state root");
        (temp, root)
    }

    #[test]
    fn uninstall_rejects_a_symlink_without_touching_its_target() {
        let (_temp, root) = root();
        let models = root.models_dir();
        let outside = tempfile::tempdir().expect("create outside directory");
        fs::create_dir_all(&models).expect("create model tree");
        fs::write(models.join("owned"), b"owned").expect("write model file");
        symlink(outside.path(), models.join("redirect")).expect("create redirect symlink");

        let error = uninstall(&root).expect_err("symlinked model tree must be rejected");
        assert!(matches!(error, EncoderError::ArtifactMismatch(_)));
        assert!(outside.path().exists());
        assert!(models.exists());
    }

    #[test]
    fn uninstall_removes_only_the_model_tree_and_is_idempotent() {
        let (_temp, root) = root();
        let models = root.models_dir();
        fs::create_dir_all(models.join("nested")).expect("create model tree");
        fs::write(models.join("nested/model.bin"), b"model").expect("write model file");
        fs::write(root.path().join("sibling"), b"keep").expect("write sibling state");

        uninstall(&root).expect("uninstall model tree");
        assert!(!models.exists());
        assert_eq!(
            fs::read(root.path().join("sibling")).expect("read sibling"),
            b"keep"
        );
        assert!(!root.path().join(JOURNAL_FILENAME).exists());
        assert!(
            fs::read_dir(root.path())
                .expect("list state root")
                .all(|entry| {
                    let name = entry.expect("read state root entry").file_name();
                    !name.to_string_lossy().starts_with(BACKUP_PREFIX)
                })
        );

        uninstall(&root).expect("repeated uninstall is a no-op");
        assert_eq!(
            fs::read(root.path().join("sibling")).expect("read sibling"),
            b"keep"
        );
    }

    #[test]
    fn recover_backed_up_publication_restores_the_original_tree() {
        let (_temp, root) = root();
        let models = root.models_dir();
        fs::create_dir_all(&models).expect("create model tree");
        fs::write(models.join("model.bin"), b"original").expect("write original model");
        let before_digest = digest_directory_path(&models).expect("digest original model");

        let operation_id = "b".repeat(32);
        let staging_name = format!("{STAGING_PREFIX}{operation_id}");
        let backup_name = format!("{BACKUP_PREFIX}{operation_id}");
        fs::create_dir(root.path().join(&staging_name)).expect("create staging directory");
        let root_directory = open_root_directory(&root).expect("open root");
        tracedecay_private_fs::capability_dir::rename_noreplace(
            &root_directory,
            OsStr::new("models"),
            &root_directory,
            OsStr::new(&backup_name),
        )
        .expect("move model tree to recovery backup");
        let journal = LifecycleJournal {
            schema_version: JOURNAL_SCHEMA_VERSION,
            operation_id,
            operation: Operation::Update,
            phase: Phase::BackedUp,
            staging_name: Some(staging_name.clone()),
            backup_name: Some(backup_name.clone()),
            before_digest: Some(before_digest),
            after_digest: Some("c".repeat(64)),
        };
        let journal_path = root.path().join(JOURNAL_FILENAME);
        write_journal(&journal_path, &journal).expect("write recovery journal");

        recover(&root).expect("recover backed-up publication");
        assert_eq!(
            fs::read(models.join("model.bin")).expect("read restored model"),
            b"original"
        );
        assert!(!root.path().join(&backup_name).exists());
        assert!(!root.path().join(&staging_name).exists());
        assert!(!journal_path.exists());
    }

    #[test]
    fn recover_leaves_an_externally_changed_published_tree_for_inspection() {
        let (_temp, root) = root();
        let models = root.models_dir();
        fs::create_dir_all(&models).expect("create candidate model tree");
        fs::write(models.join("model.bin"), b"published").expect("write candidate model");
        let after_digest = digest_directory_path(&models).expect("digest candidate model");
        fs::write(models.join("model.bin"), b"changed").expect("mutate candidate model");

        let operation_id = "d".repeat(32);
        let staging_name = format!("{STAGING_PREFIX}{operation_id}");
        let backup_name = format!("{BACKUP_PREFIX}{operation_id}");
        fs::create_dir(root.path().join(&backup_name)).expect("create backup directory");
        fs::write(
            root.path().join(&backup_name).join("model.bin"),
            b"original",
        )
        .expect("write backup model");
        let before_digest =
            digest_directory_path(&root.path().join(&backup_name)).expect("digest backup model");
        let journal = LifecycleJournal {
            schema_version: JOURNAL_SCHEMA_VERSION,
            operation_id,
            operation: Operation::Update,
            phase: Phase::Published,
            staging_name: Some(staging_name),
            backup_name: Some(backup_name.clone()),
            before_digest: Some(before_digest),
            after_digest: Some(after_digest),
        };
        let journal_path = root.path().join(JOURNAL_FILENAME);
        write_journal(&journal_path, &journal).expect("write recovery journal");

        let error = recover(&root).expect_err("external publication edit must be rejected");
        assert!(matches!(error, EncoderError::ArtifactMismatch(_)));
        assert_eq!(
            fs::read(models.join("model.bin")).expect("read changed model"),
            b"changed"
        );
        assert!(root.path().join(&backup_name).exists());
        assert!(journal_path.exists());
    }

    #[test]
    fn journal_paths_are_private_single_directory_entries() {
        let operation_id = "a".repeat(32);
        let journal = LifecycleJournal {
            schema_version: JOURNAL_SCHEMA_VERSION,
            operation_id: operation_id.clone(),
            operation: Operation::Update,
            phase: Phase::Prepared,
            staging_name: Some(format!("{STAGING_PREFIX}{operation_id}")),
            backup_name: Some(format!("{BACKUP_PREFIX}{operation_id}")),
            before_digest: None,
            after_digest: Some("a".repeat(64)),
        };
        validate_journal(&journal).expect("private journal names validate");
        let mut unsafe_journal = journal;
        unsafe_journal.staging_name = Some("../outside".to_owned());
        assert!(validate_journal(&unsafe_journal).is_err());
    }

    #[test]
    fn tree_digest_refuses_symlinked_entries() {
        let (_temp, root) = root();
        let models = root.models_dir();
        let outside = tempfile::tempdir().expect("create outside directory");
        fs::create_dir_all(&models).expect("create model tree");
        symlink(outside.path(), models.join("redirect")).expect("create redirect symlink");
        let error = digest_directory_path(&models).expect_err("digest must refuse symlink");
        assert!(matches!(error, EncoderError::ArtifactMismatch(_)));
    }
}
