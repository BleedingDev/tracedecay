//! Explicit lifecycle control for the opt-in NCM worker.
//!
//! The command owns only the operator-facing transaction around the existing
//! project configuration API and the runtime's offline worker verifier. NCM
//! stays disabled until `install` or `update` is explicitly confirmed. Native
//! participation and recall routing are deliberately read-only here.

use std::fs;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracedecay_domain::configuration::{
    ConfigurationValueV1, MEMORY_PROVIDER_NCM_OBSERVER_SETTING_KEY, MemoryProviderNcmObserverV1,
};
use tracedecay_memory_ncm_runtime::platform::current_worker_platform_capability;
use tracedecay_memory_ncm_runtime::worker_artifact::{
    WORKER_NAME, WorkerIntegrityError, stage_verified_worker_binary,
};
use tracedecay_private_fs::framed_log::{
    DirectorySyncPolicy, atomic_write, remove_conditionally, sync_file_at,
};

const CONTROL_SCHEMA_VERSION: u16 = 1;
const CONTROL_DIRECTORY_NAME: &str = "ncm";
const WORKER_DIRECTORY_NAME: &str = "worker";
const BACKUP_DIRECTORY_NAME: &str = "backups";
const JOURNAL_FILE_NAME: &str = "control-journal-v1.json";
const RECEIPT_FILE_PREFIX: &str = "control-receipt.";
const RECEIPT_FILE_SUFFIX: &str = ".v1.json";
const MAX_WORKER_BYTES: u64 = 256 * 1024 * 1024;
const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;

/// NCM's explicit operator lifecycle operations.
#[derive(Clone, Debug, Subcommand)]
pub enum NcmAction {
    /// Report NCM configuration, worker, model, and pending-recovery state.
    Status {
        #[command(flatten)]
        scope: NcmScopeArgs,
        /// Emit one JSON object instead of a human-readable report.
        #[arg(long)]
        json: bool,
    },
    /// Verify and install a worker, then explicitly enable the NCM observer.
    Install {
        #[command(flatten)]
        scope: NcmScopeArgs,
        /// Absolute worker executable with its verified sibling manifest.
        #[arg(long, alias = "worker-binary", alias = "worker-path")]
        worker: Option<PathBuf>,
        /// Absolute NCM state root. Existing model and provider state is kept.
        #[arg(long = "state-root")]
        state_root: Option<PathBuf>,
        /// Emit the committed control receipt as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Verify and stage a replacement worker while retaining NCM state.
    Update {
        #[command(flatten)]
        scope: NcmScopeArgs,
        /// Absolute replacement worker executable with its sibling manifest.
        /// When omitted, the configured worker is reverified.
        #[arg(long, alias = "worker-binary", alias = "worker-path")]
        worker: Option<PathBuf>,
        /// Absolute NCM state root. When omitted, the configured root is kept.
        #[arg(long = "state-root")]
        state_root: Option<PathBuf>,
        /// Emit the committed control receipt as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Recover one interrupted NCM control transaction under digest guards.
    Recover {
        #[command(flatten)]
        scope: NcmScopeArgs,
        /// Emit the recovery receipt as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Disable NCM and remove only the control-owned worker; model state stays.
    Uninstall {
        #[command(flatten)]
        scope: NcmScopeArgs,
        /// Emit the committed control receipt as JSON.
        #[arg(long)]
        json: bool,
    },
}

/// Project selector shared by all NCM commands.
#[derive(Args, Clone, Debug)]
pub struct NcmScopeArgs {
    /// Project path (default: current directory, with discovery).
    #[arg(short, long)]
    pub(crate) path: Option<String>,
    /// Registered project id to address instead of discovering from cwd.
    #[arg(long, conflicts_with = "path")]
    pub(crate) project_id: Option<String>,
    /// Registered project root path or alias to address instead of discovering from cwd.
    #[arg(long, conflicts_with_all = ["path", "project_id"])]
    pub(crate) project_path: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum NcmControlOperation {
    Install,
    Update,
    Recover,
    Uninstall,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum NcmJournalPhase {
    Prepared,
    Staged,
    Configured,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct NcmControlJournalV1 {
    schema_version: u16,
    operation_id: String,
    operation: NcmControlOperation,
    phase: NcmJournalPhase,
    profile_id: String,
    project_id: String,
    worker_path: PathBuf,
    manifest_path: PathBuf,
    state_root: Option<PathBuf>,
    before_document: String,
    after_document: String,
    before_worker_digest: Option<String>,
    before_manifest_digest: Option<String>,
    after_worker_digest: Option<String>,
    after_manifest_digest: Option<String>,
    before_worker_backup: Option<PathBuf>,
    before_manifest_backup: Option<PathBuf>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct NcmControlReceiptV1 {
    schema_version: u16,
    operation_id: String,
    operation: NcmControlOperation,
    outcome: &'static str,
    profile_id: String,
    project_id: String,
    worker_path: Option<PathBuf>,
    state_root: Option<PathBuf>,
    worker_sha256: Option<String>,
    manifest_sha256: Option<String>,
    /// Explicitly records that lifecycle commands did not remove model state.
    model_state: &'static str,
    /// Explicitly records that the Native setting was not part of this mutation.
    native_state: &'static str,
    /// Explicitly records that recall routing was not part of this mutation.
    recall_routing: &'static str,
    configuration_receipt: Option<String>,
    created_at_unix: u64,
}

#[derive(Clone, Debug, Serialize)]
struct NcmStatusReport {
    profile_root: PathBuf,
    project_root: PathBuf,
    profile_id: String,
    project_id: String,
    platform_supported: bool,
    platform_target: String,
    enabled: bool,
    worker_path: Option<PathBuf>,
    worker_manifest_path: Option<PathBuf>,
    state_root: Option<PathBuf>,
    worker_present: bool,
    worker_sha256: Option<String>,
    manifest_present: bool,
    manifest_sha256: Option<String>,
    model_manifest_present: bool,
    pending_recovery: bool,
    latest_receipt: Option<PathBuf>,
}

struct ControlPaths {
    root: PathBuf,
    worker_root: PathBuf,
    backup_root: PathBuf,
    journal: PathBuf,
}

#[derive(Clone, Debug)]
struct FileSnapshot {
    bytes: Vec<u8>,
    digest: String,
}

type Result<T> = tracedecay_domain::errors::Result<T>;

/// Dispatches an NCM action after the root CLI has validated the global
/// confirmation flag. Mutating actions require that explicit confirmation.
pub(crate) async fn handle_ncm_action(action: NcmAction, assume_yes: bool) -> Result<()> {
    match action {
        NcmAction::Status { scope, json } => handle_status(scope, json).await,
        NcmAction::Install {
            scope,
            worker,
            state_root,
            json,
        } => {
            require_confirmation(assume_yes, "ncm install")?;
            handle_install_or_update(
                NcmControlOperation::Install,
                scope,
                worker,
                state_root,
                json,
            )
            .await
        }
        NcmAction::Update {
            scope,
            worker,
            state_root,
            json,
        } => {
            require_confirmation(assume_yes, "ncm update")?;
            handle_install_or_update(NcmControlOperation::Update, scope, worker, state_root, json)
                .await
        }
        NcmAction::Recover { scope, json } => {
            require_confirmation(assume_yes, "ncm recover")?;
            handle_recover(scope, json).await
        }
        NcmAction::Uninstall { scope, json } => {
            require_confirmation(assume_yes, "ncm uninstall")?;
            handle_uninstall(scope, json).await
        }
    }
}

fn require_confirmation(assume_yes: bool, operation: &str) -> Result<()> {
    if assume_yes {
        Ok(())
    } else {
        Err(config_error(format!(
            "{operation} changes project configuration; pass --yes to confirm"
        )))
    }
}

async fn resolve_scope(scope: NcmScopeArgs) -> Result<crate::commands::ResolvedCliScope> {
    let requested =
        crate::resolve_cli_project_root(scope.path, scope.project_id, scope.project_path).await?;
    crate::commands::resolve_project_scope(requested).await
}

fn control_paths(create: bool) -> Result<(PathBuf, ControlPaths)> {
    let profile_root = tracedecay_runtime_core::storage::default_profile_root()?;
    require_absolute_path(&profile_root, "profile root")?;
    let root = profile_root.join(CONTROL_DIRECTORY_NAME);
    let worker_root = root.join(WORKER_DIRECTORY_NAME);
    let backup_root = root.join(BACKUP_DIRECTORY_NAME);
    if create {
        for directory in [&root, &worker_root, &backup_root] {
            fs::create_dir_all(directory)
                .map_err(|error| io_error("create NCM control directory", directory, error))?;
            tracedecay_private_fs::make_private_directory(directory)
                .map_err(|error| io_error("secure NCM control directory", directory, error))?;
        }
    }
    let journal = root.join(JOURNAL_FILE_NAME);
    Ok((
        profile_root,
        ControlPaths {
            root,
            worker_root,
            backup_root,
            journal,
        },
    ))
}

async fn handle_status(scope: NcmScopeArgs, json: bool) -> Result<()> {
    let resolved = resolve_scope(scope).await?;
    let (profile_root, paths) = control_paths(false)?;
    let (observer_document, observer) = read_observer(&resolved.project_path).await?;
    let _ = observer_document;
    let (worker_path, state_root) = match observer {
        MemoryProviderNcmObserverV1::Disabled {} => (None, None),
        MemoryProviderNcmObserverV1::Enabled {
            worker_binary,
            state_root,
        } => (Some(worker_binary), Some(state_root)),
    };
    let manifest_path = worker_path
        .as_deref()
        .and_then(Path::parent)
        .map(|parent| parent.join("worker-manifest.json"));
    let worker = worker_path
        .as_deref()
        .map(|path| read_regular_snapshot(path, MAX_WORKER_BYTES, "worker"))
        .transpose()?
        .flatten();
    let manifest = manifest_path
        .as_deref()
        .map(|path| read_regular_snapshot(path, MAX_MANIFEST_BYTES, "worker manifest"))
        .transpose()?
        .flatten();
    let model_manifest_present = state_root.as_deref().is_some_and(|root| {
        root.is_absolute()
            && root
                .join("models")
                .join("ncm-encoder-manifest.json")
                .is_file()
    });
    let capability = current_worker_platform_capability();
    let report = NcmStatusReport {
        profile_root,
        project_root: resolved.project_path,
        profile_id: resolved.profile_id.as_str().to_owned(),
        project_id: resolved.project_id.as_str().to_owned(),
        platform_supported: capability.is_supported(),
        platform_target: capability.target().to_owned(),
        enabled: worker_path.is_some(),
        worker_path,
        worker_manifest_path: manifest_path,
        state_root,
        worker_present: worker.is_some(),
        worker_sha256: worker.as_ref().map(|file| file.digest.clone()),
        manifest_present: manifest.is_some(),
        manifest_sha256: manifest.as_ref().map(|file| file.digest.clone()),
        model_manifest_present,
        pending_recovery: paths.journal.exists(),
        latest_receipt: latest_receipt(&paths.root),
    };
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report)
                .map_err(|error| config_error(error.to_string()))?
        );
    } else {
        let mode = if report.enabled {
            "enabled"
        } else {
            "disabled"
        };
        println!("NCM: {mode}");
        println!("Platform: {}", report.platform_target);
        if let Some(path) = &report.worker_path {
            println!("Worker: {}", path.display());
            println!("Worker present: {}", report.worker_present);
        }
        if let Some(root) = &report.state_root {
            println!("State root: {}", root.display());
            println!("Model manifest present: {}", report.model_manifest_present);
        }
        if report.pending_recovery {
            println!("Recovery: pending ({})", paths.journal.display());
        }
        if let Some(receipt) = &report.latest_receipt {
            println!("Latest receipt: {}", receipt.display());
        }
    }
    Ok(())
}

async fn handle_install_or_update(
    operation: NcmControlOperation,
    scope: NcmScopeArgs,
    worker: Option<PathBuf>,
    state_root: Option<PathBuf>,
    json: bool,
) -> Result<()> {
    ensure_supported_platform()?;
    let resolved = resolve_scope(scope).await?;
    let (_profile_root, paths) = control_paths(true)?;
    refuse_pending_journal(&paths)?;
    let (before_document, before_observer) = read_observer(&resolved.project_path).await?;
    let before_enabled = matches!(before_observer, MemoryProviderNcmObserverV1::Enabled { .. });
    if matches!(operation, NcmControlOperation::Install) && before_enabled {
        return Err(config_error(
            "NCM is already enabled; use `ncm update` to replace its worker",
        ));
    }
    if matches!(operation, NcmControlOperation::Update) && !before_enabled {
        return Err(config_error(
            "NCM is disabled; use `ncm install` to enable it explicitly",
        ));
    }
    let configured_worker = match &before_observer {
        MemoryProviderNcmObserverV1::Enabled { worker_binary, .. } => Some(worker_binary.clone()),
        MemoryProviderNcmObserverV1::Disabled {} => None,
    };
    let source_worker = worker.or(configured_worker).ok_or_else(|| {
        config_error(
            "NCM install requires --worker; the worker must have worker-manifest.json beside it",
        )
    })?;
    require_absolute_path(&source_worker, "worker path")?;
    let state_root = state_root
        .or_else(|| match &before_observer {
            MemoryProviderNcmObserverV1::Enabled { state_root, .. } => Some(state_root.clone()),
            MemoryProviderNcmObserverV1::Disabled {} => None,
        })
        .ok_or_else(|| config_error("NCM install requires --state-root"))?;
    require_absolute_path(&state_root, "state root")?;
    let artifact = stage_verified_worker_binary(&source_worker)
        .map_err(|error| verifier_error(&source_worker, error))?;
    let manifest_bytes = artifact.manifest_bytes();
    if manifest_bytes.is_empty() {
        return Err(config_error(
            "verified worker did not retain its manifest bytes",
        ));
    }
    let operation_id = new_operation_id(
        resolved.profile_id.as_str(),
        resolved.project_id.as_str(),
        operation,
    );
    let worker_path = paths.worker_root.join(&operation_id).join(WORKER_NAME);
    let manifest_path = worker_path
        .parent()
        .map(|parent| parent.join("worker-manifest.json"))
        .ok_or_else(|| config_error("staged worker path has no parent"))?;
    let after_observer = MemoryProviderNcmObserverV1::Enabled {
        worker_binary: worker_path.clone(),
        state_root: state_root.clone(),
    };
    after_observer
        .validate()
        .map_err(|error| config_error(error.to_string()))?;
    let after_document = serde_json::to_string(&after_observer)
        .map_err(|error| config_error(format!("encode NCM configuration: {error}")))?;
    let journal = NcmControlJournalV1 {
        schema_version: CONTROL_SCHEMA_VERSION,
        operation_id: operation_id.clone(),
        operation,
        phase: NcmJournalPhase::Prepared,
        profile_id: resolved.profile_id.as_str().to_owned(),
        project_id: resolved.project_id.as_str().to_owned(),
        worker_path: worker_path.clone(),
        manifest_path: manifest_path.clone(),
        state_root: Some(state_root.clone()),
        before_document,
        after_document,
        before_worker_digest: None,
        before_manifest_digest: None,
        after_worker_digest: Some(artifact.sha256().to_owned()),
        after_manifest_digest: Some(sha256_hex(manifest_bytes)),
        before_worker_backup: None,
        before_manifest_backup: None,
    };
    write_journal(&paths, &journal)?;
    if let Err(error) = publish_worker(&worker_path, manifest_bytes, artifact.path()) {
        return Err(error);
    }
    let mut journal = journal;
    journal.phase = NcmJournalPhase::Staged;
    write_journal(&paths, &journal)?;
    let configuration_receipt = match set_observer(&resolved, &after_observer).await {
        Ok(receipt) => receipt,
        Err(error) => {
            if let Err(recovery_error) = recover_journal(&resolved, &paths, false, false).await {
                return Err(config_error(format!(
                    "NCM configuration failed: {error}; automatic recovery failed: {recovery_error}"
                )));
            }
            return Err(error);
        }
    };
    journal.phase = NcmJournalPhase::Configured;
    write_journal(&paths, &journal)?;
    let receipt = NcmControlReceiptV1 {
        schema_version: CONTROL_SCHEMA_VERSION,
        operation_id,
        operation,
        outcome: "committed",
        profile_id: resolved.profile_id.as_str().to_owned(),
        project_id: resolved.project_id.as_str().to_owned(),
        worker_path: Some(worker_path),
        state_root: Some(state_root),
        worker_sha256: journal.after_worker_digest.clone(),
        manifest_sha256: journal.after_manifest_digest.clone(),
        model_state: "preserved",
        native_state: "preserved",
        recall_routing: "preserved",
        configuration_receipt: configuration_receipt
            .as_ref()
            .map(|effect| effect.request_id.as_str().to_owned()),
        created_at_unix: now_unix(),
    };
    let receipt_path = write_receipt(&paths, &receipt)?;
    remove_journal(&paths)?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&receipt)
                .map_err(|error| config_error(error.to_string()))?
        );
    } else {
        println!(
            "NCM {} committed; receipt {}",
            operation_name(operation),
            receipt_path.display()
        );
        tracedecay_cli_configuration_receipt(configuration_receipt.as_ref());
    }
    Ok(())
}

async fn handle_uninstall(scope: NcmScopeArgs, json: bool) -> Result<()> {
    let resolved = resolve_scope(scope).await?;
    let (_profile_root, paths) = control_paths(true)?;
    refuse_pending_journal(&paths)?;
    let (before_document, before_observer) = read_observer(&resolved.project_path).await?;
    let MemoryProviderNcmObserverV1::Enabled {
        worker_binary,
        state_root,
    } = before_observer
    else {
        let operation_id = new_operation_id(
            resolved.profile_id.as_str(),
            resolved.project_id.as_str(),
            NcmControlOperation::Uninstall,
        );
        let receipt = NcmControlReceiptV1 {
            schema_version: CONTROL_SCHEMA_VERSION,
            operation_id,
            operation: NcmControlOperation::Uninstall,
            outcome: "no_effect",
            profile_id: resolved.profile_id.as_str().to_owned(),
            project_id: resolved.project_id.as_str().to_owned(),
            worker_path: None,
            state_root: None,
            worker_sha256: None,
            manifest_sha256: None,
            model_state: "preserved",
            native_state: "preserved",
            recall_routing: "preserved",
            configuration_receipt: None,
            created_at_unix: now_unix(),
        };
        let receipt_path = write_receipt(&paths, &receipt)?;
        if json {
            println!(
                "{}",
                serde_json::to_string_pretty(&receipt)
                    .map_err(|error| config_error(error.to_string()))?
            );
        } else {
            println!("NCM already disabled; receipt {}", receipt_path.display());
        }
        return Ok(());
    };
    require_absolute_path(&worker_binary, "configured worker path")?;
    require_absolute_path(&state_root, "configured state root")?;
    let manifest_path = worker_binary
        .parent()
        .map(|parent| parent.join("worker-manifest.json"))
        .ok_or_else(|| config_error("configured worker path has no parent"))?;
    let before_worker = read_regular_snapshot(&worker_binary, MAX_WORKER_BYTES, "worker")?;
    let before_manifest =
        read_regular_snapshot(&manifest_path, MAX_MANIFEST_BYTES, "worker manifest")?;
    let operation_id = new_operation_id(
        resolved.profile_id.as_str(),
        resolved.project_id.as_str(),
        NcmControlOperation::Uninstall,
    );
    let backup_dir = paths.backup_root.join(&operation_id);
    fs::create_dir_all(&backup_dir)
        .map_err(|error| io_error("create NCM uninstall backup", &backup_dir, error))?;
    tracedecay_private_fs::make_private_directory(&backup_dir)
        .map_err(|error| io_error("secure NCM uninstall backup", &backup_dir, error))?;
    let worker_backup = before_worker
        .as_ref()
        .map(|file| backup_dir.join(WORKER_NAME));
    let manifest_backup = before_manifest
        .as_ref()
        .map(|_| backup_dir.join("worker-manifest.json"));
    if let (Some(path), Some(file)) = (&worker_backup, &before_worker) {
        write_atomic(path, "ncm-worker-backup", &file.bytes)?;
    }
    if let (Some(path), Some(file)) = (&manifest_backup, &before_manifest) {
        write_atomic(path, "ncm-manifest-backup", &file.bytes)?;
    }
    let after_observer = MemoryProviderNcmObserverV1::Disabled {};
    let after_document = serde_json::to_string(&after_observer)
        .map_err(|error| config_error(format!("encode NCM configuration: {error}")))?;
    let mut journal = NcmControlJournalV1 {
        schema_version: CONTROL_SCHEMA_VERSION,
        operation_id: operation_id.clone(),
        operation: NcmControlOperation::Uninstall,
        phase: NcmJournalPhase::Prepared,
        profile_id: resolved.profile_id.as_str().to_owned(),
        project_id: resolved.project_id.as_str().to_owned(),
        worker_path: worker_binary.clone(),
        manifest_path: manifest_path.clone(),
        state_root: Some(state_root.clone()),
        before_document,
        after_document,
        before_worker_digest: before_worker.as_ref().map(|file| file.digest.clone()),
        before_manifest_digest: before_manifest.as_ref().map(|file| file.digest.clone()),
        after_worker_digest: None,
        after_manifest_digest: None,
        before_worker_backup: worker_backup,
        before_manifest_backup: manifest_backup,
    };
    write_journal(&paths, &journal)?;
    let configuration_receipt = match set_observer(&resolved, &after_observer).await {
        Ok(receipt) => receipt,
        Err(error) => {
            if let Err(recovery_error) = recover_journal(&resolved, &paths, false, false).await {
                return Err(config_error(format!(
                    "NCM configuration failed: {error}; automatic recovery failed: {recovery_error}"
                )));
            }
            return Err(error);
        }
    };
    journal.phase = NcmJournalPhase::Configured;
    write_journal(&paths, &journal)?;
    if is_control_owned_path(&paths.root, &worker_binary) {
        remove_guarded(
            &worker_binary,
            journal.before_worker_digest.as_deref(),
            "worker",
        )?;
        remove_guarded(
            &manifest_path,
            journal.before_manifest_digest.as_deref(),
            "worker manifest",
        )?;
        if let Some(parent) = worker_binary.parent() {
            let _ = fs::remove_dir(parent);
        }
    }
    let receipt = NcmControlReceiptV1 {
        schema_version: CONTROL_SCHEMA_VERSION,
        operation_id,
        operation: NcmControlOperation::Uninstall,
        outcome: "committed",
        profile_id: resolved.profile_id.as_str().to_owned(),
        project_id: resolved.project_id.as_str().to_owned(),
        worker_path: Some(worker_binary),
        state_root: Some(state_root),
        worker_sha256: journal.before_worker_digest.clone(),
        manifest_sha256: journal.before_manifest_digest.clone(),
        model_state: "preserved",
        native_state: "preserved",
        recall_routing: "preserved",
        configuration_receipt: configuration_receipt
            .as_ref()
            .map(|effect| effect.request_id.as_str().to_owned()),
        created_at_unix: now_unix(),
    };
    let receipt_path = write_receipt(&paths, &receipt)?;
    remove_journal(&paths)?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&receipt)
                .map_err(|error| config_error(error.to_string()))?
        );
    } else {
        println!(
            "NCM uninstalled; model state preserved; receipt {}",
            receipt_path.display()
        );
        tracedecay_cli_configuration_receipt(configuration_receipt.as_ref());
    }
    Ok(())
}

async fn handle_recover(scope: NcmScopeArgs, json: bool) -> Result<()> {
    let resolved = resolve_scope(scope).await?;
    let (_profile_root, paths) = control_paths(true)?;
    if !paths.journal.exists() {
        return Err(config_error(format!(
            "no pending NCM control journal at {}",
            paths.journal.display()
        )));
    }
    let receipt = recover_journal(&resolved, &paths, true, json).await?;
    if !json {
        println!("NCM recovery committed; receipt {}", receipt.display());
    }
    Ok(())
}

async fn recover_journal(
    resolved: &crate::commands::ResolvedCliScope,
    paths: &ControlPaths,
    emit: bool,
    json: bool,
) -> Result<PathBuf> {
    let journal = read_journal(paths)?
        .ok_or_else(|| config_error("NCM control journal disappeared during recovery"))?;
    validate_journal(&journal, paths)?;
    if journal.profile_id != resolved.profile_id.as_str()
        || journal.project_id != resolved.project_id.as_str()
    {
        return Err(config_error(
            "NCM control journal belongs to another profile or project",
        ));
    }
    let (_current_document, current_observer) = read_observer(&resolved.project_path).await?;
    let current_document = serde_json::to_string(&current_observer)
        .map_err(|error| config_error(format!("encode current NCM configuration: {error}")))?;
    let current_digest = sha256_hex(current_document.as_bytes());
    let before_digest = sha256_hex(journal.before_document.as_bytes());
    let after_digest = sha256_hex(journal.after_document.as_bytes());
    if current_digest != before_digest && current_digest != after_digest {
        return Err(config_error(
            "NCM recovery refused: configuration changed outside the pending journal",
        ));
    }
    match journal.operation {
        NcmControlOperation::Install | NcmControlOperation::Update => {
            remove_guarded(
                &journal.worker_path,
                journal.after_worker_digest.as_deref(),
                "staged worker",
            )?;
            remove_guarded(
                &journal.manifest_path,
                journal.after_manifest_digest.as_deref(),
                "staged worker manifest",
            )?;
            if let Some(parent) = journal.worker_path.parent() {
                let _ = fs::remove_dir(parent);
            }
            if current_digest == after_digest {
                let before = decode_observer_document(&journal.before_document)?;
                let _ = set_observer(resolved, &before).await?;
            }
        }
        NcmControlOperation::Uninstall => {
            if let Some(expected) = journal.before_worker_digest.as_deref()
                && let Some(current) =
                    read_regular_snapshot(&journal.worker_path, MAX_WORKER_BYTES, "worker")?
                && current.digest != expected
            {
                return Err(config_error(
                    "NCM recovery refused: worker bytes changed outside the pending journal",
                ));
            }
            if let Some(backup) = &journal.before_worker_backup {
                if let Some(file) =
                    read_regular_snapshot(backup, MAX_WORKER_BYTES, "worker backup")?
                {
                    write_atomic(&journal.worker_path, "ncm-worker-recovery", &file.bytes)?;
                    set_worker_mode(&journal.worker_path)?;
                }
            }
            if let Some(backup) = &journal.before_manifest_backup
                && let Some(file) =
                    read_regular_snapshot(backup, MAX_MANIFEST_BYTES, "manifest backup")?
            {
                write_atomic(&journal.manifest_path, "ncm-manifest-recovery", &file.bytes)?;
            }
            if current_digest == after_digest {
                let before = decode_observer_document(&journal.before_document)?;
                let _ = set_observer(resolved, &before).await?;
            }
        }
        NcmControlOperation::Recover => {
            return Err(config_error("nested NCM recovery journal is invalid"));
        }
    }
    let receipt = NcmControlReceiptV1 {
        schema_version: CONTROL_SCHEMA_VERSION,
        operation_id: journal.operation_id.clone(),
        operation: NcmControlOperation::Recover,
        outcome: "recovered",
        profile_id: journal.profile_id,
        project_id: journal.project_id,
        worker_path: Some(journal.worker_path),
        state_root: journal.state_root,
        worker_sha256: journal.before_worker_digest,
        manifest_sha256: journal.before_manifest_digest,
        model_state: "preserved",
        native_state: "preserved",
        recall_routing: "preserved",
        configuration_receipt: None,
        created_at_unix: now_unix(),
    };
    let path = write_receipt(paths, &receipt)?;
    remove_journal(paths)?;
    if emit && json {
        println!(
            "{}",
            serde_json::to_string_pretty(&receipt)
                .map_err(|error| config_error(error.to_string()))?
        );
    }
    Ok(path)
}

async fn read_observer(project_path: &Path) -> Result<(String, MemoryProviderNcmObserverV1)> {
    let value = crate::commands::current_project_setting(
        project_path,
        MEMORY_PROVIDER_NCM_OBSERVER_SETTING_KEY,
    )
    .await?;
    let ConfigurationValueV1::Text(document) = value else {
        return Err(config_error(format!(
            "configuration setting {MEMORY_PROVIDER_NCM_OBSERVER_SETTING_KEY} is not text"
        )));
    };
    let observer = decode_observer_document(&document)?;
    Ok((document, observer))
}

fn decode_observer_document(document: &str) -> Result<MemoryProviderNcmObserverV1> {
    let observer = serde_json::from_str(document)
        .map_err(|error| config_error(format!("invalid NCM observer document: {error}")))?;
    observer
        .validate()
        .map_err(|error| config_error(format!("invalid NCM observer document: {error}")))?;
    Ok(observer)
}

async fn set_observer(
    resolved: &crate::commands::ResolvedCliScope,
    observer: &MemoryProviderNcmObserverV1,
) -> Result<Option<tracedecay_contracts::EffectReceipt>> {
    let (_current_document, current) = read_observer(&resolved.project_path).await?;
    if current == *observer {
        return Ok(None);
    }
    let expected_revision =
        crate::commands::current_configuration_revision(&resolved.project_path).await?;
    let value = serde_json::to_string(observer)
        .map_err(|error| config_error(format!("encode NCM observer document: {error}")))?;
    let mutation = crate::commands::project_configuration_set(
        &resolved.project_id,
        MEMORY_PROVIDER_NCM_OBSERVER_SETTING_KEY,
        ConfigurationValueV1::Text(value),
    )?;
    crate::commands::mutate_project_configuration(
        &resolved.project_path,
        &resolved.project_id,
        expected_revision,
        vec![mutation],
    )
    .await
}

fn publish_worker(worker_path: &Path, manifest_bytes: &[u8], staged_worker: &Path) -> Result<()> {
    let parent = worker_path
        .parent()
        .ok_or_else(|| config_error("staged worker path has no parent"))?;
    fs::create_dir_all(parent)
        .map_err(|error| io_error("create staged worker directory", parent, error))?;
    tracedecay_private_fs::make_private_directory(parent)
        .map_err(|error| io_error("secure staged worker directory", parent, error))?;
    let worker_bytes =
        read_regular_snapshot(staged_worker, MAX_WORKER_BYTES, "verified worker")?
            .ok_or_else(|| config_error("verified worker disappeared before publication"))?;
    write_atomic(worker_path, "ncm-worker", &worker_bytes.bytes)?;
    set_worker_mode(worker_path)?;
    let manifest_path = worker_path
        .parent()
        .map(|parent| parent.join("worker-manifest.json"))
        .ok_or_else(|| config_error("staged worker path has no parent"))?;
    write_atomic(&manifest_path, "ncm-worker-manifest", manifest_bytes)
}

fn remove_guarded(path: &Path, expected_digest: Option<&str>, role: &str) -> Result<()> {
    let Some(expected_digest) = expected_digest else {
        return Ok(());
    };
    let Some(current) = read_regular_snapshot(
        path,
        if role.contains("manifest") {
            MAX_MANIFEST_BYTES
        } else {
            MAX_WORKER_BYTES
        },
        role,
    )?
    else {
        return Ok(());
    };
    if current.digest != expected_digest {
        return Err(config_error(format!(
            "NCM recovery refused: {role} digest changed (expected {expected_digest}, got {})",
            current.digest
        )));
    }
    let digest = expected_digest.to_owned();
    remove_conditionally(
        path,
        || {},
        |rollback| {
            let bytes = fs::read(rollback)?;
            Ok(sha256_hex(&bytes) == digest)
        },
        DirectorySyncPolicy::Strict,
    )
    .map_err(|error| io_error("remove NCM control artifact", path, error))
}

fn read_regular_snapshot(path: &Path, max_bytes: u64, role: &str) -> Result<Option<FileSnapshot>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error("inspect NCM artifact", path, error)),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(config_error(format!(
            "NCM {role} path is not a regular file: {}",
            path.display()
        )));
    }
    if metadata.len() > max_bytes {
        return Err(config_error(format!(
            "NCM {role} exceeds the bounded size of {max_bytes} bytes: {}",
            path.display()
        )));
    }
    let mut file = tracedecay_private_fs::open_regular_read_no_follow(path)
        .map_err(|error| io_error("read NCM artifact", path, error))?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.read_to_end(&mut bytes)
        .map_err(|error| io_error("read NCM artifact", path, error))?;
    if bytes.len() as u64 != metadata.len() {
        return Err(config_error(format!(
            "NCM {role} changed while reading: {}",
            path.display()
        )));
    }
    let digest = sha256_hex(&bytes);
    Ok(Some(FileSnapshot { bytes, digest }))
}

fn write_journal(paths: &ControlPaths, journal: &NcmControlJournalV1) -> Result<()> {
    validate_journal(journal, paths)?;
    let bytes = serde_json::to_vec_pretty(journal)
        .map_err(|error| config_error(format!("encode NCM control journal: {error}")))?;
    write_atomic(&paths.journal, "ncm-control-journal", &bytes)
}

fn read_journal(paths: &ControlPaths) -> Result<Option<NcmControlJournalV1>> {
    let Some(file) = read_regular_snapshot(&paths.journal, 1024 * 1024, "control journal")? else {
        return Ok(None);
    };
    let journal = serde_json::from_slice(&file.bytes)
        .map_err(|error| config_error(format!("invalid NCM control journal: {error}")))?;
    validate_journal(&journal, paths)?;
    Ok(Some(journal))
}

fn validate_journal(journal: &NcmControlJournalV1, paths: &ControlPaths) -> Result<()> {
    if journal.schema_version != CONTROL_SCHEMA_VERSION || journal.operation_id.is_empty() {
        return Err(config_error("unsupported NCM control journal schema"));
    }
    for (role, path) in [
        ("worker", &journal.worker_path),
        ("manifest", &journal.manifest_path),
    ] {
        require_absolute_path(path, role)?;
    }
    if let Some(state_root) = &journal.state_root {
        require_absolute_path(state_root, "state root")?;
    }
    if !is_control_owned_path(&paths.root, &journal.worker_path)
        || !is_control_owned_path(&paths.root, &journal.manifest_path)
    {
        return Err(config_error(
            "NCM control journal names a path outside its profile root",
        ));
    }
    for digest in [
        journal.before_worker_digest.as_deref(),
        journal.before_manifest_digest.as_deref(),
        journal.after_worker_digest.as_deref(),
        journal.after_manifest_digest.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(config_error(
                "NCM control journal contains an invalid digest",
            ));
        }
    }
    Ok(())
}

fn write_receipt(paths: &ControlPaths, receipt: &NcmControlReceiptV1) -> Result<PathBuf> {
    let name = format!(
        "{RECEIPT_FILE_PREFIX}{}{RECEIPT_FILE_SUFFIX}",
        receipt.operation_id
    );
    let path = paths.root.join(name);
    let bytes = serde_json::to_vec_pretty(receipt)
        .map_err(|error| config_error(format!("encode NCM control receipt: {error}")))?;
    write_atomic(&path, "ncm-control-receipt", &bytes)?;
    Ok(path)
}

fn latest_receipt(root: &Path) -> Option<PathBuf> {
    let mut paths = fs::read_dir(root)
        .ok()?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with(RECEIPT_FILE_PREFIX) && name.ends_with(RECEIPT_FILE_SUFFIX)
                })
        })
        .collect::<Vec<_>>();
    paths.sort();
    paths.pop()
}

fn remove_journal(paths: &ControlPaths) -> Result<()> {
    match fs::symlink_metadata(&paths.journal) {
        Ok(metadata) if metadata.file_type().is_file() => {
            fs::remove_file(&paths.journal)
                .map_err(|error| io_error("remove NCM control journal", &paths.journal, error))?;
            tracedecay_private_fs::framed_log::sync_parent_directory(
                &paths.journal,
                DirectorySyncPolicy::Strict,
            )
            .map_err(|error| io_error("sync NCM control directory", &paths.root, error))?;
            Ok(())
        }
        Ok(_) => Err(config_error("NCM control journal is not a regular file")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(
            "inspect NCM control journal",
            &paths.journal,
            error,
        )),
    }
}

fn refuse_pending_journal(paths: &ControlPaths) -> Result<()> {
    if paths.journal.exists() {
        return Err(config_error(format!(
            "NCM has a pending control journal at {}; run `tracedecay ncm recover --yes` first",
            paths.journal.display()
        )));
    }
    Ok(())
}

fn write_atomic(path: &Path, kind: &str, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        config_error(format!(
            "NCM artifact path has no parent: {}",
            path.display()
        ))
    })?;
    fs::create_dir_all(parent)
        .map_err(|error| io_error("create NCM artifact parent", parent, error))?;
    atomic_write(path, kind, bytes, DirectorySyncPolicy::Strict)
        .map_err(|error| io_error("write NCM control artifact", path, error))?;
    Ok(())
}

fn set_worker_mode(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o500))
            .map_err(|error| io_error("seal staged NCM worker", path, error))?;
        sync_file_at(path).map_err(|error| io_error("sync staged NCM worker", path, error))?;
    }
    Ok(())
}

fn is_control_owned_path(root: &Path, path: &Path) -> bool {
    path.strip_prefix(root).is_ok_and(|relative| {
        !relative.as_os_str().is_empty()
            && !relative.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::Root | Component::Prefix(_)
                )
            })
    })
}

fn require_absolute_path(path: &Path, role: &str) -> Result<()> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
        || path.to_str().is_some_and(|text| text.contains('\0'))
    {
        return Err(config_error(format!(
            "NCM {role} must be absolute and must not contain parent traversal"
        )));
    }
    Ok(())
}

fn ensure_supported_platform() -> Result<()> {
    let capability = current_worker_platform_capability();
    if capability.is_supported() {
        Ok(())
    } else {
        Err(config_error(format!(
            "NCM lifecycle is supported only on arm64 macOS; {}",
            capability
        )))
    }
}

fn new_operation_id(profile_id: &str, project_id: &str, operation: NcmControlOperation) -> String {
    let now = now_nanos();
    let digest = Sha256::digest(
        format!("ncm-control.v1|{profile_id}|{project_id}|{operation:?}|{now}").as_bytes(),
    );
    digest
        .iter()
        .take(16)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn operation_name(operation: NcmControlOperation) -> &'static str {
    match operation {
        NcmControlOperation::Install => "install",
        NcmControlOperation::Update => "update",
        NcmControlOperation::Recover => "recover",
        NcmControlOperation::Uninstall => "uninstall",
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos())
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn verifier_error(
    path: &Path,
    error: WorkerIntegrityError,
) -> tracedecay_domain::errors::TraceDecayError {
    config_error(format!(
        "NCM worker '{}' failed offline verification: {error}",
        path.display()
    ))
}

fn io_error(
    operation: &str,
    path: &Path,
    error: std::io::Error,
) -> tracedecay_domain::errors::TraceDecayError {
    config_error(format!("{operation} '{}': {error}", path.display()))
}

fn config_error(message: impl Into<String>) -> tracedecay_domain::errors::TraceDecayError {
    tracedecay_domain::errors::TraceDecayError::Config {
        message: message.into(),
    }
}

fn tracedecay_cli_configuration_receipt(receipt: Option<&tracedecay_contracts::EffectReceipt>) {
    crate::commands::report_configuration_receipt(receipt);
}

#[cfg(test)]
mod tests {
    use super::{NcmControlOperation, NcmScopeArgs, new_operation_id, require_absolute_path};
    use std::path::Path;

    #[test]
    fn operation_ids_are_bounded_hex() {
        let id = new_operation_id("profile.test", "project.test", NcmControlOperation::Install);
        assert_eq!(id.len(), 32);
        assert!(id.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }

    #[test]
    fn worker_and_state_paths_require_absolute_non_traversing_values() {
        assert!(require_absolute_path(Path::new("relative/worker"), "worker").is_err());
        assert!(require_absolute_path(Path::new("/tmp/../worker"), "worker").is_err());
        assert!(require_absolute_path(Path::new("/tmp/worker"), "worker").is_ok());
    }

    #[allow(dead_code)]
    fn scope_is_constructible_for_clap() {
        let _ = NcmScopeArgs {
            path: None,
            project_id: None,
            project_path: None,
        };
    }
}
