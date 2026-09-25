//! The explicit V1-to-V2 profile replacement boundary.
//!
//! This module owns orchestration only. A first-party migration worker owns
//! reading V1 authorities and producing V2 state. The CLI owns the lifecycle
//! fence, external backup, worker phase ordering, cutover journal, namespace
//! publication, and rollback. No V1 schema or row format is interpreted here.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracedecay_domain::errors::{Result, TraceDecayError};
use tracedecay_private_fs::framed_log::{DirectorySyncPolicy, sync_directory};

use crate::cli::ReplacementProviderArg;

const REPLACEMENT_PROTOCOL: &str = "tracedecay-v1-to-v2";
const REPLACEMENT_PROTOCOL_VERSION: u32 = 1;
const JOURNAL_SCHEMA_VERSION: u32 = 1;
const JOURNAL_FILENAME: &str = ".tracedecay-v1-replacement.json";
const PROFILE_REHEARSAL_MARKER_FILENAME: &str = ".tracedecay-profile-rehearsal.json";
#[cfg(windows)]
const WORKER_STAGE_FILENAME: &str = "migration-worker.exe";
#[cfg(not(windows))]
const WORKER_STAGE_FILENAME: &str = "migration-worker";
const LIFECYCLE_LOCK_FILENAME: &str = "lifecycle.lock";
const FIRST_PARTY_MANIFEST_FILENAME: &str = ".tracedecay-v1-to-v2-manifest.json";
const STAGING_MARKER_FILENAME: &str = ".tracedecay-v1-replacement-owner.json";
const STAGING_MARKER_SCHEMA_VERSION: u32 = 1;
const MAX_WORKER_OUTPUT_BYTES: usize = 1024 * 1024;
const WORKER_POLL_INTERVAL: Duration = Duration::from_millis(50);

// The replacement worker is part of this binary. The optional worker path is
// retained as a compatibility/admission input, but apply never delegates the
// preservation proof to an arbitrary executable.
const FIRST_PARTY_WORKER_AVAILABLE: bool = true;
const FIRST_PARTY_WORKER_DESCRIPTION: &str =
    "shipped first-party worker with canonical authority migrations and independent manifests";
const NCM_REFERENCE_WORKER_MANIFEST: &str =
    include_str!("../../../../product/ncm/reference/worker-manifest.json");
/// Private argv marker used when the pinned replacement binary runs the
/// canonical async migration in a killable child process. It is deliberately
/// outside the public Clap surface: only the coordinator invokes it after the
/// executable bytes have been attested and staged.
pub(crate) const INTERNAL_REPLACEMENT_MIGRATION_ARG: &str = "--__tracedecay-replacement-migrate";
// These snapshots are immutable release inputs. The replacement admission
// path deliberately does not infer a historical schema from today's schema
// installers: a v34/v35 store is accepted only when its complete sqlite_master
// inventory matches one of the released beta.37 inventories below.
const RELEASED_V34_PROJECT_STORE_SQL: &str =
    include_str!("../../../tracedecay-runtime-core/tests/fixtures/project-store-released-v34.sql");
const RELEASED_V35_PROJECT_STORE_SQL: &str = include_str!(
    "../../../tracedecay-runtime-core/tests/fixtures/project-store-released-v35-semantic.sql"
);
const RELEASED_V35_PAYLOAD_DIGESTS_SQL: &str =
    "CREATE TABLE IF NOT EXISTS memory_v2_assertion_payload_digests (
            payload_rowid INTEGER PRIMARY KEY,
            assertion_id TEXT NOT NULL,
            fact_id TEXT NOT NULL,
            owner_kind TEXT NOT NULL,
            project_id TEXT NOT NULL,
            content_digest TEXT NOT NULL CHECK(
                length(content_digest) = 71 AND content_digest LIKE 'sha256:%'
            ),
            UNIQUE(assertion_id, fact_id, owner_kind, project_id),
            FOREIGN KEY(payload_rowid)
                REFERENCES memory_v2_assertion_payloads(rowid)
        );

        CREATE INDEX IF NOT EXISTS memory_v2_assertion_payload_digests_lookup
            ON memory_v2_assertion_payload_digests(
                owner_kind, project_id, content_digest, fact_id
            );

        CREATE TRIGGER IF NOT EXISTS memory_v2_assertion_payload_digests_no_update
        BEFORE UPDATE ON memory_v2_assertion_payload_digests BEGIN
            SELECT RAISE(ABORT, 'memory_v2 assertion payload digests are immutable');
        END;
        CREATE TRIGGER IF NOT EXISTS memory_v2_payloads_digest_delete
        AFTER DELETE ON memory_v2_assertion_payloads BEGIN
            DELETE FROM memory_v2_assertion_payload_digests
            WHERE payload_rowid = OLD.rowid;
        END;";
const RELEASED_V35_SHIPPED_ALIAS_TRIGGER: &str = "
    CREATE TRIGGER retrieval_anchor_aliases_immutable_update
    BEFORE UPDATE ON retrieval_anchor_aliases BEGIN
        SELECT RAISE(ABORT, 'retrieval anchor aliases are immutable');
    END;
";

// These are the complete profile authorities the worker must account for.
// Their names intentionally match the logical paths in the complete profile
// backup manifest, so the preflight receipt cannot silently omit a family.
const REQUIRED_AUTHORITIES: &[&str] = &[
    "global.db",
    "user-sessions.db",
    "user-memory.db",
    "projects",
    "enrollment.json",
    "config.toml",
    "migration-inventory",
    "profile-identity.json",
];

fn provider_name(provider: ReplacementProviderArg) -> &'static str {
    match provider {
        ReplacementProviderArg::Native => "native",
        ReplacementProviderArg::Ncm => "ncm",
    }
}

fn provider_readiness(
    provider: ReplacementProviderArg,
    profile_root: &Path,
) -> Result<(bool, &'static str)> {
    match provider {
        ReplacementProviderArg::Native => Ok((
            true,
            "Native serving is selected by the shipped host composition",
        )),
        ReplacementProviderArg::Ncm => {
            let configured = match std::env::var_os("TRACEDECAY_NCM_WORKER") {
                Some(worker) => Some(PathBuf::from(worker)),
                None => crate::ncm_cmd::configured_ncm_worker_for_replacement(profile_root)?,
            };
            let Some(worker) = configured else {
                return Ok((
                    false,
                    "NCM requires an installed, independently attested worker bundle",
                ));
            };
            // The dry-run path must remain read-only.  The runtime helper
            // seals a verified copy in a temporary directory, which is
            // appropriate immediately before apply but would make a plan
            // mutate the host.  Recompute the same release manifest and
            // worker-byte attestation here without staging anything.
            match verify_ncm_worker_read_only(&worker) {
                Ok(()) => Ok((
                    true,
                    "NCM worker bytes and manifest match the shipped attestation",
                )),
                Err(_) => Ok((
                    false,
                    "NCM worker is missing or does not match the shipped attestation",
                )),
            }
        }
    }
}

fn verify_ncm_worker_read_only(worker: &Path) -> Result<()> {
    let worker_metadata = fs::symlink_metadata(worker).map_err(|error| {
        config_error(format!(
            "inspect NCM worker '{}': {error}",
            worker.display()
        ))
    })?;
    if worker_metadata.file_type().is_symlink() || !worker_metadata.is_file() {
        return Err(config_error(format!(
            "NCM worker '{}' must be a regular file",
            worker.display()
        )));
    }
    #[cfg(unix)]
    if worker_metadata.permissions().mode() & 0o111 == 0 {
        return Err(config_error(format!(
            "NCM worker '{}' is not executable",
            worker.display()
        )));
    }
    let manifest_path = worker
        .parent()
        .ok_or_else(|| config_error("NCM worker path has no parent"))?
        .join("worker-manifest.json");
    let manifest_metadata = fs::symlink_metadata(&manifest_path).map_err(|error| {
        config_error(format!(
            "inspect NCM worker manifest '{}': {error}",
            manifest_path.display()
        ))
    })?;
    if manifest_metadata.file_type().is_symlink() || !manifest_metadata.is_file() {
        return Err(config_error(format!(
            "NCM worker manifest '{}' must be a regular file",
            manifest_path.display()
        )));
    }
    let trusted: serde_json::Value = serde_json::from_str(NCM_REFERENCE_WORKER_MANIFEST)
        .map_err(|error| config_error(format!("decode shipped NCM worker manifest: {error}")))?;
    let actual_bytes = fs::read(&manifest_path).map_err(|error| {
        config_error(format!(
            "read NCM worker manifest '{}': {error}",
            manifest_path.display()
        ))
    })?;
    let actual: serde_json::Value = serde_json::from_slice(&actual_bytes).map_err(|error| {
        config_error(format!(
            "decode NCM worker manifest '{}': {error}",
            manifest_path.display()
        ))
    })?;
    let trusted_digest = Sha256::digest(
        &tracedecay_domain::canonical_json_bytes(&trusted)
            .map_err(|error| config_error(format!("canonicalize shipped NCM manifest: {error}")))?,
    );
    let actual_digest = Sha256::digest(
        &tracedecay_domain::canonical_json_bytes(&actual)
            .map_err(|error| config_error(format!("canonicalize NCM worker manifest: {error}")))?,
    );
    if trusted_digest != actual_digest {
        return Err(config_error(
            "NCM worker manifest does not match the shipped attestation",
        ));
    }
    let expected_os = if cfg!(target_os = "macos") {
        "macos"
    } else if cfg!(target_os = "linux") {
        "linux"
    } else if cfg!(windows) {
        "windows"
    } else {
        std::env::consts::OS
    };
    let expected_family = if cfg!(unix) { "unix" } else { "windows" };
    let expected_arch = match std::env::consts::ARCH {
        "aarch64" => "aarch64",
        "x86_64" => "x86_64",
        other => other,
    };
    let target = actual
        .get("targets")
        .and_then(serde_json::Value::as_array)
        .and_then(|targets| {
            targets.iter().find(|target| {
                target.get("os").and_then(serde_json::Value::as_str) == Some(expected_os)
                    && target.get("arch").and_then(serde_json::Value::as_str) == Some(expected_arch)
                    && target.get("family").and_then(serde_json::Value::as_str)
                        == Some(expected_family)
            })
        })
        .ok_or_else(|| config_error("NCM worker manifest has no target for this platform"))?;
    let expected_bytes = target
        .get("bytes")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| config_error("NCM worker manifest target has no byte length"))?;
    let expected_digest = target
        .get("sha256")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| config_error("NCM worker manifest target has no SHA-256 digest"))?;
    if worker_metadata.len() != expected_bytes {
        return Err(config_error(format!(
            "NCM worker size differs from its shipped attestation (expected {expected_bytes}, got {})",
            worker_metadata.len()
        )));
    }
    let mut file = File::open(worker).map_err(|error| {
        config_error(format!("open NCM worker '{}': {error}", worker.display()))
    })?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|error| {
            config_error(format!("read NCM worker '{}': {error}", worker.display()))
        })?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    if !expected_digest.eq_ignore_ascii_case(&hex::encode(digest.finalize())) {
        return Err(config_error(
            "NCM worker bytes differ from their shipped attestation",
        ));
    }
    Ok(())
}

fn first_party_dry_run_check_for_provider(
    profile_root: &Path,
    provider: ReplacementProviderArg,
) -> Result<()> {
    first_party_dry_run_check(profile_root)?;
    if provider == ReplacementProviderArg::Ncm && !provider_readiness(provider, profile_root)?.0 {
        return Err(config_error(
            "NCM replacement requires an installed, independently attested worker bundle",
        ));
    }
    ensure_supported_replacement_platform()?;
    Ok(())
}

fn ensure_supported_replacement_platform() -> Result<()> {
    let (supported, message) = replacement_platform_validation();
    if supported {
        Ok(())
    } else {
        Err(config_error(message))
    }
}

fn replacement_platform_validation() -> (bool, &'static str) {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        (
            true,
            "durable directory exchange is released on this platform",
        )
    }
    #[cfg(windows)]
    {
        (
            false,
            "V1-to-V2 replacement is unavailable on Windows until the durable directory exchange implementation is released; no mutation was started",
        )
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    {
        (
            false,
            "V1-to-V2 replacement requires a released atomic directory exchange implementation on this platform; no mutation was started",
        )
    }
}

#[derive(Clone, Debug)]
struct ReplacementServicePlan {
    source_namespace: Option<String>,
    target_namespace: String,
    endpoint: PathBuf,
    was_installed: bool,
}

const MANAGED_SERVICE_HANDOFF_UNAVAILABLE: &str = "offline replacement cannot apply while a managed daemon service is installed in the current service namespace: this release cannot prove or perform an atomic V1-to-V2 service handoff. Use a fresh V2 profile for the supported transition path; this offline command requires an empty service namespace, and removing a service does not make an otherwise unsupported V1 profile eligible";

fn ensure_managed_service_handoff_is_supported(was_installed: bool) -> Result<()> {
    if was_installed {
        return Err(config_error(MANAGED_SERVICE_HANDOFF_UNAVAILABLE));
    }
    Ok(())
}

fn replacement_service_was_installed() -> Result<bool> {
    Ok(!matches!(
        tracedecay_daemon_control::installed_service_state()?,
        tracedecay_daemon_control::DaemonServiceState::Missing
    ))
}

fn resolve_replacement_service_plan(
    profile_root: &Path,
    operation_id: &str,
    was_installed: bool,
) -> Result<ReplacementServicePlan> {
    // Resolve the typed namespace once. The lifecycle crate owns validation
    // and identity formatting; this coordinator stores only the validated
    // suffix in its journal so a later environment change cannot retarget a
    // recovery operation.
    let source_namespace = tracedecay_daemon_control::ServiceNamespace::current()?;
    let target_namespace = tracedecay_daemon_control::ServiceNamespace::from_suffix(format!(
        "v2-{}",
        short_digest(operation_id)
    ))?;
    let endpoint = profile_root.join("daemon-v2.sock");
    Ok(ReplacementServicePlan {
        source_namespace: source_namespace.suffix().map(ToOwned::to_owned),
        target_namespace: target_namespace
            .suffix()
            .ok_or_else(|| config_error("replacement shadow namespace must have a suffix"))?
            .to_owned(),
        endpoint,
        was_installed,
    })
}

fn resolve_replacement_service_identity(
    profile_root: &Path,
    operation_id: &str,
    was_installed: bool,
) -> Result<ReplacementServiceIdentity> {
    let plan = resolve_replacement_service_plan(profile_root, operation_id, was_installed)?;
    Ok(ReplacementServiceIdentity {
        source_namespace: plan.source_namespace,
        target_namespace: plan.target_namespace,
        endpoint: plan.endpoint,
        was_installed: plan.was_installed,
        restore_source_on_rollback: plan.was_installed,
    })
}

fn validate_service_namespace(value: &str) -> Result<()> {
    tracedecay_daemon_control::ServiceNamespace::from_suffix(value.to_owned()).map(|_| ())
}

fn short_digest(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    hex::encode(&digest[..8])
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum WorkerPhase {
    Preflight,
    Apply,
    Verify,
}

impl WorkerPhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Preflight => "preflight",
            Self::Apply => "apply",
            Self::Verify => "verify",
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerReport {
    protocol: String,
    protocol_version: u32,
    operation_id: String,
    /// Serving implementation selected for the target. This is bound into
    /// every phase receipt so a retry cannot silently switch provider state.
    provider: String,
    phase: WorkerPhase,
    /// `apply` and `verify` must have committed the isolated target. The
    /// preflight phase is read-only and therefore must remain uncommitted.
    committed: bool,
    /// Only the verify phase may claim that the target has the exact V2 shape.
    exact: bool,
    /// Every complete profile authority must occur exactly once.
    authorities: Vec<String>,
    /// Content binding for the phase target. The CLI recomputes this digest
    /// from the target tree and refuses a receipt that does not match it.
    target_digest: String,
    /// Per-authority artifact bindings. The coordinator computes the same
    /// observations independently, so a worker cannot turn a list of names
    /// into a preservation claim without matching bytes and artifact counts
    /// for every authority. The shipped worker receipt is accepted only after
    /// this coordinator recomputes the same bindings independently.
    authority_manifest: Vec<WorkerAuthorityReport>,
    /// Opaque profile entries (including external LCM payload objects) are
    /// bound separately because they are outside the eight canonical backup
    /// authorities and must survive byte-for-byte.
    opaque_manifest: OpaqueManifest,
    /// Project roots and their workflow/Git working-tree bytes remain
    /// external to the profile shard. Their independent inventory is carried
    /// in every receipt so the replacement cannot silently detach a project.
    external_projects: ExternalProjectManifest,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct WorkerAuthorityReport {
    authority: String,
    source_digest: String,
    source_entries: u64,
    target_digest: String,
    target_entries: u64,
    /// A deterministic semantic census over the authority's bytes and
    /// SQLite rows. It is carried by the worker receipt and recomputed by the
    /// coordinator before the receipt is accepted.
    source_semantic_digest: String,
    target_semantic_digest: String,
    source_rows: u64,
    target_rows: u64,
    /// Table/field and row digests are retained in the manifest instead of
    /// reducing semantic preservation to a single self-attested number.
    source_tables: Vec<SemanticTableReport>,
    target_tables: Vec<SemanticTableReport>,
    /// A deterministic source-to-target mapping for every observed table. A
    /// missing side is explicit, which makes a dropped or newly invented
    /// authority visible even when aggregate row counts happen to match.
    semantic_mapping: Vec<SemanticTableMapping>,
    /// Digest of the explicit source-to-target mapping. A byte-preserved
    /// authority uses an identity mapping; migrated SQLite authorities bind
    /// the source census to the resulting canonical census.
    mapping_digest: String,
    /// Typed disposition for every source/target table pair. A dropped source
    /// table is retained in the external V1 backup unless the disposition is
    /// explicitly a rebuildable projection.
    dispositions: Vec<SemanticDisposition>,
    /// Counts for lifecycle states which must survive conversion, including
    /// live, evicted, duplicate, and tombstone records when an authority
    /// exposes those states.
    source_status_counts: BTreeMap<String, u64>,
    target_status_counts: BTreeMap<String, u64>,
    /// Aggregate row bindings are retained at the authority level as well as
    /// in each table entry.  This keeps the compact release receipt useful to
    /// consumers that do not need to walk the full table census.
    source_row_digest: String,
    target_row_digest: String,
    /// The authority-level disposition is derived from the typed table
    /// dispositions below.  It is descriptive only; the per-table entries
    /// remain the source of truth for dropped/rebuilt/transformed tables.
    disposition: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct SemanticTableReport {
    table: String,
    columns: Vec<String>,
    rows: u64,
    row_digest: String,
    #[serde(default)]
    status_counts: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct SemanticTableMapping {
    source_table: Option<String>,
    target_table: Option<String>,
    source_rows: u64,
    target_rows: u64,
    source_row_digest: String,
    target_row_digest: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct SemanticDisposition {
    source_table: Option<String>,
    target_table: Option<String>,
    source_rows: u64,
    target_rows: u64,
    source_row_digest: String,
    target_row_digest: String,
    /// `preserved`, `transformed`, `rebuilt_projection`, or
    /// `retained_in_backup`.
    disposition: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct OpaqueManifest {
    source_digest: String,
    source_entries: u64,
    target_digest: String,
    target_entries: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct ExternalProjectReport {
    project_id: String,
    project_root: PathBuf,
    digest: String,
    entries: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct ExternalProjectManifest {
    source: Vec<ExternalProjectReport>,
    target: Vec<ExternalProjectReport>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ReplacementPhase {
    Prepared,
    SourceQuarantined,
    Published,
    RolledBack,
    Verified,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReplacementJournal {
    schema_version: u32,
    operation_id: String,
    provider: String,
    profile_root: PathBuf,
    backup_root: PathBuf,
    backup_staging_root: PathBuf,
    source_view_root: PathBuf,
    backup_view_root: PathBuf,
    worker_stage_root: PathBuf,
    worker_sha256: String,
    target_root: PathBuf,
    rehearsal_root: PathBuf,
    rehearsal_staging_root: PathBuf,
    quarantine_root: PathBuf,
    rollback_root: PathBuf,
    phase: ReplacementPhase,
    /// Full source namespace attestation captured before the journal itself
    /// was written. Recovery uses it to reject a changed opaque/provider
    /// entry even when the disposable source view was lost in a crash.
    source_tree_digest: String,
    /// The complete backup attestation is committed into the journal before
    /// any target work begins. Recovery refuses a backup whose bytes,
    /// manifest, or source identity no longer match this record.
    #[serde(default)]
    backup_tree_digest: Option<String>,
    #[serde(default)]
    backup_manifest_sha256: Option<String>,
    #[serde(default)]
    backup_identity_sha256: Option<String>,
    /// The isolated target's digest is persisted before publication. This
    /// lets startup distinguish a crash after an atomic exchange from a
    /// target that was never ready.
    #[serde(default)]
    staged_target_digest: Option<String>,
    #[serde(default)]
    authority_progress: Vec<AuthorityProgress>,
    /// The external project/Git/workflow roots observed before the first
    /// migration future ran. Rechecking this baseline catches a runtime that
    /// accidentally mutates a source checkout while opening the target.
    #[serde(default)]
    source_external_projects: Vec<ExternalProjectReport>,
    /// Service identity is resolved once before the transaction and carried
    /// through recovery. It prevents an ambient namespace change from
    /// redirecting stop/start to a different installed unit.
    #[serde(default)]
    service: Option<ReplacementServiceIdentity>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct ReplacementServiceIdentity {
    source_namespace: Option<String>,
    target_namespace: String,
    endpoint: PathBuf,
    was_installed: bool,
    restore_source_on_rollback: bool,
}

/// Every disposable directory created by the replacement carries this
/// ownership record. The operation id alone in a sibling name is insufficient
/// after a crash because an operator could replace that directory before the
/// next invocation. The marker is excluded from content digests and its
/// expected digest is checked before a directory is adopted or removed.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct ReplacementStagingMarker {
    schema_version: u32,
    operation_id: String,
    kind: String,
    expected_digest: Option<String>,
}

/// The maintenance backup crate owns its rehearsal marker type. Keep a
/// private wire-compatible view here so a crash between maintenance's rename
/// and this coordinator's marker publication can be resumed safely without
/// making the marker an ambient, unvalidated cleanup signal.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MaintenanceRehearsalMarker {
    schema_version: u32,
    backup_id: String,
    backup_root: PathBuf,
    manifest_sha256: String,
    source_profile_identity_sha256: String,
    restore_root: PathBuf,
}

const MAINTENANCE_REHEARSAL_MARKER_SCHEMA_VERSION: u32 = 3;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct AuthorityProgress {
    authority: String,
    status: AuthorityProgressStatus,
    source_digest: String,
    target_digest: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum AuthorityProgressStatus {
    Pending,
    Copied,
    Migrated,
    Verified,
}

#[derive(Debug, Serialize)]
struct ReplacementSummary {
    protocol: &'static str,
    protocol_version: u32,
    operation_id: String,
    provider: String,
    backup_root: PathBuf,
    preserved_v1_root: PathBuf,
    worker: PathBuf,
    worker_sha256: String,
    service_namespace: Option<String>,
    authorities: Vec<&'static str>,
    rollback: &'static str,
}

#[derive(Debug, Serialize)]
struct ReplacementPlan {
    protocol: &'static str,
    protocol_version: u32,
    profile_root: PathBuf,
    backup_parent: PathBuf,
    backup_id: String,
    provider: String,
    backup_destination: PathBuf,
    backup_parent_state: &'static str,
    backup_destination_available: bool,
    profile_authorities_present: Vec<&'static str>,
    profile_authorities_missing: Vec<&'static str>,
    worker: PathBuf,
    worker_sha256: Option<String>,
    worker_ready: bool,
    first_party_worker_available: bool,
    first_party_preflight_ready: bool,
    backup_feasible: bool,
    target_namespace_feasible: bool,
    worker_validation: &'static str,
    apply_feasibility: &'static str,
    authorities: Vec<&'static str>,
    phases: Vec<&'static str>,
    requires_confirmation: bool,
    makes_changes: bool,
    rollback: &'static str,
    service_namespace: Option<String>,
    service_present: bool,
    service_handoff_ready: bool,
    provider_ready: bool,
    provider_validation: &'static str,
    platform_supported: bool,
    platform_validation: &'static str,
}

#[derive(Debug)]
struct WorkerOutput {
    status: std::process::ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    output_overflowed: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct FirstPartyManifest {
    schema_version: u32,
    protocol: String,
    protocol_version: u32,
    operation_id: String,
    provider: String,
    source_tree_digest: String,
    authorities: Vec<WorkerAuthorityReport>,
    opaque_manifest: OpaqueManifest,
    external_projects: ExternalProjectManifest,
}

const FIRST_PARTY_MANIFEST_SCHEMA_VERSION: u32 = 1;

/// Run the explicit first-party replacement. The optional worker path is kept
/// for protocol compatibility, while migration and its receipt always bind to
/// the implementation shipped in this binary.
pub(crate) async fn handle_replace_v1(
    profile_root: Option<String>,
    backup_to: String,
    backup_id: String,
    provider: ReplacementProviderArg,
    worker: Option<String>,
    timeout_seconds: u64,
    json: bool,
    assume_yes: bool,
    dry_run: bool,
) -> Result<()> {
    if timeout_seconds == 0 {
        return Err(config_error("--timeout-seconds must be greater than zero"));
    }

    // Check the explicit confirmation before touching the profile or worker
    // path. A rejected cutover must be observationally empty, even when the
    // operator supplied stale or missing operational paths.
    if !dry_run && !assume_yes {
        return Err(config_error(
            "storage replace-v1 is an explicit destructive cutover: it quiesces the profile, \
             creates an external verified backup, and publishes a new V2 profile. Re-run with \
             --yes to confirm",
        ));
    }

    let profile_root = resolve_profile_root(profile_root)?;
    // Establish the physical directory identity before recovery can inspect
    // or mutate a journal. A symlinked selector must never be allowed to
    // redirect recovery into a different profile.
    validate_profile_root_path(&profile_root)?;
    // Service admission must precede recovery and every profile mutation. The
    // daemon-control state is namespace-wide, so an installed unit cannot be
    // proven unrelated to this profile and must fail closed.
    let service_was_installed = replacement_service_was_installed()?;
    if !dry_run {
        ensure_managed_service_handoff_is_supported(service_was_installed)?;
    }
    if let Some(summary) =
        recover_interrupted_replacement_if_present(&profile_root, dry_run, timeout_seconds)?
    {
        print_replacement_summary(&summary, json)?;
        return Ok(());
    }
    validate_profile_root(&profile_root)?;
    let (_, missing_authorities) = inspect_profile_authorities(&profile_root)?;
    if !dry_run && !missing_authorities.is_empty() {
        return Err(config_error(format!(
            "replacement source is missing required authorities: {}",
            missing_authorities.join(", ")
        )));
    }
    validate_path_component(&backup_id, "--backup-id")?;
    let backup_parent = validate_backup_parent(&profile_root, Path::new(&backup_to))?;
    // The legacy `--worker` selector is intentionally ignored. Replacement
    // must attest and report the exact binary that is already running this
    // command; resolving an operator-supplied pathname would reintroduce a
    // TOCTOU window between validation and execution. Keep accepting the
    // option at the parser boundary so older automation gets the same
    // fail-closed first-party behavior.
    let _legacy_worker = worker;
    let worker_path = std::env::current_exe().map_err(|error| {
        config_error(format!(
            "resolve the running tracedecay binary as the first-party worker: {error}"
        ))
    })?;
    // A dry-run is a plan, so it must not require an executable worker to be
    // present. Operators commonly build the worker after reviewing the plan,
    // and probing it here would make a no-write invocation fail for a reason
    // that only applies to the later apply phases. Existing paths are still
    // checked for an obvious symlink or non-file so the rendered plan cannot
    // silently describe an unsafe target.
    let worker = if dry_run {
        validate_worker_reference_for_plan(&worker_path)?
    } else {
        validate_worker_path(&worker_path)?
    };

    if dry_run {
        return print_replacement_plan(
            profile_root,
            backup_parent,
            backup_id,
            provider,
            worker,
            service_was_installed,
            json,
        );
    }
    let worker_sha256 = sha256_worker(&worker)?;
    ensure_supported_replacement_platform()?;
    if !provider_readiness(provider, &profile_root)?.0 {
        return Err(config_error(format!(
            "selected {} replacement provider is not ready; no profile mutation was started",
            provider_name(provider)
        )));
    }

    let operation_id = replacement_operation_id();
    let service =
        resolve_replacement_service_identity(&profile_root, &operation_id, service_was_installed)?;
    let parent = profile_root
        .parent()
        .ok_or_else(|| config_error("profile root has no parent directory"))?
        .to_path_buf();
    let target_root = parent.join(format!(".{operation_id}.target"));
    let rehearsal_root = parent.join(format!(".{operation_id}.backup-rehearsal"));
    let rehearsal_staging_root = replacement_rehearsal_staging_root(&rehearsal_root)?;
    let quarantine_root = parent.join(format!(".{operation_id}.v1-preserved"));
    let rollback_root = parent.join(format!(".{operation_id}.v2-failed"));
    let source_view_root = parent.join(format!(".{operation_id}.source-view"));
    let backup_view_root = parent.join(format!(".{operation_id}.backup-view"));
    let worker_stage_root = parent.join(format!(".{operation_id}.worker"));
    ensure_absent(&target_root, "V2 staging target")?;
    ensure_absent(&rehearsal_root, "backup rehearsal target")?;
    ensure_absent(&rehearsal_staging_root, "backup rehearsal staging")?;
    ensure_absent(&quarantine_root, "V1 quarantine target")?;
    ensure_absent(&rollback_root, "V2 rollback quarantine target")?;
    ensure_absent(&source_view_root, "read-only source view")?;
    ensure_absent(&backup_view_root, "read-only backup view")?;
    ensure_absent(&worker_stage_root, "pinned migration worker")?;

    let offline = super::take_profile_offline(&profile_root, "replace-v1")?;
    let operation_result = match offline.lease() {
        Ok(lease) => {
            execute_replacement(
                &profile_root,
                &parent,
                &backup_parent,
                &backup_id,
                provider,
                &worker,
                &worker_sha256,
                timeout_seconds,
                &operation_id,
                &target_root,
                &rehearsal_root,
                &rehearsal_staging_root,
                &quarantine_root,
                &rollback_root,
                &source_view_root,
                &backup_view_root,
                &worker_stage_root,
                &service,
                lease,
            )
            .await
        }
        Err(error) => Err(error),
    };
    let finish_result = offline.finish();
    let summary = match (operation_result, finish_result) {
        (Ok(summary), Ok(())) => summary,
        (Err(error), Ok(())) => return Err(error),
        (Ok(_), Err(error)) => {
            return Err(config_error(format!(
                "V1-to-V2 replacement verified, but restoring the previous daemon state failed: \
                 {error}. The V2 profile remains active; keep the external backup and follow \
                 the downgrade guidance below"
            )));
        }
        (Err(operation_error), Err(restore_error)) => {
            return Err(config_error(format!(
                "V1-to-V2 replacement failed: {operation_error}; restoring the previous daemon \
                 state also failed: {restore_error}. Keep the external backup and follow the \
                 downgrade guidance below"
            )));
        }
    };

    print_replacement_summary(&summary, json)
}

fn print_replacement_summary(summary: &ReplacementSummary, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(summary)?);
    } else {
        println!("V1-to-V2 replacement verified");
        println!("  provider: {}", summary.provider);
        println!("  external backup: {}", summary.backup_root.display());
        println!(
            "  preserved V1 profile: {}",
            summary.preserved_v1_root.display()
        );
        println!("  rollback: {}", summary.rollback);
    }
    Ok(())
}

fn print_replacement_plan(
    profile_root: PathBuf,
    backup_parent: PathBuf,
    backup_id: String,
    provider: ReplacementProviderArg,
    worker: PathBuf,
    service_was_installed: bool,
    json: bool,
) -> Result<()> {
    let backup_destination = backup_parent.join(&backup_id);
    let backup_parent_state = backup_parent_state(&backup_parent)?;
    let backup_destination_available = match fs::symlink_metadata(&backup_destination) {
        Ok(_) => false,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(error) => {
            return Err(config_error(format!(
                "inspect replacement backup destination '{}': {error}",
                backup_destination.display()
            )));
        }
    };
    let (profile_authorities_present, profile_authorities_missing) =
        inspect_profile_authorities(&profile_root)?;
    let worker_sha256 = sha256_worker(&worker).ok();
    let target_parent_feasible = profile_root
        .parent()
        .is_some_and(|parent| directory_creation_feasible(parent));
    let worker_ready =
        worker_sha256.is_some() && worker_is_executable(&worker) && target_parent_feasible;
    let provider_readiness = provider_readiness(provider, &profile_root)?;
    let profile_authorities_ready = profile_authorities_missing.is_empty();
    let service =
        resolve_replacement_service_plan(&profile_root, &backup_id, service_was_installed)?;
    let service_handoff_ready =
        ensure_managed_service_handoff_is_supported(service.was_installed).is_ok();
    let first_party_preflight_ready = service_handoff_ready
        && profile_authorities_ready
        && first_party_dry_run_check_for_provider(&profile_root, provider).is_ok();
    let backup_feasible = backup_destination_available
        && !matches!(
            backup_parent_state,
            "symlink (rejected)" | "not a directory (rejected)"
        )
        && directory_creation_feasible(&backup_parent);
    let target_namespace_feasible = target_parent_feasible;
    let (platform_supported, platform_validation) = replacement_platform_validation();
    let plan = ReplacementPlan {
        protocol: REPLACEMENT_PROTOCOL,
        protocol_version: REPLACEMENT_PROTOCOL_VERSION,
        profile_root,
        backup_parent,
        backup_id,
        provider: provider_name(provider).to_owned(),
        backup_destination,
        backup_parent_state,
        backup_destination_available,
        profile_authorities_present,
        profile_authorities_missing,
        worker,
        worker_sha256,
        worker_ready,
        first_party_worker_available: FIRST_PARTY_WORKER_AVAILABLE,
        first_party_preflight_ready,
        backup_feasible,
        target_namespace_feasible,
        worker_validation: FIRST_PARTY_WORKER_DESCRIPTION,
        apply_feasibility: if !platform_supported {
            "unsupported platform release gate; no mutation can start"
        } else if !service_handoff_ready {
            MANAGED_SERVICE_HANDOFF_UNAVAILABLE
        } else if !profile_authorities_ready {
            "required authorities are missing from the source profile"
        } else if !worker_ready {
            "the running first-party binary cannot be staged or is not executable"
        } else if !first_party_preflight_ready {
            "first-party preflight will refuse the source until every SQLite and store authority is admissible"
        } else if !backup_feasible || !target_namespace_feasible {
            "backup or target namespace is not writable at apply"
        } else {
            "canonical backup, first-party authority migrations, independent manifests, and target checks are revalidated before apply"
        },
        authorities: REQUIRED_AUTHORITIES.to_vec(),
        phases: vec![
            "quiesce profile",
            "create complete external backup",
            "rehearse backup into isolated rollback tree",
            "worker preflight all authorities",
            "worker apply isolated V2 target",
            "worker verify exact V2 target",
            "publish target under cutover journal",
            "worker verify published profile",
            "rollback from untouched rehearsal on failure",
        ],
        requires_confirmation: true,
        makes_changes: false,
        rollback: "stop V2, select the V1 binary/service, and restore the external backup; never open V2 files with V1",
        service_namespace: Some(service.target_namespace),
        service_present: service.was_installed,
        service_handoff_ready,
        provider_ready: provider_readiness.0,
        provider_validation: provider_readiness.1,
        platform_supported,
        platform_validation,
    };
    if json {
        println!("{}", serde_json::to_string_pretty(&plan)?);
    } else {
        println!("V1-to-V2 replacement dry-run (no changes)");
        println!("  profile: {}", plan.profile_root.display());
        println!("  provider: {}", plan.provider);
        println!(
            "  shadow service namespace: {} (existing service: {})",
            plan.service_namespace.as_deref().unwrap_or("unavailable"),
            plan.service_present
        );
        println!("  service handoff ready: {}", plan.service_handoff_ready);
        println!(
            "  external backup: {}/{}",
            plan.backup_parent.display(),
            plan.backup_id
        );
        println!(
            "  backup destination: {} ({})",
            plan.backup_destination.display(),
            if plan.backup_destination_available {
                "available"
            } else {
                "already exists"
            }
        );
        println!("  backup parent: {}", plan.backup_parent_state);
        println!(
            "  platform supported: {} ({})",
            plan.platform_supported, plan.platform_validation
        );
        println!("  worker ready: {}", plan.worker_ready);
        println!(
            "  provider ready: {} ({})",
            plan.provider_ready, plan.provider_validation
        );
        println!(
            "  first-party migration worker available: {}",
            plan.first_party_worker_available
        );
        println!(
            "  first-party preflight ready: {} (backup feasible: {}, target namespace feasible: {})",
            plan.first_party_preflight_ready, plan.backup_feasible, plan.target_namespace_feasible
        );
        println!("  apply feasibility: {}", plan.apply_feasibility);
        if !plan.profile_authorities_missing.is_empty() {
            println!(
                "  authorities missing from source: {}",
                plan.profile_authorities_missing.join(", ")
            );
        }
        println!("  authorities: {}", plan.authorities.join(", "));
        println!("  phases: {}", plan.phases.join(" -> "));
        println!("  confirmation: re-run with --yes to apply");
        println!("  rollback: {}", plan.rollback);
    }
    Ok(())
}

fn first_party_dry_run_check(profile_root: &Path) -> Result<()> {
    let (_, missing) = inspect_profile_authorities(profile_root)?;
    if !missing.is_empty() {
        return Err(config_error(format!(
            "source is missing required authorities: {}",
            missing.join(", ")
        )));
    }
    // Exercise the same complete-tree admission and digest paths used by
    // apply. These checks only read bytes, but catch opaque symlinks and
    // unreadable provider payloads before the operator confirms cutover.
    if has_first_party_manifest(profile_root)? {
        return Err(config_error(
            "source profile already contains the first-party V2 manifest; refusing to migrate it as V1",
        ));
    }
    verify_profile_namespace(profile_root, true)?;
    profile_tree_digest(profile_root)?;
    validate_sqlite_authority_files(profile_root, "first-party dry-run source")?;
    let projects = profile_root.join("projects");
    let mut stores = fs::read_dir(&projects)
        .map_err(|error| config_error(format!("read project stores during dry-run: {error}")))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            config_error(format!("enumerate project stores during dry-run: {error}"))
        })?;
    stores.sort_by_key(fs::DirEntry::file_name);
    for store in stores {
        let metadata = fs::symlink_metadata(store.path()).map_err(|error| {
            config_error(format!("inspect project store during dry-run: {error}"))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(config_error(format!(
                "project store '{}' is a symlink",
                store.path().display()
            )));
        }
        if metadata.is_dir() {
            let manifest_path = store
                .path()
                .join(tracedecay_runtime_core::storage::STORE_MANIFEST_FILENAME);
            tracedecay_runtime_core::storage::read_store_manifest(&manifest_path).map_err(
                |error| {
                    config_error(format!(
                        "read project store manifest '{}' during dry-run: {error}",
                        manifest_path.display()
                    ))
                },
            )?;
        }
    }
    // The project store manifest points at the external checkout that owns
    // workflow and Git state. Read and inventory that root during dry-run so
    // apply feasibility covers the complete source closure without writing
    // to the checkout.
    external_project_reports(profile_root)?;
    authority_manifest(profile_root, profile_root)?;
    opaque_manifest(profile_root, profile_root)?;
    Ok(())
}

async fn execute_replacement(
    profile_root: &Path,
    parent: &Path,
    backup_to: &Path,
    backup_id: &str,
    provider: ReplacementProviderArg,
    worker: &Path,
    worker_sha256: &str,
    timeout_seconds: u64,
    operation_id: &str,
    target_root: &Path,
    rehearsal_root: &Path,
    rehearsal_staging_root: &Path,
    quarantine_root: &Path,
    rollback_root: &Path,
    source_view_root: &Path,
    backup_view_root: &Path,
    worker_stage_root: &Path,
    service: &ReplacementServiceIdentity,
    lifecycle: &tracedecay_runtime_core::lifecycle_lease::LifecycleLease,
) -> Result<ReplacementSummary> {
    let provider_id = provider_name(provider);
    let created_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| config_error(format!("system clock is before Unix epoch: {error}")))?
        .as_secs()
        .try_into()
        .map_err(|_| config_error("system clock exceeds supported backup timestamp range"))?;

    // Reserve the complete transaction before creating any external backup
    // bytes. A crash during backup/rehearsal setup therefore leaves a durable
    // Prepared record that startup recovery can classify and clean up.
    validate_backup_parent(profile_root, backup_to).map_err(|error| {
        replacement_error("external backup parent", error.to_string(), None, None)
    })?;
    fs::create_dir_all(backup_to).map_err(|error| {
        replacement_error("external backup parent", error.to_string(), None, None)
    })?;
    let canonical_backup_parent = backup_to.canonicalize().map_err(|error| {
        replacement_error(
            "external backup parent",
            format!("canonicalize '{}': {error}", backup_to.display()),
            None,
            None,
        )
    })?;
    if paths_overlap(&canonical_backup_parent, profile_root)? {
        return Err(replacement_error(
            "external backup parent",
            "backup destination moved inside the profile while acquiring the lease".to_owned(),
            None,
            None,
        ));
    }
    let backup_root = canonical_backup_parent.join(backup_id);
    let backup_staging_root = canonical_backup_parent.join(format!(".{backup_id}.tmp"));
    ensure_absent(&backup_root, "external backup")?;
    ensure_absent(&backup_staging_root, "external backup staging")?;
    let journal_path = profile_root.join(JOURNAL_FILENAME);
    let source_tree_digest = profile_tree_digest(profile_root).map_err(|error| {
        replacement_error(
            "source attestation",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;
    let mut journal = ReplacementJournal {
        schema_version: JOURNAL_SCHEMA_VERSION,
        operation_id: operation_id.to_owned(),
        provider: provider_name(provider).to_owned(),
        profile_root: profile_root.to_path_buf(),
        backup_root: backup_root.clone(),
        backup_staging_root: backup_staging_root.clone(),
        source_view_root: source_view_root.to_path_buf(),
        backup_view_root: backup_view_root.to_path_buf(),
        worker_stage_root: worker_stage_root.to_path_buf(),
        worker_sha256: worker_sha256.to_owned(),
        target_root: target_root.to_path_buf(),
        rehearsal_root: rehearsal_root.to_path_buf(),
        rehearsal_staging_root: rehearsal_staging_root.to_path_buf(),
        quarantine_root: quarantine_root.to_path_buf(),
        rollback_root: rollback_root.to_path_buf(),
        phase: ReplacementPhase::Prepared,
        source_tree_digest,
        backup_tree_digest: None,
        backup_manifest_sha256: None,
        backup_identity_sha256: None,
        staged_target_digest: None,
        authority_progress: initial_authority_progress(profile_root)?,
        source_external_projects: external_project_reports(profile_root)?,
        service: Some(service.clone()),
    };
    write_journal(&journal_path, &journal)
        .map_err(|error| replacement_error("replacement journal", error.to_string(), None, None))?;

    // Pin the exact bytes that will be executed before any migration phase
    // begins. The original pathname is never used for spawning after this
    // point; a replacement of that pathname cannot change the running worker.
    let staged_worker = match stage_worker(worker, worker_stage_root, worker_sha256, operation_id) {
        Ok(staged_worker) => staged_worker,
        Err(error) => {
            // The worker staging directory is marker-bound before its bytes
            // are copied. If copying fails, remove only that authenticated
            // journal-owned partial so recovery does not mistake an unbound
            // directory for foreign material.
            cleanup_owned_directory(worker_stage_root);
            return Err(replacement_error(
                "migration worker staging",
                error.to_string(),
                None,
                None,
            ));
        }
    };
    bind_staging_directory(
        worker_stage_root,
        operation_id,
        "worker",
        Some(&profile_tree_digest(worker_stage_root)?),
        false,
    )
    .map_err(|error| {
        replacement_error("migration worker ownership", error.to_string(), None, None)
    })?;
    ensure_worker_executable(&staged_worker)?;

    // The backup API snapshots every profile authority while this exact
    // exclusive lease is held. Its destination is external and remains
    // untouched for the lifetime of the replacement and any rollback. The
    // journal above is intentionally retained if this call fails: the next
    // invocation can classify and remove an interrupted backup staging tree.
    let created_backup =
        match tracedecay_maintenance::profile_backup::create_complete_profile_backup(
            profile_root,
            &canonical_backup_parent,
            backup_id,
            created_at,
            lifecycle,
        ) {
            Ok(path) => path,
            Err(error) => {
                return Err(replacement_error(
                    "external backup",
                    error.to_string(),
                    None,
                    None,
                ));
            }
        };
    if !physical_paths_equal(&created_backup, &backup_root).map_err(|error| {
        replacement_error(
            "external backup",
            format!("compare published backup identity: {error}"),
            Some(&backup_root),
            None,
        )
    })? {
        return Err(replacement_error(
            "external backup",
            format!(
                "backup API published '{}' instead of the journaled destination '{}'",
                created_backup.display(),
                backup_root.display()
            ),
            None,
            None,
        ));
    }

    // The maintenance backup contract snapshots the governed authorities and
    // deliberately ignores provider-specific roots. Replacement has a
    // stronger preservation promise: every opaque byte (provider bundles,
    // receipts, LCM payloads, and future roots unknown to this binary) must
    // remain available even if publication later quarantines the live V1
    // directory. Extend the already published, verified backup with those
    // entries before recording its digest. The canonical manifest remains the
    // authority inventory; these additional bytes are covered by the
    // replacement journal/tree digest and are never interpreted as SQLite.
    copy_first_party_opaque_entries(profile_root, &backup_root).map_err(|error| {
        replacement_error(
            "external backup opaque preservation",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;
    copy_first_party_opaque_sidecars(profile_root, &backup_root).map_err(|error| {
        replacement_error(
            "external backup opaque sidecar preservation",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;
    sync_replacement_tree(&backup_root).map_err(|error| {
        replacement_error(
            "external backup opaque durability",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;

    let backup_tree_digest = profile_tree_digest(&backup_root).map_err(|error| {
        replacement_error(
            "external backup attestation",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;
    let backup_manifest_sha256 = sha256_regular_file(&backup_root.join("backup-manifest.json"))?;
    let backup_identity_sha256 = sha256_regular_file(&backup_root.join("profile-identity.json"))?;
    journal.backup_tree_digest = Some(backup_tree_digest.clone());
    journal.backup_manifest_sha256 = Some(backup_manifest_sha256.clone());
    journal.backup_identity_sha256 = Some(backup_identity_sha256.clone());
    write_journal(&journal_path, &journal).map_err(|error| {
        replacement_error(
            "replacement journal backup attestation",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;

    // Reuse the production rehearsal path before asking the worker to mutate
    // anything. This proves the backup can be restored into an isolated
    // namespace and leaves a ready rollback tree beside the profile.
    tracedecay_maintenance::profile_backup::rehearse_complete_profile_backup(
        &backup_root,
        rehearsal_root,
    )
    .map_err(|error| {
        replacement_error(
            "backup rehearsal",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;
    // The canonical profile backup manifest intentionally names the eight
    // authorities, while V1 may also carry provider settings, external LCM
    // payloads, receipts, or other opaque roots. Keep those bytes in the
    // rollback rehearsal as well as in the live source quarantine. Without
    // this copy a post-publication rollback could restore the relational
    // authorities and silently lose an opaque root.
    copy_first_party_opaque_entries(profile_root, rehearsal_root).map_err(|error| {
        replacement_error(
            "backup rehearsal opaque preservation",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;
    copy_first_party_opaque_sidecars(profile_root, rehearsal_root).map_err(|error| {
        replacement_error(
            "backup rehearsal opaque sidecar preservation",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;
    sync_replacement_tree(rehearsal_root).map_err(|error| {
        replacement_error(
            "backup rehearsal opaque durability",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;
    let rehearsal_digest = profile_tree_digest(rehearsal_root).map_err(|error| {
        replacement_error(
            "backup rehearsal attestation",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;
    bind_staging_directory(
        rehearsal_root,
        operation_id,
        "rehearsal",
        Some(&rehearsal_digest),
        false,
    )
    .map_err(|error| {
        replacement_error(
            "backup rehearsal ownership",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;

    create_private_directory(target_root).map_err(|error| {
        replacement_error(
            "V2 staging setup",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;
    let empty_target_digest = profile_tree_digest(target_root).map_err(|error| {
        replacement_error(
            "V2 staging attestation",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;
    bind_staging_directory(
        target_root,
        operation_id,
        "target",
        Some(&empty_target_digest),
        false,
    )
    .map_err(|error| {
        replacement_error(
            "V2 staging ownership",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;

    let timeout = Duration::from_secs(timeout_seconds);
    let source_digest = profile_tree_digest(profile_root)?;
    let backup_digest = backup_tree_digest;
    copy_worker_view(profile_root, source_view_root).map_err(|error| {
        replacement_error(
            "isolated source view setup",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;
    bind_staging_directory(
        source_view_root,
        operation_id,
        "source-view",
        Some(&source_digest),
        true,
    )
    .map_err(|error| {
        replacement_error(
            "source view ownership",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;
    copy_worker_view(&backup_root, backup_view_root).map_err(|error| {
        replacement_error(
            "isolated backup view setup",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;
    bind_staging_directory(
        backup_view_root,
        operation_id,
        "backup-view",
        Some(&backup_digest),
        true,
    )
    .map_err(|error| {
        replacement_error(
            "backup view ownership",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;
    ensure_worker_inputs(
        source_view_root,
        backup_view_root,
        profile_root,
        &source_digest,
        &backup_root,
        &backup_digest,
    )
    .map_err(|error| {
        replacement_error(
            "isolated worker input verification",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;

    invoke_first_party_worker(
        &staged_worker,
        worker_sha256,
        WorkerPhase::Preflight,
        operation_id,
        provider_id,
        source_view_root,
        backup_view_root,
        target_root,
        profile_root,
        &source_digest,
        &backup_root,
        &backup_digest,
        timeout,
    )
    .and_then(|report| {
        ensure_worker_inputs(
            source_view_root,
            backup_view_root,
            profile_root,
            &source_digest,
            &backup_root,
            &backup_digest,
        )
        .map(|()| report)
    })
    .and_then(|report| {
        validate_worker_report(
            report,
            WorkerPhase::Preflight,
            operation_id,
            provider_id,
            profile_root,
            target_root,
        )
    })
    .map_err(|error| {
        // Keep the Prepared journal until the owned workspace is either
        // removed or recovered on the next invocation. Removing it first
        // would turn a cleanup failure into an orphaned transaction.
        cleanup_owned_directory(target_root);
        cleanup_owned_directory(rehearsal_root);
        cleanup_owned_directory(rehearsal_staging_root);
        cleanup_worker_views(source_view_root, backup_view_root);
        cleanup_owned_directory(worker_stage_root);
        replacement_error(
            "worker preflight",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;
    if !directory_is_empty(target_root).map_err(|error| {
        replacement_error(
            "worker preflight",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })? {
        cleanup_owned_directory(target_root);
        cleanup_owned_directory(rehearsal_root);
        cleanup_owned_directory(rehearsal_staging_root);
        cleanup_worker_views(source_view_root, backup_view_root);
        cleanup_owned_directory(worker_stage_root);
        return Err(replacement_error(
            "worker preflight",
            "preflight wrote into the isolated target; preflight must be read-only".to_owned(),
            Some(&backup_root),
            None,
        ));
    }

    let _apply_report = invoke_first_party_worker(
        &staged_worker,
        worker_sha256,
        WorkerPhase::Apply,
        operation_id,
        provider_id,
        source_view_root,
        backup_view_root,
        target_root,
        profile_root,
        &source_digest,
        &backup_root,
        &backup_digest,
        timeout,
    )
    .and_then(|report| {
        ensure_worker_inputs(
            source_view_root,
            backup_view_root,
            profile_root,
            &source_digest,
            &backup_root,
            &backup_digest,
        )
        .map(|()| report)
    })
    .and_then(|report| {
        validate_worker_report(
            report,
            WorkerPhase::Apply,
            operation_id,
            provider_id,
            profile_root,
            target_root,
        )
    })
    .map_err(|error| {
        cleanup_owned_directory(target_root);
        cleanup_owned_directory(rehearsal_root);
        cleanup_owned_directory(rehearsal_staging_root);
        cleanup_worker_views(source_view_root, backup_view_root);
        cleanup_owned_directory(worker_stage_root);
        replacement_error("worker apply", error.to_string(), Some(&backup_root), None)
    })?;
    journal.authority_progress = read_journal(&journal_path)
        .map_err(|error| {
            replacement_error(
                "authority checkpoint",
                error.to_string(),
                Some(&backup_root),
                None,
            )
        })?
        .authority_progress;
    verify_profile_namespace(target_root, true).map_err(|error| {
        cleanup_owned_directory(target_root);
        cleanup_owned_directory(rehearsal_root);
        cleanup_owned_directory(rehearsal_staging_root);
        cleanup_worker_views(source_view_root, backup_view_root);
        cleanup_owned_directory(worker_stage_root);
        replacement_error(
            "staged V2 verification",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;
    invoke_first_party_worker(
        &staged_worker,
        worker_sha256,
        WorkerPhase::Verify,
        operation_id,
        provider_id,
        source_view_root,
        backup_view_root,
        target_root,
        profile_root,
        &source_digest,
        &backup_root,
        &backup_digest,
        timeout,
    )
    .and_then(|report| {
        ensure_worker_inputs(
            source_view_root,
            backup_view_root,
            profile_root,
            &source_digest,
            &backup_root,
            &backup_digest,
        )
        .map(|()| report)
    })
    .and_then(|report| {
        validate_worker_report(
            report,
            WorkerPhase::Verify,
            operation_id,
            provider_id,
            profile_root,
            target_root,
        )
    })
    .map_err(|error| {
        cleanup_owned_directory(target_root);
        cleanup_owned_directory(rehearsal_root);
        cleanup_owned_directory(rehearsal_staging_root);
        cleanup_worker_views(source_view_root, backup_view_root);
        cleanup_owned_directory(worker_stage_root);
        replacement_error(
            "staged V2 worker verification",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;

    journal.staged_target_digest = Some(profile_tree_digest(target_root)?);
    bind_staging_directory(
        target_root,
        operation_id,
        "target",
        journal.staged_target_digest.as_deref(),
        false,
    )?;
    write_journal(&journal_path, &journal).map_err(|error| {
        replacement_error(
            "replacement journal staged-target attestation",
            error.to_string(),
            Some(&backup_root),
            None,
        )
    })?;

    publish_target(
        profile_root,
        parent,
        target_root,
        quarantine_root,
        &journal_path,
        &mut journal,
    )
    .map_err(|error| {
        let rollback = rollback_from_rehearsal(
            profile_root,
            rehearsal_root,
            rollback_root,
            &journal_path,
            &mut journal,
        );
        cleanup_worker_views(source_view_root, backup_view_root);
        cleanup_owned_directory(rehearsal_staging_root);
        cleanup_owned_directory(worker_stage_root);
        replacement_error(
            "profile publication",
            match rollback {
                Ok(()) => format!("{error}; rollback completed from the untouched backup"),
                Err(rollback_error) => {
                    format!("{error}; rollback from the untouched backup failed: {rollback_error}")
                }
            },
            Some(&backup_root),
            Some(quarantine_root),
        )
    })?;

    let quarantine_digest = profile_tree_digest(quarantine_root).map_err(|error| {
        cleanup_worker_views(source_view_root, backup_view_root);
        cleanup_owned_directory(rehearsal_staging_root);
        cleanup_owned_directory(worker_stage_root);
        replacement_error(
            "preserved V1 verification",
            error.to_string(),
            Some(&backup_root),
            Some(quarantine_root),
        )
    })?;
    let post_verify = invoke_first_party_worker(
        &staged_worker,
        worker_sha256,
        WorkerPhase::Verify,
        operation_id,
        provider_id,
        source_view_root,
        backup_view_root,
        profile_root,
        quarantine_root,
        &quarantine_digest,
        &backup_root,
        &backup_digest,
        timeout,
    )
    .and_then(|report| {
        ensure_worker_inputs(
            source_view_root,
            backup_view_root,
            quarantine_root,
            &quarantine_digest,
            &backup_root,
            &backup_digest,
        )
        .map(|()| report)
    })
    .and_then(|report| {
        validate_worker_report(
            report,
            WorkerPhase::Verify,
            operation_id,
            provider_id,
            quarantine_root,
            profile_root,
        )
    })
    .and_then(|_| verify_profile_namespace(profile_root, true));
    if let Err(error) = post_verify {
        let rollback = rollback_from_rehearsal(
            profile_root,
            rehearsal_root,
            rollback_root,
            &journal_path,
            &mut journal,
        );
        cleanup_worker_views(source_view_root, backup_view_root);
        cleanup_owned_directory(rehearsal_staging_root);
        cleanup_owned_directory(worker_stage_root);
        return Err(replacement_error(
            "post-publication V2 verification",
            match rollback {
                Ok(()) => format!("{error}; rollback completed from the untouched backup"),
                Err(rollback_error) => {
                    format!("{error}; rollback from the untouched backup failed: {rollback_error}")
                }
            },
            Some(&backup_root),
            Some(quarantine_root),
        ));
    }

    validate_staging_directory(
        profile_root,
        &journal.operation_id,
        "target",
        journal.staged_target_digest.as_deref(),
    )?;
    remove_staging_marker_for_operation(profile_root, &journal.operation_id)?;
    journal.phase = ReplacementPhase::Verified;
    write_journal(&journal_path, &journal).map_err(|error| {
        replacement_error(
            "replacement journal",
            error.to_string(),
            Some(&backup_root),
            Some(quarantine_root),
        )
    })?;
    remove_replacement_workspace(
        target_root,
        rehearsal_root,
        rehearsal_staging_root,
        rollback_root,
        source_view_root,
        backup_view_root,
        worker_stage_root,
    )
    .map_err(|error| {
        replacement_error(
            "replacement cleanup",
            error.to_string(),
            Some(&backup_root),
            Some(quarantine_root),
        )
    })?;
    remove_journal(&journal_path).map_err(|error| {
        replacement_error(
            "replacement journal cleanup",
            error.to_string(),
            Some(&backup_root),
            Some(quarantine_root),
        )
    })?;

    Ok(ReplacementSummary {
        protocol: REPLACEMENT_PROTOCOL,
        protocol_version: REPLACEMENT_PROTOCOL_VERSION,
        operation_id: operation_id.to_owned(),
        provider: provider_id.to_owned(),
        backup_root,
        preserved_v1_root: quarantine_root.to_path_buf(),
        worker: worker.to_path_buf(),
        worker_sha256: worker_sha256.to_owned(),
        service_namespace: Some(service.target_namespace.clone()),
        authorities: REQUIRED_AUTHORITIES.to_vec(),
        rollback: "stop V2, select the V1 binary/service, and restore the external backup; never open V2 files with V1",
    })
}

fn first_party_preflight(
    source_view_root: &Path,
    backup_view_root: &Path,
    target_root: &Path,
    operation_id: &str,
    provider: &str,
) -> Result<WorkerReport> {
    if !directory_is_empty(target_root)? {
        return Err(config_error(
            "first-party preflight requires an empty isolated V2 target",
        ));
    }
    validate_first_party_inputs(source_view_root, backup_view_root)?;
    Ok(WorkerReport {
        protocol: REPLACEMENT_PROTOCOL.to_owned(),
        protocol_version: REPLACEMENT_PROTOCOL_VERSION,
        operation_id: operation_id.to_owned(),
        provider: provider.to_owned(),
        phase: WorkerPhase::Preflight,
        committed: false,
        exact: false,
        authorities: REQUIRED_AUTHORITIES
            .iter()
            .map(|authority| (*authority).to_owned())
            .collect(),
        target_digest: profile_tree_digest(target_root)?,
        authority_manifest: authority_manifest(source_view_root, target_root)?,
        opaque_manifest: opaque_manifest(source_view_root, target_root)?,
        external_projects: external_project_manifest(source_view_root, target_root)?,
    })
}

fn validate_first_party_inputs(source_root: &Path, backup_root: &Path) -> Result<()> {
    for authority in REQUIRED_AUTHORITIES {
        let source_path = source_root.join(authority);
        let backup_path = backup_root.join(authority);
        let source_metadata = fs::symlink_metadata(&source_path).map_err(|error| {
            config_error(format!(
                "first-party source authority '{}' is unavailable: {error}",
                source_path.display()
            ))
        })?;
        let backup_metadata = fs::symlink_metadata(&backup_path).map_err(|error| {
            config_error(format!(
                "first-party backup authority '{}' is unavailable: {error}",
                backup_path.display()
            ))
        })?;
        if source_metadata.file_type().is_symlink()
            || backup_metadata.file_type().is_symlink()
            || (!source_metadata.is_file() && !source_metadata.is_dir())
            || (!backup_metadata.is_file() && !backup_metadata.is_dir())
        {
            return Err(config_error(format!(
                "first-party authority '{authority}' must be a regular file or directory"
            )));
        }
    }
    // The landed complete-backup contract is an independent admission check:
    // it validates the identity, project store manifests, SQLite snapshots,
    // and every required logical entry in the disposable backup view.
    tracedecay_maintenance::profile_backup::load_and_verify_backup(backup_root).map_err(
        |error| {
            config_error(format!(
                "first-party backup view failed canonical verification: {error}"
            ))
        },
    )?;
    validate_sqlite_authority_files(source_root, "first-party source")?;
    validate_sqlite_authority_files(backup_root, "first-party backup")
}

fn validate_sqlite_authority_files(root: &Path, label: &str) -> Result<()> {
    for authority in ["global.db", "user-sessions.db", "user-memory.db"] {
        let path = root.join(authority);
        if !tracedecay_runtime_core::storage::has_sqlite_database_header(&path).map_err(
            |error| {
                config_error(format!(
                    "{label} authority '{authority}' is not a readable SQLite database: {error}"
                ))
            },
        )? {
            return Err(config_error(format!(
                "{label} authority '{authority}' is not a SQLite database"
            )));
        }
    }
    let projects = root.join("projects");
    validate_sqlite_files_below(&projects, label)
}

fn validate_sqlite_files_below(root: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(root).map_err(|error| {
        config_error(format!(
            "inspect '{root:?}' for SQLite authorities: {error}"
        ))
    })?;
    if metadata.file_type().is_symlink() {
        return Err(config_error(format!(
            "{label} project authority contains a symlink '{}', refusing migration",
            root.display()
        )));
    }
    if metadata.is_file() {
        let has_sqlite_header = tracedecay_runtime_core::storage::has_sqlite_database_header(root)
            .map_err(|error| {
                config_error(format!(
                    "{label} project database '{}' is unreadable: {error}",
                    root.display()
                ))
            })?;
        // Only well-known serving artifacts are required to be SQLite. A V1
        // provider is allowed to retain an opaque payload whose filename
        // happens to end in `.db`; extension alone must never make the
        // replacement open or reject that payload. Unknown files remain in
        // the byte-preserved authority inventory.
        let file_name = root.file_name();
        let is_known_database = file_name.is_some_and(|name| {
            name == "tracedecay.db"
                || name == tracedecay_runtime_core::storage::SESSIONS_DB_FILENAME
        });
        if is_known_database && !has_sqlite_header {
            return Err(config_error(format!(
                "{label} project authority '{}' is not a SQLite database",
                root.display()
            )));
        }
        // Only the graph store is a released beta.37 v34/v35 migration
        // input. Session and provider databases have their own authorities
        // and may legitimately use a different stamp; the graph migration
        // registry is the canonical owner of this exact inventory.
        if file_name.is_some_and(|name| name == "tracedecay.db") {
            validate_released_project_shape(root, label)?;
        }
        return Ok(());
    }
    if !metadata.is_dir() {
        return Err(config_error(format!(
            "{label} project authority '{}' is not a regular directory",
            root.display()
        )));
    }
    let mut entries = fs::read_dir(root)
        .map_err(|error| {
            config_error(format!(
                "read '{}' for SQLite authorities: {error}",
                root.display()
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            config_error(format!(
                "enumerate '{}' for SQLite authorities: {error}",
                root.display()
            ))
        })?;
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in entries {
        validate_sqlite_files_below(&entry.path(), label)?;
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReleasedSchemaObject {
    object_type: String,
    name: String,
    table: String,
    sql: String,
}

fn validate_released_project_shape(path: &Path, label: &str) -> Result<()> {
    let connection =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|error| {
                config_error(format!(
                    "{label} released project store '{}' cannot be opened read-only: {error}",
                    path.display()
                ))
            })?;
    let stamp: u32 = connection
        .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
        .map_err(|error| {
            config_error(format!(
                "{label} released project store '{}' has no readable schema stamp: {error}",
                path.display()
            ))
        })?
        .try_into()
        .map_err(|_| {
            config_error(format!(
                "{label} released project store '{}' has an invalid schema stamp",
                path.display()
            ))
        })?;
    let current = tracedecay_runtime_core::db::migrations::SCHEMA_VERSION;
    if stamp == current {
        return Ok(());
    }
    if !matches!(stamp, 34 | 35) {
        return Err(config_error(format!(
            "{label} project store '{}' has unsupported schema stamp v{stamp}; expected released v34/v35 or current v{current}",
            path.display()
        )));
    }
    let actual = read_released_schema_inventory(&connection)?;
    let expected_inventories = released_schema_inventories(stamp)?;
    if expected_inventories
        .iter()
        .any(|expected| expected == &actual)
    {
        return Ok(());
    }
    let expected = expected_inventories
        .first()
        .ok_or_else(|| config_error("released project schema fixture set is empty"))?;
    let reason = released_schema_difference(&actual, expected)
        .unwrap_or_else(|| "source schema does not match any immutable released inventory".into());
    Err(config_error(format!(
        "{label} project store '{}' failed exact released v{stamp} inventory admission: {reason}",
        path.display()
    )))
}

fn released_schema_inventories(stamp: u32) -> Result<Vec<Vec<ReleasedSchemaObject>>> {
    let mut inventories = Vec::new();
    if stamp == 34 {
        inventories.push(read_released_schema_fixture(
            RELEASED_V34_PROJECT_STORE_SQL,
            &[],
        )?);
        return Ok(inventories);
    }
    // A v35 stamp admits the current release DDL and the three transitional
    // inventories explicitly handled by the runtime migration bridge.
    inventories.push(read_released_schema_fixture(
        RELEASED_V35_PROJECT_STORE_SQL,
        &[],
    )?);
    inventories.push(read_released_schema_fixture(
        RELEASED_V35_PROJECT_STORE_SQL,
        &[RELEASED_V35_SHIPPED_ALIAS_TRIGGER],
    )?);
    inventories.push(read_released_schema_fixture(
        RELEASED_V34_PROJECT_STORE_SQL,
        &[RELEASED_V35_PAYLOAD_DIGESTS_SQL],
    )?);
    inventories.push(read_released_schema_fixture(
        RELEASED_V34_PROJECT_STORE_SQL,
        &[],
    )?);
    Ok(inventories)
}

fn read_released_schema_fixture(
    ddl: &str,
    additions: &[&str],
) -> Result<Vec<ReleasedSchemaObject>> {
    let connection = rusqlite::Connection::open_in_memory()
        .map_err(|error| config_error(format!("open released schema fixture: {error}")))?;
    connection
        .execute_batch(ddl)
        .map_err(|error| config_error(format!("install released schema fixture: {error}")))?;
    for addition in additions {
        if addition.contains("CREATE TRIGGER retrieval_anchor_aliases_immutable_update") {
            connection
                .execute_batch("DROP TRIGGER IF EXISTS retrieval_anchor_aliases_immutable_update;")
                .map_err(|error| {
                    config_error(format!("prepare released alias trigger fixture: {error}"))
                })?;
        }
        connection
            .execute_batch(addition)
            .map_err(|error| config_error(format!("extend released schema fixture: {error}")))?;
    }
    read_released_schema_inventory(&connection)
}

fn read_released_schema_inventory(
    connection: &rusqlite::Connection,
) -> Result<Vec<ReleasedSchemaObject>> {
    let mut statement = connection
        .prepare(
            "SELECT type, name, tbl_name, COALESCE(sql, '')
             FROM sqlite_master
             WHERE type IN ('table', 'index', 'trigger', 'view')
               AND name NOT LIKE 'sqlite_%'
             ORDER BY name",
        )
        .map_err(|error| config_error(format!("read released schema inventory: {error}")))?;
    let rows = statement
        .query_map([], |row| {
            Ok(ReleasedSchemaObject {
                object_type: row.get(0)?,
                name: row.get(1)?,
                table: row.get(2)?,
                sql: row.get(3)?,
            })
        })
        .map_err(|error| config_error(format!("enumerate released schema inventory: {error}")))?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| config_error(format!("decode released schema inventory: {error}")))
}

fn released_schema_difference(
    actual: &[ReleasedSchemaObject],
    expected: &[ReleasedSchemaObject],
) -> Option<String> {
    expected
        .iter()
        .find_map(
            |expected| match actual.iter().find(|object| object.name == expected.name) {
                None => Some(format!(
                    "source schema is missing released {} '{}'",
                    expected.object_type, expected.name
                )),
                Some(actual) if actual != expected => Some(format!(
                    "source schema has incompatible released {} '{}'",
                    expected.object_type, expected.name
                )),
                Some(_) => None,
            },
        )
        .or_else(|| {
            actual
                .iter()
                .find(|actual| !expected.iter().any(|object| object.name == actual.name))
                .map(|actual| {
                    format!(
                        "source schema contains unexpected {} '{}'",
                        actual.object_type, actual.name
                    )
                })
        })
}

fn first_party_apply(
    source_view_root: &Path,
    backup_view_root: &Path,
    target_root: &Path,
    source_root: &Path,
    worker: &Path,
    worker_sha256: &str,
    operation_id: &str,
    provider: &str,
    timeout: Duration,
) -> Result<WorkerReport> {
    validate_first_party_inputs(source_view_root, backup_view_root)?;
    let journal_path = source_root.join(JOURNAL_FILENAME);
    let source_external_baseline = read_journal(&journal_path)?.source_external_projects;
    let observed_source_external = external_project_reports(source_view_root)?;
    if !source_external_baseline.is_empty() && observed_source_external != source_external_baseline
    {
        return Err(config_error(
            "source project/workflow/Git inventory changed before first-party migration",
        ));
    }
    for authority in REQUIRED_AUTHORITIES {
        let progress = read_authority_progress(&journal_path, authority)?;
        let target_authority = target_root.join(authority);
        let target_authority_present = match fs::symlink_metadata(&target_authority) {
            Ok(metadata) => {
                !metadata.file_type().is_symlink() && (metadata.is_file() || metadata.is_dir())
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => {
                return Err(config_error(format!(
                    "inspect resumed target authority '{}': {error}",
                    target_authority.display()
                )));
            }
        };
        if matches!(progress.status, AuthorityProgressStatus::Pending)
            || (matches!(progress.status, AuthorityProgressStatus::Copied)
                && !target_authority_present)
        {
            copy_first_party_authority(&backup_view_root.join(authority), &target_authority)?;
            sync_replacement_tree(&target_authority)?;
            let (_, target_entries) = authority_observation(target_root, authority)?;
            let (target_digest, _) = authority_observation(target_root, authority)?;
            checkpoint_authority(
                &journal_path,
                authority,
                AuthorityProgressStatus::Copied,
                target_digest,
                target_entries,
            )?;
        } else {
            let expected_target_digest = progress.target_digest.as_deref().ok_or_else(|| {
                config_error(format!(
                    "replacement journal checkpoint for '{authority}' has no target digest"
                ))
            })?;
            let (observed_target_digest, _) = authority_observation(target_root, authority)?;
            if observed_target_digest != expected_target_digest {
                if matches!(progress.status, AuthorityProgressStatus::Copied) {
                    // A crash can occur after the copy checkpoint and while
                    // the canonical migration registry is rewriting this
                    // authority.  Rebuild from the immutable backup before
                    // retrying the registry so a torn or tampered target is
                    // never interpreted as a resumable V1/V2 boundary.
                    copy_first_party_authority(
                        &backup_view_root.join(authority),
                        &target_authority,
                    )?;
                    sync_replacement_tree(&target_authority)?;
                    let (reset_digest, reset_entries) =
                        authority_observation(target_root, authority)?;
                    checkpoint_authority(
                        &journal_path,
                        authority,
                        AuthorityProgressStatus::Copied,
                        reset_digest,
                        reset_entries,
                    )?;
                } else {
                    return Err(config_error(format!(
                        "replacement authority '{authority}' checkpoint does not match the isolated target; refusing to skip a partial cutover"
                    )));
                }
            }
        }
    }
    copy_first_party_opaque_entries(source_view_root, target_root)?;
    copy_first_party_opaque_sidecars(source_view_root, target_root)?;
    rebind_first_party_store_manifests(target_root, source_root)?;

    run_landed_profile_migrations(target_root, worker, worker_sha256, timeout)?;
    let after_migration_external = external_project_reports(source_view_root)?;
    if !source_external_baseline.is_empty() && after_migration_external != source_external_baseline
    {
        return Err(config_error(
            "first-party migration modified an external source project/workflow/Git root",
        ));
    }
    for authority in REQUIRED_AUTHORITIES {
        let (target_digest, _) = authority_observation(target_root, authority)?;
        checkpoint_authority(
            &journal_path,
            authority,
            AuthorityProgressStatus::Migrated,
            target_digest,
            0,
        )?;
    }
    let manifest = FirstPartyManifest {
        schema_version: FIRST_PARTY_MANIFEST_SCHEMA_VERSION,
        protocol: REPLACEMENT_PROTOCOL.to_owned(),
        protocol_version: REPLACEMENT_PROTOCOL_VERSION,
        operation_id: operation_id.to_owned(),
        provider: provider.to_owned(),
        source_tree_digest: profile_tree_digest(source_view_root)?,
        authorities: authority_manifest(source_view_root, target_root)?,
        opaque_manifest: opaque_manifest(source_view_root, target_root)?,
        external_projects: external_project_manifest(source_view_root, target_root)?,
    };
    write_first_party_manifest(target_root, &manifest)?;
    sync_replacement_tree(target_root)?;
    // The semantic manifest is the independent verification boundary for
    // each authority. Checkpoint it only after the manifest has been durably
    // written so a restart can skip verified authorities while still
    // detecting a target that was torn down between checkpoints.
    for authority in REQUIRED_AUTHORITIES {
        let (target_digest, target_entries) = authority_observation(target_root, authority)?;
        checkpoint_authority(
            &journal_path,
            authority,
            AuthorityProgressStatus::Verified,
            target_digest,
            target_entries,
        )?;
    }
    Ok(WorkerReport {
        protocol: REPLACEMENT_PROTOCOL.to_owned(),
        protocol_version: REPLACEMENT_PROTOCOL_VERSION,
        operation_id: operation_id.to_owned(),
        provider: provider.to_owned(),
        phase: WorkerPhase::Apply,
        committed: true,
        exact: false,
        authorities: REQUIRED_AUTHORITIES
            .iter()
            .map(|authority| (*authority).to_owned())
            .collect(),
        target_digest: profile_tree_digest(target_root)?,
        authority_manifest: authority_manifest(source_view_root, target_root)?,
        opaque_manifest: opaque_manifest(source_view_root, target_root)?,
        external_projects: external_project_manifest(source_view_root, target_root)?,
    })
}

fn first_party_verify(
    target_root: &Path,
    source_root: &Path,
    operation_id: &str,
    provider: &str,
) -> Result<WorkerReport> {
    verify_profile_namespace(target_root, true)?;
    verify_landed_profile_migrations(target_root)?;
    let manifest_path = target_root.join(FIRST_PARTY_MANIFEST_FILENAME);
    let manifest: FirstPartyManifest =
        serde_json::from_slice(&fs::read(&manifest_path).map_err(|error| {
            config_error(format!("read first-party semantic manifest: {error}"))
        })?)
        .map_err(|error| config_error(format!("decode first-party semantic manifest: {error}")))?;
    if manifest.schema_version != FIRST_PARTY_MANIFEST_SCHEMA_VERSION
        || manifest.protocol != REPLACEMENT_PROTOCOL
        || manifest.protocol_version != REPLACEMENT_PROTOCOL_VERSION
        || manifest.operation_id != operation_id
        || manifest.provider != provider
        || manifest.source_tree_digest != profile_tree_digest(source_root)?
    {
        return Err(config_error(
            "first-party semantic manifest does not match the admitted operation",
        ));
    }
    let observed_manifest = authority_manifest(source_root, target_root)?;
    let mut expected_manifest = manifest.authorities;
    expected_manifest.sort_by(|left, right| left.authority.cmp(&right.authority));
    let mut observed_manifest_sorted = observed_manifest.clone();
    observed_manifest_sorted.sort_by(|left, right| left.authority.cmp(&right.authority));
    if expected_manifest != observed_manifest_sorted {
        return Err(config_error(
            "first-party semantic manifest differs from the independently observed authorities",
        ));
    }
    let observed_opaque_manifest = opaque_manifest(source_root, target_root)?;
    if manifest.opaque_manifest != observed_opaque_manifest {
        return Err(config_error(
            "first-party opaque profile manifest differs from independently observed bytes",
        ));
    }
    let observed_external_projects = external_project_manifest(source_root, target_root)?;
    if manifest.external_projects != observed_external_projects {
        return Err(config_error(
            "first-party external project manifest differs from independently observed project roots",
        ));
    }
    Ok(WorkerReport {
        protocol: REPLACEMENT_PROTOCOL.to_owned(),
        protocol_version: REPLACEMENT_PROTOCOL_VERSION,
        operation_id: operation_id.to_owned(),
        provider: provider.to_owned(),
        phase: WorkerPhase::Verify,
        committed: true,
        exact: true,
        authorities: REQUIRED_AUTHORITIES
            .iter()
            .map(|authority| (*authority).to_owned())
            .collect(),
        target_digest: profile_tree_digest(target_root)?,
        authority_manifest: observed_manifest,
        opaque_manifest: observed_opaque_manifest,
        external_projects: observed_external_projects,
    })
}

fn copy_first_party_authority(source: &Path, destination: &Path) -> Result<()> {
    if let Ok(metadata) = fs::symlink_metadata(destination) {
        if metadata.file_type().is_symlink() {
            return Err(config_error(format!(
                "first-party target authority '{}' is a symlink",
                destination.display()
            )));
        }
        // A resumed operation may be replacing an authority copied from the
        // frozen worker view. Make that private tree writable before removing
        // it; otherwise a read-only directory can make rollback/resume fail
        // even though the operation owns every entry below it.
        make_tree_writable(destination)?;
        if metadata.is_dir() {
            fs::remove_dir_all(destination).map_err(|error| {
                config_error(format!(
                    "clear partial first-party target authority '{}': {error}",
                    destination.display()
                ))
            })?;
        } else {
            fs::remove_file(destination).map_err(|error| {
                config_error(format!(
                    "clear partial first-party target authority '{}': {error}",
                    destination.display()
                ))
            })?;
        }
    }
    copy_first_party_entry(source, destination)?;
    // Backup and worker-view files are intentionally frozen read-only. The
    // target is a private mutable migration workspace, so restore writable
    // permissions before SQLite migrations or manifest rebinding open it.
    make_tree_writable(destination)
}

fn copy_first_party_entry(source: &Path, destination: &Path) -> Result<()> {
    if is_sqlite_sidecar_path(source) {
        // SQLite WAL/SHM/journal files are represented by the owning database
        // snapshot in the canonical backup contract. Keep nested opaque roots
        // on the same rule as authority/digest traversal so a sidecar cannot
        // be copied into V2 while being silently omitted from its attestation.
        return Ok(());
    }
    let metadata = fs::symlink_metadata(source).map_err(|error| {
        config_error(format!(
            "inspect first-party source '{}': {error}",
            source.display()
        ))
    })?;
    if metadata.file_type().is_symlink() {
        return Err(config_error(format!(
            "first-party source contains symlink '{}', refusing migration",
            source.display()
        )));
    }
    if metadata.is_dir() {
        create_private_directory(destination).map_err(|error| {
            config_error(format!(
                "create first-party target '{}': {error}",
                destination.display()
            ))
        })?;
        let mut entries = fs::read_dir(source)
            .map_err(|error| {
                config_error(format!(
                    "read first-party source '{}': {error}",
                    source.display()
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| {
                config_error(format!(
                    "enumerate first-party source '{}': {error}",
                    source.display()
                ))
            })?;
        entries.sort_by_key(fs::DirEntry::file_name);
        for entry in entries {
            copy_first_party_entry(&entry.path(), &destination.join(entry.file_name()))?;
        }
        make_tree_writable(destination)?;
    } else if metadata.is_file() {
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                config_error(format!(
                    "create first-party target parent '{}': {error}",
                    parent.display()
                ))
            })?;
        }
        fs::copy(source, destination).map_err(|error| {
            config_error(format!(
                "copy first-party authority '{}' to '{}': {error}",
                source.display(),
                destination.display()
            ))
        })?;
        make_tree_writable(destination)?;
    } else {
        return Err(config_error(format!(
            "first-party source contains unsupported entry '{}'",
            source.display()
        )));
    }
    Ok(())
}

fn copy_first_party_opaque_entries(source: &Path, target: &Path) -> Result<()> {
    let mut entries = fs::read_dir(source)
        .map_err(|error| {
            config_error(format!(
                "read first-party opaque source '{}': {error}",
                source.display()
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            config_error(format!(
                "enumerate first-party opaque source '{}': {error}",
                source.display()
            ))
        })?;
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in entries {
        let name = entry.file_name();
        if is_sqlite_sidecar_path(&entry.path())
            || REQUIRED_AUTHORITIES
                .iter()
                .any(|authority| name == *authority)
            || name == LIFECYCLE_LOCK_FILENAME
            || name == JOURNAL_FILENAME
            || name == PROFILE_REHEARSAL_MARKER_FILENAME
            || name == STAGING_MARKER_FILENAME
            || name == FIRST_PARTY_MANIFEST_FILENAME
        {
            continue;
        }
        let destination = target.join(&name);
        if path_exists(&destination)? {
            // A resumable target may already contain this opaque entry from a
            // completed copy checkpoint. Preserve it only when its bytes are
            // exactly the source bytes; removing an entry here could erase a
            // first-party provider artifact created by a prior migration
            // phase. Any mismatch is an ambiguous conversion and therefore a
            // fail-closed recovery boundary.
            let source_digest = replacement_entry_digest(&entry.path(), Path::new(&name))?;
            let target_digest = replacement_entry_digest(&destination, Path::new(&name))?;
            if source_digest != target_digest {
                return Err(config_error(format!(
                    "opaque replacement entry '{}' differs in the resumable target",
                    destination.display()
                )));
            }
            continue;
        }
        copy_first_party_entry(&entry.path(), &destination)?;
    }
    Ok(())
}

/// Carry through a suffix-named file when it is not actually owned by a
/// SQLite/Grafeo database. The maintenance backup contract folds genuine
/// database sidecars into their owning snapshot, but a future provider is
/// allowed to keep an opaque receipt or payload named `*-wal` (or similar).
/// Walking the required authorities here closes that gap without copying a
/// real SQLite sidecar back into a migrated store.
fn copy_first_party_opaque_sidecars(source: &Path, target: &Path) -> Result<()> {
    fn walk(source: &Path, target: &Path, relative: &Path) -> Result<()> {
        let mut entries = fs::read_dir(source)
            .map_err(|error| {
                config_error(format!(
                    "read first-party sidecar source '{}': {error}",
                    source.display()
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| {
                config_error(format!(
                    "enumerate first-party sidecar source '{}': {error}",
                    source.display()
                ))
            })?;
        entries.sort_by_key(fs::DirEntry::file_name);
        for entry in entries {
            let name = entry.file_name();
            if relative.as_os_str().is_empty()
                && (name == LIFECYCLE_LOCK_FILENAME
                    || name == JOURNAL_FILENAME
                    || name == PROFILE_REHEARSAL_MARKER_FILENAME
                    || name == STAGING_MARKER_FILENAME
                    || name == FIRST_PARTY_MANIFEST_FILENAME)
            {
                continue;
            }
            let source_path = entry.path();
            let destination_path = target.join(relative.join(&name));
            let metadata = fs::symlink_metadata(&source_path).map_err(|error| {
                config_error(format!(
                    "inspect first-party sidecar source '{}': {error}",
                    source_path.display()
                ))
            })?;
            if metadata.file_type().is_symlink() {
                return Err(config_error(format!(
                    "first-party sidecar source contains symlink '{}', refusing migration",
                    source_path.display()
                )));
            }
            if metadata.is_dir() {
                walk(&source_path, target, &relative.join(&name))?;
                continue;
            }
            if !metadata.is_file()
                || !is_sqlite_sidecar_name(&name)
                || is_sqlite_sidecar_path(&source_path)
            {
                continue;
            }
            if path_exists(&destination_path)? {
                let source_digest = replacement_entry_digest(&source_path, Path::new(&name))?;
                let target_digest = replacement_entry_digest(&destination_path, Path::new(&name))?;
                if source_digest != target_digest {
                    return Err(config_error(format!(
                        "opaque sidecar '{}' differs in the resumable target",
                        destination_path.display()
                    )));
                }
            } else {
                copy_first_party_entry(&source_path, &destination_path)?;
            }
        }
        Ok(())
    }

    walk(source, target, Path::new(""))
}

fn rebind_first_party_store_manifests(target_root: &Path, source_root: &Path) -> Result<()> {
    let projects = target_root.join("projects");
    let mut stores = fs::read_dir(&projects)
        .map_err(|error| config_error(format!("read first-party stores for rebinding: {error}")))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            config_error(format!(
                "enumerate first-party stores for rebinding: {error}"
            ))
        })?;
    stores.sort_by_key(fs::DirEntry::file_name);
    for store in stores {
        let metadata = fs::symlink_metadata(store.path()).map_err(|error| {
            config_error(format!("inspect first-party store for rebinding: {error}"))
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(config_error(format!(
                "first-party store '{}' is not a regular directory",
                store.path().display()
            )));
        }
        let manifest_path = store
            .path()
            .join(tracedecay_runtime_core::storage::STORE_MANIFEST_FILENAME);
        let mut manifest = tracedecay_runtime_core::storage::read_store_manifest(&manifest_path)
            .map_err(|error| {
                config_error(format!(
                    "read first-party store manifest '{}': {error}",
                    manifest_path.display()
                ))
            })?;
        if manifest.storage_mode != tracedecay_runtime_core::storage::StorageMode::ProfileSharded {
            return Err(config_error(format!(
                "first-party store manifest '{}' is not profile-sharded",
                manifest_path.display()
            )));
        }
        let project_store_name = store.file_name();
        let project_id = project_store_name
            .to_str()
            .ok_or_else(|| config_error("first-party project store id is not Unicode"))?;
        if paths_overlap(&manifest.project_root, source_root)? {
            return Err(config_error(format!(
                "project root '{}' is inside the source profile; external workflow/Git bytes are not covered by the canonical profile backup",
                manifest.project_root.display()
            )));
        }
        manifest.data_root = target_root.join("projects").join(project_id);
        let bytes = tracedecay_domain::canonical_json_bytes(&manifest).map_err(|error| {
            config_error(format!(
                "encode rebound first-party store manifest '{}': {error}",
                manifest_path.display()
            ))
        })?;
        let temporary = manifest_path.with_extension("json.replacement.tmp");
        tracedecay_runtime_core::storage::PrivateStoreIo::write_file_atomically(
            &manifest_path,
            &temporary,
            &bytes,
        )
        .map_err(|error| {
            config_error(format!(
                "write rebound first-party store manifest '{}': {error}",
                manifest_path.display()
            ))
        })?;
    }
    Ok(())
}

fn write_first_party_manifest(root: &Path, manifest: &FirstPartyManifest) -> Result<()> {
    let path = root.join(FIRST_PARTY_MANIFEST_FILENAME);
    let bytes = tracedecay_domain::canonical_json_bytes(manifest)
        .map_err(|error| config_error(format!("encode first-party semantic manifest: {error}")))?;
    let temporary = root.join(format!(
        ".{FIRST_PARTY_MANIFEST_FILENAME}.{}.tmp",
        manifest.operation_id
    ));
    tracedecay_runtime_core::storage::PrivateStoreIo::write_file_atomically_durable(
        &path, &temporary, &bytes,
    )
    .map_err(|error| config_error(format!("write first-party semantic manifest: {error}")))
}

/// Establish a filesystem durability barrier before an authority checkpoint
/// can claim that its bytes are resumable. SQLite's own transactions protect
/// relational consistency, while this walk protects the copied files and
/// directory entries from being lost after the journal record reaches disk.
fn sync_replacement_tree(root: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(root).map_err(|error| {
        config_error(format!(
            "inspect replacement tree '{}' for durability: {error}",
            root.display()
        ))
    })?;
    if metadata.file_type().is_symlink() {
        return Err(config_error(format!(
            "replacement durability tree contains symlink '{}'",
            root.display()
        )));
    }
    if metadata.is_dir() {
        let mut entries = fs::read_dir(root)
            .map_err(|error| {
                config_error(format!(
                    "read replacement tree '{}' for durability: {error}",
                    root.display()
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| {
                config_error(format!(
                    "enumerate replacement tree '{}' for durability: {error}",
                    root.display()
                ))
            })?;
        entries.sort_by_key(fs::DirEntry::file_name);
        for entry in entries {
            sync_replacement_tree(&entry.path())?;
        }
        sync_directory(root, DirectorySyncPolicy::Strict).map_err(|error| {
            config_error(format!(
                "sync replacement directory '{}' before checkpoint: {error}",
                root.display()
            ))
        })?;
    } else if metadata.is_file() {
        File::open(root)
            .and_then(|file| file.sync_all())
            .map_err(|error| {
                config_error(format!(
                    "sync replacement file '{}' before checkpoint: {error}",
                    root.display()
                ))
            })?;
    } else {
        return Err(config_error(format!(
            "replacement durability tree contains unsupported entry '{}'",
            root.display()
        )));
    }
    Ok(())
}

/// Execute the first-party async migration in the exact pinned binary, but in
/// a separate process. A Tokio timeout cannot stop a future which is inside a
/// blocking filesystem or SQLite call; a process boundary gives the
/// coordinator a hard kill/reap boundary and prevents a timed-out migration
/// from retaining a profile lease after the CLI has returned.
fn run_landed_profile_migrations(
    root: &Path,
    worker: &Path,
    expected_worker_sha256: &str,
    timeout: Duration,
) -> Result<()> {
    ensure_worker_identity(worker, expected_worker_sha256)?;
    let mut command = Command::new(worker);
    #[cfg(unix)]
    unsafe {
        use std::os::unix::process::CommandExt;
        command.pre_exec(|| {
            if libc::setpgid(0, 0) == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command
        .arg(INTERNAL_REPLACEMENT_MIGRATION_ARG)
        .arg(root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| {
            config_error(format!(
                "could not launch first-party migration child from '{}': {error}",
                worker.display()
            ))
        })?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| config_error("first-party migration stderr was not piped"))?;
    let stderr_thread = thread::spawn(move || read_bounded(stderr));
    let deadline = Instant::now() + timeout;
    let child_pid = child.id();
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                // The leader has exited, but a descendant may still own the
                // inherited stderr pipe. Terminate the private process tree
                // before joining the bounded reader. `wait` is retained for
                // platforms where `try_wait` observes exit without fully
                // reaping the handle.
                terminate_worker_tree(child_pid);
                let _ = child.wait();
                break status;
            }
            Ok(None) => {}
            Err(error) => {
                terminate_worker_tree(child_pid);
                let _ = child.kill();
                let _ = child.wait();
                let _ = stderr_thread.join();
                return Err(config_error(format!(
                    "wait for first-party migration child: {error}"
                )));
            }
        }
        if Instant::now() >= deadline {
            terminate_worker_tree(child_pid);
            let _ = child.kill();
            let _ = child.wait();
            let _ = stderr_thread.join();
            return Err(config_error(format!(
                "first-party migration exceeded the {} second timeout and was terminated",
                timeout.as_secs()
            )));
        }
        thread::sleep(WORKER_POLL_INTERVAL);
    };
    let (stderr, stderr_overflowed) = stderr_thread
        .join()
        .map_err(|_| config_error("first-party migration stderr reader panicked"))?;
    if stderr_overflowed {
        return Err(config_error(format!(
            "first-party migration child exceeded the {} byte stderr limit",
            MAX_WORKER_OUTPUT_BYTES
        )));
    }
    if !status.success() {
        return Err(config_error(format!(
            "first-party migration child exited with {}; stderr: {}",
            status,
            bounded_text(&stderr)
        )));
    }
    Ok(())
}

/// Entry point for the private migration argv marker. The parent coordinator
/// supplies the timeout and owns the process group; this child only runs the
/// canonical migration future and exits with a typed diagnostic on failure.
pub(crate) fn run_internal_profile_migration(root: PathBuf) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| config_error(format!("build first-party migration runtime: {error}")))?;
    runtime.block_on(migrate_target_profile_async(&root))
}

async fn migrate_target_profile_async(root: &Path) -> Result<()> {
    // Mount every registered profile authority through the same composition
    // root used by the daemon. Opening the copied files with rusqlite alone
    // would only prove that their bytes are readable; it would skip the
    // canonical global/session/memory migration registries that own V2 schema
    // evolution.
    let lifecycle = tracedecay_runtime_core::lifecycle_lease::acquire_exclusive_for_profile(
        root,
        "first-party V1-to-V2 authority migration",
    )?;
    let database_scope = tracedecay_runtime_core::db::enter_maintenance_database_scope(
        &lifecycle,
        root,
        "first-party V1-to-V2 authority migration",
    )?;
    let identity =
        tracedecay_daemon_identity::profile_identity::load_existing(root).map_err(|error| {
            config_error(format!(
                "load copied profile identity for first-party migration: {error}"
            ))
        })?;
    let registry = tracedecay_store_runtime::join_standalone_session_registry(identity)
        .await
        .map_err(|error| {
            config_error(format!(
                "mount copied profile authorities for first-party migration: {error}"
            ))
        })?;
    let _profile_database = registry.profile_database().await.map_err(|error| {
        config_error(format!(
            "migrate copied global authority through the shipped registry: {error}"
        ))
    })?;
    let _profile_sessions = registry.profile_sessions().await.map_err(|error| {
        config_error(format!(
            "migrate copied session authority through the shipped registry: {error}"
        ))
    })?;
    let _profile_memory = registry.profile_memory().await.map_err(|error| {
        config_error(format!(
            "migrate copied memory authority through the shipped registry: {error}"
        ))
    })?;

    let manifests = project_manifests(root)?;
    for manifest in manifests {
        let project_root = manifest.project_root;
        let metadata = fs::symlink_metadata(&project_root).map_err(|error| {
            config_error(format!(
                "inspect registered project root '{}' for first-party migration: {error}",
                project_root.display()
            ))
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(config_error(format!(
                "registered project root '{}' is not a regular directory",
                project_root.display()
            )));
        }
        let graph = tracedecay::project::TraceDecay::open_with_exclusive_maintenance(
            &project_root,
            tracedecay::project::TraceDecayOpenOptions {
                profile_root: Some(root.to_path_buf()),
                global_db_path: Some(root.join("global.db")),
            },
            &lifecycle,
        )
        .await
        .map_err(|error| {
            config_error(format!(
                "migrate registered project store for '{}' through the shipped runtime: {error}",
                project_root.display()
            ))
        })?;
        graph.ensure_schema_current().await.map_err(|error| {
            config_error(format!(
                "verify migrated project store for '{}' through the shipped runtime: {error}",
                project_root.display()
            ))
        })?;
        drop(graph);
    }
    // Close every mounted authority before releasing the maintenance scope
    // and removing the materialized target lock. Leaving one lease alive
    // until function return can keep a Grafeo/SQLite descriptor open across
    // the cutover checkpoint (and is a hard failure on platforms that refuse
    // unlinking an open lifecycle lock).
    drop(_profile_memory);
    drop(_profile_sessions);
    drop(_profile_database);
    drop(registry);
    drop(database_scope);
    drop(lifecycle);
    remove_materialized_target_lock(root)?;
    Ok(())
}

fn remove_materialized_target_lock(root: &Path) -> Result<()> {
    let lock = root.join(LIFECYCLE_LOCK_FILENAME);
    match fs::remove_file(&lock) {
        Ok(()) => sync_directory(root, DirectorySyncPolicy::Strict).map_err(|error| {
            config_error(format!(
                "sync removal of migrated target lifecycle lock: {error}"
            ))
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(config_error(format!(
            "remove migrated target lifecycle lock '{}': {error}",
            lock.display()
        ))),
    }
}

fn project_manifests(root: &Path) -> Result<Vec<tracedecay_runtime_core::storage::StoreManifest>> {
    let projects = root.join("projects");
    let metadata = fs::symlink_metadata(&projects).map_err(|error| {
        config_error(format!("inspect first-party projects authority: {error}"))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(config_error(
            "first-party projects authority must be a regular directory",
        ));
    }
    project_manifests_from_projects(&projects)
}

fn project_manifests_if_present(
    root: &Path,
) -> Result<Vec<tracedecay_runtime_core::storage::StoreManifest>> {
    let projects = root.join("projects");
    match fs::symlink_metadata(&projects) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => Err(
            config_error("first-party projects authority must be a regular directory"),
        ),
        Ok(_) => project_manifests_from_projects(&projects),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(config_error(format!(
            "inspect first-party projects authority: {error}"
        ))),
    }
}

fn project_manifests_from_projects(
    projects: &Path,
) -> Result<Vec<tracedecay_runtime_core::storage::StoreManifest>> {
    let mut stores = fs::read_dir(projects)
        .map_err(|error| config_error(format!("read first-party project stores: {error}")))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| config_error(format!("enumerate first-party project stores: {error}")))?;
    stores.sort_by_key(fs::DirEntry::file_name);
    let mut manifests = Vec::new();
    for store in stores {
        let metadata = fs::symlink_metadata(store.path())
            .map_err(|error| config_error(format!("inspect first-party project store: {error}")))?;
        if metadata.file_type().is_symlink() {
            return Err(config_error(format!(
                "first-party project store '{}' is a symlink",
                store.path().display()
            )));
        }
        if !metadata.is_dir() {
            continue;
        }
        let manifest_path = store
            .path()
            .join(tracedecay_runtime_core::storage::STORE_MANIFEST_FILENAME);
        manifests.push(
            tracedecay_runtime_core::storage::read_store_manifest(&manifest_path).map_err(
                |error| {
                    config_error(format!(
                        "read first-party project store manifest '{}': {error}",
                        manifest_path.display()
                    ))
                },
            )?,
        );
    }
    Ok(manifests)
}

fn verify_landed_profile_migrations(root: &Path) -> Result<()> {
    for authority in ["global.db", "user-sessions.db", "user-memory.db"] {
        verify_sqlite_integrity(&root.join(authority), authority)?;
    }
    for manifest in project_manifests(root)? {
        let graph_path = manifest.data_root.join(manifest.graph_db_relpath);
        verify_sqlite_integrity(&graph_path, "project graph")?;
        let connection = rusqlite::Connection::open_with_flags(
            &graph_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .map_err(|error| {
            config_error(format!(
                "open migrated project graph '{}': {error}",
                graph_path.display()
            ))
        })?;
        let version: i64 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(|error| config_error(format!("read migrated project schema: {error}")))?;
        if version != i64::from(tracedecay_runtime_core::db::migrations::SCHEMA_VERSION) {
            return Err(config_error(format!(
                "migrated project graph '{}' has schema v{version}, expected v{}",
                graph_path.display(),
                tracedecay_runtime_core::db::migrations::SCHEMA_VERSION
            )));
        }
        tracedecay_runtime_core::db::migrations::verify_admissible_final_shape_rusqlite(
            &connection,
        )
        .map_err(|error| config_error(format!("verify migrated project graph shape: {error}")))?;
    }
    Ok(())
}

fn verify_sqlite_integrity(path: &Path, authority: &str) -> Result<()> {
    let connection =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|error| {
                config_error(format!("open {authority} '{}': {error}", path.display()))
            })?;
    let result: String = connection
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .map_err(|error| {
            config_error(format!("check {authority} '{}': {error}", path.display()))
        })?;
    if result != "ok" {
        return Err(config_error(format!(
            "{authority} '{}' failed SQLite integrity check: {result}",
            path.display()
        )));
    }
    Ok(())
}

fn read_authority_progress(path: &Path, authority: &str) -> Result<AuthorityProgress> {
    let journal = read_journal(path)?;
    journal
        .authority_progress
        .into_iter()
        .find(|progress| progress.authority == authority)
        .ok_or_else(|| {
            config_error(format!(
                "replacement journal has no authority checkpoint for '{authority}'"
            ))
        })
}

fn checkpoint_authority(
    journal_path: &Path,
    authority: &str,
    status: AuthorityProgressStatus,
    target_digest: String,
    _target_entries: u64,
) -> Result<()> {
    let mut journal = read_journal(journal_path)?;
    let checkpoint = journal
        .authority_progress
        .iter_mut()
        .find(|progress| progress.authority == authority)
        .ok_or_else(|| {
            config_error(format!(
                "replacement journal has no authority '{authority}'"
            ))
        })?;
    checkpoint.status = status;
    checkpoint.target_digest = Some(target_digest);
    write_journal(journal_path, &journal)
}

fn verify_source_authority_checkpoints(
    profile_root: &Path,
    journal: &ReplacementJournal,
) -> Result<()> {
    for progress in &journal.authority_progress {
        let (observed_digest, _) = authority_observation(profile_root, &progress.authority)?;
        if observed_digest != progress.source_digest {
            return Err(config_error(format!(
                "source authority '{}' changed after the replacement journal was prepared; refusing to resume",
                progress.authority
            )));
        }
    }
    Ok(())
}

/// Invoke the binary's first-party migration implementation. The coordinator
/// admits only the exact running binary, and the apply phase crosses a
/// killable process boundary so a stuck SQLite/filesystem migration cannot
/// retain the profile lease. An operator-supplied executable can be useful as
/// a diagnostic, but it cannot establish semantic preservation for the
/// profile authorities and is never trusted for cutover.
fn invoke_first_party_worker(
    worker: &Path,
    expected_worker_sha256: &str,
    phase: WorkerPhase,
    operation_id: &str,
    provider: &str,
    source_view_root: &Path,
    backup_view_root: &Path,
    target_root: &Path,
    source_root: &Path,
    expected_source_digest: &str,
    backup_root: &Path,
    expected_backup_digest: &str,
    timeout: Duration,
) -> Result<WorkerReport> {
    ensure_worker_identity(worker, expected_worker_sha256)?;
    ensure_worker_inputs(
        source_view_root,
        backup_view_root,
        source_root,
        expected_source_digest,
        backup_root,
        expected_backup_digest,
    )?;

    let result = match phase {
        WorkerPhase::Preflight => first_party_preflight(
            source_view_root,
            backup_view_root,
            target_root,
            operation_id,
            provider,
        ),
        WorkerPhase::Apply => first_party_apply(
            source_view_root,
            backup_view_root,
            target_root,
            source_root,
            worker,
            expected_worker_sha256,
            operation_id,
            provider,
            timeout,
        ),
        WorkerPhase::Verify => first_party_verify(target_root, source_root, operation_id, provider),
    };
    // The CLI's normal runtime is multi-threaded. Running the bounded
    // migration future on a short-lived current-thread runtime keeps the
    // synchronous cutover helpers and the async registered-store migration
    // at one explicit boundary, including when this module is unit-tested.
    let report = match result {
        Ok(report) => report,
        Err(error) => return Err(error),
    };
    ensure_worker_inputs(
        source_view_root,
        backup_view_root,
        source_root,
        expected_source_digest,
        backup_root,
        expected_backup_digest,
    )?;
    Ok(report)
}

fn invoke_worker(
    worker: &Path,
    expected_worker_sha256: &str,
    phase: WorkerPhase,
    operation_id: &str,
    source_view_root: &Path,
    backup_view_root: &Path,
    target_root: &Path,
    source_root: &Path,
    expected_source_digest: &str,
    backup_root: &Path,
    expected_backup_digest: &str,
    timeout: Duration,
) -> Result<WorkerReport> {
    ensure_worker_identity(worker, expected_worker_sha256)?;
    let output = run_worker_process(
        worker,
        phase,
        operation_id,
        source_view_root,
        backup_view_root,
        target_root,
        timeout,
    );
    // Check both original authorities and both isolated views even when the
    // worker timed out or returned a malformed receipt. The worker runs as an
    // untrusted same-user process; only disposable views are passed to it.
    // A crash is recovered from the untouched external backup.
    let integrity = ensure_worker_inputs(
        source_view_root,
        backup_view_root,
        source_root,
        expected_source_digest,
        backup_root,
        expected_backup_digest,
    );
    integrity?;
    let output = output?;
    if !output.status.success() {
        return Err(config_error(format!(
            "worker phase '{}' exited with {}; stderr: {}",
            phase.as_str(),
            output.status,
            bounded_text(&output.stderr),
        )));
    }
    if output.output_overflowed {
        return Err(config_error(format!(
            "worker phase '{}' exceeded the {} byte output limit",
            phase.as_str(),
            MAX_WORKER_OUTPUT_BYTES
        )));
    }
    let report: WorkerReport = serde_json::from_slice(&output.stdout).map_err(|error| {
        config_error(format!(
            "worker phase '{}' returned invalid JSON: {error}; stdout: {}",
            phase.as_str(),
            bounded_text(&output.stdout),
        ))
    })?;
    Ok(report)
}

fn run_worker_process(
    worker: &Path,
    phase: WorkerPhase,
    operation_id: &str,
    profile_root: &Path,
    backup_root: &Path,
    target_root: &Path,
    timeout: Duration,
) -> Result<WorkerOutput> {
    let protocol_version = REPLACEMENT_PROTOCOL_VERSION.to_string();
    let mut command = Command::new(worker);
    #[cfg(unix)]
    // Put the worker and every descendant it launches in a private process
    // group. A phase timeout must not leave a converter holding the profile or
    // either pipe open after the CLI has returned.
    unsafe {
        use std::os::unix::process::CommandExt;
        command.pre_exec(|| {
            if libc::setpgid(0, 0) == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command
        .arg("--protocol")
        .arg(REPLACEMENT_PROTOCOL)
        .arg("--protocol-version")
        .arg(protocol_version)
        .arg("--phase")
        .arg(phase.as_str())
        .arg("--operation-id")
        .arg(operation_id)
        .arg("--source-profile")
        .arg(profile_root)
        .arg("--backup-root")
        .arg(backup_root)
        .arg("--target-profile")
        .arg(target_root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| config_error(format!("could not launch migration worker: {error}")))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| config_error("migration worker stdout was not piped"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| config_error("migration worker stderr was not piped"))?;
    let stdout_thread = thread::spawn(move || read_bounded(stdout));
    let stderr_thread = thread::spawn(move || read_bounded(stderr));

    let deadline = Instant::now() + timeout;
    let child_pid = child.id();
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                // A successful worker may leave descendants holding either
                // output pipe open. Kill the private process group while the
                // leader PID is still tied to this Child handle, then reap it
                // before joining the reader threads.
                terminate_worker_tree(child_pid);
                let _ = child.wait();
                break status;
            }
            Ok(None) => {}
            Err(error) => {
                terminate_worker_tree(child_pid);
                let _ = child.kill();
                let _ = child.wait();
                let _ = stdout_thread.join();
                let _ = stderr_thread.join();
                return Err(config_error(format!("wait for migration worker: {error}")));
            }
        }
        if Instant::now() >= deadline {
            terminate_worker_tree(child_pid);
            let _ = child.kill();
            let _ = child.wait();
            let _ = stdout_thread.join();
            let _ = stderr_thread.join();
            return Err(config_error(format!(
                "worker phase '{}' exceeded the {} second timeout",
                phase.as_str(),
                timeout.as_secs()
            )));
        }
        thread::sleep(WORKER_POLL_INTERVAL);
    };
    let (stdout, stdout_overflowed) = stdout_thread
        .join()
        .map_err(|_| config_error("migration worker stdout reader panicked"))?;
    let (stderr, stderr_overflowed) = stderr_thread
        .join()
        .map_err(|_| config_error("migration worker stderr reader panicked"))?;
    Ok(WorkerOutput {
        status,
        stdout,
        stderr,
        output_overflowed: stdout_overflowed || stderr_overflowed,
    })
}

#[cfg(unix)]
fn terminate_worker_tree(pid: u32) {
    if pid == 0 {
        return;
    }
    // SAFETY: `kill` takes a process-group id and signal; a negative positive
    // child pid addresses only the group created by `setpgid` above.
    unsafe {
        let _ = libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
    }
}

#[cfg(windows)]
fn terminate_worker_tree(pid: u32) {
    let _ = Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

#[cfg(not(any(unix, windows)))]
fn terminate_worker_tree(_pid: u32) {}

fn read_bounded(mut reader: impl Read) -> (Vec<u8>, bool) {
    let mut output = Vec::new();
    let mut overflowed = false;
    let mut buffer = [0_u8; 8192];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                if output.len() < MAX_WORKER_OUTPUT_BYTES {
                    let keep = (MAX_WORKER_OUTPUT_BYTES - output.len()).min(read);
                    output.extend_from_slice(&buffer[..keep]);
                    overflowed |= keep != read;
                } else {
                    overflowed = true;
                }
            }
            Err(_) => {
                overflowed = true;
                break;
            }
        }
    }
    (output, overflowed)
}

/// Hash a profile or backup namespace in deterministic path order. Reserved
/// lifecycle entries are excluded because the cutover journal, lock, and
/// rehearsal marker are owned by the lifecycle machinery and legitimately
/// differ as a namespace changes phase.
/// The worker receipt must carry this same digest, but the CLI always computes
/// it independently from the bytes on disk.
fn profile_tree_digest(root: &Path) -> Result<String> {
    let metadata = fs::symlink_metadata(root).map_err(|error| {
        config_error(format!(
            "inspect replacement namespace '{}' for digest: {error}",
            root.display()
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(config_error(format!(
            "replacement namespace '{}' must be a regular directory",
            root.display()
        )));
    }
    let mut digest = Sha256::new();
    digest.update(b"tracedecay-v1-to-v2-tree-v1\0");
    digest_profile_tree(root, Path::new(""), &mut digest)?;
    Ok(hex::encode(digest.finalize()))
}

fn staging_marker_path(root: &Path) -> PathBuf {
    root.join(STAGING_MARKER_FILENAME)
}

fn bind_staging_directory(
    root: &Path,
    operation_id: &str,
    kind: &str,
    expected_digest: Option<&str>,
    read_only: bool,
) -> Result<()> {
    let metadata = fs::symlink_metadata(root).map_err(|error| {
        config_error(format!(
            "inspect replacement {kind} staging '{}': {error}",
            root.display()
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(config_error(format!(
            "replacement {kind} staging '{}' must be a regular directory",
            root.display()
        )));
    }
    // Frozen worker views are made writable only for this ownership update;
    // all source/backup bytes are frozen again before they are handed to the
    // worker.
    make_tree_writable(root)?;
    let marker = ReplacementStagingMarker {
        schema_version: STAGING_MARKER_SCHEMA_VERSION,
        operation_id: operation_id.to_owned(),
        kind: kind.to_owned(),
        expected_digest: expected_digest.map(str::to_owned),
    };
    write_staging_marker(&staging_marker_path(root), &marker)?;
    if let Some(expected) = expected_digest {
        ensure_tree_digest(root, expected, &format!("{kind} staging"))?;
    }
    if read_only {
        make_tree_read_only(root)?;
    }
    Ok(())
}

fn validate_staging_directory(
    root: &Path,
    operation_id: &str,
    kind: &str,
    expected_digest: Option<&str>,
) -> Result<()> {
    let metadata = fs::symlink_metadata(root).map_err(|error| {
        config_error(format!(
            "inspect replacement {kind} staging '{}': {error}",
            root.display()
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(config_error(format!(
            "replacement {kind} staging '{}' must be a regular directory",
            root.display()
        )));
    }
    let marker_path = staging_marker_path(root);
    let marker: ReplacementStagingMarker =
        serde_json::from_slice(&fs::read(&marker_path).map_err(|error| {
            config_error(format!(
                "read replacement {kind} staging ownership marker '{}': {error}",
                marker_path.display()
            ))
        })?)
        .map_err(|error| {
            config_error(format!(
                "decode replacement {kind} staging ownership marker '{}': {error}",
                marker_path.display()
            ))
        })?;
    if marker.schema_version != STAGING_MARKER_SCHEMA_VERSION
        || marker.operation_id != operation_id
        || marker.kind != kind
    {
        return Err(config_error(format!(
            "replacement {kind} staging '{}' belongs to another operation",
            root.display()
        )));
    }
    if let Some(digest) = &marker.expected_digest {
        if !is_sha256_digest(digest) {
            return Err(config_error(format!(
                "replacement {kind} staging '{}' has an invalid content digest",
                root.display()
            )));
        }
        ensure_tree_digest(root, digest, &format!("{kind} staging"))?;
    }
    if let Some(expected) = expected_digest {
        if marker.expected_digest.as_deref() != Some(expected) {
            return Err(config_error(format!(
                "replacement {kind} staging '{}' has a foreign content binding",
                root.display()
            )));
        }
        ensure_tree_digest(root, expected, &format!("{kind} staging"))?;
    }
    Ok(())
}

/// Complete a marker that was durably written before a staging tree's bytes
/// were finished. The early marker is needed so an interrupted copy can be
/// classified as operation owned; once the tree is readable, persist its
/// content digest before recovery adopts or removes it. This also upgrades
/// journals written by an older coordinator that recorded only the operation
/// identity for an empty target.
fn complete_staging_digest_binding(
    root: &Path,
    operation_id: &str,
    kind: &str,
    expected_digest: Option<&str>,
    read_only: bool,
) -> Result<()> {
    let marker_path = staging_marker_path(root);
    let marker: ReplacementStagingMarker =
        serde_json::from_slice(&fs::read(&marker_path).map_err(|error| {
            config_error(format!(
                "read replacement {kind} staging ownership marker '{}': {error}",
                marker_path.display()
            ))
        })?)
        .map_err(|error| {
            config_error(format!(
                "decode replacement {kind} staging ownership marker '{}': {error}",
                marker_path.display()
            ))
        })?;
    if marker.expected_digest.is_some() {
        return Ok(());
    }
    let observed_digest = profile_tree_digest(root)?;
    if expected_digest.is_some_and(|expected| expected != observed_digest) {
        return Err(config_error(format!(
            "replacement {kind} staging '{}' does not match its expected content digest",
            root.display()
        )));
    }
    bind_staging_directory(root, operation_id, kind, Some(&observed_digest), read_only)
}

fn validate_staging_marker_identity(root: &Path, operation_id: &str, kind: &str) -> Result<()> {
    let marker_path = staging_marker_path(root);
    let marker: ReplacementStagingMarker =
        serde_json::from_slice(&fs::read(&marker_path).map_err(|error| {
            config_error(format!(
                "read replacement {kind} staging ownership marker '{}': {error}",
                marker_path.display()
            ))
        })?)
        .map_err(|error| {
            config_error(format!(
                "decode replacement {kind} staging ownership marker '{}': {error}",
                marker_path.display()
            ))
        })?;
    if marker.schema_version != STAGING_MARKER_SCHEMA_VERSION
        || marker.operation_id != operation_id
        || marker.kind != kind
    {
        return Err(config_error(format!(
            "replacement {kind} staging '{}' belongs to another operation",
            root.display()
        )));
    }
    if let Some(digest) = marker.expected_digest.as_deref()
        && !is_sha256_digest(digest)
    {
        return Err(config_error(format!(
            "replacement {kind} staging '{}' has an invalid content digest",
            root.display()
        )));
    }
    Ok(())
}

fn remove_staging_marker_for_operation(root: &Path, operation_id: &str) -> Result<()> {
    let marker_path = staging_marker_path(root);
    match fs::symlink_metadata(&marker_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(config_error(format!(
            "inspect replacement staging marker '{}': {error}",
            marker_path.display()
        ))),
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err(config_error(format!(
                "replacement staging marker '{}' must be a regular file",
                marker_path.display()
            )))
        }
        Ok(_) => {
            let bytes = fs::read(&marker_path).map_err(|error| {
                config_error(format!(
                    "read replacement staging marker '{}': {error}",
                    marker_path.display()
                ))
            })?;
            let marker: ReplacementStagingMarker =
                serde_json::from_slice(&bytes).map_err(|error| {
                    config_error(format!(
                        "decode replacement staging marker '{}': {error}",
                        marker_path.display()
                    ))
                })?;
            if marker.schema_version != STAGING_MARKER_SCHEMA_VERSION
                || marker.operation_id != operation_id
            {
                return Err(config_error(format!(
                    "replacement staging marker '{}' belongs to another operation",
                    marker_path.display()
                )));
            }
            remove_staging_marker(root)
        }
    }
}

fn write_staging_marker(path: &Path, marker: &ReplacementStagingMarker) -> Result<()> {
    let bytes = tracedecay_domain::canonical_json_bytes(marker)
        .map_err(|error| config_error(format!("encode replacement staging marker: {error}")))?;
    let previous = match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(config_error(format!(
                "replacement staging marker '{}' must be a regular file",
                path.display()
            )));
        }
        Ok(_) => Some(fs::read(path).map_err(|error| {
            config_error(format!(
                "read previous replacement staging marker '{}': {error}",
                path.display()
            ))
        })?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(config_error(format!(
                "inspect replacement staging marker '{}': {error}",
                path.display()
            )));
        }
    };
    let expectation = if previous.is_some() {
        tracedecay_private_fs::framed_log::ConditionalPublishExpectation::Present
    } else {
        tracedecay_private_fs::framed_log::ConditionalPublishExpectation::Missing
    };
    let previous_for_verify = previous;
    let bytes_for_verify = bytes.clone();
    tracedecay_private_fs::framed_log::atomic_write_prepared_conditionally(
        path,
        "replacement-staging-marker",
        &bytes,
        expectation,
        tracedecay_private_fs::framed_log::ConditionalPublishCallbacks {
            prepare: |temporary: &Path| {
                tracedecay_private_fs::framed_log::tighten_existing_file(temporary)
            },
            before_publish: || {},
            after_publish: || {},
            verify_displaced: move |displaced: &Path| {
                let Some(expected) = previous_for_verify.as_deref() else {
                    return Ok(false);
                };
                Ok(fs::read(displaced).ok().as_deref() == Some(expected))
            },
            verify_published: move |published: &Path| {
                Ok(fs::read(published).ok().as_deref() == Some(bytes_for_verify.as_slice()))
            },
        },
        DirectorySyncPolicy::Strict,
    )
    .map_err(|error| config_error(format!("publish replacement staging marker: {error}")))
}

fn remove_staging_marker(root: &Path) -> Result<()> {
    let marker = staging_marker_path(root);
    match fs::remove_file(&marker) {
        Ok(()) => sync_directory(root, DirectorySyncPolicy::Strict).map_err(|error| {
            config_error(format!(
                "sync removal of replacement staging marker '{}': {error}",
                marker.display()
            ))
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(config_error(format!(
            "remove replacement staging marker '{}': {error}",
            marker.display()
        ))),
    }
}

fn is_sqlite_sidecar_name(name: &std::ffi::OsStr) -> bool {
    let name = name.to_string_lossy();
    // Match the complete-backup snapshot contract's suffix vocabulary. The
    // path-aware helper below additionally proves that the corresponding
    // database exists before folding the file out of an opaque inventory.
    ["-wal", "-shm", "-journal"]
        .iter()
        .any(|suffix| name.ends_with(suffix))
        || name.ends_with(".grafeo.wal")
}

fn is_sqlite_sidecar_path(path: &Path) -> bool {
    let Some(name) = path.file_name() else {
        return false;
    };
    if !is_sqlite_sidecar_name(name) {
        return false;
    }
    let name = name.to_string_lossy();
    let base_name = if let Some(base) = name.strip_suffix(".grafeo.wal") {
        format!("{base}.grafeo")
    } else if let Some(base) = name
        .strip_suffix("-wal")
        .or_else(|| name.strip_suffix("-shm"))
        .or_else(|| name.strip_suffix("-journal"))
    {
        base.to_owned()
    } else {
        return false;
    };
    let Some(parent) = path.parent() else {
        return false;
    };
    let base = parent.join(base_name);
    let Ok(metadata) = fs::symlink_metadata(&base) else {
        return false;
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return false;
    }
    if base
        .extension()
        .is_some_and(|extension| extension == "grafeo")
    {
        return true;
    }
    // An opaque payload may legitimately end in `-wal`, `-shm`, or
    // `-journal` while its similarly named owner is a non-SQLite file. Only
    // fold a sidecar when the owner is a Grafeo store or independently proves
    // the SQLite header; extension alone is not sufficient for preservation.
    tracedecay_runtime_core::storage::has_sqlite_database_header(&base).unwrap_or(false)
}

fn digest_profile_tree(root: &Path, relative: &Path, digest: &mut Sha256) -> Result<()> {
    let mut entries = fs::read_dir(root)
        .map_err(|error| config_error(format!("read '{}' for digest: {error}", root.display())))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            config_error(format!(
                "enumerate '{}' for digest: {error}",
                root.display()
            ))
        })?;
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in entries {
        let name = entry.file_name();
        if is_sqlite_sidecar_path(&entry.path()) {
            // SQLite WAL/SHM/journal files are folded into the canonical
            // database snapshot by the complete-backup contract.
            continue;
        }
        if relative.as_os_str().is_empty()
            && (name == LIFECYCLE_LOCK_FILENAME
                || name == JOURNAL_FILENAME
                || name == PROFILE_REHEARSAL_MARKER_FILENAME
                || name == STAGING_MARKER_FILENAME
                || name == FIRST_PARTY_MANIFEST_FILENAME)
        {
            continue;
        }
        let path = entry.path();
        let child_relative = relative.join(&name);
        let relative_text = child_relative.to_string_lossy().replace('\\', "/");
        let metadata = path.symlink_metadata().map_err(|error| {
            config_error(format!("inspect '{}' for digest: {error}", path.display()))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(config_error(format!(
                "replacement namespace contains symlink '{}'",
                path.display()
            )));
        }
        digest.update(relative_text.as_bytes());
        digest.update([0]);
        if metadata.is_dir() {
            digest.update(b"D\0");
            digest_profile_tree(&path, &child_relative, digest)?;
        } else if metadata.is_file() {
            digest.update(b"F\0");
            let mut file = File::open(&path).map_err(|error| {
                config_error(format!("open '{}' for digest: {error}", path.display()))
            })?;
            let mut buffer = [0_u8; 64 * 1024];
            loop {
                let read = file.read(&mut buffer).map_err(|error| {
                    config_error(format!("read '{}' for digest: {error}", path.display()))
                })?;
                if read == 0 {
                    break;
                }
                digest.update(&buffer[..read]);
            }
            digest.update([0]);
        } else {
            return Err(config_error(format!(
                "replacement namespace contains unsupported entry '{}'",
                path.display()
            )));
        }
    }
    Ok(())
}

/// Give the worker private snapshots of both read-only inputs. The live V1
/// profile and verified external backup are never passed to an untrusted
/// executable, so a worker cannot alter the authorities that recovery relies
/// on. The snapshots are still digest-checked after every phase.
fn copy_worker_view(source: &Path, destination: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(source).map_err(|error| {
        config_error(format!(
            "inspect replacement worker input '{}' for copying: {error}",
            source.display()
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(config_error(format!(
            "replacement worker input '{}' must be a regular directory",
            source.display()
        )));
    }
    create_private_directory(destination).map_err(|error| {
        config_error(format!(
            "create isolated worker input '{}': {error}",
            destination.display()
        ))
    })?;
    copy_worker_view_entries(source, destination, Path::new(""))?;
    make_tree_read_only(destination)
}

fn copy_worker_view_entries(source: &Path, destination: &Path, relative: &Path) -> Result<()> {
    let mut entries = fs::read_dir(source)
        .map_err(|error| {
            config_error(format!(
                "read '{}' for worker view: {error}",
                source.display()
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            config_error(format!(
                "enumerate '{}' for worker view: {error}",
                source.display()
            ))
        })?;
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in entries {
        let name = entry.file_name();
        if is_sqlite_sidecar_path(&entry.path()) {
            continue;
        }
        if relative.as_os_str().is_empty()
            && (name == LIFECYCLE_LOCK_FILENAME
                || name == JOURNAL_FILENAME
                || name == PROFILE_REHEARSAL_MARKER_FILENAME
                || name == STAGING_MARKER_FILENAME
                || name == FIRST_PARTY_MANIFEST_FILENAME)
        {
            continue;
        }
        let source_path = entry.path();
        let destination_path = destination.join(&name);
        let metadata = source_path.symlink_metadata().map_err(|error| {
            config_error(format!(
                "inspect worker view source '{}': {error}",
                source_path.display()
            ))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(config_error(format!(
                "worker view source contains symlink '{}', refusing to copy it",
                source_path.display()
            )));
        }
        if metadata.is_dir() {
            create_private_directory(&destination_path).map_err(|error| {
                config_error(format!(
                    "create worker view directory '{}': {error}",
                    destination_path.display()
                ))
            })?;
            copy_worker_view_entries(&source_path, &destination_path, &relative.join(&name))?;
        } else if metadata.is_file() {
            fs::copy(&source_path, &destination_path).map_err(|error| {
                config_error(format!(
                    "copy worker view file '{}' to '{}': {error}",
                    source_path.display(),
                    destination_path.display()
                ))
            })?;
        } else {
            return Err(config_error(format!(
                "worker view source contains unsupported entry '{}', refusing to copy it",
                source_path.display()
            )));
        }
    }
    Ok(())
}

/// Freeze disposable worker inputs after copying them. The worker is allowed
/// to inspect both snapshots, but it must not be able to alter the source or
/// backup bytes that recovery and the independent manifest verifier rely on.
/// The cleanup path explicitly restores write permission before removing a
/// frozen tree.
fn make_tree_read_only(root: &Path) -> Result<()> {
    set_tree_permissions(root, true)
}

fn make_tree_writable(root: &Path) -> Result<()> {
    set_tree_permissions(root, false)
}

fn set_tree_permissions(root: &Path, read_only: bool) -> Result<()> {
    let metadata = fs::symlink_metadata(root).map_err(|error| {
        config_error(format!(
            "inspect replacement permission tree '{}': {error}",
            root.display()
        ))
    })?;
    if metadata.file_type().is_symlink() {
        return Err(config_error(format!(
            "replacement permission tree contains symlink '{}'",
            root.display()
        )));
    }
    if metadata.is_dir() {
        let mut entries = fs::read_dir(root)
            .map_err(|error| {
                config_error(format!(
                    "read replacement permission tree '{}': {error}",
                    root.display()
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| {
                config_error(format!(
                    "enumerate replacement permission tree '{}': {error}",
                    root.display()
                ))
            })?;
        entries.sort_by_key(fs::DirEntry::file_name);
        for entry in entries {
            set_tree_permissions(&entry.path(), read_only)?;
        }
    } else if !metadata.is_file() {
        return Err(config_error(format!(
            "replacement permission tree contains unsupported entry '{}'",
            root.display()
        )));
    }
    let mut permissions = metadata.permissions();
    #[cfg(unix)]
    permissions.set_mode(if metadata.is_dir() {
        if read_only { 0o500 } else { 0o700 }
    } else if read_only {
        0o400
    } else {
        0o600
    });
    #[cfg(not(unix))]
    permissions.set_readonly(read_only);
    fs::set_permissions(root, permissions).map_err(|error| {
        config_error(format!(
            "set replacement permission tree '{}' writable={}: {error}",
            root.display(),
            !read_only
        ))
    })
}

fn ensure_worker_inputs(
    source_view_root: &Path,
    backup_view_root: &Path,
    source_root: &Path,
    expected_source_digest: &str,
    backup_root: &Path,
    expected_backup_digest: &str,
) -> Result<()> {
    ensure_input_integrity(
        source_root,
        expected_source_digest,
        backup_root,
        expected_backup_digest,
    )?;
    ensure_tree_digest(
        source_view_root,
        expected_source_digest,
        "isolated source view",
    )?;
    ensure_tree_digest(
        backup_view_root,
        expected_backup_digest,
        "isolated backup view",
    )
}

fn ensure_tree_digest(root: &Path, expected: &str, label: &str) -> Result<()> {
    let observed = profile_tree_digest(root)?;
    if observed != expected {
        return Err(config_error(format!(
            "migration worker modified its {label} '{}' (expected digest {}, observed {})",
            root.display(),
            expected,
            observed
        )));
    }
    Ok(())
}

fn ensure_input_integrity(
    source_root: &Path,
    expected_source_digest: &str,
    backup_root: &Path,
    expected_backup_digest: &str,
) -> Result<()> {
    let observed_source_digest = profile_tree_digest(source_root)?;
    if observed_source_digest != expected_source_digest {
        return Err(config_error(format!(
            "migration worker modified its source namespace '{}' (expected digest {}, observed {})",
            source_root.display(),
            expected_source_digest,
            observed_source_digest
        )));
    }
    let observed_backup_digest = profile_tree_digest(backup_root)?;
    if observed_backup_digest != expected_backup_digest {
        return Err(config_error(format!(
            "migration worker modified the external backup '{}' (expected digest {}, observed {})",
            backup_root.display(),
            expected_backup_digest,
            observed_backup_digest
        )));
    }
    tracedecay_maintenance::profile_backup::load_and_verify_backup(backup_root).map_err(
        |error| {
            config_error(format!(
                "external backup '{}' failed verification after worker phase: {error}",
                backup_root.display()
            ))
        },
    )?;
    Ok(())
}

fn opaque_manifest(source_root: &Path, target_root: &Path) -> Result<OpaqueManifest> {
    let (source_digest, source_entries) = opaque_observation(source_root)?;
    let (target_digest, target_entries) = opaque_observation(target_root)?;
    Ok(OpaqueManifest {
        source_digest,
        source_entries,
        target_digest,
        target_entries,
    })
}

fn opaque_observation(root: &Path) -> Result<(String, u64)> {
    let mut digest = Sha256::new();
    digest.update(b"tracedecay-v1-to-v2-opaque-v1\0");
    let mut entries = fs::read_dir(root)
        .map_err(|error| {
            config_error(format!(
                "read replacement opaque profile entries '{}': {error}",
                root.display()
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            config_error(format!(
                "enumerate replacement opaque profile entries '{}': {error}",
                root.display()
            ))
        })?;
    entries.sort_by_key(fs::DirEntry::file_name);
    let mut count = 0_u64;
    for entry in entries {
        let name = entry.file_name();
        if is_sqlite_sidecar_path(&entry.path()) {
            continue;
        }
        if REQUIRED_AUTHORITIES
            .iter()
            .any(|authority| name == *authority)
            || name == LIFECYCLE_LOCK_FILENAME
            || name == JOURNAL_FILENAME
            || name == PROFILE_REHEARSAL_MARKER_FILENAME
            || name == STAGING_MARKER_FILENAME
            || name == FIRST_PARTY_MANIFEST_FILENAME
        {
            continue;
        }
        let child = entry.path();
        let child_count = digest_authority_entry(&child, Path::new(&name), &mut digest)?;
        count = count.saturating_add(child_count);
    }
    Ok((hex::encode(digest.finalize()), count))
}

fn external_project_manifest(
    source_root: &Path,
    target_root: &Path,
) -> Result<ExternalProjectManifest> {
    let source = external_project_reports(source_root)?;
    let target = external_project_reports(target_root)?;
    Ok(ExternalProjectManifest { source, target })
}

fn external_project_reports(root: &Path) -> Result<Vec<ExternalProjectReport>> {
    let manifests = project_manifests_if_present(root)?;
    let mut reports = Vec::with_capacity(manifests.len());
    for manifest in manifests {
        let project_id = manifest
            .project_id
            .clone()
            .ok_or_else(|| config_error("project store manifest has no project id"))?;
        if paths_overlap(&manifest.project_root, root)? {
            return Err(config_error(format!(
                "project root '{}' is inside the profile; the canonical profile backup does not cover its external workflow/Git bytes",
                manifest.project_root.display()
            )));
        }
        let metadata = fs::symlink_metadata(&manifest.project_root).map_err(|error| {
            config_error(format!(
                "inspect external project root '{}' for replacement inventory: {error}",
                manifest.project_root.display()
            ))
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(config_error(format!(
                "external project root '{}' must be a regular directory",
                manifest.project_root.display()
            )));
        }
        let (digest, entries) = external_project_observation(&manifest.project_root)?;
        reports.push(ExternalProjectReport {
            project_id,
            project_root: manifest.project_root,
            digest,
            entries,
        });
    }
    reports.sort_by(|left, right| left.project_id.cmp(&right.project_id));
    Ok(reports)
}

fn external_project_observation(root: &Path) -> Result<(String, u64)> {
    let mut digest = Sha256::new();
    digest.update(b"tracedecay-v1-to-v2-external-project-v1\0");
    let entries = digest_project_entry(root, Path::new(""), &mut digest)?;
    Ok((hex::encode(digest.finalize()), entries))
}

fn digest_project_entry(path: &Path, relative: &Path, digest: &mut Sha256) -> Result<u64> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        config_error(format!(
            "inspect external project entry '{}' for replacement inventory: {error}",
            path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() {
        return Err(config_error(format!(
            "external project root contains symlink '{}', refusing replacement",
            path.display()
        )));
    }
    if metadata.is_dir() {
        digest.update(relative.to_string_lossy().replace('\\', "/").as_bytes());
        digest.update(b"\0D\0");
        let mut entries = fs::read_dir(path)
            .map_err(|error| {
                config_error(format!(
                    "read external project directory '{}' for replacement inventory: {error}",
                    path.display()
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| {
                config_error(format!(
                    "enumerate external project directory '{}' for replacement inventory: {error}",
                    path.display()
                ))
            })?;
        entries.sort_by_key(fs::DirEntry::file_name);
        let mut count = 0_u64;
        for entry in entries {
            count = count.saturating_add(digest_project_entry(
                &entry.path(),
                &relative.join(entry.file_name()),
                digest,
            )?);
        }
        Ok(count)
    } else if metadata.is_file() {
        digest.update(relative.to_string_lossy().replace('\\', "/").as_bytes());
        digest.update(b"\0F\0");
        let mut file = File::open(path).map_err(|error| {
            config_error(format!(
                "open external project entry '{}' for replacement inventory: {error}",
                path.display()
            ))
        })?;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer).map_err(|error| {
                config_error(format!(
                    "read external project entry '{}' for replacement inventory: {error}",
                    path.display()
                ))
            })?;
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
        }
        digest.update(b"\0");
        Ok(1)
    } else {
        Err(config_error(format!(
            "external project root contains unsupported entry '{}', refusing replacement",
            path.display()
        )))
    }
}

fn authority_manifest(
    source_root: &Path,
    target_root: &Path,
) -> Result<Vec<WorkerAuthorityReport>> {
    REQUIRED_AUTHORITIES
        .iter()
        .map(|authority| {
            let (source_digest, source_entries) = authority_observation(source_root, authority)?;
            let (target_digest, target_entries) = authority_observation(target_root, authority)?;
            let (source_semantic_digest, source_rows, source_tables) =
                authority_semantic_observation(source_root, authority, &source_digest)?;
            let (target_semantic_digest, target_rows, target_tables) =
                authority_semantic_observation(target_root, authority, &target_digest)?;
            let semantic_mapping = semantic_table_mapping(&source_tables, &target_tables);
            let source_status_counts = aggregate_status_counts(&source_tables);
            let target_status_counts = aggregate_status_counts(&target_tables);
            let dispositions =
                semantic_dispositions(&semantic_mapping, &source_tables, &target_tables);
            let source_row_digest = aggregate_row_digest(&source_tables);
            let target_row_digest = aggregate_row_digest(&target_tables);
            let disposition = aggregate_disposition(&dispositions);
            let mapping_digest = authority_mapping_digest(
                authority,
                &source_semantic_digest,
                &target_semantic_digest,
                source_rows,
                target_rows,
                &semantic_mapping,
                &dispositions,
                &source_status_counts,
                &target_status_counts,
            );
            Ok(WorkerAuthorityReport {
                authority: (*authority).to_owned(),
                source_digest,
                source_entries,
                target_digest,
                target_entries,
                source_semantic_digest,
                target_semantic_digest,
                source_rows,
                target_rows,
                source_tables,
                target_tables,
                semantic_mapping,
                mapping_digest,
                dispositions,
                source_status_counts,
                target_status_counts,
                source_row_digest,
                target_row_digest,
                disposition,
            })
        })
        .collect()
}

fn aggregate_row_digest(tables: &[SemanticTableReport]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"tracedecay-v1-to-v2-authority-rows-v1\0");
    for table in tables {
        digest.update(table.table.as_bytes());
        digest.update(b"\0");
        digest.update(table.rows.to_le_bytes());
        digest.update(table.row_digest.as_bytes());
        digest.update(b"\0");
    }
    hex::encode(digest.finalize())
}

fn aggregate_disposition(dispositions: &[SemanticDisposition]) -> String {
    if dispositions
        .iter()
        .all(|disposition| disposition.disposition == "preserved")
    {
        return "preserved".to_owned();
    }
    if dispositions
        .iter()
        .any(|disposition| disposition.disposition == "retained_in_backup")
    {
        return "retained_in_backup".to_owned();
    }
    if dispositions
        .iter()
        .any(|disposition| disposition.disposition == "rebuilt_projection")
    {
        return "rebuilt_projection".to_owned();
    }
    "transformed".to_owned()
}

fn authority_semantic_observation(
    root: &Path,
    authority: &str,
    artifact_digest: &str,
) -> Result<(String, u64, Vec<SemanticTableReport>)> {
    let path = root.join(authority);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut digest = Sha256::new();
            digest.update(b"tracedecay-v1-to-v2-semantic-v1\0");
            digest.update(authority.as_bytes());
            digest.update(b"\0missing\0");
            return Ok((hex::encode(digest.finalize()), 0, Vec::new()));
        }
        Err(error) => {
            return Err(config_error(format!(
                "inspect replacement authority '{}' for semantic manifest: {error}",
                path.display()
            )));
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(config_error(format!(
            "replacement authority '{}' is a symlink",
            path.display()
        )));
    }
    let mut digest = Sha256::new();
    digest.update(b"tracedecay-v1-to-v2-semantic-v1\0");
    digest.update(authority.as_bytes());
    digest.update(b"\0");
    let mut sqlite_files = Vec::new();
    if metadata.is_file() {
        if is_sqlite_database_file(&path)? {
            sqlite_files.push((path.clone(), PathBuf::new()));
        }
    } else {
        collect_sqlite_database_files(&path, Path::new(""), &mut sqlite_files)?;
    }
    if !sqlite_files.is_empty() {
        let mut rows = 0_u64;
        let mut tables = Vec::new();
        for (sqlite_path, relative) in sqlite_files {
            let table_prefix = relative.to_string_lossy().replace('\\', "/");
            let (file_rows, file_tables) = semantic_sqlite_tables(&sqlite_path, &table_prefix)?;
            rows = rows.saturating_add(file_rows);
            for table in file_tables {
                digest.update(table.table.as_bytes());
                digest.update(b"\0");
                digest.update(table.row_digest.as_bytes());
                digest.update(b"\0");
                digest.update(table.rows.to_le_bytes());
                tables.push(table);
            }
        }
        return Ok((hex::encode(digest.finalize()), rows, tables));
    } else {
        let (_, entries) = authority_observation(root, authority)?;
        let rows = entries;
        let mut tables = Vec::new();
        tables.push(SemanticTableReport {
            table: "<opaque-artifact>".to_owned(),
            columns: Vec::new(),
            rows: entries,
            row_digest: artifact_digest.to_owned(),
            status_counts: BTreeMap::new(),
        });
        digest.update(rows.to_le_bytes());
        return Ok((hex::encode(digest.finalize()), rows, tables));
    }
}

fn is_sqlite_database_file(path: &Path) -> Result<bool> {
    if is_sqlite_sidecar_path(path) {
        return Ok(false);
    }
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        config_error(format!(
            "inspect SQLite authority candidate '{}' for semantic manifest: {error}",
            path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Ok(false);
    }
    // A filename is not an authority claim. In particular, provider-owned
    // opaque payloads can use a `.db` suffix without containing SQLite.
    // Require the on-disk SQLite header before opening a candidate for the
    // semantic census; unknown bytes are handled by the opaque inventory.
    tracedecay_runtime_core::storage::has_sqlite_database_header(path).map_err(|error| {
        config_error(format!(
            "inspect SQLite authority candidate '{}' for semantic manifest: {error}",
            path.display()
        ))
    })
}

fn collect_sqlite_database_files(
    root: &Path,
    relative: &Path,
    files: &mut Vec<(PathBuf, PathBuf)>,
) -> Result<()> {
    let mut entries = fs::read_dir(root)
        .map_err(|error| {
            config_error(format!(
                "read '{}' for semantic SQLite files: {error}",
                root.display()
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            config_error(format!(
                "enumerate '{}' for semantic SQLite files: {error}",
                root.display()
            ))
        })?;
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        let child_relative = relative.join(entry.file_name());
        let metadata = fs::symlink_metadata(&path).map_err(|error| {
            config_error(format!(
                "inspect '{}' for semantic SQLite files: {error}",
                path.display()
            ))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(config_error(format!(
                "replacement authority contains symlink '{}'",
                path.display()
            )));
        }
        if metadata.is_dir() {
            collect_sqlite_database_files(&path, &child_relative, files)?;
        } else if metadata.is_file() && is_sqlite_database_file(&path)? {
            files.push((path, child_relative));
        }
    }
    Ok(())
}

fn semantic_sqlite_tables(
    path: &Path,
    table_prefix: &str,
) -> Result<(u64, Vec<SemanticTableReport>)> {
    let connection =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|error| {
                config_error(format!(
                    "open replacement authority '{}' for semantic manifest: {error}",
                    path.display()
                ))
            })?;
    let mut table_query = connection
        .prepare(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )
        .map_err(|error| config_error(format!("read semantic SQLite tables: {error}")))?;
    let table_names = table_query
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| config_error(format!("enumerate semantic SQLite tables: {error}")))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| config_error(format!("decode semantic SQLite table: {error}")))?;
    drop(table_query);
    let mut rows = 0_u64;
    let mut tables = Vec::with_capacity(table_names.len());
    for table in table_names {
        let table_key = if table_prefix.is_empty() {
            table.clone()
        } else {
            format!("{table_prefix}:{table}")
        };
        let quoted = table.replace('"', "\"\"");
        let mut statement = connection
            .prepare(&format!("SELECT * FROM \"{quoted}\""))
            .map_err(|error| {
                config_error(format!(
                    "read semantic SQLite columns for table '{table_key}': {error}"
                ))
            })?;
        let columns = statement
            .column_names()
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let column_count = statement.column_count();
        let mut row_digests = Vec::new();
        let mut status_counts = BTreeMap::new();
        let mut result = statement.query([]).map_err(|error| {
            config_error(format!(
                "read semantic SQLite rows for table '{table_key}': {error}"
            ))
        })?;
        while let Some(row) = result.next().map_err(|error| {
            config_error(format!(
                "decode semantic SQLite row for table '{table_key}': {error}"
            ))
        })? {
            let mut row_digest = Sha256::new();
            row_digest.update(b"tracedecay-v1-to-v2-row-v1\0");
            row_digest.update(table_key.as_bytes());
            row_digest.update(b"\0");
            for index in 0..column_count {
                row_digest.update(columns[index].as_bytes());
                row_digest.update(b"\0");
                let value = row.get_ref(index).map_err(|error| {
                    config_error(format!(
                        "read semantic SQLite value in table '{table_key}': {error}"
                    ))
                })?;
                digest_sqlite_value(&mut row_digest, value);
                if let Some(status) = semantic_status_value(&columns[index], value) {
                    *status_counts.entry(status).or_insert(0) += 1;
                }
            }
            row_digests.push(hex::encode(row_digest.finalize()));
        }
        row_digests.sort_unstable();
        let count = u64::try_from(row_digests.len()).map_err(|_| {
            config_error(format!(
                "semantic SQLite table '{table_key}' has too many rows"
            ))
        })?;
        let mut table_digest = Sha256::new();
        table_digest.update(b"tracedecay-v1-to-v2-table-rows-v1\0");
        table_digest.update(table_key.as_bytes());
        table_digest.update(b"\0");
        for row_digest in &row_digests {
            table_digest.update(row_digest.as_bytes());
            table_digest.update(b"\0");
        }
        let row_digest = hex::encode(table_digest.finalize());
        rows = rows.saturating_add(count);
        tables.push(SemanticTableReport {
            table: table_key,
            columns,
            rows: count,
            row_digest,
            status_counts,
        });
    }
    Ok((rows, tables))
}

fn digest_sqlite_value(digest: &mut Sha256, value: rusqlite::types::ValueRef<'_>) {
    match value {
        rusqlite::types::ValueRef::Null => digest.update(b"N\0"),
        rusqlite::types::ValueRef::Integer(value) => {
            digest.update(b"I\0");
            digest.update(value.to_le_bytes());
        }
        rusqlite::types::ValueRef::Real(value) => {
            digest.update(b"R\0");
            digest.update(value.to_bits().to_le_bytes());
        }
        rusqlite::types::ValueRef::Text(value) => {
            digest.update(b"T\0");
            digest.update(value.len().to_le_bytes());
            digest.update(value);
        }
        rusqlite::types::ValueRef::Blob(value) => {
            digest.update(b"B\0");
            digest.update(value.len().to_le_bytes());
            digest.update(value);
        }
    }
    digest.update(b"\0");
}

fn semantic_status_value(column: &str, value: rusqlite::types::ValueRef<'_>) -> Option<String> {
    let column = column.to_ascii_lowercase();
    let status_column = column.contains("status")
        || column.contains("state")
        || column.contains("disposition")
        || column.contains("tombstone")
        || column.contains("evict");
    if !status_column {
        return None;
    }
    let text = match value {
        rusqlite::types::ValueRef::Text(value) => {
            String::from_utf8_lossy(value).to_ascii_lowercase()
        }
        rusqlite::types::ValueRef::Integer(value) => value.to_string(),
        rusqlite::types::ValueRef::Real(value) => value.to_string(),
        rusqlite::types::ValueRef::Null | rusqlite::types::ValueRef::Blob(_) => return None,
    };
    let status = if text.contains("tombstone") {
        "tombstone"
    } else if text.contains("revoke") || text.contains("deleted") {
        "revoked"
    } else if text.contains("evict") {
        "evicted"
    } else if text.contains("duplicate") {
        "duplicate"
    } else if text.contains("live") || text.contains("valid") || text.contains("active") {
        "live"
    } else {
        "other"
    };
    Some(status.to_owned())
}

fn aggregate_status_counts(tables: &[SemanticTableReport]) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for table in tables {
        for (status, count) in &table.status_counts {
            *counts.entry(status.clone()).or_insert(0) += count;
        }
    }
    counts
}

fn semantic_dispositions(
    mapping: &[SemanticTableMapping],
    source_tables: &[SemanticTableReport],
    target_tables: &[SemanticTableReport],
) -> Vec<SemanticDisposition> {
    mapping
        .iter()
        .map(|mapping| {
            let disposition = match (&mapping.source_table, &mapping.target_table) {
                (Some(source), Some(target)) if source == target => {
                    if mapping.source_row_digest == mapping.target_row_digest {
                        "preserved"
                    } else {
                        "transformed"
                    }
                }
                (Some(_), Some(_)) => "transformed",
                (Some(source), None) if is_rebuildable_projection(source) => "rebuilt_projection",
                (Some(_), None) => "retained_in_backup",
                (None, Some(target)) if is_rebuildable_projection(target) => "rebuilt_projection",
                (None, Some(_)) => "transformed",
                (None, None) => "retained_in_backup",
            };
            SemanticDisposition {
                source_table: mapping.source_table.clone(),
                target_table: mapping.target_table.clone(),
                source_rows: mapping.source_rows,
                target_rows: mapping.target_rows,
                source_row_digest: mapping.source_row_digest.clone(),
                target_row_digest: mapping.target_row_digest.clone(),
                disposition: disposition.to_owned(),
            }
        })
        .filter(|disposition| {
            // Keep the arguments part of the independent verification API:
            // a mapping may only describe tables observed on its respective
            // side. This also guards future callers from constructing a
            // disposition for an unobserved table.
            disposition.source_table.as_ref().is_none_or(|table| {
                source_tables
                    .iter()
                    .any(|candidate| candidate.table == *table)
            }) && disposition.target_table.as_ref().is_none_or(|table| {
                target_tables
                    .iter()
                    .any(|candidate| candidate.table == *table)
            })
        })
        .collect()
}

fn is_rebuildable_projection(table: &str) -> bool {
    let table = table.to_ascii_lowercase();
    table.starts_with("idx_")
        || table.starts_with("sqlite_autoindex_")
        || table.contains("_fts")
        || table.contains("projection")
        || table.contains("diagnostic")
        || table.contains("cache")
}

fn semantic_table_mapping(
    source_tables: &[SemanticTableReport],
    target_tables: &[SemanticTableReport],
) -> Vec<SemanticTableMapping> {
    let mut mapping = Vec::with_capacity(source_tables.len() + target_tables.len());
    let mut matched_targets = vec![false; target_tables.len()];
    for source in source_tables {
        if let Some((index, target)) = target_tables
            .iter()
            .enumerate()
            .find(|(index, target)| !matched_targets[*index] && target.table == source.table)
        {
            matched_targets[index] = true;
            mapping.push(SemanticTableMapping {
                source_table: Some(source.table.clone()),
                target_table: Some(target.table.clone()),
                source_rows: source.rows,
                target_rows: target.rows,
                source_row_digest: source.row_digest.clone(),
                target_row_digest: target.row_digest.clone(),
            });
        } else {
            mapping.push(SemanticTableMapping {
                source_table: Some(source.table.clone()),
                target_table: None,
                source_rows: source.rows,
                target_rows: 0,
                source_row_digest: source.row_digest.clone(),
                target_row_digest: String::new(),
            });
        }
    }
    for (index, target) in target_tables.iter().enumerate() {
        if !matched_targets[index] {
            mapping.push(SemanticTableMapping {
                source_table: None,
                target_table: Some(target.table.clone()),
                source_rows: 0,
                target_rows: target.rows,
                source_row_digest: String::new(),
                target_row_digest: target.row_digest.clone(),
            });
        }
    }
    mapping.sort_by(|left, right| {
        left.source_table
            .cmp(&right.source_table)
            .then_with(|| left.target_table.cmp(&right.target_table))
    });
    mapping
}

fn authority_mapping_digest(
    authority: &str,
    source_semantic_digest: &str,
    target_semantic_digest: &str,
    source_rows: u64,
    target_rows: u64,
    semantic_mapping: &[SemanticTableMapping],
    dispositions: &[SemanticDisposition],
    source_status_counts: &BTreeMap<String, u64>,
    target_status_counts: &BTreeMap<String, u64>,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"tracedecay-v1-to-v2-mapping-v1\0");
    digest.update(authority.as_bytes());
    digest.update(b"\0");
    digest.update(source_semantic_digest.as_bytes());
    digest.update(b"\0");
    digest.update(target_semantic_digest.as_bytes());
    digest.update(b"\0");
    digest.update(source_rows.to_le_bytes());
    digest.update(target_rows.to_le_bytes());
    for mapping in semantic_mapping {
        if let Some(table) = &mapping.source_table {
            digest.update(b"S\0");
            digest.update(table.as_bytes());
        } else {
            digest.update(b"S\0<missing>");
        }
        digest.update(b"\0");
        if let Some(table) = &mapping.target_table {
            digest.update(b"T\0");
            digest.update(table.as_bytes());
        } else {
            digest.update(b"T\0<missing>");
        }
        digest.update(b"\0");
        digest.update(mapping.source_rows.to_le_bytes());
        digest.update(mapping.target_rows.to_le_bytes());
        digest.update(mapping.source_row_digest.as_bytes());
        digest.update(b"\0");
        digest.update(mapping.target_row_digest.as_bytes());
        digest.update(b"\0");
    }
    for disposition in dispositions {
        digest.update(
            disposition
                .source_table
                .as_deref()
                .unwrap_or("<missing>")
                .as_bytes(),
        );
        digest.update(b"\0");
        digest.update(
            disposition
                .target_table
                .as_deref()
                .unwrap_or("<missing>")
                .as_bytes(),
        );
        digest.update(b"\0");
        digest.update(disposition.disposition.as_bytes());
        digest.update(b"\0");
    }
    for (status, count) in source_status_counts {
        digest.update(b"source-status\0");
        digest.update(status.as_bytes());
        digest.update(count.to_le_bytes());
    }
    for (status, count) in target_status_counts {
        digest.update(b"target-status\0");
        digest.update(status.as_bytes());
        digest.update(count.to_le_bytes());
    }
    hex::encode(digest.finalize())
}

fn authority_observation(root: &Path, authority: &str) -> Result<(String, u64)> {
    let path = root.join(authority);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut digest = Sha256::new();
            digest.update(b"tracedecay-v1-to-v2-authority-v1\0");
            digest.update(authority.as_bytes());
            digest.update(b"\0M\0");
            return Ok((hex::encode(digest.finalize()), 0));
        }
        Err(error) => {
            return Err(config_error(format!(
                "inspect replacement authority '{}' for authority manifest: {error}",
                path.display()
            )));
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(config_error(format!(
            "replacement authority '{}' is a symlink",
            path.display()
        )));
    }
    if !metadata.is_file() && !metadata.is_dir() {
        return Err(config_error(format!(
            "replacement authority '{}' is not a regular file or directory",
            path.display()
        )));
    }
    let mut digest = Sha256::new();
    digest.update(b"tracedecay-v1-to-v2-authority-v1\0");
    digest.update(authority.as_bytes());
    digest.update(b"\0");
    let entries = digest_authority_entry(&path, Path::new(""), &mut digest)?;
    Ok((hex::encode(digest.finalize()), entries))
}

fn replacement_entry_digest(path: &Path, relative: &Path) -> Result<String> {
    let mut digest = Sha256::new();
    digest.update(b"tracedecay-v1-to-v2-entry-v1\0");
    let _ = digest_authority_entry(path, relative, &mut digest)?;
    Ok(hex::encode(digest.finalize()))
}

fn digest_authority_entry(path: &Path, relative: &Path, digest: &mut Sha256) -> Result<u64> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        config_error(format!(
            "inspect '{}' for authority manifest: {error}",
            path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() {
        return Err(config_error(format!(
            "replacement authority contains symlink '{}'",
            path.display()
        )));
    }
    if metadata.is_dir() {
        digest.update(relative.to_string_lossy().replace('\\', "/").as_bytes());
        digest.update(b"\0D\0");
        let mut entries = fs::read_dir(path)
            .map_err(|error| {
                config_error(format!(
                    "read '{}' for authority manifest: {error}",
                    path.display()
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| {
                config_error(format!(
                    "enumerate '{}' for authority manifest: {error}",
                    path.display()
                ))
            })?;
        entries.sort_by_key(fs::DirEntry::file_name);
        let mut count = 0;
        for entry in entries {
            let child = entry.file_name();
            if is_sqlite_sidecar_path(&entry.path()) {
                continue;
            }
            let child_relative = relative.join(&child);
            count += digest_authority_entry(&entry.path(), &child_relative, digest)?;
        }
        Ok(count)
    } else if metadata.is_file() {
        digest.update(relative.to_string_lossy().replace('\\', "/").as_bytes());
        digest.update(b"\0F\0");
        let mut file = File::open(path).map_err(|error| {
            config_error(format!(
                "open '{}' for authority manifest: {error}",
                path.display()
            ))
        })?;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer).map_err(|error| {
                config_error(format!(
                    "read '{}' for authority manifest: {error}",
                    path.display()
                ))
            })?;
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
        }
        digest.update(b"\0");
        Ok(1)
    } else {
        Err(config_error(format!(
            "replacement authority contains unsupported entry '{}'",
            path.display()
        )))
    }
}

fn validate_worker_report(
    report: WorkerReport,
    expected_phase: WorkerPhase,
    operation_id: &str,
    expected_provider: &str,
    source_root: &Path,
    target_root: &Path,
) -> Result<WorkerReport> {
    if report.protocol != REPLACEMENT_PROTOCOL {
        return Err(config_error(format!(
            "worker reported unsupported protocol '{}' (expected '{}')",
            report.protocol, REPLACEMENT_PROTOCOL
        )));
    }
    if report.protocol_version != REPLACEMENT_PROTOCOL_VERSION {
        return Err(config_error(format!(
            "worker reported protocol version {}, expected {}",
            report.protocol_version, REPLACEMENT_PROTOCOL_VERSION
        )));
    }
    if report.operation_id != operation_id {
        return Err(config_error(format!(
            "worker receipt belongs to operation '{}' instead of '{}'",
            report.operation_id, operation_id
        )));
    }
    if report.provider != expected_provider {
        return Err(config_error(format!(
            "worker receipt selected provider '{}' instead of '{}', refusing to switch serving implementations during replacement",
            report.provider, expected_provider
        )));
    }
    if report.phase != expected_phase {
        return Err(config_error(format!(
            "worker receipt phase '{}' does not match requested '{}'",
            report.phase.as_str(),
            expected_phase.as_str()
        )));
    }
    let mut authorities = report.authorities.clone();
    authorities.sort_unstable();
    let mut expected = REQUIRED_AUTHORITIES.to_vec();
    expected.sort_unstable();
    if authorities != expected {
        return Err(config_error(format!(
            "worker {} receipt did not account for every profile authority (got {:?}, expected {:?})",
            expected_phase.as_str(),
            report.authorities,
            REQUIRED_AUTHORITIES
        )));
    }
    let observed_target_digest = profile_tree_digest(target_root)?;
    if report.target_digest != observed_target_digest {
        return Err(config_error(format!(
            "worker {} receipt target digest {} does not match the independently observed target digest {}",
            expected_phase.as_str(),
            report.target_digest,
            observed_target_digest
        )));
    }
    let mut expected_authority_manifest = authority_manifest(source_root, target_root)?;
    expected_authority_manifest.sort_by(|left, right| left.authority.cmp(&right.authority));
    let mut observed_authority_manifest = report.authority_manifest.clone();
    observed_authority_manifest.sort_by(|left, right| left.authority.cmp(&right.authority));
    if observed_authority_manifest != expected_authority_manifest {
        return Err(config_error(format!(
            "worker {} receipt authority manifest does not match independently observed source/target bindings",
            expected_phase.as_str()
        )));
    }
    let expected_opaque_manifest = opaque_manifest(source_root, target_root)?;
    if report.opaque_manifest != expected_opaque_manifest {
        return Err(config_error(format!(
            "worker {} receipt opaque profile manifest does not match independently observed bytes",
            expected_phase.as_str()
        )));
    }
    let expected_external_projects = external_project_manifest(source_root, target_root)?;
    if report.external_projects != expected_external_projects {
        return Err(config_error(format!(
            "worker {} receipt external project manifest does not match independently observed project roots",
            expected_phase.as_str()
        )));
    }
    match expected_phase {
        WorkerPhase::Preflight if report.committed || report.exact => {
            return Err(config_error(
                "worker preflight claimed a commit or exact V2 verification",
            ));
        }
        WorkerPhase::Apply if !report.committed || report.exact => {
            return Err(config_error(
                "worker apply did not report only a committed isolated target",
            ));
        }
        WorkerPhase::Verify if !report.committed || !report.exact => {
            return Err(config_error(
                "worker verify did not prove a committed exact V2 target",
            ));
        }
        _ => {}
    }
    Ok(report)
}

fn publish_target(
    profile_root: &Path,
    parent: &Path,
    target_root: &Path,
    quarantine_root: &Path,
    journal_path: &Path,
    journal: &mut ReplacementJournal,
) -> Result<()> {
    if path_exists(quarantine_root)? {
        return Err(config_error(format!(
            "V1 quarantine '{}' already exists; refusing to adopt it",
            quarantine_root.display()
        )));
    }
    verify_profile_namespace(target_root, true)?;
    let staged_target_digest = journal.staged_target_digest.as_deref().ok_or_else(|| {
        config_error("replacement journal has no staged target digest before publication")
    })?;
    validate_staging_directory(
        target_root,
        &journal.operation_id,
        "target",
        Some(staged_target_digest),
    )?;
    ensure_no_reserved_cutover_entries(target_root)?;
    // The lifecycle lease is held on `<profile>/lifecycle.lock`. A hard link
    // keeps that same locked inode reachable after the directory exchange, so
    // another process cannot acquire a fresh lock in the published tree.
    link_lifecycle_lock(profile_root, target_root)?;
    // Keep the recovery record reachable at the selected profile path across
    // the exchange. The subsequent atomic journal writes may replace this
    // hard link, but a crash before that write still leaves the Prepared
    // record discoverable.
    link_reserved_entry(profile_root, target_root, JOURNAL_FILENAME)?;
    exchange_profile_roots(target_root, profile_root)?;
    sync_directory(parent, DirectorySyncPolicy::Strict)
        .map_err(|error| config_error(format!("sync atomic profile exchange: {error}")))?;
    journal.phase = ReplacementPhase::SourceQuarantined;
    write_journal(journal_path, journal)?;
    // The exchange made the source tree the operation-owned target path. A
    // no-replace rename gives recovery one deterministic boundary and never
    // overwrites a pre-existing sibling.
    rename_noreplace_path(target_root, quarantine_root)
        .map_err(|error| config_error(format!("quarantine the exchanged V1 profile: {error}")))?;
    // The journal hard link was needed only to keep recovery discoverable
    // across the exchange. Remove that operation metadata from the retained
    // V1 namespace before exposing it as the lossless downgrade root; the
    // serving V2 path still owns the live journal inode.
    remove_reserved_cutover_entry(quarantine_root, JOURNAL_FILENAME, &journal.operation_id)?;
    journal.phase = ReplacementPhase::Published;
    write_journal(journal_path, journal)?;
    sync_directory(parent, DirectorySyncPolicy::Strict)
        .map_err(|error| config_error(format!("sync profile publication: {error}")))?;
    sync_directory(profile_root, DirectorySyncPolicy::Strict)
        .map_err(|error| config_error(format!("sync published V2 profile: {error}")))
}

fn rollback_from_rehearsal(
    profile_root: &Path,
    rehearsal_root: &Path,
    rollback_root: &Path,
    journal_path: &Path,
    journal: &mut ReplacementJournal,
) -> Result<()> {
    ensure_no_rehearsal_marker(profile_root)?;
    let profile_exists = path_exists(profile_root)?;
    let rehearsal_exists = path_exists(rehearsal_root)?;
    let rollback_exists = path_exists(rollback_root)?;
    if rollback_exists {
        validate_owned_directory_reference(rollback_root, "failed V2 quarantine")?;
    }

    // Recovery is idempotent across every rename boundary. If the first move
    // already retained V2, only the rehearsal still needs to be published.
    if !profile_exists {
        if !rehearsal_exists {
            return Err(config_error(
                "rollback has neither a live profile nor a backup rehearsal",
            ));
        }
        if has_first_party_manifest(rehearsal_root)? {
            return Err(config_error(
                "rollback rehearsal is V2 after the profile was removed; refusing to relabel it as V1",
            ));
        }
        rename_noreplace_path(rehearsal_root, profile_root)
            .map_err(|error| config_error(format!("restore the untouched V1 backup: {error}")))?;
    } else if rehearsal_exists {
        let profile_is_v2 = has_first_party_manifest(profile_root)?;
        let rehearsal_is_v2 = has_first_party_manifest(rehearsal_root)?;
        if rehearsal_is_v2 && !profile_is_v2 {
            // A crash occurred after the atomic rollback exchange but before
            // the V2 side was renamed to its quarantine path.
            if !rollback_exists {
                rename_noreplace_path(rehearsal_root, rollback_root).map_err(|error| {
                    config_error(format!(
                        "retain failed V2 profile after rollback exchange: {error}"
                    ))
                })?;
                remove_reserved_cutover_entry(
                    rollback_root,
                    JOURNAL_FILENAME,
                    &journal.operation_id,
                )?;
            }
        } else if profile_is_v2 && !rehearsal_is_v2 {
            // Normal rollback: exchange V2 and the untouched V1 rehearsal,
            // then retain the exchanged V2 directory without overwriting a
            // prior quarantine.
            if rollback_exists {
                return Err(config_error(format!(
                    "rollback quarantine '{}' already exists before exchange",
                    rollback_root.display()
                )));
            }
            link_lifecycle_lock(profile_root, rehearsal_root)?;
            link_reserved_entry(profile_root, rehearsal_root, JOURNAL_FILENAME)?;
            exchange_profile_roots(profile_root, rehearsal_root)?;
            sync_directory(
                profile_root
                    .parent()
                    .ok_or_else(|| config_error("profile root has no parent during rollback"))?,
                DirectorySyncPolicy::Strict,
            )
            .map_err(|error| {
                config_error(format!("sync atomic profile rollback exchange: {error}"))
            })?;
            rename_noreplace_path(rehearsal_root, rollback_root)
                .map_err(|error| config_error(format!("retain failed V2 profile: {error}")))?;
            remove_reserved_cutover_entry(rollback_root, JOURNAL_FILENAME, &journal.operation_id)?;
        }
    } else if has_first_party_manifest(profile_root)? {
        // A V2 profile without either a V1 rehearsal or a retained failed-V2
        // quarantine is not a completed rollback. Marking this state as
        // RolledBack would discard the only recovery boundary and allow the
        // caller to delete the journal while V2 is still serving.
        return Err(config_error(
            "rollback has a V2 profile but no untouched V1 rehearsal",
        ));
    }
    if has_first_party_manifest(profile_root)? {
        return Err(config_error(
            "rollback did not restore a V1 serving profile; the published V2 manifest is still present",
        ));
    }
    verify_profile_namespace(profile_root, true)?;
    // A rehearsal is materialized under a disposable sibling path, so its
    // project store manifests point at that sibling until the directory is
    // published. Rebind them after every rollback boundary before accepting
    // the tree as the serving V1 profile. The source digest then proves the
    // complete restored namespace (including opaque roots) is the one that
    // was admitted before cutover.
    rebind_first_party_store_manifests(profile_root, profile_root)?;
    // The rehearsal ownership marker follows the V1 directory through an
    // atomic exchange. It is valid while the directory is disposable, but a
    // terminal rollback must leave the restored serving tree free of staging
    // metadata. The marker was validated above through the journal-owned
    // rehearsal path before this removal.
    remove_staging_marker_for_operation(profile_root, &journal.operation_id)?;
    // A maintenance rehearsal is restored through SQLite/Grafeo snapshot
    // APIs, so its database bytes are allowed to differ from the source file
    // bytes even when every logical row and opaque byte is preserved.  Verify
    // the serving namespace against the immutable source view (or the
    // quarantined source tree) with the same semantic census used by worker
    // receipts.  A raw tree check remains the fallback for the narrow
    // pre-publication error path where no source snapshot survived.
    let source_reference = if path_exists(&journal.source_view_root)? {
        Some(journal.source_view_root.as_path())
    } else if path_exists(&journal.quarantine_root)? {
        Some(journal.quarantine_root.as_path())
    } else {
        None
    };
    if let Some(source_reference) = source_reference {
        ensure_restored_profile_equivalent(source_reference, profile_root)?;
    } else {
        ensure_tree_digest(
            profile_root,
            &journal.source_tree_digest,
            "rolled-back serving profile",
        )?;
    }
    journal.phase = ReplacementPhase::RolledBack;
    write_journal(journal_path, journal)?;
    sync_directory(
        profile_root
            .parent()
            .ok_or_else(|| config_error("profile root has no parent during rollback"))?,
        DirectorySyncPolicy::Strict,
    )
    .map_err(|error| config_error(format!("sync profile rollback: {error}")))?;
    Ok(())
}

/// Verify that a restored serving profile retains the complete source
/// namespace.  SQLite/Grafeo snapshots can legitimately have different file
/// bytes after a backup/restore, so relational authorities are compared by
/// their table/row/value census while opaque authorities remain byte exact.
/// Store manifests are compared by logical identity and external checkout
/// provenance; only their profile-relative data-root path is expected to be
/// rebound to the serving root.
fn ensure_restored_profile_equivalent(source_root: &Path, restored_root: &Path) -> Result<()> {
    verify_profile_namespace(restored_root, true)?;
    for authority in REQUIRED_AUTHORITIES {
        if authority_has_sqlite_bytes(&source_root.join(authority))? {
            let (source_semantic, source_rows, source_tables) = authority_semantic_observation(
                source_root,
                authority,
                &authority_observation(source_root, authority)?.0,
            )?;
            let (restored_semantic, restored_rows, restored_tables) =
                authority_semantic_observation(
                    restored_root,
                    authority,
                    &authority_observation(restored_root, authority)?.0,
                )?;
            if source_semantic != restored_semantic
                || source_rows != restored_rows
                || source_tables != restored_tables
            {
                return Err(config_error(format!(
                    "rolled-back serving authority '{authority}' differs from its source semantic census"
                )));
            }
            if *authority == "projects" {
                ensure_project_manifest_equivalence(source_root, restored_root)?;
            }
        } else {
            let source_observation = authority_observation(source_root, authority)?;
            let restored_observation = authority_observation(restored_root, authority)?;
            if source_observation != restored_observation {
                return Err(config_error(format!(
                    "rolled-back serving authority '{authority}' differs from its source bytes"
                )));
            }
        }
    }
    let source_opaque = opaque_manifest(source_root, source_root)?;
    let restored_opaque = opaque_manifest(restored_root, restored_root)?;
    if source_opaque != restored_opaque {
        return Err(config_error(
            "rolled-back serving profile differs from source opaque/provider bytes",
        ));
    }
    let source_projects = external_project_manifest(source_root, source_root)?;
    let restored_projects = external_project_manifest(restored_root, restored_root)?;
    if source_projects != restored_projects {
        return Err(config_error(
            "rolled-back serving profile differs from source workflow/Git project inventory",
        ));
    }
    Ok(())
}

fn authority_has_sqlite_bytes(path: &Path) -> Result<bool> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        config_error(format!(
            "inspect restored authority '{}' for semantic verification: {error}",
            path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() {
        return Err(config_error(format!(
            "restored authority '{}' is a symlink",
            path.display()
        )));
    }
    if metadata.is_file() {
        return tracedecay_runtime_core::storage::has_sqlite_database_header(path).map_err(
            |error| {
                config_error(format!(
                    "inspect restored authority '{}' for SQLite semantic verification: {error}",
                    path.display()
                ))
            },
        );
    }
    if !metadata.is_dir() {
        return Ok(false);
    }
    let mut entries = fs::read_dir(path)
        .map_err(|error| {
            config_error(format!(
                "read restored authority '{}' for semantic verification: {error}",
                path.display()
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            config_error(format!(
                "enumerate restored authority '{}' for semantic verification: {error}",
                path.display()
            ))
        })?;
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in entries {
        if is_sqlite_sidecar_path(&entry.path()) {
            continue;
        }
        if authority_has_sqlite_bytes(&entry.path())? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn ensure_project_manifest_equivalence(source_root: &Path, restored_root: &Path) -> Result<()> {
    let mut source_manifests = project_manifests_if_present(source_root)?;
    let mut restored_manifests = project_manifests_if_present(restored_root)?;
    source_manifests.sort_by(|left, right| left.project_id.cmp(&right.project_id));
    restored_manifests.sort_by(|left, right| left.project_id.cmp(&right.project_id));
    if source_manifests.len() != restored_manifests.len() {
        return Err(config_error(
            "rolled-back serving project store count differs from source",
        ));
    }
    for (source, restored) in source_manifests.iter().zip(restored_manifests.iter()) {
        if source.schema_version != restored.schema_version
            || source.project_id != restored.project_id
            || source.store_kind != restored.store_kind
            || source.storage_mode != restored.storage_mode
            || source.graph_db_relpath != restored.graph_db_relpath
            || source.sessions_db_relpath != restored.sessions_db_relpath
            || source.branch_meta_relpath != restored.branch_meta_relpath
            || !physical_paths_equal(&source.project_root, &restored.project_root)?
            || !is_profile_sharded_data_root(&source.data_root, source.project_id.as_deref())
            || !is_profile_sharded_data_root(&restored.data_root, restored.project_id.as_deref())
        {
            return Err(config_error(format!(
                "rolled-back project store manifest for {:?} differs from source identity",
                source.project_id
            )));
        }
    }
    Ok(())
}

fn is_profile_sharded_data_root(path: &Path, project_id: Option<&str>) -> bool {
    let Some(project_id) = project_id else {
        return false;
    };
    path.file_name().and_then(|name| name.to_str()) == Some(project_id)
        && path
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            == Some("projects")
}

fn has_first_party_manifest(root: &Path) -> Result<bool> {
    match fs::symlink_metadata(root.join(FIRST_PARTY_MANIFEST_FILENAME)) {
        Ok(metadata) => Ok(metadata.is_file() && !metadata.file_type().is_symlink()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(config_error(format!(
            "inspect first-party manifest in '{}': {error}",
            root.display()
        ))),
    }
}

fn ensure_no_reserved_cutover_entries(root: &Path) -> Result<()> {
    for name in [
        LIFECYCLE_LOCK_FILENAME,
        JOURNAL_FILENAME,
        PROFILE_REHEARSAL_MARKER_FILENAME,
    ] {
        if path_exists(&root.join(name))? {
            return Err(config_error(format!(
                "replacement target contains reserved lifecycle entry '{}'; refusing publication",
                root.join(name).display()
            )));
        }
    }
    Ok(())
}

fn link_lifecycle_lock(profile_root: &Path, target_root: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let source = profile_root.join(LIFECYCLE_LOCK_FILENAME);
        let destination = target_root.join(LIFECYCLE_LOCK_FILENAME);
        if path_exists(&destination)? {
            return Err(config_error(format!(
                "replacement target lifecycle lock '{}' already exists",
                destination.display()
            )));
        }
        fs::hard_link(&source, &destination).map_err(|error| {
            config_error(format!(
                "link held lifecycle lock into replacement target '{}': {error}",
                destination.display()
            ))
        })?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = (profile_root, target_root);
        Err(config_error(
            "replacement lifecycle lock transfer is unavailable on this platform",
        ))
    }
}

fn link_reserved_entry(source_root: &Path, target_root: &Path, name: &str) -> Result<()> {
    #[cfg(unix)]
    {
        let source = source_root.join(name);
        let destination = target_root.join(name);
        if path_exists(&destination)? {
            return Err(config_error(format!(
                "replacement target reserved entry '{}' already exists",
                destination.display()
            )));
        }
        fs::hard_link(&source, &destination).map_err(|error| {
            config_error(format!(
                "link replacement reserved entry '{}' into target: {error}",
                source.display()
            ))
        })?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = (source_root, target_root, name);
        Err(config_error(
            "replacement reserved-entry transfer is unavailable on this platform",
        ))
    }
}

fn remove_reserved_cutover_entry(
    root: &Path,
    name: &str,
    expected_operation_id: &str,
) -> Result<()> {
    let path = root.join(name);
    match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(config_error(format!(
            "inspect cutover metadata '{}' before cleanup: {error}",
            path.display()
        ))),
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err(config_error(format!(
                "cutover metadata '{}' must be a regular file",
                path.display()
            )))
        }
        Ok(_) => {
            let bytes = fs::read(&path).map_err(|error| {
                config_error(format!(
                    "read cutover metadata '{}' before cleanup: {error}",
                    path.display()
                ))
            })?;
            let metadata_journal: ReplacementJournal =
                serde_json::from_slice(&bytes).map_err(|error| {
                    config_error(format!(
                        "decode cutover metadata '{}' before cleanup: {error}",
                        path.display()
                    ))
                })?;
            if metadata_journal.operation_id != expected_operation_id {
                return Err(config_error(format!(
                    "cutover metadata '{}' belongs to another replacement operation",
                    path.display()
                )));
            }
            fs::remove_file(&path).map_err(|error| {
                config_error(format!(
                    "remove cutover metadata '{}': {error}",
                    path.display()
                ))
            })?;
            sync_directory(root, DirectorySyncPolicy::Strict).map_err(|error| {
                config_error(format!(
                    "sync cutover metadata removal '{}': {error}",
                    path.display()
                ))
            })
        }
    }
}

fn exchange_profile_roots(first: &Path, second: &Path) -> Result<()> {
    let first_parent = first
        .parent()
        .ok_or_else(|| config_error("replacement exchange source has no parent"))?;
    let second_parent = second
        .parent()
        .ok_or_else(|| config_error("replacement exchange destination has no parent"))?;
    if !physical_paths_equal(first_parent, second_parent)? {
        return Err(config_error(
            "replacement directory exchange requires sibling profile roots",
        ));
    }
    let first_metadata = fs::symlink_metadata(first).map_err(|error| {
        config_error(format!(
            "inspect replacement exchange source '{}': {error}",
            first.display()
        ))
    })?;
    let second_metadata = fs::symlink_metadata(second).map_err(|error| {
        config_error(format!(
            "inspect replacement exchange destination '{}': {error}",
            second.display()
        ))
    })?;
    if first_metadata.file_type().is_symlink()
        || second_metadata.file_type().is_symlink()
        || !first_metadata.is_dir()
        || !second_metadata.is_dir()
    {
        return Err(config_error(
            "replacement directory exchange requires two regular directories",
        ));
    }
    #[cfg(target_os = "linux")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        let first_path = first.display().to_string();
        let second_path = second.display().to_string();
        let first = CString::new(first.as_os_str().as_bytes())
            .map_err(|_| config_error("replacement exchange path contains NUL"))?;
        let second = CString::new(second.as_os_str().as_bytes())
            .map_err(|_| config_error("replacement exchange path contains NUL"))?;
        let result = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                first.as_ptr(),
                libc::AT_FDCWD,
                second.as_ptr(),
                libc::RENAME_EXCHANGE,
            )
        };
        if result != 0 {
            return Err(config_error(format!(
                "atomically exchange replacement directories '{}' and '{}': {}",
                first_path,
                second_path,
                io::Error::last_os_error()
            )));
        }
        return Ok(());
    }
    #[cfg(target_os = "macos")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        let first_path = first.display().to_string();
        let second_path = second.display().to_string();
        let first = CString::new(first.as_os_str().as_bytes())
            .map_err(|_| config_error("replacement exchange path contains NUL"))?;
        let second = CString::new(second.as_os_str().as_bytes())
            .map_err(|_| config_error("replacement exchange path contains NUL"))?;
        let result =
            unsafe { libc::renamex_np(first.as_ptr(), second.as_ptr(), libc::RENAME_SWAP) };
        if result != 0 {
            return Err(config_error(format!(
                "atomically exchange replacement directories '{}' and '{}': {}",
                first_path,
                second_path,
                io::Error::last_os_error()
            )));
        }
        return Ok(());
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (first, second);
        Err(config_error(
            "atomic replacement directory exchange is unsupported on this platform",
        ))
    }
}

fn rename_noreplace_path(source: &Path, destination: &Path) -> Result<()> {
    ensure_absent(destination, "replacement rename destination")?;
    #[cfg(target_os = "linux")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        let source = CString::new(source.as_os_str().as_bytes())
            .map_err(|_| config_error("replacement rename path contains NUL"))?;
        let destination = CString::new(destination.as_os_str().as_bytes())
            .map_err(|_| config_error("replacement rename path contains NUL"))?;
        let result = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                source.as_ptr(),
                libc::AT_FDCWD,
                destination.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if result != 0 {
            return Err(config_error(format!(
                "rename replacement path without overwrite: {}",
                io::Error::last_os_error()
            )));
        }
        return Ok(());
    }
    #[cfg(target_os = "macos")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        let source = CString::new(source.as_os_str().as_bytes())
            .map_err(|_| config_error("replacement rename path contains NUL"))?;
        let destination = CString::new(destination.as_os_str().as_bytes())
            .map_err(|_| config_error("replacement rename path contains NUL"))?;
        let result = unsafe {
            libc::renameatx_np(
                libc::AT_FDCWD,
                source.as_ptr(),
                libc::AT_FDCWD,
                destination.as_ptr(),
                libc::RENAME_EXCL,
            )
        };
        if result != 0 {
            return Err(config_error(format!(
                "rename replacement path without overwrite: {}",
                io::Error::last_os_error()
            )));
        }
        return Ok(());
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (source, destination);
        Err(config_error(
            "no durable no-replace rename is available on this platform",
        ))
    }
}

fn verify_profile_namespace(root: &Path, require_final_paths: bool) -> Result<()> {
    let metadata = fs::symlink_metadata(root).map_err(|error| {
        config_error(format!(
            "inspect profile target '{}': {error}",
            root.display()
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(config_error(format!(
            "profile target '{}' must be a regular directory",
            root.display()
        )));
    }
    if require_final_paths {
        for logical in REQUIRED_AUTHORITIES {
            let path = root.join(logical);
            let metadata = fs::symlink_metadata(&path).map_err(|error| {
                config_error(format!(
                    "exact V2 target is missing authority '{}': {error}",
                    path.display()
                ))
            })?;
            if metadata.file_type().is_symlink() {
                return Err(config_error(format!(
                    "exact V2 authority '{}' is a symlink",
                    path.display()
                )));
            }
            let expected_directory = matches!(*logical, "projects" | "migration-inventory");
            if metadata.is_dir() != expected_directory {
                return Err(config_error(format!(
                    "exact V2 authority '{}' has the wrong filesystem kind",
                    path.display()
                )));
            }
        }
    }
    verify_profile_tree(root)
}

fn verify_profile_tree(root: &Path) -> Result<()> {
    let mut entries = fs::read_dir(root)
        .map_err(|error| {
            config_error(format!("read profile target '{}': {error}", root.display()))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            config_error(format!(
                "enumerate profile target '{}': {error}",
                root.display()
            ))
        })?;
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        let metadata = path.symlink_metadata().map_err(|error| {
            config_error(format!(
                "inspect profile target '{}': {error}",
                path.display()
            ))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(config_error(format!(
                "profile target contains symlink '{}'",
                path.display()
            )));
        }
        if metadata.is_dir() {
            verify_profile_tree(&path)?;
        } else if !metadata.is_file() {
            return Err(config_error(format!(
                "profile target contains unsupported entry '{}'",
                path.display()
            )));
        }
    }
    Ok(())
}

fn resolve_profile_root(profile_root: Option<String>) -> Result<PathBuf> {
    let path = profile_root.map_or_else(
        || tracedecay_runtime_core::storage::default_profile_root(),
        |path| Ok(PathBuf::from(path)),
    )?;
    if !path.is_absolute() {
        return Err(config_error(format!(
            "replacement profile root must be absolute: '{}'",
            path.display()
        )));
    }
    Ok(path)
}

fn replacement_rehearsal_staging_root(rehearsal_root: &Path) -> Result<PathBuf> {
    let parent = rehearsal_root
        .parent()
        .ok_or_else(|| config_error("backup rehearsal target has no parent"))?;
    let name = rehearsal_root
        .file_name()
        .ok_or_else(|| config_error("backup rehearsal target has no directory name"))?
        .to_string_lossy();
    Ok(parent.join(format!(".{name}.tracedecay-rehearsal")))
}

/// Resume the replacement boundary after a process crash. The journal is
/// deliberately the only authority used to discover owned staging paths; a
/// stray directory with a similar name is never adopted or removed.
fn recover_interrupted_replacement_if_present(
    profile_root: &Path,
    dry_run: bool,
    timeout_seconds: u64,
) -> Result<Option<ReplacementSummary>> {
    let journal_path = profile_root.join(JOURNAL_FILENAME);
    let present = match fs::symlink_metadata(&journal_path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(config_error(format!(
                    "replacement journal '{}' must be a regular file",
                    journal_path.display()
                )));
            }
            true
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => {
            return Err(config_error(format!(
                "inspect replacement journal '{}': {error}",
                journal_path.display()
            )));
        }
    };
    if !present {
        return Ok(None);
    }
    if dry_run {
        return Err(config_error(format!(
            "replacement journal '{}' records an interrupted cutover; dry-run cannot recover it. \
             Re-run with --yes to validate and roll it back",
            journal_path.display()
        )));
    }

    let offline = super::take_profile_offline(profile_root, "replace-v1-recovery")?;
    let recovery_result = match offline.lease() {
        Ok(_) => recover_interrupted_replacement(profile_root, &journal_path, timeout_seconds),
        Err(error) => Err(error),
    };
    let finish_result = offline.finish();
    match (recovery_result, finish_result) {
        (Ok(summary), Ok(())) => Ok(summary),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(config_error(format!(
            "replacement recovery completed, but restoring the previous daemon state failed: {error}"
        ))),
        (Err(recovery_error), Err(finish_error)) => Err(config_error(format!(
            "replacement recovery failed: {recovery_error}; restoring the previous daemon state \
             also failed: {finish_error}"
        ))),
    }
}

fn recover_interrupted_replacement(
    profile_root: &Path,
    journal_path: &Path,
    timeout_seconds: u64,
) -> Result<Option<ReplacementSummary>> {
    let mut journal = read_journal(journal_path)?;
    validate_replacement_journal(profile_root, &journal)?;
    validate_journal_staging_bindings(&journal)?;
    let backup_exists = path_exists(&journal.backup_root)?;
    if backup_exists {
        tracedecay_maintenance::profile_backup::load_and_verify_backup(&journal.backup_root)
            .map_err(|error| {
                config_error(format!(
                    "cannot recover replacement: external backup '{}' failed verification: {error}",
                    journal.backup_root.display()
                ))
            })?;
        let observed_backup_tree_digest = profile_tree_digest(&journal.backup_root)?;
        let observed_backup_manifest_sha256 =
            sha256_regular_file(&journal.backup_root.join("backup-manifest.json"))?;
        let observed_backup_identity_sha256 =
            sha256_regular_file(&journal.backup_root.join("profile-identity.json"))?;
        if journal
            .backup_tree_digest
            .as_deref()
            .is_some_and(|expected| expected != observed_backup_tree_digest)
        {
            return Err(config_error(
                "replacement journal backup tree digest does not match the verified external backup",
            ));
        }
        if journal
            .backup_manifest_sha256
            .as_deref()
            .is_some_and(|expected| expected != observed_backup_manifest_sha256)
        {
            return Err(config_error(
                "replacement journal backup manifest digest does not match the verified external backup",
            ));
        }
        if journal
            .backup_identity_sha256
            .as_deref()
            .is_some_and(|expected| expected != observed_backup_identity_sha256)
        {
            return Err(config_error(
                "replacement journal backup identity digest does not match the verified external backup",
            ));
        }
        // A crash can occur after the maintenance backup is atomically
        // published but before this coordinator's attestation journal write.
        // The backup was independently verified above, so repair that
        // Prepared record from the observed bytes and make the identity
        // durable before adopting any recovery path. Later phases must have
        // carried these fields already; a missing field there remains a hard
        // journal corruption error.
        if journal.backup_tree_digest.is_none()
            || journal.backup_manifest_sha256.is_none()
            || journal.backup_identity_sha256.is_none()
        {
            if journal.phase != ReplacementPhase::Prepared {
                return Err(config_error(
                    "replacement journal has an external backup but no complete backup digest/identity attestation",
                ));
            }
            journal.backup_tree_digest = Some(observed_backup_tree_digest);
            journal.backup_manifest_sha256 = Some(observed_backup_manifest_sha256);
            journal.backup_identity_sha256 = Some(observed_backup_identity_sha256);
            write_journal(journal_path, &journal)?;
        }
    } else if journal.backup_tree_digest.is_some()
        || journal.backup_manifest_sha256.is_some()
        || journal.backup_identity_sha256.is_some()
        || journal.phase != ReplacementPhase::Prepared
    {
        return Err(config_error(format!(
            "cannot recover replacement phase {:?}: the journaled external backup '{}' is missing",
            journal.phase,
            journal.backup_root.display()
        )));
    }

    let quarantine_exists = path_exists(&journal.quarantine_root)?;
    match journal.phase {
        ReplacementPhase::Prepared if !quarantine_exists => {
            // An atomic directory exchange can succeed before the phase
            // record is replaced. The journal remains discoverable through
            // the reserved hard link, so restore the V1 rehearsal instead of
            // treating the exchanged source directory as an unfinished V2
            // target.
            let exchanged_target =
                journal
                    .staged_target_digest
                    .as_deref()
                    .is_some_and(|expected| {
                        has_first_party_manifest(profile_root).unwrap_or(false)
                            && profile_tree_digest(profile_root)
                                .map(|observed| observed == expected)
                                .unwrap_or(false)
                    });
            if exchanged_target {
                ensure_recovery_rehearsal(profile_root, &journal)?;
                let rehearsal_root = journal.rehearsal_root.clone();
                let rollback_root = journal.rollback_root.clone();
                rollback_from_rehearsal(
                    profile_root,
                    &rehearsal_root,
                    &rollback_root,
                    journal_path,
                    &mut journal,
                )?;
                remove_reserved_cutover_entry(
                    &journal.quarantine_root,
                    JOURNAL_FILENAME,
                    &journal.operation_id,
                )?;
                remove_owned_directory(&journal.backup_staging_root, "external backup staging")?;
                remove_target_workspace_or_exchanged_source(&journal.target_root, &journal)?;
                remove_owned_directory(&journal.rehearsal_root, "backup rehearsal target")?;
                remove_owned_directory(
                    &journal.rehearsal_staging_root,
                    "backup rehearsal staging",
                )?;
                remove_owned_directory(&journal.source_view_root, "read-only source view")?;
                remove_owned_directory(&journal.backup_view_root, "read-only backup view")?;
                remove_owned_directory(&journal.worker_stage_root, "pinned migration worker")?;
                return remove_journal(journal_path).map(|()| None);
            }
            // Authority checkpoints are durable before any cutover rename. If
            // a process dies in the middle of the first-party worker, resume
            // that exact target and continue the transaction instead of
            // discarding completed authority work. The target is still
            // isolated and the source profile has not moved yet.
            let has_authority_progress = journal
                .authority_progress
                .iter()
                .any(|progress| !matches!(progress.status, AuthorityProgressStatus::Pending));
            // A normal apply error cleans its disposable target before
            // returning, while a process crash can leave the target behind.
            // Only the latter is resumable; never try to interpret progress
            // checkpoints against a target that has already been discarded.
            if has_authority_progress && path_exists(&journal.target_root)? {
                match resume_prepared_replacement(
                    profile_root,
                    journal_path,
                    &mut journal,
                    timeout_seconds,
                ) {
                    Ok(summary) => return Ok(Some(summary)),
                    Err(error) => {
                        // No source bytes have been quarantined in this
                        // branch. Drop only the journal-owned partial target
                        // and views; the next invocation can start from the
                        // untouched source and retain the verified backup.
                        remove_target_workspace_or_exchanged_source(
                            &journal.target_root,
                            &journal,
                        )?;
                        remove_owned_directory(&journal.rehearsal_root, "backup rehearsal target")?;
                        remove_owned_directory(
                            &journal.rehearsal_staging_root,
                            "backup rehearsal staging",
                        )?;
                        remove_owned_directory(&journal.source_view_root, "read-only source view")?;
                        remove_owned_directory(&journal.backup_view_root, "read-only backup view")?;
                        remove_owned_directory(
                            &journal.worker_stage_root,
                            "pinned migration worker",
                        )?;
                        remove_journal(journal_path)?;
                        return Err(config_error(format!(
                            "resume of authority-by-authority replacement failed: {error}"
                        )));
                    }
                }
            }
            remove_owned_directory(&journal.backup_staging_root, "external backup staging")?;
            remove_target_workspace_or_exchanged_source(&journal.target_root, &journal)?;
            remove_owned_directory(&journal.rehearsal_root, "backup rehearsal target")?;
            remove_owned_directory(&journal.rehearsal_staging_root, "backup rehearsal staging")?;
            remove_owned_directory(&journal.rollback_root, "V2 rollback quarantine target")?;
            remove_owned_directory(&journal.source_view_root, "read-only source view")?;
            remove_owned_directory(&journal.backup_view_root, "read-only backup view")?;
            remove_owned_directory(&journal.worker_stage_root, "pinned migration worker")?;
            remove_journal(journal_path).map(|()| None)
        }
        ReplacementPhase::Prepared
        | ReplacementPhase::SourceQuarantined
        | ReplacementPhase::Published => {
            // The source may already have been moved when the process died
            // before its phase record was durable. Reconstruct V1 from the
            // verified rehearsal, retaining any published V2 bytes in the
            // journal-owned rollback quarantine.
            ensure_recovery_rehearsal(profile_root, &journal)?;
            let rehearsal_root = journal.rehearsal_root.clone();
            let rollback_root = journal.rollback_root.clone();
            rollback_from_rehearsal(
                profile_root,
                &rehearsal_root,
                &rollback_root,
                journal_path,
                &mut journal,
            )?;
            remove_reserved_cutover_entry(
                &journal.quarantine_root,
                JOURNAL_FILENAME,
                &journal.operation_id,
            )?;
            remove_owned_directory(&journal.backup_staging_root, "external backup staging")?;
            remove_target_workspace_or_exchanged_source(&journal.target_root, &journal)?;
            remove_owned_directory(&journal.rehearsal_root, "backup rehearsal target")?;
            remove_owned_directory(&journal.rehearsal_staging_root, "backup rehearsal staging")?;
            remove_owned_directory(&journal.source_view_root, "read-only source view")?;
            remove_owned_directory(&journal.backup_view_root, "read-only backup view")?;
            remove_owned_directory(&journal.worker_stage_root, "pinned migration worker")?;
            remove_journal(journal_path).map(|()| None)
        }
        ReplacementPhase::RolledBack => {
            // The rollback phase is written before disposable workspace
            // cleanup. A crash (or an operator editing the tree) can therefore
            // leave a durable RolledBack journal whose serving path was never
            // rechecked by this process. Re-verify the serving namespace from
            // the immutable source view/quarantine before deleting the last
            // recovery evidence.
            verify_rolled_back_serving_profile(profile_root, &journal)?;
            remove_reserved_cutover_entry(
                &journal.quarantine_root,
                JOURNAL_FILENAME,
                &journal.operation_id,
            )?;
            remove_owned_directory(&journal.backup_staging_root, "external backup staging")?;
            remove_target_workspace_or_exchanged_source(&journal.target_root, &journal)?;
            remove_owned_directory(&journal.rehearsal_root, "backup rehearsal target")?;
            remove_owned_directory(&journal.rehearsal_staging_root, "backup rehearsal staging")?;
            remove_owned_directory(&journal.source_view_root, "read-only source view")?;
            remove_owned_directory(&journal.backup_view_root, "read-only backup view")?;
            remove_owned_directory(&journal.worker_stage_root, "pinned migration worker")?;
            remove_journal(journal_path).map(|()| None)
        }
        ReplacementPhase::Verified => {
            verify_profile_namespace(profile_root, true)?;
            remove_owned_directory(&journal.backup_staging_root, "external backup staging")?;
            remove_target_workspace_or_exchanged_source(&journal.target_root, &journal)?;
            remove_owned_directory(&journal.rehearsal_root, "backup rehearsal target")?;
            remove_owned_directory(&journal.rehearsal_staging_root, "backup rehearsal staging")?;
            remove_owned_directory(&journal.rollback_root, "V2 rollback quarantine target")?;
            remove_owned_directory(&journal.source_view_root, "read-only source view")?;
            remove_owned_directory(&journal.backup_view_root, "read-only backup view")?;
            remove_owned_directory(&journal.worker_stage_root, "pinned migration worker")?;
            remove_journal(journal_path).map(|()| None)
        }
    }
}

fn resume_prepared_replacement(
    profile_root: &Path,
    journal_path: &Path,
    journal: &mut ReplacementJournal,
    timeout_seconds: u64,
) -> Result<ReplacementSummary> {
    let running_worker = std::env::current_exe()
        .map_err(|error| config_error(format!("resolve running replacement binary: {error}")))?;
    let running_worker = validate_worker_path(&running_worker)?;
    let running_worker_sha256 = sha256_worker(&running_worker)?;
    if running_worker_sha256 != journal.worker_sha256 {
        return Err(config_error(format!(
            "resuming replacement requires the originally admitted worker binary (journal SHA-256 {}, running SHA-256 {})",
            journal.worker_sha256, running_worker_sha256
        )));
    }
    ensure_recovery_rehearsal(profile_root, journal)?;
    verify_source_authority_checkpoints(profile_root, journal)?;
    let source_digest = profile_tree_digest(profile_root)?;
    if source_digest != journal.source_tree_digest {
        return Err(config_error(
            "source profile bytes changed after the replacement journal was prepared; refusing to resume",
        ));
    }
    let backup_digest = profile_tree_digest(&journal.backup_root)?;
    tracedecay_maintenance::profile_backup::load_and_verify_backup(&journal.backup_root).map_err(
        |error| {
            config_error(format!(
                "verify external backup while resuming replacement: {error}"
            ))
        },
    )?;
    if journal
        .backup_tree_digest
        .as_deref()
        .is_some_and(|expected| expected != backup_digest)
    {
        return Err(config_error(
            "replacement journal backup tree digest changed before resume",
        ));
    }
    if journal
        .backup_manifest_sha256
        .as_deref()
        .is_some_and(|expected| {
            sha256_regular_file(&journal.backup_root.join("backup-manifest.json"))
                .map(|observed| observed != expected)
                .unwrap_or(true)
        })
        || journal
            .backup_identity_sha256
            .as_deref()
            .is_some_and(|expected| {
                sha256_regular_file(&journal.backup_root.join("profile-identity.json"))
                    .map(|observed| observed != expected)
                    .unwrap_or(true)
            })
    {
        return Err(config_error(
            "replacement journal backup identity or manifest changed before resume",
        ));
    }

    if path_exists(&journal.source_view_root)? {
        validate_staging_directory(
            &journal.source_view_root,
            &journal.operation_id,
            "source-view",
            Some(&journal.source_tree_digest),
        )?;
        ensure_tree_digest(
            &journal.source_view_root,
            &journal.source_tree_digest,
            "isolated source view during resume",
        )?;
    } else {
        copy_worker_view(profile_root, &journal.source_view_root)?;
        bind_staging_directory(
            &journal.source_view_root,
            &journal.operation_id,
            "source-view",
            Some(&journal.source_tree_digest),
            true,
        )?;
    }
    if path_exists(&journal.backup_view_root)? {
        validate_staging_directory(
            &journal.backup_view_root,
            &journal.operation_id,
            "backup-view",
            Some(&backup_digest),
        )?;
        ensure_tree_digest(
            &journal.backup_view_root,
            &backup_digest,
            "isolated backup view during resume",
        )?;
    } else {
        copy_worker_view(&journal.backup_root, &journal.backup_view_root)?;
        bind_staging_directory(
            &journal.backup_view_root,
            &journal.operation_id,
            "backup-view",
            Some(&backup_digest),
            true,
        )?;
    }
    if !path_exists(&journal.target_root)? {
        create_private_directory(&journal.target_root)
            .map_err(|error| config_error(format!("create resumed V2 target: {error}")))?;
        let empty_target_digest = profile_tree_digest(&journal.target_root)?;
        // A missing target invalidates any previous staged-target attestation;
        // authority checkpoints below decide whether the target can be rebuilt
        // from the immutable backup or must fail closed.
        if journal.staged_target_digest.take().is_some() {
            write_journal(journal_path, journal)?;
        }
        bind_staging_directory(
            &journal.target_root,
            &journal.operation_id,
            "target",
            Some(&empty_target_digest),
            false,
        )?;
    } else {
        validate_staging_directory(
            &journal.target_root,
            &journal.operation_id,
            "target",
            journal.staged_target_digest.as_deref(),
        )?;
    }

    let timeout = Duration::from_secs(timeout_seconds);
    let apply_report = first_party_apply(
        &journal.source_view_root,
        &journal.backup_view_root,
        &journal.target_root,
        profile_root,
        &running_worker,
        &journal.worker_sha256,
        &journal.operation_id,
        &journal.provider,
        timeout,
    )?;
    validate_worker_report(
        apply_report,
        WorkerPhase::Apply,
        &journal.operation_id,
        &journal.provider,
        profile_root,
        &journal.target_root,
    )?;
    verify_profile_namespace(&journal.target_root, true)?;
    let verify_report = first_party_verify(
        &journal.target_root,
        profile_root,
        &journal.operation_id,
        &journal.provider,
    )?;
    validate_worker_report(
        verify_report,
        WorkerPhase::Verify,
        &journal.operation_id,
        &journal.provider,
        profile_root,
        &journal.target_root,
    )?;
    journal.staged_target_digest = Some(profile_tree_digest(&journal.target_root)?);
    bind_staging_directory(
        &journal.target_root,
        &journal.operation_id,
        "target",
        journal.staged_target_digest.as_deref(),
        false,
    )?;
    write_journal(journal_path, journal)?;

    let target_root = journal.target_root.clone();
    let quarantine_root = journal.quarantine_root.clone();
    let rehearsal_root = journal.rehearsal_root.clone();
    let rollback_root = journal.rollback_root.clone();
    let operation_id = journal.operation_id.clone();
    let provider = journal.provider.clone();
    if let Err(error) = publish_target(
        profile_root,
        profile_root
            .parent()
            .ok_or_else(|| config_error("replacement profile has no parent during resume"))?,
        &target_root,
        &quarantine_root,
        journal_path,
        journal,
    ) {
        let rollback = rollback_from_rehearsal(
            profile_root,
            &rehearsal_root,
            &rollback_root,
            journal_path,
            journal,
        );
        return Err(config_error(match rollback {
            Ok(()) => format!("resumed profile publication failed: {error}; rollback completed"),
            Err(rollback_error) => format!(
                "resumed profile publication failed: {error}; rollback failed: {rollback_error}"
            ),
        }));
    }

    let post_verify = first_party_verify(profile_root, &quarantine_root, &operation_id, &provider)?;
    validate_worker_report(
        post_verify,
        WorkerPhase::Verify,
        &operation_id,
        &provider,
        &quarantine_root,
        profile_root,
    )
    .and_then(|_| verify_profile_namespace(profile_root, true))
    .map_err(|error| {
        let rollback = rollback_from_rehearsal(
            profile_root,
            &rehearsal_root,
            &rollback_root,
            journal_path,
            journal,
        );
        config_error(match rollback {
            Ok(()) => format!(
                "resumed post-publication verification failed: {error}; rollback completed"
            ),
            Err(rollback_error) => format!(
                "resumed post-publication verification failed: {error}; rollback failed: {rollback_error}"
            ),
        })
    })?;

    validate_staging_directory(
        profile_root,
        &journal.operation_id,
        "target",
        journal.staged_target_digest.as_deref(),
    )?;
    remove_staging_marker_for_operation(profile_root, &journal.operation_id)?;
    journal.phase = ReplacementPhase::Verified;
    write_journal(journal_path, journal)?;
    remove_replacement_workspace(
        &journal.target_root,
        &journal.rehearsal_root,
        &journal.rehearsal_staging_root,
        &journal.rollback_root,
        &journal.source_view_root,
        &journal.backup_view_root,
        &journal.worker_stage_root,
    )?;
    remove_journal(journal_path)?;
    Ok(ReplacementSummary {
        protocol: REPLACEMENT_PROTOCOL,
        protocol_version: REPLACEMENT_PROTOCOL_VERSION,
        operation_id: journal.operation_id.clone(),
        provider: journal.provider.clone(),
        backup_root: journal.backup_root.clone(),
        preserved_v1_root: journal.quarantine_root.clone(),
        worker: running_worker,
        worker_sha256: running_worker_sha256,
        service_namespace: journal
            .service
            .as_ref()
            .map(|service| service.target_namespace.clone()),
        authorities: REQUIRED_AUTHORITIES.to_vec(),
        rollback: "stop V2, select the V1 binary/service, and restore the external backup; never open V2 files with V1",
    })
}

fn verify_rolled_back_serving_profile(
    profile_root: &Path,
    journal: &ReplacementJournal,
) -> Result<()> {
    verify_profile_namespace(profile_root, true)?;
    let source_reference = if path_exists(&journal.source_view_root)? {
        Some(journal.source_view_root.as_path())
    } else if path_exists(&journal.quarantine_root)? {
        Some(journal.quarantine_root.as_path())
    } else {
        None
    };
    if let Some(source_reference) = source_reference {
        ensure_restored_profile_equivalent(source_reference, profile_root)
    } else {
        ensure_tree_digest(
            profile_root,
            &journal.source_tree_digest,
            "rolled-back serving profile",
        )
    }
}

fn validate_maintenance_rehearsal_marker(
    rehearsal: &Path,
    journal: &ReplacementJournal,
) -> Result<bool> {
    let marker_path = rehearsal.join(PROFILE_REHEARSAL_MARKER_FILENAME);
    let metadata = match fs::symlink_metadata(&marker_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(config_error(format!(
                "inspect maintenance rehearsal marker '{}': {error}",
                marker_path.display()
            )));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(config_error(format!(
            "maintenance rehearsal marker '{}' must be a regular file",
            marker_path.display()
        )));
    }
    let marker: MaintenanceRehearsalMarker =
        serde_json::from_slice(&fs::read(&marker_path).map_err(|error| {
            config_error(format!(
                "read maintenance rehearsal marker '{}': {error}",
                marker_path.display()
            ))
        })?)
        .map_err(|error| {
            config_error(format!(
                "decode maintenance rehearsal marker '{}': {error}",
                marker_path.display()
            ))
        })?;
    let backup_id = journal
        .backup_root
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| config_error("replacement backup has no Unicode directory name"))?;
    let manifest_matches = journal
        .backup_manifest_sha256
        .as_deref()
        .is_some_and(|expected| marker.manifest_sha256 == expected);
    let identity_matches = journal
        .backup_identity_sha256
        .as_deref()
        .is_some_and(|expected| marker.source_profile_identity_sha256 == expected);
    Ok(
        marker.schema_version == MAINTENANCE_REHEARSAL_MARKER_SCHEMA_VERSION
            && marker.backup_id == backup_id
            && physical_paths_equal(&marker.backup_root, &journal.backup_root)?
            && manifest_matches
            && identity_matches
            && physical_paths_equal(&marker.restore_root, rehearsal)?,
    )
}

fn ensure_recovery_rehearsal(profile_root: &Path, journal: &ReplacementJournal) -> Result<()> {
    let rehearsal = &journal.rehearsal_root;
    let mut maintenance_recovered = false;
    let mut operation_marker_owned = false;
    let owned = if path_exists(rehearsal)? {
        match validate_staging_directory(rehearsal, &journal.operation_id, "rehearsal", None) {
            Ok(()) => {
                operation_marker_owned = true;
                true
            }
            Err(staging_error) => {
                // The coordinator writes its operation marker before it can
                // finish the digest check. A crash in that small window
                // leaves an owned but incomplete rehearsal. Authenticate the
                // marker and path first, then discard only that operation
                // owned partial so the next call can rebuild it from the
                // independently verified backup.
                if path_exists(&staging_marker_path(rehearsal))? {
                    validate_staging_marker_identity(
                        rehearsal,
                        &journal.operation_id,
                        "rehearsal",
                    )?;
                    operation_marker_owned = true;
                    false
                } else {
                    // The maintenance API publishes its own marker before
                    // this coordinator can add the operation marker. Finish
                    // only that exact, journal-bound rehearsal; an unmarked
                    // or foreign directory remains a hard recovery conflict.
                    if !validate_maintenance_rehearsal_marker(rehearsal, journal)? {
                        return Err(staging_error);
                    }
                    tracedecay_maintenance::profile_backup::rehearse_complete_profile_backup(
                        &journal.backup_root,
                        rehearsal,
                    )
                    .map_err(|error| {
                        config_error(format!(
                            "resume replacement rollback rehearsal '{}': {error}",
                            rehearsal.display()
                        ))
                    })?;
                    maintenance_recovered = true;
                    false
                }
            }
        }
    } else {
        false
    };
    // A completed rehearsal has the replacement ownership marker, but the
    // maintenance rehearsal marker was removed before this coordinator could
    // checkpoint its own journal. Keep that durable rehearsal and resume from
    // it; calling the maintenance rehearsal API again would reject the
    // existing destination as a conflict.
    if owned && verify_profile_namespace(rehearsal, true).is_ok() {
        return Ok(());
    }
    if path_exists(rehearsal)? && !maintenance_recovered {
        // A custom marker proves that this exact operation created the
        // incomplete namespace. Without it, the preceding branch returns a
        // conflict instead of deleting an operator-created sibling.
        if operation_marker_owned {
            remove_owned_rehearsal_after_marker_authentication(rehearsal, &journal.operation_id)?;
        } else {
            remove_owned_directory(rehearsal, "backup rehearsal target")?;
        }
    }
    if !maintenance_recovered {
        tracedecay_maintenance::profile_backup::rehearse_complete_profile_backup(
            &journal.backup_root,
            rehearsal,
        )
        .map_err(|error| {
            config_error(format!(
                "rebuild replacement rollback rehearsal '{}': {error}",
                rehearsal.display()
            ))
        })?;
    }
    // Prefer the untouched V1 copy that is still available at the point of a
    // crash. After publication it lives in the quarantine sibling; after the
    // atomic exchange but before quarantine rename it remains at target_root.
    // Before publication it is the live profile. This makes the rehearsal
    // include opaque roots even though the canonical backup manifest lists
    // only the eight governed authorities.
    let opaque_source = if path_exists(profile_root)? && !has_first_party_manifest(profile_root)? {
        Some(profile_root.to_path_buf())
    } else if path_exists(&journal.quarantine_root)? {
        Some(journal.quarantine_root.clone())
    } else if path_exists(&journal.target_root)? && !has_first_party_manifest(&journal.target_root)?
    {
        Some(journal.target_root.clone())
    } else {
        None
    };
    let Some(source) = opaque_source else {
        return Err(config_error(
            "cannot rebuild the replacement rehearsal: no untouched V1 namespace is available to preserve opaque roots",
        ));
    };
    copy_first_party_opaque_entries(&source, rehearsal)?;
    copy_first_party_opaque_sidecars(&source, rehearsal)?;
    sync_replacement_tree(rehearsal)?;
    let rehearsal_digest = profile_tree_digest(rehearsal)?;
    bind_staging_directory(
        rehearsal,
        &journal.operation_id,
        "rehearsal",
        Some(&rehearsal_digest),
        false,
    )?;
    verify_profile_namespace(rehearsal, true).map_err(|error| {
        config_error(format!(
            "verify replacement rollback rehearsal '{}': {error}",
            rehearsal.display()
        ))
    })
}

fn read_journal(path: &Path) -> Result<ReplacementJournal> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        config_error(format!(
            "inspect replacement journal '{}': {error}",
            path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(config_error(format!(
            "replacement journal '{}' must be a regular file",
            path.display()
        )));
    }
    serde_json::from_slice(&fs::read(path).map_err(|error| {
        config_error(format!(
            "read replacement journal '{}': {error}",
            path.display()
        ))
    })?)
    .map_err(|error| {
        config_error(format!(
            "decode replacement journal '{}': {error}",
            path.display()
        ))
    })
}

fn validate_replacement_journal(profile_root: &Path, journal: &ReplacementJournal) -> Result<()> {
    if journal.schema_version != JOURNAL_SCHEMA_VERSION {
        return Err(config_error(format!(
            "replacement journal schema {} is unsupported (expected {})",
            journal.schema_version, JOURNAL_SCHEMA_VERSION
        )));
    }
    if !journal.operation_id.starts_with("replace-v1-")
        || !is_single_path_component(&journal.operation_id)
    {
        return Err(config_error("replacement journal operation id is unsafe"));
    }
    if !matches!(journal.provider.as_str(), "native" | "ncm") {
        return Err(config_error(format!(
            "replacement journal provider '{}' is unsupported",
            journal.provider
        )));
    }
    let profile_identity = physical_path_identity(profile_root)?;
    let journal_profile_identity = physical_path_identity(&journal.profile_root)?;
    if profile_identity != journal_profile_identity {
        return Err(config_error(
            "replacement journal does not belong to the selected physical profile",
        ));
    }
    if !is_sha256_digest(&journal.source_tree_digest) {
        return Err(config_error(
            "replacement journal source tree digest is not a SHA-256 attestation",
        ));
    }
    if !is_sha256_digest(&journal.worker_sha256) {
        return Err(config_error(
            "replacement journal worker digest is not a SHA-256 attestation",
        ));
    }
    for (label, digest) in [
        ("backup tree", journal.backup_tree_digest.as_deref()),
        ("backup manifest", journal.backup_manifest_sha256.as_deref()),
        ("backup identity", journal.backup_identity_sha256.as_deref()),
        ("staged target", journal.staged_target_digest.as_deref()),
    ] {
        if let Some(digest) = digest {
            if !is_sha256_digest(digest) {
                return Err(config_error(format!(
                    "replacement journal {label} digest is not a SHA-256 attestation"
                )));
            }
        }
    }
    if let Some(service) = &journal.service {
        validate_service_namespace(&service.target_namespace)?;
        if !service.endpoint.is_absolute() {
            return Err(config_error(
                "replacement journal service endpoint must be absolute",
            ));
        }
        if let Some(source_namespace) = &service.source_namespace {
            validate_service_namespace(source_namespace)?;
        }
    }
    let parent = profile_root
        .parent()
        .ok_or_else(|| config_error("replacement profile has no parent during recovery"))?;
    let expected_rehearsal_staging = replacement_rehearsal_staging_root(&journal.rehearsal_root)?;
    let expected = [
        (
            "pinned migration worker",
            &journal.worker_stage_root,
            parent.join(format!(".{}.worker", journal.operation_id)),
        ),
        (
            "V2 staging target",
            &journal.target_root,
            parent.join(format!(".{}.target", journal.operation_id)),
        ),
        (
            "backup rehearsal target",
            &journal.rehearsal_root,
            parent.join(format!(".{}.backup-rehearsal", journal.operation_id)),
        ),
        (
            "backup rehearsal staging",
            &journal.rehearsal_staging_root,
            expected_rehearsal_staging,
        ),
        (
            "V1 quarantine target",
            &journal.quarantine_root,
            parent.join(format!(".{}.v1-preserved", journal.operation_id)),
        ),
        (
            "V2 rollback quarantine target",
            &journal.rollback_root,
            parent.join(format!(".{}.v2-failed", journal.operation_id)),
        ),
        (
            "read-only source view",
            &journal.source_view_root,
            parent.join(format!(".{}.source-view", journal.operation_id)),
        ),
        (
            "read-only backup view",
            &journal.backup_view_root,
            parent.join(format!(".{}.backup-view", journal.operation_id)),
        ),
    ];
    for (label, actual, expected) in expected {
        if !physical_paths_equal(actual, &expected)? {
            return Err(config_error(format!(
                "replacement journal {label} '{}' does not match its owned physical operation path '{}'",
                actual.display(),
                expected.display()
            )));
        }
        validate_owned_directory_reference(actual, label)?;
    }
    if !journal.backup_root.is_absolute() || !journal.backup_staging_root.is_absolute() {
        return Err(config_error(
            "replacement journal external backup paths must be absolute",
        ));
    }
    let backup_parent = journal
        .backup_root
        .parent()
        .ok_or_else(|| config_error("replacement journal backup has no parent"))?;
    let canonical_backup_parent = backup_parent.canonicalize().map_err(|error| {
        config_error(format!(
            "canonicalize replacement external backup parent '{}': {error}",
            backup_parent.display()
        ))
    })?;
    if paths_overlap(&canonical_backup_parent, profile_root)? {
        return Err(config_error(
            "replacement journal external backup must be outside the profile",
        ));
    }
    let backup_name = journal
        .backup_root
        .file_name()
        .ok_or_else(|| config_error("replacement journal backup has no directory name"))?;
    if !is_single_path_component(&backup_name.to_string_lossy()) {
        return Err(config_error(
            "replacement journal backup destination has an unsafe directory name",
        ));
    }
    if !physical_paths_equal(
        &journal.backup_root,
        &canonical_backup_parent.join(backup_name),
    )? {
        return Err(config_error(
            "replacement journal external backup is not the exact physical destination",
        ));
    }
    let expected_staging =
        canonical_backup_parent.join(format!(".{}.tmp", backup_name.to_string_lossy()));
    if !physical_paths_equal(&journal.backup_staging_root, &expected_staging)? {
        return Err(config_error(
            "replacement journal external backup staging path is not owned by its physical destination",
        ));
    }
    if let Ok(metadata) = fs::symlink_metadata(&journal.backup_root) {
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(config_error(format!(
                "replacement external backup '{}' is not a regular directory",
                journal.backup_root.display()
            )));
        }
    }
    validate_owned_directory_reference(&journal.backup_staging_root, "external backup staging")?;
    Ok(())
}

fn validate_journal_staging_bindings(journal: &ReplacementJournal) -> Result<()> {
    let staged = [
        (&journal.worker_stage_root, "worker", None),
        (
            &journal.target_root,
            "target",
            journal.staged_target_digest.as_deref(),
        ),
        (
            &journal.source_view_root,
            "source-view",
            Some(journal.source_tree_digest.as_str()),
        ),
        (
            &journal.backup_view_root,
            "backup-view",
            journal.backup_tree_digest.as_deref(),
        ),
        (&journal.rehearsal_root, "rehearsal", None),
    ];
    for (path, kind, expected_digest) in staged {
        if !path_exists(path)? {
            continue;
        }
        if kind == "worker" {
            // The ownership marker binds the staging directory to its tree
            // digest. The executable identity is a separate raw-byte
            // attestation; comparing that SHA to the tree digest would bind
            // two different domains and reject every valid worker.
            validate_staging_directory(path, &journal.operation_id, kind, expected_digest)?;
            let staged_worker = path.join(WORKER_STAGE_FILENAME);
            ensure_worker_identity(&staged_worker, &journal.worker_sha256)?;
            if !worker_is_executable(&staged_worker) {
                return Err(config_error(format!(
                    "replacement worker staging '{}' is no longer executable",
                    staged_worker.display()
                )));
            }
            complete_staging_digest_binding(path, &journal.operation_id, kind, None, false)?;
            continue;
        }
        if kind == "rehearsal"
            && !path_exists(&staging_marker_path(path))?
            && validate_maintenance_rehearsal_marker(path, journal)?
        {
            // The maintenance rehearsal is durable and journal-bound, but
            // the coordinator marker was not yet published when the process
            // stopped. `ensure_recovery_rehearsal` will finish that marker's
            // publication before adopting the directory.
            continue;
        }
        let exchanged_v1_target = kind == "target"
            && !has_first_party_manifest(path)?
            && !directory_is_empty(path)?
            && profile_tree_digest(path)? == journal.source_tree_digest;
        if exchanged_v1_target {
            // After the atomic exchange the old V1 directory is reachable at
            // the journaled target pathname. The target marker remains with
            // the V2 directory at the published profile path, so this
            // transitional V1 pathname is intentionally marker-free. Its
            // exact source tree digest and journal-owned pathname are the
            // ownership proof; requiring a marker here would make every
            // crash between exchange and quarantine unrecoverable.
            if path_exists(&staging_marker_path(path))? {
                return Err(config_error(
                    "exchanged V1 target unexpectedly contains a replacement staging marker",
                ));
            }
            ensure_tree_digest(path, &journal.source_tree_digest, "exchanged V1 target")?;
        } else {
            // Validate ownership and any digest already present before trying
            // to complete an older marker.  Passing the journal digest into
            // this first call would reject a legitimate crash window where
            // the marker was durably written before its content binding; the
            // completion helper is the step that attests and persists that
            // missing digest.
            validate_staging_directory(path, &journal.operation_id, kind, None)?;
            complete_staging_digest_binding(
                path,
                &journal.operation_id,
                kind,
                expected_digest,
                matches!(kind, "source-view" | "backup-view"),
            )?;
            validate_staging_directory(path, &journal.operation_id, kind, expected_digest)?;
        }
    }
    // The exchanged V1 tree is deliberately marker-free: its target marker
    // travels with the V2 tree at publication time. Its journal-owned
    // pathname plus the original source digest are therefore the ownership
    // proof. A foreign directory at that pathname must never be adopted as
    // rollback input.
    if path_exists(&journal.quarantine_root)? {
        if path_exists(&staging_marker_path(&journal.quarantine_root))? {
            return Err(config_error(
                "V1 quarantine unexpectedly contains a replacement staging marker",
            ));
        }
        ensure_tree_digest(
            &journal.quarantine_root,
            &journal.source_tree_digest,
            "journaled V1 quarantine",
        )?;
    }
    if path_exists(&journal.rollback_root)? {
        // The V2 tree retains its original `target` marker when it is moved
        // into the rollback quarantine. The cleanup validator accepts that
        // marker under the rollback label and still checks the operation and
        // staged-tree digest before any recovery path touches it.
        validate_cleanup_staging_marker(&journal.rollback_root, "V2 rollback quarantine target")?;
    }
    Ok(())
}

fn validate_owned_directory_reference(path: &Path, label: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            Err(config_error(format!(
                "replacement {label} '{}' is not an owned regular directory",
                path.display()
            )))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(config_error(format!(
            "inspect replacement {label} '{}': {error}",
            path.display()
        ))),
    }
}

fn path_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(config_error(format!(
            "inspect '{}': {error}",
            path.display()
        ))),
    }
}

fn remove_owned_directory(path: &Path, label: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(config_error(format!(
            "inspect {label} '{}': {error}",
            path.display()
        ))),
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            Err(config_error(format!(
                "refusing to remove non-directory {label} '{}'",
                path.display()
            )))
        }
        Ok(_) => {
            validate_cleanup_staging_marker(path, label)?;
            make_tree_writable(path)?;
            fs::remove_dir_all(path).map_err(|error| {
                config_error(format!("remove {label} '{}': {error}", path.display()))
            })?;
            if let Some(parent) = path.parent() {
                sync_directory(parent, DirectorySyncPolicy::Strict).map_err(|error| {
                    config_error(format!(
                        "sync removal of {label} '{}': {error}",
                        path.display()
                    ))
                })?;
            }
            Ok(())
        }
    }
}

/// Remove an incomplete rehearsal after its operation marker has been
/// authenticated. The content digest may be unavailable or intentionally
/// stale after a crash during materialization, so the normal cleanup validator
/// cannot be used; the marker identity and operation-shaped pathname are the
/// ownership proof for this recovery-only path.
fn remove_owned_rehearsal_after_marker_authentication(
    path: &Path,
    operation_id: &str,
) -> Result<()> {
    validate_staging_marker_identity(path, operation_id, "rehearsal")?;
    if !staging_path_matches_kind(path, operation_id, "rehearsal") {
        return Err(config_error(format!(
            "replacement rehearsal '{}' is not operation-shaped",
            path.display()
        )));
    }
    make_tree_writable(path)?;
    fs::remove_dir_all(path).map_err(|error| {
        config_error(format!(
            "remove incomplete replacement rehearsal '{}': {error}",
            path.display()
        ))
    })?;
    if let Some(parent) = path.parent() {
        sync_directory(parent, DirectorySyncPolicy::Strict).map_err(|error| {
            config_error(format!(
                "sync removal of incomplete replacement rehearsal '{}': {error}",
                path.display()
            ))
        })?;
    }
    Ok(())
}

/// Validate the marker before deleting a disposable replacement directory.
///
/// Most cleanup paths already have a journal and call
/// `validate_journal_staging_bindings`, but cleanup also runs on error paths
/// before the journal is reread. A directory can be replaced between those
/// two points; only an operation-shaped marker is eligible for deletion.
/// Transitional exchanged V1 roots intentionally have no marker, but they use
/// the digest-attested transition helper below rather than this generic
/// cleanup path.
fn validate_cleanup_staging_marker(path: &Path, label: &str) -> Result<()> {
    // The maintenance backup writer owns these two staging namespaces and
    // publishes them before the replacement coordinator can attach its own
    // operation marker.  A crash may therefore leave one of them without the
    // replacement marker.  Such a directory is never safe to delete from a
    // generic cleanup path: require the maintenance rehearsal marker where
    // one exists, and fail closed for the unmarked external-backup staging
    // directory.  The journal-bound recovery path can report the conflict to
    // the operator instead of risking collateral deletion of a reused backup
    // id.
    if label == "external backup staging" {
        return Err(config_error(format!(
            "replacement external backup staging '{}' has no operation ownership marker; refusing cleanup",
            path.display()
        )));
    }
    if label == "backup rehearsal staging" {
        return validate_maintenance_rehearsal_staging_marker(path);
    }
    let Some(expected_kind) = cleanup_staging_kind(label) else {
        return Ok(());
    };
    let marker_path = staging_marker_path(path);
    let marker_metadata = match fs::symlink_metadata(&marker_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(config_error(format!(
                "replacement {label} '{}' has no operation ownership marker; refusing cleanup",
                path.display()
            )));
        }
        Err(error) => {
            return Err(config_error(format!(
                "inspect replacement staging marker '{}' before cleanup: {error}",
                marker_path.display()
            )));
        }
    };
    if marker_metadata.file_type().is_symlink() || !marker_metadata.is_file() {
        return Err(config_error(format!(
            "replacement staging marker '{}' must be a regular file",
            marker_path.display()
        )));
    }
    let marker: ReplacementStagingMarker =
        serde_json::from_slice(&fs::read(&marker_path).map_err(|error| {
            config_error(format!(
                "read replacement staging marker '{}' before cleanup: {error}",
                marker_path.display()
            ))
        })?)
        .map_err(|error| {
            config_error(format!(
                "decode replacement staging marker '{}' before cleanup: {error}",
                marker_path.display()
            ))
        })?;
    let kind_matches = marker.kind == expected_kind
        // The V2 directory is moved to the rollback quarantine after an
        // exchange, retaining its original target marker while its pathname
        // changes to `.v2-failed`. It is still owned by this replacement.
        || (expected_kind == "rollback" && marker.kind == "target");
    if marker.schema_version != STAGING_MARKER_SCHEMA_VERSION || !kind_matches {
        return Err(config_error(format!(
            "replacement {label} '{}' has a foreign staging marker",
            path.display()
        )));
    }
    if !marker.operation_id.starts_with("replace-v1-")
        || !is_single_path_component(&marker.operation_id)
        || !staging_path_matches_kind(path, &marker.operation_id, expected_kind)
    {
        return Err(config_error(format!(
            "replacement {label} '{}' has a staging marker for another path",
            path.display()
        )));
    }
    if let Some(expected_digest) = marker.expected_digest.as_deref() {
        ensure_tree_digest(path, expected_digest, &format!("{label} cleanup"))?;
    }
    Ok(())
}

/// Remove the old V1 root that is briefly reachable at the journaled target
/// pathname after an atomic directory exchange and before its no-replace
/// quarantine rename. That root is deliberately marker-free because the V2
/// target marker moved with the V2 tree. Its exact source digest, operation
/// shaped pathname, and absence of V2 metadata are the complete ownership
/// proof; a generic markerless cleanup must never be used for this transition.
fn remove_exchanged_source_directory(
    path: &Path,
    expected_digest: &str,
    operation_id: &str,
    label: &str,
) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        config_error(format!(
            "inspect exchanged V1 {label} '{}': {error}",
            path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(config_error(format!(
            "exchanged V1 {label} '{}' must be a regular directory",
            path.display()
        )));
    }
    if !staging_path_matches_kind(path, operation_id, "target") {
        return Err(config_error(format!(
            "exchanged V1 {label} '{}' is not the journal-owned target path",
            path.display()
        )));
    }
    if path_exists(&staging_marker_path(path))?
        || path_exists(&path.join(PROFILE_REHEARSAL_MARKER_FILENAME))?
        || has_first_party_manifest(path)?
    {
        return Err(config_error(format!(
            "exchanged V1 {label} '{}' contains replacement metadata; refusing markerless cleanup",
            path.display()
        )));
    }
    ensure_tree_digest(path, expected_digest, &format!("exchanged V1 {label}"))?;
    // The journal is hard-linked into the target before publication so that
    // recovery remains discoverable across the exchange. Remove that metadata
    // only after authenticating its operation id; a foreign journal is a hard
    // recovery conflict rather than something to delete opportunistically.
    if path_exists(&path.join(JOURNAL_FILENAME))? {
        remove_reserved_cutover_entry(path, JOURNAL_FILENAME, operation_id)?;
    }
    make_tree_writable(path)?;
    fs::remove_dir_all(path).map_err(|error| {
        config_error(format!(
            "remove exchanged V1 {label} '{}': {error}",
            path.display()
        ))
    })?;
    if let Some(parent) = path.parent() {
        sync_directory(parent, DirectorySyncPolicy::Strict).map_err(|error| {
            config_error(format!(
                "sync removal of exchanged V1 {label} '{}': {error}",
                path.display()
            ))
        })?;
    }
    Ok(())
}

/// Recovery normally removes a marker-bound V2 target. Between the exchange
/// and no-replace quarantine rename the same pathname temporarily contains
/// the marker-free V1 source; select that one narrow, digest-attested path and
/// otherwise retain the strict marker ownership check.
fn remove_target_workspace_or_exchanged_source(
    path: &Path,
    journal: &ReplacementJournal,
) -> Result<()> {
    if !path_exists(path)? {
        return Ok(());
    }
    if !path_exists(&staging_marker_path(path))?
        && !path_exists(&path.join(PROFILE_REHEARSAL_MARKER_FILENAME))?
        && !has_first_party_manifest(path)?
        && profile_tree_digest(path).is_ok_and(|observed| observed == journal.source_tree_digest)
    {
        return remove_exchanged_source_directory(
            path,
            &journal.source_tree_digest,
            &journal.operation_id,
            "target",
        );
    }
    remove_owned_directory(path, "V2 staging target")
}

fn cleanup_staging_kind(label: &str) -> Option<&'static str> {
    match label {
        "V2 staging target" => Some("target"),
        "backup rehearsal target" => Some("rehearsal"),
        "read-only source view" => Some("source-view"),
        "read-only backup view" => Some("backup-view"),
        "pinned migration worker" => Some("worker"),
        "V2 rollback quarantine target" => Some("rollback"),
        _ => None,
    }
}

fn staging_path_matches_kind(path: &Path, operation_id: &str, kind: &str) -> bool {
    let Some(suffix) = (match kind {
        "worker" => Some("worker"),
        "target" => Some("target"),
        "rehearsal" => Some("backup-rehearsal"),
        "source-view" => Some("source-view"),
        "backup-view" => Some("backup-view"),
        "rollback" => Some("v2-failed"),
        _ => None,
    }) else {
        return false;
    };
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == format!(".{operation_id}.{suffix}"))
}

/// The maintenance rehearsal API writes its marker into its staging root
/// before copying any authority.  Validate that marker before a replacement
/// cleanup removes the staging root.  The replacement journal independently
/// binds the staging pathname to the exact backup and restore root; this
/// helper only proves that the entry is a genuine maintenance-owned marker
/// rather than an arbitrary directory with a familiar suffix.
fn validate_maintenance_rehearsal_staging_marker(path: &Path) -> Result<()> {
    let marker_path = path.join(PROFILE_REHEARSAL_MARKER_FILENAME);
    let metadata = match fs::symlink_metadata(&marker_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(config_error(format!(
                "replacement backup rehearsal staging '{}' has no maintenance ownership marker; refusing cleanup",
                path.display()
            )));
        }
        Err(error) => {
            return Err(config_error(format!(
                "inspect maintenance rehearsal staging marker '{}': {error}",
                marker_path.display()
            )));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(config_error(format!(
            "maintenance rehearsal staging marker '{}' must be a regular file",
            marker_path.display()
        )));
    }
    let marker: MaintenanceRehearsalMarker =
        serde_json::from_slice(&fs::read(&marker_path).map_err(|error| {
            config_error(format!(
                "read maintenance rehearsal staging marker '{}': {error}",
                marker_path.display()
            ))
        })?)
        .map_err(|error| {
            config_error(format!(
                "decode maintenance rehearsal staging marker '{}': {error}",
                marker_path.display()
            ))
        })?;
    let restore_path_matches = physical_paths_equal(&marker.restore_root, path)?
        || maintenance_rehearsal_staging_restore_root(path).is_some_and(|expected| {
            physical_paths_equal(&marker.restore_root, &expected).unwrap_or(false)
        });
    if marker.schema_version != MAINTENANCE_REHEARSAL_MARKER_SCHEMA_VERSION
        || marker.backup_id.is_empty()
        || !is_sha256_digest(&marker.manifest_sha256)
        || !is_sha256_digest(&marker.source_profile_identity_sha256)
        || !marker.backup_root.is_absolute()
        || !marker.restore_root.is_absolute()
        || !restore_path_matches
    {
        return Err(config_error(format!(
            "maintenance rehearsal staging marker '{}' is malformed or unsupported",
            marker_path.display()
        )));
    }
    Ok(())
}

fn maintenance_rehearsal_staging_restore_root(path: &Path) -> Option<PathBuf> {
    let name = path.file_name()?.to_str()?;
    let base = name.strip_suffix(".tracedecay-rehearsal")?;
    let base = base.strip_prefix('.')?;
    Some(path.parent()?.join(base))
}

fn is_single_path_component(value: &str) -> bool {
    let path = Path::new(value);
    path.components().count() == 1
        && matches!(
            path.components().next(),
            Some(std::path::Component::Normal(_))
        )
}

fn validate_profile_root(path: &Path) -> Result<()> {
    validate_profile_root_path(path)?;
    if has_first_party_manifest(path)? {
        return Err(config_error(format!(
            "replacement profile '{}' already contains the first-party V2 manifest; refusing to treat a published V2 profile as V1",
            path.display()
        )));
    }
    match fs::symlink_metadata(path.join(JOURNAL_FILENAME)) {
        Ok(_) => {
            return Err(config_error(format!(
                "replacement journal '{}' already exists; inspect its paths and finish or restore \
                 that operation before starting another",
                path.join(JOURNAL_FILENAME).display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(config_error(format!(
                "inspect replacement journal '{}': {error}",
                path.join(JOURNAL_FILENAME).display()
            )));
        }
    }
    match fs::symlink_metadata(path.join(PROFILE_REHEARSAL_MARKER_FILENAME)) {
        Ok(_) => {
            return Err(config_error(format!(
                "profile rehearsal marker '{}' already exists; finish or clear that maintenance \
                 rehearsal before starting replacement",
                path.join(PROFILE_REHEARSAL_MARKER_FILENAME).display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(config_error(format!(
                "inspect profile rehearsal marker '{}': {error}",
                path.join(PROFILE_REHEARSAL_MARKER_FILENAME).display()
            )));
        }
    }
    match fs::symlink_metadata(path.join(STAGING_MARKER_FILENAME)) {
        Ok(_) => {
            return Err(config_error(format!(
                "replacement staging marker '{}' already exists; finish or clear that replacement before starting another",
                path.join(STAGING_MARKER_FILENAME).display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(config_error(format!(
                "inspect replacement staging marker '{}': {error}",
                path.join(STAGING_MARKER_FILENAME).display()
            )));
        }
    }
    verify_profile_tree(path)
}

fn validate_profile_root_path(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        config_error(format!(
            "inspect replacement profile '{}': {error}",
            path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(config_error(format!(
            "replacement profile root '{}' must be a regular directory",
            path.display()
        )));
    }
    let physical = physical_path_identity(path)?;
    if physical.parent().is_none() {
        return Err(config_error(format!(
            "replacement profile root '{}' must resolve to a physical directory with a parent",
            path.display()
        )));
    }
    Ok(())
}

fn validate_worker_path(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute() {
        return Err(config_error(format!(
            "migration worker path must be absolute: '{}'",
            path.display()
        )));
    }
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        config_error(format!(
            "inspect migration worker '{}': {error}",
            path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(config_error(format!(
            "migration worker '{}' must be a regular executable file",
            path.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o111 == 0 {
            return Err(config_error(format!(
                "migration worker '{}' is not executable",
                path.display()
            )));
        }
    }
    path.canonicalize()
        .map_err(|error| config_error(format!("canonicalize migration worker: {error}")))
}

fn sha256_worker(path: &Path) -> Result<String> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        config_error(format!(
            "inspect migration worker '{}': {error}",
            path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(config_error(format!(
            "migration worker '{}' must be a regular file",
            path.display()
        )));
    }
    let mut file = File::open(path).map_err(|error| {
        config_error(format!(
            "open migration worker '{}' for identity: {error}",
            path.display()
        ))
    })?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|error| {
            config_error(format!(
                "read migration worker '{}' for identity: {error}",
                path.display()
            ))
        })?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex::encode(digest.finalize()))
}

fn sha256_regular_file(path: &Path) -> Result<String> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        config_error(format!(
            "inspect file '{}' for SHA-256: {error}",
            path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(config_error(format!(
            "file '{}' must be a regular file for SHA-256",
            path.display()
        )));
    }
    let mut file = File::open(path).map_err(|error| {
        config_error(format!(
            "open file '{}' for SHA-256: {error}",
            path.display()
        ))
    })?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|error| {
            config_error(format!(
                "read file '{}' for SHA-256: {error}",
                path.display()
            ))
        })?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex::encode(digest.finalize()))
}

fn stage_worker(
    source: &Path,
    stage_root: &Path,
    expected_sha256: &str,
    operation_id: &str,
) -> Result<PathBuf> {
    create_private_directory(stage_root).map_err(|error| {
        config_error(format!(
            "create pinned migration worker directory '{}': {error}",
            stage_root.display()
        ))
    })?;
    // Bind the directory before opening or copying the executable. A crash
    // during the copy must leave an authenticated, operation-shaped root so
    // recovery can remove only this partial worker staging tree.
    bind_staging_directory(stage_root, operation_id, "worker", None, false)?;
    let staged = stage_root.join(WORKER_STAGE_FILENAME);
    // Read the source through one opened descriptor and hash the exact bytes
    // written to the private staging file. A pathname validation followed by
    // `fs::copy(path, path)` leaves a replaceable pathname window; if the
    // running binary is swapped between those calls, the journal could attest
    // one executable while retaining another. A changed source now fails the
    // already journaled digest instead of being adopted.
    let mut source_options = OpenOptions::new();
    source_options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        source_options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    let mut source_file = source_options.open(source).map_err(|error| {
        config_error(format!(
            "open migration worker '{}' for pinned staging: {error}",
            source.display()
        ))
    })?;
    let metadata = source_file.metadata().map_err(|error| {
        config_error(format!(
            "inspect migration worker '{}' for pinned staging: {error}",
            source.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(config_error(format!(
            "migration worker '{}' must be a regular file",
            source.display()
        )));
    }
    let mut staged_file = tracedecay_private_fs::create_private_file(&staged).map_err(|error| {
        config_error(format!(
            "create pinned migration worker '{}': {error}",
            staged.display()
        ))
    })?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = source_file.read(&mut buffer).map_err(|error| {
            config_error(format!(
                "read migration worker '{}' for pinned staging: {error}",
                source.display()
            ))
        })?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        staged_file.write_all(&buffer[..read]).map_err(|error| {
            config_error(format!(
                "write pinned migration worker '{}': {error}",
                staged.display()
            ))
        })?;
    }
    staged_file.sync_all().map_err(|error| {
        config_error(format!(
            "sync pinned migration worker '{}': {error}",
            staged.display()
        ))
    })?;
    drop(staged_file);
    let observed_sha256 = hex::encode(digest.finalize());
    if observed_sha256 != expected_sha256 {
        return Err(config_error(format!(
            "pinned migration worker digest changed during staging (expected SHA-256 {}, observed {})",
            expected_sha256, observed_sha256
        )));
    }
    Ok(staged)
}

fn ensure_worker_executable(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let metadata = fs::symlink_metadata(path).map_err(|error| {
            config_error(format!(
                "inspect staged migration worker '{}': {error}",
                path.display()
            ))
        })?;
        let mut permissions = metadata.permissions();
        permissions.set_mode(permissions.mode() | 0o111);
        fs::set_permissions(path, permissions).map_err(|error| {
            config_error(format!(
                "restore staged migration worker executable permission '{}': {error}",
                path.display()
            ))
        })?;
    }
    Ok(())
}

fn worker_is_executable(path: &Path) -> bool {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return false;
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(windows)]
    {
        // Windows resolves executability from the file type and association;
        // the direct Command::new spawn below is the authoritative probe.
        true
    }
    #[cfg(not(any(unix, windows)))]
    {
        true
    }
}

fn ensure_worker_identity(path: &Path, expected_sha256: &str) -> Result<()> {
    let observed = sha256_worker(path)?;
    if observed != expected_sha256 {
        return Err(config_error(format!(
            "migration worker '{}' changed after admission (expected SHA-256 {}, observed {})",
            path.display(),
            expected_sha256,
            observed
        )));
    }
    Ok(())
}

fn validate_worker_reference_for_plan(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute() {
        return Err(config_error(format!(
            "migration worker path must be absolute: '{}'",
            path.display()
        )));
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(config_error(format!(
            "migration worker '{}' must not be a symlink",
            path.display()
        ))),
        Ok(metadata) if !metadata.is_file() => Err(config_error(format!(
            "migration worker '{}' must be a regular file",
            path.display()
        ))),
        Ok(_) => Ok(path.to_path_buf()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(path.to_path_buf()),
        Err(error) => Err(config_error(format!(
            "inspect migration worker '{}': {error}",
            path.display()
        ))),
    }
}

fn validate_backup_parent(profile_root: &Path, backup_parent: &Path) -> Result<PathBuf> {
    if !backup_parent.is_absolute() {
        return Err(config_error(format!(
            "replacement backup parent must be absolute: '{}'",
            backup_parent.display()
        )));
    }

    // The complete-backup API creates missing parents. Walk toward the nearest
    // existing ancestor with symlink_metadata, rather than exists(), so a
    // dangling symlink anywhere in a not-yet-created path is rejected too.
    let mut existing = backup_parent.to_path_buf();
    loop {
        match fs::symlink_metadata(&existing) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(config_error(format!(
                    "replacement backup parent '{}' must resolve through regular directories",
                    backup_parent.display()
                )));
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(config_error(format!(
                    "replacement backup parent '{}' must resolve through regular directories",
                    backup_parent.display()
                )));
            }
            Ok(_) => {
                existing.canonicalize().map_err(|error| {
                    config_error(format!(
                        "canonicalize replacement backup parent '{}': {error}",
                        backup_parent.display()
                    ))
                })?;
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if !existing.pop() {
                    return Err(config_error(format!(
                        "could not resolve replacement backup parent '{}'",
                        backup_parent.display()
                    )));
                }
            }
            Err(error) => {
                return Err(config_error(format!(
                    "inspect replacement backup parent '{}': {error}",
                    existing.display()
                )));
            }
        }
    }
    let canonical_profile = profile_root
        .canonicalize()
        .map_err(|error| config_error(format!("canonicalize replacement profile: {error}")))?;
    if paths_overlap(backup_parent, &canonical_profile)? {
        return Err(config_error(format!(
            "replacement backup parent '{}' must be outside profile '{}'",
            backup_parent.display(),
            profile_root.display()
        )));
    }
    Ok(backup_parent.to_path_buf())
}

/// Return a platform-normalized physical identity for a path. Canonicalizing
/// existing components resolves symlink aliases; the Unix device/inode check
/// below additionally catches hard-link aliases which have different lexical
/// names. Callers use `paths_overlap` for both containment and identity.
fn physical_path_identity(path: &Path) -> Result<PathBuf> {
    if let Ok(canonical) = path.canonicalize() {
        return Ok(normalize_physical_path(canonical));
    }
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| {
                config_error(format!(
                    "resolve physical path '{}': {error}",
                    path.display()
                ))
            })?
            .join(path)
    };
    let mut existing = absolute.clone();
    let mut suffix = Vec::new();
    while fs::symlink_metadata(&existing).is_err() {
        let Some(name) = existing.file_name() else {
            return Ok(normalize_physical_path(absolute));
        };
        suffix.push(name.to_os_string());
        if !existing.pop() {
            return Ok(normalize_physical_path(absolute));
        }
    }
    let mut result = existing.canonicalize().map_err(|error| {
        config_error(format!(
            "canonicalize physical path ancestor '{}': {error}",
            existing.display()
        ))
    })?;
    for name in suffix.iter().rev() {
        result.push(name);
    }
    Ok(normalize_physical_path(result))
}

fn normalize_physical_path(path: PathBuf) -> PathBuf {
    #[cfg(windows)]
    {
        // Windows path identity is case-insensitive. `canonicalize` already
        // resolves junctions and short names, so normalizing the remaining
        // spelling prevents recovery from rejecting the same directory when
        // the journal was written with a different drive-letter/path case.
        return PathBuf::from(path.to_string_lossy().to_ascii_lowercase());
    }
    #[cfg(not(windows))]
    {
        path
    }
}

fn physical_paths_equal(first: &Path, second: &Path) -> Result<bool> {
    Ok(physical_path_identity(first)? == physical_path_identity(second)?)
}

fn paths_overlap(first: &Path, second: &Path) -> Result<bool> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if let (Ok(first_metadata), Ok(second_metadata)) =
            (fs::symlink_metadata(first), fs::symlink_metadata(second))
        {
            if first_metadata.dev() == second_metadata.dev()
                && first_metadata.ino() == second_metadata.ino()
            {
                return Ok(true);
            }
        }
    }
    let first = physical_path_identity(first)?;
    let second = physical_path_identity(second)?;
    Ok(first == second || first.starts_with(&second) || second.starts_with(&first))
}

fn backup_parent_state(path: &Path) -> Result<&'static str> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok("will be created at apply")
        }
        Err(error) => Err(config_error(format!(
            "inspect replacement backup parent '{}': {error}",
            path.display()
        ))),
        Ok(metadata) if metadata.file_type().is_symlink() => Ok("symlink (rejected)"),
        Ok(metadata) if metadata.is_dir() => Ok("ready"),
        Ok(_) => Ok("not a directory (rejected)"),
    }
}

/// Check whether a future sibling directory can be created without touching
/// the filesystem. A dry-run cannot safely probe by creating and deleting a
/// sentinel: that would violate its zero-write contract and can race another
/// process. Permission bits still expose the common failure mode, while the
/// apply path remains the authority for ACLs and filesystem quotas.
fn directory_creation_feasible(path: &Path) -> bool {
    let mut candidate = path.to_path_buf();
    loop {
        match fs::symlink_metadata(&candidate) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return false;
            }
            Ok(metadata) => {
                #[cfg(unix)]
                {
                    let mode = metadata.permissions().mode();
                    return mode & 0o300 == 0o300 && mode & 0o100 != 0;
                }
                #[cfg(not(unix))]
                {
                    return !metadata.permissions().readonly();
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if !candidate.pop() {
                    return false;
                }
            }
            Err(_) => return false,
        }
    }
}

fn inspect_profile_authorities(
    profile_root: &Path,
) -> Result<(Vec<&'static str>, Vec<&'static str>)> {
    let mut present = Vec::new();
    let mut missing = Vec::new();
    for logical in REQUIRED_AUTHORITIES {
        match fs::symlink_metadata(profile_root.join(logical)) {
            Ok(metadata) if !metadata.file_type().is_symlink() => present.push(*logical),
            Ok(_) => missing.push(*logical),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => missing.push(*logical),
            Err(error) => {
                return Err(config_error(format!(
                    "inspect replacement authority '{}': {error}",
                    profile_root.join(logical).display()
                )));
            }
        }
    }
    Ok((present, missing))
}

fn validate_path_component(value: &str, label: &str) -> Result<()> {
    let path = Path::new(value);
    if value.is_empty()
        || path.is_absolute()
        || path.components().count() != 1
        || !matches!(
            path.components().next(),
            Some(std::path::Component::Normal(_))
        )
    {
        return Err(config_error(format!(
            "{label} must be a single non-empty directory name"
        )));
    }
    Ok(())
}

fn create_private_directory(path: &Path) -> std::io::Result<()> {
    fs::create_dir(path)?;
    tracedecay_runtime_core::storage::set_private_dir_permissions(path)
}

fn ensure_private_directory(path: &Path, label: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            Err(config_error(format!(
                "{label} '{}' is not a regular directory",
                path.display()
            )))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            create_private_directory(path)
                .map_err(|error| config_error(format!("create {label}: {error}")))
        }
        Err(error) => Err(config_error(format!(
            "inspect {label} '{}': {error}",
            path.display()
        ))),
    }
}

fn ensure_no_rehearsal_marker(profile_root: &Path) -> Result<()> {
    let marker = profile_root.join(PROFILE_REHEARSAL_MARKER_FILENAME);
    match fs::symlink_metadata(&marker) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(config_error(format!(
            "inspect profile rehearsal marker '{}': {error}",
            marker.display()
        ))),
        Ok(_) => Err(config_error(format!(
            "refusing rollback while profile rehearsal marker '{}' is present; it belongs to another operation",
            marker.display()
        ))),
    }
}

fn ensure_absent(path: &Path, label: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(config_error(format!(
            "{label} '{}' already exists; refusing to adopt it",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(config_error(format!(
            "inspect {label} '{}': {error}",
            path.display()
        ))),
    }
}

fn directory_is_empty(path: &Path) -> Result<bool> {
    let entries = fs::read_dir(path).map_err(|error| {
        config_error(format!(
            "read isolated target '{}': {error}",
            path.display()
        ))
    })?;
    // Every operation-owned staging directory carries its marker before the
    // worker is admitted. The marker is metadata, not migrated target data;
    // treating it as a payload would make the first-party preflight fail on
    // every normal invocation.
    for entry in entries {
        let entry = entry.map_err(|error| {
            config_error(format!(
                "enumerate isolated target '{}' for preflight: {error}",
                path.display()
            ))
        })?;
        if entry.file_name() != STAGING_MARKER_FILENAME {
            return Ok(false);
        }
    }
    Ok(true)
}

fn write_journal(path: &Path, journal: &ReplacementJournal) -> Result<()> {
    let bytes = tracedecay_domain::canonical_json_bytes(journal)
        .map_err(|error| config_error(format!("encode replacement journal: {error}")))?;
    let previous = match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(config_error(format!(
                "replacement journal '{}' must be a regular file",
                path.display()
            )));
        }
        Ok(_) => Some(fs::read(path).map_err(|error| {
            config_error(format!("read previous replacement journal: {error}"))
        })?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(config_error(format!(
                "inspect replacement journal '{}': {error}",
                path.display()
            )));
        }
    };
    let expectation = if previous.is_some() {
        tracedecay_private_fs::framed_log::ConditionalPublishExpectation::Present
    } else {
        tracedecay_private_fs::framed_log::ConditionalPublishExpectation::Missing
    };
    let previous_for_verify = previous;
    let bytes_for_verify = bytes.clone();
    tracedecay_private_fs::framed_log::atomic_write_prepared_conditionally(
        path,
        "replacement-journal",
        &bytes,
        expectation,
        tracedecay_private_fs::framed_log::ConditionalPublishCallbacks {
            prepare: |temporary: &Path| {
                tracedecay_private_fs::framed_log::tighten_existing_file(temporary)
            },
            before_publish: || {},
            after_publish: || {},
            verify_displaced: move |displaced: &Path| {
                let Some(expected) = previous_for_verify.as_deref() else {
                    return Ok(false);
                };
                Ok(fs::read(displaced).ok().as_deref() == Some(expected))
            },
            verify_published: move |published: &Path| {
                Ok(fs::read(published).ok().as_deref() == Some(bytes_for_verify.as_slice()))
            },
        },
        DirectorySyncPolicy::Strict,
    )
    .map_err(|error| config_error(format!("publish replacement journal: {error}")))
}

fn initial_authority_progress(profile_root: &Path) -> Result<Vec<AuthorityProgress>> {
    REQUIRED_AUTHORITIES
        .iter()
        .map(|authority| {
            let (source_digest, _) = authority_observation(profile_root, authority)?;
            Ok(AuthorityProgress {
                authority: (*authority).to_owned(),
                status: AuthorityProgressStatus::Pending,
                source_digest,
                target_digest: None,
            })
        })
        .collect()
}

fn remove_journal(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => {
            let parent = path
                .parent()
                .ok_or_else(|| config_error("replacement journal has no parent"))?;
            sync_directory(parent, DirectorySyncPolicy::Strict)
                .map_err(|error| config_error(format!("sync replacement journal removal: {error}")))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(config_error(format!("remove replacement journal: {error}"))),
    }
}

fn cleanup_owned_directory(path: &Path) {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return;
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return;
    }
    // Error paths are deliberately best-effort, but they must never turn a
    // foreign marker-bearing directory into collateral cleanup. The caller
    // cannot surface a second error here, so leave an invalid/foreign tree for
    // journal recovery to report explicitly on the next invocation.
    if validate_unlabelled_cleanup_marker(path).is_err() {
        return;
    }
    let _ = make_tree_writable(path);
    let _ = fs::remove_dir_all(path);
}

fn validate_unlabelled_cleanup_marker(path: &Path) -> Result<()> {
    let marker_path = staging_marker_path(path);
    let metadata = match fs::symlink_metadata(&marker_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // Maintenance rehearsal publication has a separate marker name.
            // Treat it as an ownership claim too, so an error-path cleanup
            // cannot remove a valid-looking rehearsal (or a foreign one)
            // merely because the replacement marker was not written yet.
            match fs::symlink_metadata(path.join(PROFILE_REHEARSAL_MARKER_FILENAME)) {
                Ok(_) => return validate_maintenance_rehearsal_staging_marker(path),
                Err(marker_error) if marker_error.kind() == std::io::ErrorKind::NotFound => {
                    if is_unowned_maintenance_staging_path(path) {
                        return Err(config_error(format!(
                            "replacement maintenance staging '{}' has no ownership marker; refusing cleanup",
                            path.display()
                        )));
                    }
                    if is_operation_shaped_staging_path(path) {
                        return Err(config_error(format!(
                            "replacement staging '{}' has no operation ownership marker; refusing cleanup",
                            path.display()
                        )));
                    }
                    return Ok(());
                }
                Err(marker_error) => {
                    return Err(config_error(format!(
                        "inspect maintenance rehearsal marker in '{}' before best-effort cleanup: {marker_error}",
                        path.display()
                    )));
                }
            }
        }
        Err(error) => {
            return Err(config_error(format!(
                "inspect replacement staging marker '{}' before best-effort cleanup: {error}",
                marker_path.display()
            )));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(config_error(format!(
            "replacement staging marker '{}' must be a regular file",
            marker_path.display()
        )));
    }
    let marker: ReplacementStagingMarker =
        serde_json::from_slice(&fs::read(&marker_path).map_err(|error| {
            config_error(format!(
                "read replacement staging marker '{}' before best-effort cleanup: {error}",
                marker_path.display()
            ))
        })?)
        .map_err(|error| {
            config_error(format!(
                "decode replacement staging marker '{}' before best-effort cleanup: {error}",
                marker_path.display()
            ))
        })?;
    if marker.schema_version != STAGING_MARKER_SCHEMA_VERSION
        || !marker.operation_id.starts_with("replace-v1-")
        || !is_single_path_component(&marker.operation_id)
        || !staging_path_matches_kind(path, &marker.operation_id, &marker.kind)
    {
        return Err(config_error(format!(
            "replacement staging marker '{}' is foreign to its path",
            marker_path.display()
        )));
    }
    if let Some(expected_digest) = marker.expected_digest.as_deref() {
        ensure_tree_digest(path, expected_digest, "best-effort cleanup")?;
    }
    Ok(())
}

fn is_unowned_maintenance_staging_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".tmp") || name.ends_with(".tracedecay-rehearsal"))
}

fn is_operation_shaped_staging_path(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let Some(rest) = name.strip_prefix('.') else {
        return false;
    };
    let Some((operation_id, suffix)) = rest.rsplit_once('.') else {
        return false;
    };
    operation_id.starts_with("replace-v1-")
        && is_single_path_component(operation_id)
        && matches!(
            suffix,
            "worker"
                | "target"
                | "backup-rehearsal"
                | "source-view"
                | "backup-view"
                | "v1-preserved"
                | "v2-failed"
        )
}

fn cleanup_worker_views(source_view_root: &Path, backup_view_root: &Path) {
    cleanup_owned_directory(source_view_root);
    cleanup_owned_directory(backup_view_root);
}

fn remove_replacement_workspace(
    target_root: &Path,
    rehearsal_root: &Path,
    rehearsal_staging_root: &Path,
    rollback_root: &Path,
    source_view_root: &Path,
    backup_view_root: &Path,
    worker_stage_root: &Path,
) -> Result<()> {
    remove_owned_directory(target_root, "V2 staging target")?;
    remove_owned_directory(rehearsal_root, "backup rehearsal target")?;
    remove_owned_directory(rehearsal_staging_root, "backup rehearsal staging")?;
    remove_owned_directory(rollback_root, "V2 rollback quarantine target")?;
    remove_owned_directory(source_view_root, "read-only source view")?;
    remove_owned_directory(backup_view_root, "read-only backup view")?;
    remove_owned_directory(worker_stage_root, "pinned migration worker")
}

fn replacement_operation_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("replace-v1-{nanos}-{}", std::process::id())
}

fn replacement_error(
    phase: &str,
    detail: String,
    backup_root: Option<&Path>,
    quarantine_root: Option<&Path>,
) -> TraceDecayError {
    let backup = backup_root.map_or_else(
        || "no external backup was published".to_owned(),
        |path| format!("the untouched external backup is at '{}'", path.display()),
    );
    let preserved = quarantine_root.map_or_else(String::new, |path| {
        format!(" The preserved V1 namespace is at '{}'.", path.display())
    });
    config_error(format!(
        "V1-to-V2 replacement failed during {phase}: {detail}. {backup}.{preserved} \
         Downgrade guidance: stop V2, select the V1 binary/service, and restore the untouched \
         backup; never open V2-created files with V1"
    ))
}

fn config_error(message: impl Into<String>) -> TraceDecayError {
    TraceDecayError::Config {
        message: message.into(),
    }
}

fn bounded_text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).trim().to_owned()
}

fn is_sha256_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(phase: WorkerPhase, source: &Path, target: &Path) -> WorkerReport {
        WorkerReport {
            protocol: REPLACEMENT_PROTOCOL.to_owned(),
            protocol_version: REPLACEMENT_PROTOCOL_VERSION,
            operation_id: "replace-v1-test".to_owned(),
            provider: "native".to_owned(),
            phase,
            committed: !matches!(phase, WorkerPhase::Preflight),
            exact: matches!(phase, WorkerPhase::Verify),
            authorities: REQUIRED_AUTHORITIES
                .iter()
                .map(|name| (*name).to_owned())
                .collect(),
            target_digest: profile_tree_digest(target).expect("target digest"),
            authority_manifest: authority_manifest(source, target).expect("authority manifest"),
            opaque_manifest: opaque_manifest(source, target).expect("opaque manifest"),
            external_projects: external_project_manifest(source, target)
                .expect("external project manifest"),
        }
    }

    #[test]
    fn worker_receipt_requires_every_authority_and_matching_phase() {
        let temp = tempfile::tempdir().expect("temporary target");
        let source = temp.path().join("source");
        let target = temp.path().join("target");
        fs::create_dir(&source).expect("source directory");
        fs::create_dir(&target).expect("target directory");
        for phase in [
            WorkerPhase::Preflight,
            WorkerPhase::Apply,
            WorkerPhase::Verify,
        ] {
            validate_worker_report(
                report(phase, &source, &target),
                phase,
                "replace-v1-test",
                "native",
                &source,
                &target,
            )
            .expect("complete worker receipt should validate");
        }

        let mut incomplete = report(WorkerPhase::Preflight, &source, &target);
        incomplete.authorities.pop();
        assert!(
            validate_worker_report(
                incomplete,
                WorkerPhase::Preflight,
                "replace-v1-test",
                "native",
                &source,
                &target,
            )
            .is_err()
        );
    }

    #[test]
    fn worker_receipt_rejects_commit_claim_from_read_only_preflight() {
        let temp = tempfile::tempdir().expect("temporary target");
        let source = temp.path().join("source");
        let target = temp.path().join("target");
        fs::create_dir(&source).expect("source directory");
        fs::create_dir(&target).expect("target directory");
        let mut report = report(WorkerPhase::Preflight, &source, &target);
        report.committed = true;
        assert!(
            validate_worker_report(
                report,
                WorkerPhase::Preflight,
                "replace-v1-test",
                "native",
                &source,
                &target,
            )
            .is_err()
        );
    }

    #[test]
    fn worker_receipt_rejects_independent_target_digest_mismatch() {
        let temp = tempfile::tempdir().expect("temporary target");
        let source = temp.path().join("source");
        let target = temp.path().join("target");
        fs::create_dir(&source).expect("source directory");
        fs::create_dir(&target).expect("target directory");
        let mut report = report(WorkerPhase::Verify, &source, &target);
        report.target_digest = "0".repeat(64);
        assert!(
            validate_worker_report(
                report,
                WorkerPhase::Verify,
                "replace-v1-test",
                "native",
                &source,
                &target,
            )
            .is_err()
        );
    }

    #[test]
    fn backup_id_must_be_one_path_component() {
        assert!(validate_path_component("backup-1", "backup id").is_ok());
        for value in ["", "/tmp/backup", "../backup", "a/b", "."] {
            assert!(
                validate_path_component(value, "backup id").is_err(),
                "{value:?}"
            );
        }
    }

    #[test]
    fn missing_backup_parent_cannot_be_nested_under_the_profile() {
        let temp = tempfile::tempdir().expect("temporary directory");
        let profile = temp.path().join("profile");
        fs::create_dir(&profile).expect("profile directory");
        let nested = profile.join("future").join("backups");
        assert!(validate_backup_parent(&profile, &nested).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn dangling_backup_parent_symlink_is_rejected() {
        let temp = tempfile::tempdir().expect("temporary directory");
        let profile = temp.path().join("profile");
        fs::create_dir(&profile).expect("profile directory");
        let backup_parent = temp.path().join("backup-link");
        std::os::unix::fs::symlink(profile.join("missing"), &backup_parent)
            .expect("dangling backup symlink");
        assert!(validate_backup_parent(&profile, &backup_parent).is_err());
    }

    #[test]
    fn namespace_verification_rejects_symlinked_authority() {
        let temp = tempfile::tempdir().expect("temporary directory");
        let root = temp.path().join("profile");
        fs::create_dir(&root).expect("profile directory");
        for logical in REQUIRED_AUTHORITIES {
            let path = root.join(logical);
            if matches!(*logical, "projects" | "migration-inventory") {
                fs::create_dir_all(path).expect("authority directory");
            } else {
                fs::write(path, b"fixture").expect("authority file");
            }
        }
        #[cfg(unix)]
        {
            fs::remove_file(root.join("config.toml")).expect("remove authority fixture");
            std::os::unix::fs::symlink(root.join("global.db"), root.join("config.toml"))
                .expect("authority symlink");
            assert!(verify_profile_namespace(&root, true).is_err());
        }
    }

    #[test]
    fn released_schema_admission_uses_immutable_beta37_inventories() {
        let v34 = read_released_schema_fixture(RELEASED_V34_PROJECT_STORE_SQL, &[])
            .expect("released v34 fixture");
        let v35 = read_released_schema_fixture(RELEASED_V35_PROJECT_STORE_SQL, &[])
            .expect("released v35 fixture");
        let v35_with_payload_bridge = read_released_schema_fixture(
            RELEASED_V34_PROJECT_STORE_SQL,
            &[RELEASED_V35_PAYLOAD_DIGESTS_SQL],
        )
        .expect("released v35 payload bridge fixture");

        assert_eq!(v34.len(), 183);
        assert_eq!(v35.len(), 190);
        assert_eq!(v35_with_payload_bridge.len(), 187);
        assert!(
            v34.iter()
                .any(|object| object.name == "retrieval_anchor_aliases")
        );
        assert!(
            v35.iter()
                .any(|object| object.name == "memory_v2_assertion_payload_digests")
        );
        assert_ne!(v34, v35);
    }

    #[test]
    fn staging_marker_binds_operation_and_tree_digest() {
        let temp = tempfile::tempdir().expect("temporary staging parent");
        let staging = temp.path().join(".replace-v1-test.target");
        fs::create_dir(&staging).expect("staging directory");
        fs::write(staging.join("payload.bin"), b"opaque bytes").expect("payload");
        let digest = profile_tree_digest(&staging).expect("staging digest");

        bind_staging_directory(&staging, "replace-v1-test", "target", Some(&digest), false)
            .expect("bind staging marker");
        validate_cleanup_staging_marker(&staging, "V2 staging target")
            .expect("owned marker should authorize cleanup");
        validate_staging_directory(&staging, "replace-v1-test", "target", Some(&digest))
            .expect("validate staging marker");
        assert!(
            validate_staging_directory(&staging, "replace-v1-foreign", "target", Some(&digest),)
                .is_err()
        );
        assert!(
            validate_staging_directory(&staging, "replace-v1-test", "source-view", Some(&digest),)
                .is_err()
        );
        remove_staging_marker_for_operation(&staging, "replace-v1-test")
            .expect("remove owned marker");
        assert!(!staging_marker_path(&staging).exists());
    }

    #[test]
    fn recovery_completes_a_marker_written_before_content_digest() {
        let temp = tempfile::tempdir().expect("temporary staging parent");
        let staging = temp.path().join(".replace-v1-test.target");
        fs::create_dir(&staging).expect("staging directory");
        fs::write(staging.join("payload.bin"), b"durable payload").expect("payload");

        // Simulate a crash after ownership metadata reached disk but before
        // the content binding was checkpointed. Recovery must bind the
        // observed tree exactly once before any cleanup/adoption path uses it.
        bind_staging_directory(&staging, "replace-v1-test", "target", None, false)
            .expect("bind marker without digest");
        let expected = profile_tree_digest(&staging).expect("staging digest");
        complete_staging_digest_binding(
            &staging,
            "replace-v1-test",
            "target",
            Some(&expected),
            false,
        )
        .expect("complete staging digest binding");
        validate_staging_directory(&staging, "replace-v1-test", "target", Some(&expected))
            .expect("completed marker should validate");
        let marker: ReplacementStagingMarker = serde_json::from_slice(
            &fs::read(staging_marker_path(&staging)).expect("read completed marker"),
        )
        .expect("decode completed marker");
        assert_eq!(marker.expected_digest.as_deref(), Some(expected.as_str()));
    }

    #[test]
    fn maintenance_staging_without_ownership_marker_is_never_removed() {
        let temp = tempfile::tempdir().expect("temporary staging parent");
        let external = temp.path().join(".backup.tmp");
        fs::create_dir(&external).expect("external backup staging");
        assert!(
            validate_cleanup_staging_marker(&external, "external backup staging").is_err(),
            "the maintenance backup API does not expose an operation marker for an interrupted staging tree"
        );

        let rehearsal = temp.path().join(".rehearsal.tracedecay-rehearsal");
        fs::create_dir(&rehearsal).expect("rehearsal staging");
        assert!(
            validate_cleanup_staging_marker(&rehearsal, "backup rehearsal staging").is_err(),
            "a rehearsal staging tree without the maintenance marker is not owned"
        );
    }

    #[test]
    fn operation_shaped_staging_without_ownership_marker_is_never_removed() {
        let temp = tempfile::tempdir().expect("temporary staging parent");
        let staging = temp.path().join(".replace-v1-test.target");
        fs::create_dir(&staging).expect("staging directory");
        assert!(
            validate_cleanup_staging_marker(&staging, "V2 staging target").is_err(),
            "an operation-shaped target without its marker is not owned"
        );
        assert!(
            validate_unlabelled_cleanup_marker(&staging).is_err(),
            "best-effort cleanup must also refuse an unmarked operation directory"
        );
    }

    #[test]
    fn exchanged_v1_target_cleanup_requires_exact_source_digest() {
        let temp = tempfile::tempdir().expect("temporary profile parent");
        let exchanged = temp.path().join(".replace-v1-test.target");
        fs::create_dir(&exchanged).expect("exchanged source directory");
        fs::write(exchanged.join("opaque.bin"), b"preserved V1 bytes").expect("source bytes");
        let source_digest = profile_tree_digest(&exchanged).expect("source digest");
        remove_exchanged_source_directory(&exchanged, &source_digest, "replace-v1-test", "target")
            .expect("authenticated exchanged source cleanup");
        assert!(!exchanged.exists());

        let foreign = temp.path().join(".replace-v1-test.target");
        fs::create_dir(&foreign).expect("foreign directory");
        fs::write(foreign.join("opaque.bin"), b"foreign bytes").expect("foreign bytes");
        assert!(
            remove_exchanged_source_directory(
                &foreign,
                &source_digest,
                "replace-v1-test",
                "target"
            )
            .is_err(),
            "a same-path foreign directory must fail the source digest check"
        );
    }

    #[test]
    fn target_preflight_treats_only_its_marker_as_empty() {
        let temp = tempfile::tempdir().expect("temporary staging parent");
        let staging = temp.path().join("staging");
        fs::create_dir(&staging).expect("staging directory");
        bind_staging_directory(&staging, "replace-v1-test", "target", None, false)
            .expect("bind empty target");
        assert!(directory_is_empty(&staging).expect("inspect empty target"));
        fs::write(staging.join("unexpected.db"), b"unexpected").expect("unexpected payload");
        assert!(!directory_is_empty(&staging).expect("inspect non-empty target"));
    }

    #[test]
    fn read_only_authority_materialization_is_writable_on_resume() {
        let temp = tempfile::tempdir().expect("temporary authority parent");
        let source = temp.path().join("source.db");
        let destination = temp.path().join("destination.db");
        fs::write(&source, b"v1 bytes").expect("source bytes");
        fs::write(&destination, b"partial bytes").expect("partial target bytes");
        make_tree_read_only(&destination).expect("freeze partial target");
        copy_first_party_authority(&source, &destination).expect("replace frozen partial target");
        assert_eq!(
            fs::read(destination).expect("materialized target"),
            b"v1 bytes"
        );
    }

    #[test]
    fn sqlite_sidecars_are_folded_out_of_replacement_tree_identity() {
        let temp = tempfile::tempdir().expect("temporary profile");
        let root = temp.path().join("profile");
        fs::create_dir(&root).expect("profile directory");
        // The sidecar rule is deliberately header based: a provider-owned
        // file named `*.db` remains opaque unless it is an actual SQLite
        // database. Create a real owner here so the WAL is folded into the
        // serving database identity as the maintenance backup contract does.
        let connection = rusqlite::Connection::open(root.join("global.db")).expect("database");
        connection
            .execute_batch(
                "CREATE TABLE facts (id INTEGER PRIMARY KEY); INSERT INTO facts VALUES (1);",
            )
            .expect("database schema");
        drop(connection);
        fs::write(root.join("global.db-wal"), b"first wal").expect("wal");
        let first = profile_tree_digest(&root).expect("first digest");
        fs::write(root.join("global.db-wal"), b"second wal").expect("wal update");
        let second = profile_tree_digest(&root).expect("second digest");
        assert_eq!(first, second);
    }

    #[test]
    fn opaque_suffix_files_without_a_database_owner_are_preserved() {
        let temp = tempfile::tempdir().expect("temporary profile");
        let root = temp.path().join("profile");
        fs::create_dir_all(root.join("provider")).expect("provider directory");
        let payload = root.join("provider/receipt-wal");
        fs::write(&payload, b"opaque receipt").expect("opaque receipt");
        assert!(!is_sqlite_sidecar_path(&payload));
        let first = profile_tree_digest(&root).expect("first digest");
        fs::write(&payload, b"changed opaque receipt").expect("changed receipt");
        let second = profile_tree_digest(&root).expect("second digest");
        assert_ne!(first, second);
    }

    #[test]
    fn opaque_project_db_suffix_is_not_admitted_as_sqlite() {
        let temp = tempfile::tempdir().expect("temporary profile");
        let projects = temp.path().join("projects");
        let provider_root = projects.join("provider");
        fs::create_dir_all(&provider_root).expect("provider project directory");
        fs::write(provider_root.join("opaque-provider.db"), b"provider bytes")
            .expect("opaque provider database-shaped payload");

        validate_sqlite_files_below(&projects, "opaque project fixture")
            .expect("unknown .db payloads remain opaque and admissible");
        assert!(
            !is_sqlite_database_file(&provider_root.join("opaque-provider.db"))
                .expect("inspect opaque provider payload")
        );
    }

    #[test]
    fn project_authority_manifest_includes_nested_sqlite_rows() {
        let temp = tempfile::tempdir().expect("temporary project authority");
        let projects = temp.path().join("projects");
        let store = projects.join("project-a");
        fs::create_dir_all(&store).expect("project store");
        let database = store.join("tracedecay.db");
        let connection = rusqlite::Connection::open(&database).expect("project database");
        connection
            .execute_batch(
                "CREATE TABLE facts (fact_id TEXT PRIMARY KEY, content TEXT NOT NULL); \
                 INSERT INTO facts(fact_id, content) VALUES ('fact-a', 'preserved');",
            )
            .expect("project rows");
        drop(connection);

        let (_, rows, tables) = authority_semantic_observation(
            temp.path(),
            "projects",
            &authority_observation(temp.path(), "projects")
                .expect("project authority observation")
                .0,
        )
        .expect("project semantic observation");
        assert_eq!(rows, 1);
        assert!(tables.iter().any(|table| {
            table.table.ends_with(":facts") && table.rows == 1 && table.columns.len() == 2
        }));
    }

    #[cfg(unix)]
    #[test]
    fn physical_path_identity_rejects_a_symlink_alias() {
        let temp = tempfile::tempdir().expect("temporary path parent");
        let real = temp.path().join("real");
        let alias = temp.path().join("alias");
        fs::create_dir(&real).expect("real directory");
        std::os::unix::fs::symlink(&real, &alias).expect("directory alias");
        assert!(paths_overlap(&real, &alias).expect("compare physical paths"));
    }

    #[cfg(unix)]
    #[test]
    fn profile_root_accepts_a_macos_style_parent_alias() {
        let temp = tempfile::tempdir().expect("temporary path parent");
        let real_parent = temp.path().join("real-parent");
        let alias_parent = temp.path().join("alias-parent");
        fs::create_dir(&real_parent).expect("real parent");
        fs::create_dir(real_parent.join("profile")).expect("profile directory");
        std::os::unix::fs::symlink(&real_parent, &alias_parent).expect("parent alias");

        // `/private/var` and `/var` on macOS (and a symlinked parent in this
        // fixture) are different spellings of one physical profile. The
        // profile itself remains a regular directory, so rejecting only a
        // root symlink is still enough to keep the selector unambiguous.
        assert!(validate_profile_root_path(&alias_parent.join("profile")).is_ok());
    }

    #[cfg(windows)]
    #[test]
    fn replacement_admission_fails_closed_on_windows() {
        let error = ensure_supported_replacement_platform()
            .expect_err("Windows has no durable directory exchange in this release");
        assert!(error.to_string().contains("no mutation was started"));
    }

    #[test]
    fn publication_rollback_is_entrywise_and_crash_idempotent() {
        let temp = tempfile::tempdir().expect("temporary replacement parent");
        let parent = temp.path();
        let profile = parent.join("profile");
        let target = parent.join(".replace-v1-test.target");
        let rehearsal = parent.join(".replace-v1-test.backup-rehearsal");
        let quarantine = parent.join(".replace-v1-test.v1-preserved");
        let rollback = parent.join(".replace-v1-test.v2-failed");
        let rehearsal_staging =
            parent.join(".replace-v1-test.backup-rehearsal.tracedecay-rehearsal");
        let source_view = parent.join(".replace-v1-test.source-view");
        let backup_view = parent.join(".replace-v1-test.backup-view");
        let worker_stage = parent.join(".replace-v1-test.worker");
        let journal_path = profile.join(JOURNAL_FILENAME);

        for root in [&profile, &target, &rehearsal] {
            fs::create_dir(root).expect("replacement namespace");
            fs::create_dir(root.join("projects")).expect("projects authority");
            fs::create_dir(root.join("migration-inventory")).expect("inventory authority");
            for authority in [
                "global.db",
                "user-sessions.db",
                "user-memory.db",
                "enrollment.json",
                "config.toml",
                "profile-identity.json",
            ] {
                fs::write(root.join(authority), authority.as_bytes()).expect("authority bytes");
            }
            fs::write(
                root.join("migration-inventory/fixture.json"),
                b"opaque inventory",
            )
            .expect("inventory bytes");
        }
        fs::write(profile.join(LIFECYCLE_LOCK_FILENAME), b"held lock").expect("lifecycle lock");
        fs::write(
            target.join(FIRST_PARTY_MANIFEST_FILENAME),
            br#"{"schema_version":1}"#,
        )
        .expect("target manifest");

        let target_digest = profile_tree_digest(&target).expect("target digest");
        bind_staging_directory(
            &target,
            "replace-v1-test",
            "target",
            Some(&target_digest),
            false,
        )
        .expect("bind target");
        let rehearsal_digest = profile_tree_digest(&rehearsal).expect("rehearsal digest");
        bind_staging_directory(
            &rehearsal,
            "replace-v1-test",
            "rehearsal",
            Some(&rehearsal_digest),
            false,
        )
        .expect("bind rehearsal");
        let source_digest = profile_tree_digest(&profile).expect("source digest");
        let mut journal = ReplacementJournal {
            schema_version: JOURNAL_SCHEMA_VERSION,
            operation_id: "replace-v1-test".to_owned(),
            provider: "native".to_owned(),
            profile_root: profile.clone(),
            backup_root: parent.join("external-backup"),
            backup_staging_root: parent.join(".external-backup.tmp"),
            source_view_root: source_view,
            backup_view_root: backup_view,
            worker_stage_root: worker_stage,
            worker_sha256: "0".repeat(64),
            target_root: target.clone(),
            rehearsal_root: rehearsal.clone(),
            rehearsal_staging_root: rehearsal_staging,
            quarantine_root: quarantine.clone(),
            rollback_root: rollback.clone(),
            phase: ReplacementPhase::Prepared,
            source_tree_digest: source_digest,
            backup_tree_digest: None,
            backup_manifest_sha256: None,
            backup_identity_sha256: None,
            staged_target_digest: Some(target_digest),
            authority_progress: Vec::new(),
            source_external_projects: Vec::new(),
            service: None,
        };
        write_journal(&journal_path, &journal).expect("prepare journal");

        publish_target(
            &profile,
            parent,
            &target,
            &quarantine,
            &journal_path,
            &mut journal,
        )
        .expect("publish target");
        assert!(has_first_party_manifest(&profile).expect("published target manifest"));
        assert!(quarantine.is_dir(), "source is retained before rollback");
        assert!(
            !quarantine.join(JOURNAL_FILENAME).exists(),
            "cutover journal metadata must not pollute the preserved V1 tree"
        );

        rollback_from_rehearsal(&profile, &rehearsal, &rollback, &journal_path, &mut journal)
            .expect("rollback published target");
        assert!(!has_first_party_manifest(&profile).expect("restored V1 manifest"));
        assert!(rollback.is_dir(), "failed V2 remains quarantined");
        assert!(
            !rollback.join(JOURNAL_FILENAME).exists(),
            "cutover journal metadata must not pollute the failed V2 quarantine"
        );
        let restored_digest = profile_tree_digest(&profile).expect("restored V1 digest");
        let rehearsal_source_digest = profile_tree_digest(&quarantine).expect("quarantine digest");
        assert_eq!(restored_digest, rehearsal_source_digest);

        // Replaying the rollback boundary after a crash is harmless: the V1
        // tree is already serving and the V2 quarantine remains untouched.
        rollback_from_rehearsal(&profile, &rehearsal, &rollback, &journal_path, &mut journal)
            .expect("replay rollback boundary");
        assert_eq!(
            profile_tree_digest(&profile).expect("replayed V1 digest"),
            restored_digest
        );
    }

    #[cfg(unix)]
    #[test]
    fn first_party_migration_timeout_terminates_the_worker_process_group() {
        let temp = tempfile::tempdir().expect("temporary worker parent");
        let worker = temp.path().join("stuck-worker");
        fs::write(&worker, b"#!/bin/sh\nsleep 30\n").expect("worker script");
        let mut permissions = fs::metadata(&worker)
            .expect("worker metadata")
            .permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&worker, permissions).expect("worker executable");
        let worker_sha256 = sha256_worker(&worker).expect("worker digest");
        let error = run_landed_profile_migrations(
            temp.path(),
            &worker,
            &worker_sha256,
            Duration::from_millis(100),
        )
        .expect_err("stuck migration must time out");
        assert!(error.to_string().contains("terminated"));
    }
}
