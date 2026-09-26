//! Offline identity and executable verification for the native NCM worker.

use crate::wire::{PROTOCOL_IDENTITY, PROTOCOL_VERSION};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// The logical worker name recorded in the NCM worker manifest.
///
/// Use [`WORKER_EXECUTABLE_NAME`] when building a filesystem path.
pub const WORKER_NAME: &str = "tracedecay-ncm-worker";
/// The worker executable file name on the compilation target.
///
/// This is [`WORKER_NAME`] followed by [`std::env::consts::EXE_SUFFIX`].
#[cfg(windows)]
pub const WORKER_EXECUTABLE_NAME: &str = "tracedecay-ncm-worker.exe";
/// The worker executable file name on the compilation target.
///
/// This is [`WORKER_NAME`] followed by [`std::env::consts::EXE_SUFFIX`].
#[cfg(not(windows))]
pub const WORKER_EXECUTABLE_NAME: &str = "tracedecay-ncm-worker";
/// The manifest file name expected beside an installed worker executable.
pub const WORKER_MANIFEST_NAME: &str = "worker-manifest.json";
const MANIFEST_SCHEMA_VERSION: u16 = 1;
const TRUSTED_MANIFEST: &str =
    include_str!(concat!(env!("OUT_DIR"), "/trusted-worker-manifest.json"));
const CURRENT_TARGET_TRIPLE: &str = env!("TRACEDECAY_NCM_TARGET_TRIPLE");

/// Returns the worker manifest this build trusts.
///
/// The build script selects it from the absolute path in
/// `TRACEDECAY_NCM_WORKER_MANIFEST` or, when that variable is unset, from the
/// checked-in `product/ncm/reference/worker-manifest.json`, which pins no
/// worker. A target that the returned manifest does not pin has no admissible
/// worker: staging fails with [`WorkerIntegrityError::UnsupportedTarget`] and
/// [`crate::platform::current_worker_platform_capability`] reports the target
/// as unsupported. Consumers must use this text instead of reading a manifest
/// file so that one build has exactly one trust root.
#[must_use]
pub fn trusted_worker_manifest() -> &'static str {
    TRUSTED_MANIFEST
}

/// Returns the Rust target triple this crate was compiled for.
#[must_use]
pub fn current_target_triple() -> &'static str {
    CURRENT_TARGET_TRIPLE
}

/// A checked-in executable identity could not be established or matched.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerIntegrityError {
    /// The checked-in manifest is malformed or internally inconsistent.
    Manifest(String),
    /// The current compilation target has no admitted worker artifact.
    UnsupportedTarget(String),
    /// The configured path is not a regular file.
    NotRegularFile,
    /// The configured file has no executable permission on Unix.
    NotExecutable,
    /// The configured file could not be read.
    Read(String),
    /// The configured file length differs from the pinned artifact.
    ///
    /// The fields contain the expected and observed byte counts, respectively.
    SizeMismatch {
        /// Pinned artifact size in bytes.
        expected: u64,
        /// Observed artifact size in bytes.
        actual: u64,
    },
    /// The configured file digest differs from the pinned artifact.
    ///
    /// The fields contain the expected and observed hexadecimal digests,
    /// respectively.
    DigestMismatch {
        /// Pinned artifact SHA-256 digest.
        expected: String,
        /// Observed artifact SHA-256 digest.
        actual: String,
    },
    /// The verified bytes could not be sealed into a private launch artifact.
    Staging(String),
}

impl fmt::Display for WorkerIntegrityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Manifest(detail) => write!(formatter, "worker manifest: {detail}"),
            Self::UnsupportedTarget(target) => {
                write!(formatter, "worker target is not supported: {target}")
            }
            Self::NotRegularFile => formatter.write_str("worker path is not a regular file"),
            Self::NotExecutable => formatter.write_str("worker path is not executable"),
            Self::Read(detail) => write!(formatter, "read worker artifact: {detail}"),
            Self::SizeMismatch { expected, actual } => write!(
                formatter,
                "worker artifact size mismatch: expected {expected} bytes, got {actual}"
            ),
            Self::DigestMismatch { expected, actual } => write!(
                formatter,
                "worker artifact digest mismatch: expected {expected}, got {actual}"
            ),
            Self::Staging(detail) => write!(formatter, "stage verified worker: {detail}"),
        }
    }
}

impl std::error::Error for WorkerIntegrityError {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerManifest {
    schema_version: u16,
    worker: String,
    protocol_version: u16,
    protocol_identity: String,
    targets: Vec<WorkerTarget>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerTarget {
    triple: String,
    os: String,
    arch: String,
    family: String,
    bytes: u64,
    sha256: String,
}

/// Platform identity that a pinned manifest target must match exactly.
pub(crate) struct CurrentTarget {
    pub(crate) triple: &'static str,
    pub(crate) os: &'static str,
    pub(crate) arch: &'static str,
    pub(crate) family: &'static str,
}

pub(crate) struct WorkerArtifactIdentity {
    pub(crate) sha256: String,
    pub(crate) bytes: u64,
    pub(crate) triple: &'static str,
    pub(crate) os: &'static str,
    pub(crate) arch: &'static str,
    pub(crate) family: &'static str,
}

struct WorkerContentIdentity {
    sha256: String,
    bytes: u64,
}

/// Owns one verified worker copy for exactly one process lifetime.
///
/// The source path is never passed to the child process. The private directory
/// and create-new file make the launch pathname stable after verification, and
/// the file is made read-only before it is returned. Dropping this value
/// removes the staging artifact after the caller has reaped its child.
pub struct VerifiedWorkerArtifact {
    _directory: TempDir,
    path: PathBuf,
    digest: String,
    manifest: Vec<u8>,
    manifest_digest: String,
}

impl VerifiedWorkerArtifact {
    /// Returns the private staged executable path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the lowercase SHA-256 digest of the verified executable bytes.
    #[must_use]
    pub fn sha256(&self) -> &str {
        &self.digest
    }

    /// Returns the exact manifest bytes verified beside the source executable.
    ///
    /// The bytes are retained so an installer can publish the manifest beside
    /// the staged executable without reopening a potentially changed source
    /// path. The slice is empty only for the crate-private unit-test helper
    /// that stages an already-selected target without a manifest.
    #[must_use]
    pub fn manifest_bytes(&self) -> &[u8] {
        &self.manifest
    }

    /// Returns the canonical JSON SHA-256 digest of the verified manifest.
    #[must_use]
    pub fn manifest_sha256(&self) -> &str {
        &self.manifest_digest
    }
}

/// Verifies the configured worker against the build's trusted manifest (see
/// [`trusted_worker_manifest`]) and seals the exact hashed bytes into a
/// private launch artifact.
///
/// This function performs only local reads, hashing, and private staging. It
/// does not resolve, download, or replace an executable or any model artifact.
pub fn stage_verified_worker_binary(
    path: &Path,
) -> Result<VerifiedWorkerArtifact, WorkerIntegrityError> {
    let manifest_path = manifest_path(path)?;
    stage_verified_worker_binary_at(path, &manifest_path, TRUSTED_MANIFEST)
}

fn stage_verified_worker_binary_at(
    path: &Path,
    manifest_path: &Path,
    trusted_manifest_text: &str,
) -> Result<VerifiedWorkerArtifact, WorkerIntegrityError> {
    let trusted_manifest = parse_manifest(trusted_manifest_text, "embedded trusted manifest")?;
    validate_manifest(&trusted_manifest)?;
    let manifest_text = open_worker_manifest(manifest_path)?;
    let manifest = parse_manifest(&manifest_text, &manifest_path.display().to_string())?;
    validate_manifest(&manifest)?;
    let expected_manifest_digest = canonical_manifest_digest(trusted_manifest_text)?;
    let actual_manifest_digest = canonical_manifest_digest(&manifest_text)?;
    if actual_manifest_digest != expected_manifest_digest {
        return Err(WorkerIntegrityError::Manifest(format!(
            "manifest is stale or not sourced from the trusted build root: expected {expected_manifest_digest}, got {actual_manifest_digest}"
        )));
    }
    let target = pinned_target(&manifest, &current_target())?;

    let file = open_worker_binary(path)?;
    let mut artifact = stage_verified_file(file, target)?;
    artifact.manifest = manifest_text.into_bytes();
    artifact.manifest_digest = actual_manifest_digest;
    Ok(artifact)
}

fn stage_verified_file(
    file: File,
    target: &WorkerTarget,
) -> Result<VerifiedWorkerArtifact, WorkerIntegrityError> {
    let bytes = validate_open_worker(&file)?;
    if bytes != target.bytes {
        return Err(WorkerIntegrityError::SizeMismatch {
            expected: target.bytes,
            actual: bytes,
        });
    }

    let staging =
        TempDir::new().map_err(|error| WorkerIntegrityError::Staging(error.to_string()))?;
    // `TempDir` honors the process umask and can therefore be group/world
    // readable. Tighten the exact directory handle before creating the
    // staged worker so the artifact remains owner-private for its lifetime.
    tracedecay_private_fs::make_private_directory(staging.path())
        .map_err(|error| WorkerIntegrityError::Staging(error.to_string()))?;
    let staged_path = staging.path().join(WORKER_EXECUTABLE_NAME);
    let mut staged = tracedecay_private_fs::create_private_file(&staged_path)
        .map_err(|error| WorkerIntegrityError::Staging(error.to_string()))?;
    let identity = measure_open_worker(file, bytes, &mut staged)?;
    if identity.sha256 != target.sha256 {
        return Err(WorkerIntegrityError::DigestMismatch {
            expected: target.sha256.clone(),
            actual: identity.sha256,
        });
    }
    staged
        .sync_all()
        .map_err(|error| WorkerIntegrityError::Staging(error.to_string()))?;
    seal_staged_worker(&staged)?;
    Ok(VerifiedWorkerArtifact {
        _directory: staging,
        path: staged_path,
        digest: identity.sha256,
        manifest: Vec::new(),
        manifest_digest: String::new(),
    })
}

pub(crate) fn worker_artifact_identity(
    path: &Path,
) -> Result<WorkerArtifactIdentity, WorkerIntegrityError> {
    let file = open_worker_binary(path)?;
    let bytes = validate_open_worker(&file)?;
    let identity = measure_open_worker(file, bytes, std::io::sink())?;
    let target = current_target();
    Ok(WorkerArtifactIdentity {
        sha256: identity.sha256,
        bytes: identity.bytes,
        triple: target.triple,
        os: target.os,
        arch: target.arch,
        family: target.family,
    })
}

fn validate_open_worker(file: &File) -> Result<u64, WorkerIntegrityError> {
    let metadata = file
        .metadata()
        .map_err(|error| WorkerIntegrityError::Read(error.to_string()))?;
    if !metadata.is_file() {
        return Err(WorkerIntegrityError::NotRegularFile);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o111 == 0 {
            return Err(WorkerIntegrityError::NotExecutable);
        }
    }
    Ok(metadata.len())
}

fn measure_open_worker(
    mut file: File,
    expected_bytes: u64,
    mut copy: impl Write,
) -> Result<WorkerContentIdentity, WorkerIntegrityError> {
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| WorkerIntegrityError::Read(error.to_string()))?;
        if read == 0 {
            break;
        }
        bytes = bytes.saturating_add(read as u64);
        digest.update(&buffer[..read]);
        copy.write_all(&buffer[..read])
            .map_err(|error| WorkerIntegrityError::Staging(error.to_string()))?;
    }
    let final_bytes = file
        .metadata()
        .map_err(|error| WorkerIntegrityError::Read(error.to_string()))?
        .len();
    if bytes != expected_bytes {
        return Err(WorkerIntegrityError::SizeMismatch {
            expected: expected_bytes,
            actual: bytes,
        });
    }
    if final_bytes != expected_bytes {
        return Err(WorkerIntegrityError::SizeMismatch {
            expected: expected_bytes,
            actual: final_bytes,
        });
    }
    Ok(WorkerContentIdentity {
        sha256: hex_digest(&digest.finalize()),
        bytes,
    })
}

fn open_worker_binary(path: &Path) -> Result<File, WorkerIntegrityError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
        match options.open(path) {
            Ok(file) => Ok(file),
            Err(error) if error.raw_os_error() == Some(libc::ELOOP) => {
                Err(WorkerIntegrityError::NotRegularFile)
            }
            Err(error) => Err(WorkerIntegrityError::Read(error.to_string())),
        }
    }
    #[cfg(not(unix))]
    {
        let metadata = fs::symlink_metadata(path)
            .map_err(|error| WorkerIntegrityError::Read(error.to_string()))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(WorkerIntegrityError::NotRegularFile);
        }
        options
            .open(path)
            .map_err(|error| WorkerIntegrityError::Read(error.to_string()))
    }
}

fn open_worker_manifest(path: &Path) -> Result<String, WorkerIntegrityError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
        let mut file = match options.open(path) {
            Ok(file) => file,
            Err(error) if error.raw_os_error() == Some(libc::ELOOP) => {
                return Err(WorkerIntegrityError::Manifest(format!(
                    "worker manifest must be a regular sibling file: {}",
                    path.display()
                )));
            }
            Err(error) => return Err(WorkerIntegrityError::Read(error.to_string())),
        };
        let metadata = file
            .metadata()
            .map_err(|error| WorkerIntegrityError::Read(error.to_string()))?;
        if !metadata.is_file() {
            return Err(WorkerIntegrityError::Manifest(format!(
                "worker manifest must be a regular sibling file: {}",
                path.display()
            )));
        }
        let mut text = String::new();
        file.read_to_string(&mut text)
            .map_err(|error| WorkerIntegrityError::Read(error.to_string()))?;
        Ok(text)
    }
    #[cfg(not(unix))]
    {
        let metadata = fs::symlink_metadata(path)
            .map_err(|error| WorkerIntegrityError::Read(error.to_string()))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(WorkerIntegrityError::Manifest(format!(
                "worker manifest must be a regular sibling file: {}",
                path.display()
            )));
        }
        options
            .open(path)
            .and_then(|mut file| {
                let mut text = String::new();
                file.read_to_string(&mut text).map(|_| text)
            })
            .map_err(|error| WorkerIntegrityError::Read(error.to_string()))
    }
}

fn seal_staged_worker(file: &File) -> Result<(), WorkerIntegrityError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o500))
            .map_err(|error| WorkerIntegrityError::Staging(error.to_string()))?;
    }
    #[cfg(not(unix))]
    {
        let mut permissions = file
            .metadata()
            .map_err(|error| WorkerIntegrityError::Staging(error.to_string()))?
            .permissions();
        permissions.set_readonly(true);
        file.set_permissions(permissions)
            .map_err(|error| WorkerIntegrityError::Staging(error.to_string()))?;
    }
    file.sync_all()
        .map_err(|error| WorkerIntegrityError::Staging(error.to_string()))
}

fn manifest_path(binary: &Path) -> Result<PathBuf, WorkerIntegrityError> {
    let sibling = binary
        .parent()
        .map(|parent| parent.join(WORKER_MANIFEST_NAME))
        .ok_or_else(|| {
            WorkerIntegrityError::Manifest(
                "worker path has no parent for manifest binding".to_owned(),
            )
        })?;
    let metadata = match fs::symlink_metadata(&sibling) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(WorkerIntegrityError::Manifest(format!(
                "worker manifest must be beside the worker: {}",
                sibling.display()
            )));
        }
        Err(error) => return Err(WorkerIntegrityError::Read(error.to_string())),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(WorkerIntegrityError::Manifest(format!(
            "worker manifest must be a regular sibling file: {}",
            sibling.display()
        )));
    }
    Ok(sibling)
}

fn validate_manifest(manifest: &WorkerManifest) -> Result<(), WorkerIntegrityError> {
    if manifest.schema_version != MANIFEST_SCHEMA_VERSION {
        return Err(WorkerIntegrityError::Manifest(format!(
            "schema version {} is not supported",
            manifest.schema_version
        )));
    }
    if manifest.worker != WORKER_NAME {
        return Err(WorkerIntegrityError::Manifest(format!(
            "manifest names {}, expected {WORKER_NAME}",
            manifest.worker
        )));
    }
    if manifest.protocol_version != PROTOCOL_VERSION {
        return Err(WorkerIntegrityError::Manifest(format!(
            "protocol version {} does not match {}",
            manifest.protocol_version, PROTOCOL_VERSION
        )));
    }
    if manifest.protocol_identity != PROTOCOL_IDENTITY {
        return Err(WorkerIntegrityError::Manifest(format!(
            "protocol identity {} does not match {PROTOCOL_IDENTITY}",
            manifest.protocol_identity
        )));
    }
    // An empty target list is a trust root that pins no worker; `pinned_target`
    // then reports every target as unsupported.
    let mut triples = HashSet::new();
    for target in &manifest.targets {
        if target.triple.is_empty()
            || target.os.is_empty()
            || target.arch.is_empty()
            || target.family.is_empty()
        {
            return Err(WorkerIntegrityError::Manifest(
                "manifest target has empty platform metadata".to_owned(),
            ));
        }
        if !triples.insert(&target.triple) {
            return Err(WorkerIntegrityError::Manifest(format!(
                "manifest repeats target {}",
                target.triple
            )));
        }
        if target.bytes == 0 {
            return Err(WorkerIntegrityError::Manifest(format!(
                "target {} has an empty artifact",
                target.triple
            )));
        }
        if target.sha256.len() != 64
            || !target
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(WorkerIntegrityError::Manifest(format!(
                "target {} has an invalid sha256",
                target.triple
            )));
        }
    }
    Ok(())
}

fn parse_manifest(text: &str, source: &str) -> Result<WorkerManifest, WorkerIntegrityError> {
    serde_json::from_str(text)
        .map_err(|error| WorkerIntegrityError::Manifest(format!("{source}: {error}")))
}

fn canonical_manifest_digest(text: &str) -> Result<String, WorkerIntegrityError> {
    let value: Value = serde_json::from_str(text).map_err(|error| {
        WorkerIntegrityError::Manifest(format!("canonicalize worker manifest: {error}"))
    })?;
    let bytes = serde_json::to_vec(&value).map_err(|error| {
        WorkerIntegrityError::Manifest(format!("canonicalize worker manifest: {error}"))
    })?;
    Ok(hex_digest(&Sha256::digest(bytes)))
}

/// Selects the manifest entry that pins `current`, requiring its platform
/// metadata to match exactly.
fn pinned_target<'manifest>(
    manifest: &'manifest WorkerManifest,
    current: &CurrentTarget,
) -> Result<&'manifest WorkerTarget, WorkerIntegrityError> {
    let target = manifest
        .targets
        .iter()
        .find(|target| target.triple == current.triple)
        .ok_or_else(|| WorkerIntegrityError::UnsupportedTarget(current.triple.to_owned()))?;
    if target.os != current.os || target.arch != current.arch || target.family != current.family {
        return Err(WorkerIntegrityError::Manifest(format!(
            "target metadata for {} does not match the current target",
            current.triple
        )));
    }
    Ok(target)
}

/// Checks that `manifest_text` is a valid worker manifest that pins `current`.
pub(crate) fn manifest_pins_target(
    manifest_text: &str,
    current: &CurrentTarget,
) -> Result<(), WorkerIntegrityError> {
    let manifest = parse_manifest(manifest_text, "embedded trusted manifest")?;
    validate_manifest(&manifest)?;
    pinned_target(&manifest, current).map(drop)
}

pub(crate) fn current_target() -> CurrentTarget {
    CurrentTarget {
        triple: CURRENT_TARGET_TRIPLE,
        os: std::env::consts::OS,
        arch: std::env::consts::ARCH,
        family: std::env::consts::FAMILY,
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    const CHECKED_IN_MANIFEST: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../product/ncm/reference/worker-manifest.json"
    ));

    fn manifest_with_targets(targets: Value) -> String {
        serde_json::to_string_pretty(&serde_json::json!({
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "worker": WORKER_NAME,
            "protocol_version": PROTOCOL_VERSION,
            "protocol_identity": PROTOCOL_IDENTITY,
            "targets": targets,
        }))
        .expect("fixture worker manifest serializes")
    }

    fn fixture_target(bytes: &[u8]) -> Value {
        let current = current_target();
        serde_json::json!({
            "triple": current.triple,
            "os": current.os,
            "arch": current.arch,
            "family": current.family,
            "bytes": bytes.len(),
            "sha256": hex_digest(&Sha256::digest(bytes)),
        })
    }

    fn fixture_manifest(bytes: &[u8]) -> String {
        manifest_with_targets(Value::Array(vec![fixture_target(bytes)]))
    }

    fn validate_text(text: &str) -> Result<(), WorkerIntegrityError> {
        validate_manifest(&parse_manifest(text, "fixture").expect("fixture manifest parses"))
    }

    #[test]
    fn executable_name_carries_the_target_executable_suffix() {
        assert_eq!(
            WORKER_EXECUTABLE_NAME,
            format!("{WORKER_NAME}{}", std::env::consts::EXE_SUFFIX)
        );
    }

    #[test]
    fn build_embeds_the_selected_trust_root() {
        // Mirrors build.rs: an absolute override path wins, otherwise the
        // checked-in trust root is embedded verbatim.
        let expected = match option_env!("TRACEDECAY_NCM_WORKER_MANIFEST") {
            Some(path) => fs::read_to_string(path).expect("read build-time trust root"),
            None => CHECKED_IN_MANIFEST.to_owned(),
        };
        assert_eq!(trusted_worker_manifest(), expected);
        // Every NCM release triple starts with its `std::env::consts::ARCH`.
        assert!(
            current_target_triple().starts_with(std::env::consts::ARCH),
            "compile-time triple {} does not name {}",
            current_target_triple(),
            std::env::consts::ARCH
        );
    }

    #[test]
    fn checked_in_trust_root_pins_no_worker_and_rejects_every_target() {
        let manifest = parse_manifest(CHECKED_IN_MANIFEST, "checked-in").expect("parse");
        validate_manifest(&manifest).expect("checked-in trust root is valid");
        assert!(
            manifest.targets.is_empty(),
            "source builds must pin no worker"
        );
        assert_eq!(
            manifest_pins_target(CHECKED_IN_MANIFEST, &current_target()),
            Err(WorkerIntegrityError::UnsupportedTarget(
                current_target_triple().to_owned()
            ))
        );
    }

    #[test]
    fn empty_trust_root_reports_the_current_target_unsupported_before_hashing() {
        let root = TempDir::new().expect("fixture root");
        let worker = root.path().join(WORKER_EXECUTABLE_NAME);
        let manifest_path = root.path().join(WORKER_MANIFEST_NAME);
        let manifest = manifest_with_targets(Value::Array(Vec::new()));
        executable(&worker, b"fixture worker bytes");
        fs::write(&manifest_path, &manifest).expect("write fixture manifest");

        assert!(matches!(
            stage_verified_worker_binary_at(&worker, &manifest_path, &manifest),
            Err(WorkerIntegrityError::UnsupportedTarget(target))
                if target == current_target_triple()
        ));
    }

    #[test]
    fn manifest_validation_rejects_duplicate_and_malformed_targets() {
        let target = fixture_target(b"fixture worker bytes");
        assert_eq!(
            validate_text(&manifest_with_targets(Value::Array(Vec::new()))),
            Ok(())
        );
        assert!(matches!(
            validate_text(&manifest_with_targets(Value::Array(vec![
                target.clone(),
                target.clone()
            ]))),
            Err(WorkerIntegrityError::Manifest(detail)) if detail.contains("repeats target")
        ));
        for (field, value) in [
            ("triple", serde_json::json!("")),
            ("os", serde_json::json!("")),
            ("bytes", serde_json::json!(0)),
            ("sha256", serde_json::json!("A".repeat(64))),
            ("sha256", serde_json::json!("0".repeat(63))),
        ] {
            let mut malformed = target.clone();
            malformed[field] = value;
            assert!(
                matches!(
                    validate_text(&manifest_with_targets(Value::Array(vec![malformed]))),
                    Err(WorkerIntegrityError::Manifest(_))
                ),
                "malformed {field} was accepted"
            );
        }
        let mut unknown_field = target;
        unknown_field["signature"] = serde_json::json!("unsigned");
        assert!(matches!(
            parse_manifest(
                &manifest_with_targets(Value::Array(vec![unknown_field])),
                "fixture"
            ),
            Err(WorkerIntegrityError::Manifest(_))
        ));
    }

    #[test]
    fn pinned_triple_with_foreign_platform_metadata_is_rejected() {
        let mut target = fixture_target(b"fixture worker bytes");
        target["family"] = serde_json::json!(if std::env::consts::FAMILY == "unix" {
            "windows"
        } else {
            "unix"
        });
        let manifest = manifest_with_targets(Value::Array(vec![target]));
        assert!(matches!(
            manifest_pins_target(&manifest, &current_target()),
            Err(WorkerIntegrityError::Manifest(detail)) if detail.contains("does not match")
        ));
        assert_eq!(
            manifest_pins_target(
                &fixture_manifest(b"fixture worker bytes"),
                &current_target()
            ),
            Ok(())
        );
    }

    fn executable(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).expect("write worker fixture");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(path)
                .expect("worker fixture metadata")
                .permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(path, permissions).expect("make worker fixture executable");
        }
    }

    fn target_for(bytes: &[u8]) -> WorkerTarget {
        let current = current_target();
        WorkerTarget {
            triple: current.triple.to_owned(),
            os: current.os.to_owned(),
            arch: current.arch.to_owned(),
            family: current.family.to_owned(),
            bytes: bytes.len() as u64,
            sha256: hex_digest(&Sha256::digest(bytes)),
        }
    }

    #[test]
    fn staging_seals_the_hashed_bytes_and_cleans_up_after_drop() {
        let source_root = TempDir::new().expect("source root");
        let source = source_root.path().join("worker");
        let original = b"verified worker bytes";
        fs::write(&source, original).expect("write source");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&source)
                .expect("source metadata")
                .permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&source, permissions).expect("make source executable");
        }

        let artifact = stage_verified_file(
            open_worker_binary(&source).expect("open source"),
            &target_for(original),
        )
        .expect("stage source");
        let staged_path = artifact.path().to_owned();
        assert_eq!(fs::read(&staged_path).expect("read staged bytes"), original);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&staged_path)
                    .expect("staged metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o500
            );
        }

        fs::write(&source, b"replacement bytes").expect("replace source");
        assert_eq!(fs::read(&staged_path).expect("read sealed bytes"), original);
        drop(artifact);
        assert!(!staged_path.exists(), "staged worker is cleaned up");
    }

    #[cfg(unix)]
    #[test]
    fn staged_artifact_executes_sealed_bytes_after_source_replacement() {
        let source_root = TempDir::new().expect("source root");
        let source = source_root.path().join("worker");
        let original = b"#!/bin/sh\nprintf '%s' verified\n";
        executable(&source, original);

        let artifact = stage_verified_file(
            open_worker_binary(&source).expect("open source"),
            &target_for(original),
        )
        .expect("stage source");
        fs::write(&source, b"#!/bin/sh\nprintf '%s' replaced\n").expect("replace source");

        let output = std::process::Command::new(artifact.path())
            .output()
            .expect("execute staged worker");
        assert!(output.status.success(), "staged worker failed: {output:?}");
        assert_eq!(output.stdout, b"verified");
    }

    #[cfg(unix)]
    #[test]
    fn source_symlink_is_rejected_before_verification() {
        let root = TempDir::new().expect("source root");
        let target = root.path().join("target");
        let link = root.path().join("worker");
        fs::write(&target, b"worker bytes").expect("write target");
        std::os::unix::fs::symlink(&target, &link).expect("create source symlink");
        assert!(matches!(
            open_worker_binary(&link),
            Err(WorkerIntegrityError::NotRegularFile)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn sibling_manifest_symlink_is_rejected_before_artifact_hashing() {
        let root = TempDir::new().expect("fixture root");
        let worker = root.path().join(WORKER_EXECUTABLE_NAME);
        let manifest_path = root.path().join("worker-manifest.json");
        let trusted_path = root.path().join("trusted-worker-manifest.json");
        let bytes = b"fixture worker bytes";
        let trusted = fixture_manifest(bytes);
        executable(&worker, bytes);
        fs::write(&trusted_path, &trusted).expect("write trusted manifest");
        std::os::unix::fs::symlink(&trusted_path, &manifest_path)
            .expect("create sibling manifest symlink");

        assert!(matches!(
            stage_verified_worker_binary_at(&worker, &manifest_path, &trusted),
            Err(WorkerIntegrityError::Manifest(detail))
                if detail.contains("regular sibling file")
        ));
    }

    #[test]
    fn trusted_manifest_accepts_an_immutable_pinned_fixture() {
        let root = TempDir::new().expect("fixture root");
        let worker = root.path().join(WORKER_EXECUTABLE_NAME);
        let manifest_path = root.path().join("worker-manifest.json");
        let bytes = b"fixture worker bytes";
        let manifest = fixture_manifest(bytes);
        executable(&worker, bytes);
        fs::write(&manifest_path, &manifest).expect("write fixture manifest");

        let artifact = stage_verified_worker_binary_at(&worker, &manifest_path, &manifest)
            .expect("pinned fixture verifies");
        assert_eq!(
            fs::read(artifact.path()).expect("read sealed fixture"),
            bytes
        );
    }

    #[test]
    fn trusted_manifest_reports_a_digest_mismatch_after_size_matches() {
        let root = TempDir::new().expect("fixture root");
        let worker = root.path().join(WORKER_EXECUTABLE_NAME);
        let manifest_path = root.path().join("worker-manifest.json");
        let expected = b"fixture worker bytes";
        let actual = b"fixture worker byteX";
        let manifest = fixture_manifest(expected);
        executable(&worker, actual);
        fs::write(&manifest_path, &manifest).expect("write fixture manifest");

        assert!(matches!(
            stage_verified_worker_binary_at(&worker, &manifest_path, &manifest),
            Err(WorkerIntegrityError::DigestMismatch { .. })
        ));
    }

    #[test]
    fn stale_sibling_manifest_is_rejected_before_artifact_hashing() {
        let root = TempDir::new().expect("fixture root");
        let worker = root.path().join(WORKER_EXECUTABLE_NAME);
        let manifest_path = root.path().join("worker-manifest.json");
        let bytes = b"fixture worker bytes";
        let trusted = fixture_manifest(bytes);
        let mut stale: Value = serde_json::from_str(&trusted).expect("decode fixture manifest");
        stale["targets"][0]["sha256"] = Value::String("0".repeat(64));
        let stale = serde_json::to_string_pretty(&stale).expect("encode stale manifest");
        executable(&worker, bytes);
        fs::write(&manifest_path, stale).expect("write stale fixture manifest");

        assert!(matches!(
            stage_verified_worker_binary_at(&worker, &manifest_path, &trusted),
            Err(WorkerIntegrityError::Manifest(detail)) if detail.contains("stale")
        ));
    }

    #[test]
    fn installed_manifest_resolution_rejects_ancestor_fallback() {
        let root = TempDir::new().expect("fixture root");
        let bundle = root.path().join("bundle");
        let worker = bundle.join("bin").join(WORKER_EXECUTABLE_NAME);
        fs::create_dir_all(worker.parent().expect("worker parent")).expect("create worker parent");
        fs::create_dir_all(bundle.join("product/ncm/reference")).expect("create ancestor root");
        fs::write(
            bundle.join("product/ncm/reference/worker-manifest.json"),
            b"{}",
        )
        .expect("write decoy ancestor manifest");

        assert!(matches!(
            manifest_path(&worker),
            Err(WorkerIntegrityError::Manifest(detail)) if detail.contains("beside the worker")
        ));
    }
}
