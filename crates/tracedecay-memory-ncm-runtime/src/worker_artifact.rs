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

/// The executable name admitted by the NCM worker manifest.
pub(crate) const WORKER_NAME: &str = "tracedecay-ncm-worker";
const MANIFEST_SCHEMA_VERSION: u16 = 1;
const REFERENCE_MANIFEST: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../product/ncm/reference/worker-manifest.json"
));

/// A checked-in executable identity could not be established or matched.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WorkerIntegrityError {
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
    SizeMismatch { expected: u64, actual: u64 },
    /// The configured file digest differs from the pinned artifact.
    DigestMismatch { expected: String, actual: String },
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

struct CurrentTarget {
    triple: &'static str,
    os: &'static str,
    arch: &'static str,
    family: &'static str,
}

/// Owns one verified worker copy for exactly one process lifetime.
///
/// The source path is never passed to the child process. The private directory
/// and create-new file make the launch pathname stable after verification, and
/// the file is made read-only before it is returned. Dropping this value
/// removes the staging artifact after the caller has reaped its child.
pub(crate) struct VerifiedWorkerArtifact {
    _directory: TempDir,
    path: PathBuf,
}

impl VerifiedWorkerArtifact {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

/// Verifies the configured worker against the checked-in offline manifest and
/// seals the exact hashed bytes into a private launch artifact.
///
/// This function performs only local reads, hashing, and private staging. It
/// does not resolve, download, or replace an executable or any model artifact.
pub(crate) fn stage_verified_worker_binary(
    path: &Path,
) -> Result<VerifiedWorkerArtifact, WorkerIntegrityError> {
    let manifest_path = manifest_path(path)?;
    stage_verified_worker_binary_at(path, &manifest_path, REFERENCE_MANIFEST)
}

fn stage_verified_worker_binary_at(
    path: &Path,
    manifest_path: &Path,
    trusted_manifest_text: &str,
) -> Result<VerifiedWorkerArtifact, WorkerIntegrityError> {
    let trusted_manifest = parse_manifest(trusted_manifest_text, "embedded reference manifest")?;
    validate_manifest(&trusted_manifest)?;
    let manifest_text = fs::read_to_string(&manifest_path)
        .map_err(|error| WorkerIntegrityError::Read(error.to_string()))?;
    let manifest = parse_manifest(&manifest_text, &manifest_path.display().to_string())?;
    validate_manifest(&manifest)?;
    let expected_manifest_digest = canonical_manifest_digest(trusted_manifest_text)?;
    let actual_manifest_digest = canonical_manifest_digest(&manifest_text)?;
    if actual_manifest_digest != expected_manifest_digest {
        return Err(WorkerIntegrityError::Manifest(format!(
            "manifest is stale or not sourced from the trusted build root: expected {expected_manifest_digest}, got {actual_manifest_digest}"
        )));
    }
    let current = current_target();
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

    let file = open_worker_binary(path)?;
    stage_verified_file(file, target)
}

fn stage_verified_file(
    mut file: File,
    target: &WorkerTarget,
) -> Result<VerifiedWorkerArtifact, WorkerIntegrityError> {
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
    if metadata.len() != target.bytes {
        return Err(WorkerIntegrityError::SizeMismatch {
            expected: target.bytes,
            actual: metadata.len(),
        });
    }

    let staging =
        TempDir::new().map_err(|error| WorkerIntegrityError::Staging(error.to_string()))?;
    tracedecay_private_fs::validate_private_directory(staging.path())
        .map_err(|error| WorkerIntegrityError::Staging(error.to_string()))?;
    let staged_path = staging.path().join(WORKER_NAME);
    let mut staged = tracedecay_private_fs::create_private_file(&staged_path)
        .map_err(|error| WorkerIntegrityError::Staging(error.to_string()))?;
    let mut digest = Sha256::new();
    let mut bytes_read = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| WorkerIntegrityError::Read(error.to_string()))?;
        if read == 0 {
            break;
        }
        bytes_read = bytes_read.saturating_add(read as u64);
        digest.update(&buffer[..read]);
        staged
            .write_all(&buffer[..read])
            .map_err(|error| WorkerIntegrityError::Staging(error.to_string()))?;
    }
    if bytes_read != target.bytes {
        return Err(WorkerIntegrityError::SizeMismatch {
            expected: target.bytes,
            actual: bytes_read,
        });
    }
    let final_size = file
        .metadata()
        .map_err(|error| WorkerIntegrityError::Read(error.to_string()))?
        .len();
    if final_size != target.bytes {
        return Err(WorkerIntegrityError::SizeMismatch {
            expected: target.bytes,
            actual: final_size,
        });
    }
    let digest = digest.finalize();
    let actual = hex_digest(&digest);
    if actual != target.sha256 {
        return Err(WorkerIntegrityError::DigestMismatch {
            expected: target.sha256.clone(),
            actual,
        });
    }
    staged
        .sync_all()
        .map_err(|error| WorkerIntegrityError::Staging(error.to_string()))?;
    seal_staged_worker(&staged)?;
    Ok(VerifiedWorkerArtifact {
        _directory: staging,
        path: staged_path,
    })
}

fn open_worker_binary(path: &Path) -> Result<File, WorkerIntegrityError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
        return match options.open(path) {
            Ok(file) => Ok(file),
            Err(error) if error.raw_os_error() == Some(libc::ELOOP) => {
                Err(WorkerIntegrityError::NotRegularFile)
            }
            Err(error) => Err(WorkerIntegrityError::Read(error.to_string())),
        };
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
        .map(|parent| parent.join("worker-manifest.json"))
        .ok_or_else(|| {
            WorkerIntegrityError::Manifest(
                "worker path has no parent for manifest binding".to_owned(),
            )
        })?;
    if sibling.is_file() {
        return Ok(sibling);
    }
    Err(WorkerIntegrityError::Manifest(format!(
        "worker manifest must be beside the worker: {}",
        sibling.display()
    )))
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
    if manifest.targets.is_empty() {
        return Err(WorkerIntegrityError::Manifest(
            "manifest has no supported targets".to_owned(),
        ));
    }
    let mut triples = HashSet::new();
    for target in &manifest.targets {
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

fn current_target() -> CurrentTarget {
    CurrentTarget {
        triple: current_target_triple(),
        os: std::env::consts::OS,
        arch: std::env::consts::ARCH,
        family: std::env::consts::FAMILY,
    }
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
const CURRENT_TARGET_TRIPLE: &str = "aarch64-apple-darwin";
#[cfg(all(target_os = "macos", target_arch = "x86_64"))]
const CURRENT_TARGET_TRIPLE: &str = "x86_64-apple-darwin";
#[cfg(all(target_os = "linux", target_arch = "aarch64", target_env = "gnu"))]
const CURRENT_TARGET_TRIPLE: &str = "aarch64-unknown-linux-gnu";
#[cfg(all(target_os = "linux", target_arch = "x86_64", target_env = "gnu"))]
const CURRENT_TARGET_TRIPLE: &str = "x86_64-unknown-linux-gnu";
#[cfg(all(target_os = "linux", target_arch = "aarch64", target_env = "musl"))]
const CURRENT_TARGET_TRIPLE: &str = "aarch64-unknown-linux-musl";
#[cfg(all(target_os = "linux", target_arch = "x86_64", target_env = "musl"))]
const CURRENT_TARGET_TRIPLE: &str = "x86_64-unknown-linux-musl";
#[cfg(not(any(
    all(target_os = "macos", target_arch = "aarch64"),
    all(target_os = "macos", target_arch = "x86_64"),
    all(target_os = "linux", target_arch = "aarch64", target_env = "gnu"),
    all(target_os = "linux", target_arch = "x86_64", target_env = "gnu"),
    all(target_os = "linux", target_arch = "aarch64", target_env = "musl"),
    all(target_os = "linux", target_arch = "x86_64", target_env = "musl")
)))]
const CURRENT_TARGET_TRIPLE: &str = "unsupported";

const fn current_target_triple() -> &'static str {
    CURRENT_TARGET_TRIPLE
}

fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_manifest(bytes: &[u8]) -> String {
        let current = current_target();
        serde_json::to_string_pretty(&serde_json::json!({
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "worker": WORKER_NAME,
            "protocol_version": PROTOCOL_VERSION,
            "protocol_identity": PROTOCOL_IDENTITY,
            "targets": [{
                "triple": current.triple,
                "os": current.os,
                "arch": current.arch,
                "family": current.family,
                "bytes": bytes.len(),
                "sha256": hex_digest(&Sha256::digest(bytes)),
            }]
        }))
        .expect("fixture worker manifest serializes")
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

    #[test]
    fn trusted_manifest_accepts_an_immutable_pinned_fixture() {
        let root = TempDir::new().expect("fixture root");
        let worker = root.path().join(WORKER_NAME);
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
        let worker = root.path().join(WORKER_NAME);
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
        let worker = root.path().join(WORKER_NAME);
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
        let worker = bundle.join("bin").join(WORKER_NAME);
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
