//! Offline identity and executable verification for the native NCM worker.

use crate::wire::{PROTOCOL_IDENTITY, PROTOCOL_VERSION};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::fmt;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

/// The executable name admitted by the NCM worker manifest.
pub(crate) const WORKER_NAME: &str = "tracedecay-ncm-worker";
const MANIFEST_SCHEMA_VERSION: u16 = 1;

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

/// Verifies the configured worker against the checked-in offline manifest.
///
/// This function performs only local reads and hashing. It does not resolve,
/// download, or replace an executable or any model artifact.
pub(crate) fn verify_worker_binary(path: &Path) -> Result<(), WorkerIntegrityError> {
    let manifest_path = manifest_path(path);
    let manifest_text = fs::read_to_string(&manifest_path)
        .map_err(|error| WorkerIntegrityError::Read(error.to_string()))?;
    let manifest: WorkerManifest = serde_json::from_str(&manifest_text)
        .map_err(|error| WorkerIntegrityError::Manifest(error.to_string()))?;
    validate_manifest(&manifest)?;
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

    let mut file =
        File::open(path).map_err(|error| WorkerIntegrityError::Read(error.to_string()))?;
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

    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| WorkerIntegrityError::Read(error.to_string()))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    let digest = digest.finalize();
    let actual = hex_digest(&digest);
    if actual != target.sha256 {
        return Err(WorkerIntegrityError::DigestMismatch {
            expected: target.sha256.clone(),
            actual,
        });
    }
    Ok(())
}

fn manifest_path(binary: &Path) -> PathBuf {
    let sibling = binary
        .parent()
        .map(|parent| parent.join("worker-manifest.json"));
    for ancestor in binary.ancestors().skip(1) {
        let path = ancestor.join("product/ncm/reference/worker-manifest.json");
        if path.is_file() {
            return path;
        }
    }
    if let Ok(current_dir) = std::env::current_dir() {
        let path = current_dir.join("product/ncm/reference/worker-manifest.json");
        if path.is_file() {
            return path;
        }
    }
    sibling
        .clone()
        .filter(|path| path.is_file())
        .or(sibling)
        .unwrap_or_else(|| PathBuf::from("worker-manifest.json"))
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
    for target in &manifest.targets {
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
