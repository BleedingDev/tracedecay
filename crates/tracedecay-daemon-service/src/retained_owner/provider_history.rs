//! Host-authorized history from the existing canonical observation, original
//! event, repository marker, disposition and provider journal authorities.
//! Values on the provider wire are claims; every use reads these authorities.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

#[cfg(unix)]
use std::ffi::CString;
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::io::{AsRawHandle, FromRawHandle};

use sha2::{Digest, Sha256};
use tracedecay_contracts::ResolvedScope;
use tracedecay_domain::framed_log::checksum as frame_checksum;
use tracedecay_domain::{
    BrainId, CanonicalObservationEnvelopeV1, CanonicalObservationIdV1, CommitId,
    EvidenceAvailabilityV1, FactOwnerV1, ObservationScopeV1, ObservationSourceIdentityV1,
    ProjectId, RepositoryId, TreeId, UserProfileId, UtcMicros, WorktreeId, canonical_json_bytes,
};
use tracedecay_memory_observation::SqliteObservationJournal;
use tracedecay_memory_provider_registry::{
    CurrentSourceDisposition, GrantedHistorySource, HistoryGrant, OriginScopeEvidence,
    OriginalSourceIdentity, OwnedExactScope, OwnedProviderId, RecordedValidity,
    RestoreDispositionCheckpoint, SourceAttribution,
};
use tracedecay_memory_provider_registry::{HistoryRelation, SourceDisposition};
use tracedecay_sessions::repository_provenance::RepositoryProvenanceAdmissionContext;
use tracedecay_store::observation::ObservationOriginV1;
use tracedecay_store::{
    AnchorDispositionStateV1, ObservationAdmissionPort, ObservationRecentWindowRequest,
    ObservationReplayRequest, RetrievalAnchorDispositionStore, StoreShardIdV1, StoreShardScopeV1,
    StoredObservation,
};

use super::cognitive_recall::control_attribution::{
    RecallControlTraceRefV1, RecallLocatorKeyV1, redact_retained_source_attribution_with_key,
    validate_opaque_retained_source, validate_retained_candidate_identity,
    validate_retained_provider_rank,
};
use super::observation_journey::exact_scope_for_session;
use tracedecay_memory_provider_registry::recall_admission::{
    RecallOutcomeScopeV1, source_attribution::RecallSourceAttributionV1,
};

const MAX_HISTORY_PAGE: usize = 256;
const MAX_LEDGER_IDENTITY_READ_ATTEMPTS: usize = 2;
const LEDGER_HEADER_BYTES: usize = 6;
const LEDGER_IDENTITY_BYTES: usize = 16;
const LEDGER_DIGEST_BYTES: usize = 32;
const LEDGER_CHECKSUM_PREFIX_BYTES: usize = 8;
const LEDGER_RECORD_BODY_BYTES: usize =
    LEDGER_IDENTITY_BYTES + LEDGER_DIGEST_BYTES + std::mem::size_of::<i64>();
const LEDGER_RECORD_BYTES: usize = LEDGER_RECORD_BODY_BYTES + LEDGER_CHECKSUM_PREFIX_BYTES;
const LEDGER_FILE_MAX_BYTES: usize = LEDGER_HEADER_BYTES
    + tracedecay_hooks::MAX_SPOOL_RECORDS_PER_HOST as usize * LEDGER_RECORD_BYTES * 2;
const LEDGER_MAGIC: &[u8; 4] = b"TDL1";
const LEDGER_FORMAT_VERSION: u16 = 1;
const LIVE_ORIGINS_FILE: &str = "admission-live-origins.json";
const RECORDS_FILE: &str = "admissions.v1.bin";
const MAX_LIVE_ORIGIN_BYTES: usize = 1024 * 1024;
const MAX_LIVE_ORIGIN_BOUNDARIES: usize = 64;
const MAX_LIVE_ORIGIN_PROOFS: usize = 64;

type HookAdmissionLedgerError = tracedecay_hooks::admission_ledger::HookAdmissionLedgerError;
type HookLiveOriginAdmissionV1 = tracedecay_hooks::admission_ledger::HookLiveOriginAdmissionV1;
type HookLiveOriginBoundaryV1 = tracedecay_hooks::admission_ledger::HookLiveOriginBoundaryV1;
type HookLiveOriginFrameV1 = tracedecay_hooks::admission_ledger::HookLiveOriginFrameV1;
type HookLiveOriginObservationV1 = tracedecay_hooks::admission_ledger::HookLiveOriginObservationV1;
type HookLiveOriginProofV1 = tracedecay_hooks::admission_ledger::HookLiveOriginProofV1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PhysicalPathKind {
    Directory,
    Entry,
}

/// Stable identity for one existing path entry. The path is deliberately not
/// part of this value: an authority binds the object reached through a path,
/// then rejects a same-path replacement on every later admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PhysicalPathIdentityV1 {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(windows)]
    volume_serial_number: u32,
    #[cfg(windows)]
    file_index: u64,
}

impl PhysicalPathIdentityV1 {
    fn capture(path: &Path, subject: &'static str, kind: PhysicalPathKind) -> HistoryResult<Self> {
        let (identity, _) = Self::capture_open(path, subject, kind)?;
        Ok(identity)
    }

    fn capture_open(
        path: &Path,
        subject: &'static str,
        kind: PhysicalPathKind,
    ) -> HistoryResult<(Self, File)> {
        tracedecay_runtime_core::storage::reject_symlink_components(path, subject).map_err(
            |error| {
                if error.kind() == std::io::ErrorKind::InvalidInput {
                    ProviderHistoryErrorV1::Ineligible(subject)
                } else {
                    ProviderHistoryErrorV1::Unavailable(subject)
                }
            },
        )?;

        // Capture the identity from one exact no-follow handle. A metadata
        // lookup followed by a separate open would let a same-path replacement
        // change the object between the check and the handle that is retained.
        let file = open_identity_path(path, kind).map_err(|error| {
            if is_symlink_error(&error) || error.kind() == std::io::ErrorKind::InvalidInput {
                ProviderHistoryErrorV1::Ineligible(subject)
            } else {
                ProviderHistoryErrorV1::Unavailable(subject)
            }
        })?;
        // Recheck every parent after opening as well. A parent rename followed
        // by a symlink swap can occur during the no-follow open itself; any
        // persistent symlink component is therefore fail-closed before its
        // handle-derived identity is accepted.
        tracedecay_runtime_core::storage::reject_symlink_components(path, subject).map_err(
            |error| {
                if error.kind() == std::io::ErrorKind::InvalidInput {
                    ProviderHistoryErrorV1::Ineligible(subject)
                } else {
                    ProviderHistoryErrorV1::Unavailable(subject)
                }
            },
        )?;
        // On Windows a reparse point can be opened for inspection with
        // `FILE_FLAG_OPEN_REPARSE_POINT`; inspect the path entry as well so a
        // final link is rejected on the stable `std` metadata surface.
        let path_metadata = std::fs::symlink_metadata(path)
            .map_err(|_| ProviderHistoryErrorV1::Unavailable(subject))?;
        if path_metadata.file_type().is_symlink() {
            return Err(ProviderHistoryErrorV1::Ineligible(subject));
        }
        let metadata = file
            .metadata()
            .map_err(|_| ProviderHistoryErrorV1::Unavailable(subject))?;
        if metadata.file_type().is_symlink()
            || (kind == PhysicalPathKind::Directory && !metadata.is_dir())
            || (kind == PhysicalPathKind::Entry && !metadata.is_file() && !metadata.is_dir())
        {
            return Err(ProviderHistoryErrorV1::Ineligible(subject));
        }
        #[cfg(unix)]
        {
            return Ok((
                Self {
                    device: metadata.dev(),
                    inode: metadata.ino(),
                },
                file,
            ));
        }
        #[cfg(windows)]
        {
            let information = tracedecay_private_fs::windows_file::information(&file)
                .map_err(|_| ProviderHistoryErrorV1::Unavailable(subject))?;
            // `GetFileInformationByHandle` exposes the volume serial as a
            // plain value; keep the optional form at this boundary so a
            // platform that cannot provide one fails closed rather than
            // manufacturing an identity from a zero sentinel.
            let volume_serial_number = Some(information.volume_serial_number)
                .filter(|value| *value != 0)
                .ok_or(ProviderHistoryErrorV1::Unavailable(subject))?;
            let file_index = Some(information.file_index)
                .filter(|value| *value != 0 && *value != u64::MAX)
                .ok_or(ProviderHistoryErrorV1::Unavailable(subject))?;
            return Ok((
                Self {
                    volume_serial_number,
                    file_index,
                },
                file,
            ));
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = (path, subject, kind, metadata);
            Err(ProviderHistoryErrorV1::Unavailable(
                "physical filesystem identity",
            ))
        }
    }

    fn from_open_file(
        file: File,
        subject: &'static str,
        kind: PhysicalPathKind,
    ) -> HistoryResult<Self> {
        let metadata = file
            .metadata()
            .map_err(|_| ProviderHistoryErrorV1::Unavailable(subject))?;
        if metadata.file_type().is_symlink()
            || (kind == PhysicalPathKind::Directory && !metadata.is_dir())
            || (kind == PhysicalPathKind::Entry && !metadata.is_file() && !metadata.is_dir())
        {
            return Err(ProviderHistoryErrorV1::Ineligible(subject));
        }
        #[cfg(unix)]
        {
            return Ok(Self {
                device: metadata.dev(),
                inode: metadata.ino(),
            });
        }
        #[cfg(windows)]
        {
            let information = tracedecay_private_fs::windows_file::information(&file)
                .map_err(|_| ProviderHistoryErrorV1::Unavailable(subject))?;
            let volume_serial_number = Some(information.volume_serial_number)
                .filter(|value| *value != 0)
                .ok_or(ProviderHistoryErrorV1::Unavailable(subject))?;
            let file_index = Some(information.file_index)
                .filter(|value| *value != 0 && *value != u64::MAX)
                .ok_or(ProviderHistoryErrorV1::Unavailable(subject))?;
            return Ok(Self {
                volume_serial_number,
                file_index,
            });
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = (file, subject, kind, metadata);
            Err(ProviderHistoryErrorV1::Unavailable(
                "physical filesystem identity",
            ))
        }
    }

    pub(crate) fn validate_current(
        &self,
        path: &Path,
        subject: &'static str,
        kind: PhysicalPathKind,
    ) -> HistoryResult<()> {
        if *self != Self::capture(path, subject, kind)? {
            return Err(ProviderHistoryErrorV1::Ineligible(subject));
        }
        Ok(())
    }
}

/// Open one path entry without following its final link/reparse point. The
/// returned handle is used for metadata identity and as the anchor for all
/// later handle-relative descendant opens.
fn open_identity_path(path: &Path, kind: PhysicalPathKind) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        let mut flags = libc::O_CLOEXEC | libc::O_NOFOLLOW;
        if kind == PhysicalPathKind::Entry {
            // A regular file should not block identity capture if a path is
            // concurrently replaced by a FIFO. The resulting handle is still
            // rejected by the kind check below.
            flags |= libc::O_NONBLOCK;
        } else {
            flags |= libc::O_DIRECTORY;
        }
        options.custom_flags(flags);
    }
    #[cfg(windows)]
    {
        // The flags are stable Win32 values and avoid unstable standard-library
        // path identity accessors on Rust 1.97.
        const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        let _ = kind;
        options.custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT);
    }
    options.open(path)
}

/// Open one direct child relative to an already-retained directory handle.
/// Every component is opened independently with no-follow semantics; callers
/// therefore never re-resolve a descendant through the mutable pathname.
fn open_relative_path(
    parent: &File,
    name: &OsStr,
    kind: PhysicalPathKind,
) -> std::io::Result<File> {
    #[cfg(unix)]
    {
        let name = CString::new(name.as_bytes())
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "NUL child"))?;
        let mut flags = libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW;
        if kind == PhysicalPathKind::Entry {
            flags |= libc::O_NONBLOCK;
        } else {
            flags |= libc::O_DIRECTORY;
        }
        // SAFETY: `parent` is a live directory handle retained by the caller,
        // and `name` is a NUL-terminated child name with no interior NUL.
        let fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags, 0) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: `fd` is newly returned by `openat` and is transferred into
        // this `File` exactly once.
        return Ok(unsafe { File::from_raw_fd(fd) });
    }
    #[cfg(windows)]
    {
        return open_relative_path_windows(parent, name, kind);
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (parent, name, kind);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "handle-relative filesystem reads are unsupported on this platform",
        ))
    }
}

#[cfg(windows)]
#[repr(C)]
struct NtUnicodeString {
    length: u16,
    maximum_length: u16,
    buffer: *mut u16,
}

#[cfg(windows)]
#[repr(C)]
struct NtObjectAttributes {
    length: u32,
    root_directory: std::os::windows::io::RawHandle,
    object_name: *mut NtUnicodeString,
    attributes: u32,
    security_descriptor: *mut std::ffi::c_void,
    security_quality_of_service: *mut std::ffi::c_void,
}

#[cfg(windows)]
#[repr(C)]
struct NtIoStatusBlock {
    status: i32,
    information: usize,
}

#[cfg(windows)]
#[link(name = "ntdll")]
unsafe extern "system" {
    fn NtCreateFile(
        file_handle: *mut std::os::windows::io::RawHandle,
        desired_access: u32,
        object_attributes: *mut NtObjectAttributes,
        io_status_block: *mut NtIoStatusBlock,
        allocation_size: *mut i64,
        file_attributes: u32,
        share_access: u32,
        create_disposition: u32,
        create_options: u32,
        ea_buffer: *mut std::ffi::c_void,
        ea_length: u32,
    ) -> i32;
}

#[cfg(windows)]
fn open_relative_path_windows(
    parent: &File,
    name: &OsStr,
    kind: PhysicalPathKind,
) -> std::io::Result<File> {
    const FILE_READ_DATA: u32 = 0x0000_0001;
    const FILE_READ_ATTRIBUTES: u32 = 0x0000_0080;
    const SYNCHRONIZE: u32 = 0x0010_0000;
    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_SHARE_DELETE: u32 = 0x0000_0004;
    const FILE_OPEN: u32 = 1;
    const FILE_DIRECTORY_FILE: u32 = 0x0000_0001;
    const FILE_NON_DIRECTORY_FILE: u32 = 0x0000_0040;
    const FILE_SYNCHRONOUS_IO_NONALERT: u32 = 0x0000_0020;
    const FILE_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    const OBJ_CASE_INSENSITIVE: u32 = 0x0000_0040;

    let mut name_units: Vec<u16> = name.encode_wide().collect();
    let byte_length = name_units
        .len()
        .checked_mul(2)
        .filter(|length| *length <= u16::MAX as usize)
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "child name too long")
        })?;
    name_units.push(0);
    let maximum_length = name_units
        .len()
        .checked_mul(2)
        .filter(|length| *length <= u16::MAX as usize)
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "child name too long")
        })?;
    let mut unicode = NtUnicodeString {
        length: byte_length as u16,
        maximum_length: maximum_length as u16,
        buffer: name_units.as_mut_ptr(),
    };
    let mut attributes = NtObjectAttributes {
        length: std::mem::size_of::<NtObjectAttributes>() as u32,
        root_directory: parent.as_raw_handle(),
        object_name: &mut unicode,
        attributes: OBJ_CASE_INSENSITIVE,
        security_descriptor: std::ptr::null_mut(),
        security_quality_of_service: std::ptr::null_mut(),
    };
    let mut status_block = NtIoStatusBlock {
        status: 0,
        information: 0,
    };
    let mut handle = std::ptr::null_mut();
    let desired_access = FILE_READ_DATA | FILE_READ_ATTRIBUTES | SYNCHRONIZE;
    let create_options = FILE_SYNCHRONOUS_IO_NONALERT
        | FILE_OPEN_REPARSE_POINT
        | if kind == PhysicalPathKind::Directory {
            FILE_DIRECTORY_FILE
        } else {
            FILE_NON_DIRECTORY_FILE
        };
    // SAFETY: all pointers refer to stack/heap values alive for the duration
    // of the syscall; the retained parent handle supplies RootDirectory.
    let status = unsafe {
        NtCreateFile(
            &mut handle,
            desired_access,
            &mut attributes,
            &mut status_block,
            std::ptr::null_mut(),
            0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            FILE_OPEN,
            create_options,
            std::ptr::null_mut(),
            0,
        )
    };
    if status < 0 {
        return Err(windows_nt_status_error(status));
    }
    if handle.is_null() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "NtCreateFile returned a null handle",
        ));
    }
    // SAFETY: a successful NtCreateFile transfers ownership of this handle to
    // the caller; `File` closes it exactly once.
    Ok(unsafe { File::from_raw_handle(handle) })
}

#[cfg(windows)]
fn windows_nt_status_error(status: i32) -> std::io::Error {
    const STATUS_OBJECT_NAME_NOT_FOUND: u32 = 0xC000_0034;
    const STATUS_OBJECT_PATH_NOT_FOUND: u32 = 0xC000_003A;
    const STATUS_REPARSE_POINT_ENCOUNTERED: u32 = 0xC000_050B;
    const STATUS_IO_REPARSE_TAG_NOT_HANDLED: u32 = 0xC000_0279;
    let kind = match status as u32 {
        STATUS_OBJECT_NAME_NOT_FOUND | STATUS_OBJECT_PATH_NOT_FOUND => std::io::ErrorKind::NotFound,
        STATUS_REPARSE_POINT_ENCOUNTERED | STATUS_IO_REPARSE_TAG_NOT_HANDLED => {
            std::io::ErrorKind::InvalidInput
        }
        _ => std::io::ErrorKind::Other,
    };
    std::io::Error::new(kind, format!("NtCreateFile failed: 0x{status:08x}"))
}

fn is_symlink_error(error: &std::io::Error) -> bool {
    #[cfg(unix)]
    {
        error.raw_os_error() == Some(libc::ELOOP)
    }
    #[cfg(windows)]
    {
        // An OPEN_REPARSE_POINT handle is inspected below. Windows reports a
        // reparse-point refusal as InvalidInput on the stable std surface.
        error.kind() == std::io::ErrorKind::InvalidInput
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = error;
        false
    }
}

/// Retained handles for one admitted host ledger. Keeping the complete
/// ancestor chain alive makes a pathname rename/replacement irrelevant to the
/// bytes returned by a descendant read.
struct HandleRelativeLedgerRoot {
    _data_root: File,
    _admissions: File,
    ledger: File,
}

impl HandleRelativeLedgerRoot {
    fn read_file(
        &self,
        name: &'static str,
        maximum: usize,
    ) -> Result<Option<Vec<u8>>, HookAdmissionLedgerError> {
        read_relative_file(&self.ledger, name, maximum)
    }

    fn read_proofs(
        &self,
        host: tracedecay_hooks::HookHostV1,
        now: UtcMicros,
    ) -> Result<Vec<HookLiveOriginProofV1>, HookAdmissionLedgerError> {
        handle_read_validated_live_origin_metadata(&self.ledger, host, now)
            .map(|metadata| metadata.proofs)
    }

    fn read_boundaries(
        &self,
        host: tracedecay_hooks::HookHostV1,
        now: UtcMicros,
    ) -> Result<Vec<HookLiveOriginBoundaryV1>, HookAdmissionLedgerError> {
        handle_read_validated_live_origin_metadata(&self.ledger, host, now)
            .map(|metadata| metadata.baselines)
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct HandleLiveOriginMetadataV1 {
    baselines: Vec<HookLiveOriginBoundaryV1>,
    proofs: Vec<HookLiveOriginProofV1>,
}

type HandleScannedRecord = (
    [u8; LEDGER_IDENTITY_BYTES],
    [u8; LEDGER_DIGEST_BYTES],
    UtcMicros,
);

fn read_relative_file(
    parent: &File,
    name: &'static str,
    maximum: usize,
) -> Result<Option<Vec<u8>>, HookAdmissionLedgerError> {
    let file = match open_relative_path(parent, OsStr::new(name), PhysicalPathKind::Entry) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => {
            return Err(HookAdmissionLedgerError::UnsafePath);
        }
        Err(_) => return Err(HookAdmissionLedgerError::Io),
    };
    let metadata = file.metadata().map_err(|_| HookAdmissionLedgerError::Io)?;
    if !metadata.file_type().is_file() {
        return Err(HookAdmissionLedgerError::UnsafePath);
    }
    let length = metadata.len();
    if length == 0 || length > maximum as u64 {
        return Err(HookAdmissionLedgerError::Io);
    }
    let mut bytes = Vec::with_capacity(length as usize);
    file.take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| HookAdmissionLedgerError::Io)?;
    if bytes.len() != length as usize {
        return Err(HookAdmissionLedgerError::Io);
    }
    Ok(Some(bytes))
}

// The public hook reader accepts a pathname and cannot consume a retained
// directory handle. Keep its validation rules local while replacing only the
// untrusted path traversal with the handle-relative reader above.
fn handle_source_provider_for_host(host: tracedecay_hooks::HookHostV1) -> Option<&'static str> {
    match host {
        tracedecay_hooks::HookHostV1::ClaudeCode => Some("claude"),
        tracedecay_hooks::HookHostV1::Codex => Some("codex"),
        tracedecay_hooks::HookHostV1::CursorDesktop | tracedecay_hooks::HookHostV1::CursorCloud => {
            Some("cursor")
        }
        tracedecay_hooks::HookHostV1::Hermes => Some("hermes"),
        tracedecay_hooks::HookHostV1::Kiro => Some("kiro"),
        tracedecay_hooks::HookHostV1::Cline => Some("cline"),
        tracedecay_hooks::HookHostV1::RooCode => Some("roo-code"),
        tracedecay_hooks::HookHostV1::Kilo => Some("kilo"),
        tracedecay_hooks::HookHostV1::KimiCode => Some("kimi"),
        tracedecay_hooks::HookHostV1::OpenCode => Some("opencode"),
    }
}

fn handle_source_provider_matches_host(
    host: tracedecay_hooks::HookHostV1,
    source: &ObservationSourceIdentityV1,
) -> bool {
    handle_source_provider_for_host(host)
        .is_some_and(|expected| expected == source.provider().as_str())
}

fn handle_valid_origin_observation(
    host: tracedecay_hooks::HookHostV1,
    value: &HookLiveOriginObservationV1,
) -> bool {
    let repository = &value.scope.repository;
    handle_source_provider_matches_host(host, &value.source)
        && value.source.validate().is_ok()
        && repository.validate().is_ok()
        && repository.project_id().is_some()
        && repository.worktree_id().is_some()
        && matches!(
            repository.evidence().attached_ref(),
            EvidenceAvailabilityV1::Known(reference)
                if reference == &value.branch_evidence.attached_ref
        )
        && matches!(
            repository.evidence().head_commit(),
            EvidenceAvailabilityV1::Known(commit)
                if commit == &value.branch_evidence.head_commit
        )
        && value.canonical_source_path.is_absolute()
        && value.canonical_source_path.as_os_str().len() <= 4096
        && value.branch_evidence.canonical_path.is_absolute()
        && value.branch_evidence.canonical_path.as_os_str().len() <= 4096
        && value.branch_evidence.frontier > 0
        && value.checkpoint.generation != 0
        && value.checkpoint.file_identity != 0
        && value.checkpoint.complete_frontier <= value.physical_eof
        && value.frames.len() <= tracedecay_hooks::admission_ledger::MAX_LIVE_ORIGIN_FRAMES
        && value
            .frames
            .iter()
            .all(|frame| frame.start < frame.end && frame.end <= value.checkpoint.complete_frontier)
        && value
            .frames
            .windows(2)
            .all(|pair| pair[0].end == pair[1].start)
        && value.frames.last().is_none_or(|last| {
            last.end == value.checkpoint.complete_frontier
                && last.resume_fingerprint == value.checkpoint.complete_prefix_fingerprint
        })
}

fn handle_same_live_origin_authority(
    left: &HookLiveOriginAdmissionV1,
    right: &HookLiveOriginAdmissionV1,
) -> bool {
    left.host == right.host
        && left.protected_session_id == right.protected_session_id
        && left.project_id == right.project_id
        && left.repository_id == right.repository_id
        && left.worktree_id == right.worktree_id
        && left.worktree_epoch == right.worktree_epoch
}

fn handle_valid_origin_boundary(boundary: &HookLiveOriginBoundaryV1) -> bool {
    boundary.start.admission.host == boundary.admission.host
        && handle_valid_origin_observation(boundary.admission.host, &boundary.observation)
        && handle_same_live_origin_authority(&boundary.start.admission, &boundary.admission)
        && boundary.start.admission.order <= boundary.admission.order
        && boundary.start.admission.admitted_at.0 <= boundary.admission.admitted_at.0
        && boundary.start.physical_eof <= boundary.observation.physical_eof
        && boundary.start.checkpoint.complete_frontier <= boundary.start.physical_eof
        && boundary.start.checkpoint.complete_frontier
            <= boundary.observation.checkpoint.complete_frontier
        && boundary.start.checkpoint.file_identity == boundary.observation.checkpoint.file_identity
        && boundary.start.checkpoint.generation == boundary.observation.checkpoint.generation
}

fn handle_live_origin_proof_ref(
    baseline: &HookLiveOriginBoundaryV1,
    seal: &HookLiveOriginAdmissionV1,
    frames: &[HookLiveOriginFrameV1],
) -> Result<String, HookAdmissionLedgerError> {
    let bytes = canonical_json_bytes(&(baseline, seal, frames))
        .map_err(|_| HookAdmissionLedgerError::RecordUnencodable)?;
    Ok(format!(
        "hook-live-origin:{}",
        tracedecay_domain::canonical_text::encode_lowercase_hex(&frame_checksum(&bytes))
    ))
}

fn handle_read_live_origin_metadata(
    root: &File,
) -> Result<HandleLiveOriginMetadataV1, HookAdmissionLedgerError> {
    let Some(bytes) = read_relative_file(root, LIVE_ORIGINS_FILE, MAX_LIVE_ORIGIN_BYTES)? else {
        return Ok(HandleLiveOriginMetadataV1::default());
    };
    let metadata: HandleLiveOriginMetadataV1 =
        serde_json::from_slice(&bytes).map_err(|_| HookAdmissionLedgerError::RecordUndecodable)?;
    if metadata.baselines.len() > MAX_LIVE_ORIGIN_BOUNDARIES
        || metadata.proofs.len() > MAX_LIVE_ORIGIN_PROOFS
        || metadata
            .baselines
            .iter()
            .any(|boundary| !handle_valid_origin_boundary(boundary))
        || metadata.proofs.iter().any(|proof| {
            !handle_valid_origin_boundary(&proof.baseline)
                || !handle_same_live_origin_authority(&proof.baseline.admission, &proof.seal)
                || proof.baseline.admission.order >= proof.seal.order
                || proof.frames.is_empty()
                || proof.frames.len() > tracedecay_hooks::admission_ledger::MAX_LIVE_ORIGIN_FRAMES
                || proof.frames.iter().any(|frame| {
                    frame.start < proof.baseline.start.physical_eof || frame.start >= frame.end
                })
                || proof
                    .frames
                    .windows(2)
                    .any(|pair| pair[0].end != pair[1].start)
                || !handle_live_origin_proof_ref(&proof.baseline, &proof.seal, &proof.frames)
                    .is_ok_and(|expected| expected == proof.proof_ref)
        })
    {
        return Err(HookAdmissionLedgerError::RecordUndecodable);
    }
    Ok(metadata)
}

fn handle_scan_records(bytes: &[u8]) -> (Vec<HandleScannedRecord>, u64) {
    if bytes.len() < LEDGER_HEADER_BYTES
        || &bytes[..4] != LEDGER_MAGIC
        || u16::from_le_bytes([bytes[4], bytes[5]]) != LEDGER_FORMAT_VERSION
    {
        return (Vec::new(), bytes.len() as u64);
    }
    let mut records = Vec::new();
    let mut offset = LEDGER_HEADER_BYTES;
    while offset + LEDGER_RECORD_BYTES <= bytes.len() {
        let record = &bytes[offset..offset + LEDGER_RECORD_BYTES];
        let checksum = frame_checksum(&record[..LEDGER_RECORD_BODY_BYTES]);
        if checksum[..LEDGER_CHECKSUM_PREFIX_BYTES] != record[LEDGER_RECORD_BODY_BYTES..] {
            break;
        }
        let mut identity = [0u8; LEDGER_IDENTITY_BYTES];
        identity.copy_from_slice(&record[..LEDGER_IDENTITY_BYTES]);
        let mut digest = [0u8; LEDGER_DIGEST_BYTES];
        digest.copy_from_slice(
            &record[LEDGER_IDENTITY_BYTES..LEDGER_IDENTITY_BYTES + LEDGER_DIGEST_BYTES],
        );
        let mut admitted = [0u8; std::mem::size_of::<i64>()];
        admitted.copy_from_slice(
            &record[LEDGER_IDENTITY_BYTES + LEDGER_DIGEST_BYTES..LEDGER_RECORD_BODY_BYTES],
        );
        records.push((identity, digest, UtcMicros(i64::from_le_bytes(admitted))));
        offset += LEDGER_RECORD_BYTES;
    }
    (records, (bytes.len() - offset) as u64)
}

fn handle_is_expired(admitted_at: UtcMicros, now: UtcMicros) -> bool {
    now.0.saturating_sub(admitted_at.0) > tracedecay_hooks::MAX_SPOOL_AGE_MICROS
}

fn handle_read_validated_live_origin_metadata(
    root: &File,
    host: tracedecay_hooks::HookHostV1,
    now: UtcMicros,
) -> Result<HandleLiveOriginMetadataV1, HookAdmissionLedgerError> {
    let mut metadata = handle_read_live_origin_metadata(root)?;
    let Some(bytes) = read_relative_file(root, RECORDS_FILE, LEDGER_FILE_MAX_BYTES)? else {
        return Ok(HandleLiveOriginMetadataV1::default());
    };
    let (records, _) = handle_scan_records(&bytes);
    let retained = |receipt: &HookLiveOriginAdmissionV1| {
        receipt.host == host
            && receipt.admitted_at.0 <= now.0
            && !handle_is_expired(receipt.admitted_at, now)
            && records
                .iter()
                .enumerate()
                .any(|(order, (event_id, digest, admitted_at))| {
                    *event_id == receipt.event_id
                        && *digest == receipt.digest
                        && *admitted_at == receipt.admitted_at
                        && order as u64 == receipt.order
                })
    };
    metadata.proofs.retain(|proof| {
        retained(&proof.baseline.start.admission)
            && retained(&proof.baseline.admission)
            && retained(&proof.seal)
    });
    metadata
        .baselines
        .retain(|boundary| retained(&boundary.start.admission) && retained(&boundary.admission));
    Ok(metadata)
}

fn map_relative_component_error(
    error: &std::io::Error,
    subject: &'static str,
) -> ProviderHistoryErrorV1 {
    if is_symlink_error(error) || error.kind() == std::io::ErrorKind::InvalidInput {
        ProviderHistoryErrorV1::Ineligible(subject)
    } else {
        ProviderHistoryErrorV1::Unavailable(subject)
    }
}

fn open_relative_component(
    parent: &File,
    name: &OsStr,
    kind: PhysicalPathKind,
    subject: &'static str,
) -> HistoryResult<Option<(PhysicalPathIdentityV1, File)>> {
    let file = match open_relative_path(parent, name, kind) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(map_relative_component_error(&error, subject)),
    };
    let identity = PhysicalPathIdentityV1::from_open_file(
        file.try_clone()
            .map_err(|_| ProviderHistoryErrorV1::Unavailable(subject))?,
        subject,
        kind,
    )?;
    Ok(Some((identity, file)))
}

/// One composition-time binding from a provider built during core admission to
/// its actual host authority once the existing project journal is mounted.
/// Decisions are never retained; every call reaches the installed authority.
#[derive(Default)]
pub(crate) struct ProviderHistoryAuthorityMountV1 {
    authority: std::sync::OnceLock<
        Arc<dyn tracedecay_memory_provider_registry::AdvisoryAdmissionAuthority>,
    >,
}

impl ProviderHistoryAuthorityMountV1 {
    pub(crate) fn bind(
        &self,
        authority: Arc<dyn tracedecay_memory_provider_registry::AdvisoryAdmissionAuthority>,
    ) -> HistoryResult<()> {
        self.authority.set(authority).map_err(|_| {
            ProviderHistoryErrorV1::ClaimMismatch("provider history authority already mounted")
        })
    }
}

impl tracedecay_memory_provider_registry::AdvisoryAdmissionAuthority
    for ProviderHistoryAuthorityMountV1
{
    fn admit(
        &self,
        call: &tracedecay_memory_provider_registry::ProviderCall,
    ) -> Result<
        tracedecay_memory_provider_registry::CurrentAdvisoryAdmission,
        tracedecay_memory_provider_registry::AdvisoryAdmissionError,
    > {
        self.authority
            .get()
            .ok_or(
                tracedecay_memory_provider_registry::AdvisoryAdmissionError::Unavailable(
                    "provider history authority not mounted",
                ),
            )?
            .admit(call)
    }
}

/// Bounded reader of the existing host admission ledger. This object can be
/// shared by live capture and background ingestion; neither path consults the
/// mutable checkout to invent an original event.
pub(crate) struct HookOriginReaderV1 {
    data_root: PathBuf,
    brain_id: BrainId,
    profile_id: UserProfileId,
    data_root_identity: OnceLock<PhysicalPathIdentityV1>,
    claude_ledger_root_identity: OnceLock<PhysicalPathIdentityV1>,
    codex_ledger_root_identity: OnceLock<PhysicalPathIdentityV1>,
}

impl HookOriginReaderV1 {
    pub(crate) fn new(data_root: PathBuf, brain_id: BrainId, profile_id: UserProfileId) -> Self {
        Self {
            data_root,
            brain_id,
            profile_id,
            data_root_identity: OnceLock::new(),
            claude_ledger_root_identity: OnceLock::new(),
            codex_ledger_root_identity: OnceLock::new(),
        }
    }

    /// Binds this reader to the same logical project-session shard that owns
    /// the mounted canonical ports. A path and profile label alone are not
    /// enough: a reader from another brain must fail before any control budget
    /// is consulted.
    pub(crate) fn validate_mount_identity(
        &self,
        profile_id: &UserProfileId,
        mounted_scope: &ResolvedScope,
        registered_shard: &StoreShardIdV1,
    ) -> HistoryResult<()> {
        validate_registered_history_shard(profile_id, mounted_scope, registered_shard)?;
        if self.brain_id != registered_shard.brain_id || self.profile_id != *profile_id {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "history reader identity",
            ));
        }
        Ok(())
    }

    /// The hook ledger is a host-owned sibling of the retained provider data.
    /// Keep the reader physically bound to the root selected during project
    /// composition so a reopened authority cannot read another project's
    /// live-origin ledger.
    pub(crate) fn validate_data_root(&self, expected: &Path) -> HistoryResult<()> {
        let actual = PhysicalPathIdentityV1::capture(
            &self.data_root,
            "history reader data root",
            PhysicalPathKind::Directory,
        )?;
        let expected_identity = PhysicalPathIdentityV1::capture(
            expected,
            "history data root",
            PhysicalPathKind::Directory,
        )?;
        if actual != expected_identity {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "history reader data root",
            ));
        }
        let bound = self.data_root_identity.get_or_init(|| actual);
        if *bound != actual || *bound != expected_identity {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "history reader data root",
            ));
        }
        Ok(())
    }

    pub(crate) fn bind_data_root(&self, expected: &Path) -> HistoryResult<PhysicalPathIdentityV1> {
        self.validate_data_root(expected)?;
        self.data_root_identity
            .get()
            .copied()
            .ok_or(ProviderHistoryErrorV1::Unavailable(
                "history reader data root",
            ))
    }

    fn validate_bound_data_root(&self) -> HistoryResult<()> {
        let expected = self.data_root.clone();
        self.validate_data_root(&expected)
    }

    fn host(provider: &str) -> Option<tracedecay_hooks::HookHostV1> {
        match provider {
            "claude" => Some(tracedecay_hooks::HookHostV1::ClaudeCode),
            "codex" => Some(tracedecay_hooks::HookHostV1::Codex),
            _ => None,
        }
    }

    fn ledger_root(&self, host: tracedecay_hooks::HookHostV1) -> PathBuf {
        self.data_root
            .join("hook-v2-admissions")
            .join(host.hook_key())
    }

    fn validated_ledger_root(&self, host: tracedecay_hooks::HookHostV1) -> HistoryResult<PathBuf> {
        let root = self.ledger_root(host);
        tracedecay_runtime_core::storage::reject_symlink_components(
            &root,
            "history reader ledger root",
        )
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::InvalidInput {
                ProviderHistoryErrorV1::Ineligible("history reader ledger root")
            } else {
                ProviderHistoryErrorV1::Unavailable("history reader ledger root")
            }
        })?;
        Ok(root)
    }

    fn ledger_root_identity_slot(
        &self,
        host: tracedecay_hooks::HookHostV1,
    ) -> &OnceLock<PhysicalPathIdentityV1> {
        match host {
            tracedecay_hooks::HookHostV1::ClaudeCode => &self.claude_ledger_root_identity,
            tracedecay_hooks::HookHostV1::Codex => &self.codex_ledger_root_identity,
            _ => unreachable!("history reader only admits Claude and Codex ledger hosts"),
        }
    }

    fn bind_ledger_root_identity(
        &self,
        host: tracedecay_hooks::HookHostV1,
        identity: PhysicalPathIdentityV1,
    ) -> HistoryResult<()> {
        let bound = self
            .ledger_root_identity_slot(host)
            .get_or_init(|| identity);
        if *bound != identity {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "history reader ledger root changed",
            ));
        }
        Ok(())
    }

    fn open_ledger_root_handle(
        &self,
        host: tracedecay_hooks::HookHostV1,
    ) -> HistoryResult<Option<(PhysicalPathIdentityV1, HandleRelativeLedgerRoot)>> {
        let expected_data_root =
            self.data_root_identity
                .get()
                .copied()
                .ok_or(ProviderHistoryErrorV1::Unavailable(
                    "history reader data root",
                ))?;
        let (data_root_identity, data_root) = PhysicalPathIdentityV1::capture_open(
            &self.data_root,
            "history reader data root",
            PhysicalPathKind::Directory,
        )?;
        if data_root_identity != expected_data_root {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "history reader data root",
            ));
        }
        let Some((_, admissions)) = open_relative_component(
            &data_root,
            OsStr::new("hook-v2-admissions"),
            PhysicalPathKind::Directory,
            "history reader admissions root",
        )?
        else {
            return Ok(None);
        };
        let Some((ledger_identity, ledger)) = open_relative_component(
            &admissions,
            OsStr::new(host.hook_key()),
            PhysicalPathKind::Directory,
            "history reader ledger root",
        )?
        else {
            return Ok(None);
        };
        Ok(Some((
            ledger_identity,
            HandleRelativeLedgerRoot {
                _data_root: data_root,
                _admissions: admissions,
                ledger,
            },
        )))
    }

    /// Reads one host ledger from a retained directory-handle chain. Path
    /// validation remains a fail-closed admission check, while every actual
    /// descendant open is handle-relative and no-follow. The bounded identity
    /// retry still rejects a root swap observed around the operation.
    fn read_ledger_with_identity<T>(
        &self,
        host: tracedecay_hooks::HookHostV1,
        unavailable_subject: &'static str,
        mut read: impl FnMut(
            Option<&HandleRelativeLedgerRoot>,
            tracedecay_hooks::HookHostV1,
        ) -> std::result::Result<
            T,
            tracedecay_hooks::admission_ledger::HookAdmissionLedgerError,
        >,
    ) -> HistoryResult<T> {
        for attempt in 0..MAX_LEDGER_IDENTITY_READ_ATTEMPTS {
            self.validate_bound_data_root()?;
            let root = self.validated_ledger_root(host)?;
            let before = self.capture_ledger_root_identity(&root)?;
            if let Some(identity) = before {
                // Bind before opening descendants. If the parent is swapped
                // while the child read is in flight, a retry must still be
                // compared with the object admitted on this first look.
                self.bind_ledger_root_identity(host, identity)?;
            }
            let opened = self.open_ledger_root_handle(host)?;
            let opened_identity = opened.as_ref().map(|(identity, _)| *identity);
            if before != opened_identity {
                if attempt + 1 < MAX_LEDGER_IDENTITY_READ_ATTEMPTS {
                    std::thread::yield_now();
                    continue;
                }
                return Err(ProviderHistoryErrorV1::Ineligible(
                    "history reader ledger root changed",
                ));
            }
            let result = read(opened.as_ref().map(|(_, root)| root), host);
            // The data root and ledger root are both checked after the child
            // opens. This is the post-read half of the TOCTOU boundary.
            self.validate_bound_data_root()?;
            let after = self.capture_ledger_root_identity(&root)?;
            if before != after {
                // A root that was absent when the operation began and appeared
                // during the read has no previously admitted identity. Do not
                // let the retry turn that race into a newly trusted root.
                if before.is_none() && after.is_some() {
                    return Err(ProviderHistoryErrorV1::Ineligible(
                        "history reader ledger root changed",
                    ));
                }
                if attempt + 1 < MAX_LEDGER_IDENTITY_READ_ATTEMPTS {
                    std::thread::yield_now();
                    continue;
                }
                return Err(ProviderHistoryErrorV1::Ineligible(
                    "history reader ledger root changed",
                ));
            }

            match before {
                Some(identity) => self.bind_ledger_root_identity(host, identity)?,
                None if self.ledger_root_identity_slot(host).get().is_some() => {
                    return Err(ProviderHistoryErrorV1::Ineligible(
                        "history reader ledger root changed",
                    ));
                }
                None => {}
            }
            return result.map_err(|_| ProviderHistoryErrorV1::Unavailable(unavailable_subject));
        }
        Err(ProviderHistoryErrorV1::Ineligible(
            "history reader ledger root changed",
        ))
    }

    fn capture_ledger_root_identity(
        &self,
        root: &Path,
    ) -> HistoryResult<Option<PhysicalPathIdentityV1>> {
        match std::fs::symlink_metadata(root) {
            Ok(_) => PhysicalPathIdentityV1::capture(
                root,
                "history reader ledger root",
                PhysicalPathKind::Directory,
            )
            .map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(_) => Err(ProviderHistoryErrorV1::Unavailable(
                "history reader ledger root",
            )),
        }
    }

    fn proofs(
        &self,
        host: tracedecay_hooks::HookHostV1,
    ) -> HistoryResult<Vec<tracedecay_hooks::admission_ledger::HookLiveOriginProofV1>> {
        self.read_ledger_with_identity(host, "original live-event ledger", |root, host| {
            root.map_or_else(
                || Ok(Vec::new()),
                |root| root.read_proofs(host, tracedecay_contracts::now_micros()),
            )
        })
    }

    fn boundary_matches_profile(
        &self,
        boundary: &tracedecay_hooks::admission_ledger::HookLiveOriginBoundaryV1,
    ) -> bool {
        boundary.observation.scope.brain_id == self.brain_id
            && boundary.observation.scope.profile_id == self.profile_id
    }

    fn resolve_source(
        &self,
        identity: &tracedecay_domain::ObservationIdentityMaterialV1,
        resume_checkpoint: Option<(u64, u64)>,
        authority_ref: Option<&str>,
    ) -> HistoryResult<
        Option<tracedecay_sessions::repository_provenance::OriginalObservationEvidenceV1>,
    > {
        self.validate_bound_data_root()?;
        identity
            .validate()
            .map_err(|_| ProviderHistoryErrorV1::Ineligible("original source identity"))?;
        let Some(host) = Self::host(identity.source().provider().as_str()) else {
            return Ok(None);
        };
        let Some((file_identity, fingerprint)) = resume_checkpoint else {
            return Ok(None);
        };
        let mut matched = None;
        for proof in self.proofs(host)? {
            let boundary = &proof.baseline;
            let baseline = &boundary.observation;
            if !self.boundary_matches_profile(boundary)
                || authority_ref.is_some_and(|expected| expected != proof.proof_ref)
                || baseline.source != *identity.source()
                || baseline
                    .scope
                    .repository
                    .project_id()
                    .map(|id| ObservationScopeV1::Project {
                        project_id: id.clone(),
                    })
                    .as_ref()
                    != Some(identity.scope())
                || baseline.checkpoint.generation != identity.generation().generation_id()
                || baseline.checkpoint.file_identity != file_identity
                || !proof.frames.iter().any(|frame| {
                    frame.start == identity.position().start()
                        && frame.end == identity.position().end()
                        && frame.start >= boundary.start.physical_eof
                        && frame.resume_fingerprint == fingerprint
                })
            {
                continue;
            }
            let evidence =
                tracedecay_sessions::repository_provenance::OriginalObservationEvidenceV1 {
                    repository: baseline.scope.repository.clone(),
                    authority_ref: proof.proof_ref,
                };
            if matched.is_some() {
                return Err(ProviderHistoryErrorV1::Ineligible(
                    "ambiguous original event proof",
                ));
            }
            matched = Some(evidence);
        }
        Ok(matched)
    }

    pub(crate) fn live_boundaries(
        &self,
    ) -> HistoryResult<Vec<tracedecay_hooks::admission_ledger::HookLiveOriginBoundaryV1>> {
        self.validate_bound_data_root()?;
        let mut boundaries = Vec::new();
        for host in [
            tracedecay_hooks::HookHostV1::ClaudeCode,
            tracedecay_hooks::HookHostV1::Codex,
        ] {
            let current =
                self.read_ledger_with_identity(host, "live session boundary", |root, host| {
                    root.map_or_else(
                        || Ok(Vec::new()),
                        |root| root.read_boundaries(host, tracedecay_contracts::now_micros()),
                    )
                })?;
            boundaries.extend(
                current
                    .into_iter()
                    .filter(|boundary| self.boundary_matches_profile(boundary)),
            );
            boundaries.extend(
                self.proofs(host)?
                    .into_iter()
                    .map(|proof| proof.baseline)
                    .filter(|boundary| self.boundary_matches_profile(boundary)),
            );
        }
        Ok(boundaries)
    }
}

impl tracedecay_sessions::repository_provenance::OriginalObservationProvenanceResolverV1
    for HookOriginReaderV1
{
    fn resolve(
        &self,
        identity: &tracedecay_domain::ObservationIdentityMaterialV1,
        resume_checkpoint: Option<(u64, u64)>,
    ) -> Option<tracedecay_sessions::repository_provenance::OriginalObservationEvidenceV1> {
        self.resolve_source(identity, resume_checkpoint, None)
            .ok()
            .flatten()
    }
}

/// Policy revision one permits history only between independently admitted
/// live sessions in the current exact checkout and profile.
pub(crate) struct MountedOriginalObservationAuthorityV1 {
    pub(crate) reader: Arc<HookOriginReaderV1>,
    pub(crate) bridge: Arc<HistoryIdentityBridgeV1>,
}

impl OriginalObservationAuthorityV1 for MountedOriginalObservationAuthorityV1 {
    fn validate_live_mount(&self) -> HistoryResult<()> {
        self.reader.validate_bound_data_root()?;
        self.bridge.revalidate()
    }

    fn validate_original_event(
        &self,
        authority_ref: &str,
        stored: &StoredObservation,
    ) -> HistoryResult<bool> {
        let cursor = stored.committed_cursor();
        let checkpoint = cursor.file_identity().zip(cursor.resume_fingerprint());
        let Some(evidence) = self.reader.resolve_source(
            stored.observation().identity(),
            checkpoint,
            Some(authority_ref),
        )?
        else {
            return Ok(false);
        };
        let attachment = stored
            .validated_repository_provenance_attachment()
            .map_err(|_| ProviderHistoryErrorV1::Ineligible("original provenance attachment"))?;
        Ok(
            matches!(attachment.availability(), EvidenceAvailabilityV1::Known(binding) if binding.capture() == &evidence.repository),
        )
    }

    fn authorizes_session(
        &self,
        source_session: &str,
        destination: &OwnedExactScope,
    ) -> HistoryResult<bool> {
        self.bridge.revalidate()?;
        self.bridge.validate_destination(destination)?;
        let mut source_admitted = false;
        let mut destination_admitted = false;
        for boundary in self.reader.live_boundaries()? {
            let repository = &boundary.observation.scope.repository;
            if repository.project_id() != Some(&self.bridge.canonical_project)
                || repository.repository_id() != &self.bridge.canonical_repository
                || repository.worktree_id() != Some(&self.bridge.canonical_worktree)
                || repository.evidence().attached_ref().value()
                    != self.bridge.scope.reference.as_ref()
            {
                continue;
            }
            let session = boundary.observation.source.session_id().as_str();
            source_admitted |= session == source_session;
            destination_admitted |= self
                .bridge
                .destination(session)
                .is_ok_and(|scope| &scope == destination);
        }
        Ok(source_admitted && destination_admitted)
    }
}

impl MountedOriginalObservationAuthorityV1 {
    /// Checks the complete composition binding before a caller's control token
    /// is read. This includes the registered brain/profile/project, the hook
    /// reader pairing and the bridge's own retained identity.
    pub(crate) fn validate_mount(
        &self,
        profile_id: &UserProfileId,
        mounted_scope: &ResolvedScope,
        registered_shard: &StoreShardIdV1,
    ) -> HistoryResult<()> {
        self.reader.validate_bound_data_root()?;
        self.reader
            .validate_mount_identity(profile_id, mounted_scope, registered_shard)?;
        self.bridge.validate_registered_shard(registered_shard)?;
        self.bridge.validate_reader(self.reader.as_ref())?;
        if self.bridge.profile_id != *profile_id || self.bridge.scope != *mounted_scope {
            return Err(ProviderHistoryErrorV1::Ineligible("original bridge mount"));
        }
        Ok(())
    }

    pub(crate) fn validate_registered_mount(
        &self,
        registered_shard: &StoreShardIdV1,
    ) -> HistoryResult<()> {
        self.validate_mount(
            &self.bridge.profile_id,
            &self.bridge.scope,
            registered_shard,
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProviderHistoryErrorV1 {
    #[error("history authority unavailable: {0}")]
    Unavailable(&'static str),
    #[error("history source is ineligible: {0}")]
    Ineligible(&'static str),
    #[error("history claim differs from its current authority: {0}")]
    ClaimMismatch(&'static str),
    #[error("history operation stopped: {0:?}")]
    Control(tracedecay_memory_provider_registry::TerminalCode),
}

type HistoryResult<T> = Result<T, ProviderHistoryErrorV1>;

fn validate_registered_history_shard(
    profile_id: &UserProfileId,
    mounted_scope: &ResolvedScope,
    registered_shard: &StoreShardIdV1,
) -> HistoryResult<()> {
    mounted_scope
        .validate()
        .map_err(|_| ProviderHistoryErrorV1::Ineligible("mounted scope"))?;
    if registered_shard.profile_id != *profile_id
        || registered_shard.scope
            != (StoreShardScopeV1::ProjectSessions {
                project_id: mounted_scope.project_id.clone(),
            })
    {
        return Err(ProviderHistoryErrorV1::Ineligible(
            "registered project/profile",
        ));
    }
    Ok(())
}

/// Existing durable live-event authority supplied by the host. Implementations
/// must resolve the receipt and its exact source identity/range; a structurally
/// valid attachment or a matching session label alone cannot return true.
pub(crate) trait OriginalObservationAuthorityV1: Send + Sync {
    /// Revalidates the composition before a reader observes the caller's
    /// control budget. Test authorities may keep the default because they do
    /// not own the host filesystem binding.
    fn validate_live_mount(&self) -> HistoryResult<()> {
        Ok(())
    }

    fn validate_original_event(
        &self,
        authority_ref: &str,
        observation: &StoredObservation,
    ) -> HistoryResult<bool>;

    /// Existing profile/session policy authorizes each original session for this
    /// exact destination. No missing grant means an implicit checkout-wide grant.
    fn authorizes_session(
        &self,
        canonical_session_id: &str,
        destination: &OwnedExactScope,
    ) -> HistoryResult<bool>;
}

/// Synchronous journal-dispatch adapter over the same canonical reader above.
/// The mounted host implements this using its retained runtime and registered
/// observation/disposition/event ports; no provider reply can supply it.
pub(crate) trait HistoryGrantRevalidationV1: Send + Sync {
    fn revalidate(
        &self,
        provider_id: &OwnedProviderId,
        grant: &HistoryGrant,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<()>;

    fn revalidate_async<'a>(
        &'a self,
        provider_id: &'a OwnedProviderId,
        grant: &'a HistoryGrant,
        control: &'a tracedecay_memory_provider_registry::OperationControl,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = HistoryResult<()>> + Send + 'a>>;
}

/// Physical Git topology retained with the bridge. Repository IDs are derived
/// from paths, so path equality alone would let a newly-created `.git` at the
/// same spelling inherit an already-admitted bridge.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct GitRepositoryIdentityV1 {
    project_root: PhysicalPathIdentityV1,
    git_entry: PhysicalPathIdentityV1,
    git_dir: PhysicalPathIdentityV1,
    common_dir: PhysicalPathIdentityV1,
}

impl GitRepositoryIdentityV1 {
    fn capture(project_root: &Path) -> HistoryResult<Self> {
        let topology =
            tracedecay_runtime_core::git_repository::repository_topology(project_root)
                .map_err(|_| ProviderHistoryErrorV1::Unavailable("git repository identity"))?;
        let git_entry = PhysicalPathIdentityV1::capture(
            &project_root.join(".git"),
            "git repository entry",
            PhysicalPathKind::Entry,
        )?;
        let git_dir = PhysicalPathIdentityV1::capture(
            &topology.git_dir,
            "git directory",
            PhysicalPathKind::Directory,
        )?;
        let common_dir = PhysicalPathIdentityV1::capture(
            &topology.common_dir,
            "git common directory",
            PhysicalPathKind::Directory,
        )?;
        let project_root = PhysicalPathIdentityV1::capture(
            project_root,
            "git project root",
            PhysicalPathKind::Directory,
        )?;
        Ok(Self {
            project_root,
            git_entry,
            git_dir,
            common_dir,
        })
    }

    fn validate_current(&self, project_root: &Path) -> HistoryResult<()> {
        let current = Self::capture(project_root)?;
        if current != *self {
            return Err(ProviderHistoryErrorV1::Ineligible("git repository changed"));
        }
        Ok(())
    }
}

/// The current Git content identity is separate from the repository topology.
/// A process can replace HEAD/ref or the index in-place while preserving the
/// `.git` directory, its common directory and the attached ref spelling.
#[derive(Clone, Debug, PartialEq, Eq)]
struct GitContentIdentityV1 {
    head_commit: EvidenceAvailabilityV1<CommitId>,
    index_tree: EvidenceAvailabilityV1<TreeId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ResolvedGitBridgeV1 {
    canonical_project: ProjectId,
    canonical_repository: RepositoryId,
    canonical_worktree: WorktreeId,
    content: GitContentIdentityV1,
}

/// One admitted bridge between the two existing identity schemes. Source IDs
/// are checked in the canonical namespace; daemon IDs are independently resolved
/// and checked against the mounted scope. The registered brain is retained so
/// a bridge cannot be paired with a reader or database from another authority.
pub(crate) struct HistoryIdentityBridgeV1 {
    project_root: PathBuf,
    brain_id: BrainId,
    git_identity: GitRepositoryIdentityV1,
    git_content: GitContentIdentityV1,
    profile_id: UserProfileId,
    scope: ResolvedScope,
    canonical_project: ProjectId,
    canonical_repository: RepositoryId,
    canonical_worktree: WorktreeId,
}

impl HistoryIdentityBridgeV1 {
    pub(crate) fn admit(
        project_root: &Path,
        profile_id: &UserProfileId,
        mounted_scope: &ResolvedScope,
        registered_shard: &StoreShardIdV1,
    ) -> HistoryResult<Self> {
        validate_registered_history_shard(profile_id, mounted_scope, registered_shard)?;
        let resolved = Self::resolve_bridge(project_root, mounted_scope)?;
        let git_identity = GitRepositoryIdentityV1::capture(project_root)?;
        Ok(Self {
            project_root: project_root.to_owned(),
            brain_id: registered_shard.brain_id.clone(),
            git_identity,
            git_content: resolved.content,
            profile_id: profile_id.clone(),
            scope: mounted_scope.clone(),
            canonical_project: resolved.canonical_project,
            canonical_repository: resolved.canonical_repository,
            canonical_worktree: resolved.canonical_worktree,
        })
    }

    pub(crate) fn validate_registered_shard(
        &self,
        registered_shard: &StoreShardIdV1,
    ) -> HistoryResult<()> {
        if self.brain_id != registered_shard.brain_id
            || self.profile_id != registered_shard.profile_id
            || registered_shard.scope
                != (StoreShardScopeV1::ProjectSessions {
                    project_id: self.scope.project_id.clone(),
                })
        {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "registered history identity",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_reader(&self, reader: &HookOriginReaderV1) -> HistoryResult<()> {
        if self.brain_id != reader.brain_id || self.profile_id != reader.profile_id {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "history reader identity",
            ));
        }
        Ok(())
    }

    fn resolve_bridge(
        project_root: &Path,
        mounted: &ResolvedScope,
    ) -> HistoryResult<ResolvedGitBridgeV1> {
        let resolved = tracedecay_code_index_runtime::resolved_scope_for_project(
            project_root,
            &mounted.project_id,
        )
        .map_err(|_| ProviderHistoryErrorV1::Unavailable("daemon scope"))?;
        if &resolved != mounted {
            return Err(ProviderHistoryErrorV1::Ineligible("mounted scope changed"));
        }
        let marker =
            tracedecay_runtime_core::storage::read_repository_identity_marker(project_root)
                .map_err(|_| ProviderHistoryErrorV1::Unavailable("repository marker"))?
                .ok_or(ProviderHistoryErrorV1::Unavailable("repository marker"))?;
        let context = RepositoryProvenanceAdmissionContext::from_authoritative_project_marker(
            project_root,
            &mounted.project_id,
            &marker,
        )
        .ok_or(ProviderHistoryErrorV1::Ineligible(
            "repository marker binding",
        ))?;
        // Probe validates the marker's common directory against the actual
        // repository before admitting its project-salted identity mapping.
        let capture = context.capture_snapshot(tracedecay_contracts::now_micros());
        let EvidenceAvailabilityV1::Known(capture) = capture.availability() else {
            return Err(ProviderHistoryErrorV1::Unavailable(
                "current repository capture",
            ));
        };
        if capture.evidence().attached_ref().value() != mounted.reference.as_ref() {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "current reference binding",
            ));
        }
        let (canonical_project, canonical_repository, canonical_worktree) = context
            .admitted_identity()
            .ok_or(ProviderHistoryErrorV1::Unavailable(
                "canonical repository identity",
            ))?;
        Ok(ResolvedGitBridgeV1 {
            canonical_project,
            canonical_repository,
            canonical_worktree,
            content: GitContentIdentityV1 {
                head_commit: capture.evidence().head_commit().clone(),
                index_tree: capture.evidence().index_tree().clone(),
            },
        })
    }

    /// Fresh marker and daemon scope read. Cached construction is not proof that
    /// a later checkout, worktree replacement or branch still matches.
    pub(crate) fn revalidate(&self) -> HistoryResult<()> {
        self.git_identity.validate_current(&self.project_root)?;
        let current = Self::resolve_bridge(&self.project_root, &self.scope)?;
        if current.content != self.git_content {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "git commit/tree changed",
            ));
        }
        if (
            current.canonical_project,
            current.canonical_repository,
            current.canonical_worktree,
        ) != (
            self.canonical_project.clone(),
            self.canonical_repository.clone(),
            self.canonical_worktree.clone(),
        ) {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "identity bridge changed",
            ));
        }
        Ok(())
    }

    pub(crate) fn destination(&self, canonical_session_id: &str) -> HistoryResult<OwnedExactScope> {
        exact_scope_for_session(&self.profile_id, &self.scope, canonical_session_id)
            .map_err(|_| ProviderHistoryErrorV1::Ineligible("destination scope"))
    }

    /// Freshly checks the retained namespace against actual mounted identities.
    /// The caller keeps the original agent-session identity carried by the trace.
    pub(crate) fn authorize_control_scope(
        &self,
        destination: &OwnedExactScope,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<()> {
        // The bridge and its reader are composition authorities. Revalidate
        // them before observing the caller's budget, so a stale mount cannot
        // turn a canceled control into a filesystem-backed authorization.
        self.revalidate()?;
        control
            .snapshot()
            .map_err(ProviderHistoryErrorV1::Control)?;
        control
            .snapshot()
            .map_err(ProviderHistoryErrorV1::Control)?;
        self.validate_destination(destination)?;
        control
            .snapshot()
            .map_err(ProviderHistoryErrorV1::Control)?;
        Ok(())
    }

    fn validate_destination(&self, destination: &OwnedExactScope) -> HistoryResult<()> {
        destination
            .validate()
            .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("destination"))?;
        let scope = &self.scope;
        if destination.profile_id != self.profile_id.as_str()
            || destination.project_id != scope.project_id.as_str()
            || destination.repository_identity != scope.repository_id.as_str()
            || destination.worktree_identity != scope.worktree_id.as_str()
            || Some(destination.branch_identity.as_str())
                != scope.reference.as_ref().map(|v| v.as_str())
            || destination.resolved_scope_digest != scope.scope_digest.as_str()
        {
            return Err(ProviderHistoryErrorV1::Ineligible("destination checkout"));
        }
        Ok(())
    }

    fn original_scope<O: OriginalObservationAuthorityV1 + ?Sized>(
        &self,
        stored: &StoredObservation,
        original_authority: &O,
    ) -> HistoryResult<OriginScopeEvidence> {
        let attachment = stored
            .validated_repository_provenance_attachment()
            .map_err(|_| ProviderHistoryErrorV1::Ineligible("original attachment binding"))?;
        let authority_ref = match attachment.origin() {
            ObservationOriginV1::Unavailable => return Ok(OriginScopeEvidence::Unavailable),
            ObservationOriginV1::IngestionOnly => return Ok(OriginScopeEvidence::IngestionOnly),
            ObservationOriginV1::Recorded { authority_ref, .. } => authority_ref,
        };
        if !original_authority.validate_original_event(authority_ref, stored)? {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "original live-event receipt",
            ));
        }
        let EvidenceAvailabilityV1::Known(binding) = attachment.availability() else {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "partial original repository capture",
            ));
        };
        let original = binding.capture();
        if original.project_id() != Some(&self.canonical_project)
            || original.repository_id() != &self.canonical_repository
            || original.worktree_id() != Some(&self.canonical_worktree)
            || original.evidence().attached_ref().value() != self.scope.reference.as_ref()
        {
            return Err(ProviderHistoryErrorV1::Ineligible("original checkout"));
        }
        Ok(OriginScopeEvidence::Recorded {
            scope: self.destination(stored.observation().source().session_id().as_str())?,
            authority_ref: authority_ref.clone(),
        })
    }
}

/// Bounded canonical page with truthful exclusions. Progress is later committed
/// by the destination's existing observation journey, never by this reader.
pub(crate) struct ProviderHistoryPageV1 {
    pub(crate) records: Vec<StoredObservation>,
    pub(crate) grant: Option<HistoryGrant>,
    pub(crate) last_scanned_sequence: u64,
    pub(crate) scanned: usize,
    pub(crate) withheld: usize,
    pub(crate) unknown_revision: usize,
    pub(crate) has_more: bool,
    pub(crate) has_older: bool,
}

pub(crate) struct ProviderHistoryReaderV1<'a, S: ?Sized, D: ?Sized, O: ?Sized> {
    pub(crate) bridge: &'a HistoryIdentityBridgeV1,
    pub(crate) observations: &'a S,
    pub(crate) dispositions: &'a D,
    pub(crate) original_authority: &'a O,
    pub(crate) journal: &'a SqliteObservationJournal,
    pub(crate) provider_id: &'a OwnedProviderId,
    pub(crate) policy_revision: u64,
}

/// Real installed provider authority over the mounted canonical ports and
/// existing journal. The runtime handle is borrowed from daemon composition.
pub(crate) struct ProviderHistoryAuthorityV1<S, D> {
    pub(crate) mounted_scope: ResolvedScope,
    pub(crate) profile_id: UserProfileId,
    pub(crate) registered_shard: StoreShardIdV1,
    pub(crate) observations: Arc<S>,
    pub(crate) dispositions: Arc<D>,
    pub(crate) original_authority: Option<Arc<MountedOriginalObservationAuthorityV1>>,
    pub(crate) journal: Arc<SqliteObservationJournal>,
    pub(crate) provider_id: OwnedProviderId,
    pub(crate) policy_revision: u64,
    pub(crate) runtime: tokio::runtime::Handle,
}

impl<S, D> ProviderHistoryAuthorityV1<S, D>
where
    S: ObservationAdmissionPort,
    D: RetrievalAnchorDispositionStore,
{
    pub(crate) fn reader(
        &self,
    ) -> HistoryResult<ProviderHistoryReaderV1<'_, S, D, MountedOriginalObservationAuthorityV1>>
    {
        self.validate_mount()?;
        let original_authority =
            self.original_authority
                .as_ref()
                .ok_or(ProviderHistoryErrorV1::Unavailable(
                    "original repository provenance",
                ))?;
        Ok(ProviderHistoryReaderV1 {
            bridge: original_authority.bridge.as_ref(),
            observations: self.observations.as_ref(),
            dispositions: self.dispositions.as_ref(),
            original_authority: original_authority.as_ref(),
            journal: self.journal.as_ref(),
            provider_id: &self.provider_id,
            policy_revision: self.policy_revision,
        })
    }

    pub(crate) fn validate_mount(&self) -> HistoryResult<()> {
        validate_registered_history_shard(
            &self.profile_id,
            &self.mounted_scope,
            &self.registered_shard,
        )?;
        if let Some(original) = self.original_authority.as_ref() {
            original.validate_mount(
                &self.profile_id,
                &self.mounted_scope,
                &self.registered_shard,
            )?;
            // The repository marker and current branch are mutable authorities;
            // construction-time identity is not enough after a reopen.
            original.bridge.revalidate()?;
        }
        Ok(())
    }

    fn validate_call_scope(&self, destination: &OwnedExactScope) -> HistoryResult<()> {
        self.validate_mount()?;
        destination
            .validate()
            .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("destination"))?;
        let scope = &self.mounted_scope;
        if destination.profile_id != self.profile_id.as_str()
            || destination.project_id != scope.project_id.as_str()
            || destination.repository_identity != scope.repository_id.as_str()
            || destination.worktree_identity != scope.worktree_id.as_str()
            || Some(destination.branch_identity.as_str())
                != scope.reference.as_ref().map(|v| v.as_str())
            || destination.resolved_scope_digest != scope.scope_digest.as_str()
        {
            return Err(ProviderHistoryErrorV1::Ineligible("destination checkout"));
        }
        Ok(())
    }

    fn validate_provider(&self, provider: &OwnedProviderId) -> HistoryResult<()> {
        if provider != &self.provider_id {
            return Err(ProviderHistoryErrorV1::ClaimMismatch("mounted provider"));
        }
        Ok(())
    }

    fn retained_admission(
        &self,
        key: &str,
        destination: &OwnedExactScope,
        registration_revision: u64,
    ) -> HistoryResult<tracedecay_memory_observation::AdmittedObservationV1> {
        let key = tracedecay_memory_observation::ObservationIdempotencyKeyV1::parse(key)
            .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("journal admission key"))?;
        let admitted = self
            .journal
            .read_admitted_observation_by_idempotency(&key)
            .map_err(|_| ProviderHistoryErrorV1::Unavailable("retained sanitized admission"))?
            .ok_or(ProviderHistoryErrorV1::Ineligible(
                "retained sanitized admission missing",
            ))?;
        if admitted.target.provider_id != self.provider_id
            || admitted.target.registration_revision != registration_revision
            || &admitted.exact_scope != destination
        {
            return Err(ProviderHistoryErrorV1::ClaimMismatch(
                "retained admission destination",
            ));
        }
        Ok(admitted)
    }

    async fn admit_current(
        &self,
        call: &tracedecay_memory_provider_registry::ProviderCall,
    ) -> Result<
        tracedecay_memory_provider_registry::CurrentAdvisoryAdmission,
        tracedecay_memory_provider_registry::AdvisoryAdmissionError,
    > {
        use tracedecay_memory_provider_registry::{
            AdvisoryAdmissionError, CurrentAdvisoryAdmission, CurrentRestoreAdmission,
            ProviderOperation,
        };
        call.validate()?;
        // A reopened or manually composed authority must reject a stale or
        // cross-bound reader before it consults the caller's control token.
        self.validate_mount().map_err(advisory_error)?;
        call.control
            .snapshot()
            .map_err(AdvisoryAdmissionError::Control)?;
        self.validate_provider(&call.provider_id)
            .map_err(advisory_error)?;
        self.validate_call_scope(&call.exact_scope)
            .map_err(advisory_error)?;
        let payload: Value = serde_json::from_slice(&call.payload.bytes)
            .map_err(|_| AdvisoryAdmissionError::Invalid("canonical advisory payload"))?;
        let payload_claimed = payload
            .get("history_grant")
            .filter(|value| !value.is_null())
            .map(history_grant_from_json)
            .transpose()
            .map_err(advisory_error)?;
        // Both carriers are claims. Every accepted source is resolved again by
        // the operation's existing canonical admission path below.
        let claimed = match (call.history_grant(), payload_claimed.as_ref()) {
            (Some(private), Some(payload)) if private != payload => {
                return Err(AdvisoryAdmissionError::Denied("conflicting history grants"));
            }
            (Some(private), _) => Some(private),
            (None, payload) => payload,
        };
        if claimed.is_some_and(|grant| grant.destination_scope != call.exact_scope) {
            return Err(AdvisoryAdmissionError::Denied("history destination"));
        }
        match call.operation {
            ProviderOperation::Observe => {
                let key = call
                    .idempotency_key
                    .as_deref()
                    .ok_or(AdvisoryAdmissionError::Invalid("observation key"))?;
                let admitted = self
                    .retained_admission(key, &call.exact_scope, call.registration_revision)
                    .map_err(advisory_error)?;
                if admitted.payload != call.payload
                    || admitted.extensions != call.extensions
                    || admitted.observation_id.as_str() != call.request_id
                    || call
                        .sanitization()
                        .map(|receipt| receipt.to_json())
                        .as_deref()
                        != Some(&admitted.sanitization.receipt_json)
                {
                    return Err(AdvisoryAdmissionError::Denied(
                        "observation differs from retained sanitized admission",
                    ));
                }
                if claimed.is_none() {
                    if payload
                        .pointer("/source_identity/original_source")
                        .is_some()
                    {
                        return Err(AdvisoryAdmissionError::Denied(
                            "original source without history grant",
                        ));
                    }
                    let id = CanonicalObservationIdV1::new(admitted.source.source_event_id.clone())
                        .map_err(|_| {
                            AdvisoryAdmissionError::Invalid("observation canonical source")
                        })?;
                    let stored = bounded_read(
                        &call.control,
                        self.observations.read_admitted_observation(&id),
                        "observation canonical source",
                    )
                    .await
                    .map_err(advisory_error)?
                    .ok_or(AdvisoryAdmissionError::Unavailable(
                        "observation canonical source",
                    ))?;
                    if stored.sequence() != admitted.source.source_sequence.0
                        || stored.observation().scope()
                            != &(ObservationScopeV1::Project {
                                project_id: self.mounted_scope.project_id.clone(),
                            })
                        || exact_scope_for_session(
                            &self.profile_id,
                            &self.mounted_scope,
                            stored.observation().source().session_id().as_str(),
                        )
                        .map_err(|_| {
                            AdvisoryAdmissionError::Denied(
                                "ordinary observation source delivery scope",
                            )
                        })? != call.exact_scope
                    {
                        return Err(AdvisoryAdmissionError::Denied(
                            "ordinary observation source delivery scope",
                        ));
                    }
                    return CurrentAdvisoryAdmission::new(call, Vec::new(), None);
                }
                let grant = claimed.ok_or(AdvisoryAdmissionError::Denied(
                    "observation history grant missing",
                ))?;
                if grant.sources.len() != 1 {
                    return Err(AdvisoryAdmissionError::Denied(
                        "observation history source coverage",
                    ));
                }
                let original = source_attribution_from_json(
                    payload.pointer("/source_identity/original_source").ok_or(
                        AdvisoryAdmissionError::Invalid("observation original source"),
                    )?,
                )
                .map_err(advisory_error)?;
                if grant.sources[0].attribution != original
                    || admitted.source.source_event_id != original.source.observation_id
                    || admitted.source.source_sequence.0 != original.source_sequence
                {
                    return Err(AdvisoryAdmissionError::Denied(
                        "observation source coverage",
                    ));
                }
                let current = self
                    .reader()
                    .map_err(advisory_error)?
                    .revalidate_grant(grant, &call.control)
                    .await
                    .map_err(advisory_error)?;
                CurrentAdvisoryAdmission::new(call, current.sources, None)
            }
            ProviderOperation::Replay => {
                let grant = claimed.ok_or(AdvisoryAdmissionError::Denied(
                    "replay history grant missing",
                ))?;
                let items = payload
                    .get("resolved_observations")
                    .and_then(Value::as_array)
                    .filter(|items| !items.is_empty() && items.len() <= MAX_HISTORY_PAGE)
                    .ok_or(AdvisoryAdmissionError::Invalid(
                        "resolved replay observations",
                    ))?;
                let refs = payload
                    .get("observation_batch_refs")
                    .and_then(Value::as_array)
                    .ok_or(AdvisoryAdmissionError::Invalid("replay receipt inventory"))?;
                let mut seen = std::collections::BTreeSet::new();
                let mut seen_sources = std::collections::BTreeSet::new();
                if items.len() != grant.sources.len() || refs.len() != items.len() {
                    return Err(AdvisoryAdmissionError::Denied("replay source coverage"));
                }
                for item in items {
                    call.control
                        .snapshot()
                        .map_err(AdvisoryAdmissionError::Control)?;
                    let observation =
                        item.get("observation")
                            .ok_or(AdvisoryAdmissionError::Invalid(
                                "resolved replay observation",
                            ))?;
                    let key = item
                        .get("idempotency_key")
                        .and_then(Value::as_str)
                        .ok_or(AdvisoryAdmissionError::Invalid("resolved replay key"))?;
                    let source_sequence =
                        item.get("source_sequence").and_then(Value::as_u64).ok_or(
                            AdvisoryAdmissionError::Invalid("resolved replay source sequence"),
                        )?;
                    let admitted = self
                        .retained_admission(key, &call.exact_scope, call.registration_revision)
                        .map_err(advisory_error)?;
                    if admitted.idempotency_key.as_str() != key
                        || admitted.source.source_sequence.0 != source_sequence
                    {
                        return Err(AdvisoryAdmissionError::Denied(
                            "replay admitted key or source sequence",
                        ));
                    }
                    let expected =
                        resolved_replay_observation(&admitted).map_err(advisory_error)?;
                    let receipt = item
                        .get("receipt_ref")
                        .and_then(Value::as_str)
                        .ok_or(AdvisoryAdmissionError::Invalid("resolved replay receipt"))?;
                    if expected != *item
                        || !refs.iter().any(|value| value.as_str() == Some(receipt))
                        || !seen.insert(receipt)
                    {
                        return Err(AdvisoryAdmissionError::Denied(
                            "replay bytes or receipt differ from retained sanitized admission",
                        ));
                    }
                    let original = source_attribution_from_json(
                        observation
                            .pointer("/source_identity/original_source")
                            .ok_or(AdvisoryAdmissionError::Invalid("replay original source"))?,
                    )
                    .map_err(advisory_error)?;
                    if !seen_sources.insert(original.source.observation_id.clone())
                        || !grant
                            .sources
                            .iter()
                            .any(|source| source.attribution == original)
                        || admitted.source.source_event_id != original.source.observation_id
                        || admitted.source.source_sequence.0 != original.source_sequence
                    {
                        return Err(AdvisoryAdmissionError::Denied("replay source coverage"));
                    }
                }
                let current = self
                    .reader()
                    .map_err(advisory_error)?
                    .refresh_grant(grant, &call.control, true)
                    .await
                    .map_err(advisory_error)?;
                CurrentAdvisoryAdmission::new(call, current.sources, None)
            }
            ProviderOperation::SnapshotRestore => {
                let reader = self.reader().map_err(advisory_error)?;
                reader.bridge.revalidate().map_err(advisory_error)?;
                if claimed.is_some() {
                    return Err(AdvisoryAdmissionError::Invalid("restore history grant"));
                }
                let inventory = payload
                    .pointer("/snapshot/sources")
                    .and_then(Value::as_array)
                    .filter(|sources| {
                        sources.len()
                            <= tracedecay_memory_provider_registry::MAX_ADVISORY_ADMISSION_SOURCES
                    })
                    .ok_or(AdvisoryAdmissionError::Invalid("restore source inventory"))?;
                let declared = payload
                    .get("source_dispositions")
                    .and_then(Value::as_array)
                    .filter(|sources| sources.len() == inventory.len())
                    .ok_or(AdvisoryAdmissionError::Invalid(
                        "restore disposition inventory",
                    ))?;
                let mut current = Vec::with_capacity(inventory.len());
                let mut attributed = Vec::with_capacity(inventory.len());
                let mut seen = std::collections::BTreeSet::new();
                for wire in inventory {
                    call.control
                        .snapshot()
                        .map_err(AdvisoryAdmissionError::Control)?;
                    let source: tracedecay_memory_provider_registry::recall_admission::source_attribution::RecallOriginalSourceIdentityV1 =
                        serde_json::from_value(wire.clone()).map_err(|_| AdvisoryAdmissionError::Invalid("restore original source"))?;
                    let source = source
                        .to_owned_source()
                        .map_err(|_| AdvisoryAdmissionError::Invalid("restore original source"))?;
                    if !seen.insert(source.observation_id.clone())
                        || declared
                            .iter()
                            .filter(|item| item.get("source") == Some(wire))
                            .count()
                            != 1
                    {
                        return Err(AdvisoryAdmissionError::Denied(
                            "restore inventory duplicate or missing source",
                        ));
                    }
                    let id = CanonicalObservationIdV1::new(source.observation_id.clone()).map_err(
                        |_| AdvisoryAdmissionError::Invalid("restore canonical observation"),
                    )?;
                    let stored = bounded_read(
                        &call.control,
                        self.observations.read_admitted_observation(&id),
                        "restore canonical observation",
                    )
                    .await
                    .map_err(advisory_error)?;
                    match stored {
                        Some(stored) => {
                            let actual = reader
                                .project_source(&stored, &call.exact_scope, &call.control)
                                .await
                                .map_err(advisory_error)?;
                            if actual.attribution.source != source {
                                return Err(AdvisoryAdmissionError::Denied(
                                    "restore canonical source differs",
                                ));
                            }
                            current.push((source, actual.current_disposition.clone()));
                            attributed.push(actual);
                        }
                        None => {
                            // A retained host fence can keep purged source bytes
                            // blocked. It cannot supply missing original attribution
                            // or make any unavailable source eligible again.
                            let key = source_fence_digest(
                                self.profile_id.as_str(),
                                self.mounted_scope.project_id.as_str(),
                                &source,
                            );
                            let fence = self
                                .journal
                                .read_provider_source_fence(self.provider_id.as_str(), &key)
                                .map_err(|_| {
                                    AdvisoryAdmissionError::Unavailable("restore source fence")
                                })?
                                .filter(|fence| {
                                    fence.blocks_revision(source.source_revision.as_deref())
                                })
                                .ok_or(AdvisoryAdmissionError::Unavailable(
                                    "restore canonical source",
                                ))?;
                            current.push((
                                source,
                                CurrentSourceDisposition {
                                    state: SourceDisposition::Deleted,
                                    authority_ref: format!(
                                        "host-source:{}",
                                        digest_fields(&[&key, &fence.revision.to_string()])
                                    ),
                                    authority_revision: None,
                                    checked_at_utc_nanos: checked_nanos(
                                        tracedecay_contracts::now_micros().0,
                                    )
                                    .map_err(advisory_error)?,
                                },
                            ));
                        }
                    }
                }
                reader.bridge.revalidate().map_err(advisory_error)?;
                let references: Vec<_> = current
                    .iter()
                    .map(|(_, disposition)| disposition.authority_ref.as_str())
                    .collect();
                let checkpoint = RestoreDispositionCheckpoint {
                    exact_scope: call.exact_scope.clone(),
                    authority_ref: format!("host-disposition:{}", digest_fields(&references)),
                    authority_revision: None,
                    checked_at_utc_nanos: checked_nanos(tracedecay_contracts::now_micros().0)
                        .map_err(advisory_error)?,
                };
                CurrentAdvisoryAdmission::new(
                    call,
                    attributed,
                    Some(CurrentRestoreAdmission::new(checkpoint, current)?),
                )
            }
            _ => {
                let sources = match claimed {
                    Some(grant) => {
                        self.reader()
                            .map_err(advisory_error)?
                            .refresh_grant(grant, &call.control, true)
                            .await
                            .map_err(advisory_error)?
                            .sources
                    }
                    None => Vec::new(),
                };
                CurrentAdvisoryAdmission::new(call, sources, None)
            }
        }
    }
}

impl<S, D> HistoryGrantRevalidationV1 for ProviderHistoryAuthorityV1<S, D>
where
    S: ObservationAdmissionPort,
    D: RetrievalAnchorDispositionStore,
{
    fn revalidate(
        &self,
        provider_id: &OwnedProviderId,
        grant: &HistoryGrant,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<()> {
        self.validate_provider(provider_id)?;
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(ProviderHistoryErrorV1::Unavailable(
                "synchronous history authority requires dispatch thread",
            ));
        }
        self.runtime
            .block_on(self.reader()?.revalidate_grant(grant, control))
            .map(|_| ())
    }

    fn revalidate_async<'a>(
        &'a self,
        provider_id: &'a OwnedProviderId,
        grant: &'a HistoryGrant,
        control: &'a tracedecay_memory_provider_registry::OperationControl,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = HistoryResult<()>> + Send + 'a>> {
        Box::pin(async move {
            self.validate_provider(provider_id)?;
            self.reader()?
                .revalidate_grant(grant, control)
                .await
                .map(|_| ())
        })
    }
}

impl<S, D> tracedecay_memory_provider_registry::AdvisoryAdmissionAuthority
    for ProviderHistoryAuthorityV1<S, D>
where
    S: ObservationAdmissionPort,
    D: RetrievalAnchorDispositionStore,
{
    fn admit(
        &self,
        call: &tracedecay_memory_provider_registry::ProviderCall,
    ) -> Result<
        tracedecay_memory_provider_registry::CurrentAdvisoryAdmission,
        tracedecay_memory_provider_registry::AdvisoryAdmissionError,
    > {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(
                tracedecay_memory_provider_registry::AdvisoryAdmissionError::Unavailable(
                    "synchronous advisory authority requires dispatch thread",
                ),
            );
        }
        self.runtime.block_on(self.admit_current(call))
    }
}

fn advisory_error(
    error: ProviderHistoryErrorV1,
) -> tracedecay_memory_provider_registry::AdvisoryAdmissionError {
    use tracedecay_memory_provider_registry::AdvisoryAdmissionError;
    match error {
        ProviderHistoryErrorV1::Unavailable(reason) => AdvisoryAdmissionError::Unavailable(reason),
        ProviderHistoryErrorV1::Ineligible(reason)
        | ProviderHistoryErrorV1::ClaimMismatch(reason) => AdvisoryAdmissionError::Denied(reason),
        ProviderHistoryErrorV1::Control(terminal) => AdvisoryAdmissionError::Control(terminal),
    }
}

fn source_attribution_from_json(value: &Value) -> HistoryResult<SourceAttribution> {
    serde_json::from_value::<RecallSourceAttributionV1>(value.clone())
        .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("source attribution"))?
        .to_owned_attribution()
        .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("source attribution"))
}

/// Exact replay projection of a retained sanitized admission.
///
/// Replay metadata is carried beside the payload so the journal's immutable
/// payload bytes remain byte-for-byte intact. The metadata is still derived
/// from that admission and is therefore part of the host-authorized item, not
/// a provider claim.
pub(crate) fn resolved_replay_observation(
    admitted: &tracedecay_memory_observation::AdmittedObservationV1,
) -> HistoryResult<Value> {
    admitted
        .validate()
        .map_err(|_| ProviderHistoryErrorV1::Unavailable("retained replay admission"))?;
    let observation: Value = serde_json::from_slice(&admitted.payload.bytes)
        .map_err(|_| ProviderHistoryErrorV1::Unavailable("retained replay payload"))?;
    let original = observation
        .pointer("/source_identity/original_source")
        .ok_or(ProviderHistoryErrorV1::Ineligible(
            "retained replay original source",
        ))?;
    let original = source_attribution_from_json(original)?;
    if admitted.source.source_event_id != original.source.observation_id
        || admitted.source.source_sequence.0 != original.source_sequence
    {
        return Err(ProviderHistoryErrorV1::ClaimMismatch(
            "retained replay source binding",
        ));
    }
    Ok(json!({
        "receipt_ref": admitted.sanitization.receipt_id,
        "idempotency_key": admitted.idempotency_key.as_str(),
        "source_sequence": admitted.source.source_sequence.0,
        "observation": observation,
    }))
}

impl<S, D, O> ProviderHistoryReaderV1<'_, S, D, O>
where
    S: ObservationAdmissionPort + ?Sized,
    D: RetrievalAnchorDispositionStore + ?Sized,
    O: OriginalObservationAuthorityV1 + ?Sized,
{
    /// Fresh recent coverage comes from canonical sequence authority, never
    /// from the destination's durable enqueue cursor.
    pub(crate) async fn select_recent_page(
        &self,
        destination: &OwnedExactScope,
        limit: usize,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<ProviderHistoryPageV1> {
        self.check_control(control)?;
        self.bridge.revalidate()?;
        self.bridge.validate_destination(destination)?;
        if self.policy_revision == 0 || limit == 0 || limit > MAX_HISTORY_PAGE {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "history page bound or policy",
            ));
        }
        let request = ObservationRecentWindowRequest::new(limit)
            .map_err(|_| ProviderHistoryErrorV1::Ineligible("canonical recent window bound"))?;
        let window = bounded_read(
            control,
            self.observations
                .recent_admitted_observation_window(request),
            "canonical recent window",
        )
        .await?;
        let (after_sequence, through_sequence, has_older) = match window {
            Some(window) => {
                window.validate().map_err(|_| {
                    ProviderHistoryErrorV1::Unavailable("canonical recent window bounds")
                })?;
                (
                    window.first_sequence - 1,
                    window.last_sequence,
                    window.has_older,
                )
            }
            None => (0, 0, false),
        };
        let mut page = self
            .select_window_page(
                destination,
                after_sequence,
                through_sequence,
                limit,
                control,
            )
            .await?;
        page.has_older = has_older;
        Ok(page)
    }

    pub(crate) async fn select_page(
        &self,
        destination: &OwnedExactScope,
        after_sequence: u64,
        limit: usize,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<ProviderHistoryPageV1> {
        self.select_page_with_upper_bound(destination, after_sequence, None, limit, control)
            .await
    }

    /// Upper bound is frozen by the preceding sequence-only window read. Rows
    /// committed later must not enter coverage, grants or downstream enqueue.
    pub(crate) async fn select_window_page(
        &self,
        destination: &OwnedExactScope,
        after_sequence: u64,
        through_sequence: u64,
        limit: usize,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<ProviderHistoryPageV1> {
        if through_sequence < after_sequence {
            return Err(ProviderHistoryErrorV1::ClaimMismatch(
                "canonical recent window bounds",
            ));
        }
        self.select_page_with_upper_bound(
            destination,
            after_sequence,
            Some(through_sequence),
            limit,
            control,
        )
        .await
    }

    async fn select_page_with_upper_bound(
        &self,
        destination: &OwnedExactScope,
        after_sequence: u64,
        through_sequence: Option<u64>,
        limit: usize,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<ProviderHistoryPageV1> {
        self.check_control(control)?;
        self.bridge.revalidate()?;
        self.bridge.validate_destination(destination)?;
        if self.policy_revision == 0 || limit == 0 || limit > MAX_HISTORY_PAGE {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "history page bound or policy",
            ));
        }
        let request = ObservationReplayRequest::new(after_sequence, limit)
            .map_err(|_| ProviderHistoryErrorV1::Ineligible("canonical page bound"))?;
        let page = bounded_read(
            control,
            self.observations.replay_admitted_observations(request),
            "canonical replay",
        )
        .await?;
        // This is deliberately before counting, origin probing, projection,
        // grant construction, or retaining any record for journal enqueue.
        let page: Vec<_> = page
            .into_iter()
            .filter(|stored| through_sequence.is_none_or(|through| stored.sequence() <= through))
            .collect();
        let has_more = page.len() == limit
            && through_sequence.is_none_or(|through| {
                page.last()
                    .is_some_and(|stored| stored.sequence() < through)
            });
        let mut result = ProviderHistoryPageV1 {
            records: Vec::new(),
            grant: None,
            last_scanned_sequence: after_sequence,
            scanned: page.len(),
            withheld: 0,
            unknown_revision: 0,
            has_more,
            has_older: false,
        };
        let mut sources = Vec::new();
        for stored in page {
            self.check_control(control)?;
            result.last_scanned_sequence = stored.sequence();
            match self.project_source(&stored, destination, control).await {
                Ok(source) if retained_history_source(source.current_disposition.state) => {
                    result.unknown_revision +=
                        usize::from(source.attribution.source.source_revision.is_none());
                    sources.push(source);
                }
                Ok(_) | Err(ProviderHistoryErrorV1::Ineligible(_)) => result.withheld += 1,
                Err(error) => return Err(error),
            }
            result.records.push(stored);
        }
        self.check_control(control)?;
        self.bridge.revalidate()?;
        if !sources.is_empty() {
            result.grant = Some(self.grant(destination, sources)?);
        }
        Ok(result)
    }

    /// Directly authorizes one retained source by its canonical key. No claimed
    /// grant is constructed from ledger data to bootstrap this authority.
    pub(crate) async fn authorize_retained_source(
        &self,
        destination: &OwnedExactScope,
        expected: &SourceAttribution,
        control: &tracedecay_memory_provider_registry::OperationControl,
        include_unavailable: bool,
    ) -> HistoryResult<HistoryGrant> {
        self.check_control(control)?;
        if self.policy_revision == 0 {
            return Err(ProviderHistoryErrorV1::ClaimMismatch(
                "history policy revision",
            ));
        }
        expected
            .validate()
            .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("retained source attribution"))?;
        self.bridge.authorize_control_scope(destination, control)?;
        let id = CanonicalObservationIdV1::new(expected.source.observation_id.clone())
            .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("canonical observation id"))?;
        let stored = bounded_read(
            control,
            self.observations.read_admitted_observation(&id),
            "canonical observation",
        )
        .await?
        .ok_or(ProviderHistoryErrorV1::Ineligible(
            "canonical observation missing",
        ))?;
        let actual = self.project_source(&stored, destination, control).await?;
        if actual.attribution != *expected {
            return Err(ProviderHistoryErrorV1::ClaimMismatch(
                "original source attribution",
            ));
        }
        if !include_unavailable && !retained_history_source(actual.current_disposition.state) {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "current source disposition",
            ));
        }
        self.bridge.authorize_control_scope(destination, control)?;
        self.grant(destination, vec![actual])
    }

    /// Re-resolves one opaque control locator through the canonical sequence
    /// authority. The persisted attribution is only a privacy-safe locator;
    /// it never supplies a canonical observation key or source identity.
    ///
    /// The single-row replay is deliberately addressed by `source_sequence`:
    /// after reopen the host has no reverse map from an opaque source locator
    /// to provider data. The replayed row is projected afresh, then redacted
    /// with the exact trace/revision/rank/alias context that produced the
    /// persisted bytes. Only a byte-for-byte match can proceed to the usual
    /// scope, disposition, policy and bridge checks.
    pub(crate) async fn authorize_retained_source_locator(
        &self,
        destination: &OwnedExactScope,
        trace: &RecallControlTraceRefV1,
        registration_revision: u64,
        provider_rank: usize,
        candidate_id: &str,
        expected: &RecallSourceAttributionV1,
        control: &tracedecay_memory_provider_registry::OperationControl,
        include_unavailable: bool,
        locator_key: &RecallLocatorKeyV1,
    ) -> HistoryResult<HistoryGrant> {
        self.check_control(control)?;
        if self.policy_revision == 0 || registration_revision == 0 {
            return Err(ProviderHistoryErrorV1::ClaimMismatch(
                "retained locator policy revision",
            ));
        }
        if !trace.is_bound_to_scope(destination) {
            return Err(ProviderHistoryErrorV1::ClaimMismatch(
                "retained locator destination scope",
            ));
        }
        validate_retained_provider_rank(provider_rank)
            .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("retained locator provider rank"))?;
        validate_retained_candidate_identity(candidate_id)
            .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("retained locator candidate"))?;
        validate_opaque_retained_source(expected).map_err(|_| {
            ProviderHistoryErrorV1::ClaimMismatch("retained locator source projection")
        })?;
        let source_sequence = expected.source_sequence;
        let after_sequence =
            source_sequence
                .checked_sub(1)
                .ok_or(ProviderHistoryErrorV1::ClaimMismatch(
                    "retained locator source sequence",
                ))?;
        self.bridge.authorize_control_scope(destination, control)?;
        let request = ObservationReplayRequest::new(after_sequence, 1)
            .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("retained locator replay bound"))?;
        let mut records = bounded_read(
            control,
            self.observations.replay_admitted_observations(request),
            "retained locator canonical replay",
        )
        .await?;
        if records.len() != 1 {
            return Err(ProviderHistoryErrorV1::ClaimMismatch(
                "retained locator canonical row count",
            ));
        }
        let stored = records.pop().ok_or(ProviderHistoryErrorV1::ClaimMismatch(
            "retained locator canonical row",
        ))?;
        if stored.sequence() != source_sequence {
            return Err(ProviderHistoryErrorV1::ClaimMismatch(
                "retained locator canonical row sequence",
            ));
        }
        let actual = self.project_source(&stored, destination, control).await?;
        if actual.attribution.source_sequence != source_sequence {
            return Err(ProviderHistoryErrorV1::ClaimMismatch(
                "retained locator projected sequence",
            ));
        }
        let fresh_wire: RecallSourceAttributionV1 = serde_json::from_value(
            source_attribution_json(&actual.attribution)?,
        )
        .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("retained locator source wire"))?;
        let redacted = redact_retained_source_attribution_with_key(
            locator_key,
            self.provider_id.as_str(),
            trace,
            registration_revision,
            provider_rank,
            candidate_id,
            &fresh_wire,
        )
        .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("retained locator redaction"))?;
        let expected_bytes = serde_json::to_vec(expected)
            .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("retained locator bytes"))?;
        let redacted_bytes = serde_json::to_vec(&redacted)
            .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("retained locator bytes"))?;
        if redacted_bytes != expected_bytes {
            return Err(ProviderHistoryErrorV1::ClaimMismatch(
                "retained locator source changed",
            ));
        }
        if !include_unavailable && !retained_history_source(actual.current_disposition.state) {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "current source disposition",
            ));
        }
        self.check_control(control)?;
        self.bridge.authorize_control_scope(destination, control)?;
        self.grant(destination, vec![actual])
    }

    /// Reads every actual source again at dispatch/recall/restore. Provider
    /// generation and disposition revision are never compared here.
    pub(crate) async fn revalidate_grant(
        &self,
        claimed: &HistoryGrant,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<HistoryGrant> {
        self.refresh_grant(claimed, control, false).await
    }

    async fn refresh_grant(
        &self,
        claimed: &HistoryGrant,
        control: &tracedecay_memory_provider_registry::OperationControl,
        include_unavailable: bool,
    ) -> HistoryResult<HistoryGrant> {
        self.check_control(control)?;
        claimed
            .validate_structure()
            .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("history grant"))?;
        if claimed.policy_revision != self.policy_revision
            || claimed.sources.len() > MAX_HISTORY_PAGE
        {
            return Err(ProviderHistoryErrorV1::ClaimMismatch(
                "policy or source bound",
            ));
        }
        self.bridge.revalidate()?;
        self.bridge
            .validate_destination(&claimed.destination_scope)?;
        let mut current = Vec::with_capacity(claimed.sources.len());
        for source in &claimed.sources {
            self.check_control(control)?;
            let id =
                CanonicalObservationIdV1::new(source.attribution.source.observation_id.clone())
                    .map_err(|_| {
                        ProviderHistoryErrorV1::ClaimMismatch("canonical observation id")
                    })?;
            let stored = bounded_read(
                control,
                self.observations.read_admitted_observation(&id),
                "canonical observation",
            )
            .await?
            .ok_or(ProviderHistoryErrorV1::Ineligible(
                "canonical observation missing",
            ))?;
            let actual = self
                .project_source(&stored, &claimed.destination_scope, control)
                .await?;
            if actual.attribution != source.attribution {
                return Err(ProviderHistoryErrorV1::ClaimMismatch(
                    "original source attribution",
                ));
            }
            if !include_unavailable && !retained_history_source(actual.current_disposition.state) {
                return Err(ProviderHistoryErrorV1::Ineligible(
                    "current source disposition",
                ));
            }
            current.push(actual);
        }
        self.check_control(control)?;
        self.bridge.revalidate()?;
        self.grant(&claimed.destination_scope, current)
    }

    async fn project_source(
        &self,
        stored: &StoredObservation,
        destination: &OwnedExactScope,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<GrantedHistorySource> {
        let observation = stored.observation();
        if observation.scope()
            != &(ObservationScopeV1::Project {
                project_id: self.bridge.canonical_project.clone(),
            })
        {
            return Err(ProviderHistoryErrorV1::Ineligible("canonical project"));
        }
        let origin = self
            .bridge
            .original_scope(stored, self.original_authority)?;
        origin
            .recorded_scope()
            .map_err(|_| ProviderHistoryErrorV1::Ineligible("original scope unavailable"))?;
        if !self
            .original_authority
            .authorizes_session(observation.source().session_id().as_str(), destination)?
        {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "ungranted source session",
            ));
        }
        let envelope: CanonicalObservationEnvelopeV1 =
            serde_json::from_value(observation.payload().clone())
                .map_err(|_| ProviderHistoryErrorV1::Ineligible("canonical envelope"))?;
        envelope
            .validate()
            .map_err(|_| ProviderHistoryErrorV1::Ineligible("canonical envelope"))?;
        if envelope.provider() != observation.source().provider()
            || envelope.relations().session_id() != observation.source().session_id()
            || envelope.evidence().ordering_domain() != observation.identity().ordering_domain()
            || envelope.evidence().range() != observation.identity().position()
            || observation
                .identity()
                .native_record_id()
                .is_some_and(|id| id != envelope.stable_record_id())
        {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "canonical envelope source binding",
            ));
        }
        let canonical_bytes =
            tracedecay_memory_hygiene::canonical_payload_bytes(observation.payload())
                .map_err(|_| ProviderHistoryErrorV1::Ineligible("canonical source bytes"))?;
        let source = OriginalSourceIdentity {
            canonical_provider_id: OwnedProviderId::new(observation.source().provider().as_str())
                .map_err(|_| {
                ProviderHistoryErrorV1::Ineligible("canonical provider")
            })?,
            canonical_session_id: observation.source().session_id().as_str().to_owned(),
            source_key: observation.source().source_key().as_str().to_owned(),
            stable_record_id: observation
                .identity()
                .native_record_id()
                .map(|v| v.as_str().to_owned()),
            observation_id: observation.observation_id().as_str().to_owned(),
            source_revision: envelope.evidence().revision().map(str::to_owned),
            content_sha256: hex::encode(Sha256::digest(canonical_bytes)),
        };
        let attribution = SourceAttribution {
            source,
            origin_scope: origin,
            source_sequence: stored.sequence(),
            occurred_at_utc_nanos: stored
                .retrieval_anchor()
                .occurred_at()
                .map(|time| checked_nanos(time.start.0))
                .transpose()?,
            ingested_at_utc_nanos: checked_nanos(stored.retrieval_anchor().ingested_at().0)?,
            validity: RecordedValidity::default(),
        };
        let owner = FactOwnerV1::Project {
            project_id: self.bridge.canonical_project.clone(),
        };
        let current = bounded_read(
            control,
            self.dispositions
                .current_disposition(stored.retrieval_anchor_id(), &owner),
            "current source disposition",
        )
        .await?;
        let (mut state, mut authority_ref) = match current {
            None => (
                SourceDisposition::Available,
                format!("anchor:{}:initial", stored.retrieval_anchor_id().as_str()),
            ),
            Some(record) => {
                if record.owner() != &owner || record.anchor_id() != stored.retrieval_anchor_id() {
                    return Err(ProviderHistoryErrorV1::Unavailable(
                        "disposition owner binding",
                    ));
                }
                let state = match record.state() {
                    AnchorDispositionStateV1::Active => SourceDisposition::Available,
                    AnchorDispositionStateV1::Superseded => SourceDisposition::Superseded,
                    AnchorDispositionStateV1::Redacted => SourceDisposition::Redacted,
                    AnchorDispositionStateV1::Expired => SourceDisposition::Expired,
                    AnchorDispositionStateV1::Deleted => SourceDisposition::Deleted,
                    AnchorDispositionStateV1::Quarantined
                    | AnchorDispositionStateV1::Unavailable => SourceDisposition::Unknown,
                };
                (
                    state,
                    format!("anchor-disposition:{}", record.disposition_id()),
                )
            }
        };
        let key = original_source_fence_digest(&attribution)?;
        let fence_control = control
            .snapshot()
            .map_err(ProviderHistoryErrorV1::Control)?;
        if let Some(fence) = self
            .journal
            .read_provider_source_fence_bounded(
                self.provider_id.as_str(),
                &key,
                tracedecay_memory_observation::RecoveryTimeBudgetV1 {
                    remaining_micros: i64::try_from(fence_control.remaining_millis)
                        .unwrap_or(i64::MAX)
                        .saturating_mul(1_000),
                },
                &control.cancellation(),
            )
            .map_err(|error| match error {
                tracedecay_memory_observation::ObservationJournalError::OperationCancelled {
                    ..
                } => ProviderHistoryErrorV1::Control(
                    tracedecay_memory_provider_registry::TerminalCode::Cancelled,
                ),
                tracedecay_memory_observation::ObservationJournalError::BudgetExhausted {
                    ..
                } => ProviderHistoryErrorV1::Control(
                    tracedecay_memory_provider_registry::TerminalCode::DeadlineExceeded,
                ),
                _ => ProviderHistoryErrorV1::Unavailable("provider source fence"),
            })?
        {
            if fence.blocks_revision(attribution.source.source_revision.as_deref()) {
                state = SourceDisposition::Deleted;
            }
            authority_ref = format!(
                "host-source:{}",
                digest_fields(&[&authority_ref, &key, &fence.revision.to_string()])
            );
        }
        attribution
            .validate()
            .map_err(|_| ProviderHistoryErrorV1::Ineligible("source attribution"))?;
        Ok(GrantedHistorySource {
            attribution,
            current_disposition: CurrentSourceDisposition {
                state,
                authority_ref,
                authority_revision: None,
                checked_at_utc_nanos: checked_nanos(tracedecay_contracts::now_micros().0)?,
            },
        })
    }

    /// Resolves one provider-carried identity through the actual canonical row.
    /// The returned full attribution is read from canonical origin proof; it is
    /// never reconstructed from the smaller snapshot identity inventory.
    pub(crate) async fn authorize_original_source_identity(
        &self,
        destination: &OwnedExactScope,
        expected: &OriginalSourceIdentity,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<GrantedHistorySource> {
        self.check_control(control)?;
        expected
            .validate()
            .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("original source identity"))?;
        self.bridge.authorize_control_scope(destination, control)?;
        let id = CanonicalObservationIdV1::new(expected.observation_id.clone())
            .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("canonical observation id"))?;
        let stored = bounded_read(
            control,
            self.observations.read_admitted_observation(&id),
            "control canonical source identity",
        )
        .await?
        .ok_or(ProviderHistoryErrorV1::Ineligible(
            "control canonical source missing",
        ))?;
        let actual = self.project_source(&stored, destination, control).await?;
        if &actual.attribution.source != expected {
            return Err(ProviderHistoryErrorV1::ClaimMismatch(
                "canonical original source changed",
            ));
        }
        self.bridge.authorize_control_scope(destination, control)?;
        Ok(actual)
    }

    pub(crate) fn disposition_checkpoint(
        &self,
        destination: &OwnedExactScope,
        sources: &[GrantedHistorySource],
    ) -> HistoryResult<RestoreDispositionCheckpoint> {
        let references: Vec<_> = sources
            .iter()
            .map(|source| source.current_disposition.authority_ref.as_str())
            .collect();
        Ok(RestoreDispositionCheckpoint {
            exact_scope: destination.clone(),
            authority_ref: format!("host-disposition:{}", digest_fields(&references)),
            authority_revision: None,
            checked_at_utc_nanos: checked_nanos(tracedecay_contracts::now_micros().0)?,
        })
    }

    pub(crate) fn grant(
        &self,
        destination: &OwnedExactScope,
        sources: Vec<GrantedHistorySource>,
    ) -> HistoryResult<HistoryGrant> {
        let checkpoint = self.disposition_checkpoint(destination, &sources)?;
        let relation = if sources
            .iter()
            .all(|source| source.attribution.origin_scope.recorded_scope() == Ok(destination))
        {
            HistoryRelation::ExactScope
        } else {
            HistoryRelation::SameCheckout
        };
        let grant = HistoryGrant {
            authorization_ref: format!(
                "host-history:{}",
                digest_fields(&[
                    &destination.exact_scope_sha256(),
                    &checkpoint.authority_ref,
                    &self.policy_revision.to_string()
                ])
            ),
            policy_revision: self.policy_revision,
            destination_scope: destination.clone(),
            relation,
            sources,
            disposition_checkpoint: checkpoint,
        };
        grant
            .validate_structure()
            .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("assembled history grant"))?;
        Ok(grant)
    }

    fn check_control(
        &self,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<()> {
        self.original_authority.validate_live_mount()?;
        control
            .snapshot()
            .map(|_| ())
            .map_err(ProviderHistoryErrorV1::Control)
    }
}

/// Retained nonprivacy states can be admitted to a provider for temporal
/// evaluation. This does not invent event times or make a current answer eligible.
pub(crate) fn retained_history_source(state: SourceDisposition) -> bool {
    matches!(
        state,
        SourceDisposition::Available | SourceDisposition::Superseded | SourceDisposition::Revoked
    )
}

/// Stable source key for host deletion fanout. The destination scope, provider
/// registration, source revision, source bytes and delivery id never enter it.
pub(crate) fn original_source_fence_digest(source: &SourceAttribution) -> HistoryResult<String> {
    source
        .validate()
        .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("fence source"))?;
    let origin = source
        .origin_scope
        .recorded_scope()
        .map_err(|_| ProviderHistoryErrorV1::Ineligible("fence original scope"))?;
    Ok(source_fence_digest(
        &origin.profile_id,
        &origin.project_id,
        &source.source,
    ))
}

fn source_fence_digest(profile: &str, project: &str, source: &OriginalSourceIdentity) -> String {
    digest_fields(&[
        profile,
        project,
        source.canonical_provider_id.as_str(),
        &source.canonical_session_id,
        &source.source_key,
    ])
}

fn digest_fields(values: &[&str]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"tracedecay.host-provider-history.v1\0");
    for value in values {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    hex::encode(digest.finalize())
}

fn checked_nanos(micros: i64) -> HistoryResult<i64> {
    micros
        .checked_mul(1000)
        .ok_or(ProviderHistoryErrorV1::Ineligible(
            "timestamp outside nanosecond range",
        ))
}

fn timestamp(nanos: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(nanos)
        .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
}

fn read_timestamp(value: &str) -> HistoryResult<i64> {
    if !value.ends_with('Z') {
        return Err(ProviderHistoryErrorV1::ClaimMismatch("UTC timestamp"));
    }
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .and_then(|v| v.timestamp_nanos_opt())
        .ok_or(ProviderHistoryErrorV1::ClaimMismatch(
            "nanosecond timestamp",
        ))
}

fn scope_wire(scope: &OwnedExactScope) -> RecallOutcomeScopeV1 {
    RecallOutcomeScopeV1 {
        profile_id: scope.profile_id.clone(),
        project_id: scope.project_id.clone(),
        repository_identity: scope.repository_identity.clone(),
        worktree_identity: scope.worktree_identity.clone(),
        branch_identity: scope.branch_identity.clone(),
        agent_session_id: scope.agent_session_id.clone(),
        resolved_scope_digest: scope.resolved_scope_digest.clone(),
    }
}

fn scope_owned(scope: RecallOutcomeScopeV1) -> HistoryResult<OwnedExactScope> {
    OwnedExactScope::new(
        scope.profile_id,
        scope.project_id,
        scope.repository_identity,
        scope.worktree_identity,
        scope.branch_identity,
        scope.agent_session_id,
        scope.resolved_scope_digest,
    )
    .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("exact scope"))
}

pub(crate) fn source_attribution_json(attribution: &SourceAttribution) -> HistoryResult<Value> {
    attribution
        .validate()
        .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("source attribution"))?;
    let source = &attribution.source;
    let validity = &attribution.validity;
    let origin = match &attribution.origin_scope {
        OriginScopeEvidence::Recorded {
            scope,
            authority_ref,
        } => {
            json!({"state":"recorded", "exact_scope_identity":scope_wire(scope), "authority_ref":authority_ref})
        }
        OriginScopeEvidence::IngestionOnly => json!({"state":"ingestion_only"}),
        OriginScopeEvidence::Unavailable => json!({"state":"unavailable"}),
    };
    Ok(json!({
        "source": {"canonical_provider_id":source.canonical_provider_id.as_str(),
            "canonical_session_id":source.canonical_session_id,"source_key":source.source_key,
            "stable_record_id":source.stable_record_id,"observation_id":source.observation_id,
            "source_revision":source.source_revision,"content_sha256":source.content_sha256},
        "origin_scope":origin,"source_sequence":attribution.source_sequence,
        "occurred_at":attribution.occurred_at_utc_nanos.map(timestamp),
        "ingested_at":timestamp(attribution.ingested_at_utc_nanos),
        "validity":{"valid_from":validity.valid_from_utc_nanos.map(timestamp),
            "valid_until":validity.valid_until_utc_nanos.map(timestamp),
            "superseded_at":validity.superseded_at_utc_nanos.map(timestamp),
            "superseded_by":validity.superseded_by,"revoked_at":validity.revoked_at_utc_nanos.map(timestamp)}
    }))
}

fn required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::deserialize(deserializer)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireDisposition {
    state: String,
    authority_ref: String,
    #[serde(deserialize_with = "required_nullable")]
    authority_revision: Option<u64>,
    checked_at: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireCheckpoint {
    exact_scope: RecallOutcomeScopeV1,
    authority_ref: String,
    #[serde(deserialize_with = "required_nullable")]
    authority_revision: Option<u64>,
    checked_at: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireGrantedSource {
    attribution: RecallSourceAttributionV1,
    current_disposition: WireDisposition,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireHistoryGrant {
    authorization_ref: String,
    policy_revision: u64,
    destination_scope: RecallOutcomeScopeV1,
    relation: String,
    sources: Vec<WireGrantedSource>,
    disposition_checkpoint: WireCheckpoint,
}

pub(crate) fn history_grant_json(grant: &HistoryGrant) -> HistoryResult<Value> {
    grant
        .validate_structure()
        .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("history grant"))?;
    let sources = grant
        .sources
        .iter()
        .map(|source| {
            Ok(WireGrantedSource {
                attribution: serde_json::from_value(source_attribution_json(&source.attribution)?)
                    .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("source wire"))?,
                current_disposition: WireDisposition {
                    state: source.current_disposition.state.as_wire().to_owned(),
                    authority_ref: source.current_disposition.authority_ref.clone(),
                    authority_revision: source.current_disposition.authority_revision,
                    checked_at: timestamp(source.current_disposition.checked_at_utc_nanos),
                },
            })
        })
        .collect::<HistoryResult<Vec<_>>>()?;
    let checkpoint = &grant.disposition_checkpoint;
    serde_json::to_value(WireHistoryGrant {
        authorization_ref: grant.authorization_ref.clone(),
        policy_revision: grant.policy_revision,
        destination_scope: scope_wire(&grant.destination_scope),
        relation: grant.relation.as_wire().to_owned(),
        sources,
        disposition_checkpoint: WireCheckpoint {
            exact_scope: scope_wire(&checkpoint.exact_scope),
            authority_ref: checkpoint.authority_ref.clone(),
            authority_revision: checkpoint.authority_revision,
            checked_at: timestamp(checkpoint.checked_at_utc_nanos),
        },
    })
    .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("history wire"))
}

pub(crate) fn history_grant_from_json(value: &Value) -> HistoryResult<HistoryGrant> {
    let wire: WireHistoryGrant = serde_json::from_value(value.clone())
        .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("history wire"))?;
    if wire.sources.is_empty() || wire.sources.len() > MAX_HISTORY_PAGE {
        return Err(ProviderHistoryErrorV1::ClaimMismatch("source count"));
    }
    let sources = wire
        .sources
        .into_iter()
        .map(|source| {
            let disposition = source.current_disposition;
            Ok(GrantedHistorySource {
                attribution: source
                    .attribution
                    .to_owned_attribution()
                    .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("source wire"))?,
                current_disposition: CurrentSourceDisposition {
                    state: SourceDisposition::from_wire(&disposition.state)
                        .ok_or(ProviderHistoryErrorV1::ClaimMismatch("source state"))?,
                    authority_ref: disposition.authority_ref,
                    authority_revision: disposition.authority_revision,
                    checked_at_utc_nanos: read_timestamp(&disposition.checked_at)?,
                },
            })
        })
        .collect::<HistoryResult<Vec<_>>>()?;
    let checkpoint = wire.disposition_checkpoint;
    let grant = HistoryGrant {
        authorization_ref: wire.authorization_ref,
        policy_revision: wire.policy_revision,
        destination_scope: scope_owned(wire.destination_scope)?,
        relation: HistoryRelation::from_wire(&wire.relation)
            .ok_or(ProviderHistoryErrorV1::ClaimMismatch("history relation"))?,
        sources,
        disposition_checkpoint: RestoreDispositionCheckpoint {
            exact_scope: scope_owned(checkpoint.exact_scope)?,
            authority_ref: checkpoint.authority_ref,
            authority_revision: checkpoint.authority_revision,
            checked_at_utc_nanos: read_timestamp(&checkpoint.checked_at)?,
        },
    };
    grant
        .validate_structure()
        .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("history grant"))?;
    Ok(grant)
}

pub(crate) fn validate_history_record<'a>(
    grant: &'a HistoryGrant,
    stored: &StoredObservation,
) -> HistoryResult<&'a SourceAttribution> {
    grant
        .validate_structure()
        .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("history grant"))?;
    let observation = stored.observation();
    let attribution = grant
        .sources
        .iter()
        .find(|source| {
            source.attribution.source.observation_id == observation.observation_id().as_str()
        })
        .map(|source| &source.attribution)
        .ok_or(ProviderHistoryErrorV1::Ineligible(
            "record outside history grant",
        ))?;
    let bytes = tracedecay_memory_hygiene::canonical_payload_bytes(observation.payload())
        .map_err(|_| ProviderHistoryErrorV1::ClaimMismatch("canonical record encoding"))?;
    if attribution.source_sequence != stored.sequence()
        || attribution.source.canonical_provider_id.as_str()
            != observation.source().provider().as_str()
        || attribution.source.canonical_session_id != observation.source().session_id().as_str()
        || attribution.source.source_key != observation.source().source_key().as_str()
        || attribution.source.content_sha256 != hex::encode(Sha256::digest(bytes))
    {
        return Err(ProviderHistoryErrorV1::ClaimMismatch(
            "history canonical record",
        ));
    }
    Ok(attribution)
}

pub(crate) async fn bounded_read<T, E>(
    control: &tracedecay_memory_provider_registry::OperationControl,
    future: impl std::future::Future<Output = Result<T, E>>,
    authority: &'static str,
) -> HistoryResult<T> {
    let snapshot = control
        .snapshot()
        .map_err(ProviderHistoryErrorV1::Control)?;
    let deadline =
        tokio::time::Instant::now() + std::time::Duration::from_millis(snapshot.remaining_millis);
    tokio::pin!(future);
    loop {
        control
            .snapshot()
            .map_err(ProviderHistoryErrorV1::Control)?;
        tokio::select! {
            biased;
            result = &mut future => {
                control.snapshot().map_err(ProviderHistoryErrorV1::Control)?;
                return result.map_err(|_| ProviderHistoryErrorV1::Unavailable(authority));
            }
            () = tokio::time::sleep_until(deadline) => {
                return Err(ProviderHistoryErrorV1::Control(control.snapshot().err().unwrap_or(
                    tracedecay_memory_provider_registry::TerminalCode::DeadlineExceeded,
                )));
            }
            () = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use tracedecay_memory_provider_registry::{CancellationToken, OperationControl, TerminalCode};

    fn git(root: &Path, args: &[&str]) {
        let output = Command::new(tracedecay_runtime_core::git::try_git_program().unwrap())
            .current_dir(root)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn init_repository(root: &Path) {
        std::fs::create_dir_all(root).unwrap();
        git(root, &["init", "-q", "-b", "main"]);
        git(root, &["config", "user.name", "History identity test"]);
        git(
            root,
            &["config", "user.email", "history-identity@example.invalid"],
        );
        std::fs::write(root.join("tracked"), b"history identity").unwrap();
        git(root, &["add", "."]);
        git(root, &["commit", "-q", "-m", "initial"]);
    }

    #[test]
    fn retained_history_gate_keeps_revocation_distinct_from_privacy_deletion() {
        for state in [
            SourceDisposition::Available,
            SourceDisposition::Superseded,
            SourceDisposition::Revoked,
        ] {
            assert!(retained_history_source(state), "{state:?}");
        }
        for state in [
            SourceDisposition::Deleted,
            SourceDisposition::Redacted,
            SourceDisposition::Expired,
            SourceDisposition::Unknown,
        ] {
            assert!(!retained_history_source(state), "{state:?}");
        }
    }

    #[test]
    fn hook_origin_reader_binds_registered_brain_profile_and_project() {
        let temporary = tempfile::tempdir().unwrap();
        let brain = BrainId::new("brain.history-reader").unwrap();
        let profile = UserProfileId::new("profile.history-reader").unwrap();
        let project = ProjectId::new("project.history-reader").unwrap();
        let scope = ResolvedScope::new(
            project.clone(),
            RepositoryId::new("repository.history-reader").unwrap(),
            WorktreeId::new("worktree.history-reader").unwrap(),
            Some(tracedecay_domain::RefId::new("refs/heads/main").unwrap()),
        )
        .unwrap();
        let registered =
            StoreShardIdV1::project_sessions(brain.clone(), profile.clone(), project.clone());
        let reader =
            HookOriginReaderV1::new(temporary.path().to_path_buf(), brain, profile.clone());

        reader
            .validate_mount_identity(&profile, &scope, &registered)
            .unwrap();

        let wrong_reader_brain = HookOriginReaderV1::new(
            temporary.path().to_path_buf(),
            BrainId::new("brain.other").unwrap(),
            profile.clone(),
        );
        assert!(matches!(
            wrong_reader_brain.validate_mount_identity(&profile, &scope, &registered),
            Err(ProviderHistoryErrorV1::Ineligible(
                "history reader identity"
            ))
        ));

        let wrong_reader_profile = HookOriginReaderV1::new(
            temporary.path().to_path_buf(),
            registered.brain_id.clone(),
            UserProfileId::new("profile.other").unwrap(),
        );
        assert!(matches!(
            wrong_reader_profile.validate_mount_identity(&profile, &scope, &registered),
            Err(ProviderHistoryErrorV1::Ineligible(
                "history reader identity"
            ))
        ));

        let wrong_brain = StoreShardIdV1::project_sessions(
            BrainId::new("brain.other").unwrap(),
            profile.clone(),
            project.clone(),
        );
        assert!(matches!(
            reader.validate_mount_identity(&profile, &scope, &wrong_brain),
            Err(ProviderHistoryErrorV1::Ineligible(
                "history reader identity"
            ))
        ));

        let wrong_project = StoreShardIdV1::project_sessions(
            registered.brain_id.clone(),
            registered.profile_id.clone(),
            ProjectId::new("project.other").unwrap(),
        );
        assert!(matches!(
            reader.validate_mount_identity(&profile, &scope, &wrong_project),
            Err(ProviderHistoryErrorV1::Ineligible(
                "registered project/profile"
            ))
        ));

        let wrong_profile = UserProfileId::new("profile.other").unwrap();
        assert!(matches!(
            reader.validate_mount_identity(&wrong_profile, &scope, &registered),
            Err(ProviderHistoryErrorV1::Ineligible(
                "registered project/profile"
            ))
        ));

        let wrong_scope = StoreShardIdV1::project(
            registered.brain_id.clone(),
            registered.profile_id.clone(),
            project,
        );
        assert!(matches!(
            reader.validate_mount_identity(&profile, &scope, &wrong_scope),
            Err(ProviderHistoryErrorV1::Ineligible(
                "registered project/profile"
            ))
        ));
    }

    #[test]
    fn hook_origin_reader_rejects_a_miswired_data_root() {
        let actual = tempfile::tempdir().unwrap();
        let expected = tempfile::tempdir().unwrap();
        let reader = HookOriginReaderV1::new(
            actual.path().to_path_buf(),
            BrainId::new("brain.history-root").unwrap(),
            UserProfileId::new("profile.history-root").unwrap(),
        );

        reader.validate_data_root(actual.path()).unwrap();
        assert!(matches!(
            reader.validate_data_root(expected.path()),
            Err(ProviderHistoryErrorV1::Ineligible(
                "history reader data root"
            ))
        ));
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn hook_origin_reader_rejects_a_same_path_data_root_replacement() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("provider-data");
        let previous = temporary.path().join("provider-data-previous");
        std::fs::create_dir(&root).unwrap();
        let reader = HookOriginReaderV1::new(
            root.clone(),
            BrainId::new("brain.history-root-replacement").unwrap(),
            UserProfileId::new("profile.history-root-replacement").unwrap(),
        );

        reader.bind_data_root(&root).unwrap();
        std::fs::rename(&root, &previous).unwrap();
        std::fs::create_dir(&root).unwrap();

        assert!(matches!(
            reader.validate_data_root(&root),
            Err(ProviderHistoryErrorV1::Ineligible(
                "history reader data root"
            ))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn hook_origin_reader_rejects_a_symlink_retarget_after_binding() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("provider-data");
        let previous = temporary.path().join("provider-data-previous");
        let replacement = temporary.path().join("provider-data-replacement");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&replacement).unwrap();
        let reader = HookOriginReaderV1::new(
            root.clone(),
            BrainId::new("brain.history-root-retarget").unwrap(),
            UserProfileId::new("profile.history-root-retarget").unwrap(),
        );

        reader.bind_data_root(&root).unwrap();
        std::fs::rename(&root, &previous).unwrap();
        symlink(&replacement, &root).unwrap();
        assert!(matches!(
            reader.validate_data_root(&root),
            Err(ProviderHistoryErrorV1::Ineligible(
                "history reader data root"
            ))
        ));
        assert!(matches!(
            reader.live_boundaries(),
            Err(ProviderHistoryErrorV1::Ineligible(
                "history reader data root"
            ))
        ));

        std::fs::remove_file(&root).unwrap();
        symlink(&previous, &root).unwrap();
        assert!(matches!(
            reader.validate_data_root(&root),
            Err(ProviderHistoryErrorV1::Ineligible(
                "history reader data root"
            ))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn hook_origin_reader_rejects_a_ledger_parent_rename_between_validation_and_open() {
        let temporary = tempfile::tempdir().unwrap();
        let data_root = temporary.path().join("provider-data");
        let admissions = data_root.join("hook-v2-admissions");
        let previous = temporary.path().join("hook-v2-admissions-previous");
        let replacement = temporary.path().join("hook-v2-admissions-replacement");
        std::fs::create_dir_all(admissions.join("claude")).unwrap();
        std::fs::create_dir_all(replacement.join("claude")).unwrap();
        let reader = HookOriginReaderV1::new(
            data_root.clone(),
            BrainId::new("brain.history-ledger-parent").unwrap(),
            UserProfileId::new("profile.history-ledger-parent").unwrap(),
        );
        reader.bind_data_root(&data_root).unwrap();

        // The callback runs after pathname admission and before the
        // handle-relative descendant read, which makes the parent swap
        // deterministic.
        let result = reader.read_ledger_with_identity(
            tracedecay_hooks::HookHostV1::ClaudeCode,
            "live session boundary",
            |root, host| {
                std::fs::rename(&admissions, &previous).unwrap();
                std::fs::rename(&replacement, &admissions).unwrap();
                root.map_or_else(
                    || Ok(Vec::new()),
                    |root| root.read_boundaries(host, tracedecay_contracts::now_micros()),
                )
            },
        );
        assert!(matches!(
            result,
            Err(ProviderHistoryErrorV1::Ineligible(
                "history reader ledger root changed"
            ))
        ));

        std::fs::remove_dir_all(&admissions).unwrap();
        std::fs::rename(previous, admissions).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn hook_origin_reader_rejects_a_ledger_parent_symlink_swap_between_validation_and_open() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().unwrap();
        let data_root = temporary.path().join("provider-data");
        let admissions = data_root.join("hook-v2-admissions");
        let previous = temporary.path().join("hook-v2-admissions-previous");
        let replacement = temporary.path().join("hook-v2-admissions-replacement");
        std::fs::create_dir_all(admissions.join("claude")).unwrap();
        std::fs::create_dir_all(replacement.join("claude")).unwrap();
        let reader = HookOriginReaderV1::new(
            data_root.clone(),
            BrainId::new("brain.history-ledger-parent-symlink").unwrap(),
            UserProfileId::new("profile.history-ledger-parent-symlink").unwrap(),
        );
        reader.bind_data_root(&data_root).unwrap();

        let result = reader.read_ledger_with_identity(
            tracedecay_hooks::HookHostV1::ClaudeCode,
            "live session boundary",
            |root, host| {
                std::fs::rename(&admissions, &previous).unwrap();
                symlink(&replacement, &admissions).unwrap();
                root.map_or_else(
                    || Ok(Vec::new()),
                    |root| root.read_boundaries(host, tracedecay_contracts::now_micros()),
                )
            },
        );
        assert!(matches!(
            result,
            Err(ProviderHistoryErrorV1::Ineligible(
                "history reader ledger root"
            ))
        ));

        std::fs::remove_file(&admissions).unwrap();
        std::fs::rename(previous, admissions).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn hook_origin_reader_uses_the_retained_ledger_handle_across_an_aba_swap() {
        let temporary = tempfile::tempdir().unwrap();
        let data_root = temporary.path().join("provider-data");
        let admissions = data_root.join("hook-v2-admissions");
        let previous = temporary.path().join("hook-v2-admissions-previous");
        let replacement = temporary.path().join("hook-v2-admissions-replacement");
        let original_ledger = admissions.join("claude");
        let replacement_ledger = replacement.join("claude");
        std::fs::create_dir_all(&original_ledger).unwrap();
        std::fs::create_dir_all(&replacement_ledger).unwrap();
        std::fs::write(
            original_ledger.join(LIVE_ORIGINS_FILE),
            b"trusted-by-handle",
        )
        .unwrap();
        std::fs::write(
            replacement_ledger.join(LIVE_ORIGINS_FILE),
            b"foreign-by-path",
        )
        .unwrap();
        let reader = HookOriginReaderV1::new(
            data_root.clone(),
            BrainId::new("brain.history-ledger-aba").unwrap(),
            UserProfileId::new("profile.history-ledger-aba").unwrap(),
        );
        reader.bind_data_root(&data_root).unwrap();

        // The path is restored before the post-read check, so pathname
        // identity has an ABA result. A handle-relative read must still come
        // from the admitted original directory and never observe the foreign
        // replacement bytes.
        let result = reader
            .read_ledger_with_identity(
                tracedecay_hooks::HookHostV1::ClaudeCode,
                "live session boundary",
                |root, _| {
                    std::fs::rename(&admissions, &previous).unwrap();
                    std::fs::rename(&replacement, &admissions).unwrap();
                    let bytes = root
                        .expect("the admitted ledger handle must be retained")
                        .read_file(LIVE_ORIGINS_FILE, MAX_LIVE_ORIGIN_BYTES)?;
                    std::fs::remove_dir_all(&admissions).unwrap();
                    std::fs::rename(&previous, &admissions).unwrap();
                    Ok(bytes)
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(result, b"trusted-by-handle");
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn history_bridge_rejects_a_same_path_git_repository_replacement() {
        let temporary = tempfile::tempdir().unwrap();
        let checkout = temporary.path().join("checkout");
        let replacement = temporary.path().join("replacement");
        init_repository(&checkout);
        init_repository(&replacement);

        let project = ProjectId::new("project.history-replacement").unwrap();
        let profile = UserProfileId::new("profile.history-replacement").unwrap();
        tracedecay_runtime_core::storage::write_repository_identity_marker(
            &checkout,
            project.as_str(),
        )
        .unwrap();
        let scope =
            tracedecay_code_index_runtime::resolved_scope_for_project(&checkout, &project).unwrap();
        let registered = StoreShardIdV1::project_sessions(
            BrainId::new("brain.history-replacement").unwrap(),
            profile.clone(),
            project.clone(),
        );
        let bridge =
            HistoryIdentityBridgeV1::admit(&checkout, &profile, &scope, &registered).unwrap();
        bridge.revalidate().unwrap();

        let previous_git = temporary.path().join("previous.git");
        std::fs::rename(checkout.join(".git"), &previous_git).unwrap();
        std::fs::rename(replacement.join(".git"), checkout.join(".git")).unwrap();
        assert!(
            tracedecay_runtime_core::storage::write_repository_identity_marker(
                &checkout,
                project.as_str(),
            )
            .unwrap()
        );

        let replacement_scope =
            tracedecay_code_index_runtime::resolved_scope_for_project(&checkout, &project).unwrap();
        assert_eq!(
            replacement_scope, scope,
            "the replacement preserves path-derived project/session identity"
        );
        let replacement_bridge =
            HistoryIdentityBridgeV1::admit(&checkout, &profile, &replacement_scope, &registered)
                .unwrap();
        replacement_bridge.revalidate().unwrap();

        assert!(matches!(
            bridge.revalidate(),
            Err(ProviderHistoryErrorV1::Ineligible("git repository changed"))
        ));
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn history_bridge_rejects_in_place_git_content_replacement_with_same_ref() {
        let temporary = tempfile::tempdir().unwrap();
        let checkout = temporary.path().join("checkout");
        init_repository(&checkout);

        let project = ProjectId::new("project.history-content-replacement").unwrap();
        let profile = UserProfileId::new("profile.history-content-replacement").unwrap();
        tracedecay_runtime_core::storage::write_repository_identity_marker(
            &checkout,
            project.as_str(),
        )
        .unwrap();
        let scope =
            tracedecay_code_index_runtime::resolved_scope_for_project(&checkout, &project).unwrap();
        let registered = StoreShardIdV1::project_sessions(
            BrainId::new("brain.history-content-replacement").unwrap(),
            profile.clone(),
            project.clone(),
        );
        let bridge =
            HistoryIdentityBridgeV1::admit(&checkout, &profile, &scope, &registered).unwrap();
        bridge.revalidate().unwrap();

        let git_dir = checkout.join(".git");
        let git_identity = PhysicalPathIdentityV1::capture(
            &git_dir,
            "git repository entry",
            PhysicalPathKind::Entry,
        )
        .unwrap();
        std::fs::write(checkout.join("tracked"), b"history identity replacement").unwrap();
        git(&checkout, &["add", "tracked"]);
        git(&checkout, &["commit", "-q", "-m", "replace git content"]);

        assert_eq!(
            PhysicalPathIdentityV1::capture(
                &git_dir,
                "git repository entry",
                PhysicalPathKind::Entry,
            )
            .unwrap(),
            git_identity,
            "the .git directory entry remains the same physical object"
        );
        let current_scope =
            tracedecay_code_index_runtime::resolved_scope_for_project(&checkout, &project).unwrap();
        assert_eq!(
            current_scope.reference, scope.reference,
            "the attached ref spelling remains unchanged"
        );
        assert_eq!(
            current_scope, scope,
            "path-derived scope identity remains unchanged after the content edit"
        );
        assert!(matches!(
            bridge.revalidate(),
            Err(ProviderHistoryErrorV1::Ineligible(
                "git commit/tree changed"
            ))
        ));
    }

    #[tokio::test]
    async fn pending_authority_read_preserves_cancellation_and_deadline_terminals() {
        let cancellation = CancellationToken::new();
        let control = OperationControl::new(
            tracedecay_contracts::now_micros().0 + 30_000_000,
            30_000,
            cancellation.clone(),
        );
        let read = bounded_read(
            &control,
            std::future::pending::<Result<(), ()>>(),
            "pending",
        );
        let cancel = async {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            cancellation.cancel();
        };
        let (result, ()) = tokio::join!(
            tokio::time::timeout(std::time::Duration::from_millis(500), read),
            cancel,
        );
        assert!(matches!(
            result,
            Ok(Err(ProviderHistoryErrorV1::Control(
                TerminalCode::Cancelled
            )))
        ));
        let deadline = OperationControl::new(
            tracedecay_contracts::now_micros().0 + 25_000,
            25,
            CancellationToken::new(),
        );
        assert!(matches!(
            bounded_read(
                &deadline,
                std::future::pending::<Result<(), ()>>(),
                "pending"
            )
            .await,
            Err(ProviderHistoryErrorV1::Control(
                TerminalCode::DeadlineExceeded
            ))
        ));
    }

    fn replay_admitted_fixture() -> tracedecay_memory_observation::AdmittedObservationV1 {
        use tracedecay_memory_observation::{
            AdmittedObservationV1, CanonicalSettlementReceiptV1, ForgetSourceKeyV1,
            ObservationIdV1, ObservationIdempotencyKeyV1, ObservationPrivacyV1,
            PrivacyClassificationV1, ProvenanceOriginV1, ProviderTargetV1, RetentionClassV1,
            SanitizationBindingV1, SourceAuthorityV1, SourceSequenceV1, SourceStreamIdV1,
            extensions_digest,
        };
        use tracedecay_memory_provider_registry::{
            CanonicalPayload, OwnedVersionedId, PayloadSanitizationReceipt,
            PayloadSanitizationReceiptParts,
        };

        let scope = OwnedExactScope::new(
            "profile.fixture",
            "project.fixture",
            "repository.fixture",
            "worktree.fixture",
            "refs/heads/fixture",
            "session.fixture",
            format!("sha256:{}", "1".repeat(64)),
        )
        .expect("fixture scope");
        let attribution = SourceAttribution {
            source: OriginalSourceIdentity {
                canonical_provider_id: OwnedProviderId::new("claude").expect("source provider"),
                canonical_session_id: "session.fixture".to_owned(),
                source_key: "source.fixture".to_owned(),
                stable_record_id: None,
                observation_id: "record.fixture.7".to_owned(),
                source_revision: Some("revision.7".to_owned()),
                content_sha256: "a".repeat(64),
            },
            origin_scope: OriginScopeEvidence::IngestionOnly,
            source_sequence: 7,
            occurred_at_utc_nanos: None,
            ingested_at_utc_nanos: 1_750_000_000_000_000,
            validity: RecordedValidity::default(),
        };
        let original = source_attribution_json(&attribution).expect("source attribution");
        let value = json!({
            "observation_kind": "session.message_committed.v1",
            "payload_contract": "tracedecay.memory.observation.session-message.v1",
            "canonical_payload": {"message": "exact fixture bytes"},
            "source_identity": {"original_source": original},
        });
        let bytes = serde_json::to_vec(&value).expect("fixture payload");
        let payload_sha256 = hex::encode(Sha256::digest(&bytes));
        let payload = CanonicalPayload::new(
            OwnedVersionedId::new("tracedecay.memory.observation.session-message.v1")
                .expect("payload contract"),
            bytes,
            payload_sha256,
        )
        .expect("canonical payload");
        let extensions = Vec::new();
        let extensions_digest = extensions_digest(&extensions).expect("extensions digest");
        let receipt = PayloadSanitizationReceipt::new(
            PayloadSanitizationReceiptParts::accepted_unmodified_with_extensions(
                "observation-hygiene-policy.v1.3",
                payload.sha256.clone(),
                extensions_digest.clone(),
            ),
        )
        .expect("sanitization receipt");
        let mut admitted = AdmittedObservationV1 {
            observation_id: ObservationIdV1::from_v7_parts(1_750_000_000_000, [7; 10])
                .expect("observation id"),
            idempotency_key: ObservationIdempotencyKeyV1::parse(&"0".repeat(64))
                .expect("temporary key"),
            target: ProviderTargetV1 {
                provider_id: OwnedProviderId::new("tracedecay.native").expect("provider"),
                provider_instance_id: "native.fixture".to_owned(),
                registration_revision: 4,
                ready_receipt_digest: "b".repeat(64),
            },
            exact_scope: scope,
            source: CanonicalSettlementReceiptV1 {
                source_authority: SourceAuthorityV1::HostSession,
                commit_point_id: "fixture.commit".to_owned(),
                source_event_id: "record.fixture.7".to_owned(),
                source_event_revision: 1,
                source_event_sha256: "c".repeat(64),
                source_stream: SourceStreamIdV1::new("fixture.stream").expect("source stream"),
                source_sequence: SourceSequenceV1(7),
                settled_at_unix_micros: 1_750_000_000_000,
                settlement_proof_sha256: "d".repeat(64),
            },
            observation_kind: OwnedVersionedId::new("session.message_committed.v1")
                .expect("observation kind"),
            payload,
            extensions,
            extensions_digest,
            provenance_origin: ProvenanceOriginV1::Agent,
            provenance_sha256: "e".repeat(64),
            privacy: ObservationPrivacyV1 {
                classification: PrivacyClassificationV1::Internal,
                retention_class: RetentionClassV1::Project,
                redaction_revision: 1,
                content_policy_revision: 1,
                forget_source_key: ForgetSourceKeyV1::new("forget:source.fixture")
                    .expect("forget key"),
                expires_at_unix_micros: 1_750_000_100_000,
            },
            sanitization: SanitizationBindingV1 {
                receipt_id: receipt.receipt_id().to_owned(),
                sanitizer_revision: receipt.sanitizer_revision().to_owned(),
                source_payload_sha256: receipt.source_payload_sha256().to_owned(),
                receipt_json: receipt.to_json(),
            },
            occurred_at_unix_micros: 1_749_999_999_000,
            admitted_at_unix_micros: 1_750_000_000_000,
            deadline_unix_micros: 1_750_000_100_000,
            request_id: "fixture.request.7".to_owned(),
            envelope_sha256: String::new(),
        };
        admitted.idempotency_key = admitted.derive_idempotency_key();
        admitted.envelope_sha256 = admitted.expected_envelope_sha256();
        admitted.validate().expect("valid admitted fixture");
        admitted
    }

    #[test]
    fn v2_replay_and_correction_keep_admitted_payload_bytes_exact() {
        let admitted = replay_admitted_fixture();
        let payload_bytes = admitted.payload.bytes.clone();
        let payload: Value = serde_json::from_slice(&payload_bytes).expect("fixture JSON");
        let resolved = resolved_replay_observation(&admitted).expect("replay projection");

        assert_eq!(resolved["observation"], payload);
        assert_eq!(
            resolved["idempotency_key"],
            admitted.idempotency_key.as_str()
        );
        assert_eq!(
            resolved["source_sequence"],
            admitted.source.source_sequence.0
        );
        assert!(payload.get("idempotency_key").is_none());
        assert!(payload.get("source_sequence").is_none());
        assert_eq!(admitted.payload.bytes, payload_bytes);
        assert_eq!(
            admitted.payload.sha256,
            hex::encode(Sha256::digest(&payload_bytes))
        );
    }
}

impl MountedOriginalObservationAuthorityV1 {
    /// Point session-row admission uses this exact provider/session boundary.
    /// Existing scope encoding remains unchanged and does not add a provider salt.
    pub(crate) fn authorize_live_canonical_session(
        &self,
        canonical_provider_id: &str,
        session_id: &str,
        project_path: &str,
        destination: &OwnedExactScope,
        control: &tracedecay_memory_provider_registry::OperationControl,
    ) -> HistoryResult<()> {
        self.bridge.authorize_control_scope(destination, control)?;
        if HookOriginReaderV1::host(canonical_provider_id).is_none() {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "canonical hook provider",
            ));
        }
        let canonical_project_path = std::fs::canonicalize(project_path)
            .map_err(|_| ProviderHistoryErrorV1::Unavailable("canonical session project path"))?;
        if canonical_project_path != self.bridge.project_root {
            return Err(ProviderHistoryErrorV1::Ineligible(
                "canonical session project",
            ));
        }
        let mut admitted = false;
        for boundary in self.reader.live_boundaries()? {
            control
                .snapshot()
                .map_err(ProviderHistoryErrorV1::Control)?;
            let repository = &boundary.observation.scope.repository;
            let source = &boundary.observation.source;
            if source.provider().as_str() == canonical_provider_id
                && source.session_id().as_str() == session_id
                && repository.project_id() == Some(&self.bridge.canonical_project)
                && repository.repository_id() == &self.bridge.canonical_repository
                && repository.worktree_id() == Some(&self.bridge.canonical_worktree)
                && repository.evidence().attached_ref().value()
                    == self.bridge.scope.reference.as_ref()
                && self
                    .bridge
                    .destination(session_id)
                    .is_ok_and(|scope| &scope == destination)
            {
                admitted = true;
            }
        }
        self.bridge.authorize_control_scope(destination, control)?;
        if admitted {
            Ok(())
        } else {
            Err(ProviderHistoryErrorV1::Ineligible(
                "current canonical session boundary",
            ))
        }
    }
}
