//! Journaled authority for the complete semantic component set.
//!
//! This module owns the last admission boundary before a semantic component
//! can be served.  Callers provide already verified, immutable identities;
//! the authority does not download, build, or select a model or index while
//! opening or restarting.  A transition becomes visible only after its
//! journal commit is durable.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use thiserror::Error;
use tracedecay_contracts::{
    SemanticActivationAuthorityReceiptV1, SemanticActivationAvailabilityV1,
    SemanticActivationBindingV1, SemanticActivationContractErrorV1,
    SemanticActivationJournalEntryV1, SemanticActivationJournalErrorV1,
    SemanticActivationJournalPhaseV1, SemanticActivationJournalPortV1,
    SemanticActivationOperationV1, SemanticActivationUnavailableReasonV1,
};
use tracedecay_domain::configuration::ConfigurationRevisionId;
use tracedecay_domain::{ManifestDigest, UtcMicros};

static JOURNAL_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// A complete, compatibility-checked candidate supplied by the configuration
/// authority.  The binding carries every identity and epoch used by the
/// admission decision; this type carries the compare-and-swap fence for the
/// currently served receipt.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SemanticActivationCandidateV1 {
    pub configuration_revision: ConfigurationRevisionId,
    pub binding: SemanticActivationBindingV1,
    pub expected_current_receipt_digest: Option<ManifestDigest>,
}

impl SemanticActivationCandidateV1 {
    pub fn new(
        configuration_revision: ConfigurationRevisionId,
        binding: SemanticActivationBindingV1,
        expected_current_receipt_digest: Option<ManifestDigest>,
    ) -> Result<Self, SemanticActivationContractErrorV1> {
        let candidate = Self {
            configuration_revision,
            binding,
            expected_current_receipt_digest,
        };
        candidate.validate()?;
        Ok(candidate)
    }

    pub fn validate(&self) -> Result<(), SemanticActivationContractErrorV1> {
        self.configuration_revision.validate().map_err(|_| {
            SemanticActivationContractErrorV1::InvalidReceipt("configuration_revision")
        })?;
        self.binding.validate()?;
        if let Some(digest) = &self.expected_current_receipt_digest {
            digest.validate().map_err(|_| {
                SemanticActivationContractErrorV1::InvalidReceipt("expected_current_receipt_digest")
            })?;
        }
        Ok(())
    }
}

/// Errors from the activation authority.  Persisted identity failures are
/// exposed as typed unavailable states so callers can fail closed without
/// guessing whether a missing, stale, corrupt, or incompatible component is
/// safe to route.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum SemanticActivationAuthorityErrorV1 {
    #[error("semantic activation is unavailable: {reason:?}")]
    Unavailable {
        reason: SemanticActivationUnavailableReasonV1,
    },
    #[error("semantic activation candidate was rejected: {0}")]
    InvalidCandidate(SemanticActivationContractErrorV1),
    #[error("semantic activation compare-and-swap conflicted")]
    Conflict,
    #[error("semantic activation journal failed: {0}")]
    Journal(SemanticActivationJournalErrorV1),
    #[error("semantic activation commit failed ({commit}) and rollback marker failed ({rollback})")]
    JournalRecovery {
        commit: SemanticActivationJournalErrorV1,
        rollback: SemanticActivationJournalErrorV1,
    },
    #[error("semantic activation authority has no rollback receipt")]
    NoRollback,
    #[error("semantic activation authority state is corrupt")]
    InvalidState,
}

impl From<SemanticActivationContractErrorV1> for SemanticActivationAuthorityErrorV1 {
    fn from(error: SemanticActivationContractErrorV1) -> Self {
        Self::InvalidCandidate(error)
    }
}

/// A small in-memory journal useful for composition tests and embedders that
/// supply their own durable implementation.  The production authority only
/// requires [`SemanticActivationJournalPortV1`].
#[derive(Clone, Default)]
pub struct MemorySemanticActivationJournalV1 {
    state: Arc<Mutex<MemorySemanticActivationJournalStateV1>>,
}

#[derive(Default)]
struct MemorySemanticActivationJournalStateV1 {
    entries: Vec<SemanticActivationJournalEntryV1>,
    fail_next_append: bool,
    append_count: usize,
    fail_on_append: Option<usize>,
}

impl MemorySemanticActivationJournalV1 {
    pub fn snapshot(&self) -> Vec<SemanticActivationJournalEntryV1> {
        self.state
            .lock()
            .map(|state| state.entries.clone())
            .unwrap_or_default()
    }

    /// Cause the next append to fail.  This is intentionally a test seam for
    /// proving that an uncommitted transition never becomes visible.
    pub fn fail_next_append(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.fail_next_append = true;
        }
    }

    /// Fail one exact append call.  The call count includes successful and
    /// failed writes, which lets recovery tests target the second commit write
    /// and the subsequent rollback marker independently.
    pub fn fail_on_append(&self, append_number: usize) {
        if let Ok(mut state) = self.state.lock() {
            state.fail_on_append = Some(append_number);
        }
    }

    /// Replace the in-memory log for corruption/recovery tests.  Durable
    /// implementations should expose their own migration or repair surface.
    #[cfg(test)]
    fn replace_for_test(&self, entries: Vec<SemanticActivationJournalEntryV1>) {
        if let Ok(mut state) = self.state.lock() {
            state.entries = entries;
        }
    }
}

impl SemanticActivationJournalPortV1 for MemorySemanticActivationJournalV1 {
    fn entries(
        &self,
    ) -> Result<Vec<SemanticActivationJournalEntryV1>, SemanticActivationJournalErrorV1> {
        self.state
            .lock()
            .map(|state| state.entries.clone())
            .map_err(|_| SemanticActivationJournalErrorV1::Corrupt)
    }

    fn append_if_sequence(
        &self,
        expected_sequence: u64,
        entry: SemanticActivationJournalEntryV1,
    ) -> Result<(), SemanticActivationJournalErrorV1> {
        entry
            .validate()
            .map_err(|_| SemanticActivationJournalErrorV1::Corrupt)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| SemanticActivationJournalErrorV1::Corrupt)?;
        state.append_count = state.append_count.saturating_add(1);
        if state.fail_next_append {
            state.fail_next_append = false;
            return Err(SemanticActivationJournalErrorV1::Unavailable);
        }
        if state.fail_on_append == Some(state.append_count) {
            state.fail_on_append = None;
            return Err(SemanticActivationJournalErrorV1::Unavailable);
        }
        validate_append(&state.entries, expected_sequence, &entry)?;
        state.entries.push(entry);
        Ok(())
    }
}

/// Durable production journal for semantic activation authority.
///
/// The data file is replaced as a complete JSON value after every phase. A
/// stable sibling lock serializes the read/validate/write sequence, so two
/// authority instances cannot both claim the same sequence even when they
/// were opened before either one appended. The lock is advisory and owned by
/// the OS; a crashed process therefore cannot leave a stale recovery marker.
#[derive(Clone, Debug)]
pub struct DurableSemanticActivationJournalV1 {
    path: Arc<PathBuf>,
}

impl DurableSemanticActivationJournalV1 {
    /// Open a journal at `path`. A missing data file is an empty journal; its
    /// parent and lock file are created on the first append.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, SemanticActivationJournalErrorV1> {
        let path = path.into();
        if path.as_os_str().is_empty() || path.is_dir() {
            return Err(SemanticActivationJournalErrorV1::Corrupt);
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|_| SemanticActivationJournalErrorV1::Unavailable)?;
        }
        let journal = Self {
            path: Arc::new(path),
        };
        let _ = journal.read_entries()?;
        Ok(journal)
    }

    /// Construct the canonical sidecar location for one registered database
    /// and scope. The scope digest is part of the filename so separate
    /// projects/worktrees never share activation authority state.
    pub fn for_database(
        database: &tracedecay_global_db::RegisteredGlobalDbLeaseV1,
        scope: &tracedecay_contracts::ResolvedScope,
    ) -> Result<Self, SemanticActivationJournalErrorV1> {
        scope
            .validate()
            .map_err(|_| SemanticActivationJournalErrorV1::Corrupt)?;
        let database_path = database.db_path();
        let file_name = database_path
            .file_name()
            .ok_or(SemanticActivationJournalErrorV1::Corrupt)?;
        let mut name = file_name.to_os_string();
        name.push(".semantic-activation-");
        // Manifest digests contain `:`, which is not a legal Windows filename
        // character. Keep the algorithm tag while using a stable separator
        // that is safe on every supported host.
        name.push(scope.scope_digest.as_str().replace(':', "-"));
        name.push(".journal.json");
        Self::open(database_path.with_file_name(name))
    }

    pub fn path(&self) -> &Path {
        self.path.as_ref()
    }

    fn read_entries(
        &self,
    ) -> Result<Vec<SemanticActivationJournalEntryV1>, SemanticActivationJournalErrorV1> {
        let bytes = match fs::read(self.path.as_ref()) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(_) => return Err(SemanticActivationJournalErrorV1::Unavailable),
        };
        if bytes.is_empty() {
            return Err(SemanticActivationJournalErrorV1::Corrupt);
        }
        let entries: Vec<SemanticActivationJournalEntryV1> = serde_json::from_slice(&bytes)
            .map_err(|_| SemanticActivationJournalErrorV1::Corrupt)?;
        validate_entries(&entries)?;
        Ok(entries)
    }

    fn write_entries(
        &self,
        entries: &[SemanticActivationJournalEntryV1],
    ) -> Result<(), SemanticActivationJournalErrorV1> {
        let payload =
            serde_json::to_vec(entries).map_err(|_| SemanticActivationJournalErrorV1::Corrupt)?;
        let sequence = JOURNAL_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let mut temporary = self.path.as_ref().to_path_buf();
        let extension = format!("tmp-{}-{}", std::process::id(), sequence);
        temporary.set_extension(extension);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|_| SemanticActivationJournalErrorV1::Unavailable)?;
        file.write_all(&payload)
            .and_then(|()| file.sync_all())
            .map_err(|_| SemanticActivationJournalErrorV1::Unavailable)?;
        drop(file);
        if let Err(error) = fs::rename(&temporary, self.path.as_ref()) {
            let _ = fs::remove_file(&temporary);
            return Err(if error.kind() == std::io::ErrorKind::AlreadyExists {
                SemanticActivationJournalErrorV1::Conflict
            } else {
                SemanticActivationJournalErrorV1::Unavailable
            });
        }
        sync_parent_directory(self.path.as_ref())
    }
}

impl SemanticActivationJournalPortV1 for DurableSemanticActivationJournalV1 {
    fn entries(
        &self,
    ) -> Result<Vec<SemanticActivationJournalEntryV1>, SemanticActivationJournalErrorV1> {
        self.read_entries()
    }

    fn append_if_sequence(
        &self,
        expected_sequence: u64,
        entry: SemanticActivationJournalEntryV1,
    ) -> Result<(), SemanticActivationJournalErrorV1> {
        entry
            .validate()
            .map_err(|_| SemanticActivationJournalErrorV1::Corrupt)?;
        let lock_path = tracedecay_runtime_core::storage::append_lock_path(self.path.as_ref());
        let lock = tracedecay_runtime_core::storage::acquire_sidecar_lock_blocking(&lock_path)
            .map_err(|_| SemanticActivationJournalErrorV1::Unavailable)?;
        let mut entries = self.read_entries()?;
        validate_append(&entries, expected_sequence, &entry)?;
        entries.push(entry);
        let result = self.write_entries(&entries);
        drop(lock);
        result
    }
}

fn validate_entries(
    entries: &[SemanticActivationJournalEntryV1],
) -> Result<(), SemanticActivationJournalErrorV1> {
    let mut expected_sequence = 0;
    let mut prepared = false;
    for entry in entries {
        entry
            .validate()
            .map_err(|_| SemanticActivationJournalErrorV1::Corrupt)?;
        match entry.phase {
            SemanticActivationJournalPhaseV1::Prepared => {
                if prepared || entry.sequence != expected_sequence.saturating_add(1) {
                    return Err(SemanticActivationJournalErrorV1::Corrupt);
                }
                prepared = true;
            }
            SemanticActivationJournalPhaseV1::Committed
            | SemanticActivationJournalPhaseV1::RolledBack => {
                if matches!(entry.phase, SemanticActivationJournalPhaseV1::Committed)
                    && matches!(
                        entry.receipt.operation,
                        SemanticActivationOperationV1::Restart
                    )
                {
                    if prepared || entry.sequence != expected_sequence.saturating_add(1) {
                        return Err(SemanticActivationJournalErrorV1::Corrupt);
                    }
                    expected_sequence = entry.sequence;
                    continue;
                }
                if !prepared || entry.sequence != expected_sequence.saturating_add(1) {
                    return Err(SemanticActivationJournalErrorV1::Corrupt);
                }
                prepared = false;
                expected_sequence = entry.sequence;
            }
        }
    }
    if prepared {
        return Err(SemanticActivationJournalErrorV1::Corrupt);
    }
    Ok(())
}

fn validate_append(
    entries: &[SemanticActivationJournalEntryV1],
    expected_sequence: u64,
    entry: &SemanticActivationJournalEntryV1,
) -> Result<(), SemanticActivationJournalErrorV1> {
    let latest_sequence = entries.last().map(|entry| entry.sequence).unwrap_or(0);
    let expected_entry_sequence = expected_sequence
        .checked_add(1)
        .ok_or(SemanticActivationJournalErrorV1::Corrupt)?;
    if entry.sequence != expected_entry_sequence {
        return Err(SemanticActivationJournalErrorV1::Conflict);
    }
    match entry.phase {
        SemanticActivationJournalPhaseV1::Prepared => {
            if latest_sequence != expected_sequence
                || entries.last().is_some_and(|last| {
                    last.sequence == entry.sequence
                        && matches!(last.phase, SemanticActivationJournalPhaseV1::Prepared)
                })
            {
                return Err(SemanticActivationJournalErrorV1::Conflict);
            }
        }
        SemanticActivationJournalPhaseV1::Committed
        | SemanticActivationJournalPhaseV1::RolledBack => {
            if matches!(entry.phase, SemanticActivationJournalPhaseV1::Committed)
                && matches!(
                    entry.receipt.operation,
                    SemanticActivationOperationV1::Restart
                )
            {
                if latest_sequence != expected_sequence {
                    return Err(SemanticActivationJournalErrorV1::Conflict);
                }
                return Ok(());
            }
            let Some(previous) = entries.last() else {
                return Err(SemanticActivationJournalErrorV1::Conflict);
            };
            if previous.sequence != entry.sequence
                || !matches!(previous.phase, SemanticActivationJournalPhaseV1::Prepared)
                || previous.receipt != entry.receipt
            {
                return Err(SemanticActivationJournalErrorV1::Conflict);
            }
        }
    }
    Ok(())
}

fn sync_parent_directory(path: &Path) -> Result<(), SemanticActivationJournalErrorV1> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    #[cfg(unix)]
    {
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| SemanticActivationJournalErrorV1::Unavailable)?;
    }
    #[cfg(not(unix))]
    let _ = parent;
    Ok(())
}

#[derive(Clone, Default)]
struct AuthorityStateV1 {
    current: Option<SemanticActivationAuthorityReceiptV1>,
    rollback: Option<SemanticActivationAuthorityReceiptV1>,
    next_sequence: u64,
    unavailable: Option<SemanticActivationUnavailableReasonV1>,
}

/// Journal-backed semantic activation authority.
pub struct SemanticActivationAuthorityV1<J>
where
    J: SemanticActivationJournalPortV1,
{
    journal: Arc<J>,
    state: Mutex<AuthorityStateV1>,
}

impl<J> SemanticActivationAuthorityV1<J>
where
    J: SemanticActivationJournalPortV1,
{
    /// Open and replay a journal.  Replay validates the complete receipt
    /// identity and transition lineage before exposing a current component.
    /// An incomplete or malformed log is typed as `Unavailable(Corrupt)`.
    pub fn open(journal: J) -> Result<Self, SemanticActivationAuthorityErrorV1> {
        Self::open_shared(Arc::new(journal))
    }

    pub fn open_shared(journal: Arc<J>) -> Result<Self, SemanticActivationAuthorityErrorV1> {
        let entries = journal.entries().map_err(|error| match error {
            SemanticActivationJournalErrorV1::Corrupt => unavailable_corrupt(),
            SemanticActivationJournalErrorV1::Unavailable => {
                SemanticActivationAuthorityErrorV1::Journal(error)
            }
            SemanticActivationJournalErrorV1::Conflict => {
                SemanticActivationAuthorityErrorV1::Conflict
            }
        })?;
        let state = replay_entries(&entries)?;
        Ok(Self {
            journal,
            state: Mutex::new(state),
        })
    }

    pub fn journal(&self) -> Arc<J> {
        Arc::clone(&self.journal)
    }

    /// Return the fail-closed serving state.  Empty journals are valid but
    /// report `Missing` until a complete activation commits.
    pub fn status(&self) -> SemanticActivationAvailabilityV1 {
        let Ok(state) = self.state.lock() else {
            return SemanticActivationAvailabilityV1::Unavailable {
                reason: SemanticActivationUnavailableReasonV1::Corrupt,
            };
        };
        if let Some(reason) = state.unavailable {
            return SemanticActivationAvailabilityV1::Unavailable { reason };
        }
        match &state.current {
            Some(receipt) => SemanticActivationAvailabilityV1::Ready {
                receipt: receipt.clone(),
            },
            None => SemanticActivationAvailabilityV1::Unavailable {
                reason: SemanticActivationUnavailableReasonV1::Missing,
            },
        }
    }

    pub fn current_receipt(&self) -> Option<SemanticActivationAuthorityReceiptV1> {
        self.state
            .lock()
            .ok()
            .and_then(|state| state.current.clone())
    }

    pub fn rollback_receipt(&self) -> Option<SemanticActivationAuthorityReceiptV1> {
        self.state
            .lock()
            .ok()
            .and_then(|state| state.rollback.clone())
    }

    /// Mark a mounted component unavailable after an external freshness or
    /// compatibility check.  No component is unloaded or rebuilt here; the
    /// next activation must supply a fresh complete binding.
    pub fn mark_unavailable(&self, reason: SemanticActivationUnavailableReasonV1) {
        if let Ok(mut state) = self.state.lock() {
            state.unavailable = Some(reason);
        }
    }

    /// Admit a complete compatible component after a durable prepared/commit
    /// pair.  State is swapped only after both journal writes succeed.
    pub fn activate(
        &self,
        candidate: SemanticActivationCandidateV1,
        now: UtcMicros,
    ) -> Result<SemanticActivationAuthorityReceiptV1, SemanticActivationAuthorityErrorV1> {
        candidate.validate()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| SemanticActivationAuthorityErrorV1::InvalidState)?;
        refresh_state(self.journal.as_ref(), &mut state)?;
        ensure_activation_available(&state)?;
        ensure_expected_current(&state, candidate.expected_current_receipt_digest.as_ref())?;
        ensure_candidate_epochs_are_fresh(&state, &candidate.binding)?;

        let sequence = next_sequence(&state)?;
        let previous_receipt_digest = state
            .current
            .as_ref()
            .map(|receipt| receipt.receipt_digest.clone());
        let receipt = SemanticActivationAuthorityReceiptV1::issue(
            SemanticActivationOperationV1::Activate,
            candidate.configuration_revision,
            candidate.binding,
            previous_receipt_digest,
            sequence,
            now,
        )?;
        let mut next = state.clone();
        apply_receipt(&mut next, receipt.clone())?;
        match append_transition(
            self.journal.as_ref(),
            state.next_sequence,
            &receipt,
            &mut next,
        ) {
            Ok(()) => {
                *state = next;
                Ok(receipt)
            }
            Err(error @ SemanticActivationAuthorityErrorV1::JournalRecovery { .. }) => {
                state.unavailable = Some(SemanticActivationUnavailableReasonV1::Corrupt);
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    /// Swap to the previously committed component after a durable
    /// prepared/commit pair. The supplied digest is mandatory whenever a
    /// current receipt exists and is the CAS fence for the currently served
    /// receipt; the empty-journal case accepts `None` exactly once.
    pub fn rollback(
        &self,
        expected_current_receipt_digest: Option<ManifestDigest>,
        now: UtcMicros,
    ) -> Result<SemanticActivationAuthorityReceiptV1, SemanticActivationAuthorityErrorV1> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SemanticActivationAuthorityErrorV1::InvalidState)?;
        refresh_state(self.journal.as_ref(), &mut state)?;
        ensure_available(&state)?;
        ensure_expected_current(&state, expected_current_receipt_digest.as_ref())?;
        let target = state
            .rollback
            .as_ref()
            .ok_or(SemanticActivationAuthorityErrorV1::NoRollback)?;
        let current =
            state
                .current
                .as_ref()
                .ok_or(SemanticActivationAuthorityErrorV1::Unavailable {
                    reason: SemanticActivationUnavailableReasonV1::Missing,
                })?;
        let receipt = SemanticActivationAuthorityReceiptV1::issue(
            SemanticActivationOperationV1::Rollback,
            target.configuration_revision.clone(),
            target.binding.clone(),
            Some(current.receipt_digest.clone()),
            next_sequence(&state)?,
            now,
        )?;
        let mut next = state.clone();
        apply_receipt(&mut next, receipt.clone())?;
        match append_transition(
            self.journal.as_ref(),
            state.next_sequence,
            &receipt,
            &mut next,
        ) {
            Ok(()) => {
                *state = next;
                Ok(receipt)
            }
            Err(error @ SemanticActivationAuthorityErrorV1::JournalRecovery { .. }) => {
                state.unavailable = Some(SemanticActivationUnavailableReasonV1::Corrupt);
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    /// Journal a restart remount for the current, already verified
    /// component.  Restart has no prepared phase because it does not change
    /// component selection; a committed restart receipt still records the
    /// lineage observed by the next process.
    pub fn restart(
        &self,
        now: UtcMicros,
    ) -> Result<SemanticActivationAuthorityReceiptV1, SemanticActivationAuthorityErrorV1> {
        let expected = self
            .current_receipt()
            .ok_or(SemanticActivationAuthorityErrorV1::Unavailable {
                reason: SemanticActivationUnavailableReasonV1::Missing,
            })?
            .receipt_digest;
        self.restart_with_expected(expected, now)
    }

    /// Journal a restart only when the caller still observes the receipt it is
    /// restarting.  The plain [`Self::restart`] convenience obtains this
    /// fence from the same authority instance, while production recovery can
    /// supply the durable receipt it observed during project open.
    pub fn restart_with_expected(
        &self,
        expected_current_receipt_digest: ManifestDigest,
        now: UtcMicros,
    ) -> Result<SemanticActivationAuthorityReceiptV1, SemanticActivationAuthorityErrorV1> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SemanticActivationAuthorityErrorV1::InvalidState)?;
        refresh_state(self.journal.as_ref(), &mut state)?;
        ensure_available(&state)?;
        ensure_expected_current(&state, Some(&expected_current_receipt_digest))?;
        let current =
            state
                .current
                .as_ref()
                .ok_or(SemanticActivationAuthorityErrorV1::Unavailable {
                    reason: SemanticActivationUnavailableReasonV1::Missing,
                })?;
        let receipt = SemanticActivationAuthorityReceiptV1::issue(
            SemanticActivationOperationV1::Restart,
            current.configuration_revision.clone(),
            current.binding.clone(),
            Some(current.receipt_digest.clone()),
            next_sequence(&state)?,
            now,
        )?;
        let mut next = state.clone();
        apply_receipt(&mut next, receipt.clone())?;
        append_restart(self.journal.as_ref(), state.next_sequence, &receipt)?;
        *state = next;
        Ok(receipt)
    }
}

fn ensure_available(state: &AuthorityStateV1) -> Result<(), SemanticActivationAuthorityErrorV1> {
    state.unavailable.map_or(Ok(()), |reason| {
        Err(SemanticActivationAuthorityErrorV1::Unavailable { reason })
    })
}

fn ensure_activation_available(
    state: &AuthorityStateV1,
) -> Result<(), SemanticActivationAuthorityErrorV1> {
    match state.unavailable {
        Some(SemanticActivationUnavailableReasonV1::Corrupt) => ensure_available(state),
        Some(
            SemanticActivationUnavailableReasonV1::Missing
            | SemanticActivationUnavailableReasonV1::Stale
            | SemanticActivationUnavailableReasonV1::Incompatible,
        )
        | None => Ok(()),
    }
}

fn ensure_expected_current(
    state: &AuthorityStateV1,
    expected: Option<&ManifestDigest>,
) -> Result<(), SemanticActivationAuthorityErrorV1> {
    let actual = state
        .current
        .as_ref()
        .map(|receipt| &receipt.receipt_digest);
    if actual != expected {
        return Err(SemanticActivationAuthorityErrorV1::Conflict);
    }
    Ok(())
}

fn ensure_candidate_epochs_are_fresh(
    state: &AuthorityStateV1,
    binding: &SemanticActivationBindingV1,
) -> Result<(), SemanticActivationAuthorityErrorV1> {
    let Some(current) = state.current.as_ref() else {
        return Ok(());
    };
    if binding_epochs_decrease(binding, &current.binding) {
        return Err(SemanticActivationAuthorityErrorV1::Unavailable {
            reason: SemanticActivationUnavailableReasonV1::Stale,
        });
    }
    Ok(())
}

fn binding_epochs_decrease(
    candidate: &SemanticActivationBindingV1,
    current: &SemanticActivationBindingV1,
) -> bool {
    [
        candidate.capability_epoch,
        candidate.profile_epoch,
        candidate.calibration_epoch,
        candidate.resource_epoch,
        candidate.runtime_epoch,
        candidate.authorization_epoch,
        candidate.privacy_epoch,
    ]
    .iter()
    .zip([
        current.capability_epoch,
        current.profile_epoch,
        current.calibration_epoch,
        current.resource_epoch,
        current.runtime_epoch,
        current.authorization_epoch,
        current.privacy_epoch,
    ])
    .any(|(candidate, current)| *candidate < current)
}

fn next_sequence(state: &AuthorityStateV1) -> Result<u64, SemanticActivationAuthorityErrorV1> {
    state
        .next_sequence
        .checked_add(1)
        .ok_or(SemanticActivationAuthorityErrorV1::InvalidState)
}

fn append_transition<J>(
    journal: &J,
    expected_sequence: u64,
    receipt: &SemanticActivationAuthorityReceiptV1,
    next: &mut AuthorityStateV1,
) -> Result<(), SemanticActivationAuthorityErrorV1>
where
    J: SemanticActivationJournalPortV1 + ?Sized,
{
    let prepared = SemanticActivationJournalEntryV1 {
        sequence: receipt.sequence,
        phase: SemanticActivationJournalPhaseV1::Prepared,
        receipt: receipt.clone(),
    };
    journal
        .append_if_sequence(expected_sequence, prepared)
        .map_err(map_journal_error)?;
    let committed = SemanticActivationJournalEntryV1 {
        sequence: receipt.sequence,
        phase: SemanticActivationJournalPhaseV1::Committed,
        receipt: receipt.clone(),
    };
    match journal.append_if_sequence(expected_sequence, committed) {
        Ok(()) => Ok(()),
        Err(error) => {
            // A failed commit must not leave a dangling prepared record that
            // could be mistaken for a future activation.  Consume the
            // sequence locally when the marker succeeds; otherwise fail
            // closed because restart cannot establish a complete transition.
            let rolled_back = SemanticActivationJournalEntryV1 {
                sequence: receipt.sequence,
                phase: SemanticActivationJournalPhaseV1::RolledBack,
                receipt: receipt.clone(),
            };
            match journal.append_if_sequence(expected_sequence, rolled_back) {
                Ok(()) => {
                    next.next_sequence = receipt.sequence;
                    Err(SemanticActivationAuthorityErrorV1::Journal(error))
                }
                Err(rollback_error) => Err(SemanticActivationAuthorityErrorV1::JournalRecovery {
                    commit: error,
                    rollback: rollback_error,
                }),
            }
        }
    }
}

fn append_restart<J>(
    journal: &J,
    expected_sequence: u64,
    receipt: &SemanticActivationAuthorityReceiptV1,
) -> Result<(), SemanticActivationAuthorityErrorV1>
where
    J: SemanticActivationJournalPortV1 + ?Sized,
{
    let committed = SemanticActivationJournalEntryV1 {
        sequence: receipt.sequence,
        phase: SemanticActivationJournalPhaseV1::Committed,
        receipt: receipt.clone(),
    };
    journal
        .append_if_sequence(expected_sequence, committed)
        .map_err(map_journal_error)
}

fn refresh_state<J>(
    journal: &J,
    state: &mut AuthorityStateV1,
) -> Result<(), SemanticActivationAuthorityErrorV1>
where
    J: SemanticActivationJournalPortV1 + ?Sized,
{
    let entries = journal.entries().map_err(map_journal_error)?;
    let replayed = replay_entries(&entries)?;
    // Preserve an externally marked stale/incompatible state until a
    // successful activation clears it. Corruption is never overwritten by a
    // refresh that happened to race with another reader.
    let unavailable = state.unavailable;
    *state = replayed;
    state.unavailable = unavailable;
    Ok(())
}

fn map_journal_error(
    error: SemanticActivationJournalErrorV1,
) -> SemanticActivationAuthorityErrorV1 {
    match error {
        SemanticActivationJournalErrorV1::Conflict => SemanticActivationAuthorityErrorV1::Conflict,
        SemanticActivationJournalErrorV1::Corrupt => unavailable_corrupt(),
        SemanticActivationJournalErrorV1::Unavailable => {
            SemanticActivationAuthorityErrorV1::Journal(error)
        }
    }
}

fn replay_entries(
    entries: &[SemanticActivationJournalEntryV1],
) -> Result<AuthorityStateV1, SemanticActivationAuthorityErrorV1> {
    let mut state = AuthorityStateV1::default();
    let mut prepared: Option<SemanticActivationJournalEntryV1> = None;
    for entry in entries {
        entry.validate().map_err(|_| unavailable_corrupt())?;
        match entry.phase {
            SemanticActivationJournalPhaseV1::Prepared => {
                if prepared.is_some() || entry.sequence <= state.next_sequence {
                    return Err(unavailable_corrupt());
                }
                prepared = Some(entry.clone());
            }
            SemanticActivationJournalPhaseV1::Committed => {
                if entry.receipt.operation == SemanticActivationOperationV1::Restart {
                    if prepared.is_some() || entry.sequence <= state.next_sequence {
                        return Err(unavailable_corrupt());
                    }
                    apply_receipt(&mut state, entry.receipt.clone())
                        .map_err(|_| unavailable_corrupt())?;
                    continue;
                }
                let Some(prepared_entry) = prepared.take() else {
                    return Err(unavailable_corrupt());
                };
                if prepared_entry.sequence != entry.sequence
                    || prepared_entry.receipt != entry.receipt
                {
                    return Err(unavailable_corrupt());
                }
                apply_receipt(&mut state, entry.receipt.clone())
                    .map_err(|_| unavailable_corrupt())?;
            }
            SemanticActivationJournalPhaseV1::RolledBack => {
                let Some(prepared_entry) = prepared.take() else {
                    return Err(unavailable_corrupt());
                };
                if prepared_entry.sequence != entry.sequence
                    || prepared_entry.receipt != entry.receipt
                {
                    return Err(unavailable_corrupt());
                }
                state.next_sequence = entry.sequence;
            }
        }
    }
    if prepared.is_some() {
        return Err(unavailable_corrupt());
    }
    Ok(state)
}

fn apply_receipt(
    state: &mut AuthorityStateV1,
    receipt: SemanticActivationAuthorityReceiptV1,
) -> Result<(), SemanticActivationAuthorityErrorV1> {
    receipt
        .validate()
        .map_err(|_| SemanticActivationAuthorityErrorV1::InvalidState)?;
    if receipt.sequence <= state.next_sequence {
        return Err(SemanticActivationAuthorityErrorV1::InvalidState);
    }
    match receipt.operation {
        SemanticActivationOperationV1::Activate => {
            if state
                .current
                .as_ref()
                .is_some_and(|current| binding_epochs_decrease(&receipt.binding, &current.binding))
            {
                return Err(SemanticActivationAuthorityErrorV1::InvalidState);
            }
            let expected_previous = state
                .current
                .as_ref()
                .map(|current| current.receipt_digest.clone());
            if receipt.previous_receipt_digest != expected_previous {
                return Err(SemanticActivationAuthorityErrorV1::InvalidState);
            }
            state.rollback = state.current.clone();
            state.current = Some(receipt.clone());
        }
        SemanticActivationOperationV1::Rollback => {
            let Some(current) = state.current.as_ref() else {
                return Err(SemanticActivationAuthorityErrorV1::InvalidState);
            };
            let Some(rollback) = state.rollback.as_ref() else {
                return Err(SemanticActivationAuthorityErrorV1::InvalidState);
            };
            if receipt.previous_receipt_digest.as_ref() != Some(&current.receipt_digest)
                || receipt.binding != rollback.binding
                || receipt.configuration_revision != rollback.configuration_revision
            {
                return Err(SemanticActivationAuthorityErrorV1::InvalidState);
            }
            state.rollback = Some(current.clone());
            state.current = Some(receipt.clone());
        }
        SemanticActivationOperationV1::Restart => {
            let Some(current) = state.current.as_ref() else {
                return Err(SemanticActivationAuthorityErrorV1::InvalidState);
            };
            if receipt.previous_receipt_digest.as_ref() != Some(&current.receipt_digest)
                || receipt.binding != current.binding
                || receipt.configuration_revision != current.configuration_revision
            {
                return Err(SemanticActivationAuthorityErrorV1::InvalidState);
            }
            state.current = Some(receipt.clone());
        }
    }
    state.next_sequence = receipt.sequence;
    state.unavailable = None;
    Ok(())
}

fn unavailable_corrupt() -> SemanticActivationAuthorityErrorV1 {
    SemanticActivationAuthorityErrorV1::Unavailable {
        reason: SemanticActivationUnavailableReasonV1::Corrupt,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracedecay_domain::CodeGenerationId;

    fn digest(byte: char) -> ManifestDigest {
        ManifestDigest::new(format!("sha256:{}", byte.to_string().repeat(64))).unwrap()
    }

    fn revision(suffix: &str) -> ConfigurationRevisionId {
        ConfigurationRevisionId::try_from(format!("configuration.revision.{suffix}")).unwrap()
    }

    fn binding(seed: char) -> SemanticActivationBindingV1 {
        SemanticActivationBindingV1 {
            model_artifact_digest: digest(seed),
            source_generation: CodeGenerationId::new(format!("source-generation.{seed}")).unwrap(),
            vector_generation_digest: digest('b'),
            projection_key_digest: digest('c'),
            search_index_key_digest: digest('d'),
            capability_digest: digest('e'),
            profile_digest: digest('f'),
            calibration_digest: digest('1'),
            resource_digest: digest('2'),
            runtime_digest: digest('3'),
            capability_epoch: 1,
            profile_epoch: 1,
            calibration_epoch: 1,
            resource_epoch: 1,
            runtime_epoch: 1,
            authorization_epoch: 1,
            privacy_epoch: 1,
        }
    }

    fn candidate(seed: char, expected: Option<ManifestDigest>) -> SemanticActivationCandidateV1 {
        SemanticActivationCandidateV1::new(
            revision(seed.to_string().as_str()),
            binding(seed),
            expected,
        )
        .unwrap()
    }

    #[test]
    fn activation_rollback_and_restart_replay_the_committed_log() {
        let journal = MemorySemanticActivationJournalV1::default();
        let authority = SemanticActivationAuthorityV1::open(journal.clone()).unwrap();
        let first = authority
            .activate(candidate('a', None), UtcMicros(1))
            .unwrap();
        let second = authority
            .activate(
                candidate('b', Some(first.receipt_digest.clone())),
                UtcMicros(2),
            )
            .unwrap();
        let rolled_back = authority
            .rollback(Some(second.receipt_digest.clone()), UtcMicros(3))
            .unwrap();
        assert_eq!(rolled_back.binding, first.binding);
        let restarted = authority.restart(UtcMicros(4)).unwrap();
        assert_eq!(restarted.operation, SemanticActivationOperationV1::Restart);

        let reopened = SemanticActivationAuthorityV1::open(journal).unwrap();
        assert_eq!(reopened.current_receipt(), Some(restarted.clone()));
        assert_eq!(
            reopened.status(),
            SemanticActivationAvailabilityV1::Ready { receipt: restarted }
        );
    }

    #[test]
    fn stale_compare_and_swap_does_not_change_active_component() {
        let authority =
            SemanticActivationAuthorityV1::open(MemorySemanticActivationJournalV1::default())
                .unwrap();
        let first = authority
            .activate(candidate('a', None), UtcMicros(1))
            .unwrap();
        let error = authority
            .activate(candidate('b', Some(digest('0'))), UtcMicros(2))
            .unwrap_err();
        assert_eq!(error, SemanticActivationAuthorityErrorV1::Conflict);
        assert_eq!(authority.current_receipt(), Some(first));
    }

    #[test]
    fn lower_authority_epoch_is_typed_stale_and_does_not_activate() {
        let authority =
            SemanticActivationAuthorityV1::open(MemorySemanticActivationJournalV1::default())
                .unwrap();
        let mut current_binding = binding('a');
        current_binding.profile_epoch = 2;
        let first = authority
            .activate(
                SemanticActivationCandidateV1::new(revision("a"), current_binding, None).unwrap(),
                UtcMicros(1),
            )
            .unwrap();
        let error = authority
            .activate(
                candidate('b', Some(first.receipt_digest.clone())),
                UtcMicros(2),
            )
            .unwrap_err();
        assert_eq!(
            error,
            SemanticActivationAuthorityErrorV1::Unavailable {
                reason: SemanticActivationUnavailableReasonV1::Stale
            }
        );
        assert_eq!(authority.current_receipt(), Some(first));
    }

    #[test]
    fn missing_and_external_stale_states_are_typed_unavailable() {
        let authority =
            SemanticActivationAuthorityV1::open(MemorySemanticActivationJournalV1::default())
                .unwrap();
        assert_eq!(
            authority.status(),
            SemanticActivationAvailabilityV1::Unavailable {
                reason: SemanticActivationUnavailableReasonV1::Missing
            }
        );
        authority.mark_unavailable(SemanticActivationUnavailableReasonV1::Stale);
        assert_eq!(
            authority.status(),
            SemanticActivationAvailabilityV1::Unavailable {
                reason: SemanticActivationUnavailableReasonV1::Stale
            }
        );
        let receipt = authority
            .activate(candidate('a', None), UtcMicros(1))
            .unwrap();
        assert_eq!(
            authority.status(),
            SemanticActivationAvailabilityV1::Ready { receipt }
        );
    }

    #[test]
    fn corrupt_or_incomplete_journal_is_unavailable_on_restart() {
        let journal = MemorySemanticActivationJournalV1::default();
        let authority = SemanticActivationAuthorityV1::open(journal.clone()).unwrap();
        let receipt = authority
            .activate(candidate('a', None), UtcMicros(1))
            .unwrap();
        journal.replace_for_test(vec![SemanticActivationJournalEntryV1 {
            sequence: receipt.sequence,
            phase: SemanticActivationJournalPhaseV1::Prepared,
            receipt,
        }]);
        assert!(matches!(
            SemanticActivationAuthorityV1::open(journal),
            Err(SemanticActivationAuthorityErrorV1::Unavailable {
                reason: SemanticActivationUnavailableReasonV1::Corrupt
            })
        ));
    }

    #[test]
    fn failed_commit_leaves_previous_state_visible() {
        let journal = MemorySemanticActivationJournalV1::default();
        let authority = SemanticActivationAuthorityV1::open(journal.clone()).unwrap();
        let first = authority
            .activate(candidate('a', None), UtcMicros(1))
            .unwrap();
        journal.fail_next_append();
        assert!(matches!(
            authority.activate(
                candidate('b', Some(first.receipt_digest.clone())),
                UtcMicros(2)
            ),
            Err(SemanticActivationAuthorityErrorV1::Journal(
                SemanticActivationJournalErrorV1::Unavailable
            ))
        ));
        assert_eq!(authority.current_receipt(), Some(first));
    }

    #[test]
    fn competing_authorities_share_the_sequence_and_receipt_cas() {
        let journal = MemorySemanticActivationJournalV1::default();
        let first_authority = SemanticActivationAuthorityV1::open(journal.clone()).unwrap();
        let second_authority = SemanticActivationAuthorityV1::open(journal).unwrap();
        let first = first_authority
            .activate(candidate('a', None), UtcMicros(1))
            .unwrap();
        let second = second_authority
            .activate(
                candidate('b', Some(first.receipt_digest.clone())),
                UtcMicros(2),
            )
            .unwrap();
        assert_eq!(second.sequence, 3);
        assert_eq!(first_authority.current_receipt(), Some(second.clone()));
        assert_eq!(
            first_authority.activate(candidate('c', Some(first.receipt_digest)), UtcMicros(3)),
            Err(SemanticActivationAuthorityErrorV1::Conflict)
        );
    }

    #[test]
    fn failed_second_append_is_rolled_back_and_marker_failure_is_corrupt() {
        let journal = MemorySemanticActivationJournalV1::default();
        let authority = SemanticActivationAuthorityV1::open(journal.clone()).unwrap();
        let first = authority
            .activate(candidate('a', None), UtcMicros(1))
            .unwrap();
        journal.fail_on_append(4);
        assert!(matches!(
            authority.activate(
                candidate('b', Some(first.receipt_digest.clone())),
                UtcMicros(2)
            ),
            Err(SemanticActivationAuthorityErrorV1::Journal(
                SemanticActivationJournalErrorV1::Unavailable
            ))
        ));
        assert_eq!(authority.current_receipt(), Some(first.clone()));
        assert_eq!(journal.snapshot().len(), 4);

        let journal = MemorySemanticActivationJournalV1::default();
        let authority = SemanticActivationAuthorityV1::open(journal.clone()).unwrap();
        let first = authority
            .activate(candidate('a', None), UtcMicros(1))
            .unwrap();
        journal.fail_on_append(5);
        assert!(matches!(
            authority.activate(candidate('b', Some(first.receipt_digest)), UtcMicros(2)),
            Err(SemanticActivationAuthorityErrorV1::JournalRecovery { .. })
        ));
        assert!(matches!(
            authority.status(),
            SemanticActivationAvailabilityV1::Unavailable {
                reason: SemanticActivationUnavailableReasonV1::Corrupt
            }
        ));
        assert!(matches!(
            SemanticActivationAuthorityV1::open(journal),
            Err(SemanticActivationAuthorityErrorV1::Unavailable {
                reason: SemanticActivationUnavailableReasonV1::Corrupt
            })
        ));
    }

    #[test]
    fn durable_journal_replays_across_instances() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("semantic-activation.journal.json");
        let first_journal = DurableSemanticActivationJournalV1::open(&path).unwrap();
        let second_journal = DurableSemanticActivationJournalV1::open(&path).unwrap();
        let first_authority = SemanticActivationAuthorityV1::open(first_journal).unwrap();
        let second_authority = SemanticActivationAuthorityV1::open(second_journal).unwrap();
        let first = first_authority
            .activate(candidate('a', None), UtcMicros(1))
            .unwrap();
        let second = second_authority
            .activate(
                candidate('b', Some(first.receipt_digest.clone())),
                UtcMicros(2),
            )
            .unwrap();
        let reopened = SemanticActivationAuthorityV1::open(
            DurableSemanticActivationJournalV1::open(path).unwrap(),
        )
        .unwrap();
        assert_eq!(reopened.current_receipt(), Some(second));
    }

    #[test]
    fn durable_journal_cas_allows_only_one_first_writer_across_threads() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory
            .path()
            .join("semantic-activation-race.journal.json");
        let first = SemanticActivationAuthorityV1::open(
            DurableSemanticActivationJournalV1::open(&path).unwrap(),
        )
        .unwrap();
        let second = SemanticActivationAuthorityV1::open(
            DurableSemanticActivationJournalV1::open(&path).unwrap(),
        )
        .unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let first_barrier = std::sync::Arc::clone(&barrier);
        let second_barrier = std::sync::Arc::clone(&barrier);
        let first_thread = std::thread::spawn(move || {
            first_barrier.wait();
            first.activate(candidate('a', None), UtcMicros(1))
        });
        let second_thread = std::thread::spawn(move || {
            second_barrier.wait();
            second.activate(candidate('b', None), UtcMicros(2))
        });
        let first_result = first_thread.join().unwrap();
        let second_result = second_thread.join().unwrap();
        assert_ne!(first_result.is_ok(), second_result.is_ok());
        let reopened = SemanticActivationAuthorityV1::open(
            DurableSemanticActivationJournalV1::open(path).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            reopened.status(),
            SemanticActivationAvailabilityV1::Ready { .. }
        ));
    }
}
