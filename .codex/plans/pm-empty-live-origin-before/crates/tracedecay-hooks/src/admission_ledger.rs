//! Durable, bounded Hook V2 admission idempotency ledger.
//!
//! This is deliberately *not* an event store. It persists only the identity of
//! an already-authorized envelope (`event_id`) plus a digest over the exact
//! canonical envelope bytes, so the daemon can answer three questions across a
//! restart:
//!
//! * has this exact envelope already been admitted? (`ExactDuplicate`)
//! * has this identity already been admitted carrying *different* bytes?
//!   (`Conflict`)
//! * otherwise: this is a first admission.
//!
//! It carries no payload, no session content, and no application state. The
//! daemon is the sole writer; the ledger is stored beside the transport spool
//! in the same daemon-owned hook data root and never touches a migrated
//! database.
//!
//! Bounds (stated, not implied): at most
//! [`HookAdmissionLedgerLimitsV1::max_records`] live entries per host and
//! nothing older than [`HookAdmissionLedgerLimitsV1::max_age_micros`]. Beyond
//! either bound the oldest entries are dropped, so idempotency converges within
//! that window and no further — a replay older than the window is admitted
//! again rather than silently believed to be new forever.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracedecay_domain::{
    BrainId, CommitId, EvidenceAvailabilityV1, ObservationSourceIdentityV1, RefId,
    RepositoryProvenanceV1, UserProfileId, UtcMicros, canonical_json_bytes,
    framed_log::checksum as frame_checksum,
};
use tracedecay_private_fs::framed_log::{
    DirectorySyncPolicy, append_durable, atomic_write as shared_atomic_write,
    read_bounded as shared_read_bounded, sync_directory as shared_sync_directory,
    truncate_file as shared_truncate_file, validate_regular_or_missing as shared_validate_regular,
};

use crate::{HookEventEnvelopeV2, HookHostV1, MAX_SPOOL_AGE_MICROS, MAX_SPOOL_RECORDS_PER_HOST};

const LEDGER_MAGIC: &[u8; 4] = b"TDL1";
const LEDGER_FORMAT_VERSION: u16 = 1;
const HEADER_BYTES: usize = 6;
const IDENTITY_BYTES: usize = 16;
const DIGEST_BYTES: usize = 32;
const CHECKSUM_PREFIX_BYTES: usize = 8;
const RECORD_BODY_BYTES: usize = IDENTITY_BYTES + DIGEST_BYTES + 8;
const RECORD_BYTES: usize = RECORD_BODY_BYTES + CHECKSUM_PREFIX_BYTES;
const RECORDS_FILE: &str = "admissions.v1.bin";
const COMPLETIONS_FILE: &str = "admission-work-completions.v1.json";
const LIVE_ORIGINS_FILE: &str = "admission-live-origins.json";
const MAX_LIVE_ORIGIN_BYTES: usize = 1024 * 1024;
const MAX_LIVE_ORIGIN_BOUNDARIES: usize = 64;
const MAX_LIVE_ORIGIN_PROOFS: usize = 64;
pub const MAX_LIVE_ORIGIN_FRAMES: usize = 256;
const LOCK_FILE: &str = "admissions.v1.lock";
const DIRECTORY_POLICY: DirectorySyncPolicy = DirectorySyncPolicy::Strict;

/// Checked-in ledger bounds. Callers may narrow these but never widen them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookAdmissionLedgerLimitsV1 {
    pub max_records: u32,
    pub max_age_micros: i64,
}

impl HookAdmissionLedgerLimitsV1 {
    pub const fn stock() -> Self {
        Self {
            max_records: MAX_SPOOL_RECORDS_PER_HOST,
            max_age_micros: MAX_SPOOL_AGE_MICROS,
        }
    }

    fn validate(self) -> Result<(), HookAdmissionLedgerError> {
        if self.max_records == 0
            || self.max_records > MAX_SPOOL_RECORDS_PER_HOST
            || self.max_age_micros <= 0
            || self.max_age_micros > MAX_SPOOL_AGE_MICROS
        {
            return Err(HookAdmissionLedgerError::InvalidLimits);
        }
        Ok(())
    }

    fn max_file_bytes(self) -> usize {
        HEADER_BYTES.saturating_add(self.max_records as usize * RECORD_BYTES * 2)
    }
}

/// What the ledger decided about one admission attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookAdmissionDecisionV1 {
    /// First durable admission for this identity inside the retained window.
    Admitted,
    /// The same identity already carries exactly these bytes.
    ExactDuplicate,
    /// The same identity already carries *different* bytes.
    Conflict,
}

/// Durable ledger decision plus the stable order assigned to its entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HookAdmissionLedgerReceiptV1 {
    pub decision: HookAdmissionDecisionV1,
    pub order: u64,
    pub work_completed: bool,
}

/// A live boundary remains authoritative only while its exact admission is
/// retained. The origin metadata never substitutes for the admission ledger.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookLiveOriginAdmissionV1 {
    pub event_id: [u8; 16],
    pub digest: [u8; 32],
    pub order: u64,
    pub admitted_at: UtcMicros,
    pub host: HookHostV1,
    pub protected_session_id: [u8; 32],
    pub project_id: [u8; 16],
    pub repository_id: [u8; 16],
    pub worktree_id: [u8; 16],
    pub worktree_epoch: u64,
}

/// Exact per-worktree HEAD reflog watermark. Equality of HEAD alone cannot
/// detect a checkout away and back between two live events.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookLiveOriginBranchEvidenceV1 {
    pub attached_ref: RefId,
    pub head_commit: CommitId,
    pub canonical_path: PathBuf,
    pub file_identity: [u64; 2],
    pub frontier: u64,
    pub fingerprint: [u8; 32],
    pub change_token: [i64; 4],
    pub head_file_identity: [u64; 2],
    pub head_change_token: [i64; 4],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookLiveOriginScopeV1 {
    pub brain_id: BrainId,
    pub profile_id: UserProfileId,
    pub repository: RepositoryProvenanceV1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookLiveOriginCheckpointV1 {
    pub generation: u64,
    pub file_identity: u64,
    pub complete_frontier: u64,
    pub complete_prefix_fingerprint: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookLiveOriginFrameV1 {
    pub start: u64,
    pub end: u64,
    pub resume_fingerprint: u64,
}

/// Content-free result of a bounded live read. `validated_checkpoint` is set
/// only when the shared source scanner verified that exact previous prefix.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookLiveOriginObservationV1 {
    pub scope: HookLiveOriginScopeV1,
    pub source: ObservationSourceIdentityV1,
    pub canonical_source_path: PathBuf,
    pub branch_evidence: HookLiveOriginBranchEvidenceV1,
    pub checkpoint: HookLiveOriginCheckpointV1,
    /// Actual extent seen at this live checkpoint. The initial exclusion
    /// floor lives on `HookLiveOriginBoundaryV1::start` and never advances
    /// across a continuously verified interval.
    pub physical_eof: u64,
    pub validated_checkpoint: Option<HookLiveOriginCheckpointV1>,
    pub frames: Vec<HookLiveOriginFrameV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookLiveOriginStartV1 {
    pub admission: HookLiveOriginAdmissionV1,
    pub checkpoint: HookLiveOriginCheckpointV1,
    pub physical_eof: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookLiveOriginBoundaryV1 {
    /// First live admission after a discontinuity, including its initial
    /// complete prefix and physical EOF. Only a real rebaseline replaces it.
    pub start: HookLiveOriginStartV1,
    /// Most recent continuously verified live checkpoint admission.
    pub admission: HookLiveOriginAdmissionV1,
    pub observation: HookLiveOriginObservationV1,
}

/// One interval sealed by a later live event. Frames before `baseline.start`'s
/// physical EOF, including extensions of an old partial frame, are absent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookLiveOriginProofV1 {
    pub proof_ref: String,
    pub baseline: HookLiveOriginBoundaryV1,
    pub seal: HookLiveOriginAdmissionV1,
    pub frames: Vec<HookLiveOriginFrameV1>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveOriginMetadata {
    baselines: Vec<HookLiveOriginBoundaryV1>,
    proofs: Vec<HookLiveOriginProofV1>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HookLiveOriginOutcomeV1 {
    Baseline,
    Checkpoint,
    Sealed,
    Unavailable,
    Duplicate,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookAdmissionLedgerError {
    #[error("hook admission ledger filesystem operation failed")]
    Io,
    #[error("hook admission ledger root or member path is unsafe")]
    UnsafePath,
    #[error("hook admission ledger limits are invalid")]
    InvalidLimits,
    #[error("hook admission ledger record is not canonically encodable")]
    RecordUnencodable,
    #[error("hook admission ledger record is not canonically decodable")]
    RecordUndecodable,
    #[error("hook admission ledger identity is invalid")]
    InvalidIdentity,
    #[error("hook admission ledger is busy in another daemon")]
    Busy,
}

/// Bounded recovery report for an opened ledger.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookAdmissionLedgerOpenReportV1 {
    pub live_records: u32,
    pub dropped_expired_records: u32,
    pub dropped_overflow_records: u32,
    pub truncated_tail_bytes: u64,
    pub live_origin_metadata_error: Option<HookAdmissionLedgerError>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LedgerEntry {
    digest: [u8; DIGEST_BYTES],
    admitted_at: UtcMicros,
    order: u64,
}

/// Digest over the exact canonical envelope bytes. Two envelopes with the same
/// `event_id` and different digests are a genuine producer conflict.
pub fn hook_admission_digest(
    envelope: &HookEventEnvelopeV2,
) -> Result<[u8; DIGEST_BYTES], HookAdmissionLedgerError> {
    let bytes =
        canonical_json_bytes(envelope).map_err(|_| HookAdmissionLedgerError::RecordUnencodable)?;
    Ok(frame_checksum(&bytes))
}

/// The daemon-owned, per-host admission ledger.
#[derive(Debug)]
pub struct HookAdmissionLedgerV1 {
    root: PathBuf,
    _writer_lock: fs::File,
    host: HookHostV1,
    limits: HookAdmissionLedgerLimitsV1,
    entries: BTreeMap<[u8; IDENTITY_BYTES], LedgerEntry>,
    completed_work: BTreeSet<[u8; IDENTITY_BYTES]>,
    live_origins: LiveOriginMetadata,
    next_order: u64,
}

impl Drop for HookAdmissionLedgerV1 {
    fn drop(&mut self) {
        let _ = self._writer_lock.unlock();
    }
}

impl HookAdmissionLedgerV1 {
    /// Open (and bounded-recover) the ledger for one host.
    #[hotpath::measure(label = "hooks.admission.open")]
    pub fn open(
        root: impl Into<PathBuf>,
        host: HookHostV1,
        limits: HookAdmissionLedgerLimitsV1,
        now: UtcMicros,
    ) -> Result<(Self, HookAdmissionLedgerOpenReportV1), HookAdmissionLedgerError> {
        limits.validate()?;
        let root = root.into();
        ensure_root(&root)?;
        let writer_lock = acquire_writer_lock(&root)?;
        let path = records_path(&root);
        ensure_header(&path)?;
        let bytes = read_bounded(&path, limits.max_file_bytes())?.unwrap_or_default();
        let (scanned, truncated_tail_bytes) = scan_records(&bytes);
        let completions_existed = completions_path(&root).is_file();
        // Optional origin evidence cannot make canonical event admission
        // unavailable. An unreadable origin file starts with no baseline;
        // the report preserves the failure and later live capture rebaselines.
        let (live_origins, live_origin_metadata_error) = match read_live_origin_metadata(&root) {
            Ok(origins) => (origins, None),
            Err(error) => (LiveOriginMetadata::default(), Some(error)),
        };
        let mut ledger = Self {
            root,
            _writer_lock: writer_lock,
            host,
            limits,
            entries: BTreeMap::new(),
            completed_work: BTreeSet::new(),
            live_origins,
            next_order: 0,
        };
        let mut dropped_expired_records = 0u32;
        for (identity, digest, admitted_at) in scanned {
            if is_expired(admitted_at, now, limits.max_age_micros) {
                dropped_expired_records = dropped_expired_records.saturating_add(1);
                // A later record for the same identity supersedes the earlier
                // one, so an expired duplicate must also evict the live entry.
                ledger.entries.remove(&identity);
                continue;
            }
            let order = ledger.next_order;
            ledger.next_order = ledger.next_order.saturating_add(1);
            ledger.entries.insert(
                identity,
                LedgerEntry {
                    digest,
                    admitted_at,
                    order,
                },
            );
        }
        ledger.completed_work = if completions_existed {
            read_work_completions(&ledger.root, limits.max_records)?
                .into_iter()
                .filter(|identity| ledger.entries.contains_key(identity))
                .collect()
        } else {
            // Records written before the durable producer-work outbox existed
            // were already treated as complete. Preserve that upgrade
            // invariant instead of redriving historical admissions.
            ledger.entries.keys().copied().collect()
        };
        let dropped_overflow_records = ledger.trim_to(limits.max_records as usize);
        if truncated_tail_bytes > 0 {
            shared_truncate_file(
                &path,
                (bytes.len() as u64).saturating_sub(truncated_tail_bytes),
                DIRECTORY_POLICY,
            )
            .map_err(|_| HookAdmissionLedgerError::Io)?;
        }
        if dropped_expired_records > 0
            || dropped_overflow_records > 0
            || ledger.entries.len() < ledger.next_order as usize
        {
            ledger.rewrite()?;
        } else if !completions_existed {
            ledger.write_work_completions()?;
        }
        let report = HookAdmissionLedgerOpenReportV1 {
            live_records: ledger.entries.len() as u32,
            dropped_expired_records,
            dropped_overflow_records,
            truncated_tail_bytes,
            live_origin_metadata_error,
        };
        hotpath::gauge!("hooks.admission.live_records").set(report.live_records);
        hotpath::gauge!("hooks.admission.open.dropped_expired").set(report.dropped_expired_records);
        hotpath::gauge!("hooks.admission.open.dropped_overflow")
            .set(report.dropped_overflow_records);
        Ok((ledger, report))
    }

    pub fn host(&self) -> HookHostV1 {
        self.host
    }

    pub fn live_records(&self) -> u32 {
        self.entries.len() as u32
    }

    pub fn live_origin_baseline(
        &self,
        protected_session_id: [u8; 32],
        now: UtcMicros,
    ) -> Option<HookLiveOriginBoundaryV1> {
        self.live_origins
            .baselines
            .iter()
            .find(|boundary| {
                boundary.admission.protected_session_id == protected_session_id
                    && self.origin_admission_retained(&boundary.start.admission, now)
                    && self.origin_admission_retained(&boundary.admission, now)
            })
            .cloned()
    }

    fn origin_admission_retained(
        &self,
        admission: &HookLiveOriginAdmissionV1,
        now: UtcMicros,
    ) -> bool {
        admission.host == self.host
            && self.entries.get(&admission.event_id).is_some_and(|entry| {
                entry.digest == admission.digest
                    && entry.order == admission.order
                    && entry.admitted_at == admission.admitted_at
                    && admission.admitted_at.0 <= now.0
                    && !is_expired(entry.admitted_at, now, self.limits.max_age_micros)
            })
    }

    /// Called only by live admission, after binding validation and durable
    /// canonical admission. Replay and ordinary transcript ingestion cannot
    /// establish a baseline. Missing evidence drops the open interval.
    pub fn record_live_origin(
        &mut self,
        envelope: &HookEventEnvelopeV2,
        receipt: HookAdmissionLedgerReceiptV1,
        observation: Option<HookLiveOriginObservationV1>,
        now: UtcMicros,
    ) -> Result<HookLiveOriginOutcomeV1, HookAdmissionLedgerError> {
        if receipt.decision != HookAdmissionDecisionV1::Admitted {
            return Ok(HookLiveOriginOutcomeV1::Duplicate);
        }
        let entry = self
            .entries
            .get(&envelope.event_id)
            .ok_or(HookAdmissionLedgerError::InvalidIdentity)?;
        let admission = HookLiveOriginAdmissionV1 {
            event_id: envelope.event_id,
            digest: hook_admission_digest(envelope)?,
            order: receipt.order,
            admitted_at: entry.admitted_at,
            host: envelope.producer,
            protected_session_id: envelope.protected_session_id,
            project_id: envelope.project_id,
            repository_id: envelope.repository_id,
            worktree_id: envelope.worktree_id,
            worktree_epoch: envelope.worktree_epoch,
        };
        if !self.origin_admission_retained(&admission, now) {
            return Err(HookAdmissionLedgerError::InvalidIdentity);
        }
        let previous = self.live_origin_baseline(envelope.protected_session_id, now);
        // A concurrently completed older live read cannot replace a newer
        // baseline, nor can retrying the same admitted receipt seal new bytes.
        if previous
            .as_ref()
            .is_some_and(|old| old.admission.order >= admission.order)
        {
            return Ok(HookLiveOriginOutcomeV1::Duplicate);
        }
        let mut next = self.live_origins.clone();
        next.baselines.retain(|boundary| {
            boundary.admission.protected_session_id != envelope.protected_session_id
                && self.origin_admission_retained(&boundary.start.admission, now)
                && self.origin_admission_retained(&boundary.admission, now)
        });
        next.proofs.retain(|proof| {
            self.origin_admission_retained(&proof.baseline.admission, now)
                && self.origin_admission_retained(&proof.baseline.start.admission, now)
                && self.origin_admission_retained(&proof.seal, now)
        });
        let mut outcome = HookLiveOriginOutcomeV1::Unavailable;
        if let Some(mut observation) = observation.filter(valid_origin_observation) {
            let continuous = previous
                .as_ref()
                .filter(|baseline| continuous_origin_interval(baseline, &admission, &observation))
                .cloned();
            let start = continuous.as_ref().map_or_else(
                || HookLiveOriginStartV1 {
                    admission: admission.clone(),
                    checkpoint: observation.checkpoint,
                    physical_eof: observation.physical_eof,
                },
                |baseline| baseline.start.clone(),
            );
            if let Some(baseline) = continuous {
                outcome = HookLiveOriginOutcomeV1::Checkpoint;
                // Freeze the original repository capture while source
                // checkpoints advance within that same proved exact scope.
                observation.scope = baseline.observation.scope.clone();
                let frames = observation
                    .frames
                    .iter()
                    .copied()
                    .filter(|frame| frame.start >= baseline.start.physical_eof)
                    .collect::<Vec<_>>();
                if !frames.is_empty() {
                    let proof_ref = live_origin_proof_ref(&baseline, &admission, &frames)?;
                    next.proofs.push(HookLiveOriginProofV1 {
                        proof_ref,
                        baseline,
                        seal: admission.clone(),
                        frames,
                    });
                    outcome = HookLiveOriginOutcomeV1::Sealed;
                }
            }
            // A boundary needs only its checkpoint. Retained frame receipts
            // live on the sealed interval and never contain transcript text.
            observation.frames.clear();
            observation.validated_checkpoint = None;
            next.baselines.push(HookLiveOriginBoundaryV1 {
                start,
                admission,
                observation,
            });
            if outcome == HookLiveOriginOutcomeV1::Unavailable {
                outcome = HookLiveOriginOutcomeV1::Baseline;
            }
        }
        if next.baselines.len() > MAX_LIVE_ORIGIN_BOUNDARIES {
            next.baselines
                .drain(..next.baselines.len() - MAX_LIVE_ORIGIN_BOUNDARIES);
        }
        if next.proofs.len() > MAX_LIVE_ORIGIN_PROOFS {
            next.proofs
                .drain(..next.proofs.len() - MAX_LIVE_ORIGIN_PROOFS);
        }
        write_live_origin_metadata(&self.root, &mut next)?;
        self.live_origins = next;
        Ok(outcome)
    }

    /// Record one admission attempt. `Admitted` is returned only after the
    /// identity is durably on disk, so a crash immediately afterwards still
    /// converges on replay.
    pub fn admit(
        &mut self,
        envelope: &HookEventEnvelopeV2,
        now: UtcMicros,
    ) -> Result<HookAdmissionDecisionV1, HookAdmissionLedgerError> {
        self.admit_with_receipt(envelope, now)
            .map(|receipt| receipt.decision)
    }

    /// Records one attempt and exposes the durable entry order. Exact
    /// duplicates reuse the original order, including after ledger reopen.
    #[hotpath::measure(label = "hooks.admission.admit")]
    pub fn admit_with_receipt(
        &mut self,
        envelope: &HookEventEnvelopeV2,
        now: UtcMicros,
    ) -> Result<HookAdmissionLedgerReceiptV1, HookAdmissionLedgerError> {
        let identity = envelope.event_id;
        if identity == [0; IDENTITY_BYTES] {
            return Err(HookAdmissionLedgerError::InvalidIdentity);
        }
        let digest = hook_admission_digest(envelope)?;
        if let Some(existing) = self.entries.get(&identity) {
            if is_expired(existing.admitted_at, now, self.limits.max_age_micros) {
                self.entries.remove(&identity);
            } else if existing.digest == digest {
                hotpath::gauge!("hooks.admission.decision.exact_duplicate").inc(1);
                return Ok(HookAdmissionLedgerReceiptV1 {
                    decision: HookAdmissionDecisionV1::ExactDuplicate,
                    order: existing.order,
                    work_completed: self.completed_work.contains(&identity),
                });
            } else {
                hotpath::gauge!("hooks.admission.decision.conflict").inc(1);
                return Ok(HookAdmissionLedgerReceiptV1 {
                    decision: HookAdmissionDecisionV1::Conflict,
                    order: existing.order,
                    work_completed: self.completed_work.contains(&identity),
                });
            }
        }
        if self.entries.len() as u32 >= self.limits.max_records {
            self.trim_to((self.limits.max_records as usize).saturating_sub(1));
            self.rewrite()?;
        }
        let record = encode_record(identity, digest, now);
        hotpath::gauge!("hooks.admission.append.bytes").set(record.len());
        hotpath::measure_block!("hooks.admission.fsync.append", {
            append_durable(&records_path(&self.root), &record, DIRECTORY_POLICY)
                .map_err(|_| HookAdmissionLedgerError::Io)
        })?;
        let order = self.next_order;
        self.next_order = self.next_order.saturating_add(1);
        self.entries.insert(
            identity,
            LedgerEntry {
                digest,
                admitted_at: now,
                order,
            },
        );
        hotpath::gauge!("hooks.admission.decision.admitted").inc(1);
        Ok(HookAdmissionLedgerReceiptV1 {
            decision: HookAdmissionDecisionV1::Admitted,
            order,
            work_completed: false,
        })
    }

    /// Mark producer work complete only after the admitted worker returns.
    /// Exact duplicate redrives remain pending until this fsync succeeds.
    pub fn mark_work_completed(
        &mut self,
        envelope: &HookEventEnvelopeV2,
    ) -> Result<bool, HookAdmissionLedgerError> {
        let identity = envelope.event_id;
        let Some(entry) = self.entries.get(&identity) else {
            return Err(HookAdmissionLedgerError::InvalidIdentity);
        };
        if entry.digest != hook_admission_digest(envelope)? {
            return Err(HookAdmissionLedgerError::InvalidIdentity);
        }
        if !self.completed_work.insert(identity) {
            return Ok(false);
        }
        match self.write_work_completions() {
            Ok(()) => Ok(true),
            Err(error) => {
                self.completed_work.remove(&identity);
                Err(error)
            }
        }
    }

    /// Drop entries older than the age bound. Returns how many were removed.
    #[hotpath::measure(label = "hooks.admission.expire")]
    pub fn expire(&mut self, now: UtcMicros) -> Result<u32, HookAdmissionLedgerError> {
        let before = self.entries.len();
        let max_age = self.limits.max_age_micros;
        self.entries
            .retain(|_, entry| !is_expired(entry.admitted_at, now, max_age));
        self.completed_work
            .retain(|identity| self.entries.contains_key(identity));
        let removed = before.saturating_sub(self.entries.len()) as u32;
        if removed > 0 {
            self.rewrite()?;
        }
        hotpath::gauge!("hooks.admission.expired.removed").inc(removed);
        hotpath::gauge!("hooks.admission.live_records").set(self.entries.len());
        Ok(removed)
    }

    /// Drop the oldest entries until at most `allowed` remain. Compaction
    /// overshoots down to three quarters of the checked-in bound so the
    /// rewrite is amortized instead of firing on every later admission.
    fn trim_to(&mut self, allowed: usize) -> u32 {
        if self.entries.len() <= allowed {
            return 0;
        }
        let retained = allowed.min((self.limits.max_records as usize).saturating_mul(3) / 4);
        let mut ordered = self
            .entries
            .iter()
            .map(|(identity, entry)| (entry.order, *identity))
            .collect::<Vec<_>>();
        ordered.sort_unstable();
        let dropped = ordered.len().saturating_sub(retained);
        for (_, identity) in ordered.into_iter().take(dropped) {
            self.entries.remove(&identity);
            self.completed_work.remove(&identity);
        }
        dropped as u32
    }

    #[hotpath::measure(label = "hooks.admission.rewrite")]
    fn rewrite(&mut self) -> Result<(), HookAdmissionLedgerError> {
        let mut ordered = self
            .entries
            .iter()
            .map(|(identity, entry)| (entry.order, *identity, *entry))
            .collect::<Vec<_>>();
        ordered.sort_unstable_by_key(|(order, _, _)| *order);
        let mut bytes = Vec::with_capacity(HEADER_BYTES + ordered.len() * RECORD_BYTES);
        bytes.extend_from_slice(LEDGER_MAGIC);
        bytes.extend_from_slice(&LEDGER_FORMAT_VERSION.to_le_bytes());
        self.next_order = 0;
        for (_, identity, entry) in &ordered {
            bytes.extend_from_slice(&encode_record(*identity, entry.digest, entry.admitted_at));
        }
        for (index, (_, identity, _)) in ordered.iter().enumerate() {
            if let Some(entry) = self.entries.get_mut(identity) {
                entry.order = index as u64;
            }
        }
        self.next_order = ordered.len() as u64;
        hotpath::gauge!("hooks.admission.rewrite.bytes").set(bytes.len());
        hotpath::measure_block!("hooks.admission.fsync.rewrite", {
            shared_atomic_write(
                &records_path(&self.root),
                "hook-admissions",
                &bytes,
                DIRECTORY_POLICY,
            )
            .map_err(|_| HookAdmissionLedgerError::Io)
        })?;
        self.write_work_completions()
    }

    #[hotpath::measure(label = "hooks.admission.write_completions")]
    fn write_work_completions(&self) -> Result<(), HookAdmissionLedgerError> {
        let bytes = canonical_json_bytes(&self.completed_work.iter().copied().collect::<Vec<_>>())
            .map_err(|_| HookAdmissionLedgerError::RecordUnencodable)?;
        hotpath::gauge!("hooks.admission.completions.bytes").set(bytes.len());
        hotpath::measure_block!("hooks.admission.fsync.completions", {
            shared_atomic_write(
                &completions_path(&self.root),
                "hook-admission-work-completions",
                &bytes,
                DIRECTORY_POLICY,
            )
            .map_err(|_| HookAdmissionLedgerError::Io)
        })
    }
}

fn valid_origin_observation(value: &HookLiveOriginObservationV1) -> bool {
    let repository = &value.scope.repository;
    value.source.validate().is_ok()
        && repository.validate().is_ok()
        && repository.project_id().is_some()
        && repository.worktree_id().is_some()
        && matches!(
            repository.evidence().attached_ref(),
            EvidenceAvailabilityV1::Known(reference) if reference == &value.branch_evidence.attached_ref
        )
        && matches!(
            repository.evidence().head_commit(),
            EvidenceAvailabilityV1::Known(commit) if commit == &value.branch_evidence.head_commit
        )
        && value.canonical_source_path.is_absolute()
        && value.canonical_source_path.as_os_str().len() <= 4096
        && value.branch_evidence.canonical_path.is_absolute()
        && value.branch_evidence.canonical_path.as_os_str().len() <= 4096
        && value.branch_evidence.frontier > 0
        && value.checkpoint.generation != 0
        && value.checkpoint.file_identity != 0
        && value.checkpoint.complete_frontier <= value.physical_eof
        && value.frames.len() <= MAX_LIVE_ORIGIN_FRAMES
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

fn same_origin_scope(left: &HookLiveOriginScopeV1, right: &HookLiveOriginScopeV1) -> bool {
    left.brain_id == right.brain_id
        && left.profile_id == right.profile_id
        && left.repository.project_id() == right.repository.project_id()
        && left.repository.repository_id() == right.repository.repository_id()
        && left.repository.worktree_id() == right.repository.worktree_id()
        && left.repository.canonical_root_digest() == right.repository.canonical_root_digest()
        && left.repository.evidence().attached_ref() == right.repository.evidence().attached_ref()
        && left.repository.evidence().head_commit() == right.repository.evidence().head_commit()
}

fn continuous_origin_interval(
    baseline: &HookLiveOriginBoundaryV1,
    seal: &HookLiveOriginAdmissionV1,
    next: &HookLiveOriginObservationV1,
) -> bool {
    let previous = &baseline.observation;
    same_live_origin_authority(&baseline.admission, seal)
        && baseline.admission.order < seal.order
        && baseline.admission.admitted_at.0 <= seal.admitted_at.0
        && same_origin_scope(&previous.scope, &next.scope)
        && previous.source == next.source
        && previous.canonical_source_path == next.canonical_source_path
        && previous.branch_evidence == next.branch_evidence
        && next.validated_checkpoint == Some(previous.checkpoint)
        && previous.checkpoint.generation == next.checkpoint.generation
        && previous.checkpoint.file_identity == next.checkpoint.file_identity
        && previous.physical_eof <= next.physical_eof
        && next.frames.first().map_or_else(
            || next.checkpoint == previous.checkpoint,
            |first| first.start == previous.checkpoint.complete_frontier,
        )
}

fn same_live_origin_authority(
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

fn valid_origin_boundary(boundary: &HookLiveOriginBoundaryV1) -> bool {
    valid_origin_observation(&boundary.observation)
        && same_live_origin_authority(&boundary.start.admission, &boundary.admission)
        && boundary.start.admission.order <= boundary.admission.order
        && boundary.start.admission.admitted_at.0 <= boundary.admission.admitted_at.0
        && boundary.start.physical_eof <= boundary.observation.physical_eof
        && boundary.start.checkpoint.complete_frontier <= boundary.start.physical_eof
        && boundary.start.checkpoint.complete_frontier
            <= boundary.observation.checkpoint.complete_frontier
        && boundary.start.checkpoint.file_identity == boundary.observation.checkpoint.file_identity
        && boundary.start.checkpoint.generation == boundary.observation.checkpoint.generation
}

fn live_origin_proof_ref(
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

fn read_live_origin_metadata(root: &Path) -> Result<LiveOriginMetadata, HookAdmissionLedgerError> {
    let Some(bytes) = read_bounded(&root.join(LIVE_ORIGINS_FILE), MAX_LIVE_ORIGIN_BYTES)? else {
        return Ok(LiveOriginMetadata::default());
    };
    let metadata: LiveOriginMetadata =
        serde_json::from_slice(&bytes).map_err(|_| HookAdmissionLedgerError::RecordUndecodable)?;
    if metadata.baselines.len() > MAX_LIVE_ORIGIN_BOUNDARIES
        || metadata.proofs.len() > MAX_LIVE_ORIGIN_PROOFS
        || metadata
            .baselines
            .iter()
            .any(|boundary| !valid_origin_boundary(boundary))
        || metadata.proofs.iter().any(|proof| {
            !valid_origin_boundary(&proof.baseline)
                || !same_live_origin_authority(&proof.baseline.admission, &proof.seal)
                || proof.baseline.admission.order >= proof.seal.order
                || proof.frames.is_empty()
                || proof.frames.len() > MAX_LIVE_ORIGIN_FRAMES
                || proof.frames.iter().any(|frame| {
                    frame.start < proof.baseline.start.physical_eof || frame.start >= frame.end
                })
                || proof
                    .frames
                    .windows(2)
                    .any(|pair| pair[0].end != pair[1].start)
                || !live_origin_proof_ref(&proof.baseline, &proof.seal, &proof.frames)
                    .is_ok_and(|expected| expected == proof.proof_ref)
        })
    {
        return Err(HookAdmissionLedgerError::RecordUndecodable);
    }
    Ok(metadata)
}

fn write_live_origin_metadata(
    root: &Path,
    metadata: &mut LiveOriginMetadata,
) -> Result<(), HookAdmissionLedgerError> {
    let bytes = loop {
        let bytes = canonical_json_bytes(metadata)
            .map_err(|_| HookAdmissionLedgerError::RecordUnencodable)?;
        if bytes.len() <= MAX_LIVE_ORIGIN_BYTES {
            break bytes;
        }
        if !metadata.proofs.is_empty() {
            metadata.proofs.remove(0);
        } else if metadata.baselines.len() > 1 {
            metadata.baselines.remove(0);
        } else {
            return Err(HookAdmissionLedgerError::InvalidLimits);
        }
    };
    shared_atomic_write(
        &root.join(LIVE_ORIGINS_FILE),
        "hook-admission-live-origins",
        &bytes,
        DIRECTORY_POLICY,
    )
    .map_err(|_| HookAdmissionLedgerError::Io)
}

/// Read-only bounded proof lookup while the daemon retains its writer lock.
/// Missing or pruned admissions cannot be reconstructed from origin metadata.
pub fn read_hook_live_origin_proofs(
    root: &Path,
    host: HookHostV1,
    now: UtcMicros,
) -> Result<Vec<HookLiveOriginProofV1>, HookAdmissionLedgerError> {
    read_validated_live_origin_metadata(root, host, now).map(|metadata| metadata.proofs)
}

/// A live destination may have established its baseline before it has emitted
/// any complete new source frames. Its exact retained receipt still matters.
pub fn read_hook_live_origin_boundaries(
    root: &Path,
    host: HookHostV1,
    now: UtcMicros,
) -> Result<Vec<HookLiveOriginBoundaryV1>, HookAdmissionLedgerError> {
    read_validated_live_origin_metadata(root, host, now).map(|metadata| metadata.baselines)
}

fn read_validated_live_origin_metadata(
    root: &Path,
    host: HookHostV1,
    now: UtcMicros,
) -> Result<LiveOriginMetadata, HookAdmissionLedgerError> {
    let mut metadata = read_live_origin_metadata(root)?;
    let limits = HookAdmissionLedgerLimitsV1::stock();
    let Some(bytes) = read_bounded(&records_path(root), limits.max_file_bytes())? else {
        return Ok(LiveOriginMetadata::default());
    };
    let (records, _) = scan_records(&bytes);
    let retained = |receipt: &HookLiveOriginAdmissionV1| {
        receipt.host == host
            && receipt.admitted_at.0 <= now.0
            && !is_expired(receipt.admitted_at, now, limits.max_age_micros)
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

fn completions_path(root: &Path) -> PathBuf {
    root.join(COMPLETIONS_FILE)
}

fn lock_path(root: &Path) -> PathBuf {
    root.join(LOCK_FILE)
}

fn acquire_writer_lock(root: &Path) -> Result<fs::File, HookAdmissionLedgerError> {
    let path = lock_path(root);
    shared_validate_regular(&path).map_err(|_| HookAdmissionLedgerError::UnsafePath)?;
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|_| HookAdmissionLedgerError::Io)?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => {
            hotpath::gauge!("hooks.admission.lock.contended").inc(1);
            Err(HookAdmissionLedgerError::Busy)
        }
        Err(std::fs::TryLockError::Error(_)) => Err(HookAdmissionLedgerError::Io),
    }
}

fn read_work_completions(
    root: &Path,
    max_records: u32,
) -> Result<Vec<[u8; IDENTITY_BYTES]>, HookAdmissionLedgerError> {
    let maximum = (max_records as usize)
        .saturating_mul(IDENTITY_BYTES.saturating_mul(4).saturating_add(8))
        .saturating_add(2);
    let Some(bytes) = read_bounded(&completions_path(root), maximum)? else {
        return Ok(Vec::new());
    };
    serde_json::from_slice(&bytes).map_err(|_| HookAdmissionLedgerError::RecordUndecodable)
}

fn is_expired(admitted_at: UtcMicros, now: UtcMicros, max_age_micros: i64) -> bool {
    now.0.saturating_sub(admitted_at.0) > max_age_micros
}

fn encode_record(
    identity: [u8; IDENTITY_BYTES],
    digest: [u8; DIGEST_BYTES],
    admitted_at: UtcMicros,
) -> [u8; RECORD_BYTES] {
    let mut record = [0u8; RECORD_BYTES];
    record[..IDENTITY_BYTES].copy_from_slice(&identity);
    record[IDENTITY_BYTES..IDENTITY_BYTES + DIGEST_BYTES].copy_from_slice(&digest);
    record[IDENTITY_BYTES + DIGEST_BYTES..RECORD_BODY_BYTES]
        .copy_from_slice(&admitted_at.0.to_le_bytes());
    let checksum = frame_checksum(&record[..RECORD_BODY_BYTES]);
    record[RECORD_BODY_BYTES..].copy_from_slice(&checksum[..CHECKSUM_PREFIX_BYTES]);
    record
}

type ScannedRecord = ([u8; IDENTITY_BYTES], [u8; DIGEST_BYTES], UtcMicros);

/// Scan the on-disk ledger. Returns every intact record in file order plus the
/// number of trailing bytes that are a partial or corrupt tail.
fn scan_records(bytes: &[u8]) -> (Vec<ScannedRecord>, u64) {
    if bytes.len() < HEADER_BYTES
        || &bytes[..4] != LEDGER_MAGIC
        || u16::from_le_bytes([bytes[4], bytes[5]]) != LEDGER_FORMAT_VERSION
    {
        return (Vec::new(), bytes.len() as u64);
    }
    let mut records = Vec::new();
    let mut offset = HEADER_BYTES;
    while offset + RECORD_BYTES <= bytes.len() {
        let record = &bytes[offset..offset + RECORD_BYTES];
        let checksum = frame_checksum(&record[..RECORD_BODY_BYTES]);
        if checksum[..CHECKSUM_PREFIX_BYTES] != record[RECORD_BODY_BYTES..] {
            break;
        }
        let mut identity = [0u8; IDENTITY_BYTES];
        identity.copy_from_slice(&record[..IDENTITY_BYTES]);
        let mut digest = [0u8; DIGEST_BYTES];
        digest.copy_from_slice(&record[IDENTITY_BYTES..IDENTITY_BYTES + DIGEST_BYTES]);
        let mut admitted = [0u8; 8];
        admitted.copy_from_slice(&record[IDENTITY_BYTES + DIGEST_BYTES..RECORD_BODY_BYTES]);
        records.push((identity, digest, UtcMicros(i64::from_le_bytes(admitted))));
        offset += RECORD_BYTES;
    }
    (records, (bytes.len() - offset) as u64)
}

fn records_path(root: &Path) -> PathBuf {
    root.join(RECORDS_FILE)
}

fn ledger_header() -> [u8; HEADER_BYTES] {
    let mut header = [0u8; HEADER_BYTES];
    header[..4].copy_from_slice(LEDGER_MAGIC);
    header[4..].copy_from_slice(&LEDGER_FORMAT_VERSION.to_le_bytes());
    header
}

/// Appends only ever add fixed-width records, so the header must exist before
/// the first admission or the whole file would scan as foreign bytes.
fn ensure_header(path: &Path) -> Result<(), HookAdmissionLedgerError> {
    shared_validate_regular(path).map_err(|_| HookAdmissionLedgerError::UnsafePath)?;
    let length = match fs::metadata(path) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => 0,
        Err(_) => return Err(HookAdmissionLedgerError::Io),
    };
    if length >= HEADER_BYTES as u64 {
        return Ok(());
    }
    shared_atomic_write(path, "hook-admissions", &ledger_header(), DIRECTORY_POLICY)
        .map_err(|_| HookAdmissionLedgerError::Io)
}

fn ensure_root(root: &Path) -> Result<(), HookAdmissionLedgerError> {
    match fs::symlink_metadata(root) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(HookAdmissionLedgerError::UnsafePath);
        }
        Ok(_) => return Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(_) => return Err(HookAdmissionLedgerError::Io),
    }
    fs::create_dir_all(root).map_err(|_| HookAdmissionLedgerError::Io)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(root, fs::Permissions::from_mode(0o700))
            .map_err(|_| HookAdmissionLedgerError::Io)?;
    }
    shared_sync_directory(root, DIRECTORY_POLICY).map_err(|_| HookAdmissionLedgerError::Io)
}

fn read_bounded(path: &Path, maximum: usize) -> Result<Option<Vec<u8>>, HookAdmissionLedgerError> {
    shared_validate_regular(path).map_err(|_| HookAdmissionLedgerError::UnsafePath)?;
    match shared_read_bounded(path, maximum) {
        Ok(bytes) => Ok(bytes),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) if error.kind() == io::ErrorKind::InvalidInput => {
            Err(HookAdmissionLedgerError::UnsafePath)
        }
        Err(_) => Err(HookAdmissionLedgerError::Io),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HOOK_EVENT_SCHEMA_VERSION, HookBoundaryV1, HookEventV2, HookOrderingV1};
    use std::process::Command;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct TestDir(PathBuf);

    impl TestDir {
        fn new(label: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(1);
            let path = std::env::temp_dir().join(format!(
                "tracedecay-hook-admissions-{label}-{}-{}",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn envelope(event_id: u8, epoch: u64) -> HookEventEnvelopeV2 {
        HookEventEnvelopeV2 {
            schema_version: HOOK_EVENT_SCHEMA_VERSION,
            event_id: [event_id; 16],
            producer: HookHostV1::ClaudeCode,
            protected_session_id: [7; 32],
            project_id: [1; 16],
            repository_id: [2; 16],
            worktree_id: [3; 16],
            worktree_epoch: epoch,
            binding_token: [4; 32],
            ordering: HookOrderingV1::Unknown,
            observed_at: UtcMicros(11),
            event: HookEventV2::SessionBoundary {
                boundary: HookBoundaryV1::TurnComplete,
            },
        }
    }

    fn open(root: &Path, now: UtcMicros) -> HookAdmissionLedgerV1 {
        HookAdmissionLedgerV1::open(
            root,
            HookHostV1::ClaudeCode,
            HookAdmissionLedgerLimitsV1::stock(),
            now,
        )
        .unwrap()
        .0
    }

    fn origin_observation(frontier: u64, eof: u64) -> HookLiveOriginObservationV1 {
        use tracedecay_domain::{
            CommitId, PrivacyDomainBoundLocatorDigest, ProjectId, ProviderId, RefId,
            RepositoryEvidenceV1, RepositoryId, RepositoryRemoteIdentityV1, SessionId, WorktreeId,
        };
        let evidence = RepositoryEvidenceV1::new(
            EvidenceAvailabilityV1::Known(RefId::new("refs/heads/main").unwrap()),
            EvidenceAvailabilityV1::Known(CommitId::new("a".repeat(40)).unwrap()),
            EvidenceAvailabilityV1::Unknown,
            EvidenceAvailabilityV1::Unknown,
            RepositoryRemoteIdentityV1::Unknown,
            EvidenceAvailabilityV1::Unknown,
        )
        .unwrap();
        HookLiveOriginObservationV1 {
            scope: HookLiveOriginScopeV1 {
                brain_id: BrainId::new("brain.fixture").unwrap(),
                profile_id: UserProfileId::new("profile.fixture").unwrap(),
                repository: RepositoryProvenanceV1::new(
                    RepositoryId::new("repository.fixture").unwrap(),
                    Some(ProjectId::new("project.fixture").unwrap()),
                    Some(WorktreeId::new("worktree.fixture").unwrap()),
                    PrivacyDomainBoundLocatorDigest::new(format!("sha256:{}", "b".repeat(64)))
                        .unwrap(),
                    evidence,
                    UtcMicros(1),
                )
                .unwrap(),
            },
            source: ObservationSourceIdentityV1::for_provider_source(
                ProviderId::new("claude").unwrap(),
                SessionId::new("session.fixture").unwrap(),
                SessionId::new("source.fixture").unwrap(),
            )
            .unwrap(),
            canonical_source_path: std::env::temp_dir().join("origin-session.fixture.jsonl"),
            branch_evidence: HookLiveOriginBranchEvidenceV1 {
                attached_ref: RefId::new("refs/heads/main").unwrap(),
                head_commit: CommitId::new("a".repeat(40)).unwrap(),
                canonical_path: std::env::temp_dir().join("origin-repo/logs/HEAD"),
                file_identity: [1, 2],
                frontier: 100,
                fingerprint: [3; 32],
                change_token: [4; 4],
                head_file_identity: [5, 6],
                head_change_token: [7; 4],
            },
            checkpoint: HookLiveOriginCheckpointV1 {
                generation: 10,
                file_identity: 10,
                complete_frontier: frontier,
                complete_prefix_fingerprint: frontier + 1000,
            },
            physical_eof: eof,
            validated_checkpoint: None,
            frames: Vec::new(),
        }
    }

    fn origin_append(
        previous: &HookLiveOriginObservationV1,
        ends: &[u64],
    ) -> HookLiveOriginObservationV1 {
        let frontier = *ends.last().unwrap();
        let mut next = previous.clone();
        next.validated_checkpoint = Some(previous.checkpoint);
        next.checkpoint.complete_frontier = frontier;
        next.checkpoint.complete_prefix_fingerprint = frontier + 1000;
        next.physical_eof = frontier;
        let mut start = previous.checkpoint.complete_frontier;
        next.frames = ends
            .iter()
            .map(|end| {
                let frame = HookLiveOriginFrameV1 {
                    start,
                    end: *end,
                    resume_fingerprint: *end + 1000,
                };
                start = *end;
                frame
            })
            .collect();
        next
    }

    fn record_origin(
        ledger: &mut HookAdmissionLedgerV1,
        event_id: u8,
        observation: Option<HookLiveOriginObservationV1>,
    ) -> HookLiveOriginOutcomeV1 {
        let envelope = envelope(event_id, 5);
        let now = UtcMicros(i64::from(event_id));
        let receipt = ledger.admit_with_receipt(&envelope, now).unwrap();
        ledger
            .record_live_origin(&envelope, receipt, observation, now)
            .unwrap()
    }

    #[test]
    fn live_origin_baseline_excludes_legacy_cursor_content_and_partial_frame_extension() {
        let root = TestDir::new("origin-partial");
        let mut ledger = open(root.path(), UtcMicros(1));
        // Ordinary admission/catch-up has no authority to establish origin.
        ledger.admit(&envelope(8, 5), UtcMicros(8)).unwrap();
        assert!(ledger.live_origin_baseline([7; 32], UtcMicros(8)).is_none());
        let baseline = origin_observation(100, 110);
        assert_eq!(
            record_origin(&mut ledger, 9, Some(baseline.clone())),
            HookLiveOriginOutcomeV1::Baseline
        );
        assert!(
            read_hook_live_origin_proofs(root.path(), HookHostV1::ClaudeCode, UtcMicros(9))
                .unwrap()
                .is_empty()
        );
        // 100..130 completes a frame already begun at physical EOF 110.
        assert_eq!(
            record_origin(&mut ledger, 10, Some(origin_append(&baseline, &[130, 150]))),
            HookLiveOriginOutcomeV1::Sealed
        );
        let proofs =
            read_hook_live_origin_proofs(root.path(), HookHostV1::ClaudeCode, UtcMicros(10))
                .unwrap();
        assert_eq!(proofs.len(), 1);
        assert_eq!(
            proofs[0].frames,
            vec![HookLiveOriginFrameV1 {
                start: 130,
                end: 150,
                resume_fingerprint: 1150
            }]
        );
        assert_eq!(proofs[0].baseline.observation.physical_eof, 110);
    }

    #[test]
    fn stable_live_origin_and_exact_frame_fingerprints_survive_reopen() {
        let root = TestDir::new("origin-reopen");
        let baseline = origin_observation(100, 100);
        let expected;
        {
            let mut ledger = open(root.path(), UtcMicros(1));
            record_origin(&mut ledger, 9, Some(baseline.clone()));
            record_origin(&mut ledger, 10, Some(origin_append(&baseline, &[125, 150])));
            expected =
                read_hook_live_origin_proofs(root.path(), HookHostV1::ClaudeCode, UtcMicros(10))
                    .unwrap();
            assert_eq!(expected.len(), 1);
            assert_eq!(expected[0].frames.len(), 2);
            assert_ne!(
                expected[0].baseline.admission.event_id,
                expected[0].seal.event_id
            );
            assert_ne!(
                expected[0].baseline.admission.digest,
                expected[0].seal.digest
            );
        }
        let mut ledger = open(root.path(), UtcMicros(11));
        assert_eq!(
            read_hook_live_origin_proofs(root.path(), HookHostV1::ClaudeCode, UtcMicros(11))
                .unwrap(),
            expected
        );
        assert_eq!(
            read_hook_live_origin_boundaries(root.path(), HookHostV1::ClaudeCode, UtcMicros(11))
                .unwrap()[0]
                .observation
                .checkpoint
                .complete_frontier,
            150
        );
        assert_eq!(
            record_origin(&mut ledger, 10, Some(origin_append(&baseline, &[175]))),
            HookLiveOriginOutcomeV1::Duplicate
        );
        assert_eq!(
            read_hook_live_origin_proofs(root.path(), HookHostV1::ClaudeCode, UtcMicros(11))
                .unwrap(),
            expected
        );
    }

    #[test]
    fn new_partial_frame_keeps_origin_across_an_intermediate_hook_and_restart() {
        for initial_eof in [100, 110] {
            let root = TestDir::new("origin-partial-across-hooks");
            let baseline = origin_observation(100, initial_eof);
            let mut partial = baseline.clone();
            partial.physical_eof = 120;
            partial.validated_checkpoint = Some(baseline.checkpoint);
            {
                let mut ledger = open(root.path(), UtcMicros(1));
                record_origin(&mut ledger, 9, Some(baseline));
                record_origin(&mut ledger, 10, Some(partial.clone()));
                let retained = ledger.live_origin_baseline([7; 32], UtcMicros(10)).unwrap();
                assert_eq!(retained.start.physical_eof, initial_eof);
                assert_eq!(retained.observation.physical_eof, 120);
                assert_eq!(retained.start.admission.event_id, [9; 16]);
                assert_eq!(retained.admission.event_id, [10; 16]);
            }
            let mut ledger = open(root.path(), UtcMicros(11));
            assert_eq!(
                record_origin(&mut ledger, 11, Some(origin_append(&partial, &[150, 170]))),
                HookLiveOriginOutcomeV1::Sealed
            );
            let proofs =
                read_hook_live_origin_proofs(root.path(), HookHostV1::ClaudeCode, UtcMicros(11))
                    .unwrap();
            assert_eq!(proofs.len(), 1);
            let starts = proofs[0]
                .frames
                .iter()
                .map(|frame| frame.start)
                .collect::<Vec<_>>();
            if initial_eof == 100 {
                assert_eq!(
                    starts,
                    [100, 150],
                    "new partial content retains original live origin"
                );
            } else {
                assert_eq!(
                    starts,
                    [150],
                    "pre-baseline partial content remains excluded"
                );
            }
        }
    }

    #[test]
    fn advancing_complete_checkpoints_do_not_raise_the_initial_exclusion_floor() {
        let root = TestDir::new("origin-advancing-checkpoints");
        let mut ledger = open(root.path(), UtcMicros(1));
        let baseline = origin_observation(100, 100);
        record_origin(&mut ledger, 9, Some(baseline.clone()));
        let mut first = origin_append(&baseline, &[125]);
        first.physical_eof = 140;
        record_origin(&mut ledger, 10, Some(first.clone()));
        let mut partial = first.clone();
        partial.frames.clear();
        partial.validated_checkpoint = Some(first.checkpoint);
        partial.physical_eof = 145;
        record_origin(&mut ledger, 11, Some(partial.clone()));
        record_origin(&mut ledger, 12, Some(origin_append(&partial, &[160])));
        let proofs =
            read_hook_live_origin_proofs(root.path(), HookHostV1::ClaudeCode, UtcMicros(12))
                .unwrap();
        assert_eq!(proofs.len(), 2);
        assert_eq!(proofs[1].baseline.start.physical_eof, 100);
        assert_eq!(proofs[1].baseline.observation.physical_eof, 145);
        assert_eq!(
            proofs[1].frames,
            [HookLiveOriginFrameV1 {
                start: 125,
                end: 160,
                resume_fingerprint: 1160
            }]
        );
    }

    #[test]
    fn branch_aba_source_replacement_truncation_and_prefix_changes_rebaseline() {
        for change in [
            "branch_aba",
            "source_replaced",
            "source_truncated",
            "prefix_changed",
            "other_profile",
        ] {
            let root = TestDir::new(change);
            let mut ledger = open(root.path(), UtcMicros(1));
            let baseline = origin_observation(100, 100);
            record_origin(&mut ledger, 9, Some(baseline.clone()));
            let mut next = origin_append(&baseline, &[150]);
            match change {
                "branch_aba" => {
                    // Same current branch and HEAD; a changed reflog frontier
                    // records the intervening checkout transitions.
                    next.branch_evidence.frontier += 1;
                    next.branch_evidence.fingerprint = [9; 32];
                }
                "source_replaced" => {
                    next.checkpoint.file_identity += 1;
                    next.checkpoint.generation += 1;
                }
                "source_truncated" => {
                    next = origin_observation(50, 50);
                }
                "prefix_changed" => {
                    next.validated_checkpoint = None;
                    next.checkpoint.generation += 1;
                }
                "other_profile" => {
                    next.scope.profile_id = UserProfileId::new("profile.other").unwrap();
                }
                _ => unreachable!(),
            }
            let cutoff = next.physical_eof;
            assert_eq!(
                record_origin(&mut ledger, 10, Some(next)),
                HookLiveOriginOutcomeV1::Baseline,
                "{change}"
            );
            assert!(
                read_hook_live_origin_proofs(root.path(), HookHostV1::ClaudeCode, UtcMicros(10))
                    .unwrap()
                    .is_empty(),
                "{change}"
            );
            assert_eq!(
                ledger
                    .live_origin_baseline([7; 32], UtcMicros(10))
                    .unwrap()
                    .observation
                    .physical_eof,
                cutoff
            );
        }
    }

    #[test]
    fn missing_origin_evidence_discards_interval_before_a_new_live_baseline() {
        let root = TestDir::new("origin-unavailable");
        let mut ledger = open(root.path(), UtcMicros(1));
        let baseline = origin_observation(100, 100);
        record_origin(&mut ledger, 9, Some(baseline.clone()));
        assert_eq!(
            record_origin(&mut ledger, 10, None),
            HookLiveOriginOutcomeV1::Unavailable
        );
        assert!(
            ledger
                .live_origin_baseline([7; 32], UtcMicros(10))
                .is_none()
        );
        assert_eq!(
            record_origin(&mut ledger, 11, Some(origin_append(&baseline, &[150]))),
            HookLiveOriginOutcomeV1::Baseline
        );
        assert!(
            read_hook_live_origin_proofs(root.path(), HookHostV1::ClaudeCode, UtcMicros(11))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn live_origin_proof_requires_both_retained_unexpired_admission_receipts() {
        let root = TestDir::new("origin-retained-receipts");
        let mut ledger = open(root.path(), UtcMicros(1));
        let baseline = origin_observation(100, 100);
        record_origin(&mut ledger, 9, Some(baseline.clone()));
        record_origin(&mut ledger, 10, Some(origin_append(&baseline, &[150])));
        assert_eq!(
            read_hook_live_origin_proofs(root.path(), HookHostV1::ClaudeCode, UtcMicros(10))
                .unwrap()
                .len(),
            1
        );
        let expired = UtcMicros(10 + HookAdmissionLedgerLimitsV1::stock().max_age_micros);
        assert!(
            read_hook_live_origin_proofs(root.path(), HookHostV1::ClaudeCode, expired)
                .unwrap()
                .is_empty()
        );
        assert!(
            read_hook_live_origin_proofs(root.path(), HookHostV1::Codex, UtcMicros(10))
                .unwrap()
                .is_empty()
        );
        // Retaining only the seal is insufficient; the original baseline
        // receipt may never be reconstructed from the metadata sidecar.
        let mut bytes = ledger_header().to_vec();
        let seal = &ledger.entries[&[10; 16]];
        bytes.extend_from_slice(&encode_record([10; 16], seal.digest, seal.admitted_at));
        fs::write(records_path(root.path()), bytes).unwrap();
        assert!(
            read_hook_live_origin_proofs(root.path(), HookHostV1::ClaudeCode, UtcMicros(10))
                .unwrap()
                .is_empty()
        );
        fs::remove_file(records_path(root.path())).unwrap();
        assert!(
            read_hook_live_origin_boundaries(root.path(), HookHostV1::ClaudeCode, UtcMicros(10))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn corrupt_origin_metadata_preserves_admission_but_requires_a_fresh_baseline() {
        let root = TestDir::new("origin-corrupt");
        let baseline = origin_observation(100, 100);
        {
            let mut ledger = open(root.path(), UtcMicros(1));
            record_origin(&mut ledger, 9, Some(baseline.clone()));
        }
        fs::write(
            root.path().join(LIVE_ORIGINS_FILE),
            b"incomplete origin metadata",
        )
        .unwrap();
        let (mut ledger, report) = HookAdmissionLedgerV1::open(
            root.path(),
            HookHostV1::ClaudeCode,
            HookAdmissionLedgerLimitsV1::stock(),
            UtcMicros(10),
        )
        .unwrap();
        assert_eq!(
            report.live_origin_metadata_error,
            Some(HookAdmissionLedgerError::RecordUndecodable)
        );
        assert_eq!(
            ledger.admit(&envelope(9, 5), UtcMicros(10)).unwrap(),
            HookAdmissionDecisionV1::ExactDuplicate
        );
        assert!(
            ledger
                .live_origin_baseline([7; 32], UtcMicros(10))
                .is_none()
        );
        assert_eq!(
            record_origin(&mut ledger, 11, Some(origin_append(&baseline, &[150]))),
            HookLiveOriginOutcomeV1::Baseline
        );
        assert!(
            read_hook_live_origin_proofs(root.path(), HookHostV1::ClaudeCode, UtcMicros(11))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn identical_bytes_converge_on_exact_duplicate() {
        let root = TestDir::new("ledger");
        let mut ledger = open(root.path(), UtcMicros(1));

        assert_eq!(
            ledger.admit(&envelope(9, 5), UtcMicros(2)).unwrap(),
            HookAdmissionDecisionV1::Admitted
        );
        assert_eq!(
            ledger.admit(&envelope(9, 5), UtcMicros(3)).unwrap(),
            HookAdmissionDecisionV1::ExactDuplicate
        );
        assert_eq!(ledger.live_records(), 1);
    }

    #[test]
    fn same_identity_with_different_bytes_is_a_conflict() {
        let root = TestDir::new("ledger");
        let mut ledger = open(root.path(), UtcMicros(1));

        assert_eq!(
            ledger.admit(&envelope(9, 5), UtcMicros(2)).unwrap(),
            HookAdmissionDecisionV1::Admitted
        );
        assert_eq!(
            ledger.admit(&envelope(9, 6), UtcMicros(3)).unwrap(),
            HookAdmissionDecisionV1::Conflict
        );
    }

    #[test]
    fn idempotency_survives_a_reopen() {
        let root = TestDir::new("ledger");
        {
            let mut ledger = open(root.path(), UtcMicros(1));
            assert_eq!(
                ledger.admit(&envelope(9, 5), UtcMicros(2)).unwrap(),
                HookAdmissionDecisionV1::Admitted
            );
        }
        let mut reopened = open(root.path(), UtcMicros(4));

        assert_eq!(reopened.live_records(), 1);
        assert_eq!(
            reopened.admit(&envelope(9, 5), UtcMicros(5)).unwrap(),
            HookAdmissionDecisionV1::ExactDuplicate
        );
        assert_eq!(
            reopened.admit(&envelope(9, 6), UtcMicros(6)).unwrap(),
            HookAdmissionDecisionV1::Conflict
        );
    }

    #[test]
    fn writer_lock_contends_and_releases_across_processes() {
        const MODE_ENV: &str = "TRACEDECAY_HOOK_ADMISSION_LOCK_PROBE";
        const ROOT_ENV: &str = "TRACEDECAY_HOOK_ADMISSION_LOCK_ROOT";
        if let Ok(mode) = std::env::var(MODE_ENV) {
            let root = PathBuf::from(std::env::var_os(ROOT_ENV).expect("child lock root"));
            match mode.as_str() {
                "contended" => assert!(matches!(
                    HookAdmissionLedgerV1::open(
                        &root,
                        HookHostV1::ClaudeCode,
                        HookAdmissionLedgerLimitsV1::stock(),
                        UtcMicros(2),
                    ),
                    Err(HookAdmissionLedgerError::Busy)
                )),
                "released" => {
                    HookAdmissionLedgerV1::open(
                        &root,
                        HookHostV1::ClaudeCode,
                        HookAdmissionLedgerLimitsV1::stock(),
                        UtcMicros(3),
                    )
                    .expect("OS releases the ledger lock when its owner exits");
                }
                other => panic!("unknown child lock probe mode: {other}"),
            }
            return;
        }

        let root = TestDir::new("process-lock");
        let first = open(root.path(), UtcMicros(1));
        let test_name =
            "admission_ledger::tests::writer_lock_contends_and_releases_across_processes";
        let run_child = |mode: &str| {
            Command::new(std::env::current_exe().expect("current test binary"))
                .args(["--exact", test_name, "--nocapture"])
                .env(MODE_ENV, mode)
                .env(ROOT_ENV, root.path())
                .status()
                .expect("run admission lock probe child")
        };
        assert!(run_child("contended").success());
        drop(first);
        assert!(run_child("released").success());
    }

    #[test]
    fn durable_receipt_order_is_successive_and_survives_duplicate_reopen() {
        let root = TestDir::new("ledger-receipt-order");
        let first_order;
        {
            let mut ledger = open(root.path(), UtcMicros(1));
            let first = ledger
                .admit_with_receipt(&envelope(9, 5), UtcMicros(2))
                .unwrap();
            let second = ledger
                .admit_with_receipt(&envelope(10, 5), UtcMicros(3))
                .unwrap();
            assert_eq!(first.decision, HookAdmissionDecisionV1::Admitted);
            assert_eq!(second.decision, HookAdmissionDecisionV1::Admitted);
            assert_eq!(second.order, first.order + 1);
            first_order = first.order;
        }

        let mut reopened = open(root.path(), UtcMicros(4));
        let duplicate = reopened
            .admit_with_receipt(&envelope(9, 5), UtcMicros(5))
            .unwrap();
        assert_eq!(duplicate.decision, HookAdmissionDecisionV1::ExactDuplicate);
        assert_eq!(duplicate.order, first_order);
    }

    #[test]
    fn pending_producer_work_redrives_until_completion_survives_reopen() {
        let root = TestDir::new("ledger-work-completion");
        let admitted = envelope(9, 5);
        {
            let mut ledger = open(root.path(), UtcMicros(1));
            let first = ledger.admit_with_receipt(&admitted, UtcMicros(2)).unwrap();
            assert!(!first.work_completed);
        }

        {
            let mut restarted = open(root.path(), UtcMicros(3));
            let duplicate = restarted
                .admit_with_receipt(&admitted, UtcMicros(4))
                .unwrap();
            assert_eq!(duplicate.decision, HookAdmissionDecisionV1::ExactDuplicate);
            assert!(!duplicate.work_completed);
            assert!(restarted.mark_work_completed(&admitted).unwrap());
        }

        let mut completed = open(root.path(), UtcMicros(5));
        let duplicate = completed
            .admit_with_receipt(&admitted, UtcMicros(6))
            .unwrap();
        assert!(duplicate.work_completed);
        assert!(!completed.mark_work_completed(&admitted).unwrap());
    }

    #[test]
    fn failed_completion_persistence_keeps_work_pending_in_memory() {
        let root = TestDir::new("ledger-work-completion-failure");
        let admitted = envelope(9, 5);
        let mut ledger = open(root.path(), UtcMicros(1));
        ledger.admit_with_receipt(&admitted, UtcMicros(2)).unwrap();

        let completions = completions_path(root.path());
        fs::remove_file(&completions).unwrap();
        fs::create_dir(&completions).unwrap();
        assert_eq!(
            ledger.mark_work_completed(&admitted),
            Err(HookAdmissionLedgerError::Io)
        );
        assert!(
            !ledger
                .admit_with_receipt(&admitted, UtcMicros(3))
                .unwrap()
                .work_completed,
            "failed completion fsync must not suppress in-process redrive"
        );

        fs::remove_dir(&completions).unwrap();
        assert!(ledger.mark_work_completed(&admitted).unwrap());
    }

    #[test]
    fn entries_beyond_the_age_bound_stop_suppressing_admission() {
        let root = TestDir::new("ledger");
        let mut ledger = open(root.path(), UtcMicros(1));
        ledger.admit(&envelope(9, 5), UtcMicros(2)).unwrap();
        let beyond = UtcMicros(2 + MAX_SPOOL_AGE_MICROS + 1);

        assert_eq!(
            ledger.admit(&envelope(9, 5), beyond).unwrap(),
            HookAdmissionDecisionV1::Admitted
        );
        assert_eq!(ledger.expire(UtcMicros(beyond.0 * 2)).unwrap(), 1);
        assert_eq!(ledger.live_records(), 0);
    }

    #[test]
    fn record_bound_evicts_oldest_and_stays_durable() {
        let root = TestDir::new("ledger");
        let limits = HookAdmissionLedgerLimitsV1 {
            max_records: 8,
            max_age_micros: MAX_SPOOL_AGE_MICROS,
        };
        let mut ledger =
            HookAdmissionLedgerV1::open(root.path(), HookHostV1::ClaudeCode, limits, UtcMicros(1))
                .unwrap()
                .0;
        for index in 1..=9u8 {
            assert_eq!(
                ledger
                    .admit(&envelope(index, 5), UtcMicros(i64::from(index) + 1))
                    .unwrap(),
                HookAdmissionDecisionV1::Admitted
            );
        }

        assert!(ledger.live_records() <= 8);
        // The newest identity is still deduplicated after eviction + reopen.
        drop(ledger);
        let mut reopened =
            HookAdmissionLedgerV1::open(root.path(), HookHostV1::ClaudeCode, limits, UtcMicros(20))
                .unwrap()
                .0;
        assert_eq!(
            reopened.admit(&envelope(9, 5), UtcMicros(21)).unwrap(),
            HookAdmissionDecisionV1::ExactDuplicate
        );
    }

    #[test]
    fn a_corrupt_tail_is_truncated_without_losing_the_valid_prefix() {
        let root = TestDir::new("ledger");
        {
            let mut ledger = open(root.path(), UtcMicros(1));
            ledger.admit(&envelope(9, 5), UtcMicros(2)).unwrap();
        }
        let path = records_path(root.path());
        let mut bytes = fs::read(&path).unwrap();
        bytes.extend_from_slice(&[0xAB; RECORD_BYTES]);
        fs::write(&path, &bytes).unwrap();

        let (mut ledger, report) = HookAdmissionLedgerV1::open(
            root.path(),
            HookHostV1::ClaudeCode,
            HookAdmissionLedgerLimitsV1::stock(),
            UtcMicros(3),
        )
        .unwrap();

        assert_eq!(report.truncated_tail_bytes, RECORD_BYTES as u64);
        assert_eq!(report.live_records, 1);
        assert_eq!(
            ledger.admit(&envelope(9, 5), UtcMicros(4)).unwrap(),
            HookAdmissionDecisionV1::ExactDuplicate
        );
    }
}
