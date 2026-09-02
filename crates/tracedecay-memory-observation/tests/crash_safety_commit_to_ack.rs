#![cfg(unix)]
//! tdmem-0508: crash safety at every boundary between the host commit and the
//! durable provider acknowledgement.
//!
//! # A life is a real process, and it is really killed
//!
//! Every journey here runs the *real* runtime seam — [`IngressRuntimeV1`] over
//! a file-backed [`SqliteObservationJournal`], the wake edge, then
//! [`DeliveryRuntimeV1`] over a provider whose effects live in their own file.
//! The life that dies runs in a **child process** re-executed from this test
//! binary. It reaches one named boundary, records its arrival in a durable
//! marker file, and then parks; the parent observes the marker and sends
//! `SIGKILL`. Nothing is unwound: no destructor runs, the SQLite connection is
//! never closed, the WAL is never checkpointed, and the parent asserts the
//! child really died on signal 9.
//!
//! Recovery then happens in a *different* process — the parent — which opens
//! the same canonical stream, the same journal, and the same provider ledger
//! from disk and drives them to convergence. Nothing crosses the process
//! boundary in memory: not the replay position, not the lease, not the
//! provider's effect set. What comes back is what the three files hold.
//!
//! # The six boundaries, and where each hook actually sits
//!
//! | boundary | hook site |
//! | --- | --- |
//! | canonical commit → outbox write | inside `append_admitted`, *before* the inner journal transaction, for the target sequence |
//! | outbox write → enqueue | inside `append_admitted`, *after* the inner transaction committed and before `ingest` reaches `wake.signal()` |
//! | enqueue → provider receive | first statement of `deliver`, before the provider sees the bytes |
//! | provider receive → provider commit → ack return | inside `deliver`, immediately after the provider's effect is fsync'd and before the terminal is returned |
//! | ack return → ack persistence | inside `record_attempt`, before the inner journal transaction |
//! | after ack persistence | inside `record_attempt`, immediately after the inner transaction committed |
//!
//! Each hook fires at most once per life and the parent asserts the marker file
//! holds exactly one arrival, so a hook that is deleted, moved, or never
//! reached fails the test rather than degrading into a healthy run.
//!
//! # Why the canonical stream is its own file
//!
//! The host's canonical authority settles first and the observation pipeline
//! follows. If the "committed" stream lived only in the parent's memory, the
//! before-outbox case would have nothing durable to recover *from* and the
//! no-loss claim would be vacuous. So the canonical stream is an append-only
//! fsync'd file the child commits to **before** the pipeline runs, and every
//! restarted life discovers what to replay by reading that file back. The
//! expected stream is never handed to recovery as an argument.
//!
//! # Why the provider ledger is a file of lines
//!
//! A provider that deduplicates on the idempotency key is the only thing that
//! makes redelivery safe, so the harness models one honestly: it appends one
//! line per committed effect and dedupes by reading its own file back. A second
//! effect for the same mutation is therefore a second *line*, not something a
//! `HashSet` quietly absorbs — which is what lets AC3 be checked by counting
//! rather than by trusting the harness.

mod support;

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};

use support::{
    Builder, DAY, LEASE, MINUTE, PROVENANCE_DIGEST, PROVIDER_RECEIPT_DIGEST, SECOND, T0,
    TestIngestControl, TestResult, digest_hex, gate, journal, lane, lease_request, policy,
    stream_key,
};

use tracedecay_memory_observation::{
    AdmissionDecisionV1, AdmittedObservationV1, AppendOutcomeV1, AttemptOrphanCauseV1,
    AttemptOrphanRecordV1, AttemptOrphanRecoveryV1, AttemptOutcomeV1, AttemptRefusalOutcomeV1,
    AttemptRefusalRecordV1, DeliveryAttemptV1, DeliveryControlV1, DeliveryReceiptIdV1,
    DeliveryRuntimeV1, DeliveryStateV1, DeliveryWakeV1, DispatchLeaseIdV1, DispatchPolicyV1,
    DispatchRequestV1, IngressControlV1, IngressRuntimeV1, JournalInspectionFilterV1,
    JournalInspectionPageV1, LeaseRequestV1, LeasedObservationV1, ObservationAdmissionAdapterV1,
    ObservationDeliveryReceiptV1, ObservationDispatchPortV1, ObservationIdV1,
    ObservationJournalError, ObservationJournalReaderV1, ObservationLaneKeyV1,
    ObservationLoadClassV1, ObservationOutcomeV1, ProviderDeliveryAdapterV1, QueuePressureV1,
    ReplayCursorV1, RetentionClassV1, RetryBackoffV1, SourceRecordV1, SourceSequenceV1,
    SourceStreamKeyV1, SqliteObservationJournal, WithheldAdmissionV1,
};
use tracedecay_memory_provider_api::contract::TerminalCode;
use tracedecay_memory_provider_api::{
    CommittedEffectEvidence, FallbackDirective, ProviderOperation, TerminalRecord,
};

/// Per-attempt budget, shorter than the fixture lease so the lease is never the
/// binding bound by accident.
const ATTEMPT_BUDGET: i64 = 5 * SECOND;

/// Restarts one journey is allowed before the harness calls it a stall. A
/// journey that needs more than this is not converging, which is itself the
/// defect AC1 is about.
const MAX_RECOVERY_LIVES: usize = 8;

/// The canonical stream every journey replays.
const CANONICAL_STREAM: &str = "session-1";

/// Carries one child life's whole assignment. Present exactly when this process
/// *is* a child life.
const CHILD_LIFE_ENV: &str = "TDMEM_0508_CHILD_LIFE";

/// How long the parent waits for a child to reach its boundary before calling
/// the hook unreachable.
const HOOK_ARRIVAL_TIMEOUT: Duration = Duration::from_secs(120);

/// Signal number the parent kills a parked child with, and the only exit status
/// a crashed life may have.
const SIGKILL: i32 = 9;

// ------------------------------------------------------------------ errors --

#[derive(Debug)]
struct HarnessError(String);

impl Display for HarnessError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for HarnessError {}

fn harness(detail: impl Display) -> HarnessError {
    HarnessError(detail.to_string())
}

// ----------------------------------------------------- the canonical store --

/// The host's canonical authority, durable and separate from the outbox.
///
/// One appended, fsync'd line per settled position. This is what the host has
/// *committed*; the observation journal is downstream of it. A restarted life
/// learns what to replay by reading this file, never from a value the test
/// carried across the crash.
struct CanonicalStreamV1 {
    path: PathBuf,
}

impl CanonicalStreamV1 {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Settles one position durably, once. A position the authority already
    /// settled is not settled a second time, so a restarted host re-declaring
    /// its stream cannot corrupt the canonical order.
    fn commit(&self, sequence: u64) -> Result<(), Box<dyn Error>> {
        if self.committed()?.contains(&sequence) {
            return Ok(());
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        writeln!(file, "{sequence}")?;
        file.sync_all()?;
        Ok(())
    }

    /// Every settled position, read back from disk in commit order.
    fn committed(&self) -> Result<Vec<u64>, Box<dyn Error>> {
        match fs::read_to_string(&self.path) {
            Ok(text) => text
                .lines()
                .map(|line| line.parse::<u64>().map_err(|error| harness(error).into()))
                .collect(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(error) => Err(error.into()),
        }
    }
}

// ------------------------------------------------------- the provider side --

/// The provider's own durable effect ledger: one appended, fsync'd line per
/// effect it committed.
struct ProviderLedgerV1 {
    path: PathBuf,
}

impl ProviderLedgerV1 {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Every effect line, in commit order, duplicates included.
    fn effects(&self) -> Result<Vec<String>, Box<dyn Error>> {
        match fs::read_to_string(&self.path) {
            Ok(text) => Ok(text.lines().map(str::to_owned).collect()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(error) => Err(error.into()),
        }
    }

    /// Commits one effect durably before any answer leaves the provider.
    fn commit(&self, key: &str) -> Result<(), Box<dyn Error>> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        writeln!(file, "{key}")?;
        file.sync_all()?;
        Ok(())
    }
}

/// A provider that deduplicates on the idempotency key against its own durable
/// ledger, exactly as a real one must for redelivery to be safe.
struct DurableProviderV1<'a> {
    ledger: &'a ProviderLedgerV1,
    hooks: &'a HooksV1,
    now: i64,
    received: AtomicU32,
}

impl ProviderDeliveryAdapterV1 for DurableProviderV1<'_> {
    type Error = HarnessError;

    fn deliver(
        &self,
        leased: &LeasedObservationV1,
        _control: &DeliveryControlV1,
    ) -> Result<DeliveryAttemptV1, Self::Error> {
        let key = leased.idempotency_key.as_str().to_owned();
        let sequence = leased.source_sequence.0;
        // The transport dies before the provider ever sees the bytes. Nothing
        // is committed, and the lease is left held by a process that no longer
        // exists.
        self.hooks
            .check(HookPointV1::BeforeProviderReceive, sequence);
        self.received.fetch_add(1, Ordering::SeqCst);

        // A redelivery that carried different bytes would break the provider's
        // own dedupe contract, so the provider checks rather than assumes.
        let delivered_digest = digest_hex(&leased.payload.bytes);
        if delivered_digest != leased.payload.sha256 {
            return Err(HarnessError(format!(
                "delivered bytes digest {delivered_digest} does not match the leased \
                 payload digest {}",
                leased.payload.sha256
            )));
        }

        let started = self.now;
        let finished = started.saturating_add(1_000);
        let already = self.ledger.effects().map_err(harness)?;
        if already.iter().any(|line| line == &key) {
            let terminal = duplicate_terminal(leased, &key).map_err(harness)?;
            return Ok(DeliveryAttemptV1::Answered {
                terminal: Box::new(terminal),
                started_at_unix_micros: started,
                finished_at_unix_micros: finished,
            });
        }

        // The provider commits before it answers. That ordering is what makes
        // "lost answer" and "never received" two genuinely different faults.
        self.ledger.commit(&key).map_err(harness)?;
        // The effect is durable and the answer has not left the provider. This
        // is the most dangerous window in the whole journey.
        self.hooks
            .check(HookPointV1::AfterProviderCommitBeforeAckReturn, sequence);
        let terminal = success_terminal(leased).map_err(harness)?;
        Ok(DeliveryAttemptV1::Answered {
            terminal: Box::new(terminal),
            started_at_unix_micros: started,
            finished_at_unix_micros: finished,
        })
    }
}

fn success_terminal(leased: &LeasedObservationV1) -> Result<TerminalRecord, Box<dyn Error>> {
    Ok(TerminalRecord::new(
        ProviderOperation::Observe,
        leased.target.provider_id.clone(),
        TerminalCode::Success,
        CommittedEffectEvidence::committed(
            1,
            2,
            Vec::new(),
            PROVIDER_RECEIPT_DIGEST,
            PROVENANCE_DIGEST,
        )?,
        FallbackDirective::forbidden(),
        format!("observe-{}", leased.observation_id.as_str()),
        leased.exact_scope_sha256.clone(),
        None,
    )?)
}

fn duplicate_terminal(
    leased: &LeasedObservationV1,
    key: &str,
) -> Result<TerminalRecord, Box<dyn Error>> {
    Ok(TerminalRecord::new(
        ProviderOperation::Observe,
        leased.target.provider_id.clone(),
        TerminalCode::Success,
        CommittedEffectEvidence::duplicate(
            2,
            key,
            format!("observe-{}", leased.observation_id.as_str()),
            PROVIDER_RECEIPT_DIGEST,
        )?,
        FallbackDirective::forbidden(),
        format!("observe-{}", leased.observation_id.as_str()),
        leased.exact_scope_sha256.clone(),
        None,
    )?)
}

// ------------------------------------------------------------- crash hooks --

/// One named production boundary a life can die at.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HookPointV1 {
    /// The host settled canonically and died before the outbox write.
    BeforeOutboxWrite,
    /// The outbox transaction committed; the host died before `ingest`
    /// signalled the delivery wake, so nothing was ever enqueued.
    AfterOutboxWriteBeforeEnqueue,
    /// The row was leased and the host died before the provider received it.
    BeforeProviderReceive,
    /// The provider committed its effect and the host died before the answer
    /// came back.
    AfterProviderCommitBeforeAckReturn,
    /// The provider answered and the host died before the acknowledgement was
    /// persisted.
    AfterAckReturnBeforeAckPersist,
    /// The acknowledgement transaction committed and the host died immediately
    /// after, before any further row was dispatched.
    AfterAckPersist,
}

impl HookPointV1 {
    const ALL: [Self; 6] = [
        Self::BeforeOutboxWrite,
        Self::AfterOutboxWriteBeforeEnqueue,
        Self::BeforeProviderReceive,
        Self::AfterProviderCommitBeforeAckReturn,
        Self::AfterAckReturnBeforeAckPersist,
        Self::AfterAckPersist,
    ];

    const fn as_wire(self) -> &'static str {
        match self {
            Self::BeforeOutboxWrite => "before_outbox_write",
            Self::AfterOutboxWriteBeforeEnqueue => "after_outbox_write_before_enqueue",
            Self::BeforeProviderReceive => "before_provider_receive",
            Self::AfterProviderCommitBeforeAckReturn => "after_provider_commit_before_ack_return",
            Self::AfterAckReturnBeforeAckPersist => "after_ack_return_before_ack_persist",
            Self::AfterAckPersist => "after_ack_persist",
        }
    }

    fn from_wire(value: &str) -> Result<Self, Box<dyn Error>> {
        Self::ALL
            .into_iter()
            .find(|point| point.as_wire() == value)
            .ok_or_else(|| harness(format!("unknown hook point {value}")).into())
    }

    /// Which canonical position the hook is armed for. The append hooks are
    /// armed on the last settled position, the delivery hooks on the first, so
    /// each boundary leaves a distinguishable durable shape behind it.
    fn target_sequence(self, committed: &[u64]) -> Option<u64> {
        match self {
            Self::BeforeOutboxWrite | Self::AfterOutboxWriteBeforeEnqueue => {
                committed.last().copied()
            }
            _ => committed.first().copied(),
        }
    }
}

/// The one boundary this process is armed to die at, and the durable proof it
/// arrived there.
///
/// A hook fires at most once: the first arrival appends its line to the marker
/// file and then parks forever, waiting for the parent's `SIGKILL`. Parking
/// rather than exiting is what makes the kill *external* and abrupt — no
/// destructor, no `atexit`, no SQLite close, no WAL checkpoint.
struct HooksV1 {
    armed: Option<HookPointV1>,
    target_sequence: u64,
    marker: PathBuf,
    fired: AtomicBool,
}

impl HooksV1 {
    /// A life with no hook: the recovery process.
    fn disarmed() -> Self {
        Self {
            armed: None,
            target_sequence: 0,
            marker: PathBuf::new(),
            fired: AtomicBool::new(false),
        }
    }

    fn armed_at(point: HookPointV1, target_sequence: u64, marker: PathBuf) -> Self {
        Self {
            armed: Some(point),
            target_sequence,
            marker,
            fired: AtomicBool::new(false),
        }
    }

    /// Dies here when this is the armed boundary for this position.
    fn check(&self, point: HookPointV1, sequence: u64) {
        if self.armed != Some(point) || sequence != self.target_sequence {
            return;
        }
        if self.fired.swap(true, Ordering::SeqCst) {
            return;
        }
        self.arrive(point)
    }

    fn arrive(&self, point: HookPointV1) -> ! {
        // The marker is the barrier the parent waits on. If it cannot be
        // written the parent times out and reports the hook as unreached,
        // which is the correct failure — so there is nothing to do with the
        // error here but park and let the parent decide.
        let _ = self.record_arrival(point);
        loop {
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn record_arrival(&self, point: HookPointV1) -> Result<(), Box<dyn Error>> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.marker)?;
        writeln!(file, "{}", point.as_wire())?;
        file.sync_all()?;
        Ok(())
    }
}

// -------------------------------------------------- crash-armed journal ports --

/// The ingress side of the journal with the two outbox boundaries armed.
///
/// Everything is delegated; the only thing this adds is a place to die that is
/// exactly at the seam the boundary names — before the append transaction, and
/// after it committed but before `IngressRuntimeV1::ingest` reaches
/// `wake.signal()`.
struct CrashingDispatchPortV1<'a> {
    inner: &'a SqliteObservationJournal,
    hooks: &'a HooksV1,
}

impl ObservationDispatchPortV1 for CrashingDispatchPortV1<'_> {
    fn append_admitted(
        &self,
        admitted: &AdmittedObservationV1,
    ) -> Result<AppendOutcomeV1, ObservationJournalError> {
        let sequence = admitted.source.source_sequence.0;
        self.hooks.check(HookPointV1::BeforeOutboxWrite, sequence);
        let outcome = self.inner.append_admitted(admitted)?;
        self.hooks
            .check(HookPointV1::AfterOutboxWriteBeforeEnqueue, sequence);
        Ok(outcome)
    }

    fn record_withheld(
        &self,
        withheld: &WithheldAdmissionV1,
    ) -> Result<(), ObservationJournalError> {
        self.inner.record_withheld(withheld)
    }

    fn replay_cursor(
        &self,
        stream: &SourceStreamKeyV1,
    ) -> Result<Option<ReplayCursorV1>, ObservationJournalError> {
        self.inner.replay_cursor(stream)
    }

    fn lane_pressure(
        &self,
        lane: &ObservationLaneKeyV1,
    ) -> Result<QueuePressureV1, ObservationJournalError> {
        self.inner.lane_pressure(lane)
    }
}

/// The delivery side of the journal with the two acknowledgement boundaries
/// armed, on either side of the inner `record_attempt` transaction.
struct CrashingJournalReaderV1<'a> {
    inner: &'a SqliteObservationJournal,
    hooks: &'a HooksV1,
    /// Source position of the row whose receipt is being written, tracked from
    /// the lease so the acknowledgement hooks can be armed per position exactly
    /// like the outbox ones.
    leased_positions: std::sync::Mutex<Vec<(String, u64)>>,
}

impl<'a> CrashingJournalReaderV1<'a> {
    fn new(inner: &'a SqliteObservationJournal, hooks: &'a HooksV1) -> Self {
        Self {
            inner,
            hooks,
            leased_positions: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn position_of(&self, observation_id: &ObservationIdV1) -> u64 {
        match self.leased_positions.lock() {
            Ok(slot) => slot
                .iter()
                .find(|(id, _)| id == observation_id.as_str())
                .map_or(0, |(_, sequence)| *sequence),
            Err(poisoned) => poisoned
                .into_inner()
                .iter()
                .find(|(id, _)| id == observation_id.as_str())
                .map_or(0, |(_, sequence)| *sequence),
        }
    }
}

impl ObservationJournalReaderV1 for CrashingJournalReaderV1<'_> {
    fn lease_pending(
        &self,
        request: &LeaseRequestV1,
    ) -> Result<Vec<LeasedObservationV1>, ObservationJournalError> {
        let leased = self.inner.lease_pending(request)?;
        let mut slot = match self.leased_positions.lock() {
            Ok(slot) => slot,
            Err(poisoned) => poisoned.into_inner(),
        };
        for item in &leased {
            slot.push((
                item.observation_id.as_str().to_owned(),
                item.source_sequence.0,
            ));
        }
        Ok(leased)
    }

    fn record_attempt(
        &self,
        receipt: &ObservationDeliveryReceiptV1,
    ) -> Result<AttemptOutcomeV1, ObservationJournalError> {
        let sequence = self.position_of(&receipt.observation_id);
        self.hooks
            .check(HookPointV1::AfterAckReturnBeforeAckPersist, sequence);
        let outcome = self.inner.record_attempt(receipt)?;
        self.hooks.check(HookPointV1::AfterAckPersist, sequence);
        Ok(outcome)
    }

    fn record_attempt_refusal(
        &self,
        refusal: &AttemptRefusalRecordV1,
    ) -> Result<AttemptRefusalOutcomeV1, ObservationJournalError> {
        self.inner.record_attempt_refusal(refusal)
    }

    fn release_lease(
        &self,
        lease: &DispatchLeaseIdV1,
        retry_after_unix_micros: i64,
    ) -> Result<(), ObservationJournalError> {
        self.inner.release_lease(lease, retry_after_unix_micros)
    }

    fn reap_expired_leases(
        &self,
        now_unix_micros: i64,
        budget: u32,
    ) -> Result<u32, ObservationJournalError> {
        self.inner.reap_expired_leases(now_unix_micros, budget)
    }

    fn inspect(
        &self,
        filter: &JournalInspectionFilterV1,
    ) -> Result<JournalInspectionPageV1, ObservationJournalError> {
        self.inner.inspect(filter)
    }

    fn receipts_for(
        &self,
        observation_id: &ObservationIdV1,
    ) -> Result<Vec<ObservationDeliveryReceiptV1>, ObservationJournalError> {
        self.inner.receipts_for(observation_id)
    }

    fn attempt_refusals_for(
        &self,
        observation_id: &ObservationIdV1,
    ) -> Result<Vec<AttemptRefusalRecordV1>, ObservationJournalError> {
        self.inner.attempt_refusals_for(observation_id)
    }

    fn attempt_orphans_for(
        &self,
        observation_id: &ObservationIdV1,
    ) -> Result<Vec<AttemptOrphanRecordV1>, ObservationJournalError> {
        self.inner.attempt_orphans_for(observation_id)
    }
}

// ---------------------------------------------------------- the host side --

/// Admission that mints the same envelope for a given canonical sequence every
/// time, so a replayed record derives the same content-keyed idempotency key
/// after a restart. `unsettled` marks positions whose canonical transaction
/// rolled back after the observation pipeline had already run for them.
struct ReplayAdmissionV1 {
    lane: ObservationLaneKeyV1,
    unsettled: BTreeSet<u64>,
}

impl ReplayAdmissionV1 {
    fn admitting(lane: ObservationLaneKeyV1) -> Self {
        Self {
            lane,
            unsettled: BTreeSet::new(),
        }
    }

    fn with_unsettled(lane: ObservationLaneKeyV1, sequences: &[u64]) -> Self {
        Self {
            lane,
            unsettled: sequences.iter().copied().collect(),
        }
    }
}

impl ObservationAdmissionAdapterV1 for ReplayAdmissionV1 {
    type Record = ();
    type Error = HarnessError;
    type Control = dyn IngressControlV1;

    fn lane(&self, _record: &SourceRecordV1<Self::Record>) -> ObservationLaneKeyV1 {
        self.lane.clone()
    }

    fn classify(&self, _record: &SourceRecordV1<Self::Record>) -> ObservationLoadClassV1 {
        ObservationLoadClassV1::of(RetentionClassV1::Project)
    }

    fn decide(
        &self,
        record: &SourceRecordV1<Self::Record>,
        _control: &Self::Control,
    ) -> Result<AdmissionDecisionV1, Self::Error> {
        let sequence = record.source_sequence.0;
        let mut admitted = Builder::at_sequence(sequence).build().map_err(harness)?;
        if self.unsettled.contains(&sequence) {
            // The canonical authority never settled this position: its
            // transaction rolled back after the pipeline had already built an
            // envelope for it. The proof digest is therefore absent.
            admitted.source.settlement_proof_sha256 = String::new();
            support::seal(&mut admitted);
        }
        Ok(AdmissionDecisionV1::Admit(Box::new(admitted)))
    }
}

// ------------------------------------------------------------- the journey --

fn dispatch_policy() -> DispatchPolicyV1 {
    DispatchPolicyV1 {
        lease_duration_micros: LEASE,
        batch_max_items: 8,
        batch_max_bytes: 1_048_576,
        attempt_budget_micros: ATTEMPT_BUDGET,
        reap_budget: 16,
        max_rounds_per_drain: 4,
        drain_budget_micros: 30 * SECOND,
    }
}

fn records(sequences: &[u64]) -> Result<Vec<SourceRecordV1<()>>, Box<dyn Error>> {
    sequences
        .iter()
        .copied()
        .map(|sequence| {
            Ok(SourceRecordV1 {
                stream: stream_key(CANONICAL_STREAM)?,
                source_sequence: SourceSequenceV1(sequence),
                source_event_id: format!("event-{sequence}"),
                source_event_revision: 0,
                record: (),
            })
        })
        .collect()
}

/// Where one life's three durable artefacts live.
struct JourneyPathsV1 {
    journal: PathBuf,
    ledger: PathBuf,
    canonical: PathBuf,
    marker: PathBuf,
}

impl JourneyPathsV1 {
    fn in_directory(directory: &Path) -> Self {
        Self {
            journal: directory.join("observation-journal.sqlite3"),
            ledger: directory.join("provider-effects.log"),
            canonical: directory.join("canonical-stream.log"),
            marker: directory.join("hook-arrivals.log"),
        }
    }
}

/// Runs one host life: recover the replay position from the journal, ingest
/// whatever the *canonical store* says has settled, then dispatch.
///
/// `hooks` is armed only in a child process. In the parent this is plain
/// recovery over the same files.
fn live(
    paths: &JourneyPathsV1,
    now: i64,
    hooks: &HooksV1,
) -> Result<DeliveryWakeV1, Box<dyn Error>> {
    let canonical = CanonicalStreamV1::new(paths.canonical.clone());
    // Read from disk, not from an argument. This is the whole point of the
    // before-outbox case: recovery must be able to find a settled position the
    // outbox never saw.
    let settled = canonical.committed()?;
    let ledger = ProviderLedgerV1::new(paths.ledger.clone());

    let store = journal(&paths.journal)?;
    let wake = DeliveryWakeV1::new();
    let backpressure = gate()?;
    let control = TestIngestControl::at(now, DAY);
    let admission = ReplayAdmissionV1::admitting(lane()?);
    let port = CrashingDispatchPortV1 {
        inner: &store,
        hooks,
    };
    let ingress = IngressRuntimeV1::new(&port, &admission, &wake, &backpressure, &control);
    let resume = ingress.recover(&stream_key(CANONICAL_STREAM)?)?;
    ingress.ingest(&resume, &records(&settled)?)?;

    let provider = DurableProviderV1 {
        ledger: &ledger,
        hooks,
        now,
        received: AtomicU32::new(0),
    };
    let reader = CrashingJournalReaderV1::new(&store, hooks);
    let request = DispatchRequestV1 {
        lease: lease_request(now, 8),
        retry_backoff: RetryBackoffV1::of(&policy()),
        attempt_budget_micros: ATTEMPT_BUDGET,
    };
    let bounds = dispatch_policy().drain_bounds(&policy(), now)?;
    let delivery = DeliveryRuntimeV1::new(&reader, &provider, &wake);
    // Reclaiming lapsed leases is what turns a crashed attempt into a durable
    // orphan record, so it runs before the drain rather than as a side effect
    // of one.
    delivery.reap(now, 16)?;
    delivery.drain(&request, &bounds, || now)?;
    Ok(wake)
}

// -------------------------------------------------------- the child process --

/// One child life's whole assignment, encoded for the environment.
struct ChildLifeSpecV1 {
    directory: PathBuf,
    hook: HookPointV1,
    target_sequence: u64,
    now: i64,
    /// Positions the canonical authority settles before the pipeline runs.
    settle: Vec<u64>,
}

impl ChildLifeSpecV1 {
    fn encode(&self) -> String {
        let settle = self
            .settle
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "{}|{}|{}|{}|{settle}",
            self.directory.display(),
            self.hook.as_wire(),
            self.target_sequence,
            self.now,
        )
    }

    fn decode(value: &str) -> Result<Self, Box<dyn Error>> {
        let parts: Vec<&str> = value.split('|').collect();
        let [directory, hook, target, now, settle] = parts.as_slice() else {
            return Err(harness(format!("malformed child life spec {value}")).into());
        };
        Ok(Self {
            directory: PathBuf::from(directory),
            hook: HookPointV1::from_wire(hook)?,
            target_sequence: target.parse()?,
            now: now.parse()?,
            settle: settle
                .split(',')
                .filter(|piece| !piece.is_empty())
                .map(|piece| piece.parse::<u64>().map_err(|error| harness(error).into()))
                .collect::<Result<Vec<u64>, Box<dyn Error>>>()?,
        })
    }
}

/// The child-process entry point.
///
/// Under `cargo test` this is a no-op: the environment carries no assignment,
/// so it passes immediately. When the parent re-executes this binary with
/// `--exact crash_child_process_entrypoint` and the assignment in the
/// environment, this *is* the life that dies — it settles the canonical stream,
/// runs the journey, reaches its armed boundary, and parks there until the
/// parent kills it.
#[test]
fn crash_child_process_entrypoint() -> TestResult {
    let Ok(spec) = std::env::var(CHILD_LIFE_ENV) else {
        return Ok(());
    };
    let spec = ChildLifeSpecV1::decode(&spec)?;
    let paths = JourneyPathsV1::in_directory(&spec.directory);
    let canonical = CanonicalStreamV1::new(paths.canonical.clone());
    // The host's canonical authority commits first and durably. Everything
    // after this point is downstream of a settlement that already survived.
    for sequence in &spec.settle {
        canonical.commit(*sequence)?;
    }
    let hooks = HooksV1::armed_at(spec.hook, spec.target_sequence, paths.marker.clone());
    live(&paths, spec.now, &hooks)?;
    // Reaching here means the armed boundary was never traversed: the life
    // completed instead of dying. The parent sees a clean exit and fails.
    Err(harness(format!(
        "child life completed without reaching {}",
        spec.hook.as_wire()
    ))
    .into())
}

/// Spawns one life as a child process, waits for it to reach its boundary, and
/// kills it there.
///
/// Returns only after the child is confirmed dead by `SIGKILL` and the marker
/// file holds exactly one arrival for the armed boundary.
fn crash_at(spec: &ChildLifeSpecV1) -> Result<(), Box<dyn Error>> {
    let paths = JourneyPathsV1::in_directory(&spec.directory);
    let arrivals_before = marker_arrivals(&paths.marker)?.len();
    let executable = std::env::current_exe()?;
    let mut child = Command::new(executable)
        .args([
            "--exact",
            "crash_child_process_entrypoint",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_LIFE_ENV, spec.encode())
        .stdin(Stdio::null())
        .stdout(Stdio::from(fs::File::create(
            spec.directory.join("child-stdout.log"),
        )?))
        .stderr(Stdio::from(fs::File::create(
            spec.directory.join("child-stderr.log"),
        )?))
        .spawn()?;

    let deadline = Instant::now() + HOOK_ARRIVAL_TIMEOUT;
    loop {
        if marker_arrivals(&paths.marker)?.len() > arrivals_before {
            break;
        }
        if let Some(status) = child.try_wait()? {
            let stderr = fs::read_to_string(spec.directory.join("child-stderr.log"))
                .unwrap_or_else(|_| String::new());
            return Err(harness(format!(
                "the child exited {status} before reaching {}; stderr: {stderr}",
                spec.hook.as_wire()
            ))
            .into());
        }
        if Instant::now() >= deadline {
            child.kill()?;
            child.wait()?;
            return Err(harness(format!(
                "the child never reached {} inside {HOOK_ARRIVAL_TIMEOUT:?}",
                spec.hook.as_wire()
            ))
            .into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    // The boundary is reached and the process is parked on it: kill it where it
    // stands. Nothing is unwound, nothing is flushed, and the SQLite connection
    // is never closed.
    child.kill()?;
    let status = child.wait()?;
    if status.signal() != Some(SIGKILL) {
        return Err(harness(format!(
            "the child at {} did not die on SIGKILL: {status}",
            spec.hook.as_wire()
        ))
        .into());
    }

    let arrivals = marker_arrivals(&paths.marker)?;
    let new: Vec<&String> = arrivals.iter().skip(arrivals_before).collect();
    if new.len() != 1 || new.first().map(|line| line.as_str()) != Some(spec.hook.as_wire()) {
        return Err(harness(format!(
            "expected exactly one arrival at {}, observed {new:?}",
            spec.hook.as_wire()
        ))
        .into());
    }
    Ok(())
}

fn marker_arrivals(path: &Path) -> Result<Vec<String>, Box<dyn Error>> {
    match fs::read_to_string(path) {
        Ok(text) => Ok(text.lines().map(str::to_owned).collect()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error.into()),
    }
}

// ----------------------------------------------------------------- recovery --

fn inspect_all(path: &Path) -> Result<JournalInspectionPageV1, Box<dyn Error>> {
    let store = journal(path)?;
    Ok(store.inspect(&JournalInspectionFilterV1 {
        limit: 256,
        ..JournalInspectionFilterV1::default()
    })?)
}

fn all_acknowledged(path: &Path, expected_rows: usize) -> Result<bool, Box<dyn Error>> {
    let page = inspect_all(path)?;
    Ok(page.rows.len() == expected_rows
        && page.rows.iter().all(|row| {
            matches!(
                row.state,
                DeliveryStateV1::Acknowledged | DeliveryStateV1::DuplicateAcknowledged
            )
        }))
}

/// Restarts the host in this process — a fresh process from the crashed
/// child's point of view — until every canonically settled observation is
/// durably acknowledged.
///
/// The expected stream is never passed in: the row count that has to be reached
/// comes from the canonical store on disk.
fn recover_until_acknowledged(paths: &JourneyPathsV1, start: i64) -> Result<(), Box<dyn Error>> {
    let canonical = CanonicalStreamV1::new(paths.canonical.clone());
    let expected_rows = canonical.committed()?.len();
    let mut now = start;
    for _ in 0..MAX_RECOVERY_LIVES {
        now = now.saturating_add(LEASE + MINUTE);
        live(paths, now, &HooksV1::disarmed())?;
        if all_acknowledged(&paths.journal, expected_rows)? {
            return Ok(());
        }
    }
    Err(harness(format!(
        "the journey never converged: {:?}",
        inspect_all(&paths.journal)?
            .rows
            .iter()
            .map(|row| (row.source_sequence.0, row.state, row.attempt_number))
            .collect::<Vec<_>>()
    ))
    .into())
}

fn expected_keys(committed: &[u64]) -> Result<BTreeSet<String>, Box<dyn Error>> {
    committed
        .iter()
        .copied()
        .map(|sequence| {
            Ok(Builder::at_sequence(sequence)
                .build()?
                .idempotency_key
                .as_str()
                .to_owned())
        })
        .collect()
}

/// What one converged journey's durable artefacts say.
struct JourneyAuditV1 {
    /// Rows that settled through a duplicate acknowledgement.
    duplicate_rows: usize,
    /// Orphaned-attempt records across every row.
    orphaned_attempts: usize,
}

/// The four acceptance invariants, checked against the three durable artefacts
/// the journey leaves behind.
///
/// Every count here is derived from the store, never from a per-fault constant:
/// the caller gets the audit back and asserts the boundary-specific numbers on
/// top.
fn assert_journey_invariants(
    paths: &JourneyPathsV1,
    label: &str,
) -> Result<JourneyAuditV1, Box<dyn Error>> {
    let canonical = CanonicalStreamV1::new(paths.canonical.clone());
    let mut committed = canonical.committed()?;
    committed.sort_unstable();
    let ledger = ProviderLedgerV1::new(paths.ledger.clone());
    let store = journal(&paths.journal)?;
    let page = store.inspect(&JournalInspectionFilterV1 {
        limit: 256,
        ..JournalInspectionFilterV1::default()
    })?;

    // AC1 — no committed observation is lost. "Committed" is the canonical
    // store on disk, so a position the outbox never saw still has to appear.
    assert!(
        !committed.is_empty(),
        "{label}: the canonical store settled nothing, so AC1 would be vacuous"
    );
    let mut sequences: Vec<u64> = page.rows.iter().map(|row| row.source_sequence.0).collect();
    sequences.sort_unstable();
    assert_eq!(
        sequences, committed,
        "{label}: the journal does not hold exactly the canonically settled stream"
    );
    assert_eq!(page.total_rows as usize, committed.len(), "{label}");

    // AC3 — retries create no duplicate provider effects. Counted over the
    // provider's own append-only lines, so a second effect cannot hide.
    let effects = ledger.effects()?;
    let keys = expected_keys(&committed)?;
    assert_eq!(
        effects.len(),
        committed.len(),
        "{label}: the provider committed {} effects for {} observations: {effects:?}",
        effects.len(),
        committed.len()
    );
    assert_eq!(
        effects.iter().cloned().collect::<BTreeSet<_>>(),
        keys,
        "{label}: the provider's effects are not exactly the committed observations"
    );

    // AC4 — every attempt number a row ever spent is accounted for by a durable
    // typed record, and the recovery evidence is a real receipt rather than a
    // reconstruction.
    let mut duplicate_rows = 0_usize;
    let mut orphaned_attempts = 0_usize;
    for row in &page.rows {
        assert!(
            matches!(
                row.state,
                DeliveryStateV1::Acknowledged | DeliveryStateV1::DuplicateAcknowledged
            ),
            "{label}: sequence {} settled {:?}",
            row.source_sequence.0,
            row.state
        );
        let receipts = store.receipts_for(&row.observation_id)?;
        let refusals = store.attempt_refusals_for(&row.observation_id)?;
        let orphans = store.attempt_orphans_for(&row.observation_id)?;
        orphaned_attempts = orphaned_attempts.saturating_add(orphans.len());
        assert!(
            !receipts.is_empty(),
            "{label}: sequence {} is acknowledged with no receipt behind it",
            row.source_sequence.0
        );
        // Attempts are consumed by the lease claim. A receipt or a refusal
        // accounts for an attempt the provider answered; an orphan record
        // accounts for one whose dispatcher died before any answer became
        // durable. The three together must cover the counter exactly — a gap
        // is an attempt nobody can explain, and an excess means a spent
        // attempt number was handed back, which is how two dispatchers end up
        // sharing a receipt slot for one row.
        let accounted = u32::try_from(receipts.len() + refusals.len() + orphans.len())?;
        assert_eq!(
            row.attempt_number,
            accounted,
            "{label}: sequence {} consumed {} attempts against {accounted} durable records \
             ({} receipts, {} refusals, {} orphans)",
            row.source_sequence.0,
            row.attempt_number,
            receipts.len(),
            refusals.len(),
            orphans.len()
        );
        let occupied: Vec<u32> = receipts
            .iter()
            .map(|receipt| receipt.attempt_number)
            .chain(refusals.iter().map(|refusal| refusal.attempt_number))
            .chain(orphans.iter().map(|orphan| orphan.attempt_number))
            .collect();
        let unique: BTreeSet<u32> = occupied.iter().copied().collect();
        assert_eq!(
            unique.len(),
            occupied.len(),
            "{label}: sequence {} has two durable records for one attempt",
            row.source_sequence.0
        );
        assert_eq!(
            unique.into_iter().collect::<Vec<u32>>(),
            (1..=row.attempt_number).collect::<Vec<u32>>(),
            "{label}: sequence {} does not account for every attempt number it spent",
            row.source_sequence.0
        );
        // Each orphan names the exact claim it came from, so the audit says
        // which lease died and on which content, not merely that something did.
        for orphan in &orphans {
            assert_eq!(
                orphan.observation_id, row.observation_id,
                "{label}: an orphan record names another observation"
            );
            assert_eq!(
                orphan.payload_sha256, row.payload_sha256,
                "{label}: an orphan record describes other content"
            );
            assert_eq!(orphan.idempotency_key, row.idempotency_key, "{label}");
            assert_eq!(
                orphan.cause,
                AttemptOrphanCauseV1::LeaseExpiredWithoutAnswer,
                "{label}"
            );
            assert_eq!(
                orphan.recovery,
                AttemptOrphanRecoveryV1::RedeliveryScheduled,
                "{label}: sequence {} was not recovered by redelivery",
                row.source_sequence.0
            );
            assert!(
                !orphan.lease_id.as_str().is_empty(),
                "{label}: an orphan record names no lease"
            );
            assert!(
                orphan.attempt_number < row.attempt_number,
                "{label}: the row's current attempt cannot already be orphaned"
            );
        }
        let last = receipts
            .iter()
            .max_by_key(|receipt| receipt.attempt_number)
            .ok_or_else(|| harness("receipts were non-empty a line ago"))?;
        assert_eq!(
            last.implied_state(),
            row.state,
            "{label}: sequence {} state disagrees with its final receipt",
            row.source_sequence.0
        );
        // Every receipt names the exact bytes the journal holds, so the audit
        // chain ties the provider's answer to the delivered content.
        for receipt in &receipts {
            assert_eq!(
                receipt.payload_sha256, row.payload_sha256,
                "{label}: a receipt describes other content"
            );
            assert_eq!(
                receipt.receipt_id,
                DeliveryReceiptIdV1::derive(&row.observation_id, receipt.attempt_number),
                "{label}: a receipt id is not derived from its own attempt"
            );
        }
        if last.outcome == ObservationOutcomeV1::DuplicateAcknowledged {
            duplicate_rows += 1;
            assert!(
                last.attempt_number > 1,
                "{label}: a first attempt cannot be a duplicate of itself"
            );
        }
    }
    Ok(JourneyAuditV1 {
        duplicate_rows,
        orphaned_attempts,
    })
}

// ------------------------------------------------------------------ faults --

impl HookPointV1 {
    /// How many rows must come back through a duplicate acknowledgement — the
    /// provider had already committed the effect, so the only correct recovery
    /// is a duplicate receipt, never a second effect.
    const fn expected_duplicate_rows(self) -> usize {
        match self {
            // The effect for the killed row landed before the host learned
            // about it; the rest of the batch was still leased and undelivered.
            Self::AfterProviderCommitBeforeAckReturn | Self::AfterAckReturnBeforeAckPersist => 1,
            _ => 0,
        }
    }

    /// How many attempt numbers the crash orphaned. Every row the dying process
    /// held a lease on loses exactly one, and the lease claim is what consumes
    /// it — so this is a function of how far into the batch the life got.
    const fn expected_orphaned_attempts(self, committed: usize) -> usize {
        match self {
            // Nothing had been leased yet.
            Self::BeforeOutboxWrite | Self::AfterOutboxWriteBeforeEnqueue => 0,
            // The whole batch was leased in one claim and the life died on the
            // first row, so every row's attempt died with it.
            Self::BeforeProviderReceive
            | Self::AfterProviderCommitBeforeAckReturn
            | Self::AfterAckReturnBeforeAckPersist => committed,
            // The first row's acknowledgement was already durable, so only the
            // rows still leased behind it lost an attempt.
            Self::AfterAckPersist => committed.saturating_sub(1),
        }
    }
}

/// What the durable artefacts must look like the instant after the kill.
///
/// This is what makes each hook load-bearing: a hook that moved to the other
/// side of its transaction, or that never fired, leaves a different shape here.
fn assert_durable_state_after_crash(
    paths: &JourneyPathsV1,
    hook: HookPointV1,
    committed: &[u64],
) -> Result<(), Box<dyn Error>> {
    let label = hook.as_wire();
    let canonical = CanonicalStreamV1::new(paths.canonical.clone());
    let mut settled = canonical.committed()?;
    settled.sort_unstable();
    assert_eq!(
        settled, committed,
        "{label}: the canonical store did not retain every settled position"
    );

    let store = journal(&paths.journal)?;
    let page = store.inspect(&JournalInspectionFilterV1 {
        limit: 256,
        ..JournalInspectionFilterV1::default()
    })?;
    let mut rows: Vec<(u64, DeliveryStateV1, u32)> = page
        .rows
        .iter()
        .map(|row| (row.source_sequence.0, row.state, row.attempt_number))
        .collect();
    rows.sort_by_key(|(sequence, _, _)| *sequence);
    let ledger = ProviderLedgerV1::new(paths.ledger.clone());
    let effects = ledger.effects()?;
    let first_key = Builder::at_sequence(
        *committed
            .first()
            .ok_or_else(|| harness("no committed positions"))?,
    )
    .build()?
    .idempotency_key
    .as_str()
    .to_owned();

    match hook {
        HookPointV1::BeforeOutboxWrite => {
            // The last settled position never reached the outbox.
            let expected: Vec<(u64, DeliveryStateV1, u32)> = committed[..committed.len() - 1]
                .iter()
                .map(|sequence| (*sequence, DeliveryStateV1::Pending, 0))
                .collect();
            assert_eq!(rows, expected, "{label}: unexpected outbox state");
            assert!(effects.is_empty(), "{label}: the provider was reached");
        }
        HookPointV1::AfterOutboxWriteBeforeEnqueue => {
            // Every append committed; nothing was ever dispatched.
            let expected: Vec<(u64, DeliveryStateV1, u32)> = committed
                .iter()
                .map(|sequence| (*sequence, DeliveryStateV1::Pending, 0))
                .collect();
            assert_eq!(rows, expected, "{label}: unexpected outbox state");
            assert!(effects.is_empty(), "{label}: the provider was reached");
        }
        HookPointV1::BeforeProviderReceive => {
            let expected: Vec<(u64, DeliveryStateV1, u32)> = committed
                .iter()
                .map(|sequence| (*sequence, DeliveryStateV1::Leased, 1))
                .collect();
            assert_eq!(rows, expected, "{label}: unexpected lease state");
            assert!(
                effects.is_empty(),
                "{label}: the provider committed an effect it never received: {effects:?}"
            );
        }
        HookPointV1::AfterProviderCommitBeforeAckReturn
        | HookPointV1::AfterAckReturnBeforeAckPersist => {
            let expected: Vec<(u64, DeliveryStateV1, u32)> = committed
                .iter()
                .map(|sequence| (*sequence, DeliveryStateV1::Leased, 1))
                .collect();
            assert_eq!(rows, expected, "{label}: unexpected lease state");
            assert_eq!(
                effects,
                vec![first_key],
                "{label}: the provider's committed effect is not the one the host lost"
            );
            for row in &page.rows {
                assert!(
                    store.receipts_for(&row.observation_id)?.is_empty(),
                    "{label}: an acknowledgement survived a crash before it was persisted"
                );
            }
        }
        HookPointV1::AfterAckPersist => {
            let mut expected: Vec<(u64, DeliveryStateV1, u32)> = committed
                .iter()
                .skip(1)
                .map(|sequence| (*sequence, DeliveryStateV1::Leased, 1))
                .collect();
            expected.insert(
                0,
                (
                    *committed
                        .first()
                        .ok_or_else(|| harness("no committed positions"))?,
                    DeliveryStateV1::Acknowledged,
                    1,
                ),
            );
            assert_eq!(
                rows, expected,
                "{label}: unexpected post-acknowledgement state"
            );
            assert_eq!(effects, vec![first_key], "{label}");
            let acknowledged = page
                .rows
                .iter()
                .find(|row| row.state == DeliveryStateV1::Acknowledged)
                .ok_or_else(|| harness("no acknowledged row survived the crash"))?;
            assert_eq!(
                store.receipts_for(&acknowledged.observation_id)?.len(),
                1,
                "{label}: the acknowledgement did not survive the kill"
            );
        }
    }
    Ok(())
}

// ------------------------------------------------------------------- tests --

#[test]
fn every_boundary_from_host_commit_to_ack_persistence_is_crash_safe() -> TestResult {
    let committed = vec![1_u64, 2, 3];
    for hook in HookPointV1::ALL {
        let label = hook.as_wire();
        let directory = tempfile::tempdir()?;
        let paths = JourneyPathsV1::in_directory(directory.path());

        // ---- a real process, killed at a real boundary ----
        crash_at(&ChildLifeSpecV1 {
            directory: directory.path().to_path_buf(),
            hook,
            target_sequence: hook
                .target_sequence(&committed)
                .ok_or_else(|| harness("no target sequence"))?,
            now: T0,
            settle: committed.clone(),
        })?;
        assert_durable_state_after_crash(&paths, hook, &committed)?;

        // ---- a fresh process replays the canonical store and converges ----
        recover_until_acknowledged(&paths, T0)?;
        let audit = assert_journey_invariants(&paths, label)?;
        assert_eq!(
            audit.duplicate_rows,
            hook.expected_duplicate_rows(),
            "{label}: wrong number of rows recovered through a duplicate acknowledgement"
        );
        assert_eq!(
            audit.orphaned_attempts,
            hook.expected_orphaned_attempts(committed.len()),
            "{label}: wrong number of orphaned attempts recorded"
        );
    }
    Ok(())
}

#[test]
fn a_committed_observation_the_outbox_never_saw_is_recovered_from_the_canonical_store() -> TestResult
{
    // AC1 at its sharpest, and the one case a memory-only harness cannot make:
    // the host settled sequence 3 canonically and died before the outbox write.
    // Nothing in the journal, nothing in the provider ledger, and nothing in
    // this process knows about it — only the canonical file on disk does.
    let committed = vec![1_u64, 2, 3];
    let directory = tempfile::tempdir()?;
    let paths = JourneyPathsV1::in_directory(directory.path());

    crash_at(&ChildLifeSpecV1 {
        directory: directory.path().to_path_buf(),
        hook: HookPointV1::BeforeOutboxWrite,
        target_sequence: 3,
        now: T0,
        settle: committed.clone(),
    })?;

    let before = inspect_all(&paths.journal)?;
    let mut journalled: Vec<u64> = before
        .rows
        .iter()
        .map(|row| row.source_sequence.0)
        .collect();
    journalled.sort_unstable();
    assert_eq!(
        journalled,
        vec![1, 2],
        "the outbox must not hold the position the host died before writing"
    );
    assert_eq!(
        CanonicalStreamV1::new(paths.canonical.clone()).committed()?,
        committed,
        "the canonical store must hold every settled position"
    );

    recover_until_acknowledged(&paths, T0)?;
    let audit = assert_journey_invariants(&paths, "before-outbox")?;
    assert_eq!(audit.duplicate_rows, 0);
    assert_eq!(audit.orphaned_attempts, 0);
    Ok(())
}

#[test]
fn a_lost_acknowledgement_recovers_as_a_duplicate_receipt_not_a_second_effect() -> TestResult {
    // The single most dangerous window: the provider committed and the host
    // died before it learned. The only safe recovery is a redelivery the
    // provider recognises, and the only honest record of the lost attempt is an
    // orphan record plus a duplicate receipt under the next attempt number.
    let committed = vec![1_u64];
    let directory = tempfile::tempdir()?;
    let paths = JourneyPathsV1::in_directory(directory.path());

    crash_at(&ChildLifeSpecV1 {
        directory: directory.path().to_path_buf(),
        hook: HookPointV1::AfterProviderCommitBeforeAckReturn,
        target_sequence: 1,
        now: T0,
        settle: committed.clone(),
    })?;

    let ledger = ProviderLedgerV1::new(paths.ledger.clone());
    assert_eq!(ledger.effects()?.len(), 1, "the provider did commit");
    assert!(
        !all_acknowledged(&paths.journal, 1)?,
        "the host must not believe a delivery it never heard about"
    );

    recover_until_acknowledged(&paths, T0)?;

    let store = journal(&paths.journal)?;
    let page = store.inspect(&JournalInspectionFilterV1 {
        limit: 8,
        ..JournalInspectionFilterV1::default()
    })?;
    let row = page
        .rows
        .first()
        .ok_or_else(|| harness("no delivery row"))?;
    assert_eq!(row.state, DeliveryStateV1::DuplicateAcknowledged);
    assert_eq!(row.attempt_number, 2);
    let receipts = store.receipts_for(&row.observation_id)?;
    assert_eq!(
        receipts.len(),
        1,
        "the lost attempt wrote no receipt, and the recovery attempt wrote one"
    );
    assert_eq!(receipts[0].attempt_number, 2);
    assert_eq!(
        receipts[0].outcome,
        ObservationOutcomeV1::DuplicateAcknowledged
    );
    let orphans = store.attempt_orphans_for(&row.observation_id)?;
    assert_eq!(
        orphans.len(),
        1,
        "the attempt the crash consumed has no durable explanation"
    );
    assert_eq!(orphans[0].attempt_number, 1);
    assert_eq!(
        orphans[0].cause,
        AttemptOrphanCauseV1::LeaseExpiredWithoutAnswer
    );
    assert_eq!(
        orphans[0].recovery,
        AttemptOrphanRecoveryV1::RedeliveryScheduled
    );
    assert_eq!(orphans[0].payload_sha256, row.payload_sha256);
    assert_eq!(
        ledger.effects()?.len(),
        1,
        "the effect was not applied twice"
    );
    Ok(())
}

#[test]
fn a_rolled_back_source_is_never_journalled_delivered_or_skipped() -> TestResult {
    // AC2. A canonical transaction that rolls back leaves an envelope with no
    // settlement proof behind it. The append refuses it, the watermark holds in
    // front of it, no provider ever sees it, and the stream still converges on
    // the positions that really did settle.
    let directory = tempfile::tempdir()?;
    let paths = JourneyPathsV1::in_directory(directory.path());
    let ledger = ProviderLedgerV1::new(paths.ledger.clone());
    let store = journal(&paths.journal)?;
    let wake = DeliveryWakeV1::new();
    let backpressure = gate()?;
    let control = TestIngestControl::at(T0, DAY);
    let admission = ReplayAdmissionV1::with_unsettled(lane()?, &[2]);
    let hooks = HooksV1::disarmed();
    let port = CrashingDispatchPortV1 {
        inner: &store,
        hooks: &hooks,
    };
    let ingress = IngressRuntimeV1::new(&port, &admission, &wake, &backpressure, &control);
    let resume = ingress.recover(&stream_key(CANONICAL_STREAM)?)?;

    let error = ingress
        .ingest(&resume, &records(&[1, 2, 3])?)
        .err()
        .ok_or_else(|| harness("an unsettled source was admitted"))?;
    assert!(
        matches!(
            error,
            tracedecay_memory_observation::ObservationRuntimeError::Journal(
                ObservationJournalError::UnsettledSource {
                    field: "settlement_proof_sha256"
                }
            )
        ),
        "unexpected refusal: {error}"
    );

    // The watermark stopped in front of the rolled-back position, so nothing
    // after it was silently skipped.
    let cursor = store
        .replay_cursor(&stream_key(CANONICAL_STREAM)?)?
        .ok_or_else(|| harness("no replay cursor"))?;
    assert_eq!(cursor.last_admitted_sequence, SourceSequenceV1(1));
    assert_eq!(store.inspect(&Default::default())?.total_rows, 1);

    // Delivery only ever sees what settled.
    let provider = DurableProviderV1 {
        ledger: &ledger,
        hooks: &hooks,
        now: T0,
        received: AtomicU32::new(0),
    };
    let delivery = DeliveryRuntimeV1::new(&store, &provider, &wake);
    delivery.drain(
        &DispatchRequestV1 {
            lease: lease_request(T0, 8),
            retry_backoff: RetryBackoffV1::of(&policy()),
            attempt_budget_micros: ATTEMPT_BUDGET,
        },
        &dispatch_policy().drain_bounds(&policy(), T0)?,
        || T0,
    )?;
    let rolled_back_key = {
        let mut admitted = Builder::at_sequence(2).build()?;
        admitted.source.settlement_proof_sha256 = String::new();
        support::seal(&mut admitted);
        admitted.idempotency_key.as_str().to_owned()
    };
    let effects = ledger.effects()?;
    assert_eq!(effects.len(), 1);
    assert!(
        !effects.iter().any(|line| line == &rolled_back_key),
        "a rolled-back observation reached the provider"
    );

    // The authority re-presents the positions that really settled. Sequence 2
    // is gone for good; sequence 3 still lands.
    let resume = ingress.recover(&stream_key(CANONICAL_STREAM)?)?;
    ingress.ingest(&resume, &records(&[1, 3])?)?;
    let page = store.inspect(&JournalInspectionFilterV1 {
        limit: 8,
        ..JournalInspectionFilterV1::default()
    })?;
    let mut sequences: Vec<u64> = page.rows.iter().map(|row| row.source_sequence.0).collect();
    sequences.sort_unstable();
    assert_eq!(sequences, vec![1, 3]);
    assert_eq!(
        store
            .replay_cursor(&stream_key(CANONICAL_STREAM)?)?
            .ok_or_else(|| harness("no replay cursor"))?
            .last_admitted_sequence,
        SourceSequenceV1(3)
    );
    Ok(())
}

#[test]
fn repeated_crashes_never_double_apply_and_never_lose_the_stream() -> TestResult {
    // Two real process deaths per journey. The first kills the host part-way
    // through the outbox write, the second at the seed's own boundary, and the
    // journey has to converge from both at once.
    let committed = vec![1_u64, 2, 3, 4];
    for (seed, hook) in HookPointV1::ALL.into_iter().enumerate() {
        let label = format!("seed-{seed}-{}", hook.as_wire());
        let directory = tempfile::tempdir()?;
        let paths = JourneyPathsV1::in_directory(directory.path());

        // First life: the canonical store settles everything, the outbox gets
        // part of it, and the host dies mid-append.
        let partial = (seed % (committed.len() - 1)) + 1;
        crash_at(&ChildLifeSpecV1 {
            directory: directory.path().to_path_buf(),
            hook: HookPointV1::AfterOutboxWriteBeforeEnqueue,
            target_sequence: committed[partial - 1],
            now: T0,
            settle: committed.clone(),
        })?;
        let after_first = inspect_all(&paths.journal)?;
        assert_eq!(
            after_first.rows.len(),
            partial,
            "{label}: the first life did not stop where its hook is"
        );

        // Second life: the same canonical store, killed at the seed's boundary.
        // The append hooks are armed on the last settled position, which the
        // first life may already have written — arm them on a position that is
        // still missing so the boundary is genuinely reachable.
        let second_target = match hook {
            HookPointV1::BeforeOutboxWrite | HookPointV1::AfterOutboxWriteBeforeEnqueue => {
                if partial == committed.len() {
                    // Nothing left to append: this seed's boundary is spent, so
                    // recover from the single crash instead of forcing a second.
                    recover_until_acknowledged(&paths, T0 + LEASE + MINUTE)?;
                    let audit = assert_journey_invariants(&paths, &label)?;
                    assert_eq!(audit.duplicate_rows, 0, "{label}");
                    continue;
                }
                committed[partial]
            }
            _ => committed[0],
        };
        crash_at(&ChildLifeSpecV1 {
            directory: directory.path().to_path_buf(),
            hook,
            target_sequence: second_target,
            now: T0 + LEASE + MINUTE,
            settle: committed.clone(),
        })?;

        recover_until_acknowledged(&paths, T0 + 2 * (LEASE + MINUTE))?;

        // The strong, seed-independent claims: the journal holds the canonical
        // stream exactly once, the provider applied each mutation exactly once,
        // and every attempt number either journey spent has a durable record.
        assert_journey_invariants(&paths, &label)?;
    }
    Ok(())
}
