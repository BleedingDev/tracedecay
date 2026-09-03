//! Seeded crash and restart fuzzing of the **mounted** observation journey
//! (`tdmem-5lc`).
//!
//! # What is under test, and why it is this mount
//!
//! The sibling driver in `tracedecay-memory-observation` kills the runtime
//! seam. What it cannot reach is the thing the product actually ships: the
//! journey the composition root mounts — [`mount_and_replay`] over
//! [`ProjectMemoryProviderComposition`], the registry delivery adapter, the
//! supervised readiness handshake, the recovery gate, the sanitizer, the
//! bounded provider call, and the live replay task. Every life here runs that
//! journey. Nothing in this file composes a runtime by hand, and the whole
//! module compiles only under `--features memory-provider-host`, which is the
//! build the mount exists in.
//!
//! # Where a life dies, and why no production hook exists for it
//!
//! Process death is injected at the two seams the *composition root itself*
//! hands the journey — the canonical observation store it replays from, and
//! the Native application port the provider composition dispatches to. Both
//! are production arguments, not test hooks: the mount takes an
//! `ObservationAdmissionPort` by value and a `NativeMemoryApplicationPort`
//! inside the composition, exactly as the daemon does. The journey itself is
//! untouched, and no `#[cfg(test)]` seam is compiled into it.
//!
//! | boundary the bead names | where this driver stands | what the parent proves after the kill |
//! | --- | --- | --- |
//! | canonical commit → journal write | the store parks on the pass that would serve the target | the canonical log holds it and the journal has no row for it |
//! | journal write → dispatch | the provider parks inside the **delivery preflight handshake**, which the host performs before it builds the observation call | the journal row is durable and this life's provider-entry log never named the row's key |
//! | dispatch → provider effect | the provider parks on entry, after it has durably recorded that it was entered and before it touches its ledger | the entry log names the key and the ledger did not grow in this life |
//! | provider effect → answer exists | the provider parks after its effect is fsync'd and before the terminal is built | the ledger holds the key and the sealed-answer log does not |
//! | answer handed back → receipt persisted | the provider seals its answer, takes the journal's own write lock on a sentinel thread, hands the answer back, and is killed while the host cannot reach the journal | the sealed-answer log and the ledger hold the key and the journal holds no acknowledging receipt for it |
//! | acknowledgement persisted | the store parks on the first replay pass after the target's acknowledging receipt is in the journal | receipt *and* acknowledged watermark are both durable |
//!
//! The fifth row is how this driver reaches the bead's "after reply before
//! receipt persist" on the **mounted** path. Everything the host runs between
//! the provider frame returning and `record_attempt` — the bounded-call
//! handover, `verify_terminal`, `ObservationDeliveryReceiptV1::from_terminal` —
//! is pure host code with no injectable port, so there is no legitimate place
//! to park inside it. What *is* legitimate is to make the window wide and
//! deterministic from outside: the provider takes a `BEGIN IMMEDIATE`
//! transaction on the journal database from its own connection before it hands
//! the answer back, so the host's receipt write cannot commit while that
//! sentinel lives, and only then does the sentinel tell the parent to kill.
//! The kill therefore lands with the answer already handed back and the
//! journal provably still without it — which is exactly the window — and the
//! parent verifies both halves from the durable files rather than trusting the
//! ordering.
//!
//! The last row is the honest form of the bead's "after receipt before
//! watermark advance": the host writes the receipt and the acknowledged
//! watermark in **one** transaction ([`advance_watermark_for`] runs inside the
//! receipt's own transaction), so there is no instant between them to stand
//! in. What can be proved instead — and is — is that a life killed after the
//! answer returned leaves either both or neither.
//!
//! # No sleeps, and no marker polling
//!
//! The parent blocks on a socket the child attaches before its journey starts.
//! One byte means "fsync'd my arrival and parked on the boundary"; end of
//! stream means "died without reaching it". Both ends of that blocking read are
//! terminal, and `recv_timeout` bounds it with a deadline rather than a poll
//! interval. The child parks with [`std::thread::park`], never a timed sleep,
//! so no timing assumption can leak into where the kill lands.
//!
//! The one bounded wait that remains is convergence *after* recovery: the
//! delivery worker is asynchronous and durable, so the parent re-reads the
//! journal until it settles or the budget is spent. That is a liveness bound on
//! a real worker, not a synchronisation trick — nothing is assumed from the
//! passage of time, and a spent budget is a failure that prints the journal.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write as _};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use super::*;

// ------------------------------------------------------------- fixed shape --

/// Carries one child life's whole assignment. Present exactly when this
/// process *is* a child life.
const CHILD_LIFE_ENV: &str = "TDMEM_5LC_MOUNTED_CHILD_LIFE";

/// Raises or lowers the developer tier's seed window. Lowering it to `1`
/// together with [`BASE_SEED_ENV`] reproduces exactly one failure.
const SEEDS_ENV: &str = "TDMEM_5LC_MOUNTED_SEEDS";

/// Raises or lowers the soak tier's seed window.
const SOAK_SEEDS_ENV: &str = "TDMEM_5LC_SOAK_SEEDS";

/// How many seeds the soak tier keeps in flight at once.
const SOAK_LANES_ENV: &str = "TDMEM_5LC_SOAK_LANES";

/// The soak tier's wall-clock ceiling, in seconds.
const SOAK_BUDGET_ENV: &str = "TDMEM_5LC_SOAK_BUDGET_SECONDS";

/// Moves the start of the seed window, for reproducing one seed.
const BASE_SEED_ENV: &str = "TDMEM_5LC_MOUNTED_BASE_SEED";

/// Seeds the developer tier covers.
///
/// Every seed spends one to three **real process kills**, and each killed life
/// is a full mounted journey in a fresh process: a project database, a SQLite
/// journal, a readiness handshake and a live replay task. That costs about a
/// second per life, so this window is the largest one that keeps a bare
/// `cargo test` honest for a developer. It is *not* the gate: the soak tier
/// below runs [`SOAK_SEEDS`] seeds against the same mounted path and is the
/// command the convergence map registers.
const DEFAULT_SEEDS: u64 = 12;

/// Seeds the soak tier covers. The bead asks for hundreds against the real
/// mounted path, and this is that number.
const SOAK_SEEDS: u64 = 200;

/// The floor the registered gate is asserted to have, checked at compile time
/// so lowering [`SOAK_SEEDS`] cannot quietly turn the soak tier into a short
/// one.
const _: () = assert!(SOAK_SEEDS >= 200);

/// Seeds the soak tier keeps in flight when the machine does not say otherwise.
const SOAK_LANES: usize = 6;

/// The soak tier's wall-clock ceiling. A window that cannot finish inside it
/// fails with the seeds it never reached, so the tier can never quietly become
/// a shorter one.
const SOAK_BUDGET: Duration = Duration::from_secs(1_800);

/// Where the seed windows start. Fixed forever: moving it would silently
/// retire the coverage these tiers are asserted to have. The soak window is a
/// superset of the developer window, so a developer failure is always inside
/// the gate too.
const BASE_SEED: u64 = 0x0000_5FC1_0000_0001;

/// Seeds that must always run, whatever the budget is. A seed lands here when
/// it once found a defect. It is empty while no seed has.
const REGRESSION_SEEDS: &[u64] = &[];

/// Kills at a delivery boundary one journey will spend. The retention policy
/// allows eight attempts and each killed lease consumes one, so this leaves
/// the row a comfortable margin to settle in: a row that terminalised as
/// `exhausted` would be the policy working, not a crash-safety defect.
const MAX_DELIVERY_KILLS: usize = 3;

const PROJECT: &str = "project.observation-crash-fuzz";
const PROFILE: &str = "profile.observation-crash-fuzz";
const SESSION: &str = "session.observation-crash-fuzz";

/// The registration revision [`composition`] registers the port under, and the
/// one the acknowledged watermark is keyed by.
const REGISTRATION_REVISION: u64 = 1;

/// How long the parent blocks for the child to attach its signalling socket.
const CHILD_ATTACH_TIMEOUT: Duration = Duration::from_secs(60);

/// How long the parent blocks for the child's arrival signal before calling the
/// boundary unreachable.
const HOOK_ARRIVAL_TIMEOUT: Duration = Duration::from_secs(120);

/// How long recovery may run before the journal is reported instead of a bare
/// deadline.
const CONVERGENCE_BUDGET: Duration = Duration::from_secs(60);

/// How often recovery re-reads the durable journal while it converges.
const CONVERGENCE_REREAD: Duration = Duration::from_millis(25);

/// How long the answer-handback sentinel waits for the journal's write lock
/// before it reports the boundary unverified instead of hanging the child.
const RECEIPT_LOCK_TIMEOUT: Duration = Duration::from_secs(10);

/// The only exit status a crashed life may have.
const SIGKILL: i32 = 9;

/// The child life's own test path in this binary. The parent re-executes the
/// test binary with `--exact` on it, so a rename that missed this constant
/// fails loudly with "child never attached" rather than silently running
/// nothing.
const CHILD_TEST_PATH: &str = "daemon::retained_owner::observation_journey::tests::\
crash_restart_fuzz::mounted_crash_child_process_entrypoint";

/// States a row may never be left in once the journey has converged.
fn is_settled(state: &str) -> bool {
    matches!(
        state,
        "acknowledged" | "duplicate_acknowledged" | "rejected"
    )
}

/// Receipt outcomes that *are* an acknowledgement of a provider effect, and so
/// the only ones that may move the acknowledged watermark.
fn is_acknowledging(outcome: &str) -> bool {
    matches!(
        outcome,
        "applied" | "partial_effect" | "duplicate_acknowledged"
    )
}

/// The bounds every mounted journey in this module runs under.
///
/// Only the dispatch lease is narrowed, and it is narrowed because of what is
/// being tested rather than to make a test pass: a life killed mid-attempt
/// leaves a lease that nothing but its own expiry can reclaim, so the
/// production thirty-second lease would make every delivery-boundary seed cost
/// thirty seconds of wall clock. Two seconds is still far longer than any
/// attempt here, and the policy is validated by the mount exactly like the
/// production one — the composition root passes its own validated policy too.
fn fuzz_policy() -> ObservationJourneyPolicyV1 {
    let mut policy = ObservationJourneyPolicyV1::project_default();
    policy.dispatch.lease_duration_micros = 2_000_000;
    policy.dispatch.attempt_budget_micros = 2_000_000;
    policy.dispatch.drain_budget_micros = 2_000_000;
    policy
}

// ------------------------------------------------------------------ layout --

/// The durable artefacts one journey owns. Everything a restarted life knows
/// is in one of these files; nothing crosses a process boundary in memory.
struct JourneyPathsV1 {
    /// Where the mount puts the journal, i.e. the journey's `store_data_root`.
    root: PathBuf,
    journal: PathBuf,
    /// The host's canonical authority: positions it has settled.
    canonical: PathBuf,
    /// The provider's own durable effects, one line per committed key.
    ledger: PathBuf,
    /// Keys whose answer the provider fully built and sealed before handing it
    /// back to the host.
    answers: PathBuf,
    /// Positions the provider refuses permanently.
    refusals: PathBuf,
    /// Boundary arrivals, appended and fsync'd by the life that reaches one.
    marker: PathBuf,
    /// The directory every per-life artefact is derived from.
    directory: PathBuf,
}

impl JourneyPathsV1 {
    fn in_directory(directory: &Path) -> Self {
        let root = directory.join("journey");
        Self {
            journal: root.join(JOURNAL_FILE_NAME),
            root,
            canonical: directory.join("canonical-stream.log"),
            ledger: directory.join("provider-effects.log"),
            answers: directory.join("provider-answers.log"),
            refusals: directory.join("provider-refusals.log"),
            marker: directory.join("hook-arrivals.log"),
            directory: directory.to_path_buf(),
        }
    }

    /// Where **one life** records that the provider was entered, and the key it
    /// was entered for.
    ///
    /// Per life on purpose: "the provider was never asked about this row" is a
    /// claim about *this* process, and a log shared with the lives before it
    /// could only ever answer the weaker question of whether any life ever
    /// asked.
    fn entries_for(&self, life: &str) -> PathBuf {
        self.directory.join(format!("provider-entries-{life}.log"))
    }
}

/// Appends one fsync'd line, creating the file if this is the first.
fn append_line(path: &Path, line: &str) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("durable append");
    writeln!(file, "{line}").expect("durable write");
    file.sync_all().expect("durable fsync");
}

fn read_lines(path: &Path) -> Vec<String> {
    match fs::read_to_string(path) {
        Ok(text) => text.lines().map(str::to_owned).collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => panic!("cannot read {}: {error}", path.display()),
    }
}

/// How many lines of `path` are exactly `needle`.
fn line_count(path: &Path, needle: &str) -> usize {
    read_lines(path)
        .iter()
        .filter(|line| *line == needle)
        .count()
}

// ------------------------------------------------------ the canonical store --

/// The host's canonical authority, durable and upstream of the journal.
///
/// A restarted life learns what to replay by reading this file back; the
/// expected stream is never handed to recovery as an argument, which is what
/// makes the "committed, never journalled" case a real recovery rather than a
/// replay of something the test still held.
struct CanonicalStreamV1 {
    path: PathBuf,
}

impl CanonicalStreamV1 {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn commit(&self, sequence: u64) {
        if self.committed().contains(&sequence) {
            return;
        }
        append_line(&self.path, &sequence.to_string());
    }

    fn committed(&self) -> Vec<u64> {
        let mut positions: Vec<u64> = read_lines(&self.path)
            .iter()
            .map(|line| line.parse::<u64>().expect("canonical position"))
            .collect();
        positions.sort_unstable();
        positions.dedup();
        positions
    }
}

/// The text one canonical position carries, delimited on both sides so the
/// provider can recover the position from the payload it is handed without a
/// prefix of one number matching another.
fn observation_text(sequence: u64) -> String {
    format!("crash-fuzz[{sequence}]observation")
}

/// The settled canonical record for one position, rebuilt identically in every
/// process from the position alone.
fn canonical_record(sequence: u64) -> StoredObservation {
    let project_id = ProjectId::new(PROJECT).expect("project id");
    let session_id = SessionId::new(SESSION).expect("session id");
    settled_record(
        sequence,
        canonical_observation_at(
            &project_id,
            &session_id,
            &observation_text(sequence),
            sequence,
        ),
    )
}

/// The canonical store the mount replays from, over the positions the
/// authority has settled — and the place two of the six boundaries stand.
struct FuzzCanonicalPortV1 {
    records: Vec<StoredObservation>,
    hooks: Arc<HooksV1>,
    /// Read only to prove, at the instant of arrival, that the boundary is
    /// where it says it is.
    journal: PathBuf,
}

impl FuzzCanonicalPortV1 {
    fn over(paths: &JourneyPathsV1, committed: &[u64], hooks: Arc<HooksV1>) -> Self {
        Self {
            records: committed.iter().copied().map(canonical_record).collect(),
            hooks,
            journal: paths.journal.clone(),
        }
    }

    /// Whether the journal already holds a row for one canonical position.
    fn journalled(&self, sequence: u64) -> bool {
        idempotency_key(&self.journal, sequence).is_some()
    }
}

impl ObservationAdmissionPort for FuzzCanonicalPortV1 {
    async fn read_admitted_observation(
        &self,
        observation_id: &CanonicalObservationIdV1,
    ) -> Result<Option<StoredObservation>, ObservationStoreError> {
        Ok(self
            .records
            .iter()
            .find(|record| record.observation().observation_id() == observation_id)
            .cloned())
    }

    async fn replay_admitted_observations(
        &self,
        request: ObservationReplayRequest,
    ) -> Result<Vec<StoredObservation>, ObservationStoreError> {
        let page: Vec<StoredObservation> = self
            .records
            .iter()
            .filter(|record| record.sequence() > request.after_sequence())
            .take(request.limit())
            .cloned()
            .collect();
        // The host has durably acknowledged the target: the receipt is in the
        // journal. That the watermark went with it is proved rather than
        // assumed, because the two are written in one transaction and a life
        // that finds one without the other has found the defect this boundary
        // exists to catch.
        self.hooks.check_verified(
            HookPointV1::AfterAckPersisted,
            |target| acknowledging_receipt_exists(&self.journal, target),
            |target| acknowledgement_is_durable(&self.journal, target),
        );
        // The target is the next record this pass would admit, and it is still
        // only in the canonical store: dying here is dying between the host's
        // commit and the journal write.
        self.hooks.check_verified(
            HookPointV1::BeforeJournalWrite,
            |target| page.first().map(StoredObservation::sequence) == Some(target),
            |target| !self.journalled(target),
        );
        Ok(page)
    }
}

// ------------------------------------------------------------ the provider --

/// The provider's durable effects.
///
/// One appended, fsync'd line per committed effect, deduplicated by reading the
/// file back. A second effect for one mutation is therefore a second *line*
/// rather than something a set quietly absorbs, which is what lets "never
/// applied twice" be checked by counting.
struct ProviderLedgerV1 {
    path: PathBuf,
}

impl ProviderLedgerV1 {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn effects(&self) -> Vec<String> {
        read_lines(&self.path)
    }

    fn holds(&self, key: &str) -> bool {
        self.effects().iter().any(|line| line == key)
    }

    /// Commits one effect durably. The provider commits *before* it answers,
    /// which is what makes "the answer was lost" and "the provider never saw
    /// it" two genuinely different faults.
    fn commit(&self, key: &str) {
        append_line(&self.path, key);
    }
}

/// Positions the provider refuses forever, durable so every life of one
/// journey refuses the same ones.
fn refused_positions(path: &Path) -> BTreeSet<u64> {
    read_lines(path)
        .iter()
        .map(|line| line.parse::<u64>().expect("refused position"))
        .collect()
}

fn write_refusals(path: &Path, positions: &BTreeSet<u64>) {
    let mut file = fs::File::create(path).expect("refusal policy");
    for position in positions {
        writeln!(file, "{position}").expect("refusal policy write");
    }
    file.sync_all().expect("refusal policy fsync");
}

/// The Native application port the composition dispatches to: a provider with
/// a durable, deduplicating ledger, a permanent refusal set, and four of the
/// six boundaries.
struct FuzzNativePortV1 {
    descriptor: ProviderDescriptor,
    ledger: ProviderLedgerV1,
    refused: BTreeSet<u64>,
    hooks: Arc<HooksV1>,
    journal: PathBuf,
    answers: PathBuf,
    /// This life's own record of every provider entry, keyed by the host's
    /// idempotency key.
    entries: PathBuf,
}

impl FuzzNativePortV1 {
    fn new(paths: &JourneyPathsV1, hooks: Arc<HooksV1>, entries: PathBuf) -> Self {
        let capabilities = BTreeSet::from([
            OwnedVersionedId::new("provider.health.v1").expect("health capability"),
            OwnedVersionedId::new("observation.accept.v1").expect("observe capability"),
            OwnedVersionedId::new("recall.query.v1").expect("recall capability"),
        ]);
        let descriptor = ProviderDescriptor::new(
            OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("native provider"),
            "0".repeat(64),
            "crash-fuzz-v1",
            0,
            capabilities,
            crate::daemon::retained_owner::native_provider::native_provider_limits(),
        )
        .expect("provider descriptor");
        Self {
            descriptor,
            ledger: ProviderLedgerV1::new(paths.ledger.clone()),
            refused: refused_positions(&paths.refusals),
            hooks,
            journal: paths.journal.clone(),
            answers: paths.answers.clone(),
            entries,
        }
    }

    fn unexpected<T>() -> T {
        panic!("the crash fuzzer reached an unrelated provider operation")
    }

    /// The canonical position these bytes carry. The payload is the sanitized
    /// provider envelope, so the position is read out of the delivered bytes
    /// rather than from anything this process remembered.
    fn position_of(&self, call: &ProviderCall) -> u64 {
        let body = String::from_utf8_lossy(&call.payload.bytes);
        for sequence in 1..=64_u64 {
            if body.contains(&observation_text(sequence)) {
                return sequence;
            }
        }
        panic!("delivered payload carries no crash-fuzz position: {body}")
    }
}

impl NativeMemoryApplicationPort for FuzzNativePortV1 {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        // The host re-proves readiness at the head of **every** delivery
        // attempt, before it builds the observation call and before one byte
        // reaches `deliver_observation`. Standing here with the target's row
        // already durable in the journal is therefore standing exactly between
        // the journal write and the dispatch — and the seal this takes means
        // no later thread of this life can enter the provider for that row.
        self.hooks
            .seal_before_entry(HookPointV1::AfterJournalBeforeProviderCall, |target| {
                idempotency_key(&self.journal, target).is_some_and(|key| {
                    !self.ledger.holds(&key) && line_count(&self.entries, &key) == 0
                })
            });
        HandshakeResponse {
            terminal: TerminalRecord::new(
                ProviderOperation::Handshake,
                request.provider_id.clone(),
                TerminalCode::Success,
                CommittedEffectEvidence::none(Some(self.descriptor.state_generation)),
                FallbackDirective::forbidden(),
                request.request_id.clone(),
                request.exact_scope.exact_scope_sha256(),
                None,
            )
            .expect("handshake terminal"),
            descriptor: Some(self.descriptor.clone()),
            provider_instance_id: Some(
                crate::daemon::retained_owner::native_provider::PROVIDER_INSTANCE_ID.to_owned(),
            ),
            state_namespace: Some("tracedecay.native.crash-fuzz".to_owned()),
            accepted_scope: Some(request.exact_scope.clone()),
            effective_limits: Some(request.host_limits.minimum(self.descriptor.limits)),
            ready_receipt_sha256: Some(READY_RECEIPT.to_owned()),
            warnings: Vec::new(),
        }
    }

    fn health(&self, _call: &ProviderCall) -> ProviderReply {
        Self::unexpected()
    }

    fn observe(&self, observation: NativeObservation<'_>) -> ProviderReply {
        let call = observation.call();
        let sequence = self.position_of(call);
        let key = call
            .idempotency_key
            .clone()
            .expect("an observation call carries an idempotency key");

        // Durable, fsync'd, and taken under the same gate the
        // never-entered boundary seals: after this line no life can claim the
        // provider was not asked about this row, and before it no entry can
        // slip past a seal that has already been taken.
        self.hooks.record_entry(&self.entries, &key);

        // The row is leased, the provider has been entered, and it has not
        // touched its ledger.
        self.hooks
            .check(HookPointV1::AtProviderEntry, |target| sequence == target);

        if self.refused.contains(&sequence) {
            return ProviderReply {
                terminal: TerminalRecord::new(
                    ProviderOperation::Observe,
                    call.provider_id.clone(),
                    TerminalCode::ContractViolation,
                    CommittedEffectEvidence::none(Some(call.expected_state_generation)),
                    FallbackDirective::forbidden(),
                    call.operation_id.clone(),
                    call.exact_scope.exact_scope_sha256(),
                    Some("observation-refused-by-contract".to_owned()),
                )
                .expect("refusal terminal"),
                payload: None,
                warnings: Vec::new(),
                extensions: call.extensions.clone(),
                state_generation: call.expected_state_generation,
            };
        }

        if self.ledger.holds(&key) {
            // The provider recognises its own key. This is the path a
            // redelivery after a lost answer must take, and the only reason
            // redelivery is safe at all.
            return ProviderReply {
                terminal: TerminalRecord::new(
                    ProviderOperation::Observe,
                    call.provider_id.clone(),
                    TerminalCode::Success,
                    CommittedEffectEvidence::duplicate(
                        call.expected_state_generation,
                        key.clone(),
                        call.operation_id.clone(),
                        PROVIDER_RECEIPT,
                    )
                    .expect("duplicate evidence"),
                    FallbackDirective::forbidden(),
                    call.operation_id.clone(),
                    call.exact_scope.exact_scope_sha256(),
                    None,
                )
                .expect("duplicate terminal"),
                payload: Some(call.payload.clone()),
                warnings: Vec::new(),
                extensions: call.extensions.clone(),
                state_generation: call.expected_state_generation,
            };
        }

        self.ledger.commit(&key);
        // The effect is durable and the answer does not exist yet.
        self.hooks
            .check(HookPointV1::AfterEffectBeforeAnswer, |target| {
                sequence == target
            });

        let reply = ProviderReply {
            terminal: TerminalRecord::new(
                ProviderOperation::Observe,
                call.provider_id.clone(),
                TerminalCode::Success,
                CommittedEffectEvidence::committed(
                    call.expected_state_generation,
                    call.expected_state_generation,
                    vec![format!("observation:{sequence}")],
                    PROVIDER_RECEIPT,
                    EFFECT_DIGEST,
                )
                .expect("committed effect"),
                FallbackDirective::forbidden(),
                call.operation_id.clone(),
                call.exact_scope.exact_scope_sha256(),
                None,
            )
            .expect("observation terminal"),
            payload: Some(call.payload.clone()),
            warnings: Vec::new(),
            extensions: call.extensions.clone(),
            state_generation: call.expected_state_generation,
        };
        // The answer above is complete. Sealing it, holding the journal's own
        // write lock, and only then handing it back is what makes the window
        // between the handback and `record_attempt` a place a kill can land.
        HooksV1::hand_back_answer(&self.hooks, sequence, &self.journal, &self.answers, &key);
        reply
    }

    fn recall(&self, _call: &ProviderCall) -> ProviderReply {
        Self::unexpected()
    }

    fn feedback(&self, _call: &ProviderCall) -> ProviderReply {
        Self::unexpected()
    }

    fn maintenance(&self, _call: &ProviderCall) -> ProviderReply {
        Self::unexpected()
    }

    fn inspection(&self, _call: &ProviderCall) -> ProviderReply {
        Self::unexpected()
    }

    fn correction(&self, _call: &ProviderCall) -> ProviderReply {
        Self::unexpected()
    }

    fn delete_by_source(&self, _call: &ProviderCall) -> ProviderReply {
        Self::unexpected()
    }

    fn snapshot_export(&self, _call: &ProviderCall) -> ProviderReply {
        Self::unexpected()
    }

    fn snapshot_restore(&self, _call: &ProviderCall) -> ProviderReply {
        Self::unexpected()
    }

    fn replay(&self, _call: &ProviderCall) -> ProviderReply {
        Self::unexpected()
    }
}

// ------------------------------------------------------------- crash hooks --

/// One named boundary a life can die at.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HookPointV1 {
    /// The host settled the position canonically and died before the journal
    /// write.
    BeforeJournalWrite,
    /// The journal row is durable and the provider has not been entered for it
    /// in this life; the stand is the delivery preflight handshake, which the
    /// host performs before it builds the observation call.
    AfterJournalBeforeProviderCall,
    /// The provider was entered — durably, in this life's own entry log — and
    /// has not touched its ledger.
    AtProviderEntry,
    /// The provider's effect is fsync'd and its answer does not exist yet.
    AfterEffectBeforeAnswer,
    /// The provider's answer is built and sealed, the journal's write lock is
    /// held so the host cannot persist a receipt, and the answer has been
    /// handed back.
    AfterReplyBeforeReceipt,
    /// The target's acknowledgement — receipt and watermark, one transaction —
    /// is durable.
    AfterAckPersisted,
}

impl HookPointV1 {
    const ALL: [Self; 6] = [
        Self::BeforeJournalWrite,
        Self::AfterJournalBeforeProviderCall,
        Self::AtProviderEntry,
        Self::AfterEffectBeforeAnswer,
        Self::AfterReplyBeforeReceipt,
        Self::AfterAckPersisted,
    ];

    const fn as_wire(self) -> &'static str {
        match self {
            Self::BeforeJournalWrite => "before_journal_write",
            Self::AfterJournalBeforeProviderCall => "after_journal_before_provider_call",
            Self::AtProviderEntry => "at_provider_entry",
            Self::AfterEffectBeforeAnswer => "after_effect_before_answer",
            Self::AfterReplyBeforeReceipt => "after_reply_before_receipt",
            Self::AfterAckPersisted => "after_ack_persisted",
        }
    }

    fn from_wire(value: &str) -> Self {
        Self::ALL
            .into_iter()
            .find(|point| point.as_wire() == value)
            .unwrap_or_else(|| panic!("unknown hook point {value}"))
    }

    /// Whether reaching this boundary spends one of the row's delivery
    /// attempts. Only the canonical-commit boundary does not.
    const fn spends_an_attempt(self) -> bool {
        !matches!(self, Self::BeforeJournalWrite)
    }
}

/// The suffix an arrival carries when the boundary was reached but the durable
/// state it claims was not there. It is never a legal arrival name, so the
/// parent's exact-match check turns it into a named failure instead of a pass.
const UNVERIFIED: &str = "-unverified";

/// Parks the calling thread until the process dies.
///
/// Parking, never sleeping: the thread stops being schedulable and nothing
/// wakes it on a timer, so the instant of the kill cannot depend on a duration
/// this test chose.
fn park_forever() -> ! {
    loop {
        std::thread::park();
    }
}

/// The one boundary this process is armed to die at, and the durable proof it
/// arrived there.
struct HooksV1 {
    armed: Option<HookPointV1>,
    target: u64,
    marker: PathBuf,
    fired: AtomicBool,
    /// Set at most once, by whichever thread actually announces the arrival.
    announced: AtomicBool,
    /// Once the never-entered boundary has been taken, no thread may enter the
    /// provider: an entry after the seal would falsify the very claim the
    /// arrival makes.
    sealed: AtomicBool,
    /// Serialises "the provider was entered" against "the provider was never
    /// entered", so the two can never both be true of one row.
    entry_gate: Mutex<()>,
    /// The stream the parent blocks on, attached before the journey starts.
    signal: Mutex<Option<UnixStream>>,
}

impl HooksV1 {
    /// A life with no hook: recovery.
    fn disarmed() -> Self {
        Self {
            armed: None,
            target: 0,
            marker: PathBuf::new(),
            fired: AtomicBool::new(false),
            announced: AtomicBool::new(false),
            sealed: AtomicBool::new(false),
            entry_gate: Mutex::new(()),
            signal: Mutex::new(None),
        }
    }

    fn armed_at(point: HookPointV1, target: u64, marker: PathBuf, signal: UnixStream) -> Self {
        Self {
            armed: Some(point),
            target,
            marker,
            fired: AtomicBool::new(false),
            announced: AtomicBool::new(false),
            sealed: AtomicBool::new(false),
            entry_gate: Mutex::new(()),
            signal: Mutex::new(Some(signal)),
        }
    }

    /// Dies here when this is the armed boundary and `reached` says the life
    /// is standing on it.
    fn check(&self, point: HookPointV1, reached: impl FnOnce(u64) -> bool) {
        self.check_verified(point, reached, |_| true);
    }

    /// The same, for a boundary whose whole claim is about durable state: the
    /// arrival is recorded as unverified when `durable` disagrees, so a
    /// boundary that is not where it says it is fails the run rather than
    /// passing quietly.
    fn check_verified(
        &self,
        point: HookPointV1,
        reached: impl FnOnce(u64) -> bool,
        durable: impl FnOnce(u64) -> bool,
    ) {
        if self.armed != Some(point) || !reached(self.target) {
            return;
        }
        if self.fired.swap(true, Ordering::SeqCst) {
            return;
        }
        let name = if durable(self.target) {
            point.as_wire().to_owned()
        } else {
            format!("{}{UNVERIFIED}", point.as_wire())
        };
        self.arrive(&name)
    }

    /// Records, durably and under the entry gate, that the provider was entered
    /// for one idempotency key — and refuses to enter at all once the
    /// never-entered boundary has sealed this life.
    fn record_entry(&self, entries: &Path, key: &str) {
        let guard = self
            .entry_gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if self.sealed.load(Ordering::SeqCst) {
            // The arrival has already claimed that no thread of this life
            // entered the provider. Honour it rather than race it: this thread
            // stops here and the process dies parked, exactly as the boundary
            // says it did.
            drop(guard);
            park_forever();
        }
        append_line(entries, key);
        drop(guard);
    }

    /// Takes the "journal durable, provider never entered" boundary.
    ///
    /// The gate is held across the whole decision, so the claim cannot be
    /// falsified after it is made: any entry that had already happened is
    /// visible to `reached`, and any entry still to come finds the seal.
    fn seal_before_entry(&self, point: HookPointV1, reached: impl FnOnce(u64) -> bool) {
        if self.armed != Some(point) {
            return;
        }
        let guard = self
            .entry_gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if !reached(self.target) {
            return;
        }
        if self.fired.swap(true, Ordering::SeqCst) {
            return;
        }
        self.sealed.store(true, Ordering::SeqCst);
        drop(guard);
        self.arrive(point.as_wire())
    }

    /// Takes the "answer handed back, receipt not persisted" boundary.
    ///
    /// The sentinel holds a `BEGIN IMMEDIATE` transaction on the journal for as
    /// long as it lives, so the host's `record_attempt` cannot commit; it then
    /// waits for this thread to say the answer is leaving the provider frame
    /// before it tells the parent to kill. The kill therefore lands after the
    /// handback and provably before the receipt.
    fn hand_back_answer(
        hooks: &Arc<Self>,
        sequence: u64,
        journal: &Path,
        answers: &Path,
        key: &str,
    ) {
        if hooks.armed != Some(HookPointV1::AfterReplyBeforeReceipt) || sequence != hooks.target {
            return;
        }
        if hooks.fired.swap(true, Ordering::SeqCst) {
            return;
        }
        // Durable first: the provider's answer exists, whatever happens next.
        append_line(answers, key);
        let verified =
            !acknowledging_receipt_exists(journal, sequence) && line_count(answers, key) == 1;
        let name = if verified {
            HookPointV1::AfterReplyBeforeReceipt.as_wire().to_owned()
        } else {
            format!(
                "{}{UNVERIFIED}",
                HookPointV1::AfterReplyBeforeReceipt.as_wire()
            )
        };

        let (locked, lock_taken) = mpsc::channel::<()>();
        let (handback, handed_back) = mpsc::channel::<()>();
        let sentinel = Arc::clone(hooks);
        let journal = journal.to_path_buf();
        let announcement = name.clone();
        std::thread::spawn(move || {
            let connection = rusqlite::Connection::open(&journal).expect("receipt lock connection");
            connection
                .busy_timeout(RECEIPT_LOCK_TIMEOUT)
                .expect("receipt lock busy timeout");
            if connection.execute_batch("BEGIN IMMEDIATE").is_err() {
                // The lock could not be taken, so this boundary cannot be
                // guaranteed. Say so instead of killing at an unknown point.
                sentinel.arrive(&format!(
                    "{}{UNVERIFIED}",
                    HookPointV1::AfterReplyBeforeReceipt.as_wire()
                ));
            }
            let _ = locked.send(());
            // Blocks on the handback itself, never on a duration.
            let _ = handed_back.recv();
            sentinel.arrive(&announcement)
        });

        match lock_taken.recv_timeout(RECEIPT_LOCK_TIMEOUT + Duration::from_secs(5)) {
            Ok(()) => {}
            Err(_) => hooks.arrive(&format!(
                "{}{UNVERIFIED}",
                HookPointV1::AfterReplyBeforeReceipt.as_wire()
            )),
        }
        // The answer is leaving this frame now. The journal is locked against
        // the receipt that would follow it, so the sentinel can safely tell the
        // parent to kill.
        let _ = handback.send(());
    }

    fn arrive(&self, name: &str) -> ! {
        if self.announced.swap(true, Ordering::SeqCst) {
            park_forever();
        }
        // Durable first, then the signal: the arrival the parent verifies after
        // the kill is already on disk when the kill is issued.
        append_line(&self.marker, name);
        if let Ok(mut slot) = self.signal.lock() {
            if let Some(stream) = slot.as_mut() {
                let _ = stream.write_all(b"1");
                let _ = stream.flush();
            }
        }
        park_forever()
    }
}

/// Whether the journal itself holds a durable acknowledgement for `sequence`:
/// an acknowledging receipt and an acknowledged watermark that has reached it.
///
/// Both are read from the durable journal in one connection. They are written
/// in one transaction, so a life that finds one without the other has found the
/// defect this boundary exists to catch.
fn acknowledgement_is_durable(journal: &Path, sequence: u64) -> bool {
    acknowledging_receipt_exists(journal, sequence)
        && acknowledged_watermark(journal).is_some_and(|position| position >= sequence)
}

/// Whether the journal holds a durable acknowledging receipt for `sequence`.
///
/// This is the half of an acknowledgement the *receipt* table carries. It is
/// asked separately from the watermark on purpose: the boundary fires on this
/// half and then requires the other, which is how a watermark that moved
/// without its receipt — or a receipt whose watermark never moved — becomes a
/// named failure instead of a quiet pass.
fn acknowledging_receipt_exists(journal: &Path, sequence: u64) -> bool {
    if !journal.exists() {
        return false;
    }
    let Ok(connection) = rusqlite::Connection::open(journal) else {
        return false;
    };
    let receipts: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM tdmem_observation_journal_v1 journal \
             JOIN tdmem_observation_receipt_v1 receipt \
               ON receipt.observation_id = journal.observation_id \
             WHERE journal.source_sequence = ?1 \
               AND receipt.outcome IN ('applied', 'partial_effect', 'duplicate_acknowledged')",
            rusqlite::params![i64::try_from(sequence).unwrap_or(i64::MAX)],
            |row| row.get(0),
        )
        .unwrap_or(0);
    receipts > 0
}

// -------------------------------------------------------- the child process --

/// One child life's whole assignment.
struct ChildLifeSpecV1 {
    directory: PathBuf,
    hook: HookPointV1,
    target: u64,
    /// Which life of this journey this is, so its provider-entry log is its
    /// own.
    life: usize,
    /// Positions the canonical authority settles before the journey mounts.
    settle: Vec<u64>,
}

impl ChildLifeSpecV1 {
    /// Encodes the assignment together with the socket the parent is already
    /// listening on. The socket is the parent's, not the plan's, which is why
    /// it is an argument rather than a field.
    fn encode(&self, signal: &Path) -> String {
        let settle = self
            .settle
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "{}|{}|{}|{}|{settle}|{}",
            self.directory.display(),
            self.hook.as_wire(),
            self.target,
            self.life,
            signal.display(),
        )
    }

    fn decode(value: &str) -> (Self, PathBuf) {
        let parts: Vec<&str> = value.split('|').collect();
        let [directory, hook, target, life, settle, signal] = parts.as_slice() else {
            panic!("malformed child life spec {value}")
        };
        (
            Self {
                directory: PathBuf::from(directory),
                hook: HookPointV1::from_wire(hook),
                target: target.parse().expect("target sequence"),
                life: life.parse().expect("life ordinal"),
                settle: settle
                    .split(',')
                    .filter(|piece| !piece.is_empty())
                    .map(|piece| piece.parse::<u64>().expect("settled position"))
                    .collect(),
            },
            PathBuf::from(signal),
        )
    }

    fn entries(&self) -> PathBuf {
        JourneyPathsV1::in_directory(&self.directory).entries_for(&self.life.to_string())
    }
}

/// The child-process entry point.
///
/// Under a normal `cargo test` this is a no-op: the environment carries no
/// assignment, so it returns at once. When the parent re-executes this binary
/// with the assignment in the environment, this *is* the life that dies — it
/// settles the canonical stream, mounts the production journey, reaches its
/// armed boundary, and parks there until the parent kills it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mounted_crash_child_process_entrypoint() {
    let Ok(encoded) = std::env::var(CHILD_LIFE_ENV) else {
        return;
    };
    let (spec, signal_path) = ChildLifeSpecV1::decode(&encoded);
    let paths = JourneyPathsV1::in_directory(&spec.directory);
    // Attach before anything else happens. From here on the parent's blocking
    // read has two possible ends: the arrival byte, or the close this socket
    // suffers when the process dies for any other reason.
    let signal = UnixStream::connect(&signal_path).expect("arrival socket");

    let canonical = CanonicalStreamV1::new(paths.canonical.clone());
    // The host's canonical authority commits first and durably. Everything
    // after this point is downstream of a settlement that already survived.
    for sequence in &spec.settle {
        canonical.commit(*sequence);
    }

    let hooks = Arc::new(HooksV1::armed_at(
        spec.hook,
        spec.target,
        paths.marker.clone(),
        signal,
    ));
    let journey = mount_life(&paths, Arc::clone(&hooks), spec.entries()).await;

    // The armed thread parks where it stands; this task has nothing left to
    // do but wait to be killed with it. A life that instead converges is a
    // boundary that was never traversed, and the parent fails on the missing
    // arrival rather than on a timeout it cannot explain.
    std::future::pending::<()>().await;
    drop(journey);
}

/// Mounts the production journey over this life's durable artefacts.
///
/// This is the composition root's own entry point: the mount inputs are the
/// ones `project_composition` fills in, the store is passed by value the way
/// the daemon passes the canonical observation store, and the startup replay
/// plus live replay task are started by `mount_and_replay` itself.
async fn mount_life(
    paths: &JourneyPathsV1,
    hooks: Arc<HooksV1>,
    entries: PathBuf,
) -> Arc<ProjectObservationJourneyV1> {
    fs::create_dir_all(&paths.root).expect("journal root");
    let project_id = ProjectId::new(PROJECT).expect("project id");
    let port = Arc::new(FuzzNativePortV1::new(paths, Arc::clone(&hooks), entries));
    let committed = CanonicalStreamV1::new(paths.canonical.clone()).committed();
    let store = FuzzCanonicalPortV1::over(paths, &committed, hooks);
    mount_and_replay(
        ObservationJourneyMountInputsV1 {
            composition: composition(port as Arc<dyn NativeMemoryApplicationPort>),
            profile_id: UserProfileId::new(PROFILE).expect("profile id"),
            scope: scope(project_id.clone()),
            authoritative_project_id: project_id,
            store_data_root: paths.root.clone(),
            registration_revision: REGISTRATION_REVISION,
            host_limits: crate::daemon::retained_owner::native_provider::native_provider_limits(),
            policy: fuzz_policy(),
        },
        store,
        &HostCancellationToken::new(),
    )
    .await
    .expect("mounted journey")
}

// ---------------------------------------------------------- killing a life --

/// A parent-owned rendezvous point for one child life. Removed on every path,
/// because `Drop` owns the removal rather than any one exit.
struct ArrivalSocketV1 {
    path: PathBuf,
    listener: UnixListener,
}

impl ArrivalSocketV1 {
    /// Binds a short, unique path. Short on purpose: a Unix socket path is
    /// capped near a hundred bytes, and the journey directory is a deep
    /// temporary one.
    fn bind() -> ArrivalSocketV1 {
        static NEXT: OnceLock<AtomicU32> = OnceLock::new();
        let ordinal = NEXT
            .get_or_init(|| AtomicU32::new(0))
            .fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("tdmem5lcm-{}-{ordinal}.sock", std::process::id()));
        let _ = fs::remove_file(&path);
        let listener = UnixListener::bind(&path).expect("arrival socket");
        Self { path, listener }
    }
}

impl Drop for ArrivalSocketV1 {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// What the parent's one blocking read can end with.
enum ChildSignalV1 {
    Attached,
    Arrived,
    Departed,
}

/// Spawns one life as a child process, blocks until it reaches its boundary,
/// kills it there, and proves from the durable files that the boundary is where
/// it says it is.
///
/// Returns only after the child is confirmed dead by `SIGKILL`, the marker file
/// holds exactly one arrival for the armed boundary, and the seam's own durable
/// evidence agrees.
fn crash_at(spec: &ChildLifeSpecV1) -> Result<(), String> {
    let paths = JourneyPathsV1::in_directory(&spec.directory);
    let arrivals_before = read_lines(&paths.marker).len();
    let key_before = idempotency_key(&paths.journal, spec.target);
    let effects_before = key_before
        .as_deref()
        .map_or(0, |key| line_count(&paths.ledger, key));
    // Bound before the child exists, so there is no window in which the child
    // could reach its boundary and find nobody listening.
    let socket = ArrivalSocketV1::bind();
    let executable = std::env::current_exe().map_err(|error| error.to_string())?;
    let mut child = Command::new(executable)
        .args([
            "--exact",
            CHILD_TEST_PATH,
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_LIFE_ENV, spec.encode(&socket.path))
        .stdin(Stdio::null())
        .stdout(Stdio::from(
            fs::File::create(
                spec.directory
                    .join(format!("child-{}-stdout.log", spec.life)),
            )
            .map_err(|error| error.to_string())?,
        ))
        .stderr(Stdio::from(
            fs::File::create(
                spec.directory
                    .join(format!("child-{}-stderr.log", spec.life)),
            )
            .map_err(|error| error.to_string())?,
        ))
        .spawn()
        .map_err(|error| error.to_string())?;

    if let Err(error) = await_arrival(spec, socket, &mut child) {
        let _ = child.kill();
        let _ = child.wait();
        return Err(error);
    }

    // The boundary is reached and the process is parked on it: kill it where it
    // stands. Nothing is unwound, nothing is flushed, and the SQLite connection
    // is never closed.
    child.kill().map_err(|error| error.to_string())?;
    let status = child.wait().map_err(|error| error.to_string())?;
    if status.signal() != Some(SIGKILL) {
        return Err(format!(
            "the child at {} did not die on SIGKILL: {status}",
            spec.hook.as_wire()
        ));
    }

    let arrivals = read_lines(&paths.marker);
    let new: Vec<&String> = arrivals.iter().skip(arrivals_before).collect();
    if new.len() != 1 || new.first().map(|line| line.as_str()) != Some(spec.hook.as_wire()) {
        return Err(format!(
            "expected exactly one arrival at {}, observed {new:?}",
            spec.hook.as_wire()
        ));
    }
    prove_boundary(spec, &paths, effects_before)
}

/// Reads the durable files the dead child left and refuses any boundary whose
/// name the artefacts do not support.
///
/// This is the half the child cannot do for itself: the child can only claim
/// where it stood, and every claim here is re-derived after the kill from the
/// journal, the provider's ledger, this life's own provider-entry log, and the
/// sealed-answer log.
fn prove_boundary(
    spec: &ChildLifeSpecV1,
    paths: &JourneyPathsV1,
    effects_before: usize,
) -> Result<(), String> {
    let target = spec.target;
    let entries = spec.entries();
    let key = idempotency_key(&paths.journal, target);
    let refuse = |detail: String| -> Result<(), String> {
        Err(format!(
            "{} at sequence {target} is not where it says it is: {detail}; {}",
            spec.hook.as_wire(),
            snapshot(paths)
        ))
    };
    match spec.hook {
        HookPointV1::BeforeJournalWrite => {
            if !CanonicalStreamV1::new(paths.canonical.clone())
                .committed()
                .contains(&target)
            {
                return refuse("the canonical authority never settled it".to_owned());
            }
            if key.is_some() {
                return refuse("the journal already holds a row for it".to_owned());
            }
        }
        HookPointV1::AfterJournalBeforeProviderCall => {
            let Some(key) = key else {
                return refuse("the journal holds no row for it".to_owned());
            };
            if line_count(&entries, &key) != 0 {
                return refuse("this life entered the provider for it".to_owned());
            }
            if line_count(&paths.ledger, &key) != 0 {
                return refuse("the provider holds an effect for it".to_owned());
            }
        }
        HookPointV1::AtProviderEntry => {
            let Some(key) = key else {
                return refuse("the journal holds no row for it".to_owned());
            };
            if line_count(&entries, &key) == 0 {
                return refuse("this life never entered the provider for it".to_owned());
            }
            if line_count(&paths.ledger, &key) != effects_before {
                return refuse("the provider's ledger grew inside this life".to_owned());
            }
        }
        HookPointV1::AfterEffectBeforeAnswer => {
            let Some(key) = key else {
                return refuse("the journal holds no row for it".to_owned());
            };
            if line_count(&paths.ledger, &key) != 1 {
                return refuse("the provider does not hold exactly one effect for it".to_owned());
            }
            if line_count(&paths.answers, &key) != 0 {
                return refuse("the provider had already sealed an answer for it".to_owned());
            }
            if acknowledging_receipt_exists(&paths.journal, target) {
                return refuse("the journal already holds an acknowledging receipt".to_owned());
            }
        }
        HookPointV1::AfterReplyBeforeReceipt => {
            let Some(key) = key else {
                return refuse("the journal holds no row for it".to_owned());
            };
            if line_count(&paths.answers, &key) != 1 {
                return refuse("the provider sealed no single answer for it".to_owned());
            }
            if line_count(&paths.ledger, &key) != 1 {
                return refuse("the provider does not hold exactly one effect for it".to_owned());
            }
            if acknowledging_receipt_exists(&paths.journal, target) {
                return refuse(
                    "the host persisted the receipt the boundary claims it could not".to_owned(),
                );
            }
        }
        HookPointV1::AfterAckPersisted => {
            if !acknowledging_receipt_exists(&paths.journal, target) {
                return refuse("the journal holds no acknowledging receipt for it".to_owned());
            }
            if !acknowledged_watermark(&paths.journal).is_some_and(|position| position >= target) {
                return refuse("the acknowledged watermark never reached it".to_owned());
            }
        }
    }
    Ok(())
}

/// Blocks until the child says it is parked on the armed boundary.
///
/// Two stages, each with its own terminal deadline and its own diagnostic: a
/// child that never attaches failed before the journey started, and a child
/// that attaches and then closes the socket died on its own instead of on the
/// boundary.
fn await_arrival(
    spec: &ChildLifeSpecV1,
    socket: ArrivalSocketV1,
    child: &mut Child,
) -> Result<(), String> {
    let (signals, arrivals) = mpsc::channel::<Result<ChildSignalV1, String>>();
    let listener = socket.listener.try_clone().map_err(|e| e.to_string())?;
    let waiter = std::thread::spawn(move || {
        (|| -> Result<(), String> {
            let (mut stream, _) = listener
                .accept()
                .map_err(|error| format!("the arrival socket could not be accepted: {error}"))?;
            signals
                .send(Ok(ChildSignalV1::Attached))
                .map_err(|error| error.to_string())?;
            let mut byte = [0_u8; 1];
            let signal = match stream.read_exact(&mut byte) {
                Ok(()) => ChildSignalV1::Arrived,
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    ChildSignalV1::Departed
                }
                Err(error) => return Err(format!("the arrival socket failed mid-life: {error}")),
            };
            signals
                .send(Ok(signal))
                .map_err(|error| error.to_string())?;
            Ok(())
        })()
    });

    let attached = arrivals.recv_timeout(CHILD_ATTACH_TIMEOUT);
    let outcome = stage(spec, child, attached, "attach", CHILD_ATTACH_TIMEOUT).and_then(|()| {
        let arrived = arrivals.recv_timeout(HOOK_ARRIVAL_TIMEOUT);
        stage(spec, child, arrived, "arrival", HOOK_ARRIVAL_TIMEOUT)
    });
    // The waiter owns the accepted stream, so it is joined on every path: a
    // detached thread holding a descriptor past the life it belongs to is
    // exactly the leak this suite is not allowed to have.
    drop(socket);
    let _ = waiter.join();
    outcome
}

/// Interprets one stage of the blocking wait.
fn stage(
    spec: &ChildLifeSpecV1,
    child: &mut Child,
    received: Result<Result<ChildSignalV1, String>, RecvTimeoutError>,
    label: &str,
    bound: Duration,
) -> Result<(), String> {
    match received {
        Ok(Ok(ChildSignalV1::Attached | ChildSignalV1::Arrived)) => Ok(()),
        Ok(Ok(ChildSignalV1::Departed)) => {
            let status = child.wait().map_err(|error| error.to_string())?;
            Err(format!(
                "the child exited {status} before reaching {}; stderr: {}",
                spec.hook.as_wire(),
                child_stderr(spec)
            ))
        }
        Ok(Err(error)) => Err(format!(
            "the {label} wait for {} failed: {error}",
            spec.hook.as_wire()
        )),
        Err(RecvTimeoutError::Timeout) => Err(format!(
            "the child never reached the {label} stage for {} inside {bound:?}; stderr: {}",
            spec.hook.as_wire(),
            child_stderr(spec)
        )),
        Err(RecvTimeoutError::Disconnected) => Err(format!(
            "the {label} wait for {} lost its waiter",
            spec.hook.as_wire()
        )),
    }
}

fn child_stderr(spec: &ChildLifeSpecV1) -> String {
    fs::read_to_string(
        spec.directory
            .join(format!("child-{}-stderr.log", spec.life)),
    )
    .unwrap_or_default()
}

// ---------------------------------------------------------------- recovery --

/// One journal row as the invariants need it.
#[derive(Clone, Debug)]
struct JournalRowV1 {
    sequence: u64,
    state: String,
    /// Outcomes of every receipt this row carries, in attempt order.
    receipts: Vec<String>,
}

/// Everything the durable journal holds, read from the file rather than from
/// any journey object.
fn journal_rows(journal: &Path) -> Vec<JournalRowV1> {
    if !journal.exists() {
        return Vec::new();
    }
    let connection = rusqlite::Connection::open(journal).expect("journal connection");
    let mut statement = connection
        .prepare(
            "SELECT journal.source_sequence, delivery.state, journal.observation_id \
             FROM tdmem_observation_journal_v1 journal \
             JOIN tdmem_observation_delivery_v1 delivery \
               ON delivery.observation_id = journal.observation_id \
             ORDER BY journal.source_sequence",
        )
        .expect("journal statement");
    let rows: Vec<(u64, String, String)> = statement
        .query_map([], |row| {
            Ok((
                u64::try_from(row.get::<_, i64>(0)?).unwrap_or_default(),
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .expect("journal rows")
        .map(|row| row.expect("journal row"))
        .collect();
    rows.into_iter()
        .map(|(sequence, state, observation_id)| {
            let mut statement = connection
                .prepare(
                    "SELECT outcome FROM tdmem_observation_receipt_v1 \
                     WHERE observation_id = ?1 ORDER BY attempt_number",
                )
                .expect("receipt statement");
            let receipts = statement
                .query_map(rusqlite::params![observation_id], |row| {
                    row.get::<_, String>(0)
                })
                .expect("receipt rows")
                .map(|row| row.expect("receipt row"))
                .collect();
            JournalRowV1 {
                sequence,
                state,
                receipts,
            }
        })
        .collect()
}

/// The durable acknowledged watermark, the position a restart resumes the
/// provider stream from.
fn acknowledged_watermark(journal: &Path) -> Option<u64> {
    if !journal.exists() {
        return None;
    }
    let connection = rusqlite::Connection::open(journal).expect("journal connection");
    connection
        .query_row(
            "SELECT acknowledged_sequence FROM tdmem_observation_recovery_v1",
            [],
            |row| row.get::<_, Option<i64>>(0),
        )
        .ok()
        .flatten()
        .map(|position| u64::try_from(position).unwrap_or_default())
}

/// A one-line description of the journal, so a failed wait explains itself.
fn snapshot(paths: &JourneyPathsV1) -> String {
    let rows = journal_rows(&paths.journal)
        .into_iter()
        .map(|row| format!("{}:{}:{:?}", row.sequence, row.state, row.receipts))
        .collect::<Vec<_>>()
        .join(" | ");
    format!(
        "watermark={:?} effects={:?} answers={:?} rows=[{rows}]",
        acknowledged_watermark(&paths.journal),
        ProviderLedgerV1::new(paths.ledger.clone()).effects(),
        read_lines(&paths.answers)
    )
}

/// Mounts one recovery life in this process and drives it to convergence.
///
/// Recovery reads what the durable files hold, exactly like the killed lives
/// did: the canonical positions, the provider's own effects, and the journal.
async fn recover(paths: &JourneyPathsV1, expected: &[u64], label: &str) {
    let journey = mount_life(
        paths,
        Arc::new(HooksV1::disarmed()),
        paths.entries_for(&format!("recovery-{label}")),
    )
    .await;
    let deadline = Instant::now() + CONVERGENCE_BUDGET;
    loop {
        let rows = journal_rows(&paths.journal);
        let converged = expected.iter().all(|sequence| {
            rows.iter()
                .any(|row| row.sequence == *sequence && is_settled(&row.state))
        });
        if converged {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the mounted journey never converged; {}",
            snapshot(paths)
        );
        // A bounded re-read of durable state, not a synchronisation delay: the
        // predicate above is the only thing that ends this loop.
        tokio::time::sleep(CONVERGENCE_REREAD).await;
    }
    let failures = journey
        .shutdown(tokio::time::Instant::now() + Duration::from_secs(10))
        .await;
    assert!(
        failures.is_empty(),
        "the recovered journey did not stop cleanly: {failures:?}"
    );
}

// -------------------------------------------------------------- invariants --

/// AC1 and AC3 — every settled commit is delivered, exactly once.
fn assert_no_loss_and_no_double_effect(
    paths: &JourneyPathsV1,
    committed: &[u64],
    refused: &BTreeSet<u64>,
    label: &str,
) {
    let rows = journal_rows(&paths.journal);
    for sequence in committed {
        let matching: Vec<&JournalRowV1> = rows
            .iter()
            .filter(|row| row.sequence == *sequence)
            .collect();
        assert_eq!(
            matching.len(),
            1,
            "{label}: sequence {sequence} has {} journal rows; {}",
            matching.len(),
            snapshot(paths)
        );
        let row = matching[0];
        if refused.contains(sequence) {
            assert_eq!(
                row.state,
                "rejected",
                "{label}: a permanently refused position settled {}; {}",
                row.state,
                snapshot(paths)
            );
        } else {
            assert!(
                row.state == "acknowledged" || row.state == "duplicate_acknowledged",
                "{label}: sequence {sequence} settled {} instead of acknowledged; {}",
                row.state,
                snapshot(paths)
            );
        }
    }

    // One durable effect per delivered position, counted as lines rather than
    // as set members, so a second application is visible.
    let effects = ProviderLedgerV1::new(paths.ledger.clone()).effects();
    let unique: BTreeSet<&String> = effects.iter().collect();
    assert_eq!(
        unique.len(),
        effects.len(),
        "{label}: the provider committed an effect twice for one key: {effects:?}"
    );
    let delivered = committed.iter().filter(|s| !refused.contains(s)).count();
    assert_eq!(
        effects.len(),
        delivered,
        "{label}: {} durable effects for {delivered} delivered positions; {}",
        effects.len(),
        snapshot(paths)
    );
}

/// AC2 — the durable acknowledgement watermark is exactly the last sequence a
/// provider acknowledgement was persisted for.
///
/// The watermark is written inside the acknowledging receipt's own transaction,
/// which is what this asserts after every killed life: never a watermark whose
/// receipt is missing, and never an acknowledging receipt whose watermark did
/// not move.
///
/// What is deliberately *not* claimed mid-flight is "no row below the watermark
/// lacks a receipt". Delivery is per row: a position whose attempt failed goes
/// back to `pending` behind a retry delay while a later position acknowledges,
/// and a permanently refused position never acknowledges at all. That both are
/// nevertheless finished is proved after convergence by
/// [`assert_no_loss_and_no_double_effect`].
fn assert_acknowledged_watermark(paths: &JourneyPathsV1, label: &str) {
    let rows = journal_rows(&paths.journal);
    let acknowledged: BTreeSet<u64> = rows
        .iter()
        .filter(|row| row.receipts.iter().any(|outcome| is_acknowledging(outcome)))
        .map(|row| row.sequence)
        .collect();
    let watermark = acknowledged_watermark(&paths.journal);

    match (watermark, acknowledged.iter().next_back().copied()) {
        (None, None) => {}
        (None, Some(highest)) => panic!(
            "{label}: sequence {highest} carries a durable acknowledgement receipt with no \
             watermark behind it, so a restart would redeliver an effect the provider already \
             holds; {}",
            snapshot(paths)
        ),
        (Some(watermark), None) => panic!(
            "{label}: the acknowledged watermark stands at {watermark} with no acknowledging \
             receipt anywhere, so it passed a record nothing answered for; {}",
            snapshot(paths)
        ),
        (Some(watermark), Some(highest)) => {
            assert_eq!(
                watermark,
                highest,
                "{label}: the acknowledged watermark is {watermark} but the highest durably \
                 acknowledged sequence is {highest}; {}",
                snapshot(paths)
            );
            assert!(
                acknowledged.contains(&watermark),
                "{label}: the acknowledged watermark stands on sequence {watermark}, which \
                 carries no acknowledging receipt; {}",
                snapshot(paths)
            );
        }
    }
}

/// AC4 — a permanent refusal is terminal and stays terminal.
///
/// Checked twice over: the durable state is what a refusal should leave behind,
/// and then one more complete life over the same artefacts is required to
/// change nothing. A row a later life picked up again would burn attempts and
/// keep asking a question the provider has already answered.
async fn assert_refusals_stay_terminal(
    paths: &JourneyPathsV1,
    committed: &[u64],
    refused: &BTreeSet<u64>,
    label: &str,
) {
    if refused.is_empty() {
        return;
    }
    let before: Vec<JournalRowV1> = journal_rows(&paths.journal)
        .into_iter()
        .filter(|row| refused.contains(&row.sequence))
        .collect();
    assert_eq!(
        before.len(),
        refused.len(),
        "{label}: a refused position never reached the journal; {}",
        snapshot(paths)
    );
    for row in &before {
        assert_eq!(row.state, "rejected", "{label}: {row:?}");
        assert_eq!(
            row.receipts.last().map(String::as_str),
            Some("rejected_contract_violation"),
            "{label}: the refusal was not recorded as the refusal the provider gave; {row:?}"
        );
        let refused_key = idempotency_key(&paths.journal, row.sequence)
            .expect("a journalled refusal carries an idempotency key");
        assert!(
            !ProviderLedgerV1::new(paths.ledger.clone()).holds(&refused_key),
            "{label}: the provider committed an effect for a position it refused"
        );
    }

    recover(paths, committed, "refusal").await;

    let after: Vec<JournalRowV1> = journal_rows(&paths.journal)
        .into_iter()
        .filter(|row| refused.contains(&row.sequence))
        .collect();
    for (before, after) in before.iter().zip(after.iter()) {
        assert_eq!(
            (before.state.as_str(), before.receipts.len()),
            (after.state.as_str(), after.receipts.len()),
            "{label}: a later life redelivered a permanently refused position; {}",
            snapshot(paths)
        );
    }
}

// --------------------------------------------------------------- the plans --

/// Deterministic 64-bit stream (splitmix64), dependency-free and identical on
/// every platform, which is what makes a failing seed a reproduction.
struct SeedStreamV1(u64);

impl SeedStreamV1 {
    const fn from_seed(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut mixed = self.0;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        mixed ^ (mixed >> 31)
    }

    fn below(&mut self, bound: usize) -> usize {
        assert!(
            bound > 0,
            "the fuzz plan asked for a choice with no options"
        );
        usize::try_from(self.next() % bound as u64).expect("bounded choice")
    }
}

/// One seed's whole assignment.
struct FuzzPlanV1 {
    committed: Vec<u64>,
    lives: usize,
    refused: BTreeSet<u64>,
    /// The boundary this seed tries first, so a window of consecutive seeds
    /// covers every boundary by construction instead of by luck. It is derived
    /// from the seed itself, so reproducing a seed reproduces the preference.
    preferred: HookPointV1,
}

impl FuzzPlanV1 {
    fn from_seed(seed: u64) -> Self {
        let mut stream = SeedStreamV1::from_seed(seed);
        let count = 2 + stream.below(3);
        let committed: Vec<u64> = (1..=u64::try_from(count).expect("committed count")).collect();
        let lives = 1 + stream.below(3);
        // One seed in three gives the provider a position it refuses forever.
        let refused = if stream.below(3) == 0 {
            let position = stream.below(committed.len());
            BTreeSet::from([committed[position]])
        } else {
            BTreeSet::new()
        };
        let preferred = HookPointV1::ALL
            [usize::try_from(seed % HookPointV1::ALL.len() as u64).expect("preferred boundary")];
        Self {
            committed,
            lives,
            refused,
            preferred,
        }
    }
}

/// A boundary this journey, in the durable state it is actually in, will
/// traverse in its next life.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct KillPointV1 {
    hook: HookPointV1,
    sequence: u64,
}

/// Every boundary the next life will really cross, derived from the durable
/// artefacts rather than from what the plan wishes were true.
///
/// This is what keeps the fuzzer honest: an arming that this journey would
/// never reach becomes a failed run rather than a healthy one that proves
/// nothing, so the candidate set is computed from the journal and the ledger
/// as they stand.
fn reachable_kill_points(paths: &JourneyPathsV1, plan: &FuzzPlanV1) -> Vec<KillPointV1> {
    let rows = journal_rows(&paths.journal);
    let state_of = |sequence: u64| -> Option<String> {
        rows.iter()
            .find(|row| row.sequence == sequence)
            .map(|row| row.state.clone())
    };
    let ledger = ProviderLedgerV1::new(paths.ledger.clone());
    let effects = ledger.effects();

    let mut points = Vec::new();
    let mut first_unjournalled = None;
    for sequence in &plan.committed {
        let sequence = *sequence;
        let state = state_of(sequence);
        let settled = state.as_deref().is_some_and(is_settled);
        // A redelivery of an effect the provider already holds takes the
        // duplicate path, so an earlier life that died after the effect was
        // committed retires every boundary whose claim is "the provider has
        // committed nothing for this position".
        let holds_effect =
            idempotency_key(&paths.journal, sequence).is_some_and(|key| effects.contains(&key));
        if state.is_none() && first_unjournalled.is_none() {
            // The next position ingress will admit is the lowest one with no
            // row, and only that one is *about* to be journalled.
            first_unjournalled = Some(sequence);
            points.push(KillPointV1 {
                hook: HookPointV1::BeforeJournalWrite,
                sequence,
            });
        }
        if settled {
            continue;
        }
        // Ingress runs before dispatch in every life, so a position missing
        // from the journal still reaches the delivery boundaries: the row is
        // written, then the delivery preflight handshake runs, then the
        // provider is entered.
        if !holds_effect {
            // The claim is "the provider holds nothing for this row and was
            // never entered for it in this life". An effect an earlier life
            // committed retires it: the boundary would never be reached and
            // the child would hang on an arrival that cannot happen.
            points.push(KillPointV1 {
                hook: HookPointV1::AfterJournalBeforeProviderCall,
                sequence,
            });
        }
        points.push(KillPointV1 {
            hook: HookPointV1::AtProviderEntry,
            sequence,
        });
        if !plan.refused.contains(&sequence) && !holds_effect {
            points.push(KillPointV1 {
                hook: HookPointV1::AfterEffectBeforeAnswer,
                sequence,
            });
            points.push(KillPointV1 {
                hook: HookPointV1::AfterReplyBeforeReceipt,
                sequence,
            });
        }
        // The acknowledgement boundary is observed from the replay pass that
        // follows it, and the live replay task runs for as long as the journey
        // does, so it needs nothing but a position this life will acknowledge.
        if !plan.refused.contains(&sequence) {
            points.push(KillPointV1 {
                hook: HookPointV1::AfterAckPersisted,
                sequence,
            });
        }
    }
    points
}

/// The idempotency key the host minted for one canonical position, read from
/// the durable journal row.
///
/// The key is the host's, not the provider's, so the mapping from a ledger line
/// back to a canonical position runs through the journal rather than through
/// anything the ledger itself knows.
fn idempotency_key(journal: &Path, sequence: u64) -> Option<String> {
    if !journal.exists() {
        return None;
    }
    let connection = rusqlite::Connection::open(journal).ok()?;
    connection
        .query_row(
            "SELECT idempotency_key FROM tdmem_observation_journal_v1 \
             WHERE source_sequence = ?1",
            rusqlite::params![i64::try_from(sequence).unwrap_or(i64::MAX)],
            |row| row.get::<_, String>(0),
        )
        .ok()
}

// ------------------------------------------------------------------- tiers --

fn env_u64(name: &str, fallback: u64) -> u64 {
    match std::env::var(name) {
        Ok(value) => value
            .trim()
            .parse::<u64>()
            .unwrap_or_else(|_| panic!("{name} must be a number, not {value:?}")),
        Err(_) => fallback,
    }
}

fn seed_budget() -> u64 {
    env_u64(SEEDS_ENV, DEFAULT_SEEDS)
}

fn base_seed() -> u64 {
    match std::env::var(BASE_SEED_ENV) {
        Ok(value) => {
            let trimmed = value.trim().to_owned();
            match trimmed.strip_prefix("0x") {
                Some(hexadecimal) => u64::from_str_radix(hexadecimal, 16).expect("base seed"),
                None => trimmed.parse::<u64>().expect("base seed"),
            }
        }
        Err(_) => BASE_SEED,
    }
}

/// What one seed exercised, so the tier can prove it is not vacuous.
#[derive(Default)]
struct FuzzCoverageV1 {
    /// Kills per boundary. A histogram rather than a set, so a window that
    /// reached a boundary once and a window that reached it fifty times are
    /// distinguishable in the report a run prints.
    hooks: BTreeMap<&'static str, usize>,
    kills: usize,
    multi_life_seeds: usize,
    refusal_seeds: usize,
}

impl FuzzCoverageV1 {
    fn record(&mut self, hook: HookPointV1) {
        *self.hooks.entry(hook.as_wire()).or_default() += 1;
        self.kills += 1;
    }

    fn absorb(&mut self, other: &Self) {
        for (hook, kills) in &other.hooks {
            *self.hooks.entry(hook).or_default() += kills;
        }
        self.kills += other.kills;
        self.multi_life_seeds += other.multi_life_seeds;
        self.refusal_seeds += other.refusal_seeds;
    }

    /// The line a finished window prints: which boundaries were really killed
    /// at, and how often. A reader should never have to take "the boundary was
    /// covered" on the word of an assertion that did not fire.
    fn report(&self, tier: &str, seeds: usize) -> String {
        let boundaries = HookPointV1::ALL
            .iter()
            .map(|hook| {
                format!(
                    "{}={}",
                    hook.as_wire(),
                    self.hooks.get(hook.as_wire()).copied().unwrap_or_default()
                )
            })
            .collect::<Vec<_>>()
            .join(" ");
        format!(
            "tdmem-5lc {tier}: {seeds} seeds, {} process kills, {} multi-life seeds, {} refusal \
             seeds; kills per boundary: {boundaries}",
            self.kills, self.multi_life_seeds, self.refusal_seeds
        )
    }
}

/// Runs one seed end to end over the mounted journey.
async fn run_seed(seed: u64) -> FuzzCoverageV1 {
    let plan = FuzzPlanV1::from_seed(seed);
    let label = format!("seed-{seed:#018x}");
    let directory = tempfile::tempdir().expect("seed directory");
    let paths = JourneyPathsV1::in_directory(directory.path());
    fs::create_dir_all(&paths.root).expect("journal root");
    write_refusals(&paths.refusals, &plan.refused);

    let mut coverage = FuzzCoverageV1 {
        multi_life_seeds: usize::from(plan.lives > 1),
        refusal_seeds: usize::from(!plan.refused.is_empty()),
        ..FuzzCoverageV1::default()
    };
    let mut stream = SeedStreamV1::from_seed(seed ^ 0xA5A5_5A5A_A5A5_5A5A);
    let mut delivery_kills = 0_usize;

    for life in 0..plan.lives {
        let mut candidates = reachable_kill_points(&paths, &plan);
        if delivery_kills >= MAX_DELIVERY_KILLS {
            candidates.retain(|point| !point.hook.spends_an_attempt());
        }
        if candidates.is_empty() {
            break;
        }
        // The seed's preferred boundary goes first when this journey can reach
        // it at all, which is what makes a window of consecutive seeds cover
        // every boundary deterministically.
        let preferred: Vec<KillPointV1> = candidates
            .iter()
            .copied()
            .filter(|point| point.hook == plan.preferred)
            .collect();
        let choice = if life == 0 && !preferred.is_empty() {
            preferred[stream.below(preferred.len())]
        } else {
            candidates[stream.below(candidates.len())]
        };
        if choice.hook.spends_an_attempt() {
            delivery_kills += 1;
        }
        let spec = ChildLifeSpecV1 {
            directory: directory.path().to_path_buf(),
            hook: choice.hook,
            target: choice.sequence,
            life,
            settle: plan.committed.clone(),
        };
        // A child life is a blocking process, so it runs off the runtime's
        // worker threads rather than parking one for a whole life.
        let killed = tokio::task::spawn_blocking(move || crash_at(&spec))
            .await
            .expect("crash driver");
        if let Err(error) = killed {
            panic!(
                "{label} life {life} at {} sequence {}: {error}",
                choice.hook.as_wire(),
                choice.sequence
            );
        }
        coverage.record(choice.hook);
        // Checked in the mid-flight state, where a watermark that ran ahead of
        // its own receipt would still be visible.
        assert_acknowledged_watermark(&paths, &label);
    }

    recover(&paths, &plan.committed, "final").await;
    assert_acknowledged_watermark(&paths, &label);
    assert_no_loss_and_no_double_effect(&paths, &plan.committed, &plan.refused, &label);
    assert_refusals_stay_terminal(&paths, &plan.committed, &plan.refused, &label).await;
    coverage
}

/// Everything both tiers assert about a finished window.
fn assert_window_is_not_vacuous(total: &FuzzCoverageV1, seeds: usize) {
    assert!(seeds > 0, "the seed window ran nothing");
    assert!(
        total.kills >= seeds,
        "the window spent {} kills across {seeds} seeds",
        total.kills
    );
    for hook in HookPointV1::ALL {
        assert!(
            total.hooks.contains_key(hook.as_wire()),
            "the seed window never killed at {}: {}",
            hook.as_wire(),
            total.report("window", seeds)
        );
    }
    assert!(
        total.multi_life_seeds > 0,
        "the seed window never ran a journey that died more than once"
    );
    assert!(
        total.refusal_seeds > 0,
        "the seed window never gave the provider a position to refuse"
    );
}

/// The seeds one window covers: the permanent regressions first, then a fixed
/// run from the base.
fn window(base: u64, budget: u64) -> Vec<u64> {
    REGRESSION_SEEDS
        .iter()
        .copied()
        .chain((0..budget).map(|offset| base.wrapping_add(offset)))
        .collect()
}

/// Acceptance (`tdmem-5lc`), developer tier: a short window of randomized crash
/// plans against the **mounted** journey holds every invariant.
///
/// This tier exists so a bare `cargo test` on this file is honest and fast. The
/// gate the convergence map registers is the soak tier below, which runs the
/// same seeds and hundreds more.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seeded_crash_restart_fuzzing_holds_every_invariant_on_the_mounted_journey() {
    let base = base_seed();
    let single = base != BASE_SEED;
    let mut total = FuzzCoverageV1::default();
    let mut count = 0_usize;
    for seed in window(base, seed_budget()) {
        total.absorb(&run_seed(seed).await);
        count += 1;
    }
    println!("{}", total.report("developer tier", count));
    if single {
        assert!(count > 0, "the seed window ran nothing");
        return;
    }
    assert_window_is_not_vacuous(&total, count);
}

/// Acceptance (`tdmem-5lc`), soak tier: **two hundred** deterministic seeds of
/// randomized crash plans against the mounted journey, inside a bounded wall
/// budget.
///
/// This is the tier the bead's "run N seeds (hundreds)" names and the one
/// `product/upstream/convergence-map.json` registers as the repeatable mounted
/// verification. It is `#[ignore]`d so that a developer's `cargo test` runs the
/// short tier, and the registered command runs exactly this one with
/// `--ignored`.
///
/// Seeds are independent journeys over their own temporary directories, so the
/// window runs [`SOAK_LANES`] of them at a time. The budget is enforced while
/// the window runs rather than reported afterwards: a window that cannot finish
/// inside it fails naming the seeds it never reached, so this tier can never
/// silently shrink into a shorter one.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "soak tier: hundreds of seeds and thousands of process kills; run it by name"]
async fn mounted_crash_restart_soak_holds_every_invariant_across_two_hundred_seeds() {
    let seeds = window(base_seed(), env_u64(SOAK_SEEDS_ENV, SOAK_SEEDS));
    let planned = seeds.len();
    let lanes = usize::try_from(env_u64(
        SOAK_LANES_ENV,
        u64::try_from(SOAK_LANES).expect("soak lanes"),
    ))
    .expect("soak lanes")
    .max(1);
    let budget = Duration::from_secs(env_u64(
        SOAK_BUDGET_ENV,
        u64::try_from(SOAK_BUDGET.as_secs()).expect("soak budget"),
    ));
    let deadline = Instant::now() + budget;

    let mut pending = seeds.into_iter();
    let mut running = tokio::task::JoinSet::new();
    let mut total = FuzzCoverageV1::default();
    let mut finished = 0_usize;
    let mut unreached: Vec<u64> = Vec::new();

    loop {
        while running.len() < lanes {
            if Instant::now() >= deadline {
                unreached.extend(pending.by_ref());
                break;
            }
            let Some(seed) = pending.next() else {
                break;
            };
            running.spawn(async move { run_seed(seed).await });
        }
        let Some(joined) = running.join_next().await else {
            break;
        };
        total.absorb(&joined.expect("soak seed"));
        finished += 1;
    }

    println!("{}", total.report("soak tier", finished));
    assert!(
        unreached.is_empty(),
        "the soak window spent its {budget:?} budget after {finished} of {planned} seeds; \
         {} seeds were never run, starting at {:#018x}",
        unreached.len(),
        unreached.first().copied().unwrap_or_default()
    );
    assert_eq!(
        finished, planned,
        "the soak window finished {finished} of {planned} seeds"
    );
    if std::env::var(SOAK_SEEDS_ENV).is_err() {
        assert!(
            planned >= 200,
            "the soak window ran {planned} seeds, which is not the hundreds this tier is for"
        );
    }
    assert_window_is_not_vacuous(&total, finished);
}
