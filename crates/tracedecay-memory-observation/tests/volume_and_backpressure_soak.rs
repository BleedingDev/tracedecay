// A soak whose measurements are not emitted is a pass/fail bit, not a soak:
// peak queue depth, drop count, foreground percentiles, and disk growth per
// thousand observations are the deliverable, and they have to reach the run
// log to be read off it.
#![allow(clippy::print_stdout)]

//! Volume and backpressure soak through the journal and the dispatcher.
//!
//! Everything else in this suite proves one property against a handful of
//! rows. This file pushes real volume — ten thousand records by default, and
//! whatever `TDMEM_SOAK_RECORDS` says otherwise — through the *pair* of
//! runtimes against a real file-backed journal, and asserts the properties
//! that only appear once a producer outruns its provider:
//!
//! 1. **The queue stays inside its declared bound.** Every measurement taken
//!    during the run, and every measurement the journal itself reports, is
//!    checked against `max_queue_items` and `max_queue_bytes`. A ceiling that
//!    holds for eight rows and not for eight thousand is not a ceiling.
//! 2. **Nothing is dropped.** A shed is a refusal the source re-presents, so
//!    the accounting at the end is exact: every source sequence in `1..=N`
//!    appears in the journal exactly once and every one of them reaches the
//!    terminal `Acknowledged`. Zero is the only acceptable drop count.
//! 3. **The thresholds are honoured at or above their declared point.** The
//!    soak re-reads the lane itself before every admission and recomputes the
//!    projected utilization from the journal's own pressure, then checks the
//!    gate's answer against it: an optional record is never admitted above
//!    `shed_optional_at_ppm`, a required one is never admitted above
//!    `refuse_at_ppm`, and every shed names a measurement that justifies it.
//! 4. **The foreground budget is honoured.** Each record is admitted through
//!    its own `ingest` call whose real wall time is measured and fed back into
//!    the gate exactly as a mounted host feeds it. No admission may overrun
//!    the hard budget, and the lane must never have to shed on foreground
//!    grounds — which is what "admission did not degrade as the journal grew"
//!    means in practice.
//! 5. **Nothing starves.** Every iteration must make progress; a lane that
//!    admits nothing and delivers nothing is a stall, and the soak fails on
//!    the first one rather than spinning out a round budget. Both load classes
//!    must finish, so the reserved band cannot be a permanent refusal of the
//!    optional stream.
//!
//! The provider is deliberately *slow but alive*: it answers every attempt,
//! and it spends ninety per cent of the per-attempt share of the round's
//! budget doing it. The producer is deliberately faster than the drain — it
//! offers half again as many records per iteration as one drain can deliver —
//! so the lane really does fill, really does cross both thresholds, and really
//! does have to recover.
//!
//! Time is virtual. The runtimes own no clock, so the soak supplies one, and
//! the provider's latency and the queue's backlog age are measured on it.
//! Foreground admission latency is the one figure taken from the real clock,
//! because it is the one figure about this machine rather than about the
//! model.
//!
//! The second test in this file is the open-cost proof: a journal whose
//! withheld audit table holds thousands of rows must open in about the time an
//! empty one does, because the audit is a bounded page plus a resumable
//! cursor rather than a full-table scan.

mod support;

use std::cell::Cell;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::time::Instant;

use support::{
    Builder, DAY, MINUTE, PROVENANCE_DIGEST, PROVIDER, PROVIDER_RECEIPT_DIGEST, SECOND, T0,
    TestIngestControl, TestResult, gate_with, lane, stream_key, withheld_at,
};

use tracedecay_memory_observation::{
    AdmissionDecisionV1, BackpressurePolicyV1, BackpressureReasonV1, BackpressureRefusalV1,
    DeliveryAttemptV1, DeliveryControlV1, DeliveryRuntimeV1, DeliveryStateV1, DeliveryWakeV1,
    DispatchPolicyV1, DispatchRequestV1, IngressControlV1, IngressRuntimeV1,
    JournalInspectionFilterV1, LeaseRequestV1, LeasedObservationV1, OPEN_WITHHELD_AUDIT_ROWS,
    ObservationAdmissionAdapterV1, ObservationDispatchPortV1, ObservationJournalError,
    ObservationJournalReaderV1, ObservationLaneKeyV1, ObservationLoadClassV1,
    ObservationRetentionPortV1, ProviderDeliveryAdapterV1, QueuePressureV1, RetentionClassV1,
    RetentionPolicyV1, RetryBackoffV1, SourceRecordV1, SourceSequenceV1, SqliteObservationJournal,
    UTILIZATION_SCALE_PPM,
};
use tracedecay_memory_provider_api::contract::TerminalCode;
use tracedecay_memory_provider_api::{
    CommittedEffectEvidence, FallbackDirective, ProviderOperation, TerminalRecord,
};

// ------------------------------------------------------------------ bounds --

/// Records the soak drives by default. `TDMEM_SOAK_RECORDS` overrides it, so a
/// longer run is a environment variable rather than a rebuild.
const DEFAULT_SOAK_RECORDS: u64 = 10_000;

/// Non-terminal rows the lane may hold. Small relative to the record count on
/// purpose: the whole point is to cross the thresholds repeatedly.
const MAX_QUEUE_ITEMS: u64 = 128;

/// Non-terminal bytes the lane may hold. Sized so that the byte ceiling and
/// the item ceiling are both reachable given the payload sizes below, rather
/// than leaving one of them decorative.
const MAX_QUEUE_BYTES: u64 = 163_840;

/// Rows one delivery round leases.
const BATCH_MAX_ITEMS: u32 = 32;

/// Rounds one drain may run: two, so a drain delivers sixty-four rows.
const MAX_ROUNDS_PER_DRAIN: u32 = 2;

/// Records the producer offers per iteration: half again what one drain can
/// deliver, so the lane fills rather than idling.
const OFFERED_PER_ITERATION: u64 = 96;

/// Wall budget one delivery round may consume, in virtual micros.
const ATTEMPT_BUDGET_MICROS: i64 = 320_000;

/// Virtual time one admission consumes, so backlog age is a real measurement
/// rather than a constant zero.
const ADMISSION_TICK_MICROS: i64 = 1_000;

/// Utilization at or above which the optional stream stops being admitted.
const SHED_OPTIONAL_AT_PPM: u32 = 600_000;

/// Utilization at or above which every class stops being admitted.
const REFUSE_AT_PPM: u32 = 900_000;

/// Budget one foreground admission is declared to have. Generous by the
/// standard of a single indexed read plus one committed append, and tight
/// enough that a lane whose admission path had gone quadratic would trip it.
const FOREGROUND_BUDGET_MICROS: i64 = 250_000;

/// Consecutive over-budget admissions that start shedding optional work.
const FOREGROUND_BREACH_STREAK: u32 = 3;

/// The absolute ceiling a single foreground admission may not cross, whatever
/// else the machine is doing. An admission slower than this is a defect, not
/// noise.
const FOREGROUND_HARD_BUDGET_MICROS: i64 = 2 * SECOND;

/// Seed for every per-record draw. Recorded here so a failure reproduces.
const SOAK_SEED: u64 = 0x5150_7A17_D0C0_FFEE;

/// Smallest and largest canonical payload the soak mints.
const MIN_PAYLOAD_BYTES: u64 = 96;
const MAX_PAYLOAD_BYTES: u64 = 3_072;

/// Bytes of store growth per admitted observation the soak refuses to exceed.
/// Comfortably above the largest payload plus its row, receipt, and index
/// overhead, and far below anything that would indicate the journal is
/// retaining something per record that it should not.
const MAX_STORE_BYTES_PER_RECORD: u64 = 16_384;

// --------------------------------------------------------------- fixtures --

#[derive(Debug)]
struct SoakError(String);

impl Display for SoakError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for SoakError {}

/// One deterministic draw for one source position.
///
/// Derived from the seed and the sequence rather than from a running
/// generator, because a record that is shed is re-presented later and must
/// come back identical: `classify` and `decide` are re-entered for it, and
/// ingress refuses the batch if the two disagree.
const fn draw(sequence: u64, salt: u64) -> u64 {
    let mut z = sequence
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(SOAK_SEED)
        .wrapping_add(salt.wrapping_mul(0xD1B5_4A32_D192_ED03));
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Retention class of one source position: seventy advisory records, then a
/// burst of thirty durable ones, repeating.
///
/// The shape is deliberate rather than random. One source stream is admitted
/// in order and stops at the first refusal, so the *only* way the lane can
/// climb past the point where optional work starts being shed is a run of
/// required records — and with an independently drawn class the runs long
/// enough to cross the reserved band never occur. A soak whose lane could
/// only ever reach the shed threshold would leave the refusal threshold, and
/// therefore the entire reserved band, unexercised: the burst is what makes
/// "required work keeps the headroom to itself" a measured property instead
/// of an untested comment.
const fn retention_class_at(sequence: u64) -> RetentionClassV1 {
    if sequence % 100 < 70 {
        RetentionClassV1::Session
    } else {
        RetentionClassV1::Project
    }
}

/// Canonical payload body of one source position, sized by its own draw.
fn body_at(sequence: u64) -> String {
    let span = MAX_PAYLOAD_BYTES - MIN_PAYLOAD_BYTES;
    let filler = MIN_PAYLOAD_BYTES + (draw(sequence, 2) % span);
    let mut body = String::with_capacity(usize::try_from(filler).unwrap_or(0) + 32);
    body.push_str("{\"seq\":");
    body.push_str(&sequence.to_string());
    body.push_str(",\"m\":\"");
    for _ in 0..filler {
        body.push('x');
    }
    body.push_str("\"}");
    body
}

/// The soak's virtual clock, shared by admission, the gate, the dispatcher,
/// and the provider's own latency.
#[derive(Debug)]
struct VirtualClock {
    now: Cell<i64>,
}

impl VirtualClock {
    const fn at(now: i64) -> Self {
        Self {
            now: Cell::new(now),
        }
    }

    fn now(&self) -> i64 {
        self.now.get()
    }

    fn advance(&self, micros: i64) -> i64 {
        let next = self.now.get().saturating_add(micros);
        self.now.set(next);
        next
    }
}

/// Admission for the soak: one envelope per source position, in that
/// position's own drawn retention class and payload size.
struct SoakAdmission<'a> {
    lane: ObservationLaneKeyV1,
    clock: &'a VirtualClock,
}

impl ObservationAdmissionAdapterV1 for SoakAdmission<'_> {
    type Record = ();
    type Error = SoakError;
    type Control = dyn IngressControlV1;

    fn lane(&self, _record: &SourceRecordV1<Self::Record>) -> ObservationLaneKeyV1 {
        self.lane.clone()
    }

    fn classify(&self, record: &SourceRecordV1<Self::Record>) -> ObservationLoadClassV1 {
        ObservationLoadClassV1::of(retention_class_at(record.source_sequence.0))
    }

    fn decide(
        &self,
        record: &SourceRecordV1<Self::Record>,
        _control: &Self::Control,
    ) -> Result<AdmissionDecisionV1, Self::Error> {
        let sequence = record.source_sequence.0;
        let admitted_at = self.clock.now();
        let admitted = Builder {
            retention_class: retention_class_at(sequence),
            body: body_at(sequence),
            admitted_at,
            // Long enough that nothing expires during the soak itself; the
            // reclaim phase moves the clock past it deliberately.
            expires_at: admitted_at.saturating_add(30 * DAY),
            deadline: admitted_at.saturating_add(7 * DAY),
            ..Builder::at_sequence(sequence)
        }
        .build()
        .map_err(|error| SoakError(error.to_string()))?;
        Ok(AdmissionDecisionV1::Admit(Box::new(admitted)))
    }
}

/// A provider that is slow but alive.
///
/// It answers every attempt with a success terminal derived from the leased
/// row, and it spends ninety per cent of the per-attempt share of the round's
/// budget doing so. The share, not the whole budget: a round leases
/// `BATCH_MAX_ITEMS` rows against one deadline, so a provider that spent
/// ninety per cent of the *round's* remaining budget on its first row would
/// leave nothing for the other thirty-one and the soak would be measuring a
/// timeout, not a slow provider.
struct SlowButAliveProvider<'a> {
    clock: &'a VirtualClock,
    answers: Cell<u64>,
    /// Largest fraction, in parts per million, of any attempt's own control
    /// budget that this provider actually consumed. Reported, and asserted
    /// against the deadline the runtime handed it.
    peak_deadline_use_ppm: Cell<u32>,
}

impl SlowButAliveProvider<'_> {
    fn answers(&self) -> u64 {
        self.answers.get()
    }
}

impl ProviderDeliveryAdapterV1 for SlowButAliveProvider<'_> {
    type Error = SoakError;

    fn deliver(
        &self,
        leased: &LeasedObservationV1,
        control: &DeliveryControlV1,
    ) -> Result<DeliveryAttemptV1, Self::Error> {
        let started_at_unix_micros = self.clock.now();
        let share = ATTEMPT_BUDGET_MICROS / i64::from(BATCH_MAX_ITEMS);
        let spend = share.saturating_mul(9) / 10;
        let remaining = control.remaining_micros(started_at_unix_micros);
        if remaining > 0 {
            let used = u64::try_from(spend.min(remaining)).unwrap_or(0);
            let ppm = u32::try_from(
                used.saturating_mul(u64::from(UTILIZATION_SCALE_PPM))
                    / u64::try_from(remaining).unwrap_or(1).max(1),
            )
            .unwrap_or(UTILIZATION_SCALE_PPM);
            if ppm > self.peak_deadline_use_ppm.get() {
                self.peak_deadline_use_ppm.set(ppm);
            }
        }
        let finished_at_unix_micros = self.clock.advance(spend);
        self.answers.set(self.answers.get().saturating_add(1));
        let terminal = TerminalRecord::new(
            ProviderOperation::Observe,
            leased.target.provider_id.clone(),
            TerminalCode::Success,
            CommittedEffectEvidence::committed(
                1,
                2,
                Vec::new(),
                PROVIDER_RECEIPT_DIGEST,
                PROVENANCE_DIGEST,
            )
            .map_err(|error| SoakError(error.to_string()))?,
            FallbackDirective::forbidden(),
            format!("observe-{}", leased.observation_id.as_str()),
            leased.exact_scope_sha256.clone(),
            None,
        )
        .map_err(|error| SoakError(error.to_string()))?;
        Ok(DeliveryAttemptV1::Answered {
            terminal: Box::new(terminal),
            started_at_unix_micros,
            finished_at_unix_micros,
        })
    }
}

// ----------------------------------------------------------------- policies --

fn soak_retention_policy() -> RetentionPolicyV1 {
    RetentionPolicyV1 {
        ephemeral_max_age_micros: 10 * MINUTE,
        session_max_age_micros: DAY,
        project_max_age_micros: 30 * DAY,
        profile_max_age_micros: 365 * DAY,
        receipt_retention_micros: 7 * DAY,
        max_queue_items: MAX_QUEUE_ITEMS,
        max_queue_bytes: MAX_QUEUE_BYTES,
        max_attempts: 5,
        backoff_base_micros: SECOND,
        backoff_max_micros: 30 * SECOND,
        sweep_batch_rows: 256,
    }
}

fn soak_backpressure_policy() -> BackpressurePolicyV1 {
    BackpressurePolicyV1 {
        shed_optional_at_ppm: SHED_OPTIONAL_AT_PPM,
        refuse_at_ppm: REFUSE_AT_PPM,
        // Generous: the age trigger has its own test, and a soak whose sheds
        // were driven by age would prove nothing about the utilization band.
        max_backlog_age_micros: 30 * MINUTE,
        foreground_budget_micros: FOREGROUND_BUDGET_MICROS,
        foreground_breach_streak: FOREGROUND_BREACH_STREAK,
    }
}

fn soak_dispatch_policy() -> DispatchPolicyV1 {
    DispatchPolicyV1 {
        lease_duration_micros: 60 * SECOND,
        batch_max_items: BATCH_MAX_ITEMS,
        batch_max_bytes: MAX_QUEUE_BYTES,
        attempt_budget_micros: ATTEMPT_BUDGET_MICROS,
        reap_budget: 64,
        max_rounds_per_drain: MAX_ROUNDS_PER_DRAIN,
        drain_budget_micros: 2 * SECOND,
    }
}

// ------------------------------------------------------------- measurement --

/// The utilization one more row of `additional_bytes` would put the lane at,
/// recomputed by the soak from the journal's own pressure so the gate's answer
/// is checked against an independent reading rather than against itself.
fn projected_ppm(pressure: &QueuePressureV1, additional_bytes: u64) -> u32 {
    let items = ratio_ppm(
        pressure.queue_items.saturating_add(1),
        pressure.max_queue_items,
    );
    let bytes = ratio_ppm(
        pressure.queue_bytes.saturating_add(additional_bytes),
        pressure.max_queue_bytes,
    );
    items.max(bytes)
}

fn ratio_ppm(used: u64, capacity: u64) -> u32 {
    if capacity == 0 {
        return UTILIZATION_SCALE_PPM;
    }
    u32::try_from(used.saturating_mul(u64::from(UTILIZATION_SCALE_PPM)) / capacity)
        .unwrap_or(UTILIZATION_SCALE_PPM)
}

/// Every number the soak reports, so a run is a measurement rather than a
/// pass/fail bit.
#[derive(Debug, Default)]
struct SoakMetrics {
    iterations: u64,
    appended: u64,
    shed: u64,
    delivered: u64,
    peak_queue_items: u64,
    peak_queue_bytes: u64,
    peak_utilization_ppm: u32,
    peak_backlog_age_micros: i64,
    sheds_optional: u64,
    sheds_required: u64,
    sheds_by_ceiling: u64,
    sheds_by_foreground: u64,
    sheds_by_age: u64,
    max_shed_repeats: u64,
    foreground_micros: Vec<i64>,
}

impl SoakMetrics {
    fn foreground_percentile(&self, permille: u64) -> i64 {
        if self.foreground_micros.is_empty() {
            return 0;
        }
        let mut sorted = self.foreground_micros.clone();
        sorted.sort_unstable();
        let index = usize::try_from(
            (sorted.len() as u64)
                .saturating_sub(1)
                .saturating_mul(permille)
                / 1_000,
        )
        .unwrap_or(0);
        sorted.get(index).copied().unwrap_or(0)
    }

    fn foreground_max(&self) -> i64 {
        self.foreground_micros.iter().copied().max().unwrap_or(0)
    }
}

/// Bytes the store occupies on disk, database file plus its write-ahead log.
fn store_bytes(path: &std::path::Path) -> u64 {
    let database = std::fs::metadata(path).map(|meta| meta.len()).unwrap_or(0);
    let log = std::fs::metadata(path.with_extension("sqlite3-wal"))
        .map(|meta| meta.len())
        .unwrap_or(0);
    database.saturating_add(log)
}

/// Checks one shed against the measurement the gate refused it on.
///
/// This is where "the thresholds are honoured at or above their declared
/// point" is enforced: a refusal has to name a reading that justifies it, and
/// a required record may only be refused by the ceiling, the refusal
/// threshold, or a measured state that is already saturated.
fn verify_shed(refusal: &BackpressureRefusalV1, metrics: &mut SoakMetrics) -> TestResult {
    match refusal.load_class {
        ObservationLoadClassV1::Optional => metrics.sheds_optional += 1,
        ObservationLoadClassV1::Required => metrics.sheds_required += 1,
    }
    match refusal.reason {
        BackpressureReasonV1::QueueCeiling => {
            metrics.sheds_by_ceiling += 1;
            if refusal.backlog.queue_items < refusal.backlog.max_queue_items
                && refusal
                    .backlog
                    .queue_bytes
                    .saturating_add(refusal.additional_bytes)
                    <= refusal.backlog.max_queue_bytes
            {
                return Err(Box::new(SoakError(format!(
                    "queue-ceiling shed below the ceiling: {} of {} items, {} of {} bytes",
                    refusal.backlog.queue_items,
                    refusal.backlog.max_queue_items,
                    refusal.backlog.queue_bytes,
                    refusal.backlog.max_queue_bytes,
                ))));
            }
        }
        BackpressureReasonV1::QueueUtilization => {
            let threshold = match refusal.load_class {
                ObservationLoadClassV1::Optional => SHED_OPTIONAL_AT_PPM,
                ObservationLoadClassV1::Required => REFUSE_AT_PPM,
            };
            // Either the projection crossed the class's own threshold, or the
            // lane was already measured at it. One of the two must hold; a
            // refusal that can point at neither is a refusal below threshold.
            if refusal.projected_utilization_ppm < threshold
                && refusal.backlog.utilization_ppm < threshold
            {
                return Err(Box::new(SoakError(format!(
                    "{} shed at {} ppm projected / {} ppm measured, below its {} ppm threshold",
                    refusal.load_class.as_wire(),
                    refusal.projected_utilization_ppm,
                    refusal.backlog.utilization_ppm,
                    threshold,
                ))));
            }
        }
        BackpressureReasonV1::BacklogAge => {
            metrics.sheds_by_age += 1;
            if refusal.load_class == ObservationLoadClassV1::Required {
                return Err(Box::new(SoakError(
                    "backlog age shed a required record: the reserved band was not reserved"
                        .to_owned(),
                )));
            }
        }
        BackpressureReasonV1::ForegroundBudget => {
            metrics.sheds_by_foreground += 1;
            if refusal.load_class == ObservationLoadClassV1::Required {
                return Err(Box::new(SoakError(
                    "foreground budget shed a required record: the reserved band was not reserved"
                        .to_owned(),
                )));
            }
        }
    }
    Ok(())
}

fn soak_records() -> u64 {
    std::env::var("TDMEM_SOAK_RECORDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_SOAK_RECORDS)
}

// ---------------------------------------------------------------- the soak --

#[test]
fn volume_soak_keeps_the_queue_bounded_drops_nothing_and_never_starves() -> TestResult {
    let records = soak_records();
    let directory = tempfile::TempDir::new()?;
    let path = directory.path().join("soak-journal.sqlite3");
    let store = SqliteObservationJournal::open(&path, soak_retention_policy())?;
    let bytes_before = store_bytes(&path);

    let clock = VirtualClock::at(T0);
    let lane = lane()?;
    let admission = SoakAdmission {
        lane: lane.clone(),
        clock: &clock,
    };
    let provider = SlowButAliveProvider {
        clock: &clock,
        answers: Cell::new(0),
        peak_deadline_use_ppm: Cell::new(0),
    };
    let wake = DeliveryWakeV1::new();
    let gate = gate_with(soak_backpressure_policy())?;
    let control = TestIngestControl::at(T0, 365 * DAY);
    let ingress = IngressRuntimeV1::new(&store, &admission, &wake, &gate, &control);
    let delivery = DeliveryRuntimeV1::new(&store, &provider, &wake);
    let dispatch = soak_dispatch_policy();
    let stream = stream_key("session-1")?;

    let mut metrics = SoakMetrics::default();
    let mut next_sequence: u64 = 1;
    let mut shed_repeats: BTreeMap<u64, u64> = BTreeMap::new();
    // Finite by construction, and far above what a healthy lane needs: the
    // producer offers ninety-six per iteration and one drain delivers
    // sixty-four, so a converging soak finishes in roughly `records / 64`.
    let iteration_budget = records / 8 + 512;

    while next_sequence <= records || delivered_rows(&store)? < records {
        metrics.iterations += 1;
        if metrics.iterations > iteration_budget {
            return Err(Box::new(SoakError(format!(
                "soak did not converge within {iteration_budget} iterations: {} of {records} \
                 appended, {} delivered",
                next_sequence - 1,
                metrics.delivered,
            ))));
        }
        let appended_before = metrics.appended;
        let delivered_before = metrics.delivered;

        // ---- admission: one record per call, so the measured wall time is a
        // real per-record foreground admission latency rather than an average.
        let offer_until = (next_sequence + OFFERED_PER_ITERATION - 1).min(records);
        while next_sequence <= offer_until {
            let sequence = next_sequence;
            let class = ObservationLoadClassV1::of(retention_class_at(sequence));
            // The lane as the journal holds it, read by the soak, before the
            // gate reads it for itself.
            let pressure = store.lane_pressure(&lane)?;
            let projected = projected_ppm(&pressure, 0);

            control.set_now(clock.now());
            let resume = ingress.recover(&stream)?;
            let record = SourceRecordV1 {
                stream: stream.clone(),
                source_sequence: SourceSequenceV1(sequence),
                source_event_id: format!("event-{sequence}"),
                source_event_revision: 0,
                record: (),
            };
            let started = Instant::now();
            let report = ingress.ingest(&resume, &[record])?;
            let elapsed = i64::try_from(started.elapsed().as_micros()).unwrap_or(i64::MAX);
            metrics.foreground_micros.push(elapsed);
            // Exactly what a mounted host does with the measurement: hand it
            // back to the gate, so a degrading admission path becomes a shed
            // trigger instead of an invisible cost.
            gate.observe_foreground(elapsed);
            clock.advance(ADMISSION_TICK_MICROS);

            if let Some(backlog) = report.backlog {
                metrics.peak_queue_items = metrics.peak_queue_items.max(backlog.queue_items);
                metrics.peak_queue_bytes = metrics.peak_queue_bytes.max(backlog.queue_bytes);
                metrics.peak_utilization_ppm =
                    metrics.peak_utilization_ppm.max(backlog.utilization_ppm);
                metrics.peak_backlog_age_micros = metrics
                    .peak_backlog_age_micros
                    .max(backlog.oldest_backlog_age_micros);
                if backlog.queue_items > backlog.max_queue_items
                    || backlog.queue_bytes > backlog.max_queue_bytes
                {
                    return Err(Box::new(SoakError(format!(
                        "queue exceeded its declared bound: {} of {} items, {} of {} bytes",
                        backlog.queue_items,
                        backlog.max_queue_items,
                        backlog.queue_bytes,
                        backlog.max_queue_bytes,
                    ))));
                }
            }
            if let Some(halt) = report.halted_on {
                return Err(Box::new(SoakError(format!(
                    "journal refused sequence {} with {:?}",
                    halt.source_sequence.0, halt.outcome,
                ))));
            }
            if let Some(stop) = report.stopped_on {
                return Err(Box::new(SoakError(format!(
                    "ingest stopped on the caller's own bound at sequence {}: {}",
                    stop.source_sequence.0,
                    stop.reason.as_wire(),
                ))));
            }

            if report.appended == 1 {
                metrics.appended += 1;
                // The gate admitted it, so the independently measured
                // projection must have been inside this class's threshold.
                let ceiling = match class {
                    ObservationLoadClassV1::Optional => SHED_OPTIONAL_AT_PPM,
                    ObservationLoadClassV1::Required => REFUSE_AT_PPM,
                };
                if projected > ceiling {
                    return Err(Box::new(SoakError(format!(
                        "{} sequence {sequence} was admitted at {projected} ppm projected, above \
                         its {ceiling} ppm threshold",
                        class.as_wire(),
                    ))));
                }
                next_sequence += 1;
                continue;
            }

            let shed = report
                .shed_on
                .ok_or("ingest neither appended nor shed the offered record")?;
            metrics.shed += 1;
            let repeats = shed_repeats.entry(sequence).or_insert(0);
            *repeats += 1;
            metrics.max_shed_repeats = metrics.max_shed_repeats.max(*repeats);
            if shed.refusal.load_class != class {
                return Err(Box::new(SoakError(format!(
                    "shed classified sequence {sequence} as {} but the record is {}",
                    shed.refusal.load_class.as_wire(),
                    class.as_wire(),
                ))));
            }
            verify_shed(&shed.refusal, &mut metrics)?;
            // A shed is a refusal, not a drop: the watermark holds here and
            // this exact sequence is offered again after the lane drains.
            break;
        }

        // ---- delivery: one bounded drain against the slow-but-alive provider.
        let now = clock.now();
        let bounds = dispatch.drain_bounds(store.policy(), now)?;
        let request = DispatchRequestV1 {
            lease: LeaseRequestV1 {
                provider_id: PROVIDER.to_owned(),
                registration_revision: 4,
                provider_instance_id: "soak-instance".to_owned(),
                exact_scope_sha256: None,
                lease_owner: "soak-dispatcher".to_owned(),
                now_unix_micros: now,
                lease_duration_micros: dispatch.lease_duration_micros,
                max_items: dispatch.batch_max_items,
                max_bytes: dispatch.batch_max_bytes,
            },
            retry_backoff: RetryBackoffV1::of(store.policy()),
            attempt_budget_micros: dispatch.attempt_budget_micros,
        };
        let drained = delivery.drain(&request, &bounds, || clock.now())?;
        if !drained.totals.failures.is_empty() {
            return Err(Box::new(SoakError(format!(
                "the slow-but-alive provider produced {} delivery failures; the first is {}",
                drained.totals.failures.len(),
                drained
                    .totals
                    .failures
                    .first()
                    .map_or_else(|| "<none>".to_owned(), |failure| failure.cause.to_string()),
            ))));
        }
        if drained.totals.leases_lost > 0 {
            return Err(Box::new(SoakError(format!(
                "{} leases were lost mid-round; a lease lapsed inside its own drain",
                drained.totals.leases_lost,
            ))));
        }
        metrics.delivered += u64::from(drained.totals.settled_terminal);

        if metrics.appended == appended_before && metrics.delivered == delivered_before {
            return Err(Box::new(SoakError(format!(
                "iteration {} admitted nothing and delivered nothing: the lane is starved at \
                 sequence {next_sequence}",
                metrics.iterations,
            ))));
        }
    }

    let bytes_after = store_bytes(&path);

    // ---- accounting: every offered record, exactly once, all terminal.
    let mut seen: Vec<bool> = vec![false; usize::try_from(records).unwrap_or(0)];
    let mut cursor: Option<String> = None;
    let mut rows_seen: u64 = 0;
    loop {
        let page = store.inspect(&JournalInspectionFilterV1 {
            limit: 512,
            after_cursor: cursor.clone(),
            ..JournalInspectionFilterV1::default()
        })?;
        for row in &page.rows {
            rows_seen += 1;
            if row.state != DeliveryStateV1::Acknowledged {
                return Err(Box::new(SoakError(format!(
                    "sequence {} settled as {:?}, not Acknowledged",
                    row.source_sequence.0, row.state,
                ))));
            }
            let index = usize::try_from(row.source_sequence.0.saturating_sub(1)).unwrap_or(0);
            let slot = seen
                .get_mut(index)
                .ok_or("the journal holds a sequence the soak never offered")?;
            if *slot {
                return Err(Box::new(SoakError(format!(
                    "sequence {} is journalled more than once",
                    row.source_sequence.0,
                ))));
            }
            *slot = true;
        }
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    let dropped = seen.iter().filter(|present| !**present).count();
    if dropped != 0 {
        return Err(Box::new(SoakError(format!(
            "{dropped} of {records} offered observations are absent from the journal"
        ))));
    }
    assert_eq!(rows_seen, records, "journal row count");
    assert_eq!(metrics.appended, records, "appended count");
    assert_eq!(metrics.delivered, records, "delivered count");
    assert_eq!(provider.answers(), records, "provider answer count");

    // ---- bounds actually bound.
    assert!(
        metrics.peak_queue_items <= MAX_QUEUE_ITEMS,
        "peak queue depth {} exceeded the {MAX_QUEUE_ITEMS}-item ceiling",
        metrics.peak_queue_items,
    );
    assert!(
        metrics.peak_queue_bytes <= MAX_QUEUE_BYTES,
        "peak queue bytes {} exceeded the {MAX_QUEUE_BYTES}-byte ceiling",
        metrics.peak_queue_bytes,
    );
    // The producer really did outrun the drain, and it did so hard enough to
    // exercise both thresholds. A soak that only ever reached the shed point
    // would have proven nothing about the refusal point, and one that never
    // refused a required record would have left the reserved band untested.
    assert!(
        metrics.sheds_optional > 0 && metrics.peak_utilization_ppm >= SHED_OPTIONAL_AT_PPM,
        "the lane never reached the shed threshold: peak {} ppm, {} optional sheds",
        metrics.peak_utilization_ppm,
        metrics.sheds_optional,
    );
    assert!(
        metrics.sheds_required > 0 && metrics.peak_utilization_ppm >= REFUSE_AT_PPM,
        "the lane never reached the refusal threshold: peak {} ppm, {} required sheds",
        metrics.peak_utilization_ppm,
        metrics.sheds_required,
    );

    // ---- the foreground budget.
    assert_eq!(
        metrics.sheds_by_foreground, 0,
        "the lane had to shed on foreground latency: admission overran its {FOREGROUND_BUDGET_MICROS} \
         micros budget {FOREGROUND_BREACH_STREAK} times running",
    );
    let foreground_max = metrics.foreground_max();
    assert!(
        foreground_max <= FOREGROUND_HARD_BUDGET_MICROS,
        "one foreground admission took {foreground_max} micros, past the \
         {FOREGROUND_HARD_BUDGET_MICROS} micros hard budget",
    );

    // ---- disk growth stays proportional to what was stored.
    let grew = bytes_after.saturating_sub(bytes_before);
    assert!(
        grew <= records.saturating_mul(MAX_STORE_BYTES_PER_RECORD),
        "the store grew {grew} bytes for {records} observations, past the \
         {MAX_STORE_BYTES_PER_RECORD} bytes-per-record bound",
    );

    // ---- retention reclaims what the soak wrote.
    //
    // Two phases, because the store deliberately does not delete a row in the
    // same instant it purges its bytes: content ages out on the retention
    // class, and the row itself only leaves once its audit window has closed
    // behind the purge. Moving the clock twice is what a real store's calendar
    // does, and it is the only honest way to reach the second step.
    let mut sweeps = 0_u32;
    let mut payloads_purged: u64 = 0;
    let mut rows_deleted: u64 = 0;
    for step in [400 * DAY, 30 * DAY] {
        let at = clock.advance(step);
        loop {
            let receipt = store.sweep_expired(at, 256)?;
            sweeps += 1;
            payloads_purged += u64::from(receipt.payloads_purged);
            rows_deleted += u64::from(receipt.journal_rows_deleted);
            if receipt.remaining_candidates == 0 {
                break;
            }
            if sweeps > 4_000 {
                return Err(Box::new(SoakError(format!(
                    "retention did not converge in {sweeps} bounded sweeps: {} candidates remain",
                    receipt.remaining_candidates,
                ))));
            }
        }
    }
    let remaining = store.inspect(&JournalInspectionFilterV1 {
        limit: 1,
        ..JournalInspectionFilterV1::default()
    })?;
    assert_eq!(
        remaining.total_rows, 0,
        "retention left {} rows behind after {sweeps} bounded sweeps",
        remaining.total_rows,
    );
    assert_eq!(payloads_purged, records, "every payload was purged");
    assert_eq!(rows_deleted, records, "every journal row was reclaimed");

    // ---- the measured run, for the record.
    let per_thousand = grew.saturating_mul(1_000) / records.max(1);
    println!(
        "soak seed={SOAK_SEED:#x} records={records} iterations={} appended={} shed={} \
         delivered={} peak_queue_items={} peak_queue_bytes={} peak_utilization_ppm={} \
         peak_backlog_age_micros={} sheds_optional={} sheds_required={} sheds_by_ceiling={} \
         max_shed_repeats={} dropped=0 foreground_p50={} foreground_p99={} foreground_max={} \
         provider_peak_deadline_use_ppm={} store_bytes_before={bytes_before} \
         store_bytes_after={bytes_after} store_bytes_per_1k={per_thousand} sweeps={sweeps} \
         payloads_purged={payloads_purged} rows_reclaimed={rows_deleted}",
        metrics.iterations,
        metrics.appended,
        metrics.shed,
        metrics.delivered,
        metrics.peak_queue_items,
        metrics.peak_queue_bytes,
        metrics.peak_utilization_ppm,
        metrics.peak_backlog_age_micros,
        metrics.sheds_optional,
        metrics.sheds_required,
        metrics.sheds_by_ceiling,
        metrics.max_shed_repeats,
        metrics.foreground_percentile(500),
        metrics.foreground_percentile(990),
        foreground_max,
        provider.peak_deadline_use_ppm.get(),
    );
    Ok(())
}

/// Non-terminal work is what the lane holds; this is everything the journal
/// has settled, which is what the soak's convergence condition reads.
fn delivered_rows(store: &SqliteObservationJournal) -> Result<u64, Box<dyn Error>> {
    Ok(store
        .inspect(&JournalInspectionFilterV1 {
            states: vec![DeliveryStateV1::Acknowledged],
            limit: 1,
            ..JournalInspectionFilterV1::default()
        })?
        .total_rows)
}

// -------------------------------------------------- bounded withheld audit --

/// Rows the open-cost proof puts in the withheld audit. Large enough that a
/// full-table revalidation on every open is measurable, small enough that
/// writing them is not the slow part of the test.
const WITHHELD_AUDIT_ROWS: u64 = 6_000;

#[test]
fn opening_a_journal_costs_the_same_whatever_the_withheld_audit_holds() -> TestResult {
    let directory = tempfile::TempDir::new()?;
    let empty_path = directory.path().join("empty.sqlite3");
    let full_path = directory.path().join("full.sqlite3");

    {
        let empty = SqliteObservationJournal::open(&empty_path, soak_retention_policy())?;
        drop(empty);
        let full = SqliteObservationJournal::open(&full_path, soak_retention_policy())?;
        for sequence in 1..=WITHHELD_AUDIT_ROWS {
            full.record_withheld(&withheld_at(sequence, "forget:soak")?)?;
        }
    }

    // Both opens are measured the same way, on the same disk, in the same
    // process: the difference between them is the audit table and nothing
    // else.
    let empty_open = time_open(&empty_path)?;
    let full_open = time_open(&full_path)?;

    // A full-table revalidation of six thousand rows is not free; a bounded
    // page of `OPEN_WITHHELD_AUDIT_ROWS` is. Allowing the loaded open a
    // millisecond of slack plus four times the empty open keeps this a
    // statement about *flatness* rather than a race against the disk, and it
    // still fails loudly if the scan comes back: an unbounded pass costs
    // roughly `WITHHELD_AUDIT_ROWS / 256` — over twenty times — more.
    let allowance = 1_000 + empty_open.saturating_mul(4);
    assert!(
        full_open <= allowance,
        "opening a journal with {WITHHELD_AUDIT_ROWS} withheld rows took {full_open} micros \
         against {empty_open} micros for an empty one, past the {allowance} micros allowance: \
         open is still scanning the whole audit table",
    );

    // Flat open cost must not have bought silence. The rest of the table is
    // still revalidated, one bounded page at a time, and the walk terminates.
    let store = SqliteObservationJournal::open(&full_path, soak_retention_policy())?;
    let mut validated: u64 = 0;
    let mut passes: u32 = 0;
    loop {
        let progress = store.validate_withheld_backlog(512)?;
        validated += u64::from(progress.rows_validated);
        passes += 1;
        if progress.complete {
            break;
        }
        if passes > 1_000 {
            return Err(Box::new(SoakError(
                "the resumable withheld audit did not terminate".to_owned(),
            )));
        }
    }
    // Open validated its own page; the resumable walk covered the remainder
    // exactly once, with no row read twice and none skipped.
    assert_eq!(
        validated,
        WITHHELD_AUDIT_ROWS - u64::from(OPEN_WITHHELD_AUDIT_ROWS),
        "the resumable audit revalidated {validated} rows after the open-time page",
    );
    // Finished means finished: a further call costs nothing and reads nothing.
    let after = store.validate_withheld_backlog(512)?;
    assert_eq!(after.rows_validated, 0);
    assert!(after.complete);
    println!(
        "withheld audit open cost: empty={empty_open}us loaded={full_open}us rows={WITHHELD_AUDIT_ROWS} \
         resumable_passes={passes} resumable_rows={validated}"
    );
    Ok(())
}

fn time_open(path: &std::path::Path) -> Result<i64, Box<dyn Error>> {
    let started = Instant::now();
    let store = SqliteObservationJournal::open(path, soak_retention_policy())?;
    let elapsed = i64::try_from(started.elapsed().as_micros()).unwrap_or(i64::MAX);
    drop(store);
    Ok(elapsed)
}

#[test]
fn the_resumable_withheld_audit_still_fails_closed_on_a_corrupt_receipt() -> TestResult {
    let directory = tempfile::TempDir::new()?;
    let path = directory.path().join("corrupt.sqlite3");
    // Past the open-time page, so the corruption is one the *resumable* pass
    // has to find: the whole point of bounding the open is that the rest of
    // the table is still audited rather than blessed.
    let rows: u64 = 400;
    {
        let store = SqliteObservationJournal::open(&path, soak_retention_policy())?;
        for sequence in 1..=rows {
            store.record_withheld(&withheld_at(sequence, "forget:corrupt")?)?;
        }
    }
    let connection = rusqlite::Connection::open(&path)?;
    // A receipt identity that no longer derives from the evidence beside it:
    // exactly the drift the audit exists to catch.
    let changed = connection.execute(
        "UPDATE tdmem_observation_withheld_v2 SET findings_digest = ?1 \
         WHERE source_sequence = ?2",
        rusqlite::params![
            "0000000000000000000000000000000000000000000000000000000000000000",
            i64::try_from(rows)?,
        ],
    )?;
    assert_eq!(changed, 1, "the fixture row to corrupt");
    drop(connection);

    // The open itself succeeds: its bounded page does not reach the row.
    let store = SqliteObservationJournal::open(&path, soak_retention_policy())?;
    let mut passes = 0_u32;
    let failure = loop {
        passes += 1;
        match store.validate_withheld_backlog(64) {
            Ok(progress) if progress.complete => {
                return Err(Box::new(SoakError(
                    "the resumable audit completed over a corrupt withheld receipt".to_owned(),
                )));
            }
            Ok(_) if passes > 100 => {
                return Err(Box::new(SoakError(
                    "the resumable audit neither completed nor refused".to_owned(),
                )));
            }
            Ok(_) => continue,
            Err(error) => break error,
        }
    };
    assert!(
        matches!(
            failure,
            ObservationJournalError::Corrupt {
                table: "tdmem_observation_withheld_v2",
                ..
            }
        ),
        "the audit refused with {failure} rather than reporting store corruption",
    );
    // The cursor did not advance past the defect: the next pass meets it again
    // rather than walking over it once and reporting a clean table.
    let again = store.validate_withheld_backlog(64);
    assert!(
        again.is_err(),
        "the audit stepped over the corrupt row and reported progress",
    );
    Ok(())
}
