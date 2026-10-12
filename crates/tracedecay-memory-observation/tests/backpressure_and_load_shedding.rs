//! Backpressure, load shedding, and the no-silent-drop invariant.
//!
//! Every test here drives the *runtime* against a real journal, because the
//! properties that matter are properties of the pair: a shed must be a refusal
//! the source can retry rather than a record that quietly disappeared, and the
//! measurements the gate decides on must be the ones the store actually holds.
//!
//! The four things proven, in the order the bead states them:
//!
//! 1. saturation is reproducible — fill the lane, watch every class stop;
//! 2. nothing disappears — a shed leaves the watermark, the source record, and
//!    the eventual admission of that exact record intact;
//! 3. the foreground budget is an input — a slow admission sheds optional work
//!    even when the queue looks empty, and recovers when a sample comes back;
//! 4. the metrics carry backlog age and size, measured, not estimated.

mod support;

use std::cell::Cell;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::time::Duration;

use support::{
    Builder, DAY, HOUR, MINUTE, SECOND, T0, TestIngestControl, TestResult, applied_receipt,
    backpressure_policy, gate, gate_with, ingest_control, lane, lease_request, policy, stream_key,
    target,
};

use tracedecay_memory_observation::{
    AdmissionDecisionV1, BackpressureDecisionV1, BackpressureGateV1, BackpressurePolicyV1,
    BackpressureReasonV1, BackpressureStateV1, DeliveryWakeV1, IngressControlV1, IngressRuntimeV1,
    IngressStopReasonV1, JournalInspectionFilterV1, ObservationAdmissionAdapterV1,
    ObservationDispatchPortV1, ObservationJournalError, ObservationJournalReaderV1,
    ObservationLaneKeyV1, ObservationLoadClassV1, ObservationRuntimeError, QueuePressureV1,
    RetentionClassV1, RetentionPolicyV1, SourceRecordV1, SourceSequenceV1,
    SqliteObservationJournal, WakeOutcomeV1,
};

// --------------------------------------------------------------- fixtures --

/// A caller-owned adapter failure, so the runtime has something typed to carry.
#[derive(Debug)]
struct AdapterError(String);

impl Display for AdapterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for AdapterError {}

/// Admission that mints one envelope per record in a caller-chosen retention
/// class, which is what the load class is derived from.
struct ClassAdmission {
    lane: ObservationLaneKeyV1,
    retention_class: RetentionClassV1,
    admitted_at: i64,
    /// How many times admission was actually paid for. A gate that refuses
    /// before hygiene, digests, and a readiness proof leaves this at zero.
    decisions: Cell<u32>,
}

impl ClassAdmission {
    fn new(
        lane: ObservationLaneKeyV1,
        retention_class: RetentionClassV1,
        admitted_at: i64,
    ) -> Self {
        Self {
            lane,
            retention_class,
            admitted_at,
            decisions: Cell::new(0),
        }
    }

    fn decisions(&self) -> u32 {
        self.decisions.get()
    }
}

impl ObservationAdmissionAdapterV1 for ClassAdmission {
    type Record = ();
    type Error = AdapterError;
    type Control = dyn IngressControlV1;

    fn lane(&self, _record: &SourceRecordV1<Self::Record>) -> ObservationLaneKeyV1 {
        self.lane.clone()
    }

    fn classify(&self, _record: &SourceRecordV1<Self::Record>) -> ObservationLoadClassV1 {
        ObservationLoadClassV1::of(self.retention_class)
    }

    fn decide(
        &self,
        record: &SourceRecordV1<Self::Record>,
        _control: &Self::Control,
    ) -> Result<AdmissionDecisionV1, Self::Error> {
        self.decisions.set(self.decisions.get().saturating_add(1));
        let admitted = Builder {
            retention_class: self.retention_class,
            admitted_at: self.admitted_at,
            expires_at: self.admitted_at.saturating_add(30 * DAY),
            deadline: self.admitted_at.saturating_add(HOUR),
            ..Builder::at_sequence(record.source_sequence.0)
        }
        .build()
        .map_err(|error| AdapterError(error.to_string()))?;
        Ok(AdmissionDecisionV1::Admit(Box::new(admitted)))
    }
}

fn record_at(sequence: u64) -> Result<SourceRecordV1<()>, Box<dyn Error>> {
    Ok(SourceRecordV1 {
        stream: stream_key("session-1")?,
        source_sequence: SourceSequenceV1(sequence),
        source_event_id: format!("event-{sequence}"),
        source_event_revision: 0,
        record: (),
    })
}

/// A journal whose item ceiling is small enough that a handful of rows moves
/// utilization across a declared threshold.
fn bounded_journal(path: &std::path::Path) -> Result<SqliteObservationJournal, Box<dyn Error>> {
    Ok(SqliteObservationJournal::open(
        path,
        RetentionPolicyV1 {
            max_queue_items: 10,
            ..policy()
        },
    )?)
}

/// Thresholds with a real reserved band: optional work stops at half full,
/// everything stops at eight tenths.
fn banded_policy() -> BackpressurePolicyV1 {
    BackpressurePolicyV1 {
        shed_optional_at_ppm: 500_000,
        refuse_at_ppm: 800_000,
        max_backlog_age_micros: DAY,
        foreground_budget_micros: 50 * MINUTE,
        foreground_breach_streak: 3,
    }
}

/// Appends `count` project-lifetime rows straight through the port, so a test
/// can create pressure without going through the gate it is testing.
fn fill(store: &SqliteObservationJournal, count: u64, admitted_at: i64) -> TestResult {
    for sequence in 1..=count {
        let admitted = Builder {
            retention_class: RetentionClassV1::Project,
            admitted_at,
            expires_at: admitted_at.saturating_add(30 * DAY),
            deadline: admitted_at.saturating_add(HOUR),
            ..Builder::at_sequence(sequence)
        }
        .build()?;
        store.append_admitted(&admitted)?;
    }
    Ok(())
}

fn rows_at(
    store: &SqliteObservationJournal,
    sequence: u64,
) -> Result<usize, Box<dyn std::error::Error>> {
    let page = store.inspect(&JournalInspectionFilterV1 {
        limit: 100,
        ..JournalInspectionFilterV1::default()
    })?;
    Ok(page
        .rows
        .iter()
        .filter(|row| row.source_sequence == SourceSequenceV1(sequence))
        .count())
}

// ------------------------------------------------------------------ tests --

/// Saturation is reproducible from the store's own numbers, and at saturation
/// *every* class stops — including the class that keeps the reserved band.
///
/// A gate that only ever shed optional work would leave a required lane free to
/// grow to the ceiling and turn a bounded journal into an unbounded one.
#[test]
fn queue_saturation_refuses_every_class_and_leaves_the_watermark_where_it_was() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = bounded_journal(&directory.path().join("journal.sqlite3"))?;
    fill(&store, 8, T0)?;

    let wake = DeliveryWakeV1::new();
    let admission = ClassAdmission::new(lane()?, RetentionClassV1::Project, T0);
    let gate = gate_with(banded_policy())?;
    let control = ingest_control();
    let ingress = IngressRuntimeV1::new(&store, &admission, &wake, &gate, &control);
    let stream = stream_key("session-1")?;

    let resume = ingress.recover(&stream)?;
    let report = ingress.ingest(&resume, &[record_at(9)?])?;

    assert_eq!(report.appended, 0);
    assert_eq!(report.shed, 1);
    let shed = report.shed_on.ok_or("expected a saturation shed")?;
    assert_eq!(shed.source_sequence, SourceSequenceV1(9));
    assert_eq!(shed.refusal.load_class, ObservationLoadClassV1::Required);
    assert_eq!(shed.refusal.state, BackpressureStateV1::Saturated);
    assert_eq!(shed.refusal.reason, BackpressureReasonV1::QueueUtilization);

    // 8 of 10 rows is exactly the declared refusal point, and the store agrees.
    let backlog = report
        .backlog
        .ok_or("a gated ingest must report a backlog")?;
    assert_eq!(backlog.queue_items, 8);
    assert_eq!(backlog.items_utilization_ppm, 800_000);

    // Nothing was written and the watermark never moved past the row that was
    // already there, so the refused record is still the source's to re-present.
    let cursor = store
        .replay_cursor(&stream)?
        .ok_or("the prefill must have left a cursor")?;
    assert_eq!(cursor.last_admitted_sequence, SourceSequenceV1(8));
    assert_eq!(rows_at(&store, 9)?, 0);
    Ok(())
}

/// In the reserved band, optional work is refused and required work is not.
///
/// This is the whole point of the load class. A gate that ignored it would
/// either refuse both — throwing away the headroom the band exists to reserve —
/// or admit both, which is no backpressure at all.
#[test]
fn the_reserved_band_sheds_optional_work_and_still_admits_required_work() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = bounded_journal(&directory.path().join("journal.sqlite3"))?;
    fill(&store, 5, T0)?;

    let wake = DeliveryWakeV1::new();
    let gate = gate_with(banded_policy())?;
    let stream = stream_key("session-1")?;

    let optional = ClassAdmission::new(lane()?, RetentionClassV1::Session, T0);
    let control = ingest_control();
    let optional_ingress = IngressRuntimeV1::new(&store, &optional, &wake, &gate, &control);
    let resume = optional_ingress.recover(&stream)?;
    let shed_report = optional_ingress.ingest(&resume, &[record_at(6)?])?;
    assert_eq!(shed_report.appended, 0);
    let shed = shed_report.shed_on.ok_or("expected an optional shed")?;
    assert_eq!(shed.refusal.load_class, ObservationLoadClassV1::Optional);
    assert_eq!(shed.refusal.state, BackpressureStateV1::SheddingOptional);
    assert_eq!(shed.refusal.reason, BackpressureReasonV1::QueueUtilization);

    // Same lane, same pressure, same source position — only the class differs.
    let required = ClassAdmission::new(lane()?, RetentionClassV1::Project, T0);
    let control = ingest_control();
    let required_ingress = IngressRuntimeV1::new(&store, &required, &wake, &gate, &control);
    let resume = required_ingress.recover(&stream)?;
    let admitted_report = required_ingress.ingest(&resume, &[record_at(6)?])?;
    assert_eq!(admitted_report.shed, 0);
    assert_eq!(admitted_report.appended, 1);
    assert_eq!(rows_at(&store, 6)?, 1);
    Ok(())
}

/// A shed record is re-presented and admitted once the lane drains, exactly
/// once, under the key it would always have had.
///
/// This is the no-silent-drop proof. A shed that advanced the watermark, or one
/// that let the record back in as a *second* row, would both fail here.
#[test]
fn a_shed_record_is_admitted_once_the_lane_drains_and_never_twice() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = bounded_journal(&directory.path().join("journal.sqlite3"))?;
    fill(&store, 5, T0)?;

    let wake = DeliveryWakeV1::new();
    let gate = gate_with(banded_policy())?;
    let admission = ClassAdmission::new(lane()?, RetentionClassV1::Session, T0);
    let control = ingest_control();
    let ingress = IngressRuntimeV1::new(&store, &admission, &wake, &gate, &control);
    let stream = stream_key("session-1")?;

    let resume = ingress.recover(&stream)?;
    let shed_report = ingress.ingest(&resume, &[record_at(6)?])?;
    assert_eq!(shed_report.shed, 1);
    // A shed is a lane that needs draining, so it wakes delivery rather than
    // waiting for a poll that has nothing new to notice.
    assert!(shed_report.delivery_signalled);
    assert_eq!(wake.wait(Duration::from_millis(0)), WakeOutcomeV1::Work);

    // Drain three rows to terminal, which is what pressure is measured on.
    let leased = store.lease_pending(&lease_request(T0 + SECOND, 3))?;
    assert_eq!(leased.len(), 3);
    for lease in &leased {
        store.record_attempt(&applied_receipt(lease, T0 + SECOND))?;
    }
    assert_eq!(store.queue_pressure(&target()?)?.queue_items, 2);

    // The same record, re-presented from the same source position.
    let resume = ingress.recover(&stream)?;
    let admitted_report = ingress.ingest(&resume, &[record_at(6)?])?;
    assert_eq!(admitted_report.shed, 0);
    assert_eq!(admitted_report.appended, 1);
    assert_eq!(rows_at(&store, 6)?, 1);

    // And a source that replays its tail again does not create a second row.
    let resume = ingress.recover(&stream)?;
    let replay = ingress.ingest(&resume, &[record_at(6)?])?;
    assert_eq!(replay.appended, 0);
    assert_eq!(replay.already_processed, 1);
    assert_eq!(rows_at(&store, 6)?, 1);
    Ok(())
}

/// A lane that is not draining sheds optional work on backlog *age*, however
/// few rows it holds.
///
/// A gate that watched only depth would keep feeding a stalled provider with a
/// nearly empty queue — the exact shape of a silent, growing stall.
#[test]
fn an_aged_backlog_sheds_optional_work_while_the_queue_is_nearly_empty() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = bounded_journal(&directory.path().join("journal.sqlite3"))?;
    fill(&store, 1, T0)?;

    let aging = BackpressurePolicyV1 {
        max_backlog_age_micros: HOUR,
        ..banded_policy()
    };
    let wake = DeliveryWakeV1::new();
    let gate = gate_with(aging)?;
    let stream = stream_key("session-1")?;
    let two_hours_later = T0 + 2 * HOUR;

    let optional = ClassAdmission::new(lane()?, RetentionClassV1::Session, two_hours_later);
    // The lane is measured on the caller's own instant, two hours after the
    // row that is still sitting in it.
    let control = TestIngestControl::at(two_hours_later, DAY);
    let optional_ingress = IngressRuntimeV1::new(&store, &optional, &wake, &gate, &control);
    let resume = optional_ingress.recover(&stream)?;
    let report = optional_ingress.ingest(&resume, &[record_at(2)?])?;

    let shed = report.shed_on.ok_or("expected an age-driven shed")?;
    assert_eq!(shed.refusal.reason, BackpressureReasonV1::BacklogAge);
    assert_eq!(shed.refusal.state, BackpressureStateV1::SheddingOptional);
    let backlog = report
        .backlog
        .ok_or("a gated ingest must report a backlog")?;
    assert_eq!(backlog.queue_items, 1);
    assert_eq!(backlog.items_utilization_ppm, 100_000);
    assert_eq!(backlog.oldest_backlog_age_micros, 2 * HOUR);

    // Required work still gets through: age reserves the band, it does not
    // close the lane.
    let required = ClassAdmission::new(lane()?, RetentionClassV1::Project, two_hours_later);
    let required_ingress = IngressRuntimeV1::new(&store, &required, &wake, &gate, &control);
    let resume = required_ingress.recover(&stream)?;
    assert_eq!(
        required_ingress.ingest(&resume, &[record_at(2)?])?.appended,
        1
    );
    Ok(())
}

/// A foreground admission that overran its declared budget sheds optional work
/// even at zero queue pressure, and stops as soon as a sample recovers.
///
/// Queue depth cannot see a slow *journal*: nothing is getting in, so the lane
/// looks idle while the coding agent waits. Only a measured foreground sample
/// catches that, and only a sample that decays catches the recovery.
#[test]
fn a_foreground_admission_over_budget_sheds_optional_work_until_it_recovers() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = bounded_journal(&directory.path().join("journal.sqlite3"))?;

    let budgeted = BackpressurePolicyV1 {
        foreground_budget_micros: 250_000,
        foreground_breach_streak: 2,
        ..banded_policy()
    };
    let wake = DeliveryWakeV1::new();
    let gate = gate_with(budgeted)?;
    let stream = stream_key("session-1")?;

    assert_eq!(gate.foreground_sample(), None);
    assert_eq!(gate.foreground_breaches(), 0);

    // One overrun is noise. A lane that shed over noise would refuse work
    // every time the disk hiccupped, so the declared run length is two.
    assert!(!gate.observe_foreground(400_000).within_budget());
    assert_eq!(gate.foreground_sample(), Some(400_000));
    assert_eq!(gate.foreground_breaches(), 1);

    let optional = ClassAdmission::new(lane()?, RetentionClassV1::Session, T0);
    let control = ingest_control();
    let optional_ingress = IngressRuntimeV1::new(&store, &optional, &wake, &gate, &control);
    let resume = optional_ingress.recover(&stream)?;
    let tolerated = optional_ingress.ingest(&resume, &[record_at(1)?])?;
    assert_eq!(tolerated.shed, 0, "a single overrun must shed nothing");
    assert_eq!(tolerated.appended, 1);

    // A second consecutive overrun is a lane whose admission path is not
    // keeping up, and there shedding optional traffic is the actual remedy.
    assert!(!gate.observe_foreground(400_000).within_budget());
    assert_eq!(gate.foreground_breaches(), 2);
    let resume = optional_ingress.recover(&stream)?;
    let report = optional_ingress.ingest(&resume, &[record_at(2)?])?;
    let shed = report.shed_on.ok_or("expected a budget-driven shed")?;
    assert_eq!(shed.refusal.reason, BackpressureReasonV1::ForegroundBudget);
    let backlog = report
        .backlog
        .ok_or("a gated ingest must report a backlog")?;
    assert_eq!(
        backlog.utilization_ppm, 100_000,
        "the queue itself was never loaded"
    );
    assert_eq!(backlog.foreground_latency_micros, Some(400_000));
    assert_eq!(backlog.foreground_breaches, 2);

    // One admission back inside the budget clears the run, so the lane
    // recovers on its own instead of staying latched.
    assert!(gate.observe_foreground(10_000).within_budget());
    assert_eq!(gate.foreground_breaches(), 0);
    let resume = optional_ingress.recover(&stream)?;
    let recovered = optional_ingress.ingest(&resume, &[record_at(2)?])?;
    assert_eq!(recovered.shed, 0);
    assert_eq!(recovered.appended, 1);
    assert_eq!(
        recovered
            .backlog
            .ok_or("a gated ingest must report a backlog")?
            .state,
        BackpressureStateV1::Nominal
    );
    Ok(())
}

/// The metrics an operator reads carry backlog size *and* age, taken from the
/// store rather than from anything the runtime remembered.
#[test]
fn backlog_metrics_expose_measured_size_and_age() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = bounded_journal(&directory.path().join("journal.sqlite3"))?;
    let gate = gate_with(banded_policy())?;
    assert_eq!(gate.metrics(), None, "nothing has been measured yet");

    fill(&store, 4, T0)?;
    let pressure = store.queue_pressure(&target()?)?;
    let observed = gate.observe(&pressure, T0 + 30 * MINUTE);

    assert_eq!(observed.queue_items, 4);
    assert_eq!(observed.max_queue_items, 10);
    assert_eq!(observed.items_utilization_ppm, 400_000);
    assert_eq!(observed.oldest_backlog_age_micros, 30 * MINUTE);
    assert_eq!(observed.state, BackpressureStateV1::Nominal);
    assert!(observed.queue_bytes > 0);
    assert_eq!(
        gate.metrics(),
        Some(observed),
        "the gate must publish the measurement it decided on"
    );

    // An empty lane reports a zero age rather than an age against nothing.
    let empty_directory = tempfile::tempdir()?;
    let empty_store = bounded_journal(&empty_directory.path().join("journal.sqlite3"))?;
    let empty = gate.observe(&empty_store.queue_pressure(&target()?)?, T0 + 30 * MINUTE);
    assert!(empty.is_empty());
    assert_eq!(empty.oldest_backlog_age_micros, 0);
    assert_eq!(empty.items_utilization_ppm, 0);
    assert_eq!(empty.observed_at_unix_micros, T0 + 30 * MINUTE);
    assert_eq!(empty.state, BackpressureStateV1::Nominal);
    Ok(())
}

/// A policy that reserves no headroom, or that bounds nothing, is refused at
/// construction rather than at the first saturation.
#[test]
fn a_policy_without_a_reserved_band_is_refused() -> TestResult {
    let equal = BackpressurePolicyV1 {
        shed_optional_at_ppm: 800_000,
        refuse_at_ppm: 800_000,
        ..banded_policy()
    };
    assert!(matches!(
        BackpressureGateV1::new(equal),
        Err(ObservationJournalError::InvalidBackpressurePolicy {
            field: "refuse_at_ppm"
        })
    ));

    let unbounded_age = BackpressurePolicyV1 {
        max_backlog_age_micros: 0,
        ..banded_policy()
    };
    assert!(matches!(
        BackpressureGateV1::new(unbounded_age),
        Err(ObservationJournalError::InvalidBackpressurePolicy {
            field: "max_backlog_age_micros"
        })
    ));

    let unbounded_foreground = BackpressurePolicyV1 {
        foreground_budget_micros: 0,
        ..banded_policy()
    };
    assert!(matches!(
        BackpressureGateV1::new(unbounded_foreground),
        Err(ObservationJournalError::InvalidBackpressurePolicy {
            field: "foreground_budget_micros"
        })
    ));

    // A zero run length would shed on the very first admission, before any
    // sample exists at all — a lane that starts out refusing work.
    let no_streak = BackpressurePolicyV1 {
        foreground_breach_streak: 0,
        ..banded_policy()
    };
    assert!(matches!(
        BackpressureGateV1::new(no_streak),
        Err(ObservationJournalError::InvalidBackpressurePolicy {
            field: "foreground_breach_streak"
        })
    ));

    // The fixture policy every other suite runs under is itself admissible.
    backpressure_policy().validate()?;
    gate()?;
    Ok(())
}

/// The load class is derived from the retention lifetime, not declared, so no
/// caller can mark its own high-volume stream unsheddable.
#[test]
fn the_load_class_follows_the_retention_lifetime() {
    assert_eq!(
        ObservationLoadClassV1::of(RetentionClassV1::Ephemeral),
        ObservationLoadClassV1::Optional
    );
    assert_eq!(
        ObservationLoadClassV1::of(RetentionClassV1::Session),
        ObservationLoadClassV1::Optional
    );
    assert_eq!(
        ObservationLoadClassV1::of(RetentionClassV1::Project),
        ObservationLoadClassV1::Required
    );
    assert_eq!(
        ObservationLoadClassV1::of(RetentionClassV1::Profile),
        ObservationLoadClassV1::Required
    );
}

// ------------------------------------- projection, bounds, and freshness --

/// Thresholds tight enough that one record's own bytes decide the answer:
/// optional work stops at three quarters, everything stops at 95 %.
fn byte_policy() -> BackpressurePolicyV1 {
    BackpressurePolicyV1 {
        shed_optional_at_ppm: 750_000,
        refuse_at_ppm: 950_000,
        ..banded_policy()
    }
}

/// A lane at 70 % of its byte ceiling with a 20-byte optional record in hand
/// is a lane that would be at 90 % — past the shed threshold — the instant the
/// append committed. It is refused *now*, on what the append would make it.
///
/// A gate that read the thresholds off the pre-append measurement would call
/// this nominal and admit it, and with payloads allowed into the megabytes one
/// such admission can swallow the whole reserved band in a single step.
#[test]
fn one_heavy_optional_record_that_would_cross_the_shed_threshold_is_refused() -> TestResult {
    let gate = gate_with(byte_policy())?;
    let pressure = QueuePressureV1 {
        queue_items: 1,
        queue_bytes: 70,
        oldest_admitted_at_unix_micros: Some(T0),
        max_queue_items: 1_000,
        max_queue_bytes: 100,
    };
    let backlog = gate.observe(&pressure, T0);
    assert_eq!(backlog.utilization_ppm, 700_000);
    assert_eq!(
        backlog.state,
        BackpressureStateV1::Nominal,
        "the lane as measured is below every threshold"
    );

    let BackpressureDecisionV1::Shed(refusal) =
        gate.decide(&backlog, ObservationLoadClassV1::Optional, 20)
    else {
        return Err(
            "an optional record that would cross the shed threshold must be refused".into(),
        );
    };
    assert_eq!(refusal.reason, BackpressureReasonV1::QueueUtilization);
    assert_eq!(refusal.state, BackpressureStateV1::SheddingOptional);
    assert_eq!(refusal.projected_utilization_ppm, 900_000);
    assert_eq!(refusal.additional_bytes, 20);
    assert_eq!(
        refusal.backlog.utilization_ppm, 700_000,
        "the measurement stays the honest pre-append reading"
    );

    // 90 % is inside the band the shed threshold reserves, so the identical
    // record in the required class is still admitted. The projection tightens
    // the gate; it does not collapse the two classes together.
    assert_eq!(
        gate.decide(&backlog, ObservationLoadClassV1::Required, 20),
        BackpressureDecisionV1::Admit
    );
    Ok(())
}

/// Required work is refused too, once one record would take the lane past the
/// refusal threshold.
///
/// This is the case a pre-append check cannot see at all: measured at 90 % the
/// lane is merely shedding optional work, so required work is admitted — and
/// six more bytes put it at 96 %, past the point where nothing may be admitted.
#[test]
fn one_required_record_that_would_cross_the_refusal_threshold_is_refused() -> TestResult {
    let gate = gate_with(byte_policy())?;
    let pressure = QueuePressureV1 {
        queue_items: 1,
        queue_bytes: 90,
        oldest_admitted_at_unix_micros: Some(T0),
        max_queue_items: 1_000,
        max_queue_bytes: 100,
    };
    let backlog = gate.observe(&pressure, T0);
    assert_eq!(
        backlog.state,
        BackpressureStateV1::SheddingOptional,
        "measured, the lane only sheds optional work"
    );

    let BackpressureDecisionV1::Shed(refusal) =
        gate.decide(&backlog, ObservationLoadClassV1::Required, 6)
    else {
        return Err(
            "a required record that would cross the refusal threshold must be refused".into(),
        );
    };
    assert_eq!(refusal.load_class, ObservationLoadClassV1::Required);
    assert_eq!(refusal.state, BackpressureStateV1::Saturated);
    assert_eq!(refusal.reason, BackpressureReasonV1::QueueUtilization);
    assert_eq!(refusal.projected_utilization_ppm, 960_000);

    // A smaller record that keeps the lane inside the band is still admitted,
    // so the refusal is about this record's weight and not about the lane.
    assert_eq!(
        gate.decide(&backlog, ObservationLoadClassV1::Required, 1),
        BackpressureDecisionV1::Admit
    );
    Ok(())
}

/// The published backlog describes the journal *after* the append that just
/// happened, including the append that crossed a threshold.
///
/// A metric refreshed only before an append reports the lane one row short of
/// the truth, so the exact append that starts the shedding is the one an
/// operator cannot see. An idle pass afterwards observes nothing, so a stale
/// reading would stay stale for as long as the lane stayed quiet.
#[test]
fn the_published_backlog_describes_the_journal_after_the_final_append() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = bounded_journal(&directory.path().join("journal.sqlite3"))?;
    fill(&store, 4, T0)?;

    let wake = DeliveryWakeV1::new();
    let gate = gate_with(banded_policy())?;
    let control = TestIngestControl::at(T0 + MINUTE, DAY);
    let admission = ClassAdmission::new(lane()?, RetentionClassV1::Project, T0 + MINUTE);
    let ingress = IngressRuntimeV1::new(&store, &admission, &wake, &gate, &control);
    let stream = stream_key("session-1")?;

    let resume = ingress.recover(&stream)?;
    let report = ingress.ingest(&resume, &[record_at(5)?])?;
    assert_eq!(report.appended, 1);

    let backlog = report
        .backlog
        .ok_or("a gated ingest must report a backlog")?;
    let pressure = store.queue_pressure(&target()?)?;
    assert_eq!(pressure.queue_items, 5);
    assert_eq!(
        backlog.queue_items, 5,
        "the published size must be the size the journal holds, not the size it held"
    );
    assert_eq!(backlog.queue_bytes, pressure.queue_bytes);
    assert_eq!(backlog.items_utilization_ppm, 500_000);
    assert_eq!(
        backlog.state,
        BackpressureStateV1::SheddingOptional,
        "the append that crossed the threshold must be the one that reports it"
    );
    assert_eq!(backlog.oldest_backlog_age_micros, MINUTE);
    assert_eq!(gate.metrics(), Some(backlog));

    // An idle pass commits nothing and therefore measures nothing, so the
    // published reading must already have been the current one.
    let resume = ingress.recover(&stream)?;
    let idle = ingress.ingest(&resume, &[record_at(5)?])?;
    assert_eq!(idle.already_processed, 1);
    assert_eq!(idle.appended, 0);
    assert_eq!(gate.metrics(), Some(backlog));

    // After a drain the lane is a different lane, and an explicit refresh is
    // what says so — delivery moves rows without ever passing through ingress.
    let leased = store.lease_pending(&lease_request(T0 + 2 * MINUTE, 3))?;
    assert_eq!(leased.len(), 3);
    for lease in &leased {
        store.record_attempt(&applied_receipt(lease, T0 + 2 * MINUTE))?;
    }
    control.set_now(T0 + 2 * MINUTE);
    let refreshed = ingress.refresh_backlog(&lane()?)?;
    assert_eq!(refreshed.queue_items, 2);
    assert_eq!(refreshed.state, BackpressureStateV1::Nominal);
    assert_eq!(refreshed.oldest_backlog_age_micros, 2 * MINUTE);
    assert_eq!(gate.metrics(), Some(refreshed));
    Ok(())
}

/// A lane that refuses every class refuses the record before admission is paid
/// for at all.
///
/// Hygiene walks the whole envelope, digests are derived over it, and a
/// readiness proof may talk to a provider. Paying all of that to learn what one
/// indexed pressure read already knew is the foreground cost the gate exists to
/// avoid, and it is exactly the cost a saturated lane can least afford.
#[test]
fn a_saturated_lane_refuses_before_admission_is_paid_for() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = bounded_journal(&directory.path().join("journal.sqlite3"))?;
    fill(&store, 8, T0)?;

    let wake = DeliveryWakeV1::new();
    let gate = gate_with(banded_policy())?;
    let control = ingest_control();
    let admission = ClassAdmission::new(lane()?, RetentionClassV1::Project, T0);
    let ingress = IngressRuntimeV1::new(&store, &admission, &wake, &gate, &control);
    let stream = stream_key("session-1")?;

    let resume = ingress.recover(&stream)?;
    let report = ingress.ingest(&resume, &[record_at(9)?])?;

    assert_eq!(report.shed, 1);
    assert_eq!(report.appended, 0);
    assert_eq!(
        admission.decisions(),
        0,
        "a saturated lane must not pay for hygiene, digests, or a readiness proof"
    );
    let shed = report.shed_on.ok_or("expected a saturation shed")?;
    assert_eq!(shed.refusal.state, BackpressureStateV1::Saturated);
    assert_eq!(rows_at(&store, 9)?, 0);
    Ok(())
}

/// A caller that has already cancelled is not charged for the record at all,
/// and the watermark does not move.
#[test]
fn a_cancelled_caller_stops_before_admission_and_holds_the_watermark() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = bounded_journal(&directory.path().join("journal.sqlite3"))?;

    let wake = DeliveryWakeV1::new();
    let gate = gate_with(banded_policy())?;
    let control = ingest_control();
    let admission = ClassAdmission::new(lane()?, RetentionClassV1::Project, T0);
    let ingress = IngressRuntimeV1::new(&store, &admission, &wake, &gate, &control);
    let stream = stream_key("session-1")?;
    let resume = ingress.recover(&stream)?;

    control.cancel();
    let report = ingress.ingest(&resume, &[record_at(1)?])?;

    let stop = report
        .stopped_on
        .ok_or("a cancelled caller must produce a typed stop")?;
    assert_eq!(stop.reason, IngressStopReasonV1::Cancelled);
    assert_eq!(stop.source_sequence, SourceSequenceV1(1));
    assert_eq!(report.appended, 0);
    assert_eq!(admission.decisions(), 0);
    assert!(store.replay_cursor(&stream)?.is_none());
    assert_eq!(rows_at(&store, 1)?, 0);
    Ok(())
}

/// A deadline that elapses is the same typed refusal under a different name.
#[test]
fn an_elapsed_deadline_stops_the_batch_with_its_own_typed_terminal() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = bounded_journal(&directory.path().join("journal.sqlite3"))?;

    let wake = DeliveryWakeV1::new();
    let gate = gate_with(banded_policy())?;
    let control = TestIngestControl::at(T0, HOUR);
    let admission = ClassAdmission::new(lane()?, RetentionClassV1::Project, T0);
    let ingress = IngressRuntimeV1::new(&store, &admission, &wake, &gate, &control);
    let stream = stream_key("session-1")?;
    let resume = ingress.recover(&stream)?;

    control.set_now(T0 + HOUR);
    let report = ingress.ingest(&resume, &[record_at(1)?])?;

    let stop = report
        .stopped_on
        .ok_or("an elapsed deadline must produce a typed stop")?;
    assert_eq!(stop.reason, IngressStopReasonV1::DeadlineExceeded);
    assert_eq!(report.appended, 0);
    assert_eq!(admission.decisions(), 0);
    assert_eq!(rows_at(&store, 1)?, 0);
    Ok(())
}

/// Cancellation that fires *while* one record is being admitted still stops
/// before the append, so the watermark holds and the record is re-presented.
///
/// This is the case a between-records check cannot catch: admission is the
/// expensive part, and a caller that gives up during it must not have its
/// answer committed anyway.
#[test]
fn cancellation_during_admission_stops_before_the_append() -> TestResult {
    /// Admission that cancels the caller after it has produced its decision,
    /// standing in for a caller that gives up during the expensive part.
    struct CancellingAdmission<'a> {
        inner: ClassAdmission,
        control: &'a TestIngestControl,
    }

    impl ObservationAdmissionAdapterV1 for CancellingAdmission<'_> {
        type Record = ();
        type Error = AdapterError;
        type Control = dyn IngressControlV1;

        fn lane(&self, record: &SourceRecordV1<Self::Record>) -> ObservationLaneKeyV1 {
            self.inner.lane(record)
        }

        fn classify(&self, record: &SourceRecordV1<Self::Record>) -> ObservationLoadClassV1 {
            self.inner.classify(record)
        }

        fn decide(
            &self,
            record: &SourceRecordV1<Self::Record>,
            control: &Self::Control,
        ) -> Result<AdmissionDecisionV1, Self::Error> {
            let decision = self.inner.decide(record, control)?;
            self.control.cancel();
            Ok(decision)
        }
    }

    let directory = tempfile::tempdir()?;
    let store = bounded_journal(&directory.path().join("journal.sqlite3"))?;

    let wake = DeliveryWakeV1::new();
    let gate = gate_with(banded_policy())?;
    let control = ingest_control();
    let admission = CancellingAdmission {
        inner: ClassAdmission::new(lane()?, RetentionClassV1::Project, T0),
        control: &control,
    };
    let ingress = IngressRuntimeV1::new(&store, &admission, &wake, &gate, &control);
    let stream = stream_key("session-1")?;
    let resume = ingress.recover(&stream)?;

    let report = ingress.ingest(&resume, &[record_at(1)?])?;

    let stop = report
        .stopped_on
        .ok_or("cancellation inside a record must produce a typed stop")?;
    assert_eq!(stop.reason, IngressStopReasonV1::Cancelled);
    assert_eq!(stop.source_sequence, SourceSequenceV1(1));
    assert_eq!(
        admission.inner.decisions(),
        1,
        "the record was admitted, and then the caller's cancellation refused the commit"
    );
    assert_eq!(report.appended, 0);
    assert!(store.replay_cursor(&stream)?.is_none());
    assert_eq!(rows_at(&store, 1)?, 0);
    Ok(())
}

/// An adapter whose pre-admission class disagrees with the envelope it then
/// produces is refused, so the cheap gate cannot be talked past.
///
/// Without this the early class would be a declaration, and a high-volume
/// stream could answer `Required` up front to keep feeding a lane that is
/// shedding exactly its kind of traffic.
#[test]
fn an_adapter_that_misdeclares_its_load_class_is_refused() -> TestResult {
    /// Answers `Required` up front and then admits session-lifetime content.
    struct LyingAdmission {
        inner: ClassAdmission,
    }

    impl ObservationAdmissionAdapterV1 for LyingAdmission {
        type Record = ();
        type Error = AdapterError;
        type Control = dyn IngressControlV1;

        fn lane(&self, record: &SourceRecordV1<Self::Record>) -> ObservationLaneKeyV1 {
            self.inner.lane(record)
        }

        fn classify(&self, _record: &SourceRecordV1<Self::Record>) -> ObservationLoadClassV1 {
            ObservationLoadClassV1::Required
        }

        fn decide(
            &self,
            record: &SourceRecordV1<Self::Record>,
            control: &Self::Control,
        ) -> Result<AdmissionDecisionV1, Self::Error> {
            self.inner.decide(record, control)
        }
    }

    let directory = tempfile::tempdir()?;
    let store = bounded_journal(&directory.path().join("journal.sqlite3"))?;
    fill(&store, 5, T0)?;

    let wake = DeliveryWakeV1::new();
    let gate = gate_with(banded_policy())?;
    let control = ingest_control();
    let admission = LyingAdmission {
        inner: ClassAdmission::new(lane()?, RetentionClassV1::Session, T0),
    };
    let ingress = IngressRuntimeV1::new(&store, &admission, &wake, &gate, &control);
    let stream = stream_key("session-1")?;
    let resume = ingress.recover(&stream)?;

    let error = ingress
        .ingest(&resume, &[record_at(6)?])
        .err()
        .ok_or("a class the envelope contradicts must not be admitted")?;
    assert!(matches!(
        error,
        ObservationRuntimeError::LoadClassMismatch {
            declared: "required",
            derived: "optional",
            ..
        }
    ));
    assert_eq!(rows_at(&store, 6)?, 0);
    Ok(())
}
