//! Ingress: recover a stream's replay position, then walk canonical records
//! through admission in authoritative order.
//!
//! # What "atomic append-or-withhold with watermark advancement" means here
//!
//! Each decision is committed by exactly one journal call, and that call is one
//! transaction that moves the row *and* the watermark together:
//!
//! * an admission runs journal insert, delivery insert, ingress replay cursor,
//!   and per-target cursor as a single unit;
//! * a withholding runs the withheld audit insert and the ingress replay cursor
//!   as a single unit.
//!
//! So a crash can never leave a row without its watermark, or a watermark
//! without its row. What this runtime does **not** claim is batch atomicity: a
//! crash midway through a batch leaves the earlier records committed and the
//! rest untouched, which is precisely what [`IngressRuntimeV1::recover`] is for.
//! Re-presenting the whole batch afterwards is safe because the watermark skips
//! what landed and the content-derived idempotency key catches the rest.
//!
//! # Bounds and pressure reach inside a record
//!
//! Everything expensive about a record happens inside one adapter call:
//! hygiene walks the envelope, digests are derived over it, and a readiness
//! proof may talk to a provider. So the caller's deadline and cancellation are
//! checked *before* that call and again before the append, and the record's
//! own lane is measured before it too — a lane that is refusing every class
//! refuses this record whatever its content turns out to be, and that is the
//! one answer that needs no classification and none of the admission cost.
//! A caller-supplied [`IngressControlV1`] carries the deadline, the
//! cancellation, and the instant every measurement is stamped with; a bound
//! that stopped a batch produces a typed [`IngressStopV1`], never a silent
//! return.
//!
//! # Why there are exactly two decisions
//!
//! The journal has two watermark-advancing primitives, so the adapter has two
//! answers. There is no "ignore this record" decision, because advancing a
//! watermark past a record nobody decided about is a silent drop with extra
//! steps. A caller whose source carries records that are not observable filters
//! them before they reach ingress, and the watermark simply never covers them.

use crate::envelope::{AdmittedObservationV1, WithheldAdmissionV1};
use crate::identity::SourceSequenceV1;
use crate::inspection::{ObservationLaneKeyV1, ReplayDispositionV1};
use crate::port::{AppendOutcomeV1, ObservationDispatchPortV1};
use crate::settlement::SourceStreamKeyV1;

use super::backpressure::{
    BackpressureDecisionV1, BackpressureGateV1, BackpressureHaltV1, ObservationLoadClassV1,
    QueueBacklogV1,
};
use super::error::{AdapterFailureV1, ObservationRuntimeError};
use super::wake::DeliveryWakeV1;

/// One settled canonical record, positioned in its source stream.
///
/// The runtime reads only the position and identity fields; `record` is the
/// caller's own payload and stays opaque, which is what keeps this crate free
/// of any TraceDecay store or provider-registry type.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SourceRecordV1<T> {
    /// Stream the record belongs to.
    pub stream: SourceStreamKeyV1,
    /// Position of the record inside that stream.
    pub source_sequence: SourceSequenceV1,
    /// Settled source event identity.
    pub source_event_id: String,
    /// Settled source event revision.
    pub source_event_revision: u64,
    /// The caller's canonical record.
    pub record: T,
}

/// The caller's bound on one ingest call, and the caller's clock.
///
/// Ingress owns no clock and no cancellation of its own. A record's admission
/// is the slowest thing a coding agent waits behind — hygiene walks the whole
/// envelope, a readiness proof may talk to a provider, and the append is an
/// fsync'd transaction — so the caller's deadline and cancellation identity
/// have to reach *inside* a record rather than only between records. Otherwise
/// one record can outlive the pass that started it, and a project that is
/// closing waits for work nobody will read.
///
/// Every method is answered by the caller, which is also what supplies the
/// instant every backlog measurement is stamped with.
pub trait IngressControlV1: std::fmt::Debug + Send + Sync {
    /// The caller's current instant. Backlog age and the metrics stamp are
    /// measured against this, so the runtime still mints no clock.
    fn now_unix_micros(&self) -> i64;

    /// The absolute instant the caller's work is no longer wanted.
    fn deadline_unix_micros(&self) -> i64;

    /// Whether the caller's cancellation has fired.
    fn is_cancelled(&self) -> bool;

    /// Budget left before the deadline, floored at zero.
    fn remaining_micros(&self) -> i64 {
        self.deadline_unix_micros()
            .saturating_sub(self.now_unix_micros())
            .max(0)
    }

    /// Whether the deadline has already passed.
    fn is_expired(&self) -> bool {
        self.remaining_micros() == 0
    }
}

/// Why ingress stopped a batch on the caller's own bound.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum IngressStopReasonV1 {
    /// The caller's cancellation fired.
    Cancelled,
    /// The caller's deadline elapsed.
    DeadlineExceeded,
}

impl IngressStopReasonV1 {
    /// Returns the canonical wire value.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Cancelled => "cancelled",
            Self::DeadlineExceeded => "deadline_exceeded",
        }
    }
}

/// The typed terminal a caller's own bound produced, positioned in the stream.
///
/// Like every other stop in this module it is a refusal, not a drop: nothing
/// was appended or withheld for this position, the watermark holds before it,
/// and the canonical source still owns the record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IngressStopV1 {
    /// Why the batch stopped.
    pub reason: IngressStopReasonV1,
    /// Position the batch stopped at.
    pub source_sequence: SourceSequenceV1,
    /// Settled event identity at that position.
    pub source_event_id: String,
}

/// What admission decided about one record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmissionDecisionV1 {
    /// Admit the record as this observation.
    Admit(Box<AdmittedObservationV1>),
    /// Withhold the record, recording digests and a typed reason only.
    Withhold(Box<WithheldAdmissionV1>),
}

/// The caller-supplied admission and hygiene seam.
///
/// Sanitization, canonicalization, digest derivation, provider targeting, and
/// the hygiene refusal decision all live behind this trait. The runtime supplies
/// order and durability; it never inspects content and never decides hygiene.
pub trait ObservationAdmissionAdapterV1 {
    /// The caller's canonical record type.
    type Record;
    /// The adapter's own failure type, preserved whole when it fails.
    type Error: std::error::Error + Send + Sync + 'static;
    /// The concrete shape of the caller's bound.
    ///
    /// Ingress itself only ever uses it through [`IngressControlV1`]. The
    /// adapter names the concrete type because a host's cancellation is an
    /// *identity*, not a boolean: an adapter that has to rebuild one from a
    /// flag would be minting a fresh token, which is exactly how a cancelled
    /// project ends up waiting on work it already gave up on. An adapter with
    /// no host type of its own uses `dyn IngressControlV1`.
    type Control: IngressControlV1 + ?Sized;

    /// The provider lane this record would be admitted into.
    ///
    /// Answered without hygiene, without digests, and without a readiness
    /// handshake, because it exists so ingress can read the lane's real
    /// pressure *before* it pays for any of those. A lane is a registration,
    /// which the adapter already knows; it is not the instance a handshake
    /// would prove.
    fn lane(&self, record: &SourceRecordV1<Self::Record>) -> ObservationLaneKeyV1;

    /// The load class this record's content puts it in, answered before
    /// admission is paid for.
    ///
    /// This is not a producer declaring its own priority. The adapter answers
    /// from the record's own settled content, exactly as it will when it fills
    /// in the envelope's retention class, and ingress *re-derives* the class
    /// from the admitted envelope afterwards and refuses the batch if the two
    /// disagree. So a stream cannot buy itself out of shedding by answering
    /// `Required` here: it would have to lie in the envelope too, and then the
    /// lie is the envelope's and is caught by every other envelope check.
    fn classify(&self, record: &SourceRecordV1<Self::Record>) -> ObservationLoadClassV1;

    /// Decides one record, under the caller's own deadline and cancellation.
    ///
    /// The control is the caller's, propagated verbatim: an adapter that
    /// proves readiness, talks to a provider, or does any other bounded work
    /// inside a record derives that work's bound from this, so a cancelled
    /// project or an elapsed pass reaches *inside* the record rather than
    /// waiting for it. Returning an error leaves the record undecided and its
    /// replay position untouched.
    fn decide(
        &self,
        record: &SourceRecordV1<Self::Record>,
        control: &Self::Control,
    ) -> Result<AdmissionDecisionV1, Self::Error>;
}

/// Where one stream's ingress must resume after recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IngressResumeV1 {
    /// Stream this position belongs to.
    pub stream: SourceStreamKeyV1,
    /// Highest position already admitted or withheld, when the stream has one.
    /// `None` means the stream has never been processed.
    pub resume_after: Option<SourceSequenceV1>,
    /// Whether that position was admitted or withheld.
    pub last_disposition: Option<ReplayDispositionV1>,
    /// Identity of the event at that position.
    pub last_source_event_id: Option<String>,
    /// Instant the cursor last moved.
    pub updated_at_unix_micros: Option<i64>,
}

impl IngressResumeV1 {
    /// Whether a record at `sequence` is still ahead of the recovered position.
    #[must_use]
    pub const fn accepts(&self, sequence: SourceSequenceV1) -> bool {
        match self.resume_after {
            Some(resume_after) => sequence.0 > resume_after.0,
            None => true,
        }
    }
}

/// The typed refusal that stopped a batch.
///
/// Ingress stops at the first non-duplicate refusal rather than stepping over
/// it. Admitting the next record would advance the per-target cursor past the
/// refused position and make it permanently un-admittable, turning a visible,
/// recoverable refusal into a silent hole.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IngressHaltV1 {
    /// Position the journal refused.
    pub source_sequence: SourceSequenceV1,
    /// Settled event identity at that position.
    pub source_event_id: String,
    /// The journal's typed refusal, carried verbatim.
    pub outcome: AppendOutcomeV1,
}

/// What one ingest call did.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IngressBatchReportV1 {
    /// Records inspected, including those already past the watermark.
    pub records_considered: u32,
    /// Records at or below the recovered position, skipped as already decided
    /// on an earlier run. Counted, never silently discarded.
    pub already_processed: u32,
    /// Records appended as new observations.
    pub appended: u32,
    /// Records recorded as withheld by hygiene.
    pub withheld: u32,
    /// Records the journal already held under the same key or the same settled
    /// event. Idempotent replay, not new work.
    pub duplicates: u32,
    /// Highest position this call confirmed as decided.
    pub high_watermark: Option<SourceSequenceV1>,
    /// The typed refusal that stopped the batch, when one did.
    pub halted_on: Option<IngressHaltV1>,
    /// Records the backpressure gate refused before any append was attempted.
    /// Refused, never discarded: the watermark holds at the first of them.
    pub shed: u32,
    /// The typed backpressure refusal that stopped the batch, when one did.
    pub shed_on: Option<BackpressureHaltV1>,
    /// The caller's own bound that stopped the batch, when one did. Nothing
    /// was committed for the named position.
    pub stopped_on: Option<IngressStopV1>,
    /// The lane's backlog as of the *end* of this call. This is the metrics
    /// surface — queue size, queue bytes, utilization, and backlog age — and
    /// it describes the journal after everything this call committed, not the
    /// journal as it was before the last append. A metric that stopped one
    /// append short of a threshold would report a nominal lane at the exact
    /// moment the lane started refusing.
    pub backlog: Option<QueueBacklogV1>,
    /// Whether delivery was woken. That happens when something new was
    /// appended, and also when the gate shed — a lane that refused work is by
    /// definition a lane that needs draining.
    pub delivery_signalled: bool,
}

/// Drives canonical records through admission into the journal.
#[derive(Debug)]
pub struct IngressRuntimeV1<'a, P: ?Sized, A: ObservationAdmissionAdapterV1> {
    port: &'a P,
    adapter: &'a A,
    wake: &'a DeliveryWakeV1,
    backpressure: &'a BackpressureGateV1,
    control: &'a A::Control,
}

impl<'a, P, A> IngressRuntimeV1<'a, P, A>
where
    P: ObservationDispatchPortV1 + ?Sized,
    A: ObservationAdmissionAdapterV1,
{
    /// Binds one admission port, one adapter, the delivery wake edge, the
    /// backpressure gate every admission is measured against, and the caller's
    /// own deadline, cancellation, and clock.
    ///
    /// Neither the gate nor the control is optional. An ingress constructible
    /// without a gate would be an ingress whose bounds are a convention, and
    /// the first slow provider would turn that convention into an unbounded
    /// journal; an ingress constructible without a control would be one whose
    /// caller can never get out of a record it no longer wants.
    #[must_use]
    pub const fn new(
        port: &'a P,
        adapter: &'a A,
        wake: &'a DeliveryWakeV1,
        backpressure: &'a BackpressureGateV1,
        control: &'a A::Control,
    ) -> Self {
        Self {
            port,
            adapter,
            wake,
            backpressure,
            control,
        }
    }

    /// Re-reads the lane and republishes its backlog on the caller's instant.
    ///
    /// This is the only way the published metric can describe the journal as
    /// it is *now*: the gate remembers the last measurement it took, and a
    /// measurement taken before an append describes a journal that no longer
    /// exists. Callers publish through this after a pass, after a delivery
    /// round, and at shutdown, so an idle lane's metric is still current
    /// rather than frozen at whatever the last admission happened to see.
    pub fn refresh_backlog(
        &self,
        lane: &ObservationLaneKeyV1,
    ) -> Result<QueueBacklogV1, ObservationRuntimeError> {
        let pressure = self.port.lane_pressure(lane)?;
        Ok(self
            .backpressure
            .observe(&pressure, self.control.now_unix_micros()))
    }

    /// The caller's own bound, as a typed stop positioned at `record`.
    fn caller_stop<T>(&self, record: &SourceRecordV1<T>) -> Option<IngressStopV1> {
        let reason = if self.control.is_cancelled() {
            IngressStopReasonV1::Cancelled
        } else if self.control.is_expired() {
            IngressStopReasonV1::DeadlineExceeded
        } else {
            return None;
        };
        Some(IngressStopV1 {
            reason,
            source_sequence: record.source_sequence,
            source_event_id: record.source_event_id.clone(),
        })
    }

    /// Reads one stream's durable replay position.
    ///
    /// This is the whole restart story: the journal's cursor, not any in-memory
    /// state, says where to resume, so a process that died mid-batch and one
    /// that never ran before take the same path.
    pub fn recover(
        &self,
        stream: &SourceStreamKeyV1,
    ) -> Result<IngressResumeV1, ObservationRuntimeError> {
        let cursor = self.port.replay_cursor(stream)?;
        Ok(match cursor {
            Some(cursor) => IngressResumeV1 {
                stream: stream.clone(),
                resume_after: Some(cursor.last_admitted_sequence),
                last_disposition: Some(cursor.last_disposition),
                last_source_event_id: Some(cursor.last_source_event_id),
                updated_at_unix_micros: Some(cursor.updated_at_unix_micros),
            },
            None => IngressResumeV1 {
                stream: stream.clone(),
                resume_after: None,
                last_disposition: None,
                last_source_event_id: None,
                updated_at_unix_micros: None,
            },
        })
    }

    /// Walks one batch of records through admission in authoritative order.
    ///
    /// The whole batch is checked for stream identity and strictly ascending
    /// order *before* any of it is committed, so a source that reorders is
    /// refused with nothing appended rather than halfway through. Both are
    /// checked rather than assumed: a batch that goes backwards would otherwise
    /// advance the watermark past events it never presented.
    ///
    /// No clock is minted. Admission timestamps belong to the settled
    /// envelope, the journal stamps its own cursor instants, and every backlog
    /// measurement is stamped with the instant the caller's own control
    /// reports; a runtime that read a clock of its own would be fabricating
    /// one.
    pub fn ingest(
        &self,
        resume: &IngressResumeV1,
        records: &[SourceRecordV1<A::Record>],
    ) -> Result<IngressBatchReportV1, ObservationRuntimeError> {
        require_authoritative_order(resume, records)?;
        let mut report = IngressBatchReportV1::default();
        let mut last_lane: Option<ObservationLaneKeyV1> = None;

        for record in records {
            let sequence = record.source_sequence.0;
            report.records_considered = report.records_considered.saturating_add(1);

            if !resume.accepts(record.source_sequence) {
                report.already_processed = report.already_processed.saturating_add(1);
                continue;
            }

            // The caller's bound is checked before the expensive part of the
            // record, not only between records: hygiene, digests, and a
            // readiness proof are all paid inside `decide`, and a caller that
            // has already given up must not be charged for them.
            if let Some(stop) = self.caller_stop(record) {
                report.stopped_on = Some(stop);
                break;
            }

            // Cheap pressure first. The lane is named from the registration
            // the adapter already knows, so this is one indexed read — and a
            // lane that is refusing *every* class refuses this record whatever
            // its content turns out to be, which is exactly the case where the
            // answer needs no classification and none of the admission cost.
            let lane = self.adapter.lane(record);
            let pressure = self.port.lane_pressure(&lane)?;
            let backlog = self
                .backpressure
                .observe(&pressure, self.control.now_unix_micros());
            report.backlog = Some(backlog);
            last_lane = Some(lane);
            let declared_class = self.adapter.classify(record);
            if let BackpressureDecisionV1::Shed(refusal) =
                self.backpressure.decide(&backlog, declared_class, 0)
            {
                report.shed = report.shed.saturating_add(1);
                report.shed_on = Some(BackpressureHaltV1 {
                    source_sequence: record.source_sequence,
                    source_event_id: record.source_event_id.clone(),
                    refusal,
                });
                break;
            }

            let decision = match self.adapter.decide(record, self.control) {
                Ok(decision) => decision,
                Err(cause) => {
                    // The caller's bound is spent *inside* `decide`: hygiene,
                    // digest derivation, and a readiness handshake all run
                    // there, each of them narrowed to the caller's own
                    // remaining budget. So the ordinary way a bounded caller
                    // learns it is out of time is that one of those
                    // sub-operations gives up and reports a refusal — and
                    // reporting that refusal as an adapter failure would turn
                    // "this pass ran out of time" into "this canonical record
                    // cannot be admitted", which is a claim about the record
                    // that nothing established. The bound is checked first,
                    // and when it has elapsed the batch stops on the caller's
                    // own typed reason. Nothing was appended on this path
                    // either way, so the watermark holds and the canonical
                    // source still owns the record for the next pass, which
                    // runs on a fresh budget and surfaces any real refusal
                    // then.
                    if let Some(stop) = self.caller_stop(record) {
                        report.stopped_on = Some(stop);
                        break;
                    }
                    return Err(ObservationRuntimeError::Admission {
                        source_event_id: record.source_event_id.clone(),
                        source_sequence: sequence,
                        cause: AdapterFailureV1::new(cause),
                    });
                }
            };

            match decision {
                AdmissionDecisionV1::Admit(admitted) => {
                    verify_admitted(record, &admitted)?;
                    // The class the pre-gate ran on has to be the class the
                    // envelope actually carries, or the cheap gate would be a
                    // hole a stream could declare its way through.
                    let class = ObservationLoadClassV1::of(admitted.privacy.retention_class);
                    if class != declared_class {
                        return Err(ObservationRuntimeError::LoadClassMismatch {
                            source_event_id: record.source_event_id.clone(),
                            source_sequence: sequence,
                            declared: declared_class.as_wire(),
                            derived: class.as_wire(),
                        });
                    }
                    // The caller may have cancelled or run out of budget while
                    // admission was being paid for. Stopping here leaves the
                    // watermark exactly where it was, so the record is still
                    // the canonical source's to re-present.
                    if let Some(stop) = self.caller_stop(record) {
                        report.stopped_on = Some(stop);
                        break;
                    }
                    // Now the size-aware decision, against the same real
                    // pressure read and on the caller's own instant. A shed
                    // stops the batch exactly like a journal refusal: nothing
                    // is appended, the watermark holds here, and the canonical
                    // source still holds the record for the next pass.
                    if let BackpressureDecisionV1::Shed(refusal) =
                        self.backpressure
                            .decide(&backlog, class, admitted.queue_bytes())
                    {
                        report.shed = report.shed.saturating_add(1);
                        report.shed_on = Some(BackpressureHaltV1 {
                            source_sequence: record.source_sequence,
                            source_event_id: record.source_event_id.clone(),
                            refusal,
                        });
                        break;
                    }
                    match self.port.append_admitted(&admitted)? {
                        AppendOutcomeV1::Appended { .. } => {
                            report.appended = report.appended.saturating_add(1);
                            report.high_watermark = Some(record.source_sequence);
                        }
                        AppendOutcomeV1::DuplicateIdempotencyKey { .. }
                        | AppendOutcomeV1::DuplicateSourceEvent { .. } => {
                            report.duplicates = report.duplicates.saturating_add(1);
                            report.high_watermark = Some(record.source_sequence);
                        }
                        refusal => {
                            report.halted_on = Some(IngressHaltV1 {
                                source_sequence: record.source_sequence,
                                source_event_id: record.source_event_id.clone(),
                                outcome: refusal,
                            });
                            break;
                        }
                    }
                }
                AdmissionDecisionV1::Withhold(withheld) => {
                    verify_withheld(record, &withheld)?;
                    self.port.record_withheld(&withheld)?;
                    report.withheld = report.withheld.saturating_add(1);
                    report.high_watermark = Some(record.source_sequence);
                }
            }
        }

        // Republish the lane *after* everything this call committed. The
        // measurement the last record decided on described the journal one
        // append ago, and a final append that crosses a threshold would
        // otherwise leave the lane reporting nominal until some later call
        // happened to measure it again.
        if report.appended > 0
            && let Some(lane) = last_lane
        {
            report.backlog = Some(self.refresh_backlog(&lane)?);
        }

        // A shed wakes delivery too. The lane refused work because it is not
        // draining fast enough, so the one action that can clear the refusal is
        // a delivery round — parking until the next poll would leave the lane
        // shedding for no reason a drain could not have fixed.
        if report.appended > 0 || report.shed > 0 {
            self.wake.signal();
            report.delivery_signalled = true;
        }
        Ok(report)
    }
}

/// Refuses a batch that is not one stream in strictly ascending order, before
/// any of it is committed.
fn require_authoritative_order<T>(
    resume: &IngressResumeV1,
    records: &[SourceRecordV1<T>],
) -> Result<(), ObservationRuntimeError> {
    let mut previous: Option<u64> = None;
    for record in records {
        if record.stream != resume.stream {
            return Err(ObservationRuntimeError::StreamMismatch {
                expected: resume.stream.source_stream.as_str().to_owned(),
                received: record.stream.source_stream.as_str().to_owned(),
            });
        }
        let sequence = record.source_sequence.0;
        if let Some(previous) = previous
            && sequence <= previous
        {
            return Err(ObservationRuntimeError::UnorderedIngressBatch {
                previous,
                received: sequence,
            });
        }
        previous = Some(sequence);
    }
    Ok(())
}

fn mismatch<T>(
    record: &SourceRecordV1<T>,
    field: &'static str,
    expected: &str,
    provided: &str,
) -> Result<(), ObservationRuntimeError> {
    if expected == provided {
        return Ok(());
    }
    Err(ObservationRuntimeError::AdmissionIdentityMismatch {
        source_event_id: record.source_event_id.clone(),
        source_sequence: record.source_sequence.0,
        field,
        expected: expected.to_owned(),
        provided: provided.to_owned(),
    })
}

/// Proves an admission decision describes the record it answers.
fn verify_admitted<T>(
    record: &SourceRecordV1<T>,
    admitted: &AdmittedObservationV1,
) -> Result<(), ObservationRuntimeError> {
    mismatch(
        record,
        "source_authority",
        record.stream.source_authority.as_wire(),
        admitted.source.source_authority.as_wire(),
    )?;
    mismatch(
        record,
        "exact_scope_sha256",
        &record.stream.exact_scope_sha256,
        &admitted.exact_scope_sha256(),
    )?;
    mismatch(
        record,
        "source_stream",
        record.stream.source_stream.as_str(),
        admitted.source.source_stream.as_str(),
    )?;
    mismatch(
        record,
        "source_sequence",
        &record.source_sequence.0.to_string(),
        &admitted.source.source_sequence.0.to_string(),
    )?;
    mismatch(
        record,
        "source_event_id",
        &record.source_event_id,
        &admitted.source.source_event_id,
    )?;
    mismatch(
        record,
        "source_event_revision",
        &record.source_event_revision.to_string(),
        &admitted.source.source_event_revision.to_string(),
    )
}

/// Proves a withholding decision describes the record it answers.
fn verify_withheld<T>(
    record: &SourceRecordV1<T>,
    withheld: &WithheldAdmissionV1,
) -> Result<(), ObservationRuntimeError> {
    mismatch(
        record,
        "source_authority",
        record.stream.source_authority.as_wire(),
        &withheld.source_authority,
    )?;
    mismatch(
        record,
        "exact_scope_sha256",
        &record.stream.exact_scope_sha256,
        &withheld.exact_scope_sha256,
    )?;
    mismatch(
        record,
        "source_stream",
        record.stream.source_stream.as_str(),
        &withheld.source_stream,
    )?;
    mismatch(
        record,
        "source_sequence",
        &record.source_sequence.0.to_string(),
        &withheld.source_sequence.to_string(),
    )?;
    mismatch(
        record,
        "source_event_id",
        &record.source_event_id,
        &withheld.source_event_id,
    )?;
    mismatch(
        record,
        "source_event_revision",
        &record.source_event_revision.to_string(),
        &withheld.source_event_revision,
    )
}
