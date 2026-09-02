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
//! # Why there are exactly two decisions
//!
//! The journal has two watermark-advancing primitives, so the adapter has two
//! answers. There is no "ignore this record" decision, because advancing a
//! watermark past a record nobody decided about is a silent drop with extra
//! steps. A caller whose source carries records that are not observable filters
//! them before they reach ingress, and the watermark simply never covers them.

use crate::envelope::{AdmittedObservationV1, WithheldAdmissionV1};
use crate::identity::SourceSequenceV1;
use crate::inspection::ReplayDispositionV1;
use crate::port::{AppendOutcomeV1, ObservationDispatchPortV1};
use crate::settlement::SourceStreamKeyV1;

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

    /// Decides one record. Returning an error leaves the record undecided and
    /// its replay position untouched.
    fn decide(
        &self,
        record: &SourceRecordV1<Self::Record>,
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
    /// Whether delivery was woken, which happens only when something new was
    /// appended.
    pub delivery_signalled: bool,
}

/// Drives canonical records through admission into the journal.
#[derive(Debug)]
pub struct IngressRuntimeV1<'a, P: ?Sized, A> {
    port: &'a P,
    adapter: &'a A,
    wake: &'a DeliveryWakeV1,
}

impl<'a, P, A> IngressRuntimeV1<'a, P, A>
where
    P: ObservationDispatchPortV1 + ?Sized,
    A: ObservationAdmissionAdapterV1,
{
    /// Binds one admission port, one adapter, and the delivery wake edge.
    #[must_use]
    pub const fn new(port: &'a P, adapter: &'a A, wake: &'a DeliveryWakeV1) -> Self {
        Self {
            port,
            adapter,
            wake,
        }
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
    /// No clock is taken. Admission timestamps belong to the settled envelope,
    /// and the journal stamps its own cursor instants; a runtime that minted its
    /// own would be fabricating one.
    pub fn ingest(
        &self,
        resume: &IngressResumeV1,
        records: &[SourceRecordV1<A::Record>],
    ) -> Result<IngressBatchReportV1, ObservationRuntimeError> {
        require_authoritative_order(resume, records)?;
        let mut report = IngressBatchReportV1::default();

        for record in records {
            let sequence = record.source_sequence.0;
            report.records_considered = report.records_considered.saturating_add(1);

            if !resume.accepts(record.source_sequence) {
                report.already_processed = report.already_processed.saturating_add(1);
                continue;
            }

            let decision = self.adapter.decide(record).map_err(|cause| {
                ObservationRuntimeError::Admission {
                    source_event_id: record.source_event_id.clone(),
                    source_sequence: sequence,
                    cause: AdapterFailureV1::new(cause),
                }
            })?;

            match decision {
                AdmissionDecisionV1::Admit(admitted) => {
                    verify_admitted(record, &admitted)?;
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

        if report.appended > 0 {
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
