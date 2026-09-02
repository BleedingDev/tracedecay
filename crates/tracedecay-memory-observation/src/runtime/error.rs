//! Typed failures for the ingress and delivery runtime seam.
//!
//! The runtime owns no new failure taxonomy for the journal itself: every store
//! failure is carried through as [`ObservationJournalError`]. What it adds is
//! the two things a seam can get wrong that the journal cannot see — a
//! caller-supplied adapter blowing up, and an adapter answering about something
//! other than the record or delivery it was handed.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use thiserror::Error as ThisError;

use crate::error::ObservationJournalError;

/// One caller-supplied adapter's own failure, preserved verbatim.
///
/// The runtime never interprets an adapter error, never maps it onto a provider
/// outcome, and never records it as a delivery receipt: an adapter that failed
/// before the provider answered proves nothing about what the provider did. The
/// error is boxed and kept whole so the caller's own type survives the trip
/// through [`Error::source`].
#[derive(Debug)]
pub struct AdapterFailureV1(Box<dyn Error + Send + Sync>);

impl AdapterFailureV1 {
    /// Captures one adapter failure.
    #[must_use]
    pub fn new(cause: impl Error + Send + Sync + 'static) -> Self {
        Self(Box::new(cause))
    }

    /// Borrows the captured failure.
    #[must_use]
    pub fn cause(&self) -> &(dyn Error + Send + Sync) {
        self.0.as_ref()
    }
}

impl Display for AdapterFailureV1 {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

impl Error for AdapterFailureV1 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}

/// A provider terminal that does not describe the delivery it answers.
///
/// A receipt minted from such a terminal would attribute a provider's answer
/// about one observation to another, so the runtime refuses to mint one and
/// reports the attempt as an adapter failure instead.
#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
#[error(
    "provider terminal {field} does not describe the leased delivery: \
     expected {expected}, provider returned {provided}"
)]
pub struct TerminalIdentityMismatchV1 {
    /// Logical field that disagreed.
    pub field: &'static str,
    /// Value the leased delivery carries.
    pub expected: String,
    /// Value the provider terminal carried.
    pub provided: String,
}

/// Every way the runtime seam can refuse or fail a batch.
#[derive(Debug, ThisError)]
#[non_exhaustive]
pub enum ObservationRuntimeError {
    /// The journal refused or failed the operation.
    #[error("observation journal failure: {0}")]
    Journal(#[from] ObservationJournalError),

    /// The dispatch request cannot bound an attempt, so no lease is taken.
    #[error("dispatch request field {field} is invalid")]
    InvalidDispatchRequest {
        /// Logical request field name.
        field: &'static str,
    },

    /// The admission adapter failed on one record. Nothing was appended or
    /// withheld for it, so its replay position is untouched and the caller may
    /// re-present it.
    #[error(
        "admission adapter failed for source event {source_event_id} \
         at sequence {source_sequence}: {cause}"
    )]
    Admission {
        /// Settled source event the adapter was handed.
        source_event_id: String,
        /// Position of that event in its stream.
        source_sequence: u64,
        /// The adapter's own failure.
        #[source]
        cause: AdapterFailureV1,
    },

    /// The admission decision described a different source event than the
    /// record it answers.
    ///
    /// This is the seam's central safety check. Appending or withholding
    /// advances a replay watermark, so a decision that names another position
    /// would move the cursor past an event nobody decided about — a silent
    /// drop wearing the shape of progress.
    #[error(
        "admission decision for source event {source_event_id} at sequence \
         {source_sequence} disagrees on {field}: record carries {expected}, \
         decision carries {provided}"
    )]
    AdmissionIdentityMismatch {
        /// Settled source event the adapter was handed.
        source_event_id: String,
        /// Position of that event in its stream.
        source_sequence: u64,
        /// Logical field that disagreed.
        field: &'static str,
        /// Value the record carries.
        expected: String,
        /// Value the decision carried.
        provided: String,
    },

    /// The batch was not presented in authoritative source order.
    ///
    /// Ingress advances one watermark per stream, so a batch that goes
    /// backwards or repeats a position cannot be processed without either
    /// dragging the watermark backwards or skipping a position.
    #[error(
        "ingress batch is not in authoritative order: sequence {received} \
         does not follow {previous}"
    )]
    UnorderedIngressBatch {
        /// Sequence of the preceding record in the batch.
        previous: u64,
        /// Sequence that broke the order.
        received: u64,
    },

    /// The class the adapter answered before admission is not the class the
    /// admitted envelope carries.
    ///
    /// Ingress refuses a lane's work early on the pre-admission answer, so
    /// that answer has to be the same one the envelope will produce. A
    /// disagreement means either the adapter is inconsistent or a stream is
    /// trying to buy itself out of shedding, and neither may be resolved by
    /// picking one of the two answers.
    #[error(
        "admission adapter classified source event {source_event_id} at sequence \
         {source_sequence} as {declared} before admission but the admitted envelope \
         is {derived}"
    )]
    LoadClassMismatch {
        /// Settled source event the adapter was handed.
        source_event_id: String,
        /// Position of that event in its stream.
        source_sequence: u64,
        /// Canonical wire value the adapter answered up front.
        declared: &'static str,
        /// Canonical wire value derived from the admitted envelope.
        derived: &'static str,
    },

    /// The delivery that owns this recovery assessment was cancelled. Nothing
    /// was decided and nothing was written at or after the named stage, so the
    /// next attempt assesses the same incarnation from scratch.
    #[error("recovery assessment was cancelled before {stage}")]
    RecoveryCancelled {
        /// Stage the assessment stopped in front of.
        stage: &'static str,
    },

    /// The assessment's own deadline expired. Same terminal shape as
    /// cancellation: no decision, no write, no consumed repair attempt.
    #[error("recovery assessment budget expired before {stage}")]
    RecoveryDeadlineExceeded {
        /// Stage the assessment stopped in front of.
        stage: &'static str,
    },

    /// A record in the batch belongs to a different source stream than the one
    /// being resumed. One ingest call drives exactly one stream's watermark.
    #[error("ingress batch is scoped to stream {expected} but carries a record from {received}")]
    StreamMismatch {
        /// Stream the resume position names.
        expected: String,
        /// Stream the offending record names.
        received: String,
    },
}
