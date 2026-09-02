//! The storage-neutral ingress and delivery runtime seam.
//!
//! [`ObservationDispatchPortV1`] and [`ObservationJournalReaderV1`] say what the
//! journal can be asked to do. This module says in what order, under what
//! recovery discipline, and with what bounds a process actually asks — without
//! knowing what store is behind the port, what produced the canonical records,
//! or what a provider is.
//!
//! [`ObservationDispatchPortV1`]: crate::ObservationDispatchPortV1
//! [`ObservationJournalReaderV1`]: crate::ObservationJournalReaderV1
//!
//! Two runtimes, one wake edge between them:
//!
//! * [`IngressRuntimeV1`] recovers a stream's replay position from the journal,
//!   walks canonical records through a caller-supplied admission and hygiene
//!   adapter in authoritative sequence, and commits each decision as an append
//!   or a withholding — each of which advances the watermark inside the same
//!   journal transaction as the row it describes.
//! * [`DeliveryRuntimeV1`] leases pending rows, hands the journal's exact stored
//!   bytes to a typed provider delivery adapter, records an immutable attempt
//!   receipt for every provider answer, releases the lease when there was no
//!   answer, reaps lapsed leases, and stops within an explicit bound.
//! * [`DeliveryWakeV1`] carries the signal from the first to the second.
//! * [`RetentionSweeperV1`] decides, on the caller's clock, when the journal's
//!   bounded retention sweep is due, so an expired row is terminalized and
//!   purged by the same loop that delivers — never by a sweep nobody mounts.
//!
//! # What it deliberately is not
//!
//! It owns no store, no thread, no clock, and no fact authority. Every instant
//! is supplied by the caller or stamped by the journal, every loop belongs to
//! the caller, and both adapters are traits with associated error types so no
//! TraceDecay store, provider registry, or host type crosses this boundary.
//! Nothing here reads content: the runtime moves bytes it never inspects.

mod delivery;
mod dispatch_policy;
mod error;
mod ingress;
mod retention;
mod wake;

pub use delivery::{
    DeliveryAttemptV1, DeliveryBatchReportV1, DeliveryControlV1, DeliveryFailureV1,
    DeliveryRuntimeV1, DispatchRequestV1, ProviderDeliveryAdapterV1, ShutdownReportV1,
    ShutdownRequestV1,
};
pub use dispatch_policy::DispatchPolicyV1;
pub use error::{AdapterFailureV1, ObservationRuntimeError, TerminalIdentityMismatchV1};
pub use ingress::{
    AdmissionDecisionV1, IngressBatchReportV1, IngressHaltV1, IngressResumeV1, IngressRuntimeV1,
    ObservationAdmissionAdapterV1, SourceRecordV1,
};
pub use retention::{RetentionSweepScheduleV1, RetentionSweeperV1, RetentionTickV1};
pub use wake::{DeliveryWakeV1, WakeOutcomeV1};
