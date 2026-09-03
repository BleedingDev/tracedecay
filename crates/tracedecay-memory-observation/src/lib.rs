#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(warnings)]
#![deny(clippy::dbg_macro)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(clippy::print_stderr)]
#![deny(clippy::print_stdout)]
#![deny(clippy::todo)]
#![deny(clippy::unimplemented)]
#![deny(clippy::unwrap_used)]
//! Durable provider observation outbox/journal authority.
//!
//! This crate owns the boundary between a **committed** TraceDecay action and
//! provider delivery. It is the single source of truth for delivery status,
//! attempts, acknowledgements, and replay position — and nothing else.
//!
//! # What it is not
//!
//! It is **not** a second authority for Native facts. ADR-0005 is explicit
//! about that, and the design keeps it true structurally rather than by
//! convention:
//!
//! * there is no full-text index, no payload predicate, and no content-keyed
//!   lookup anywhere in the schema;
//! * [`JournalInspectionFilterV1`] filters only by provider, scope, state,
//!   authority, forget key, and time window, and its rows carry digests, never
//!   payload bytes;
//! * the crate depends on no TraceDecay store, database, or domain crate.
//!
//! Any future change that adds an FTS table, a payload-content filter, or a
//! "read the last observation of kind X" helper silently converts this into a
//! second authority. Do not add one.
//!
//! # Admission order
//!
//! Fixed by `provider-observation-contract.json`: sanitize, then canonicalize
//! and derive digests, then append, then dispatch. So the bytes this journal
//! stores are already sanitized, and they are exactly the bytes a dispatcher
//! sends — a provider deduplicating on `payload_sha256` always sees the digest
//! its receipt carries. Pre-sanitization payloads never reach the journal; only
//! their digest does, via [`SanitizationBindingV1::source_payload_sha256`].
//!
//! A settled event that hygiene refuses is recorded through
//! [`ObservationDispatchPortV1::record_withheld`], which advances the replay
//! cursor past it and stores digests only. Without that, a secret-bearing event
//! would be re-emitted forever.
//!
//! # Atomicity
//!
//! Acceptance requires the append to be "atomic with **or** causally bound to"
//! the committed host action. The canonical settlement authorities live behind
//! forbidden exception zones and ADR-0005 rejects cross-store distributed
//! transactions, so admission is causally bound: the append is one atomic unit
//! keyed by exact settled source identity, and a crash between the canonical
//! commit and the append is recovered by re-emitting from
//! [`ObservationDispatchPortV1::replay_cursor`] — safe precisely because the
//! idempotency key is content-derived. `append_admitted_in_transaction` gives a
//! co-located caller true atomicity when it can offer a transaction.
//!
//! # Runtime seam
//!
//! [`IngressRuntimeV1`] and [`DeliveryRuntimeV1`] turn those ports into an
//! ordered, restartable process: recover from the replay cursor, decide each
//! record through a caller-supplied admission adapter, commit the decision and
//! its watermark in one journal transaction, wake delivery, lease, dispatch the
//! exact stored bytes, and record or release. They own no store, no thread, and
//! no clock, so nothing in this crate learns what a TraceDecay store or a
//! provider registry is.

mod envelope;
mod error;
mod identity;
mod inspection;
mod lease;
mod orphan;
mod port;
mod receipt;
mod recovery;
mod refusal;
mod retention;
mod runtime;
mod settlement;
mod sqlite;
mod state;

pub use envelope::{
    AdmittedObservationV1, MAX_PAYLOAD_BYTES, MAX_SANITIZATION_RECEIPT_JSON_BYTES,
    ObservationPrivacyV1, PrivacyClassificationV1, ProvenanceOriginV1, ProviderTargetV1,
    RetentionClassV1, SanitizationBindingV1, WithheldAdmissionV1,
};
pub use error::ObservationJournalError;
pub use identity::{
    DeliveryReceiptIdV1, DispatchLeaseIdV1, ForgetSourceKeyV1, IdempotencyInputV1,
    OBSERVATION_CONTRACT_ID, ObservationIdV1, ObservationIdempotencyKeyV1,
    SOURCE_EVENT_ID_MAX_BYTES, SourceSequenceV1, SourceStreamIdV1, extensions_digest,
};
pub use inspection::{
    JournalInspectionFilterV1, JournalInspectionPageV1, JournalInspectionRowV1,
    ObservationLaneKeyV1, QueuePressureV1, ReplayCursorV1, ReplayDispositionV1,
};
pub use lease::{AttemptOutcomeV1, LeaseRequestV1, LeasedObservationV1};
pub use orphan::{AttemptOrphanCauseV1, AttemptOrphanRecordV1, AttemptOrphanRecoveryV1};
pub use port::{
    AppendOutcomeV1, ObservationDispatchPortV1, ObservationJournalReaderV1,
    ObservationRetentionPortV1,
};
pub use receipt::{
    ObservationCommittedEffectV1, ObservationDeliveryReceiptV1, ObservationOutcomeV1,
    ProviderEffectSummaryV1,
};
pub use recovery::{
    AcknowledgedPositionV1, HostRecoveryStateV1, ObservationRecoveryPortV1, ProviderCheckpointV1,
    ProviderReplayPositionV1, RecoveryAssessmentIdV1, RecoveryBudgetV1, RecoveryControlV1,
    RecoveryPlanV1, RecoveryRefusalWriteV1, RecoveryTargetKeyV1, RecoveryTimeBudgetV1,
    RepairActionV1, STATE_SCHEMA_VERSION_MAX_BYTES, StateIncompatibilityV1,
    UnacknowledgedFrontierV1,
};
pub use refusal::{
    AttemptRefusalCategoryV1, AttemptRefusalOutcomeV1, AttemptRefusalRecordV1,
    REFUSAL_TEXT_MAX_BYTES,
};
pub use retention::{
    ForgetReceiptV1, ForgetSourceRequestV1, ForgetVerificationV1, RetentionPolicyV1,
    RetentionSweepReceiptV1,
};
pub use runtime::{
    AdapterFailureV1, AdmissionDecisionV1, BackpressureDecisionV1, BackpressureGateV1,
    BackpressureHaltV1, BackpressurePolicyV1, BackpressureReasonV1, BackpressureRefusalV1,
    BackpressureStateV1, DeliveryAttemptV1, DeliveryBatchReportV1, DeliveryControlV1,
    DeliveryFailureV1, DeliveryRuntimeV1, DeliveryWakeV1, DispatchPolicyV1, DispatchRequestV1,
    DrainBoundsV1, DrainReportV1, DrainStopV1, ForegroundOutcomeV1, IngressBatchReportV1,
    IngressControlV1, IngressHaltV1, IngressResumeV1, IngressRuntimeV1, IngressStopReasonV1,
    IngressStopV1, ObservationAdmissionAdapterV1, ObservationLoadClassV1, ObservationRuntimeError,
    ProviderDeliveryAdapterV1, QueueBacklogV1, RecoveryRuntimeV1, RetentionSweepScheduleV1,
    RetentionSweeperV1, RetentionTickV1, RetryBackoffV1, ShutdownReportV1, ShutdownRequestV1,
    SourceRecordV1, TerminalIdentityMismatchV1, UTILIZATION_SCALE_PPM, WakeOutcomeV1,
};
pub use settlement::{CanonicalSettlementReceiptV1, SourceAuthorityV1, SourceStreamKeyV1};
pub use sqlite::{
    OPEN_WITHHELD_AUDIT_ROWS, SCHEMA_VERSION, SqliteObservationJournal, WithheldAuditProgressV1,
};
pub use state::DeliveryStateV1;
