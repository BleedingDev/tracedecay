//! The storage-neutral ports the authority matrix names.

use crate::envelope::{AdmittedObservationV1, ProviderTargetV1, WithheldAdmissionV1};
use crate::error::ObservationJournalError;
use crate::identity::{
    DispatchLeaseIdV1, ForgetSourceKeyV1, ObservationIdV1, ObservationIdempotencyKeyV1,
    SourceSequenceV1,
};
use crate::inspection::{
    JournalInspectionFilterV1, JournalInspectionPageV1, ObservationLaneKeyV1, QueuePressureV1,
    ReplayCursorV1,
};
use crate::lease::{AttemptOutcomeV1, LeaseRequestV1, LeasedObservationV1};
use crate::orphan::AttemptOrphanRecordV1;
use crate::receipt::ObservationDeliveryReceiptV1;
use crate::refusal::{AttemptRefusalOutcomeV1, AttemptRefusalRecordV1};
use crate::retention::{
    ForgetReceiptV1, ForgetSourceRequestV1, ForgetVerificationV1, RetentionPolicyV1,
    RetentionSweepReceiptV1,
};
use crate::settlement::SourceStreamKeyV1;
use crate::state::DeliveryStateV1;

/// What an admission attempt did. Every refusal is typed and visible; none of
/// them is a silent drop.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AppendOutcomeV1 {
    /// A new journal row and its pending delivery row were created.
    Appended {
        /// Identity of the new observation.
        observation_id: ObservationIdV1,
        /// Position it occupies in its source stream.
        source_sequence: SourceSequenceV1,
    },
    /// The same key already exists with the same canonical content.
    ///
    /// There is deliberately no "same key, different content" *outcome*: the
    /// key is derived from `payload_sha256`, and `validate()` re-derives it, so
    /// no caller state can present one key over other content. A stored row
    /// whose payload digest disagrees with a matching key is therefore store
    /// corruption, and admission fails closed with
    /// [`ObservationJournalError::Corrupt`] rather than reporting an outcome
    /// no legitimate caller can produce.
    ///
    /// [`ObservationJournalError::Corrupt`]: crate::ObservationJournalError::Corrupt
    DuplicateIdempotencyKey {
        /// Identity of the row already present.
        observation_id: ObservationIdV1,
        /// Delivery state of that row.
        state: DeliveryStateV1,
    },
    /// The same settled source event is already journalled for this provider
    /// target at this sequence under a *different* idempotency key. This
    /// happens when the sanitizer rule corpus changed between the first
    /// admission and a crash replay, and it must not create a second row.
    DuplicateSourceEvent {
        /// Identity of the row already present.
        observation_id: ObservationIdV1,
        /// Key that row was admitted under.
        stored_idempotency_key: ObservationIdempotencyKeyV1,
    },
    /// A *different* settled event already occupies this source sequence.
    SourceSequenceConflict {
        /// Event identity already recorded at that sequence.
        stored_source_event_id: String,
        /// Revision already recorded at that sequence.
        stored_source_event_revision: u64,
    },
    /// This provider registration is already past this sequence and the event
    /// is new.
    ///
    /// Regression is tracked **per provider registration**, not per stream: one
    /// settled event legitimately fans out to several registrations, and a
    /// target that is behind must still be able to receive sequence `n` after
    /// another target has advanced to `n + 1`.
    RejectedSourceSequenceRegression {
        /// Highest sequence this provider registration has admitted on the
        /// stream.
        last_admitted: SourceSequenceV1,
    },
    /// Hygiene withheld this exact source position, so it is closed to every
    /// provider registration. Re-admitting it would smuggle refused content
    /// past the decision that refused it.
    RejectedWithheldSource {
        /// Position the hygiene lane refused.
        source_sequence: SourceSequenceV1,
    },
    /// The request deadline had already passed at admission.
    RejectedDeadlineExpired {
        /// The deadline that had passed.
        deadline_unix_micros: i64,
    },
    /// The provider instance's bounded queue is full.
    RejectedCapacity {
        /// Non-terminal rows currently queued.
        queue_items: u64,
        /// Non-terminal queue bytes currently held.
        queue_bytes: u64,
    },
}

/// Admission side of the journal: the seam a settled host action writes through.
pub trait ObservationDispatchPortV1: Send + Sync {
    /// Appends one admitted observation, causally bound to its settled source.
    fn append_admitted(
        &self,
        admitted: &AdmittedObservationV1,
    ) -> Result<AppendOutcomeV1, ObservationJournalError>;

    /// Records a settled event that hygiene refused, advancing the replay
    /// cursor past it so it is never re-emitted.
    fn record_withheld(
        &self,
        withheld: &WithheldAdmissionV1,
    ) -> Result<(), ObservationJournalError>;

    /// Reads the ingress replay position for one source stream.
    fn replay_cursor(
        &self,
        stream: &SourceStreamKeyV1,
    ) -> Result<Option<ReplayCursorV1>, ObservationJournalError>;

    /// Reads bounded queue pressure for one provider lane.
    ///
    /// Addressed by registration rather than by a proven readiness target, so
    /// a caller can measure the lane before it has paid for the handshake that
    /// would name an instance — which is what makes a pre-admission pressure
    /// check cheap enough to run first.
    fn lane_pressure(
        &self,
        lane: &ObservationLaneKeyV1,
    ) -> Result<QueuePressureV1, ObservationJournalError>;

    /// The same pressure, addressed by a target whose readiness is already
    /// proven. One implementation, two ways in: the target is revalidated and
    /// then reduced to the lane it addresses.
    fn queue_pressure(
        &self,
        target: &ProviderTargetV1,
    ) -> Result<QueuePressureV1, ObservationJournalError> {
        target.validate()?;
        self.lane_pressure(&ObservationLaneKeyV1::of(target))
    }
}

/// Delivery side of the journal: lease, acknowledge, reap, inspect.
pub trait ObservationJournalReaderV1: Send + Sync {
    /// Leases deliverable rows for one provider instance.
    fn lease_pending(
        &self,
        request: &LeaseRequestV1,
    ) -> Result<Vec<LeasedObservationV1>, ObservationJournalError>;

    /// Records one immutable delivery attempt receipt and advances the row.
    fn record_attempt(
        &self,
        receipt: &ObservationDeliveryReceiptV1,
    ) -> Result<AttemptOutcomeV1, ObservationJournalError>;

    /// Records host-owned evidence for a cancelled or refused terminal attempt
    /// and returns the matching lease to `Pending` in the same transaction.
    /// Unsettled evidence never terminalizes delivery because effect is unknown.
    fn record_unsettled_attempt(
        &self,
        receipt: &ObservationDeliveryReceiptV1,
        lease: &DispatchLeaseIdV1,
        retry_after_unix_micros: i64,
    ) -> Result<AttemptOutcomeV1, ObservationJournalError>;

    /// Records one immutable refusal of a provider terminal that the host
    /// answered but could not accept as delivery evidence.
    ///
    /// This is deliberately *not* `record_attempt`: nothing about the delivery
    /// row moves and no provider effect is attributed.
    /// It exists so a crash cannot erase the fact that an attempt number was
    /// consumed by an answer the host refused.
    fn record_attempt_refusal(
        &self,
        refusal: &AttemptRefusalRecordV1,
    ) -> Result<AttemptRefusalOutcomeV1, ObservationJournalError>;

    /// Atomically records a rejected terminal, its unknown-effect receipt, and
    /// releases the exact lease that produced both records.
    fn record_refused_terminal_attempt(
        &self,
        refusal: &AttemptRefusalRecordV1,
        receipt: &ObservationDeliveryReceiptV1,
        lease: &DispatchLeaseIdV1,
        retry_after_unix_micros: i64,
    ) -> Result<(AttemptRefusalOutcomeV1, AttemptOutcomeV1), ObservationJournalError>;

    /// Returns one lease to `Pending` with an explicit retry instant.
    fn release_lease(
        &self,
        lease: &DispatchLeaseIdV1,
        retry_after_unix_micros: i64,
    ) -> Result<(), ObservationJournalError>;

    /// Returns lapsed leases to `Pending`, bounded per call.
    fn reap_expired_leases(
        &self,
        now_unix_micros: i64,
        budget: u32,
    ) -> Result<u32, ObservationJournalError>;

    /// Inspects deliveries by operational metadata only.
    fn inspect(
        &self,
        filter: &JournalInspectionFilterV1,
    ) -> Result<JournalInspectionPageV1, ObservationJournalError>;

    /// Reads every receipt recorded for one observation.
    fn receipts_for(
        &self,
        observation_id: &ObservationIdV1,
    ) -> Result<Vec<ObservationDeliveryReceiptV1>, ObservationJournalError>;

    /// Reads every refused provider terminal recorded for one observation, in
    /// attempt order.
    fn attempt_refusals_for(
        &self,
        observation_id: &ObservationIdV1,
    ) -> Result<Vec<AttemptRefusalRecordV1>, ObservationJournalError>;

    /// Reads every orphaned attempt recorded for one observation, in attempt
    /// order.
    ///
    /// Together with [`Self::receipts_for`] and [`Self::attempt_refusals_for`]
    /// this closes the audit over a row's `attempt_number`: every attempt the
    /// lease claim consumed is represented by a receipt, a refusal, or an
    /// orphan record, so a crash between the claim and the answer leaves
    /// durable evidence instead of an unexplained gap in the counter.
    fn attempt_orphans_for(
        &self,
        observation_id: &ObservationIdV1,
    ) -> Result<Vec<AttemptOrphanRecordV1>, ObservationJournalError>;
}

/// Retention and privacy deletion side of the journal.
pub trait ObservationRetentionPortV1: Send + Sync {
    /// The bounds this store enforces.
    fn retention_policy(&self) -> &RetentionPolicyV1;

    /// Runs one bounded retention sweep.
    fn sweep_expired(
        &self,
        now_unix_micros: i64,
        budget: u32,
    ) -> Result<RetentionSweepReceiptV1, ObservationJournalError>;

    /// Deletes the content of every row carrying one forget-source key.
    fn forget_source(
        &self,
        request: &ForgetSourceRequestV1,
    ) -> Result<ForgetReceiptV1, ObservationJournalError>;

    /// Re-queries the store for the deletion postcondition.
    fn verify_forgotten(
        &self,
        key: &ForgetSourceKeyV1,
    ) -> Result<ForgetVerificationV1, ObservationJournalError>;
}
