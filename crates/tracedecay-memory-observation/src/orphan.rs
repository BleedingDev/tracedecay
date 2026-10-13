//! Durable evidence that a delivery attempt was consumed and never answered.
//!
//! A lease claim increments `attempt_number` before any provider is contacted,
//! and that number is never handed back. So a dispatcher that dies between the
//! claim and the durable answer leaves a spent attempt with no receipt and no
//! refusal behind it: the row's counter says an attempt happened, and nothing
//! in the store says what became of it. Reconstructing that from the counter
//! alone is guesswork — it cannot distinguish an attempt that died before the
//! provider received the bytes from one that died after the provider committed
//! its effect, and it cannot say whether the row was then recovered or left to
//! exhaust.
//!
//! This record closes that hole. When the reaper reclaims a lapsed lease it
//! writes one immutable row, keyed by `(observation, attempt)` exactly like a
//! receipt and a refusal, naming the lease that was reclaimed, the payload
//! digest the attempt carried, and the recovery the reaper chose. The three
//! tables together are then a complete audit of every attempt number a
//! delivery row ever spent:
//!
//! ```text
//! attempt_number == receipts + attempt refusals + orphaned attempts
//! ```
//!
//! Like a refusal, an orphan record is deliberately *not* a receipt: it makes
//! no claim about whether a provider effect committed. That is precisely what
//! it does not know, and pretending otherwise would let a crash launder itself
//! into delivery evidence.

use crate::error::ObservationJournalError;
use crate::identity::{
    DispatchLeaseIdV1, ObservationIdV1, ObservationIdempotencyKeyV1, SOURCE_EVENT_ID_MAX_BYTES,
    require_bounded, require_sha256,
};

/// Why an attempt number was spent without a durable answer.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum AttemptOrphanCauseV1 {
    /// The lease that consumed the attempt lapsed while it was still held: the
    /// dispatcher that claimed it never released it and never recorded an
    /// answer, which is what a process death anywhere between the claim and
    /// the acknowledgement write looks like from the store.
    LeaseExpiredWithoutAnswer,
}

impl AttemptOrphanCauseV1 {
    /// Returns the canonical wire value.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::LeaseExpiredWithoutAnswer => "lease_expired_without_answer",
        }
    }

    /// Decodes one canonical wire value.
    pub fn from_wire(value: &str) -> Result<Self, ObservationJournalError> {
        match value {
            "lease_expired_without_answer" => Ok(Self::LeaseExpiredWithoutAnswer),
            other => Err(ObservationJournalError::UnknownWireValue {
                field: "attempt_orphan_cause",
                value: other.to_owned(),
            }),
        }
    }
}

/// What the host did with the row whose attempt was orphaned.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum AttemptOrphanRecoveryV1 {
    /// The row went back to `Pending` with attempts left, so the next dispatch
    /// round redelivers it. Recovery is the redelivery, and its evidence is the
    /// receipt that redelivery writes under the *next* attempt number.
    RedeliveryScheduled,
    /// The orphaned attempt was the row's last one. Nothing will redeliver it;
    /// the next lease pass terminalizes the row as exhausted. Recorded as its
    /// own outcome so an operator can tell "recovered by redelivery" from
    /// "recovery was no longer possible".
    AttemptsExhausted,
}

impl AttemptOrphanRecoveryV1 {
    /// Returns the canonical wire value.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::RedeliveryScheduled => "redelivery_scheduled",
            Self::AttemptsExhausted => "attempts_exhausted",
        }
    }

    /// Decodes one canonical wire value.
    pub fn from_wire(value: &str) -> Result<Self, ObservationJournalError> {
        match value {
            "redelivery_scheduled" => Ok(Self::RedeliveryScheduled),
            "attempts_exhausted" => Ok(Self::AttemptsExhausted),
            other => Err(ObservationJournalError::UnknownWireValue {
                field: "attempt_orphan_recovery",
                value: other.to_owned(),
            }),
        }
    }
}

/// One immutable record of an attempt number spent with no durable answer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttemptOrphanRecordV1 {
    /// Observation the orphaned attempt addressed.
    pub observation_id: ObservationIdV1,
    /// Attempt number the lapsed lease claim consumed. Never handed out again,
    /// so this record occupies the slot the missing answer would have.
    pub attempt_number: u32,
    /// Idempotency key of the delivery.
    pub idempotency_key: ObservationIdempotencyKeyV1,
    /// Provider registration the attempt was addressed to.
    pub provider_id: String,
    /// Provider instance that most recently claimed the row, when the store
    /// recorded one. Evidence, not an address.
    pub provider_instance_id: Option<String>,
    /// Pinned registration revision.
    pub registration_revision: u64,
    /// Exact coding scope digest the delivery carried.
    pub exact_scope_sha256: String,
    /// The lease that was reclaimed. Ties the orphan to the exact claim.
    pub lease_id: DispatchLeaseIdV1,
    /// Owner named by that lease.
    pub lease_owner: String,
    /// Digest of the payload the orphaned attempt would have delivered, so the
    /// record names the content and not only the row.
    pub payload_sha256: String,
    /// Why the attempt was orphaned.
    pub cause: AttemptOrphanCauseV1,
    /// What the reaper did with the row.
    pub recovery: AttemptOrphanRecoveryV1,
    /// Instant the reclaimed lease had expired at.
    pub lease_expired_at_unix_micros: i64,
    /// Instant the reaper recorded the orphan.
    pub recorded_at_unix_micros: i64,
}

impl AttemptOrphanRecordV1 {
    /// Rejects a record the journal cannot store as audit evidence.
    pub fn validate(&self) -> Result<(), ObservationJournalError> {
        if self.attempt_number == 0 {
            return Err(ObservationJournalError::ValueOutOfRange {
                field: "attempt_orphan_attempt_number",
            });
        }
        require_bounded(&self.provider_id, "provider_id", SOURCE_EVENT_ID_MAX_BYTES)?;
        if let Some(instance) = &self.provider_instance_id {
            require_bounded(instance, "provider_instance_id", SOURCE_EVENT_ID_MAX_BYTES)?;
        }
        if self.registration_revision == 0 {
            return Err(ObservationJournalError::ValueOutOfRange {
                field: "registration_revision",
            });
        }
        require_sha256(&self.exact_scope_sha256, "exact_scope_sha256")?;
        require_bounded(&self.lease_owner, "lease_owner", SOURCE_EVENT_ID_MAX_BYTES)?;
        require_sha256(&self.payload_sha256, "payload_sha256")?;
        Ok(())
    }
}
