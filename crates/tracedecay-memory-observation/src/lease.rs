//! Dispatch leases.
//!
//! A lease is a row, not a lock. That is the whole recovery story: a dispatcher
//! that dies mid-flight leaves an expiring lease behind, and any process can
//! reap it without a daemon, a coordinator, or an external lease service.

use tracedecay_memory_provider_api::{
    CanonicalPayload, OwnedExactScope, OwnedOpaqueExtension, OwnedVersionedId,
};

use crate::envelope::{
    ObservationPrivacyV1, ProvenanceOriginV1, ProviderTargetV1, SanitizationBindingV1,
};
use crate::error::ObservationJournalError;
use crate::identity::{
    DispatchLeaseIdV1, ObservationIdV1, ObservationIdempotencyKeyV1, SOURCE_EVENT_ID_MAX_BYTES,
    SourceSequenceV1, require_bounded, require_sha256,
};
use crate::state::DeliveryStateV1;

/// One bounded request for deliverable work.
///
/// Work is addressed by **provider registration**, not by provider instance:
/// the idempotency key is derived over `(provider_id, registration_revision)`,
/// so a restarted provider that re-handshakes as a new instance of the same
/// registration must be able to drain what the previous instance left queued.
/// `provider_instance_id` names the instance making *this* attempt and is
/// recorded as per-attempt evidence on the delivery row and its receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaseRequestV1 {
    /// Provider to lease work for.
    pub provider_id: String,
    /// Pinned provider registration revision to lease work for.
    pub registration_revision: u64,
    /// Provider instance making this attempt. Evidence, not an address.
    pub provider_instance_id: String,
    /// Optional exact-scope restriction.
    pub exact_scope_sha256: Option<String>,
    /// Stable identity of the leasing dispatcher.
    pub lease_owner: String,
    /// Current instant.
    pub now_unix_micros: i64,
    /// How long the lease should hold.
    pub lease_duration_micros: i64,
    /// Maximum rows to lease.
    pub max_items: u32,
    /// Maximum queue bytes to lease.
    pub max_bytes: u64,
}

impl LeaseRequestV1 {
    /// Revalidates the lease request.
    pub fn validate(&self) -> Result<(), ObservationJournalError> {
        require_bounded(&self.provider_id, "provider_id", SOURCE_EVENT_ID_MAX_BYTES)?;
        require_bounded(
            &self.provider_instance_id,
            "provider_instance_id",
            SOURCE_EVENT_ID_MAX_BYTES,
        )?;
        require_bounded(&self.lease_owner, "lease_owner", SOURCE_EVENT_ID_MAX_BYTES)?;
        if self.registration_revision == 0 {
            return Err(ObservationJournalError::ValueOutOfRange {
                field: "registration_revision",
            });
        }
        if let Some(scope) = &self.exact_scope_sha256 {
            require_sha256(scope, "exact_scope_sha256")?;
        }
        if self.lease_duration_micros <= 0 {
            return Err(ObservationJournalError::ValueOutOfRange {
                field: "lease_duration_micros",
            });
        }
        if self.max_items == 0 {
            return Err(ObservationJournalError::ValueOutOfRange { field: "max_items" });
        }
        Ok(())
    }
}

/// One leased observation, ready to hand to a provider.
///
/// The payload here is the journal's own sanitized bytes; a dispatcher sends
/// them verbatim, so the provider's `payload_sha256` comparison always matches
/// the receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeasedObservationV1 {
    /// Lease held over the delivery row.
    pub lease_id: DispatchLeaseIdV1,
    /// When the lease lapses.
    pub lease_expires_at_unix_micros: i64,
    /// Observation identity.
    pub observation_id: ObservationIdV1,
    /// Idempotency key.
    pub idempotency_key: ObservationIdempotencyKeyV1,
    /// Pinned provider registration, carrying the *claiming* instance id so a
    /// receipt records the instance that actually made the attempt.
    pub target: ProviderTargetV1,
    /// Full exact coding scope reconstructed and revalidated from the journal.
    pub exact_scope: OwnedExactScope,
    /// Digest of the exact coding scope, retained for indexed comparisons.
    pub exact_scope_sha256: String,
    /// Observation kind identity.
    pub observation_kind: OwnedVersionedId,
    /// Sanitized canonical payload — the exact bytes to deliver.
    pub payload: CanonicalPayload,
    /// Canonical extension set.
    pub extensions: Vec<OwnedOpaqueExtension>,
    /// Digest over the extension set.
    pub extensions_digest: String,
    /// Admitted privacy metadata.
    pub privacy: ObservationPrivacyV1,
    /// Origin of the content.
    pub provenance_origin: ProvenanceOriginV1,
    /// Hygiene binding as admitted, returned verbatim so a restarted dispatcher
    /// re-attaches the exact minted receipt to its provider call.
    pub sanitization: SanitizationBindingV1,
    /// Attempt number this lease consumed (one-based). The claim increments the
    /// delivery row's counter, so no two leases of one row — including a lease
    /// taken after a reap — ever share an attempt number or a receipt slot.
    pub attempt_number: u32,
    /// Instant after which delivery must stop.
    pub deadline_unix_micros: i64,
    /// Position of the observation in its source stream.
    pub source_sequence: SourceSequenceV1,
}

/// Result of recording one delivery attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AttemptOutcomeV1 {
    /// The receipt was written and the delivery row advanced.
    Recorded {
        /// New delivery state.
        state: DeliveryStateV1,
        /// When the next attempt becomes eligible, when one is scheduled.
        next_attempt_at_unix_micros: Option<i64>,
    },
    /// A receipt already exists for this `(observation, attempt)` pair. The
    /// original stands; receipts are never rewritten. The delivery row is still
    /// settled — from the *standing* receipt, not from the resubmitted one — so
    /// a duplicate submission can never leave a row leased against an attempt
    /// that already finished.
    DuplicateReceipt {
        /// Delivery state the standing receipt settles the row to, or the row's
        /// current state when another attempt already advanced it.
        state: DeliveryStateV1,
    },
    /// The lease had already been reaped or completed, so the delivery row was
    /// not advanced. The receipt is still recorded: nothing is lost.
    LeaseLost {
        /// Receipt identity that was written before the lease check failed.
        receipt_id: crate::identity::DeliveryReceiptIdV1,
    },
}
