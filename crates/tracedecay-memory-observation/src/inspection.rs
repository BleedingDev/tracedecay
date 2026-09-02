//! Operational inspection and the ingress replay position.
//!
//! [`JournalInspectionFilterV1`] deliberately offers no content predicate and
//! no kind-keyed content lookup, and [`JournalInspectionRowV1`] carries digests
//! rather than bytes. Inspection is an operations surface, never a recall path:
//! the outbox must not become a second authority for Native facts.

use tracedecay_memory_provider_api::OwnedProviderId;

use crate::envelope::{PrivacyClassificationV1, ProviderTargetV1, RetentionClassV1};
use crate::error::ObservationJournalError;
use crate::identity::{
    ForgetSourceKeyV1, ObservationIdV1, ObservationIdempotencyKeyV1, SourceSequenceV1,
};
use crate::settlement::SourceAuthorityV1;
use crate::state::DeliveryStateV1;

/// How far the journal has admitted from one source stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayCursorV1 {
    /// Highest source sequence admitted or withheld on this stream.
    pub last_admitted_sequence: SourceSequenceV1,
    /// Identity of the event at that sequence.
    pub last_source_event_id: String,
    /// Revision of the event at that sequence, as text: an admitted event's
    /// revision is numeric while a withheld one arrives as the hygiene lane
    /// emits it.
    pub last_source_event_revision: String,
    /// Settlement proof of the last admitted event, absent when the last
    /// position was withheld rather than admitted.
    pub last_settlement_proof_sha256: Option<String>,
    /// Whether the last position was admitted or withheld.
    pub last_disposition: ReplayDispositionV1,
    /// Instant the cursor last moved.
    pub updated_at_unix_micros: i64,
}

/// Whether a replay position was admitted into the journal or withheld.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ReplayDispositionV1 {
    /// The event was admitted and a delivery row exists.
    Admitted,
    /// Hygiene refused the event. No payload and no delivery row exist, and the
    /// event must not be re-emitted.
    Withheld,
}

impl ReplayDispositionV1 {
    /// Returns the canonical wire value.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::Withheld => "withheld",
        }
    }

    /// Decodes one canonical wire value.
    pub fn from_wire(value: &str) -> Result<Self, ObservationJournalError> {
        match value {
            "admitted" => Ok(Self::Admitted),
            "withheld" => Ok(Self::Withheld),
            other => Err(ObservationJournalError::UnknownWireValue {
                field: "last_disposition",
                value: other.to_owned(),
            }),
        }
    }
}

/// The bounded queue one provider *registration* owns.
///
/// Pressure is held per registration, never per instance: a restart mints a
/// new instance identity but inherits exactly the same undelivered backlog. So
/// the lane a record would land in can be named from the registration alone —
/// before any readiness handshake has proven which incarnation will serve it.
///
/// That is not a convenience. It is what lets a caller read the lane's real
/// pressure and refuse a saturated lane *before* it pays for hygiene,
/// canonicalization, digest derivation, and a readiness proof — the whole
/// foreground cost the gate exists to avoid spending on work that cannot be
/// admitted.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ObservationLaneKeyV1 {
    /// Logical provider identity that owns the queue.
    pub provider_id: OwnedProviderId,
    /// Pinned registration revision the queue is accounted under.
    pub registration_revision: u64,
}

impl ObservationLaneKeyV1 {
    /// The lane a proven readiness target addresses.
    #[must_use]
    pub fn of(target: &ProviderTargetV1) -> Self {
        Self {
            provider_id: target.provider_id.clone(),
            registration_revision: target.registration_revision,
        }
    }

    /// Refuses a lane key that cannot address a registration.
    pub fn validate(&self) -> Result<(), ObservationJournalError> {
        if self.registration_revision == 0 {
            return Err(ObservationJournalError::ValueOutOfRange {
                field: "registration_revision",
            });
        }
        Ok(())
    }
}

/// Queue pressure against one provider instance.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueuePressureV1 {
    /// Non-terminal rows queued.
    pub queue_items: u64,
    /// Non-terminal queue bytes.
    pub queue_bytes: u64,
    /// Admission instant of the oldest non-terminal row.
    pub oldest_admitted_at_unix_micros: Option<i64>,
    /// Configured item ceiling.
    pub max_queue_items: u64,
    /// Configured byte ceiling.
    pub max_queue_bytes: u64,
}

impl QueuePressureV1 {
    /// Whether one more row of `queue_bytes` would exceed either ceiling.
    #[must_use]
    pub const fn would_exceed(&self, additional_bytes: u64) -> bool {
        self.queue_items >= self.max_queue_items
            || self.queue_bytes.saturating_add(additional_bytes) > self.max_queue_bytes
    }
}

/// Operational filter over journalled deliveries.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct JournalInspectionFilterV1 {
    /// Restrict to one provider.
    pub provider_id: Option<String>,
    /// Restrict to the instance an observation was admitted for. Deliveries are
    /// not addressed by instance, so this is an audit filter over admission
    /// evidence, never a delivery selector.
    pub provider_instance_id: Option<String>,
    /// Restrict to one exact coding scope.
    pub exact_scope_sha256: Option<String>,
    /// Restrict to a set of delivery states.
    pub states: Vec<DeliveryStateV1>,
    /// Restrict to one source authority.
    pub source_authority: Option<SourceAuthorityV1>,
    /// Restrict to one privacy deletion key.
    pub forget_source_key: Option<ForgetSourceKeyV1>,
    /// Lower admission bound, exclusive.
    pub admitted_after_unix_micros: Option<i64>,
    /// Upper admission bound, exclusive.
    pub admitted_before_unix_micros: Option<i64>,
    /// Page size.
    pub limit: u32,
    /// Idempotency key to resume after.
    pub after_cursor: Option<String>,
}

/// One inspected delivery. Digests and metadata only — never payload bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalInspectionRowV1 {
    /// Observation identity.
    pub observation_id: ObservationIdV1,
    /// Idempotency key.
    pub idempotency_key: ObservationIdempotencyKeyV1,
    /// Provider identity.
    pub provider_id: String,
    /// Provider instance the observation was *admitted* for.
    pub provider_instance_id: String,
    /// Pinned registration revision the delivery is addressed to. This, with
    /// `provider_id`, is what leases and capacity are scoped by.
    pub registration_revision: u64,
    /// Provider instance that most recently claimed a lease on this delivery,
    /// when one has. Per-attempt evidence: it legitimately differs from
    /// `provider_instance_id` once a provider has restarted.
    pub last_provider_instance_id: Option<String>,
    /// Digest of the exact coding scope.
    pub exact_scope_sha256: String,
    /// Source authority.
    pub source_authority: SourceAuthorityV1,
    /// Source stream identity.
    pub source_stream: String,
    /// Position in the source stream.
    pub source_sequence: SourceSequenceV1,
    /// Observation kind identity.
    pub observation_kind: String,
    /// Payload contract identity.
    pub payload_contract: String,
    /// Digest of the sanitized canonical payload.
    pub payload_sha256: String,
    /// Digest of the extension set.
    pub extensions_digest: String,
    /// Privacy classification.
    pub privacy_classification: PrivacyClassificationV1,
    /// Retention class.
    pub retention_class: RetentionClassV1,
    /// Privacy deletion key.
    pub forget_source_key: ForgetSourceKeyV1,
    /// Delivery state.
    pub state: DeliveryStateV1,
    /// Attempts recorded so far.
    pub attempt_number: u32,
    /// When the next attempt becomes eligible.
    pub next_attempt_at_unix_micros: i64,
    /// Instant of admission.
    pub admitted_at_unix_micros: i64,
    /// Instant after which delivery must stop.
    pub deadline_unix_micros: i64,
    /// Whether the row's content bytes are still present.
    pub content_present: bool,
    /// When the content was purged or forgotten.
    pub content_forgotten_at_unix_micros: Option<i64>,
}

/// One page of inspected deliveries.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct JournalInspectionPageV1 {
    /// Rows in this page.
    pub rows: Vec<JournalInspectionRowV1>,
    /// Total rows matching the filter, ignoring the page bound.
    pub total_rows: u64,
    /// Cursor to resume after, when more rows remain.
    pub next_cursor: Option<String>,
}
