//! Durable evidence that a provider answered an attempt and the host refused
//! the answer.
//!
//! A delivery receipt is provider-effect evidence: it exists only when the
//! provider's own terminal described *this* delivery and could be expressed as
//! an observation outcome. When it could not — the terminal named another
//! scope, another provider, another operation, or carried semantics the
//! observation contract does not admit — the runtime must not mint a receipt,
//! because that would attribute a provider effect the host cannot vouch for.
//!
//! But the attempt still happened. The lease claim already consumed the
//! attempt number, the provider was already handed the bytes, and the provider
//! already answered. Keeping that only in an in-memory batch report means a
//! crash erases every trace of a terminal that was refused, after the attempt
//! it belonged to was spent. This record is the durable half: keyed by
//! `(observation, attempt)` exactly like a receipt, immutable exactly like a
//! receipt, and deliberately *not* a receipt — it carries the refusal category
//! and safe terminal metadata (operation, terminal code, operation id, receipt
//! digest), never a committed-effect claim the host refused to believe.

use crate::error::ObservationJournalError;
use crate::identity::{
    ObservationIdV1, ObservationIdempotencyKeyV1, SOURCE_EVENT_ID_MAX_BYTES, require_bounded,
    require_sha256,
};

/// Longest refusal evidence text the journal stores per field.
///
/// The values come from a provider terminal the host already decided it cannot
/// trust, so they are bounded on the way in rather than stored whole.
pub const REFUSAL_TEXT_MAX_BYTES: usize = SOURCE_EVENT_ID_MAX_BYTES;

/// Why a provider's answered terminal was refused as delivery evidence.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum AttemptRefusalCategoryV1 {
    /// The terminal did not describe the leased delivery: it named another
    /// operation, another provider, or another exact coding scope.
    TerminalIdentityMismatch,
    /// The terminal described this delivery but could not be expressed as an
    /// observation receipt — an inadmissible terminal code, an unbound
    /// duplicate acknowledgement, or a missing provider acknowledgement.
    ReceiptNotAdmissible,
}

impl AttemptRefusalCategoryV1 {
    /// Returns the canonical wire value.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::TerminalIdentityMismatch => "terminal_identity_mismatch",
            Self::ReceiptNotAdmissible => "receipt_not_admissible",
        }
    }

    /// Decodes one canonical wire value.
    pub fn from_wire(value: &str) -> Result<Self, ObservationJournalError> {
        match value {
            "terminal_identity_mismatch" => Ok(Self::TerminalIdentityMismatch),
            "receipt_not_admissible" => Ok(Self::ReceiptNotAdmissible),
            other => Err(ObservationJournalError::UnknownWireValue {
                field: "attempt_refusal_category",
                value: other.to_owned(),
            }),
        }
    }
}

/// One durable record of an answered attempt whose terminal the host refused.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttemptRefusalRecordV1 {
    /// Observation the attempt addressed.
    pub observation_id: ObservationIdV1,
    /// Attempt number the lease claim consumed. Never handed out again, so this
    /// record occupies the same slot the refused receipt would have.
    pub attempt_number: u32,
    /// Idempotency key of the delivery.
    pub idempotency_key: ObservationIdempotencyKeyV1,
    /// Provider registration the attempt was addressed to.
    pub provider_id: String,
    /// Provider instance that made the attempt. Evidence, not an address.
    pub provider_instance_id: String,
    /// Pinned registration revision.
    pub registration_revision: u64,
    /// Exact coding scope digest the delivery carried.
    pub exact_scope_sha256: String,
    /// Why the terminal was refused.
    pub category: AttemptRefusalCategoryV1,
    /// Logical field that failed, when the refusal named one.
    pub refused_field: String,
    /// Value the leased delivery carried, when the refusal compared two.
    pub expected: Option<String>,
    /// Value the provider terminal carried, when the refusal compared two.
    pub provided: Option<String>,
    /// The typed refusal rendered for an operator. Bounded, never payload.
    pub detail: String,
    /// Operation the terminal claimed, as a wire value.
    pub terminal_operation: String,
    /// Closed terminal code the provider answered with.
    pub terminal_code: String,
    /// Provider's own operation identity for the answered call.
    pub terminal_operation_id: String,
    /// Provider receipt digest carried by the refused terminal, when it carried
    /// a well-formed one. Retained as metadata; it is never treated as proof
    /// that a provider effect committed.
    pub provider_receipt_digest: Option<String>,
    /// Instant the attempt started.
    pub started_at_unix_micros: i64,
    /// Instant the attempt finished.
    pub finished_at_unix_micros: i64,
    /// Instant the host refused the terminal.
    pub recorded_at_unix_micros: i64,
}

impl AttemptRefusalRecordV1 {
    /// Bounds one provider-supplied evidence string to what the journal stores.
    ///
    /// Truncation is on a UTF-8 boundary, so a bounded value is still a valid
    /// string rather than a broken one.
    #[must_use]
    pub fn bound_text(value: &str) -> String {
        if value.len() <= REFUSAL_TEXT_MAX_BYTES {
            return value.to_owned();
        }
        let mut end = REFUSAL_TEXT_MAX_BYTES;
        while end > 0 && !value.is_char_boundary(end) {
            end -= 1;
        }
        value.get(..end).unwrap_or_default().to_owned()
    }

    /// Keeps a provider-supplied digest only when it really is one.
    ///
    /// A refused terminal is untrusted input; storing a malformed digest would
    /// either fail the write or persist something no reader can compare.
    #[must_use]
    pub fn bound_digest(value: Option<&str>) -> Option<String> {
        value
            .filter(|digest| require_sha256(digest, "provider_receipt_digest").is_ok())
            .map(str::to_owned)
    }

    /// Rejects a record the journal cannot store or a reader cannot decode.
    pub fn validate(&self) -> Result<(), ObservationJournalError> {
        if self.attempt_number == 0 {
            return Err(ObservationJournalError::ValueOutOfRange {
                field: "attempt_number",
            });
        }
        if self.registration_revision == 0 {
            return Err(ObservationJournalError::ValueOutOfRange {
                field: "registration_revision",
            });
        }
        require_bounded(&self.provider_id, "provider_id", REFUSAL_TEXT_MAX_BYTES)?;
        require_bounded(
            &self.provider_instance_id,
            "provider_instance_id",
            REFUSAL_TEXT_MAX_BYTES,
        )?;
        require_sha256(&self.exact_scope_sha256, "exact_scope_sha256")?;
        require_bounded(&self.refused_field, "refused_field", REFUSAL_TEXT_MAX_BYTES)?;
        require_bounded(&self.detail, "detail", REFUSAL_TEXT_MAX_BYTES)?;
        require_bounded(
            &self.terminal_operation,
            "terminal_operation",
            REFUSAL_TEXT_MAX_BYTES,
        )?;
        require_bounded(&self.terminal_code, "terminal_code", REFUSAL_TEXT_MAX_BYTES)?;
        require_bounded(
            &self.terminal_operation_id,
            "terminal_operation_id",
            REFUSAL_TEXT_MAX_BYTES,
        )?;
        for (field, value) in [("expected", &self.expected), ("provided", &self.provided)] {
            if let Some(value) = value {
                require_bounded(value, field, REFUSAL_TEXT_MAX_BYTES)?;
            }
        }
        if let Some(digest) = &self.provider_receipt_digest {
            require_sha256(digest, "provider_receipt_digest")?;
        }
        if self.finished_at_unix_micros < self.started_at_unix_micros {
            return Err(ObservationJournalError::ValueOutOfRange {
                field: "finished_at_unix_micros",
            });
        }
        Ok(())
    }
}

/// Result of recording one refused terminal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptRefusalOutcomeV1 {
    /// The refusal was written. It is immutable from here on.
    Recorded,
    /// A refusal already exists for this `(observation, attempt)` pair. The
    /// original stands; refusals are never rewritten, exactly like receipts.
    AlreadyRecorded,
}
