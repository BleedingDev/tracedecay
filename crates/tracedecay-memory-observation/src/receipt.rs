//! Immutable per-attempt delivery receipts.
//!
//! The journal writes receipts and never updates them: there is no `UPDATE`
//! statement against the receipt table anywhere in this crate, which is how
//! the contract's `receipt_is_immutable` is enforced structurally rather than
//! by convention.

use serde::{Deserialize, Serialize};
use tracedecay_memory_provider_api::contract::{CommittedEffectState, TerminalCode};
use tracedecay_memory_provider_api::{OwnedProviderId, TerminalRecord};

use crate::error::ObservationJournalError;
use crate::identity::{
    DeliveryReceiptIdV1, ObservationIdV1, ObservationIdempotencyKeyV1, SOURCE_EVENT_ID_MAX_BYTES,
    require_bounded, require_sha256,
};
use crate::lease::LeasedObservationV1;
use crate::state::DeliveryStateV1;

/// The fourteen `provider_acceptance_outcomes` of the observation contract.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ObservationOutcomeV1 {
    /// The provider applied the observation.
    Applied,
    /// The provider recognised the idempotency key and acknowledged a duplicate.
    DuplicateAcknowledged,
    /// The provider rejected a contract violation.
    RejectedContractViolation,
    /// The provider rejected a scope mismatch.
    RejectedScopeMismatch,
    /// The provider rejected missing provenance.
    RejectedProvenanceUnavailable,
    /// The provider rejected the observation on privacy policy grounds.
    RejectedPrivacyPolicy,
    /// The payload exceeded the provider's bounds.
    RejectedPayloadTooLarge,
    /// A required extension was unsupported.
    RejectedExtensionUnsupported,
    /// The same key arrived with different canonical content.
    IdempotencyConflict,
    /// The provider was unavailable. Retryable.
    ProviderUnavailable,
    /// The deadline elapsed. Retryable while the observation deadline holds.
    DeadlineExceeded,
    /// The attempt was cancelled.
    Cancelled,
    /// The provider committed part of the observation and said so honestly.
    PartialEffect,
    /// The provider's effect is genuinely unknown. Retryable via reconciliation.
    EffectUnknown,
}

impl ObservationOutcomeV1 {
    /// Returns the canonical wire value.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::DuplicateAcknowledged => "duplicate_acknowledged",
            Self::RejectedContractViolation => "rejected_contract_violation",
            Self::RejectedScopeMismatch => "rejected_scope_mismatch",
            Self::RejectedProvenanceUnavailable => "rejected_provenance_unavailable",
            Self::RejectedPrivacyPolicy => "rejected_privacy_policy",
            Self::RejectedPayloadTooLarge => "rejected_payload_too_large",
            Self::RejectedExtensionUnsupported => "rejected_extension_unsupported",
            Self::IdempotencyConflict => "idempotency_conflict",
            Self::ProviderUnavailable => "provider_unavailable",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::Cancelled => "cancelled",
            Self::PartialEffect => "partial_effect",
            Self::EffectUnknown => "effect_unknown",
        }
    }

    /// Decodes one canonical wire value.
    pub fn from_wire(value: &str) -> Result<Self, ObservationJournalError> {
        match value {
            "applied" => Ok(Self::Applied),
            "duplicate_acknowledged" => Ok(Self::DuplicateAcknowledged),
            "rejected_contract_violation" => Ok(Self::RejectedContractViolation),
            "rejected_scope_mismatch" => Ok(Self::RejectedScopeMismatch),
            "rejected_provenance_unavailable" => Ok(Self::RejectedProvenanceUnavailable),
            "rejected_privacy_policy" => Ok(Self::RejectedPrivacyPolicy),
            "rejected_payload_too_large" => Ok(Self::RejectedPayloadTooLarge),
            "rejected_extension_unsupported" => Ok(Self::RejectedExtensionUnsupported),
            "idempotency_conflict" => Ok(Self::IdempotencyConflict),
            "provider_unavailable" => Ok(Self::ProviderUnavailable),
            "deadline_exceeded" => Ok(Self::DeadlineExceeded),
            "cancelled" => Ok(Self::Cancelled),
            "partial_effect" => Ok(Self::PartialEffect),
            "effect_unknown" => Ok(Self::EffectUnknown),
            other => Err(ObservationJournalError::UnknownWireValue {
                field: "outcome",
                value: other.to_owned(),
            }),
        }
    }

    /// Whether the contract requires a provider receipt digest for this
    /// outcome (`provider_receipt_digest_required_for`).
    #[must_use]
    pub const fn requires_provider_receipt(self) -> bool {
        matches!(
            self,
            Self::Applied | Self::DuplicateAcknowledged | Self::PartialEffect
        )
    }

    /// Whether another attempt is admissible.
    #[must_use]
    pub const fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::ProviderUnavailable | Self::DeadlineExceeded | Self::EffectUnknown
        )
    }

    /// Delivery state this outcome implies when retries are still available.
    #[must_use]
    pub const fn implied_state(self) -> DeliveryStateV1 {
        match self {
            // A partial effect is an acknowledged effect: the receipt records
            // `partial` so the partiality is never rounded up to success.
            Self::Applied | Self::PartialEffect => DeliveryStateV1::Acknowledged,
            Self::DuplicateAcknowledged => DeliveryStateV1::DuplicateAcknowledged,
            Self::RejectedContractViolation
            | Self::RejectedScopeMismatch
            | Self::RejectedProvenanceUnavailable
            | Self::RejectedPrivacyPolicy
            | Self::RejectedPayloadTooLarge
            | Self::RejectedExtensionUnsupported
            | Self::IdempotencyConflict => DeliveryStateV1::Rejected,
            Self::ProviderUnavailable | Self::DeadlineExceeded => DeliveryStateV1::Pending,
            Self::Cancelled => DeliveryStateV1::Cancelled,
            Self::EffectUnknown => DeliveryStateV1::EffectUnknown,
        }
    }

    /// Maps one generated terminal code and its committed-effect state onto an
    /// observation outcome.
    ///
    /// The effect state is what separates `applied` from `duplicate_acknowledged`
    /// on a successful write: delivery is at least once, so `success` alone
    /// cannot say whether this attempt created the effect. Both are read from
    /// the provider's own typed evidence — nothing here infers a duplicate from
    /// an attempt number, an empty payload, a diagnostic string, or a provider
    /// identity.
    ///
    /// `SuccessZeroResults` is a recall shape and is not admissible for an
    /// observation write.
    pub fn from_terminal_semantics(
        code: TerminalCode,
        effect_state: CommittedEffectState,
    ) -> Result<Self, ObservationJournalError> {
        match code {
            TerminalCode::Success if effect_state == CommittedEffectState::Duplicate => {
                Ok(Self::DuplicateAcknowledged)
            }
            TerminalCode::Success => Ok(Self::Applied),
            TerminalCode::Partial | TerminalCode::PartialEffect => Ok(Self::PartialEffect),
            TerminalCode::EffectUnknown => Ok(Self::EffectUnknown),
            TerminalCode::Conflict => Ok(Self::IdempotencyConflict),
            TerminalCode::ScopeMismatch => Ok(Self::RejectedScopeMismatch),
            TerminalCode::ContractViolation
            | TerminalCode::InvalidRequest
            | TerminalCode::Unauthorized
            | TerminalCode::StaleIdentity => Ok(Self::RejectedContractViolation),
            TerminalCode::CapabilityUnsupported => Ok(Self::RejectedExtensionUnsupported),
            TerminalCode::CapacityExceeded
            | TerminalCode::ProviderUnavailable
            | TerminalCode::ScopeUnavailable
            | TerminalCode::ResetRequired
            | TerminalCode::StateIncompatible
            | TerminalCode::InternalFailure => Ok(Self::ProviderUnavailable),
            TerminalCode::DeadlineExceeded => Ok(Self::DeadlineExceeded),
            TerminalCode::Cancelled => Ok(Self::Cancelled),
            TerminalCode::SuccessZeroResults => {
                Err(ObservationJournalError::TerminalNotAdmissible {
                    terminal_code: code.as_wire(),
                })
            }
        }
    }
}

/// Committed-effect state as the *observation* contract enumerates it.
///
/// Kept distinct from the generated `CommittedEffectState` because the two
/// name the same five facts differently: the provider reports `committed`,
/// while the journal records what that commit meant for *this* delivery and
/// calls it `applied`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ObservationCommittedEffectV1 {
    /// No provider-local effect.
    None,
    /// A provider-local effect was applied.
    Applied,
    /// The provider recognised the observation as already applied.
    Duplicate,
    /// A partial provider-local effect.
    Partial,
    /// The effect is genuinely unknown.
    Unknown,
}

impl ObservationCommittedEffectV1 {
    /// Returns the canonical wire value.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Applied => "applied",
            Self::Duplicate => "duplicate",
            Self::Partial => "partial",
            Self::Unknown => "unknown",
        }
    }

    /// Decodes one canonical wire value.
    pub fn from_wire(value: &str) -> Result<Self, ObservationJournalError> {
        match value {
            "none" => Ok(Self::None),
            "applied" => Ok(Self::Applied),
            "duplicate" => Ok(Self::Duplicate),
            "partial" => Ok(Self::Partial),
            "unknown" => Ok(Self::Unknown),
            other => Err(ObservationJournalError::UnknownWireValue {
                field: "committed_effect",
                value: other.to_owned(),
            }),
        }
    }

    /// Projects one generated committed-effect state onto the observation
    /// enumeration.
    ///
    /// This is a one-to-one rename now that the wire contract carries
    /// `duplicate` in its own right: the projection never has to guess which
    /// kind of commit it is looking at.
    #[must_use]
    pub const fn from_generated(state: CommittedEffectState) -> Self {
        match state {
            CommittedEffectState::None => Self::None,
            CommittedEffectState::Committed => Self::Applied,
            CommittedEffectState::Duplicate => Self::Duplicate,
            CommittedEffectState::Partial => Self::Partial,
            CommittedEffectState::Unknown => Self::Unknown,
        }
    }
}

/// What the provider reported about its own opaque effects. Never canonical
/// memory state.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderEffectSummaryV1 {
    /// Number of provider-local effects the provider claims.
    pub effect_count: u32,
    /// Opaque provider-local memory references.
    pub stable_memory_refs: Vec<String>,
    /// Opaque provider-local trace references.
    pub provider_trace_refs: Vec<String>,
    /// Why the provider produced no effect, when it produced none.
    pub no_effect_reason: Option<String>,
}

/// One immutable delivery attempt receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservationDeliveryReceiptV1 {
    /// Receipt identity, derived from the attempt it describes.
    pub receipt_id: DeliveryReceiptIdV1,
    /// Observation the attempt delivered.
    pub observation_id: ObservationIdV1,
    /// Idempotency key of that observation.
    pub idempotency_key: ObservationIdempotencyKeyV1,
    /// Digest of the delivered payload. Delivered bytes are journal bytes, so
    /// this is also the journal row's digest.
    pub payload_sha256: String,
    /// Digest of the delivered extension set.
    pub extensions_digest: String,
    /// Provider the attempt addressed.
    pub provider_id: OwnedProviderId,
    /// Provider instance that actually made this attempt.
    ///
    /// Per-attempt evidence, not an address: successive attempts on one
    /// observation legitimately name different instances of the same
    /// registration, and a receipt the journal wrote for itself — a deadline,
    /// a retention expiry, a privacy deletion — names none, because no provider
    /// instance was involved.
    pub provider_instance_id: Option<String>,
    /// Pinned registration revision the attempt was addressed to.
    pub registration_revision: u64,
    /// Provider state generation before the attempt, when reported.
    pub state_generation_before: Option<u64>,
    /// Provider state generation after the attempt, when reported.
    pub state_generation_after: Option<u64>,
    /// One-based attempt number.
    pub attempt_number: u32,
    /// Typed acceptance outcome.
    pub outcome: ObservationOutcomeV1,
    /// Committed effect the provider reported.
    pub committed_effect: ObservationCommittedEffectV1,
    /// Provider effect summary.
    pub provider_effect_summary: ProviderEffectSummaryV1,
    /// Provider acknowledgement digest.
    pub provider_receipt_digest: Option<String>,
    /// Instant the attempt started.
    pub started_at_unix_micros: i64,
    /// Instant the attempt finished.
    pub finished_at_unix_micros: i64,
    /// Non-fatal warnings recorded with the attempt.
    pub warnings: Vec<String>,
}

impl ObservationDeliveryReceiptV1 {
    /// Builds a receipt from what the provider actually returned.
    ///
    /// Terminal semantics are read from the generated summary rather than
    /// re-invented, so a contract regeneration that changes them shows up here.
    pub fn from_terminal(
        terminal: &TerminalRecord,
        leased: &LeasedObservationV1,
        started_at_unix_micros: i64,
        finished_at_unix_micros: i64,
    ) -> Result<Self, ObservationJournalError> {
        let summary = terminal.borrowed();
        let outcome = ObservationOutcomeV1::from_terminal_semantics(
            summary.terminal_code,
            summary.committed_effect.state,
        )?;
        let committed_effect =
            ObservationCommittedEffectV1::from_generated(summary.committed_effect.state);
        if outcome == ObservationOutcomeV1::DuplicateAcknowledged {
            // The provider says it already applied *a* mutation. The journal
            // records a duplicate only once that claim names the key of the
            // observation actually being delivered; anything else is a
            // duplicate acknowledgement for someone else's work.
            let claimed = summary
                .committed_effect
                .duplicate_of_idempotency_key
                .ok_or(ObservationJournalError::UnboundDuplicateAcknowledgement {
                    field: "duplicate_of_idempotency_key",
                })?;
            if claimed != leased.idempotency_key.as_str() {
                return Err(ObservationJournalError::DuplicateAcknowledgementKeyMismatch);
            }
            let committing_operation = summary
                .committed_effect
                .duplicate_of_operation_id
                .unwrap_or_default();
            if committing_operation.is_empty() {
                return Err(ObservationJournalError::UnboundDuplicateAcknowledgement {
                    field: "duplicate_of_operation_id",
                });
            }
        }
        let receipt = Self {
            receipt_id: DeliveryReceiptIdV1::derive(&leased.observation_id, leased.attempt_number),
            observation_id: leased.observation_id.clone(),
            idempotency_key: leased.idempotency_key.clone(),
            payload_sha256: leased.payload.sha256.clone(),
            extensions_digest: leased.extensions_digest.clone(),
            provider_id: leased.target.provider_id.clone(),
            provider_instance_id: Some(leased.target.provider_instance_id.clone()),
            registration_revision: leased.target.registration_revision,
            state_generation_before: summary.committed_effect.state_generation_before,
            state_generation_after: summary.committed_effect.state_generation_after,
            attempt_number: leased.attempt_number,
            outcome,
            committed_effect,
            provider_effect_summary: ProviderEffectSummaryV1 {
                effect_count: u32::try_from(summary.committed_effect.committed_item_refs.len())
                    .unwrap_or(u32::MAX),
                stable_memory_refs: summary
                    .committed_effect
                    .committed_item_refs
                    .iter()
                    .map(String::from)
                    .collect(),
                provider_trace_refs: summary
                    .committed_effect
                    .uncommitted_item_refs
                    .iter()
                    .map(String::from)
                    .collect(),
                no_effect_reason: summary.diagnostic_id.map(String::from),
            },
            provider_receipt_digest: summary
                .committed_effect
                .provider_receipt_digest
                .map(String::from),
            started_at_unix_micros,
            finished_at_unix_micros,
            warnings: Vec::new(),
        };
        receipt.validate()?;
        Ok(receipt)
    }

    /// Rejects a receipt that claims success the provider never acknowledged.
    pub fn validate(&self) -> Result<(), ObservationJournalError> {
        if self.attempt_number == 0 {
            return Err(ObservationJournalError::ValueOutOfRange {
                field: "attempt_number",
            });
        }
        if let Some(instance) = &self.provider_instance_id {
            require_bounded(instance, "provider_instance_id", SOURCE_EVENT_ID_MAX_BYTES)?;
        }
        require_sha256(&self.payload_sha256, "payload_sha256")?;
        require_sha256(&self.extensions_digest, "extensions_digest")?;
        if let Some(digest) = &self.provider_receipt_digest {
            require_sha256(digest, "provider_receipt_digest")?;
        } else if self.outcome.requires_provider_receipt() {
            return Err(
                ObservationJournalError::AcknowledgementWithoutProviderReceipt {
                    outcome: self.outcome.as_wire(),
                },
            );
        }
        if self.finished_at_unix_micros < self.started_at_unix_micros {
            return Err(ObservationJournalError::ValueOutOfRange {
                field: "finished_at_unix_micros",
            });
        }
        let derived = DeliveryReceiptIdV1::derive(&self.observation_id, self.attempt_number);
        if derived != self.receipt_id {
            return Err(ObservationJournalError::ReceiptDigestMismatch {
                field: "receipt_id",
            });
        }
        // `from_terminal` reads the outcome and the committed effect from one
        // piece of provider evidence, so the two can only disagree on a receipt
        // that was assembled by hand or a persisted row that drifted. Both
        // directions are refused: a duplicate acknowledgement with no duplicate
        // effect behind it is the exact "duplicate inferred from a bare
        // success" claim the provider evidence exists to prevent, and a
        // duplicate effect filed under another outcome hides one.
        if (self.outcome == ObservationOutcomeV1::DuplicateAcknowledged)
            != (self.committed_effect == ObservationCommittedEffectV1::Duplicate)
        {
            return Err(
                ObservationJournalError::DuplicateAcknowledgementIncoherent {
                    outcome: self.outcome.as_wire(),
                    committed_effect: self.committed_effect.as_wire(),
                },
            );
        }
        Ok(())
    }

    /// Delivery state this receipt implies before retry bounds are applied.
    #[must_use]
    pub const fn implied_state(&self) -> DeliveryStateV1 {
        self.outcome.implied_state()
    }

    /// Whether the journal may schedule another attempt.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        self.outcome.is_retryable()
    }
}
