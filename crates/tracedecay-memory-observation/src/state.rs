//! The delivery state machine. This — and only this — is the authority for
//! whether a provider has seen an observation.

use crate::error::ObservationJournalError;

/// Delivery state of one journalled observation against one provider instance.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DeliveryStateV1 {
    /// Admitted and waiting for a dispatcher.
    Pending,
    /// Held by a dispatch lease.
    Leased,
    /// The provider acknowledged an effect (or an honest partial effect) with a
    /// provider receipt digest.
    Acknowledged,
    /// The provider recognised the idempotency key and acknowledged a duplicate.
    DuplicateAcknowledged,
    /// The provider refused the observation permanently.
    Rejected,
    /// The provider's effect is genuinely unknown and needs reconciliation.
    /// This is *not* terminal: it stays retryable.
    EffectUnknown,
    /// Delivery was cancelled before acknowledgement.
    Cancelled,
    /// The observation's deadline or retention expiry passed before delivery.
    Expired,
    /// Retries hit the bounded maximum. Visible and inspectable, never deleted
    /// silently.
    Exhausted,
    /// Content was removed by a privacy deletion request.
    Forgotten,
}

impl DeliveryStateV1 {
    /// Returns the canonical wire value.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Leased => "leased",
            Self::Acknowledged => "acknowledged",
            Self::DuplicateAcknowledged => "duplicate_acknowledged",
            Self::Rejected => "rejected",
            Self::EffectUnknown => "effect_unknown",
            Self::Cancelled => "cancelled",
            Self::Expired => "expired",
            Self::Exhausted => "exhausted",
            Self::Forgotten => "forgotten",
        }
    }

    /// Decodes one canonical wire value.
    pub fn from_wire(value: &str) -> Result<Self, ObservationJournalError> {
        match value {
            "pending" => Ok(Self::Pending),
            "leased" => Ok(Self::Leased),
            "acknowledged" => Ok(Self::Acknowledged),
            "duplicate_acknowledged" => Ok(Self::DuplicateAcknowledged),
            "rejected" => Ok(Self::Rejected),
            "effect_unknown" => Ok(Self::EffectUnknown),
            "cancelled" => Ok(Self::Cancelled),
            "expired" => Ok(Self::Expired),
            "exhausted" => Ok(Self::Exhausted),
            "forgotten" => Ok(Self::Forgotten),
            other => Err(ObservationJournalError::UnknownWireValue {
                field: "delivery_state",
                value: other.to_owned(),
            }),
        }
    }

    /// Whether the state ends delivery. `EffectUnknown` deliberately does not:
    /// an unknown effect is reconciled, not abandoned.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Acknowledged
                | Self::DuplicateAcknowledged
                | Self::Rejected
                | Self::Cancelled
                | Self::Expired
                | Self::Exhausted
                | Self::Forgotten
        )
    }

    /// Whether a dispatcher may still pick the row up.
    #[must_use]
    pub const fn is_deliverable(self) -> bool {
        matches!(self, Self::Pending | Self::EffectUnknown)
    }

    /// The explicit legal transition table.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        match self {
            // `Pending -> Exhausted` is reachable without a delivery receipt:
            // a lease consumes an attempt number at claim time, so a row whose
            // leases were all reaped mid-flight runs out of attempts while
            // sitting pending. Retries must converge whether or not a
            // dispatcher ever came back to record one.
            Self::Pending => matches!(
                next,
                Self::Leased | Self::Cancelled | Self::Expired | Self::Exhausted | Self::Forgotten
            ),
            Self::Leased => matches!(
                next,
                Self::Pending
                    | Self::Acknowledged
                    | Self::DuplicateAcknowledged
                    | Self::Rejected
                    | Self::EffectUnknown
                    | Self::Cancelled
                    | Self::Expired
                    | Self::Exhausted
                    | Self::Forgotten
            ),
            Self::EffectUnknown => matches!(
                next,
                Self::Leased | Self::Exhausted | Self::Expired | Self::Forgotten
            ),
            Self::Acknowledged
            | Self::DuplicateAcknowledged
            | Self::Rejected
            | Self::Cancelled
            | Self::Expired
            | Self::Exhausted => matches!(next, Self::Forgotten),
            Self::Forgotten => false,
        }
    }
}
