//! Proof that a canonical TraceDecay authority settled before an observation
//! was ever created.
//!
//! `AdmittedObservationV1` cannot be constructed without one of these receipts,
//! which is the mechanical form of the observation contract's
//! `unsettled_source_policy: reject_not_canonically_settled`.

use serde::{Deserialize, Serialize};

use crate::error::ObservationJournalError;
use crate::identity::{
    SOURCE_EVENT_ID_MAX_BYTES, SourceSequenceV1, SourceStreamIdV1, require_bounded, require_sha256,
};

/// The nine canonical source authorities the observation contract admits.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceAuthorityV1 {
    /// Host session admission authority.
    HostSession,
    /// Tool execution settlement authority.
    ToolExecution,
    /// Source edit publication authority.
    SourceEdit,
    /// Test execution settlement authority.
    TestExecution,
    /// Diagnostic broker authority.
    DiagnosticBroker,
    /// Git evidence authority.
    GitEvidence,
    /// Native fact promotion authority.
    NativeFactPromotion,
    /// Feedback outcome authority.
    FeedbackOutcome,
    /// Automation outcome authority.
    AutomationOutcome,
}

impl SourceAuthorityV1 {
    /// Returns the canonical wire value.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::HostSession => "host_session",
            Self::ToolExecution => "tool_execution",
            Self::SourceEdit => "source_edit",
            Self::TestExecution => "test_execution",
            Self::DiagnosticBroker => "diagnostic_broker",
            Self::GitEvidence => "git_evidence",
            Self::NativeFactPromotion => "native_fact_promotion",
            Self::FeedbackOutcome => "feedback_outcome",
            Self::AutomationOutcome => "automation_outcome",
        }
    }

    /// Decodes one canonical wire value.
    pub fn from_wire(value: &str) -> Result<Self, ObservationJournalError> {
        match value {
            "host_session" => Ok(Self::HostSession),
            "tool_execution" => Ok(Self::ToolExecution),
            "source_edit" => Ok(Self::SourceEdit),
            "test_execution" => Ok(Self::TestExecution),
            "diagnostic_broker" => Ok(Self::DiagnosticBroker),
            "git_evidence" => Ok(Self::GitEvidence),
            "native_fact_promotion" => Ok(Self::NativeFactPromotion),
            "feedback_outcome" => Ok(Self::FeedbackOutcome),
            "automation_outcome" => Ok(Self::AutomationOutcome),
            other => Err(ObservationJournalError::UnknownWireValue {
                field: "source_authority",
                value: other.to_owned(),
            }),
        }
    }
}

/// The `(authority, exact scope, stream)` triple that scopes source-sequence
/// monotonicity and the ingress replay cursor.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SourceStreamKeyV1 {
    /// Canonical source authority.
    pub source_authority: SourceAuthorityV1,
    /// Digest of the exact coding scope.
    pub exact_scope_sha256: String,
    /// Stream identity inside that authority and scope.
    pub source_stream: SourceStreamIdV1,
}

impl SourceStreamKeyV1 {
    /// Revalidates the stream key.
    pub fn validate(&self) -> Result<(), ObservationJournalError> {
        require_sha256(&self.exact_scope_sha256, "exact_scope_sha256")
    }
}

/// Proof that the named canonical commit point already settled this source
/// event, carried verbatim from the owning authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalSettlementReceiptV1 {
    /// Authority that settled the event.
    pub source_authority: SourceAuthorityV1,
    /// `canonical_commit_point.point_id` from the host-event observation policy.
    pub commit_point_id: String,
    /// Settled source event identity.
    pub source_event_id: String,
    /// Settled source event revision.
    pub source_event_revision: u64,
    /// Digest of the settled source event.
    pub source_event_sha256: String,
    /// Stream the event belongs to.
    pub source_stream: SourceStreamIdV1,
    /// Position of the event inside that stream.
    pub source_sequence: SourceSequenceV1,
    /// Instant the authority reported settlement.
    pub settled_at_unix_micros: i64,
    /// Digest over the authority's own `receipt_fields` for this commit point.
    pub settlement_proof_sha256: String,
}

impl CanonicalSettlementReceiptV1 {
    /// Rejects a receipt that does not actually prove settlement.
    pub fn validate(&self) -> Result<(), ObservationJournalError> {
        if self.commit_point_id.is_empty() {
            return Err(ObservationJournalError::UnsettledSource {
                field: "commit_point_id",
            });
        }
        if self.source_event_id.is_empty() {
            return Err(ObservationJournalError::UnsettledSource {
                field: "source_event_id",
            });
        }
        require_bounded(
            &self.source_event_id,
            "source_event_id",
            SOURCE_EVENT_ID_MAX_BYTES,
        )?;
        require_bounded(
            &self.commit_point_id,
            "commit_point_id",
            SOURCE_EVENT_ID_MAX_BYTES,
        )?;
        if !crate::identity::is_sha256(&self.source_event_sha256) {
            return Err(ObservationJournalError::UnsettledSource {
                field: "source_event_sha256",
            });
        }
        if !crate::identity::is_sha256(&self.settlement_proof_sha256) {
            return Err(ObservationJournalError::UnsettledSource {
                field: "settlement_proof_sha256",
            });
        }
        Ok(())
    }
}
