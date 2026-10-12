//! The immutable admitted observation envelope the journal stores.
//!
//! Admission order is fixed by
//! `product/contracts/memory-provider-v1/provider-observation-contract.json`:
//! sanitize, then canonicalize and derive digests, then append, then dispatch.
//! Everything reaching this module is therefore already sanitized — the bytes
//! in [`AdmittedObservationV1::payload`] are exactly the bytes that will be
//! delivered, and no pre-sanitization payload ever reaches the journal.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracedecay_memory_provider_api::{
    CanonicalPayload, OwnedExactScope, OwnedOpaqueExtension, OwnedProviderId, OwnedVersionedId,
    PayloadSanitizationReceipt, WithheldReason, derive_withheld_receipt_id,
};

use crate::error::ObservationJournalError;
use crate::identity::{
    ForgetSourceKeyV1, IdempotencyInputV1, OBSERVATION_CONTRACT_ID, ObservationIdV1,
    ObservationIdempotencyKeyV1, SANITIZATION_BINDING_DOMAIN, SOURCE_EVENT_ID_MAX_BYTES,
    SourceSequenceV1, absorb, envelope_digest, extensions_digest, lowercase_hex, require_bounded,
    require_sha256,
};
use crate::settlement::{CanonicalSettlementReceiptV1, SourceAuthorityV1, SourceStreamKeyV1};

/// Maximum canonical payload bytes, from the observation contract.
pub const MAX_PAYLOAD_BYTES: usize = 3_145_728;

/// Maximum UTF-8 bytes of one serialized hygiene receipt the journal will hold.
///
/// The receipt is a flat object of digests and counts, so this is generous by
/// two orders of magnitude; it exists so an unbounded blob cannot ride into the
/// journal on the hygiene seam.
pub const MAX_SANITIZATION_RECEIPT_JSON_BYTES: usize = 4_096;

/// The pinned provider registration one observation is addressed to.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderTargetV1 {
    /// Logical provider identity.
    pub provider_id: OwnedProviderId,
    /// Concrete provider instance identity.
    pub provider_instance_id: String,
    /// Pinned registration revision.
    pub registration_revision: u64,
    /// Digest of the compatible readiness receipt.
    pub ready_receipt_digest: String,
}

impl ProviderTargetV1 {
    /// Revalidates the provider target.
    pub fn validate(&self) -> Result<(), ObservationJournalError> {
        require_bounded(
            &self.provider_instance_id,
            "provider_instance_id",
            SOURCE_EVENT_ID_MAX_BYTES,
        )?;
        if self.registration_revision == 0 {
            return Err(ObservationJournalError::ValueOutOfRange {
                field: "registration_revision",
            });
        }
        require_sha256(&self.ready_receipt_digest, "ready_receipt_digest")
    }
}

/// Privacy classification of admitted content.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrivacyClassificationV1 {
    /// Public content.
    Public,
    /// Internal content.
    Internal,
    /// Sensitive content.
    Sensitive,
    /// Restricted content.
    Restricted,
}

impl PrivacyClassificationV1 {
    /// Returns the canonical wire value.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Internal => "internal",
            Self::Sensitive => "sensitive",
            Self::Restricted => "restricted",
        }
    }

    /// Decodes one canonical wire value.
    pub fn from_wire(value: &str) -> Result<Self, ObservationJournalError> {
        match value {
            "public" => Ok(Self::Public),
            "internal" => Ok(Self::Internal),
            "sensitive" => Ok(Self::Sensitive),
            "restricted" => Ok(Self::Restricted),
            other => Err(ObservationJournalError::UnknownWireValue {
                field: "privacy_classification",
                value: other.to_owned(),
            }),
        }
    }
}

/// Retention class that bounds how long admitted content may sit at rest.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetentionClassV1 {
    /// Shortest-lived content.
    Ephemeral,
    /// Session-scoped content.
    Session,
    /// Project-scoped content.
    Project,
    /// Profile-scoped content.
    Profile,
}

impl RetentionClassV1 {
    /// Returns the canonical wire value.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Ephemeral => "ephemeral",
            Self::Session => "session",
            Self::Project => "project",
            Self::Profile => "profile",
        }
    }

    /// Decodes one canonical wire value.
    pub fn from_wire(value: &str) -> Result<Self, ObservationJournalError> {
        match value {
            "ephemeral" => Ok(Self::Ephemeral),
            "session" => Ok(Self::Session),
            "project" => Ok(Self::Project),
            "profile" => Ok(Self::Profile),
            other => Err(ObservationJournalError::UnknownWireValue {
                field: "retention_class",
                value: other.to_owned(),
            }),
        }
    }
}

/// Origin of the admitted content.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProvenanceOriginV1 {
    /// A human user.
    User,
    /// A coding agent.
    Agent,
    /// A tool invocation.
    Tool,
    /// The repository itself.
    Repository,
    /// TraceDecay Native authority.
    TracedecayNative,
    /// TraceDecay automation.
    Automation,
}

impl ProvenanceOriginV1 {
    /// Returns the canonical wire value.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Agent => "agent",
            Self::Tool => "tool",
            Self::Repository => "repository",
            Self::TracedecayNative => "tracedecay_native",
            Self::Automation => "automation",
        }
    }

    /// Decodes one canonical wire value.
    pub fn from_wire(value: &str) -> Result<Self, ObservationJournalError> {
        match value {
            "user" => Ok(Self::User),
            "agent" => Ok(Self::Agent),
            "tool" => Ok(Self::Tool),
            "repository" => Ok(Self::Repository),
            "tracedecay_native" => Ok(Self::TracedecayNative),
            "automation" => Ok(Self::Automation),
            other => Err(ObservationJournalError::UnknownWireValue {
                field: "provenance_origin",
                value: other.to_owned(),
            }),
        }
    }
}

/// Complete privacy metadata admitted before dispatch. A provider can never
/// extend any of it: nothing in the journal ever reads an expiry from a receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservationPrivacyV1 {
    /// Content classification.
    pub classification: PrivacyClassificationV1,
    /// Retention class.
    pub retention_class: RetentionClassV1,
    /// Revision of the redaction rules applied before admission.
    pub redaction_revision: u32,
    /// Revision of the content policy applied before admission.
    pub content_policy_revision: u32,
    /// Privacy deletion key.
    pub forget_source_key: ForgetSourceKeyV1,
    /// Admitted expiry instant.
    pub expires_at_unix_micros: i64,
}

impl ObservationPrivacyV1 {
    /// Revalidates the privacy metadata.
    pub fn validate(&self) -> Result<(), ObservationJournalError> {
        require_bounded(
            self.forget_source_key.as_str(),
            "forget_source_key",
            SOURCE_EVENT_ID_MAX_BYTES,
        )
    }

    pub(crate) fn digest(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"tracedecay.memory-provider.observation-privacy.v1\0");
        absorb(&mut digest, self.classification.as_wire().as_bytes());
        absorb(&mut digest, self.retention_class.as_wire().as_bytes());
        absorb(
            &mut digest,
            &u64::from(self.redaction_revision).to_be_bytes(),
        );
        absorb(
            &mut digest,
            &u64::from(self.content_policy_revision).to_be_bytes(),
        );
        absorb(&mut digest, self.forget_source_key.as_str().as_bytes());
        absorb(&mut digest, &self.expires_at_unix_micros.to_be_bytes());
        lowercase_hex(&digest.finalize())
    }
}

/// Mandatory binding to the payload hygiene receipt produced at admission.
///
/// Every admitted observation carries one. The journal stores `receipt_json`
/// verbatim so a restarted dispatcher re-attaches the exact minted receipt, but
/// it does **not** take the seam on trust: [`SanitizationBindingV1::validate`]
/// reparses the receipt with
/// [`PayloadSanitizationReceipt::from_json`], which revalidates the receipt's
/// own derived identifier, and then proves the receipt describes *these* bytes.
/// A binding whose columns were edited, whose receipt was re-pointed at other
/// content, or whose sanitized digest is not the payload digest is refused.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SanitizationBindingV1 {
    /// Derived hygiene receipt identity.
    pub receipt_id: String,
    /// Versioned identity of the sanitizer rule corpus that produced it.
    pub sanitizer_revision: String,
    /// Digest of the payload *before* sanitization. A digest only — the
    /// pre-sanitization bytes themselves never reach the journal.
    pub source_payload_sha256: String,
    /// Serialized hygiene receipt, stored and returned byte-for-byte.
    pub receipt_json: String,
}

impl SanitizationBindingV1 {
    /// Revalidates the seam *and* the receipt it names against the sanitized
    /// payload digest the journal holds.
    ///
    /// `payload_sha256` is the digest of the canonical bytes that will actually
    /// be delivered. The receipt's `sanitized_payload_sha256` must equal it, so
    /// a receipt minted for other content cannot be attached to this payload.
    pub fn validate(
        &self,
        payload_sha256: &str,
        extensions_digest: &str,
    ) -> Result<(), ObservationJournalError> {
        require_bounded(
            &self.receipt_id,
            "sanitization_receipt_id",
            SOURCE_EVENT_ID_MAX_BYTES,
        )?;
        require_bounded(
            &self.sanitizer_revision,
            "sanitizer_revision",
            SOURCE_EVENT_ID_MAX_BYTES,
        )?;
        require_sha256(&self.source_payload_sha256, "source_payload_sha256")?;
        require_bounded(
            &self.receipt_json,
            "sanitization_receipt_json",
            MAX_SANITIZATION_RECEIPT_JSON_BYTES,
        )?;

        // Strict reparse: the receipt re-derives its own identifier, so this
        // rejects a receipt whose fields were edited after minting.
        let receipt = PayloadSanitizationReceipt::from_json(&self.receipt_json)?;
        if receipt.receipt_id() != self.receipt_id {
            return Err(ObservationJournalError::ReceiptDigestMismatch {
                field: "sanitization_receipt_id",
            });
        }
        if receipt.sanitizer_revision() != self.sanitizer_revision {
            return Err(ObservationJournalError::ReceiptDigestMismatch {
                field: "sanitizer_revision",
            });
        }
        if receipt.source_payload_sha256() != self.source_payload_sha256 {
            return Err(ObservationJournalError::ReceiptDigestMismatch {
                field: "sanitization_source_payload_sha256",
            });
        }
        // The sanitized digest must be the digest of the bytes about to be
        // delivered; otherwise the receipt proves hygiene for other content.
        receipt.verify_binding(payload_sha256, extensions_digest)?;
        Ok(())
    }

    /// Digest over the whole binding, folded into the envelope digest so the
    /// hygiene evidence cannot be swapped without breaking `envelope_sha256`.
    #[must_use]
    pub fn digest(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(SANITIZATION_BINDING_DOMAIN);
        absorb(&mut digest, self.receipt_id.as_bytes());
        absorb(&mut digest, self.sanitizer_revision.as_bytes());
        absorb(&mut digest, self.source_payload_sha256.as_bytes());
        absorb(&mut digest, self.receipt_json.as_bytes());
        lowercase_hex(&digest.finalize())
    }
}

/// One immutable admitted observation, addressed to exactly one provider
/// registration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmittedObservationV1 {
    /// UUIDv7 observation identity.
    pub observation_id: ObservationIdV1,
    /// Content-derived idempotency key.
    pub idempotency_key: ObservationIdempotencyKeyV1,
    /// Pinned provider registration.
    pub target: ProviderTargetV1,
    /// Exact coding scope.
    pub exact_scope: OwnedExactScope,
    /// Proof the source already settled.
    pub source: CanonicalSettlementReceiptV1,
    /// Observation kind identity.
    pub observation_kind: OwnedVersionedId,
    /// Sanitized canonical payload, digest-bound. These are the bytes that will
    /// be delivered.
    pub payload: CanonicalPayload,
    /// Canonical extension set, ascending by extension id.
    pub extensions: Vec<OwnedOpaqueExtension>,
    /// Digest over the extension set.
    pub extensions_digest: String,
    /// Origin of the content.
    pub provenance_origin: ProvenanceOriginV1,
    /// Digest over the full provenance record held by the admitting authority.
    pub provenance_sha256: String,
    /// Admitted privacy metadata.
    pub privacy: ObservationPrivacyV1,
    /// Mandatory hygiene binding for the payload being admitted.
    pub sanitization: SanitizationBindingV1,
    /// Instant the source event occurred.
    pub occurred_at_unix_micros: i64,
    /// Instant the observation was admitted.
    pub admitted_at_unix_micros: i64,
    /// Instant after which delivery must stop.
    pub deadline_unix_micros: i64,
    /// Request identity that carried the admission.
    pub request_id: String,
    /// Digest over every immutable envelope field.
    pub envelope_sha256: String,
}

impl AdmittedObservationV1 {
    /// Revalidates every bound the journal relies on.
    ///
    /// Critically, the idempotency key is re-derived from the eleven contract
    /// inputs and any mismatch is refused, so a caller cannot smuggle in a
    /// random or timestamp-derived key.
    pub fn validate(&self) -> Result<(), ObservationJournalError> {
        self.target.validate()?;
        self.exact_scope.validate()?;
        self.source.validate()?;
        self.payload.validate()?;
        self.privacy.validate()?;
        let derived_extensions = extensions_digest(&self.extensions)?;
        if derived_extensions != self.extensions_digest {
            return Err(ObservationJournalError::ExtensionsDigestMismatch);
        }
        // Hygiene is not optional: an envelope with no proven binding to the
        // payload and extension bytes it carries cannot be admitted at all.
        self.sanitization
            .validate(&self.payload.sha256, &self.extensions_digest)?;
        if self.payload.bytes.len() > MAX_PAYLOAD_BYTES {
            return Err(ObservationJournalError::FieldTooLarge {
                field: "canonical_payload",
                maximum_bytes: MAX_PAYLOAD_BYTES,
            });
        }
        require_bounded(&self.request_id, "request_id", SOURCE_EVENT_ID_MAX_BYTES)?;
        require_sha256(&self.provenance_sha256, "provenance_sha256")?;

        if self.deadline_unix_micros <= self.admitted_at_unix_micros {
            return Err(ObservationJournalError::DeadlineBeforeAdmission {
                deadline_unix_micros: self.deadline_unix_micros,
                admitted_at_unix_micros: self.admitted_at_unix_micros,
            });
        }

        let derived_key = self.derive_idempotency_key();
        if derived_key != self.idempotency_key {
            return Err(ObservationJournalError::IdempotencyKeyMismatch {
                expected: derived_key.as_str().to_owned(),
                provided: self.idempotency_key.as_str().to_owned(),
            });
        }
        if self.expected_envelope_sha256() != self.envelope_sha256 {
            return Err(ObservationJournalError::EnvelopeDigestMismatch);
        }
        Ok(())
    }

    /// Re-derives this envelope's idempotency key from the eleven contract
    /// inputs it carries.
    #[must_use]
    pub fn derive_idempotency_key(&self) -> ObservationIdempotencyKeyV1 {
        let exact_scope_sha256 = self.exact_scope.exact_scope_sha256();
        ObservationIdempotencyKeyV1::derive(&IdempotencyInputV1 {
            contract_id: OBSERVATION_CONTRACT_ID,
            provider_id: self.target.provider_id.as_str(),
            registration_revision: self.target.registration_revision,
            exact_scope_sha256: &exact_scope_sha256,
            source_authority: self.source.source_authority,
            source_event_id: &self.source.source_event_id,
            source_event_revision: self.source.source_event_revision,
            observation_kind: self.observation_kind.as_str(),
            payload_contract: self.payload.contract_id.as_str(),
            payload_sha256: &self.payload.sha256,
            extensions_digest: &self.extensions_digest,
        })
    }

    /// Returns the digest of the complete exact coding scope.
    #[must_use]
    pub fn exact_scope_sha256(&self) -> String {
        self.exact_scope.exact_scope_sha256()
    }

    /// Returns the stream key that scopes source-sequence monotonicity.
    #[must_use]
    pub fn stream_key(&self) -> SourceStreamKeyV1 {
        SourceStreamKeyV1 {
            source_authority: self.source.source_authority,
            exact_scope_sha256: self.exact_scope_sha256(),
            source_stream: self.source.source_stream.clone(),
        }
    }

    /// Returns the position of this observation inside its source stream.
    #[must_use]
    pub const fn source_sequence(&self) -> SourceSequenceV1 {
        self.source.source_sequence
    }

    /// Returns the queue weight of this observation in bytes.
    #[must_use]
    pub fn queue_bytes(&self) -> u64 {
        let extensions: usize = self
            .extensions
            .iter()
            .map(|extension| extension.canonical_payload.len())
            .sum();
        u64::try_from(self.payload.bytes.len().saturating_add(extensions)).unwrap_or(u64::MAX)
    }

    /// Recomputes the digest that binds every immutable envelope field. An
    /// admission pipeline stamps this into `envelope_sha256`; `validate()`
    /// refuses any envelope whose stored digest disagrees.
    #[must_use]
    pub fn expected_envelope_sha256(&self) -> String {
        envelope_digest(
            self.observation_id.as_str(),
            self.idempotency_key.as_str(),
            self.target.provider_id.as_str(),
            &self.target.provider_instance_id,
            self.target.registration_revision,
            &self.target.ready_receipt_digest,
            &self.exact_scope_sha256(),
            &self.source.source_event_sha256,
            self.source.source_sequence.0,
            self.observation_kind.as_str(),
            self.payload.contract_id.as_str(),
            &self.payload.sha256,
            &self.extensions_digest,
            &self.provenance_sha256,
            &self.privacy.digest(),
            &self.sanitization.digest(),
            self.occurred_at_unix_micros,
            self.admitted_at_unix_micros,
            self.deadline_unix_micros,
            &self.request_id,
        )
    }
}

/// A settled source event that hygiene refused to admit.
///
/// No payload and no delivery row is ever created for it. The journal records
/// digests plus a typed reason and advances the ingress replay cursor past the
/// withheld sequence, so a secret-bearing event is not re-emitted forever.
///
/// A withheld record is subject to the same privacy lifecycle as an admitted
/// one: it names a [`ForgetSourceKeyV1`], so a deletion request reaches it and
/// a retention sweep ages it out.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WithheldAdmissionV1 {
    /// Canonical source authority wire value.
    pub source_authority: String,
    /// Digest of the exact coding scope.
    pub exact_scope_sha256: String,
    /// Stream the event belongs to.
    pub source_stream: String,
    /// Position of the event inside that stream.
    pub source_sequence: u64,
    /// Settled source event identity.
    pub source_event_id: String,
    /// Settled source event revision, carried as the hygiene lane emits it.
    pub source_event_revision: String,
    /// Hygiene receipt identity.
    pub receipt_id: String,
    /// Typed withholding reason.
    pub reason: String,
    /// Digest of the payload that was refused. A digest only.
    pub source_payload_sha256: String,
    /// Digest of the exact ordered extension set that was inspected.
    pub extensions_digest: String,
    /// Sanitizer and policy revision that made the decision.
    pub sanitizer_revision: String,
    /// Number of canonical findings supporting the decision.
    pub finding_count: u32,
    /// Digest over the canonical findings supporting the decision.
    pub findings_digest: String,
    /// Privacy deletion key this record answers to, so a forget request and a
    /// retention sweep both reach the withheld audit.
    pub forget_source_key: ForgetSourceKeyV1,
}

impl WithheldAdmissionV1 {
    /// Revalidates the withheld record.
    pub fn validate(&self) -> Result<(), ObservationJournalError> {
        SourceAuthorityV1::from_wire(&self.source_authority)?;
        require_sha256(&self.exact_scope_sha256, "exact_scope_sha256")?;
        require_bounded(
            &self.source_stream,
            "source_stream",
            SOURCE_EVENT_ID_MAX_BYTES,
        )?;
        require_bounded(
            &self.source_event_id,
            "source_event_id",
            SOURCE_EVENT_ID_MAX_BYTES,
        )?;
        require_bounded(
            &self.source_event_revision,
            "source_event_revision",
            SOURCE_EVENT_ID_MAX_BYTES,
        )?;
        require_bounded(&self.receipt_id, "receipt_id", SOURCE_EVENT_ID_MAX_BYTES)?;
        require_bounded(&self.reason, "reason", SOURCE_EVENT_ID_MAX_BYTES)?;
        require_sha256(&self.source_payload_sha256, "source_payload_sha256")?;
        require_sha256(&self.extensions_digest, "extensions_digest")?;
        require_bounded(
            &self.sanitizer_revision,
            "sanitizer_revision",
            SOURCE_EVENT_ID_MAX_BYTES,
        )?;
        require_sha256(&self.findings_digest, "sanitization_findings_digest")?;
        let reason = WithheldReason::from_wire(&self.reason).ok_or_else(|| {
            ObservationJournalError::UnknownWireValue {
                field: "withheld_reason",
                value: self.reason.clone(),
            }
        })?;
        let expected_receipt_id = derive_withheld_receipt_id(
            &self.sanitizer_revision,
            &self.source_payload_sha256,
            &self.extensions_digest,
            reason,
            self.finding_count,
            &self.findings_digest,
        );
        if expected_receipt_id != self.receipt_id {
            return Err(ObservationJournalError::ReceiptDigestMismatch {
                field: "withheld_receipt_id",
            });
        }
        require_bounded(
            self.forget_source_key.as_str(),
            "forget_source_key",
            SOURCE_EVENT_ID_MAX_BYTES,
        )
    }

    /// Returns the stream key whose replay cursor this record advances.
    pub fn stream_key(&self) -> Result<SourceStreamKeyV1, ObservationJournalError> {
        Ok(SourceStreamKeyV1 {
            source_authority: SourceAuthorityV1::from_wire(&self.source_authority)?,
            exact_scope_sha256: self.exact_scope_sha256.clone(),
            source_stream: crate::identity::SourceStreamIdV1::new(self.source_stream.clone())?,
        })
    }
}
