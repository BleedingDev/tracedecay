//! Content-derived identities for the observation journal.
//!
//! Everything in this module is deterministic. No identity depends on a clock,
//! a random retry value, or a database row id, which is what makes an
//! idempotency key reproducible across delivery retries, dispatcher restart,
//! provider restart, and transport topology.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracedecay_memory_provider_api::{OwnedOpaqueExtension, observation_extensions_digest};

use crate::error::ObservationJournalError;
use crate::settlement::SourceAuthorityV1;

/// Canonical observation contract identity mixed into every idempotency key.
pub const OBSERVATION_CONTRACT_ID: &str = "tracedecay.memory.provider.observation.v1";

const IDEMPOTENCY_DOMAIN: &[u8] = b"tracedecay.memory-provider.observation-idempotency.v1\0";
const ENVELOPE_DOMAIN: &[u8] = b"tracedecay.memory-provider.observation-envelope.v1\0";
const RECEIPT_ID_DOMAIN: &[u8] = b"tracedecay.memory-provider.observation-receipt.v1\0";
const LEASE_ID_DOMAIN: &[u8] = b"tracedecay.memory-provider.observation-lease.v1\0";
pub(crate) const SANITIZATION_BINDING_DOMAIN: &[u8] =
    b"tracedecay.memory-provider.observation-sanitization-binding.v1\0";

/// Maximum UTF-8 bytes in one source event identity, from the observation
/// contract's `source_event_id_maximum_bytes`.
pub const SOURCE_EVENT_ID_MAX_BYTES: usize = 256;

const HEX: &[u8; 16] = b"0123456789abcdef";

pub(crate) fn lowercase_hex(value: &[u8]) -> String {
    let mut output = String::with_capacity(value.len().saturating_mul(2));
    for byte in value {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

pub(crate) fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn require_sha256(
    value: &str,
    field: &'static str,
) -> Result<(), ObservationJournalError> {
    if is_sha256(value) {
        Ok(())
    } else {
        Err(ObservationJournalError::InvalidDigest { field })
    }
}

pub(crate) fn require_non_empty(
    value: &str,
    field: &'static str,
) -> Result<(), ObservationJournalError> {
    if value.is_empty() {
        Err(ObservationJournalError::EmptyField { field })
    } else {
        Ok(())
    }
}

pub(crate) fn require_bounded(
    value: &str,
    field: &'static str,
    maximum_bytes: usize,
) -> Result<(), ObservationJournalError> {
    require_non_empty(value, field)?;
    if value.len() > maximum_bytes {
        return Err(ObservationJournalError::FieldTooLarge {
            field,
            maximum_bytes,
        });
    }
    Ok(())
}

/// Appends one length-framed field so no two field boundaries can collide.
pub(crate) fn absorb(digest: &mut Sha256, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value);
}

/// Lowercase UUIDv7 observation identity.
///
/// Entropy is injected rather than sampled: the crate stays deterministic under
/// test and free of a `getrandom` dependency. A composition root that feeds a
/// counter instead of real entropy will produce colliding identities and trip
/// the journal's unique index, so callers must supply real randomness.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ObservationIdV1(String);

impl ObservationIdV1 {
    /// Builds a UUIDv7 from a Unix millisecond stamp and ten entropy bytes.
    pub fn from_v7_parts(
        unix_millis: u64,
        entropy: [u8; 10],
    ) -> Result<Self, ObservationJournalError> {
        if unix_millis >= (1u64 << 48) {
            return Err(ObservationJournalError::InvalidObservationId {
                detail: "unix millisecond stamp exceeds 48 bits".to_owned(),
            });
        }
        let mut bytes = [0u8; 16];
        let stamp = unix_millis.to_be_bytes();
        bytes[..6].copy_from_slice(&stamp[2..]);
        bytes[6..].copy_from_slice(&entropy);
        bytes[6] = 0x70 | (bytes[6] & 0x0f);
        bytes[8] = 0x80 | (bytes[8] & 0x3f);
        let hex = lowercase_hex(&bytes);
        let mut value = String::with_capacity(36);
        for (index, character) in hex.chars().enumerate() {
            if matches!(index, 8 | 12 | 16 | 20) {
                value.push('-');
            }
            value.push(character);
        }
        Ok(Self(value))
    }

    /// Parses and revalidates a stored UUIDv7 identity.
    pub fn parse(value: &str) -> Result<Self, ObservationJournalError> {
        let invalid = |detail: &str| ObservationJournalError::InvalidObservationId {
            detail: detail.to_owned(),
        };
        if value.len() != 36 {
            return Err(invalid("uuid must be 36 characters"));
        }
        for (index, character) in value.chars().enumerate() {
            if matches!(index, 8 | 13 | 18 | 23) {
                if character != '-' {
                    return Err(invalid("uuid group separator is missing"));
                }
            } else if !character.is_ascii_digit() && !('a'..='f').contains(&character) {
                return Err(invalid("uuid must be lowercase hexadecimal"));
            }
        }
        let version = value.as_bytes().get(14).copied().unwrap_or(b'0');
        if version != b'7' {
            return Err(invalid("uuid version must be 7"));
        }
        let variant = value.as_bytes().get(19).copied().unwrap_or(b'0');
        if !matches!(variant, b'8' | b'9' | b'a' | b'b') {
            return Err(invalid("uuid variant must be RFC 4122"));
        }
        Ok(Self(value.to_owned()))
    }

    /// Returns the canonical identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The eleven contract inputs an observation idempotency key is derived from.
///
/// The set is closed on purpose: adding a clock, a row id, or a retry counter
/// here would break `stable_across_delivery_retries`.
#[derive(Clone, Copy, Debug)]
pub struct IdempotencyInputV1<'a> {
    /// Observation contract identity.
    pub contract_id: &'a str,
    /// Target provider identity.
    pub provider_id: &'a str,
    /// Pinned provider registration revision.
    pub registration_revision: u64,
    /// Digest of the complete exact coding scope.
    pub exact_scope_sha256: &'a str,
    /// Canonical source authority.
    pub source_authority: SourceAuthorityV1,
    /// Settled source event identity.
    pub source_event_id: &'a str,
    /// Settled source event revision.
    pub source_event_revision: u64,
    /// Observation kind identity.
    pub observation_kind: &'a str,
    /// Payload contract identity.
    pub payload_contract: &'a str,
    /// Digest of the canonical (already sanitized) payload bytes.
    pub payload_sha256: &'a str,
    /// Digest of the canonical extension set.
    pub extensions_digest: &'a str,
}

/// Lowercase 64-hex observation idempotency key.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ObservationIdempotencyKeyV1(String);

impl ObservationIdempotencyKeyV1 {
    /// Derives the contract key over exactly the eleven inputs, in contract
    /// order, each length-framed under one domain constant.
    #[must_use]
    pub fn derive(input: &IdempotencyInputV1<'_>) -> Self {
        let mut digest = Sha256::new();
        digest.update(IDEMPOTENCY_DOMAIN);
        absorb(&mut digest, input.contract_id.as_bytes());
        absorb(&mut digest, input.provider_id.as_bytes());
        absorb(&mut digest, &input.registration_revision.to_be_bytes());
        absorb(&mut digest, input.exact_scope_sha256.as_bytes());
        absorb(&mut digest, input.source_authority.as_wire().as_bytes());
        absorb(&mut digest, input.source_event_id.as_bytes());
        absorb(&mut digest, &input.source_event_revision.to_be_bytes());
        absorb(&mut digest, input.observation_kind.as_bytes());
        absorb(&mut digest, input.payload_contract.as_bytes());
        absorb(&mut digest, input.payload_sha256.as_bytes());
        absorb(&mut digest, input.extensions_digest.as_bytes());
        Self(lowercase_hex(&digest.finalize()))
    }

    /// Parses a stored key.
    pub fn parse(value: &str) -> Result<Self, ObservationJournalError> {
        require_sha256(value, "idempotency_key")?;
        Ok(Self(value.to_owned()))
    }

    /// Returns the canonical key.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Digest over the canonical extension set carried by one observation.
///
/// Extensions must already be in ascending, duplicate-free `extension_id`
/// order; otherwise two callers carrying the same set would derive different
/// idempotency keys.
///
/// This reuses the same `observation_extensions_digest` boundary the dispatch
/// path (`ProviderCall::validate`) enforces — at most 32 extensions, at most
/// 256 KiB each, at most 512 KiB in aggregate. Admission must reject an
/// oversized extension set with the same bound dispatch would apply; a set
/// that only `opaque_extensions_digest` would accept could otherwise be
/// durably queued and then fail every dispatch attempt forever.
pub fn extensions_digest(
    extensions: &[OwnedOpaqueExtension],
) -> Result<String, ObservationJournalError> {
    observation_extensions_digest(extensions).map_err(ObservationJournalError::from)
}

/// Positional identity of one settled event inside its source stream.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SourceSequenceV1(pub u64);

/// Stream identity that scopes source-sequence monotonicity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SourceStreamIdV1(String);

impl SourceStreamIdV1 {
    /// Validates and owns one source stream identity.
    pub fn new(value: impl Into<String>) -> Result<Self, ObservationJournalError> {
        let value = value.into();
        require_bounded(&value, "source_stream", SOURCE_EVENT_ID_MAX_BYTES)?;
        Ok(Self(value))
    }

    /// Returns the stream identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Privacy deletion key. Every admitted observation names one, so a forget
/// request always has an exact, indexable target.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ForgetSourceKeyV1(String);

impl ForgetSourceKeyV1 {
    /// Validates and owns one forget-source key.
    pub fn new(value: impl Into<String>) -> Result<Self, ObservationJournalError> {
        let value = value.into();
        require_bounded(&value, "forget_source_key", SOURCE_EVENT_ID_MAX_BYTES)?;
        Ok(Self(value))
    }

    /// Returns the forget-source key.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Identity of one dispatch lease over a pending delivery row.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DispatchLeaseIdV1(String);

impl DispatchLeaseIdV1 {
    /// Derives a deterministic lease identity from the leased row, its owner,
    /// and the lease instant, so a lease is reproducible in a crash trace.
    #[must_use]
    pub fn derive(
        idempotency_key: &ObservationIdempotencyKeyV1,
        lease_owner: &str,
        leased_at_unix_micros: i64,
        attempt_number: u32,
    ) -> Self {
        let mut digest = Sha256::new();
        digest.update(LEASE_ID_DOMAIN);
        absorb(&mut digest, idempotency_key.as_str().as_bytes());
        absorb(&mut digest, lease_owner.as_bytes());
        absorb(&mut digest, &leased_at_unix_micros.to_be_bytes());
        absorb(&mut digest, &u64::from(attempt_number).to_be_bytes());
        Self(lowercase_hex(&digest.finalize()))
    }

    /// Parses a stored lease identity.
    pub fn parse(value: &str) -> Result<Self, ObservationJournalError> {
        require_sha256(value, "lease_id")?;
        Ok(Self(value.to_owned()))
    }

    /// Returns the lease identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Identity of one immutable delivery receipt.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DeliveryReceiptIdV1(String);

impl DeliveryReceiptIdV1 {
    /// Derives the receipt identity from the attempt it describes. Two
    /// dispatchers racing the same attempt therefore derive the same id and
    /// collide on the receipt primary key instead of writing two histories.
    #[must_use]
    pub fn derive(observation_id: &ObservationIdV1, attempt_number: u32) -> Self {
        let mut digest = Sha256::new();
        digest.update(RECEIPT_ID_DOMAIN);
        absorb(&mut digest, observation_id.as_str().as_bytes());
        absorb(&mut digest, &u64::from(attempt_number).to_be_bytes());
        Self(lowercase_hex(&digest.finalize()))
    }

    /// Parses a stored receipt identity.
    pub fn parse(value: &str) -> Result<Self, ObservationJournalError> {
        require_sha256(value, "receipt_id")?;
        Ok(Self(value.to_owned()))
    }

    /// Returns the receipt identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Digest binding every immutable field of one admitted envelope.
#[allow(clippy::too_many_arguments)]
#[must_use]
pub(crate) fn envelope_digest(
    observation_id: &str,
    idempotency_key: &str,
    provider_id: &str,
    provider_instance_id: &str,
    registration_revision: u64,
    ready_receipt_digest: &str,
    exact_scope_sha256: &str,
    source_event_sha256: &str,
    source_sequence: u64,
    observation_kind: &str,
    payload_contract: &str,
    payload_sha256: &str,
    extensions_digest: &str,
    provenance_sha256: &str,
    privacy_digest: &str,
    sanitization_binding_digest: &str,
    occurred_at_unix_micros: i64,
    admitted_at_unix_micros: i64,
    deadline_unix_micros: i64,
    request_id: &str,
) -> String {
    let mut digest = Sha256::new();
    digest.update(ENVELOPE_DOMAIN);
    absorb(&mut digest, observation_id.as_bytes());
    absorb(&mut digest, idempotency_key.as_bytes());
    absorb(&mut digest, provider_id.as_bytes());
    absorb(&mut digest, provider_instance_id.as_bytes());
    absorb(&mut digest, &registration_revision.to_be_bytes());
    absorb(&mut digest, ready_receipt_digest.as_bytes());
    absorb(&mut digest, exact_scope_sha256.as_bytes());
    absorb(&mut digest, source_event_sha256.as_bytes());
    absorb(&mut digest, &source_sequence.to_be_bytes());
    absorb(&mut digest, observation_kind.as_bytes());
    absorb(&mut digest, payload_contract.as_bytes());
    absorb(&mut digest, payload_sha256.as_bytes());
    absorb(&mut digest, extensions_digest.as_bytes());
    absorb(&mut digest, provenance_sha256.as_bytes());
    absorb(&mut digest, privacy_digest.as_bytes());
    absorb(&mut digest, sanitization_binding_digest.as_bytes());
    absorb(&mut digest, &occurred_at_unix_micros.to_be_bytes());
    absorb(&mut digest, &admitted_at_unix_micros.to_be_bytes());
    absorb(&mut digest, &deadline_unix_micros.to_be_bytes());
    absorb(&mut digest, request_id.as_bytes());
    lowercase_hex(&digest.finalize())
}
