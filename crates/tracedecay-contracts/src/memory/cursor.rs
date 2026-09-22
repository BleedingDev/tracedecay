//! Authenticated continuation cursors for retained fact reads.
//
// The public cursor is an opaque, bounded identifier. Its encrypted payload
// contains only request binding digests and the private store position; the
// query, scope, owner, score, timestamp, and fact id never occur in the
// transport value in cleartext. Key material is supplied by the durable
// session cursor provider so a restart or replica can verify a retained
// cursor. This module deliberately does not depend on that provider: the
// contract boundary owns only the keyring and authenticated envelope.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tracedecay_domain::{FactId, UtcMicros};

use crate::retained_surfaces::{MemoryScopeV1, RetainedProjectSelectorV1};

use super::{FactCategoryV1, FactCommitOwnerV1, FactListCursorV1, FactSearchCursorV1};

/// Version marker for the encrypted fact cursor envelope.
pub const FACT_CURSOR_FORMAT_VERSION_V2: &str = "fsc2";
/// Maximum encoded cursor size accepted by the public identifier contract.
pub const MAX_FACT_CURSOR_BYTES_V2: usize = 4_096;
/// Maximum query text accepted by a fact read. The query is hashed into a
/// cursor binding, so this limit does not affect cursor size.
pub const MAX_FACT_CURSOR_QUERY_BYTES_V2: usize = 4_096;
/// Maximum number of entities that can participate in a reason binding.
pub const MAX_FACT_CURSOR_ENTITY_COUNT_V2: usize = 200;
/// Stable ranking revision used by paged retained fact reads.
pub const FACT_CURSOR_RANKING_REVISION_V2: &str = "fact-memory-ranking.v2.stable";
/// Default lifetime for a retained fact continuation.
pub const FACT_CURSOR_TTL_MICROS_V2: i64 = 15 * 60 * 1_000_000;

const CURSOR_PAYLOAD_VERSION: u8 = 2;
const CURSOR_NONCE_BYTES: usize = 12;
const CURSOR_EPOCH_HEX_BYTES: usize = 16;
const CURSOR_NONCE_HEX_BYTES: usize = CURSOR_NONCE_BYTES * 2;
const CURSOR_AEAD_TAG_BYTES: usize = 16;
// Keep enough room for fixed-size binding digests plus the domain's maximum
// fact identifier. The encoded envelope remains below the public 4 KiB
// bound after AEAD and hexadecimal framing overhead.
const MAX_CURSOR_PLAINTEXT_BYTES: usize = 1_984;
const MAX_CURSOR_CIPHERTEXT_BYTES: usize = MAX_CURSOR_PLAINTEXT_BYTES + CURSOR_AEAD_TAG_BYTES;
const CURSOR_AAD_DOMAIN: &[u8] = b"tracedecay.fact-cursor.aead.v2";

/// Typed rejection from the fact cursor boundary.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum FactCursorErrorV1 {
    #[error("fact cursor is malformed")]
    Malformed,
    #[error("fact cursor authentication failed")]
    Tampered,
    #[error("fact cursor belongs to a different request")]
    RequestMismatch,
    #[error("fact cursor belongs to a different scope")]
    ScopeMismatch,
    #[error("fact cursor query or filter changed")]
    QueryMismatch,
    #[error("fact cursor kind does not match this read")]
    KindMismatch,
    #[error("fact cursor position is invalid")]
    PositionMismatch,
    #[error("fact cursor encryption key is unavailable")]
    KeyUnavailable,
    #[error("fact cursor encryption key was revoked")]
    KeyRevoked,
    #[error("fact cursor has expired")]
    Expired,
    #[error("fact cursor profile binding does not match")]
    ProfileMismatch,
    #[error("fact cursor nonce could not be generated")]
    NonceUnavailable,
}

impl FactCursorErrorV1 {
    /// Stable diagnostic code used by adapters that collapse cursor failures
    /// into their existing invalid-request transport class.
    pub const fn code(self) -> &'static str {
        match self {
            Self::Malformed => "fact_cursor_malformed",
            Self::Tampered => "fact_cursor_tampered",
            Self::RequestMismatch => "fact_cursor_request_mismatch",
            Self::ScopeMismatch => "fact_cursor_scope_mismatch",
            Self::QueryMismatch => "fact_cursor_query_mismatch",
            Self::KindMismatch => "fact_cursor_kind_mismatch",
            Self::PositionMismatch => "fact_cursor_position_mismatch",
            Self::KeyUnavailable => "fact_cursor_key_unavailable",
            Self::KeyRevoked => "fact_cursor_key_revoked",
            Self::Expired => "fact_cursor_expired",
            Self::ProfileMismatch => "fact_cursor_profile_mismatch",
            Self::NonceUnavailable => "fact_cursor_nonce_unavailable",
        }
    }
}

#[derive(Clone)]
struct FactCursorKeyMaterialV1 {
    key: Arc<LessSafeKey>,
    revoked: bool,
}

/// Durable-key-backed AEAD keyring for fact cursors.
///
/// The session temporal store constructs this value from its persisted active
/// and retained key rows. A caller may retain old keys across rotation and
/// revoke a compromised epoch. The profile binding is included in the AEAD
/// associated data and must be the same on every replica serving one profile.
#[derive(Clone)]
pub struct FactCursorKeyringV1 {
    profile_binding: [u8; 32],
    active_epoch: u64,
    keys: BTreeMap<u64, FactCursorKeyMaterialV1>,
    cursor_ttl_micros: i64,
}

impl fmt::Debug for FactCursorKeyringV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FactCursorKeyringV1")
            .field("profile_binding", &hex::encode(self.profile_binding))
            .field("active_epoch", &self.active_epoch)
            .field("retained_key_count", &self.keys.len())
            .field("cursor_ttl_micros", &self.cursor_ttl_micros)
            .field("key_material", &"REDACTED")
            .finish()
    }
}

impl FactCursorKeyringV1 {
    /// Construct a keyring from a persisted profile-derived AES-256 secret.
    pub fn new(
        profile_binding: [u8; 32],
        active_epoch: u64,
        secret: impl AsRef<[u8]>,
        cursor_ttl_micros: i64,
    ) -> Result<Self, FactCursorErrorV1> {
        if active_epoch == 0 || cursor_ttl_micros <= 0 {
            return Err(FactCursorErrorV1::KeyUnavailable);
        }
        let mut keyring = Self {
            profile_binding,
            active_epoch,
            keys: BTreeMap::new(),
            cursor_ttl_micros,
        };
        keyring.retain(active_epoch, secret)?;
        Ok(keyring)
    }

    /// Alias that makes the persistence boundary explicit at provider call
    /// sites.
    pub fn from_secret_bytes(
        profile_binding: [u8; 32],
        active_epoch: u64,
        secret: impl AsRef<[u8]>,
        cursor_ttl_micros: i64,
    ) -> Result<Self, FactCursorErrorV1> {
        Self::new(profile_binding, active_epoch, secret, cursor_ttl_micros)
    }

    /// Retain a non-active key for verification after rotation.
    pub fn retain(
        &mut self,
        epoch: u64,
        secret: impl AsRef<[u8]>,
    ) -> Result<(), FactCursorErrorV1> {
        if epoch == 0 || self.keys.contains_key(&epoch) {
            return Err(FactCursorErrorV1::KeyUnavailable);
        }
        let secret = secret.as_ref();
        if secret.len() != AES_256_GCM.key_len() {
            return Err(FactCursorErrorV1::KeyUnavailable);
        }
        let unbound =
            UnboundKey::new(&AES_256_GCM, secret).map_err(|_| FactCursorErrorV1::KeyUnavailable)?;
        self.keys.insert(
            epoch,
            FactCursorKeyMaterialV1 {
                key: Arc::new(LessSafeKey::new(unbound)),
                revoked: false,
            },
        );
        Ok(())
    }

    /// Make a new persisted key epoch active while retaining prior keys.
    pub fn rotate(
        &mut self,
        epoch: u64,
        secret: impl AsRef<[u8]>,
    ) -> Result<(), FactCursorErrorV1> {
        if epoch <= self.active_epoch {
            return Err(FactCursorErrorV1::KeyUnavailable);
        }
        self.retain(epoch, secret)?;
        self.active_epoch = epoch;
        Ok(())
    }

    /// Revoke one retained epoch. Existing cursors under that epoch fail with
    /// KeyRevoked, even if the key is still present for audit retention.
    pub fn revoke(&mut self, epoch: u64) -> Result<(), FactCursorErrorV1> {
        let key = self
            .keys
            .get_mut(&epoch)
            .ok_or(FactCursorErrorV1::KeyUnavailable)?;
        key.revoked = true;
        Ok(())
    }

    #[must_use]
    pub const fn profile_binding(&self) -> [u8; 32] {
        self.profile_binding
    }

    #[must_use]
    pub const fn active_epoch(&self) -> u64 {
        self.active_epoch
    }

    #[must_use]
    pub const fn cursor_ttl_micros(&self) -> i64 {
        self.cursor_ttl_micros
    }

    fn key(&self, epoch: u64) -> Result<&FactCursorKeyMaterialV1, FactCursorErrorV1> {
        let material = self
            .keys
            .get(&epoch)
            .ok_or(FactCursorErrorV1::KeyUnavailable)?;
        if material.revoked {
            return Err(FactCursorErrorV1::KeyRevoked);
        }
        Ok(material)
    }
}

/// Fact read family bound into a continuation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FactCursorOperationV1 {
    Search,
    Probe,
    Related,
    Reason,
    List,
}

/// Query identity bound into a continuation. Values are request semantics,
/// rather than storage implementation details. They are hashed before they
/// enter the encrypted payload.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum FactCursorQueryV1 {
    Text(String),
    Entity(String),
    Entities(Vec<String>),
}

/// Request identity that a fact cursor is authorized to continue.
///
/// This type is deliberately not serializable. Its transport representation
/// is the fixed-size digest set carried by the encrypted cursor payload.
#[derive(Clone, PartialEq, Eq)]
pub struct FactCursorBindingV1 {
    owner: FactCommitOwnerV1,
    operation: FactCursorOperationV1,
    profile_binding: [u8; 32],
    owner_digest: [u8; 32],
    scope_digest: [u8; 32],
    query_digest: [u8; 32],
    filter_digest: [u8; 32],
    ranking_revision: [u8; 32],
    limit: u32,
}

impl fmt::Debug for FactCursorBindingV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FactCursorBindingV1")
            .field("owner", &self.owner)
            .field("operation", &self.operation)
            .field("profile_binding", &hex::encode(self.profile_binding))
            .field("owner_digest", &hex::encode(self.owner_digest))
            .field("scope_digest", &hex::encode(self.scope_digest))
            .field("query_digest", &hex::encode(self.query_digest))
            .field("filter_digest", &hex::encode(self.filter_digest))
            .field("ranking_revision", &hex::encode(self.ranking_revision))
            .field("limit", &self.limit)
            .finish()
    }
}

impl FactCursorBindingV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        owner: FactCommitOwnerV1,
        operation: FactCursorOperationV1,
        query: Option<FactCursorQueryV1>,
        memory_scope: Option<MemoryScopeV1>,
        project_selector: Option<RetainedProjectSelectorV1>,
        category: Option<FactCategoryV1>,
        min_trust_millionths: Option<u32>,
        limit: usize,
    ) -> Result<Self, FactCursorErrorV1> {
        validate_owner(&owner)?;
        if limit == 0 || limit > u32::MAX as usize {
            return Err(FactCursorErrorV1::QueryMismatch);
        }
        if min_trust_millionths.is_some_and(|value| value > 1_000_000) {
            return Err(FactCursorErrorV1::QueryMismatch);
        }
        if let Some(selector) = &project_selector {
            selector
                .project_id
                .validate()
                .map_err(|_| FactCursorErrorV1::ScopeMismatch)?;
        }
        validate_query(&query)?;
        if matches!(&operation, FactCursorOperationV1::List) && query.is_some() {
            return Err(FactCursorErrorV1::QueryMismatch);
        }
        if matches!(
            &operation,
            FactCursorOperationV1::Search | FactCursorOperationV1::Probe
        ) && query.is_none()
        {
            return Err(FactCursorErrorV1::QueryMismatch);
        }

        let profile_binding = profile_binding_for_owner(&owner)?;
        Ok(Self {
            owner_digest: digest("tracedecay.fact-cursor-owner.v2", &owner)?,
            scope_digest: digest(
                "tracedecay.fact-cursor-scope.v2",
                &(memory_scope, project_selector),
            )?,
            query_digest: digest("tracedecay.fact-cursor-query.v2", &query)?,
            filter_digest: digest(
                "tracedecay.fact-cursor-filter.v2",
                &(category, min_trust_millionths),
            )?,
            ranking_revision: digest(
                "tracedecay.fact-cursor-ranking-revision.v2",
                &FACT_CURSOR_RANKING_REVISION_V2,
            )?,
            owner,
            operation,
            profile_binding,
            limit: u32::try_from(limit).map_err(|_| FactCursorErrorV1::QueryMismatch)?,
        })
    }

    /// Return the durable profile binding used to derive this cursor key.
    #[must_use]
    pub const fn profile_binding(&self) -> [u8; 32] {
        self.profile_binding
    }

    #[must_use]
    pub fn owner(&self) -> &FactCommitOwnerV1 {
        &self.owner
    }

    #[must_use]
    pub fn operation(&self) -> &FactCursorOperationV1 {
        &self.operation
    }

    #[must_use]
    pub const fn limit(&self) -> usize {
        self.limit as usize
    }
}

/// Derive the profile binding without creating an operation-specific request
/// binding. Providers use this when loading a keyring before a cursor can be
/// decoded.
pub fn profile_binding_for_owner(owner: &FactCommitOwnerV1) -> Result<[u8; 32], FactCursorErrorV1> {
    validate_owner(owner)?;
    digest("tracedecay.fact-cursor-profile.v2", owner)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum FactCursorPositionV2 {
    Search {
        score_millionths: u32,
        updated_at: UtcMicros,
        fact_id: String,
    },
    List {
        fact_id: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct FactCursorPayloadV2 {
    version: u8,
    operation: u8,
    profile_binding: [u8; 32],
    owner_digest: [u8; 32],
    scope_digest: [u8; 32],
    query_digest: [u8; 32],
    filter_digest: [u8; 32],
    ranking_revision: [u8; 32],
    limit: u32,
    issued_at: UtcMicros,
    expires_at: UtcMicros,
    position: FactCursorPositionV2,
}

/// Mint a cursor using the durable keyring active epoch.
pub fn encode_fact_search_cursor_with_keyring(
    binding: &FactCursorBindingV1,
    score_millionths: u32,
    updated_at: UtcMicros,
    fact_id: FactId,
    keyring: &FactCursorKeyringV1,
) -> Result<FactSearchCursorV1, FactCursorErrorV1> {
    encode_fact_search_cursor_at(
        binding,
        score_millionths,
        updated_at,
        fact_id,
        keyring,
        crate::now_micros(),
    )
}

/// Mint a cursor at an explicit instant. The explicit-clock form keeps
/// expiry and rotation tests independent of wall-clock scheduling.
pub fn encode_fact_search_cursor_at(
    binding: &FactCursorBindingV1,
    score_millionths: u32,
    updated_at: UtcMicros,
    fact_id: FactId,
    keyring: &FactCursorKeyringV1,
    issued_at: UtcMicros,
) -> Result<FactSearchCursorV1, FactCursorErrorV1> {
    let position = FactCursorPositionV2::Search {
        score_millionths,
        updated_at,
        fact_id: fact_id.as_str().to_owned(),
    };
    let token = encode(binding, position, keyring, issued_at)?;
    FactSearchCursorV1::new(token).map_err(|_| FactCursorErrorV1::Malformed)
}

/// Mint a list cursor using the durable keyring active epoch.
pub fn encode_fact_list_cursor_with_keyring(
    binding: &FactCursorBindingV1,
    fact_id: FactId,
    keyring: &FactCursorKeyringV1,
) -> Result<FactListCursorV1, FactCursorErrorV1> {
    encode_fact_list_cursor_at(binding, fact_id, keyring, crate::now_micros())
}

/// Mint a list cursor at an explicit instant.
pub fn encode_fact_list_cursor_at(
    binding: &FactCursorBindingV1,
    fact_id: FactId,
    keyring: &FactCursorKeyringV1,
    issued_at: UtcMicros,
) -> Result<FactListCursorV1, FactCursorErrorV1> {
    let token = encode(
        binding,
        FactCursorPositionV2::List {
            fact_id: fact_id.as_str().to_owned(),
        },
        keyring,
        issued_at,
    )?;
    FactListCursorV1::new(token).map_err(|_| FactCursorErrorV1::Malformed)
}

/// Verify a ranked cursor using the durable keyring and recover its private
/// ordering position. The tuple is intentionally not a public position
/// struct; this function is the only adapter-to-store handoff.
pub fn decode_fact_search_cursor_with_keyring(
    cursor: &FactSearchCursorV1,
    expected: &FactCursorBindingV1,
    keyring: &FactCursorKeyringV1,
) -> Result<(u32, UtcMicros, FactId), FactCursorErrorV1> {
    decode_fact_search_cursor_at(cursor, expected, keyring, crate::now_micros())
}

/// Verify a list cursor using the durable keyring and recover its private
/// position.
pub fn decode_fact_list_cursor_with_keyring(
    cursor: &FactListCursorV1,
    expected: &FactCursorBindingV1,
    keyring: &FactCursorKeyringV1,
) -> Result<FactId, FactCursorErrorV1> {
    decode_fact_list_cursor_at(cursor, expected, keyring, crate::now_micros())
}

/// Explicit-clock verification form used by expiry tests and deterministic
/// callers that already carry a request clock.
pub fn decode_fact_search_cursor_at(
    cursor: &FactSearchCursorV1,
    expected: &FactCursorBindingV1,
    keyring: &FactCursorKeyringV1,
    now: UtcMicros,
) -> Result<(u32, UtcMicros, FactId), FactCursorErrorV1> {
    let position = decode(cursor.as_str(), expected, keyring, now)?;
    let FactCursorPositionV2::Search {
        score_millionths,
        updated_at,
        fact_id,
    } = position
    else {
        return Err(FactCursorErrorV1::KindMismatch);
    };
    if score_millionths > 1_500_000 {
        return Err(FactCursorErrorV1::PositionMismatch);
    }
    let fact_id = FactId::new(fact_id).map_err(|_| FactCursorErrorV1::PositionMismatch)?;
    fact_id
        .validate()
        .map_err(|_| FactCursorErrorV1::PositionMismatch)?;
    Ok((score_millionths, updated_at, fact_id))
}

/// Explicit-clock list verification form.
pub fn decode_fact_list_cursor_at(
    cursor: &FactListCursorV1,
    expected: &FactCursorBindingV1,
    keyring: &FactCursorKeyringV1,
    now: UtcMicros,
) -> Result<FactId, FactCursorErrorV1> {
    let position = decode(cursor.as_str(), expected, keyring, now)?;
    let FactCursorPositionV2::List { fact_id } = position else {
        return Err(FactCursorErrorV1::KindMismatch);
    };
    let fact_id = FactId::new(fact_id).map_err(|_| FactCursorErrorV1::PositionMismatch)?;
    fact_id
        .validate()
        .map_err(|_| FactCursorErrorV1::PositionMismatch)?;
    Ok(fact_id)
}

/// Compatibility symbol for callers that have not yet acquired the durable
/// keyring. It intentionally fails closed rather than falling back to a
/// process-local secret.
pub fn encode_fact_search_cursor(
    _binding: &FactCursorBindingV1,
    _score_millionths: u32,
    _updated_at: UtcMicros,
    _fact_id: FactId,
) -> Result<FactSearchCursorV1, FactCursorErrorV1> {
    Err(FactCursorErrorV1::KeyUnavailable)
}

/// Compatibility symbol for callers that have not yet acquired the durable
/// keyring. It intentionally fails closed.
pub fn encode_fact_list_cursor(
    _binding: &FactCursorBindingV1,
    _fact_id: FactId,
) -> Result<FactListCursorV1, FactCursorErrorV1> {
    Err(FactCursorErrorV1::KeyUnavailable)
}

/// Compatibility symbol for callers that have not yet acquired the durable
/// keyring. It intentionally fails closed.
pub fn decode_fact_search_cursor(
    _cursor: &FactSearchCursorV1,
    _expected: &FactCursorBindingV1,
) -> Result<(u32, UtcMicros, FactId), FactCursorErrorV1> {
    Err(FactCursorErrorV1::KeyUnavailable)
}

/// Compatibility symbol for callers that have not yet acquired the durable
/// keyring. It intentionally fails closed.
pub fn decode_fact_list_cursor(
    _cursor: &FactListCursorV1,
    _expected: &FactCursorBindingV1,
) -> Result<FactId, FactCursorErrorV1> {
    Err(FactCursorErrorV1::KeyUnavailable)
}

fn encode(
    binding: &FactCursorBindingV1,
    position: FactCursorPositionV2,
    keyring: &FactCursorKeyringV1,
    issued_at: UtcMicros,
) -> Result<String, FactCursorErrorV1> {
    if keyring.profile_binding != binding.profile_binding {
        return Err(FactCursorErrorV1::ProfileMismatch);
    }
    validate_position(&position)?;
    let expires_at = issued_at
        .0
        .checked_add(keyring.cursor_ttl_micros)
        .map(UtcMicros)
        .ok_or(FactCursorErrorV1::Expired)?;
    let payload = FactCursorPayloadV2 {
        version: CURSOR_PAYLOAD_VERSION,
        operation: operation_tag(&binding.operation),
        profile_binding: binding.profile_binding,
        owner_digest: binding.owner_digest,
        scope_digest: binding.scope_digest,
        query_digest: binding.query_digest,
        filter_digest: binding.filter_digest,
        ranking_revision: binding.ranking_revision,
        limit: binding.limit,
        issued_at,
        expires_at,
        position,
    };
    let plaintext = serde_json::to_vec(&payload).map_err(|_| FactCursorErrorV1::Malformed)?;
    if plaintext.is_empty() || plaintext.len() > MAX_CURSOR_PLAINTEXT_BYTES {
        return Err(FactCursorErrorV1::Malformed);
    }

    let key = keyring.key(keyring.active_epoch)?;
    let mut nonce_bytes = [0_u8; CURSOR_NONCE_BYTES];
    getrandom::getrandom(&mut nonce_bytes).map_err(|_| FactCursorErrorV1::NonceUnavailable)?;
    let nonce = Nonce::assume_unique_for_key(nonce_bytes);
    let mut ciphertext = plaintext;
    let aad_bytes = aad(keyring, keyring.active_epoch);
    key.key
        .seal_in_place_append_tag(nonce, Aad::from(aad_bytes.as_slice()), &mut ciphertext)
        .map_err(|_| FactCursorErrorV1::KeyUnavailable)?;
    if ciphertext.len() > MAX_CURSOR_CIPHERTEXT_BYTES {
        return Err(FactCursorErrorV1::Malformed);
    }
    let token = format!(
        "{}.{:016x}.{}.{}",
        FACT_CURSOR_FORMAT_VERSION_V2,
        keyring.active_epoch,
        hex::encode(nonce_bytes),
        hex::encode(ciphertext),
    );
    if token.len() > MAX_FACT_CURSOR_BYTES_V2 {
        return Err(FactCursorErrorV1::Malformed);
    }
    Ok(token)
}

fn decode(
    encoded: &str,
    expected: &FactCursorBindingV1,
    keyring: &FactCursorKeyringV1,
    now: UtcMicros,
) -> Result<FactCursorPositionV2, FactCursorErrorV1> {
    if encoded.is_empty() || encoded.len() > MAX_FACT_CURSOR_BYTES_V2 {
        return Err(FactCursorErrorV1::Malformed);
    }
    let mut parts = encoded.split('.');
    let version = parts.next().ok_or(FactCursorErrorV1::Malformed)?;
    let epoch_hex = parts.next().ok_or(FactCursorErrorV1::Malformed)?;
    let nonce_hex = parts.next().ok_or(FactCursorErrorV1::Malformed)?;
    let ciphertext_hex = parts.next().ok_or(FactCursorErrorV1::Malformed)?;
    if parts.next().is_some()
        || version != FACT_CURSOR_FORMAT_VERSION_V2
        || epoch_hex.len() != CURSOR_EPOCH_HEX_BYTES
        || nonce_hex.len() != CURSOR_NONCE_HEX_BYTES
        || ciphertext_hex.is_empty()
        || ciphertext_hex.len() > MAX_CURSOR_CIPHERTEXT_BYTES * 2
    {
        return Err(FactCursorErrorV1::Malformed);
    }
    let epoch = parse_hex_u64(epoch_hex)?;
    let nonce_bytes = parse_fixed_hex::<CURSOR_NONCE_BYTES>(nonce_hex)?;
    let ciphertext = hex::decode(ciphertext_hex).map_err(|_| FactCursorErrorV1::Malformed)?;
    if ciphertext.len() < CURSOR_AEAD_TAG_BYTES || ciphertext.len() > MAX_CURSOR_CIPHERTEXT_BYTES {
        return Err(FactCursorErrorV1::Malformed);
    }
    if keyring.profile_binding != expected.profile_binding {
        return Err(FactCursorErrorV1::ProfileMismatch);
    }
    let key = keyring.key(epoch)?;
    let nonce = Nonce::assume_unique_for_key(nonce_bytes);
    let mut plaintext = ciphertext;
    let aad_bytes = aad(keyring, epoch);
    let plaintext = key
        .key
        .open_in_place(nonce, Aad::from(aad_bytes.as_slice()), &mut plaintext)
        .map_err(|_| FactCursorErrorV1::Tampered)?;
    let payload: FactCursorPayloadV2 =
        serde_json::from_slice(plaintext).map_err(|_| FactCursorErrorV1::Tampered)?;
    if payload.version != CURSOR_PAYLOAD_VERSION
        || payload.profile_binding != keyring.profile_binding
    {
        return Err(FactCursorErrorV1::Tampered);
    }
    if now.0 < payload.issued_at.0 {
        return Err(FactCursorErrorV1::Tampered);
    }
    if now.0 >= payload.expires_at.0 {
        return Err(FactCursorErrorV1::Expired);
    }
    if payload.owner_digest != expected.owner_digest {
        return Err(FactCursorErrorV1::ScopeMismatch);
    }
    if payload.scope_digest != expected.scope_digest {
        return Err(FactCursorErrorV1::ScopeMismatch);
    }
    if payload.operation != operation_tag(&expected.operation) {
        if operation_family_tag(payload.operation) != operation_family(&expected.operation) {
            return Err(FactCursorErrorV1::KindMismatch);
        }
        return Err(FactCursorErrorV1::RequestMismatch);
    }
    if payload.query_digest != expected.query_digest
        || payload.filter_digest != expected.filter_digest
        || payload.ranking_revision != expected.ranking_revision
        || payload.limit != expected.limit
    {
        return Err(FactCursorErrorV1::QueryMismatch);
    }
    validate_position(&payload.position)?;
    match (&expected.operation, &payload.position) {
        (FactCursorOperationV1::List, FactCursorPositionV2::List { .. })
        | (FactCursorOperationV1::Search, FactCursorPositionV2::Search { .. })
        | (FactCursorOperationV1::Probe, FactCursorPositionV2::Search { .. })
        | (FactCursorOperationV1::Related, FactCursorPositionV2::Search { .. })
        | (FactCursorOperationV1::Reason, FactCursorPositionV2::Search { .. }) => {}
        _ => return Err(FactCursorErrorV1::KindMismatch),
    }
    Ok(payload.position)
}

fn aad(keyring: &FactCursorKeyringV1, epoch: u64) -> Vec<u8> {
    let mut value = Vec::with_capacity(CURSOR_AAD_DOMAIN.len() + 1 + 8 + 32);
    value.extend_from_slice(CURSOR_AAD_DOMAIN);
    value.push(b':');
    value.extend_from_slice(&epoch.to_be_bytes());
    value.extend_from_slice(&keyring.profile_binding);
    value
}

fn digest<T: Serialize>(domain: &'static str, value: &T) -> Result<[u8; 32], FactCursorErrorV1> {
    let bytes = serde_json::to_vec(&(domain, value)).map_err(|_| FactCursorErrorV1::Malformed)?;
    Ok(Sha256::digest(bytes).into())
}

fn operation_tag(operation: &FactCursorOperationV1) -> u8 {
    match operation {
        FactCursorOperationV1::Search => 1,
        FactCursorOperationV1::Probe => 2,
        FactCursorOperationV1::Related => 3,
        FactCursorOperationV1::Reason => 4,
        FactCursorOperationV1::List => 5,
    }
}

fn operation_family_tag(operation: u8) -> &'static str {
    match operation {
        5 => "list",
        1..=4 => "ranked",
        _ => "unknown",
    }
}

fn operation_family(operation: &FactCursorOperationV1) -> &'static str {
    match operation {
        FactCursorOperationV1::List => "list",
        FactCursorOperationV1::Search
        | FactCursorOperationV1::Probe
        | FactCursorOperationV1::Related
        | FactCursorOperationV1::Reason => "ranked",
    }
}

fn validate_query(query: &Option<FactCursorQueryV1>) -> Result<(), FactCursorErrorV1> {
    let Some(query) = query else {
        return Ok(());
    };
    match query {
        FactCursorQueryV1::Text(value) | FactCursorQueryV1::Entity(value) => {
            validate_query_text(value)?;
        }
        FactCursorQueryV1::Entities(values) => {
            if values.is_empty() || values.len() > MAX_FACT_CURSOR_ENTITY_COUNT_V2 {
                return Err(FactCursorErrorV1::Malformed);
            }
            for value in values {
                validate_query_text(value)?;
            }
            if values.windows(2).any(|pair| pair[0] >= pair[1]) {
                return Err(FactCursorErrorV1::Malformed);
            }
        }
    }
    Ok(())
}

fn validate_owner(owner: &FactCommitOwnerV1) -> Result<(), FactCursorErrorV1> {
    if let FactCommitOwnerV1::Project { project_id } = owner {
        project_id
            .validate()
            .map_err(|_| FactCursorErrorV1::ScopeMismatch)?;
    }
    Ok(())
}

fn validate_query_text(value: &str) -> Result<(), FactCursorErrorV1> {
    if value.is_empty()
        || value.trim().is_empty()
        || value.len() > MAX_FACT_CURSOR_QUERY_BYTES_V2
        || value.chars().any(char::is_control)
    {
        return Err(FactCursorErrorV1::Malformed);
    }
    Ok(())
}

fn validate_position(position: &FactCursorPositionV2) -> Result<(), FactCursorErrorV1> {
    match position {
        FactCursorPositionV2::Search {
            score_millionths,
            fact_id,
            ..
        } => {
            if *score_millionths > 1_500_000 {
                return Err(FactCursorErrorV1::PositionMismatch);
            }
            validate_fact_id_text(fact_id)?;
        }
        FactCursorPositionV2::List { fact_id } => validate_fact_id_text(fact_id)?,
    }
    Ok(())
}

fn validate_fact_id_text(value: &str) -> Result<(), FactCursorErrorV1> {
    let fact_id = FactId::new(value.to_owned()).map_err(|_| FactCursorErrorV1::PositionMismatch)?;
    fact_id
        .validate()
        .map_err(|_| FactCursorErrorV1::PositionMismatch)
}

fn parse_hex_u64(value: &str) -> Result<u64, FactCursorErrorV1> {
    if value
        .bytes()
        .any(|byte| !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase())
    {
        return Err(FactCursorErrorV1::Malformed);
    }
    u64::from_str_radix(value, 16).map_err(|_| FactCursorErrorV1::Malformed)
}

fn parse_fixed_hex<const N: usize>(value: &str) -> Result<[u8; N], FactCursorErrorV1> {
    if value.len() != N * 2
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase())
    {
        return Err(FactCursorErrorV1::Malformed);
    }
    let bytes = hex::decode(value).map_err(|_| FactCursorErrorV1::Malformed)?;
    bytes.try_into().map_err(|_| FactCursorErrorV1::Malformed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding() -> FactCursorBindingV1 {
        FactCursorBindingV1::new(
            FactCommitOwnerV1::Profile,
            FactCursorOperationV1::Search,
            Some(FactCursorQueryV1::Text("compiler".to_owned())),
            Some(MemoryScopeV1::User),
            None,
            Some(FactCategoryV1::Tool),
            Some(300_000),
            20,
        )
        .expect("valid binding")
    }

    fn list_binding() -> FactCursorBindingV1 {
        FactCursorBindingV1::new(
            FactCommitOwnerV1::Profile,
            FactCursorOperationV1::List,
            None,
            Some(MemoryScopeV1::User),
            None,
            None,
            None,
            20,
        )
        .expect("valid list binding")
    }

    fn fact_id() -> FactId {
        FactId::new(format!("fact.v1.{}", "a".repeat(128))).expect("fact id")
    }

    fn keyring(binding: &FactCursorBindingV1) -> FactCursorKeyringV1 {
        FactCursorKeyringV1::new(
            binding.profile_binding(),
            7,
            [42_u8; 32],
            FACT_CURSOR_TTL_MICROS_V2,
        )
        .expect("keyring")
    }

    #[test]
    fn cursor_is_opaque_confidential_and_round_trips() {
        let binding = binding();
        let keyring = keyring(&binding);
        let first = encode_fact_search_cursor_at(
            &binding,
            700_000,
            UtcMicros(42),
            fact_id(),
            &keyring,
            UtcMicros(100),
        )
        .expect("cursor");
        let second = encode_fact_search_cursor_at(
            &binding,
            700_000,
            UtcMicros(42),
            fact_id(),
            &keyring,
            UtcMicros(100),
        )
        .expect("cursor");
        assert_ne!(first, second, "AEAD nonces must not repeat");
        assert!(first.as_str().len() <= MAX_FACT_CURSOR_BYTES_V2);
        assert!(!first.as_str().contains("compiler"));
        assert!(!first.as_str().contains("700000"));
        assert!(!first.as_str().contains("fact.v1"));
        let wire = serde_json::to_value(&first).expect("cursor wire");
        assert!(wire.is_string());
        let position = decode_fact_search_cursor_at(&first, &binding, &keyring, UtcMicros(101))
            .expect("verified position");
        assert_eq!(position.0, 700_000);
        assert_eq!(position.1, UtcMicros(42));
    }

    #[test]
    fn cursor_tamper_and_binding_mismatch_are_typed() {
        let binding = binding();
        let keyring = keyring(&binding);
        let cursor = encode_fact_search_cursor_at(
            &binding,
            700_000,
            UtcMicros(42),
            fact_id(),
            &keyring,
            UtcMicros(100),
        )
        .expect("cursor");
        let mut tampered = cursor.as_str().to_owned();
        let last = tampered.pop().expect("ciphertext");
        tampered.push(if last == '0' { '1' } else { '0' });
        let tampered = FactSearchCursorV1::new(tampered).expect("bounded cursor");
        assert_eq!(
            decode_fact_search_cursor_at(&tampered, &binding, &keyring, UtcMicros(101)),
            Err(FactCursorErrorV1::Tampered)
        );
        let other = FactCursorBindingV1::new(
            FactCommitOwnerV1::Profile,
            FactCursorOperationV1::Search,
            Some(FactCursorQueryV1::Text("different".to_owned())),
            Some(MemoryScopeV1::User),
            None,
            Some(FactCategoryV1::Tool),
            Some(300_000),
            20,
        )
        .expect("valid binding");
        assert_eq!(
            decode_fact_search_cursor_at(&cursor, &other, &keyring, UtcMicros(101)),
            Err(FactCursorErrorV1::QueryMismatch)
        );
    }

    #[test]
    fn list_cursor_round_trips_and_scope_mismatch_is_rejected() {
        let binding = list_binding();
        let keyring = keyring(&binding);
        let cursor = encode_fact_list_cursor_at(&binding, fact_id(), &keyring, UtcMicros(100))
            .expect("cursor");
        let position = decode_fact_list_cursor_at(&cursor, &binding, &keyring, UtcMicros(101))
            .expect("position");
        assert_eq!(position, fact_id());

        let other_scope = FactCursorBindingV1::new(
            FactCommitOwnerV1::Profile,
            FactCursorOperationV1::List,
            None,
            Some(MemoryScopeV1::Project),
            None,
            None,
            None,
            20,
        )
        .expect("valid list binding");
        assert_eq!(
            decode_fact_list_cursor_at(&cursor, &other_scope, &keyring, UtcMicros(101)),
            Err(FactCursorErrorV1::ScopeMismatch)
        );
    }

    #[test]
    fn rotation_retains_old_cursor_until_revoke_and_restart_reconstructs() {
        let binding = binding();
        let mut keyring = FactCursorKeyringV1::new(
            binding.profile_binding(),
            7,
            [42_u8; 32],
            FACT_CURSOR_TTL_MICROS_V2,
        )
        .expect("keyring");
        let cursor = encode_fact_search_cursor_at(
            &binding,
            700_000,
            UtcMicros(42),
            fact_id(),
            &keyring,
            UtcMicros(100),
        )
        .expect("cursor");
        keyring.rotate(8, [43_u8; 32]).expect("rotate");
        assert!(decode_fact_search_cursor_at(&cursor, &binding, &keyring, UtcMicros(101)).is_ok());
        keyring.revoke(7).expect("revoke");
        assert_eq!(
            decode_fact_search_cursor_at(&cursor, &binding, &keyring, UtcMicros(101)),
            Err(FactCursorErrorV1::KeyRevoked)
        );
        let restarted = FactCursorKeyringV1::new(
            binding.profile_binding(),
            7,
            [42_u8; 32],
            FACT_CURSOR_TTL_MICROS_V2,
        )
        .expect("restarted keyring");
        assert!(
            decode_fact_search_cursor_at(&cursor, &binding, &restarted, UtcMicros(101)).is_ok()
        );
    }

    #[test]
    fn four_kilobyte_query_keeps_cursor_bounded() {
        let query = "q".repeat(MAX_FACT_CURSOR_QUERY_BYTES_V2);
        let binding = FactCursorBindingV1::new(
            FactCommitOwnerV1::Profile,
            FactCursorOperationV1::Search,
            Some(FactCursorQueryV1::Text(query)),
            Some(MemoryScopeV1::User),
            None,
            None,
            None,
            20,
        )
        .expect("maximum query");
        let keyring = keyring(&binding);
        let cursor = encode_fact_search_cursor_at(
            &binding,
            700_000,
            UtcMicros(42),
            fact_id(),
            &keyring,
            UtcMicros(100),
        )
        .expect("cursor");
        assert!(cursor.as_str().len() <= MAX_FACT_CURSOR_BYTES_V2);
    }

    #[test]
    fn expiry_is_typed_and_process_key_fallback_fails_closed() {
        let binding = binding();
        let keyring = FactCursorKeyringV1::new(binding.profile_binding(), 7, [42_u8; 32], 10)
            .expect("keyring");
        let cursor = encode_fact_search_cursor_at(
            &binding,
            700_000,
            UtcMicros(42),
            fact_id(),
            &keyring,
            UtcMicros(100),
        )
        .expect("cursor");
        assert_eq!(
            decode_fact_search_cursor_at(&cursor, &binding, &keyring, UtcMicros(110)),
            Err(FactCursorErrorV1::Expired)
        );
        assert_eq!(
            encode_fact_search_cursor(&binding, 1, UtcMicros(1), fact_id()),
            Err(FactCursorErrorV1::KeyUnavailable)
        );
    }

    #[test]
    fn malformed_structural_wire_and_profile_binding_are_rejected() {
        let binding = binding();
        let structural = serde_json::json!({
            "score_millionths": 700_000,
            "updated_at": 42,
            "fact_id": fact_id(),
        });
        assert!(serde_json::from_value::<FactSearchCursorV1>(structural).is_err());
        let other_keyring =
            FactCursorKeyringV1::new([9_u8; 32], 7, [42_u8; 32], FACT_CURSOR_TTL_MICROS_V2)
                .expect("keyring");
        assert_eq!(
            encode_fact_search_cursor_with_keyring(
                &binding,
                700_000,
                UtcMicros(42),
                fact_id(),
                &other_keyring,
            ),
            Err(FactCursorErrorV1::ProfileMismatch)
        );
    }
}
