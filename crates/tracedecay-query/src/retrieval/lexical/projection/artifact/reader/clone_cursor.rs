//! Authenticated continuation cursors for the verified clone readers.
//!
//! The legacy clone readers expose typed cursors whose wire representation is
//! just hex-encoded JSON. That representation is useful inside the query
//! kernel, but it is not an authority: a caller can edit the position,
//! generation, or request digest and send it back. This module supplies the
//! narrow codec seam for replacing that wire with the same daemon-owned HMAC
//! authority used by prepared retrieval cursors.
//!
//! `CloneCursorCodecV1` deliberately owns no key material. It delegates
//! signing and verification to [`QueryAuthorityV1`], so principal, full scope,
//! temporal mode, profile, freshness, and authorization revision are covered
//! by the existing prepared-cursor MAC input. The clone payload adds the
//! artifact identity, generation, snapshot, query descriptor, cursor position,
//! and expiry that belong to the clone reader.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracedecay_code_index::clones::{CloneExactKeyV1, CloneNormalizationClassV1};
use tracedecay_domain::{
    AuthorizationRevision, CodeGenerationId, ManifestDigest, PrincipalId, QueryDigest,
    RetrievalCursorKeyId, RetrievalRequest, SymbolOccurrenceId, UtcMicros, canonical_sha256,
};

use crate::retrieval::{QueryAuthorityErrorV1, QueryAuthorityV1};

const CLONE_CURSOR_PREFIX_V2: &str = "ccclone2.";
const CLONE_CURSOR_REVISION_V2: u16 = 2;
const CLONE_CURSOR_TTL_MICROS_V1: i64 = 15 * 60 * 1_000_000;
const CLONE_CURSOR_MAX_ENCODED_BYTES_V1: usize = 32 * 1024;
const CLONE_ARTIFACT_CURSOR_OPERATION_V1: &str = "tracedecay.clone-artifact-cursor.v2";
const CLONE_FAMILY_CURSOR_OPERATION_V1: &str = "tracedecay.clone-family-cursor.v2";
const CLONE_CURSOR_SCOPE_DIGEST_DOMAIN_V1: &str = "tracedecay.clone-cursor-scope.v1";

/// Typed failure from clone cursor parsing, authentication, and binding.
///
/// `Tampered` is reserved for a cursor that has a valid shape but fails the
/// daemon-owned MAC. `Stale` covers a correctly authenticated cursor whose
/// generation, snapshot, request descriptor, or expiry no longer matches the
/// current read. Callers can therefore preserve the existing distinction
/// between retrying with a fresh cursor and rejecting malformed input.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum CloneCursorErrorV1 {
    #[error("clone cursor is invalid")]
    Invalid,
    #[error("clone cursor authentication failed")]
    Tampered,
    #[error("clone cursor is stale")]
    Stale,
    #[error("clone cursor authority is unavailable: {0}")]
    Unavailable(String),
}

/// Failure while serving a clone page through the authenticated cursor
/// boundary. Keeping cursor failures separate from artifact failures lets the
/// application preserve invalid/tampered/stale/unavailable cursor semantics
/// instead of collapsing them into a generic reader error.
#[derive(Debug, Error)]
pub enum CloneCursorReadErrorV1 {
    #[error(transparent)]
    Cursor(#[from] CloneCursorErrorV1),
    #[error(transparent)]
    Artifact(#[from] super::super::CodeLexicalArtifactErrorV1),
}

/// Exact-posting continuation position for the authenticated wire.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub enum CloneArtifactCursorPositionV2 {
    Exact {
        symbol_occurrence_id: SymbolOccurrenceId,
    },
    Fingerprint {
        body_digest: ManifestDigest,
        payload_digest: ManifestDigest,
    },
    /// Discovery stopped inside an ordered fingerprint posting stream. The
    /// optional comparison key is the last candidate whose expensive body
    /// comparison completed; the discovery frontier is independent because a
    /// posting budget can stop before the first candidate is compared.
    FingerprintDiscovery {
        discovery: CloneFingerprintDiscoveryPositionV2,
        #[serde(default)]
        comparison_body_digest: Option<ManifestDigest>,
        #[serde(default)]
        comparison_payload_digest: Option<ManifestDigest>,
    },
}

/// Ordered position within one fingerprint's posting stream.
///
/// `symbol_occurrence_id` and `token_position` are both absent only when the
/// fingerprint's complete posting stream has been consumed. They are paired
/// so a cursor can resume after exactly one `(occurrence, token)` row without
/// replaying or skipping a row at the budget boundary.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CloneFingerprintDiscoveryPositionV2 {
    /// The immutable posting count is part of the ordered fingerprint key.
    /// Keeping it in the cursor prevents a future artifact revision from
    /// interpreting the same fingerprint as a different discovery boundary.
    pub posting_count: u64,
    pub fingerprint: u64,
    #[serde(default)]
    pub symbol_occurrence_id: Option<SymbolOccurrenceId>,
    #[serde(default)]
    pub token_position: Option<u32>,
}

impl CloneFingerprintDiscoveryPositionV2 {
    fn validate(&self) -> Result<(), CloneCursorErrorV1> {
        if self.symbol_occurrence_id.is_some() != self.token_position.is_some() {
            return Err(CloneCursorErrorV1::Invalid);
        }
        Ok(())
    }
}

/// Family-report continuation position for the authenticated wire.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CloneFamilyCursorPositionV2 {
    pub reviewable_source_bytes: u64,
    pub member_count: u64,
    pub class: CloneNormalizationClassV1,
    pub normalization_revision: u16,
    pub digest: ManifestDigest,
    /// A partial family page resumes after this complete family key. It is
    /// absent when the cursor represents the ranked family boundary.
    #[serde(default)]
    pub scan_after: Option<CloneExactKeyV1>,
    /// When a bounded posting read stopped in the middle of one family, seek
    /// past this occurrence within `scan_after` before rebuilding that family.
    /// Keeping the row boundary prevents an oversized family from being
    /// skipped when the sentinel falls inside its posting run.
    #[serde(default)]
    pub scan_after_occurrence: Option<SymbolOccurrenceId>,
    /// Prefix aggregate state retained when the posting budget stopped inside
    /// a family. It lets the next page finish that family without replaying
    /// the bounded prefix or dropping its members.
    #[serde(default)]
    pub minimum_source_bytes: u64,
    #[serde(default)]
    pub representative: Option<SymbolOccurrenceId>,
    #[serde(default)]
    pub has_pull_request_member: bool,
}

/// Authenticated artifact-reader cursor body returned after verification.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CloneArtifactCursorV2 {
    pub artifact_digest: ManifestDigest,
    pub generation: CodeGenerationId,
    pub snapshot_digest: ManifestDigest,
    pub query_descriptor: ManifestDigest,
    pub after: CloneArtifactCursorPositionV2,
    pub expires_at: UtcMicros,
}

/// Authenticated family-report cursor body returned after verification.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CloneFamilyCursorV2 {
    pub artifact_digest: ManifestDigest,
    pub generation: CodeGenerationId,
    pub snapshot_digest: ManifestDigest,
    pub query_descriptor: ManifestDigest,
    pub after: CloneFamilyCursorPositionV2,
    pub expires_at: UtcMicros,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct CloneArtifactCursorPayloadV2 {
    revision: u16,
    operation: String,
    authentication_key_id: RetrievalCursorKeyId,
    principal: PrincipalId,
    scope_digest: ManifestDigest,
    authorization_revision: AuthorizationRevision,
    artifact_digest: ManifestDigest,
    generation: CodeGenerationId,
    snapshot_digest: ManifestDigest,
    query_descriptor: ManifestDigest,
    after: CloneArtifactCursorPositionV2,
    expires_at: UtcMicros,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct CloneFamilyCursorPayloadV2 {
    revision: u16,
    operation: String,
    authentication_key_id: RetrievalCursorKeyId,
    principal: PrincipalId,
    scope_digest: ManifestDigest,
    authorization_revision: AuthorizationRevision,
    artifact_digest: ManifestDigest,
    generation: CodeGenerationId,
    snapshot_digest: ManifestDigest,
    query_descriptor: ManifestDigest,
    after: CloneFamilyCursorPositionV2,
    expires_at: UtcMicros,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct AuthenticatedCloneArtifactCursorV2 {
    payload: CloneArtifactCursorPayloadV2,
    authentication: QueryDigest,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct AuthenticatedCloneFamilyCursorV2 {
    payload: CloneFamilyCursorPayloadV2,
    authentication: QueryDigest,
}

/// Codec that delegates MAC key lifecycle to the existing retrieval authority.
///
/// The borrowed request is the authority's complete request binding. The
/// caller supplies the immutable clone artifact/snapshot identity and the
/// canonical descriptor of the filters/target used to produce one page.
pub struct CloneCursorCodecV1<'a> {
    authority: &'a QueryAuthorityV1,
    request: &'a RetrievalRequest,
}

impl<'a> CloneCursorCodecV1<'a> {
    pub fn new(
        authority: &'a QueryAuthorityV1,
        request: &'a RetrievalRequest,
    ) -> Result<Self, CloneCursorErrorV1> {
        request
            .scope
            .privacy_domain
            .validate()
            .map_err(|_| CloneCursorErrorV1::Invalid)?;
        request
            .snapshot
            .authorization_revision
            .validate()
            .map_err(|_| CloneCursorErrorV1::Invalid)?;
        if authority.privacy_domain() != &request.scope.privacy_domain {
            return Err(CloneCursorErrorV1::Unavailable(
                "query authority is mounted for a different privacy domain".to_owned(),
            ));
        }
        Ok(Self { authority, request })
    }

    /// Sign one exact/fingerprint continuation with the active retrieval key.
    pub fn issue_artifact(
        &self,
        artifact_digest: ManifestDigest,
        generation: CodeGenerationId,
        snapshot_digest: ManifestDigest,
        query_descriptor: ManifestDigest,
        after: CloneArtifactCursorPositionV2,
        now: UtcMicros,
    ) -> Result<String, CloneCursorErrorV1> {
        validate_artifact_position(&after)?;
        let expires_at = expiry_from(now)?;
        let payload = CloneArtifactCursorPayloadV2 {
            revision: CLONE_CURSOR_REVISION_V2,
            operation: CLONE_ARTIFACT_CURSOR_OPERATION_V1.to_owned(),
            authentication_key_id: self.authority.active_query_key_id(),
            principal: self.request.principal.clone(),
            scope_digest: scope_digest(self.request)?,
            authorization_revision: self.request.snapshot.authorization_revision.clone(),
            artifact_digest,
            generation,
            snapshot_digest,
            query_descriptor,
            after,
            expires_at,
        };
        let authentication = self.authenticate(&payload)?;
        encode_envelope(&AuthenticatedCloneArtifactCursorV2 {
            payload,
            authentication,
        })
    }

    /// Verify and decode an exact/fingerprint continuation.
    #[allow(clippy::too_many_arguments)]
    pub fn decode_artifact(
        &self,
        encoded: &str,
        artifact_digest: &ManifestDigest,
        generation: &CodeGenerationId,
        snapshot_digest: &ManifestDigest,
        query_descriptor: &ManifestDigest,
        now: UtcMicros,
    ) -> Result<CloneArtifactCursorV2, CloneCursorErrorV1> {
        let envelope = decode_envelope::<AuthenticatedCloneArtifactCursorV2>(encoded)?;
        let payload_bytes =
            serde_json::to_vec(&envelope.payload).map_err(|_| CloneCursorErrorV1::Invalid)?;
        self.verify(
            &envelope.payload.authentication_key_id,
            &payload_bytes,
            &envelope.authentication,
        )?;
        let payload = envelope.payload;
        validate_common(
            &payload.operation,
            CLONE_ARTIFACT_CURSOR_OPERATION_V1,
            payload.revision,
            &payload.principal,
            &payload.scope_digest,
            &payload.authorization_revision,
            &payload.artifact_digest,
            artifact_digest,
            &payload.generation,
            generation,
            &payload.snapshot_digest,
            snapshot_digest,
            &payload.query_descriptor,
            Some(query_descriptor),
            payload.expires_at,
            now,
            self.request,
        )?;
        validate_artifact_position(&payload.after)?;
        Ok(CloneArtifactCursorV2 {
            artifact_digest: payload.artifact_digest,
            generation: payload.generation,
            snapshot_digest: payload.snapshot_digest,
            query_descriptor: payload.query_descriptor,
            after: payload.after,
            expires_at: payload.expires_at,
        })
    }

    /// Verify and decode an artifact continuation before the reader has
    /// computed its operation-specific descriptor.
    ///
    /// The MAC still covers the complete admitted [`RetrievalRequest`], and
    /// the artifact, generation, snapshot, principal, authorization, and
    /// expiry bindings are checked here. The reader subsequently compares
    /// `query_descriptor` with the canonical descriptor for the exact or
    /// fingerprint operation. Keeping that final comparison in the reader is
    /// necessary because the fingerprint descriptor includes the selected
    /// token block, which is only available at the serving call site.
    #[allow(clippy::too_many_arguments)]
    pub fn decode_artifact_unbound(
        &self,
        encoded: &str,
        artifact_digest: &ManifestDigest,
        generation: &CodeGenerationId,
        snapshot_digest: &ManifestDigest,
        now: UtcMicros,
    ) -> Result<CloneArtifactCursorV2, CloneCursorErrorV1> {
        let envelope = decode_envelope::<AuthenticatedCloneArtifactCursorV2>(encoded)?;
        let payload_bytes =
            serde_json::to_vec(&envelope.payload).map_err(|_| CloneCursorErrorV1::Invalid)?;
        self.verify(
            &envelope.payload.authentication_key_id,
            &payload_bytes,
            &envelope.authentication,
        )?;
        let payload = envelope.payload;
        validate_common(
            &payload.operation,
            CLONE_ARTIFACT_CURSOR_OPERATION_V1,
            payload.revision,
            &payload.principal,
            &payload.scope_digest,
            &payload.authorization_revision,
            &payload.artifact_digest,
            artifact_digest,
            &payload.generation,
            generation,
            &payload.snapshot_digest,
            snapshot_digest,
            &payload.query_descriptor,
            None,
            payload.expires_at,
            now,
            self.request,
        )?;
        validate_artifact_position(&payload.after)?;
        Ok(CloneArtifactCursorV2 {
            artifact_digest: payload.artifact_digest,
            generation: payload.generation,
            snapshot_digest: payload.snapshot_digest,
            query_descriptor: payload.query_descriptor,
            after: payload.after,
            expires_at: payload.expires_at,
        })
    }

    /// Sign one family-report continuation with the active retrieval key.
    pub fn issue_family(
        &self,
        artifact_digest: ManifestDigest,
        generation: CodeGenerationId,
        snapshot_digest: ManifestDigest,
        query_descriptor: ManifestDigest,
        after: CloneFamilyCursorPositionV2,
        now: UtcMicros,
    ) -> Result<String, CloneCursorErrorV1> {
        let expires_at = expiry_from(now)?;
        let payload = CloneFamilyCursorPayloadV2 {
            revision: CLONE_CURSOR_REVISION_V2,
            operation: CLONE_FAMILY_CURSOR_OPERATION_V1.to_owned(),
            authentication_key_id: self.authority.active_query_key_id(),
            principal: self.request.principal.clone(),
            scope_digest: scope_digest(self.request)?,
            authorization_revision: self.request.snapshot.authorization_revision.clone(),
            artifact_digest,
            generation,
            snapshot_digest,
            query_descriptor,
            after,
            expires_at,
        };
        let authentication = self.authenticate(&payload)?;
        encode_envelope(&AuthenticatedCloneFamilyCursorV2 {
            payload,
            authentication,
        })
    }

    /// Verify and decode a family-report continuation.
    #[allow(clippy::too_many_arguments)]
    pub fn decode_family(
        &self,
        encoded: &str,
        artifact_digest: &ManifestDigest,
        generation: &CodeGenerationId,
        snapshot_digest: &ManifestDigest,
        query_descriptor: &ManifestDigest,
        now: UtcMicros,
    ) -> Result<CloneFamilyCursorV2, CloneCursorErrorV1> {
        let envelope = decode_envelope::<AuthenticatedCloneFamilyCursorV2>(encoded)?;
        let payload_bytes =
            serde_json::to_vec(&envelope.payload).map_err(|_| CloneCursorErrorV1::Invalid)?;
        self.verify(
            &envelope.payload.authentication_key_id,
            &payload_bytes,
            &envelope.authentication,
        )?;
        let payload = envelope.payload;
        validate_common(
            &payload.operation,
            CLONE_FAMILY_CURSOR_OPERATION_V1,
            payload.revision,
            &payload.principal,
            &payload.scope_digest,
            &payload.authorization_revision,
            &payload.artifact_digest,
            artifact_digest,
            &payload.generation,
            generation,
            &payload.snapshot_digest,
            snapshot_digest,
            &payload.query_descriptor,
            Some(query_descriptor),
            payload.expires_at,
            now,
            self.request,
        )?;
        Ok(CloneFamilyCursorV2 {
            artifact_digest: payload.artifact_digest,
            generation: payload.generation,
            snapshot_digest: payload.snapshot_digest,
            query_descriptor: payload.query_descriptor,
            after: payload.after,
            expires_at: payload.expires_at,
        })
    }

    /// Verify and decode a family continuation before the family reader has
    /// computed its filter descriptor.
    #[allow(clippy::too_many_arguments)]
    pub fn decode_family_unbound(
        &self,
        encoded: &str,
        artifact_digest: &ManifestDigest,
        generation: &CodeGenerationId,
        snapshot_digest: &ManifestDigest,
        now: UtcMicros,
    ) -> Result<CloneFamilyCursorV2, CloneCursorErrorV1> {
        let envelope = decode_envelope::<AuthenticatedCloneFamilyCursorV2>(encoded)?;
        let payload_bytes =
            serde_json::to_vec(&envelope.payload).map_err(|_| CloneCursorErrorV1::Invalid)?;
        self.verify(
            &envelope.payload.authentication_key_id,
            &payload_bytes,
            &envelope.authentication,
        )?;
        let payload = envelope.payload;
        validate_common(
            &payload.operation,
            CLONE_FAMILY_CURSOR_OPERATION_V1,
            payload.revision,
            &payload.principal,
            &payload.scope_digest,
            &payload.authorization_revision,
            &payload.artifact_digest,
            artifact_digest,
            &payload.generation,
            generation,
            &payload.snapshot_digest,
            snapshot_digest,
            &payload.query_descriptor,
            None,
            payload.expires_at,
            now,
            self.request,
        )?;
        Ok(CloneFamilyCursorV2 {
            artifact_digest: payload.artifact_digest,
            generation: payload.generation,
            snapshot_digest: payload.snapshot_digest,
            query_descriptor: payload.query_descriptor,
            after: payload.after,
            expires_at: payload.expires_at,
        })
    }

    fn authenticate<P: Serialize>(&self, payload: &P) -> Result<QueryDigest, CloneCursorErrorV1> {
        let bytes = serde_json::to_vec(payload).map_err(|_| CloneCursorErrorV1::Invalid)?;
        self.authority
            .authenticate_prepared_cursor_payload(self.request, &bytes)
            .map_err(map_authority_error)
    }

    fn verify(
        &self,
        key_id: &RetrievalCursorKeyId,
        payload_bytes: &[u8],
        authentication: &QueryDigest,
    ) -> Result<(), CloneCursorErrorV1> {
        self.authority
            .verify_prepared_cursor_payload(key_id, self.request, payload_bytes, authentication)
            .map_err(map_authority_error)
    }
}

fn scope_digest(request: &RetrievalRequest) -> Result<ManifestDigest, CloneCursorErrorV1> {
    canonical_sha256(&(CLONE_CURSOR_SCOPE_DIGEST_DOMAIN_V1, &request.scope))
        .map_err(|_| CloneCursorErrorV1::Invalid)
}

fn validate_artifact_position(
    position: &CloneArtifactCursorPositionV2,
) -> Result<(), CloneCursorErrorV1> {
    if let CloneArtifactCursorPositionV2::FingerprintDiscovery {
        discovery,
        comparison_body_digest,
        comparison_payload_digest,
    } = position
    {
        discovery.validate()?;
        if comparison_body_digest.is_some() != comparison_payload_digest.is_some() {
            return Err(CloneCursorErrorV1::Invalid);
        }
    }
    Ok(())
}

fn expiry_from(now: UtcMicros) -> Result<UtcMicros, CloneCursorErrorV1> {
    now.0
        .checked_add(CLONE_CURSOR_TTL_MICROS_V1)
        .map(UtcMicros)
        .ok_or(CloneCursorErrorV1::Invalid)
}

fn validate_common(
    operation: &str,
    expected_operation: &str,
    revision: u16,
    principal: &PrincipalId,
    encoded_scope_digest: &ManifestDigest,
    authorization_revision: &AuthorizationRevision,
    encoded_artifact_digest: &ManifestDigest,
    expected_artifact_digest: &ManifestDigest,
    encoded_generation: &CodeGenerationId,
    expected_generation: &CodeGenerationId,
    encoded_snapshot_digest: &ManifestDigest,
    expected_snapshot_digest: &ManifestDigest,
    encoded_query_descriptor: &ManifestDigest,
    expected_query_descriptor: Option<&ManifestDigest>,
    expires_at: UtcMicros,
    now: UtcMicros,
    request: &RetrievalRequest,
) -> Result<(), CloneCursorErrorV1> {
    if revision != CLONE_CURSOR_REVISION_V2 || operation != expected_operation {
        return Err(CloneCursorErrorV1::Invalid);
    }
    if principal != &request.principal
        || encoded_scope_digest != &scope_digest(request)?
        || authorization_revision != &request.snapshot.authorization_revision
    {
        return Err(CloneCursorErrorV1::Stale);
    }
    if encoded_artifact_digest != expected_artifact_digest
        || encoded_generation != expected_generation
        || encoded_snapshot_digest != expected_snapshot_digest
        || expected_query_descriptor
            .is_some_and(|expected| encoded_query_descriptor != expected)
    {
        return Err(CloneCursorErrorV1::Stale);
    }
    if now.0 >= expires_at.0 {
        return Err(CloneCursorErrorV1::Stale);
    }
    Ok(())
}

fn encode_envelope<T: Serialize>(envelope: &T) -> Result<String, CloneCursorErrorV1> {
    let bytes = serde_json::to_vec(envelope).map_err(|_| CloneCursorErrorV1::Invalid)?;
    Ok(format!("{CLONE_CURSOR_PREFIX_V2}{}", hex::encode(bytes)))
}

fn decode_envelope<T>(encoded: &str) -> Result<T, CloneCursorErrorV1>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    if encoded.len() > CLONE_CURSOR_MAX_ENCODED_BYTES_V1 {
        return Err(CloneCursorErrorV1::Invalid);
    }
    let encoded = encoded
        .strip_prefix(CLONE_CURSOR_PREFIX_V2)
        .ok_or(CloneCursorErrorV1::Invalid)?;
    let bytes = hex::decode(encoded).map_err(|_| CloneCursorErrorV1::Invalid)?;
    if hex::encode(&bytes) != encoded {
        return Err(CloneCursorErrorV1::Invalid);
    }
    let envelope = serde_json::from_slice::<T>(&bytes).map_err(|_| CloneCursorErrorV1::Invalid)?;
    if serde_json::to_vec(&envelope).map_err(|_| CloneCursorErrorV1::Invalid)? != bytes {
        return Err(CloneCursorErrorV1::Invalid);
    }
    Ok(envelope)
}

fn map_authority_error(error: QueryAuthorityErrorV1) -> CloneCursorErrorV1 {
    match error {
        QueryAuthorityErrorV1::QueryAuthentication(
            crate::retrieval::fusion::QueryDigestAuthenticationError::AuthenticationFailed,
        ) => CloneCursorErrorV1::Tampered,
        QueryAuthorityErrorV1::QueryAuthentication(
            crate::retrieval::fusion::QueryDigestAuthenticationError::KeyRevoked,
        )
        | QueryAuthorityErrorV1::Retrieval(tracedecay_domain::RetrievalError::CursorExpired) => {
            CloneCursorErrorV1::Stale
        }
        authority @ (QueryAuthorityErrorV1::QueryAuthentication(
            crate::retrieval::fusion::QueryDigestAuthenticationError::KeyUnavailable,
        )
        | QueryAuthorityErrorV1::AuthorityUnavailable) => {
            CloneCursorErrorV1::Unavailable(authority.to_string())
        }
        QueryAuthorityErrorV1::QueryAuthentication(
            crate::retrieval::fusion::QueryDigestAuthenticationError::PrivacyDomainMismatch,
        ) => CloneCursorErrorV1::Stale,
        _ => CloneCursorErrorV1::Invalid,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fmt;

    use super::*;
    use crate::retrieval::fusion::RetrievalCursorKeyringV1;
    use tracedecay_domain::{
        CalibrationProfileId, DiversityPolicy, FusionProfile, RetrievalAnchorId, RetrievalBudget,
        RetrievalScope, RetrievalSnapshot, RetrieverKind, ScoreDomainCalibrationV1,
        SingleRootScopeV1, TemporalModeV1, VectorWatermark,
    };

    fn id<T>(value: &str) -> T
    where
        T: TryFrom<String>,
        <T as TryFrom<String>>::Error: fmt::Debug,
    {
        T::try_from(value.to_owned()).expect("valid fixture identity")
    }

    fn digest(label: &str) -> ManifestDigest {
        canonical_sha256(&label).expect("valid fixture digest")
    }

    fn budget() -> RetrievalBudget {
        RetrievalBudget {
            max_candidates_per_lane: 32,
            max_fused_candidates: 16,
            max_hydrated_results: 8,
            max_hydration_bytes: 65_536,
            deadline_micros: None,
        }
    }

    fn request() -> RetrievalRequest {
        RetrievalRequest {
            principal: id("principal.clone-cursor"),
            scope: RetrievalScope {
                privacy_domain: id("privacy.clone-cursor"),
                root: SingleRootScopeV1 {
                    repository: id("repository.clone-cursor"),
                    worktree: None,
                    reference: None,
                },
            },
            temporal_mode: TemporalModeV1::Current,
            snapshot: RetrievalSnapshot {
                watermarks: VectorWatermark::default(),
                freshness_digest: digest("freshness.clone-cursor"),
                authorization_revision: id("authorization.clone-cursor.v1"),
                captured_at: UtcMicros(7),
            },
            profile_id: id("profile.clone-cursor.v1"),
            budget: budget(),
        }
    }

    fn profile() -> FusionProfile {
        let lanes = RetrieverKind::QUERY_FALLBACK_LANES;
        FusionProfile {
            profile_id: id("profile.clone-cursor.v1"),
            evaluation_result_anchor: RetrievalAnchorId::new("evaluation.clone-cursor")
                .expect("evaluation anchor"),
            calibrations: lanes
                .into_iter()
                .map(|lane| {
                    (
                        lane,
                        id::<CalibrationProfileId>(&format!(
                            "calibration.{}.clone-cursor.v1",
                            lane.as_str()
                        )),
                    )
                })
                .collect(),
            score_domain_calibrations: lanes
                .into_iter()
                .map(|lane| {
                    let score_domain: tracedecay_domain::ScoreDomainId =
                        id(&format!("score.{}.clone-cursor.v1", lane.as_str()));
                    (
                        score_domain.clone(),
                        ScoreDomainCalibrationV1 {
                            calibration_profile_id: id(&format!(
                                "calibration.{}.clone-cursor.v1",
                                lane.as_str()
                            )),
                            score_domain,
                            raw_min_micros: 0,
                            raw_max_micros: 1_000_000,
                        },
                    )
                })
                .collect(),
            minimum_calibrated_feature_micros: BTreeMap::new(),
            weights_micros: [
                (RetrieverKind::ExactLiteral, 1_000_000),
                (RetrieverKind::Lexical, 500_000),
                (RetrieverKind::Graph, 250_000),
            ]
            .into_iter()
            .collect(),
            diversity_policy_id: id("diversity.clone-cursor.v1"),
            retrieval_budget: budget(),
        }
    }

    fn authority(request: &RetrievalRequest) -> QueryAuthorityV1 {
        let keyring = RetrievalCursorKeyringV1::new(
            request.scope.privacy_domain.clone(),
            id("retrieval-key.clone-cursor.v1"),
            1,
            vec![7_u8; 32],
            CLONE_CURSOR_TTL_MICROS_V1 as u64,
        )
        .expect("keyring");
        QueryAuthorityV1::new(
            profile(),
            DiversityPolicy {
                policy_id: id("diversity.clone-cursor.v1"),
                evaluation_result_anchor: Some(
                    RetrievalAnchorId::new("evaluation.clone-cursor").expect("anchor"),
                ),
                per_source_namespace: None,
                per_source_instance: None,
                per_repository: None,
                per_file: None,
                per_session_or_thread: None,
                per_copy_cluster: None,
                per_evidence_role: None,
            },
            id("ranking.clone-cursor.v1"),
            keyring,
        )
        .expect("query authority")
    }

    fn codec<'a>(
        request: &'a RetrievalRequest,
        authority: &'a QueryAuthorityV1,
    ) -> CloneCursorCodecV1<'a> {
        CloneCursorCodecV1::new(authority, request).expect("clone cursor codec")
    }

    fn artifact_args() -> (
        ManifestDigest,
        CodeGenerationId,
        ManifestDigest,
        ManifestDigest,
    ) {
        (
            digest("artifact.clone-cursor"),
            id("generation.clone-cursor.v1"),
            digest("snapshot.clone-cursor"),
            digest("descriptor.clone-cursor"),
        )
    }

    #[test]
    fn artifact_cursor_round_trips_through_the_retrieval_hmac_authority() {
        let request = request();
        let authority = authority(&request);
        let codec = codec(&request, &authority);
        let (artifact, generation, snapshot, descriptor) = artifact_args();
        let encoded = codec
            .issue_artifact(
                artifact.clone(),
                generation.clone(),
                snapshot.clone(),
                descriptor.clone(),
                CloneArtifactCursorPositionV2::Exact {
                    symbol_occurrence_id: id("occurrence.clone-cursor"),
                },
                UtcMicros(100),
            )
            .expect("signed artifact cursor");

        assert!(encoded.starts_with(CLONE_CURSOR_PREFIX_V2));
        assert_eq!(
            codec
                .decode_artifact(
                    &encoded,
                    &artifact,
                    &generation,
                    &snapshot,
                    &descriptor,
                    UtcMicros(101),
                )
                .expect("verified artifact cursor")
                .after,
            CloneArtifactCursorPositionV2::Exact {
                symbol_occurrence_id: id("occurrence.clone-cursor"),
            }
        );
    }

    #[test]
    fn fingerprint_discovery_cursor_round_trips_without_a_comparison_key() {
        let request = request();
        let authority = authority(&request);
        let codec = codec(&request, &authority);
        let (artifact, generation, snapshot, descriptor) = artifact_args();
        let discovery = CloneFingerprintDiscoveryPositionV2 {
            posting_count: 31,
            fingerprint: 7,
            symbol_occurrence_id: Some(id("occurrence.discovery")),
            token_position: Some(4),
        };
        let encoded = codec
            .issue_artifact(
                artifact.clone(),
                generation.clone(),
                snapshot.clone(),
                descriptor.clone(),
                CloneArtifactCursorPositionV2::FingerprintDiscovery {
                    discovery: discovery.clone(),
                    comparison_body_digest: None,
                    comparison_payload_digest: None,
                },
                UtcMicros(100),
            )
            .expect("signed discovery cursor");
        let decoded = codec
            .decode_artifact(
                &encoded,
                &artifact,
                &generation,
                &snapshot,
                &descriptor,
                UtcMicros(101),
            )
            .expect("verified discovery cursor");
        assert_eq!(
            decoded.after,
            CloneArtifactCursorPositionV2::FingerprintDiscovery {
                discovery,
                comparison_body_digest: None,
                comparison_payload_digest: None,
            }
        );
    }

    #[test]
    fn family_cursor_round_trips_and_cannot_be_decoded_as_an_artifact_cursor() {
        let request = request();
        let authority = authority(&request);
        let codec = codec(&request, &authority);
        let (artifact, generation, snapshot, descriptor) = artifact_args();
        let encoded = codec
            .issue_family(
                artifact.clone(),
                generation.clone(),
                snapshot.clone(),
                descriptor.clone(),
                CloneFamilyCursorPositionV2 {
                    reviewable_source_bytes: 400,
                    member_count: 3,
                    class: CloneNormalizationClassV1::Rename,
                    normalization_revision: 2,
                    digest: digest("family.clone-cursor"),
                    scan_after: None,
                    scan_after_occurrence: None,
                    minimum_source_bytes: 0,
                    representative: None,
                    has_pull_request_member: false,
                },
                UtcMicros(100),
            )
            .expect("signed family cursor");

        let family = codec
            .decode_family(
                &encoded,
                &artifact,
                &generation,
                &snapshot,
                &descriptor,
                UtcMicros(101),
            )
            .expect("verified family cursor");
        assert_eq!(family.after.member_count, 3);
        assert_eq!(
            codec.decode_artifact(
                &encoded,
                &artifact,
                &generation,
                &snapshot,
                &descriptor,
                UtcMicros(101),
            ),
            Err(CloneCursorErrorV1::Invalid)
        );
    }

    #[test]
    fn authenticated_cursor_rejects_tampering_and_preserves_stale_binding_failures() {
        let request = request();
        let authority = authority(&request);
        let codec = codec(&request, &authority);
        let (artifact, generation, snapshot, descriptor) = artifact_args();
        let encoded = codec
            .issue_artifact(
                artifact.clone(),
                generation.clone(),
                snapshot.clone(),
                descriptor.clone(),
                CloneArtifactCursorPositionV2::Fingerprint {
                    body_digest: digest("body.clone-cursor"),
                    payload_digest: digest("payload.clone-cursor"),
                },
                UtcMicros(100),
            )
            .expect("signed artifact cursor");

        let mut wire: serde_json::Value = serde_json::from_slice(
            &hex::decode(encoded.strip_prefix(CLONE_CURSOR_PREFIX_V2).unwrap()).unwrap(),
        )
        .unwrap();
        wire["payload"]["generation"] = serde_json::json!("generation.forged");
        let forged = format!(
            "{CLONE_CURSOR_PREFIX_V2}{}",
            hex::encode(serde_json::to_vec(&wire).unwrap())
        );
        assert_eq!(
            codec.decode_artifact(
                &forged,
                &artifact,
                &generation,
                &snapshot,
                &descriptor,
                UtcMicros(101),
            ),
            Err(CloneCursorErrorV1::Tampered)
        );

        let wrong_generation = id("generation.clone-cursor.v2");
        assert_eq!(
            codec.decode_artifact(
                &encoded,
                &artifact,
                &wrong_generation,
                &snapshot,
                &descriptor,
                UtcMicros(101),
            ),
            Err(CloneCursorErrorV1::Stale)
        );
        let wrong_descriptor = digest("descriptor.clone-cursor.v2");
        assert_eq!(
            codec.decode_artifact(
                &encoded,
                &artifact,
                &generation,
                &snapshot,
                &wrong_descriptor,
                UtcMicros(101),
            ),
            Err(CloneCursorErrorV1::Stale)
        );
        assert_eq!(
            codec.decode_artifact(
                &encoded,
                &artifact,
                &generation,
                &snapshot,
                &descriptor,
                UtcMicros(900_000_101),
            ),
            Err(CloneCursorErrorV1::Stale)
        );

        let mut changed_authorization = request.clone();
        changed_authorization.snapshot.authorization_revision = id("authorization.clone-cursor.v2");
        let changed_codec = codec(&changed_authorization, &authority);
        assert_eq!(
            changed_codec.decode_artifact(
                &encoded,
                &artifact,
                &generation,
                &snapshot,
                &descriptor,
                UtcMicros(101),
            ),
            Err(CloneCursorErrorV1::Tampered)
        );
    }

    #[test]
    fn legacy_hex_json_cursor_is_rejected_before_authority_use() {
        let request = request();
        let authority = authority(&request);
        let codec = codec(&request, &authority);
        let (artifact, generation, snapshot, descriptor) = artifact_args();
        let legacy = hex::encode(
            serde_json::to_vec(&serde_json::json!({
                "artifact_digest": artifact,
                "generation": generation,
                "request_digest": descriptor,
                "after": {"Exact": "occurrence.clone-cursor"}
            }))
            .unwrap(),
        );
        assert_eq!(
            codec.decode_artifact(
                &legacy,
                &digest("artifact.clone-cursor"),
                &id("generation.clone-cursor.v1"),
                &snapshot,
                &descriptor,
                UtcMicros(101),
            ),
            Err(CloneCursorErrorV1::Invalid)
        );
    }
}
