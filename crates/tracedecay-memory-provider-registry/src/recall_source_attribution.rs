//! Strict wire projections of canonical original-source attribution. Parsing
//! proves structure only; the host must resolve original evidence and current
//! disposition before granting cross-session reuse or hydrated provenance.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracedecay_memory_provider_api::{
    OriginScopeEvidence, OriginalSourceIdentity, OwnedExactScope, OwnedProviderId,
    RecordedValidity, SourceAttribution,
};

use super::{RecallDenialReason, RecallOutcomeScopeV1, parse_rfc3339_nanos, required_nullable};

fn malformed(detail: &'static str) -> RecallDenialReason {
    RecallDenialReason::InvalidSourceAttribution {
        detail: detail.to_owned(),
    }
}

fn timestamp(value: &str) -> Result<i64, RecallDenialReason> {
    parse_rfc3339_nanos(value)
        .ok_or_else(|| malformed("source timestamp is not representable UTC RFC3339 nanoseconds"))
}

fn optional_timestamp(value: Option<&String>) -> Result<Option<i64>, RecallDenialReason> {
    value.map(|value| timestamp(value)).transpose()
}

/// Original canonical identity on the shared wire. Required nullable fields
/// preserve missing legacy evidence as null, never as an invented revision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecallOriginalSourceIdentityV1 {
    /// Original canonical source provider.
    pub canonical_provider_id: String,
    /// Original canonical source session.
    pub canonical_session_id: String,
    /// Original host source/deletion key.
    pub source_key: String,
    /// Retained native stable source record, if available.
    #[serde(deserialize_with = "required_nullable")]
    pub stable_record_id: Option<String>,
    /// Canonical observation identity.
    pub observation_id: String,
    /// Opaque canonical source revision, independent of envelope version.
    #[serde(deserialize_with = "required_nullable")]
    pub source_revision: Option<String>,
    /// Original canonical source-content digest.
    pub content_sha256: String,
}

impl RecallOriginalSourceIdentityV1 {
    /// Projects the wire fields through the owned API's source invariants.
    pub fn to_owned_source(&self) -> Result<OriginalSourceIdentity, RecallDenialReason> {
        let source = OriginalSourceIdentity {
            canonical_provider_id: OwnedProviderId::new(&self.canonical_provider_id)
                .map_err(|_| malformed("canonical source provider identity is invalid"))?,
            canonical_session_id: self.canonical_session_id.clone(),
            source_key: self.source_key.clone(),
            stable_record_id: self.stable_record_id.clone(),
            observation_id: self.observation_id.clone(),
            source_revision: self.source_revision.clone(),
            content_sha256: self.content_sha256.clone(),
        };
        source
            .validate()
            .map_err(|_| malformed("original source identity is invalid"))?;
        Ok(source)
    }
}

/// Explicit source-origin evidence. Recorded scope is still a provider claim
/// until the host verifies the original marker/observation authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum RecallOriginScopeEvidenceV1 {
    /// Exact original scope recorded during original source admission.
    Recorded {
        /// Immutable original source scope, separate from delivery scope.
        exact_scope_identity: RecallOutcomeScopeV1,
        /// Reference to the existing host's original evidence authority.
        authority_ref: String,
    },
    /// Only ingestion-time checkout evidence exists.
    IngestionOnly,
    /// Legacy source origin evidence is unavailable.
    Unavailable,
}

impl RecallOriginScopeEvidenceV1 {
    /// Validates the structural claim without admitting its scope relation.
    pub fn to_owned_evidence(&self) -> Result<OriginScopeEvidence, RecallDenialReason> {
        let evidence = match self {
            Self::Recorded {
                exact_scope_identity: scope,
                authority_ref,
            } => OriginScopeEvidence::Recorded {
                scope: OwnedExactScope::new(
                    &scope.profile_id,
                    &scope.project_id,
                    &scope.repository_identity,
                    &scope.worktree_identity,
                    &scope.branch_identity,
                    &scope.agent_session_id,
                    &scope.resolved_scope_digest,
                )
                .map_err(|_| malformed("original source scope is malformed"))?,
                authority_ref: authority_ref.clone(),
            },
            Self::IngestionOnly => OriginScopeEvidence::IngestionOnly,
            Self::Unavailable => OriginScopeEvidence::Unavailable,
        };
        evidence
            .validate()
            .map_err(|_| malformed("original scope evidence is malformed"))?;
        Ok(evidence)
    }
}

/// Retained original assertion validity, without a synthesized temporal state.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecallRecordedValidityV1 {
    /// Inclusive recorded validity start.
    #[serde(deserialize_with = "required_nullable")]
    pub valid_from: Option<String>,
    /// Exclusive recorded validity end.
    #[serde(deserialize_with = "required_nullable")]
    pub valid_until: Option<String>,
    /// Recorded supersession instant.
    #[serde(deserialize_with = "required_nullable")]
    pub superseded_at: Option<String>,
    /// Stable replacement paired with the supersession instant.
    #[serde(deserialize_with = "required_nullable")]
    pub superseded_by: Option<String>,
    /// Recorded ordinary revocation, distinct from current privacy authority.
    #[serde(deserialize_with = "required_nullable")]
    pub revoked_at: Option<String>,
}

impl RecallRecordedValidityV1 {
    /// Preserves nanosecond timestamps exactly while applying shared invariants.
    pub fn to_owned_validity(&self) -> Result<RecordedValidity, RecallDenialReason> {
        let validity = RecordedValidity {
            valid_from_utc_nanos: optional_timestamp(self.valid_from.as_ref())?,
            valid_until_utc_nanos: optional_timestamp(self.valid_until.as_ref())?,
            superseded_at_utc_nanos: optional_timestamp(self.superseded_at.as_ref())?,
            superseded_by: self.superseded_by.clone(),
            revoked_at_utc_nanos: optional_timestamp(self.revoked_at.as_ref())?,
        };
        validity
            .validate()
            .map_err(|_| malformed("source validity interval or lineage is malformed"))?;
        Ok(validity)
    }
}

/// Canonical `provenance.original_sources` element. This is the exact shared
/// `sourceAttribution` schema, including original scope and distinct clocks.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecallSourceAttributionV1 {
    /// Original source identity and actual nullable revision.
    pub source: RecallOriginalSourceIdentityV1,
    /// Recorded original scope or explicit unavailable evidence.
    pub origin_scope: RecallOriginScopeEvidenceV1,
    /// Source-local ordering, independent of time and destination delivery.
    pub source_sequence: u64,
    /// Original occurrence time, if retained.
    #[serde(deserialize_with = "required_nullable")]
    pub occurred_at: Option<String>,
    /// Actual canonical ingestion time.
    pub ingested_at: String,
    /// Retained assertion validity and lineage.
    pub validity: RecallRecordedValidityV1,
}

impl RecallSourceAttributionV1 {
    /// Validates and projects source claims to owned runtime values. This does
    /// not confirm source existence, grant history, or confer privacy authority.
    pub fn to_owned_attribution(&self) -> Result<SourceAttribution, RecallDenialReason> {
        let attribution = SourceAttribution {
            source: self.source.to_owned_source()?,
            origin_scope: self.origin_scope.to_owned_evidence()?,
            source_sequence: self.source_sequence,
            occurred_at_utc_nanos: optional_timestamp(self.occurred_at.as_ref())?,
            ingested_at_utc_nanos: timestamp(&self.ingested_at)?,
            validity: self.validity.to_owned_validity()?,
        };
        attribution
            .validate()
            .map_err(|_| malformed("source attribution is malformed"))?;
        Ok(attribution)
    }
}

/// Reads the optional canonical field from legacy-compatible provenance. Every
/// present item is strictly decoded and validated; absent metadata grants no
/// authority and produces an empty typed attribution slice.
pub(super) fn original_sources_from_provenance(
    provenance: &Value,
) -> Result<Vec<RecallSourceAttributionV1>, RecallDenialReason> {
    let Some(value) = provenance.get("original_sources") else {
        return Ok(Vec::new());
    };
    let Some(items) = value.as_array() else {
        return Err(malformed("original_sources must be an array"));
    };
    if items.is_empty() || items.len() > 64 {
        return Err(malformed("original_sources must contain 1 to 64 sources"));
    }
    let sources: Vec<RecallSourceAttributionV1> = serde_json::from_value(value.clone())
        .map_err(|_| malformed("original_sources does not match the canonical source schema"))?;
    let mut seen = BTreeSet::new();
    for attribution in &sources {
        attribution.to_owned_attribution()?;
        let source = &attribution.source;
        if !seen.insert((
            source.canonical_provider_id.as_str(),
            source.canonical_session_id.as_str(),
            source.source_key.as_str(),
            source.observation_id.as_str(),
            source.source_revision.as_deref(),
        )) {
            return Err(malformed("original_sources repeats a source revision"));
        }
    }
    Ok(sources)
}
