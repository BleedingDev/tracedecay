//! Owned common advisory values. These are runtime projections of the canonical
//! contracts, not a second serialized protocol or an authorization authority.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use crate::contract::{HistoryRelation, SourceDisposition, TemporalMode, UnknownValidityPolicy};
use crate::{ApiError, OwnedExactScope, OwnedProviderId, require_sha256};

/// Invalid common advisory attribution, temporal constraint, or lifecycle value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdvisoryContractError {
    /// An existing API identity or digest invariant failed.
    Boundary(ApiError),
    /// A bounded value is malformed or inconsistent.
    Invalid(&'static str),
    /// Origin evidence cannot authorize a historical source.
    OriginUnavailable,
    /// A correction cannot compare an unknown or stale source revision.
    RevisionConflict,
}

impl fmt::Display for AdvisoryContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Boundary(error) => error.fmt(f),
            Self::Invalid(field) => write!(f, "invalid common advisory {field}"),
            Self::OriginUnavailable => f.write_str("original source scope is not recorded"),
            Self::RevisionConflict => f.write_str("source revision is unknown or does not match"),
        }
    }
}

impl Error for AdvisoryContractError {}

impl From<ApiError> for AdvisoryContractError {
    fn from(value: ApiError) -> Self {
        Self::Boundary(value)
    }
}

fn reference(value: &str, field: &'static str) -> Result<(), AdvisoryContractError> {
    if value.is_empty()
        || value.trim() != value
        || value.len() > 1024
        || value.chars().any(char::is_control)
    {
        return Err(AdvisoryContractError::Invalid(field));
    }
    Ok(())
}

/// Immutable canonical source identity, independent of destination provider,
/// delivery key, request candidate ID, and envelope schema version.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OriginalSourceIdentity {
    /// Canonical observation provider identity, preserved from its source.
    pub canonical_provider_id: OwnedProviderId,
    /// Original canonical session identity.
    pub canonical_session_id: String,
    /// Original host source key used to fan out deletion to every copied effect.
    pub source_key: String,
    /// Native provider's stable record identity, if retained by the source.
    pub stable_record_id: Option<String>,
    /// Canonical observation identity; never a request-scoped candidate ID.
    pub observation_id: String,
    /// Actual source content revision. Legacy envelope versions remain unknown.
    pub source_revision: Option<String>,
    /// Lowercase SHA-256 of the canonical JSON bytes of the original host-retained
    /// canonical source payload. Preserve this host-validated digest: projected
    /// message text and the payload after hygiene sanitization cannot reconstruct
    /// it. Delivered envelopes and emitted candidate/trace UTF-8 content have
    /// separate digests.
    pub content_sha256: String,
}

impl OriginalSourceIdentity {
    /// Validates syntax without resolving or granting authority over a source.
    pub fn validate(&self) -> Result<(), AdvisoryContractError> {
        reference(&self.canonical_session_id, "canonical_session_id")?;
        reference(&self.source_key, "source_key")?;
        reference(&self.observation_id, "observation_id")?;
        if let Some(record) = &self.stable_record_id {
            reference(record, "stable_record_id")?;
        }
        if let Some(revision) = &self.source_revision {
            reference(revision, "source_revision")?;
        }
        require_sha256(&self.content_sha256, "original_source.content_sha256")?;
        Ok(())
    }
}

/// Retained evidence of the original source scope. Capturing a checkout while
/// importing an old transcript does not prove that transcript's original scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OriginScopeEvidence {
    /// Scope recorded by an authoritative original-source admission.
    Recorded {
        /// Immutable original exact scope, never replaced by delivery scope.
        scope: OwnedExactScope,
        /// Existing host marker/observation evidence supporting this capture.
        authority_ref: String,
    },
    /// Only the ingestion checkout was captured; unavailable as origin proof.
    IngestionOnly,
    /// Legacy source has no retained origin scope evidence.
    Unavailable,
}

impl OriginScopeEvidence {
    /// Validates retained metadata without turning it into host authorization.
    pub fn validate(&self) -> Result<(), AdvisoryContractError> {
        if let Self::Recorded {
            scope,
            authority_ref,
        } = self
        {
            scope.validate()?;
            reference(authority_ref, "origin_scope.authority_ref")?;
        }
        Ok(())
    }

    /// Returns recorded origin scope, refusing missing or ingestion-only proof.
    pub fn recorded_scope(&self) -> Result<&OwnedExactScope, AdvisoryContractError> {
        self.validate()?;
        match self {
            Self::Recorded { scope, .. } => Ok(scope),
            Self::IngestionOnly | Self::Unavailable => {
                Err(AdvisoryContractError::OriginUnavailable)
            }
        }
    }
}

/// Retained assertion validity in UTC nanoseconds. A missing start is unknown,
/// not the ingestion clock or the occurrence timestamp. End is exclusive.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RecordedValidity {
    /// Inclusive start supported by retained source evidence.
    pub valid_from_utc_nanos: Option<i64>,
    /// Exclusive end supported by retained source evidence.
    pub valid_until_utc_nanos: Option<i64>,
    /// Recorded supersession event time.
    pub superseded_at_utc_nanos: Option<i64>,
    /// Stable replacement identity, retained with a supersession event.
    pub superseded_by: Option<String>,
    /// Ordinary revocation event time; this is distinct from privacy deletion.
    pub revoked_at_utc_nanos: Option<i64>,
}

impl RecordedValidity {
    /// Checks interval and lineage consistency without fabricating missing time.
    pub fn validate(&self) -> Result<(), AdvisoryContractError> {
        if let (Some(start), Some(end)) = (self.valid_from_utc_nanos, self.valid_until_utc_nanos)
            && start >= end
        {
            return Err(AdvisoryContractError::Invalid("validity interval"));
        }
        if self.superseded_at_utc_nanos.is_some() != self.superseded_by.is_some() {
            return Err(AdvisoryContractError::Invalid("supersession lineage"));
        }
        if let Some(replacement) = &self.superseded_by {
            reference(replacement, "superseded_by")?;
        }
        for event in [self.superseded_at_utc_nanos, self.revoked_at_utc_nanos]
            .into_iter()
            .flatten()
        {
            if self.valid_from_utc_nanos.is_some_and(|start| event < start) {
                return Err(AdvisoryContractError::Invalid("validity event time"));
            }
        }
        Ok(())
    }

    /// Evaluates retained evidence for one canonical query. Canonical privacy
    /// disposition and a provider's privacy fence dominate every history flag.
    /// Unknown eligibility must be reflected in response coverage by the caller.
    pub fn eligibility(
        &self,
        query: &OwnedTemporalQuery,
        disposition: SourceDisposition,
        provider_privacy_deleted: bool,
    ) -> Result<TemporalEligibility, AdvisoryContractError> {
        self.validate()?;
        query.validate()?;
        if provider_privacy_deleted
            || matches!(
                disposition,
                SourceDisposition::Deleted
                    | SourceDisposition::Redacted
                    | SourceDisposition::Expired
            )
        {
            return Ok(TemporalEligibility::Excluded);
        }
        if disposition == SourceDisposition::Unknown {
            // Missing current privacy evidence can never authorize source content.
            return Ok(TemporalEligibility::WithheldUnknown);
        }
        // A current status alone cannot locate a supersession/revocation in
        // history. An event time must be retained before historical admission.
        if (disposition == SourceDisposition::Superseded && self.superseded_at_utc_nanos.is_none())
            || (disposition == SourceDisposition::Revoked && self.revoked_at_utc_nanos.is_none())
        {
            return Ok(TemporalEligibility::WithheldUnknown);
        }
        let mut end = self.valid_until_utc_nanos;
        if !query.include_superseded {
            end = minimum_time(end, self.superseded_at_utc_nanos);
        }
        if !query.include_revoked {
            end = minimum_time(end, self.revoked_at_utc_nanos);
        }
        let Some(start) = self.valid_from_utc_nanos else {
            let earliest_requested = match query.mode {
                TemporalMode::Current | TemporalMode::History => {
                    Some(query.evaluation_time_utc_nanos)
                }
                TemporalMode::AsOf => query.as_of_utc_nanos,
                TemporalMode::Interval => query.interval_start_utc_nanos,
            };
            if matches!((earliest_requested, end), (Some(at), Some(until)) if at >= until) {
                return Ok(TemporalEligibility::Excluded);
            }
            return Ok(match query.unknown_validity_policy {
                UnknownValidityPolicy::Degrade | UnknownValidityPolicy::AllowWithWarning => {
                    TemporalEligibility::IncludedUnknown
                }
                UnknownValidityPolicy::Exclude => TemporalEligibility::WithheldUnknown,
            });
        };
        if query.mode != TemporalMode::History && end.is_some_and(|end| end <= start) {
            return Ok(TemporalEligibility::Excluded);
        }
        let eligible = match query.mode {
            TemporalMode::Current => contains_time(start, end, query.evaluation_time_utc_nanos),
            TemporalMode::AsOf => query
                .as_of_utc_nanos
                .is_some_and(|at| contains_time(start, end, at)),
            TemporalMode::Interval => {
                match (query.interval_start_utc_nanos, query.interval_end_utc_nanos) {
                    (Some(from), Some(until)) => start < until && end.is_none_or(|end| end > from),
                    _ => false,
                }
            }
            TemporalMode::History => {
                start <= query.evaluation_time_utc_nanos
                    && (query.include_superseded
                        || self
                            .superseded_at_utc_nanos
                            .is_none_or(|at| at > query.evaluation_time_utc_nanos))
                    && (query.include_revoked
                        || self
                            .revoked_at_utc_nanos
                            .is_none_or(|at| at > query.evaluation_time_utc_nanos))
            }
        };
        Ok(if eligible {
            TemporalEligibility::Eligible
        } else {
            TemporalEligibility::Excluded
        })
    }
}

fn minimum_time(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (left, right) => left.or(right),
    }
}

fn contains_time(start: i64, end: Option<i64>, at: i64) -> bool {
    start <= at && end.is_none_or(|end| at < end)
}

/// Result of temporal admission, separate from search coverage and ranking.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TemporalEligibility {
    /// Retained validity covers the requested time.
    Eligible,
    /// The source is outside the admitted interval or is privacy-forbidden.
    Excluded,
    /// Unknown evidence prevents admission and requires explicit partial coverage.
    WithheldUnknown,
    /// Unknown validity was explicitly allowed; warnings and partial coverage remain required.
    IncludedUnknown,
}

/// Canonical temporal query with generated closed mode and policy values.
/// Host wire adapters convert RFC3339 UTC timestamps without inferring clocks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedTemporalQuery {
    /// Canonical current/as-of/interval/history selection.
    pub mode: TemporalMode,
    /// Host-admitted evaluation clock in UTC nanoseconds.
    pub evaluation_time_utc_nanos: i64,
    /// Required exactly for as-of mode.
    pub as_of_utc_nanos: Option<i64>,
    /// Required inclusive interval start exactly for interval mode.
    pub interval_start_utc_nanos: Option<i64>,
    /// Required exclusive interval end exactly for interval mode.
    pub interval_end_utc_nanos: Option<i64>,
    /// Permit superseded historical evidence, never privacy-deleted sources.
    pub include_superseded: bool,
    /// Permit ordinarily revoked evidence, never privacy-deleted sources.
    pub include_revoked: bool,
    /// Explicit unknown-validity policy from the canonical contract.
    pub unknown_validity_policy: UnknownValidityPolicy,
}

impl OwnedTemporalQuery {
    /// Current-time query with the canonical conservative defaults.
    #[must_use]
    pub fn current(evaluation_time_utc_nanos: i64) -> Self {
        Self {
            mode: TemporalMode::Current,
            evaluation_time_utc_nanos,
            as_of_utc_nanos: None,
            interval_start_utc_nanos: None,
            interval_end_utc_nanos: None,
            include_superseded: false,
            include_revoked: false,
            unknown_validity_policy: UnknownValidityPolicy::Exclude,
        }
    }

    /// Validates mode-specific bounds without reading a clock.
    pub fn validate(&self) -> Result<(), AdvisoryContractError> {
        let valid = match self.mode {
            TemporalMode::Current | TemporalMode::History => {
                self.as_of_utc_nanos.is_none()
                    && self.interval_start_utc_nanos.is_none()
                    && self.interval_end_utc_nanos.is_none()
            }
            TemporalMode::AsOf => {
                self.as_of_utc_nanos
                    .is_some_and(|at| at <= self.evaluation_time_utc_nanos)
                    && self.interval_start_utc_nanos.is_none()
                    && self.interval_end_utc_nanos.is_none()
            }
            TemporalMode::Interval => {
                self.as_of_utc_nanos.is_none()
                    && matches!((self.interval_start_utc_nanos, self.interval_end_utc_nanos), (Some(start), Some(end)) if start < end)
            }
        };
        if !valid {
            return Err(AdvisoryContractError::Invalid("temporal query bounds"));
        }
        Ok(())
    }

    /// Checks the request against the caller's actual admission clock.
    pub fn validate_at(&self, now_utc_nanos: i64) -> Result<(), AdvisoryContractError> {
        self.validate()?;
        if self.evaluation_time_utc_nanos > now_utc_nanos {
            return Err(AdvisoryContractError::Invalid("future evaluation time"));
        }
        Ok(())
    }
}

/// All canonical exclusion classes. Consumers apply these before truncation
/// and compare content digests against full source content, not output snippets.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OwnedRecallExclusions {
    /// Stable provider-local memory identities.
    pub stable_memory_refs: Vec<String>,
    /// Request-scoped candidate identities.
    pub candidate_ids: Vec<String>,
    /// Canonical source references.
    pub source_refs: Vec<String>,
    /// Retained trace references.
    pub trace_refs: Vec<String>,
    /// Canonical observation identities.
    pub observation_ids: Vec<String>,
    /// Lowercase SHA-256 over full original candidate content.
    pub content_sha256: Vec<String>,
}

impl OwnedRecallExclusions {
    /// Rejects oversized, duplicate, empty, and noncanonical exclusions.
    pub fn validate(&self) -> Result<(), AdvisoryContractError> {
        for (field, values) in [
            ("stable_memory_refs", &self.stable_memory_refs),
            ("candidate_ids", &self.candidate_ids),
            ("source_refs", &self.source_refs),
            ("trace_refs", &self.trace_refs),
            ("observation_ids", &self.observation_ids),
            ("content_sha256", &self.content_sha256),
        ] {
            if values.len() > 1024 {
                return Err(AdvisoryContractError::Invalid(field));
            }
            let mut seen = BTreeSet::new();
            for value in values {
                reference(value, field)?;
                if !seen.insert(value) {
                    return Err(AdvisoryContractError::Invalid(field));
                }
            }
        }
        for digest in &self.content_sha256 {
            require_sha256(digest, "exclusions.content_sha256")?;
        }
        Ok(())
    }
}

/// Original source identity, occurrence, ingestion and retained validity are
/// independent values; replay must carry them without destination relabeling.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceAttribution {
    /// Original canonical source identity.
    pub source: OriginalSourceIdentity,
    /// Explicit original scope evidence or its absence.
    pub origin_scope: OriginScopeEvidence,
    /// Canonical source ordering, independent of occurrence and ingestion clocks.
    pub source_sequence: u64,
    /// Original occurrence time, unknown when not retained.
    pub occurred_at_utc_nanos: Option<i64>,
    /// Actual canonical ingestion time, not reconstructed original validity.
    pub ingested_at_utc_nanos: i64,
    /// Recorded assertion validity and lineage.
    pub validity: RecordedValidity,
}

impl SourceAttribution {
    /// Validates the retained values without promoting them to host proof.
    pub fn validate(&self) -> Result<(), AdvisoryContractError> {
        self.source.validate()?;
        self.origin_scope.validate()?;
        self.validity.validate()
    }
}

/// Current canonical source disposition read through the existing host authority.
/// Its revision has no ordering relationship with provider state generations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentSourceDisposition {
    /// Current canonical disposition, distinct from provider-local deletion.
    pub state: SourceDisposition,
    /// Existing authoritative disposition/checkpoint reference.
    pub authority_ref: String,
    /// Authority-local revision, if that authority exposes one.
    pub authority_revision: Option<u64>,
    /// Time of the actual current disposition read, not snapshot creation time.
    pub checked_at_utc_nanos: i64,
}

impl CurrentSourceDisposition {
    /// Validates structural fields; freshness must be checked by the host port.
    pub fn validate(&self) -> Result<(), AdvisoryContractError> {
        reference(&self.authority_ref, "current_disposition.authority_ref")
    }
}

/// One original source covered by a host-admitted history relation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GrantedHistorySource {
    /// Original source, origin evidence and retained time/validity metadata.
    pub attribution: SourceAttribution,
    /// Current canonical disposition, including deletions needed by replay.
    pub current_disposition: CurrentSourceDisposition,
}

/// Typed host claim for bounded historical source delivery. Construction and
/// hashing never prove authorization. Before dispatch the host must validate
/// the marker/observation identity bridge, source evidence, and current
/// disposition through its existing ports, then revalidate at use time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryGrant {
    /// Host-owned admission/authorization reference.
    pub authorization_ref: String,
    /// Host policy revision under which the relation was admitted.
    pub policy_revision: u64,
    /// Full exact destination call scope; original source scope stays separate.
    pub destination_scope: OwnedExactScope,
    /// Host-admitted exact-scope or same-checkout relation.
    pub relation: HistoryRelation,
    /// Bounded original sources and their current canonical disposition.
    pub sources: Vec<GrantedHistorySource>,
    /// Current authoritative disposition checkpoint, revalidated by the host.
    pub disposition_checkpoint: RestoreDispositionCheckpoint,
}

impl HistoryGrant {
    /// Checks structure and retained original scope, not the authority claim.
    /// Identity bridges between canonical-source and daemon namespaces must be
    /// validated by the host; comparing or rewriting prefixes cannot do that.
    pub fn validate_structure(&self) -> Result<(), AdvisoryContractError> {
        reference(&self.authorization_ref, "history.authorization_ref")?;
        if self.policy_revision == 0 || self.sources.is_empty() || self.sources.len() > 4096 {
            return Err(AdvisoryContractError::Invalid("history grant bounds"));
        }
        self.destination_scope.validate()?;
        self.disposition_checkpoint
            .validate_for(&self.destination_scope)?;
        let mut seen = BTreeSet::new();
        for source in &self.sources {
            source.attribution.validate()?;
            let original = source.attribution.origin_scope.recorded_scope()?;
            if self.relation == HistoryRelation::ExactScope && original != &self.destination_scope {
                return Err(AdvisoryContractError::Invalid("exact history relation"));
            }
            source.current_disposition.validate()?;
            let identity = &source.attribution.source;
            if !seen.insert((
                identity.canonical_provider_id.as_str(),
                identity.canonical_session_id.as_str(),
                identity.source_key.as_str(),
                identity.observation_id.as_str(),
                identity.source_revision.as_deref(),
            )) {
                return Err(AdvisoryContractError::Invalid("duplicate history source"));
            }
        }
        Ok(())
    }
}

/// Current host disposition revalidation required before restored state can
/// become ready. An unchanged authoritative revision can be valid; this is
/// neither a provider generation nor a claim of verified provider erasure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreDispositionCheckpoint {
    /// Exact destination scope of the current host disposition read.
    pub exact_scope: OwnedExactScope,
    /// Existing canonical disposition/host deletion journal checkpoint reference.
    pub authority_ref: String,
    /// Revision in that authority's own ordering, if available.
    pub authority_revision: Option<u64>,
    /// Time the host actually revalidated its current source state.
    pub checked_at_utc_nanos: i64,
}

impl RestoreDispositionCheckpoint {
    /// Checks scope and shape. The calling host must still establish freshness
    /// against its current disposition authority immediately before readiness.
    pub fn validate_for(&self, destination: &OwnedExactScope) -> Result<(), AdvisoryContractError> {
        self.exact_scope.validate()?;
        destination.validate()?;
        reference(&self.authority_ref, "disposition_checkpoint.authority_ref")?;
        if &self.exact_scope != destination {
            return Err(AdvisoryContractError::Invalid(
                "disposition checkpoint scope",
            ));
        }
        Ok(())
    }
}

/// Existing stable or retained attribution references allowed by lifecycle
/// operations. A request candidate ID is deliberately not a target variant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LifecycleTargetReference {
    /// Stable provider memory reference preserved across admitted requests.
    StableMemoryRef(String),
    /// Retained recall trace that resolves to the original candidate/source.
    RecallTraceRef(String),
    /// Retained context-pack item that resolves to the producing provider.
    ContextPackItemRef(String),
}

/// Provider-local lifecycle target. A provider switch cannot redirect this
/// target; the host resolves retained attribution before admitting a control.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LifecycleTarget {
    /// Producing provider identity, not the currently selected provider.
    pub provider_id: OwnedProviderId,
    /// Producing registration revision retained with the candidate attribution.
    pub registration_revision: u64,
    /// Retained original scope evidence; legacy control targets may lack it.
    pub original_scope: OriginScopeEvidence,
    /// Exact scope in which the provider produced the retained target.
    pub delivery_scope: OwnedExactScope,
    /// Original source and actual source revision.
    pub source: OriginalSourceIdentity,
    /// Stable reference or retained recall/context attribution.
    pub reference: LifecycleTargetReference,
}

impl LifecycleTarget {
    /// Validates target shape; host retained attribution remains the authority.
    pub fn validate(&self) -> Result<(), AdvisoryContractError> {
        if self.registration_revision == 0 {
            return Err(AdvisoryContractError::Invalid(
                "target registration revision",
            ));
        }
        self.original_scope.validate()?;
        self.delivery_scope.validate()?;
        self.source.validate()?;
        let value = match &self.reference {
            LifecycleTargetReference::StableMemoryRef(value)
            | LifecycleTargetReference::RecallTraceRef(value)
            | LifecycleTargetReference::ContextPackItemRef(value) => value,
        };
        reference(value, "lifecycle target reference")
    }

    /// Checks caller attribution before dispatch; it cannot grant access to a
    /// provider or namespace the host did not independently admit.
    pub fn validate_for(
        &self,
        provider: &OwnedProviderId,
        delivery: &OwnedExactScope,
    ) -> Result<(), AdvisoryContractError> {
        self.validate()?;
        if &self.provider_id != provider || &self.delivery_scope != delivery {
            return Err(AdvisoryContractError::Invalid(
                "lifecycle target attribution",
            ));
        }
        Ok(())
    }

    /// Refuses stale or unknown source revision before applying correction.
    pub fn validate_expected_revision(&self, expected: &str) -> Result<(), AdvisoryContractError> {
        self.validate()?;
        reference(expected, "expected_target_revision")?;
        if self.source.source_revision.as_deref() != Some(expected) {
            return Err(AdvisoryContractError::RevisionConflict);
        }
        Ok(())
    }
}

/// Partition of an admitted replay page. Source already applied is separate
/// from a delivery-key duplicate whose receipt names the original commit.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReplayAccounting {
    /// Inputs admitted in this page.
    pub attempted: u64,
    /// Newly applied source observations.
    pub applied: u64,
    /// Same delivery key with retained original committing receipt.
    pub delivery_duplicates: u64,
    /// New delivery key for a source revision already applied.
    pub sources_already_applied: u64,
    /// Inputs rejected with known no effect.
    pub rejected: u64,
    /// Dispatched inputs whose commit result requires reconciliation.
    pub effect_unknown: u64,
}

impl ReplayAccounting {
    /// Requires every attempted input to have exactly one reported outcome.
    pub fn validate(&self) -> Result<(), AdvisoryContractError> {
        let total = [
            self.applied,
            self.delivery_duplicates,
            self.sources_already_applied,
            self.rejected,
            self.effect_unknown,
        ]
        .into_iter()
        .try_fold(0_u64, u64::checked_add);
        if total != Some(self.attempted) {
            return Err(AdvisoryContractError::Invalid("replay accounting"));
        }
        Ok(())
    }
}
