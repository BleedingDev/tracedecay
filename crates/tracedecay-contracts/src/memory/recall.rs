//! Transport-neutral advisory cognitive recall contracts.
//!
//! Cognitive recall is deliberately a read-only contribution to context
//! compilation.  The request carries the scope and execution controls that
//! TraceDecay admitted; the result carries only bounded, provenance-labelled
//! candidates.  Nothing in this module represents a provider store, a
//! retrieval anchor, a canonical fact, or a final context pack.

use std::collections::BTreeSet;
use std::fmt::Debug;
use std::future::Future;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracedecay_domain::UtcMicros;

use crate::context::{CancellationContext, Deadline, RequestId, ResolvedScope};
use crate::error::ApplicationContractError;

/// Maximum number of candidates that one application recall request may ask
/// an adapter to return.
pub const MAX_COGNITIVE_RECALL_CANDIDATES: usize = 128;

/// Maximum UTF-8 byte length of a recall query.
pub const MAX_COGNITIVE_RECALL_QUERY_BYTES: usize = 32 * 1024;

/// Maximum UTF-8 byte length of one inline advisory candidate.
pub const MAX_COGNITIVE_RECALL_CANDIDATE_BYTES: usize = 64 * 1024;

/// Maximum UTF-8 byte length of an opaque candidate identity or source
/// reference.  These are labels, not provider-row or retrieval-anchor IDs.
pub const MAX_COGNITIVE_RECALL_REFERENCE_BYTES: usize = 1024;

/// Maximum UTF-8 byte length of an optional provider explanation summary.
pub const MAX_COGNITIVE_RECALL_EXPLANATION_BYTES: usize = 8 * 1024;

/// Maximum exclusions in each canonical exclusion class.
pub const MAX_COGNITIVE_RECALL_EXCLUSIONS_PER_CLASS: usize = 1024;

/// Canonical temporal modes at the application boundary. Host adapters map
/// these values to the provider contract without narrowing the requested mode.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CognitiveRecallTemporalMode {
    /// Valid at the admitted evaluation time.
    Current,
    /// Valid at the specified historical time.
    AsOf,
    /// Validity overlaps the inclusive-start, exclusive-end interval.
    Interval,
    /// Retained history with explicit supersession/revocation policy.
    History,
}

/// Explicit policy for candidates with no retained assertion validity.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CognitiveRecallUnknownValidityPolicy {
    /// Withhold candidates whose validity is unknown.
    Exclude,
    /// Permit unknown validity with a warning and degraded temporal coverage.
    Degrade,
    /// Permit unknown validity with an explicit warning and partial coverage.
    AllowWithWarning,
}

/// Optional application temporal selection. Times use the application UTC
/// microsecond value; wire adapters retain the canonical RFC3339 representation.
/// Source occurrence and ingestion are never substituted for assertion validity.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CognitiveRecallTemporalQuery {
    mode: CognitiveRecallTemporalMode,
    evaluation_time: UtcMicros,
    as_of: Option<UtcMicros>,
    interval_start: Option<UtcMicros>,
    interval_end: Option<UtcMicros>,
    include_superseded: bool,
    include_revoked: bool,
    unknown_validity_policy: CognitiveRecallUnknownValidityPolicy,
}

impl CognitiveRecallTemporalQuery {
    /// Current query using the caller's admitted clock and canonical defaults.
    #[must_use]
    pub fn current(evaluation_time: UtcMicros) -> Self {
        Self {
            mode: CognitiveRecallTemporalMode::Current,
            evaluation_time,
            as_of: None,
            interval_start: None,
            interval_end: None,
            include_superseded: false,
            include_revoked: false,
            unknown_validity_policy: CognitiveRecallUnknownValidityPolicy::Exclude,
        }
    }

    /// Select a recorded historical instant, preserving the evaluation clock.
    pub fn with_as_of(mut self, as_of: UtcMicros) -> Result<Self, ApplicationContractError> {
        self.mode = CognitiveRecallTemporalMode::AsOf;
        self.as_of = Some(as_of);
        self.interval_start = None;
        self.interval_end = None;
        self.validate()?;
        Ok(self)
    }

    /// Select an inclusive-start, exclusive-end historical interval.
    pub fn with_interval(
        mut self,
        start: UtcMicros,
        end: UtcMicros,
    ) -> Result<Self, ApplicationContractError> {
        self.mode = CognitiveRecallTemporalMode::Interval;
        self.as_of = None;
        self.interval_start = Some(start);
        self.interval_end = Some(end);
        self.validate()?;
        Ok(self)
    }

    /// Select retained history using the explicit inclusion policies.
    #[must_use]
    pub fn with_history(mut self) -> Self {
        self.mode = CognitiveRecallTemporalMode::History;
        self.as_of = None;
        self.interval_start = None;
        self.interval_end = None;
        self
    }

    /// Set temporal inclusion policy. Privacy deletion always overrides these flags.
    #[must_use]
    pub fn with_policy(
        mut self,
        include_superseded: bool,
        include_revoked: bool,
        unknown_validity_policy: CognitiveRecallUnknownValidityPolicy,
    ) -> Self {
        self.include_superseded = include_superseded;
        self.include_revoked = include_revoked;
        self.unknown_validity_policy = unknown_validity_policy;
        self
    }

    /// Validates mode-specific bounds without consulting a clock or provider.
    pub fn validate(&self) -> Result<(), ApplicationContractError> {
        let valid = match self.mode {
            CognitiveRecallTemporalMode::Current | CognitiveRecallTemporalMode::History => {
                self.as_of.is_none() && self.interval_start.is_none() && self.interval_end.is_none()
            }
            CognitiveRecallTemporalMode::AsOf => {
                self.as_of.is_some_and(|at| at <= self.evaluation_time)
                    && self.interval_start.is_none()
                    && self.interval_end.is_none()
            }
            CognitiveRecallTemporalMode::Interval => {
                self.as_of.is_none()
                    && matches!((self.interval_start, self.interval_end), (Some(start), Some(end)) if start < end)
            }
        };
        if !valid {
            return Err(ApplicationContractError::InvalidRange {
                field: "cognitive recall temporal bounds",
            });
        }
        Ok(())
    }

    /// Reject a future evaluation clock at actual host admission.
    pub fn validate_at(&self, now: UtcMicros) -> Result<(), ApplicationContractError> {
        self.validate()?;
        if self.evaluation_time > now {
            return Err(ApplicationContractError::InvalidRange {
                field: "cognitive recall evaluation time",
            });
        }
        Ok(())
    }

    /// Requested canonical temporal mode.
    #[must_use]
    pub const fn mode(&self) -> CognitiveRecallTemporalMode {
        self.mode
    }
    /// Host-admitted evaluation clock.
    #[must_use]
    pub const fn evaluation_time(&self) -> UtcMicros {
        self.evaluation_time
    }
    /// Requested historical instant.
    #[must_use]
    pub const fn as_of(&self) -> Option<UtcMicros> {
        self.as_of
    }
    /// Inclusive interval start.
    #[must_use]
    pub const fn interval_start(&self) -> Option<UtcMicros> {
        self.interval_start
    }
    /// Exclusive interval end.
    #[must_use]
    pub const fn interval_end(&self) -> Option<UtcMicros> {
        self.interval_end
    }
    /// Whether ordinarily superseded evidence is permitted.
    #[must_use]
    pub const fn include_superseded(&self) -> bool {
        self.include_superseded
    }
    /// Whether ordinarily revoked evidence is permitted.
    #[must_use]
    pub const fn include_revoked(&self) -> bool {
        self.include_revoked
    }
    /// Requested policy for unknown validity.
    #[must_use]
    pub const fn unknown_validity_policy(&self) -> CognitiveRecallUnknownValidityPolicy {
        self.unknown_validity_policy
    }
}

/// Canonical exclusion classes, applied before candidate limits. A content
/// digest identifies full candidate content before output truncation.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CognitiveRecallExclusions {
    /// Stable provider memory references.
    pub stable_memory_refs: Vec<String>,
    /// Request-scoped candidate IDs.
    pub candidate_ids: Vec<String>,
    /// Original source references.
    pub source_refs: Vec<String>,
    /// Retained trace references.
    pub trace_refs: Vec<String>,
    /// Original canonical observation IDs.
    pub observation_ids: Vec<String>,
    /// Lowercase SHA-256 digests of full content.
    pub content_sha256: Vec<String>,
}

impl CognitiveRecallExclusions {
    /// Validates every exclusion class and rejects duplicates within each class.
    pub fn validate(&self) -> Result<(), ApplicationContractError> {
        for values in [
            &self.stable_memory_refs,
            &self.candidate_ids,
            &self.source_refs,
            &self.trace_refs,
            &self.observation_ids,
            &self.content_sha256,
        ] {
            if values.len() > MAX_COGNITIVE_RECALL_EXCLUSIONS_PER_CLASS {
                return Err(ApplicationContractError::InvalidRange {
                    field: "cognitive recall exclusions",
                });
            }
            let mut seen = BTreeSet::new();
            for value in values {
                validate_reference(value, "cognitive recall exclusion")?;
                if !seen.insert(value) {
                    return Err(ApplicationContractError::Duplicate {
                        field: "cognitive recall exclusion",
                    });
                }
            }
        }
        for digest in &self.content_sha256 {
            if digest.len() != 64
                || !digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(ApplicationContractError::InvalidIdentifier {
                    field: "cognitive recall content exclusion digest",
                });
            }
        }
        Ok(())
    }
}

/// Immutable request for one bounded advisory recall attempt.
///
/// `scope`, `request_id`, `deadline`, and `cancellation` are copied from the
/// application admission boundary.  An adapter must use them as-is: it may
/// not infer a path or repository-only scope, widen the deadline, or replace
/// the cancellation identity.  `query` is an application-owned string rather
/// than a provider or transport query type.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CognitiveRecallRequest {
    scope: ResolvedScope,
    request_id: RequestId,
    deadline: Deadline,
    cancellation: CancellationContext,
    query: String,
    maximum_candidates: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    temporal_query: Option<CognitiveRecallTemporalQuery>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    exclusions: Option<CognitiveRecallExclusions>,
}

impl CognitiveRecallRequest {
    /// Construct a validated bounded recall request.
    pub fn new(
        scope: ResolvedScope,
        request_id: RequestId,
        deadline: Deadline,
        cancellation: CancellationContext,
        query: impl Into<String>,
        maximum_candidates: usize,
    ) -> Result<Self, ApplicationContractError> {
        let request = Self {
            scope,
            request_id,
            deadline,
            cancellation,
            query: query.into(),
            maximum_candidates,
            temporal_query: None,
            exclusions: None,
        };
        request.validate()?;
        Ok(request)
    }

    /// Validate scope identity and request-owned bounds without reading a
    /// clock or consulting an adapter.
    pub fn validate(&self) -> Result<(), ApplicationContractError> {
        self.scope.validate()?;
        if self.query.is_empty()
            || self.query.trim().is_empty()
            || self.query.len() > MAX_COGNITIVE_RECALL_QUERY_BYTES
            || self.query.chars().any(char::is_control)
        {
            return Err(ApplicationContractError::InvalidRange {
                field: "cognitive recall query",
            });
        }
        if self.maximum_candidates == 0 {
            return Err(ApplicationContractError::ZeroValue {
                field: "cognitive recall maximum candidates",
            });
        }
        if self.maximum_candidates > MAX_COGNITIVE_RECALL_CANDIDATES {
            return Err(ApplicationContractError::InvalidRange {
                field: "cognitive recall maximum candidates",
            });
        }
        if let Some(temporal_query) = &self.temporal_query {
            temporal_query.validate()?;
        }
        if let Some(exclusions) = &self.exclusions {
            exclusions.validate()?;
        }
        Ok(())
    }

    /// Attach a validated temporal selection; absence preserves the legacy
    /// current-time selection using the host's actual admission clock.
    pub fn with_temporal_query(
        mut self,
        temporal_query: CognitiveRecallTemporalQuery,
    ) -> Result<Self, ApplicationContractError> {
        self.temporal_query = Some(temporal_query);
        self.validate()?;
        Ok(self)
    }

    /// Attach bounded canonical exclusions without silently dropping any class.
    pub fn with_exclusions(
        mut self,
        exclusions: CognitiveRecallExclusions,
    ) -> Result<Self, ApplicationContractError> {
        self.exclusions = Some(exclusions);
        self.validate()?;
        Ok(self)
    }

    /// Explicit temporal selection, or legacy current-time behavior if absent.
    #[must_use]
    pub fn temporal_query(&self) -> Option<&CognitiveRecallTemporalQuery> {
        self.temporal_query.as_ref()
    }

    /// Explicit exclusions, or the legacy empty set if absent.
    #[must_use]
    pub fn exclusions(&self) -> Option<&CognitiveRecallExclusions> {
        self.exclusions.as_ref()
    }

    #[must_use]
    pub fn scope(&self) -> &ResolvedScope {
        &self.scope
    }

    #[must_use]
    pub fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    #[must_use]
    pub fn deadline(&self) -> &Deadline {
        &self.deadline
    }

    #[must_use]
    pub fn cancellation(&self) -> &CancellationContext {
        &self.cancellation
    }

    #[must_use]
    pub fn query(&self) -> &str {
        &self.query
    }

    #[must_use]
    pub const fn maximum_candidates(&self) -> usize {
        self.maximum_candidates
    }
}

/// Explicit provenance state for one advisory candidate.
///
/// A missing source is not represented by an empty successful string.  The
/// state remains visible so a later context compiler can apply its own
/// provenance policy without granting a provider authority over that policy.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum CognitiveRecallProvenance {
    /// The candidate has an opaque, bounded source label.
    Available { source: String },
    /// The source exists but its identifying detail was intentionally hidden.
    Redacted { reason: String },
    /// The adapter could not establish source provenance.
    Unavailable,
}

impl CognitiveRecallProvenance {
    /// Construct available provenance from a bounded opaque source label.
    pub fn available(source: impl Into<String>) -> Result<Self, ApplicationContractError> {
        let source = source.into();
        validate_reference(&source, "cognitive recall provenance source")?;
        Ok(Self::Available { source })
    }

    /// Construct redacted provenance while retaining the explicit reason.
    pub fn redacted(reason: impl Into<String>) -> Result<Self, ApplicationContractError> {
        let reason = reason.into();
        validate_reference(&reason, "cognitive recall provenance redaction reason")?;
        Ok(Self::Redacted { reason })
    }

    /// Construct an explicit unavailable-provenance state.
    pub const fn unavailable() -> Self {
        Self::Unavailable
    }
}

/// One bounded, advisory candidate returned by a recall adapter.
///
/// The inline content is evidence only.  It is not a canonical fact and does
/// not identify or hydrate a retrieval anchor.  The optional stable reference
/// is an opaque provider label that remains advisory and request-scoped.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CognitiveRecallCandidate {
    candidate_id: String,
    stable_reference: Option<String>,
    content: String,
    provenance: CognitiveRecallProvenance,
    explanation: Option<String>,
}

impl CognitiveRecallCandidate {
    /// Construct a candidate with inline content and explicit provenance.
    pub fn new(
        candidate_id: impl Into<String>,
        content: impl Into<String>,
        provenance: CognitiveRecallProvenance,
    ) -> Result<Self, ApplicationContractError> {
        let candidate = Self {
            candidate_id: candidate_id.into(),
            stable_reference: None,
            content: content.into(),
            provenance,
            explanation: None,
        };
        candidate.validate()?;
        Ok(candidate)
    }

    /// Validate candidate identity and all local byte bounds.
    pub fn validate(&self) -> Result<(), ApplicationContractError> {
        validate_reference(&self.candidate_id, "cognitive recall candidate id")?;
        if self.content.is_empty() || self.content.len() > MAX_COGNITIVE_RECALL_CANDIDATE_BYTES {
            return Err(ApplicationContractError::InvalidRange {
                field: "cognitive recall candidate content",
            });
        }
        if let Some(stable_reference) = &self.stable_reference {
            validate_reference(
                stable_reference,
                "cognitive recall candidate stable reference",
            )?;
        }
        if let Some(explanation) = &self.explanation
            && (explanation.is_empty()
                || explanation.len() > MAX_COGNITIVE_RECALL_EXPLANATION_BYTES
                || explanation.chars().any(char::is_control))
        {
            return Err(ApplicationContractError::InvalidRange {
                field: "cognitive recall candidate explanation",
            });
        }
        validate_provenance(&self.provenance)
    }

    /// Add an opaque stable provider reference without turning it into a
    /// TraceDecay retrieval-anchor identity.
    pub fn with_stable_reference(
        mut self,
        stable_reference: impl Into<String>,
    ) -> Result<Self, ApplicationContractError> {
        self.stable_reference = Some(stable_reference.into());
        self.validate()?;
        Ok(self)
    }

    /// Add a bounded explanation summary.  Explanations are evidence for
    /// inspection, never executable instruction authority.
    pub fn with_explanation(
        mut self,
        explanation: impl Into<String>,
    ) -> Result<Self, ApplicationContractError> {
        self.explanation = Some(explanation.into());
        self.validate()?;
        Ok(self)
    }

    #[must_use]
    pub fn candidate_id(&self) -> &str {
        &self.candidate_id
    }

    #[must_use]
    pub fn stable_reference(&self) -> Option<&str> {
        self.stable_reference.as_deref()
    }

    #[must_use]
    pub fn content(&self) -> &str {
        &self.content
    }

    #[must_use]
    pub fn provenance(&self) -> &CognitiveRecallProvenance {
        &self.provenance
    }

    #[must_use]
    pub fn explanation(&self) -> Option<&str> {
        self.explanation.as_deref()
    }
}

/// Typed degradation for a recall lane.  A successful response carrying one
/// of these values is not equivalent to a complete zero-result response.
#[derive(
    Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, Ord, PartialOrd, Eq, PartialEq, Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum CognitiveRecallDegradation {
    Unsupported,
    Unavailable,
    Cancelled,
    TimedOut,
    Partial,
    Stale,
    BudgetExhausted,
}

/// Identity of the configured provider a recall result is attributed to.
///
/// The host pins `provider_id` and `registration_revision` in routing
/// configuration before any provider is contacted, so every result — including
/// one degraded before contact — names the provider the host selected rather
/// than whichever provider happened to answer.  `provider_instance_id` is the
/// runtime instance the readiness handshake reported and is absent only when
/// no handshake preceded the outcome.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CognitiveRecallProviderIdentity {
    provider_id: String,
    registration_revision: u64,
    provider_instance_id: Option<String>,
}

impl CognitiveRecallProviderIdentity {
    /// Construct the identity of the host-configured provider before any
    /// provider contact.
    pub fn configured(
        provider_id: impl Into<String>,
        registration_revision: u64,
    ) -> Result<Self, ApplicationContractError> {
        let identity = Self {
            provider_id: provider_id.into(),
            registration_revision,
            provider_instance_id: None,
        };
        identity.validate()?;
        Ok(identity)
    }

    /// Attach the runtime instance identity a readiness handshake reported.
    pub fn with_instance(
        mut self,
        provider_instance_id: impl Into<String>,
    ) -> Result<Self, ApplicationContractError> {
        self.provider_instance_id = Some(provider_instance_id.into());
        self.validate()?;
        Ok(self)
    }

    /// Validate that the identity is non-empty, bounded, and pinned to a
    /// positive registration revision.
    pub fn validate(&self) -> Result<(), ApplicationContractError> {
        validate_reference(&self.provider_id, "cognitive recall provider id")?;
        if self.registration_revision == 0 {
            return Err(ApplicationContractError::ZeroValue {
                field: "cognitive recall provider registration revision",
            });
        }
        if let Some(instance) = &self.provider_instance_id {
            validate_reference(instance, "cognitive recall provider instance id")?;
        }
        Ok(())
    }

    #[must_use]
    pub fn provider_id(&self) -> &str {
        &self.provider_id
    }

    #[must_use]
    pub fn registration_revision(&self) -> u64 {
        self.registration_revision
    }

    #[must_use]
    pub fn provider_instance_id(&self) -> Option<&str> {
        self.provider_instance_id.as_deref()
    }
}

/// One scope- and request-bound advisory recall result.
///
/// `degradation == None` means the adapter completed its admitted search; an
/// empty candidate vector is therefore an explicit successful zero-result,
/// never an unavailable/fallback signal.  A non-empty degradation remains
/// typed even when an adapter can return useful partial candidates.  Every
/// result, complete or degraded, names the configured provider it is
/// attributed to.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CognitiveRecallResult {
    scope: ResolvedScope,
    request_id: RequestId,
    provider: CognitiveRecallProviderIdentity,
    candidates: Vec<CognitiveRecallCandidate>,
    degradation: Option<CognitiveRecallDegradation>,
}

/// Result name used by ports that make the application-boundary role
/// explicit.  Both names describe the same app-owned value.
pub type CognitiveRecallPortResult = CognitiveRecallResult;

impl CognitiveRecallResult {
    /// Construct a complete result, including a valid zero-result response.
    pub fn complete(
        scope: ResolvedScope,
        request_id: RequestId,
        provider: CognitiveRecallProviderIdentity,
        candidates: Vec<CognitiveRecallCandidate>,
    ) -> Result<Self, ApplicationContractError> {
        Self::new(scope, request_id, provider, candidates, None)
    }

    /// Construct a result with an explicit typed lane degradation.
    pub fn degraded(
        scope: ResolvedScope,
        request_id: RequestId,
        provider: CognitiveRecallProviderIdentity,
        candidates: Vec<CognitiveRecallCandidate>,
        degradation: CognitiveRecallDegradation,
    ) -> Result<Self, ApplicationContractError> {
        Self::new(scope, request_id, provider, candidates, Some(degradation))
    }

    /// Construct and validate a result without consulting wall-clock state.
    pub fn new(
        scope: ResolvedScope,
        request_id: RequestId,
        provider: CognitiveRecallProviderIdentity,
        candidates: Vec<CognitiveRecallCandidate>,
        degradation: Option<CognitiveRecallDegradation>,
    ) -> Result<Self, ApplicationContractError> {
        let result = Self {
            scope,
            request_id,
            provider,
            candidates,
            degradation,
        };
        result.validate()?;
        Ok(result)
    }

    /// Validate scope identity, provider identity, candidate bounds, and
    /// request-scoped candidate uniqueness.  Request-specific limits are
    /// checked by [`Self::validate_for`].
    pub fn validate(&self) -> Result<(), ApplicationContractError> {
        self.scope.validate()?;
        self.provider.validate()?;
        if self.candidates.len() > MAX_COGNITIVE_RECALL_CANDIDATES {
            return Err(ApplicationContractError::InvalidRange {
                field: "cognitive recall candidates",
            });
        }

        let mut candidate_ids = BTreeSet::new();
        for candidate in &self.candidates {
            candidate.validate()?;
            if !candidate_ids.insert(candidate.candidate_id()) {
                return Err(ApplicationContractError::Duplicate {
                    field: "cognitive recall candidate id",
                });
            }
        }
        Ok(())
    }

    /// Revalidate that an adapter response is tied to the exact admitted
    /// scope/request identity and does not exceed the request's candidate
    /// budget.
    pub fn validate_for(
        &self,
        request: &CognitiveRecallRequest,
    ) -> Result<(), ApplicationContractError> {
        request.validate()?;
        self.validate()?;
        if self.scope != *request.scope() || self.request_id != *request.request_id() {
            return Err(ApplicationContractError::Inconsistent {
                field: "cognitive recall result identity",
            });
        }
        if self.candidates.len() > request.maximum_candidates() {
            return Err(ApplicationContractError::InvalidRange {
                field: "cognitive recall result candidate budget",
            });
        }
        Ok(())
    }

    #[must_use]
    pub fn scope(&self) -> &ResolvedScope {
        &self.scope
    }

    #[must_use]
    pub fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    /// The configured provider this result is attributed to.
    #[must_use]
    pub fn provider(&self) -> &CognitiveRecallProviderIdentity {
        &self.provider
    }

    #[must_use]
    pub fn candidates(&self) -> &[CognitiveRecallCandidate] {
        &self.candidates
    }

    #[must_use]
    pub fn degradation(&self) -> Option<CognitiveRecallDegradation> {
        self.degradation
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.degradation.is_none()
    }

    #[must_use]
    pub fn into_candidates(self) -> Vec<CognitiveRecallCandidate> {
        self.candidates
    }
}

/// Narrow application boundary for one bounded advisory recall attempt.
///
/// Implementations own the provider/fabric integration outside this crate.
/// The port exposes only app-owned contracts and requires an associated error
/// plus a `Send` future, matching the canonical memory ports.
pub trait CognitiveRecallPort {
    type Error: Debug;

    fn recall(
        &self,
        request: CognitiveRecallRequest,
    ) -> impl Future<Output = Result<CognitiveRecallPortResult, Self::Error>> + Send;
}

fn validate_reference(value: &str, field: &'static str) -> Result<(), ApplicationContractError> {
    if value.is_empty()
        || value.trim() != value
        || value.len() > MAX_COGNITIVE_RECALL_REFERENCE_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(ApplicationContractError::InvalidIdentifier { field });
    }
    Ok(())
}

fn validate_provenance(
    provenance: &CognitiveRecallProvenance,
) -> Result<(), ApplicationContractError> {
    match provenance {
        CognitiveRecallProvenance::Available { source } => {
            validate_reference(source, "cognitive recall provenance source")
        }
        CognitiveRecallProvenance::Redacted { reason } => {
            validate_reference(reason, "cognitive recall provenance redaction reason")
        }
        CognitiveRecallProvenance::Unavailable => Ok(()),
    }
}
