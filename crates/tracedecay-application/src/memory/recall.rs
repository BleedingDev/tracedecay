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
pub const MAX_COGNITIVE_RECALL_REFERENCE_BYTES: usize = 1 * 1024;

/// Maximum UTF-8 byte length of an optional provider explanation summary.
pub const MAX_COGNITIVE_RECALL_EXPLANATION_BYTES: usize = 8 * 1024;

// Keep V1-style aliases available to callers that expose versioned catalog
// names while retaining the short names used by the application port.
pub const MAX_COGNITIVE_RECALL_CANDIDATES_V1: usize = MAX_COGNITIVE_RECALL_CANDIDATES;
pub const MAX_COGNITIVE_RECALL_QUERY_BYTES_V1: usize = MAX_COGNITIVE_RECALL_QUERY_BYTES;
pub const MAX_COGNITIVE_RECALL_CANDIDATE_BYTES_V1: usize = MAX_COGNITIVE_RECALL_CANDIDATE_BYTES;

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

    /// Alias for callers that use the shorter budget terminology.
    #[must_use]
    pub const fn max_candidates(&self) -> usize {
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
        if let Some(explanation) = &self.explanation {
            if explanation.is_empty()
                || explanation.len() > MAX_COGNITIVE_RECALL_EXPLANATION_BYTES
                || explanation.chars().any(char::is_control)
            {
                return Err(ApplicationContractError::InvalidRange {
                    field: "cognitive recall candidate explanation",
                });
            }
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

/// One scope- and request-bound advisory recall result.
///
/// `degradation == None` means the adapter completed its admitted search; an
/// empty candidate vector is therefore an explicit successful zero-result,
/// never an unavailable/fallback signal.  A non-empty degradation remains
/// typed even when an adapter can return useful partial candidates.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CognitiveRecallResult {
    scope: ResolvedScope,
    request_id: RequestId,
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
        candidates: Vec<CognitiveRecallCandidate>,
    ) -> Result<Self, ApplicationContractError> {
        Self::new(scope, request_id, candidates, None)
    }

    /// Construct a result with an explicit typed lane degradation.
    pub fn degraded(
        scope: ResolvedScope,
        request_id: RequestId,
        candidates: Vec<CognitiveRecallCandidate>,
        degradation: CognitiveRecallDegradation,
    ) -> Result<Self, ApplicationContractError> {
        Self::new(scope, request_id, candidates, Some(degradation))
    }

    /// Construct and validate a result without consulting wall-clock state.
    pub fn new(
        scope: ResolvedScope,
        request_id: RequestId,
        candidates: Vec<CognitiveRecallCandidate>,
        degradation: Option<CognitiveRecallDegradation>,
    ) -> Result<Self, ApplicationContractError> {
        let result = Self {
            scope,
            request_id,
            candidates,
            degradation,
        };
        result.validate()?;
        Ok(result)
    }

    /// Validate scope identity, candidate bounds, and request-scoped candidate
    /// uniqueness.  Request-specific limits are checked by [`Self::validate_for`].
    pub fn validate(&self) -> Result<(), ApplicationContractError> {
        self.scope.validate()?;
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
