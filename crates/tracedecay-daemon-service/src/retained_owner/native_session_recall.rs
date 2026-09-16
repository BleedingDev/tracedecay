//! Ephemeral Native recall over the application-admitted session projection.
//!
//! This adapter is deliberately a read-only boundary.  The session retrieval
//! port owns admission, temporal projection, and canonical retrieval anchors;
//! this module only turns the returned page into bounded Native-shaped
//! candidates.  It does not open a provider database, stage observations, or
//! retain a provider-local copy of a session.

use std::collections::BTreeSet;

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tracedecay_contracts::memory::{
    CognitiveRecallExclusions, CognitiveRecallUnknownValidityPolicy,
};
use tracedecay_contracts::{CancellationSignal, RequestContext};
use tracedecay_domain::{RetrievalAnchorId, TemporalModeV1};
use tracedecay_session_memory::session::{SessionRetrievalScope, SessionTemporalQuery};
use tracedecay_session_runtime::session_retrieval::{
    SessionApplicationRetrievalPortV1, SessionRetrievalPageView, SessionRetrievalServiceOutcome,
    SessionRetrievalStoreScope, SessionRetrievalUnavailableReason,
};
use tracedecay_sessions::runtime::{
    SessionMessageRecord, SessionMessageSearchResult, SessionRecord,
};
use tracedecay_temporal_query::ports::{
    TemporalCandidateFilterV1, TemporalMessageTypeFilterV1, TemporalSessionScopeFilterV1,
};

/// The fixed-point score domain used by the parent Native projection.
pub const NATIVE_SESSION_RECALL_SCORE_DOMAIN: &str = "tracedecay.native.session.score.v1";
/// Memory class used when the parent maps these candidates to the registry wire.
pub const NATIVE_SESSION_RECALL_MEMORY_CLASS: &str = "session_observation";

const DEFAULT_MAXIMUM_CANDIDATES: usize = 32;
const DEFAULT_MAXIMUM_CANDIDATE_CONTENT_BYTES: usize = 64 * 1024;
const DEFAULT_MAXIMUM_TOTAL_CONTENT_BYTES: usize = 256 * 1024;
const DEFAULT_MAXIMUM_WORK_UNITS: u64 = 100_000;

/// Local ceilings applied after canonical admission and deterministic ranking.
///
/// A limit is a projection ceiling, not an admission rule.  In particular, an
/// excluded high-ranked row does not consume `maximum_candidates` or content
/// bytes for a later eligible row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct NativeSessionRecallLimits {
    pub maximum_candidates: usize,
    pub maximum_candidate_content_bytes: usize,
    pub maximum_total_content_bytes: usize,
    pub maximum_work_units: u64,
}

impl Default for NativeSessionRecallLimits {
    fn default() -> Self {
        Self {
            maximum_candidates: DEFAULT_MAXIMUM_CANDIDATES,
            maximum_candidate_content_bytes: DEFAULT_MAXIMUM_CANDIDATE_CONTENT_BYTES,
            maximum_total_content_bytes: DEFAULT_MAXIMUM_TOTAL_CONTENT_BYTES,
            maximum_work_units: DEFAULT_MAXIMUM_WORK_UNITS,
        }
    }
}

impl NativeSessionRecallLimits {
    pub fn validate(self) -> Result<(), NativeSessionRecallLimitError> {
        if self.maximum_candidates == 0 {
            return Err(NativeSessionRecallLimitError::ZeroCandidates);
        }
        if self.maximum_candidate_content_bytes == 0 {
            return Err(NativeSessionRecallLimitError::ZeroCandidateBytes);
        }
        if self.maximum_total_content_bytes == 0 {
            return Err(NativeSessionRecallLimitError::ZeroTotalBytes);
        }
        if self.maximum_work_units == 0 {
            return Err(NativeSessionRecallLimitError::ZeroWorkUnits);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeSessionRecallLimitError {
    ZeroCandidates,
    ZeroCandidateBytes,
    ZeroTotalBytes,
    ZeroWorkUnits,
}

/// Temporal selection understood by this adapter.
///
/// `Interval` and `History` are represented so the Native boundary can return
/// a typed unsupported result before calling the current session port.  The
/// port currently exposes only `Current`, `AsOf`, `Evolution`, and `Forensic`;
/// there is no interval query to forward without changing that contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum NativeSessionRecallTemporal {
    Current,
    AsOf { cutoff_micros: i64 },
    Interval { start_micros: i64, end_micros: i64 },
    History,
}

/// Controls supplied by the Native retained surface for one ephemeral read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeSessionRecallOptions {
    pub temporal: NativeSessionRecallTemporal,
    /// The admitted application clock.  `None` means the caller has not
    /// supplied a clock and the adapter must not invent one.
    pub evaluation_time_micros: Option<i64>,
    pub include_superseded: bool,
    pub include_revoked: bool,
    pub unknown_validity_policy: CognitiveRecallUnknownValidityPolicy,
    pub exclusions: CognitiveRecallExclusions,
    pub limits: NativeSessionRecallLimits,
}

impl Default for NativeSessionRecallOptions {
    fn default() -> Self {
        Self {
            temporal: NativeSessionRecallTemporal::Current,
            evaluation_time_micros: None,
            include_superseded: false,
            include_revoked: false,
            unknown_validity_policy: CognitiveRecallUnknownValidityPolicy::Exclude,
            exclusions: CognitiveRecallExclusions::default(),
            limits: NativeSessionRecallLimits::default(),
        }
    }
}

/// Canonical validity state retained on an ephemeral candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeSessionRecallValidityState {
    Current,
    Expired,
    Future,
    Superseded,
    Revoked,
    Unknown,
}

/// Validity evidence available on the session message projection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NativeSessionRecallValidity {
    pub observed_at_micros: Option<i64>,
    pub valid_from_micros: Option<i64>,
    pub valid_until_micros: Option<i64>,
    pub superseded_at_micros: Option<i64>,
    pub revoked_at_micros: Option<i64>,
    pub source_revision: Option<String>,
    pub state: NativeSessionRecallValidityState,
}

/// Provenance carried with every candidate.
///
/// The retrieval page's anchor is the canonical occurrence/source anchor.  It
/// is intentionally retained in both fields: this avoids fabricating a
/// provider source identity when the current `SessionMessageRecord` has only a
/// source path and byte offset.  `session_anchor` is an exact session message
/// range derived from the canonical session ID and ordinal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NativeSessionRecallProvenance {
    pub session_anchor: RetrievalAnchorId,
    pub observation_anchor: RetrievalAnchorId,
    pub source_anchor: RetrievalAnchorId,
    pub provider: String,
    pub session_id: String,
    pub message_id: String,
    pub source_observation_id: Option<String>,
}

/// A bounded, ephemeral Native recall candidate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NativeSessionRecallCandidate {
    pub candidate_id: String,
    pub stable_memory_ref: String,
    pub content: String,
    pub content_sha256: String,
    pub score_millionths: u64,
    pub session_anchor: RetrievalAnchorId,
    pub observation_anchor: RetrievalAnchorId,
    pub source_anchor: RetrievalAnchorId,
    pub provider: String,
    pub session_id: String,
    pub message_id: String,
    pub ordinal: i64,
    pub timestamp_micros: Option<i64>,
    pub role: String,
    pub kind: Option<String>,
    pub provenance: NativeSessionRecallProvenance,
    pub validity: NativeSessionRecallValidity,
    pub source_refs: Vec<String>,
    pub trace_refs: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeSessionRecallBatchStatus {
    Complete,
    Partial { omitted: u64 },
    Stale,
}

/// The projection returned to the Native owner.  `temporal` remains intact so
/// the owner can forward watermarks, explanations, and continuation cursors.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NativeSessionRecallBatch {
    pub candidates: Vec<NativeSessionRecallCandidate>,
    pub temporal: tracedecay_session_runtime::session_retrieval::SessionTemporalMetadataView,
    pub status: NativeSessionRecallBatchStatus,
    pub scanned_items: u64,
    pub excluded_items: u64,
    pub admitted_items: u64,
    pub work_units: u64,
    pub total_content_bytes: u64,
    pub degraded: bool,
}

/// Modes the current application retrieval port cannot satisfy for Native.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeSessionRecallUnsupported {
    Interval,
    History,
    Evolution,
    Forensic,
    TemporalMismatch,
    NonSessionScope,
}

/// Typed failures returned by the mounted session authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeSessionRecallUnavailable {
    Retrieval(SessionRetrievalUnavailableReason),
    CursorStale,
    WrongScope,
    Locked,
    Redacted,
    Deleted,
    Denied,
    ResetRequired(SessionRetrievalStoreScope),
    CursorManifestLimitExceeded,
    BudgetExhausted,
    TimedOut,
    Cancelled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeSessionRecallInvalidPage {
    AnchorResultCountMismatch,
    InvalidSessionIdentity,
    InvalidFilter,
    InvalidExclusions,
}

/// Adapter errors intentionally distinguish unsupported semantics from a
/// missing/unavailable mounted authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeSessionRecallAdapterError {
    Unsupported(NativeSessionRecallUnsupported),
    Unavailable(NativeSessionRecallUnavailable),
    InvalidPage(NativeSessionRecallInvalidPage),
    InvalidLimits(NativeSessionRecallLimitError),
}

/// Retrieve an admitted page and project it into ephemeral Native candidates.
pub async fn retrieve_native_session_recall(
    port: &dyn SessionApplicationRetrievalPortV1,
    context: &RequestContext,
    query: SessionTemporalQuery,
    options: &NativeSessionRecallOptions,
) -> Result<NativeSessionRecallBatch, NativeSessionRecallAdapterError> {
    validate_request(&query, options)?;
    let projection_query = query.clone();
    let outcome = port.retrieve_admitted(context, query).await;
    adapt_service_outcome(&projection_query, outcome, options)
}

/// Cancellation-preserving variant for Native request paths that already have
/// the admitted `CancellationSignal`.
pub async fn retrieve_native_session_recall_with_cancellation(
    port: &dyn SessionApplicationRetrievalPortV1,
    context: &RequestContext,
    cancellation: &CancellationSignal,
    query: SessionTemporalQuery,
    options: &NativeSessionRecallOptions,
) -> Result<NativeSessionRecallBatch, NativeSessionRecallAdapterError> {
    validate_request(&query, options)?;
    let projection_query = query.clone();
    let outcome = port
        .retrieve_admitted_with_cancellation(context, cancellation, query)
        .await;
    adapt_service_outcome(&projection_query, outcome, options)
}

/// Map the typed application outcome without persisting anything locally.
pub fn adapt_service_outcome(
    query: &SessionTemporalQuery,
    outcome: SessionRetrievalServiceOutcome,
    options: &NativeSessionRecallOptions,
) -> Result<NativeSessionRecallBatch, NativeSessionRecallAdapterError> {
    validate_request(query, options)?;
    match outcome {
        SessionRetrievalServiceOutcome::Complete { page, .. } => {
            project_page(query, page, options, 0)
        }
        SessionRetrievalServiceOutcome::CompleteZero { temporal, .. } => Ok(empty_batch(
            temporal,
            NativeSessionRecallBatchStatus::Complete,
        )),
        SessionRetrievalServiceOutcome::Stale { temporal, .. } => {
            Ok(empty_batch(temporal, NativeSessionRecallBatchStatus::Stale))
        }
        SessionRetrievalServiceOutcome::Partial { page, omitted, .. } => {
            project_page(query, page, options, omitted)
        }
        SessionRetrievalServiceOutcome::CursorStale => {
            Err(NativeSessionRecallAdapterError::Unavailable(
                NativeSessionRecallUnavailable::CursorStale,
            ))
        }
        SessionRetrievalServiceOutcome::WrongScope => {
            Err(unavailable(NativeSessionRecallUnavailable::WrongScope))
        }
        SessionRetrievalServiceOutcome::Locked => {
            Err(unavailable(NativeSessionRecallUnavailable::Locked))
        }
        SessionRetrievalServiceOutcome::Redacted => {
            Err(unavailable(NativeSessionRecallUnavailable::Redacted))
        }
        SessionRetrievalServiceOutcome::Deleted => {
            Err(unavailable(NativeSessionRecallUnavailable::Deleted))
        }
        SessionRetrievalServiceOutcome::Denied => {
            Err(unavailable(NativeSessionRecallUnavailable::Denied))
        }
        SessionRetrievalServiceOutcome::ResetRequired { store_scope } => Err(unavailable(
            NativeSessionRecallUnavailable::ResetRequired(store_scope),
        )),
        SessionRetrievalServiceOutcome::Unavailable(unavailable_reason) => Err(unavailable(
            NativeSessionRecallUnavailable::Retrieval(unavailable_reason.reason),
        )),
        SessionRetrievalServiceOutcome::CursorManifestLimitExceeded { .. } => Err(unavailable(
            NativeSessionRecallUnavailable::CursorManifestLimitExceeded,
        )),
        SessionRetrievalServiceOutcome::BudgetExhausted { .. } => {
            Err(unavailable(NativeSessionRecallUnavailable::BudgetExhausted))
        }
        SessionRetrievalServiceOutcome::TimedOut => {
            Err(unavailable(NativeSessionRecallUnavailable::TimedOut))
        }
        SessionRetrievalServiceOutcome::Cancelled => {
            Err(unavailable(NativeSessionRecallUnavailable::Cancelled))
        }
    }
}

/// Adapt one complete canonical page.  Admission is finished before local
/// candidate, content-byte, and total-byte ceilings are applied.
pub fn adapt_session_retrieval_page(
    query: &SessionTemporalQuery,
    page: SessionRetrievalPageView,
    options: &NativeSessionRecallOptions,
) -> Result<NativeSessionRecallBatch, NativeSessionRecallAdapterError> {
    validate_request(query, options)?;
    project_page(query, page, options, 0)
}

fn unavailable(reason: NativeSessionRecallUnavailable) -> NativeSessionRecallAdapterError {
    NativeSessionRecallAdapterError::Unavailable(reason)
}

fn validate_request(
    query: &SessionTemporalQuery,
    options: &NativeSessionRecallOptions,
) -> Result<(), NativeSessionRecallAdapterError> {
    options
        .limits
        .validate()
        .map_err(NativeSessionRecallAdapterError::InvalidLimits)?;
    options.exclusions.validate().map_err(|_| {
        NativeSessionRecallAdapterError::InvalidPage(
            NativeSessionRecallInvalidPage::InvalidExclusions,
        )
    })?;
    query.semantic_filter().validate().map_err(|_| {
        NativeSessionRecallAdapterError::InvalidPage(NativeSessionRecallInvalidPage::InvalidFilter)
    })?;

    if !matches!(
        query.retrieval_scope(),
        SessionRetrievalScope::Session(_) | SessionRetrievalScope::AllSessionsInAuthorizedRoot
    ) {
        return Err(NativeSessionRecallAdapterError::Unsupported(
            NativeSessionRecallUnsupported::NonSessionScope,
        ));
    }

    match (options.temporal, query.temporal_mode()) {
        (NativeSessionRecallTemporal::Current, TemporalModeV1::Current) => {}
        (NativeSessionRecallTemporal::AsOf { cutoff_micros }, TemporalModeV1::AsOf { cutoff })
            if cutoff_micros == cutoff.0 => {}
        (NativeSessionRecallTemporal::Interval { .. }, _) => {
            return Err(NativeSessionRecallAdapterError::Unsupported(
                NativeSessionRecallUnsupported::Interval,
            ));
        }
        (NativeSessionRecallTemporal::History, _) => {
            return Err(NativeSessionRecallAdapterError::Unsupported(
                NativeSessionRecallUnsupported::History,
            ));
        }
        (_, TemporalModeV1::Evolution) => {
            return Err(NativeSessionRecallAdapterError::Unsupported(
                NativeSessionRecallUnsupported::Evolution,
            ));
        }
        (_, TemporalModeV1::Forensic) => {
            return Err(NativeSessionRecallAdapterError::Unsupported(
                NativeSessionRecallUnsupported::Forensic,
            ));
        }
        _ => {
            return Err(NativeSessionRecallAdapterError::Unsupported(
                NativeSessionRecallUnsupported::TemporalMismatch,
            ));
        }
    }
    Ok(())
}

fn empty_batch(
    temporal: tracedecay_session_runtime::session_retrieval::SessionTemporalMetadataView,
    status: NativeSessionRecallBatchStatus,
) -> NativeSessionRecallBatch {
    NativeSessionRecallBatch {
        candidates: Vec::new(),
        temporal,
        status,
        scanned_items: 0,
        excluded_items: 0,
        admitted_items: 0,
        work_units: 0,
        total_content_bytes: 0,
        degraded: false,
    }
}

fn project_page(
    query: &SessionTemporalQuery,
    page: SessionRetrievalPageView,
    options: &NativeSessionRecallOptions,
    upstream_omitted: u64,
) -> Result<NativeSessionRecallBatch, NativeSessionRecallAdapterError> {
    let SessionRetrievalPageView { results, temporal } = page;
    if temporal.anchors.len() != results.len() {
        return Err(NativeSessionRecallAdapterError::InvalidPage(
            NativeSessionRecallInvalidPage::AnchorResultCountMismatch,
        ));
    }

    let mut work_units = 0_u64;
    let mut scanned_items = 0_u64;
    let mut excluded_items = 0_u64;
    let mut work_omitted = 0_u64;
    let mut degraded = false;
    let mut candidates = Vec::with_capacity(results.len());
    let mut seen_candidates = BTreeSet::new();

    for (index, result) in results.iter().enumerate() {
        let cost = result_work_units(result);
        if work_units.saturating_add(cost) > options.limits.maximum_work_units {
            work_omitted = results.len().saturating_sub(index) as u64;
            break;
        }
        work_units = work_units.saturating_add(cost);
        scanned_items = scanned_items.saturating_add(1);

        let anchor = temporal.anchors[index].clone();
        match admit_result(query, result, anchor, options) {
            CandidateAdmission::Accept {
                candidate,
                degraded: candidate_degraded,
            } => {
                if !seen_candidates.insert(candidate.candidate_id.clone()) {
                    excluded_items = excluded_items.saturating_add(1);
                    continue;
                }
                degraded |= candidate_degraded;
                candidates.push(candidate);
            }
            CandidateAdmission::Exclude => {
                excluded_items = excluded_items.saturating_add(1);
            }
        }
    }

    candidates.sort_by(deterministic_candidate_order);
    let admitted_items = candidates.len() as u64;
    let mut retained = Vec::with_capacity(candidates.len().min(options.limits.maximum_candidates));
    let mut total_content_bytes = 0_u64;
    let mut ceiling_omitted = 0_u64;

    for candidate in candidates {
        if candidate.content.len() > options.limits.maximum_candidate_content_bytes {
            ceiling_omitted = ceiling_omitted.saturating_add(1);
            continue;
        }
        if retained.len() >= options.limits.maximum_candidates {
            ceiling_omitted = ceiling_omitted.saturating_add(1);
            continue;
        }
        let content_bytes = candidate.content.len() as u64;
        if total_content_bytes.saturating_add(content_bytes)
            > options.limits.maximum_total_content_bytes as u64
        {
            ceiling_omitted = ceiling_omitted.saturating_add(1);
            continue;
        }
        total_content_bytes = total_content_bytes.saturating_add(content_bytes);
        retained.push(candidate);
    }

    let omitted = upstream_omitted
        .saturating_add(work_omitted)
        .saturating_add(ceiling_omitted);
    let status = if omitted == 0 {
        NativeSessionRecallBatchStatus::Complete
    } else {
        NativeSessionRecallBatchStatus::Partial { omitted }
    };

    Ok(NativeSessionRecallBatch {
        candidates: retained,
        temporal,
        status,
        scanned_items,
        excluded_items,
        admitted_items,
        work_units,
        total_content_bytes,
        degraded,
    })
}

enum CandidateAdmission {
    Accept {
        candidate: NativeSessionRecallCandidate,
        degraded: bool,
    },
    Exclude,
}

fn admit_result(
    query: &SessionTemporalQuery,
    result: &SessionMessageSearchResult,
    observation_anchor: RetrievalAnchorId,
    options: &NativeSessionRecallOptions,
) -> CandidateAdmission {
    let session = &result.session;
    let message = &result.message;
    let filter = query.semantic_filter();

    if !record_identity_matches(query, session, message)
        || !filter_matches(filter, session, message)
    {
        return CandidateAdmission::Exclude;
    }

    let metadata = message
        .metadata_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok());
    let source_observation_id = metadata.as_ref().and_then(|value| {
        metadata_string(Some(value), &["source_observation_id", "observation_id"])
    });
    let session_anchor = match exact_session_anchor(session, message) {
        Some(anchor) => anchor,
        None => return CandidateAdmission::Exclude,
    };
    let content_sha256 = sha256_hex(message.text.as_bytes());
    let identity = format!(
        "{}\u{0}{}\u{0}{}\u{0}{}",
        message.provider,
        message.session_id,
        message.message_id,
        observation_anchor.as_str()
    );
    let identity_digest = sha256_hex(identity.as_bytes());
    let candidate_id = format!("native-session:{identity_digest}");
    let stable_memory_ref = candidate_id.clone();
    let source_anchor = metadata
        .as_ref()
        .and_then(|value| {
            metadata_string(
                Some(value),
                &["source_anchor_id", "source_retrieval_anchor_id"],
            )
        })
        .and_then(|value| RetrievalAnchorId::new(value.to_owned()).ok())
        .unwrap_or_else(|| observation_anchor.clone());

    let (validity, mut warnings, validity_degraded) =
        match validity_for(message, metadata.as_ref(), options) {
            Some(value) => value,
            None => return CandidateAdmission::Exclude,
        };

    let source_refs = source_refs(metadata.as_ref(), source_anchor.as_str(), message);
    let trace_refs = metadata_string_vec(metadata.as_ref(), "trace_refs");
    if is_excluded(
        &options.exclusions,
        &candidate_id,
        &stable_memory_ref,
        &source_refs,
        &trace_refs,
        &observation_anchor,
        source_observation_id.as_deref(),
        &content_sha256,
    ) {
        return CandidateAdmission::Exclude;
    }

    let provenance = NativeSessionRecallProvenance {
        session_anchor: session_anchor.clone(),
        observation_anchor: observation_anchor.clone(),
        source_anchor: source_anchor.clone(),
        provider: message.provider.clone(),
        session_id: message.session_id.clone(),
        message_id: message.message_id.clone(),
        source_observation_id: source_observation_id.map(str::to_owned),
    };

    CandidateAdmission::Accept {
        candidate: NativeSessionRecallCandidate {
            candidate_id,
            stable_memory_ref,
            content: message.text.clone(),
            content_sha256,
            score_millionths: score_millionths(result.score),
            session_anchor,
            observation_anchor,
            source_anchor,
            provider: message.provider.clone(),
            session_id: message.session_id.clone(),
            message_id: message.message_id.clone(),
            ordinal: message.ordinal,
            timestamp_micros: message.timestamp,
            role: message.role.clone(),
            kind: message.kind.clone(),
            provenance,
            validity,
            source_refs,
            trace_refs,
            warnings,
        },
        degraded: validity_degraded,
    }
}

fn record_identity_matches(
    query: &SessionTemporalQuery,
    session: &SessionRecord,
    message: &SessionMessageRecord,
) -> bool {
    if session.provider != message.provider
        || session.session_id != message.session_id
        || session.provider.is_empty()
        || session.session_id.is_empty()
    {
        return false;
    }
    if let Some(provider) = query.provider()
        && (provider != session.provider || provider != message.provider)
    {
        return false;
    }
    if let SessionRetrievalScope::Session(session_id) = query.retrieval_scope()
        && session_id.as_str() != session.session_id
    {
        return false;
    }
    true
}

fn filter_matches(
    filter: &TemporalCandidateFilterV1,
    session: &SessionRecord,
    message: &SessionMessageRecord,
) -> bool {
    if filter
        .project_key
        .as_deref()
        .is_some_and(|project| project != session.project_key)
        || filter
            .parent_session_id
            .as_deref()
            .is_some_and(|parent| session.parent_session_id.as_deref() != Some(parent))
        || filter
            .source
            .as_deref()
            .is_some_and(|source| source != session.provider)
    {
        return false;
    }
    match filter.session_scope {
        TemporalSessionScopeFilterV1::All => {}
        TemporalSessionScopeFilterV1::ParentsOnly if session.is_subagent => return false,
        TemporalSessionScopeFilterV1::SubagentsOnly if !session.is_subagent => return false,
        TemporalSessionScopeFilterV1::ParentsOnly | TemporalSessionScopeFilterV1::SubagentsOnly => {
        }
    }
    if !filter.include_summaries
        && (message.kind.as_deref() == Some("summary") || message.role == "summary")
    {
        return false;
    }
    if !filter.roles.is_empty() && !filter.roles.iter().any(|role| role == &message.role) {
        return false;
    }
    if filter
        .start_time
        .is_some_and(|start| message.timestamp.is_none_or(|timestamp| timestamp < start))
        || filter
            .end_time
            .is_some_and(|end| message.timestamp.is_none_or(|timestamp| timestamp > end))
    {
        return false;
    }

    let tool_result = message.role == "tool"
        || message.role == "tool_result"
        || message.kind.as_deref() == Some("tool_result");
    match filter.message_type {
        TemporalMessageTypeFilterV1::All => {}
        TemporalMessageTypeFilterV1::DirectUser if message.role != "user" || tool_result => {
            return false;
        }
        TemporalMessageTypeFilterV1::ToolResult if !tool_result => return false,
        TemporalMessageTypeFilterV1::DirectUser | TemporalMessageTypeFilterV1::ToolResult => {}
    }

    // Git/workflow/goal filters are resolved by the mounted application port,
    // whose canonical occurrence projection carries relations not present on a
    // SessionMessageRecord.  The adapter intentionally does not infer them
    // from transcript paths or free-form text.
    true
}

fn exact_session_anchor(
    session: &SessionRecord,
    message: &SessionMessageRecord,
) -> Option<RetrievalAnchorId> {
    if message.ordinal < 0 {
        return None;
    }
    RetrievalAnchorId::new(format!(
        "session:{}#{}-{}",
        session.session_id, message.ordinal, message.ordinal
    ))
    .ok()
}

fn validity_for(
    message: &SessionMessageRecord,
    metadata: Option<&Value>,
    options: &NativeSessionRecallOptions,
) -> Option<(NativeSessionRecallValidity, Vec<String>, bool)> {
    let timestamp = message.timestamp;
    let valid_from = metadata_i64(metadata, &["valid_from_micros", "valid_from"]).or(timestamp);
    let valid_until = metadata_i64(metadata, &["valid_until_micros", "valid_until"]);
    let superseded_at = metadata_i64(metadata, &["superseded_at_micros", "superseded_at"]);
    let revoked_at = metadata_i64(metadata, &["revoked_at_micros", "revoked_at"]);
    let explicit_state = metadata_string(metadata, &["validity_state", "temporal_state"]);
    let explicit_state = explicit_state.map(str::to_ascii_lowercase);
    let deleted = metadata_bool(metadata, &["deleted", "redacted"]).unwrap_or(false);
    if deleted {
        return None;
    }

    let explicit_state = match explicit_state.as_deref() {
        Some("expired") => NativeSessionRecallValidityState::Expired,
        Some("future") => NativeSessionRecallValidityState::Future,
        Some("superseded") => NativeSessionRecallValidityState::Superseded,
        Some("revoked") => NativeSessionRecallValidityState::Revoked,
        Some("unknown") => NativeSessionRecallValidityState::Unknown,
        _ if valid_from.is_none() => NativeSessionRecallValidityState::Unknown,
        _ => NativeSessionRecallValidityState::Current,
    };
    let mut state = explicit_state;

    let temporal_cutoff = match options.temporal {
        NativeSessionRecallTemporal::AsOf { cutoff_micros } => Some(cutoff_micros),
        NativeSessionRecallTemporal::Current | NativeSessionRecallTemporal::History => {
            options.evaluation_time_micros
        }
        NativeSessionRecallTemporal::Interval { .. } => return None,
    };
    if let Some(cutoff) = temporal_cutoff {
        if valid_from.is_some_and(|value| value > cutoff)
            || timestamp.is_some_and(|value| value > cutoff)
        {
            return None;
        }
        if valid_until.is_some_and(|value| value <= cutoff) {
            state = NativeSessionRecallValidityState::Expired;
        }
        if superseded_at.is_some_and(|value| value <= cutoff) {
            state = NativeSessionRecallValidityState::Superseded;
        }
        if revoked_at.is_some_and(|value| value <= cutoff) {
            state = NativeSessionRecallValidityState::Revoked;
        }
    } else if matches!(options.temporal, NativeSessionRecallTemporal::Current) {
        if revoked_at.is_some() {
            state = NativeSessionRecallValidityState::Revoked;
        } else if superseded_at.is_some() {
            state = NativeSessionRecallValidityState::Superseded;
        }
    }

    if matches!(state, NativeSessionRecallValidityState::Future) {
        return None;
    }
    if matches!(state, NativeSessionRecallValidityState::Expired)
        || (matches!(state, NativeSessionRecallValidityState::Superseded)
            && !options.include_superseded)
        || (matches!(state, NativeSessionRecallValidityState::Revoked) && !options.include_revoked)
    {
        return None;
    }

    let unknown = matches!(state, NativeSessionRecallValidityState::Unknown);
    if unknown && options.unknown_validity_policy == CognitiveRecallUnknownValidityPolicy::Exclude {
        return None;
    }
    let degraded = unknown;
    let warnings = if unknown {
        vec!["validity_unknown".to_owned()]
    } else {
        Vec::new()
    };
    Some((
        NativeSessionRecallValidity {
            observed_at_micros: timestamp,
            valid_from_micros: valid_from,
            valid_until_micros: valid_until,
            superseded_at_micros: superseded_at,
            revoked_at_micros: revoked_at,
            source_revision: metadata_string(metadata, &["source_revision"]).map(str::to_owned),
            state,
        },
        warnings,
        degraded,
    ))
}

fn is_excluded(
    exclusions: &CognitiveRecallExclusions,
    candidate_id: &str,
    stable_memory_ref: &str,
    source_refs: &[String],
    trace_refs: &[String],
    observation_anchor: &RetrievalAnchorId,
    source_observation_id: Option<&str>,
    content_sha256: &str,
) -> bool {
    exclusions
        .candidate_ids
        .iter()
        .any(|value| value == candidate_id)
        || exclusions
            .stable_memory_refs
            .iter()
            .any(|value| value == stable_memory_ref)
        || exclusions
            .source_refs
            .iter()
            .any(|value| source_refs.iter().any(|source| source == value))
        || exclusions
            .trace_refs
            .iter()
            .any(|value| trace_refs.iter().any(|trace| trace == value))
        || exclusions.observation_ids.iter().any(|value| {
            value == observation_anchor.as_str()
                || source_observation_id.is_some_and(|observation| observation == value)
        })
        || exclusions
            .content_sha256
            .iter()
            .any(|value| value == content_sha256)
}

fn source_refs(
    metadata: Option<&Value>,
    source_anchor: &str,
    message: &SessionMessageRecord,
) -> Vec<String> {
    let mut refs = vec![source_anchor.to_owned()];
    refs.extend(metadata_string_vec(metadata, "source_refs"));
    if let Some(path) = &message.source_path {
        refs.push(path.clone());
    }
    refs.sort();
    refs.dedup();
    refs
}

fn metadata_string_vec(metadata: Option<&Value>, key: &str) -> Vec<String> {
    let Some(value) = metadata.and_then(|metadata| metadata.get(key)) else {
        return Vec::new();
    };
    match value {
        Value::Array(values) => values
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        Value::String(value) => vec![value.clone()],
        _ => Vec::new(),
    }
}

fn metadata_string<'a>(metadata: Option<&'a Value>, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|key| {
        metadata
            .and_then(|metadata| metadata.get(*key))
            .and_then(Value::as_str)
    })
}

fn metadata_i64(metadata: Option<&Value>, keys: &[&str]) -> Option<i64> {
    keys.iter().find_map(|key| {
        metadata.and_then(|metadata| {
            let value = metadata.get(*key)?;
            value
                .as_i64()
                .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        })
    })
}

fn metadata_bool(metadata: Option<&Value>, keys: &[&str]) -> Option<bool> {
    keys.iter().find_map(|key| {
        metadata
            .and_then(|metadata| metadata.get(*key))
            .and_then(Value::as_bool)
    })
}

fn result_work_units(result: &SessionMessageSearchResult) -> u64 {
    let content_units = result.message.text.len().saturating_add(1023) / 1024;
    let metadata_units = result
        .message
        .metadata_json
        .as_ref()
        .map_or(0, |metadata| metadata.len().saturating_add(1023) / 1024);
    1_u64
        .saturating_add(content_units as u64)
        .saturating_add(metadata_units as u64)
}

fn score_millionths(score: f64) -> u64 {
    if !score.is_finite() || score <= 0.0 {
        return 0;
    }
    let scaled = (score.min(1_000_000.0) * 1_000_000.0).round();
    if scaled >= u64::MAX as f64 {
        u64::MAX
    } else {
        scaled as u64
    }
}

fn sha256_hex(value: &[u8]) -> String {
    hex::encode(Sha256::digest(value))
}

fn deterministic_candidate_order(
    left: &NativeSessionRecallCandidate,
    right: &NativeSessionRecallCandidate,
) -> std::cmp::Ordering {
    right
        .score_millionths
        .cmp(&left.score_millionths)
        .then_with(|| left.observation_anchor.cmp(&right.observation_anchor))
        .then_with(|| left.session_anchor.cmp(&right.session_anchor))
        .then_with(|| left.candidate_id.cmp(&right.candidate_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracedecay_domain::{RetrievalGrainV1, SessionId, TemporalModeV1, UtcMicros};
    use tracedecay_session_runtime::session_retrieval::SessionTemporalMetadataView;
    use tracedecay_temporal_query::context::ContextBudget;
    use tracedecay_temporal_query::ranking::DiversityLimits;

    fn query(mode: TemporalModeV1) -> SessionTemporalQuery {
        SessionTemporalQuery::new(
            SessionId::new("session.adapter.test").unwrap(),
            Some("native".to_owned()),
            "recall",
            None,
            mode,
            RetrievalGrainV1::Occurrence,
            16,
            DiversityLimits::unbounded(),
            ContextBudget {
                max_bytes: 4096,
                max_tokens: 1024,
                estimator_version: "test".to_owned(),
            },
        )
        .unwrap()
    }

    fn result(
        ordinal: i64,
        score: f64,
        text: &str,
        timestamp: Option<i64>,
    ) -> SessionMessageSearchResult {
        SessionMessageSearchResult {
            session: SessionRecord {
                provider: "native".to_owned(),
                session_id: "session.adapter.test".to_owned(),
                project_key: "project".to_owned(),
                project_path: "/project".to_owned(),
                title: None,
                started_at: None,
                ended_at: None,
                transcript_path: None,
                metadata_json: None,
                parent_session_id: None,
                is_subagent: false,
                agent_id: None,
                parent_tool_use_id: None,
            },
            message: SessionMessageRecord {
                provider: "native".to_owned(),
                message_id: format!("message-{ordinal}"),
                session_id: "session.adapter.test".to_owned(),
                role: "user".to_owned(),
                timestamp,
                ordinal,
                text: text.to_owned(),
                kind: None,
                model: None,
                tool_names: None,
                source_path: None,
                source_offset: None,
                metadata_json: Some("{}".to_owned()),
            },
            score,
        }
    }

    fn page(results: Vec<SessionMessageSearchResult>) -> SessionRetrievalPageView {
        let anchors = results
            .iter()
            .map(|result| {
                RetrievalAnchorId::new(format!("observation:{}", result.message.ordinal)).unwrap()
            })
            .collect();
        SessionRetrievalPageView {
            results,
            temporal: SessionTemporalMetadataView {
                anchors,
                ..SessionTemporalMetadataView::default()
            },
        }
    }

    #[test]
    fn projects_canonical_anchors_and_full_content_digest() {
        let result = result(7, 0.5, "hello Native", Some(100));
        let batch = adapt_session_retrieval_page(
            &query(TemporalModeV1::Current),
            page(vec![result]),
            &NativeSessionRecallOptions::default(),
        )
        .unwrap();
        let candidate = &batch.candidates[0];
        assert_eq!(candidate.observation_anchor.as_str(), "observation:7");
        assert_eq!(candidate.source_anchor, candidate.observation_anchor);
        assert_eq!(
            candidate.session_anchor.as_str(),
            "session:session.adapter.test#7-7"
        );
        assert_eq!(candidate.content_sha256, sha256_hex(b"hello Native"));
        assert_eq!(candidate.score_millionths, 500_000);
        assert_eq!(batch.status, NativeSessionRecallBatchStatus::Complete);
    }

    #[test]
    fn as_of_rejects_future_messages_and_interval_is_typed_unsupported() {
        let as_of = query(TemporalModeV1::AsOf {
            cutoff: UtcMicros(100),
        });
        let mut options = NativeSessionRecallOptions::default();
        options.temporal = NativeSessionRecallTemporal::AsOf { cutoff_micros: 100 };
        let batch = adapt_session_retrieval_page(
            &as_of,
            page(vec![result(1, 1.0, "future", Some(101))]),
            &options,
        )
        .unwrap();
        assert!(batch.candidates.is_empty());
        assert_eq!(batch.excluded_items, 1);

        options.temporal = NativeSessionRecallTemporal::Interval {
            start_micros: 1,
            end_micros: 2,
        };
        assert_eq!(
            adapt_session_retrieval_page(&as_of, page(Vec::new()), &options),
            Err(NativeSessionRecallAdapterError::Unsupported(
                NativeSessionRecallUnsupported::Interval
            ))
        );
    }

    #[test]
    fn admission_precedes_candidate_and_byte_ceilings() {
        let first = result(1, 1.0, "excluded", Some(10));
        let second = result(2, 0.9, "too-large", Some(20));
        let third = result(3, 0.8, "kept", Some(30));
        let mut options = NativeSessionRecallOptions::default();
        options.limits = NativeSessionRecallLimits {
            maximum_candidates: 1,
            maximum_candidate_content_bytes: 8,
            maximum_total_content_bytes: 8,
            maximum_work_units: 100,
        };
        let first_anchor = RetrievalAnchorId::new("observation:1").unwrap();
        options
            .exclusions
            .observation_ids
            .push(first_anchor.to_string());
        let batch = adapt_session_retrieval_page(
            &query(TemporalModeV1::Current),
            page(vec![first, second, third]),
            &options,
        )
        .unwrap();
        assert_eq!(batch.candidates.len(), 1);
        assert_eq!(batch.candidates[0].content, "kept");
        assert_eq!(batch.admitted_items, 2);
        assert_eq!(
            batch.status,
            NativeSessionRecallBatchStatus::Partial { omitted: 1 }
        );
    }

    #[test]
    fn tie_order_is_independent_of_page_order() {
        let left = result(1, 0.5, "left", Some(1));
        let right = result(2, 0.5, "right", Some(2));
        let options = NativeSessionRecallOptions::default();
        let forward = adapt_session_retrieval_page(
            &query(TemporalModeV1::Current),
            page(vec![left.clone(), right.clone()]),
            &options,
        )
        .unwrap();
        let reverse = adapt_session_retrieval_page(
            &query(TemporalModeV1::Current),
            page(vec![right, left]),
            &options,
        )
        .unwrap();
        let forward_ids: Vec<_> = forward
            .candidates
            .iter()
            .map(|candidate| candidate.candidate_id.clone())
            .collect();
        let reverse_ids: Vec<_> = reverse
            .candidates
            .iter()
            .map(|candidate| candidate.candidate_id.clone())
            .collect();
        assert_eq!(forward_ids, reverse_ids);
    }

    #[test]
    fn malformed_page_is_typed_invalid() {
        let mut page = page(vec![result(1, 1.0, "one", Some(1))]);
        page.temporal.anchors.clear();
        assert_eq!(
            adapt_session_retrieval_page(
                &query(TemporalModeV1::Current),
                page,
                &NativeSessionRecallOptions::default(),
            ),
            Err(NativeSessionRecallAdapterError::InvalidPage(
                NativeSessionRecallInvalidPage::AnchorResultCountMismatch
            ))
        );
    }
}
