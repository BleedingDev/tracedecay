//! Immutable session/Git evidence projected into verified graph generations.
//!
//! Commit/session evidence and branch/worktree spans are not relational rows.
//! A complete [`GitEvidenceProjectionV1`] is published atomically through the
//! project graph runtime and every query is evaluated from its verified
//! snapshot. SQLite retains only resumable-history receipts and watermarks.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use serde::{Deserialize, Serialize};
use tracedecay_domain::canonical_text::sha256_hex;
use tracedecay_domain::{
    CanonicalGitEvidenceKindV1, CanonicalObservationEnvelopeV1, CanonicalObservationFactV1,
};
use tracedecay_runtime_core::db::engine::{Executor, QueryExecutor, Value, params};

use super::SessionMessageRecord;

mod error;
pub use error::GitCorrelationError;

const MIGRATION_NAME: &str = "git_correlation";
const MESSAGE_WORKTREE_KEYS: [&str; 9] = [
    "codex_turn_worktree",
    "claude_message_worktree",
    "cursor_session_worktree",
    "kiro_workspace_worktree",
    "cline_like_task_worktree",
    "vibe_session_worktree",
    "codex_session_worktree",
    "claude_session_worktree",
    "hermes_session_worktree",
];

/// Receipt schema version. This schema owns only convergence receipts and
/// watermarks; Git evidence itself remains in the verified graph authority.
pub const GIT_CORRELATION_SCHEMA_VERSION: i64 = 5;

const GIT_CORRELATION_FINAL_TABLES: [&str; 9] = [
    "git_correlation_meta",
    "git_evidence_publication_outbox",
    "git_history_index_progress",
    "git_history_index_segments",
    "git_history_index_pending",
    "git_history_index_seen",
    "git_history_index_staged_spans",
    "git_history_index_staged_commits",
    "git_history_index_failures",
];

const GIT_CORRELATION_FINAL_SCHEMA_OBJECTS: [(&str, &str, &str); 11] = [
    ("table", "git_correlation_meta", "git_correlation_meta"),
    (
        "table",
        "git_evidence_publication_outbox",
        "git_evidence_publication_outbox",
    ),
    (
        "table",
        "git_history_index_progress",
        "git_history_index_progress",
    ),
    (
        "table",
        "git_history_index_segments",
        "git_history_index_segments",
    ),
    (
        "table",
        "git_history_index_pending",
        "git_history_index_pending",
    ),
    ("table", "git_history_index_seen", "git_history_index_seen"),
    (
        "table",
        "git_history_index_staged_spans",
        "git_history_index_staged_spans",
    ),
    (
        "table",
        "git_history_index_staged_commits",
        "git_history_index_staged_commits",
    ),
    (
        "table",
        "git_history_index_failures",
        "git_history_index_failures",
    ),
    (
        "index",
        "idx_git_evidence_publication_outbox_pending",
        "git_evidence_publication_outbox",
    ),
    (
        "trigger",
        "git_evidence_publication_outbox_immutable",
        "git_evidence_publication_outbox",
    ),
];

// `(name, declared type, not-null flag, default, primary-key ordinal, hidden)`
// is the part of SQLite's table shape that can be compared without running
// any write. Constraints remain owned by the installer, while this catches
// dropped, added, retyped, or re-keyed columns before a final store is used.
const GIT_CORRELATION_FINAL_TABLE_COLUMNS: &[(
    &str,
    &[(&str, &str, i64, Option<&str>, i64, i64)],
)] = &[
    (
        "git_correlation_meta",
        &[
            ("key", "TEXT", 0, None, 1, 0),
            ("value", "INTEGER", 1, None, 0, 0),
            ("updated_at", "INTEGER", 1, Some("unixepoch()"), 0, 0),
        ],
    ),
    (
        "git_evidence_publication_outbox",
        &[
            ("receipt_id", "TEXT", 0, None, 1, 0),
            ("publication_prefix", "TEXT", 1, None, 0, 0),
            ("evidence_json", "TEXT", 1, None, 0, 0),
            ("created_at", "INTEGER", 1, Some("unixepoch()"), 0, 0),
        ],
    ),
    (
        "git_history_index_progress",
        &[
            ("activity_timestamp", "INTEGER", 1, None, 0, 0),
            ("source_rowid", "INTEGER", 1, None, 1, 0),
            ("provider", "TEXT", 1, None, 0, 0),
            ("session_id", "TEXT", 1, None, 0, 0),
            ("project_path", "TEXT", 1, None, 0, 0),
            ("window_start", "INTEGER", 1, None, 0, 0),
            ("window_end", "INTEGER", 1, None, 0, 0),
            ("worktree", "BLOB", 1, None, 0, 0),
            ("worktree_identity", "BLOB", 1, None, 0, 0),
            ("git_dir", "BLOB", 1, None, 0, 0),
            ("git_dir_identity", "BLOB", 1, None, 0, 0),
            ("common_dir", "BLOB", 1, None, 0, 0),
            ("common_dir_identity", "BLOB", 1, None, 0, 0),
            ("generation", "INTEGER", 1, None, 0, 0),
            ("scan_mode", "TEXT", 1, None, 0, 0),
            ("reflog_path", "BLOB", 1, None, 0, 0),
            ("reflog_byte_offset", "INTEGER", 1, None, 0, 0),
            ("reflog_byte_length", "INTEGER", 1, None, 0, 0),
            ("source_generation", "TEXT", 1, None, 0, 0),
            ("reflog_digest", "TEXT", 1, None, 0, 0),
            ("capture_target_offset", "INTEGER", 0, None, 0, 0),
            ("verify_byte_offset", "INTEGER", 1, None, 0, 0),
            ("verify_digest", "TEXT", 1, None, 0, 0),
            ("source_head_referent", "BLOB", 0, None, 0, 0),
            ("source_head_oid", "TEXT", 1, None, 0, 0),
            ("cursor_head_state", "TEXT", 1, None, 0, 0),
            ("cursor_head_branch", "TEXT", 0, None, 0, 0),
            ("cursor_oid", "TEXT", 1, None, 0, 0),
            ("segment_end", "INTEGER", 1, None, 0, 0),
            ("segment_tip_oid", "TEXT", 1, None, 0, 0),
            ("segment_cursor", "INTEGER", 1, None, 0, 0),
            ("emitted_count", "INTEGER", 1, None, 0, 0),
            ("consulted_ref_seal_json", "TEXT", 1, None, 0, 0),
        ],
    ),
    (
        "git_history_index_segments",
        &[
            ("source_rowid", "INTEGER", 1, None, 1, 0),
            ("ordinal", "INTEGER", 1, None, 2, 0),
            ("branch", "TEXT", 0, None, 0, 0),
            ("start_ts", "INTEGER", 1, None, 0, 0),
            ("end_ts", "INTEGER", 1, None, 0, 0),
            ("tip_oid", "TEXT", 1, None, 0, 0),
            ("applied", "INTEGER", 1, Some("0"), 0, 0),
            ("completed", "INTEGER", 1, Some("0"), 0, 0),
        ],
    ),
    (
        "git_history_index_pending",
        &[
            ("source_rowid", "INTEGER", 1, None, 1, 0),
            ("segment_ordinal", "INTEGER", 1, None, 2, 0),
            ("oid", "TEXT", 1, None, 3, 0),
        ],
    ),
    (
        "git_history_index_seen",
        &[
            ("source_rowid", "INTEGER", 1, None, 1, 0),
            ("segment_ordinal", "INTEGER", 1, None, 2, 0),
            ("oid", "TEXT", 1, None, 3, 0),
        ],
    ),
    (
        "git_history_index_staged_spans",
        &[
            ("source_rowid", "INTEGER", 1, None, 1, 0),
            ("segment_ordinal", "INTEGER", 1, None, 2, 0),
            ("boundary", "INTEGER", 1, None, 3, 0),
            ("branch", "TEXT", 0, None, 0, 0),
            ("timestamp", "INTEGER", 1, None, 0, 0),
        ],
    ),
    (
        "git_history_index_staged_commits",
        &[
            ("source_rowid", "INTEGER", 1, None, 1, 0),
            ("segment_ordinal", "INTEGER", 1, None, 2, 0),
            ("oid", "TEXT", 1, None, 3, 0),
            ("branch", "TEXT", 0, None, 0, 0),
            ("committed_at", "INTEGER", 1, None, 0, 0),
        ],
    ),
    (
        "git_history_index_failures",
        &[
            ("source_rowid", "INTEGER", 1, None, 1, 0),
            ("activity_timestamp", "INTEGER", 1, None, 0, 0),
            ("provider", "TEXT", 1, None, 0, 0),
            ("session_id", "TEXT", 1, None, 0, 0),
            ("project_path", "TEXT", 1, None, 0, 0),
            ("window_start", "INTEGER", 1, None, 0, 0),
            ("window_end", "INTEGER", 1, None, 0, 0),
            ("reason", "TEXT", 1, None, 0, 0),
            ("source_generation", "TEXT", 0, None, 0, 0),
            ("reflog_digest", "TEXT", 0, None, 0, 0),
        ],
    ),
];

const GIT_CORRELATION_FINAL_INDEX_SQL: &str = "CREATE INDEX idx_git_evidence_publication_outbox_pending ON git_evidence_publication_outbox(created_at, receipt_id)";
const GIT_CORRELATION_FINAL_TRIGGER_SQL: &str = "CREATE TRIGGER git_evidence_publication_outbox_immutable BEFORE UPDATE ON git_evidence_publication_outbox BEGIN SELECT RAISE(ABORT, 'Git evidence publication receipt is immutable'); END";
/// Projector revision this build publishes. It is part of the generation
/// identity, so a graph-shape change (index entities, relation keys,
/// projection metadata) re-publishes an unchanged projection under a distinct
/// generation instead of colliding with the previous shape's rows.
pub const GIT_EVIDENCE_PROJECTOR_REVISION: &str = "session-git-evidence-projector.v2";
/// The pre-index projector revision. A verified head that records no projector
/// revision was published under it. Its span and commit rows are identical to
/// the current shape, so full recovery still verifies and merges it, but it
/// carries no query index and answers bounded reads as unavailable until the
/// next publication re-projects it.
pub const GIT_EVIDENCE_LEGACY_PROJECTOR_REVISION_V1: &str = "session-git-evidence-projector.v1";
pub const DEFAULT_SPAN_MERGE_GAP_SECS: i64 = 30 * 60;
pub const DEFAULT_SPAN_OBSERVATION_DEBOUNCE_SECS: i64 = 30;
// The scope value type and session cap are owned by the LCM engine crate so
// its grep filters and this correlation engine narrow by the same rules.
pub use tracedecay_lcm::{GitScopeFilter, MAX_SESSIONS_FOR_LIMIT};
pub const AUTO_BACKFILL_WATERMARK_KEY: &str = "auto_backfill_activity_watermark";
pub const GIT_HISTORY_ROWID_FRONTIER_KEY: &str = "git_history_session_rowid_frontier";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpanSource {
    HookRoute,
    Ingest,
    Backfill,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpanOverlapKind {
    Direct,
    WithinSpan,
    ExtendedWindow,
    Reflog,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitRelation {
    Produced,
    Observed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitEvidence {
    ToolResult,
    HostEvent,
    HeadObservation,
    ReflogOverlap,
    TimeOverlap,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CommitRelationFilter {
    #[default]
    Produced,
    Observed,
    All,
}

impl CommitRelationFilter {
    pub fn parse(value: Option<&str>) -> Result<Self, GitCorrelationError> {
        match value.unwrap_or("produced") {
            "produced" => Ok(Self::Produced),
            "observed" => Ok(Self::Observed),
            "all" => Ok(Self::All),
            other => Err(GitCorrelationError::InvalidArgument(format!(
                "relation must be one of produced, observed, all (got `{other}`)"
            ))),
        }
    }

    #[hotpath::skip]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Produced => "produced",
            Self::Observed => "observed",
            Self::All => "all",
        }
    }

    #[hotpath::skip]
    const fn matches(self, relation: CommitRelation) -> bool {
        matches!(
            (self, relation),
            (Self::All, _)
                | (Self::Produced, CommitRelation::Produced)
                | (Self::Observed, CommitRelation::Observed)
        )
    }
}

/// One immutable activity span entity in the Git evidence projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionGitSpan {
    /// Stable projector-issued identity, not a relational row id.
    pub span_id: String,
    pub provider: String,
    pub session_id: String,
    pub thread_id: Option<String>,
    pub branch: Option<String>,
    pub worktree: String,
    pub first_ts: i64,
    pub last_ts: i64,
    pub event_count: i64,
    pub source: SpanSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpanObservation {
    pub provider: String,
    pub session_id: String,
    pub thread_id: Option<String>,
    pub branch: Option<String>,
    pub worktree: String,
    pub ts: i64,
    pub source: SpanSource,
}

/// Evidence carried by a session-to-commit graph relation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitSessionRecord {
    pub commit_sha: String,
    pub provider: String,
    pub session_id: String,
    pub branch: Option<String>,
    pub worktree: Option<String>,
    pub committed_at: i64,
    pub span_overlap_kind: SpanOverlapKind,
    pub span_id: Option<String>,
    pub relation: CommitRelation,
    pub evidence: CommitEvidence,
    pub confidence: i64,
    pub evidence_message_id: Option<String>,
}

/// Canonical, complete input to one immutable graph generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitEvidenceProjectionV1 {
    source_watermark: String,
    spans: Vec<SessionGitSpan>,
    commit_sessions: Vec<CommitSessionRecord>,
}

impl GitEvidenceProjectionV1 {
    #[hotpath::measure(label = "sessions.git_correlation.projection_new")]
    pub fn new(
        source_watermark: impl Into<String>,
        mut spans: Vec<SessionGitSpan>,
        mut commit_sessions: Vec<CommitSessionRecord>,
    ) -> Result<Self, GitCorrelationError> {
        let source_watermark = source_watermark.into();
        if source_watermark.trim().is_empty() {
            return Err(GitCorrelationError::Contract(
                "Git evidence source watermark must not be empty".to_owned(),
            ));
        }
        for span in &mut spans {
            validate_span(span)?;
            span.worktree = normalize_worktree(&span.worktree);
        }
        for record in &mut commit_sessions {
            validate_commit_record(record)?;
            record.commit_sha = parse_commit_sha(&record.commit_sha)?;
            record.worktree = record.worktree.as_deref().map(normalize_worktree);
        }
        spans.sort_by(|left, right| left.span_id.cmp(&right.span_id));
        commit_sessions.sort_by(commit_record_order);
        if spans
            .windows(2)
            .any(|pair| pair[0].span_id == pair[1].span_id)
        {
            return Err(GitCorrelationError::Contract(
                "Git evidence span identities must be unique".to_owned(),
            ));
        }
        if commit_sessions.windows(2).any(|pair| {
            pair[0].commit_sha == pair[1].commit_sha && pair[0].session_id == pair[1].session_id
        }) {
            return Err(GitCorrelationError::Contract(
                "Git evidence commit/session relations must be unique".to_owned(),
            ));
        }
        let span_ids = spans
            .iter()
            .map(|span| span.span_id.as_str())
            .collect::<HashSet<_>>();
        if commit_sessions.iter().any(|record| {
            record
                .span_id
                .as_deref()
                .is_some_and(|span_id| !span_ids.contains(span_id))
        }) {
            return Err(GitCorrelationError::Contract(
                "Git evidence relation references an absent span".to_owned(),
            ));
        }
        canonical_providers(&mut spans, &mut commit_sessions)?;
        Ok(Self {
            source_watermark,
            spans,
            commit_sessions,
        })
    }

    pub fn source_watermark(&self) -> &str {
        &self.source_watermark
    }

    pub fn spans(&self) -> &[SessionGitSpan] {
        &self.spans
    }

    pub fn commit_sessions(&self) -> &[CommitSessionRecord] {
        &self.commit_sessions
    }

    /// Evaluates the query over the complete in-memory projection. Bounded
    /// production reads go through the indexed graph view
    /// ([`GitEvidenceGraphView`]), which feeds the same aggregation
    /// helpers only the rows that can contribute to the result.
    #[hotpath::measure(label = "sessions.git_correlation.sessions_for")]
    pub fn sessions_for(
        &self,
        query: &SessionsForQuery,
        relation: CommitRelationFilter,
    ) -> Vec<SessionGitCorrelationHit> {
        let limit = sessions_for_limit(query);
        match &query.git_ref {
            GitRefFilter::Branch(_) | GitRefFilter::Worktree(_) => span_hits(
                self.spans
                    .iter()
                    .filter(|span| span_matches_query(span, query)),
                limit,
            ),
            GitRefFilter::Commit(commit) => commit_hits(
                self.commit_sessions
                    .iter()
                    .filter(|record| commit_record_matches_query(record, commit, relation, query)),
                limit,
            ),
        }
    }

    #[hotpath::measure(label = "sessions.git_correlation.session_ids_for_scope")]
    pub fn session_ids_for_scope(&self, filter: &GitScopeFilter) -> Option<Vec<(String, String)>> {
        if filter.is_empty() {
            return None;
        }
        let mut selected: Option<BTreeMap<String, String>> = None;
        if let Some(branch) = &filter.branch {
            selected = Some(intersect_id_maps(
                selected,
                span_identities(
                    self.spans
                        .iter()
                        .filter(|span| span.branch.as_deref() == Some(branch.as_str())),
                ),
            ));
        }
        if let Some(worktree) = &filter.worktree {
            selected = Some(intersect_id_maps(
                selected,
                span_identities(self.spans.iter().filter(|span| &span.worktree == worktree)),
            ));
        }
        if let Some(commit) = &filter.commit {
            selected = Some(intersect_id_maps(
                selected,
                commit_identities_with_producer_fallback(
                    self.commit_sessions
                        .iter()
                        .filter(|record| record.commit_sha.starts_with(commit.as_str())),
                ),
            ));
        }
        Some(scope_session_ids(selected))
    }
}

fn sessions_for_limit(query: &SessionsForQuery) -> usize {
    query.limit.clamp(1, MAX_SESSIONS_FOR_LIMIT)
}

/// Whether `span` carries the queried branch or worktree. A commit query never
/// matches a span.
fn span_matches_ref(span: &SessionGitSpan, git_ref: &GitRefFilter) -> bool {
    match git_ref {
        GitRefFilter::Branch(branch) => span.branch.as_deref() == Some(branch.as_str()),
        GitRefFilter::Worktree(worktree) => &span.worktree == worktree,
        GitRefFilter::Commit(_) => false,
    }
}

/// Whether `span` overlaps the query's activity window: it must end at or
/// after `since` and start at or before `until`.
fn span_in_query_window(span: &SessionGitSpan, query: &SessionsForQuery) -> bool {
    query.since.is_none_or(|since| span.last_ts >= since)
        && query.until.is_none_or(|until| span.first_ts <= until)
}

fn span_matches_query(span: &SessionGitSpan, query: &SessionsForQuery) -> bool {
    span_matches_ref(span, &query.git_ref) && span_in_query_window(span, query)
}

fn commit_record_matches_query(
    record: &CommitSessionRecord,
    sha: &str,
    relation: CommitRelationFilter,
    query: &SessionsForQuery,
) -> bool {
    record.commit_sha.starts_with(sha)
        && relation.matches(record.relation)
        && query.since.is_none_or(|since| record.committed_at >= since)
        && query.until.is_none_or(|until| record.committed_at <= until)
}

/// Groups already-matching spans per session, orders the sessions by their
/// latest activity, and keeps the first `limit`.
fn span_hits<'a>(
    spans: impl IntoIterator<Item = &'a SessionGitSpan>,
    limit: usize,
) -> Vec<SessionGitCorrelationHit> {
    let mut grouped = BTreeMap::<&str, Vec<&SessionGitSpan>>::new();
    for span in spans {
        grouped
            .entry(span.session_id.as_str())
            .or_default()
            .push(span);
    }
    let mut hits = grouped
        .into_values()
        .map(|spans| span_hit(&spans))
        .collect::<Vec<_>>();
    hits.sort_by(|left, right| {
        right
            .last_ts
            .cmp(&left.last_ts)
            .then_with(|| left.session_id.cmp(&right.session_id))
    });
    hits.truncate(limit);
    hits
}

/// Keeps the strongest already-matching record per session. Records must
/// arrive in canonical projection order ([`commit_record_order`]) so equal
/// strengths resolve to the same record on every read path.
fn commit_hits<'a>(
    records: impl IntoIterator<Item = &'a CommitSessionRecord>,
    limit: usize,
) -> Vec<SessionGitCorrelationHit> {
    let mut by_session = HashMap::<&str, SessionGitCorrelationHit>::new();
    for record in records {
        let candidate = commit_hit(record);
        match by_session.get_mut(record.session_id.as_str()) {
            Some(existing) if commit_hit_strength(&candidate) > commit_hit_strength(existing) => {
                *existing = candidate;
            }
            Some(existing) if existing.provider.is_empty() && !candidate.provider.is_empty() => {
                existing.provider = candidate.provider;
            }
            Some(_) => {}
            None => {
                by_session.insert(record.session_id.as_str(), candidate);
            }
        }
    }
    let mut hits = by_session.into_values().collect::<Vec<_>>();
    hits.sort_by(|left, right| {
        right
            .committed_at
            .cmp(&left.committed_at)
            .then_with(|| left.session_id.cmp(&right.session_id))
    });
    hits.truncate(limit);
    hits
}

/// Session identities named by already-matching spans, keyed by session and
/// carrying the first non-empty provider.
fn span_identities<'a>(
    spans: impl IntoIterator<Item = &'a SessionGitSpan>,
) -> BTreeMap<String, String> {
    spans.into_iter().fold(BTreeMap::new(), |mut ids, span| {
        ids.entry(span.session_id.clone())
            .and_modify(|provider| {
                if provider.is_empty() {
                    provider.clone_from(&span.provider);
                }
            })
            .or_insert_with(|| span.provider.clone());
        ids
    })
}

/// Session identities named by already prefix-matching commit records. When
/// any producer relation exists only producers count; otherwise observers do.
fn commit_identities_with_producer_fallback<'a>(
    records: impl IntoIterator<Item = &'a CommitSessionRecord>,
) -> BTreeMap<String, String> {
    let matching = records.into_iter().collect::<Vec<_>>();
    let has_producer = matching
        .iter()
        .any(|record| record.relation == CommitRelation::Produced);
    matching
        .into_iter()
        .filter(|record| !has_producer || record.relation == CommitRelation::Produced)
        .map(|record| (record.session_id.clone(), record.provider.clone()))
        .collect()
}

fn scope_session_ids(selected: Option<BTreeMap<String, String>>) -> Vec<(String, String)> {
    selected
        .unwrap_or_default()
        .into_iter()
        .map(|(session_id, provider)| (provider, session_id))
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GitRefFilter {
    Branch(String),
    Worktree(String),
    Commit(String),
}

impl GitRefFilter {
    pub fn parse(kind: &str, value: &str) -> Result<Self, GitCorrelationError> {
        let value = value.trim();
        if value.is_empty() {
            return Err(GitCorrelationError::InvalidArgument(
                "value must be a non-empty string".to_owned(),
            ));
        }
        match kind {
            "branch" => Ok(Self::Branch(value.to_owned())),
            "worktree" => Ok(Self::Worktree(normalize_worktree(value))),
            "commit" => parse_commit_sha(value).map(Self::Commit),
            other => Err(GitCorrelationError::InvalidArgument(format!(
                "git_ref must be one of branch, worktree, commit (got `{other}`)"
            ))),
        }
    }

    #[hotpath::skip]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Branch(_) => "branch",
            Self::Worktree(_) => "worktree",
            Self::Commit(_) => "commit",
        }
    }

    pub fn value(&self) -> &str {
        match self {
            Self::Branch(value) | Self::Worktree(value) | Self::Commit(value) => value,
        }
    }
}

/// Parse and normalize raw scope arguments into a [`GitScopeFilter`].
///
/// The value type lives in `tracedecay-lcm`; this constructor stays here
/// because worktree normalization and commit-SHA validation are
/// correlation-engine rules.
pub fn git_scope_filter_from_args(
    branch: Option<&str>,
    worktree: Option<&str>,
    commit: Option<&str>,
) -> Result<GitScopeFilter, GitCorrelationError> {
    Ok(GitScopeFilter {
        branch: nonempty(branch).map(str::to_owned),
        worktree: nonempty(worktree).map(normalize_worktree),
        commit: nonempty(commit).map(parse_commit_sha).transpose()?,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionsForQuery {
    pub git_ref: GitRefFilter,
    pub since: Option<i64>,
    pub until: Option<i64>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionGitCorrelationHit {
    pub provider: String,
    pub session_id: String,
    pub branch: Option<String>,
    pub worktree: Option<String>,
    pub first_ts: Option<i64>,
    pub last_ts: Option<i64>,
    pub event_count: i64,
    pub span_count: i64,
    pub sources: Vec<String>,
    pub commit_sha: Option<String>,
    pub committed_at: Option<i64>,
    pub span_overlap_kind: Option<SpanOverlapKind>,
    pub relation: Option<CommitRelation>,
    pub evidence: Option<CommitEvidence>,
    pub confidence: Option<i64>,
    pub evidence_message_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CorrelationIndexHealth {
    pub projection_available: bool,
    pub generation: Option<String>,
    pub source_watermark: Option<String>,
    pub span_count: u64,
    pub commit_count: u64,
    pub backfill_watermark: Option<i64>,
}

impl CorrelationIndexHealth {
    #[hotpath::skip]
    pub const fn is_empty(&self) -> bool {
        self.span_count == 0
    }

    #[hotpath::skip]
    pub const fn is_empty_for(&self, git_ref: &GitRefFilter) -> bool {
        match git_ref {
            GitRefFilter::Branch(_) | GitRefFilter::Worktree(_) => self.span_count == 0,
            GitRefFilter::Commit(_) => self.commit_count == 0,
        }
    }
}

/// Bounded row-family presence for the session/Git evidence projection.
///
/// Query paths need only distinguish a projection with no applicable evidence
/// from a populated projection that produced no matches. They must not pay for
/// exact health counts, which remain a diagnostics concern.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CorrelationIndexPresence {
    pub projection_available: bool,
    pub generation: Option<String>,
    pub source_watermark: Option<String>,
    pub spans_present: bool,
    pub commits_present: bool,
    pub backfill_watermark: Option<i64>,
}

impl CorrelationIndexPresence {
    #[hotpath::skip]
    pub const fn is_empty_for(&self, git_ref: &GitRefFilter) -> bool {
        match git_ref {
            GitRefFilter::Branch(_) | GitRefFilter::Worktree(_) => !self.spans_present,
            GitRefFilter::Commit(_) => !self.commits_present,
        }
    }
}

pub fn normalize_worktree(path: &str) -> String {
    let mut normalized = path.trim().replace('\\', "/");
    if let Some(stripped) = normalized.strip_prefix("//?/UNC/") {
        normalized = format!("//{stripped}");
    } else if let Some(stripped) = normalized.strip_prefix("//?/") {
        normalized = stripped.to_owned();
    }
    if let Some(stripped) = normalized.strip_prefix("/private/var/") {
        normalized = format!("/var/{stripped}");
    }
    while normalized.len() > 1 && normalized.ends_with('/') {
        normalized.pop();
    }
    normalized
}

pub fn observation_extends_span(first_ts: i64, last_ts: i64, ts: i64, gap_secs: i64) -> bool {
    ts >= first_ts.saturating_sub(gap_secs) && ts <= last_ts.saturating_add(gap_secs)
}

#[derive(Debug, Default)]
pub struct SpanObservationDebounce {
    last_write: HashMap<String, i64>,
}

impl SpanObservationDebounce {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn should_record(&mut self, key: &str, ts: i64, min_interval_secs: i64) -> bool {
        let stale_before = ts.saturating_sub(min_interval_secs);
        self.last_write
            .retain(|stored, last| stored == key || *last > stale_before);
        if self
            .last_write
            .get(key)
            .is_some_and(|last| ts >= *last && ts - *last < min_interval_secs)
        {
            return false;
        }
        self.last_write.insert(key.to_owned(), ts);
        true
    }
}

pub fn span_debounce_key(
    provider: &str,
    session_id: &str,
    branch: Option<&str>,
    worktree: &str,
) -> String {
    digest_bytes(
        format!(
            "{provider}\u{1f}{session_id}\u{1f}{}\u{1f}{worktree}",
            branch.unwrap_or("\u{0}")
        )
        .as_bytes(),
    )
}

/// Parses each message's `metadata_json` once for both commit evidence and
/// ingest span observations. The repository is discovered only after a
/// message actually carries commit candidates.
#[hotpath::measure(label = "sessions.git_correlation.transcript_evidence")]
pub fn transcript_git_evidence(
    messages: &[SessionMessageRecord],
    project_root: &std::path::Path,
) -> (Vec<CommitSessionRecord>, Vec<SpanObservation>) {
    let mut repo: Option<gix::Repository> = None;
    let mut repo_unavailable = false;
    let mut records = BTreeMap::<(String, String), CommitSessionRecord>::new();
    let mut spans = Vec::new();
    for message in messages {
        let Some(parsed) = parsed_message_metadata(message) else {
            continue;
        };
        let Some(metadata) = parsed.as_object() else {
            continue;
        };
        if let Some(span) = span_observation_from_metadata(message, metadata) {
            spans.push(span);
        }
        if repo_unavailable {
            continue;
        }
        for (key, relation, default_evidence, confidence) in [
            (
                "produced_commit_candidates",
                CommitRelation::Produced,
                CommitEvidence::ToolResult,
                100,
            ),
            (
                "observed_commit_candidates",
                CommitRelation::Observed,
                CommitEvidence::HeadObservation,
                60,
            ),
        ] {
            let Some(candidates) = metadata.get(key).and_then(serde_json::Value::as_array) else {
                continue;
            };
            let repo = match &mut repo {
                Some(repo) => repo,
                slot => match gix::discover(project_root) {
                    Ok(discovered) => slot.insert(discovered),
                    Err(_) => {
                        // Keep spans already collected; later messages can
                        // still contribute observations without commit rows.
                        repo_unavailable = true;
                        break;
                    }
                },
            };
            for candidate in candidates.iter().filter_map(serde_json::Value::as_str) {
                let Ok(spec) = repo.rev_parse_single(candidate) else {
                    continue;
                };
                let Ok(object) = spec.object() else {
                    continue;
                };
                let Ok(commit) = object.try_into_commit() else {
                    continue;
                };
                let sha = commit.id.to_string();
                let evidence = if relation == CommitRelation::Produced
                    && metadata
                        .get("produced_commit_evidence")
                        .and_then(serde_json::Value::as_str)
                        == Some("host_event")
                {
                    CommitEvidence::HostEvent
                } else {
                    default_evidence
                };
                let record = CommitSessionRecord {
                    commit_sha: sha.clone(),
                    provider: message.provider.clone(),
                    session_id: message.session_id.clone(),
                    branch: metadata
                        .get("git_branch")
                        .or_else(|| metadata.get("codex_git_branch"))
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned),
                    worktree: metadata_worktree(metadata)
                        .map(normalize_worktree)
                        .or_else(|| Some(normalize_worktree(&project_root.to_string_lossy()))),
                    committed_at: commit
                        .time()
                        .map_or(message.timestamp.unwrap_or_default(), |time| time.seconds),
                    span_overlap_kind: SpanOverlapKind::Direct,
                    span_id: None,
                    relation,
                    evidence,
                    confidence,
                    evidence_message_id: Some(message.message_id.clone()),
                };
                let slot = (sha, message.session_id.clone());
                if records
                    .get(&slot)
                    .is_none_or(|existing| record_strength(&record) > record_strength(existing))
                {
                    records.insert(slot, record);
                }
            }
        }
    }
    (records.into_values().collect(), spans)
}

/// Derives Git evidence from one privacy-approved canonical observation.
///
/// The envelope contributes only typed Git facts and native identity/time.
/// Worktree identity comes exclusively from the daemon-admitted repository
/// root. Commit facts become relations only when the referenced object resolves
/// independently to a commit in that admitted repository.
#[hotpath::measure(label = "sessions.git_correlation.canonical_observation_evidence")]
pub fn canonical_observation_git_evidence(
    sanitized_payload: &serde_json::Value,
    admitted_project_root: &std::path::Path,
) -> Result<(Vec<CommitSessionRecord>, Vec<SpanObservation>), GitCorrelationError> {
    let envelope: CanonicalObservationEnvelopeV1 =
        serde_json::from_value(sanitized_payload.clone()).map_err(|error| {
            GitCorrelationError::Contract(format!(
                "sanitized canonical observation is invalid: {error}"
            ))
        })?;
    let mut branch = None;
    let mut commit_references = BTreeSet::new();
    for fact in envelope.facts() {
        let CanonicalObservationFactV1::Git {
            evidence_kind,
            reference: Some(reference),
            ..
        } = fact
        else {
            continue;
        };
        match evidence_kind {
            CanonicalGitEvidenceKindV1::Branch if !reference.trim().is_empty() => {
                branch.get_or_insert_with(|| reference.clone());
            }
            CanonicalGitEvidenceKindV1::Commit if !reference.trim().is_empty() => {
                commit_references.insert(reference.clone());
            }
            _ => {}
        }
    }
    if branch.is_none() && commit_references.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let provider = envelope.provider().as_str().to_owned();
    let session_id = envelope.relations().session_id().as_str().to_owned();
    let timestamp = envelope.evidence().native_timestamp();
    let worktree = normalize_worktree(&admitted_project_root.to_string_lossy());
    let spans = timestamp
        .map(|ts| {
            vec![SpanObservation {
                provider: provider.clone(),
                session_id: session_id.clone(),
                thread_id: envelope
                    .relations()
                    .thread_id()
                    .map(|thread_id| thread_id.as_str().to_owned()),
                branch: branch.clone(),
                worktree: worktree.clone(),
                ts,
                source: SpanSource::Ingest,
            }]
        })
        .unwrap_or_default();

    let repo = gix::discover(admitted_project_root).map_err(|error| {
        GitCorrelationError::Unavailable(format!(
            "admitted repository could not be opened for canonical commit evidence: {error}"
        ))
    })?;
    let mut commits = Vec::new();
    for reference in commit_references {
        let Ok(prefix) = gix::hash::Prefix::from_hex(reference.as_str()) else {
            // A non-hex historical value is not independently verifiable
            // commit evidence.
            continue;
        };
        let object_id = match repo.objects.lookup_prefix(prefix, None) {
            Ok(Some(Ok(object_id))) => object_id,
            // Missing and ambiguous historical prefixes are unresolved, so
            // neither is evidence for a particular commit.
            Ok(None | Some(Err(()))) => continue,
            Err(error) => {
                return Err(GitCorrelationError::Unavailable(format!(
                    "canonical commit prefix `{reference}` could not be read: {error}"
                )));
            }
        };
        let object = match repo.try_find_object(object_id) {
            Ok(Some(object)) => object,
            Ok(None) => {
                return Err(GitCorrelationError::Unavailable(format!(
                    "canonical commit `{reference}` disappeared after prefix resolution"
                )));
            }
            Err(error) => {
                return Err(GitCorrelationError::Unavailable(format!(
                    "canonical commit object `{reference}` could not be read: {error}"
                )));
            }
        };
        let commit = match object.try_into_commit() {
            Ok(commit) => commit,
            // A provider may retain another Git object identifier. It is not
            // independently verified commit evidence.
            Err(_) => continue,
        };
        let commit_time = commit.time().map_err(|error| {
            GitCorrelationError::Corrupt(format!(
                "canonical commit `{reference}` timestamp could not be decoded: {error}"
            ))
        })?;
        let commit_sha = commit.id.to_string();
        commits.push(CommitSessionRecord {
            commit_sha,
            provider: provider.clone(),
            session_id: session_id.clone(),
            branch: branch.clone(),
            worktree: Some(worktree.clone()),
            committed_at: commit_time.seconds,
            span_overlap_kind: SpanOverlapKind::Direct,
            span_id: None,
            relation: CommitRelation::Observed,
            evidence: CommitEvidence::HeadObservation,
            confidence: 60,
            evidence_message_id: envelope
                .relations()
                .message_id()
                .map(|message_id| message_id.as_str().to_owned()),
        });
    }
    Ok((commits, spans))
}

fn parsed_message_metadata(message: &SessionMessageRecord) -> Option<serde_json::Value> {
    message
        .metadata_json
        .as_deref()
        .and_then(|json| serde_json::from_str(json).ok())
}

fn span_observation_from_metadata(
    message: &SessionMessageRecord,
    metadata: &serde_json::Map<String, serde_json::Value>,
) -> Option<SpanObservation> {
    let timestamp = message.timestamp?;
    let worktree = metadata_worktree(metadata).filter(|path| !path.is_empty())?;
    Some(SpanObservation {
        provider: message.provider.clone(),
        session_id: message.session_id.clone(),
        thread_id: metadata
            .get("turn_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        branch: metadata
            .get("git_branch")
            .or_else(|| metadata.get("codex_git_branch"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        worktree: normalize_worktree(worktree),
        ts: timestamp,
        source: SpanSource::Ingest,
    })
}

/// Installs only relational receipts used by bounded history convergence.
#[hotpath::measure(label = "sessions.git_correlation.ensure_schema", future = true)]
pub async fn ensure_git_correlation_receipt_schema_in_transaction(
    conn: &(impl Executor + ?Sized),
) -> Result<(), GitCorrelationError> {
    match inspect_git_correlation_schema(conn).await? {
        GitCorrelationSchemaAdmission::Current => return Ok(()),
        GitCorrelationSchemaAdmission::Fresh => {}
    }

    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS session_schema_migrations (
            name TEXT PRIMARY KEY,
            version INTEGER NOT NULL,
            applied_at INTEGER NOT NULL DEFAULT (unixepoch())
        );
        CREATE TABLE IF NOT EXISTS git_correlation_meta (
            key TEXT PRIMARY KEY,
            value INTEGER NOT NULL,
            updated_at INTEGER NOT NULL DEFAULT (unixepoch())
        );
        CREATE TABLE IF NOT EXISTS git_evidence_publication_outbox (
            receipt_id TEXT PRIMARY KEY CHECK(length(receipt_id) > 0),
            publication_prefix TEXT NOT NULL CHECK(length(publication_prefix) > 0),
            evidence_json TEXT NOT NULL CHECK(length(evidence_json) > 0),
            created_at INTEGER NOT NULL DEFAULT (unixepoch())
        );
        CREATE INDEX IF NOT EXISTS idx_git_evidence_publication_outbox_pending
            ON git_evidence_publication_outbox(created_at, receipt_id);
        CREATE TRIGGER IF NOT EXISTS git_evidence_publication_outbox_immutable
        BEFORE UPDATE ON git_evidence_publication_outbox
        BEGIN
            SELECT RAISE(ABORT, 'Git evidence publication receipt is immutable');
        END;",
    )
    .await?;
    backfill::history_progress::install_final_schema(conn).await?;
    backfill::history_failures::install_final_schema(conn).await?;
    conn.execute(
        "INSERT INTO session_schema_migrations(name, version)
         VALUES (?1, ?2)",
        params![MIGRATION_NAME, GIT_CORRELATION_SCHEMA_VERSION],
    )
    .await?;
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GitCorrelationSchemaAdmission {
    Current,
    Fresh,
}

fn git_correlation_schema_reset(found_version: Option<i64>) -> GitCorrelationError {
    GitCorrelationError::ResetRequired {
        found_version,
        required_version: GIT_CORRELATION_SCHEMA_VERSION,
    }
}

/// Classifies the receipt namespace before the first schema write.
///
/// A missing Git marker is fresh only when no Git receipt objects exist. A
/// marker from a released version, a future marker, or any partial/legacy
/// namespace is a reset-required store. The read-only probe intentionally
/// does not create `session_schema_migrations`, so refusal leaves the target
/// byte-for-byte unchanged.
async fn inspect_git_correlation_schema(
    conn: &(impl QueryExecutor + ?Sized),
) -> Result<GitCorrelationSchemaAdmission, GitCorrelationError> {
    let Some(found_version) = stored_git_correlation_schema_version(conn).await? else {
        return if git_correlation_objects(conn).await?.is_empty() {
            Ok(GitCorrelationSchemaAdmission::Fresh)
        } else {
            Err(git_correlation_schema_reset(None))
        };
    };

    if found_version != GIT_CORRELATION_SCHEMA_VERSION {
        return Err(git_correlation_schema_reset(Some(found_version)));
    }

    if final_git_correlation_schema_is_intact(conn).await? {
        Ok(GitCorrelationSchemaAdmission::Current)
    } else {
        Err(git_correlation_schema_reset(Some(found_version)))
    }
}

async fn stored_git_correlation_schema_version(
    conn: &(impl QueryExecutor + ?Sized),
) -> Result<Option<i64>, GitCorrelationError> {
    let mut kind_rows = conn
        .query(
            "SELECT type FROM sqlite_master WHERE name = 'session_schema_migrations'",
            (),
        )
        .await?;
    let Some(kind_row) = kind_rows.next().await? else {
        return Ok(None);
    };
    let kind: String = kind_row.get(0)?;
    if kind != "table" {
        return Err(git_correlation_schema_reset(None));
    }

    let mut rows = conn
        .query(
            "SELECT version FROM session_schema_migrations WHERE name = ?1",
            params![MIGRATION_NAME],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    match row.get::<Value>(0)? {
        Value::Integer(version) => Ok(Some(version)),
        Value::Null | Value::Real(_) | Value::Text(_) | Value::Blob(_) => {
            Err(git_correlation_schema_reset(None))
        }
    }
}

async fn git_correlation_objects(
    conn: &(impl QueryExecutor + ?Sized),
) -> Result<BTreeSet<(String, String, String)>, GitCorrelationError> {
    let mut rows = conn
        .query(
            "SELECT type, name, tbl_name
             FROM sqlite_master
             WHERE name NOT LIKE 'sqlite_%'",
            (),
        )
        .await?;
    let mut objects = BTreeSet::new();
    while let Some(row) = rows.next().await? {
        let kind: String = row.get(0)?;
        let name: String = row.get(1)?;
        let table: String = row.get(2)?;
        if is_git_correlation_object(&name, &table) {
            objects.insert((kind.to_ascii_lowercase(), name, table));
        }
    }
    Ok(objects)
}

fn is_git_correlation_object(name: &str, table: &str) -> bool {
    let name = name.to_ascii_lowercase();
    let table = table.to_ascii_lowercase();
    GIT_CORRELATION_FINAL_TABLES
        .iter()
        .any(|final_table| final_table.eq_ignore_ascii_case(&table))
        || name.starts_with("git_correlation_")
        || name.starts_with("git_evidence_")
        || name.starts_with("git_history_index_")
        || name.starts_with("idx_git_evidence_")
        || name.starts_with("idx_git_history_index_")
        || name.starts_with("idx_session_git_")
        || name.starts_with("session_git_")
        || name == "commit_sessions"
        || name.starts_with("commit_sessions_")
}

fn expected_git_correlation_objects() -> BTreeSet<(String, String, String)> {
    GIT_CORRELATION_FINAL_SCHEMA_OBJECTS
        .iter()
        .map(|(kind, name, table)| ((*kind).to_owned(), (*name).to_owned(), (*table).to_owned()))
        .collect()
}

async fn final_git_correlation_schema_is_intact(
    conn: &(impl QueryExecutor + ?Sized),
) -> Result<bool, GitCorrelationError> {
    if git_correlation_objects(conn).await? != expected_git_correlation_objects() {
        return Ok(false);
    }

    for (table, expected_columns) in GIT_CORRELATION_FINAL_TABLE_COLUMNS {
        let mut rows = conn
            .query(
                "SELECT name, type, \"notnull\", dflt_value, pk, hidden
                 FROM pragma_table_xinfo(?1)
                 ORDER BY cid",
                params![*table],
            )
            .await?;
        let mut actual_columns = Vec::new();
        while let Some(row) = rows.next().await? {
            actual_columns.push((
                row.get::<String>(0)?,
                row.get::<String>(1)?,
                row.get::<i64>(2)?,
                row.get::<Option<String>>(3)?,
                row.get::<i64>(4)?,
                row.get::<i64>(5)?,
            ));
        }
        let expected_columns = expected_columns
            .iter()
            .map(
                |(name, declared_type, not_null, default_value, primary_key, hidden)| {
                    (
                        (*name).to_owned(),
                        (*declared_type).to_owned(),
                        *not_null,
                        (*default_value).map(str::to_owned),
                        *primary_key,
                        *hidden,
                    )
                },
            )
            .collect::<Vec<_>>();
        if actual_columns != expected_columns {
            return Ok(false);
        }
    }

    let Some(index_sql) =
        schema_definition(conn, "index", "idx_git_evidence_publication_outbox_pending").await?
    else {
        return Ok(false);
    };
    if normalize_schema_sql(&index_sql) != normalize_schema_sql(GIT_CORRELATION_FINAL_INDEX_SQL) {
        return Ok(false);
    }
    let Some(trigger_sql) =
        schema_definition(conn, "trigger", "git_evidence_publication_outbox_immutable").await?
    else {
        return Ok(false);
    };
    Ok(normalize_schema_sql(&trigger_sql)
        == normalize_schema_sql(GIT_CORRELATION_FINAL_TRIGGER_SQL))
}

async fn schema_definition(
    conn: &(impl QueryExecutor + ?Sized),
    kind: &str,
    name: &str,
) -> Result<Option<String>, GitCorrelationError> {
    let mut rows = conn
        .query(
            "SELECT sql FROM sqlite_master WHERE type = ?1 AND name = ?2",
            params![kind, name],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some(row.get(0)?)),
        None => Ok(None),
    }
}

fn normalize_schema_sql(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[hotpath::measure(label = "sessions.git_correlation.read_meta", future = true)]
pub async fn read_meta_value(
    conn: &(impl QueryExecutor + ?Sized),
    key: &str,
) -> Result<Option<i64>, GitCorrelationError> {
    let mut rows = conn
        .query(
            "SELECT value FROM git_correlation_meta WHERE key = ?1",
            params![key],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| row.get(0).map_err(GitCorrelationError::from))
        .transpose()
}

#[hotpath::measure(label = "sessions.git_correlation.write_meta", future = true)]
pub async fn write_meta_value(
    conn: &(impl Executor + ?Sized),
    key: &str,
    value: i64,
) -> Result<(), GitCorrelationError> {
    conn.execute(
        "INSERT INTO git_correlation_meta(key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = unixepoch()",
        params![key, value],
    )
    .await?;
    Ok(())
}

/// Whether two span provider labels can identify the same session lineage.
///
/// An empty provider is an unattributed observation (hook routes cannot
/// always name the provider); it matches any canonical provider, and the
/// canonical map in [`GitEvidenceProjectionV1::new`] settles the final
/// label. Two distinct non-empty providers never match — one session
/// carrying both is rejected by `canonical_provider_map`.
pub fn providers_compatible(left: &str, right: &str) -> bool {
    left.is_empty() || right.is_empty() || left == right
}

fn validate_span(span: &SessionGitSpan) -> Result<(), GitCorrelationError> {
    if span.span_id.is_empty()
        || span.session_id.is_empty()
        || span.worktree.trim().is_empty()
        || span.first_ts > span.last_ts
        || span.event_count <= 0
    {
        return Err(GitCorrelationError::Contract(
            "Git evidence span is incomplete or has invalid bounds".to_owned(),
        ));
    }
    Ok(())
}

fn validate_commit_record(record: &CommitSessionRecord) -> Result<(), GitCorrelationError> {
    parse_commit_sha(&record.commit_sha)?;
    if record.session_id.is_empty() || !(0..=100).contains(&record.confidence) {
        return Err(GitCorrelationError::Contract(
            "Git commit evidence has an invalid session or confidence".to_owned(),
        ));
    }
    Ok(())
}

fn canonical_providers(
    spans: &mut [SessionGitSpan],
    commits: &mut [CommitSessionRecord],
) -> Result<(), GitCorrelationError> {
    let providers = canonical_provider_map(spans, commits)?;
    for span in spans.iter_mut() {
        if let Some(provider) = providers.get(&span.session_id) {
            span.provider.clone_from(provider);
        }
    }
    for record in commits.iter_mut() {
        if let Some(provider) = providers.get(&record.session_id) {
            record.provider.clone_from(provider);
        }
    }
    Ok(())
}

fn canonical_provider_map(
    spans: &[SessionGitSpan],
    commits: &[CommitSessionRecord],
) -> Result<BTreeMap<String, String>, GitCorrelationError> {
    let mut providers = BTreeMap::new();
    for (session_id, provider) in spans
        .iter()
        .map(|span| (&span.session_id, &span.provider))
        .chain(
            commits
                .iter()
                .map(|record| (&record.session_id, &record.provider)),
        )
    {
        let current = providers
            .entry(session_id.clone())
            .or_insert_with(String::new);
        if current.is_empty() {
            current.clone_from(provider);
        } else if !provider.is_empty() && current != provider {
            return Err(GitCorrelationError::Contract(format!(
                "session `{session_id}` has conflicting providers"
            )));
        }
    }
    Ok(providers)
}

fn commit_record_order(
    left: &CommitSessionRecord,
    right: &CommitSessionRecord,
) -> std::cmp::Ordering {
    left.commit_sha
        .cmp(&right.commit_sha)
        .then_with(|| left.session_id.cmp(&right.session_id))
}

fn digest_bytes(bytes: &[u8]) -> String {
    sha256_hex(bytes)
}

fn metadata_worktree(metadata: &serde_json::Map<String, serde_json::Value>) -> Option<&str> {
    MESSAGE_WORKTREE_KEYS
        .into_iter()
        .find_map(|key| metadata.get(key).and_then(serde_json::Value::as_str))
}

fn parse_commit_sha(value: &str) -> Result<String, GitCorrelationError> {
    if !(6..=64).contains(&value.len()) || !value.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(GitCorrelationError::InvalidArgument(
            "commit must be 6-64 hexadecimal characters".to_owned(),
        ));
    }
    Ok(value.to_ascii_lowercase())
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn intersect_id_maps(
    accumulated: Option<BTreeMap<String, String>>,
    next: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    accumulated.map_or(next.clone(), |existing| {
        existing
            .into_iter()
            .filter(|(session_id, _)| next.contains_key(session_id))
            .collect()
    })
}

fn span_hit(spans: &[&SessionGitSpan]) -> SessionGitCorrelationHit {
    let providers = spans
        .iter()
        .map(|span| span.provider.as_str())
        .filter(|provider| !provider.is_empty())
        .collect::<BTreeSet<_>>();
    let branches = spans
        .iter()
        .filter_map(|span| span.branch.as_deref())
        .collect::<BTreeSet<_>>();
    let worktrees = spans
        .iter()
        .map(|span| span.worktree.as_str())
        .collect::<BTreeSet<_>>();
    let sources = spans
        .iter()
        .map(|span| format!("{:?}", span.source).to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    SessionGitCorrelationHit {
        provider: providers
            .iter()
            .next()
            .copied()
            .unwrap_or_default()
            .to_owned(),
        session_id: spans[0].session_id.clone(),
        branch: one_value(branches),
        worktree: one_value(worktrees),
        first_ts: spans.iter().map(|span| span.first_ts).min(),
        last_ts: spans.iter().map(|span| span.last_ts).max(),
        event_count: spans.iter().map(|span| span.event_count).sum(),
        span_count: i64::try_from(spans.len()).unwrap_or(i64::MAX),
        sources: sources.into_iter().collect(),
        commit_sha: None,
        committed_at: None,
        span_overlap_kind: None,
        relation: None,
        evidence: None,
        confidence: None,
        evidence_message_id: None,
    }
}

fn one_value(values: BTreeSet<&str>) -> Option<String> {
    (values.len() == 1)
        .then(|| values.into_iter().next().map(str::to_owned))
        .flatten()
}

fn commit_hit(record: &CommitSessionRecord) -> SessionGitCorrelationHit {
    SessionGitCorrelationHit {
        provider: record.provider.clone(),
        session_id: record.session_id.clone(),
        branch: record.branch.clone(),
        worktree: record.worktree.clone(),
        first_ts: None,
        last_ts: None,
        event_count: 0,
        span_count: 0,
        sources: Vec::new(),
        commit_sha: Some(record.commit_sha.clone()),
        committed_at: Some(record.committed_at),
        span_overlap_kind: Some(record.span_overlap_kind),
        relation: Some(record.relation),
        evidence: Some(record.evidence),
        confidence: Some(record.confidence),
        evidence_message_id: record.evidence_message_id.clone(),
    }
}

fn record_strength(record: &CommitSessionRecord) -> (u8, i64) {
    (
        u8::from(record.relation == CommitRelation::Produced),
        record.confidence,
    )
}

fn commit_hit_strength(hit: &SessionGitCorrelationHit) -> (u8, i64) {
    (
        u8::from(hit.relation == Some(CommitRelation::Produced)),
        hit.confidence.unwrap_or_default(),
    )
}

mod attribution;
mod backfill;
mod publication_outbox;
mod store;
#[cfg(test)]
pub(crate) use attribution::publish_graph_evidence_controlled;
pub use attribution::{
    CommitAttributionSweepOutcome, ScannedCommit, SpanScanTarget, SpanWindow, TargetScan,
    commit_overlap_kind, graph_evidence_publication_key, match_commit_to_spans,
    publish_graph_evidence, publish_transcript_graph_evidence, run_commit_attribution_sweep,
};
pub use backfill::{
    BackfillOptions, BackfillSkipReason, BackfillStats, BoundedBackfillInterruption,
    BoundedBackfillOutcome, BoundedGitControl, BranchTimelineEntry,
    DEFAULT_AUTO_BACKFILL_SESSIONS_PER_PASS, GitHistoryIndexFrontier, GitReflogSource,
    IncrementalBackfillOutcome, SessionActivityRow, SystemGit, WindowBranchSegment,
    branch_timeline_from_reflog, git_commit_reference_exists, parse_commit_log,
    run_bounded_history_index_page, run_incremental_backfill_outcome, window_branch_segments,
};
pub use backfill::{run_backfill, run_incremental_backfill};
pub use publication_outbox::{
    DEFAULT_GIT_EVIDENCE_PUBLICATION_REPLAY_LIMIT, GitEvidencePublicationReplayOutcome,
    enqueue_git_evidence_publication, pending_git_evidence_publication_count,
    replay_pending_git_evidence_publications, replay_pending_git_evidence_publications_outcome,
};
#[cfg(any(test, feature = "test-helpers"))]
pub use store::legacy_git_evidence_manifest_for_test;
pub use store::{
    AnalyticsSessionTimestamp, AnalyticsSessionTimestampSource, GitCorrelationSessionStore,
    GitCorrelationWriteTxn, GitEvidenceGraphHead, GitEvidenceGraphView, GitEvidenceProjectionStore,
    GitEvidenceProjectorRevision, build_git_evidence_manifest_checked, git_evidence_generation_id,
    git_evidence_projection_identity, open_git_evidence_graph_view,
    publish_git_evidence_projection, recover_git_evidence_projection,
};

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod graph_view_tests;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
