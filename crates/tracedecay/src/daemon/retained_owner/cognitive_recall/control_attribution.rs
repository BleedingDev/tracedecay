//! Retained recall locators and bounded attribution reads from the existing ledger.
//!
//! These values record what the host confirmed for an earlier recall. They never
//! authorize a later action: the caller must resolve current canonical authority.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::TryLockError;
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::{Deserialize, Serialize};
use tracedecay_memory_provider_registry::{
    OperationControl, OwnedExactScope, OwnedProviderId, RecallExplainHostDecisionV1,
    RecallExplainStageV1, RecallExplainTraceV1, RecallOutcomeScopeV1, TerminalCode,
    recall_admission::source_attribution::RecallSourceAttributionV1,
};

use super::{PROJECT_RECALL_BUDGETS, RecallAdmissionLedgerV1};

const TRACE_PREFIX: &str = "recall-trace-v1:";
const ITEM_PREFIX: &str = "recall-item-v1:";
const MAX_SOURCES: usize = 64;
pub(super) const MAX_ID_BYTES: usize = 1_024;
const MAX_SCOPE_BYTES: usize = 16_384;
const MAX_SOURCES_BYTES: usize = 1_048_576;
pub(super) const MAX_DECISION_BYTES: usize = 16_384;

/// Typed refusal of retained attribution; absence never supplies authority.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RecallControlAttributionErrorV1 {
    #[error("invalid retained recall control attribution: {0}")]
    Invalid(&'static str),
    #[error("retained recall control target was not found")]
    NotFound,
    #[error("retained recall has no host-confirmed control attribution")]
    MissingAuthority,
    #[error("retained recall control read stopped: {0:?}")]
    Control(TerminalCode),
    #[error("retained recall control storage read failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("retained recall control metadata could not be encoded or decoded: {0}")]
    Json(#[from] serde_json::Error),
}

type Result<T> = std::result::Result<T, RecallControlAttributionErrorV1>;

/// Versioned address of the ledger's existing `(scope digest, trace id)` key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecallControlTraceRefV1 {
    wire: String,
    exact_scope_sha256: String,
    trace_id: String,
}

impl RecallControlTraceRefV1 {
    pub(crate) fn parse(value: &str) -> Result<Self> {
        if value.len() != TRACE_PREFIX.len() + 129 {
            return Err(invalid("trace reference length"));
        }
        let (scope, trace) = value
            .strip_prefix(TRACE_PREFIX)
            .and_then(|body| body.split_once(':'))
            .ok_or_else(|| invalid("trace reference version"))?;
        require_sha256(scope)?;
        require_sha256(trace)?;
        Ok(Self {
            wire: value.to_owned(),
            exact_scope_sha256: scope.to_owned(),
            trace_id: trace.to_owned(),
        })
    }

    #[cfg(feature = "test-helpers")]
    pub(super) fn trace_id(&self) -> &str {
        &self.trace_id
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.wire
    }
}

/// Original provider rank in the final trace, never a normalization-list rank.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecallControlItemRefV1 {
    wire: String,
    provider_rank: usize,
}

impl RecallControlItemRefV1 {
    pub(crate) fn parse(value: &str) -> Result<Self> {
        if value.len() > ITEM_PREFIX.len() + 20 {
            return Err(invalid("item reference length"));
        }
        let rank = value
            .strip_prefix(ITEM_PREFIX)
            .ok_or_else(|| invalid("item reference version"))?;
        let provider_rank = rank
            .parse::<usize>()
            .map_err(|_| invalid("item reference rank"))?;
        if rank != provider_rank.to_string() || !valid_rank(provider_rank) {
            return Err(invalid("item reference rank"));
        }
        Ok(Self {
            wire: value.to_owned(),
            provider_rank,
        })
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.wire
    }
}

/// Previously confirmed data, structurally checked but never fresh authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct RetainedRecallControlBindingV1 {
    pub(crate) stable_memory_ref: String,
    pub(crate) original_sources: Vec<RecallSourceAttributionV1>,
}

impl RetainedRecallControlBindingV1 {
    fn validated(&self, control: Option<&OperationControl>) -> Result<Self> {
        require_id(&self.stable_memory_ref)?;
        if self.original_sources.is_empty() || self.original_sources.len() > MAX_SOURCES {
            return Err(invalid("source count"));
        }
        let mut sources = self.original_sources.clone();
        let mut seen = BTreeSet::new();
        for source in &sources {
            if let Some(control) = control {
                control
                    .snapshot()
                    .map_err(RecallControlAttributionErrorV1::Control)?;
            }
            let attribution = source
                .to_owned_attribution()
                .map_err(|_| invalid("original source attribution"))?;
            attribution
                .origin_scope
                .recorded_scope()
                .map_err(|_| invalid("missing recorded original scope"))?;
            require_id(&source.source.observation_id)?;
            if !seen.insert(source.source.observation_id.clone()) {
                return Err(invalid("duplicate source observation identity"));
            }
        }
        sources.sort_by(|left, right| left.source.observation_id.cmp(&right.source.observation_id));
        Ok(Self {
            stable_memory_ref: self.stable_memory_ref.clone(),
            original_sources: sources,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct PreparedItemV1 {
    candidate_id: String,
    binding: RetainedRecallControlBindingV1,
    original_sources_json: String,
    control_binding_sha256: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ScopeBindingEnvelopeV1 {
    version: u32,
    delivery_scope: RecallOutcomeScopeV1,
    scope_binding_sha256: String,
}

/// Deterministic metadata prepared before entering the existing trace transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedRecallControlMetadataV1 {
    trace_ref: RecallControlTraceRefV1,
    delivery_scope_json: String,
    metadata_sha256: String,
    rows: BTreeMap<usize, PreparedItemV1>,
}

impl PreparedRecallControlMetadataV1 {
    /// `bindings` must come from final successful host hydration, keyed by the
    /// original candidate identity. This function confers no source authority.
    pub(crate) fn prepare(
        trace: &RecallExplainTraceV1,
        delivery_scope: &OwnedExactScope,
        bindings: &BTreeMap<String, RetainedRecallControlBindingV1>,
    ) -> Result<Self> {
        delivery_scope
            .validate()
            .map_err(|_| invalid("delivery scope"))?;
        OwnedProviderId::new(&trace.provider_id).map_err(|_| invalid("provider identity"))?;
        if trace.registration_revision == 0
            || trace.registration_revision > i64::MAX as u64
            || trace.requested_count != trace.items.len()
            || trace.requested_count as u64 > PROJECT_RECALL_BUDGETS.maximum_candidates
        {
            return Err(invalid("trace identity or candidate bound"));
        }
        let trace_ref = RecallControlTraceRefV1::parse(&format!(
            "{TRACE_PREFIX}{}:{}",
            delivery_scope.exact_scope_sha256(),
            trace.trace_id
        ))?;
        let delivery_scope_json = serde_json::to_string(&ScopeBindingEnvelopeV1 {
            version: 1,
            delivery_scope: scope_wire(delivery_scope),
            scope_binding_sha256: scope_binding_sha256(
                &trace.provider_id,
                trace.registration_revision,
                delivery_scope,
            )?,
        })?;
        require_size(&delivery_scope_json, MAX_SCOPE_BYTES)?;
        let mut rows = BTreeMap::new();
        let mut candidates = BTreeSet::new();
        for (rank, item) in trace.items.iter().enumerate() {
            require_id(&item.candidate_id)?;
            if item.provider_rank != rank
                || !candidates.insert(item.candidate_id.as_str())
                || item.stage != item.host_decision.stage()
            {
                return Err(invalid("final trace rank or decision"));
            }
            if !eligible_stage(item.stage) {
                continue;
            }
            let Some(binding) = bindings.get(&item.candidate_id) else {
                continue;
            };
            let binding = binding.validated(None)?;
            let original_sources_json = serde_json::to_string(&binding.original_sources)?;
            require_size(&original_sources_json, MAX_SOURCES_BYTES)?;
            let control_binding_sha256 = binding_sha256(
                &trace.provider_id,
                trace.registration_revision,
                &trace_ref.exact_scope_sha256,
                rank,
                &item.candidate_id,
                &binding,
            )?;
            rows.insert(
                rank,
                PreparedItemV1 {
                    candidate_id: item.candidate_id.clone(),
                    binding,
                    original_sources_json,
                    control_binding_sha256,
                },
            );
        }
        if bindings
            .keys()
            .any(|candidate| !candidates.contains(candidate.as_str()))
        {
            return Err(invalid("binding outside final trace"));
        }
        // A deterministic struct/tuple and BTreeMap keep hashing independent of
        // caller insertion order. Scope retains every identity, not just its hash.
        let bytes = serde_json::to_vec(&(
            "recall-control-metadata-v1",
            &trace.provider_id,
            trace.registration_revision,
            &delivery_scope_json,
            &rows,
        ))?;
        let metadata_sha256 = tracedecay_domain::canonical_text::sha256_hex(&bytes);
        Ok(Self {
            trace_ref,
            delivery_scope_json,
            metadata_sha256,
            rows,
        })
    }

    pub(crate) fn delivery_scope_json(&self) -> &str {
        &self.delivery_scope_json
    }
    pub(crate) fn metadata_sha256(&self) -> &str {
        &self.metadata_sha256
    }

    /// Use only after the enclosing atomic trace write reports success.
    pub(crate) fn trace_ref(&self) -> &RecallControlTraceRefV1 {
        &self.trace_ref
    }

    pub(crate) fn item_ref(&self, provider_rank: usize) -> Option<RecallControlItemRefV1> {
        self.rows.get(&provider_rank)?;
        Some(RecallControlItemRefV1 {
            wire: format!("{ITEM_PREFIX}{provider_rank}"),
            provider_rank,
        })
    }

    pub(crate) fn item_sql(&self, provider_rank: usize) -> Option<(&str, &str, &str)> {
        self.rows.get(&provider_rank).map(|row| {
            (
                row.binding.stable_memory_ref.as_str(),
                row.original_sources_json.as_str(),
                row.control_binding_sha256.as_str(),
            )
        })
    }

    pub(crate) fn matches_trace(&self, scope_sha256: &str, trace: &RecallExplainTraceV1) -> bool {
        if self.trace_ref.exact_scope_sha256 != scope_sha256
            || self.trace_ref.trace_id != trace.trace_id
        {
            return false;
        }
        let Ok(scope) = decode_scope_envelope(
            &self.delivery_scope_json,
            &trace.provider_id,
            trace.registration_revision,
        ) else {
            return false;
        };
        let bindings = self
            .rows
            .values()
            .map(|row| (row.candidate_id.clone(), row.binding.clone()))
            .collect();
        Self::prepare(trace, &scope, &bindings).is_ok_and(|prepared| prepared == *self)
    }
}

/// Retained producing identity and delivery scope; the caller must authorize them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RetainedRecallControlScopeV1 {
    pub(crate) provider_id: OwnedProviderId,
    pub(crate) registration_revision: u64,
    pub(crate) delivery_scope: OwnedExactScope,
}

/// Exact retained member selected by observation identity, including multi-source items.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RetainedRecallControlSourceV1 {
    pub(crate) scope: RetainedRecallControlScopeV1,
    pub(crate) candidate_id: String,
    pub(crate) provider_rank: usize,
    pub(crate) stable_memory_ref: String,
    pub(crate) original_source: RecallSourceAttributionV1,
}

/// Startup-only additive migration. No data is inferred for historical rows.
pub(crate) fn initialize_schema(connection: &Connection) -> rusqlite::Result<()> {
    for (table, column) in [
        ("recall_explain_traces", "delivery_scope_json"),
        ("recall_explain_traces", "control_metadata_sha256"),
        ("recall_explain_trace_items", "stable_memory_ref"),
        ("recall_explain_trace_items", "original_sources_json"),
        ("recall_explain_trace_items", "control_binding_sha256"),
    ] {
        let present: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info(?1) WHERE name = ?2)",
            params![table, column],
            |row| row.get(0),
        )?;
        if !present {
            // Both identifiers are the fixed literals above, never caller data.
            connection.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column} TEXT;"))?;
        }
    }
    Ok(())
}

impl RecallAdmissionLedgerV1 {
    /// Execute on the caller's existing blocking pool with its original control.
    pub(crate) fn read_retained_control_scope(
        &self,
        trace: &RecallControlTraceRefV1,
        control: &OperationControl,
    ) -> Result<RetainedRecallControlScopeV1> {
        self.with_control_connection(control, |connection| {
            read_retained_control_scope_on_connection(connection, trace, control)
        })
    }

    /// One composite-key JOIN reads a single item; siblings are never fetched.
    pub(crate) fn read_retained_control_source(
        &self,
        trace: &RecallControlTraceRefV1,
        item: &RecallControlItemRefV1,
        observation_id: &str,
        control: &OperationControl,
    ) -> Result<RetainedRecallControlSourceV1> {
        control
            .snapshot()
            .map_err(RecallControlAttributionErrorV1::Control)?;
        require_id(observation_id)?;
        self.with_control_connection(control, |connection| {
            let result = connection
                .query_row(
                    "SELECT substr(CAST(t.exact_scope_sha256 AS BLOB),1,65),
                        substr(CAST(t.trace_id AS BLOB),1,65),
                        substr(CAST(t.provider_id AS BLOB),1,1025), t.registration_revision,
                        substr(CAST(t.delivery_scope_json AS BLOB),1,16385),
                        substr(CAST(t.control_metadata_sha256 AS BLOB),1,65),
                        i.provider_rank, substr(CAST(i.candidate_id AS BLOB),1,1025),
                        substr(CAST(i.stage AS BLOB),1,65),
                        substr(CAST(i.host_decision_json AS BLOB),1,16385),
                        substr(CAST(i.stable_memory_ref AS BLOB),1,1025),
                        substr(CAST(i.original_sources_json AS BLOB),1,1048577),
                        substr(CAST(i.control_binding_sha256 AS BLOB),1,65)
                 FROM recall_explain_traces t
                 JOIN recall_explain_trace_items i
                   ON i.exact_scope_sha256=t.exact_scope_sha256 AND i.trace_id=t.trace_id
                 WHERE t.exact_scope_sha256=?1 AND t.trace_id=?2 AND i.provider_rank=?3 LIMIT 1",
                    params![
                        trace.exact_scope_sha256,
                        trace.trace_id,
                        item.provider_rank as i64
                    ],
                    |row| Ok(decode_source(row, trace, item, observation_id, control)),
                )
                .optional()?;
            result.ok_or(RecallControlAttributionErrorV1::NotFound)?
        })
    }

    fn with_control_connection<T>(
        &self,
        control: &OperationControl,
        read: impl FnOnce(&Connection) -> Result<T>,
    ) -> Result<T> {
        let connection = loop {
            let remaining = control
                .snapshot()
                .map_err(RecallControlAttributionErrorV1::Control)?;
            match self.connection.try_lock() {
                Ok(connection) => break connection,
                Err(TryLockError::Poisoned(_)) => return Err(invalid("ledger mutex poisoned")),
                Err(TryLockError::WouldBlock) => {
                    std::thread::sleep(Duration::from_millis(remaining.remaining_millis.min(1)));
                }
            }
        };
        let remaining = control
            .snapshot()
            .map_err(RecallControlAttributionErrorV1::Control)?;
        let previous: i64 =
            connection.pragma_query_value(None, "busy_timeout", |row| row.get(0))?;
        let previous = u64::try_from(previous).map_err(|_| invalid("ledger busy timeout"))?;
        // A short busy allowance also bounds cancellation latency inside SQLite.
        connection.busy_timeout(Duration::from_millis(remaining.remaining_millis.min(10)))?;
        let restore = RestoreBusyTimeout(&connection, Duration::from_millis(previous));
        let result = read(&connection);
        drop(restore);
        control
            .snapshot()
            .map_err(RecallControlAttributionErrorV1::Control)?;
        result
    }
}

/// Shares the existing scope query with the read-only test reader's transaction.
pub(super) fn read_retained_control_scope_on_connection(
    connection: &Connection,
    trace: &RecallControlTraceRefV1,
    control: &OperationControl,
) -> Result<RetainedRecallControlScopeV1> {
    control
        .snapshot()
        .map_err(RecallControlAttributionErrorV1::Control)?;
    let result = connection
        .query_row(
            "SELECT substr(CAST(exact_scope_sha256 AS BLOB),1,65),
                substr(CAST(trace_id AS BLOB),1,65),
                substr(CAST(provider_id AS BLOB),1,1025), registration_revision,
                substr(CAST(delivery_scope_json AS BLOB),1,16385),
                substr(CAST(control_metadata_sha256 AS BLOB),1,65)
         FROM recall_explain_traces
         WHERE exact_scope_sha256=?1 AND trace_id=?2 LIMIT 1",
            params![trace.exact_scope_sha256, trace.trace_id],
            |row| Ok(decode_scope(row, trace)),
        )
        .optional();
    control
        .snapshot()
        .map_err(RecallControlAttributionErrorV1::Control)?;
    result?.ok_or(RecallControlAttributionErrorV1::NotFound)?
}

struct RestoreBusyTimeout<'a>(&'a Connection, Duration);

impl Drop for RestoreBusyTimeout<'_> {
    fn drop(&mut self) {
        let _ = self.0.busy_timeout(self.1);
    }
}

fn decode_scope(
    row: &Row<'_>,
    trace: &RecallControlTraceRefV1,
) -> Result<RetainedRecallControlScopeV1> {
    let scope_sha256 = required_text(row, 0, 64)?;
    let trace_id = required_text(row, 1, 64)?;
    if scope_sha256 != trace.exact_scope_sha256 || trace_id != trace.trace_id {
        return Err(invalid("retained composite identity"));
    }
    let provider = required_text(row, 2, MAX_ID_BYTES)?;
    require_id(&provider)?;
    let provider_id =
        OwnedProviderId::new(provider).map_err(|_| invalid("retained provider identity"))?;
    let revision: i64 = row.get(3)?;
    let registration_revision = u64::try_from(revision)
        .ok()
        .filter(|revision| *revision > 0)
        .ok_or_else(|| invalid("retained registration revision"))?;
    let scope_json = optional_text(row, 4, MAX_SCOPE_BYTES)?
        .ok_or(RecallControlAttributionErrorV1::MissingAuthority)?;
    let metadata_sha256 =
        optional_text(row, 5, 64)?.ok_or(RecallControlAttributionErrorV1::MissingAuthority)?;
    require_sha256(&metadata_sha256)?;
    let delivery_scope =
        decode_scope_envelope(&scope_json, provider_id.as_str(), registration_revision)?;
    if delivery_scope.exact_scope_sha256() != scope_sha256 {
        return Err(invalid("retained full scope digest"));
    }
    Ok(RetainedRecallControlScopeV1 {
        provider_id,
        registration_revision,
        delivery_scope,
    })
}

fn decode_source(
    row: &Row<'_>,
    trace: &RecallControlTraceRefV1,
    item: &RecallControlItemRefV1,
    observation_id: &str,
    control: &OperationControl,
) -> Result<RetainedRecallControlSourceV1> {
    let scope = decode_scope(row, trace)?;
    let rank: i64 = row.get(6)?;
    if usize::try_from(rank).ok() != Some(item.provider_rank) || !valid_rank(item.provider_rank) {
        return Err(invalid("retained provider rank"));
    }
    let candidate_id = required_text(row, 7, MAX_ID_BYTES)?;
    require_id(&candidate_id)?;
    let stage = required_text(row, 8, 64)?;
    let decision: RecallExplainHostDecisionV1 =
        serde_json::from_str(&required_text(row, 9, MAX_DECISION_BYTES)?)?;
    if !eligible_stage(decision.stage()) || decision.stage().label() != stage {
        return Err(invalid("retained item was not selected or injected"));
    }
    let stable_memory_ref = optional_text(row, 10, MAX_ID_BYTES)?
        .ok_or(RecallControlAttributionErrorV1::MissingAuthority)?;
    let sources_json = optional_text(row, 11, MAX_SOURCES_BYTES)?
        .ok_or(RecallControlAttributionErrorV1::MissingAuthority)?;
    let retained_binding_sha256 =
        optional_text(row, 12, 64)?.ok_or(RecallControlAttributionErrorV1::MissingAuthority)?;
    require_sha256(&retained_binding_sha256)?;
    let binding = RetainedRecallControlBindingV1 {
        stable_memory_ref,
        original_sources: serde_json::from_str::<BoundedSourcesV1>(&sources_json)?.0,
    }
    .validated(Some(control))?;
    if retained_binding_sha256
        != binding_sha256(
            scope.provider_id.as_str(),
            scope.registration_revision,
            &scope.delivery_scope.exact_scope_sha256(),
            item.provider_rank,
            &candidate_id,
            &binding,
        )?
    {
        return Err(invalid("retained source binding digest"));
    }
    control
        .snapshot()
        .map_err(RecallControlAttributionErrorV1::Control)?;
    let original_source = binding
        .original_sources
        .into_iter()
        .find(|source| source.source.observation_id == observation_id)
        .ok_or_else(|| invalid("observation identity outside retained item"))?;
    Ok(RetainedRecallControlSourceV1 {
        scope,
        candidate_id,
        provider_rank: item.provider_rank,
        stable_memory_ref: binding.stable_memory_ref,
        original_source,
    })
}

pub(super) fn optional_text(
    row: &Row<'_>,
    column: usize,
    maximum: usize,
) -> Result<Option<String>> {
    let bytes: Option<Vec<u8>> = row.get(column)?;
    bytes
        .map(|bytes| {
            if bytes.len() > maximum {
                return Err(invalid("retained field byte bound"));
            }
            String::from_utf8(bytes).map_err(|_| invalid("retained field UTF-8"))
        })
        .transpose()
}

pub(super) fn required_text(row: &Row<'_>, column: usize, maximum: usize) -> Result<String> {
    optional_text(row, column, maximum)?.ok_or_else(|| invalid("missing retained field"))
}

fn binding_sha256(
    provider_id: &str,
    registration_revision: u64,
    exact_scope_sha256: &str,
    provider_rank: usize,
    candidate_id: &str,
    binding: &RetainedRecallControlBindingV1,
) -> Result<String> {
    let bytes = serde_json::to_vec(&(
        "recall-control-binding-v1",
        provider_id,
        registration_revision,
        exact_scope_sha256,
        provider_rank,
        candidate_id,
        binding,
    ))?;
    Ok(tracedecay_domain::canonical_text::sha256_hex(&bytes))
}

fn scope_binding_sha256(
    provider_id: &str,
    registration_revision: u64,
    scope: &OwnedExactScope,
) -> Result<String> {
    let bytes = serde_json::to_vec(&(
        "recall-control-scope-binding-v1",
        provider_id,
        registration_revision,
        scope_wire(scope),
    ))?;
    Ok(tracedecay_domain::canonical_text::sha256_hex(&bytes))
}

fn decode_scope_envelope(
    scope_json: &str,
    provider_id: &str,
    registration_revision: u64,
) -> Result<OwnedExactScope> {
    let envelope: ScopeBindingEnvelopeV1 = serde_json::from_str(scope_json)?;
    if envelope.version != 1 {
        return Err(invalid("retained scope envelope version"));
    }
    require_sha256(&envelope.scope_binding_sha256)?;
    let scope = scope_owned(envelope.delivery_scope)?;
    if envelope.scope_binding_sha256
        != scope_binding_sha256(provider_id, registration_revision, &scope)?
    {
        return Err(invalid("retained scope binding digest"));
    }
    Ok(scope)
}

struct BoundedSourcesV1(Vec<RecallSourceAttributionV1>);

impl<'de> Deserialize<'de> for BoundedSourcesV1 {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct SourcesVisitor;
        impl<'de> serde::de::Visitor<'de> for SourcesVisitor {
            type Value = BoundedSourcesV1;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an array of 1 to 64 canonical source attributions")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut sources = Vec::new();
                while let Some(source) = sequence.next_element::<RecallSourceAttributionV1>()? {
                    if sources.len() == MAX_SOURCES {
                        return Err(serde::de::Error::custom("source count exceeds 64"));
                    }
                    sources.push(source);
                }
                if sources.is_empty() {
                    return Err(serde::de::Error::custom("source array is empty"));
                }
                Ok(BoundedSourcesV1(sources))
            }
        }
        deserializer.deserialize_seq(SourcesVisitor)
    }
}

fn scope_wire(scope: &OwnedExactScope) -> RecallOutcomeScopeV1 {
    RecallOutcomeScopeV1 {
        profile_id: scope.profile_id.clone(),
        project_id: scope.project_id.clone(),
        repository_identity: scope.repository_identity.clone(),
        worktree_identity: scope.worktree_identity.clone(),
        branch_identity: scope.branch_identity.clone(),
        agent_session_id: scope.agent_session_id.clone(),
        resolved_scope_digest: scope.resolved_scope_digest.clone(),
    }
}

fn scope_owned(scope: RecallOutcomeScopeV1) -> Result<OwnedExactScope> {
    OwnedExactScope::new(
        scope.profile_id,
        scope.project_id,
        scope.repository_identity,
        scope.worktree_identity,
        scope.branch_identity,
        scope.agent_session_id,
        scope.resolved_scope_digest,
    )
    .map_err(|_| invalid("retained delivery scope"))
}

fn invalid(field: &'static str) -> RecallControlAttributionErrorV1 {
    RecallControlAttributionErrorV1::Invalid(field)
}
fn valid_rank(rank: usize) -> bool {
    (rank as u64) < PROJECT_RECALL_BUDGETS.maximum_candidates
}
fn eligible_stage(stage: RecallExplainStageV1) -> bool {
    matches!(
        stage,
        RecallExplainStageV1::Selected | RecallExplainStageV1::Injected
    )
}
fn require_sha256(value: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid("canonical SHA-256"));
    }
    Ok(())
}
fn require_id(value: &str) -> Result<()> {
    if value.is_empty() || value.trim() != value || value.chars().any(char::is_control) {
        return Err(invalid("bounded identity"));
    }
    require_size(value, MAX_ID_BYTES)
}
fn require_size(value: &str, maximum: usize) -> Result<()> {
    if value.len() > maximum {
        return Err(invalid("metadata byte bound"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::{RecallAdmissionLedgerError, RecallAdmissionLedgerWriteV1};
    use super::*;
    use std::sync::Mutex;
    use std::time::Instant;
    use tracedecay_memory_provider_registry::{
        CancellationToken, RecallExplainItemV1, RecallExplainProviderExplanationV1,
    };

    fn control(millis: u64) -> OperationControl {
        OperationControl::new(
            tracedecay_contracts::now_micros()
                .0
                .saturating_add((millis * 1_000) as i64),
            millis,
            CancellationToken::new(),
        )
    }

    fn scope() -> OwnedExactScope {
        OwnedExactScope::new(
            "profile.test",
            "project.test",
            "repository.test",
            "worktree.test",
            "branch.test",
            "session.destination",
            format!("sha256:{}", "b".repeat(64)),
        )
        .unwrap()
    }

    fn source(observation_id: &str, sequence: u64) -> RecallSourceAttributionV1 {
        let mut origin = scope();
        origin.agent_session_id = "session.original".to_owned();
        serde_json::from_value(serde_json::json!({
            "source": {
                "canonical_provider_id": "codex", "canonical_session_id": "session.original",
                "source_key": format!("source.{observation_id}"), "stable_record_id": null,
                "observation_id": observation_id, "source_revision": null,
                "content_sha256": "c".repeat(64)
            },
            "origin_scope": {"state": "recorded", "exact_scope_identity": scope_wire(&origin),
                "authority_ref": "host-original:test"},
            "source_sequence": sequence, "occurred_at": null,
            "ingested_at": "2025-05-01T00:00:00.000000000Z",
            "validity": {"valid_from":null,"valid_until":null,"superseded_at":null,
                "superseded_by":null,"revoked_at":null}
        }))
        .unwrap()
    }

    fn trace() -> RecallExplainTraceV1 {
        RecallExplainTraceV1 {
            trace_id: "a".repeat(64),
            request_id: "request.test".to_owned(),
            provider_id: "tracedecay.native".to_owned(),
            registration_revision: 7,
            requested_count: 3,
            degraded: false,
            token_summary: None,
            items: (0..3)
                .map(|rank| {
                    let host_decision = if rank == 1 {
                        RecallExplainHostDecisionV1::HostWithheld {
                            reason_code: "unresolvable".to_owned(),
                            detail: None,
                        }
                    } else {
                        RecallExplainHostDecisionV1::Selected
                    };
                    RecallExplainItemV1 {
                        candidate_id: format!("candidate.{rank}"),
                        provider_rank: rank,
                        stage: host_decision.stage(),
                        host_reason_code: "selected".to_owned(),
                        host_reason_detail: None,
                        host_decision,
                        provider_explanation: RecallExplainProviderExplanationV1::NotProvided,
                        section: None,
                        tokens: None,
                    }
                })
                .collect(),
        }
    }

    fn bindings() -> BTreeMap<String, RetainedRecallControlBindingV1> {
        BTreeMap::from([
            (
                "candidate.0".to_owned(),
                RetainedRecallControlBindingV1 {
                    stable_memory_ref: "memory.first".to_owned(),
                    original_sources: vec![source("observation.1", 1)],
                },
            ),
            (
                "candidate.2".to_owned(),
                RetainedRecallControlBindingV1 {
                    stable_memory_ref: "memory.multisource".to_owned(),
                    original_sources: vec![source("observation.3", 3), source("observation.2", 2)],
                },
            ),
        ])
    }

    fn prepared() -> PreparedRecallControlMetadataV1 {
        PreparedRecallControlMetadataV1::prepare(&trace(), &scope(), &bindings()).unwrap()
    }

    fn retained() -> (
        tempfile::TempDir,
        RecallAdmissionLedgerV1,
        PreparedRecallControlMetadataV1,
    ) {
        let temporary = tempfile::tempdir().unwrap();
        let ledger =
            RecallAdmissionLedgerV1::open(temporary.path().join("ledger.sqlite3")).unwrap();
        let metadata = prepared();
        assert_eq!(
            ledger
                .retain_explain_trace_with_control(
                    &scope().exact_scope_sha256(),
                    &trace(),
                    Some(&metadata),
                )
                .unwrap(),
            RecallAdmissionLedgerWriteV1::Recorded
        );
        (temporary, ledger, metadata)
    }

    #[test]
    fn control_locators_and_final_rank_metadata_are_strict_and_deterministic() {
        let metadata = prepared();
        assert_eq!(
            RecallControlTraceRefV1::parse(metadata.trace_ref().as_str()).unwrap(),
            *metadata.trace_ref()
        );
        assert_eq!(metadata.item_ref(2).unwrap().as_str(), "recall-item-v1:2");
        assert!(metadata.item_ref(1).is_none());
        for invalid in [
            "recall-item-v1:+1",
            "recall-item-v1:01",
            "recall-item-v1:-1",
            "recall-item-v2:1",
            "recall-item-v1:8",
        ] {
            assert!(RecallControlItemRefV1::parse(invalid).is_err(), "{invalid}");
        }
        for invalid in [
            metadata.trace_ref().as_str().to_uppercase(),
            format!("{}:extra", metadata.trace_ref().as_str()),
            "a".repeat(64),
        ] {
            assert!(RecallControlTraceRefV1::parse(&invalid).is_err());
        }
        let mut same_bindings = bindings();
        same_bindings
            .get_mut("candidate.2")
            .unwrap()
            .original_sources
            .reverse();
        assert_eq!(
            PreparedRecallControlMetadataV1::prepare(&trace(), &scope(), &same_bindings).unwrap(),
            metadata
        );
        same_bindings
            .get_mut("candidate.2")
            .unwrap()
            .stable_memory_ref = "memory.changed".to_owned();
        assert_ne!(
            PreparedRecallControlMetadataV1::prepare(&trace(), &scope(), &same_bindings)
                .unwrap()
                .metadata_sha256(),
            metadata.metadata_sha256()
        );
        let mut malformed = trace();
        malformed.items[2].provider_rank = 1;
        assert!(
            PreparedRecallControlMetadataV1::prepare(&malformed, &scope(), &bindings()).is_err()
        );
        assert!(!metadata.matches_trace(&"f".repeat(64), &trace()));
    }

    #[test]
    fn control_reads_reopen_exact_multisource_member_without_reading_siblings() {
        let (temporary, ledger, metadata) = retained();
        assert_eq!(
            ledger
                .retain_explain_trace_with_control(
                    &scope().exact_scope_sha256(),
                    &trace(),
                    Some(&metadata),
                )
                .unwrap(),
            RecallAdmissionLedgerWriteV1::AlreadyRecorded
        );
        drop(ledger);
        let ledger =
            RecallAdmissionLedgerV1::open(temporary.path().join("ledger.sqlite3")).unwrap();
        let item = metadata.item_ref(2).unwrap();
        let read = ledger
            .read_retained_control_source(
                metadata.trace_ref(),
                &item,
                "observation.3",
                &control(1_000),
            )
            .unwrap();
        assert_eq!(read.scope.provider_id.as_str(), "tracedecay.native");
        assert_eq!(read.scope.registration_revision, 7);
        assert_eq!(read.scope.delivery_scope, scope());
        assert_eq!(read.provider_rank, 2);
        assert_eq!(read.candidate_id, "candidate.2");
        assert_eq!(read.stable_memory_ref, "memory.multisource");
        assert_eq!(read.original_source, source("observation.3", 3));
        assert_eq!(
            ledger
                .read_retained_control_scope(metadata.trace_ref(), &control(1_000))
                .unwrap(),
            read.scope
        );
        assert!(
            ledger
                .read_retained_control_source(
                    metadata.trace_ref(),
                    &item,
                    "observation.1",
                    &control(1_000)
                )
                .is_err()
        );
        ledger.connection.lock().unwrap().execute(
            "UPDATE recall_explain_trace_items SET original_sources_json='broken' WHERE provider_rank=0", [],
        ).unwrap();
        assert!(
            ledger
                .read_retained_control_source(
                    metadata.trace_ref(),
                    &item,
                    "observation.3",
                    &control(1_000)
                )
                .is_ok()
        );
        let missing = RecallControlTraceRefV1::parse(&format!(
            "{TRACE_PREFIX}{}:{}",
            scope().exact_scope_sha256(),
            "d".repeat(64)
        ))
        .unwrap();
        assert!(matches!(
            ledger.read_retained_control_scope(&missing, &control(1_000)),
            Err(RecallControlAttributionErrorV1::NotFound)
        ));
    }

    #[test]
    fn control_binding_hash_refuses_changed_stable_ref_source_and_producer() {
        for changed in [
            "stable",
            "source",
            "provider",
            "revision",
            "scope",
            "rank",
            "state",
            "missing_hash",
        ] {
            let (_temporary, ledger, metadata) = retained();
            {
                let connection = ledger.connection.lock().unwrap();
                match changed {
                    "stable" => {
                        connection.execute("UPDATE recall_explain_trace_items SET stable_memory_ref='memory.changed' WHERE provider_rank=2", []).unwrap();
                    }
                    "source" => {
                        let mut sources =
                            bindings().remove("candidate.2").unwrap().original_sources;
                        sources[0].source.content_sha256 = "e".repeat(64);
                        connection.execute("UPDATE recall_explain_trace_items SET original_sources_json=?1 WHERE provider_rank=2", [serde_json::to_string(&sources).unwrap()]).unwrap();
                    }
                    "provider" => {
                        connection
                            .execute(
                                "UPDATE recall_explain_traces SET provider_id='another.provider'",
                                [],
                            )
                            .unwrap();
                    }
                    "revision" => {
                        connection
                            .execute(
                                "UPDATE recall_explain_traces SET registration_revision=8",
                                [],
                            )
                            .unwrap();
                    }
                    "scope" => {
                        let mut changed = scope();
                        changed.agent_session_id = "session.changed".to_owned();
                        connection
                            .execute(
                                "UPDATE recall_explain_traces SET delivery_scope_json=?1",
                                [serde_json::to_string(&scope_wire(&changed)).unwrap()],
                            )
                            .unwrap();
                    }
                    "rank" => {
                        connection.execute("UPDATE recall_explain_trace_items SET candidate_id='candidate.changed' WHERE provider_rank=2", []).unwrap();
                    }
                    "state" => {
                        connection.execute("UPDATE recall_explain_trace_items SET stage='denied' WHERE provider_rank=2", []).unwrap();
                    }
                    "missing_hash" => {
                        connection.execute("UPDATE recall_explain_trace_items SET control_binding_sha256=NULL WHERE provider_rank=2", []).unwrap();
                    }
                    _ => unreachable!(),
                }
            }
            let result = ledger.read_retained_control_source(
                metadata.trace_ref(),
                &metadata.item_ref(2).unwrap(),
                "observation.3",
                &control(1_000),
            );
            assert!(result.is_err(), "{changed}");
            if changed == "missing_hash" {
                assert!(matches!(
                    result,
                    Err(RecallControlAttributionErrorV1::MissingAuthority)
                ));
            }
        }
    }

    #[test]
    fn control_metadata_conflicts_are_atomic_and_legacy_nulls_do_not_gain_authority() {
        let (_temporary, ledger, metadata) = retained();
        let mut changed = bindings();
        changed.get_mut("candidate.2").unwrap().stable_memory_ref = "memory.changed".to_owned();
        let changed =
            PreparedRecallControlMetadataV1::prepare(&trace(), &scope(), &changed).unwrap();
        assert!(matches!(
            ledger.retain_explain_trace_with_control(
                &scope().exact_scope_sha256(),
                &trace(),
                Some(&changed)
            ),
            Err(RecallAdmissionLedgerError::ConflictingTrace { .. })
        ));
        assert_eq!(
            ledger
                .read_retained_control_source(
                    metadata.trace_ref(),
                    &metadata.item_ref(2).unwrap(),
                    "observation.3",
                    &control(1_000)
                )
                .unwrap()
                .stable_memory_ref,
            "memory.multisource"
        );
        let mut legacy = trace();
        legacy.trace_id = "d".repeat(64);
        ledger
            .retain_explain_trace(&scope().exact_scope_sha256(), &legacy)
            .unwrap();
        let metadata =
            PreparedRecallControlMetadataV1::prepare(&legacy, &scope(), &bindings()).unwrap();
        assert!(matches!(
            ledger.read_retained_control_scope(metadata.trace_ref(), &control(1_000)),
            Err(RecallControlAttributionErrorV1::MissingAuthority)
        ));
        assert!(matches!(
            ledger.retain_explain_trace_with_control(
                &scope().exact_scope_sha256(),
                &legacy,
                Some(&metadata)
            ),
            Err(RecallAdmissionLedgerError::ConflictingTrace { .. })
        ));
        initialize_schema(&ledger.connection.lock().unwrap()).unwrap();
        assert!(matches!(
            ledger.read_retained_control_source(
                metadata.trace_ref(),
                &metadata.item_ref(2).unwrap(),
                "observation.3",
                &control(1_000)
            ),
            Err(RecallControlAttributionErrorV1::MissingAuthority)
        ));
    }

    #[test]
    fn control_scope_read_checks_producer_revision_scope_and_envelope_digest() {
        for changed in ["provider", "revision", "scope", "hash", "version"] {
            let (_temporary, ledger, metadata) = retained();
            assert_eq!(
                ledger
                    .read_retained_control_scope(metadata.trace_ref(), &control(1_000))
                    .unwrap()
                    .delivery_scope,
                scope()
            );
            {
                let connection = ledger.connection.lock().unwrap();
                match changed {
                    "provider" => {
                        connection
                            .execute(
                                "UPDATE recall_explain_traces SET provider_id='other.provider'",
                                [],
                            )
                            .unwrap();
                    }
                    "revision" => {
                        connection
                            .execute(
                                "UPDATE recall_explain_traces SET registration_revision=8",
                                [],
                            )
                            .unwrap();
                    }
                    _ => {
                        let mut envelope: serde_json::Value =
                            serde_json::from_str(metadata.delivery_scope_json()).unwrap();
                        match changed {
                            "scope" => {
                                envelope["delivery_scope"]["agent_session_id"] =
                                    "session.changed".into();
                            }
                            "hash" => {
                                envelope["scope_binding_sha256"] = "f".repeat(64).into();
                            }
                            "version" => {
                                envelope["version"] = 2.into();
                            }
                            _ => unreachable!(),
                        }
                        connection
                            .execute(
                                "UPDATE recall_explain_traces SET delivery_scope_json=?1",
                                [serde_json::to_string(&envelope).unwrap()],
                            )
                            .unwrap();
                    }
                }
            }
            assert!(
                ledger
                    .read_retained_control_scope(metadata.trace_ref(), &control(1_000))
                    .is_err(),
                "{changed}"
            );
        }
    }

    #[test]
    fn control_reads_refuse_source_count_and_field_byte_overflow() {
        let (_temporary, ledger, metadata) = retained();
        let sources: Vec<_> = (1..=65)
            .map(|sequence| source(&format!("observation.{sequence}"), sequence))
            .collect();
        ledger.connection.lock().unwrap().execute("UPDATE recall_explain_trace_items SET original_sources_json=?1 WHERE provider_rank=2", [serde_json::to_string(&sources).unwrap()]).unwrap();
        assert!(
            ledger
                .read_retained_control_source(
                    metadata.trace_ref(),
                    &metadata.item_ref(2).unwrap(),
                    "observation.3",
                    &control(1_000)
                )
                .is_err()
        );
        ledger.connection.lock().unwrap().execute("UPDATE recall_explain_trace_items SET original_sources_json=?1 WHERE provider_rank=2", ["x".repeat(MAX_SOURCES_BYTES + 1)]).unwrap();
        assert!(matches!(
            ledger.read_retained_control_source(
                metadata.trace_ref(),
                &metadata.item_ref(2).unwrap(),
                "observation.3",
                &control(1_000)
            ),
            Err(RecallControlAttributionErrorV1::Invalid(
                "retained field byte bound"
            ))
        ));
    }

    #[test]
    fn control_connection_wait_keeps_original_deadline_and_cancellation() {
        let ledger = RecallAdmissionLedgerV1 {
            path: "unused".into(),
            connection: Mutex::new(Connection::open_in_memory().unwrap()),
        };
        let held = ledger.connection.lock().unwrap();
        let metadata = prepared();
        let started = Instant::now();
        assert!(matches!(
            ledger.read_retained_control_scope(metadata.trace_ref(), &control(10)),
            Err(RecallControlAttributionErrorV1::Control(
                TerminalCode::DeadlineExceeded
            ))
        ));
        assert!(started.elapsed() < Duration::from_secs(1));
        let cancellable = control(5_000);
        let token = cancellable.cancellation();
        std::thread::scope(|threads| {
            threads.spawn(|| {
                std::thread::sleep(Duration::from_millis(5));
                token.cancel();
            });
            let started = Instant::now();
            assert!(matches!(
                ledger.read_retained_control_scope(metadata.trace_ref(), &cancellable),
                Err(RecallControlAttributionErrorV1::Control(
                    TerminalCode::Cancelled
                ))
            ));
            assert!(started.elapsed() < Duration::from_secs(1));
        });
        drop(held);
    }
}
