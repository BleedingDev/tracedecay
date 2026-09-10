//! Canonical CLI/MCP wire contracts for the established primitive tools.
//!
//! These types own the JSON decoded by the daemon handlers and the JSON
//! schemas projected into both public SDKs. Presentation-only transport keys
//! such as `format` and registered-project selectors are removed before these
//! request bodies are decoded.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracedecay_domain::{ComplexityAnalysisV1, FactAssertionId, FactEventId, FactId, UtcMicros};

use crate::error::ApplicationContractError;
use crate::memory::{
    CognitiveRecallExclusions, CognitiveRecallTemporalMode, CognitiveRecallTemporalQuery,
    FactCommitOwnerV1, FactIdentitySourceResultV1, FactSearchGraphCoverageV1, FactSearchHitV1,
};

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PrimitiveSemanticModeV1 {
    FallbackAllowed,
    StrictSemantic,
}

impl PrimitiveSemanticModeV1 {
    #[hotpath::skip]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FallbackAllowed => "fallback_allowed",
            Self::StrictSemantic => "strict_semantic",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextModeV1 {
    Explore,
    Plan,
}

impl ContextModeV1 {
    #[hotpath::skip]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Explore => "explore",
            Self::Plan => "plan",
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContextSurfaceRequestV1 {
    pub task: String,
    pub max_nodes: Option<u32>,
    pub include_code: Option<bool>,
    pub max_code_blocks: Option<u32>,
    pub mode: Option<ContextModeV1>,
    pub include_memory: Option<bool>,
    pub memory_limit: Option<u32>,
    pub memory_min_trust: Option<f64>,
    /// Exact caller-supplied memory temporal policy, validated at host admission.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temporal_query: Option<CognitiveRecallTemporalQuery>,
    /// Memory exclusions preserved for the host's advisory admission; these do
    /// not invent a mapping from provider references to canonical fact identities.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclusions: Option<CognitiveRecallExclusions>,
    pub semantic_mode: Option<PrimitiveSemanticModeV1>,
    /// Exact identifiers or technical terms ranked through the lexical lane as
    /// additional routes fused with the task text. Bounded and validated by
    /// the retrieval kernel; a violation is a typed request rejection.
    pub lexical_anchors: Option<Vec<String>>,
    /// Add a symbol-name lexical route for the identifier-shaped words of the
    /// task text.
    pub prefer_symbol: Option<bool>,
}

impl ContextSurfaceRequestV1 {
    /// Validates memory policy against one actual host admission clock without
    /// changing the supplied query, exclusions, or disabled-memory preference.
    pub fn validate_memory_policy_at(
        &self,
        now: UtcMicros,
    ) -> Result<(), ApplicationContractError> {
        if let Some(temporal) = &self.temporal_query {
            temporal.validate_at(now)?;
        }
        if let Some(exclusions) = &self.exclusions {
            exclusions.validate()?;
        }
        Ok(())
    }
}

/// Existing maximum canonical fact hits returned by a context memory read.
pub const MAX_CONTEXT_MEMORY_CONTRIBUTION_FACTS: usize = 10;

/// Explicit withholding by the current-only canonical fact search lane.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextMemoryTemporalCoverageV1 {
    /// No current facts were searched or substituted for the requested history.
    WithheldCurrentOnly {
        /// Exact non-current mode that the canonical fact search cannot serve.
        requested_mode: CognitiveRecallTemporalMode,
    },
}

/// Identity metadata for a canonical fact that actually contributed to context.
/// The revision identity is `(owner, fact_id, last_event_id)`; assertion identity
/// remains separate. No fact payload, tags, telemetry, or metadata is copied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextMemoryFactIdentityV1 {
    pub owner: FactCommitOwnerV1,
    pub fact_id: FactId,
    pub last_event_id: FactEventId,
    pub active_assertion_id: FactAssertionId,
    /// Exact typed canonical source evidence. An anchor or stable key does not
    /// establish an original observation revision or authorize history access.
    pub source: FactIdentitySourceResultV1,
}

impl From<&FactSearchHitV1> for ContextMemoryFactIdentityV1 {
    fn from(hit: &FactSearchHitV1) -> Self {
        Self {
            owner: hit.fact.owner.clone(),
            fact_id: hit.fact.fact_id.clone(),
            last_event_id: hit.fact.last_event_id.clone(),
            active_assertion_id: hit.fact.active_assertion_id.clone(),
            source: hit.fact.source.clone(),
        }
    }
}

/// Internal typed contribution carried past context rendering into host recall.
/// This has no serialized form. Policy is validated at the same captured clock
/// used for request admission and remains available when canonical memory is off.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextMemoryContributionV1 {
    facts: Vec<ContextMemoryFactIdentityV1>,
    graph_coverage: Option<FactSearchGraphCoverageV1>,
    temporal_coverage: Option<ContextMemoryTemporalCoverageV1>,
    temporal_query: Option<CognitiveRecallTemporalQuery>,
    exclusions: Option<CognitiveRecallExclusions>,
}

impl ContextMemoryContributionV1 {
    /// Preserves metadata from the actual canonical hits without parsing rendered
    /// output or inferring source identities. `admitted_at` is the request's one
    /// captured host clock, reused after retrieval completes.
    pub fn from_matches(
        request: &ContextSurfaceRequestV1,
        hits: &[FactSearchHitV1],
        graph_coverage: Option<FactSearchGraphCoverageV1>,
        temporal_coverage: Option<ContextMemoryTemporalCoverageV1>,
        admitted_at: UtcMicros,
    ) -> Result<Self, ApplicationContractError> {
        request.validate_memory_policy_at(admitted_at)?;
        if hits.len() > MAX_CONTEXT_MEMORY_CONTRIBUTION_FACTS {
            return Err(ApplicationContractError::InvalidRange {
                field: "context memory contribution facts",
            });
        }
        let expected_coverage = request.temporal_query.as_ref().and_then(|query| {
            (request.include_memory.unwrap_or(true)
                && query.mode() != CognitiveRecallTemporalMode::Current)
                .then_some(ContextMemoryTemporalCoverageV1::WithheldCurrentOnly {
                    requested_mode: query.mode(),
                })
        });
        if temporal_coverage != expected_coverage
            || (temporal_coverage.is_some() && (!hits.is_empty() || graph_coverage.is_some()))
            || (!request.include_memory.unwrap_or(true)
                && (!hits.is_empty() || graph_coverage.is_some()))
        {
            return Err(ApplicationContractError::Inconsistent {
                field: "context memory temporal coverage",
            });
        }
        Ok(Self {
            facts: hits.iter().map(ContextMemoryFactIdentityV1::from).collect(),
            graph_coverage,
            temporal_coverage,
            temporal_query: request.temporal_query.clone(),
            exclusions: request.exclusions.clone(),
        })
    }

    /// Canonical fact identities that actually contributed to rendered context.
    pub fn facts(&self) -> &[ContextMemoryFactIdentityV1] {
        &self.facts
    }

    /// Coverage from the canonical fact read, absent when it was not attempted.
    pub fn graph_coverage(&self) -> Option<&FactSearchGraphCoverageV1> {
        self.graph_coverage.as_ref()
    }

    /// Explicit temporal withholding by the current-only canonical fact lane.
    pub fn temporal_coverage(&self) -> Option<&ContextMemoryTemporalCoverageV1> {
        self.temporal_coverage.as_ref()
    }

    /// Exact validated caller policy, retained even when canonical memory is disabled.
    pub fn temporal_query(&self) -> Option<&CognitiveRecallTemporalQuery> {
        self.temporal_query.as_ref()
    }

    /// Exact validated exclusions for the host advisory consumer.
    pub fn exclusions(&self) -> Option<&CognitiveRecallExclusions> {
        self.exclusions.as_ref()
    }
}

/// Whether the served code generation is known current at serve time.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PrimitiveFreshnessStateV1 {
    Fresh,
    PossiblyStale,
}

impl PrimitiveFreshnessStateV1 {
    #[hotpath::skip]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::PossiblyStale => "possibly_stale",
        }
    }
}

/// The indexing state behind a `possibly_stale` verdict: the served
/// generation, the scheduler's latest sealed generation, its staleness-ladder
/// state, and the lanes that answered from an older generation. `summary` is
/// the one-line rendering agents read.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PrimitiveIndexingStateV1 {
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub served_generation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_generation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staleness_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rebuild_in_flight: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stale_lanes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Freshness verdict carried by every search and context response. `indexing`
/// is present exactly when the state is `possibly_stale`.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PrimitiveSearchFreshnessV1 {
    pub state: PrimitiveFreshnessStateV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub indexing: Option<PrimitiveIndexingStateV1>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NodeDepthSurfaceRequestV1 {
    pub node_id: String,
    pub max_depth: Option<u32>,
}

pub type ImpactSurfaceRequestV1 = NodeDepthSurfaceRequestV1;

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CalleesSurfaceRequestV1 {
    pub node_id: String,
    pub max_depth: Option<u32>,
    pub resolve_dispatch: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSurfaceRequestV1 {
    pub node_id: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SimilarSurfaceRequestV1 {
    pub symbol: String,
    pub limit: Option<u32>,
    pub semantic_mode: Option<PrimitiveSemanticModeV1>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RenamePreviewPrimitiveRequestV1 {
    pub node_id: String,
    pub new_name: Option<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortStatusSurfaceRequestV1 {
    pub source_dir: String,
    pub target_dir: String,
    pub kinds: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortOrderSurfaceRequestV1 {
    pub source_dir: String,
    pub kinds: Option<Vec<String>>,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RedundancySurfaceRequestV1 {
    pub path: Option<String>,
    pub min_lines: Option<u32>,
    pub max_pairs: Option<u32>,
    pub similarity_threshold: Option<f64>,
    pub include_naming_only: Option<bool>,
    pub include_generated_paths: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TodosSurfaceRequestV1 {
    pub kinds: Option<Vec<String>>,
    pub path: Option<String>,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PrimitiveSymbolLocationV1 {
    pub node_id: String,
    pub name: String,
    pub qualified_name: String,
    pub kind: String,
    pub file: String,
    pub start_line: u32,
    pub end_line: u32,
    pub unavailable_fields: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContextCodeBlockV1 {
    pub node_id: String,
    pub file: String,
    pub start_line: u32,
    pub end_line: u32,
    pub code: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContextSearchMatchV1 {
    pub anchor_id: String,
    pub name: String,
    pub qualified_name: String,
    pub kind: String,
    pub file: String,
    pub exact_class: String,
    pub rank: u32,
    pub utility_micros: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PrimitiveUnavailableStatusV1 {
    Unavailable,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PrimitiveUnavailableEvidenceV1 {
    pub status: PrimitiveUnavailableStatusV1,
    pub reason_code: String,
    pub retryable: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PrimitiveLaneStateV1 {
    Stale,
    Partial,
    Unavailable,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum PrimitiveLaneStatusV1 {
    Complete(PrimitiveLaneCompleteV1),
    State {
        status: PrimitiveLaneStateV1,
        #[serde(skip_serializing_if = "Option::is_none")]
        generation: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PrimitiveLaneCompleteV1 {
    Complete,
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PrimitiveRecallV1 {
    Full,
    Partial,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PrimitiveSearchCoverageV1 {
    pub exact: PrimitiveLaneStatusV1,
    pub lexical: PrimitiveLaneStatusV1,
    pub graph: PrimitiveLaneStatusV1,
    pub semantic: PrimitiveLaneStatusV1,
    pub recall: PrimitiveRecallV1,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContextResultV1 {
    pub task: String,
    pub mode: ContextModeV1,
    /// Freshness of the served code generation, derived from the typed lane
    /// coverage and the daemon scheduler's worktree state.
    pub freshness: PrimitiveSearchFreshnessV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code_generation: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub search_matches: Vec<ContextSearchMatchV1>,
    pub symbols: Vec<PrimitiveSymbolLocationV1>,
    pub related_symbols: Vec<PrimitiveSymbolLocationV1>,
    pub code: Vec<ContextCodeBlockV1>,
    pub coverage: PrimitiveSearchCoverageV1,
    pub memory_matches: Vec<FactSearchHitV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_graph_coverage: Option<FactSearchGraphCoverageV1>,
    /// Present only when a non-current policy withheld current-only fact search.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_temporal_coverage: Option<ContextMemoryTemporalCoverageV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_matches_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified_graph_evidence: Option<PrimitiveUnavailableEvidenceV1>,
}

impl ContextResultV1 {
    pub fn with_memory_graph_coverage(
        mut self,
        memory_graph_coverage: Option<FactSearchGraphCoverageV1>,
    ) -> Self {
        self.memory_graph_coverage = memory_graph_coverage;
        self
    }

    pub fn memory_graph_coverage(&self) -> Option<FactSearchGraphCoverageV1> {
        self.memory_graph_coverage
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CalleeV1 {
    pub node_id: String,
    pub name: String,
    pub kind: String,
    pub file: String,
    pub line: u32,
    pub edge_kind: String,
    pub dispatch_via_trait: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub depth: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dispatch_from: Option<String>,
}

pub type CalleesResultV1 = Vec<CalleeV1>;

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ImpactNodeV1 {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub file: String,
    pub line: u32,
    pub depth: u32,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ImpactResultV1 {
    pub node_count: usize,
    pub complete: bool,
    pub unavailable_fields: Vec<String>,
    pub nodes: Vec<ImpactNodeV1>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NodeExpansionCostV1 {
    pub body: u64,
    pub full_file: u64,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NodeDetailsV1 {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub qualified_name: String,
    pub file: String,
    pub start_line: u32,
    pub end_line: u32,
    pub signature: Option<String>,
    pub visibility: String,
    /// Exact counters; `None`, and listed in `unavailable_fields`, when
    /// `complexity_analysis` reports the bounded walk did not cover the body.
    pub branches: Option<u32>,
    pub loops: Option<u32>,
    pub max_nesting: Option<u32>,
    pub cyclomatic_complexity: Option<u32>,
    pub complexity_analysis: ComplexityAnalysisV1,
    pub cost_to_expand: NodeExpansionCostV1,
    pub unavailable_fields: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PrimitiveNotFoundV1 {
    pub status: String,
    pub reason_code: String,
    pub node_id: String,
    pub message: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum NodeResultV1 {
    Found(NodeDetailsV1),
    NotFound(PrimitiveNotFoundV1),
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SimilarSymbolV1 {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub file: String,
    pub line: u32,
    pub signature: Option<String>,
    pub utility_micros: u64,
}

pub type SimilarResultV1 = Vec<SimilarSymbolV1>;

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RenamePreviewNodeV1 {
    pub id: String,
    pub name: String,
    pub qualified_name: String,
    pub kind: String,
    pub file: String,
    pub line: u32,
    pub snippet: Option<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RenamePreviewReferenceV1 {
    pub from_node_id: String,
    pub from_name: String,
    pub from_kind: String,
    pub edge_kind: String,
    pub file: String,
    pub line: u32,
    pub snippet: Option<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RenamePreviewTextOnlyMatchV1 {
    pub file: String,
    pub text_only_count: usize,
    pub note: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RenamePreviewPrimitiveResultV1 {
    pub read_only: bool,
    pub note: String,
    pub symbol: String,
    pub new_name: Option<String>,
    pub node: RenamePreviewNodeV1,
    pub reference_count: usize,
    pub references: Vec<RenamePreviewReferenceV1>,
    pub text_only_matches: Vec<RenamePreviewTextOnlyMatchV1>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum RenamePreviewPrimitiveOutcomeV1 {
    Preview(RenamePreviewPrimitiveResultV1),
    NotFound(PrimitiveNotFoundV1),
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortMatchedSymbolV1 {
    pub name: String,
    pub source_kind: String,
    pub target_kind: String,
    pub source_file: String,
    pub target_file: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortUnmatchedSymbolV1 {
    pub name: String,
    pub kind: String,
    pub line: u32,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortTargetOnlySymbolV1 {
    pub name: String,
    pub kind: String,
    pub file: String,
    pub line: u32,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortStatusResultV1 {
    pub source_dir: String,
    pub target_dir: String,
    pub source_count: usize,
    pub target_count: usize,
    pub matched: usize,
    pub unmatched: usize,
    pub target_only: usize,
    pub coverage_percent: f64,
    pub unmatched_by_file: BTreeMap<String, Vec<PortUnmatchedSymbolV1>>,
    pub matched_symbols: Vec<PortMatchedSymbolV1>,
    pub target_only_symbols: Vec<PortTargetOnlySymbolV1>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortOrderSymbolV1 {
    pub name: String,
    pub kind: String,
    pub file: String,
    pub line: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub depends_on: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortOrderLevelV1 {
    pub level: usize,
    pub description: String,
    pub symbols: Vec<PortOrderSymbolV1>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortCycleFileV1 {
    pub file: String,
    pub members_in_cycle: usize,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortCycleSymbolV1 {
    pub name: String,
    pub kind: String,
    pub file: String,
    pub line: u32,
    pub in_cycle_out_degree: usize,
    pub in_cycle_in_degree: usize,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortCycleAnchorV1 {
    pub name: String,
    pub file: String,
    pub line: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rationale: Option<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortCycleV1 {
    pub size: usize,
    pub files: Vec<PortCycleFileV1>,
    pub symbols: Vec<PortCycleSymbolV1>,
    pub entry_point: Option<PortCycleAnchorV1>,
    pub break_point_candidate: Option<PortCycleAnchorV1>,
    pub note: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortOrderResultV1 {
    pub source_dir: String,
    pub total_symbols: usize,
    pub returned: usize,
    pub levels: Vec<PortOrderLevelV1>,
    pub cycles: Vec<PortCycleV1>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TodoMarkerV1 {
    pub kind: String,
    pub file: String,
    pub line: u32,
    pub text: String,
    pub enclosing: Option<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TodosResultV1 {
    pub match_count: usize,
    pub by_kind: BTreeMap<String, u64>,
    pub markers: Vec<TodoMarkerV1>,
}

#[cfg(test)]
mod tests {
    use schemars::schema_for;
    use serde_json::{Value, json};

    use super::{
        ContextModeV1, ContextResultV1, ContextSurfaceRequestV1, PrimitiveFreshnessStateV1,
        PrimitiveIndexingStateV1, PrimitiveLaneCompleteV1, PrimitiveLaneStatusV1,
        PrimitiveRecallV1, PrimitiveSearchCoverageV1, PrimitiveSearchFreshnessV1,
    };
    use crate::memory::{FactSearchGraphCoverageV1, FactSearchGraphDegradationV1};

    #[test]
    fn context_memory_policy_is_optional_preserved_and_validated_at_admission() {
        use crate::memory::{CognitiveRecallExclusions, CognitiveRecallTemporalQuery};
        use tracedecay_domain::UtcMicros;

        let absent: ContextSurfaceRequestV1 =
            serde_json::from_value(json!({"task": "default"})).expect("old context request");
        absent
            .validate_memory_policy_at(UtcMicros(100))
            .expect("absent policy");
        let encoded = serde_json::to_value(&absent).expect("request JSON");
        assert!(encoded.get("temporal_query").is_none());
        assert!(encoded.get("exclusions").is_none());

        let temporal = CognitiveRecallTemporalQuery::current(UtcMicros(50))
            .with_as_of(UtcMicros(20))
            .expect("historical policy");
        let exclusions = CognitiveRecallExclusions {
            observation_ids: vec!["original-observation".to_owned()],
            ..CognitiveRecallExclusions::default()
        };
        let request: ContextSurfaceRequestV1 = serde_json::from_value(json!({
            "task": "historical context", "temporal_query": temporal, "exclusions": exclusions
        }))
        .expect("explicit policy decodes");
        request
            .validate_memory_policy_at(UtcMicros(100))
            .expect("admitted policy");
        assert_eq!(request.temporal_query.as_ref(), Some(&temporal));
        assert_eq!(request.exclusions.as_ref(), Some(&exclusions));
        assert!(request.validate_memory_policy_at(UtcMicros(49)).is_err());

        let mut malformed = request.clone();
        malformed
            .exclusions
            .as_mut()
            .expect("exclusions")
            .observation_ids
            .push("original-observation".to_owned());
        assert!(malformed.validate_memory_policy_at(UtcMicros(100)).is_err());
        malformed
            .exclusions
            .as_mut()
            .expect("exclusions")
            .observation_ids
            .clear();
        malformed
            .exclusions
            .as_mut()
            .expect("exclusions")
            .content_sha256
            .push("bad digest".to_owned());
        assert!(malformed.validate_memory_policy_at(UtcMicros(100)).is_err());

        let mut bad_temporal = serde_json::to_value(&request).expect("request JSON");
        bad_temporal["temporal_query"]["as_of"] = json!(60);
        let malformed: ContextSurfaceRequestV1 =
            serde_json::from_value(bad_temporal).expect("typed but invalid bounds");
        assert!(malformed.validate_memory_policy_at(UtcMicros(100)).is_err());
        let schema =
            serde_json::to_value(schema_for!(ContextSurfaceRequestV1)).expect("request schema");
        for field in ["temporal_query", "exclusions"] {
            assert!(schema["properties"][field].is_object());
            assert!(
                schema["required"]
                    .as_array()
                    .is_none_or(|required| !required.contains(&json!(field)))
            );
        }
    }

    #[test]
    fn context_memory_contribution_rejects_inconsistent_withheld_or_disabled_coverage() {
        use super::{ContextMemoryContributionV1, ContextMemoryTemporalCoverageV1};
        use crate::memory::{CognitiveRecallTemporalMode, CognitiveRecallTemporalQuery};
        use tracedecay_domain::UtcMicros;

        let mut request: ContextSurfaceRequestV1 = serde_json::from_value(json!({
            "task": "history", "temporal_query": CognitiveRecallTemporalQuery::current(UtcMicros(10)).with_history()
        })).expect("request");
        let withheld = Some(ContextMemoryTemporalCoverageV1::WithheldCurrentOnly {
            requested_mode: CognitiveRecallTemporalMode::History,
        });
        assert!(
            ContextMemoryContributionV1::from_matches(&request, &[], None, withheld, UtcMicros(20))
                .is_ok()
        );
        assert!(
            ContextMemoryContributionV1::from_matches(&request, &[], None, None, UtcMicros(20))
                .is_err()
        );
        assert!(
            ContextMemoryContributionV1::from_matches(
                &request,
                &[],
                Some(FactSearchGraphCoverageV1::NotMounted),
                withheld,
                UtcMicros(20)
            )
            .is_err()
        );
        request.include_memory = Some(false);
        let contribution =
            ContextMemoryContributionV1::from_matches(&request, &[], None, None, UtcMicros(20))
                .expect("disabled lane");
        assert_eq!(
            contribution.temporal_query(),
            request.temporal_query.as_ref()
        );
        assert!(
            ContextMemoryContributionV1::from_matches(&request, &[], None, withheld, UtcMicros(20))
                .is_err()
        );
        assert!(
            ContextMemoryContributionV1::from_matches(
                &request,
                &[],
                Some(FactSearchGraphCoverageV1::NotMounted),
                None,
                UtcMicros(20)
            )
            .is_err()
        );
    }

    #[test]
    fn context_memory_temporal_coverage_is_optional_and_explicit() {
        use super::ContextMemoryTemporalCoverageV1;
        use crate::memory::CognitiveRecallTemporalMode;

        let mut result = context_result();
        assert!(
            serde_json::to_value(&result)
                .expect("default result")
                .get("memory_temporal_coverage")
                .is_none()
        );
        result.memory_temporal_coverage =
            Some(ContextMemoryTemporalCoverageV1::WithheldCurrentOnly {
                requested_mode: CognitiveRecallTemporalMode::History,
            });
        assert_eq!(
            serde_json::to_value(result).expect("withheld result")["memory_temporal_coverage"],
            json!({"kind": "withheld_current_only", "requested_mode": "history"})
        );
        let schema = serde_json::to_value(schema_for!(ContextResultV1)).expect("result schema");
        assert!(
            schema["required"]
                .as_array()
                .is_none_or(|required| !required.contains(&json!("memory_temporal_coverage")))
        );
    }

    fn context_result() -> ContextResultV1 {
        ContextResultV1 {
            task: "explain memory".to_owned(),
            mode: ContextModeV1::Explore,
            freshness: PrimitiveSearchFreshnessV1 {
                state: PrimitiveFreshnessStateV1::Fresh,
                indexing: None,
            },
            code_generation: Some("generation.test".to_owned()),
            search_matches: vec![],
            symbols: vec![],
            related_symbols: vec![],
            code: vec![],
            coverage: PrimitiveSearchCoverageV1 {
                exact: PrimitiveLaneStatusV1::Complete(PrimitiveLaneCompleteV1::Complete),
                lexical: PrimitiveLaneStatusV1::Complete(PrimitiveLaneCompleteV1::Complete),
                graph: PrimitiveLaneStatusV1::Complete(PrimitiveLaneCompleteV1::Complete),
                semantic: PrimitiveLaneStatusV1::Complete(PrimitiveLaneCompleteV1::Complete),
                recall: PrimitiveRecallV1::Full,
            },
            memory_matches: vec![],
            memory_graph_coverage: None,
            memory_temporal_coverage: None,
            memory_matches_error: None,
            verified_graph_evidence: None,
        }
    }

    #[test]
    fn context_result_preserves_optional_memory_graph_coverage() {
        let absent = context_result().with_memory_graph_coverage(None);
        assert_eq!(absent.memory_graph_coverage(), None);
        assert!(
            serde_json::to_value(&absent)
                .expect("context result serializes")
                .get("memory_graph_coverage")
                .is_none()
        );

        for (coverage, expected) in [
            (
                FactSearchGraphCoverageV1::NotMounted,
                json!({"kind": "not_mounted"}),
            ),
            (
                FactSearchGraphCoverageV1::Complete {
                    root_count: 2,
                    relation_count: 3,
                    expanded_fact_count: 4,
                },
                json!({
                    "kind": "complete",
                    "root_count": 2,
                    "relation_count": 3,
                    "expanded_fact_count": 4
                }),
            ),
            (
                FactSearchGraphCoverageV1::Degraded {
                    reason: FactSearchGraphDegradationV1::BudgetExhausted,
                },
                json!({"kind": "degraded", "reason": "budget_exhausted"}),
            ),
        ] {
            let result = context_result().with_memory_graph_coverage(Some(coverage));
            assert_eq!(result.memory_graph_coverage(), Some(coverage));
            assert_eq!(
                serde_json::to_value(result).expect("context result serializes")["memory_graph_coverage"],
                expected
            );
        }
    }

    #[test]
    fn context_result_schema_exposes_optional_typed_memory_graph_coverage() {
        let schema = serde_json::to_value(schema_for!(ContextResultV1))
            .expect("context result schema serializes");
        assert!(schema["properties"]["memory_graph_coverage"].is_object());
        assert!(schema["required"].as_array().is_none_or(|required| {
            !required.contains(&Value::String("memory_graph_coverage".to_owned()))
        }));
    }

    #[test]
    fn context_contract_carries_freshness_and_lexical_routing() {
        let request: ContextSurfaceRequestV1 = serde_json::from_value(json!({
            "task": "how is stock reserved",
            "lexical_anchors": ["reserve_stock"],
            "prefer_symbol": true,
        }))
        .expect("context request decodes routing fields");
        assert_eq!(
            request.lexical_anchors.as_deref(),
            Some(&["reserve_stock".to_owned()][..])
        );
        assert_eq!(request.prefer_symbol, Some(true));

        let fresh = serde_json::to_value(context_result()).expect("context result serializes");
        assert_eq!(fresh["freshness"], json!({"state": "fresh"}));

        let mut stale = context_result();
        stale.freshness = PrimitiveSearchFreshnessV1 {
            state: PrimitiveFreshnessStateV1::PossiblyStale,
            indexing: Some(PrimitiveIndexingStateV1 {
                summary: "state=refreshing".to_owned(),
                served_generation: Some("generation.old".to_owned()),
                latest_generation: Some("generation.new".to_owned()),
                staleness_state: Some("refreshing".to_owned()),
                rebuild_in_flight: Some(true),
                stale_lanes: vec!["lexical".to_owned()],
                reason: None,
            }),
        };
        let stale = serde_json::to_value(stale).expect("context result serializes");
        assert_eq!(stale["freshness"]["state"], "possibly_stale");
        assert_eq!(
            stale["freshness"]["indexing"]["summary"],
            "state=refreshing"
        );
        assert_eq!(
            stale["freshness"]["indexing"]["stale_lanes"],
            json!(["lexical"])
        );

        let schema = serde_json::to_value(schema_for!(ContextResultV1))
            .expect("context result schema serializes");
        assert!(
            schema["required"]
                .as_array()
                .is_some_and(|required| required.contains(&Value::String("freshness".to_owned()))),
            "freshness is part of every context result"
        );
    }
}
