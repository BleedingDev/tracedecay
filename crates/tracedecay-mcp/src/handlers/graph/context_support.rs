//! Rendering and memory enrichment support for the verified context handler.

use std::fmt::Write as _;
use std::future::Future;
use std::sync::Arc;

use serde_json::{Value, json};
use tracedecay_contracts::memory::CognitiveRecallTemporalMode;
use tracedecay_contracts::retained_surfaces::{
    FactCategoryV1, FactSearchGraphCoverageV1, FactSearchGraphDegradationV1, FactSearchHitV1,
};
use tracedecay_contracts::retrieval::{
    ContextMemoryTemporalCoverageV1, ContextSurfaceRequestV1, MAX_CONTEXT_MEMORY_CONTRIBUTION_FACTS,
};
use tracedecay_contracts::{
    CancellationSignal, Deadline, now_micros, retained_surface_execution_problem,
};
use tracedecay_domain::Confidence;
use tracedecay_session_memory::memory::memory_application_error;
use tracedecay_store::{
    FactReadControl, ProjectMemoryFactSearchFilterV1, ProjectMemoryFactSearchKindV1,
    ProjectMemoryFactSearchQuery,
};

use crate::McpToolContext;
use crate::context_headings::{
    CONTEXT_CODE_HEADING, CONTEXT_ENTRY_POINTS_HEADING, CONTEXT_EXTENSION_POINTS_HEADING,
    CONTEXT_INDEX_COVERAGE_HINT_HEADING, CONTEXT_MEMORY_FEEDBACK_HINT,
    CONTEXT_MEMORY_MATCHES_HEADING, CONTEXT_RELATED_SYMBOLS_HEADING, CONTEXT_SEEN_NODE_IDS_LABEL,
    CONTEXT_TEST_COVERAGE_HEADING,
};
use tracedecay_application::tracedecay::project_memory_owner_from_layout_id;
use tracedecay_domain::errors::{Result, TraceDecayError};
use tracedecay_runtime_core::text::utf8_prefix_at_or_before;
use tracedecay_session_memory::fact_store::{ProjectFactStore, ProjectMemoryDbHandle};
use tracedecay_session_memory::memory::MemoryApplication;

const CONTEXT_MEMORY_MATCH_LIMIT: usize = 3;
const CONTEXT_MEMORY_MATCH_LIMIT_MAX: usize = MAX_CONTEXT_MEMORY_CONTRIBUTION_FACTS;
const CONTEXT_LANE_TRUNCATED_NOTE: &str =
    "\n... lane truncated; retrieve the full response handle for omitted details.\n";

pub(super) fn context_markdown_lane_preview(markdown: &str) -> String {
    let mut preview = String::with_capacity(markdown.len().min(24_000));
    let mut lane = String::new();
    let mut lane_key = String::new();
    let mut in_fence = false;

    for line in markdown.split_inclusive('\n') {
        if !in_fence && let Some(key) = context_lane_key(line) {
            push_context_lane_preview(&mut preview, &lane_key, &lane);
            lane.clear();
            lane_key = key.to_string();
        }
        lane.push_str(line);
        if line.trim_start().starts_with("```") {
            in_fence = !in_fence;
        }
    }
    push_context_lane_preview(&mut preview, &lane_key, &lane);
    preview
}

fn context_lane_key(line: &str) -> Option<&str> {
    if line.starts_with("### ") || line.starts_with(CONTEXT_SEEN_NODE_IDS_LABEL) {
        Some(line.trim_end())
    } else {
        None
    }
}

fn push_context_lane_preview(preview: &mut String, lane_key: &str, lane: &str) {
    if lane.is_empty() {
        return;
    }
    let budget = context_lane_budget(lane_key);
    if lane.len() <= budget {
        preview.push_str(lane);
        return;
    }
    let prefix = utf8_prefix_at_or_before(lane, budget);
    preview.push_str(prefix);
    if crate::tools::render::has_open_markdown_fence(prefix) {
        preview.push_str("\n```\n");
    }
    preview.push_str(CONTEXT_LANE_TRUNCATED_NOTE);
}

fn context_lane_budget(lane_key: &str) -> usize {
    if lane_key.starts_with(CONTEXT_SEEN_NODE_IDS_LABEL) {
        usize::MAX
    } else if lane_key.starts_with(CONTEXT_CODE_HEADING) {
        8_500
    } else if lane_key.starts_with(CONTEXT_RELATED_SYMBOLS_HEADING) {
        3_500
    } else if lane_key.starts_with(CONTEXT_ENTRY_POINTS_HEADING)
        || lane_key.starts_with(CONTEXT_TEST_COVERAGE_HEADING)
    {
        3_000
    } else if lane_key.starts_with(CONTEXT_MEMORY_MATCHES_HEADING) {
        2_500
    } else if lane_key.starts_with(CONTEXT_EXTENSION_POINTS_HEADING) {
        1_500
    } else if lane_key.starts_with(CONTEXT_INDEX_COVERAGE_HINT_HEADING) {
        1_000
    } else {
        2_000
    }
}

pub(super) fn insert_context_memory_section(
    output: &mut String,
    memory_matches: &[FactSearchHitV1],
    memory_matches_error: Option<&str>,
    temporal_coverage: Option<ContextMemoryTemporalCoverageV1>,
) {
    let Some(section) =
        context_memory_section(memory_matches, memory_matches_error, temporal_coverage)
    else {
        return;
    };
    if let Some(idx) = output.find(&format!("\n{CONTEXT_ENTRY_POINTS_HEADING}")) {
        output.insert_str(idx, &section);
    } else {
        output.push_str(&section);
    }
}

pub(super) fn context_memory_section(
    memory_matches: &[FactSearchHitV1],
    memory_matches_error: Option<&str>,
    temporal_coverage: Option<ContextMemoryTemporalCoverageV1>,
) -> Option<String> {
    let mut section = String::new();
    if let Some(ContextMemoryTemporalCoverageV1::WithheldCurrentOnly { requested_mode }) =
        temporal_coverage
    {
        let mode = match requested_mode {
            CognitiveRecallTemporalMode::Current => "current",
            CognitiveRecallTemporalMode::AsOf => "as_of",
            CognitiveRecallTemporalMode::Interval => "interval",
            CognitiveRecallTemporalMode::History => "history",
        };
        let _ = writeln!(
            section,
            "\n{CONTEXT_MEMORY_MATCHES_HEADING}\nWithheld: canonical fact search supports current state only; requested temporal mode={mode}."
        );
        return Some(section);
    }
    if !memory_matches.is_empty() {
        section.push('\n');
        section.push_str(CONTEXT_MEMORY_MATCHES_HEADING);
        section.push('\n');
        for hit in memory_matches {
            let _ = writeln!(
                section,
                "- fact_id={} category={} trust={:.2} score={:.3}: {}",
                hit.fact.fact_id,
                context_fact_category(hit.fact.category),
                f64::from(hit.fact.trust_score_millionths) / 1_000_000.0,
                f64::from(hit.scores.score_millionths) / 1_000_000.0,
                compact_memory_content(&hit.fact.content)
            );
        }
        section.push('\n');
        section.push_str(CONTEXT_MEMORY_FEEDBACK_HINT);
        section.push('\n');
        return Some(section);
    }
    if let Some(err) = memory_matches_error {
        let _ = writeln!(
            section,
            "\n{CONTEXT_MEMORY_MATCHES_HEADING}\nUnavailable: {err}"
        );
        return Some(section);
    }
    None
}

fn compact_memory_content(content: &str) -> String {
    content.split_whitespace().collect::<Vec<_>>().join(" ")
}

const fn context_fact_category(category: FactCategoryV1) -> &'static str {
    match category {
        FactCategoryV1::General => "general",
        FactCategoryV1::UserPref => "user_pref",
        FactCategoryV1::Project => "project",
        FactCategoryV1::Tool => "tool",
        FactCategoryV1::Decision => "decision",
        FactCategoryV1::CodeArea => "code_area",
    }
}

pub(super) struct ContextMemoryOptions {
    include_memory: bool,
    limit: usize,
    min_trust: f64,
    temporal_coverage: Option<ContextMemoryTemporalCoverageV1>,
}

pub(super) fn context_memory_options(request: &ContextSurfaceRequestV1) -> ContextMemoryOptions {
    let include_memory = request.include_memory.unwrap_or(true);
    let limit = request
        .memory_limit
        .map_or(CONTEXT_MEMORY_MATCH_LIMIT, |value| value as usize)
        .clamp(1, CONTEXT_MEMORY_MATCH_LIMIT_MAX);
    let min_trust = request.memory_min_trust.unwrap_or(0.5).clamp(0.0, 1.0);
    let temporal_coverage = request.temporal_query.as_ref().and_then(|query| {
        (include_memory && query.mode() != CognitiveRecallTemporalMode::Current).then_some(
            ContextMemoryTemporalCoverageV1::WithheldCurrentOnly {
                requested_mode: query.mode(),
            },
        )
    });
    ContextMemoryOptions {
        include_memory,
        limit,
        min_trust,
        temporal_coverage,
    }
}

pub(super) fn context_memory_enabled(options: &ContextMemoryOptions) -> bool {
    options.include_memory && options.temporal_coverage.is_none()
}

pub(super) fn context_memory_read_control(
    options: &ContextMemoryOptions,
    deadline: Option<&Deadline>,
    cancellation: Option<&CancellationSignal>,
) -> Result<Option<FactReadControl>> {
    if !context_memory_enabled(options) {
        return Ok(None);
    }
    let deadline = deadline.ok_or_else(|| TraceDecayError::Config {
        message: "context memory search requires the admitted request deadline".to_owned(),
    })?;
    let cancellation = cancellation
        .cloned()
        .ok_or_else(|| TraceDecayError::Config {
            message: "context memory search requires the admitted cancellation signal".to_owned(),
        })?;
    let expires_at = deadline.expires_at;
    Ok(Some(FactReadControl::new(Arc::new(move || {
        cancellation.is_cancelled() || now_micros() >= expires_at
    }))))
}

pub(super) fn context_memory_analytics_value(
    options: &ContextMemoryOptions,
    memory_matches: &[FactSearchHitV1],
    memory_matches_error: Option<&str>,
) -> Value {
    let fact_ids: Vec<Value> = memory_matches
        .iter()
        .map(|hit| Value::String(hit.fact.fact_id.as_str().to_owned()))
        .collect();
    json!({
        "include_memory": options.include_memory,
        "limit": options.limit,
        "min_trust": options.min_trust,
        "match_count": fact_ids.len(),
        "fact_ids": fact_ids,
        "error": memory_matches_error,
    })
}

pub(super) struct ContextMemoryMatches {
    pub(super) hits: Vec<FactSearchHitV1>,
    pub(super) graph_coverage: FactSearchGraphCoverageV1,
}

pub(super) struct ContextMemoryOutcome {
    pub(super) hits: Vec<FactSearchHitV1>,
    pub(super) graph_coverage: Option<FactSearchGraphCoverageV1>,
    pub(super) temporal_coverage: Option<ContextMemoryTemporalCoverageV1>,
    pub(super) error: Option<String>,
}

#[hotpath::measure(future = true, label = "mcp.graph.context_memory")]
pub(super) async fn context_memory_outcome<'read, Read, ReadFuture>(
    options: &ContextMemoryOptions,
    read_control: Option<&'read FactReadControl>,
    read: Read,
) -> ContextMemoryOutcome
where
    Read: FnOnce(&'read FactReadControl) -> ReadFuture,
    ReadFuture: Future<Output = Result<ContextMemoryMatches>>,
{
    if let Some(temporal_coverage) = options.temporal_coverage {
        return ContextMemoryOutcome {
            hits: Vec::new(),
            graph_coverage: None,
            temporal_coverage: Some(temporal_coverage),
            error: None,
        };
    }
    let Some(read_control) = read_control.filter(|_| options.include_memory) else {
        return ContextMemoryOutcome {
            hits: Vec::new(),
            graph_coverage: None,
            temporal_coverage: None,
            error: None,
        };
    };
    match read(read_control).await {
        Ok(matches) => ContextMemoryOutcome {
            hits: matches.hits,
            graph_coverage: Some(matches.graph_coverage),
            temporal_coverage: None,
            error: None,
        },
        Err(error) => ContextMemoryOutcome {
            hits: Vec::new(),
            graph_coverage: Some(FactSearchGraphCoverageV1::Degraded {
                reason: FactSearchGraphDegradationV1::Unavailable,
            }),
            temporal_coverage: None,
            error: Some(error.to_string()),
        },
    }
}

fn context_memory_application<'a>(
    ctx: &'a McpToolContext<'a>,
) -> Result<MemoryApplication<ProjectFactStore<'a>>> {
    let project = ctx.project();
    let database = project.graph_database();
    let owner =
        project_memory_owner_from_layout_id(project.store_layout().identity.project_id.as_deref())?;
    let store = ProjectMemoryDbHandle::Active(database).into_fact_store();
    MemoryApplication::new(owner, store).map_err(memory_application_error)
}

pub(super) async fn context_memory_matches(
    ctx: &McpToolContext<'_>,
    task: &str,
    options: &ContextMemoryOptions,
    read_control: &FactReadControl,
) -> Result<ContextMemoryMatches> {
    let memory = context_memory_application(ctx)?;
    let min_trust =
        Confidence::new(options.min_trust).map_err(|error| TraceDecayError::Config {
            message: format!("invalid context memory trust threshold: {error}"),
        })?;
    let filter =
        ProjectMemoryFactSearchFilterV1::new(None, Some(min_trust), None).map_err(|error| {
            TraceDecayError::database_operation("construct context memory filter", error)
        })?;
    let query = ProjectMemoryFactSearchQuery::with_filter(
        memory.owner().clone(),
        ProjectMemoryFactSearchKindV1::Search,
        Some(task.to_owned()),
        filter,
        None,
        options.limit,
    )
    .map_err(|error| {
        TraceDecayError::database_operation("construct context memory query", error)
    })?;
    let page = memory
        .search_project_memory_facts(query, read_control)
        .await
        .map_err(memory_application_error)?;
    let mapped =
        tracedecay_session_memory::memory_mapping::search_page(&page).map_err(|error| {
            let problem = retained_surface_execution_problem(error);
            TraceDecayError::Database {
                operation: "project canonical context memory".to_string(),
                message: problem.canonical_code().to_string(),
            }
        })?;
    Ok(ContextMemoryMatches {
        hits: mapped.hits,
        graph_coverage: mapped.graph_coverage,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tracedecay_contracts::memory::CognitiveRecallTemporalQuery;
    use tracedecay_domain::UtcMicros;

    #[tokio::test]
    async fn non_current_policy_never_invokes_current_fact_read() {
        let reads = AtomicUsize::new(0);
        let control = FactReadControl::new(Arc::new(|| false));
        for query in [
            CognitiveRecallTemporalQuery::current(UtcMicros(10))
                .with_as_of(UtcMicros(5))
                .expect("as-of"),
            CognitiveRecallTemporalQuery::current(UtcMicros(10))
                .with_interval(UtcMicros(3), UtcMicros(8))
                .expect("interval"),
            CognitiveRecallTemporalQuery::current(UtcMicros(10)).with_history(),
        ] {
            let request: ContextSurfaceRequestV1 =
                serde_json::from_value(json!({"task": "history", "temporal_query": query}))
                    .expect("context request");
            request
                .validate_memory_policy_at(UtcMicros(20))
                .expect("admitted policy");
            let options = context_memory_options(&request);
            assert!(
                context_memory_read_control(&options, None, None)
                    .expect("withheld lane needs no read control")
                    .is_none()
            );
            let outcome = context_memory_outcome(&options, Some(&control), |_| {
                reads.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok(ContextMemoryMatches {
                    hits: Vec::new(),
                    graph_coverage: FactSearchGraphCoverageV1::NotMounted,
                }))
            })
            .await;
            assert_eq!(reads.load(Ordering::SeqCst), 0);
            assert!(outcome.hits.is_empty());
            assert!(outcome.graph_coverage.is_none());
            assert!(outcome.error.is_none());
            assert_eq!(
                outcome.temporal_coverage,
                Some(ContextMemoryTemporalCoverageV1::WithheldCurrentOnly {
                    requested_mode: query.mode()
                })
            );
            let section = context_memory_section(&[], None, outcome.temporal_coverage)
                .expect("explicit withholding");
            assert!(section.contains("current state only"));
            assert!(section.contains("requested temporal mode="));
        }
    }

    #[tokio::test]
    async fn default_and_current_keep_fact_read_while_disabled_skips_it() {
        let control = FactReadControl::new(Arc::new(|| false));
        let reads = AtomicUsize::new(0);
        for request in [
            json!({"task": "default"}),
            json!({"task": "current", "temporal_query": CognitiveRecallTemporalQuery::current(UtcMicros(10))}),
        ] {
            let request: ContextSurfaceRequestV1 =
                serde_json::from_value(request).expect("context request");
            let options = context_memory_options(&request);
            let outcome = context_memory_outcome(&options, Some(&control), |_| {
                reads.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok(ContextMemoryMatches {
                    hits: Vec::new(),
                    graph_coverage: FactSearchGraphCoverageV1::NotMounted,
                }))
            })
            .await;
            assert!(outcome.temporal_coverage.is_none());
            assert_eq!(
                outcome.graph_coverage,
                Some(FactSearchGraphCoverageV1::NotMounted)
            );
        }
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        let request: ContextSurfaceRequestV1 = serde_json::from_value(json!({
            "task": "disabled", "include_memory": false,
            "temporal_query": CognitiveRecallTemporalQuery::current(UtcMicros(10)).with_history()
        }))
        .expect("disabled request");
        let options = context_memory_options(&request);
        let read_control =
            context_memory_read_control(&options, None, None).expect("disabled control");
        assert!(read_control.is_none());
        // Even a mistakenly supplied control cannot turn disabled memory into a read.
        let outcome = context_memory_outcome(&options, Some(&control), |_| {
            reads.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok(ContextMemoryMatches {
                hits: Vec::new(),
                graph_coverage: FactSearchGraphCoverageV1::NotMounted,
            }))
        })
        .await;
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        assert!(outcome.temporal_coverage.is_none());
        assert!(outcome.graph_coverage.is_none());
        assert!(context_memory_section(&[], None, None).is_none());
    }
}
