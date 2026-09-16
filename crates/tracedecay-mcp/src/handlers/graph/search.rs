//! Graph leftover handlers that read the admitted project/request authorities:
//! `search`, `context`, `similar`, `find_exact_symbol`, `rename_preview`.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::future::Future;
use std::path::Path;

use serde_json::{Value, json};
use tracedecay_code_index::graph_projection::CodeGraphSymbolSummaryV1;
use tracedecay_contracts::retrieval::{
    ContextCodeBlockV1, ContextMemoryContributionV1, ContextModeV1, ContextResultV1,
    ContextSearchMatchV1, ContextSurfaceRequestV1, RedundancyScopeV1, RedundancySurfaceRequestV1,
    RenamePreviewNodeV1, RenamePreviewPrimitiveRequestV1, RenamePreviewPrimitiveResultV1,
    RenamePreviewReferenceV1, RenamePreviewTextOnlyMatchV1, SimilarAlignedDifferenceV1,
    SimilarAlignmentAnchorV1, SimilarAlignmentV1, SimilarContainmentV1, SimilarCoverageV1,
    SimilarFamilyV1, SimilarMatchClassV1, SimilarNearCoverageV1, SimilarNearMatchV1,
    SimilarNearPartialReasonV1, SimilarNearResultV1, SimilarNearUnavailableReasonV1,
    SimilarOccurrenceV1, SimilarResultV1, SimilarSourceExtentV1, SimilarSurfaceRequestV1,
    SimilarTargetV1, SimilarTokenSpanV1,
};
use tracedecay_domain::ExactClass;
use tracedecay_domain::errors::{Result, TraceDecayError};
#[cfg(test)]
use tracedecay_query::retrieval::lexical::LexicalRoutingV1;

use crate::context_headings::CONTEXT_SEEN_NODE_IDS_LABEL;
use crate::handlers::dependency_hints;
use crate::handlers::support::{
    CONTEXT_MEMORY_ANALYTICS_KEY, decode_primitive_request, generic_tool_result as support_generic,
    rendered_tool_result as support_rendered, retrieval_cursor,
    take_internal_context_memory_analytics, text_tool_result, unique_file_paths,
};
use crate::tools::render::{self, Md};
use crate::{McpToolContext, ToolResult};

use super::context_markdown::{append_verified_plan_context, verified_context_markdown};
use super::context_support::{
    ContextMemoryOutcome, context_markdown_lane_preview, context_memory_analytics_value,
    context_memory_matches, context_memory_options, context_memory_outcome,
    context_memory_read_control, insert_context_memory_section,
};
use super::primitive_surface::{
    search_coverage as primitive_search_coverage, symbol_location as primitive_symbol_location,
};
use super::search_evidence::{
    SearchGraphEvidence, bind_verified_graph_to_search, race_primary_search_with_graph,
};
use super::search_freshness::{
    ServedGenerationV1, freshness_lines, search_freshness, worktree_freshness_from_payload,
};
use super::verified::CODE_SYMBOL_EVIDENCE_PREFIX;
use super::{
    graph_occurrence_id, graph_symbol_end_line, graph_symbol_paths, graph_symbols_in_scope,
    line_for_byte_offset, node_not_found as node_not_found_result, required_graph_file_path,
    required_graph_metadata, single_graph_adjacency_batch,
};
use super::{lexical_routing, search_evidence};

#[cfg(test)]
use super::context_support::context_memory_section;

async fn execute_code_index_search(
    executor: Option<&tracedecay_query::code_search::CodeIndexSearchExecutor>,
    request: tracedecay_query::code_search::CodeIndexSearchRequestV1,
) -> tracedecay_query::code_search::CodeIndexSearchOutcomeV1 {
    match executor {
        Some(executor) => executor(request).await,
        None => tracedecay_query::code_search::CodeIndexSearchOutcomeV1::Unavailable(
            tracedecay_query::code_search::CodeIndexSearchUnavailableV1 {
                code_generation: None,
                reason:
                    tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::CapabilityUnavailable,
                coverage: tracedecay_query::code_search::CodeIndexSearchCoverageV1::unavailable(
                    "code_index_unavailable",
                ),
            },
        ),
    }
}

const IGNORED_DEPENDENCY_GENERATION_ADVANCED: &str =
    "application.symbol-graph.ignored-dependency-generation-advanced";

fn preserve_complete_search_after_lazy_admission(result: Result<()>) -> Result<()> {
    match result {
        Err(error)
            if error
                .project_route_context()
                .is_some_and(|(reason_code, _, _)| {
                    reason_code == IGNORED_DEPENDENCY_GENERATION_ADVANCED
                }) =>
        {
            Ok(())
        }
        result => result,
    }
}

/// Renders the per-lane recall marker so a caller can tell a full-recall
/// answer from one produced while a lane was down. Emitted on every search
/// response, including the successful ones, because "no matches" and "the
/// matching lane was not running" are otherwise indistinguishable.
fn coverage_value(coverage: &tracedecay_query::code_search::CodeIndexSearchCoverageV1) -> Value {
    fn lane(status: &tracedecay_query::code_search::CodeIndexLaneStatusV1) -> Value {
        match status {
            tracedecay_query::code_search::CodeIndexLaneStatusV1::Complete => json!("complete"),
            tracedecay_query::code_search::CodeIndexLaneStatusV1::Stale { generation } => json!({
                "status": "stale",
                "generation": generation,
            }),
            tracedecay_query::code_search::CodeIndexLaneStatusV1::Partial {
                generation,
                reason,
            } => {
                json!({
                    "status": "partial",
                    "generation": generation,
                    "reason": reason,
                })
            }
            tracedecay_query::code_search::CodeIndexLaneStatusV1::Unavailable { reason } => json!({
                "status": "unavailable",
                "reason": reason,
            }),
        }
    }

    json!({
        "exact": lane(&coverage.exact),
        "lexical": lane(&coverage.lexical),
        "graph": lane(&coverage.graph),
        "recall": if coverage.is_degraded() { "partial" } else { "full" },
    })
}

fn user_line(line: u32) -> u32 {
    line.saturating_add(1)
}

fn rendered_tool_result<F>(
    ctx: &McpToolContext<'_>,
    args: &Value,
    value: &Value,
    touched_files: Vec<String>,
    md: F,
) -> ToolResult
where
    F: FnOnce() -> String,
{
    support_rendered(Some(ctx.project_root()), args, value, touched_files, md)
}

/// [`rendered_tool_result`] with the default [`render::generic_md`] body.
fn generic_tool_result(
    ctx: &McpToolContext<'_>,
    args: &Value,
    value: &Value,
    touched_files: Vec<String>,
) -> ToolResult {
    support_generic(Some(ctx.project_root()), args, value, touched_files)
}

fn rendered_context_tool_result(
    project_root: &Path,
    args: &Value,
    mut value: Value,
    touched_files: Vec<String>,
    full_markdown: String,
    preview_markdown: Option<&str>,
    memory_contribution: ContextMemoryContributionV1,
) -> ToolResult {
    let internal_analytics = take_internal_context_memory_analytics(&mut value);
    let text = if render::wants_json(args) {
        render::finalize(Some(project_root), args, &value, || full_markdown)
    } else {
        render::markdown_preview_with_handle(
            Some(project_root),
            &full_markdown,
            preview_markdown.unwrap_or(&full_markdown),
        )
    };
    let result = text_tool_result(&text, touched_files)
        .with_context_memory_contribution(memory_contribution);
    if let Some(internal_analytics) = internal_analytics {
        result.with_internal_analytics(internal_analytics)
    } else {
        result
    }
}

#[hotpath::measure(label = "mcp.graph.search.total")]
pub async fn handle_search<F>(
    ctx: &McpToolContext<'_>,
    graph: F,
    args: Value,
    scope_prefix: Option<&str>,
    ignored_dependency_admission: Option<
        &dyn tracedecay_application::code_index::CodeIndexIgnoredDependencyAdmissionPortV1,
    >,
) -> Result<ToolResult>
where
    F: Future<Output = Result<tracedecay_graph_query::VerifiedGraphQuery>>,
{
    let search_executor = ctx.code_index_search_executor();
    let search_authority = ctx.code_index_search_authority();
    let deadline = ctx.deadline().cloned();
    let cancellation = ctx.cancellation().cloned();
    let query =
        args.get("query")
            .and_then(|v| v.as_str())
            .ok_or_else(|| TraceDecayError::Config {
                message: "missing required parameter: query".to_string(),
            })?;

    let lexical_routing = lexical_routing::routing_from_args(&args)?;
    let lazy_indexing_requested = dependency_hints::lazy_indexing_requested(&args);
    let cursor = retrieval_cursor(&args)?;
    let include_graph_node_ids = render::wants_json(&args);
    let limit = args
        .get("limit")
        .and_then(serde_json::Value::as_u64)
        .map_or(10, |v| v.min(500) as usize);
    // A scope prefix cannot be applied as a post-filter here the way the
    // sibling handlers do it: the retrieval pipeline returns anchor-keyed
    // candidates that carry no file path. Refusing to search at all would make
    // the tool return nothing for the whole session (any serve launched from a
    // subdirectory sets a scope), so run the search and report below that the
    // scope was not honored rather than silently implying it was.
    let search_request = tracedecay_query::code_search::CodeIndexSearchRequestV1 {
        project_root: ctx.project_root().to_path_buf(),
        query: query.to_owned(),
        source_revision: None,
        source_tree: None,
        source_reference: None,
        limit,
        cursor,
        lexical_routing,
        authority: search_authority.cloned(),
        deadline: deadline.clone(),
        cancellation: cancellation.clone(),
    };
    let search = execute_code_index_search(search_executor, search_request.clone());
    let (mut outcome, graph) = race_primary_search_with_graph(
        search,
        graph,
        lazy_indexing_requested,
        Some(limit),
        scope_prefix.is_some(),
    )
    .await;
    let refresh_after_generation_mismatch = matches!(
        (&outcome, &graph),
        (
            tracedecay_query::code_search::CodeIndexSearchOutcomeV1::Complete(complete),
            Ok(graph),
        ) if graph.generation().as_str() != complete.code_generation
            && (scope_prefix.is_some()
                || dependency_hints::should_check_external_import_hint(
                    complete.ordered_candidates.len(),
                    limit,
                ))
    );
    if refresh_after_generation_mismatch {
        let refreshed = execute_code_index_search(search_executor, search_request).await;
        if matches!(
            refreshed,
            tracedecay_query::code_search::CodeIndexSearchOutcomeV1::Complete(_)
        ) {
            outcome = refreshed;
        }
    }
    // Read after the search settles: the verdict must describe the scheduler
    // state at serve time, not a snapshot taken before the lanes ran.
    let freshness_payload = ctx.freshness().await;
    let worktree_freshness = worktree_freshness_from_payload(freshness_payload.as_ref());
    match outcome {
        tracedecay_query::code_search::CodeIndexSearchOutcomeV1::Complete(complete) => {
            let graph = if lazy_indexing_requested && complete.ordered_candidates.is_empty() {
                // Explicit ignored-dependency admission is generation-checked
                // by the canonical admission port against the graph's own
                // active generation. It must therefore inspect the verified
                // graph before binding optional enrichment to the text-search
                // generation: text can truthfully serve one generation while
                // graph activation has already advanced to its successor.
                let graph = graph?;
                preserve_complete_search_after_lazy_admission(
                    hotpath::future!(
                        dependency_hints::admit_verified_ignored_dependency(
                            ctx,
                            ignored_dependency_admission,
                            &graph,
                            query,
                            scope_prefix
                        ),
                        label = "mcp.graph.search.admit"
                    )
                    .await,
                )?;
                bind_verified_graph_to_search(Ok(graph), &complete.code_generation)
            } else {
                bind_verified_graph_to_search(graph, &complete.code_generation)
            };
            let mut results = Vec::with_capacity(complete.ordered_candidates.len());
            let mut graph_evidence = SearchGraphEvidence::new(graph.as_ref());
            // The generation-bound display metadata names each result's
            // declaring file; that set is the raw-read counterfactual the
            // savings accounting charges this response against.
            let touched_files = unique_file_paths(
                complete
                    .ordered_candidates
                    .iter()
                    .filter_map(|ranked| {
                        complete.display_by_anchor.get(&ranked.candidate.anchor_id)
                    })
                    .map(|display| display.path.as_str()),
            );
            hotpath::measure_block!("mcp.graph.search.graph", {
                for ranked in &complete.ordered_candidates {
                    let mut result = json!(ranked);
                    let anchor = ranked.candidate.anchor_id.as_str();
                    if anchor.starts_with(CODE_SYMBOL_EVIDENCE_PREFIX) {
                        result["node_id"] = json!(graph_occurrence_id(anchor)?);
                    }
                    if let Some(display) =
                        complete.display_by_anchor.get(&ranked.candidate.anchor_id)
                    {
                        result["display"] = json!({
                            "name": display.name,
                            "qualified_name": display.qualified_name,
                            "kind": display.kind,
                            "path": display.path,
                        });
                        if include_graph_node_ids && result.get("node_id").is_none() {
                            graph_evidence.enrich_node_id(&mut result, display);
                        }
                    }
                    results.push(result);
                }
            });
            let result_count = results.len();
            let freshness = search_freshness(
                ServedGenerationV1::Served(&complete.code_generation),
                &complete.coverage,
                &worktree_freshness,
            );
            let mut output = hotpath::measure_block!(
                "mcp.graph.search.serialize",
                json!({
                "freshness": freshness,
                "code_generation": complete.code_generation,
                "query_fallback_digest": &complete.query_fallback.digest,
                "next_cursor": complete.next_cursor
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()?,
                "coverage": coverage_value(&complete.coverage),
                })
            );
            lexical_routing::attach_route_evidence(
                &mut output,
                &mut results,
                &complete.lexical_routes,
            )?;
            output["results"] = Value::Array(results);
            if let Some(scope) = scope_prefix {
                output["scope_prefix"] = json!(scope);
                output["scope_prefix_applied"] = json!(false);
            }
            if let Some(unavailable) = graph_evidence.unavailable() {
                output["verified_graph_evidence"] = unavailable.clone();
            }
            if (scope_prefix.is_some()
                || dependency_hints::should_check_external_import_hint(result_count, limit))
                && let Some(hint) =
                    graph_evidence.external_import_hint(ctx, query, limit, scope_prefix)
            {
                output["external_import_hint"] = hint;
            }
            let output = output;
            Ok(rendered_tool_result(
                ctx,
                &args,
                &output,
                touched_files,
                || {
                    format!(
                        "{}{}",
                        freshness_lines(&freshness),
                        render_search_md(&output)
                    )
                },
            ))
        }
        tracedecay_query::code_search::CodeIndexSearchOutcomeV1::Unavailable(unavailable) => {
            let reason = unavailable.reason.as_str();
            let graph_evidence = SearchGraphEvidence::new(graph.as_ref());
            let freshness = search_freshness(
                ServedGenerationV1::Unavailable { reason },
                &unavailable.coverage,
                &worktree_freshness,
            );
            let mut output = hotpath::measure_block!(
                "mcp.graph.search.serialize",
                json!({
                    "freshness": freshness,
                    "results": [],
                    "code_generation": unavailable.code_generation,
                    "query_fallback_digest": Value::Null,
                    "status": "unavailable",
                    "reason": reason,
                    "coverage": coverage_value(&unavailable.coverage),
                })
            );
            if let Some(unavailable_graph) = graph_evidence.unavailable() {
                output["verified_graph_evidence"] = unavailable_graph.clone();
            }
            let failure = format!("code-index search unavailable: {reason}");
            Ok(rendered_tool_result(ctx, &args, &output, Vec::new(), || {
                format!(
                    "{}{}",
                    freshness_lines(&freshness),
                    render_search_md(&output)
                )
            })
            .with_failure_message(failure))
        }
    }
}

/// Warns, in the human-facing body, that a result list is short because a lane
/// was missing. A degraded page is otherwise indistinguishable from a thorough
/// one, which is exactly how a partial answer gets trusted as a complete one.
fn append_coverage_md(md: &mut Md, value: &Value) {
    let Some(coverage) = value.get("coverage") else {
        return;
    };
    if coverage.get("recall").and_then(Value::as_str) != Some("partial") {
        return;
    }
    let mut notes = Vec::new();
    for lane in ["exact", "lexical", "graph"] {
        let status = coverage.get(lane);
        match status
            .and_then(|status| status.get("status"))
            .and_then(Value::as_str)
        {
            Some("stale") => {
                let generation = status
                    .and_then(|status| status.get("generation"))
                    .and_then(Value::as_str)
                    .unwrap_or("previous");
                notes.push(format!("{lane}: stale (generation `{generation}`)"));
            }
            Some("unavailable") => {
                let reason = status
                    .and_then(|status| status.get("reason"))
                    .and_then(Value::as_str)
                    .unwrap_or("unavailable");
                notes.push(format!("{lane}: unavailable ({reason})"));
            }
            _ => {}
        }
    }
    if notes.is_empty() {
        return;
    }
    md.blank()
        .heading(3, "Coverage")
        .line("Partial recall — some retrieval lanes did not answer:");
    for note in notes {
        md.bullet(&note);
    }
}

fn render_search_md(value: &Value) -> String {
    let items = if value.is_array() {
        value.as_array()
    } else {
        value.get("results").and_then(Value::as_array)
    };
    let mut md = Md::new();
    md.heading(2, "Search Results");
    match items {
        Some(items) if !items.is_empty() => {
            for it in items {
                if let Some(candidate) = it.get("candidate") {
                    let anchor = render::field_str(candidate, "anchor_id");
                    let exact_class = render::field_str(candidate, "exact_class");
                    let utility = candidate
                        .get("utility_micros")
                        .and_then(Value::as_u64)
                        .unwrap_or_default();
                    let ordinal = it
                        .get("final_ordinal")
                        .and_then(Value::as_u64)
                        .unwrap_or_default();
                    let via = lexical_routing::result_route_suffix(it);
                    if let Some(display) = it.get("display") {
                        let name = render::field_str(display, "name");
                        let kind = render::field_str(display, "kind");
                        md.bullet(&format!(
                            "**{name}** ({kind}, {exact_class}) — rank {} · utility {utility}{via}",
                            ordinal.saturating_add(1)
                        ));
                        md.line(&format!("  anchor_id: `{anchor}`"));
                    } else {
                        md.bullet(&format!(
                            "**{anchor}** ({exact_class}) — rank {} · utility {utility}{via}",
                            ordinal.saturating_add(1)
                        ));
                    }
                    if let Some(node_id) = it.get("node_id").and_then(Value::as_str) {
                        md.line(&format!(
                            "  Read source: `tracedecay_source_body` with `node_id: {node_id}`"
                        ));
                    }
                    continue;
                }
                let name = render::field_str(it, "name");
                let kind = render::field_str(it, "kind");
                let file = render::field_str(it, "file");
                let line = render::field_i64(it, "line");
                let id = render::field_str(it, "id");
                let score = it.get("score").and_then(Value::as_f64).unwrap_or(0.0);
                md.bullet(&format!(
                    "**{name}** ({kind}) — {file}:{line} · score {score:.1}"
                ));
                let sig = render::field_str(it, "signature");
                if sig.is_empty() {
                    md.line(&format!("  `{id}`"));
                } else {
                    md.line(&format!("  `{id}` · `{sig}`"));
                }
            }
        }
        _ => {
            md.empty_note("No matching symbols.");
        }
    }
    if let Some(reason) = value.get("reason").and_then(Value::as_str) {
        md.blank()
            .heading(3, "Availability")
            .line(&format!("Search unavailable: {reason}."));
    }
    lexical_routing::append_routes_md(&mut md, value);
    append_coverage_md(&mut md, value);
    if let Some(msg) = value
        .get("index_coverage_hint")
        .and_then(|h| h.get("message"))
        .and_then(Value::as_str)
    {
        md.blank().heading(3, "Index Coverage Hint").line(msg);
    }
    dependency_hints::append_external_import_hint_md(&mut md, value);
    search_evidence::append_verified_graph_evidence_md(&mut md, value);
    md.render()
}

/// Related-symbol assembly is a page, not a complete fan-out. The callers
/// tool uses a 50k refuse budget; context used to keep that budget, hydrate
/// every edge, then discard all but `max_nodes`. That walk is CPU-bound and
/// shows no warm benefit. Cap examination at a small multiple of the kept
/// page. Semantic kind lives on the edge entity (not the physical
/// SOURCE/TARGET relation type), so the page is all-kinds — the same
/// neighborhood the previous complete walk returned, just a prefix.
fn context_related_relation_budget(max_nodes: usize) -> usize {
    max_nodes.saturating_mul(4).clamp(16, 64)
}

#[derive(Default)]
struct ContextGraphProjection {
    selected: Vec<CodeGraphSymbolSummaryV1>,
    related: Vec<CodeGraphSymbolSummaryV1>,
    code_blocks: Vec<ContextCodeBlockV1>,
    touched_files: Vec<String>,
}

fn context_search_matches(
    complete: &tracedecay_query::code_search::CodeIndexSearchCompletedV1,
    scope_prefix: Option<&str>,
) -> Vec<ContextSearchMatchV1> {
    complete
        .ordered_candidates
        .iter()
        .filter_map(|ranked| {
            let display = complete
                .display_by_anchor
                .get(&ranked.candidate.anchor_id)?;
            if scope_prefix.is_some_and(|prefix| !display.path.starts_with(prefix)) {
                return None;
            }
            let exact_class = match ranked.candidate.exact_class {
                ExactClass::ExactMessage => "exact_message",
                ExactClass::ExactLiteralPhrase => "exact_literal_phrase",
                ExactClass::Approximate => "approximate",
            };
            Some(ContextSearchMatchV1 {
                anchor_id: ranked.candidate.anchor_id.as_str().to_owned(),
                name: display.name.clone(),
                qualified_name: display.qualified_name.clone(),
                kind: display.kind.clone(),
                file: display.path.clone(),
                exact_class: exact_class.to_owned(),
                rank: ranked.final_ordinal.saturating_add(1),
                utility_micros: ranked.candidate.utility_micros,
            })
        })
        .collect()
}

fn context_graph_projection(
    ctx: &McpToolContext<'_>,
    graph: &tracedecay_graph_query::VerifiedGraphQuery,
    complete: &tracedecay_query::code_search::CodeIndexSearchCompletedV1,
    scope_prefix: Option<&str>,
    max_nodes: usize,
    include_code: bool,
    max_code_blocks: usize,
) -> Result<ContextGraphProjection> {
    let mut selected = Vec::new();
    for ranked in &complete.ordered_candidates {
        let Some(display) = complete.display_by_anchor.get(&ranked.candidate.anchor_id) else {
            continue;
        };
        if scope_prefix.is_some_and(|prefix| !display.path.starts_with(prefix)) {
            continue;
        }
        let candidates =
            graph.resolve_qualified_name(&display.qualified_name, Some(&display.kind), 16)?;
        for candidate in candidates {
            if required_graph_file_path(&candidate)? == display.path.as_str()
                && !selected.iter().any(|existing: &CodeGraphSymbolSummaryV1| {
                    existing.occurrence == candidate.occurrence
                })
            {
                selected.push(candidate);
                break;
            }
        }
    }
    let seeds = selected
        .iter()
        .map(|symbol| symbol.occurrence.clone())
        .collect::<Vec<_>>();
    let mut related = Vec::new();
    if !seeds.is_empty() {
        let related_budget = context_related_relation_budget(max_nodes);
        for batches in [
            graph.callers_truncated(&seeds, &[], related_budget)?,
            graph.callees_truncated(&seeds, &[], related_budget)?,
        ] {
            for edge in batches.into_iter().flatten() {
                if !seeds.contains(&edge.neighbor.occurrence)
                    && !related.iter().any(|existing: &CodeGraphSymbolSummaryV1| {
                        existing.occurrence == edge.neighbor.occurrence
                    })
                {
                    related.push(edge.neighbor);
                }
            }
        }
    }
    related.truncate(max_nodes);

    let mut all_symbols = selected.clone();
    all_symbols.extend(related.iter().cloned());
    let touched_files = graph_symbol_paths(&all_symbols)?;
    let mut code_blocks = Vec::new();
    if include_code {
        // Context snippets are filesystem windows, not the mmap'd sealed
        // lexical artifact (that path serves n-gram search). Cache by path
        // so five symbols in one file do not re-read the whole source.
        let mut source_by_path = HashMap::<String, String>::new();
        for symbol in selected.iter().take(max_code_blocks) {
            let metadata = required_graph_metadata(symbol)?;
            let file_path = required_graph_file_path(symbol)?;
            if !source_by_path.contains_key(file_path) {
                source_by_path.insert(
                    file_path.to_owned(),
                    tracedecay_runtime_core::sync::read_source_file(
                        &ctx.project_root().join(file_path),
                    )?,
                );
            }
            let Some(source) = source_by_path.get(file_path) else {
                return Err(TraceDecayError::Config {
                    message: format!("context source window missing for '{file_path}'"),
                });
            };
            code_blocks.push(ContextCodeBlockV1 {
                node_id: symbol.occurrence.as_str().to_owned(),
                file: file_path.to_owned(),
                start_line: user_line(metadata.start_line),
                end_line: user_line(graph_symbol_end_line(metadata)?),
                code: crate::handlers::info::extract_lines(
                    source,
                    metadata.start_line,
                    graph_symbol_end_line(metadata)?,
                ),
            });
        }
    }
    Ok(ContextGraphProjection {
        selected,
        related,
        code_blocks,
        touched_files,
    })
}

fn append_context_search_matches(output: &mut String, matches: &[ContextSearchMatchV1]) {
    if matches.is_empty() {
        return;
    }
    output.push_str("\n### Available Code Search Matches\n");
    for search_match in matches {
        let _ = writeln!(
            output,
            "- **{}** ({}) — `{}` · rank {} · utility {}",
            search_match.name,
            search_match.kind,
            search_match.file,
            search_match.rank,
            search_match.utility_micros,
        );
    }
}

#[hotpath::measure(label = "mcp.graph.context.total")]
pub async fn handle_context<F>(
    ctx: &McpToolContext<'_>,
    graph: F,
    args: Value,
    scope_prefix: Option<&str>,
) -> Result<ToolResult>
where
    F: Future<Output = Result<tracedecay_graph_query::VerifiedGraphQuery>>,
{
    let search_executor = ctx.code_index_search_executor();
    let search_authority = ctx.code_index_search_authority();
    let deadline = ctx.deadline().cloned();
    let cancellation = ctx.cancellation().cloned();
    let request: ContextSurfaceRequestV1 = decode_primitive_request(&args, "tracedecay_context")?;
    let memory_policy_admitted_at =
        tracedecay_contracts::try_now_micros().map_err(|error| TraceDecayError::Config {
            message: format!("context memory policy admission clock unavailable: {error}"),
        })?;
    request
        .validate_memory_policy_at(memory_policy_admitted_at)
        .map_err(|error| TraceDecayError::Config {
            message: format!("invalid context memory policy: {error}"),
        })?;
    let task = request.task.as_str();
    let mode = request.mode.unwrap_or(ContextModeV1::Explore);
    let max_nodes = request
        .max_nodes
        .map_or(20, |value| value.clamp(1, 200) as usize);
    let include_code = request.include_code.unwrap_or(false);
    let max_code_blocks = request
        .max_code_blocks
        .map_or(5, |value| value.clamp(1, 20) as usize);
    let lexical_routing = lexical_routing::routing_from_parts(
        request.lexical_anchors.clone().unwrap_or_default(),
        request.prefer_symbol.unwrap_or(false),
    )?;
    let memory_options = context_memory_options(&request);
    let memory_read_control =
        context_memory_read_control(&memory_options, deadline.as_ref(), cancellation.as_ref())?;
    // Graph enrichment is optional unless the caller asks for source bodies.
    // That request waits for graph admission under the same deadline and
    // cancellation as search; ordinary lexical/exact retrieval stays independent.
    let search = execute_code_index_search(
        search_executor,
        tracedecay_query::code_search::CodeIndexSearchRequestV1 {
            project_root: ctx.project_root().to_path_buf(),
            query: task.to_owned(),
            source_revision: None,
            source_tree: None,
            source_reference: None,
            limit: max_nodes,
            cursor: None,
            lexical_routing,
            authority: search_authority.cloned(),
            deadline,
            cancellation,
        },
    );
    let memory = context_memory_outcome(
        &memory_options,
        memory_read_control.as_ref(),
        |read_control| context_memory_matches(ctx, task, &memory_options, read_control),
    );
    let search_and_graph = race_primary_search_with_graph(search, graph, false, None, include_code);
    let ((outcome, graph), memory_outcome) = tokio::join!(search_and_graph, memory);
    // Read after the search settles: the verdict must describe the scheduler
    // state at serve time, not a snapshot taken before the lanes ran.
    let freshness_payload = ctx.freshness().await;
    let worktree_freshness = worktree_freshness_from_payload(freshness_payload.as_ref());
    let (complete, code_generation, coverage, freshness, search_matches) = match outcome {
        tracedecay_query::code_search::CodeIndexSearchOutcomeV1::Complete(complete) => {
            let search_matches = context_search_matches(&complete, scope_prefix);
            let code_generation = Some(complete.code_generation.clone());
            let coverage = primitive_search_coverage(&complete.coverage);
            let freshness = search_freshness(
                ServedGenerationV1::Served(&complete.code_generation),
                &complete.coverage,
                &worktree_freshness,
            );
            (
                Some(complete),
                code_generation,
                coverage,
                freshness,
                search_matches,
            )
        }
        tracedecay_query::code_search::CodeIndexSearchOutcomeV1::Unavailable(unavailable) => (
            None,
            unavailable.code_generation,
            primitive_search_coverage(&unavailable.coverage),
            search_freshness(
                ServedGenerationV1::Unavailable {
                    reason: unavailable.reason.as_str(),
                },
                &unavailable.coverage,
                &worktree_freshness,
            ),
            Vec::new(),
        ),
    };
    let graph = match complete.as_ref() {
        Some(complete) => bind_verified_graph_to_search(graph, &complete.code_generation),
        None => graph,
    };
    let (graph, projection, verified_graph_evidence) = match (graph, complete.as_ref()) {
        (Ok(graph), Some(complete)) => match hotpath::measure_block!(
            "mcp.graph.context.graph",
            context_graph_projection(
                ctx,
                &graph,
                complete,
                scope_prefix,
                max_nodes,
                include_code,
                max_code_blocks,
            )
        ) {
            Ok(projection) => (Some(graph), projection, None),
            Err(error) => (
                None,
                ContextGraphProjection::default(),
                Some(dependency_hints::unavailable_evidence(&error)),
            ),
        },
        (Ok(graph), None) => (Some(graph), ContextGraphProjection::default(), None),
        (Err(error), _) => (
            None,
            ContextGraphProjection::default(),
            Some(dependency_hints::unavailable_evidence(&error)),
        ),
    };
    let ContextMemoryOutcome {
        hits: memory_matches,
        graph_coverage: memory_graph_coverage,
        temporal_coverage: memory_temporal_coverage,
        error: memory_matches_error,
    } = memory_outcome;
    let seeds = projection
        .selected
        .iter()
        .map(|symbol| symbol.occurrence.clone())
        .collect::<Vec<_>>();
    let symbol_values = projection
        .selected
        .iter()
        .map(primitive_symbol_location)
        .collect::<Result<Vec<_>>>()?;
    let related_values = projection
        .related
        .iter()
        .map(primitive_symbol_location)
        .collect::<Result<Vec<_>>>()?;
    let symbol_render_values = symbol_values
        .iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let related_render_values = related_values
        .iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let code_render_values = projection
        .code_blocks
        .iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut output = freshness_lines(&freshness);
    output.push_str(&verified_context_markdown(
        task,
        &symbol_render_values,
        &related_render_values,
        &code_render_values,
    )?);
    if symbol_values.is_empty() {
        append_context_search_matches(&mut output, &search_matches);
    }
    insert_context_memory_section(
        &mut output,
        &memory_matches,
        memory_matches_error.as_deref(),
        memory_temporal_coverage,
    );
    if mode == ContextModeV1::Plan
        && let Some(graph) = graph.as_ref()
    {
        append_verified_plan_context(graph, &projection.selected, &mut output)?;
    }

    if !seeds.is_empty() {
        let _ = write!(
            output,
            "\n{} {}\n",
            CONTEXT_SEEN_NODE_IDS_LABEL,
            serde_json::to_string(&seeds)?
        );
    }

    let memory_contribution = ContextMemoryContributionV1::from_matches(
        &request,
        &memory_matches,
        memory_graph_coverage,
        memory_temporal_coverage,
        memory_policy_admitted_at,
    )
    .map_err(|error| TraceDecayError::Config {
        message: format!("invalid canonical context memory contribution: {error}"),
    })?;
    let result = ContextResultV1 {
        task: request.task,
        mode,
        freshness,
        code_generation,
        search_matches: search_matches.clone(),
        symbols: symbol_values,
        related_symbols: related_values,
        code: projection.code_blocks,
        coverage,
        memory_matches: memory_matches.clone(),
        memory_graph_coverage,
        memory_temporal_coverage,
        memory_matches_error: memory_matches_error.clone(),
        verified_graph_evidence,
    };
    let mut value =
        hotpath::measure_block!("mcp.graph.context.serialize", serde_json::to_value(result)?);
    if let Some(object) = value.as_object_mut() {
        object.insert(
            CONTEXT_MEMORY_ANALYTICS_KEY.to_string(),
            json!({
                "context_memory": context_memory_analytics_value(
                    &memory_options,
                    &memory_matches,
                    memory_matches_error.as_deref()
                ),
            }),
        );
    }
    let mut degradation = Md::new();
    append_coverage_md(&mut degradation, &value);
    search_evidence::append_verified_graph_evidence_md(&mut degradation, &value);
    let degradation = degradation.render();
    if !degradation.is_empty() {
        output.push('\n');
        output.push_str(&degradation);
    }
    let touched_files = unique_file_paths(
        projection.touched_files.iter().map(String::as_str).chain(
            search_matches
                .iter()
                .map(|search_match| search_match.file.as_str()),
        ),
    );
    let preview = (!render::wants_json(&args)).then(|| context_markdown_lane_preview(&output));
    Ok(rendered_context_tool_result(
        ctx.project_root(),
        &args,
        value,
        touched_files,
        output,
        preview.as_deref(),
        memory_contribution,
    ))
}

/// Bare-name lookup against `idx_nodes_name` — no BM25 scoring, no fuzzy
/// match, no qualified-name suffix walk. Returns every node whose `name`
/// column equals the query exactly. Useful when you already know the symbol
/// and want the apples-to-apples cost of an index hit instead of
/// `tracedecay_search`'s ranked query.
#[hotpath::measure(label = "mcp.graph.find_exact_symbol.total")]
pub async fn handle_find_exact_symbol(
    ctx: &McpToolContext<'_>,
    graph: &tracedecay_graph_query::VerifiedGraphQuery,
    args: Value,
    scope_prefix: Option<&str>,
    ignored_dependency_admission: Option<
        &dyn tracedecay_application::code_index::CodeIndexIgnoredDependencyAdmissionPortV1,
    >,
) -> Result<ToolResult> {
    let name =
        args.get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| TraceDecayError::Config {
                message: "missing required parameter: name".to_string(),
            })?;
    let limit = args
        .get("limit")
        .and_then(serde_json::Value::as_u64)
        .map_or(20, |v| v.min(200) as usize);

    let mut nodes = hotpath::measure_block!("mcp.graph.find_exact_symbol.graph", {
        let nodes = graph.resolve_simple_name(name, None, limit.saturating_mul(4))?;
        graph_symbols_in_scope(nodes, scope_prefix)?
    });
    if nodes.is_empty() && dependency_hints::lazy_indexing_requested(&args) {
        hotpath::future!(
            dependency_hints::admit_verified_ignored_dependency(
                ctx,
                ignored_dependency_admission,
                graph,
                name,
                scope_prefix,
            ),
            label = "mcp.graph.find_exact_symbol.admit"
        )
        .await?;
    }
    if nodes.len() > limit {
        nodes.truncate(limit);
    }

    let touched_files = graph_symbol_paths(&nodes)?;
    let items = nodes
        .iter()
        .map(|node| {
            let metadata = required_graph_metadata(node)?;
            let file_path = required_graph_file_path(node)?;
            Ok(json!({
                "id": node.occurrence.as_str(),
                "name": metadata.simple_name,
                "qualified_name": metadata.qualified_name,
                "kind": metadata.kind,
                "file": file_path,
                "line": user_line(metadata.start_line),
                "signature": metadata.signature,
            }))
        })
        .collect::<Result<Vec<_>>>()?;

    let body = hotpath::measure_block!(
        "mcp.graph.find_exact_symbol.serialize",
        json!({
            "name": name,
            "count": items.len(),
            "matches": items,
        })
    );
    Ok(generic_tool_result(ctx, &args, &body, touched_files))
}

#[hotpath::measure(label = "mcp.graph.similar.total")]
pub async fn handle_similar(ctx: &McpToolContext<'_>, args: Value) -> Result<ToolResult> {
    let request: SimilarSurfaceRequestV1 = decode_primitive_request(&args, "tracedecay_similar")?;
    if request.project_id != ctx.admitted_scope().project_id
        || request.repository_id != ctx.admitted_scope().repository_id
    {
        return Err(TraceDecayError::ProjectRoute {
            reason_code: "similar-repository-not-authorized".to_owned(),
            retryable: false,
            detail: "the selected repository is outside the authorized repository scope".to_owned(),
        });
    }
    let source_extent =
        request
            .validated_source_extent()
            .map_err(|error| TraceDecayError::Config {
                message: format!("invalid arguments for tracedecay_similar: {error}"),
            })?;
    let project_id = request.project_id;
    let repository_id = request.repository_id;
    let target = match request.target {
        SimilarTargetV1::SymbolOccurrence {
            symbol_occurrence_id,
        } => tracedecay_query::code_search::CodeIndexSimilarTargetV1::SymbolOccurrence(
            symbol_occurrence_id,
        ),
        SimilarTargetV1::SourceRange { path, span } => {
            tracedecay_query::code_search::CodeIndexSimilarTargetV1::SourceRange { path, span }
        }
    };
    let match_classes = request
        .match_classes
        .iter()
        .map(|class| match class {
            SimilarMatchClassV1::ConservativeExact => {
                tracedecay_code_index::clones::CloneNormalizationClassV1::Conservative
            }
            SimilarMatchClassV1::RenameNormalizedExact => {
                tracedecay_code_index::clones::CloneNormalizationClassV1::Rename
            }
        })
        .collect();
    let executor =
        ctx.code_index_similar_executor()
            .ok_or_else(|| TraceDecayError::ProjectRoute {
                reason_code: "verified-code-similarity-unavailable".to_owned(),
                retryable: false,
                detail: "the maintained clone similarity lane is unavailable".to_owned(),
            })?;
    let similar = match executor(tracedecay_query::code_search::CodeIndexSimilarRequestV1 {
        project_root: ctx.project_root().to_path_buf(),
        target,
        source_extent: code_index_source_extent(&source_extent)?,
        match_classes,
        result_limit: request.result_limit as usize,
        work_limit: request.work_limit as usize,
        cursor: request.cursor,
        authority: ctx.code_index_search_authority().cloned(),
        deadline: ctx.deadline().cloned(),
        cancellation: ctx.cancellation().cloned(),
    })
    .await
    {
        tracedecay_query::code_search::CodeIndexSimilarOutcomeV1::Complete(similar) => *similar,
        tracedecay_query::code_search::CodeIndexSimilarOutcomeV1::NotFound => {
            return Err(TraceDecayError::ProjectRoute {
                reason_code: "similar-source-not-found".to_owned(),
                retryable: false,
                detail: "the selected source has no body in the verified clone index".to_owned(),
            });
        }
        tracedecay_query::code_search::CodeIndexSimilarOutcomeV1::Unavailable(reason) => {
            return Err(similar_unavailable_error(reason));
        }
    };
    if similar.source.occurrence.project_id != project_id
        || similar.source.occurrence.repository_id != repository_id
    {
        return Err(TraceDecayError::ProjectRoute {
            reason_code: "similar-source-not-found".to_owned(),
            retryable: false,
            detail: "the selected source is outside the authorized repository scope".to_owned(),
        });
    }
    let source = similar_occurrence(&similar.source.occurrence);
    let mut touched_files = vec![source.path.clone()];
    let mut complete = true;
    let mut remaining_wire_occurrences = request.result_limit as usize;
    let mut wire_limit_reached = false;
    let families = similar
        .exact_groups
        .into_iter()
        .map(|group| -> Result<SimilarFamilyV1> {
            complete &= group.complete;
            let match_class = match group.key.class {
                tracedecay_code_index::clones::CloneNormalizationClassV1::Conservative => {
                    SimilarMatchClassV1::ConservativeExact
                }
                tracedecay_code_index::clones::CloneNormalizationClassV1::Rename => {
                    SimilarMatchClassV1::RenameNormalizedExact
                }
            };
            let mut members = Vec::new();
            let mut group_members_omitted = false;
            for member in group.members {
                if member.occurrence.project_id != project_id
                    || member.occurrence.repository_id != repository_id
                {
                    continue;
                }
                if remaining_wire_occurrences == 0 {
                    group_members_omitted = true;
                    wire_limit_reached = true;
                    continue;
                }
                let occurrence = similar_occurrence(&member.occurrence);
                touched_files.push(occurrence.path.clone());
                members.push(occurrence);
                remaining_wire_occurrences = remaining_wire_occurrences.saturating_sub(1);
            }
            if group_members_omitted {
                complete = false;
            }
            Ok(SimilarFamilyV1 {
                match_class,
                normalization_revision: group.key.normalization_revision,
                family_digest: group.key.digest,
                representative_payload_digest: similar.source.payload.payload_digest.clone(),
                member_count: members.len(),
                members,
                complete: group.complete && !group_members_omitted,
                next_cursor: group.next_cursor,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let (near, near_touched_files, near_wire_limit_reached) = similar_near_result(
        &similar.source,
        source_extent,
        similar.near,
        project_id.clone(),
        repository_id.clone(),
        &mut remaining_wire_occurrences,
    )?;
    wire_limit_reached |= near_wire_limit_reached;
    complete &= matches!(near.coverage, SimilarNearCoverageV1::Complete);
    touched_files.extend(near_touched_files);
    touched_files.sort();
    touched_files.dedup();
    let coverage = match similar.source.occurrence.eligibility {
        tracedecay_code_index::clones::CloneBodyEligibilityV1::Eligible
            if complete && !wire_limit_reached =>
        {
            SimilarCoverageV1::Complete
        }
        tracedecay_code_index::clones::CloneBodyEligibilityV1::Eligible => {
            SimilarCoverageV1::Partial
        }
        tracedecay_code_index::clones::CloneBodyEligibilityV1::ExcludedTooSmall {
            minimum_tokens,
        } => SimilarCoverageV1::ExcludedTooSmall { minimum_tokens },
        tracedecay_code_index::clones::CloneBodyEligibilityV1::ExcludedIncompleteTokenization => {
            SimilarCoverageV1::ExcludedIncompleteTokenization
        }
    };
    let result = SimilarResultV1 {
        source_generation: source.source_generation.clone(),
        source,
        families,
        coverage,
        near: Some(near),
    };
    let value =
        hotpath::measure_block!("mcp.graph.similar.serialize", serde_json::to_value(result)?);
    Ok(generic_tool_result(ctx, &args, &value, touched_files))
}

fn code_index_source_extent(
    extent: &SimilarSourceExtentV1,
) -> Result<tracedecay_query::code_search::CodeIndexSimilarSourceExtentV1> {
    Ok(match extent {
        SimilarSourceExtentV1::WholeBody => {
            tracedecay_query::code_search::CodeIndexSimilarSourceExtentV1::WholeBody
        }
        SimilarSourceExtentV1::SelectedTokenRange { start, end } => {
            tracedecay_query::code_search::CodeIndexSimilarSourceExtentV1::SelectedTokenRange {
                start: usize::try_from(*start).map_err(|_| TraceDecayError::Config {
                    message: "invalid arguments for tracedecay_similar: source token range start is too large"
                        .to_owned(),
                })?,
                end: usize::try_from(*end).map_err(|_| TraceDecayError::Config {
                    message: "invalid arguments for tracedecay_similar: source token range end is too large"
                        .to_owned(),
                })?,
            }
        }
    })
}

fn similar_near_result(
    source: &tracedecay_code_index::clones::CodeIndexCloneBodyV1,
    extent: SimilarSourceExtentV1,
    near: tracedecay_query::code_search::CodeIndexSimilarNearReadV1,
    project_id: tracedecay_domain::ProjectId,
    repository_id: tracedecay_domain::RepositoryId,
    remaining_wire_occurrences: &mut usize,
) -> Result<(SimilarNearResultV1, Vec<String>, bool)> {
    let mut touched_files = Vec::new();
    let mut matches = Vec::new();
    let mut wire_limit_reached = false;
    let coverage;
    let next_cursor;
    match near {
        tracedecay_query::code_search::CodeIndexSimilarNearReadV1::WholeBody(read) => {
            if !matches!(&extent, SimilarSourceExtentV1::WholeBody) {
                return Err(TraceDecayError::Config {
                    message: "verified similar near read extent does not match its request"
                        .to_owned(),
                });
            }
            coverage = similar_near_coverage(
                read.source_eligibility,
                &read.partial_reasons,
                read.coverage.unknown > 0 || read.coverage.capped > 0,
            );
            next_cursor = read.page.next_cursor;
            for pair in read.page.members {
                let match_class = similar_match_class(pair.class);
                let alignment = SimilarAlignmentV1 {
                    shared_token_count: pair.shared_ordered_token_count,
                    anchors: pair
                        .ordered_anchors
                        .into_iter()
                        .map(|anchor| similar_alignment_anchor(anchor, 0))
                        .collect(),
                };
                let differences = pair
                    .differences
                    .into_iter()
                    .map(similar_difference)
                    .collect::<Vec<_>>();
                for occurrence in pair.occurrences {
                    if occurrence.project_id != project_id
                        || occurrence.repository_id != repository_id
                    {
                        continue;
                    }
                    if *remaining_wire_occurrences == 0 {
                        wire_limit_reached = true;
                        continue;
                    }
                    touched_files.push(occurrence.path.clone());
                    matches.push(SimilarNearMatchV1 {
                        candidate: similar_occurrence(&occurrence),
                        match_class,
                        extent: SimilarSourceExtentV1::WholeBody,
                        source_coverage_millionths: pair.source_coverage_millionths,
                        candidate_coverage_millionths: pair.candidate_coverage_millionths,
                        alignment: alignment.clone(),
                        differences: differences.clone(),
                        containment: None,
                    });
                    *remaining_wire_occurrences = (*remaining_wire_occurrences).saturating_sub(1);
                }
            }
        }
        tracedecay_query::code_search::CodeIndexSimilarNearReadV1::SelectedTokenRange(read) => {
            coverage = similar_near_coverage(
                source.occurrence.eligibility,
                &read.partial_reasons,
                read.coverage.unknown > 0 || read.coverage.capped > 0,
            );
            next_cursor = read.page.next_cursor;
            let (source_offset, selected_token_count) = match &extent {
                SimilarSourceExtentV1::WholeBody => {
                    return Err(TraceDecayError::Config {
                        message: "verified selected similar near read has no selected token range"
                            .to_owned(),
                    });
                }
                SimilarSourceExtentV1::SelectedTokenRange { start, end } => {
                    (*start, u64::from(end.saturating_sub(*start)))
                }
            };
            let match_class = similar_match_class(read.stream.class);
            for candidate in read.page.members {
                let containment = similar_containment(candidate.containment);
                let candidate_token_count = u64::from(candidate.payload.token_count);
                let shared_token_count = match containment {
                    SimilarContainmentV1::SelectedRangeContainsCandidate => {
                        selected_token_count.min(candidate_token_count)
                    }
                    SimilarContainmentV1::Equal
                    | SimilarContainmentV1::CandidateContainsSelectedRange => {
                        candidate_token_count.min(selected_token_count)
                    }
                };
                let alignment = SimilarAlignmentV1 {
                    shared_token_count: u32::try_from(shared_token_count).unwrap_or(u32::MAX),
                    anchors: candidate
                        .anchors
                        .iter()
                        .copied()
                        .map(|anchor| similar_alignment_anchor(anchor, source_offset))
                        .collect(),
                };
                for occurrence in candidate.occurrences {
                    if occurrence.project_id != project_id
                        || occurrence.repository_id != repository_id
                    {
                        continue;
                    }
                    if *remaining_wire_occurrences == 0 {
                        wire_limit_reached = true;
                        continue;
                    }
                    touched_files.push(occurrence.path.clone());
                    matches.push(SimilarNearMatchV1 {
                        candidate: similar_occurrence(&occurrence),
                        match_class,
                        extent: extent.clone(),
                        source_coverage_millionths: directional_coverage(
                            shared_token_count,
                            selected_token_count,
                        ),
                        candidate_coverage_millionths: directional_coverage(
                            shared_token_count,
                            candidate_token_count,
                        ),
                        alignment: alignment.clone(),
                        differences: Vec::new(),
                        containment: Some(containment),
                    });
                    *remaining_wire_occurrences = (*remaining_wire_occurrences).saturating_sub(1);
                }
            }
        }
        tracedecay_query::code_search::CodeIndexSimilarNearReadV1::Unavailable(reason) => {
            coverage = SimilarNearCoverageV1::Unavailable {
                reason: similar_unavailable_reason(reason),
            };
            next_cursor = None;
        }
    }
    touched_files.sort();
    touched_files.dedup();
    Ok((
        SimilarNearResultV1 {
            extent,
            matches,
            coverage,
            next_cursor,
        },
        touched_files,
        wire_limit_reached,
    ))
}

fn similar_near_coverage(
    eligibility: tracedecay_code_index::clones::CloneBodyEligibilityV1,
    partial_reasons: &[tracedecay_query::retrieval::lexical::CloneFingerprintPartialReasonV1],
    interrupted_or_capped: bool,
) -> SimilarNearCoverageV1 {
    match eligibility {
        tracedecay_code_index::clones::CloneBodyEligibilityV1::ExcludedTooSmall {
            minimum_tokens,
        } => SimilarNearCoverageV1::ExcludedTooSmall { minimum_tokens },
        tracedecay_code_index::clones::CloneBodyEligibilityV1::ExcludedIncompleteTokenization => {
            SimilarNearCoverageV1::ExcludedIncompleteTokenization
        }
        tracedecay_code_index::clones::CloneBodyEligibilityV1::Eligible
            if partial_reasons.is_empty() && !interrupted_or_capped =>
        {
            SimilarNearCoverageV1::Complete
        }
        tracedecay_code_index::clones::CloneBodyEligibilityV1::Eligible => {
            let mut reasons = partial_reasons
                .iter()
                .copied()
                .map(similar_partial_reason)
                .collect::<Vec<_>>();
            if reasons.is_empty() {
                reasons.push(SimilarNearPartialReasonV1::VerificationWorkBudget);
            }
            reasons.sort_by_key(|reason| *reason as u8);
            reasons.dedup();
            SimilarNearCoverageV1::Partial { reasons }
        }
    }
}

fn similar_match_class(
    class: tracedecay_code_index::clones::CloneNormalizationClassV1,
) -> SimilarMatchClassV1 {
    match class {
        tracedecay_code_index::clones::CloneNormalizationClassV1::Conservative => {
            SimilarMatchClassV1::ConservativeExact
        }
        tracedecay_code_index::clones::CloneNormalizationClassV1::Rename => {
            SimilarMatchClassV1::RenameNormalizedExact
        }
    }
}

fn similar_alignment_anchor(
    anchor: tracedecay_code_index::clones::CloneTokenAnchorV1,
    source_offset: u32,
) -> SimilarAlignmentAnchorV1 {
    SimilarAlignmentAnchorV1 {
        fingerprint: anchor.fingerprint,
        source_token_position: anchor.left_token_position.saturating_add(source_offset),
        candidate_token_position: anchor.right_token_position,
    }
}

fn similar_difference(
    difference: tracedecay_code_index::clones::CloneAlignedDifferenceV1,
) -> SimilarAlignedDifferenceV1 {
    SimilarAlignedDifferenceV1 {
        source_span: SimilarTokenSpanV1 {
            start: difference.left_span.start,
            end: difference.left_span.end,
        },
        candidate_span: SimilarTokenSpanV1 {
            start: difference.right_span.start,
            end: difference.right_span.end,
        },
        source_token_count: difference.left_tokens.len().min(u32::MAX as usize) as u32,
        candidate_token_count: difference.right_tokens.len().min(u32::MAX as usize) as u32,
    }
}

fn similar_containment(
    containment: tracedecay_query::retrieval::lexical::CloneSelectedBlockContainmentClassV1,
) -> SimilarContainmentV1 {
    match containment {
        tracedecay_query::retrieval::lexical::CloneSelectedBlockContainmentClassV1::Equal => {
            SimilarContainmentV1::Equal
        }
        tracedecay_query::retrieval::lexical::CloneSelectedBlockContainmentClassV1::CandidateContainsSelectedBlock => {
            SimilarContainmentV1::CandidateContainsSelectedRange
        }
        tracedecay_query::retrieval::lexical::CloneSelectedBlockContainmentClassV1::SelectedBlockContainsCandidate => {
            SimilarContainmentV1::SelectedRangeContainsCandidate
        }
    }
}

fn similar_partial_reason(
    reason: tracedecay_query::retrieval::lexical::CloneFingerprintPartialReasonV1,
) -> SimilarNearPartialReasonV1 {
    match reason {
        tracedecay_query::retrieval::lexical::CloneFingerprintPartialReasonV1::PostingRowBudget => {
            SimilarNearPartialReasonV1::PostingRowBudget
        }
        tracedecay_query::retrieval::lexical::CloneFingerprintPartialReasonV1::CandidateBodyBudget => {
            SimilarNearPartialReasonV1::CandidateBodyBudget
        }
        tracedecay_query::retrieval::lexical::CloneFingerprintPartialReasonV1::HotPostings => {
            SimilarNearPartialReasonV1::HotPostings
        }
        tracedecay_query::retrieval::lexical::CloneFingerprintPartialReasonV1::VerificationBodyBudget => {
            SimilarNearPartialReasonV1::VerificationBodyBudget
        }
        tracedecay_query::retrieval::lexical::CloneFingerprintPartialReasonV1::VerificationWorkBudget => {
            SimilarNearPartialReasonV1::VerificationWorkBudget
        }
        tracedecay_query::retrieval::lexical::CloneFingerprintPartialReasonV1::Cancelled => {
            SimilarNearPartialReasonV1::Cancelled
        }
        tracedecay_query::retrieval::lexical::CloneFingerprintPartialReasonV1::DeadlineExceeded => {
            SimilarNearPartialReasonV1::DeadlineExceeded
        }
    }
}

fn similar_unavailable_reason(
    reason: tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1,
) -> SimilarNearUnavailableReasonV1 {
    match reason {
        tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::CapabilityUnavailable => {
            SimilarNearUnavailableReasonV1::CapabilityUnavailable
        }
        tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::AuthorityUnavailable => {
            SimilarNearUnavailableReasonV1::AuthorityUnavailable
        }
        tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::LinkedWorktreeDisabled => {
            SimilarNearUnavailableReasonV1::LinkedWorktreeDisabled
        }
        tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::Cancelled => {
            SimilarNearUnavailableReasonV1::Cancelled
        }
        tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::TimedOut => {
            SimilarNearUnavailableReasonV1::TimedOut
        }
        tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::CapacityUnavailable => {
            SimilarNearUnavailableReasonV1::CapacityUnavailable
        }
        tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::GenerationUnavailable => {
            SimilarNearUnavailableReasonV1::GenerationUnavailable
        }
        tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::GenerationUnverified => {
            SimilarNearUnavailableReasonV1::GenerationUnverified
        }
        tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::InvalidRequest => {
            SimilarNearUnavailableReasonV1::InvalidRequest
        }
        tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::CorruptionResetRequired => {
            SimilarNearUnavailableReasonV1::CorruptionResetRequired
        }
        tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::Internal => {
            SimilarNearUnavailableReasonV1::Internal
        }
    }
}

fn directional_coverage(shared: u64, total: u64) -> u32 {
    if total == 0 {
        return 0;
    }
    shared
        .saturating_mul(1_000_000)
        .checked_div(total)
        .unwrap_or_default()
        .min(1_000_000) as u32
}

fn similar_unavailable_error(
    reason: tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1,
) -> TraceDecayError {
    TraceDecayError::ProjectRoute {
        reason_code: reason.as_str().to_owned(),
        retryable: matches!(
            reason,
            tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::Cancelled
                | tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::TimedOut
                | tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::CapacityUnavailable
                | tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::GenerationUnavailable
                | tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::GenerationUnverified
        ),
        detail: format!(
            "the maintained clone similarity lane is unavailable: {}",
            reason.as_str()
        ),
    }
}

#[hotpath::measure(label = "mcp.graph.redundancy.total")]
pub async fn handle_redundancy(ctx: &McpToolContext<'_>, args: Value) -> Result<ToolResult> {
    let request: RedundancySurfaceRequestV1 =
        decode_primitive_request(&args, "tracedecay_redundancy")?;
    if request.project_id != ctx.admitted_scope().project_id
        || request.repository_id != ctx.admitted_scope().repository_id
    {
        return Err(TraceDecayError::ProjectRoute {
            reason_code: "redundancy-repository-not-authorized".to_owned(),
            retryable: false,
            detail: "the selected repository is outside the authorized repository scope".to_owned(),
        });
    }
    let match_classes = request
        .match_classes
        .iter()
        .map(|class| match class {
            SimilarMatchClassV1::ConservativeExact => {
                tracedecay_code_index::clones::CloneNormalizationClassV1::Conservative
            }
            SimilarMatchClassV1::RenameNormalizedExact => {
                tracedecay_code_index::clones::CloneNormalizationClassV1::Rename
            }
        })
        .collect();
    let scope = match request.scope {
        RedundancyScopeV1::Repository => {
            tracedecay_query::code_search::CodeIndexRedundancyScopeV1::Repository
        }
        RedundancyScopeV1::Path { path } => {
            tracedecay_query::code_search::CodeIndexRedundancyScopeV1::Path(path)
        }
        RedundancyScopeV1::PullRequest {
            provider,
            pull_request_id,
            head_commit_id,
            mut changed_paths,
        } => {
            changed_paths.sort();
            changed_paths.dedup();
            let pull_request_id = tracedecay_domain::feedback::GitHubPullRequestIdV1::new(
                pull_request_id,
            )
            .map_err(|error| TraceDecayError::Config {
                message: format!("invalid arguments for tracedecay_redundancy: {error}"),
            })?;
            tracedecay_query::code_search::CodeIndexRedundancyScopeV1::PullRequest {
                provider,
                pull_request_id,
                head_commit_id,
                changed_paths,
            }
        }
    };
    let executor =
        ctx.code_index_redundancy_executor()
            .ok_or_else(|| TraceDecayError::ProjectRoute {
                reason_code: "verified-code-redundancy-unavailable".to_owned(),
                retryable: false,
                detail: "the maintained clone family lane is unavailable".to_owned(),
            })?;
    let outcome = executor(tracedecay_query::code_search::CodeIndexRedundancyQueryV1 {
        project_root: ctx.project_root().to_path_buf(),
        project_id: request.project_id,
        repository_id: request.repository_id,
        match_classes,
        scope,
        include_generated_paths: request.include_generated_paths,
        family_limit: request.family_limit as usize,
        member_limit: request.member_limit as usize,
        work_limit: request.work_limit as usize,
        cursor: request.cursor,
        authority: ctx.code_index_search_authority().cloned(),
        deadline: ctx.deadline().cloned(),
        cancellation: ctx.cancellation().cloned(),
    })
    .await
    .map_err(|reason| TraceDecayError::ProjectRoute {
                reason_code: "verified-code-redundancy-unavailable".to_owned(),
                retryable: matches!(
                    reason,
                    tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::Cancelled
                        | tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::TimedOut
                        | tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::CapacityUnavailable
                ),
                detail: format!("the maintained clone family lane is unavailable: {}", reason.as_str()),
            })?;
    let mut touched_files = outcome
        .families
        .iter()
        .flat_map(|group| group.family.members.iter())
        .map(|member| member.path.clone())
        .collect::<Vec<_>>();
    touched_files.sort();
    touched_files.dedup();
    let value = hotpath::measure_block!(
        "mcp.graph.redundancy.serialize",
        serde_json::to_value(outcome)?
    );
    Ok(generic_tool_result(ctx, &args, &value, touched_files))
}

fn similar_occurrence(
    occurrence: &tracedecay_code_index::clones::CloneBodyOccurrenceV1,
) -> SimilarOccurrenceV1 {
    SimilarOccurrenceV1 {
        project_id: occurrence.project_id.clone(),
        repository_id: occurrence.repository_id.clone(),
        worktree_id: occurrence.worktree_id.clone(),
        source_generation: occurrence.source_generation.clone(),
        snapshot_digest: occurrence.snapshot_digest.clone(),
        symbol_occurrence_id: occurrence.symbol_occurrence_id.clone(),
        path: occurrence.path.clone(),
        body_span: occurrence.body_span,
    }
}

/// Reads a file's lines (0-based) for snippet extraction, memoizing by path so
/// a file with many references is read once. `None` when the file cannot be
/// read (e.g. deleted since indexing).
fn cached_file_lines<'a>(
    project_root: &Path,
    cache: &'a mut HashMap<String, Option<Vec<String>>>,
    file_path: &str,
) -> Option<&'a [String]> {
    if !cache.contains_key(file_path) {
        let abs = project_root.join(file_path);
        let lines = std::fs::read_to_string(&abs)
            .ok()
            .map(|source| source.lines().map(str::to_string).collect::<Vec<_>>());
        cache.insert(file_path.to_string(), lines);
    }
    cache
        .get(file_path)
        .and_then(Option::as_ref)
        .map(Vec::as_slice)
}

/// Trims and length-caps a source line for use as a preview snippet.
fn snippet_text(line: &str) -> String {
    tracedecay_runtime_core::text::utf8_prefix_at_or_before(line.trim(), 160).to_string()
}

/// Picks a current-text snippet near `approx_line` (0-based; edge line bases are
/// approximate, so neighbors are tried) that actually contains `name`, falling
/// back to the line itself. `None` when no line is available.
fn reference_line_snippet(
    lines: &[String],
    approx_line: Option<u32>,
    name: &str,
) -> Option<String> {
    let approx = approx_line? as usize;
    let candidates = [approx, approx.saturating_sub(1), approx + 1];
    let idx = candidates
        .into_iter()
        .find(|&i| lines.get(i).is_some_and(|line| line.contains(name)))
        .unwrap_or(approx);
    lines.get(idx).map(|line| snippet_text(line))
}

/// True for bytes that can appear inside an identifier. Non-ASCII bytes count so
/// multi-byte unicode identifiers are not falsely split at a boundary.
fn is_ident_byte(b: u8) -> bool {
    b == b'_' || b.is_ascii_alphanumeric() || b >= 0x80
}

/// Counts occurrences of `name` in `haystack` bounded as a whole identifier
/// (neither neighbouring byte is an identifier byte). Used to estimate the
/// literal textual matches a rename would touch, independent of the graph.
fn count_identifier_occurrences(haystack: &str, name: &str) -> usize {
    if name.is_empty() {
        return 0;
    }
    let bytes = haystack.as_bytes();
    let name_len = name.len();
    let mut count = 0;
    let mut start = 0;
    while let Some(pos) = haystack[start..].find(name) {
        let abs = start + pos;
        let before_ok = abs == 0 || !is_ident_byte(bytes[abs - 1]);
        let after_idx = abs + name_len;
        let after_ok = after_idx >= bytes.len() || !is_ident_byte(bytes[after_idx]);
        if before_ok && after_ok {
            count += 1;
        }
        start = abs + name_len;
    }
    count
}

/// Graph-derived inputs for one rename-preview reference site, extracted
/// before the blocking file walk so the worker needs no graph access.
struct RenameReferenceSiteInput {
    from_node_id: String,
    from_name: String,
    from_kind: String,
    edge_kind: String,
    file: String,
    evidence_start_byte: u64,
}

/// READ-ONLY: reports what a rename of the given symbol WOULD touch — the
/// declaration site and every graph reference site (incoming edges; outgoing
/// edges reference other symbols and so are excluded), each with a
/// current-text snippet, plus a per-file count of literal name occurrences
/// that are NOT backed by a graph edge ("text-only matches — review
/// manually"). Nothing is rewritten.
#[hotpath::measure(label = "mcp.graph.rename_preview.total")]
pub async fn handle_rename_preview(
    ctx: &McpToolContext<'_>,
    graph: &tracedecay_graph_query::VerifiedGraphQuery,
    args: Value,
) -> Result<ToolResult> {
    let request: RenamePreviewPrimitiveRequestV1 =
        decode_primitive_request(&args, "tracedecay_rename_preview")?;

    let occurrence = graph_occurrence_id(&request.node_id)?;
    // Graph occurrences per file (declaration + reference sites) — subtracted
    // from the literal textual count to isolate the text-only matches.
    let mut graph_counts: HashMap<String, usize> = HashMap::new();
    let mut touched: Vec<String> = Vec::new();
    // Graph phase: extract owned declaration fields and per-reference-site
    // inputs so the blocking file walk below needs no graph value at all.
    let (mut declaration, declaration_line, symbol_name, reference_inputs) =
        hotpath::measure_block!("mcp.graph.rename_preview.graph", {
            let Some(node) = graph.symbol_summary(&occurrence)? else {
                return node_not_found_result(&request.node_id);
            };
            let node_metadata = required_graph_metadata(&node)?;
            let node_file = required_graph_file_path(&node)?;
            let symbol_name = node_metadata.simple_name.clone();

            touched.push(node_file.to_owned());
            *graph_counts.entry(node_file.to_owned()).or_default() += 1;
            let declaration = RenamePreviewNodeV1 {
                id: node.occurrence.as_str().to_owned(),
                name: node_metadata.simple_name.clone(),
                qualified_name: node_metadata.qualified_name.clone(),
                kind: node_metadata.kind.clone(),
                file: node_file.to_owned(),
                line: user_line(node_metadata.start_line),
                snippet: None,
            };

            // Reference sites: incoming edges are the callers/users that name this
            // symbol. NOTE: call-edge coverage improves as the resolver improves;
            // the text-only counts below catch what the graph currently misses.
            let incoming = single_graph_adjacency_batch(graph.callers(
                std::slice::from_ref(&node.occurrence),
                &[],
                2_000_000,
            )?)?;
            let mut reference_inputs =
                Vec::<RenameReferenceSiteInput>::with_capacity(incoming.len());
            for edge in incoming {
                let source_node = edge.neighbor;
                let source_metadata = required_graph_metadata(&source_node)?;
                let source_file = required_graph_file_path(&source_node)?;
                touched.push(source_file.to_owned());
                *graph_counts.entry(source_file.to_owned()).or_default() += 1;
                reference_inputs.push(RenameReferenceSiteInput {
                    from_node_id: source_node.occurrence.as_str().to_owned(),
                    from_name: source_metadata.simple_name.clone(),
                    from_kind: source_metadata.kind.clone(),
                    edge_kind: edge.edge.kind.as_str().to_owned(),
                    file: source_file.to_owned(),
                    evidence_start_byte: edge.edge.evidence_span.start_byte,
                });
            }
            (
                declaration,
                node_metadata.start_line,
                symbol_name,
                reference_inputs,
            )
        });

    let touched_files = unique_file_paths(touched.iter().map(std::string::String::as_str));

    // File-walk phase: every referenced source file is read from disk, so it
    // runs on a blocking worker like the sibling analysis scans instead of
    // holding the async dispatch thread through the reads.
    let project_root = ctx.project_root().to_path_buf();
    let declaration_file = declaration.file.clone();
    let walk_symbol_name = symbol_name.clone();
    let walk_graph_counts = graph_counts;
    let walk_touched_files = touched_files.clone();
    let (decl_snippet, references, text_only_matches) = hotpath::future!(
        tokio::task::spawn_blocking(
        move || -> Result<(
            Option<String>,
            Vec<RenamePreviewReferenceV1>,
            Vec<RenamePreviewTextOnlyMatchV1>,
        )> {
            let mut lines_cache: HashMap<String, Option<Vec<String>>> = HashMap::new();
            let decl_snippet =
                cached_file_lines(&project_root, &mut lines_cache, &declaration_file).and_then(
                    |lines| {
                        lines
                            .get(declaration_line as usize)
                            .map(|line| snippet_text(line))
                    },
                );

            let mut references =
                Vec::<RenamePreviewReferenceV1>::with_capacity(reference_inputs.len());
            for input in reference_inputs {
                let source = tracedecay_runtime_core::sync::read_source_file(&project_root.join(&input.file))?;
                let line = line_for_byte_offset(&source, input.evidence_start_byte)?;
                let snippet = cached_file_lines(&project_root, &mut lines_cache, &input.file)
                    .and_then(|lines| reference_line_snippet(lines, Some(line), &walk_symbol_name));
                references.push(RenamePreviewReferenceV1 {
                    from_node_id: input.from_node_id,
                    from_name: input.from_name,
                    from_kind: input.from_kind,
                    edge_kind: input.edge_kind,
                    file: input.file,
                    line: user_line(line),
                    snippet,
                });
            }

            // Text-only matches per touched file: literal identifier occurrences
            // of the name minus the graph occurrences already accounted for.
            // These are the comments/strings/dynamic-dispatch/unresolved sites a
            // graph-only rename would miss — the scan is bounded to files that
            // already appear in the preview, so occurrences in wholly unrelated
            // files are not counted.
            let mut text_only_matches = Vec::<RenamePreviewTextOnlyMatchV1>::new();
            for file in &walk_touched_files {
                let total =
                    cached_file_lines(&project_root, &mut lines_cache, file).map_or(0, |lines| {
                        lines
                            .iter()
                            .map(|line| count_identifier_occurrences(line, &walk_symbol_name))
                            .sum::<usize>()
                    });
                let graph = walk_graph_counts.get(file).copied().unwrap_or(0);
                let text_only = total.saturating_sub(graph);
                if text_only > 0 {
                    text_only_matches.push(RenamePreviewTextOnlyMatchV1 {
                        file: file.clone(),
                        text_only_count: text_only,
                        note: "text-only matches — review manually".to_owned(),
                    });
                }
            }
            Ok((decl_snippet, references, text_only_matches))
        }
        ),
        label = "mcp.graph.rename_preview.walk"
    )
    .await
    .map_err(|join_error| TraceDecayError::Config {
        message: format!("rename preview file scan task failed: {join_error}"),
    })??;
    declaration.snippet = decl_snippet;

    let output = hotpath::measure_block!(
        "mcp.graph.rename_preview.serialize",
        serde_json::to_value(RenamePreviewPrimitiveResultV1 {
            read_only: true,
            note: "Preview only — nothing is edited. 'references' are graph reference sites \
               (the declaration is reported separately in 'node'); 'text_only_matches' are \
               literal name occurrences NOT backed by a graph edge (comments, strings, \
               dynamic dispatch, unresolved refs) and must be reviewed by hand. Graph \
               call-edge coverage improves as the resolver does."
                .to_owned(),
            symbol: symbol_name,
            new_name: request.new_name,
            node: declaration,
            reference_count: references.len(),
            references,
            text_only_matches,
        })?
    );

    Ok(generic_tool_result(ctx, &args, &output, touched_files))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracedecay_contracts::memory::FactSearchHitV1;

    #[test]
    fn complete_search_preserves_generation_advance_but_not_stale_admission() {
        let advanced = TraceDecayError::project_route(
            IGNORED_DEPENDENCY_GENERATION_ADVANCED,
            true,
            "new generation published",
        );
        assert!(preserve_complete_search_after_lazy_admission(Err(advanced)).is_ok());

        let stale = TraceDecayError::project_route(
            "application.symbol-graph.ignored-dependency-generation-stale",
            true,
            "source generation is stale",
        );
        let error = preserve_complete_search_after_lazy_admission(Err(stale))
            .expect_err("stale admission must remain a typed retrieval failure");
        assert!(matches!(
            error.project_route_context(),
            Some((
                "application.symbol-graph.ignored-dependency-generation-stale",
                true,
                _
            ))
        ));
    }

    #[test]
    fn schema_anchor_bound_matches_the_retrieval_kernel_bound() {
        assert_eq!(
            tracedecay_mcp_catalog::SEARCH_MAX_LEXICAL_ANCHORS,
            tracedecay_query::retrieval::lexical::MAX_LEXICAL_ANCHORS_V1
        );
        assert_eq!(
            tracedecay_mcp_catalog::SEARCH_MAX_LEXICAL_ANCHOR_BYTES,
            tracedecay_query::retrieval::lexical::MAX_LEXICAL_ANCHOR_BYTES_V1
        );
    }

    #[test]
    fn near_alignment_preserves_direction_and_offsets_selected_source_positions() {
        let anchor = tracedecay_code_index::clones::CloneTokenAnchorV1 {
            fingerprint: 17,
            left_token_position: 2,
            right_token_position: 9,
        };
        assert_eq!(
            similar_alignment_anchor(anchor, 11),
            SimilarAlignmentAnchorV1 {
                fingerprint: 17,
                source_token_position: 13,
                candidate_token_position: 9,
            }
        );
        assert_eq!(directional_coverage(7, 10), 700_000);
        assert_eq!(directional_coverage(10, 0), 0);
    }

    #[test]
    fn near_coverage_distinguishes_complete_partial_and_excluded() {
        use tracedecay_code_index::clones::CloneBodyEligibilityV1;
        use tracedecay_query::retrieval::lexical::CloneFingerprintPartialReasonV1;

        assert_eq!(
            similar_near_coverage(CloneBodyEligibilityV1::Eligible, &[], false),
            SimilarNearCoverageV1::Complete
        );
        assert_eq!(
            similar_near_coverage(
                CloneBodyEligibilityV1::Eligible,
                &[CloneFingerprintPartialReasonV1::HotPostings],
                false,
            ),
            SimilarNearCoverageV1::Partial {
                reasons: vec![SimilarNearPartialReasonV1::HotPostings]
            }
        );
        assert_eq!(
            similar_near_coverage(
                CloneBodyEligibilityV1::ExcludedTooSmall { minimum_tokens: 14 },
                &[],
                false,
            ),
            SimilarNearCoverageV1::ExcludedTooSmall { minimum_tokens: 14 }
        );
    }

    #[test]
    fn near_unavailable_reason_remains_typed_at_the_mcp_boundary() {
        assert_eq!(
            similar_unavailable_reason(
                tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::GenerationUnverified,
            ),
            SimilarNearUnavailableReasonV1::GenerationUnverified
        );
        assert_eq!(
            similar_match_class(tracedecay_code_index::clones::CloneNormalizationClassV1::Rename,),
            SimilarMatchClassV1::RenameNormalizedExact
        );
    }

    #[tokio::test]
    async fn similar_unavailable_wire_preserves_reason_and_retryability() {
        let temp = tempfile::tempdir().expect("temp root");
        let admitted = crate::tool_context::tests::scope("similar-unavailable");
        let project = crate::tool_context::tests::project_bundle(temp.path(), &admitted, None);
        let authority = tracedecay_query::code_search::CodeIndexSearchAuthorityV1 {
            principal: tracedecay_domain::PrincipalId::new("principal.similar-unavailable")
                .expect("principal"),
            authorization_revision: tracedecay_domain::AuthorizationRevision::new(
                "revision.similar-unavailable",
            )
            .expect("revision"),
        };

        for (reason, reason_code, retryable) in [
            (
                tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::GenerationUnavailable,
                "generation_unavailable",
                true,
            ),
            (
                tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::CorruptionResetRequired,
                "index_corruption_reset_required",
                false,
            ),
        ] {
            let executor: tracedecay_query::code_search::CodeIndexSimilarExecutor =
                std::sync::Arc::new(move |_| {
                    Box::pin(async move {
                        tracedecay_query::code_search::CodeIndexSimilarOutcomeV1::Unavailable(reason)
                    })
                });
            let code_index =
                crate::AdmittedCodeIndex::new(&authority, None, Some(&executor), None, None)
                    .expect("similar executor admits");
            let ctx = crate::McpToolContext::bind(crate::McpToolBinding {
                project: &project,
                request: crate::McpRequestAuthoritiesV1 {
                    code_index: Some(code_index),
                    ..crate::McpRequestAuthoritiesV1::default()
                },
            })
            .expect("admitted similar binding");
            let result = handle_similar(
                &ctx,
                json!({
                    "project_id": admitted.project_id,
                    "repository_id": admitted.repository_id,
                    "target": {
                        "kind": "symbol_occurrence",
                        "symbol_occurrence_id": "symbol.similar-unavailable",
                    },
                    "match_classes": ["conservative_exact"],
                    "result_limit": 10,
                    "work_limit": 20,
                }),
            )
            .await;
            let Err(error) = result else {
                panic!("unavailable similar executor must remain a transport failure");
            };
            let response =
                crate::tool_error_response(json!(1), "tracedecay_similar", &error);
            let wire: Value = serde_json::from_str(&crate::serialize_response_line(&response))
                .expect("JSON-RPC response");

            assert_eq!(wire["error"]["data"]["reason_code"], reason_code);
            assert_eq!(wire["error"]["data"]["retryable"], retryable);
        }
    }

    fn context_memory_hit(content: &str) -> FactSearchHitV1 {
        serde_json::from_value(json!({
            "fact": {
                "owner": {"kind": "profile"},
                "fact_id": "fact.0000000000000000000000000000000000000000000000000000000000000000.1111111111111111111111111111111111111111111111111111111111111111",
                "content": content,
                "category": "project",
                "tags": [],
                "entities": [],
                "trust_score_millionths": 900_000,
                "source": {"kind": "application", "operation_id": "operation.context-memory"},
                "source_label": "context-test",
                "active_assertion_id": "assertion.context-memory",
                "last_event_id": "event.context-memory",
                "projected_as_of": 1,
                "telemetry": {
                    "retrieval_count": 0,
                    "access_count": 0,
                    "helpful_count": 0,
                    "unhelpful_count": 0,
                    "created_at": 1,
                    "updated_at": 1,
                    "last_retrieved_at": null,
                    "last_recalled_at": null,
                    "last_feedback_at": null
                },
                "metadata": {}
            },
            "scores": {
                "score_millionths": 500_000,
                "fts_score_millionths": 250_000,
                "jaccard_score_millionths": 250_000,
                "holographic_score_millionths": 0,
                "trust_score_millionths": 900_000
            },
            "why": null
        }))
        .expect("canonical context memory hit")
    }

    #[test]
    fn context_related_relation_budget_is_a_page_not_a_complete_walk() {
        assert_eq!(context_related_relation_budget(1), 16);
        assert_eq!(context_related_relation_budget(20), 64);
        assert_eq!(context_related_relation_budget(200), 64);
        assert!(
            context_related_relation_budget(20) < 50_000,
            "context must not reuse the callers complete-walk budget"
        );
    }

    /// A warm response must render exactly as it did before coverage existed:
    /// every lane complete, no coverage section, no added lines.
    #[test]
    fn warm_coverage_leaves_the_rendered_body_unchanged() {
        let coverage =
            coverage_value(&tracedecay_query::code_search::CodeIndexSearchCoverageV1::warm());
        assert_eq!(coverage["recall"], json!("full"));
        assert_eq!(coverage["exact"], json!("complete"));

        let without = json!({
            "results": [{
                "candidate": {
                    "anchor_id": "code-symbol:symbol.v1",
                    "exact_class": "exact_message",
                    "utility_micros": 4_000_000
                },
                "final_ordinal": 0,
            }],
            "code_generation": "generation.warm",
        });
        let mut with = without.clone();
        with["coverage"] = coverage;

        assert_eq!(
            render_search_md(&with),
            render_search_md(&without),
            "warm coverage must be additive metadata, never rendered output"
        );
    }

    #[test]
    fn search_renders_symbol_id_for_source_body_without_graph_enrichment() {
        let node_id =
            "symbol.v1.sha256:4ddd636456fccc2962006c7803bd94b2d7d732c6830993a429535e0b0ff0b688";
        let anchor = format!("{CODE_SYMBOL_EVIDENCE_PREFIX}{node_id}");
        let symbol = graph_occurrence_id(&anchor).expect("search symbol anchor");
        for display in [
            Value::Null,
            json!({"name": "load_current_mutations", "kind": "function"}),
        ] {
            let mut result = json!({
                "candidate": {"anchor_id": anchor, "exact_class": "approximate"},
                "node_id": symbol,
            });
            if !display.is_null() {
                result["display"] = display;
            }
            let rendered = render_search_md(&json!({"results": [result]}));
            assert!(rendered.contains(&format!(
                "Read source: `tracedecay_source_body` with `node_id: {node_id}`"
            )));
            assert!(!rendered.contains("node_id: code-symbol:"));
        }
        let chunk = render_search_md(&json!({"results": [{
            "candidate": {"anchor_id": "code-chunk:chunk.fixture"}
        }]}));
        assert!(!chunk.contains("tracedecay_source_body"));
    }

    #[tokio::test]
    async fn search_reads_freshness_after_the_search_future_resolves() {
        let order = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let search_order = std::sync::Arc::clone(&order);
        let freshness_order = std::sync::Arc::clone(&order);
        let executor: tracedecay_query::code_search::CodeIndexSearchExecutor = std::sync::Arc::new(
            move |_| {
                let search_order = std::sync::Arc::clone(&search_order);
                Box::pin(async move {
                    search_order.lock().expect("order").push("search_start");
                    tokio::task::yield_now().await;
                    search_order.lock().expect("order").push("search_done");
                    tracedecay_query::code_search::CodeIndexSearchOutcomeV1::Unavailable(
                        tracedecay_query::code_search::CodeIndexSearchUnavailableV1 {
                            code_generation: None,
                            reason: tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::AuthorityUnavailable,
                            coverage: tracedecay_query::code_search::CodeIndexSearchCoverageV1::unavailable(
                                "authority_unavailable",
                            ),
                        },
                    )
                })
            },
        );
        let freshness: tracedecay_contracts::code_index_freshness::CodeIndexFreshnessReader =
            std::sync::Arc::new(move |_| {
                let freshness_order = std::sync::Arc::clone(&freshness_order);
                Box::pin(async move {
                    freshness_order.lock().expect("order").push("freshness");
                    None
                })
            });
        let temp = tempfile::tempdir().expect("temp root");
        let admitted = crate::tool_context::tests::scope("freshness-order");
        let project = crate::tool_context::tests::project_bundle(temp.path(), &admitted, None);
        let authority = tracedecay_query::code_search::CodeIndexSearchAuthorityV1 {
            principal: tracedecay_domain::PrincipalId::new("principal.freshness-order")
                .expect("principal"),
            authorization_revision: tracedecay_domain::AuthorizationRevision::new(
                "revision.freshness-order",
            )
            .expect("revision"),
        };
        let code_index =
            crate::AdmittedCodeIndex::new(&authority, Some(&executor), None, None, None)
                .expect("search executor admits");
        let ctx = crate::McpToolContext::bind(crate::McpToolBinding {
            project: &project,
            request: crate::McpRequestAuthoritiesV1 {
                code_index: Some(code_index),
                freshness: Some(&freshness),
                ..crate::McpRequestAuthoritiesV1::default()
            },
        })
        .expect("admitted search binding");

        handle_search(
            &ctx,
            async {
                Err(TraceDecayError::project_route(
                    "verified-code-graph-read-unavailable",
                    true,
                    "ordering test does not admit a graph",
                ))
            },
            json!({"query": "fixture"}),
            None,
            None,
        )
        .await
        .expect("search renders without a graph");

        assert_eq!(
            *order.lock().expect("order"),
            ["search_start", "search_done", "freshness"],
            "freshness must be read after the search future resolves"
        );
    }

    #[tokio::test]
    async fn installed_search_executor_owns_dispatch() {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = std::sync::Arc::clone(&calls);
        let executor: tracedecay_query::code_search::CodeIndexSearchExecutor = std::sync::Arc::new(
            move |request| {
                observed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                assert_eq!(request.query, "fixture");
                Box::pin(async {
                    tracedecay_query::code_search::CodeIndexSearchOutcomeV1::Unavailable(
                        tracedecay_query::code_search::CodeIndexSearchUnavailableV1 {
                            code_generation: Some("generation.fixture".to_owned()),
                            reason: tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::AuthorityUnavailable,
                            coverage: tracedecay_query::code_search::CodeIndexSearchCoverageV1::unavailable(
                                "authority_unavailable",
                            ),
                        },
                    )
                })
            },
        );
        let outcome = execute_code_index_search(
            Some(&executor),
            tracedecay_query::code_search::CodeIndexSearchRequestV1 {
                project_root: std::path::PathBuf::from("/fixture"),
                query: "fixture".to_owned(),
                source_revision: None,
                source_tree: None,
                source_reference: None,
                limit: 10,
                cursor: None,
                lexical_routing: LexicalRoutingV1::default(),
                authority: None,
                deadline: None,
                cancellation: None,
            },
        )
        .await;
        assert!(matches!(
            outcome,
            tracedecay_query::code_search::CodeIndexSearchOutcomeV1::Unavailable(
                tracedecay_query::code_search::CodeIndexSearchUnavailableV1 {
                    reason:
                        tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::AuthorityUnavailable,
                    ..
                }
            )
        ));
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn missing_search_executor_is_typed_capability_unavailable() {
        let outcome = execute_code_index_search(
            None,
            tracedecay_query::code_search::CodeIndexSearchRequestV1 {
                project_root: std::path::PathBuf::from("/fixture"),
                query: "fixture".to_owned(),
                source_revision: None,
                source_tree: None,
                source_reference: None,
                limit: 10,
                cursor: None,
                lexical_routing: LexicalRoutingV1::default(),
                authority: None,
                deadline: None,
                cancellation: None,
            },
        )
        .await;
        assert!(matches!(
            outcome,
            tracedecay_query::code_search::CodeIndexSearchOutcomeV1::Unavailable(
                tracedecay_query::code_search::CodeIndexSearchUnavailableV1 {
                    reason:
                        tracedecay_query::code_search::CodeIndexSearchUnavailableReasonV1::CapabilityUnavailable,
                    ..
                }
            )
        ));
    }

    #[test]
    fn context_markdown_lane_preview_keeps_all_lanes_visible() {
        let full = format!(
            "## Code Context\n**Query:** q\n\n### Memory Matches\n{}\n### Entry Points\n{}\n### Related Symbols\n{}\n### Code\n{}\n### Index Coverage Hint\n{}\n### Extension Points\n{}\n### Test Coverage\n{}\nseen_node_ids: [{}]\n",
            "memory fact with unicode caf\u{e9}\n".repeat(300),
            "- **entry** src/lib.rs:1\n".repeat(300),
            "- related\n".repeat(500),
            "```rust\nfn demo() {}\n```\n".repeat(500),
            "hint\n".repeat(500),
            "- trait\n".repeat(400),
            "- tests/context_test.rs\n".repeat(400),
            "\"node-id\",".repeat(400)
        );

        let preview = context_markdown_lane_preview(&full);

        for heading in [
            "## Code Context",
            "### Memory Matches",
            "### Entry Points",
            "### Related Symbols",
            "### Code",
            "### Index Coverage Hint",
            "### Extension Points",
            "### Test Coverage",
            "seen_node_ids:",
        ] {
            assert!(preview.contains(heading), "missing {heading}: {preview}");
        }
        assert!(preview.len() < full.len());
        assert!(preview.contains("lane truncated"));
        assert!(preview.is_char_boundary(preview.len()));
    }

    #[test]
    fn context_lane_preview_keeps_seen_node_ids_parseable() {
        let ids: Vec<String> = (0..100).map(|i| format!("function:{i:032x}")).collect();
        let markdown = format!(
            "{} {}\n",
            CONTEXT_SEEN_NODE_IDS_LABEL,
            serde_json::to_string(&ids)
                .unwrap_or_else(|err| panic!("failed to serialize seen node ids: {err}"))
        );

        let preview = context_markdown_lane_preview(&markdown);
        let json = match preview.strip_prefix(CONTEXT_SEEN_NODE_IDS_LABEL) {
            Some(json) => json.trim(),
            None => panic!("preview should keep seen_node_ids label: {preview}"),
        };
        let parsed: Vec<String> = serde_json::from_str(json)
            .unwrap_or_else(|err| panic!("failed to parse seen node ids: {err}: {json}"));

        assert_eq!(parsed, ids);
        assert!(!preview.contains("lane truncated"));
    }

    #[test]
    fn canonical_fact_identity_and_source_survive_both_context_renderings() {
        use tracedecay_contracts::memory::{FactCommitOwnerV1, FactIdentitySourceResultV1};
        use tracedecay_domain::{
            FactAssertionId, FactEventId, LocatorDigest, ProjectId, RetrievalAnchorId, UtcMicros,
        };

        let profile_hit = context_memory_hit("profile content");
        let mut project_hit = context_memory_hit("project content");
        project_hit.fact.owner = FactCommitOwnerV1::Project {
            project_id: ProjectId::new("project.context-memory").expect("project identity"),
        };
        project_hit.fact.active_assertion_id =
            FactAssertionId::new("assertion.project-context").expect("assertion identity");
        project_hit.fact.last_event_id =
            FactEventId::new("event.project-context").expect("event identity");
        project_hit.fact.source = FactIdentitySourceResultV1::Evidence {
            anchor_id: RetrievalAnchorId::new("retrieval.source.context").expect("source anchor"),
            stable_key: LocatorDigest::new(format!("sha256:{}", "a".repeat(64)))
                .expect("stable locator"),
        };
        let hits = vec![profile_hit, project_hit];
        let request: ContextSurfaceRequestV1 =
            serde_json::from_value(json!({"task": "context"})).expect("request");
        let contribution =
            ContextMemoryContributionV1::from_matches(&request, &hits, None, None, UtcMicros(20))
                .expect("canonical contribution");
        assert!(
            ContextMemoryContributionV1::from_matches(
                &request,
                &vec![hits[0].clone(); 11],
                None,
                None,
                UtcMicros(20)
            )
            .is_err()
        );
        let history_request: ContextSurfaceRequestV1 = serde_json::from_value(json!({
            "task": "history", "temporal_query": tracedecay_contracts::memory::CognitiveRecallTemporalQuery::current(UtcMicros(10)).with_history()
        })).expect("history request");
        for (supplied_hits, supplied_mode) in [
            (
                hits.as_slice(),
                tracedecay_contracts::memory::CognitiveRecallTemporalMode::History,
            ),
            (
                &[][..],
                tracedecay_contracts::memory::CognitiveRecallTemporalMode::AsOf,
            ),
        ] {
            assert!(ContextMemoryContributionV1::from_matches(
                &history_request,
                supplied_hits,
                None,
                Some(tracedecay_contracts::retrieval::ContextMemoryTemporalCoverageV1::WithheldCurrentOnly {
                    requested_mode: supplied_mode,
                }),
                UtcMicros(20)
            ).is_err());
        }
        for args in [json!({}), json!({"format": "json"})] {
            let result = rendered_context_tool_result(
                Path::new("."),
                &args,
                json!({"memory_matches": hits}),
                Vec::new(),
                "### Memory Matches\nRendered summary with no identity parsing.\n".to_owned(),
                None,
                contribution.clone(),
            );
            let carried = result
                .context_memory_contribution()
                .expect("typed context sidecar");
            assert_eq!(carried.facts().len(), hits.len());
            for (carried, hit) in carried.facts().iter().zip(&hits) {
                assert_eq!(carried.owner, hit.fact.owner);
                assert_eq!(carried.fact_id, hit.fact.fact_id);
                assert_eq!(carried.last_event_id, hit.fact.last_event_id);
                assert_eq!(carried.active_assertion_id, hit.fact.active_assertion_id);
                assert_eq!(carried.source, hit.fact.source);
            }
            assert!(result.value.get("context_memory_contribution").is_none());
            assert!(!response_text(&result).contains("context_memory_contribution"));
        }
    }

    #[test]
    fn context_memory_section_keeps_full_content_for_retrieval_handle() {
        let content = format!("{}tail-marker", "long memory body ".repeat(100));
        let hit = context_memory_hit(&content);

        let Some(section) = context_memory_section(&[hit], None, None) else {
            panic!("memory hit should render");
        };

        assert!(section.contains(&content));
        assert!(section.contains("tail-marker"));
        assert!(!section.contains("..."));
        assert!(section.contains("tracedecay_fact_feedback"));
    }

    #[test]
    fn context_memory_section_compacts_multiline_content() {
        let hit = context_memory_hit("first line\n# heading\n- item");

        let Some(section) = context_memory_section(&[hit], None, None) else {
            panic!("memory hit should render");
        };

        assert!(section.contains("first line # heading - item"));
        assert!(!section.contains("\n# heading"));
        assert!(!section.contains("\n- item"));
    }

    #[test]
    fn context_lane_preview_closes_open_code_fence_before_truncation_note() {
        let markdown = format!("### Code\n```rust\n{}\n", "fn demo() {}\n".repeat(1_000));

        let preview = context_markdown_lane_preview(&markdown);

        assert!(preview.contains("```\n\n... lane truncated"));
    }

    #[test]
    fn context_lane_preview_ignores_heading_markers_inside_code_fences() {
        let markdown = format!(
            "### Code\n```markdown\n{}\n```\n### Test Coverage\n- real lane\n",
            "### not a lane\n".repeat(1_000)
        );

        let preview = context_markdown_lane_preview(&markdown);

        assert!(preview.contains("### Code"));
        assert!(preview.contains("### Test Coverage"));
        assert_eq!(preview.matches("lane truncated").count(), 1);
    }
}
