//! tracedecay_simplify_scan: one bounded pass over the verified analysis
//! reports that are useful when simplifying a change.
//!
//! The old implementation mixed a private SQLite similarity query with
//! graph reads. V2 has one admitted graph generation, so this composite builds
//! its dead-code, complexity, and coupling findings from that generation.
//! Similarity is represented as explicitly unavailable until a canonical clone
//! authority can be admitted to this handler family.

use std::collections::{HashMap, HashSet};

use serde_json::{Value, json};
use tracedecay_code_index::graph_projection::CodeGraphSemanticEdgeV1;
use tracedecay_code_index::lineage::LineageSymbolRecordV1;
use tracedecay_domain::code_intelligence::NodeKind;
use tracedecay_domain::errors::{Result, TraceDecayError};
use tracedecay_domain::{RelationEdgeKindV1, SymbolOccurrenceId};
use tracedecay_graph_query::VerifiedGraphQuery;

use crate::handlers::graph::user_line;
use crate::{ToolResult, rendered_tool_result, require_object_args, unique_file_paths};

const DEFAULT_LIMIT: usize = 25;
const MAX_LIMIT: usize = 100;
const MAX_FILES: usize = 64;
const MAX_FILE_PATH_BYTES: usize = 4_096;
const MAX_SYMBOLS_PER_FILE: usize = 10_000;
const MAX_RELATIONS: usize = 2_000_000;
const DEFAULT_COMPLEXITY_THRESHOLD: u64 = 100;
const MAX_COMPLEXITY_THRESHOLD: u64 = 1_000_000;
const DEFAULT_COUPLING_THRESHOLD: usize = 15;
const MAX_COUPLING_THRESHOLD: usize = 10_000;

/// A target symbol with the binding and metadata required by every V2 report.
#[derive(Clone)]
struct TargetSymbol {
    occurrence: SymbolOccurrenceId,
    file: String,
    metadata: LineageSymbolRecordV1,
}

struct TargetSymbols {
    symbols: Vec<TargetSymbol>,
    /// Files whose symbol rows were not complete enough to participate in a
    /// verified report. Their omission is carried into report status.
    incomplete_files: Vec<IncompleteFile>,
}

#[derive(Clone)]
struct IncompleteFile {
    path: String,
    reason_code: String,
}

#[derive(Clone)]
struct Report {
    status: &'static str,
    complete: bool,
    findings: Vec<Value>,
    omitted_count: Option<usize>,
    reason_code: Option<String>,
    detail: Option<String>,
}

impl Report {
    fn complete(mut findings: Vec<Value>, limit: usize) -> Self {
        let omitted_count = findings.len().saturating_sub(limit);
        findings.truncate(limit);
        Self {
            status: "complete",
            complete: true,
            findings,
            omitted_count: Some(omitted_count),
            reason_code: None,
            detail: None,
        }
    }

    fn partial(mut findings: Vec<Value>, limit: usize, detail: impl Into<String>) -> Self {
        findings.truncate(limit);
        Self {
            status: "partial",
            complete: false,
            findings,
            omitted_count: None,
            reason_code: Some("analysis-input-incomplete".to_owned()),
            detail: Some(detail.into()),
        }
    }

    fn unavailable(report: &str, error: &TraceDecayError) -> Self {
        let (reason_code, retryable) = error.project_route_context().map_or_else(
            || ("analysis-report-unavailable".to_owned(), false),
            |(reason_code, retryable, _)| (reason_code.to_owned(), retryable),
        );
        let detail = if retryable {
            format!("{report} is temporarily unavailable from the admitted V2 report authority")
        } else {
            format!("{report} is unavailable from the admitted V2 report authority")
        };
        Self {
            status: "unavailable",
            complete: false,
            findings: Vec::new(),
            omitted_count: None,
            reason_code: Some(reason_code),
            detail: Some(detail),
        }
    }

    fn as_value(&self) -> Value {
        let mut object = json!({
            "status": self.status,
            "complete": self.complete,
            "finding_count": self.findings.len(),
            "findings": self.findings.clone(),
        });
        if let Some(omitted_count) = self.omitted_count {
            object["omitted_count"] = json!(omitted_count);
        } else {
            object["omitted_count"] = Value::Null;
        }
        if let Some(reason_code) = &self.reason_code {
            object["reason_code"] = json!(reason_code);
        }
        if let Some(detail) = &self.detail {
            object["detail"] = json!(detail);
        }
        object
    }
}

/// Runs the simplification composite against one generation-pinned graph.
///
/// Every graph read is bounded by the same per-request graph budget and every
/// file/edge loop checks the request admission state. A report failure is
/// retained as an explicit unavailable section; cancellation and deadline
/// failures remain transport errors so the server can settle them correctly.
#[hotpath::measure(future = true, label = "mcp.analysis.simplify_scan.total")]
pub async fn handle_simplify_scan(
    graph: &VerifiedGraphQuery,
    args: Value,
    scope_prefix: Option<&str>,
) -> Result<ToolResult> {
    let request = SimplifyScanRequest::parse(&args, scope_prefix)?;
    ensure_request_open(graph)?;

    let targets = collect_target_symbols(graph, &request.files).await?;
    ensure_request_open(graph)?;

    let seeds = targets
        .symbols
        .iter()
        .map(|symbol| symbol.occurrence.clone())
        .collect::<Vec<_>>();

    let (incoming, outgoing) = if seeds.is_empty() {
        (Vec::new(), Vec::new())
    } else {
        match (
            graph.callers(&seeds, &[], MAX_RELATIONS),
            graph.callees(&seeds, &[], MAX_RELATIONS),
        ) {
            (Ok(incoming), Ok(outgoing)) => (incoming, outgoing),
            (Err(error), _) | (_, Err(error)) => {
                if is_interrupt_error(&error) {
                    return Err(error);
                }
                let unavailable = Report::unavailable("verified analysis relations", &error);
                let output = assemble_output(
                    &request,
                    ReportBundle {
                        dead_code: unavailable.clone(),
                        complexity: unavailable.clone(),
                        coupling: unavailable,
                        duplications: unavailable_similarity_report(),
                    },
                    &targets.incomplete_files,
                );
                let touched_files = unique_file_paths(request.files.iter().map(String::as_str));
                return Ok(render_output(graph, &args, &output, touched_files));
            }
        }
    };
    ensure_request_open(graph)?;

    let incoming_by_symbol = edges_by_seed(&targets.symbols, incoming)?;
    let outgoing_by_symbol = edges_by_seed(&targets.symbols, outgoing)?;

    let dead_code = dead_code_report(
        &targets.symbols,
        &incoming_by_symbol,
        request.limit,
        &targets.incomplete_files,
    );
    let complexity = complexity_report(
        &targets.symbols,
        &incoming_by_symbol,
        &outgoing_by_symbol,
        request.limit,
        request.complexity_threshold,
        &targets.incomplete_files,
    );
    let coupling = coupling_report(
        &targets.symbols,
        &incoming_by_symbol,
        request.limit,
        request.coupling_threshold,
        scope_prefix,
        &targets.incomplete_files,
    );
    let output = assemble_output(
        &request,
        ReportBundle {
            dead_code,
            complexity,
            coupling,
            duplications: unavailable_similarity_report(),
        },
        &targets.incomplete_files,
    );
    let touched_files = unique_file_paths(request.files.iter().map(String::as_str));
    Ok(rendered_tool_result(
        Some(graph.project_root()?),
        &args,
        &output,
        touched_files,
        || render_simplify_scan_markdown(&output),
    ))
}

struct SimplifyScanRequest {
    files: Vec<String>,
    limit: usize,
    complexity_threshold: u64,
    coupling_threshold: usize,
}

impl SimplifyScanRequest {
    fn parse(args: &Value, scope_prefix: Option<&str>) -> Result<Self> {
        require_object_args(args, "tracedecay_simplify_scan")?;
        let values =
            args.get("files")
                .and_then(Value::as_array)
                .ok_or_else(|| TraceDecayError::Config {
                    message: "missing required parameter: files (array of strings)".to_owned(),
                })?;
        if values.is_empty() {
            return Err(TraceDecayError::Config {
                message: "tracedecay_simplify_scan requires at least one file".to_owned(),
            });
        }
        if values.len() > MAX_FILES {
            return Err(TraceDecayError::Config {
                message: format!(
                    "tracedecay_simplify_scan accepts at most {MAX_FILES} files per call"
                ),
            });
        }

        let mut files = HashSet::with_capacity(values.len());
        for value in values {
            let file = value
                .as_str()
                .ok_or_else(|| TraceDecayError::Config {
                    message: "tracedecay_simplify_scan files must be strings".to_owned(),
                })?
                .trim();
            if file.is_empty() {
                return Err(TraceDecayError::Config {
                    message: "tracedecay_simplify_scan file paths must not be empty".to_owned(),
                });
            }
            if file.len() > MAX_FILE_PATH_BYTES {
                return Err(TraceDecayError::Config {
                    message: format!(
                        "tracedecay_simplify_scan file path exceeds {MAX_FILE_PATH_BYTES} bytes"
                    ),
                });
            }
            if !tracedecay_runtime_core::path_scope::path_matches_scope(file, scope_prefix) {
                return Err(TraceDecayError::project_route(
                    "analysis-file-outside-scope",
                    false,
                    "a simplify-scan file is outside the admitted project scope",
                ));
            }
            files.insert(file.to_owned());
        }
        let mut files = files.into_iter().collect::<Vec<_>>();
        files.sort();

        let limit = args
            .get("limit")
            .and_then(Value::as_u64)
            .map_or(DEFAULT_LIMIT, |value| value as usize);
        if limit == 0 {
            return Err(TraceDecayError::Config {
                message: "tracedecay_simplify_scan limit must be at least 1".to_owned(),
            });
        }
        let limit = limit.min(MAX_LIMIT);

        let complexity_threshold = args
            .get("complexity_threshold")
            .and_then(Value::as_u64)
            .map_or(DEFAULT_COMPLEXITY_THRESHOLD, |value| {
                value.min(MAX_COMPLEXITY_THRESHOLD)
            });
        let coupling_threshold = args
            .get("coupling_threshold")
            .and_then(Value::as_u64)
            .map_or(DEFAULT_COUPLING_THRESHOLD, |value| {
                (value as usize).min(MAX_COUPLING_THRESHOLD)
            });

        Ok(Self {
            files,
            limit,
            complexity_threshold,
            coupling_threshold,
        })
    }
}

async fn collect_target_symbols(
    graph: &VerifiedGraphQuery,
    files: &[String],
) -> Result<TargetSymbols> {
    let mut symbols = Vec::new();
    let mut incomplete_files = Vec::new();
    for file in files {
        ensure_request_open(graph)?;
        let rows = match graph.symbols_in_logical_file(file, MAX_SYMBOLS_PER_FILE) {
            Ok(rows) => rows,
            Err(error) if is_interrupt_error(&error) => return Err(error),
            Err(error) => {
                incomplete_files.push(IncompleteFile {
                    path: file.clone(),
                    reason_code: reason_code_for(&error),
                });
                continue;
            }
        };
        for row in rows {
            let Some(binding) = row.binding else {
                incomplete_files.push(IncompleteFile {
                    path: file.clone(),
                    reason_code: "code-graph-symbol-binding-missing".to_owned(),
                });
                continue;
            };
            let Some(logical_path) = binding.logical_path else {
                incomplete_files.push(IncompleteFile {
                    path: file.clone(),
                    reason_code: "code-graph-symbol-path-missing".to_owned(),
                });
                continue;
            };
            if logical_path != *file {
                continue;
            }
            let Some(metadata) = row.metadata else {
                incomplete_files.push(IncompleteFile {
                    path: file.clone(),
                    reason_code: "code-graph-symbol-metadata-missing".to_owned(),
                });
                continue;
            };
            symbols.push(TargetSymbol {
                occurrence: row.occurrence,
                file: logical_path,
                metadata,
            });
        }
        tokio::task::yield_now().await;
    }
    Ok(TargetSymbols {
        symbols,
        incomplete_files,
    })
}

fn edges_by_seed(
    symbols: &[TargetSymbol],
    edges: Vec<Vec<CodeGraphSemanticEdgeV1>>,
) -> Result<HashMap<SymbolOccurrenceId, Vec<CodeGraphSemanticEdgeV1>>> {
    if symbols.len() != edges.len() {
        return Err(TraceDecayError::project_route(
            "code-graph-corrupt",
            false,
            "verified analysis relation batch does not match its symbol census",
        ));
    }
    Ok(symbols
        .iter()
        .zip(edges)
        .map(|(symbol, edges)| (symbol.occurrence.clone(), edges))
        .collect())
}

fn dead_code_report(
    symbols: &[TargetSymbol],
    incoming: &HashMap<SymbolOccurrenceId, Vec<CodeGraphSemanticEdgeV1>>,
    limit: usize,
    incomplete_files: &[IncompleteFile],
) -> Report {
    let mut findings = Vec::new();
    for symbol in symbols {
        if !is_function_or_method(&symbol.metadata.kind)
            || symbol.metadata.visibility == "public"
            || symbol.metadata.simple_name == "main"
            || symbol.metadata.simple_name.starts_with("test")
        {
            continue;
        }
        let mut live = false;
        let mut test_annotated = false;
        if let Some(edges) = incoming.get(&symbol.occurrence) {
            for edge in edges {
                if edge.edge.kind == RelationEdgeKindV1::Annotates {
                    if edge
                        .neighbor
                        .metadata
                        .as_ref()
                        .is_some_and(tracedecay_code_index::is_test_marker)
                    {
                        test_annotated = true;
                    }
                } else {
                    live = true;
                }
            }
        }
        if !live && !test_annotated {
            findings.push(json!({
                "id": symbol.occurrence.as_str(),
                "symbol": symbol.metadata.simple_name,
                "name": symbol.metadata.simple_name,
                "kind": symbol.metadata.kind,
                "file": symbol.file,
                "line": user_line(symbol.metadata.start_line),
                "reason": "no incoming edges (unreferenced)",
            }));
        }
    }
    findings.sort_by(|left, right| {
        finding_location(left)
            .cmp(&finding_location(right))
            .then_with(|| field_str(left, "id").cmp(field_str(right, "id")))
    });
    report_from_findings(
        findings,
        limit,
        incomplete_files,
        "dead-code symbol census is incomplete",
    )
}

fn complexity_report(
    symbols: &[TargetSymbol],
    incoming: &HashMap<SymbolOccurrenceId, Vec<CodeGraphSemanticEdgeV1>>,
    outgoing: &HashMap<SymbolOccurrenceId, Vec<CodeGraphSemanticEdgeV1>>,
    limit: usize,
    threshold: u64,
    incomplete_files: &[IncompleteFile],
) -> Report {
    let mut findings = Vec::new();
    for symbol in symbols {
        if !is_function_or_method(&symbol.metadata.kind) {
            continue;
        }
        let fan_in = incoming.get(&symbol.occurrence).map_or(0, Vec::len) as u64;
        let fan_out = outgoing.get(&symbol.occurrence).map_or(0, Vec::len) as u64;
        let score = u64::from(symbol.metadata.line_span)
            .saturating_add(fan_out.saturating_mul(3))
            .saturating_add(fan_in);
        if score > threshold {
            findings.push(json!({
                "id": symbol.occurrence.as_str(),
                "symbol": symbol.metadata.simple_name,
                "name": symbol.metadata.simple_name,
                "kind": symbol.metadata.kind,
                "file": symbol.file,
                "line": user_line(symbol.metadata.start_line),
                "lines": symbol.metadata.line_span,
                "fan_in": fan_in,
                "fan_out": fan_out,
                "score": score,
            }));
        }
    }
    findings.sort_by(|left, right| {
        field_u64(right, "score")
            .cmp(&field_u64(left, "score"))
            .then_with(|| finding_location(left).cmp(&finding_location(right)))
            .then_with(|| field_str(left, "id").cmp(field_str(right, "id")))
    });
    report_from_findings(
        findings,
        limit,
        incomplete_files,
        "complexity symbol census is incomplete",
    )
}

fn coupling_report(
    symbols: &[TargetSymbol],
    incoming: &HashMap<SymbolOccurrenceId, Vec<CodeGraphSemanticEdgeV1>>,
    limit: usize,
    threshold: usize,
    scope_prefix: Option<&str>,
    incomplete_files: &[IncompleteFile],
) -> Report {
    let mut related = HashMap::<String, HashSet<String>>::new();
    for symbol in symbols {
        let paths = related.entry(symbol.file.clone()).or_default();
        if let Some(edges) = incoming.get(&symbol.occurrence) {
            for edge in edges {
                if !matches!(
                    edge.edge.kind,
                    RelationEdgeKindV1::Calls | RelationEdgeKindV1::Uses
                ) {
                    continue;
                }
                let Some(path) = edge
                    .neighbor
                    .binding
                    .as_ref()
                    .and_then(|binding| binding.logical_path.as_deref())
                else {
                    continue;
                };
                if path != symbol.file
                    && tracedecay_runtime_core::path_scope::path_matches_scope(path, scope_prefix)
                {
                    paths.insert(path.to_owned());
                }
            }
        }
    }
    let mut findings = related
        .into_iter()
        .filter_map(|(file, paths)| {
            let fan_in = paths.len();
            (fan_in > threshold).then(|| {
                json!({
                    "file": file,
                    "fan_in": fan_in,
                    "coupled_files": fan_in,
                    "warning": "high fan-in — changes here affect many dependents",
                })
            })
        })
        .collect::<Vec<_>>();
    findings.sort_by(|left, right| {
        field_u64(right, "fan_in")
            .cmp(&field_u64(left, "fan_in"))
            .then_with(|| field_str(left, "file").cmp(field_str(right, "file")))
    });
    report_from_findings(
        findings,
        limit,
        incomplete_files,
        "coupling relation evidence is incomplete",
    )
}

fn report_from_findings(
    findings: Vec<Value>,
    limit: usize,
    incomplete_files: &[IncompleteFile],
    partial_detail: &str,
) -> Report {
    if incomplete_files.is_empty() {
        Report::complete(findings, limit)
    } else {
        Report::partial(findings, limit, partial_detail)
    }
}

fn unavailable_similarity_report() -> Report {
    Report {
        status: "unavailable",
        complete: false,
        findings: Vec::new(),
        omitted_count: None,
        reason_code: Some("verified-code-redundancy-unavailable".to_owned()),
        detail: Some(
            "duplication findings require the admitted canonical clone/redundancy authority"
                .to_owned(),
        ),
    }
}

struct ReportBundle {
    dead_code: Report,
    complexity: Report,
    coupling: Report,
    duplications: Report,
}

fn assemble_output(
    request: &SimplifyScanRequest,
    reports: ReportBundle,
    incomplete_files: &[IncompleteFile],
) -> Value {
    let all_reports = [
        ("dead_code", &reports.dead_code),
        ("complexity", &reports.complexity),
        ("coupling", &reports.coupling),
        ("duplications", &reports.duplications),
    ];
    let unavailable_reports = all_reports
        .iter()
        .filter(|(_, report)| report.status == "unavailable")
        .map(|(name, _)| *name)
        .collect::<Vec<_>>();
    let partial_reports = all_reports
        .iter()
        .filter(|(_, report)| report.status == "partial")
        .map(|(name, _)| *name)
        .collect::<Vec<_>>();
    let finding_count = all_reports
        .iter()
        .map(|(_, report)| report.findings.len())
        .sum::<usize>();
    let complete = unavailable_reports.is_empty() && partial_reports.is_empty();
    let incomplete_files = incomplete_files
        .iter()
        .map(|file| json!({ "file": file.path, "reason_code": file.reason_code }))
        .collect::<Vec<_>>();

    json!({
        "schema_version": "simplify_scan.v2",
        "files": request.files,
        "limit": request.limit,
        "complexity_threshold": request.complexity_threshold,
        "coupling_threshold": request.coupling_threshold,
        "complete": complete,
        "partial": !complete,
        "finding_count": finding_count,
        "unavailable_reports": unavailable_reports,
        "partial_reports": partial_reports,
        "incomplete_files": incomplete_files,
        "reports": {
            "dead_code": reports.dead_code.as_value(),
            "complexity": reports.complexity.as_value(),
            "coupling": reports.coupling.as_value(),
            "duplications": reports.duplications.as_value(),
        },
        // Stable V1 field names remain as projections of the V2 reports. The
        // explicit status fields above prevent these arrays from implying
        // that unavailable duplication evidence was an empty scan.
        "duplications": reports.duplications.findings.clone(),
        "dead_introductions": reports.dead_code.findings.clone(),
        "complexity_warnings": reports.complexity.findings.clone(),
        "coupling_warnings": reports.coupling.findings.clone(),
    })
}

fn render_output(
    graph: &VerifiedGraphQuery,
    args: &Value,
    output: &Value,
    touched_files: Vec<String>,
) -> ToolResult {
    rendered_tool_result(
        graph.project_root().ok().as_deref(),
        args,
        output,
        touched_files,
        || render_simplify_scan_markdown(output),
    )
}

fn render_simplify_scan_markdown(output: &Value) -> String {
    let mut md = crate::tools::render::Md::new();
    md.heading(1, "Simplify Scan");

    if output["complete"] == json!(false) {
        md.field("Status", "partial");
        if let Some(reports) = output["unavailable_reports"].as_array()
            && !reports.is_empty()
        {
            md.field(
                "Unavailable reports",
                &reports
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        md.blank();
    }

    let finding_count = output["finding_count"].as_u64().unwrap_or(0);
    if finding_count == 0 {
        md.empty_note(if output["complete"] == json!(true) {
            "No simplification findings for the scanned files."
        } else {
            "No verified simplification findings were available for the scanned files."
        });
        return md.render();
    }
    md.field("Findings", &finding_count.to_string()).blank();

    for (key, title, label) in [
        ("dead_code", "Potential Dead Code", "symbol"),
        ("complexity", "Complexity Warnings", "symbol"),
        ("coupling", "Coupling Warnings", "file"),
        ("duplications", "Possible Duplications", "symbol"),
    ] {
        let report = &output["reports"][key];
        let items = report["findings"].as_array().map_or(&[][..], Vec::as_slice);
        if items.is_empty() {
            continue;
        }
        md.heading(2, title);
        for item in items {
            let label_value = crate::tools::render::field_str(item, label);
            md.bullet(&format!("**{label_value}**"));
            if let Some(file) = item.get("file").and_then(Value::as_str) {
                let line = item.get("line").and_then(Value::as_u64).unwrap_or(0);
                md.line(&format!("  **Location:** {file}:{line}"));
            }
            if let Some(reason) = item.get("reason").and_then(Value::as_str) {
                md.line(&format!("  **Reason:** {reason}"));
            }
            if let Some(score) = item.get("score").and_then(Value::as_u64) {
                md.line(&format!("  **Score:** {score}"));
            }
            if let Some(fan_in) = item.get("fan_in").and_then(Value::as_u64) {
                md.line(&format!("  **Fan-in:** {fan_in}"));
            }
            md.blank();
        }
    }
    md.render()
}

fn ensure_request_open(graph: &VerifiedGraphQuery) -> Result<()> {
    match graph
        .request_context()
        .admission_at(tracedecay_contracts::now_micros())
    {
        tracedecay_contracts::RequestAdmission::Admitted => Ok(()),
        tracedecay_contracts::RequestAdmission::Cancelled => Err(TraceDecayError::project_route(
            "code-graph-cancelled",
            false,
            "simplify scan was cancelled",
        )),
        tracedecay_contracts::RequestAdmission::TimedOut => Err(TraceDecayError::project_route(
            "code-graph-timed-out",
            true,
            "simplify scan exceeded its admitted deadline",
        )),
    }
}

fn is_interrupt_error(error: &TraceDecayError) -> bool {
    matches!(
        error.project_route_context().map(|context| context.0),
        Some("code-graph-cancelled" | "code-graph-timed-out")
    )
}

fn reason_code_for(error: &TraceDecayError) -> String {
    error.project_route_context().map_or_else(
        || "analysis-report-unavailable".to_owned(),
        |context| context.0.to_owned(),
    )
}

fn is_function_or_method(kind: &str) -> bool {
    matches!(
        NodeKind::from_str(kind),
        Some(NodeKind::Function | NodeKind::Method)
    )
}

fn finding_location(value: &Value) -> String {
    format!(
        "{}:{}",
        field_str(value, "file"),
        value.get("line").and_then(Value::as_u64).unwrap_or(0)
    )
}

fn field_str<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or_default()
}

fn field_u64(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{SimplifyScanRequest, assemble_output, render_simplify_scan_markdown};
    use serde_json::json;

    #[test]
    fn request_deduplicates_files_and_clamps_the_output_limit() {
        let request = SimplifyScanRequest::parse(
            &json!({
                "files": ["src/z.rs", "src/a.rs", "src/z.rs"],
                "limit": 1000,
            }),
            None,
        )
        .expect("valid request");
        assert_eq!(request.files, ["src/a.rs", "src/z.rs"]);
        assert_eq!(request.limit, super::MAX_LIMIT);
    }

    #[test]
    fn request_rejects_empty_files_and_out_of_scope_paths() {
        assert!(SimplifyScanRequest::parse(&json!({"files": []}), None).is_err());
        assert!(SimplifyScanRequest::parse(&json!({"files": [""]}), None).is_err());
        let error = SimplifyScanRequest::parse(&json!({"files": ["src2/main.rs"]}), Some("src"))
            .expect_err("outside-scope files must fail closed");
        assert_eq!(
            error.project_route_context().map(|context| context.0),
            Some("analysis-file-outside-scope")
        );
    }

    #[test]
    fn markdown_exposes_unavailable_reports_instead_of_claiming_no_findings() {
        let request = SimplifyScanRequest::parse(&json!({"files": ["src/lib.rs"]}), None)
            .expect("valid request");
        let output = assemble_output(
            &request,
            super::ReportBundle {
                dead_code: super::Report::complete(Vec::new(), request.limit),
                complexity: super::Report::complete(Vec::new(), request.limit),
                coupling: super::Report::complete(Vec::new(), request.limit),
                duplications: super::unavailable_similarity_report(),
            },
            &[],
        );
        assert_eq!(output["complete"], false);
        assert_eq!(output["unavailable_reports"], json!(["duplications"]));
        let markdown = render_simplify_scan_markdown(&output);
        assert!(markdown.contains("Status"));
        assert!(markdown.contains("duplications"));
        assert!(!markdown.contains("No simplification findings"));
    }
}
