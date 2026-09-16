//! `tracedecay_unused_imports` — parser-backed import bindings whose local
//! names never occur in the rest of the indexed source file.
//!
//! Import declarations are not symbols in the V2 graph.  This handler therefore
//! takes its file set and content identity from the admitted graph generation,
//! then asks the same extraction registry used by the index for the structured
//! import rows.  It never reads compiler diagnostics or the retired V1 graph
//! reader.  Before a source file is parsed its digest is checked against the
//! generation's file record; a changed file becomes an explicit partial result
//! instead of a false clean answer.

use std::collections::HashSet;
use std::path::Path;

use serde_json::{Value, json};
use tracedecay_code_extraction::{ImportNamespaceV1, LanguageRegistry};
use tracedecay_domain::SnapshotFileDispositionV1;
use tracedecay_domain::code_intelligence::{Node, NodeKind, Visibility};
use tracedecay_domain::errors::{Result, TraceDecayError};
use tracedecay_graph_query::VerifiedGraphQuery;

use super::path_matches_optional_scope;
use crate::ToolResult;
use crate::handlers::graph::user_line;
use crate::handlers::support::require_object_args;
use crate::tools::render;

const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 500;
/// The source walk is intentionally finite.  A partial answer carries the
/// last completed file as a continuation so callers can finish the census.
const FILE_BUDGET: usize = 400;
/// Prevents a file census from allocating an unbounded in-memory snapshot
/// before the bounded source walk starts.
const FILE_CENSUS_BUDGET: usize = 100_000;
const MAX_CURSOR_BYTES: usize = 4_096;
const CURSOR_PREFIX: &str = "unused-imports.v2:";
const MAX_IMPORT_OFFSET: usize = 1_000_000;
/// Keep this scan aligned with the source prefix the code-index extractor
/// admits. Findings after the indexed parser boundary would not be
/// generation-attested and therefore cannot be reported as verified.
const MAX_SOURCE_BYTES: usize = tracedecay_code_index::extract::MAX_EXTRACTION_SOURCE_BYTES;

#[derive(Clone)]
struct IndexedFile {
    path: String,
    digest: tracedecay_domain::ContentDigest,
    import_offset: usize,
}

struct ScanOutput {
    findings: Vec<Value>,
    touched: Vec<String>,
    scanned_files: usize,
    partial_reason: Option<String>,
    next_cursor: Option<String>,
    incomplete_files: Vec<Value>,
}

#[derive(Debug, Eq, PartialEq)]
enum ScanCursor {
    AfterPath(String),
    WithinFile { path: String, import_offset: usize },
}

/// Finds private, non-glob bindings from the current graph/index generation
/// that have no code reference in their owning source file.
#[hotpath::measure(future = true, label = "mcp.analysis.unused_imports.total")]
pub async fn handle_unused_imports(
    graph: &VerifiedGraphQuery,
    args: Value,
    scope_prefix: Option<&str>,
) -> Result<ToolResult> {
    require_object_args(&args, "tracedecay_unused_imports")?;
    let limit = args
        .get("limit")
        .and_then(Value::as_u64)
        .map_or(DEFAULT_LIMIT, |value| {
            usize::try_from(value)
                .unwrap_or(MAX_LIMIT)
                .clamp(1, MAX_LIMIT)
        });
    let cursor = parse_cursor(&args)?;

    let mut files = graph
        .files(FILE_CENSUS_BUDGET)?
        .into_iter()
        .filter(|file| file.disposition == SnapshotFileDispositionV1::Present)
        .filter(|file| path_is_rust(&file.logical_path))
        .filter(|file| path_matches_optional_scope(&file.logical_path, scope_prefix))
        .map(|file| {
            let path = file.logical_path;
            let import_offset = cursor.as_ref().map_or(0, |cursor| match cursor {
                ScanCursor::WithinFile {
                    path: cursor_path,
                    import_offset,
                } if cursor_path == &path => *import_offset,
                _ => 0,
            });
            IndexedFile {
                path,
                digest: file.content_digest,
                import_offset,
            }
        })
        .filter(|file| {
            cursor.as_ref().is_none_or(|cursor| match cursor {
                ScanCursor::AfterPath(after) => file.path.as_str() > after,
                ScanCursor::WithinFile { path, .. } => file.path.as_str() >= path,
            })
        })
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left.path.cmp(&right.path));

    let request_context = graph.request_context().clone();
    let (sources, pre_scan_incomplete, source_page_truncated, last_read) = hotpath::measure_block!(
        "mcp.analysis.unused_imports.read",
        read_sources(graph, files, &request_context)?
    );

    // Parsing is CPU-heavy and must not run on the async executor.  The
    // request context is copied into the worker so cancellation/deadline is
    // checked between files and never turned into a successful partial answer.
    let scan = hotpath::future!(
        tokio::task::spawn_blocking(move || {
            scan_sources(
                sources,
                limit,
                request_context,
                pre_scan_incomplete,
                source_page_truncated,
                last_read,
            )
        }),
        label = "mcp.analysis.unused_imports.scan"
    )
    .await
    .map_err(|join_error| TraceDecayError::Config {
        message: format!("tracedecay_unused_imports scan failed to join: {join_error}"),
    })??;

    let mut partial_reason = scan.partial_reason;
    let next_cursor = scan.next_cursor;
    if next_cursor.is_none() {
        partial_reason = None;
    }

    let incomplete_files = scan.incomplete_files;
    if partial_reason.is_none() && !incomplete_files.is_empty() {
        partial_reason = Some("source_evidence_incomplete".to_owned());
    }
    let complete = partial_reason.is_none();
    let output = json!({
        "schema_version": "unused_imports.v2",
        "unused_import_count": scan.findings.len(),
        "imports": scan.findings,
        "limit": limit,
        "scanned_files": scan.scanned_files,
        "complete": complete,
        "partial_reason": partial_reason,
        "next_cursor": next_cursor,
        "incomplete_files": incomplete_files,
    });

    Ok(crate::handlers::support::rendered_tool_result(
        Some(graph.project_root()?),
        &args,
        &output,
        scan.touched,
        || render_unused_imports_md(&output),
    ))
}

fn render_unused_imports_md(output: &Value) -> String {
    let mut md = render::Md::new();
    md.heading(2, "Unused Imports");
    let count = output
        .get("unused_import_count")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    md.field("Unused import count", &count.to_string());
    if output.get("complete").and_then(Value::as_bool) == Some(false) {
        let reason = output
            .get("partial_reason")
            .and_then(Value::as_str)
            .unwrap_or("incomplete evidence");
        md.field("Coverage", &format!("partial ({reason})"));
        if let Some(cursor) = output.get("next_cursor").and_then(Value::as_str) {
            md.field("Next cursor", &format!("`{cursor}`"));
        }
    }
    md.blank();

    let imports = output
        .get("imports")
        .and_then(Value::as_array)
        .map_or(&[][..], Vec::as_slice);
    if imports.is_empty() {
        md.empty_note("No unused private imports found.");
        return md.render();
    }

    md.heading(3, "Findings");
    for import in imports {
        let name = import
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("<unknown import>");
        let file = import
            .get("file")
            .and_then(Value::as_str)
            .unwrap_or("<unknown file>");
        let line = import.get("line").and_then(Value::as_u64).unwrap_or(0);
        md.bullet(&format!("**{name}** at `{file}:{line}`"));
        if let Some(module) = import.get("module").and_then(Value::as_str) {
            md.line(&format!("  **Module:** `{module}`"));
        }
    }
    md.render()
}

fn parse_cursor(args: &Value) -> Result<Option<ScanCursor>> {
    let Some(cursor) = args.get("cursor").and_then(Value::as_str) else {
        return Ok(None);
    };
    if cursor.is_empty() || cursor.len() > MAX_CURSOR_BYTES {
        return Err(TraceDecayError::Config {
            message: "tracedecay_unused_imports cursor must be a non-empty bounded path".to_owned(),
        });
    }
    if let Some(encoded) = cursor.strip_prefix(CURSOR_PREFIX) {
        let Some((path, offset)) = encoded.rsplit_once('#') else {
            return Err(TraceDecayError::Config {
                message: "tracedecay_unused_imports cursor continuation is malformed".to_owned(),
            });
        };
        let import_offset = offset
            .parse::<usize>()
            .map_err(|_| TraceDecayError::Config {
                message:
                    "tracedecay_unused_imports cursor continuation has an invalid import offset"
                        .to_owned(),
            })?;
        if import_offset > MAX_IMPORT_OFFSET {
            return Err(TraceDecayError::Config {
                message: "tracedecay_unused_imports cursor continuation exceeds its import bound"
                    .to_owned(),
            });
        }
        if !is_safe_logical_path(path) {
            return Err(TraceDecayError::Config {
                message: "tracedecay_unused_imports cursor continuation must name a project-relative path"
                    .to_owned(),
            });
        }
        return Ok(Some(ScanCursor::WithinFile {
            path: path.to_owned(),
            import_offset,
        }));
    }
    if !is_safe_logical_path(cursor) {
        return Err(TraceDecayError::Config {
            message: "tracedecay_unused_imports cursor must be a project-relative path".to_owned(),
        });
    }
    Ok(Some(ScanCursor::AfterPath(cursor.to_owned())))
}

fn encode_cursor(path: &str, import_offset: usize) -> String {
    format!("{CURSOR_PREFIX}{path}#{import_offset}")
}

fn read_sources(
    graph: &VerifiedGraphQuery,
    files: Vec<IndexedFile>,
    request_context: &tracedecay_contracts::RequestContext,
) -> Result<(Vec<SourceInput>, Vec<Value>, bool, Option<String>)> {
    let mut sources = Vec::new();
    let mut incomplete = Vec::new();
    let source_page_truncated = files.len() > FILE_BUDGET;
    let mut last_read = None;
    for file in files.into_iter().take(FILE_BUDGET) {
        ensure_request_open(request_context)?;
        last_read = Some(file.path.clone());
        let source = match graph.read_indexed_source_file(&file.path) {
            Ok(source) => source,
            Err(error) => {
                incomplete.push(incomplete_file(&file.path, reason_code_for(&error)));
                continue;
            }
        };
        let observed = tracedecay_code_index::chunks::content_digest(source.as_bytes());
        if observed != file.digest {
            incomplete.push(incomplete_file(&file.path, "source_changed_since_index"));
            continue;
        }
        sources.push(SourceInput { file, source });
    }
    Ok((sources, incomplete, source_page_truncated, last_read))
}

struct SourceInput {
    file: IndexedFile,
    source: String,
}

enum SourceScanOutcome {
    /// Each finding carries its position in the file's candidate-import list.
    /// The position is part of the continuation cursor when the result limit
    /// cuts a file in half.
    Findings(Vec<(usize, Value)>),
    Incomplete(&'static str),
}

fn scan_sources(
    sources: Vec<SourceInput>,
    limit: usize,
    request_context: tracedecay_contracts::RequestContext,
    mut incomplete_files: Vec<Value>,
    source_page_truncated: bool,
    last_read: Option<String>,
) -> Result<ScanOutput> {
    let registry = LanguageRegistry::new();
    let mut findings = Vec::new();
    let mut touched = Vec::new();
    let mut scanned_files = 0;
    let mut last_scanned = last_read;
    let mut partial_reason = None;
    let mut next_cursor = None;

    for input in sources {
        ensure_request_open(&request_context)?;
        scanned_files += 1;

        let file_findings = match scan_source_file(
            &registry,
            &input.file.path,
            &input.source,
            input.file.import_offset,
        )? {
            SourceScanOutcome::Incomplete(reason) => {
                incomplete_files.push(incomplete_file(&input.file.path, reason));
                continue;
            }
            SourceScanOutcome::Findings(findings) => findings,
        };
        for (import_offset, finding) in file_findings {
            ensure_request_open(&request_context)?;
            findings.push(finding);
            if !touched.contains(&input.file.path) {
                touched.push(input.file.path.clone());
            }
            if findings.len() >= limit {
                partial_reason = Some("limit_reached".to_owned());
                next_cursor = Some(encode_cursor(
                    &input.file.path,
                    import_offset.saturating_add(1),
                ));
                break;
            }
        }
        if partial_reason.is_some() {
            break;
        }
        last_scanned = Some(input.file.path.clone());
    }

    if partial_reason.is_none() && source_page_truncated {
        partial_reason = Some("file_budget_exhausted".to_owned());
        next_cursor = last_scanned;
    }

    Ok(ScanOutput {
        findings,
        touched,
        scanned_files,
        partial_reason,
        next_cursor,
        incomplete_files,
    })
}

fn scan_source_file(
    registry: &LanguageRegistry,
    path: &str,
    source: &str,
    import_offset: usize,
) -> Result<SourceScanOutcome> {
    if source.len() > MAX_SOURCE_BYTES {
        return Ok(SourceScanOutcome::Incomplete(
            "source_byte_budget_exhausted",
        ));
    }
    let Some(extractor) = registry.extractor_for_file(path) else {
        return Ok(SourceScanOutcome::Incomplete(
            "import_extractor_unavailable",
        ));
    };
    let artifact = extractor.extract_artifact(path, source);
    if !artifact.result.errors.is_empty() {
        return Ok(SourceScanOutcome::Incomplete("import_parser_incomplete"));
    }

    let imports = artifact
        .imports
        .iter()
        .filter(|import| {
            import.namespace != ImportNamespaceV1::SideEffect
                && !import.is_glob
                && !import.is_public
                && is_private_import(import, &artifact.result.nodes)
                && import.local_name.as_deref().is_some_and(|name| name != "_")
        })
        .collect::<Vec<_>>();
    if imports.is_empty() {
        return Ok(SourceScanOutcome::Findings(Vec::new()));
    }

    let masked = tracedecay_code_extraction::source_mask::masked_rust_source_with(
        source,
        tracedecay_code_extraction::source_mask::MaskOptions::UNUSED_IMPORTS,
    );
    let masked = without_use_declarations(masked, source, &artifact.result.nodes)?;
    let identifiers = identifiers_in_source(&masked);
    let findings = imports
        .into_iter()
        .enumerate()
        .skip(import_offset)
        .filter_map(|(import_offset, import)| {
            let local_name = import.local_name.as_deref()?;
            if identifiers.contains(local_name) {
                return None;
            }
            Some((import_offset, json!({
                "name": import_name(import.module_specifier.as_str(), import.imported_name.as_deref(), Some(local_name)),
                "unused": local_name,
                "file": import.logical_path,
                "line": user_line(import.start_line),
                "column": import.start_column.saturating_add(1),
                "module": import.module_specifier,
                "imported": import.imported_name,
                "local": import.local_name,
                "namespace": import.namespace,
                "module_kind": import.module_kind,
                "is_public": import.is_public,
                "is_glob": import.is_glob,
            })))
        })
        .collect();
    Ok(SourceScanOutcome::Findings(findings))
}

fn is_private_import(
    import: &tracedecay_code_extraction::ExtractedImportEvidenceV1,
    nodes: &[Node],
) -> bool {
    let position = (import.start_line, import.start_column);
    !nodes.iter().any(|node| {
        node.kind == NodeKind::Use
            && (node.start_line, node.start_column) <= position
            && position <= (node.end_line, node.end_column)
            && node.visibility != Visibility::Private
    })
}

fn without_use_declarations(masked: String, source: &str, nodes: &[Node]) -> Result<String> {
    let mut line_starts = vec![0usize];
    for (offset, byte) in source.bytes().enumerate() {
        if byte == b'\n' {
            line_starts.push(offset + 1);
        }
    }
    let mut bytes = masked.into_bytes();
    for node in nodes.iter().filter(|node| node.kind == NodeKind::Use) {
        let start = line_starts
            .get(node.start_line as usize)
            .and_then(|line| line.checked_add(node.start_column as usize));
        let end = line_starts
            .get(node.end_line as usize)
            .and_then(|line| line.checked_add(node.end_column as usize));
        let Some((start, end)) = start.zip(end) else {
            return Err(TraceDecayError::project_route(
                "verified-unused-imports-evidence-incomplete",
                false,
                "the parser returned an invalid import declaration span",
            ));
        };
        let Some(range) = bytes.get_mut(start..end) else {
            return Err(TraceDecayError::project_route(
                "verified-unused-imports-evidence-incomplete",
                false,
                "the parser returned an import span outside the indexed source",
            ));
        };
        for byte in range {
            if *byte != b'\n' {
                *byte = b' ';
            }
        }
    }
    String::from_utf8(bytes).map_err(|_| {
        TraceDecayError::project_route(
            "verified-unused-imports-evidence-incomplete",
            false,
            "the source mask produced invalid UTF-8",
        )
    })
}

fn identifiers_in_source(source: &str) -> HashSet<String> {
    let mut identifiers = HashSet::new();
    let mut current = String::new();
    for character in source.chars() {
        if character.is_alphanumeric() || character == '_' {
            current.push(character);
        } else if !current.is_empty() {
            identifiers.insert(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        identifiers.insert(current);
    }
    identifiers
}

fn import_name(module: &str, imported: Option<&str>, local: Option<&str>) -> String {
    let imported = imported.unwrap_or_default();
    let mut name = if module.is_empty() {
        imported.to_owned()
    } else if imported.is_empty() {
        module.to_owned()
    } else {
        format!("{module}::{imported}")
    };
    if local != Some(imported)
        && let Some(local) = local
    {
        name.push_str(" as ");
        name.push_str(local);
    }
    name
}

fn incomplete_file(path: &str, reason: &str) -> Value {
    json!({ "file": path, "reason_code": reason })
}

fn reason_code_for(error: &TraceDecayError) -> &'static str {
    error
        .project_route_context()
        .map(|context| context.0)
        .unwrap_or("source_read_unavailable")
}

fn ensure_request_open(context: &tracedecay_contracts::RequestContext) -> Result<()> {
    match context.admission_at(tracedecay_contracts::now_micros()) {
        tracedecay_contracts::RequestAdmission::Admitted => Ok(()),
        tracedecay_contracts::RequestAdmission::Cancelled => Err(TraceDecayError::project_route(
            "code-graph-cancelled",
            false,
            "unused import analysis was cancelled",
        )),
        tracedecay_contracts::RequestAdmission::TimedOut => Err(TraceDecayError::project_route(
            "code-graph-timed-out",
            true,
            "unused import analysis exceeded its admitted deadline",
        )),
    }
}

fn path_is_rust(path: &str) -> bool {
    Path::new(path)
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("rs"))
}

fn is_safe_logical_path(path: &str) -> bool {
    !path.is_empty()
        && !path.starts_with('/')
        && !path.contains('\\')
        && !path.chars().any(char::is_control)
        && path
            .split('/')
            .all(|segment| !segment.is_empty() && !matches!(segment, "." | ".."))
}

#[cfg(test)]
mod tests {
    use super::{
        CURSOR_PREFIX, LanguageRegistry, ScanCursor, SourceScanOutcome, encode_cursor,
        identifiers_in_source, import_name, is_safe_logical_path, parse_cursor, scan_source_file,
    };
    use serde_json::json;

    #[test]
    fn cursor_is_bounded_and_project_relative() {
        assert_eq!(parse_cursor(&json!({})).expect("missing cursor"), None);
        assert_eq!(
            parse_cursor(&json!({ "cursor": "src/lib.rs" })).expect("valid cursor"),
            Some(ScanCursor::AfterPath("src/lib.rs".to_owned()))
        );
        assert!(parse_cursor(&json!({ "cursor": "../lib.rs" })).is_err());
        assert!(parse_cursor(&json!({ "cursor": "/tmp/lib.rs" })).is_err());
        assert!(!is_safe_logical_path("src/../lib.rs"));
        let encoded = encode_cursor("src/lib.rs", 3);
        assert!(encoded.starts_with(CURSOR_PREFIX));
        assert_eq!(
            parse_cursor(&json!({ "cursor": encoded })).expect("valid continuation"),
            Some(ScanCursor::WithinFile {
                path: "src/lib.rs".to_owned(),
                import_offset: 3,
            })
        );
    }

    #[test]
    fn identifier_scan_is_token_based_and_import_names_keep_aliases() {
        let identifiers = identifiers_in_source("HashMap::new() // HashSet\n");
        assert!(identifiers.contains("HashMap"));
        assert!(identifiers.contains("HashSet"));
        assert_eq!(
            import_name("std::collections", Some("HashMap"), Some("Map")),
            "std::collections::HashMap as Map"
        );
    }

    #[test]
    fn parser_scan_reports_only_unreferenced_private_bindings() {
        let source = r#"
use std::collections::BTreeMap;
use std::collections::HashMap as Map;
use std::collections::HashSet;
pub use std::path::Path;
pub(crate) use std::path::PathBuf;
use std::fmt::Display;

fn main() {
    let _map = Map::<String, String>::new();
    println!("HashSet");
}

fn render(value: impl Display) {
    println!("{value}");
}
"#;
        let registry = LanguageRegistry::new();
        let outcome = scan_source_file(&registry, "src/imports.rs", source, 0)
            .expect("parser-backed import scan");
        let SourceScanOutcome::Findings(mut findings) = outcome else {
            panic!("valid Rust source must produce findings");
        };
        findings
            .sort_by_key(|(_, finding)| finding["unused"].as_str().unwrap_or_default().to_owned());
        let unused = findings
            .iter()
            .map(|(_, finding)| finding["unused"].as_str().expect("binding name"))
            .collect::<Vec<_>>();
        assert_eq!(unused, ["BTreeMap", "HashSet"]);
        assert!(
            findings
                .iter()
                .all(|(_, finding)| finding["is_public"] != json!(true))
        );
        let continuation = scan_source_file(&registry, "src/imports.rs", source, 1)
            .expect("parser-backed continuation scan");
        let SourceScanOutcome::Findings(continuation) = continuation else {
            panic!("valid Rust continuation must produce findings");
        };
        assert_eq!(continuation.len(), 1);
        assert_eq!(continuation[0].1["unused"], "HashSet");
    }
}
