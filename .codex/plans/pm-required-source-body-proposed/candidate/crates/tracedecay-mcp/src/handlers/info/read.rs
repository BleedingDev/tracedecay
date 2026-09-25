//! `tracedecay_read` — mode-aware file read with cross-session cache.

use crate::ToolResult;
use crate::{rendered_tool_result, text_tool_result};
use crate::tools::render::{self, Md};
use serde::Serialize;
use serde_json::{Value, json};
use tracedecay_contracts::retrieval::SourceReadBodyPolicyV1;
use tracedecay_domain::errors::{Result, TraceDecayError};
use tracedecay_graph_query::VerifiedGraphQuery;
use tracedecay_graph_query::context::read_modes::{LineRange, ReadMode};
use tracedecay_graph_query::context::source_read::SourceReadRequest;

use super::{enrich_markdown_sections, render_section_md};

#[hotpath::measure(future = true, label = "mcp.info.read.total")]
pub async fn handle_read(graph: &VerifiedGraphQuery, args: Value) -> Result<ToolResult> {
    let file =
        args.get("file")
            .and_then(|v| v.as_str())
            .ok_or_else(|| TraceDecayError::Config {
                message: "missing required parameter: file".to_string(),
            })?;

    let mode_str = args.get("mode").and_then(|v| v.as_str()).unwrap_or("full");
    let mode = ReadMode::parse(mode_str).ok_or_else(|| TraceDecayError::Config {
        message: format!("unknown mode '{mode_str}'; expected one of full, lines, map, signatures"),
    })?;
    let body_policy = parse_body_policy(&args, mode)?;
    let include_symbols = args
        .get("include_symbols")
        .and_then(Value::as_bool)
        .unwrap_or(mode == ReadMode::Lines);

    let line_range = if mode == ReadMode::Lines {
        let raw =
            args.get("lines")
                .and_then(|v| v.as_str())
                .ok_or_else(|| TraceDecayError::Config {
                    message: "mode='lines' requires the 'lines' argument (e.g. '120-180')"
                        .to_string(),
                })?;
        Some(
            LineRange::parse(raw).ok_or_else(|| TraceDecayError::Config {
                message: format!("invalid 'lines' value '{raw}'; expected 'A' or 'A-B'"),
            })?,
        )
    } else {
        None
    };

    let project_root = graph.project_root()?.to_path_buf();
    let project_id = graph.project_id()?.to_owned();
    // The source-read future carries the whole read pipeline's state; boxing
    // it keeps this handler's own future small.
    let output = hotpath::future!(
        Box::pin(graph.read_source(SourceReadRequest {
            file,
            mode,
            body_policy,
            line_range,
            raw_lines: args.get("lines").and_then(Value::as_str),
            include_symbols,
            project_id: &project_id,
        })),
        label = "mcp.info.read.source"
    )
    .await?;
    let display_file = output.file;
    let mut payload = json!({
        "file": &display_file,
        "mode": output.mode.as_str(),
        "mtime_ns": output.mtime_ns,
        "digest": output.digest,
        "token_count": output.token_count,
    });
    if output.unchanged {
        payload["unchanged"] = Value::Bool(true);
    }
    if let Some(body) = output.body {
        payload["body"] = Value::String(body);
    }
    if let Some(mut context) = output.context {
        // `display_file` is the repository-relative logical path the read
        // resolved to, so this is the same file the symbol context describes.
        enrich_markdown_sections(
            &project_root,
            &project_root.join(&display_file),
            &display_file,
            &mut context,
        );
        payload["context"] = context;
    }
    if body_policy == SourceReadBodyPolicyV1::Required {
        return Ok(required_read_tool_result(&args, &payload, vec![display_file]));
    }
    Ok(rendered_tool_result(
        Some(&project_root),
        &args,
        &payload,
        vec![display_file],
        || render_read_md(&payload),
    ))
}

fn parse_body_policy(args: &Value, mode: ReadMode) -> Result<SourceReadBodyPolicyV1> {
    let policy = args
        .get("body_policy")
        .map(|value| serde_json::from_value::<SourceReadBodyPolicyV1>(value.clone()))
        .transpose()
        .map_err(|error| TraceDecayError::Config {
            message: format!("invalid body_policy; expected if_changed or required: {error}"),
        })?
        .unwrap_or_default();
    if policy == SourceReadBodyPolicyV1::Required
        && !matches!(mode, ReadMode::Full | ReadMode::Lines)
    {
        return Err(TraceDecayError::Config {
            message: "body_policy='required' is supported only for full and lines reads".to_owned(),
        });
    }
    Ok(policy)
}

#[derive(Serialize)]
struct SourceReadCapacityRefusal {
    success: bool,
    reason_code: &'static str,
    limit_utf8_bytes: usize,
    rendered_utf8_bytes: usize,
}

fn required_read_tool_result(
    args: &Value,
    payload: &Value,
    touched_files: Vec<String>,
) -> ToolResult {
    // Render the complete result once. Required reads never call the shared
    // truncating finalizer or create a response handle.
    let rendered = if render::wants_json(args) {
        payload.to_string()
    } else {
        render_read_md(payload)
    };
    let rendered_utf8_bytes = rendered.len();
    if rendered_utf8_bytes <= crate::tools::MAX_RESPONSE_CHARS {
        return text_tool_result(&rendered, touched_files).with_semantic_error(false);
    }
    let refusal = json!(SourceReadCapacityRefusal {
        success: false,
        reason_code: "source_read_capacity_exceeded",
        limit_utf8_bytes: crate::tools::MAX_RESPONSE_CHARS,
        rendered_utf8_bytes,
    });
    let rendered_refusal = if render::wants_json(args) {
        refusal.to_string()
    } else {
        render::generic_md(&refusal)
    };
    text_tool_result(&rendered_refusal, touched_files)
        .with_semantic_error(true)
        .with_failure_message("source_read_capacity_exceeded")
}

fn render_read_md(value: &Value) -> String {
    let mut md = Md::new();
    let file = render::field_str(value, "file");
    let mode = render::field_str(value, "mode");
    md.heading(2, &format!("{file} ({mode})"));
    if value
        .get("unchanged")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        md.field("unchanged", "true");
        let digest = render::field_str(value, "digest");
        if !digest.is_empty() {
            md.field("digest", digest);
        }
    }
    md.field(
        "tokens",
        &render::field_i64(value, "token_count").to_string(),
    );
    render_read_context_md(&mut md, value.get("context"));
    if value.get("body").is_none() {
        return md.render();
    }
    md.blank();
    let lang = file.rsplit_once('.').map_or("", |(_, ext)| ext);
    md.code(lang, render::field_str(value, "body"));
    md.render()
}

fn render_read_context_md(md: &mut Md, context: Option<&Value>) {
    let Some(context) = context else {
        return;
    };
    let Some(symbols) = context.get("symbols").and_then(Value::as_array) else {
        return;
    };
    if symbols.is_empty() {
        return;
    }

    md.blank();
    md.heading(3, "Context");
    let symbol_count = context
        .get("symbol_count")
        .and_then(Value::as_u64)
        .unwrap_or(symbols.len() as u64);
    md.field("symbols", &symbol_count.to_string());
    for symbol in symbols {
        let kind = render::field_str(symbol, "kind");
        let name = render::field_str(symbol, "name");
        let line = render::field_i64(symbol, "line");
        let end_line = render::field_i64(symbol, "end_line");
        let signature = render::field_str(symbol, "signature");
        let span = if end_line > line {
            format!("{line}-{end_line}")
        } else {
            line.to_string()
        };
        if signature.is_empty() {
            md.bullet(&format!("{kind} {name} {span}"));
        } else {
            md.bullet(&format!("{kind} {name} {span}: `{signature}`"));
        }
        render_section_md(md, symbol.get("section"));
    }
    if context
        .get("truncated")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        md.empty_note("symbol list truncated");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::MAX_RESPONSE_CHARS;

    fn payload(body: &str) -> Value {
        json!({
            "file": "src/lib.rs", "mode": "full", "mtime_ns": 7,
            "digest": "fixture-digest", "token_count": 1, "body": body,
        })
    }

    fn text(result: &ToolResult) -> &str {
        result.value["content"][0]["text"].as_str().expect("text carrier")
    }

    fn rendered(args: &Value, payload: &Value) -> String {
        if render::wants_json(args) { payload.to_string() } else { render_read_md(payload) }
    }

    fn assert_capacity_refusal(args: &Value, payload: &Value) {
        let rendered_utf8_bytes = rendered(args, payload).len();
        assert!(rendered_utf8_bytes > MAX_RESPONSE_CHARS);
        let mut result = required_read_tool_result(args, payload, vec!["src/lib.rs".to_owned()]);
        assert_eq!(result.semantic_error(), Some(true));
        crate::tool_errors::mark_semantic_tool_error(&mut result);
        assert_eq!(result.value["isError"], true);
        let response = text(&result);
        assert!(response.len() <= MAX_RESPONSE_CHARS);
        if render::wants_json(args) {
            let refusal: Value = serde_json::from_str(response).expect("refusal JSON");
            assert_eq!(refusal, json!({
                "success": false,
                "reason_code": "source_read_capacity_exceeded",
                "limit_utf8_bytes": MAX_RESPONSE_CHARS,
                "rendered_utf8_bytes": rendered_utf8_bytes,
            }));
        } else {
            assert!(response.contains("source_read_capacity_exceeded"));
            assert!(response.contains("limit_utf8_bytes"));
            assert!(response.contains(&MAX_RESPONSE_CHARS.to_string()));
            assert!(response.contains("rendered_utf8_bytes"));
            assert!(response.contains(&rendered_utf8_bytes.to_string()));
        }
        for forbidden in ["fixture-digest", "src/lib.rs", "body", "preview", "handle"] {
            assert!(!response.contains(forbidden), "refusal leaked {forbidden}");
        }
    }

    #[test]
    fn source_read_body_policy_parser_preserves_default_and_refuses_bad_inputs() {
        assert_eq!(parse_body_policy(&json!({}), ReadMode::Full).unwrap(), SourceReadBodyPolicyV1::IfChanged);
        for mode in [ReadMode::Full, ReadMode::Lines, ReadMode::Map, ReadMode::Signatures] {
            assert_eq!(parse_body_policy(&json!({"body_policy": "if_changed"}), mode).unwrap(), SourceReadBodyPolicyV1::IfChanged);
            assert_eq!(parse_body_policy(&json!({"body_policy": "required"}), mode).is_ok(), matches!(mode, ReadMode::Full | ReadMode::Lines));
        }
        for value in [json!(null), json!("always"), json!(1), json!(true), json!([]), json!({})] {
            assert!(parse_body_policy(&json!({"body_policy": value}), ReadMode::Full).is_err());
        }
    }

    #[test]
    fn required_source_read_returns_complete_render_at_or_below_byte_limit() {
        for args in [json!({"format": "json"}), json!({"format": "markdown"})] {
            for body in ["", "pub fn complete() {}", "é\n\"quoted\""] {
                let payload = payload(body);
                let expected = rendered(&args, &payload);
                let mut result = required_read_tool_result(&args, &payload, vec![]);
                assert_eq!(text(&result), expected);
                assert_eq!(result.semantic_error(), Some(false));
                crate::tool_errors::mark_semantic_tool_error(&mut result);
                assert!(result.value.get("isError").is_none());
            }
            let overhead = rendered(&args, &payload("")).len();
            let exact = payload(&"x".repeat(MAX_RESPONSE_CHARS - overhead));
            assert_eq!(rendered(&args, &exact).len(), MAX_RESPONSE_CHARS);
            let result = required_read_tool_result(&args, &exact, vec![]);
            assert_eq!(text(&result), rendered(&args, &exact));
            assert_eq!(result.semantic_error(), Some(false));
            let oversized = payload(&"x".repeat(MAX_RESPONSE_CHARS - overhead + 1));
            assert_capacity_refusal(&args, &oversized);
        }
    }

    #[test]
    fn required_source_read_capacity_counts_utf8_and_json_escaping() {
        let utf8 = payload(&"é".repeat(MAX_RESPONSE_CHARS / 2));
        assert!(utf8["body"].as_str().unwrap().chars().count() < MAX_RESPONSE_CHARS);
        for args in [json!({"format": "json"}), json!({"format": "markdown"})] {
            assert_capacity_refusal(&args, &utf8);
        }
        let escaped = payload(&"\"".repeat(MAX_RESPONSE_CHARS / 2));
        assert!(escaped["body"].as_str().unwrap().len() < MAX_RESPONSE_CHARS);
        assert_capacity_refusal(&json!({"format": "json"}), &escaped);
        let markdown = json!({"format": "markdown"});
        let result = required_read_tool_result(&markdown, &escaped, vec![]);
        assert_eq!(result.semantic_error(), Some(false));
        assert_eq!(text(&result), render_read_md(&escaped));
    }
}
