//! Compact human-view contract for CLI and MCP presenters.

use super::view::{CanonicalHumanView, HumanFieldValue, PER_CALL_FIELDS};

/// Presentation-ready human output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkdownView {
    text: String,
}

impl MarkdownView {
    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }

    pub fn as_str(&self) -> &str {
        &self.text
    }
}

#[tracing::instrument(name = "mcp.application_output.render", level = "trace", skip_all)]
pub fn render(view: CanonicalHumanView) -> MarkdownView {
    let mut text = format!("## {}\n", escape_text(&view.heading));
    for field in view.fields {
        match field.value {
            HumanFieldValue::Markdown(value) => {
                text.push_str("\n### ");
                text.push_str(field.label);
                text.push_str("\n\n");
                text.push_str(&value);
                if !value.ends_with('\n') {
                    text.push('\n');
                }
            }
            HumanFieldValue::Block(value) => {
                text.push_str("\n### ");
                text.push_str(field.label);
                text.push_str("\n\n");
                for line in value.lines() {
                    text.push_str("    ");
                    text.push_str(line);
                    text.push('\n');
                }
                if value.is_empty() {
                    text.push_str("    \n");
                }
            }
            HumanFieldValue::Code(value) => {
                text.push_str("\n- ");
                text.push_str(field.label);
                text.push_str(": ");
                text.push('`');
                text.push_str(&value.replace('`', "\\`"));
                text.push('`');
            }
            HumanFieldValue::Text(value) => {
                text.push_str("\n- ");
                text.push_str(field.label);
                text.push_str(": ");
                text.push_str(&escape_text(&value));
            }
        }
    }
    MarkdownView::new(text)
}

/// Whether `line` of a rendered view is a per-call field, which differs on
/// every call for the same answer. Payload lines are indented, so no payload
/// line can match.
pub fn is_per_call_line(line: &str) -> bool {
    line.strip_prefix("- ").is_some_and(|field| {
        PER_CALL_FIELDS.iter().any(|label| {
            field
                .strip_prefix(label)
                .is_some_and(|value| value.starts_with(": "))
        })
    })
}

fn escape_text(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace(['\n', '\r'], " ")
        .replace('*', "\\*")
        .replace('_', "\\_")
        .replace('`', "\\`")
}

#[cfg(test)]
mod tests {
    use super::super::view::{
        CanonicalHumanView, HumanField, HumanFieldValue, PROVENANCE_FIELD, RECEIPT_FIELD,
        REQUEST_FIELD, TRACE_FIELD,
    };
    use super::{is_per_call_line, render};

    #[test]
    fn canonical_markdown_golden_formats_only_the_supplied_view() {
        let rendered = render(CanonicalHumanView {
            heading: "feedback_list".to_owned(),
            fields: vec![
                HumanField {
                    label: "Payload",
                    value: HumanFieldValue::Block("src/lib.rs:1-1\nfn main() {}".to_owned()),
                },
                HumanField {
                    label: "Status",
                    value: HumanFieldValue::Code("problem".to_owned()),
                },
                HumanField {
                    label: "Message",
                    value: HumanFieldValue::Text("Retry *after* refresh".to_owned()),
                },
            ],
        });

        assert_eq!(
            rendered.as_str(),
            "## feedback\\_list\n\n### Payload\n\n    src/lib.rs:1-1\n    fn main() {}\n\n- Status: `problem`\n- Message: Retry \\*after\\* refresh"
        );
    }

    #[test]
    fn per_call_lines_are_only_the_per_call_fields() {
        let rendered = render(CanonicalHumanView {
            heading: "source_body".to_owned(),
            fields: vec![
                HumanField {
                    label: "Payload",
                    value: HumanFieldValue::Block("- Provenance: `in the source`".to_owned()),
                },
                HumanField {
                    label: PROVENANCE_FIELD,
                    value: HumanFieldValue::Code("request=r1; elapsed_us=9".to_owned()),
                },
                HumanField {
                    label: RECEIPT_FIELD,
                    value: HumanFieldValue::Code("started=1, ended=2".to_owned()),
                },
                HumanField {
                    label: "Receipt operation",
                    value: HumanFieldValue::Code("read".to_owned()),
                },
                HumanField {
                    label: REQUEST_FIELD,
                    value: HumanFieldValue::Code("r1".to_owned()),
                },
                HumanField {
                    label: TRACE_FIELD,
                    value: HumanFieldValue::Code("t1".to_owned()),
                },
            ],
        });
        let kept = rendered
            .as_str()
            .lines()
            .filter(|line| !is_per_call_line(line))
            .collect::<Vec<_>>();
        assert_eq!(
            kept,
            [
                "## source\\_body",
                "",
                "### Payload",
                "",
                "    - Provenance: `in the source`",
                "",
                "- Receipt operation: `read`",
            ]
        );
    }
}
