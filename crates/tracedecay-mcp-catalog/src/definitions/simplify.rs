//! Composite V2 analysis definition for simplification reviews.

use serde_json::json;

use super::{def, number_property};
use crate::ToolDefinition;

/// Advertises the bounded V2 composite over the admitted analysis reports.
pub(super) fn def_simplify_scan() -> ToolDefinition {
    def(
        "tracedecay_simplify_scan",
        "Simplify Scan",
        "Run one bounded V2 analysis pass over changed files. Combines verified dead-code, complexity, and fan-in reports. Each report carries an explicit complete, partial, or unavailable status; clone similarity is unavailable until its canonical authority is admitted.",
        json!({
            "type": "object",
            "properties": {
                "files": {
                    "type": "array",
                    "items": { "type": "string", "minLength": 1 },
                    "minItems": 1,
                    "maxItems": 64,
                    "description": "Logical project-relative file paths to analyze."
                },
                "limit": number_property("Maximum findings returned per report (default: 25, max: 100)."),
                "complexity_threshold": number_property("Warn when the verified complexity score exceeds this value (default: 100, max: 1000000)."),
                "coupling_threshold": number_property("Warn when a file has more than this many distinct in-scope dependent files (default: 15, max: 10000).")
            },
            "required": ["files"],
            "additionalProperties": false
        }),
    )
}
