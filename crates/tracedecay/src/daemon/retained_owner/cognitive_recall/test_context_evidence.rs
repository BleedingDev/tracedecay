//! Read-only context evidence for tests using the actual delivered CLI result.
//!
//! Retained traces describe an earlier recall; they never authorize a control
//! operation. Projection classifies observed output with the production renderer
//! tables and does not reconstruct a historical context pack or its decisions.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, OpenFlags, OptionalExtension, Row, TransactionBehavior, params};
use serde::Serialize;
use serde_json::Value;
use tracedecay_memory_provider_registry::{
    ContextSectionKind, RecallExplainHostDecisionV1, RecallExplainItemV1, RecallExplainTraceV1,
    TerminalCode,
};

/// Existing control/scope types and common-profile policy for test-only callers.
pub use tracedecay_memory_provider_registry::{
    COMMON_ADVISORY_PROFILE_ID, COMMON_ADVISORY_REQUIRED_CAPABILITIES, CancellationToken,
    OperationControl, OwnedExactScope,
};

use super::control_attribution::{
    MAX_DECISION_BYTES, MAX_ID_BYTES, RecallControlAttributionErrorV1, RecallControlTraceRefV1,
    optional_text, read_retained_control_scope_on_connection, required_text,
};
use super::{ADVISORY_CONTEXT_PACK_JSON_KEY, LEDGER_FILE_NAME, PROJECT_RECALL_BUDGETS};

/// All eight finite ceilings copied from a production provider declaration.
/// These become numeric evidence only after the complete fresh-health match.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProviderDeclaredLimitsForTestV1 {
    /// Maximum canonical request bytes.
    pub request_bytes: u64,
    /// Maximum canonical response bytes.
    pub response_bytes: u64,
    /// Maximum observations in one batch.
    pub observation_batch_items: u64,
    /// Maximum recall candidates.
    pub recall_candidates: u64,
    /// Maximum concurrent operations.
    pub concurrent_operations: u64,
    /// Maximum operation duration in milliseconds.
    pub operation_millis: u64,
    /// Maximum snapshot bytes.
    pub snapshot_bytes: u64,
    /// Maximum inspection items.
    pub inspection_items: u64,
}

/// A pure production declaration, carrying no claim about a running provider.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProviderNumericDeclarationForTestV1 {
    /// Actual logical provider ID in the production descriptor.
    pub provider_id: String,
    /// Exact production composition declaration, usable only as a Health request
    /// expectation until the fresh response validates the observed registration.
    pub declared_registration_revision: u64,
    /// Production instance identity expected in a successful health result.
    pub declared_provider_instance_id: String,
    /// Production descriptor's immutable implementation digest.
    pub declared_implementation_identity_digest: String,
    /// Declared numeric ceilings; lower negotiated limits fail the digest match.
    pub declared_limits: ProviderDeclaredLimitsForTestV1,
    /// Production health digest of the complete eight-field declared vector.
    pub declared_limits_digest: String,
}

/// Copies an existing production declaration without constructing provider state.
///
/// A fixture may accept these numeric fields only after validating a fresh actual
/// Health success with Ready state, the actual selected provider and registration,
/// the full delivery scope and its digest, required available capabilities, this
/// exact instance and implementation identity, and equality of the complete
/// eight-field effective-limits digest. Missing or mismatched evidence stays
/// explicitly unavailable; this function does not guess a negotiated minimum.
///
/// The declared registration revision can seed that Health request; it is kept
/// separate from the observed revision until the full response binding succeeds.
/// Live state identity/generation come only from that same health response.
/// Descriptor registration-time generation is deliberately omitted here. Actual
/// executable/build hashes must be captured by the fixture from the running build.
///
/// # Errors
/// Returns an invalid declaration error if the existing production descriptor
/// cannot be produced. Unsupported provider IDs return `Ok(None)`.
pub fn production_provider_numeric_declaration_for_test(
    provider_id: &str,
) -> Result<Option<ProviderNumericDeclarationForTestV1>> {
    let (descriptor, instance, digest) = match provider_id {
        tracedecay_memory_provider_registry::NATIVE_PROVIDER_ID => {
            super::super::native_provider::production_provider_declaration_for_test()
                .map_err(|_| ContextEvidenceReadErrorV1::Invalid("Native production declaration"))?
        }
        tracedecay_memory_provider_ncm::NCM_PROVIDER_ID => {
            tracedecay_memory_provider_ncm::rust_backend::production_provider_declaration_for_test()
                .map_err(|_| ContextEvidenceReadErrorV1::Invalid("NCM production declaration"))?
        }
        _ => return Ok(None),
    };
    let declared_registration_revision =
        crate::daemon::project_composition::declared_project_provider_registration_revision_for_test(
            descriptor.provider_id.as_str(),
        ).ok_or(ContextEvidenceReadErrorV1::Invalid("production registration declaration"))?;
    let limits = descriptor.limits;
    Ok(Some(ProviderNumericDeclarationForTestV1 {
        provider_id: descriptor.provider_id.as_str().to_owned(),
        declared_registration_revision,
        declared_provider_instance_id: instance,
        declared_implementation_identity_digest: descriptor.implementation_identity_sha256,
        declared_limits: ProviderDeclaredLimitsForTestV1 {
            request_bytes: limits.request_bytes,
            response_bytes: limits.response_bytes,
            observation_batch_items: limits.observation_batch_items,
            recall_candidates: limits.recall_candidates,
            concurrent_operations: limits.concurrent_operations,
            operation_millis: limits.operation_millis,
            snapshot_bytes: limits.snapshot_bytes,
            inspection_items: limits.inspection_items,
        },
        declared_limits_digest: digest,
    }))
}

/// A bounded evidence read failed; absence is never converted into an empty trace.
#[derive(Debug, thiserror::Error)]
pub enum ContextEvidenceReadErrorV1 {
    /// The caller's original deadline or cancellation stopped the read.
    #[error("context evidence read stopped: {0:?}")]
    Stopped(TerminalCode),
    /// No matching retained trace or retained full scope exists.
    #[error("context evidence was not retained")]
    Missing,
    /// Retained bytes or caller bindings do not reconcile.
    #[error("invalid context evidence: {0}")]
    Invalid(&'static str),
    /// SQLite refused the read-only open or bounded query.
    #[error("context evidence storage read failed: {0}")]
    Storage(#[from] rusqlite::Error),
    /// A bounded retained JSON value is malformed.
    #[error("context evidence JSON could not be decoded: {0}")]
    Decode(#[from] serde_json::Error),
}

type Result<T> = std::result::Result<T, ContextEvidenceReadErrorV1>;

impl From<RecallControlAttributionErrorV1> for ContextEvidenceReadErrorV1 {
    fn from(error: RecallControlAttributionErrorV1) -> Self {
        match error {
            RecallControlAttributionErrorV1::Control(code) => Self::Stopped(code),
            RecallControlAttributionErrorV1::NotFound
            | RecallControlAttributionErrorV1::MissingAuthority => Self::Missing,
            RecallControlAttributionErrorV1::Invalid(reason) => Self::Invalid(reason),
            RecallControlAttributionErrorV1::Sqlite(error) => Self::Storage(error),
            RecallControlAttributionErrorV1::Json(error) => Self::Decode(error),
        }
    }
}

fn check_control(control: &OperationControl) -> Result<()> {
    control
        .snapshot()
        .map(|_| ())
        .map_err(ContextEvidenceReadErrorV1::Stopped)
}

/// Reads one actual retained trace from the canonical store data root.
///
/// Uses a genuine SQLite READ_ONLY connection and one ordinary deferred read
/// transaction, so it can observe a committed WAL trace while the daemon runs.
/// It never creates a ledger, initializes schema, migrates, checkpoints, copies
/// files, or runs PRAGMA statements. Execute on the caller's blocking pool with
/// its original operation control. Every SQL read checks that same control.
///
/// All expected bindings must come from observed call/registration/scope
/// evidence. Requested configuration pins alone are not observed evidence.
///
/// # Errors
/// Returns missing, stopped, or invalid evidence without inventing an empty
/// result or falling back to another provider, request, scope, or trace.
#[allow(clippy::too_many_arguments)]
pub fn read_retained_context_trace_for_test(
    store_data_root: &Path,
    trace_ref: &str,
    expected_request_id: &str,
    expected_provider_id: &str,
    expected_registration_revision: u64,
    expected_delivery_scope: &OwnedExactScope,
    control: &OperationControl,
) -> Result<RecallExplainTraceV1> {
    check_control(control)?;
    let reference = RecallControlTraceRefV1::parse(trace_ref)?;
    expected_delivery_scope
        .validate()
        .map_err(|_| ContextEvidenceReadErrorV1::Invalid("expected full scope"))?;
    if expected_request_id.is_empty()
        || expected_request_id.len() > MAX_ID_BYTES
        || expected_request_id.trim() != expected_request_id
        || expected_registration_revision == 0
    {
        return Err(ContextEvidenceReadErrorV1::Invalid(
            "expected request or registration",
        ));
    }
    let path = store_data_root.join(LEDGER_FILE_NAME);
    let mut connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let remaining = control
        .snapshot()
        .map_err(ContextEvidenceReadErrorV1::Stopped)?;
    // sqlite3_busy_timeout changes only this connection's handler, not storage.
    connection.busy_timeout(Duration::from_millis(remaining.remaining_millis.min(10)))?;
    check_control(control)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
    let scope = read_retained_control_scope_on_connection(&transaction, &reference, control)?;
    if scope.provider_id.as_str() != expected_provider_id
        || scope.registration_revision != expected_registration_revision
        || &scope.delivery_scope != expected_delivery_scope
    {
        return Err(ContextEvidenceReadErrorV1::Invalid(
            "retained provider, registration, or full scope mismatch",
        ));
    }
    let scope_sha256 = expected_delivery_scope.exact_scope_sha256();
    check_control(control)?;
    let header = transaction
        .query_row(
            "SELECT substr(CAST(request_id AS BLOB),1,1025), requested_count, degraded,
                substr(CAST(token_summary_json AS BLOB),1,16385),
                substr(CAST(trace_sha256 AS BLOB),1,65)
         FROM recall_explain_traces WHERE exact_scope_sha256=?1 AND trace_id=?2 LIMIT 1",
            params![scope_sha256, reference.trace_id()],
            |row| Ok(decode_header(row)),
        )
        .optional();
    check_control(control)?;
    let (request_id, requested_count, degraded, token_summary_json, trace_sha256) =
        header?.ok_or(ContextEvidenceReadErrorV1::Missing)??;
    if request_id != expected_request_id {
        return Err(ContextEvidenceReadErrorV1::Invalid(
            "retained request mismatch",
        ));
    }
    // This is the actual provider-reply admission cap, including denied rows:
    // PROJECT_RECALL_BUDGETS -> admitted_budget -> admit_recall_reply_with_profile.
    // PreparedRecallControlMetadataV1 also checks this cap before retention.
    // The smaller final context-pack selection size is not a trace row cap.
    let maximum_items = usize::try_from(PROJECT_RECALL_BUDGETS.maximum_candidates)
        .map_err(|_| ContextEvidenceReadErrorV1::Invalid("producer row bound"))?;
    if requested_count > maximum_items {
        return Err(ContextEvidenceReadErrorV1::Invalid("trace row bound"));
    }
    check_control(control)?;
    let mut statement = transaction.prepare(
        "SELECT provider_rank, substr(CAST(candidate_id AS BLOB),1,1025),
                substr(CAST(stage AS BLOB),1,65), substr(CAST(host_reason_code AS BLOB),1,1025),
                substr(CAST(host_reason_detail AS BLOB),1,16385),
                substr(CAST(host_decision_json AS BLOB),1,16385),
                substr(CAST(provider_explanation_json AS BLOB),1,16385),
                substr(CAST(section AS BLOB),1,1025), tokens
         FROM recall_explain_trace_items WHERE exact_scope_sha256=?1 AND trace_id=?2
         ORDER BY provider_rank ASC LIMIT ?3",
    )?;
    let mut rows = statement.query(params![
        scope_sha256,
        reference.trace_id(),
        (maximum_items + 1) as i64
    ])?;
    check_control(control)?;
    let mut items = Vec::new();
    let mut candidate_ids = BTreeSet::new();
    loop {
        check_control(control)?;
        let row = rows.next();
        check_control(control)?;
        let Some(row) = row? else {
            break;
        };
        if items.len() >= maximum_items {
            return Err(ContextEvidenceReadErrorV1::Invalid("trace row overflow"));
        }
        let item = decode_item(row)?;
        if item.provider_rank != items.len() || !candidate_ids.insert(item.candidate_id.clone()) {
            return Err(ContextEvidenceReadErrorV1::Invalid(
                "trace rank or candidate partition",
            ));
        }
        items.push(item);
    }
    if items.len() != requested_count {
        return Err(ContextEvidenceReadErrorV1::Invalid(
            "incomplete trace partition",
        ));
    }
    let trace = RecallExplainTraceV1 {
        trace_id: reference.trace_id().to_owned(),
        request_id,
        provider_id: scope.provider_id.as_str().to_owned(),
        registration_revision: scope.registration_revision,
        requested_count,
        degraded,
        items,
        token_summary: token_summary_json
            .map(|json| serde_json::from_str(&json))
            .transpose()?,
    };
    check_control(control)?;
    let actual_sha256 = tracedecay_domain::canonical_text::sha256_hex(&serde_json::to_vec(&trace)?);
    if trace_sha256 != actual_sha256 {
        return Err(ContextEvidenceReadErrorV1::Invalid(
            "retained trace digest mismatch",
        ));
    }
    check_control(control)?;
    // Dropping the read transaction releases its snapshot; no commit/write path.
    Ok(trace)
}

type TraceHeader = (String, usize, bool, Option<String>, String);

fn decode_header(row: &Row<'_>) -> Result<TraceHeader> {
    let count: i64 = row.get(1)?;
    let degraded: i64 = row.get(2)?;
    if !matches!(degraded, 0 | 1) {
        return Err(ContextEvidenceReadErrorV1::Invalid(
            "retained degraded state",
        ));
    }
    Ok((
        required_text(row, 0, MAX_ID_BYTES)?,
        usize::try_from(count)
            .map_err(|_| ContextEvidenceReadErrorV1::Invalid("retained requested count"))?,
        degraded == 1,
        optional_text(row, 3, MAX_DECISION_BYTES)?,
        required_text(row, 4, 64)?,
    ))
}

fn decode_item(row: &Row<'_>) -> Result<RecallExplainItemV1> {
    let rank: i64 = row.get(0)?;
    let tokens: Option<i64> = row.get(8)?;
    let host_decision: RecallExplainHostDecisionV1 =
        serde_json::from_str(&required_text(row, 5, MAX_DECISION_BYTES)?)?;
    let stage = host_decision.stage();
    if required_text(row, 2, 64)? != stage.label() {
        return Err(ContextEvidenceReadErrorV1::Invalid(
            "retained stage and decision mismatch",
        ));
    }
    Ok(RecallExplainItemV1 {
        candidate_id: required_text(row, 1, MAX_ID_BYTES)?,
        provider_rank: usize::try_from(rank)
            .map_err(|_| ContextEvidenceReadErrorV1::Invalid("retained provider rank"))?,
        stage,
        host_reason_code: required_text(row, 3, MAX_ID_BYTES)?,
        host_reason_detail: optional_text(row, 4, MAX_DECISION_BYTES)?,
        host_decision,
        provider_explanation: serde_json::from_str(&required_text(row, 6, MAX_DECISION_BYTES)?)?,
        section: optional_text(row, 7, MAX_ID_BYTES)?,
        tokens: tokens
            .map(|value| {
                u64::try_from(value)
                    .map_err(|_| ContextEvidenceReadErrorV1::Invalid("retained tokens"))
            })
            .transpose()?,
    })
}

/// Every actual text block, including completion warnings and metrics.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DeliveredContextTextBlockV1 {
    /// Index in the original result's `content` array.
    pub content_index: usize,
    /// Exact original decoded text; no blocks are removed or reordered.
    pub text: String,
}

/// Production renderer classification of an observed payload member or range.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DeliveredHostEvidenceProjectionV1 {
    /// A semantic JSON member boundary; it makes no byte-offset claim.
    JsonMember {
        /// JSON pointer inside the selected decoded payload.
        pointer: String,
        /// Original decoded member value.
        value: Value,
        /// Existing production host section classification.
        section: ContextSectionKind,
        /// Existing production host authority label.
        authority: String,
    },
    /// Exact byte range in the selected decoded Markdown string.
    MarkdownRange {
        /// Inclusive byte offset.
        start: usize,
        /// Exclusive byte offset.
        end: usize,
        /// Exact original substring.
        text: String,
        /// Existing production host section classification.
        section: ContextSectionKind,
        /// Existing production host authority label.
        authority: String,
    },
}

/// Actual advisory output, kept independently of the enclosing CLI error state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DeliveredAdvisoryProjectionV1 {
    /// The complete observed object includes state/reason/candidates and any
    /// actual correlation fields; no terminal or success state is inferred.
    JsonMember {
        /// JSON pointer inside the selected decoded payload.
        pointer: String,
        /// Exact decoded advisory member, including unknown/future fields.
        value: Value,
    },
    /// Actual Markdown advisory suffix, including its receipt and metadata.
    MarkdownRange {
        /// Inclusive byte offset in the selected text.
        start: usize,
        /// Exclusive byte offset in the selected text.
        end: usize,
        /// Exact original substring. This is output, not a reconstructed ledger.
        text: String,
    },
}

/// Lossless output plus explicit renderer projections for one observed payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DeliveredContextProjectionV1 {
    /// Complete original parsed result, preserving all blocks and metadata.
    pub result: Value,
    /// Every delivered text block in its actual order, for full-input accounting.
    pub text_blocks: Vec<DeliveredContextTextBlockV1>,
    /// Caller-established actual payload block, after any prepended warnings.
    pub payload_content_index: usize,
    /// Existing host renderer classifications; these are not historical decisions.
    pub host_evidence: Vec<DeliveredHostEvidenceProjectionV1>,
    /// Actual advisory output; absence does not imply recall success or no results.
    pub advisory: Option<DeliveredAdvisoryProjectionV1>,
}

/// Projects an observed final result using a caller-established payload index.
///
/// Completion can prepend warnings after context packing, so this function never
/// guesses the payload index. The caller must retain the raw stdout separately
/// if original JSON byte encoding is needed: `Value` preserves decoded values,
/// not original object whitespace or byte offsets. All text blocks remain intact
/// for the caller's complete model-input accounting, including warnings/metrics.
///
/// # Errors
/// Rejects invalid/nontext selected blocks or ambiguous Markdown boundaries.
pub fn project_delivered_context_for_test(
    result: &Value,
    payload_content_index: usize,
) -> Result<DeliveredContextProjectionV1> {
    let content = result
        .get("content")
        .and_then(Value::as_array)
        .ok_or(ContextEvidenceReadErrorV1::Invalid("result content array"))?;
    let text_blocks = content
        .iter()
        .enumerate()
        .filter_map(|(content_index, block)| {
            (block.get("type").and_then(Value::as_str) == Some("text"))
                .then(|| block.get("text").and_then(Value::as_str))
                .flatten()
                .map(|text| DeliveredContextTextBlockV1 {
                    content_index,
                    text: text.to_owned(),
                })
        })
        .collect::<Vec<_>>();
    let text = text_blocks
        .iter()
        .find(|block| block.content_index == payload_content_index)
        .map(|block| block.text.as_str())
        .ok_or(ContextEvidenceReadErrorV1::Invalid(
            "selected payload is not a text block",
        ))?;
    let mut host_evidence = Vec::new();
    let advisory;
    if let Ok(Value::Object(members)) = serde_json::from_str::<Value>(text) {
        advisory = members.get(ADVISORY_CONTEXT_PACK_JSON_KEY).map(|value| {
            DeliveredAdvisoryProjectionV1::JsonMember {
                pointer: format!("/{ADVISORY_CONTEXT_PACK_JSON_KEY}"),
                value: value.clone(),
            }
        });
        for (key, value) in members {
            if key == ADVISORY_CONTEXT_PACK_JSON_KEY {
                continue;
            }
            let (section, authority) = super::json_member_evidence(&key);
            host_evidence.push(DeliveredHostEvidenceProjectionV1::JsonMember {
                pointer: format!("/{}", key.replace('~', "~0").replace('/', "~1")),
                value,
                section,
                authority: authority.to_owned(),
            });
        }
    } else {
        // Exact current production renderer delimiter. A duplicate is ambiguous
        // observed output, so no guessed boundary is returned.
        const ADVISORY_HEADING: &str = "\n### Provider memory (advisory)\n";
        let starts = text
            .match_indices(ADVISORY_HEADING)
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        if starts.len() > 1 {
            return Err(ContextEvidenceReadErrorV1::Invalid(
                "ambiguous Markdown advisory boundary",
            ));
        }
        let host_end = starts.first().copied().unwrap_or(text.len());
        advisory = starts
            .first()
            .map(|start| DeliveredAdvisoryProjectionV1::MarkdownRange {
                start: *start,
                end: text.len(),
                text: text[*start..].to_owned(),
            });
        let mut start = 0;
        for item in super::markdown_evidence(&text[..host_end]) {
            let end = start + item.content.len();
            if text.get(start..end) != Some(item.content.as_str()) {
                return Err(ContextEvidenceReadErrorV1::Invalid(
                    "Markdown renderer range mismatch",
                ));
            }
            host_evidence.push(DeliveredHostEvidenceProjectionV1::MarkdownRange {
                start,
                end,
                text: item.content,
                section: item.section,
                authority: item.authority,
            });
            start = end;
        }
        if start != host_end {
            return Err(ContextEvidenceReadErrorV1::Invalid(
                "incomplete Markdown projection",
            ));
        }
    }
    Ok(DeliveredContextProjectionV1 {
        result: result.clone(),
        text_blocks,
        payload_content_index,
        host_evidence,
        advisory,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn selected_payload_projection_preserves_every_warning_and_actual_advisory_state() {
        let payload = json!({"memory": [{"fact_id": "fact-actual"}], "warnings": ["coverage"],
            "a/b~c": "exact value", "advisory_provider_memory": {"state": "unavailable", "reason": "provider_unavailable", "future_field": [1, 2]}});
        let result = json!({"isError": false, "content": [
            {"type": "text", "text": "version warning before payload"},
            {"type": "text", "text": payload.to_string()},
            {"type": "image", "data": "unchanged"},
            {"type": "text", "text": "metrics after payload"}
        ]});
        let projection = project_delivered_context_for_test(&result, 1).unwrap();
        assert_eq!(projection.result, result);
        assert_eq!(
            projection
                .text_blocks
                .iter()
                .map(|block| block.content_index)
                .collect::<Vec<_>>(),
            [0, 1, 3]
        );
        assert_eq!(
            projection.text_blocks[0].text,
            "version warning before payload"
        );
        assert_eq!(projection.text_blocks[2].text, "metrics after payload");
        let Some(DeliveredAdvisoryProjectionV1::JsonMember { value, .. }) = projection.advisory
        else {
            panic!("actual advisory");
        };
        assert_eq!(value, payload[ADVISORY_CONTEXT_PACK_JSON_KEY]);
        assert_eq!(value["state"], "unavailable");
        assert!(projection.host_evidence.iter().any(|item| matches!(item,
            DeliveredHostEvidenceProjectionV1::JsonMember { pointer, .. } if pointer == "/a~1b~0c")));
        assert!(projection.host_evidence.iter().any(|item| matches!(item,
            DeliveredHostEvidenceProjectionV1::JsonMember { pointer, section: ContextSectionKind::NativeFacts, .. } if pointer == "/memory")));
        for index in [2, 4] {
            assert!(project_delivered_context_for_test(&result, index).is_err());
        }
    }

    #[test]
    fn markdown_ranges_reassemble_actual_text_without_reclassifying_completion_blocks() {
        let host = "## Code Context\nactual code é\n### Memory Matches\nactual fact\n";
        let lane = super::super::AdvisoryMemoryContextV1::Answered {
            provider_id: "provider.native".to_owned(),
            registration_revision: 31,
            degradation: None,
            candidates: Vec::new(),
            explain: None,
        };
        let delivered = lane.appended_to(super::super::ToolResult::new(
            json!({"content": [{"type": "text", "text": host}]}),
            Vec::new(),
        ));
        let text = delivered.value["content"][0]["text"].as_str().unwrap();
        let result = json!({"content": [{"type": "text", "text": "warning"}, {"type": "text", "text": text}]});
        let projection = project_delivered_context_for_test(&result, 1).unwrap();
        let mut rebuilt = String::new();
        for item in &projection.host_evidence {
            let DeliveredHostEvidenceProjectionV1::MarkdownRange {
                start,
                end,
                text: piece,
                ..
            } = item
            else {
                panic!("Markdown");
            };
            assert_eq!(&text[*start..*end], piece);
            assert_eq!(*start, rebuilt.len());
            rebuilt.push_str(piece);
        }
        let Some(DeliveredAdvisoryProjectionV1::MarkdownRange {
            start,
            end,
            text: piece,
        }) = projection.advisory
        else {
            panic!("advisory suffix");
        };
        assert_eq!(&text[start..end], piece);
        assert_eq!(start, rebuilt.len());
        rebuilt.push_str(&piece);
        assert_eq!(rebuilt, text);
        assert_eq!(projection.result, result);
    }
}
