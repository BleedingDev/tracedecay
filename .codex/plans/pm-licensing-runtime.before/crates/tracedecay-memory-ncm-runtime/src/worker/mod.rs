//! Single-owner worker loop joining bounded wire requests to [`NcmEngine`].

use crate::engine::{
    CorrectionRequest, EngineReply, FeedbackRequest, MaintenanceKind, MaintenanceRequest,
    NcmEngine, ObserveAffect, ObserveRequest, Outcome, RecallRequest,
};
use crate::ports::Deadline;
use crate::wire::{self, Operation, PROTOCOL_VERSION, Reply, Request};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tracedecay_memory_ncm_core::types::{RecordId, SourceId};

/// Worker-loop controls that are enabled only by the named test-double binary mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServeOptions {
    /// Permit deterministic delay fields used to exercise hard cancellation.
    pub allow_test_delays: bool,
    /// Whether the configured encoder is loaded and ready for text operations.
    pub encoder_ready: bool,
}

impl Default for ServeOptions {
    fn default() -> Self {
        Self {
            allow_test_delays: false,
            encoder_ready: true,
        }
    }
}

/// Serves requests serially until clean input EOF.
///
/// Production callers get no test-only delay hooks. Use [`serve_with_options`]
/// only from the worker binary after it has explicitly admitted test-double mode.
pub fn serve(
    input: impl Read,
    output: impl Write,
    engine: Arc<NcmEngine>,
) -> Result<(), wire::FrameError> {
    serve_with_options(input, output, engine, ServeOptions::default())
}

/// Serves requests serially with explicit worker-loop options.
pub fn serve_with_options(
    mut input: impl Read,
    mut output: impl Write,
    engine: Arc<NcmEngine>,
    options: ServeOptions,
) -> Result<(), wire::FrameError> {
    loop {
        let request = match wire::read_request(&mut input) {
            Ok(Some(request)) => request,
            Ok(None) => return Ok(()),
            Err(error) => {
                let recoverable = error.is_recoverable();
                let reply = Reply::protocol_error(0, error.kind(), error.to_string());
                write_bounded_reply(&mut output, reply)?;
                if !recoverable {
                    return Ok(());
                }
                continue;
            }
        };
        let id = request.id;
        let reply = dispatch(&engine, request, options);
        write_bounded_reply(&mut output, Reply::from_engine(id, reply))?;
    }
}

fn write_bounded_reply(output: &mut impl Write, reply: Reply) -> Result<(), wire::FrameError> {
    match wire::write_reply(output, &reply) {
        Ok(()) => Ok(()),
        Err(wire::FrameError::Oversized { .. }) => wire::write_reply(
            output,
            &Reply::protocol_error(reply.id, "oversized_reply", "reply exceeds 1 MiB"),
        ),
        Err(error) => Err(error),
    }
}

fn dispatch(engine: &NcmEngine, request: Request, options: ServeOptions) -> EngineReply {
    let started = Instant::now();
    if request.protocol_version != PROTOCOL_VERSION {
        return incompatible(&format!(
            "protocol version {} is incompatible with {}",
            request.protocol_version, PROTOCOL_VERSION
        ));
    }
    if request.deadline_ms == 0 {
        return EngineReply {
            outcome: Outcome::Cancelled,
            state_generation: 0,
            payload: Value::Null,
        };
    }
    if options.allow_test_delays {
        apply_delay(&request.payload, "test_sleep_before_ms");
    }
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let remaining_ms = request.deadline_ms.saturating_sub(elapsed_ms);
    if remaining_ms == 0 {
        return EngineReply {
            outcome: Outcome::Cancelled,
            state_generation: 0,
            payload: Value::Null,
        };
    }
    let deadline = Deadline { remaining_ms };
    if let Some(control) = request.payload.get("common_portability") {
        if !matches!(
            request.op,
            Operation::SnapshotExport | Operation::SnapshotRestore | Operation::Replay
        ) || serde_json::to_value(request.op).ok().as_ref() != Some(&control["action"])
        {
            return rejected("common portability operation mismatch".to_owned());
        }
        if request.op == Operation::SnapshotExport {
            let mut reply = crate::snapshot::export_to_file(engine, &request.namespace, deadline);
            if reply.outcome == Outcome::Success {
                reply.payload["common_portability"] = json!("snapshot_export");
            }
            return reply;
        }
        let mut control = control.clone();
        if request.op == Operation::SnapshotRestore && control.get("snapshot_file").is_some() {
            let Some(file) = control["snapshot_file"].as_str() else {
                return rejected("invalid snapshot file".to_owned());
            };
            let Some(length) = control["byte_length"].as_u64() else {
                return rejected("invalid snapshot length".to_owned());
            };
            let Some(digest) = control["content_sha256"].as_str() else {
                return rejected("invalid snapshot digest".to_owned());
            };
            let bytes = match crate::snapshot::read_transport_file(
                &engine.root,
                &request.namespace,
                std::path::Path::new(file),
                length,
                digest,
            ) {
                Ok(bytes) => bytes,
                Err(reason) => return rejected(reason),
            };
            let Some(object) = control.as_object_mut() else {
                return rejected("invalid snapshot control".to_owned());
            };
            object.remove("snapshot_file");
            object.remove("byte_length");
            object.remove("content_sha256");
            object.insert("bytes".to_owned(), json!(bytes));
        }
        let result = engine.common_portability(&request.namespace, control, deadline);
        hold_committed_reply(&request, &result, options);
        return result;
    }
    if let Some(control) = request.payload.get("common_control") {
        if !matches!(
            request.op,
            Operation::Feedback
                | Operation::Correction
                | Operation::Health
                | Operation::Inspection
                | Operation::Maintenance
                | Operation::DeleteBySource
        ) || serde_json::to_value(request.op).ok().as_ref() != Some(&control["action"])
        {
            return rejected("common control operation mismatch".to_owned());
        }
        return engine.common_control(&request.namespace, control.clone(), deadline);
    }
    let result = match request.op {
        Operation::Handshake => {
            if options.encoder_ready {
                dispatch_handshake(engine, &request.namespace, &request.payload)
            } else {
                EngineReply {
                    outcome: Outcome::Unavailable("encoder not ready".to_owned()),
                    state_generation: 0,
                    payload: json!({"process_alive": true, "encoder_ready": false}),
                }
            }
        }
        Operation::Health => health(engine, options.encoder_ready),
        Operation::Observe => {
            parse_payload::<ObservePayload>(&request.payload).map_or_else(rejected, |payload| {
                engine.observe(
                    &request.namespace,
                    ObserveRequest {
                        idempotency_key: payload.idempotency_key,
                        payload_sha256: payload.payload_sha256,
                        source: SourceId(payload.source),
                        key_text: payload.key_text,
                        value_text: payload.value_text,
                        affect: payload.affect,
                        surprise: payload.surprise,
                        intensity: payload.intensity,
                        provenance: payload.provenance,
                        deadline,
                    },
                )
            })
        }
        Operation::Recall => {
            parse_payload::<RecallPayload>(&request.payload).map_or_else(rejected, |payload| {
                let recall = RecallRequest {
                    query_text: payload.query_text,
                    top_k: payload.top_k,
                    deadline,
                };
                match payload.selection {
                    Some(selection) => {
                        engine.recall_selected(&request.namespace, recall, selection)
                    }
                    None => engine.recall(&request.namespace, recall),
                }
            })
        }
        Operation::Feedback => {
            parse_payload::<FeedbackPayload>(&request.payload).map_or_else(rejected, |payload| {
                engine.feedback(
                    &request.namespace,
                    FeedbackRequest {
                        idempotency_key: payload.idempotency_key,
                        record_ids: payload.record_ids.into_iter().map(RecordId).collect(),
                        deadline,
                    },
                )
            })
        }
        Operation::Correction => {
            parse_payload::<CorrectionPayload>(&request.payload).map_or_else(rejected, |payload| {
                engine.correction(
                    &request.namespace,
                    CorrectionRequest {
                        idempotency_key: payload.idempotency_key,
                        superseded: RecordId(payload.superseded),
                        superseding: RecordId(payload.superseding),
                        evidence: payload.evidence,
                        deadline,
                    },
                )
            })
        }
        Operation::Maintenance => parse_payload::<MaintenancePayload>(&request.payload)
            .map_or_else(rejected, |payload| {
                engine.maintenance(
                    &request.namespace,
                    MaintenanceRequest {
                        idempotency_key: payload.idempotency_key,
                        kind: payload.kind,
                        deadline,
                    },
                )
            }),
        Operation::Inspection => engine.inspection(&request.namespace),
        Operation::DeleteBySource => {
            parse_payload::<DeletePayload>(&request.payload).map_or_else(rejected, |payload| {
                engine.delete_by_source(
                    &request.namespace,
                    &SourceId(payload.source),
                    &payload.idempotency_key,
                    deadline,
                )
            })
        }
        Operation::SnapshotExport => {
            crate::snapshot::export_to_file(engine, &request.namespace, deadline)
        }
        Operation::SnapshotRestore => parse_payload::<SnapshotRestorePayload>(&request.payload)
            .map_or_else(rejected, |payload| {
                match (
                    payload.snapshot,
                    payload.snapshot_file,
                    payload.byte_length,
                    payload.content_sha256,
                ) {
                    (Some(snapshot), None, None, None) => engine.snapshot_restore(
                        &request.namespace,
                        &snapshot,
                        &payload.idempotency_key,
                        deadline,
                    ),
                    (None, Some(snapshot_file), Some(byte_length), Some(content_sha256)) => {
                        crate::snapshot::restore_from_file(
                            engine,
                            &request.namespace,
                            &payload.idempotency_key,
                            &snapshot_file,
                            byte_length,
                            &content_sha256,
                            deadline,
                        )
                    }
                    _ => rejected(
                        "snapshot restore requires exactly one complete inline or file transport",
                    ),
                }
            }),
        Operation::Replay => engine.replay(&request.namespace, deadline),
    };
    hold_committed_reply(&request, &result, options);
    result
}

fn hold_committed_reply(request: &Request, result: &EngineReply, options: ServeOptions) {
    let replayed = result
        .payload
        .get("replayed")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if options.allow_test_delays
        && request.op.is_mutating()
        && result.outcome == Outcome::Success
        && !replayed
    {
        apply_delay(&request.payload, "test_sleep_after_commit_ms");
    }
}

fn health(engine: &NcmEngine, encoder_ready: bool) -> EngineReply {
    let mut reply = engine.health();
    if let Some(payload) = reply.payload.as_object_mut() {
        payload.insert("process_alive".to_owned(), Value::Bool(true));
        payload.insert("encoder_ready".to_owned(), Value::Bool(encoder_ready));
    }
    reply
}

fn dispatch_handshake(engine: &NcmEngine, namespace: &str, payload: &Value) -> EngineReply {
    let expected = match parse_payload::<HandshakePayload>(payload) {
        Ok(expected) => expected,
        Err(reason) => return rejected(reason),
    };
    if let Some(version) = expected.protocol_version
        && version != PROTOCOL_VERSION
    {
        return incompatible("handshake protocol identity mismatch");
    }
    let reply = engine.handshake(namespace);
    if reply.outcome != Outcome::Success {
        return reply;
    }
    if let Some(profile) = expected.algorithm_profile.as_deref()
        && reply
            .payload
            .pointer("/algorithm/profile")
            .and_then(Value::as_str)
            != Some(profile)
    {
        return incompatible("handshake algorithm identity mismatch");
    }
    if let Some(model) = expected.model.as_deref()
        && reply
            .payload
            .pointer("/encoder/model")
            .and_then(Value::as_str)
            != Some(model)
    {
        return incompatible("handshake model identity mismatch");
    }
    if let Some(epoch) = expected.epoch
        && reply.payload.get("epoch").and_then(Value::as_u64) != Some(epoch)
    {
        return incompatible("handshake epoch identity mismatch");
    }
    reply
}

fn parse_payload<T: DeserializeOwned>(payload: &Value) -> Result<T, String> {
    serde_json::from_value(payload.clone())
        .map_err(|error| format!("invalid request payload: {error}"))
}

fn rejected(reason: impl Into<String>) -> EngineReply {
    EngineReply {
        outcome: Outcome::Rejected(crate::engine::RejectReason::InvalidRequest(reason.into())),
        state_generation: 0,
        payload: Value::Null,
    }
}

fn incompatible(reason: &str) -> EngineReply {
    EngineReply {
        outcome: Outcome::Incompatible,
        state_generation: 0,
        payload: json!({"reason": reason}),
    }
}

fn apply_delay(payload: &Value, field: &str) {
    if let Some(milliseconds) = payload.get(field).and_then(Value::as_u64) {
        thread::sleep(Duration::from_millis(milliseconds.min(30_000)));
    }
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct HandshakePayload {
    protocol_version: Option<u16>,
    algorithm_profile: Option<String>,
    model: Option<String>,
    epoch: Option<u64>,
}

#[derive(Deserialize)]
struct ObservePayload {
    idempotency_key: String,
    payload_sha256: String,
    source: String,
    key_text: String,
    value_text: String,
    affect: Option<ObserveAffect>,
    surprise: f32,
    intensity: f32,
    #[serde(default)]
    provenance: Value,
}

#[derive(Deserialize)]
struct RecallPayload {
    query_text: String,
    top_k: usize,
    #[serde(default)]
    selection: Option<Value>,
}

#[derive(Deserialize)]
struct FeedbackPayload {
    idempotency_key: String,
    record_ids: Vec<u64>,
}

#[derive(Deserialize)]
struct CorrectionPayload {
    idempotency_key: String,
    superseded: u64,
    superseding: u64,
    evidence: String,
}

#[derive(Deserialize)]
struct MaintenancePayload {
    idempotency_key: String,
    kind: MaintenanceKind,
}

#[derive(Deserialize)]
struct DeletePayload {
    idempotency_key: String,
    source: String,
}

#[derive(Deserialize)]
struct SnapshotRestorePayload {
    idempotency_key: String,
    #[serde(default)]
    snapshot: Option<Vec<u8>>,
    #[serde(default)]
    snapshot_file: Option<PathBuf>,
    #[serde(default)]
    byte_length: Option<u64>,
    #[serde(default)]
    content_sha256: Option<String>,
}
