//! Single-owner worker loop joining bounded wire requests to [`NcmEngine`].

use crate::engine::{
    CorrectionRequest, EngineReply, FeedbackRequest, NcmEngine, ObserveAffect, ObserveRequest,
    Outcome, RecallRequest,
};
use crate::ports::Deadline;
use crate::wire::{self, Operation, PROTOCOL_IDENTITY, PROTOCOL_VERSION, Reply, Request};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
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
    /// Keep the sparse V1 ready shape for an explicitly admitted test double.
    /// Production workers always emit and validate complete V2 identity.
    pub allow_legacy_identity: bool,
}

impl Default for ServeOptions {
    fn default() -> Self {
        Self {
            allow_test_delays: false,
            encoder_ready: true,
            allow_legacy_identity: false,
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
        let result = engine.common_control(&request.namespace, control.clone(), deadline);
        hold_committed_reply(&request, &result, options);
        return result;
    }
    let result = match request.op {
        Operation::Handshake => {
            if options.encoder_ready {
                dispatch_handshake(
                    engine,
                    &request.namespace,
                    &request.payload,
                    options.allow_legacy_identity,
                )
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
        Operation::Maintenance => rejected("maintenance requires common control".to_owned()),
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
            .and_then(|payload| {
                let blocked_sources = parse_restore_authority(payload.blocked_sources)?;
                match (
                    payload.snapshot,
                    payload.snapshot_file,
                    payload.byte_length,
                    payload.content_sha256,
                ) {
                    (Some(snapshot), None, None, None) => {
                        Ok(crate::snapshot::restore_with_revocations(
                            engine,
                            &request.namespace,
                            crate::snapshot::RestoreRequest {
                                idempotency_key: payload.idempotency_key,
                                bytes: snapshot,
                            },
                            deadline,
                            &blocked_sources,
                            None,
                        ))
                    }
                    (None, Some(snapshot_file), Some(byte_length), Some(content_sha256)) => {
                        Ok(crate::snapshot::restore_from_file_with_revocations(
                            engine,
                            &request.namespace,
                            &payload.idempotency_key,
                            &snapshot_file,
                            byte_length,
                            &content_sha256,
                            &blocked_sources,
                            deadline,
                        ))
                    }
                    _ => Err(
                        "snapshot restore requires exactly one complete inline or file transport"
                            .to_owned(),
                    ),
                }
            })
            .unwrap_or_else(rejected),
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

fn dispatch_handshake(
    engine: &NcmEngine,
    namespace: &str,
    payload: &Value,
    allow_legacy_identity: bool,
) -> EngineReply {
    let expected = match parse_payload::<HandshakePayload>(payload) {
        Ok(expected) => expected,
        Err(reason) => return rejected(reason),
    };
    if let Some(version) = expected.protocol_version
        && version != PROTOCOL_VERSION
    {
        return incompatible("handshake protocol identity mismatch");
    }
    if let Some(identity) = expected.protocol_identity.as_deref()
        && identity != PROTOCOL_IDENTITY
    {
        return incompatible("handshake protocol identity mismatch");
    }
    let mut reply = engine.handshake(namespace);
    if reply.outcome != Outcome::Success {
        return reply;
    }
    if allow_legacy_identity {
        downgrade_ready_identity(&mut reply.payload);
    } else if let Err(reason) = attach_worker_identity(&mut reply.payload) {
        return incompatible(&reason);
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
    if let Some(revision) = expected.identity_revision {
        if revision != 2 {
            return incompatible("unsupported handshake identity revision");
        }
        if reply
            .payload
            .get("identity_revision")
            .and_then(Value::as_u64)
            != Some(2)
        {
            return incompatible("handshake did not prove a complete V2 identity");
        }
        if reply
            .payload
            .get("projection_sha256")
            .and_then(Value::as_str)
            .is_none()
            || reply.payload.get("epoch").and_then(Value::as_u64).is_none()
        {
            return incompatible("handshake V2 identity omitted runtime state");
        }
        for (field, expected_value) in [
            ("algorithm", expected.algorithm.as_ref()),
            ("worker", expected.worker.as_ref()),
            ("encoder", expected.encoder.as_ref()),
        ] {
            let Some(expected_value) = expected_value else {
                return incompatible("handshake V2 request omitted pinned identity");
            };
            if reply.payload.get(field) != Some(expected_value) {
                return incompatible("handshake V2 identity mismatch");
            }
        }
        if let Some(expected_projection) = expected.projection_sha256.as_ref()
            && reply
                .payload
                .get("projection_sha256")
                .and_then(Value::as_str)
                != Some(expected_projection)
        {
            return incompatible("handshake projection identity mismatch");
        }
    }
    reply
}

fn attach_worker_identity(payload: &mut Value) -> Result<(), String> {
    static IDENTITY: OnceLock<Result<Value, String>> = OnceLock::new();
    let identity = IDENTITY.get_or_init(|| {
        let path = std::env::current_exe()
            .map_err(|error| format!("resolve executing worker artifact: {error}"))?;
        let identity = crate::worker_artifact::worker_artifact_identity(&path)
            .map_err(|error| format!("measure executing worker artifact: {error}"))?;
        Ok(json!({
            "sha256": identity.sha256,
            "bytes": identity.bytes,
            "target": {
                "triple": identity.triple,
                "os": identity.os,
                "arch": identity.arch,
                "family": identity.family,
            }
        }))
    });
    let worker = identity.as_ref().map_err(Clone::clone)?;
    let object = payload
        .as_object_mut()
        .ok_or_else(|| "handshake ready payload is not an object".to_owned())?;
    object.insert("identity_revision".to_owned(), Value::from(2_u64));
    object.insert("worker".to_owned(), worker.clone());
    Ok(())
}

fn downgrade_ready_identity(payload: &mut Value) {
    let Some(object) = payload.as_object_mut() else {
        return;
    };
    object.remove("identity_revision");
    object.remove("worker");
    if let Some(encoder) = object.get_mut("encoder").and_then(Value::as_object_mut) {
        encoder.retain(|field, _| matches!(field.as_str(), "model" | "artifact_sha256"));
    }
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
    protocol_identity: Option<String>,
    algorithm_profile: Option<String>,
    model: Option<String>,
    epoch: Option<u64>,
    identity_revision: Option<u16>,
    algorithm: Option<Value>,
    projection_sha256: Option<String>,
    worker: Option<Value>,
    encoder: Option<Value>,
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
struct DeletePayload {
    idempotency_key: String,
    source: String,
}

#[derive(Deserialize)]
struct SnapshotRestorePayload {
    idempotency_key: String,
    #[serde(default)]
    blocked_sources: Option<Vec<String>>,
    #[serde(default)]
    snapshot: Option<Vec<u8>>,
    #[serde(default)]
    snapshot_file: Option<PathBuf>,
    #[serde(default)]
    byte_length: Option<u64>,
    #[serde(default)]
    content_sha256: Option<String>,
}

fn parse_restore_authority(values: Option<Vec<String>>) -> Result<Vec<SourceId>, String> {
    let Some(values) = values else {
        return Err("snapshot restore requires deletion authority".to_owned());
    };
    if values.len() > 4096 {
        return Err("snapshot restore deletion authority exceeds its bound".to_owned());
    }
    let mut sources = BTreeSet::new();
    for value in values {
        if value.is_empty() || !sources.insert(value) {
            return Err("snapshot restore deletion authority is invalid".to_owned());
        }
    }
    Ok(sources.into_iter().map(SourceId).collect())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::embedding::doubles::HashEncoder;
    use crate::ports::StateRoot;
    use tempfile::tempdir;
    use tracedecay_memory_ncm_core::types::NcmConfig;

    #[test]
    fn handshake_observes_fresh_and_reopened_epoch_and_rejects_stale_epoch() {
        let root = tempdir().expect("create worker handshake state root");
        let state_root = StateRoot::new(root.path()).expect("state root is absolute");
        let namespace = "ab".repeat(32);
        let engine = NcmEngine::new(
            state_root.clone(),
            Arc::new(HashEncoder::new()),
            NcmConfig::default(),
        );

        let fresh = dispatch_handshake(&engine, &namespace, &json!({}), false);
        assert_eq!(fresh.outcome, Outcome::Success);
        assert_eq!(fresh.payload["empty"].as_bool(), Some(true));
        assert_eq!(fresh.payload["epoch"], 0);
        let executable = std::env::current_exe().expect("resolve test executable");
        let executable = crate::worker_artifact::worker_artifact_identity(&executable)
            .expect("measure test executable");
        assert_eq!(fresh.payload["worker"]["sha256"], executable.sha256);
        assert_eq!(fresh.payload["worker"]["bytes"], executable.bytes);

        let mut observation = ObserveRequest {
            idempotency_key: "handshake-seed".to_owned(),
            payload_sha256: String::new(),
            source: SourceId("handshake-source".to_owned()),
            key_text: "handshake key".to_owned(),
            value_text: "handshake value".to_owned(),
            affect: None,
            surprise: 0.4,
            intensity: 1.0,
            provenance: json!({"origin": "worker-handshake-test"}),
            deadline: Deadline {
                remaining_ms: u64::MAX,
            },
        };
        observation.payload_sha256 = observation
            .canonical_payload_sha256()
            .expect("observation payload hashes");
        assert_eq!(
            engine.observe(&namespace, observation).outcome,
            Outcome::Success
        );
        drop(engine);

        let reopened = NcmEngine::new(
            state_root,
            Arc::new(HashEncoder::new()),
            NcmConfig::default(),
        );
        let unknown_epoch = dispatch_handshake(&reopened, &namespace, &json!({}), false);
        assert_eq!(unknown_epoch.outcome, Outcome::Success);
        assert_eq!(unknown_epoch.payload["empty"].as_bool(), Some(false));
        assert_eq!(unknown_epoch.payload["epoch"], 1);

        let exact = dispatch_handshake(&reopened, &namespace, &json!({"epoch": 1}), false);
        assert_eq!(exact.outcome, Outcome::Success);
        let stale = dispatch_handshake(&reopened, &namespace, &json!({"epoch": 0}), false);
        assert_eq!(stale.outcome, Outcome::Incompatible);
        assert_eq!(
            stale.payload["reason"],
            Value::String("handshake epoch identity mismatch".to_owned())
        );
    }
}
