use super::super::*;
use crate::ports::{Deadline, EncoderError, EncoderIdentity, StateRoot};
use crate::store::{Mutation, StoreError, StoreIdentity, StoreMeta};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fs;
use std::sync::Arc;
use std::time::Instant;
use tracedecay_memory_ncm_core::kernel::{LayerWriteKind, NcmKernel};
use tracedecay_memory_ncm_core::signals::affect;
use tracedecay_memory_ncm_core::types::{AffectVector, CoreError};

pub(super) fn lookup_replay(
    handle: &mut NamespaceHandle,
    key: &str,
    payload_sha256: &str,
) -> Result<Option<EngineReply>, EngineReply> {
    let mutation = handle
        .store
        .begin_mutation()
        .map_err(|error| store_reply(error, handle.commit_seq))?;
    let found = mutation
        .lookup_idempotency(key)
        .map_err(|error| store_reply(error, handle.commit_seq))?;
    drop(mutation);
    let Some((stored_digest, receipt_json, seq)) = found else {
        return Ok(None);
    };
    if stored_digest != payload_sha256 {
        return Ok(Some(EngineReply::rejected(
            RejectReason::IdempotencyConflict,
            handle.commit_seq,
        )));
    }
    let durable: DurableReceipt = serde_json::from_str(&receipt_json).map_err(|error| {
        corrupt_reply(
            handle.commit_seq,
            &format!("decode replay receipt: {error}"),
        )
    })?;
    if durable.reply.state_generation != seq {
        return Err(corrupt_reply(
            handle.commit_seq,
            "idempotency receipt sequence mismatch",
        ));
    }
    let mut replay = durable.reply;
    mark_replayed(&mut replay.payload);
    Ok(Some(replay))
}

fn mark_replayed(payload: &mut Value) {
    if let Some(object) = payload.as_object_mut() {
        object.insert("replayed".to_owned(), Value::Bool(true));
    } else {
        *payload = json!({"value": payload.take(), "replayed": true});
    }
}

pub(super) fn publish(
    handle: &mut NamespaceHandle,
    candidate: NcmKernel,
) -> Result<(), EngineReply> {
    let mut live = handle
        .live
        .write()
        .map_err(|_| corrupt_reply(handle.commit_seq, "published kernel lock poisoned"))?;
    *live = Arc::new(candidate);
    Ok(())
}

pub(super) fn read_live(handle: &NamespaceHandle) -> Result<Arc<NcmKernel>, EngineReply> {
    handle
        .live
        .read()
        .map(|kernel| Arc::clone(&kernel))
        .map_err(|_| corrupt_reply(handle.commit_seq, "published kernel lock poisoned"))
}

pub(super) fn durable_receipt(
    reply: &EngineReply,
    operation: DurableOperation,
    kernel: &NcmKernel,
) -> Result<String, EngineReply> {
    serde_json::to_string(&DurableReceipt {
        reply: reply.clone(),
        operation,
        state_digest: sha256_hex(&kernel.state_digest()),
    })
    .map_err(|error| {
        corrupt_reply(
            reply.state_generation,
            &format!("serialize receipt: {error}"),
        )
    })
}

pub(super) fn put_checkpoint(
    mutation: &mut Mutation<'_>,
    seq: u64,
    epoch: u64,
    kernel: &NcmKernel,
) -> Result<(), EngineReply> {
    let state = serde_json::to_vec(&CheckpointEnvelope {
        kernel: kernel.clone(),
        state_digest: sha256_hex(&kernel.state_digest()),
    })
    .map_err(|error| corrupt_reply(seq, &format!("serialize checkpoint: {error}")))?;
    mutation
        .put_checkpoint(seq, epoch, &state)
        .map_err(|error| store_reply(error, seq.saturating_sub(1)))
}

pub(super) fn meta_for_kernel(
    kernel: &NcmKernel,
    epoch: u64,
    commit_seq: u64,
    last_maintenance: Option<String>,
) -> StoreMeta {
    StoreMeta {
        epoch,
        commit_seq,
        tick: kernel.scheduler.tick.0,
        fatigue: kernel.scheduler.fatigue,
        steps_since_consolidation: kernel.scheduler.steps_since_consolidation,
        last_maintenance,
    }
}

pub(super) fn resolve_affect(input: Option<&ObserveAffect>) -> Result<AffectVector, CoreError> {
    match input {
        None => Ok(affect::neutral()),
        Some(ObserveAffect::Values(values)) => affect::validated(*values),
        Some(ObserveAffect::Preset(name)) => affect::from_name(name),
    }
}

pub(super) fn validate_idempotency_key(key: &str) -> Result<(), String> {
    if key.is_empty() {
        return Err("idempotency key cannot be empty".to_owned());
    }
    if key.len() > MAX_IDEMPOTENCY_KEY_BYTES {
        return Err("idempotency key exceeds 256 bytes".to_owned());
    }
    Ok(())
}

pub(crate) fn canonical_digest<T: Serialize>(value: &T) -> Result<String, String> {
    serde_json::to_vec(value)
        .map(|bytes| sha256_hex(&bytes))
        .map_err(|error| format!("serialize canonical payload: {error}"))
}

pub(super) fn remaining_deadline(deadline: Deadline, started: Instant) -> Deadline {
    let elapsed = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    Deadline {
        remaining_ms: deadline.remaining_ms.saturating_sub(elapsed),
    }
}

pub(super) fn namespace_seed(namespace: &str) -> Option<u64> {
    if namespace.len() != 64 {
        return None;
    }
    let mut bytes = [0_u8; 8];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let offset = index * 2;
        *byte = u8::from_str_radix(namespace.get(offset..offset + 2)?, 16).ok()?;
    }
    Some(u64::from_be_bytes(bytes))
}

pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(super) fn layer_write_name(kind: LayerWriteKind) -> &'static str {
    match kind {
        LayerWriteKind::Ignored => "ignored",
        LayerWriteKind::Created => "created",
        LayerWriteKind::Reinforced => "reinforced",
    }
}

pub(super) fn maintenance_name(kind: &MaintenanceKind) -> &'static str {
    match kind {
        MaintenanceKind::Advance { .. } => "advance",
        MaintenanceKind::Consolidate => "consolidate",
        MaintenanceKind::MergePrune => "merge_prune",
        MaintenanceKind::Checkpoint => "checkpoint",
        MaintenanceKind::Compact => "compact",
    }
}

pub(super) fn ready_payload(identity: &StoreIdentity, epoch: u64, empty: bool) -> Value {
    json!({
        "ready": true,
        "empty": empty,
        "algorithm": identity.algorithm,
        "projection_sha256": identity.projection_sha256,
        "encoder": {
            "model": identity.encoder_model,
            "artifact_sha256": identity.encoder_artifact_sha256
        },
        "epoch": epoch
    })
}

pub(super) fn encoder_payload(identity: &EncoderIdentity) -> Value {
    json!({
        "model": identity.model,
        "artifact_sha256": identity.artifact_sha256,
        "max_length": identity.max_length
    })
}

pub(super) fn catalog_count(root: &StateRoot) -> Result<usize, EngineReply> {
    let path = root.path().join("namespaces");
    let entries = match fs::read_dir(&path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => {
            return Err(EngineReply::new(
                Outcome::Unavailable(format!("read namespace catalog: {error}")),
                0,
                Value::Null,
            ));
        }
    };
    let mut count = 0_usize;
    for entry in entries {
        let entry = entry.map_err(|error| {
            EngineReply::new(
                Outcome::Unavailable(format!("read namespace catalog entry: {error}")),
                0,
                Value::Null,
            )
        })?;
        if entry.path().join("ncm.sqlite").is_file() {
            count = count.saturating_add(1);
        }
    }
    Ok(count)
}

pub(super) fn unavailable_recovery(commit_seq: u64) -> EngineReply {
    EngineReply::new(
        Outcome::Unavailable("rebuilding".to_owned()),
        commit_seq,
        Value::Null,
    )
}

pub(super) fn corrupt_reply(commit_seq: u64, reason: &str) -> EngineReply {
    EngineReply::new(Outcome::Corrupt, commit_seq, json!({"reason": reason}))
}

pub(super) fn encoder_reply(error: EncoderError, commit_seq: u64) -> EngineReply {
    match error {
        EncoderError::Cancelled => EngineReply::new(Outcome::Cancelled, commit_seq, Value::Null),
        EncoderError::ArtifactsMissing(reason) | EncoderError::Inference(reason) => {
            EngineReply::new(Outcome::Unavailable(reason), commit_seq, Value::Null)
        }
        EncoderError::ArtifactMismatch(_) => {
            EngineReply::new(Outcome::Incompatible, commit_seq, Value::Null)
        }
        EncoderError::InputTooLarge => EngineReply::rejected(
            RejectReason::InvalidRequest("encoder input exceeds budget".to_owned()),
            commit_seq,
        ),
    }
}

pub(super) fn core_reply(error: CoreError, commit_seq: u64) -> EngineReply {
    match error {
        CoreError::BudgetExceeded(_) | CoreError::CapacityExhausted(_) => {
            EngineReply::new(Outcome::BudgetExceeded, commit_seq, Value::Null)
        }
        CoreError::UnknownRecord(record_id) => {
            EngineReply::rejected(RejectReason::UnknownRecord(record_id), commit_seq)
        }
        CoreError::Unsupported(_) => {
            EngineReply::new(Outcome::Unsupported, commit_seq, Value::Null)
        }
        CoreError::InvalidState(reason) => corrupt_reply(commit_seq, &reason),
        other => EngineReply::rejected(RejectReason::InvalidRequest(other.to_string()), commit_seq),
    }
}

pub(super) fn store_reply(error: StoreError, commit_seq: u64) -> EngineReply {
    match error {
        StoreError::Busy => EngineReply::new(Outcome::Busy, commit_seq, Value::Null),
        StoreError::Corrupt(reason) => corrupt_reply(commit_seq, &reason),
        StoreError::Incompatible { .. } => {
            EngineReply::new(Outcome::Incompatible, commit_seq, Value::Null)
        }
        StoreError::BudgetExceeded => {
            EngineReply::new(Outcome::BudgetExceeded, commit_seq, Value::Null)
        }
        StoreError::IdempotencyConflict => {
            EngineReply::rejected(RejectReason::IdempotencyConflict, commit_seq)
        }
        StoreError::InvalidInput(reason) | StoreError::InvalidNamespace(reason) => {
            EngineReply::rejected(RejectReason::InvalidRequest(reason), commit_seq)
        }
        StoreError::UnknownRecord(record_id) => {
            EngineReply::rejected(RejectReason::UnknownRecord(record_id), commit_seq)
        }
        StoreError::Missing => EngineReply::new(Outcome::Empty, commit_seq, Value::Null),
        StoreError::AlreadyExists => EngineReply::new(Outcome::Busy, commit_seq, Value::Null),
        StoreError::Io(reason) | StoreError::Sqlite(reason) => {
            EngineReply::new(Outcome::Unavailable(reason), commit_seq, Value::Null)
        }
    }
}
