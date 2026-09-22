use super::super::*;
use super::util::{core_reply, corrupt_reply, sha256_hex, store_reply, validate_common_capsule};
use crate::store::{CapsuleStatus, Event, NamespaceStore, StoreMeta, StoredCapsule};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeSet;
use tracedecay_memory_ncm_core::kernel::{NcmKernel, NewRecord};
use tracedecay_memory_ncm_core::types::{CoreError, NcmConfig};

/// The durable deletion fence that must be completed before a namespace can publish a
/// reconstructed kernel.
#[derive(Clone, Debug)]
pub(crate) struct PendingDeletionFence {
    pub(crate) source: SourceId,
    pub(crate) sources: Vec<SourceId>,
    pub(crate) target_epoch: u64,
    pub(crate) idempotency_key: String,
    pub(crate) payload_sha256: String,
    pub(crate) deleted_records: u64,
    pub(crate) deleted_record_ids: Vec<RecordId>,
    pub(crate) canonical_input: Option<serde_json::Value>,
    pub(crate) tick_before: u64,
    pub(crate) fatigue_before: f32,
    pub(crate) steps_since_consolidation_before: u64,
    pub(crate) event_seq: u64,
    pub(crate) state_digest: String,
}

/// Authenticated metadata for a journal range whose original maintenance
/// events were intentionally omitted from a portable snapshot.  The state
/// digest on each synthetic row identifies the omission row itself; it is not
/// a claim about the unavailable intermediate kernel state.  The imported
/// snapshot digest and the eventual restore checkpoint bind the contract to
/// the source export before recovery can accept the opaque range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotGapContract {
    pub(crate) first_sequence: u64,
    pub(crate) end_sequence: u64,
    pub(crate) checkpoint_sequence: u64,
    pub(crate) snapshot_content_sha256: String,
    pub(crate) terminal_state_digest: String,
}

/// Authenticated metadata for an observe receipt synthesized while staging a
/// snapshot whose capsule was retained but whose original observe row was
/// omitted from the portable event list.  The receipt still carries the
/// capsule's real payload digest; this contract supplies a per-sequence state
/// digest without pretending that the imported final kernel was an earlier
/// state in the journal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotObserveContract {
    pub(crate) sequence: u64,
    pub(crate) record_id: u64,
    pub(crate) checkpoint_sequence: u64,
    pub(crate) snapshot_content_sha256: String,
}

/// Returns the deterministic per-row digest for a snapshot omission marker.
/// It deliberately does not reuse the imported kernel's final digest for an
/// earlier synthetic event.
pub(crate) fn snapshot_gap_state_digest(contract: &Value, sequence: u64) -> Result<String, String> {
    super::canonical_digest(&serde_json::json!({
        "snapshot_gap": contract,
        "sequence": sequence,
    }))
}

/// Returns the deterministic per-row digest for a synthesized snapshot
/// observe receipt.
pub(crate) fn snapshot_observe_state_digest(
    contract: &Value,
    sequence: u64,
) -> Result<String, String> {
    super::canonical_digest(&serde_json::json!({
        "snapshot_observe": contract,
        "sequence": sequence,
    }))
}

/// Validates and decodes the explicit omission contract carried by a
/// synthetic snapshot gap receipt.  A marker is accepted only when its
/// canonical operation input and reply payload are byte-for-byte equivalent
/// JSON values and its per-sequence state digest is derived from that marker.
pub(crate) fn snapshot_gap_contract(
    event: &Event,
    durable: &DurableReceipt,
    commit_seq: u64,
) -> Result<Option<SnapshotGapContract>, EngineReply> {
    let payload_gap = durable.reply.payload.get("snapshot_gap");
    let input_gap = match &durable.operation {
        DurableOperation::Maintenance {
            canonical_input: Some(input),
            ..
        } => input.get("snapshot_gap"),
        _ => None,
    };
    if payload_gap.is_none() && input_gap.is_none() {
        return Ok(None);
    }
    let Some(payload_gap) = payload_gap else {
        return Err(corrupt_reply(
            commit_seq,
            "snapshot gap receipt is missing its canonical contract",
        ));
    };
    let Some(input_gap) = input_gap else {
        return Err(corrupt_reply(
            commit_seq,
            "snapshot gap operation is missing its canonical contract",
        ));
    };
    if payload_gap != input_gap
        || durable.reply.payload.get("replayed") != Some(&Value::Bool(false))
    {
        return Err(corrupt_reply(
            commit_seq,
            "snapshot gap contract differs between receipt fields",
        ));
    }
    let object = payload_gap
        .as_object()
        .ok_or_else(|| corrupt_reply(commit_seq, "snapshot gap contract is not an object"))?;
    if object.len() != 6
        || object.get("version").and_then(Value::as_u64) != Some(1)
        || object
            .get("first_sequence")
            .and_then(Value::as_u64)
            .is_none()
        || object.get("end_sequence").and_then(Value::as_u64).is_none()
        || object
            .get("checkpoint_sequence")
            .and_then(Value::as_u64)
            .is_none()
    {
        return Err(corrupt_reply(
            commit_seq,
            "snapshot gap contract shape is invalid",
        ));
    }
    let first_sequence = object["first_sequence"].as_u64().unwrap_or(0);
    let end_sequence = object["end_sequence"].as_u64().unwrap_or(0);
    let checkpoint_sequence = object["checkpoint_sequence"].as_u64().unwrap_or(0);
    let snapshot_content_sha256 = object
        .get("snapshot_content_sha256")
        .and_then(Value::as_str)
        .filter(|value| is_sha256(value))
        .ok_or_else(|| corrupt_reply(commit_seq, "snapshot gap source digest is invalid"))?;
    let terminal_state_digest = object
        .get("terminal_state_digest")
        .and_then(Value::as_str)
        .filter(|value| is_sha256(value))
        .ok_or_else(|| corrupt_reply(commit_seq, "snapshot gap terminal digest is invalid"))?;
    if first_sequence == 0
        || end_sequence <= first_sequence
        || event.seq < first_sequence
        || event.seq >= end_sequence
        || checkpoint_sequence < end_sequence
        || input_gap
            .as_object()
            .is_none_or(|input| input.len() != object.len())
    {
        return Err(corrupt_reply(
            commit_seq,
            "snapshot gap contract sequence range is invalid",
        ));
    }
    let DurableOperation::Maintenance {
        kind,
        canonical_input: Some(input),
    } = &durable.operation
    else {
        return Err(corrupt_reply(
            commit_seq,
            "snapshot gap operation shape is invalid",
        ));
    };
    if input != &serde_json::json!({"snapshot_gap": payload_gap})
        || !matches!(
            kind,
            MaintenanceKind::Advance { .. } | MaintenanceKind::Checkpoint
        )
    {
        return Err(corrupt_reply(
            commit_seq,
            "snapshot gap operation contract is invalid",
        ));
    }
    if matches!(kind, MaintenanceKind::Advance { .. }) && event.seq != first_sequence {
        return Err(corrupt_reply(
            commit_seq,
            "snapshot gap advance is not the first omission row",
        ));
    }
    let expected_state_digest = snapshot_gap_state_digest(payload_gap, event.seq)
        .map_err(|reason| corrupt_reply(commit_seq, &reason))?;
    let expected_key = format!("snapshot-gap-{}", event.seq);
    if durable.state_digest != expected_state_digest
        || event.idempotency_key.as_deref() != Some(expected_key.as_str())
    {
        return Err(corrupt_reply(
            commit_seq,
            "snapshot gap row authentication is invalid",
        ));
    }
    Ok(Some(SnapshotGapContract {
        first_sequence,
        end_sequence,
        checkpoint_sequence,
        snapshot_content_sha256: snapshot_content_sha256.to_owned(),
        terminal_state_digest: terminal_state_digest.to_owned(),
    }))
}

/// Validates the explicit contract attached to a synthesized snapshot observe
/// receipt.  Its canonical payload digest is still checked independently by
/// [`validate_event_payload_digest`].
pub(crate) fn snapshot_observe_contract(
    event: &Event,
    durable: &DurableReceipt,
    capsules: &[StoredCapsule],
    commit_seq: u64,
) -> Result<Option<SnapshotObserveContract>, EngineReply> {
    let Some(payload_contract) = durable.reply.payload.get("snapshot_observe") else {
        return Ok(None);
    };
    let DurableOperation::Observe { record_id } = &durable.operation else {
        return Err(corrupt_reply(
            commit_seq,
            "snapshot observe contract has a non-observe operation",
        ));
    };
    let object = payload_contract
        .as_object()
        .ok_or_else(|| corrupt_reply(commit_seq, "snapshot observe contract is not an object"))?;
    if object.len() != 5 || object.get("version").and_then(Value::as_u64) != Some(1) {
        return Err(corrupt_reply(
            commit_seq,
            "snapshot observe contract shape is invalid",
        ));
    }
    let sequence = object
        .get("sequence")
        .and_then(Value::as_u64)
        .filter(|sequence| *sequence > 0)
        .ok_or_else(|| corrupt_reply(commit_seq, "snapshot observe sequence is invalid"))?;
    let contract_record_id = object
        .get("record_id")
        .and_then(Value::as_u64)
        .filter(|record_id| *record_id > 0)
        .ok_or_else(|| corrupt_reply(commit_seq, "snapshot observe record ID is invalid"))?;
    let checkpoint_sequence = object
        .get("checkpoint_sequence")
        .and_then(Value::as_u64)
        .filter(|sequence| *sequence >= event.seq)
        .ok_or_else(|| corrupt_reply(commit_seq, "snapshot observe checkpoint is invalid"))?;
    let snapshot_content_sha256 = object
        .get("snapshot_content_sha256")
        .and_then(Value::as_str)
        .filter(|value| is_sha256(value))
        .ok_or_else(|| corrupt_reply(commit_seq, "snapshot observe source digest is invalid"))?;
    let expected_key = format!("snapshot-observe-{sequence}");
    if event.seq != sequence
        || record_id.0 != contract_record_id
        || event.idempotency_key.as_deref() != Some(expected_key.as_str())
        || durable.reply.payload.get("replayed") != Some(&Value::Bool(false))
        || !capsules.iter().any(|capsule| {
            capsule.record_id == *record_id
                && capsule.commit_seq == event.seq
                && capsule.status != CapsuleStatus::Revoked
        })
    {
        return Err(corrupt_reply(
            commit_seq,
            "snapshot observe contract is not bound to its capsule",
        ));
    }
    let expected_state_digest = snapshot_observe_state_digest(payload_contract, event.seq)
        .map_err(|reason| corrupt_reply(commit_seq, &reason))?;
    if durable.state_digest != expected_state_digest {
        return Err(corrupt_reply(
            commit_seq,
            "snapshot observe contract state digest is invalid",
        ));
    }
    Ok(Some(SnapshotObserveContract {
        sequence,
        record_id: contract_record_id,
        checkpoint_sequence,
        snapshot_content_sha256: snapshot_content_sha256.to_owned(),
    }))
}

pub(crate) fn recover_kernel(
    store: &NamespaceStore,
    config: &NcmConfig,
    seed: u64,
    meta: &StoreMeta,
) -> Result<NcmKernel, EngineReply> {
    let checkpoint = store
        .latest_checkpoint()
        .map_err(|error| store_reply(error, meta.commit_seq))?;
    let (mut kernel, mut applied_seq) = match checkpoint {
        Some(checkpoint) => {
            if checkpoint.epoch != meta.epoch || checkpoint.seq > meta.commit_seq {
                return Err(corrupt_reply(
                    meta.commit_seq,
                    "checkpoint generation mismatch",
                ));
            }
            let envelope: CheckpointEnvelope =
                serde_json::from_slice(&checkpoint.state).map_err(|error| {
                    corrupt_reply(meta.commit_seq, &format!("decode checkpoint: {error}"))
                })?;
            if sha256_hex(&envelope.kernel.state_digest()) != envelope.state_digest {
                return Err(corrupt_reply(
                    meta.commit_seq,
                    "checkpoint state digest mismatch",
                ));
            }
            (envelope.kernel, checkpoint.seq)
        }
        None => (
            NcmKernel::new(seed, config.clone())
                .map_err(|error| core_reply(error, meta.commit_seq))?,
            0,
        ),
    };
    if kernel.config != *config {
        return Err(corrupt_reply(meta.commit_seq, "checkpoint config mismatch"));
    }
    let projection_bytes = serde_json::to_vec(&kernel.projections).map_err(|error| {
        corrupt_reply(meta.commit_seq, &format!("serialize projections: {error}"))
    })?;
    if projection_bytes != store.identity().projection_bytes {
        return Err(corrupt_reply(
            meta.commit_seq,
            "checkpoint projection mismatch",
        ));
    }
    let capsules = store
        .capsules_in_commit_order(false)
        .map_err(|error| store_reply(error, meta.commit_seq))?;
    let all_capsules = store
        .capsules_in_commit_order(true)
        .map_err(|error| store_reply(error, meta.commit_seq))?;
    validate_checkpoint_chain(store, applied_seq, &kernel, meta, &all_capsules)?;
    for capsule in &capsules {
        let provenance: serde_json::Value = serde_json::from_str(&capsule.provenance)
            .map_err(|_| corrupt_reply(meta.commit_seq, "invalid capsule provenance"))?;
        validate_common_capsule(&provenance)
            .map_err(|error| store_reply(error, meta.commit_seq))?;
    }
    let events = store
        .events_after(applied_seq)
        .map_err(|error| store_reply(error, meta.commit_seq))?;
    let mut expected_tick = kernel.scheduler.tick.0;
    let mut state_chain_valid = true;
    for event in events {
        let expected = applied_seq
            .checked_add(1)
            .ok_or_else(|| corrupt_reply(meta.commit_seq, "event sequence overflow"))?;
        let durable = validate_recovery_event(&event, expected, meta.commit_seq)?;
        validate_common_maintenance_event(store, &event, &durable)?;
        expected_tick =
            validate_checkpoint_event_tick(&event, &durable, expected_tick, meta.commit_seq)?;
        // Keep revoked capsules in the validation view.  Their retained
        // inputs are deliberately erased, but the journal still contains the
        // original observe event and needs the explicit revoked exception in
        // `validate_event_payload_digest` rather than looking like a missing
        // capsule.
        validate_event_payload_digest(&event, &durable, &all_capsules)?;
        validate_deletion_completion_fence(
            store,
            &event,
            &durable,
            &all_capsules,
            meta.commit_seq,
        )?;
        if snapshot_gap_contract(&event, &durable, meta.commit_seq)?.is_some()
            || snapshot_observe_contract(&event, &durable, &all_capsules, meta.commit_seq)?
                .is_some()
        {
            state_chain_valid = false;
        }
        replay_event(
            &mut kernel,
            &event,
            &durable.operation,
            &all_capsules,
            meta.commit_seq,
        )?;
        if state_chain_valid && sha256_hex(&kernel.state_digest()) != durable.state_digest {
            return Err(corrupt_reply(
                meta.commit_seq,
                "replayed state digest mismatch",
            ));
        }
        applied_seq = event.seq;
    }
    if applied_seq != meta.commit_seq {
        return Err(corrupt_reply(
            meta.commit_seq,
            "metadata sequence is not journal-backed",
        ));
    }
    if kernel.scheduler.tick.0 != meta.tick
        || kernel.scheduler.fatigue != meta.fatigue
        || kernel.scheduler.steps_since_consolidation != meta.steps_since_consolidation
    {
        return Err(corrupt_reply(
            meta.commit_seq,
            "scheduler metadata mismatch",
        ));
    }
    Ok(kernel)
}

/// Binds a checkpoint to the journal prefix it claims to replace.
///
/// A checkpoint row and its kernel digest are stored in the same mutation as
/// the event that produced them.  Checking only the checkpoint bytes would
/// therefore allow a detached checkpoint to hide a missing or corrupted
/// journal prefix.  Validate every prefix envelope and require the event at
/// the checkpoint sequence to carry the same state digest before replaying
/// any suffix.
fn validate_checkpoint_chain(
    store: &NamespaceStore,
    checkpoint_seq: u64,
    checkpoint_kernel: &NcmKernel,
    meta: &StoreMeta,
    capsules: &[StoredCapsule],
) -> Result<(), EngineReply> {
    if checkpoint_seq == 0 {
        return Ok(());
    }
    let expected_checkpoint_digest = sha256_hex(&checkpoint_kernel.state_digest());
    let events = store
        .events_after(0)
        .map_err(|error| store_reply(error, meta.commit_seq))?;
    let mut expected_seq = 1_u64;
    let mut expected_tick = 0_u64;
    let mut checkpoint_receipt = None;
    let mut checkpoint_event_tick = None;
    let mut gap_contracts = Vec::new();
    let mut observe_contracts = Vec::new();
    for event in events {
        if event.seq > checkpoint_seq {
            break;
        }
        let durable = validate_recovery_event(&event, expected_seq, meta.commit_seq)?;
        validate_common_maintenance_event(store, &event, &durable)?;
        validate_event_payload_digest(&event, &durable, capsules)?;
        validate_deletion_completion_fence(store, &event, &durable, capsules, meta.commit_seq)?;
        if let Some(contract) = snapshot_gap_contract(&event, &durable, meta.commit_seq)? {
            gap_contracts.push(contract);
        }
        if let Some(contract) =
            snapshot_observe_contract(&event, &durable, capsules, meta.commit_seq)?
        {
            observe_contracts.push(contract);
        }
        expected_tick =
            validate_checkpoint_event_tick(&event, &durable, expected_tick, meta.commit_seq)?;
        if event.seq == checkpoint_seq {
            checkpoint_receipt = Some(durable.state_digest);
            checkpoint_event_tick = Some(event.created_tick);
            break;
        }
        expected_seq = expected_seq
            .checked_add(1)
            .ok_or_else(|| corrupt_reply(meta.commit_seq, "checkpoint sequence overflow"))?;
    }
    if expected_seq != checkpoint_seq {
        return Err(corrupt_reply(
            meta.commit_seq,
            "checkpoint journal prefix has a sequence gap",
        ));
    }
    if checkpoint_receipt.as_deref() != Some(expected_checkpoint_digest.as_str())
        || checkpoint_event_tick != Some(checkpoint_kernel.scheduler.tick.0)
    {
        return Err(corrupt_reply(
            meta.commit_seq,
            "checkpoint is detached from its journal receipt",
        ));
    }
    validate_snapshot_omission_checkpoint(
        store,
        checkpoint_seq,
        &gap_contracts,
        &observe_contracts,
        meta.commit_seq,
    )?;
    Ok(())
}

/// Binds every synthetic omission range to the public snapshot restore
/// checkpoint that completed the staged import.  A pending deletion fence is
/// allowed to use the contract's terminal source digest while rebuilding, but
/// a published generation must retain this checkpoint binding across restart.
fn validate_snapshot_omission_checkpoint(
    store: &NamespaceStore,
    checkpoint_seq: u64,
    gap_contracts: &[SnapshotGapContract],
    observe_contracts: &[SnapshotObserveContract],
    commit_seq: u64,
) -> Result<(), EngineReply> {
    if gap_contracts.is_empty() && observe_contracts.is_empty() {
        return Ok(());
    }
    // Omission rows remain in the journal after a later maintenance or
    // deletion checkpoint supersedes the restore checkpoint.  Resolve each
    // contract's own immutable snapshot_restore event instead of requiring it
    // to point at whichever checkpoint happens to be latest today.
    let validate_contract =
        |contract_sequence: u64, source_digest: &str, kind: &str| -> Result<(), EngineReply> {
            if contract_sequence > checkpoint_seq {
                return Err(corrupt_reply(
                    commit_seq,
                    &format!("{kind} omission is bound to a future restore checkpoint"),
                ));
            }
            let checkpoint = store
                .event(contract_sequence)
                .map_err(|error| store_reply(error, commit_seq))?
                .ok_or_else(|| {
                    corrupt_reply(commit_seq, &format!("{kind} checkpoint event is missing"))
                })?;
            if checkpoint.kind != "snapshot_restore" {
                return Err(corrupt_reply(
                    commit_seq,
                    &format!("{kind} omission is not bound to a restore checkpoint"),
                ));
            }
            let durable: DurableReceipt =
                serde_json::from_str(&checkpoint.receipt).map_err(|error| {
                    corrupt_reply(
                        commit_seq,
                        &format!("decode {kind} checkpoint receipt: {error}"),
                    )
                })?;
            let content_sha256 = durable
                .reply
                .payload
                .get("content_sha256")
                .and_then(Value::as_str)
                .filter(|value| is_sha256(value))
                .ok_or_else(|| {
                    corrupt_reply(
                        commit_seq,
                        &format!("{kind} checkpoint source digest is missing"),
                    )
                })?;
            if source_digest != content_sha256 {
                return Err(corrupt_reply(
                    commit_seq,
                    &format!("{kind} omission is detached from its restore checkpoint"),
                ));
            }
            Ok(())
        };
    for contract in gap_contracts {
        validate_contract(
            contract.checkpoint_sequence,
            &contract.snapshot_content_sha256,
            "snapshot gap",
        )?;
    }
    for contract in observe_contracts {
        validate_contract(
            contract.checkpoint_sequence,
            &contract.snapshot_content_sha256,
            "snapshot observe",
        )?;
    }
    Ok(())
}

/// Validates one journal row before any recovery or privacy replay can use it.
///
/// This is deliberately shared with the privacy rebuild path. The rebuild may
/// have scrubbed a source capsule, so it cannot always reconstruct the old
/// state digest, but it must still enforce the same durable envelope rules as
/// ordinary recovery.
pub(crate) fn validate_recovery_event(
    event: &Event,
    expected_seq: u64,
    commit_seq: u64,
) -> Result<DurableReceipt, EngineReply> {
    if expected_seq > commit_seq || event.seq != expected_seq {
        return Err(corrupt_reply(commit_seq, "event sequence gap"));
    }
    if !is_sha256(&event.payload_sha256) {
        return Err(corrupt_reply(commit_seq, "event payload digest is invalid"));
    }
    let durable: DurableReceipt = serde_json::from_str(&event.receipt)
        .map_err(|error| corrupt_reply(commit_seq, &format!("decode receipt: {error}")))?;
    super::util::validate_receipt_idempotency_key_json(
        &event.receipt,
        &event.kind,
        event.idempotency_key.as_deref(),
    )
    .map_err(|reason| corrupt_reply(commit_seq, &reason))?;
    // Receipts written before the deletion canonical-input migration omitted
    // the receipt copy of the request key. Their event row still carries the
    // key, and the legacy operation shape below is checked independently.
    // Keep this exception limited to that exact shape; current receipts must
    // bind their inner key to the outer event key.
    let legacy_deletion_without_receipt_key = matches!(
        &durable.operation,
        DurableOperation::DeleteBySource {
            payload_sha256: None,
            canonical_input: None,
            ..
        }
    ) && durable.idempotency_key.is_none();
    let expected_receipt_key = if legacy_deletion_without_receipt_key {
        None
    } else {
        event.idempotency_key.as_deref()
    };
    super::util::validate_durable_receipt(&durable, event.seq, expected_receipt_key)
        .map_err(|reason| corrupt_reply(commit_seq, &reason))?;
    super::util::validate_common_control_digest(event, &durable)
        .map_err(|reason| corrupt_reply(commit_seq, &reason))?;
    if !is_sha256(&durable.state_digest) {
        return Err(corrupt_reply(commit_seq, "receipt state digest is invalid"));
    }
    if !is_sha256(&durable.integrity_digest)
        || super::util::durable_integrity_digest(
            &durable.reply,
            &durable.operation,
            &durable.state_digest,
        )
        .map_err(|reason| corrupt_reply(commit_seq, &reason))?
            != durable.integrity_digest
    {
        return Err(corrupt_reply(
            commit_seq,
            "receipt integrity digest mismatch",
        ));
    }
    if durable.reply.outcome != Outcome::Success {
        return Err(corrupt_reply(
            commit_seq,
            "durable receipt outcome is not success",
        ));
    }
    if durable.reply.state_generation != event.seq {
        return Err(corrupt_reply(commit_seq, "receipt generation mismatch"));
    }
    let requires_key = !matches!(&durable.operation, DurableOperation::DeletionFence { .. });
    if requires_key {
        if event
            .idempotency_key
            .as_deref()
            .is_none_or(|key| key.is_empty() || key.len() > MAX_IDEMPOTENCY_KEY_BYTES)
        {
            return Err(corrupt_reply(
                commit_seq,
                "event idempotency key is invalid",
            ));
        }
    } else if event.idempotency_key.is_some() {
        return Err(corrupt_reply(
            commit_seq,
            "deletion fence event unexpectedly has an idempotency key",
        ));
    }
    let kind_matches = match &durable.operation {
        DurableOperation::CommonControl { .. } => event.kind == "common_control",
        DurableOperation::Observe { .. } => event.kind == "observe",
        DurableOperation::Feedback { .. } => event.kind == "feedback",
        DurableOperation::Correction { .. } => event.kind == "correction",
        DurableOperation::Maintenance { kind, .. } => {
            event.kind == "maintenance"
                || (matches!(kind, MaintenanceKind::Checkpoint) && event.kind == "snapshot_restore")
        }
        DurableOperation::DeletionFence { .. } => event.kind == "deletion_fence",
        DurableOperation::DeleteBySource { .. } => event.kind == "delete_by_source",
    };
    if !kind_matches {
        return Err(corrupt_reply(commit_seq, "receipt operation kind mismatch"));
    }
    Ok(durable)
}

/// Validates the request capsule retained by a common-maintenance receipt.
///
/// The ordinary durable integrity digest covers the reply and operation, but
/// the request semantic digest is intentionally carried in the retained
/// admission capsule.  Validate that capsule against the namespace and event
/// before recovery trusts the derived reply field.
fn validate_common_maintenance_event(
    store: &NamespaceStore,
    event: &Event,
    durable: &DurableReceipt,
) -> Result<(), EngineReply> {
    let DurableOperation::Maintenance {
        canonical_input, ..
    } = &durable.operation
    else {
        return Ok(());
    };
    if snapshot_gap_contract(event, durable, event.seq)?.is_some() {
        return Ok(());
    }
    if durable.reply.payload.get("common_maintenance").is_none() {
        if canonical_input.is_some() {
            return Err(corrupt_reply(
                event.seq,
                "common maintenance receipt is missing its request evidence",
            ));
        }
        return Ok(());
    }
    match super::common_maintenance::portable_event(store.namespace(), event) {
        Ok(true) => Ok(()),
        Ok(false) => Err(corrupt_reply(
            event.seq,
            "common maintenance receipt is not portable",
        )),
        Err(reason) => Err(corrupt_reply(event.seq, &reason)),
    }
}

/// Binds a completed deletion event to the fence that authorized it.
///
/// The mutable revocation projection can be replaced by a later deletion, so
/// the completion event must carry its own immutable fence linkage. Checking
/// this during ordinary recovery also prevents a self-consistent but detached
/// completion from being hidden behind its latest checkpoint.
pub(crate) fn validate_deletion_completion_fence(
    store: &NamespaceStore,
    event: &Event,
    completion: &DurableReceipt,
    capsules: &[StoredCapsule],
    commit_seq: u64,
) -> Result<(), EngineReply> {
    let DurableOperation::DeleteBySource {
        source: completion_source,
        sources: completion_sources,
        target_epoch: completion_epoch,
        deleted_records: completion_deleted_records,
        deleted_record_ids: completion_deleted_record_ids,
        canonical_input: completion_canonical_input,
        payload_sha256: completion_payload_sha256,
    } = &completion.operation
    else {
        return Ok(());
    };
    let fence_seq = event
        .seq
        .checked_sub(1)
        .ok_or_else(|| corrupt_reply(commit_seq, "completed deletion fence sequence underflow"))?;
    let fence_event = store
        .event(fence_seq)
        .map_err(|error| store_reply(error, commit_seq))?
        .ok_or_else(|| corrupt_reply(commit_seq, "completed deletion fence is missing"))?;
    let fence: DurableReceipt = serde_json::from_str(&fence_event.receipt).map_err(|error| {
        corrupt_reply(
            commit_seq,
            &format!("decode completed deletion fence receipt: {error}"),
        )
    })?;
    validate_recovery_event(&fence_event, fence_seq, commit_seq)?;
    validate_common_maintenance_event(store, &fence_event, &fence)?;
    validate_event_payload_digest(&fence_event, &fence, capsules)?;
    let DurableOperation::DeletionFence {
        source: fence_source,
        sources: fence_sources,
        target_epoch: fence_epoch,
        idempotency_key: fence_key,
        payload_sha256: fence_payload_sha256,
        deleted_records: fence_deleted_records,
        deleted_record_ids: fence_deleted_record_ids,
        pre_fence_state_digest,
        canonical_input: fence_canonical_input,
        ..
    } = &fence.operation
    else {
        return Err(corrupt_reply(
            commit_seq,
            "completed deletion is not preceded by a deletion fence",
        ));
    };
    let completion_key = event
        .idempotency_key
        .as_deref()
        .ok_or_else(|| corrupt_reply(commit_seq, "completed deletion key is missing"))?;
    let tick_anchor = deletion_tick_anchor(event, completion, commit_seq)?;
    if let Some((tick_before, tick_after)) = tick_anchor
        && (tick_before != fence_event.created_tick || tick_after != event.created_tick)
    {
        return Err(corrupt_reply(
            commit_seq,
            "completed deletion tick anchor is not bound to its fence",
        ));
    }
    if event.kind != "delete_by_source"
        || fence_event.kind != "deletion_fence"
        || fence_event.idempotency_key.is_some()
        || fence_event.seq.checked_add(1) != Some(event.seq)
        || completion_source != fence_source
        || completion_sources != fence_sources
        || completion_epoch != fence_epoch
        || completion_key != fence_key
        || event.payload_sha256 != *fence_payload_sha256
        || completion_payload_sha256
            .as_deref()
            .is_some_and(|payload| payload != fence_payload_sha256.as_str())
        || completion_deleted_records != fence_deleted_records
        || completion_deleted_record_ids != fence_deleted_record_ids
        || completion_canonical_input != fence_canonical_input
        || !is_sha256(pre_fence_state_digest)
        || fence.reply.payload["fenced"] != true
        || fence.reply.payload["target_epoch"] != serde_json::json!(fence_epoch)
        || fence.reply.state_generation != fence_event.seq
        || fence.state_digest != *pre_fence_state_digest
    {
        return Err(corrupt_reply(
            commit_seq,
            "completed deletion is not bound to its original fence",
        ));
    }
    Ok(())
}

/// Returns the explicit logical-tick reset carried by a sanitized completion.
///
/// Privacy replay removes erased observations, so its rebuilt scheduler tick
/// may be lower than the original journal timeline.  The completion receipt
/// records both sides of that reset; requiring the event tick to equal the
/// reported post-reset value keeps the reset bound to its durable checkpoint.
fn deletion_tick_anchor(
    event: &Event,
    durable: &DurableReceipt,
    commit_seq: u64,
) -> Result<Option<(u64, u64)>, EngineReply> {
    if !matches!(&durable.operation, DurableOperation::DeleteBySource { .. }) {
        return Ok(None);
    }
    let legacy_shape = matches!(
        &durable.operation,
        DurableOperation::DeleteBySource {
            payload_sha256: None,
            canonical_input: None,
            ..
        }
    );
    let Some(report) = durable
        .reply
        .payload
        .get("sanitized_replay")
        .and_then(serde_json::Value::as_object)
    else {
        if legacy_shape {
            // The original DeleteBySource envelope had no explicit reset
            // anchor. Its operation delta is zero, so the ordinary chain
            // still checks the event tick without inventing a reset.
            return Ok(None);
        }
        return Err(corrupt_reply(
            commit_seq,
            "sanitized deletion receipt is missing its tick anchor",
        ));
    };
    let tick_before = report
        .get("tick_before")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| corrupt_reply(commit_seq, "sanitized deletion tick anchor is invalid"))?;
    let tick_after = report
        .get("tick_after")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| corrupt_reply(commit_seq, "sanitized deletion tick anchor is invalid"))?;
    let report_digest = report
        .get("digest")
        .cloned()
        .ok_or_else(|| corrupt_reply(commit_seq, "sanitized deletion digest is missing"))
        .and_then(|digest| {
            serde_json::from_value::<[u8; 32]>(digest)
                .map_err(|_| corrupt_reply(commit_seq, "sanitized deletion digest is invalid"))
        })?;
    if sha256_hex(&report_digest) != durable.state_digest {
        return Err(corrupt_reply(
            commit_seq,
            "sanitized deletion digest does not match its receipt",
        ));
    }
    if tick_after != event.created_tick || tick_after > tick_before {
        return Err(corrupt_reply(
            commit_seq,
            "sanitized deletion tick anchor does not match its event",
        ));
    }
    Ok(Some((tick_before, tick_after)))
}

/// Advances the journal tick while honoring an explicit sanitized reset.
fn validate_checkpoint_event_tick(
    event: &Event,
    durable: &DurableReceipt,
    previous_tick: u64,
    commit_seq: u64,
) -> Result<u64, EngineReply> {
    if let Some((tick_before, tick_after)) = deletion_tick_anchor(event, durable, commit_seq)? {
        if tick_before != previous_tick {
            return Err(corrupt_reply(
                commit_seq,
                "sanitized deletion tick anchor does not match its prefix",
            ));
        }
        return Ok(tick_after);
    }
    let expected_tick = previous_tick
        .checked_add(operation_tick_delta(&durable.operation))
        .ok_or_else(|| corrupt_reply(commit_seq, "checkpoint journal tick overflow"))?;
    if event.created_tick != expected_tick {
        return Err(corrupt_reply(
            commit_seq,
            &format!(
                "checkpoint journal tick does not match its operation history (seq {}, expected {}, actual {})",
                event.seq, expected_tick, event.created_tick
            ),
        ));
    }
    Ok(expected_tick)
}

/// Verifies payload digests against durable operation inputs and retained capsules.
///
/// Revoked observe capsules are the one deliberate exception: erasure removes
/// their source text before a restart can validate the original request. Those
/// rows remain covered by the fence's pre-fence anchor and journal envelope.
pub(crate) fn validate_event_payload_digest(
    event: &Event,
    durable: &DurableReceipt,
    capsules: &[StoredCapsule],
) -> Result<(), EngineReply> {
    let expected = match &durable.operation {
        DurableOperation::Feedback { record_ids } => Some(
            super::canonical_digest(record_ids)
                .map_err(|reason| corrupt_reply(event.seq, &reason))?,
        ),
        DurableOperation::Correction {
            superseded,
            superseding,
            evidence,
        } => {
            #[derive(Serialize)]
            struct Payload<'a> {
                superseded: RecordId,
                superseding: RecordId,
                evidence: &'a str,
            }
            Some(
                super::canonical_digest(&Payload {
                    superseded: *superseded,
                    superseding: *superseding,
                    evidence,
                })
                .map_err(|reason| corrupt_reply(event.seq, &reason))?,
            )
        }
        DurableOperation::Maintenance {
            kind,
            canonical_input,
        } => {
            if event.kind == "snapshot_restore" && matches!(kind, MaintenanceKind::Checkpoint) {
                return Ok(());
            }
            if snapshot_gap_contract(event, durable, event.seq)?.is_some() {
                let expected = super::canonical_digest(kind)
                    .map_err(|reason| corrupt_reply(event.seq, &reason))?;
                if event.payload_sha256 != expected {
                    return Err(corrupt_reply(
                        event.seq,
                        "snapshot gap maintenance payload digest mismatch",
                    ));
                }
                return Ok(());
            }
            if canonical_input.is_some()
                && durable.reply.payload.get("common_maintenance").is_none()
            {
                return Err(corrupt_reply(
                    event.seq,
                    "common maintenance receipt is missing its request evidence",
                ));
            }
            if let Some(common) = durable.reply.payload.get("common_maintenance") {
                let digest = common
                    .get("request_semantic_sha256")
                    .and_then(serde_json::Value::as_str)
                    .filter(|digest| is_sha256(digest))
                    .ok_or_else(|| {
                        corrupt_reply(event.seq, "common maintenance payload digest is invalid")
                    })?;
                if let Some(canonical_input) = canonical_input {
                    let canonical_digest = super::canonical_digest(canonical_input)
                        .map_err(|reason| corrupt_reply(event.seq, &reason))?;
                    if digest != canonical_digest {
                        return Err(corrupt_reply(
                            event.seq,
                            "common maintenance semantic digest mismatch",
                        ));
                    }
                    Some(canonical_digest)
                } else {
                    // The pre-canonical-input envelope carried only the
                    // retained semantic digest. The maintenance owner
                    // independently checks its admission capsule and event
                    // basis, so this exact legacy shape remains replayable
                    // even though newer receipts persist canonical_input.
                    Some(digest.to_owned())
                }
            } else {
                Some(
                    super::canonical_digest(kind)
                        .map_err(|reason| corrupt_reply(event.seq, &reason))?,
                )
            }
        }
        DurableOperation::DeletionFence {
            sources,
            payload_sha256,
            canonical_input,
            ..
        } => {
            if !valid_source_set(sources) {
                return Err(corrupt_reply(
                    event.seq,
                    "deletion fence source set is invalid",
                ));
            }
            if !deletion_input_matches(
                canonical_input.as_ref(),
                sources,
                payload_sha256,
                event.seq,
                event.seq.checked_sub(1),
            )? {
                return Err(corrupt_reply(
                    event.seq,
                    "deletion fence canonical request mismatch",
                ));
            }
            Some(payload_sha256.to_owned())
        }
        DurableOperation::DeleteBySource {
            sources,
            payload_sha256,
            canonical_input,
            ..
        } => {
            if !valid_source_set(sources) {
                return Err(corrupt_reply(event.seq, "deletion source set is invalid"));
            }
            if payload_sha256.is_some() != canonical_input.is_some() {
                return Err(corrupt_reply(
                    event.seq,
                    "deletion canonical fields are incomplete",
                ));
            }
            let expected = match payload_sha256 {
                Some(payload_sha256) => payload_sha256.clone(),
                None => legacy_deletion_digest(sources, &event.payload_sha256, event.seq)?,
            };
            if !deletion_input_matches(
                canonical_input.as_ref(),
                sources,
                &expected,
                event.seq,
                event.seq.checked_sub(2),
            )? {
                return Err(corrupt_reply(
                    event.seq,
                    "completed deletion canonical request mismatch",
                ));
            }
            Some(expected)
        }
        DurableOperation::Observe { record_id } => {
            let Some(capsule) = capsules
                .iter()
                .find(|capsule| capsule.record_id == *record_id && capsule.commit_seq == event.seq)
            else {
                return Err(corrupt_reply(
                    event.seq,
                    "observe payload capsule is missing",
                ));
            };
            if capsule.status == crate::store::CapsuleStatus::Revoked {
                None
            } else {
                Some(observe_payload_digest(
                    capsule,
                    &event.payload_sha256,
                    event.seq,
                )?)
            }
        }
        DurableOperation::CommonControl { .. } => {
            let digest = durable
                .reply
                .payload
                .get("request_semantic_sha256")
                .and_then(serde_json::Value::as_str)
                .filter(|digest| is_sha256(digest))
                .ok_or_else(|| {
                    corrupt_reply(event.seq, "common control payload digest is missing")
                })?;
            Some(digest.to_owned())
        }
    };
    if let Some(expected) = expected
        && event.payload_sha256 != expected
    {
        return Err(corrupt_reply(event.seq, "event payload digest mismatch"));
    }
    Ok(())
}

/// Validates every durable row and the selected pending deletion fence.
///
/// A fenced namespace is publishable only when the fence is the final journal
/// event, all preceding sequences are present, and revocation metadata binds
/// to that exact event and target epoch.
pub(crate) fn validate_pending_deletion_fence(
    store: &NamespaceStore,
) -> Result<Option<PendingDeletionFence>, EngineReply> {
    let Some(reason) = store.fenced().map_err(|error| store_reply(error, 0))? else {
        return Ok(None);
    };
    if reason != "rebuilding" {
        return Err(corrupt_reply(0, "unknown privacy fence reason"));
    }
    let meta = store.meta().map_err(|error| store_reply(error, 0))?;
    if meta.commit_seq == 0 {
        return Err(corrupt_reply(
            0,
            "fenced namespace has no committed generation",
        ));
    }
    let events = store
        .events_after(0)
        .map_err(|error| store_reply(error, meta.commit_seq))?;
    let capsules = store
        .capsules_in_commit_order(true)
        .map_err(|error| store_reply(error, meta.commit_seq))?;
    let mut expected_seq = 1_u64;
    let mut expected_tick = 0_u64;
    let mut validated = Vec::with_capacity(events.len());
    for event in &events {
        let durable = validate_recovery_event(event, expected_seq, meta.commit_seq)?;
        validate_common_maintenance_event(store, event, &durable)?;
        validate_event_payload_digest(event, &durable, &capsules)?;
        validate_deletion_completion_fence(store, event, &durable, &capsules, meta.commit_seq)?;
        expected_tick =
            validate_checkpoint_event_tick(event, &durable, expected_tick, meta.commit_seq)?;
        expected_seq = expected_seq
            .checked_add(1)
            .ok_or_else(|| corrupt_reply(meta.commit_seq, "event sequence overflow"))?;
        validated.push((event, durable));
    }
    if expected_seq != meta.commit_seq.saturating_add(1) {
        return Err(corrupt_reply(
            meta.commit_seq,
            "metadata sequence is not journal-backed",
        ));
    }
    let Some((event, durable)) = validated.last() else {
        return Err(corrupt_reply(
            meta.commit_seq,
            "pending deletion fence is missing",
        ));
    };
    let DurableOperation::DeletionFence {
        source,
        sources,
        target_epoch,
        idempotency_key,
        payload_sha256,
        deleted_records,
        deleted_record_ids,
        pre_fence_state_digest,
        fatigue,
        steps_since_consolidation,
        canonical_input,
    } = &durable.operation
    else {
        return Err(corrupt_reply(
            meta.commit_seq,
            "pending deletion fence is not the final journal event",
        ));
    };
    let deletion_input_valid = deletion_input_matches(
        canonical_input.as_ref(),
        sources,
        payload_sha256,
        meta.commit_seq,
        meta.commit_seq.checked_sub(1),
    )?;
    if !is_sha256(pre_fence_state_digest)
        || sources.is_empty()
        || sources.first() != Some(source)
        || sources.windows(2).any(|window| window[0] >= window[1])
        || event.kind != "deletion_fence"
        || event.seq != meta.commit_seq
        || event.created_tick != meta.tick
        || durable.reply.payload["fenced"] != true
        || durable.reply.payload["target_epoch"] != *target_epoch
        || durable.reply.state_generation != event.seq
        || event.payload_sha256 != *payload_sha256
        || !deletion_input_valid
        || idempotency_key.is_empty()
        || idempotency_key.len() > MAX_IDEMPOTENCY_KEY_BYTES
        || *fatigue != meta.fatigue
        || *steps_since_consolidation != meta.steps_since_consolidation
    {
        return Err(corrupt_reply(
            meta.commit_seq,
            "pending deletion fence does not match its persisted metadata",
        ));
    }
    if *target_epoch != meta.epoch.saturating_add(1) {
        return Err(corrupt_reply(
            meta.commit_seq,
            "pending deletion fence epoch does not match metadata",
        ));
    }
    let revocations = store
        .revocations()
        .map_err(|error| store_reply(error, meta.commit_seq))?;
    let expected_sources = sources.iter().cloned().collect::<BTreeSet<_>>();
    let actual_sources = revocations
        .iter()
        .filter(|revocation| revocation.epoch == *target_epoch && revocation.seq == event.seq)
        .map(|revocation| revocation.source_id.clone())
        .collect::<BTreeSet<_>>();
    if actual_sources != expected_sources {
        return Err(corrupt_reply(
            meta.commit_seq,
            "pending deletion fence revocations do not match its source set",
        ));
    }
    let deleted_ids = deleted_record_ids.iter().copied().collect::<BTreeSet<_>>();
    if deleted_ids.len() != deleted_record_ids.len()
        || !deleted_record_ids
            .windows(2)
            .all(|window| window[0] < window[1])
        || u64::try_from(deleted_record_ids.len()).ok() != Some(*deleted_records)
        || usize::try_from(*deleted_records).is_err()
    {
        return Err(corrupt_reply(
            meta.commit_seq,
            "pending deletion fence record set is invalid",
        ));
    }
    let previously_deleted = validated
        .iter()
        .take(validated.len().saturating_sub(1))
        .flat_map(|(_, durable)| match &durable.operation {
            DurableOperation::DeletionFence {
                deleted_record_ids, ..
            }
            | DurableOperation::DeleteBySource {
                deleted_record_ids, ..
            } => deleted_record_ids.iter().copied().collect::<Vec<_>>(),
            _ => Vec::new(),
        })
        .collect::<BTreeSet<_>>();
    let expected_deleted = capsules
        .iter()
        .filter(|capsule| {
            capsule.status == crate::store::CapsuleStatus::Revoked
                && capsule.commit_seq < event.seq
                && !previously_deleted.contains(&capsule.record_id)
        })
        .map(|capsule| capsule.record_id)
        .collect::<BTreeSet<_>>();
    if deleted_ids != expected_deleted {
        return Err(corrupt_reply(
            meta.commit_seq,
            "pending deletion fence record set is not exact",
        ));
    }
    for record_id in &deleted_ids {
        let Some(capsule) = capsules
            .iter()
            .find(|capsule| capsule.record_id == *record_id)
        else {
            return Err(corrupt_reply(
                meta.commit_seq,
                "pending deletion fence references an unknown record",
            ));
        };
        if capsule.status != crate::store::CapsuleStatus::Revoked || capsule.commit_seq >= event.seq
        {
            return Err(corrupt_reply(
                meta.commit_seq,
                "pending deletion fence record set does not match revoked capsules",
            ));
        }
    }

    // Recompute the complete pre-fence state from an independent checkpoint
    // and its journal suffix. A copied state digest in the fence receipt is
    // not an anchor: a corrupt store can otherwise rewrite both that receipt
    // field and its public integrity digest together.
    let anchor = replay_state_digest_prefix(store, event.seq.saturating_sub(1), &capsules)?;
    if event.seq > 1 {
        let previous = store
            .event(event.seq - 1)
            .map_err(|error| store_reply(error, meta.commit_seq))?
            .ok_or_else(|| {
                corrupt_reply(meta.commit_seq, "deletion fence anchor event is missing")
            })?;
        let previous_receipt = validate_recovery_event(&previous, previous.seq, meta.commit_seq)?;
        validate_common_maintenance_event(store, &previous, &previous_receipt)?;
        validate_event_payload_digest(&previous, &previous_receipt, &capsules)?;
        let previous_gap = snapshot_gap_contract(&previous, &previous_receipt, meta.commit_seq)?;
        if !anchor.opaque
            && previous_receipt.state_digest != anchor.digest
            && previous_gap
                .as_ref()
                .is_none_or(|gap| gap.terminal_state_digest != anchor.digest)
        {
            return Err(corrupt_reply(
                meta.commit_seq,
                "deletion fence anchor differs from prior receipt",
            ));
        }
    }
    if anchor.digest != *pre_fence_state_digest || anchor.digest != durable.state_digest {
        return Err(corrupt_reply(
            meta.commit_seq,
            "pending deletion fence pre-fence digest anchor mismatch",
        ));
    }
    Ok(Some(PendingDeletionFence {
        source: source.clone(),
        sources: sources.clone(),
        target_epoch: *target_epoch,
        idempotency_key: idempotency_key.clone(),
        payload_sha256: payload_sha256.clone(),
        deleted_records: *deleted_records,
        deleted_record_ids: deleted_record_ids.clone(),
        canonical_input: canonical_input.clone(),
        tick_before: event.created_tick,
        fatigue_before: *fatigue,
        steps_since_consolidation_before: *steps_since_consolidation,
        event_seq: event.seq,
        state_digest: durable.state_digest.clone(),
    }))
}

/// Replays the journal up to (and including) `target_seq` from the newest
/// checkpoint that precedes it, independently recomputing each receipt's
/// state digest. This is used for the deletion fence's pre-erasure anchor.
fn replay_state_digest_prefix(
    store: &NamespaceStore,
    target_seq: u64,
    capsules: &[StoredCapsule],
) -> Result<PrefixDigest, EngineReply> {
    let meta = store
        .meta()
        .map_err(|error| store_reply(error, target_seq))?;
    if target_seq > meta.commit_seq {
        return Err(corrupt_reply(
            meta.commit_seq,
            "state digest prefix exceeds metadata sequence",
        ));
    }
    let config: NcmConfig =
        serde_json::from_str(&store.identity().config_json).map_err(|error| {
            corrupt_reply(
                meta.commit_seq,
                &format!("decode store config for fence anchor: {error}"),
            )
        })?;
    let projections: tracedecay_memory_ncm_core::projections::ProjectionBundle =
        serde_json::from_slice(&store.identity().projection_bytes).map_err(|error| {
            corrupt_reply(
                meta.commit_seq,
                &format!("decode store projections for fence anchor: {error}"),
            )
        })?;
    let checkpoint = store
        .latest_checkpoint()
        .map_err(|error| store_reply(error, meta.commit_seq))?;
    let (mut kernel, mut applied_seq) = if let Some(checkpoint) = checkpoint {
        if checkpoint.seq > target_seq || checkpoint.epoch > meta.epoch {
            return Err(corrupt_reply(
                meta.commit_seq,
                "fence anchor checkpoint is from a later generation",
            ));
        }
        let envelope: CheckpointEnvelope =
            serde_json::from_slice(&checkpoint.state).map_err(|error| {
                corrupt_reply(
                    meta.commit_seq,
                    &format!("decode fence anchor checkpoint: {error}"),
                )
            })?;
        if envelope.state_digest != sha256_hex(&envelope.kernel.state_digest())
            || envelope.kernel.config != config
        {
            return Err(corrupt_reply(
                meta.commit_seq,
                "fence anchor checkpoint digest or config mismatch",
            ));
        }
        let projection_bytes =
            serde_json::to_vec(&envelope.kernel.projections).map_err(|error| {
                corrupt_reply(
                    meta.commit_seq,
                    &format!("serialize fence anchor projections: {error}"),
                )
            })?;
        if projection_bytes != store.identity().projection_bytes {
            return Err(corrupt_reply(
                meta.commit_seq,
                "fence anchor checkpoint projection mismatch",
            ));
        }
        validate_checkpoint_chain(store, checkpoint.seq, &envelope.kernel, &meta, capsules)?;
        (envelope.kernel, checkpoint.seq)
    } else {
        let mut kernel = NcmKernel::new(store.identity().seed, config)
            .map_err(|error| core_reply(error, meta.commit_seq))?;
        kernel.projections = projections;
        (kernel, 0)
    };
    if applied_seq == target_seq {
        return Ok(PrefixDigest {
            digest: sha256_hex(&kernel.state_digest()),
            opaque: false,
        });
    }
    let events = store
        .events_after(applied_seq)
        .map_err(|error| store_reply(error, meta.commit_seq))?;
    let mut expected_seq = applied_seq
        .checked_add(1)
        .ok_or_else(|| corrupt_reply(meta.commit_seq, "fence anchor sequence overflow"))?;
    let mut expected_tick = kernel.scheduler.tick.0;
    let mut state_chain_valid = true;
    let mut erased_suffix_digest = None;
    let mut gap_terminal_anchor = false;
    for event in events {
        if event.seq > target_seq {
            break;
        }
        let durable = validate_recovery_event(&event, expected_seq, target_seq)?;
        validate_common_maintenance_event(store, &event, &durable)?;
        validate_event_payload_digest(&event, &durable, capsules)?;
        validate_deletion_completion_fence(store, &event, &durable, capsules, meta.commit_seq)?;
        let snapshot_gap = snapshot_gap_contract(&event, &durable, meta.commit_seq)?;
        let snapshot_observe =
            snapshot_observe_contract(&event, &durable, capsules, meta.commit_seq)?;
        expected_tick =
            validate_checkpoint_event_tick(&event, &durable, expected_tick, meta.commit_seq)?;
        if let Some(gap) = snapshot_gap {
            // A gap marker authenticates the opaque terminal state of the
            // omitted range.  A scrubbed observe may precede that range, so
            // the marker is allowed to replace the earlier unavailable
            // receipt digest with the range's terminal anchor.
            state_chain_valid = false;
            erased_suffix_digest = Some(gap.terminal_state_digest);
            gap_terminal_anchor = true;
        } else if snapshot_observe.is_some() {
            // A synthesized observe row carries a per-row contract digest,
            // not the imported kernel digest.  Treat it as the first opaque
            // point in the prefix just like a revoked observe.
            state_chain_valid = false;
            if !gap_terminal_anchor {
                erased_suffix_digest = Some(durable.state_digest.clone());
            }
        } else if state_chain_valid {
            if operation_contains_revoked_observe(&durable.operation, capsules) {
                state_chain_valid = false;
                erased_suffix_digest = Some(durable.state_digest.clone());
                gap_terminal_anchor = false;
            } else {
                replay_event(
                    &mut kernel,
                    &event,
                    &durable.operation,
                    capsules,
                    target_seq,
                )?;
                if sha256_hex(&kernel.state_digest()) != durable.state_digest {
                    return Err(corrupt_reply(
                        meta.commit_seq,
                        "fence anchor journal state digest mismatch",
                    ));
                }
            }
        } else {
            // Once a revoked observation has been scrubbed, its original
            // nonlinear state cannot be recomputed. Keep validating every
            // envelope, payload, and logical tick. An authenticated omission
            // contract already carries the exported terminal digest; retain
            // that stronger anchor through later scrubbed rows so an omitted
            // maintenance followed by a revoked observation does not replace
            // it with an unverifiable source receipt digest. Ordinary
            // deletion rows have no omission contract, so their latest
            // durable receipt remains the best available anchor.
            if !gap_terminal_anchor {
                erased_suffix_digest = Some(durable.state_digest.clone());
            }
        }
        applied_seq = event.seq;
        expected_seq = expected_seq
            .checked_add(1)
            .ok_or_else(|| corrupt_reply(meta.commit_seq, "fence anchor sequence overflow"))?;
    }
    if applied_seq != target_seq {
        return Err(corrupt_reply(
            meta.commit_seq,
            "fence anchor journal prefix is incomplete",
        ));
    }
    if let Some(digest) = erased_suffix_digest {
        Ok(PrefixDigest {
            digest,
            opaque: !state_chain_valid,
        })
    } else {
        Ok(PrefixDigest {
            digest: sha256_hex(&kernel.state_digest()),
            opaque: false,
        })
    }
}

/// Digest of a journal prefix, with whether the digest became opaque after an
/// erased observation or an authenticated snapshot omission. Once that point
/// is reached, a later receipt still has to pass envelope and payload checks,
/// but its original nonlinear state digest cannot be recomputed from scrubbed
/// inputs.
struct PrefixDigest {
    digest: String,
    opaque: bool,
}

pub(crate) fn replay_event(
    kernel: &mut NcmKernel,
    event: &Event,
    operation: &DurableOperation,
    capsules: &[StoredCapsule],
    commit_seq: u64,
) -> Result<(), EngineReply> {
    match operation {
        DurableOperation::CommonControl { operations, .. } => {
            if event.kind != "common_control" {
                return Err(corrupt_reply(
                    commit_seq,
                    "common control receipt kind mismatch",
                ));
            }
            for operation in operations {
                let mut inner = event.clone();
                inner.kind = match operation {
                    DurableOperation::Observe { .. } => "observe",
                    DurableOperation::Feedback { .. } => "feedback",
                    DurableOperation::Correction { .. } => "correction",
                    DurableOperation::Maintenance { .. } => "maintenance",
                    _ => {
                        return Err(corrupt_reply(
                            commit_seq,
                            "invalid common control operation",
                        ));
                    }
                }
                .to_owned();
                replay_event(kernel, &inner, operation, capsules, commit_seq)?;
            }
        }
        DurableOperation::Observe { record_id } => {
            if event.kind != "observe" {
                return Err(corrupt_reply(commit_seq, "observe receipt kind mismatch"));
            }
            let capsule = capsules
                .iter()
                .find(|capsule| capsule.record_id == *record_id && capsule.commit_seq == event.seq)
                .ok_or_else(|| corrupt_reply(commit_seq, "observe capsule is missing"))?;
            if capsule.status == crate::store::CapsuleStatus::Revoked {
                // Privacy erasure intentionally removes the embeddings before
                // ordinary recovery can see this row.  The deleted observe has
                // no replayable kernel effect; its durable tick/state anchor
                // is validated by the caller's erased-input path.
                if event.created_tick
                    != kernel
                        .scheduler
                        .tick
                        .0
                        .saturating_add(operation_tick_delta(operation))
                {
                    return Err(corrupt_reply(commit_seq, "event logical tick mismatch"));
                }
                return Ok(());
            }
            let report = kernel
                .observe(
                    &capsule.key_embedding,
                    &capsule.value_embedding,
                    NewRecord {
                        source: capsule.source_id.clone(),
                        key_text: capsule.key_text.clone(),
                        value_text: capsule.value_text.clone(),
                        affect: capsule.affect,
                        surprise: capsule.surprise,
                        intensity: capsule.intensity,
                    },
                )
                .map_err(|error| core_reply(error, commit_seq))?;
            if report.record_id != *record_id {
                return Err(corrupt_reply(
                    commit_seq,
                    "replayed record identifier mismatch",
                ));
            }
            let record = kernel
                .records
                .get(*record_id)
                .ok_or_else(|| corrupt_reply(commit_seq, "replayed record is missing"))?;
            if record.ltm_key != capsule.ltm_key {
                return Err(corrupt_reply(commit_seq, "replayed LTM key mismatch"));
            }
        }
        DurableOperation::Feedback { record_ids } => {
            if event.kind != "feedback" {
                return Err(corrupt_reply(commit_seq, "feedback receipt kind mismatch"));
            }
            kernel
                .feedback(record_ids)
                .map_err(|error| core_reply(error, commit_seq))?;
        }
        DurableOperation::Correction {
            superseded,
            superseding,
            evidence,
        } => {
            if event.kind != "correction" {
                return Err(corrupt_reply(
                    commit_seq,
                    "correction receipt kind mismatch",
                ));
            }
            kernel
                .correction(*superseded, *superseding, evidence.clone())
                .map_err(|error| core_reply(error, commit_seq))?;
        }
        DurableOperation::Maintenance { kind, .. } => {
            if event.kind != "maintenance" {
                return Err(corrupt_reply(
                    commit_seq,
                    "maintenance receipt kind mismatch",
                ));
            }
            apply_maintenance(kernel, kind).map_err(|error| core_reply(error, commit_seq))?;
        }
        DurableOperation::DeletionFence { .. } => {
            if event.kind != "deletion_fence" {
                return Err(corrupt_reply(
                    commit_seq,
                    "deletion fence receipt kind mismatch",
                ));
            }
        }
        DurableOperation::DeleteBySource { .. } => {
            if event.kind != "delete_by_source" {
                return Err(corrupt_reply(commit_seq, "deletion receipt kind mismatch"));
            }
        }
    }
    if kernel.scheduler.tick.0 != event.created_tick {
        return Err(corrupt_reply(commit_seq, "event logical tick mismatch"));
    }
    Ok(())
}

pub(crate) fn apply_maintenance(
    kernel: &mut NcmKernel,
    kind: &MaintenanceKind,
) -> Result<(), CoreError> {
    match kind {
        MaintenanceKind::Advance { ticks } => {
            kernel.advance(*ticks)?;
        }
        MaintenanceKind::Consolidate => {
            kernel.consolidate()?;
        }
        MaintenanceKind::MergePrune => {
            kernel.merge_prune()?;
        }
        MaintenanceKind::Checkpoint | MaintenanceKind::Compact => {}
    }
    Ok(())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn valid_source_set(sources: &[SourceId]) -> bool {
    !sources.is_empty()
        && sources.len() <= 1024
        && sources.iter().all(|source| !source.0.is_empty())
        && sources.windows(2).all(|window| window[0] < window[1])
}

fn deletion_input_matches(
    canonical_input: Option<&serde_json::Value>,
    sources: &[SourceId],
    payload_sha256: &str,
    seq: u64,
    expected_generation: Option<u64>,
) -> Result<bool, EngineReply> {
    let Some(canonical_input) = canonical_input else {
        // Receipts written before canonical deletion semantics were persisted
        // remain readable through their source-only event digest.
        return legacy_deletion_digest(sources, payload_sha256, seq).map(|_| true);
    };
    let Some(object) = canonical_input.as_object() else {
        return Ok(false);
    };
    let Some(generation) = object.get("expected_generation") else {
        return Ok(false);
    };
    if object.get("action").and_then(serde_json::Value::as_str) != Some("delete_by_source")
        || object.get("sources") != Some(&serde_json::json!(sources))
        || (!generation.is_null()
            && generation
                .as_u64()
                .is_none_or(|value| Some(value) != expected_generation))
    {
        return Ok(false);
    }
    let digest =
        canonical_deletion_digest(canonical_input).map_err(|reason| corrupt_reply(seq, &reason))?;
    Ok(digest == payload_sha256)
}

/// Canonicalizes the effect semantics while leaving the generation precondition
/// outside the idempotency identity. A retry may carry the generation that is
/// current when it arrives, but it must still carry a well-formed generation
/// field in the persisted request capsule.
fn canonical_deletion_digest(input: &serde_json::Value) -> Result<String, String> {
    let mut semantics = input.clone();
    if let Some(object) = semantics.as_object_mut() {
        object.remove("expected_generation");
    }
    super::canonical_digest(&semantics)
}

/// Accepts the two source-only deletion digests emitted before the full
/// canonical request capsule was introduced. Direct single-source deletion
/// historically hashed the source value itself; the multi-source path hashed
/// the sorted source array.
fn legacy_deletion_digest(
    sources: &[SourceId],
    payload_sha256: &str,
    seq: u64,
) -> Result<String, EngineReply> {
    let array_digest = canonical_source_digest(sources, seq)?;
    if payload_sha256 == array_digest {
        return Ok(array_digest);
    }
    if sources.len() == 1 {
        let single_digest =
            super::canonical_digest(&sources[0]).map_err(|reason| corrupt_reply(seq, &reason))?;
        if payload_sha256 == single_digest {
            return Ok(single_digest);
        }
    }
    Err(corrupt_reply(
        seq,
        "legacy deletion payload digest does not match its source set",
    ))
}

fn operation_contains_revoked_observe(
    operation: &DurableOperation,
    capsules: &[StoredCapsule],
) -> bool {
    match operation {
        DurableOperation::CommonControl { operations, .. } => operations
            .iter()
            .any(|operation| operation_contains_revoked_observe(operation, capsules)),
        DurableOperation::Observe { record_id } => capsules.iter().any(|capsule| {
            capsule.record_id == *record_id
                && capsule.status == crate::store::CapsuleStatus::Revoked
        }),
        DurableOperation::Feedback { .. }
        | DurableOperation::Correction { .. }
        | DurableOperation::Maintenance { .. }
        | DurableOperation::DeletionFence { .. }
        | DurableOperation::DeleteBySource { .. } => false,
    }
}

fn operation_tick_delta(operation: &DurableOperation) -> u64 {
    match operation {
        DurableOperation::CommonControl { operations, .. } => {
            operations.iter().fold(0_u64, |total, operation| {
                total.saturating_add(operation_tick_delta(operation))
            })
        }
        DurableOperation::Observe { .. } => 1,
        DurableOperation::Maintenance { kind, .. } => match kind {
            MaintenanceKind::Advance { ticks } => u64::from(*ticks),
            MaintenanceKind::Consolidate
            | MaintenanceKind::MergePrune
            | MaintenanceKind::Checkpoint
            | MaintenanceKind::Compact => 0,
        },
        DurableOperation::Feedback { .. }
        | DurableOperation::Correction { .. }
        | DurableOperation::DeletionFence { .. }
        | DurableOperation::DeleteBySource { .. } => 0,
    }
}

fn canonical_source_digest(sources: &[SourceId], seq: u64) -> Result<String, EngineReply> {
    super::canonical_digest(sources).map_err(|reason| corrupt_reply(seq, &reason))
}

fn observe_payload_digest(
    capsule: &StoredCapsule,
    expected: &str,
    seq: u64,
) -> Result<String, EngineReply> {
    let digests = canonical_observe_payload_digests(capsule, seq)?;
    if digests.iter().any(|digest| digest == expected) {
        Ok(expected.to_owned())
    } else {
        Err(corrupt_reply(
            seq,
            "observe payload digest cannot be reconstructed",
        ))
    }
}

/// Reconstructs the canonical observe payload digest from a retained capsule.
/// Snapshot staging uses this same routine when it has to synthesize a missing
/// observe journal row, keeping the event payload bound to the retained
/// capsule rather than inventing a source digest.
pub(crate) fn canonical_observe_payload_digest(
    capsule: &StoredCapsule,
    seq: u64,
) -> Result<String, EngineReply> {
    canonical_observe_payload_digests(capsule, seq)?
        .into_iter()
        .next()
        .ok_or_else(|| corrupt_reply(seq, "observe payload digest candidates are empty"))
}

fn canonical_observe_payload_digests(
    capsule: &StoredCapsule,
    seq: u64,
) -> Result<Vec<String>, EngineReply> {
    #[derive(Serialize)]
    struct Payload<'a> {
        source: &'a SourceId,
        key_text: &'a str,
        value_text: &'a str,
        affect: &'a Option<ObserveAffect>,
        surprise: f32,
        intensity: f32,
        provenance: &'a serde_json::Value,
    }

    let mut provenance = serde_json::from_str::<serde_json::Value>(&capsule.provenance)
        .map_err(|_| corrupt_reply(seq, "observe capsule provenance is invalid"))?;
    if let Some(object) = provenance.as_object_mut() {
        object.remove("delivery_capsule");
    }
    // The persisted capsule stores the resolved affect vector, while the
    // original request may have encoded that same vector as neutral `null`,
    // explicit channels, or one of the pinned preset spellings.  Preserve
    // all equivalent wire representations so raw and typed observations are
    // validated against the exact digest they originally committed.
    let mut affects = vec![None, Some(ObserveAffect::Values(capsule.affect.0))];
    for name in [
        "positive",
        "curious",
        "negative",
        "stressed",
        "social",
        "dopamin",
        "serotonin",
        "kortizol",
        "oxytocin",
    ] {
        if tracedecay_memory_ncm_core::signals::affect::from_name(name)
            .ok()
            .is_some_and(|affect| affect.0 == capsule.affect.0)
        {
            affects.push(Some(ObserveAffect::Preset(name.to_owned())));
        }
    }
    affects
        .iter()
        .map(|affect| {
            super::canonical_digest(&Payload {
                source: &capsule.source_id,
                key_text: &capsule.key_text,
                value_text: &capsule.value_text,
                affect,
                surprise: capsule.surprise,
                intensity: capsule.intensity,
                provenance: &provenance,
            })
            .map_err(|reason| corrupt_reply(seq, &reason))
        })
        .collect()
}
