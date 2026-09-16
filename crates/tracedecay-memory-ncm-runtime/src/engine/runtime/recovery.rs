use super::super::*;
use super::util::{core_reply, corrupt_reply, sha256_hex, store_reply, validate_common_capsule};
use crate::store::{Event, NamespaceStore, StoreMeta, StoredCapsule};
use serde::Serialize;
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
    pub(crate) tick_before: u64,
    pub(crate) fatigue_before: f32,
    pub(crate) steps_since_consolidation_before: u64,
    pub(crate) event_seq: u64,
    pub(crate) state_digest: String,
}

pub(super) fn recover_kernel(
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
    for event in events {
        let expected = applied_seq
            .checked_add(1)
            .ok_or_else(|| corrupt_reply(meta.commit_seq, "event sequence overflow"))?;
        let durable = validate_recovery_event(&event, expected, meta.commit_seq)?;
        // Keep revoked capsules in the validation view.  Their retained
        // inputs are deliberately erased, but the journal still contains the
        // original observe event and needs the explicit revoked exception in
        // `validate_event_payload_digest` rather than looking like a missing
        // capsule.
        validate_event_payload_digest(&event, &durable, &all_capsules)?;
        replay_event(
            &mut kernel,
            &event,
            &durable.operation,
            &all_capsules,
            meta.commit_seq,
        )?;
        if sha256_hex(&kernel.state_digest()) != durable.state_digest {
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
    for event in events {
        if event.seq > checkpoint_seq {
            break;
        }
        let durable = validate_recovery_event(&event, expected_seq, meta.commit_seq)?;
        validate_event_payload_digest(&event, &durable, capsules)?;
        expected_tick = expected_tick.saturating_add(operation_tick_delta(&durable.operation));
        if event.created_tick != expected_tick {
            return Err(corrupt_reply(
                meta.commit_seq,
                "checkpoint journal tick does not match its operation history",
            ));
        }
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
        DurableOperation::Maintenance { kind } => {
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
        DurableOperation::Maintenance { kind } => {
            if event.kind == "snapshot_restore" && matches!(kind, MaintenanceKind::Checkpoint) {
                return Ok(());
            }
            if let Some(common) = durable.reply.payload.get("common_maintenance") {
                let digest = common
                    .get("request_semantic_sha256")
                    .and_then(serde_json::Value::as_str)
                    .filter(|digest| is_sha256(digest))
                    .ok_or_else(|| {
                        corrupt_reply(event.seq, "common maintenance payload digest is invalid")
                    })?;
                Some(digest.to_owned())
            } else {
                Some(
                    super::canonical_digest(kind)
                        .map_err(|reason| corrupt_reply(event.seq, &reason))?,
                )
            }
        }
        DurableOperation::DeletionFence { sources, .. } => {
            if !valid_source_set(sources) {
                return Err(corrupt_reply(
                    event.seq,
                    "deletion fence source set is invalid",
                ));
            }
            Some(canonical_source_digest(sources, event.seq)?)
        }
        DurableOperation::DeleteBySource { sources, .. } => {
            if !valid_source_set(sources) {
                return Err(corrupt_reply(event.seq, "deletion source set is invalid"));
            }
            Some(canonical_source_digest(sources, event.seq)?)
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
    let mut previous_tick = None;
    let mut validated = Vec::with_capacity(events.len());
    for event in &events {
        let durable = validate_recovery_event(event, expected_seq, meta.commit_seq)?;
        validate_event_payload_digest(event, &durable, &capsules)?;
        if previous_tick.is_some_and(|tick| event.created_tick < tick) {
            return Err(corrupt_reply(
                meta.commit_seq,
                "event scheduler ticks are not monotonic",
            ));
        }
        previous_tick = Some(event.created_tick);
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
    } = &durable.operation
    else {
        return Err(corrupt_reply(
            meta.commit_seq,
            "pending deletion fence is not the final journal event",
        ));
    };
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
        validate_event_payload_digest(&previous, &previous_receipt, &capsules)?;
        if previous_receipt.state_digest != anchor {
            return Err(corrupt_reply(
                meta.commit_seq,
                "deletion fence anchor differs from prior receipt",
            ));
        }
    }
    if anchor != *pre_fence_state_digest || anchor != durable.state_digest {
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
) -> Result<String, EngineReply> {
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
        return Ok(sha256_hex(&kernel.state_digest()));
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
    for event in events {
        if event.seq > target_seq {
            break;
        }
        let durable = validate_recovery_event(&event, expected_seq, target_seq)?;
        validate_event_payload_digest(&event, &durable, capsules)?;
        expected_tick = expected_tick.saturating_add(operation_tick_delta(&durable.operation));
        if event.created_tick != expected_tick {
            return Err(corrupt_reply(
                meta.commit_seq,
                "fence anchor journal tick does not match its operation history",
            ));
        }
        if state_chain_valid {
            if operation_contains_revoked_observe(&durable.operation, capsules) {
                state_chain_valid = false;
                erased_suffix_digest = Some(durable.state_digest.clone());
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
            // nonlinear state cannot be recomputed.  Keep validating every
            // envelope, payload, and logical tick, and carry the final
            // receipt digest as the opaque pre-fence anchor.
            erased_suffix_digest = Some(durable.state_digest.clone());
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
        Ok(digest)
    } else {
        Ok(sha256_hex(&kernel.state_digest()))
    }
}

pub(crate) fn replay_event(
    kernel: &mut NcmKernel,
    event: &Event,
    operation: &DurableOperation,
    capsules: &[StoredCapsule],
    commit_seq: u64,
) -> Result<(), EngineReply> {
    match operation {
        DurableOperation::CommonControl { operations } => {
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
        DurableOperation::Maintenance { kind } => {
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

fn operation_contains_revoked_observe(
    operation: &DurableOperation,
    capsules: &[StoredCapsule],
) -> bool {
    match operation {
        DurableOperation::CommonControl { operations } => operations
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
        DurableOperation::CommonControl { operations } => {
            operations.iter().fold(0_u64, |total, operation| {
                total.saturating_add(operation_tick_delta(operation))
            })
        }
        DurableOperation::Observe { .. } => 1,
        DurableOperation::Maintenance { kind } => match kind {
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
    let mut candidates = vec![None, Some(ObserveAffect::Values(capsule.affect.0))];
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
        candidates.push(Some(ObserveAffect::Preset(name.to_owned())));
    }
    for affect in candidates {
        let digest = super::canonical_digest(&Payload {
            source: &capsule.source_id,
            key_text: &capsule.key_text,
            value_text: &capsule.value_text,
            affect: &affect,
            surprise: capsule.surprise,
            intensity: capsule.intensity,
            provenance: &provenance,
        })
        .map_err(|reason| corrupt_reply(seq, &reason))?;
        if digest == expected {
            return Ok(digest);
        }
    }
    Err(corrupt_reply(
        seq,
        "observe payload digest cannot be reconstructed",
    ))
}
