use super::super::*;
use super::util::{core_reply, corrupt_reply, sha256_hex, store_reply, validate_common_capsule};
use crate::store::{Event, NamespaceStore, StoreMeta, StoredCapsule};
use tracedecay_memory_ncm_core::kernel::{NcmKernel, NewRecord};
use tracedecay_memory_ncm_core::types::{CoreError, NcmConfig};

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
        if event.seq != expected {
            return Err(corrupt_reply(meta.commit_seq, "event sequence gap"));
        }
        let durable: DurableReceipt = serde_json::from_str(&event.receipt)
            .map_err(|error| corrupt_reply(meta.commit_seq, &format!("decode receipt: {error}")))?;
        replay_event(
            &mut kernel,
            &event,
            &durable.operation,
            &capsules,
            meta.commit_seq,
        )?;
        if sha256_hex(&kernel.state_digest()) != durable.state_digest {
            return Err(corrupt_reply(
                meta.commit_seq,
                "replayed state digest mismatch",
            ));
        }
        if durable.reply.state_generation != event.seq {
            return Err(corrupt_reply(
                meta.commit_seq,
                "receipt generation mismatch",
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

fn replay_event(
    kernel: &mut NcmKernel,
    event: &Event,
    operation: &DurableOperation,
    capsules: &[StoredCapsule],
    commit_seq: u64,
) -> Result<(), EngineReply> {
    match operation {
        DurableOperation::CommonControl { operations } => {
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

pub(super) fn apply_maintenance(
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
