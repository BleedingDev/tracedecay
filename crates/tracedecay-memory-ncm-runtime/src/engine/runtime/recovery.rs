use super::super::*;
use super::util::{core_reply, corrupt_reply, sha256_hex, store_reply, validate_common_capsule};
use crate::store::{Event, NamespaceStore, StoreMeta, StoredCapsule};
use serde::Serialize;
use tracedecay_memory_ncm_core::kernel::{NcmKernel, NewRecord};
use tracedecay_memory_ncm_core::types::{CoreError, NcmConfig};

/// The durable deletion fence that must be completed before a namespace can publish a
/// reconstructed kernel.
#[derive(Clone, Debug)]
pub(crate) struct PendingDeletionFence {
    pub(crate) source: SourceId,
    pub(crate) target_epoch: u64,
    pub(crate) idempotency_key: String,
    pub(crate) payload_sha256: String,
    pub(crate) deleted_records: u64,
    pub(crate) tick_before: u64,
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
        validate_event_payload_digest(&event, &durable)?;
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
        DurableOperation::Maintenance { .. } => event.kind == "maintenance",
        DurableOperation::DeletionFence { .. } => event.kind == "deletion_fence",
        DurableOperation::DeleteBySource { .. } => event.kind == "delete_by_source",
    };
    if !kind_matches {
        return Err(corrupt_reply(commit_seq, "receipt operation kind mismatch"));
    }
    Ok(durable)
}

/// Verifies payload digests that are reconstructible from the durable operation.
///
/// Observe payloads intentionally remain opaque after source erasure because
/// the capsule no longer retains their source text/provenance. Their digest is
/// still required to be a well-formed SHA-256 by [`validate_recovery_event`].
pub(crate) fn validate_event_payload_digest(
    event: &Event,
    durable: &DurableReceipt,
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
        DurableOperation::DeletionFence { payload_sha256, .. } => {
            if !is_sha256(payload_sha256) {
                return Err(corrupt_reply(
                    event.seq,
                    "deletion fence payload digest is invalid",
                ));
            }
            Some(payload_sha256.clone())
        }
        DurableOperation::Observe { .. }
        | DurableOperation::CommonControl { .. }
        | DurableOperation::DeleteBySource { .. } => None,
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
        validate_event_payload_digest(event, &durable)?;
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
        target_epoch,
        idempotency_key,
        payload_sha256,
        deleted_records,
    } = &durable.operation
    else {
        return Err(corrupt_reply(
            meta.commit_seq,
            "pending deletion fence is not the final journal event",
        ));
    };
    if event.kind != "deletion_fence"
        || event.seq != meta.commit_seq
        || event.created_tick != meta.tick
        || durable.reply.payload["fenced"] != true
        || durable.reply.payload["target_epoch"] != *target_epoch
        || durable.reply.state_generation != event.seq
        || event.payload_sha256 != *payload_sha256
        || idempotency_key.is_empty()
        || idempotency_key.len() > MAX_IDEMPOTENCY_KEY_BYTES
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
    if !revocations
        .iter()
        .any(|revocation| {
            revocation.source_id == *source
                && revocation.epoch == *target_epoch
                && revocation.seq == event.seq
        })
    {
        return Err(corrupt_reply(
            meta.commit_seq,
            "pending deletion fence is not bound to revocation metadata",
        ));
    }
    let revoked_records = capsules
        .iter()
        .filter(|capsule| capsule.status == crate::store::CapsuleStatus::Revoked)
        .count();
    if usize::try_from(*deleted_records).is_ok_and(|count| count > revoked_records) {
        return Err(corrupt_reply(
            meta.commit_seq,
            "pending deletion fence record count exceeds revoked capsules",
        ));
    }
    Ok(Some(PendingDeletionFence {
        source: source.clone(),
        target_epoch: *target_epoch,
        idempotency_key: idempotency_key.clone(),
        payload_sha256: payload_sha256.clone(),
        deleted_records: *deleted_records,
        tick_before: event.created_tick,
        event_seq: event.seq,
        state_digest: durable.state_digest.clone(),
    }))
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
