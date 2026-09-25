//! Source-scoped erasure by durable fencing and deterministic sanitized replay.

use crate::engine::{
    CheckpointEnvelope, DurableOperation, DurableReceipt, EngineReply, FaultPoint, MaintenanceKind,
    NamespaceHandle, NcmEngine, Outcome, RejectReason,
};
use crate::ports::Deadline;
use crate::store::{CapsuleStatus, NamespaceStore, StoreError, StoreMeta, StoredCapsule};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;
use tracedecay_memory_ncm_core::kernel::{NcmKernel, NewRecord};
use tracedecay_memory_ncm_core::projections::ProjectionBundle;
use tracedecay_memory_ncm_core::types::{CoreError, NcmConfig, RecordId, SourceId};

/// Idempotent source-deletion request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteRequest {
    /// Namespace-local idempotency key.
    pub idempotency_key: String,
    /// Opaque host-admitted source identity to revoke.
    pub source: SourceId,
    /// Remaining operation budget.
    pub deadline: Deadline,
}

/// Deterministic accounting for one sanitized reconstruction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SanitizedReplayReport {
    /// Retained source records replayed into the fresh kernel.
    pub replayed_records: u64,
    /// Revoked source records excluded from the fresh kernel.
    pub excluded_records: u64,
    /// Logical tick represented by the fenced pre-deletion generation.
    pub tick_before: u64,
    /// Logical tick after replaying only authorized retained inputs.
    pub tick_after: u64,
    /// Pure kernel digest of the sanitized generation.
    pub digest: [u8; 32],
}

pub(crate) struct ResumedRebuild {
    pub(crate) kernel: NcmKernel,
    pub(crate) meta: StoreMeta,
}

/// Deletes a source's capsules and all nonlinear learned influence.
#[must_use]
pub fn delete_by_source(
    engine: &NcmEngine,
    namespace: &str,
    request: DeleteRequest,
) -> EngineReply {
    delete_sources_inner(engine, namespace, request, None, None, None)
}

pub(crate) fn delete_sources(
    engine: &NcmEngine,
    namespace: &str,
    sources: &[SourceId],
    key: &str,
    deadline: Deadline,
    expected_generation: u64,
    bindings: Option<&[crate::source_binding::DeletionSourceBinding]>,
) -> EngineReply {
    if let Some(bindings) = bindings {
        if let Err(reason) =
            crate::source_binding::DeletionSourceBinding::validate_set(bindings, sources)
        {
            return EngineReply::rejected(RejectReason::InvalidRequest(reason), 0);
        }
    }
    let Some(first) = sources.first() else {
        return EngineReply::rejected(
            RejectReason::InvalidRequest("empty deletion source set".to_owned()),
            0,
        );
    };
    if sources.len() > 1024
        || sources.iter().any(|source| source.0.is_empty())
        || sources.iter().collect::<BTreeSet<_>>().len() != sources.len()
    {
        return EngineReply::rejected(
            RejectReason::InvalidRequest("invalid deletion source set".to_owned()),
            0,
        );
    }
    delete_sources_inner(
        engine,
        namespace,
        DeleteRequest {
            idempotency_key: key.to_owned(),
            source: first.clone(),
            deadline,
        },
        Some(sources),
        Some(expected_generation),
        bindings,
    )
}

fn delete_sources_inner(
    engine: &NcmEngine,
    namespace: &str,
    request: DeleteRequest,
    source_set: Option<&[SourceId]>,
    expected_generation: Option<u64>,
    bindings: Option<&[crate::source_binding::DeletionSourceBinding]>,
) -> EngineReply {
    let started = Instant::now();
    if request.deadline.remaining_ms == 0 {
        return EngineReply::new(Outcome::Cancelled, 0, Value::Null);
    }
    if let Err(reason) = validate_request(&request) {
        return EngineReply::rejected(RejectReason::InvalidRequest(reason), 0);
    }
    let sources = source_set
        .map(|sources| sources.iter().cloned().collect::<BTreeSet<_>>())
        .unwrap_or_else(|| BTreeSet::from([request.source.clone()]));
    let payload_sha256 = match if source_set.is_some() {
        serde_json::to_vec(&sources)
            .map(|bytes| sha256_hex(&bytes))
            .map_err(|error| error.to_string())
    } else {
        canonical_source_digest(&request.source)
    } {
        Ok(digest) => digest,
        Err(reason) => return EngineReply::rejected(RejectReason::InvalidRequest(reason), 0),
    };
    let mut namespaces = match engine.namespace_lock() {
        Ok(namespaces) => namespaces,
        Err(reply) => return reply,
    };
    let handle = match engine.ensure_handle(&mut namespaces, namespace, true) {
        Ok(Some(handle)) => handle,
        Ok(None) => return EngineReply::new(Outcome::Empty, 0, Value::Null),
        Err(reply) => return reply,
    };
    if handle.fenced {
        return unavailable_rebuilding(handle.commit_seq);
    }
    match lookup_replay(handle, &request.idempotency_key, &payload_sha256) {
        Ok(Some(reply)) => return reply,
        Ok(None) => {}
        Err(reply) => return reply,
    }
    if expected_generation.is_some_and(|expected| expected != handle.commit_seq) {
        return EngineReply::rejected(RejectReason::IdempotencyConflict, handle.commit_seq);
    }
    if remaining_ms(request.deadline, started) == 0 {
        return EngineReply::new(Outcome::Cancelled, handle.commit_seq, Value::Null);
    }
    if let Some(bindings) = bindings {
        for binding in bindings {
            match handle.store.has_retained_source(&binding.legacy_source_id) {
                Ok(false) => {}
                Ok(true) => {
                    return EngineReply::rejected(
                        RejectReason::InvalidRequest(
                            "targeted deletion cannot disambiguate retained legacy source identity"
                                .to_owned(),
                        ),
                        handle.commit_seq,
                    );
                }
                Err(error) => return store_reply(error, handle.commit_seq),
            }
        }
    }
    let live = match handle.live.read() {
        Ok(live) => Arc::clone(&live),
        Err(_) => return corrupt_reply(handle.commit_seq, "published kernel lock poisoned"),
    };
    let target_epoch = match handle.epoch.checked_add(1) {
        Some(epoch) => epoch,
        None => return corrupt_reply(handle.commit_seq, "privacy epoch overflow"),
    };
    let fence_seq = match handle.commit_seq.checked_add(1) {
        Some(seq) => seq,
        None => return corrupt_reply(handle.commit_seq, "commit sequence overflow"),
    };
    let capsules = match handle.store.capsules_in_commit_order(true) {
        Ok(capsules) => capsules,
        Err(error) => return store_reply(error, handle.commit_seq),
    };
    let mut matched_ids = BTreeSet::new();
    for capsule in &capsules {
        let matched = if bindings.is_some() {
            sources.contains(&capsule.source_id)
        } else {
            match crate::source_binding::matches_sources(
                namespace,
                &capsule.source_id,
                &capsule.provenance,
                &sources,
            ) {
                Ok(matched) => matched,
                Err(reason) => return corrupt_reply(handle.commit_seq, &reason),
            }
        };
        if matched {
            matched_ids.insert(capsule.record_id);
        }
    }
    let revoked_ids = capsules
        .iter()
        .filter(|capsule| {
            matched_ids.contains(&capsule.record_id) && capsule.status != CapsuleStatus::Revoked
        })
        .map(|capsule| capsule.record_id)
        .collect::<Vec<_>>();
    let fence_reply = EngineReply::new(
        Outcome::Success,
        fence_seq,
        json!({"fenced": true, "target_epoch": target_epoch}),
    );
    let fence_receipt = match durable_receipt(
        &fence_reply,
        DurableOperation::DeletionFence {
            source: request.source.clone(),
            target_epoch,
            idempotency_key: request.idempotency_key.clone(),
            payload_sha256: payload_sha256.clone(),
            deleted_records: u64::try_from(revoked_ids.len()).unwrap_or(u64::MAX),
        },
        &live,
    ) {
        Ok(receipt) => receipt,
        Err(reply) => return reply,
    };
    let prior_meta = match handle.store.meta() {
        Ok(meta) => meta,
        Err(error) => return store_reply(error, handle.commit_seq),
    };
    let mut mutation = match handle.store.begin_mutation() {
        Ok(mutation) => mutation,
        Err(error) => return store_reply(error, handle.commit_seq),
    };
    for source in &sources {
        if let Err(error) = mutation.add_revocation(&source.0, target_epoch, fence_seq) {
            return store_reply(error, handle.commit_seq);
        }
    }
    if let Err(error) = mutation.set_fence("rebuilding") {
        return store_reply(error, handle.commit_seq);
    }
    // Reapply erasure to older tombstones too, while counting only newly revoked records.
    for capsule in capsules
        .iter()
        .filter(|capsule| matched_ids.contains(&capsule.record_id))
    {
        if let Err(error) = mutation.mark_capsule_status(capsule.record_id, CapsuleStatus::Revoked)
        {
            return store_reply(error, handle.commit_seq);
        }
    }
    let event_seq = match mutation.append_event(
        "deletion_fence",
        None,
        &payload_sha256,
        &fence_receipt,
        live.scheduler.tick.0,
    ) {
        Ok(seq) => seq,
        Err(error) => return store_reply(error, handle.commit_seq),
    };
    if event_seq != fence_seq {
        return corrupt_reply(handle.commit_seq, "deletion fence event sequence mismatch");
    }
    if let Err(error) = mutation.set_meta(&StoreMeta {
        epoch: prior_meta.epoch,
        commit_seq: fence_seq,
        tick: live.scheduler.tick.0,
        fatigue: live.scheduler.fatigue,
        steps_since_consolidation: live.scheduler.steps_since_consolidation,
        last_maintenance: prior_meta.last_maintenance,
    }) {
        return store_reply(error, handle.commit_seq);
    }
    let committed = match mutation.commit() {
        Ok(seq) => seq,
        Err(error) => return store_reply(error, handle.commit_seq),
    };
    handle.commit_seq = committed;
    handle.fenced = true;
    if committed != fence_seq {
        return corrupt_reply(committed, "deletion fence commit sequence mismatch");
    }
    if engine.consume_fault(FaultPoint::AfterDeletionFenceCommit) {
        return EngineReply::new(
            Outcome::EffectUnknown,
            committed,
            json!({"rebuilding": true, "target_epoch": target_epoch}),
        );
    }
    if remaining_ms(request.deadline, started) == 0 {
        return EngineReply::new(
            Outcome::EffectUnknown,
            committed,
            json!({"rebuilding": true, "target_epoch": target_epoch}),
        );
    }
    let report = match sanitized_replay(&handle.store, &engine.config, live.scheduler.tick.0) {
        Ok(result) => result,
        Err(reply) => return reply,
    };
    finish_rebuild(
        handle,
        request.source,
        target_epoch,
        request.idempotency_key,
        payload_sha256,
        report,
        u64::try_from(revoked_ids.len()).unwrap_or(u64::MAX),
        Some(engine),
        request.deadline,
        started,
    )
}

/// Returns the durable revocation authority for snapshot sanitization.
pub fn revoked_sources(engine: &NcmEngine, namespace: &str) -> Result<Vec<SourceId>, EngineReply> {
    let mut namespaces = engine.namespace_lock()?;
    let handle = match engine.ensure_handle(&mut namespaces, namespace, false)? {
        Some(handle) => handle,
        None => return Ok(Vec::new()),
    };
    handle
        .store
        .revocations()
        .map(|rows| rows.into_iter().map(|row| row.source_id).collect())
        .map_err(|error| store_reply(error, handle.commit_seq))
}

pub(crate) fn resume_pending_rebuild(
    store: &mut NamespaceStore,
    config: &NcmConfig,
) -> Result<Option<ResumedRebuild>, EngineReply> {
    if store
        .fenced()
        .map_err(|error| store_reply(error, 0))?
        .is_none()
    {
        return Ok(None);
    }
    let meta = store.meta().map_err(|error| store_reply(error, 0))?;
    let events = store
        .events_after(0)
        .map_err(|error| store_reply(error, meta.commit_seq))?;
    let pending = events.iter().rev().find_map(|event| {
        serde_json::from_str::<DurableReceipt>(&event.receipt)
            .ok()
            .and_then(|receipt| match receipt.operation {
                DurableOperation::DeletionFence {
                    source,
                    target_epoch,
                    idempotency_key,
                    payload_sha256,
                    deleted_records,
                } => Some((
                    source,
                    target_epoch,
                    idempotency_key,
                    payload_sha256,
                    deleted_records,
                    event.created_tick,
                )),
                _ => None,
            })
    });
    let Some((source, target_epoch, idempotency_key, payload_sha256, deleted_records, tick_before)) =
        pending
    else {
        return Ok(None);
    };
    let report = sanitized_replay(store, config, tick_before)?;
    let (kernel, meta, _) = finish_store_rebuild(
        store,
        source,
        target_epoch,
        idempotency_key,
        payload_sha256,
        report,
        deleted_records,
    )?;
    Ok(Some(ResumedRebuild { kernel, meta }))
}

#[allow(clippy::too_many_arguments)]
fn finish_rebuild(
    handle: &mut NamespaceHandle,
    source: SourceId,
    target_epoch: u64,
    idempotency_key: String,
    payload_sha256: String,
    report: ReplayResult,
    deleted_records: u64,
    engine: Option<&NcmEngine>,
    deadline: Deadline,
    started: Instant,
) -> EngineReply {
    let (kernel, meta, reply) = match finish_store_rebuild(
        &mut handle.store,
        source,
        target_epoch,
        idempotency_key,
        payload_sha256,
        report,
        deleted_records,
    ) {
        Ok(result) => result,
        Err(reply) => return reply,
    };
    handle.commit_seq = meta.commit_seq;
    handle.epoch = meta.epoch;
    if let Some(engine) = engine
        && engine.consume_fault(FaultPoint::AfterCommitBeforePublish)
    {
        return EngineReply::new(
            Outcome::EffectUnknown,
            meta.commit_seq,
            json!({"commit_seq": meta.commit_seq}),
        );
    }
    let mut live = match handle.live.write() {
        Ok(live) => live,
        Err(_) => return corrupt_reply(meta.commit_seq, "published kernel lock poisoned"),
    };
    *live = Arc::new(kernel);
    handle.fenced = false;
    drop(live);
    if let Some(engine) = engine
        && engine.consume_fault(FaultPoint::AfterPublishBeforeAck)
    {
        return EngineReply::new(
            Outcome::EffectUnknown,
            meta.commit_seq,
            json!({"commit_seq": meta.commit_seq}),
        );
    }
    if remaining_ms(deadline, started) == 0 {
        return EngineReply::new(
            Outcome::EffectUnknown,
            meta.commit_seq,
            json!({"commit_seq": meta.commit_seq}),
        );
    }
    reply
}

fn finish_store_rebuild(
    store: &mut NamespaceStore,
    source: SourceId,
    target_epoch: u64,
    idempotency_key: String,
    payload_sha256: String,
    report: ReplayResult,
    deleted_records: u64,
) -> Result<(NcmKernel, StoreMeta, EngineReply), EngineReply> {
    store
        .compact(true)
        .map_err(|error| store_reply(error, store.meta().map_or(0, |meta| meta.commit_seq)))?;
    let prior_meta = store.meta().map_err(|error| store_reply(error, 0))?;
    let pending_seq = prior_meta
        .commit_seq
        .checked_add(1)
        .ok_or_else(|| corrupt_reply(prior_meta.commit_seq, "commit sequence overflow"))?;
    let public_report = SanitizedReplayReport {
        replayed_records: report.replayed_records,
        excluded_records: report.excluded_records,
        tick_before: report.tick_before,
        tick_after: report.kernel.scheduler.tick.0,
        digest: report.kernel.state_digest(),
    };
    let reply = EngineReply::new(
        Outcome::Success,
        pending_seq,
        json!({
            "source": source,
            "deleted_records": deleted_records,
            "epoch": target_epoch,
            "sanitized_replay": public_report,
            "replayed": false
        }),
    );
    let receipt = durable_receipt(
        &reply,
        DurableOperation::DeleteBySource {
            source,
            target_epoch,
            deleted_records,
        },
        &report.kernel,
    )?;
    let mut mutation = store
        .begin_mutation()
        .map_err(|error| store_reply(error, prior_meta.commit_seq))?;
    let event_seq = mutation
        .append_event(
            "delete_by_source",
            Some(&idempotency_key),
            &payload_sha256,
            &receipt,
            report.kernel.scheduler.tick.0,
        )
        .map_err(|error| store_reply(error, prior_meta.commit_seq))?;
    if event_seq != pending_seq {
        return Err(corrupt_reply(
            prior_meta.commit_seq,
            "deletion event sequence mismatch",
        ));
    }
    let checkpoint = checkpoint_bytes(&report.kernel, pending_seq)?;
    mutation
        .put_checkpoint(pending_seq, target_epoch, &checkpoint)
        .map_err(|error| store_reply(error, prior_meta.commit_seq))?;
    mutation
        .prune_checkpoints_before(pending_seq)
        .map_err(|error| store_reply(error, prior_meta.commit_seq))?;
    let meta = StoreMeta {
        epoch: target_epoch,
        commit_seq: pending_seq,
        tick: report.kernel.scheduler.tick.0,
        fatigue: report.kernel.scheduler.fatigue,
        steps_since_consolidation: report.kernel.scheduler.steps_since_consolidation,
        last_maintenance: Some("sanitized_rebuild".to_owned()),
    };
    mutation
        .set_meta(&meta)
        .map_err(|error| store_reply(error, prior_meta.commit_seq))?;
    mutation
        .clear_fence()
        .map_err(|error| store_reply(error, prior_meta.commit_seq))?;
    let committed = mutation
        .commit()
        .map_err(|error| store_reply(error, prior_meta.commit_seq))?;
    if committed != pending_seq {
        return Err(corrupt_reply(
            committed,
            "deletion commit sequence mismatch",
        ));
    }
    store
        .compact(true)
        .map_err(|error| store_reply(error, committed))?;
    Ok((report.kernel, meta, reply))
}

struct ReplayResult {
    kernel: NcmKernel,
    replayed_records: u64,
    excluded_records: u64,
    tick_before: u64,
}

fn sanitized_replay(
    store: &NamespaceStore,
    config: &NcmConfig,
    tick_before: u64,
) -> Result<ReplayResult, EngineReply> {
    let capsules = store
        .capsules_in_commit_order(true)
        .map_err(|error| store_reply(error, 0))?;
    let events = store
        .events_after(0)
        .map_err(|error| store_reply(error, 0))?;
    let projections: ProjectionBundle = serde_json::from_slice(&store.identity().projection_bytes)
        .map_err(|error| corrupt_reply(0, &format!("decode persisted projections: {error}")))?;
    let mut kernel = NcmKernel::new(store.identity().seed, config.clone())
        .map_err(|error| core_reply(error, 0))?;
    kernel.projections = projections;
    let by_record = capsules
        .iter()
        .map(|capsule| (capsule.record_id, capsule))
        .collect::<BTreeMap<_, _>>();
    let mut old_to_new = BTreeMap::new();
    let mut replayed_records = 0_u64;
    let excluded_records = u64::try_from(
        capsules
            .iter()
            .filter(|capsule| capsule.status == CapsuleStatus::Revoked)
            .count(),
    )
    .unwrap_or(u64::MAX);
    for event in events {
        let durable: DurableReceipt = serde_json::from_str(&event.receipt)
            .map_err(|error| corrupt_reply(event.seq, &format!("decode receipt: {error}")))?;
        let operations = match durable.operation {
            DurableOperation::CommonControl { operations } => operations,
            operation => vec![operation],
        };
        for operation in operations {
            match operation {
                DurableOperation::CommonControl { .. } => {
                    return Err(corrupt_reply(event.seq, "nested common control operation"));
                }
                DurableOperation::Observe { record_id } => {
                    let capsule = by_record.get(&record_id).ok_or_else(|| {
                        corrupt_reply(event.seq, "observe receipt capsule is missing")
                    })?;
                    if capsule.status == CapsuleStatus::Revoked {
                        continue;
                    }
                    let observed = kernel
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
                        .map_err(|error| core_reply(error, event.seq))?;
                    old_to_new.insert(record_id, observed.record_id);
                    replayed_records = replayed_records.saturating_add(1);
                }
                DurableOperation::Feedback { record_ids } => {
                    let retained = record_ids
                        .into_iter()
                        .filter_map(|record_id| old_to_new.get(&record_id).copied())
                        .collect::<Vec<_>>();
                    if !retained.is_empty() {
                        kernel
                            .feedback(&retained)
                            .map_err(|error| core_reply(error, event.seq))?;
                    }
                }
                DurableOperation::Correction {
                    superseded,
                    superseding,
                    evidence,
                } => {
                    if let (Some(old), Some(new)) = (
                        old_to_new.get(&superseded).copied(),
                        old_to_new.get(&superseding).copied(),
                    ) {
                        kernel
                            .correction(old, new, evidence)
                            .map_err(|error| core_reply(error, event.seq))?;
                    }
                }
                DurableOperation::Maintenance { kind } => {
                    apply_maintenance(&mut kernel, &kind)
                        .map_err(|error| core_reply(error, event.seq))?;
                }
                DurableOperation::DeletionFence { .. }
                | DurableOperation::DeleteBySource { .. } => {}
            }
        }
    }
    let next_id = capsules
        .iter()
        .map(|capsule| capsule.record_id.0)
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| corrupt_reply(0, "record identity overflow"))?
        .max(
            store
                .next_record_id()
                .map_err(|error| store_reply(error, 0))?,
        );
    kernel = remap_record_ids(kernel, &old_to_new, next_id)?;
    Ok(ReplayResult {
        kernel,
        replayed_records,
        excluded_records,
        tick_before,
    })
}

fn apply_maintenance(kernel: &mut NcmKernel, kind: &MaintenanceKind) -> Result<(), CoreError> {
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

fn remap_record_ids(
    kernel: NcmKernel,
    old_to_new: &BTreeMap<RecordId, RecordId>,
    next_id: u64,
) -> Result<NcmKernel, EngineReply> {
    let new_to_old = old_to_new
        .iter()
        .map(|(old, new)| (new.0, old.0))
        .collect::<BTreeMap<_, _>>();
    let mut value = serde_json::to_value(kernel)
        .map_err(|error| corrupt_reply(0, &format!("serialize sanitized kernel: {error}")))?;
    remap_record_table(&mut value, &new_to_old, next_id)?;
    for layer in ["stm", "ltm"] {
        let centers = value
            .get_mut(layer)
            .ok_or_else(|| corrupt_reply(0, "kernel center bank missing"))?;
        remap_optional_ids(
            centers
                .get_mut("record")
                .and_then(Value::as_array_mut)
                .ok_or_else(|| corrupt_reply(0, "kernel center record array missing"))?,
            &new_to_old,
        );
        let support = centers
            .get_mut("support")
            .and_then(Value::as_array_mut)
            .ok_or_else(|| corrupt_reply(0, "kernel center support array missing"))?;
        for records in support {
            if let Some(records) = records.as_array_mut() {
                remap_numeric_ids(records, &new_to_old);
            }
        }
    }
    if let Some(layers) = value
        .get_mut("support")
        .and_then(|support| support.get_mut("by_layer"))
        .and_then(Value::as_object_mut)
    {
        for slots in layers.values_mut().filter_map(Value::as_object_mut) {
            for slot in slots.values_mut() {
                if let Some(records) = slot.get_mut("records").and_then(Value::as_array_mut) {
                    remap_numeric_ids(records, &new_to_old);
                }
            }
        }
    }
    serde_json::from_value(value)
        .map_err(|error| corrupt_reply(0, &format!("decode remapped sanitized kernel: {error}")))
}

fn remap_record_table(
    kernel: &mut Value,
    new_to_old: &BTreeMap<u64, u64>,
    next_id: u64,
) -> Result<(), EngineReply> {
    let table = kernel
        .get_mut("records")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| corrupt_reply(0, "kernel record table missing"))?;
    table.insert("next_id".to_owned(), Value::from(next_id));
    let records = table
        .get_mut("records")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| corrupt_reply(0, "kernel record map missing"))?;
    let original = std::mem::take(records);
    let mut remapped = Map::new();
    for (new_key, mut record) in original {
        let new_id = new_key
            .parse::<u64>()
            .map_err(|_| corrupt_reply(0, "record map key is not numeric"))?;
        let old_id = new_to_old
            .get(&new_id)
            .copied()
            .ok_or_else(|| corrupt_reply(0, "sanitized record remap is incomplete"))?;
        if let Some(object) = record.as_object_mut() {
            object.insert("id".to_owned(), Value::from(old_id));
            if let Some(by) = object
                .get_mut("state")
                .and_then(|state| state.get_mut("Superseded"))
                .and_then(|state| state.get_mut("by"))
            {
                remap_numeric_value(by, new_to_old);
            }
        }
        remapped.insert(old_id.to_string(), record);
    }
    *records = remapped;
    Ok(())
}

fn remap_optional_ids(values: &mut [Value], map: &BTreeMap<u64, u64>) {
    for value in values {
        if !value.is_null() {
            remap_numeric_value(value, map);
        }
    }
}

fn remap_numeric_ids(values: &mut [Value], map: &BTreeMap<u64, u64>) {
    for value in values {
        remap_numeric_value(value, map);
    }
}

fn remap_numeric_value(value: &mut Value, map: &BTreeMap<u64, u64>) {
    if let Some(id) = value.as_u64()
        && let Some(remapped) = map.get(&id)
    {
        *value = Value::from(*remapped);
    }
}

fn lookup_replay(
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
    let mut durable: DurableReceipt = serde_json::from_str(&receipt_json)
        .map_err(|error| corrupt_reply(seq, &format!("decode deletion receipt: {error}")))?;
    if durable.reply.state_generation != seq {
        return Err(corrupt_reply(seq, "deletion receipt sequence mismatch"));
    }
    if let Some(object) = durable.reply.payload.as_object_mut() {
        object.insert("replayed".to_owned(), Value::Bool(true));
    }
    Ok(Some(durable.reply))
}

fn durable_receipt(
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

fn checkpoint_bytes(kernel: &NcmKernel, seq: u64) -> Result<Vec<u8>, EngineReply> {
    serde_json::to_vec(&CheckpointEnvelope {
        kernel: kernel.clone(),
        state_digest: sha256_hex(&kernel.state_digest()),
    })
    .map_err(|error| corrupt_reply(seq, &format!("serialize checkpoint: {error}")))
}

fn validate_request(request: &DeleteRequest) -> Result<(), String> {
    if request.idempotency_key.is_empty() {
        return Err("idempotency key cannot be empty".to_owned());
    }
    if request.idempotency_key.len() > 256 {
        return Err("idempotency key exceeds 256 bytes".to_owned());
    }
    if request.source.0.is_empty() {
        return Err("source identity cannot be empty".to_owned());
    }
    Ok(())
}

fn canonical_source_digest(source: &SourceId) -> Result<String, String> {
    serde_json::to_vec(source)
        .map(|bytes| sha256_hex(&bytes))
        .map_err(|error| format!("serialize deletion payload: {error}"))
}

fn remaining_ms(deadline: Deadline, started: Instant) -> u64 {
    let elapsed = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    deadline.remaining_ms.saturating_sub(elapsed)
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn unavailable_rebuilding(commit_seq: u64) -> EngineReply {
    EngineReply::new(
        Outcome::Unavailable("rebuilding".to_owned()),
        commit_seq,
        Value::Null,
    )
}

fn corrupt_reply(commit_seq: u64, reason: &str) -> EngineReply {
    EngineReply::new(Outcome::Corrupt, commit_seq, json!({"reason": reason}))
}

fn core_reply(error: CoreError, commit_seq: u64) -> EngineReply {
    match error {
        CoreError::BudgetExceeded(_) | CoreError::CapacityExhausted(_) => {
            EngineReply::new(Outcome::BudgetExceeded, commit_seq, Value::Null)
        }
        CoreError::UnknownRecord(record_id) => {
            EngineReply::rejected(RejectReason::UnknownRecord(record_id), commit_seq)
        }
        CoreError::InvalidState(reason) => corrupt_reply(commit_seq, &reason),
        CoreError::Unsupported(_) => {
            EngineReply::new(Outcome::Unsupported, commit_seq, Value::Null)
        }
        other => EngineReply::rejected(RejectReason::InvalidRequest(other.to_string()), commit_seq),
    }
}

fn store_reply(error: StoreError, commit_seq: u64) -> EngineReply {
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
        StoreError::SourceRevoked => EngineReply::rejected(RejectReason::SourceRevoked, commit_seq),
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

#[allow(dead_code)]
fn retained_sources(capsules: &[StoredCapsule]) -> BTreeSet<SourceId> {
    capsules
        .iter()
        .filter(|capsule| capsule.status != CapsuleStatus::Revoked)
        .map(|capsule| capsule.source_id.clone())
        .collect()
}
