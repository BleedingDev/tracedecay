//! Source-scoped erasure by durable fencing and deterministic sanitized replay.

use crate::engine::{
    CheckpointEnvelope, DurableOperation, DurableReceipt, EngineReply, FaultPoint, MaintenanceKind,
    NamespaceHandle, NcmEngine, Outcome, PendingDeletionFence, RejectReason,
    durable_integrity_digest, replay_recovery_event, validate_deletion_completion_fence,
    validate_event_payload_digest, validate_pending_deletion_fence, validate_recovery_event,
};
use crate::ports::Deadline;
use crate::store::{CapsuleStatus, Event, NamespaceStore, StoreError, StoreMeta, StoredCapsule};
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
    delete_sources_inner(engine, namespace, request, None, None, None, None)
}

pub(crate) fn delete_sources(
    engine: &NcmEngine,
    namespace: &str,
    sources: &[SourceId],
    key: &str,
    deadline: Deadline,
    expected_generation: u64,
    bindings: Option<&[crate::source_binding::DeletionSourceBinding]>,
    canonical_input: Option<Value>,
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
        canonical_input,
    )
}

fn delete_sources_inner(
    engine: &NcmEngine,
    namespace: &str,
    request: DeleteRequest,
    source_set: Option<&[SourceId]>,
    expected_generation: Option<u64>,
    bindings: Option<&[crate::source_binding::DeletionSourceBinding]>,
    canonical_input: Option<Value>,
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
    // All durable deletion envelopes use the sorted source set as their
    // canonical identity.  The common-control caller may provide sources in
    // any order, so do not let its first element become a second, unstable
    // source identity in the fence and completion receipts.
    let Some(canonical_source) = sources.first().cloned() else {
        return EngineReply::rejected(
            RejectReason::InvalidRequest("empty deletion source set".to_owned()),
            0,
        );
    };
    let mut request = request;
    request.source = canonical_source;
    let canonical_input = canonical_input
        .unwrap_or_else(|| deletion_request_input(&sources, expected_generation, None));
    let payload_sha256 = match canonical_deletion_digest(&canonical_input) {
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
        Ok(Some(reply)) => {
            if matches!(
                &reply.outcome,
                Outcome::Rejected(RejectReason::IdempotencyConflict)
            ) {
                match lookup_legacy_deletion_replay(
                    handle,
                    &request.idempotency_key,
                    &sources,
                    &canonical_input,
                ) {
                    Ok(Some(legacy)) => return legacy,
                    Ok(None) => {}
                    Err(reply) => return reply,
                }
            }
            if matches!(reply.outcome, Outcome::Success) {
                if let Err(reply) = validate_completed_deletion_replay(
                    handle,
                    &request.idempotency_key,
                    &payload_sha256,
                    &sources,
                ) {
                    return reply;
                }
            }
            return reply;
        }
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
    let deleted_records = match u64::try_from(revoked_ids.len()) {
        Ok(count) => count,
        Err(_) => return corrupt_reply(handle.commit_seq, "deleted record count overflow"),
    };
    let persisted_sources = sources.iter().cloned().collect::<Vec<_>>();
    let pre_fence_state_digest = sha256_hex(&live.state_digest());
    let fence_reply = EngineReply::new(
        Outcome::Success,
        fence_seq,
        json!({"fenced": true, "target_epoch": target_epoch}),
    );
    let fence_receipt = match durable_receipt(
        &fence_reply,
        DurableOperation::DeletionFence {
            source: request.source.clone(),
            sources: persisted_sources.clone(),
            target_epoch,
            idempotency_key: request.idempotency_key.clone(),
            payload_sha256: payload_sha256.clone(),
            deleted_records,
            deleted_record_ids: revoked_ids.clone(),
            pre_fence_state_digest: pre_fence_state_digest.clone(),
            fatigue: live.scheduler.fatigue,
            steps_since_consolidation: live.scheduler.steps_since_consolidation,
            canonical_input: Some(canonical_input.clone()),
        },
        &live,
        None,
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
    let pending = match validate_pending_deletion_fence(&handle.store) {
        Ok(Some(pending)) => pending,
        Ok(None) => return corrupt_reply(handle.commit_seq, "deletion fence disappeared"),
        Err(reply) => return reply,
    };
    if pending.tick_before != live.scheduler.tick.0
        || pending.fatigue_before != live.scheduler.fatigue
        || pending.steps_since_consolidation_before != live.scheduler.steps_since_consolidation
        || pending.state_digest != sha256_hex(&live.state_digest())
    {
        return corrupt_reply(
            handle.commit_seq,
            "deletion fence does not match the pre-fence scheduler state",
        );
    }
    let report = match sanitized_replay(
        &handle.store,
        &engine.config,
        &pending,
        Some(&pending.state_digest),
    ) {
        Ok(result) => result,
        Err(reply) => return reply,
    };
    finish_rebuild(
        handle,
        request.source,
        target_epoch,
        request.idempotency_key,
        payload_sha256,
        Some(canonical_input),
        report,
        deleted_records,
        persisted_sources,
        revoked_ids,
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
        .as_deref()
        == Some("compacting")
    {
        return finish_compacting_rebuild(store, config).map(Some);
    }
    let Some(pending) = validate_pending_deletion_fence(store)? else {
        return Ok(None);
    };
    let report = sanitized_replay(store, config, &pending, None)?;
    let (kernel, meta, _) = finish_store_rebuild(
        store,
        pending.source,
        pending.sources,
        pending.target_epoch,
        pending.idempotency_key,
        pending.payload_sha256,
        pending.canonical_input,
        report,
        pending.deleted_records,
        pending.deleted_record_ids,
        None,
    )?;
    Ok(Some(ResumedRebuild { kernel, meta }))
}

/// Completes the durable post-rebuild phase after a crash between the
/// completion journal commit and physical compaction/fence clearing.
fn finish_compacting_rebuild(
    store: &mut NamespaceStore,
    config: &NcmConfig,
) -> Result<ResumedRebuild, EngineReply> {
    let meta = store.meta().map_err(|error| store_reply(error, 0))?;
    let event = store
        .event(meta.commit_seq)
        .map_err(|error| store_reply(error, meta.commit_seq))?
        .ok_or_else(|| corrupt_reply(meta.commit_seq, "compacting completion event is missing"))?;
    let durable: DurableReceipt = serde_json::from_str(&event.receipt).map_err(|error| {
        corrupt_reply(
            meta.commit_seq,
            &format!("decode compacting completion receipt: {error}"),
        )
    })?;
    validate_recovery_event(&event, meta.commit_seq, meta.commit_seq)?;
    let capsules = store
        .capsules_in_commit_order(true)
        .map_err(|error| store_reply(error, meta.commit_seq))?;
    validate_event_payload_digest(&event, &durable, &capsules)?;
    let DurableOperation::DeleteBySource {
        source: completion_source,
        target_epoch,
        sources,
        deleted_records,
        deleted_record_ids,
        ..
    } = &durable.operation
    else {
        return Err(corrupt_reply(
            meta.commit_seq,
            "compacting fence is not bound to a deletion completion",
        ));
    };
    if event.kind != "delete_by_source"
        || event.idempotency_key.is_none()
        || durable.reply.outcome != Outcome::Success
        || durable.reply.state_generation != event.seq
        || *target_epoch != meta.epoch
        || event.created_tick != meta.tick
        || !valid_deletion_source_set(sources)
        || sources.first() != Some(completion_source)
        || durable.reply.payload["source"] != serde_json::json!(completion_source)
        || durable.reply.payload["deleted_records"] != serde_json::json!(deleted_records)
        || durable.reply.payload["epoch"] != serde_json::json!(target_epoch)
        || durable.reply.payload["replayed"] != false
    {
        return Err(corrupt_reply(
            meta.commit_seq,
            "compacting deletion completion does not match metadata",
        ));
    }
    validate_compacting_completion_fence(store, &event, &durable, &capsules)?;
    validate_deleted_record_set(
        deleted_records,
        deleted_record_ids,
        &capsules,
        event.seq.saturating_sub(1),
        meta.commit_seq,
    )?;
    // Validate the entire committed generation while the fence still blocks
    // readers. A malformed completion must leave the namespace fenced rather
    // than clearing the admission bit before recovery has accepted it.
    let kernel = crate::engine::recover_kernel(store, config, store.identity().seed, &meta)?;
    store
        .compact(true)
        .map_err(|error| store_reply(error, meta.commit_seq))?;
    let mut clear = store
        .begin_mutation()
        .map_err(|error| store_reply(error, meta.commit_seq))?;
    clear
        .clear_fence()
        .map_err(|error| store_reply(error, meta.commit_seq))?;
    let cleared = clear
        .commit_without_generation()
        .map_err(|error| store_reply(error, meta.commit_seq))?;
    if cleared != meta.commit_seq {
        return Err(corrupt_reply(
            cleared,
            "compacting fence clear sequence mismatch",
        ));
    }
    Ok(ResumedRebuild { kernel, meta })
}

fn validate_compacting_completion_fence(
    store: &NamespaceStore,
    completion_event: &Event,
    completion: &DurableReceipt,
    capsules: &[StoredCapsule],
) -> Result<(), EngineReply> {
    let commit_seq = completion_event.seq;
    let fence_seq = commit_seq
        .checked_sub(1)
        .ok_or_else(|| corrupt_reply(commit_seq, "compacting deletion fence sequence underflow"))?;
    let fence_event = store
        .event(fence_seq)
        .map_err(|error| store_reply(error, commit_seq))?
        .ok_or_else(|| corrupt_reply(commit_seq, "compacting deletion fence is missing"))?;
    let fence: DurableReceipt = serde_json::from_str(&fence_event.receipt).map_err(|error| {
        corrupt_reply(
            commit_seq,
            &format!("decode compacting deletion fence receipt: {error}"),
        )
    })?;
    validate_recovery_event(&fence_event, fence_seq, commit_seq)?;
    validate_event_payload_digest(&fence_event, &fence, capsules)?;
    let DurableOperation::DeleteBySource {
        source: completion_source,
        sources: completion_sources,
        target_epoch: completion_epoch,
        deleted_records: completion_deleted_records,
        deleted_record_ids: completion_deleted_record_ids,
        ..
    } = &completion.operation
    else {
        return Err(corrupt_reply(
            commit_seq,
            "compacting completion is not a source deletion",
        ));
    };
    let DurableOperation::DeletionFence {
        source: fence_source,
        sources: fence_sources,
        target_epoch: fence_epoch,
        idempotency_key: fence_key,
        payload_sha256: fence_payload,
        deleted_records: fence_deleted_records,
        deleted_record_ids: fence_deleted_record_ids,
        pre_fence_state_digest,
        ..
    } = &fence.operation
    else {
        return Err(corrupt_reply(
            commit_seq,
            "compacting completion is not preceded by a deletion fence",
        ));
    };
    let completion_key = completion_event
        .idempotency_key
        .as_deref()
        .ok_or_else(|| corrupt_reply(commit_seq, "compacting completion key is missing"))?;
    let report = completion
        .reply
        .payload
        .get("sanitized_replay")
        .and_then(Value::as_object)
        .ok_or_else(|| corrupt_reply(commit_seq, "compacting tick anchor is missing"))?;
    let tick_before = report
        .get("tick_before")
        .and_then(Value::as_u64)
        .ok_or_else(|| corrupt_reply(commit_seq, "compacting tick anchor is invalid"))?;
    let tick_after = report
        .get("tick_after")
        .and_then(Value::as_u64)
        .ok_or_else(|| corrupt_reply(commit_seq, "compacting tick anchor is invalid"))?;
    if fence_event.kind != "deletion_fence"
        || fence_event.idempotency_key.is_some()
        || fence_event.seq.checked_add(1) != Some(completion_event.seq)
        || completion_source != fence_source
        || completion_sources != fence_sources
        || completion_epoch != fence_epoch
        || completion_key != fence_key
        || completion_event.payload_sha256.as_str() != fence_payload.as_str()
        || completion_deleted_records != fence_deleted_records
        || completion_deleted_record_ids != fence_deleted_record_ids
        || completion_event.created_tick != tick_after
        || fence_event.created_tick != tick_before
        || tick_after > tick_before
        || completion.reply.payload["source"] != serde_json::json!(completion_source)
        || completion.reply.payload["deleted_records"]
            != serde_json::json!(completion_deleted_records)
        || completion.reply.payload["epoch"] != serde_json::json!(completion_epoch)
        || completion.reply.payload["replayed"] != false
        || !is_sha256_hex(pre_fence_state_digest)
        || fence.reply.payload["fenced"] != true
        || fence.reply.payload["target_epoch"] != serde_json::json!(fence_epoch)
        || fence.reply.state_generation != fence_event.seq
        || fence.state_digest != *pre_fence_state_digest
    {
        return Err(corrupt_reply(
            commit_seq,
            "compacting completion is not bound to its deletion fence",
        ));
    }
    let revocations = store
        .revocations()
        .map_err(|error| store_reply(error, commit_seq))?;
    let expected_sources = fence_sources.iter().cloned().collect::<BTreeSet<_>>();
    let actual_sources = revocations
        .iter()
        .filter(|row| row.epoch == *fence_epoch && row.seq == fence_event.seq)
        .map(|row| row.source_id.clone())
        .collect::<BTreeSet<_>>();
    if actual_sources != expected_sources {
        return Err(corrupt_reply(
            commit_seq,
            "compacting completion revocations do not match its fence",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn finish_rebuild(
    handle: &mut NamespaceHandle,
    source: SourceId,
    target_epoch: u64,
    idempotency_key: String,
    payload_sha256: String,
    canonical_input: Option<Value>,
    report: ReplayResult,
    deleted_records: u64,
    sources: Vec<SourceId>,
    deleted_record_ids: Vec<RecordId>,
    engine: Option<&NcmEngine>,
    deadline: Deadline,
    started: Instant,
) -> EngineReply {
    let (kernel, meta, reply) = match finish_store_rebuild(
        &mut handle.store,
        source,
        sources,
        target_epoch,
        idempotency_key,
        payload_sha256,
        canonical_input,
        report,
        deleted_records,
        deleted_record_ids,
        engine,
    ) {
        Ok(result) => result,
        Err(reply) => return reply,
    };
    handle.commit_seq = meta.commit_seq;
    handle.epoch = meta.epoch;
    // The durable rebuild is complete, but publication is still an
    // interruptible part of the request.  Leave the resident handle fenced so
    // the next handshake reconciles the committed kernel instead of serving a
    // stale live view after the caller's budget has expired.
    if remaining_ms(deadline, started) == 0 {
        handle.fenced = true;
        return EngineReply::new(
            Outcome::EffectUnknown,
            meta.commit_seq,
            json!({"commit_seq": meta.commit_seq}),
        );
    }
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
    sources: Vec<SourceId>,
    target_epoch: u64,
    idempotency_key: String,
    payload_sha256: String,
    canonical_input: Option<Value>,
    report: ReplayResult,
    deleted_records: u64,
    deleted_record_ids: Vec<RecordId>,
    engine: Option<&NcmEngine>,
) -> Result<(NcmKernel, StoreMeta, EngineReply), EngineReply> {
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
            sources,
            target_epoch,
            deleted_records,
            deleted_record_ids,
            payload_sha256: canonical_input.as_ref().map(|_| payload_sha256.clone()),
            canonical_input,
        },
        &report.kernel,
        Some(&idempotency_key),
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
        // Keep the namespace fenced while the completion checkpoint is
        // physically compacted. A crash after this commit therefore resumes
        // the phase instead of serving a database whose old pages were not
        // yet scrubbed.
        .set_fence("compacting")
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
    if engine.is_some_and(|engine| engine.consume_fault(FaultPoint::AfterDeletionCompletionCommit))
    {
        return Err(EngineReply::new(
            Outcome::EffectUnknown,
            committed,
            json!({"compacting": true, "commit_seq": committed}),
        ));
    }
    store
        .compact(true)
        .map_err(|error| store_reply(error, committed))?;
    let mut clear = store
        .begin_mutation()
        .map_err(|error| store_reply(error, committed))?;
    clear
        .clear_fence()
        .map_err(|error| store_reply(error, committed))?;
    let cleared = clear
        .commit_without_generation()
        .map_err(|error| store_reply(error, committed))?;
    if cleared != committed {
        return Err(corrupt_reply(
            cleared,
            "deletion fence clear sequence mismatch",
        ));
    }
    Ok((report.kernel, meta, reply))
}

struct ReplayResult {
    kernel: NcmKernel,
    replayed_records: u64,
    excluded_records: u64,
    tick_before: u64,
}

/// Loads the original validation view from the newest pre-fence checkpoint.
/// A checkpoint may contain an observation that has since been erased from its
/// capsule, so it is the only durable state anchor that lets the original
/// digest chain resume after that erased input.  The sanitized kernel below
/// is still rebuilt from retained capsules and never publishes this view.
fn validation_kernel_from_checkpoint(
    store: &NamespaceStore,
    config: &NcmConfig,
    pending: &PendingDeletionFence,
    projections: &ProjectionBundle,
    capsules: &[StoredCapsule],
) -> Result<(NcmKernel, u64), EngineReply> {
    let meta = store
        .meta()
        .map_err(|error| store_reply(error, pending.event_seq))?;
    let Some(checkpoint) = store
        .latest_checkpoint()
        .map_err(|error| store_reply(error, pending.event_seq))?
    else {
        let mut kernel = NcmKernel::new(store.identity().seed, config.clone())
            .map_err(|error| core_reply(error, pending.event_seq))?;
        kernel.projections = projections.clone();
        return Ok((kernel, 0));
    };
    if checkpoint.seq >= pending.event_seq || checkpoint.epoch != meta.epoch {
        return Err(corrupt_reply(
            pending.event_seq,
            "sanitized replay checkpoint is outside the pre-fence generation",
        ));
    }
    let envelope: CheckpointEnvelope =
        serde_json::from_slice(&checkpoint.state).map_err(|error| {
            corrupt_reply(
                pending.event_seq,
                &format!("decode sanitized replay checkpoint: {error}"),
            )
        })?;
    if envelope.state_digest != sha256_hex(&envelope.kernel.state_digest())
        || envelope.kernel.config != *config
    {
        return Err(corrupt_reply(
            pending.event_seq,
            "sanitized replay checkpoint digest or config mismatch",
        ));
    }
    let checkpoint_projections =
        serde_json::to_vec(&envelope.kernel.projections).map_err(|error| {
            corrupt_reply(
                pending.event_seq,
                &format!("serialize sanitized replay projections: {error}"),
            )
        })?;
    if checkpoint_projections != store.identity().projection_bytes {
        return Err(corrupt_reply(
            pending.event_seq,
            "sanitized replay checkpoint projection mismatch",
        ));
    }
    validate_checkpoint_anchor(store, checkpoint.seq, &envelope.kernel, &meta, capsules)?;
    Ok((envelope.kernel, checkpoint.seq))
}

/// Validates the event envelope at a checkpoint boundary so state validation
/// can continue from that captured original state even when an earlier source
/// capsule has already been scrubbed.
fn validate_checkpoint_anchor(
    store: &NamespaceStore,
    checkpoint_seq: u64,
    checkpoint_kernel: &NcmKernel,
    meta: &StoreMeta,
    capsules: &[StoredCapsule],
) -> Result<(), EngineReply> {
    let expected_digest = sha256_hex(&checkpoint_kernel.state_digest());
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
        validate_common_maintenance_event(store, &event, &durable)?;
        validate_event_payload_digest(&event, &durable, capsules)?;
        validate_deletion_completion_fence(store, &event, &durable, capsules, meta.commit_seq)?;
        expected_tick = validate_privacy_checkpoint_event_tick(
            &event,
            &durable,
            expected_tick,
            meta.commit_seq,
        )?;
        if event.seq == checkpoint_seq {
            checkpoint_receipt = Some(durable.state_digest);
            checkpoint_event_tick = Some(event.created_tick);
            break;
        }
        expected_seq = expected_seq
            .checked_add(1)
            .ok_or_else(|| corrupt_reply(meta.commit_seq, "checkpoint sequence overflow"))?;
    }
    if expected_seq != checkpoint_seq
        || checkpoint_receipt.as_deref() != Some(expected_digest.as_str())
        || checkpoint_event_tick != Some(checkpoint_kernel.scheduler.tick.0)
    {
        return Err(corrupt_reply(
            meta.commit_seq,
            "sanitized replay checkpoint is detached from its journal",
        ));
    }
    Ok(())
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
    if crate::engine::snapshot_gap_contract(event, durable, event.seq)?.is_some() {
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
    match crate::engine::portable_common_maintenance_event(store.namespace(), event) {
        Ok(true) => Ok(()),
        Ok(false) => Err(corrupt_reply(
            event.seq,
            "common maintenance receipt is not portable",
        )),
        Err(reason) => Err(corrupt_reply(event.seq, &reason)),
    }
}

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
        .and_then(Value::as_object)
    else {
        if legacy_shape {
            // Legacy DeleteBySource receipts had no explicit reset anchor;
            // retain the ordinary zero-delta tick check for those rows.
            return Ok(None);
        }
        return Err(corrupt_reply(
            commit_seq,
            "sanitized deletion receipt is missing its tick anchor",
        ));
    };
    let tick_before = report
        .get("tick_before")
        .and_then(Value::as_u64)
        .ok_or_else(|| corrupt_reply(commit_seq, "sanitized deletion tick anchor is invalid"))?;
    let tick_after = report
        .get("tick_after")
        .and_then(Value::as_u64)
        .ok_or_else(|| corrupt_reply(commit_seq, "sanitized deletion tick anchor is invalid"))?;
    if tick_after != event.created_tick || tick_after > tick_before {
        return Err(corrupt_reply(
            commit_seq,
            "sanitized deletion tick anchor does not match its event",
        ));
    }
    Ok(Some((tick_before, tick_after)))
}

fn validate_privacy_checkpoint_event_tick(
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
            "sanitized replay checkpoint tick does not match its operation history",
        ));
    }
    Ok(expected_tick)
}

fn sanitized_replay(
    store: &NamespaceStore,
    config: &NcmConfig,
    pending: &PendingDeletionFence,
    expected_fence_state_digest: Option<&str>,
) -> Result<ReplayResult, EngineReply> {
    if let Some(expected) = expected_fence_state_digest
        && pending.state_digest != expected
    {
        return Err(corrupt_reply(
            pending.event_seq,
            "deletion fence state digest does not match the pre-fence kernel",
        ));
    }
    let capsules = store
        .capsules_in_commit_order(true)
        .map_err(|error| store_reply(error, pending.event_seq))?;
    let events = store
        .events_after(0)
        .map_err(|error| store_reply(error, pending.event_seq))?;
    let projections: ProjectionBundle = serde_json::from_slice(&store.identity().projection_bytes)
        .map_err(|error| {
            corrupt_reply(
                pending.event_seq,
                &format!("decode persisted projections: {error}"),
            )
        })?;
    let (mut validation_kernel, validation_checkpoint_seq) =
        validation_kernel_from_checkpoint(store, config, pending, &projections, &capsules)?;
    let mut kernel = NcmKernel::new(store.identity().seed, config.clone())
        .map_err(|error| core_reply(error, pending.event_seq))?;
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
    .map_err(|_| corrupt_reply(pending.event_seq, "excluded record count overflow"))?;
    let mut expected_seq = 1_u64;
    let mut previous_tick = None;
    let mut original_tick = 0_u64;
    let mut fence_tick = None;
    let mut sanitized_tick = 0_u64;
    // The original kernel digest chain is independently checkable until the
    // first erased observation.  Once that input is gone, continue validating
    // every retained operation against its capsule and replay the sanitized
    // kernel; never disable validation for the rest of the journal.
    let mut original_state_chain_valid = true;
    for event in events {
        let durable = validate_recovery_event(&event, expected_seq, pending.event_seq)?;
        validate_common_maintenance_event(store, &event, &durable)?;
        validate_event_payload_digest(&event, &durable, &capsules)?;
        validate_deletion_completion_fence(store, &event, &durable, &capsules, pending.event_seq)?;
        let snapshot_gap =
            crate::engine::snapshot_gap_contract(&event, &durable, pending.event_seq)?;
        let reset = deletion_tick_anchor(&event, &durable, pending.event_seq)?;
        original_tick = validate_privacy_checkpoint_event_tick(
            &event,
            &durable,
            original_tick,
            pending.event_seq,
        )?;
        if reset.is_none() && previous_tick.is_some_and(|tick| event.created_tick < tick) {
            return Err(corrupt_reply(
                pending.event_seq,
                "event scheduler ticks are not monotonic",
            ));
        }
        previous_tick = Some(event.created_tick);
        if event.seq == pending.event_seq {
            fence_tick = Some(event.created_tick);
        }
        expected_seq = expected_seq
            .checked_add(1)
            .ok_or_else(|| corrupt_reply(pending.event_seq, "event sequence overflow"))?;
        if original_state_chain_valid && snapshot_gap.is_some() {
            // The omitted operation's nonlinear state transition is not
            // present in the portable event stream. The marker's source
            // digest and eventual restore checkpoint authenticate the range;
            // do not compare this synthetic row to an invented kernel state.
            original_state_chain_valid = false;
        } else if original_state_chain_valid
            && event.seq > validation_checkpoint_seq
            && operation_contains_revoked_observe(&durable.operation, &by_record)
        {
            original_state_chain_valid = false;
        } else if original_state_chain_valid && event.seq > validation_checkpoint_seq {
            replay_recovery_event(
                &mut validation_kernel,
                &event,
                &durable.operation,
                &capsules,
                pending.event_seq,
            )?;
            if sha256_hex(&validation_kernel.state_digest()) != durable.state_digest {
                return Err(corrupt_reply(
                    pending.event_seq,
                    "replayed state digest mismatch",
                ));
            }
        }
        let operations = match durable.operation {
            DurableOperation::CommonControl { operations, .. } => operations,
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
                    if capsule.commit_seq != event.seq {
                        return Err(corrupt_reply(
                            event.seq,
                            "observe receipt capsule sequence mismatch",
                        ));
                    }
                    if capsule.status == CapsuleStatus::Revoked {
                        continue;
                    }
                    sanitized_tick = sanitized_tick.saturating_add(1);
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
                    let record = kernel.records.get(observed.record_id).ok_or_else(|| {
                        corrupt_reply(event.seq, "sanitized observe record is missing")
                    })?;
                    if record.ltm_key != capsule.ltm_key {
                        return Err(corrupt_reply(
                            event.seq,
                            "sanitized observe LTM key mismatch",
                        ));
                    }
                    old_to_new.insert(record_id, observed.record_id);
                    replayed_records = replayed_records.checked_add(1).ok_or_else(|| {
                        corrupt_reply(event.seq, "replayed record count overflow")
                    })?;
                }
                DurableOperation::Feedback { record_ids } => {
                    let mut seen = BTreeSet::new();
                    let mut retained = Vec::with_capacity(record_ids.len());
                    for record_id in record_ids {
                        if !seen.insert(record_id) {
                            return Err(corrupt_reply(
                                event.seq,
                                "feedback receipt contains duplicate record IDs",
                            ));
                        }
                        let capsule = by_record.get(&record_id).ok_or_else(|| {
                            corrupt_reply(event.seq, "feedback receipt capsule is missing")
                        })?;
                        if capsule.status == CapsuleStatus::Revoked {
                            continue;
                        }
                        retained.push(old_to_new.get(&record_id).copied().ok_or_else(|| {
                            corrupt_reply(event.seq, "feedback record was not replayed")
                        })?);
                    }
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
                    let superseded_capsule = by_record.get(&superseded).ok_or_else(|| {
                        corrupt_reply(event.seq, "correction superseded capsule is missing")
                    })?;
                    let superseding_capsule = by_record.get(&superseding).ok_or_else(|| {
                        corrupt_reply(event.seq, "correction superseding capsule is missing")
                    })?;
                    if superseded_capsule.status == CapsuleStatus::Revoked
                        || superseding_capsule.status == CapsuleStatus::Revoked
                    {
                        continue;
                    }
                    let old = old_to_new.get(&superseded).copied().ok_or_else(|| {
                        corrupt_reply(event.seq, "correction superseded record was not replayed")
                    })?;
                    let new = old_to_new.get(&superseding).copied().ok_or_else(|| {
                        corrupt_reply(event.seq, "correction superseding record was not replayed")
                    })?;
                    kernel
                        .correction(old, new, evidence)
                        .map_err(|error| core_reply(error, event.seq))?;
                }
                DurableOperation::Maintenance { kind, .. } => {
                    if let MaintenanceKind::Advance { ticks } = &kind {
                        sanitized_tick = sanitized_tick.saturating_add(u64::from(*ticks));
                    }
                    crate::engine::apply_recovery_maintenance(&mut kernel, &kind)
                        .map_err(|error| core_reply(error, event.seq))?;
                }
                DurableOperation::DeletionFence { .. }
                | DurableOperation::DeleteBySource { .. } => {}
            }
        }
    }
    if expected_seq != pending.event_seq.saturating_add(1) {
        return Err(corrupt_reply(
            pending.event_seq,
            "metadata sequence is not journal-backed",
        ));
    }
    if fence_tick != Some(pending.tick_before) {
        return Err(corrupt_reply(
            pending.event_seq,
            "deletion fence tick does not match its operation history",
        ));
    }
    if kernel.scheduler.tick.0 != sanitized_tick {
        return Err(corrupt_reply(
            pending.event_seq,
            "sanitized replay tick does not match its retained history",
        ));
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
        tick_before: pending.tick_before,
    })
}

fn operation_contains_revoked_observe(
    operation: &DurableOperation,
    by_record: &BTreeMap<RecordId, &StoredCapsule>,
) -> bool {
    match operation {
        DurableOperation::CommonControl { operations, .. } => operations
            .iter()
            .any(|operation| operation_contains_revoked_observe(operation, by_record)),
        DurableOperation::Observe { record_id } => by_record
            .get(record_id)
            .is_some_and(|capsule| capsule.status == CapsuleStatus::Revoked),
        DurableOperation::Feedback { .. }
        | DurableOperation::Correction { .. }
        | DurableOperation::Maintenance { .. }
        | DurableOperation::DeletionFence { .. }
        | DurableOperation::DeleteBySource { .. } => false,
    }
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

fn lookup_legacy_deletion_replay(
    handle: &mut NamespaceHandle,
    key: &str,
    expected_sources: &BTreeSet<SourceId>,
    canonical_input: &Value,
) -> Result<Option<EngineReply>, EngineReply> {
    if !legacy_deletion_request_shape(canonical_input, expected_sources) {
        return Ok(None);
    }
    let Some(event) = handle
        .store
        .event_for_key(key)
        .map_err(|error| store_reply(error, handle.commit_seq))?
    else {
        return Ok(None);
    };
    if event.kind != "delete_by_source" || event.idempotency_key.as_deref() != Some(key) {
        return Ok(None);
    }
    let durable: DurableReceipt = serde_json::from_str(&event.receipt).map_err(|error| {
        corrupt_reply(
            handle.commit_seq,
            &format!("decode legacy deletion receipt: {error}"),
        )
    })?;
    let DurableOperation::DeleteBySource {
        sources,
        payload_sha256: None,
        canonical_input: None,
        ..
    } = &durable.operation
    else {
        return Ok(None);
    };
    let expected_sources = expected_sources.iter().cloned().collect::<Vec<_>>();
    if sources != &expected_sources {
        return Ok(None);
    }
    let legacy_digest = legacy_deletion_digest(sources, &event.payload_sha256, handle.commit_seq)?;
    let capsules = handle
        .store
        .capsules_in_commit_order(true)
        .map_err(|error| store_reply(error, handle.commit_seq))?;
    let durable = validate_recovery_event(&event, event.seq, handle.commit_seq)?;
    validate_event_payload_digest(&event, &durable, &capsules)?;
    let expected_source_set = expected_sources.iter().cloned().collect::<BTreeSet<_>>();
    validate_completed_deletion_replay(handle, key, &legacy_digest, &expected_source_set)?;
    let mut replay = durable.reply;
    if let Some(object) = replay.payload.as_object_mut() {
        object.insert("replayed".to_owned(), Value::Bool(true));
    }
    Ok(Some(replay))
}

fn legacy_deletion_request_shape(
    canonical_input: &Value,
    expected_sources: &BTreeSet<SourceId>,
) -> bool {
    let Some(object) = canonical_input.as_object() else {
        return false;
    };
    let expected_sources = Value::Array(
        expected_sources
            .iter()
            .cloned()
            .map(|source| json!(source))
            .collect(),
    );
    object.len() == 3
        && object.get("action").and_then(Value::as_str) == Some("delete_by_source")
        && object.get("sources") == Some(&expected_sources)
        && object
            .get("expected_generation")
            .is_some_and(|generation| generation.is_null() || generation.as_u64().is_some())
}

fn legacy_deletion_digest(
    sources: &[SourceId],
    payload_sha256: &str,
    seq: u64,
) -> Result<String, EngineReply> {
    let array_digest =
        crate::engine::canonical_digest(sources).map_err(|reason| corrupt_reply(seq, &reason))?;
    if payload_sha256 == array_digest {
        return Ok(array_digest);
    }
    if sources.len() == 1 {
        let single_digest = crate::engine::canonical_digest(&sources[0])
            .map_err(|reason| corrupt_reply(seq, &reason))?;
        if payload_sha256 == single_digest {
            return Ok(single_digest);
        }
    }
    Err(corrupt_reply(
        seq,
        "legacy deletion payload digest does not match its source set",
    ))
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
    let event = handle
        .store
        .event(seq)
        .map_err(|error| store_reply(error, handle.commit_seq))?
        .ok_or_else(|| corrupt_reply(handle.commit_seq, "idempotency event is missing"))?;
    if event.idempotency_key.as_deref() != Some(key)
        || event.payload_sha256 != stored_digest
        || event.receipt != receipt_json
    {
        return Err(corrupt_reply(
            handle.commit_seq,
            "idempotency envelope does not match its journal row",
        ));
    }
    let capsules = handle
        .store
        .capsules_in_commit_order(true)
        .map_err(|error| store_reply(error, handle.commit_seq))?;
    let durable = validate_recovery_event(&event, seq, handle.commit_seq)?;
    validate_event_payload_digest(&event, &durable, &capsules)?;
    let mut reply = durable.reply;
    if let Some(object) = reply.payload.as_object_mut() {
        object.insert("replayed".to_owned(), Value::Bool(true));
    }
    Ok(Some(reply))
}

/// Validates the whole two-event deletion record before replaying a completed
/// request.  Generic idempotency validation proves that a key and payload
/// digest are present, but it does not prove that the row is the exact
/// deletion operation that minted the key.  The fence immediately preceding
/// the completion event is the durable source of truth for that binding.
fn validate_completed_deletion_replay(
    handle: &NamespaceHandle,
    key: &str,
    payload_sha256: &str,
    expected_sources: &BTreeSet<SourceId>,
) -> Result<(), EngineReply> {
    let meta = handle
        .store
        .meta()
        .map_err(|error| store_reply(error, handle.commit_seq))?;
    let event = handle
        .store
        .event_for_key(key)
        .map_err(|error| store_reply(error, handle.commit_seq))?
        .ok_or_else(|| corrupt_reply(handle.commit_seq, "completed deletion event is missing"))?;
    if event.idempotency_key.as_deref() != Some(key)
        || event.payload_sha256 != payload_sha256
        || event.seq < 2
    {
        return Err(corrupt_reply(
            handle.commit_seq,
            "completed deletion idempotency envelope is invalid",
        ));
    }
    let capsules = handle
        .store
        .capsules_in_commit_order(true)
        .map_err(|error| store_reply(error, handle.commit_seq))?;
    let durable: DurableReceipt = serde_json::from_str(&event.receipt)
        .map_err(|error| corrupt_reply(handle.commit_seq, &format!("decode receipt: {error}")))?;
    validate_recovery_event(&event, event.seq, meta.commit_seq)?;
    let legacy_without_receipt_key = matches!(
        &durable.operation,
        DurableOperation::DeleteBySource {
            payload_sha256: None,
            canonical_input: None,
            ..
        }
    ) && durable.idempotency_key.is_none();
    if durable.idempotency_key.as_deref() != Some(key) && !legacy_without_receipt_key {
        return Err(corrupt_reply(
            handle.commit_seq,
            "completed deletion receipt idempotency key mismatch",
        ));
    }
    validate_event_payload_digest(&event, &durable, &capsules)?;
    validate_deletion_completion_fence(
        &handle.store,
        &event,
        &durable,
        &capsules,
        meta.commit_seq,
    )?;
    let DurableOperation::DeleteBySource {
        source,
        sources,
        target_epoch,
        deleted_records,
        deleted_record_ids,
        payload_sha256: operation_payload_sha256,
        canonical_input: _,
        ..
    } = &durable.operation
    else {
        return Err(corrupt_reply(
            handle.commit_seq,
            "idempotency key is not bound to a completed deletion",
        ));
    };
    let tick_anchor = deletion_tick_anchor(&event, &durable, meta.commit_seq)?;
    let canonical_sources = expected_sources.iter().cloned().collect::<Vec<_>>();
    if sources != &canonical_sources
        || sources.first() != Some(source)
        || !valid_deletion_source_set(sources)
        || operation_payload_sha256
            .as_deref()
            .unwrap_or(payload_sha256)
            != payload_sha256
        || durable.reply.outcome != Outcome::Success
        || durable.reply.state_generation != event.seq
        || durable.reply.payload["source"] != serde_json::json!(source)
        || durable.reply.payload["deleted_records"] != serde_json::json!(deleted_records)
        || durable.reply.payload["epoch"] != serde_json::json!(target_epoch)
        || durable.reply.payload["replayed"] != false
    {
        return Err(corrupt_reply(
            handle.commit_seq,
            "completed deletion receipt does not match its request",
        ));
    }
    validate_deleted_record_set(
        deleted_records,
        deleted_record_ids,
        &capsules,
        event.seq - 1,
        handle.commit_seq,
    )?;

    let fence_event = handle
        .store
        .event(event.seq - 1)
        .map_err(|error| store_reply(error, handle.commit_seq))?
        .ok_or_else(|| corrupt_reply(handle.commit_seq, "completed deletion fence is missing"))?;
    let fence: DurableReceipt = serde_json::from_str(&fence_event.receipt).map_err(|error| {
        corrupt_reply(
            handle.commit_seq,
            &format!("decode completed deletion fence receipt: {error}"),
        )
    })?;
    validate_recovery_event(&fence_event, fence_event.seq, meta.commit_seq)?;
    if fence.idempotency_key.is_some() {
        return Err(corrupt_reply(
            handle.commit_seq,
            "deletion fence receipt unexpectedly has an idempotency key",
        ));
    }
    validate_event_payload_digest(&fence_event, &fence, &capsules)?;
    let DurableOperation::DeletionFence {
        source: fence_source,
        sources: fence_sources,
        target_epoch: fence_epoch,
        idempotency_key: fence_key,
        payload_sha256: fence_payload,
        deleted_records: fence_deleted_records,
        deleted_record_ids: fence_deleted_record_ids,
        pre_fence_state_digest,
        fatigue,
        steps_since_consolidation: _,
        canonical_input: _,
    } = &fence.operation
    else {
        return Err(corrupt_reply(
            handle.commit_seq,
            "completed deletion is not preceded by a deletion fence",
        ));
    };
    if let Some((tick_before, tick_after)) = tick_anchor
        && (tick_before != fence_event.created_tick || tick_after != event.created_tick)
    {
        return Err(corrupt_reply(
            handle.commit_seq,
            "completed deletion tick anchor is not bound to its fence",
        ));
    }
    if fence_event.kind != "deletion_fence"
        || fence_event.idempotency_key.is_some()
        || fence_event.seq.checked_add(1) != Some(event.seq)
        || fence_source != source
        || fence_sources != sources
        || fence_epoch != target_epoch
        || fence_key != key
        || fence_payload != payload_sha256
        || fence_deleted_records != deleted_records
        || fence_deleted_record_ids != deleted_record_ids
        || !is_sha256_hex(pre_fence_state_digest)
        || !fatigue.is_finite()
        || fence.reply.payload["fenced"] != true
        || fence.reply.payload["target_epoch"] != serde_json::json!(target_epoch)
        || fence.reply.state_generation != fence_event.seq
        || fence.state_digest != *pre_fence_state_digest
    {
        return Err(corrupt_reply(
            handle.commit_seq,
            "completed deletion is not bound to its original fence",
        ));
    }
    validate_historical_revocation_authority(
        handle,
        fence_event.seq,
        *target_epoch,
        fence_sources,
        &capsules,
    )?;
    if *target_epoch > meta.epoch {
        return Err(corrupt_reply(
            handle.commit_seq,
            "completed deletion epoch is newer than metadata",
        ));
    }
    Ok(())
}

/// Validates the mutable revocation projection against its immutable fence
/// history. A later deletion legitimately replaces a source's projection row,
/// so a completed older receipt must remain replayable after K1/K2. Every row
/// still has to point at a real, integrity-checked fence event; accepting an
/// arbitrary newer sequence would turn the projection into an authority of
/// its own.
fn validate_historical_revocation_authority(
    handle: &NamespaceHandle,
    original_fence_seq: u64,
    original_epoch: u64,
    original_sources: &[SourceId],
    capsules: &[StoredCapsule],
) -> Result<(), EngineReply> {
    let meta = handle
        .store
        .meta()
        .map_err(|error| store_reply(error, handle.commit_seq))?;
    let rows = handle
        .store
        .revocations()
        .map_err(|error| store_reply(error, handle.commit_seq))?;
    let original_sources = original_sources.iter().collect::<BTreeSet<_>>();
    for row in rows {
        if row.epoch == 0 || row.seq == 0 || row.epoch > meta.epoch || row.seq > meta.commit_seq {
            return Err(corrupt_reply(
                handle.commit_seq,
                "completed deletion revocation authority is invalid",
            ));
        }
        let event = handle
            .store
            .event(row.seq)
            .map_err(|error| store_reply(error, handle.commit_seq))?
            .ok_or_else(|| {
                corrupt_reply(
                    handle.commit_seq,
                    "completed deletion revocation authority event is missing",
                )
            })?;
        let durable: DurableReceipt = serde_json::from_str(&event.receipt).map_err(|error| {
            corrupt_reply(
                handle.commit_seq,
                &format!("decode revocation authority receipt: {error}"),
            )
        })?;
        validate_recovery_event(&event, row.seq, handle.commit_seq)?;
        validate_event_payload_digest(&event, &durable, capsules)?;
        let DurableOperation::DeletionFence {
            sources,
            target_epoch,
            ..
        } = &durable.operation
        else {
            return Err(corrupt_reply(
                handle.commit_seq,
                "revocation authority does not point to a deletion fence",
            ));
        };
        if event.kind != "deletion_fence"
            || event.seq != row.seq
            || *target_epoch != row.epoch
            || !sources.iter().any(|source| source == &row.source_id)
        {
            return Err(corrupt_reply(
                handle.commit_seq,
                "revocation authority does not match its fence event",
            ));
        }
        let completion_seq = row.seq.checked_add(1).ok_or_else(|| {
            corrupt_reply(
                handle.commit_seq,
                "completed deletion revocation completion sequence overflow",
            )
        })?;
        let completion = handle
            .store
            .event(completion_seq)
            .map_err(|error| store_reply(error, handle.commit_seq))?
            .ok_or_else(|| {
                corrupt_reply(
                    handle.commit_seq,
                    "revocation authority fence has no completion event",
                )
            })?;
        let completion_receipt: DurableReceipt = serde_json::from_str(&completion.receipt)
            .map_err(|error| {
                corrupt_reply(
                    handle.commit_seq,
                    &format!("decode revocation completion receipt: {error}"),
                )
            })?;
        validate_recovery_event(&completion, completion_seq, meta.commit_seq)?;
        validate_event_payload_digest(&completion, &completion_receipt, capsules)?;
        validate_deletion_completion_fence(
            &handle.store,
            &completion,
            &completion_receipt,
            capsules,
            meta.commit_seq,
        )?;
        let DurableOperation::DeleteBySource {
            source: completion_source,
            sources: completion_sources,
            target_epoch: completion_epoch,
            ..
        } = &completion_receipt.operation
        else {
            return Err(corrupt_reply(
                handle.commit_seq,
                "revocation authority completion is not a deletion",
            ));
        };
        if completion.kind != "delete_by_source"
            || completion.seq != completion_seq
            || completion_source != &sources[0]
            || completion_sources != sources
            || completion_epoch != target_epoch
        {
            return Err(corrupt_reply(
                handle.commit_seq,
                "revocation authority completion does not match its fence",
            ));
        }
        if original_sources.contains(&row.source_id) {
            if row.epoch < original_epoch
                || (row.epoch == original_epoch && row.seq != original_fence_seq)
                || (row.epoch > original_epoch && row.seq <= original_fence_seq)
            {
                return Err(corrupt_reply(
                    handle.commit_seq,
                    "completed deletion revocation authority regressed",
                ));
            }
        }
    }
    for source in original_sources {
        let found = handle
            .store
            .revocations()
            .map_err(|error| store_reply(error, handle.commit_seq))?
            .into_iter()
            .any(|row| {
                row.source_id == *source
                    && row.epoch >= original_epoch
                    && (row.epoch > original_epoch || row.seq == original_fence_seq)
            });
        if !found {
            return Err(corrupt_reply(
                handle.commit_seq,
                "completed deletion revocation authority is missing",
            ));
        }
    }
    Ok(())
}

fn validate_deleted_record_set(
    deleted_records: &u64,
    deleted_record_ids: &[RecordId],
    capsules: &[StoredCapsule],
    fence_seq: u64,
    commit_seq: u64,
) -> Result<(), EngineReply> {
    if u64::try_from(deleted_record_ids.len()).ok() != Some(*deleted_records)
        || deleted_record_ids
            .windows(2)
            .any(|window| window[0] >= window[1])
    {
        return Err(corrupt_reply(
            commit_seq,
            "completed deletion record set is invalid",
        ));
    }
    for record_id in deleted_record_ids {
        let Some(capsule) = capsules
            .iter()
            .find(|capsule| capsule.record_id == *record_id)
        else {
            return Err(corrupt_reply(
                commit_seq,
                "completed deletion references an unknown record",
            ));
        };
        if capsule.status != CapsuleStatus::Revoked || capsule.commit_seq >= fence_seq {
            return Err(corrupt_reply(
                commit_seq,
                "completed deletion record set is not revoked by its fence",
            ));
        }
    }
    Ok(())
}

fn valid_deletion_source_set(sources: &[SourceId]) -> bool {
    !sources.is_empty()
        && sources.len() <= 1024
        && sources.iter().all(|source| !source.0.is_empty())
        && sources.windows(2).all(|window| window[0] < window[1])
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn durable_receipt(
    reply: &EngineReply,
    operation: DurableOperation,
    kernel: &NcmKernel,
    idempotency_key: Option<&str>,
) -> Result<String, EngineReply> {
    let state_digest = sha256_hex(&kernel.state_digest());
    let integrity_digest = durable_integrity_digest(&reply, &operation, &state_digest)
        .map_err(|reason: String| corrupt_reply(reply.state_generation, &reason))?;
    serde_json::to_string(&DurableReceipt {
        reply: reply.clone(),
        operation,
        state_digest,
        integrity_digest,
        idempotency_key: idempotency_key.map(str::to_owned),
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

/// Builds the canonical semantic input used by both the event digest and the
/// public idempotency lookup. Identity and generation fields are handled
/// separately: the generation is retained as a required precondition in the
/// envelope, while the semantic digest omits it so a replay can arrive after
/// the namespace advances.
pub(crate) fn deletion_request_input(
    sources: &BTreeSet<SourceId>,
    expected_generation: Option<u64>,
    payload: Option<&Value>,
) -> Value {
    let mut input = payload
        .cloned()
        .unwrap_or_else(|| json!({"action": "delete_by_source"}));
    if let Some(object) = input.as_object_mut() {
        object.remove("idempotency_key");
        object.insert(
            "sources".to_owned(),
            Value::Array(
                sources
                    .iter()
                    .cloned()
                    .map(|source| json!(source))
                    .collect(),
            ),
        );
        object.insert(
            "expected_generation".to_owned(),
            expected_generation.map_or(Value::Null, Value::from),
        );
    }
    input
}

fn canonical_deletion_digest(input: &Value) -> Result<String, String> {
    let mut semantics = input.clone();
    if let Some(object) = semantics.as_object_mut() {
        object.remove("expected_generation");
    }
    crate::engine::canonical_digest(&semantics)
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
