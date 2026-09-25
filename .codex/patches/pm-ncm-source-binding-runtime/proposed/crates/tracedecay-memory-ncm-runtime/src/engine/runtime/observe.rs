use super::super::*;
use super::util::{
    core_reply, durable_receipt, layer_write_name, lookup_replay, meta_for_kernel, publish,
    put_checkpoint, read_live, remaining_deadline, resolve_affect, store_reply,
    unavailable_recovery, validate_idempotency_key,
};
use crate::store::Capsule;
use serde_json::{Value, json};
use std::time::Instant;
use tracedecay_memory_ncm_core::kernel::NewRecord;

impl NcmEngine {
    /// Encodes, learns, commits, and publishes one exactly-once observation.
    pub fn observe(&self, namespace: &str, request: ObserveRequest) -> EngineReply {
        let started = Instant::now();
        if request.deadline.remaining_ms == 0 {
            return EngineReply::new(Outcome::Cancelled, 0, Value::Null);
        }
        if let Err(reason) = validate_idempotency_key(&request.idempotency_key) {
            return EngineReply::rejected(RejectReason::InvalidRequest(reason), 0);
        }
        let canonical = match request.canonical_payload_sha256() {
            Ok(digest) => digest,
            Err(reason) => return EngineReply::rejected(RejectReason::InvalidRequest(reason), 0),
        };
        if request.payload_sha256 != canonical {
            return EngineReply::rejected(
                RejectReason::InvalidRequest(
                    "payload_sha256 does not match canonical payload".to_owned(),
                ),
                0,
            );
        }
        let affect = match resolve_affect(request.affect.as_ref()) {
            Ok(affect) => affect,
            Err(error) => return core_reply(error, 0),
        };
        if !request.surprise.is_finite() || !request.intensity.is_finite() {
            return EngineReply::rejected(
                RejectReason::InvalidRequest("surprise and intensity must be finite".to_owned()),
                0,
            );
        }
        let provenance = match serde_json::to_string(&request.provenance) {
            Ok(provenance) => provenance,
            Err(error) => {
                return EngineReply::rejected(
                    RejectReason::InvalidRequest(format!("invalid provenance: {error}")),
                    0,
                );
            }
        };

        {
            let mut namespaces = match self.namespace_lock() {
                Ok(namespaces) => namespaces,
                Err(reply) => return reply,
            };
            let handle = match self.ensure_handle(&mut namespaces, namespace, true) {
                Ok(Some(handle)) => handle,
                Ok(None) => return EngineReply::new(Outcome::Corrupt, 0, Value::Null),
                Err(reply) => return reply,
            };
            if handle.fenced {
                return unavailable_recovery(handle.commit_seq);
            }
            match lookup_observe_replay(handle, &request) {
                Ok(Some(reply)) => return reply,
                Ok(None) => {}
                Err(reply) => return reply,
            }
            if let Err(error) = handle
                .store
                .ensure_source_provenance_not_revoked(&request.source, &request.provenance)
            {
                return store_reply(error, handle.commit_seq);
            }
            if let Err(reply) = reject_reused_source(handle, &request, started) {
                return reply;
            }
        }

        let deadline = remaining_deadline(request.deadline, started);
        if deadline.remaining_ms == 0 {
            return EngineReply::new(Outcome::Cancelled, 0, Value::Null);
        }
        let embeddings = match self.encoder.encode(
            &[request.key_text.as_str(), request.value_text.as_str()],
            deadline,
        ) {
            Ok(embeddings) if embeddings.len() == 2 => embeddings,
            Ok(_) => {
                return EngineReply::new(
                    Outcome::Corrupt,
                    0,
                    json!({"reason": "encoder returned an unexpected batch length"}),
                );
            }
            Err(error) => return super::util::encoder_reply(error, 0),
        };
        if remaining_deadline(request.deadline, started).remaining_ms == 0 {
            return EngineReply::new(Outcome::Cancelled, 0, Value::Null);
        }

        let mut namespaces = match self.namespace_lock() {
            Ok(namespaces) => namespaces,
            Err(reply) => return reply,
        };
        let handle = match self.ensure_handle(&mut namespaces, namespace, true) {
            Ok(Some(handle)) => handle,
            Ok(None) => return EngineReply::new(Outcome::Corrupt, 0, Value::Null),
            Err(reply) => return reply,
        };
        if handle.fenced {
            return unavailable_recovery(handle.commit_seq);
        }
        match lookup_observe_replay(handle, &request) {
            Ok(Some(reply)) => return reply,
            Ok(None) => {}
            Err(reply) => return reply,
        }
        if let Err(error) = handle
            .store
            .ensure_source_provenance_not_revoked(&request.source, &request.provenance)
        {
            return store_reply(error, handle.commit_seq);
        }
        if let Err(reply) = reject_reused_source(handle, &request, started) {
            return reply;
        }
        let live = match read_live(handle) {
            Ok(live) => live,
            Err(reply) => return reply,
        };
        let mut candidate = (*live).clone();
        let report = match candidate.observe(
            &embeddings[0].0,
            &embeddings[1].0,
            NewRecord {
                source: request.source.clone(),
                key_text: request.key_text.clone(),
                value_text: request.value_text.clone(),
                affect,
                surprise: request.surprise,
                intensity: request.intensity,
            },
        ) {
            Ok(report) => report,
            Err(error) => return core_reply(error, handle.commit_seq),
        };
        let projected_ltm = match candidate.records.get(report.record_id) {
            Some(record) => record.ltm_key.clone(),
            None => return EngineReply::new(Outcome::Corrupt, handle.commit_seq, Value::Null),
        };
        let pending_seq = match handle.commit_seq.checked_add(1) {
            Some(seq) => seq,
            None => return EngineReply::new(Outcome::Corrupt, handle.commit_seq, Value::Null),
        };
        let mut reply = EngineReply::new(
            Outcome::Success,
            pending_seq,
            json!({
                "record_id": report.record_id.0,
                "created_or_reinforced": layer_write_name(report.created_or_reinforced.stm),
                "tick": report.tick.0,
                "consolidated": report.consolidated,
                "replayed": false
            }),
        );
        let receipt = match durable_receipt(
            &reply,
            DurableOperation::Observe {
                record_id: report.record_id,
            },
            &candidate,
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
        let stored_record = match mutation.insert_capsule(Capsule::new(
            request.source,
            request.key_text,
            request.value_text,
            affect,
            request.surprise,
            request.intensity,
            embeddings[0].0.clone(),
            embeddings[1].0.clone(),
            projected_ltm,
            provenance,
        )) {
            Ok(record_id) => record_id,
            Err(error) => return store_reply(error, handle.commit_seq),
        };
        if stored_record != report.record_id {
            return EngineReply::new(
                Outcome::Corrupt,
                handle.commit_seq,
                json!({"reason": "kernel and store record identifiers diverged"}),
            );
        }
        if let Err(error) = mutation.append_event(
            "observe",
            Some(&request.idempotency_key),
            &request.payload_sha256,
            &receipt,
            report.tick.0,
        ) {
            return store_reply(error, handle.commit_seq);
        }
        if let Err(error) = mutation.set_meta(&meta_for_kernel(
            &candidate,
            handle.epoch,
            pending_seq,
            prior_meta.last_maintenance,
        )) {
            return store_reply(error, handle.commit_seq);
        }
        if pending_seq % CHECKPOINT_INTERVAL == 0 {
            if self.consume_fault(FaultPoint::DuringCheckpoint) {
                return EngineReply::new(
                    Outcome::Unavailable("injected checkpoint failure".to_owned()),
                    handle.commit_seq,
                    Value::Null,
                );
            }
            if let Err(reply) = put_checkpoint(&mut mutation, pending_seq, handle.epoch, &candidate)
            {
                return reply;
            }
        }
        if self.consume_fault(FaultPoint::BeforeCommit) {
            return EngineReply::new(
                Outcome::Unavailable("injected failure before commit".to_owned()),
                handle.commit_seq,
                Value::Null,
            );
        }
        let committed = match mutation.commit() {
            Ok(seq) => seq,
            Err(error) => return store_reply(error, handle.commit_seq),
        };
        handle.commit_seq = committed;
        if committed != pending_seq {
            handle.fenced = true;
            return EngineReply::new(Outcome::Corrupt, committed, Value::Null);
        }
        if self.consume_fault(FaultPoint::AfterCommitBeforePublish) {
            handle.fenced = true;
            return EngineReply::new(
                Outcome::EffectUnknown,
                committed,
                json!({"commit_seq": committed}),
            );
        }
        if let Err(reply) = publish(handle, candidate) {
            handle.fenced = true;
            return EngineReply::new(Outcome::EffectUnknown, committed, reply.payload);
        }
        if self.consume_fault(FaultPoint::AfterPublishBeforeAck) {
            return EngineReply::new(
                Outcome::EffectUnknown,
                committed,
                json!({"commit_seq": committed}),
            );
        }
        if remaining_deadline(request.deadline, started).remaining_ms == 0 {
            return EngineReply::new(
                Outcome::EffectUnknown,
                committed,
                json!({"commit_seq": committed}),
            );
        }
        if let Err(reply) = super::util::attach_observation_delivery(handle, &mut reply) {
            return reply;
        }
        reply
    }
}

fn reject_reused_source(
    handle: &NamespaceHandle,
    request: &ObserveRequest,
    started: Instant,
) -> Result<(), EngineReply> {
    let Some(identity) = request
        .provenance
        .pointer("/selection/observation_identity")
        .and_then(Value::as_str)
    else {
        return Ok(());
    };
    let binding = crate::source_binding::read(
        handle.store.namespace(),
        &request.source,
        &request.provenance,
    )
    .map_err(|reason| {
        EngineReply::rejected(RejectReason::InvalidRequest(reason), handle.commit_seq)
    })?;
    let filter = binding
        .as_ref()
        .map_or(&request.source, |binding| &binding.legacy_source_id);
    let mut after = 0;
    let mut scanned = 0_u64;
    loop {
        if remaining_deadline(request.deadline, started).remaining_ms == 0 {
            return Err(EngineReply::new(
                Outcome::Cancelled,
                handle.commit_seq,
                Value::Null,
            ));
        }
        let limit = 128_u64.min(1_000_000_u64.saturating_sub(scanned).saturating_add(1));
        let capsules = handle
            .store
            .capsule_page(Some(&filter.0), after, limit)
            .map_err(|error| store_reply(error, handle.commit_seq))?;
        let complete = capsules.len() < limit as usize;
        for capsule in capsules {
            if remaining_deadline(request.deadline, started).remaining_ms == 0 {
                return Err(EngineReply::new(
                    Outcome::Cancelled,
                    handle.commit_seq,
                    Value::Null,
                ));
            }
            if scanned == 1_000_000 {
                return Err(EngineReply::new(
                    Outcome::BudgetExceeded,
                    handle.commit_seq,
                    Value::Null,
                ));
            }
            if capsule.record_id.0 <= after {
                return Err(EngineReply::new(
                    Outcome::Corrupt,
                    handle.commit_seq,
                    Value::Null,
                ));
            }
            after = capsule.record_id.0;
            scanned += 1;
            let provenance: Value = serde_json::from_str(&capsule.provenance)
                .map_err(|_| EngineReply::new(Outcome::Corrupt, handle.commit_seq, Value::Null))?;
            let retained = crate::source_binding::read(
                handle.store.namespace(),
                &capsule.source_id,
                &provenance,
            )
            .map_err(|reason| {
                EngineReply::new(
                    Outcome::Corrupt,
                    handle.commit_seq,
                    json!({"reason":reason}),
                )
            })?;
            if binding.as_ref().is_some_and(|request| {
                !request.is_legacy
                    && retained
                        .as_ref()
                        .and_then(|binding| binding.full_source_id.as_ref())
                        != request.full_source_id.as_ref()
            }) {
                continue;
            }
            if provenance
                .pointer("/selection/observation_identity")
                .and_then(Value::as_str)
                == Some(identity)
            {
                return Err(EngineReply::rejected(
                    RejectReason::IdempotencyConflict,
                    handle.commit_seq,
                ));
            }
        }
        if complete {
            return Ok(());
        }
    }
}

/// Upgraded common requests can reconcile only the exact retained legacy body.
/// All fresh keys and all other digest differences retain ordinary semantics.
pub(super) fn lookup_observe_replay(
    handle: &mut NamespaceHandle,
    request: &ObserveRequest,
) -> Result<Option<EngineReply>, EngineReply> {
    let event = handle
        .store
        .event_for_key(&request.idempotency_key)
        .map_err(|error| store_reply(error, handle.commit_seq))?;
    let Some(event) = event else {
        return Ok(None);
    };
    if event.payload_sha256 == request.payload_sha256 {
        return lookup_replay(handle, &request.idempotency_key, &request.payload_sha256);
    }
    let binding = crate::source_binding::read(
        handle.store.namespace(),
        &request.source,
        &request.provenance,
    )
    .map_err(|reason| {
        EngineReply::rejected(RejectReason::InvalidRequest(reason), handle.commit_seq)
    })?;
    if let Some(binding) = binding.filter(|binding| !binding.is_legacy) {
        let mut legacy = request.clone();
        legacy.source = binding.legacy_source_id.clone();
        if let Some(provenance) = legacy.provenance.as_object_mut() {
            provenance.remove("source_binding");
        }
        let digest = legacy.canonical_payload_sha256().map_err(|reason| {
            EngineReply::rejected(RejectReason::InvalidRequest(reason), handle.commit_seq)
        })?;
        if digest == event.payload_sha256 {
            let durable: DurableReceipt = serde_json::from_str(&event.receipt)
                .map_err(|_| EngineReply::new(Outcome::Corrupt, handle.commit_seq, Value::Null))?;
            if let DurableOperation::Observe { record_id } = durable.operation {
                let capsule = handle
                    .store
                    .capsule(record_id)
                    .map_err(|error| store_reply(error, handle.commit_seq))?;
                if let Some(capsule) =
                    capsule.filter(|capsule| capsule.status != crate::store::CapsuleStatus::Revoked)
                {
                    let provenance: Value =
                        serde_json::from_str(&capsule.provenance).map_err(|_| {
                            EngineReply::new(Outcome::Corrupt, handle.commit_seq, Value::Null)
                        })?;
                    let retained = crate::source_binding::read(
                        handle.store.namespace(),
                        &capsule.source_id,
                        &provenance,
                    )
                    .map_err(|reason| {
                        EngineReply::new(
                            Outcome::Corrupt,
                            handle.commit_seq,
                            json!({"reason":reason}),
                        )
                    })?;
                    if retained.is_some_and(|retained| {
                        retained.is_legacy
                            && retained.full_source_id == binding.full_source_id
                            && retained.legacy_source_id == binding.legacy_source_id
                    }) && provenance["common_capsule"] == request.provenance["common_capsule"]
                    {
                        return lookup_replay(handle, &request.idempotency_key, &digest);
                    }
                }
            }
        }
    }
    lookup_replay(handle, &request.idempotency_key, &request.payload_sha256)
}
