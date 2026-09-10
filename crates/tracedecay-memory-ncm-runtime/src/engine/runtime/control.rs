//! Durable common controls over the existing kernel and capsule journal.

use super::super::*;
use super::selection::stable_reference;
use super::util::{
    canonical_digest, core_reply, durable_receipt, lookup_replay, meta_for_kernel, publish,
    put_checkpoint, read_live, remaining_deadline, resolve_affect, store_reply,
    unavailable_recovery,
};
use crate::store::{Capsule, CapsuleStatus, StoredCapsule};
use serde_json::{Value, json};
use std::time::Instant;
use tracedecay_memory_ncm_core::kernel::{NcmKernel, NewRecord};
use tracedecay_memory_ncm_core::records::RecordState;

impl NcmEngine {
    /// Executes a bounded canonical lifecycle control whose identity fields
    /// have already been projected into namespace-local opaque references.
    pub fn common_control(
        &self,
        namespace: &str,
        payload: Value,
        deadline: Deadline,
    ) -> EngineReply {
        if matches!(
            payload["action"].as_str(),
            Some("health" | "inspection" | "maintenance")
        ) {
            return self.common_read(namespace, &payload, deadline);
        }
        if payload["action"] == "delete_by_source" {
            let Some(sources) = payload["sources"].as_array().and_then(|sources| {
                sources
                    .iter()
                    .map(|source| source.as_str().map(|value| SourceId(value.to_owned())))
                    .collect::<Option<Vec<_>>>()
            }) else {
                return invalid("invalid deletion sources", 0);
            };
            let Some(key) = payload["idempotency_key"].as_str() else {
                return invalid("missing deletion key", 0);
            };
            let Some(before) = payload["expected_generation"].as_u64() else {
                return invalid("missing deletion generation", 0);
            };
            let bindings = match payload.get("source_bindings") {
                None => None,
                Some(value) => match serde_json::from_value::<
                    Vec<crate::source_binding::DeletionSourceBinding>,
                >(value.clone())
                {
                    Ok(bindings) => Some(bindings),
                    Err(_) => return invalid("invalid claimed deletion bindings", before),
                },
            };
            let mut reply = crate::privacy::delete_sources(
                self,
                namespace,
                &sources,
                key,
                deadline,
                before,
                bindings.as_deref(),
            );
            if reply.outcome == Outcome::Success {
                let matched = reply.payload["deleted_records"].as_u64().unwrap_or(0);
                let receipt_basis =
                    json!({"generation": reply.state_generation, "payload": reply.payload});
                reply.payload = json!({"common_control": "delete_by_source", "replayed": receipt_basis["payload"]["replayed"], "_retained_receipt": receipt_basis, "postcondition": {
                    "matched_effects": matched, "removed_effects": matched, "anonymized_effects": 0, "retained_under_lock": 0,
                    "remaining_influence_count": 0, "snapshots_examined": 0, "snapshots_rewritten": 0,
                    "verification_query_digest": payload["verification_query_digest"], "verification_state": "verified_absent",
                    "state_generation_before": before, "state_generation_after": reply.state_generation}});
            }
            return reply;
        }
        let started = Instant::now();
        let Some(key) = payload["idempotency_key"]
            .as_str()
            .filter(|value| !value.is_empty())
        else {
            return invalid("missing common control key", 0);
        };
        let Some(action) = payload["action"].as_str() else {
            return invalid("missing common action", 0);
        };
        let mut effect_payload = payload.clone();
        if let Some(object) = effect_payload.as_object_mut() {
            object.remove("expected_generation");
            object.remove("idempotency_key");
            object.remove("feedback_delivery_capsule");
        }
        if let Some(replacement) = effect_payload
            .get_mut("replacement")
            .and_then(Value::as_object_mut)
        {
            replacement.remove("idempotency_key");
            if let Some(provenance) = replacement
                .get_mut("provenance")
                .and_then(Value::as_object_mut)
            {
                provenance.remove("delivery_capsule");
            }
        }
        let digest = match canonical_digest(&effect_payload) {
            Ok(digest) => digest,
            Err(reason) => return invalid(&reason, 0),
        };
        if let Some(capsule) = payload.get("feedback_delivery_capsule") {
            if action != "feedback"
                || validate_feedback_delivery(namespace, key, &digest, capsule).is_err()
            {
                return invalid("invalid feedback delivery admission", 0);
            }
        }
        if deadline.remaining_ms == 0 {
            return EngineReply::new(Outcome::Cancelled, 0, Value::Null);
        }
        let replacement = if action == "correction" && payload["replacement"].is_object() {
            match replacement_request(&payload["replacement"], deadline) {
                Ok(request) => Some(request),
                Err(reason) => return invalid(&reason, 0),
            }
        } else {
            None
        };
        // Source deletion is checked before potentially expensive encoding and
        // again in the committing transaction below.
        {
            let mut namespaces = match self.namespace_lock() {
                Ok(value) => value,
                Err(reply) => return reply,
            };
            let handle = match self.ensure_handle(&mut namespaces, namespace, false) {
                Ok(Some(handle)) => handle,
                Ok(None) => return invalid("control target is absent", 0),
                Err(reply) => return reply,
            };
            if handle.fenced {
                return unavailable_recovery(handle.commit_seq);
            }
            match lookup_replay(handle, key, &digest) {
                Ok(Some(reply)) => return reply,
                Ok(None) => {}
                Err(reply) => return reply,
            }
            if let Some(request) = &replacement {
                if let Err(error) = handle
                    .store
                    .ensure_source_provenance_not_revoked(&request.source, &request.provenance)
                {
                    return store_reply(error, handle.commit_seq);
                }
            }
        }
        let embeddings = if let Some(request) = &replacement {
            match self.encoder.encode(
                &[&request.key_text, &request.value_text],
                remaining_deadline(deadline, started),
            ) {
                Ok(embeddings) if embeddings.len() == 2 => Some(embeddings),
                Ok(_) => return EngineReply::new(Outcome::Corrupt, 0, Value::Null),
                Err(error) => return super::util::encoder_reply(error, 0),
            }
        } else {
            None
        };
        let mut namespaces = match self.namespace_lock() {
            Ok(value) => value,
            Err(reply) => return reply,
        };
        let handle = match self.ensure_handle(&mut namespaces, namespace, false) {
            Ok(Some(handle)) => handle,
            Ok(None) => return invalid("control target is absent", 0),
            Err(reply) => return reply,
        };
        if handle.fenced {
            return unavailable_recovery(handle.commit_seq);
        }
        match lookup_replay(handle, key, &digest) {
            Ok(Some(reply)) => return reply,
            Ok(None) => {}
            Err(reply) => return reply,
        }
        if payload["expected_generation"].as_u64() != Some(handle.commit_seq) {
            return EngineReply::rejected(RejectReason::IdempotencyConflict, handle.commit_seq);
        }
        let live = match read_live(handle) {
            Ok(live) => live,
            Err(reply) => return reply,
        };
        let (target, mut provenance) =
            match resolve_target(namespace, handle, &payload["target"], deadline, started) {
                Ok(found) => found,
                Err(reply) => return reply,
            };
        if let Err(error) = handle
            .store
            .ensure_source_provenance_not_revoked(&target.source_id, &provenance)
        {
            return store_reply(error, handle.commit_seq);
        }
        let mut candidate = (*live).clone();
        let mut operations = Vec::new();
        let mut new_capsule = None;
        let mut output = json!({"common_control": action, "target_digest": payload["target_digest"], "replayed": false,
            "state_generation_before": handle.commit_seq, "state_generation_after": handle.commit_seq.saturating_add(1), "warnings": []});
        if let Some(capsule) = payload.get("feedback_delivery_capsule") {
            output["feedback_delivery_capsule"] = capsule.clone();
        }
        match action {
            "feedback" => {
                let Some(signal) = payload["signal"].as_str().filter(|signal| {
                    matches!(
                        *signal,
                        "helpful" | "harmful" | "ignored" | "corrected" | "superseded"
                    )
                }) else {
                    return invalid("invalid feedback signal", handle.commit_seq);
                };
                let Some(weight) = payload["weight"]
                    .as_f64()
                    .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
                else {
                    return invalid("invalid feedback weight", handle.commit_seq);
                };
                let previously_suppressed = provenance
                    .pointer("/control/feedback/suppressed")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let suppressed = if weight == 0.0 || signal == "ignored" {
                    previously_suppressed
                } else {
                    match signal {
                        "helpful" => false,
                        "harmful" | "corrected" | "superseded" => true,
                        _ => return invalid("invalid feedback signal", handle.commit_seq),
                    }
                };
                let mut centers_updated = 0;
                if signal == "helpful"
                    && weight > 0.0
                    && matches!(
                        candidate
                            .records
                            .get(target.record_id)
                            .map(|record| &record.state),
                        Some(RecordState::Valid)
                    )
                {
                    match candidate.feedback(&[target.record_id]) {
                        Ok(report) => centers_updated = report.centers_updated,
                        Err(error) => return core_reply(error, handle.commit_seq),
                    }
                    operations.push(DurableOperation::Feedback {
                        record_ids: vec![target.record_id],
                    });
                }
                if !provenance["control"].is_object() {
                    provenance["control"] = json!({});
                }
                let count = provenance
                    .pointer("/control/feedback/count")
                    .and_then(Value::as_u64)
                    .unwrap_or(0)
                    .saturating_add(1);
                let mut settled_counts = json!({"helpful": 0, "harmful": 0, "ignored": 0, "corrected": 0, "superseded": 0});
                if let Some(stored) = provenance.pointer("/control/feedback/settled_counts") {
                    let Some(stored) = stored.as_object() else {
                        return EngineReply::new(Outcome::Corrupt, handle.commit_seq, Value::Null);
                    };
                    for (signal, count) in stored {
                        if settled_counts.get(signal).is_none() || count.as_u64().is_none() {
                            return EngineReply::new(
                                Outcome::Corrupt,
                                handle.commit_seq,
                                Value::Null,
                            );
                        }
                        settled_counts[signal] = count.clone();
                    }
                }
                settled_counts[signal] = json!(
                    settled_counts[signal]
                        .as_u64()
                        .unwrap_or(0)
                        .saturating_add(1)
                );
                provenance["control"]["feedback"] = json!({"signal": signal, "weight": payload["weight"], "suppressed": suppressed, "settled_counts": settled_counts,
                    "count": count, "receipt": payload["outcome_receipt"], "occurred_at": payload["occurred_at"], "centers_updated": centers_updated});
                output["signal"] = json!(signal);
                output["applied_effect"] = json!({"feedback_recorded": true, "suppressed": suppressed,
                    "influence_changed": suppressed != previously_suppressed || centers_updated > 0, "centers_updated": centers_updated});
            }
            "correction" => {
                if target.status != CapsuleStatus::Valid {
                    return EngineReply::rejected(
                        RejectReason::IdempotencyConflict,
                        handle.commit_seq,
                    );
                }
                if payload["expected_revision"] != provenance["selection"]["revision_digest"]
                    || payload["expected_revision"].is_null()
                {
                    return EngineReply::rejected(
                        RejectReason::IdempotencyConflict,
                        handle.commit_seq,
                    );
                }
                let Some(kind) = payload["correction_kind"].as_str() else {
                    return invalid("missing correction kind", handle.commit_seq);
                };
                if let (Some(request), Some(embeddings)) = (replacement, embeddings) {
                    if let Err(error) = handle
                        .store
                        .ensure_source_provenance_not_revoked(&request.source, &request.provenance)
                    {
                        return store_reply(error, handle.commit_seq);
                    }
                    let affect = match resolve_affect(request.affect.as_ref()) {
                        Ok(value) => value,
                        Err(error) => return core_reply(error, handle.commit_seq),
                    };
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
                    let Some(record) = candidate.records.get(report.record_id) else {
                        return EngineReply::new(Outcome::Corrupt, handle.commit_seq, Value::Null);
                    };
                    let ltm_key = record.ltm_key.clone();
                    let Some(capsule_digest) = request
                        .provenance
                        .pointer("/common_capsule/sha256")
                        .and_then(Value::as_str)
                    else {
                        return invalid("replacement omitted retained evidence", handle.commit_seq);
                    };
                    let replacement_ref =
                        stable_reference(namespace, report.record_id.0, capsule_digest);
                    let Some(evidence) = payload["evidence_digest"].as_str() else {
                        return invalid("missing correction evidence", handle.commit_seq);
                    };
                    if let Err(error) = candidate.correction(
                        target.record_id,
                        report.record_id,
                        evidence.to_owned(),
                    ) {
                        return core_reply(error, handle.commit_seq);
                    }
                    operations.push(DurableOperation::Observe {
                        record_id: report.record_id,
                    });
                    operations.push(DurableOperation::Correction {
                        superseded: target.record_id,
                        superseding: report.record_id,
                        evidence: evidence.to_owned(),
                    });
                    new_capsule = Some(Capsule::new(
                        request.source,
                        request.key_text,
                        request.value_text,
                        affect,
                        request.surprise,
                        request.intensity,
                        embeddings[0].0.clone(),
                        embeddings[1].0.clone(),
                        ltm_key,
                        match serde_json::to_string(&request.provenance) {
                            Ok(value) => value,
                            Err(error) => return invalid(&error.to_string(), handle.commit_seq),
                        },
                    ));
                    let mut selection = provenance["control"]["selection"]
                        .as_object()
                        .cloned()
                        .unwrap_or_else(|| {
                            provenance["selection"]
                                .as_object()
                                .cloned()
                                .unwrap_or_default()
                        });
                    selection.insert(
                        "superseded_at".to_owned(),
                        payload["transition_time"].clone(),
                    );
                    selection.insert("superseded_by".to_owned(), json!(replacement_ref));
                    if !provenance["control"].is_object() {
                        provenance["control"] = json!({});
                    }
                    provenance["control"]["selection"] = Value::Object(selection);
                    provenance["control"]["validity_patch"] = json!({"superseded_at": payload["transition_wire"], "superseded_by": replacement_ref});
                    output["replacement_ref"] = json!(replacement_ref);
                } else {
                    let patch = &payload["selection_patch"];
                    if !patch.is_object() {
                        return invalid("missing metadata correction patch", handle.commit_seq);
                    }
                    let mut selection = provenance["control"]["selection"]
                        .as_object()
                        .cloned()
                        .unwrap_or_else(|| {
                            provenance["selection"]
                                .as_object()
                                .cloned()
                                .unwrap_or_default()
                        });
                    for (field, value) in patch.as_object().into_iter().flatten() {
                        selection.insert(field.clone(), value.clone());
                    }
                    if !provenance["control"].is_object() {
                        provenance["control"] = json!({});
                    }
                    provenance["control"]["selection"] = Value::Object(selection);
                    provenance["control"]["validity_patch"] = payload["validity_patch"].clone();
                    if kind == "restrict_scope" {
                        provenance["control"]["restricted"] = json!(true);
                    }
                }
                output["correction_kind"] = json!(kind);
                output["affected_provider_effects"] = json!(1);
            }
            _ => return invalid("unknown common control", handle.commit_seq),
        }
        commit_common(
            self,
            handle,
            candidate,
            key,
            &digest,
            operations,
            vec![(target.record_id, provenance)],
            new_capsule,
            output,
            deadline,
            started,
        )
    }
}

pub(super) fn replacement_request(
    value: &Value,
    deadline: Deadline,
) -> Result<ObserveRequest, String> {
    let request = ObserveRequest {
        idempotency_key: value["idempotency_key"]
            .as_str()
            .ok_or("replacement key missing")?
            .to_owned(),
        payload_sha256: value["payload_sha256"]
            .as_str()
            .ok_or("replacement digest missing")?
            .to_owned(),
        source: SourceId(
            value["source"]
                .as_str()
                .ok_or("replacement source missing")?
                .to_owned(),
        ),
        key_text: value["key_text"]
            .as_str()
            .ok_or("replacement key text missing")?
            .to_owned(),
        value_text: value["value_text"]
            .as_str()
            .ok_or("replacement value text missing")?
            .to_owned(),
        affect: serde_json::from_value(value["affect"].clone())
            .map_err(|error| error.to_string())?,
        surprise: value["surprise"]
            .as_f64()
            .ok_or("replacement surprise missing")? as f32,
        intensity: value["intensity"]
            .as_f64()
            .ok_or("replacement intensity missing")? as f32,
        provenance: value["provenance"].clone(),
        deadline,
    };
    if request.canonical_payload_sha256()? != request.payload_sha256 {
        return Err("replacement digest differs".to_owned());
    }
    Ok(request)
}

fn resolve_target(
    namespace: &str,
    handle: &NamespaceHandle,
    target: &Value,
    deadline: Deadline,
    started: Instant,
) -> Result<(StoredCapsule, Value), EngineReply> {
    let stable = target["stable_memory_ref"]
        .as_str()
        .ok_or_else(|| invalid("missing stable target", handle.commit_seq))?;
    let filter = target["legacy_source"]
        .as_str()
        .or_else(|| target["source"].as_str())
        .ok_or_else(|| invalid("missing source target", handle.commit_seq))?;
    let mut after = 0;
    let mut scanned = 0_u64;
    loop {
        if remaining_deadline(deadline, started).remaining_ms == 0 {
            return Err(EngineReply::new(
                Outcome::Cancelled,
                handle.commit_seq,
                Value::Null,
            ));
        }
        let limit = 128_u64.min(1_000_000_u64.saturating_sub(scanned).saturating_add(1));
        let capsules = handle
            .store
            .capsule_page(Some(filter), after, limit)
            .map_err(|error| store_reply(error, handle.commit_seq))?;
        let complete = capsules.len() < limit as usize;
        for capsule in capsules {
            if remaining_deadline(deadline, started).remaining_ms == 0 {
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
            if (target["source"].as_str() != Some(&capsule.source_id.0)
                && target["legacy_source"].as_str() != Some(&capsule.source_id.0))
                || capsule.status == CapsuleStatus::Revoked
            {
                continue;
            }
            let provenance: Value = serde_json::from_str(&capsule.provenance)
                .map_err(|error| invalid(&error.to_string(), handle.commit_seq))?;
            let Some(digest) = provenance
                .pointer("/common_capsule/sha256")
                .and_then(Value::as_str)
            else {
                continue;
            };
            if stable_reference(namespace, capsule.record_id.0, digest) != stable {
                continue;
            }
            if target["source_identity_sha256"] != provenance["selection"]["source_identity_sha256"]
            {
                return Err(invalid("source target binding differs", handle.commit_seq));
            }
            let binding = crate::source_binding::read(namespace, &capsule.source_id, &provenance)
                .map_err(|reason| invalid(&reason, handle.commit_seq))?;
            if target.get("legacy_source").is_some() {
                let binding = binding.ok_or_else(|| {
                    invalid(
                        "source target lacks verified original binding",
                        handle.commit_seq,
                    )
                })?;
                if binding
                    .full_source_id
                    .as_ref()
                    .map(|source| source.0.as_str())
                    != target["source"].as_str()
                    || Some(binding.legacy_source_id.0.as_str()) != target["legacy_source"].as_str()
                {
                    return Err(invalid("source target aliases differ", handle.commit_seq));
                }
            }
            return Ok((capsule, provenance));
        }
        if complete {
            return Err(invalid("stable target is unknown", handle.commit_seq));
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn commit_common(
    engine: &NcmEngine,
    handle: &mut NamespaceHandle,
    candidate: NcmKernel,
    key: &str,
    digest: &str,
    operations: Vec<DurableOperation>,
    updates: Vec<(RecordId, Value)>,
    capsule: Option<Capsule>,
    mut payload: Value,
    deadline: Deadline,
    started: Instant,
) -> EngineReply {
    if remaining_deadline(deadline, started).remaining_ms == 0 {
        return EngineReply::new(Outcome::Cancelled, handle.commit_seq, Value::Null);
    }
    let Some(pending_seq) = handle.commit_seq.checked_add(1) else {
        return EngineReply::new(Outcome::Corrupt, handle.commit_seq, Value::Null);
    };
    let prior = match handle.store.meta() {
        Ok(meta) => meta,
        Err(error) => return store_reply(error, handle.commit_seq),
    };
    payload["state_generation_after"] = json!(pending_seq);
    let reply = EngineReply::new(Outcome::Success, pending_seq, payload);
    let mut mutation = match handle.store.begin_mutation() {
        Ok(mutation) => mutation,
        Err(error) => return store_reply(error, handle.commit_seq),
    };
    if let Some(capsule) = capsule {
        let inserted = match mutation.insert_capsule(capsule) {
            Ok(id) => id,
            Err(error) => return store_reply(error, handle.commit_seq),
        };
        if !operations.iter().any(|operation| matches!(operation, DurableOperation::Observe { record_id } if record_id == &inserted)) { return EngineReply::new(Outcome::Corrupt, handle.commit_seq, Value::Null); }
    }
    for (id, value) in updates {
        let provenance = match serde_json::to_string(&value) {
            Ok(value) => value,
            Err(error) => return invalid(&error.to_string(), handle.commit_seq),
        };
        if let Err(error) = mutation.update_capsule_provenance(id, &provenance) {
            return store_reply(error, handle.commit_seq);
        }
    }
    for operation in &operations {
        if let DurableOperation::Correction { superseded, .. } = operation {
            if let Err(error) = mutation.mark_capsule_status(*superseded, CapsuleStatus::Superseded)
            {
                return store_reply(error, handle.commit_seq);
            }
        }
    }
    if let Err(error) = mutation.set_meta(&meta_for_kernel(
        &candidate,
        handle.epoch,
        pending_seq,
        prior.last_maintenance,
    )) {
        return store_reply(error, handle.commit_seq);
    }
    if pending_seq % CHECKPOINT_INTERVAL == 0 {
        if engine.consume_fault(FaultPoint::DuringCheckpoint) {
            return EngineReply::new(
                Outcome::Unavailable("injected failure during checkpoint".to_owned()),
                handle.commit_seq,
                Value::Null,
            );
        }
        if let Err(reply) = put_checkpoint(&mut mutation, pending_seq, handle.epoch, &candidate) {
            return reply;
        }
    }
    let receipt = match durable_receipt(
        &reply,
        DurableOperation::CommonControl { operations },
        &candidate,
    ) {
        Ok(value) => value,
        Err(reply) => return reply,
    };
    if let Err(error) = mutation.append_event(
        "common_control",
        Some(key),
        digest,
        &receipt,
        candidate.scheduler.tick.0,
    ) {
        return store_reply(error, handle.commit_seq);
    }
    if engine.consume_fault(FaultPoint::BeforeCommit) {
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
    if engine.consume_fault(FaultPoint::AfterCommitBeforePublish) {
        handle.fenced = true;
        return EngineReply::new(
            Outcome::EffectUnknown,
            committed,
            json!({"commit_seq": committed}),
        );
    }
    if publish(handle, candidate).is_err() {
        handle.fenced = true;
        return EngineReply::new(
            Outcome::EffectUnknown,
            committed,
            json!({"commit_seq": committed}),
        );
    }
    if engine.consume_fault(FaultPoint::AfterPublishBeforeAck)
        || remaining_deadline(deadline, started).remaining_ms == 0
    {
        return EngineReply::new(
            Outcome::EffectUnknown,
            committed,
            json!({"commit_seq": committed}),
        );
    }
    reply
}

// Optional for existing common-control wire requests; an absent capsule never
// supplies a public identity. New capsules are retained in the same event as the effect.
fn validate_feedback_delivery(
    namespace: &str,
    key: &str,
    semantic: &str,
    capsule: &Value,
) -> Result<(), ()> {
    let object = capsule.as_object().ok_or(())?;
    if object.len() != 3
        || !["version", "bytes", "sha256"]
            .iter()
            .all(|field| object.contains_key(*field))
    {
        return Err(());
    }
    super::util::validate_common_capsule(&json!({"common_capsule": capsule})).map_err(|_| ())?;
    let bytes = capsule["bytes"]
        .as_array()
        .ok_or(())?
        .iter()
        .map(|value| {
            value
                .as_u64()
                .and_then(|value| u8::try_from(value).ok())
                .ok_or(())
        })
        .collect::<Result<Vec<_>, _>>()?;
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Admission {
        namespace: String,
        operation_id: String,
        idempotency_key: String,
        request_semantic_sha256: String,
    }
    let admission: Admission = serde_json::from_slice(&bytes).map_err(|_| ())?;
    super::util::validate_idempotency_key(&admission.idempotency_key).map_err(|_| ())?;
    if admission.namespace != namespace
        || admission.request_semantic_sha256 != semantic
        || admission.operation_id.is_empty()
        || admission.operation_id.len() > 256
        || crate::source_binding::opaque_id(
            namespace,
            b"idempotency-key",
            &admission.idempotency_key,
        ) != key
    {
        return Err(());
    }
    Ok(())
}

fn invalid(reason: &str, generation: u64) -> EngineReply {
    EngineReply::rejected(RejectReason::InvalidRequest(reason.to_owned()), generation)
}
