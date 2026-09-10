//! Bounded common inspection and explicitly scheduled maintenance.

use super::super::*;
use super::selection::stable_reference;
use super::util::{read_live, remaining_deadline, store_reply, unavailable_recovery};
use crate::store::CapsuleStatus;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::time::Instant;

impl NcmEngine {
    pub(super) fn common_read(
        &self,
        namespace: &str,
        payload: &Value,
        deadline: Deadline,
    ) -> EngineReply {
        let started = Instant::now();
        let action = payload["action"].as_str().unwrap_or("");
        if action == "maintenance" {
            return self.common_maintenance(namespace, payload, deadline);
        }
        let maximum = payload["maximum_items"]
            .as_u64()
            .unwrap_or(1)
            .min(1_000_000);
        let max_bytes = payload["maximum_bytes"]
            .as_u64()
            .unwrap_or(1_048_576)
            .min(1_073_741_824);
        let after = payload["after"].as_u64().unwrap_or(0);
        let mut namespaces = match self.namespace_lock() {
            Ok(value) => value,
            Err(reply) => return reply,
        };
        let handle = match self.ensure_handle(&mut namespaces, namespace, false) {
            Ok(Some(handle)) => handle,
            Ok(None) => {
                if action == "inspection"
                    && matches!(
                        payload["view"].as_str(),
                        Some("delivery_receipt" | "maintenance_receipt")
                    )
                {
                    if payload["expected_generation"].as_u64() != Some(0) {
                        return EngineReply::rejected(RejectReason::IdempotencyConflict, 0);
                    }
                    return EngineReply::new(
                        Outcome::Success,
                        0,
                        json!({
                            "common_control": "inspection", "view": payload["view"], "items": [],
                            "partial": true, "cursor_after": null, "state_generation": 0, "scanned_items": 0
                        }),
                    );
                }
                if action == "inspection" && payload["view"] == "capability_status" {
                    if payload["expected_generation"].as_u64() != Some(0) {
                        return EngineReply::rejected(RejectReason::IdempotencyConflict, 0);
                    }
                    return EngineReply::new(
                        Outcome::Success,
                        0,
                        json!({"common_control": "inspection", "view": "capability_status", "state_generation": 0}),
                    );
                }
                return EngineReply::new(
                    Outcome::Success,
                    0,
                    json!({"common_control": action, "no_change": true,
                "items": [], "partial": false, "cursor_after": null, "state_generation": 0, "scanned_items": 0,
                "task": payload["task"], "dry_run": payload["dry_run"], "changed_items": 0, "removed_items": 0,
                "state_generation_before": 0, "state_generation_after": 0, "resume_cursor": null, "warnings": []}),
                );
            }
            Err(reply) => return reply,
        };
        if handle.fenced {
            return unavailable_recovery(handle.commit_seq);
        }
        let generation = handle.commit_seq;
        if payload["expected_generation"].as_u64() != Some(generation) {
            return EngineReply::rejected(RejectReason::IdempotencyConflict, generation);
        }
        if action == "inspection" && payload["view"] == "capability_status" {
            if let Err(reply) = read_live(handle) {
                return reply;
            }
            return EngineReply::new(
                Outcome::Success,
                generation,
                json!({"common_control": "inspection", "view": "capability_status", "state_generation": generation}),
            );
        }
        if action == "health" {
            let live = match read_live(handle) {
                Ok(value) => value,
                Err(reply) => return reply,
            };
            let usage = match handle.store.usage() {
                Ok(value) => value,
                Err(error) => return store_reply(error, generation),
            };
            return EngineReply::new(
                Outcome::Success,
                generation,
                json!({"common_control": "health", "state_generation": generation,
                "records": live.records.len(), "state_digest": super::util::sha256_hex(&live.state_digest()), "quota_usage": usage, "fenced": false}),
            );
        }
        if action == "inspection" && payload["view"] == "maintenance_receipt" {
            return super::common_maintenance::inspect_receipt(namespace, handle, payload);
        }
        if action == "inspection" && payload["view"] == "delivery_receipt" {
            return delivery_receipt(namespace, handle, payload, deadline, started);
        }
        if action == "inspection"
            && payload["view"] == "trace"
            && payload.get("legacy_record_id").is_some()
        {
            return legacy_trace(namespace, handle, payload, deadline, started);
        }
        let capsules = match handle.store.capsule_page(
            payload["source"].as_str(),
            after,
            maximum.saturating_add(1),
        ) {
            Ok(value) => value,
            Err(error) => return store_reply(error, generation),
        };
        let mut partial = capsules.len() as u64 > maximum;
        let mut bytes = 0_u64;
        let mut rows = Vec::new();
        let mut scanned = 0_u64;
        let mut cursor = after;
        for capsule in capsules.into_iter().take(maximum as usize) {
            if remaining_deadline(deadline, started).remaining_ms == 0 {
                partial = true;
                break;
            }
            let provenance: Value = match serde_json::from_str(&capsule.provenance) {
                Ok(value) => value,
                Err(_) => return EngineReply::new(Outcome::Corrupt, generation, Value::Null),
            };
            if let Err(reason) =
                crate::source_binding::read(namespace, &capsule.source_id, &provenance)
            {
                return EngineReply::new(Outcome::Corrupt, generation, json!({"reason":reason}));
            }
            let size = (capsule.provenance.len()
                + capsule.key_text.len()
                + capsule.value_text.len()) as u64;
            if bytes.saturating_add(size) > max_bytes {
                partial = true;
                break;
            }
            bytes += size;
            scanned += 1;
            cursor = capsule.record_id.0;
            let Some(digest) = provenance
                .pointer("/common_capsule/sha256")
                .and_then(Value::as_str)
            else {
                partial = true;
                continue;
            };
            let stable = stable_reference(namespace, capsule.record_id.0, digest);
            if (payload["view"] == "trace"
                || (payload["view"] == "source_influence"
                    && payload.get("stable_memory_ref").is_some()))
                && payload["stable_memory_ref"] != stable
            {
                continue;
            }
            let suppressed = provenance
                .pointer("/control/feedback/suppressed")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let restricted = provenance
                .pointer("/control/restricted")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let selection = provenance
                .pointer("/control/selection")
                .unwrap_or(&provenance["selection"]);
            let revoked = selection["revoked_at"].as_i64().is_some();
            rows.push(json!({"stable_memory_ref": stable,
                "record_id": capsule.record_id.0, "source": capsule.source_id.0, "provenance": provenance,
                "content": capsule.value_text,
                "active": capsule.status == CapsuleStatus::Valid && !suppressed && !restricted && !revoked,
                "disposition": if revoked { "revoked" } else if capsule.status == CapsuleStatus::Superseded { "superseded" } else { "available" }}));
        }
        if action == "inspection" {
            return EngineReply::new(
                Outcome::Success,
                generation,
                json!({"common_control": action, "view": payload["view"], "items": rows,
                "partial": partial, "cursor_after": if partial { Some(cursor) } else { None }, "state_generation": generation, "scanned_items": scanned}),
            );
        }
        EngineReply::rejected(
            RejectReason::InvalidRequest("unknown common read action".to_owned()),
            generation,
        )
    }
}

// A decimal reference is only the original raw record ID in this exact namespace.
// It never becomes a canonical source or an alias for a common-capsule reference.
fn legacy_trace(
    namespace: &str,
    handle: &NamespaceHandle,
    payload: &Value,
    deadline: Deadline,
    started: Instant,
) -> EngineReply {
    let generation = handle.commit_seq;
    let result = |items: Vec<Value>, scanned| {
        EngineReply::new(
            Outcome::Success,
            generation,
            json!({"common_control":"inspection","view":"trace","items":items,
            "partial":true,"cursor_after":null,"state_generation":generation,"scanned_items":scanned}),
        )
    };
    let Some(id) = payload["legacy_record_id"]
        .as_u64()
        .filter(|id| *id > 0 && *id <= i64::MAX as u64)
    else {
        return EngineReply::rejected(
            RejectReason::InvalidRequest("invalid legacy trace reference".into()),
            generation,
        );
    };
    if payload["stable_memory_ref"].as_str() != Some(id.to_string().as_str()) {
        return EngineReply::rejected(
            RejectReason::InvalidRequest("legacy trace reference differs".into()),
            generation,
        );
    }
    if remaining_deadline(deadline, started).remaining_ms == 0 {
        return EngineReply::new(Outcome::Cancelled, generation, Value::Null);
    }
    if payload["after"].as_u64().unwrap_or(0) != 0
        || payload["maximum_items"].as_u64().unwrap_or(0) == 0
    {
        return result(Vec::new(), 0);
    }
    let capsule = match handle.store.capsule(RecordId(id)) {
        Ok(Some(capsule)) => capsule,
        Ok(None) => return result(Vec::new(), 0),
        Err(error) => return store_reply(error, generation),
    };
    let provenance: Value = match serde_json::from_str(&capsule.provenance) {
        Ok(value) => value,
        Err(_) => return EngineReply::new(Outcome::Corrupt, generation, Value::Null),
    };
    // Existing common evidence must use its original common reference. Never
    // reinterpret an incomplete or damaged common capsule as a legacy record.
    if capsule.status != CapsuleStatus::Valid
        || provenance.get("common_capsule").is_some()
        || provenance.get("delivery_capsule").is_some()
        || provenance.get("source_binding").is_some()
        || provenance["control"]["restricted"] == true
        || provenance["control"]["feedback"]["suppressed"] == true
        || provenance
            .pointer("/control/selection/revoked_at")
            .is_some_and(|value| !value.is_null())
        || provenance
            .pointer("/selection/revoked_at")
            .is_some_and(|value| !value.is_null())
    {
        return result(Vec::new(), 1);
    }
    if let Err(error) = handle
        .store
        .ensure_source_provenance_not_revoked(&capsule.source_id, &provenance)
    {
        return if matches!(error, crate::store::StoreError::SourceRevoked) {
            result(Vec::new(), 1)
        } else {
            store_reply(error, generation)
        };
    }
    let maximum_bytes = payload["maximum_bytes"]
        .as_u64()
        .unwrap_or(0)
        .min(1_073_741_824);
    let maximum_prefix = usize::try_from(maximum_bytes)
        .unwrap_or(usize::MAX)
        .min(capsule.value_text.len());
    let boundaries = capsule
        .value_text
        .char_indices()
        .map(|(index, _)| index)
        .chain(std::iter::once(capsule.value_text.len()))
        .take_while(|index| *index <= maximum_prefix)
        .collect::<Vec<_>>();
    // Bind the selected namespace without returning its forbidden raw surface identity.
    let legacy_namespace_sha256 = super::util::sha256_hex(namespace.as_bytes());
    let (mut low, mut high) = (0, boundaries.len());
    let mut selected = None;
    while low < high {
        if remaining_deadline(deadline, started).remaining_ms == 0 {
            return EngineReply::new(Outcome::Cancelled, generation, Value::Null);
        }
        let middle = low + (high - low) / 2;
        let content = &capsule.value_text[..boundaries[middle]];
        let row = json!({"legacy_record_id":id,"legacy_namespace_sha256":legacy_namespace_sha256,"stable_memory_ref":id.to_string(),
            "content":content,"content_sha256":super::util::sha256_hex(content.as_bytes()),"original_source":null});
        let encoded = match serde_json::to_vec(&row) {
            Ok(encoded) => encoded,
            Err(_) => return EngineReply::new(Outcome::Corrupt, generation, Value::Null),
        };
        if encoded.len() as u64 <= maximum_bytes {
            selected = Some(row);
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    result(selected.into_iter().collect(), 1)
}

fn delivery_receipt(
    namespace: &str,
    handle: &NamespaceHandle,
    payload: &Value,
    deadline: Deadline,
    started: Instant,
) -> EngineReply {
    let generation = handle.commit_seq;
    let corrupt = || EngineReply::new(Outcome::Corrupt, generation, Value::Null);
    let result = (|| -> Result<Option<(Vec<Value>, bool, Option<u64>, u64)>, EngineReply> {
        let Some(key) = payload["delivery_key"].as_str() else {
            return Ok(None);
        };
        let Some(event) = handle
            .store
            .event_for_key(key)
            .map_err(|error| store_reply(error, generation))?
        else {
            return Ok(None);
        };
        let durable: DurableReceipt =
            serde_json::from_str(&event.receipt).map_err(|_| corrupt())?;
        let mut records = std::collections::BTreeSet::new();
        let (operation, page_capsule) = match &durable.operation {
            DurableOperation::Observe { record_id } if event.kind == "observe" => {
                records.insert(*record_id);
                ("observe", None)
            }
            DurableOperation::CommonControl { operations }
                if event.kind == "common_control"
                    && operations.is_empty()
                    && durable.reply.payload["common_portability"] == "replay" =>
            {
                if durable.reply.payload["partial"] != false {
                    return Ok(None);
                }
                let Some(capsule) = durable.reply.payload.get("page_delivery_capsule") else {
                    return Ok(None);
                };
                let items = durable.reply.payload["items"]
                    .as_array()
                    .ok_or_else(corrupt)?;
                if items.len() > 4096 {
                    return Err(corrupt());
                }
                for item in items {
                    match item["state"].as_str() {
                        Some("applied" | "delivery_duplicate" | "source_already_applied") => {
                            let id = item["record_id"]
                                .as_u64()
                                .filter(|id| *id > 0)
                                .ok_or_else(corrupt)?;
                            records.insert(RecordId(id));
                        }
                        Some("rejected") if item["record_id"].is_null() => {}
                        _ => return Err(corrupt()),
                    }
                }
                ("replay", Some(capsule))
            }
            _ => return Ok(None),
        };
        if event.idempotency_key.as_deref() != Some(key)
            || event.seq > generation
            || durable.reply.state_generation != event.seq
            || durable.reply.outcome != Outcome::Success
        {
            return Err(corrupt());
        }
        let validate_delivery = |capsule: &Value| -> Result<(), EngineReply> {
            let object = capsule.as_object().ok_or_else(corrupt)?;
            if object.len() != 3
                || !["version", "bytes", "sha256"]
                    .iter()
                    .all(|key| object.contains_key(*key))
            {
                return Err(corrupt());
            }
            super::util::validate_common_capsule(&json!({"common_capsule": capsule}))
                .map_err(|error| store_reply(error, generation))
        };
        if let Some(capsule) = page_capsule {
            validate_delivery(capsule)?;
        }
        // Match the worker's original durable payload and operation domain exactly.
        let mut basis = durable.reply.payload.clone();
        if let Some(object) = basis.as_object_mut() {
            object.remove("replayed");
            object.remove("common_observation");
        }
        let basis_bytes = serde_json::to_vec(&basis).map_err(|_| corrupt())?;
        let mut digest = Sha256::new();
        digest.update(b"tracedecay.ncm.rust-worker-receipt.v1\0");
        digest.update((operation.len() as u64).to_be_bytes());
        digest.update(operation.as_bytes());
        digest.update(durable.reply.state_generation.to_be_bytes());
        digest.update((basis_bytes.len() as u64).to_be_bytes());
        digest.update(&basis_bytes);
        let receipt_digest: String = digest
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let after = payload["after"].as_u64().unwrap_or(0);
        let maximum = payload["maximum_items"]
            .as_u64()
            .unwrap_or(1)
            .min(1_000_000);
        let maximum_bytes = payload["maximum_bytes"]
            .as_u64()
            .unwrap_or(1_048_576)
            .min(1_073_741_824);
        let mut rows = Vec::new();
        let mut partial = records.is_empty();
        let mut cursor = after;
        let mut next_cursor = None;
        let mut scanned = 0_u64;
        let mut bytes = 0_u64;
        for id in records.into_iter().filter(|id| id.0 > after) {
            if remaining_deadline(deadline, started).remaining_ms == 0
                || rows.len() as u64 >= maximum
            {
                partial = true;
                next_cursor = Some(cursor);
                break;
            }
            let Some(capsule) = handle
                .store
                .capsule(id)
                .map_err(|error| store_reply(error, generation))?
            else {
                partial = true;
                cursor = id.0;
                continue;
            };
            if capsule.status == CapsuleStatus::Revoked {
                partial = true;
                cursor = id.0;
                continue;
            }
            if capsule.commit_seq > event.seq
                || (operation == "observe" && capsule.commit_seq != event.seq)
            {
                return Err(corrupt());
            }
            let provenance: Value =
                serde_json::from_str(&capsule.provenance).map_err(|_| corrupt())?;
            super::util::validate_common_capsule(&provenance)
                .map_err(|error| store_reply(error, generation))?;
            let Some(capsule_digest) = provenance["common_capsule"]["sha256"].as_str() else {
                partial = true;
                cursor = id.0;
                continue;
            };
            let stable = stable_reference(namespace, id.0, capsule_digest);
            scanned += 1;
            if payload
                .get("stable_memory_ref")
                .is_some_and(|selected| selected.as_str() != Some(stable.as_str()))
            {
                cursor = id.0;
                continue;
            }
            let Some(delivery_capsule) =
                page_capsule.or_else(|| provenance.get("delivery_capsule"))
            else {
                partial = true;
                cursor = id.0;
                continue;
            };
            validate_delivery(delivery_capsule)?;
            let row = json!({"record_id": id.0, "source": capsule.source_id.0,
                "provenance": provenance, "stable_memory_ref": stable,
                "delivery_capsule": delivery_capsule, "provider_receipt_digest": receipt_digest});
            let size = serde_json::to_vec(&row).map_err(|_| corrupt())?.len() as u64;
            if bytes.saturating_add(size) > maximum_bytes {
                partial = true;
                next_cursor = Some(cursor);
                break;
            }
            bytes += size;
            cursor = id.0;
            rows.push(row);
        }
        if rows.is_empty() {
            partial = true;
        }
        Ok(Some((rows, partial, next_cursor, scanned)))
    })();
    match result {
        Ok(result) => {
            let (items, partial, cursor, scanned) =
                result.unwrap_or_else(|| (Vec::new(), true, None, 0));
            EngineReply::new(
                Outcome::Success,
                generation,
                json!({"common_control": "inspection", "view": "delivery_receipt", "items": items,
                    "partial": partial, "cursor_after": cursor, "state_generation": generation, "scanned_items": scanned}),
            )
        }
        Err(reply) => reply,
    }
}
