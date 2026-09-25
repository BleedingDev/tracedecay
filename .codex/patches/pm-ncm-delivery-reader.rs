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
        let Some(key) = payload["delivery_key"].as_str() else { return Ok(None); };
        let Some(event) = handle.store.event_for_key(key).map_err(|error| store_reply(error, generation))? else {
            return Ok(None);
        };
        let durable: DurableReceipt = serde_json::from_str(&event.receipt).map_err(|_| corrupt())?;
        let mut records = std::collections::BTreeSet::new();
        let (operation, page_capsule) = match &durable.operation {
            DurableOperation::Observe { record_id } if event.kind == "observe" => {
                records.insert(*record_id);
                ("observe", None)
            }
            DurableOperation::CommonControl { operations }
                if event.kind == "common_control" && operations.is_empty()
                    && durable.reply.payload["common_portability"] == "replay" => {
                if durable.reply.payload["partial"] != false { return Ok(None); }
                let Some(capsule) = durable.reply.payload.get("page_delivery_capsule") else { return Ok(None); };
                let items = durable.reply.payload["items"].as_array().ok_or_else(corrupt)?;
                if items.len() > 4096 { return Err(corrupt()); }
                for item in items {
                    match item["state"].as_str() {
                        Some("applied" | "delivery_duplicate" | "source_already_applied") => {
                            let id = item["record_id"].as_u64().filter(|id| *id > 0).ok_or_else(corrupt)?;
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
        { return Err(corrupt()); }
        let validate_delivery = |capsule: &Value| -> Result<(), EngineReply> {
            let object = capsule.as_object().ok_or_else(corrupt)?;
            if object.len() != 3 || !["version", "bytes", "sha256"].iter().all(|key| object.contains_key(*key)) {
                return Err(corrupt());
            }
            super::util::validate_common_capsule(&json!({"common_capsule": capsule}))
                .map_err(|error| store_reply(error, generation))
        };
        if let Some(capsule) = page_capsule { validate_delivery(capsule)?; }
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
        let receipt_digest: String = digest.finalize().iter().map(|byte| format!("{byte:02x}")).collect();
        let after = payload["after"].as_u64().unwrap_or(0);
        let maximum = payload["maximum_items"].as_u64().unwrap_or(1).min(1_000_000);
        let maximum_bytes = payload["maximum_bytes"].as_u64().unwrap_or(1_048_576).min(1_073_741_824);
        let mut rows = Vec::new();
        let mut partial = records.is_empty();
        let mut cursor = after;
        let mut next_cursor = None;
        let mut scanned = 0_u64;
        let mut bytes = 0_u64;
        for id in records.into_iter().filter(|id| id.0 > after) {
            if remaining_deadline(deadline, started).remaining_ms == 0 || rows.len() as u64 >= maximum {
                partial = true;
                next_cursor = Some(cursor);
                break;
            }
            let Some(capsule) = handle.store.capsule(id).map_err(|error| store_reply(error, generation))? else {
                partial = true;
                cursor = id.0;
                continue;
            };
            if capsule.status == CapsuleStatus::Revoked {
                partial = true;
                cursor = id.0;
                continue;
            }
            if capsule.commit_seq > event.seq || (operation == "observe" && capsule.commit_seq != event.seq) {
                return Err(corrupt());
            }
            let provenance: Value = serde_json::from_str(&capsule.provenance).map_err(|_| corrupt())?;
            super::util::validate_common_capsule(&provenance).map_err(|error| store_reply(error, generation))?;
            let Some(capsule_digest) = provenance["common_capsule"]["sha256"].as_str() else {
                partial = true;
                cursor = id.0;
                continue;
            };
            let stable = stable_reference(namespace, id.0, capsule_digest);
            scanned += 1;
            if payload.get("stable_memory_ref").is_some_and(|selected| selected.as_str() != Some(stable.as_str())) {
                cursor = id.0;
                continue;
            }
            let Some(delivery_capsule) = page_capsule.or_else(|| provenance.get("delivery_capsule")) else {
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
        if rows.is_empty() { partial = true; }
        Ok(Some((rows, partial, next_cursor, scanned)))
    })();
    match result {
        Ok(result) => {
            let (items, partial, cursor, scanned) = result.unwrap_or_else(|| (Vec::new(), true, None, 0));
            EngineReply::new(Outcome::Success, generation,
                json!({"common_control": "inspection", "view": "delivery_receipt", "items": items,
                    "partial": partial, "cursor_after": cursor, "state_generation": generation, "scanned_items": scanned}))
        }
        Err(reply) => reply,
    }
}
