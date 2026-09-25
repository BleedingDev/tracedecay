from pathlib import Path
base=Path('.codex/patches/pm-ncm-shapes-draft')
def edit(path,old,new):
 p=base/path;s=p.read_text();assert s.count(old)==1,(path,s.count(old),old[:70]);p.write_text(s.replace(old,new))
l='crates/tracedecay-memory-provider-ncm/src/common/lifecycle.rs'
p='crates/tracedecay-memory-provider-ncm/src/common/portability.rs'
r='crates/tracedecay-memory-ncm-runtime/src/engine/runtime/portability.rs'
c='crates/tracedecay-memory-ncm-runtime/src/engine/runtime/common_reads.rs'
edit(l,'''                "delivery_receipt" | "maintenance_receipt" => {
                    fields(&value["selector"], &["idempotency_key"])?;''','''                "delivery_receipt" | "maintenance_receipt" => {
                    let optional = if view == "delivery_receipt" { "stable_memory_ref" } else { "operation_id" };
                    if let Some(selected) = selector.get(optional) {
                        fields(&value["selector"], &["idempotency_key", optional])?;
                        let selected = string(selected)?;
                        if optional == "stable_memory_ref" {
                            if !crate::NcmProviderAdapter::valid_sha256(selected.strip_prefix("ncm-memory:")?) {
                                return None;
                            }
                            control["stable_memory_ref"] = json!(selected);
                        }
                    } else {
                        fields(&value["selector"], &["idempotency_key"])?;
                    }''')
# Strict wrapper validation before any allocation/inner JSON parsing. Shared with public Replay reconstruction.
edit(l,'fn cursor_binding(call: &ProviderCall, value: &Value) -> Option<String> {','''pub(super) fn decode_receipt_capsule(capsule: &Value) -> Option<Value> {
    fields(capsule, &["version", "bytes", "sha256"])?;
    if capsule["bytes"].as_array()?.len() > 131_072
        || !crate::NcmProviderAdapter::valid_sha256(capsule["sha256"].as_str()?)
    {
        return None;
    }
    decode_named_capsule(&json!({"receipt": capsule}), "receipt")
}

fn maintenance_receipt_item(call: &ProviderCall, request: &Value, row: &Value) -> Option<Value> {
    fields(row, &["maintenance_receipt", "provider_receipt_digest"])?;
    let retained = &row["maintenance_receipt"];
    fields(retained, &["admission", "request_semantic_sha256", "outcome", "event_basis"])?;
    let admission = decode_receipt_capsule(&retained["admission"])?;
    fields(&admission, &["namespace", "operation_id", "idempotency_key", "request_semantic_sha256"])?;
    let operation_id = string(&admission["operation_id"])?;
    let idempotency_key = string(&admission["idempotency_key"])?;
    let semantic = string(&admission["request_semantic_sha256"])?;
    let digest = string(&row["provider_receipt_digest"])?;
    let basis = &retained["event_basis"];
    let outcome = &retained["outcome"];
    let before = outcome["state_generation_before"].as_u64()?;
    let after = outcome["state_generation_after"].as_u64()?;
    let scanned = outcome["scanned_items"].as_u64()?;
    let changed = outcome["changed_items"].as_u64()?;
    let removed = outcome["removed_items"].as_u64()?;
    if admission["namespace"] != NcmNamespace::from_exact_scope(&call.exact_scope).as_str()
        || idempotency_key != request["selector"]["idempotency_key"].as_str()?
        || request["selector"].get("operation_id").is_some_and(|selected| selected.as_str() != Some(operation_id))
        || !crate::NcmProviderAdapter::valid_sha256(semantic)
        || !crate::NcmProviderAdapter::valid_sha256(digest)
        || retained["request_semantic_sha256"] != semantic
        || basis["payload_sha256"] != semantic
        || basis["sequence"].as_u64()? != after
        || before.checked_add(1)? != after
        || after > call.expected_state_generation
        || changed.checked_add(removed)? > scanned
        || outcome["dry_run"] != false
        || outcome["partial"] != false
        || !outcome["resume_cursor"].is_null()
        || !outcome["warnings"].as_array()?.is_empty()
    {
        return None;
    }
    let task = string(&outcome["task"])?;
    if !["consolidate", "decay", "prune_expired", "repair", "compact"].contains(&task) {
        return None;
    }
    Some(json!({"operation_id": operation_id, "idempotency_key": idempotency_key,
        "outcome": {"task": task, "dry_run": false, "scanned_items": scanned,
            "changed_items": changed, "removed_items": removed, "state_changed": outcome["state_changed"].as_bool()?,
            "partial": false, "resume_cursor": null,
            "receipt": {"state_generation_before": before, "state_generation_after": after, "provider_receipt_digest": digest}}}))
}

fn cursor_binding(call: &ProviderCall, value: &Value) -> Option<String> {''')
# Receipt path before provenance hydration: maintenance has no source capsule.
edit(l,'''        for row in payload["items"].as_array()? {
            let retained = decode_capsule(&row["provenance"])?;''','''        let mut delivery_identity = None;
        let mut seen_stable = BTreeSet::new();
        for row in payload["items"].as_array()? {
            if items.len() as u64 >= request["maximum_items"].as_u64()? {
                partial = true;
                break;
            }
            if request["view"] == "maintenance_receipt" {
                if !items.is_empty() { return None; }
                let item = maintenance_receipt_item(call, &request, row)?;
                let item_bytes = serde_json::to_vec(&item).ok()?.len() as u64;
                if bytes_used.saturating_add(item_bytes) > request["maximum_bytes"].as_u64()? {
                    partial = true;
                    break;
                }
                bytes_used += item_bytes;
                items.push(item);
                continue;
            }
            let retained = decode_capsule(&row["provenance"])?;''')
edit(l,'''            let item = if matches!(
                request["view"].as_str(),
                Some("delivery_receipt" | "maintenance_receipt")
            ) {
                let delivery = decode_named_capsule(&row["provenance"], "delivery_capsule")?;
                if delivery["idempotency_key"] != request["selector"]["idempotency_key"] {
                    return None;
                }
                let receipt = string(&row["provider_receipt_digest"])?;
                if !crate::NcmProviderAdapter::valid_sha256(receipt) {
                    return None;
                }
                json!({"operation_id": string(&delivery["operation_id"])?, "idempotency_key": delivery["idempotency_key"], "provider_receipt_digest": receipt, "stable_memory_ref": stable})''','''            let item = if request["view"] == "delivery_receipt" {
                let delivery = decode_receipt_capsule(&row["delivery_capsule"])?;
                fields(&delivery, &["operation_id", "idempotency_key"])?;
                let operation_id = string(&delivery["operation_id"])?;
                let idempotency_key = string(&delivery["idempotency_key"])?;
                let receipt = string(&row["provider_receipt_digest"])?;
                if idempotency_key != request["selector"]["idempotency_key"].as_str()?
                    || request["selector"].get("stable_memory_ref").is_some_and(|selected| selected.as_str() != Some(stable.as_str()))
                    || !crate::NcmProviderAdapter::valid_sha256(receipt)
                    || !seen_stable.insert(stable.clone())
                { return None; }
                let identity = (operation_id.to_owned(), idempotency_key.to_owned(), receipt.to_owned());
                if delivery_identity.as_ref().is_some_and(|previous| previous != &identity) { return None; }
                delivery_identity = Some(identity);
                json!({"operation_id": operation_id, "idempotency_key": idempotency_key, "provider_receipt_digest": receipt, "stable_memory_ref": stable})''')
edit(p,'''    control["items"] = json!(projected);''','''    let page_delivery = serde_json::to_vec(&json!({
        "operation_id": call.operation_id,
        "idempotency_key": call.idempotency_key.as_deref()?
    })).ok()?;
    if page_delivery.len() > 131_072 { return None; }
    control["page_delivery_capsule"] = json!({"version": 1,
        "sha256": hex_digest(&Sha256::digest(&page_delivery)), "bytes": page_delivery});
    control["items"] = json!(projected);''')
edit(p,'''        if call.operation == ProviderOperation::Replay {
            let request: Value''','''        if call.operation == ProviderOperation::Replay {
            let delivery = super::lifecycle::decode_receipt_capsule(&worker["page_delivery_capsule"])?;
            fields(&delivery, &["operation_id", "idempotency_key"])?;
            let original_key = string(&delivery["idempotency_key"])?;
            let original_operation = string(&delivery["operation_id"])?;
            if Some(original_key) != call.idempotency_key.as_deref() { return None; }
            if reply.terminal.committed_effect().state() == crate::CommittedEffectState::Duplicate {
                let effect = tracedecay_memory_provider_api::CommittedEffectEvidence::duplicate(
                    reply.state_generation, original_key, original_operation,
                    reply.terminal.committed_effect().provider_receipt_sha256()?,
                ).ok()?;
                reply.terminal = tracedecay_memory_provider_api::TerminalRecord::new(
                    call.operation, call.provider_id.clone(), reply.terminal.terminal_code(), effect,
                    reply.terminal.fallback().clone(), call.operation_id.clone(),
                    call.exact_scope.exact_scope_sha256(), reply.terminal.diagnostic_id().map(str::to_owned),
                ).ok()?;
            }
            output.remove("page_delivery_capsule");
            let request: Value''')
edit(r,'''            object.remove("idempotency_key");''','''            object.remove("idempotency_key");
            // Caller operation IDs do not change replay effects; the first retained page wins.
            object.remove("page_delivery_capsule");''')
edit(r,'''    items: Vec<ReplayItem>,
}''','''    items: Vec<ReplayItem>,
    page_delivery_capsule: Option<Value>,
}''')
edit(r,'''        let values = value["items"].as_array().ok_or_else(fail)?;''','''        let page_delivery_capsule = value.get("page_delivery_capsule").cloned();
        if let Some(capsule) = &page_delivery_capsule {
            let object = capsule.as_object().ok_or_else(fail)?;
            if object.len() != 3 || !["version", "bytes", "sha256"].iter().all(|key| object.contains_key(*key)) {
                return Err(fail());
            }
            super::util::validate_common_capsule(&json!({"common_capsule": capsule})).map_err(|_| fail())?;
        }
        let values = value["items"].as_array().ok_or_else(fail)?;''')
edit(r,'''            previous,
            items,
        })''','''            previous,
            items,
            page_delivery_capsule,
        })''')
edit(r,'''        json!({"common_portability":"replay","first_source_sequence":self.first,"last_source_sequence":self.last,"acknowledged_sequence":self.previous,"state_generation_before":self.expected,"state_generation_after":self.expected,"applied_observations":0,"duplicate_observations":0,"sources_already_applied":0,"rejected_observations":0,"effect_unknown_observations":0,"partial":false,"replayed":false,"warnings":[],"items":[]})''','''        let mut output = json!({"common_portability":"replay","first_source_sequence":self.first,"last_source_sequence":self.last,"acknowledged_sequence":self.previous,"state_generation_before":self.expected,"state_generation_after":self.expected,"applied_observations":0,"duplicate_observations":0,"sources_already_applied":0,"rejected_observations":0,"effect_unknown_observations":0,"partial":false,"replayed":false,"warnings":[],"items":[]});
        if let Some(capsule) = &self.page_delivery_capsule {
            output["page_delivery_capsule"] = capsule.clone();
        }
        output''')
edit(c,'''        let action = payload["action"].as_str().unwrap_or("");''','''        let action = payload["action"].as_str().unwrap_or("");
        if action == "maintenance" {
            return self.common_maintenance(namespace, payload, deadline);
        }''')
edit(c,'''            Ok(None) => {
''','''            Ok(None) => {
                if action == "inspection" && matches!(payload["view"].as_str(), Some("delivery_receipt" | "maintenance_receipt")) {
                    if payload["expected_generation"].as_u64() != Some(0) {
                        return EngineReply::rejected(RejectReason::IdempotencyConflict, 0);
                    }
                    return EngineReply::new(Outcome::Success, 0, json!({
                        "common_control": "inspection", "view": payload["view"], "items": [],
                        "partial": true, "cursor_after": null, "state_generation": 0, "scanned_items": 0
                    }));
                }
''')
edit(c,'''        if action == "inspection"
            && matches!(
                payload["view"].as_str(),
                Some("delivery_receipt" | "maintenance_receipt")
            )
        {
            return delivery_receipt(namespace, handle, payload);
        }''','''        if action == "inspection" && payload["view"] == "maintenance_receipt" {
            return super::common_maintenance::inspect_receipt(namespace, handle, payload);
        }
        if action == "inspection" && payload["view"] == "delivery_receipt" {
            return delivery_receipt(namespace, handle, payload, deadline, started);
        }''')
# Remove obsolete maintenance scan/presentation tail entirely.
s=(base/c).read_text();start=s.index('        let task = payload["task"].as_str().unwrap_or("");');end=s.index('\n    }\n}',start)
s=s[:start]+'''        EngineReply::rejected(RejectReason::InvalidRequest("unknown common read action".to_owned()), generation)'''+s[end:];(base/c).write_text(s)
print('Extended provider + portability drafts and common-read routing')
