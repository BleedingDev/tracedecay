//! Canonical portability over the private NCM snapshot and observation formats.

use super::*;
use std::collections::BTreeMap;
use tracedecay_memory_provider_api::{
    ProviderDescriptor, ProviderLimits, ProviderOperation, ReplayAccounting,
};

const MAX_ITEMS: usize = 4096;
const SNAPSHOT_FORMAT: &str = "ncm-snapshot.v1";

pub(crate) fn project(
    call: &ProviderCall,
    value: &Value,
    namespace: &NcmNamespace,
    admission: Option<&CurrentAdvisoryAdmission>,
    descriptor: &ProviderDescriptor,
    limits: ProviderLimits,
) -> Option<Option<Value>> {
    if !matches!(
        call.operation,
        ProviderOperation::SnapshotExport
            | ProviderOperation::SnapshotRestore
            | ProviderOperation::Replay
    ) || value.get("common_request").is_none()
    {
        return Some(None);
    }
    super::lifecycle::common_context(call, &value["common_request"])?;
    let mut control = json!({"action":call.operation.as_wire(),"expected_generation":call.expected_state_generation});
    match call.operation {
        ProviderOperation::SnapshotExport => fields(value, &["common_request"])?,
        ProviderOperation::SnapshotRestore => {
            fields(
                value,
                &[
                    "common_request",
                    "snapshot",
                    "disposition_checkpoint",
                    "source_dispositions",
                ],
            )?;
            let snapshot = &value["snapshot"];
            fields(snapshot, &["identity", "bytes", "sources"])?;
            let bytes = byte_array(&snapshot["bytes"], limits.snapshot_bytes)?;
            let inventory = snapshot_inventory(&bytes, namespace, &call.exact_scope)?;
            validate_snapshot_identity(
                call,
                &snapshot["identity"],
                descriptor,
                &bytes,
                &inventory,
            )?;
            let declared = snapshot["sources"].as_array()?;
            if declared.len() != inventory.sources.len()
                || canonical_sources(declared)? != canonical_sources(&inventory.wire_sources)?
            {
                return None;
            }
            let admitted = admission?;
            admitted.verify_for(call).ok()?;
            let current = admitted.restore.as_ref()?;
            current.verify_inventory(&inventory.sources).ok()?;
            for source in &inventory.attributions {
                if let OriginScopeEvidence::Recorded { scope, .. } = &source.origin_scope
                    && scope != &call.exact_scope
                    && !admitted
                        .history_sources
                        .iter()
                        .any(|entry| entry.attribution == *source)
                {
                    return None;
                }
            }
            validate_checkpoint(&value["disposition_checkpoint"], &call.exact_scope)?;
            let dispositions = value["source_dispositions"].as_array()?;
            let mut declared_dispositions = Vec::new();
            for item in dispositions {
                fields(item, &["source", "current_disposition"])?;
                validate_disposition(&item["current_disposition"])?;
                declared_dispositions.push(item["source"].clone());
            }
            if canonical_sources(&declared_dispositions)? != canonical_sources(declared)? {
                return None;
            }
            // The installed authority's fresh result determines which sources
            // are fenced. Wire dispositions only declare checked inventory.
            let mut blocked = BTreeSet::new();
            for (source, disposition) in &current.sources {
                match disposition.state.as_wire() {
                    "available" => {}
                    "superseded" | "revoked" => {
                        let effective = inventory.effective_validities.get(&source_key(source))?;
                        if (disposition.state.as_wire() == "superseded"
                            && effective.superseded_at_utc_nanos.is_none())
                            || (disposition.state.as_wire() == "revoked"
                                && effective.revoked_at_utc_nanos.is_none())
                        {
                            return None;
                        }
                    }
                    "deleted" | "redacted" | "expired" => {
                        blocked.insert(opaque_surface_id(
                            namespace,
                            b"forget-source-key",
                            &source.source_key,
                        ));
                    }
                    _ => return None,
                }
            }
            control["bytes"] = json!(bytes);
            control["blocked_sources"] = json!(blocked);
            control["snapshot_id"] = snapshot["identity"]["snapshot_id"].clone();
            control["observation_sequence"] = json!(inventory.observation_sequence);
        }
        ProviderOperation::Replay => {
            project_replay(call, value, namespace, admission?, &mut control)?
        }
        _ => return Some(None),
    }
    Some(Some(json!({"common_portability":control})))
}

fn project_replay(
    call: &ProviderCall,
    value: &Value,
    namespace: &NcmNamespace,
    admission: &CurrentAdvisoryAdmission,
    control: &mut Value,
) -> Option<()> {
    fields(
        value,
        &[
            "common_request",
            "observation_batch_refs",
            "first_source_sequence",
            "last_source_sequence",
            "expected_state_generation",
            "expected_previous_acknowledged_sequence",
            "history_grant",
            "resolved_observations",
        ],
    )?;
    admission.verify_for(call).ok()?;
    if value["expected_state_generation"].as_u64()? != call.expected_state_generation {
        return None;
    }
    let refs = strings(&value["observation_batch_refs"])?;
    let refs_set = refs.iter().collect::<BTreeSet<_>>();
    if refs.is_empty() || refs.len() > MAX_ITEMS || refs_set.len() != refs.len() {
        return None;
    }
    let items = value["resolved_observations"].as_array()?;
    let first = value["first_source_sequence"].as_u64()?;
    let last = value["last_source_sequence"].as_u64()?;
    if first == 0
        || items.is_empty()
        || items.len() > MAX_ITEMS
        || last.checked_sub(first)?.checked_add(1)? != items.len() as u64
    {
        return None;
    }
    let grant = &value["history_grant"];
    fields(
        grant,
        &[
            "authorization_ref",
            "policy_revision",
            "destination_scope",
            "relation",
            "sources",
            "disposition_checkpoint",
        ],
    )?;
    string(&grant["authorization_ref"])?;
    if grant["policy_revision"].as_u64()? == 0
        || scope(&grant["destination_scope"])? != call.exact_scope
    {
        return None;
    }
    let exact = match string(&grant["relation"])? {
        "exact_scope" => true,
        "same_checkout" => false,
        _ => return None,
    };
    validate_checkpoint(&grant["disposition_checkpoint"], &call.exact_scope)?;
    let claims = grant["sources"].as_array()?;
    if claims.is_empty() || claims.len() > MAX_ITEMS {
        return None;
    }
    let mut claim_sources = BTreeSet::new();
    for claim in claims {
        fields(claim, &["attribution", "current_disposition"])?;
        let source = attribution(&claim["attribution"])?;
        validate_disposition(&claim["current_disposition"])?;
        if !claim_sources.insert(source_key(&source.source)) {
            return None;
        }
    }
    let mut seen_refs = BTreeSet::new();
    let mut seen_sources = BTreeSet::new();
    let mut projected = Vec::new();
    for (index, item) in items.iter().enumerate() {
        fields(item, &["receipt_ref", "observation"])?;
        let receipt = string(&item["receipt_ref"])?;
        if !refs.iter().any(|candidate| candidate == receipt) || !seen_refs.insert(receipt) {
            return None;
        }
        let observation = &item["observation"];
        let original = observation.pointer("/source_identity/original_source")?;
        let source = attribution(original)?;
        if source.source_sequence != first.checked_add(index as u64)?
            || observation["source_sequence"].as_u64()? != source.source_sequence
            || !seen_sources.insert(source_key(&source.source))
        {
            return None;
        }
        let origin = source.origin_scope.recorded_scope().ok()?;
        if exact && origin != &call.exact_scope {
            return None;
        }
        if !claims.iter().any(|claim| claim["attribution"] == *original) {
            return None;
        }
        let current = admission
            .history_sources
            .iter()
            .find(|entry| entry.attribution == source)?;
        let state = current.current_disposition.state.as_wire();
        let blocked = matches!(state, "deleted" | "redacted" | "expired");
        let eligible = match state {
            "available" => true,
            "superseded" => source.validity.superseded_at_utc_nanos.is_some(),
            "revoked" => source.validity.revoked_at_utc_nanos.is_some(),
            "deleted" | "redacted" | "expired" | "unknown" => false,
            _ => return None,
        };
        let kind = string(&observation["observation_kind"])?;
        let (key_text, value_text) = evidence_text(kind, &observation["canonical_payload"])?;
        let source_id =
            opaque_surface_id(namespace, b"forget-source-key", &source.source.source_key);
        let delivery_key = opaque_surface_id(
            namespace,
            b"replay-delivery-key",
            string(&observation["idempotency_key"])?,
        );
        let provenance = if eligible {
            let mut provenance =
                project_attribution(call, observation, namespace, Some(admission))??;
            let delivery = serde_json::to_vec(&json!({"operation_id":call.operation_id,"idempotency_key":string(&observation["idempotency_key"])?})).ok()?;
            provenance["delivery_capsule"] = json!({"version":1,"sha256":hex_digest(&Sha256::digest(&delivery)),"bytes":delivery});
            provenance
        } else {
            Value::Null
        };
        projected.push(json!({"source_sequence":source.source_sequence,
            "receipt_digest":opaque_surface_id(namespace,b"replay-receipt",receipt),
            "delivery_key":delivery_key,"source":source_id,"admitted":eligible,"blocked":blocked,
            "observation":if eligible { json!({"observation_kind":kind,"payload_contract":observation["payload_contract"],
                "canonical_payload":{"forget_source_key":source_id,"_ncm_key_text":key_text,"_ncm_value_text":value_text},"provenance":provenance}) } else { Value::Null }}));
    }
    if seen_refs.len() != refs.len()
        || seen_sources != claim_sources
        || admission.history_sources.len() != seen_sources.len()
    {
        return None;
    }
    let page_delivery = serde_json::to_vec(&json!({
        "operation_id": call.operation_id,
        "idempotency_key": call.idempotency_key.as_deref()?
    }))
    .ok()?;
    if page_delivery.len() > 131_072 {
        return None;
    }
    control["page_delivery_capsule"] = json!({"version": 1,
        "sha256": hex_digest(&Sha256::digest(&page_delivery)), "bytes": page_delivery});
    control["items"] = json!(projected);
    for name in [
        "first_source_sequence",
        "last_source_sequence",
        "expected_previous_acknowledged_sequence",
    ] {
        control[name] = json!(value[name].as_u64()?);
    }
    Some(())
}

struct Inventory {
    generation: u64,
    observation_sequence: u64,
    sources: Vec<OriginalSourceIdentity>,
    attributions: Vec<SourceAttribution>,
    effective_validities: BTreeMap<String, RecordedValidity>,
    wire_sources: Vec<Value>,
}

fn snapshot_inventory(
    bytes: &[u8],
    namespace: &NcmNamespace,
    exact: &OwnedExactScope,
) -> Option<Inventory> {
    let snapshot: Value = serde_json::from_slice(bytes).ok()?;
    if snapshot["format"] != SNAPSHOT_FORMAT || snapshot["namespace"] != namespace.as_str() {
        return None;
    }
    let capsules = snapshot["capsules"].as_array()?;
    if capsules.len() > MAX_ITEMS {
        return None;
    }
    let mut inventory = Inventory {
        generation: snapshot["commit_seq"].as_u64()?,
        observation_sequence: 0,
        sources: Vec::new(),
        attributions: Vec::new(),
        effective_validities: BTreeMap::new(),
        wire_sources: Vec::new(),
    };
    let mut seen = BTreeSet::new();
    for capsule in capsules {
        if !matches!(capsule["status"].as_str()?, "valid" | "superseded") {
            return None;
        }
        let provenance: Value = serde_json::from_str(capsule["provenance"].as_str()?).ok()?;
        let retained = decode_capsule(&provenance)?;
        fields(
            &retained,
            &[
                "original_source",
                "canonical_payload",
                "source_refs",
                "delivery_scope",
                "projection",
            ],
        )?;
        let source = attribution(&retained["original_source"])?;
        if scope(&retained["delivery_scope"])? != *exact
            || !seen.insert(source_key(&source.source))
            || capsule["source_id"]
                != opaque_surface_id(namespace, b"forget-source-key", &source.source.source_key)
        {
            return None;
        }
        let (key, value) = evidence_text(
            string(&retained["projection"]["observation_kind"])?,
            &retained["canonical_payload"],
        )?;
        if capsule["key_text"] != key
            || capsule["value_text"] != value
            || retained["projection"]["key_text"] != key
            || retained["projection"]["value_text"] != value
        {
            return None;
        }
        let mut effective = retained["original_source"]["validity"].clone();
        if let Some(patch) = provenance.pointer("/control/validity_patch") {
            for (field, value) in patch.as_object()? {
                effective[field] = value.clone();
            }
        }
        inventory
            .effective_validities
            .insert(source_key(&source.source), validity(&effective)?);
        inventory.observation_sequence = inventory.observation_sequence.max(source.source_sequence);
        inventory
            .wire_sources
            .push(retained["original_source"]["source"].clone());
        inventory.sources.push(source.source.clone());
        inventory.attributions.push(source);
    }
    Some(inventory)
}

fn source_key(source: &OriginalSourceIdentity) -> String {
    json!([
        source.canonical_provider_id.as_str(),
        source.canonical_session_id,
        source.source_key,
        source.observation_id,
        source.source_revision
    ])
    .to_string()
}

fn canonical_sources(values: &[Value]) -> Option<BTreeMap<String, Value>> {
    let mut sources = BTreeMap::new();
    for value in values {
        fields(
            value,
            &[
                "canonical_provider_id",
                "canonical_session_id",
                "source_key",
                "stable_record_id",
                "observation_id",
                "source_revision",
                "content_sha256",
            ],
        )?;
        let key = json!([
            string(&value["canonical_provider_id"])?,
            string(&value["canonical_session_id"])?,
            string(&value["source_key"])?,
            string(&value["observation_id"])?,
            nullable_string(&value["source_revision"])?
        ])
        .to_string();
        if sources.insert(key, value.clone()).is_some() {
            return None;
        }
    }
    Some(sources)
}

fn validate_snapshot_identity(
    call: &ProviderCall,
    identity: &Value,
    descriptor: &ProviderDescriptor,
    bytes: &[u8],
    inventory: &Inventory,
) -> Option<()> {
    fields(
        identity,
        &[
            "snapshot_id",
            "provider_id",
            "implementation_identity_digest",
            "state_schema_version",
            "exact_scope_digest",
            "state_generation",
            "observation_sequence",
            "parent_snapshot_id",
            "content_sha256",
            "byte_length",
            "created_at",
        ],
    )?;
    let digest = hex_digest(&Sha256::digest(bytes));
    if identity["snapshot_id"] != format!("ncm-snapshot:{digest}")
        || identity["provider_id"] != call.provider_id.as_str()
        || identity["implementation_identity_digest"] != descriptor.implementation_identity_sha256
        || identity["state_schema_version"] != descriptor.state_schema_version
        || identity["exact_scope_digest"] != call.exact_scope.exact_scope_sha256()
        || identity["state_generation"].as_u64()? != inventory.generation
        || identity["observation_sequence"].as_u64()? != inventory.observation_sequence
        || identity["content_sha256"] != digest
        || identity["byte_length"].as_u64()? != bytes.len() as u64
    {
        return None;
    }
    nullable_string(&identity["parent_snapshot_id"])?;
    instant(&identity["created_at"])?;
    Some(())
}

fn validate_checkpoint(value: &Value, exact: &OwnedExactScope) -> Option<()> {
    fields(
        value,
        &[
            "exact_scope",
            "authority_ref",
            "authority_revision",
            "checked_at",
        ],
    )?;
    if scope(&value["exact_scope"])? != *exact {
        return None;
    }
    string(&value["authority_ref"])?;
    if !value["authority_revision"].is_null() {
        value["authority_revision"].as_u64()?;
    }
    instant(&value["checked_at"])?;
    Some(())
}

fn validate_disposition(value: &Value) -> Option<()> {
    fields(
        value,
        &["state", "authority_ref", "authority_revision", "checked_at"],
    )?;
    if !matches!(
        string(&value["state"])?,
        "available" | "superseded" | "revoked" | "deleted" | "redacted" | "expired"
    ) {
        return None;
    }
    string(&value["authority_ref"])?;
    if !value["authority_revision"].is_null() {
        value["authority_revision"].as_u64()?;
    }
    instant(&value["checked_at"])?;
    Some(())
}

fn byte_array(value: &Value, limit: u64) -> Option<Vec<u8>> {
    let array = value.as_array()?;
    if array.is_empty() || array.len() as u64 > limit {
        return None;
    }
    array
        .iter()
        .map(|value| u8::try_from(value.as_u64()?).ok())
        .collect()
}

pub(crate) fn reconstruct(
    call: &ProviderCall,
    reply: &mut ProviderReply,
    descriptor: &ProviderDescriptor,
    limits: ProviderLimits,
) -> Option<()> {
    let worker: Value = serde_json::from_slice(&reply.payload.as_ref()?.bytes).ok()?;
    if worker["common_portability"] != call.operation.as_wire() {
        return None;
    }
    let mut output = if call.operation == ProviderOperation::SnapshotExport {
        let bytes = byte_array(&worker["bytes"], limits.snapshot_bytes)?;
        let inventory = snapshot_inventory(
            &bytes,
            &NcmNamespace::from_exact_scope(&call.exact_scope),
            &call.exact_scope,
        )?;
        if inventory.generation != reply.state_generation {
            return None;
        }
        let digest = hex_digest(&Sha256::digest(&bytes));
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?;
        let created_at =
            DateTime::from_timestamp(i64::try_from(now.as_secs()).ok()?, now.subsec_nanos())?
                .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        json!({"snapshot":{"identity":{"snapshot_id":format!("ncm-snapshot:{digest}"),"provider_id":call.provider_id.as_str(),
            "implementation_identity_digest":descriptor.implementation_identity_sha256,"state_schema_version":descriptor.state_schema_version,
            "exact_scope_digest":call.exact_scope.exact_scope_sha256(),"state_generation":inventory.generation,"observation_sequence":inventory.observation_sequence,
            "parent_snapshot_id":null,"content_sha256":digest,"byte_length":bytes.len(),"created_at":created_at},"bytes":bytes,"sources":inventory.wire_sources},"warnings":[]})
    } else {
        let mut output = worker.as_object()?.clone();
        output.remove("common_portability");
        output.remove("replayed");
        output.remove("_retained_receipt");
        if call.operation == ProviderOperation::Replay {
            let delivery =
                super::lifecycle::decode_receipt_capsule(&worker["page_delivery_capsule"])?;
            fields(&delivery, &["operation_id", "idempotency_key"])?;
            let original_key = string(&delivery["idempotency_key"])?;
            let original_operation = string(&delivery["operation_id"])?;
            if Some(original_key) != call.idempotency_key.as_deref() {
                return None;
            }
            if reply.terminal.committed_effect().state() == crate::CommittedEffectState::Duplicate {
                let effect = tracedecay_memory_provider_api::CommittedEffectEvidence::duplicate(
                    reply.state_generation,
                    original_key,
                    original_operation,
                    reply
                        .terminal
                        .committed_effect()
                        .provider_receipt_sha256()?,
                )
                .ok()?;
                reply.terminal = tracedecay_memory_provider_api::TerminalRecord::new(
                    call.operation,
                    call.provider_id.clone(),
                    reply.terminal.terminal_code(),
                    effect,
                    reply.terminal.fallback().clone(),
                    call.operation_id.clone(),
                    call.exact_scope.exact_scope_sha256(),
                    reply.terminal.diagnostic_id().map(str::to_owned),
                )
                .ok()?;
            }
            output.remove("page_delivery_capsule");
            let request: Value = serde_json::from_slice(&call.payload.bytes).ok()?;
            let mut accounting = ReplayAccounting {
                attempted: request["resolved_observations"].as_array()?.len() as u64,
                applied: worker["applied_observations"].as_u64()?,
                delivery_duplicates: worker["duplicate_observations"].as_u64()?,
                sources_already_applied: worker["sources_already_applied"].as_u64()?,
                rejected: worker["rejected_observations"].as_u64()?,
                effect_unknown: worker["effect_unknown_observations"].as_u64()?,
            };
            accounting.validate().ok()?;
            if worker["replayed"] == true {
                accounting.delivery_duplicates = accounting
                    .delivery_duplicates
                    .checked_add(accounting.applied)?
                    .checked_add(accounting.sources_already_applied)?;
                output.insert(
                    "duplicate_observations".to_owned(),
                    json!(accounting.delivery_duplicates),
                );
                output.insert("applied_observations".to_owned(), json!(0));
                output.insert("sources_already_applied".to_owned(), json!(0));
                for item in output.get_mut("items")?.as_array_mut()? {
                    if matches!(
                        item["state"].as_str()?,
                        "applied" | "source_already_applied"
                    ) {
                        item["state"] = json!("delivery_duplicate");
                    }
                }
            }
        }
        Value::Object(output)
    };
    if call.operation != ProviderOperation::SnapshotExport {
        if let Some(receipt) = reply.terminal.committed_effect().provider_receipt_sha256() {
            output["provider_receipt_digest"] = json!(receipt);
        } else if reply.terminal.terminal_code() == TerminalCode::Success {
            return None;
        }
    }
    let bytes = serde_json::to_vec(&output).ok()?;
    if bytes.len() as u64 > limits.response_bytes {
        return None;
    }
    reply.payload = Some(
        CanonicalPayload::new(
            call.payload.contract_id.clone(),
            bytes.clone(),
            hex_digest(&Sha256::digest(&bytes)),
        )
        .ok()?,
    );
    Some(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn canonical_payload_digest(value: &Value) -> String {
        let mut canonical = value.clone();
        canonical.sort_all_objects();
        hex_digest(&Sha256::digest(serde_json::to_vec(&canonical).unwrap()))
    }

    fn fixture() -> (OwnedExactScope, NcmNamespace, Value, Value) {
        let exact = OwnedExactScope {
            profile_id: "profile".to_owned(),
            project_id: "project".to_owned(),
            repository_identity: "repository".to_owned(),
            worktree_identity: "worktree".to_owned(),
            branch_identity: "master".to_owned(),
            agent_session_id: "session".to_owned(),
            resolved_scope_digest: format!("sha256:{}", "ab".repeat(32)),
        };
        let namespace = NcmNamespace::from_exact_scope(&exact);
        let payload = json!({"role":"assistant","content":"retained answer"});
        let source = json!({"canonical_provider_id":"claude","canonical_session_id":"source-session","source_key":"source-key","stable_record_id":null,
            "observation_id":"observation-1","source_revision":"revision-1","content_sha256":canonical_payload_digest(&payload)});
        let original = json!({"source":source,"origin_scope":{"state":"recorded","exact_scope_identity":scope_value(&exact),"authority_ref":"source.authority"},
            "source_sequence":1,"occurred_at":"2026-01-01T00:00:00Z","ingested_at":"2026-01-01T00:00:00Z",
            "validity":{"valid_from":"2026-01-01T00:00:00Z","valid_until":null,"superseded_at":null,"superseded_by":null,"revoked_at":null}});
        let retained = json!({"original_source":original,"canonical_payload":payload,"source_refs":["record:observation-1"],"delivery_scope":scope_value(&exact),
            "projection":{"key_text":"retained answer","value_text":"assistant: retained answer","observation_kind":"session.message_committed.v1"}});
        let capsule_bytes = serde_json::to_vec(&retained).unwrap();
        let provenance = json!({"common_capsule":{"version":1,"sha256":hex_digest(&Sha256::digest(&capsule_bytes)),"bytes":capsule_bytes}});
        let snapshot = json!({"format":SNAPSHOT_FORMAT,"namespace":namespace.as_str(),"commit_seq":1,"capsules":[{"record_id":1,"source_id":opaque_surface_id(&namespace,b"forget-source-key","source-key"),
            "key_text":"retained answer","value_text":"assistant: retained answer","status":"valid","provenance":provenance.to_string()}]});
        (exact, namespace, snapshot, retained)
    }

    #[test]
    fn inventory_requires_unique_actual_sources_and_exact_declared_identities() {
        let (exact, namespace, mut snapshot, _) = fixture();
        let actual =
            snapshot_inventory(&serde_json::to_vec(&snapshot).unwrap(), &namespace, &exact)
                .unwrap();
        assert_eq!(actual.sources.len(), 1);
        let canonical = canonical_sources(&actual.wire_sources).unwrap();
        let mut forged = actual.wire_sources[0].clone();
        forged["content_sha256"] = json!("ff".repeat(32));
        assert_ne!(canonical_sources(&[forged]).unwrap(), canonical);
        assert_ne!(canonical_sources(&[]).unwrap(), canonical);
        assert!(
            canonical_sources(&[
                actual.wire_sources[0].clone(),
                actual.wire_sources[0].clone()
            ])
            .is_none()
        );
        let duplicate = snapshot["capsules"][0].clone();
        snapshot["capsules"].as_array_mut().unwrap().push(duplicate);
        assert!(
            snapshot_inventory(&serde_json::to_vec(&snapshot).unwrap(), &namespace, &exact)
                .is_none()
        );
    }

    #[test]
    fn snapshot_inventory_keeps_effective_corrections_separate_from_original_validity() {
        for patch in [
            json!({"superseded_at":"2026-01-02T00:00:00Z","superseded_by":"ncm-memory:replacement"}),
            json!({"revoked_at":"2026-01-02T00:00:00Z"}),
        ] {
            let (exact, namespace, mut snapshot, retained) = fixture();
            let mut provenance: Value =
                serde_json::from_str(snapshot["capsules"][0]["provenance"].as_str().unwrap())
                    .unwrap();
            provenance["control"] = json!({"validity_patch":patch});
            snapshot["capsules"][0]["provenance"] = json!(provenance.to_string());
            let inventory =
                snapshot_inventory(&serde_json::to_vec(&snapshot).unwrap(), &namespace, &exact)
                    .unwrap();
            assert_eq!(
                inventory.attributions[0],
                attribution(&retained["original_source"]).unwrap()
            );
            let effective = &inventory.effective_validities[&source_key(&inventory.sources[0])];
            assert_eq!(
                effective.superseded_at_utc_nanos.is_some(),
                patch.get("superseded_at").is_some()
            );
            assert_eq!(
                effective.revoked_at_utc_nanos.is_some(),
                patch.get("revoked_at").is_some()
            );
            assert!(
                inventory.attributions[0]
                    .validity
                    .superseded_at_utc_nanos
                    .is_none()
            );
            assert!(
                inventory.attributions[0]
                    .validity
                    .revoked_at_utc_nanos
                    .is_none()
            );
        }
        let (exact, namespace, mut snapshot, _) = fixture();
        let mut provenance: Value =
            serde_json::from_str(snapshot["capsules"][0]["provenance"].as_str().unwrap()).unwrap();
        provenance["control"] = json!({"validity_patch":{"revoked_at":"invalid-time"}});
        snapshot["capsules"][0]["provenance"] = json!(provenance.to_string());
        assert!(
            snapshot_inventory(&serde_json::to_vec(&snapshot).unwrap(), &namespace, &exact)
                .is_none()
        );
    }

    #[test]
    fn changed_capsule_bytes_require_their_matching_integrity_digest() {
        let (exact, namespace, mut snapshot, mut retained) = fixture();
        let unchanged = serde_json::to_vec(&retained).unwrap();
        retained["canonical_payload"]["content"] = json!("forged answer");
        retained["projection"]["key_text"] = json!("forged answer");
        retained["projection"]["value_text"] = json!("assistant: forged answer");
        let changed = serde_json::to_vec(&retained).unwrap();
        let provenance = json!({"common_capsule":{"version":1,"sha256":hex_digest(&Sha256::digest(&unchanged)),"bytes":changed}});
        snapshot["capsules"][0]["provenance"] = json!(provenance.to_string());
        snapshot["capsules"][0]["key_text"] = json!("forged answer");
        snapshot["capsules"][0]["value_text"] = json!("assistant: forged answer");
        assert!(
            snapshot_inventory(&serde_json::to_vec(&snapshot).unwrap(), &namespace, &exact)
                .is_none()
        );
    }

    #[test]
    fn sanitized_delivery_preserves_the_original_host_source_digest() {
        let (exact, namespace, mut snapshot, mut retained) = fixture();
        let original_digest = canonical_payload_digest(
            &json!({"role":"assistant","content":"retained answer with removed secret"}),
        );
        retained["original_source"]["source"]["content_sha256"] = json!(original_digest);
        let bytes = serde_json::to_vec(&retained).unwrap();
        snapshot["capsules"][0]["provenance"]=json!(json!({"common_capsule":{"version":1,"sha256":hex_digest(&Sha256::digest(&bytes)),"bytes":bytes}}).to_string());
        let inventory =
            snapshot_inventory(&serde_json::to_vec(&snapshot).unwrap(), &namespace, &exact)
                .unwrap();
        assert_eq!(inventory.sources[0].content_sha256, original_digest);
    }
}
