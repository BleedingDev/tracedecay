//! Canonical host evidence projection. The worker retains opaque capsules;
//! only this adapter reconstructs host-facing source and scope claims.

use std::collections::BTreeSet;

use chrono::DateTime;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tracedecay_memory_provider_api::contract::{TemporalMode, TerminalCode, UnknownValidityPolicy};
use tracedecay_memory_provider_api::{
    CanonicalPayload, CurrentAdvisoryAdmission, OwnedExactScope, OwnedProviderId, ProviderCall,
    ProviderReply,
};
use tracedecay_memory_provider_api::{
    OriginScopeEvidence, OriginalSourceIdentity, OwnedRecallExclusions, OwnedTemporalQuery,
    RecordedValidity, SourceAttribution,
};

use crate::{NcmNamespace, hex_digest, opaque_surface_id};

mod lifecycle;
mod portability;
mod replay_boundary;
mod source_binding;
pub(crate) use lifecycle::{project as project_lifecycle, reconstruct as reconstruct_lifecycle};
pub(crate) use portability::{
    project as project_portability, reconstruct as reconstruct_portability,
};
pub(crate) use replay_boundary::valid_replay_payload;
pub(crate) use source_binding::project_source_id;
use source_binding::{source_binding, validate_source_binding};

const BUDGETS: &[&str] = &[
    "maximum_candidates",
    "maximum_candidate_content_bytes",
    "maximum_total_content_bytes",
    "maximum_source_refs_per_candidate",
    "maximum_trace_refs_per_candidate",
    "maximum_warnings",
    "maximum_extensions_per_candidate",
];
const EXCLUSIONS: &[&str] = &[
    "stable_memory_refs",
    "candidate_ids",
    "source_refs",
    "trace_refs",
    "observation_ids",
    "content_sha256",
];

fn fields(value: &Value, names: &[&str]) -> Option<()> {
    let object = value.as_object()?;
    (object.len() == names.len() && names.iter().all(|name| object.contains_key(*name)))
        .then_some(())
}

fn string(value: &Value) -> Option<&str> {
    value
        .as_str()
        .filter(|text| !text.trim().is_empty() && !text.chars().any(char::is_control))
}

fn nullable_string(value: &Value) -> Option<Option<String>> {
    if value.is_null() {
        Some(None)
    } else {
        Some(Some(string(value)?.to_owned()))
    }
}

fn instant(value: &Value) -> Option<i64> {
    let text = value.as_str()?;
    if !text.ends_with('Z') && !text.ends_with("+00:00") {
        return None;
    }
    let time = DateTime::parse_from_rfc3339(text).ok()?;
    time.timestamp_nanos_opt()
}

fn nullable_instant(value: &Value) -> Option<Option<i64>> {
    if value.is_null() {
        Some(None)
    } else {
        Some(Some(instant(value)?))
    }
}

fn scope(value: &Value) -> Option<OwnedExactScope> {
    fields(
        value,
        &[
            "profile_id",
            "project_id",
            "repository_identity",
            "worktree_identity",
            "branch_identity",
            "agent_session_id",
            "resolved_scope_digest",
        ],
    )?;
    let scope = OwnedExactScope {
        profile_id: string(&value["profile_id"])?.to_owned(),
        project_id: string(&value["project_id"])?.to_owned(),
        repository_identity: string(&value["repository_identity"])?.to_owned(),
        worktree_identity: string(&value["worktree_identity"])?.to_owned(),
        branch_identity: string(&value["branch_identity"])?.to_owned(),
        agent_session_id: string(&value["agent_session_id"])?.to_owned(),
        resolved_scope_digest: string(&value["resolved_scope_digest"])?.to_owned(),
    };
    scope.validate().ok()?;
    Some(scope)
}

fn validity(value: &Value) -> Option<RecordedValidity> {
    fields(
        value,
        &[
            "valid_from",
            "valid_until",
            "superseded_at",
            "superseded_by",
            "revoked_at",
        ],
    )?;
    let validity = RecordedValidity {
        valid_from_utc_nanos: nullable_instant(&value["valid_from"])?,
        valid_until_utc_nanos: nullable_instant(&value["valid_until"])?,
        superseded_at_utc_nanos: nullable_instant(&value["superseded_at"])?,
        superseded_by: nullable_string(&value["superseded_by"])?,
        revoked_at_utc_nanos: nullable_instant(&value["revoked_at"])?,
    };
    validity.validate().ok()?;
    Some(validity)
}

fn attribution(value: &Value) -> Option<SourceAttribution> {
    fields(
        value,
        &[
            "source",
            "origin_scope",
            "source_sequence",
            "occurred_at",
            "ingested_at",
            "validity",
        ],
    )?;
    let source = &value["source"];
    fields(
        source,
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
    let original = OriginalSourceIdentity {
        canonical_provider_id: OwnedProviderId::new(string(&source["canonical_provider_id"])?)
            .ok()?,
        canonical_session_id: string(&source["canonical_session_id"])?.to_owned(),
        source_key: string(&source["source_key"])?.to_owned(),
        stable_record_id: nullable_string(&source["stable_record_id"])?,
        observation_id: string(&source["observation_id"])?.to_owned(),
        source_revision: nullable_string(&source["source_revision"])?,
        content_sha256: string(&source["content_sha256"])?.to_owned(),
    };
    let origin = &value["origin_scope"];
    let origin_scope = match origin["state"].as_str()? {
        "recorded" => {
            fields(origin, &["state", "exact_scope_identity", "authority_ref"])?;
            OriginScopeEvidence::Recorded {
                scope: scope(&origin["exact_scope_identity"])?,
                authority_ref: string(&origin["authority_ref"])?.to_owned(),
            }
        }
        "ingestion_only" => {
            fields(origin, &["state"])?;
            OriginScopeEvidence::IngestionOnly
        }
        "unavailable" => {
            fields(origin, &["state"])?;
            OriginScopeEvidence::Unavailable
        }
        _ => return None,
    };
    let result = SourceAttribution {
        source: original,
        origin_scope,
        source_sequence: value["source_sequence"].as_u64()?,
        occurred_at_utc_nanos: nullable_instant(&value["occurred_at"])?,
        ingested_at_utc_nanos: instant(&value["ingested_at"])?,
        validity: validity(&value["validity"])?,
    };
    result.validate().ok()?;
    Some(result)
}

fn temporal(value: &Value) -> Option<OwnedTemporalQuery> {
    fields(
        value,
        &[
            "mode",
            "evaluation_time",
            "as_of",
            "interval_start",
            "interval_end",
            "include_superseded",
            "include_revoked",
            "unknown_validity_policy",
        ],
    )?;
    let result = OwnedTemporalQuery {
        mode: match value["mode"].as_str()? {
            "current" => TemporalMode::Current,
            "as_of" => TemporalMode::AsOf,
            "interval" => TemporalMode::Interval,
            "history" => TemporalMode::History,
            _ => return None,
        },
        evaluation_time_utc_nanos: instant(&value["evaluation_time"])?,
        as_of_utc_nanos: nullable_instant(&value["as_of"])?,
        interval_start_utc_nanos: nullable_instant(&value["interval_start"])?,
        interval_end_utc_nanos: nullable_instant(&value["interval_end"])?,
        include_superseded: value["include_superseded"].as_bool()?,
        include_revoked: value["include_revoked"].as_bool()?,
        unknown_validity_policy: match value["unknown_validity_policy"].as_str()? {
            "exclude" => UnknownValidityPolicy::Exclude,
            "degrade" => UnknownValidityPolicy::Degrade,
            "allow_with_warning" => UnknownValidityPolicy::AllowWithWarning,
            _ => return None,
        },
    };
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_nanos(),
    )
    .ok()?;
    result.validate_at(now).ok()?;
    Some(result)
}

fn strings(value: &Value) -> Option<Vec<String>> {
    value
        .as_array()?
        .iter()
        .map(|item| Some(string(item)?.to_owned()))
        .collect()
}

pub(crate) fn project_recall(
    call: &ProviderCall,
    value: &Value,
    namespace: &NcmNamespace,
) -> Option<Value> {
    let names = [
        "provider_id",
        "registration_revision",
        "ready_receipt_digest",
        "exact_scope_identity",
        "request_identity",
        "objective",
        "query",
        "temporal_query",
        "budgets",
        "exclusions",
        "required_capabilities",
        "policy_revision",
        "extensions",
        "deadline",
        "cancellation",
    ];
    let object = value.as_object()?;
    if object
        .keys()
        .any(|name| !names.contains(&name.as_str()) && name != "history_grant")
        || names.iter().any(|name| !object.contains_key(*name))
    {
        return None;
    }
    if value["provider_id"].as_str()? != call.provider_id.as_str()
        || value["registration_revision"].as_u64()? != call.registration_revision
        || value["ready_receipt_digest"].as_str()? != call.ready_receipt_sha256
        || value["request_identity"].as_str()? != call.request_id
        || scope(&value["exact_scope_identity"])? != call.exact_scope
        || string(&value["objective"])?.len() > 8192
        || string(&value["query"])?.len() > 32768
        || value["policy_revision"].as_u64()? == 0
        || value["cancellation"] != "live"
    {
        return None;
    }
    let capabilities = strings(&value["required_capabilities"])?;
    let expected: BTreeSet<_> = call
        .required_capabilities
        .iter()
        .map(|item| item.as_str())
        .collect();
    if capabilities
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>()
        != expected
        || capabilities.len() != expected.len()
    {
        return None;
    }
    fields(
        &value["deadline"],
        &["deadline_utc_micros", "remaining_millis"],
    )?;
    value["deadline"]["deadline_utc_micros"].as_i64()?;
    if value["deadline"]["remaining_millis"].as_u64()? == 0 {
        return None;
    }
    // Optional extensions remain inert; the outer adapter round-trips them.
    let extensions = value["extensions"].as_array()?;
    if extensions.len() > 16
        || extensions
            .iter()
            .any(|item| item["criticality"] != "optional")
    {
        return None;
    }
    let query = temporal(&value["temporal_query"])?;
    fields(&value["budgets"], BUDGETS)?;
    for field in BUDGETS {
        if value["budgets"][*field].as_u64()? == 0 {
            return None;
        }
    }
    fields(&value["exclusions"], EXCLUSIONS)?;
    let exclusions = OwnedRecallExclusions {
        stable_memory_refs: strings(&value["exclusions"]["stable_memory_refs"])?,
        candidate_ids: strings(&value["exclusions"]["candidate_ids"])?,
        source_refs: strings(&value["exclusions"]["source_refs"])?,
        trace_refs: strings(&value["exclusions"]["trace_refs"])?,
        observation_ids: strings(&value["exclusions"]["observation_ids"])?,
        content_sha256: strings(&value["exclusions"]["content_sha256"])?,
    };
    exclusions.validate().ok()?;
    let mut projected_exclusions = json!({});
    for name in EXCLUSIONS {
        let values = strings(&value["exclusions"][*name])?;
        projected_exclusions[*name] = json!(
            values
                .iter()
                .map(|item| opaque_surface_id(namespace, name.as_bytes(), item))
                .collect::<Vec<_>>()
        );
    }
    Some(json!({
        "query_text": value["query"], "top_k": 16,
        "selection": {
            "mode": value["temporal_query"]["mode"], "evaluation": query.evaluation_time_utc_nanos,
            "as_of": query.as_of_utc_nanos, "start": query.interval_start_utc_nanos, "end": query.interval_end_utc_nanos,
            "include_superseded": query.include_superseded, "include_revoked": query.include_revoked,
            "unknown_policy": value["temporal_query"]["unknown_validity_policy"], "exclusions": projected_exclusions,
            "maximum_candidates": value["budgets"]["maximum_candidates"].as_u64()?.min(16),
            "request_token": opaque_surface_id(namespace, b"recall-request", &call.request_id),
        }
    }))
}

pub(crate) fn project_attribution(
    call: &ProviderCall,
    value: &Value,
    namespace: &NcmNamespace,
    admission: Option<&CurrentAdvisoryAdmission>,
) -> Option<Option<Value>> {
    let Some(original) = value.pointer("/source_identity/original_source") else {
        return Some(None);
    };
    let admitted = attribution(original)?;
    if let OriginScopeEvidence::Recorded { scope, .. } = &admitted.origin_scope {
        // A claim in history_grant is not host authorization. Cross-session
        // records arrive through the explicit replay admission path.
        if scope != &call.exact_scope && !history_source_admitted(&admitted, admission) {
            return None;
        }
    }
    let canonical = value.get("canonical_payload")?;
    let source_ref = format!(
        "record:{}",
        admitted
            .source
            .stable_record_id
            .as_deref()
            .unwrap_or(&admitted.source.observation_id)
    );
    let kind = value["observation_kind"].as_str()?;
    let (key_text, value_text) = evidence_text(kind, canonical)?;
    let retained = json!({"original_source": original, "canonical_payload": canonical, "source_refs": [source_ref], "delivery_scope": scope_value(&call.exact_scope),
        "projection": {"key_text": key_text, "value_text": value_text, "observation_kind": kind}});
    let bytes = serde_json::to_vec(&retained).ok()?;
    let digest = hex_digest(&Sha256::digest(&bytes));
    let v = &admitted.validity;
    let delivery_bytes = serde_json::to_vec(
        &json!({"operation_id": call.operation_id, "idempotency_key": call.idempotency_key}),
    )
    .ok()?;
    let delivery_digest = hex_digest(&Sha256::digest(&delivery_bytes));
    Some(Some(json!({
        "source_binding": source_binding(namespace, &admitted)?.provenance(),
        "common_capsule": {"version": 1, "bytes": bytes, "sha256": digest},
        "delivery_capsule": {"version": 1, "bytes": delivery_bytes, "sha256": delivery_digest},
        "selection": {
            "valid_from": v.valid_from_utc_nanos, "valid_until": v.valid_until_utc_nanos,
            "superseded_at": v.superseded_at_utc_nanos,
            "superseded_by": v.superseded_by.as_ref().map(|item| opaque_surface_id(namespace, b"stable_memory_refs", item)),
            "revoked_at": v.revoked_at_utc_nanos,
            "source_refs": [opaque_surface_id(namespace, b"source_refs", &source_ref)],
            "observation_ids": [opaque_surface_id(namespace, b"observation_ids", &admitted.source.observation_id)],
            "unknown_revision": admitted.source.source_revision.is_none(),
            "source_identity_sha256": opaque_surface_id(namespace, b"source-target", &serde_json::to_string(&json!({"source": original["source"], "origin_scope": original["origin_scope"]})).ok()?),
            "revision_digest": admitted.source.source_revision.as_ref().map(|revision| opaque_surface_id(namespace, b"source-revision", revision)),
            "observation_identity": opaque_surface_id(namespace, b"observation-identity", &serde_json::to_string(&json!([admitted.source.canonical_provider_id.as_str(), admitted.source.canonical_session_id, admitted.source.observation_id])).ok()?)
        }
    })))
}

fn history_source_admitted(
    source: &SourceAttribution,
    admission: Option<&CurrentAdvisoryAdmission>,
) -> bool {
    admission.is_some_and(|admission| {
        admission.history_sources.iter().any(|entry| {
            entry.attribution == *source
                && matches!(
                    entry.current_disposition.state,
                    tracedecay_memory_provider_api::contract::SourceDisposition::Available
                        | tracedecay_memory_provider_api::contract::SourceDisposition::Superseded
                        | tracedecay_memory_provider_api::contract::SourceDisposition::Revoked
                )
        })
    })
}

pub(crate) fn evidence_text(kind: &str, canonical: &Value) -> Option<(String, String)> {
    let payload = canonical.get("payload").unwrap_or(canonical);
    if kind == "session.message_committed.v1" {
        if let Some(facts) = canonical["facts"].as_array() {
            let mut roles = Vec::new();
            let mut texts = Vec::new();
            for fact in facts {
                if fact["kind"] != "message" {
                    continue;
                }
                roles.push(string(&fact["role"])?.to_owned());
                texts.push(crate::message_fact_text(&fact["content"])?);
            }
            if texts.is_empty() {
                return None;
            }
            let content = texts.join("\n");
            return Some((content.clone(), format!("{}: {content}", roles.join("\n"))));
        }
        let content = crate::message_fact_text(&payload["content"])?;
        let role = string(&payload["role"])?;
        return Some((content.clone(), format!("{role}: {content}")));
    }
    let names: &[&str] = match kind {
        "source.edit_settled.v1" => &["claim", "status", "assertion", "reason"],
        "test.execution_settled.v1" => &["assertion", "status", "approach", "outcome", "reason"],
        "feedback.outcome_settled.v1" => {
            &["approach", "outcome", "signal", "reason", "claim", "status"]
        }
        _ => return None,
    };
    let mut lines = Vec::new();
    for name in names {
        if let Some(field) = payload.get(*name) {
            let text = field.as_str()?;
            if text.len() > 8192 {
                return None;
            }
            if !text.trim().is_empty() {
                lines.push(format!("{name}: {text}"));
            }
        }
    }
    let text = lines.join("\n");
    if text.is_empty() || text.len() > 32768 {
        return None;
    }
    Some((text.clone(), text))
}

fn scope_value(scope: &OwnedExactScope) -> Value {
    json!({"profile_id": scope.profile_id, "project_id": scope.project_id,
        "repository_identity": scope.repository_identity, "worktree_identity": scope.worktree_identity,
        "branch_identity": scope.branch_identity, "agent_session_id": scope.agent_session_id,
        "resolved_scope_digest": scope.resolved_scope_digest})
}

fn decode_capsule(provenance: &Value) -> Option<Value> {
    decode_named_capsule(provenance, "common_capsule")
}

fn decode_named_capsule(provenance: &Value, name: &str) -> Option<Value> {
    let capsule = provenance.get(name)?;
    if capsule["version"] != 1 {
        return None;
    }
    let bytes = capsule["bytes"]
        .as_array()?
        .iter()
        .map(|item| u8::try_from(item.as_u64()?).ok())
        .collect::<Option<Vec<_>>>()?;
    if bytes.len() > 131_072 || capsule["sha256"].as_str()? != hex_digest(&Sha256::digest(&bytes)) {
        return None;
    }
    serde_json::from_slice(&bytes).ok()
}

fn stable_reference(namespace: &NcmNamespace, record: u64, capsule_digest: &str) -> String {
    let mut digest = Sha256::new();
    for field in [
        b"tracedecay.ncm.memory-reference.v1".as_slice(),
        namespace.as_str().as_bytes(),
        &record.to_be_bytes(),
        capsule_digest.as_bytes(),
    ] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    format!("ncm-memory:{}", hex_digest(&digest.finalize()))
}

pub(crate) fn reconstruct_observation(
    call: &ProviderCall,
    reply: &mut ProviderReply,
) -> Option<()> {
    let mut payload: Value = serde_json::from_slice(&reply.payload.as_ref()?.bytes).ok()?;
    let metadata = payload.get("common_observation")?;
    let retained = decode_capsule(&metadata["provenance"])?;
    let delivery = decode_named_capsule(&metadata["provenance"], "delivery_capsule")?;
    let source = attribution(&retained["original_source"])?;
    let namespace = NcmNamespace::from_exact_scope(&call.exact_scope);
    if scope(&retained["delivery_scope"])? != call.exact_scope {
        return None;
    }
    validate_source_binding(
        &namespace,
        &source,
        &metadata["provenance"],
        metadata["source"].as_str()?,
    )?;
    let original_key = string(&delivery["idempotency_key"])?;
    if Some(original_key) != call.idempotency_key.as_deref() {
        return None;
    }
    let original_operation = string(&delivery["operation_id"])?;
    let stable = stable_reference(
        &namespace,
        metadata["record_id"].as_u64()?,
        metadata["provenance"]["common_capsule"]["sha256"].as_str()?,
    );
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
    payload.as_object_mut()?.remove("common_observation");
    payload["stable_memory_ref"] = json!(stable);
    payload["provider_receipt_digest"] = json!(
        reply
            .terminal
            .committed_effect()
            .provider_receipt_sha256()?
    );
    let bytes = serde_json::to_vec(&payload).ok()?;
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

pub(crate) fn reconstruct_recall(
    call: &ProviderCall,
    instance: &str,
    reply: &mut ProviderReply,
    admission: Option<&CurrentAdvisoryAdmission>,
) -> Option<()> {
    let request: Value = serde_json::from_slice(&call.payload.bytes).ok()?;
    let query = temporal(&request["temporal_query"])?;
    let worker: Value = serde_json::from_slice(&reply.payload.as_ref()?.bytes).ok()?;
    let rows = worker["common_recall"]["candidates"].as_array()?;
    let budgets = &request["budgets"];
    let max_candidates = budgets["maximum_candidates"].as_u64()?.min(16) as usize;
    let max_content = budgets["maximum_candidate_content_bytes"].as_u64()?;
    let max_total = budgets["maximum_total_content_bytes"].as_u64()?;
    let mut candidates = Vec::new();
    let mut total = 0_u64;
    let mut truncated = worker["common_recall"]["truncated"].as_bool()?;
    let mut unknown = worker["common_recall"]["unknown_items"].as_u64()?;
    let mut excluded = worker["common_recall"]["excluded_items"].as_u64()?;
    let mut truncated_items = 0_u64;
    for row in rows {
        let retained = decode_capsule(&row["provenance"])?;
        let original = &retained["original_source"];
        let source = attribution(original)?;
        if scope(&retained["delivery_scope"])? != call.exact_scope {
            return None;
        }
        let namespace = NcmNamespace::from_exact_scope(&call.exact_scope);
        validate_source_binding(
            &namespace,
            &source,
            &row["provenance"],
            row["source"].as_str()?,
        )?;
        if row["key_text"] != retained["projection"]["key_text"]
            || row["value_text"] != retained["projection"]["value_text"]
        {
            return None;
        }
        if let OriginScopeEvidence::Recorded { scope, .. } = &source.origin_scope {
            if scope != &call.exact_scope && !history_source_admitted(&source, admission) {
                return None;
            }
        }
        let content = row["value_text"].as_str()?;
        let digest = hex_digest(&Sha256::digest(content.as_bytes()));
        let stable = row["stable_memory_ref"].as_str()?;
        let candidate_id = row["candidate_id"].as_str()?;
        if stable
            != stable_reference(
                &namespace,
                row["record_id"].as_u64()?,
                row["provenance"]["common_capsule"]["sha256"].as_str()?,
            )
        {
            return None;
        }
        let request_token = opaque_surface_id(&namespace, b"recall-request", &call.request_id);
        let mut candidate_digest = Sha256::new();
        for field in [request_token.as_bytes(), stable.as_bytes()] {
            candidate_digest.update((field.len() as u64).to_be_bytes());
            candidate_digest.update(field);
        }
        if candidate_id != format!("ncm-candidate:{}", hex_digest(&candidate_digest.finalize())) {
            return None;
        }
        let source_refs = strings(&retained["source_refs"])?;
        let is_excluded = [
            ("stable_memory_refs", stable),
            ("trace_refs", stable),
            ("candidate_ids", candidate_id),
            ("observation_ids", source.source.observation_id.as_str()),
            ("content_sha256", digest.as_str()),
        ]
        .iter()
        .any(|(name, value)| {
            request["exclusions"][*name]
                .as_array()
                .is_some_and(|items| items.iter().any(|item| item.as_str() == Some(value)))
        }) || source_refs
            .iter()
            .chain(std::iter::once(&source.source.source_key))
            .any(|source| {
                request["exclusions"]["source_refs"]
                    .as_array()
                    .is_some_and(|items| items.iter().any(|item| item.as_str() == Some(source)))
            });
        if is_excluded {
            excluded += 1;
            continue;
        }
        if candidates.len() == max_candidates
            || source_refs.len() as u64 > budgets["maximum_source_refs_per_candidate"].as_u64()?
        {
            truncated = true;
            truncated_items += 1;
            continue;
        }
        let content_limit = max_content.min(max_total.saturating_sub(total));
        let mut boundary = usize::try_from(content_limit)
            .unwrap_or(usize::MAX)
            .min(content.len());
        while !content.is_char_boundary(boundary) {
            boundary -= 1;
        }
        if boundary < content.len() {
            truncated = true;
            truncated_items += 1;
        }
        let emitted_content = &content[..boundary];
        if emitted_content.is_empty() {
            continue;
        }
        // Exclusions above use the complete retained content; this digest
        // describes exactly the UTF-8 excerpt emitted under the caller's budget.
        let emitted_digest = hex_digest(&Sha256::digest(emitted_content.as_bytes()));
        let score = row["activation"].as_f64()?;
        if !score.is_finite() {
            return None;
        }
        let mut wire_validity = original["validity"].clone();
        if let Some(patch) = row
            .pointer("/provenance/control/validity_patch")
            .and_then(Value::as_object)
        {
            for (field, value) in patch {
                wire_validity[field] = value.clone();
            }
        }
        let validity = validity(&wire_validity)?;
        let at = query
            .as_of_utc_nanos
            .unwrap_or(query.evaluation_time_utc_nanos);
        let temporal_state = if validity.valid_from_utc_nanos.is_none() {
            "unknown"
        } else if validity.revoked_at_utc_nanos.is_some_and(|time| time <= at) {
            "revoked"
        } else if validity
            .superseded_at_utc_nanos
            .is_some_and(|time| time <= at)
        {
            "superseded"
        } else if validity.valid_from_utc_nanos.is_some_and(|time| time > at) {
            "future"
        } else if validity
            .valid_until_utc_nanos
            .is_some_and(|time| time <= at)
        {
            "expired"
        } else {
            "current"
        };
        let degraded = temporal_state == "unknown" || source.source.source_revision.is_none();
        if degraded {
            unknown += 1;
        }
        let warnings: Vec<&str> = if degraded {
            vec!["ncm.original_source_evidence_incomplete"]
        } else {
            Vec::new()
        };
        let mut candidate_scope = scope_value(&call.exact_scope);
        candidate_scope["scope_binding"] = json!("exact_coding_scope");
        total += emitted_content.len() as u64;
        // The validated stable reference is also the existing retained trace
        // inspection selector. It does not attest a kernel activation trace.
        candidates.push(json!({
            "candidate_id": candidate_id, "stable_memory_ref": stable,
            "content": emitted_content, "content_ref": null, "content_sha256": emitted_digest,
            "native_score": {"score_domain_id": "ncm.rbf_activation.v1", "score_domain_version": 1,
                "raw_value": (score as f32).to_string(), "direction": "higher_is_better", "declared_minimum": "0",
                "declared_maximum": (worker["common_recall"]["score_upper_bound"].as_f64()? as f32).to_string(), "calibration_state": "uncalibrated",
                "semantics": "Peak intensity-weighted RBF activation across supporting NCM centers", "components": {}},
            "confidence": row.pointer("/confidence/Uncalibrated").cloned().unwrap_or(Value::Null),
            "exact_scope_identity": candidate_scope,
            "validity": {"observed_at": original["occurred_at"], "valid_from": wire_validity["valid_from"],
                "valid_until": wire_validity["valid_until"], "superseded_at": wire_validity["superseded_at"],
                "superseded_by": wire_validity["superseded_by"], "revoked_at": wire_validity["revoked_at"],
                "source_revision": original["source"]["source_revision"], "temporal_state": temporal_state},
            "provenance": {"state": "available", "origin_refs": source_refs, "observation_refs": [source.source.observation_id],
                "source_refs": source_refs, "transform_chain": ["ncm.canonical_evidence_projection.v1"],
                "provider_trace_refs": [stable], "redaction_reason": null, "original_sources": [original]},
            "explanation": {"summary": "Retained source content recalled through NCM center support", "matched_features": [],
                "activation_trace_refs": [], "limitations": ["Uncalibrated provider-native score"]},
            "source_refs": source_refs, "trace_refs": [stable], "sensitivity": "internal",
            "memory_class": "session_observation", "warnings": warnings, "extensions": []
        }));
    }
    candidates.sort_by(|left, right| {
        let score = |value: &Value| {
            value["native_score"]["raw_value"]
                .as_str()
                .and_then(|value| value.parse::<f64>().ok())
                .unwrap_or(0.0)
        };
        score(right).total_cmp(&score(left)).then_with(|| {
            left["candidate_id"]
                .as_str()
                .cmp(&right["candidate_id"].as_str())
        })
    });
    let partial = truncated || unknown > 0;
    let zero = candidates.is_empty();
    let code = if partial {
        TerminalCode::Partial
    } else if zero {
        TerminalCode::SuccessZeroResults
    } else {
        TerminalCode::Success
    };
    let mut reasons = Vec::new();
    if truncated {
        reasons.push("ncm.recall_budget_truncated");
    }
    if unknown > 0 {
        reasons.push("ncm.original_source_evidence_incomplete");
    }
    let warnings = reasons
        .iter()
        .take(budgets["maximum_warnings"].as_u64()? as usize)
        .copied()
        .collect::<Vec<_>>();
    let value = json!({
        "provider_id": call.provider_id.as_str(), "provider_instance_id": instance,
        "registration_revision": call.registration_revision, "ready_receipt_digest": call.ready_receipt_sha256,
        "request_identity": call.request_id, "exact_scope_identity": scope_value(&call.exact_scope),
        "provider_state_generation": reply.state_generation, "candidates": candidates,
        "coverage": {"state": if partial {"partial"} else if zero {"zero_results"} else {"complete"},
            "searched_scope_digest": call.exact_scope.exact_scope_sha256(),
            "searched_temporal_digest": hex_digest(&Sha256::digest(serde_json::to_vec(&request["temporal_query"]).ok()?)),
            "scanned_items": worker["common_recall"]["scanned_items"], "matched_items": rows.len(),
            "returned_items": candidates.len(), "excluded_items": excluded, "truncated_items": truncated_items,
            "next_cursor": null, "reasons": reasons},
        "ordering": {"provider_order": "deterministic_native_score_then_candidate_id_within_one_score_domain", "tie_breaker": "candidate_id_lexicographic_utf8"},
        "terminal": {"code": code.as_wire()}, "warnings": warnings
    });
    let bytes = serde_json::to_vec(&value).ok()?;
    reply.payload = Some(
        CanonicalPayload::new(
            call.payload.contract_id.clone(),
            bytes.clone(),
            hex_digest(&Sha256::digest(&bytes)),
        )
        .ok()?,
    );
    reply.terminal = tracedecay_memory_provider_api::TerminalRecord::new(
        call.operation,
        call.provider_id.clone(),
        code,
        tracedecay_memory_provider_api::CommittedEffectEvidence::none(Some(reply.state_generation)),
        tracedecay_memory_provider_api::FallbackDirective::forbidden(),
        &call.operation_id,
        call.exact_scope.exact_scope_sha256(),
        None,
    )
    .ok()?;
    Some(())
}
