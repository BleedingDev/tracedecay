//! Snapshot staging and canonical replay after the adapter's opaque projection.

use super::super::*;
use super::control::{commit_common, replacement_request};
use super::util::{
    canonical_digest, core_reply, lookup_replay, read_live, remaining_deadline, resolve_affect,
    sha256_hex, store_reply,
};
use crate::snapshot::{self, RestoreRequest};
use crate::store::{Capsule, CapsuleStatus};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::time::Instant;
use tracedecay_memory_ncm_core::kernel::NewRecord;

const MAX_ITEMS: usize = 4096;
const MAX_SNAPSHOT_BYTES: usize = 256 * 1024 * 1024;

impl NcmEngine {
    /// Applies private portability controls whose source authority was checked
    /// by the composing adapter. No host identity is interpreted here.
    pub fn common_portability(
        &self,
        namespace: &str,
        payload: Value,
        deadline: Deadline,
    ) -> EngineReply {
        if deadline.remaining_ms == 0 {
            return EngineReply::new(Outcome::Cancelled, 0, Value::Null);
        }
        let Some(expected) = payload["expected_generation"].as_u64() else {
            return invalid("missing expected generation", 0);
        };
        match payload["action"].as_str() {
            Some("snapshot_export") => {
                let snapshot = match snapshot::export(self, namespace, deadline) {
                    Ok(value) => value,
                    Err(reply) => return reply,
                };
                if snapshot.state_generation() != expected {
                    return conflict(snapshot.state_generation());
                }
                EngineReply::new(
                    Outcome::Success,
                    snapshot.state_generation(),
                    json!({"common_portability":"snapshot_export","bytes":snapshot.into_vec(),"warnings":[]}),
                )
            }
            Some("snapshot_restore") => {
                let Some(key) = opaque(&payload["idempotency_key"]) else {
                    return invalid("missing restore key", expected);
                };
                let Some(bytes) = bytes(&payload["bytes"]) else {
                    return invalid("invalid snapshot bytes", expected);
                };
                let Some(blocked) = payload["blocked_sources"].as_array() else {
                    return invalid("missing restore fences", expected);
                };
                if blocked.len() > MAX_ITEMS {
                    return invalid("restore fence bound", expected);
                }
                let mut sources = BTreeSet::new();
                for source in blocked {
                    let Some(source) = opaque(source) else {
                        return invalid("invalid restore fence", expected);
                    };
                    if !sources.insert(SourceId(source.to_owned())) {
                        return invalid("duplicate restore fence", expected);
                    }
                }
                let Some(snapshot_id) = payload["snapshot_id"]
                    .as_str()
                    .filter(|value| value.starts_with("ncm-snapshot:") && value.len() == 77)
                else {
                    return invalid("invalid snapshot identity", expected);
                };
                let Some(sequence) = payload["observation_sequence"].as_u64() else {
                    return invalid("missing snapshot sequence", expected);
                };
                if snapshot_id != format!("ncm-snapshot:{}", sha256_hex(&bytes)) {
                    return invalid("snapshot identity digest differs", expected);
                }
                let mut reply = snapshot::restore_with_revocations(
                    self,
                    namespace,
                    RestoreRequest {
                        idempotency_key: key.to_owned(),
                        bytes,
                    },
                    deadline,
                    &sources.into_iter().collect::<Vec<_>>(),
                    Some(expected),
                );
                if reply.outcome == Outcome::Success {
                    let retained_receipt =
                        json!({"generation": reply.state_generation, "payload":reply.payload});
                    reply.payload = json!({"common_portability":"snapshot_restore","snapshot_id":snapshot_id,"state_generation_before":expected,
                        "state_generation_after":reply.state_generation,"restored_observation_sequence":sequence,"replayed":reply.payload["replayed"],"warnings":[],"_retained_receipt":retained_receipt});
                }
                reply
            }
            Some("replay") => self.replay_page(namespace, payload, deadline),
            _ => invalid("unknown portability action", expected),
        }
    }

    fn replay_page(&self, namespace: &str, payload: Value, deadline: Deadline) -> EngineReply {
        let started = Instant::now();
        let page = match ReplayPage::parse(&payload, deadline) {
            Ok(page) => page,
            Err(reply) => return reply,
        };
        let mut effect = payload.clone();
        if let Some(object) = effect.as_object_mut() {
            object.remove("expected_generation");
            object.remove("idempotency_key");
            // Caller operation IDs do not change replay effects; the first retained page wins.
            object.remove("page_delivery_capsule");
        }
        if let Some(items) = effect["items"].as_array_mut() {
            for item in items {
                item["observation"] = item["observation"]["payload_sha256"].clone();
            }
        }
        let digest = match canonical_digest(&effect) {
            Ok(digest) => digest,
            Err(reason) => return invalid(&reason, 0),
        };
        {
            let mut namespaces = match self.namespace_lock() {
                Ok(value) => value,
                Err(reply) => return reply,
            };
            let existing = match self.ensure_handle(&mut namespaces, namespace, false) {
                Ok(value) => value,
                Err(reply) => return reply,
            };
            if let Some(handle) = existing {
                if handle.fenced {
                    return EngineReply::new(Outcome::Busy, handle.commit_seq, Value::Null);
                }
                match lookup_replay(handle, &page.key, &digest) {
                    Ok(Some(reply)) => return reply,
                    Ok(None) => {}
                    Err(reply) => return reply,
                }
                if handle.commit_seq != page.expected {
                    return conflict(handle.commit_seq);
                }
                let acknowledged = match replay_sequence(handle, None) {
                    Ok(value) => value,
                    Err(reply) => return reply,
                };
                if acknowledged != page.previous || page.first > acknowledged.saturating_add(1) {
                    return invalid("replay sequence gap", handle.commit_seq);
                }
            } else if page.expected != 0 || page.previous != 0 || page.first != 1 {
                return invalid("replay sequence gap", 0);
            }
        }
        let mut output = page.output();
        let mut generation = page.expected;
        let mut acknowledged = page.previous;
        let mut stopped = None;
        for item in &page.items {
            if stopped.is_some() || remaining_deadline(deadline, started).remaining_ms == 0 {
                if stopped.is_none() {
                    stopped = Some(Outcome::Cancelled);
                }
                if let Err(reply) = push_item(
                    &mut output,
                    item,
                    "rejected",
                    Some("not_dispatched"),
                    None,
                    generation,
                ) {
                    return reply;
                }
                continue;
            }
            if item.blocked {
                let fence_key = match canonical_digest(&json!([
                    "replay-fence",
                    item.delivery_key,
                    item.source
                ])) {
                    Ok(value) => value,
                    Err(reason) => return invalid(&reason, generation),
                };
                let fenced = self.delete_by_source(
                    namespace,
                    &SourceId(item.source.clone()),
                    &fence_key,
                    remaining_deadline(deadline, started),
                );
                generation = generation.max(fenced.state_generation);
                if fenced.outcome != Outcome::Success {
                    let unknown = fenced.outcome == Outcome::EffectUnknown;
                    if let Err(reply) = push_item(
                        &mut output,
                        item,
                        if unknown {
                            "effect_unknown"
                        } else {
                            "rejected"
                        },
                        Some("source_fence_failed"),
                        None,
                        generation,
                    ) {
                        return reply;
                    }
                    stopped = Some(fenced.outcome);
                    continue;
                }
            }
            let reply = self.replay_item(namespace, item, deadline, started);
            generation = generation.max(reply.state_generation);
            if reply.outcome == Outcome::Success {
                let Some(mut state) = reply.payload["state"].as_str() else {
                    return EngineReply::new(Outcome::EffectUnknown, generation, output);
                };
                if reply.payload["replayed"] == true && state != "rejected" {
                    state = "delivery_duplicate";
                }
                if let Err(reply) = push_item(
                    &mut output,
                    item,
                    state,
                    reply.payload["reason"].as_str(),
                    reply.payload["record_id"].as_u64(),
                    reply.state_generation,
                ) {
                    return reply;
                }
                acknowledged = acknowledged.max(item.sequence);
                output["acknowledged_sequence"] = json!(acknowledged);
            } else {
                let unknown = reply.outcome == Outcome::EffectUnknown;
                if let Err(reply) = push_item(
                    &mut output,
                    item,
                    if unknown {
                        "effect_unknown"
                    } else {
                        "rejected"
                    },
                    Some(if unknown {
                        "commit_unknown"
                    } else {
                        "not_applied"
                    }),
                    None,
                    generation,
                ) {
                    return reply;
                }
                if !matches!(reply.outcome, Outcome::Rejected(_)) {
                    stopped = Some(reply.outcome);
                }
            }
        }
        output["state_generation_after"] = json!(generation);
        if let Some(outcome) = stopped {
            output["partial"] = json!(true);
            // All previous item receipts are already durable. A known failure
            // after those commits must report a partial effect, never no effect.
            let outcome = if outcome == Outcome::EffectUnknown {
                outcome
            } else if generation > page.expected {
                Outcome::Success
            } else {
                outcome
            };
            return EngineReply::new(outcome, generation, output);
        }
        let mut namespaces = match self.namespace_lock() {
            Ok(value) => value,
            Err(reply) => return partial_reply(output, generation, page.expected, reply.outcome),
        };
        let handle = match self.ensure_handle(&mut namespaces, namespace, true) {
            Ok(Some(value)) => value,
            Ok(None) => return partial_reply(output, generation, page.expected, Outcome::Corrupt),
            Err(reply) => return partial_reply(output, generation, page.expected, reply.outcome),
        };
        if handle.fenced {
            output["partial"] = json!(true);
            output["state_generation_after"] = json!(handle.commit_seq);
            return EngineReply::new(Outcome::EffectUnknown, handle.commit_seq, output);
        }
        match lookup_replay(handle, &page.key, &digest) {
            Ok(Some(reply)) => return reply,
            Ok(None) => {}
            Err(reply) => return reply,
        }
        let live = match read_live(handle) {
            Ok(value) => value,
            Err(reply) => return partial_reply(output, generation, page.expected, reply.outcome),
        };
        output["acknowledged_sequence"] = json!(page.previous.max(page.last));
        let result = commit_common(
            self,
            handle,
            (*live).clone(),
            &page.key,
            &digest,
            Vec::new(),
            Vec::new(),
            None,
            output.clone(),
            deadline,
            started,
        );
        if result.outcome == Outcome::Success {
            return result;
        }
        output["partial"] = json!(true);
        output["acknowledged_sequence"] = json!(acknowledged);
        output["state_generation_after"] = json!(result.state_generation);
        let outcome = if result.outcome == Outcome::EffectUnknown {
            Outcome::EffectUnknown
        } else if generation > page.expected {
            Outcome::Success
        } else {
            result.outcome
        };
        EngineReply::new(outcome, result.state_generation, output)
    }

    fn replay_item(
        &self,
        namespace: &str,
        item: &ReplayItem,
        deadline: Deadline,
        started: Instant,
    ) -> EngineReply {
        let digest = match canonical_digest(
            &json!({"sequence":item.sequence,"receipt":item.receipt_digest,"source":item.source,"admitted":item.admitted,"observation":item.observation["payload_sha256"]}),
        ) {
            Ok(value) => value,
            Err(reason) => return invalid(&reason, 0),
        };
        let request = if item.admitted {
            match replacement_request(&item.observation, deadline) {
                Ok(value) => Some(value),
                Err(reason) => return invalid(&reason, 0),
            }
        } else {
            None
        };
        {
            let mut namespaces = match self.namespace_lock() {
                Ok(value) => value,
                Err(reply) => return reply,
            };
            let handle = match self.ensure_handle(&mut namespaces, namespace, true) {
                Ok(Some(value)) => value,
                Ok(None) => return EngineReply::new(Outcome::Corrupt, 0, Value::Null),
                Err(reply) => return reply,
            };
            if let Some(reply) = existing_item(
                self,
                handle,
                item,
                request.as_ref(),
                &digest,
                deadline,
                started,
            ) {
                return reply;
            }
        }
        let Some(request) = request else {
            return invalid("missing replay observation", 0);
        };
        let embeddings = match self.encoder.encode(
            &[&request.key_text, &request.value_text],
            remaining_deadline(deadline, started),
        ) {
            Ok(value) if value.len() == 2 => value,
            Ok(_) => return EngineReply::new(Outcome::Corrupt, 0, Value::Null),
            Err(error) => return super::util::encoder_reply(error, 0),
        };
        let mut namespaces = match self.namespace_lock() {
            Ok(value) => value,
            Err(reply) => return reply,
        };
        let handle = match self.ensure_handle(&mut namespaces, namespace, true) {
            Ok(Some(value)) => value,
            Ok(None) => return EngineReply::new(Outcome::Corrupt, 0, Value::Null),
            Err(reply) => return reply,
        };
        if let Some(reply) = existing_item(
            self,
            handle,
            item,
            Some(&request),
            &digest,
            deadline,
            started,
        ) {
            return reply;
        }
        let live = match read_live(handle) {
            Ok(value) => value,
            Err(reply) => return reply,
        };
        let mut candidate = (*live).clone();
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
            Ok(value) => value,
            Err(error) => return core_reply(error, handle.commit_seq),
        };
        let Some(record) = candidate.records.get(report.record_id) else {
            return EngineReply::new(Outcome::Corrupt, handle.commit_seq, Value::Null);
        };
        let provenance = match serde_json::to_string(&request.provenance) {
            Ok(value) => value,
            Err(error) => return invalid(&error.to_string(), handle.commit_seq),
        };
        let capsule = Capsule::new(
            request.source,
            request.key_text,
            request.value_text,
            affect,
            request.surprise,
            request.intensity,
            embeddings[0].0.clone(),
            embeddings[1].0.clone(),
            record.ltm_key.clone(),
            provenance,
        );
        commit_common(
            self,
            handle,
            candidate,
            &item.delivery_key,
            &digest,
            vec![DurableOperation::Observe {
                record_id: report.record_id,
            }],
            Vec::new(),
            Some(capsule),
            item.output("applied", None, Some(report.record_id.0)),
            deadline,
            started,
        )
    }
}

fn existing_item(
    engine: &NcmEngine,
    handle: &mut NamespaceHandle,
    item: &ReplayItem,
    request: Option<&ObserveRequest>,
    digest: &str,
    deadline: Deadline,
    started: Instant,
) -> Option<EngineReply> {
    if handle.fenced {
        return Some(EngineReply::new(
            Outcome::Busy,
            handle.commit_seq,
            Value::Null,
        ));
    }
    match lookup_replay(handle, &item.delivery_key, digest) {
        Ok(Some(reply)) => return Some(reply),
        Ok(None) => {}
        Err(reply) => return Some(reply),
    }
    let previous = match replay_sequence(handle, Some(item)) {
        Ok(value) => value,
        Err(reply) => return Some(reply),
    };
    if item.sequence > previous.saturating_add(1) {
        return Some(invalid("replay sequence gap", handle.commit_seq));
    }
    let state = if !item.admitted {
        Some(("rejected", Some("current_disposition"), None))
    } else if let Err(error) = handle
        .store
        .ensure_source_not_revoked(&SourceId(item.source.clone()))
    {
        if matches!(error, crate::store::StoreError::InvalidInput(_)) {
            Some(("rejected", Some("source_revoked"), None))
        } else {
            return Some(store_reply(error, handle.commit_seq));
        }
    } else {
        None
    };
    let state = if let Some(state) = state {
        Some(state)
    } else {
        let request = request?;
        let capsules = match handle.store.capsules_in_commit_order(false) {
            Ok(value) => value,
            Err(error) => return Some(store_reply(error, handle.commit_seq)),
        };
        let mut found = None;
        for capsule in capsules {
            if capsule.status == CapsuleStatus::Revoked {
                continue;
            }
            let provenance: Value = match serde_json::from_str(&capsule.provenance) {
                Ok(value) => value,
                Err(_) => {
                    return Some(EngineReply::new(
                        Outcome::Corrupt,
                        handle.commit_seq,
                        Value::Null,
                    ));
                }
            };
            let original = &request.provenance["selection"];
            if provenance["selection"]["observation_identity"] == original["observation_identity"]
                && provenance["selection"]["revision_digest"] == original["revision_digest"]
            {
                if capsule.source_id != request.source
                    || provenance["common_capsule"]["sha256"]
                        != request.provenance["common_capsule"]["sha256"]
                {
                    return Some(conflict(handle.commit_seq));
                }
                found = Some(("source_already_applied", None, Some(capsule.record_id.0)));
                break;
            }
        }
        found
    };
    let (state, reason, record) = state?;
    let live = match read_live(handle) {
        Ok(value) => value,
        Err(reply) => return Some(reply),
    };
    Some(commit_common(
        engine,
        handle,
        (*live).clone(),
        &item.delivery_key,
        digest,
        Vec::new(),
        Vec::new(),
        None,
        item.output(state, reason, record),
        deadline,
        started,
    ))
}

fn replay_sequence(
    handle: &NamespaceHandle,
    item: Option<&ReplayItem>,
) -> Result<u64, EngineReply> {
    let events = handle
        .store
        .events_after(0)
        .map_err(|error| store_reply(error, handle.commit_seq))?;
    let mut last = 0;
    for event in events {
        if event.kind != "common_control" {
            continue;
        }
        let receipt: DurableReceipt = serde_json::from_str(&event.receipt)
            .map_err(|_| EngineReply::new(Outcome::Corrupt, handle.commit_seq, Value::Null))?;
        let payload = &receipt.reply.payload;
        match payload["common_portability"].as_str() {
            Some("replay_item") => {
                let sequence = payload["source_sequence"].as_u64().ok_or_else(|| {
                    EngineReply::new(Outcome::Corrupt, handle.commit_seq, Value::Null)
                })?;
                if let Some(item) = item
                    && item.sequence == sequence
                    && payload["receipt_digest"] != item.receipt_digest
                {
                    return Err(conflict(handle.commit_seq));
                }
                last = last.max(sequence);
            }
            Some("replay") => {
                if let Some(item) = item {
                    let items = payload["items"].as_array().ok_or_else(|| {
                        EngineReply::new(Outcome::Corrupt, handle.commit_seq, Value::Null)
                    })?;
                    for previous in items {
                        if previous["source_sequence"].as_u64() == Some(item.sequence)
                            && previous["receipt_digest"] != item.receipt_digest
                        {
                            return Err(conflict(handle.commit_seq));
                        }
                    }
                }
                last = last.max(payload["acknowledged_sequence"].as_u64().ok_or_else(|| {
                    EngineReply::new(Outcome::Corrupt, handle.commit_seq, Value::Null)
                })?)
            }
            _ => {}
        }
    }
    Ok(last)
}

struct ReplayPage {
    key: String,
    expected: u64,
    first: u64,
    last: u64,
    previous: u64,
    items: Vec<ReplayItem>,
    page_delivery_capsule: Option<Value>,
}
struct ReplayItem {
    sequence: u64,
    receipt_digest: String,
    delivery_key: String,
    source: String,
    admitted: bool,
    blocked: bool,
    observation: Value,
}

impl ReplayPage {
    fn parse(value: &Value, deadline: Deadline) -> Result<Self, EngineReply> {
        let fail = || invalid("invalid replay page", 0);
        let key = opaque(&value["idempotency_key"])
            .ok_or_else(fail)?
            .to_owned();
        let expected = value["expected_generation"].as_u64().ok_or_else(fail)?;
        let first = value["first_source_sequence"].as_u64().ok_or_else(fail)?;
        let last = value["last_source_sequence"].as_u64().ok_or_else(fail)?;
        let previous = value["expected_previous_acknowledged_sequence"]
            .as_u64()
            .ok_or_else(fail)?;
        let page_delivery_capsule = value.get("page_delivery_capsule").cloned();
        if let Some(capsule) = &page_delivery_capsule {
            let object = capsule.as_object().ok_or_else(fail)?;
            if object.len() != 3
                || !["version", "bytes", "sha256"]
                    .iter()
                    .all(|key| object.contains_key(*key))
            {
                return Err(fail());
            }
            super::util::validate_common_capsule(&json!({"common_capsule": capsule}))
                .map_err(|_| fail())?;
        }
        let values = value["items"].as_array().ok_or_else(fail)?;
        if first == 0
            || values.is_empty()
            || values.len() > MAX_ITEMS
            || last
                .checked_sub(first)
                .and_then(|value| value.checked_add(1))
                != Some(values.len() as u64)
        {
            return Err(fail());
        }
        let mut seen = BTreeSet::new();
        let mut receipts = BTreeSet::new();
        let mut items = Vec::new();
        for (index, value) in values.iter().enumerate() {
            let item = ReplayItem {
                sequence: value["source_sequence"].as_u64().ok_or_else(fail)?,
                receipt_digest: opaque(&value["receipt_digest"])
                    .ok_or_else(fail)?
                    .to_owned(),
                delivery_key: opaque(&value["delivery_key"]).ok_or_else(fail)?.to_owned(),
                source: opaque(&value["source"]).ok_or_else(fail)?.to_owned(),
                admitted: value["admitted"].as_bool().ok_or_else(fail)?,
                blocked: value["blocked"].as_bool().ok_or_else(fail)?,
                observation: value["observation"].clone(),
            };
            if item.sequence != first + index as u64
                || !seen.insert(item.delivery_key.clone())
                || !receipts.insert(item.receipt_digest.clone())
                || (item.admitted && item.blocked)
            {
                return Err(fail());
            }
            if item.admitted {
                let request =
                    replacement_request(&item.observation, deadline).map_err(|_| fail())?;
                if request.idempotency_key != item.delivery_key
                    || request.source.0 != item.source
                    || opaque(&request.provenance["common_capsule"]["sha256"]).is_none()
                    || opaque(&request.provenance["selection"]["observation_identity"]).is_none()
                    || (!request.provenance["selection"]["revision_digest"].is_null()
                        && opaque(&request.provenance["selection"]["revision_digest"]).is_none())
                {
                    return Err(fail());
                }
            } else if !item.observation.is_null() {
                return Err(fail());
            }
            items.push(item);
        }
        Ok(Self {
            key,
            expected,
            first,
            last,
            previous,
            items,
            page_delivery_capsule,
        })
    }
    fn output(&self) -> Value {
        let mut output = json!({"common_portability":"replay","first_source_sequence":self.first,"last_source_sequence":self.last,"acknowledged_sequence":self.previous,"state_generation_before":self.expected,"state_generation_after":self.expected,"applied_observations":0,"duplicate_observations":0,"sources_already_applied":0,"rejected_observations":0,"effect_unknown_observations":0,"partial":false,"replayed":false,"warnings":[],"items":[]});
        if let Some(capsule) = &self.page_delivery_capsule {
            output["page_delivery_capsule"] = capsule.clone();
        }
        output
    }
}

impl ReplayItem {
    fn output(&self, state: &str, reason: Option<&str>, record: Option<u64>) -> Value {
        json!({"common_portability":"replay_item","source_sequence":self.sequence,"receipt_digest":self.receipt_digest,"state":state,"reason":reason,"record_id":record,"replayed":false})
    }
}

fn push_item(
    output: &mut Value,
    item: &ReplayItem,
    state: &str,
    reason: Option<&str>,
    record: Option<u64>,
    generation: u64,
) -> Result<(), EngineReply> {
    let corrupt = || EngineReply::new(Outcome::EffectUnknown, generation, Value::Null);
    let counter = match state {
        "applied" => "applied_observations",
        "delivery_duplicate" => "duplicate_observations",
        "source_already_applied" => "sources_already_applied",
        "effect_unknown" => "effect_unknown_observations",
        "rejected" => "rejected_observations",
        _ => return Err(corrupt()),
    };
    let count = output[counter]
        .as_u64()
        .and_then(|value| value.checked_add(1))
        .ok_or_else(corrupt)?;
    output[counter] = json!(count);
    output["items"].as_array_mut().ok_or_else(corrupt)?.push(json!({"source_sequence":item.sequence,"receipt_digest":item.receipt_digest,"state":state,"reason":reason,"record_id":record,"state_generation":generation}));
    Ok(())
}

fn partial_reply(
    mut payload: Value,
    generation: u64,
    before: u64,
    outcome: Outcome,
) -> EngineReply {
    payload["partial"] = json!(true);
    payload["state_generation_after"] = json!(generation);
    let outcome = if outcome == Outcome::EffectUnknown {
        outcome
    } else if generation > before {
        Outcome::Success
    } else {
        outcome
    };
    EngineReply::new(outcome, generation, payload)
}

fn opaque(value: &Value) -> Option<&str> {
    value.as_str().filter(|value| {
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}
fn bytes(value: &Value) -> Option<Vec<u8>> {
    let values = value.as_array()?;
    if values.is_empty() || values.len() > MAX_SNAPSHOT_BYTES {
        return None;
    }
    values
        .iter()
        .map(|value| u8::try_from(value.as_u64()?).ok())
        .collect()
}
fn invalid(reason: &str, generation: u64) -> EngineReply {
    EngineReply::rejected(RejectReason::InvalidRequest(reason.to_owned()), generation)
}
fn conflict(generation: u64) -> EngineReply {
    EngineReply::rejected(RejectReason::IdempotencyConflict, generation)
}
