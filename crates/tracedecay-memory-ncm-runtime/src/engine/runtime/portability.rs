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
        let page = match ReplayPage::parse(namespace, &payload, deadline) {
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
                match lookup_page_replay(handle, &page, &effect, &digest, deadline) {
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
                if let Some(reply) = public_replay_no_change(Some(handle), &page, deadline, started)
                {
                    return reply;
                }
            } else if page.expected != 0 || page.previous != 0 || page.first != 1 {
                return invalid("replay sequence gap", 0);
            } else if let Some(reply) = public_replay_no_change(None, &page, deadline, started) {
                return reply;
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
                let fenced = if let Some(legacy_source) = &item.legacy_source {
                    let sources = [SourceId(item.source.clone())];
                    let bindings = [crate::source_binding::DeletionSourceBinding {
                        source_id: sources[0].clone(),
                        legacy_source_id: SourceId(legacy_source.clone()),
                    }];
                    crate::privacy::delete_sources(
                        self,
                        namespace,
                        &sources,
                        &fence_key,
                        remaining_deadline(deadline, started),
                        generation,
                        Some(&bindings),
                    )
                } else {
                    self.delete_by_source(
                        namespace,
                        &SourceId(item.source.clone()),
                        &fence_key,
                        remaining_deadline(deadline, started),
                    )
                };
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
            let reply = self.replay_item(
                namespace,
                item,
                page.page_delivery_capsule.is_some(),
                deadline,
                started,
            );
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
        match lookup_page_replay(handle, &page, &effect, &digest, deadline) {
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
        public_page: bool,
        deadline: Deadline,
        started: Instant,
    ) -> EngineReply {
        let digest = match replay_item_digest(item) {
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
                public_page,
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
            public_page,
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

#[allow(clippy::too_many_arguments)]
fn existing_item(
    engine: &NcmEngine,
    handle: &mut NamespaceHandle,
    item: &ReplayItem,
    public_page: bool,
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
    let retained_delivery = match lookup_item_replay(handle, item, request, digest) {
        Ok(Some(reply)) if public_page && reply.outcome == Outcome::Success => true,
        Ok(Some(reply)) => return Some(reply),
        Ok(None) => false,
        Err(reply) => return Some(reply),
    };
    let previous = match replay_sequence(handle, Some(item)) {
        Ok(value) => value,
        Err(reply) => return Some(reply),
    };
    if item.sequence > previous.saturating_add(1) {
        return Some(invalid("replay sequence gap", handle.commit_seq));
    }
    let (state, reason, record) =
        match classify_replay_item(handle, item, request, deadline, started) {
            Ok(Some(state)) => state,
            Ok(None) if retained_delivery => {
                return Some(EngineReply::new(
                    Outcome::Corrupt,
                    handle.commit_seq,
                    Value::Null,
                ));
            }
            Ok(None) => return None,
            Err(reply) => return Some(reply),
        };
    if retained_delivery {
        // A new public page resolves current source state; only a retained page
        // receipt can establish a duplicate public delivery.
        return Some(EngineReply::new(
            Outcome::Success,
            handle.commit_seq,
            item.output(state, reason, record),
        ));
    }
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

type ExistingReplayItem = (&'static str, Option<&'static str>, Option<u64>);

fn classify_replay_item(
    handle: &NamespaceHandle,
    item: &ReplayItem,
    request: Option<&ObserveRequest>,
    deadline: Deadline,
    started: Instant,
) -> Result<Option<ExistingReplayItem>, EngineReply> {
    let state = if !item.admitted {
        Some(("rejected", Some("current_disposition"), None))
    } else if let Err(error) = handle.store.ensure_source_provenance_not_revoked(
        &SourceId(item.source.clone()),
        &request.map_or(Value::Null, |request| request.provenance.clone()),
    ) {
        if matches!(error, crate::store::StoreError::SourceRevoked) {
            Some(("rejected", Some("source_revoked"), None))
        } else {
            return Err(store_reply(error, handle.commit_seq));
        }
    } else {
        None
    };
    let state = if let Some(state) = state {
        Some(state)
    } else {
        let Some(request) = request else {
            return Ok(None);
        };
        let binding = match crate::source_binding::read(
            handle.store.namespace(),
            &request.source,
            &request.provenance,
        ) {
            Ok(binding) => binding,
            Err(reason) => return Err(invalid(&reason, handle.commit_seq)),
        };
        let filter = binding
            .as_ref()
            .map_or(&request.source, |binding| &binding.legacy_source_id);
        let mut after = 0;
        let mut scanned = 0_u64;
        let mut found = None;
        loop {
            if remaining_deadline(deadline, started).remaining_ms == 0 {
                return Err(EngineReply::new(
                    Outcome::Cancelled,
                    handle.commit_seq,
                    Value::Null,
                ));
            }
            let limit = 128_u64.min(1_000_000_u64.saturating_sub(scanned).saturating_add(1));
            let capsules = match handle.store.capsule_page(Some(&filter.0), after, limit) {
                Ok(value) => value,
                Err(error) => return Err(store_reply(error, handle.commit_seq)),
            };
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
                if capsule.status == CapsuleStatus::Revoked {
                    continue;
                }
                let provenance: Value = match serde_json::from_str(&capsule.provenance) {
                    Ok(value) => value,
                    Err(_) => {
                        return Err(EngineReply::new(
                            Outcome::Corrupt,
                            handle.commit_seq,
                            Value::Null,
                        ));
                    }
                };
                let retained_binding = match crate::source_binding::read(
                    handle.store.namespace(),
                    &capsule.source_id,
                    &provenance,
                ) {
                    Ok(binding) => binding,
                    Err(reason) => {
                        return Err(EngineReply::new(
                            Outcome::Corrupt,
                            handle.commit_seq,
                            json!({"reason":reason}),
                        ));
                    }
                };
                if binding.as_ref().is_some_and(|request| {
                    !request.is_legacy
                        && retained_binding
                            .as_ref()
                            .and_then(|binding| binding.full_source_id.as_ref())
                            != request.full_source_id.as_ref()
                }) {
                    continue;
                }
                let original = &request.provenance["selection"];
                if provenance["selection"]["observation_identity"]
                    == original["observation_identity"]
                    && provenance["selection"]["revision_digest"] == original["revision_digest"]
                {
                    let same_source = capsule.source_id == request.source
                        || binding.as_ref().is_some_and(|request| {
                            !request.is_legacy
                                && retained_binding.as_ref().is_some_and(|retained| {
                                    retained.is_legacy
                                        && retained.full_source_id == request.full_source_id
                                })
                        });
                    if !same_source
                        || provenance["common_capsule"]["sha256"]
                            != request.provenance["common_capsule"]["sha256"]
                    {
                        return Err(conflict(handle.commit_seq));
                    }
                    found = Some(("source_already_applied", None, Some(capsule.record_id.0)));
                    break;
                }
            }
            if found.is_some() || complete {
                break;
            }
        }
        found
    };
    Ok(state)
}

fn replay_item_digest(item: &ReplayItem) -> Result<String, String> {
    canonical_digest(
        &json!({"sequence":item.sequence,"receipt":item.receipt_digest,"source":item.source,"admitted":item.admitted,"observation":item.observation["payload_sha256"]}),
    )
}

// This read-only path runs with the namespace lock after the page identity,
// generation, and contiguous-cursor checks. No receipt or acknowledgement is
// recorded when every item is already present or already durably fenced.
fn public_replay_no_change(
    mut handle: Option<&mut NamespaceHandle>,
    page: &ReplayPage,
    deadline: Deadline,
    started: Instant,
) -> Option<EngineReply> {
    page.page_delivery_capsule.as_ref()?;
    if let Some(handle) = handle.as_deref() {
        if let Err(reply) = read_live(handle) {
            return Some(reply);
        }
    }
    let mut output = page.output();
    for item in &page.items {
        if remaining_deadline(deadline, started).remaining_ms == 0 {
            return Some(EngineReply::new(
                Outcome::Cancelled,
                page.expected,
                Value::Null,
            ));
        }
        let state = if let Some(handle) = handle.as_deref_mut() {
            if let Err(reply) = replay_sequence(handle, Some(item)) {
                return Some(reply);
            }
            if item.blocked {
                let mut fenced = false;
                for source in std::iter::once(&item.source).chain(item.legacy_source.iter()) {
                    match handle.store.ensure_source_provenance_not_revoked(
                        &SourceId(source.clone()),
                        &Value::Null,
                    ) {
                        Err(crate::store::StoreError::SourceRevoked) => fenced = true,
                        Ok(()) => {}
                        Err(error) => return Some(store_reply(error, handle.commit_seq)),
                    }
                }
                if !fenced {
                    return None;
                }
                for source in std::iter::once(&item.source).chain(item.legacy_source.iter()) {
                    match handle.store.has_retained_source(&SourceId(source.clone())) {
                        Ok(false) => {}
                        Ok(true) => return None,
                        Err(error) => return Some(store_reply(error, handle.commit_seq)),
                    }
                }
            }
            let request = if item.admitted {
                match replacement_request(&item.observation, deadline) {
                    Ok(request) => Some(request),
                    Err(reason) => return Some(invalid(&reason, handle.commit_seq)),
                }
            } else {
                None
            };
            let digest = match replay_item_digest(item) {
                Ok(digest) => digest,
                Err(reason) => return Some(invalid(&reason, handle.commit_seq)),
            };
            let retained = match lookup_item_replay(handle, item, request.as_ref(), &digest) {
                Ok(Some(reply)) if reply.outcome == Outcome::Success => true,
                Ok(Some(reply)) => return Some(reply),
                Ok(None) => false,
                Err(reply) => return Some(reply),
            };
            match classify_replay_item(handle, item, request.as_ref(), deadline, started) {
                Ok(Some(state)) => state,
                Ok(None) if retained => {
                    return Some(EngineReply::new(
                        Outcome::Corrupt,
                        handle.commit_seq,
                        Value::Null,
                    ));
                }
                Ok(None) => return None,
                Err(reply) => return Some(reply),
            }
        } else if !item.admitted && !item.blocked {
            ("rejected", Some("current_disposition"), None)
        } else {
            return None;
        };
        if let Err(reply) = push_item(&mut output, item, state.0, state.1, state.2, page.expected) {
            return Some(reply);
        }
    }
    if remaining_deadline(deadline, started).remaining_ms == 0 {
        return Some(EngineReply::new(
            Outcome::Cancelled,
            page.expected,
            Value::Null,
        ));
    }
    output["no_change"] = json!(true);
    Some(EngineReply::new(Outcome::Success, page.expected, output))
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
    legacy_source: Option<String>,
    admitted: bool,
    blocked: bool,
    observation: Value,
}

impl ReplayPage {
    fn parse(namespace: &str, value: &Value, deadline: Deadline) -> Result<Self, EngineReply> {
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
                legacy_source: match value.get("legacy_source") {
                    None => None,
                    Some(value) => Some(opaque(value).ok_or_else(fail)?.to_owned()),
                },
                admitted: value["admitted"].as_bool().ok_or_else(fail)?,
                blocked: value["blocked"].as_bool().ok_or_else(fail)?,
                observation: value["observation"].clone(),
            };
            if item.sequence != first + index as u64
                || !seen.insert(item.delivery_key.clone())
                || !receipts.insert(item.receipt_digest.clone())
                || (item.admitted && item.blocked)
                || item
                    .legacy_source
                    .as_ref()
                    .is_some_and(|legacy| legacy == &item.source)
            {
                return Err(fail());
            }
            if item.admitted {
                let request =
                    replacement_request(&item.observation, deadline).map_err(|_| fail())?;
                let binding =
                    crate::source_binding::read(namespace, &request.source, &request.provenance)
                        .map_err(|_| fail())?;
                if let Some(legacy) = &item.legacy_source {
                    if !binding.as_ref().is_some_and(|binding| {
                        !binding.is_legacy
                            && binding.legacy_source_id.0 == *legacy
                            && binding.full_source_id.as_ref() == Some(&request.source)
                    }) {
                        return Err(fail());
                    }
                }
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

fn legacy_request(
    namespace: &str,
    request: &ObserveRequest,
) -> Result<Option<ObserveRequest>, String> {
    let Some(binding) =
        crate::source_binding::read(namespace, &request.source, &request.provenance)?
            .filter(|binding| !binding.is_legacy)
    else {
        return Ok(None);
    };
    let mut legacy = request.clone();
    legacy.source = binding.legacy_source_id;
    if let Some(provenance) = legacy.provenance.as_object_mut() {
        provenance.remove("source_binding");
    }
    legacy.payload_sha256 = legacy.canonical_payload_sha256()?;
    Ok(Some(legacy))
}

fn item_digest(item: &ReplayItem, request: &ObserveRequest) -> Result<String, String> {
    canonical_digest(
        &json!({"sequence":item.sequence,"receipt":item.receipt_digest,
        "source":request.source,"admitted":item.admitted,"observation":request.payload_sha256}),
    )
}

fn legacy_item_matches(
    handle: &NamespaceHandle,
    item: &ReplayItem,
    request: &ObserveRequest,
    digest: &str,
) -> Result<bool, EngineReply> {
    let Some(event) = handle
        .store
        .event_for_key(&item.delivery_key)
        .map_err(|error| store_reply(error, handle.commit_seq))?
    else {
        return Ok(false);
    };
    if event.payload_sha256 != digest {
        return Ok(false);
    }
    let durable: DurableReceipt = serde_json::from_str(&event.receipt)
        .map_err(|_| EngineReply::new(Outcome::Corrupt, handle.commit_seq, Value::Null))?;
    if durable.reply.payload["common_portability"] != "replay_item" {
        return Ok(false);
    }
    let Some(record_id) = durable.reply.payload["record_id"].as_u64() else {
        return Ok(false);
    };
    let Some(capsule) = handle
        .store
        .capsule(RecordId(record_id))
        .map_err(|error| store_reply(error, handle.commit_seq))?
        .filter(|capsule| capsule.status != CapsuleStatus::Revoked)
    else {
        return Ok(false);
    };
    let provenance: Value = serde_json::from_str(&capsule.provenance)
        .map_err(|_| EngineReply::new(Outcome::Corrupt, handle.commit_seq, Value::Null))?;
    let requested = crate::source_binding::read(
        handle.store.namespace(),
        &request.source,
        &request.provenance,
    )
    .map_err(|reason| invalid(&reason, handle.commit_seq))?;
    let retained =
        crate::source_binding::read(handle.store.namespace(), &capsule.source_id, &provenance)
            .map_err(|reason| {
                EngineReply::new(
                    Outcome::Corrupt,
                    handle.commit_seq,
                    json!({"reason":reason}),
                )
            })?;
    Ok(requested.as_ref().is_some_and(|requested| {
        !requested.is_legacy
            && retained.as_ref().is_some_and(|retained| {
                retained.is_legacy
                    && retained.full_source_id == requested.full_source_id
                    && retained.legacy_source_id == requested.legacy_source_id
            })
    }) && provenance["common_capsule"] == request.provenance["common_capsule"])
}

fn lookup_item_replay(
    handle: &mut NamespaceHandle,
    item: &ReplayItem,
    request: Option<&ObserveRequest>,
    digest: &str,
) -> Result<Option<EngineReply>, EngineReply> {
    if let Some(request) = request {
        if let Some(legacy) = legacy_request(handle.store.namespace(), request)
            .map_err(|reason| invalid(&reason, handle.commit_seq))?
        {
            let legacy_digest =
                item_digest(item, &legacy).map_err(|reason| invalid(&reason, handle.commit_seq))?;
            if legacy_item_matches(handle, item, request, &legacy_digest)? {
                return lookup_replay(handle, &item.delivery_key, &legacy_digest);
            }
        }
    }
    lookup_replay(handle, &item.delivery_key, digest)
}

fn lookup_page_replay(
    handle: &mut NamespaceHandle,
    page: &ReplayPage,
    effect: &Value,
    digest: &str,
    deadline: Deadline,
) -> Result<Option<EngineReply>, EngineReply> {
    let Some(event) = handle
        .store
        .event_for_key(&page.key)
        .map_err(|error| store_reply(error, handle.commit_seq))?
    else {
        return Ok(None);
    };
    if event.payload_sha256 == digest {
        return lookup_replay(handle, &page.key, digest);
    }
    let mut legacy_effect = effect.clone();
    let Some(values) = legacy_effect["items"].as_array_mut() else {
        return Err(invalid("missing replay items", handle.commit_seq));
    };
    let mut converted = Vec::new();
    for (item, value) in page.items.iter().zip(values.iter_mut()) {
        if !item.admitted {
            if item.legacy_source.is_some() {
                // The old receipt retained no full-source capsule for this
                // blocked item. A raw alias cannot establish its identity.
                return lookup_replay(handle, &page.key, digest);
            }
            continue;
        }
        let request = replacement_request(&item.observation, deadline)
            .map_err(|reason| invalid(&reason, handle.commit_seq))?;
        let Some(legacy) = legacy_request(handle.store.namespace(), &request)
            .map_err(|reason| invalid(&reason, handle.commit_seq))?
        else {
            continue;
        };
        let legacy_digest =
            item_digest(item, &legacy).map_err(|reason| invalid(&reason, handle.commit_seq))?;
        value["source"] = json!(legacy.source);
        value["observation"] = json!(legacy.payload_sha256);
        if let Some(value) = value.as_object_mut() {
            value.remove("legacy_source");
        }
        converted.push((item, request, legacy_digest));
    }
    let legacy_digest =
        canonical_digest(&legacy_effect).map_err(|reason| invalid(&reason, handle.commit_seq))?;
    if !converted.is_empty() && event.payload_sha256 == legacy_digest {
        for (item, request, item_digest) in converted {
            if !legacy_item_matches(handle, item, &request, &item_digest)? {
                return lookup_replay(handle, &page.key, digest);
            }
        }
        return lookup_replay(handle, &page.key, &legacy_digest);
    }
    lookup_replay(handle, &page.key, digest)
}
