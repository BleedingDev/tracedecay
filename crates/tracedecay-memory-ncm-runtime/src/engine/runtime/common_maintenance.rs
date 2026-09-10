//! Common maintenance receipts retained atomically with the existing event journal.

use super::super::{
    DurableOperation, DurableReceipt, EngineReply, MaintenanceKind, MaintenanceRequest,
    NamespaceHandle, NcmEngine, Outcome, RejectReason,
};
use super::util::{
    canonical_digest, sha256_hex, store_reply, unavailable_recovery, validate_idempotency_key,
};
use crate::ports::Deadline;
use crate::store::Event;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::time::Instant;
use tracedecay_memory_ncm_core::kernel::NcmKernel;

const MAX_CAPSULE_BYTES: usize = 131_072;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Task {
    Consolidate,
    Decay,
    PruneExpired,
    ValidateState,
    Repair,
    Compact,
}

impl Task {
    fn as_str(self) -> &'static str {
        match self {
            Self::Consolidate => "consolidate",
            Self::Decay => "decay",
            Self::PruneExpired => "prune_expired",
            Self::ValidateState => "validate_state",
            Self::Repair => "repair",
            Self::Compact => "compact",
        }
    }

    fn kind(self) -> Option<MaintenanceKind> {
        match self {
            Self::Consolidate => Some(MaintenanceKind::Consolidate),
            Self::Decay => Some(MaintenanceKind::Advance { ticks: 1 }),
            Self::PruneExpired => Some(MaintenanceKind::MergePrune),
            Self::ValidateState => None,
            Self::Repair => Some(MaintenanceKind::Checkpoint),
            Self::Compact => Some(MaintenanceKind::Compact),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EncodedAdmission {
    version: u64,
    sha256: String,
    bytes: Vec<u8>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Admission {
    namespace: String,
    operation_id: String,
    idempotency_key: String,
    request_semantic_sha256: String,
}

impl EncodedAdmission {
    fn decode(&self) -> Result<Admission, String> {
        if self.version != 1
            || self.bytes.len() > MAX_CAPSULE_BYTES
            || self.sha256 != sha256_hex(&self.bytes)
        {
            return Err("maintenance admission capsule integrity mismatch".to_owned());
        }
        let admission: Admission = serde_json::from_slice(&self.bytes)
            .map_err(|error| format!("invalid maintenance admission capsule: {error}"))?;
        validate_idempotency_key(&admission.idempotency_key)?;
        if admission.operation_id.is_empty() || admission.operation_id.len() > 256 {
            return Err("invalid original maintenance operation identity".to_owned());
        }
        if !is_sha256(&admission.namespace) || !is_sha256(&admission.request_semantic_sha256) {
            return Err("invalid maintenance admission digest".to_owned());
        }
        Ok(admission)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    action: String,
    idempotency_key: String,
    expected_generation: u64,
    task: Task,
    maximum_items: u64,
    maximum_bytes: u64,
    maximum_duration_millis: u64,
    dry_run: bool,
    #[serde(default)]
    resume_cursor: Option<String>,
    policy_revision: u64,
    extensions: Vec<Value>,
    maintenance_capsule: EncodedAdmission,
}

impl Request {
    fn context(&self, namespace: &str) -> Result<CommonMaintenanceContext, String> {
        validate_idempotency_key(&self.idempotency_key)?;
        if self.action != "maintenance"
            || !(1..=1_000_000).contains(&self.maximum_items)
            || !(1..=1_073_741_824).contains(&self.maximum_bytes)
            || !(1..=3_600_000).contains(&self.maximum_duration_millis)
            || self.policy_revision == 0
            || self.extensions.len() > 16
            || self
                .extensions
                .iter()
                .any(|extension| extension["criticality"] != "optional")
        {
            return Err("invalid common maintenance request".to_owned());
        }
        let (cursor_generation, after) = match &self.resume_cursor {
            None => (None, 0),
            Some(cursor) => {
                let (generation, after) = parse_cursor(namespace, self.task, cursor)?;
                (Some(generation), after)
            }
        };
        let request_semantic_sha256 = canonical_digest(&json!({
            "action": "maintenance",
            "task": self.task,
            "dry_run": self.dry_run,
            "maximum_items": self.maximum_items,
            "maximum_bytes": self.maximum_bytes,
            "maximum_duration_millis": self.maximum_duration_millis,
            "resume_cursor": self.resume_cursor,
            "policy_revision": self.policy_revision,
            "extensions": self.extensions,
        }))?;
        let admission = self.maintenance_capsule.decode()?;
        if admission.namespace != namespace
            || admission.request_semantic_sha256 != request_semantic_sha256
        {
            return Err("maintenance admission does not match the request".to_owned());
        }
        Ok(CommonMaintenanceContext {
            admission: self.maintenance_capsule.clone(),
            public_idempotency_key: admission.idempotency_key,
            request_semantic_sha256,
            expected_generation: self.expected_generation,
            cursor_generation,
            after,
            task: self.task,
            scanned_items: 0,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommonOutput {
    task: Task,
    dry_run: bool,
    state_generation_before: u64,
    state_generation_after: u64,
    scanned_items: u64,
    changed_items: u64,
    removed_items: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    state_changed: Option<bool>,
    partial: bool,
    resume_cursor: Option<String>,
    warnings: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EventBasis {
    kind: MaintenanceKind,
    sequence: u64,
    payload_sha256: String,
    created_tick: u64,
    state_digest: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedReceipt {
    admission: EncodedAdmission,
    request_semantic_sha256: String,
    outcome: CommonOutput,
    event_basis: EventBasis,
}

/// Admission and bounded scan evidence handed to the existing maintenance transaction.
pub(super) struct CommonMaintenanceContext {
    admission: EncodedAdmission,
    public_idempotency_key: String,
    pub(super) request_semantic_sha256: String,
    expected_generation: u64,
    cursor_generation: Option<u64>,
    after: u64,
    task: Task,
    scanned_items: u64,
}

impl CommonMaintenanceContext {
    pub(super) fn check_generation(&self, generation: u64) -> Result<(), EngineReply> {
        if self.expected_generation != generation
            || self
                .cursor_generation
                .is_some_and(|cursor_generation| cursor_generation != generation)
        {
            return Err(EngineReply::rejected(
                RejectReason::IdempotencyConflict,
                generation,
            ));
        }
        Ok(())
    }

    pub(super) fn lookup_replay(
        &self,
        namespace: &str,
        handle: &NamespaceHandle,
        key: &str,
    ) -> Result<Option<EngineReply>, EngineReply> {
        let Some(event) = handle
            .store
            .event_for_key(key)
            .map_err(|error| store_reply(error, handle.commit_seq))?
        else {
            return Ok(None);
        };
        if event.payload_sha256 != self.request_semantic_sha256 {
            return Ok(Some(EngineReply::rejected(
                RejectReason::IdempotencyConflict,
                handle.commit_seq,
            )));
        }
        let Some((mut durable, retained)) = checked_event(namespace, &event)
            .map_err(|reason| corrupt(handle.commit_seq, reason))?
        else {
            return Err(corrupt(
                handle.commit_seq,
                "common maintenance receipt is missing",
            ));
        };
        let admission = retained
            .admission
            .decode()
            .map_err(|reason| corrupt(handle.commit_seq, reason))?;
        if admission.idempotency_key != self.public_idempotency_key {
            return Ok(Some(EngineReply::rejected(
                RejectReason::IdempotencyConflict,
                handle.commit_seq,
            )));
        }
        durable.reply.payload["replayed"] = json!(true);
        Ok(Some(durable.reply))
    }

    /// Adds hash-free common evidence before the core receipt is journaled.
    pub(super) fn retain(
        &self,
        reply: &mut EngineReply,
        kind: &MaintenanceKind,
        before: &NcmKernel,
        after: &NcmKernel,
    ) -> Result<(), EngineReply> {
        let changed_items = after
            .records
            .iter()
            .filter(|(id, record)| before.records.get(**id) != Some(*record))
            .count();
        let removed_items = before
            .records
            .iter()
            .filter(|(id, _)| after.records.get(**id).is_none())
            .count();
        let count = |value| {
            u64::try_from(value)
                .map_err(|_| corrupt(self.expected_generation, "record count overflow"))
        };
        let retained = RetainedReceipt {
            admission: self.admission.clone(),
            request_semantic_sha256: self.request_semantic_sha256.clone(),
            outcome: CommonOutput {
                task: self.task,
                dry_run: false,
                state_generation_before: self.expected_generation,
                state_generation_after: reply.state_generation,
                scanned_items: self.scanned_items,
                changed_items: count(changed_items)?,
                removed_items: count(removed_items)?,
                state_changed: Some(before.state_digest() != after.state_digest()),
                partial: false,
                resume_cursor: None,
                warnings: Vec::new(),
            },
            event_basis: EventBasis {
                kind: kind.clone(),
                sequence: reply.state_generation,
                payload_sha256: self.request_semantic_sha256.clone(),
                created_tick: after.scheduler.tick.0,
                state_digest: sha256_hex(&after.state_digest()),
            },
        };
        reply.payload["common_maintenance"] = serde_json::to_value(retained)
            .map_err(|error| corrupt(self.expected_generation, error.to_string()))?;
        Ok(())
    }
}

impl NcmEngine {
    pub(super) fn common_maintenance(
        &self,
        namespace: &str,
        payload: &Value,
        deadline: Deadline,
    ) -> EngineReply {
        let started = Instant::now();
        if deadline.remaining_ms == 0 {
            return EngineReply::new(Outcome::Cancelled, 0, Value::Null);
        }
        let request: Request = match serde_json::from_value(payload.clone()) {
            Ok(request) => request,
            Err(error) => return invalid(format!("invalid maintenance request: {error}"), 0),
        };
        let mut context = match request.context(namespace) {
            Ok(context) => context,
            Err(reason) => return invalid(reason, 0),
        };
        let deadline = Deadline {
            remaining_ms: deadline.remaining_ms.min(request.maximum_duration_millis),
        };
        let mut namespaces = match self.namespace_lock() {
            Ok(namespaces) => namespaces,
            Err(reply) => return reply,
        };
        let handle = match self.ensure_handle(&mut namespaces, namespace, false) {
            Ok(Some(handle)) => handle,
            Ok(None) => {
                if let Err(reply) = context.check_generation(0) {
                    return reply;
                }
                return no_change_reply(&request, namespace, 0, 0, false, 0);
            }
            Err(reply) => return reply,
        };
        if handle.fenced {
            return unavailable_recovery(handle.commit_seq);
        }
        if super::util::remaining_deadline(deadline, started).remaining_ms == 0 {
            return EngineReply::new(Outcome::Cancelled, handle.commit_seq, Value::Null);
        }
        // A matching historical operation wins before current generation/cursor checks.
        match context.lookup_replay(namespace, handle, &request.idempotency_key) {
            Ok(Some(reply)) => return present_reply(reply),
            Ok(None) => {}
            Err(reply) => return reply,
        }
        if let Err(reply) = context.check_generation(handle.commit_seq) {
            return reply;
        }
        let generation = handle.commit_seq;
        let capsules =
            match handle
                .store
                .capsule_page(None, context.after, request.maximum_items + 1)
            {
                Ok(capsules) => capsules,
                Err(error) => return store_reply(error, generation),
            };
        let mut partial = capsules.len() as u64 > request.maximum_items;
        let mut bytes = 0_u64;
        let mut cursor = context.after;
        for capsule in capsules.into_iter().take(request.maximum_items as usize) {
            if super::util::remaining_deadline(deadline, started).remaining_ms == 0 {
                partial = true;
                break;
            }
            let size = (capsule.provenance.len()
                + capsule.key_text.len()
                + capsule.value_text.len()) as u64;
            if bytes.saturating_add(size) > request.maximum_bytes {
                partial = true;
                break;
            }
            let provenance: Value = match serde_json::from_str(&capsule.provenance) {
                Ok(provenance) => provenance,
                Err(error) => return corrupt(generation, error.to_string()),
            };
            bytes += size;
            context.scanned_items += 1;
            cursor = capsule.record_id.0;
            if provenance
                .pointer("/common_capsule/sha256")
                .and_then(Value::as_str)
                .is_none()
            {
                partial = true;
            }
        }
        if request.dry_run || request.task == Task::ValidateState || partial {
            return no_change_reply(
                &request,
                namespace,
                generation,
                context.scanned_items,
                partial,
                cursor,
            );
        }
        let Some(kind) = request.task.kind() else {
            return invalid("maintenance task has no mutation", generation);
        };
        drop(namespaces);
        self.maintenance_with_common_context(
            namespace,
            MaintenanceRequest {
                idempotency_key: request.idempotency_key,
                kind,
                deadline: super::util::remaining_deadline(deadline, started),
            },
            context,
        )
    }
}

fn no_change_reply(
    request: &Request,
    namespace: &str,
    generation: u64,
    scanned_items: u64,
    partial: bool,
    cursor: u64,
) -> EngineReply {
    EngineReply::new(
        Outcome::Success,
        generation,
        json!({
            "common_control": "maintenance", "no_change": true,
            "task": request.task, "dry_run": request.dry_run,
            "state_generation_before": generation, "state_generation_after": generation,
            "scanned_items": scanned_items, "changed_items": 0, "removed_items": 0,
            "partial": partial,
            "resume_cursor": partial.then(|| format!(
                "ncm-maintenance:{namespace}:{}:{generation}:{cursor}", request.task.as_str()
            )),
            "warnings": [],
        }),
    )
}

pub(super) fn present_reply(reply: EngineReply) -> EngineReply {
    if reply.outcome != Outcome::Success {
        return reply;
    }
    let retained: RetainedReceipt =
        match serde_json::from_value(reply.payload["common_maintenance"].clone()) {
            Ok(retained) => retained,
            Err(error) => return corrupt(reply.state_generation, error.to_string()),
        };
    let mut payload = match serde_json::to_value(retained.outcome) {
        Ok(payload) => payload,
        Err(error) => return corrupt(reply.state_generation, error.to_string()),
    };
    payload["common_control"] = json!("maintenance");
    payload["no_change"] = json!(false);
    payload["replayed"] = reply.payload["replayed"].clone();
    // The original core reply includes the common capsule but no receipt hash.
    // Hashing this retained basis cannot refer back to the presentation envelope.
    payload["_retained_receipt"] =
        json!({"generation": reply.state_generation, "payload": reply.payload});
    EngineReply::new(reply.outcome, reply.state_generation, payload)
}

pub(super) fn inspect_receipt(
    namespace: &str,
    handle: &NamespaceHandle,
    payload: &Value,
) -> EngineReply {
    let generation = handle.commit_seq;
    let Some(key) = payload["delivery_key"].as_str() else {
        return invalid("missing maintenance receipt key", generation);
    };
    let Some(maximum_bytes) = payload["maximum_bytes"].as_u64().filter(|value| *value > 0) else {
        return invalid("missing maintenance receipt byte bound", generation);
    };
    let result = (|| -> Result<Option<Value>, EngineReply> {
        let Some(event) = handle
            .store
            .event_for_key(key)
            .map_err(|error| store_reply(error, generation))?
        else {
            return Ok(None);
        };
        let Some((durable, retained)) =
            checked_event(namespace, &event).map_err(|reason| corrupt(generation, reason))?
        else {
            return Ok(None);
        };
        let digest =
            receipt_digest(&durable.reply).map_err(|reason| corrupt(generation, reason))?;
        let item = json!({"maintenance_receipt": retained, "provider_receipt_digest": digest});
        let bytes =
            serde_json::to_vec(&item).map_err(|error| corrupt(generation, error.to_string()))?;
        Ok((bytes.len() as u64 <= maximum_bytes).then_some(item))
    })();
    match result {
        Ok(item) => EngineReply::new(
            Outcome::Success,
            generation,
            json!({
                "common_control": "inspection", "view": "maintenance_receipt",
                "partial": item.is_none(), "cursor_after": null,
                "state_generation": generation, "scanned_items": u64::from(item.is_some()),
                "items": item.into_iter().collect::<Vec<_>>(),
            }),
        ),
        Err(reply) => reply,
    }
}

/// Only verified common maintenance events carry portable original receipt evidence.
pub(crate) fn portable_event(namespace: &str, event: &Event) -> Result<bool, String> {
    checked_event(namespace, event).map(|receipt| receipt.is_some())
}

fn checked_event(
    namespace: &str,
    event: &Event,
) -> Result<Option<(DurableReceipt, RetainedReceipt)>, String> {
    if event.kind != "maintenance" {
        return Ok(None);
    }
    let durable: DurableReceipt = serde_json::from_str(&event.receipt)
        .map_err(|error| format!("invalid durable maintenance receipt: {error}"))?;
    let Some(value) = durable.reply.payload.get("common_maintenance") else {
        return Ok(None);
    };
    let retained: RetainedReceipt = serde_json::from_value(value.clone())
        .map_err(|error| format!("invalid common maintenance receipt: {error}"))?;
    let admission = retained.admission.decode()?;
    let DurableOperation::Maintenance { kind } = &durable.operation else {
        return Err("common maintenance receipt has a non-maintenance operation".to_owned());
    };
    let outcome = &retained.outcome;
    let basis = &retained.event_basis;
    if event.idempotency_key.is_none()
        || admission.namespace != namespace
        || admission.request_semantic_sha256 != retained.request_semantic_sha256
        || event.payload_sha256 != retained.request_semantic_sha256
        || durable.reply.outcome != Outcome::Success
        || durable.reply.state_generation != event.seq
        || basis.kind != *kind
        || basis.sequence != event.seq
        || basis.payload_sha256 != event.payload_sha256
        || basis.created_tick != event.created_tick
        || basis.state_digest != durable.state_digest
        || !is_sha256(&basis.state_digest)
        || outcome.task.kind().as_ref() != Some(kind)
        || outcome.state_generation_before.checked_add(1) != Some(event.seq)
        || outcome.state_generation_after != event.seq
        || outcome.dry_run
        || outcome.partial
        || outcome.resume_cursor.is_some()
        || outcome.state_changed.is_none()
        || outcome
            .changed_items
            .checked_add(outcome.removed_items)
            .is_none_or(|changes| changes > outcome.scanned_items)
    {
        return Err("common maintenance receipt does not match its event".to_owned());
    }
    Ok(Some((durable, retained)))
}

fn receipt_digest(reply: &EngineReply) -> Result<String, String> {
    let mut payload = reply.payload.clone();
    if let Some(object) = payload.as_object_mut() {
        object.remove("replayed");
        object.remove("common_observation");
    }
    let bytes = serde_json::to_vec(&payload)
        .map_err(|error| format!("serialize maintenance receipt basis: {error}"))?;
    let mut digest = Sha256::new();
    digest.update(b"tracedecay.ncm.rust-worker-receipt.v1\0");
    digest.update((b"maintenance".len() as u64).to_be_bytes());
    digest.update(b"maintenance");
    digest.update(reply.state_generation.to_be_bytes());
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
    Ok(digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn parse_cursor(namespace: &str, task: Task, cursor: &str) -> Result<(u64, u64), String> {
    let invalid = || "invalid maintenance cursor".to_owned();
    if cursor.is_empty() || cursor.len() > 1024 {
        return Err(invalid());
    }
    let prefix = format!("ncm-maintenance:{namespace}:{}:", task.as_str());
    let tail = cursor.strip_prefix(&prefix).ok_or_else(invalid)?;
    let (generation, after) = tail.split_once(':').ok_or_else(invalid)?;
    let canonical_number = |value: &str| {
        value
            .parse::<u64>()
            .ok()
            .filter(|number| *number <= i64::MAX as u64 && number.to_string() == value)
            .ok_or_else(invalid)
    };
    Ok((canonical_number(generation)?, canonical_number(after)?))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn invalid(reason: impl Into<String>, generation: u64) -> EngineReply {
    EngineReply::rejected(RejectReason::InvalidRequest(reason.into()), generation)
}

fn corrupt(generation: u64, reason: impl Into<String>) -> EngineReply {
    EngineReply::new(
        Outcome::Corrupt,
        generation,
        json!({"reason": reason.into()}),
    )
}
