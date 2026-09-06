use super::super::*;
use super::util::{
    canonical_digest, core_reply, durable_receipt, lookup_replay, maintenance_name,
    meta_for_kernel, publish, put_checkpoint, read_live, remaining_deadline, store_reply,
    unavailable_recovery, validate_idempotency_key,
};
use crate::store::CapsuleStatus;
use serde::Serialize;
use serde_json::{Value, json};
use std::time::Instant;
use tracedecay_memory_ncm_core::kernel::NcmKernel;
use tracedecay_memory_ncm_core::types::CoreError;

impl NcmEngine {
    /// Applies idempotent explicit usage feedback without advancing logical time.
    pub fn feedback(&self, namespace: &str, request: FeedbackRequest) -> EngineReply {
        let digest = match canonical_digest(&request.record_ids) {
            Ok(digest) => digest,
            Err(reason) => return EngineReply::rejected(RejectReason::InvalidRequest(reason), 0),
        };
        self.mutate_existing(
            namespace,
            &request.idempotency_key,
            &digest,
            request.deadline,
            "feedback",
            |kernel| {
                let report = kernel.feedback(&request.record_ids)?;
                Ok((
                    DurableOperation::Feedback {
                        record_ids: request.record_ids.clone(),
                    },
                    json!({"centers_updated": report.centers_updated, "replayed": false}),
                    None,
                    false,
                ))
            },
        )
    }

    /// Applies idempotent supersession lineage without advancing logical time.
    pub fn correction(&self, namespace: &str, request: CorrectionRequest) -> EngineReply {
        if request.evidence.len() != 64
            || !request
                .evidence
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        {
            return EngineReply::rejected(
                RejectReason::InvalidRequest(
                    "correction evidence must be lowercase SHA-256 hex".to_owned(),
                ),
                0,
            );
        }
        #[derive(Serialize)]
        struct Payload<'a> {
            superseded: RecordId,
            superseding: RecordId,
            evidence: &'a str,
        }
        let digest = match canonical_digest(&Payload {
            superseded: request.superseded,
            superseding: request.superseding,
            evidence: &request.evidence,
        }) {
            Ok(digest) => digest,
            Err(reason) => return EngineReply::rejected(RejectReason::InvalidRequest(reason), 0),
        };
        self.mutate_existing(
            namespace,
            &request.idempotency_key,
            &digest,
            request.deadline,
            "correction",
            |kernel| {
                kernel.correction(
                    request.superseded,
                    request.superseding,
                    request.evidence.clone(),
                )?;
                Ok((
                    DurableOperation::Correction {
                        superseded: request.superseded,
                        superseding: request.superseding,
                        evidence: request.evidence.clone(),
                    },
                    json!({
                        "superseded": request.superseded.0,
                        "superseding": request.superseding.0,
                        "replayed": false
                    }),
                    None,
                    false,
                ))
            },
        )
    }

    /// Runs one idempotent bounded maintenance mutation.
    pub fn maintenance(&self, namespace: &str, request: MaintenanceRequest) -> EngineReply {
        let digest = match canonical_digest(&request.kind) {
            Ok(digest) => digest,
            Err(reason) => return EngineReply::rejected(RejectReason::InvalidRequest(reason), 0),
        };
        self.mutate_existing(
            namespace,
            &request.idempotency_key,
            &digest,
            request.deadline,
            "maintenance",
            |kernel| {
                let payload = match &request.kind {
                    MaintenanceKind::Advance { ticks } => {
                        let report = kernel.advance(*ticks)?;
                        json!({
                            "ticks_applied": report.ticks_applied,
                            "sleep_due": report.sleep_due,
                            "replayed": false
                        })
                    }
                    MaintenanceKind::Consolidate => {
                        let report = kernel.consolidate()?;
                        json!({"report": report, "replayed": false})
                    }
                    MaintenanceKind::MergePrune => {
                        let report = kernel.merge_prune()?;
                        json!({"report": report, "replayed": false})
                    }
                    MaintenanceKind::Checkpoint => {
                        json!({"checkpoint": true, "replayed": false})
                    }
                };
                Ok((
                    DurableOperation::Maintenance {
                        kind: request.kind.clone(),
                    },
                    payload,
                    Some(maintenance_name(&request.kind).to_owned()),
                    matches!(&request.kind, MaintenanceKind::Checkpoint),
                ))
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn mutate_existing<F>(
        &self,
        namespace: &str,
        idempotency_key: &str,
        payload_sha256: &str,
        deadline: Deadline,
        event_kind: &str,
        mutate: F,
    ) -> EngineReply
    where
        F: FnOnce(
            &mut NcmKernel,
        ) -> Result<(DurableOperation, Value, Option<String>, bool), CoreError>,
    {
        let started = Instant::now();
        if deadline.remaining_ms == 0 {
            return EngineReply::new(Outcome::Cancelled, 0, Value::Null);
        }
        if let Err(reason) = validate_idempotency_key(idempotency_key) {
            return EngineReply::rejected(RejectReason::InvalidRequest(reason), 0);
        }
        let mut namespaces = match self.namespace_lock() {
            Ok(namespaces) => namespaces,
            Err(reply) => return reply,
        };
        let handle = match self.ensure_handle(&mut namespaces, namespace, false) {
            Ok(Some(handle)) => handle,
            Ok(None) => return EngineReply::new(Outcome::Empty, 0, Value::Null),
            Err(reply) => return reply,
        };
        if handle.fenced {
            return unavailable_recovery(handle.commit_seq);
        }
        match lookup_replay(handle, idempotency_key, payload_sha256) {
            Ok(Some(reply)) => return reply,
            Ok(None) => {}
            Err(reply) => return reply,
        }
        if remaining_deadline(deadline, started).remaining_ms == 0 {
            return EngineReply::new(Outcome::Cancelled, handle.commit_seq, Value::Null);
        }
        let live = match read_live(handle) {
            Ok(live) => live,
            Err(reply) => return reply,
        };
        let mut candidate = (*live).clone();
        let (operation, payload, new_last_maintenance, force_checkpoint) =
            match mutate(&mut candidate) {
                Ok(result) => result,
                Err(error) => return core_reply(error, handle.commit_seq),
            };
        let correction_record = match &operation {
            DurableOperation::Correction { superseded, .. } => Some(*superseded),
            _ => None,
        };
        let pending_seq = match handle.commit_seq.checked_add(1) {
            Some(seq) => seq,
            None => return EngineReply::new(Outcome::Corrupt, handle.commit_seq, Value::Null),
        };
        let reply = EngineReply::new(Outcome::Success, pending_seq, payload);
        let receipt = match durable_receipt(&reply, operation, &candidate) {
            Ok(receipt) => receipt,
            Err(reply) => return reply,
        };
        let prior_meta = match handle.store.meta() {
            Ok(meta) => meta,
            Err(error) => return store_reply(error, handle.commit_seq),
        };
        let last_maintenance = new_last_maintenance.or(prior_meta.last_maintenance);
        let mut mutation = match handle.store.begin_mutation() {
            Ok(mutation) => mutation,
            Err(error) => return store_reply(error, handle.commit_seq),
        };
        if let Some(record_id) = correction_record
            && let Err(error) = mutation.mark_capsule_status(record_id, CapsuleStatus::Superseded)
        {
            return store_reply(error, handle.commit_seq);
        }
        if let Err(error) = mutation.append_event(
            event_kind,
            Some(idempotency_key),
            payload_sha256,
            &receipt,
            candidate.scheduler.tick.0,
        ) {
            return store_reply(error, handle.commit_seq);
        }
        if let Err(error) = mutation.set_meta(&meta_for_kernel(
            &candidate,
            handle.epoch,
            pending_seq,
            last_maintenance,
        )) {
            return store_reply(error, handle.commit_seq);
        }
        if force_checkpoint || pending_seq % CHECKPOINT_INTERVAL == 0 {
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
        if remaining_deadline(deadline, started).remaining_ms == 0 {
            return EngineReply::new(
                Outcome::EffectUnknown,
                committed,
                json!({"commit_seq": committed}),
            );
        }
        reply
    }
}
