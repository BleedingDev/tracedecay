//! Versioned, bounded NCM state export and atomic restore.
//!
//! Snapshots carry learned state and replay inputs, but never the durable source
//! revocation authority. Restore intersects imported capsules with the
//! revocations trusted by the destination before publication.

use crate::engine::{
    CheckpointEnvelope, DurableOperation, DurableReceipt, EngineReply, MaintenanceKind,
    NamespaceHandle, NcmEngine, Outcome, RejectReason,
};
use crate::ports::{Deadline, StateRoot};
use crate::store::{
    CapsuleStatus, Event, NamespaceStore, Revocation, StoreIdentity, StoredCapsule,
};
use rusqlite::{Connection, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use tracedecay_memory_ncm_core::centers::MemoryCenters;
use tracedecay_memory_ncm_core::kernel::NcmKernel;
use tracedecay_memory_ncm_core::projections::ProjectionBundle;
use tracedecay_memory_ncm_core::records::RecordState;
use tracedecay_memory_ncm_core::types::{
    AFFECT_DIM, AlgorithmIdentity, CONTEXT_DIM, EMBEDDING_DIM, LTM_KEY_DIM, NcmConfig, SourceId,
    TERRAIN_DIM, VALUE_DIM,
};

const FORMAT: &str = "ncm-snapshot.v1";
const MAX_SNAPSHOT_BYTES: usize = 256 * 1024 * 1024;
const STAGING_PREFIX: &str = ".ncm-snapshot-staging";

/// Owned bytes in the `ncm-snapshot.v1` JSON envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotBytes {
    bytes: Vec<u8>,
    state_generation: u64,
}

impl SnapshotBytes {
    /// Borrows the serialized snapshot envelope.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the exported namespace commit sequence.
    #[must_use]
    pub const fn state_generation(&self) -> u64 {
        self.state_generation
    }

    /// Consumes the wrapper and returns the serialized envelope.
    #[must_use]
    pub fn into_vec(self) -> Vec<u8> {
        self.bytes
    }
}

/// Idempotent snapshot restore input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreRequest {
    /// Namespace-local idempotency key.
    pub idempotency_key: String,
    /// Complete `ncm-snapshot.v1` envelope bytes.
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotLengths {
    kernel_state_bytes: u64,
    capsules_bytes: u64,
    events_bytes: u64,
    capsule_count: u64,
    event_count: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotContent {
    format: String,
    namespace: String,
    algorithm: AlgorithmIdentity,
    projection_sha256: String,
    encoder_model: String,
    encoder_artifact_sha256: String,
    seed: u64,
    config_json: String,
    epoch: u64,
    commit_seq: u64,
    tick: u64,
    kernel_state: String,
    capsules: Vec<StoredCapsule>,
    events: Vec<Event>,
    lengths: SnapshotLengths,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotEnvelope {
    #[serde(flatten)]
    content: SnapshotContent,
    content_sha256: String,
}

struct ValidatedSnapshot {
    content: SnapshotContent,
    kernel: NcmKernel,
    identity: StoreIdentity,
    content_sha256: String,
}

struct StageGuard {
    root: PathBuf,
}

impl Drop for StageGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

/// Exports one materialized namespace without exporting its revocation table.
///
/// The returned envelope is bounded by the namespace controlled-storage quota.
pub fn export(
    engine: &NcmEngine,
    namespace: &str,
    deadline: Deadline,
) -> Result<SnapshotBytes, EngineReply> {
    if deadline.remaining_ms == 0 {
        return Err(EngineReply::new(Outcome::Cancelled, 0, Value::Null));
    }
    let mut namespaces = engine.namespace_lock()?;
    let handle = match engine.ensure_handle(&mut namespaces, namespace, false)? {
        Some(handle) => handle,
        None => return Err(EngineReply::new(Outcome::Empty, 0, Value::Null)),
    };
    if handle.fenced {
        return Err(EngineReply::new(
            Outcome::Busy,
            handle.commit_seq,
            Value::Null,
        ));
    }
    let kernel = handle
        .live
        .read()
        .map(|live| Arc::clone(&live))
        .map_err(|_| corrupt_reply(handle.commit_seq, "published kernel lock poisoned"))?;
    let capsules = handle
        .store
        .capsules_in_commit_order(false)
        .map_err(|error| store_reply(error, handle.commit_seq))?;
    let events = handle
        .store
        .events_after(0)
        .map_err(|error| store_reply(error, handle.commit_seq))?
        .into_iter()
        .filter(|event| matches!(event.kind.as_str(), "feedback" | "correction"))
        .collect::<Vec<_>>();
    let meta = handle
        .store
        .meta()
        .map_err(|error| store_reply(error, handle.commit_seq))?;
    let kernel_state = serde_json::to_string(&CheckpointEnvelope {
        kernel: (*kernel).clone(),
        state_digest: sha256_hex(&kernel.state_digest()),
    })
    .map_err(|error| {
        corrupt_reply(
            handle.commit_seq,
            &format!("serialize snapshot kernel: {error}"),
        )
    })?;
    let lengths = section_lengths(&kernel_state, &capsules, &events)
        .map_err(|reason| corrupt_reply(handle.commit_seq, &reason))?;
    let identity = handle.store.identity();
    let content = SnapshotContent {
        format: FORMAT.to_owned(),
        namespace: namespace.to_owned(),
        algorithm: identity.algorithm.clone(),
        projection_sha256: identity.projection_sha256.clone(),
        encoder_model: identity.encoder_model.clone(),
        encoder_artifact_sha256: identity.encoder_artifact_sha256.clone(),
        seed: identity.seed,
        config_json: identity.config_json.clone(),
        epoch: meta.epoch,
        commit_seq: meta.commit_seq,
        tick: meta.tick,
        kernel_state,
        capsules,
        events,
        lengths,
    };
    let content_bytes = serde_json::to_vec(&content).map_err(|error| {
        corrupt_reply(
            handle.commit_seq,
            &format!("serialize snapshot content: {error}"),
        )
    })?;
    let envelope = SnapshotEnvelope {
        content,
        content_sha256: sha256_hex(&content_bytes),
    };
    let bytes = serde_json::to_vec(&envelope).map_err(|error| {
        corrupt_reply(
            handle.commit_seq,
            &format!("serialize snapshot envelope: {error}"),
        )
    })?;
    let limit = usize::try_from(handle.store.quota().controlled_bytes)
        .unwrap_or(usize::MAX)
        .min(MAX_SNAPSHOT_BYTES);
    if bytes.len() > limit {
        return Err(EngineReply::new(
            Outcome::BudgetExceeded,
            handle.commit_seq,
            Value::Null,
        ));
    }
    Ok(SnapshotBytes {
        bytes,
        state_generation: meta.commit_seq,
    })
}

/// Restores through a staging store, applying destination-authoritative revocations.
#[must_use]
pub fn restore(
    engine: &NcmEngine,
    namespace: &str,
    request: RestoreRequest,
    deadline: Deadline,
) -> EngineReply {
    if deadline.remaining_ms == 0 {
        return EngineReply::new(Outcome::Cancelled, 0, Value::Null);
    }
    if let Err(reason) = validate_idempotency_key(&request.idempotency_key) {
        return EngineReply::rejected(RejectReason::InvalidRequest(reason), 0);
    }
    let snapshot_digest = sha256_hex(&request.bytes);
    let validated = match validate_snapshot(engine, namespace, &request.bytes) {
        Ok(snapshot) => snapshot,
        Err(reply) => return reply,
    };
    if validated
        .content
        .events
        .iter()
        .any(|event| event.idempotency_key.as_deref() == Some(request.idempotency_key.as_str()))
    {
        return EngineReply::rejected(RejectReason::IdempotencyConflict, 0);
    }
    let mut namespaces = match engine.namespace_lock() {
        Ok(namespaces) => namespaces,
        Err(reply) => return reply,
    };
    if !namespaces.contains_key(namespace) && NamespaceStore::exists(&engine.root, namespace) {
        let existing = match NamespaceStore::open(&engine.root, namespace, &validated.identity) {
            Ok(store) => store,
            Err(error) => return store_reply(error, 0),
        };
        let fenced = match existing.fenced() {
            Ok(fenced) => fenced.is_some(),
            Err(error) => return store_reply(error, 0),
        };
        if fenced {
            return EngineReply::new(Outcome::Busy, 0, Value::Null);
        }
    }
    let current = match engine.ensure_handle(&mut namespaces, namespace, false) {
        Ok(current) => current,
        Err(reply) => return reply,
    };
    let (current_seq, current_epoch, revocations) = if let Some(handle) = current {
        if handle.fenced {
            return EngineReply::new(Outcome::Busy, handle.commit_seq, Value::Null);
        }
        match lookup_restore_replay(handle, &request.idempotency_key, &snapshot_digest) {
            Ok(Some(reply)) => return reply,
            Ok(None) => {}
            Err(reply) => return reply,
        }
        let revocations = match handle.store.revocations() {
            Ok(revocations) => revocations,
            Err(error) => return store_reply(error, handle.commit_seq),
        };
        (handle.commit_seq, handle.epoch, revocations)
    } else {
        (0, 0, Vec::new())
    };
    let revoked = revocations
        .iter()
        .map(|row| row.source_id.clone())
        .collect::<BTreeSet<_>>();
    let stripped = validated
        .content
        .capsules
        .iter()
        .filter(|capsule| revoked.contains(&capsule.source_id))
        .map(|capsule| capsule.source_id.clone())
        .collect::<BTreeSet<_>>();
    let target_epoch = match current_epoch.max(validated.content.epoch).checked_add(1) {
        Some(epoch) => epoch,
        None => return corrupt_reply(current_seq, "snapshot restore epoch overflow"),
    };
    let base_seq = current_seq.max(validated.content.commit_seq);
    let target_seq = match base_seq.checked_add(if stripped.is_empty() { 1 } else { 2 }) {
        Some(seq) => seq,
        None => return corrupt_reply(current_seq, "snapshot restore sequence overflow"),
    };
    let stage = match create_stage(engine.root.path(), namespace, &snapshot_digest) {
        Ok(stage) => stage,
        Err(reason) => return unavailable_reply(current_seq, &reason),
    };
    let stage_root = match StateRoot::new(stage.root.clone()) {
        Ok(root) => root,
        Err(reason) => return unavailable_reply(current_seq, &reason),
    };
    let stage_store =
        match NamespaceStore::create(&stage_root, namespace, validated.identity.clone()) {
            Ok(store) => store,
            Err(error) => return store_reply(error, current_seq),
        };
    let stage_db = stage_store.path().to_path_buf();
    drop(stage_store);
    let build = if stripped.is_empty() {
        build_direct_stage(
            &stage_db,
            &validated,
            &revocations,
            target_epoch,
            target_seq,
            &request.idempotency_key,
            &snapshot_digest,
        )
        .map(|()| (validated.kernel.clone(), target_seq))
    } else {
        build_sanitized_stage(
            &stage_root,
            namespace,
            &stage_db,
            &validated,
            &revocations,
            &stripped,
            target_epoch,
            base_seq,
            target_seq,
            &request.idempotency_key,
            &snapshot_digest,
        )
    };
    let (kernel, committed_seq) = match build {
        Ok(result) => result,
        Err(reply) => return reply,
    };
    if committed_seq != target_seq {
        return corrupt_reply(current_seq, "staged restore sequence mismatch");
    }
    let reply = EngineReply::new(
        Outcome::Success,
        target_seq,
        json!({
            "format": FORMAT,
            "epoch": target_epoch,
            "commit_seq": target_seq,
            "tick": kernel.scheduler.tick.0,
            "state_digest": sha256_hex(&kernel.state_digest()),
            "content_sha256": validated.content_sha256,
            "stripped_sources": stripped.len(),
            "replayed": false
        }),
    );
    if let Err(error) = replace_restore_receipt(
        &stage_db,
        target_seq,
        &request.idempotency_key,
        &snapshot_digest,
        &reply,
        &kernel,
    ) {
        return error;
    }
    let mut staged = match NamespaceStore::open(&stage_root, namespace, &validated.identity) {
        Ok(store) => store,
        Err(error) => return store_reply(error, current_seq),
    };
    let usage = match staged.usage() {
        Ok(usage) => usage,
        Err(error) => return store_reply(error, current_seq),
    };
    if usage.physical_bytes() > staged.quota().controlled_bytes
        || usage.source_basis_bytes() > staged.quota().source_basis_bytes
    {
        return EngineReply::new(Outcome::BudgetExceeded, current_seq, Value::Null);
    }
    if let Err(error) = staged.compact(true) {
        return store_reply(error, current_seq);
    }
    drop(staged);
    namespaces.remove(namespace);
    if let Err(reason) = publish_stage(engine.root.path(), namespace, &stage_db) {
        return unavailable_reply(current_seq, &reason);
    }
    let store = match NamespaceStore::open(&engine.root, namespace, &validated.identity) {
        Ok(store) => store,
        Err(error) => return store_reply(error, target_seq),
    };
    let meta = match store.meta() {
        Ok(meta) => meta,
        Err(error) => return store_reply(error, target_seq),
    };
    if meta.commit_seq != target_seq || meta.epoch != target_epoch {
        return corrupt_reply(target_seq, "published restore metadata mismatch");
    }
    let use_id = engine
        .use_clock
        .fetch_add(1, Ordering::Relaxed)
        .saturating_add(1);
    namespaces.insert(
        namespace.to_owned(),
        NamespaceHandle {
            store,
            live: Arc::new(RwLock::new(Arc::new(kernel))),
            commit_seq: target_seq,
            epoch: target_epoch,
            fenced: false,
            last_used: use_id,
        },
    );
    reply
}

fn validate_snapshot(
    engine: &NcmEngine,
    namespace: &str,
    bytes: &[u8],
) -> Result<ValidatedSnapshot, EngineReply> {
    if bytes.is_empty() || bytes.len() > MAX_SNAPSHOT_BYTES {
        return Err(rejected(
            "snapshot size is outside the controlled-storage budget",
        ));
    }
    let envelope: SnapshotEnvelope = serde_json::from_slice(bytes)
        .map_err(|error| rejected(&format!("decode snapshot envelope: {error}")))?;
    let content_bytes = serde_json::to_vec(&envelope.content)
        .map_err(|error| rejected(&format!("serialize snapshot checksum content: {error}")))?;
    if !is_sha256_hex(&envelope.content_sha256)
        || sha256_hex(&content_bytes) != envelope.content_sha256
    {
        return Err(rejected("snapshot content checksum mismatch"));
    }
    if envelope.content.format != FORMAT {
        return Err(rejected("unsupported snapshot format"));
    }
    if envelope.content.namespace != namespace {
        return Err(rejected("snapshot namespace mismatch"));
    }
    let expected_lengths = section_lengths(
        &envelope.content.kernel_state,
        &envelope.content.capsules,
        &envelope.content.events,
    )
    .map_err(|reason| rejected(&reason))?;
    let lengths = &envelope.content.lengths;
    if expected_lengths.kernel_state_bytes != lengths.kernel_state_bytes
        || expected_lengths.capsules_bytes != lengths.capsules_bytes
        || expected_lengths.events_bytes != lengths.events_bytes
        || expected_lengths.capsule_count != lengths.capsule_count
        || expected_lengths.event_count != lengths.event_count
    {
        return Err(rejected("snapshot section length mismatch"));
    }
    let checkpoint: CheckpointEnvelope = serde_json::from_str(&envelope.content.kernel_state)
        .map_err(|error| rejected(&format!("decode snapshot kernel: {error}")))?;
    validate_kernel(&checkpoint.kernel, &engine.config)?;
    if checkpoint.state_digest != sha256_hex(&checkpoint.kernel.state_digest()) {
        return Err(rejected("snapshot kernel digest mismatch"));
    }
    if checkpoint.kernel.scheduler.tick.0 != envelope.content.tick {
        return Err(rejected("snapshot tick does not match kernel scheduler"));
    }
    validate_capsules(&envelope.content, &checkpoint.kernel, &engine.config)?;
    validate_events(&envelope.content)?;
    let projection_bytes = serde_json::to_vec(&checkpoint.kernel.projections)
        .map_err(|error| rejected(&format!("serialize snapshot projections: {error}")))?;
    let identity = StoreIdentity {
        algorithm: envelope.content.algorithm.clone(),
        projection_sha256: envelope.content.projection_sha256.clone(),
        encoder_model: envelope.content.encoder_model.clone(),
        encoder_artifact_sha256: envelope.content.encoder_artifact_sha256.clone(),
        projection_bytes,
        seed: envelope.content.seed,
        config_json: envelope.content.config_json.clone(),
    };
    let expected = expected_identity(engine, namespace)?;
    if identity != expected {
        return Err(EngineReply::new(Outcome::Incompatible, 0, Value::Null));
    }
    Ok(ValidatedSnapshot {
        content: envelope.content,
        kernel: checkpoint.kernel,
        identity,
        content_sha256: envelope.content_sha256,
    })
}

fn expected_identity(engine: &NcmEngine, namespace: &str) -> Result<StoreIdentity, EngineReply> {
    let seed = namespace_seed(namespace)
        .ok_or_else(|| rejected("namespace is not lowercase hexadecimal"))?;
    let config_json = serde_json::to_string(&engine.config)
        .map_err(|error| corrupt_reply(0, &format!("serialize config: {error}")))?;
    let projections = ProjectionBundle::generate(seed);
    let projection_bytes = serde_json::to_vec(&projections)
        .map_err(|error| corrupt_reply(0, &format!("serialize projections: {error}")))?;
    let encoder = engine.encoder.identity();
    Ok(StoreIdentity {
        algorithm: AlgorithmIdentity {
            profile: tracedecay_memory_ncm_core::types::ALGORITHM_PROFILE.to_owned(),
            config_sha256: sha256_hex(config_json.as_bytes()),
        },
        projection_sha256: sha256_hex(&projection_bytes),
        encoder_model: encoder.model,
        encoder_artifact_sha256: encoder.artifact_sha256,
        projection_bytes,
        seed,
        config_json,
    })
}

fn validate_kernel(kernel: &NcmKernel, config: &NcmConfig) -> Result<(), EngineReply> {
    if kernel.config != *config {
        return Err(EngineReply::new(Outcome::Incompatible, 0, Value::Null));
    }
    validate_centers(&kernel.stm, "STM")?;
    validate_centers(&kernel.ltm, "LTM")?;
    for (terrain, name) in [(&kernel.terrain.stm, "STM"), (&kernel.terrain.ltm, "LTM")] {
        let cells = terrain
            .resolution
            .checked_pow(3)
            .ok_or_else(|| rejected(&format!("{name} terrain cell count overflow")))?;
        if terrain.resolution != config.terrain_resolution
            || terrain.h.len() != cells
            || terrain.e.len() != AFFECT_DIM.saturating_mul(cells)
            || !terrain
                .h
                .iter()
                .chain(&terrain.e)
                .all(|value| value.is_finite())
            || ![terrain.alpha_h, terrain.alpha_e, terrain.leak]
                .iter()
                .all(|value| value.is_finite())
        {
            return Err(rejected(&format!("{name} terrain state is invalid")));
        }
    }
    if !kernel.scheduler.fatigue.is_finite() {
        return Err(rejected("snapshot scheduler fatigue is non-finite"));
    }
    let record_ids = kernel
        .records
        .iter()
        .map(|(id, _)| *id)
        .collect::<BTreeSet<_>>();
    for centers in [&kernel.stm, &kernel.ltm] {
        for id in centers
            .record
            .iter()
            .flatten()
            .chain(centers.support.iter().flatten())
        {
            if !record_ids.contains(id) {
                return Err(rejected(
                    "snapshot center support references an unknown record",
                ));
            }
        }
    }
    Ok(())
}

fn validate_centers(centers: &MemoryCenters, name: &str) -> Result<(), EngineReply> {
    let n = centers.config.n_centers;
    let lengths_ok = centers.keys.len() == n.saturating_mul(centers.config.d_key)
        && centers.ltm_keys.len() == n.saturating_mul(LTM_KEY_DIM)
        && centers.values.len() == n.saturating_mul(VALUE_DIM)
        && centers.intensity.len() == n
        && centers.affect.len() == n.saturating_mul(AFFECT_DIM)
        && centers.usage.len() == n
        && centers.age.len() == n
        && centers.active.len() == n
        && centers.incarnation.len() == n
        && centers.context.len() == n.saturating_mul(CONTEXT_DIM)
        && centers.terrain.len() == n.saturating_mul(TERRAIN_DIM)
        && centers.record.len() == n
        && centers.support.len() == n;
    let finite = centers
        .keys
        .iter()
        .chain(&centers.ltm_keys)
        .chain(&centers.values)
        .chain(&centers.intensity)
        .chain(&centers.affect)
        .chain(&centers.context)
        .chain(&centers.terrain)
        .all(|value| value.is_finite());
    if !lengths_ok || !finite {
        return Err(rejected(&format!("{name} center state is invalid")));
    }
    Ok(())
}

fn validate_capsules(
    content: &SnapshotContent,
    kernel: &NcmKernel,
    config: &NcmConfig,
) -> Result<(), EngineReply> {
    let mut prior = 0_u64;
    let by_id = content
        .capsules
        .iter()
        .map(|capsule| (capsule.record_id, capsule))
        .collect::<BTreeMap<_, _>>();
    if by_id.len() != content.capsules.len() {
        return Err(rejected("snapshot contains duplicate capsule record IDs"));
    }
    for capsule in &content.capsules {
        if capsule.record_id.0 == 0 || capsule.record_id.0 <= prior {
            return Err(rejected("snapshot capsule IDs are not strictly ascending"));
        }
        prior = capsule.record_id.0;
        if capsule.status == CapsuleStatus::Revoked {
            return Err(rejected("snapshot contains a revoked capsule"));
        }
        let text_bytes = capsule
            .key_text
            .len()
            .checked_add(capsule.value_text.len())
            .ok_or_else(|| rejected("snapshot capsule text length overflow"))?;
        if capsule.source_id.0.is_empty()
            || text_bytes > config.max_record_bytes
            || capsule.key_embedding.len() != EMBEDDING_DIM
            || capsule.value_embedding.len() != EMBEDDING_DIM
            || capsule.ltm_key.len() != LTM_KEY_DIM
            || capsule.commit_seq == 0
            || capsule.commit_seq > content.commit_seq
            || !capsule
                .key_embedding
                .iter()
                .chain(&capsule.value_embedding)
                .chain(&capsule.ltm_key)
                .chain(capsule.affect.0.iter())
                .all(|value| value.is_finite())
            || !capsule.surprise.is_finite()
            || !capsule.intensity.is_finite()
            || serde_json::from_str::<Value>(&capsule.provenance).is_err()
        {
            return Err(rejected("snapshot capsule is invalid"));
        }
    }
    if kernel.records.len() != content.capsules.len() {
        return Err(rejected(
            "snapshot capsule count does not match kernel records",
        ));
    }
    for (record_id, record) in kernel.records.iter() {
        let capsule = by_id
            .get(record_id)
            .ok_or_else(|| rejected("snapshot kernel record has no source capsule"))?;
        let expected_status = match record.state {
            RecordState::Valid => CapsuleStatus::Valid,
            RecordState::Superseded { .. } => CapsuleStatus::Superseded,
            RecordState::Deleted { .. } => {
                return Err(rejected("snapshot kernel contains a deleted record"));
            }
        };
        if capsule.status != expected_status
            || record.source != capsule.source_id
            || record.key_text != capsule.key_text
            || record.value_text != capsule.value_text
            || record.ltm_key != capsule.ltm_key
        {
            return Err(rejected(
                "snapshot capsule does not match its kernel record",
            ));
        }
    }
    Ok(())
}

fn validate_events(content: &SnapshotContent) -> Result<(), EngineReply> {
    let capsule_sequences = content
        .capsules
        .iter()
        .map(|capsule| capsule.commit_seq)
        .collect::<BTreeSet<_>>();
    let mut sequences = BTreeSet::new();
    let mut keys = BTreeSet::new();
    for event in &content.events {
        if event.seq == 0
            || event.seq > content.commit_seq
            || capsule_sequences.contains(&event.seq)
            || !sequences.insert(event.seq)
        {
            return Err(rejected("snapshot event sequence is invalid"));
        }
        if let Some(key) = &event.idempotency_key
            && !keys.insert(key.clone())
        {
            return Err(rejected(
                "snapshot contains duplicate event idempotency keys",
            ));
        }
        let receipt: DurableReceipt = serde_json::from_str(&event.receipt)
            .map_err(|error| rejected(&format!("decode snapshot event receipt: {error}")))?;
        if !matches!(
            (event.kind.as_str(), &receipt.operation),
            ("feedback", DurableOperation::Feedback { .. })
                | ("correction", DurableOperation::Correction { .. })
        ) {
            return Err(rejected("snapshot contains a non-portable event"));
        }
    }
    Ok(())
}

fn section_lengths(
    kernel_state: &str,
    capsules: &[StoredCapsule],
    events: &[Event],
) -> Result<SnapshotLengths, String> {
    let capsule_bytes = serde_json::to_vec(capsules)
        .map_err(|error| format!("serialize snapshot capsules: {error}"))?;
    let event_bytes = serde_json::to_vec(events)
        .map_err(|error| format!("serialize snapshot events: {error}"))?;
    Ok(SnapshotLengths {
        kernel_state_bytes: to_u64(kernel_state.len(), "kernel state")?,
        capsules_bytes: to_u64(capsule_bytes.len(), "capsules")?,
        events_bytes: to_u64(event_bytes.len(), "events")?,
        capsule_count: to_u64(capsules.len(), "capsule count")?,
        event_count: to_u64(events.len(), "event count")?,
    })
}

#[allow(clippy::too_many_arguments)]
fn build_direct_stage(
    db_path: &Path,
    snapshot: &ValidatedSnapshot,
    revocations: &[Revocation],
    target_epoch: u64,
    target_seq: u64,
    idempotency_key: &str,
    payload_sha256: &str,
) -> Result<(), EngineReply> {
    let reply = EngineReply::new(
        Outcome::Success,
        target_seq,
        json!({
            "format": FORMAT,
            "epoch": target_epoch,
            "commit_seq": target_seq,
            "tick": snapshot.kernel.scheduler.tick.0,
            "state_digest": sha256_hex(&snapshot.kernel.state_digest()),
            "content_sha256": snapshot.content_sha256,
            "stripped_sources": 0,
            "replayed": false
        }),
    );
    let receipt = restore_receipt(&reply, &snapshot.kernel)?;
    let checkpoint = checkpoint_bytes(&snapshot.kernel)?;
    populate_stage(
        db_path,
        &snapshot.content.capsules,
        &snapshot.content.events,
        revocations,
        &BTreeSet::new(),
        target_epoch,
        target_seq,
        &snapshot.kernel,
        kernel_next_record_id(&snapshot.kernel)?,
        Some(FinalRows {
            seq: target_seq,
            idempotency_key,
            payload_sha256,
            receipt: &receipt,
            checkpoint: &checkpoint,
        }),
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_sanitized_stage(
    stage_root: &StateRoot,
    namespace: &str,
    db_path: &Path,
    snapshot: &ValidatedSnapshot,
    revocations: &[Revocation],
    stripped: &BTreeSet<SourceId>,
    target_epoch: u64,
    base_seq: u64,
    target_seq: u64,
    idempotency_key: &str,
    payload_sha256: &str,
) -> Result<(NcmKernel, u64), EngineReply> {
    let fence_seq = base_seq
        .checked_add(1)
        .ok_or_else(|| corrupt_reply(base_seq, "snapshot fence sequence overflow"))?;
    if target_seq != fence_seq.saturating_add(1) {
        return Err(corrupt_reply(base_seq, "snapshot target sequence mismatch"));
    }
    let fence_receipt = serde_json::to_string(&DurableReceipt {
        reply: EngineReply::new(
            Outcome::Success,
            fence_seq,
            json!({"fenced": true, "target_epoch": target_epoch}),
        ),
        operation: DurableOperation::DeletionFence {
            source: SourceId("snapshot-restore".to_owned()),
            target_epoch,
            idempotency_key: idempotency_key.to_owned(),
            payload_sha256: payload_sha256.to_owned(),
            deleted_records: u64::try_from(stripped.len()).unwrap_or(u64::MAX),
        },
        state_digest: sha256_hex(&snapshot.kernel.state_digest()),
    })
    .map_err(|error| {
        corrupt_reply(
            base_seq,
            &format!("serialize snapshot fence receipt: {error}"),
        )
    })?;
    populate_stage(
        db_path,
        &snapshot.content.capsules,
        &snapshot.content.events,
        revocations,
        stripped,
        snapshot.content.epoch,
        fence_seq,
        &snapshot.kernel,
        kernel_next_record_id(&snapshot.kernel)?,
        None,
        Some(FenceRows {
            seq: fence_seq,
            receipt: &fence_receipt,
        }),
    )?;
    let mut store = NamespaceStore::open(stage_root, namespace, &snapshot.identity)
        .map_err(|error| store_reply(error, base_seq))?;
    let resumed = crate::privacy::resume_pending_rebuild(&mut store, &snapshot.kernel.config)?
        .ok_or_else(|| corrupt_reply(base_seq, "snapshot sanitized rebuild did not resume"))?;
    let result = (resumed.kernel, resumed.meta.commit_seq);
    drop(store);
    Ok(result)
}

struct FinalRows<'a> {
    seq: u64,
    idempotency_key: &'a str,
    payload_sha256: &'a str,
    receipt: &'a str,
    checkpoint: &'a [u8],
}

struct FenceRows<'a> {
    seq: u64,
    receipt: &'a str,
}

#[allow(clippy::too_many_arguments)]
fn populate_stage(
    db_path: &Path,
    capsules: &[StoredCapsule],
    events: &[Event],
    revocations: &[Revocation],
    stripped: &BTreeSet<SourceId>,
    epoch: u64,
    commit_seq: u64,
    kernel: &NcmKernel,
    next_record_id: u64,
    final_rows: Option<FinalRows<'_>>,
    fence_rows: Option<FenceRows<'_>>,
) -> Result<(), EngineReply> {
    let mut conn = Connection::open(db_path).map_err(|error| {
        unavailable_reply(0, &format!("open snapshot staging database: {error}"))
    })?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
        .map_err(|error| {
            unavailable_reply(0, &format!("configure snapshot staging database: {error}"))
        })?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| {
            unavailable_reply(0, &format!("begin snapshot staging transaction: {error}"))
        })?;
    tx.execute("DELETE FROM capsules", [])
        .and_then(|_| tx.execute("DELETE FROM events", []))
        .and_then(|_| tx.execute("DELETE FROM checkpoints", []))
        .and_then(|_| tx.execute("DELETE FROM revocations", []))
        .and_then(|_| tx.execute("DELETE FROM fence", []))
        .map_err(|error| {
            unavailable_reply(0, &format!("clear snapshot staging tables: {error}"))
        })?;

    for capsule in capsules {
        let is_stripped = stripped.contains(&capsule.source_id);
        let affect = f32_blob(&capsule.affect.0);
        let key_embedding = if is_stripped {
            Vec::new()
        } else {
            f32_blob(&capsule.key_embedding)
        };
        let value_embedding = if is_stripped {
            Vec::new()
        } else {
            f32_blob(&capsule.value_embedding)
        };
        let ltm_key = if is_stripped {
            Vec::new()
        } else {
            f32_blob(&capsule.ltm_key)
        };
        tx.execute(
            "INSERT INTO capsules(
                record_id, source_id, key_text, value_text, affect, surprise, intensity,
                key_embedding, value_embedding, ltm_key, provenance, status, commit_seq
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                sqlite_i64(capsule.record_id.0, "record ID")?,
                capsule.source_id.0,
                if is_stripped { "" } else { &capsule.key_text },
                if is_stripped { "" } else { &capsule.value_text },
                affect,
                capsule.surprise,
                capsule.intensity,
                key_embedding,
                value_embedding,
                ltm_key,
                if is_stripped {
                    "{}"
                } else {
                    &capsule.provenance
                },
                if is_stripped {
                    "revoked"
                } else {
                    status_name(capsule.status)
                },
                sqlite_i64(capsule.commit_seq, "capsule commit sequence")?
            ],
        )
        .map_err(|error| unavailable_reply(0, &format!("insert snapshot capsule: {error}")))?;
    }

    for capsule in capsules {
        let receipt = serde_json::to_string(&DurableReceipt {
            reply: EngineReply::new(Outcome::Success, capsule.commit_seq, Value::Null),
            operation: DurableOperation::Observe {
                record_id: capsule.record_id,
            },
            state_digest: String::new(),
        })
        .map_err(|error| corrupt_reply(0, &format!("serialize staged observe receipt: {error}")))?;
        let created_tick = kernel
            .records
            .get(capsule.record_id)
            .map_or(0, |record| record.created_tick.0);
        insert_event(
            &tx,
            capsule.commit_seq,
            "observe",
            None,
            &sha256_hex(&capsule.record_id.0.to_le_bytes()),
            &receipt,
            created_tick,
        )?;
    }
    for event in events {
        insert_event(
            &tx,
            event.seq,
            &event.kind,
            event.idempotency_key.as_deref(),
            &event.payload_sha256,
            &event.receipt,
            event.created_tick,
        )?;
    }
    for revocation in revocations {
        tx.execute(
            "INSERT INTO revocations(source_id, epoch, seq) VALUES (?1, ?2, ?3)",
            params![
                revocation.source_id.0,
                sqlite_i64(revocation.epoch, "revocation epoch")?,
                sqlite_i64(revocation.seq, "revocation sequence")?
            ],
        )
        .map_err(|error| unavailable_reply(0, &format!("insert snapshot revocation: {error}")))?;
    }
    if let Some(fence) = fence_rows {
        insert_event(
            &tx,
            fence.seq,
            "deletion_fence",
            None,
            &sha256_hex(fence.receipt.as_bytes()),
            fence.receipt,
            kernel.scheduler.tick.0,
        )?;
        tx.execute("INSERT INTO fence(id, reason) VALUES (1, 'rebuilding')", [])
            .map_err(|error| {
                unavailable_reply(0, &format!("set snapshot restore fence: {error}"))
            })?;
    }
    if let Some(final_rows) = final_rows {
        insert_event(
            &tx,
            final_rows.seq,
            "snapshot_restore",
            Some(final_rows.idempotency_key),
            final_rows.payload_sha256,
            final_rows.receipt,
            kernel.scheduler.tick.0,
        )?;
        tx.execute(
            "INSERT INTO checkpoints(seq, epoch, state) VALUES (?1, ?2, ?3)",
            params![
                sqlite_i64(final_rows.seq, "checkpoint sequence")?,
                sqlite_i64(epoch, "checkpoint epoch")?,
                final_rows.checkpoint
            ],
        )
        .map_err(|error| unavailable_reply(0, &format!("insert snapshot checkpoint: {error}")))?;
    }
    set_meta(&tx, "epoch", epoch)?;
    set_meta(&tx, "commit_seq", commit_seq)?;
    set_meta(&tx, "tick", kernel.scheduler.tick.0)?;
    set_meta_f32(&tx, "fatigue", kernel.scheduler.fatigue)?;
    set_meta(
        &tx,
        "steps_since_consolidation",
        kernel.scheduler.steps_since_consolidation,
    )?;
    set_meta_text(&tx, "last_maintenance", "snapshot_restore")?;
    set_meta(&tx, "next_record_id", next_record_id)?;
    tx.commit().map_err(|error| {
        unavailable_reply(0, &format!("commit snapshot staging database: {error}"))
    })?;
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .map_err(|error| {
            unavailable_reply(0, &format!("checkpoint snapshot staging database: {error}"))
        })?;
    Ok(())
}

fn insert_event(
    tx: &rusqlite::Transaction<'_>,
    seq: u64,
    kind: &str,
    idempotency_key: Option<&str>,
    payload_sha256: &str,
    receipt: &str,
    created_tick: u64,
) -> Result<(), EngineReply> {
    tx.execute(
        "INSERT INTO events(seq, kind, idempotency_key, payload_sha256, receipt, created_tick)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            sqlite_i64(seq, "event sequence")?,
            kind,
            idempotency_key,
            payload_sha256,
            receipt,
            sqlite_i64(created_tick, "event tick")?
        ],
    )
    .map_err(|error| unavailable_reply(0, &format!("insert snapshot event: {error}")))?;
    Ok(())
}

fn replace_restore_receipt(
    db_path: &Path,
    seq: u64,
    idempotency_key: &str,
    payload_sha256: &str,
    reply: &EngineReply,
    kernel: &NcmKernel,
) -> Result<(), EngineReply> {
    let receipt = restore_receipt(reply, kernel)?;
    let conn = Connection::open(db_path).map_err(|error| {
        unavailable_reply(seq, &format!("open staged restore receipt: {error}"))
    })?;
    let changed = conn
        .execute(
            "UPDATE events SET kind = 'snapshot_restore', idempotency_key = ?1,
                    payload_sha256 = ?2, receipt = ?3 WHERE seq = ?4",
            params![
                idempotency_key,
                payload_sha256,
                receipt,
                sqlite_i64(seq, "restore receipt sequence")?
            ],
        )
        .map_err(|error| {
            unavailable_reply(seq, &format!("update staged restore receipt: {error}"))
        })?;
    if changed != 1 {
        return Err(corrupt_reply(seq, "staged restore receipt is missing"));
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .map_err(|error| {
            unavailable_reply(seq, &format!("checkpoint staged restore receipt: {error}"))
        })?;
    Ok(())
}

fn lookup_restore_replay(
    handle: &mut NamespaceHandle,
    key: &str,
    payload_sha256: &str,
) -> Result<Option<EngineReply>, EngineReply> {
    let mutation = handle
        .store
        .begin_mutation()
        .map_err(|error| store_reply(error, handle.commit_seq))?;
    let found = mutation
        .lookup_idempotency(key)
        .map_err(|error| store_reply(error, handle.commit_seq))?;
    drop(mutation);
    let Some((stored_digest, receipt, seq)) = found else {
        return Ok(None);
    };
    if stored_digest != payload_sha256 {
        return Ok(Some(EngineReply::rejected(
            RejectReason::IdempotencyConflict,
            handle.commit_seq,
        )));
    }
    let durable: DurableReceipt = serde_json::from_str(&receipt)
        .map_err(|error| corrupt_reply(seq, &format!("decode restore receipt: {error}")))?;
    let mut reply = durable.reply;
    if let Some(object) = reply.payload.as_object_mut() {
        object.insert("replayed".to_owned(), Value::Bool(true));
    }
    Ok(Some(reply))
}

fn restore_receipt(reply: &EngineReply, kernel: &NcmKernel) -> Result<String, EngineReply> {
    serde_json::to_string(&DurableReceipt {
        reply: reply.clone(),
        operation: DurableOperation::Maintenance {
            kind: MaintenanceKind::Checkpoint,
        },
        state_digest: sha256_hex(&kernel.state_digest()),
    })
    .map_err(|error| {
        corrupt_reply(
            reply.state_generation,
            &format!("serialize restore receipt: {error}"),
        )
    })
}

fn checkpoint_bytes(kernel: &NcmKernel) -> Result<Vec<u8>, EngineReply> {
    serde_json::to_vec(&CheckpointEnvelope {
        kernel: kernel.clone(),
        state_digest: sha256_hex(&kernel.state_digest()),
    })
    .map_err(|error| corrupt_reply(0, &format!("serialize restore checkpoint: {error}")))
}

fn kernel_next_record_id(kernel: &NcmKernel) -> Result<u64, EngineReply> {
    let value = serde_json::to_value(&kernel.records)
        .map_err(|error| corrupt_reply(0, &format!("serialize record table: {error}")))?;
    value
        .get("next_id")
        .and_then(Value::as_u64)
        .ok_or_else(|| rejected("snapshot record table next identity is missing"))
}

fn create_stage(root: &Path, namespace: &str, digest: &str) -> Result<StageGuard, String> {
    for attempt in 0_u32..100 {
        let name = format!(
            "{STAGING_PREFIX}-{}-{}-{attempt}",
            std::process::id(),
            digest.get(..16).unwrap_or(digest)
        );
        let path = root.join(name);
        match fs::create_dir(&path) {
            Ok(()) => return Ok(StageGuard { root: path }),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(format!("create snapshot staging root: {error}")),
        }
    }
    Err(format!(
        "create snapshot staging root for namespace {}",
        namespace.get(..8).unwrap_or(namespace)
    ))
}

fn publish_stage(root: &Path, namespace: &str, stage_db: &Path) -> Result<(), String> {
    let namespaces = root.join("namespaces");
    fs::create_dir_all(&namespaces)
        .map_err(|error| format!("create namespace catalog: {error}"))?;
    let target_dir = namespaces.join(namespace);
    let stage_dir = stage_db
        .parent()
        .ok_or_else(|| "snapshot staging database has no parent".to_owned())?;
    if target_dir.exists() {
        let target_db = target_dir.join("ncm.sqlite");
        let _ = fs::remove_file(target_dir.join("ncm.sqlite-wal"));
        let _ = fs::remove_file(target_dir.join("ncm.sqlite-shm"));
        fs::rename(stage_db, &target_db)
            .map_err(|error| format!("atomically replace namespace database: {error}"))?;
        let _ = fs::remove_dir(stage_dir);
    } else {
        fs::rename(stage_dir, &target_dir)
            .map_err(|error| format!("atomically publish namespace directory: {error}"))?;
    }
    if let Ok(directory) = fs::File::open(&namespaces) {
        let _ = directory.sync_all();
    }
    Ok(())
}

fn set_meta(conn: &Connection, key: &str, value: u64) -> Result<(), EngineReply> {
    set_meta_text(conn, key, &value.to_string())
}

fn set_meta_f32(conn: &Connection, key: &str, value: f32) -> Result<(), EngineReply> {
    if !value.is_finite() {
        return Err(rejected("snapshot metadata contains non-finite fatigue"));
    }
    set_meta_text(conn, key, &value.to_string())
}

fn set_meta_text(conn: &Connection, key: &str, value: &str) -> Result<(), EngineReply> {
    let changed = conn
        .execute(
            "UPDATE meta SET value = ?1 WHERE key = ?2",
            params![value.as_bytes(), key],
        )
        .map_err(|error| unavailable_reply(0, &format!("write snapshot metadata: {error}")))?;
    if changed != 1 {
        return Err(corrupt_reply(0, "snapshot staging metadata key is missing"));
    }
    Ok(())
}

fn f32_blob(values: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(std::mem::size_of_val(values));
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

fn status_name(status: CapsuleStatus) -> &'static str {
    match status {
        CapsuleStatus::Valid => "valid",
        CapsuleStatus::Superseded => "superseded",
        CapsuleStatus::Revoked => "revoked",
    }
}

fn namespace_seed(namespace: &str) -> Option<u64> {
    if namespace.len() != 64 {
        return None;
    }
    let mut bytes = [0_u8; 8];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let offset = index * 2;
        *byte = u8::from_str_radix(namespace.get(offset..offset + 2)?, 16).ok()?;
    }
    Some(u64::from_be_bytes(bytes))
}

fn validate_idempotency_key(key: &str) -> Result<(), String> {
    if key.is_empty() {
        return Err("idempotency key cannot be empty".to_owned());
    }
    if key.len() > 256 {
        return Err("idempotency key exceeds 256 bytes".to_owned());
    }
    Ok(())
}

fn sqlite_i64(value: u64, what: &str) -> Result<i64, EngineReply> {
    i64::try_from(value).map_err(|_| rejected(&format!("{what} does not fit SQLite INTEGER")))
}

fn to_u64(value: usize, what: &str) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| format!("{what} length overflow"))
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn rejected(reason: &str) -> EngineReply {
    EngineReply::rejected(RejectReason::InvalidRequest(reason.to_owned()), 0)
}

fn corrupt_reply(commit_seq: u64, reason: &str) -> EngineReply {
    EngineReply::new(Outcome::Corrupt, commit_seq, json!({"reason": reason}))
}

fn unavailable_reply(commit_seq: u64, reason: &str) -> EngineReply {
    EngineReply::new(
        Outcome::Unavailable(reason.to_owned()),
        commit_seq,
        Value::Null,
    )
}

fn store_reply(error: crate::store::StoreError, commit_seq: u64) -> EngineReply {
    match error {
        crate::store::StoreError::Busy => EngineReply::new(Outcome::Busy, commit_seq, Value::Null),
        crate::store::StoreError::Corrupt(reason) => corrupt_reply(commit_seq, &reason),
        crate::store::StoreError::Incompatible { .. } => {
            EngineReply::new(Outcome::Incompatible, commit_seq, Value::Null)
        }
        crate::store::StoreError::BudgetExceeded => {
            EngineReply::new(Outcome::BudgetExceeded, commit_seq, Value::Null)
        }
        crate::store::StoreError::IdempotencyConflict => {
            EngineReply::rejected(RejectReason::IdempotencyConflict, commit_seq)
        }
        crate::store::StoreError::InvalidInput(reason)
        | crate::store::StoreError::InvalidNamespace(reason) => {
            EngineReply::rejected(RejectReason::InvalidRequest(reason), commit_seq)
        }
        crate::store::StoreError::UnknownRecord(record_id) => {
            EngineReply::rejected(RejectReason::UnknownRecord(record_id), commit_seq)
        }
        crate::store::StoreError::Missing => {
            EngineReply::new(Outcome::Empty, commit_seq, Value::Null)
        }
        crate::store::StoreError::AlreadyExists => {
            EngineReply::new(Outcome::Busy, commit_seq, Value::Null)
        }
        crate::store::StoreError::Io(reason) | crate::store::StoreError::Sqlite(reason) => {
            unavailable_reply(commit_seq, &reason)
        }
    }
}
