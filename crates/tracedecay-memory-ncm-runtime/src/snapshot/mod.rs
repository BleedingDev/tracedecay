//! Versioned, bounded NCM state export and atomic restore.
//!
//! Snapshots carry learned state, replay inputs, and authenticated source
//! revocation authority. Restore intersects imported capsules with the
//! revocations trusted by the destination before publication.

use crate::engine::{
    CheckpointEnvelope, DurableOperation, DurableReceipt, EngineReply, MaintenanceKind,
    NamespaceHandle, NcmEngine, Outcome, RejectReason, canonical_digest, durable_integrity_digest,
    portable_common_maintenance_event, validate_event_payload_digest,
    validate_receipt_idempotency_key_json,
};
use crate::ports::{Deadline, StateRoot};
use crate::store::{
    CapsuleStatus, Event, NamespaceStore, Revocation, StoreIdentity, StoreMeta, StoredCapsule,
};
use rusqlite::{Connection, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tracedecay_memory_ncm_core::centers::MemoryCenters;
use tracedecay_memory_ncm_core::kernel::NcmKernel;
use tracedecay_memory_ncm_core::projections::ProjectionBundle;
use tracedecay_memory_ncm_core::records::RecordState;
use tracedecay_memory_ncm_core::types::{
    AFFECT_DIM, AlgorithmIdentity, CONTEXT_DIM, EMBEDDING_DIM, LTM_KEY_DIM, NcmConfig, RecordId,
    SourceId, TERRAIN_DIM, VALUE_DIM,
};

const FORMAT: &str = "ncm-snapshot.v1";
const MAX_SNAPSHOT_BYTES: usize = 256 * 1024 * 1024;
const STAGING_PREFIX: &str = ".ncm-snapshot-staging";
const MAX_SYNTHETIC_GAP_EVENTS: u64 = 65_536;
const MAX_SYNTHETIC_ADVANCE_TICKS: u32 = 10_000;
static NEXT_TRANSPORT_FILE: AtomicU64 = AtomicU64::new(1);

/// Owned bytes in the `ncm-snapshot.v1` JSON envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotBytes {
    bytes: Vec<u8>,
    state_generation: u64,
    state_epoch: u64,
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

    const fn state_epoch(&self) -> u64 {
        self.state_epoch
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
    /// Durable source revocations bound to deletion-fence journal receipts.
    ///
    /// The default keeps snapshots written before revocation export readable;
    /// worker restores still require an explicit deletion-authority inventory
    /// so an old snapshot cannot silently resurrect a deleted source.
    #[serde(default)]
    revocations: Vec<Revocation>,
    lengths: SnapshotLengths,
}

/// The pre-revocation snapshot content shape, retained only to verify the
/// checksum of envelopes written before the authority field was introduced.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacySnapshotContent {
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
    revocation_authority_present: bool,
}

struct StageGuard {
    root: PathBuf,
}

impl Drop for StageGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

/// Exports one materialized namespace together with its authenticated
/// revocation/deletion authority.
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
        .capsules_in_commit_order(true)
        .map_err(|error| store_reply(error, handle.commit_seq))?;
    let revocations = handle
        .store
        .revocations()
        .map_err(|error| store_reply(error, handle.commit_seq))?;
    let retained_events = handle
        .store
        .events_after(0)
        .map_err(|error| store_reply(error, handle.commit_seq))?;
    let mut events = Vec::new();
    for event in retained_events {
        validate_receipt_idempotency_key_json(
            &event.receipt,
            &event.kind,
            event.idempotency_key.as_deref(),
        )
        .map_err(|reason| corrupt_reply(handle.commit_seq, &reason))?;
        let common_maintenance = portable_common_maintenance_event(namespace, &event)
            .map_err(|reason| corrupt_reply(handle.commit_seq, &reason))?;
        if common_maintenance
            || matches!(
                event.kind.as_str(),
                "feedback"
                    | "correction"
                    | "common_control"
                    | "deletion_fence"
                    | "delete_by_source"
                    | "snapshot_restore"
            )
            || (event.kind == "observe"
                && capsules
                    .iter()
                    .any(|capsule| capsule.commit_seq == event.seq))
        {
            events.push(event);
        }
    }
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
        revocations,
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
        state_epoch: meta.epoch,
    })
}

/// Exports one snapshot through an atomically published namespace-local file.
///
/// The reply contains only bounded file metadata; the supervising client reads,
/// verifies, deletes, and hydrates the snapshot bytes before returning upstream.
#[must_use]
pub fn export_to_file(engine: &NcmEngine, namespace: &str, deadline: Deadline) -> EngineReply {
    let snapshot = match export(engine, namespace, deadline) {
        Ok(snapshot) => snapshot,
        Err(reply) => return reply,
    };
    let state_generation = snapshot.state_generation();
    let state_epoch = snapshot.state_epoch();
    let byte_length = match u64::try_from(snapshot.as_slice().len()) {
        Ok(length) => length,
        Err(_) => return unavailable_reply(state_generation, "snapshot length overflow"),
    };
    let content_sha256 = sha256_hex(snapshot.as_slice());
    let directory = match transport_directory(&engine.root, namespace) {
        Ok(directory) => directory,
        Err(reason) => return rejected(&reason),
    };
    if let Err(error) = fs::create_dir_all(&directory) {
        return unavailable_reply(
            state_generation,
            &format!("create snapshot transport directory: {error}"),
        );
    }
    let serial = NEXT_TRANSPORT_FILE.fetch_add(1, Ordering::Relaxed);
    let file_name = format!("{state_epoch}-{state_generation}-{serial}.ncm-snapshot.v1.json");
    let snapshot_file = directory.join(file_name);
    let temporary_file = directory.join(format!(
        ".snapshot-{state_epoch}-{state_generation}-{serial}.tmp"
    ));
    if let Err(reason) = atomic_write(&temporary_file, &snapshot_file, snapshot.as_slice()) {
        return unavailable_reply(state_generation, &reason);
    }
    EngineReply::new(
        Outcome::Success,
        state_generation,
        json!({
            "format": FORMAT,
            "snapshot_file": snapshot_file,
            "byte_length": byte_length,
            "content_sha256": content_sha256,
            "state_generation": state_generation
        }),
    )
}

/// Restores a snapshot from a verified namespace-local transport file.
///
/// An admitted file is deleted after the read attempt, including digest or
/// length failures, so transport artifacts do not accumulate.
#[must_use]
pub fn restore_from_file(
    engine: &NcmEngine,
    namespace: &str,
    idempotency_key: &str,
    snapshot_file: &Path,
    byte_length: u64,
    content_sha256: &str,
    deadline: Deadline,
) -> EngineReply {
    restore_from_file_with_revocations(
        engine,
        namespace,
        idempotency_key,
        snapshot_file,
        byte_length,
        content_sha256,
        &[],
        deadline,
    )
}

pub(crate) fn restore_from_file_with_revocations(
    engine: &NcmEngine,
    namespace: &str,
    idempotency_key: &str,
    snapshot_file: &Path,
    byte_length: u64,
    content_sha256: &str,
    blocked_sources: &[SourceId],
    deadline: Deadline,
) -> EngineReply {
    let bytes = match read_transport_file(
        &engine.root,
        namespace,
        snapshot_file,
        byte_length,
        content_sha256,
    ) {
        Ok(bytes) => bytes,
        Err(reason) => return rejected(&reason),
    };
    restore_with_revocations(
        engine,
        namespace,
        RestoreRequest {
            idempotency_key: idempotency_key.to_owned(),
            bytes,
        },
        deadline,
        blocked_sources,
        None,
    )
}

fn transport_directory(root: &StateRoot, namespace: &str) -> Result<PathBuf, String> {
    root.namespace_dir(namespace)
        .map(|path| path.join("snapshots"))
}

fn atomic_write(temporary_file: &Path, destination: &Path, bytes: &[u8]) -> Result<(), String> {
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(temporary_file)
            .map_err(|error| format!("create snapshot transport file: {error}"))?;
        file.write_all(bytes)
            .map_err(|error| format!("write snapshot transport file: {error}"))?;
        file.sync_all()
            .map_err(|error| format!("sync snapshot transport file: {error}"))?;
        fs::rename(temporary_file, destination)
            .map_err(|error| format!("publish snapshot transport file: {error}"))
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary_file);
    }
    result
}

pub(crate) fn read_transport_file(
    root: &StateRoot,
    namespace: &str,
    snapshot_file: &Path,
    byte_length: u64,
    content_sha256: &str,
) -> Result<Vec<u8>, String> {
    if !snapshot_file.is_absolute() {
        return Err("snapshot transport file must be absolute".to_owned());
    }
    if !is_sha256_hex(content_sha256) {
        return Err("snapshot transport digest must be lowercase sha256 hex".to_owned());
    }
    let expected_length = usize::try_from(byte_length)
        .map_err(|_| "snapshot transport length overflow".to_owned())?;
    if expected_length == 0 || expected_length > MAX_SNAPSHOT_BYTES {
        return Err("snapshot transport length is outside the snapshot budget".to_owned());
    }
    let directory = transport_directory(root, namespace)?;
    let canonical_directory = fs::canonicalize(&directory)
        .map_err(|error| format!("open snapshot transport directory: {error}"))?;
    let canonical_file = fs::canonicalize(snapshot_file)
        .map_err(|error| format!("open snapshot transport file: {error}"))?;
    if !canonical_file.starts_with(&canonical_directory)
        || canonical_file.parent() != Some(canonical_directory.as_path())
    {
        return Err("snapshot transport file escapes the namespace snapshot directory".to_owned());
    }
    let _guard = TransportFileGuard {
        path: canonical_file.clone(),
    };
    let file = File::open(&canonical_file)
        .map_err(|error| format!("open snapshot transport file: {error}"))?;
    let actual_length = file
        .metadata()
        .map_err(|error| format!("inspect snapshot transport file: {error}"))?
        .len();
    if actual_length != byte_length {
        return Err("snapshot transport byte length mismatch".to_owned());
    }
    let read_limit = byte_length
        .checked_add(1)
        .ok_or_else(|| "snapshot transport read limit overflow".to_owned())?;
    let mut bytes = Vec::with_capacity(expected_length);
    file.take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("read snapshot transport file: {error}"))?;
    if bytes.len() != expected_length {
        return Err("snapshot transport byte length changed during read".to_owned());
    }
    if sha256_hex(&bytes) != content_sha256 {
        return Err("snapshot transport content digest mismatch".to_owned());
    }
    Ok(bytes)
}

struct TransportFileGuard {
    path: PathBuf,
}

impl Drop for TransportFileGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Restores through a staging store, applying destination-authoritative revocations.
#[must_use]
pub fn restore(
    engine: &NcmEngine,
    namespace: &str,
    request: RestoreRequest,
    deadline: Deadline,
) -> EngineReply {
    restore_with_revocations(engine, namespace, request, deadline, &[], None)
}

pub(crate) fn restore_with_revocations(
    engine: &NcmEngine,
    namespace: &str,
    request: RestoreRequest,
    deadline: Deadline,
    blocked_sources: &[SourceId],
    expected_generation: Option<u64>,
) -> EngineReply {
    if deadline.remaining_ms == 0 {
        return EngineReply::new(Outcome::Cancelled, 0, Value::Null);
    }
    if let Err(reason) = validate_idempotency_key(&request.idempotency_key) {
        return EngineReply::rejected(RejectReason::InvalidRequest(reason), 0);
    }
    let snapshot_digest = sha256_hex(&request.bytes);
    let mut validated = match validate_snapshot(engine, namespace, &request.bytes) {
        Ok(snapshot) => snapshot,
        Err(reply) => return reply,
    };
    if blocked_sources.len() > 1024
        || blocked_sources.iter().any(|source| source.0.is_empty())
        || blocked_sources.iter().collect::<BTreeSet<_>>().len() != blocked_sources.len()
    {
        return rejected("invalid blocked restore inventory");
    }
    if !validated.revocation_authority_present
        && blocked_sources.is_empty()
        && !NamespaceStore::exists(&engine.root, namespace)
    {
        return rejected("snapshot deletion authority is missing");
    }
    let mut canonical_blocked_sources = blocked_sources.to_vec();
    canonical_blocked_sources.sort();
    let restore_digest = match restore_identity_digest(
        &snapshot_digest,
        &canonical_blocked_sources,
        &validated.content.revocations,
    ) {
        Ok(digest) => digest,
        Err(reason) => return corrupt_reply(0, &reason),
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
        // A published bootstrap store is an occupied namespace even before
        // its first event.  Refuse to hydrate a snapshot over that empty
        // identity: doing so would make a stale snapshot appear to resurrect
        // a namespace that already had a durable owner.
        let existing_meta = match existing.meta() {
            Ok(meta) => meta,
            Err(error) => return store_reply(error, 0),
        };
        if existing_meta.commit_seq == 0 {
            return rejected("snapshot restore target namespace is already occupied");
        }
    }
    let current = match engine.ensure_handle(&mut namespaces, namespace, false) {
        Ok(current) => current,
        Err(reply) => return reply,
    };
    let (current_seq, current_epoch, next_record_id, mut revocations) =
        if let Some(handle) = current {
            if handle.fenced {
                return EngineReply::new(Outcome::Busy, handle.commit_seq, Value::Null);
            }
            if handle.commit_seq == 0 {
                return rejected("snapshot restore target namespace is already occupied");
            }
            let revocations = match handle.store.revocations() {
                Ok(revocations) => revocations,
                Err(error) => return store_reply(error, handle.commit_seq),
            };
            match lookup_restore_replay(handle, &request.idempotency_key, &restore_digest) {
                Ok(Some(reply)) => {
                    if reply.outcome == Outcome::Success
                        && !destination_authority_matches(
                            &revocations,
                            &validated.content.revocations,
                            &canonical_blocked_sources,
                            reply.payload["deletion_authority_sha256"].as_str(),
                        )
                    {
                        return corrupt_reply(
                            handle.commit_seq,
                            "restore replay deletion authority differs from its receipt",
                        );
                    }
                    return reply;
                }
                Ok(None) => {}
                Err(reply) => return reply,
            }
            let next_record_id = match handle.store.next_record_id() {
                Ok(next_record_id) => next_record_id,
                Err(error) => return store_reply(error, handle.commit_seq),
            };
            (handle.commit_seq, handle.epoch, next_record_id, revocations)
        } else {
            (0, 0, 1, Vec::new())
        };
    // A legacy envelope has no authenticated source revocation authority.  It
    // is safe to admit only when the destination is genuinely fresh or has no
    // revocation history.  An existing namespace may have no capsules after a
    // completed deletion, but its revocation rows still prove that importing a
    // legacy kernel could resurrect erased content.
    if !validated.revocation_authority_present
        && blocked_sources.is_empty()
        && !revocations.is_empty()
    {
        return rejected("snapshot deletion authority is missing for existing namespace");
    }
    if expected_generation.is_some_and(|expected| expected != current_seq) {
        return EngineReply::rejected(RejectReason::IdempotencyConflict, current_seq);
    }
    if let Err(reply) = merge_revocations(
        &mut revocations,
        &validated.content.revocations,
        current_seq,
    ) {
        return reply;
    }
    // Rollback discards records, but their saved numeric targets must never
    // identify a later observation. Stage the destination's allocation floor
    // with the imported kernel so the store, checkpoint, and live state agree.
    if let Err(reply) = preserve_record_identity_floor(&mut validated.kernel, next_record_id) {
        return reply;
    }
    let target_epoch = match current_epoch.max(validated.content.epoch).checked_add(1) {
        Some(epoch) => epoch,
        None => return corrupt_reply(current_seq, "snapshot restore epoch overflow"),
    };
    let new_revocation_seq = match current_seq.max(validated.content.commit_seq).checked_add(1) {
        Some(seq) => seq,
        None => return corrupt_reply(current_seq, "snapshot restore sequence overflow"),
    };
    let mut authority_fence_sources = BTreeSet::new();
    for source in &canonical_blocked_sources {
        if !revocations.iter().any(|row| &row.source_id == source) {
            revocations.push(Revocation {
                source_id: source.clone(),
                epoch: target_epoch,
                seq: new_revocation_seq,
            });
        }
        // An explicit blocked source always requires a fenced rebuild, even
        // when the destination already carries that authority row.  The
        // existing row will be rebound to the new import fence below.
        authority_fence_sources.insert(source.clone());
    }
    let revoked = revocations
        .iter()
        .map(|row| row.source_id.clone())
        .collect::<BTreeSet<_>>();
    let mut stripped = BTreeSet::new();
    for capsule in &validated.content.capsules {
        if capsule.status == CapsuleStatus::Revoked {
            continue;
        }
        let binding = match crate::source_binding::read_text(
            namespace,
            &capsule.source_id,
            &capsule.provenance,
        ) {
            Ok(binding) => binding,
            Err(reason) => return corrupt_reply(current_seq, &reason),
        };
        if let Some(binding) = &binding {
            if binding.is_legacy
                && match &binding.full_source_id {
                    Some(full) => revoked.contains(full),
                    None => !revoked.is_empty(),
                }
            {
                return EngineReply::rejected(
                    RejectReason::InvalidRequest(
                        "legacy snapshot source cannot cross current full-source revocation"
                            .to_owned(),
                    ),
                    current_seq,
                );
            }
        }
        if revoked.contains(&capsule.source_id)
            || binding
                .as_ref()
                .is_some_and(|binding| revoked.contains(&binding.legacy_source_id))
        {
            stripped.insert(capsule.source_id.clone());
            // Keep the scrubbed capsule's stored identity separate from the
            // authority identity that fenced it.  A raw deletion can revoke
            // several typed origins sharing one legacy key; the synthetic
            // fence must retain that single legacy authority row while its
            // deleted-record set covers every scrubbed capsule.
            if let Some(authority) = revocations.iter().find(|row| {
                row.source_id == capsule.source_id
                    || binding
                        .as_ref()
                        .is_some_and(|binding| row.source_id == binding.legacy_source_id)
            }) {
                authority_fence_sources.insert(authority.source_id.clone());
            }
        }
    }
    // An authority inventory may name a source that is absent from this
    // snapshot. Persist its revocation through the same fenced rebuild path so
    // the resulting snapshot remains journal-backed and cannot later be
    // mistaken for an unauthenticated row.
    for source in &stripped {
        if validated
            .content
            .revocations
            .iter()
            .any(|row| &row.source_id == source)
        {
            return corrupt_reply(
                current_seq,
                "snapshot active capsule conflicts with imported revocation authority",
            );
        }
    }
    for source in &authority_fence_sources {
        let Some(row) = revocations.iter_mut().find(|row| &row.source_id == source) else {
            return corrupt_reply(
                current_seq,
                "snapshot deletion authority source is missing from its fence",
            );
        };
        // The destination row is authoritative for a sanitized import, so
        // rebind it to the synthetic fence that will erase the staged
        // capsule. Imported rows were rejected above because their completed
        // receipts must retain their original (epoch, seq).
        row.epoch = target_epoch;
        row.seq = new_revocation_seq;
    }
    revocations.sort_by(|left, right| left.source_id.cmp(&right.source_id));
    let base_seq = current_seq.max(validated.content.commit_seq);
    // A sanitized import commits a fence, then its immutable deletion
    // completion, then a separate restore checkpoint.  Keep the completion
    // row intact: its receipt is the authority that proves which fence and
    // full deleted-record set were applied.
    let target_seq = match base_seq.checked_add(
        if stripped.is_empty() && authority_fence_sources.is_empty() {
            1
        } else {
            3
        },
    ) {
        Some(seq) => seq,
        None => return corrupt_reply(current_seq, "snapshot restore sequence overflow"),
    };
    let deletion_authority_sha256 = match deletion_authority_digest(
        &revocations,
        &validated.content.revocations,
        &canonical_blocked_sources,
    ) {
        Ok(digest) => digest,
        Err(reason) => return corrupt_reply(current_seq, &reason),
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
    let build = if stripped.is_empty() && authority_fence_sources.is_empty() {
        build_direct_stage(
            &stage_db,
            &validated,
            &revocations,
            target_epoch,
            target_seq,
            &request.idempotency_key,
            &restore_digest,
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
            &authority_fence_sources,
            target_epoch,
            base_seq,
            target_seq,
            &request.idempotency_key,
            &restore_digest,
            &deletion_authority_sha256,
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
            "deletion_authority_sha256": deletion_authority_sha256,
            "replayed": false
        }),
    );
    if let Err(error) = replace_restore_receipt(
        &stage_db,
        target_seq,
        &request.idempotency_key,
        &restore_digest,
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
    let raw: Value = serde_json::from_slice(bytes)
        .map_err(|error| rejected(&format!("decode snapshot envelope: {error}")))?;
    let revocation_authority_present = raw.get("revocations").is_some();
    let mut raw_content = raw.clone();
    if let Some(object) = raw_content.as_object_mut() {
        object.remove("content_sha256");
    }
    let envelope: SnapshotEnvelope = serde_json::from_value(raw)
        .map_err(|error| rejected(&format!("decode snapshot envelope: {error}")))?;
    let content_bytes = if revocation_authority_present {
        serde_json::to_vec(&envelope.content)
            .map_err(|error| rejected(&format!("serialize snapshot checksum content: {error}")))?
    } else {
        let legacy: LegacySnapshotContent = serde_json::from_value(raw_content)
            .map_err(|error| rejected(&format!("decode legacy snapshot content: {error}")))?;
        serde_json::to_vec(&legacy).map_err(|error| {
            rejected(&format!(
                "serialize legacy snapshot checksum content: {error}"
            ))
        })?
    };
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
    validate_revocations(&envelope.content)?;
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
        revocation_authority_present,
    })
}

#[derive(Serialize)]
struct RestoreIdentity<'a> {
    snapshot_sha256: &'a str,
    blocked_sources: &'a [SourceId],
    revocations: &'a [Revocation],
}

fn restore_identity_digest(
    snapshot_digest: &str,
    blocked_sources: &[SourceId],
    revocations: &[Revocation],
) -> Result<String, String> {
    canonical_digest(&RestoreIdentity {
        snapshot_sha256: snapshot_digest,
        blocked_sources,
        revocations,
    })
}

fn merge_revocations(
    destination: &mut Vec<Revocation>,
    imported: &[Revocation],
    commit_seq: u64,
) -> Result<(), EngineReply> {
    for expected in imported {
        if let Some(actual) = destination
            .iter()
            .find(|actual| actual.source_id == expected.source_id)
        {
            if actual != expected {
                return Err(EngineReply::rejected(
                    RejectReason::InvalidRequest(
                        "snapshot revocation authority conflicts with destination".to_owned(),
                    ),
                    commit_seq,
                ));
            }
        } else {
            destination.push(expected.clone());
        }
    }
    destination.sort_by(|left, right| left.source_id.cmp(&right.source_id));
    Ok(())
}

fn destination_authority_matches(
    destination: &[Revocation],
    imported: &[Revocation],
    blocked_sources: &[SourceId],
    expected_digest: Option<&str>,
) -> bool {
    let Some(expected_digest) = expected_digest.filter(|digest| is_sha256_hex(digest)) else {
        return false;
    };
    let Some(actual_digest) =
        deletion_authority_digest(destination, imported, blocked_sources).ok()
    else {
        return false;
    };
    expected_digest == actual_digest
}

fn deletion_authority_digest(
    destination: &[Revocation],
    imported: &[Revocation],
    blocked_sources: &[SourceId],
) -> Result<String, String> {
    let sources = imported
        .iter()
        .map(|row| row.source_id.clone())
        .chain(blocked_sources.iter().cloned())
        .collect::<BTreeSet<_>>();
    for source in sources {
        if !destination.iter().any(|row| row.source_id == source) {
            return Err(format!(
                "destination deletion authority is missing {source:?}"
            ));
        }
    }
    // Bind the complete destination inventory. A restore replay must observe
    // the same authority rows that governed its source intersection, while
    // the explicit subset checks above ensure every imported/fenced source is
    // represented before the inventory digest is admitted.
    canonical_digest(destination)
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
        let text_bytes = capsule
            .key_text
            .len()
            .checked_add(capsule.value_text.len())
            .ok_or_else(|| rejected("snapshot capsule text length overflow"))?;
        if capsule.source_id.0.is_empty()
            || capsule.commit_seq == 0
            || capsule.commit_seq > content.commit_seq
            || !capsule.affect.0.iter().all(|value| value.is_finite())
            || !capsule.surprise.is_finite()
            || !capsule.intensity.is_finite()
        {
            return Err(rejected("snapshot capsule is invalid"));
        }
        if capsule.status == CapsuleStatus::Revoked {
            if !capsule.key_text.is_empty()
                || !capsule.value_text.is_empty()
                || !capsule.key_embedding.is_empty()
                || !capsule.value_embedding.is_empty()
                || !capsule.ltm_key.is_empty()
                || capsule.provenance != "{}"
            {
                return Err(rejected("snapshot revoked capsule is not scrubbed"));
            }
        } else if text_bytes > config.max_record_bytes
            || capsule.key_embedding.len() != EMBEDDING_DIM
            || capsule.value_embedding.len() != EMBEDDING_DIM
            || capsule.ltm_key.len() != LTM_KEY_DIM
            || !capsule
                .key_embedding
                .iter()
                .chain(&capsule.value_embedding)
                .chain(&capsule.ltm_key)
                .all(|value| value.is_finite())
            || serde_json::from_str::<Value>(&capsule.provenance).is_err()
        {
            return Err(rejected("snapshot capsule is invalid"));
        }
    }
    let active_capsules = content
        .capsules
        .iter()
        .filter(|capsule| capsule.status != CapsuleStatus::Revoked)
        .collect::<Vec<_>>();
    if kernel.records.len() != active_capsules.len() {
        return Err(rejected(
            "snapshot capsule count does not match kernel records",
        ));
    }
    for (record_id, record) in kernel.records.iter() {
        let capsule = by_id
            .get(record_id)
            .ok_or_else(|| rejected("snapshot kernel record has no source capsule"))?;
        if capsule.status == CapsuleStatus::Revoked {
            return Err(rejected("snapshot kernel references a revoked capsule"));
        }
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
    let mut gap_contracts = Vec::new();
    for event in &content.events {
        if event.seq == 0 || event.seq > content.commit_seq || !sequences.insert(event.seq) {
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
        validate_receipt_idempotency_key_json(
            &event.receipt,
            &event.kind,
            event.idempotency_key.as_deref(),
        )
        .map_err(|reason| rejected(reason.as_str()))?;
        validate_snapshot_event_envelope(event, &receipt)?;
        validate_event_payload_digest(event, &receipt, &content.capsules).map_err(|reply| {
            rejected(&format!(
                "snapshot event payload is invalid: {:?}",
                reply.payload
            ))
        })?;
        let portable_maintenance = portable_common_maintenance_event(&content.namespace, event)
            .map_err(|reason| rejected(&reason))?;
        let snapshot_gap =
            crate::engine::snapshot_gap_contract(event, &receipt, content.commit_seq).map_err(
                |reply| {
                    rejected(
                        reply.payload["reason"]
                            .as_str()
                            .unwrap_or("snapshot gap contract is invalid"),
                    )
                },
            )?;
        if let Some(contract) = snapshot_gap.as_ref() {
            gap_contracts.push((event.seq, contract.clone()));
        }
        let portable_control = match &receipt.operation {
            DurableOperation::CommonControl { operations, .. } => {
                event.kind == "common_control"
                    && operations.iter().all(|operation| match operation {
                        DurableOperation::Observe { record_id } => {
                            content.capsules.iter().any(|capsule| {
                                capsule.record_id == *record_id && capsule.commit_seq == event.seq
                            })
                        }
                        DurableOperation::Feedback { .. } | DurableOperation::Correction { .. } => {
                            true
                        }
                        _ => false,
                    })
            }
            _ => false,
        };
        let owns_capsule = match &receipt.operation {
            DurableOperation::Observe { record_id } => content.capsules.iter().filter(|capsule| capsule.commit_seq == event.seq).count() == 1 && content.capsules.iter().any(|capsule| capsule.commit_seq == event.seq && capsule.record_id == *record_id),
            DurableOperation::CommonControl { operations, .. } => content.capsules.iter().filter(|capsule| capsule.commit_seq == event.seq).all(|capsule| operations.iter().filter(|operation| matches!(operation, DurableOperation::Observe { record_id } if *record_id == capsule.record_id)).count() == 1),
            _ => false,
        };
        if capsule_sequences.contains(&event.seq) && !owns_capsule {
            return Err(rejected("snapshot event overlaps a capsule sequence"));
        }
        if !portable_control
            && !portable_maintenance
            && snapshot_gap.is_none()
            && !matches!(
                (event.kind.as_str(), &receipt.operation),
                ("feedback", DurableOperation::Feedback { .. })
                    | ("correction", DurableOperation::Correction { .. })
                    | ("observe", DurableOperation::Observe { .. })
                    | ("deletion_fence", DurableOperation::DeletionFence { .. })
                    | ("delete_by_source", DurableOperation::DeleteBySource { .. })
                    | (
                        "snapshot_restore",
                        DurableOperation::Maintenance {
                            kind: MaintenanceKind::Checkpoint,
                            ..
                        }
                    )
            )
        {
            return Err(rejected("snapshot contains a non-portable event"));
        }
    }
    for (_, contract) in gap_contracts {
        let Some(checkpoint) = content
            .events
            .iter()
            .find(|event| event.seq == contract.checkpoint_sequence)
        else {
            return Err(rejected("snapshot gap omission checkpoint is missing"));
        };
        let receipt: DurableReceipt = serde_json::from_str(&checkpoint.receipt)
            .map_err(|error| rejected(&format!("decode snapshot gap checkpoint: {error}")))?;
        if checkpoint.kind != "snapshot_restore"
            || receipt.reply.payload["content_sha256"]
                != Value::String(contract.snapshot_content_sha256.clone())
        {
            return Err(rejected(
                "snapshot gap omission is detached from its restore checkpoint",
            ));
        }
    }
    Ok(())
}

fn validate_revocations(content: &SnapshotContent) -> Result<(), EngineReply> {
    let mut rows = BTreeMap::new();
    let mut prior_source = None;
    for revocation in &content.revocations {
        if revocation.source_id.0.is_empty()
            || revocation.epoch == 0
            || revocation.seq == 0
            || revocation.seq > content.commit_seq
            || prior_source
                .as_ref()
                .is_some_and(|prior: &SourceId| prior >= &revocation.source_id)
            || rows
                .insert(
                    revocation.source_id.clone(),
                    (revocation.epoch, revocation.seq),
                )
                .is_some()
        {
            return Err(rejected("snapshot revocation authority is invalid"));
        }
        prior_source = Some(revocation.source_id.clone());
    }

    let mut fences = BTreeMap::<u64, (u64, BTreeSet<SourceId>, BTreeSet<RecordId>)>::new();
    let mut completed_fences = BTreeSet::new();
    let mut all_deleted_record_ids = BTreeSet::new();
    for event in &content.events {
        let receipt: DurableReceipt = serde_json::from_str(&event.receipt)
            .map_err(|error| rejected(&format!("decode snapshot deletion receipt: {error}")))?;
        match &receipt.operation {
            DurableOperation::DeletionFence {
                source,
                sources,
                target_epoch,
                deleted_records,
                deleted_record_ids,
                ..
            } => {
                if event.kind != "deletion_fence"
                    || !valid_source_set(sources)
                    || sources.first() != Some(source)
                    || u64::try_from(deleted_record_ids.len()).ok() != Some(*deleted_records)
                    || deleted_record_ids.iter().collect::<BTreeSet<_>>().len()
                        != deleted_record_ids.len()
                    || !fences
                        .insert(
                            event.seq,
                            (
                                *target_epoch,
                                sources.iter().cloned().collect(),
                                deleted_record_ids.iter().copied().collect(),
                            ),
                        )
                        .is_none()
                {
                    return Err(rejected("snapshot deletion fence authority is invalid"));
                }
                // The durable source set is the deletion authority, while a
                // raw deletion may have scrubbed capsules whose stored IDs
                // are the distinct typed IDs derived from that raw key.  The
                // fence's complete record-ID set is therefore the stable
                // relation to validate here; source-ID equality would reject
                // a correctly scrubbed raw/typed snapshot.
                let expected_ids = content
                    .capsules
                    .iter()
                    .filter(|capsule| {
                        capsule.status == CapsuleStatus::Revoked
                            && !all_deleted_record_ids.contains(&capsule.record_id)
                    })
                    .map(|capsule| capsule.record_id)
                    .collect::<BTreeSet<_>>();
                let actual_ids = deleted_record_ids.iter().copied().collect::<BTreeSet<_>>();
                if actual_ids != expected_ids {
                    return Err(rejected(
                        "snapshot deletion fence does not carry the full deleted-record set",
                    ));
                }
                all_deleted_record_ids.extend(actual_ids);
            }
            DurableOperation::DeleteBySource {
                source,
                sources,
                target_epoch,
                deleted_records,
                deleted_record_ids,
                ..
            } => {
                let Some(previous) = event
                    .seq
                    .checked_sub(1)
                    .and_then(|seq| content.events.iter().find(|candidate| candidate.seq == seq))
                else {
                    return Err(rejected("snapshot deletion completion fence is missing"));
                };
                let previous_receipt: DurableReceipt = serde_json::from_str(&previous.receipt)
                    .map_err(|error| {
                        rejected(&format!(
                            "decode snapshot preceding deletion receipt: {error}"
                        ))
                    })?;
                let DurableOperation::DeletionFence {
                    source: fence_source,
                    sources: fence_sources,
                    target_epoch: fence_epoch,
                    ..
                } = &previous_receipt.operation
                else {
                    return Err(rejected(
                        "snapshot deletion completion is not preceded by a fence",
                    ));
                };
                if event.kind != "delete_by_source"
                    || !valid_source_set(sources)
                    || source != fence_source
                    || sources != fence_sources
                    || target_epoch != fence_epoch
                    || u64::try_from(deleted_record_ids.len()).ok() != Some(*deleted_records)
                    || deleted_record_ids.iter().collect::<BTreeSet<_>>().len()
                        != deleted_record_ids.len()
                {
                    return Err(rejected(
                        "snapshot deletion completion authority is invalid",
                    ));
                }
                let Some((_, _, fence_record_ids)) = fences.get(&previous.seq) else {
                    return Err(rejected("snapshot deletion fence authority is missing"));
                };
                let completion_ids = deleted_record_ids.iter().copied().collect::<BTreeSet<_>>();
                if &completion_ids != fence_record_ids {
                    return Err(rejected(
                        "snapshot deletion completion does not carry the full deleted-record set",
                    ));
                }
                for record_id in &completion_ids {
                    let Some(capsule) = content
                        .capsules
                        .iter()
                        .find(|capsule| capsule.record_id == *record_id)
                    else {
                        return Err(rejected(
                            "snapshot deletion completion references an unknown record",
                        ));
                    };
                    if capsule.status != CapsuleStatus::Revoked {
                        return Err(rejected(
                            "snapshot deletion completion references a record outside its fence",
                        ));
                    }
                }
                completed_fences.insert(previous.seq);
            }
            _ => {}
        }
    }

    for (source, (epoch, seq)) in &rows {
        let Some((fence_epoch, fence_sources, _fence_record_ids)) = fences.get(seq) else {
            return Err(rejected(
                "snapshot revocation is missing its deletion fence",
            ));
        };
        if fence_epoch != epoch || !fence_sources.contains(source) {
            return Err(rejected(
                "snapshot revocation does not match its deletion fence",
            ));
        }
    }
    for (seq, (_epoch, _sources, _record_ids)) in fences {
        if !completed_fences.contains(&seq) {
            return Err(rejected("snapshot deletion fence completion is missing"));
        }
    }
    for capsule in &content.capsules {
        if capsule.status != CapsuleStatus::Revoked && rows.contains_key(&capsule.source_id) {
            return Err(rejected(
                "snapshot active capsule conflicts with revocation authority",
            ));
        }
        if capsule.status == CapsuleStatus::Revoked
            && !all_deleted_record_ids.contains(&capsule.record_id)
        {
            return Err(rejected(
                "snapshot revoked capsule is missing deletion authority",
            ));
        }
    }
    Ok(())
}

fn valid_source_set(sources: &[SourceId]) -> bool {
    !sources.is_empty()
        && sources.len() <= 1024
        && sources.first().is_some_and(|source| !source.0.is_empty())
        && sources.iter().all(|source| !source.0.is_empty())
        && sources.windows(2).all(|window| window[0] < window[1])
}

fn validate_snapshot_event_envelope(
    event: &Event,
    receipt: &DurableReceipt,
) -> Result<(), EngineReply> {
    if event.seq == 0 || receipt.reply.state_generation != event.seq {
        return Err(rejected(
            "snapshot event sequence does not match its receipt",
        ));
    }
    if !is_sha256_hex(&event.payload_sha256) {
        return Err(rejected("snapshot event payload digest is invalid"));
    }
    if !is_sha256_hex(&receipt.state_digest) {
        return Err(rejected("snapshot event state digest is invalid"));
    }
    if !is_sha256_hex(&receipt.integrity_digest) {
        return Err(rejected("snapshot event integrity digest is invalid"));
    }
    let expected_integrity =
        durable_integrity_digest(&receipt.reply, &receipt.operation, &receipt.state_digest)
            .map_err(|reason| rejected(&format!("snapshot event receipt integrity: {reason}")))?;
    if expected_integrity != receipt.integrity_digest {
        return Err(rejected("snapshot event receipt integrity mismatch"));
    }
    if receipt.reply.outcome != Outcome::Success {
        return Err(rejected("snapshot event receipt outcome is not success"));
    }
    let key_required = !matches!(&receipt.operation, DurableOperation::DeletionFence { .. });
    if key_required
        && event
            .idempotency_key
            .as_deref()
            .is_none_or(|key| key.is_empty() || key.len() > 256)
    {
        return Err(rejected("snapshot event idempotency key is invalid"));
    }
    if !key_required && event.idempotency_key.is_some() {
        return Err(rejected(
            "snapshot deletion fence unexpectedly has an idempotency key",
        ));
    }
    let compact_maintenance = matches!(
        &receipt.operation,
        DurableOperation::Maintenance {
            canonical_input: None,
            ..
        }
    );
    let legacy_maintenance = receipt.idempotency_key.is_none() && compact_maintenance;
    if receipt.idempotency_key.as_deref() != event.idempotency_key.as_deref() && !legacy_maintenance
    {
        return Err(rejected("snapshot event receipt idempotency key mismatch"));
    }
    if let DurableOperation::CommonControl {
        canonical_input, ..
    } = &receipt.operation
    {
        let expected = canonical_digest(canonical_input)
            .map_err(|reason| rejected(&format!("snapshot common control digest: {reason}")))?;
        if event.payload_sha256 != expected
            || receipt.reply.payload["request_semantic_sha256"] != expected
        {
            return Err(rejected("snapshot common control semantic digest mismatch"));
        }
    }
    let kind_matches = match &receipt.operation {
        DurableOperation::CommonControl { .. } => event.kind == "common_control",
        DurableOperation::Observe { .. } => event.kind == "observe",
        DurableOperation::Feedback { .. } => event.kind == "feedback",
        DurableOperation::Correction { .. } => event.kind == "correction",
        DurableOperation::Maintenance { kind, .. } => {
            event.kind == "maintenance"
                || (matches!(kind, MaintenanceKind::Checkpoint) && event.kind == "snapshot_restore")
        }
        DurableOperation::DeletionFence { .. } => event.kind == "deletion_fence",
        DurableOperation::DeleteBySource { .. } => event.kind == "delete_by_source",
    };
    if !kind_matches {
        return Err(rejected("snapshot event operation kind mismatch"));
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
    let receipt = restore_receipt(&reply, &snapshot.kernel, Some(idempotency_key))?;
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
        &snapshot.content_sha256,
        target_seq,
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
    fence_source_set: &BTreeSet<SourceId>,
    target_epoch: u64,
    base_seq: u64,
    target_seq: u64,
    idempotency_key: &str,
    restore_digest: &str,
    deletion_authority_sha256: &str,
) -> Result<(NcmKernel, u64), EngineReply> {
    let fence_seq = base_seq
        .checked_add(1)
        .ok_or_else(|| corrupt_reply(base_seq, "snapshot fence sequence overflow"))?;
    if target_seq != fence_seq.saturating_add(2) {
        return Err(corrupt_reply(base_seq, "snapshot target sequence mismatch"));
    }
    let stripped_sources = fence_source_set.iter().cloned().collect::<Vec<_>>();
    let stripped_record_ids = snapshot
        .content
        .capsules
        .iter()
        .filter(|capsule| stripped.contains(&capsule.source_id))
        .map(|capsule| capsule.record_id)
        .collect::<Vec<_>>();
    let fence_source = stripped_sources
        .first()
        .cloned()
        .unwrap_or_else(|| SourceId("snapshot-restore".to_owned()));
    let fence_sources = if stripped_sources.is_empty() {
        vec![fence_source.clone()]
    } else {
        stripped_sources
    };
    // The deletion completion is a separate durable operation from the
    // caller's restore checkpoint.  Give the internal fence its own
    // deterministic key so the completion row cannot collide with the final
    // snapshot_restore idempotency row.
    let deletion_idempotency_key = format!("snapshot-deletion-{restore_digest}");
    let fence_source_set = fence_sources.iter().cloned().collect::<BTreeSet<_>>();
    let fence_canonical_input =
        crate::privacy::deletion_request_input(&fence_source_set, Some(base_seq), None);
    let mut fence_semantics = fence_canonical_input.clone();
    if let Some(object) = fence_semantics.as_object_mut() {
        object.remove("expected_generation");
    }
    let fence_payload_sha256 = canonical_digest(&fence_semantics).map_err(|reason| {
        corrupt_reply(
            base_seq,
            &format!("serialize snapshot fence payload: {reason}"),
        )
    })?;
    let fence_reply = EngineReply::new(
        Outcome::Success,
        fence_seq,
        json!({"fenced": true, "target_epoch": target_epoch}),
    );
    let fence_operation = DurableOperation::DeletionFence {
        source: fence_source,
        sources: fence_sources,
        target_epoch,
        idempotency_key: deletion_idempotency_key,
        payload_sha256: fence_payload_sha256.clone(),
        deleted_records: u64::try_from(stripped_record_ids.len()).unwrap_or(u64::MAX),
        deleted_record_ids: stripped_record_ids,
        pre_fence_state_digest: sha256_hex(&snapshot.kernel.state_digest()),
        fatigue: snapshot.kernel.scheduler.fatigue,
        steps_since_consolidation: snapshot.kernel.scheduler.steps_since_consolidation,
        canonical_input: Some(fence_canonical_input),
    };
    let fence_state_digest = sha256_hex(&snapshot.kernel.state_digest());
    let fence_integrity_digest =
        durable_integrity_digest(&fence_reply, &fence_operation, &fence_state_digest).map_err(
            |error| {
                corrupt_reply(
                    base_seq,
                    &format!("serialize snapshot fence digest: {error}"),
                )
            },
        )?;
    let fence_receipt = serde_json::to_string(&DurableReceipt {
        reply: fence_reply,
        operation: fence_operation,
        state_digest: fence_state_digest,
        integrity_digest: fence_integrity_digest,
        idempotency_key: None,
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
        target_epoch - 1,
        fence_seq,
        &snapshot.kernel,
        kernel_next_record_id(&snapshot.kernel)?,
        None,
        Some(FenceRows {
            seq: fence_seq,
            payload_sha256: &fence_payload_sha256,
            receipt: &fence_receipt,
        }),
        &snapshot.content_sha256,
        target_seq,
    )?;
    let mut store = NamespaceStore::open(stage_root, namespace, &snapshot.identity)
        .map_err(|error| store_reply(error, base_seq))?;
    let resumed = crate::privacy::resume_pending_rebuild(&mut store, &snapshot.kernel.config)?
        .ok_or_else(|| corrupt_reply(base_seq, "snapshot sanitized rebuild did not resume"))?;
    if resumed.meta.commit_seq.checked_add(1) != Some(target_seq) {
        return Err(corrupt_reply(
            base_seq,
            "snapshot sanitized completion sequence mismatch",
        ));
    }
    // The completion above is a deletion receipt and remains immutable.  Add
    // the restore checkpoint after it so restore idempotency has its own
    // journal row without relabelling the deletion operation.
    let restore_reply = EngineReply::new(
        Outcome::Success,
        target_seq,
        json!({
            "format": FORMAT,
            "epoch": target_epoch,
            "commit_seq": target_seq,
            "tick": resumed.kernel.scheduler.tick.0,
            "state_digest": sha256_hex(&resumed.kernel.state_digest()),
            "content_sha256": snapshot.content_sha256,
            "stripped_sources": stripped.len(),
            "deletion_authority_sha256": deletion_authority_sha256,
            "replayed": false
        }),
    );
    let restore_receipt = restore_receipt(&restore_reply, &resumed.kernel, Some(idempotency_key))?;
    let checkpoint = checkpoint_bytes(&resumed.kernel)?;
    let mut mutation = store
        .begin_mutation()
        .map_err(|error| store_reply(error, resumed.meta.commit_seq))?;
    let event_seq = mutation
        .append_event(
            "snapshot_restore",
            Some(idempotency_key),
            restore_digest,
            &restore_receipt,
            resumed.kernel.scheduler.tick.0,
        )
        .map_err(|error| store_reply(error, resumed.meta.commit_seq))?;
    if event_seq != target_seq {
        return Err(corrupt_reply(
            resumed.meta.commit_seq,
            "snapshot restore checkpoint sequence mismatch",
        ));
    }
    mutation
        .put_checkpoint(target_seq, target_epoch, &checkpoint)
        .map_err(|error| store_reply(error, resumed.meta.commit_seq))?;
    mutation
        .prune_checkpoints_before(target_seq)
        .map_err(|error| store_reply(error, resumed.meta.commit_seq))?;
    mutation
        .set_meta(&StoreMeta {
            epoch: target_epoch,
            commit_seq: target_seq,
            tick: resumed.kernel.scheduler.tick.0,
            fatigue: resumed.kernel.scheduler.fatigue,
            steps_since_consolidation: resumed.kernel.scheduler.steps_since_consolidation,
            last_maintenance: Some("snapshot_restore".to_owned()),
        })
        .map_err(|error| store_reply(error, resumed.meta.commit_seq))?;
    let committed = mutation
        .commit()
        .map_err(|error| store_reply(error, resumed.meta.commit_seq))?;
    if committed != target_seq {
        return Err(corrupt_reply(
            committed,
            "snapshot restore checkpoint commit sequence mismatch",
        ));
    }
    let result = (resumed.kernel, committed);
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
    payload_sha256: &'a str,
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
    snapshot_content_sha256: &str,
    checkpoint_sequence: u64,
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
    let mut staged_event_ticks = BTreeMap::new();
    let mut staged_event_tick_deltas = BTreeMap::new();
    let mut staged_event_tick_resets = BTreeMap::new();

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
        if capsule.status == CapsuleStatus::Revoked
            || stripped.contains(&capsule.source_id)
            || events.iter().any(|event| event.seq == capsule.commit_seq)
        {
            continue;
        }
        let contract = json!({
            "version": 1,
            "sequence": capsule.commit_seq,
            "record_id": capsule.record_id.0,
            "checkpoint_sequence": checkpoint_sequence,
            "snapshot_content_sha256": snapshot_content_sha256,
        });
        let reply = EngineReply::new(
            Outcome::Success,
            capsule.commit_seq,
            json!({"snapshot_observe": contract.clone(), "replayed": false}),
        );
        let operation = DurableOperation::Observe {
            record_id: capsule.record_id,
        };
        let state_digest =
            crate::engine::snapshot_observe_state_digest(&contract, capsule.commit_seq)
                .map_err(|reason| corrupt_reply(capsule.commit_seq, &reason))?;
        let integrity_digest = durable_integrity_digest(&reply, &operation, &state_digest)
            .map_err(|reason| corrupt_reply(capsule.commit_seq, &reason))?;
        let idempotency_key = format!("snapshot-observe-{}", capsule.commit_seq);
        let receipt = serde_json::to_string(&DurableReceipt {
            reply,
            operation,
            state_digest,
            integrity_digest,
            idempotency_key: Some(idempotency_key.clone()),
        })
        .map_err(|error| {
            corrupt_reply(
                capsule.commit_seq,
                &format!("serialize staged observe receipt: {error}"),
            )
        })?;
        let payload_sha256 =
            crate::engine::canonical_observe_payload_digest(capsule, capsule.commit_seq)?;
        let created_tick = kernel
            .records
            .get(capsule.record_id)
            .map_or(0, |record| record.created_tick.0);
        insert_event(
            &tx,
            capsule.commit_seq,
            "observe",
            Some(&idempotency_key),
            &payload_sha256,
            &receipt,
            created_tick,
        )?;
        if staged_event_ticks
            .insert(capsule.commit_seq, created_tick)
            .is_some()
        {
            return Err(corrupt_reply(
                capsule.commit_seq,
                "snapshot staging has duplicate event sequences",
            ));
        }
        staged_event_tick_deltas.insert(capsule.commit_seq, 1);
    }
    for event in events {
        let durable: DurableReceipt = serde_json::from_str(&event.receipt).map_err(|error| {
            corrupt_reply(
                event.seq,
                &format!("decode snapshot staging event receipt: {error}"),
            )
        })?;
        let tick_delta = durable_operation_tick_delta(&durable.operation);
        insert_event(
            &tx,
            event.seq,
            &event.kind,
            event.idempotency_key.as_deref(),
            &event.payload_sha256,
            &event.receipt,
            event.created_tick,
        )?;
        if staged_event_ticks
            .insert(event.seq, event.created_tick)
            .is_some()
        {
            return Err(corrupt_reply(
                event.seq,
                "snapshot staging has duplicate event sequences",
            ));
        }
        staged_event_tick_deltas.insert(event.seq, tick_delta);
        if let Some(tick_before) = durable
            .reply
            .payload
            .get("sanitized_replay")
            .and_then(Value::as_object)
            .and_then(|report| report.get("tick_before"))
            .and_then(Value::as_u64)
        {
            staged_event_tick_resets.insert(event.seq, tick_before);
        }
    }
    let terminal_seq = final_rows
        .as_ref()
        .map(|rows| rows.seq)
        .or_else(|| fence_rows.as_ref().map(|rows| rows.seq))
        .unwrap_or(commit_seq);
    if staged_event_ticks
        .keys()
        .any(|sequence| *sequence == 0 || *sequence > terminal_seq)
    {
        return Err(corrupt_reply(
            terminal_seq,
            "snapshot staging event exceeds its terminal sequence",
        ));
    }
    if let Some(fence) = fence_rows.as_ref() {
        if staged_event_ticks
            .insert(fence.seq, kernel.scheduler.tick.0)
            .is_some()
        {
            return Err(corrupt_reply(
                fence.seq,
                "snapshot staging fence overlaps an event",
            ));
        }
        staged_event_tick_deltas.insert(fence.seq, 0);
    }
    if let Some(final_rows) = final_rows.as_ref() {
        if staged_event_ticks
            .insert(final_rows.seq, kernel.scheduler.tick.0)
            .is_some()
        {
            return Err(corrupt_reply(
                final_rows.seq,
                "snapshot staging restore overlaps an event",
            ));
        }
        staged_event_tick_deltas.insert(final_rows.seq, 0);
    }
    fill_snapshot_journal_gaps(
        &tx,
        terminal_seq,
        &mut staged_event_ticks,
        &staged_event_tick_deltas,
        &staged_event_tick_resets,
        kernel,
        snapshot_content_sha256,
        checkpoint_sequence,
    )?;
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
            fence.payload_sha256,
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

/// Restoring an older snapshot over a namespace with a later generation must
/// retain the journal sequence occupied by the destination's discarded
/// suffix. The copied snapshot events remain the authenticated prefix; these
/// bounded maintenance receipts make the staged journal contiguous so normal
/// recovery can validate the fence/checkpoint at its terminal sequence.
fn durable_operation_tick_delta(operation: &DurableOperation) -> u64 {
    match operation {
        DurableOperation::CommonControl { operations, .. } => operations
            .iter()
            .map(durable_operation_tick_delta)
            .fold(0_u64, u64::saturating_add),
        DurableOperation::Observe { .. } => 1,
        DurableOperation::Maintenance { kind, .. } => match kind {
            MaintenanceKind::Advance { ticks } => u64::from(*ticks),
            MaintenanceKind::Consolidate
            | MaintenanceKind::MergePrune
            | MaintenanceKind::Checkpoint
            | MaintenanceKind::Compact => 0,
        },
        DurableOperation::Feedback { .. }
        | DurableOperation::Correction { .. }
        | DurableOperation::DeletionFence { .. }
        | DurableOperation::DeleteBySource { .. } => 0,
    }
}

fn fill_snapshot_journal_gaps(
    tx: &rusqlite::Transaction<'_>,
    terminal_seq: u64,
    occupied: &mut BTreeMap<u64, u64>,
    occupied_deltas: &BTreeMap<u64, u64>,
    occupied_tick_resets: &BTreeMap<u64, u64>,
    kernel: &NcmKernel,
    snapshot_content_sha256: &str,
    checkpoint_sequence: u64,
) -> Result<(), EngineReply> {
    // Check the worst-case missing count before walking the occupied keys.
    // Walking the sorted key list, rather than probing every sequence up to an
    // attacker-controlled terminal generation, keeps the scan proportional to
    // the authenticated snapshot rows and the bounded synthetic omissions.
    let occupied_count = u64::try_from(
        occupied
            .keys()
            .filter(|sequence| **sequence > 0 && **sequence <= terminal_seq)
            .count(),
    )
    .map_err(|_| corrupt_reply(terminal_seq, "snapshot staging occupied count overflow"))?;
    let potential_missing = terminal_seq.saturating_sub(occupied_count);
    if potential_missing > MAX_SYNTHETIC_GAP_EVENTS {
        return Err(corrupt_reply(
            terminal_seq,
            "snapshot staging journal gap exceeds its bounded repair budget",
        ));
    }
    let keys = occupied
        .iter()
        .filter(|(sequence, _)| **sequence > 0 && **sequence <= terminal_seq)
        .map(|(sequence, created_tick)| (*sequence, *created_tick))
        .collect::<Vec<_>>();
    let mut previous_tick = 0_u64;
    let mut sequence = 1_u64;
    let mut missing_events = 0_u64;
    for (next_sequence, next_tick) in keys {
        if next_sequence < sequence {
            return Err(corrupt_reply(
                next_sequence,
                "snapshot staging event sequence is not ordered",
            ));
        }
        let count = next_sequence
            .checked_sub(sequence)
            .ok_or_else(|| corrupt_reply(terminal_seq, "snapshot staging sequence underflow"))?;
        let first_missing = sequence;
        missing_events = missing_events
            .checked_add(count)
            .ok_or_else(|| corrupt_reply(terminal_seq, "snapshot staging gap count overflow"))?;
        if missing_events > MAX_SYNTHETIC_GAP_EVENTS {
            return Err(corrupt_reply(
                terminal_seq,
                "snapshot staging journal gap exceeds its bounded repair budget",
            ));
        }
        if count != 0 {
            let next_delta = occupied_deltas.get(&next_sequence).copied().unwrap_or(0);
            let target_tick = occupied_tick_resets
                .get(&next_sequence)
                .copied()
                .or_else(|| next_tick.checked_sub(next_delta))
                .ok_or_else(|| {
                    corrupt_reply(
                        next_sequence,
                        "snapshot staging event tick precedes its operation delta",
                    )
                })?;
            insert_snapshot_gap_run(
                tx,
                first_missing,
                next_sequence,
                &mut previous_tick,
                target_tick,
                snapshot_content_sha256,
                checkpoint_sequence,
                &sha256_hex(&kernel.state_digest()),
            )?;
            for gap_sequence in first_missing..next_sequence {
                occupied.insert(gap_sequence, target_tick);
            }
        }
        if let Some(tick_before) = occupied_tick_resets.get(&next_sequence) {
            if *tick_before != previous_tick {
                return Err(corrupt_reply(
                    next_sequence,
                    "snapshot staging deletion tick anchor does not match its prefix",
                ));
            }
        } else if next_tick < previous_tick {
            return Err(corrupt_reply(
                next_sequence,
                "snapshot staging event ticks are not monotonic",
            ));
        }
        previous_tick = next_tick;
        sequence = next_sequence
            .checked_add(1)
            .ok_or_else(|| corrupt_reply(terminal_seq, "snapshot staging sequence overflow"))?;
    }
    let end = terminal_seq
        .checked_add(1)
        .ok_or_else(|| corrupt_reply(terminal_seq, "snapshot staging sequence overflow"))?;
    if sequence < end {
        let count = end
            .checked_sub(sequence)
            .ok_or_else(|| corrupt_reply(terminal_seq, "snapshot staging sequence underflow"))?;
        missing_events = missing_events
            .checked_add(count)
            .ok_or_else(|| corrupt_reply(terminal_seq, "snapshot staging gap count overflow"))?;
        if missing_events > MAX_SYNTHETIC_GAP_EVENTS {
            return Err(corrupt_reply(
                terminal_seq,
                "snapshot staging journal gap exceeds its bounded repair budget",
            ));
        }
        insert_snapshot_gap_run(
            tx,
            sequence,
            end,
            &mut previous_tick,
            kernel.scheduler.tick.0,
            snapshot_content_sha256,
            checkpoint_sequence,
            &sha256_hex(&kernel.state_digest()),
        )?;
        for gap_sequence in sequence..end {
            occupied.insert(gap_sequence, kernel.scheduler.tick.0);
        }
    }
    Ok(())
}

fn insert_snapshot_gap_run(
    tx: &rusqlite::Transaction<'_>,
    first_sequence: u64,
    end_sequence: u64,
    previous_tick: &mut u64,
    target_tick: u64,
    snapshot_content_sha256: &str,
    checkpoint_sequence: u64,
    state_digest: &str,
) -> Result<(), EngineReply> {
    let count = end_sequence
        .checked_sub(first_sequence)
        .ok_or_else(|| corrupt_reply(first_sequence, "snapshot staging gap sequence underflow"))?;
    if count == 0 {
        return Ok(());
    }
    if count > MAX_SYNTHETIC_GAP_EVENTS {
        return Err(corrupt_reply(
            first_sequence,
            "snapshot staging journal gap exceeds its bounded repair budget",
        ));
    }
    if target_tick < *previous_tick {
        return Err(corrupt_reply(
            first_sequence,
            "snapshot staging gap ticks are not monotonic",
        ));
    }
    let tick_delta = target_tick - *previous_tick;
    let advance_ticks = u32::try_from(tick_delta).map_err(|_| {
        corrupt_reply(
            first_sequence,
            "snapshot staging gap tick delta exceeds its bounded repair budget",
        )
    })?;
    if advance_ticks > MAX_SYNTHETIC_ADVANCE_TICKS {
        return Err(corrupt_reply(
            first_sequence,
            "snapshot staging gap advance exceeds its bounded maintenance budget",
        ));
    }
    let contract = json!({
        "version": 1,
        "first_sequence": first_sequence,
        "end_sequence": end_sequence,
        "checkpoint_sequence": checkpoint_sequence,
        "snapshot_content_sha256": snapshot_content_sha256,
        "terminal_state_digest": state_digest,
    });
    for (offset, sequence) in (first_sequence..end_sequence).enumerate() {
        let kind = if offset == 0 && advance_ticks != 0 {
            MaintenanceKind::Advance {
                ticks: advance_ticks,
            }
        } else {
            MaintenanceKind::Checkpoint
        };
        let canonical_input = Some(json!({"snapshot_gap": contract.clone()}));
        let operation = DurableOperation::Maintenance {
            kind: kind.clone(),
            canonical_input,
        };
        let reply = EngineReply::new(
            Outcome::Success,
            sequence,
            json!({"snapshot_gap": contract.clone(), "replayed": false}),
        );
        let row_state_digest = crate::engine::snapshot_gap_state_digest(&contract, sequence)
            .map_err(|reason| corrupt_reply(sequence, &reason))?;
        let integrity_digest = durable_integrity_digest(&reply, &operation, &row_state_digest)
            .map_err(|reason| corrupt_reply(sequence, &reason))?;
        let idempotency_key = format!("snapshot-gap-{sequence}");
        let receipt = serde_json::to_string(&DurableReceipt {
            reply,
            operation,
            state_digest: row_state_digest,
            integrity_digest,
            idempotency_key: Some(idempotency_key.clone()),
        })
        .map_err(|error| {
            corrupt_reply(
                sequence,
                &format!("serialize snapshot staging gap receipt: {error}"),
            )
        })?;
        let payload_sha256 =
            canonical_digest(&kind).map_err(|reason| corrupt_reply(sequence, &reason))?;
        insert_event(
            tx,
            sequence,
            "maintenance",
            Some(&idempotency_key),
            &payload_sha256,
            &receipt,
            target_tick,
        )?;
    }
    *previous_tick = target_tick;
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
    let receipt = restore_receipt(reply, kernel, Some(idempotency_key))?;
    let conn = Connection::open(db_path).map_err(|error| {
        unavailable_reply(seq, &format!("open staged restore receipt: {error}"))
    })?;
    let changed = conn
        .execute(
            "UPDATE events SET kind = 'snapshot_restore', idempotency_key = ?1,
                    payload_sha256 = ?2, receipt = ?3
             WHERE seq = ?4 AND kind = 'snapshot_restore'",
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
    let event = handle
        .store
        .event(seq)
        .map_err(|error| store_reply(error, handle.commit_seq))?
        .ok_or_else(|| corrupt_reply(handle.commit_seq, "restore idempotency event is missing"))?;
    if event.seq != seq
        || event.kind != "snapshot_restore"
        || event.idempotency_key.as_deref() != Some(key)
        || event.payload_sha256 != stored_digest
        || event.receipt != receipt
    {
        return Err(corrupt_reply(
            handle.commit_seq,
            "restore idempotency envelope does not match its journal row",
        ));
    }
    let durable: DurableReceipt = serde_json::from_str(&receipt)
        .map_err(|error| corrupt_reply(seq, &format!("decode restore receipt: {error}")))?;
    validate_receipt_idempotency_key_json(&receipt, &event.kind, event.idempotency_key.as_deref())
        .map_err(|reason| corrupt_reply(handle.commit_seq, reason.as_str()))?;
    validate_snapshot_event_envelope(&event, &durable).map_err(|reply| {
        corrupt_reply(
            handle.commit_seq,
            reply.payload["reason"]
                .as_str()
                .unwrap_or("restore event envelope is invalid"),
        )
    })?;
    if !matches!(
        durable.operation,
        DurableOperation::Maintenance {
            kind: MaintenanceKind::Checkpoint,
            ..
        }
    ) {
        return Err(corrupt_reply(
            handle.commit_seq,
            "restore receipt operation is not a checkpoint",
        ));
    }
    let mut reply = durable.reply;
    if let Some(object) = reply.payload.as_object_mut() {
        object.insert("replayed".to_owned(), Value::Bool(true));
    }
    Ok(Some(reply))
}

fn restore_receipt(
    reply: &EngineReply,
    kernel: &NcmKernel,
    idempotency_key: Option<&str>,
) -> Result<String, EngineReply> {
    let operation = DurableOperation::Maintenance {
        kind: MaintenanceKind::Checkpoint,
        canonical_input: None,
    };
    let state_digest = sha256_hex(&kernel.state_digest());
    let integrity_digest = durable_integrity_digest(reply, &operation, &state_digest)
        .map_err(|reason: String| corrupt_reply(reply.state_generation, &reason))?;
    serde_json::to_string(&DurableReceipt {
        reply: reply.clone(),
        operation,
        state_digest,
        integrity_digest,
        idempotency_key: idempotency_key.map(str::to_owned),
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

fn preserve_record_identity_floor(kernel: &mut NcmKernel, minimum: u64) -> Result<(), EngineReply> {
    let mut records = serde_json::to_value(&kernel.records)
        .map_err(|error| corrupt_reply(0, &format!("serialize record table: {error}")))?;
    let next_id = records
        .get_mut("next_id")
        .ok_or_else(|| rejected("snapshot record table next identity is missing"))?;
    let imported_next_id = next_id
        .as_u64()
        .ok_or_else(|| rejected("snapshot record table next identity is invalid"))?;
    if imported_next_id < minimum {
        *next_id = Value::from(minimum);
        kernel.records = serde_json::from_value(records).map_err(|error| {
            corrupt_reply(0, &format!("restore record identity floor: {error}"))
        })?;
    }
    Ok(())
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
        crate::store::StoreError::SourceRevoked => {
            EngineReply::rejected(RejectReason::SourceRevoked, commit_seq)
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
