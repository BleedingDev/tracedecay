#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
#![doc = "Integration tests for the provider-owned NCM namespace store."]

use rusqlite::Connection;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;
use tempfile::TempDir;
use tracedecay_memory_ncm_core::types::{AffectVector, AlgorithmIdentity, RecordId, SourceId};
use tracedecay_memory_ncm_runtime::ports::StateRoot;
use tracedecay_memory_ncm_runtime::store::{
    Capsule, CapsuleStatus, Checkpoint, NamespaceStore, Quota, StoreError, StoreIdentity, StoreMeta,
};

fn namespace() -> String {
    "ab".repeat(32)
}

fn state_root(tempdir: &TempDir) -> StateRoot {
    StateRoot::new(tempdir.path()).expect("tempdir path should be absolute")
}

fn identity() -> StoreIdentity {
    let projection_bytes = (0_u8..=31).collect::<Vec<_>>();
    let digest = Sha256::digest(&projection_bytes);
    let projection_sha256 = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    StoreIdentity {
        algorithm: AlgorithmIdentity {
            profile: "ncm-biomem-rs.v1".to_owned(),
            config_sha256: "config-digest".to_owned(),
        },
        projection_sha256,
        encoder_model: "paraphrase-multilingual-MiniLM-L12-v2".to_owned(),
        encoder_artifact_sha256: "encoder-digest".to_owned(),
        projection_bytes,
        seed: 7,
        config_json: "{}".to_owned(),
    }
}

fn capsule(source: &str, key: &str, value: &str) -> Capsule {
    Capsule::new(
        SourceId(source.to_owned()),
        key.to_owned(),
        value.to_owned(),
        AffectVector::neutral(),
        0.25,
        0.75,
        vec![0.5; 384],
        vec![0.25; 384],
        vec![0.125; 64],
        "{\"origin\":\"test\"}".to_owned(),
    )
}

fn create_store(tempdir: &TempDir) -> (StateRoot, String, StoreIdentity, NamespaceStore) {
    let root = state_root(tempdir);
    let ns = namespace();
    let identity = identity();
    let store = NamespaceStore::create(&root, &ns, identity.clone()).expect("store creates");
    (root, ns, identity, store)
}

#[test]
fn create_open_round_trip_and_identity_match() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let (root, ns, identity, store) = create_store(&tempdir);
    assert!(NamespaceStore::exists(&root, &ns));
    assert_eq!(store.identity(), &identity);
    assert_eq!(store.meta().unwrap(), StoreMeta::default());
    assert!(store.latest_checkpoint().unwrap().is_none());
    drop(store);

    let reopened = NamespaceStore::open(&root, &ns, &identity).expect("matching store opens");
    assert_eq!(reopened.identity(), &identity);
    assert_eq!(reopened.meta().unwrap(), StoreMeta::default());
}

#[test]
fn explicit_create_refuses_an_existing_namespace() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let (root, ns, identity, store) = create_store(&tempdir);
    drop(store);

    let error = NamespaceStore::create(&root, &ns, identity)
        .expect_err("explicit creation must not replace existing state");
    assert!(matches!(error, StoreError::AlreadyExists));
}

#[test]
fn missing_open_does_not_create_namespace_directory() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let root = state_root(&tempdir);
    let ns = namespace();
    let error = NamespaceStore::open(&root, &ns, &identity()).expect_err("store is absent");
    assert!(matches!(error, StoreError::Missing));
    assert!(!root.path().join("namespaces").exists());
    assert!(!NamespaceStore::exists(&root, &ns));
}

#[test]
fn incompatible_identity_fields_fail_closed() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let (root, ns, identity, store) = create_store(&tempdir);
    drop(store);

    let mut algorithm_mismatch = identity.clone();
    algorithm_mismatch.algorithm.profile.push_str("-other");
    let error = NamespaceStore::open(&root, &ns, &algorithm_mismatch)
        .expect_err("algorithm mismatch must fail");
    assert!(matches!(error, StoreError::Incompatible { field } if field == "algorithm"));

    let mut projection_mismatch = identity.clone();
    projection_mismatch.projection_bytes[0] ^= 1;
    let error = NamespaceStore::open(&root, &ns, &projection_mismatch)
        .expect_err("projection mismatch must fail");
    assert!(matches!(error, StoreError::Incompatible { field } if field == "projection_bytes"));

    let mut encoder_mismatch = identity;
    encoder_mismatch.encoder_model.push_str("-other");
    let error = NamespaceStore::open(&root, &ns, &encoder_mismatch)
        .expect_err("encoder mismatch must fail");
    assert!(matches!(error, StoreError::Incompatible { field } if field == "encoder_model"));
}

#[test]
fn corrupt_garbage_truncated_and_missing_table_fail_closed() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let root = state_root(&tempdir);
    let ns = namespace();
    let dir = root.path().join("namespaces").join(&ns);
    fs::create_dir_all(&dir).expect("namespace directory creates");
    let path = dir.join("ncm.sqlite");
    fs::write(&path, b"not sqlite").expect("garbage writes");
    let error = NamespaceStore::open(&root, &ns, &identity()).expect_err("garbage is corrupt");
    assert!(matches!(error, StoreError::Corrupt(_)));

    let tempdir = TempDir::new().expect("tempdir creates");
    let (root, ns, stored_identity, store) = create_store(&tempdir);
    let path = store.path().to_owned();
    drop(store);
    let file = fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("created database opens for truncation");
    file.set_len(32).expect("database truncates");
    let error =
        NamespaceStore::open(&root, &ns, &stored_identity).expect_err("truncated is corrupt");
    assert!(matches!(error, StoreError::Corrupt(_)));

    let tempdir = TempDir::new().expect("tempdir creates");
    let root = state_root(&tempdir);
    let ns = namespace();
    let dir = root.path().join("namespaces").join(&ns);
    fs::create_dir_all(&dir).expect("namespace directory creates");
    let path = dir.join("ncm.sqlite");
    let connection = Connection::open(&path).expect("sqlite file creates");
    connection
        .execute("CREATE TABLE meta(key TEXT PRIMARY KEY, value BLOB)", [])
        .expect("partial schema creates");
    drop(connection);
    let error =
        NamespaceStore::open(&root, &ns, &identity()).expect_err("partial schema is corrupt");
    assert!(matches!(error, StoreError::Corrupt(_)));
}

#[test]
fn second_writer_is_busy() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let (root, ns, identity, store) = create_store(&tempdir);
    let error = NamespaceStore::open(&root, &ns, &identity).expect_err("exclusive writer is held");
    assert!(matches!(error, StoreError::Busy));
    drop(store);
}

#[test]
fn dropped_mutation_rolls_back_and_commit_survives_reopen() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let (root, ns, identity, mut store) = create_store(&tempdir);
    {
        let mut mutation = store.begin_mutation().expect("mutation begins");
        assert_eq!(
            mutation
                .append_event("observe", Some("k1"), "p1", "{}", 1)
                .unwrap(),
            1
        );
        assert_eq!(
            mutation
                .insert_capsule(capsule("source-a", "k", "v"))
                .unwrap(),
            RecordId(1)
        );
    }
    drop(store);

    let mut store = NamespaceStore::open(&root, &ns, &identity)
        .expect("rolled-back store reopens without partial state");
    assert!(store.events_after(0).unwrap().is_empty());
    assert!(store.capsules_in_commit_order(true).unwrap().is_empty());

    {
        let mut mutation = store.begin_mutation().expect("mutation begins");
        let event_seq = mutation
            .append_event("observe", Some("k1"), "p1", "{\"ok\":true}", 2)
            .expect("event appends");
        let record_id = mutation
            .insert_capsule(capsule("source-a", "k", "v"))
            .expect("capsule inserts");
        assert_eq!(event_seq, 1);
        assert_eq!(record_id, RecordId(1));
        assert_eq!(mutation.commit().unwrap(), 1);
    }
    assert_eq!(store.events_after(0).unwrap().len(), 1);
    assert_eq!(store.capsule(RecordId(1)).unwrap().unwrap().commit_seq, 1);
    drop(store);

    let reopened = NamespaceStore::open(&root, &ns, &identity).expect("committed store reopens");
    assert_eq!(reopened.meta().unwrap().commit_seq, 1);
    assert_eq!(reopened.events_after(0).unwrap()[0].seq, 1);
    assert_eq!(
        reopened.capsule(RecordId(1)).unwrap().unwrap().key_text,
        "k"
    );
}

#[test]
fn commit_then_reopen_replays_exact_event_sequences_after_checkpoint() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let (root, ns, identity, mut store) = create_store(&tempdir);
    {
        let mut mutation = store.begin_mutation().expect("mutation begins");
        assert_eq!(mutation.append_event("one", None, "a", "{}", 1).unwrap(), 1);
        mutation
            .put_checkpoint(0, 1, b"checkpoint-before-events")
            .expect("checkpoint writes");
        assert_eq!(mutation.commit().unwrap(), 1);
    }
    {
        let mut mutation = store.begin_mutation().expect("mutation begins");
        assert_eq!(mutation.append_event("two", None, "b", "{}", 2).unwrap(), 2);
        assert_eq!(mutation.commit().unwrap(), 2);
    }
    drop(store);

    let reopened = NamespaceStore::open(&root, &ns, &identity).expect("store reopens");
    let checkpoint = reopened
        .latest_checkpoint()
        .unwrap()
        .expect("checkpoint exists");
    assert_eq!(
        checkpoint,
        Checkpoint {
            seq: 0,
            epoch: 1,
            state: b"checkpoint-before-events".to_vec()
        }
    );
    let replay = reopened.events_after(checkpoint.seq).unwrap();
    assert_eq!(
        replay.iter().map(|event| event.seq).collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[test]
fn checkpoint_prune_keeps_newer_state() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let (_root, _ns, _identity, mut store) = create_store(&tempdir);
    {
        let mut mutation = store.begin_mutation().expect("mutation begins");
        mutation
            .put_checkpoint(1, 1, b"old")
            .expect("old checkpoint writes");
        mutation
            .put_checkpoint(2, 1, b"new")
            .expect("new checkpoint writes");
        mutation.commit().expect("checkpoint commit succeeds");
    }
    {
        let mut mutation = store.begin_mutation().expect("mutation begins");
        mutation
            .prune_checkpoints_before(2)
            .expect("checkpoint prune succeeds");
        mutation.commit().expect("prune commit succeeds");
    }
    let checkpoint = store
        .latest_checkpoint()
        .unwrap()
        .expect("new checkpoint remains");
    assert_eq!(checkpoint.seq, 2);
    assert_eq!(checkpoint.state, b"new");
}

#[test]
fn idempotency_lookup_and_conflict_are_durable() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let (root, ns, identity, mut store) = create_store(&tempdir);
    {
        let mut mutation = store.begin_mutation().expect("mutation begins");
        mutation
            .append_event("observe", Some("same-key"), "digest", "{\"receipt\":1}", 3)
            .expect("first event appends");
        mutation.commit().expect("first event commits");
    }
    {
        let mut mutation = store.begin_mutation().expect("mutation begins");
        let lookup = mutation
            .lookup_idempotency("same-key")
            .expect("lookup succeeds")
            .expect("idempotency row exists");
        assert_eq!(lookup.0, "digest");
        assert_eq!(lookup.1, "{\"receipt\":1}");
        assert_eq!(lookup.2, 1);
        let error = mutation
            .append_event("observe", Some("same-key"), "digest", "{}", 4)
            .expect_err("duplicate key conflicts");
        assert!(matches!(error, StoreError::IdempotencyConflict));
    }
    drop(store);
    let reopened = NamespaceStore::open(&root, &ns, &identity).expect("store reopens");
    assert_eq!(reopened.events_after(0).unwrap().len(), 1);
}

#[test]
fn status_fence_metadata_and_revocation_round_trip() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let (_root, _ns, _identity, mut store) = create_store(&tempdir);
    {
        let mut mutation = store.begin_mutation().expect("mutation begins");
        let record_id = mutation
            .insert_capsule(capsule("source-a", "k", "v"))
            .unwrap();
        mutation
            .mark_capsule_status(record_id, CapsuleStatus::Superseded)
            .expect("supersedes record");
        mutation.set_fence("rebuilding").expect("fence sets");
        mutation
            .add_revocation("source-a", 2, 1)
            .expect("revocation adds");
        mutation
            .set_meta(&StoreMeta {
                epoch: 2,
                commit_seq: 999,
                tick: 8,
                fatigue: 1.5,
                steps_since_consolidation: 4,
                last_maintenance: Some("rebuild".to_owned()),
            })
            .expect("metadata sets");
        assert_eq!(mutation.get_meta().unwrap().epoch, 2);
        mutation.commit().expect("metadata commit succeeds");
    }
    assert_eq!(store.fenced().unwrap().as_deref(), Some("rebuilding"));
    assert_eq!(store.revocations().unwrap()[0].epoch, 2);
    assert_eq!(store.meta().unwrap().commit_seq, 1);
    assert_eq!(
        store.meta().unwrap().last_maintenance.as_deref(),
        Some("rebuild")
    );
    assert_eq!(
        store.capsule(RecordId(1)).unwrap().unwrap().status,
        CapsuleStatus::Superseded
    );
    {
        let mut mutation = store.begin_mutation().expect("mutation begins");
        mutation
            .mark_capsule_status(RecordId(1), CapsuleStatus::Revoked)
            .expect("revocation tombstones record");
        mutation.clear_fence().expect("fence clears");
        mutation.commit().expect("revocation commit succeeds");
    }
    let tombstone = store.capsule(RecordId(1)).unwrap().unwrap();
    assert_eq!(tombstone.status, CapsuleStatus::Revoked);
    assert!(tombstone.key_text.is_empty());
    assert!(tombstone.key_embedding.is_empty());
    assert!(store.capsules_in_commit_order(false).unwrap().is_empty());
    assert!(store.capsules_in_commit_order(true).unwrap().len() == 1);
    assert!(store.fenced().unwrap().is_none());

    let mut mutation = store.begin_mutation().expect("mutation begins");
    let next_record = mutation
        .insert_capsule(capsule("source-b", "next", "record"))
        .expect("record after tombstone inserts");
    assert_eq!(next_record, RecordId(2), "record IDs must never be reused");
    mutation.commit().expect("second record commits");
}

#[test]
fn quota_fill_has_no_partial_write_and_privacy_uses_reserve() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let (_root, _ns, _identity, mut store) = create_store(&tempdir);
    let target = Quota::default().source_basis_bytes;
    let receipt = format!("\"{}\"", "x".repeat((target - 2) as usize));
    {
        let mut mutation = store.begin_mutation().expect("mutation begins");
        mutation
            .append_event("", None, "", &receipt, 0)
            .expect("event fills source basis quota");
        mutation.commit().expect("quota fill commits");
    }
    let before = store.usage().expect("usage reads");
    assert_eq!(before.source_basis_bytes(), target);
    {
        let mut mutation = store.begin_mutation().expect("mutation begins");
        let error = mutation
            .append_event("x", None, "", "{}", 0)
            .expect_err("source basis quota rejects another event");
        assert!(matches!(error, StoreError::BudgetExceeded));
    }
    let after = store.usage().expect("usage reads");
    assert_eq!(after, before);
    {
        let mut mutation = store.begin_mutation().expect("privacy mutation begins");
        mutation
            .add_revocation("source-under-pressure", 2, 2)
            .expect("privacy revocation uses reserved capacity");
        mutation.commit().expect("privacy mutation commits");
    }
    assert_eq!(store.revocations().unwrap().len(), 1);
}

#[test]
fn usage_reports_payload_components_and_wal() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let (_root, _ns, _identity, mut store) = create_store(&tempdir);
    let before = store.usage().expect("initial usage reads");
    assert!(before.db_bytes > 0);
    {
        let mut mutation = store.begin_mutation().expect("mutation begins");
        mutation
            .append_event("kind", Some("key"), "digest", "{\"payload\":true}", 1)
            .expect("event appends");
        mutation
            .insert_capsule(capsule("source", "key", "value"))
            .expect("capsule inserts");
        mutation
            .put_checkpoint(1, 1, b"state")
            .expect("checkpoint writes");
        mutation.commit().expect("mutation commits");
    }
    let after = store.usage().expect("usage reads");
    assert!(
        after.wal_bytes > 0,
        "uncheckpointed commit should remain in WAL"
    );
    assert!(after.capsule_bytes > before.capsule_bytes);
    assert!(after.event_bytes > before.event_bytes);
    assert!(after.checkpoint_bytes > before.checkpoint_bytes);
    assert!(after.db_bytes >= before.db_bytes);
}

#[test]
fn namespace_escape_and_uppercase_names_are_rejected_without_side_effects() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let root = state_root(&tempdir);
    for invalid in ["../x", &"A".repeat(64), &"f".repeat(63)] {
        let result = NamespaceStore::create(&root, invalid, identity());
        assert!(matches!(result, Err(StoreError::InvalidNamespace(_))));
        assert!(!NamespaceStore::exists(&root, invalid));
    }
    assert!(!Path::new(&format!("{}/x", tempdir.path().display())).exists());
    assert!(!root.path().join("namespaces").exists());
}
