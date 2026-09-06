#![allow(
    clippy::expect_used,
    clippy::field_reassign_with_default,
    clippy::panic,
    clippy::unwrap_used
)]
#![doc = "Integration tests for fenced source deletion and sanitized reconstruction."]

use rusqlite::Connection;
use serde_json::{Value, json};
use std::fs;
use std::sync::Arc;
use tempfile::TempDir;
use tracedecay_memory_ncm_core::types::{NcmConfig, RecordId, SourceId};
use tracedecay_memory_ncm_runtime::embedding::doubles::HashEncoder;
use tracedecay_memory_ncm_runtime::engine::{
    FaultPoint, MaintenanceKind, MaintenanceRequest, NcmEngine, ObserveRequest, Outcome,
    RecallRequest,
};
use tracedecay_memory_ncm_runtime::ports::{Deadline, StateRoot};

const DEADLINE: Deadline = Deadline {
    remaining_ms: u64::MAX,
};
const B_TOKEN: &str = "BETA_ERASE_TOKEN_91f36d";

fn namespace() -> String {
    "d1".repeat(32)
}

fn config() -> NcmConfig {
    let mut config = NcmConfig::default();
    config.terrain_resolution = 3;
    config.stm.n_centers = 16;
    config.stm.top_k_read = 16;
    config.stm.top_k_write = 8;
    config.ltm.n_centers = 16;
    config.ltm.top_k_read = 16;
    config.ltm.top_k_write = 8;
    config.hybrid_candidates = 16;
    config
}

fn root(tempdir: &TempDir) -> StateRoot {
    StateRoot::new(tempdir.path()).expect("tempdir is absolute")
}

fn make_engine(tempdir: &TempDir) -> NcmEngine {
    NcmEngine::new(root(tempdir), Arc::new(HashEncoder::new()), config())
}

fn observe(
    engine: &NcmEngine,
    namespace: &str,
    source: &str,
    key: &str,
    value: &str,
    idempotency_key: &str,
) -> RecordId {
    let mut request = ObserveRequest {
        idempotency_key: idempotency_key.to_owned(),
        payload_sha256: String::new(),
        source: SourceId(source.to_owned()),
        key_text: key.to_owned(),
        value_text: value.to_owned(),
        affect: None,
        surprise: 0.35,
        intensity: 1.0,
        provenance: json!({"origin": "deletion-test"}),
        deadline: DEADLINE,
    };
    request.payload_sha256 = request
        .canonical_payload_sha256()
        .expect("observe payload serializes");
    let reply = engine.observe(namespace, request);
    assert_eq!(reply.outcome, Outcome::Success);
    RecordId(reply.payload["record_id"].as_u64().expect("record id"))
}

fn maintain(engine: &NcmEngine, namespace: &str, key: &str, kind: MaintenanceKind) {
    let reply = engine.maintenance(
        namespace,
        MaintenanceRequest {
            idempotency_key: key.to_owned(),
            kind,
            deadline: DEADLINE,
        },
    );
    assert_eq!(reply.outcome, Outcome::Success);
}

fn inspect(engine: &NcmEngine, namespace: &str) -> Value {
    let reply = engine.inspection(namespace);
    assert_eq!(reply.outcome, Outcome::Success);
    reply.payload
}

fn candidate_ids(payload: &Value) -> Vec<RecordId> {
    payload
        .get("Candidates")
        .and_then(|value| value.get("candidates"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|candidate| candidate.get("record_id").and_then(Value::as_u64))
        .map(RecordId)
        .collect()
}

fn candidate_texts(payload: &Value) -> Vec<&str> {
    payload
        .get("Candidates")
        .and_then(|value| value.get("candidates"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(|candidate| {
            ["key_text", "value_text"]
                .into_iter()
                .filter_map(|key| candidate.get(key).and_then(Value::as_str))
        })
        .collect()
}

fn sqlite_path(tempdir: &TempDir, namespace: &str) -> std::path::PathBuf {
    tempdir
        .path()
        .join("namespaces")
        .join(namespace)
        .join("ncm.sqlite")
}

fn support_ids(kernel: &Value) -> Vec<RecordId> {
    let mut ids = Vec::new();
    for layer in ["stm", "ltm"] {
        for row in kernel[layer]["support"]
            .as_array()
            .expect("center support rows")
        {
            for id in row.as_array().expect("center support row") {
                ids.push(RecordId(id.as_u64().expect("support record id")));
            }
        }
    }
    ids
}

#[test]
fn deletion_rebuilds_mixed_state_and_physically_removes_source_text() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let namespace = namespace();
    let engine = make_engine(&tempdir);
    let _a1 = observe(
        &engine,
        &namespace,
        "source-a",
        "alpha one",
        "A retained one",
        "a1",
    );
    let b1 = observe(
        &engine,
        &namespace,
        "source-b",
        &format!("{B_TOKEN} key one"),
        &format!("{B_TOKEN} value one"),
        "b1",
    );
    let _c1 = observe(
        &engine,
        &namespace,
        "source-c",
        "charlie one",
        "C retained one",
        "c1",
    );
    let _a2 = observe(
        &engine,
        &namespace,
        "source-a",
        "alpha two",
        "A retained two",
        "a2",
    );
    let b2 = observe(
        &engine,
        &namespace,
        "source-b",
        &format!("{B_TOKEN} key two"),
        &format!("{B_TOKEN} value two"),
        "b2",
    );
    let _c2 = observe(
        &engine,
        &namespace,
        "source-c",
        "charlie two",
        "C retained two",
        "c2",
    );
    maintain(
        &engine,
        &namespace,
        "consolidate",
        MaintenanceKind::Consolidate,
    );
    maintain(
        &engine,
        &namespace,
        "merge-prune",
        MaintenanceKind::MergePrune,
    );
    maintain(
        &engine,
        &namespace,
        "old-checkpoint",
        MaintenanceKind::Checkpoint,
    );
    let before = inspect(&engine, &namespace);

    let deleted = engine.delete_by_source(
        &namespace,
        &SourceId("source-b".to_owned()),
        "delete-b",
        DEADLINE,
    );
    assert_eq!(deleted.outcome, Outcome::Success);
    assert_eq!(deleted.payload["deleted_records"], 2);
    assert_eq!(deleted.payload["epoch"], 2);
    let after = inspect(&engine, &namespace);
    assert_ne!(after["stm_terrain_digest"], before["stm_terrain_digest"]);
    assert_ne!(after["state_digest"], before["state_digest"]);

    for query in [format!("{B_TOKEN} key one"), format!("{B_TOKEN} key two")] {
        let recalled = engine.recall(
            &namespace,
            RecallRequest {
                query_text: query,
                top_k: 16,
                deadline: DEADLINE,
            },
        );
        assert!(matches!(
            recalled.outcome,
            Outcome::Success | Outcome::Empty
        ));
        assert!(
            candidate_texts(&recalled.payload)
                .iter()
                .all(|text| !text.contains(B_TOKEN))
        );
        assert!(
            candidate_ids(&recalled.payload)
                .iter()
                .all(|id| *id != b1 && *id != b2)
        );
    }

    drop(engine);
    let path = sqlite_path(&tempdir, &namespace);
    let connection = Connection::open(&path).expect("compacted store opens");
    let checkpoint_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM checkpoints", [], |row| row.get(0))
        .expect("checkpoint count");
    assert_eq!(checkpoint_count, 1, "all pre-deletion checkpoints are gone");
    let state: Vec<u8> = connection
        .query_row("SELECT state FROM checkpoints", [], |row| row.get(0))
        .expect("sanitized checkpoint reads");
    let checkpoint: Value = serde_json::from_slice(&state).expect("checkpoint JSON");
    let ids = support_ids(&checkpoint["kernel"]);
    assert!(!ids.contains(&b1));
    assert!(!ids.contains(&b2));
    drop(connection);
    for candidate in [
        path.clone(),
        path.with_file_name("ncm.sqlite-wal"),
        path.with_file_name("ncm.sqlite-shm"),
    ] {
        let bytes = fs::read(candidate).unwrap_or_default();
        assert!(
            !bytes
                .windows(B_TOKEN.len())
                .any(|window| window == B_TOKEN.as_bytes()),
            "revoked source text remains in controlled SQLite bytes"
        );
    }

    let reopened = make_engine(&tempdir);
    let reopened_state = inspect(&reopened, &namespace);
    assert_eq!(reopened_state["epoch"], 2);
    assert_eq!(
        reopened_state["stm_terrain_digest"],
        after["stm_terrain_digest"]
    );
    let replay = reopened.delete_by_source(
        &namespace,
        &SourceId("source-b".to_owned()),
        "delete-b",
        DEADLINE,
    );
    assert_eq!(replay.outcome, Outcome::Success);
    assert_eq!(replay.state_generation, deleted.state_generation);
    assert_eq!(replay.payload["replayed"], true);
    assert_eq!(inspect(&reopened, &namespace)["epoch"], 2);

    let scratch = TempDir::new().expect("scratch tempdir creates");
    let scratch_engine = make_engine(&scratch);
    observe(
        &scratch_engine,
        &namespace,
        "source-a",
        "alpha one",
        "A retained one",
        "sa1",
    );
    observe(
        &scratch_engine,
        &namespace,
        "source-c",
        "charlie one",
        "C retained one",
        "sc1",
    );
    observe(
        &scratch_engine,
        &namespace,
        "source-a",
        "alpha two",
        "A retained two",
        "sa2",
    );
    observe(
        &scratch_engine,
        &namespace,
        "source-c",
        "charlie two",
        "C retained two",
        "sc2",
    );
    maintain(
        &scratch_engine,
        &namespace,
        "scratch-consolidate",
        MaintenanceKind::Consolidate,
    );
    maintain(
        &scratch_engine,
        &namespace,
        "scratch-merge",
        MaintenanceKind::MergePrune,
    );
    maintain(
        &scratch_engine,
        &namespace,
        "scratch-checkpoint",
        MaintenanceKind::Checkpoint,
    );
    let scratch_state = inspect(&scratch_engine, &namespace);
    assert_eq!(
        after["stm_terrain_digest"],
        scratch_state["stm_terrain_digest"]
    );
    assert_eq!(
        after["ltm_terrain_digest"],
        scratch_state["ltm_terrain_digest"]
    );
}

#[test]
fn interrupted_deletion_resumes_before_serving_and_bumps_epoch_once() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let namespace = namespace();
    let engine = make_engine(&tempdir);
    observe(
        &engine,
        &namespace,
        "source-a",
        "kept",
        "kept value",
        "kept",
    );
    let deleted_id = observe(
        &engine,
        &namespace,
        "source-b",
        &format!("{B_TOKEN} crash key"),
        &format!("{B_TOKEN} crash value"),
        "deleted",
    );
    engine
        .inject_fault_once(FaultPoint::AfterDeletionFenceCommit)
        .expect("fault arms");
    let interrupted = engine.delete_by_source(
        &namespace,
        &SourceId("source-b".to_owned()),
        "crash-delete",
        DEADLINE,
    );
    assert_eq!(interrupted.outcome, Outcome::EffectUnknown);
    assert!(matches!(
        engine.handshake(&namespace).outcome,
        Outcome::Unavailable(_)
    ));
    drop(engine);

    let reopened = make_engine(&tempdir);
    let ready = reopened.handshake(&namespace);
    assert_eq!(ready.outcome, Outcome::Success);
    assert_eq!(ready.payload["epoch"], 2);
    let recalled = reopened.recall(
        &namespace,
        RecallRequest {
            query_text: format!("{B_TOKEN} crash key"),
            top_k: 16,
            deadline: DEADLINE,
        },
    );
    assert!(!candidate_ids(&recalled.payload).contains(&deleted_id));
    assert!(
        candidate_texts(&recalled.payload)
            .iter()
            .all(|text| !text.contains(B_TOKEN))
    );

    let first_completed = reopened.delete_by_source(
        &namespace,
        &SourceId("source-b".to_owned()),
        "crash-delete",
        DEADLINE,
    );
    assert_eq!(first_completed.outcome, Outcome::Success);
    assert_eq!(first_completed.payload["replayed"], true);
    let generation = first_completed.state_generation;
    let duplicate = reopened.delete_by_source(
        &namespace,
        &SourceId("source-b".to_owned()),
        "crash-delete",
        DEADLINE,
    );
    assert_eq!(duplicate.state_generation, generation);
    assert_eq!(duplicate.payload["replayed"], true);
    assert_eq!(inspect(&reopened, &namespace)["epoch"], 2);
}

#[test]
fn deleting_an_unknown_source_is_successful_and_idempotent() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let namespace = namespace();
    let engine = make_engine(&tempdir);
    observe(&engine, &namespace, "source-a", "kept", "value", "kept");
    let first = engine.delete_by_source(
        &namespace,
        &SourceId("unknown-source".to_owned()),
        "delete-unknown",
        DEADLINE,
    );
    assert_eq!(first.outcome, Outcome::Success);
    assert_eq!(first.payload["deleted_records"], 0);
    let replay = engine.delete_by_source(
        &namespace,
        &SourceId("unknown-source".to_owned()),
        "delete-unknown",
        DEADLINE,
    );
    assert_eq!(replay.state_generation, first.state_generation);
    assert_eq!(replay.payload["replayed"], true);
}
