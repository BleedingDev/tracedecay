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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tempfile::TempDir;
use tracedecay_memory_ncm_core::types::{NcmConfig, RecordId, SourceId};
use tracedecay_memory_ncm_runtime::embedding::doubles::HashEncoder;
use tracedecay_memory_ncm_runtime::engine::{
    FaultPoint, MaintenanceKind, MaintenanceRequest, NcmEngine, ObserveRequest, Outcome,
    RecallRequest, RejectReason,
};
use tracedecay_memory_ncm_runtime::ports::{
    Deadline, Embedding, EncoderError, EncoderIdentity, StateRoot, TextEncoder,
};
use tracedecay_memory_ncm_runtime::snapshot;

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
    let request = observe_request(source, key, value, idempotency_key);
    let reply = engine.observe(namespace, request);
    assert_eq!(reply.outcome, Outcome::Success);
    RecordId(reply.payload["record_id"].as_u64().expect("record id"))
}

fn observe_request(source: &str, key: &str, value: &str, idempotency_key: &str) -> ObserveRequest {
    let mut request = ObserveRequest {
        idempotency_key: idempotency_key.to_owned(),
        payload_sha256: String::new(),
        source: SourceId(source.to_owned()),
        key_text: key.to_owned(),
        value_text: value.to_owned(),
        affect: None,
        surprise: 0.35,
        intensity: 1.0,
        provenance: json!({"origin": "deletion-test", "retained_input": value}),
        deadline: DEADLINE,
    };
    request.payload_sha256 = request
        .canonical_payload_sha256()
        .expect("observe payload serializes");
    request
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
fn deletion_rebuilds_mixed_state_and_erases_source_text_and_provenance() {
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
    let original_snapshot =
        snapshot::export(&engine, &namespace, DEADLINE).expect("snapshot before deletion exports");
    let original_snapshot: Value =
        serde_json::from_slice(original_snapshot.as_slice()).expect("snapshot JSON");
    assert!(
        original_snapshot["capsules"]
            .as_array()
            .expect("snapshot capsules")
            .iter()
            .any(|capsule| capsule["provenance"]
                .as_str()
                .expect("capsule provenance")
                .contains(B_TOKEN))
    );

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
    let exported =
        snapshot::export(&engine, &namespace, DEADLINE).expect("sanitized snapshot exports");
    let exported_text = std::str::from_utf8(exported.as_slice()).expect("snapshot UTF-8");
    assert!(!exported_text.contains(B_TOKEN));
    let exported: Value = serde_json::from_str(exported_text).expect("snapshot JSON");
    assert_eq!(exported["capsules"].as_array().expect("capsules").len(), 4);
    assert!(
        exported["capsules"]
            .as_array()
            .expect("capsules")
            .iter()
            .any(|capsule| capsule["provenance"]
                .as_str()
                .expect("capsule provenance")
                .contains("A retained one"))
    );

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
    let erased_capsules: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM capsules WHERE source_id = 'source-b'
             AND status = 'revoked' AND key_text = '' AND value_text = ''
             AND length(key_embedding) = 0 AND length(value_embedding) = 0
             AND length(ltm_key) = 0 AND provenance = '{}'",
            [],
            |row| row.get(0),
        )
        .expect("erased capsule count");
    assert_eq!(erased_capsules, 2);
    let retained_provenance: String = connection
        .query_row(
            "SELECT provenance FROM capsules WHERE value_text = 'A retained one'",
            [],
            |row| row.get(0),
        )
        .expect("unaffected provenance reads");
    assert_eq!(
        serde_json::from_str::<Value>(&retained_provenance).expect("provenance JSON"),
        json!({"origin": "deletion-test", "retained_input": "A retained one"})
    );
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
            "revoked source text or provenance remains in controlled SQLite bytes"
        );
    }

    let reopened = make_engine(&tempdir);
    let reopened_state = inspect(&reopened, &namespace);
    assert_eq!(reopened_state["epoch"], 2);
    assert_eq!(
        reopened_state["stm_terrain_digest"],
        after["stm_terrain_digest"]
    );
    let reopened_export = snapshot::export(&reopened, &namespace, DEADLINE)
        .expect("reopened sanitized snapshot exports");
    assert!(
        !std::str::from_utf8(reopened_export.as_slice())
            .expect("snapshot UTF-8")
            .contains(B_TOKEN)
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
    for restart_before_recovery in [false, true] {
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
        let durable_state = || {
            let connection = Connection::open_with_flags(
                sqlite_path(&tempdir, &namespace),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .unwrap();
            connection.query_row(
                "SELECT
                    (SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'epoch'),
                    (SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'commit_seq'),
                    (SELECT COUNT(*) FROM fence),
                    (SELECT COUNT(*) FROM events),
                    (SELECT MAX(seq) FROM events),
                    (SELECT COUNT(*) FROM checkpoints),
                    (SELECT COUNT(*) FROM capsules WHERE status != 'revoked'),
                    (SELECT COUNT(*) FROM capsules WHERE source_id = 'source-b' AND status = 'revoked'
                        AND key_text = '' AND value_text = '' AND length(key_embedding) = 0
                        AND length(value_embedding) = 0 AND length(ltm_key) = 0 AND provenance = '{}'),
                    (SELECT epoch FROM revocations WHERE source_id = 'source-b')",
                [],
                |row| {
                    let nonnegative = |column: usize| -> rusqlite::Result<u64> {
                        let value = row.get::<_, i64>(column)?;
                        Ok(u64::try_from(value).expect("durable counts and generations must be nonnegative"))
                    };
                    Ok(json!({"epoch":nonnegative(0)?,"commit_seq":nonnegative(1)?,"fences":nonnegative(2)?,
                        "events":nonnegative(3)?,"last_event":nonnegative(4)?,"checkpoints":nonnegative(5)?,
                        "retained_records":nonnegative(6)?,"scrubbed_sources":nonnegative(7)?,"revocation_epoch":nonnegative(8)?}))
                },
            ).unwrap()
        };
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
        let assert_reads_refuse_recovery = |engine: &NcmEngine| {
            let recalled = engine.recall(
                &namespace,
                RecallRequest {
                    query_text: format!("{B_TOKEN} crash key"),
                    top_k: 16,
                    deadline: DEADLINE,
                },
            );
            assert!(matches!(recalled.outcome, Outcome::Unavailable(_)));
            assert_eq!(recalled.state_generation, interrupted.state_generation);
            assert_eq!(recalled.payload, Value::Null);
            let health = engine.common_control(
                &namespace,
                json!({"action":"health","expected_generation":interrupted.state_generation}),
                DEADLINE,
            );
            assert!(matches!(health.outcome, Outcome::Unavailable(_)));
            assert_eq!(health.state_generation, interrupted.state_generation);
            assert_eq!(health.payload, Value::Null);
        };
        assert_reads_refuse_recovery(&engine);
        let recovering = if restart_before_recovery {
            drop(engine);
            let pending = durable_state();
            assert_eq!(pending["epoch"], 1);
            assert_eq!(pending["commit_seq"], interrupted.state_generation);
            assert_eq!(pending["fences"], 1);
            let read_only = make_engine(&tempdir);
            assert_reads_refuse_recovery(&read_only);
            drop(read_only);
            assert_eq!(
                durable_state(),
                pending,
                "read-only calls cannot complete pending privacy recovery"
            );
            make_engine(&tempdir)
        } else {
            engine
        };
        assert_reads_refuse_recovery(&recovering);
        let ready = recovering.handshake(&namespace);
        assert_eq!(ready.outcome, Outcome::Success);
        assert_eq!(ready.payload["epoch"], 2);
        assert_eq!(ready.state_generation, interrupted.state_generation + 1);
        let repeated_ready = recovering.handshake(&namespace);
        assert_eq!(repeated_ready.outcome, Outcome::Success);
        assert_eq!(repeated_ready.payload["epoch"], 2);
        assert_eq!(repeated_ready.state_generation, ready.state_generation);
        drop(recovering);
        let completed = durable_state();
        assert_eq!(completed["epoch"], 2);
        assert_eq!(completed["commit_seq"], ready.state_generation);
        assert_eq!(completed["last_event"], ready.state_generation);
        assert_eq!(completed["fences"], 0);
        assert_eq!(
            completed["events"].as_u64().unwrap(),
            interrupted.state_generation + 1
        );
        assert_eq!(completed["retained_records"], 1);
        assert_eq!(completed["scrubbed_sources"], 1);
        assert_eq!(completed["revocation_epoch"], 2);
        assert_eq!(completed["checkpoints"], 1);

        let reopened = make_engine(&tempdir);
        let restarted_ready = reopened.handshake(&namespace);
        assert_eq!(restarted_ready.outcome, Outcome::Success);
        assert_eq!(restarted_ready.payload["epoch"], 2);
        assert_eq!(restarted_ready.state_generation, ready.state_generation);
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
        let exported = snapshot::export(&reopened, &namespace, DEADLINE).unwrap();
        assert!(
            !std::str::from_utf8(exported.as_slice())
                .unwrap()
                .contains(B_TOKEN)
        );
        assert_eq!(
            reopened.revoked_sources(&namespace).unwrap(),
            vec![SourceId("source-b".to_owned())]
        );
        let first_completed = reopened.delete_by_source(
            &namespace,
            &SourceId("source-b".to_owned()),
            "crash-delete",
            DEADLINE,
        );
        assert_eq!(first_completed.outcome, Outcome::Success);
        assert_eq!(first_completed.payload["replayed"], true);
        assert_eq!(first_completed.state_generation, ready.state_generation);
        let duplicate = reopened.delete_by_source(
            &namespace,
            &SourceId("source-b".to_owned()),
            "crash-delete",
            DEADLINE,
        );
        assert_eq!(duplicate.state_generation, ready.state_generation);
        assert_eq!(duplicate.payload["replayed"], true);
        drop(reopened);
        assert_eq!(
            durable_state(),
            completed,
            "restarting and reading a recovered namespace cannot bump its epoch again"
        );
    }
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

#[test]
fn revoked_source_rejects_fresh_keys_and_preserves_duplicates_across_restart() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let namespace = namespace();
    let mut engine = make_engine(&tempdir);
    let original = observe_request(
        "source-b",
        &format!("{B_TOKEN} key"),
        &format!("{B_TOKEN} value"),
        "original-observation",
    );
    let observed = engine.observe(&namespace, original.clone());
    assert_eq!(observed.outcome, Outcome::Success);
    let kept_id = observe(
        &engine,
        &namespace,
        "source-a",
        "kept",
        "kept value",
        "kept",
    );
    let deleted = engine.delete_by_source(&namespace, &original.source, "delete-source", DEADLINE);
    assert_eq!(deleted.outcome, Outcome::Success);
    let after_deletion = inspect(&engine, &namespace);

    for reopen in [false, true] {
        if reopen {
            drop(engine);
            engine = make_engine(&tempdir);
        }
        let replay = engine.observe(&namespace, original.clone());
        let mut expected_replay = observed.clone();
        expected_replay.payload["replayed"] = json!(true);
        assert_eq!(
            replay, expected_replay,
            "duplicate retains the original receipt"
        );

        let mut fresh = original.clone();
        fresh.idempotency_key = format!("fresh-observation-{reopen}");
        let rejected = engine.observe(&namespace, fresh);
        assert_eq!(
            rejected.outcome,
            Outcome::Rejected(RejectReason::SourceRevoked)
        );
        assert_eq!(rejected.state_generation, deleted.state_generation);

        let mut conflict = original.clone();
        conflict.value_text = "changed payload".to_owned();
        conflict.payload_sha256 = conflict.canonical_payload_sha256().expect("payload digest");
        assert_eq!(
            engine.observe(&namespace, conflict).outcome,
            Outcome::Rejected(RejectReason::IdempotencyConflict)
        );
        let after_rejection = inspect(&engine, &namespace);
        assert_eq!(
            after_rejection["state_digest"],
            after_deletion["state_digest"]
        );
        assert_eq!(after_rejection["commit_seq"], after_deletion["commit_seq"]);
        let recalled = engine.recall(
            &namespace,
            RecallRequest {
                query_text: "kept".to_owned(),
                top_k: 16,
                deadline: DEADLINE,
            },
        );
        assert_eq!(recalled.outcome, Outcome::Success);
        assert!(candidate_ids(&recalled.payload).contains(&kept_id));
        assert!(
            candidate_texts(&recalled.payload)
                .iter()
                .all(|text| !text.contains(B_TOKEN))
        );
    }

    drop(engine);
    let connection =
        Connection::open(sqlite_path(&tempdir, &namespace)).expect("store opens for inspection");
    let active_revoked_sources: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM capsules
             WHERE source_id = 'source-b' AND status <> 'revoked'",
            [],
            |row| row.get(0),
        )
        .expect("active source count");
    assert_eq!(active_revoked_sources, 0);
    drop(connection);

    let engine = make_engine(&tempdir);
    let accepted = engine.observe(
        &namespace,
        observe_request(
            "source-a",
            "another kept key",
            "another kept value",
            "kept-again",
        ),
    );
    assert_eq!(accepted.outcome, Outcome::Success);
    assert_eq!(accepted.state_generation, deleted.state_generation + 1);
}

#[test]
fn fresh_deletion_scrubs_legacy_tombstone_provenance_without_recounting_records() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let namespace = namespace();
    let engine = make_engine(&tempdir);
    observe(
        &engine,
        &namespace,
        "source-b",
        "old key",
        "old value",
        "old",
    );
    let deleted = engine.delete_by_source(
        &namespace,
        &SourceId("source-b".to_owned()),
        "original-delete",
        DEADLINE,
    );
    assert_eq!(deleted.outcome, Outcome::Success);
    assert_eq!(deleted.payload["deleted_records"], 1);
    drop(engine);

    // Older stores cleared text and embeddings but retained this JSON column.
    let path = sqlite_path(&tempdir, &namespace);
    let connection = Connection::open(&path).expect("fixture store opens");
    let legacy_provenance = json!({"retained_input": B_TOKEN}).to_string();
    assert_eq!(
        connection
            .execute(
                "UPDATE capsules SET provenance = ?1 WHERE source_id = 'source-b'",
                [legacy_provenance],
            )
            .expect("legacy tombstone fixture writes"),
        1
    );
    drop(connection);

    let reopened = make_engine(&tempdir);
    let scrubbed = reopened.delete_by_source(
        &namespace,
        &SourceId("source-b".to_owned()),
        "fresh-delete",
        DEADLINE,
    );
    assert_eq!(scrubbed.outcome, Outcome::Success);
    assert_eq!(scrubbed.payload["deleted_records"], 0);
    drop(reopened);
    let connection = Connection::open(&path).expect("scrubbed store opens");
    let provenance: String = connection
        .query_row("SELECT provenance FROM capsules", [], |row| row.get(0))
        .expect("tombstone provenance reads");
    assert_eq!(provenance, "{}");
    drop(connection);
    assert!(
        !fs::read(path)
            .expect("compacted store bytes")
            .windows(B_TOKEN.len())
            .any(|window| window == B_TOKEN.as_bytes())
    );
}

struct DeleteDuringEncode {
    engine: Mutex<Option<Weak<NcmEngine>>>,
    encode_calls: AtomicUsize,
}

impl TextEncoder for DeleteDuringEncode {
    fn identity(&self) -> EncoderIdentity {
        HashEncoder::new().identity()
    }

    fn encode(&self, texts: &[&str], deadline: Deadline) -> Result<Vec<Embedding>, EncoderError> {
        self.encode_calls.fetch_add(1, Ordering::Relaxed);
        let engine = self.engine.lock().expect("encoder hook locks").take();
        if let Some(engine) = engine {
            let engine = engine.upgrade().expect("engine remains alive");
            let deleted = engine.delete_by_source(
                &namespace(),
                &SourceId("source-b".to_owned()),
                "delete-during-encode",
                DEADLINE,
            );
            assert_eq!(deleted.outcome, Outcome::Success);
        }
        HashEncoder::new().encode(texts, deadline)
    }
}

#[test]
fn deletion_during_encoding_fences_the_observation_transaction() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let namespace = namespace();
    let encoder = Arc::new(DeleteDuringEncode {
        engine: Mutex::new(None),
        encode_calls: AtomicUsize::new(0),
    });
    let engine = Arc::new(NcmEngine::new(root(&tempdir), encoder.clone(), config()));
    observe(
        &engine,
        &namespace,
        "source-b",
        "old key",
        "old value",
        "old",
    );
    *encoder.engine.lock().expect("encoder hook locks") = Some(Arc::downgrade(&engine));

    let request = observe_request("source-b", "new key", "new value", "racing-observation");
    let rejected = engine.observe(&namespace, request.clone());
    assert_eq!(
        rejected.outcome,
        Outcome::Rejected(RejectReason::SourceRevoked)
    );
    assert_eq!(encoder.encode_calls.load(Ordering::Relaxed), 2);
    let after_deletion = inspect(&engine, &namespace);
    assert_eq!(after_deletion["records"], 0);
    assert_eq!(after_deletion["epoch"], 2);

    let retried = engine.observe(&namespace, request);
    assert_eq!(retried, rejected);
    assert_eq!(
        encoder.encode_calls.load(Ordering::Relaxed),
        2,
        "revoked input never reaches encoding"
    );
    assert_eq!(inspect(&engine, &namespace), after_deletion);
    let next_id = observe(
        &engine,
        &namespace,
        "source-a",
        "kept key",
        "kept value",
        "kept",
    );
    assert_eq!(
        next_id,
        RecordId(2),
        "rejection consumes no record identity"
    );
}

#[test]
fn source_revocation_adds_a_wire_case_without_changing_existing_rejections() {
    let cases = [
        (
            RejectReason::IdempotencyConflict,
            json!("idempotency_conflict"),
        ),
        (
            RejectReason::InvalidRequest("invalid".into()),
            json!({"invalid_request": "invalid"}),
        ),
        (
            RejectReason::UnknownRecord(RecordId(7)),
            json!({"unknown_record": 7}),
        ),
        (RejectReason::SourceRevoked, json!("source_revoked")),
    ];
    for (reason, encoded) in cases {
        assert_eq!(serde_json::to_value(&reason).unwrap(), encoded);
        assert_eq!(
            serde_json::from_value::<RejectReason>(encoded).unwrap(),
            reason
        );
    }
}
