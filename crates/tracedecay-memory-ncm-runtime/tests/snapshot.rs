#![allow(
    clippy::expect_used,
    clippy::field_reassign_with_default,
    clippy::panic,
    clippy::unwrap_used
)]
#![doc = "Integration tests for versioned snapshot export, restore, and privacy lineage."]

use serde_json::{Value, json};
use std::fs;
use std::sync::Arc;
use tempfile::TempDir;
use tracedecay_memory_ncm_core::types::{NcmConfig, RecordId, SourceId};
use tracedecay_memory_ncm_runtime::embedding::doubles::HashEncoder;
use tracedecay_memory_ncm_runtime::engine::{
    CorrectionRequest, FeedbackRequest, MaintenanceKind, MaintenanceRequest, NcmEngine,
    ObserveRequest, Outcome, RecallRequest, RejectReason,
};
use tracedecay_memory_ncm_runtime::ports::{Deadline, StateRoot};
use tracedecay_memory_ncm_runtime::snapshot::{self, RestoreRequest};

const DEADLINE: Deadline = Deadline {
    remaining_ms: u64::MAX,
};
const ERASED_TOKEN: &str = "SNAPSHOT_B_ERASE_7e43a9";

fn namespace() -> String {
    "e4".repeat(32)
}

fn config() -> NcmConfig {
    let mut config = NcmConfig::default();
    config.terrain_resolution = 3;
    config.stm.n_centers = 12;
    config.stm.top_k_read = 12;
    config.stm.top_k_write = 8;
    config.ltm.n_centers = 12;
    config.ltm.top_k_read = 12;
    config.ltm.top_k_write = 8;
    config.hybrid_candidates = 12;
    config
}

fn root(tempdir: &TempDir) -> StateRoot {
    StateRoot::new(tempdir.path()).expect("tempdir is absolute")
}

fn engine(tempdir: &TempDir) -> NcmEngine {
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
        provenance: json!({"origin": "snapshot-test"}),
        deadline: DEADLINE,
    };
    request.payload_sha256 = request
        .canonical_payload_sha256()
        .expect("observe payload serializes");
    let reply = engine.observe(namespace, request);
    assert_eq!(reply.outcome, Outcome::Success);
    RecordId(
        reply.payload["record_id"]
            .as_u64()
            .expect("observed record ID"),
    )
}

fn inspect(engine: &NcmEngine, namespace: &str) -> Value {
    let reply = engine.inspection(namespace);
    assert_eq!(reply.outcome, Outcome::Success);
    reply.payload
}

fn export(engine: &NcmEngine, namespace: &str) -> Vec<u8> {
    snapshot::export(engine, namespace, DEADLINE)
        .expect("snapshot export succeeds")
        .into_vec()
}

fn restore(engine: &NcmEngine, namespace: &str, key: &str, bytes: &[u8]) -> Value {
    let reply = snapshot::restore(
        engine,
        namespace,
        RestoreRequest {
            idempotency_key: key.to_owned(),
            bytes: bytes.to_vec(),
        },
        DEADLINE,
    );
    assert_eq!(reply.outcome, Outcome::Success, "{reply:?}");
    reply.payload
}

fn namespace_dir(tempdir: &TempDir, namespace: &str) -> std::path::PathBuf {
    tempdir.path().join("namespaces").join(namespace)
}

fn assert_kernel_state_equal(left: &Value, right: &Value) {
    for field in [
        "stm_active",
        "ltm_active",
        "records",
        "sources",
        "tick",
        "fatigue",
        "steps_since_consolidation",
        "state_digest",
        "stm_terrain_digest",
        "ltm_terrain_digest",
    ] {
        assert_eq!(left[field], right[field], "state differs at {field}");
    }
}

#[test]
fn export_wipe_restore_round_trip_survives_restart_and_next_mutation() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let namespace = namespace();
    let original = engine(&tempdir);
    let source_a_id = observe(
        &original,
        &namespace,
        "source-a",
        "alpha key",
        "alpha value",
        "observe-a",
    );
    observe(
        &original,
        &namespace,
        "source-c",
        "charlie key",
        "charlie value",
        "observe-c",
    );
    let advance = original.maintenance(
        &namespace,
        MaintenanceRequest {
            idempotency_key: "advance-before-export".to_owned(),
            kind: MaintenanceKind::Advance { ticks: 7 },
            deadline: DEADLINE,
        },
    );
    assert_eq!(advance.outcome, Outcome::Success);
    let before = inspect(&original, &namespace);
    let bytes = export(&original, &namespace);
    drop(original);
    fs::remove_dir_all(namespace_dir(&tempdir, &namespace)).expect("wipe namespace directory");

    let restored = engine(&tempdir);
    let payload = restore(&restored, &namespace, "restore-round-trip", &bytes);
    assert_eq!(payload["state_digest"], before["state_digest"]);
    let after = inspect(&restored, &namespace);
    assert_kernel_state_equal(&before, &after);
    drop(restored);

    let restarted = engine(&tempdir);
    let reopened = inspect(&restarted, &namespace);
    assert_kernel_state_equal(&before, &reopened);
    let redelivered = observe(
        &restarted,
        &namespace,
        "source-a",
        "alpha key",
        "alpha value",
        "observe-a",
    );
    assert_eq!(
        redelivered, source_a_id,
        "restore retains the original observation receipt"
    );
    assert_eq!(
        inspect(&restarted, &namespace)["commit_seq"],
        reopened["commit_seq"],
        "receipt replay must not allocate another commit"
    );
    let old_snapshot: Value = serde_json::from_slice(&bytes).unwrap();
    let restored_snapshot: Value = serde_json::from_slice(&export(&restarted, &namespace)).unwrap();
    for event in old_snapshot["events"].as_array().unwrap() {
        assert!(
            restored_snapshot["events"]
                .as_array()
                .unwrap()
                .contains(event),
            "restore preserves retained event and receipt identity"
        );
    }
    observe(
        &restarted,
        &namespace,
        "source-d",
        "delta key",
        "delta value",
        "observe-after-restore",
    );
    let consolidated = restarted.maintenance(
        &namespace,
        MaintenanceRequest {
            idempotency_key: "consolidate-after-restore".to_owned(),
            kind: MaintenanceKind::Consolidate,
            deadline: DEADLINE,
        },
    );
    assert_eq!(consolidated.outcome, Outcome::Success);
    let deleted = restarted.delete_by_source(
        &namespace,
        &SourceId("source-a".to_owned()),
        "delete-after-restore",
        DEADLINE,
    );
    assert_eq!(deleted.outcome, Outcome::Success, "{deleted:?}");
    assert_eq!(inspect(&restarted, &namespace)["records"], 2);
}

#[test]
fn rollback_never_reuses_discarded_record_targets() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let namespace = namespace();
    let live = engine(&tempdir);
    let retained = observe(&live, &namespace, "source-a", "alpha", "retained", "a");
    let bytes = export(&live, &namespace);
    let discarded = observe(&live, &namespace, "source-b", "bravo", "discarded", "b");
    let before_restore = inspect(&live, &namespace);

    let restored = restore(&live, &namespace, "rollback", &bytes);
    assert!(
        restored["commit_seq"].as_u64().expect("restored sequence")
            > before_restore["commit_seq"]
                .as_u64()
                .expect("prior sequence")
    );
    assert!(
        restored["epoch"].as_u64().expect("restored epoch")
            > before_restore["epoch"].as_u64().expect("prior epoch")
    );
    let replacement = observe(&live, &namespace, "source-c", "charlie", "new value", "c");
    assert!(
        replacement.0 > discarded.0,
        "rollback reused a discarded target"
    );
    let before_stale_target = inspect(&live, &namespace);

    let stale_feedback = live.feedback(
        &namespace,
        FeedbackRequest {
            idempotency_key: "stale-feedback".to_owned(),
            record_ids: vec![discarded],
            deadline: DEADLINE,
        },
    );
    assert_eq!(
        stale_feedback.outcome,
        Outcome::Rejected(RejectReason::UnknownRecord(discarded))
    );
    let stale_correction = live.correction(
        &namespace,
        CorrectionRequest {
            idempotency_key: "stale-correction".to_owned(),
            superseded: discarded,
            superseding: replacement,
            evidence: "ab".repeat(32),
            deadline: DEADLINE,
        },
    );
    assert_eq!(
        stale_correction.outcome,
        Outcome::Rejected(RejectReason::UnknownRecord(discarded))
    );
    assert_eq!(inspect(&live, &namespace), before_stale_target);

    let replayed = restore(&live, &namespace, "rollback", &bytes);
    assert_eq!(replayed["replayed"], true);
    assert_eq!(replayed["commit_seq"], restored["commit_seq"]);
    assert_eq!(inspect(&live, &namespace), before_stale_target);

    let second_restore = restore(&live, &namespace, "rollback-again", &bytes);
    assert!(
        second_restore["commit_seq"]
            .as_u64()
            .expect("restored sequence")
            > before_stale_target["commit_seq"]
                .as_u64()
                .expect("prior sequence")
    );
    drop(live);

    let restarted = engine(&tempdir);
    let newest = observe(
        &restarted,
        &namespace,
        "source-d",
        "delta",
        "after restart",
        "d",
    );
    assert!(
        newest.0 > replacement.0,
        "restart lost the allocation floor"
    );
    let before_stale_target = inspect(&restarted, &namespace);
    let stale = restarted.feedback(
        &namespace,
        FeedbackRequest {
            idempotency_key: "stale-after-restart".to_owned(),
            record_ids: vec![replacement],
            deadline: DEADLINE,
        },
    );
    assert_eq!(
        stale.outcome,
        Outcome::Rejected(RejectReason::UnknownRecord(replacement))
    );
    assert_eq!(inspect(&restarted, &namespace), before_stale_target);
    let retained_feedback = restarted.feedback(
        &namespace,
        FeedbackRequest {
            idempotency_key: "retained-after-restart".to_owned(),
            record_ids: vec![retained],
            deadline: DEADLINE,
        },
    );
    assert_eq!(retained_feedback.outcome, Outcome::Success);
    assert!(
        retained_feedback.payload["centers_updated"]
            .as_u64()
            .expect("updated centers")
            > 0
    );
}

#[test]
fn old_snapshot_cannot_resurrect_a_source_deleted_after_export() {
    let live_dir = TempDir::new().expect("live tempdir creates");
    let expected_dir = TempDir::new().expect("expected tempdir creates");
    let namespace = namespace();
    let live = engine(&live_dir);
    observe(
        &live,
        &namespace,
        "source-a",
        "alpha retained",
        "A retained value",
        "a",
    );
    observe(
        &live,
        &namespace,
        "source-b",
        &format!("{ERASED_TOKEN} key"),
        &format!("{ERASED_TOKEN} value"),
        "b",
    );
    observe(
        &live,
        &namespace,
        "source-c",
        "charlie retained",
        "C retained value",
        "c",
    );
    let old_snapshot = export(&live, &namespace);
    let discarded = observe(
        &live,
        &namespace,
        "source-d",
        "discarded delta",
        "discarded after export",
        "d",
    );
    let deleted = live.delete_by_source(
        &namespace,
        &SourceId("source-b".to_owned()),
        "delete-b",
        DEADLINE,
    );
    assert_eq!(deleted.outcome, Outcome::Success, "{deleted:?}");

    let expected = engine(&expected_dir);
    observe(
        &expected,
        &namespace,
        "source-a",
        "alpha retained",
        "A retained value",
        "a",
    );
    observe(
        &expected,
        &namespace,
        "source-c",
        "charlie retained",
        "C retained value",
        "c",
    );
    let expected_state = inspect(&expected, &namespace);

    let restored = restore(&live, &namespace, "restore-old", &old_snapshot);
    assert_eq!(restored["stripped_sources"], 1);
    let actual = inspect(&live, &namespace);
    for field in [
        "stm_active",
        "ltm_active",
        "records",
        "sources",
        "stm_terrain_digest",
        "ltm_terrain_digest",
    ] {
        assert_eq!(
            actual[field], expected_state[field],
            "replay differs at {field}"
        );
    }
    let recalled = live.recall(
        &namespace,
        RecallRequest {
            query_text: ERASED_TOKEN.to_owned(),
            top_k: 12,
            deadline: DEADLINE,
        },
    );
    assert!(matches!(
        recalled.outcome,
        Outcome::Success | Outcome::Empty
    ));
    let recall_json = recalled.payload.to_string();
    assert!(!recall_json.contains(ERASED_TOKEN));
    assert!(!recall_json.contains("source-b"));

    let stored = fs::read(namespace_dir(&live_dir, &namespace).join("ncm.sqlite"))
        .expect("read restored sqlite");
    assert!(
        !stored
            .windows(ERASED_TOKEN.len())
            .any(|window| window == ERASED_TOKEN.as_bytes()),
        "the central negative control fails if restore trusts the old kernel/capsule: the erased B token remains in SQLite"
    );
    let exported_after = export(&live, &namespace);
    let parsed: Value = serde_json::from_slice(&exported_after).expect("snapshot JSON parses");
    assert!(parsed.get("revocations").is_none());
    assert!(
        !exported_after
            .windows(b"source-b".len())
            .any(|window| window == b"source-b")
    );

    let deleted_survivor = live.delete_by_source(
        &namespace,
        &SourceId("source-c".to_owned()),
        "delete-after-sanitized-restore",
        DEADLINE,
    );
    assert_eq!(
        deleted_survivor.outcome,
        Outcome::Success,
        "{deleted_survivor:?}"
    );
    drop(live);
    let restarted = engine(&live_dir);
    let replacement = observe(
        &restarted,
        &namespace,
        "source-e",
        "echo after restart",
        "retained after rebuild",
        "e",
    );
    assert!(
        replacement.0 > discarded.0,
        "sanitized restore or later deletion reused a discarded target"
    );
    let before_stale_target = inspect(&restarted, &namespace);
    let stale_feedback = restarted.feedback(
        &namespace,
        FeedbackRequest {
            idempotency_key: "stale-after-sanitized-restore".to_owned(),
            record_ids: vec![discarded],
            deadline: DEADLINE,
        },
    );
    assert_eq!(
        stale_feedback.outcome,
        Outcome::Rejected(RejectReason::UnknownRecord(discarded))
    );
    assert_eq!(inspect(&restarted, &namespace), before_stale_target);
}

#[test]
fn tampering_is_rejected_without_changing_live_state() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let namespace = namespace();
    let engine = engine(&tempdir);
    observe(&engine, &namespace, "source-a", "alpha", "value", "a");
    let before = inspect(&engine, &namespace);
    let mut bytes = export(&engine, &namespace);
    let midpoint = bytes.len() / 2;
    bytes[midpoint] ^= 1;
    let reply = snapshot::restore(
        &engine,
        &namespace,
        RestoreRequest {
            idempotency_key: "tampered".to_owned(),
            bytes,
        },
        DEADLINE,
    );
    assert!(matches!(reply.outcome, Outcome::Rejected(_)));
    let after = inspect(&engine, &namespace);
    assert_kernel_state_equal(&before, &after);
    assert_eq!(before["commit_seq"], after["commit_seq"]);
    assert_eq!(before["epoch"], after["epoch"]);
}

#[test]
fn identity_mismatch_is_incompatible_and_restore_is_idempotent() {
    let source_dir = TempDir::new().expect("source tempdir creates");
    let target_dir = TempDir::new().expect("target tempdir creates");
    let mismatch_dir = TempDir::new().expect("mismatch tempdir creates");
    let namespace = namespace();
    let source = engine(&source_dir);
    observe(&source, &namespace, "source-a", "alpha", "value", "a");
    let bytes = export(&source, &namespace);

    let target = engine(&target_dir);
    let first = snapshot::restore(
        &target,
        &namespace,
        RestoreRequest {
            idempotency_key: "same-restore".to_owned(),
            bytes: bytes.clone(),
        },
        DEADLINE,
    );
    assert_eq!(first.outcome, Outcome::Success);
    let second = snapshot::restore(
        &target,
        &namespace,
        RestoreRequest {
            idempotency_key: "same-restore".to_owned(),
            bytes: bytes.clone(),
        },
        DEADLINE,
    );
    assert_eq!(second.outcome, Outcome::Success);
    assert_eq!(first.state_generation, second.state_generation);
    assert_eq!(second.payload["replayed"], true);

    let mut mismatched_config = config();
    mismatched_config.stm.sigma_read = 0.375;
    let mismatched = NcmEngine::new(
        root(&mismatch_dir),
        Arc::new(HashEncoder::new()),
        mismatched_config,
    );
    let reply = snapshot::restore(
        &mismatched,
        &namespace,
        RestoreRequest {
            idempotency_key: "wrong-identity".to_owned(),
            bytes,
        },
        DEADLINE,
    );
    assert_eq!(reply.outcome, Outcome::Incompatible);
    assert!(!namespace_dir(&mismatch_dir, &namespace).exists());
}

#[test]
fn engine_export_delegates_and_envelope_omits_revocation_authority() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let namespace = namespace();
    let engine = engine(&tempdir);
    observe(&engine, &namespace, "source-a", "alpha", "value", "a");
    let reply = engine.snapshot_export(&namespace, DEADLINE);
    assert_eq!(reply.outcome, Outcome::Success);
    assert!(reply.state_generation > 0);
    let bytes = reply.payload["bytes"]
        .as_array()
        .expect("snapshot byte array")
        .iter()
        .map(|value| value.as_u64().expect("snapshot byte") as u8)
        .collect::<Vec<_>>();
    let parsed: Value = serde_json::from_slice(&bytes).expect("snapshot envelope parses");
    assert_eq!(parsed["format"], "ncm-snapshot.v1");
    assert!(parsed.get("content_sha256").is_some());
    assert!(parsed.get("revocations").is_none());
    assert!(parsed["capsules"].is_array());
    assert!(parsed["events"].is_array());
}

#[test]
fn file_transport_exports_atomically_and_restore_consumes_the_file() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let namespace = namespace();
    let engine = engine(&tempdir);
    observe(
        &engine,
        &namespace,
        "source-file",
        "file transport",
        "snapshot value",
        "file-observe",
    );

    let exported = snapshot::export_to_file(&engine, &namespace, DEADLINE);
    assert_eq!(exported.outcome, Outcome::Success, "{exported:?}");
    let metadata = exported.payload.as_object().expect("file metadata object");
    let snapshot_file = metadata["snapshot_file"]
        .as_str()
        .map(std::path::PathBuf::from)
        .expect("snapshot file path");
    assert!(snapshot_file.is_absolute());
    assert_eq!(
        snapshot_file.parent(),
        Some(
            namespace_dir(&tempdir, &namespace)
                .join("snapshots")
                .as_path()
        )
    );
    assert!(snapshot_file.is_file());
    assert!(
        fs::read_dir(snapshot_file.parent().expect("snapshot directory"))
            .expect("read snapshot directory")
            .all(|entry| !entry
                .expect("snapshot entry")
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")),
        "atomic export must not leave a temporary file"
    );

    let restored = snapshot::restore_from_file(
        &engine,
        &namespace,
        "file-restore",
        &snapshot_file,
        metadata["byte_length"].as_u64().expect("snapshot length"),
        metadata["content_sha256"]
            .as_str()
            .expect("snapshot digest"),
        DEADLINE,
    );
    assert_eq!(restored.outcome, Outcome::Success, "{restored:?}");
    assert!(
        !snapshot_file.exists(),
        "restore must consume its transport file"
    );
}

#[test]
fn restore_refuses_a_durably_fenced_namespace() {
    let source_dir = TempDir::new().expect("source tempdir creates");
    let target_dir = TempDir::new().expect("target tempdir creates");
    let namespace = namespace();
    let source = engine(&source_dir);
    observe(
        &source, &namespace, "source-a", "alpha", "value", "source-a",
    );
    let bytes = export(&source, &namespace);

    let target = engine(&target_dir);
    observe(
        &target, &namespace, "source-c", "charlie", "value", "target-c",
    );
    drop(target);
    let connection =
        rusqlite::Connection::open(namespace_dir(&target_dir, &namespace).join("ncm.sqlite"))
            .expect("open target sqlite");
    connection
        .execute("INSERT INTO fence(id, reason) VALUES (1, 'rebuilding')", [])
        .expect("set durable fence");
    drop(connection);

    let reopened = engine(&target_dir);
    let reply = snapshot::restore(
        &reopened,
        &namespace,
        RestoreRequest {
            idempotency_key: "fenced-restore".to_owned(),
            bytes,
        },
        DEADLINE,
    );
    assert_eq!(reply.outcome, Outcome::Busy);
}
