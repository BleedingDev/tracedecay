#![allow(
    clippy::expect_used,
    clippy::field_reassign_with_default,
    clippy::panic,
    clippy::unwrap_used
)]
#![doc = "End-to-end transaction and exactly-once tests for the native NCM engine."]

use rusqlite::Connection;
use serde_json::{Value, json};
use std::sync::Arc;
use tempfile::TempDir;
use tracedecay_memory_ncm_core::types::{NcmConfig, RecordId, SourceId};
use tracedecay_memory_ncm_runtime::embedding::doubles::HashEncoder;
#[cfg(feature = "real-encoder")]
use tracedecay_memory_ncm_runtime::embedding::{MiniLmEncoder, PinnedEncoder, install};
use tracedecay_memory_ncm_runtime::engine::{
    CorrectionRequest, FaultPoint, FeedbackRequest, MaintenanceKind, MaintenanceRequest, NcmEngine,
    ObserveRequest, Outcome, RecallRequest, RejectReason,
};
use tracedecay_memory_ncm_runtime::ports::{
    Deadline, Embedding, EncoderError, EncoderIdentity, StateRoot, TextEncoder,
};

const DEADLINE: Deadline = Deadline {
    remaining_ms: u64::MAX,
};

fn namespace(index: u8) -> String {
    format!("{index:02x}{}", "0".repeat(62))
}

fn config() -> NcmConfig {
    let mut config = NcmConfig::default();
    config.terrain_resolution = 3;
    config.stm.n_centers = 8;
    config.stm.top_k_read = 8;
    config.stm.top_k_write = 8;
    config.ltm.n_centers = 8;
    config.ltm.top_k_read = 8;
    config.ltm.top_k_write = 8;
    config.hybrid_candidates = 8;
    config
}

fn state_root(tempdir: &TempDir) -> StateRoot {
    StateRoot::new(tempdir.path()).expect("tempdir path is absolute")
}

fn make_engine(tempdir: &TempDir) -> NcmEngine {
    NcmEngine::new(state_root(tempdir), Arc::new(HashEncoder::new()), config())
}

fn observe_request(key: &str, value: &str, idempotency_key: &str) -> ObserveRequest {
    let mut request = ObserveRequest {
        idempotency_key: idempotency_key.to_owned(),
        payload_sha256: String::new(),
        source: SourceId(format!("source-{idempotency_key}")),
        key_text: key.to_owned(),
        value_text: value.to_owned(),
        affect: None,
        surprise: 0.4,
        intensity: 1.0,
        provenance: json!({"origin": "engine-test"}),
        deadline: DEADLINE,
    };
    request.payload_sha256 = request
        .canonical_payload_sha256()
        .expect("canonical payload serializes");
    request
}

fn inspect(engine: &NcmEngine, namespace: &str) -> Value {
    let reply = engine.inspection(namespace);
    assert_eq!(reply.outcome, Outcome::Success);
    reply.payload
}

fn scalar(payload: &Value, key: &str) -> u64 {
    payload[key].as_u64().expect("inspection scalar exists")
}

fn assert_semantic_state_eq(left: &Value, right: &Value) {
    for key in [
        "stm_active",
        "ltm_active",
        "records",
        "sources",
        "tick",
        "fatigue",
        "steps_since_consolidation",
        "epoch",
        "commit_seq",
        "state_digest",
        "stm_terrain_digest",
        "ltm_terrain_digest",
    ] {
        assert_eq!(left[key], right[key], "state field differs: {key}");
    }
}

fn recalled_values(payload: &Value) -> Vec<&str> {
    payload["Candidates"]["candidates"]
        .as_array()
        .expect("candidate array")
        .iter()
        .map(|candidate| {
            candidate["value_text"]
                .as_str()
                .expect("candidate value text")
        })
        .collect()
}

#[test]
fn observe_then_recall_returns_supported_text_and_changes_state() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let engine = make_engine(&tempdir);
    let ns = namespace(1);
    let empty = engine.handshake(&ns);
    assert_eq!(empty.outcome, Outcome::Success);
    assert_eq!(empty.state_generation, 0);
    assert_eq!(empty.payload["empty"], true);

    let observed = engine.observe(
        &ns,
        observe_request(
            "Rust ownership keeps memory safe",
            "The stored answer is borrow checking.",
            "observe-1",
        ),
    );
    assert_eq!(observed.outcome, Outcome::Success);
    assert_eq!(observed.state_generation, 1);
    assert_eq!(observed.payload["record_id"], 1);

    let state = inspect(&engine, &ns);
    assert_eq!(scalar(&state, "records"), 1);
    assert_eq!(scalar(&state, "tick"), 1);
    assert_ne!(state["state_digest"], Value::Null);
    assert_ne!(scalar(&state, "stm_terrain_digest"), 0);

    let recalled = engine.recall(
        &ns,
        RecallRequest {
            query_text: "Rust ownership keeps memory safe".to_owned(),
            top_k: 4,
            deadline: DEADLINE,
        },
    );
    assert_eq!(recalled.outcome, Outcome::Success);
    assert_eq!(recalled.state_generation, 1);
    assert!(recalled_values(&recalled.payload).contains(&"The stored answer is borrow checking."));
}

#[test]
fn duplicate_delivery_replays_receipt_and_changes_nothing() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let engine = make_engine(&tempdir);
    let ns = namespace(2);
    let request = observe_request("same key", "same value", "duplicate-key");
    let first = engine.observe(&ns, request.clone());
    assert_eq!(first.outcome, Outcome::Success);
    let before = inspect(&engine, &ns);

    let replay = engine.observe(&ns, request);
    assert_eq!(replay.outcome, Outcome::Success);
    assert_eq!(replay.state_generation, first.state_generation);
    assert_eq!(replay.payload["replayed"], true);
    let after = inspect(&engine, &ns);

    for key in [
        "tick",
        "records",
        "sources",
        "stm_terrain_digest",
        "ltm_terrain_digest",
        "state_digest",
        "commit_seq",
    ] {
        assert_eq!(after[key], before[key], "duplicate changed {key}");
    }
    assert_eq!(after["fatigue"], before["fatigue"]);
}

#[test]
fn same_key_with_different_payload_is_rejected() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let engine = make_engine(&tempdir);
    let ns = namespace(3);
    let first = engine.observe(&ns, observe_request("key", "first", "conflict-key"));
    assert_eq!(first.outcome, Outcome::Success);
    let before = inspect(&engine, &ns);

    let conflict = engine.observe(&ns, observe_request("key", "second", "conflict-key"));
    assert_eq!(
        conflict.outcome,
        Outcome::Rejected(RejectReason::IdempotencyConflict)
    );
    assert_eq!(inspect(&engine, &ns), before);
}

#[test]
fn crash_before_commit_rolls_back_and_reopen_matches_prior_state() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let ns = namespace(4);
    let engine = make_engine(&tempdir);
    assert_eq!(
        engine
            .observe(&ns, observe_request("baseline", "kept", "base"))
            .outcome,
        Outcome::Success
    );
    let before = inspect(&engine, &ns);
    engine
        .inject_fault_once(FaultPoint::BeforeCommit)
        .expect("fault arms");
    let failed = engine.observe(&ns, observe_request("rolled back", "absent", "fail"));
    assert!(matches!(failed.outcome, Outcome::Unavailable(_)));
    assert_eq!(inspect(&engine, &ns), before);
    drop(engine);

    let reopened = make_engine(&tempdir);
    assert_semantic_state_eq(&inspect(&reopened, &ns), &before);
    let recall = reopened.recall(
        &ns,
        RecallRequest {
            query_text: "rolled back".to_owned(),
            top_k: 4,
            deadline: DEADLINE,
        },
    );
    assert!(
        recall.outcome == Outcome::Empty || !recalled_values(&recall.payload).contains(&"absent")
    );
}

#[test]
fn crash_after_commit_before_publish_recovers_exactly_one_effect() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let ns = namespace(5);
    let engine = make_engine(&tempdir);
    let first = engine.observe(&ns, observe_request("first", "one", "first"));
    assert_eq!(first.state_generation, 1);
    engine
        .inject_fault_once(FaultPoint::AfterCommitBeforePublish)
        .expect("fault arms");
    let unknown = engine.observe(&ns, observe_request("second", "two", "second"));
    assert_eq!(unknown.outcome, Outcome::EffectUnknown);
    assert_eq!(unknown.state_generation, 2);
    drop(engine);

    let reopened = make_engine(&tempdir);
    let state = inspect(&reopened, &ns);
    assert_eq!(scalar(&state, "commit_seq"), 2);
    assert_eq!(scalar(&state, "records"), 2);
    assert_eq!(scalar(&state, "tick"), 2);
    let replay = reopened.observe(&ns, observe_request("second", "two", "second"));
    assert_eq!(replay.outcome, Outcome::Success);
    assert_eq!(replay.state_generation, 2);
    assert_eq!(replay.payload["replayed"], true);
    assert_eq!(scalar(&inspect(&reopened, &ns), "records"), 2);
}

#[test]
fn loss_after_publication_replays_without_a_second_effect() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let ns = namespace(67);
    let engine = make_engine(&tempdir);
    engine
        .inject_fault_once(FaultPoint::AfterPublishBeforeAck)
        .expect("fault arms");
    let request = observe_request("published", "once", "published-once");
    let unknown = engine.observe(&ns, request.clone());
    assert_eq!(unknown.outcome, Outcome::EffectUnknown);
    let published = inspect(&engine, &ns);
    assert_eq!(scalar(&published, "records"), 1);
    assert_eq!(scalar(&published, "commit_seq"), 1);

    let replay = engine.observe(&ns, request);
    assert_eq!(replay.outcome, Outcome::Success);
    assert_eq!(replay.state_generation, 1);
    assert_eq!(replay.payload["replayed"], true);
    assert_semantic_state_eq(&inspect(&engine, &ns), &published);
}

#[test]
fn recall_never_changes_commit_sequence_or_state_digest() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let engine = make_engine(&tempdir);
    let ns = namespace(6);
    engine.observe(&ns, observe_request("read only", "value", "read-base"));
    let before = inspect(&engine, &ns);
    for _ in 0..3 {
        let recalled = engine.recall(
            &ns,
            RecallRequest {
                query_text: "read only".to_owned(),
                top_k: 4,
                deadline: DEADLINE,
            },
        );
        assert_eq!(recalled.outcome, Outcome::Success);
    }
    let after = inspect(&engine, &ns);
    assert_eq!(after["commit_seq"], before["commit_seq"]);
    assert_eq!(after["state_digest"], before["state_digest"]);
    assert_eq!(after["tick"], before["tick"]);
}

#[derive(Clone, Copy)]
struct FailingEncoder;

impl TextEncoder for FailingEncoder {
    fn identity(&self) -> EncoderIdentity {
        EncoderIdentity {
            model: "test-double/failing".to_owned(),
            artifact_sha256: "test-double/failing-v1".to_owned(),
            max_length: 128,
        }
    }

    fn encode(&self, _texts: &[&str], _deadline: Deadline) -> Result<Vec<Embedding>, EncoderError> {
        Err(EncoderError::Inference(
            "injected encoder failure".to_owned(),
        ))
    }
}

#[test]
fn failed_embed_produces_no_receipt_event_or_capsule() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let root = state_root(&tempdir);
    let engine = NcmEngine::new(root.clone(), Arc::new(FailingEncoder), config());
    let ns = namespace(7);
    let failed = engine.observe(&ns, observe_request("cannot", "encode", "failed-embed"));
    assert!(matches!(failed.outcome, Outcome::Unavailable(_)));
    let state = inspect(&engine, &ns);
    assert_eq!(scalar(&state, "commit_seq"), 0);
    assert_eq!(scalar(&state, "records"), 0);
    drop(engine);

    let connection = Connection::open(root.path().join("namespaces").join(&ns).join("ncm.sqlite"))
        .expect("store opens for readback");
    for table in ["events", "capsules"] {
        let count: i64 = connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .expect("count query succeeds");
        assert_eq!(count, 0, "failed embed wrote {table}");
    }
}

#[test]
fn expired_deadline_before_encode_has_no_effect_or_namespace() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let root = state_root(&tempdir);
    let engine = NcmEngine::new(root.clone(), Arc::new(HashEncoder::new()), config());
    let ns = namespace(8);
    let mut request = observe_request("expired", "absent", "expired");
    request.deadline = Deadline { remaining_ms: 0 };
    let reply = engine.observe(&ns, request);
    assert_eq!(reply.outcome, Outcome::Cancelled);
    assert!(!root.path().join("namespaces").join(ns).exists());
}

#[test]
fn resident_namespace_lru_is_bounded_to_four() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let engine = make_engine(&tempdir);
    for index in 10..15 {
        let ns = namespace(index);
        let reply = engine.observe(
            &ns,
            observe_request(
                "resident",
                &format!("value-{index}"),
                &format!("key-{index}"),
            ),
        );
        assert_eq!(reply.outcome, Outcome::Success);
    }
    let health = engine.health();
    assert_eq!(health.outcome, Outcome::Success);
    assert_eq!(health.payload["resident_namespaces"], 4);
    assert_eq!(health.payload["catalog_namespaces"], 5);
}

#[test]
fn catalog_bound_rejects_the_thirty_third_namespace() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let engine = make_engine(&tempdir);
    for index in 32..64 {
        let ns = namespace(index);
        let reply = engine.observe(
            &ns,
            observe_request("catalog", "value", &format!("catalog-{index}")),
        );
        assert_eq!(reply.outcome, Outcome::Success, "namespace {index}");
    }
    let rejected = engine.observe(
        &namespace(64),
        observe_request("catalog", "overflow", "catalog-overflow"),
    );
    assert_eq!(rejected.outcome, Outcome::BudgetExceeded);
    assert_eq!(rejected.state_generation, 0);
}

#[test]
fn feedback_correction_and_checkpoint_are_idempotent_and_recoverable() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let ns = namespace(9);
    let engine = make_engine(&tempdir);
    engine.observe(&ns, observe_request("old", "old value", "old"));
    engine.observe(&ns, observe_request("new", "new value", "new"));

    let feedback = FeedbackRequest {
        idempotency_key: "feedback".to_owned(),
        record_ids: vec![RecordId(1)],
        deadline: DEADLINE,
    };
    let first = engine.feedback(&ns, feedback.clone());
    assert_eq!(first.outcome, Outcome::Success);
    let before_replay = inspect(&engine, &ns);
    let replay = engine.feedback(&ns, feedback);
    assert_eq!(replay.payload["replayed"], true);
    assert_eq!(inspect(&engine, &ns), before_replay);

    let corrected = engine.correction(
        &ns,
        CorrectionRequest {
            idempotency_key: "correction".to_owned(),
            superseded: RecordId(1),
            superseding: RecordId(2),
            evidence: "ab".repeat(32),
            deadline: DEADLINE,
        },
    );
    assert_eq!(corrected.outcome, Outcome::Success);
    let checkpoint = engine.maintenance(
        &ns,
        MaintenanceRequest {
            idempotency_key: "checkpoint".to_owned(),
            kind: MaintenanceKind::Checkpoint,
            deadline: DEADLINE,
        },
    );
    assert_eq!(checkpoint.outcome, Outcome::Success);
    let before = inspect(&engine, &ns);
    drop(engine);

    let reopened = make_engine(&tempdir);
    assert_semantic_state_eq(&inspect(&reopened, &ns), &before);
}

#[test]
fn checkpoint_failure_rolls_back_the_entire_maintenance_transaction() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let ns = namespace(65);
    let engine = make_engine(&tempdir);
    engine.observe(&ns, observe_request("base", "value", "base"));
    let before = inspect(&engine, &ns);
    engine
        .inject_fault_once(FaultPoint::DuringCheckpoint)
        .expect("fault arms");
    let failed = engine.maintenance(
        &ns,
        MaintenanceRequest {
            idempotency_key: "failed-checkpoint".to_owned(),
            kind: MaintenanceKind::Checkpoint,
            deadline: DEADLINE,
        },
    );
    assert!(matches!(failed.outcome, Outcome::Unavailable(_)));
    assert_eq!(inspect(&engine, &ns), before);
    drop(engine);
    let reopened = make_engine(&tempdir);
    assert_semantic_state_eq(&inspect(&reopened, &ns), &before);
}

#[test]
fn recovery_digest_mismatch_fails_closed() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let root = state_root(&tempdir);
    let ns = namespace(68);
    let engine = make_engine(&tempdir);
    let observed = engine.observe(&ns, observe_request("durable", "truth", "digest"));
    assert_eq!(observed.outcome, Outcome::Success);
    drop(engine);

    let path = root.path().join("namespaces").join(&ns).join("ncm.sqlite");
    let connection = Connection::open(path).expect("store opens for corruption fixture");
    let receipt: String = connection
        .query_row("SELECT receipt FROM events WHERE seq = 1", [], |row| {
            row.get(0)
        })
        .expect("receipt reads");
    let mut receipt_json: Value = serde_json::from_str(&receipt).expect("receipt is JSON");
    receipt_json["state_digest"] = Value::String("00".repeat(32));
    connection
        .execute(
            "UPDATE events SET receipt = ?1 WHERE seq = 1",
            [serde_json::to_string(&receipt_json).expect("receipt serializes")],
        )
        .expect("receipt corruption writes");
    drop(connection);

    let reopened = make_engine(&tempdir);
    assert_eq!(reopened.handshake(&ns).outcome, Outcome::Corrupt);
}

#[cfg(feature = "real-encoder")]
#[test]
fn real_encoder_paraphrase_journey_returns_the_matching_record_first() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let root = state_root(&tempdir);
    install(&root, DEADLINE).expect("pinned model installs");
    let pinned = PinnedEncoder::reference().expect("reference manifest loads");
    let encoder = MiniLmEncoder::open(&root, &pinned).expect("pinned model opens offline");
    let engine = NcmEngine::new(root, Arc::new(encoder), config());
    let ns = namespace(66);
    let records = [
        (
            "Astronomy describes planets orbiting distant stars.",
            "space-record",
        ),
        (
            "Sourdough bread ferments with a living starter.",
            "bread-record",
        ),
        (
            "Rust ownership prevents data races without garbage collection.",
            "rust-record",
        ),
    ];
    for (index, (key, value)) in records.iter().enumerate() {
        let reply = engine.observe(&ns, observe_request(key, value, &format!("real-{index}")));
        assert_eq!(reply.outcome, Outcome::Success);
    }
    let recalled = engine.recall(
        &ns,
        RecallRequest {
            query_text: "Which programming language uses ownership for safe concurrency?"
                .to_owned(),
            top_k: 3,
            deadline: DEADLINE,
        },
    );
    assert_eq!(recalled.outcome, Outcome::Success);
    assert_eq!(
        recalled_values(&recalled.payload).first(),
        Some(&"rust-record")
    );
}
