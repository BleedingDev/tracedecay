#![allow(
    clippy::expect_used,
    clippy::field_reassign_with_default,
    clippy::panic,
    clippy::unwrap_used
)]
#![doc = "Durable public feedback delivery identity and legacy wire compatibility."]

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tempfile::TempDir;
use tracedecay_memory_ncm_core::types::{NcmConfig, SourceId};
use tracedecay_memory_ncm_runtime::embedding::doubles::HashEncoder;
use tracedecay_memory_ncm_runtime::engine::{
    FaultPoint, NcmEngine, ObserveRequest, Outcome, RejectReason,
};
use tracedecay_memory_ncm_runtime::ports::{Deadline, StateRoot};

const DEADLINE: Deadline = Deadline {
    remaining_ms: u64::MAX,
};

fn namespace() -> String {
    "ce".repeat(32)
}

fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn engine(directory: &TempDir) -> NcmEngine {
    let mut config = NcmConfig::default();
    config.terrain_resolution = 3;
    config.stm.n_centers = 8;
    config.stm.top_k_read = 8;
    config.stm.top_k_write = 8;
    config.ltm.n_centers = 8;
    config.ltm.top_k_read = 8;
    config.ltm.top_k_write = 8;
    config.hybrid_candidates = 8;
    NcmEngine::new(
        StateRoot::new(directory.path()).unwrap(),
        Arc::new(HashEncoder::new()),
        config,
    )
}

fn feedback(engine: &NcmEngine, key: &str) -> Value {
    let capsule_digest = digest(b"{}");
    let source = "feedback-source";
    let mut observation = ObserveRequest {
        idempotency_key: "seed".into(),
        payload_sha256: String::new(),
        source: SourceId(source.into()),
        key_text: "cache key".into(),
        value_text: "cache value".into(),
        affect: None,
        surprise: 0.2,
        intensity: 1.0,
        provenance: json!({"common_capsule":{"version":1,"bytes":b"{}","sha256":capsule_digest}}),
        deadline: DEADLINE,
    };
    observation.payload_sha256 = observation.canonical_payload_sha256().unwrap();
    let seeded = engine.observe(&namespace(), observation);
    assert_eq!(seeded.outcome, Outcome::Success);
    let id = seeded.payload["record_id"].as_u64().unwrap();
    let mut stable = Sha256::new();
    for field in [
        b"tracedecay.ncm.memory-reference.v1".as_slice(),
        namespace().as_bytes(),
        &id.to_be_bytes(),
        capsule_digest.as_bytes(),
    ] {
        stable.update((field.len() as u64).to_be_bytes());
        stable.update(field);
    }
    let reference = format!(
        "ncm-memory:{}",
        stable
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    let mut opaque = Sha256::new();
    opaque.update(b"tracedecay.ncm.opaque-id.v1\0");
    for field in [namespace().as_bytes(), b"idempotency-key", key.as_bytes()] {
        opaque.update((field.len() as u64).to_be_bytes());
        opaque.update(field);
    }
    json!({"action":"feedback","expected_generation":seeded.state_generation,
        "idempotency_key":opaque.finalize().iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
        "target":{"stable_memory_ref":reference,"source":source}, "signal":"helpful","weight":1.0,
        "outcome_receipt":"outcome","occurred_at":123,"evidence_digest":digest(b"evidence"),"target_digest":digest(b"target")})
}

fn seal(request: &mut Value, key: &str, operation: &str) {
    let mut semantic = request.clone();
    let object = semantic.as_object_mut().unwrap();
    object.remove("expected_generation");
    object.remove("idempotency_key");
    object.remove("feedback_delivery_capsule");
    let bytes = serde_json::to_vec(&json!({"namespace":namespace(),"operation_id":operation,
        "idempotency_key":key,"request_semantic_sha256":digest(&serde_json::to_vec(&semantic).unwrap())})).unwrap();
    request["feedback_delivery_capsule"] =
        json!({"version":1,"sha256":digest(&bytes),"bytes":bytes});
}

#[test]
fn retry_after_restart_preserves_first_delivery_and_rejects_changed_semantics() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let mut request = feedback(&live, "public-key");
    seal(&mut request, "public-key", "original-operation");
    let committed = live.common_control(&namespace(), request.clone(), DEADLINE);
    assert_eq!(committed.outcome, Outcome::Success, "{committed:?}");
    assert_eq!(
        committed.payload["feedback_delivery_capsule"],
        request["feedback_delivery_capsule"]
    );
    drop(live);
    let live = engine(&directory);
    let before = live.inspection(&namespace());
    let mut retry = request.clone();
    retry["expected_generation"] = json!(before.state_generation);
    seal(&mut retry, "public-key", "retry-operation");
    let replay = live.common_control(&namespace(), retry.clone(), DEADLINE);
    let mut expected = committed;
    expected.payload["replayed"] = json!(true);
    assert_eq!(replay, expected);
    retry["signal"] = json!("harmful");
    seal(&mut retry, "public-key", "changed-operation");
    assert_eq!(
        live.common_control(&namespace(), retry, DEADLINE).outcome,
        Outcome::Rejected(RejectReason::IdempotencyConflict)
    );
    assert_eq!(live.inspection(&namespace()), before);
}

#[test]
fn feedback_admission_is_bounded_bound_to_request_and_atomic_with_effect() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let mut request = feedback(&live, "public-key");
    seal(&mut request, "public-key", "original-operation");
    let before = live.inspection(&namespace());
    let mut bad_digest = request.clone();
    bad_digest["feedback_delivery_capsule"]["sha256"] = json!("00".repeat(32));
    let mut wrong_key = request.clone();
    seal(&mut wrong_key, "other-key", "original-operation");
    let mut wrong_semantics = request.clone();
    wrong_semantics["weight"] = json!(0.0);
    let mut too_large = request.clone();
    too_large["feedback_delivery_capsule"]["bytes"] = json!(vec![0_u8; 131_073]);
    let mut wrong_namespace = request.clone();
    let mut admission: Value = serde_json::from_slice(
        &serde_json::from_value::<Vec<u8>>(
            wrong_namespace["feedback_delivery_capsule"]["bytes"].clone(),
        )
        .unwrap(),
    )
    .unwrap();
    admission["namespace"] = json!("ef".repeat(32));
    let bytes = serde_json::to_vec(&admission).unwrap();
    wrong_namespace["feedback_delivery_capsule"] =
        json!({"version":1,"sha256":digest(&bytes),"bytes":bytes});
    for malformed in [
        bad_digest,
        wrong_key,
        wrong_semantics,
        too_large,
        wrong_namespace,
    ] {
        assert!(matches!(
            live.common_control(&namespace(), malformed, DEADLINE)
                .outcome,
            Outcome::Rejected(RejectReason::InvalidRequest(_))
        ));
        assert_eq!(live.inspection(&namespace()), before);
    }
    live.inject_fault_once(FaultPoint::BeforeCommit).unwrap();
    assert!(matches!(
        live.common_control(&namespace(), request.clone(), DEADLINE)
            .outcome,
        Outcome::Unavailable(_)
    ));
    assert_eq!(live.inspection(&namespace()), before);
    let committed = live.common_control(&namespace(), request.clone(), DEADLINE);
    assert_eq!(committed.outcome, Outcome::Success);
    assert_eq!(committed.payload["replayed"], false);
    assert_eq!(
        committed.payload["feedback_delivery_capsule"],
        request["feedback_delivery_capsule"]
    );
}

#[test]
fn legacy_common_feedback_without_capsule_replays_without_inventing_public_identity() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let request = feedback(&live, "legacy-key");
    let committed = live.common_control(&namespace(), request.clone(), DEADLINE);
    assert_eq!(committed.outcome, Outcome::Success);
    assert!(committed.payload.get("feedback_delivery_capsule").is_none());
    drop(live);
    let live = engine(&directory);
    let replay = live.common_control(&namespace(), request, DEADLINE);
    let mut expected = committed;
    expected.payload["replayed"] = json!(true);
    assert_eq!(replay, expected);
    assert!(replay.payload.get("feedback_delivery_capsule").is_none());
}
