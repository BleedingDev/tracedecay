#![allow(
    clippy::expect_used,
    clippy::field_reassign_with_default,
    clippy::panic,
    clippy::unwrap_used
)]
#![doc = "Durable public feedback delivery identity and legacy wire compatibility."]

use rusqlite::Connection;
use serde::Serialize;
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

fn observation() -> ObserveRequest {
    let bytes = serde_json::to_vec(&json!({
        "canonical_payload": {"command": "cache key", "outcome_summary": "cache value"},
        "projection": {
            "key_text": "cache key",
            "value_text": "cache value",
            "observation_kind": "tool.execution_settled.v1"
        },
        "source_refs": ["record:feedback-source"]
    }))
    .unwrap();
    let mut observation = ObserveRequest {
        idempotency_key: "seed".into(),
        payload_sha256: String::new(),
        source: SourceId("feedback-source".into()),
        key_text: "cache key".into(),
        value_text: "cache value".into(),
        affect: None,
        surprise: 0.2,
        intensity: 1.0,
        provenance: json!({
            "common_capsule": {"version": 1, "sha256": digest(&bytes), "bytes": bytes},
            "selection": {"revision_digest": digest(b"initial revision")}
        }),
        deadline: DEADLINE,
    };
    observation.payload_sha256 = observation.canonical_payload_sha256().unwrap();
    observation
}

fn feedback(engine: &NcmEngine, key: &str) -> Value {
    let observation = observation();
    let capsule_digest = observation.provenance["common_capsule"]["sha256"]
        .as_str()
        .unwrap()
        .to_owned();
    let source = observation.source.0.clone();
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

fn correction(engine: &NcmEngine, key: &str) -> Value {
    let mut request = feedback(engine, key);
    let object = request.as_object_mut().unwrap();
    for field in ["signal", "weight", "outcome_receipt", "occurred_at"] {
        object.remove(field);
    }
    request["action"] = json!("correction");
    request["expected_revision"] = json!(digest(b"initial revision"));
    request["correction_kind"] = json!("record_validity");
    request["selection_patch"] = json!({"revoked_at": 123});
    request["validity_patch"] = json!({"revoked_at": "2025-01-01T00:00:00Z"});
    request
}

fn inspected_target(engine: &NcmEngine, target: &Value) -> Value {
    let generation = engine.inspection(&namespace());
    assert_eq!(generation.outcome, Outcome::Success, "{generation:?}");
    let inspection = engine.common_control(
        &namespace(),
        json!({
            "action": "inspection",
            "view": "trace",
            "expected_generation": generation.state_generation,
            "source": target["source"],
            "stable_memory_ref": target["stable_memory_ref"],
            "maximum_items": 8
        }),
        DEADLINE,
    );
    assert_eq!(inspection.outcome, Outcome::Success, "{inspection:?}");
    let items = inspection.payload["items"].as_array().unwrap();
    assert_eq!(items.len(), 1, "{inspection:?}");
    items[0].clone()
}

fn ingest_payload_digest(directory: &TempDir) -> String {
    let connection = Connection::open_with_flags(
        directory
            .path()
            .join("namespaces")
            .join(namespace())
            .join("ncm.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    connection
        .query_row(
            "SELECT payload_sha256 FROM events WHERE kind = 'observe' AND idempotency_key = 'seed'",
            [],
            |row| row.get(0),
        )
        .unwrap()
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
    let original = observation();
    drop(live);
    let ingest_digest = ingest_payload_digest(&directory);
    assert_eq!(ingest_digest, original.payload_sha256);
    let live = engine(&directory);
    seal(&mut request, "public-key", "original-operation");
    let committed = live.common_control(&namespace(), request.clone(), DEADLINE);
    assert_eq!(committed.outcome, Outcome::Success, "{committed:?}");
    assert_eq!(
        committed.payload["feedback_delivery_capsule"],
        request["feedback_delivery_capsule"]
    );
    let retained = inspected_target(&live, &request["target"]);
    assert_eq!(
        retained["stable_memory_ref"],
        request["target"]["stable_memory_ref"]
    );
    assert_eq!(
        retained["provenance"]["common_capsule"],
        original.provenance["common_capsule"]
    );
    assert_eq!(
        retained["provenance"]["selection"],
        original.provenance["selection"]
    );
    assert_eq!(
        retained["provenance"]["control"]["feedback"]["suppressed"],
        false
    );
    assert_eq!(retained["provenance"]["control"]["feedback"]["count"], 1);
    assert_eq!(
        retained["provenance"]["control"]["feedback"]["settled_counts"]["helpful"],
        1
    );
    drop(live);
    assert_eq!(ingest_payload_digest(&directory), ingest_digest);
    let live = engine(&directory);
    let handshake = live.handshake(&namespace());
    assert_eq!(handshake.outcome, Outcome::Success, "{handshake:?}");
    let before = live.inspection(&namespace());
    assert_eq!(before.outcome, Outcome::Success, "{before:?}");
    let reopened = inspected_target(&live, &request["target"]);
    assert_eq!(reopened, retained);
    let mut recovered_ingest = observation();
    recovered_ingest.provenance = reopened["provenance"].clone();
    recovered_ingest
        .provenance
        .as_object_mut()
        .unwrap()
        .remove("control");
    assert_eq!(
        recovered_ingest.canonical_payload_sha256().unwrap(),
        original.payload_sha256
    );
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
    drop(live);
    assert_eq!(ingest_payload_digest(&directory), ingest_digest);
}

#[test]
fn metadata_correction_survives_restart_with_effective_selection() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let request = correction(&live, "metadata-correction");
    drop(live);
    let ingest_digest = ingest_payload_digest(&directory);
    assert_eq!(ingest_digest, observation().payload_sha256);
    let live = engine(&directory);
    let initial = inspected_target(&live, &request["target"]);
    assert_eq!(initial["active"], true);
    let committed = live.common_control(&namespace(), request.clone(), DEADLINE);
    assert_eq!(committed.outcome, Outcome::Success, "{committed:?}");
    assert_eq!(committed.payload["correction_kind"], "record_validity");
    assert_eq!(committed.payload["affected_provider_effects"], 1);
    let corrected = inspected_target(&live, &request["target"]);
    assert_eq!(corrected["stable_memory_ref"], initial["stable_memory_ref"]);
    assert_eq!(
        corrected["provenance"]["common_capsule"],
        initial["provenance"]["common_capsule"]
    );
    assert_eq!(
        corrected["provenance"]["selection"],
        initial["provenance"]["selection"]
    );
    assert_eq!(
        corrected["provenance"]["control"]["selection"]["revoked_at"],
        123
    );
    assert_eq!(
        corrected["provenance"]["control"]["validity_patch"],
        request["validity_patch"]
    );
    assert_eq!(corrected["active"], false);
    assert_eq!(corrected["disposition"], "revoked");
    drop(live);

    let reopened = engine(&directory);
    let handshake = reopened.handshake(&namespace());
    assert_eq!(handshake.outcome, Outcome::Success, "{handshake:?}");
    assert_eq!(handshake.state_generation, committed.state_generation);
    assert_eq!(inspected_target(&reopened, &request["target"]), corrected);
    drop(reopened);
    assert_eq!(ingest_payload_digest(&directory), ingest_digest);
}

#[test]
fn persisted_control_tampering_is_rejected_after_common_control() {
    for action in ["feedback", "correction"] {
        let directory = TempDir::new().unwrap();
        let live = engine(&directory);
        let mut request = if action == "feedback" {
            feedback(&live, "control-tamper")
        } else {
            correction(&live, "control-tamper")
        };
        if action == "feedback" {
            request["signal"] = json!("harmful");
        }
        let committed = live.common_control(&namespace(), request.clone(), DEADLINE);
        assert_eq!(
            committed.outcome,
            Outcome::Success,
            "{action}: {committed:?}"
        );
        let retained = inspected_target(&live, &request["target"]);
        if action == "feedback" {
            assert_eq!(
                retained["provenance"]["control"]["feedback"]["suppressed"],
                true
            );
            assert_eq!(retained["provenance"]["control"]["feedback"]["count"], 1);
        }
        drop(live);
        let ingest_digest = ingest_payload_digest(&directory);
        assert_eq!(ingest_digest, observation().payload_sha256);

        let connection = Connection::open(
            directory
                .path()
                .join("namespaces")
                .join(namespace())
                .join("ncm.sqlite"),
        )
        .unwrap();
        let stored: String = connection
            .query_row(
                "SELECT provenance FROM capsules WHERE record_id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let mut provenance: Value = serde_json::from_str(&stored).unwrap();
        assert_eq!(provenance, retained["provenance"]);
        if action == "feedback" {
            provenance["control"]["feedback"]["count"] = json!(2);
        } else {
            provenance["control"]["selection"]["revoked_at"] = json!(124);
        }
        let mut recovered_ingest = observation();
        recovered_ingest.provenance = provenance.clone();
        recovered_ingest
            .provenance
            .as_object_mut()
            .unwrap()
            .remove("control");
        assert_eq!(
            recovered_ingest.canonical_payload_sha256().unwrap(),
            observation().payload_sha256
        );
        connection
            .execute(
                "UPDATE capsules SET provenance = ?1 WHERE record_id = 1",
                [provenance.to_string()],
            )
            .unwrap();
        drop(connection);
        assert_eq!(ingest_payload_digest(&directory), ingest_digest);

        let reopened = engine(&directory);
        let handshake = reopened.handshake(&namespace());
        assert_eq!(
            handshake.outcome,
            Outcome::Corrupt,
            "{action}: {handshake:?}"
        );
        assert_eq!(reopened.inspection(&namespace()).outcome, Outcome::Corrupt);
    }
}

#[test]
fn noncanonical_initial_control_remains_digest_bound() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let mut request = observation();
    request.provenance = json!({
        "raw_observation": "retained ingest metadata",
        "control": {"raw_marker": "original"}
    });
    request.payload_sha256 = request.canonical_payload_sha256().unwrap();
    let committed = live.observe(&namespace(), request.clone());
    assert_eq!(committed.outcome, Outcome::Success, "{committed:?}");
    drop(live);

    let reopened = engine(&directory);
    let handshake = reopened.handshake(&namespace());
    assert_eq!(handshake.outcome, Outcome::Success, "{handshake:?}");
    assert_eq!(reopened.inspection(&namespace()).outcome, Outcome::Success);
    drop(reopened);
    assert_eq!(ingest_payload_digest(&directory), request.payload_sha256);

    let connection = Connection::open(
        directory
            .path()
            .join("namespaces")
            .join(namespace())
            .join("ncm.sqlite"),
    )
    .unwrap();
    let stored: String = connection
        .query_row(
            "SELECT provenance FROM capsules WHERE record_id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut provenance: Value = serde_json::from_str(&stored).unwrap();
    assert_eq!(provenance, request.provenance);
    provenance["control"]["raw_marker"] = json!("changed");
    let mut changed = request.clone();
    changed.provenance = provenance.clone();
    assert_ne!(
        changed.canonical_payload_sha256().unwrap(),
        request.payload_sha256
    );
    connection
        .execute(
            "UPDATE capsules SET provenance = ?1 WHERE record_id = 1",
            [provenance.to_string()],
        )
        .unwrap();
    drop(connection);
    assert_eq!(ingest_payload_digest(&directory), request.payload_sha256);

    let reopened = engine(&directory);
    let handshake = reopened.handshake(&namespace());
    assert_eq!(handshake.outcome, Outcome::Corrupt, "{handshake:?}");
    assert_eq!(reopened.inspection(&namespace()).outcome, Outcome::Corrupt);
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

#[test]
fn common_control_recovery_recomputes_semantics_from_receipt_input() {
    #[derive(Serialize)]
    struct ReplyIntegrityBasis<'a> {
        outcome: &'a Value,
        state_generation: &'a Value,
        payload: &'a Value,
    }

    #[derive(Serialize)]
    struct CommonControlIntegrityBasis<'a> {
        operations: &'a Value,
        canonical_input: &'a Value,
    }

    #[derive(Serialize)]
    struct OperationIntegrityBasis<'a> {
        common_control: CommonControlIntegrityBasis<'a>,
    }

    #[derive(Serialize)]
    struct IntegrityBasis<'a> {
        reply: ReplyIntegrityBasis<'a>,
        operation: OperationIntegrityBasis<'a>,
        state_digest: &'a str,
    }

    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let mut request = feedback(&live, "semantic-recovery-key");
    seal(&mut request, "semantic-recovery-key", "semantic-recovery");
    let committed = live.common_control(&namespace(), request.clone(), DEADLINE);
    assert_eq!(committed.outcome, Outcome::Success, "{committed:?}");
    let key = request["idempotency_key"].as_str().unwrap().to_owned();
    drop(live);

    let path = directory
        .path()
        .join("namespaces")
        .join(namespace())
        .join("ncm.sqlite");
    let connection = Connection::open(path).unwrap();
    let receipt: String = connection
        .query_row(
            "SELECT receipt FROM events WHERE idempotency_key = ?1",
            [&key],
            |row| row.get(0),
        )
        .unwrap();
    let mut receipt: Value = serde_json::from_str(&receipt).unwrap();
    assert!(receipt["operation"]["common_control"]["canonical_input"].is_object());
    let state_digest = receipt["state_digest"].as_str().unwrap().to_owned();
    let integrity_digest = |receipt: &Value| {
        let integrity = IntegrityBasis {
            reply: ReplyIntegrityBasis {
                outcome: &receipt["reply"]["outcome"],
                state_generation: &receipt["reply"]["state_generation"],
                payload: &receipt["reply"]["payload"],
            },
            operation: OperationIntegrityBasis {
                common_control: CommonControlIntegrityBasis {
                    operations: &receipt["operation"]["common_control"]["operations"],
                    canonical_input: &receipt["operation"]["common_control"]["canonical_input"],
                },
            },
            state_digest: &state_digest,
        };
        digest(&serde_json::to_vec(&integrity).unwrap())
    };
    assert_eq!(
        integrity_digest(&receipt),
        receipt["integrity_digest"].as_str().unwrap()
    );
    receipt["operation"]["common_control"]["canonical_input"] = json!({"tampered": true});
    receipt["integrity_digest"] = json!(integrity_digest(&receipt));
    connection
        .execute(
            "UPDATE events SET receipt = ?1 WHERE idempotency_key = ?2",
            [serde_json::to_string(&receipt).unwrap(), key],
        )
        .unwrap();
    drop(connection);

    let reopened = engine(&directory);
    assert_eq!(reopened.handshake(&namespace()).outcome, Outcome::Corrupt);
}
