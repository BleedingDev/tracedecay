#![allow(
    clippy::expect_used,
    clippy::field_reassign_with_default,
    clippy::panic,
    clippy::unwrap_used
)]
#![doc = "Durable replay cursor validation tests against forged journal receipts."]

use rusqlite::{Connection, params};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tempfile::TempDir;
use tracedecay_memory_ncm_core::types::{NcmConfig, SourceId};
use tracedecay_memory_ncm_runtime::embedding::doubles::HashEncoder;
use tracedecay_memory_ncm_runtime::engine::{NcmEngine, ObserveRequest, Outcome, RejectReason};
use tracedecay_memory_ncm_runtime::ports::{Deadline, StateRoot};

const DEADLINE: Deadline = Deadline {
    remaining_ms: u64::MAX,
};

fn digest(bytes: impl AsRef<[u8]>) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn namespace() -> String {
    "ec".repeat(32)
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

fn observation(source: &str, key: &str) -> ObserveRequest {
    let mut request = ObserveRequest {
        idempotency_key: digest(key),
        payload_sha256: String::new(),
        source: SourceId(digest(source)),
        key_text: format!("{source} knowledge"),
        value_text: format!("{source} retained answer"),
        affect: None,
        surprise: 0.4,
        intensity: 1.0,
        provenance: json!({
            "common_capsule": {
                "version": 1,
                "bytes": [],
                "sha256": digest("")
            },
            "selection": {
                "observation_identity": digest(format!("observation-{source}")),
                "revision_digest": digest("revision-1")
            }
        }),
        deadline: DEADLINE,
    };
    request.payload_sha256 = request.canonical_payload_sha256().unwrap();
    request
}

fn item(sequence: u64, source: &str, key: &str) -> Value {
    let request = observation(source, key);
    json!({
        "source_sequence": sequence,
        "receipt_digest": digest(format!("receipt-{sequence}")),
        "delivery_key": request.idempotency_key,
        "source": request.source.0,
        "admitted": true,
        "blocked": false,
        "observation": {
            "idempotency_key": request.idempotency_key,
            "payload_sha256": request.payload_sha256,
            "source": request.source.0,
            "key_text": request.key_text,
            "value_text": request.value_text,
            "affect": request.affect,
            "surprise": request.surprise,
            "intensity": request.intensity,
            "provenance": request.provenance
        }
    })
}

fn page(key: &str, generation: u64, previous: u64, items: Vec<Value>) -> Value {
    json!({
        "action": "replay",
        "idempotency_key": digest(key),
        "expected_generation": generation,
        "first_source_sequence": items.first().unwrap()["source_sequence"],
        "last_source_sequence": items.last().unwrap()["source_sequence"],
        "expected_previous_acknowledged_sequence": previous,
        "items": items
    })
}

fn blocked_item(sequence: u64, source: &str, delivery_key: &str, receipt: &str) -> Value {
    json!({
        "source_sequence": sequence,
        "receipt_digest": receipt,
        "delivery_key": digest(delivery_key),
        "source": digest(source),
        "admitted": false,
        "blocked": true,
        "observation": null
    })
}

fn delivery_capsule(operation: &str, key: &str) -> Value {
    let bytes = serde_json::to_vec(&json!({
        "operation_id": operation,
        "idempotency_key": key
    }))
    .unwrap();
    json!({
        "version": 1,
        "sha256": digest(&bytes),
        "bytes": bytes
    })
}

fn public_page(
    key: &str,
    operation: &str,
    generation: u64,
    previous: u64,
    items: Vec<Value>,
) -> Value {
    let mut page = page(key, generation, previous, items);
    page["page_delivery_capsule"] = delivery_capsule(operation, key);
    page
}

fn inspection(engine: &NcmEngine) -> Value {
    let reply = engine.inspection(&namespace());
    assert_eq!(reply.outcome, Outcome::Success, "{reply:?}");
    let mut payload = reply.payload;
    // Receipt-forging fixtures intentionally rewrite the event row, which
    // changes physical event/WAL accounting. Compare the durable namespace
    // projection below; the state digest, commit sequence, records, sources,
    // and all other logical fields still prove that replay added no effect.
    payload.as_object_mut().unwrap().remove("quota_usage");
    payload
}

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

fn recompute_integrity(receipt: &Value) -> String {
    let basis = IntegrityBasis {
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
        state_digest: receipt["state_digest"].as_str().unwrap(),
    };
    digest(serde_json::to_vec(&basis).unwrap())
}

fn mutate_receipt(directory: &TempDir, mutate: impl FnOnce(&mut Value)) {
    let path = directory
        .path()
        .join("namespaces")
        .join(namespace())
        .join("ncm.sqlite");
    let connection = Connection::open(path).unwrap();
    let mut statement = connection
        .prepare("SELECT seq, receipt FROM events WHERE kind = 'common_control' ORDER BY seq ASC")
        .unwrap();
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(statement);
    let (sequence, receipt_json) = rows
        .into_iter()
        .find(|(_, receipt)| {
            serde_json::from_str::<Value>(receipt)
                .map(|receipt| receipt["reply"]["payload"]["common_portability"] == "replay")
                .unwrap_or(false)
        })
        .expect("replay page receipt exists");
    let mut receipt: Value = serde_json::from_str(&receipt_json).unwrap();
    assert_eq!(
        recompute_integrity(&receipt),
        receipt["integrity_digest"].as_str().unwrap()
    );
    mutate(&mut receipt);
    receipt["integrity_digest"] = json!(recompute_integrity(&receipt));
    connection
        .execute(
            "UPDATE events SET receipt = ?1 WHERE seq = ?2",
            params![serde_json::to_string(&receipt).unwrap(), sequence],
        )
        .unwrap();
}

fn mutate_item_receipt(directory: &TempDir) {
    let path = directory
        .path()
        .join("namespaces")
        .join(namespace())
        .join("ncm.sqlite");
    let connection = Connection::open(path).unwrap();
    let (sequence, receipt_json): (i64, String) = connection
        .query_row(
            "SELECT seq, receipt FROM events WHERE kind = 'common_control' ORDER BY seq ASC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let mut receipt: Value = serde_json::from_str(&receipt_json).unwrap();
    assert_eq!(
        recompute_integrity(&receipt),
        receipt["integrity_digest"].as_str().unwrap()
    );
    receipt["reply"]["payload"]["source_sequence"] = json!(3);
    receipt["integrity_digest"] = json!(recompute_integrity(&receipt));
    connection
        .execute(
            "UPDATE events SET receipt = ?1 WHERE seq = ?2",
            params![serde_json::to_string(&receipt).unwrap(), sequence],
        )
        .unwrap();
}

fn mutate_item_event_key(directory: &TempDir) {
    let path = directory
        .path()
        .join("namespaces")
        .join(namespace())
        .join("ncm.sqlite");
    let connection = Connection::open(path).unwrap();
    let mut statement = connection
        .prepare("SELECT seq, receipt FROM events WHERE kind = 'common_control' ORDER BY seq ASC")
        .unwrap();
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(statement);
    let (sequence, receipt_json) = rows
        .into_iter()
        .find(|(_, receipt)| {
            serde_json::from_str::<Value>(receipt)
                .map(|receipt| receipt["reply"]["payload"]["common_portability"] == "replay_item")
                .unwrap_or(false)
        })
        .expect("replay item receipt exists");
    let mut receipt: Value = serde_json::from_str(&receipt_json).unwrap();
    assert_eq!(
        recompute_integrity(&receipt),
        receipt["integrity_digest"].as_str().unwrap()
    );
    let forged_key = digest("forged-item-event-key");
    receipt["idempotency_key"] = json!(forged_key);
    connection
        .execute(
            "UPDATE events SET idempotency_key = ?1, receipt = ?2 WHERE seq = ?3",
            params![
                forged_key,
                serde_json::to_string(&receipt).unwrap(),
                sequence
            ],
        )
        .unwrap();
}

fn mutate_page_output_record(directory: &TempDir) {
    mutate_receipt(directory, |receipt| {
        receipt["reply"]["payload"]["items"][0]["record_id"] = json!(9_999);
    });
}

fn mutate_page_output_field(directory: &TempDir, field: &str, value: Value) {
    mutate_receipt(directory, |receipt| {
        receipt["reply"]["payload"]["items"][0][field] = value;
    });
}

fn mutate_page_before_generation(directory: &TempDir) {
    mutate_receipt(directory, |receipt| {
        receipt["reply"]["payload"]["state_generation_before"] = json!(1);
    });
}

fn mutate_page_capsule_key(directory: &TempDir) {
    mutate_receipt(directory, |receipt| {
        let capsule = &mut receipt["reply"]["payload"]["page_delivery_capsule"];
        let bytes = capsule["bytes"].as_array_mut().unwrap();
        let mut delivery: Value = serde_json::from_slice(
            &bytes
                .iter()
                .map(|value| value.as_u64().unwrap() as u8)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        delivery["idempotency_key"] = json!("forged-page-key");
        let encoded = serde_json::to_vec(&delivery).unwrap();
        capsule["bytes"] = json!(encoded);
        capsule["sha256"] = json!(digest(&encoded));
    });
}

#[test]
fn forged_page_acknowledgement_is_rejected_before_successor_mutation() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let first_page = page("first", 0, 0, vec![item(1, "alpha", "first-delivery")]);
    let first = live.common_portability(&namespace(), first_page.clone(), DEADLINE);
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    let before = inspection(&live);
    drop(live);

    mutate_receipt(&directory, |receipt| {
        receipt["reply"]["payload"]["acknowledged_sequence"] = json!(9);
    });
    let reopened = engine(&directory);
    assert_eq!(inspection(&reopened), before);
    let forged_retry = reopened.common_portability(&namespace(), first_page, DEADLINE);
    assert_eq!(forged_retry.outcome, Outcome::Corrupt, "{forged_retry:?}");
    assert_eq!(inspection(&reopened), before);
    let forged_successor = reopened.common_portability(
        &namespace(),
        page(
            "forged-successor",
            before["commit_seq"].as_u64().unwrap(),
            9,
            vec![item(10, "successor", "successor-delivery")],
        ),
        DEADLINE,
    );
    assert_eq!(
        forged_successor.outcome,
        Outcome::Corrupt,
        "{forged_successor:?}"
    );
    assert_eq!(inspection(&reopened), before);
}

#[test]
fn out_of_order_item_receipt_is_rejected_before_successor_mutation() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let first_page = page("first", 0, 0, vec![item(1, "alpha", "first-delivery")]);
    let first = live.common_portability(&namespace(), first_page.clone(), DEADLINE);
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    let before = inspection(&live);
    drop(live);

    mutate_item_receipt(&directory);
    let reopened = engine(&directory);
    assert_eq!(inspection(&reopened), before);
    let forged_retry = reopened.common_portability(&namespace(), first_page, DEADLINE);
    assert_eq!(forged_retry.outcome, Outcome::Corrupt, "{forged_retry:?}");
    assert_eq!(inspection(&reopened), before);
    let forged_successor = reopened.common_portability(
        &namespace(),
        page(
            "forged-successor",
            before["commit_seq"].as_u64().unwrap(),
            3,
            vec![item(4, "successor", "successor-delivery")],
        ),
        DEADLINE,
    );
    assert_eq!(
        forged_successor.outcome,
        Outcome::Corrupt,
        "{forged_successor:?}"
    );
    assert_eq!(inspection(&reopened), before);
}

#[test]
fn forged_item_event_identity_is_rejected_before_successor_mutation() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let first_page = page("first", 0, 0, vec![item(1, "alpha", "first-delivery")]);
    let first = live.common_portability(&namespace(), first_page.clone(), DEADLINE);
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    let before = inspection(&live);
    drop(live);

    mutate_item_event_key(&directory);
    let reopened = engine(&directory);
    assert_eq!(inspection(&reopened), before);
    let forged_retry = reopened.common_portability(&namespace(), first_page, DEADLINE);
    assert_eq!(forged_retry.outcome, Outcome::Corrupt, "{forged_retry:?}");
    assert_eq!(inspection(&reopened), before);
    let forged_successor = reopened.common_portability(
        &namespace(),
        page(
            "forged-successor",
            before["commit_seq"].as_u64().unwrap(),
            1,
            vec![item(2, "successor", "successor-delivery")],
        ),
        DEADLINE,
    );
    assert_eq!(
        forged_successor.outcome,
        Outcome::Corrupt,
        "{forged_successor:?}"
    );
    assert_eq!(inspection(&reopened), before);
}

#[test]
fn blocked_identity_mismatch_is_rejected_before_source_fence() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let first_page = page("first", 0, 0, vec![item(1, "alpha", "first-delivery")]);
    let first = live.common_portability(&namespace(), first_page, DEADLINE);
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    let before = inspection(&live);
    let before_revocations = live.revoked_sources(&namespace()).unwrap();

    let rejected = live.common_portability(
        &namespace(),
        page(
            "blocked-mismatch",
            before["commit_seq"].as_u64().unwrap(),
            1,
            vec![blocked_item(
                1,
                "alpha",
                "blocked-delivery",
                &digest("forged-receipt"),
            )],
        ),
        DEADLINE,
    );
    assert_eq!(
        rejected.outcome,
        Outcome::Rejected(RejectReason::IdempotencyConflict),
        "{rejected:?}"
    );
    assert_eq!(inspection(&live), before);
    assert_eq!(
        live.revoked_sources(&namespace()).unwrap(),
        before_revocations
    );
}

#[test]
fn blocked_reused_delivery_key_is_rejected_before_source_fence() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let first_page = page("first", 0, 0, vec![item(1, "alpha", "first-delivery")]);
    let first = live.common_portability(&namespace(), first_page, DEADLINE);
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    let before = inspection(&live);
    let before_revocations = live.revoked_sources(&namespace()).unwrap();
    let reused_receipt = digest("receipt-1");

    let rejected = live.common_portability(
        &namespace(),
        page(
            "blocked-reused-key",
            before["commit_seq"].as_u64().unwrap(),
            1,
            vec![blocked_item(
                2,
                "blocked",
                "first-delivery",
                &reused_receipt,
            )],
        ),
        DEADLINE,
    );
    assert_eq!(rejected.outcome, Outcome::Corrupt, "{rejected:?}");
    assert_eq!(inspection(&live), before);
    assert_eq!(
        live.revoked_sources(&namespace()).unwrap(),
        before_revocations
    );
}

#[test]
fn forged_page_output_record_is_rejected_before_successor_mutation() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let first_page = page("first", 0, 0, vec![item(1, "alpha", "first-delivery")]);
    let first = live.common_portability(&namespace(), first_page.clone(), DEADLINE);
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    let before = inspection(&live);
    drop(live);

    mutate_page_output_record(&directory);
    let reopened = engine(&directory);
    let forged_retry = reopened.common_portability(&namespace(), first_page, DEADLINE);
    assert_eq!(forged_retry.outcome, Outcome::Corrupt, "{forged_retry:?}");
    assert_eq!(inspection(&reopened), before);
}

#[test]
fn forged_page_output_state_generation_or_receipt_is_rejected_before_successor_mutation() {
    for (field, value) in [
        ("state", json!("source_already_applied")),
        ("state_generation", json!(9_999)),
        ("receipt_digest", json!(digest("forged-page-receipt"))),
    ] {
        let directory = TempDir::new().unwrap();
        let live = engine(&directory);
        let first_page = page("first", 0, 0, vec![item(1, "alpha", "first-delivery")]);
        let first = live.common_portability(&namespace(), first_page.clone(), DEADLINE);
        assert_eq!(first.outcome, Outcome::Success, "{first:?}");
        let before = inspection(&live);
        drop(live);

        mutate_page_output_field(&directory, field, value);
        let reopened = engine(&directory);
        let forged_retry = reopened.common_portability(&namespace(), first_page, DEADLINE);
        assert_eq!(forged_retry.outcome, Outcome::Corrupt, "{forged_retry:?}");
        assert_eq!(inspection(&reopened), before);
    }
}

#[test]
fn forged_page_generation_is_rejected_before_successor_mutation() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let first_page = page("first", 0, 0, vec![item(1, "alpha", "first-delivery")]);
    let first = live.common_portability(&namespace(), first_page.clone(), DEADLINE);
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    let before = inspection(&live);
    drop(live);

    mutate_page_before_generation(&directory);
    let reopened = engine(&directory);
    let forged_retry = reopened.common_portability(&namespace(), first_page, DEADLINE);
    assert_eq!(forged_retry.outcome, Outcome::Corrupt, "{forged_retry:?}");
    assert_eq!(inspection(&reopened), before);
}

#[test]
fn forged_page_after_generation_is_rejected_before_successor_mutation() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let first_page = page("first", 0, 0, vec![item(1, "alpha", "first-delivery")]);
    let first = live.common_portability(&namespace(), first_page.clone(), DEADLINE);
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    let before = inspection(&live);
    drop(live);

    mutate_receipt(&directory, |receipt| {
        receipt["reply"]["payload"]["state_generation_after"] = json!(0);
    });
    let reopened = engine(&directory);
    let forged_retry = reopened.common_portability(&namespace(), first_page, DEADLINE);
    assert_eq!(forged_retry.outcome, Outcome::Corrupt, "{forged_retry:?}");
    assert_eq!(inspection(&reopened), before);
}

#[test]
fn forged_page_delivery_capsule_binding_is_rejected_before_successor_mutation() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let first_page = public_page(
        "first-public",
        "original-operation",
        0,
        0,
        vec![item(1, "alpha", "first-delivery")],
    );
    let first = live.common_portability(&namespace(), first_page.clone(), DEADLINE);
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    let before = inspection(&live);
    drop(live);

    mutate_page_capsule_key(&directory);
    let reopened = engine(&directory);
    let forged_retry = reopened.common_portability(&namespace(), first_page, DEADLINE);
    assert_eq!(forged_retry.outcome, Outcome::Corrupt, "{forged_retry:?}");
    assert_eq!(inspection(&reopened), before);
}

#[test]
fn unknown_durable_receipt_fields_are_rejected_before_successor_mutation() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let first_page = page("first", 0, 0, vec![item(1, "alpha", "first-delivery")]);
    let first = live.common_portability(&namespace(), first_page.clone(), DEADLINE);
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    let before = inspection(&live);
    drop(live);

    mutate_receipt(&directory, |receipt| {
        receipt["forged_envelope_field"] = json!(true);
    });
    let reopened = engine(&directory);
    let forged_retry = reopened.common_portability(&namespace(), first_page, DEADLINE);
    assert_eq!(forged_retry.outcome, Outcome::Corrupt, "{forged_retry:?}");
    assert_eq!(inspection(&reopened), before);
}

#[test]
fn replay_page_receipt_can_be_retried_after_its_source_is_revoked() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let first_page = page("first", 0, 0, vec![item(1, "alpha", "first-delivery")]);
    let first = live.common_portability(&namespace(), first_page.clone(), DEADLINE);
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");

    let deleted = live.delete_by_source(
        &namespace(),
        &SourceId(digest("alpha")),
        &digest("delete-alpha"),
        DEADLINE,
    );
    assert_eq!(deleted.outcome, Outcome::Success, "{deleted:?}");

    let retry = live.common_portability(&namespace(), first_page, DEADLINE);
    assert_eq!(retry.outcome, Outcome::Success, "{retry:?}");
    assert_eq!(retry.payload["replayed"], true);
    assert_eq!(retry.state_generation, first.state_generation);
}
