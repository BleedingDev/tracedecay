#![allow(
    clippy::expect_used,
    clippy::field_reassign_with_default,
    clippy::panic,
    clippy::unwrap_used
)]
#![doc = "Atomic common maintenance receipts through the durable NCM engine."]

use rusqlite::Connection;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tempfile::TempDir;
use tracedecay_memory_ncm_core::types::{NcmConfig, SourceId};
use tracedecay_memory_ncm_runtime::embedding::doubles::HashEncoder;
use tracedecay_memory_ncm_runtime::engine::{
    EngineReply, FaultPoint, MaintenanceKind, MaintenanceRequest, NcmEngine, ObserveRequest,
    Outcome, RejectReason,
};
use tracedecay_memory_ncm_runtime::ports::{Deadline, StateRoot};
use tracedecay_memory_ncm_runtime::snapshot::{self, RestoreRequest};

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

fn seed(engine: &NcmEngine, source: &str) -> EngineReply {
    let capsule = serde_json::to_vec(&json!({"origin": source})).unwrap();
    let mut request = ObserveRequest {
        idempotency_key: format!("seed-{source}"),
        payload_sha256: String::new(),
        source: SourceId(source.to_owned()),
        key_text: format!("{source} key"),
        value_text: format!("{source} value"),
        affect: None,
        surprise: 0.2,
        intensity: 1.0,
        provenance: json!({"common_capsule": {"version": 1, "sha256": digest(&capsule), "bytes": capsule}}),
        deadline: DEADLINE,
    };
    request.payload_sha256 = request.canonical_payload_sha256().unwrap();
    let reply = engine.observe(&namespace(), request);
    assert_eq!(reply.outcome, Outcome::Success, "{reply:?}");
    reply
}

fn request(key: &str, task: &str, generation: u64) -> Value {
    let mut request = json!({
        "action": "maintenance", "idempotency_key": digest(key.as_bytes()),
        "expected_generation": generation, "task": task,
        "maximum_items": 100, "maximum_bytes": 1_048_576,
        "maximum_duration_millis": 60_000, "dry_run": false,
        "resume_cursor": null, "policy_revision": 1, "extensions": [],
    });
    seal(&mut request, key, "01993262-4d00-7000-8000-000000000001");
    request
}

fn seal(request: &mut Value, key: &str, operation_id: &str) {
    let semantics = json!({
        "action": "maintenance", "task": request["task"], "dry_run": request["dry_run"],
        "maximum_items": request["maximum_items"], "maximum_bytes": request["maximum_bytes"],
        "maximum_duration_millis": request["maximum_duration_millis"],
        "resume_cursor": request["resume_cursor"], "policy_revision": request["policy_revision"],
        "extensions": request["extensions"],
    });
    let admission = serde_json::to_vec(&json!({
        "namespace": namespace(), "operation_id": operation_id, "idempotency_key": key,
        "request_semantic_sha256": digest(&serde_json::to_vec(&semantics).unwrap()),
    }))
    .unwrap();
    request["maintenance_capsule"] =
        json!({"version": 1, "sha256": digest(&admission), "bytes": admission});
}

fn state(engine: &NcmEngine) -> EngineReply {
    let reply = engine.inspection(&namespace());
    assert_eq!(reply.outcome, Outcome::Success, "{reply:?}");
    reply
}

fn invoke(engine: &NcmEngine, request: Value) -> EngineReply {
    engine.common_control(&namespace(), request, DEADLINE)
}

fn inspect_receipt(engine: &NcmEngine, key: &str) -> EngineReply {
    let generation = engine.handshake(&namespace()).state_generation;
    engine.common_control(
        &namespace(),
        json!({
            "action": "inspection", "expected_generation": generation,
            "view": "maintenance_receipt", "delivery_key": digest(key.as_bytes()),
            "maximum_items": 1, "maximum_bytes": 1_048_576, "after": 0,
        }),
        DEADLINE,
    )
}

fn receipt_item(engine: &NcmEngine, key: &str) -> Value {
    let reply = inspect_receipt(engine, key);
    assert_eq!(reply.outcome, Outcome::Success, "{reply:?}");
    assert_eq!(reply.payload["partial"], false);
    let items = reply.payload["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    items[0].clone()
}

fn identity(receipt: &Value) -> Value {
    let bytes: Vec<u8> =
        serde_json::from_value(receipt["maintenance_receipt"]["admission"]["bytes"].clone())
            .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[test]
fn historical_maintenance_receipt_preserves_identity_and_counts_after_work_and_restart() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    seed(&live, "alpha");
    let generation = seed(&live, "beta").state_generation;
    let original_request = request("decay-operation", "decay", generation);
    let first = invoke(&live, original_request.clone());
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    assert_eq!(first.payload["scanned_items"], 2);
    assert_eq!(first.payload["changed_items"], 0);
    assert_eq!(first.payload["removed_items"], 0);
    assert_eq!(first.payload["state_changed"], true);
    assert_eq!(first.payload["state_generation_before"], generation);
    assert_eq!(
        first.payload["state_generation_after"],
        first.state_generation
    );
    let original_receipt = receipt_item(&live, "decay-operation");
    assert_eq!(
        identity(&original_receipt)["operation_id"],
        "01993262-4d00-7000-8000-000000000001"
    );
    assert_eq!(
        identity(&original_receipt)["idempotency_key"],
        "decay-operation"
    );
    assert_eq!(
        original_receipt["maintenance_receipt"],
        first.payload["_retained_receipt"]["payload"]["common_maintenance"]
    );

    let later_generation = seed(&live, "gamma").state_generation;
    let before_retry = state(&live);
    let mut retry = original_request.clone();
    retry["expected_generation"] = json!(later_generation);
    seal(
        &mut retry,
        "decay-operation",
        "01993262-4d00-7000-8000-000000000099",
    );
    let replay = invoke(&live, retry.clone());
    let mut expected = first.clone();
    expected.payload["replayed"] = json!(true);
    expected.payload["_retained_receipt"]["payload"]["replayed"] = json!(true);
    assert_eq!(replay, expected);
    assert_eq!(
        state(&live).payload["state_digest"],
        before_retry.payload["state_digest"]
    );
    drop(live);

    let reopened = engine(&directory);
    assert_eq!(invoke(&reopened, retry), expected);
    assert_eq!(receipt_item(&reopened, "decay-operation"), original_receipt);
    let mut changed = original_request;
    changed["expected_generation"] = json!(later_generation);
    changed["maximum_items"] = json!(101);
    seal(
        &mut changed,
        "decay-operation",
        "01993262-4d00-7000-8000-000000000001",
    );
    assert_eq!(
        invoke(&reopened, changed).outcome,
        Outcome::Rejected(RejectReason::IdempotencyConflict)
    );
    assert_eq!(state(&reopened).state_generation, later_generation);
}

#[test]
fn maintenance_effect_and_common_receipt_share_commit_uncertainty() {
    for fault in [
        FaultPoint::BeforeCommit,
        FaultPoint::AfterCommitBeforePublish,
        FaultPoint::AfterPublishBeforeAck,
    ] {
        let directory = TempDir::new().unwrap();
        let live = engine(&directory);
        let generation = seed(&live, "alpha").state_generation;
        let original = request("uncertain-maintenance", "decay", generation);
        live.inject_fault_once(fault).unwrap();
        let interrupted = invoke(&live, original.clone());
        if fault == FaultPoint::BeforeCommit {
            assert!(matches!(interrupted.outcome, Outcome::Unavailable(_)));
        } else {
            assert_eq!(interrupted.outcome, Outcome::EffectUnknown);
        }
        drop(live);

        let reopened = engine(&directory);
        let before_retry = state(&reopened);
        let retained_before_retry = inspect_receipt(&reopened, "uncertain-maintenance");
        assert_eq!(retained_before_retry.outcome, Outcome::Success);
        if fault == FaultPoint::BeforeCommit {
            assert_eq!(before_retry.state_generation, generation);
            assert!(
                retained_before_retry.payload["items"]
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
        } else {
            assert_eq!(before_retry.state_generation, generation + 1);
            assert_eq!(
                retained_before_retry.payload["items"]
                    .as_array()
                    .unwrap()
                    .len(),
                1
            );
        }
        let mut retry = original;
        retry["expected_generation"] = json!(before_retry.state_generation);
        let completed = invoke(&reopened, retry);
        assert_eq!(completed.outcome, Outcome::Success, "{completed:?}");
        assert_eq!(completed.state_generation, generation + 1);
        assert_eq!(completed.payload["changed_items"], 0);
        assert_eq!(completed.payload["state_changed"], true);
        assert_eq!(
            completed.payload["replayed"],
            fault != FaultPoint::BeforeCommit
        );
        let retained = receipt_item(&reopened, "uncertain-maintenance");
        assert_eq!(
            retained["maintenance_receipt"],
            completed.payload["_retained_receipt"]["payload"]["common_maintenance"]
        );
        if fault != FaultPoint::BeforeCommit {
            assert_eq!(retained, retained_before_retry.payload["items"][0]);
        }
    }
}

#[test]
fn maintenance_cursor_retries_reconcile_before_stale_generation_checks() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    seed(&live, "alpha");
    seed(&live, "beta");
    let generation = seed(&live, "gamma").state_generation;
    let mut page = request("paged-repair", "repair", generation);
    page["maximum_items"] = json!(1);
    for expected_after in [1, 2] {
        seal(
            &mut page,
            "paged-repair",
            "01993262-4d00-7000-8000-000000000001",
        );
        let partial = invoke(&live, page.clone());
        assert_eq!(partial.outcome, Outcome::Success);
        assert_eq!(partial.state_generation, generation);
        assert_eq!(partial.payload["no_change"], true);
        assert_eq!(partial.payload["partial"], true);
        assert!(partial.payload.get("state_changed").is_none());
        assert_eq!(
            partial.payload["resume_cursor"],
            format!(
                "ncm-maintenance:{}:repair:{generation}:{expected_after}",
                namespace()
            )
        );
        page["resume_cursor"] = partial.payload["resume_cursor"].clone();
    }
    seal(
        &mut page,
        "paged-repair",
        "01993262-4d00-7000-8000-000000000001",
    );
    let committed = invoke(&live, page.clone());
    assert_eq!(committed.outcome, Outcome::Success, "{committed:?}");
    assert_eq!(committed.payload["partial"], false);
    assert_eq!(committed.payload["scanned_items"], 1);
    assert_eq!(committed.payload["changed_items"], 0);
    assert_eq!(committed.payload["state_changed"], false);
    page["expected_generation"] = json!(committed.state_generation);
    let replay = invoke(&live, page.clone());
    assert_eq!(replay.outcome, Outcome::Success);
    assert_eq!(replay.state_generation, committed.state_generation);
    assert_eq!(replay.payload["replayed"], true);

    let mut fresh = page.clone();
    fresh["idempotency_key"] = json!(digest(b"fresh-stale-cursor"));
    seal(
        &mut fresh,
        "fresh-stale-cursor",
        "01993262-4d00-7000-8000-000000000002",
    );
    assert_eq!(
        invoke(&live, fresh).outcome,
        Outcome::Rejected(RejectReason::IdempotencyConflict)
    );
    let mut numeric_cursor = page.clone();
    numeric_cursor["after"] = json!(2);
    assert!(matches!(
        invoke(&live, numeric_cursor).outcome,
        Outcome::Rejected(_)
    ));
    assert_eq!(
        live.common_control(&namespace(), page, Deadline { remaining_ms: 0 })
            .outcome,
        Outcome::Cancelled
    );
    assert_eq!(state(&live).state_generation, committed.state_generation);
}

#[test]
fn maintenance_receipt_survives_snapshot_restore_and_privacy_rebuild() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    seed(&live, "alpha");
    let generation = seed(&live, "beta").state_generation;
    let original = request("consolidate-before-snapshot", "consolidate", generation);
    let completed = invoke(&live, original.clone());
    assert_eq!(completed.outcome, Outcome::Success, "{completed:?}");
    let retained = receipt_item(&live, "consolidate-before-snapshot");
    let bytes = snapshot::export(&live, &namespace(), DEADLINE)
        .unwrap()
        .into_vec();
    seed(&live, "discarded-later");
    let restored = snapshot::restore(
        &live,
        &namespace(),
        RestoreRequest {
            idempotency_key: "restore-maintenance".to_owned(),
            bytes,
        },
        DEADLINE,
    );
    assert_eq!(restored.outcome, Outcome::Success, "{restored:?}");
    assert_eq!(receipt_item(&live, "consolidate-before-snapshot"), retained);
    let deleted = live.delete_by_source(
        &namespace(),
        &SourceId("alpha".to_owned()),
        "delete-alpha",
        DEADLINE,
    );
    assert_eq!(deleted.outcome, Outcome::Success, "{deleted:?}");
    assert_eq!(state(&live).payload["records"], 1);
    assert_eq!(receipt_item(&live, "consolidate-before-snapshot"), retained);
    drop(live);

    let reopened = engine(&directory);
    let before_retry = state(&reopened);
    let mut retry = original;
    retry["expected_generation"] = json!(before_retry.state_generation);
    let duplicate = invoke(&reopened, retry);
    assert_eq!(duplicate.outcome, Outcome::Success);
    assert_eq!(duplicate.state_generation, completed.state_generation);
    assert_eq!(duplicate.payload["replayed"], true);
    assert_eq!(
        receipt_item(&reopened, "consolidate-before-snapshot"),
        retained
    );
    assert_eq!(
        state(&reopened).payload["state_digest"],
        before_retry.payload["state_digest"]
    );
}

#[test]
fn compact_records_actual_measurements_without_claiming_record_or_kernel_changes() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let generation = seed(&live, "alpha").state_generation;
    let original = request("common-compact", "compact", generation);
    let compacted = invoke(&live, original.clone());
    assert_eq!(compacted.outcome, Outcome::Success, "{compacted:?}");
    assert_eq!(compacted.payload["changed_items"], 0);
    assert_eq!(compacted.payload["removed_items"], 0);
    assert_eq!(compacted.payload["state_changed"], false);
    let basis = &compacted.payload["_retained_receipt"]["payload"];
    assert_eq!(basis["compact"], true);
    assert!(basis["bytes_before"].as_u64().is_some());
    assert!(basis["bytes_after"].as_u64().is_some());
    let retained = receipt_item(&live, "common-compact");
    let mut retry = original;
    retry["expected_generation"] = json!(seed(&live, "beta").state_generation);
    let replay = invoke(&live, retry);
    assert_eq!(replay.outcome, Outcome::Success);
    assert_eq!(
        replay.payload["_retained_receipt"]["payload"]["bytes_after"],
        basis["bytes_after"]
    );
    assert_eq!(receipt_item(&live, "common-compact"), retained);

    let raw = live.maintenance(
        &namespace(),
        MaintenanceRequest {
            idempotency_key: digest(b"legacy-raw-maintenance"),
            kind: MaintenanceKind::Checkpoint,
            deadline: DEADLINE,
        },
    );
    assert_eq!(raw.outcome, Outcome::Success);
    let absent = inspect_receipt(&live, "legacy-raw-maintenance");
    assert_eq!(absent.outcome, Outcome::Success);
    assert!(absent.payload["items"].as_array().unwrap().is_empty());
    assert_eq!(absent.payload["partial"], true);
}

#[test]
fn corrupted_common_maintenance_evidence_is_rejected_for_retry_inspection_and_export() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let generation = seed(&live, "alpha").state_generation;
    let original = request("corrupt-receipt", "repair", generation);
    let first = invoke(&live, original.clone());
    assert_eq!(first.outcome, Outcome::Success);
    drop(live);

    let path = directory
        .path()
        .join("namespaces")
        .join(namespace())
        .join("ncm.sqlite");
    let connection = Connection::open(path).unwrap();
    let key = digest(b"corrupt-receipt");
    let receipt: String = connection
        .query_row(
            "SELECT receipt FROM events WHERE idempotency_key = ?1",
            [&key],
            |row| row.get(0),
        )
        .unwrap();
    let mut receipt: Value = serde_json::from_str(&receipt).unwrap();
    receipt["reply"]["payload"]["common_maintenance"]["event_basis"]["sequence"] = json!(999);
    connection
        .execute(
            "UPDATE events SET receipt = ?1 WHERE idempotency_key = ?2",
            [serde_json::to_string(&receipt).unwrap(), key],
        )
        .unwrap();
    drop(connection);

    let reopened = engine(&directory);
    assert_eq!(
        inspect_receipt(&reopened, "corrupt-receipt").outcome,
        Outcome::Corrupt
    );
    let mut retry = original;
    retry["expected_generation"] = json!(first.state_generation);
    assert_eq!(invoke(&reopened, retry).outcome, Outcome::Corrupt);
    assert_eq!(
        snapshot::export(&reopened, &namespace(), DEADLINE)
            .unwrap_err()
            .outcome,
        Outcome::Corrupt
    );
}

#[test]
fn empty_and_dry_run_maintenance_do_not_invent_retained_effects() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let empty = invoke(&live, request("empty-maintenance", "decay", 0));
    assert_eq!(empty.outcome, Outcome::Success);
    assert_eq!(empty.state_generation, 0);
    assert_eq!(empty.payload["scanned_items"], 0);
    assert_eq!(empty.payload["no_change"], true);
    assert!(empty.payload.get("state_changed").is_none());
    assert!(
        !directory
            .path()
            .join("namespaces")
            .join(namespace())
            .exists()
    );
    assert_eq!(
        invoke(&live, request("invalid-empty-generation", "decay", 1)).outcome,
        Outcome::Rejected(RejectReason::IdempotencyConflict)
    );
    let generation = seed(&live, "alpha").state_generation;
    let mut dry_run = request("dry-run", "decay", generation);
    dry_run["dry_run"] = json!(true);
    seal(
        &mut dry_run,
        "dry-run",
        "01993262-4d00-7000-8000-000000000001",
    );
    let preview = invoke(&live, dry_run);
    assert_eq!(preview.outcome, Outcome::Success);
    assert_eq!(preview.state_generation, generation);
    assert_eq!(preview.payload["scanned_items"], 1);
    assert_eq!(preview.payload["changed_items"], 0);
    assert!(preview.payload.get("state_changed").is_none());
    assert_eq!(inspect_receipt(&live, "dry-run").payload["partial"], true);
}
