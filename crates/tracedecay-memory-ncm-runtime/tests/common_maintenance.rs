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
fn maintenance_cursor_rejects_resume_after_generation_advance() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    seed(&live, "alpha");
    let generation = seed(&live, "beta").state_generation;
    let mut page = request("stale-maintenance", "repair", generation);
    page["maximum_items"] = json!(1);
    seal(
        &mut page,
        "stale-maintenance",
        "01993262-4d00-7000-8000-000000000003",
    );
    let partial = invoke(&live, page.clone());
    assert_eq!(partial.outcome, Outcome::Success, "{partial:?}");
    assert_eq!(partial.payload["partial"], true);
    page["resume_cursor"] = partial.payload["resume_cursor"].clone();

    let advanced = seed(&live, "gamma").state_generation;
    assert_eq!(advanced, generation + 1);
    seal(
        &mut page,
        "stale-maintenance",
        "01993262-4d00-7000-8000-000000000004",
    );
    let stale = invoke(&live, page);
    assert_eq!(
        stale.outcome,
        Outcome::Rejected(RejectReason::IdempotencyConflict)
    );
    assert_eq!(stale.state_generation, advanced);
    assert_eq!(state(&live).state_generation, advanced);
}

#[test]
fn oversized_capsule_returns_typed_capacity_without_repeating_cursor() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let generation = seed(&live, "oversized").state_generation;
    let mut oversized = request("oversized-maintenance", "repair", generation);
    oversized["maximum_items"] = json!(1);
    oversized["maximum_bytes"] = json!(1);
    seal(
        &mut oversized,
        "oversized-maintenance",
        "01993262-4d00-7000-8000-000000000005",
    );

    let first = invoke(&live, oversized.clone());
    assert_eq!(first.outcome, Outcome::BudgetExceeded, "{first:?}");
    assert_eq!(first.state_generation, generation);
    assert_eq!(first.payload, Value::Null);

    let second = invoke(&live, oversized);
    assert_eq!(second.outcome, Outcome::BudgetExceeded, "{second:?}");
    assert_eq!(second.state_generation, generation);
    assert_eq!(second.payload, Value::Null);
    assert_eq!(state(&live).state_generation, generation);
}

#[test]
fn maintenance_cursor_rejects_unissued_current_generation() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    seed(&live, "alpha");
    let generation = seed(&live, "beta").state_generation;
    let mut forged = request("forged-maintenance-cursor", "repair", generation);
    forged["maximum_items"] = json!(1);
    forged["resume_cursor"] = json!(format!(
        "ncm-maintenance:{}:repair:{generation}:2",
        namespace()
    ));
    seal(
        &mut forged,
        "forged-maintenance-cursor",
        "01993262-4d00-7000-8000-000000000006",
    );

    let reply = invoke(&live, forged);
    assert_eq!(
        reply.outcome,
        Outcome::Rejected(RejectReason::InvalidRequest(
            "maintenance cursor was not issued for this operation".to_owned()
        ))
    );
    assert_eq!(state(&live).state_generation, generation);
}

#[test]
fn maintenance_cursor_grants_keep_same_position_for_distinct_operations() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    seed(&live, "alpha");
    seed(&live, "beta");
    let generation = seed(&live, "gamma").state_generation;
    let key_a = "same-position-maintenance-a";
    let key_b = "same-position-maintenance-b";
    let operation_a = "01993262-4d00-0000-8000-000000000011";
    let operation_b = "01993262-4d00-0000-8000-000000000012";

    let mut page_a = request(key_a, "repair", generation);
    page_a["maximum_items"] = json!(1);
    seal(&mut page_a, key_a, operation_a);
    let first_a = invoke(&live, page_a.clone());
    assert_eq!(first_a.payload["partial"], true);

    let mut page_b = request(key_b, "repair", generation);
    page_b["maximum_items"] = json!(1);
    seal(&mut page_b, key_b, operation_b);
    let first_b = invoke(&live, page_b.clone());
    assert_eq!(first_b.payload["partial"], true);
    assert_eq!(
        first_a.payload["resume_cursor"],
        first_b.payload["resume_cursor"]
    );

    page_a["resume_cursor"] = first_a.payload["resume_cursor"].clone();
    seal(&mut page_a, key_a, operation_a);
    let second_a = invoke(&live, page_a.clone());
    assert_eq!(second_a.outcome, Outcome::Success, "{second_a:?}");
    assert_eq!(second_a.payload["partial"], true);

    page_b["resume_cursor"] = first_b.payload["resume_cursor"].clone();
    seal(&mut page_b, key_b, operation_b);
    let second_b = invoke(&live, page_b);
    assert_eq!(second_b.outcome, Outcome::Success, "{second_b:?}");
    assert_eq!(second_b.payload["partial"], true);
    assert_eq!(
        second_a.payload["resume_cursor"],
        second_b.payload["resume_cursor"]
    );

    page_a["resume_cursor"] = second_a.payload["resume_cursor"].clone();
    seal(&mut page_a, key_a, operation_a);
    let committed = invoke(&live, page_a);
    assert_eq!(committed.outcome, Outcome::Success, "{committed:?}");
    assert_eq!(committed.state_generation, generation + 1);
}

#[test]
fn maintenance_cursor_survives_restart_before_resume_and_commits_once() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    seed(&live, "alpha");
    seed(&live, "beta");
    let generation = seed(&live, "gamma").state_generation;
    let key = "restart-paged-maintenance";
    let operation_id = "01993262-4d00-7000-8000-000000000007";
    let mut page = request(key, "repair", generation);
    page["maximum_items"] = json!(1);
    seal(&mut page, key, operation_id);
    let first = invoke(&live, page.clone());
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    assert_eq!(first.payload["partial"], true);
    assert_eq!(first.state_generation, generation);
    let first_cursor = first.payload["resume_cursor"].clone();
    drop(live);

    let reopened = engine(&directory);
    page["resume_cursor"] = first_cursor;
    seal(&mut page, key, operation_id);
    let second = invoke(&reopened, page.clone());
    assert_eq!(second.outcome, Outcome::Success, "{second:?}");
    assert_eq!(second.payload["partial"], true);
    assert_eq!(second.state_generation, generation);
    assert_eq!(second.payload["scanned_items"], 1);

    page["resume_cursor"] = second.payload["resume_cursor"].clone();
    seal(&mut page, key, operation_id);
    let committed = invoke(&reopened, page.clone());
    assert_eq!(committed.outcome, Outcome::Success, "{committed:?}");
    assert_eq!(committed.payload["partial"], false);
    assert_eq!(committed.payload["scanned_items"], 1);
    assert_eq!(committed.state_generation, generation + 1);

    page["expected_generation"] = json!(committed.state_generation);
    seal(&mut page, key, operation_id);
    let replay = invoke(&reopened, page);
    assert_eq!(replay.outcome, Outcome::Success, "{replay:?}");
    assert_eq!(replay.payload["replayed"], true);
    assert_eq!(replay.state_generation, committed.state_generation);
}

#[test]
fn deadline_after_partial_page_can_resume_without_a_second_commit() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    seed(&live, "alpha");
    seed(&live, "beta");
    let generation = seed(&live, "gamma").state_generation;
    let key = "deadline-paged-maintenance";
    let operation_id = "01993262-4d00-0000-8000-000000000008";
    let mut page = request(key, "repair", generation);
    page["maximum_items"] = json!(2);
    seal(&mut page, key, operation_id);
    let first = invoke(&live, page.clone());
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    assert_eq!(first.payload["partial"], true);
    page["resume_cursor"] = first.payload["resume_cursor"].clone();

    seal(&mut page, key, operation_id);
    let interrupted = live.common_control(&namespace(), page.clone(), Deadline { remaining_ms: 0 });
    assert_eq!(interrupted.outcome, Outcome::Cancelled);
    assert_eq!(state(&live).state_generation, generation);

    let resumed = invoke(&live, page);
    assert_eq!(resumed.outcome, Outcome::Success, "{resumed:?}");
    assert_eq!(resumed.payload["partial"], false);
    assert_eq!(resumed.state_generation, generation + 1);
    assert_eq!(state(&live).state_generation, generation + 1);
}

#[test]
fn paged_consolidate_and_merge_prune_receipts_validate_global_effects() {
    for (task, key, operation_id) in [
        (
            "consolidate",
            "paged-consolidate-receipt",
            "01993262-4d00-0000-8000-000000000009",
        ),
        (
            "prune_expired",
            "paged-prune-receipt",
            "01993262-4d00-0000-8000-000000000010",
        ),
    ] {
        let directory = TempDir::new().unwrap();
        let live = engine(&directory);
        seed(&live, "alpha");
        seed(&live, "beta");
        let generation = seed(&live, "gamma").state_generation;
        let mut page = request(key, task, generation);
        page["maximum_items"] = json!(1);
        loop {
            seal(&mut page, key, operation_id);
            let reply = invoke(&live, page.clone());
            assert_eq!(reply.outcome, Outcome::Success, "{reply:?}");
            if reply.payload["partial"] == true {
                page["resume_cursor"] = reply.payload["resume_cursor"].clone();
                continue;
            }
            assert_eq!(reply.payload["scanned_items"], 1);
            assert!(reply.payload["_retained_receipt"].is_object());
            let receipt = receipt_item(&live, key);
            let outcome = &receipt["maintenance_receipt"]["outcome"];
            assert_eq!(outcome["scanned_items"], 1);
            assert!(
                outcome["changed_items"].as_u64().unwrap()
                    + outcome["removed_items"].as_u64().unwrap()
                    <= outcome["scanned_items"].as_u64().unwrap()
            );
            assert!(outcome["partial"] == false);
            let committed_generation = reply.state_generation;
            drop(live);
            let reopened = engine(&directory);
            page["expected_generation"] = json!(committed_generation);
            seal(&mut page, key, operation_id);
            let replay = invoke(&reopened, page);
            assert_eq!(replay.outcome, Outcome::Success, "{replay:?}");
            assert_eq!(replay.payload["replayed"], true);
            assert_eq!(replay.state_generation, committed_generation);
            assert!(snapshot::export(&reopened, &namespace(), DEADLINE).is_ok());
            break;
        }
    }
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
fn maintenance_event_key_mismatch_is_rejected_as_corrupt() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let generation = seed(&live, "event-key").state_generation;
    let original = request("event-key-mismatch", "repair", generation);
    let committed = invoke(&live, original);
    assert_eq!(committed.outcome, Outcome::Success, "{committed:?}");
    drop(live);

    let path = directory
        .path()
        .join("namespaces")
        .join(namespace())
        .join("ncm.sqlite");
    let connection = Connection::open(path).unwrap();
    let key = digest(b"event-key-mismatch");
    let receipt: String = connection
        .query_row(
            "SELECT receipt FROM events WHERE idempotency_key = ?1",
            [&key],
            |row| row.get(0),
        )
        .unwrap();
    let mut receipt: Value = serde_json::from_str(&receipt).unwrap();
    receipt["reply"]["payload"]["common_maintenance"]["event_basis"]["idempotency_key"] =
        json!("other-event-key");
    connection
        .execute(
            "UPDATE events SET receipt = ?1 WHERE idempotency_key = ?2",
            [serde_json::to_string(&receipt).unwrap(), key],
        )
        .unwrap();
    drop(connection);

    let reopened = engine(&directory);
    assert_eq!(
        inspect_receipt(&reopened, "event-key-mismatch").outcome,
        Outcome::Corrupt
    );
    assert_eq!(
        snapshot::export(&reopened, &namespace(), DEADLINE)
            .unwrap_err()
            .outcome,
        Outcome::Corrupt
    );
}

#[test]
fn common_maintenance_receipt_integrity_is_verified_on_all_replay_surfaces() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let generation = seed(&live, "integrity").state_generation;
    let original = request("integrity-receipt", "repair", generation);
    let first = invoke(&live, original.clone());
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    drop(live);

    let path = directory
        .path()
        .join("namespaces")
        .join(namespace())
        .join("ncm.sqlite");
    let connection = Connection::open(path).unwrap();
    let key = digest(b"integrity-receipt");
    let receipt: String = connection
        .query_row(
            "SELECT receipt FROM events WHERE idempotency_key = ?1",
            [&key],
            |row| row.get(0),
        )
        .unwrap();
    let mut receipt: Value = serde_json::from_str(&receipt).unwrap();
    receipt["integrity_digest"] = json!("0".repeat(64));
    connection
        .execute(
            "UPDATE events SET receipt = ?1 WHERE idempotency_key = ?2",
            [serde_json::to_string(&receipt).unwrap(), key],
        )
        .unwrap();
    drop(connection);

    let reopened = engine(&directory);
    assert_eq!(
        inspect_receipt(&reopened, "integrity-receipt").outcome,
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
fn common_maintenance_receipt_idempotency_key_is_bound_to_its_journal_event() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let generation = seed(&live, "receipt-key").state_generation;
    let original = request("receipt-key", "repair", generation);
    let first = invoke(&live, original.clone());
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    drop(live);

    let path = directory
        .path()
        .join("namespaces")
        .join(namespace())
        .join("ncm.sqlite");
    let connection = Connection::open(path).unwrap();
    let key = digest(b"receipt-key");
    let receipt: String = connection
        .query_row(
            "SELECT receipt FROM events WHERE idempotency_key = ?1",
            [&key],
            |row| row.get(0),
        )
        .unwrap();
    let mut receipt: Value = serde_json::from_str(&receipt).unwrap();
    assert_eq!(receipt["idempotency_key"], json!(key));
    receipt["idempotency_key"] = json!("different-receipt-key");
    connection
        .execute(
            "UPDATE events SET receipt = ?1 WHERE idempotency_key = ?2",
            [serde_json::to_string(&receipt).unwrap(), key],
        )
        .unwrap();
    drop(connection);

    let reopened = engine(&directory);
    assert_eq!(
        inspect_receipt(&reopened, "receipt-key").outcome,
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
