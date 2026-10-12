#![allow(
    clippy::expect_used,
    clippy::field_reassign_with_default,
    clippy::panic,
    clippy::unwrap_used
)]
#![doc = "Atomic common maintenance receipts through the durable NCM engine."]

use rusqlite::Connection;
use serde::Serialize;
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

fn opaque_id(namespace: &str, kind: &[u8], value: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"tracedecay.ncm.opaque-id.v1\0");
    for field in [namespace.as_bytes(), kind, value.as_bytes()] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn worker_key(public_key: &str) -> String {
    opaque_id(&namespace(), b"idempotency-key", public_key)
}

fn legacy_worker_key(public_key: &str) -> String {
    digest(public_key.as_bytes())
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
        "action": "maintenance", "idempotency_key": worker_key(key),
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
            "view": "maintenance_receipt", "delivery_key": worker_key(key),
            "maximum_items": 1, "maximum_bytes": 1_048_576, "after": 0,
        }),
        DEADLINE,
    )
}

fn receipt_item(engine: &NcmEngine, key: &str) -> Value {
    let reply = inspect_receipt(engine, key);
    assert_eq!(reply.outcome, Outcome::Success);
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

#[derive(Serialize)]
struct IntegrityReply<'a> {
    outcome: &'a Value,
    state_generation: &'a Value,
    payload: &'a Value,
}

#[derive(Serialize)]
struct IntegrityMaintenance<'a> {
    kind: &'a Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    canonical_input: Option<&'a Value>,
}

#[derive(Serialize)]
struct IntegrityOperation<'a> {
    maintenance: IntegrityMaintenance<'a>,
}

#[derive(Serialize)]
struct IntegrityBasis<'a> {
    reply: IntegrityReply<'a>,
    operation: IntegrityOperation<'a>,
    state_digest: &'a str,
}

fn receipt_integrity_digest(receipt: &Value) -> String {
    let reply = receipt["reply"].clone();
    let operation = receipt["operation"].clone();
    let state_digest = receipt["state_digest"].as_str().unwrap();
    digest(
        &serde_json::to_vec(&IntegrityBasis {
            reply: IntegrityReply {
                outcome: &reply["outcome"],
                state_generation: &reply["state_generation"],
                payload: &reply["payload"],
            },
            operation: IntegrityOperation {
                maintenance: IntegrityMaintenance {
                    kind: &operation["maintenance"]["kind"],
                    canonical_input: operation["maintenance"].get("canonical_input"),
                },
            },
            state_digest,
        })
        .unwrap(),
    )
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
    let mut page = request("paged-repair-1", "repair", generation);
    page["maximum_items"] = json!(1);
    for (page_number, (key, operation_id)) in [
        ("paged-repair-1", "01993262-4d00-7000-8000-000000000001"),
        ("paged-repair-2", "01993262-4d00-7000-8000-000000000002"),
    ]
    .into_iter()
    .enumerate()
    {
        page["idempotency_key"] = json!(worker_key(key));
        seal(&mut page, key, operation_id);
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
                namespace(),
                expected_after = page_number + 1,
            )
        );
        page["resume_cursor"] = partial.payload["resume_cursor"].clone();
    }
    page["idempotency_key"] = json!(worker_key("paged-repair-3"));
    seal(
        &mut page,
        "paged-repair-3",
        "01993262-4d00-7000-8000-000000000003",
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
    fresh["idempotency_key"] = json!(worker_key("fresh-stale-cursor"));
    seal(
        &mut fresh,
        "fresh-stale-cursor",
        "01993262-4d00-7000-8000-000000000004",
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

    let key_a_second = "same-position-maintenance-a-second";
    let operation_a_second = "01993262-4d00-0000-8000-000000000013";
    let mut page_a_second = request(key_a_second, "repair", generation);
    page_a_second["maximum_items"] = json!(1);
    page_a_second["resume_cursor"] = first_a.payload["resume_cursor"].clone();
    seal(&mut page_a_second, key_a_second, operation_a_second);
    let second_a = invoke(&live, page_a_second.clone());
    assert_eq!(second_a.outcome, Outcome::Success, "{second_a:?}");
    assert_eq!(second_a.payload["partial"], true);

    let key_b_second = "same-position-maintenance-b-second";
    let operation_b_second = "01993262-4d00-0000-8000-000000000014";
    let mut page_b_second = request(key_b_second, "repair", generation);
    page_b_second["maximum_items"] = json!(1);
    page_b_second["resume_cursor"] = first_b.payload["resume_cursor"].clone();
    seal(&mut page_b_second, key_b_second, operation_b_second);
    let second_b = invoke(&live, page_b_second);
    assert_eq!(second_b.outcome, Outcome::Success, "{second_b:?}");
    assert_eq!(second_b.payload["partial"], true);
    assert_eq!(
        second_a.payload["resume_cursor"],
        second_b.payload["resume_cursor"]
    );

    let final_key = "same-position-maintenance-a-final";
    let final_operation_id = "01993262-4d00-0000-8000-000000000019";
    let mut final_page = request(final_key, "repair", generation);
    final_page["maximum_items"] = json!(1);
    final_page["resume_cursor"] = second_a.payload["resume_cursor"].clone();
    seal(&mut final_page, final_key, final_operation_id);
    let committed = invoke(&live, final_page);
    assert_eq!(committed.outcome, Outcome::Success, "{committed:?}");
    assert_eq!(committed.state_generation, generation + 1);
}

#[test]
fn maintenance_continuation_accepts_a_fresh_identity_for_each_page() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    seed(&live, "alpha");
    seed(&live, "beta");
    let generation = seed(&live, "gamma").state_generation;

    let mut first = request("fresh-page-1", "repair", generation);
    first["maximum_items"] = json!(1);
    seal(
        &mut first,
        "fresh-page-1",
        "01993262-4d00-0000-8000-000000000013",
    );
    let first = invoke(&live, first);
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    assert_eq!(first.state_generation, generation);
    assert_eq!(first.payload["partial"], true);
    let first_cursor = first.payload["resume_cursor"].clone();

    let mut second = request("fresh-page-2", "repair", generation);
    second["maximum_items"] = json!(1);
    second["resume_cursor"] = first_cursor;
    seal(
        &mut second,
        "fresh-page-2",
        "01993262-4d00-0000-8000-000000000014",
    );
    let second = invoke(&live, second);
    assert_eq!(second.outcome, Outcome::Success, "{second:?}");
    assert_eq!(second.state_generation, generation);
    assert_eq!(second.payload["partial"], true);
    let second_cursor = second.payload["resume_cursor"].clone();

    let mut final_page = request("fresh-page-3", "repair", generation);
    final_page["maximum_items"] = json!(1);
    final_page["resume_cursor"] = second_cursor;
    seal(
        &mut final_page,
        "fresh-page-3",
        "01993262-4d00-0000-8000-000000000015",
    );
    let committed = invoke(&live, final_page.clone());
    assert_eq!(committed.outcome, Outcome::Success, "{committed:?}");
    assert_eq!(committed.payload["partial"], false);
    assert_eq!(committed.state_generation, generation + 1);

    final_page["expected_generation"] = json!(committed.state_generation);
    seal(
        &mut final_page,
        "fresh-page-3",
        "01993262-4d00-0000-8000-000000000015",
    );
    let replay = invoke(&live, final_page);
    assert_eq!(replay.outcome, Outcome::Success, "{replay:?}");
    assert_eq!(replay.payload["replayed"], true);
    assert_eq!(replay.state_generation, committed.state_generation);
}

#[test]
fn maintenance_cursor_rejects_same_key_on_a_changed_cursor_without_mutation() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    seed(&live, "alpha");
    seed(&live, "beta");
    let generation = seed(&live, "gamma").state_generation;

    let mut first = request("cursor-identity-a", "repair", generation);
    first["maximum_items"] = json!(1);
    seal(
        &mut first,
        "cursor-identity-a",
        "01993262-4d00-0000-8000-000000000020",
    );
    let first = invoke(&live, first);
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    assert_eq!(first.payload["partial"], true);
    let first_cursor = first.payload["resume_cursor"].clone();

    let mut second = request("cursor-identity-b", "repair", generation);
    second["maximum_items"] = json!(1);
    second["resume_cursor"] = first_cursor;
    seal(
        &mut second,
        "cursor-identity-b",
        "01993262-4d00-0000-8000-000000000021",
    );
    let second = invoke(&live, second);
    assert_eq!(second.outcome, Outcome::Success, "{second:?}");
    assert_eq!(second.payload["partial"], true);
    let second_cursor = second.payload["resume_cursor"].clone();

    let mut retry = request("cursor-identity-b", "repair", generation);
    retry["maximum_items"] = json!(1);
    retry["resume_cursor"] = first.payload["resume_cursor"].clone();
    seal(
        &mut retry,
        "cursor-identity-b",
        "01993262-4d00-0000-8000-000000000021",
    );
    let retry = invoke(&live, retry);
    assert_eq!(retry.outcome, Outcome::Success, "{retry:?}");
    assert_eq!(retry.payload["partial"], true);
    assert_eq!(retry.payload["resume_cursor"], second_cursor);

    let before_conflict = state(&live);
    let mut substituted = request("cursor-identity-b", "repair", generation);
    substituted["maximum_items"] = json!(1);
    substituted["resume_cursor"] = second_cursor;
    seal(
        &mut substituted,
        "cursor-identity-b",
        "01993262-4d00-0000-8000-000000000022",
    );
    let conflict = invoke(&live, substituted);
    assert_eq!(
        conflict.outcome,
        Outcome::Rejected(RejectReason::IdempotencyConflict),
        "{conflict:?}"
    );
    assert_eq!(conflict.state_generation, generation);

    let after_conflict = state(&live);
    assert_eq!(
        after_conflict.state_generation,
        before_conflict.state_generation
    );
    assert_eq!(
        after_conflict.payload["state_digest"],
        before_conflict.payload["state_digest"]
    );
}

#[test]
fn maintenance_admission_key_is_bound_to_the_opaque_request_key() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let generation = seed(&live, "admission-binding").state_generation;
    let mut substituted = request("admission-key-a", "repair", generation);
    seal(
        &mut substituted,
        "admission-key-b",
        "01993262-4d00-0000-8000-000000000016",
    );

    assert_eq!(
        invoke(&live, substituted).outcome,
        Outcome::Rejected(RejectReason::InvalidRequest(
            "maintenance admission does not match the request".to_owned()
        ))
    );
    assert_eq!(state(&live).state_generation, generation);
}

#[test]
fn maintenance_cursor_survives_restart_before_resume_and_commits_once() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    seed(&live, "alpha");
    seed(&live, "beta");
    let generation = seed(&live, "gamma").state_generation;
    let first_key = "restart-paged-maintenance-1";
    let second_key = "restart-paged-maintenance-2";
    let final_key = "restart-paged-maintenance-3";
    let mut page = request(first_key, "repair", generation);
    page["maximum_items"] = json!(1);
    seal(&mut page, first_key, "01993262-4d00-7000-8000-000000000007");
    let first = invoke(&live, page.clone());
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    assert_eq!(first.payload["partial"], true);
    assert_eq!(first.state_generation, generation);
    let first_cursor = first.payload["resume_cursor"].clone();
    drop(live);

    let reopened = engine(&directory);
    page["idempotency_key"] = json!(worker_key(second_key));
    page["resume_cursor"] = first_cursor;
    seal(
        &mut page,
        second_key,
        "01993262-4d00-0000-8000-000000000023",
    );
    let second = invoke(&reopened, page.clone());
    assert_eq!(second.outcome, Outcome::Success, "{second:?}");
    assert_eq!(second.payload["partial"], true);
    assert_eq!(second.state_generation, generation);
    assert_eq!(second.payload["scanned_items"], 1);

    page["idempotency_key"] = json!(worker_key(final_key));
    page["resume_cursor"] = second.payload["resume_cursor"].clone();
    seal(&mut page, final_key, "01993262-4d00-0000-8000-000000000024");
    let committed = invoke(&reopened, page.clone());
    assert_eq!(committed.outcome, Outcome::Success, "{committed:?}");
    assert_eq!(committed.payload["partial"], false);
    assert_eq!(committed.payload["scanned_items"], 1);
    assert_eq!(committed.state_generation, generation + 1);

    page["expected_generation"] = json!(committed.state_generation);
    seal(&mut page, final_key, "01993262-4d00-0000-8000-000000000024");
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
    let mut page = request("deadline-paged-maintenance", "repair", generation);
    page["maximum_items"] = json!(2);
    seal(
        &mut page,
        "deadline-paged-maintenance",
        "01993262-4d00-0000-8000-000000000008",
    );
    let first = invoke(&live, page.clone());
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    assert_eq!(first.payload["partial"], true);
    let first_cursor = first.payload["resume_cursor"].clone();

    // The cursor grant is durable before this continuation reaches the
    // deadline. A fresh public identity must still be able to resume it.
    let mut interrupted = request("deadline-paged-interrupted", "repair", generation);
    interrupted["maximum_items"] = json!(2);
    interrupted["resume_cursor"] = first_cursor.clone();
    seal(
        &mut interrupted,
        "deadline-paged-interrupted",
        "01993262-4d00-0000-8000-000000000017",
    );
    let interrupted = live.common_control(&namespace(), interrupted, Deadline { remaining_ms: 0 });
    assert_eq!(interrupted.outcome, Outcome::Cancelled);
    assert_eq!(state(&live).state_generation, generation);

    let mut resumed_request = request("deadline-paged-resumed", "repair", generation);
    resumed_request["maximum_items"] = json!(2);
    resumed_request["resume_cursor"] = first_cursor;
    seal(
        &mut resumed_request,
        "deadline-paged-resumed",
        "01993262-4d00-0000-8000-000000000018",
    );
    let resumed = invoke(&live, resumed_request);
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
        let mut page_number = 0_u64;
        loop {
            let page_key = if page_number == 0 {
                key.to_owned()
            } else {
                format!("{key}-page-{page_number}")
            };
            let page_operation_id = if page_number == 0 {
                operation_id.to_owned()
            } else {
                format!("{operation_id}-page-{page_number}")
            };
            page["idempotency_key"] = json!(worker_key(&page_key));
            seal(&mut page, &page_key, &page_operation_id);
            let reply = invoke(&live, page.clone());
            assert_eq!(reply.outcome, Outcome::Success, "{reply:?}");
            if reply.payload["partial"] == true {
                page["resume_cursor"] = reply.payload["resume_cursor"].clone();
                page_number += 1;
                continue;
            }
            assert_eq!(reply.payload["scanned_items"], 1);
            assert!(reply.payload["_retained_receipt"].is_object());
            let receipt = receipt_item(&live, &page_key);
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
            page["idempotency_key"] = json!(worker_key(&page_key));
            seal(&mut page, &page_key, &page_operation_id);
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
    let key = worker_key("corrupt-receipt");
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
    let key = worker_key("event-key-mismatch");
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
    let key = worker_key("integrity-receipt");
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
    let key = worker_key("receipt-key");
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
fn legacy_common_maintenance_receipt_survives_restart_replay_and_export() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let generation = seed(&live, "legacy-receipt").state_generation;
    let original = request("legacy-receipt", "repair", generation);
    let first = invoke(&live, original.clone());
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    drop(live);

    let path = directory
        .path()
        .join("namespaces")
        .join(namespace())
        .join("ncm.sqlite");
    let connection = Connection::open(path).unwrap();
    let key = worker_key("legacy-receipt");
    let legacy_key = legacy_worker_key("legacy-receipt");
    let receipt: String = connection
        .query_row(
            "SELECT receipt FROM events WHERE idempotency_key = ?1",
            [&key],
            |row| row.get(0),
        )
        .unwrap();
    let mut receipt: Value = serde_json::from_str(&receipt).unwrap();
    receipt["operation"]["maintenance"]
        .as_object_mut()
        .unwrap()
        .remove("canonical_input");
    receipt["reply"]["payload"]["common_maintenance"]["event_basis"]["idempotency_key"] =
        json!(legacy_key);
    // The immediately preceding HEAD receipt type had no outer
    // `idempotency_key` field. Keep this fixture's wire shape historical
    // instead of adding the field back with a legacy value.
    receipt.as_object_mut().unwrap().remove("idempotency_key");
    assert!(receipt.get("idempotency_key").is_none());
    receipt["integrity_digest"] = json!(receipt_integrity_digest(&receipt));
    connection
        .execute(
            "UPDATE events SET idempotency_key = ?1, receipt = ?2 WHERE idempotency_key = ?3",
            [
                legacy_key.clone(),
                serde_json::to_string(&receipt).unwrap(),
                key,
            ],
        )
        .unwrap();
    drop(connection);

    // Opening the engine exercises the recovery validator against the exact
    // pre-canonical-input envelope before any replay or export is attempted.
    let reopened = engine(&directory);
    let recovered = reopened.handshake(&namespace());
    assert_eq!(recovered.outcome, Outcome::Success, "{recovered:?}");
    assert_eq!(recovered.state_generation, first.state_generation);
    let before_retry = state(&reopened);
    assert_eq!(
        inspect_receipt(&reopened, "legacy-receipt").outcome,
        Outcome::Success
    );

    let mut retry = original.clone();
    retry["expected_generation"] = json!(before_retry.state_generation);
    let replay = invoke(&reopened, retry);
    assert_eq!(replay.outcome, Outcome::Success, "{replay:?}");
    assert_eq!(replay.payload["replayed"], true);
    assert_eq!(replay.state_generation, before_retry.state_generation);
    let mut legacy_retry = original;
    legacy_retry["idempotency_key"] = json!(legacy_key);
    legacy_retry["expected_generation"] = json!(before_retry.state_generation);
    let legacy_replay = invoke(&reopened, legacy_retry);
    assert_eq!(legacy_replay.outcome, Outcome::Success, "{legacy_replay:?}");
    assert_eq!(legacy_replay.payload["replayed"], true);
    assert_eq!(
        legacy_replay.state_generation,
        before_retry.state_generation
    );
    assert_eq!(
        state(&reopened).payload["state_digest"],
        before_retry.payload["state_digest"]
    );
    assert!(snapshot::export(&reopened, &namespace(), DEADLINE).is_ok());
}

#[test]
fn maintenance_event_key_substitution_is_rejected_without_mutation() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let generation = seed(&live, "event-substitution").state_generation;
    let original = request("event-substitution", "repair", generation);
    let first = invoke(&live, original.clone());
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    drop(live);

    let path = directory
        .path()
        .join("namespaces")
        .join(namespace())
        .join("ncm.sqlite");
    let connection = Connection::open(path).unwrap();
    let key = worker_key("event-substitution");
    let replacement = worker_key("event-substitution-other");
    let receipt: String = connection
        .query_row(
            "SELECT receipt FROM events WHERE idempotency_key = ?1",
            [&key],
            |row| row.get(0),
        )
        .unwrap();
    let mut receipt: Value = serde_json::from_str(&receipt).unwrap();
    receipt["idempotency_key"] = json!(replacement);
    receipt["reply"]["payload"]["common_maintenance"]["event_basis"]["idempotency_key"] =
        json!(replacement);
    receipt["integrity_digest"] = json!(receipt_integrity_digest(&receipt));
    connection
        .execute(
            "UPDATE events SET idempotency_key = ?1, receipt = ?2 WHERE idempotency_key = ?3",
            [
                replacement.clone(),
                serde_json::to_string(&receipt).unwrap(),
                key.clone(),
            ],
        )
        .unwrap();
    drop(connection);

    let reopened = engine(&directory);
    let mut retry = original;
    retry["expected_generation"] = json!(first.state_generation);
    let inspection = inspect_receipt(&reopened, "event-substitution");
    assert_eq!(inspection.outcome, Outcome::Corrupt);
    assert_eq!(inspection.state_generation, first.state_generation);
    assert_eq!(invoke(&reopened, retry).outcome, Outcome::Corrupt);
    let exported = snapshot::export(&reopened, &namespace(), DEADLINE).unwrap_err();
    assert_eq!(exported.outcome, Outcome::Corrupt);
    assert_eq!(exported.state_generation, first.state_generation);
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
