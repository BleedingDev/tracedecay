#![allow(
    clippy::expect_used,
    clippy::field_reassign_with_default,
    clippy::panic,
    clippy::unwrap_used
)]
#![doc = "Durable common replay and fresh snapshot privacy tests through the NCM engine."]

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tempfile::TempDir;
use tracedecay_memory_ncm_core::types::{NcmConfig, SourceId};
use tracedecay_memory_ncm_runtime::embedding::doubles::HashEncoder;
use tracedecay_memory_ncm_runtime::engine::{
    FaultPoint, NcmEngine, ObserveRequest, Outcome, RecallRequest,
};
use tracedecay_memory_ncm_runtime::ports::{Deadline, StateRoot};
use tracedecay_memory_ncm_runtime::snapshot;

const DEADLINE: Deadline = Deadline {
    remaining_ms: u64::MAX,
};

fn digest(text: &str) -> String {
    Sha256::digest(text.as_bytes())
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
        provenance: json!({"common_capsule":{"version":1,"bytes":[],"sha256":digest("")},"selection":{"observation_identity":digest(&format!("observation-{source}")),"revision_digest":digest("revision-1")}}),
        deadline: DEADLINE,
    };
    request.payload_sha256 = request.canonical_payload_sha256().unwrap();
    request
}

fn item(sequence: u64, source: &str, key: &str) -> Value {
    let request = observation(source, key);
    json!({"source_sequence":sequence,"receipt_digest":digest(&format!("receipt-{sequence}")),"delivery_key":request.idempotency_key,"source":request.source.0,"admitted":true,"blocked":false,
        "observation":{"idempotency_key":request.idempotency_key,"payload_sha256":request.payload_sha256,"source":request.source.0,"key_text":request.key_text,"value_text":request.value_text,"affect":request.affect,"surprise":request.surprise,"intensity":request.intensity,"provenance":request.provenance}})
}

fn page(key: &str, generation: u64, previous: u64, items: Vec<Value>) -> Value {
    json!({"action":"replay","idempotency_key":digest(key),"expected_generation":generation,"first_source_sequence":items.first().unwrap()["source_sequence"],"last_source_sequence":items.last().unwrap()["source_sequence"],"expected_previous_acknowledged_sequence":previous,"items":items})
}

fn inspect(engine: &NcmEngine) -> Value {
    let reply = engine.inspection(&namespace());
    assert_eq!(reply.outcome, Outcome::Success, "{reply:?}");
    reply.payload
}

#[test]
fn blocked_first_replay_item_persists_its_empty_namespace_fence_and_processes_successor() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let mut blocked = item(1, "blocked-first", "blocked-delivery");
    blocked["admitted"] = json!(false);
    blocked["blocked"] = json!(true);
    blocked["observation"] = Value::Null;
    let result = live.common_portability(
        &namespace(),
        page(
            "blocked-first-page",
            0,
            0,
            vec![blocked, item(2, "eligible-second", "eligible-delivery")],
        ),
        DEADLINE,
    );
    assert_eq!(result.outcome, Outcome::Success, "{result:?}");
    assert_eq!(result.payload["rejected_observations"], 1);
    assert_eq!(result.payload["applied_observations"], 1);
    assert_eq!(result.payload["effect_unknown_observations"], 0);
    assert_eq!(result.payload["acknowledged_sequence"], 2);
    assert_eq!(result.payload["items"][0]["state"], "rejected");
    assert_eq!(result.payload["items"][1]["state"], "applied");
    assert!(result.state_generation > 0);
    assert_eq!(inspect(&live)["records"], 1);
    drop(live);
    let reopened = engine(&directory);
    assert!(
        reopened
            .revoked_sources(&namespace())
            .unwrap()
            .contains(&SourceId(digest("blocked-first")))
    );
    let rejected = reopened.observe(
        &namespace(),
        observation("blocked-first", "fresh-blocked-delivery"),
    );
    assert!(
        matches!(rejected.outcome, Outcome::Rejected(_)),
        "{rejected:?}"
    );
    assert_eq!(inspect(&reopened)["records"], 1);
}

#[test]
fn replay_distinguishes_delivery_duplicates_and_already_applied_sources_after_restart() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let first = live.common_portability(
        &namespace(),
        page("first", 0, 0, vec![item(1, "alpha", "delivery-a")]),
        DEADLINE,
    );
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    assert_eq!(first.payload["applied_observations"], 1);
    assert_eq!(inspect(&live)["records"], 1);
    let learned = inspect(&live)["state_digest"].clone();
    let duplicate = live.common_portability(
        &namespace(),
        page(
            "duplicate-page",
            first.state_generation,
            1,
            vec![item(1, "alpha", "delivery-a")],
        ),
        DEADLINE,
    );
    assert_eq!(duplicate.outcome, Outcome::Success, "{duplicate:?}");
    assert_eq!(duplicate.payload["duplicate_observations"], 1);
    assert_eq!(duplicate.payload["applied_observations"], 0);
    let already = live.common_portability(
        &namespace(),
        page(
            "already-page",
            duplicate.state_generation,
            1,
            vec![item(1, "alpha", "delivery-b")],
        ),
        DEADLINE,
    );
    assert_eq!(already.outcome, Outcome::Success, "{already:?}");
    assert_eq!(already.payload["sources_already_applied"], 1);
    assert_eq!(already.payload["duplicate_observations"], 0);
    assert_eq!(inspect(&live)["state_digest"], learned);
    drop(live);
    let reopened = engine(&directory);
    let repeated = reopened.common_portability(
        &namespace(),
        page(
            "after-restart",
            already.state_generation,
            1,
            vec![item(1, "alpha", "delivery-c")],
        ),
        DEADLINE,
    );
    assert_eq!(repeated.outcome, Outcome::Success, "{repeated:?}");
    assert_eq!(repeated.payload["sources_already_applied"], 1);
    assert_eq!(inspect(&reopened)["records"], 1);
    assert_eq!(inspect(&reopened)["state_digest"], learned);
    let recalled = reopened.recall(
        &namespace(),
        RecallRequest {
            query_text: "alpha knowledge".to_owned(),
            top_k: 8,
            deadline: DEADLINE,
        },
    );
    assert_eq!(recalled.outcome, Outcome::Success, "{recalled:?}");
    assert!(
        recalled
            .payload
            .to_string()
            .contains("alpha retained answer")
    );
}

#[test]
fn fresh_restore_scrubs_currently_blocked_sources_and_preserves_allocation_floor() {
    let source_dir = TempDir::new().unwrap();
    let destination_dir = TempDir::new().unwrap();
    let source = engine(&source_dir);
    let observed = source.observe(&namespace(), observation("erased-secret", "source"));
    assert_eq!(observed.outcome, Outcome::Success);
    let snapshot = snapshot::export(&source, &namespace(), DEADLINE)
        .unwrap()
        .into_vec();
    let snapshot_digest: String = Sha256::digest(&snapshot)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let id = format!("ncm-snapshot:{snapshot_digest}");
    let destination = engine(&destination_dir);
    let restored=destination.common_portability(&namespace(),json!({"action":"snapshot_restore","idempotency_key":digest("restore"),"expected_generation":0,"bytes":snapshot,"blocked_sources":[digest("erased-secret")],"snapshot_id":id,"observation_sequence":1}),DEADLINE);
    assert_eq!(restored.outcome, Outcome::Success, "{restored:?}");
    assert_eq!(inspect(&destination)["records"], 0);
    let refused = destination.observe(&namespace(), observation("erased-secret", "fresh-delivery"));
    assert!(
        matches!(refused.outcome, Outcome::Rejected(_)),
        "{refused:?}"
    );
    let safe_bytes = snapshot::export(&destination, &namespace(), DEADLINE)
        .unwrap()
        .into_vec();
    assert!(
        !safe_bytes
            .windows("erased-secret".len())
            .any(|part| part == b"erased-secret")
    );
    drop(destination);
    let reopened = engine(&destination_dir);
    let fresh = reopened.observe(&namespace(), observation("new-source", "after-restart"));
    assert_eq!(fresh.outcome, Outcome::Success, "{fresh:?}");
    assert!(
        fresh.payload["record_id"].as_u64().unwrap()
            > observed.payload["record_id"].as_u64().unwrap()
    );
    let denied_again = reopened.observe(&namespace(), observation("erased-secret", "another-key"));
    assert!(matches!(denied_again.outcome, Outcome::Rejected(_)));
}

#[test]
fn unknown_item_and_undispatched_tail_are_partitioned_and_reconcile_without_relearning() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    live.inject_fault_once(FaultPoint::AfterCommitBeforePublish)
        .unwrap();
    let failed = live.common_portability(
        &namespace(),
        page(
            "interrupted",
            0,
            0,
            vec![item(1, "alpha", "a"), item(2, "bravo", "b")],
        ),
        DEADLINE,
    );
    assert_eq!(failed.outcome, Outcome::EffectUnknown, "{failed:?}");
    assert_eq!(failed.payload["effect_unknown_observations"], 1);
    assert_eq!(failed.payload["rejected_observations"], 1);
    assert_eq!(failed.payload["items"][0]["state"], "effect_unknown");
    assert_eq!(failed.payload["items"][1]["reason"], "not_dispatched");
    assert_eq!(failed.payload["partial"], true);
    drop(live);
    let reopened = engine(&directory);
    let ready = reopened.handshake(&namespace());
    assert_eq!(ready.outcome, Outcome::Success, "{ready:?}");
    let recovered = reopened.common_portability(
        &namespace(),
        page(
            "reconcile",
            ready.state_generation,
            1,
            vec![item(1, "alpha", "a"), item(2, "bravo", "b")],
        ),
        DEADLINE,
    );
    assert_eq!(recovered.outcome, Outcome::Success, "{recovered:?}");
    assert_eq!(recovered.payload["duplicate_observations"], 1);
    assert_eq!(recovered.payload["applied_observations"], 1);
    assert_eq!(recovered.payload["effect_unknown_observations"], 0);
    assert_eq!(inspect(&reopened)["records"], 2);
}

#[test]
fn malformed_restore_leaves_fresh_destination_unmaterialized() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let rejected=live.common_portability(&namespace(),json!({"action":"snapshot_restore","idempotency_key":digest("invalid"),"expected_generation":0,"bytes":[123],"blocked_sources":[digest("source")],"snapshot_id":format!("ncm-snapshot:{}",digest("invalid")),"observation_sequence":1}),DEADLINE);
    assert!(
        matches!(rejected.outcome, Outcome::Rejected(_)),
        "{rejected:?}"
    );
    assert!(
        !directory
            .path()
            .join("namespaces")
            .join(namespace())
            .exists()
    );
}

fn delivery_capsule(operation: &str, key: &str) -> Value {
    let bytes =
        serde_json::to_vec(&json!({"operation_id": operation, "idempotency_key": key})).unwrap();
    json!({"version": 1, "sha256": digest(std::str::from_utf8(&bytes).unwrap()), "bytes": bytes})
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

fn receipt_inspection(
    live: &NcmEngine,
    generation: u64,
    key: &str,
    stable: Option<&str>,
    after: u64,
    maximum: u64,
) -> Value {
    let mut request = json!({"action": "inspection", "view": "delivery_receipt", "expected_generation": generation,
        "delivery_key": digest(key), "maximum_items": maximum, "maximum_bytes": 1_048_576, "after": after});
    if let Some(stable) = stable {
        request["stable_memory_ref"] = json!(stable);
    }
    let reply = live.common_control(&namespace(), request, DEADLINE);
    assert_eq!(reply.outcome, Outcome::Success, "{reply:?}");
    reply.payload
}

fn worker_receipt(
    operation: &str,
    reply: &tracedecay_memory_ncm_runtime::engine::EngineReply,
) -> String {
    let mut basis = reply.payload.clone();
    let object = basis.as_object_mut().unwrap();
    object.remove("replayed");
    object.remove("common_observation");
    let bytes = serde_json::to_vec(&basis).unwrap();
    let mut digest = Sha256::new();
    digest.update(b"tracedecay.ncm.rust-worker-receipt.v1\0");
    digest.update((operation.len() as u64).to_be_bytes());
    digest.update(operation.as_bytes());
    digest.update(reply.state_generation.to_be_bytes());
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[test]
fn public_fresh_pages_resolve_existing_sources_without_new_receipts_or_acknowledgements() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let original = public_page(
        "first-public",
        "original-operation",
        0,
        0,
        vec![item(1, "alpha", "original-internal")],
    );
    let applied = live.common_portability(&namespace(), original.clone(), DEADLINE);
    assert_eq!(applied.outcome, Outcome::Success, "{applied:?}");
    let original_receipt = worker_receipt("replay", &applied);
    let generation = applied.state_generation;
    let before = inspect(&live);
    for (key, internal_key) in [
        ("fresh-public-same-item", "original-internal"),
        ("fresh-public-new-item", "different-internal"),
    ] {
        let resolved = live.common_portability(
            &namespace(),
            public_page(
                key,
                "fresh-operation",
                generation,
                1,
                vec![item(1, "alpha", internal_key)],
            ),
            DEADLINE,
        );
        assert_eq!(resolved.outcome, Outcome::Success, "{resolved:?}");
        assert_eq!(resolved.state_generation, generation);
        assert_eq!(resolved.payload["no_change"], true);
        assert_eq!(resolved.payload["sources_already_applied"], 1);
        assert_eq!(resolved.payload["duplicate_observations"], 0);
        assert_eq!(resolved.payload["applied_observations"], 0);
        assert_eq!(resolved.payload["acknowledged_sequence"], 1);
        assert_eq!(resolved.payload["state_generation_before"], generation);
        assert_eq!(resolved.payload["state_generation_after"], generation);
        assert_eq!(inspect(&live), before);
        let receipt = receipt_inspection(&live, generation, key, None, 0, 16);
        assert!(receipt["items"].as_array().unwrap().is_empty());
    }
    drop(live);
    let reopened = engine(&directory);
    let mut retry = original;
    retry["expected_generation"] = json!(generation);
    retry["page_delivery_capsule"] = delivery_capsule("retry-operation", "first-public");
    let duplicate = reopened.common_portability(&namespace(), retry, DEADLINE);
    assert_eq!(duplicate.outcome, Outcome::Success, "{duplicate:?}");
    assert_eq!(duplicate.payload["replayed"], true);
    assert_eq!(duplicate.state_generation, generation);
    assert_eq!(worker_receipt("replay", &duplicate), original_receipt);
    assert_eq!(
        duplicate.payload["page_delivery_capsule"],
        delivery_capsule("original-operation", "first-public")
    );
    let recovered = inspect(&reopened);
    assert_eq!(recovered["commit_seq"], before["commit_seq"]);
    assert_eq!(recovered["state_digest"], before["state_digest"]);
    assert_eq!(
        recovered["quota_usage"]["event_bytes"],
        before["quota_usage"]["event_bytes"]
    );
}

#[test]
fn already_fenced_public_replay_after_restore_preserves_generation_and_cursor() {
    for fresh in [false, true] {
        let source_dir = TempDir::new().unwrap();
        let source = engine(&source_dir);
        let observed = source.observe(
            &namespace(),
            observation("erased-source", "first-observation"),
        );
        assert_eq!(observed.outcome, Outcome::Success, "{observed:?}");
        let exported = snapshot::export(&source, &namespace(), DEADLINE)
            .unwrap()
            .into_vec();
        let deleted = source.delete_by_source(
            &namespace(),
            &SourceId(digest("erased-source")),
            &digest("first-deletion"),
            DEADLINE,
        );
        assert_eq!(deleted.outcome, Outcome::Success, "{deleted:?}");
        let destination_dir = TempDir::new().unwrap();
        let (live, state_dir, expected) = if fresh {
            drop(source);
            (engine(&destination_dir), &destination_dir, 0)
        } else {
            (source, &source_dir, deleted.state_generation)
        };
        let snapshot_hash: String = Sha256::digest(&exported)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let restored = live.common_portability(&namespace(), json!({"action":"snapshot_restore",
            "idempotency_key":digest("restored"),"expected_generation":expected,"bytes":exported,
            "blocked_sources":[digest("erased-source")],"snapshot_id":format!("ncm-snapshot:{snapshot_hash}"),"observation_sequence":1}), DEADLINE);
        assert_eq!(restored.outcome, Outcome::Success, "{restored:?}");
        let generation = restored.state_generation;
        let before = inspect(&live);
        let mut blocked = item(1, "erased-source", "fresh-blocked-internal");
        blocked["admitted"] = json!(false);
        blocked["blocked"] = json!(true);
        blocked["observation"] = Value::Null;
        let request = public_page(
            "no-change-fenced",
            "no-change-operation",
            generation,
            0,
            vec![blocked.clone()],
        );
        let rejected = live.common_portability(&namespace(), request.clone(), DEADLINE);
        assert_eq!(rejected.outcome, Outcome::Success, "{rejected:?}");
        assert_eq!(rejected.state_generation, generation);
        assert_eq!(rejected.payload["no_change"], true);
        assert_eq!(rejected.payload["rejected_observations"], 1);
        assert_eq!(rejected.payload["applied_observations"], 0);
        assert_eq!(rejected.payload["acknowledged_sequence"], 0);
        assert_eq!(inspect(&live), before);
        let receipt = receipt_inspection(&live, generation, "no-change-fenced", None, 0, 16);
        assert!(receipt["items"].as_array().unwrap().is_empty());
        let mut stale = request.clone();
        stale["expected_generation"] = json!(generation + 1);
        assert!(matches!(
            live.common_portability(&namespace(), stale, DEADLINE)
                .outcome,
            Outcome::Rejected(_)
        ));
        let mut stale_cursor = request.clone();
        stale_cursor["expected_previous_acknowledged_sequence"] = json!(1);
        assert!(matches!(
            live.common_portability(&namespace(), stale_cursor, DEADLINE)
                .outcome,
            Outcome::Rejected(_)
        ));
        // A no-effect page cannot create the missing acknowledgement for seq 2.
        let successor = public_page(
            "gapped-successor",
            "gapped-operation",
            generation,
            0,
            vec![item(2, "positive-source", "positive-internal")],
        );
        assert!(matches!(
            live.common_portability(&namespace(), successor, DEADLINE)
                .outcome,
            Outcome::Rejected(_)
        ));
        let mut blocked_gap = blocked.clone();
        blocked_gap["source_sequence"] = json!(2);
        assert!(matches!(
            live.common_portability(
                &namespace(),
                public_page(
                    "blocked-gap",
                    "blocked-gap-operation",
                    generation,
                    0,
                    vec![blocked_gap]
                ),
                DEADLINE
            )
            .outcome,
            Outcome::Rejected(_)
        ));
        assert_eq!(inspect(&live), before);
        drop(live);
        let reopened = engine(state_dir);
        let rejected_again = reopened.common_portability(&namespace(), request, DEADLINE);
        assert_eq!(
            rejected_again.outcome,
            Outcome::Success,
            "{rejected_again:?}"
        );
        assert_eq!(rejected_again.state_generation, generation);
        assert_eq!(rejected_again.payload["acknowledged_sequence"], 0);
        let recovered = inspect(&reopened);
        assert_eq!(recovered["commit_seq"], before["commit_seq"]);
        assert_eq!(recovered["state_digest"], before["state_digest"]);
        assert_eq!(
            recovered["quota_usage"]["event_bytes"],
            before["quota_usage"]["event_bytes"]
        );
        let forbidden = reopened.observe(
            &namespace(),
            observation("erased-source", "another-observation"),
        );
        assert!(matches!(forbidden.outcome, Outcome::Rejected(_)));
        // A contiguous successor page still executes its actual positive effect.
        let contiguous = reopened.common_portability(
            &namespace(),
            public_page(
                "contiguous-successor",
                "contiguous-operation",
                generation,
                0,
                vec![blocked, item(2, "positive-source", "positive-internal")],
            ),
            DEADLINE,
        );
        assert_eq!(contiguous.outcome, Outcome::Success, "{contiguous:?}");
        assert_eq!(contiguous.payload["rejected_observations"], 1);
        assert_eq!(contiguous.payload["applied_observations"], 1);
        assert_eq!(contiguous.payload["acknowledged_sequence"], 2);
        assert!(contiguous.state_generation > generation);
        assert_ne!(contiguous.payload["no_change"], true);
    }
}

#[test]
fn public_replay_page_receipt_keeps_original_identity_hash_and_actual_stable_refs_after_restart() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let request = public_page(
        "public-page",
        "first-public-operation",
        0,
        0,
        vec![
            item(1, "page-alpha", "internal-alpha"),
            item(2, "page-bravo", "internal-bravo"),
        ],
    );
    let first = live.common_portability(&namespace(), request.clone(), DEADLINE);
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    let expected_receipt = worker_receipt("replay", &first);
    assert_ne!(expected_receipt, worker_receipt("observe", &first));
    let influence = live.common_control(
        &namespace(),
        json!({"action": "inspection", "view": "source_influence",
        "expected_generation": first.state_generation, "source": digest("page-alpha"),
        "maximum_items": 16, "maximum_bytes": 1_048_576, "after": 0}),
        DEADLINE,
    );
    assert_eq!(influence.outcome, Outcome::Success, "{influence:?}");
    let stable = influence.payload["items"][0]["stable_memory_ref"]
        .as_str()
        .unwrap();
    let selected = receipt_inspection(
        &live,
        first.state_generation,
        "public-page",
        Some(stable),
        0,
        16,
    );
    assert_eq!(selected["items"].as_array().unwrap().len(), 1);
    assert_eq!(selected["items"][0]["stable_memory_ref"], stable);
    assert_eq!(
        selected["items"][0]["provider_receipt_digest"],
        expected_receipt
    );
    assert_eq!(
        selected["items"][0]["delivery_capsule"],
        request["page_delivery_capsule"]
    );
    let bounded = receipt_inspection(&live, first.state_generation, "public-page", None, 0, 1);
    assert_eq!(bounded["items"].as_array().unwrap().len(), 1);
    assert_eq!(bounded["partial"], true);
    let next = receipt_inspection(
        &live,
        first.state_generation,
        "public-page",
        None,
        bounded["cursor_after"].as_u64().unwrap(),
        1,
    );
    assert_eq!(next["items"].as_array().unwrap().len(), 1);
    assert_ne!(
        bounded["items"][0]["record_id"],
        next["items"][0]["record_id"]
    );
    let internal = receipt_inspection(&live, first.state_generation, "internal-alpha", None, 0, 16);
    assert!(internal["items"].as_array().unwrap().is_empty());
    assert_eq!(internal["partial"], true);
    let mut retry = request.clone();
    retry["expected_generation"] = json!(first.state_generation);
    retry["page_delivery_capsule"] = delivery_capsule("retry-public-operation", "public-page");
    let duplicate = live.common_portability(&namespace(), retry.clone(), DEADLINE);
    assert_eq!(duplicate.outcome, Outcome::Success, "{duplicate:?}");
    assert_eq!(duplicate.payload["replayed"], true);
    assert_eq!(
        duplicate.payload["page_delivery_capsule"],
        request["page_delivery_capsule"]
    );
    assert_eq!(worker_receipt("replay", &duplicate), expected_receipt);
    drop(live);
    let reopened = engine(&directory);
    let duplicate = reopened.common_portability(&namespace(), retry, DEADLINE);
    assert_eq!(duplicate.outcome, Outcome::Success, "{duplicate:?}");
    assert_eq!(
        duplicate.payload["page_delivery_capsule"],
        request["page_delivery_capsule"]
    );
    assert_eq!(worker_receipt("replay", &duplicate), expected_receipt);
    let retained = receipt_inspection(
        &reopened,
        first.state_generation,
        "public-page",
        Some(stable),
        0,
        16,
    );
    assert_eq!(retained["items"], selected["items"]);
}

#[test]
fn legacy_replay_page_without_original_identity_has_no_public_delivery_receipt() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let reply = live.common_portability(
        &namespace(),
        page(
            "legacy-page",
            0,
            0,
            vec![item(1, "legacy-source", "legacy-internal")],
        ),
        DEADLINE,
    );
    assert_eq!(reply.outcome, Outcome::Success, "{reply:?}");
    let receipt = receipt_inspection(&live, reply.state_generation, "legacy-page", None, 0, 16);
    assert!(receipt["items"].as_array().unwrap().is_empty());
    assert_eq!(receipt["partial"], true);
}

#[test]
fn malformed_public_page_identity_is_rejected_before_any_replay_effect() {
    for capsule in [
        json!({"version": 1, "bytes": [], "sha256": digest("wrong")}),
        json!({"version": 1, "bytes": [256], "sha256": digest("")}),
        json!({"version": 1, "bytes": [], "sha256": digest(""), "extra": true}),
    ] {
        let directory = TempDir::new().unwrap();
        let live = engine(&directory);
        let mut request = page(
            "bad-page",
            0,
            0,
            vec![item(1, "untouched", "untouched-delivery")],
        );
        request["page_delivery_capsule"] = capsule;
        let rejected = live.common_portability(&namespace(), request, DEADLINE);
        assert!(
            matches!(rejected.outcome, Outcome::Rejected(_)),
            "{rejected:?}"
        );
        assert_eq!(rejected.state_generation, 0);
        let untouched = live.inspection(&namespace());
        assert_eq!(untouched.outcome, Outcome::Empty);
        assert_eq!(untouched.state_generation, 0);
    }
}

#[test]
fn ordinary_observe_receipt_uses_its_original_operation_domain_and_delivery_capsule() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let mut request = observation("ordinary-source", "ordinary-key");
    request.provenance["delivery_capsule"] = delivery_capsule("ordinary-operation", "ordinary-key");
    request.payload_sha256 = request.canonical_payload_sha256().unwrap();
    let observed = live.observe(&namespace(), request.clone());
    assert_eq!(observed.outcome, Outcome::Success, "{observed:?}");
    let receipt = receipt_inspection(
        &live,
        observed.state_generation,
        "ordinary-key",
        None,
        0,
        16,
    );
    assert_eq!(receipt["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        receipt["items"][0]["provider_receipt_digest"],
        worker_receipt("observe", &observed)
    );
    assert_eq!(
        receipt["items"][0]["delivery_capsule"],
        request.provenance["delivery_capsule"]
    );
    let nonexistent = format!("ncm-memory:{}", "00".repeat(32));
    let missing = receipt_inspection(
        &live,
        observed.state_generation,
        "ordinary-key",
        Some(&nonexistent),
        0,
        16,
    );
    assert!(missing["items"].as_array().unwrap().is_empty());
    assert_eq!(missing["partial"], true);
}
