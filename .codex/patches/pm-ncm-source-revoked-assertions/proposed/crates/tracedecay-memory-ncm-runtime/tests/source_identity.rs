#![allow(
    clippy::expect_used,
    clippy::field_reassign_with_default,
    clippy::panic,
    clippy::unwrap_used
)]
#![doc = "Public engine regressions for common source identity and retained raw source compatibility."]

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
use tempfile::TempDir;
use tracedecay_memory_ncm_core::types::{NcmConfig, SourceId};
use tracedecay_memory_ncm_runtime::embedding::doubles::HashEncoder;
use tracedecay_memory_ncm_runtime::engine::{
    EngineReply, NcmEngine, ObserveRequest, Outcome, RecallRequest, RejectReason,
};
use tracedecay_memory_ncm_runtime::ports::{
    Deadline, Embedding, EncoderError, EncoderIdentity, StateRoot, TextEncoder,
};
use tracedecay_memory_ncm_runtime::snapshot::{self, RestoreRequest};

const DEADLINE: Deadline = Deadline {
    remaining_ms: u64::MAX,
};

#[derive(Clone, Copy)]
struct CommonSource<'a> {
    provider: &'a str,
    session: &'a str,
    key: &'a str,
}

const A: CommonSource<'_> = CommonSource {
    provider: "claude",
    session: "original-session-a",
    key: "shared-raw-source-key",
};
const B: CommonSource<'_> = CommonSource {
    provider: "claude",
    session: "original-session-b",
    key: "shared-raw-source-key",
};
const OTHER: CommonSource<'_> = CommonSource {
    provider: "claude",
    session: "original-session-other",
    key: "unrelated-source-key",
};

fn namespace() -> String {
    "d7".repeat(32)
}

fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

// Fixture construction follows the public projection protocol. Assertions below
// observe real engine state, durable receipts, fences, and exported capsules.
fn opaque(kind: &[u8], value: &str) -> String {
    let namespace = namespace();
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

fn scope(session: &str) -> Value {
    json!({
        "profile_id": "profile", "project_id": "project",
        "repository_identity": "repository", "worktree_identity": "worktree",
        "branch_identity": "master", "agent_session_id": session,
        "resolved_scope_digest": format!("sha256:{}", "ab".repeat(32))
    })
}

fn encoded(value: &Value) -> Value {
    let bytes = serde_json::to_vec(value).unwrap();
    json!({"version": 1, "sha256": digest(&bytes), "bytes": bytes})
}

impl CommonSource<'_> {
    fn full_id(self) -> SourceId {
        let identity = serde_json::to_string(&json!([
            "profile",
            "project",
            self.provider,
            self.session,
            self.key
        ]))
        .unwrap();
        SourceId(opaque(b"common-source-key-v1", &identity))
    }

    fn legacy_id(self) -> SourceId {
        SourceId(opaque(b"forget-source-key", self.key))
    }

    fn binding(self) -> Value {
        json!({"version": 1, "source_id": self.full_id().0, "legacy_source_id": self.legacy_id().0})
    }

    fn observation(self, observation: &str, content: &str, typed: bool) -> ObserveRequest {
        self.message(observation, content, "assistant", typed)
    }

    fn message(self, observation: &str, content: &str, role: &str, typed: bool) -> ObserveRequest {
        let canonical = json!({"role": role, "content": content});
        let revision = format!("revision-{observation}");
        let original = json!({
            "source": {
                "canonical_provider_id": self.provider, "canonical_session_id": self.session,
                "source_key": self.key, "stable_record_id": null, "observation_id": observation,
                "source_revision": revision,
                "content_sha256": digest(&serde_json::to_vec(&canonical).unwrap())
            },
            "origin_scope": {"state": "recorded", "exact_scope_identity": scope(self.session),
                "authority_ref": "source.authority"},
            "source_sequence": 1,
            "occurred_at": "2026-01-01T00:00:00Z", "ingested_at": "2026-01-01T00:00:00Z",
            "validity": {"valid_from": "2026-01-01T00:00:00Z", "valid_until": null,
                "superseded_at": null, "superseded_by": null, "revoked_at": null}
        });
        let source_ref = format!("record:{observation}");
        let value_text = format!("{role}: {content}");
        let retained = json!({
            "original_source": original, "canonical_payload": canonical,
            "source_refs": [source_ref], "delivery_scope": scope("delivery-session"),
            "projection": {"key_text": content, "value_text": value_text,
                "observation_kind": "session.message_committed.v1"}
        });
        let idempotency_key =
            digest(format!("{}:{}:{observation}", self.provider, self.session).as_bytes());
        let mut provenance = json!({
            "common_capsule": encoded(&retained),
            "delivery_capsule": encoded(&json!({"operation_id": format!("operation-{observation}"),
                "idempotency_key": idempotency_key})),
            "selection": {
                "valid_from": 1_767_225_600_000_000_000_i64, "valid_until": null,
                "superseded_at": null, "superseded_by": null, "revoked_at": null,
                "source_refs": [opaque(b"source_refs", &source_ref)],
                "observation_ids": [opaque(b"observation_ids", observation)],
                "unknown_revision": false,
                "source_identity_sha256": opaque(b"source-target", &serde_json::to_string(&json!({
                    "source": original["source"], "origin_scope": original["origin_scope"]
                })).unwrap()),
                "revision_digest": opaque(b"source-revision", &revision),
                "observation_identity": opaque(b"observation-identity", &serde_json::to_string(&json!([
                    self.provider, self.session, observation
                ])).unwrap())
            }
        });
        if typed {
            provenance["source_binding"] = self.binding();
        }
        let mut request = ObserveRequest {
            idempotency_key,
            payload_sha256: String::new(),
            source: if typed {
                self.full_id()
            } else {
                self.legacy_id()
            },
            key_text: content.to_owned(),
            value_text,
            affect: None,
            surprise: 0.4,
            intensity: 1.0,
            provenance,
            deadline: DEADLINE,
        };
        request.payload_sha256 = request.canonical_payload_sha256().unwrap();
        request
    }
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

fn with_encoder(directory: &TempDir, encoder: Arc<dyn TextEncoder>) -> NcmEngine {
    NcmEngine::new(StateRoot::new(directory.path()).unwrap(), encoder, config())
}

fn engine(directory: &TempDir) -> NcmEngine {
    with_encoder(directory, Arc::new(HashEncoder::new()))
}

fn inspect(engine: &NcmEngine) -> EngineReply {
    let reply = engine.inspection(&namespace());
    assert_eq!(reply.outcome, Outcome::Success, "{reply:?}");
    reply
}

fn assert_same_durable_inspection(mut actual: EngineReply, expected: &EngineReply) {
    let mut expected = expected.clone();
    // Reopening can checkpoint the WAL into the database without changing any
    // durable state. Exclude only those two physical file measurements.
    for reply in [&mut actual, &mut expected] {
        let usage = reply.payload["quota_usage"].as_object_mut().unwrap();
        for key in ["db_bytes", "wal_bytes"] {
            assert!(
                usage
                    .remove(key)
                    .is_some_and(|bytes| bytes.as_u64().is_some()),
                "inspection must retain the numeric {key} measurement"
            );
        }
    }
    assert_eq!(actual, expected);
}

fn observe(engine: &NcmEngine, request: ObserveRequest) -> EngineReply {
    let reply = engine.observe(&namespace(), request);
    assert_eq!(reply.outcome, Outcome::Success, "{reply:?}");
    reply
}

fn export(engine: &NcmEngine) -> Vec<u8> {
    snapshot::export(engine, &namespace(), DEADLINE)
        .unwrap()
        .into_vec()
}

fn record_ids(engine: &NcmEngine) -> Vec<u64> {
    let snapshot: Value = serde_json::from_slice(&export(engine)).unwrap();
    snapshot["capsules"]
        .as_array()
        .unwrap()
        .iter()
        .map(|capsule| capsule["record_id"].as_u64().unwrap())
        .collect()
}

fn delete(engine: &NcmEngine, source: CommonSource<'_>, key: &str, targeted: bool) -> EngineReply {
    let mut request = json!({
        "action": "delete_by_source", "idempotency_key": digest(key.as_bytes()),
        "sources": [if targeted { source.full_id().0 } else { source.legacy_id().0 }],
        "expected_generation": inspect(engine).state_generation,
        "verification_query_digest": digest(b"source deletion verification")
    });
    if targeted {
        request["source_bindings"] = json!([{
            "source_id": source.full_id().0, "legacy_source_id": source.legacy_id().0
        }]);
    }
    engine.common_control(&namespace(), request, DEADLINE)
}

fn replay_item(sequence: u64, request: ObserveRequest) -> Value {
    let legacy_source = request
        .provenance
        .get("source_binding")
        .map(|binding| binding["legacy_source_id"].clone());
    let mut item = json!({
        "source_sequence": sequence, "receipt_digest": digest(format!("source-receipt-{sequence}").as_bytes()),
        "delivery_key": request.idempotency_key, "source": request.source.0,
        "admitted": true, "blocked": false,
        "observation": {
            "idempotency_key": request.idempotency_key, "payload_sha256": request.payload_sha256,
            "source": request.source.0, "key_text": request.key_text, "value_text": request.value_text,
            "affect": request.affect, "surprise": request.surprise, "intensity": request.intensity,
            "provenance": request.provenance
        }
    });
    if let Some(legacy_source) = legacy_source {
        item["legacy_source"] = legacy_source;
    }
    item
}

fn replay_page(key: &str, generation: u64, previous: u64, items: Vec<Value>) -> Value {
    json!({
        "action": "replay", "idempotency_key": digest(key.as_bytes()),
        "expected_generation": generation, "expected_previous_acknowledged_sequence": previous,
        "first_source_sequence": items.first().unwrap()["source_sequence"],
        "last_source_sequence": items.last().unwrap()["source_sequence"], "items": items
    })
}

#[test]
fn targeted_delete_groups_one_original_source_and_preserves_colliding_sources_after_restart() {
    for other in [
        B,
        CommonSource {
            provider: "codex",
            ..A
        },
    ] {
        let directory = TempDir::new().unwrap();
        let live = engine(&directory);
        observe(&live, A.observation("a-first", "A first revision", true));
        observe(&live, A.observation("a-second", "A second revision", true));
        let b = observe(
            &live,
            other.observation("b-first", "B retained answer", true),
        );

        let deleted = delete(&live, A, "target-delete-a", true);
        assert_eq!(deleted.outcome, Outcome::Success, "{deleted:?}");
        assert_eq!(deleted.payload["postcondition"]["matched_effects"], 2);
        assert_eq!(
            record_ids(&live),
            vec![b.payload["record_id"].as_u64().unwrap()]
        );
        assert_eq!(
            live.revoked_sources(&namespace()).unwrap(),
            vec![A.full_id()]
        );
        drop(live);

        let reopened = engine(&directory);
        let before = inspect(&reopened);
        let refused = reopened.observe(&namespace(), A.observation("a-future", "A future", true));
        assert_eq!(
            refused.outcome,
            Outcome::Rejected(RejectReason::SourceRevoked),
            "{refused:?}"
        );
        assert_eq!(inspect(&reopened), before);
        observe(&reopened, other.observation("b-future", "B future", true));

        let page = reopened.common_portability(
            &namespace(),
            replay_page(
                "future-page",
                inspect(&reopened).state_generation,
                0,
                vec![
                    replay_item(
                        1,
                        A.observation("a-replayed", "A replay must stay erased", true),
                    ),
                    replay_item(
                        2,
                        other.observation("b-replayed", "B replay remains admissible", true),
                    ),
                ],
            ),
            DEADLINE,
        );
        assert_eq!(page.outcome, Outcome::Success, "{page:?}");
        assert_eq!(page.payload["rejected_observations"], 1);
        assert_eq!(page.payload["applied_observations"], 1);
        assert_eq!(page.payload["items"][0]["state"], "rejected");
        assert_eq!(page.payload["items"][1]["state"], "applied");
        assert_eq!(inspect(&reopened).payload["records"], 3);
        assert_eq!(
            reopened.revoked_sources(&namespace()).unwrap(),
            vec![A.full_id()]
        );
    }
}

#[derive(Default)]
struct CountingEncoder {
    calls: AtomicUsize,
}

impl TextEncoder for CountingEncoder {
    fn identity(&self) -> EncoderIdentity {
        HashEncoder::new().identity()
    }

    fn encode(&self, texts: &[&str], deadline: Deadline) -> Result<Vec<Embedding>, EncoderError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        HashEncoder::new().encode(texts, deadline)
    }
}

#[test]
fn broad_raw_delete_fences_both_typed_sources_and_old_raw_fences_stop_encoding() {
    for legacy_capsules in [false, true] {
        let directory = TempDir::new().unwrap();
        let live = engine(&directory);
        observe(
            &live,
            A.observation("a-before", "A before broad deletion", !legacy_capsules),
        );
        observe(
            &live,
            B.observation("b-before", "B before broad deletion", !legacy_capsules),
        );
        let before_delete = export(&live);
        let deleted = delete(&live, A, "raw-delete", false);
        assert_eq!(deleted.outcome, Outcome::Success, "{deleted:?}");
        assert_eq!(deleted.payload["postcondition"]["matched_effects"], 2);
        assert_eq!(inspect(&live).payload["records"], 0);
        assert_eq!(
            live.revoked_sources(&namespace()).unwrap(),
            vec![A.legacy_id()]
        );
        drop(live);

        let encoder = Arc::new(CountingEncoder::default());
        let reopened = with_encoder(&directory, encoder.clone());
        let before = inspect(&reopened);
        for source in [A, B] {
            let refused = reopened.observe(
                &namespace(),
                source.observation("future", "future input", true),
            );
            assert_eq!(
                refused.outcome,
                Outcome::Rejected(RejectReason::SourceRevoked),
                "{refused:?}"
            );
        }
        assert_eq!(
            encoder.calls.load(Ordering::Relaxed),
            0,
            "durable raw fences must reject before encoding"
        );
        assert_eq!(inspect(&reopened), before);

        // Old raw snapshots and newly bound snapshots both obey the same broad
        // raw fence; neither may restore either source's learned influence.
        let restored = snapshot::restore(
            &reopened,
            &namespace(),
            RestoreRequest {
                idempotency_key: "restore-before-raw-delete".to_owned(),
                bytes: before_delete,
            },
            DEADLINE,
        );
        assert_eq!(restored.outcome, Outcome::Success, "{restored:?}");
        assert_eq!(inspect(&reopened).payload["records"], 0);
        assert!(record_ids(&reopened).is_empty());
        assert_eq!(
            reopened.revoked_sources(&namespace()).unwrap(),
            vec![A.legacy_id()]
        );
        assert_eq!(encoder.calls.load(Ordering::Relaxed), 0);
    }
}

struct PausingEncoder {
    armed: AtomicBool,
    calls: AtomicUsize,
    entered: mpsc::Sender<()>,
    resume: Mutex<mpsc::Receiver<()>>,
}

impl TextEncoder for PausingEncoder {
    fn identity(&self) -> EncoderIdentity {
        HashEncoder::new().identity()
    }

    fn encode(&self, texts: &[&str], deadline: Deadline) -> Result<Vec<Embedding>, EncoderError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.armed.swap(false, Ordering::SeqCst) {
            self.entered.send(()).unwrap();
            self.resume
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(30))
                .unwrap();
        }
        HashEncoder::new().encode(texts, deadline)
    }
}

#[test]
fn raw_delete_while_typed_observation_is_encoding_wins_at_insert_transaction() {
    let directory = TempDir::new().unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let encoder = Arc::new(PausingEncoder {
        armed: AtomicBool::new(false),
        calls: AtomicUsize::new(0),
        entered: entered_tx,
        resume: Mutex::new(resume_rx),
    });
    let live = Arc::new(with_encoder(&directory, encoder.clone()));
    observe(
        &live,
        OTHER.observation("baseline", "unrelated baseline", true),
    );
    let racing = A.observation("racing", "must never publish", true);
    encoder.armed.store(true, Ordering::SeqCst);
    let writer_engine = Arc::clone(&live);
    let writer_request = racing.clone();
    let writer = std::thread::spawn(move || writer_engine.observe(&namespace(), writer_request));
    entered_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    let deleted = delete(&live, A, "delete-during-encode", false);
    let after_delete = inspect(&live);
    resume_tx.send(()).unwrap();
    let refused = writer.join().unwrap();

    assert_eq!(deleted.outcome, Outcome::Success, "{deleted:?}");
    assert_eq!(deleted.payload["postcondition"]["matched_effects"], 0);
    assert_eq!(
        refused.outcome,
        Outcome::Rejected(RejectReason::SourceRevoked),
        "{refused:?}"
    );
    assert_eq!(inspect(&live), after_delete);
    assert_eq!(encoder.calls.load(Ordering::Relaxed), 2);
    let retry = live.observe(&namespace(), racing);
    assert_eq!(retry, refused);
    assert_eq!(
        encoder.calls.load(Ordering::Relaxed),
        2,
        "retry is rejected before a second encoding"
    );
    let next = observe(
        &live,
        OTHER.observation("next", "next unrelated input", true),
    );
    assert_eq!(
        next.payload["record_id"], 2,
        "failed insert must not allocate a record"
    );
    drop(live);
    let reopened = engine(&directory);
    assert_eq!(
        reopened.revoked_sources(&namespace()).unwrap(),
        vec![A.legacy_id()]
    );
    assert_eq!(inspect(&reopened).payload["records"], 2);
}

#[test]
fn targeted_delete_with_retained_legacy_alias_rejects_without_receipt_fence_or_state_change() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    observe(&live, A.observation("old-a", "retained legacy A", false));
    observe(&live, B.observation("new-b", "retained typed B", true));
    let before = inspect(&live);
    let before_bytes = export(&live);
    let rejected = delete(&live, A, "same-delete-key", true);
    assert!(
        matches!(
            rejected.outcome,
            Outcome::Rejected(RejectReason::InvalidRequest(_))
        ),
        "{rejected:?}"
    );
    assert_eq!(rejected.state_generation, before.state_generation);
    assert_eq!(inspect(&live), before);
    assert_eq!(export(&live), before_bytes);
    assert!(live.revoked_sources(&namespace()).unwrap().is_empty());
    drop(live);

    let reopened = engine(&directory);
    assert_same_durable_inspection(inspect(&reopened), &before);
    assert_eq!(delete(&reopened, A, "same-delete-key", true), rejected);
    assert!(reopened.revoked_sources(&namespace()).unwrap().is_empty());
    // Reusing the attempted key for a different valid effect proves the
    // fail-closed attempt did not reserve an idempotency receipt.
    let broad = delete(&reopened, A, "same-delete-key", false);
    assert_eq!(broad.outcome, Outcome::Success, "{broad:?}");
    assert_eq!(broad.payload["postcondition"]["matched_effects"], 2);
}

#[test]
fn old_snapshot_of_full_revoked_original_rejects_but_colliding_legacy_original_restores() {
    let old_a_directory = TempDir::new().unwrap();
    let old_b_directory = TempDir::new().unwrap();
    let destination_directory = TempDir::new().unwrap();
    let old_a = engine(&old_a_directory);
    let old_b = engine(&old_b_directory);
    observe(&old_a, A.observation("old-a", "legacy snapshot A", false));
    observe(&old_b, B.observation("old-b", "legacy snapshot B", false));
    let a_bytes = export(&old_a);
    let b_bytes = export(&old_b);
    let old: Value = serde_json::from_slice(&a_bytes).unwrap();
    assert_eq!(old["capsules"][0]["source_id"], A.legacy_id().0);
    let live = engine(&destination_directory);
    observe(&live, A.observation("current-a", "current A", true));
    let deleted = delete(&live, A, "typed-delete-before-old-restore", true);
    assert_eq!(deleted.outcome, Outcome::Success, "{deleted:?}");
    drop(live);

    let reopened = engine(&destination_directory);
    let before = inspect(&reopened);
    let before_bytes = export(&reopened);
    let rejected = snapshot::restore(
        &reopened,
        &namespace(),
        RestoreRequest {
            idempotency_key: "old-restore-key".to_owned(),
            bytes: a_bytes,
        },
        DEADLINE,
    );
    assert!(
        matches!(
            rejected.outcome,
            Outcome::Rejected(RejectReason::InvalidRequest(_))
        ),
        "{rejected:?}"
    );
    assert_eq!(rejected.state_generation, before.state_generation);
    assert_eq!(inspect(&reopened), before);
    assert_eq!(export(&reopened), before_bytes);
    assert_eq!(
        reopened.revoked_sources(&namespace()).unwrap(),
        vec![A.full_id()]
    );

    // The identical raw alias is safe when its retained original tuple is B.
    // Reusing the key also proves the rejection did not retain a restore receipt.
    let restored = snapshot::restore(
        &reopened,
        &namespace(),
        RestoreRequest {
            idempotency_key: "old-restore-key".to_owned(),
            bytes: b_bytes,
        },
        DEADLINE,
    );
    assert_eq!(restored.outcome, Outcome::Success, "{restored:?}");
    assert_eq!(inspect(&reopened).payload["records"], 1);
    let exported: Value = serde_json::from_slice(&export(&reopened)).unwrap();
    assert_eq!(
        exported["capsules"][0]["source_id"],
        B.legacy_id().0,
        "restore must retain old source IDs"
    );
    assert_eq!(
        exported["capsules"][0]["value_text"],
        "assistant: legacy snapshot B"
    );
    let provenance: Value =
        serde_json::from_str(exported["capsules"][0]["provenance"].as_str().unwrap()).unwrap();
    assert!(provenance.get("source_binding").is_none());
    drop(reopened);
    let again = engine(&destination_directory);
    assert_eq!(inspect(&again).payload["records"], 1);
    assert_eq!(
        again.revoked_sources(&namespace()).unwrap(),
        vec![A.full_id()]
    );
}

#[test]
fn source_influence_filters_raw_alias_before_item_and_byte_bounds_for_old_and_new_capsules() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    for (observation, typed) in [("unrelated-old", false), ("unrelated-new", true)] {
        observe(
            &live,
            OTHER.observation(observation, &"unrelated prefix ".repeat(300), typed),
        );
    }
    let first = observe(&live, A.observation("selected-a", "selected A", true));
    let second = observe(&live, B.observation("selected-b", "selected B", true));
    let third = observe(
        &live,
        B.observation("selected-legacy", "selected legacy", false),
    );
    let snapshot: Value = serde_json::from_slice(&export(&live)).unwrap();
    let maximum_bytes = snapshot["capsules"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|capsule| {
            capsule["record_id"].as_u64().unwrap() >= first.payload["record_id"].as_u64().unwrap()
        })
        .map(|capsule| {
            ["provenance", "key_text", "value_text"]
                .iter()
                .map(|field| capsule[*field].as_str().unwrap().len())
                .sum::<usize>()
        })
        .max()
        .unwrap();
    let expected = [first, second, third].map(|reply| reply.payload["record_id"].as_u64().unwrap());
    let mut after = 0;
    for (index, id) in expected.into_iter().enumerate() {
        let reply = live.common_control(
            &namespace(),
            json!({
                "action": "inspection", "view": "source_influence",
                "expected_generation": inspect(&live).state_generation, "source": A.legacy_id().0,
                "maximum_items": 1, "maximum_bytes": maximum_bytes, "after": after,
            }),
            DEADLINE,
        );
        assert_eq!(reply.outcome, Outcome::Success, "{reply:?}");
        assert_eq!(
            reply.payload["items"].as_array().unwrap().len(),
            1,
            "unrelated records must consume neither bound"
        );
        assert_eq!(reply.payload["items"][0]["record_id"], id);
        assert_eq!(reply.payload["scanned_items"], 1);
        assert_eq!(reply.payload["partial"], index < 2);
        if index < 2 {
            assert_eq!(reply.payload["cursor_after"], id);
        }
        after = id;
    }
}

fn selection(excluded_key: Option<&str>, maximum_candidates: usize) -> Value {
    json!({
        "mode": "current", "evaluation": 1_767_225_600_000_000_001_i64,
        "as_of": null, "start": null, "end": null,
        "include_superseded": false, "include_revoked": false, "unknown_policy": "exclude",
        "request_token": digest(b"selected recall request"), "maximum_candidates": maximum_candidates,
        "exclusions": {
            "stable_memory_refs": [], "candidate_ids": [],
            "source_refs": excluded_key.map(|key| vec![opaque(b"source_refs", key)]).unwrap_or_default(),
            "trace_refs": [], "observation_ids": [], "content_sha256": []
        }
    })
}

#[test]
fn raw_source_key_exclusion_runs_before_candidate_and_recall_byte_limits_for_old_and_new() {
    for typed in [false, true] {
        for byte_bound in [false, true] {
            let directory = TempDir::new().unwrap();
            let mut config = config();
            let query = "shared source retrieval evidence";
            if byte_bound {
                // Both canonical messages have identical key embeddings. The
                // assistant prefix makes the excluded record five bytes larger
                // than the eligible user message, which exactly fits the limit.
                config.max_recall_bytes = query.len() * 2 + "user: ".len();
            }
            let live = NcmEngine::new(
                StateRoot::new(directory.path()).unwrap(),
                Arc::new(HashEncoder::new()),
                config,
            );
            let excluded_reply = observe(&live, A.observation("excluded-first", query, typed));
            let safe = observe(
                &live,
                OTHER.message("eligible-second", query, "user", typed),
            );
            let recall = || RecallRequest {
                query_text: query.to_owned(),
                top_k: 16,
                deadline: DEADLINE,
            };
            let baseline = live.recall_selected(&namespace(), recall(), selection(None, 1));
            if byte_bound {
                assert!(
                    baseline.payload["common_recall"]["candidates"]
                        .as_array()
                        .unwrap()
                        .is_empty(),
                    "{baseline:?}"
                );
                assert_eq!(baseline.payload["common_recall"]["truncated"], true);
            } else {
                assert_eq!(baseline.outcome, Outcome::Success, "{baseline:?}");
                assert_eq!(
                    baseline.payload["common_recall"]["candidates"][0]["record_id"],
                    excluded_reply.payload["record_id"]
                );
            }
            let before = inspect(&live);
            let selected = live.recall_selected(&namespace(), recall(), selection(Some(A.key), 1));
            assert_eq!(selected.outcome, Outcome::Success, "{selected:?}");
            let rows = selected.payload["common_recall"]["candidates"]
                .as_array()
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0]["record_id"], safe.payload["record_id"]);
            assert_eq!(selected.payload["common_recall"]["excluded_items"], 1);
            assert_eq!(
                inspect(&live),
                before,
                "recall exclusions must remain request-local"
            );
        }
    }
}

#[test]
fn aliased_control_target_requires_original_binding_while_old_raw_target_remains_valid() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let mut raw = A.observation("unattributed-raw", "retained raw content", false);
    // Older raw clients may retain a valid opaque capsule without attribution.
    raw.provenance["common_capsule"] = encoded(&json!({}));
    raw.payload_sha256 = raw.canonical_payload_sha256().unwrap();
    observe(&live, raw);
    let before = inspect(&live);
    let before_bytes = export(&live);
    let influence = live.common_control(
        &namespace(),
        json!({
            "action": "inspection", "view": "source_influence",
            "expected_generation": before.state_generation, "source": A.legacy_id().0,
            "maximum_items": 1, "maximum_bytes": 131_072, "after": 0
        }),
        DEADLINE,
    );
    assert_eq!(influence.outcome, Outcome::Success, "{influence:?}");
    assert_eq!(influence.payload["items"].as_array().unwrap().len(), 1);
    let row = &influence.payload["items"][0];
    let mut control = json!({
        "action": "feedback", "idempotency_key": digest(b"aliased-unattributed-control"),
        "expected_generation": before.state_generation, "signal": "ignored", "weight": 0.0,
        "target_digest": digest(b"unattributed-target"),
        "target": {
            "stable_memory_ref": row["stable_memory_ref"], "source": A.full_id().0,
            "legacy_source": A.legacy_id().0,
            "source_identity_sha256": row["provenance"]["selection"]["source_identity_sha256"]
        }
    });
    let rejected = live.common_control(&namespace(), control.clone(), DEADLINE);
    assert_eq!(
        rejected.outcome,
        Outcome::Rejected(RejectReason::InvalidRequest(
            "source target lacks verified original binding".to_owned()
        ))
    );
    assert_eq!(rejected.state_generation, before.state_generation);
    assert_eq!(inspect(&live), before);
    assert_eq!(export(&live), before_bytes);

    control["idempotency_key"] = json!(digest(b"old-raw-control"));
    control["target"]["source"] = json!(A.legacy_id().0);
    control["target"]
        .as_object_mut()
        .unwrap()
        .remove("legacy_source");
    let accepted = live.common_control(&namespace(), control, DEADLINE);
    assert_eq!(accepted.outcome, Outcome::Success, "{accepted:?}");
    assert_eq!(accepted.state_generation, before.state_generation + 1);
}

#[test]
fn upgraded_observe_retry_returns_old_receipt_and_record_while_changed_content_conflicts() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let legacy = A.observation("same-delivery", "original retained content", false);
    let first = observe(&live, legacy.clone());
    let before = inspect(&live);
    let before_bytes = export(&live);
    drop(live);

    let encoder = Arc::new(CountingEncoder::default());
    let reopened = with_encoder(&directory, encoder.clone());
    let upgraded = A.observation("same-delivery", "original retained content", true);
    assert_ne!(upgraded.payload_sha256, legacy.payload_sha256);
    assert_eq!(
        upgraded.provenance["common_capsule"],
        legacy.provenance["common_capsule"]
    );
    assert_eq!(
        upgraded.provenance["selection"],
        legacy.provenance["selection"]
    );
    let repeated = reopened.observe(&namespace(), upgraded);
    let mut expected = first.clone();
    expected.payload["replayed"] = json!(true);
    assert_eq!(
        repeated, expected,
        "retry must return the retained receipt and raw record identity"
    );
    assert_same_durable_inspection(inspect(&reopened), &before);
    assert_eq!(export(&reopened), before_bytes);
    assert_eq!(encoder.calls.load(Ordering::Relaxed), 0);

    let changed = reopened.observe(
        &namespace(),
        A.observation("same-delivery", "changed retained content", true),
    );
    assert_eq!(
        changed.outcome,
        Outcome::Rejected(RejectReason::IdempotencyConflict)
    );
    assert_same_durable_inspection(inspect(&reopened), &before);
    assert_eq!(encoder.calls.load(Ordering::Relaxed), 0);

    // A new delivery of the old observation through replay must recognize the
    // same retained source without attempting to relearn it under the full ID.
    let mut replayed_observation =
        A.observation("same-delivery", "original retained content", true);
    replayed_observation.idempotency_key = digest(b"new-replay-delivery");
    let replayed = reopened.common_portability(
        &namespace(),
        replay_page(
            "upgraded-replay",
            before.state_generation,
            0,
            vec![replay_item(1, replayed_observation)],
        ),
        DEADLINE,
    );
    assert_eq!(replayed.outcome, Outcome::Success, "{replayed:?}");
    assert_eq!(replayed.payload["sources_already_applied"], 1);
    assert_eq!(replayed.payload["applied_observations"], 0);
    assert_eq!(
        replayed.payload["items"][0]["record_id"],
        first.payload["record_id"]
    );
    assert_eq!(
        inspect(&reopened).payload["state_digest"],
        before.payload["state_digest"]
    );
    assert_eq!(
        record_ids(&reopened),
        vec![first.payload["record_id"].as_u64().unwrap()]
    );
    assert_eq!(encoder.calls.load(Ordering::Relaxed), 0);
}

#[test]
fn upgraded_replay_page_retry_returns_old_page_and_item_receipts_without_relearning() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let legacy_item = replay_item(
        1,
        A.observation("legacy-replay-delivery", "retained replay evidence", false),
    );
    assert!(legacy_item.get("legacy_source").is_none());
    let mut legacy_page = replay_page("legacy-replay-page", 0, 0, vec![legacy_item]);
    legacy_page["page_delivery_capsule"] = encoded(&json!({
        "operation_id": "original-page-operation", "idempotency_key": "public-page-key"
    }));
    let first = live.common_portability(&namespace(), legacy_page.clone(), DEADLINE);
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    assert_eq!(first.payload["applied_observations"], 1);
    assert_eq!(first.payload["items"][0]["record_id"], 1);
    let before = inspect(&live);
    let before_bytes = export(&live);
    drop(live);

    let encoder = Arc::new(CountingEncoder::default());
    let reopened = with_encoder(&directory, encoder.clone());
    let mut upgraded_page = legacy_page.clone();
    upgraded_page["items"] = json!([replay_item(
        1,
        A.observation("legacy-replay-delivery", "retained replay evidence", true),
    )]);
    upgraded_page["page_delivery_capsule"] = encoded(&json!({
        "operation_id": "retry-page-operation", "idempotency_key": "public-page-key"
    }));
    assert_eq!(upgraded_page["expected_generation"], 0);
    assert_eq!(upgraded_page["items"][0]["legacy_source"], A.legacy_id().0);
    let repeated = reopened.common_portability(&namespace(), upgraded_page.clone(), DEADLINE);
    let mut expected = first.clone();
    expected.payload["replayed"] = json!(true);
    assert_eq!(
        repeated, expected,
        "retry must return the original page, item receipts, and delivery identity"
    );
    assert_same_durable_inspection(inspect(&reopened), &before);
    assert_eq!(export(&reopened), before_bytes);
    assert_eq!(record_ids(&reopened), vec![1]);
    assert_eq!(encoder.calls.load(Ordering::Relaxed), 0);

    upgraded_page["items"] = json!([replay_item(
        1,
        A.observation("legacy-replay-delivery", "changed replay evidence", true),
    )]);
    let changed = reopened.common_portability(&namespace(), upgraded_page, DEADLINE);
    assert_eq!(
        changed.outcome,
        Outcome::Rejected(RejectReason::IdempotencyConflict)
    );
    assert_same_durable_inspection(inspect(&reopened), &before);
    assert_eq!(export(&reopened), before_bytes);
    assert_eq!(encoder.calls.load(Ordering::Relaxed), 0);
}

#[test]
fn public_blocked_replay_reuses_a_verified_legacy_fence_without_writes_after_restart() {
    for legacy_capsule in [false, true] {
        let directory = TempDir::new().unwrap();
        let live = engine(&directory);
        observe(
            &live,
            A.observation(
                "before-legacy-fence",
                "retained before deletion",
                !legacy_capsule,
            ),
        );
        let deleted = delete(&live, A, "legacy-fence", false);
        assert_eq!(deleted.outcome, Outcome::Success, "{deleted:?}");
        assert!(record_ids(&live).is_empty());
        assert_eq!(
            live.revoked_sources(&namespace()).unwrap(),
            vec![A.legacy_id()]
        );
        let before = inspect(&live);
        let before_bytes = export(&live);
        drop(live);

        let encoder = Arc::new(CountingEncoder::default());
        let reopened = with_encoder(&directory, encoder.clone());
        assert_same_durable_inspection(inspect(&reopened), &before);
        // A real canonical request with verified retained original attribution
        // is already rejected by this legacy-only fence before any encoding.
        let future = A.observation("future-bound-observation", "must remain erased", true);
        assert_eq!(future.source, A.full_id());
        assert_eq!(future.provenance["source_binding"], A.binding());
        let refused = reopened.observe(&namespace(), future);
        assert_eq!(
            refused.outcome,
            Outcome::Rejected(RejectReason::SourceRevoked)
        );
        assert_same_durable_inspection(inspect(&reopened), &before);
        assert_eq!(encoder.calls.load(Ordering::Relaxed), 0);

        let mut blocked = replay_item(
            1,
            A.observation("blocked-public-delivery", "not admitted", true),
        );
        blocked["admitted"] = json!(false);
        blocked["blocked"] = json!(true);
        blocked["observation"] = Value::Null;
        assert_eq!(blocked["source"], A.full_id().0);
        assert_eq!(blocked["legacy_source"], A.legacy_id().0);
        let mut request = replay_page(
            "no-change-legacy-fence",
            before.state_generation,
            0,
            vec![blocked],
        );
        request["page_delivery_capsule"] = encoded(&json!({
            "operation_id": "no-change-legacy-operation", "idempotency_key": "no-change-legacy-fence"
        }));
        let live_before = inspect(&reopened);
        let rejected = reopened.common_portability(&namespace(), request.clone(), DEADLINE);
        assert_eq!(rejected.outcome, Outcome::Success, "{rejected:?}");
        assert_eq!(rejected.state_generation, before.state_generation);
        assert_eq!(rejected.payload["no_change"], true);
        assert_eq!(rejected.payload["rejected_observations"], 1);
        assert_eq!(rejected.payload["applied_observations"], 0);
        assert_eq!(rejected.payload["duplicate_observations"], 0);
        assert_eq!(rejected.payload["acknowledged_sequence"], 0);
        assert_eq!(inspect(&reopened), live_before);
        assert_same_durable_inspection(inspect(&reopened), &before);
        assert_eq!(export(&reopened), before_bytes);
        assert_eq!(
            reopened.revoked_sources(&namespace()).unwrap(),
            vec![A.legacy_id()]
        );
        assert_eq!(encoder.calls.load(Ordering::Relaxed), 0);
        drop(reopened);

        let recovered = engine(&directory);
        let again = recovered.common_portability(&namespace(), request, DEADLINE);
        assert_eq!(again, rejected);
        assert_same_durable_inspection(inspect(&recovered), &before);
        assert_eq!(export(&recovered), before_bytes);
        assert_eq!(
            recovered.revoked_sources(&namespace()).unwrap(),
            vec![A.legacy_id()]
        );
    }
}

#[test]
fn blocked_replay_uses_guarded_full_source_deletion_and_refuses_unverifiable_old_page_upgrade() {
    for legacy_capsule in [true, false] {
        let directory = TempDir::new().unwrap();
        let encoder = Arc::new(CountingEncoder::default());
        let live = with_encoder(&directory, encoder.clone());
        observe(
            &live,
            A.observation("retained-a", "retained A before block", !legacy_capsule),
        );
        let b = observe(
            &live,
            B.observation("retained-b", "retained B before block", true),
        );
        let before = inspect(&live);
        let before_bytes = export(&live);
        let mut blocked = replay_item(
            1,
            A.observation("fresh-blocked-delivery", "not admitted", true),
        );
        blocked["admitted"] = json!(false);
        blocked["blocked"] = json!(true);
        blocked["observation"] = Value::Null;
        assert_eq!(blocked["source"], A.full_id().0);
        assert_eq!(blocked["legacy_source"], A.legacy_id().0);
        let page = replay_page(
            "fresh-blocked-page",
            before.state_generation,
            0,
            vec![blocked],
        );
        let reply = live.common_portability(&namespace(), page.clone(), DEADLINE);

        if legacy_capsule {
            assert!(
                matches!(
                    reply.outcome,
                    Outcome::Rejected(RejectReason::InvalidRequest(_))
                ),
                "{reply:?}"
            );
            assert_eq!(reply.state_generation, before.state_generation);
            assert_eq!(reply.payload["items"][0]["reason"], "source_fence_failed");
            assert_eq!(inspect(&live), before);
            assert_eq!(export(&live), before_bytes);
            assert!(live.revoked_sources(&namespace()).unwrap().is_empty());
            drop(live);
            let reopened = engine(&directory);
            let retried = reopened.common_portability(&namespace(), page, DEADLINE);
            assert_eq!(retried, reply);
            assert_same_durable_inspection(inspect(&reopened), &before);
            assert_eq!(export(&reopened), before_bytes);
            assert!(reopened.revoked_sources(&namespace()).unwrap().is_empty());
        } else {
            assert_eq!(reply.outcome, Outcome::Success, "{reply:?}");
            assert_eq!(reply.payload["rejected_observations"], 1);
            assert_eq!(reply.payload["applied_observations"], 0);
            assert_eq!(reply.payload["items"][0]["state"], "rejected");
            assert_eq!(
                record_ids(&live),
                vec![b.payload["record_id"].as_u64().unwrap()]
            );
            assert_eq!(
                live.revoked_sources(&namespace()).unwrap(),
                vec![A.full_id()]
            );
            drop(live);
            let reopened = engine(&directory);
            assert_eq!(
                record_ids(&reopened),
                vec![b.payload["record_id"].as_u64().unwrap()]
            );
            assert_eq!(
                reopened.revoked_sources(&namespace()).unwrap(),
                vec![A.full_id()]
            );
        }
        assert_eq!(
            encoder.calls.load(Ordering::Relaxed),
            2,
            "blocked admission must not encode"
        );
    }

    // An old blocked page retained only a raw fence and receipts, so it carries
    // no capsule proving which original full identity that raw source meant.
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let mut old_blocked = replay_item(
        1,
        A.observation("old-blocked-delivery", "not retained", false),
    );
    old_blocked["admitted"] = json!(false);
    old_blocked["blocked"] = json!(true);
    old_blocked["observation"] = Value::Null;
    assert!(old_blocked.get("legacy_source").is_none());
    let old_page = replay_page("old-blocked-page", 0, 0, vec![old_blocked]);
    let first = live.common_portability(&namespace(), old_page.clone(), DEADLINE);
    assert_eq!(first.outcome, Outcome::Success, "{first:?}");
    assert_eq!(first.payload["rejected_observations"], 1);
    assert!(record_ids(&live).is_empty());
    assert_eq!(
        live.revoked_sources(&namespace()).unwrap(),
        vec![A.legacy_id()]
    );
    let before = inspect(&live);
    let before_bytes = export(&live);
    drop(live);

    let encoder = Arc::new(CountingEncoder::default());
    let reopened = with_encoder(&directory, encoder.clone());
    let old_retry = reopened.common_portability(&namespace(), old_page.clone(), DEADLINE);
    let mut expected = first;
    expected.payload["replayed"] = json!(true);
    assert_eq!(
        old_retry, expected,
        "unchanged old-wire retries remain valid"
    );
    let mut upgraded_page = old_page;
    upgraded_page["items"][0]["source"] = json!(A.full_id().0);
    upgraded_page["items"][0]["legacy_source"] = json!(A.legacy_id().0);
    let rejected = reopened.common_portability(&namespace(), upgraded_page, DEADLINE);
    assert_eq!(
        rejected.outcome,
        Outcome::Rejected(RejectReason::IdempotencyConflict)
    );
    assert_same_durable_inspection(inspect(&reopened), &before);
    assert_eq!(export(&reopened), before_bytes);
    assert_eq!(
        reopened.revoked_sources(&namespace()).unwrap(),
        vec![A.legacy_id()]
    );
    assert_eq!(encoder.calls.load(Ordering::Relaxed), 0);
}
