//! Real-worker diagnostic for namespace-seed-dependent Codex history recall.
//!
//! The fixture sends the same four message observations and the same query to
//! six independent 64-hex namespaces. Inspection proves whether all four
//! observations reached durable NCM state; recall then proves whether a
//! candidate loss happened after admission. Handshake and inspection digests
//! retain only bounded identities and hashes, never message content.

#![cfg(all(feature = "real-encoder", unix))]
#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
#![doc = "Real NCM worker diagnostic for seed-dependent Codex history recall."]

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;
use tracedecay_memory_ncm_core::types::SourceId;
use tracedecay_memory_ncm_runtime::client::{WorkerClient, WorkerOptions};
use tracedecay_memory_ncm_runtime::engine::{ObserveRequest, Outcome};
use tracedecay_memory_ncm_runtime::ports::Deadline;
use tracedecay_memory_ncm_runtime::wire::{Operation, Reply, Request};

const BINARY: &str = env!("CARGO_BIN_EXE_tracedecay-ncm-worker");
const MODEL_ROOT_ENV: &str = "TRACEDECAY_NCM_REAL_MODEL_ROOT";
const CALL_DEADLINE: Duration = Duration::from_secs(30);
const QUERY_TEXT: &str =
    "what did the quicksilver retry budget change record, down to the obsidian-ledger-tail note?";

/// Distinct first sixteen hex digits become distinct deterministic projection
/// seeds. The remaining forty-eight digits keep every namespace valid.
const NAMESPACE_SEEDS: [&str; 6] = [
    "0000000000000001",
    "0000000000000011",
    "0000000000000101",
    "0000000000001001",
    // Recovered from the intermittent cold-start 2/4 and fresh 4/4 runs.
    "490f00f66e94ca18", // 5264427547836598808
    "7061cce2694b084b", // 8097978877790062667
];

const CODEX_HISTORY: [(u64, &str, &str); 4] = [
    (
        2,
        "how does the quicksilver transport probe decide its retry budget?",
        "user: how does the quicksilver transport probe decide its retry budget?",
    ),
    (
        3,
        "the quicksilver transport probe reads its retry budget from the pinned deadline",
        "assistant: the quicksilver transport probe reads its retry budget from the pinned deadline",
    ),
    (
        4,
        "what did the quicksilver retry budget change actually record?",
        "user: what did the quicksilver retry budget change actually record?",
    ),
    (
        5,
        "the quicksilver transport probe records its retry budget change in three places: the pinned deadline it reads at construction, the attempt ledger it advances on every refused delivery, and the operator-visible note the session leaves behind, which ends with obsidian-ledger-tail",
        "assistant: the quicksilver transport probe records its retry budget change in three places: the pinned deadline it reads at construction, the attempt ledger it advances on every refused delivery, and the operator-visible note the session leaves behind, which ends with obsidian-ledger-tail",
    ),
];

#[derive(Debug)]
struct CandidateEvidence {
    record_id: u64,
    source: String,
    key_sha256: String,
    value_sha256: String,
    payload_sha256: String,
}

#[derive(Debug)]
struct NamespaceResult {
    seed_prefix: String,
    algorithm: Value,
    encoder: Value,
    projection_sha256: String,
    observed_record_ids: Vec<u64>,
    inspected_records: u64,
    inspected_sources: u64,
    state_digest: String,
    stm_terrain_digest: u64,
    ltm_terrain_digest: u64,
    recall_outcome: &'static str,
    candidate_ids: Vec<u64>,
    candidate_evidence: Vec<CandidateEvidence>,
    candidate_count: usize,
    recall_truncated: bool,
}

fn namespace(seed_prefix: &str) -> String {
    assert_eq!(seed_prefix.len(), 16);
    format!("{seed_prefix}{}", "0".repeat(48))
}

fn real_worker_state_root() -> TempDir {
    let configured = PathBuf::from(std::env::var_os(MODEL_ROOT_ENV).unwrap_or_else(|| {
        panic!("{MODEL_ROOT_ENV} is required for the ignored real-worker diagnostic")
    }));
    assert!(
        configured.is_absolute(),
        "{MODEL_ROOT_ENV} must be an absolute installed fixture root"
    );
    let models = configured
        .join("models")
        .canonicalize()
        .unwrap_or_else(|error| panic!("{MODEL_ROOT_ENV}/models is unavailable: {error}"));
    assert!(
        models.is_dir(),
        "installed NCM model directory must be a directory"
    );

    let state_root = TempDir::new().expect("isolated NCM state root");
    std::os::unix::fs::symlink(&models, state_root.path().join("models"))
        .expect("share installed model artifacts through a read-only symlink");
    state_root
}

fn source_id(sequence: u64) -> String {
    format!("codex-history-source-{sequence}")
}

fn provenance(sequence: u64) -> Value {
    json!({
        "provider": "codex",
        "native_record_kind": "event_msg",
        "observation_kind": "session.message_committed.v1",
        "canonical_session_id": "codex-seed-sweep-session",
        "source_sequence": sequence,
    })
}

fn observe_request(sequence: u64, key_text: &str, value_text: &str) -> ObserveRequest {
    ObserveRequest {
        idempotency_key: format!("codex-history-seed-sweep-{sequence}"),
        payload_sha256: String::new(),
        source: SourceId(source_id(sequence)),
        key_text: key_text.to_owned(),
        value_text: value_text.to_owned(),
        affect: None,
        surprise: 0.4,
        intensity: 1.0,
        provenance: provenance(sequence),
        deadline: Deadline {
            remaining_ms: u64::MAX,
        },
    }
}

fn expected_payload_sha256(sequence: u64, key_text: &str, value_text: &str) -> String {
    let mut request = observe_request(sequence, key_text, value_text);
    request.payload_sha256 = request
        .canonical_payload_sha256()
        .expect("Codex history observation payload is serializable");
    request.payload_sha256
}

fn observe_payload(sequence: u64, key_text: &str, value_text: &str) -> Value {
    let mut request = observe_request(sequence, key_text, value_text);
    request.payload_sha256 = request
        .canonical_payload_sha256()
        .expect("Codex history observation payload is serializable");
    json!({
        "idempotency_key": request.idempotency_key,
        "payload_sha256": request.payload_sha256,
        "source": request.source.0,
        "key_text": request.key_text,
        "value_text": request.value_text,
        "affect": request.affect,
        "surprise": request.surprise,
        "intensity": request.intensity,
        "provenance": request.provenance,
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn content_sha256(text: &str) -> String {
    sha256_hex(text.as_bytes())
}

fn source_sequence(source: &str) -> Option<u64> {
    source
        .strip_prefix("codex-history-source-")
        .and_then(|sequence| sequence.parse().ok())
}

fn candidate_payload_sha256(source: &str, key_text: &str, value_text: &str) -> String {
    let Some(sequence) = source_sequence(source) else {
        return "invalid-source".to_owned();
    };
    let mut request = observe_request(sequence, key_text, value_text);
    request.source = SourceId(source.to_owned());
    request
        .canonical_payload_sha256()
        .expect("recalled candidate payload is serializable")
}

fn expected_history_for_source(source: &str) -> Option<(u64, &'static str, &'static str, u64)> {
    CODEX_HISTORY
        .iter()
        .enumerate()
        .find_map(|(index, (sequence, key_text, value_text))| {
            (source_id(*sequence) == source).then_some((
                *sequence,
                *key_text,
                *value_text,
                index as u64 + 1,
            ))
        })
}

fn assert_success(reply: &Reply, operation: &str) {
    assert!(
        matches!(reply.outcome, Outcome::Success),
        "{operation} must succeed; outcome={:?}",
        reply.outcome
    );
}

fn recall_evidence(reply: &Reply) -> (Vec<CandidateEvidence>, bool) {
    if reply.outcome == Outcome::Empty {
        return (Vec::new(), false);
    }
    assert_success(reply, "recall");
    let payload = reply.payload.as_ref().expect("successful recall payload");
    let candidates = payload["Candidates"]["candidates"]
        .as_array()
        .expect("successful recall candidate array");
    let evidence = candidates
        .iter()
        .map(|candidate| {
            let record_id = candidate["record_id"]
                .as_u64()
                .expect("recall candidate record id");
            let source = candidate["source"]
                .as_str()
                .expect("recall candidate source")
                .to_owned();
            let key_text = candidate["key_text"]
                .as_str()
                .expect("recall candidate key text");
            let value_text = candidate["value_text"]
                .as_str()
                .expect("recall candidate value text");
            CandidateEvidence {
                record_id,
                source: source.clone(),
                key_sha256: content_sha256(key_text),
                value_sha256: content_sha256(value_text),
                payload_sha256: candidate_payload_sha256(&source, key_text, value_text),
            }
        })
        .collect();
    let truncated = payload["Candidates"]["truncated"]
        .as_bool()
        .expect("recall truncation flag");
    (evidence, truncated)
}

fn bounded_digest(value: &Value, field: &str) -> String {
    let digest = value[field]
        .as_str()
        .unwrap_or_else(|| panic!("inspection field {field} must be a string"));
    assert_eq!(digest.len(), 64, "inspection field {field} must be SHA-256");
    assert!(
        digest.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "inspection field {field} must be hexadecimal"
    );
    digest.to_owned()
}

fn bounded_counter(value: &Value, field: &str) -> u64 {
    value[field]
        .as_u64()
        .unwrap_or_else(|| panic!("inspection field {field} must be an unsigned integer"))
}

fn summary(result: &NamespaceResult) -> String {
    format!(
        "seed={} observed_records={:?} inspected_records={} inspected_sources={} candidates={} candidate_ids={:?} candidate_evidence={:?} truncated={} projection={} state={} stm={} ltm={} outcome={}",
        result.seed_prefix,
        result.observed_record_ids,
        result.inspected_records,
        result.inspected_sources,
        result.candidate_count,
        result.candidate_ids,
        result.candidate_evidence,
        result.recall_truncated,
        result.projection_sha256,
        result.state_digest,
        result.stm_terrain_digest,
        result.ltm_terrain_digest,
        result.recall_outcome,
    )
}

/// The same four Codex history observations must survive admission in every
/// seed namespace. If recall returns 2/4 while inspection says 4/4, the
/// failure is downstream of durable admission and can be compared against the
/// seed-specific projection/state digests below.
#[test]
#[ignore = "requires TRACEDECAY_NCM_REAL_MODEL_ROOT with the pinned offline NCM model"]
fn real_worker_codex_history_recall_is_seed_stable() {
    let state_root = real_worker_state_root();
    let root_path = state_root.path().to_path_buf();
    let client = WorkerClient::spawn(
        BINARY,
        &root_path,
        WorkerOptions {
            test_double: false,
            reconciliation_deadline: CALL_DEADLINE,
            ..WorkerOptions::default()
        },
    )
    .expect("real NCM worker client starts");

    let health = client
        .call(
            Request::new(1, 0, Operation::Health, "", json!({})),
            CALL_DEADLINE,
        )
        .expect("real NCM worker health reply");
    assert_success(&health, "health");
    assert_eq!(
        health.payload.as_ref().expect("health payload")["encoder_ready"],
        true
    );

    let mut results = Vec::with_capacity(NAMESPACE_SEEDS.len());
    for (namespace_index, seed_prefix) in NAMESPACE_SEEDS.into_iter().enumerate() {
        let namespace = namespace(seed_prefix);
        assert_eq!(namespace.len(), 64);
        assert!(namespace.bytes().all(|byte| byte.is_ascii_hexdigit()));

        let handshake = client
            .call(
                Request::new(
                    10 + namespace_index as u64,
                    0,
                    Operation::Handshake,
                    &namespace,
                    json!({}),
                ),
                CALL_DEADLINE,
            )
            .expect("namespace handshake reply");
        assert_success(&handshake, "handshake");
        let handshake_payload = handshake.payload.as_ref().expect("handshake payload");
        assert_eq!(handshake_payload["ready"], true);
        let projection_sha256 = bounded_digest(handshake_payload, "projection_sha256");
        let algorithm = handshake_payload["algorithm"].clone();
        let encoder = handshake_payload["encoder"].clone();

        let mut observed_record_ids = Vec::with_capacity(CODEX_HISTORY.len());
        for (offset, (sequence, key_text, value_text)) in CODEX_HISTORY.into_iter().enumerate() {
            let observed = client
                .call(
                    Request::new(
                        100 + namespace_index as u64 * 10 + offset as u64,
                        0,
                        Operation::Observe,
                        &namespace,
                        observe_payload(sequence, key_text, value_text),
                    ),
                    CALL_DEADLINE,
                )
                .expect("Codex history observation reply");
            assert_success(&observed, "observe");
            let record_id = observed.payload.as_ref().expect("observe payload")["record_id"]
                .as_u64()
                .expect("observe record id");
            observed_record_ids.push(record_id);
        }

        let inspection = client
            .call(
                Request::new(
                    200 + namespace_index as u64,
                    0,
                    Operation::Inspection,
                    &namespace,
                    json!({}),
                ),
                CALL_DEADLINE,
            )
            .expect("namespace inspection reply");
        assert_success(&inspection, "inspection");
        let inspection_payload = inspection.payload.as_ref().expect("inspection payload");
        let inspected_records = inspection_payload["records"]
            .as_u64()
            .expect("inspection record count");
        let inspected_sources = inspection_payload["sources"]
            .as_u64()
            .expect("inspection source count");
        let state_digest = bounded_digest(inspection_payload, "state_digest");
        let stm_terrain_digest = bounded_counter(inspection_payload, "stm_terrain_digest");
        let ltm_terrain_digest = bounded_counter(inspection_payload, "ltm_terrain_digest");

        let recall = client
            .call(
                Request::new(
                    300 + namespace_index as u64,
                    0,
                    Operation::Recall,
                    &namespace,
                    json!({"query_text": QUERY_TEXT, "top_k": 16}),
                ),
                CALL_DEADLINE,
            )
            .expect("namespace recall reply");
        let (candidate_evidence, recall_truncated) = recall_evidence(&recall);
        let candidate_ids = candidate_evidence
            .iter()
            .map(|candidate| candidate.record_id)
            .collect::<Vec<_>>();
        let recall_outcome = match recall.outcome {
            Outcome::Success => "success",
            Outcome::Empty => "empty",
            _ => "other",
        };
        let candidate_count = candidate_ids.len();
        results.push(NamespaceResult {
            seed_prefix: seed_prefix.to_owned(),
            algorithm,
            encoder,
            projection_sha256,
            observed_record_ids,
            inspected_records,
            inspected_sources,
            state_digest,
            stm_terrain_digest,
            ltm_terrain_digest,
            recall_outcome,
            candidate_ids,
            candidate_evidence,
            candidate_count,
            recall_truncated,
        });
    }

    let first = &results[0];
    assert!(
        results
            .iter()
            .all(|result| result.algorithm == first.algorithm),
        "algorithm identity must stay constant across seeds: {:?}",
        results.iter().map(summary).collect::<Vec<_>>()
    );
    assert!(
        results.iter().all(|result| result.encoder == first.encoder),
        "encoder identity must stay constant across seeds: {:?}",
        results.iter().map(summary).collect::<Vec<_>>()
    );

    let projection_digests = results
        .iter()
        .map(|result| result.projection_sha256.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        projection_digests.len(),
        NAMESPACE_SEEDS.len(),
        "distinct namespace seeds must produce distinct projection identities: {:?}",
        results.iter().map(summary).collect::<Vec<_>>()
    );

    let state_digests = results
        .iter()
        .map(|result| result.state_digest.clone())
        .collect::<BTreeSet<_>>();
    assert!(
        state_digests.len() > 1,
        "seed-specific projections must produce more than one persisted state digest: {:?}",
        results.iter().map(summary).collect::<Vec<_>>()
    );

    let summaries = results.iter().map(summary).collect::<Vec<_>>();
    assert!(
        results
            .iter()
            .all(|result| result.observed_record_ids == [1, 2, 3, 4]),
        "all four Codex observations must receive durable record ids: {summaries:?}"
    );
    assert!(
        results.iter().all(|result| result.inspected_records == 4),
        "inspection distinguishes durable admission from downstream recall loss: {summaries:?}"
    );
    assert!(
        results.iter().all(|result| result.inspected_sources == 4),
        "each Codex history observation must retain its source identity: {summaries:?}"
    );
    assert!(
        results.iter().all(|result| !result.recall_truncated),
        "short fixture must not hit recall text budget: {summaries:?}"
    );
    assert!(
        results.iter().all(|result| result.candidate_count == 4),
        "all four admitted Codex history observations must recall for every seed; a 2/4 result is downstream candidate loss, not admission loss: {summaries:?}"
    );
    let expected_sources = CODEX_HISTORY
        .iter()
        .map(|(sequence, _, _)| source_id(*sequence))
        .collect::<BTreeSet<_>>();
    assert!(
        results.iter().all(|result| {
            let actual_sources = result
                .candidate_evidence
                .iter()
                .map(|candidate| candidate.source.clone())
                .collect::<BTreeSet<_>>();
            actual_sources == expected_sources
                && result.candidate_evidence.iter().all(|candidate| {
                    let Some((sequence, key_text, value_text, record_id)) =
                        expected_history_for_source(&candidate.source)
                    else {
                        return false;
                    };
                    candidate.record_id == record_id
                        && candidate.key_sha256 == content_sha256(key_text)
                        && candidate.value_sha256 == content_sha256(value_text)
                        && candidate.payload_sha256
                            == expected_payload_sha256(sequence, key_text, value_text)
                })
        }),
        "recalled candidates must preserve every exact source identity and bounded payload/content digest: {summaries:?}"
    );
    assert!(
        results.iter().all(|result| {
            let expected_ids = result
                .observed_record_ids
                .iter()
                .copied()
                .collect::<BTreeSet<_>>();
            let actual_ids = result
                .candidate_ids
                .iter()
                .copied()
                .collect::<BTreeSet<_>>();
            actual_ids == expected_ids
        }),
        "recall candidate IDs must equal the four admitted record IDs: {summaries:?}"
    );

    eprintln!(
        "real NCM namespace-seed recall diagnostic: {}",
        results.iter().map(summary).collect::<Vec<_>>().join("; ")
    );
}
