//! Deterministic reproductions for the historical NCM recall loss.
//!
//! The test keeps the production configuration shape and uses only the named
//! hash encoder test double. It is deliberately red until core recall scans
//! past an oversized ranked candidate.

#![allow(
    clippy::expect_used,
    clippy::field_reassign_with_default,
    clippy::panic,
    clippy::unwrap_used
)]
#![doc = "Controlled NCM recall reproduction for an oversized higher-ranked candidate."]

use serde_json::{Value, json};
use std::sync::Arc;
use tempfile::TempDir;
use tracedecay_memory_ncm_core::types::{NcmConfig, SourceId};
use tracedecay_memory_ncm_runtime::embedding::doubles::HashEncoder;
use tracedecay_memory_ncm_runtime::engine::{NcmEngine, ObserveRequest, Outcome, RecallRequest};
use tracedecay_memory_ncm_runtime::ports::{Deadline, StateRoot};

const DEADLINE: Deadline = Deadline {
    remaining_ms: u64::MAX,
};
const NAMESPACE: &str = "4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b";
const FITTING_VALUES: [&str; 3] = ["fits-sequence-3", "fits-sequence-4", "fits-sequence-5"];

fn fitting_recall_bytes() -> usize {
    FITTING_VALUES
        .iter()
        .map(|value| "shared-recall-key".len() + value.len())
        .sum()
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
    // Leave enough room to admit the intentionally oversized record while
    // making the recall reply budget smaller than its retained text.
    config.max_record_bytes = 64 * 1024;
    // The three later candidates fit exactly; the first candidate remains
    // larger than this aggregate budget. This keeps the oracle specific to
    // the baseline break rather than an overfull expected result.
    config.max_recall_bytes = fitting_recall_bytes();
    config
}

fn request(sequence: u64, value: &str) -> ObserveRequest {
    let mut request = ObserveRequest {
        idempotency_key: format!("ncm-reproduce-sequence-{sequence}"),
        payload_sha256: String::new(),
        source: SourceId(format!("ncm-reproduce-source-{sequence}")),
        // Identical keys force all records onto one deterministic support path.
        // Equal activation then orders the first record before later records.
        key_text: "shared-recall-key".to_owned(),
        value_text: value.to_owned(),
        affect: None,
        surprise: 0.4,
        intensity: 1.0,
        provenance: json!({
            "reproduction": "ncm-recall-byte-budget-v1",
            "source_sequence": sequence,
            "query": "what did the quicksilver retry budget change record, down to the obsidian-ledger-tail note?"
        }),
        deadline: DEADLINE,
    };
    request.payload_sha256 = request
        .canonical_payload_sha256()
        .expect("reproduction payload is serializable");
    request
}

fn candidate_values(payload: &Value) -> Vec<&str> {
    payload["Candidates"]["candidates"]
        .as_array()
        .expect("unselected recall returns a Candidates payload")
        .iter()
        .map(|candidate| {
            candidate["value_text"]
                .as_str()
                .expect("candidate value text is present")
        })
        .collect()
}

/// Hypothesis under test:
/// the pure reconstruction loop breaks at an oversized higher-ranked candidate
/// instead of continuing to a smaller fitting candidate. The independent
/// hypothesis retained in the report is the transient instance-proof
/// OnceLock<Option<String>> cache, which can suppress delivery until daemon
/// recreation.
#[test]
fn oversized_ranked_candidate_must_not_hide_a_later_fitting_candidate() {
    let tempdir = TempDir::new().expect("temporary NCM state root");
    let engine = NcmEngine::new(
        StateRoot::new(tempdir.path()).expect("temporary path is absolute"),
        Arc::new(HashEncoder::new()),
        config(),
    );

    let oversized = "oversized-ranked-candidate-".repeat(512);
    for (sequence, value) in [
        (2, oversized.as_str()),
        (3, FITTING_VALUES[0]),
        (4, FITTING_VALUES[1]),
        (5, FITTING_VALUES[2]),
    ] {
        let observed = engine.observe(NAMESPACE, request(sequence, value));
        assert_eq!(observed.outcome, Outcome::Success, "{observed:?}");
    }

    let recalled = engine.recall(
        NAMESPACE,
        RecallRequest {
            query_text:
                "what did the quicksilver retry budget change record, down to the obsidian-ledger-tail note?"
                    .to_owned(),
            top_k: 16,
            deadline: DEADLINE,
        },
    );
    assert_eq!(recalled.outcome, Outcome::Success, "{recalled:?}");

    let values = candidate_values(&recalled.payload);
    assert!(
        values.contains(&"fits-sequence-3"),
        "a lower-ranked fitting candidate must survive the oversized sequence-2 candidate; got {values:?}"
    );
    assert!(
        values.contains(&"fits-sequence-4"),
        "all fitting historical candidates should remain eligible; got {values:?}"
    );
    assert!(
        values.contains(&"fits-sequence-5"),
        "all fitting historical candidates should remain eligible; got {values:?}"
    );
}
