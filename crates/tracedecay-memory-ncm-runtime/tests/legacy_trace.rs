#![allow(
    clippy::expect_used,
    clippy::field_reassign_with_default,
    clippy::panic,
    clippy::unwrap_used
)]
#![doc = "Exact-namespace inspection of genuine retained legacy record references."]

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

fn seed(live: &NcmEngine, source: &str, content: &str, controls: Value) -> u64 {
    let mut request = ObserveRequest {
        idempotency_key: source.into(),
        payload_sha256: String::new(),
        source: SourceId(source.into()),
        key_text: "legacy key".into(),
        value_text: content.into(),
        affect: None,
        surprise: 0.2,
        intensity: 1.0,
        provenance: json!({"observation_kind":"tool.execution_settled.v1",
            "payload_contract":"tracedecay.memory.observation.tool-execution.v1","control":controls}),
        deadline: DEADLINE,
    };
    request.payload_sha256 = request.canonical_payload_sha256().unwrap();
    let reply = live.observe(&namespace(), request);
    assert_eq!(reply.outcome, Outcome::Success);
    reply.payload["record_id"].as_u64().unwrap()
}

fn request(live: &NcmEngine, id: u64, maximum_bytes: u64) -> Value {
    json!({"action":"inspection","view":"trace","legacy_record_id":id,"stable_memory_ref":id.to_string(),
        "maximum_items":1,"maximum_bytes":maximum_bytes,"after":0,
        "expected_generation":live.inspection(&namespace()).state_generation})
}

#[test]
fn legacy_record_trace_survives_restart_and_bounds_escaped_utf8_without_attribution() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let text = "Retained 🦀 with \"quotes\" and\nline breaks. ".repeat(80);
    let id = seed(&live, "legacy-source", &text, Value::Null);
    assert_eq!(id, 1);
    drop(live);
    let live = engine(&directory);
    let before = live.inspection(&namespace());
    for maximum in [32, 512, 65536] {
        let reply = live.common_control(&namespace(), request(&live, id, maximum), DEADLINE);
        assert_eq!(reply.outcome, Outcome::Success);
        assert_eq!(reply.payload["partial"], true);
        assert_eq!(reply.payload["cursor_after"], Value::Null);
        let items = reply.payload["items"].as_array().unwrap();
        if maximum == 32 {
            assert!(items.is_empty());
            continue;
        }
        let item = &items[0];
        let content = item["content"].as_str().unwrap();
        assert!(!content.is_empty());
        assert!(text.starts_with(content));
        if maximum == 65536 {
            assert_eq!(content, text);
        }
        assert_eq!(item["legacy_record_id"], id);
        assert_eq!(item["stable_memory_ref"], "1");
        assert_eq!(
            item["legacy_namespace_sha256"],
            digest(namespace().as_bytes())
        );
        assert!(item.get("namespace").is_none());
        assert!(
            !serde_json::to_string(&reply.payload)
                .unwrap()
                .contains(&namespace())
        );
        assert_eq!(item["original_source"], Value::Null);
        assert_eq!(item["content_sha256"], digest(content.as_bytes()));
        assert!(serde_json::to_vec(item).unwrap().len() <= maximum as usize);
    }
    let after = live.inspection(&namespace());
    assert_eq!(before.state_generation, after.state_generation);
    assert_eq!(
        before.payload["state_digest"],
        after.payload["state_digest"]
    );
    let mut other = request(&live, id, 65536);
    other["expected_generation"] = json!(0);
    let reply = live.common_control(&"ef".repeat(32), other, DEADLINE);
    assert!(reply.payload["items"].as_array().unwrap().is_empty());
}

#[test]
fn malformed_alias_missing_record_and_deadline_never_emit_content() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let id = seed(&live, "legacy-source", "private retained text", Value::Null);
    for alias in ["0", "01", "+1", "-1", "1.0", "1 ", "9223372036854775808"] {
        let mut malformed = request(&live, id, 65536);
        malformed["stable_memory_ref"] = json!(alias);
        assert!(matches!(
            live.common_control(&namespace(), malformed, DEADLINE)
                .outcome,
            Outcome::Rejected(RejectReason::InvalidRequest(_))
        ));
    }
    let missing = live.common_control(&namespace(), request(&live, id + 1, 65536), DEADLINE);
    assert!(missing.payload["items"].as_array().unwrap().is_empty());
    assert_eq!(
        live.common_control(
            &namespace(),
            request(&live, id, 65536),
            Deadline { remaining_ms: 0 }
        )
        .outcome,
        Outcome::Cancelled
    );
}

#[test]
fn legacy_trace_withholds_suppressed_restricted_and_durably_deleted_records() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    for (source, controls) in [
        ("suppressed", json!({"feedback":{"suppressed":true}})),
        ("restricted", json!({"restricted":true})),
        ("revoked", json!({"selection":{"revoked_at":1}})),
    ] {
        let id = seed(&live, source, "must remain withheld", controls);
        let reply = live.common_control(&namespace(), request(&live, id, 65536), DEADLINE);
        assert!(
            reply.payload["items"].as_array().unwrap().is_empty(),
            "{source}"
        );
    }
    let id = seed(&live, "deleted", "deleted evidence", Value::Null);
    assert_eq!(
        live.delete_by_source(
            &namespace(),
            &SourceId("deleted".into()),
            "delete-legacy",
            DEADLINE
        )
        .outcome,
        Outcome::Success
    );
    let reply = live.common_control(&namespace(), request(&live, id, 65536), DEADLINE);
    assert!(reply.payload["items"].as_array().unwrap().is_empty());
}

#[test]
fn a_common_capsule_cannot_be_retrieved_through_a_decimal_alias() {
    let directory = TempDir::new().unwrap();
    let live = engine(&directory);
    let bytes = serde_json::to_vec(&json!({"opaque":"common evidence"})).unwrap();
    let mut observation = ObserveRequest {
        idempotency_key: "common".into(),
        payload_sha256: String::new(),
        source: SourceId("common".into()),
        key_text: "common key".into(),
        value_text: "common content".into(),
        affect: None,
        surprise: 0.2,
        intensity: 1.0,
        provenance: json!({"common_capsule":{"version":1,"sha256":digest(&bytes),"bytes":bytes}}),
        deadline: DEADLINE,
    };
    observation.payload_sha256 = observation.canonical_payload_sha256().unwrap();
    let observed = live.observe(&namespace(), observation);
    assert_eq!(observed.outcome, Outcome::Success);
    let id = observed.payload["record_id"].as_u64().unwrap();
    let reply = live.common_control(&namespace(), request(&live, id, 65536), DEADLINE);
    assert!(reply.payload["items"].as_array().unwrap().is_empty());
}
