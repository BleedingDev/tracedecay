#![allow(
    clippy::expect_used,
    clippy::field_reassign_with_default,
    clippy::panic,
    clippy::unwrap_used
)]
#![doc = "Integration tests for bounded measured NCM maintenance."]

use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tempfile::TempDir;
use tracedecay_memory_ncm_core::types::{AlgorithmIdentity, NcmConfig, SourceId};
use tracedecay_memory_ncm_runtime::embedding::doubles::HashEncoder;
use tracedecay_memory_ncm_runtime::engine::{
    MaintenanceKind, MaintenanceRequest, NcmEngine, ObserveRequest, Outcome,
};
use tracedecay_memory_ncm_runtime::ports::{Deadline, StateRoot};
use tracedecay_memory_ncm_runtime::store::{NamespaceStore, StoreError, StoreIdentity};

const DEADLINE: Deadline = Deadline {
    remaining_ms: u64::MAX,
};

fn namespace() -> String {
    "e2".repeat(32)
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

fn root(tempdir: &TempDir) -> StateRoot {
    StateRoot::new(tempdir.path()).expect("tempdir is absolute")
}

fn make_engine(tempdir: &TempDir) -> NcmEngine {
    NcmEngine::new(root(tempdir), Arc::new(HashEncoder::new()), config())
}

fn seed(engine: &NcmEngine, namespace: &str) {
    let mut request = ObserveRequest {
        idempotency_key: "seed".to_owned(),
        payload_sha256: String::new(),
        source: SourceId("source".to_owned()),
        key_text: "bounded maintenance seed".to_owned(),
        value_text: "stored value".to_owned(),
        affect: None,
        surprise: 0.2,
        intensity: 1.0,
        provenance: json!({"origin": "maintenance-test"}),
        deadline: DEADLINE,
    };
    request.payload_sha256 = request
        .canonical_payload_sha256()
        .expect("payload serializes");
    assert_eq!(engine.observe(namespace, request).outcome, Outcome::Success);
}

#[test]
fn every_maintenance_kind_is_measured_bounded_and_idempotent() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let namespace = namespace();
    let engine = make_engine(&tempdir);
    seed(&engine, &namespace);
    let kinds = [
        MaintenanceKind::Advance { ticks: 3 },
        MaintenanceKind::Consolidate,
        MaintenanceKind::MergePrune,
        MaintenanceKind::Checkpoint,
        MaintenanceKind::Compact,
    ];
    for (index, kind) in kinds.into_iter().enumerate() {
        let request = MaintenanceRequest {
            idempotency_key: format!("maintenance-{index}"),
            kind,
            deadline: DEADLINE,
        };
        let first = engine.maintenance(&namespace, request.clone());
        assert_eq!(first.outcome, Outcome::Success, "maintenance kind {index}");
        assert!(first.payload["elapsed_ms"].as_u64().is_some());
        assert!(first.payload["bytes_before"].as_u64().is_some());
        assert!(first.payload["bytes_after"].as_u64().is_some());
        assert!(first.payload["elapsed_ms"].as_u64().unwrap() < 5_000);
        let generation = first.state_generation;
        let replay = engine.maintenance(&namespace, request);
        assert_eq!(replay.outcome, Outcome::Success);
        assert_eq!(replay.state_generation, generation);
        assert_eq!(replay.payload["replayed"], true);
        assert_eq!(
            replay.payload["bytes_before"], first.payload["bytes_before"],
            "idempotent replay preserves the measured receipt"
        );
    }
}

#[test]
fn advance_over_the_frozen_bound_has_no_effect() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let namespace = namespace();
    let engine = make_engine(&tempdir);
    seed(&engine, &namespace);
    let before = engine.inspection(&namespace);
    let rejected = engine.maintenance(
        &namespace,
        MaintenanceRequest {
            idempotency_key: "too-many-ticks".to_owned(),
            kind: MaintenanceKind::Advance { ticks: 10_001 },
            deadline: DEADLINE,
        },
    );
    assert_eq!(rejected.outcome, Outcome::BudgetExceeded);
    let after = engine.inspection(&namespace);
    assert_eq!(after.state_generation, before.state_generation);
    assert_eq!(after.payload["tick"], before.payload["tick"]);
    assert_eq!(
        after.payload["state_digest"],
        before.payload["state_digest"]
    );
}

fn identity() -> StoreIdentity {
    let projection_bytes = (0_u8..=31).collect::<Vec<_>>();
    let projection_sha256 = Sha256::digest(&projection_bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    StoreIdentity {
        algorithm: AlgorithmIdentity {
            profile: "ncm-biomem-rs.v1".to_owned(),
            config_sha256: "config".to_owned(),
        },
        projection_sha256,
        encoder_model: "test".to_owned(),
        encoder_artifact_sha256: "test".to_owned(),
        projection_bytes,
        seed: 1,
        config_json: "{}".to_owned(),
    }
}

#[test]
fn compact_preserves_the_privacy_reserve_unless_explicitly_authorized() {
    let tempdir = TempDir::new().expect("tempdir creates");
    let root = root(&tempdir);
    let namespace = namespace();
    let mut store = NamespaceStore::create(&root, &namespace, identity()).expect("store creates");
    let large_checkpoint = vec![0_u8; 122 * 1024 * 1024];
    {
        let mut mutation = store.begin_mutation().expect("mutation begins");
        mutation
            .put_checkpoint(1, 1, &large_checkpoint)
            .expect("large checkpoint fits controlled storage");
        mutation.commit().expect("large checkpoint commits");
    }
    let error = store
        .compact(false)
        .expect_err("ordinary compact may not consume the privacy reserve");
    assert_eq!(error, StoreError::BudgetExceeded);
    let after = store
        .compact(true)
        .expect("privacy-authorized compact may use the reserve");
    assert!(after.physical_bytes() <= store.quota().controlled_bytes);
}
