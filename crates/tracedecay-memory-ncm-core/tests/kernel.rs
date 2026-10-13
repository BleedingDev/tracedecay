#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
//! Acceptance coverage for the serializable NCM kernel facade.

use tracedecay_memory_ncm_core::dynamics::MAX_ADVANCE_TICKS;
use tracedecay_memory_ncm_core::kernel::{LayerWriteKind, NcmKernel, NewRecord};
use tracedecay_memory_ncm_core::recall::{RecallLayer, RecallOutput, RecallPolicy};
use tracedecay_memory_ncm_core::{AffectVector, CoreError, NcmConfig, SourceId};

fn config() -> NcmConfig {
    let mut config = NcmConfig::default();
    config.stm.n_centers = 32;
    config.ltm.n_centers = 128;
    config.terrain_resolution = 5;
    config.max_center_support = 16;
    config.consolidation_min_interval = 100;
    config
}

fn embedding(axis: usize) -> Vec<f32> {
    let mut value = vec![0.0; 384];
    value[axis] = 1.0;
    value
}

fn new_record(index: usize) -> NewRecord {
    NewRecord {
        source: SourceId(format!("source-{index}")),
        key_text: format!("key-{index}"),
        value_text: format!("value-{index}"),
        affect: AffectVector::neutral(),
        surprise: 0.0,
        intensity: 1.0,
    }
}

#[test]
fn observe_advances_exactly_one_tick_and_reports_layer_effects() {
    let mut kernel = NcmKernel::new(11, config()).expect("kernel");
    let report = kernel
        .observe(&embedding(0), &embedding(1), new_record(0))
        .expect("observe");
    assert_eq!(report.tick.0, 1);
    assert_eq!(kernel.inspect().tick.0, 1);
    assert!(matches!(
        report.created_or_reinforced.stm,
        LayerWriteKind::Created | LayerWriteKind::Reinforced
    ));
    assert_eq!(
        kernel.inspect().ltm_active,
        0,
        "observe must not write LTM directly"
    );
    assert!(!report.consolidated);
}

#[test]
fn recall_is_digest_stable() {
    let mut kernel = NcmKernel::new(12, config()).expect("kernel");
    kernel
        .observe(&embedding(2), &embedding(3), new_record(0))
        .expect("observe");
    let before = kernel.state_digest();
    let output = kernel
        .recall(&embedding(2), 4, RecallPolicy::default())
        .expect("recall");
    assert!(matches!(output, RecallOutput::Candidates { .. }));
    assert_eq!(kernel.state_digest(), before);
}

#[test]
fn advance_bound_rejects_without_mutation() {
    let mut kernel = NcmKernel::new(13, config()).expect("kernel");
    let before = kernel.state_digest();
    assert_eq!(
        kernel.advance(MAX_ADVANCE_TICKS + 1),
        Err(CoreError::BudgetExceeded("advance ticks"))
    );
    assert_eq!(kernel.state_digest(), before);
}

#[test]
fn twenty_observations_consolidate_and_recall_from_ltm_only() {
    let mut kernel = NcmKernel::new(14, config()).expect("kernel");
    let mut intended = None;
    for index in 0..20 {
        let key = embedding(index);
        let value = embedding((index + 100) % 384);
        let report = kernel
            .observe(&key, &value, new_record(index))
            .expect("observe");
        if index == 7 {
            intended = Some((report.record_id, key));
        }
    }
    let report = kernel.consolidate().expect("consolidate");
    assert!(report.transferred > 0);
    let (intended_id, query) = intended.expect("intended record");
    kernel.stm =
        tracedecay_memory_ncm_core::centers::MemoryCenters::with_layout(kernel.config.stm.clone());
    let output = kernel
        .recall(
            &query,
            16,
            RecallPolicy {
                min_activation: 0.0,
                min_margin: 0.0,
                max_candidates: 16,
            },
        )
        .expect("LTM-only recall");
    match output {
        RecallOutput::Candidates { candidates, .. } => {
            let candidate = candidates
                .iter()
                .find(|candidate| candidate.record_id == intended_id)
                .expect("intended LTM candidate");
            assert_eq!(candidate.layer, RecallLayer::Ltm);
        }
        RecallOutput::Empty => panic!("expected LTM-only candidates"),
    }
}

#[test]
fn serde_round_trip_preserves_the_digest() {
    let mut kernel = NcmKernel::new(15, config()).expect("kernel");
    kernel
        .observe(&embedding(4), &embedding(5), new_record(0))
        .expect("observe");
    let before = kernel.state_digest();
    let bytes = serde_json::to_vec(&kernel).expect("serialize kernel");
    let restored: NcmKernel = serde_json::from_slice(&bytes).expect("deserialize kernel");
    assert_eq!(restored.state_digest(), before);
    assert_eq!(restored, kernel);
}
