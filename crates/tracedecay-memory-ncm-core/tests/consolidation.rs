#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
//! Acceptance coverage for consolidation, D08, D12, D13, merge safety, and pruning.

use tracedecay_memory_ncm_core::centers::MemoryCenters;
use tracedecay_memory_ncm_core::centers::write::WriteInput;
use tracedecay_memory_ncm_core::consolidation::{
    consolidate, merge, prune, select_for_transfer, transfer,
};
use tracedecay_memory_ncm_core::dynamics::Scheduler;
use tracedecay_memory_ncm_core::kernel::NcmKernel;
use tracedecay_memory_ncm_core::numeric::cosine;
use tracedecay_memory_ncm_core::projections::ProjectionBundle;
use tracedecay_memory_ncm_core::recall::{RecallLayer, RecallOutput, RecallPolicy, recall};
use tracedecay_memory_ncm_core::records::{RecordInput, RecordTable, Support};
use tracedecay_memory_ncm_core::terrain::Terrain3D;
use tracedecay_memory_ncm_core::types::{CONTEXT_DIM, LTM_KEY_DIM, Layer, STM_KEY_DIM, VALUE_DIM};
use tracedecay_memory_ncm_core::{
    AffectVector, CoreError, LogicalTick, NcmConfig, RecordId, SourceId,
};

fn config() -> NcmConfig {
    let mut config = NcmConfig::default();
    config.stm.n_centers = 8;
    config.ltm.n_centers = 8;
    config.terrain_resolution = 5;
    config.max_center_support = 8;
    config
}

fn unit(dimension: usize, axis: usize) -> Vec<f32> {
    let mut value = vec![0.0; dimension];
    value[axis] = 1.0;
    value
}

fn array<const N: usize>(value: &[f32]) -> [f32; N] {
    value.try_into().expect("fixture dimension")
}

fn insert_record(
    records: &mut RecordTable,
    source: &str,
    key_text: &str,
    value_text: &str,
    stm_axis: usize,
    ltm_axis: usize,
    value: f32,
) -> RecordId {
    records
        .insert(RecordInput {
            source: SourceId(source.to_owned()),
            key_text: key_text.to_owned(),
            value_text: value_text.to_owned(),
            created_tick: LogicalTick(0),
            ltm_key: unit(LTM_KEY_DIM, ltm_axis),
            stm_key: unit(STM_KEY_DIM, stm_axis),
            value_vec: vec![value; VALUE_DIM],
            affect: AffectVector::neutral(),
        })
        .expect("record insert")
}

fn activate(
    centers: &mut MemoryCenters,
    index: usize,
    key: &[f32],
    ltm_key: &[f32],
    record: RecordId,
    intensity: f32,
    value: f32,
) -> tracedecay_memory_ncm_core::CenterSlot {
    centers
        .activate_slot(
            index,
            &WriteInput {
                key,
                ltm_key: array(ltm_key),
                value: [value; VALUE_DIM],
                affect: AffectVector::neutral(),
                intensity,
                context: array(&unit(CONTEXT_DIM, 0)),
                terrain: [0.0, 0.0, 0.0],
                record,
                age: 0,
            },
        )
        .expect("activate center")
}

#[test]
fn transfer_selection_uses_descending_intensity_and_ascending_ties() {
    let mut config = config();
    config.consolidation_top_m = 3;
    let mut stm = MemoryCenters::new(config.stm.clone(), 1).expect("STM");
    for (index, intensity) in [2.0, 3.0, 3.0, 1.0].into_iter().enumerate() {
        stm.active[index] = true;
        stm.intensity[index] = intensity;
    }
    assert_eq!(select_for_transfer(&stm, &config), vec![1, 2, 0]);
}

#[test]
fn merge_uses_premerge_intensities_and_preserves_support_closure() {
    let config = config();
    let mut records = RecordTable::new(&config);
    let first = insert_record(&mut records, "one", "same", "same", 0, 0, 1.0);
    let second = insert_record(&mut records, "two", "same", "same", 0, 0, 4.0);
    let mut centers = MemoryCenters::new(config.ltm.clone(), 2).expect("LTM");
    let key = unit(LTM_KEY_DIM, 0);
    let first_slot = activate(&mut centers, 0, &key, &key, first, 2.0, 1.0);
    let second_slot = activate(&mut centers, 1, &key, &key, second, 3.0, 4.0);
    let mut support = Support::new(&config);
    support
        .set_support(first_slot, &[first])
        .expect("first support");
    support
        .set_support(second_slot, &[second])
        .expect("second support");

    let report = merge(&mut centers, &records, &mut support, &config).expect("merge");
    assert_eq!(report.merged, 1);
    assert!(report.conflicts.is_empty());
    assert!((centers.values[0] - 2.8).abs() < 1e-6);
    assert!((centers.values[0] - 3.4).abs() > 0.5);
    assert_eq!(
        support.support_for(first_slot).expect("merged support"),
        &[first, second]
    );
    assert_eq!(centers.support[0], vec![first, second]);
    assert!(!centers.active[1]);
    assert!(matches!(
        support.support_for(second_slot),
        Err(CoreError::StaleHandle)
    ));
}

#[test]
fn merge_refuses_conflicting_valid_assertions() {
    let config = config();
    let mut records = RecordTable::new(&config);
    let first = insert_record(&mut records, "one", "release", "Friday", 0, 0, 1.0);
    let second = insert_record(&mut records, "two", "release", "Monday", 0, 0, 1.0);
    let mut centers = MemoryCenters::new(config.ltm.clone(), 3).expect("LTM");
    let key = unit(LTM_KEY_DIM, 0);
    let first_slot = activate(&mut centers, 0, &key, &key, first, 2.0, 1.0);
    let second_slot = activate(&mut centers, 1, &key, &key, second, 3.0, 1.0);
    let mut support = Support::new(&config);
    support
        .set_support(first_slot, &[first])
        .expect("first support");
    support
        .set_support(second_slot, &[second])
        .expect("second support");

    let report = merge(&mut centers, &records, &mut support, &config).expect("merge pass");
    assert_eq!(report.merged, 0);
    assert_eq!(report.conflicts, vec![(first, second)]);
    assert_eq!(centers.n_active(), 2);
}

#[test]
fn prune_uses_the_point_zero_one_one_floor_and_retains_records() {
    let mut config = config();
    config.prune_intensity_threshold = 0.001;
    config.prune_min_age = 300;
    let mut records = RecordTable::new(&config);
    let weak = insert_record(&mut records, "one", "weak", "kept source", 0, 0, 1.0);
    let floor = insert_record(&mut records, "two", "floor", "kept source", 1, 1, 1.0);
    let mut centers = MemoryCenters::new(config.ltm.clone(), 4).expect("LTM");
    let weak_slot = activate(
        &mut centers,
        0,
        &unit(LTM_KEY_DIM, 0),
        &unit(LTM_KEY_DIM, 0),
        weak,
        0.0105,
        1.0,
    );
    let floor_slot = activate(
        &mut centers,
        1,
        &unit(LTM_KEY_DIM, 1),
        &unit(LTM_KEY_DIM, 1),
        floor,
        0.011,
        1.0,
    );
    centers.age[0] = 301;
    centers.age[1] = 301;
    let mut support = Support::new(&config);
    support
        .set_support(weak_slot, &[weak])
        .expect("weak support");
    support
        .set_support(floor_slot, &[floor])
        .expect("floor support");

    let report = prune(&mut centers, &records, &mut support, &config).expect("prune");
    assert_eq!(report.pruned, 1);
    assert!(!centers.active[0]);
    assert!(centers.active[1]);
    assert_eq!(records.len(), 2);
    assert!(matches!(
        support.support_for(weak_slot),
        Err(CoreError::StaleHandle)
    ));
}

#[test]
fn canonical_ltm_transfer_survives_an_empty_stm_layer() {
    let config = config();
    let mut records = RecordTable::new(&config);
    let record = insert_record(&mut records, "source", "key", "value", 0, 5, 1.0);
    let mut stm = MemoryCenters::new(config.stm.clone(), 5).expect("STM");
    let mut ltm = MemoryCenters::new(config.ltm.clone(), 6).expect("LTM");
    let stm_slot = activate(
        &mut stm,
        0,
        &unit(STM_KEY_DIM, 0),
        &unit(LTM_KEY_DIM, 5),
        record,
        2.0,
        1.0,
    );
    let mut support = Support::new(&config);
    support
        .set_support(stm_slot, &[record])
        .expect("STM support");

    let report =
        transfer(&mut stm, &mut ltm, &records, &mut support, &config).expect("canonical transfer");
    assert_eq!(report.transferred, 1);
    assert!((stm.intensity[0] - 0.4_f32.ln_1p()).abs() < 1e-6);
    assert!((ltm.intensity[0] - 1.6_f32.ln_1p()).abs() < 1e-6);
    stm = MemoryCenters::with_layout(config.stm.clone());
    let output = recall(
        &unit(STM_KEY_DIM, 0),
        &unit(LTM_KEY_DIM, 5),
        &unit(CONTEXT_DIM, 0),
        &stm,
        &ltm,
        0.0,
        &records,
        &support,
        &RecallPolicy::default(),
        4,
    )
    .expect("LTM-only recall");
    match output {
        RecallOutput::Candidates { candidates, .. } => {
            assert_eq!(candidates[0].record_id, record);
            assert_eq!(candidates[0].layer, RecallLayer::Ltm);
        }
        RecallOutput::Empty => panic!("canonical LTM key must survive without STM"),
    }
}

#[test]
fn consolidation_pours_a_real_blur_and_applies_fatigue_relief() {
    let config = config();
    let mut stm = MemoryCenters::new(config.stm.clone(), 81).expect("STM");
    let mut ltm = MemoryCenters::new(config.ltm.clone(), 82).expect("LTM");
    let mut stm_terrain = Terrain3D::new(
        config.terrain_resolution,
        config.stm.terrain_alpha_h,
        config.stm.terrain_alpha_e,
        config.stm.terrain_lambda,
    )
    .expect("STM terrain");
    let mut ltm_terrain = Terrain3D::new(
        config.terrain_resolution,
        config.ltm.terrain_alpha_h,
        config.ltm.terrain_alpha_e,
        config.ltm.terrain_lambda,
    )
    .expect("LTM terrain");
    let center = (2 * config.terrain_resolution + 2) * config.terrain_resolution + 2;
    let neighbor = (2 * config.terrain_resolution + 2) * config.terrain_resolution + 3;
    stm_terrain.h[center] = 1.0;
    let records = RecordTable::new(&config);
    let mut support = Support::new(&config);
    let mut scheduler = Scheduler {
        tick: LogicalTick(50),
        fatigue: 5.0,
        steps_since_consolidation: 42,
        last_maintenance: None,
    };

    let report = consolidate(
        &mut stm,
        &mut ltm,
        &mut stm_terrain,
        &mut ltm_terrain,
        &records,
        &mut support,
        &mut scheduler,
        &config,
    )
    .expect("consolidation");
    assert_eq!(report.transferred, 0);
    assert_eq!(report.fatigue_before, 5.0);
    assert_eq!(report.fatigue_after, 1.0);
    assert_eq!(scheduler.steps_since_consolidation, 0);
    assert!(ltm_terrain.h[center] > 0.0);
    assert!(ltm_terrain.h[neighbor] > 0.0);
}

#[test]
fn direct_ltm_and_u_stm_projections_are_not_aligned() {
    let projections = ProjectionBundle::generate(0x5eed);
    for sample in 0..20 {
        let raw: Vec<f32> = (0..384)
            .map(|index| ((index * 37 + sample * 101 + 17) as f32).sin())
            .collect();
        let norm = raw.iter().map(|value| value * value).sum::<f32>().sqrt();
        let embedding: Vec<f32> = raw.into_iter().map(|value| value / norm).collect();
        let direct = projections.project_to_ltm(&embedding).expect("direct LTM");
        let stm = projections.project_to_stm(&embedding).expect("STM");
        let via_u = projections.consolidate_key(&stm).expect("U STM");
        assert!(cosine(&direct, &via_u).expect("cosine") < 0.9);
    }
}

#[test]
fn failed_capacity_transfer_does_not_publish_partial_kernel_state() {
    let mut config = config();
    config.stm.n_centers = 2;
    config.ltm.n_centers = 1;
    let mut kernel = NcmKernel::new(7, config.clone()).expect("kernel");
    let occupied = insert_record(
        &mut kernel.records,
        "occupied",
        "occupied",
        "occupied",
        0,
        0,
        1.0,
    );
    let transfer_record = insert_record(
        &mut kernel.records,
        "transfer",
        "transfer",
        "transfer",
        1,
        1,
        1.0,
    );
    let ltm_slot = activate(
        &mut kernel.ltm,
        0,
        &unit(LTM_KEY_DIM, 0),
        &unit(LTM_KEY_DIM, 0),
        occupied,
        1.0,
        1.0,
    );
    let stm_slot = activate(
        &mut kernel.stm,
        1,
        &unit(STM_KEY_DIM, 1),
        &unit(LTM_KEY_DIM, 1),
        transfer_record,
        2.0,
        1.0,
    );
    kernel
        .support
        .set_support(ltm_slot, &[occupied])
        .expect("LTM support");
    kernel
        .support
        .set_support(stm_slot, &[transfer_record])
        .expect("STM support");
    let before = kernel.state_digest();

    assert_eq!(
        kernel.consolidate(),
        Err(CoreError::CapacityExhausted(Layer::Ltm))
    );
    assert_eq!(kernel.state_digest(), before);
}
