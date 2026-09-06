#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
//! Behavioral and executable-oracle tests for center writes and allocation.

use serde::Deserialize;
use tracedecay_memory_ncm_core::centers::MemoryCenters;
use tracedecay_memory_ncm_core::centers::write::{WriteInput, WriteOutcome, WriteParams};
use tracedecay_memory_ncm_core::types::{
    AFFECT_DIM, AffectVector, CONTEXT_DIM, CoreError, LTM_KEY_DIM, LayerConfig, NcmConfig,
    RecordId, TERRAIN_DIM, VALUE_DIM,
};

#[derive(Clone, Copy, Deserialize)]
struct Tolerance {
    atol: f32,
    rtol: f32,
}

#[derive(Deserialize)]
struct OracleBank {
    #[serde(rename = "K")]
    keys: Vec<Vec<f32>>,
    #[serde(rename = "K_context")]
    context: Vec<Vec<f32>>,
    #[serde(rename = "K_terrain")]
    terrain: Vec<Vec<f32>>,
    #[serde(rename = "V")]
    values: Vec<Vec<f32>>,
    h: Vec<f32>,
    e: Vec<Vec<f32>>,
    usage: Vec<u64>,
    age: Vec<u64>,
    active: Vec<bool>,
    n_centers: usize,
    d_key: usize,
    sigma_read: f32,
    sigma_write: f32,
    leak: f32,
    leak_emotion: f32,
    leak_value: f32,
    alpha_value: f32,
    alpha_emotion: f32,
    alpha_key: f32,
}

impl OracleBank {
    fn config(&self) -> LayerConfig {
        let mut config = NcmConfig::default().stm;
        config.n_centers = self.n_centers;
        config.d_key = self.d_key;
        config.sigma_read = self.sigma_read;
        config.sigma_write = self.sigma_write;
        config.leak = self.leak;
        config.leak_emotion = self.leak_emotion;
        config.leak_value = self.leak_value;
        config.alpha_value = self.alpha_value;
        config.alpha_emotion = self.alpha_emotion;
        config.alpha_key = self.alpha_key;
        config
    }

    fn centers(&self) -> MemoryCenters {
        let mut centers = MemoryCenters::with_layout(self.config());
        centers.keys = flatten(&self.keys);
        centers.context = flatten(&self.context);
        centers.terrain = flatten(&self.terrain);
        centers.values = flatten(&self.values);
        centers.intensity.clone_from(&self.h);
        centers.affect = flatten(&self.e);
        centers.usage.clone_from(&self.usage);
        centers.age.clone_from(&self.age);
        centers.active.clone_from(&self.active);
        for (incarnation, active) in centers.incarnation.iter_mut().zip(&centers.active) {
            *incarnation = u32::from(*active);
        }
        centers
    }
}

#[derive(Deserialize)]
struct OracleInputs {
    keys: Vec<Vec<f32>>,
    values: Vec<Vec<f32>>,
    emotions: Vec<Vec<f32>>,
    intensities: Vec<f32>,
    top_k: usize,
    new_center_threshold: f32,
    context_keys: Vec<Vec<f32>>,
    terrain_positions: Vec<Vec<f32>>,
    ages: Vec<u64>,
}

#[derive(Deserialize)]
struct OracleResult {
    index: Option<usize>,
    status: String,
}

#[derive(Deserialize)]
struct OracleOperation {
    inputs: OracleInputs,
    write_results: Vec<OracleResult>,
    state: OracleBank,
}

#[derive(Deserialize)]
struct WriteFixture {
    tolerance: Tolerance,
    initial: OracleBank,
    operations: Vec<OracleOperation>,
}

fn flatten(rows: &[Vec<f32>]) -> Vec<f32> {
    rows.iter().flatten().copied().collect()
}

fn array<const N: usize>(values: &[f32]) -> [f32; N] {
    values
        .try_into()
        .expect("fixture vector has expected width")
}

fn assert_close(actual: &[f32], expected: &[f32], tolerance: Tolerance, label: &str) {
    assert_eq!(actual.len(), expected.len(), "{label} length");
    for (index, (actual_value, expected_value)) in actual.iter().zip(expected).enumerate() {
        let limit = tolerance.atol + tolerance.rtol * expected_value.abs();
        assert!(
            (*actual_value - *expected_value).abs() <= limit,
            "{label}[{index}]: actual={actual_value} expected={expected_value} limit={limit}"
        );
    }
}

fn config(n_centers: usize) -> LayerConfig {
    let mut config = NcmConfig::default().stm;
    config.n_centers = n_centers;
    config.d_key = 2;
    config.top_k_write = n_centers.max(1);
    config
}

fn input<'a>(key: &'a [f32], record: u64, intensity: f32) -> WriteInput<'a> {
    let mut value = [0.0; VALUE_DIM];
    value[0] = record as f32;
    let mut ltm_key = [0.0; LTM_KEY_DIM];
    ltm_key[0] = key[0];
    WriteInput {
        key,
        ltm_key,
        value,
        affect: AffectVector([1.0, 1.1, 0.9, 1.2]),
        intensity,
        context: [record as f32; CONTEXT_DIM],
        terrain: [0.1, -0.2, 0.3],
        record: RecordId(record),
        age: record,
    }
}

fn params(top_k_write: usize, threshold: f32, support: usize) -> WriteParams {
    WriteParams {
        top_k_write,
        new_center_threshold: threshold,
        max_center_support: support,
    }
}

#[test]
fn executable_write_fixture_matches_created_reinforced_ignored_and_full_capacity() {
    let fixture: WriteFixture = serde_json::from_str(include_str!(
        "../../../product/ncm/reference/oracle/write.json"
    ))
    .expect("write fixture parses");
    let mut centers = fixture.initial.centers();

    for (operation_index, operation) in fixture.operations.iter().enumerate() {
        let inputs = &operation.inputs;
        let key = &inputs.keys[0];
        let outcome = centers
            .write(
                WriteInput {
                    key,
                    ltm_key: [operation_index as f32; LTM_KEY_DIM],
                    value: array(&inputs.values[0]),
                    affect: AffectVector(array(&inputs.emotions[0])),
                    intensity: inputs.intensities[0],
                    context: array(&inputs.context_keys[0]),
                    terrain: array(&inputs.terrain_positions[0]),
                    record: RecordId(operation_index as u64),
                    age: inputs.ages[0],
                },
                &params(inputs.top_k, inputs.new_center_threshold, 32),
            )
            .expect("fixture write succeeds");
        let expected = &operation.write_results[0];
        match (&outcome, expected.status.as_str()) {
            (WriteOutcome::Created { slot }, "created") => {
                assert_eq!(Some(slot.index as usize), expected.index);
            }
            (WriteOutcome::Reinforced { slot, .. }, "reinforced") => {
                assert_eq!(Some(slot.index as usize), expected.index);
            }
            (WriteOutcome::Ignored, "ignored_zero_intensity")
            | (WriteOutcome::CapacityExhausted, "capacity_exhausted") => {}
            _ => panic!(
                "operation {operation_index}: unexpected {outcome:?} for {}",
                expected.status
            ),
        }

        let expected_state = &operation.state;
        assert_close(
            &centers.keys,
            &flatten(&expected_state.keys),
            fixture.tolerance,
            "keys",
        );
        assert_close(
            &centers.values,
            &flatten(&expected_state.values),
            fixture.tolerance,
            "values",
        );
        assert_close(
            &centers.intensity,
            &expected_state.h,
            fixture.tolerance,
            "intensity",
        );
        assert_close(
            &centers.affect,
            &flatten(&expected_state.e),
            fixture.tolerance,
            "affect",
        );
        assert_close(
            &centers.context,
            &flatten(&expected_state.context),
            fixture.tolerance,
            "context",
        );
        assert_close(
            &centers.terrain,
            &flatten(&expected_state.terrain),
            fixture.tolerance,
            "terrain",
        );
        assert_eq!(centers.usage, expected_state.usage);
        assert_eq!(centers.age, expected_state.age);
        assert_eq!(centers.active, expected_state.active);
    }
}

#[test]
fn capacity_exhaustion_leaves_state_byte_identical() {
    let mut centers = MemoryCenters::new(config(1), 7).expect("bank initializes");
    centers
        .write(input(&[1.0, 0.0], 1, 1.0), &params(1, 0.5, 4))
        .expect("first write succeeds");
    let before = serde_json::to_vec(&centers).expect("state serializes");
    let outcome = centers
        .write(input(&[0.0, 1.0], 2, 1.0), &params(1, 0.5, 4))
        .expect("capacity is an explicit effect");
    let after = serde_json::to_vec(&centers).expect("state serializes");
    assert_eq!(outcome, WriteOutcome::CapacityExhausted);
    assert_eq!(before, after);
}

#[test]
fn slot_reuse_increments_incarnation_and_rejects_stale_handle() {
    let mut centers = MemoryCenters::new(config(1), 11).expect("bank initializes");
    let first = match centers
        .write(input(&[1.0, 0.0], 1, 1.0), &params(1, 0.5, 4))
        .expect("first write succeeds")
    {
        WriteOutcome::Created { slot } => slot,
        other => panic!("expected creation, got {other:?}"),
    };
    centers
        .deactivate_slot(first)
        .expect("deactivation succeeds");
    assert!(centers.keys.iter().all(|value| *value == 0.0));
    assert!(centers.affect.iter().all(|value| *value == 0.0));
    assert!(centers.support[0].is_empty());
    assert_eq!(centers.resolve(first), Err(CoreError::StaleHandle));

    let second = match centers
        .write(input(&[0.0, 1.0], 2, 1.0), &params(1, 0.5, 4))
        .expect("second write succeeds")
    {
        WriteOutcome::Created { slot } => slot,
        other => panic!("expected creation, got {other:?}"),
    };
    assert_eq!(second.index, first.index);
    assert_eq!(second.incarnation, first.incarnation + 1);
    assert_eq!(centers.resolve(first), Err(CoreError::StaleHandle));
    assert_eq!(centers.resolve(second), Ok(0));
    assert_eq!(centers.support[0], vec![RecordId(2)]);
}

#[test]
fn every_updated_center_tracks_bounded_recency_support_and_reports_truncation() {
    let mut centers = MemoryCenters::new(config(2), 13).expect("bank initializes");
    let create = params(2, 0.5, 2);
    centers
        .write(input(&[1.0, 0.0], 1, 1.0), &create)
        .expect("first center created");
    centers
        .write(input(&[0.0, 1.0], 2, 1.0), &create)
        .expect("second center created");

    let diagonal = std::f32::consts::FRAC_1_SQRT_2;
    let reinforce = params(2, 0.0, 2);
    centers
        .write(input(&[diagonal, diagonal], 3, 1.0), &reinforce)
        .expect("both centers reinforced");
    let outcome = centers
        .write(input(&[diagonal, diagonal], 4, 1.0), &reinforce)
        .expect("bounded support reinforced");
    let (updated, truncated) = match outcome {
        WriteOutcome::Reinforced {
            updated,
            support_truncated,
            ..
        } => (updated, support_truncated),
        other => panic!("expected reinforcement, got {other:?}"),
    };
    assert_eq!(updated.len(), 2);
    assert_eq!(truncated, updated);
    assert_eq!(centers.support[0], vec![RecordId(3), RecordId(4)]);
    assert_eq!(centers.support[1], vec![RecordId(3), RecordId(4)]);
}

#[test]
fn d02_write_candidates_use_sigma_read_not_sigma_write() {
    let mut layer = config(1);
    layer.sigma_read = 0.4;
    layer.sigma_write = 0.2;
    let mut centers = MemoryCenters::new(layer, 17).expect("bank initializes");
    centers
        .write(input(&[1.0, 0.0], 1, 1.0), &params(1, 0.5, 4))
        .expect("initial center created");

    let query = [0.92, 0.391_918_36];
    let read_width = centers
        .compute_rbf_weights(&query, 1, false, Some(0.4))
        .expect("read-width comparison succeeds")
        .weights[0];
    let write_width_mutant = centers
        .compute_rbf_weights(&query, 1, false, Some(0.2))
        .expect("write-width comparison succeeds")
        .weights[0];
    assert!(read_width > 0.5);
    assert!(write_width_mutant < 0.5);

    let outcome = centers
        .write(input(&query, 2, 1.0), &params(1, 0.5, 4))
        .expect("D02 write succeeds");
    assert!(matches!(outcome, WriteOutcome::Reinforced { .. }));
    assert_eq!(centers.n_active(), 1);
}

#[test]
fn explicit_record_path_reinforces_only_matching_center() {
    let mut centers = MemoryCenters::new(config(2), 19).expect("bank initializes");
    let create = params(2, 0.5, 4);
    centers
        .write(input(&[1.0, 0.0], 10, 1.0), &create)
        .expect("first center created");
    centers
        .write(input(&[0.0, 1.0], 20, 1.0), &create)
        .expect("second center created");
    let before_second = centers.intensity[1];

    let outcome = centers
        .write_to_record(
            RecordId(10),
            input(&[0.0, 1.0], 10, 2.0),
            &params(2, 0.5, 4),
        )
        .expect("record-targeted reinforcement succeeds");
    match outcome {
        WriteOutcome::Reinforced { slot, updated, .. } => {
            assert_eq!(slot.index, 0);
            assert_eq!(updated, vec![slot]);
        }
        other => panic!("expected reinforcement, got {other:?}"),
    }
    assert_eq!(centers.intensity[1], before_second);
}

#[test]
fn malformed_and_nonfinite_inputs_fail_before_mutation() {
    let mut centers = MemoryCenters::new(config(2), 23).expect("bank initializes");
    let before = serde_json::to_vec(&centers).expect("state serializes");
    let short = centers.write(input(&[1.0], 1, 1.0), &params(1, 0.5, 4));
    assert!(matches!(short, Err(CoreError::DimensionMismatch { .. })));
    assert_eq!(
        before,
        serde_json::to_vec(&centers).expect("state serializes")
    );

    let nonfinite = centers.write(input(&[f32::NAN, 0.0], 2, 1.0), &params(1, 0.5, 4));
    assert_eq!(nonfinite, Err(CoreError::NonFinite("center write key")));
    assert_eq!(
        before,
        serde_json::to_vec(&centers).expect("state serializes")
    );

    let zero_support = centers.write(input(&[1.0, 0.0], 3, 1.0), &params(1, 0.5, 0));
    assert_eq!(
        zero_support,
        Err(CoreError::BudgetExceeded("center support"))
    );
    assert_eq!(
        before,
        serde_json::to_vec(&centers).expect("state serializes")
    );
}

#[test]
fn deterministic_constructor_produces_unit_keys_and_zero_strength_is_ignored() {
    let first = MemoryCenters::new(config(3), 29).expect("bank initializes");
    let second = MemoryCenters::new(config(3), 29).expect("bank initializes");
    assert_eq!(first.keys, second.keys);
    for key in first.keys.chunks_exact(2) {
        let norm = (key[0] * key[0] + key[1] * key[1]).sqrt();
        assert!((norm - 1.0).abs() < 1e-6);
    }

    let mut centers = first;
    let before = serde_json::to_vec(&centers).expect("state serializes");
    let outcome = centers
        .write(input(&[1.0, 0.0], 1, 0.5e-6), &params(1, 0.5, 4))
        .expect("zero-strength write is explicit");
    assert_eq!(outcome, WriteOutcome::Ignored);
    assert_eq!(
        before,
        serde_json::to_vec(&centers).expect("state serializes")
    );
}

#[test]
fn metadata_moves_only_to_argmax_while_numeric_updates_reach_top_k() {
    let mut centers = MemoryCenters::new(config(2), 31).expect("bank initializes");
    let create = params(2, 0.5, 4);
    centers
        .write(input(&[1.0, 0.0], 1, 1.0), &create)
        .expect("first center created");
    centers
        .write(input(&[0.0, 1.0], 2, 1.0), &create)
        .expect("second center created");
    let before = centers.intensity.clone();
    let query = [0.8, 0.6];
    centers
        .write(input(&query, 9, 1.0), &params(2, 0.0, 4))
        .expect("top-k reinforcement succeeds");
    assert!(centers.intensity[0] > before[0]);
    assert!(centers.intensity[1] > before[1]);
    assert_eq!(centers.record[0], Some(RecordId(9)));
    assert_eq!(centers.record[1], Some(RecordId(2)));
    assert_eq!(centers.context[0], 9.0);
    assert_eq!(centers.context[CONTEXT_DIM], 2.0);
    assert_eq!(centers.support[0], vec![RecordId(1), RecordId(9)]);
    assert_eq!(centers.support[1], vec![RecordId(2), RecordId(9)]);
    assert_eq!(centers.affect.len(), 2 * AFFECT_DIM);
    assert_eq!(centers.terrain.len(), 2 * TERRAIN_DIM);
}
