#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
//! Behavioral and executable-oracle tests for immutable center reads.

use serde::Deserialize;
use tracedecay_memory_ncm_core::centers::MemoryCenters;
use tracedecay_memory_ncm_core::centers::read::{CompoundWeights, ReadParams};
use tracedecay_memory_ncm_core::types::{
    AFFECT_DIM, CONTEXT_DIM, LayerConfig, NcmConfig, RecordId, TERRAIN_DIM, VALUE_DIM,
};

#[derive(Clone, Copy, Deserialize)]
struct Tolerance {
    atol: f32,
    rtol: f32,
}

#[derive(Clone, Deserialize)]
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
    d_value: usize,
    d_emotion: usize,
    sigma_read: f32,
    sigma_write: f32,
    leak: f32,
    leak_emotion: f32,
    leak_value: f32,
    alpha_value: f32,
    alpha_emotion: f32,
    alpha_key: f32,
    use_hybrid_metric: bool,
    minkowski_p: f32,
    weight_cosine: f32,
    weight_minkowski: f32,
    hybrid_candidates: usize,
}

impl OracleBank {
    fn config(&self) -> LayerConfig {
        assert_eq!(self.d_value, VALUE_DIM);
        assert_eq!(self.d_emotion, AFFECT_DIM);
        let mut config = NcmConfig::default().ltm;
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
        centers.values = flatten(&self.values);
        centers.intensity.clone_from(&self.h);
        centers.affect = flatten(&self.e);
        centers.usage.clone_from(&self.usage);
        centers.age.clone_from(&self.age);
        centers.active.clone_from(&self.active);
        centers.context = flatten(&self.context);
        centers.terrain = flatten(&self.terrain);
        for (incarnation, active) in centers.incarnation.iter_mut().zip(&centers.active) {
            *incarnation = u32::from(*active);
        }
        centers
    }

    fn read_params(&self, top_k: usize) -> ReadParams {
        ReadParams {
            top_k,
            use_hybrid_metric: self.use_hybrid_metric,
            minkowski_p: self.minkowski_p,
            weight_cosine: self.weight_cosine,
            weight_minkowski: self.weight_minkowski,
            hybrid_candidates: self.hybrid_candidates,
        }
    }
}

#[derive(Deserialize)]
struct FixtureSelection {
    weights: Vec<Vec<Vec<f32>>>,
    indices: Vec<Vec<Vec<usize>>>,
}

#[derive(Deserialize)]
struct FixtureRead {
    #[serde(rename = "r_V")]
    r_v: Vec<Vec<Vec<f32>>>,
    #[serde(rename = "r_E")]
    r_e: Vec<Vec<Vec<f32>>>,
    weights: Vec<Vec<Vec<f32>>>,
    indices: Vec<Vec<Vec<usize>>>,
}

#[derive(Deserialize)]
struct RbfCase {
    name: String,
    bank: OracleBank,
    queries: Vec<Vec<Vec<f32>>>,
    sigma: f32,
    top_k: usize,
    hybrid_active: bool,
    unnormalized: FixtureSelection,
    normalized: FixtureSelection,
    read: FixtureRead,
}

#[derive(Deserialize)]
struct RbfTies {
    bank: OracleBank,
    queries: Vec<Vec<Vec<f32>>>,
    top_k: usize,
    reference: FixtureRead,
    v1_expected_indices: Vec<Vec<Vec<usize>>>,
}

#[derive(Deserialize)]
struct RbfFixture {
    tolerance: Tolerance,
    cases: Vec<RbfCase>,
    ties: RbfTies,
}

#[derive(Deserialize)]
struct CompoundCase {
    queries: Vec<Vec<Vec<f32>>>,
    context_queries: Option<Vec<Vec<Vec<f32>>>>,
    terrain_queries: Option<Vec<Vec<Vec<f32>>>>,
    top_k: usize,
    semantic_part: Vec<Vec<Vec<f32>>>,
    context_part: Vec<Vec<Vec<f32>>>,
    terrain_part: Vec<Vec<Vec<f32>>>,
    combined_score: Vec<Vec<Vec<f32>>>,
    combined_weights: Vec<Vec<Vec<f32>>>,
    #[serde(rename = "r_V")]
    r_v: Vec<Vec<Vec<f32>>>,
    #[serde(rename = "r_E")]
    r_e: Vec<Vec<Vec<f32>>>,
    weights: Vec<Vec<Vec<f32>>>,
    indices: Vec<Vec<Vec<usize>>>,
}

#[derive(Deserialize)]
struct CompoundFixture {
    tolerance: Tolerance,
    bank: OracleBank,
    cases: Vec<CompoundCase>,
}

fn flatten(rows: &[Vec<f32>]) -> Vec<f32> {
    rows.iter().flatten().copied().collect()
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

fn indices(selection: &tracedecay_memory_ncm_core::centers::read::RbfSelection) -> Vec<usize> {
    selection
        .centers
        .iter()
        .map(|center| center.index)
        .collect()
}

fn small_bank(n_centers: usize, d_key: usize) -> MemoryCenters {
    let mut config = NcmConfig::default().ltm;
    config.n_centers = n_centers;
    config.d_key = d_key;
    config.sigma_read = 0.5;
    MemoryCenters::with_layout(config)
}

fn activate(
    centers: &mut MemoryCenters,
    index: usize,
    key: &[f32],
    context: &[f32; CONTEXT_DIM],
    terrain: &[f32; TERRAIN_DIM],
    value: f32,
    intensity: f32,
) {
    let d_key = centers.config.d_key;
    centers.keys[index * d_key..(index + 1) * d_key].copy_from_slice(key);
    centers.context[index * CONTEXT_DIM..(index + 1) * CONTEXT_DIM].copy_from_slice(context);
    centers.terrain[index * TERRAIN_DIM..(index + 1) * TERRAIN_DIM].copy_from_slice(terrain);
    centers.values[index * VALUE_DIM] = value;
    centers.intensity[index] = intensity;
    centers.active[index] = true;
    centers.incarnation[index] = 1;
}

#[test]
fn empty_and_singleton_reads_expose_relevance_not_certainty() {
    let mut centers = small_bank(2, 2);
    let empty = centers
        .read(&[0.0, 1.0], ReadParams::default())
        .expect("empty read should succeed");
    assert!(empty.selection.centers.is_empty());
    assert_eq!(empty.r_v, [0.0; VALUE_DIM]);
    assert_eq!(empty.r_e, [0.0; AFFECT_DIM]);

    activate(
        &mut centers,
        0,
        &[1.0, 0.0],
        &[0.0; CONTEXT_DIM],
        &[0.0; TERRAIN_DIM],
        7.0,
        2.0,
    );
    centers.record[0] = Some(RecordId(41));
    centers.support[0] = vec![RecordId(40), RecordId(41)];
    let singleton = centers
        .read(
            &[0.0, 1.0],
            ReadParams {
                top_k: 1,
                ..ReadParams::default()
            },
        )
        .expect("singleton read should succeed");
    let trace = &singleton.selection.centers[0];
    assert_eq!(trace.normalized_weight, 1.0);
    assert!(trace.raw_rbf_weight < 0.1);
    assert_eq!(trace.slot.index, 0);
    assert_eq!(trace.slot.incarnation, 1);
    assert_eq!(trace.record, Some(RecordId(41)));
    assert_eq!(trace.support, vec![RecordId(40), RecordId(41)]);
    assert_eq!(singleton.r_v[0], 7.0);
}

#[test]
fn intensity_changes_normalized_weights_but_not_raw_rbf() {
    let mut centers = small_bank(2, 2);
    for index in 0..2 {
        activate(
            &mut centers,
            index,
            &[1.0, 0.0],
            &[0.0; CONTEXT_DIM],
            &[0.0; TERRAIN_DIM],
            index as f32,
            1.0,
        );
    }
    let before = centers
        .compute_rbf_weights(&[1.0, 0.0], 2, false, None)
        .expect("first selection should succeed");
    centers.intensity[1] = 9.0;
    let after = centers
        .compute_rbf_weights(&[1.0, 0.0], 2, false, None)
        .expect("second selection should succeed");

    assert_eq!(before.weights, after.weights);
    assert_eq!(
        before.centers[0].raw_rbf_weight,
        after.centers[0].raw_rbf_weight
    );
    assert_eq!(
        before.centers[1].raw_rbf_weight,
        after.centers[1].raw_rbf_weight
    );
    assert!(after.centers[1].normalized_weight > before.centers[1].normalized_weight);
    assert!(after.centers[0].normalized_weight < before.centers[0].normalized_weight);
}

#[test]
fn compound_context_and_terrain_perturb_selection_and_none_is_zero() {
    let mut centers = small_bank(2, 2);
    let mut context_positive = [0.0; CONTEXT_DIM];
    let mut context_negative = [0.0; CONTEXT_DIM];
    context_positive[0] = 1.0;
    context_negative[0] = -1.0;
    let terrain_positive = [1.0, 0.0, 0.0];
    let terrain_negative = [-1.0, 0.0, 0.0];
    activate(
        &mut centers,
        0,
        &[1.0, 0.0],
        &context_positive,
        &terrain_positive,
        1.0,
        1.0,
    );
    activate(
        &mut centers,
        1,
        &[1.0, 0.0],
        &context_negative,
        &terrain_negative,
        2.0,
        1.0,
    );

    let context_forward = centers
        .read_compound(
            &[1.0, 0.0],
            Some(&context_positive),
            None,
            CompoundWeights::default(),
            1,
        )
        .expect("positive context read should succeed");
    let context_reverse = centers
        .read_compound(
            &[1.0, 0.0],
            Some(&context_negative),
            None,
            CompoundWeights::default(),
            1,
        )
        .expect("negative context read should succeed");
    assert_eq!(indices(&context_forward.selection), vec![0]);
    assert_eq!(indices(&context_reverse.selection), vec![1]);

    let terrain_forward = centers
        .read_compound(
            &[1.0, 0.0],
            None,
            Some(&terrain_positive),
            CompoundWeights::default(),
            1,
        )
        .expect("positive terrain read should succeed");
    let terrain_reverse = centers
        .read_compound(
            &[1.0, 0.0],
            None,
            Some(&terrain_negative),
            CompoundWeights::default(),
            1,
        )
        .expect("negative terrain read should succeed");
    assert_eq!(indices(&terrain_forward.selection), vec![0]);
    assert_eq!(indices(&terrain_reverse.selection), vec![1]);

    let terrain_none = centers
        .read_compound(
            &[1.0, 0.0],
            Some(&context_positive),
            None,
            CompoundWeights::default(),
            2,
        )
        .expect("missing terrain read should succeed");
    let terrain_zeros = centers
        .read_compound(
            &[1.0, 0.0],
            Some(&context_positive),
            Some(&[0.0; TERRAIN_DIM]),
            CompoundWeights::default(),
            2,
        )
        .expect("zero terrain read should succeed");
    assert_eq!(terrain_none, terrain_zeros);
    assert!(
        terrain_none
            .selection
            .centers
            .iter()
            .all(|trace| trace.terrain_cosine == 0.0)
    );
}

#[test]
fn repeated_reads_leave_serialized_state_unchanged() {
    let mut centers = small_bank(2, 2);
    activate(
        &mut centers,
        0,
        &[1.0, 0.0],
        &[0.0; CONTEXT_DIM],
        &[0.0; TERRAIN_DIM],
        3.0,
        1.0,
    );
    let before = serde_json::to_vec(&centers).expect("state should serialize");
    for _ in 0..5 {
        let _result = centers
            .read(&[1.0, 0.0], ReadParams::default())
            .expect("plain read should succeed");
        let _compound = centers
            .read_compound(&[1.0, 0.0], None, None, CompoundWeights::default(), 1)
            .expect("compound read should succeed");
    }
    let after = serde_json::to_vec(&centers).expect("state should serialize");
    assert_eq!(before, after);
    assert_eq!(centers.usage[0], 0);
    assert_eq!(centers.total_step, 0);
}

#[test]
fn exact_ties_resolve_by_ascending_center_index() {
    let mut centers = small_bank(4, 2);
    for index in 0..4 {
        activate(
            &mut centers,
            index,
            &[1.0, 0.0],
            &[0.0; CONTEXT_DIM],
            &[0.0; TERRAIN_DIM],
            index as f32,
            1.0,
        );
    }
    let result = centers
        .read(
            &[1.0, 0.0],
            ReadParams {
                top_k: 3,
                ..ReadParams::default()
            },
        )
        .expect("tie read should succeed");
    assert_eq!(indices(&result.selection), vec![0, 1, 2]);
}

#[test]
fn rbf_oracle_matches_empty_five_boundary_distant_and_intensity_cases() {
    let fixture: RbfFixture = serde_json::from_str(include_str!(
        "../../../product/ncm/reference/oracle/rbf_read.json"
    ))
    .expect("RBF fixture should parse");

    for case in &fixture.cases {
        let centers = case.bank.centers();
        for (query_index, query_steps) in case.queries.iter().enumerate() {
            let query = &query_steps[0];
            let raw = centers
                .compute_rbf_weights(query, case.top_k, false, Some(case.sigma))
                .expect("raw oracle selection should succeed");
            assert_eq!(raw.hybrid_applied, case.hybrid_active, "{}", case.name);
            assert_eq!(
                indices(&raw),
                case.unnormalized.indices[query_index][0],
                "{} raw indices",
                case.name
            );
            assert_close(
                &raw.weights,
                &case.unnormalized.weights[query_index][0],
                fixture.tolerance,
                &format!("{} raw weights", case.name),
            );

            let normalized = centers
                .compute_rbf_weights(query, case.top_k, true, Some(case.sigma))
                .expect("normalized oracle selection should succeed");
            assert_eq!(
                indices(&normalized),
                case.normalized.indices[query_index][0],
                "{} normalized indices",
                case.name
            );
            assert_close(
                &normalized.weights,
                &case.normalized.weights[query_index][0],
                fixture.tolerance,
                &format!("{} normalized weights", case.name),
            );

            let read = centers
                .read(query, case.bank.read_params(case.top_k))
                .expect("oracle read should succeed");
            assert_eq!(
                indices(&read.selection),
                case.read.indices[query_index][0],
                "{} read indices",
                case.name
            );
            assert_close(
                &read.selection.weights,
                &case.read.weights[query_index][0],
                fixture.tolerance,
                &format!("{} read weights", case.name),
            );
            assert_close(
                &read.r_v,
                &case.read.r_v[query_index][0],
                fixture.tolerance,
                &format!("{} r_V", case.name),
            );
            assert_close(
                &read.r_e,
                &case.read.r_e[query_index][0],
                fixture.tolerance,
                &format!("{} r_E", case.name),
            );
        }
    }

    let boundary_64 = fixture
        .cases
        .iter()
        .find(|case| case.name == "64")
        .expect("64-center case should exist");
    let boundary_65 = fixture
        .cases
        .iter()
        .find(|case| case.name == "65")
        .expect("65-center case should exist");
    assert!(!boundary_64.hybrid_active);
    assert!(boundary_65.hybrid_active);

    let ties = &fixture.ties;
    let tie_centers = ties.bank.centers();
    let tie_read = tie_centers
        .read(&ties.queries[0][0], ties.bank.read_params(ties.top_k))
        .expect("tie fixture read should succeed");
    assert_eq!(indices(&tie_read.selection), ties.v1_expected_indices[0][0]);
    assert_close(
        &tie_read.selection.weights,
        &ties.reference.weights[0][0],
        fixture.tolerance,
        "tie normalized weights",
    );
    assert_close(
        &tie_read.r_v,
        &ties.reference.r_v[0][0],
        fixture.tolerance,
        "tie r_V",
    );
    assert_close(
        &tie_read.r_e,
        &ties.reference.r_e[0][0],
        fixture.tolerance,
        "tie r_E",
    );
}

#[test]
fn compound_oracle_matches_components_selection_and_readouts() {
    let fixture: CompoundFixture = serde_json::from_str(include_str!(
        "../../../product/ncm/reference/oracle/compound_read.json"
    ))
    .expect("compound fixture should parse");
    let centers = fixture.bank.centers();
    let active_indices: Vec<usize> = centers
        .active
        .iter()
        .enumerate()
        .filter_map(|(index, active)| active.then_some(index))
        .collect();

    for case in &fixture.cases {
        for (query_index, query_steps) in case.queries.iter().enumerate() {
            let context = case
                .context_queries
                .as_ref()
                .map(|queries| queries[query_index][0].as_slice());
            let terrain = case
                .terrain_queries
                .as_ref()
                .map(|queries| queries[query_index][0].as_slice());
            let result = centers
                .read_compound(
                    &query_steps[0],
                    context,
                    terrain,
                    CompoundWeights::default(),
                    case.top_k,
                )
                .expect("compound oracle read should succeed");

            assert_eq!(indices(&result.selection), case.indices[query_index][0]);
            assert_close(
                &result.selection.weights,
                &case.weights[query_index][0],
                fixture.tolerance,
                "compound normalized weights",
            );
            assert_close(
                &result.r_v,
                &case.r_v[query_index][0],
                fixture.tolerance,
                "compound r_V",
            );
            assert_close(
                &result.r_e,
                &case.r_e[query_index][0],
                fixture.tolerance,
                "compound r_E",
            );

            for trace in &result.selection.centers {
                let local = active_indices
                    .iter()
                    .position(|index| *index == trace.index)
                    .expect("selected center should be active");
                assert_close(
                    &[trace.cosine],
                    &[case.semantic_part[query_index][0][local]],
                    fixture.tolerance,
                    "semantic component",
                );
                assert_close(
                    &[trace.context_cosine],
                    &[case.context_part[query_index][0][local]],
                    fixture.tolerance,
                    "context component",
                );
                assert_close(
                    &[trace.terrain_cosine],
                    &[case.terrain_part[query_index][0][local]],
                    fixture.tolerance,
                    "terrain component",
                );
                assert_close(
                    &[trace.combined_score],
                    &[case.combined_score[query_index][0][local]],
                    fixture.tolerance,
                    "combined score",
                );
                assert_close(
                    &[trace.raw_rbf_weight],
                    &[case.combined_weights[query_index][0][local]],
                    fixture.tolerance,
                    "compound raw RBF",
                );
            }
        }
    }
}
