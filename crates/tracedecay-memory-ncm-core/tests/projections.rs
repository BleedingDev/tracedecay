//! Integration tests for persisted NCM projection bundles.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use serde::Deserialize;
use tracedecay_memory_ncm_core::CoreError;
use tracedecay_memory_ncm_core::projections::{
    ProjectionBundle, ProjectionMatrices, ProjectionMatrix,
};
use tracedecay_memory_ncm_core::types::{
    CONTEXT_DIM, EMBEDDING_DIM, LTM_KEY_DIM, STM_KEY_DIM, TERRAIN_DIM, VALUE_DIM,
};

const ORACLE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../product/ncm/reference/oracle/projections.json"
));

#[derive(Debug, Deserialize)]
struct Tolerance {
    atol: f32,
    rtol: f32,
}

#[derive(Debug, Deserialize)]
struct OracleParameter {
    weight: Vec<Vec<f32>>,
    bias: Option<Vec<f32>>,
}

#[derive(Debug, Deserialize)]
struct OracleParameters {
    to_ltm_key: OracleParameter,
    to_stm_key: OracleParameter,
    to_value: OracleParameter,
    to_context: OracleParameter,
    ltm_to_terrain: OracleParameter,
    stm_to_terrain: OracleParameter,
    stm_to_ltm: OracleParameter,
}

#[derive(Debug, Deserialize)]
struct OracleOutputs {
    to_ltm_key: Vec<Vec<f32>>,
    to_stm_key: Vec<Vec<f32>>,
    to_value: Vec<Vec<f32>>,
    to_context: Vec<Vec<f32>>,
    ltm_to_3d: Vec<Vec<f32>>,
    stm_to_3d: Vec<Vec<f32>>,
    stm_to_ltm: Vec<Vec<f32>>,
}

#[derive(Debug, Deserialize)]
struct ProjectionFixture {
    tolerance: Tolerance,
    input_embeddings: Vec<Vec<f32>>,
    parameters: OracleParameters,
    outputs: OracleOutputs,
}

fn from_oracle(parameter: &OracleParameter) -> ProjectionMatrix {
    let cols = parameter.weight.first().map_or(0, Vec::len);
    assert!(parameter.weight.iter().all(|row| row.len() == cols));
    ProjectionMatrix {
        rows: parameter.weight.len(),
        cols,
        weights: parameter.weight.iter().flatten().copied().collect(),
        bias: parameter.bias.clone(),
    }
}

fn matrices_from_oracle(parameters: OracleParameters) -> ProjectionMatrices {
    ProjectionMatrices {
        to_ltm_key: from_oracle(&parameters.to_ltm_key),
        to_stm_key: from_oracle(&parameters.to_stm_key),
        to_value: from_oracle(&parameters.to_value),
        to_context: from_oracle(&parameters.to_context),
        ltm_to_terrain: from_oracle(&parameters.ltm_to_terrain),
        stm_to_terrain: from_oracle(&parameters.stm_to_terrain),
        stm_to_ltm: from_oracle(&parameters.stm_to_ltm),
    }
}

fn assert_close(actual: &[f32], expected: &[f32], tolerance: &Tolerance) {
    assert_eq!(actual.len(), expected.len());
    for (index, (actual_value, expected_value)) in actual.iter().zip(expected).enumerate() {
        let limit = tolerance.atol + tolerance.rtol * expected_value.abs();
        let error = (actual_value - expected_value).abs();
        assert!(
            error <= limit,
            "index {index}: actual {actual_value} expected {expected_value}, error {error}, limit {limit}"
        );
    }
}

fn gram_error_rows(matrix: &ProjectionMatrix) -> f32 {
    let mut maximum = 0.0_f32;
    for left in 0..matrix.rows {
        for right in 0..matrix.rows {
            let mut product = 0.0_f32;
            for column in 0..matrix.cols {
                product += matrix.weights[left * matrix.cols + column]
                    * matrix.weights[right * matrix.cols + column];
            }
            let expected = if left == right { 1.0 } else { 0.0 };
            maximum = maximum.max((product - expected).abs());
        }
    }
    maximum
}

fn gram_error_columns(matrix: &ProjectionMatrix) -> f32 {
    let mut maximum = 0.0_f32;
    for left in 0..matrix.cols {
        for right in 0..matrix.cols {
            let mut product = 0.0_f32;
            for row in 0..matrix.rows {
                product += matrix.weights[row * matrix.cols + left]
                    * matrix.weights[row * matrix.cols + right];
            }
            let expected = if left == right { 1.0 } else { 0.0 };
            maximum = maximum.max((product - expected).abs());
        }
    }
    maximum
}

#[test]
fn generated_bundle_is_deterministic_and_has_the_frozen_shapes() {
    let first = ProjectionBundle::generate(42);
    let second = ProjectionBundle::generate(42);
    let changed = ProjectionBundle::generate(43);
    assert_eq!(first, second);
    assert_ne!(first.identity(), changed.identity());

    let matrices = first.matrices();
    assert_eq!(
        (matrices.to_ltm_key.rows, matrices.to_ltm_key.cols),
        (64, 384)
    );
    assert_eq!(
        (matrices.to_stm_key.rows, matrices.to_stm_key.cols),
        (16, 384)
    );
    assert_eq!((matrices.to_value.rows, matrices.to_value.cols), (128, 384));
    assert_eq!(
        (matrices.to_context.rows, matrices.to_context.cols),
        (16, 384)
    );
    assert_eq!(
        (matrices.ltm_to_terrain.rows, matrices.ltm_to_terrain.cols),
        (3, 64)
    );
    assert_eq!(
        (matrices.stm_to_terrain.rows, matrices.stm_to_terrain.cols),
        (3, 16)
    );
    assert_eq!(
        (matrices.stm_to_ltm.rows, matrices.stm_to_ltm.cols),
        (64, 16)
    );
    assert!(matrices.to_ltm_key.bias.is_some());
    assert!(matrices.to_stm_key.bias.is_some());
    assert!(matrices.to_value.bias.is_some());
    assert!(matrices.to_context.bias.is_none());
    assert!(matrices.ltm_to_terrain.bias.is_some());
    assert!(matrices.stm_to_terrain.bias.is_some());
    assert!(matrices.stm_to_ltm.bias.is_none());
}

#[test]
fn generated_orthogonal_families_are_orthonormal() {
    let bundle = ProjectionBundle::generate(200);
    let matrices = bundle.matrices();
    assert!(gram_error_rows(&matrices.to_context) < 1.0e-4);
    assert!(gram_error_columns(&matrices.stm_to_ltm) < 1.0e-4);
}

#[test]
fn generated_forward_outputs_have_expected_ranges_and_dimensions() {
    let bundle = ProjectionBundle::generate(7);
    let input = vec![0.25_f32; EMBEDDING_DIM];
    let ltm = bundle.project_to_ltm(&input).unwrap();
    let stm = bundle.project_to_stm(&input).unwrap();
    let value = bundle.project_to_value(&input).unwrap();
    let context = bundle.project_to_context(&input).unwrap();
    assert_eq!(ltm.len(), LTM_KEY_DIM);
    assert_eq!(stm.len(), STM_KEY_DIM);
    assert_eq!(value.len(), VALUE_DIM);
    assert_eq!(context.len(), CONTEXT_DIM);
    assert_eq!(bundle.ltm_to_3d(&ltm).unwrap().len(), TERRAIN_DIM);
    assert_eq!(bundle.stm_to_3d(&stm).unwrap().len(), TERRAIN_DIM);
    assert_eq!(bundle.consolidate_key(&stm).unwrap().len(), LTM_KEY_DIM);
    assert!(
        bundle
            .ltm_to_3d(&ltm)
            .unwrap()
            .iter()
            .all(|coordinate| (-1.0..=1.0).contains(coordinate))
    );
    assert!(
        bundle
            .stm_to_3d(&stm)
            .unwrap()
            .iter()
            .all(|coordinate| (-1.0..=1.0).contains(coordinate))
    );
}

#[test]
fn malformed_matrices_fail_before_a_bundle_is_constructed() {
    let valid = ProjectionBundle::generate(12);
    let identity = valid.identity();

    let mut nonfinite = valid.matrices().clone();
    nonfinite.to_ltm_key.weights[0] = f32::NAN;
    assert!(matches!(
        ProjectionBundle::from_matrices(nonfinite),
        Err(CoreError::NonFinite("matrix weights"))
    ));

    let mut wrong_shape = valid.matrices().clone();
    wrong_shape.to_stm_key.rows = 17;
    assert!(matches!(
        ProjectionBundle::from_matrices(wrong_shape),
        Err(CoreError::DimensionMismatch {
            what: "matrix rows",
            expected: 16,
            actual: 17
        })
    ));

    let mut wrong_bias = valid.matrices().clone();
    wrong_bias.to_context.bias = Some(vec![0.0; CONTEXT_DIM]);
    assert!(matches!(
        ProjectionBundle::from_matrices(wrong_bias),
        Err(CoreError::InvalidState(_))
    ));

    let mut serialized = serde_json::to_value(&valid).unwrap();
    serialized["to_context"]["bias"] = serde_json::json!(vec![0.0_f32; CONTEXT_DIM]);
    assert!(serde_json::from_value::<ProjectionBundle>(serialized).is_err());
    assert_eq!(valid.identity(), identity);
}

#[test]
fn malformed_inputs_return_typed_errors_without_forward_state() {
    let bundle = ProjectionBundle::generate(13);
    let short_input = vec![0.0; EMBEDDING_DIM - 1];
    assert!(matches!(
        bundle.project_to_ltm(&short_input),
        Err(CoreError::DimensionMismatch {
            what: "projection input",
            expected: EMBEDDING_DIM,
            actual: _
        })
    ));
    assert!(matches!(
        bundle.project_to_stm(&vec![f32::INFINITY; EMBEDDING_DIM]),
        Err(CoreError::NonFinite("projection input"))
    ));
    assert!(matches!(
        bundle.ltm_to_3d(&vec![0.0; LTM_KEY_DIM - 1]),
        Err(CoreError::DimensionMismatch { .. })
    ));
}

#[test]
fn serialize_round_trip_preserves_matrices_and_identity() {
    let bundle = ProjectionBundle::generate(99);
    let identity = bundle.identity();
    let encoded = serde_json::to_string(&bundle).unwrap();
    let restored: ProjectionBundle = serde_json::from_str(&encoded).unwrap();
    assert_eq!(restored, bundle);
    assert_eq!(restored.identity(), identity);
}

#[test]
fn projected_record_retains_the_direct_ltm_basis_key() {
    let bundle = ProjectionBundle::generate(19);
    let key = vec![0.0_f32; EMBEDDING_DIM];
    let value = vec![1.0_f32; EMBEDDING_DIM];
    let record = bundle.project_record(&key, &value).unwrap();
    assert_eq!(record.ltm_key, bundle.project_to_ltm(&key).unwrap());
    assert_eq!(record.stm_key, bundle.project_to_stm(&key).unwrap());
    assert_eq!(record.value, bundle.project_to_value(&value).unwrap());
    assert_eq!(record.context, bundle.project_to_context(&key).unwrap());
    assert_eq!(
        record.ltm_terrain,
        bundle.ltm_to_3d(&record.ltm_key).unwrap()
    );
    assert_eq!(
        record.stm_terrain,
        bundle.stm_to_3d(&record.stm_key).unwrap()
    );
}

#[test]
fn projection_oracle_matches_all_forward_families() {
    let fixture: ProjectionFixture = serde_json::from_str(ORACLE).unwrap();
    let bundle = ProjectionBundle::from_matrices(matrices_from_oracle(fixture.parameters)).unwrap();
    for (index, input) in fixture.input_embeddings.iter().enumerate() {
        let ltm = bundle.project_to_ltm(input).unwrap();
        let stm = bundle.project_to_stm(input).unwrap();
        assert_close(&ltm, &fixture.outputs.to_ltm_key[index], &fixture.tolerance);
        assert_close(&stm, &fixture.outputs.to_stm_key[index], &fixture.tolerance);
        assert_close(
            &bundle.project_to_value(input).unwrap(),
            &fixture.outputs.to_value[index],
            &fixture.tolerance,
        );
        assert_close(
            &bundle.project_to_context(input).unwrap(),
            &fixture.outputs.to_context[index],
            &fixture.tolerance,
        );
        assert_close(
            &bundle.ltm_to_3d(&ltm).unwrap(),
            &fixture.outputs.ltm_to_3d[index],
            &fixture.tolerance,
        );
        assert_close(
            &bundle.stm_to_3d(&stm).unwrap(),
            &fixture.outputs.stm_to_3d[index],
            &fixture.tolerance,
        );
        assert_close(
            &bundle.consolidate_key(&stm).unwrap(),
            &fixture.outputs.stm_to_ltm[index],
            &fixture.tolerance,
        );
    }
}
