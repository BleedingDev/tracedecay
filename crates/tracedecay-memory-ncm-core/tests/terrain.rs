//! Integration tests for three-dimensional NCM terrain dynamics.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use serde_json::Value;
use tracedecay_memory_ncm_core::terrain::{Terrain3D, DEFAULT_SPLAT_SIGMA};
use tracedecay_memory_ncm_core::types::{CoreError, NcmConfig, AFFECT_DIM};

const ORACLE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../product/ncm/reference/oracle/terrain.json"
));

fn flat(x: usize, y: usize, z: usize, g: usize) -> usize {
    (x * g + y) * g + z
}

fn flat_swapped(x: usize, y: usize, z: usize, g: usize) -> usize {
    (z * g + y) * g + x
}

fn oracle() -> Value {
    serde_json::from_str(ORACLE).unwrap()
}

fn scalar(value: &Value) -> f32 {
    value.as_f64().unwrap() as f32
}

fn flatten_numbers(value: &Value, output: &mut Vec<f32>) {
    if let Some(values) = value.as_array() {
        for item in values {
            flatten_numbers(item, output);
        }
    } else {
        output.push(scalar(value));
    }
}

fn field(value: &Value) -> Vec<f32> {
    let mut output = Vec::new();
    flatten_numbers(value, &mut output);
    output
}

fn vector<const N: usize>(value: &Value) -> [f32; N] {
    let values = value.as_array().unwrap();
    assert_eq!(values.len(), N);
    std::array::from_fn(|index| scalar(&values[index]))
}

fn assert_close(actual: &[f32], expected: &[f32], atol: f32, rtol: f32) {
    assert_eq!(actual.len(), expected.len());
    for (index, (actual_value, expected_value)) in actual.iter().zip(expected).enumerate() {
        let limit = atol + rtol * expected_value.abs();
        let error = (actual_value - expected_value).abs();
        assert!(
            error <= limit,
            "index {index}: actual {actual_value} expected {expected_value}, error {error}, limit {limit}"
        );
    }
}

fn fixture_tolerance(fixture: &Value) -> (f32, f32) {
    (
        scalar(&fixture["tolerance"]["atol"]),
        scalar(&fixture["tolerance"]["rtol"]),
    )
}

fn terrain_from_fixture(value: &Value) -> Terrain3D {
    Terrain3D {
        resolution: value["resolution"].as_u64().unwrap() as usize,
        h: field(&value["H"]),
        e: field(&value["E"]),
        alpha_h: scalar(&value["alpha_h"]),
        alpha_e: scalar(&value["alpha_e"]),
        leak: scalar(&value["leak"]),
    }
}

fn blur_noop(terrain: &Terrain3D) -> (Vec<f32>, Vec<f32>) {
    (terrain.h.clone(), terrain.e.clone())
}

fn spread_detected(original: &[f32], blurred: &[f32], impulse_index: usize) -> bool {
    blurred[impulse_index] < original[impulse_index]
        && blurred
            .iter()
            .enumerate()
            .any(|(index, value)| index != impulse_index && *value > 0.0)
}

fn laplacian_at(field: &[f32], x: usize, y: usize, z: usize, g: usize) -> f32 {
    field[flat(x.saturating_sub(1), y, z, g)]
        + field[flat(x, y.saturating_sub(1), z, g)]
        + field[flat(x, y, z.saturating_sub(1), g)]
        - 6.0 * field[flat(x, y, z, g)]
        + field[flat(x, y, (z + 1).min(g - 1), g)]
        + field[flat(x, (y + 1).min(g - 1), z, g)]
        + field[flat((x + 1).min(g - 1), y, z, g)]
}

#[test]
fn construction_bounds_resolution_and_stability() {
    assert!(matches!(
        Terrain3D::new(0, 0.0, 0.0, 0.0),
        Err(CoreError::BudgetExceeded(_))
    ));
    assert!(matches!(
        Terrain3D::new(65, 0.0, 0.0, 0.0),
        Err(CoreError::BudgetExceeded(_))
    ));
    assert!(matches!(
        Terrain3D::new(8, 0.17, 0.0, 0.0),
        Err(CoreError::InvalidState(_))
    ));
    assert!(matches!(
        Terrain3D::new(8, -0.01, 0.0, 0.0),
        Err(CoreError::InvalidState(_))
    ));
    assert!(matches!(
        Terrain3D::new(8, f32::NAN, 0.0, 0.0),
        Err(CoreError::NonFinite(_))
    ));

    let config = NcmConfig::default();
    assert!(config.ltm.terrain_lambda + 6.0 * config.ltm.terrain_alpha_h <= 1.0);
    assert!(config.stm.terrain_lambda + 6.0 * config.stm.terrain_alpha_h <= 1.0);
    assert!(Terrain3D::new(
        config.terrain_resolution,
        config.ltm.terrain_alpha_h,
        config.ltm.terrain_alpha_e,
        config.ltm.terrain_lambda,
    )
    .is_ok());
    assert!(Terrain3D::new(
        config.terrain_resolution,
        config.stm.terrain_alpha_h,
        config.stm.terrain_alpha_e,
        config.stm.terrain_lambda,
    )
    .is_ok());
}

#[test]
fn real_blur_spreads_impulse_preserves_interior_mass_and_constants() {
    let g = 17;
    let mut terrain = Terrain3D::new(g, 0.0, 0.0, 0.0).unwrap();
    let impulse = flat(7, 8, 9, g);
    terrain.h[impulse] = 1.0;
    let (blurred_h, _) = terrain.blur(1.0).unwrap();
    assert!(spread_detected(&terrain.h, &blurred_h, impulse));
    assert!((blurred_h.iter().sum::<f32>() - 1.0).abs() <= 1e-6);
    assert!(blurred_h[flat(6, 8, 9, g)] > 0.0);
    assert!(blurred_h[flat(7, 8, 10, g)] > 0.0);
    assert_ne!(blurred_h[flat(7, 8, 9, g)], blurred_h[flat(9, 8, 7, g)]);

    let (noop_h, _) = blur_noop(&terrain);
    assert!(!spread_detected(&terrain.h, &noop_h, impulse));
    assert_ne!(blurred_h, noop_h);

    terrain.h.fill(3.25);
    for channel in 0..AFFECT_DIM {
        let value = 0.5 + channel as f32;
        let cells = g.pow(3);
        terrain.e[channel * cells..(channel + 1) * cells].fill(value);
    }
    let (constant_h, constant_e) = terrain.blur(1.7).unwrap();
    assert!(constant_h.iter().all(|value| (*value - 3.25).abs() <= 1e-6));
    for channel in 0..AFFECT_DIM {
        let expected = 0.5 + channel as f32;
        let cells = g.pow(3);
        assert!(constant_e[channel * cells..(channel + 1) * cells]
            .iter()
            .all(|value| (*value - expected).abs() <= 1e-6));
    }
}

#[test]
fn sample_covers_corners_faces_fractional_points_and_border_clamp() {
    let g = 3;
    let mut terrain = Terrain3D::new(g, 0.0, 0.0, 0.0).unwrap();
    for x in 0..g {
        for y in 0..g {
            for z in 0..g {
                let value = (100 * x + 10 * y + z) as f32;
                let index = flat(x, y, z, g);
                terrain.h[index] = value;
                for channel in 0..AFFECT_DIM {
                    terrain.e[channel * g.pow(3) + index] = value + channel as f32 * 1000.0;
                }
            }
        }
    }

    let (corner_low, _) = terrain.sample([-1.0, -1.0, -1.0]);
    let (corner_high, _) = terrain.sample([1.0, 1.0, 1.0]);
    let (face, _) = terrain.sample([-1.0, 0.0, 0.0]);
    let (fractional, fractional_e) = terrain.sample([-0.5, 0.5, 0.0]);
    let (clamped, _) = terrain.sample([2.0, -2.0, 0.0]);
    assert_eq!(corner_low, 0.0);
    assert_eq!(corner_high, 222.0);
    assert_eq!(face, 11.0);
    assert!((fractional - 66.0).abs() <= 1e-6);
    assert!((fractional_e[3] - 3066.0).abs() <= 1e-6);
    assert_eq!(clamped, 201.0);
}

#[test]
fn asymmetric_splat_and_sample_enforce_xyz_axis_order() {
    let g = 16;
    let mut terrain = Terrain3D::new(g, 0.0, 0.0, 0.0).unwrap();
    let position = [-0.6, -0.2, 0.6];
    terrain
        .splat(position, 1.0, None, DEFAULT_SPLAT_SIGMA, 1.0)
        .unwrap();

    let xyz = flat(3, 6, 12, g);
    let swapped = flat_swapped(3, 6, 12, g);
    assert!(terrain.h[xyz] > 0.999_999);
    assert_eq!(terrain.h[swapped], 0.0);
    let (at_splat, _) = terrain.sample(position);
    let (at_swapped, _) = terrain.sample([position[2], position[1], position[0]]);
    assert!(at_splat > 0.999_998);
    assert_eq!(at_swapped, 0.0);
    assert!(at_splat > at_swapped);
}

#[test]
fn step_uses_replicate_boundaries_and_constant_laplacian_is_zero() {
    let mut terrain = Terrain3D::new(5, 0.12, 0.08, 0.01).unwrap();
    terrain.h.fill(2.0);
    terrain.e.fill(3.0);
    terrain.step();
    assert!(terrain.h.iter().all(|value| (*value - 1.98).abs() <= 1e-6));
    assert!(terrain.e.iter().all(|value| (*value - 2.98).abs() <= 1e-6));
}

#[test]
fn step_is_double_buffered_against_the_old_generation() {
    let g = 4;
    let mut terrain = Terrain3D::new(g, 0.11, 0.07, 0.03).unwrap();
    for (index, value) in terrain.h.iter_mut().enumerate() {
        *value = ((index * 17 + 3) % 29) as f32 / 7.0;
    }
    for (index, value) in terrain.e.iter_mut().enumerate() {
        *value = ((index * 11 + 5) % 31) as f32 / 9.0;
    }
    let old_h = terrain.h.clone();
    let old_e = terrain.e.clone();
    terrain.step();

    for x in 0..g {
        for y in 0..g {
            for z in 0..g {
                let index = flat(x, y, z, g);
                let expected_h = ((1.0 - 0.03) * old_h[index]
                    + 0.11 * laplacian_at(&old_h, x, y, z, g))
                .max(0.0);
                assert!((terrain.h[index] - expected_h).abs() <= 1e-6);
                for channel in 0..AFFECT_DIM {
                    let offset = channel * g.pow(3);
                    let expected_e = ((1.0 - 0.03) * old_e[offset + index]
                        + 0.03
                        + 0.07 * laplacian_at(&old_e[offset..offset + g.pow(3)], x, y, z, g))
                    .max(0.0);
                    assert!((terrain.e[offset + index] - expected_e).abs() <= 1e-6);
                }
            }
        }
    }
}

#[test]
fn homeostasis_remains_finite_nonnegative_and_moves_toward_neutral() {
    let g = 8;
    let mut terrain = Terrain3D::new(g, 0.02, 0.01, 0.0007).unwrap();
    for (index, value) in terrain.h.iter_mut().enumerate() {
        *value = ((index * 13 + 7) % 23) as f32 / 10.0;
    }
    for (index, value) in terrain.e.iter_mut().enumerate() {
        *value = 0.25 + ((index * 19 + 2) % 37) as f32 / 10.0;
    }
    let initial_h_sum = terrain.h.iter().sum::<f32>();
    let initial_e_error = terrain
        .e
        .iter()
        .map(|value| (value - 1.0).abs())
        .sum::<f32>();
    for _ in 0..500 {
        terrain.step();
    }
    let final_h_sum = terrain.h.iter().sum::<f32>();
    let final_e_error = terrain
        .e
        .iter()
        .map(|value| (value - 1.0).abs())
        .sum::<f32>();
    assert!(terrain
        .h
        .iter()
        .all(|value| value.is_finite() && *value >= 0.0));
    assert!(terrain
        .e
        .iter()
        .all(|value| value.is_finite() && *value >= 0.0));
    assert!(final_h_sum < initial_h_sum);
    assert!(final_e_error < initial_e_error);
}

#[test]
fn splat_validation_is_atomic_and_reset_restores_neutral_fields() {
    let mut terrain = Terrain3D::new(6, 0.02, 0.01, 0.0007).unwrap();
    let initial_digest = terrain.digest();
    assert!(matches!(
        terrain.splat([0.0; 3], 1.0, None, 0.0, 1.0),
        Err(CoreError::InvalidState(_))
    ));
    assert_eq!(terrain.digest(), initial_digest);
    terrain
        .splat([0.1, -0.3, 0.7], 2.0, Some([1.2, 0.7, 1.4, 0.9]), 0.1, 0.02)
        .unwrap();
    assert_ne!(terrain.digest(), initial_digest);
    let encoded = serde_json::to_string(&terrain).unwrap();
    let restored: Terrain3D = serde_json::from_str(&encoded).unwrap();
    assert_eq!(restored, terrain);
    let stats = terrain.stats();
    assert!(stats.h_max > 0.0);
    assert!(stats.h_nonzero > 0);
    assert!(stats.e_max > 1.0);
    terrain.reset();
    assert_eq!(terrain.digest(), initial_digest);
    assert!(terrain.h.iter().all(|value| *value == 0.0));
    assert!(terrain.e.iter().all(|value| *value == 1.0));
}

#[test]
fn fixture_splat_and_corrected_sampling_match() {
    let fixture = oracle();
    let (atol, rtol) = fixture_tolerance(&fixture);
    let mut terrain = terrain_from_fixture(&fixture["initial"]);
    let positions = fixture["splat_inputs"]["positions"].as_array().unwrap();
    let intensities = fixture["splat_inputs"]["intensities"].as_array().unwrap();
    let emotions = fixture["splat_inputs"]["emotions"].as_array().unwrap();
    let sigma = scalar(&fixture["splat_inputs"]["sigma"]);
    let eta = scalar(&fixture["splat_inputs"]["eta"]);
    for index in 0..positions.len() {
        terrain
            .splat(
                vector(&positions[index]),
                scalar(&intensities[index]),
                Some(vector(&emotions[index])),
                sigma,
                eta,
            )
            .unwrap();
    }
    assert_close(&terrain.h, &field(&fixture["after_splat"]["H"]), atol, rtol);
    assert_close(&terrain.e, &field(&fixture["after_splat"]["E"]), atol, rtol);

    let probes = fixture["axis_probe"]["sample_positions"][0]
        .as_array()
        .unwrap();
    let expected = field(&fixture["axis_probe"]["corrected_H"]);
    let mut axis_terrain = terrain_from_fixture(&fixture["initial"]);
    axis_terrain.h = field(&fixture["axis_probe"]["H"]);
    for (index, position) in probes.iter().enumerate() {
        let (actual, affect) = axis_terrain.sample(vector(position));
        let limit = atol + rtol * expected[index].abs();
        assert!((actual - expected[index]).abs() <= limit);
        assert_close(&affect, &[1.0; AFFECT_DIM], atol, rtol);
    }
}

#[test]
fn fixture_step_and_corrected_sample_grid_match() {
    let fixture = oracle();
    let (atol, rtol) = fixture_tolerance(&fixture);
    let mut terrain = terrain_from_fixture(&fixture["after_splat"]);
    let g = terrain.resolution;
    let mut actual_laplacian_h = vec![0.0; g.pow(3)];
    for x in 0..g {
        for y in 0..g {
            for z in 0..g {
                let index = flat(x, y, z, g);
                actual_laplacian_h[index] = laplacian_at(&terrain.h, x, y, z, g);
            }
        }
    }
    assert_close(
        &actual_laplacian_h,
        &field(&fixture["laplacian_before_step"]["H"]),
        atol,
        rtol,
    );
    let cells = g.pow(3);
    let mut actual_laplacian_e = vec![0.0; AFFECT_DIM * cells];
    for channel in 0..AFFECT_DIM {
        let source = &terrain.e[channel * cells..(channel + 1) * cells];
        for x in 0..g {
            for y in 0..g {
                for z in 0..g {
                    let index = flat(x, y, z, g);
                    actual_laplacian_e[channel * cells + index] = laplacian_at(source, x, y, z, g);
                }
            }
        }
    }
    assert_close(
        &actual_laplacian_e,
        &field(&fixture["laplacian_before_step"]["E"]),
        atol,
        rtol,
    );
    terrain.step();
    assert_close(&terrain.h, &field(&fixture["after_step"]["H"]), atol, rtol);
    assert_close(&terrain.e, &field(&fixture["after_step"]["E"]), atol, rtol);

    let positions = fixture["sample_positions"][0].as_array().unwrap();
    let mut expected_h = field(&fixture["sample_after_step"][0]);
    let mut expected_e = field(&fixture["sample_after_step"][1]);
    expected_h.swap(6, 7);
    for channel in 0..AFFECT_DIM {
        expected_e.swap(6 * AFFECT_DIM + channel, 7 * AFFECT_DIM + channel);
    }
    for (index, position) in positions.iter().enumerate() {
        let (actual_h, actual_e) = terrain.sample(vector(position));
        assert_close(&[actual_h], &expected_h[index..=index], atol, rtol);
        assert_close(
            &actual_e,
            &expected_e[index * AFFECT_DIM..(index + 1) * AFFECT_DIM],
            atol,
            rtol,
        );
    }
}

#[test]
fn fixture_corrected_blur_matches_and_reference_noop_differs() {
    let fixture = oracle();
    let (atol, rtol) = fixture_tolerance(&fixture);
    let terrain = terrain_from_fixture(&fixture["after_step"]);
    let sigma = scalar(&fixture["blur_corrected"]["sigma"]);
    let (blurred_h, blurred_e) = terrain.blur(sigma).unwrap();
    let expected_h = field(&fixture["blur_corrected"]["H"]);
    let expected_e = field(&fixture["blur_corrected"]["E"]);
    assert_close(&blurred_h, &expected_h, atol, rtol);
    assert_close(&blurred_e, &expected_e, atol, rtol);

    let reference_h = field(&fixture["blur_reference"]["H"]);
    let reference_e = field(&fixture["blur_reference"]["E"]);
    assert_ne!(blurred_h, reference_h);
    assert_ne!(blurred_e, reference_e);
}

#[test]
fn merge_uses_corrected_blur_fixture_not_reference_clone() {
    let fixture = oracle();
    let (atol, rtol) = fixture_tolerance(&fixture);
    let other = terrain_from_fixture(&fixture["after_step"]);
    let mut terrain = terrain_from_fixture(&fixture["merge_initial"]);
    let xi_h = scalar(&fixture["merge_inputs"]["xi_h"]);
    let xi_e = scalar(&fixture["merge_inputs"]["xi_e"]);
    let blur_sigma = scalar(&fixture["merge_inputs"]["blur_sigma"]);
    terrain.merge_from(&other, xi_h, xi_e, blur_sigma).unwrap();

    let expected_blurred_h = field(&fixture["blur_corrected"]["H"]);
    let expected_blurred_e = field(&fixture["blur_corrected"]["E"]);
    let expected_h: Vec<f32> = expected_blurred_h
        .iter()
        .map(|value| xi_h * value)
        .collect();
    let expected_e: Vec<f32> = expected_blurred_e
        .iter()
        .map(|value| 1.0 + xi_e * value)
        .collect();
    assert_close(&terrain.h, &expected_h, atol, rtol);
    assert_close(&terrain.e, &expected_e, atol, rtol);
    assert_ne!(terrain.h, field(&fixture["after_merge"]["H"]));
}
