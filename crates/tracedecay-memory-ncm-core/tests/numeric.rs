//! Integration tests for the NCM numerical kernels.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use tracedecay_memory_ncm_core::CoreError;
use tracedecay_memory_ncm_core::numeric::{
    DeterministicRng, Digest128, WEIGHT_EPSILON, digest128, dot, l2_norm, log_space_softmax, log1p,
    minkowski, minkowski_p_half, normalize, rbf, rbf_weight, sigmoid, splitmix64, squared_distance,
    stable_softmax, tanh, top_k_largest, top_k_smallest, validate_dimension, validate_finite,
};

fn assert_close(actual: f32, expected: f32, tolerance: f32) {
    let error = (actual - expected).abs();
    assert!(
        error <= tolerance,
        "actual {actual} differs from expected {expected} by {error}"
    );
}

#[test]
fn normalize_zero_and_small_vectors_use_the_torch_floor() {
    assert_eq!(normalize(&[0.0, 0.0, 0.0]).unwrap(), vec![0.0, 0.0, 0.0]);

    let normalized = normalize(&[1.0e-13]).unwrap();
    assert_close(normalized[0], 0.1, 1.0e-6);
    assert_close(l2_norm(&[3.0, 4.0]).unwrap(), 5.0, 1.0e-6);
}

#[test]
fn dot_cosine_and_squared_distance_are_checked_and_clamped() {
    assert_close(dot(&[1.0, 2.0], &[3.0, 4.0]).unwrap(), 11.0, 1.0e-6);
    assert_eq!(
        tracedecay_memory_ncm_core::numeric::cosine(&[2.0, 0.0], &[2.0, 0.0]).unwrap(),
        1.0
    );
    assert_eq!(squared_distance(&[2.0, 0.0], &[2.0, 0.0]).unwrap(), 0.0);
    assert_close(
        tracedecay_memory_ncm_core::numeric::cosine(&[-2.0, 0.0], &[2.0, 0.0]).unwrap(),
        -1.0,
        1.0e-6,
    );
}

#[test]
fn rbf_uses_the_reference_sigma_denominator() {
    assert_close(rbf_weight(2.0, 1.0).unwrap(), (-1.0_f32).exp(), 1.0e-7);
    assert_eq!(rbf(0.0, 0.5).unwrap(), 1.0);
    assert!(matches!(
        rbf_weight(-1.0, 1.0),
        Err(CoreError::InvalidState(_))
    ));
    assert!(matches!(
        rbf_weight(1.0, 0.0),
        Err(CoreError::InvalidState(_))
    ));
}

#[test]
fn stable_softmax_and_log_space_softmax_keep_reference_epsilons() {
    let stable = stable_softmax(&[1000.0, 999.0]).unwrap();
    let expected_first = 1.0 / (1.0 + (-1.0_f32).exp());
    assert_close(stable[0], expected_first, 1.0e-6);
    assert_close(stable.iter().sum(), 1.0, 1.0e-6);

    let weighted = log_space_softmax(&[0.0, 0.0], &[0.0, 1.0]).unwrap();
    let first_logit = (WEIGHT_EPSILON).ln() + (WEIGHT_EPSILON).ln();
    let second_logit = (WEIGHT_EPSILON).ln() + (1.0 + WEIGHT_EPSILON).ln();
    let expected = stable_softmax(&[first_logit, second_logit]).unwrap();
    assert_close(weighted[0], expected[0], 1.0e-7);
    assert_close(weighted[1], expected[1], 1.0e-7);
    assert!(
        weighted[0] > 0.0,
        "the +1e-8 terms preserve zero candidates"
    );
}

#[test]
fn minkowski_half_matches_the_non_metric_reference_formula() {
    let left = [0.0, 1.0, 4.0];
    let right = [1.0, 4.0, 0.0];
    let expected = (1.0_f32.sqrt() + 3.0_f32.sqrt() + 4.0_f32.sqrt()).powi(2);
    assert_close(minkowski_p_half(&left, &right).unwrap(), expected, 1.0e-5);
    assert_close(minkowski(&left, &right, 0.5).unwrap(), expected, 1.0e-5);
    assert!(matches!(
        minkowski(&left, &right, 0.0),
        Err(CoreError::InvalidState(_))
    ));
}

#[test]
fn top_k_is_deterministic_for_largest_and_smallest_ties() {
    let values = [1.0, 3.0, 3.0, 2.0, 3.0];
    assert_eq!(
        top_k_largest(&values, 3).unwrap(),
        vec![(3.0, 1), (3.0, 2), (3.0, 4)]
    );
    assert_eq!(
        top_k_smallest(&[1.0, 0.0, 0.0, 1.0], 3).unwrap(),
        vec![(0.0, 1), (0.0, 2), (1.0, 0)]
    );
    assert_eq!(
        top_k_smallest(&[-0.0, 0.0], 2).unwrap(),
        vec![(-0.0, 0), (0.0, 1)]
    );
}

#[test]
fn scalar_helpers_match_f32_math_and_reject_invalid_inputs() {
    assert_close(log1p(1.0).unwrap(), 2.0_f32.ln(), 1.0e-7);
    assert_close(tanh(2.0).unwrap(), 2.0_f32.tanh(), 1.0e-7);
    assert_close(sigmoid(0.0).unwrap(), 0.5, 1.0e-7);
    assert!(matches!(log1p(-1.0), Err(CoreError::InvalidState(_))));
    assert!(matches!(sigmoid(f32::NAN), Err(CoreError::NonFinite(_))));
    assert!(matches!(tanh(f32::INFINITY), Err(CoreError::NonFinite(_))));
}

#[test]
fn validation_rejects_nonfinite_values_and_dimension_mismatches() {
    assert_eq!(validate_finite(&[1.0, 2.0], "values"), Ok(()));
    assert!(matches!(
        validate_finite(&[1.0, f32::NAN], "values"),
        Err(CoreError::NonFinite("values"))
    ));
    assert!(matches!(
        validate_dimension(&[1.0], 2, "values"),
        Err(CoreError::DimensionMismatch {
            what: "values",
            expected: 2,
            actual: 1
        })
    ));
    assert!(matches!(
        dot(&[f32::INFINITY], &[1.0]),
        Err(CoreError::NonFinite(_))
    ));
    assert!(matches!(
        dot(&[1.0], &[1.0, 2.0]),
        Err(CoreError::DimensionMismatch { .. })
    ));
}

#[test]
fn rng_seed_splitmix_and_gaussian_sequences_are_reproducible() {
    let mut splitmix_a = 0_u64;
    let mut splitmix_b = 0_u64;
    for _ in 0..8 {
        assert_eq!(splitmix64(&mut splitmix_a), splitmix64(&mut splitmix_b));
    }

    let mut first = DeterministicRng::from_seed(0);
    let mut second = DeterministicRng::new(0);
    let mut changed = DeterministicRng::new(1);
    let mut changed_any = false;
    for _ in 0..32 {
        let first_word = first.next_u64();
        let second_word = second.next_u64();
        assert_eq!(first_word, second_word);
        if changed.next_u64() != second_word {
            changed_any = true;
        }
        let uniform_a = first.uniform();
        let uniform_b = second.uniform();
        assert!((0.0..1.0).contains(&uniform_a));
        assert_eq!(uniform_a, uniform_b);
        let gaussian_a = first.gaussian();
        let gaussian_b = second.gaussian();
        assert_eq!(gaussian_a, gaussian_b);
    }
    assert!(changed_any);
}

#[test]
fn digest_is_streaming_stable_hex_and_sensitive_to_bytes() {
    let direct = digest128(b"projection bytes");
    let mut streaming = Digest128::default();
    streaming.update(b"projection ");
    streaming.update(b"bytes");
    assert_eq!(direct, streaming.hex());
    assert_eq!(direct.len(), 32);
    assert_ne!(direct, digest128(b"projection bytez"));
}
