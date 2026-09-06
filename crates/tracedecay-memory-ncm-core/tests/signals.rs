#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
//! Executable-oracle and negative-control tests for admitted NCM signals.

use serde::Deserialize;
use std::collections::HashMap;
use tracedecay_memory_ncm_core::centers::MemoryCenters;
use tracedecay_memory_ncm_core::signals::affect::{
    extract_keyword_affect, from_dict, from_name, neutral, validated,
};
use tracedecay_memory_ncm_core::signals::{novelty_from_ltm, write_strength, SignalSource};
use tracedecay_memory_ncm_core::types::{
    AffectVector, CoreError, NcmConfig, AFFECT_DIM, LTM_KEY_DIM,
};

#[derive(Clone, Copy, Deserialize)]
struct Tolerance {
    atol: f32,
    rtol: f32,
}

#[derive(Deserialize)]
struct StrengthCase {
    emotions: Vec<f32>,
    surprise: Option<Vec<f32>>,
    intensity: f32,
    novelty: Vec<f32>,
    omega: Vec<f32>,
}

#[derive(Deserialize)]
struct ExtractCase {
    text: String,
    output: Vec<f32>,
}

#[derive(Deserialize)]
struct NameCase {
    name: String,
    output: Vec<f32>,
}

#[derive(Deserialize)]
struct DictCase {
    input: HashMap<String, f32>,
    output: Vec<f32>,
}

#[derive(Deserialize)]
struct SignalFixture {
    tolerance: Tolerance,
    cases: Vec<StrengthCase>,
    extract: Vec<ExtractCase>,
    from_name: Vec<NameCase>,
    from_dict: Vec<DictCase>,
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

fn affect(values: &[f32]) -> AffectVector {
    AffectVector(values.try_into().expect("fixture affect has four channels"))
}

#[test]
fn write_strength_matches_all_executable_oracle_closed_form_cases() {
    let fixture: SignalFixture = serde_json::from_str(include_str!(
        "../../../product/ncm/reference/oracle/write_strength.json"
    ))
    .expect("write-strength fixture parses");
    let config = NcmConfig::default();
    for (index, case) in fixture.cases.iter().enumerate() {
        let surprise = case.surprise.as_ref().map_or(0.0, |values| values[0]);
        let actual = write_strength(
            &config,
            case.novelty[0],
            surprise,
            affect(&case.emotions),
            case.intensity,
        )
        .expect("oracle write strength succeeds");
        assert_close(
            &[actual],
            &case.omega,
            fixture.tolerance,
            &format!("write strength case {index}"),
        );
    }
}

#[test]
fn affect_presets_dictionaries_and_keywords_match_executable_oracle() {
    let fixture: SignalFixture = serde_json::from_str(include_str!(
        "../../../product/ncm/reference/oracle/write_strength.json"
    ))
    .expect("write-strength fixture parses");

    for case in &fixture.from_name {
        let actual = from_name(&case.name).expect("preset is finite");
        assert_close(
            &actual.0,
            &case.output,
            fixture.tolerance,
            &format!("preset {}", case.name),
        );
    }
    for (index, case) in fixture.from_dict.iter().enumerate() {
        let actual = from_dict(&case.input).expect("dictionary is finite");
        assert_close(
            &actual.0,
            &case.output,
            fixture.tolerance,
            &format!("dictionary {index}"),
        );
    }
    for (index, case) in fixture.extract.iter().enumerate() {
        let (actual, source) = extract_keyword_affect(&case.text);
        assert_eq!(source, SignalSource::KeywordHeuristicV1);
        assert_close(
            &actual.0,
            &case.output,
            fixture.tolerance,
            &format!("keyword case {index}"),
        );
    }
}

#[test]
fn neutral_explicit_vectors_and_exact_dictionary_spellings_are_preserved() {
    assert_eq!(neutral(), AffectVector([1.0; AFFECT_DIM]));
    assert_eq!(
        validated([0.2, 0.4, 0.6, 0.8]).expect("explicit vector is finite"),
        AffectVector([0.2, 0.4, 0.6, 0.8])
    );

    let czech_keys = HashMap::from([("dopamin".to_owned(), 1.8), ("kortizol".to_owned(), 0.6)]);
    assert_eq!(
        from_dict(&czech_keys).expect("Czech keys are admitted"),
        AffectVector([1.8, 1.0, 0.6, 1.0])
    );

    let docstring_only_english =
        HashMap::from([("dopamine".to_owned(), 1.8), ("cortisol".to_owned(), 0.6)]);
    assert_eq!(
        from_dict(&docstring_only_english).expect("ignored aliases are finite"),
        AffectVector::neutral()
    );
    assert_eq!(
        from_name("Positive").expect("unknown preset is neutral"),
        neutral()
    );
}

#[test]
fn keyword_policy_characterizes_substring_repetition_and_negation_limits() {
    let (repeated, _) = extract_keyword_affect("GREAT great great");
    assert_eq!(repeated, AffectVector([1.3, 1.0, 1.0, 1.0]));

    let (substrings, _) = extract_keyword_affect("window contentment");
    assert_eq!(substrings, AffectVector([1.3, 1.2, 1.0, 1.0]));

    let (negated, source) = extract_keyword_affect("not great and no success");
    assert!((negated.0[0] - 1.6).abs() < 1e-6);
    assert_eq!(&negated.0[1..], &[1.0, 1.0, 1.0]);
    assert_eq!(source, SignalSource::KeywordHeuristicV1);
}

#[test]
fn malformed_or_nonfinite_affect_and_signal_inputs_are_rejected() {
    assert_eq!(
        validated([1.0, f32::NAN, 1.0, 1.0]),
        Err(CoreError::NonFinite("affect"))
    );
    let dictionary = HashMap::from([("dopamin".to_owned(), f32::INFINITY)]);
    assert_eq!(
        from_dict(&dictionary),
        Err(CoreError::NonFinite("affect dictionary"))
    );
    assert_eq!(
        write_strength(
            &NcmConfig::default(),
            f32::NAN,
            0.0,
            AffectVector::neutral(),
            1.0,
        ),
        Err(CoreError::NonFinite("write-strength inputs"))
    );
    assert_eq!(
        write_strength(
            &NcmConfig::default(),
            1.0,
            0.0,
            AffectVector([1.0, f32::NAN, 1.0, 1.0]),
            1.0,
        ),
        Err(CoreError::NonFinite("write-strength affect"))
    );
}

#[test]
fn novelty_is_one_when_empty_and_one_minus_top_raw_rbf_when_populated() {
    let mut layer = NcmConfig::default().ltm;
    layer.n_centers = 2;
    let mut centers = MemoryCenters::with_layout(layer);
    let query = {
        let mut key = [0.0; LTM_KEY_DIM];
        key[0] = 1.0;
        key
    };
    assert_eq!(
        novelty_from_ltm(&centers, &query).expect("empty novelty succeeds"),
        1.0
    );

    centers.keys[..LTM_KEY_DIM].copy_from_slice(&query);
    centers.active[0] = true;
    centers.incarnation[0] = 1;
    centers.intensity[0] = 1.0;
    let exact = novelty_from_ltm(&centers, &query).expect("exact novelty succeeds");
    assert!(exact.abs() <= f32::EPSILON);

    let mut orthogonal = [0.0; LTM_KEY_DIM];
    orthogonal[1] = 1.0;
    let expected_weight =
        (-1.0_f32 / (centers.config.sigma_read * centers.config.sigma_read)).exp();
    let actual = novelty_from_ltm(&centers, &orthogonal).expect("orthogonal novelty succeeds");
    assert!((actual - (1.0 - expected_weight)).abs() < 1e-6);
}

#[test]
fn zero_external_intensity_forces_zero_write_strength() {
    let omega = write_strength(
        &NcmConfig::default(),
        1.0,
        10.0,
        AffectVector([2.0; AFFECT_DIM]),
        0.0,
    )
    .expect("finite zero intensity succeeds");
    assert_eq!(omega, 0.0);
}
