//! The reject floor is evaluated over every class a string carries.
//!
//! `detect_secret_like` returns the reason of the FIRST pattern that matched.
//! For every payload in this file that first reason is
//! `credential-like key=value assignment` — a `redact` class — while the string
//! also carries a class on the reject floor. Classifying the string by the
//! first reason alone therefore delivered a redacted copy of a live credential.
//! Each test asserts the withheld outcome *and* pins the masking reason, so a
//! regression that reverts the multi-signal pass fails here rather than
//! silently shipping bytes.
//!
//! Every credential literal is assembled with `concat!` so this file is not
//! itself a corpus of secret-shaped strings, and none of them is live.
#![allow(clippy::expect_used, clippy::panic)]

use serde_json::{Value, json};
use tracedecay_memory_hygiene::{
    HygieneClass, ObservationAdmission, ObservationSanitizer, WithheldReason,
};
use tracedecay_runtime_core::memory::hygiene::detect_secret_like;

fn sanitizer() -> ObservationSanitizer {
    ObservationSanitizer::new().expect("canonical hygiene policy")
}

/// Asserts the shared detector really does answer with the masking class, so
/// these fixtures keep exercising the multi-class path rather than quietly
/// becoming single-class cases after a corpus refresh.
fn assert_first_reason_is_the_assignment(text: &str) {
    assert_eq!(
        detect_secret_like(text).as_deref(),
        Some("credential-like key=value assignment"),
        "this fixture no longer exercises a masked reject-floor class"
    );
}

fn assert_withheld_as_secret(payload: &Value, expected_class: HygieneClass) {
    let sanitizer = sanitizer();
    let findings = sanitizer.classify(payload).expect("classification");
    assert!(
        findings
            .iter()
            .any(|finding| finding.class() == expected_class),
        "expected a {expected_class:?} finding, got {findings:?}"
    );
    match sanitizer.admit(payload).expect("admission") {
        ObservationAdmission::Withheld { reason, .. } => {
            assert_eq!(reason, WithheldReason::SecretRejected);
        }
        other => panic!("expected the payload to be withheld, got {other:?}"),
    }
}

#[test]
fn an_assignment_whose_value_is_an_issuer_token_is_rejected_not_redacted() {
    let text = concat!("token: ", "ghp_", "KsY7QwT2mZ4bV9nR6cX1jH8pL3dG5fA0eUwQ");
    assert_first_reason_is_the_assignment(text);
    assert_withheld_as_secret(
        &json!({ "note": text }),
        HygieneClass::KnownCredentialPrefix,
    );
}

#[test]
fn an_api_key_assignment_carrying_a_known_prefix_is_rejected_not_redacted() {
    let text = concat!("api_", "key=", "sk-", "proj1234567890abcdefghijklmn");
    assert_first_reason_is_the_assignment(text);
    assert_withheld_as_secret(&json!({ "env": text }), HygieneClass::KnownCredentialPrefix);
}

#[test]
fn an_assignment_beside_a_separate_high_entropy_token_is_rejected_not_redacted() {
    let text = concat!(
        "password: hunter2hunter2hunter2 value ",
        "Qm9vZ2llV29vZ2llMTIzNDU2Nzg5MGFiY2RlZmdoaWprbG1ub3A4OTc2NTQzMjE"
    );
    assert_first_reason_is_the_assignment(text);
    assert_withheld_as_secret(&json!({ "note": text }), HygieneClass::HighEntropyToken);
}

#[test]
fn an_assignment_whose_own_value_is_high_entropy_is_rejected_not_redacted() {
    // The high-entropy token is only visible once the name in front of it stops
    // diluting the score, which is what splitting on the declared separators is
    // for.
    let text = concat!(
        "secret=",
        "Qm9vZ2llV29vZ2llMTIzNDU2Nzg5MGFiY2RlZmdoaWprbG1ub3A4OTc2NTQzMjE"
    );
    assert_first_reason_is_the_assignment(text);
    assert_withheld_as_secret(&json!({ "note": text }), HygieneClass::HighEntropyToken);
}

/// The catalogue currently orders the private-key and bearer rules ahead of the
/// assignment rule, so those two classes happen to survive as the first reason
/// today. Ordering is not a contract, and both shapes span a whitespace
/// boundary that a candidate probe cannot re-derive, which is why they are
/// checked as direct signals too. These two cases pin the outcome so a
/// reordering of the shared catalogue cannot quietly turn either into a
/// delivered redaction.
#[test]
fn a_pem_block_beside_an_assignment_is_rejected_whatever_the_catalogue_orders_first() {
    let text = concat!(
        "token: abcdefghijklmnopqrst\n-----BEGIN RSA ",
        "PRIVATE KEY-----\nnot-a-real-key\n"
    );
    assert_withheld_as_secret(&json!({ "note": text }), HygieneClass::PrivateKey);
}

#[test]
fn a_bearer_token_beside_an_assignment_is_rejected_whatever_the_catalogue_orders_first() {
    let text = concat!(
        "token: abcdefghijklmnopqrst and Authorization: ",
        "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
    );
    assert_withheld_as_secret(&json!({ "note": text }), HygieneClass::BearerToken);
}

#[test]
fn a_masked_class_in_an_object_key_is_rejected_too() {
    let key = concat!("token: ", "AKIA", "4S27TQXBVCZ5MJ6L");
    assert_first_reason_is_the_assignment(key);
    let payload = json!({ key: "value" });
    assert_withheld_as_secret(&payload, HygieneClass::KnownCredentialPrefix);
}

#[test]
fn the_multi_signal_pass_never_echoes_the_material_it_names() {
    let sanitizer = sanitizer();
    let secrets = [
        concat!("token: ", "ghp_", "KsY7QwT2mZ4bV9nR6cX1jH8pL3dG5fA0eUwQ"),
        concat!("api_", "key=", "sk-", "proj1234567890abcdefghijklmn"),
        concat!(
            "secret=",
            "Qm9vZ2llV29vZ2llMTIzNDU2Nzg5MGFiY2RlZmdoaWprbG1ub3A4OTc2NTQzMjE"
        ),
    ];
    for secret in secrets {
        let payload = json!({ "note": secret });
        let findings = sanitizer.classify(&payload).expect("classification");
        let rendered = format!("{findings:?}");
        assert!(
            !rendered.contains(secret),
            "a finding echoed detected material: {rendered}"
        );
        match sanitizer.admit(&payload).expect("admission") {
            ObservationAdmission::Withheld {
                receipt_id,
                source_payload_sha256,
                ..
            } => {
                assert!(!receipt_id.contains(secret));
                assert!(!source_payload_sha256.contains(secret));
            }
            other => panic!("expected a withheld admission, got {other:?}"),
        }
    }
}

#[test]
fn the_supplementary_pass_leaves_ordinary_prose_alone() {
    // The direct prefix signal is length-bounded precisely so a fixture profile
    // name that shares a credential prefix's letters stays durable knowledge.
    let sanitizer = sanitizer();
    for text in [
        "Use the sk-test fixture profile for dry runs",
        "risk-assessment-frameworks are documented under docs/",
        "the fixture project id is 550e8400-e29b-41d4-a716-446655440000",
        "commit 3bc562b8a1f0d9e7c6b5a4d3e2f1a0b9c8d7e6f5 introduced the seam",
        "the incremental pass finished after CamelCaseIdentifiersAreFineEvenWhenLong ran",
        "npm_config_registry",
        "npm_config_cache",
        "npm_package_version",
        "hf_hub_download_timeout",
        "ASIA_SOUTHEAST_1",
        "sk-learn-preprocessing",
    ] {
        let payload = json!({ "note": text });
        assert!(
            sanitizer
                .classify(&payload)
                .expect("classification")
                .is_empty(),
            "{text} raised a finding"
        );
        assert!(
            matches!(
                sanitizer.admit(&payload).expect("admission"),
                ObservationAdmission::Admitted { .. }
            ),
            "{text} was withheld"
        );
    }
}
