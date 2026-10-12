//! An unexplained byte change is an error, never an invented finding.
//!
//! Pass C of admission diffs the canonical redactor's output against the source
//! and records what changed. Its fallback branch used to assert
//! `credential_assignment` + `redact` for *any* difference it could not walk
//! into, which put a fabricated class and a fabricated location on the receipt
//! — and, because the fabricated action rewrites bytes, satisfied the very
//! consistency gate that was supposed to catch it. `UnattributedRedaction` was
//! consequently unreachable.
//!
//! The attribution step is now [`attribute_sanitizer_output`], a pure analysis
//! function: it mints nothing and admits nothing, so a fixture can hand it a
//! fabricated sanitizer output without that being a way around the pipeline.
#![allow(clippy::expect_used, clippy::panic)]

use serde_json::{Value, json};
use tracedecay_memory_hygiene::{
    HygieneAction, HygieneClass, HygieneError, ObservationAdmission, ObservationHygienePolicyV1,
    ObservationSanitizer, SanitizationDisposition, attribute_sanitizer_output,
};

fn policy() -> ObservationHygienePolicyV1 {
    ObservationHygienePolicyV1::canonical().expect("canonical policy")
}

fn attribute(source: &Value, sanitized: &Value) -> Result<Vec<String>, HygieneError> {
    attribute_sanitizer_output(&policy(), source, sanitized).map(|findings| {
        findings
            .iter()
            .map(|finding| {
                format!(
                    "{}:{}:{}",
                    finding.class().as_str(),
                    finding.action().as_str(),
                    finding.location()
                )
            })
            .collect()
    })
}

#[test]
fn a_dropped_object_member_is_unattributed_rather_than_a_credential_assignment() {
    let source = json!({ "keep": "durable", "note": "also durable" });
    let sanitized = json!({ "keep": "durable" });
    assert_eq!(
        attribute(&source, &sanitized),
        Err(HygieneError::UnattributedRedaction)
    );
}

#[test]
fn a_renamed_object_member_is_unattributed() {
    let source = json!({ "note": "durable" });
    let sanitized = json!({ "renamed": "durable" });
    assert_eq!(
        attribute(&source, &sanitized),
        Err(HygieneError::UnattributedRedaction)
    );
}

#[test]
fn a_truncated_array_is_unattributed() {
    let source = json!({ "runs": ["one", "two"] });
    let sanitized = json!({ "runs": ["one"] });
    assert_eq!(
        attribute(&source, &sanitized),
        Err(HygieneError::UnattributedRedaction)
    );
}

#[test]
fn a_rewritten_string_carrying_no_canonical_marker_is_unattributed() {
    let source = json!({ "note": "Use pnpm rather than npm for installs in this repo" });
    let sanitized = json!({ "note": "Use npm rather than pnpm for installs in this repo" });
    assert_eq!(
        attribute(&source, &sanitized),
        Err(HygieneError::UnattributedRedaction)
    );
}

#[test]
fn a_retyped_value_is_unattributed() {
    let source = json!({ "revision": 3 });
    let sanitized = json!({ "revision": true });
    assert_eq!(
        attribute(&source, &sanitized),
        Err(HygieneError::UnattributedRedaction)
    );
}

#[test]
fn a_nested_unexplained_change_is_not_masked_by_an_explained_sibling() {
    let source = json!({
        "config": { "refresh_token": "abc" },
        "runs": ["one", "two"]
    });
    let sanitized = json!({
        "config": { "refresh_token": "[TraceDecay redacted: sensitive field]" },
        "runs": ["one"]
    });
    assert_eq!(
        attribute(&source, &sanitized),
        Err(HygieneError::UnattributedRedaction)
    );
}

#[test]
fn a_canonical_sensitive_field_replacement_is_attributed_to_that_class() {
    let source = json!({ "config": { "refresh_token": "abc" } });
    let sanitized =
        json!({ "config": { "refresh_token": "[TraceDecay redacted: sensitive field]" } });
    assert_eq!(
        attribute(&source, &sanitized).expect("attributed"),
        vec!["sensitive_field:redact:$.config.refresh_token".to_owned()]
    );
}

#[test]
fn a_pre_existing_marker_cannot_launder_a_fresh_redaction() {
    // The source already quotes the marker, so presence proves nothing; only a
    // rise in the number of markers does.
    let quoted = "the sanitizer writes [TraceDecay redacted: sensitive field] in place";
    let source = json!({ "note": quoted });
    let sanitized = json!({ "note": format!("{quoted} and dropped the rest") });
    assert_eq!(
        attribute(&source, &sanitized),
        Err(HygieneError::UnattributedRedaction)
    );
    let genuinely_redacted =
        json!({ "note": format!("{quoted} [TraceDecay redacted: sensitive field]") });
    assert_eq!(
        attribute(&source, &genuinely_redacted).expect("attributed"),
        vec!["sensitive_field:redact:$.note".to_owned()]
    );
}

#[test]
fn an_unchanged_payload_attributes_nothing() {
    let payload = json!({ "note": "Use pnpm rather than npm for installs in this repo" });
    assert!(
        attribute(&payload, &payload)
            .expect("attributed")
            .is_empty()
    );
}

#[test]
fn the_helper_attributes_what_admission_actually_records() {
    // The same helper admission uses, run over the real redactor's output: what
    // a fixture can inject is exactly what production attributes.
    let sanitizer = ObservationSanitizer::new().expect("canonical hygiene policy");
    let source = json!({
        "config": { "refresh_token": "abc", "endpoint": "https://example.invalid/v1" }
    });
    match sanitizer.admit(&source).expect("admission") {
        ObservationAdmission::Admitted { sanitized, receipt } => {
            assert_eq!(receipt.disposition(), SanitizationDisposition::Redacted);
            let attributed =
                attribute_sanitizer_output(&policy(), &source, &sanitized).expect("attributed");
            assert_eq!(attributed.len(), 1);
            assert_eq!(attributed[0].class(), HygieneClass::SensitiveField);
            assert_eq!(attributed[0].action(), HygieneAction::Redact);
            assert_eq!(
                u32::try_from(attributed.len()).expect("count fits"),
                receipt.finding_count()
            );
        }
        other => panic!("expected a redacted admission, got {other:?}"),
    }
}

#[test]
fn a_reject_floor_class_the_redactor_surfaces_withholds_instead_of_delivering() {
    // The canonical redactor runs a wider detector profile than the
    // classification scan, so it can prove a class the scan did not. Delivering
    // its rewritten bytes would ship a payload the policy says must never be
    // delivered, and erroring would lose the audit row, so admission withholds.
    let sanitizer = ObservationSanitizer::new().expect("canonical hygiene policy");
    // The memory profile requires a longer token after `bearer` than the
    // profile the redactor runs, so this string is invisible to Pass A and
    // rewritten by Pass B.
    let text = concat!("the gateway sends ", "Bearer abcdefghijklmnop");
    let payload = json!({ "note": text });
    assert!(
        sanitizer
            .classify(&payload)
            .expect("classification")
            .is_empty(),
        "the fixture no longer exercises a class only the redactor can see"
    );
    match sanitizer.admit(&payload).expect("admission") {
        ObservationAdmission::Withheld { reason, .. } => {
            assert_eq!(
                reason,
                tracedecay_memory_hygiene::WithheldReason::SecretRejected
            );
        }
        other => panic!("expected the payload to be withheld, got {other:?}"),
    }
}

#[test]
fn a_credential_bearing_key_stays_opaque_in_an_attribution_location() {
    let key = concat!("AKIA", "4S27TQXBVCZ5MJ6L");
    let source = json!({ key: { "refresh_token": "abc" } });
    let sanitized = json!({ key: { "refresh_token": "[TraceDecay redacted: sensitive field]" } });
    let attributed =
        attribute_sanitizer_output(&policy(), &source, &sanitized).expect("attributed");
    assert_eq!(attributed.len(), 1);
    assert!(
        !attributed[0].location().contains(key),
        "attribution echoed the credential-bearing key: {}",
        attributed[0].location()
    );
}
