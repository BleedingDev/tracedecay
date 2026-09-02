//! Acceptance criterion 4 — ordinary code facts stay accepted.
//!
//! Each fixture asserts the strongest form of "unmodified": the admitted value
//! is `==` to the input, the canonical bytes are identical, and the receipt
//! says `accepted`. A future credential-corpus refresh that starts flagging one
//! of these fails loudly here rather than silently mangling durable facts in
//! production.
#![allow(clippy::expect_used, clippy::panic)]

use serde_json::Value;
use tracedecay_memory_hygiene::{
    HygieneAction, HygieneClass, ObservationAdmission, ObservationHygienePolicyV1,
    ObservationSanitizer, SanitizationDisposition, canonical_payload_bytes,
};

const ORDINARY_CODE_FACTS: &str = include_str!("fixtures/ordinary_code_facts.json");

fn cases() -> Vec<(String, Value)> {
    let document: Value = serde_json::from_str(ORDINARY_CODE_FACTS).expect("fixture json");
    let rows = document["cases"].as_array().expect("fixture cases");
    assert!(rows.len() >= 12, "the false-positive corpus is too thin");
    rows.iter()
        .map(|row| {
            (
                row["id"].as_str().expect("case id").to_owned(),
                row["payload"].clone(),
            )
        })
        .collect()
}

fn assert_accepted_byte_identical(id: &str, payload: &Value, sanitizer: &ObservationSanitizer) {
    let source_bytes = canonical_payload_bytes(payload).expect("canonical bytes");
    match sanitizer.admit(payload).expect("admission") {
        ObservationAdmission::Admitted { sanitized, receipt } => {
            assert_eq!(
                receipt.disposition(),
                SanitizationDisposition::Accepted,
                "{id} was modified by the pipeline"
            );
            assert_eq!(&sanitized, payload, "{id} did not round-trip unchanged");
            assert_eq!(
                canonical_payload_bytes(&sanitized).expect("canonical bytes"),
                source_bytes,
                "{id} was re-encoded"
            );
            assert_eq!(
                receipt.source_payload_sha256(),
                receipt.sanitized_payload_sha256(),
                "{id} claimed accepted with differing digests"
            );
        }
        other => panic!("{id} must be admitted, got {other:?}"),
    }
}

#[test]
fn ordinary_code_facts_are_admitted_byte_identical() {
    let sanitizer = ObservationSanitizer::new().expect("canonical hygiene policy");
    for (id, payload) in cases() {
        assert_accepted_byte_identical(&id, &payload, &sanitizer);
    }
}

#[test]
fn ordinary_code_facts_raise_only_annotate_level_findings() {
    let sanitizer = ObservationSanitizer::new().expect("canonical hygiene policy");
    for (id, payload) in cases() {
        for finding in sanitizer.classify(&payload).expect("classification") {
            assert!(
                finding.action() <= HygieneAction::Annotate,
                "{id} raised {finding:?}, which would rewrite or withhold a durable fact"
            );
        }
    }
}

#[test]
fn documented_bind_address_and_run_log_prose_are_annotated_not_rewritten() {
    let sanitizer = ObservationSanitizer::new().expect("canonical hygiene policy");
    let annotated: Vec<(String, HygieneClass)> = cases()
        .into_iter()
        .flat_map(|(id, payload)| {
            sanitizer
                .classify(&payload)
                .expect("classification")
                .into_iter()
                .map(move |finding| (id.clone(), finding.class()))
        })
        .collect();
    assert!(
        annotated
            .iter()
            .any(|(id, class)| id == "documented_bind_address"
                && *class == HygieneClass::TransientEphemeralPort),
        "the corpus no longer exercises the annotate path for ports: {annotated:?}"
    );
    assert!(
        annotated
            .iter()
            .any(|(id, class)| id == "run_log_phrasing_in_prose"
                && *class == HygieneClass::TransientRunLog),
        "the corpus no longer exercises the annotate path for run logs: {annotated:?}"
    );
}

#[test]
fn the_corpus_survives_a_maximally_hardened_policy_without_being_withheld() {
    // Raising every non-floor class as far as the ladder allows must still not
    // withhold a durable fact: hardening may rewrite more, never reject more.
    let hardened = ObservationHygienePolicyV1::with_overrides(&[
        (HygieneClass::TransientEphemeralPort, HygieneAction::Redact),
        (HygieneClass::TransientRunLog, HygieneAction::Redact),
    ])
    .expect("raising severity is allowed");
    let sanitizer = ObservationSanitizer::with_policy(hardened);
    for (id, payload) in cases() {
        let outcome = sanitizer.admit(&payload).expect("admission");
        assert!(
            matches!(outcome, ObservationAdmission::Admitted { .. }),
            "{id} was withheld under a hardened policy: {outcome:?}"
        );
    }
}

#[test]
fn admission_is_reproducible() {
    let sanitizer = ObservationSanitizer::new().expect("canonical hygiene policy");
    for (id, payload) in cases() {
        let first = sanitizer.admit(&payload).expect("admission");
        let second = sanitizer.admit(&payload).expect("admission");
        assert_eq!(first, second, "{id} did not admit deterministically");
    }
}
