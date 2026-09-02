//! Acceptance criterion 1 — known secret classes are rejected or redacted.
//!
//! Every credential literal here is assembled from fragments with `concat!` so
//! this file is not itself a corpus of secret-shaped strings for a scanner to
//! find. None of them is a live credential.
#![allow(clippy::expect_used, clippy::panic)]

use serde_json::{Value, json};
use tracedecay_memory_hygiene::{
    HygieneAction, HygieneClass, ObservationAdmission, ObservationSanitizer,
    SanitizationDisposition, WithheldReason, canonical_payload_bytes,
};

fn sanitizer() -> ObservationSanitizer {
    ObservationSanitizer::new().expect("canonical hygiene policy")
}

fn admit(payload: &Value) -> ObservationAdmission {
    sanitizer().admit(payload).expect("admission")
}

fn assert_withheld(payload: &Value, expected: WithheldReason, expected_class: HygieneClass) {
    let sanitizer = sanitizer();
    let findings = sanitizer.classify(payload).expect("classification");
    assert!(
        findings
            .iter()
            .any(|finding| finding.class() == expected_class),
        "expected a {expected_class:?} finding, got {findings:?}"
    );
    match sanitizer.admit(payload).expect("admission") {
        ObservationAdmission::Withheld {
            reason,
            receipt_id,
            source_payload_sha256,
            extensions_digest,
            sanitizer_revision,
            finding_count,
            findings_digest,
        } => {
            assert_eq!(reason, expected);
            assert!(receipt_id.starts_with("obs-hygiene-withheld.v1."));
            let expected_digest = tracedecay_domain::canonical_text::sha256_hex(
                &canonical_payload_bytes(payload).expect("canonical bytes"),
            );
            assert_eq!(
                source_payload_sha256, expected_digest,
                "a withheld admission must point back at untouched evidence"
            );
            assert_eq!(
                finding_count,
                u32::try_from(findings.len()).unwrap_or(u32::MAX)
            );
            assert_eq!(
                findings_digest,
                tracedecay_memory_hygiene::findings_digest(&findings)
            );
            assert_eq!(
                receipt_id,
                tracedecay_memory_hygiene::withheld_receipt_id(
                    &sanitizer_revision,
                    &source_payload_sha256,
                    &extensions_digest,
                    reason,
                    finding_count,
                    &findings_digest,
                )
            );
        }
        other => panic!("expected the payload to be withheld, got {other:?}"),
    }
}

#[test]
fn pem_private_key_is_rejected() {
    let payload = json!({
        "note": concat!("-----BEGIN RSA ", "PRIVATE KEY-----\nnot-a-real-key\n-----END RSA ", "PRIVATE KEY-----")
    });
    assert_withheld(
        &payload,
        WithheldReason::SecretRejected,
        HygieneClass::PrivateKey,
    );
}

#[test]
fn known_credential_prefixes_are_rejected() {
    let cases = [
        json!({ "note": concat!("AKIA", "4S27TQXBVCZ5MJ6L is the access key") }),
        json!({ "note": concat!("ghp_", "KsY7QwT2mZ4bV9nR6cX1jH8pL3dG5fA0eUwQ") }),
        json!({ "note": concat!("sk-", "proj1234567890abcdefghijklmn") }),
    ];
    for payload in cases {
        assert_withheld(
            &payload,
            WithheldReason::SecretRejected,
            HygieneClass::KnownCredentialPrefix,
        );
    }
}

#[test]
fn bearer_token_is_rejected() {
    let payload = json!({
        "header": concat!("Authorization: ", "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9")
    });
    assert_withheld(
        &payload,
        WithheldReason::SecretRejected,
        HygieneClass::BearerToken,
    );
}

#[test]
fn high_entropy_token_is_rejected_because_no_exact_span_is_provable() {
    let payload = json!({
        "note": "value Qm9vZ2llV29vZ2llMTIzNDU2Nzg5MGFiY2RlZmdoaWprbG1ub3A4OTc2NTQzMjE"
    });
    assert_withheld(
        &payload,
        WithheldReason::SecretRejected,
        HygieneClass::HighEntropyToken,
    );
}

#[test]
fn credential_material_inside_an_object_key_is_rejected_without_echoing_the_key() {
    let key = concat!("AKIA", "4S27TQXBVCZ5MJ6L");
    let payload = json!({ key: "value" });
    let findings = sanitizer().classify(&payload).expect("classification");
    assert_eq!(findings.len(), 2);
    assert!(
        findings
            .iter()
            .any(|finding| finding.class() == HygieneClass::KnownCredentialPrefix)
    );
    assert!(
        findings
            .iter()
            .any(|finding| finding.class() == HygieneClass::CredentialBearingKey)
    );
    let expected_location = format!(
        "$.{}",
        tracedecay_memory_hygiene::credential_bearing_key_marker(key)
    );
    assert!(
        findings
            .iter()
            .all(|finding| finding.location() == expected_location)
    );
    assert!(
        findings
            .iter()
            .all(|finding| !finding.location().contains(key)),
        "a finding must never echo the material it names"
    );
    assert_withheld(
        &payload,
        WithheldReason::SecretRejected,
        HygieneClass::KnownCredentialPrefix,
    );
}

#[test]
fn credential_assignment_is_redacted_rather_than_withheld() {
    let secret = concat!("api_", "key=", "0000000000000000");
    let payload = json!({ "env": secret, "keep": "this durable sibling" });
    match admit(&payload) {
        ObservationAdmission::Admitted { sanitized, receipt } => {
            assert_eq!(receipt.disposition(), SanitizationDisposition::Redacted);
            assert_ne!(
                receipt.sanitized_payload_sha256(),
                receipt.source_payload_sha256()
            );
            let encoded = serde_json::to_string(&sanitized).expect("sanitized json");
            assert!(
                !encoded.contains(secret),
                "the assignment survived: {encoded}"
            );
            assert_eq!(sanitized["keep"], json!("this durable sibling"));
        }
        other => panic!("expected a redacted admission, got {other:?}"),
    }
}

#[test]
fn credential_assignment_inside_an_object_key_is_quarantined() {
    let key = concat!("api_", "key=", "0000000000000000");
    let payload = json!({ key: { "note": "server started with pid 48213" } });
    let findings = sanitizer().classify(&payload).expect("classification");
    assert!(
        findings
            .iter()
            .any(|finding| finding.class() == HygieneClass::CredentialAssignment)
    );
    assert!(
        findings
            .iter()
            .any(|finding| finding.class() == HygieneClass::CredentialBearingKey)
    );
    assert!(
        findings
            .iter()
            .all(|finding| !finding.location().contains(key)),
        "a finding must never echo the credential-bearing key"
    );
    assert_withheld(
        &payload,
        WithheldReason::Quarantined,
        HygieneClass::CredentialBearingKey,
    );
}

#[test]
fn key_proven_sensitive_field_is_redacted_and_siblings_survive() {
    let payload = json!({
        "config": { "refresh_token": "abc", "endpoint": "https://example.invalid/v1" }
    });
    match admit(&payload) {
        ObservationAdmission::Admitted { sanitized, receipt } => {
            assert_eq!(receipt.disposition(), SanitizationDisposition::Redacted);
            assert_eq!(receipt.finding_count(), 1);
            assert_ne!(sanitized["config"]["refresh_token"], json!("abc"));
            assert_eq!(
                sanitized["config"]["endpoint"],
                json!("https://example.invalid/v1")
            );
        }
        other => panic!("expected a redacted admission, got {other:?}"),
    }
}

#[test]
fn oversize_and_over_deep_payloads_are_admission_errors_not_silent_admissions() {
    let sanitizer = sanitizer();
    let filler = "a".repeat(sanitizer.policy().max_canonical_bytes() + 1);
    let oversize = json!({ "note": filler });
    assert!(
        matches!(
            sanitizer.admit(&oversize),
            Err(tracedecay_memory_hygiene::HygieneError::PayloadTooLarge { .. })
        ),
        "an oversize payload must fail closed before any pattern runs"
    );

    let mut deep = json!("leaf");
    for _ in 0..(tracedecay_memory_hygiene::MAX_STRUCTURAL_DEPTH + 2) {
        deep = Value::Array(vec![deep]);
    }
    assert!(matches!(
        sanitizer.admit(&deep),
        Err(tracedecay_memory_hygiene::HygieneError::PayloadTooDeep { .. })
    ));
}

#[test]
fn no_finding_receipt_or_error_ever_carries_matched_bytes() {
    let secrets = [
        concat!("AKIA", "4S27TQXBVCZ5MJ6L"),
        concat!("ghp_", "KsY7QwT2mZ4bV9nR6cX1jH8pL3dG5fA0eUwQ"),
        concat!("api_", "key=", "0000000000000000"),
        "Qm9vZ2llV29vZ2llMTIzNDU2Nzg5MGFiY2RlZmdoaWprbG1ub3A4OTc2NTQzMjE",
    ];
    let sanitizer = sanitizer();
    for secret in secrets {
        let payload = json!({ "note": format!("value {secret}") });
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
            ObservationAdmission::Admitted { sanitized, receipt } => {
                let rendered = format!("{receipt:?}");
                assert!(!rendered.contains(secret), "a receipt echoed material");
                let encoded = serde_json::to_string(&sanitized).expect("sanitized json");
                assert!(!encoded.contains(secret), "the payload kept the material");
            }
        }
    }
}

#[test]
fn reject_floor_classes_cannot_be_lowered_and_overrides_may_only_raise_severity() {
    use tracedecay_memory_hygiene::{ObservationHygienePolicyV1, PolicyError};

    for class in [
        HygieneClass::PrivateKey,
        HygieneClass::BearerToken,
        HygieneClass::KnownCredentialPrefix,
        HygieneClass::HighEntropyToken,
        HygieneClass::DetectorUnavailable,
    ] {
        assert!(
            ObservationHygienePolicyV1::canonical()
                .expect("canonical policy")
                .is_reject_floor(class),
            "{class:?} must sit on the reject floor"
        );
        assert_eq!(
            ObservationHygienePolicyV1::with_overrides(&[(class, HygieneAction::Redact)]),
            Err(PolicyError::SeverityDowngrade {
                class: class.as_str()
            })
        );
    }

    assert_eq!(
        ObservationHygienePolicyV1::with_overrides(&[(
            HygieneClass::SensitiveField,
            HygieneAction::Annotate
        )]),
        Err(PolicyError::SeverityDowngrade {
            class: "sensitive_field"
        })
    );

    let hardened = ObservationHygienePolicyV1::with_overrides(&[(
        HygieneClass::TransientEphemeralPort,
        HygieneAction::Redact,
    )])
    .expect("raising severity is allowed");
    assert_eq!(
        hardened.action(HygieneClass::TransientEphemeralPort),
        HygieneAction::Redact
    );
    assert_ne!(
        hardened.revision(),
        ObservationHygienePolicyV1::canonical()
            .expect("canonical policy")
            .revision(),
        "an override must be visible to anyone reading a receipt"
    );

    // The hardened table rewrites what the canonical table only annotates.
    let payload = json!({ "note": "dashboard listening on 127.0.0.1:43817 for this run" });
    match ObservationSanitizer::with_policy(hardened)
        .admit(&payload)
        .expect("admission")
    {
        ObservationAdmission::Admitted { sanitized, receipt } => {
            assert_eq!(receipt.disposition(), SanitizationDisposition::Redacted);
            assert!(
                !serde_json::to_string(&sanitized)
                    .expect("sanitized json")
                    .contains("43817")
            );
        }
        other => panic!("expected a redacted admission, got {other:?}"),
    }
}
