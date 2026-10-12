//! Observation extensions are inspected before any hygiene receipt is minted.
#![allow(clippy::expect_used, clippy::panic)]

use serde_json::{Value, json};
use tracedecay_domain::canonical_text::sha256_hex;
use tracedecay_memory_hygiene::{
    HygieneError, ObservationAdmission, ObservationSanitizer, SanitizationDisposition,
    WithheldReason, canonical_payload_bytes,
};
use tracedecay_memory_provider_api::{
    ApiError, CancellationToken, CanonicalPayload, OperationControl, OwnedExactScope,
    OwnedOpaqueExtension, OwnedProviderId, OwnedVersionedId, PayloadSanitizationReceipt,
    ProviderCall, ProviderCallParts, ProviderOperation, observation_extensions_digest,
};

const RESOLVED_SCOPE_DIGEST: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";

fn extension(id: &str, required: bool, bytes: Vec<u8>) -> OwnedOpaqueExtension {
    let digest = sha256_hex(&bytes);
    OwnedOpaqueExtension::new(
        OwnedVersionedId::new(id).expect("extension id"),
        1,
        required,
        digest,
        bytes,
    )
    .expect("extension")
}

fn json_extension(id: &str, required: bool, value: &Value) -> OwnedOpaqueExtension {
    extension(
        id,
        required,
        canonical_payload_bytes(value).expect("canonical extension json"),
    )
}

fn admitted_with_extensions(
    payload: &Value,
    extensions: &[OwnedOpaqueExtension],
) -> (Value, PayloadSanitizationReceipt) {
    match ObservationSanitizer::new()
        .expect("sanitizer")
        .admit_observation(payload, extensions)
        .expect("admission")
    {
        ObservationAdmission::Admitted { sanitized, receipt } => (sanitized, receipt),
        other => panic!("expected admitted observation, got {other:?}"),
    }
}

fn observe_call(
    sanitized: &Value,
    extensions: Vec<OwnedOpaqueExtension>,
    receipt: PayloadSanitizationReceipt,
) -> Result<ProviderCall, ApiError> {
    let bytes = canonical_payload_bytes(sanitized).expect("canonical payload");
    let sha256 = sha256_hex(&bytes);
    Ok(ProviderCall::new(ProviderCallParts {
        operation: ProviderOperation::Observe,
        provider_id: OwnedProviderId::new("test.provider")?,
        registration_revision: 1,
        ready_receipt_sha256: "a".repeat(64),
        exact_scope: OwnedExactScope::new(
            "profile-1",
            "project-1",
            "repo-1",
            "worktree-1",
            "refs/heads/main",
            "session-1",
            RESOLVED_SCOPE_DIGEST,
        )?,
        request_id: "request-extension-hygiene".to_owned(),
        operation_id: "operation-extension-hygiene".to_owned(),
        expected_state_generation: 0,
        idempotency_key: Some(format!("idempotency-{sha256}")),
        control: OperationControl::new(i64::MAX, 100, CancellationToken::new()),
        payload: CanonicalPayload::new(
            OwnedVersionedId::new("tracedecay.memory.provider.observation.v1")?,
            bytes,
            sha256,
        )?,
        required_capabilities: vec![OwnedVersionedId::new(
            ProviderOperation::Observe.capability_id(),
        )?],
        extensions,
    })?
    .with_sanitization(receipt))
}

#[test]
fn safe_optional_extension_is_inspected_and_dispatched_unchanged() -> Result<(), ApiError> {
    let payload = json!({"message": "durable"});
    let extension = json_extension(
        "tracedecay.memory.extension.safe.v1",
        false,
        &json!({"language": "rust", "priority": 2}),
    );
    let original_bytes = extension.canonical_payload.clone();
    let extensions = vec![extension];
    let (sanitized, receipt) = admitted_with_extensions(&payload, &extensions);

    assert_eq!(receipt.disposition(), SanitizationDisposition::Accepted);
    assert_eq!(
        receipt.extensions_digest(),
        observation_extensions_digest(&extensions)?
    );
    assert_eq!(extensions[0].canonical_payload, original_bytes);
    observe_call(&sanitized, extensions, receipt)?.validate()?;
    Ok(())
}

#[test]
fn dispatch_rejects_any_extension_set_other_than_the_one_inspected() -> Result<(), ApiError> {
    let payload = json!({"message": "durable"});
    let inspected = vec![json_extension(
        "tracedecay.memory.extension.safe.v1",
        false,
        &json!({"value": 1}),
    )];
    let replacement = vec![json_extension(
        "tracedecay.memory.extension.safe.v1",
        false,
        &json!({"value": 2}),
    )];
    let (sanitized, receipt) = admitted_with_extensions(&payload, &inspected);

    observe_call(&sanitized, inspected, receipt.clone())?.validate()?;
    assert_eq!(
        observe_call(&sanitized, replacement, receipt)?.validate(),
        Err(ApiError::SanitizationReceiptUnbound)
    );
    Ok(())
}

#[test]
fn secret_in_optional_extension_withholds_the_whole_observation() {
    let payload = json!({"message": "durable"});
    let secret = concat!("ghp_", "KsY7QwT2mZ4bV9nR6cX1jH8pL3dG5fA0eUwQ");
    let extensions = vec![json_extension(
        "tracedecay.memory.extension.secret.v1",
        false,
        &json!({"note": secret}),
    )];

    match ObservationSanitizer::new()
        .expect("sanitizer")
        .admit_observation(&payload, &extensions)
        .expect("admission")
    {
        ObservationAdmission::Withheld {
            reason,
            receipt_id,
            extensions_digest,
            finding_count,
            findings_digest,
            ..
        } => {
            assert_eq!(reason, WithheldReason::SecretRejected);
            assert_eq!(
                extensions_digest,
                observation_extensions_digest(&extensions).expect("extension digest")
            );
            assert!(finding_count > 0);
            assert!(!receipt_id.contains(secret));
            assert!(!findings_digest.contains(secret));
        }
        other => panic!("expected withheld observation, got {other:?}"),
    }
}

#[test]
fn extension_requiring_redaction_is_withheld_instead_of_mutated() {
    let payload = json!({"message": "durable"});
    let extension = json_extension(
        "tracedecay.memory.extension.sensitive.v1",
        false,
        &json!({"refresh_token": "ordinary-value"}),
    );
    let original_bytes = extension.canonical_payload.clone();

    match ObservationSanitizer::new()
        .expect("sanitizer")
        .admit_observation(&payload, std::slice::from_ref(&extension))
        .expect("admission")
    {
        ObservationAdmission::Withheld { reason, .. } => {
            assert_eq!(reason, WithheldReason::Quarantined);
        }
        other => panic!("expected withheld observation, got {other:?}"),
    }
    assert_eq!(extension.canonical_payload, original_bytes);
}

#[test]
fn malformed_noncanonical_and_required_extensions_fail_before_receipt_minting() {
    let sanitizer = ObservationSanitizer::new().expect("sanitizer");
    let payload = json!({"message": "durable"});
    let malformed = extension(
        "tracedecay.memory.extension.malformed.v1",
        false,
        b"not-json".to_vec(),
    );
    assert_eq!(
        sanitizer.admit_observation(&payload, &[malformed]),
        Err(HygieneError::InvalidExtensionJson { index: 0 })
    );

    let noncanonical = extension(
        "tracedecay.memory.extension.noncanonical.v1",
        false,
        br#"{"b":1,"a":2}"#.to_vec(),
    );
    assert_eq!(
        sanitizer.admit_observation(&payload, &[noncanonical]),
        Err(HygieneError::NonCanonicalExtensionJson { index: 0 })
    );

    let required = json_extension(
        "tracedecay.memory.extension.required.v1",
        true,
        &json!({"value": 1}),
    );
    assert_eq!(
        sanitizer.admit_observation(&payload, &[required]),
        Err(HygieneError::RequiredExtensionUnsupported { index: 0 })
    );
}

#[test]
fn unordered_and_duplicate_extension_sets_fail_at_the_shared_boundary() {
    let sanitizer = ObservationSanitizer::new().expect("sanitizer");
    let payload = json!({"message": "durable"});
    let first = json_extension("tracedecay.memory.extension.a.v1", false, &json!({"a": 1}));
    let second = json_extension("tracedecay.memory.extension.b.v1", false, &json!({"b": 2}));

    assert!(matches!(
        sanitizer.admit_observation(&payload, &[second, first.clone()]),
        Err(HygieneError::ExtensionBoundary(
            ApiError::UnorderedExtensions
        ))
    ));
    assert!(matches!(
        sanitizer.admit_observation(&payload, &[first.clone(), first]),
        Err(HygieneError::ExtensionBoundary(
            ApiError::UnorderedExtensions
        ))
    ));
}

#[test]
fn extension_evidence_changes_with_location_and_exact_unsafe_set() {
    let sanitizer = ObservationSanitizer::new().expect("sanitizer");
    let payload = json!({"message": "durable"});
    let safe = json_extension("tracedecay.memory.extension.a.v1", false, &json!({"a": 1}));
    let secret = concat!("ghp_", "KsY7QwT2mZ4bV9nR6cX1jH8pL3dG5fA0eUwQ");
    let unsafe_extension = json_extension(
        "tracedecay.memory.extension.b.v1",
        false,
        &json!({"note": secret}),
    );
    let only_unsafe = vec![unsafe_extension.clone()];
    let safe_then_unsafe = vec![safe, unsafe_extension];

    let evidence = |extensions: &[OwnedOpaqueExtension]| match sanitizer
        .admit_observation(&payload, extensions)
        .expect("admission")
    {
        ObservationAdmission::Withheld {
            receipt_id,
            finding_count,
            findings_digest,
            ..
        } => (receipt_id, finding_count, findings_digest),
        other => panic!("expected withheld observation, got {other:?}"),
    };
    let first = evidence(&only_unsafe);
    let second = evidence(&safe_then_unsafe);
    assert_ne!(first.0, second.0);
    assert_ne!(first.2, second.2);
    assert_eq!(first.1, second.1);
}
