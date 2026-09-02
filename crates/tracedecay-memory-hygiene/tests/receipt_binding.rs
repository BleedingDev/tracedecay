//! Acceptance criterion 3 — sanitization receipts bind the delivered payload.
//!
//! These tests run the receipt through the seam that actually enforces it: a
//! `ProviderCall` built over the admitted bytes validates, and the same receipt
//! attached to any other payload does not.
#![allow(clippy::expect_used, clippy::panic)]

use std::collections::BTreeSet;

use serde_json::{Value, json};
use tracedecay_domain::canonical_text::sha256_hex;
use tracedecay_memory_provider_api::{
    ApiError, CancellationToken, CanonicalPayload, OperationControl, OwnedExactScope,
    OwnedProviderId, OwnedVersionedId, PayloadSanitizationReceipt, ProviderCall, ProviderCallParts,
    ProviderOperation,
};

use tracedecay_memory_hygiene::{
    ObservationAdmission, ObservationSanitizer, SanitizationDisposition, canonical_payload_bytes,
};

const RESOLVED_SCOPE_DIGEST: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";

fn admitted(payload: &Value) -> (Value, PayloadSanitizationReceipt) {
    let sanitizer = ObservationSanitizer::new().expect("canonical hygiene policy");
    match sanitizer.admit(payload).expect("admission") {
        ObservationAdmission::Admitted { sanitized, receipt } => (sanitized, receipt),
        other => panic!("expected an admission, got {other:?}"),
    }
}

fn observe_call(
    sanitized: &Value,
    receipt: PayloadSanitizationReceipt,
) -> Result<ProviderCall, ApiError> {
    let bytes = canonical_payload_bytes(sanitized).expect("canonical bytes");
    let sha256 = sha256_hex(&bytes);
    let call = ProviderCall::new(ProviderCallParts {
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
        request_id: "request-hygiene".to_owned(),
        operation_id: "operation-hygiene".to_owned(),
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
        extensions: Vec::new(),
    })?;
    Ok(call.with_sanitization(receipt))
}

#[test]
fn an_admitted_payload_produces_a_call_that_validates() -> Result<(), ApiError> {
    let payload = json!({ "note": "Use pnpm rather than npm for installs in this repo" });
    let (sanitized, receipt) = admitted(&payload);
    receipt.validate()?;
    let call = observe_call(&sanitized, receipt)?;
    call.validate()?;
    Ok(())
}

#[test]
fn a_redacted_receipt_binds_the_delivered_bytes_not_the_source_bytes() -> Result<(), ApiError> {
    let payload = json!({ "note": "server started with pid 48213" });
    let source_digest = sha256_hex(&canonical_payload_bytes(&payload).expect("canonical bytes"));
    let (sanitized, receipt) = admitted(&payload);
    assert_eq!(receipt.disposition(), SanitizationDisposition::Redacted);
    assert_eq!(receipt.source_payload_sha256(), source_digest);
    assert_eq!(
        receipt.sanitized_payload_sha256(),
        sha256_hex(&canonical_payload_bytes(&sanitized).expect("canonical bytes"))
    );
    assert_ne!(
        receipt.sanitized_payload_sha256(),
        receipt.source_payload_sha256()
    );

    let call = observe_call(&sanitized, receipt.clone())?;
    call.validate()?;

    // The same receipt against the pre-sanitization bytes is refused: the
    // journal and the provider must see the same bytes the receipt names.
    let unbound = observe_call(&payload, receipt)?;
    assert_eq!(
        unbound.validate(),
        Err(ApiError::SanitizationReceiptUnbound)
    );
    Ok(())
}

#[test]
fn a_receipt_survives_the_journal_round_trip_and_re_attaches() -> Result<(), ApiError> {
    let payload = json!({ "config": { "refresh_token": "abc" }, "keep": "durable" });
    let (sanitized, receipt) = admitted(&payload);

    // What tdmem-0502 persists: an opaque JSON string it never parses.
    let receipt_json = receipt.to_json();
    let reconstructed = PayloadSanitizationReceipt::from_json(&receipt_json)?;
    assert_eq!(reconstructed, receipt);
    assert_eq!(reconstructed.receipt_id(), receipt.receipt_id());

    // A restarted dispatcher rebuilds the envelope and re-attaches the receipt.
    let call = observe_call(&sanitized, reconstructed)?;
    call.validate()?;
    assert_eq!(
        call.sanitization()
            .map(PayloadSanitizationReceipt::receipt_id),
        Some(receipt.receipt_id())
    );
    Ok(())
}

#[test]
fn an_observation_without_a_receipt_cannot_be_dispatched() -> Result<(), ApiError> {
    let payload = json!({ "note": "durable" });
    let bytes = canonical_payload_bytes(&payload).expect("canonical bytes");
    let sha256 = sha256_hex(&bytes);
    let call = ProviderCall::new(ProviderCallParts {
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
        request_id: "request-unsanitized".to_owned(),
        operation_id: "operation-unsanitized".to_owned(),
        expected_state_generation: 0,
        idempotency_key: Some("idempotency-unsanitized".to_owned()),
        control: OperationControl::new(i64::MAX, 100, CancellationToken::new()),
        payload: CanonicalPayload::new(
            OwnedVersionedId::new("tracedecay.memory.provider.observation.v1")?,
            bytes,
            sha256,
        )?,
        required_capabilities: vec![OwnedVersionedId::new(
            ProviderOperation::Observe.capability_id(),
        )?],
        extensions: Vec::new(),
    })?;
    assert_eq!(call.validate(), Err(ApiError::UnsanitizedObservation));
    assert!(call.required_capabilities.contains(&OwnedVersionedId::new(
        ProviderOperation::Observe.capability_id()
    )?));
    assert_eq!(
        call.required_capabilities,
        BTreeSet::from([OwnedVersionedId::new(
            ProviderOperation::Observe.capability_id()
        )?])
    );
    Ok(())
}

#[test]
fn withheld_identities_are_distinct_per_reason_and_per_source() {
    use tracedecay_memory_hygiene::{WithheldReason, withheld_receipt_id};

    let digest = sha256_hex(b"one");
    let other = sha256_hex(b"two");
    let empty = tracedecay_memory_provider_api::empty_findings_digest();
    let extensions = tracedecay_memory_provider_api::empty_opaque_extensions_digest();
    let other_extensions = sha256_hex(b"other-extensions");
    let base = withheld_receipt_id(
        "rev.v1",
        &digest,
        &extensions,
        WithheldReason::SecretRejected,
        1,
        &empty,
    );
    assert!(base.starts_with("obs-hygiene-withheld.v1."));
    assert_ne!(
        base,
        withheld_receipt_id(
            "rev.v1",
            &other,
            &extensions,
            WithheldReason::SecretRejected,
            1,
            &empty,
        )
    );
    assert_ne!(
        base,
        withheld_receipt_id(
            "rev.v1",
            &digest,
            &other_extensions,
            WithheldReason::SecretRejected,
            1,
            &empty,
        )
    );
    assert_ne!(
        base,
        withheld_receipt_id(
            "rev.v1",
            &digest,
            &extensions,
            WithheldReason::Quarantined,
            1,
            &empty,
        )
    );
    assert_ne!(
        base,
        withheld_receipt_id(
            "rev.v2",
            &digest,
            &extensions,
            WithheldReason::SecretRejected,
            1,
            &empty,
        )
    );
    assert_ne!(
        base,
        withheld_receipt_id(
            "rev.v1",
            &digest,
            &extensions,
            WithheldReason::SecretRejected,
            2,
            &empty,
        )
    );
}
