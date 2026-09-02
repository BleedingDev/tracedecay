//! The hygiene ceilings are derived from the canonical store contract.
//!
//! A record the host has already settled must always be *classifiable*: a
//! withheld or admitted answer, never a structural admission error that would
//! stall replay on evidence the store accepted. Only a shape no store would
//! have accepted may be refused, and a mounted journey turns even that refusal
//! into a typed, digests-only withheld terminal.
#![allow(clippy::expect_used, clippy::panic)]

use serde_json::{Value, json};
use tracedecay_domain::canonical_text::sha256_hex;
use tracedecay_domain::{MAX_OBSERVATION_RECORD_BYTES, MAX_OBSERVATION_STRUCTURE_DEPTH};
use tracedecay_memory_hygiene::{
    HygieneError, MAX_STRUCTURAL_DEPTH, OBSERVATION_HYGIENE_WITHHELD_ID_PREFIX,
    ObservationAdmission, ObservationSanitizer, SanitizationDisposition, WithheldReason,
    canonical_payload_bytes, withheld_receipt_id,
};
use tracedecay_memory_provider_api::{empty_findings_digest, observation_extensions_digest};

fn sanitizer() -> ObservationSanitizer {
    ObservationSanitizer::new().expect("canonical hygiene policy")
}

/// The envelope the production journey wraps around a canonical record before
/// hygiene walks it: the record sits one level below the envelope root.
fn provider_envelope(canonical_record: Value) -> Value {
    json!({
        "canonical_payload": canonical_record,
        "observation_kind": "session.message_committed.v1",
        "payload_contract": "tracedecay.memory.observation.session-message.v1",
    })
}

/// A leaf wrapped in `containers` nested arrays.
fn nested(containers: usize) -> Value {
    let mut value = json!("leaf");
    for _ in 0..containers {
        value = Value::Array(vec![value]);
    }
    value
}

/// Ordinary prose of exactly `len` bytes with no separator or long token, so
/// nothing in it is a credential candidate and the only thing under test is
/// its size.
fn benign_text(len: usize) -> String {
    let mut text = "fact ".repeat(len / 5);
    text.push_str(&"f".repeat(len % 5));
    assert_eq!(text.len(), len);
    text
}

fn expect_accepted_unchanged(envelope: &Value) {
    match sanitizer()
        .admit_observation(envelope, &[])
        .expect("a settled record is classifiable")
    {
        ObservationAdmission::Admitted { sanitized, receipt } => {
            assert_eq!(receipt.disposition(), SanitizationDisposition::Accepted);
            assert_eq!(&sanitized, envelope);
            assert_eq!(receipt.finding_count(), 0);
        }
        other => panic!("expected an accepted admission, got {other:?}"),
    }
}

/// The store counts the canonical root at depth 1 and refuses any value deeper
/// than `MAX_OBSERVATION_STRUCTURE_DEPTH`, so the deepest record it settles has
/// its leaf at exactly that depth. Inside the provider envelope that leaf sits
/// one level lower still, and hygiene must walk it without complaint.
#[test]
fn a_record_at_the_store_depth_ceiling_is_classified_inside_its_provider_envelope() {
    let deepest_settled_record = nested(MAX_OBSERVATION_STRUCTURE_DEPTH - 1);
    expect_accepted_unchanged(&provider_envelope(deepest_settled_record));
}

/// A canonical record whose canonical bytes equal `MAX_OBSERVATION_RECORD_BYTES`
/// is the largest the store settles; wrapped in the envelope it is larger than
/// the store ceiling and still under the hygiene ceiling.
#[test]
fn a_record_at_the_store_byte_ceiling_is_classified_inside_its_provider_envelope() {
    let overhead = canonical_payload_bytes(&json!({ "text": "" }))
        .expect("canonical bytes")
        .len();
    let record = json!({ "text": benign_text(MAX_OBSERVATION_RECORD_BYTES - overhead) });
    assert_eq!(
        canonical_payload_bytes(&record)
            .expect("canonical bytes")
            .len(),
        MAX_OBSERVATION_RECORD_BYTES,
        "the fixture must sit exactly on the store ceiling"
    );
    let envelope = provider_envelope(record);
    let envelope_len = canonical_payload_bytes(&envelope)
        .expect("canonical bytes")
        .len();
    assert!(envelope_len > MAX_OBSERVATION_RECORD_BYTES);
    assert!(envelope_len <= sanitizer().policy().max_canonical_bytes());
    expect_accepted_unchanged(&envelope);
}

/// Both ceilings dominate the canonical contract, and the structural error only
/// appears for a shape beyond it.
#[test]
fn only_shapes_beyond_the_canonical_contract_are_structural_refusals() {
    let sanitizer = sanitizer();
    const { assert!(MAX_STRUCTURAL_DEPTH > MAX_OBSERVATION_STRUCTURE_DEPTH) }
    assert!(sanitizer.policy().max_canonical_bytes() > MAX_OBSERVATION_RECORD_BYTES);

    // The last shape hygiene walks is one container short of its own ceiling;
    // the next one is refused — and it is already far past the store ceiling.
    expect_accepted_unchanged(&provider_envelope(nested(MAX_STRUCTURAL_DEPTH - 1)));
    const REFUSED_CONTAINERS: usize = MAX_STRUCTURAL_DEPTH;
    const { assert!(REFUSED_CONTAINERS + 1 > MAX_OBSERVATION_STRUCTURE_DEPTH) }
    assert_eq!(
        sanitizer.admit_observation(&provider_envelope(nested(REFUSED_CONTAINERS)), &[]),
        Err(HygieneError::PayloadTooDeep {
            maximum: MAX_STRUCTURAL_DEPTH
        })
    );

    let maximum = sanitizer.policy().max_canonical_bytes();
    let overhead = canonical_payload_bytes(&provider_envelope(json!({ "text": "" })))
        .expect("canonical bytes")
        .len();
    let just_over = provider_envelope(json!({ "text": benign_text(maximum + 1 - overhead) }));
    assert_eq!(
        canonical_payload_bytes(&just_over)
            .expect("canonical bytes")
            .len(),
        maximum + 1
    );
    assert_eq!(
        sanitizer.admit_observation(&just_over, &[]),
        Err(HygieneError::PayloadTooLarge { maximum })
    );
}

/// A mounted journey needs a typed terminal for a structural refusal, not a
/// stalled cursor. The terminal carries digests only, derives its identity the
/// way every other withheld admission does, and is minted for structural
/// refusals alone.
#[test]
fn a_structural_refusal_becomes_a_digests_only_unclassifiable_withheld_terminal() {
    let sanitizer = sanitizer();
    let too_deep = provider_envelope(nested(MAX_STRUCTURAL_DEPTH));
    let error = sanitizer
        .admit_observation(&too_deep, &[])
        .expect_err("beyond the ceiling");
    let source_payload_sha256 =
        sha256_hex(&canonical_payload_bytes(&too_deep).expect("canonical bytes"));
    let extensions_digest = observation_extensions_digest(&[]).expect("empty extension set");

    match sanitizer
        .withhold_unclassifiable(&too_deep, &[], error)
        .expect("a structural refusal has a withheld terminal")
    {
        ObservationAdmission::Withheld {
            reason,
            receipt_id,
            source_payload_sha256: recorded_source,
            extensions_digest: recorded_extensions,
            sanitizer_revision,
            finding_count,
            findings_digest,
        } => {
            assert_eq!(reason, WithheldReason::UnclassifiablePayload);
            assert_eq!(recorded_source, source_payload_sha256);
            assert_eq!(recorded_extensions, extensions_digest);
            assert_eq!(sanitizer_revision, sanitizer.revision());
            assert_eq!(finding_count, 0, "nothing was classified");
            assert_eq!(findings_digest, empty_findings_digest());
            assert!(receipt_id.starts_with(OBSERVATION_HYGIENE_WITHHELD_ID_PREFIX));
            assert_eq!(
                receipt_id,
                withheld_receipt_id(
                    sanitizer.revision(),
                    &source_payload_sha256,
                    &extensions_digest,
                    WithheldReason::UnclassifiablePayload,
                    0,
                    &findings_digest,
                ),
                "the identity must be re-derivable from the audit row alone"
            );
            assert_ne!(
                receipt_id,
                withheld_receipt_id(
                    sanitizer.revision(),
                    &source_payload_sha256,
                    &extensions_digest,
                    WithheldReason::SecretRejected,
                    0,
                    &findings_digest,
                ),
                "the reason is part of the identity"
            );
        }
        other => panic!("expected a withheld admission, got {other:?}"),
    }

    let maximum = sanitizer.policy().max_canonical_bytes();
    let too_large = provider_envelope(json!({ "text": benign_text(maximum) }));
    let error = sanitizer
        .admit_observation(&too_large, &[])
        .expect_err("beyond the ceiling");
    match sanitizer
        .withhold_unclassifiable(&too_large, &[], error)
        .expect("a structural refusal has a withheld terminal")
    {
        ObservationAdmission::Withheld {
            reason,
            source_payload_sha256,
            finding_count,
            ..
        } => {
            assert_eq!(reason, WithheldReason::UnclassifiablePayload);
            assert_eq!(
                source_payload_sha256,
                sha256_hex(&canonical_payload_bytes(&too_large).expect("canonical bytes"))
            );
            assert_eq!(finding_count, 0);
        }
        other => panic!("expected a withheld admission, got {other:?}"),
    }

    // Anything that is not a structural refusal keeps failing closed: a
    // detector fault has proven nothing and must be retried, and a caller bug
    // must stay visible as the error it is.
    for error in [
        HygieneError::TransientCorpusUnavailable,
        HygieneError::UnattributedRedaction,
        HygieneError::RequiredExtensionUnsupported { index: 0 },
        HygieneError::CanonicalEncoding,
    ] {
        let expected = error.clone();
        assert_eq!(
            sanitizer.withhold_unclassifiable(&json!({ "ok": true }), &[], error),
            Err(expected)
        );
    }
}
