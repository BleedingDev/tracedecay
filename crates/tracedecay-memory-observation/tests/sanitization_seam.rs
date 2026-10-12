//! The tdmem-0507 hygiene seam, enforced rather than merely carried.
//!
//! The normative ordering is: sanitize at admission, derive digests over the
//! sanitized payload, append the sanitized bytes, dispatch those same bytes.
//! These tests prove the journal holds up its end — the binding is mandatory,
//! it is reparsed and checked against the payload it claims to describe, it is
//! covered by the envelope digest, delivered bytes are journal bytes, the
//! pre-sanitization payload never lands, and the binding round-trips across a
//! restart so a recovered dispatcher can re-attach the receipt.

mod support;

use support::{
    Builder, SANITIZER_REVISION, T0, TestResult, accepted_binding_for, applied_receipt,
    binding_for, digest_hex, extension, journal, lease_request, receipt_json, seal,
};

use tracedecay_memory_observation::{
    MAX_SANITIZATION_RECEIPT_JSON_BYTES, ObservationDispatchPortV1, ObservationJournalError,
    ObservationJournalReaderV1, SanitizationBindingV1, extensions_digest,
};
use tracedecay_memory_provider_api::{
    ApiError, PayloadSanitizationReceiptParts, SanitizationDisposition,
    empty_opaque_extensions_digest,
};

const BODY: &str = "{\"message\":\"hello-1\"}";

#[test]
fn the_sanitization_binding_round_trips_through_a_restart() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    let admitted = Builder::at_sequence(1).build()?;
    let binding = admitted.sanitization.clone();
    {
        let store = journal(&path)?;
        store.append_admitted(&admitted)?;
    }

    let store = journal(&path)?;
    let leased = store.lease_pending(&lease_request(T0, 4))?;
    assert_eq!(leased.len(), 1);
    assert_eq!(leased[0].sanitization, binding);
    // The receipt is returned byte-for-byte, so a restarted dispatcher
    // re-attaches the receipt that was actually minted.
    assert_eq!(leased[0].sanitization.receipt_json, binding.receipt_json);
    Ok(())
}

#[test]
fn delivered_bytes_are_journal_bytes() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let admitted = Builder::at_sequence(1).build()?;
    store.append_admitted(&admitted)?;

    let leased = store.lease_pending(&lease_request(T0, 4))?;
    // What a dispatcher sends is exactly what was journalled, so a provider
    // deduplicating on payload_sha256 sees the digest the receipt carries.
    assert_eq!(leased[0].payload.bytes, admitted.payload.bytes);
    assert_eq!(leased[0].payload.sha256, admitted.payload.sha256);

    let receipt = applied_receipt(&leased[0], T0);
    assert_eq!(receipt.payload_sha256, admitted.payload.sha256);
    store.record_attempt(&receipt)?;
    Ok(())
}

#[test]
fn extension_bytes_are_part_of_the_mandatory_hygiene_binding() -> TestResult {
    let original = extension("vendor.safe-context.v1", r#"{"note":"safe"}"#)?;
    let mut admitted = Builder {
        extensions: vec![original],
        ..Builder::at_sequence(1)
    }
    .build()?;
    admitted.validate()?;

    admitted.extensions = vec![extension(
        "vendor.safe-context.v1",
        r#"{"note":"different"}"#,
    )?];
    admitted.extensions_digest = extensions_digest(&admitted.extensions)?;
    seal(&mut admitted);
    assert!(matches!(
        admitted.validate(),
        Err(ObservationJournalError::Api(
            ApiError::SanitizationReceiptUnbound
        ))
    ));
    Ok(())
}

#[test]
fn the_pre_sanitization_payload_never_reaches_the_journal() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    let admitted = Builder::at_sequence(1).build()?;
    let store = journal(&path)?;
    store.append_admitted(&admitted)?;
    drop(store);

    let connection = rusqlite::Connection::open(&path)?;
    // Only a digest of the pre-sanitization payload is at rest; the bytes are
    // nowhere in the schema.
    let stored: String = connection.query_row(
        "SELECT source_payload_sha256 FROM tdmem_observation_journal_v1",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(stored, admitted.sanitization.source_payload_sha256);
    assert_ne!(stored, admitted.payload.sha256, "the fixture must redact");
    let payload_columns: i64 = connection.query_row(
        "SELECT COUNT(*) FROM pragma_table_info('tdmem_observation_journal_v1') \
         WHERE name IN ('raw_payload_bytes', 'source_payload_bytes', 'unsanitized_payload')",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(payload_columns, 0);
    Ok(())
}

#[test]
fn the_leased_item_carries_every_signal_a_sanitizer_audit_needs() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let admitted = Builder::at_sequence(1).build()?;
    store.append_admitted(&admitted)?;
    let leased = store.lease_pending(&lease_request(T0, 4))?;
    let item = &leased[0];
    assert_eq!(item.privacy.classification, admitted.privacy.classification);
    assert_eq!(
        item.privacy.retention_class,
        admitted.privacy.retention_class
    );
    assert_eq!(
        item.privacy.redaction_revision,
        admitted.privacy.redaction_revision
    );
    assert_eq!(
        item.privacy.content_policy_revision,
        admitted.privacy.content_policy_revision
    );
    assert_eq!(item.provenance_origin, admitted.provenance_origin);
    assert_eq!(item.sanitization.sanitizer_revision, SANITIZER_REVISION);
    Ok(())
}

#[test]
fn an_accepted_unmodified_payload_is_admissible() -> TestResult {
    // Hygiene that changed no byte still has to produce a receipt; "nothing to
    // redact" is not the same as "no evidence".
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let admitted = Builder {
        sanitization: Some(accepted_binding_for(BODY)?),
        body: BODY.to_owned(),
        ..Builder::at_sequence(1)
    }
    .build()?;
    assert_eq!(
        admitted.sanitization.source_payload_sha256,
        admitted.payload.sha256
    );
    store.append_admitted(&admitted)?;
    assert_eq!(store.lease_pending(&lease_request(T0, 4))?.len(), 1);
    Ok(())
}

#[test]
fn every_perturbation_of_the_binding_is_refused() -> TestResult {
    let sanitized = digest_hex(BODY.as_bytes());
    let source = digest_hex(format!("raw-source-of:{BODY}").as_bytes());
    let findings = digest_hex(b"finding-set:secret-span-redacted");
    let valid = binding_for(BODY)?;

    // A receipt that is internally self-consistent but describes *other* bytes.
    let other_payload = receipt_json(PayloadSanitizationReceiptParts {
        sanitizer_revision: SANITIZER_REVISION.to_owned(),
        source_payload_sha256: source.clone(),
        sanitized_payload_sha256: digest_hex(b"{\"message\":\"something-else\"}"),
        extensions_digest: empty_opaque_extensions_digest(),
        disposition: SanitizationDisposition::Redacted,
        finding_count: 1,
        findings_digest: findings.clone(),
    })?;
    // A self-consistent receipt minted by a different sanitizer revision, with
    // the column still claiming the admitted one.
    let other_revision = receipt_json(PayloadSanitizationReceiptParts {
        sanitizer_revision: "observation-hygiene-policy.v0.1".to_owned(),
        source_payload_sha256: source.clone(),
        sanitized_payload_sha256: sanitized.clone(),
        extensions_digest: empty_opaque_extensions_digest(),
        disposition: SanitizationDisposition::Redacted,
        finding_count: 1,
        findings_digest: findings.clone(),
    })?;
    // A self-consistent receipt over a different pre-sanitization payload.
    let other_source = receipt_json(PayloadSanitizationReceiptParts {
        sanitizer_revision: SANITIZER_REVISION.to_owned(),
        source_payload_sha256: digest_hex(b"some-other-raw-payload"),
        sanitized_payload_sha256: sanitized.clone(),
        extensions_digest: empty_opaque_extensions_digest(),
        disposition: SanitizationDisposition::Redacted,
        finding_count: 1,
        findings_digest: findings,
    })?;
    // A receipt whose finding count was edited after minting: its own derived
    // identifier no longer matches, which `from_json` catches.
    let tampered_count = valid
        .receipt_json
        .replace("\"finding_count\":1", "\"finding_count\":9");
    assert_ne!(tampered_count, valid.receipt_json);

    let perturbations: Vec<(&str, SanitizationBindingV1)> = vec![
        (
            "receipt describes other payload bytes",
            SanitizationBindingV1 {
                receipt_json: other_payload,
                ..valid.clone()
            },
        ),
        (
            "receipt names a different sanitizer revision",
            SanitizationBindingV1 {
                receipt_json: other_revision,
                ..valid.clone()
            },
        ),
        (
            "receipt names a different source payload",
            SanitizationBindingV1 {
                receipt_json: other_source,
                ..valid.clone()
            },
        ),
        (
            "receipt fields edited after minting",
            SanitizationBindingV1 {
                receipt_json: tampered_count,
                ..valid.clone()
            },
        ),
        (
            "receipt id column re-pointed",
            SanitizationBindingV1 {
                receipt_id: "obs-hygiene-receipt.v1.0000".to_owned(),
                ..valid.clone()
            },
        ),
        (
            "revision column re-pointed",
            SanitizationBindingV1 {
                sanitizer_revision: "observation-hygiene-policy.v9.9".to_owned(),
                ..valid.clone()
            },
        ),
        (
            "source digest column re-pointed",
            SanitizationBindingV1 {
                source_payload_sha256: digest_hex(b"unrelated"),
                ..valid.clone()
            },
        ),
        (
            "receipt json is not a receipt",
            SanitizationBindingV1 {
                receipt_json: "{\"receipt_id\":\"x\"}".to_owned(),
                ..valid.clone()
            },
        ),
        (
            "receipt json is unbounded",
            SanitizationBindingV1 {
                receipt_json: "x".repeat(MAX_SANITIZATION_RECEIPT_JSON_BYTES + 1),
                ..valid.clone()
            },
        ),
    ];

    for (label, binding) in perturbations {
        let result = Builder {
            sanitization: Some(binding),
            body: BODY.to_owned(),
            ..Builder::at_sequence(1)
        }
        .build();
        assert!(result.is_err(), "a broken binding was accepted: {label}");
    }

    // The unperturbed binding still admits, so the test above is not passing
    // for some unrelated reason.
    assert!(
        Builder {
            sanitization: Some(valid),
            body: BODY.to_owned(),
            ..Builder::at_sequence(1)
        }
        .build()
        .is_ok()
    );
    Ok(())
}

#[test]
fn the_binding_is_covered_by_the_envelope_digest() -> TestResult {
    // Swapping the hygiene evidence for other *valid* evidence must break the
    // envelope digest: otherwise a stored envelope could be re-pointed at a
    // different sanitizer decision without the seal noticing.
    let mut admitted = Builder::at_sequence(1).build()?;
    let sealed = admitted.envelope_sha256.clone();
    admitted.sanitization =
        accepted_binding_for(&String::from_utf8(admitted.payload.bytes.clone())?)?;
    assert_ne!(
        admitted.expected_envelope_sha256(),
        sealed,
        "the envelope digest does not cover the hygiene binding"
    );
    assert!(matches!(
        admitted.validate(),
        Err(ObservationJournalError::EnvelopeDigestMismatch)
    ));

    // Re-sealing accepts it again, so the digest tracks the binding rather than
    // rejecting every binding.
    seal(&mut admitted);
    admitted.validate()?;
    Ok(())
}

#[test]
fn a_stored_binding_that_drifts_is_a_corrupt_row_not_a_delivery() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    {
        let store = journal(&path)?;
        store.append_admitted(&Builder::at_sequence(1).build()?)?;
    }
    {
        // A receipt that is valid on its own but bound to other bytes, written
        // straight past the domain types.
        let forged = receipt_json(PayloadSanitizationReceiptParts::accepted_unmodified(
            SANITIZER_REVISION,
            digest_hex(b"entirely-different-bytes"),
        ))?;
        let connection = rusqlite::Connection::open(&path)?;
        connection.execute(
            "UPDATE tdmem_observation_journal_v1 SET sanitization_receipt_json = ?1",
            [&forged],
        )?;
    }
    let store = journal(&path)?;
    let error = store
        .lease_pending(&lease_request(T0, 4))
        .err()
        .ok_or("a row whose hygiene binding drifted was delivered")?;
    assert!(matches!(
        error,
        ObservationJournalError::Corrupt {
            field: "sanitization_receipt_json",
            ..
        }
    ));
    Ok(())
}

#[test]
fn content_may_not_sit_in_the_store_without_its_hygiene_evidence() -> TestResult {
    // The schema, not just the domain type, refuses the combination: clearing
    // the binding while leaving the payload is not a state that can exist.
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    {
        let store = journal(&path)?;
        store.append_admitted(&Builder::at_sequence(1).build()?)?;
    }
    let connection = rusqlite::Connection::open(&path)?;
    let cleared = connection.execute(
        "UPDATE tdmem_observation_journal_v1 SET sanitization_receipt_id = NULL, \
         sanitizer_revision = NULL, source_payload_sha256 = NULL, \
         sanitization_receipt_json = NULL",
        [],
    );
    assert!(
        cleared.is_err(),
        "content was left in the store with no hygiene evidence"
    );
    // And a partial clear is refused too, so no undecodable combination exists.
    let partial = connection.execute(
        "UPDATE tdmem_observation_journal_v1 SET source_payload_sha256 = NULL",
        [],
    );
    assert!(partial.is_err(), "a half-cleared binding was accepted");
    Ok(())
}

// -- Extension admission bounds --------------------------------------------
//
// `identity::extensions_digest` used to call `opaque_extensions_digest`
// directly, which digests whatever set it is given with no count, per-item,
// or aggregate bound. `ProviderCall::validate` on the dispatch boundary
// enforces exactly those bounds via `observation_extensions_digest`. An
// envelope that only the unbounded admission path would accept could be
// durably queued and then fail every dispatch attempt forever — a poison
// pill that exhausts retries without ever succeeding. These tests pin
// admission to the same bound dispatch enforces.

fn extensions_of(
    count: usize,
    body_len: usize,
) -> Result<Vec<tracedecay_memory_provider_api::OwnedOpaqueExtension>, Box<dyn std::error::Error>> {
    let body = "x".repeat(body_len);
    (0..count)
        .map(|index| extension(&format!("vendor.ext-{index:04}.v1"), &body))
        .collect()
}

#[test]
fn extensions_digest_rejects_more_than_the_maximum_extension_count() -> TestResult {
    // The dispatch boundary admits at most 32 extensions.
    let too_many = extensions_of(33, 8)?;
    let error = extensions_digest(&too_many)
        .err()
        .ok_or("an oversized extension count was digested without error")?;
    assert!(matches!(
        error,
        ObservationJournalError::Api(ApiError::TooManyBoundaryItems {
            field: "extensions",
            maximum: 32,
        })
    ));

    // Exactly at the boundary still digests, so the bound is exclusive, not
    // off-by-one.
    let at_capacity = extensions_of(32, 8)?;
    extensions_digest(&at_capacity)?;
    Ok(())
}

#[test]
fn extensions_digest_rejects_an_oversized_single_extension() -> TestResult {
    // The dispatch boundary admits at most 256 KiB per extension.
    let oversized = extensions_of(1, 262_144 + 1)?;
    let error = extensions_digest(&oversized)
        .err()
        .ok_or("an oversized extension payload was digested without error")?;
    assert!(matches!(
        error,
        ObservationJournalError::Api(ApiError::BoundaryBytesExceeded {
            field: "extension_canonical_payload",
            maximum: 262_144,
        })
    ));

    // Exactly at the boundary still digests.
    let at_capacity = extensions_of(1, 262_144)?;
    extensions_digest(&at_capacity)?;
    Ok(())
}

#[test]
fn extensions_digest_rejects_an_oversized_aggregate_extension_set() -> TestResult {
    // The dispatch boundary admits at most 512 KiB in aggregate, even when
    // every individual extension is within its own per-item bound.
    let over_aggregate = extensions_of(3, 200_000)?; // 600,000 > 524,288
    let error = extensions_digest(&over_aggregate)
        .err()
        .ok_or("an oversized aggregate extension set was digested without error")?;
    assert!(matches!(
        error,
        ObservationJournalError::Api(ApiError::BoundaryBytesExceeded {
            field: "extensions",
            maximum: 524_288,
        })
    ));
    Ok(())
}

#[test]
fn admission_refuses_the_same_oversized_extension_set_dispatch_would_refuse() -> TestResult {
    // The poison-pill scenario itself: an admission pipeline that tried to
    // append an envelope carrying an extension set only dispatch's bound
    // would reject must fail at admission, not succeed and then jam the
    // dispatch queue on every retry.
    let too_many = extensions_of(33, 8)?;
    let result = Builder {
        extensions: too_many,
        ..Builder::at_sequence(1)
    }
    .build();
    let error = result
        .err()
        .ok_or("an envelope with an oversized extension set was admitted")?;
    // The refusal is the dispatch boundary's own typed failure, not a generic
    // admission error that happens to fire here.
    let journal_error = error
        .downcast_ref::<ObservationJournalError>()
        .ok_or_else(|| format!("unexpected admission failure: {error}"))?;
    assert!(
        matches!(
            journal_error,
            ObservationJournalError::Api(ApiError::TooManyBoundaryItems {
                field: "extensions",
                maximum: 32,
            })
        ),
        "unexpected admission failure: {journal_error}"
    );
    Ok(())
}

#[test]
fn admission_still_admits_an_empty_extension_set() -> TestResult {
    // Optional extension semantics must survive the bound: no extensions at
    // all remains a valid, ordinary observation.
    let admitted = Builder::at_sequence(1).build()?;
    assert!(admitted.extensions.is_empty());
    assert_eq!(
        admitted.extensions_digest,
        extensions_digest(&[])?,
        "an empty extension set must still digest to the canonical empty digest"
    );
    admitted.validate()?;
    Ok(())
}

#[test]
fn admission_still_admits_an_extension_set_within_every_bound() -> TestResult {
    // A well-formed, in-bound extension set is unaffected by aligning
    // admission with the dispatch boundary.
    let extensions = extensions_of(4, 128)?;
    let admitted = Builder {
        extensions: extensions.clone(),
        ..Builder::at_sequence(1)
    }
    .build()?;
    assert_eq!(admitted.extensions, extensions);
    admitted.validate()?;
    Ok(())
}
