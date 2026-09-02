//! Closes the drift loop on `observation-hygiene-policy-v1.json` from the Rust
//! side.
//!
//! `tests/product_observation_hygiene_policy_test.py` asserts the same document
//! structurally. Between them, neither the table nor the code can move without
//! the other.
#![allow(clippy::expect_used, clippy::panic)]

use serde_json::Value;
use tracedecay_memory_hygiene::{
    HygieneAction, HygieneClass, OBSERVATION_HYGIENE_POLICY_V1_CANONICAL_PATH,
    OBSERVATION_HYGIENE_POLICY_V1_EMBEDDED_PATH, OBSERVATION_HYGIENE_POLICY_V1_JSON,
    OBSERVATION_HYGIENE_SANITIZER_ID, ObservationHygienePolicyV1, ObservationSanitizer,
    SanitizationDisposition, WithheldReason, findings_digest, withheld_receipt_id,
};

fn document() -> Value {
    serde_json::from_str(OBSERVATION_HYGIENE_POLICY_V1_JSON).expect("policy document json")
}

#[test]
fn the_embedded_document_is_the_product_contract() {
    let document = document();
    assert_eq!(document["schema_version"], 1);
    assert_eq!(
        document["contract_id"],
        "tracedecay.observation-hygiene-policy.v1"
    );
    assert_eq!(document["bead_id"], "tdmem-0507");
    assert_eq!(document["status"], "accepted");
    assert_eq!(document["sanitizer_id"], OBSERVATION_HYGIENE_SANITIZER_ID);
}

#[test]
fn every_class_in_the_document_matches_the_compiled_table() {
    let document = document();
    let policy = ObservationHygienePolicyV1::canonical().expect("canonical policy");
    let rows = document["classes"].as_array().expect("classes");
    assert_eq!(
        rows.len(),
        HygieneClass::ALL.len(),
        "the document and the enum disagree on how many classes exist"
    );
    for row in rows {
        let class_id = row["class_id"].as_str().expect("class id");
        let class = HygieneClass::from_wire(class_id).expect("the build implements this class");
        let action =
            HygieneAction::from_wire(row["action"].as_str().expect("action")).expect("action");
        assert_eq!(
            policy.action(class),
            action,
            "{class_id} drifted between the document and the table"
        );
        assert_eq!(
            action.rewrites_payload(),
            row["mutates_payload"].as_bool().expect("mutates_payload"),
            "{class_id} disagrees about whether it rewrites bytes"
        );
        match row["withheld_reason"].as_str() {
            Some("secret_rejected") => assert_eq!(action, HygieneAction::Reject),
            Some("quarantined") => assert_eq!(action, HygieneAction::Quarantine),
            Some(other) => panic!("{class_id} declares unknown withheld reason {other}"),
            None => assert!(!action.withholds(), "{class_id} withholds without a reason"),
        }
    }
}

#[test]
fn the_reject_floor_matches_the_document() {
    let document = document();
    let policy = ObservationHygienePolicyV1::canonical().expect("canonical policy");
    let declared: Vec<&str> = document["reject_floor_classes"]
        .as_array()
        .expect("reject floor")
        .iter()
        .map(|value| value.as_str().expect("class id"))
        .collect();
    assert!(!declared.is_empty());
    for class_id in &declared {
        let class = HygieneClass::from_wire(class_id).expect("known class");
        assert!(policy.is_reject_floor(class));
        assert_eq!(policy.action(class), HygieneAction::Reject);
    }
    for class in HygieneClass::ALL {
        assert_eq!(
            policy.is_reject_floor(class),
            declared.contains(&class.as_str()),
            "{class:?} disagrees with the document about the reject floor"
        );
    }
}

#[test]
fn the_severity_ladder_is_totally_ordered_and_matches_the_document() {
    let document = document();
    let declared: Vec<HygieneAction> = document["severity_ladder"]
        .as_array()
        .expect("severity ladder")
        .iter()
        .map(|value| {
            HygieneAction::from_wire(value.as_str().expect("action")).expect("known action")
        })
        .collect();
    assert_eq!(
        declared,
        vec![
            HygieneAction::Accept,
            HygieneAction::Annotate,
            HygieneAction::Redact,
            HygieneAction::Quarantine,
            HygieneAction::Reject,
        ]
    );
    for window in declared.windows(2) {
        assert!(window[0] < window[1], "the ladder is not ascending");
    }
}

#[test]
fn the_document_and_the_boundary_agree_on_dispositions_and_reasons() {
    let document = document();
    let delivered: Vec<&str> = document["dispositions"]["delivered"]
        .as_array()
        .expect("delivered dispositions")
        .iter()
        .map(|value| value.as_str().expect("disposition"))
        .collect();
    assert_eq!(
        delivered,
        vec![
            SanitizationDisposition::Accepted.as_str(),
            SanitizationDisposition::Redacted.as_str()
        ]
    );
    let withheld: Vec<&str> = document["dispositions"]["withheld"]
        .as_array()
        .expect("withheld reasons")
        .iter()
        .map(|value| value.as_str().expect("reason"))
        .collect();
    assert_eq!(
        withheld,
        vec![
            WithheldReason::SecretRejected.as_str(),
            WithheldReason::Quarantined.as_str(),
            WithheldReason::UnclassifiablePayload.as_str()
        ]
    );
    assert_eq!(
        document["receipt"]["id_prefix"],
        tracedecay_memory_provider_api::OBSERVATION_HYGIENE_RECEIPT_ID_PREFIX
    );
    assert_eq!(
        document["receipt"]["withheld_id_prefix"],
        tracedecay_memory_hygiene::OBSERVATION_HYGIENE_WITHHELD_ID_PREFIX
    );
}

#[test]
fn the_revision_is_bound_to_the_document_bytes_and_to_the_effective_table() {
    let policy = ObservationHygienePolicyV1::canonical().expect("canonical policy");
    let digest = tracedecay_domain::canonical_text::sha256_hex(
        OBSERVATION_HYGIENE_POLICY_V1_JSON.as_bytes(),
    );
    assert_eq!(policy.document_digest(), digest);
    assert!(
        policy
            .revision()
            .starts_with(OBSERVATION_HYGIENE_SANITIZER_ID)
    );
    assert!(policy.revision().contains(&digest[..16]));
    assert_eq!(
        ObservationSanitizer::new().expect("sanitizer").revision(),
        policy.revision()
    );
}

#[test]
fn the_findings_digest_domain_is_pinned_by_the_document() {
    // The digest a receipt carries is only comparable across builds if the
    // domain and the framing are both fixed. The document publishes the digest
    // of the empty finding set as the golden value; drift in either the domain
    // separator or the length framing changes it.
    let document = document();
    assert_eq!(
        document["receipt"]["empty_findings_digest"]
            .as_str()
            .expect("declared empty findings digest"),
        findings_digest(&[]),
        "the findings digest drifted from the product contract"
    );
}

#[test]
fn a_withheld_identity_is_framed_under_its_own_domain() {
    // A withheld audit row and a delivered receipt derive over different field
    // sets; sharing a domain separator would let one framed input be replayed
    // as the other.
    let document = document();
    let receipt_domain = document["receipt"]["digest_domain"]
        .as_str()
        .expect("receipt digest domain");
    let withheld_domain = document["receipt"]["withheld_digest_domain"]
        .as_str()
        .expect("withheld digest domain");
    assert_ne!(receipt_domain, withheld_domain);

    let extensions_digest = tracedecay_memory_provider_api::empty_opaque_extensions_digest();
    let expected = format!(
        "{}{}",
        tracedecay_memory_hygiene::OBSERVATION_HYGIENE_WITHHELD_ID_PREFIX,
        tracedecay_domain::canonical_text::canonical_framed_sha256(
            withheld_domain.as_bytes(),
            &[
                b"rev.v1".as_slice(),
                b"a".repeat(64).as_slice(),
                extensions_digest.as_bytes(),
                WithheldReason::SecretRejected.as_str().as_bytes(),
                &1u32.to_be_bytes(),
                findings_digest(&[]).as_bytes(),
            ],
        )
    );
    assert_eq!(
        withheld_receipt_id(
            "rev.v1",
            &"a".repeat(64),
            &extensions_digest,
            WithheldReason::SecretRejected,
            1,
            &findings_digest(&[]),
        ),
        expected,
        "the withheld identity is no longer framed under the declared domain"
    );

    let under_receipt_domain = format!(
        "{}{}",
        tracedecay_memory_hygiene::OBSERVATION_HYGIENE_WITHHELD_ID_PREFIX,
        tracedecay_domain::canonical_text::canonical_framed_sha256(
            receipt_domain.as_bytes(),
            &[
                b"rev.v1".as_slice(),
                b"a".repeat(64).as_slice(),
                extensions_digest.as_bytes(),
                WithheldReason::SecretRejected.as_str().as_bytes(),
                &1u32.to_be_bytes(),
                findings_digest(&[]).as_bytes(),
            ],
        )
    );
    assert_ne!(expected, under_receipt_domain);
}

#[test]
fn the_reject_floor_signals_only_ever_name_reject_floor_classes() {
    let document = document();
    let policy = ObservationHygienePolicyV1::canonical().expect("canonical policy");
    let signals = &document["reject_floor_signals"];
    for key in ["direct_signal_classes", "probe_signal_classes"] {
        let declared = signals[key].as_array().expect("declared signal classes");
        assert!(!declared.is_empty(), "{key} is empty");
        for value in declared {
            let class_id = value.as_str().expect("class id");
            let class = HygieneClass::from_wire(class_id).expect("known class");
            assert!(
                policy.is_reject_floor(class),
                "{class_id} is a {key} entry but is not on the reject floor, so the \
                 supplementary pass could change a classification instead of hardening it"
            );
        }
    }
    assert!(
        !signals["known_credential_prefixes_are_exhaustive"]
            .as_bool()
            .expect("exhaustiveness flag"),
        "the vendored catalogue has one owner; this list is a floor, not a copy"
    );

    let compiled = policy.signals();
    let declared_prefixes: Vec<&str> = signals["known_credential_prefixes"]
        .as_array()
        .expect("prefixes")
        .iter()
        .map(|value| value.as_str().expect("prefix"))
        .collect();
    assert_eq!(compiled.known_credential_prefixes(), declared_prefixes);
    assert_eq!(
        compiled.minimum_credential_run_length(),
        usize::try_from(
            signals["minimum_credential_run_length"]
                .as_u64()
                .expect("run length")
        )
        .expect("fits")
    );
    assert_eq!(
        compiled.entropy_candidate_minimum_length(),
        usize::try_from(
            signals["entropy_candidate_minimum_length"]
                .as_u64()
                .expect("candidate length")
        )
        .expect("fits")
    );
    assert_eq!(
        compiled.maximum_detector_probes_per_payload(),
        usize::try_from(
            signals["maximum_detector_probes_per_payload"]
                .as_u64()
                .expect("probe budget")
        )
        .expect("fits")
    );
    let declared_separators: Vec<char> = signals["candidate_separators"]
        .as_array()
        .expect("separators")
        .iter()
        .map(|value| {
            let text = value.as_str().expect("separator");
            let mut characters = text.chars();
            let character = characters.next().expect("one character");
            assert!(characters.next().is_none(), "separators are single chars");
            character
        })
        .collect();
    assert_eq!(compiled.candidate_separators(), declared_separators);
}

#[test]
fn the_embedded_document_names_the_canonical_copy_it_must_equal() {
    // The crate embeds its own copy so it compiles and packages inside its
    // ownership area; the Python gate asserts the two files are byte-identical.
    // Naming both paths here keeps the two lanes pointing at the same files.
    assert_eq!(
        OBSERVATION_HYGIENE_POLICY_V1_CANONICAL_PATH,
        "product/observations/observation-hygiene-policy-v1.json"
    );
    assert_eq!(
        OBSERVATION_HYGIENE_POLICY_V1_EMBEDDED_PATH,
        "crates/tracedecay-memory-hygiene/policy/observation-hygiene-policy-v1.json"
    );
    assert!(
        OBSERVATION_HYGIENE_POLICY_V1_EMBEDDED_PATH
            .starts_with("crates/tracedecay-memory-hygiene/"),
        "the embedded copy must live inside this crate's ownership area"
    );
    assert_ne!(
        OBSERVATION_HYGIENE_POLICY_V1_CANONICAL_PATH,
        OBSERVATION_HYGIENE_POLICY_V1_EMBEDDED_PATH
    );
}

#[test]
fn the_bounded_scan_ceiling_comes_from_the_document() {
    let document = document();
    let policy = ObservationHygienePolicyV1::canonical().expect("canonical policy");
    assert_eq!(
        u64::try_from(policy.max_canonical_bytes()).expect("ceiling fits"),
        document["payload_limits"]["max_canonical_bytes"]
            .as_u64()
            .expect("declared ceiling")
    );
}
