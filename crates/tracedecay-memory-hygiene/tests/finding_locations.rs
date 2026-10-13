//! A credential-bearing object key never reaches a structural location.
//!
//! The finding on the key itself was already anchored to a placeholder, but the
//! key also formed a path *segment*, so every finding on a value nested beneath
//! it carried the key verbatim into the receipt's findings digest. These tests
//! walk the whole nested case and assert that no substring of the key survives
//! anywhere in the classification result.
#![allow(clippy::expect_used, clippy::panic)]

use serde_json::json;
use tracedecay_memory_hygiene::{
    CREDENTIAL_BEARING_KEY_MARKER_PREFIX, HygieneClass, HygieneFindingV1, ObservationSanitizer,
    credential_bearing_key_marker,
};

fn sanitizer() -> ObservationSanitizer {
    ObservationSanitizer::new().expect("canonical hygiene policy")
}

/// Every substring of `key` of at least `MINIMUM` characters. A location that
/// contains none of them cannot be reconstructed into the key.
fn substrings(key: &str) -> Vec<&str> {
    const MINIMUM: usize = 6;
    let mut out = Vec::new();
    for start in 0..key.len() {
        for end in (start + MINIMUM)..=key.len() {
            if let Some(slice) = key.get(start..end) {
                out.push(slice);
            }
        }
    }
    assert!(!out.is_empty(), "the fixture key is too short to matter");
    out
}

fn assert_no_key_material(findings: &[HygieneFindingV1], key: &str) {
    for finding in findings {
        for fragment in substrings(key) {
            assert!(
                !finding.location().contains(fragment),
                "location {} leaked {fragment:?} from the credential-bearing key",
                finding.location()
            );
        }
    }
}

#[test]
fn a_nested_finding_under_a_credential_bearing_key_never_names_the_key() {
    let key = concat!("AKIA", "4S27TQXBVCZ5MJ6L");
    // The nested value raises its own transient finding, so the walk is forced
    // to render a location *through* the credential-bearing key.
    let payload = json!({
        key: {
            "runs": [{ "note": "server started with pid 48213 and stayed up" }]
        }
    });
    let findings = sanitizer().classify(&payload).expect("classification");
    assert!(
        findings
            .iter()
            .any(|finding| finding.class() == HygieneClass::TransientProcessId),
        "the fixture no longer produces a descendant finding: {findings:?}"
    );
    assert_no_key_material(&findings, key);

    let marker = credential_bearing_key_marker(key);
    let nested = findings
        .iter()
        .find(|finding| finding.class() == HygieneClass::TransientProcessId)
        .expect("descendant finding");
    assert_eq!(
        nested.location(),
        format!("$.{marker}.runs[0].note"),
        "the descendant path must route through the opaque marker"
    );
}

#[test]
fn the_finding_on_the_key_itself_uses_the_same_opaque_marker() {
    let key = concat!("AKIA", "4S27TQXBVCZ5MJ6L");
    let payload = json!({ key: "value" });
    let findings = sanitizer().classify(&payload).expect("classification");
    assert_eq!(findings.len(), 2);
    assert!(
        findings
            .iter()
            .any(|finding| finding.class() == HygieneClass::KnownCredentialPrefix)
    );
    let key_finding = findings
        .iter()
        .find(|finding| finding.class() == HygieneClass::CredentialBearingKey)
        .expect("credential-bearing-key finding");
    assert_eq!(
        key_finding.location(),
        format!("$.{}", credential_bearing_key_marker(key))
    );
    assert!(
        key_finding
            .location()
            .contains(CREDENTIAL_BEARING_KEY_MARKER_PREFIX)
    );
    assert_no_key_material(&findings, key);
}

#[test]
fn two_distinct_credential_keys_stay_two_distinct_findings() {
    // A single shared placeholder would collapse these into one finding during
    // canonicalization and under-report the receipt's finding count.
    let first = concat!("AKIA", "4S27TQXBVCZ5MJ6L");
    let second = concat!("ghp_", "KsY7QwT2mZ4bV9nR6cX1jH8pL3dG5fA0eUwQ");
    let payload = json!({ first: "a", second: "b" });
    let findings = sanitizer().classify(&payload).expect("classification");
    assert_eq!(findings.len(), 4, "{findings:?}");
    let key_locations: Vec<&str> = findings
        .iter()
        .filter(|finding| finding.class() == HygieneClass::CredentialBearingKey)
        .map(HygieneFindingV1::location)
        .collect();
    assert_eq!(key_locations.len(), 2);
    assert_ne!(key_locations[0], key_locations[1]);
    assert_no_key_material(&findings, first);
    assert_no_key_material(&findings, second);
}

#[test]
fn the_marker_is_stable_and_reveals_nothing_of_the_key() {
    let key = concat!("AKIA", "4S27TQXBVCZ5MJ6L");
    let marker = credential_bearing_key_marker(key);
    assert_eq!(marker, credential_bearing_key_marker(key));
    assert_ne!(marker, credential_bearing_key_marker("other-key"));
    assert!(marker.starts_with(CREDENTIAL_BEARING_KEY_MARKER_PREFIX));
    assert!(marker.ends_with('>'));
    for fragment in substrings(key) {
        assert!(!marker.contains(fragment), "the marker leaked {fragment:?}");
    }
}

#[test]
fn an_ordinary_key_is_still_rendered_verbatim() {
    let payload = json!({ "config": { "notes": "server started with pid 48213" } });
    let findings = sanitizer().classify(&payload).expect("classification");
    assert_eq!(findings.len(), 1);
    assert_eq!(findings[0].location(), "$.config.notes");
}
