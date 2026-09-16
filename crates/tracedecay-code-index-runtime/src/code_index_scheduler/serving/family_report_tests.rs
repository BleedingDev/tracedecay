use super::*;

fn member_cursor(after: &str) -> CloneArtifactCursorV1 {
    serde_json::from_value(serde_json::json!({
        "artifact_digest": format!("sha256:{}", "0".repeat(64)),
        "generation": "generation-1",
        "request_digest": format!("sha256:{}", "1".repeat(64)),
        "after": { "Exact": after },
    }))
    .expect("valid exact member cursor")
}

fn family_key() -> CloneExactKeyV1 {
    CloneExactKeyV1 {
        class: CloneNormalizationClassV1::Conservative,
        normalization_revision: 1,
        digest: tracedecay_domain::ManifestDigest::new(format!("sha256:{}", "2".repeat(64)))
            .expect("valid family digest"),
    }
}

#[test]
fn three_member_family_member_limit_two_resumes_from_family_before_member() {
    let all_members = ["representative", "member-1", "member-2"];
    let member_limit = 2;
    let first_page = &all_members[..member_limit];
    assert_eq!(first_page, ["representative", "member-1"]);
    assert_eq!(all_members.len() - first_page.len(), 1);

    let continuation = RedundancyMemberContinuationCursorV1 {
        family_cursor: Some("family-before".to_owned()),
        family_key: family_key(),
        member_cursor: member_cursor("member-1"),
    };
    let encoded = continuation.encode().expect("encode member continuation");
    let (family_cursor, decoded) =
        decode_redundancy_cursor(Some(&encoded)).expect("decode member continuation");
    let decoded = decoded.expect("member continuation state");

    assert_eq!(family_cursor.as_deref(), Some("family-before"));
    assert_eq!(decoded.family_key, family_key());
    assert_eq!(decoded.member_cursor, member_cursor("member-1"));

    let (coverage, next_cursor) = redundancy_coverage(
        false,
        false,
        true,
        None,
        Some(encoded.clone()),
        1,
        first_page.len(),
    );
    assert!(matches!(
        coverage,
        RedundancyCoverageV1::Partial {
            reason: RedundancyPartialReasonV1::WorkLimit,
            examined_families: 1,
            examined_members: 2,
        }
    ));
    assert_eq!(next_cursor, Some(encoded));
}

#[test]
fn generated_member_labels_follow_the_canonical_path_policy() {
    assert!(is_generated_path("vendor/generated.rs"));
    assert!(is_generated_path("src/node_modules/generated.ts"));
    assert!(!is_generated_path("src/generated.rs"));
}
