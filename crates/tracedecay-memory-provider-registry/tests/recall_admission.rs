//! Behavioral tests for host-side recall admission: exact scope, typed
//! stale/unknown identity, rank-final validity and revocation, and an audit
//! ledger that never carries candidate content.
#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use serde_json::{Value, json};
use tracedecay_memory_provider_api::contract::TerminalCode;
use tracedecay_memory_provider_api::{CanonicalPayload, OwnedProviderId, OwnedVersionedId};
use tracedecay_memory_provider_native::NATIVE_PROVIDER_ID;
use tracedecay_memory_provider_registry::{
    AdmittedTemporalQuery, RECALL_PAYLOAD_CONTRACT_ID, RECALL_QUERY_CAPABILITY_ID,
    RecallAdmissionError, RecallBudgetsV1, RecallCandidateContent, RecallCandidateV1,
    RecallConfidenceDefect, RecallDenialReason, RecallRequestParts, RecallScopeBindingsV1,
    ScopeBinding, ScopeField, TemporalState, UnknownValidityPolicy, admit_recall_candidates,
    admit_recall_reply, build_recall_request_payload, decode_recall_outcome,
};

mod recall_fixture;
use recall_fixture::*;

#[test]
fn cross_repository_worktree_branch_session_project_profile_candidates_are_denied()
-> Result<(), Box<dyn Error>> {
    let cases = [
        ("repository_identity", ScopeField::RepositoryIdentity),
        ("worktree_identity", ScopeField::WorktreeIdentity),
        ("branch_identity", ScopeField::BranchIdentity),
        ("agent_session_id", ScopeField::AgentSessionId),
        ("project_id", ScopeField::ProjectId),
        ("profile_id", ScopeField::ProfileId),
    ];
    for (wire_field, field) in cases {
        let foreign = candidate(
            "foreign",
            with_scope_field(wire_field, "other"),
            current_validity(),
        );
        let admission = admit_recall_candidates(
            &admitted_scope(),
            "request",
            &current_query(),
            &authorized_exact(),
            vec![foreign],
        )?;
        assert!(admission.admitted.is_empty(), "{wire_field} must be denied");
        assert_eq!(admission.report.denied.len(), 1);
        assert_eq!(
            admission.report.denied[0].reason,
            RecallDenialReason::ScopeMismatch { field },
            "{wire_field}"
        );
        assert!(
            admission.report.denied[0]
                .provider_claimed_scope_sha256
                .as_deref()
                .is_some_and(|digest| digest != admitted_scope().exact_scope_sha256())
        );
    }
    Ok(())
}

#[test]
fn stale_and_unknown_identity_are_typed_distinctly() -> Result<(), Box<dyn Error>> {
    let stale = candidate(
        "stale",
        with_scope_field("resolved_scope_digest", STALE_SCOPE_DIGEST),
        current_validity(),
    );
    let unknown = candidate(
        "unknown",
        with_scope_field("worktree_identity", ""),
        current_validity(),
    );
    let malformed_digest = candidate(
        "malformed",
        with_scope_field("resolved_scope_digest", "  "),
        current_validity(),
    );
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_exact(),
        vec![stale, unknown, malformed_digest],
    )?;
    assert!(admission.admitted.is_empty());
    let reasons: Vec<_> = admission
        .report
        .denied
        .iter()
        .map(|denied| denied.reason.clone())
        .collect();
    assert_eq!(
        reasons,
        vec![
            RecallDenialReason::StaleIdentity,
            RecallDenialReason::UnknownIdentity {
                field: ScopeField::WorktreeIdentity
            },
            RecallDenialReason::UnknownIdentity {
                field: ScopeField::ResolvedScopeDigest
            },
        ]
    );
    // A malformed claim yields no fabricated digest.
    assert_eq!(
        admission.report.denied[2].provider_claimed_scope_sha256,
        None
    );
    assert!(
        admission.report.denied[0]
            .provider_claimed_scope_sha256
            .is_some()
    );
    Ok(())
}

#[test]
fn validity_windows_revocation_and_supersession_are_enforced_in_current_mode()
-> Result<(), Box<dyn Error>> {
    let scope = scope_value(&admitted_scope());
    let candidates = vec![
        candidate("current", scope.clone(), current_validity()),
        candidate(
            "expired",
            scope.clone(),
            validity_with(
                "expired",
                &[("valid_until", json!("2026-09-01T12:00:00.000000Z"))],
            ),
        ),
        candidate(
            "future",
            scope.clone(),
            validity_with(
                "future",
                &[("valid_from", json!("2026-09-01T12:00:00.000000001Z"))],
            ),
        ),
        candidate(
            "revoked",
            scope.clone(),
            validity_with(
                "revoked",
                &[("revoked_at", json!("2026-08-15T00:00:00.000000Z"))],
            ),
        ),
        candidate(
            "superseded",
            scope.clone(),
            validity_with(
                "superseded",
                &[
                    ("superseded_at", json!("2026-08-15T00:00:00.000000Z")),
                    ("superseded_by", json!("memory:newer")),
                ],
            ),
        ),
        candidate("unknown", scope.clone(), validity_with("unknown", &[])),
        candidate(
            "still-valid-until-later",
            scope,
            validity_with(
                "current",
                &[("valid_until", json!("2026-09-01T12:00:00.000000001Z"))],
            ),
        ),
    ];
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_exact(),
        candidates,
    )?;
    let admitted: Vec<_> = admission
        .admitted
        .iter()
        .map(|entry| entry.candidate().candidate_id.as_str())
        .collect();
    assert_eq!(admitted, vec!["current", "still-valid-until-later"]);
    assert!(
        admission
            .admitted
            .iter()
            .all(|entry| entry.host_temporal_state() == TemporalState::Current)
    );
    let denied: Vec<_> = admission
        .report
        .denied
        .iter()
        .map(|denied| (denied.candidate_id.as_str(), denied.reason.clone()))
        .collect();
    assert_eq!(
        denied,
        vec![
            ("expired", RecallDenialReason::Expired),
            ("future", RecallDenialReason::NotYetValid),
            ("revoked", RecallDenialReason::Revoked),
            ("superseded", RecallDenialReason::Superseded),
            ("unknown", RecallDenialReason::UnknownValidity),
        ]
    );
    assert!(!admission.report.degraded);
    Ok(())
}

#[test]
fn provider_claims_cannot_expand_validity() -> Result<(), Box<dyn Error>> {
    let scope = scope_value(&admitted_scope());
    let candidates = vec![
        // Claims current while carrying a revocation.
        candidate(
            "revoked-as-current",
            scope.clone(),
            validity_with(
                "current",
                &[("revoked_at", json!("2026-08-15T00:00:00.000000Z"))],
            ),
        ),
        // Claims current while the window is already closed.
        candidate(
            "expired-as-current",
            scope.clone(),
            validity_with(
                "current",
                &[("valid_until", json!("2026-08-31T00:00:00.000000Z"))],
            ),
        ),
        // Claims unknown while asserting supersession.
        candidate(
            "unknown-with-supersession",
            scope.clone(),
            validity_with(
                "unknown",
                &[("superseded_at", json!("2026-08-15T00:00:00.000000Z"))],
            ),
        ),
        // Missing source revision.
        candidate(
            "no-source-revision",
            scope.clone(),
            validity_with("current", &[("source_revision", Value::Null)]),
        ),
        // Not a contract state.
        candidate("bogus-state", scope.clone(), validity_with("live", &[])),
        // Malformed timestamp.
        candidate(
            "bad-timestamp",
            scope.clone(),
            validity_with("current", &[("valid_from", json!("yesterday"))]),
        ),
        // Inverted window.
        candidate(
            "inverted-window",
            scope,
            validity_with(
                "current",
                &[("valid_until", json!("2026-07-01T00:00:00.000000Z"))],
            ),
        ),
    ];
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_exact(),
        candidates,
    )?;
    assert!(admission.admitted.is_empty());
    assert_eq!(admission.report.denied.len(), 7);
    for denied in &admission.report.denied {
        assert!(
            matches!(
                denied.reason,
                RecallDenialReason::InvalidValidityRecord { .. }
            ),
            "{} must be an invalid validity record, got {:?}",
            denied.candidate_id,
            denied.reason
        );
    }
    Ok(())
}

#[test]
fn unknown_validity_policy_is_host_owned() -> Result<(), Box<dyn Error>> {
    let scope = scope_value(&admitted_scope());
    let unknown = || candidate("unknown", scope.clone(), validity_with("unknown", &[]));

    let excluded = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query().with_unknown_validity_policy(UnknownValidityPolicy::Exclude),
        &authorized_exact(),
        vec![unknown()],
    )?;
    assert!(excluded.admitted.is_empty());
    assert_eq!(
        excluded.report.denied[0].reason,
        RecallDenialReason::UnknownValidity
    );

    let degraded = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query().with_unknown_validity_policy(UnknownValidityPolicy::Degrade),
        &authorized_exact(),
        vec![unknown()],
    )?;
    assert_eq!(degraded.admitted.len(), 1);
    assert!(degraded.report.degraded);
    assert_eq!(
        degraded.admitted[0].host_temporal_state(),
        TemporalState::Unknown
    );
    assert!(!degraded.admitted[0].warnings().is_empty());

    let warned = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query().with_unknown_validity_policy(UnknownValidityPolicy::AllowWithWarning),
        &authorized_exact(),
        vec![unknown()],
    )?;
    assert_eq!(warned.admitted.len(), 1);
    assert!(
        warned.report.degraded,
        "warning-only annotation must not bypass the stale lane gate"
    );
    assert!(!warned.admitted[0].warnings().is_empty());
    Ok(())
}

#[test]
fn include_flags_admit_revoked_and_superseded_with_host_state() -> Result<(), Box<dyn Error>> {
    let scope = scope_value(&admitted_scope());
    let candidates = vec![
        candidate(
            "revoked",
            scope.clone(),
            validity_with(
                "revoked",
                &[("revoked_at", json!("2026-08-15T00:00:00.000000Z"))],
            ),
        ),
        candidate(
            "superseded",
            scope,
            validity_with(
                "superseded",
                &[("superseded_at", json!("2026-08-15T00:00:00.000000Z"))],
            ),
        ),
    ];
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query()
            .with_include_revoked(true)
            .with_include_superseded(true),
        &authorized_exact(),
        candidates,
    )?;
    assert_eq!(admission.admitted.len(), 2);
    assert_eq!(
        admission.admitted[0].host_temporal_state(),
        TemporalState::Revoked
    );
    assert_eq!(
        admission.admitted[1].host_temporal_state(),
        TemporalState::Superseded
    );
    assert!(admission.report.denied.is_empty());
    Ok(())
}

#[test]
fn as_of_interval_and_history_modes_evaluate_windows_deterministically()
-> Result<(), Box<dyn Error>> {
    let scope = scope_value(&admitted_scope());
    let windowed = || {
        candidate(
            "windowed",
            scope.clone(),
            validity_with(
                "expired",
                &[
                    ("valid_from", json!("2026-08-01T00:00:00.000000Z")),
                    ("valid_until", json!("2026-08-10T00:00:00.000000Z")),
                ],
            ),
        )
    };

    // Point query before the window closed: the provider's "expired" claim
    // (relative to its evaluation instant) contradicts the as_of instant.
    let as_of = AdmittedTemporalQuery::as_of(EVALUATION_TIME, "2026-08-05T00:00:00.000000Z")?;
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &as_of,
        &authorized_exact(),
        vec![windowed()],
    )?;
    assert!(matches!(
        admission.report.denied[0].reason,
        RecallDenialReason::InvalidValidityRecord { .. }
    ));

    // Interval overlapping the window admits it as current-in-window.
    let interval = AdmittedTemporalQuery::interval(
        EVALUATION_TIME,
        "2026-08-05T00:00:00.000000Z",
        "2026-08-20T00:00:00.000000Z",
    )?;
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &interval,
        &authorized_exact(),
        vec![windowed()],
    )?;
    assert_eq!(admission.admitted.len(), 1);
    assert_eq!(
        admission.admitted[0].host_temporal_state(),
        TemporalState::Current
    );

    // Interval entirely after the window denies it as expired.
    let later = AdmittedTemporalQuery::interval(
        EVALUATION_TIME,
        "2026-08-20T00:00:00.000000Z",
        "2026-08-30T00:00:00.000000Z",
    )?;
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &later,
        &authorized_exact(),
        vec![windowed()],
    )?;
    assert_eq!(
        admission.report.denied[0].reason,
        RecallDenialReason::Expired
    );

    // History retains the expired record with its host-computed state.
    let history = AdmittedTemporalQuery::history(EVALUATION_TIME)?;
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &history,
        &authorized_exact(),
        vec![windowed()],
    )?;
    assert_eq!(admission.admitted.len(), 1);
    assert_eq!(
        admission.admitted[0].host_temporal_state(),
        TemporalState::Expired
    );

    // Malformed temporal queries fail closed before any candidate is seen.
    assert!(matches!(
        AdmittedTemporalQuery::interval(
            EVALUATION_TIME,
            "2026-08-20T00:00:00.000000Z",
            "2026-08-20T00:00:00.000000Z"
        ),
        Err(RecallAdmissionError::InvalidTemporalQuery {
            field: "interval_end",
            ..
        })
    ));
    assert!(matches!(
        AdmittedTemporalQuery::as_of(EVALUATION_TIME, "2027-01-01T00:00:00.000000Z"),
        Err(RecallAdmissionError::InvalidTemporalQuery { field: "as_of", .. })
    ));
    assert!(matches!(
        AdmittedTemporalQuery::current("not-a-time"),
        Err(RecallAdmissionError::InvalidTemporalQuery {
            field: "evaluation_time",
            ..
        })
    ));
    Ok(())
}

#[test]
fn content_binding_is_verified_and_denied_rows_never_carry_content() -> Result<(), Box<dyn Error>> {
    let scope = scope_value(&admitted_scope());
    let mut forged = candidate_value("forged", SECRET_CONTENT, scope.clone(), current_validity());
    forged["content_sha256"] = json!(ZERO_SHA);
    let mut both = candidate_value("both", "inline", scope.clone(), current_validity());
    both["content_ref"] = json!({"reference_kind": "provider_local"});
    let mut neither = candidate_value("neither", "x", scope.clone(), current_validity());
    neither["content"] = Value::Null;
    let mut referenced = candidate_value("referenced", "x", scope.clone(), current_validity());
    referenced["content"] = Value::Null;
    referenced["content_ref"] = json!({
        "reference_kind": "tracedecay_native_fact",
        "reference_identity": "fact-1",
        "reference_revision": "1",
        "content_sha256": ONE_SHA,
        "hydration_authority": "tracedecay",
    });
    let secret_cross_scope = candidate_value(
        "leak",
        SECRET_CONTENT,
        with_scope_field("repository_identity", "other-repo"),
        current_validity(),
    );

    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_exact(),
        vec![
            decode(forged),
            decode(both),
            decode(neither),
            decode(referenced),
            decode(secret_cross_scope),
        ],
    )?;
    let admitted: Vec<_> = admission
        .admitted
        .iter()
        .map(|entry| entry.candidate().candidate_id.as_str())
        .collect();
    assert_eq!(admitted, vec!["referenced"]);
    assert!(matches!(
        admission.admitted[0].content(),
        RecallCandidateContent::Reference(_)
    ));
    let reasons: Vec<_> = admission
        .report
        .denied
        .iter()
        .map(|denied| denied.reason.clone())
        .collect();
    assert_eq!(
        reasons,
        vec![
            RecallDenialReason::ContentDigestMismatch,
            RecallDenialReason::ContentSelectionInvalid,
            RecallDenialReason::ContentSelectionInvalid,
            RecallDenialReason::ScopeMismatch {
                field: ScopeField::RepositoryIdentity
            },
        ]
    );
    // The serialized ledger is audit-visible but structurally content-free.
    let ledger = serde_json::to_string(&admission.report)?;
    assert!(!ledger.contains(SECRET_CONTENT));
    assert!(!ledger.contains("\"content\""));
    assert!(ledger.contains("\"leak\""));
    assert!(ledger.contains("scope_mismatch"));
    assert_eq!(admission.report.received_count, 5);
    assert_eq!(admission.report.admitted_count, 1);
    assert_eq!(
        admission.report.denial_counts(),
        vec![
            ("content_digest_mismatch", 1),
            ("content_selection_invalid", 2),
            ("scope_mismatch", 1),
        ]
    );
    Ok(())
}

#[test]
fn admission_preserves_provider_order_and_is_deterministic() -> Result<(), Box<dyn Error>> {
    let scope = scope_value(&admitted_scope());
    let build = || {
        vec![
            candidate("c", scope.clone(), current_validity()),
            candidate(
                "denied-1",
                with_scope_field("branch_identity", "refs/heads/other"),
                current_validity(),
            ),
            candidate("a", scope.clone(), current_validity()),
            candidate(
                "denied-2",
                scope.clone(),
                validity_with(
                    "revoked",
                    &[("revoked_at", json!("2026-08-15T00:00:00.000000Z"))],
                ),
            ),
            candidate("b", scope.clone(), current_validity()),
        ]
    };
    let first = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_exact(),
        build(),
    )?;
    let second = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_exact(),
        build(),
    )?;
    assert_eq!(first, second);
    let admitted: Vec<_> = first
        .admitted
        .iter()
        .map(|entry| entry.candidate().candidate_id.as_str())
        .collect();
    assert_eq!(admitted, vec!["c", "a", "b"]);
    let denied: Vec<_> = first
        .report
        .denied
        .iter()
        .map(|denied| denied.candidate_id.as_str())
        .collect();
    assert_eq!(denied, vec!["denied-1", "denied-2"]);
    assert_eq!(
        first.report.exact_scope_sha256,
        admitted_scope().exact_scope_sha256()
    );
    assert_eq!(first.report.request_id, "request");
    assert_eq!(first.report.temporal_mode, "current");

    let duplicate = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_exact(),
        vec![
            candidate("dup", scope.clone(), current_validity()),
            candidate("dup", scope, current_validity()),
        ],
    );
    assert_eq!(
        duplicate.err(),
        Some(RecallAdmissionError::DuplicateCandidateId("dup".to_owned()))
    );
    Ok(())
}

#[test]
fn overflowing_confidence_number_decodes_to_typed_candidate_denial() -> Result<(), Box<dyn Error>> {
    let port = Arc::new(RecallFixturePort::new());
    let composition = compose(port.clone());
    let registry = composition.registry().expect("enabled registry");
    let response = registry.handshake(&handshake())?;
    let temporal = current_query();
    let call = recall_call(&response, &temporal);

    let id = "overflow-confidence";
    let literal = "1e400";
    let mut outcome = port.outcome_value(&call);
    outcome["candidates"] = json!([candidate_value(
        id,
        &format!("content of {id}"),
        scope_value(&admitted_scope()),
        current_validity(),
    )]);
    let json = serde_json::to_string(&outcome)?;
    let json = json.replacen(
        "\"confidence\":null",
        &format!("\"confidence\":{literal}"),
        1,
    );
    let bytes = json.into_bytes();
    let payload = CanonicalPayload::new(
        OwnedVersionedId::new(RECALL_PAYLOAD_CONTRACT_ID)?,
        bytes.clone(),
        sha256_hex(&bytes),
    )?;

    let mut reply = registry.invoke_active(&call)?;
    reply.payload = Some(payload);
    let admission = admit_recall_reply(&call, &temporal, 8, &authorized_exact(), &reply)?;
    assert!(admission.admitted.is_empty(), "{literal}");
    assert_eq!(admission.report.denied.len(), 1, "{literal}");
    assert_eq!(
        admission.report.denied[0].reason,
        RecallDenialReason::ConfidenceMalformed {
            defect: RecallConfidenceDefect::NotFinite,
        },
        "{literal}"
    );
    Ok(())
}

#[test]
fn invalid_confidence_literals_are_payload_decode_errors() -> Result<(), Box<dyn Error>> {
    let port = Arc::new(RecallFixturePort::new());
    let composition = compose(port.clone());
    let registry = composition.registry().expect("enabled registry");
    let response = registry.handshake(&handshake())?;
    let temporal = current_query();
    let call = recall_call(&response, &temporal);
    let mut outcome = port.outcome_value(&call);
    outcome["candidates"] = json!([candidate_value(
        "invalid-confidence",
        "content of invalid-confidence",
        scope_value(&admitted_scope()),
        current_validity(),
    )]);
    let json = serde_json::to_string(&outcome)?;

    for literal in ["NaN", "Infinity", "-Infinity"] {
        for leading_whitespace in ["", " \t\r\n"] {
            let invalid_json = json.replacen(
                "\"confidence\":null",
                &format!("\"confidence\":{literal}"),
                1,
            );
            let bytes = format!("{leading_whitespace}{invalid_json}").into_bytes();
            let payload = CanonicalPayload::new(
                OwnedVersionedId::new(RECALL_PAYLOAD_CONTRACT_ID)?,
                bytes.clone(),
                sha256_hex(&bytes),
            )?;
            let mut reply = registry.invoke_active(&call)?;
            reply.payload = Some(payload);

            assert!(
                matches!(
                    admit_recall_reply(&call, &temporal, 8, &authorized_exact(), &reply),
                    Err(RecallAdmissionError::PayloadDecode { .. })
                ),
                "literal={literal}, leading_whitespace={leading_whitespace:?}"
            );
        }
    }
    Ok(())
}

#[test]
fn confidence_f64_rounding_at_the_upper_boundary_is_admitted() -> Result<(), Box<dyn Error>> {
    let port = Arc::new(RecallFixturePort::new());
    let composition = compose(port.clone());
    let registry = composition.registry().expect("enabled registry");
    let response = registry.handshake(&handshake())?;
    let temporal = current_query();
    let call = recall_call(&response, &temporal);
    let mut outcome = port.outcome_value(&call);
    outcome["candidates"] = json!([candidate_value(
        "rounded-confidence",
        "content of rounded-confidence",
        scope_value(&admitted_scope()),
        current_validity(),
    )]);
    let json = serde_json::to_string(&outcome)?.replacen(
        "\"confidence\":null",
        "\"confidence\":1.0000000000000001",
        1,
    );
    let bytes = json.into_bytes();
    let payload = CanonicalPayload::new(
        OwnedVersionedId::new(RECALL_PAYLOAD_CONTRACT_ID)?,
        bytes.clone(),
        sha256_hex(&bytes),
    )?;
    let mut reply = registry.invoke_active(&call)?;
    reply.payload = Some(payload);

    let admission = admit_recall_reply(&call, &temporal, 8, &authorized_exact(), &reply)?;
    assert_eq!(admission.admitted.len(), 1);
    assert!(admission.report.denied.is_empty());
    assert_eq!(
        admission.admitted[0]
            .confidence()
            .and_then(serde_json::Number::as_f64),
        Some(1.0)
    );
    Ok(())
}

#[test]
fn nonfinite_literals_outside_candidate_confidence_remain_payload_decode_errors()
-> Result<(), Box<dyn Error>> {
    let port = Arc::new(RecallFixturePort::new());
    let composition = compose(port.clone());
    let registry = composition.registry().expect("enabled registry");
    let response = registry.handshake(&handshake())?;
    let call = recall_call(&response, &current_query());
    let mut outcome = port.outcome_value(&call);
    outcome["candidates"] = json!([candidate_value(
        "nested-nan",
        "content of nested-nan",
        scope_value(&admitted_scope()),
        current_validity(),
    )]);
    let json = serde_json::to_string(&outcome)?;
    let json = json.replacen("\"provenance\":{", "\"provenance\":{\"confidence\":NaN,", 1);
    let bytes = json.into_bytes();
    let payload = CanonicalPayload::new(
        OwnedVersionedId::new(RECALL_PAYLOAD_CONTRACT_ID)?,
        bytes.clone(),
        sha256_hex(&bytes),
    )?;

    assert!(matches!(
        decode_recall_outcome(&payload),
        Err(RecallAdmissionError::PayloadDecode { .. })
    ));
    Ok(())
}

#[test]
fn candidate_wire_shape_is_closed() {
    let scope = scope_value(&admitted_scope());
    let mut extra = candidate_value("extra", "x", scope.clone(), current_validity());
    extra["provider_private"] = json!(true);
    assert!(serde_json::from_value::<RecallCandidateV1>(extra).is_err());

    let mut missing_confidence_field =
        candidate_value("missing-confidence", "x", scope.clone(), current_validity());
    missing_confidence_field
        .as_object_mut()
        .expect("candidate object")
        .remove("confidence");
    assert!(
        serde_json::from_value::<RecallCandidateV1>(missing_confidence_field).is_err(),
        "confidence must be present even when its value is explicitly null"
    );

    let mut missing_native_score_field =
        candidate_value("missing-score", "x", scope.clone(), current_validity());
    missing_native_score_field
        .as_object_mut()
        .expect("candidate object")
        .remove("native_score");
    assert!(
        serde_json::from_value::<RecallCandidateV1>(missing_native_score_field).is_err(),
        "native_score must be present even when its value is explicitly null"
    );

    let mut missing_validity_field =
        candidate_value("missing", "x", scope.clone(), current_validity());
    missing_validity_field["validity"]
        .as_object_mut()
        .expect("validity object")
        .remove("revoked_at");
    assert!(
        serde_json::from_value::<RecallCandidateV1>(missing_validity_field).is_err(),
        "a nullable validity field must still be present on the wire"
    );

    let mut extra_scope = candidate_value("scope", "x", scope, current_validity());
    extra_scope["exact_scope_identity"]["path"] = json!("/tmp/repo");
    assert!(serde_json::from_value::<RecallCandidateV1>(extra_scope).is_err());
}

#[test]
fn request_payload_carries_exactly_the_admitted_scope_and_temporal_query()
-> Result<(), Box<dyn Error>> {
    let temporal = current_query().with_unknown_validity_policy(UnknownValidityPolicy::Degrade);
    let payload = build_recall_request_payload(&RecallRequestParts {
        provider_id: OwnedProviderId::new(NATIVE_PROVIDER_ID)?,
        registration_revision: 31,
        ready_receipt_sha256: ONE_SHA.to_owned(),
        exact_scope: admitted_scope(),
        request_id: "r".to_owned(),
        objective: "o".to_owned(),
        query: "q".to_owned(),
        temporal: temporal.clone(),
        budgets: budgets(),
        policy_revision: 3,
        deadline_utc_micros: 42,
        remaining_millis: 7,
    })?;
    assert_eq!(payload.contract_id.as_str(), RECALL_PAYLOAD_CONTRACT_ID);
    let value: Value = serde_json::from_slice(&payload.bytes)?;
    let mut keys: Vec<_> = value
        .as_object()
        .expect("request object")
        .keys()
        .cloned()
        .collect();
    keys.sort();
    let mut expected: Vec<_> =
        tracedecay_memory_provider_api::contract::RECALL_REQUEST_REQUIRED_FIELDS
            .iter()
            .map(|field| (*field).to_owned())
            .collect();
    expected.sort();
    assert_eq!(keys, expected);
    assert_eq!(
        value["exact_scope_identity"],
        scope_value(&admitted_scope())
    );
    assert_eq!(value["temporal_query"], temporal.to_wire_value());
    assert_eq!(
        value["temporal_query"]["unknown_validity_policy"],
        json!("degrade")
    );
    assert_eq!(
        value["required_capabilities"],
        json!([RECALL_QUERY_CAPABILITY_ID])
    );
    assert_eq!(
        value["deadline"],
        json!({"deadline_utc_micros": 42, "remaining_millis": 7})
    );
    assert_eq!(value["cancellation"], json!("live"));
    let round_trip: AdmittedTemporalQuery =
        serde_json::from_value(value["temporal_query"].clone())?;
    assert_eq!(round_trip, temporal);

    let zero_budget = build_recall_request_payload(&RecallRequestParts {
        provider_id: OwnedProviderId::new(NATIVE_PROVIDER_ID)?,
        registration_revision: 31,
        ready_receipt_sha256: ONE_SHA.to_owned(),
        exact_scope: admitted_scope(),
        request_id: "r".to_owned(),
        objective: "o".to_owned(),
        query: "q".to_owned(),
        temporal,
        budgets: RecallBudgetsV1 {
            maximum_candidates: 0,
            ..budgets()
        },
        policy_revision: 3,
        deadline_utc_micros: 42,
        remaining_millis: 7,
    });
    assert!(matches!(
        zero_budget,
        Err(RecallAdmissionError::InvalidTemporalQuery {
            field: "maximum_candidates",
            ..
        })
    ));
    Ok(())
}

#[test]
fn real_fabric_route_admits_only_exact_scope_current_candidates() -> Result<(), Box<dyn Error>> {
    let port = Arc::new(RecallFixturePort::new());
    let composition = compose(port.clone());
    let registry = composition.registry().expect("enabled registry");
    let response = registry.handshake(&handshake())?;
    let temporal = current_query();
    let call = recall_call(&response, &temporal);

    let reply = registry.invoke_active(&call)?;
    assert_eq!(reply.terminal.terminal_code(), TerminalCode::Success);
    let admission = admit_recall_reply(&call, &temporal, 8, &authorized_native(), &reply)?;

    let admitted: Vec<_> = admission
        .admitted
        .iter()
        .map(|entry| entry.candidate().candidate_id.as_str())
        .collect();
    assert_eq!(admitted, vec!["in-scope-1", "in-scope-2"]);
    let denied: Vec<_> = admission
        .report
        .denied
        .iter()
        .map(|denied| (denied.candidate_id.as_str(), denied.reason.clone()))
        .collect();
    assert_eq!(
        denied,
        vec![
            (
                "cross-worktree",
                RecallDenialReason::ScopeMismatch {
                    field: ScopeField::WorktreeIdentity
                }
            ),
            ("revoked", RecallDenialReason::Revoked),
            (
                "cross-repository",
                RecallDenialReason::ScopeMismatch {
                    field: ScopeField::RepositoryIdentity
                }
            ),
            ("stale-exact-scope", RecallDenialReason::StaleIdentity),
        ]
    );
    assert_eq!(
        admission.report.authorized_scope_bindings,
        *registry
            .recall_scope_bindings(&call.provider_id)
            .expect("native bindings recorded at registration")
    );
    assert_eq!(
        admission.report.authorized_scope_bindings,
        authorized_native()
    );
    for entry in &admission.admitted {
        assert_eq!(entry.scope_binding(), ScopeBinding::ProjectFacts);
    }
    let ledger = serde_json::to_string(&admission.report)?;
    assert!(!ledger.contains(SECRET_CONTENT));
    for entry in &admission.admitted {
        let RecallCandidateContent::Inline(content) = entry.content() else {
            panic!("fixture candidates are inline");
        };
        assert!(!content.contains(SECRET_CONTENT));
    }
    assert_eq!(port.recall_calls.load(Ordering::Relaxed), 1);

    // The budget the host admitted binds the outcome.
    assert_eq!(
        admit_recall_reply(&call, &temporal, 3, &authorized_native(), &reply).err(),
        Some(RecallAdmissionError::CandidateBudgetExceeded {
            returned: 6,
            maximum: 3
        })
    );
    Ok(())
}

#[test]
fn unattributable_outcomes_and_failed_terminals_are_typed_errors_not_empty_success()
-> Result<(), Box<dyn Error>> {
    let mut mismatched = RecallFixturePort::new();
    mismatched.outcome_request_identity = Some("someone-elses-request".to_owned());
    let composition = compose(Arc::new(mismatched));
    let registry = composition.registry().expect("enabled registry");
    let response = registry.handshake(&handshake())?;
    let temporal = current_query();
    let call = recall_call(&response, &temporal);
    let reply = registry.invoke_active(&call)?;
    assert_eq!(
        admit_recall_reply(&call, &temporal, 8, &authorized_native(), &reply).err(),
        Some(RecallAdmissionError::OutcomeBinding {
            field: "request_identity"
        })
    );

    let mut unavailable = RecallFixturePort::new();
    unavailable.terminal_code = TerminalCode::ProviderUnavailable;
    let composition = compose(Arc::new(unavailable));
    let registry = composition.registry().expect("enabled registry");
    let response = registry.handshake(&handshake())?;
    let call = recall_call(&response, &temporal);
    let reply = registry.invoke_active(&call)?;
    assert_eq!(
        admit_recall_reply(&call, &temporal, 8, &authorized_native(), &reply).err(),
        Some(RecallAdmissionError::TerminalNotSuccessful {
            terminal_code: TerminalCode::ProviderUnavailable
        })
    );

    let wrong_contract = CanonicalPayload::new(
        OwnedVersionedId::new("tracedecay.memory.provider.health.v1")?,
        b"{}".to_vec(),
        sha256_hex(b"{}"),
    )?;
    assert!(matches!(
        decode_recall_outcome(&wrong_contract),
        Err(RecallAdmissionError::PayloadContractMismatch { .. })
    ));
    Ok(())
}

// --- Scope bindings ---------------------------------------------------------

fn admit_native(
    candidates: Vec<RecallCandidateV1>,
) -> Result<Vec<RecallDenialReason>, Box<dyn Error>> {
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_native(),
        candidates,
    )?;
    Ok(admission
        .report
        .denied
        .iter()
        .map(|denied| denied.reason.clone())
        .collect())
}

#[test]
fn scope_binding_wire_values_match_the_generated_contract() -> Result<(), Box<dyn Error>> {
    use tracedecay_memory_provider_api::contract::{
        RECALL_CANDIDATE_SCOPE_REQUIRED_FIELDS, RecallScopeBinding,
    };
    let bindings = [
        ScopeBinding::ExactCodingScope,
        ScopeBinding::ProjectFacts,
        ScopeBinding::ProfileFacts,
    ];
    for binding in bindings {
        let wire = binding.as_wire();
        assert_eq!(
            RecallScopeBinding::from_wire(wire).map(RecallScopeBinding::as_wire),
            Some(wire),
            "{wire} must be a generated contract value"
        );
        assert_eq!(ScopeBinding::from_wire(wire), Some(binding));
        assert_eq!(serde_json::to_value(binding)?, json!(wire));
        assert_eq!(
            serde_json::from_value::<ScopeBinding>(json!(wire))?,
            binding
        );
    }
    assert_eq!(ScopeBinding::from_wire("session_facts"), None);
    assert!(serde_json::from_value::<ScopeBinding>(json!("session_facts")).is_err());

    let mut keys: Vec<_> = exact_scope_candidate_value(&admitted_scope())
        .as_object()
        .expect("scope object")
        .keys()
        .cloned()
        .collect();
    keys.sort();
    let mut expected: Vec<_> = RECALL_CANDIDATE_SCOPE_REQUIRED_FIELDS
        .iter()
        .map(|field| (*field).to_owned())
        .collect();
    expected.sort();
    assert_eq!(keys, expected);
    Ok(())
}

#[test]
fn candidate_without_or_with_unknown_scope_binding_is_a_contract_violation() {
    let mut missing = candidate_value(
        "missing",
        "x",
        exact_scope_candidate_value(&admitted_scope()),
        current_validity(),
    );
    missing["exact_scope_identity"]
        .as_object_mut()
        .expect("scope object")
        .remove("scope_binding");
    assert!(serde_json::from_value::<RecallCandidateV1>(missing).is_err());

    let mut unknown = candidate_value(
        "unknown",
        "x",
        exact_scope_candidate_value(&admitted_scope()),
        current_validity(),
    );
    unknown["exact_scope_identity"]["scope_binding"] = json!("session_facts");
    assert!(serde_json::from_value::<RecallCandidateV1>(unknown).is_err());
}

#[test]
fn project_facts_candidate_with_empty_optionals_is_admitted() -> Result<(), Box<dyn Error>> {
    let mut scope = project_facts_candidate_value(&admitted_scope());
    for optional in [
        "repository_identity",
        "worktree_identity",
        "branch_identity",
    ] {
        scope[optional] = json!("");
    }
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_native(),
        vec![
            candidate("project-fact", scope, current_validity()),
            candidate(
                "project-fact-checkout",
                project_facts_candidate_value(&admitted_scope()),
                current_validity(),
            ),
        ],
    )?;
    assert!(admission.report.denied.is_empty());
    let admitted: Vec<_> = admission
        .admitted
        .iter()
        .map(|entry| {
            (
                entry.candidate().candidate_id.as_str(),
                entry.scope_binding(),
            )
        })
        .collect();
    assert_eq!(
        admitted,
        vec![
            ("project-fact", ScopeBinding::ProjectFacts),
            ("project-fact-checkout", ScopeBinding::ProjectFacts),
        ]
    );
    assert_eq!(
        admission.report.authorized_scope_bindings,
        authorized_native()
    );
    Ok(())
}

#[test]
fn project_facts_candidate_from_another_worktree_of_the_same_project_is_denied()
-> Result<(), Box<dyn Error>> {
    let mut scope = project_facts_candidate_value(&admitted_scope());
    scope["worktree_identity"] = json!("worktree-other");
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_native(),
        vec![candidate("other-worktree", scope, current_validity())],
    )?;
    assert!(admission.admitted.is_empty());
    let denied = &admission.report.denied[0];
    assert_eq!(
        denied.reason,
        RecallDenialReason::ScopeMismatch {
            field: ScopeField::WorktreeIdentity
        }
    );
    assert_eq!(denied.reason.label(), "scope_mismatch");
    assert_eq!(
        denied.provider_claimed_scope_binding,
        ScopeBinding::ProjectFacts
    );
    assert_eq!(
        denied.provider_claimed_scope_sha256, None,
        "a partial binding has no exact-scope digest to claim"
    );
    let ledger = serde_json::to_value(&admission.report)?;
    assert_eq!(
        ledger["denied"][0]["provider_claimed_scope_binding"],
        json!("project_facts")
    );
    assert_eq!(
        ledger["authorized_scope_bindings"],
        json!(["exact_coding_scope", "project_facts", "profile_facts"])
    );
    Ok(())
}

#[test]
fn project_facts_candidate_carrying_forbidden_identity_is_denied() -> Result<(), Box<dyn Error>> {
    let mut with_session = project_facts_candidate_value(&admitted_scope());
    with_session["agent_session_id"] = json!(admitted_scope().agent_session_id);
    let mut with_digest = project_facts_candidate_value(&admitted_scope());
    with_digest["resolved_scope_digest"] = json!(admitted_scope().resolved_scope_digest);
    let denied = admit_native(vec![
        candidate("session", with_session, current_validity()),
        candidate("digest", with_digest, current_validity()),
    ])?;
    assert_eq!(
        denied,
        vec![
            RecallDenialReason::ForbiddenIdentity {
                field: ScopeField::AgentSessionId
            },
            RecallDenialReason::ForbiddenIdentity {
                field: ScopeField::ResolvedScopeDigest
            },
        ]
    );
    assert_eq!(denied[0].label(), "forbidden_identity");
    Ok(())
}

#[test]
fn unauthorized_scope_binding_is_denied_before_any_field_comparison() -> Result<(), Box<dyn Error>>
{
    // A provider authorized only for owner facts cannot attest
    // exact_coding_scope, even with a scope that would match byte-for-byte:
    // the binding is refused before any field is compared.
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_facts_only(),
        vec![candidate(
            "exact",
            exact_scope_candidate_value(&admitted_scope()),
            current_validity(),
        )],
    )?;
    assert!(admission.admitted.is_empty());
    let denied: Vec<_> = admission
        .report
        .denied
        .iter()
        .map(|denied| denied.reason.clone())
        .collect();
    assert_eq!(
        denied,
        vec![RecallDenialReason::ScopeBindingUnauthorized {
            binding: ScopeBinding::ExactCodingScope
        }]
    );
    assert_eq!(denied[0].label(), "scope_binding_unauthorized");

    // An exact-scope provider cannot attest project_facts.
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_exact(),
        vec![candidate(
            "project-fact",
            project_facts_candidate_value(&admitted_scope()),
            current_validity(),
        )],
    )?;
    assert_eq!(
        admission.report.denied[0].reason,
        RecallDenialReason::ScopeBindingUnauthorized {
            binding: ScopeBinding::ProjectFacts
        }
    );

    // No recorded authorization admits nothing.
    let none = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &RecallScopeBindingsV1::default(),
        vec![candidate(
            "exact",
            exact_scope_candidate_value(&admitted_scope()),
            current_validity(),
        )],
    )?;
    assert!(none.admitted.is_empty());
    assert_eq!(none.report.denied.len(), 1);
    Ok(())
}

#[test]
fn exact_coding_scope_rules_are_unchanged_by_bindings() -> Result<(), Box<dyn Error>> {
    // Optional-under-project_facts fields stay required under exact scope.
    for field in [
        ("repository_identity", ScopeField::RepositoryIdentity),
        ("worktree_identity", ScopeField::WorktreeIdentity),
        ("branch_identity", ScopeField::BranchIdentity),
    ] {
        let admission = admit_recall_candidates(
            &admitted_scope(),
            "request",
            &current_query(),
            &authorized_exact(),
            vec![candidate(
                "partial",
                with_scope_field(field.0, ""),
                current_validity(),
            )],
        )?;
        assert_eq!(
            admission.report.denied[0].reason,
            RecallDenialReason::UnknownIdentity { field: field.1 },
            "{}",
            field.0
        );
    }
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_exact(),
        vec![candidate(
            "exact",
            exact_scope_candidate_value(&admitted_scope()),
            current_validity(),
        )],
    )?;
    assert_eq!(admission.admitted.len(), 1);
    assert_eq!(
        admission.admitted[0].scope_binding(),
        ScopeBinding::ExactCodingScope
    );
    Ok(())
}

#[test]
fn profile_facts_candidate_binds_only_the_profile() -> Result<(), Box<dyn Error>> {
    let profile_only = json!({
        "scope_binding": "profile_facts",
        "profile_id": admitted_scope().profile_id,
        "project_id": "",
        "repository_identity": "",
        "worktree_identity": "",
        "branch_identity": "",
        "agent_session_id": "",
        "resolved_scope_digest": "",
    });
    let mut with_project = profile_only.clone();
    with_project["project_id"] = json!(admitted_scope().project_id);
    let mut other_profile = profile_only.clone();
    other_profile["profile_id"] = json!("profile-other");
    let mut empty_profile = profile_only.clone();
    empty_profile["profile_id"] = json!("");

    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_native(),
        vec![
            candidate("profile-fact", profile_only, current_validity()),
            candidate("with-project", with_project, current_validity()),
            candidate("other-profile", other_profile, current_validity()),
            candidate("empty-profile", empty_profile, current_validity()),
        ],
    )?;
    let admitted: Vec<_> = admission
        .admitted
        .iter()
        .map(|entry| {
            (
                entry.candidate().candidate_id.as_str(),
                entry.scope_binding(),
            )
        })
        .collect();
    assert_eq!(admitted, vec![("profile-fact", ScopeBinding::ProfileFacts)]);
    let denied: Vec<_> = admission
        .report
        .denied
        .iter()
        .map(|denied| (denied.candidate_id.as_str(), denied.reason.clone()))
        .collect();
    assert_eq!(
        denied,
        vec![
            (
                "with-project",
                RecallDenialReason::ForbiddenIdentity {
                    field: ScopeField::ProjectId
                }
            ),
            (
                "other-profile",
                RecallDenialReason::ScopeMismatch {
                    field: ScopeField::ProfileId
                }
            ),
            (
                "empty-profile",
                RecallDenialReason::UnknownIdentity {
                    field: ScopeField::ProfileId
                }
            ),
        ]
    );
    Ok(())
}

#[test]
fn project_facts_candidate_from_a_foreign_project_or_profile_is_denied()
-> Result<(), Box<dyn Error>> {
    let mut foreign_project = project_facts_candidate_value(&admitted_scope());
    foreign_project["project_id"] = json!("project-other");
    let mut foreign_profile = project_facts_candidate_value(&admitted_scope());
    foreign_profile["profile_id"] = json!("profile-other");
    let mut malformed = project_facts_candidate_value(&admitted_scope());
    malformed["project_id"] = json!(" project-recall");
    let denied = admit_native(vec![
        candidate("foreign-project", foreign_project, current_validity()),
        candidate("foreign-profile", foreign_profile, current_validity()),
        candidate("malformed", malformed, current_validity()),
    ])?;
    assert_eq!(
        denied,
        vec![
            RecallDenialReason::ScopeMismatch {
                field: ScopeField::ProjectId
            },
            RecallDenialReason::ScopeMismatch {
                field: ScopeField::ProfileId
            },
            RecallDenialReason::UnknownIdentity {
                field: ScopeField::ProjectId
            },
        ]
    );
    Ok(())
}

/// A staged session observation may only be admitted under
/// `exact_coding_scope`, whatever else its provider is authorized for.
///
/// The real defect this catches is provider-wide authorization being read as
/// per-candidate authorization. Native is authorized for `exact_coding_scope`,
/// `project_facts`, and `profile_facts`, so nothing in binding authorization
/// alone stops a provider-local staged row from claiming `project_facts` —
/// and that binding makes the checkout fields optional and *forbids* the
/// session identity and the resolved scope digest, so the row would be
/// admitted in another checkout, another branch, or another agent session
/// than the one it was observed in. The class-to-binding policy denies it
/// before any field is compared, which is why the mutated candidates below
/// carry a scope that would otherwise pass.
#[test]
fn a_staged_session_observation_cannot_be_admitted_as_project_or_profile_facts()
-> Result<(), Box<dyn Error>> {
    for (binding, scope) in [
        (
            ScopeBinding::ProjectFacts,
            project_facts_candidate_value(&admitted_scope()),
        ),
        (
            ScopeBinding::ProfileFacts,
            json!({
                "scope_binding": "profile_facts",
                "profile_id": admitted_scope().profile_id,
                "project_id": "",
                "repository_identity": "",
                "worktree_identity": "",
                "branch_identity": "",
                "agent_session_id": "",
                "resolved_scope_digest": "",
            }),
        ),
    ] {
        let mut value = candidate_value("staged", "staged session text", scope, current_validity());
        value["memory_class"] = json!("session_observation");
        let admission = admit_recall_candidates(
            &admitted_scope(),
            "request",
            &current_query(),
            // Native's own authorization set: the binding itself is allowed,
            // so only the class policy can refuse this candidate.
            &authorized_native(),
            vec![decode(value)],
        )?;
        assert!(admission.admitted.is_empty(), "{:?}", admission.admitted);
        assert_eq!(
            admission.report.denied[0].reason,
            RecallDenialReason::MemoryClassBindingUnauthorized {
                memory_class: "session_observation".to_owned(),
                binding,
            }
        );
        assert_eq!(
            admission.report.denied[0].reason.label(),
            "memory_class_binding_unauthorized"
        );
    }

    // The same class under the binding it belongs to is admitted, so the
    // policy is a class-to-binding rule and not a blanket refusal.
    let mut value = candidate_value(
        "staged",
        "staged session text",
        exact_scope_candidate_value(&admitted_scope()),
        current_validity(),
    );
    value["memory_class"] = json!("session_observation");
    let admission = admit_recall_candidates(
        &admitted_scope(),
        "request",
        &current_query(),
        &authorized_native(),
        vec![decode(value)],
    )?;
    assert_eq!(admission.admitted.len(), 1);
    assert!(admission.report.denied.is_empty());
    Ok(())
}
