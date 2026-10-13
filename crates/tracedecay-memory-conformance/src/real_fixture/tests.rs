use std::time::Duration;

use super::*;
use crate::compatibility::{T1, common_advisory_scenarios, common_observation};
use serde_json::json;
use tracedecay_memory_provider_api::contract::{CommittedEffectState, TerminalCode};
use tracedecay_memory_provider_api::{
    CancellationToken, CanonicalPayload, CommittedEffectEvidence, CommittedEffectEvidenceParts,
    FallbackDirective, HandshakeRequest, HandshakeRequestParts, HandshakeResponse,
    OperationControl, OwnedOpaqueExtension, OwnedVersionedId, PayloadSanitizationReceipt,
    PayloadSanitizationReceiptParts, PinnedFallbackPolicy, ProviderCallParts, ProviderDescriptor,
    ProviderLimits, ProviderReply, TerminalRecord,
};

fn scope() -> Result<OwnedExactScope, String> {
    OwnedExactScope::new(
        "fixture-profile",
        "fixture-project",
        "fixture-repository",
        "fixture-worktree",
        "fixture-branch",
        "fixture-session",
        format!("sha256:{}", "a".repeat(64)),
    )
    .map_err(err)
}
fn call(
    operation: ProviderOperation,
    value: &Value,
    scope: &OwnedExactScope,
) -> Result<ProviderCall, String> {
    let payload = CanonicalPayload::new(
        OwnedVersionedId::new("tracedecay.memory.provider.fixture.v1").map_err(err)?,
        crate::canonical_json(value).map_err(err)?,
        crate::canonical_json_sha256(value).map_err(err)?,
    )
    .map_err(err)?;
    let receipt =
        PayloadSanitizationReceipt::new(PayloadSanitizationReceiptParts::accepted_unmodified(
            "fixture-sanitizer",
            payload.sha256.clone(),
        ))
        .map_err(err)?;
    let call = ProviderCall::new(ProviderCallParts {
        operation,
        provider_id: OwnedProviderId::new("fixture-provider").map_err(err)?,
        registration_revision: 3,
        ready_receipt_sha256: "b".repeat(64),
        exact_scope: scope.clone(),
        request_id: "fixture.request.1".into(),
        operation_id: "fixture.operation.1".into(),
        expected_state_generation: 7,
        idempotency_key: operation
            .mutates_provider_state()
            .then(|| "fixture.delivery.1".into()),
        control: OperationControl::new(i64::MAX, 30_000, CancellationToken::new()),
        payload,
        required_capabilities: vec![OwnedVersionedId::new(operation.capability_id()).map_err(err)?],
        extensions: vec![],
    })
    .map_err(err)?;
    Ok(if operation == ProviderOperation::Observe {
        call.with_sanitization(receipt)
    } else {
        call
    })
}
fn source() -> Result<Value, String> {
    Ok(common_observation(
        &scope()?,
        1,
        Some("opaque_revision"),
        "compatibility beacon original",
        Some(T1),
        None,
    ))
}
fn authority(observation: &Value) -> Result<FixtureAuthority, String> {
    let mut state = FixtureAuthoritySnapshot {
        base_scope: crate::compatibility::scope_json(&scope()?),
        observations: BTreeMap::new(),
        dispositions: BTreeMap::new(),
        available: true,
        revision: 1,
    };
    register(&mut state, observation)?;
    FixtureAuthority::from_snapshot(state)
}
fn history(observation: &Value, destination: &OwnedExactScope) -> Value {
    json!({"history_grant":{"authorization_ref":ADMISSION_REF,"policy_revision":1,
        "destination_scope":crate::compatibility::scope_json(destination),"relation":"same_checkout",
        "sources":[{"attribution":observation["source_identity"]["original_source"],"current_disposition":{"state":"available"}}]}})
}

#[test]
fn every_unchanged_scenario_registers_without_provider_or_expected_result_rules()
-> Result<(), String> {
    let scope = scope()?;
    for scenario in common_advisory_scenarios(&scope, 1)? {
        let authority = FixtureAuthority::from_scenario(&scenario, &scope)?;
        let encoded = serde_json::to_vec(&authority.snapshot()?).map_err(err)?;
        let restored =
            FixtureAuthority::from_snapshot(serde_json::from_slice(&encoded).map_err(err)?)?;
        assert_eq!(
            serde_json::to_value(authority.snapshot()?).map_err(err)?,
            serde_json::to_value(restored.snapshot()?).map_err(err)?
        );
    }
    Ok(())
}

#[test]
fn authority_checks_immutable_sources_and_current_disposition_after_restart() -> Result<(), String>
{
    let original = source()?;
    let authority = authority(&original)?;
    let original_call = call(ProviderOperation::Observe, &original, &scope()?)?;
    let admitted = authority.admit(&original_call).map_err(err)?;
    assert_eq!(admitted.history_sources.len(), 1);
    let mut destination = scope()?;
    destination.agent_session_id = "destination-session".into();
    let mut claim = history(&original, &destination);
    let recall = call(ProviderOperation::Recall, &claim, &destination)?;
    let admitted = authority.admit(&recall).map_err(err)?;
    assert_eq!(
        admitted.history_sources[0].current_disposition.state,
        SourceDisposition::Available
    );
    assert_eq!(
        admitted.history_sources[0].attribution,
        parse_attribution(&original["source_identity"]["original_source"])?
    );
    assert!(
        admitted
            .verify_for(&call(ProviderOperation::Recall, &claim, &destination)?)
            .is_err()
    );
    authority.record_disposition("common-advisory/source/1", SourceDisposition::Deleted)?;
    let restored = FixtureAuthority::from_snapshot(authority.snapshot()?)?;
    assert_eq!(
        restored.admit(&recall).map_err(err)?.history_sources[0]
            .current_disposition
            .state,
        SourceDisposition::Deleted
    );
    assert_eq!(
        claim["history_grant"]["sources"][0]["current_disposition"]["state"],
        "available"
    );
    restored.set_available(false)?;
    assert!(matches!(
        restored.admit(&recall),
        Err(AdvisoryAdmissionError::Unavailable(_))
    ));
    restored.set_available(true)?;
    assert!(restored.admit(&recall).is_ok());
    claim["history_grant"]["authorization_ref"] = json!("forged");
    assert!(
        restored
            .admit(&call(ProviderOperation::Recall, &claim, &destination)?)
            .is_err()
    );
    let mut changed = original.clone();
    changed["source_identity"]["original_source"]["source"]["source_revision"] = json!("invented");
    assert!(
        restored
            .admit(&call(ProviderOperation::Observe, &changed, &scope()?)?)
            .is_err()
    );
    Ok(())
}

#[test]
fn authority_denies_foreign_checkout_and_unregistered_restore_inventory() -> Result<(), String> {
    let original = source()?;
    let authority = authority(&original)?;
    for field in ["worktree", "branch", "project"] {
        let mut foreign = scope()?;
        match field {
            "worktree" => foreign.worktree_identity = "foreign".into(),
            "branch" => foreign.branch_identity = "foreign".into(),
            _ => foreign.project_id = "foreign".into(),
        }
        assert!(
            authority
                .admit(&call(
                    ProviderOperation::Recall,
                    &history(&original, &foreign),
                    &foreign
                )?)
                .is_err()
        );
        assert!(
            authority
                .admit(&call(ProviderOperation::Recall, &json!({}), &foreign)?)
                .map_err(err)?
                .history_sources
                .is_empty()
        );
    }
    let mut restore =
        json!({"snapshot":{"sources":[original["source_identity"]["original_source"]["source"]]}});
    let admitted = authority
        .admit(&call(
            ProviderOperation::SnapshotRestore,
            &restore,
            &scope()?,
        )?)
        .map_err(err)?;
    let checkpoint = admitted.restore.ok_or("missing restore admission")?;
    assert_eq!(checkpoint.sources.len(), 1);
    authority.record_disposition("common-advisory/source/1", SourceDisposition::Redacted)?;
    assert_eq!(
        authority
            .admit(&call(
                ProviderOperation::SnapshotRestore,
                &restore,
                &scope()?
            )?)
            .map_err(err)?
            .restore
            .ok_or("missing restore")?
            .sources[0]
            .1
            .state,
        SourceDisposition::Redacted
    );
    restore["snapshot"]["sources"][0]["source_revision"] = json!("unregistered");
    assert!(
        authority
            .admit(&call(
                ProviderOperation::SnapshotRestore,
                &restore,
                &scope()?
            )?)
            .is_err()
    );
    Ok(())
}

#[test]
fn replay_requires_original_receipts_and_exact_registered_payload() -> Result<(), String> {
    let original = source()?;
    let authority = authority(&original)?;
    let mut value = history(&original, &scope()?);
    value["resolved_observations"] =
        json!([{"receipt_ref":"fixture.host.observation-receipt.1","observation":original}]);
    assert!(
        authority
            .admit(&call(ProviderOperation::Replay, &value, &scope()?)?)
            .is_ok()
    );
    value["resolved_observations"][0]["receipt_ref"] = json!("forged.receipt");
    assert!(
        authority
            .admit(&call(ProviderOperation::Replay, &value, &scope()?)?)
            .is_err()
    );
    value["resolved_observations"][0]["receipt_ref"] = json!("fixture.host.observation-receipt.1");
    value["resolved_observations"][0]["observation"]["canonical_payload"]["content"] =
        json!("rewritten");
    assert!(
        authority
            .admit(&call(ProviderOperation::Replay, &value, &scope()?)?)
            .is_err()
    );
    Ok(())
}

fn limits() -> ProviderLimits {
    ProviderLimits {
        request_bytes: 1_000_000,
        response_bytes: 2_000_000,
        observation_batch_items: 100,
        recall_candidates: 20,
        concurrent_operations: 2,
        operation_millis: 30_000,
        snapshot_bytes: 4_000_000,
        inspection_items: 100,
    }
}
fn descriptor() -> Result<ProviderDescriptor, String> {
    ProviderDescriptor::new(
        OwnedProviderId::new("fixture-provider").map_err(err)?,
        "c".repeat(64),
        "real.schema.v2",
        7,
        [
            "provider.health.v1",
            "observation.accept.v1",
            "recall.query.v1",
        ]
        .into_iter()
        .map(OwnedVersionedId::new)
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?,
        limits(),
    )
    .map_err(err)
}
fn extension() -> Result<OwnedOpaqueExtension, String> {
    let value = json!({"opaque":"preserve bytes"});
    OwnedOpaqueExtension::new(
        OwnedVersionedId::new("fixture.opaque.v1").map_err(err)?,
        2,
        false,
        crate::canonical_json_sha256(&value).map_err(err)?,
        crate::canonical_json(&value).map_err(err)?,
    )
    .map_err(err)
}

#[test]
fn child_dtos_preserve_descriptor_handshake_and_observation_receipt() -> Result<(), String> {
    let descriptor = descriptor()?;
    assert_eq!(
        DescriptorDto::decode(&DescriptorDto::capture(&descriptor)?.encode()?)?.restore()?,
        descriptor
    );
    let request = HandshakeRequest::new(HandshakeRequestParts {
        provider_id: descriptor.provider_id.clone(),
        registration_revision: 3,
        exact_scope: scope()?,
        request_id: "original.handshake".into(),
        required_capabilities: descriptor.capabilities.iter().cloned().collect(),
        host_limits: limits(),
        control: OperationControl::new(i64::MAX, 30_000, CancellationToken::new()),
        challenge_nonce: [42; 32],
    })
    .map_err(err)?;
    let copy = HandshakeRequestDto::decode(&HandshakeRequestDto::capture(&request)?.encode()?)?
        .restore(CancellationToken::new(), Duration::from_millis(20))?;
    assert_eq!(copy.provider_id, request.provider_id);
    assert_eq!(copy.exact_scope, request.exact_scope);
    assert_eq!(copy.request_id, request.request_id);
    assert_eq!(copy.challenge_nonce, request.challenge_nonce);
    assert_eq!(copy.required_capabilities, request.required_capabilities);
    assert_eq!(copy.host_limits, request.host_limits);
    assert_eq!(
        copy.control.deadline_utc_micros(),
        request.control.deadline_utc_micros()
    );
    assert!(copy.control.remaining_millis() <= 29_980);
    let response = HandshakeResponse {
        terminal: TerminalRecord::new(
            ProviderOperation::Handshake,
            descriptor.provider_id.clone(),
            TerminalCode::Success,
            CommittedEffectEvidence::none(Some(7)),
            FallbackDirective::forbidden(),
            "original.handshake",
            scope()?.exact_scope_sha256(),
            None,
        )
        .map_err(err)?,
        descriptor: Some(descriptor),
        provider_instance_id: Some("process-actual-instance".into()),
        state_namespace: Some("actual.namespace".into()),
        accepted_scope: Some(scope()?),
        effective_limits: Some(limits()),
        ready_receipt_sha256: Some("d".repeat(64)),
        warnings: vec!["actual warning".into()],
    };
    assert_eq!(
        HandshakeResponseDto::decode(&HandshakeResponseDto::capture(&response)?.encode()?)?
            .restore()?,
        response
    );
    let original = call(ProviderOperation::Observe, &source()?, &scope()?)?;
    let restored = ProviderCallDto::decode(&ProviderCallDto::capture(&original)?.encode()?)?
        .restore(CancellationToken::new(), Duration::from_millis(10))?;
    assert_eq!(restored.payload, original.payload);
    assert_eq!(restored.sanitization(), original.sanitization());
    assert_eq!(restored.provider_id, original.provider_id);
    assert_eq!(restored.exact_scope, original.exact_scope);
    assert_eq!(restored.operation, original.operation);
    assert_eq!(restored.operation_id, original.operation_id);
    assert_eq!(restored.request_id, original.request_id);
    assert_eq!(restored.idempotency_key, original.idempotency_key);
    assert_eq!(
        restored.expected_state_generation,
        original.expected_state_generation
    );
    assert_eq!(
        restored.registration_revision,
        original.registration_revision
    );
    assert_eq!(restored.ready_receipt_sha256, original.ready_receipt_sha256);
    assert_eq!(
        restored.required_capabilities,
        original.required_capabilities
    );
    assert_eq!(
        restored.control.deadline_utc_micros(),
        original.control.deadline_utc_micros()
    );
    assert!(restored.control.remaining_millis() < original.control.remaining_millis());
    Ok(())
}

#[test]
fn child_dto_does_not_extend_expired_or_cancelled_control() -> Result<(), String> {
    let original = call(
        ProviderOperation::Recall,
        &json!({"query":"bytes"}),
        &scope()?,
    )?;
    let dto = ProviderCallDto::capture(&original)?;
    let exhausted = dto
        .clone()
        .restore(CancellationToken::new(), Duration::from_secs(31))?;
    assert_eq!(
        exhausted.control.snapshot(),
        Err(TerminalCode::DeadlineExceeded)
    );
    let bridge = CancellationToken::new();
    let live = dto.restore(bridge.clone(), Duration::ZERO)?;
    bridge.cancel();
    assert_eq!(live.control.snapshot(), Err(TerminalCode::Cancelled));
    original.control.cancellation().cancel();
    let cancelled =
        ProviderCallDto::capture(&original)?.restore(CancellationToken::new(), Duration::ZERO)?;
    assert_eq!(cancelled.control.snapshot(), Err(TerminalCode::Cancelled));
    Ok(())
}

#[test]
fn child_reply_preserves_complete_effect_and_fallback_evidence_and_corrupt_digest()
-> Result<(), String> {
    let provider = OwnedProviderId::new("fixture-provider").map_err(err)?;
    let effect = CommittedEffectEvidence::from_parts(CommittedEffectEvidenceParts {
        state: CommittedEffectState::Duplicate,
        committed_boundary: None,
        state_generation_before: Some(7),
        state_generation_after: Some(7),
        committed_item_refs: vec![],
        uncommitted_item_refs: vec![],
        provider_receipt_sha256: Some("e".repeat(64)),
        reconciliation_action: None,
        verification_sha256: None,
        duplicate_of_idempotency_key: Some("original-key".into()),
        duplicate_of_operation_id: Some("original-operation".into()),
    })
    .map_err(err)?;
    let mut reply = ProviderReply {
        terminal: TerminalRecord::new(
            ProviderOperation::Observe,
            provider.clone(),
            TerminalCode::Success,
            effect,
            FallbackDirective::forbidden(),
            "current-operation",
            scope()?.exact_scope_sha256(),
            Some("actual-diagnostic".into()),
        )
        .map_err(err)?,
        payload: Some(
            call(
                ProviderOperation::Recall,
                &json!({"kept":"exact bytes"}),
                &scope()?,
            )?
            .payload,
        ),
        warnings: vec!["original-warning".into()],
        extensions: vec![extension()?],
        state_generation: 7,
    };
    if let Some(payload) = &mut reply.payload {
        payload.sha256 = "0".repeat(64);
    }
    assert_eq!(
        ProviderReplyDto::decode(&ProviderReplyDto::capture(&reply)?.encode()?)?.restore()?,
        reply
    );
    let policy = PinnedFallbackPolicy::new(
        "host-policy",
        3,
        OwnedProviderId::new("other-provider").map_err(err)?,
    )
    .map_err(err)?;
    reply.terminal = TerminalRecord::new(
        ProviderOperation::Recall,
        provider.clone(),
        TerminalCode::ProviderUnavailable,
        CommittedEffectEvidence::none(Some(7)),
        FallbackDirective::explicit_policy_only(&provider, policy, "actual provider unavailable")
            .map_err(err)?,
        "actual-recall",
        scope()?.exact_scope_sha256(),
        Some("retained-diagnostic".into()),
    )
    .map_err(err)?;
    reply.payload = None;
    assert_eq!(ProviderReplyDto::capture(&reply)?.restore()?, reply);
    Ok(())
}

#[test]
fn foreign_isolation_requires_prior_content_and_rejects_outages_and_leaks() -> Result<(), String> {
    use crate::compatibility::{CompatibilityAssertion, assertion_errors};
    let scope = scope()?;
    let make_reply = |code, value: Option<Value>| -> Result<ProviderReply, String> {
        Ok(ProviderReply {
            terminal: TerminalRecord::new(
                ProviderOperation::Recall,
                OwnedProviderId::new("fixture-provider").map_err(err)?,
                code,
                CommittedEffectEvidence::none(Some(7)),
                FallbackDirective::forbidden(),
                "isolation-recall",
                scope.exact_scope_sha256(),
                (!matches!(
                    code,
                    TerminalCode::Success
                        | TerminalCode::SuccessZeroResults
                        | TerminalCode::Partial
                ))
                .then(|| "independent-transport-diagnostic".into()),
            )
            .map_err(err)?,
            payload: value
                .map(|value| {
                    call(ProviderOperation::Recall, &value, &scope).map(|call| call.payload)
                })
                .transpose()?,
            warnings: vec![],
            extensions: vec![],
            state_generation: 7,
        })
    };
    let populated = make_reply(
        TerminalCode::Success,
        Some(
            json!({"candidates":[{"content":"original secret fixture content","stable_memory_ref":"original-stable-ref",
        "provenance":{"source_refs":["original-source-reference"],"original_sources":[{"source":{"source_key":"original-source-key"}}]}}]}),
        ),
    )?;
    let prior = BTreeMap::from([("positive".into(), populated)]);
    let assertions = [CompatibilityAssertion::ForeignScopeIsolated {
        populated_step: "positive".into(),
    }];
    let refusal = make_reply(TerminalCode::ScopeMismatch, None)?;
    assert!(assertion_errors(&assertions, &refusal, &prior, &scope).is_empty());
    assert!(!assertion_errors(&assertions, &refusal, &BTreeMap::new(), &scope).is_empty());
    let empty = make_reply(
        TerminalCode::SuccessZeroResults,
        Some(json!({"candidates":[],"coverage":{"state":"zero_results"}})),
    )?;
    assert!(assertion_errors(&assertions, &empty, &prior, &scope).is_empty());
    let outage = make_reply(TerminalCode::ProviderUnavailable, None)?;
    assert!(!assertion_errors(&assertions, &outage, &prior, &scope).is_empty());
    for (diagnostic, leaks_source) in [
        ("original-source-key", true),
        ("original-stable-ref", true),
        ("independent-transport-diagnostic", false),
    ] {
        let mut diagnostic_reply = make_reply(TerminalCode::ScopeMismatch, None)?;
        diagnostic_reply.terminal = TerminalRecord::new(
            ProviderOperation::Recall,
            OwnedProviderId::new("fixture-provider").map_err(err)?,
            TerminalCode::ScopeMismatch,
            CommittedEffectEvidence::none(Some(7)),
            FallbackDirective::forbidden(),
            "isolation-recall",
            scope.exact_scope_sha256(),
            Some(diagnostic.into()),
        )
        .map_err(err)?;
        assert_eq!(
            !assertion_errors(&assertions, &diagnostic_reply, &prior, &scope).is_empty(),
            leaks_source,
        );
    }
    let mut leak = refusal;
    leak.warnings.push("original secret fixture content".into());
    assert!(!assertion_errors(&assertions, &leak, &prior, &scope).is_empty());
    let leaked_metadata = make_reply(
        TerminalCode::SuccessZeroResults,
        Some(
            json!({"candidates":[],"coverage":{"state":"zero_results"},"debug":{"source":"original-source-key"}}),
        ),
    )?;
    assert!(!assertion_errors(&assertions, &leaked_metadata, &prior, &scope).is_empty());
    Ok(())
}

#[test]
fn child_reply_preserves_partial_and_unknown_reconciliation_fields() -> Result<(), String> {
    for (code, effect) in [
        (
            TerminalCode::PartialEffect,
            CommittedEffectEvidence::partial(
                "actual-commit-boundary",
                7,
                8,
                vec!["committed-ref".into()],
                vec!["uncommitted-ref".into()],
                "e".repeat(64),
                "resume-original-operation",
                "f".repeat(64),
            )
            .map_err(err)?,
        ),
        (
            TerminalCode::EffectUnknown,
            CommittedEffectEvidence::unknown("e".repeat(64), "reconcile-original-delivery")
                .map_err(err)?,
        ),
    ] {
        let reply = ProviderReply {
            terminal: TerminalRecord::new(
                ProviderOperation::Observe,
                OwnedProviderId::new("fixture-provider").map_err(err)?,
                code,
                effect,
                FallbackDirective::forbidden(),
                "actual-original-operation",
                scope()?.exact_scope_sha256(),
                Some("actual-diagnostic".into()),
            )
            .map_err(err)?,
            payload: None,
            warnings: vec![],
            extensions: vec![],
            state_generation: 8,
        };
        assert_eq!(
            ProviderReplyDto::decode(&ProviderReplyDto::capture(&reply)?.encode()?)?.restore()?,
            reply
        );
    }
    Ok(())
}
