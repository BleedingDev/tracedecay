//! Discriminating controls for the single common advisory program; no concrete adapters.

use std::collections::BTreeMap;
use std::error::Error;
use std::io;
use std::sync::Arc;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tracedecay_memory_conformance::adversarial::{CommonProfileAdversary, CommonProfileMutation};
use tracedecay_memory_conformance::compatibility::{
    CommonAdvisoryFixture, CommonAdvisoryFixtureFactory, CompatibilityAssertion,
    CompatibilityScenario, CompatibilityStep, CompatibilityVerdict, ExpectedSource,
    FixtureEnvironmentAction, FixtureEnvironmentEvidence, FixtureUnavailable, T1, T2, T3, T4,
    assertion_errors, common_advisory_scenarios, common_observation, common_recall,
    run_compatibility_program, scope_json,
};
use tracedecay_memory_conformance::{
    AdversarialPayloadSourceV1, AdversarialProviderInputsV1, AdversarialProviderV1,
    AdversarialScriptV1, HandshakeMisbehaviourV1, MisbehaviourV1, RecallTemporalQuery,
};
use tracedecay_memory_provider_api::contract::{
    COMMON_ADVISORY_PROFILE_ID, COMMON_ADVISORY_REQUIRED_CAPABILITIES, TerminalCode,
};
use tracedecay_memory_provider_api::{
    CanonicalPayload, CommittedEffectEvidence, FallbackDirective, MemoryProvider, OwnedExactScope,
    OwnedProviderId, OwnedVersionedId, ProviderCall, ProviderDescriptor, ProviderLimits,
    ProviderOperation, ProviderReply, TerminalRecord,
};

fn scope() -> Result<OwnedExactScope, Box<dyn Error>> {
    Ok(OwnedExactScope::new(
        "conformance",
        "project",
        "repository",
        "worktree",
        "main",
        "source-session",
        format!("sha256:{}", "1".repeat(64)),
    )?)
}

fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn payload(value: &Value) -> Result<CanonicalPayload, Box<dyn Error>> {
    let bytes = serde_json::to_vec(value)?;
    Ok(CanonicalPayload::new(
        OwnedVersionedId::new("tracedecay.memory.provider.recall.v1")?,
        bytes.clone(),
        digest(&bytes),
    )?)
}

fn candidate(observation: &Value, scope: &OwnedExactScope, stable: &str) -> Value {
    let original = observation["source_identity"]["original_source"].clone();
    let mut candidate_scope = scope_json(scope);
    candidate_scope["scope_binding"] = json!("exact_coding_scope");
    let mut validity = original["validity"].clone();
    validity["source_revision"] = original["source"]["source_revision"].clone();
    validity["temporal_state"] = json!(if validity["valid_from"].is_null() {
        "unknown"
    } else {
        "current"
    });
    json!({ "candidate_id": format!("candidate.{stable}"), "stable_memory_ref": stable,
        "content": observation["canonical_payload"]["content"], "content_ref": null,
        "content_sha256": digest(observation["canonical_payload"]["content"].as_str().unwrap_or_default().as_bytes()), "native_score": 1,
        "confidence": null, "exact_scope_identity": candidate_scope, "validity": validity,
        "provenance": { "state": "available", "original_sources": [original],
            "observation_refs": [observation["observation_id"]] },
        "source_refs": [observation["source_identity"]["original_source"]["source"]["source_key"]],
        "trace_refs": [format!("trace.{stable}")], "warnings": [], "extensions": [] })
}

fn recall_value(candidates: Vec<Value>) -> Value {
    let count = candidates.len();
    json!({ "candidates": candidates, "coverage": { "state": "complete", "scanned_items": 2,
        "returned_items": count, "reasons": [] }, "warnings": [] })
}

fn reply(scope: &OwnedExactScope, value: Value) -> Result<ProviderReply, Box<dyn Error>> {
    Ok(ProviderReply {
        terminal: TerminalRecord::new(
            ProviderOperation::Recall,
            OwnedProviderId::new("test.common-profile")?,
            TerminalCode::Success,
            CommittedEffectEvidence::none(Some(0)),
            FallbackDirective::forbidden(),
            "recall.operation",
            scope.exact_scope_sha256(),
            None,
        )?,
        payload: Some(payload(&value)?),
        warnings: Vec::new(),
        extensions: Vec::new(),
        state_generation: 0,
    })
}

fn expected(observation: &Value) -> ExpectedSource {
    ExpectedSource {
        attribution: observation["source_identity"]["original_source"].clone(),
        content: observation["canonical_payload"]["content"]
            .as_str()
            .unwrap_or_default()
            .into(),
        effective_validity: None,
    }
}

#[test]
fn populated_original_source_assertion_rejects_empty_forged_scope_revision_and_digest()
-> Result<(), Box<dyn Error>> {
    let scope = scope()?;
    let source = common_observation(
        &scope,
        1,
        Some("opaque_revision_r2"),
        "compatibility beacon",
        Some(T1),
        None,
    );
    let good = recall_value(vec![candidate(&source, &scope, "stable.one")]);
    let assertions = vec![CompatibilityAssertion::RecallSources(vec![expected(
        &source,
    )])];
    assert!(
        assertion_errors(
            &assertions,
            &reply(&scope, good.clone())?,
            &BTreeMap::new(),
            &scope
        )
        .is_empty()
    );
    for pointer in [
        "/candidates/0/provenance/original_sources/0/source/canonical_session_id",
        "/candidates/0/exact_scope_identity/branch_identity",
        "/candidates/0/provenance/original_sources/0/source/source_revision",
        "/candidates/0/provenance/original_sources/0/source/content_sha256",
    ] {
        let mut bad = good.clone();
        *bad.pointer_mut(pointer)
            .ok_or_else(|| io::Error::other(pointer))? = json!("forged");
        assert!(
            !assertion_errors(&assertions, &reply(&scope, bad)?, &BTreeMap::new(), &scope)
                .is_empty(),
            "{pointer}"
        );
    }
    assert!(
        !assertion_errors(
            &assertions,
            &reply(&scope, recall_value(vec![]))?,
            &BTreeMap::new(),
            &scope
        )
        .is_empty()
    );
    let mut wrong_digest = reply(&scope, good)?;
    wrong_digest
        .payload
        .as_mut()
        .ok_or_else(|| io::Error::other("payload"))?
        .sha256 = "a".repeat(64);
    assert!(!assertion_errors(&assertions, &wrong_digest, &BTreeMap::new(), &scope).is_empty());
    Ok(())
}

#[test]
fn candidate_scope_requires_exact_binding_and_all_seven_identity_fields()
-> Result<(), Box<dyn Error>> {
    let scope = scope()?;
    let source = common_observation(
        &scope,
        1,
        Some("opaque_revision"),
        "compatibility beacon",
        Some(T1),
        None,
    );
    let good = recall_value(vec![candidate(&source, &scope, "stable.exact-scope")]);
    let assertions = [CompatibilityAssertion::RecallSources(vec![expected(
        &source,
    )])];
    assert!(
        assertion_errors(
            &assertions,
            &reply(&scope, good.clone())?,
            &BTreeMap::new(),
            &scope
        )
        .is_empty()
    );
    for field in [
        "scope_binding",
        "profile_id",
        "project_id",
        "repository_identity",
        "worktree_identity",
        "branch_identity",
        "agent_session_id",
        "resolved_scope_digest",
    ] {
        let mut bad = good.clone();
        bad["candidates"][0]["exact_scope_identity"][field] = json!(if field == "scope_binding" {
            "checkout_observations"
        } else {
            "different-identity"
        });
        assert!(
            !assertion_errors(&assertions, &reply(&scope, bad)?, &BTreeMap::new(), &scope)
                .is_empty(),
            "{field}"
        );
    }
    let mut missing_binding = good.clone();
    missing_binding["candidates"][0]["exact_scope_identity"]
        .as_object_mut()
        .ok_or_else(|| io::Error::other("candidate scope"))?
        .remove("scope_binding");
    assert!(
        !assertion_errors(
            &assertions,
            &reply(&scope, missing_binding)?,
            &BTreeMap::new(),
            &scope
        )
        .is_empty()
    );
    let mut extra_field = good;
    extra_field["candidates"][0]["exact_scope_identity"]["unexpected_scope_field"] =
        json!("not-authorized");
    assert!(
        !assertion_errors(
            &assertions,
            &reply(&scope, extra_field)?,
            &BTreeMap::new(),
            &scope
        )
        .is_empty()
    );
    Ok(())
}

#[test]
fn exclusion_requires_next_eligible_for_either_native_ranking_order() -> Result<(), Box<dyn Error>>
{
    let scope = scope()?;
    let a = common_observation(
        &scope,
        1,
        Some("r_a"),
        "compatibility beacon A",
        Some(T1),
        None,
    );
    let b = common_observation(
        &scope,
        2,
        Some("r_b"),
        "compatibility beacon B",
        Some(T1),
        None,
    );
    let a = candidate(&a, &scope, "stable.a");
    let b = candidate(&b, &scope, "stable.b");
    let assertions = vec![CompatibilityAssertion::NextEligible {
        ranked_step: "ranked".into(),
    }];
    for candidates in [vec![a.clone(), b.clone()], vec![b, a]] {
        let previous = BTreeMap::from([(
            "ranked".into(),
            reply(&scope, recall_value(candidates.clone()))?,
        )]);
        assert!(
            assertion_errors(
                &assertions,
                &reply(&scope, recall_value(vec![candidates[1].clone()]))?,
                &previous,
                &scope
            )
            .is_empty()
        );
        for bad in [vec![], vec![candidates[0].clone()], candidates.clone()] {
            assert!(
                !assertion_errors(
                    &assertions,
                    &reply(&scope, recall_value(bad))?,
                    &previous,
                    &scope
                )
                .is_empty()
            );
        }
    }
    Ok(())
}

#[test]
fn unknown_revision_and_unknown_validity_require_distinct_retained_evidence()
-> Result<(), Box<dyn Error>> {
    let scope = scope()?;
    let source = common_observation(&scope, 1, None, "compatibility beacon", Some(T1), None);
    let mut value = recall_value(vec![candidate(&source, &scope, "stable.unknown-revision")]);
    value["coverage"]["state"] = json!("partial");
    value["coverage"]["reasons"] = json!(["source_revision_unknown"]);
    value["warnings"] = json!(["source revision not retained"]);
    let assertions = vec![
        CompatibilityAssertion::RecallSources(vec![expected(&source)]),
        CompatibilityAssertion::DegradedCoverage,
    ];
    assert!(
        assertion_errors(
            &assertions,
            &reply(&scope, value.clone())?,
            &BTreeMap::new(),
            &scope
        )
        .is_empty()
    );
    let mut lost_validity = value.clone();
    lost_validity["candidates"][0]["provenance"]["original_sources"][0]["validity"]["valid_from"] =
        Value::Null;
    assert!(
        !assertion_errors(
            &assertions,
            &reply(&scope, lost_validity)?,
            &BTreeMap::new(),
            &scope
        )
        .is_empty()
    );
    value["coverage"]["state"] = json!("complete");
    assert!(
        !assertion_errors(
            &assertions,
            &reply(&scope, value)?,
            &BTreeMap::new(),
            &scope
        )
        .is_empty()
    );
    Ok(())
}

#[test]
fn privacy_absence_requires_populated_prerequisite_and_complete_search()
-> Result<(), Box<dyn Error>> {
    let scope = scope()?;
    let source = common_observation(
        &scope,
        1,
        Some("r1"),
        "compatibility beacon",
        Some(T1),
        None,
    );
    let assertions = vec![CompatibilityAssertion::RecallAbsent {
        populated_step: "before".into(),
    }];
    let absent = reply(&scope, recall_value(vec![]))?;
    assert!(!assertion_errors(&assertions, &absent, &BTreeMap::new(), &scope).is_empty());
    let before = reply(
        &scope,
        recall_value(vec![candidate(&source, &scope, "stable")]),
    )?;
    let previous = BTreeMap::from([("before".into(), before.clone())]);
    assert!(assertion_errors(&assertions, &absent, &previous, &scope).is_empty());
    assert!(!assertion_errors(&assertions, &before, &previous, &scope).is_empty());
    Ok(())
}

#[test]
fn temporal_corpus_accepts_all_modes_and_preserves_nanosecond_boundaries()
-> Result<(), Box<dyn Error>> {
    for mode in ["current", "as_of", "interval", "history"] {
        let input = common_recall(mode, T2, (mode == "interval").then_some(T3));
        let query: RecallTemporalQuery = serde_json::from_value(input["temporal_query"].clone())?;
        assert!(query.valid_shape(), "{mode}");
    }
    let mut input = common_recall("interval", T2, Some("2025-01-01T00:00:02.000000001Z"));
    let query: RecallTemporalQuery = serde_json::from_value(input["temporal_query"].clone())?;
    assert!(query.valid_shape());
    input["temporal_query"]["interval_end"] = json!(T2);
    assert!(
        !serde_json::from_value::<RecallTemporalQuery>(input["temporal_query"].clone())?
            .valid_shape()
    );
    input["temporal_query"]["mode"] = json!("unknown_future_mode");
    assert!(
        !serde_json::from_value::<RecallTemporalQuery>(input["temporal_query"].clone())?
            .valid_shape()
    );
    let legacy: RecallTemporalQuery =
        serde_json::from_value(json!({ "mode": "current", "evaluation_time": T4 }))?;
    assert!(legacy.valid_shape());
    assert_eq!(legacy.unknown_validity_policy, "exclude");
    Ok(())
}

#[test]
fn common_program_contains_every_required_operation_and_all_exclusion_classes()
-> Result<(), Box<dyn Error>> {
    let scenarios = common_advisory_scenarios(&scope()?, 1).map_err(io::Error::other)?;
    let operations: std::collections::BTreeSet<_> = scenarios
        .iter()
        .flat_map(|scenario| &scenario.steps)
        .filter_map(|step| match step {
            CompatibilityStep::Call { fixture, .. } => Some(fixture.operation.capability_id()),
            _ => None,
        })
        .collect();
    for capability in COMMON_ADVISORY_REQUIRED_CAPABILITIES {
        if *capability != "recall.temporal.v1" {
            assert!(operations.contains(capability), "{capability}");
        }
    }
    let exclusion_case = scenarios
        .iter()
        .find(|case| case.case_id == "common.exclusions.all_classes")
        .ok_or_else(|| io::Error::other("exclusions case"))?;
    for name in [
        "stable_memory_refs",
        "candidate_ids",
        "source_refs",
        "trace_refs",
        "observation_ids",
        "content_sha256",
    ] {
        assert!(
            exclusion_case
                .steps
                .iter()
                .any(|step| step.step_id() == format!("exclusions.{name}"))
        );
    }
    assert!(
        scenarios
            .iter()
            .flat_map(|scenario| &scenario.steps)
            .any(|step| step.step_id().contains("end_minus_one_ns"))
    );
    let observation = common_observation(
        &scope()?,
        1,
        Some("opaque_r2"),
        "compatibility beacon",
        Some(T1),
        None,
    );
    assert_eq!(
        observation["observation_kind"],
        "session.message_committed.v1"
    );
    assert_eq!(
        observation["payload_contract"],
        "tracedecay.memory.observation.session-message.v1"
    );
    assert_eq!(
        observation["source_identity"]["original_source"]["source"]["source_revision"],
        "opaque_r2"
    );
    assert!(
        observation["source_identity"]["original_source"]["source"]
            .get("stable_record_id")
            .is_some()
    );
    Ok(())
}

struct FixedPayload(Value);
impl AdversarialPayloadSourceV1 for FixedPayload {
    fn payload_for(&self, call: &ProviderCall) -> Result<Option<CanonicalPayload>, String> {
        let mut payload = payload(&self.0).map_err(|error| error.to_string())?;
        payload.contract_id = call.payload.contract_id.clone();
        Ok(Some(payload))
    }
}

fn controlled_provider(value: Value) -> Result<Arc<dyn MemoryProvider>, Box<dyn Error>> {
    let capabilities = std::iter::once(COMMON_ADVISORY_PROFILE_ID)
        .chain(COMMON_ADVISORY_REQUIRED_CAPABILITIES.iter().copied())
        .map(OwnedVersionedId::new)
        .collect::<Result<Vec<_>, _>>()?;
    let descriptor = ProviderDescriptor::new(
        OwnedProviderId::new("test.common-profile")?,
        "a".repeat(64),
        "controlled.v1",
        0,
        capabilities,
        ProviderLimits {
            request_bytes: 1_048_576,
            response_bytes: 1_048_576,
            observation_batch_items: 64,
            recall_candidates: 64,
            concurrent_operations: 1,
            operation_millis: 30_000,
            snapshot_bytes: 1_048_576,
            inspection_items: 64,
        },
    )?;
    Ok(Arc::new(AdversarialProviderV1::new(
        AdversarialProviderInputsV1 {
            descriptor,
            provider_instance_id: "controlled-test-instance".into(),
            state_namespace: "controlled-test-namespace".into(),
            ready_receipt_sha256: "c".repeat(64),
            handshake_script: AdversarialScriptV1::always(HandshakeMisbehaviourV1::Compliant),
            invoke_script: AdversarialScriptV1::always(MisbehaviourV1::Compliant),
            payloads: Arc::new(FixedPayload(value)),
        },
    )))
}

struct ControlledFixture(Arc<dyn MemoryProvider>);
impl CommonAdvisoryFixture for ControlledFixture {
    fn provider(&self) -> &dyn MemoryProvider {
        self.0.as_ref()
    }
    fn apply_environment(
        &mut self,
        _: &FixtureEnvironmentAction,
    ) -> Result<FixtureEnvironmentEvidence, FixtureUnavailable> {
        Err(FixtureUnavailable {
            reason: "controlled provider has no physical restart capability".into(),
        })
    }
}
struct ControlledFactory(Arc<dyn MemoryProvider>);
impl CommonAdvisoryFixtureFactory for ControlledFactory {
    fn create(
        &self,
        _: &CompatibilityScenario,
        _: &OwnedExactScope,
    ) -> Result<Box<dyn CommonAdvisoryFixture>, FixtureUnavailable> {
        Ok(Box::new(ControlledFixture(Arc::clone(&self.0))))
    }
}

#[test]
fn canonical_dispatch_rejects_wrong_contract_and_bad_digest_even_without_semantic_assertions()
-> Result<(), Box<dyn Error>> {
    let scope = scope()?;
    let first = common_advisory_scenarios(&scope, 1).map_err(io::Error::other)?[0].steps[0].clone();
    let program = CompatibilityScenario {
        case_id: "control.canonical_observe_envelope".into(),
        steps: vec![first],
    };
    let control = run_compatibility_program(
        &ControlledFactory(controlled_provider(json!({}))?),
        &scope,
        1,
        &[program.clone()],
    );
    assert_eq!(control.denominators.passed, 1, "{:?}", control.results);
    for mutation in [
        CommonProfileMutation::WrongPayloadContract,
        CommonProfileMutation::CorruptPayloadDigest,
    ] {
        let adversary = Arc::new(CommonProfileAdversary::new(
            controlled_provider(json!({}))?,
            mutation,
        ));
        let provider: Arc<dyn MemoryProvider> = adversary.clone();
        let rejected =
            run_compatibility_program(&ControlledFactory(provider), &scope, 1, &[program.clone()]);
        assert_eq!(rejected.denominators.failed, 1, "{:?}", rejected.results);
        assert_eq!(adversary.exhibited(), 1);
    }
    Ok(())
}

#[test]
fn unchanged_positive_recall_rejects_forged_effective_validity_and_temporal_claims()
-> Result<(), Box<dyn Error>> {
    let scope = scope()?;
    let cases = common_advisory_scenarios(&scope, 1).map_err(io::Error::other)?;
    let case = cases
        .iter()
        .find(|case| case.case_id == "common.source_authority_session_scope_isolation")
        .ok_or_else(|| io::Error::other("authority case"))?;
    let observation = case
        .steps
        .iter()
        .find_map(|step| match step {
            CompatibilityStep::Call { fixture, .. }
                if fixture.step_id == "authority.observe_original" =>
            {
                serde_json::from_slice::<Value>(&fixture.payload.bytes).ok()
            }
            _ => None,
        })
        .ok_or_else(|| io::Error::other("original observation"))?;
    let positive = case
        .steps
        .iter()
        .find(|step| step.step_id() == "authority.original_positive")
        .ok_or_else(|| io::Error::other("positive recall"))?
        .clone();
    let program = CompatibilityScenario {
        case_id: "control.effective_validity".into(),
        steps: vec![positive],
    };
    let value = recall_value(vec![candidate(
        &observation,
        &scope,
        "stable.temporal-control",
    )]);
    let control = run_compatibility_program(
        &ControlledFactory(controlled_provider(value.clone())?),
        &scope,
        1,
        &[program.clone()],
    );
    assert_eq!(control.denominators.passed, 1, "{:?}", control.results);
    for mutation in [
        CommonProfileMutation::ForgeCandidateValidityTimestamp,
        CommonProfileMutation::ForgeCandidateTemporalState,
    ] {
        let adversary = Arc::new(CommonProfileAdversary::new(
            controlled_provider(value.clone())?,
            mutation,
        ));
        let provider: Arc<dyn MemoryProvider> = adversary.clone();
        let rejected =
            run_compatibility_program(&ControlledFactory(provider), &scope, 1, &[program.clone()]);
        assert_eq!(rejected.denominators.failed, 1, "{:?}", rejected.results);
        assert_eq!(adversary.exhibited(), 1);
    }
    Ok(())
}

#[test]
fn unsupported_and_successful_noop_required_mutation_cannot_pass() -> Result<(), Box<dyn Error>> {
    let scope = scope()?;
    let scenarios = common_advisory_scenarios(&scope, 1).map_err(io::Error::other)?;
    let first = scenarios[0].steps[0].clone();
    for mutation in [
        CommonProfileMutation::UnsupportedRequired(ProviderOperation::Observe),
        CommonProfileMutation::SuccessfulNoOp(ProviderOperation::Observe),
    ] {
        let adversary = Arc::new(CommonProfileAdversary::new(
            controlled_provider(json!({}))?,
            mutation,
        ));
        let provider: Arc<dyn MemoryProvider> = adversary.clone();
        let scenario = CompatibilityScenario {
            case_id: "control.required_mutation".into(),
            steps: vec![first.clone()],
        };
        let report =
            run_compatibility_program(&ControlledFactory(provider), &scope, 1, &[scenario]);
        assert!(!report.compatible());
        assert_eq!(report.denominators.failed, 1, "{:?}", report.results);
        assert_eq!(adversary.exhibited(), 1);
    }
    Ok(())
}

#[test]
fn populated_budget_and_structured_controls_reject_extra_candidates_and_metadata()
-> Result<(), Box<dyn Error>> {
    let scope = scope()?;
    let cases = common_advisory_scenarios(&scope, 1).map_err(io::Error::other)?;
    let budget_case = cases
        .iter()
        .find(|case| case.case_id == "common.all_recall_budgets_full_content_digest")
        .ok_or_else(|| io::Error::other("budget case"))?;
    let mut sources = Vec::new();
    let mut budgets = Value::Null;
    let mut recall_requests = Vec::new();
    for step in &budget_case.steps {
        if let CompatibilityStep::Call { fixture, .. } = step {
            let value: Value = serde_json::from_slice(&fixture.payload.bytes)?;
            if fixture.operation == ProviderOperation::Observe {
                sources.push(value);
            } else if fixture.operation == ProviderOperation::Recall {
                if fixture.step_id == "budgets.bounded" {
                    budgets = value["budgets"].clone();
                }
                recall_requests.push(value);
            }
        }
    }
    assert_eq!(sources.len(), 2);
    assert_eq!(recall_requests.len(), 2);
    let query = recall_requests[0]["query"]
        .as_str()
        .ok_or_else(|| io::Error::other("budget query"))?;
    assert!(query.contains("café"));
    for request in &recall_requests {
        assert_eq!(request["query"], json!(query));
        assert_eq!(request["objective"], json!(query));
    }
    let quota = budgets["maximum_candidate_content_bytes"]
        .as_u64()
        .ok_or_else(|| io::Error::other("candidate byte quota"))?;
    for source in &sources {
        let content = source["canonical_payload"]["content"]
            .as_str()
            .ok_or_else(|| io::Error::other("budget source content"))?;
        assert!(content.len() as u64 > quota);
        assert!(content.matches(query).count() > 1);
    }
    assert_ne!(
        sources[0]["canonical_payload"]["content"],
        sources[1]["canonical_payload"]["content"]
    );
    let mut candidates: Vec<Value> = sources
        .iter()
        .enumerate()
        .map(|(index, source)| candidate(source, &scope, &format!("budget.{index}")))
        .collect();
    for candidate in &mut candidates {
        candidate["content"] = json!("compatibility beacon bounded");
        candidate["content_sha256"] = json!(digest(b"compatibility beacon bounded"));
    }
    let assertion = vec![CompatibilityAssertion::BoundedRecall { budgets }];
    assert!(
        assertion_errors(
            &assertion,
            &reply(&scope, recall_value(vec![candidates[0].clone()]))?,
            &BTreeMap::new(),
            &scope
        )
        .is_empty()
    );
    assert!(
        !assertion_errors(
            &assertion,
            &reply(&scope, recall_value(candidates.clone()))?,
            &BTreeMap::new(),
            &scope
        )
        .is_empty()
    );
    let exclude = vec![CompatibilityAssertion::ExcludesText(
        "opaque fixture metadata".into(),
    )];
    let clean = recall_value(vec![candidates[0].clone()]);
    assert!(
        assertion_errors(
            &exclude,
            &reply(&scope, clean.clone())?,
            &BTreeMap::new(),
            &scope
        )
        .is_empty()
    );
    for pointer in ["/candidates/0/content", "/candidates/0/provenance"] {
        let mut leaked = clean.clone();
        *leaked
            .pointer_mut(pointer)
            .ok_or_else(|| io::Error::other("leak target"))? =
            json!("expected projection plus opaque fixture metadata");
        assert!(
            !assertion_errors(&exclude, &reply(&scope, leaked)?, &BTreeMap::new(), &scope)
                .is_empty()
        );
    }
    Ok(())
}

#[test]
fn missing_physical_action_stays_unknown_and_blocks_dependent_assertions()
-> Result<(), Box<dyn Error>> {
    let scope = scope()?;
    let scenario = CompatibilityScenario {
        case_id: "control.unavailable_environment".into(),
        steps: vec![
            CompatibilityStep::Environment {
                step_id: "restart".into(),
                action: FixtureEnvironmentAction::Restart,
            },
            CompatibilityStep::Environment {
                step_id: "next".into(),
                action: FixtureEnvironmentAction::Shutdown,
            },
        ],
    };
    let report = run_compatibility_program(
        &ControlledFactory(controlled_provider(json!({}))?),
        &scope,
        1,
        &[scenario],
    );
    assert!(!report.compatible());
    assert_eq!(report.denominators.planned, 2);
    assert_eq!(report.denominators.unknown, 2);
    assert_eq!(report.denominators.passed, 0);
    assert!(
        report
            .results
            .iter()
            .all(|row| row.verdict == CompatibilityVerdict::Unknown)
    );
    assert_eq!(report.timings.len(), 2);
    Ok(())
}

struct SearchPayload(Vec<Value>);
impl AdversarialPayloadSourceV1 for SearchPayload {
    fn payload_for(&self, call: &ProviderCall) -> Result<Option<CanonicalPayload>, String> {
        let request: Value =
            serde_json::from_slice(&call.payload.bytes).map_err(|error| error.to_string())?;
        let mode = request
            .pointer("/temporal_query/mode")
            .and_then(Value::as_str)
            .unwrap_or("current");
        let at = request
            .pointer(if mode == "as_of" {
                "/temporal_query/as_of"
            } else {
                "/temporal_query/evaluation_time"
            })
            .and_then(Value::as_str)
            .unwrap_or(T4);
        let maximum = request
            .pointer("/budgets/maximum_candidates")
            .and_then(Value::as_u64)
            .unwrap_or(16) as usize;
        let candidates = self
            .0
            .iter()
            .filter(|candidate| {
                let temporal = candidate
                    .pointer("/validity/valid_from")
                    .and_then(Value::as_str)
                    .is_none_or(|start| start <= at)
                    && candidate
                        .pointer("/validity/valid_until")
                        .and_then(Value::as_str)
                        .is_none_or(|end| at < end);
                temporal
                    && !request
                        .pointer("/exclusions/stable_memory_refs")
                        .and_then(Value::as_array)
                        .is_some_and(|excluded| excluded.contains(&candidate["stable_memory_ref"]))
            })
            .take(maximum)
            .cloned()
            .collect();
        payload(&recall_value(candidates))
            .map(Some)
            .map_err(|error| error.to_string())
    }
}

fn search_provider(candidates: Vec<Value>) -> Result<Arc<dyn MemoryProvider>, Box<dyn Error>> {
    let descriptor = controlled_provider(json!({}))?.descriptor();
    Ok(Arc::new(AdversarialProviderV1::new(
        AdversarialProviderInputsV1 {
            descriptor,
            provider_instance_id: "controlled-search".into(),
            state_namespace: "controlled-search-namespace".into(),
            ready_receipt_sha256: "c".repeat(64),
            handshake_script: AdversarialScriptV1::always(HandshakeMisbehaviourV1::Compliant),
            invoke_script: AdversarialScriptV1::always(MisbehaviourV1::Compliant),
            payloads: Arc::new(SearchPayload(candidates)),
        },
    )))
}

#[test]
fn unchanged_program_rejects_exclusions_dropped_before_search() -> Result<(), Box<dyn Error>> {
    let scope = scope()?;
    let all = common_advisory_scenarios(&scope, 1).map_err(io::Error::other)?;
    let case = all
        .iter()
        .find(|scenario| scenario.case_id == "common.exclusions.all_classes")
        .ok_or_else(|| io::Error::other("case"))?;
    let observations: Vec<Value> = case
        .steps
        .iter()
        .take(2)
        .filter_map(|step| match step {
            CompatibilityStep::Call { fixture, .. } => {
                serde_json::from_slice(&fixture.payload.bytes).ok()
            }
            _ => None,
        })
        .collect();
    let candidates = observations
        .iter()
        .enumerate()
        .map(|(index, source)| candidate(source, &scope, &format!("stable.{index}")))
        .collect::<Vec<_>>();
    let steps: Vec<_> = case
        .steps
        .iter()
        .filter(|step| {
            matches!(
                step.step_id(),
                "exclusions.ranked" | "exclusions.stable_memory_refs"
            )
        })
        .cloned()
        .collect();
    let program = CompatibilityScenario {
        case_id: "controlled.exclusion-regression".into(),
        steps,
    };
    let control = run_compatibility_program(
        &ControlledFactory(search_provider(candidates.clone())?),
        &scope,
        1,
        &[program.clone()],
    );
    assert_eq!(control.denominators.passed, 2, "{:?}", control.results);
    let adversary = Arc::new(CommonProfileAdversary::new(
        search_provider(candidates)?,
        CommonProfileMutation::DropExclusions,
    ));
    let provider: Arc<dyn MemoryProvider> = adversary.clone();
    let mutated = run_compatibility_program(&ControlledFactory(provider), &scope, 1, &[program]);
    assert_eq!(mutated.denominators.failed, 1, "{:?}", mutated.results);
    assert_eq!(adversary.exhibited(), 1);
    Ok(())
}

#[test]
fn unchanged_as_of_assertion_rejects_current_substitution() -> Result<(), Box<dyn Error>> {
    let scope = scope()?;
    let all = common_advisory_scenarios(&scope, 1).map_err(io::Error::other)?;
    let case = all
        .iter()
        .find(|scenario| scenario.case_id == "common.temporal.all_modes_end_boundaries")
        .ok_or_else(|| io::Error::other("case"))?;
    let first = match &case.steps[0] {
        CompatibilityStep::Call { fixture, .. } => {
            serde_json::from_slice::<Value>(&fixture.payload.bytes)?
        }
        _ => return Err(io::Error::other("observe").into()),
    };
    let current = common_observation(
        &scope,
        99,
        Some("current_revision"),
        "compatibility beacon current",
        Some(T2),
        None,
    );
    let candidates = vec![
        candidate(&first, &scope, "old"),
        candidate(&current, &scope, "current"),
    ];
    let selected = case
        .steps
        .iter()
        .find(|step| step.step_id() == "temporal.as_of.start")
        .ok_or_else(|| io::Error::other("as-of"))?
        .clone();
    let program = CompatibilityScenario {
        case_id: "controlled.temporal-regression".into(),
        steps: vec![selected],
    };
    let control = run_compatibility_program(
        &ControlledFactory(search_provider(candidates.clone())?),
        &scope,
        1,
        &[program.clone()],
    );
    assert_eq!(control.denominators.passed, 1, "{:?}", control.results);
    let adversary = Arc::new(CommonProfileAdversary::new(
        search_provider(candidates)?,
        CommonProfileMutation::DropTemporalQuery,
    ));
    let provider: Arc<dyn MemoryProvider> = adversary.clone();
    let mutated = run_compatibility_program(&ControlledFactory(provider), &scope, 1, &[program]);
    assert_eq!(mutated.denominators.failed, 1, "{:?}", mutated.results);
    assert_eq!(adversary.exhibited(), 1);
    Ok(())
}

#[test]
fn successful_replay_counters_cannot_hide_effect_unknown_or_false_delivery_duplicate()
-> Result<(), Box<dyn Error>> {
    let scope = scope()?;
    let value = json!({ "applied_observations": 0, "duplicate_observations": 0,
        "sources_already_applied": 1, "rejected_observations": 0, "effect_unknown_observations": 0 });
    let assertion = vec![CompatibilityAssertion::ReplayAccounting {
        applied: 0,
        sources_already_applied: 1,
        rejected: 0,
    }];
    assert!(
        assertion_errors(
            &assertion,
            &reply(&scope, value.clone())?,
            &BTreeMap::new(),
            &scope
        )
        .is_empty()
    );
    for field in [
        "duplicate_observations",
        "effect_unknown_observations",
        "applied_observations",
    ] {
        let mut invalid = value.clone();
        invalid[field] = json!(1);
        assert!(
            !assertion_errors(
                &assertion,
                &reply(&scope, invalid)?,
                &BTreeMap::new(),
                &scope
            )
            .is_empty()
        );
    }
    Ok(())
}
