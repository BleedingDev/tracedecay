//! Populated common advisory tests through the real Native adapter/application port.
use super::*;
use tracedecay_memory_conformance::compatibility::common_observation;

const T1: &str = "2025-01-01T00:00:00.000000001Z";
const T2: &str = "2025-01-02T00:00:00.000000002Z";
const T3: &str = "2025-01-03T00:00:00.000000003Z";
const T4: &str = "2025-01-04T00:00:00.000000004Z";

fn common_call(
    provider: &NativeProvider,
    scope: &OwnedExactScope,
    operation: ProviderOperation,
    label: &str,
    mut value: Value,
) -> ProviderCall {
    let contract = match operation {
        ProviderOperation::Observe => OBSERVATION_CONTRACT_ID,
        ProviderOperation::Recall => RECALL_CONTRACT_ID,
        ProviderOperation::Health => "tracedecay.memory.provider.health.v1",
        ProviderOperation::Feedback => "tracedecay.memory.provider.feedback.v1",
        ProviderOperation::Correction => "tracedecay.memory.provider.correction.v1",
        ProviderOperation::Inspection => "tracedecay.memory.provider.inspection.v1",
        ProviderOperation::Maintenance => "tracedecay.memory.provider.maintenance.v1",
        ProviderOperation::DeleteBySource => "tracedecay.memory.provider.deletion-by-source.v1",
        ProviderOperation::SnapshotExport => "tracedecay.memory.provider.snapshot-export.v1",
        ProviderOperation::SnapshotRestore => "tracedecay.memory.provider.snapshot-restore.v1",
        ProviderOperation::Replay => "tracedecay.memory.provider.replay.v1",
        ProviderOperation::Handshake => unreachable!(),
    };
    let generation = provider.descriptor().state_generation;
    let key = sha256_hex(label.as_bytes());
    let request_id = format!("request.{label}");
    let operation_id = format!("operation.{label}");
    let context = json!({"provider_id":NATIVE_PROVIDER_ID,"registration_revision":1,"ready_receipt_digest":"a".repeat(64),
        "exact_scope_identity":scope_value(scope),"request_identity":request_id,"policy_revision":1,
        "deadline":{"deadline_utc_micros":i64::MAX,"remaining_millis":5000},"cancellation":"live","extensions":[]});
    if matches!(
        operation,
        ProviderOperation::Observe | ProviderOperation::Recall
    ) {
        value
            .as_object_mut()
            .unwrap()
            .extend(context.as_object().unwrap().clone());
        if operation == ProviderOperation::Observe {
            value["idempotency_key"] = key.clone().into();
            value.as_object_mut().unwrap().remove("policy_revision");
        }
    } else {
        value["common_request"] = context;
        value["common_request"]["operation_id"] = operation_id.clone().into();
        value["common_request"]["idempotency_key"] = key.clone().into();
        value["common_request"]["expected_state_generation"] = generation.into();
    }
    let bytes = serde_json::to_vec(&value).unwrap();
    admitted(
        ProviderCall::new(ProviderCallParts {
            operation,
            provider_id: OwnedProviderId::new(NATIVE_PROVIDER_ID).unwrap(),
            registration_revision: 1,
            ready_receipt_sha256: "a".repeat(64),
            exact_scope: scope.clone(),
            request_id,
            operation_id,
            expected_state_generation: generation,
            idempotency_key: operation.mutates_provider_state().then_some(key),
            control: OperationControl::new(i64::MAX, 10000, CancellationToken::new()),
            payload: CanonicalPayload::new(
                OwnedVersionedId::new(contract).unwrap(),
                bytes.clone(),
                sha256_hex(&bytes),
            )
            .unwrap(),
            required_capabilities: vec![OwnedVersionedId::new(operation.capability_id()).unwrap()],
            extensions: Vec::new(),
        })
        .unwrap(),
    )
}

fn scope_value(scope: &OwnedExactScope) -> Value {
    json!({"profile_id":scope.profile_id,"project_id":scope.project_id,"repository_identity":scope.repository_identity,"worktree_identity":scope.worktree_identity,
        "branch_identity":scope.branch_identity,"agent_session_id":scope.agent_session_id,"resolved_scope_digest":scope.resolved_scope_digest})
}

fn common_query(mode: &str, at: &str) -> Value {
    let mut request = recall_request_value("placeholder");
    request["required_capabilities"] = json!([
        "recall.query.v1",
        "recall.temporal.v1",
        "memory.advisory_common.v1"
    ]);
    request["query"] = "compatibility beacon".into();
    request["temporal_query"]["mode"] = mode.into();
    request["temporal_query"]["evaluation_time"] = T4.into();
    if mode == "current" {
        request["temporal_query"]["evaluation_time"] = at.into();
    }
    if mode == "as_of" {
        request["temporal_query"]["as_of"] = at.into();
    }
    if mode == "interval" {
        request["temporal_query"]["interval_start"] = at.into();
        request["temporal_query"]["interval_end"] = T4.into();
    }
    request
}

fn body(reply: &ProviderReply) -> Value {
    assert!(
        matches!(
            reply.terminal.terminal_code(),
            TerminalCode::Success | TerminalCode::SuccessZeroResults | TerminalCode::Partial
        ),
        "unexpected terminal: {:?}",
        reply.terminal
    );
    serde_json::from_slice(&reply.payload.as_ref().unwrap().bytes).unwrap()
}

fn recall_common(
    provider: &NativeProvider,
    scope: &OwnedExactScope,
    label: &str,
    query: Value,
) -> Value {
    body(&provider.invoke(&common_call(
        provider,
        scope,
        ProviderOperation::Recall,
        label,
        query,
    )))
}

fn lifecycle_target(scope: &OwnedExactScope, observation: &Value, reference: &str) -> Value {
    let source = &observation["source_identity"]["original_source"];
    json!({"provider_id":NATIVE_PROVIDER_ID,"registration_revision":1,"original_scope":source["origin_scope"],"delivery_scope":scope_value(scope),
        "source":source["source"],"reference":{"kind":"stable_memory_ref","reference":reference}})
}

fn new_port(
    graph: &Arc<TraceDecay>,
    project_root: &Path,
) -> Arc<ProjectNativeMemoryApplicationPort> {
    Arc::new(
        ProjectNativeMemoryApplicationPort::new(
            Arc::new(tokio::sync::RwLock::new(Arc::clone(graph))),
            project_root.to_path_buf(),
            test_profile_id(),
            &test_provider_state_root(project_root),
        )
        .unwrap(),
    )
}

// Declared fixture authority: its source inventory is fixed by test arrangement,
// independent of the caller's payload/grant/checkpoint JSON.
struct FixtureAuthority {
    scope: OwnedExactScope,
    sources: Vec<tracedecay_memory_provider_registry::SourceAttribution>,
    state: tracedecay_memory_provider_registry::SourceDisposition,
}

impl tracedecay_memory_provider_registry::AdvisoryAdmissionAuthority for FixtureAuthority {
    fn admit(
        &self,
        call: &ProviderCall,
    ) -> Result<
        tracedecay_memory_provider_registry::CurrentAdvisoryAdmission,
        tracedecay_memory_provider_registry::AdvisoryAdmissionError,
    > {
        use tracedecay_memory_provider_registry::{
            CurrentAdvisoryAdmission, CurrentRestoreAdmission, CurrentSourceDisposition,
            GrantedHistorySource, RestoreDispositionCheckpoint,
        };
        if call.exact_scope != self.scope {
            return Err(
                tracedecay_memory_provider_registry::AdvisoryAdmissionError::Denied(
                    "fixture scope",
                ),
            );
        }
        let current = CurrentSourceDisposition {
            state: self.state,
            authority_ref: "fixture.live.current-source".into(),
            authority_revision: Some(9),
            checked_at_utc_nanos: parse_rfc3339_nanos(T4).unwrap() + 9,
        };
        if call.operation == ProviderOperation::SnapshotRestore {
            let checkpoint = RestoreDispositionCheckpoint {
                exact_scope: self.scope.clone(),
                authority_ref: "fixture.live.checkpoint".into(),
                authority_revision: Some(9),
                checked_at_utc_nanos: parse_rfc3339_nanos(T4).unwrap() + 9,
            };
            CurrentAdvisoryAdmission::new(
                call,
                self.sources
                    .iter()
                    .map(|source| GrantedHistorySource {
                        attribution: source.clone(),
                        current_disposition: current.clone(),
                    })
                    .collect(),
                Some(CurrentRestoreAdmission::new(
                    checkpoint,
                    self.sources
                        .iter()
                        .map(|source| (source.source.clone(), current.clone()))
                        .collect(),
                )?),
            )
        } else {
            CurrentAdvisoryAdmission::new(
                call,
                self.sources
                    .iter()
                    .map(|source| GrantedHistorySource {
                        attribution: source.clone(),
                        current_disposition: current.clone(),
                    })
                    .collect(),
                None,
            )
        }
    }
}

fn fixture_source(observation: &Value) -> tracedecay_memory_provider_registry::SourceAttribution {
    use tracedecay_memory_provider_registry::{
        OriginScopeEvidence, OriginalSourceIdentity, SourceAttribution,
    };
    let attribution = &observation["source_identity"]["original_source"];
    let source = &attribution["source"];
    SourceAttribution {
        source: OriginalSourceIdentity {
            canonical_provider_id: OwnedProviderId::new(
                source["canonical_provider_id"].as_str().unwrap(),
            )
            .unwrap(),
            canonical_session_id: source["canonical_session_id"].as_str().unwrap().into(),
            source_key: source["source_key"].as_str().unwrap().into(),
            stable_record_id: source["stable_record_id"].as_str().map(str::to_owned),
            observation_id: source["observation_id"].as_str().unwrap().into(),
            source_revision: source["source_revision"].as_str().map(str::to_owned),
            content_sha256: source["content_sha256"].as_str().unwrap().into(),
        },
        origin_scope: OriginScopeEvidence::Recorded {
            scope: super::super::super::native_staged_observations::exact_scope_from_value(
                &attribution["origin_scope"]["exact_scope_identity"],
            )
            .unwrap(),
            authority_ref: attribution["origin_scope"]["authority_ref"]
                .as_str()
                .unwrap()
                .into(),
        },
        source_sequence: attribution["source_sequence"].as_u64().unwrap(),
        occurred_at_utc_nanos: attribution["occurred_at"]
            .as_str()
            .and_then(parse_rfc3339_nanos),
        ingested_at_utc_nanos: parse_rfc3339_nanos(attribution["ingested_at"].as_str().unwrap())
            .unwrap(),
        validity: super::super::super::native_staged_observations::recorded_validity(Some(
            attribution,
        ))
        .unwrap(),
    }
}

fn new_authorized_port(
    graph: &Arc<TraceDecay>,
    root: &Path,
    scope: &OwnedExactScope,
    sources: &[Value],
    state: tracedecay_memory_provider_registry::SourceDisposition,
) -> Arc<ProjectNativeMemoryApplicationPort> {
    Arc::new(
        ProjectNativeMemoryApplicationPort::new(
            Arc::new(tokio::sync::RwLock::new(Arc::clone(graph))),
            root.to_path_buf(),
            test_profile_id(),
            &test_provider_state_root(root),
        )
        .unwrap()
        .with_admission_authority(Arc::new(FixtureAuthority {
            scope: scope.clone(),
            sources: sources.iter().map(fixture_source).collect(),
            state,
        })),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn common_advisory_temporal_exclusions_and_canonical_authority() {
    let (_temp, root, graph, owner, project) = real_project_fixture().await;
    let fact = add_real_project_fact(
        &graph,
        "compatibility beacon canonical authority",
        "common-authority",
    )
    .await;
    let before = read_store_snapshot(&graph, &owner, &fact.fact_id).await;
    let port = new_port(&graph, &root);
    let provider = NativeProvider::new(port.clone()).unwrap();
    let scope = recall_exact_scope(project.as_str());
    let first = common_observation(
        &scope,
        1,
        Some("cache_policy_r1"),
        "compatibility beacon original",
        Some(T1),
        Some(T3),
    );
    let second = common_observation(
        &scope,
        2,
        Some("cache_policy_r2"),
        "compatibility beacon later",
        Some(T3),
        None,
    );
    for (label, value) in [("original", first.clone()), ("later", second)] {
        let reply = provider.invoke(&common_call(
            &provider,
            &scope,
            ProviderOperation::Observe,
            label,
            value,
        ));
        assert_eq!(reply.terminal.terminal_code(), TerminalCode::Success);
    }
    assert_eq!(provider.descriptor().state_generation, 2);
    let past = recall_common(&provider, &scope, "past", common_query("as_of", T2));
    assert_eq!(past["candidates"].as_array().unwrap().len(), 1);
    assert_eq!(
        past["candidates"][0]["content"],
        "compatibility beacon original"
    );
    assert_eq!(past["candidates"][0]["validity"]["valid_from"], T1);
    assert_eq!(past["candidates"][0]["validity"]["valid_until"], T3);
    assert_eq!(
        past["candidates"][0]["provenance"]["original_sources"],
        json!([first["source_identity"]["original_source"].clone()])
    );
    let current = recall_common(&provider, &scope, "current", common_query("current", T4));
    assert_eq!(current["candidates"].as_array().unwrap().len(), 1);
    assert_eq!(
        current["candidates"][0]["content"],
        "compatibility beacon later"
    );
    let interval = recall_common(&provider, &scope, "interval", common_query("interval", T2));
    assert_eq!(interval["candidates"].as_array().unwrap().len(), 2);
    let history = recall_common(&provider, &scope, "history", common_query("history", T4));
    assert_eq!(history["candidates"].as_array().unwrap().len(), 2);
    for (class,value) in [("stable_memory_refs",history["candidates"][0]["stable_memory_ref"].clone()),("content_sha256",history["candidates"][0]["content_sha256"].clone()),
        ("observation_ids",history["candidates"][0]["provenance"]["original_sources"][0]["source"]["observation_id"].clone()),
        ("source_refs",history["candidates"][0]["source_refs"][0].clone()),
        ("trace_refs",history["candidates"][0]["trace_refs"][0].clone()),
        ("candidate_ids",json!(format!("request.exclude-candidate_ids:{}",history["candidates"][0]["stable_memory_ref"].as_str().unwrap())))
    ]{
        let mut query=common_query("history",T4);query["budgets"]["maximum_candidates"]=1.into();query["exclusions"][class]=json!([value]);
        let excluded=recall_common(&provider,&scope,&format!("exclude-{class}"),query);
        assert_eq!(excluded["candidates"].as_array().unwrap().len(),1);assert_ne!(excluded["candidates"][0]["stable_memory_ref"],history["candidates"][0]["stable_memory_ref"]);
    }
    assert_eq!(
        read_store_snapshot(&graph, &owner, &fact.fact_id).await,
        before
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn common_advisory_source_influence_selector_matches_one_recorded_revision() {
    let (_temp, root, graph, _owner, project) = real_project_fixture().await;
    let scope = recall_exact_scope(project.as_str());
    let provider = NativeProvider::new(new_port(&graph, &root)).unwrap();
    let first = common_observation(
        &scope,
        1,
        Some("revision.alpha"),
        "compatibility beacon original revision",
        Some(T1),
        Some(T3),
    );
    let source_key = first["source_identity"]["original_source"]["source"]["source_key"].clone();
    let mut second = common_observation(
        &scope,
        2,
        Some("revision.beta"),
        "compatibility beacon later revision",
        Some(T3),
        None,
    );
    second["source_identity"]["original_source"]["source"]["source_key"] = source_key.clone();
    for (label, observation) in [("selector-alpha", first), ("selector-beta", second)] {
        body(&provider.invoke(&common_call(
            &provider,
            &scope,
            ProviderOperation::Observe,
            label,
            observation,
        )));
    }
    let aggregate = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Inspection,
        "selector-all-revisions",
        inspection("source_influence", json!({"source_key":source_key})),
    )));
    let items = aggregate["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["source"]["source_revision"], "revision.alpha");
    assert_eq!(items[1]["source"]["source_revision"], "revision.beta");
    assert_ne!(
        items[0]["target"]["reference"],
        items[1]["target"]["reference"]
    );
    for (index, item) in items.iter().enumerate() {
        let exact = body(&provider.invoke(&common_call(
            &provider,
            &scope,
            ProviderOperation::Inspection,
            &format!("selector-exact-{index}"),
            inspection(
                "source_influence",
                json!({"source_key":source_key,"stable_memory_ref":item["target"]["reference"]["reference"]}),
            ),
        )));
        assert_eq!(exact["items"], json!([item]));
    }
    let mismatched = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Inspection,
        "selector-mismatched-source",
        inspection(
            "source_influence",
            json!({"source_key":"source.unrelated","stable_memory_ref":items[0]["target"]["reference"]["reference"]}),
        ),
    )));
    assert!(mismatched["items"].as_array().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn common_advisory_feedback_correction_restart_and_effect_receipts() {
    let (_temp, root, graph, _owner, project) = real_project_fixture().await;
    let scope = recall_exact_scope(project.as_str());
    let port = new_port(&graph, &root);
    let provider = NativeProvider::new(port.clone()).unwrap();
    let original = common_observation(
        &scope,
        1,
        Some("revision.alpha"),
        "compatibility beacon original",
        Some(T1),
        None,
    );
    let observation = common_call(
        &provider,
        &scope,
        ProviderOperation::Observe,
        "feedback-original",
        original.clone(),
    );
    let committed = provider.invoke(&observation);
    body(&committed);
    let duplicate = provider.invoke(&observation);
    assert_eq!(
        duplicate.terminal.committed_effect().state(),
        CommittedEffectState::Duplicate
    );
    assert_eq!(
        duplicate
            .terminal
            .committed_effect()
            .provider_receipt_sha256(),
        committed
            .terminal
            .committed_effect()
            .provider_receipt_sha256()
    );
    assert_eq!(provider.descriptor().state_generation, 1);
    let recalled = recall_common(
        &provider,
        &scope,
        "feedback-recall",
        common_query("current", T4),
    );
    let reference = recalled["candidates"][0]["stable_memory_ref"]
        .as_str()
        .unwrap();
    let target = lifecycle_target(&scope, &original, reference);
    let harmful = json!({"target":target,"signal":"harmful","weight":"1","canonical_outcome_receipt":"host.settled.harmful","evidence_refs":[],"occurred_at":T2});
    let feedback = common_call(
        &provider,
        &scope,
        ProviderOperation::Feedback,
        "harmful",
        harmful.clone(),
    );
    let first = provider.invoke(&feedback);
    assert_eq!(
        body(&first)["applied_effect"]["feedback_suppressed_after"],
        true
    );
    let duplicate = provider.invoke(&feedback);
    assert_eq!(
        duplicate.terminal.committed_effect().state(),
        CommittedEffectState::Duplicate
    );
    assert_eq!(provider.descriptor().state_generation, 2);
    assert!(
        recall_common(&provider, &scope, "suppressed", common_query("current", T4))["candidates"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let inspection = json!({"view":"source_influence","selector":{"source_key":original["source_identity"]["original_source"]["source"]["source_key"]},"maximum_items":64,"maximum_bytes":65536,"redaction_policy_revision":1,"cursor":null});
    let inspected = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Inspection,
        "inspect-feedback",
        inspection,
    )));
    assert_eq!(inspected["items"][0]["settled_feedback"]["harmful"], 1);
    assert_eq!(inspected["items"][0]["active"], false);
    let mut helpful = harmful;
    helpful["signal"] = "helpful".into();
    body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Feedback,
        "helpful",
        helpful,
    )));
    assert_eq!(
        recall_common(
            &provider,
            &scope,
            "helpful-recall",
            common_query("current", T4)
        )["candidates"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let mut replacement = common_observation(
        &scope,
        2,
        Some("revision.beta"),
        "compatibility beacon replacement",
        Some(T2),
        None,
    );
    replacement["source_identity"]["original_source"]["source"]["source_key"] =
        original["source_identity"]["original_source"]["source"]["source_key"].clone();
    let correction = json!({"target":target,"expected_target_revision":"revision.alpha","correction_kind":"replace_content","replacement":replacement,"reason":"settled correction","evidence_refs":[]});
    let corrected = provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Correction,
        "correct",
        correction,
    ));
    body(&corrected);
    let after = recall_common(
        &provider,
        &scope,
        "after-correct",
        common_query("current", T4),
    );
    assert_eq!(
        after["candidates"][0]["content"],
        "compatibility beacon replacement"
    );
    let before = recall_common(
        &provider,
        &scope,
        "before-correct",
        common_query("as_of", T1),
    );
    assert_eq!(
        before["candidates"][0]["content"],
        "compatibility beacon original"
    );
    assert_eq!(before["candidates"][0]["validity"]["valid_until"], T2);
    assert_eq!(
        before["candidates"][0]["provenance"]["original_sources"][0],
        original["source_identity"]["original_source"]
    );
    let generation = provider.descriptor().state_generation;
    drop(provider);
    drop(port);
    let reopened = new_port(&graph, &root);
    let provider = NativeProvider::new(reopened).unwrap();
    assert_eq!(provider.descriptor().state_generation, generation);
    assert_eq!(
        recall_common(&provider, &scope, "reopened", common_query("current", T4))["candidates"][0]
            ["content"],
        "compatibility beacon replacement"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn common_advisory_delete_before_observe_and_old_snapshot_restore() {
    let (_temp, root, graph, _owner, project) = real_project_fixture().await;
    let scope = recall_exact_scope(project.as_str());
    let original = common_observation(
        &scope,
        1,
        Some("r1"),
        "compatibility beacon deleted secret",
        Some(T1),
        None,
    );
    let port = new_authorized_port(
        &graph,
        &root,
        &scope,
        std::slice::from_ref(&original),
        tracedecay_memory_provider_registry::SourceDisposition::Available,
    );
    let provider = NativeProvider::new(port.clone()).unwrap();
    body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Observe,
        "snapshot-original",
        original.clone(),
    )));
    let exported = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::SnapshotExport,
        "export",
        json!({}),
    )));
    assert!(!exported["snapshot"]["bytes"].as_array().unwrap().is_empty());
    let source = original["source_identity"]["original_source"]["source"].clone();
    let deletion = json!({"forget_source_keys":[source["source_key"],"future.source"],"mode":"hard_delete","include_snapshots":true,"retention_lock_policy_revision":1,"verification_query":source["source_key"]});
    let deleted = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::DeleteBySource,
        "delete",
        deletion,
    )));
    assert_eq!(deleted["postcondition"]["removed_effects"], 1);
    assert_eq!(deleted["postcondition"]["remaining_influence_count"], 0);
    let later = provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Observe,
        "fresh-key",
        original.clone(),
    ));
    assert_eq!(later.terminal.terminal_code(), TerminalCode::Conflict);
    let mut future = common_observation(&scope, 2, Some("new"), "future forbidden", Some(T1), None);
    future["source_identity"]["original_source"]["source"]["source_key"] = "future.source".into();
    assert_eq!(
        provider
            .invoke(&common_call(
                &provider,
                &scope,
                ProviderOperation::Observe,
                "future",
                future
            ))
            .terminal
            .terminal_code(),
        TerminalCode::Conflict
    );
    let restore = json!({"snapshot":exported["snapshot"],"disposition_checkpoint":{"exact_scope":scope_value(&scope),"authority_ref":"host.current.checkpoint","authority_revision":1,"checked_at":T4},
        "source_dispositions":[{"source":source,"current_disposition":{"state":"available","authority_ref":"host.current.source","authority_revision":1,"checked_at":T4}}]});
    let generation = provider.descriptor().state_generation;
    body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::SnapshotRestore,
        "restore",
        restore,
    )));
    assert!(provider.descriptor().state_generation > generation);
    for mode in ["current", "as_of", "interval", "history"] {
        let mut query = common_query(mode, T1);
        query["temporal_query"]["include_superseded"] = true.into();
        query["temporal_query"]["include_revoked"] = true.into();
        assert!(
            recall_common(&provider, &scope, &format!("deleted-{mode}"), query)["candidates"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }
    drop(provider);
    drop(port);
    let provider = NativeProvider::new(new_port(&graph, &root)).unwrap();
    assert!(
        recall_common(
            &provider,
            &scope,
            "deleted-reopen",
            common_query("current", T4)
        )["candidates"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn common_advisory_unknown_validity_structured_evidence_and_commit_fault() {
    let (_temp, root, graph, _owner, project) = real_project_fixture().await;
    let scope = recall_exact_scope(project.as_str());
    let port = new_port(&graph, &root);
    let provider = NativeProvider::new(port.clone()).unwrap();
    let unknown = common_observation(
        &scope,
        1,
        None,
        "compatibility beacon unknown validity",
        None,
        None,
    );
    let call = common_call(
        &provider,
        &scope,
        ProviderOperation::Observe,
        "fault",
        unknown.clone(),
    );
    port.staged_store().fail_next_commit();
    assert_eq!(
        provider.invoke(&call).terminal.terminal_code(),
        TerminalCode::ProviderUnavailable
    );
    assert_eq!(provider.descriptor().state_generation, 0);
    body(&provider.invoke(&call));
    let excluded = recall_common(
        &provider,
        &scope,
        "unknown-excluded",
        common_query("current", T4),
    );
    assert!(excluded["candidates"].as_array().unwrap().is_empty());
    assert_eq!(excluded["coverage"]["state"], "partial");
    let mut query = common_query("current", T4);
    query["temporal_query"]["unknown_validity_policy"] = "allow_with_warning".into();
    let admitted = recall_common(&provider, &scope, "unknown-admitted", query);
    assert_eq!(
        admitted["candidates"][0]["validity"]["temporal_state"],
        "unknown"
    );
    assert!(admitted["candidates"][0]["validity"]["source_revision"].is_null());
    let mut structured = common_observation(&scope, 2, Some("test.v2"), "fixture", Some(T1), None);
    structured["observation_kind"] = "test.execution_settled.v1".into();
    structured["payload_contract"] = "tracedecay.memory.observation.test-execution.v1".into();
    structured["canonical_payload"] = json!({"assertion":"compatibility beacon tests passed","status":"passed","secret_unknown":"must never become raw JSON"});
    let digest =
        tracedecay_memory_conformance::canonical_json_sha256(&structured["canonical_payload"])
            .unwrap();
    structured["payload_sha256"] = digest.clone().into();
    structured["source_identity"]["original_source"]["source"]["content_sha256"] = digest.into();
    body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Observe,
        "structured",
        structured,
    )));
    let recalled = recall_common(
        &provider,
        &scope,
        "structured-recall",
        common_query("current", T4),
    );
    assert_eq!(
        recalled["candidates"][0]["content"],
        "assertion: compatibility beacon tests passed\nstatus: passed"
    );
    assert!(!recalled.to_string().contains("must never become raw JSON"));
}

fn inspection(view: &str, selector: Value) -> Value {
    json!({"view":view,"selector":selector,"maximum_items":64,"maximum_bytes":32768,
        "redaction_policy_revision":1,"cursor":null})
}

fn fixture_history(scope: &OwnedExactScope, observations: &[Value]) -> Value {
    json!({"authorization_ref":"fixture.host.admitted-history","policy_revision":1,"destination_scope":scope_value(scope),"relation":"exact_scope",
        "sources":observations.iter().map(|observation|json!({"attribution":observation["source_identity"]["original_source"],
            "current_disposition":{"state":"available","authority_ref":"fixture.frozen.disposition","authority_revision":1,"checked_at":T1}})).collect::<Vec<_>>(),
        "disposition_checkpoint":{"exact_scope":scope_value(scope),"authority_ref":"fixture.frozen.checkpoint","authority_revision":1,"checked_at":T1}})
}

fn fixture_replay(scope: &OwnedExactScope, observations: &[Value], previous: u64) -> Value {
    let resolved:Vec<Value> = observations.iter().map(|observation|json!({"receipt_ref":format!("fixture.receipt.{}",observation["source_sequence"]),"observation":observation})).collect();
    json!({"observation_batch_refs":resolved.iter().map(|item|item["receipt_ref"].clone()).collect::<Vec<_>>(),
        "resolved_observations":resolved,"first_source_sequence":observations.first().unwrap()["source_sequence"],
        "last_source_sequence":observations.last().unwrap()["source_sequence"],"expected_previous_acknowledged_sequence":previous,
        "history_grant":fixture_history(scope,observations)})
}

// These original sources are declared by test arrangement, before any store
// operation. Revisions retain the same canonical provider/session/source key.
fn deletion_source_fixture(
    scope: &OwnedExactScope,
    sequence: u64,
    canonical_provider: &str,
    canonical_session: &str,
    source_key: &str,
) -> Value {
    let mut observation = common_observation(
        scope,
        sequence,
        Some(&format!("revision.{sequence}")),
        &format!("compatibility beacon {canonical_provider} {canonical_session} {sequence}"),
        Some(T1),
        None,
    );
    observation["canonical_payload"]["session_id"] = canonical_session.into();
    let digest =
        tracedecay_memory_conformance::canonical_json_sha256(&observation["canonical_payload"])
            .unwrap();
    observation["payload_sha256"] = digest.clone().into();
    let source = &mut observation["source_identity"]["original_source"]["source"];
    source["canonical_provider_id"] = canonical_provider.into();
    source["canonical_session_id"] = canonical_session.into();
    source["source_key"] = source_key.into();
    source["content_sha256"] = digest.into();
    observation["provenance"]["source_refs"] = json!([source_key]);
    observation
}

fn fixture_private_history(
    scope: &OwnedExactScope,
    observations: &[Value],
) -> tracedecay_memory_provider_registry::HistoryGrant {
    use tracedecay_memory_provider_registry::{
        CurrentSourceDisposition, GrantedHistorySource, HistoryGrant, HistoryRelation,
        RestoreDispositionCheckpoint, SourceDisposition,
    };
    // A caller claim only. Each test installs a separately arranged live
    // FixtureAuthority; this helper never constructs an admission result.
    HistoryGrant {
        authorization_ref: "fixture.claimed.source-delete".into(),
        policy_revision: 1,
        destination_scope: scope.clone(),
        relation: HistoryRelation::ExactScope,
        sources: observations
            .iter()
            .map(|observation| GrantedHistorySource {
                attribution: fixture_source(observation),
                current_disposition: CurrentSourceDisposition {
                    state: SourceDisposition::Available,
                    authority_ref: "fixture.frozen.claim-disposition".into(),
                    authority_revision: Some(1),
                    checked_at_utc_nanos: parse_rfc3339_nanos(T1).unwrap(),
                },
            })
            .collect(),
        disposition_checkpoint: RestoreDispositionCheckpoint {
            exact_scope: scope.clone(),
            authority_ref: "fixture.frozen.claim-checkpoint".into(),
            authority_revision: Some(1),
            checked_at_utc_nanos: parse_rfc3339_nanos(T1).unwrap(),
        },
    }
}

fn source_deletion_request(source_keys: &[&str]) -> Value {
    json!({"forget_source_keys":source_keys,"mode":"hard_delete","include_snapshots":true,
        "retention_lock_policy_revision":1,"verification_query":"compatibility beacon"})
}

fn exported_snapshot_body(snapshot: &Value) -> Value {
    let bytes: Vec<u8> = serde_json::from_value(snapshot["bytes"].clone()).unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn fixture_restore_request(scope: &OwnedExactScope, snapshot: &Value, sources: &[Value]) -> Value {
    json!({"snapshot":snapshot,
        "disposition_checkpoint":{"exact_scope":scope_value(scope),"authority_ref":"fixture.frozen.restore-checkpoint","authority_revision":1,"checked_at":T1},
        "source_dispositions":sources.iter().map(|observation|json!({
            "source":observation["source_identity"]["original_source"]["source"],
            "current_disposition":{"state":"available","authority_ref":"fixture.frozen.restore-source","authority_revision":1,"checked_at":T1}
        })).collect::<Vec<_>>()})
}

// Per-source dispositions are fixed by test arrangement. This authority never
// derives its source inventory or current state from a restore request/snapshot.
struct FixtureRestoreDispositionAuthority {
    scope: OwnedExactScope,
    sources: Vec<(
        tracedecay_memory_provider_registry::SourceAttribution,
        tracedecay_memory_provider_registry::SourceDisposition,
    )>,
}

impl tracedecay_memory_provider_registry::AdvisoryAdmissionAuthority
    for FixtureRestoreDispositionAuthority
{
    fn admit(
        &self,
        call: &ProviderCall,
    ) -> Result<
        tracedecay_memory_provider_registry::CurrentAdvisoryAdmission,
        tracedecay_memory_provider_registry::AdvisoryAdmissionError,
    > {
        use tracedecay_memory_provider_registry::{
            AdvisoryAdmissionError, CurrentAdvisoryAdmission, CurrentRestoreAdmission,
            CurrentSourceDisposition, GrantedHistorySource, RestoreDispositionCheckpoint,
        };
        if call.exact_scope != self.scope || call.operation != ProviderOperation::SnapshotRestore {
            return Err(AdvisoryAdmissionError::Denied(
                "fixture restore scope/operation",
            ));
        }
        let history: Vec<_> = self
            .sources
            .iter()
            .map(|(source, state)| GrantedHistorySource {
                attribution: source.clone(),
                current_disposition: CurrentSourceDisposition {
                    state: *state,
                    authority_ref: "fixture.live.per-source-disposition".into(),
                    authority_revision: Some(9),
                    checked_at_utc_nanos: parse_rfc3339_nanos(T4).unwrap() + 9,
                },
            })
            .collect();
        let restore = CurrentRestoreAdmission::new(
            RestoreDispositionCheckpoint {
                exact_scope: self.scope.clone(),
                authority_ref: "fixture.live.per-source-checkpoint".into(),
                authority_revision: Some(9),
                checked_at_utc_nanos: parse_rfc3339_nanos(T4).unwrap() + 9,
            },
            history
                .iter()
                .map(|source| {
                    (
                        source.attribution.source.clone(),
                        source.current_disposition.clone(),
                    )
                })
                .collect(),
        )?;
        CurrentAdvisoryAdmission::new(call, history, Some(restore))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn common_advisory_claimed_source_delete_preserves_other_origin_and_fences_reopen() {
    // Each canonical identity component independently protects the survivor.
    for (b_provider, b_session) in [
        ("claude", "session.canonical.b"),
        ("codex", "session.canonical.b"),
        ("claude", "session.canonical.a"),
    ] {
        assert_claimed_source_identity_survives_reopen(b_provider, b_session).await;
    }
}

async fn assert_claimed_source_identity_survives_reopen(b_provider: &str, b_session: &str) {
    use tracedecay_memory_provider_registry::SourceDisposition;
    let (_temp, root, graph, _owner, project) = real_project_fixture().await;
    let scope = recall_exact_scope(project.as_str());
    let key = "common-advisory/source/shared";
    let source_a = deletion_source_fixture(&scope, 1, "codex", "session.canonical.a", key);
    let source_b = deletion_source_fixture(&scope, 2, b_provider, b_session, key);
    let later_a = deletion_source_fixture(&scope, 3, "codex", "session.canonical.a", key);
    let later_b = deletion_source_fixture(&scope, 4, b_provider, b_session, key);
    let port = new_authorized_port(
        &graph,
        &root,
        &scope,
        std::slice::from_ref(&source_a),
        SourceDisposition::Available,
    );
    let provider = NativeProvider::new(port.clone()).unwrap();
    for (label, observation) in [("origin-a", &source_a), ("origin-b", &source_b)] {
        body(&provider.invoke(&common_call(
            &provider,
            &scope,
            ProviderOperation::Observe,
            label,
            observation.clone(),
        )));
    }
    let before = recall_common(
        &provider,
        &scope,
        "origins-before",
        common_query("current", T4),
    );
    assert_eq!(before["candidates"].as_array().unwrap().len(), 2);
    let b_ref = before["candidates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|candidate| {
            candidate["provenance"]["original_sources"][0]
                == source_b["source_identity"]["original_source"]
        })
        .unwrap()["stable_memory_ref"]
        .as_str()
        .unwrap()
        .to_owned();
    let feedback = json!({"target":lifecycle_target(&scope,&source_b,&b_ref),
    "signal":"helpful","weight":"0.5","occurred_at":T2,
    "canonical_outcome_receipt":"fixture.settled.b-before-delete"});
    let settled = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Feedback,
        "b-feedback-before",
        feedback.clone(),
    )));
    let deletion = common_call(
        &provider,
        &scope,
        ProviderOperation::DeleteBySource,
        "delete-origin-a",
        source_deletion_request(&[key]),
    )
    .with_history_grant(fixture_private_history(
        &scope,
        std::slice::from_ref(&source_a),
    ));
    let deleted = body(&provider.invoke(&deletion));
    assert_eq!(deleted["postcondition"]["removed_effects"], 1);
    assert_eq!(deleted["postcondition"]["remaining_influence_count"], 0);
    let generation = provider.descriptor().state_generation;
    drop(provider);
    drop(port);

    let provider = NativeProvider::new(new_authorized_port(
        &graph,
        &root,
        &scope,
        std::slice::from_ref(&source_a),
        SourceDisposition::Available,
    ))
    .unwrap();
    assert_eq!(provider.descriptor().state_generation, generation);
    for mode in ["current", "as_of", "interval", "history"] {
        let mut query = common_query(mode, T1);
        query["temporal_query"]["include_revoked"] = true.into();
        query["temporal_query"]["include_superseded"] = true.into();
        let recalled = recall_common(&provider, &scope, &format!("b-survives-{mode}"), query);
        assert_eq!(recalled["candidates"].as_array().unwrap().len(), 1);
        assert_eq!(recalled["candidates"][0]["stable_memory_ref"], b_ref);
        assert_eq!(
            recalled["candidates"][0]["provenance"]["original_sources"][0],
            source_b["source_identity"]["original_source"]
        );
    }
    let influence = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Inspection,
        "b-influence-after-delete",
        inspection("source_influence", json!({"source_key":key})),
    )));
    assert_eq!(influence["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        influence["items"][0]["source"],
        source_b["source_identity"]["original_source"]["source"]
    );
    assert_eq!(influence["items"][0]["settled_feedback"]["helpful"], 1);
    assert_eq!(
        influence["items"][0]["last_feedback_receipt"],
        settled["provider_receipt_digest"]
    );
    assert_eq!(
        provider
            .invoke(&common_call(
                &provider,
                &scope,
                ProviderOperation::Feedback,
                "b-feedback-before",
                feedback.clone(),
            ))
            .terminal
            .committed_effect()
            .state(),
        CommittedEffectState::Duplicate
    );
    body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Feedback,
        "b-feedback-after",
        feedback,
    )));
    assert_eq!(
        provider
            .invoke(&common_call(
                &provider,
                &scope,
                ProviderOperation::Observe,
                "a-later-refused",
                later_a,
            ))
            .terminal
            .terminal_code(),
        TerminalCode::Conflict
    );
    let replay_call = common_call(
        &provider,
        &scope,
        ProviderOperation::Observe,
        "a-replayed-fresh-delivery",
        source_a,
    );
    let envelope: Value = serde_json::from_slice(&replay_call.payload.bytes).unwrap();
    let replayed = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Replay,
        "a-replay-after-reopen",
        fixture_replay(&scope, &[envelope], 0),
    )));
    assert_eq!(replayed["applied_observations"], 0);
    assert_eq!(replayed["rejected_observations"], 1);
    body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Observe,
        "b-later-survives",
        later_b.clone(),
    )));
    let after = recall_common(
        &provider,
        &scope,
        "b-revisions",
        common_query("history", T4),
    );
    assert_eq!(after["candidates"].as_array().unwrap().len(), 2);
    assert!(
        after["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .all(|candidate| {
                let source = &candidate["provenance"]["original_sources"][0]["source"];
                source["canonical_provider_id"] == b_provider
                    && source["canonical_session_id"] == b_session
            })
    );
    assert!(
        after["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .any(|candidate| {
                candidate["provenance"]["original_sources"][0]
                    == later_b["source_identity"]["original_source"]
            })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn common_advisory_typed_source_fence_restore_import_and_schema_downgrade() {
    use tracedecay_memory_provider_registry::SourceDisposition;
    let (_temp, root, graph, _owner, project) = real_project_fixture().await;
    let scope = recall_exact_scope(project.as_str());
    let key = "common-advisory/source/snapshot-shared";
    let source_a = deletion_source_fixture(&scope, 1, "codex", "session.canonical.a", key);
    let source_b = deletion_source_fixture(&scope, 2, "claude", "session.canonical.b", key);
    let inventory = vec![source_a.clone(), source_b.clone()];
    let provider = NativeProvider::new(new_authorized_port(
        &graph,
        &root,
        &scope,
        std::slice::from_ref(&source_a),
        SourceDisposition::Available,
    ))
    .unwrap();
    for (label, observation) in [("snapshot-a", &source_a), ("snapshot-b", &source_b)] {
        body(&provider.invoke(&common_call(
            &provider,
            &scope,
            ProviderOperation::Observe,
            label,
            observation.clone(),
        )));
    }
    let predelete = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::SnapshotExport,
        "typed-predelete",
        json!({}),
    )))["snapshot"]
        .clone();
    assert_eq!(
        exported_snapshot_body(&predelete)["schema"],
        "native-staged-v2"
    );
    let deletion = common_call(
        &provider,
        &scope,
        ProviderOperation::DeleteBySource,
        "typed-snapshot-delete",
        source_deletion_request(&[key]),
    )
    .with_history_grant(fixture_private_history(
        &scope,
        std::slice::from_ref(&source_a),
    ));
    body(&provider.invoke(&deletion));
    let postdelete = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::SnapshotExport,
        "typed-postdelete",
        json!({}),
    )))["snapshot"]
        .clone();
    let post_body = exported_snapshot_body(&postdelete);
    assert_eq!(
        postdelete["identity"]["state_schema_version"],
        predelete["identity"]["state_schema_version"]
    );
    assert_eq!(
        postdelete["identity"]["state_schema_version"],
        "native-staged-v2"
    );
    assert_eq!(
        post_body["schema"],
        "native-staged-snapshot-source-fences-v1"
    );
    assert_eq!(post_body["deletion_fences"].as_array().unwrap().len(), 1);
    assert_eq!(
        post_body["deletion_fences"][0]["kind"],
        "canonical_source_v1"
    );
    assert_ne!(post_body["deletion_fences"][0]["source_key"], key);
    drop(provider);

    // Even a fresh Available disposition cannot resurrect A over the local fence.
    let provider = NativeProvider::new(new_authorized_port(
        &graph,
        &root,
        &scope,
        &inventory,
        SourceDisposition::Available,
    ))
    .unwrap();
    body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::SnapshotRestore,
        "typed-predelete-restore",
        fixture_restore_request(&scope, &predelete, &inventory),
    )));
    let restored = recall_common(
        &provider,
        &scope,
        "typed-predelete-visible",
        common_query("history", T4),
    );
    assert_eq!(restored["candidates"].as_array().unwrap().len(), 1);
    assert_eq!(
        restored["candidates"][0]["provenance"]["original_sources"][0],
        source_b["source_identity"]["original_source"]
    );
    drop(provider);

    let fresh_state = root.join("typed-source-fence-import-state");
    // Reserve the snapshot's actual references and globally unique sequences in
    // their original scope before assigning sequences to other-delivery copies.
    let seed = NativeProvider::new(Arc::new(
        ProjectNativeMemoryApplicationPort::new(
            Arc::new(tokio::sync::RwLock::new(Arc::clone(&graph))),
            root.clone(),
            test_profile_id(),
            &fresh_state,
        )
        .unwrap()
        .with_admission_authority(Arc::new(FixtureAuthority {
            scope: scope.clone(),
            sources: inventory.iter().map(fixture_source).collect(),
            state: SourceDisposition::Available,
        })),
    ))
    .unwrap();
    body(&seed.invoke(&common_call(
        &seed,
        &scope,
        ProviderOperation::SnapshotRestore,
        "typed-import-predelete-seed",
        fixture_restore_request(&scope, &predelete, &inventory),
    )));
    drop(seed);
    let mut other_scope = scope.clone();
    other_scope.agent_session_id = "session.typed-fence-other-delivery".into();
    let other_port = Arc::new(
        ProjectNativeMemoryApplicationPort::new(
            Arc::new(tokio::sync::RwLock::new(Arc::clone(&graph))),
            root.clone(),
            test_profile_id(),
            &fresh_state,
        )
        .unwrap()
        .with_admission_authority(Arc::new(FixtureAuthority {
            scope: other_scope.clone(),
            sources: inventory.iter().map(fixture_source).collect(),
            state: SourceDisposition::Available,
        })),
    );
    let other = NativeProvider::new(other_port).unwrap();
    for (label, observation) in [("import-other-a", &source_a), ("import-other-b", &source_b)] {
        body(&other.invoke(&common_call(
            &other,
            &other_scope,
            ProviderOperation::Observe,
            label,
            observation.clone(),
        )));
    }
    let before_other = recall_common(
        &other,
        &other_scope,
        "import-other-before",
        common_query("history", T4),
    );
    assert_eq!(before_other["candidates"].as_array().unwrap().len(), 2);
    let a_ref = before_other["candidates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|candidate| {
            candidate["provenance"]["original_sources"][0]["source"]["canonical_provider_id"]
                == "codex"
        })
        .unwrap()["stable_memory_ref"]
        .as_str()
        .unwrap()
        .to_owned();
    drop(other);

    // The imported snapshot has only B in its active source inventory. Its typed
    // A fence must also scrub A's already staged copy in another delivery scope.
    let fresh = NativeProvider::new(Arc::new(
        ProjectNativeMemoryApplicationPort::new(
            Arc::new(tokio::sync::RwLock::new(Arc::clone(&graph))),
            root.clone(),
            test_profile_id(),
            &fresh_state,
        )
        .unwrap()
        .with_admission_authority(Arc::new(FixtureAuthority {
            scope: scope.clone(),
            sources: vec![fixture_source(&source_b)],
            state: SourceDisposition::Available,
        })),
    ))
    .unwrap();
    let export_state = |scope: &OwnedExactScope, label: &str| {
        exported_snapshot_body(
            &body(&fresh.invoke(&common_call(
                &fresh,
                scope,
                ProviderOperation::SnapshotExport,
                label,
                json!({}),
            )))["snapshot"],
        )
    };
    let before_import = export_state(&scope, "downgrade-before-scope");
    let before_other = export_state(&other_scope, "downgrade-before-other");
    let generation = fresh.descriptor().state_generation;
    let mut downgraded = postdelete.clone();
    let mut downgraded_body = post_body;
    downgraded_body["schema"] = "native-staged-v2".into();
    let bytes = serde_json::to_vec(&downgraded_body).unwrap();
    let digest = sha256_hex(&bytes);
    downgraded["bytes"] = json!(bytes);
    downgraded["identity"]["byte_length"] = bytes.len().into();
    downgraded["identity"]["snapshot_id"] = format!("native-snapshot:{digest}").into();
    downgraded["identity"]["content_sha256"] = digest.into();
    let refused = fresh.invoke(&common_call(
        &fresh,
        &scope,
        ProviderOperation::SnapshotRestore,
        "typed-downgrade-refused",
        fixture_restore_request(&scope, &downgraded, std::slice::from_ref(&source_b)),
    ));
    assert_eq!(refused.terminal.terminal_code(), TerminalCode::Conflict);
    assert_eq!(fresh.descriptor().state_generation, generation);
    assert_eq!(export_state(&scope, "downgrade-after-scope"), before_import);
    assert_eq!(
        export_state(&other_scope, "downgrade-after-other"),
        before_other
    );
    body(&fresh.invoke(&common_call(
        &fresh,
        &scope,
        ProviderOperation::SnapshotRestore,
        "typed-import-accepted",
        fixture_restore_request(&scope, &postdelete, std::slice::from_ref(&source_b)),
    )));
    for delivery in [&scope, &other_scope] {
        for mode in ["current", "as_of", "interval", "history"] {
            let mut query = common_query(mode, T1);
            query["temporal_query"]["include_revoked"] = true.into();
            query["temporal_query"]["include_superseded"] = true.into();
            let visible = recall_common(
                &fresh,
                delivery,
                &format!("import-visible-{}-{mode}", delivery.agent_session_id),
                query,
            );
            assert_eq!(visible["candidates"].as_array().unwrap().len(), 1);
            assert_eq!(
                visible["candidates"][0]["provenance"]["original_sources"][0],
                source_b["source_identity"]["original_source"]
            );
        }
    }
    let trace = body(&fresh.invoke(&common_call(
        &fresh,
        &other_scope,
        ProviderOperation::Inspection,
        "typed-import-a-scrubbed",
        inspection("trace", json!({"stable_memory_ref":a_ref})),
    )));
    assert_eq!(trace["items"].as_array().unwrap().len(), 1);
    assert!(trace["items"][0]["content"].is_null());
    assert!(trace["items"][0]["original_source"].is_null());
    let imported = export_state(&scope, "typed-import-exported");
    assert_eq!(
        imported["schema"],
        "native-staged-snapshot-source-fences-v1"
    );
    assert_eq!(
        imported["deletion_fences"][0]["kind"],
        "canonical_source_v1"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn common_advisory_fresh_deleted_restore_preserves_same_key_other_origin_across_deliveries() {
    use tracedecay_memory_provider_registry::SourceDisposition;
    let (_temp, root, graph, _owner, project) = real_project_fixture().await;
    let scope = recall_exact_scope(project.as_str());
    let mut other_scope = scope.clone();
    other_scope.agent_session_id = "session.fresh-deleted-other-delivery".into();
    let key = "common-advisory/source/fresh-deleted-shared";
    let source_a = deletion_source_fixture(&scope, 1, "codex", "session.canonical.a", key);
    let source_b = deletion_source_fixture(&scope, 2, "claude", "session.canonical.b", key);
    let inventory = vec![source_a.clone(), source_b.clone()];
    let provider = NativeProvider::new(new_port(&graph, &root)).unwrap();
    for (label, observation) in [
        ("fresh-deleted-a", &source_a),
        ("fresh-deleted-b", &source_b),
    ] {
        body(&provider.invoke(&common_call(
            &provider,
            &scope,
            ProviderOperation::Observe,
            label,
            observation.clone(),
        )));
    }
    let original = recall_common(
        &provider,
        &scope,
        "fresh-deleted-original-before",
        common_query("history", T4),
    );
    let reference_for = |recalled: &Value, observation: &Value| {
        recalled["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .find(|candidate| {
                candidate["provenance"]["original_sources"][0]
                    == observation["source_identity"]["original_source"]
            })
            .unwrap()["stable_memory_ref"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let original_a_ref = reference_for(&original, &source_a);
    let original_b_ref = reference_for(&original, &source_b);
    let settled = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Feedback,
        "fresh-deleted-b-feedback",
        json!({"target":lifecycle_target(&scope,&source_b,&original_b_ref),
            "signal":"helpful","weight":"0.5","occurred_at":T2,
            "canonical_outcome_receipt":"fixture.fresh-deleted-b-settled"}),
    )));
    let predelete = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::SnapshotExport,
        "fresh-deleted-predelete",
        json!({}),
    )))["snapshot"]
        .clone();
    assert!(
        exported_snapshot_body(&predelete)["deletion_fences"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    drop(provider);

    let other = NativeProvider::new(new_authorized_port(
        &graph,
        &root,
        &other_scope,
        &inventory,
        SourceDisposition::Available,
    ))
    .unwrap();
    for (label, observation) in [
        ("fresh-deleted-other-a", &source_a),
        ("fresh-deleted-other-b", &source_b),
    ] {
        body(&other.invoke(&common_call(
            &other,
            &other_scope,
            ProviderOperation::Observe,
            label,
            observation.clone(),
        )));
    }
    let other_before = recall_common(
        &other,
        &other_scope,
        "fresh-deleted-other-before",
        common_query("history", T4),
    );
    assert_eq!(other_before["candidates"].as_array().unwrap().len(), 2);
    let other_a_ref = reference_for(&other_before, &source_a);
    let other_b_ref = reference_for(&other_before, &source_b);
    let other_settled = body(&other.invoke(&common_call(
        &other,
        &other_scope,
        ProviderOperation::Feedback,
        "fresh-deleted-other-b-feedback",
        json!({"target":lifecycle_target(&other_scope,&source_b,&other_b_ref),
            "signal":"helpful","weight":"0.5","occurred_at":T2,
            "canonical_outcome_receipt":"fixture.fresh-deleted-other-b-settled"}),
    )));
    drop(other);

    let restored = NativeProvider::new(Arc::new(
        ProjectNativeMemoryApplicationPort::new(
            Arc::new(tokio::sync::RwLock::new(Arc::clone(&graph))),
            root.clone(),
            test_profile_id(),
            &test_provider_state_root(&root),
        )
        .unwrap()
        .with_admission_authority(Arc::new(FixtureRestoreDispositionAuthority {
            scope: scope.clone(),
            sources: vec![
                (fixture_source(&source_a), SourceDisposition::Deleted),
                (fixture_source(&source_b), SourceDisposition::Available),
            ],
        })),
    ))
    .unwrap();
    let before_restore = body(&restored.invoke(&common_call(
        &restored,
        &scope,
        ProviderOperation::SnapshotExport,
        "fresh-deleted-no-local-fence",
        json!({}),
    )));
    assert!(
        exported_snapshot_body(&before_restore["snapshot"])["deletion_fences"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let request = fixture_restore_request(&scope, &predelete, &inventory);
    assert_eq!(
        request["source_dispositions"][0]["current_disposition"]["state"],
        "available"
    );
    body(&restored.invoke(&common_call(
        &restored,
        &scope,
        ProviderOperation::SnapshotRestore,
        "fresh-deleted-restore",
        request,
    )));
    for (delivery, a_ref, b_ref, receipt) in [
        (
            &scope,
            &original_a_ref,
            &original_b_ref,
            &settled["provider_receipt_digest"],
        ),
        (
            &other_scope,
            &other_a_ref,
            &other_b_ref,
            &other_settled["provider_receipt_digest"],
        ),
    ] {
        for mode in ["current", "as_of", "interval", "history"] {
            let mut query = common_query(mode, T1);
            query["temporal_query"]["include_revoked"] = true.into();
            query["temporal_query"]["include_superseded"] = true.into();
            let visible = recall_common(
                &restored,
                delivery,
                &format!("fresh-deleted-visible-{}-{mode}", delivery.agent_session_id),
                query,
            );
            assert_eq!(visible["candidates"].as_array().unwrap().len(), 1);
            assert_eq!(
                visible["candidates"][0]["stable_memory_ref"],
                b_ref.as_str()
            );
            assert_eq!(
                visible["candidates"][0]["content"],
                source_b["canonical_payload"]["content"]
            );
            assert_eq!(
                visible["candidates"][0]["provenance"]["original_sources"][0],
                source_b["source_identity"]["original_source"]
            );
        }
        let influence = body(&restored.invoke(&common_call(
            &restored,
            delivery,
            ProviderOperation::Inspection,
            &format!("fresh-deleted-influence-{}", delivery.agent_session_id),
            inspection("source_influence", json!({"source_key":key})),
        )));
        assert_eq!(influence["items"].as_array().unwrap().len(), 1);
        assert_eq!(
            influence["items"][0]["source"],
            source_b["source_identity"]["original_source"]["source"]
        );
        assert_eq!(influence["items"][0]["settled_feedback"]["helpful"], 1);
        assert_eq!(&influence["items"][0]["last_feedback_receipt"], receipt);
        let scrubbed = body(&restored.invoke(&common_call(
            &restored,
            delivery,
            ProviderOperation::Inspection,
            &format!("fresh-deleted-a-scrubbed-{}", delivery.agent_session_id),
            inspection("trace", json!({"stable_memory_ref":a_ref})),
        )));
        assert_eq!(scrubbed["items"].as_array().unwrap().len(), 1);
        assert!(scrubbed["items"][0]["content"].is_null());
        assert!(scrubbed["items"][0]["original_source"].is_null());
    }
    let after_restore = body(&restored.invoke(&common_call(
        &restored,
        &scope,
        ProviderOperation::SnapshotExport,
        "fresh-deleted-fence-created",
        json!({}),
    )));
    let after_body = exported_snapshot_body(&after_restore["snapshot"]);
    assert_eq!(after_body["deletion_fences"].as_array().unwrap().len(), 1);
    assert_eq!(
        after_body["deletion_fences"][0]["kind"],
        "canonical_source_v1"
    );
    drop(restored);
    let reopened = NativeProvider::new(new_port(&graph, &root)).unwrap();
    for delivery in [&scope, &other_scope] {
        let visible = recall_common(
            &reopened,
            delivery,
            &format!("fresh-deleted-reopened-{}", delivery.agent_session_id),
            common_query("history", T4),
        );
        assert_eq!(visible["candidates"].as_array().unwrap().len(), 1);
        assert_eq!(
            visible["candidates"][0]["provenance"]["original_sources"][0],
            source_b["source_identity"]["original_source"]
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn common_advisory_restore_refuses_other_delivery_legacy_ambiguity_atomically() {
    use tracedecay_memory_provider_registry::SourceDisposition;
    for import_typed_fence in [false, true] {
        let (_temp, root, graph, _owner, project) = real_project_fixture().await;
        let scope = recall_exact_scope(project.as_str());
        let mut other_scope = scope.clone();
        other_scope.agent_session_id = "session.restore-ambiguous-other-delivery".into();
        let key = "common-advisory/source/restore-ambiguous";
        let source_a = deletion_source_fixture(&scope, 1, "codex", "session.canonical.a", key);
        let source_b = deletion_source_fixture(&scope, 2, "claude", "session.canonical.b", key);
        let inventory = vec![source_a.clone(), source_b.clone()];
        let source = NativeProvider::new(new_authorized_port(
            &graph,
            &root,
            &scope,
            std::slice::from_ref(&source_a),
            SourceDisposition::Available,
        ))
        .unwrap();
        for (label, observation) in [
            ("ambiguous-snapshot-a", &source_a),
            ("ambiguous-snapshot-b", &source_b),
        ] {
            body(&source.invoke(&common_call(
                &source,
                &scope,
                ProviderOperation::Observe,
                label,
                observation.clone(),
            )));
        }
        let predelete = body(&source.invoke(&common_call(
            &source,
            &scope,
            ProviderOperation::SnapshotExport,
            "ambiguous-predelete",
            json!({}),
        )))["snapshot"]
            .clone();
        let snapshot = if import_typed_fence {
            body(
                &source.invoke(
                    &common_call(
                        &source,
                        &scope,
                        ProviderOperation::DeleteBySource,
                        "ambiguous-source-delete",
                        source_deletion_request(&[key]),
                    )
                    .with_history_grant(fixture_private_history(
                        &scope,
                        std::slice::from_ref(&source_a),
                    )),
                ),
            );
            body(&source.invoke(&common_call(
                &source,
                &scope,
                ProviderOperation::SnapshotExport,
                "ambiguous-postdelete",
                json!({}),
            )))["snapshot"]
                .clone()
        } else {
            predelete.clone()
        };
        drop(source);

        let state_root = root.join("restore-ambiguity-destination-state");
        // Seed the original snapshot first so later copies use actual new global
        // sequences; this test changes privacy evidence, never snapshot identity.
        let seed = NativeProvider::new(Arc::new(
            ProjectNativeMemoryApplicationPort::new(
                Arc::new(tokio::sync::RwLock::new(Arc::clone(&graph))),
                root.clone(),
                test_profile_id(),
                &state_root,
            )
            .unwrap()
            .with_admission_authority(Arc::new(FixtureAuthority {
                scope: scope.clone(),
                sources: inventory.iter().map(fixture_source).collect(),
                state: SourceDisposition::Available,
            })),
        ))
        .unwrap();
        body(&seed.invoke(&common_call(
            &seed,
            &scope,
            ProviderOperation::SnapshotRestore,
            "ambiguous-destination-seed",
            fixture_restore_request(&scope, &predelete, &inventory),
        )));
        drop(seed);

        let other = NativeProvider::new(Arc::new(
            ProjectNativeMemoryApplicationPort::new(
                Arc::new(tokio::sync::RwLock::new(Arc::clone(&graph))),
                root.clone(),
                test_profile_id(),
                &state_root,
            )
            .unwrap()
            .with_admission_authority(Arc::new(FixtureAuthority {
                scope: other_scope.clone(),
                sources: inventory.iter().map(fixture_source).collect(),
                state: SourceDisposition::Available,
            })),
        ))
        .unwrap();
        for (label, observation) in [
            ("ambiguous-other-a", &source_a),
            ("ambiguous-other-b", &source_b),
        ] {
            body(&other.invoke(&common_call(
                &other,
                &other_scope,
                ProviderOperation::Observe,
                label,
                observation.clone(),
            )));
        }
        // A fresh Deleted source has a known key. An imported digest has no
        // preimage for any unattributed row, even one with an unrelated raw key.
        let legacy_key = if import_typed_fence {
            "common-advisory/source/unattributed-unrelated"
        } else {
            key
        };
        body(&other.invoke(&common_call(
            &other, &other_scope, ProviderOperation::Observe, "ambiguous-other-legacy",
            json!({"canonical_payload":session_message_payload(legacy_key, &other_scope.agent_session_id, "compatibility beacon retained legacy"),
                "observation_kind":NATIVE_STAGED_SESSION_OBSERVATION_KIND,
                "payload_contract":NATIVE_STAGED_SESSION_PAYLOAD_CONTRACT_ID}),
        )));
        let legacy_receipt = body(&other.invoke(&common_call(
            &other,
            &other_scope,
            ProviderOperation::Inspection,
            "ambiguous-legacy-receipt",
            inspection(
                "delivery_receipt",
                json!({"idempotency_key":sha256_hex(b"ambiguous-other-legacy")}),
            ),
        )));
        let legacy_ref = legacy_receipt["items"][0]["stable_memory_ref"]
            .as_str()
            .unwrap()
            .to_owned();
        drop(other);

        let declared = if import_typed_fence {
            vec![source_b.clone()]
        } else {
            inventory.clone()
        };
        let dispositions = if import_typed_fence {
            vec![(fixture_source(&source_b), SourceDisposition::Available)]
        } else {
            vec![
                (fixture_source(&source_a), SourceDisposition::Deleted),
                (fixture_source(&source_b), SourceDisposition::Available),
            ]
        };
        let provider = NativeProvider::new(Arc::new(
            ProjectNativeMemoryApplicationPort::new(
                Arc::new(tokio::sync::RwLock::new(Arc::clone(&graph))),
                root.clone(),
                test_profile_id(),
                &state_root,
            )
            .unwrap()
            .with_admission_authority(Arc::new(FixtureRestoreDispositionAuthority {
                scope: scope.clone(),
                sources: dispositions,
            })),
        ))
        .unwrap();
        let inspect_state = |provider: &NativeProvider, delivery: &OwnedExactScope, label: &str| {
            body(&provider.invoke(&common_call(
                provider,
                delivery,
                ProviderOperation::Inspection,
                label,
                inspection("state_summary", json!({})),
            )))["items"]
                .clone()
        };
        let export_state = |provider: &NativeProvider, label: &str| {
            exported_snapshot_body(
                &body(&provider.invoke(&common_call(
                    provider,
                    &scope,
                    ProviderOperation::SnapshotExport,
                    label,
                    json!({}),
                )))["snapshot"],
            )
        };
        let original_before = inspect_state(&provider, &scope, "ambiguous-original-before");
        let other_before = inspect_state(&provider, &other_scope, "ambiguous-other-before");
        assert_eq!(original_before.as_array().unwrap().len(), 2);
        assert_eq!(other_before.as_array().unwrap().len(), 3);
        let export_before = export_state(&provider, "ambiguous-fences-before");
        assert!(
            export_before["deletion_fences"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        let generation = provider.descriptor().state_generation;
        let reply = provider.invoke(&common_call(
            &provider,
            &scope,
            ProviderOperation::SnapshotRestore,
            "ambiguous-restore-refused",
            fixture_restore_request(&scope, &snapshot, &declared),
        ));
        assert_eq!(reply.terminal.terminal_code(), TerminalCode::Conflict);
        assert_eq!(provider.descriptor().state_generation, generation);
        assert_eq!(
            inspect_state(&provider, &scope, "ambiguous-original-after"),
            original_before
        );
        assert_eq!(
            inspect_state(&provider, &other_scope, "ambiguous-other-after"),
            other_before
        );
        assert_eq!(
            export_state(&provider, "ambiguous-fences-after"),
            export_before
        );
        let legacy = body(&provider.invoke(&common_call(
            &provider,
            &other_scope,
            ProviderOperation::Inspection,
            "ambiguous-legacy-preserved",
            inspection("trace", json!({"stable_memory_ref":legacy_ref})),
        )));
        assert_eq!(
            legacy["items"][0]["content"],
            "compatibility beacon retained legacy"
        );
        assert!(legacy["items"][0]["original_source"].is_null());
        drop(provider);

        let reopened = NativeProvider::new(Arc::new(
            ProjectNativeMemoryApplicationPort::new(
                Arc::new(tokio::sync::RwLock::new(Arc::clone(&graph))),
                root.clone(),
                test_profile_id(),
                &state_root,
            )
            .unwrap(),
        ))
        .unwrap();
        assert_eq!(reopened.descriptor().state_generation, generation);
        assert_eq!(
            inspect_state(&reopened, &scope, "ambiguous-original-reopened"),
            original_before
        );
        assert_eq!(
            inspect_state(&reopened, &other_scope, "ambiguous-other-reopened"),
            other_before
        );
        assert_eq!(
            export_state(&reopened, "ambiguous-fences-reopened"),
            export_before
        );
        // This also detects a typed fence that leaked from a refused restore.
        body(&reopened.invoke(&common_call(
            &reopened,
            &scope,
            ProviderOperation::Observe,
            "ambiguous-a-after-refusal",
            deletion_source_fixture(&scope, 5, "codex", "session.canonical.a", key),
        )));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn common_advisory_legacy_raw_fence_digest_collision_keeps_its_raw_meaning() {
    use tracedecay_memory_provider_registry::SourceDisposition;
    let (_temp, root, graph, _owner, project) = real_project_fixture().await;
    let scope = recall_exact_scope(project.as_str());
    let key = "common-advisory/source/digest-shared";
    let source_a = deletion_source_fixture(&scope, 1, "codex", "session.canonical.a", key);
    let source_b = deletion_source_fixture(&scope, 2, "claude", "session.canonical.b", key);
    let probe = NativeProvider::new(new_authorized_port(
        &graph,
        &root,
        &scope,
        std::slice::from_ref(&source_a),
        SourceDisposition::Available,
    ))
    .unwrap();
    // Obtain the identity from a real typed deletion, without using its hash helper.
    body(
        &probe.invoke(
            &common_call(
                &probe,
                &scope,
                ProviderOperation::DeleteBySource,
                "digest-probe-delete",
                source_deletion_request(&[key]),
            )
            .with_history_grant(fixture_private_history(
                &scope,
                std::slice::from_ref(&source_a),
            )),
        ),
    );
    let probe_snapshot = body(&probe.invoke(&common_call(
        &probe,
        &scope,
        ProviderOperation::SnapshotExport,
        "digest-probe-export",
        json!({}),
    )));
    let probe_body = exported_snapshot_body(&probe_snapshot["snapshot"]);
    let fence = &probe_body["deletion_fences"][0];
    assert_eq!(fence["kind"], "canonical_source_v1");
    let digest = fence["source_key"].as_str().unwrap().to_owned();
    assert_eq!(digest.len(), 64);
    drop(probe);

    let raw_source = deletion_source_fixture(&scope, 3, "gemini", "session.canonical.raw", &digest);
    // The opposite collision direction matters too: a typed A fence must not
    // erase or block a live source whose literal raw key happens to be its digest.
    let typed_state = root.join("typed-digest-collision-state");
    let typed = NativeProvider::new(Arc::new(
        ProjectNativeMemoryApplicationPort::new(
            Arc::new(tokio::sync::RwLock::new(Arc::clone(&graph))),
            root.clone(),
            test_profile_id(),
            &typed_state,
        )
        .unwrap()
        .with_admission_authority(Arc::new(FixtureAuthority {
            scope: scope.clone(),
            sources: vec![fixture_source(&source_a)],
            state: SourceDisposition::Available,
        })),
    ))
    .unwrap();
    for (label, observation) in [
        ("typed-collision-a", &source_a),
        ("typed-collision-b", &source_b),
        ("typed-collision-raw", &raw_source),
    ] {
        body(&typed.invoke(&common_call(
            &typed,
            &scope,
            ProviderOperation::Observe,
            label,
            observation.clone(),
        )));
    }
    let deleted_a = body(
        &typed.invoke(
            &common_call(
                &typed,
                &scope,
                ProviderOperation::DeleteBySource,
                "typed-collision-delete-a",
                source_deletion_request(&[key]),
            )
            .with_history_grant(fixture_private_history(
                &scope,
                std::slice::from_ref(&source_a),
            )),
        ),
    );
    assert_eq!(deleted_a["postcondition"]["removed_effects"], 1);
    let visible = recall_common(
        &typed,
        &scope,
        "typed-collision-raw-survives",
        common_query("history", T4),
    );
    assert_eq!(visible["candidates"].as_array().unwrap().len(), 2);
    assert!(
        visible["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .any(|candidate| {
                candidate["provenance"]["original_sources"][0]
                    == raw_source["source_identity"]["original_source"]
            })
    );
    let later_raw = deletion_source_fixture(&scope, 4, "gemini", "session.canonical.raw", &digest);
    body(&typed.invoke(&common_call(
        &typed,
        &scope,
        ProviderOperation::Observe,
        "typed-collision-raw-revision",
        later_raw.clone(),
    )));
    let later = recall_common(
        &typed,
        &scope,
        "typed-collision-raw-later-visible",
        common_query("history", T4),
    );
    assert!(
        later["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .any(|candidate| {
                candidate["provenance"]["original_sources"][0]
                    == later_raw["source_identity"]["original_source"]
            })
    );
    drop(typed);

    let fresh_state = root.join("raw-digest-collision-state");
    let fresh = NativeProvider::new(Arc::new(
        ProjectNativeMemoryApplicationPort::new(
            Arc::new(tokio::sync::RwLock::new(Arc::clone(&graph))),
            root.clone(),
            test_profile_id(),
            &fresh_state,
        )
        .unwrap(),
    ))
    .unwrap();
    for (label, observation) in [
        ("digest-a", &source_a),
        ("digest-b", &source_b),
        ("digest-raw", &raw_source),
    ] {
        body(&fresh.invoke(&common_call(
            &fresh,
            &scope,
            ProviderOperation::Observe,
            label,
            observation.clone(),
        )));
    }
    let deleted_raw = body(&fresh.invoke(&common_call(
        &fresh,
        &scope,
        ProviderOperation::DeleteBySource,
        "digest-legacy-raw-delete",
        source_deletion_request(&[&digest]),
    )));
    assert_eq!(deleted_raw["postcondition"]["removed_effects"], 1);
    let visible = recall_common(
        &fresh,
        &scope,
        "digest-raw-leaves-a-b",
        common_query("current", T4),
    );
    assert_eq!(visible["candidates"].as_array().unwrap().len(), 2);
    assert!(
        visible["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .any(|candidate| {
                candidate["provenance"]["original_sources"][0]
                    == source_a["source_identity"]["original_source"]
            })
    );
    let raw_snapshot = body(&fresh.invoke(&common_call(
        &fresh,
        &scope,
        ProviderOperation::SnapshotExport,
        "digest-legacy-export",
        json!({}),
    )))["snapshot"]
        .clone();
    let raw_body = exported_snapshot_body(&raw_snapshot);
    assert_eq!(raw_body["schema"], "native-staged-v2");
    assert_eq!(raw_body["deletion_fences"].as_array().unwrap().len(), 1);
    assert_eq!(raw_body["deletion_fences"][0]["source_key"], digest);
    assert!(raw_body["deletion_fences"][0].get("kind").is_none());
    drop(fresh);

    // Importing the kind-absent legacy fence must not reinterpret its raw key
    // as the identically spelled canonical-source digest for A.
    let imported_state = root.join("raw-digest-collision-import-state");
    let inventory = vec![source_a.clone(), source_b.clone()];
    let imported = NativeProvider::new(Arc::new(
        ProjectNativeMemoryApplicationPort::new(
            Arc::new(tokio::sync::RwLock::new(Arc::clone(&graph))),
            root.clone(),
            test_profile_id(),
            &imported_state,
        )
        .unwrap()
        .with_admission_authority(Arc::new(FixtureAuthority {
            scope: scope.clone(),
            sources: inventory.iter().map(fixture_source).collect(),
            state: SourceDisposition::Available,
        })),
    ))
    .unwrap();
    body(&imported.invoke(&common_call(
        &imported,
        &scope,
        ProviderOperation::SnapshotRestore,
        "digest-legacy-import",
        fixture_restore_request(&scope, &raw_snapshot, &inventory),
    )));
    assert_eq!(
        recall_common(
            &imported,
            &scope,
            "digest-import-a-b",
            common_query("history", T4)
        )["candidates"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        imported
            .invoke(&common_call(
                &imported,
                &scope,
                ProviderOperation::Observe,
                "digest-raw-resurrection",
                raw_source,
            ))
            .terminal
            .terminal_code(),
        TerminalCode::Conflict
    );
    drop(imported);

    let imported = NativeProvider::new(Arc::new(
        ProjectNativeMemoryApplicationPort::new(
            Arc::new(tokio::sync::RwLock::new(Arc::clone(&graph))),
            root.clone(),
            test_profile_id(),
            &imported_state,
        )
        .unwrap()
        .with_admission_authority(Arc::new(FixtureAuthority {
            scope: scope.clone(),
            sources: vec![fixture_source(&source_a)],
            state: SourceDisposition::Available,
        })),
    ))
    .unwrap();
    let deleted_a = body(
        &imported.invoke(
            &common_call(
                &imported,
                &scope,
                ProviderOperation::DeleteBySource,
                "digest-typed-a-delete",
                source_deletion_request(&[key]),
            )
            .with_history_grant(fixture_private_history(
                &scope,
                std::slice::from_ref(&source_a),
            )),
        ),
    );
    assert_eq!(deleted_a["postcondition"]["removed_effects"], 1);
    let remaining = recall_common(
        &imported,
        &scope,
        "digest-typed-only-b",
        common_query("history", T4),
    );
    assert_eq!(remaining["candidates"].as_array().unwrap().len(), 1);
    assert_eq!(
        remaining["candidates"][0]["provenance"]["original_sources"][0],
        source_b["source_identity"]["original_source"]
    );
    let exported = body(&imported.invoke(&common_call(
        &imported,
        &scope,
        ProviderOperation::SnapshotExport,
        "digest-both-fences-export",
        json!({}),
    )));
    let final_body = exported_snapshot_body(&exported["snapshot"]);
    assert_eq!(
        final_body["schema"],
        "native-staged-snapshot-source-fences-v1"
    );
    let fences = final_body["deletion_fences"].as_array().unwrap();
    assert_eq!(fences.len(), 2);
    assert!(fences.iter().all(|fence| fence["source_key"] == digest));
    assert_eq!(
        fences
            .iter()
            .filter(|fence| fence.get("kind").is_none())
            .count(),
        1
    );
    assert_eq!(
        fences
            .iter()
            .filter(|fence| fence["kind"] == "canonical_source_v1")
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn common_advisory_claimed_source_delete_revalidates_authority_and_idempotency_origin() {
    use tracedecay_memory_provider_registry::SourceDisposition;
    let (_temp, root, graph, _owner, project) = real_project_fixture().await;
    let scope = recall_exact_scope(project.as_str());
    let key = "common-advisory/source/admission-shared";
    let source_a = deletion_source_fixture(&scope, 1, "codex", "session.canonical.a", key);
    let source_b = deletion_source_fixture(&scope, 2, "claude", "session.canonical.b", key);
    let inventory = vec![source_a.clone(), source_b.clone()];
    let provider = NativeProvider::new(new_port(&graph, &root)).unwrap();
    for (label, observation) in [("admission-a", &source_a), ("admission-b", &source_b)] {
        body(&provider.invoke(&common_call(
            &provider,
            &scope,
            ProviderOperation::Observe,
            label,
            observation.clone(),
        )));
    }
    let export = |provider: &NativeProvider, label: &str| {
        exported_snapshot_body(
            &body(&provider.invoke(&common_call(
                provider,
                &scope,
                ProviderOperation::SnapshotExport,
                label,
                json!({}),
            )))["snapshot"],
        )
    };
    let before = export(&provider, "admission-before");
    let generation = provider.descriptor().state_generation;
    let claim_a = common_call(
        &provider,
        &scope,
        ProviderOperation::DeleteBySource,
        "same-deletion-identity",
        source_deletion_request(&[key]),
    )
    .with_history_grant(fixture_private_history(
        &scope,
        std::slice::from_ref(&source_a),
    ));
    let missing = provider.invoke(&claim_a);
    assert!(!matches!(
        missing.terminal.terminal_code(),
        TerminalCode::Success | TerminalCode::SuccessZeroResults | TerminalCode::Partial
    ));
    assert_eq!(provider.descriptor().state_generation, generation);
    assert_eq!(export(&provider, "admission-after-missing"), before);
    drop(provider);

    // This installed authority independently knows B only. A's fabricated
    // private grant cannot add A to that fresh inventory.
    let provider = NativeProvider::new(new_authorized_port(
        &graph,
        &root,
        &scope,
        std::slice::from_ref(&source_b),
        SourceDisposition::Available,
    ))
    .unwrap();
    let fabricated = provider.invoke(&claim_a);
    assert!(!matches!(
        fabricated.terminal.terminal_code(),
        TerminalCode::Success | TerminalCode::SuccessZeroResults | TerminalCode::Partial
    ));
    assert_eq!(provider.descriptor().state_generation, generation);
    assert_eq!(export(&provider, "admission-after-fabricated"), before);
    drop(provider);

    let provider = NativeProvider::new(new_authorized_port(
        &graph,
        &root,
        &scope,
        &inventory,
        SourceDisposition::Available,
    ))
    .unwrap();
    let ambiguous = claim_a
        .clone()
        .with_history_grant(fixture_private_history(&scope, &inventory));
    assert_eq!(
        provider.invoke(&ambiguous).terminal.terminal_code(),
        TerminalCode::Conflict
    );
    assert_eq!(provider.descriptor().state_generation, generation);
    assert_eq!(export(&provider, "admission-after-ambiguous"), before);
    drop(provider);

    let provider = NativeProvider::new(new_authorized_port(
        &graph,
        &root,
        &scope,
        std::slice::from_ref(&source_a),
        SourceDisposition::Available,
    ))
    .unwrap();
    let deletion = body(&provider.invoke(&claim_a));
    assert_eq!(deletion["postcondition"]["removed_effects"], 1);
    assert_eq!(
        provider
            .invoke(&claim_a)
            .terminal
            .committed_effect()
            .state(),
        CommittedEffectState::Duplicate
    );
    let after_a = export(&provider, "admission-after-real-a");
    let generation_after_a = provider.descriptor().state_generation;
    drop(provider);

    // The fresh source inventory actually changes to B, while every original
    // call field and payload byte remains identical except the private claim.
    let provider = NativeProvider::new(new_authorized_port(
        &graph,
        &root,
        &scope,
        std::slice::from_ref(&source_b),
        SourceDisposition::Available,
    ))
    .unwrap();
    let claim_b = claim_a.clone().with_history_grant(fixture_private_history(
        &scope,
        std::slice::from_ref(&source_b),
    ));
    assert_eq!(claim_b.idempotency_key, claim_a.idempotency_key);
    assert_eq!(claim_b.payload.bytes, claim_a.payload.bytes);
    assert_eq!(
        provider.invoke(&claim_b).terminal.terminal_code(),
        TerminalCode::Conflict
    );
    assert_eq!(provider.descriptor().state_generation, generation_after_a);
    assert_eq!(export(&provider, "admission-after-reused-origin"), after_a);
    let visible = recall_common(
        &provider,
        &scope,
        "admission-b-survives-conflict",
        common_query("history", T4),
    );
    assert_eq!(visible["candidates"].as_array().unwrap().len(), 1);
    assert_eq!(
        visible["candidates"][0]["provenance"]["original_sources"][0],
        source_b["source_identity"]["original_source"]
    );
    let deleted_b = body(
        &provider.invoke(
            &common_call(
                &provider,
                &scope,
                ProviderOperation::DeleteBySource,
                "fresh-b-deletion-identity",
                source_deletion_request(&[key]),
            )
            .with_history_grant(fixture_private_history(
                &scope,
                std::slice::from_ref(&source_b),
            )),
        ),
    );
    assert_eq!(deleted_b["postcondition"]["removed_effects"], 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn common_advisory_claimed_source_delete_refuses_legacy_ambiguity_atomically() {
    use tracedecay_memory_provider_registry::SourceDisposition;
    let (_temp, root, graph, _owner, project) = real_project_fixture().await;
    let scope = recall_exact_scope(project.as_str());
    let key = "common-advisory/source/legacy-shared";
    let clean_key = "common-advisory/source/clean-first";
    let source_a = deletion_source_fixture(&scope, 1, "codex", "session.canonical.a", key);
    let source_b = deletion_source_fixture(&scope, 2, "claude", "session.canonical.b", key);
    let clean = deletion_source_fixture(&scope, 3, "codex", "session.canonical.clean", clean_key);
    let inventory = vec![source_a.clone(), clean.clone()];
    let provider = NativeProvider::new(new_authorized_port(
        &graph,
        &root,
        &scope,
        &inventory,
        SourceDisposition::Available,
    ))
    .unwrap();
    for (label, observation) in [
        ("legacy-a", &source_a),
        ("legacy-b", &source_b),
        ("legacy-clean", &clean),
    ] {
        body(&provider.invoke(&common_call(
            &provider,
            &scope,
            ProviderOperation::Observe,
            label,
            observation.clone(),
        )));
    }
    // A real old envelope has only its raw stable record key, with no original
    // attribution. It cannot be silently assigned to either canonical origin.
    let legacy = json!({
        "canonical_payload":session_message_payload(key, &scope.agent_session_id, "compatibility beacon legacy unattributed"),
        "observation_kind":NATIVE_STAGED_SESSION_OBSERVATION_KIND,
        "payload_contract":NATIVE_STAGED_SESSION_PAYLOAD_CONTRACT_ID,
    });
    body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Observe,
        "legacy-unattributed",
        legacy,
    )));
    let state = |provider: &NativeProvider, label: &str| {
        body(&provider.invoke(&common_call(
            provider,
            &scope,
            ProviderOperation::Inspection,
            label,
            inspection("state_summary", json!({})),
        )))
    };
    let before = state(&provider, "legacy-before-refusal");
    assert_eq!(before["items"].as_array().unwrap().len(), 4);
    assert!(
        before["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["privacy_or_retention_tombstone"] == false)
    );
    let generation = provider.descriptor().state_generation;
    let refused = provider.invoke(
        &common_call(
            &provider,
            &scope,
            ProviderOperation::DeleteBySource,
            "legacy-ambiguous-batch",
            // Put an independently resolvable source first to catch partial effects.
            source_deletion_request(&[clean_key, key]),
        )
        .with_history_grant(fixture_private_history(&scope, &inventory)),
    );
    assert_eq!(refused.terminal.terminal_code(), TerminalCode::Conflict);
    assert_eq!(provider.descriptor().state_generation, generation);
    assert_eq!(
        state(&provider, "legacy-after-refusal")["items"],
        before["items"]
    );
    let legacy_receipt = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Inspection,
        "legacy-delivery-ref",
        inspection(
            "delivery_receipt",
            json!({"idempotency_key":sha256_hex(b"legacy-unattributed")}),
        ),
    )));
    let legacy_ref = legacy_receipt["items"][0]["stable_memory_ref"]
        .as_str()
        .unwrap()
        .to_owned();
    let trace = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Inspection,
        "legacy-content-preserved",
        inspection("trace", json!({"stable_memory_ref":legacy_ref})),
    )));
    assert_eq!(
        trace["items"][0]["content"],
        "compatibility beacon legacy unattributed"
    );
    assert!(trace["items"][0]["original_source"].is_null());
    drop(provider);

    let provider = NativeProvider::new(new_port(&graph, &root)).unwrap();
    assert_eq!(provider.descriptor().state_generation, generation);
    assert_eq!(
        state(&provider, "legacy-refusal-reopen")["items"],
        before["items"]
    );
    for (label, observation) in [
        (
            "legacy-a-after-refusal",
            deletion_source_fixture(&scope, 5, "codex", "session.canonical.a", key),
        ),
        (
            "legacy-clean-after-refusal",
            deletion_source_fixture(&scope, 6, "codex", "session.canonical.clean", clean_key),
        ),
    ] {
        // Success after reopen also proves no partial privacy fence survived.
        body(&provider.invoke(&common_call(
            &provider,
            &scope,
            ProviderOperation::Observe,
            label,
            observation,
        )));
    }
    // Existing unclaimed deletion still intentionally covers all raw-key matches:
    // both attributed origins, A's later revision, and the unattributed old row.
    let broad = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::DeleteBySource,
        "legacy-unclaimed-broad",
        source_deletion_request(&[key]),
    )));
    assert_eq!(broad["postcondition"]["removed_effects"], 4);
    assert_eq!(broad["postcondition"]["remaining_influence_count"], 0);
    let exported = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::SnapshotExport,
        "legacy-broad-export",
        json!({}),
    )));
    let snapshot = exported_snapshot_body(&exported["snapshot"]);
    assert_eq!(snapshot["schema"], "native-staged-v2");
    assert_eq!(snapshot["deletion_fences"].as_array().unwrap().len(), 1);
    assert_eq!(snapshot["deletion_fences"][0]["source_key"], key);
    assert!(snapshot["deletion_fences"][0].get("kind").is_none());
    drop(provider);

    let provider = NativeProvider::new(new_port(&graph, &root)).unwrap();
    let visible = recall_common(
        &provider,
        &scope,
        "legacy-broad-reopen",
        common_query("history", T4),
    );
    assert_eq!(visible["candidates"].as_array().unwrap().len(), 2);
    assert!(
        visible["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .all(|candidate| {
                candidate["provenance"]["original_sources"][0]["source"]["source_key"] == clean_key
            })
    );
    let trace = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Inspection,
        "legacy-broad-content-scrubbed",
        inspection("trace", json!({"stable_memory_ref":legacy_ref})),
    )));
    assert!(trace["items"][0]["content"].is_null());
    assert_eq!(
        provider
            .invoke(&common_call(
                &provider,
                &scope,
                ProviderOperation::Observe,
                "legacy-b-fenced-after-reopen",
                deletion_source_fixture(&scope, 7, "claude", "session.canonical.b", key),
            ))
            .terminal
            .terminal_code(),
        TerminalCode::Conflict
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn common_advisory_lost_reply_reconciles_original_receipt_and_dry_run_writes_nothing() {
    let (_temp, root, graph, _owner, project) = real_project_fixture().await;
    let scope = recall_exact_scope(project.as_str());
    let port = new_port(&graph, &root);
    let provider = NativeProvider::new(port.clone()).unwrap();
    let observation = common_observation(
        &scope,
        1,
        Some("r1"),
        "compatibility beacon lost reply",
        Some(T1),
        None,
    );
    let call = common_call(
        &provider,
        &scope,
        ProviderOperation::Observe,
        "lost",
        observation,
    );
    port.staged.lose_next_reply();
    let lost = provider.invoke(&call);
    assert_eq!(
        lost.terminal.committed_effect().state(),
        CommittedEffectState::Unknown
    );
    let generation = provider.descriptor().state_generation;
    assert!(generation > 0);
    let retry = provider.invoke(&call);
    assert_eq!(
        retry.terminal.committed_effect().state(),
        CommittedEffectState::Duplicate
    );
    assert_eq!(provider.descriptor().state_generation, generation);
    let receipt = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Inspection,
        "inspect-receipt",
        inspection(
            "delivery_receipt",
            json!({"idempotency_key":call.idempotency_key}),
        ),
    )));
    assert_eq!(receipt["items"].as_array().unwrap().len(), 1);
    assert_eq!(receipt["items"][0]["operation_id"], call.operation_id);
    assert_eq!(
        receipt["items"][0]["idempotency_key"],
        call.idempotency_key.clone().unwrap()
    );
    let reference = receipt["items"][0]["stable_memory_ref"].as_str().unwrap();
    let trace = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Inspection,
        "inspect-trace",
        inspection("trace", json!({"stable_memory_ref":reference})),
    )));
    assert_eq!(
        trace["items"][0]["content"],
        "compatibility beacon lost reply"
    );
    assert_eq!(
        trace["items"][0]["content_sha256"],
        sha256_hex(b"compatibility beacon lost reply")
    );
    let connection = rusqlite::Connection::open(port.staged.path()).unwrap();
    let before: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM tdmem_native_operation_v2",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let dry=provider.invoke(&common_call(&provider,&scope,ProviderOperation::Maintenance,"dry",
        json!({"task":"validate_state","dry_run":true,"maximum_items":10,"maximum_bytes":65536,"maximum_duration_millis":1000,"resume_cursor":null})));
    assert_eq!(
        dry.terminal.committed_effect().state(),
        CommittedEffectState::None
    );
    assert_eq!(body(&dry)["scanned_items"], 1);
    assert_eq!(provider.descriptor().state_generation, generation);
    let after: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM tdmem_native_operation_v2",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(before, after);
    let cancelled = common_call(
        &provider,
        &scope,
        ProviderOperation::DeleteBySource,
        "cancel-before",
        json!({"forget_source_keys":["unrelated"],"mode":"hard_delete","include_snapshots":true,"retention_lock_policy_revision":1,"verification_query":"unrelated"}),
    );
    cancelled.control.cancellation().cancel();
    assert_eq!(
        provider.invoke(&cancelled).terminal.terminal_code(),
        TerminalCode::Cancelled
    );
    assert_eq!(provider.descriptor().state_generation, generation);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn common_advisory_replay_stages_real_rows_and_uses_fresh_dispositions() {
    let (_temp, root, graph, _owner, project) = real_project_fixture().await;
    let scope = recall_exact_scope(project.as_str());
    let first = common_observation(
        &scope,
        1,
        Some("r1"),
        "compatibility beacon first replay",
        Some(T1),
        None,
    );
    let second = common_observation(
        &scope,
        2,
        Some("r2"),
        "compatibility beacon second replay",
        Some(T1),
        None,
    );
    let sources = vec![first.clone(), second.clone()];
    let port = new_authorized_port(
        &graph,
        &root,
        &scope,
        &sources,
        tracedecay_memory_provider_registry::SourceDisposition::Available,
    );
    let provider = NativeProvider::new(port.clone()).unwrap();
    let first_call = common_call(
        &provider,
        &scope,
        ProviderOperation::Observe,
        "replay-first",
        first,
    );
    body(&provider.invoke(&first_call));
    let second_call = common_call(
        &provider,
        &scope,
        ProviderOperation::Observe,
        "replay-second",
        second,
    );
    let observations = vec![
        serde_json::from_slice::<Value>(&first_call.payload.bytes).unwrap(),
        serde_json::from_slice::<Value>(&second_call.payload.bytes).unwrap(),
    ];
    let replay = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Replay,
        "replay-a",
        fixture_replay(&scope, &observations, 0),
    )));
    assert_eq!(replay["applied_observations"], 1);
    assert_eq!(replay["duplicate_observations"], 1);
    assert_eq!(replay["acknowledged_sequence"], 2);
    assert_eq!(
        recall_common(
            &provider,
            &scope,
            "replay-recall",
            common_query("current", T4)
        )["candidates"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let mut fresh = observations.clone();
    fresh[0]["idempotency_key"] = sha256_hex(b"fresh.replayed.delivery").into();
    let again = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Replay,
        "replay-b",
        fixture_replay(&scope, &fresh, 2),
    )));
    assert_eq!(again["applied_observations"], 0);
    assert_eq!(again["duplicate_observations"], 1);
    assert_eq!(again["sources_already_applied"], 1);
    drop(provider);
    drop(port);
    let deleted_port = new_authorized_port(
        &graph,
        &root,
        &scope,
        &sources,
        tracedecay_memory_provider_registry::SourceDisposition::Deleted,
    );
    let deleted_provider = NativeProvider::new(deleted_port).unwrap();
    let mut query = common_query("history", T4);
    query["history_grant"] = fixture_history(&scope, &observations);
    query["temporal_query"]["include_revoked"] = true.into();
    query["temporal_query"]["include_superseded"] = true.into();
    assert!(
        recall_common(&deleted_provider, &scope, "fresh-deletion", query)["candidates"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let refused = body(&deleted_provider.invoke(&common_call(
        &deleted_provider,
        &scope,
        ProviderOperation::Replay,
        "replay-deleted",
        fixture_replay(&scope, &observations, 2),
    )));
    assert_eq!(refused["applied_observations"], 0);
    assert_eq!(refused["rejected_observations"], 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn common_advisory_snapshot_fresh_namespace_preserves_feedback_receipts_and_empty_source_fences()
 {
    let (_temp, root, graph, _owner, project) = real_project_fixture().await;
    let scope = recall_exact_scope(project.as_str());
    let original = common_observation(
        &scope,
        1,
        Some("r1"),
        "compatibility beacon portable snapshot",
        Some(T1),
        None,
    );
    let port = new_port(&graph, &root);
    let provider = NativeProvider::new(port.clone()).unwrap();
    body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Observe,
        "portable-source",
        original.clone(),
    )));
    let recalled = recall_common(
        &provider,
        &scope,
        "portable-recall",
        common_query("current", T4),
    );
    let reference = recalled["candidates"][0]["stable_memory_ref"]
        .as_str()
        .unwrap();
    let feedback = json!({"target":lifecycle_target(&scope,&original,reference),"signal":"helpful","weight":"0.5","occurred_at":T4,"canonical_outcome_receipt":"host.outcome.portable"});
    let settled = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Feedback,
        "portable-feedback",
        feedback.clone(),
    )));
    body(&provider.invoke(&common_call(&provider,&scope,ProviderOperation::DeleteBySource,"portable-empty-fence",
        json!({"forget_source_keys":["never-existed"],"mode":"hard_delete","include_snapshots":true,"retention_lock_policy_revision":1,"verification_query":"never-existed"}))));
    let exported = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::SnapshotExport,
        "portable-export",
        json!({}),
    )));
    assert_eq!(exported["snapshot"]["identity"]["observation_sequence"], 1);
    assert!(
        exported["snapshot"]["identity"]["state_generation"]
            .as_u64()
            .unwrap()
            > 1
    );
    assert_eq!(
        exported["snapshot"]["identity"]["implementation_identity_digest"],
        provider.descriptor().implementation_identity_sha256
    );
    let fresh_state = root.join("portable-provider-state");
    let fresh_port = Arc::new(
        ProjectNativeMemoryApplicationPort::new(
            Arc::new(tokio::sync::RwLock::new(Arc::clone(&graph))),
            root.clone(),
            test_profile_id(),
            &fresh_state,
        )
        .unwrap()
        .with_admission_authority(Arc::new(FixtureAuthority {
            scope: scope.clone(),
            sources: vec![fixture_source(&original)],
            state: tracedecay_memory_provider_registry::SourceDisposition::Available,
        })),
    );
    let fresh = NativeProvider::new(fresh_port.clone()).unwrap();
    assert_eq!(fresh.descriptor().state_generation, 0);
    let restore = json!({"snapshot":exported["snapshot"],"disposition_checkpoint":{"exact_scope":scope_value(&scope),"authority_ref":"fixture.old.checkpoint","authority_revision":1,"checked_at":T1},
        "source_dispositions":[{"source":original["source_identity"]["original_source"]["source"],"current_disposition":{"state":"available","authority_ref":"fixture.old.source","authority_revision":1,"checked_at":T1}}]});
    let restored = body(&fresh.invoke(&common_call(
        &fresh,
        &scope,
        ProviderOperation::SnapshotRestore,
        "portable-restore",
        restore,
    )));
    assert_eq!(restored["restored_rows"], 1);
    let current = recall_common(
        &fresh,
        &scope,
        "portable-fresh-recall",
        common_query("current", T4),
    );
    assert_eq!(current["candidates"][0]["stable_memory_ref"], reference);
    assert_eq!(
        current["candidates"][0]["provenance"]["original_sources"][0],
        original["source_identity"]["original_source"]
    );
    let source_key = original["source_identity"]["original_source"]["source"]["source_key"].clone();
    let influence = body(&fresh.invoke(&common_call(
        &fresh,
        &scope,
        ProviderOperation::Inspection,
        "portable-influence",
        inspection("source_influence", json!({"source_key":source_key})),
    )));
    assert_eq!(influence["items"][0]["settled_feedback"]["helpful"], 1);
    assert_eq!(
        influence["items"][0]["last_feedback_receipt"],
        settled["provider_receipt_digest"]
    );
    let effect_summary = influence["items"][0]["provider_local_effect_summary"]
        .as_str()
        .expect("source influence summary is a string");
    assert!(!effect_summary.is_empty() && effect_summary.len() <= 8192);
    let effect: Value = serde_json::from_str(effect_summary).expect("complete effect details");
    assert_eq!(effect["ranking_bias"], 0.5);
    assert_eq!(effect["feedback_suppressed"], false);
    assert_eq!(
        effect["validity"],
        original["source_identity"]["original_source"]["validity"]
    );
    let retry = fresh.invoke(&common_call(
        &fresh,
        &scope,
        ProviderOperation::Feedback,
        "portable-feedback",
        feedback,
    ));
    assert_eq!(
        retry.terminal.committed_effect().state(),
        CommittedEffectState::Duplicate
    );
    let mut forbidden = common_observation(
        &scope,
        2,
        Some("r1"),
        "compatibility beacon forbidden resurrection",
        Some(T1),
        None,
    );
    forbidden["source_identity"]["original_source"]["source"]["source_key"] =
        "never-existed".into();
    assert_eq!(
        fresh
            .invoke(&common_call(
                &fresh,
                &scope,
                ProviderOperation::Observe,
                "portable-forbidden",
                forbidden
            ))
            .terminal
            .terminal_code(),
        TerminalCode::Conflict
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn common_advisory_restore_privacy_fence_scrubs_existing_other_session_copy() {
    let (_temp, root, graph, _owner, project) = real_project_fixture().await;
    let scope_a = recall_exact_scope(project.as_str());
    let mut scope_b = scope_a.clone();
    scope_b.agent_session_id = "session.restore-other".into();
    let source = common_observation(
        &scope_a,
        1,
        Some("r1"),
        "compatibility beacon cross session deleted",
        Some(T1),
        None,
    );
    let port_a = new_port(&graph, &root);
    let provider_a = NativeProvider::new(port_a.clone()).unwrap();
    body(&provider_a.invoke(&common_call(
        &provider_a,
        &scope_a,
        ProviderOperation::Observe,
        "cross-a",
        source.clone(),
    )));
    let snapshot = body(&provider_a.invoke(&common_call(
        &provider_a,
        &scope_a,
        ProviderOperation::SnapshotExport,
        "cross-export",
        json!({}),
    )));
    let port_b = new_authorized_port(
        &graph,
        &root,
        &scope_b,
        std::slice::from_ref(&source),
        tracedecay_memory_provider_registry::SourceDisposition::Available,
    );
    let provider_b = NativeProvider::new(port_b.clone()).unwrap();
    body(&provider_b.invoke(&common_call(
        &provider_b,
        &scope_b,
        ProviderOperation::Observe,
        "cross-b",
        source.clone(),
    )));
    let unrelated = common_observation(
        &scope_b,
        2,
        Some("r2"),
        "compatibility beacon unrelated survives",
        Some(T1),
        None,
    );
    body(&provider_b.invoke(&common_call(
        &provider_b,
        &scope_b,
        ProviderOperation::Observe,
        "cross-unrelated",
        unrelated.clone(),
    )));
    assert_eq!(
        recall_common(
            &provider_b,
            &scope_b,
            "cross-before",
            common_query("current", T4)
        )["candidates"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    drop(provider_a);
    drop(port_a);
    drop(provider_b);
    drop(port_b);
    let restored_port = new_authorized_port(
        &graph,
        &root,
        &scope_a,
        std::slice::from_ref(&source),
        tracedecay_memory_provider_registry::SourceDisposition::Deleted,
    );
    let restored = NativeProvider::new(restored_port).unwrap();
    let request = json!({"snapshot":snapshot["snapshot"],"disposition_checkpoint":{"exact_scope":scope_value(&scope_a),"authority_ref":"fixture.old.checkpoint","authority_revision":1,"checked_at":T1},
        "source_dispositions":[{"source":source["source_identity"]["original_source"]["source"],"current_disposition":{"state":"available","authority_ref":"fixture.old.source","authority_revision":1,"checked_at":T1}}]});
    body(&restored.invoke(&common_call(
        &restored,
        &scope_a,
        ProviderOperation::SnapshotRestore,
        "cross-restore",
        request,
    )));
    let reader = NativeProvider::new(new_port(&graph, &root)).unwrap();
    for mode in ["current", "as_of", "interval", "history"] {
        let mut query = common_query(mode, T1);
        query["temporal_query"]["include_revoked"] = true.into();
        query["temporal_query"]["include_superseded"] = true.into();
        let hits = recall_common(&reader, &scope_b, &format!("cross-after-{mode}"), query);
        assert_eq!(hits["candidates"].as_array().unwrap().len(), 1);
        assert_eq!(
            hits["candidates"][0]["content"],
            "compatibility beacon unrelated survives"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn common_advisory_restores_revocation_overlay_without_rewriting_original_source() {
    let (_temp, root, graph, _owner, project) = real_project_fixture().await;
    let scope = recall_exact_scope(project.as_str());
    let source = common_observation(
        &scope,
        1,
        Some("r1"),
        "compatibility beacon corrected history",
        Some(T1),
        None,
    );
    let port = new_port(&graph, &root);
    let provider = NativeProvider::new(port.clone()).unwrap();
    body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Observe,
        "revoked-source",
        source.clone(),
    )));
    let original = recall_common(
        &provider,
        &scope,
        "revoked-target",
        common_query("current", T4),
    );
    let reference = original["candidates"][0]["stable_memory_ref"]
        .as_str()
        .unwrap();
    body(&provider.invoke(&common_call(&provider,&scope,ProviderOperation::Correction,"revoked-correction",
        json!({"target":lifecycle_target(&scope,&source,reference),"expected_target_revision":"r1","correction_kind":"mark_incorrect","reason":"settled correction","replacement":{"revoked_at":T2}}))));
    let snapshot = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::SnapshotExport,
        "revoked-export",
        json!({}),
    )));
    drop(provider);
    drop(port);
    let fresh_root = root.join("revoked-fresh-provider-state");
    let port = Arc::new(
        ProjectNativeMemoryApplicationPort::new(
            Arc::new(tokio::sync::RwLock::new(Arc::clone(&graph))),
            root.clone(),
            test_profile_id(),
            &fresh_root,
        )
        .unwrap()
        .with_admission_authority(Arc::new(FixtureAuthority {
            scope: scope.clone(),
            sources: vec![fixture_source(&source)],
            state: tracedecay_memory_provider_registry::SourceDisposition::Revoked,
        })),
    );
    let provider = NativeProvider::new(port).unwrap();
    let request = json!({"snapshot":snapshot["snapshot"],"disposition_checkpoint":{"exact_scope":scope_value(&scope),"authority_ref":"fixture.old.checkpoint","authority_revision":1,"checked_at":T1},
        "source_dispositions":[{"source":source["source_identity"]["original_source"]["source"],"current_disposition":{"state":"available","authority_ref":"fixture.old.source","authority_revision":1,"checked_at":T1}}]});
    body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::SnapshotRestore,
        "revoked-restore",
        request,
    )));
    let before = recall_common(&provider, &scope, "revoked-asof", common_query("as_of", T1));
    assert_eq!(before["candidates"].as_array().unwrap().len(), 1);
    assert_eq!(
        before["candidates"][0]["provenance"]["original_sources"][0],
        source["source_identity"]["original_source"]
    );
    assert_eq!(before["candidates"][0]["validity"]["revoked_at"], T2);
    assert!(
        recall_common(
            &provider,
            &scope,
            "revoked-current",
            common_query("current", T4)
        )["candidates"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let mut include = common_query("current", T4);
    include["temporal_query"]["include_revoked"] = true.into();
    assert_eq!(
        recall_common(&provider, &scope, "revoked-included", include)["candidates"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    body(&provider.invoke(&common_call(&provider,&scope,ProviderOperation::DeleteBySource,"revoked-delete",
        json!({"forget_source_keys":[source["source_identity"]["original_source"]["source"]["source_key"]],"mode":"hard_delete","include_snapshots":true,"retention_lock_policy_revision":1,"verification_query":"corrected history"}))));
    let mut include = common_query("history", T4);
    include["temporal_query"]["include_revoked"] = true.into();
    assert!(
        recall_common(&provider, &scope, "revoked-privacy-wins", include)["candidates"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn common_advisory_maintenance_changes_real_bias_and_prunes_expired_content() {
    let (_temp, root, graph, owner, project) = real_project_fixture().await;
    let canonical = add_real_project_fact(
        &graph,
        "compatibility beacon canonical maintenance authority",
        "maintenance-authority",
    )
    .await;
    let authority_before = read_store_snapshot(&graph, &owner, &canonical.fact_id).await;
    let scope = recall_exact_scope(project.as_str());
    let port = new_port(&graph, &root);
    let provider = NativeProvider::new(port.clone()).unwrap();
    let expired = common_observation(
        &scope,
        1,
        Some("r1"),
        "compatibility beacon expired retained history",
        Some(T1),
        Some(T2),
    );
    let expired_call = common_call(
        &provider,
        &scope,
        ProviderOperation::Observe,
        "maintenance-expired",
        expired,
    );
    body(&provider.invoke(&expired_call));
    let active = common_observation(
        &scope,
        2,
        Some("r2"),
        "compatibility beacon active maintained",
        Some(T1),
        None,
    );
    body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Observe,
        "maintenance-active",
        active.clone(),
    )));
    let current = recall_common(
        &provider,
        &scope,
        "maintenance-target",
        common_query("current", T4),
    );
    let reference = current["candidates"][0]["stable_memory_ref"]
        .as_str()
        .unwrap();
    body(&provider.invoke(&common_call(&provider,&scope,ProviderOperation::Feedback,"maintenance-helpful",
        json!({"target":lifecycle_target(&scope,&active,reference),"signal":"helpful","weight":"0.5","occurred_at":T4,"canonical_outcome_receipt":"host.outcome.maintenance"}))));
    let request = |task| json!({"task":task,"dry_run":false,"maximum_items":64,"maximum_bytes":65536,"maximum_duration_millis":1000,"resume_cursor":null});
    let decay = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Maintenance,
        "maintenance-decay",
        request("decay"),
    )));
    assert_eq!(decay["scanned_items"], 2);
    assert_eq!(decay["changed_items"], 1);
    let influence=body(&provider.invoke(&common_call(&provider,&scope,ProviderOperation::Inspection,"maintenance-influence",
        inspection("source_influence",json!({"source_key":active["source_identity"]["original_source"]["source"]["source_key"]})))));
    let effect_summary = influence["items"][0]["provider_local_effect_summary"]
        .as_str()
        .expect("source influence summary is a string");
    assert!(!effect_summary.is_empty() && effect_summary.len() <= 8192);
    let effect: Value = serde_json::from_str(effect_summary).expect("complete effect details");
    assert_eq!(effect["ranking_bias"], 0.45);
    assert_eq!(effect["feedback_suppressed"], false);
    assert_eq!(
        effect["validity"],
        active["source_identity"]["original_source"]["validity"]
    );
    assert_eq!(
        recall_common(
            &provider,
            &scope,
            "maintenance-before-prune",
            common_query("history", T4)
        )["candidates"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let prune = body(&provider.invoke(&common_call(
        &provider,
        &scope,
        ProviderOperation::Maintenance,
        "maintenance-prune",
        request("prune_expired"),
    )));
    assert_eq!(prune["removed_items"], 1);
    let history = recall_common(
        &provider,
        &scope,
        "maintenance-after-prune",
        common_query("history", T4),
    );
    assert_eq!(history["candidates"].as_array().unwrap().len(), 1);
    assert_eq!(
        history["candidates"][0]["content"],
        "compatibility beacon active maintained"
    );
    assert_eq!(
        provider
            .invoke(&expired_call)
            .terminal
            .committed_effect()
            .state(),
        CommittedEffectState::Duplicate
    );
    assert_eq!(
        read_store_snapshot(&graph, &owner, &canonical.fact_id).await,
        authority_before
    );
}
