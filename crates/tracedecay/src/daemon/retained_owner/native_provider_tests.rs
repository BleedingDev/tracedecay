#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde_json::{Value, json};
use tracedecay_application::retained_surfaces::{
    FactCommitDispositionV1, FactCommitOwnerV1, FactCommitReceiptV1, FactIdentitySourceResultV1,
    FactProjectionV1, FactTelemetryV1, FactV1,
};
use tracedecay_domain::canonical_text::sha256_hex;
use tracedecay_domain::{
    Confidence, FactAssertionId, FactCategoryV1, FactId, FactIdentityMaterialV1,
    FactIdentitySourceV1, FactLineageEventKindV1, FactLineageEventV1, FactOwnerV1, ProjectId,
    ProvenanceId, UtcMicros,
};
use tracedecay_memory_provider_registry::{
    CancellationToken, CanonicalPayload, CommittedEffectState, HandshakeRequest,
    NATIVE_FACT_PROMOTION_OBSERVATION_KIND, NATIVE_FACT_PROMOTION_PAYLOAD_CONTRACT_ID,
    NATIVE_PROVIDER_ID, NativeMemoryApplicationPort, NativeObservation, OBSERVATION_CONTRACT_ID,
    OperationControl, OwnedExactScope, OwnedProviderId, OwnedVersionedId, ProviderCall,
    ProviderCallParts, ProviderOperation, TerminalCode,
};
use tracedecay_store::{
    FactReadControl, FactWriteControl, ProjectMemoryFactHistoryQueryV1, ProjectMemoryFactHistoryV1,
    ProjectMemoryFactIdV1,
};
use tracedecay_usecases::memory::{
    ProjectMemoryFactAddRequest, ProjectMemoryFactAddRequestOutcome,
};

use super::*;
use crate::tracedecay::{TraceDecay, TraceDecayOpenOptions};

fn project_parts(label: &str) -> (ProjectId, FactOwnerV1, FactCommitOwnerV1) {
    let project_id =
        ProjectId::new(format!("project.native-bridge-{label}")).expect("valid project identity");
    (
        project_id.clone(),
        FactOwnerV1::Project {
            project_id: project_id.clone(),
        },
        FactCommitOwnerV1::Project { project_id },
    )
}

fn source_and_fact_id(owner: &FactOwnerV1, label: &str) -> (FactIdentitySourceV1, FactId) {
    let source = FactIdentitySourceV1::Application {
        operation_id: ProvenanceId::new(format!("operation.native-bridge-{label}"))
            .expect("valid operation identity"),
    };
    let material = FactIdentityMaterialV1::new(owner.clone(), source.clone())
        .expect("valid fact identity material");
    let fact_id = FactId::derive(&material).expect("valid fact identity");
    (source, fact_id)
}

fn event_for(
    owner: &FactOwnerV1,
    fact_id: &FactId,
    assertion_id: &FactAssertionId,
    occurred_at: i64,
) -> FactLineageEventV1 {
    FactLineageEventV1::new(
        fact_id.clone(),
        owner.clone(),
        FactLineageEventKindV1::AssertionRecorded {
            assertion_id: assertion_id.clone(),
        },
        UtcMicros(occurred_at),
        None,
    )
    .expect("valid fact lineage event")
}

fn fact_for(
    public_owner: &FactCommitOwnerV1,
    fact_id: &FactId,
    source: &FactIdentitySourceV1,
    assertion_id: &FactAssertionId,
    last_event: &FactLineageEventV1,
    as_of: i64,
) -> FactV1 {
    let operation_id = match source {
        FactIdentitySourceV1::Application { operation_id } => operation_id.clone(),
        FactIdentitySourceV1::Evidence { .. } => unreachable!("test source is application-owned"),
    };
    FactV1 {
        owner: public_owner.clone(),
        fact_id: fact_id.clone(),
        content: "Native bridge acceptance fact".to_owned(),
        category: FactCategoryV1::Project,
        tags: vec!["native".to_owned(), "bridge".to_owned()],
        entities: vec!["TraceDecay".to_owned()],
        trust_score_millionths: 876_543,
        source: FactIdentitySourceResultV1::Application { operation_id },
        source_label: Some("native bridge test".to_owned()),
        active_assertion_id: assertion_id.clone(),
        last_event_id: last_event.event_id().clone(),
        projected_as_of: UtcMicros(as_of),
        telemetry: FactTelemetryV1 {
            retrieval_count: 7,
            access_count: 5,
            helpful_count: 3,
            unhelpful_count: 1,
            created_at: UtcMicros(1),
            updated_at: UtcMicros(as_of),
            last_retrieved_at: Some(UtcMicros(4)),
            last_recalled_at: Some(UtcMicros(5)),
            last_feedback_at: Some(UtcMicros(6)),
        },
        metadata: BTreeMap::from([("origin".to_owned(), json!("native"))]),
    }
}

fn commit_for(
    public_owner: &FactCommitOwnerV1,
    fact_id: &FactId,
    events: &[FactLineageEventV1],
    assertion_id: &FactAssertionId,
) -> FactCommitReceiptV1 {
    FactCommitReceiptV1 {
        disposition: FactCommitDispositionV1::Committed,
        fact_id: fact_id.clone(),
        owner: public_owner.clone(),
        committed_event_ids: events
            .iter()
            .map(|event| event.event_id().clone())
            .collect(),
        last_event_id: events
            .last()
            .expect("receipt has a final event")
            .event_id()
            .clone(),
        active_assertion_id: Some(assertion_id.clone()),
    }
}

fn call_for(project_id: &str) -> ProviderCall {
    ProviderCall {
        operation: ProviderOperation::Observe,
        provider_id: OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("valid provider id"),
        registration_revision: 1,
        ready_receipt_sha256: "a".repeat(64),
        exact_scope: OwnedExactScope::new(
            "profile.native-bridge-test",
            project_id,
            "repo.native-bridge-test",
            "worktree.native-bridge-test",
            "branch.native-bridge-test",
            "agent.native-bridge-test",
            1,
        )
        .expect("valid exact scope"),
        request_id: "request.native-bridge-test".to_owned(),
        operation_id: "operation.native-bridge-test".to_owned(),
        expected_state_generation: 0,
        idempotency_key: Some("idempotency.native-bridge-test".to_owned()),
        control: OperationControl::new(i64::MAX, 1_000, CancellationToken::new()),
        payload: CanonicalPayload {
            contract_id: OwnedVersionedId::new(OBSERVATION_CONTRACT_ID)
                .expect("valid observation contract"),
            bytes: vec![1],
            sha256: "b".repeat(64),
        },
        required_capabilities: BTreeSet::new(),
        extensions: Vec::new(),
    }
}

fn valid_observation_call(project_id: &str, canonical_payload: &Value) -> ProviderCall {
    let envelope = json!({
        "canonical_payload": canonical_payload,
        "observation_kind": NATIVE_FACT_PROMOTION_OBSERVATION_KIND,
        "payload_contract": NATIVE_FACT_PROMOTION_PAYLOAD_CONTRACT_ID,
    });
    let envelope_bytes = serde_json::to_vec(&envelope).expect("observation envelope bytes");
    ProviderCall::new(ProviderCallParts {
        operation: ProviderOperation::Observe,
        provider_id: OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("valid provider id"),
        registration_revision: 1,
        ready_receipt_sha256: "a".repeat(64),
        exact_scope: OwnedExactScope::new(
            "profile.native-bridge-store",
            project_id,
            "repo.native-bridge-store",
            "worktree.native-bridge-store",
            "branch.native-bridge-store",
            "agent.native-bridge-store",
            1,
        )
        .expect("valid exact scope"),
        request_id: "request.native-bridge-store".to_owned(),
        operation_id: "operation.native-bridge-store".to_owned(),
        expected_state_generation: 0,
        idempotency_key: Some("idempotency.native-bridge-store".to_owned()),
        control: OperationControl::new(i64::MAX, 1_000, CancellationToken::new()),
        payload: CanonicalPayload::new(
            OwnedVersionedId::new(OBSERVATION_CONTRACT_ID).expect("valid observation contract"),
            envelope_bytes.clone(),
            sha256_hex(&envelope_bytes),
        )
        .expect("valid observation payload"),
        required_capabilities: vec![
            OwnedVersionedId::new(ProviderOperation::Observe.capability_id())
                .expect("observe capability"),
        ],
        extensions: Vec::new(),
    })
    .expect("valid provider call")
}

fn settled_fixture() -> (ProviderCall, SettledNativeFactWriteV1) {
    let (project_id, owner, public_owner) = project_parts("settled");
    let (source, fact_id) = source_and_fact_id(&owner, "settled");
    let assertion_id =
        FactAssertionId::new("assertion.native-bridge-settled").expect("valid assertion identity");
    let event = event_for(&owner, &fact_id, &assertion_id, 2);
    let fact = fact_for(&public_owner, &fact_id, &source, &assertion_id, &event, 2);
    let commit = commit_for(
        &public_owner,
        &fact_id,
        std::slice::from_ref(&event),
        &assertion_id,
    );
    (
        call_for(project_id.as_str()),
        SettledNativeFactWriteV1 {
            kind: "settled_native_fact_write".to_owned(),
            fact,
            commit,
        },
    )
}

fn ready_request() -> HandshakeRequest {
    HandshakeRequest {
        provider_id: OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("valid provider id"),
        registration_revision: 7,
        exact_scope: OwnedExactScope::new(
            "profile.native-bridge-ready",
            "project.native-bridge-ready",
            "repo.native-bridge-ready",
            "worktree.native-bridge-ready",
            "branch.native-bridge-ready",
            "agent.native-bridge-ready",
            2,
        )
        .expect("valid exact scope"),
        request_id: "request.native-bridge-ready".to_owned(),
        required_capabilities: BTreeSet::new(),
        host_limits: native_descriptor().expect("descriptor").limits,
        control: OperationControl::new(i64::MAX, 1_000, CancellationToken::new()),
        challenge_nonce: [7; 32],
    }
}

fn history_fixture() -> (
    FactOwnerV1,
    FactV1,
    FactCommitReceiptV1,
    ProjectMemoryFactHistoryV1,
) {
    let (_project_id, owner, public_owner) = project_parts("history");
    let (source, fact_id) = source_and_fact_id(&owner, "history");
    let prefix_assertion =
        FactAssertionId::new("assertion.native-bridge-prefix").expect("valid assertion");
    let first_assertion =
        FactAssertionId::new("assertion.native-bridge-first").expect("valid assertion");
    let last_assertion =
        FactAssertionId::new("assertion.native-bridge-last").expect("valid assertion");
    let prefix = event_for(&owner, &fact_id, &prefix_assertion, 1);
    let first = event_for(&owner, &fact_id, &first_assertion, 2);
    let last = event_for(&owner, &fact_id, &last_assertion, 3);
    let fact = fact_for(&public_owner, &fact_id, &source, &last_assertion, &last, 3);
    let commit = commit_for(
        &public_owner,
        &fact_id,
        &[first.clone(), last.clone()],
        &last_assertion,
    );
    let history =
        ProjectMemoryFactHistoryV1::new(owner.clone(), fact_id, vec![prefix, first, last], None)
            .expect("valid project fact history");
    (owner, fact, commit, history)
}

async fn read_store_snapshot(
    graph: &TraceDecay,
    owner: &FactOwnerV1,
    fact_id: &FactId,
) -> (FactProjectionV1, ProjectMemoryFactHistoryV1) {
    let memory = graph
        .project_memory_application()
        .await
        .expect("project memory application");
    let target = ProjectMemoryFactIdV1::new(owner.clone(), fact_id.clone())
        .expect("owner-bound project fact target");
    let read_control = FactReadControl::new(Arc::new(|| false));
    let projection = memory
        .get_project_memory_fact(target.clone(), &read_control)
        .await
        .expect("read project fact")
        .expect("project fact exists");
    let public = super::memory_mapping::projection(&projection).expect("public fact projection");
    let history = memory
        .get_project_memory_history(
            ProjectMemoryFactHistoryQueryV1::new(target, None, MAX_NATIVE_FACT_LINEAGE)
                .expect("fact history query"),
            &read_control,
        )
        .await
        .expect("read project fact history");
    (public, history)
}

fn observation_for(call: &ProviderCall, canonical_payload: Value) -> NativeObservation<'_> {
    NativeObservation {
        call,
        observation_kind: NATIVE_FACT_PROMOTION_OBSERVATION_KIND.to_owned(),
        payload_contract: NATIVE_FACT_PROMOTION_PAYLOAD_CONTRACT_ID.to_owned(),
        canonical_payload,
    }
}

#[test]
fn ready_receipt_is_deterministic_and_nonce_bound() {
    let request = ready_request();
    let limits = native_descriptor().expect("descriptor").limits;
    let first = ready_receipt(&request, limits);
    assert_eq!(first, ready_receipt(&request, limits));

    let mut changed = request;
    changed.challenge_nonce[0] ^= 0xff;
    assert_ne!(first, ready_receipt(&changed, limits));
}

#[test]
fn settled_fact_validation_accepts_exact_projection() {
    let (call, payload) = settled_fixture();
    assert!(validate_settled_native_fact(&call, &payload).is_ok());
}

#[test]
fn settled_fact_validation_rejects_owner_fact_active_last_and_telemetry_mismatch() {
    let (call, mut owner_mismatch) = settled_fixture();
    owner_mismatch.commit.owner = FactCommitOwnerV1::Project {
        project_id: ProjectId::new("project.native-bridge-foreign").expect("valid project"),
    };
    assert!(matches!(
        validate_settled_native_fact(&call, &owner_mismatch),
        Err(NativeReadFailure::ScopeUnavailable)
    ));

    let (call, mut fact_mismatch) = settled_fixture();
    let (_project_id, owner, _public_owner) = project_parts("settled");
    let (_source, other_fact_id) = source_and_fact_id(&owner, "other-fact");
    fact_mismatch.commit.fact_id = other_fact_id;
    assert!(matches!(
        validate_settled_native_fact(&call, &fact_mismatch),
        Err(NativeReadFailure::PromotionMismatch)
    ));

    let (call, mut active_mismatch) = settled_fixture();
    active_mismatch.commit.active_assertion_id = Some(
        FactAssertionId::new("assertion.native-bridge-other").expect("valid assertion identity"),
    );
    assert!(matches!(
        validate_settled_native_fact(&call, &active_mismatch),
        Err(NativeReadFailure::PromotionMismatch)
    ));

    let (call, mut last_event_mismatch) = settled_fixture();
    let (_project_id, owner, _public_owner) = project_parts("settled");
    let fact_id = last_event_mismatch.fact.fact_id.clone();
    let other_assertion =
        FactAssertionId::new("assertion.native-bridge-other-event").expect("valid assertion");
    let other_event = event_for(&owner, &fact_id, &other_assertion, 3);
    last_event_mismatch.commit.last_event_id = other_event.event_id().clone();
    assert!(matches!(
        validate_settled_native_fact(&call, &last_event_mismatch),
        Err(NativeReadFailure::PromotionMismatch)
    ));

    let (call, mut telemetry_mismatch) = settled_fixture();
    telemetry_mismatch.fact.telemetry.updated_at = UtcMicros(3);
    assert!(matches!(
        validate_settled_native_fact(&call, &telemetry_mismatch),
        Err(NativeReadFailure::PromotionMismatch)
    ));
}

#[test]
fn receipt_matches_exact_authoritative_history_suffix() {
    let (owner, fact, commit, history) = history_fixture();
    assert!(receipt_matches_authoritative_history(
        &history, &owner, &fact, &commit
    ));
}

#[test]
fn receipt_rejects_reordered_truncated_and_foreign_history_claims() {
    let (owner, fact, commit, history) = history_fixture();

    let mut reordered = commit.clone();
    reordered.committed_event_ids.swap(0, 1);
    assert!(!receipt_matches_authoritative_history(
        &history, &owner, &fact, &reordered
    ));

    let mut truncated = commit.clone();
    truncated.committed_event_ids.pop();
    assert!(!receipt_matches_authoritative_history(
        &history, &owner, &fact, &truncated
    ));

    let mut foreign = commit;
    foreign.owner = FactCommitOwnerV1::Project {
        project_id: ProjectId::new("project.native-bridge-foreign").expect("valid project"),
    };
    assert!(!receipt_matches_authoritative_history(
        &history, &owner, &fact, &foreign
    ));
}

#[test]
fn control_preflight_reports_cancellation_and_deadline_without_contact() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(matches!(
        control_failure(&OperationControl::new(i64::MAX, 1_000, cancellation)),
        Err(NativeReadFailure::Cancelled)
    ));
    assert!(matches!(
        control_failure(&OperationControl::new(0, 1_000, CancellationToken::new())),
        Err(NativeReadFailure::DeadlineExceeded)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_observe_verifies_real_store_without_writing() {
    let temporary = tempfile::tempdir().expect("native bridge fixture root");
    let project_root = temporary.path().join("project");
    let profile_root = temporary.path().join("profile");
    std::fs::create_dir_all(&project_root).expect("project root");
    std::fs::create_dir_all(&profile_root).expect("profile root");
    let graph = Arc::new(
        TraceDecay::init_with_options(
            &project_root,
            TraceDecayOpenOptions {
                global_db_path: Some(profile_root.join("global.db")),
                profile_root: Some(profile_root),
            },
        )
        .await
        .expect("initialize TraceDecay project graph"),
    );
    let owner = graph.project_memory_owner().expect("project memory owner");
    let FactOwnerV1::Project { project_id } = owner.clone() else {
        panic!("project fixture must have a project memory owner");
    };
    let memory = graph
        .project_memory_application()
        .await
        .expect("project memory application");
    let preflight = memory
        .preflight_project_memory_fact_add(
            ProjectMemoryFactAddRequest {
                content: "Native bridge real-store verification fact".to_owned(),
                category: FactCategoryV1::Project,
                source_label: Some("native-bridge-real-store".to_owned()),
                tags: vec!["native".to_owned(), "bridge".to_owned()],
                entities: vec!["TraceDecay".to_owned()],
                trust: Some(Confidence::new(0.91).expect("fact trust")),
                metadata: json!({"fixture": "native-bridge-real-store"}),
            },
            None,
        )
        .expect("preflight project fact");
    let outcome = memory
        .add_preflighted_project_memory_fact(
            preflight,
            &FactWriteControl::new(Arc::new(|| false), Arc::new(|| true)),
        )
        .await
        .expect("commit project fact");
    let ProjectMemoryFactAddRequestOutcome::Applied(applied) = outcome else {
        panic!("real-store fixture fact must be applied");
    };
    let expected_public =
        super::memory_mapping::projection(applied.fact()).expect("map stored fact projection");
    let expected_fact = match &expected_public {
        FactProjectionV1::Available { fact } => fact.as_ref().clone(),
        FactProjectionV1::Unavailable { .. } => {
            panic!("real-store fixture fact must remain available")
        }
    };
    let expected_commit = super::memory_mapping::commit_receipt(
        applied
            .commit_receipt()
            .expect("real-store add commit receipt"),
        applied.commit_replayed(),
    );
    let fact_id = expected_fact.fact_id.clone();
    drop(applied);
    drop(memory);
    let (before_public, before_history) = read_store_snapshot(&graph, &owner, &fact_id).await;
    assert_eq!(before_public, expected_public);

    let canonical_payload = json!({
        "kind": "settled_native_fact_write",
        "fact": expected_fact.clone(),
        "commit": expected_commit.clone(),
    });
    let graph_cell = Arc::new(tokio::sync::RwLock::new(Arc::clone(&graph)));
    let port = ProjectNativeMemoryApplicationPort::new(graph_cell, project_root)
        .expect("construct project Native application port");
    let call = valid_observation_call(project_id.as_str(), &canonical_payload);
    let reply = port.observe(observation_for(&call, canonical_payload.clone()));
    assert_eq!(reply.terminal.terminal_code(), TerminalCode::Success);
    assert_eq!(reply.payload, Some(call.payload.clone()));
    assert_eq!(
        reply.terminal.committed_effect().state(),
        CommittedEffectState::None
    );
    assert_eq!(reply.state_generation, call.expected_state_generation);

    let (after_public, after_history) = read_store_snapshot(&graph, &owner, &fact_id).await;
    assert_eq!(after_public, expected_public);
    assert_eq!(after_history, before_history);

    let mut mismatched_payload = canonical_payload;
    let fact_object = mismatched_payload
        .get_mut("fact")
        .and_then(Value::as_object_mut)
        .expect("fact payload object");
    fact_object.insert(
        "trust_score_millionths".to_owned(),
        json!(expected_fact.trust_score_millionths ^ 1),
    );
    let mismatch_call = valid_observation_call(project_id.as_str(), &mismatched_payload);
    let mismatch_reply = port.observe(observation_for(&mismatch_call, mismatched_payload));
    assert_eq!(
        mismatch_reply.terminal.terminal_code(),
        TerminalCode::ContractViolation
    );
    assert_eq!(
        mismatch_reply.terminal.committed_effect().state(),
        CommittedEffectState::None
    );
    assert_eq!(
        mismatch_reply.state_generation,
        mismatch_call.expected_state_generation
    );
    assert_eq!(mismatch_reply.payload, None);
    let (final_public, final_history) = read_store_snapshot(&graph, &owner, &fact_id).await;
    assert_eq!(final_public, expected_public);
    assert_eq!(final_history, before_history);
}
