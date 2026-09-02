#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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
    OperationControl, OwnedExactScope, OwnedProviderId, OwnedVersionedId,
    PayloadSanitizationReceipt, PayloadSanitizationReceiptParts, ProviderCall, ProviderCallParts,
    ProviderOperation, ProviderReply, TerminalCode,
};
use tracedecay_store::{
    FactReadControl, FactWriteControl, ProjectMemoryFactHistoryQueryV1, ProjectMemoryFactHistoryV1,
    ProjectMemoryFactIdV1, ProjectMemoryFactSearchKindV1, ProjectMemoryFactSearchQuery,
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

/// Canonical tagged scope digest standing in for the authoritative
/// project-open resolved scope in fixtures that do not vary it.
const SCOPE_DIGEST: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";

/// A distinct canonical tagged scope digest used where a fixture needs a
/// resolved scope digest that differs from `SCOPE_DIGEST`.
const OTHER_SCOPE_DIGEST: &str =
    "sha256:2222222222222222222222222222222222222222222222222222222222222222";

/// A routing fixture, deliberately not a dispatchable call.
///
/// The settled-write path under test never inspects the payload or the
/// capability set, so this keeps the original opaque single-byte payload and
/// empty capability set. `ProviderCall` now carries a private sanitization
/// receipt, so the envelope is built through the constructor and the fixture's
/// unvalidated fields are restored afterwards.
fn call_for(project_id: &str) -> ProviderCall {
    let mut call = ProviderCall::new(ProviderCallParts {
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
            SCOPE_DIGEST,
        )
        .expect("valid exact scope"),
        request_id: "request.native-bridge-test".to_owned(),
        operation_id: "operation.native-bridge-test".to_owned(),
        expected_state_generation: 0,
        idempotency_key: Some("idempotency.native-bridge-test".to_owned()),
        control: OperationControl::new(i64::MAX, 1_000, CancellationToken::new()),
        payload: CanonicalPayload::new(
            OwnedVersionedId::new(OBSERVATION_CONTRACT_ID).expect("valid observation contract"),
            vec![1],
            sha256_hex(&[1]),
        )
        .expect("valid fixture payload"),
        required_capabilities: vec![
            OwnedVersionedId::new(ProviderOperation::Observe.capability_id())
                .expect("observe capability"),
        ],
        extensions: Vec::new(),
    })
    .expect("valid fixture envelope");
    call.payload.sha256 = "b".repeat(64);
    call.required_capabilities = BTreeSet::new();
    call
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
            SCOPE_DIGEST,
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
    .map(admitted)
    .expect("valid provider call")
}

/// Sanitizer revision this harness stands in for. The real revision is derived
/// by `tracedecay-memory-hygiene` from the canonical policy document.
const TEST_SANITIZER_REVISION: &str = "tracedecay.memory.observation.hygiene.v1+native-bridge-test";

/// Attaches the receipt the admitted hygiene pipeline mints for a payload it
/// read and left byte-identical. Observation dispatch fails closed without one.
fn admitted(call: ProviderCall) -> ProviderCall {
    if call.operation != ProviderOperation::Observe {
        return call;
    }
    let receipt =
        PayloadSanitizationReceipt::new(PayloadSanitizationReceiptParts::accepted_unmodified(
            TEST_SANITIZER_REVISION,
            call.payload.sha256.clone(),
        ))
        .expect("accepted sanitization receipt");
    call.with_sanitization(receipt)
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
            OTHER_SCOPE_DIGEST,
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

async fn real_project_fixture() -> (
    tempfile::TempDir,
    PathBuf,
    Arc<TraceDecay>,
    FactOwnerV1,
    ProjectId,
) {
    let temporary = tempfile::tempdir().expect("native recall fixture root");
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
        .expect("initialize TraceDecay recall fixture"),
    );
    let owner = graph.project_memory_owner().expect("project memory owner");
    let FactOwnerV1::Project { project_id } = owner.clone() else {
        panic!("recall fixture must have a project memory owner");
    };
    (temporary, project_root, graph, owner, project_id)
}

async fn add_real_project_fact(graph: &TraceDecay, content: &str, source_label: &str) -> FactV1 {
    let memory = graph
        .project_memory_application()
        .await
        .expect("project memory application");
    let preflight = memory
        .preflight_project_memory_fact_add(
            ProjectMemoryFactAddRequest {
                content: content.to_owned(),
                category: FactCategoryV1::Project,
                source_label: Some(source_label.to_owned()),
                tags: vec!["native".to_owned(), "bridge".to_owned()],
                entities: vec!["TraceDecay".to_owned()],
                trust: Some(Confidence::new(0.91).expect("fact trust")),
                metadata: json!({"fixture": source_label}),
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
        panic!("recall fixture fact must be applied");
    };
    let projection = super::memory_mapping::projection(applied.fact())
        .expect("map stored recall fixture fact projection");
    match projection {
        FactProjectionV1::Available { fact } => fact.as_ref().clone(),
        FactProjectionV1::Unavailable { .. } => {
            panic!("recall fixture fact must remain available")
        }
    }
}

/// The daemon profile the port under test is mounted for; it is the profile
/// every Native candidate attests, and the profile the recall requests name.
fn test_profile_id() -> tracedecay_domain::UserProfileId {
    tracedecay_domain::UserProfileId::new("profile.native-bridge-recall").expect("profile id")
}

fn recall_scope_value(project_id: &str) -> Value {
    json!({
        "profile_id": "profile.native-bridge-recall",
        "project_id": project_id,
        "repository_identity": "repo.native-bridge-recall",
        "worktree_identity": "worktree.native-bridge-recall",
        "branch_identity": "branch.native-bridge-recall",
        "agent_session_id": "agent.native-bridge-recall",
        "resolved_scope_digest": SCOPE_DIGEST,
    })
}

fn current_rfc3339_micros() -> String {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("test clock after Unix epoch")
        .as_micros();
    let seconds = i64::try_from(micros / 1_000_000).expect("test time seconds fit i64");
    let fraction = u32::try_from(micros % 1_000_000).expect("microsecond fraction fits u32");
    let days = seconds.div_euclid(86_400);
    let second_of_day = seconds.rem_euclid(86_400);
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    let hour = second_of_day / 3_600;
    let minute = second_of_day % 3_600 / 60;
    let second = second_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{fraction:06}Z")
}

fn recall_request_value(project_id: &str) -> Value {
    json!({
        "provider_id": NATIVE_PROVIDER_ID,
        "registration_revision": 1,
        "ready_receipt_digest": "a".repeat(64),
        "exact_scope_identity": recall_scope_value(project_id),
        "request_identity": "request.native-bridge-recall",
        "objective": "search",
        "query": "native bridge",
        "temporal_query": {
            "mode": "current",
            "evaluation_time": current_rfc3339_micros(),
            "as_of": Value::Null,
            "interval_start": Value::Null,
            "interval_end": Value::Null,
            "include_superseded": false,
            "include_revoked": false,
            "unknown_validity_policy": "exclude",
        },
        "budgets": {
            "maximum_candidates": 8,
            "maximum_candidate_content_bytes": 4_096,
            "maximum_total_content_bytes": 8_192,
            "maximum_source_refs_per_candidate": 8,
            "maximum_trace_refs_per_candidate": 8,
            "maximum_warnings": 8,
            "maximum_extensions_per_candidate": 8,
        },
        "exclusions": {
            "stable_memory_refs": [],
            "candidate_ids": [],
            "source_refs": [],
            "trace_refs": [],
            "observation_ids": [],
            "content_sha256": [],
        },
        "required_capabilities": ["recall.query.v1"],
        "policy_revision": 1,
        "extensions": [],
        "deadline": {
            "deadline_utc_micros": i64::MAX,
            "remaining_millis": 5_000,
        },
        "cancellation": "live",
    })
}

fn valid_recall_call(project_id: &str, request: Value) -> ProviderCall {
    let bytes = serde_json::to_vec(&request).expect("recall request bytes");
    ProviderCall::new(ProviderCallParts {
        operation: ProviderOperation::Recall,
        provider_id: OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("valid provider id"),
        registration_revision: 1,
        ready_receipt_sha256: "a".repeat(64),
        exact_scope: OwnedExactScope::new(
            "profile.native-bridge-recall",
            project_id,
            "repo.native-bridge-recall",
            "worktree.native-bridge-recall",
            "branch.native-bridge-recall",
            "agent.native-bridge-recall",
            SCOPE_DIGEST,
        )
        .expect("valid recall exact scope"),
        request_id: "request.native-bridge-recall".to_owned(),
        operation_id: "operation.native-bridge-recall".to_owned(),
        expected_state_generation: 0,
        idempotency_key: Some("idempotency.native-bridge-recall".to_owned()),
        control: OperationControl::new(i64::MAX, 10_000, CancellationToken::new()),
        payload: CanonicalPayload::new(
            OwnedVersionedId::new(RECALL_CONTRACT_ID).expect("valid recall contract"),
            bytes.clone(),
            sha256_hex(&bytes),
        )
        .expect("valid recall payload"),
        required_capabilities: vec![
            OwnedVersionedId::new("recall.query.v1").expect("recall capability"),
        ],
        extensions: Vec::new(),
    })
    .expect("valid recall provider call")
}

fn recall_payload(reply: &ProviderReply) -> Value {
    let payload = reply.payload.as_ref().expect("recall reply payload");
    assert_eq!(payload.contract_id.as_str(), RECALL_CONTRACT_ID);
    serde_json::from_slice(&payload.bytes).expect("canonical recall response JSON")
}

async fn direct_search_scores(
    graph: &TraceDecay,
    owner: &FactOwnerV1,
    query: &str,
    limit: usize,
) -> Vec<(String, [u32; 5])> {
    let memory = graph
        .project_memory_application()
        .await
        .expect("project memory application");
    let search = ProjectMemoryFactSearchQuery::new(
        owner.clone(),
        ProjectMemoryFactSearchKindV1::Search,
        Some(query.to_owned()),
        None,
        limit,
    )
    .expect("direct search query");
    let page = memory
        .search_project_memory_facts(search, &FactReadControl::new(Arc::new(|| false)))
        .await
        .expect("direct project-memory search");
    page.hits()
        .iter()
        .map(|hit| {
            let scores = hit.scores();
            (
                hit.fact().fact_id().to_string(),
                [
                    scores.score_millionths(),
                    scores.fts_score_millionths(),
                    scores.jaccard_score_millionths(),
                    scores.holographic_score_millionths(),
                    scores.trust_score_millionths(),
                ],
            )
        })
        .collect()
}

fn assert_recall_failure(reply: &ProviderReply, terminal_code: TerminalCode, diagnostic_id: &str) {
    assert_eq!(reply.terminal.terminal_code(), terminal_code);
    assert_eq!(reply.terminal.diagnostic_id(), Some(diagnostic_id));
    assert_eq!(
        reply.terminal.committed_effect().state(),
        CommittedEffectState::None
    );
    assert_eq!(reply.payload, None);
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
    let port = ProjectNativeMemoryApplicationPort::new(graph_cell, project_root, test_profile_id())
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_recall_current_preserves_order_and_projects_native_score_explain_provenance() {
    let (_temporary, project_root, graph, owner, project_id) = real_project_fixture().await;
    let alpha = add_real_project_fact(
        &graph,
        "Native bridge deterministic alpha",
        "native-bridge-recall-alpha",
    )
    .await;
    let beta = add_real_project_fact(
        &graph,
        "Native bridge deterministic beta",
        "native-bridge-recall-beta",
    )
    .await;
    let expected_facts = BTreeMap::from([
        (alpha.fact_id.to_string(), alpha),
        (beta.fact_id.to_string(), beta),
    ]);
    let expected_scores = direct_search_scores(&graph, &owner, "native bridge", 8).await;
    assert_eq!(expected_scores.len(), expected_facts.len());

    let graph_cell = Arc::new(tokio::sync::RwLock::new(Arc::clone(&graph)));
    let port = ProjectNativeMemoryApplicationPort::new(graph_cell, project_root, test_profile_id())
        .expect("construct project Native application port");
    let call = valid_recall_call(
        project_id.as_str(),
        recall_request_value(project_id.as_str()),
    );
    let reply = port.recall(&call);
    assert_eq!(reply.terminal.terminal_code(), TerminalCode::Success);
    assert_eq!(
        reply.terminal.committed_effect().state(),
        CommittedEffectState::None
    );
    assert_eq!(reply.state_generation, call.expected_state_generation);
    reply
        .validate(
            native_descriptor()
                .expect("descriptor")
                .limits
                .response_bytes,
        )
        .expect("valid canonical recall reply");
    let body = recall_payload(&reply);

    assert_eq!(body["provider_id"], json!(NATIVE_PROVIDER_ID));
    assert_eq!(body["provider_instance_id"], json!(PROVIDER_INSTANCE_ID));
    assert_eq!(body["registration_revision"], json!(1));
    assert_eq!(body["request_identity"], json!(call.request_id));
    assert_eq!(
        body["exact_scope_identity"],
        recall_scope_value(project_id.as_str())
    );
    assert_eq!(
        body["coverage"]["state"],
        json!("complete"),
        "the current fixture should fit the valid recall budgets"
    );
    assert_eq!(
        body["coverage"]["searched_scope_digest"],
        json!(call.exact_scope.exact_scope_sha256())
    );
    assert_eq!(
        body["coverage"]["matched_items"],
        json!(expected_scores.len())
    );
    assert_eq!(
        body["coverage"]["returned_items"],
        json!(expected_scores.len())
    );
    assert_eq!(body["coverage"]["excluded_items"], json!(0));
    assert_eq!(body["coverage"]["truncated_items"], json!(0));
    assert_eq!(body["coverage"]["next_cursor"], Value::Null);
    assert_eq!(body["coverage"]["reasons"], json!([]));
    assert_eq!(
        body["ordering"],
        json!({
            "score_domain_id": RECALL_SCORE_DOMAIN,
            "direction": "higher_is_better",
            "tie_breaker": "candidate_id_lexicographic_utf8",
        })
    );
    assert_eq!(
        body["terminal"],
        json!({"terminal_code": "success", "diagnostic_id": null})
    );

    let candidates = body["candidates"]
        .as_array()
        .expect("recall candidates array");
    assert_eq!(candidates.len(), expected_scores.len());
    for (candidate, (fact_id, scores)) in candidates.iter().zip(expected_scores.iter()) {
        let fact = expected_facts
            .get(fact_id)
            .expect("direct search fact is in the fixture");
        let source_refs = match &fact.source {
            FactIdentitySourceResultV1::Application { operation_id } => {
                vec![operation_id.to_string()]
            }
            FactIdentitySourceResultV1::Evidence {
                anchor_id,
                stable_key,
            } => vec![anchor_id.to_string(), stable_key.to_string()],
        };
        assert_eq!(
            candidate["candidate_id"],
            json!(format!("{}:{fact_id}", call.request_id))
        );
        assert_eq!(candidate["stable_memory_ref"], json!(fact_id));
        assert_eq!(candidate["content"], json!(fact.content));
        assert_eq!(candidate["content_ref"], Value::Null);
        assert_eq!(
            candidate["content_sha256"],
            json!(sha256_hex(fact.content.as_bytes()))
        );
        assert_eq!(
            candidate["native_score"],
            json!({
                "score_domain_id": RECALL_SCORE_DOMAIN,
                "score_domain_version": RECALL_SCORE_DOMAIN_VERSION,
                "raw_value": format!("{}.{:06}", scores[0] / 1_000_000, scores[0] % 1_000_000),
                "direction": "higher_is_better",
                "declared_minimum": "0.000000",
                "declared_maximum": "1.500000",
                "calibration_state": "provider_calibrated",
                "semantics": "project-memory combined score; fixed-point millionths",
                "components": {
                    "score_millionths": scores[0],
                    "fts_score_millionths": scores[1],
                    "jaccard_score_millionths": scores[2],
                    "holographic_score_millionths": scores[3],
                    "trust_score_millionths": scores[4],
                },
            })
        );
        assert!(
            candidate["native_score"]
                .get("host_normalized_score")
                .is_none()
        );
        // The adapter attests the fact as `project_facts`: the owner project
        // the fact record proves and the profile fixed at mount. The optional
        // checkout fields and the forbidden session/digest fields stay empty,
        // so the host can never admit the candidate under the requester's
        // worktree/branch/session identity.
        assert_eq!(
            candidate["exact_scope_identity"],
            json!({
                "scope_binding": "project_facts",
                "profile_id": "profile.native-bridge-recall",
                "project_id": project_id.as_str(),
                "repository_identity": "",
                "worktree_identity": "",
                "branch_identity": "",
                "agent_session_id": "",
                "resolved_scope_digest": "",
            })
        );
        assert_ne!(
            candidate["exact_scope_identity"],
            recall_scope_value(project_id.as_str())
        );
        assert_eq!(candidate["validity"]["temporal_state"], json!("current"));
        assert_eq!(
            candidate["validity"]["source_revision"],
            json!(fact.last_event_id.to_string())
        );
        assert_eq!(candidate["provenance"]["state"], json!("available"));
        assert_eq!(candidate["provenance"]["origin_refs"], json!(source_refs));
        assert_eq!(candidate["provenance"]["source_refs"], json!(source_refs));
        assert_eq!(
            candidate["provenance"]["native_linkage"],
            json!({
                "outcome_history": {
                    "state": "partial",
                    "active_assertion_id": fact.active_assertion_id.to_string(),
                    "last_event_id": fact.last_event_id.to_string(),
                    "full_lineage": {
                        "state": "unavailable",
                        "reason": RECALL_HISTORY_UNAVAILABLE_REASON,
                        "refs": [],
                    },
                },
            })
        );
        assert_eq!(candidate["provenance"]["observation_refs"], json!([]));
        assert_eq!(candidate["provenance"]["transform_chain"], json!([]));
        assert_eq!(candidate["provenance"]["provider_trace_refs"], json!([]));
        assert_eq!(candidate["provenance"]["redaction_reason"], Value::Null);
        assert!(
            !candidate["explanation"]["summary"]
                .as_str()
                .unwrap_or_default()
                .is_empty()
        );
        assert_eq!(
            candidate["explanation"]["native_linkage_ref"],
            json!("provenance.native_linkage")
        );
        assert_eq!(
            candidate["explanation"]["native_score_ref"],
            json!("native_score")
        );
        assert_eq!(candidate["explanation"]["matched_features"], json!([]));
        assert_eq!(candidate["explanation"]["activation_trace_refs"], json!([]));
        assert_eq!(
            candidate["explanation"]["limitations"],
            json!(["native score is not host-normalized"])
        );
        assert_eq!(candidate["source_refs"], json!(source_refs));
        assert_eq!(candidate["trace_refs"], json!([]));
        assert_eq!(candidate["memory_class"], json!("project"));
        assert_eq!(candidate["warnings"], json!([]));
        assert_eq!(candidate["extensions"], json!([]));
    }

    let repeated = port.recall(&call);
    assert_eq!(repeated.terminal.terminal_code(), TerminalCode::Success);
    assert_eq!(recall_payload(&repeated), body);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_recall_zero_results_returns_success_zero_results_payload() {
    let (_temporary, project_root, graph, _owner, project_id) = real_project_fixture().await;
    let graph_cell = Arc::new(tokio::sync::RwLock::new(Arc::clone(&graph)));
    let port = ProjectNativeMemoryApplicationPort::new(graph_cell, project_root, test_profile_id())
        .expect("construct project Native application port");
    let mut request = recall_request_value(project_id.as_str());
    request["query"] = json!("query-with-no-native-bridge-match");
    let call = valid_recall_call(project_id.as_str(), request);
    let reply = port.recall(&call);
    assert_eq!(
        reply.terminal.terminal_code(),
        TerminalCode::SuccessZeroResults
    );
    assert_eq!(
        reply.terminal.committed_effect().state(),
        CommittedEffectState::None
    );
    reply
        .validate(
            native_descriptor()
                .expect("descriptor")
                .limits
                .response_bytes,
        )
        .expect("valid zero-result recall reply");
    let body = recall_payload(&reply);
    assert_eq!(body["candidates"], json!([]));
    assert_eq!(body["coverage"]["state"], json!("zero_results"));
    assert_eq!(
        body["coverage"]["searched_scope_digest"],
        json!(call.exact_scope.exact_scope_sha256())
    );
    assert_eq!(body["coverage"]["scanned_items"], json!(0));
    assert_eq!(body["coverage"]["matched_items"], json!(0));
    assert_eq!(body["coverage"]["returned_items"], json!(0));
    assert_eq!(body["coverage"]["excluded_items"], json!(0));
    assert_eq!(body["coverage"]["truncated_items"], json!(0));
    assert_eq!(body["coverage"]["next_cursor"], Value::Null);
    assert_eq!(body["coverage"]["reasons"], json!([]));
    assert_eq!(
        body["terminal"],
        json!({"terminal_code": "success_zero_results", "diagnostic_id": null})
    );
    assert_eq!(
        body["ordering"]["score_domain_id"],
        json!(RECALL_SCORE_DOMAIN)
    );
    assert_eq!(body["ordering"]["direction"], json!("higher_is_better"));
    assert_eq!(
        body["ordering"]["tie_breaker"],
        json!("candidate_id_lexicographic_utf8")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_recall_rejects_malformed_unsupported_inputs_without_mutating_store() {
    let (_temporary, project_root, graph, owner, project_id) = real_project_fixture().await;
    let first = add_real_project_fact(
        &graph,
        "Native bridge malformed request fixture one",
        "native-bridge-recall-invalid-one",
    )
    .await;
    let second = add_real_project_fact(
        &graph,
        "Native bridge malformed request fixture two",
        "native-bridge-recall-invalid-two",
    )
    .await;
    let fact_ids = [first.fact_id, second.fact_id];
    let mut before_snapshots = BTreeMap::new();
    for fact_id in fact_ids {
        let value = read_store_snapshot(&graph, &owner, &fact_id).await;
        before_snapshots.insert(fact_id, value);
    }

    let graph_cell = Arc::new(tokio::sync::RwLock::new(Arc::clone(&graph)));
    let port = ProjectNativeMemoryApplicationPort::new(graph_cell, project_root, test_profile_id())
        .expect("construct project Native application port");
    let base = recall_request_value(project_id.as_str());
    let cases = vec![
        (
            "malformed temporal",
            {
                let mut request = base.clone();
                request["temporal_query"]["evaluation_time"] = json!("");
                request
            },
            TerminalCode::InvalidRequest,
            RECALL_INVALID_DIAGNOSTIC,
        ),
        (
            "unsupported temporal mode",
            {
                let mut request = base.clone();
                request["temporal_query"]["mode"] = json!("as_of");
                request
            },
            TerminalCode::CapabilityUnsupported,
            RECALL_UNSUPPORTED_DIAGNOSTIC,
        ),
        (
            "foreign scope",
            {
                let mut request = base.clone();
                request["exact_scope_identity"]["worktree_identity"] = json!("worktree.foreign");
                request
            },
            TerminalCode::ScopeMismatch,
            RECALL_SCOPE_MISMATCH_DIAGNOSTIC,
        ),
        (
            "zero candidate budget",
            {
                let mut request = base.clone();
                request["budgets"]["maximum_candidates"] = json!(0);
                request
            },
            TerminalCode::InvalidRequest,
            RECALL_INVALID_DIAGNOSTIC,
        ),
        (
            "duplicate exclusion",
            {
                let mut request = base.clone();
                request["exclusions"]["candidate_ids"] = json!(["duplicate", "duplicate"]);
                request
            },
            TerminalCode::InvalidRequest,
            RECALL_INVALID_DIAGNOSTIC,
        ),
        (
            "unsupported exclusion",
            {
                let mut request = base.clone();
                request["exclusions"]["candidate_ids"] = json!(["already-returned"]);
                request
            },
            TerminalCode::CapabilityUnsupported,
            RECALL_UNSUPPORTED_DIAGNOSTIC,
        ),
    ];
    for (label, request, terminal_code, diagnostic_id) in cases {
        let reply = port.recall(&valid_recall_call(project_id.as_str(), request));
        assert_recall_failure(&reply, terminal_code, diagnostic_id);
        assert_eq!(
            reply.state_generation, 0,
            "{label} changes state generation"
        );
    }

    for (fact_id, (before_public, before_history)) in before_snapshots {
        let (after_public, after_history) = read_store_snapshot(&graph, &owner, &fact_id).await;
        assert_eq!(
            after_public, before_public,
            "fact changed after rejected recall"
        );
        assert_eq!(
            after_history, before_history,
            "fact history changed after rejected recall"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_recall_does_not_mutate_authoritative_fact_telemetry_or_history() {
    let (_temporary, project_root, graph, owner, project_id) = real_project_fixture().await;
    let first = add_real_project_fact(
        &graph,
        "Native bridge read-only telemetry fixture one",
        "native-bridge-recall-read-only-one",
    )
    .await;
    let second = add_real_project_fact(
        &graph,
        "Native bridge read-only telemetry fixture two",
        "native-bridge-recall-read-only-two",
    )
    .await;
    let fact_ids = vec![first.fact_id.clone(), second.fact_id.clone()];
    let mut before_snapshots = BTreeMap::new();
    for fact_id in &fact_ids {
        let value = read_store_snapshot(&graph, &owner, fact_id).await;
        before_snapshots.insert(fact_id.clone(), value);
    }

    let graph_cell = Arc::new(tokio::sync::RwLock::new(Arc::clone(&graph)));
    let port = ProjectNativeMemoryApplicationPort::new(graph_cell, project_root, test_profile_id())
        .expect("construct project Native application port");
    for _ in 0..2 {
        let reply = port.recall(&valid_recall_call(
            project_id.as_str(),
            recall_request_value(project_id.as_str()),
        ));
        assert_eq!(reply.terminal.terminal_code(), TerminalCode::Success);
        assert_eq!(
            reply.terminal.committed_effect().state(),
            CommittedEffectState::None
        );
        assert_eq!(reply.state_generation, 0);
    }

    for (fact_id, (before_public, before_history)) in before_snapshots {
        let (after_public, after_history) = read_store_snapshot(&graph, &owner, &fact_id).await;
        assert_eq!(
            after_public, before_public,
            "recall changed fact telemetry/state"
        );
        assert_eq!(
            after_history, before_history,
            "recall changed authoritative fact history"
        );
    }
}

/// A recall whose exact scope names a project other than the one this Native
/// instance owns is a typed scope mismatch (a different authority), not an
/// unavailable scope, and never touches the owning project's facts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_recall_for_foreign_project_scope_is_typed_scope_mismatch() {
    let (_temporary, project_root, graph, owner, project_id) = real_project_fixture().await;
    let fact = add_real_project_fact(
        &graph,
        "Native bridge foreign project recall fixture",
        "native-bridge-recall-foreign-project",
    )
    .await;
    let before = read_store_snapshot(&graph, &owner, &fact.fact_id).await;

    let graph_cell = Arc::new(tokio::sync::RwLock::new(Arc::clone(&graph)));
    let port = ProjectNativeMemoryApplicationPort::new(graph_cell, project_root, test_profile_id())
        .expect("construct project Native application port");
    let foreign_project_id = format!("{}-foreign", project_id.as_str());
    assert_ne!(foreign_project_id, project_id.as_str());
    let reply = port.recall(&valid_recall_call(
        &foreign_project_id,
        recall_request_value(&foreign_project_id),
    ));
    assert_recall_failure(
        &reply,
        TerminalCode::ScopeMismatch,
        RECALL_SCOPE_MISMATCH_DIAGNOSTIC,
    );
    assert_eq!(reply.state_generation, 0);

    // The owning project still answers its own scope after the refusal.
    let own = port.recall(&valid_recall_call(
        project_id.as_str(),
        recall_request_value(project_id.as_str()),
    ));
    assert_eq!(own.terminal.terminal_code(), TerminalCode::Success);
    let after = read_store_snapshot(&graph, &owner, &fact.fact_id).await;
    assert_eq!(
        after, before,
        "foreign-scope recall changed the owning project's fact"
    );
}
