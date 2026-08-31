#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use tracedecay_application::retained_surfaces::{
    FactIdentitySourceResultV1, FactSearchHitV1, FactStoreSearchResultV1,
};
use tracedecay_domain::{Confidence, FactCategoryV1, FactOwnerV1, ProjectId};
use tracedecay_memory_provider_registry::{
    CancellationToken, CanonicalPayload, CommittedEffectState, NATIVE_PROVIDER_ID,
    NativeMemoryApplicationPort, OperationControl, OwnedExactScope, OwnedProviderId,
    OwnedVersionedId, ProviderCall, ProviderCallParts, ProviderOperation, ProviderReply,
    TerminalCode,
};
use tracedecay_store::{
    FactReadControl, FactWriteControl, ProjectMemoryFactSearchKindV1, ProjectMemoryFactSearchQuery,
};
use tracedecay_usecases::memory::{
    ProjectMemoryFactAddRequest, ProjectMemoryFactAddRequestOutcome,
};

use super::memory_mapping::public_search_page;
use super::native_provider::ProjectNativeMemoryApplicationPort;
use crate::tracedecay::{TraceDecay, TraceDecayOpenOptions};

const PROJECT_ID: &str = "project.native-provider-parity";
const RECALL_CONTRACT_ID: &str = "tracedecay.memory.provider.recall.v1";

struct StoreFixture {
    _temporary: tempfile::TempDir,
    project_root: PathBuf,
    graph: Arc<TraceDecay>,
    owner: FactOwnerV1,
    project_id: ProjectId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NormalizedCandidate {
    fact_id: String,
    content: String,
    content_sha256: String,
    scores: [u32; 5],
    why: Option<String>,
    provenance: NormalizedProvenance,
    validity: NormalizedValidity,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NormalizedProvenance {
    state: String,
    origin_refs: Vec<String>,
    source_refs: Vec<String>,
    observation_refs: Vec<String>,
    transform_chain_empty: bool,
    provider_trace_refs: Vec<String>,
    redaction_reason_null: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NormalizedValidity {
    temporal_state: String,
    observed_at_present: bool,
    valid_from_present: bool,
    valid_until_null: bool,
    superseded_at_null: bool,
    superseded_by_null: bool,
    revoked_at_null: bool,
    source_revision_present: bool,
}

async fn project_fixture() -> StoreFixture {
    let temporary = tempfile::tempdir().expect("native parity fixture root");
    let project_root = temporary.path().join("project");
    let profile_root = temporary.path().join("profile");
    std::fs::create_dir_all(&project_root).expect("project root");
    std::fs::create_dir_all(&profile_root).expect("profile root");
    crate::storage::pin_fixture_repository_identity(&project_root, PROJECT_ID)
        .expect("project enrollment");
    let graph = Arc::new(
        TraceDecay::init_with_options(
            &project_root,
            TraceDecayOpenOptions {
                global_db_path: Some(profile_root.join("global.db")),
                profile_root: Some(profile_root),
            },
        )
        .await
        .expect("initialize native parity fixture"),
    );
    let owner = graph.project_memory_owner().expect("project memory owner");
    let FactOwnerV1::Project { project_id } = owner.clone() else {
        panic!("native parity fixture must have a project owner");
    };
    assert_eq!(project_id.as_str(), PROJECT_ID);
    StoreFixture {
        _temporary: temporary,
        project_root,
        graph,
        owner,
        project_id,
    }
}

async fn seed_fixture(fixture: &StoreFixture) {
    let memory = fixture
        .graph
        .project_memory_application()
        .await
        .expect("project memory application");
    for (content, source_label) in [
        (
            "native provider parity alpha durable retrieval",
            "native-provider-parity-alpha",
        ),
        (
            "native provider parity beta durable retrieval",
            "native-provider-parity-beta",
        ),
    ] {
        let preflight = memory
            .preflight_project_memory_fact_add(
                ProjectMemoryFactAddRequest {
                    content: content.to_owned(),
                    category: FactCategoryV1::Project,
                    source_label: Some(source_label.to_owned()),
                    tags: vec!["native".to_owned(), "parity".to_owned()],
                    entities: vec!["TraceDecay".to_owned()],
                    trust: Some(Confidence::new(0.91).expect("fact trust")),
                    metadata: json!({"fixture": "native-provider-parity"}),
                },
                None,
            )
            .expect("preflight parity fact");
        let outcome = memory
            .add_preflighted_project_memory_fact(
                preflight,
                &FactWriteControl::new(Arc::new(|| false), Arc::new(|| true)),
            )
            .await
            .expect("commit parity fact");
        assert!(matches!(
            outcome,
            ProjectMemoryFactAddRequestOutcome::Applied(_)
        ));
    }
}

async fn identically_seeded_fixtures() -> (StoreFixture, StoreFixture) {
    let (direct, provider) = tokio::join!(project_fixture(), project_fixture());
    seed_fixture(&direct).await;
    seed_fixture(&provider).await;
    assert_eq!(direct.owner, provider.owner);
    (direct, provider)
}

async fn direct_search(fixture: &StoreFixture) -> FactStoreSearchResultV1 {
    let memory = fixture
        .graph
        .project_memory_application()
        .await
        .expect("project memory application");
    let query = ProjectMemoryFactSearchQuery::new(
        fixture.owner.clone(),
        ProjectMemoryFactSearchKindV1::Search,
        Some("native provider parity".to_owned()),
        None,
        8,
    )
    .expect("direct parity search query");
    let page = memory
        .search_project_memory_facts(query, &FactReadControl::new(Arc::new(|| false)))
        .await
        .expect("direct project-memory search");
    public_search_page(&page).expect("public direct search projection")
}

fn native_port(fixture: &StoreFixture) -> ProjectNativeMemoryApplicationPort {
    let graph_cell = Arc::new(tokio::sync::RwLock::new(Arc::clone(&fixture.graph)));
    ProjectNativeMemoryApplicationPort::new(graph_cell, fixture.project_root.clone())
        .expect("construct project Native application port")
}

fn source_refs(source: &FactIdentitySourceResultV1) -> Vec<String> {
    match source {
        FactIdentitySourceResultV1::Application { operation_id } => {
            vec![operation_id.to_string()]
        }
        FactIdentitySourceResultV1::Evidence {
            anchor_id,
            stable_key,
        } => vec![anchor_id.to_string(), stable_key.to_string()],
    }
}

fn normalized_direct(page: &FactStoreSearchResultV1) -> Vec<NormalizedCandidate> {
    page.hits.iter().map(normalized_direct_hit).collect()
}

fn normalized_direct_hit(hit: &FactSearchHitV1) -> NormalizedCandidate {
    let refs = source_refs(&hit.fact.source);
    NormalizedCandidate {
        fact_id: hit.fact.fact_id.to_string(),
        content: hit.fact.content.clone(),
        content_sha256: tracedecay_domain::canonical_text::sha256_hex(hit.fact.content.as_bytes()),
        scores: [
            hit.scores.score_millionths,
            hit.scores.fts_score_millionths,
            hit.scores.jaccard_score_millionths,
            hit.scores.holographic_score_millionths,
            hit.scores.trust_score_millionths,
        ],
        why: hit.why.clone(),
        provenance: NormalizedProvenance {
            state: "available".to_owned(),
            origin_refs: refs.clone(),
            source_refs: refs,
            observation_refs: Vec::new(),
            transform_chain_empty: true,
            provider_trace_refs: Vec::new(),
            redaction_reason_null: true,
        },
        validity: NormalizedValidity {
            temporal_state: "current".to_owned(),
            observed_at_present: true,
            valid_from_present: true,
            valid_until_null: true,
            superseded_at_null: true,
            superseded_by_null: true,
            revoked_at_null: true,
            source_revision_present: true,
        },
    }
}

fn string_array(value: &Value) -> Vec<String> {
    value
        .as_array()
        .expect("string array")
        .iter()
        .map(|entry| entry.as_str().expect("string array entry").to_owned())
        .collect()
}

fn normalized_provider(body: &Value) -> Vec<NormalizedCandidate> {
    body["candidates"]
        .as_array()
        .expect("provider candidates")
        .iter()
        .map(|candidate| {
            let components = &candidate["native_score"]["components"];
            NormalizedCandidate {
                fact_id: candidate["stable_memory_ref"]
                    .as_str()
                    .expect("stable memory reference")
                    .to_owned(),
                content: candidate["content"]
                    .as_str()
                    .expect("candidate content")
                    .to_owned(),
                content_sha256: candidate["content_sha256"]
                    .as_str()
                    .expect("candidate content digest")
                    .to_owned(),
                scores: [
                    components["score_millionths"]
                        .as_u64()
                        .expect("combined native score") as u32,
                    components["fts_score_millionths"]
                        .as_u64()
                        .expect("fts native score") as u32,
                    components["jaccard_score_millionths"]
                        .as_u64()
                        .expect("jaccard native score") as u32,
                    components["holographic_score_millionths"]
                        .as_u64()
                        .expect("holographic native score") as u32,
                    components["trust_score_millionths"]
                        .as_u64()
                        .expect("trust native score") as u32,
                ],
                why: Some(
                    candidate["explanation"]["summary"]
                        .as_str()
                        .expect("provider explanation summary")
                        .to_owned(),
                ),
                provenance: NormalizedProvenance {
                    state: candidate["provenance"]["state"]
                        .as_str()
                        .expect("provenance state")
                        .to_owned(),
                    origin_refs: string_array(&candidate["provenance"]["origin_refs"]),
                    source_refs: string_array(&candidate["provenance"]["source_refs"]),
                    observation_refs: string_array(&candidate["provenance"]["observation_refs"]),
                    transform_chain_empty: candidate["provenance"]["transform_chain"]
                        .as_array()
                        .expect("transform chain")
                        .is_empty(),
                    provider_trace_refs: string_array(
                        &candidate["provenance"]["provider_trace_refs"],
                    ),
                    redaction_reason_null: candidate["provenance"]["redaction_reason"].is_null(),
                },
                validity: NormalizedValidity {
                    temporal_state: candidate["validity"]["temporal_state"]
                        .as_str()
                        .expect("temporal state")
                        .to_owned(),
                    observed_at_present: !candidate["validity"]["observed_at"].is_null(),
                    valid_from_present: !candidate["validity"]["valid_from"].is_null(),
                    valid_until_null: candidate["validity"]["valid_until"].is_null(),
                    superseded_at_null: candidate["validity"]["superseded_at"].is_null(),
                    superseded_by_null: candidate["validity"]["superseded_by"].is_null(),
                    revoked_at_null: candidate["validity"]["revoked_at"].is_null(),
                    source_revision_present: candidate["validity"]["source_revision"]
                        .as_str()
                        .is_some_and(|value| !value.is_empty()),
                },
            }
        })
        .collect()
}

fn current_recall_time() -> String {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("test clock after Unix epoch")
        .as_micros();
    let seconds = i64::try_from(micros / 1_000_000).expect("test seconds fit i64");
    let fraction = u32::try_from(micros % 1_000_000).expect("test fraction fits u32");
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

fn recall_scope(project_id: &str) -> Value {
    json!({
        "profile_id": "profile.native-provider-parity",
        "project_id": project_id,
        "repository_identity": "repo.native-provider-parity",
        "worktree_identity": "worktree.native-provider-parity",
        "branch_identity": "branch.native-provider-parity",
        "agent_session_id": "agent.native-provider-parity",
        "scope_revision": 1,
    })
}

fn recall_request(project_id: &str) -> Value {
    json!({
        "provider_id": NATIVE_PROVIDER_ID,
        "registration_revision": 1,
        "ready_receipt_digest": "a".repeat(64),
        "exact_scope_identity": recall_scope(project_id),
        "request_identity": "request.native-provider-parity",
        "objective": "search",
        "query": "native provider parity",
        "temporal_query": {
            "mode": "current",
            "evaluation_time": current_recall_time(),
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

fn recall_call(project_id: &str, request: Value) -> ProviderCall {
    let bytes = serde_json::to_vec(&request).expect("recall request bytes");
    ProviderCall::new(ProviderCallParts {
        operation: ProviderOperation::Recall,
        provider_id: OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("provider id"),
        registration_revision: 1,
        ready_receipt_sha256: "a".repeat(64),
        exact_scope: OwnedExactScope::new(
            "profile.native-provider-parity",
            project_id,
            "repo.native-provider-parity",
            "worktree.native-provider-parity",
            "branch.native-provider-parity",
            "agent.native-provider-parity",
            1,
        )
        .expect("recall exact scope"),
        request_id: "request.native-provider-parity".to_owned(),
        operation_id: "operation.native-provider-parity".to_owned(),
        expected_state_generation: 0,
        idempotency_key: Some("idempotency.native-provider-parity".to_owned()),
        control: OperationControl::new(i64::MAX, 10_000, CancellationToken::new()),
        payload: CanonicalPayload::new(
            OwnedVersionedId::new(RECALL_CONTRACT_ID).expect("recall contract"),
            bytes.clone(),
            tracedecay_domain::canonical_text::sha256_hex(&bytes),
        )
        .expect("recall payload"),
        required_capabilities: vec![
            OwnedVersionedId::new("recall.query.v1").expect("recall capability"),
        ],
        extensions: Vec::new(),
    })
    .expect("valid recall call")
}

fn recall_body(reply: &ProviderReply) -> Value {
    assert_eq!(reply.terminal.terminal_code(), TerminalCode::Success);
    assert_eq!(
        reply.terminal.committed_effect().state(),
        CommittedEffectState::None
    );
    let payload = reply.payload.as_ref().expect("recall response payload");
    assert_eq!(payload.contract_id.as_str(), RECALL_CONTRACT_ID);
    serde_json::from_slice(&payload.bytes).expect("recall response JSON")
}

fn assert_provider_validity_is_current(candidate: &Value) {
    let validity = &candidate["validity"];
    let observed_at = validity["observed_at"]
        .as_i64()
        .expect("typed observed timestamp");
    let valid_from = validity["valid_from"]
        .as_i64()
        .expect("typed valid-from timestamp");
    assert!(observed_at >= valid_from);
    assert!(validity["valid_until"].is_null());
    assert!(validity["superseded_at"].is_null());
    assert!(validity["superseded_by"].is_null());
    assert!(validity["revoked_at"].is_null());
    assert!(
        !validity["source_revision"]
            .as_str()
            .expect("typed source revision")
            .is_empty()
    );
    assert_eq!(validity["temporal_state"], json!("current"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_recall_matches_direct_search_semantically_on_identical_stores() {
    let (direct_fixture, provider_fixture) = identically_seeded_fixtures().await;
    let direct = direct_search(&direct_fixture).await;
    let provider = native_port(&provider_fixture);
    let request = recall_request(provider_fixture.project_id.as_str());
    let call = recall_call(provider_fixture.project_id.as_str(), request);
    let reply = provider.recall(&call);
    let body = recall_body(&reply);

    assert_eq!(
        body["candidates"].as_array().map(Vec::len),
        Some(direct.hits.len())
    );
    assert_eq!(normalized_provider(&body), normalized_direct(&direct));
    assert_eq!(body["coverage"]["state"], json!("complete"));
    assert_eq!(body["coverage"]["matched_items"], json!(direct.hits.len()));
    assert_eq!(body["coverage"]["returned_items"], json!(direct.hits.len()));
    assert_eq!(body["coverage"]["excluded_items"], json!(0));
    assert_eq!(body["coverage"]["truncated_items"], json!(0));
    assert_eq!(body["coverage"]["reasons"], json!([]));
    assert_eq!(
        body["ordering"],
        json!({
            "score_domain_id": "tracedecay.native.project-memory.search.v1",
            "direction": "higher_is_better",
            "tie_breaker": "candidate_id_lexicographic_utf8",
        })
    );

    let candidates = body["candidates"].as_array().expect("provider candidates");
    for (candidate, hit) in candidates.iter().zip(direct.hits.iter()) {
        let fact_id = hit.fact.fact_id.to_string();
        assert_eq!(
            candidate["candidate_id"],
            json!(format!("{}:{fact_id}", call.request_id))
        );
        assert_eq!(candidate["stable_memory_ref"], json!(fact_id));
        assert_eq!(candidate["content"], json!(hit.fact.content));
        assert_eq!(candidate["content_ref"], Value::Null);
        assert_eq!(
            candidate["content_sha256"],
            json!(tracedecay_domain::canonical_text::sha256_hex(
                hit.fact.content.as_bytes()
            ))
        );
        assert_provider_validity_is_current(candidate);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_recall_rejects_exact_scope_mismatch_as_typed_failure() {
    let fixture = project_fixture().await;
    let provider = native_port(&fixture);
    let mut request = recall_request(fixture.project_id.as_str());
    request["exact_scope_identity"]["worktree_identity"] = json!("worktree.foreign");
    let call = recall_call(fixture.project_id.as_str(), request);
    let reply = provider.recall(&call);

    assert_eq!(reply.terminal.terminal_code(), TerminalCode::ScopeMismatch);
    assert_eq!(
        reply.terminal.diagnostic_id(),
        Some("native.recall_scope_mismatch")
    );
    assert_eq!(
        reply.terminal.committed_effect().state(),
        CommittedEffectState::None
    );
    assert_eq!(reply.payload, None);
    assert_eq!(reply.state_generation, call.expected_state_generation);
}
