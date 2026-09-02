//! Real Native baseline over the deterministic coding-memory scenario corpus.
//!
//! The Native lane is the production `NativeProvider` bound to the real
//! `ProjectNativeMemoryApplicationPort` over a temporary TraceDecay store
//! whose project owner is the corpus project identity. The no-memory and
//! explicit-documentation lanes run through the same runner, corpus, fixture,
//! scope catalog, recall catalog, and host configuration, so the three
//! reports carry one shared-inputs digest and are comparable.
//!
//! Assertions state measured Native behavior: the corpus observation kinds
//! are not the Native fact-promotion kind, so Native stages them with a typed
//! `capability_unsupported` terminal, and its recalls at the corpus project
//! resolve to typed zero-result or scope-mismatch terminals without admitting
//! any context.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::path::PathBuf;
use std::sync::Arc;

use tracedecay_domain::FactOwnerV1;
use tracedecay_memory_conformance::{
    BaselineComparison, BaselineLane, BaselineReport, BaselineRunConfig, BaselineRunner, LaneKind,
    O200K_BASE_ESTIMATOR_ID, O200K_BASE_ESTIMATOR_REVISION, ProviderLane, ScenarioCorpus,
    ScenarioStep, StepOutcome, TokenEstimatorIdentity, TokenRecord,
};
use tracedecay_memory_provider_registry::{NATIVE_PROVIDER_ID, NativeProvider};

use super::ProjectNativeMemoryApplicationPort;
use crate::tracedecay::{TraceDecay, TraceDecayOpenOptions};

/// Project identity of the corpus scope catalog's ledger scopes.
const CORPUS_PROJECT_ID: &str = "project_ledger_v1";
const REGISTRATION_REVISION: u64 = 1;

struct StoreFixture {
    _temporary: tempfile::TempDir,
    project_root: PathBuf,
    fixture_root: PathBuf,
    graph: Arc<TraceDecay>,
}

async fn project_fixture() -> StoreFixture {
    let temporary = tempfile::tempdir().expect("native baseline fixture root");
    let project_root = temporary.path().join("project");
    let profile_root = temporary.path().join("profile");
    let fixture_root = temporary.path().join("baseline-workspaces");
    std::fs::create_dir_all(&project_root).expect("project root");
    std::fs::create_dir_all(&profile_root).expect("profile root");
    crate::storage::pin_fixture_repository_identity(&project_root, CORPUS_PROJECT_ID)
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
        .expect("initialize native baseline fixture"),
    );
    let owner = graph.project_memory_owner().expect("project memory owner");
    let FactOwnerV1::Project { project_id } = owner else {
        panic!("native baseline fixture must have a project owner");
    };
    assert_eq!(project_id.as_str(), CORPUS_PROJECT_ID);
    StoreFixture {
        _temporary: temporary,
        project_root,
        fixture_root,
        graph,
    }
}

fn corpus() -> ScenarioCorpus {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../product/evaluation/coding-memory-scenarios.v1.json");
    let bytes = std::fs::read(path).expect("read coding-memory scenario corpus");
    ScenarioCorpus::from_json_bytes(&bytes).expect("load coding-memory scenario corpus")
}

fn native_provider(fixture: &StoreFixture) -> NativeProvider {
    let graph_cell = Arc::new(tokio::sync::RwLock::new(Arc::clone(&fixture.graph)));
    let port = ProjectNativeMemoryApplicationPort::new(
        graph_cell,
        fixture.project_root.clone(),
        tracedecay_domain::UserProfileId::new("profile.native-baseline").expect("profile id"),
    )
    .expect("construct project Native application port");
    NativeProvider::new(Arc::new(port)).expect("construct Native provider")
}

fn run_three_lanes(
    corpus: &ScenarioCorpus,
    fixture: &StoreFixture,
) -> (BaselineReport, BaselineReport, BaselineReport) {
    let provider = native_provider(fixture);
    let runner = BaselineRunner::new(corpus, BaselineRunConfig::new(fixture.fixture_root.clone()))
        .expect("baseline runner");
    let no_memory = runner
        .run(&BaselineLane::NoMemory)
        .expect("no-memory lane")
        .report;
    let docs = runner
        .run(&BaselineLane::ExplicitDocumentation)
        .expect("explicit-documentation lane")
        .report;
    let lane = BaselineLane::Provider(
        ProviderLane::new(&provider, REGISTRATION_REVISION).expect("native provider lane"),
    );
    let native = runner.run(&lane).expect("native lane").report;
    (no_memory, docs, native)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_baseline_shares_inputs_with_no_memory_and_documentation_lanes() {
    let corpus = corpus();
    let fixture = project_fixture().await;
    let (no_memory, docs, native) = run_three_lanes(&corpus, &fixture);

    assert_eq!(native.identity.lane.kind, LaneKind::Provider);
    assert_eq!(
        native.identity.lane.lane_id,
        format!("provider:{NATIVE_PROVIDER_ID}")
    );
    let provider_identity = native
        .identity
        .lane
        .provider
        .as_ref()
        .expect("native provider identity");
    assert_eq!(provider_identity.provider_id, NATIVE_PROVIDER_ID);
    assert_eq!(
        provider_identity.registration_revision,
        REGISTRATION_REVISION
    );
    assert_eq!(
        no_memory.identity.shared_inputs_sha256,
        native.identity.shared_inputs_sha256
    );
    assert_eq!(
        docs.identity.shared_inputs_sha256,
        native.identity.shared_inputs_sha256
    );
    assert_eq!(
        no_memory.identity.shared_inputs,
        native.identity.shared_inputs
    );
    assert_eq!(
        native.identity.shared_inputs.corpus_sha256,
        corpus.corpus_sha256()
    );
    // All three lanes count tokens under the one pinned production estimator.
    let pinned = TokenEstimatorIdentity::Pinned {
        estimator_id: O200K_BASE_ESTIMATOR_ID.to_owned(),
        estimator_revision: O200K_BASE_ESTIMATOR_REVISION.to_owned(),
    };
    assert_eq!(native.identity.token_estimator, pinned);
    assert_eq!(no_memory.identity.token_estimator, pinned);
    assert_eq!(docs.identity.token_estimator, pinned);
    let docs_tokens: u64 = docs
        .scenarios
        .iter()
        .map(|scenario| match scenario.cost.estimated_tokens {
            TokenRecord::Estimated { tokens } => tokens,
            TokenRecord::Indeterminate => panic!("{} tokens indeterminate", scenario.scenario_id),
        })
        .sum();
    assert!(docs_tokens > 0, "documentation lane admitted no tokens");

    let comparison =
        BaselineComparison::compare(&[&no_memory, &docs, &native]).expect("comparable lanes");
    assert_eq!(comparison.lane_ids.len(), 3);
    assert_eq!(comparison.rows.len(), corpus.scenarios().len());
    for row in &comparison.rows {
        assert_eq!(row.lanes.len(), 3, "{}", row.scenario_id);
    }
    let comparison_bytes = comparison
        .to_canonical_json()
        .expect("canonical comparison bytes");
    assert!(!comparison_bytes.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_baseline_records_typed_terminals_and_zero_admitted_context() {
    let corpus = corpus();
    let fixture = project_fixture().await;
    let (_, _, native) = run_three_lanes(&corpus, &fixture);

    let mut observe_calls = 0_u64;
    let mut recall_contexts = 0_usize;
    for scenario in &native.scenarios {
        for step in &scenario.steps {
            for call in &step.provider_calls {
                assert!(
                    call.request_id.starts_with(&scenario.scenario_id),
                    "{} step {} request {}",
                    scenario.scenario_id,
                    step.step,
                    call.request_id
                );
                match call.operation.as_str() {
                    "handshake" => {
                        assert!(call.provider_contacted);
                        assert_eq!(
                            call.terminal_code, "success",
                            "{} step {} handshake {:?}",
                            scenario.scenario_id, step.step, call.diagnostic_id
                        );
                    }
                    "observe" => {
                        observe_calls += 1;
                        if call.provider_contacted {
                            // Corpus observation kinds are not the Native
                            // fact-promotion kind: Native stages them with a
                            // typed unsupported terminal and commits nothing.
                            assert_eq!(
                                call.terminal_code, "capability_unsupported",
                                "{} step {} {:?}",
                                scenario.scenario_id, step.step, call.diagnostic_id
                            );
                            assert_eq!(
                                call.diagnostic_id.as_deref(),
                                Some("native.observation_staged")
                            );
                        } else {
                            // Only the host cancellation preflight refuses dispatch.
                            assert_eq!(call.terminal_code, "cancelled");
                        }
                        assert_eq!(call.committed_effect_state, "none");
                        assert_eq!(call.state_generation_before, call.state_generation_after);
                    }
                    "recall" => {
                        assert!(call.provider_contacted);
                        let scope = corpus
                            .recall_request(&step_request_id(step))
                            .map(|request| request.scope_id.clone())
                            .expect("catalogued recall request");
                        let expected = if scope == "scope_other_project" {
                            "scope_mismatch"
                        } else {
                            "success_zero_results"
                        };
                        assert_eq!(
                            call.terminal_code, expected,
                            "{} step {} {:?}",
                            scenario.scenario_id, step.step, call.diagnostic_id
                        );
                    }
                    "health" => {
                        assert!(call.provider_contacted);
                        assert_eq!(call.terminal_code, "success");
                    }
                    "deletion_by_source" | "snapshot_restore" => {
                        // Native declares neither optional capability; the host
                        // refuses before dispatch with a typed terminal.
                        assert!(!call.provider_contacted);
                        assert_eq!(call.terminal_code, "capability_unsupported");
                        assert_eq!(
                            call.diagnostic_id.as_deref(),
                            Some("host.capability_undeclared")
                        );
                    }
                    other => panic!("unexpected operation {other}"),
                }
            }
            if let Some(context) = &step.context {
                recall_contexts += 1;
                assert_eq!(context.admitted_context_bytes, 0);
                assert!(context.entries.is_empty());
                assert!(context.provider_call_count >= 1);
                assert_eq!(
                    context.estimated_tokens,
                    TokenRecord::Estimated { tokens: 0 }
                );
            }
        }
        assert_eq!(scenario.cost.admitted_context_bytes, 0);
        assert_eq!(scenario.cost.admitted_entries, 0);
        // Token cost is determinate under the pinned production estimator.
        assert_eq!(
            scenario.cost.estimated_tokens,
            TokenRecord::Estimated { tokens: 0 }
        );
    }
    assert!(observe_calls >= 15, "{observe_calls}");
    // Only recall-class steps (recall, verify_absence) admit context; health
    // steps carry no admission record, so the expected count is derived from
    // the corpus rather than pinned by hand.
    let expected_contexts = corpus
        .scenarios()
        .iter()
        .flat_map(|scenario| scenario.steps.iter())
        .filter(|step| {
            matches!(
                step,
                ScenarioStep::Recall { .. } | ScenarioStep::VerifyAbsence { .. }
            )
        })
        .count();
    assert!(
        expected_contexts > 0,
        "corpus carries no recall-class steps"
    );
    assert_eq!(recall_contexts, expected_contexts);

    let scope = native
        .scenario("project_worktree_scope")
        .expect("scope scenario");
    let other = scope
        .steps
        .iter()
        .find(|step| step.step == 5)
        .expect("other-project recall");
    assert!(matches!(
        &other.outcome,
        StepOutcome::Terminal { terminal_code, .. } if terminal_code == "scope_mismatch"
    ));
    assert!(scope.adjudication.terminal_gate.passed);

    let restart = native.scenario("restart").expect("restart scenario");
    let restarted = restart
        .steps
        .iter()
        .find(|step| step.step == 2)
        .expect("restart step");
    assert!(matches!(
        &restarted.outcome,
        StepOutcome::ProviderRestarted {
            descriptor_stable: true,
            ..
        }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_baseline_report_is_byte_identical_across_fresh_stores() {
    let corpus = corpus();
    let first_fixture = project_fixture().await;
    let (_, _, first) = run_three_lanes(&corpus, &first_fixture);
    let second_fixture = project_fixture().await;
    let (_, _, second) = run_three_lanes(&corpus, &second_fixture);
    assert_eq!(
        first.identity.run_identity_sha256,
        second.identity.run_identity_sha256
    );
    assert_eq!(
        first.to_canonical_json().expect("first report bytes"),
        second.to_canonical_json().expect("second report bytes")
    );
}

fn step_request_id(step: &tracedecay_memory_conformance::BaselineStepRecord) -> String {
    step.context
        .as_ref()
        .map(|context| context.request_id.clone())
        .expect("recall-class step carries its catalogued request")
}
