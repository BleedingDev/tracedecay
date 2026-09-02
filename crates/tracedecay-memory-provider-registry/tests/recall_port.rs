//! Behavioral tests for the production cognitive-recall port: exact-scope
//! admission through the real fabric and Native adapter, typed degradations
//! for cancellation, deadlines, and provider unavailability, typed errors
//! for scope rejection, unattributable outcomes, and disabled composition,
//! and explicit routing: the pinned provider identity on every result,
//! pre-contact refusal of observer or mismatched registrations, and fallback
//! that is never dispatched without a matching host rule.
#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

mod recall_fixture;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use recall_fixture::*;
use tracedecay_application::memory::{
    CognitiveRecallDegradation, CognitiveRecallPort, CognitiveRecallRequest,
};
use tracedecay_application::{CancellationContext, Deadline, RequestId, ResolvedScope, now_micros};
use tracedecay_domain::{ProjectId, RefId, RepositoryId, UtcMicros, WorktreeId};
use tracedecay_memory_provider_api::contract::TerminalCode;
use tracedecay_memory_provider_api::{OwnedExactScope, OwnedProviderId, PinnedFallbackPolicy};
use tracedecay_memory_provider_registry::{
    ActiveRoutingPolicy, CognitiveRecallPortError, CognitiveRecallPortInputsV1,
    EnabledProviderMode, ExactScopeBinding, ExactScopeBindingError, FabricConfig, FallbackDecision,
    FallbackDeclinedReason, FallbackRule, NATIVE_PROVIDER_ID, NativeProviderActivation,
    ProjectCognitiveRecallPortV1, ProjectMemoryProviderComposition, ProviderMode,
    ProviderReadiness, RecallAdmissionAuditError, RecallAdmissionError, RecallAdmissionObserver,
    RecallAdmissionReport, RecallBudgetsV1, RecallDenialReason, RecallScopeBindingsV1,
    RoutingError, ScopeBinding, ScopeField,
};

/// Host-side scope binding standing in for the composition root: profile and
/// session identities are host-owned constants, checkout identity comes from
/// the resolved scope verbatim.
struct TestScopeBinding;

impl ExactScopeBinding for TestScopeBinding {
    fn bind_exact_scope(
        &self,
        scope: &ResolvedScope,
    ) -> Result<OwnedExactScope, ExactScopeBindingError> {
        let reference = scope.reference.as_ref().ok_or_else(|| {
            ExactScopeBindingError::ReferenceUnavailable {
                project_id: scope.project_id.as_str().to_owned(),
            }
        })?;
        Ok(OwnedExactScope::new(
            "profile-recall",
            scope.project_id.as_str(),
            scope.repository_id.as_str(),
            scope.worktree_id.as_str(),
            reference.as_str(),
            "session-recall",
            scope.scope_digest.as_str(),
        )?)
    }
}

#[derive(Default)]
struct LedgerObserver(Mutex<Vec<RecallAdmissionReport>>);

impl RecallAdmissionObserver for LedgerObserver {
    fn observe_admission(
        &self,
        report: &RecallAdmissionReport,
    ) -> Result<(), RecallAdmissionAuditError> {
        self.0.lock().expect("ledger lock").push(report.clone());
        Ok(())
    }
}

/// Audit sink that refuses every report, standing in for an unwritable ledger.
struct RefusingObserver;

impl RecallAdmissionObserver for RefusingObserver {
    fn observe_admission(
        &self,
        report: &RecallAdmissionReport,
    ) -> Result<(), RecallAdmissionAuditError> {
        Err(RecallAdmissionAuditError {
            request_id: report.request_id.clone(),
            source: "ledger unwritable".into(),
        })
    }
}

fn resolved_scope(reference: Option<&str>) -> ResolvedScope {
    ResolvedScope::new(
        ProjectId::new("project.recall-port").expect("project id"),
        RepositoryId::new("repository.recall-port").expect("repository id"),
        WorktreeId::new("worktree.recall-port").expect("worktree id"),
        reference.map(|reference| RefId::new(reference).expect("reference id")),
    )
    .expect("resolved scope")
}

fn request(
    scope: ResolvedScope,
    deadline_offset_micros: i64,
    cancelled: bool,
) -> CognitiveRecallRequest {
    let now = now_micros();
    let cancellation = if cancelled {
        CancellationContext::cancelled("token.recall-port", now).expect("cancelled context")
    } else {
        CancellationContext::active("token.recall-port").expect("active context")
    };
    CognitiveRecallRequest::new(
        scope,
        RequestId::new("request.recall-port").expect("request id"),
        Deadline::new(UtcMicros(now.0.saturating_add(deadline_offset_micros))).expect("deadline"),
        cancellation,
        "recall admission",
        8,
    )
    .expect("recall request")
}

fn compose_mode(
    port: Arc<RecallFixturePort>,
    mode: EnabledProviderMode,
) -> Arc<ProjectMemoryProviderComposition> {
    Arc::new(
        ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
            fabric_config: FabricConfig {
                max_registered_providers: 1,
                max_in_flight: 2,
            },
            port,
            registration_revision: 31,
            mode,
        })
        .expect("enabled composition"),
    )
}

fn mount(
    composition: Arc<ProjectMemoryProviderComposition>,
    observer: Arc<LedgerObserver>,
) -> Result<ProjectCognitiveRecallPortV1, CognitiveRecallPortError> {
    mount_with(composition, observer, budgets())
}

fn mount_with(
    composition: Arc<ProjectMemoryProviderComposition>,
    observer: Arc<dyn RecallAdmissionObserver>,
    budgets: RecallBudgetsV1,
) -> Result<ProjectCognitiveRecallPortV1, CognitiveRecallPortError> {
    mount_routed(
        composition,
        observer,
        budgets,
        routing(NATIVE_PROVIDER_ID, 31, FallbackRule::Forbidden),
    )
}

fn mount_routed(
    composition: Arc<ProjectMemoryProviderComposition>,
    observer: Arc<dyn RecallAdmissionObserver>,
    budgets: RecallBudgetsV1,
    routing: ActiveRoutingPolicy,
) -> Result<ProjectCognitiveRecallPortV1, CognitiveRecallPortError> {
    ProjectCognitiveRecallPortV1::mount(CognitiveRecallPortInputsV1 {
        composition,
        scope_binding: Arc::new(TestScopeBinding),
        admission_observer: observer,
        routing,
        host_limits: limits(),
        policy_revision: 1,
        budgets,
    })
}

fn routing(provider: &str, revision: u64, fallback: FallbackRule) -> ActiveRoutingPolicy {
    ActiveRoutingPolicy::new(
        OwnedProviderId::new(provider).expect("provider id"),
        revision,
        fallback,
    )
    .expect("routing policy")
}

/// The registered provider's readiness as the fabric reports it; `NotReady`
/// proves no handshake was ever accepted for it.
fn native_readiness(composition: &ProjectMemoryProviderComposition) -> ProviderReadiness {
    let statuses = composition
        .registry()
        .expect("enabled composition")
        .statuses()
        .expect("statuses");
    assert_eq!(statuses.len(), 1);
    statuses[0].readiness
}

/// The host-owned candidate budget is the one admission enforces: when it is
/// smaller than the application request, a provider that returns more than
/// the host budget is refused even though it stayed under the request.
#[tokio::test]
async fn host_candidate_budget_is_enforced_when_smaller_than_the_request() {
    let provider = Arc::new(RecallFixturePort::new());
    let observer = Arc::new(LedgerObserver::default());
    // The fixture returns six candidates; the request asks for eight.
    let host_budgets = RecallBudgetsV1 {
        maximum_candidates: 5,
        ..budgets()
    };
    let port = mount_with(
        compose_mode(provider.clone(), EnabledProviderMode::Active),
        observer.clone(),
        host_budgets,
    )
    .expect("mounted port");
    let scope = resolved_scope(Some("refs/heads/recall-port"));
    let request = request(scope, 60_000_000, false);
    assert_eq!(request.maximum_candidates(), 8);

    let error = port
        .recall_admitted(request)
        .await
        .expect_err("provider exceeded the host candidate budget");
    assert!(
        matches!(
            error,
            CognitiveRecallPortError::Admission(RecallAdmissionError::CandidateBudgetExceeded {
                returned: 6,
                maximum: 5,
            })
        ),
        "{error:?}"
    );
    assert_eq!(provider.recall_calls.load(Ordering::Relaxed), 1);
    assert!(observer.0.lock().unwrap().is_empty());
}

/// An audit sink that cannot retain the report fails the recall: admitted
/// content is never delivered without its denial ledger.
#[tokio::test]
async fn unwritable_audit_sink_withholds_the_admitted_result() {
    let provider = Arc::new(RecallFixturePort::new());
    let port = mount_with(
        compose_mode(provider.clone(), EnabledProviderMode::Active),
        Arc::new(RefusingObserver),
        budgets(),
    )
    .expect("mounted port");
    let scope = resolved_scope(Some("refs/heads/recall-port"));

    let error = port
        .recall_admitted(request(scope, 60_000_000, false))
        .await
        .expect_err("audit sink refused the report");
    match error {
        CognitiveRecallPortError::AdmissionAudit(audit) => {
            assert_eq!(audit.request_id, "request.recall-port");
        }
        other => panic!("expected an admission audit error, got {other:?}"),
    }
    assert_eq!(provider.recall_calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn port_admits_only_exact_scope_current_candidates_and_reports_denials() {
    let provider = Arc::new(RecallFixturePort::new());
    let observer = Arc::new(LedgerObserver::default());
    let port = mount(
        compose_mode(provider.clone(), EnabledProviderMode::Active),
        observer.clone(),
    )
    .expect("mounted port");
    let scope = resolved_scope(Some("refs/heads/recall-port"));

    let outcome = port
        .recall_admitted(request(scope.clone(), 60_000_000, false))
        .await
        .expect("bridged recall");

    assert!(outcome.result.is_complete(), "{:?}", outcome.result);
    assert_eq!(outcome.result.scope(), &scope);
    let admitted: Vec<_> = outcome
        .result
        .candidates()
        .iter()
        .map(|candidate| candidate.candidate_id())
        .collect();
    assert_eq!(admitted, vec!["in-scope-1", "in-scope-2"]);
    for candidate in outcome.result.candidates() {
        assert!(!candidate.content().contains(SECRET_CONTENT));
        assert_eq!(
            candidate.stable_reference(),
            Some(format!("memory:{}", candidate.candidate_id()).as_str())
        );
        assert_eq!(candidate.explanation(), Some("fixture match"));
    }
    assert!(outcome.unhydrated_reference_candidate_ids.is_empty());

    let report = outcome.report.expect("admission report");
    let denied: Vec<_> = report
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
            (
                "unauthorized-exact-scope",
                RecallDenialReason::ScopeBindingUnauthorized {
                    binding: ScopeBinding::ExactCodingScope
                }
            ),
        ]
    );
    assert_eq!(
        report.authorized_scope_bindings,
        RecallScopeBindingsV1::new([ScopeBinding::ProjectFacts, ScopeBinding::ProfileFacts])
    );
    assert_eq!(report.request_id, "request.recall-port");
    let expected_scope = TestScopeBinding
        .bind_exact_scope(&scope)
        .expect("bound scope");
    assert_eq!(
        report.exact_scope_sha256,
        expected_scope.exact_scope_sha256()
    );
    assert!(
        !serde_json::to_string(&report)
            .unwrap()
            .contains(SECRET_CONTENT)
    );
    assert_eq!(observer.0.lock().unwrap().len(), 1);
    assert_eq!(provider.recall_calls.load(Ordering::Relaxed), 1);

    // The application port path returns exactly the admitted result and
    // reports through the same observer.
    let via_port = port
        .recall(request(scope, 60_000_000, false))
        .await
        .expect("port recall");
    assert_eq!(via_port.candidates().len(), 2);
    assert!(via_port.is_complete());
    assert_eq!(observer.0.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn cancelled_and_elapsed_requests_degrade_without_provider_contact() {
    let provider = Arc::new(RecallFixturePort::new());
    let observer = Arc::new(LedgerObserver::default());
    let port = mount(
        compose_mode(provider.clone(), EnabledProviderMode::Active),
        observer.clone(),
    )
    .expect("mounted port");
    let scope = resolved_scope(Some("refs/heads/recall-port"));

    let cancelled = port
        .recall_admitted(request(scope.clone(), 60_000_000, true))
        .await
        .expect("cancelled recall");
    assert_eq!(
        cancelled.result.degradation(),
        Some(CognitiveRecallDegradation::Cancelled)
    );
    assert!(cancelled.result.candidates().is_empty());
    assert!(cancelled.report.is_none());

    let elapsed = port
        .recall_admitted(request(scope, -1, false))
        .await
        .expect("elapsed recall");
    assert_eq!(
        elapsed.result.degradation(),
        Some(CognitiveRecallDegradation::TimedOut)
    );
    assert!(elapsed.result.candidates().is_empty());
    assert!(elapsed.report.is_none());

    assert_eq!(provider.recall_calls.load(Ordering::Relaxed), 0);
    assert!(observer.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn observer_only_and_disabled_compositions_are_typed() {
    let provider = Arc::new(RecallFixturePort::new());
    let observer = Arc::new(LedgerObserver::default());
    let port = mount(
        compose_mode(provider.clone(), EnabledProviderMode::Observer),
        observer.clone(),
    )
    .expect("mounted port");
    let error = port
        .recall_admitted(request(
            resolved_scope(Some("refs/heads/recall-port")),
            60_000_000,
            false,
        ))
        .await
        .expect_err("observer composition cannot serve recall");
    match error {
        CognitiveRecallPortError::ProviderNotActive { provider_id, mode } => {
            assert_eq!(provider_id, NATIVE_PROVIDER_ID);
            assert_eq!(mode, ProviderMode::Observer);
        }
        other => panic!("observer must be refused by routing before contact: {other:?}"),
    }
    // Refused before any contact: no handshake, no recall.
    assert_eq!(provider.handshake_calls.load(Ordering::Relaxed), 0);
    assert_eq!(provider.recall_calls.load(Ordering::Relaxed), 0);

    let disabled = Arc::new(
        ProjectMemoryProviderComposition::compose(NativeProviderActivation::Disabled)
            .expect("disabled composition"),
    );
    assert!(matches!(
        mount(disabled, observer).err(),
        Some(CognitiveRecallPortError::CompositionDisabled)
    ));
}

#[tokio::test]
async fn unresolved_reference_is_a_typed_scope_error_before_any_provider_contact() {
    let provider = Arc::new(RecallFixturePort::new());
    let port = mount(
        compose_mode(provider.clone(), EnabledProviderMode::Active),
        Arc::new(LedgerObserver::default()),
    )
    .expect("mounted port");
    let error = port
        .recall_admitted(request(resolved_scope(None), 60_000_000, false))
        .await
        .expect_err("a scope without a reference has no exact branch identity");
    assert!(
        matches!(
            error,
            CognitiveRecallPortError::Scope(ExactScopeBindingError::ReferenceUnavailable { .. })
        ),
        "{error:?}"
    );
    assert_eq!(provider.recall_calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn provider_terminals_map_to_typed_degradations_or_errors_never_empty_success() {
    let mut unavailable = RecallFixturePort::new();
    unavailable.terminal_code = TerminalCode::ProviderUnavailable;
    let port = mount(
        compose_mode(Arc::new(unavailable), EnabledProviderMode::Active),
        Arc::new(LedgerObserver::default()),
    )
    .expect("mounted port");
    let outcome = port
        .recall_admitted(request(
            resolved_scope(Some("refs/heads/recall-port")),
            60_000_000,
            false,
        ))
        .await
        .expect("unavailable provider degrades the lane");
    assert_eq!(
        outcome.result.degradation(),
        Some(CognitiveRecallDegradation::Unavailable)
    );
    assert!(outcome.result.candidates().is_empty());
    assert!(outcome.report.is_none());

    let mut rejecting = RecallFixturePort::new();
    rejecting.terminal_code = TerminalCode::ScopeMismatch;
    let port = mount(
        compose_mode(Arc::new(rejecting), EnabledProviderMode::Active),
        Arc::new(LedgerObserver::default()),
    )
    .expect("mounted port");
    let error = port
        .recall_admitted(request(
            resolved_scope(Some("refs/heads/recall-port")),
            60_000_000,
            false,
        ))
        .await
        .expect_err("scope rejection is not a degradation");
    assert!(
        matches!(
            error,
            CognitiveRecallPortError::ScopeRejected {
                terminal_code: TerminalCode::ScopeMismatch,
                ..
            }
        ),
        "{error:?}"
    );

    let mut mismatched = RecallFixturePort::new();
    mismatched.outcome_request_identity = Some("someone-elses-request".to_owned());
    let port = mount(
        compose_mode(Arc::new(mismatched), EnabledProviderMode::Active),
        Arc::new(LedgerObserver::default()),
    )
    .expect("mounted port");
    let error = port
        .recall_admitted(request(
            resolved_scope(Some("refs/heads/recall-port")),
            60_000_000,
            false,
        ))
        .await
        .expect_err("unattributable outcome is refused");
    assert!(
        matches!(
            error,
            CognitiveRecallPortError::Admission(RecallAdmissionError::OutcomeBinding {
                field: "request_identity"
            })
        ),
        "{error:?}"
    );
}

/// Every result names the configured provider: complete results and
/// terminal degradations carry the handshake's runtime instance, and lanes
/// degraded before any contact still carry the pinned provider and revision.
#[tokio::test]
async fn every_result_carries_the_configured_provider_identity() {
    let provider = Arc::new(RecallFixturePort::new());
    let port = mount(
        compose_mode(provider.clone(), EnabledProviderMode::Active),
        Arc::new(LedgerObserver::default()),
    )
    .expect("mounted port");
    let scope = resolved_scope(Some("refs/heads/recall-port"));

    let complete = port
        .recall_admitted(request(scope.clone(), 60_000_000, false))
        .await
        .expect("complete recall");
    let identity = complete.result.provider();
    assert_eq!(identity.provider_id(), NATIVE_PROVIDER_ID);
    assert_eq!(identity.registration_revision(), 31);
    assert_eq!(
        identity.provider_instance_id(),
        Some("native.recall-fixture")
    );
    assert_eq!(complete.fallback, FallbackDecision::NotApplicable);

    let cancelled = port
        .recall_admitted(request(scope.clone(), 60_000_000, true))
        .await
        .expect("cancelled recall");
    let identity = cancelled.result.provider();
    assert_eq!(identity.provider_id(), NATIVE_PROVIDER_ID);
    assert_eq!(identity.registration_revision(), 31);
    assert_eq!(identity.provider_instance_id(), None);
    assert_eq!(cancelled.fallback, FallbackDecision::NotApplicable);

    let mut unavailable = RecallFixturePort::new();
    unavailable.terminal_code = TerminalCode::ProviderUnavailable;
    let port = mount(
        compose_mode(Arc::new(unavailable), EnabledProviderMode::Active),
        Arc::new(LedgerObserver::default()),
    )
    .expect("mounted port");
    let degraded = port
        .recall_admitted(request(scope, 60_000_000, false))
        .await
        .expect("unavailable recall");
    let identity = degraded.result.provider();
    assert_eq!(identity.provider_id(), NATIVE_PROVIDER_ID);
    assert_eq!(
        identity.provider_instance_id(),
        Some("native.recall-fixture")
    );
    assert_eq!(
        degraded.result.degradation(),
        Some(CognitiveRecallDegradation::Unavailable)
    );
}

/// A complete zero-result search and a provider that is unavailable are
/// different typed outcomes with different evidence, and neither raises
/// fallback.
#[tokio::test]
async fn zero_results_and_unavailable_are_distinct_typed_outcomes() {
    let mut zero = RecallFixturePort::new();
    zero.terminal_code = TerminalCode::SuccessZeroResults;
    let observer = Arc::new(LedgerObserver::default());
    let port = mount(
        compose_mode(Arc::new(zero), EnabledProviderMode::Active),
        observer.clone(),
    )
    .expect("mounted port");
    let scope = resolved_scope(Some("refs/heads/recall-port"));
    let zero = port
        .recall_admitted(request(scope.clone(), 60_000_000, false))
        .await
        .expect("zero-result recall");
    assert!(zero.result.is_complete(), "{:?}", zero.result);
    assert!(zero.result.candidates().is_empty());
    let report = zero
        .report
        .expect("zero results still produce an admission report");
    assert_eq!(report.received_count, 0);
    assert_eq!(report.admitted_count, 0);
    assert_eq!(zero.fallback, FallbackDecision::NotApplicable);
    assert_eq!(observer.0.lock().unwrap().len(), 1);

    let mut down = RecallFixturePort::new();
    down.terminal_code = TerminalCode::ProviderUnavailable;
    let observer = Arc::new(LedgerObserver::default());
    let port = mount(
        compose_mode(Arc::new(down), EnabledProviderMode::Active),
        observer.clone(),
    )
    .expect("mounted port");
    let unavailable = port
        .recall_admitted(request(scope, 60_000_000, false))
        .await
        .expect("unavailable recall");
    assert!(!unavailable.result.is_complete());
    assert_eq!(
        unavailable.result.degradation(),
        Some(CognitiveRecallDegradation::Unavailable)
    );
    assert!(unavailable.result.candidates().is_empty());
    assert!(unavailable.report.is_none());
    // The provider's own directive forbade fallback; the host rule was never
    // consulted and nothing else was contacted.
    assert_eq!(
        unavailable.fallback,
        FallbackDecision::Declined(FallbackDeclinedReason::DirectiveForbidden)
    );
    assert!(observer.0.lock().unwrap().is_empty());
}

/// A routing policy that names a provider other than the registered one, or
/// the registered provider under a stale revision, is refused before any
/// handshake or recall reaches the provider. Native facts are not consulted
/// in place of the missing provider.
#[tokio::test]
async fn misconfigured_routing_is_refused_before_any_provider_contact() {
    let provider = Arc::new(RecallFixturePort::new());
    let composition = compose_mode(provider.clone(), EnabledProviderMode::Active);
    let scope = resolved_scope(Some("refs/heads/recall-port"));

    let foreign = mount_routed(
        Arc::clone(&composition),
        Arc::new(LedgerObserver::default()),
        budgets(),
        routing("provider.ncm-local", 31, FallbackRule::Forbidden),
    )
    .expect("mounted port");
    let error = foreign
        .recall_admitted(request(scope.clone(), 60_000_000, false))
        .await
        .expect_err("an unregistered configured provider has no route");
    match error {
        CognitiveRecallPortError::Routing(RoutingError::ProviderNotRegistered { provider_id }) => {
            assert_eq!(provider_id.as_str(), "provider.ncm-local");
        }
        other => panic!("expected a routing refusal, got {other:?}"),
    }

    let stale = mount_routed(
        Arc::clone(&composition),
        Arc::new(LedgerObserver::default()),
        budgets(),
        routing(NATIVE_PROVIDER_ID, 30, FallbackRule::Forbidden),
    )
    .expect("mounted port");
    let error = stale
        .recall_admitted(request(scope, 60_000_000, false))
        .await
        .expect_err("a stale pinned revision has no route");
    match error {
        CognitiveRecallPortError::Routing(RoutingError::RegistrationRevisionMismatch {
            configured,
            registered,
            ..
        }) => {
            assert_eq!((configured, registered), (30, 31));
        }
        other => panic!("expected a revision refusal, got {other:?}"),
    }

    assert_eq!(provider.handshake_calls.load(Ordering::Relaxed), 0);
    assert_eq!(provider.recall_calls.load(Ordering::Relaxed), 0);
    assert_eq!(native_readiness(&composition), ProviderReadiness::NotReady);
}

/// A host-pinned fallback rule is never a substitute for the provider's own
/// directive: the Native provider's failure terminal forbids fallback, so the
/// pinned target is neither handshaken nor called, and the reply stays
/// attributed to the configured provider.
#[tokio::test]
async fn pinned_fallback_rule_alone_never_dispatches_a_second_provider() {
    let mut down = RecallFixturePort::new();
    down.terminal_code = TerminalCode::ProviderUnavailable;
    let down = Arc::new(down);
    let pinned = PinnedFallbackPolicy::new(
        "policy.memory-failover",
        7,
        OwnedProviderId::new("provider.ncm-local").expect("target id"),
    )
    .expect("pinned policy");
    let port = mount_routed(
        compose_mode(down.clone(), EnabledProviderMode::Active),
        Arc::new(LedgerObserver::default()),
        budgets(),
        routing(NATIVE_PROVIDER_ID, 31, FallbackRule::ExplicitPinned(pinned)),
    )
    .expect("mounted port");
    let outcome = port
        .recall_admitted(request(
            resolved_scope(Some("refs/heads/recall-port")),
            60_000_000,
            false,
        ))
        .await
        .expect("unavailable recall");
    assert_eq!(
        outcome.result.degradation(),
        Some(CognitiveRecallDegradation::Unavailable)
    );
    assert_eq!(outcome.result.provider().provider_id(), NATIVE_PROVIDER_ID);
    assert_eq!(
        outcome.fallback,
        FallbackDecision::Declined(FallbackDeclinedReason::DirectiveForbidden)
    );
    assert_eq!(down.handshake_calls.load(Ordering::Relaxed), 1);
    assert_eq!(down.recall_calls.load(Ordering::Relaxed), 1);
}

/// A fallback rule that names the active provider itself is rejected when the
/// policy is built, so no route can loop back to its own source.
#[test]
fn routing_policy_refuses_self_targeting_fallback() {
    let pinned = PinnedFallbackPolicy::new(
        "policy.memory-failover",
        7,
        OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("target id"),
    )
    .expect("pinned policy");
    assert!(
        ActiveRoutingPolicy::new(
            OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("provider id"),
            31,
            FallbackRule::ExplicitPinned(pinned),
        )
        .is_err()
    );
}
