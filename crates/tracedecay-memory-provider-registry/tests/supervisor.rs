//! Behavioral proof of provider lifecycle supervision.
//!
//! Every test here fails against a supervisor that fabricates readiness from
//! a bare `Success` terminal, that lets one supervisor serve two exact
//! scopes, that spawns a replacement before the predecessor is confirmed
//! dead, that treats its backoff as advice, or that lets an adapter panic
//! escape into the host.
#![allow(clippy::expect_used, clippy::panic)]

use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::fmt;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;

use tracedecay_memory_provider_registry::{
    AdapterOperationV1, CancellationToken, CommittedEffectEvidence, DegradationCauseV1,
    DegradationKindV1, FallbackDirective, HandshakeRequest, HandshakeRequestParts,
    HandshakeResponse, OperationControl, OwnedExactScope, OwnedProviderId, OwnedVersionedId,
    PredecessorStateV1, ProviderAvailabilityV1, ProviderDescriptor, ProviderLifecycleAdapterV1,
    ProviderLimits, ProviderOperation, ProviderSupervisorV1, ReadinessDefectV1, ReproveOutcomeV1,
    RestartBudgetV1, ScopeFieldV1, ShutdownBudgetV1, SupervisedScopeV1, SupervisorConfigError,
    SupervisorOutcomeV1, TerminalCode, TerminalRecord,
};

const RESOLVED_SCOPE_DIGEST: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const READY_RECEIPT: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const IMPLEMENTATION_SHA: &str = "2222222222222222222222222222222222222222222222222222222222222222";
const PROVIDER_ID: &str = "supervised.provider";

fn exact_scope(worktree: &str, session: &str) -> OwnedExactScope {
    OwnedExactScope::new(
        "profile-supervisor",
        "project-supervisor",
        "repository-supervisor",
        worktree,
        "refs/heads/supervisor",
        session,
        RESOLVED_SCOPE_DIGEST,
    )
    .expect("exact scope")
}

fn primary_scope() -> OwnedExactScope {
    exact_scope("worktree-primary", "session-primary")
}

fn limits() -> ProviderLimits {
    ProviderLimits {
        request_bytes: 65_536,
        response_bytes: 65_536,
        observation_batch_items: 16,
        recall_candidates: 16,
        concurrent_operations: 4,
        operation_millis: 1_000,
        snapshot_bytes: 65_536,
        inspection_items: 64,
    }
}

fn capabilities() -> Vec<OwnedVersionedId> {
    [
        "provider.health.v1",
        "observation.accept.v1",
        "recall.query.v1",
    ]
    .into_iter()
    .map(|value| OwnedVersionedId::new(value).expect("capability"))
    .collect()
}

fn descriptor() -> ProviderDescriptor {
    ProviderDescriptor::new(
        OwnedProviderId::new(PROVIDER_ID).expect("provider id"),
        IMPLEMENTATION_SHA,
        "native.state.v1",
        7,
        capabilities(),
        limits(),
    )
    .expect("descriptor")
}

fn supervised_scope(scope: OwnedExactScope) -> SupervisedScopeV1 {
    SupervisedScopeV1::new(
        OwnedProviderId::new(PROVIDER_ID).expect("provider id"),
        1,
        scope,
        limits(),
    )
    .expect("supervised scope")
}

fn handshake_request_requiring(required: &[&str]) -> HandshakeRequest {
    handshake_request_parts(primary_scope(), required)
}

fn handshake_request_for(scope: OwnedExactScope) -> HandshakeRequest {
    handshake_request_parts(scope, &["provider.health.v1", "recall.query.v1"])
}

fn handshake_request_parts(scope: OwnedExactScope, required: &[&str]) -> HandshakeRequest {
    HandshakeRequest::new(HandshakeRequestParts {
        provider_id: OwnedProviderId::new(PROVIDER_ID).expect("provider id"),
        registration_revision: 1,
        exact_scope: scope,
        request_id: "supervisor-handshake".to_owned(),
        required_capabilities: required
            .iter()
            .map(|value| OwnedVersionedId::new(*value).expect("capability"))
            .collect(),
        host_limits: limits(),
        control: OperationControl::new(i64::MAX, 1_000, CancellationToken::new()),
        challenge_nonce: [9; 32],
    })
    .expect("handshake request")
}

fn handshake_request() -> HandshakeRequest {
    handshake_request_for(primary_scope())
}

fn terminal(code: TerminalCode, request: &HandshakeRequest) -> TerminalRecord {
    terminal_as(
        code,
        request,
        ProviderOperation::Handshake,
        PROVIDER_ID,
        None,
    )
}

fn terminal_as(
    code: TerminalCode,
    request: &HandshakeRequest,
    operation: ProviderOperation,
    provider_id: &str,
    scope_digest: Option<String>,
) -> TerminalRecord {
    let diagnostic_id = if matches!(
        code,
        TerminalCode::Success | TerminalCode::SuccessZeroResults | TerminalCode::Partial
    ) {
        None
    } else {
        Some("test-diagnostic".to_owned())
    };
    TerminalRecord::new(
        operation,
        OwnedProviderId::new(provider_id).expect("terminal provider"),
        code,
        CommittedEffectEvidence::none(None),
        FallbackDirective::forbidden(),
        "test-operation",
        scope_digest.unwrap_or_else(|| request.exact_scope.exact_scope_sha256()),
        diagnostic_id,
    )
    .expect("terminal")
}

/// The complete, contract-satisfying successful handshake response. Every
/// malformed-success test mutates exactly one field of this.
fn ready_response(request: &HandshakeRequest) -> HandshakeResponse {
    HandshakeResponse {
        terminal: terminal(TerminalCode::Success, request),
        descriptor: Some(descriptor()),
        provider_instance_id: Some("scripted-instance-1".to_owned()),
        state_namespace: Some("scripted-namespace-1".to_owned()),
        accepted_scope: Some(request.exact_scope.clone()),
        effective_limits: Some(limits()),
        ready_receipt_sha256: Some(READY_RECEIPT.to_owned()),
        warnings: Vec::new(),
    }
}

/// What one scripted adapter call does.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Act {
    /// Succeed.
    Ok,
    /// Return the adapter's own typed error.
    Fail,
    /// Panic, which the supervisor must contain.
    Panic,
}

type ResponseScript = Box<dyn Fn(&HandshakeRequest) -> HandshakeResponse>;

/// Scripted adapter. Every call is recorded **in order** so a test can assert
/// exactly what the supervisor did, did not do, and in what sequence — which
/// is what proves predecessor-death-before-respawn rather than merely
/// trusting a return value.
struct ScriptedAdapter {
    start: Cell<Act>,
    handshake: Cell<Act>,
    stop: Cell<Act>,
    stop_confirms: Cell<bool>,
    kill: Cell<Act>,
    response: RefCell<ResponseScript>,
    calls: RefCell<Vec<&'static str>>,
}

impl ScriptedAdapter {
    fn ready() -> Self {
        Self {
            start: Cell::new(Act::Ok),
            handshake: Cell::new(Act::Ok),
            stop: Cell::new(Act::Ok),
            stop_confirms: Cell::new(true),
            kill: Cell::new(Act::Ok),
            response: RefCell::new(Box::new(ready_response)),
            calls: RefCell::new(Vec::new()),
        }
    }

    fn responding(script: ResponseScript) -> Self {
        let adapter = Self::ready();
        *adapter.response.borrow_mut() = script;
        adapter
    }

    fn calls(&self) -> Vec<&'static str> {
        self.calls.borrow().clone()
    }

    fn count(&self, name: &str) -> usize {
        self.calls
            .borrow()
            .iter()
            .filter(|call| **call == name)
            .count()
    }

    fn record(&self, name: &'static str) {
        self.calls.borrow_mut().push(name);
    }
}

#[derive(Debug)]
struct AdapterError(&'static str);

impl fmt::Display for AdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for AdapterError {}

impl ProviderLifecycleAdapterV1 for ScriptedAdapter {
    type Error = AdapterError;

    fn start(&self, _deadline_unix_micros: i64) -> Result<(), Self::Error> {
        self.record("start");
        match self.start.get() {
            Act::Ok => Ok(()),
            Act::Fail => Err(AdapterError("scripted start failure")),
            Act::Panic => panic!("scripted start panic"),
        }
    }

    fn handshake(
        &self,
        request: &HandshakeRequest,
        _deadline_unix_micros: i64,
    ) -> Result<HandshakeResponse, Self::Error> {
        self.record("handshake");
        match self.handshake.get() {
            Act::Ok => Ok((self.response.borrow())(request)),
            Act::Fail => Err(AdapterError("scripted handshake transport failure")),
            Act::Panic => panic!("scripted handshake panic"),
        }
    }

    fn request_stop(&self, _deadline_unix_micros: i64) -> Result<bool, Self::Error> {
        self.record("request_stop");
        match self.stop.get() {
            Act::Ok => Ok(self.stop_confirms.get()),
            Act::Fail => Err(AdapterError("scripted stop failure")),
            Act::Panic => panic!("scripted stop panic"),
        }
    }

    fn kill(&self, _deadline_unix_micros: i64) -> Result<(), Self::Error> {
        self.record("kill");
        match self.kill.get() {
            Act::Ok => Ok(()),
            Act::Fail => Err(AdapterError("scripted kill failure")),
            Act::Panic => panic!("scripted kill panic"),
        }
    }
}

fn restart_budget(max_attempts_per_window: u32) -> RestartBudgetV1 {
    RestartBudgetV1 {
        max_attempts_per_window,
        window_micros: 60_000_000,
        backoff_base_micros: 1_000,
        backoff_max_micros: 8_000,
    }
}

fn shutdown_budget() -> ShutdownBudgetV1 {
    ShutdownBudgetV1 {
        grace_micros: 5_000,
        kill_micros: 2_000,
    }
}

fn supervisor(adapter: ScriptedAdapter, attempts: u32) -> ProviderSupervisorV1<ScriptedAdapter> {
    ProviderSupervisorV1::new(
        adapter,
        supervised_scope(primary_scope()),
        restart_budget(attempts),
        shutdown_budget(),
    )
    .expect("supervisor")
}

fn expect_defect<E: fmt::Debug>(outcome: SupervisorOutcomeV1<E>) -> ReadinessDefectV1 {
    match outcome {
        SupervisorOutcomeV1::Unavailable(DegradationCauseV1::HandshakeContractViolation(
            defect,
        )) => defect,
        other => panic!("expected a handshake contract violation, got {other:?}"),
    }
}

/// Runs `body` with the panic hook silenced, so a deliberately panicking
/// adapter does not litter test output. Restores whatever hook was set.
fn without_panic_noise(body: impl FnOnce()) {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    body();
    std::panic::set_hook(previous);
}

// ---------------------------------------------------------------------------
// Readiness is a fully validated handshake, never a bare `Success`.
// ---------------------------------------------------------------------------

/// A complete, contract-satisfying handshake reaches `Ready` and yields
/// validated evidence with every field present. Catches a supervisor that
/// fabricates readiness from process start alone.
#[test]
fn complete_handshake_reaches_ready_with_validated_evidence() {
    let mut supervisor = supervisor(ScriptedAdapter::ready(), 3);
    assert_eq!(
        supervisor.current_availability(),
        ProviderAvailabilityV1::NotStarted
    );

    match supervisor.start_or_restart(&handshake_request(), 0, 1_000, 2_000) {
        SupervisorOutcomeV1::Ready(evidence) => {
            assert_eq!(evidence.provider_instance_id(), "scripted-instance-1");
            assert_eq!(evidence.state_namespace(), "scripted-namespace-1");
            assert_eq!(evidence.ready_receipt_sha256(), READY_RECEIPT);
            assert_eq!(
                evidence.implementation_identity_sha256(),
                IMPLEMENTATION_SHA
            );
            assert_eq!(evidence.state_schema_version(), "native.state.v1");
            assert_eq!(evidence.state_generation(), 7);
            assert_eq!(evidence.effective_limits(), limits());
        }
        SupervisorOutcomeV1::Unavailable(cause) => panic!("unexpected unavailable: {cause}"),
    }
    assert_eq!(
        supervisor.current_availability(),
        ProviderAvailabilityV1::Ready
    );
    assert!(supervisor.current_degradation().is_none());
    assert_eq!(supervisor.predecessor_state(), PredecessorStateV1::Live);
}

/// A `Success` terminal with no reported instance identity is refused rather
/// than defaulted to an empty successful identity. Catches the
/// `unwrap_or_default` promotion.
#[test]
fn success_without_instance_identity_is_refused() {
    let mut supervisor = supervisor(
        ScriptedAdapter::responding(Box::new(|request| HandshakeResponse {
            provider_instance_id: None,
            ..ready_response(request)
        })),
        3,
    );
    let defect = expect_defect(supervisor.start_or_restart(&handshake_request(), 0, 1, 2));
    assert_eq!(defect, ReadinessDefectV1::MissingInstanceIdentity);
    assert_eq!(
        supervisor.current_availability(),
        ProviderAvailabilityV1::Unavailable
    );
    assert!(supervisor.ready_evidence().is_none());
    assert_eq!(
        supervisor.current_degradation().map(|record| record.kind()),
        Some(DegradationKindV1::HandshakeContractViolation)
    );
}

/// An empty instance identity is not an identity. Catches a validator that
/// only checks `Option::is_some`.
#[test]
fn success_with_empty_instance_identity_is_refused() {
    let mut supervisor = supervisor(
        ScriptedAdapter::responding(Box::new(|request| HandshakeResponse {
            provider_instance_id: Some(String::new()),
            ..ready_response(request)
        })),
        3,
    );
    let defect = expect_defect(supervisor.start_or_restart(&handshake_request(), 0, 1, 2));
    assert_eq!(defect, ReadinessDefectV1::InvalidInstanceIdentity);
}

/// A missing ready receipt cannot be promoted: every downstream provider call
/// carries that receipt, so an absent one means no admitted call exists.
#[test]
fn success_without_ready_receipt_is_refused() {
    let mut supervisor = supervisor(
        ScriptedAdapter::responding(Box::new(|request| HandshakeResponse {
            ready_receipt_sha256: None,
            ..ready_response(request)
        })),
        3,
    );
    assert_eq!(
        expect_defect(supervisor.start_or_restart(&handshake_request(), 0, 1, 2)),
        ReadinessDefectV1::MissingReadyReceipt
    );
}

/// A ready receipt that is not a bare lowercase 64-hex digest is refused
/// rather than stored and later rejected by the journal.
#[test]
fn success_with_malformed_ready_receipt_is_refused() {
    let mut supervisor = supervisor(
        ScriptedAdapter::responding(Box::new(|request| HandshakeResponse {
            ready_receipt_sha256: Some(format!("sha256:{READY_RECEIPT}")),
            ..ready_response(request)
        })),
        3,
    );
    assert_eq!(
        expect_defect(supervisor.start_or_restart(&handshake_request(), 0, 1, 2)),
        ReadinessDefectV1::InvalidReadyReceipt
    );
}

/// A terminal naming another provider is a foreign terminal, not this
/// incarnation's readiness.
#[test]
fn success_terminal_from_a_foreign_provider_is_refused() {
    let mut supervisor = supervisor(
        ScriptedAdapter::responding(Box::new(|request| HandshakeResponse {
            terminal: terminal_as(
                TerminalCode::Success,
                request,
                ProviderOperation::Handshake,
                "other.provider",
                None,
            ),
            ..ready_response(request)
        })),
        3,
    );
    assert_eq!(
        expect_defect(supervisor.start_or_restart(&handshake_request(), 0, 1, 2)),
        ReadinessDefectV1::ForeignTerminalProvider {
            terminal_provider_id: "other.provider".to_owned(),
        }
    );
}

/// A terminal naming another operation did not answer the handshake.
#[test]
fn success_terminal_for_a_foreign_operation_is_refused() {
    let mut supervisor = supervisor(
        ScriptedAdapter::responding(Box::new(|request| HandshakeResponse {
            terminal: terminal_as(
                TerminalCode::Success,
                request,
                ProviderOperation::Health,
                PROVIDER_ID,
                None,
            ),
            ..ready_response(request)
        })),
        3,
    );
    assert_eq!(
        expect_defect(supervisor.start_or_restart(&handshake_request(), 0, 1, 2)),
        ReadinessDefectV1::ForeignTerminalOperation {
            terminal_operation: "health",
        }
    );
}

/// A terminal carrying a different exact-scope digest answered for another
/// scope. Catches a supervisor that trusts the terminal code alone.
#[test]
fn success_terminal_with_a_foreign_scope_digest_is_refused() {
    let foreign = exact_scope("worktree-other", "session-other").exact_scope_sha256();
    let mut supervisor = supervisor(
        ScriptedAdapter::responding(Box::new(move |request| HandshakeResponse {
            terminal: terminal_as(
                TerminalCode::Success,
                request,
                ProviderOperation::Handshake,
                PROVIDER_ID,
                Some(foreign.clone()),
            ),
            ..ready_response(request)
        })),
        3,
    );
    assert_eq!(
        expect_defect(supervisor.start_or_restart(&handshake_request(), 0, 1, 2)),
        ReadinessDefectV1::TerminalScopeMismatch
    );
}

/// A provider that accepts a *different* exact scope than the one requested
/// is refused, naming the field it changed.
#[test]
fn success_accepting_a_foreign_scope_is_refused() {
    let mut supervisor = supervisor(
        ScriptedAdapter::responding(Box::new(|request| HandshakeResponse {
            accepted_scope: Some(exact_scope("worktree-primary", "session-other")),
            ..ready_response(request)
        })),
        3,
    );
    assert_eq!(
        expect_defect(supervisor.start_or_restart(&handshake_request(), 0, 1, 2)),
        ReadinessDefectV1::AcceptedScopeMismatch {
            field: ScopeFieldV1::AgentSessionId,
        }
    );
}

/// A success with no accepted scope proved nothing about scope ownership.
#[test]
fn success_without_accepted_scope_is_refused() {
    let mut supervisor = supervisor(
        ScriptedAdapter::responding(Box::new(|request| HandshakeResponse {
            accepted_scope: None,
            ..ready_response(request)
        })),
        3,
    );
    assert_eq!(
        expect_defect(supervisor.start_or_restart(&handshake_request(), 0, 1, 2)),
        ReadinessDefectV1::MissingAcceptedScope
    );
}

/// A success with no state namespace has no proven loaded-state ownership.
#[test]
fn success_without_state_namespace_is_refused() {
    let mut supervisor = supervisor(
        ScriptedAdapter::responding(Box::new(|request| HandshakeResponse {
            state_namespace: None,
            ..ready_response(request)
        })),
        3,
    );
    assert_eq!(
        expect_defect(supervisor.start_or_restart(&handshake_request(), 0, 1, 2)),
        ReadinessDefectV1::MissingStateNamespace
    );
}

/// A success with no descriptor carries no build, state, or capability
/// identity to verify, so readiness is refused fail-closed.
#[test]
fn success_without_descriptor_is_refused() {
    let mut supervisor = supervisor(
        ScriptedAdapter::responding(Box::new(|request| HandshakeResponse {
            descriptor: None,
            ..ready_response(request)
        })),
        3,
    );
    assert_eq!(
        expect_defect(supervisor.start_or_restart(&handshake_request(), 0, 1, 2)),
        ReadinessDefectV1::MissingDescriptor
    );
}

/// A descriptor missing a *mandatory* capability is not a valid contract
/// value at all, so readiness is refused before anything else is inspected.
#[test]
fn success_with_a_descriptor_missing_a_mandatory_capability_is_refused() {
    let mut supervisor = supervisor(
        ScriptedAdapter::responding(Box::new(|request| {
            let mut descriptor = descriptor();
            descriptor.capabilities = capabilities()
                .into_iter()
                .filter(|capability| capability.as_str() != "recall.query.v1")
                .collect::<BTreeSet<_>>();
            HandshakeResponse {
                descriptor: Some(descriptor),
                ..ready_response(request)
            }
        })),
        3,
    );
    assert!(matches!(
        expect_defect(supervisor.start_or_restart(&handshake_request(), 0, 1, 2)),
        ReadinessDefectV1::InvalidDescriptor { .. }
    ));
}

/// A descriptor that does not declare a capability *this host required in
/// this request* cannot serve the host's calls, so readiness is refused
/// naming that capability. Catches a supervisor that never compares the
/// negotiated capability set against the request.
#[test]
fn success_missing_a_host_required_capability_is_refused() {
    let request = handshake_request_requiring(&["provider.health.v1", "feedback.record.v1"]);
    let mut supervisor = supervisor(ScriptedAdapter::ready(), 3);
    assert_eq!(
        expect_defect(supervisor.start_or_restart(&request, 0, 1, 2)),
        ReadinessDefectV1::MissingRequiredCapability {
            capability_id: "feedback.record.v1".to_owned(),
        }
    );
}

/// A descriptor naming another provider is refused even when the terminal is
/// correct.
#[test]
fn success_with_a_foreign_descriptor_provider_is_refused() {
    let mut supervisor = supervisor(
        ScriptedAdapter::responding(Box::new(|request| {
            let mut descriptor = descriptor();
            descriptor.provider_id =
                OwnedProviderId::new("other.provider").expect("other provider");
            HandshakeResponse {
                descriptor: Some(descriptor),
                ..ready_response(request)
            }
        })),
        3,
    );
    assert_eq!(
        expect_defect(supervisor.start_or_restart(&handshake_request(), 0, 1, 2)),
        ReadinessDefectV1::DescriptorProviderMismatch {
            descriptor_provider_id: "other.provider".to_owned(),
        }
    );
}

/// A provider may only negotiate limits *down*. One negotiated above the
/// host's own ceiling is refused, naming the limit.
#[test]
fn success_negotiating_a_limit_above_the_host_ceiling_is_refused() {
    let mut supervisor = supervisor(
        ScriptedAdapter::responding(Box::new(|request| {
            let mut effective = limits();
            effective.recall_candidates = limits().recall_candidates + 1;
            HandshakeResponse {
                effective_limits: Some(effective),
                ..ready_response(request)
            }
        })),
        3,
    );
    assert_eq!(
        expect_defect(supervisor.start_or_restart(&handshake_request(), 0, 1, 2)),
        ReadinessDefectV1::EffectiveLimitAboveHostCeiling {
            limit: "recall_candidates",
        }
    );
}

/// A success with no negotiated limits leaves every downstream call
/// unbounded, so it is refused.
#[test]
fn success_without_effective_limits_is_refused() {
    let mut supervisor = supervisor(
        ScriptedAdapter::responding(Box::new(|request| HandshakeResponse {
            effective_limits: None,
            ..ready_response(request)
        })),
        3,
    );
    assert_eq!(
        expect_defect(supervisor.start_or_restart(&handshake_request(), 0, 1, 2)),
        ReadinessDefectV1::MissingEffectiveLimits
    );
}

/// A pinned build identity that the provider does not report is a
/// fail-closed mismatch, not a warning.
#[test]
fn success_reporting_an_unpinned_build_identity_is_refused() {
    let scope = supervised_scope(primary_scope()).with_pinned_identity(
        Some("3333333333333333333333333333333333333333333333333333333333333333".to_owned()),
        None,
    );
    let mut supervisor = ProviderSupervisorV1::new(
        ScriptedAdapter::ready(),
        scope,
        restart_budget(3),
        shutdown_budget(),
    )
    .expect("supervisor");
    assert_eq!(
        expect_defect(supervisor.start_or_restart(&handshake_request(), 0, 1, 2)),
        ReadinessDefectV1::PinnedIdentityMismatch {
            reported: IMPLEMENTATION_SHA.to_owned(),
        }
    );
}

/// An unbounded warning list on a successful handshake is refused rather
/// than stored.
#[test]
fn success_with_unbounded_warnings_is_refused() {
    let mut supervisor = supervisor(
        ScriptedAdapter::responding(Box::new(|request| HandshakeResponse {
            warnings: (0..64).map(|index| format!("warning-{index}")).collect(),
            ..ready_response(request)
        })),
        3,
    );
    assert_eq!(
        expect_defect(supervisor.start_or_restart(&handshake_request(), 0, 1, 2)),
        ReadinessDefectV1::UnboundedWarnings { warnings: 64 }
    );
}

/// A non-success terminal is typed degradation carrying the provider's own
/// terminal code.
#[test]
fn refused_handshake_reports_the_provider_terminal_code() {
    let mut supervisor = supervisor(
        ScriptedAdapter::responding(Box::new(|request| HandshakeResponse {
            terminal: terminal(TerminalCode::ScopeUnavailable, request),
            descriptor: None,
            provider_instance_id: None,
            state_namespace: None,
            accepted_scope: None,
            effective_limits: None,
            ready_receipt_sha256: None,
            warnings: Vec::new(),
        })),
        3,
    );
    match supervisor.start_or_restart(&handshake_request(), 0, 1, 2) {
        SupervisorOutcomeV1::Unavailable(DegradationCauseV1::HandshakeRefused {
            terminal_code,
        }) => assert_eq!(terminal_code, TerminalCode::ScopeUnavailable),
        other => panic!("expected a refused handshake, got {other:?}"),
    }
    assert!(supervisor.ready_evidence().is_none());
}

// ---------------------------------------------------------------------------
// One owner, one exact scope.
// ---------------------------------------------------------------------------

/// A request for a second worktree is refused before the adapter is touched
/// at all. Catches a supervisor that would start under one exact scope and
/// restart under another.
#[test]
fn a_request_for_a_second_worktree_never_reaches_the_adapter() {
    let mut supervisor = supervisor(ScriptedAdapter::ready(), 3);
    let foreign = handshake_request_for(exact_scope("worktree-secondary", "session-primary"));

    match supervisor.start_or_restart(&foreign, 0, 1, 2) {
        SupervisorOutcomeV1::Unavailable(DegradationCauseV1::ScopeMismatch { field }) => {
            assert_eq!(field, ScopeFieldV1::WorktreeIdentity);
        }
        other => panic!("expected a scope mismatch, got {other:?}"),
    }
    assert!(supervisor.adapter().calls().is_empty());
    assert_eq!(
        supervisor.current_availability(),
        ProviderAvailabilityV1::Unavailable
    );
    assert_eq!(
        supervisor.current_degradation().map(|record| record.kind()),
        Some(DegradationKindV1::ScopeMismatch)
    );

    // The owner still serves its own scope afterwards: a foreign request is
    // refused, not fatal.
    assert!(matches!(
        supervisor.start_or_restart(&handshake_request(), 0, 1, 2),
        SupervisorOutcomeV1::Ready(_)
    ));
}

/// A request naming another registration revision is refused before contact,
/// because the revision is part of the owner's identity.
#[test]
fn a_request_for_another_registration_revision_never_reaches_the_adapter() {
    let mut supervisor = supervisor(ScriptedAdapter::ready(), 3);
    let mut foreign = handshake_request();
    foreign.registration_revision = 2;
    match supervisor.start_or_restart(&foreign, 0, 1, 2) {
        SupervisorOutcomeV1::Unavailable(DegradationCauseV1::ScopeMismatch { field }) => {
            assert_eq!(field, ScopeFieldV1::RegistrationRevision);
        }
        other => panic!("expected a scope mismatch, got {other:?}"),
    }
    assert!(supervisor.adapter().calls().is_empty());
}

// ---------------------------------------------------------------------------
// No overlapping owners: predecessor death before replacement.
// ---------------------------------------------------------------------------

/// A restart confirms the predecessor's death before the replacement spawn.
/// Catches a supervisor that calls `start` twice with no termination between.
#[test]
fn a_restart_confirms_predecessor_death_before_the_replacement_spawn() {
    let mut supervisor = supervisor(ScriptedAdapter::ready(), 3);
    assert!(matches!(
        supervisor.start_or_restart(&handshake_request(), 0, 1, 2),
        SupervisorOutcomeV1::Ready(_)
    ));
    // Far enough past the armed backoff to be admitted.
    assert!(matches!(
        supervisor.start_or_restart(&handshake_request(), 1_000_000, 1, 2),
        SupervisorOutcomeV1::Ready(_)
    ));
    assert_eq!(
        supervisor.adapter().calls(),
        vec!["start", "handshake", "request_stop", "start", "handshake"]
    );
}

/// A failed start may still have spawned a child, so the next pass terminates
/// before it spawns again. Catches the "start error left no owner" assumption
/// that produces two live owners for one namespace.
#[test]
fn a_failed_start_is_terminated_before_the_next_spawn() {
    let mut supervisor = supervisor(ScriptedAdapter::ready(), 3);
    supervisor.adapter().start.set(Act::Fail);
    match supervisor.start_or_restart(&handshake_request(), 0, 1, 2) {
        SupervisorOutcomeV1::Unavailable(DegradationCauseV1::StartFailed(_)) => {}
        other => panic!("expected a start failure, got {other:?}"),
    }
    assert_eq!(supervisor.predecessor_state(), PredecessorStateV1::Live);

    supervisor.adapter().start.set(Act::Ok);
    assert!(matches!(
        supervisor.start_or_restart(&handshake_request(), 1_000_000, 1, 2),
        SupervisorOutcomeV1::Ready(_)
    ));
    assert_eq!(
        supervisor.adapter().calls(),
        vec!["start", "request_stop", "start", "handshake"]
    );
}

/// A graceful stop that does not confirm death escalates to a forced kill
/// before the replacement spawn, never straight to a second `start`.
#[test]
fn an_unconfirmed_graceful_stop_escalates_to_kill_before_respawn() {
    let mut supervisor = supervisor(ScriptedAdapter::ready(), 3);
    assert!(matches!(
        supervisor.start_or_restart(&handshake_request(), 0, 1, 2),
        SupervisorOutcomeV1::Ready(_)
    ));
    supervisor.adapter().stop_confirms.set(false);
    assert!(matches!(
        supervisor.start_or_restart(&handshake_request(), 1_000_000, 1, 2),
        SupervisorOutcomeV1::Ready(_)
    ));
    assert_eq!(
        supervisor.adapter().calls(),
        vec![
            "start",
            "handshake",
            "request_stop",
            "kill",
            "start",
            "handshake"
        ]
    );
}

/// A termination that cannot be confirmed refuses every further spawn until
/// death is actually confirmed. Catches a supervisor that respawns over an
/// instance it never proved dead.
#[test]
fn an_unconfirmed_termination_refuses_every_replacement_spawn() {
    let mut supervisor = supervisor(ScriptedAdapter::ready(), 8);
    assert!(matches!(
        supervisor.start_or_restart(&handshake_request(), 0, 1, 2),
        SupervisorOutcomeV1::Ready(_)
    ));
    supervisor.adapter().stop.set(Act::Fail);

    match supervisor.start_or_restart(&handshake_request(), 1_000_000, 1, 2) {
        SupervisorOutcomeV1::Unavailable(DegradationCauseV1::PredecessorTerminationFailed(_)) => {}
        other => panic!("expected a termination failure, got {other:?}"),
    }
    assert_eq!(
        supervisor.predecessor_state(),
        PredecessorStateV1::DeathUnknown
    );

    // A second pass reconciles by kill only; it must not spawn while death is
    // still unknown.
    supervisor.adapter().kill.set(Act::Fail);
    match supervisor.start_or_restart(&handshake_request(), 2_000_000, 1, 2) {
        SupervisorOutcomeV1::Unavailable(DegradationCauseV1::PredecessorDeathUnknown) => {}
        other => panic!("expected an unknown predecessor death, got {other:?}"),
    }
    assert_eq!(supervisor.adapter().count("start"), 1);
    assert_eq!(
        supervisor.adapter().calls(),
        vec!["start", "handshake", "request_stop", "kill"]
    );

    // Once the kill confirms death, and only then, a replacement spawns.
    supervisor.adapter().kill.set(Act::Ok);
    assert!(matches!(
        supervisor.start_or_restart(&handshake_request(), 3_000_000, 1, 2),
        SupervisorOutcomeV1::Ready(_)
    ));
    assert_eq!(
        supervisor.adapter().calls(),
        vec![
            "start",
            "handshake",
            "request_stop",
            "kill",
            "kill",
            "start",
            "handshake"
        ]
    );
}

/// A reported crash does not assume death: the next replacement still
/// confirms it first.
#[test]
fn a_reported_crash_still_requires_confirmed_death_before_respawn() {
    let mut supervisor = supervisor(ScriptedAdapter::ready(), 3);
    assert!(matches!(
        supervisor.start_or_restart(&handshake_request(), 0, 1, 2),
        SupervisorOutcomeV1::Ready(_)
    ));
    match supervisor.report_crash() {
        SupervisorOutcomeV1::Unavailable(DegradationCauseV1::Crashed) => {}
        other => panic!("expected a typed crash, got {other:?}"),
    }
    assert_eq!(
        supervisor.current_degradation().map(|record| record.kind()),
        Some(DegradationKindV1::Crashed)
    );
    assert!(supervisor.ready_evidence().is_none());
    assert_eq!(supervisor.predecessor_state(), PredecessorStateV1::Live);

    assert!(matches!(
        supervisor.start_or_restart(&handshake_request(), 1_000_000, 1, 2),
        SupervisorOutcomeV1::Ready(_)
    ));
    assert_eq!(
        supervisor.adapter().calls(),
        vec!["start", "handshake", "request_stop", "start", "handshake"]
    );
}

// ---------------------------------------------------------------------------
// Bounded restart: enforced, not advisory.
// ---------------------------------------------------------------------------

/// Two passes at the same instant cannot both reach the adapter: the second
/// is refused by the enforced backoff. Catches a caller that consumes the
/// whole restart budget in a tight loop at one instant.
#[test]
fn a_second_pass_at_the_same_instant_is_refused_by_the_enforced_backoff() {
    let mut supervisor = supervisor(ScriptedAdapter::ready(), 5);
    supervisor.adapter().handshake.set(Act::Fail);
    match supervisor.start_or_restart(&handshake_request(), 0, 1, 2) {
        SupervisorOutcomeV1::Unavailable(DegradationCauseV1::HandshakeTransportFailed(_)) => {}
        other => panic!("expected a transport failure, got {other:?}"),
    }
    match supervisor.start_or_restart(&handshake_request(), 0, 1, 2) {
        SupervisorOutcomeV1::Unavailable(DegradationCauseV1::BackoffNotElapsed {
            retry_at_unix_micros,
            remaining_micros,
        }) => {
            assert_eq!(retry_at_unix_micros, 1_000);
            assert_eq!(remaining_micros, 1_000);
        }
        other => panic!("expected an enforced backoff refusal, got {other:?}"),
    }
    assert_eq!(supervisor.adapter().count("start"), 1);
    assert_eq!(supervisor.adapter().count("handshake"), 1);
}

/// The backoff grows exponentially and each step is enforced: an attempt one
/// microsecond early is refused, and the same attempt at the eligible instant
/// is admitted.
#[test]
fn the_enforced_backoff_grows_exponentially() {
    let mut supervisor = supervisor(ScriptedAdapter::ready(), 5);
    supervisor.adapter().handshake.set(Act::Fail);
    let _ = supervisor.start_or_restart(&handshake_request(), 0, 1, 2);
    assert_eq!(
        supervisor.next_restart_eligible_at_unix_micros(),
        Some(1_000)
    );

    // Second pass admitted at its eligible instant; it arms a doubled delay.
    let _ = supervisor.start_or_restart(&handshake_request(), 1_000, 1, 2);
    assert_eq!(
        supervisor.next_restart_eligible_at_unix_micros(),
        Some(1_000 + 2_000)
    );
    assert!(matches!(
        supervisor.start_or_restart(&handshake_request(), 2_999, 1, 2),
        SupervisorOutcomeV1::Unavailable(DegradationCauseV1::BackoffNotElapsed { .. })
    ));
    assert_eq!(supervisor.adapter().count("start"), 2);
    assert_eq!(supervisor.next_restart_delay_micros(2_999), Some(1));
}

/// The rolling window's attempt ceiling stops spawning entirely, and the
/// refusal names the attempts already spent.
#[test]
fn the_restart_budget_ceiling_stops_spawning() {
    let mut supervisor = supervisor(ScriptedAdapter::ready(), 2);
    supervisor.adapter().handshake.set(Act::Fail);
    let _ = supervisor.start_or_restart(&handshake_request(), 0, 1, 2);
    let _ = supervisor.start_or_restart(&handshake_request(), 1_000, 1, 2);
    match supervisor.start_or_restart(&handshake_request(), 100_000, 1, 2) {
        SupervisorOutcomeV1::Unavailable(DegradationCauseV1::RestartBudgetExhausted {
            attempts_in_window,
        }) => assert_eq!(attempts_in_window, 2),
        other => panic!("expected budget exhaustion, got {other:?}"),
    }
    assert_eq!(supervisor.adapter().count("start"), 2);
    assert_eq!(supervisor.next_restart_delay_micros(100_000), None);

    // Once the attempts age out of the window the supervisor spawns again:
    // the ceiling bounds a hot loop, it does not wedge the provider forever.
    supervisor.adapter().handshake.set(Act::Ok);
    assert!(matches!(
        supervisor.start_or_restart(&handshake_request(), 61_000_000, 1, 2),
        SupervisorOutcomeV1::Ready(_)
    ));
}

/// A validated readiness clears the armed pacing, so a healthy provider is
/// not made to wait out a backoff it never earned.
#[test]
fn a_validated_readiness_clears_the_armed_backoff() {
    let mut supervisor = supervisor(ScriptedAdapter::ready(), 3);
    assert!(matches!(
        supervisor.start_or_restart(&handshake_request(), 0, 1, 2),
        SupervisorOutcomeV1::Ready(_)
    ));
    assert_eq!(supervisor.next_restart_eligible_at_unix_micros(), None);
    assert_eq!(supervisor.next_restart_delay_micros(0), Some(0));
}

// ---------------------------------------------------------------------------
// Readiness re-proof is not a restart.
// ---------------------------------------------------------------------------

/// Re-proving a `Ready` incarnation calls only `handshake`, spends no restart
/// attempt, and arms no backoff. Catches a host that proves readiness per
/// request and thereby burns the crash-loop budget on a healthy provider.
#[test]
fn a_readiness_reproof_spends_no_restart_budget() {
    let mut supervisor = supervisor(ScriptedAdapter::ready(), 1);
    assert!(matches!(
        supervisor.start_or_restart(&handshake_request(), 0, 1, 2),
        SupervisorOutcomeV1::Ready(_)
    ));

    for _ in 0..5 {
        match supervisor.reprove_readiness(&handshake_request(), 2) {
            ReproveOutcomeV1::Ready(evidence) => {
                assert_eq!(evidence.provider_instance_id(), "scripted-instance-1");
            }
            other => panic!("expected a re-proved readiness, got {other:?}"),
        }
    }
    assert_eq!(supervisor.adapter().count("start"), 1);
    assert_eq!(supervisor.adapter().count("handshake"), 6);
    assert_eq!(supervisor.next_restart_eligible_at_unix_micros(), None);
    // The single-attempt budget is still unspent beyond the one real spawn.
    assert_eq!(supervisor.next_restart_delay_micros(0), None);
    assert_eq!(
        supervisor.current_availability(),
        ProviderAvailabilityV1::Ready
    );
}

/// A supervisor that is not `Ready` has nothing to re-prove, and says so
/// without contacting the adapter.
#[test]
fn a_reproof_before_readiness_never_reaches_the_adapter() {
    let mut supervisor = supervisor(ScriptedAdapter::ready(), 3);
    assert!(matches!(
        supervisor.reprove_readiness(&handshake_request(), 2),
        ReproveOutcomeV1::NotReady
    ));
    assert!(supervisor.adapter().calls().is_empty());
}

/// A re-proof for a foreign exact scope is refused typed, without contacting
/// the adapter, even while the supervisor is `Ready`.
#[test]
fn a_reproof_for_a_foreign_scope_never_reaches_the_adapter() {
    let mut supervisor = supervisor(ScriptedAdapter::ready(), 3);
    let _ = supervisor.start_or_restart(&handshake_request(), 0, 1, 2);
    let foreign = handshake_request_for(exact_scope("worktree-secondary", "session-primary"));
    match supervisor.reprove_readiness(&foreign, 2) {
        ReproveOutcomeV1::Unavailable(DegradationCauseV1::ScopeMismatch { field }) => {
            assert_eq!(field, ScopeFieldV1::WorktreeIdentity);
        }
        other => panic!("expected a scope mismatch, got {other:?}"),
    }
    assert_eq!(supervisor.adapter().count("handshake"), 1);
}

/// A failed re-proof invalidates readiness, persists the typed degradation,
/// and leaves the instance possibly-live so the next replacement confirms its
/// death before spawning.
#[test]
fn a_failed_reproof_invalidates_readiness_and_keeps_a_live_predecessor() {
    let mut supervisor = supervisor(ScriptedAdapter::ready(), 3);
    let _ = supervisor.start_or_restart(&handshake_request(), 0, 1, 2);
    supervisor.adapter().handshake.set(Act::Fail);
    match supervisor.reprove_readiness(&handshake_request(), 2) {
        ReproveOutcomeV1::Unavailable(DegradationCauseV1::HandshakeTransportFailed(_)) => {}
        other => panic!("expected a transport failure, got {other:?}"),
    }
    assert!(supervisor.ready_evidence().is_none());
    assert_eq!(
        supervisor.current_availability(),
        ProviderAvailabilityV1::Unavailable
    );
    assert_eq!(supervisor.predecessor_state(), PredecessorStateV1::Live);

    supervisor.adapter().handshake.set(Act::Ok);
    assert!(matches!(
        supervisor.start_or_restart(&handshake_request(), 1_000_000, 1, 2),
        SupervisorOutcomeV1::Ready(_)
    ));
    assert_eq!(
        supervisor.adapter().calls(),
        vec![
            "start",
            "handshake",
            "handshake",
            "request_stop",
            "start",
            "handshake"
        ]
    );
}

// ---------------------------------------------------------------------------
// Crash and panic isolation.
// ---------------------------------------------------------------------------

/// An adapter that panics inside `handshake` is contained: the host keeps
/// running and observes typed degradation naming the call that panicked.
#[test]
fn an_adapter_panic_in_handshake_is_contained_and_typed() {
    without_panic_noise(|| {
        let mut supervisor = supervisor(ScriptedAdapter::ready(), 3);
        supervisor.adapter().handshake.set(Act::Panic);
        match supervisor.start_or_restart(&handshake_request(), 0, 1, 2) {
            SupervisorOutcomeV1::Unavailable(DegradationCauseV1::AdapterPanicked { operation }) => {
                assert_eq!(operation, AdapterOperationV1::Handshake)
            }
            other => panic!("expected a contained panic, got {other:?}"),
        }
        assert_eq!(
            supervisor.current_degradation().map(|record| record.kind()),
            Some(DegradationKindV1::AdapterPanicked)
        );

        // The host is still usable: the same supervisor serves the next pass.
        supervisor.adapter().handshake.set(Act::Ok);
        assert!(matches!(
            supervisor.start_or_restart(&handshake_request(), 1_000_000, 1, 2),
            SupervisorOutcomeV1::Ready(_)
        ));
    });
}

/// An adapter that panics inside `start` is contained, and the possibly-live
/// child is terminated before the next spawn.
#[test]
fn an_adapter_panic_in_start_is_contained_and_leaves_a_live_predecessor() {
    without_panic_noise(|| {
        let mut supervisor = supervisor(ScriptedAdapter::ready(), 3);
        supervisor.adapter().start.set(Act::Panic);
        match supervisor.start_or_restart(&handshake_request(), 0, 1, 2) {
            SupervisorOutcomeV1::Unavailable(DegradationCauseV1::AdapterPanicked { operation }) => {
                assert_eq!(operation, AdapterOperationV1::Start)
            }
            other => panic!("expected a contained panic, got {other:?}"),
        }
        assert_eq!(supervisor.predecessor_state(), PredecessorStateV1::Live);

        supervisor.adapter().start.set(Act::Ok);
        assert!(matches!(
            supervisor.start_or_restart(&handshake_request(), 1_000_000, 1, 2),
            SupervisorOutcomeV1::Ready(_)
        ));
        assert_eq!(
            supervisor.adapter().calls(),
            vec!["start", "request_stop", "start", "handshake"]
        );
    });
}

// ---------------------------------------------------------------------------
// Bounded shutdown.
// ---------------------------------------------------------------------------

/// Shutdown escalates to a forced kill only after the grace budget elapses
/// without a confirmed stop.
#[test]
fn shutdown_escalates_to_kill_only_after_the_grace_budget() {
    let mut supervisor = supervisor(ScriptedAdapter::ready(), 3);
    let _ = supervisor.start_or_restart(&handshake_request(), 0, 1, 2);

    supervisor.adapter().stop_confirms.set(false);
    let report = supervisor.shutdown(10).expect("shutdown");
    assert!(report.escalated_to_kill);
    assert!(report.confirmed_dead);
    assert_eq!(
        supervisor.current_availability(),
        ProviderAvailabilityV1::NotStarted
    );
    assert_eq!(supervisor.predecessor_state(), PredecessorStateV1::None);
    assert_eq!(
        supervisor.adapter().calls(),
        vec!["start", "handshake", "request_stop", "kill"]
    );
}

/// A confirmed graceful stop never escalates.
#[test]
fn a_confirmed_graceful_stop_never_escalates() {
    let mut supervisor = supervisor(ScriptedAdapter::ready(), 3);
    let _ = supervisor.start_or_restart(&handshake_request(), 0, 1, 2);
    let report = supervisor.shutdown(10).expect("shutdown");
    assert!(!report.escalated_to_kill);
    assert_eq!(supervisor.adapter().count("kill"), 0);
}

/// A shutdown that cannot confirm death is a typed failure that leaves the
/// predecessor unknown, so no later pass spawns over it.
#[test]
fn a_shutdown_that_cannot_confirm_death_blocks_later_spawns() {
    let mut supervisor = supervisor(ScriptedAdapter::ready(), 5);
    let _ = supervisor.start_or_restart(&handshake_request(), 0, 1, 2);
    supervisor.adapter().stop.set(Act::Fail);
    match supervisor.shutdown(10) {
        Err(DegradationCauseV1::PredecessorTerminationFailed(_)) => {}
        other => panic!("expected a typed shutdown failure, got {other:?}"),
    }
    assert_eq!(
        supervisor.predecessor_state(),
        PredecessorStateV1::DeathUnknown
    );
    supervisor.adapter().kill.set(Act::Fail);
    assert!(matches!(
        supervisor.start_or_restart(&handshake_request(), 1_000_000, 1, 2),
        SupervisorOutcomeV1::Unavailable(DegradationCauseV1::PredecessorDeathUnknown)
    ));
    assert_eq!(supervisor.adapter().count("start"), 1);
}

// ---------------------------------------------------------------------------
// Configuration.
// ---------------------------------------------------------------------------

/// A budget that can never admit a second attempt is refused at construction
/// rather than accepted as an unusually strict policy.
#[test]
fn unusable_budgets_are_refused_at_construction() {
    let outcome = ProviderSupervisorV1::new(
        ScriptedAdapter::ready(),
        supervised_scope(primary_scope()),
        RestartBudgetV1 {
            max_attempts_per_window: 0,
            ..restart_budget(1)
        },
        shutdown_budget(),
    );
    assert!(matches!(
        outcome.err(),
        Some(SupervisorConfigError::InvalidField {
            field: "max_attempts_per_window"
        })
    ));

    assert!(matches!(
        SupervisedScopeV1::new(
            OwnedProviderId::new(PROVIDER_ID).expect("provider id"),
            0,
            primary_scope(),
            limits(),
        )
        .err(),
        Some(SupervisorConfigError::InvalidField {
            field: "registration_revision"
        })
    ));
}

// ---------------------------------------------------------------------------
// A real external process topology.
// ---------------------------------------------------------------------------

/// A supervised adapter over a real OS child process. `start` really spawns,
/// `handshake` really probes liveness, and `request_stop`/`kill` really reap.
struct ProcessAdapter {
    program: &'static str,
    child: Mutex<Option<Child>>,
}

impl ProcessAdapter {
    fn new(program: &'static str) -> Self {
        Self {
            program,
            child: Mutex::new(None),
        }
    }
}

impl Drop for ProcessAdapter {
    fn drop(&mut self) {
        if let Ok(mut slot) = self.child.lock()
            && let Some(mut child) = slot.take()
        {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[derive(Debug)]
enum ProcessAdapterError {
    Spawn(std::io::Error),
    NoInstance,
    Exited(String),
    Wait(std::io::Error),
}

impl fmt::Display for ProcessAdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Spawn(source) => write!(formatter, "spawn failed: {source}"),
            Self::NoInstance => formatter.write_str("no supervised child process exists"),
            Self::Exited(status) => write!(formatter, "supervised child exited: {status}"),
            Self::Wait(source) => write!(formatter, "wait failed: {source}"),
        }
    }
}

impl std::error::Error for ProcessAdapterError {}

impl ProviderLifecycleAdapterV1 for ProcessAdapter {
    type Error = ProcessAdapterError;

    fn start(&self, _deadline_unix_micros: i64) -> Result<(), Self::Error> {
        let child = Command::new("/bin/sh")
            .arg("-c")
            .arg(self.program)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(ProcessAdapterError::Spawn)?;
        let mut slot = self.child.lock().unwrap_or_else(|error| error.into_inner());
        *slot = Some(child);
        Ok(())
    }

    fn handshake(
        &self,
        request: &HandshakeRequest,
        _deadline_unix_micros: i64,
    ) -> Result<HandshakeResponse, Self::Error> {
        // Give a short-lived child time to actually exit, so liveness is
        // observed rather than assumed from the spawn call returning.
        std::thread::sleep(std::time::Duration::from_millis(120));
        let mut slot = self.child.lock().unwrap_or_else(|error| error.into_inner());
        let child = slot.as_mut().ok_or(ProcessAdapterError::NoInstance)?;
        match child.try_wait().map_err(ProcessAdapterError::Wait)? {
            Some(status) => Err(ProcessAdapterError::Exited(status.to_string())),
            None => Ok(ready_response(request)),
        }
    }

    fn request_stop(&self, _deadline_unix_micros: i64) -> Result<bool, Self::Error> {
        let mut slot = self.child.lock().unwrap_or_else(|error| error.into_inner());
        let Some(mut child) = slot.take() else {
            return Ok(true);
        };
        child.kill().map_err(ProcessAdapterError::Wait)?;
        child.wait().map_err(ProcessAdapterError::Wait)?;
        Ok(true)
    }

    fn kill(&self, deadline_unix_micros: i64) -> Result<(), Self::Error> {
        self.request_stop(deadline_unix_micros).map(|_| ())
    }
}

/// A real child process that exits immediately produces typed unavailability
/// and leaves the host running; a real long-lived child reaches `Ready` and is
/// reaped by a bounded shutdown. Catches a supervisor that treats "the spawn
/// call returned" as readiness.
#[test]
fn a_real_external_process_exit_is_typed_unavailability_and_the_host_survives() {
    let mut dead = ProviderSupervisorV1::new(
        ProcessAdapter::new("exit 7"),
        supervised_scope(primary_scope()),
        restart_budget(3),
        shutdown_budget(),
    )
    .expect("supervisor");
    match dead.start_or_restart(&handshake_request(), 0, i64::MAX, i64::MAX) {
        SupervisorOutcomeV1::Unavailable(DegradationCauseV1::HandshakeTransportFailed(cause)) => {
            assert!(matches!(cause, ProcessAdapterError::Exited(_)));
        }
        other => panic!("expected typed transport failure, got {other:?}"),
    }
    assert_eq!(
        dead.current_availability(),
        ProviderAvailabilityV1::Unavailable
    );

    // The host is unaffected: a separate supervised long-lived process still
    // reaches readiness and shuts down inside its bounded budget.
    let mut live = ProviderSupervisorV1::new(
        ProcessAdapter::new("sleep 30"),
        supervised_scope(primary_scope()),
        restart_budget(3),
        shutdown_budget(),
    )
    .expect("supervisor");
    assert!(matches!(
        live.start_or_restart(&handshake_request(), 0, i64::MAX, i64::MAX),
        SupervisorOutcomeV1::Ready(_)
    ));
    let report = live.shutdown(1_000).expect("bounded shutdown");
    assert!(report.confirmed_dead);
    assert_eq!(live.predecessor_state(), PredecessorStateV1::None);
}
