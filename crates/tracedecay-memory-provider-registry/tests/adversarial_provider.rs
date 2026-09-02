//! Behavioral proof that a malicious, crashing, hanging, or
//! protocol-violating provider cannot wedge the host, cannot be respawned
//! forever, and cannot claim state outside the namespace it was admitted to
//! own (`tdmem-1107`).
//!
//! Every test here fails against a supervisor that
//!
//! * forgives a crash loop once the rolling restart window ages out, so a
//!   permanently broken provider is respawned once per window for the life of
//!   the host;
//! * counts the host's *own* pacing refusals as provider misbehavior, so a
//!   caller in a tight loop can quarantine a healthy provider;
//! * lets a shutdown, a foreign-scope request, or a crash report launder a
//!   quarantine;
//! * releases a quarantine automatically, or over an instance whose death was
//!   never confirmed;
//! * accepts a provider-reported state namespace that traverses out of the
//!   host-owned root or names another authority's namespace.
#![allow(clippy::expect_used, clippy::panic)]

use std::cell::{Cell, RefCell};
use std::fmt;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use tracedecay_memory_provider_api::contract::TerminalCode;
use tracedecay_memory_provider_api::{
    CancellationToken, CommittedEffectEvidence, FallbackDirective, HandshakeRequest,
    HandshakeRequestParts, HandshakeResponse, OperationControl, OwnedExactScope, OwnedProviderId,
    OwnedVersionedId, ProviderCall, ProviderDescriptor, ProviderLimits, ProviderOperation,
    ProviderReply, TerminalRecord,
};
use tracedecay_memory_provider_native::{
    NATIVE_PROVIDER_ID, NativeMemoryApplicationPort, NativeObservation,
};
mod isolation_fixture;

use isolation_fixture::ThreadBoundedProviderCallV1;
use tracedecay_memory_provider_registry::{
    DegradationCauseV1, DegradationKindV1, EnabledProviderMode, FabricConfig,
    NativeProviderActivation, ProjectMemoryProviderComposition, ProviderAvailabilityV1,
    ProviderLifecycleAdapterV1, ProviderSupervisorV1, QuarantinePolicyV1, QuarantineReleaseError,
    ReadinessDefectV1, ReproveOutcomeV1, RestartBudgetV1, ShutdownBudgetV1,
    SupervisedProviderReadinessV1, SupervisedReadinessConfigV1, SupervisedReadinessError,
    SupervisedScopeV1, SupervisorOutcomeV1,
};

const RESOLVED_SCOPE_DIGEST: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const READY_RECEIPT: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const IMPLEMENTATION_SHA: &str = "2222222222222222222222222222222222222222222222222222222222222222";
const PROVIDER_ID: &str = "adversarial.provider";
/// The namespace prefix the host admits for `PROVIDER_ID`.
const ADMITTED_PREFIX: &str = "adversarial.provider";
/// Windows are 60s, so one pass per 100s is always a fresh window with the
/// enforced backoff long elapsed: every pass under test reaches the adapter.
const FRESH_WINDOW_MICROS: i64 = 100_000_000;
/// Quarantine ceiling used by these tests. Small enough to reach in a test,
/// large enough that a single window's attempts cannot reach it by accident.
const VIOLATION_CEILING: u32 = 4;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

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

fn descriptor_for(provider_id: &str) -> ProviderDescriptor {
    ProviderDescriptor::new(
        OwnedProviderId::new(provider_id).expect("provider id"),
        IMPLEMENTATION_SHA,
        "native.state.v1",
        7,
        capabilities(),
        limits(),
    )
    .expect("descriptor")
}

fn exact_scope(worktree: &str) -> OwnedExactScope {
    OwnedExactScope::new(
        "profile-adversarial",
        "project-adversarial",
        "repository-adversarial",
        worktree,
        "refs/heads/adversarial",
        "session-adversarial",
        RESOLVED_SCOPE_DIGEST,
    )
    .expect("exact scope")
}

fn handshake_request_for(provider_id: &str, scope: OwnedExactScope) -> HandshakeRequest {
    HandshakeRequest::new(HandshakeRequestParts {
        provider_id: OwnedProviderId::new(provider_id).expect("provider id"),
        registration_revision: 1,
        exact_scope: scope,
        request_id: "adversarial-handshake".to_owned(),
        required_capabilities: vec![
            OwnedVersionedId::new("provider.health.v1").expect("capability"),
        ],
        host_limits: limits(),
        control: OperationControl::new(i64::MAX, 1_000, CancellationToken::new()),
        challenge_nonce: [7; 32],
    })
    .expect("handshake request")
}

fn handshake_request() -> HandshakeRequest {
    handshake_request_for(PROVIDER_ID, exact_scope("worktree-primary"))
}

/// A contract-satisfying success whose state namespace is exactly the one the
/// caller wants to fuzz.
fn response_with_namespace(
    request: &HandshakeRequest,
    namespace: Option<&str>,
) -> HandshakeResponse {
    HandshakeResponse {
        terminal: TerminalRecord::new(
            ProviderOperation::Handshake,
            OwnedProviderId::new(PROVIDER_ID).expect("provider id"),
            TerminalCode::Success,
            CommittedEffectEvidence::none(None),
            FallbackDirective::forbidden(),
            "adversarial-operation",
            request.exact_scope.exact_scope_sha256(),
            None,
        )
        .expect("terminal"),
        descriptor: Some(descriptor_for(PROVIDER_ID)),
        provider_instance_id: Some("adversarial-instance-1".to_owned()),
        state_namespace: namespace.map(str::to_owned),
        accepted_scope: Some(request.exact_scope.clone()),
        effective_limits: Some(limits()),
        ready_receipt_sha256: Some(READY_RECEIPT.to_owned()),
        warnings: Vec::new(),
    }
}

/// What one scripted adapter call does.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Act {
    Ok,
    Fail,
    Panic,
}

#[derive(Debug)]
struct AdapterError(&'static str);

impl fmt::Display for AdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for AdapterError {}

/// A scripted, deliberately hostile provider adapter. Every call is recorded
/// so a test can prove the supervisor made **no** call at all, which is what
/// distinguishes a quarantine from a mere refusal.
struct HostileAdapter {
    start: Cell<Act>,
    handshake: Cell<Act>,
    stop: Cell<Act>,
    stop_confirms: Cell<bool>,
    kill: Cell<Act>,
    namespace: RefCell<Option<String>>,
    instance_id: RefCell<Option<String>>,
    warnings: RefCell<Vec<String>>,
    calls: RefCell<Vec<&'static str>>,
}

impl HostileAdapter {
    fn healthy() -> Self {
        Self {
            start: Cell::new(Act::Ok),
            handshake: Cell::new(Act::Ok),
            stop: Cell::new(Act::Ok),
            stop_confirms: Cell::new(true),
            kill: Cell::new(Act::Ok),
            namespace: RefCell::new(Some(ADMITTED_PREFIX.to_owned())),
            instance_id: RefCell::new(None),
            warnings: RefCell::new(Vec::new()),
            calls: RefCell::new(Vec::new()),
        }
    }

    fn oversized_instance_id(instance_id: &str) -> Self {
        let adapter = Self::healthy();
        *adapter.instance_id.borrow_mut() = Some(instance_id.to_owned());
        adapter
    }

    fn warning_flood(count: usize, bytes: usize) -> Self {
        let adapter = Self::healthy();
        *adapter.warnings.borrow_mut() = (0..count).map(|_| "w".repeat(bytes)).collect();
        adapter
    }

    fn claiming(namespace: Option<&str>) -> Self {
        let adapter = Self::healthy();
        *adapter.namespace.borrow_mut() = namespace.map(str::to_owned);
        adapter
    }

    fn total_calls(&self) -> usize {
        self.calls.borrow().len()
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

impl ProviderLifecycleAdapterV1 for HostileAdapter {
    type Error = AdapterError;

    fn start(&self, _deadline_unix_micros: i64) -> Result<(), Self::Error> {
        self.record("start");
        match self.start.get() {
            Act::Ok => Ok(()),
            Act::Fail => Err(AdapterError("hostile start failure")),
            Act::Panic => panic!("hostile start panic"),
        }
    }

    fn handshake(
        &self,
        request: &HandshakeRequest,
        _deadline_unix_micros: i64,
    ) -> Result<HandshakeResponse, Self::Error> {
        self.record("handshake");
        match self.handshake.get() {
            Act::Ok => {
                let mut response =
                    response_with_namespace(request, self.namespace.borrow().as_deref());
                if let Some(instance_id) = self.instance_id.borrow().as_deref() {
                    response.provider_instance_id = Some(instance_id.to_owned());
                }
                response.warnings = self.warnings.borrow().clone();
                Ok(response)
            }
            Act::Fail => Err(AdapterError("hostile handshake transport failure")),
            Act::Panic => panic!("hostile handshake panic"),
        }
    }

    fn request_stop(&self, _deadline_unix_micros: i64) -> Result<bool, Self::Error> {
        self.record("request_stop");
        match self.stop.get() {
            Act::Ok => Ok(self.stop_confirms.get()),
            Act::Fail => Err(AdapterError("hostile stop failure")),
            Act::Panic => panic!("hostile stop panic"),
        }
    }

    fn kill(&self, _deadline_unix_micros: i64) -> Result<(), Self::Error> {
        self.record("kill");
        match self.kill.get() {
            Act::Ok => Ok(()),
            Act::Fail => Err(AdapterError("hostile kill failure")),
            Act::Panic => panic!("hostile kill panic"),
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

fn supervised_scope() -> SupervisedScopeV1 {
    SupervisedScopeV1::new(
        OwnedProviderId::new(PROVIDER_ID).expect("provider id"),
        1,
        exact_scope("worktree-primary"),
        limits(),
    )
    .expect("supervised scope")
    .with_admitted_state_namespace_prefix(ADMITTED_PREFIX)
    .expect("admitted namespace prefix")
}

fn supervisor(adapter: HostileAdapter) -> ProviderSupervisorV1<HostileAdapter> {
    ProviderSupervisorV1::new(
        adapter,
        supervised_scope(),
        restart_budget(2),
        shutdown_budget(),
    )
    .expect("supervisor")
    .with_quarantine_policy(QuarantinePolicyV1 {
        max_provider_violations: VIOLATION_CEILING,
    })
    .expect("quarantine policy")
}

fn without_panic_noise(body: impl FnOnce()) {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    body();
    std::panic::set_hook(previous);
}

/// Drives `passes` start-or-restart passes, each in its own restart window so
/// every one of them actually reaches the adapter.
fn drive_fresh_windows(supervisor: &mut ProviderSupervisorV1<HostileAdapter>, passes: i64) {
    for pass in 0..passes {
        let now = pass.saturating_mul(FRESH_WINDOW_MICROS);
        let _ = supervisor.start_or_restart(&handshake_request(), now, now + 1, now + 2);
    }
}

fn expect_defect(outcome: SupervisorOutcomeV1<AdapterError>) -> ReadinessDefectV1 {
    match outcome {
        SupervisorOutcomeV1::Unavailable(DegradationCauseV1::HandshakeContractViolation(
            defect,
        )) => defect,
        other => panic!("expected a handshake contract violation, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// A crash loop is terminal, not merely paced.
// ---------------------------------------------------------------------------

/// A provider that never reaches a validated readiness is quarantined after a
/// finite number of violations counted **across** restart windows, and the
/// supervisor then stops spawning it entirely.
///
/// Catches the defect the rolling window alone leaves open: attempts age out,
/// so a permanently broken or malicious provider is respawned once per window
/// for the life of the host.
#[test]
fn a_provider_that_never_reaches_readiness_is_quarantined_across_windows() {
    let mut supervisor = supervisor(HostileAdapter::healthy());
    supervisor.adapter().handshake.set(Act::Fail);

    drive_fresh_windows(&mut supervisor, i64::from(VIOLATION_CEILING));
    assert_eq!(
        supervisor.adapter().count("start"),
        VIOLATION_CEILING as usize,
        "every pass before the ceiling must reach the adapter"
    );
    assert_eq!(
        supervisor.current_availability(),
        ProviderAvailabilityV1::Quarantined
    );
    let record = supervisor.quarantine().expect("quarantine record").clone();
    assert_eq!(record.violations(), VIOLATION_CEILING);
    assert_eq!(
        record.first_violation(),
        DegradationKindV1::HandshakeTransportFailed
    );
    assert_eq!(
        record.last_violation(),
        DegradationKindV1::HandshakeTransportFailed
    );

    // Ten further windows: not one of them touches the adapter again.
    let calls_at_quarantine = supervisor.adapter().total_calls();
    for pass in i64::from(VIOLATION_CEILING)..i64::from(VIOLATION_CEILING) + 10 {
        let now = pass.saturating_mul(FRESH_WINDOW_MICROS);
        match supervisor.start_or_restart(&handshake_request(), now, now + 1, now + 2) {
            SupervisorOutcomeV1::Unavailable(DegradationCauseV1::Quarantined {
                violations,
                ..
            }) => assert_eq!(violations, VIOLATION_CEILING),
            other => panic!("expected a quarantine refusal, got {other:?}"),
        }
    }
    assert_eq!(
        supervisor.adapter().total_calls(),
        calls_at_quarantine,
        "a quarantined provider must receive no further adapter call"
    );
    assert_eq!(
        supervisor.current_degradation().map(|record| record.kind()),
        Some(DegradationKindV1::Quarantined)
    );
}

/// A provider that will not die — every graceful stop and every kill fails —
/// is quarantined rather than reconciled forever. Catches an unbounded
/// terminate-retry loop against a hung instance.
#[test]
fn a_provider_that_will_not_die_is_quarantined_rather_than_reconciled_forever() {
    let mut supervisor = supervisor(HostileAdapter::healthy());
    assert!(matches!(
        supervisor.start_or_restart(&handshake_request(), 0, 1, 2),
        SupervisorOutcomeV1::Ready(_)
    ));
    supervisor.adapter().stop.set(Act::Fail);
    supervisor.adapter().kill.set(Act::Fail);

    for pass in 1..=i64::from(VIOLATION_CEILING) {
        let now = pass.saturating_mul(FRESH_WINDOW_MICROS);
        let _ = supervisor.start_or_restart(&handshake_request(), now, now + 1, now + 2);
    }
    assert_eq!(
        supervisor.current_availability(),
        ProviderAvailabilityV1::Quarantined
    );
    // Only the one successful spawn ever happened: a supervisor that could not
    // confirm death never spawned a second owner.
    assert_eq!(supervisor.adapter().count("start"), 1);

    let kills_at_quarantine = supervisor.adapter().count("kill");
    let now = 20 * FRESH_WINDOW_MICROS;
    let _ = supervisor.start_or_restart(&handshake_request(), now, now + 1, now + 2);
    assert_eq!(
        supervisor.adapter().count("kill"),
        kills_at_quarantine,
        "a quarantined provider must not be killed again by a readiness pass"
    );
}

/// An adapter that panics on every call is contained on every pass, and the
/// run of contained panics is itself bounded by quarantine. Catches a host
/// that survives one panic but re-enters the panicking adapter forever.
#[test]
fn a_panicking_adapter_is_contained_and_then_quarantined() {
    without_panic_noise(|| {
        let mut supervisor = supervisor(HostileAdapter::healthy());
        supervisor.adapter().start.set(Act::Panic);
        supervisor.adapter().stop.set(Act::Panic);
        supervisor.adapter().kill.set(Act::Panic);

        drive_fresh_windows(&mut supervisor, i64::from(VIOLATION_CEILING));
        assert_eq!(
            supervisor.current_availability(),
            ProviderAvailabilityV1::Quarantined
        );
        let record = supervisor.quarantine().expect("quarantine record").clone();
        assert_eq!(record.first_violation(), DegradationKindV1::AdapterPanicked);

        let calls = supervisor.adapter().total_calls();
        let now = 50 * FRESH_WINDOW_MICROS;
        let _ = supervisor.start_or_restart(&handshake_request(), now, now + 1, now + 2);
        assert_eq!(supervisor.adapter().total_calls(), calls);
    });
}

/// The host's own refusals — an exhausted restart window and an unelapsed
/// backoff — never count toward quarantine, no matter how often a caller
/// provokes them.
///
/// Catches the inverse defect of the one quarantine exists for: a caller in a
/// tight loop quarantining a provider that was never even contacted.
#[test]
fn host_pacing_refusals_never_count_toward_quarantine() {
    let mut supervisor = supervisor(HostileAdapter::healthy());
    supervisor.adapter().handshake.set(Act::Fail);

    // One real violation, then fifty passes at the same instant: every one of
    // them is refused by pacing or by the window ceiling without an adapter
    // call, so the violation count must stay at one.
    let _ = supervisor.start_or_restart(&handshake_request(), 0, 1, 2);
    assert_eq!(supervisor.provider_violations(), 1);
    for _ in 0..50 {
        match supervisor.start_or_restart(&handshake_request(), 0, 1, 2) {
            SupervisorOutcomeV1::Unavailable(DegradationCauseV1::BackoffNotElapsed { .. }) => {}
            other => panic!("expected an enforced backoff refusal, got {other:?}"),
        }
    }
    assert_eq!(supervisor.provider_violations(), 1);
    assert!(!supervisor.is_quarantined());
    assert_eq!(supervisor.adapter().count("start"), 1);

    // Now spend the window ceiling and hammer the exhausted-window refusal.
    let _ = supervisor.start_or_restart(&handshake_request(), 1_000, 1, 2);
    assert_eq!(supervisor.provider_violations(), 2);
    for _ in 0..50 {
        match supervisor.start_or_restart(&handshake_request(), 10_000, 1, 2) {
            SupervisorOutcomeV1::Unavailable(DegradationCauseV1::RestartBudgetExhausted {
                ..
            }) => {}
            other => panic!("expected a budget-exhaustion refusal, got {other:?}"),
        }
    }
    assert_eq!(supervisor.provider_violations(), 2);
    assert!(!supervisor.is_quarantined());
}

/// A validated readiness clears the violation run, so a provider that fails
/// transiently and recovers is never quarantined by the sum of failures it
/// already recovered from.
#[test]
fn a_recovered_provider_clears_its_violation_run() {
    let mut supervisor = supervisor(HostileAdapter::healthy());
    supervisor.adapter().handshake.set(Act::Fail);
    drive_fresh_windows(&mut supervisor, i64::from(VIOLATION_CEILING) - 1);
    assert_eq!(supervisor.provider_violations(), VIOLATION_CEILING - 1);
    assert!(!supervisor.is_quarantined());

    supervisor.adapter().handshake.set(Act::Ok);
    let now = i64::from(VIOLATION_CEILING) * FRESH_WINDOW_MICROS;
    assert!(matches!(
        supervisor.start_or_restart(&handshake_request(), now, now + 1, now + 2),
        SupervisorOutcomeV1::Ready(_)
    ));
    assert_eq!(supervisor.provider_violations(), 0);

    // The same number of failures again is still below the ceiling, because
    // the recovered run was cleared rather than carried.
    supervisor.adapter().handshake.set(Act::Fail);
    for pass in 1..i64::from(VIOLATION_CEILING) {
        let now = (i64::from(VIOLATION_CEILING) + pass).saturating_mul(FRESH_WINDOW_MICROS);
        let _ = supervisor.start_or_restart(&handshake_request(), now, now + 1, now + 2);
    }
    assert!(!supervisor.is_quarantined());
    assert_eq!(
        supervisor.current_availability(),
        ProviderAvailabilityV1::Unavailable
    );
}

// ---------------------------------------------------------------------------
// A quarantine cannot be laundered.
// ---------------------------------------------------------------------------

/// Neither a request for a foreign exact scope, nor a reported crash, nor a
/// readiness re-proof can move a quarantined provider out of quarantine, and
/// none of them reaches the adapter.
#[test]
fn no_caller_can_launder_a_quarantine() {
    let mut supervisor = supervisor(HostileAdapter::healthy());
    supervisor.adapter().handshake.set(Act::Fail);
    drive_fresh_windows(&mut supervisor, i64::from(VIOLATION_CEILING));
    assert!(supervisor.is_quarantined());
    let calls = supervisor.adapter().total_calls();

    let foreign = handshake_request_for(PROVIDER_ID, exact_scope("worktree-foreign"));
    match supervisor.start_or_restart(&foreign, 10 * FRESH_WINDOW_MICROS, 1, 2) {
        SupervisorOutcomeV1::Unavailable(DegradationCauseV1::Quarantined { .. }) => {}
        other => panic!("expected a quarantine refusal, got {other:?}"),
    }
    match supervisor.reprove_readiness(&handshake_request(), i64::MAX) {
        ReproveOutcomeV1::Unavailable(DegradationCauseV1::Quarantined { .. }) => {}
        other => panic!("expected a quarantine refusal, got {other:?}"),
    }
    match supervisor.report_crash() {
        SupervisorOutcomeV1::Unavailable(DegradationCauseV1::Quarantined { .. }) => {}
        other => panic!("expected a quarantine refusal, got {other:?}"),
    }
    assert_eq!(
        supervisor.current_availability(),
        ProviderAvailabilityV1::Quarantined
    );
    assert_eq!(supervisor.adapter().total_calls(), calls);
}

/// A bounded shutdown of a quarantined provider confirms the instance's death
/// — which is exactly what a release needs — without clearing the quarantine.
/// Catches a shutdown path that resets availability and drops the persisted
/// evidence, turning "stop the provider" into "forgive the provider".
#[test]
fn shutdown_confirms_death_without_releasing_the_quarantine() {
    let mut supervisor = supervisor(HostileAdapter::healthy());
    supervisor.adapter().handshake.set(Act::Fail);
    drive_fresh_windows(&mut supervisor, i64::from(VIOLATION_CEILING));
    assert!(supervisor.is_quarantined());

    let report = supervisor
        .shutdown(50 * FRESH_WINDOW_MICROS)
        .expect("bounded shutdown");
    assert!(report.confirmed_dead);
    assert!(supervisor.is_quarantined());
    assert_eq!(
        supervisor.current_availability(),
        ProviderAvailabilityV1::Quarantined
    );
    // The evidence of the violation that quarantined the provider survives the
    // shutdown; the shutdown neither clears it nor rewrites it as a clean stop.
    assert_eq!(
        supervisor.current_degradation().map(|record| record.kind()),
        Some(DegradationKindV1::HandshakeTransportFailed)
    );
}

/// A quarantine is released only explicitly, and only once the quarantined
/// instance's death is confirmed. Catches a release that spawns a replacement
/// over a live hostile instance.
#[test]
fn releasing_a_quarantine_is_explicit_and_requires_confirmed_death() {
    let mut supervisor = supervisor(HostileAdapter::healthy());
    supervisor.adapter().handshake.set(Act::Fail);
    drive_fresh_windows(&mut supervisor, i64::from(VIOLATION_CEILING));
    assert!(supervisor.is_quarantined());

    // The failed handshakes left the instance possibly-live, so a release now
    // is refused rather than silently accepted.
    assert_eq!(
        supervisor.release_quarantine(),
        Err(QuarantineReleaseError::InstanceNotConfirmedDead)
    );
    assert!(supervisor.is_quarantined());

    supervisor
        .shutdown(50 * FRESH_WINDOW_MICROS)
        .expect("bounded shutdown");
    let record = supervisor.release_quarantine().expect("explicit release");
    assert_eq!(record.violations(), VIOLATION_CEILING);
    assert!(!supervisor.is_quarantined());
    assert_eq!(supervisor.provider_violations(), 0);
    assert_eq!(
        supervisor.current_availability(),
        ProviderAvailabilityV1::NotStarted
    );
    assert_eq!(
        supervisor.release_quarantine(),
        Err(QuarantineReleaseError::NotQuarantined)
    );

    // A released provider is startable again, and a fixed one reaches Ready.
    supervisor.adapter().handshake.set(Act::Ok);
    let now = 60 * FRESH_WINDOW_MICROS;
    assert!(matches!(
        supervisor.start_or_restart(&handshake_request(), now, now + 1, now + 2),
        SupervisorOutcomeV1::Ready(_)
    ));
}

// ---------------------------------------------------------------------------
// State-namespace containment: a provider cannot claim state it does not own.
// ---------------------------------------------------------------------------

/// Every namespace shape that could address storage outside the host-owned
/// namespace root is refused fail-closed, and no readiness is claimed.
///
/// Catches a supervisor that only bounds the namespace's length and control
/// characters — the shape the wire contract already guarantees — and so lets
/// a provider name a parent directory, an absolute path, or a
/// percent-encoded traversal as "its" state.
#[test]
fn a_state_namespace_that_escapes_the_host_root_is_refused() {
    for hostile in [
        "../../etc/passwd",
        "..",
        "/absolute/root",
        "adversarial.provider/../../escape",
        ".hidden",
        "adversarial.provider/",
        "adversarial.provider.",
        "adversarial//provider",
        "adversarial.provider/./here",
        "C:/windows",
        "adversarial.provider%2e%2e",
        "~/home",
    ] {
        let mut supervisor = supervisor(HostileAdapter::claiming(Some(hostile)));
        let outcome = supervisor.start_or_restart(&handshake_request(), 0, 1, 2);
        match expect_defect(outcome) {
            ReadinessDefectV1::StateNamespaceEscapesContainment { state_namespace } => {
                assert_eq!(state_namespace, hostile);
            }
            other => panic!("namespace {hostile} produced {other:?}"),
        }
        assert!(supervisor.ready_evidence().is_none());
    }
}

/// A well-shaped namespace that is nonetheless outside the prefix this
/// provider was admitted to own is refused, including one that merely shares
/// the prefix's leading characters.
///
/// Catches prefix admission implemented as a bare `starts_with`, which would
/// admit `adversarial.provider-evil` as if it were inside
/// `adversarial.provider`.
#[test]
fn a_state_namespace_outside_the_admitted_prefix_is_refused() {
    for hostile in [
        "tracedecay.native.project",
        "adversarial.provider-evil",
        "adversarial.providerevil",
        "host.authority",
    ] {
        let mut supervisor = supervisor(HostileAdapter::claiming(Some(hostile)));
        let outcome = supervisor.start_or_restart(&handshake_request(), 0, 1, 2);
        match expect_defect(outcome) {
            ReadinessDefectV1::StateNamespaceNotAdmitted {
                state_namespace,
                admitted_prefix,
            } => {
                assert_eq!(state_namespace, hostile);
                assert_eq!(admitted_prefix, ADMITTED_PREFIX);
            }
            other => panic!("namespace {hostile} produced {other:?}"),
        }
        assert!(supervisor.ready_evidence().is_none());
    }
}

/// The admitted prefix itself, and any namespace beneath it at a real segment
/// boundary, reach readiness — so containment refuses attackers rather than
/// the provider's own legitimate state.
#[test]
fn the_admitted_prefix_and_its_segments_reach_readiness() {
    for admitted in [
        "adversarial.provider",
        "adversarial.provider.project",
        "adversarial.provider/worktree-primary",
        "adversarial.provider.project_2-b",
    ] {
        let mut supervisor = supervisor(HostileAdapter::claiming(Some(admitted)));
        match supervisor.start_or_restart(&handshake_request(), 0, 1, 2) {
            SupervisorOutcomeV1::Ready(evidence) => {
                assert_eq!(evidence.state_namespace(), admitted);
            }
            other => panic!("namespace {admitted} was refused: {other:?}"),
        }
    }
}

/// An oversized namespace is refused as unusable rather than truncated into
/// something the host would then treat as a real state path.
#[test]
fn an_oversized_state_namespace_is_refused() {
    let oversized = format!("{ADMITTED_PREFIX}.{}", "n".repeat(300));
    let mut supervisor = supervisor(HostileAdapter::claiming(Some(&oversized)));
    let outcome = supervisor.start_or_restart(&handshake_request(), 0, 1, 2);
    assert_eq!(
        expect_defect(outcome),
        ReadinessDefectV1::InvalidStateNamespace
    );
}

/// A namespace carrying characters the host does not own is refused as
/// unusable.
#[test]
fn a_state_namespace_with_foreign_characters_is_refused() {
    for hostile in [
        "adversarial.provider name",
        "adversarial.provider\u{0}null",
        "adversarial.provider\nline",
        "adversarial.provider?query",
        "adversarial.provider*glob",
    ] {
        let mut supervisor = supervisor(HostileAdapter::claiming(Some(hostile)));
        let outcome = supervisor.start_or_restart(&handshake_request(), 0, 1, 2);
        assert_eq!(
            expect_defect(outcome),
            ReadinessDefectV1::InvalidStateNamespace,
            "namespace {hostile:?} must be refused as unusable"
        );
    }
}

/// A host that pinned no prefix still refuses an escaping namespace: the
/// structural containment rule is unconditional, not a configuration the host
/// can forget to switch on.
#[test]
fn containment_holds_without_an_admitted_prefix() {
    let scope = SupervisedScopeV1::new(
        OwnedProviderId::new(PROVIDER_ID).expect("provider id"),
        1,
        exact_scope("worktree-primary"),
        limits(),
    )
    .expect("supervised scope");
    let mut supervisor = ProviderSupervisorV1::new(
        HostileAdapter::claiming(Some("../../escape")),
        scope,
        restart_budget(2),
        shutdown_budget(),
    )
    .expect("supervisor");
    let outcome = supervisor.start_or_restart(&handshake_request(), 0, 1, 2);
    assert!(matches!(
        expect_defect(outcome),
        ReadinessDefectV1::StateNamespaceEscapesContainment { .. }
    ));
}

/// A prefix a host could never bound a namespace with is refused at
/// configuration time rather than accepted and then silently ignored.
#[test]
fn an_unusable_admitted_prefix_is_refused_at_configuration() {
    for unusable in ["", "../escape", "/absolute", "prefix/"] {
        let scope = SupervisedScopeV1::new(
            OwnedProviderId::new(PROVIDER_ID).expect("provider id"),
            1,
            exact_scope("worktree-primary"),
            limits(),
        )
        .expect("supervised scope");
        assert!(
            scope
                .with_admitted_state_namespace_prefix(unusable)
                .is_err(),
            "prefix {unusable:?} must be refused"
        );
    }
}

/// An excessive handshake payload is refused field by field, and none of it
/// is ever copied into readiness evidence: an oversized instance identity, an
/// oversized single warning, and a flood of warnings are each a fail-closed
/// refusal.
///
/// Catches a supervisor that bounds only what the wire contract already
/// bounds and then hands an unbounded provider-controlled string to the host
/// as this incarnation's identity.
#[test]
fn an_excessive_handshake_payload_is_refused_field_by_field() {
    let oversized_identity = "i".repeat(4_096);
    let mut oversized = supervisor(HostileAdapter::oversized_instance_id(&oversized_identity));
    let outcome = oversized.start_or_restart(&handshake_request(), 0, 1, 2);
    assert_eq!(
        expect_defect(outcome),
        ReadinessDefectV1::InvalidInstanceIdentity
    );
    assert!(oversized.ready_evidence().is_none());

    let mut oversized_warning = supervisor(HostileAdapter::warning_flood(1, 4_096));
    match expect_defect(oversized_warning.start_or_restart(&handshake_request(), 0, 1, 2)) {
        ReadinessDefectV1::UnboundedWarnings { warnings } => assert_eq!(warnings, 1),
        other => panic!("expected an unbounded-warning refusal, got {other:?}"),
    }
    assert!(oversized_warning.ready_evidence().is_none());

    let mut warning_flood = supervisor(HostileAdapter::warning_flood(4_096, 8));
    match expect_defect(warning_flood.start_or_restart(&handshake_request(), 0, 1, 2)) {
        ReadinessDefectV1::UnboundedWarnings { warnings } => assert_eq!(warnings, 4_096),
        other => panic!("expected an unbounded-warning refusal, got {other:?}"),
    }
    assert!(warning_flood.ready_evidence().is_none());
}

/// A quarantine ceiling of zero could never admit a single attempt, so it is
/// refused rather than accepted as an unusually strict policy.
#[test]
fn an_unusable_quarantine_ceiling_is_refused() {
    assert!(
        QuarantinePolicyV1 {
            max_provider_violations: 0,
        }
        .validate()
        .is_err()
    );
    assert!(QuarantinePolicyV1::DEFAULT.validate().is_ok());
}

// ---------------------------------------------------------------------------
// The mounted seam: the same containment and quarantine over a real composed
// provider set, driven through the value a composition root mounts.
// ---------------------------------------------------------------------------

/// A Native application port whose reported state namespace a test controls,
/// so the mounted path's own validation is exercised rather than only the
/// supervisor unit's.
struct MountedHostilePort {
    descriptor: ProviderDescriptor,
    namespace: RwLock<String>,
    handshake_calls: AtomicUsize,
}

impl MountedHostilePort {
    fn new(namespace: &str) -> Self {
        Self {
            descriptor: descriptor_for(NATIVE_PROVIDER_ID),
            namespace: RwLock::new(namespace.to_owned()),
            handshake_calls: AtomicUsize::new(0),
        }
    }
}

impl NativeMemoryApplicationPort for MountedHostilePort {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        self.handshake_calls.fetch_add(1, Ordering::Relaxed);
        let namespace = self
            .namespace
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        HandshakeResponse {
            terminal: TerminalRecord::new(
                ProviderOperation::Handshake,
                OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("native provider"),
                TerminalCode::Success,
                CommittedEffectEvidence::none(Some(self.descriptor.state_generation)),
                FallbackDirective::forbidden(),
                request.request_id.clone(),
                request.exact_scope.exact_scope_sha256(),
                None,
            )
            .expect("handshake terminal"),
            descriptor: Some(self.descriptor.clone()),
            provider_instance_id: Some("native.hostile-instance".to_owned()),
            state_namespace: Some(namespace),
            accepted_scope: Some(request.exact_scope.clone()),
            effective_limits: Some(request.host_limits.minimum(self.descriptor.limits)),
            ready_receipt_sha256: Some(READY_RECEIPT.to_owned()),
            warnings: Vec::new(),
        }
    }

    fn health(&self, _call: &ProviderCall) -> ProviderReply {
        mounted_unexpected()
    }

    fn observe(&self, _observation: NativeObservation<'_>) -> ProviderReply {
        mounted_unexpected()
    }

    fn recall(&self, _call: &ProviderCall) -> ProviderReply {
        mounted_unexpected()
    }

    fn feedback(&self, _call: &ProviderCall) -> ProviderReply {
        mounted_unexpected()
    }

    fn maintenance(&self, _call: &ProviderCall) -> ProviderReply {
        mounted_unexpected()
    }

    fn inspection(&self, _call: &ProviderCall) -> ProviderReply {
        mounted_unexpected()
    }

    fn correction(&self, _call: &ProviderCall) -> ProviderReply {
        mounted_unexpected()
    }

    fn delete_by_source(&self, _call: &ProviderCall) -> ProviderReply {
        mounted_unexpected()
    }

    fn snapshot_export(&self, _call: &ProviderCall) -> ProviderReply {
        mounted_unexpected()
    }

    fn snapshot_restore(&self, _call: &ProviderCall) -> ProviderReply {
        mounted_unexpected()
    }

    fn replay(&self, _call: &ProviderCall) -> ProviderReply {
        mounted_unexpected()
    }
}

fn mounted_unexpected<T>() -> T {
    panic!("adversarial mounted tests must not execute provider operations")
}

fn mounted(port: Arc<MountedHostilePort>) -> SupervisedProviderReadinessV1 {
    mounted_with_scope_ceiling(port, 4)
}

fn mounted_with_scope_ceiling(
    port: Arc<MountedHostilePort>,
    max_supervised_scopes: usize,
) -> SupervisedProviderReadinessV1 {
    let composition = Arc::new(
        ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
            fabric_config: FabricConfig {
                max_registered_providers: 1,
                max_in_flight: 1,
            },
            port,
            registration_revision: 1,
            mode: EnabledProviderMode::Active,
        })
        .expect("enabled composition"),
    );
    SupervisedProviderReadinessV1::new(
        composition,
        Arc::new(ThreadBoundedProviderCallV1::default()),
        OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("native provider"),
        1,
        limits(),
        SupervisedReadinessConfigV1 {
            restart_budget: restart_budget(2),
            shutdown_budget: shutdown_budget(),
            start_budget_micros: 1_000_000,
            handshake_budget_micros: 1_000_000,
            max_supervised_scopes,
        },
    )
    .expect("mounted supervised readiness")
    .with_admitted_state_namespace_prefix(NATIVE_PROVIDER_ID)
    .expect("admitted namespace prefix")
    .with_quarantine_policy(QuarantinePolicyV1 {
        max_provider_violations: VIOLATION_CEILING,
    })
    .expect("quarantine policy")
}

/// Through the mounted value a composition root holds, a provider claiming a
/// namespace outside its admitted prefix never becomes ready, and a provider
/// claiming one inside it does.
///
/// Catches an admitted-prefix authority that exists only as a type: the
/// mounted owner must carry it into every per-scope supervisor it creates.
#[test]
fn the_mounted_seam_refuses_an_unadmitted_namespace() {
    let port = Arc::new(MountedHostilePort::new("host.authority.stolen"));
    let readiness = mounted(Arc::clone(&port));
    let request = handshake_request_for(NATIVE_PROVIDER_ID, exact_scope("worktree-mounted"));

    let error = readiness
        .ready_target(&request, 1_000)
        .expect_err("an unadmitted namespace cannot be ready");
    match error {
        SupervisedReadinessError::Unavailable { kind, detail, .. } => {
            assert_eq!(kind, DegradationKindV1::HandshakeContractViolation);
            assert!(
                detail.contains("host.authority.stolen"),
                "detail must name the refused namespace, got {detail}"
            );
        }
        other => panic!("expected typed unavailability, got {other}"),
    }
    assert_eq!(port.handshake_calls.load(Ordering::Relaxed), 1);

    *port
        .namespace
        .write()
        .unwrap_or_else(|poison| poison.into_inner()) = format!("{NATIVE_PROVIDER_ID}.project");
    let target = readiness
        .ready_target(&request, FRESH_WINDOW_MICROS)
        .expect("an admitted namespace reaches readiness");
    assert_eq!(target.provider_id().as_str(), NATIVE_PROVIDER_ID);
}

/// Through the mounted value, a provider that keeps violating the contract is
/// quarantined, the quarantine is visible per exact scope for operations, the
/// provider stops being contacted, and an explicit release restores it.
#[test]
fn the_mounted_seam_quarantines_a_persistently_violating_provider() {
    let port = Arc::new(MountedHostilePort::new("host.authority.stolen"));
    let readiness = mounted(Arc::clone(&port));
    let request = handshake_request_for(NATIVE_PROVIDER_ID, exact_scope("worktree-mounted"));

    for pass in 0..i64::from(VIOLATION_CEILING) {
        let error = readiness
            .ready_target(&request, pass.saturating_mul(FRESH_WINDOW_MICROS))
            .expect_err("a violating provider cannot be ready");
        assert!(
            matches!(error, SupervisedReadinessError::Unavailable { .. }),
            "expected typed unavailability, got {error}"
        );
    }
    let handshakes_at_quarantine = port.handshake_calls.load(Ordering::Relaxed);
    assert_eq!(handshakes_at_quarantine, VIOLATION_CEILING as usize);

    let quarantined = readiness.quarantined_scopes();
    assert_eq!(quarantined.len(), 1);
    let quarantined_scope = quarantined.first().expect("one quarantined scope").clone();
    assert_eq!(
        quarantined_scope.exact_scope_sha256,
        request.exact_scope.exact_scope_sha256()
    );
    assert_eq!(quarantined_scope.record.violations(), VIOLATION_CEILING);

    // Further passes never reach the provider again.
    for pass in i64::from(VIOLATION_CEILING)..i64::from(VIOLATION_CEILING) + 5 {
        let error = readiness
            .ready_target(&request, pass.saturating_mul(FRESH_WINDOW_MICROS))
            .expect_err("a quarantined provider cannot be ready");
        match error {
            SupervisedReadinessError::Unavailable { kind, .. } => {
                assert_eq!(kind, DegradationKindV1::Quarantined);
            }
            other => panic!("expected typed unavailability, got {other}"),
        }
    }
    assert_eq!(
        port.handshake_calls.load(Ordering::Relaxed),
        handshakes_at_quarantine,
        "a quarantined scope must stop contacting its provider"
    );

    // A release is refused for a scope that is not quarantined, and accepted
    // for the one that is.
    assert!(matches!(
        readiness.release_quarantine("not-a-supervised-scope", FRESH_WINDOW_MICROS),
        Err(SupervisedReadinessError::QuarantineRelease { .. })
    ));
    *port
        .namespace
        .write()
        .unwrap_or_else(|poison| poison.into_inner()) = format!("{NATIVE_PROVIDER_ID}.project");
    let released = readiness
        .release_quarantine(
            &request.exact_scope.exact_scope_sha256(),
            50 * FRESH_WINDOW_MICROS,
        )
        .expect("explicit release");
    assert_eq!(released.violations(), VIOLATION_CEILING);
    assert!(readiness.quarantined_scopes().is_empty());

    let target = readiness
        .ready_target(&request, 60 * FRESH_WINDOW_MICROS)
        .expect("a released and repaired provider reaches readiness");
    assert_eq!(target.provider_id().as_str(), NATIVE_PROVIDER_ID);
}

/// Quarantine is scoped to exactly one exact coding scope: a hostile
/// provider quarantined for one worktree's session must not stop the same
/// provider from serving another worktree that never misbehaved.
///
/// Catches a quarantine held at the mount instead of per supervised scope,
/// which would let one worktree's crash loop blind every other worktree in
/// the host.
#[test]
fn a_quarantine_is_confined_to_its_own_exact_scope() {
    let port = Arc::new(MountedHostilePort::new("host.authority.stolen"));
    let readiness = mounted(Arc::clone(&port));
    let hostile_request =
        handshake_request_for(NATIVE_PROVIDER_ID, exact_scope("worktree-hostile"));
    let innocent_request =
        handshake_request_for(NATIVE_PROVIDER_ID, exact_scope("worktree-innocent"));

    for pass in 0..i64::from(VIOLATION_CEILING) {
        let _ = readiness.ready_target(&hostile_request, pass.saturating_mul(FRESH_WINDOW_MICROS));
    }
    let quarantined = readiness.quarantined_scopes();
    assert_eq!(quarantined.len(), 1);
    assert_eq!(
        quarantined
            .first()
            .expect("one quarantined scope")
            .exact_scope_sha256,
        hostile_request.exact_scope.exact_scope_sha256()
    );

    // The provider is repaired. The quarantined scope stays quarantined —
    // release is an operator decision, not a side effect of a fixed build —
    // while the scope that never misbehaved becomes ready immediately.
    *port
        .namespace
        .write()
        .unwrap_or_else(|poison| poison.into_inner()) = format!("{NATIVE_PROVIDER_ID}.project");
    let target = readiness
        .ready_target(&innocent_request, 20 * FRESH_WINDOW_MICROS)
        .expect("an unaffected scope reaches readiness");
    assert_eq!(target.provider_id().as_str(), NATIVE_PROVIDER_ID);

    match readiness.ready_target(&hostile_request, 21 * FRESH_WINDOW_MICROS) {
        Err(SupervisedReadinessError::Unavailable { kind, .. }) => {
            assert_eq!(kind, DegradationKindV1::Quarantined);
        }
        other => panic!("the quarantined scope must stay quarantined, got {other:?}"),
    }
    assert_eq!(readiness.quarantined_scopes().len(), 1);
}

// ---------------------------------------------------------------------------
// A crash report is bound to an incarnation, so a looping host caller cannot
// quarantine a provider it never even started.
// ---------------------------------------------------------------------------

/// Repeated crash reports against a supervisor that never started an instance
/// are refused host-side: they reach no adapter, count no violation, and can
/// never reach the quarantine ceiling.
///
/// Catches the inverse of the defect quarantine exists for — a host loop that
/// converts its own bogus reports into a quarantine of a healthy, never-even
/// -contacted provider.
#[test]
fn repeated_crash_reports_without_a_live_incarnation_are_refused_host_side() {
    let mut supervisor = supervisor(HostileAdapter::healthy());

    for _ in 0..(VIOLATION_CEILING.saturating_mul(4)) {
        match supervisor.report_crash() {
            SupervisorOutcomeV1::Unavailable(
                DegradationCauseV1::CrashReportWithoutLiveIncarnation,
            ) => {}
            other => panic!("expected a host-side crash-report refusal, got {other:?}"),
        }
    }

    assert_eq!(
        supervisor.adapter().total_calls(),
        0,
        "a refused crash report must contact no adapter"
    );
    assert_eq!(supervisor.provider_violations(), 0);
    assert!(!supervisor.is_quarantined());
    assert_eq!(
        supervisor.current_availability(),
        ProviderAvailabilityV1::NotStarted
    );
    assert!(
        !DegradationKindV1::CrashReportWithoutLiveIncarnation.is_provider_attributable(),
        "a host-side refusal must never be attributed to the provider"
    );
}

/// One incarnation crashes once. A caller repeating the same crash report is
/// refused with the incarnation it names, the violation is counted exactly
/// once, and the provider is not quarantined by the repetition.
#[test]
fn a_duplicate_crash_report_for_one_incarnation_is_counted_once() {
    let mut supervisor = supervisor(HostileAdapter::healthy());
    assert!(matches!(
        supervisor.start_or_restart(&handshake_request(), 0, 1, 2),
        SupervisorOutcomeV1::Ready(_)
    ));
    let incarnation = supervisor.live_incarnation().expect("a live incarnation");

    match supervisor.report_crash() {
        SupervisorOutcomeV1::Unavailable(DegradationCauseV1::Crashed) => {}
        other => panic!("expected the first report to be accepted, got {other:?}"),
    }
    for _ in 0..(VIOLATION_CEILING.saturating_mul(4)) {
        match supervisor.report_crash() {
            SupervisorOutcomeV1::Unavailable(DegradationCauseV1::CrashAlreadyRecorded {
                incarnation: reported,
            }) => assert_eq!(reported, incarnation),
            other => panic!("expected a duplicate crash-report refusal, got {other:?}"),
        }
    }

    assert_eq!(
        supervisor.provider_violations(),
        1,
        "one incarnation's crash counts once no matter how often it is reported"
    );
    assert!(!supervisor.is_quarantined());
    assert_eq!(
        supervisor.current_degradation().map(|record| record.kind()),
        Some(DegradationKindV1::Crashed)
    );
}

/// A crash report after the instance's death was confirmed names no live
/// incarnation and is refused, and a genuinely fresh incarnation can still be
/// reported crashed afterwards.
#[test]
fn a_crash_report_after_confirmed_death_is_refused_but_a_new_incarnation_is_reportable() {
    let mut supervisor = supervisor(HostileAdapter::healthy());
    assert!(matches!(
        supervisor.start_or_restart(&handshake_request(), 0, 1, 2),
        SupervisorOutcomeV1::Ready(_)
    ));
    supervisor.shutdown(1_000).expect("bounded shutdown");

    match supervisor.report_crash() {
        SupervisorOutcomeV1::Unavailable(DegradationCauseV1::CrashReportWithoutLiveIncarnation) => {
        }
        other => panic!("expected a host-side crash-report refusal, got {other:?}"),
    }
    assert_eq!(supervisor.provider_violations(), 0);

    assert!(matches!(
        supervisor.start_or_restart(&handshake_request(), FRESH_WINDOW_MICROS, 1, 2),
        SupervisorOutcomeV1::Ready(_)
    ));
    let second = supervisor.live_incarnation().expect("a second incarnation");
    assert_ne!(second, 1, "a replacement is a different incarnation");
    match supervisor.report_crash() {
        SupervisorOutcomeV1::Unavailable(DegradationCauseV1::Crashed) => {}
        other => panic!("a fresh incarnation must be reportable, got {other:?}"),
    }
    assert_eq!(supervisor.provider_violations(), 1);
}

// ---------------------------------------------------------------------------
// Quarantine outlives the owner that earned it.
// ---------------------------------------------------------------------------

/// A quarantined scope is not the one the finite owner ceiling retires while
/// any un-quarantined owner exists, so scope churn cannot evict the evidence.
///
/// Catches the eviction path that treats a quarantined owner as ordinary cold
/// state: retiring it and rebuilding it from scratch is a silent, automatic
/// quarantine release.
#[test]
fn scope_churn_never_retires_a_quarantined_owner_while_others_exist() {
    let port = Arc::new(MountedHostilePort::new("host.authority.stolen"));
    let readiness = mounted_with_scope_ceiling(Arc::clone(&port), 2);
    let hostile = handshake_request_for(NATIVE_PROVIDER_ID, exact_scope("worktree-hostile"));

    for pass in 0..i64::from(VIOLATION_CEILING) {
        let _ = readiness.ready_target(&hostile, pass.saturating_mul(FRESH_WINDOW_MICROS));
    }
    assert_eq!(readiness.quarantined_scopes().len(), 1);

    // The provider is repaired, so nothing but the quarantine itself can keep
    // this scope from readiness.
    *port
        .namespace
        .write()
        .unwrap_or_else(|poison| poison.into_inner()) = format!("{NATIVE_PROVIDER_ID}.project");

    for (index, worktree) in ["worktree-b", "worktree-c", "worktree-d", "worktree-e"]
        .into_iter()
        .enumerate()
    {
        let request = handshake_request_for(NATIVE_PROVIDER_ID, exact_scope(worktree));
        let now = (10_i64.saturating_add(i64::try_from(index).unwrap_or(0)))
            .saturating_mul(FRESH_WINDOW_MICROS);
        readiness
            .ready_target(&request, now)
            .expect("an innocent scope reaches readiness");
    }
    assert_eq!(readiness.supervised_scopes(), 2);

    let quarantined = readiness.quarantined_scopes();
    assert_eq!(quarantined.len(), 1, "the quarantine evidence must survive");
    assert_eq!(
        quarantined
            .first()
            .expect("one quarantined scope")
            .exact_scope_sha256,
        hostile.exact_scope.exact_scope_sha256()
    );

    let contacts = port.handshake_calls.load(Ordering::Relaxed);
    match readiness.ready_target(&hostile, 30 * FRESH_WINDOW_MICROS) {
        Err(SupervisedReadinessError::Unavailable { kind, .. }) => {
            assert_eq!(kind, DegradationKindV1::Quarantined);
        }
        other => panic!("the churned scope must stay quarantined, got {other:?}"),
    }
    assert_eq!(
        port.handshake_calls.load(Ordering::Relaxed),
        contacts,
        "a returning quarantined scope must make zero adapter calls"
    );
}

/// When every live owner is quarantined the ceiling may still retire one — and
/// the quarantine evidence moves to durable mount-level state instead of being
/// deleted. The retired scope comes back quarantined, contacts no adapter, and
/// is released only explicitly.
///
/// Catches exactly the bypass an adversary would use: churn scopes until the
/// quarantined owner is evicted, then return under the same scope and be
/// handed a fresh supervisor with zero violations.
#[test]
fn a_retired_quarantined_scope_keeps_its_evidence_and_stays_uncontactable() {
    let port = Arc::new(MountedHostilePort::new("host.authority.stolen"));
    let readiness = mounted_with_scope_ceiling(Arc::clone(&port), 2);
    let first = handshake_request_for(NATIVE_PROVIDER_ID, exact_scope("worktree-first"));
    let second = handshake_request_for(NATIVE_PROVIDER_ID, exact_scope("worktree-second"));

    for pass in 0..i64::from(VIOLATION_CEILING) {
        let now = pass.saturating_mul(FRESH_WINDOW_MICROS);
        let _ = readiness.ready_target(&first, now);
        let _ = readiness.ready_target(&second, now);
    }
    assert_eq!(readiness.quarantined_scopes().len(), 2);
    assert_eq!(readiness.supervised_scopes(), 2);

    // Repaired provider, and a third scope that forces the ceiling to retire
    // one of the two quarantined owners.
    *port
        .namespace
        .write()
        .unwrap_or_else(|poison| poison.into_inner()) = format!("{NATIVE_PROVIDER_ID}.project");
    let third = handshake_request_for(NATIVE_PROVIDER_ID, exact_scope("worktree-third"));
    readiness
        .ready_target(&third, 10 * FRESH_WINDOW_MICROS)
        .expect("a fresh scope reaches readiness");
    assert_eq!(readiness.retired_quarantined_scopes(), 1);
    assert_eq!(
        readiness.quarantined_scopes().len(),
        2,
        "retirement must move the evidence, never delete it"
    );

    let contacts = port.handshake_calls.load(Ordering::Relaxed);
    match readiness.ready_target(&first, 11 * FRESH_WINDOW_MICROS) {
        Err(SupervisedReadinessError::Unavailable { kind, .. }) => {
            assert_eq!(kind, DegradationKindV1::Quarantined);
        }
        other => panic!("a retired quarantined scope must stay quarantined, got {other:?}"),
    }
    assert_eq!(
        port.handshake_calls.load(Ordering::Relaxed),
        contacts,
        "a rebuilt quarantined owner must make zero adapter calls"
    );

    // Only an explicit release lets it be contacted again.
    readiness
        .release_quarantine(
            &first.exact_scope.exact_scope_sha256(),
            12 * FRESH_WINDOW_MICROS,
        )
        .expect("explicit release");
    readiness
        .ready_target(&first, 13 * FRESH_WINDOW_MICROS)
        .expect("a released and repaired scope reaches readiness");
    assert!(
        port.handshake_calls.load(Ordering::Relaxed) > contacts,
        "an explicitly released scope contacts its provider again"
    );
}

// ---------------------------------------------------------------------------
// State containment is a host-granted capability, not a validated string.
// ---------------------------------------------------------------------------

/// Through the mounted value a composition root holds, a validated readiness
/// carries a **host-granted** state capability rooted under the host's own
/// directory, an admitted path resolves inside it, and every traversal or
/// absolute path resolves to nothing while the file outside the root is
/// unchanged.
///
/// Catches a state namespace that is only ever compared as a string: the
/// assertion that matters is the byte content of the file outside the root
/// after the provider's own reported namespace has been admitted.
#[test]
fn the_mounted_seam_grants_a_contained_state_capability() {
    let base = std::fs::canonicalize(std::env::temp_dir()).expect("absolute temp dir");
    let base = base
        .join("tdmem-mounted-state-capability")
        .join(format!("{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).expect("scratch directory");
    let outside = base.join("outside-secret.txt");
    std::fs::write(&outside, b"host-owned").expect("outside file");

    let namespace = format!("{NATIVE_PROVIDER_ID}.project");
    let port = Arc::new(MountedHostilePort::new(&namespace));
    let readiness = mounted(Arc::clone(&port))
        .with_state_root(base.join("provider-state"))
        .expect("host-owned state root");
    let request = handshake_request_for(NATIVE_PROVIDER_ID, exact_scope("worktree-mounted"));

    let (_, evidence) = readiness
        .ready_target_with_evidence(&request, 1_000)
        .expect("readiness target and evidence");
    let capability = evidence
        .state_capability()
        .expect("readiness evidence carries the host-granted state capability");
    assert_eq!(capability.state_namespace(), namespace);
    let host_root = readiness
        .state_authority()
        .expect("mounted state authority")
        .root()
        .to_path_buf();
    assert!(
        capability.root().starts_with(&host_root),
        "the granted root must sit under the host-owned root"
    );
    assert!(capability.root().ends_with(&namespace));

    let admitted = capability
        .resolve("facts/first.json")
        .expect("an admitted path resolves");
    assert!(admitted.starts_with(capability.root()));

    for escape in [
        "../../outside-secret.txt",
        "/etc/hosts",
        "facts/../../../outside-secret.txt",
        "~/outside-secret.txt",
    ] {
        assert!(
            capability.resolve(escape).is_err(),
            "{escape} must resolve to nothing"
        );
    }
    assert_eq!(
        std::fs::read(&outside).expect("outside file survives"),
        b"host-owned".to_vec(),
        "no provider-named path may become a write outside the granted root"
    );
}

/// A provider that reports a namespace outside its admitted prefix gets no
/// capability at all, because readiness itself is refused: the host never
/// resolves a state root for a namespace it did not admit.
#[test]
fn an_unadmitted_namespace_is_granted_no_state_capability() {
    let base = std::fs::canonicalize(std::env::temp_dir()).expect("absolute temp dir");
    let port = Arc::new(MountedHostilePort::new("host.authority.stolen"));
    let readiness = mounted(Arc::clone(&port))
        .with_state_root(base.join("tdmem-unadmitted-state-capability"))
        .expect("host-owned state root");
    let request = handshake_request_for(NATIVE_PROVIDER_ID, exact_scope("worktree-mounted"));

    match readiness.ready_target_with_evidence(&request, 1_000) {
        Err(SupervisedReadinessError::Unavailable { kind, .. }) => {
            assert_eq!(kind, DegradationKindV1::HandshakeContractViolation);
        }
        other => panic!("expected a fail-closed readiness refusal, got {other:?}"),
    }
}
