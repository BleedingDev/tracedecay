//! Behavioral proof that provider lifecycle supervision is mountable over a
//! real composed provider set, and that the host keeps working while a
//! supervised provider is unavailable.
#![allow(clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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
    DegradationKindV1, EnabledProviderMode, FabricConfig, NativeProviderActivation,
    ProjectMemoryProviderComposition, RestartBudgetV1, ShutdownBudgetV1,
    SupervisedProviderReadinessV1, SupervisedReadinessConfigV1, SupervisedReadinessError,
};

const ONE_SHA: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const IMPLEMENTATION_SHA: &str = "3333333333333333333333333333333333333333333333333333333333333333";
const RESOLVED_SCOPE_DIGEST: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";

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

fn descriptor() -> ProviderDescriptor {
    ProviderDescriptor::new(
        OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("native provider"),
        IMPLEMENTATION_SHA,
        "native.state.v1",
        3,
        [
            "provider.health.v1",
            "observation.accept.v1",
            "recall.query.v1",
        ]
        .into_iter()
        .map(|value| OwnedVersionedId::new(value).expect("capability")),
        limits(),
    )
    .expect("descriptor")
}

fn descriptor_with_replay() -> ProviderDescriptor {
    ProviderDescriptor::new(
        OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("native provider"),
        IMPLEMENTATION_SHA,
        "native.state.v1",
        3,
        [
            "provider.health.v1",
            "observation.accept.v1",
            "recall.query.v1",
            "replay.apply.v1",
        ]
        .into_iter()
        .map(|value| OwnedVersionedId::new(value).expect("capability")),
        limits(),
    )
    .expect("descriptor")
}

fn exact_scope(worktree: &str) -> OwnedExactScope {
    OwnedExactScope::new(
        "profile-mounted",
        "project-mounted",
        "repository-mounted",
        worktree,
        "refs/heads/mounted",
        "session-mounted",
        RESOLVED_SCOPE_DIGEST,
    )
    .expect("exact scope")
}

fn handshake_request(scope: OwnedExactScope) -> HandshakeRequest {
    HandshakeRequest::new(HandshakeRequestParts {
        provider_id: OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("native provider"),
        registration_revision: 1,
        exact_scope: scope,
        request_id: "mounted-readiness".to_owned(),
        required_capabilities: vec![
            OwnedVersionedId::new("provider.health.v1").expect("health capability"),
        ],
        host_limits: limits(),
        control: OperationControl::new(i64::MAX, 1_000, CancellationToken::new()),
        challenge_nonce: [5; 32],
    })
    .expect("handshake request")
}

/// A Native application port that answers a real handshake, and can be told
/// to answer a *malformed* success so the mounted path's own validation is
/// exercised rather than only the supervisor unit's.
struct MountedNativePort {
    descriptor: ProviderDescriptor,
    omit_state_namespace: AtomicBool,
    handshake_calls: AtomicUsize,
}

impl MountedNativePort {
    fn new() -> Self {
        Self {
            descriptor: descriptor(),
            omit_state_namespace: AtomicBool::new(false),
            handshake_calls: AtomicUsize::new(0),
        }
    }

    /// A provider that also declares the replay capability, which is the only
    /// sanctioned channel for a provider-local acknowledged position.
    fn retaining_a_replay_position() -> Self {
        Self {
            descriptor: descriptor_with_replay(),
            omit_state_namespace: AtomicBool::new(false),
            handshake_calls: AtomicUsize::new(0),
        }
    }
}

impl NativeMemoryApplicationPort for MountedNativePort {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        self.handshake_calls.fetch_add(1, Ordering::Relaxed);
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
            provider_instance_id: Some("native.mounted-instance".to_owned()),
            state_namespace: if self.omit_state_namespace.load(Ordering::Relaxed) {
                None
            } else {
                Some("native.mounted-namespace".to_owned())
            },
            accepted_scope: Some(request.exact_scope.clone()),
            effective_limits: Some(request.host_limits.minimum(self.descriptor.limits)),
            ready_receipt_sha256: Some(ONE_SHA.to_owned()),
            warnings: Vec::new(),
        }
    }

    fn health(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn observe(&self, _observation: NativeObservation<'_>) -> ProviderReply {
        unexpected()
    }

    fn recall(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn feedback(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn maintenance(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn inspection(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn correction(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn delete_by_source(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn snapshot_export(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn snapshot_restore(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn replay(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }
}

fn unexpected<T>() -> T {
    panic!("supervised readiness tests must not execute provider operations")
}

fn enabled_composition(port: Arc<MountedNativePort>) -> Arc<ProjectMemoryProviderComposition> {
    Arc::new(
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
    )
}

fn disabled_composition() -> Arc<ProjectMemoryProviderComposition> {
    Arc::new(
        ProjectMemoryProviderComposition::compose(NativeProviderActivation::Disabled)
            .expect("disabled composition"),
    )
}

fn readiness_config(max_supervised_scopes: usize) -> SupervisedReadinessConfigV1 {
    SupervisedReadinessConfigV1 {
        restart_budget: RestartBudgetV1 {
            max_attempts_per_window: 3,
            window_micros: 60_000_000,
            backoff_base_micros: 1_000,
            backoff_max_micros: 8_000,
        },
        shutdown_budget: ShutdownBudgetV1 {
            grace_micros: 5_000,
            kill_micros: 2_000,
        },
        start_budget_micros: 1_000_000,
        handshake_budget_micros: 1_000_000,
        max_supervised_scopes,
    }
}

fn mount(
    composition: Arc<ProjectMemoryProviderComposition>,
    max_supervised_scopes: usize,
) -> SupervisedProviderReadinessV1 {
    SupervisedProviderReadinessV1::new(
        composition,
        Arc::new(ThreadBoundedProviderCallV1::default()),
        OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("native provider"),
        1,
        limits(),
        readiness_config(max_supervised_scopes),
    )
    .expect("mounted supervised readiness")
}

/// A supervised readiness pass over a real composed provider set reaches a
/// readiness target built from the provider's own validated handshake.
/// Catches a mount that never actually drives the supervisor.
#[test]
fn supervised_readiness_over_a_real_composition_reaches_a_readiness_target() {
    let port = Arc::new(MountedNativePort::new());
    let readiness = mount(enabled_composition(Arc::clone(&port)), 4);
    let request = handshake_request(exact_scope("worktree-a"));

    let target = readiness
        .ready_target(&request, 1_000)
        .expect("readiness target");
    assert_eq!(target.provider_id().as_str(), NATIVE_PROVIDER_ID);
    assert_eq!(target.provider_instance_id(), "native.mounted-instance");
    assert_eq!(target.registration_revision(), 1);
    assert_eq!(target.ready_receipt_sha256(), ONE_SHA);
    assert_eq!(port.handshake_calls.load(Ordering::Relaxed), 1);
    assert_eq!(readiness.supervised_scopes(), 1);
}

/// One handshake yields both the delivery address and the provider state
/// identity restart recovery compares against. Catches a mount that would take
/// the address from one incarnation and the state evidence from another, or
/// that would spend a second handshake to obtain the second half.
#[test]
fn one_readiness_pass_carries_both_the_target_and_the_state_evidence() {
    let port = Arc::new(MountedNativePort::new());
    let readiness = mount(enabled_composition(Arc::clone(&port)), 4);
    let request = handshake_request(exact_scope("worktree-a"));

    let (target, evidence) = readiness
        .ready_target_with_evidence(&request, 1_000)
        .expect("readiness target and evidence");
    assert_eq!(port.handshake_calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        target.provider_instance_id(),
        evidence.provider_instance_id()
    );
    assert_eq!(
        target.ready_receipt_sha256(),
        evidence.ready_receipt_sha256()
    );
    assert_eq!(evidence.state_schema_version(), "native.state.v1");
    assert_eq!(evidence.state_generation(), 3);
    assert_eq!(
        evidence.implementation_identity_sha256(),
        IMPLEMENTATION_SHA
    );
    assert!(
        !evidence.retains_replay_position(),
        "a provider that declares no replay capability must be reported as keeping no \
         replay position, not as evidence the host may ignore"
    );
}

/// The replay-position policy is read from the incarnation's own validated
/// descriptor, so a host can tell a provider that keeps an acknowledged
/// position apart from one that keeps none. Without this the two collapse into
/// a single absent value and restart recovery silently stops comparing.
#[test]
fn readiness_evidence_reports_whether_the_provider_retains_a_replay_position() {
    let port = Arc::new(MountedNativePort::retaining_a_replay_position());
    let readiness = mount(enabled_composition(Arc::clone(&port)), 4);
    let request = handshake_request(exact_scope("worktree-replay"));

    let (_, evidence) = readiness
        .ready_target_with_evidence(&request, 1_000)
        .expect("readiness target and evidence");
    assert!(evidence.retains_replay_position());
}

/// A disabled composition produces typed unavailability on every pass, and
/// the host keeps running: repeated passes stay typed, allocate no extra
/// owner, and never panic. This is the acceptance criterion "host remains
/// usable when the provider is unavailable" at the mounted seam.
#[test]
fn a_disabled_composition_is_typed_unavailability_and_the_host_continues() {
    let readiness = mount(disabled_composition(), 4);
    let request = handshake_request(exact_scope("worktree-a"));

    for pass in 0..4_i64 {
        // Instants far enough apart to clear the enforced backoff, so the
        // refusal under test is the composition's, not the pacing's.
        let error = readiness
            .ready_target(&request, pass.saturating_mul(10_000_000))
            .expect_err("disabled composition cannot be ready");
        match error {
            SupervisedReadinessError::Unavailable { kind, .. } => assert!(
                matches!(
                    kind,
                    DegradationKindV1::StartFailed | DegradationKindV1::RestartBudgetExhausted
                ),
                "unexpected degradation kind {kind}"
            ),
            other => panic!("expected typed unavailability, got {other}"),
        }
    }
    assert_eq!(readiness.supervised_scopes(), 1);

    // The host still has a working, bounded mount afterwards: a different
    // exact scope is still admitted into supervision.
    let other = handshake_request(exact_scope("worktree-b"));
    assert!(readiness.owner_for(&other, 90_000_000).is_ok());
    assert_eq!(readiness.supervised_scopes(), 2);
}

/// Two distinct worktrees get two distinct owners; the finite ceiling holds
/// by retiring the coldest scope after confirming its instance is dead, never
/// by growing an unbounded map of supervisors and never by refusing every new
/// scope for the rest of the process's life.
#[test]
fn distinct_worktrees_get_distinct_owners_under_a_finite_ceiling() {
    let port = Arc::new(MountedNativePort::new());
    let readiness = mount(enabled_composition(port), 2);

    let first = readiness
        .owner_for(&handshake_request(exact_scope("worktree-a")), 1_000)
        .expect("first owner");
    let second = readiness
        .owner_for(&handshake_request(exact_scope("worktree-b")), 2_000)
        .expect("second owner");
    assert_ne!(first.exact_scope_sha256(), second.exact_scope_sha256());

    // The same scope reuses its own owner rather than creating a second one,
    // and using it makes it the warmer of the two.
    let again = readiness
        .owner_for(&handshake_request(exact_scope("worktree-a")), 3_000)
        .expect("existing owner");
    assert!(Arc::ptr_eq(&first, &again));
    assert_eq!(readiness.supervised_scopes(), 2);

    // A third scope retires the coldest (`worktree-b`) and stays at the
    // ceiling.
    let third = readiness
        .owner_for(&handshake_request(exact_scope("worktree-c")), 4_000)
        .expect("third owner");
    assert_eq!(readiness.supervised_scopes(), 2);
    assert_ne!(third.exact_scope_sha256(), second.exact_scope_sha256());

    // `worktree-a` was the warm one, so it survived untouched.
    let survivor = readiness
        .owner_for(&handshake_request(exact_scope("worktree-a")), 5_000)
        .expect("surviving owner");
    assert!(Arc::ptr_eq(&first, &survivor));

    // The retired scope comes back as a *fresh* owner, never the retired one.
    let revived = readiness
        .owner_for(&handshake_request(exact_scope("worktree-b")), 6_000)
        .expect("revived owner");
    assert!(!Arc::ptr_eq(&second, &revived));
    assert_eq!(readiness.supervised_scopes(), 2);
}

/// A healthy provider answering many readiness requests spends **no** restart
/// budget: the steady-state path re-proves readiness with one handshake and
/// never restarts. Catches a mount that treats every request as a restart and
/// wedges a working provider into `RestartBudgetExhausted`.
#[test]
fn a_healthy_provider_never_spends_its_crash_loop_budget() {
    let port = Arc::new(MountedNativePort::new());
    let mut config = readiness_config(4);
    // One single spawn attempt per window: any restart at all would fail the
    // second request.
    config.restart_budget.max_attempts_per_window = 1;
    let readiness = SupervisedProviderReadinessV1::new(
        enabled_composition(Arc::clone(&port)),
        Arc::new(ThreadBoundedProviderCallV1::default()),
        OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("native provider"),
        1,
        limits(),
        config,
    )
    .expect("mounted supervised readiness");
    let request = handshake_request(exact_scope("worktree-a"));

    for pass in 0..8_i64 {
        readiness
            .ready_target(&request, 1_000 + pass)
            .expect("readiness target on a healthy provider");
    }
    // Readiness is proven per request, so the provider really was contacted
    // every time — the budget simply was not spent doing it.
    assert_eq!(port.handshake_calls.load(Ordering::Relaxed), 8);
}

/// An owner bound to one worktree refuses a request for another, and the
/// refusal is the supervisor's typed scope mismatch. Catches an owner map
/// that hands the wrong supervisor to a scope.
#[test]
fn an_owner_refuses_a_request_for_another_worktree() {
    let port = Arc::new(MountedNativePort::new());
    let readiness = mount(enabled_composition(Arc::clone(&port)), 4);
    let owner = readiness
        .owner_for(&handshake_request(exact_scope("worktree-a")), 1_000)
        .expect("owner");

    let foreign = handshake_request(exact_scope("worktree-b"));
    match owner.ready_target(&foreign, 1_000) {
        Err(SupervisedReadinessError::Unavailable { kind, .. }) => {
            assert_eq!(kind, DegradationKindV1::ScopeMismatch);
        }
        other => panic!("expected a scope mismatch, got {other:?}"),
    }
    assert_eq!(port.handshake_calls.load(Ordering::Relaxed), 0);
}

/// A provider that reports a build identity other than the pinned one is
/// refused fail-closed by the mounted path. This defect is invisible to the
/// fabric — only the supervisor knows what the host pinned — so it catches a
/// mount that never runs the supervisor's own readiness validation.
#[test]
fn an_unpinned_build_identity_is_refused_at_the_mounted_seam() {
    let port = Arc::new(MountedNativePort::new());
    let readiness = SupervisedProviderReadinessV1::new(
        enabled_composition(Arc::clone(&port)),
        Arc::new(ThreadBoundedProviderCallV1::default()),
        OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("native provider"),
        1,
        limits(),
        readiness_config(4),
    )
    .expect("mounted supervised readiness")
    .with_pinned_identity(
        Some("4444444444444444444444444444444444444444444444444444444444444444".to_owned()),
        None,
    );
    let request = handshake_request(exact_scope("worktree-a"));

    match readiness.ready_target(&request, 1_000) {
        Err(SupervisedReadinessError::Unavailable { kind, detail, .. }) => {
            assert_eq!(kind, DegradationKindV1::HandshakeContractViolation);
            assert!(detail.contains("pinned"), "unexpected detail: {detail}");
        }
        other => panic!("expected a contract violation, got {other:?}"),
    }
    assert_eq!(port.handshake_calls.load(Ordering::Relaxed), 1);
}

/// A provider that answers `Success` while omitting its state namespace never
/// yields a readiness target. The refusal is typed and the host continues;
/// once the provider answers correctly the very next admitted pass succeeds,
/// so the mount holds no sticky failure.
#[test]
fn a_malformed_success_never_yields_a_readiness_target() {
    let port = Arc::new(MountedNativePort::new());
    port.omit_state_namespace.store(true, Ordering::Relaxed);
    let readiness = mount(enabled_composition(Arc::clone(&port)), 4);
    let request = handshake_request(exact_scope("worktree-a"));

    match readiness.ready_target(&request, 1_000) {
        Err(SupervisedReadinessError::Unavailable { kind, .. }) => assert!(
            matches!(
                kind,
                DegradationKindV1::HandshakeContractViolation
                    | DegradationKindV1::HandshakeTransportFailed
            ),
            "unexpected degradation kind {kind}"
        ),
        other => panic!("expected typed unavailability, got {other:?}"),
    }
    assert_eq!(port.handshake_calls.load(Ordering::Relaxed), 1);

    port.omit_state_namespace.store(false, Ordering::Relaxed);
    assert!(readiness.ready_target(&request, 10_000_000).is_ok());
}

/// A crash reported through the mounted owner produces the typed `Crashed`
/// degradation and invalidates readiness. Catches a crash report that only
/// flips a coarse availability flag.
#[test]
fn a_crash_reported_through_the_mount_is_typed() {
    let port = Arc::new(MountedNativePort::new());
    let readiness = mount(enabled_composition(port), 4);
    let request = handshake_request(exact_scope("worktree-a"));
    let owner = readiness.owner_for(&request, 500).expect("owner");
    assert!(owner.ready_target(&request, 1_000).is_ok());

    assert_eq!(
        owner.report_crash().expect("crash"),
        DegradationKindV1::Crashed
    );
    assert_eq!(
        owner.current_degradation(),
        Some(DegradationKindV1::Crashed)
    );
}

/// A provider whose handshake never returns is a genuinely non-returning
/// call, not an error: this port blocks until the test releases it.
struct HangingNativePort {
    descriptor: ProviderDescriptor,
    gate: std::sync::Mutex<bool>,
    released: std::sync::Condvar,
    hanging: AtomicBool,
    handshake_calls: AtomicUsize,
    entered: AtomicUsize,
}

impl HangingNativePort {
    fn new() -> Self {
        Self {
            descriptor: descriptor(),
            gate: std::sync::Mutex::new(false),
            released: std::sync::Condvar::new(),
            hanging: AtomicBool::new(true),
            handshake_calls: AtomicUsize::new(0),
            entered: AtomicUsize::new(0),
        }
    }

    /// Releases every blocked handshake so the abandoned worker can exit.
    fn release(&self) {
        let mut released = self
            .gate
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        *released = true;
        self.released.notify_all();
    }

    fn block_until_released(&self) {
        self.entered.fetch_add(1, Ordering::SeqCst);
        let mut released = self
            .gate
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        // The wall-clock cap is a test-suite safety net, never the bound under
        // test: the assertion is that the *host* returned long before it.
        let deadline = std::time::Duration::from_secs(30);
        let started = std::time::Instant::now();
        while !*released && started.elapsed() < deadline {
            let (guard, _) = self
                .released
                .wait_timeout(released, std::time::Duration::from_millis(50))
                .unwrap_or_else(|poison| poison.into_inner());
            released = guard;
        }
    }
}

impl NativeMemoryApplicationPort for HangingNativePort {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        self.handshake_calls.fetch_add(1, Ordering::SeqCst);
        if self.hanging.load(Ordering::SeqCst) {
            self.block_until_released();
        }
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
            provider_instance_id: Some("native.hanging-instance".to_owned()),
            state_namespace: Some("native.mounted-namespace".to_owned()),
            accepted_scope: Some(request.exact_scope.clone()),
            effective_limits: Some(request.host_limits.minimum(self.descriptor.limits)),
            ready_receipt_sha256: Some(ONE_SHA.to_owned()),
            warnings: Vec::new(),
        }
    }

    fn health(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn observe(&self, _observation: NativeObservation<'_>) -> ProviderReply {
        unexpected()
    }

    fn recall(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn feedback(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn maintenance(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn inspection(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn correction(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn delete_by_source(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn snapshot_export(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn snapshot_restore(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }

    fn replay(&self, _call: &ProviderCall) -> ProviderReply {
        unexpected()
    }
}

fn hanging_composition(port: Arc<HangingNativePort>) -> Arc<ProjectMemoryProviderComposition> {
    Arc::new(
        ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
            fabric_config: FabricConfig {
                max_registered_providers: 1,
                max_in_flight: 4,
            },
            port,
            registration_revision: 1,
            mode: EnabledProviderMode::Active,
        })
        .expect("enabled composition"),
    )
}

/// A provider whose handshake **never returns** does not wedge the host: the
/// supervised pass returns a typed refusal inside its own budget, the host
/// stays usable while the provider is still stuck, and the same mount reaches
/// readiness once the provider answers again.
///
/// This is the acceptance criterion "host remains bounded and usable" against
/// a genuinely non-returning call rather than a fast error. It fails against
/// any lifecycle path that synchronously invokes the provider and merely
/// carries a deadline value alongside it.
#[test]
fn a_provider_that_never_returns_is_abandoned_and_the_host_stays_usable() {
    let port = Arc::new(HangingNativePort::new());
    let mut config = readiness_config(4);
    // 100ms is the whole budget a handshake gets. The provider is stuck for
    // 30 seconds.
    config.handshake_budget_micros = 100_000;
    let readiness = SupervisedProviderReadinessV1::new(
        hanging_composition(Arc::clone(&port)),
        Arc::new(ThreadBoundedProviderCallV1::default()),
        OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("native provider"),
        1,
        limits(),
        config,
    )
    .expect("mounted supervised readiness");
    let request = handshake_request(exact_scope("worktree-hanging"));

    let started = std::time::Instant::now();
    let error = readiness
        .ready_target(&request, 1_000)
        .expect_err("a provider that never answers cannot be ready");
    let elapsed = started.elapsed();
    match error {
        SupervisedReadinessError::Unavailable { kind, detail, .. } => {
            assert_eq!(kind, DegradationKindV1::HandshakeTransportFailed);
            assert!(detail.contains("abandoned"), "unexpected detail: {detail}");
        }
        other => panic!("expected typed unavailability, got {other}"),
    }
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "the host waited {elapsed:?} on a provider it gave 100ms"
    );
    assert_eq!(
        port.entered.load(Ordering::SeqCst),
        1,
        "the provider really was entered and really did not return"
    );

    // The host is still usable while the provider is still stuck: another
    // exact scope is admitted and refused typed, again inside its budget.
    let other = handshake_request(exact_scope("worktree-other"));
    let second_started = std::time::Instant::now();
    assert!(
        readiness.ready_target(&other, 200_000_000).is_err(),
        "a stuck provider cannot make another scope ready"
    );
    assert!(
        second_started.elapsed() < std::time::Duration::from_secs(5),
        "a second scope must not inherit the first scope's hang"
    );

    // The provider answers again, and the same mount reaches readiness: the
    // abandonment left no sticky failure.
    port.hanging.store(false, Ordering::SeqCst);
    port.release();
    let recovered = handshake_request(exact_scope("worktree-recovered"));
    readiness
        .ready_target(&recovered, 400_000_000)
        .expect("a provider that answers again reaches readiness");
}

/// A caller whose own deadline has already elapsed is refused before any
/// adapter is contacted at all.
#[test]
fn an_elapsed_caller_deadline_is_refused_without_contacting_the_provider() {
    let port = Arc::new(MountedNativePort::new());
    let readiness = mount(enabled_composition(Arc::clone(&port)), 4);
    let request = HandshakeRequest::new(HandshakeRequestParts {
        provider_id: OwnedProviderId::new(NATIVE_PROVIDER_ID).expect("native provider"),
        registration_revision: 1,
        exact_scope: exact_scope("worktree-a"),
        request_id: "elapsed-readiness".to_owned(),
        required_capabilities: vec![
            OwnedVersionedId::new("provider.health.v1").expect("health capability"),
        ],
        host_limits: limits(),
        control: OperationControl::new(5_000, 1_000, CancellationToken::new()),
        challenge_nonce: [5; 32],
    })
    .expect("handshake request");

    match readiness.ready_target(&request, 6_000) {
        Err(SupervisedReadinessError::DeadlineElapsed {
            deadline_utc_micros,
            ..
        }) => assert_eq!(deadline_utc_micros, 5_000),
        other => panic!("expected an elapsed-deadline refusal, got {other:?}"),
    }
    assert_eq!(port.handshake_calls.load(Ordering::Relaxed), 0);
}
