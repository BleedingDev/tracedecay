//! Injected registration preserves provider identity, participation and owner authority.
#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use tracedecay_memory_provider_api::MemoryProvider;
use tracedecay_memory_provider_registry::*;

const SHA: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const EMPTY_SHA: &str = "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a";

fn limits() -> ProviderLimits {
    ProviderLimits {
        request_bytes: 65_536,
        response_bytes: 32_768,
        observation_batch_items: 8,
        recall_candidates: 7,
        concurrent_operations: 2,
        operation_millis: 1_000,
        snapshot_bytes: 65_536,
        inspection_items: 16,
    }
}

fn config() -> FabricConfig {
    FabricConfig {
        max_registered_providers: 3,
        max_in_flight: 2,
    }
}

struct FixtureProvider {
    descriptor: ProviderDescriptor,
    calls: AtomicUsize,
}

impl FixtureProvider {
    fn new(id: &str, common: bool) -> Arc<Self> {
        let capabilities = if common {
            std::iter::once(COMMON_ADVISORY_PROFILE_ID)
                .chain(COMMON_ADVISORY_REQUIRED_CAPABILITIES.iter().copied())
                .collect()
        } else {
            vec![
                "provider.health.v1",
                "observation.accept.v1",
                "recall.query.v1",
            ]
        };
        Arc::new(Self {
            descriptor: ProviderDescriptor::new(
                OwnedProviderId::new(id).unwrap(),
                SHA,
                "fixture.state.v1",
                1,
                capabilities
                    .into_iter()
                    .map(|id| OwnedVersionedId::new(id).unwrap()),
                limits(),
            )
            .unwrap(),
            calls: AtomicUsize::new(0),
        })
    }
}

impl MemoryProvider for FixtureProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        self.calls.fetch_add(1, Ordering::SeqCst);
        HandshakeResponse {
            terminal: TerminalRecord::new(
                ProviderOperation::Handshake,
                self.descriptor.provider_id.clone(),
                TerminalCode::Success,
                CommittedEffectEvidence::none(Some(1)),
                FallbackDirective::forbidden(),
                request.request_id.clone(),
                request.exact_scope.exact_scope_sha256(),
                None,
            )
            .unwrap(),
            descriptor: Some(self.descriptor.clone()),
            provider_instance_id: Some(format!("{}.fixture", self.descriptor.provider_id.as_str())),
            state_namespace: Some("fixture.exact-scope".to_owned()),
            accepted_scope: Some(request.exact_scope.clone()),
            effective_limits: Some(request.host_limits.minimum(self.descriptor.limits)),
            ready_receipt_sha256: Some(SHA.to_owned()),
            warnings: Vec::new(),
        }
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        self.calls.fetch_add(1, Ordering::SeqCst);
        ProviderReply {
            terminal: TerminalRecord::new(
                call.operation,
                self.descriptor.provider_id.clone(),
                TerminalCode::Success,
                CommittedEffectEvidence::none(Some(1)),
                FallbackDirective::forbidden(),
                call.operation_id.clone(),
                call.exact_scope.exact_scope_sha256(),
                None,
            )
            .unwrap(),
            payload: Some(call.payload.clone()),
            warnings: Vec::new(),
            extensions: Vec::new(),
            state_generation: 1,
        }
    }
}

fn registration(
    provider: Arc<FixtureProvider>,
    mode: EnabledProviderMode,
) -> ProviderRegistrationV1 {
    ProviderRegistrationV1 {
        provider_id: provider.descriptor.provider_id.clone(),
        provider,
        registration_revision: 19,
        mode,
        execution_shape: ProviderExecutionShapeV1::HostAuthoredInProcess,
        recall_scope_bindings: RecallScopeBindingsV1::from_wire(["exact_coding_scope"]).unwrap(),
        lifecycle: ProviderLifecycleOwnershipV1::CompositionBound,
    }
}

fn selection(registration: ProviderRegistrationV1) -> SelectedProviderActivationV1 {
    SelectedProviderActivationV1::Injected {
        fabric_config: config(),
        registration,
    }
}

fn handshake(id: &str) -> HandshakeRequest {
    HandshakeRequest::new(HandshakeRequestParts {
        provider_id: OwnedProviderId::new(id).unwrap(),
        registration_revision: 19,
        exact_scope: OwnedExactScope::new(
            "profile",
            "project",
            "repository",
            "worktree",
            "refs/heads/main",
            "session",
            format!("sha256:{SHA}"),
        )
        .unwrap(),
        request_id: "handshake.fixture".to_owned(),
        required_capabilities: vec![OwnedVersionedId::new(COMMON_ADVISORY_PROFILE_ID).unwrap()],
        host_limits: limits(),
        control: OperationControl::new(i64::MAX, 1_000, CancellationToken::new()),
        challenge_nonce: [3; 32],
    })
    .unwrap()
}

fn recall_call(id: &str) -> ProviderCall {
    ProviderCall::new(ProviderCallParts {
        operation: ProviderOperation::Recall,
        provider_id: OwnedProviderId::new(id).unwrap(),
        registration_revision: 19,
        ready_receipt_sha256: SHA.to_owned(),
        exact_scope: handshake(id).exact_scope,
        request_id: "request.fixture".to_owned(),
        operation_id: "recall.fixture".to_owned(),
        expected_state_generation: 1,
        idempotency_key: None,
        control: OperationControl::new(i64::MAX, 1_000, CancellationToken::new()),
        payload: CanonicalPayload::new(
            OwnedVersionedId::new("tracedecay.memory.provider.recall.v1").unwrap(),
            b"{}".to_vec(),
            EMPTY_SHA,
        )
        .unwrap(),
        required_capabilities: vec![OwnedVersionedId::new("recall.query.v1").unwrap()],
        extensions: Vec::new(),
    })
    .unwrap()
}

#[test]
fn injected_ncm_active_has_actual_metadata_without_a_native_registration() {
    let provider = FixtureProvider::new("ncm", true);
    let composition = ProjectMemoryProviderComposition::compose_registered(
        selection(registration(provider.clone(), EnabledProviderMode::Active)),
        vec![],
    )
    .unwrap();
    let registry = composition.registry().unwrap();
    let selected = registry.selected_registration().unwrap();
    assert_eq!(selected.provider_id.as_str(), "ncm");
    assert_eq!(selected.registration_revision, 19);
    assert!(selected.requires_common_advisory_profile);
    assert_eq!(selected.limits, provider.descriptor.limits);
    assert_eq!(registry.statuses().unwrap().len(), 1);
    assert!(
        registry
            .recall_scope_bindings(&selected.provider_id)
            .unwrap()
            .authorizes(ScopeBinding::ExactCodingScope)
    );
    assert!(
        registry
            .registration(&OwnedProviderId::new(NATIVE_PROVIDER_ID).unwrap())
            .is_none()
    );
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        0,
        "registration performs no provider work"
    );
}

#[test]
fn injected_compatibility_depends_on_profile_and_identity_not_provider_name() {
    for id in ["ncm", "tracedecay.native", "vendor.compatible"] {
        let incompatible = FixtureProvider::new(id, false);
        assert!(matches!(
            ProjectMemoryProviderComposition::compose_registered(
                selection(registration(
                    incompatible.clone(),
                    EnabledProviderMode::Active
                )),
                vec![]
            ),
            Err(RegistryError::Api(ApiError::MandatoryCapabilityMissing(
                COMMON_ADVISORY_PROFILE_ID
            )))
        ));
        assert_eq!(incompatible.calls.load(Ordering::SeqCst), 0);
        assert!(
            ProjectMemoryProviderComposition::compose_registered(
                selection(registration(
                    FixtureProvider::new(id, true),
                    EnabledProviderMode::Active
                )),
                vec![]
            )
            .is_ok()
        );
    }
    let mut injected = registration(
        FixtureProvider::new("ncm", true),
        EnabledProviderMode::Active,
    );
    injected.provider_id = OwnedProviderId::new(NATIVE_PROVIDER_ID).unwrap();
    assert!(
        matches!(ProjectMemoryProviderComposition::compose_registered(selection(injected), vec![]),
        Err(RegistryError::SelectedProviderIdentityMismatch { expected, declared }) if expected == NATIVE_PROVIDER_ID && declared == "ncm")
    );
}

#[test]
fn native_observer_on_or_off_cannot_change_ncm_active_output() {
    let mut replies = Vec::new();
    for with_observer in [false, true] {
        let ncm = FixtureProvider::new("ncm", true);
        let native = FixtureProvider::new(NATIVE_PROVIDER_ID, true);
        let observers = if with_observer {
            vec![registration(native.clone(), EnabledProviderMode::Observer)]
        } else {
            vec![]
        };
        let composition = ProjectMemoryProviderComposition::compose_registered(
            selection(registration(ncm.clone(), EnabledProviderMode::Active)),
            observers,
        )
        .unwrap();
        let registry = composition.registry().unwrap();
        registry.handshake(&handshake("ncm")).unwrap();
        let reply = registry.invoke_active(&recall_call("ncm")).unwrap();
        assert_eq!(reply.terminal.provider_id().as_str(), "ncm");
        replies.push(reply.payload.unwrap().bytes);
        if with_observer {
            assert!(
                registry
                    .recall_scope_bindings(&native.descriptor.provider_id)
                    .is_none()
            );
            assert!(matches!(
                registry.invoke_active(&recall_call(NATIVE_PROVIDER_ID)),
                Err(FabricError::ProviderObserverOnly(_))
            ));
        }
        assert_eq!(native.calls.load(Ordering::SeqCst), 0);
        assert_eq!(ncm.calls.load(Ordering::SeqCst), 2);
    }
    assert_eq!(replies[0], replies[1]);
}

#[derive(Default)]
struct Owner {
    starts: AtomicUsize,
    stops: AtomicUsize,
    kills: AtomicUsize,
    terminated: AtomicBool,
}

impl ProviderLifecycleOwnerV1 for Owner {
    fn start(&self, _: i64) -> Result<(), ProviderLifecycleOwnerErrorV1> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn request_stop(&self, _: i64) -> Result<bool, ProviderLifecycleOwnerErrorV1> {
        self.stops.fetch_add(1, Ordering::SeqCst);
        Ok(self.terminated.load(Ordering::SeqCst))
    }
    fn kill(&self, _: i64) -> Result<(), ProviderLifecycleOwnerErrorV1> {
        self.kills.fetch_add(1, Ordering::SeqCst);
        if self.terminated.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(ProviderLifecycleOwnerErrorV1::TerminationUnconfirmed)
        }
    }
}

#[derive(Debug)]
struct NoHandshake;
impl BoundedProviderCallV1 for NoHandshake {
    fn handshake_within(
        &self,
        _: u64,
        _: &CancellationToken,
        _: ProviderHandshakeWorkV1,
    ) -> Result<Result<HandshakeResponse, CompositionLifecycleError>, BoundedCallRefusalV1> {
        panic!("lifecycle transitions must not fabricate readiness");
    }
}

#[test]
fn observer_only_uses_its_existing_owner_and_never_claims_unconfirmed_termination() {
    let owner = Arc::new(Owner::default());
    let observer = FixtureProvider::new("ncm", true);
    let mut injected = registration(observer.clone(), EnabledProviderMode::Observer);
    injected.execution_shape = ProviderExecutionShapeV1::Foreign;
    injected.lifecycle = ProviderLifecycleOwnershipV1::Owned(owner.clone());
    let composition = Arc::new(
        ProjectMemoryProviderComposition::compose_registered(
            SelectedProviderActivationV1::ObserversOnly {
                fabric_config: config(),
            },
            vec![injected],
        )
        .unwrap(),
    );
    let registry = composition.registry().unwrap();
    assert!(registry.selected_registration().is_none());
    assert!(
        registry
            .recall_scope_bindings(&observer.descriptor.provider_id)
            .is_none()
    );
    assert_eq!(owner.starts.load(Ordering::SeqCst), 0);
    let lifecycle = CompositionLifecycleAdapterV1::for_provider(
        composition,
        Arc::new(NoHandshake),
        observer.descriptor.provider_id.clone(),
    );
    lifecycle.start(i64::MAX).unwrap();
    assert!(!lifecycle.request_stop(i64::MAX).unwrap());
    assert!(matches!(
        lifecycle.kill(i64::MAX),
        Err(CompositionLifecycleError::Owner(
            ProviderLifecycleOwnerErrorV1::TerminationUnconfirmed
        ))
    ));
    owner.terminated.store(true, Ordering::SeqCst);
    lifecycle.kill(i64::MAX).unwrap();
    assert_eq!(owner.starts.load(Ordering::SeqCst), 1);
    assert_eq!(owner.stops.load(Ordering::SeqCst), 1);
    assert_eq!(owner.kills.load(Ordering::SeqCst), 2);
    assert_eq!(observer.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn disabled_creates_no_registry_or_lifecycle_work() {
    assert!(matches!(
        ProjectMemoryProviderComposition::compose_registered(
            SelectedProviderActivationV1::Disabled,
            vec![]
        )
        .unwrap(),
        ProjectMemoryProviderComposition::Disabled
    ));
    assert!(matches!(
        ProjectMemoryProviderComposition::compose_registered(
            SelectedProviderActivationV1::ObserversOnly {
                fabric_config: config()
            },
            vec![]
        )
        .unwrap(),
        ProjectMemoryProviderComposition::Disabled
    ));
}

#[test]
fn foreign_execution_requires_an_owner_and_observer_mode_cannot_promote_itself() {
    let mut injected = registration(
        FixtureProvider::new("ncm", true),
        EnabledProviderMode::Active,
    );
    injected.execution_shape = ProviderExecutionShapeV1::Foreign;
    assert!(
        matches!(ProjectMemoryProviderComposition::compose_registered(selection(injected), vec![]), Err(RegistryError::LifecycleOwnerRequired(provider)) if provider == "ncm")
    );
    assert!(matches!(
        ProjectMemoryProviderComposition::compose_registered(
            SelectedProviderActivationV1::ObserversOnly {
                fabric_config: config()
            },
            vec![registration(
                FixtureProvider::new("ncm", true),
                EnabledProviderMode::Active
            )],
        ),
        Err(RegistryError::ObserverModeRequired(_))
    ));
}

struct ChangingDescriptor {
    first: ProviderDescriptor,
    registered: ProviderDescriptor,
    reads: AtomicUsize,
}

impl MemoryProvider for ChangingDescriptor {
    fn descriptor(&self) -> ProviderDescriptor {
        if self.reads.fetch_add(1, Ordering::SeqCst) == 0 {
            self.first.clone()
        } else {
            self.registered.clone()
        }
    }

    fn handshake(&self, _: &HandshakeRequest) -> HandshakeResponse {
        panic!("registration must not handshake");
    }

    fn invoke(&self, _: &ProviderCall) -> ProviderReply {
        panic!("registration must not invoke operations");
    }
}

#[test]
fn profile_and_limits_come_from_the_descriptor_retained_by_the_fabric() {
    let first = FixtureProvider::new("ncm", true);
    let dropped_profile = FixtureProvider::new("ncm", false);
    let mut injected = registration(first.clone(), EnabledProviderMode::Active);
    injected.provider = Arc::new(ChangingDescriptor {
        first: first.descriptor.clone(),
        registered: dropped_profile.descriptor.clone(),
        reads: AtomicUsize::new(0),
    });
    assert!(matches!(
        ProjectMemoryProviderComposition::compose_registered(selection(injected), vec![]),
        Err(RegistryError::Api(ApiError::MandatoryCapabilityMissing(
            COMMON_ADVISORY_PROFILE_ID
        )))
    ));

    let mut actual = first.descriptor.clone();
    actual.limits.recall_candidates = 3;
    let mut injected = registration(first.clone(), EnabledProviderMode::Active);
    injected.provider = Arc::new(ChangingDescriptor {
        first: first.descriptor.clone(),
        registered: actual.clone(),
        reads: AtomicUsize::new(0),
    });
    injected.execution_shape = ProviderExecutionShapeV1::Foreign;
    let owner = Arc::new(Owner::default());
    injected.lifecycle = ProviderLifecycleOwnershipV1::Owned(owner.clone());
    let composition =
        ProjectMemoryProviderComposition::compose_registered(selection(injected), vec![]).unwrap();
    let registry = composition.registry().unwrap();
    assert_eq!(
        registry.selected_registration().unwrap().limits,
        actual.limits
    );
    assert_eq!(
        registry.selected_execution_shape(),
        ProviderExecutionShapeV1::Foreign
    );
    assert_eq!(owner.starts.load(Ordering::SeqCst), 0);
}

#[test]
fn an_active_registration_without_admitted_scope_bindings_is_incompatible() {
    let mut injected = registration(
        FixtureProvider::new("ncm", true),
        EnabledProviderMode::Active,
    );
    injected.recall_scope_bindings = RecallScopeBindingsV1::new([]);
    assert!(
        matches!(ProjectMemoryProviderComposition::compose_registered(selection(injected), vec![]), Err(RegistryError::ActiveRecallScopeBindingsMissing(provider)) if provider == "ncm")
    );
}

#[test]
fn missing_provider_in_enabled_composition_cannot_claim_runtime_termination() {
    let composition = Arc::new(
        ProjectMemoryProviderComposition::compose_registered(
            selection(registration(
                FixtureProvider::new("ncm", true),
                EnabledProviderMode::Active,
            )),
            vec![],
        )
        .unwrap(),
    );
    let lifecycle = CompositionLifecycleAdapterV1::for_provider(
        composition,
        Arc::new(NoHandshake),
        OwnedProviderId::new("provider.absent").unwrap(),
    );
    assert!(matches!(
        lifecycle.request_stop(i64::MAX),
        Err(CompositionLifecycleError::ProviderNotRegistered)
    ));
    assert!(matches!(
        lifecycle.kill(i64::MAX),
        Err(CompositionLifecycleError::ProviderNotRegistered)
    ));
}

#[test]
fn controls_keep_the_original_provider_after_selection_changes() {
    let original = FixtureProvider::new("vendor.original", true);
    let previous = ProjectMemoryProviderComposition::compose_registered(
        selection(registration(original.clone(), EnabledProviderMode::Active)),
        vec![],
    )
    .unwrap();
    assert_eq!(
        previous
            .registry()
            .unwrap()
            .selected_registration()
            .unwrap()
            .provider_id
            .as_str(),
        "vendor.original"
    );
    drop(previous);

    let selected = FixtureProvider::new("vendor.selected", true);
    let composition = ProjectMemoryProviderComposition::compose_registered(
        selection(registration(selected.clone(), EnabledProviderMode::Active)),
        vec![registration(
            original.clone(),
            EnabledProviderMode::Observer,
        )],
    )
    .unwrap();
    let registry = composition.registry().unwrap();
    assert_eq!(
        registry
            .selected_registration()
            .unwrap()
            .provider_id
            .as_str(),
        "vendor.selected"
    );
    let original_id = OwnedProviderId::new("vendor.original").unwrap();
    assert!(
        !registry
            .registration(&original_id)
            .unwrap()
            .requires_common_advisory_profile
    );
    assert!(registry.recall_scope_bindings(&original_id).is_none());
    registry.handshake(&handshake("vendor.original")).unwrap();
    let mut control = recall_call("vendor.original");
    control.operation = ProviderOperation::Health;
    control.operation_id = "health.original".to_owned();
    control.required_capabilities = [OwnedVersionedId::new("provider.health.v1").unwrap()]
        .into_iter()
        .collect();
    let reply = registry.invoke_control(&control).unwrap();
    assert_eq!(reply.terminal.provider_id(), &original_id);
    assert_eq!(reply.terminal.operation(), ProviderOperation::Health);
    assert_eq!(reply.terminal.operation_id(), "health.original");
    assert!(matches!(
        registry.invoke_active(&recall_call("vendor.original")),
        Err(FabricError::ProviderObserverOnly(_))
    ));
    assert_eq!(
        registry.invoke_control(&recall_call("vendor.original")),
        Err(FabricError::OperationNotControl)
    );
    control.registration_revision = 18;
    assert_eq!(
        registry.invoke_control(&control),
        Err(FabricError::RegistrationRevisionMismatch {
            accepted: 19,
            requested: 18
        })
    );
    assert_eq!(original.calls.load(Ordering::SeqCst), 2);
    assert_eq!(selected.calls.load(Ordering::SeqCst), 0);
}
