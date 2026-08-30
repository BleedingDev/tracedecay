//! Default-off Native Memory Fabric composition journeys.

use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tracedecay::{
    FabricError, NativeMemoryApplicationPort, NativeMemoryFabricConfig, NativeMemoryMode,
    NativeMemoryMountError, ProviderMode, compose_native_memory_fabric,
};
use tracedecay_memory_provider_api::contract::{
    CommittedEffectState, FallbackEligibility, TerminalCode,
};
use tracedecay_memory_provider_api::{
    ApiError, HandshakeRequest, HandshakeResponse, OwnedProviderId, OwnedVersionedId, ProviderCall,
    ProviderDescriptor, ProviderLimits, ProviderReply, TerminalRecord,
};
use tracedecay_memory_provider_native::NATIVE_PROVIDER_ID;

type TestResult = Result<(), Box<dyn Error>>;

const ZERO_SHA: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const ONE_SHA: &str = "1111111111111111111111111111111111111111111111111111111111111111";

struct MockNativePort {
    descriptor: ProviderDescriptor,
    descriptor_calls: Arc<AtomicUsize>,
}

impl MockNativePort {
    fn new(descriptor_calls: Arc<AtomicUsize>) -> Result<Self, ApiError> {
        Ok(Self {
            descriptor: ProviderDescriptor::new(
                OwnedProviderId::new(NATIVE_PROVIDER_ID)?,
                ONE_SHA,
                "native-state-v1",
                7,
                [
                    OwnedVersionedId::new("provider.health.v1")?,
                    OwnedVersionedId::new("observation.accept.v1")?,
                    OwnedVersionedId::new("recall.query.v1")?,
                ],
                limits(),
            )?,
            descriptor_calls,
        })
    }

    fn terminal(
        operation_id: String,
        terminal_code: TerminalCode,
        committed_effect: CommittedEffectState,
    ) -> TerminalRecord {
        TerminalRecord {
            terminal_code,
            committed_effect,
            fallback: FallbackEligibility::Forbidden,
            operation_id,
            exact_scope_sha256: ZERO_SHA.to_owned(),
            provider_receipt_sha256: (committed_effect == CommittedEffectState::Committed)
                .then(|| ONE_SHA.to_owned()),
            diagnostic_id: (terminal_code != TerminalCode::Success)
                .then(|| "native.mock_rejection".to_owned()),
        }
    }

    fn reply(&self, call: &ProviderCall, terminal_code: TerminalCode) -> ProviderReply {
        let committed_effect =
            if terminal_code == TerminalCode::Success && call.operation.mutates_provider_state() {
                CommittedEffectState::Committed
            } else {
                CommittedEffectState::None
            };
        ProviderReply {
            terminal: Self::terminal(
                call.operation_id.clone(),
                terminal_code,
                committed_effect,
            ),
            payload: (terminal_code == TerminalCode::Success).then(|| call.payload.clone()),
            warnings: Vec::new(),
            extensions: Vec::new(),
            state_generation: self.descriptor.state_generation,
        }
    }
}

impl NativeMemoryApplicationPort for MockNativePort {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor_calls.fetch_add(1, Ordering::SeqCst);
        self.descriptor.clone()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        HandshakeResponse {
            terminal: Self::terminal(
                request.request_id.clone(),
                TerminalCode::Success,
                CommittedEffectState::None,
            ),
            descriptor: Some(self.descriptor.clone()),
            provider_instance_id: Some("native-test-instance".to_owned()),
            state_namespace: Some("native-test-namespace".to_owned()),
            accepted_scope: Some(request.exact_scope.clone()),
            effective_limits: Some(self.descriptor.limits),
            ready_receipt_sha256: Some(ZERO_SHA.to_owned()),
            warnings: Vec::new(),
        }
    }

    fn health(&self, call: &ProviderCall) -> ProviderReply {
        self.reply(call, TerminalCode::Success)
    }

    fn observe(&self, call: &ProviderCall) -> ProviderReply {
        self.reply(call, TerminalCode::Success)
    }

    fn recall(&self, call: &ProviderCall) -> ProviderReply {
        self.reply(call, TerminalCode::Success)
    }

    fn lifecycle(&self, call: &ProviderCall) -> ProviderReply {
        self.reply(call, TerminalCode::Success)
    }

    fn reject(
        &self,
        call: &ProviderCall,
        terminal_code: TerminalCode,
        _diagnostic_id: &'static str,
    ) -> ProviderReply {
        self.reply(call, terminal_code)
    }
}

fn limits() -> ProviderLimits {
    ProviderLimits {
        request_bytes: 1024,
        response_bytes: 2048,
        observation_batch_items: 8,
        recall_candidates: 16,
        concurrent_operations: 2,
        operation_millis: 500,
        snapshot_bytes: 4096,
        inspection_items: 32,
    }
}

fn port(counter: Arc<AtomicUsize>) -> Result<Arc<dyn NativeMemoryApplicationPort>, ApiError> {
    Ok(Arc::new(MockNativePort::new(counter)?))
}

#[test]
fn explicit_observer_mount_registers_only_the_native_adapter() -> TestResult {
    let descriptor_calls = Arc::new(AtomicUsize::new(0));
    let mount = compose_native_memory_fabric(
        port(Arc::clone(&descriptor_calls))?,
        NativeMemoryFabricConfig::new(1, 2, 9, NativeMemoryMode::Observer),
    )?;
    let statuses = mount.fabric().statuses()?;

    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].provider_id.as_str(), NATIVE_PROVIDER_ID);
    assert_eq!(statuses[0].registration_revision, 9);
    assert_eq!(statuses[0].mode, ProviderMode::Observer);
    assert_eq!(mount.provider_id(), NATIVE_PROVIDER_ID);
    assert_eq!(mount.registration_revision(), 9);
    assert_eq!(mount.mode(), NativeMemoryMode::Observer);
    assert_eq!(descriptor_calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[test]
fn zero_revision_fails_before_inspecting_native_authority() -> TestResult {
    let descriptor_calls = Arc::new(AtomicUsize::new(0));
    let result = compose_native_memory_fabric(
        port(Arc::clone(&descriptor_calls))?,
        NativeMemoryFabricConfig::new(1, 1, 0, NativeMemoryMode::Active),
    );

    assert!(matches!(
        result,
        Err(NativeMemoryMountError::InvalidRegistrationRevision)
    ));
    assert_eq!(descriptor_calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn invalid_fabric_limits_fail_before_inspecting_native_authority() -> TestResult {
    let descriptor_calls = Arc::new(AtomicUsize::new(0));
    let result = compose_native_memory_fabric(
        port(Arc::clone(&descriptor_calls))?,
        NativeMemoryFabricConfig::new(0, 1, 1, NativeMemoryMode::Active),
    );

    assert!(matches!(
        result,
        Err(NativeMemoryMountError::Fabric(FabricError::InvalidConfig(
            "max_registered_providers must be positive"
        )))
    ));
    assert_eq!(descriptor_calls.load(Ordering::SeqCst), 0);
    Ok(())
}
