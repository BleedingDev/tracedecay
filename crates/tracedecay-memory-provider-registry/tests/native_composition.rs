//! Focused journeys for the default-off Native provider composition root.
#![allow(clippy::expect_used)]

use std::error::Error;
use std::sync::Arc;

use tracedecay_memory_fabric::ProviderMode;
use tracedecay_memory_provider_api::contract::{
    CommittedEffectState, FallbackEligibility, TerminalCode,
};
use tracedecay_memory_provider_api::{
    HandshakeRequest, HandshakeResponse, OwnedProviderId, OwnedVersionedId, ProviderCall,
    ProviderDescriptor, ProviderLimits, ProviderReply, TerminalRecord,
};
use tracedecay_memory_provider_native::{NATIVE_PROVIDER_ID, NativeAdapterError};
use tracedecay_memory_provider_registry::{
    CompositionError, FabricConfig, NativeCompositionConfig, NativeMemoryApplicationPort,
    compose_native_memory,
};

const ZERO_SHA: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const ONE_SHA: &str = "1111111111111111111111111111111111111111111111111111111111111111";

struct MockNativePort {
    descriptor: ProviderDescriptor,
}

impl MockNativePort {
    fn new(provider_id: &str) -> Self {
        let capabilities = [
            "provider.health.v1",
            "observation.accept.v1",
            "recall.query.v1",
        ]
        .into_iter()
        .map(|value| OwnedVersionedId::new(value).expect("capability"))
        .collect::<Vec<_>>();
        Self {
            descriptor: ProviderDescriptor::new(
                OwnedProviderId::new(provider_id).expect("provider id"),
                ZERO_SHA,
                "native-state-v1",
                7,
                capabilities,
                limits(),
            )
            .expect("descriptor"),
        }
    }

    fn reply(&self, call: &ProviderCall, terminal_code: TerminalCode) -> ProviderReply {
        ProviderReply {
            terminal: TerminalRecord::new(
                terminal_code,
                CommittedEffectState::None,
                FallbackEligibility::Forbidden,
                call.operation_id.clone(),
                ZERO_SHA,
                None,
                (terminal_code != TerminalCode::Success)
                    .then(|| "native.test_rejection".to_owned()),
            )
            .expect("terminal"),
            payload: (terminal_code == TerminalCode::Success).then(|| call.payload.clone()),
            warnings: Vec::new(),
            extensions: Vec::new(),
            state_generation: call.expected_state_generation,
        }
    }
}

impl NativeMemoryApplicationPort for MockNativePort {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        HandshakeResponse {
            terminal: TerminalRecord::new(
                TerminalCode::Success,
                CommittedEffectState::None,
                FallbackEligibility::Forbidden,
                request.request_id.clone(),
                ZERO_SHA,
                None,
                None,
            )
            .expect("handshake terminal"),
            descriptor: Some(self.descriptor.clone()),
            provider_instance_id: Some("native.instance-1".to_owned()),
            state_namespace: Some("native.project".to_owned()),
            accepted_scope: Some(request.exact_scope.clone()),
            effective_limits: Some(request.host_limits.minimum(self.descriptor.limits)),
            ready_receipt_sha256: Some(ONE_SHA.to_owned()),
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
        request_bytes: 4096,
        response_bytes: 8192,
        observation_batch_items: 16,
        recall_candidates: 32,
        concurrent_operations: 4,
        operation_millis: 1000,
        snapshot_bytes: 65536,
        inspection_items: 64,
    }
}

#[test]
fn composition_registers_exactly_one_native_provider() -> Result<(), Box<dyn Error>> {
    let config = NativeCompositionConfig::new(
        FabricConfig::new(2, 4)?,
        3,
        ProviderMode::Observer,
    )?;
    let composition = compose_native_memory(
        Arc::new(MockNativePort::new(NATIVE_PROVIDER_ID)),
        config,
    )?;
    let statuses = composition.fabric().statuses()?;
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].provider_id.as_str(), NATIVE_PROVIDER_ID);
    assert_eq!(statuses[0].registration_revision, 3);
    assert_eq!(statuses[0].mode, ProviderMode::Observer);
    assert_eq!(composition.native_provider_id().as_str(), NATIVE_PROVIDER_ID);
    assert_eq!(composition.registration_revision(), 3);
    Ok(())
}

#[test]
fn zero_registration_revision_fails_before_composition() -> Result<(), Box<dyn Error>> {
    let result = NativeCompositionConfig::new(
        FabricConfig::new(1, 1)?,
        0,
        ProviderMode::Disabled,
    );
    assert!(matches!(
        result,
        Err(CompositionError::InvalidRegistrationRevision)
    ));
    Ok(())
}

#[test]
fn wrong_native_identity_fails_without_registration() -> Result<(), Box<dyn Error>> {
    let config = NativeCompositionConfig::new(
        FabricConfig::new(1, 1)?,
        1,
        ProviderMode::Active,
    )?;
    let result = compose_native_memory(Arc::new(MockNativePort::new("vendor.other")), config);
    assert!(matches!(
        result,
        Err(CompositionError::Native(
            NativeAdapterError::ProviderIdMismatch { .. }
        ))
    ));
    Ok(())
}
