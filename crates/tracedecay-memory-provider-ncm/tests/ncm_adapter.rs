//! Focused tests for the topology-neutral NCM adapter boundary.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use tracedecay_memory_provider_api::contract::{
    CommittedEffectState, FallbackEligibility, TerminalCode,
};
use tracedecay_memory_provider_api::{
    ApiError, CancellationToken, CanonicalPayload, HandshakeRequest, HandshakeRequestParts,
    HandshakeResponse, MemoryProvider, OperationControl, OwnedExactScope, OwnedProviderId,
    OwnedVersionedId, ProviderCall, ProviderCallParts, ProviderDescriptor, ProviderLimits,
    ProviderOperation, ProviderReply, TerminalRecord,
};
use tracedecay_memory_provider_ncm::{NcmAdapterError, NcmProviderAdapter, NcmRuntimePort};

type TestResult = Result<(), Box<dyn Error>>;

const ZERO_SHA: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const ONE_SHA: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const NCM_ID: &str = "ncm.test-runtime";

fn provider_id(value: &str) -> Result<OwnedProviderId, ApiError> {
    OwnedProviderId::new(value)
}

fn capability(value: &str) -> Result<OwnedVersionedId, ApiError> {
    OwnedVersionedId::new(value)
}

fn limits() -> ProviderLimits {
    ProviderLimits {
        request_bytes: 4096,
        response_bytes: 8192,
        observation_batch_items: 16,
        recall_candidates: 32,
        concurrent_operations: 4,
        operation_millis: 1000,
        snapshot_bytes: 65_536,
        inspection_items: 64,
    }
}

fn scope() -> Result<OwnedExactScope, ApiError> {
    OwnedExactScope::new(
        "profile-1",
        "project-1",
        "repo-1",
        "worktree-1",
        "refs/heads/feature",
        "agent-session-1",
        9,
    )
}

fn descriptor(id: &str, optional: &[&str]) -> Result<ProviderDescriptor, ApiError> {
    let mut capabilities = vec![
        capability("provider.health.v1")?,
        capability("observation.accept.v1")?,
        capability("recall.query.v1")?,
    ];
    for value in optional {
        capabilities.push(capability(value)?);
    }
    ProviderDescriptor::new(
        provider_id(id)?,
        ZERO_SHA,
        "ncm-state-v1",
        4,
        capabilities,
        limits(),
    )
}

fn handshake(id: &str) -> Result<HandshakeRequest, ApiError> {
    HandshakeRequest::new(HandshakeRequestParts {
        provider_id: provider_id(id)?,
        registration_revision: 3,
        exact_scope: scope()?,
        request_id: "handshake-request-1".to_owned(),
        required_capabilities: vec![
            capability("provider.health.v1")?,
            capability("observation.accept.v1")?,
            capability("recall.query.v1")?,
        ],
        host_limits: limits(),
        control: OperationControl::new(1_000_000, 500, CancellationToken::new()),
        challenge_nonce: [7; 32],
    })
}

fn payload() -> Result<CanonicalPayload, ApiError> {
    CanonicalPayload::new(
        capability("tracedecay.memory.test-payload.v1")?,
        br#"{"fixture":true}"#.to_vec(),
        ONE_SHA,
    )
}

fn call(id: &str, operation: ProviderOperation) -> Result<ProviderCall, ApiError> {
    ProviderCall::new(ProviderCallParts {
        operation,
        provider_id: provider_id(id)?,
        registration_revision: 3,
        ready_receipt_sha256: ONE_SHA.to_owned(),
        exact_scope: scope()?,
        request_id: format!("request-{}", operation.capability_id()),
        operation_id: format!("operation-{}", operation.capability_id()),
        expected_state_generation: 4,
        idempotency_key: operation
            .mutates_provider_state()
            .then(|| format!("idempotency-{}", operation.capability_id())),
        control: OperationControl::new(1_000_000, 500, CancellationToken::new()),
        payload: payload()?,
        required_capabilities: vec![capability(operation.capability_id())?],
        extensions: Vec::new(),
    })
}

struct MockRuntime {
    descriptor: ProviderDescriptor,
    handshake_calls: AtomicUsize,
    invoke_calls: AtomicUsize,
    rejected_handshakes: AtomicUsize,
    rejected_calls: AtomicUsize,
    saw_complete_handshake: AtomicBool,
    saw_complete_call: AtomicBool,
}

impl MockRuntime {
    fn new(id: &str, optional: &[&str]) -> Result<Self, ApiError> {
        Ok(Self {
            descriptor: descriptor(id, optional)?,
            handshake_calls: AtomicUsize::new(0),
            invoke_calls: AtomicUsize::new(0),
            rejected_handshakes: AtomicUsize::new(0),
            rejected_calls: AtomicUsize::new(0),
            saw_complete_handshake: AtomicBool::new(false),
            saw_complete_call: AtomicBool::new(false),
        })
    }

    fn terminal(
        operation_id: &str,
        terminal_code: TerminalCode,
        committed_effect: CommittedEffectState,
        diagnostic_id: Option<String>,
    ) -> TerminalRecord {
        TerminalRecord {
            terminal_code,
            committed_effect,
            fallback: FallbackEligibility::Forbidden,
            operation_id: operation_id.to_owned(),
            exact_scope_sha256: ZERO_SHA.to_owned(),
            provider_receipt_sha256: (committed_effect != CommittedEffectState::None)
                .then(|| ONE_SHA.to_owned()),
            diagnostic_id,
        }
    }
}

impl NcmRuntimePort for MockRuntime {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        self.handshake_calls.fetch_add(1, Ordering::Relaxed);
        self.saw_complete_handshake.store(
            request.request_id == "handshake-request-1"
                && request.registration_revision == 3
                && request.exact_scope.project_id == "project-1"
                && request.exact_scope.worktree_identity == "worktree-1"
                && request.challenge_nonce == [7; 32]
                && request.control.snapshot().is_ok(),
            Ordering::Relaxed,
        );
        HandshakeResponse {
            terminal: Self::terminal(
                &request.request_id,
                TerminalCode::Success,
                CommittedEffectState::None,
                None,
            ),
            descriptor: Some(self.descriptor.clone()),
            provider_instance_id: Some("ncm.instance-1".to_owned()),
            state_namespace: Some("ncm.scope-1".to_owned()),
            accepted_scope: Some(request.exact_scope.clone()),
            effective_limits: Some(request.host_limits.minimum(self.descriptor.limits)),
            ready_receipt_sha256: Some(ONE_SHA.to_owned()),
            warnings: Vec::new(),
        }
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        self.invoke_calls.fetch_add(1, Ordering::Relaxed);
        self.saw_complete_call.store(
            call.registration_revision == 3
                && call.ready_receipt_sha256 == ONE_SHA
                && call.exact_scope.project_id == "project-1"
                && call.exact_scope.agent_session_id == "agent-session-1"
                && call.expected_state_generation == 4
                && call.payload.bytes == br#"{"fixture":true}"#
                && call.control.snapshot().is_ok()
                && call.extensions.is_empty(),
            Ordering::Relaxed,
        );
        let committed_effect = if call.operation.mutates_provider_state() {
            CommittedEffectState::Committed
        } else {
            CommittedEffectState::None
        };
        ProviderReply {
            terminal: Self::terminal(
                &call.operation_id,
                TerminalCode::Success,
                committed_effect,
                None,
            ),
            payload: Some(call.payload.clone()),
            warnings: Vec::new(),
            extensions: call.extensions.clone(),
            state_generation: if committed_effect == CommittedEffectState::None {
                call.expected_state_generation
            } else {
                call.expected_state_generation.saturating_add(1)
            },
        }
    }

    fn reject_handshake(
        &self,
        request: &HandshakeRequest,
        terminal_code: TerminalCode,
        diagnostic_id: &'static str,
    ) -> HandshakeResponse {
        self.rejected_handshakes.fetch_add(1, Ordering::Relaxed);
        HandshakeResponse {
            terminal: Self::terminal(
                &request.request_id,
                terminal_code,
                CommittedEffectState::None,
                Some(diagnostic_id.to_owned()),
            ),
            descriptor: None,
            provider_instance_id: None,
            state_namespace: None,
            accepted_scope: None,
            effective_limits: None,
            ready_receipt_sha256: None,
            warnings: Vec::new(),
        }
    }

    fn reject_call(
        &self,
        call: &ProviderCall,
        terminal_code: TerminalCode,
        diagnostic_id: &'static str,
    ) -> ProviderReply {
        self.rejected_calls.fetch_add(1, Ordering::Relaxed);
        ProviderReply {
            terminal: Self::terminal(
                &call.operation_id,
                terminal_code,
                CommittedEffectState::None,
                Some(diagnostic_id.to_owned()),
            ),
            payload: None,
            warnings: Vec::new(),
            extensions: call.extensions.clone(),
            state_generation: call.expected_state_generation,
        }
    }
}

#[test]
fn construction_requires_configured_and_runtime_identity_to_match() -> TestResult {
    let runtime = Arc::new(MockRuntime::new("ncm.runtime", &[])?);
    let result = NcmProviderAdapter::new(provider_id("ncm.configured")?, runtime);
    match result {
        Err(NcmAdapterError::ProviderIdMismatch { expected, declared }) => {
            assert_eq!(expected.as_str(), "ncm.configured");
            assert_eq!(declared.as_str(), "ncm.runtime");
        }
        Ok(_) => {
            return Err(std::io::Error::other("identity mismatch unexpectedly succeeded").into());
        }
    }
    Ok(())
}

#[test]
fn descriptor_and_handshake_are_delegated_without_scope_changes() -> TestResult {
    let runtime = Arc::new(MockRuntime::new(NCM_ID, &[])?);
    let provider = NcmProviderAdapter::new(provider_id(NCM_ID)?, runtime.clone())?;
    assert_eq!(provider.provider_id().as_str(), NCM_ID);
    assert_eq!(provider.descriptor(), runtime.descriptor());

    let request = handshake(NCM_ID)?;
    let response = provider.handshake(&request);
    assert_eq!(response.terminal.terminal_code, TerminalCode::Success);
    assert_eq!(response.accepted_scope, Some(request.exact_scope));
    assert_eq!(runtime.handshake_calls.load(Ordering::Relaxed), 1);
    assert!(runtime.saw_complete_handshake.load(Ordering::Relaxed));
    Ok(())
}

#[test]
fn wrong_handshake_target_is_rejected_before_runtime_handshake() -> TestResult {
    let runtime = Arc::new(MockRuntime::new(NCM_ID, &[])?);
    let provider = NcmProviderAdapter::new(provider_id(NCM_ID)?, runtime.clone())?;
    let response = provider.handshake(&handshake("ncm.other")?);
    assert_eq!(
        response.terminal.terminal_code,
        TerminalCode::InvalidRequest
    );
    assert_eq!(runtime.handshake_calls.load(Ordering::Relaxed), 0);
    assert_eq!(runtime.rejected_handshakes.load(Ordering::Relaxed), 1);
    Ok(())
}

#[test]
fn mandatory_and_declared_optional_calls_are_forwarded_unchanged() -> TestResult {
    let runtime = Arc::new(MockRuntime::new(NCM_ID, &["feedback.record.v1"])?);
    let provider = NcmProviderAdapter::new(provider_id(NCM_ID)?, runtime.clone())?;
    for operation in [
        ProviderOperation::Health,
        ProviderOperation::Observe,
        ProviderOperation::Recall,
        ProviderOperation::Feedback,
    ] {
        let request = call(NCM_ID, operation)?;
        let expected_payload = request.payload.clone();
        let response = provider.invoke(&request);
        assert_eq!(response.terminal.terminal_code, TerminalCode::Success);
        assert_eq!(response.payload, Some(expected_payload));
    }
    assert_eq!(runtime.invoke_calls.load(Ordering::Relaxed), 4);
    assert_eq!(runtime.rejected_calls.load(Ordering::Relaxed), 0);
    assert!(runtime.saw_complete_call.load(Ordering::Relaxed));
    Ok(())
}

#[test]
fn undeclared_capability_is_explicitly_rejected() -> TestResult {
    let runtime = Arc::new(MockRuntime::new(NCM_ID, &[])?);
    let provider = NcmProviderAdapter::new(provider_id(NCM_ID)?, runtime.clone())?;
    let response = provider.invoke(&call(NCM_ID, ProviderOperation::Maintenance)?);
    assert_eq!(
        response.terminal.terminal_code,
        TerminalCode::CapabilityUnsupported
    );
    assert_eq!(runtime.invoke_calls.load(Ordering::Relaxed), 0);
    assert_eq!(runtime.rejected_calls.load(Ordering::Relaxed), 1);
    Ok(())
}

#[test]
fn wrong_call_target_and_handshake_misuse_are_rejected() -> TestResult {
    let runtime = Arc::new(MockRuntime::new(NCM_ID, &[])?);
    let provider = NcmProviderAdapter::new(provider_id(NCM_ID)?, runtime.clone())?;
    let wrong_target = provider.invoke(&call("ncm.other", ProviderOperation::Recall)?);
    assert_eq!(
        wrong_target.terminal.terminal_code,
        TerminalCode::InvalidRequest
    );
    let handshake_call = provider.invoke(&call(NCM_ID, ProviderOperation::Handshake)?);
    assert_eq!(
        handshake_call.terminal.terminal_code,
        TerminalCode::InvalidRequest
    );
    assert_eq!(runtime.invoke_calls.load(Ordering::Relaxed), 0);
    assert_eq!(runtime.rejected_calls.load(Ordering::Relaxed), 2);
    Ok(())
}

#[test]
fn adapter_owns_no_provider_memory_state() -> TestResult {
    let runtime = Arc::new(MockRuntime::new(NCM_ID, &[])?);
    let provider = NcmProviderAdapter::new(provider_id(NCM_ID)?, runtime)?;
    let before = provider.descriptor();
    let response = provider.invoke(&call(NCM_ID, ProviderOperation::Observe)?);
    assert_eq!(
        response.terminal.committed_effect,
        CommittedEffectState::Committed
    );
    assert_eq!(provider.descriptor(), before);
    Ok(())
}

#[test]
fn crate_has_only_the_provider_api_and_no_topology_dependency() {
    let manifest = include_str!("../Cargo.toml");
    assert!(manifest.contains("tracedecay-memory-provider-api"));
    for forbidden in [
        "tracedecay-store",
        "tracedecay-code-index",
        "tracedecay-memory-provider-native",
        "pyo3",
        "tokio",
        "reqwest",
    ] {
        assert!(
            !manifest.contains(forbidden),
            "forbidden dependency: {forbidden}"
        );
    }

    let source = include_str!("../src/lib.rs");
    for forbidden in [
        "std::net",
        "std::process",
        "TcpStream",
        "Command::new",
        "pyo3",
        "reqwest",
        "biomem",
        "tracedecay_store",
        "tracedecay_code_index",
    ] {
        assert!(
            !source.contains(forbidden),
            "topology leaked through: {forbidden}"
        );
    }
}
