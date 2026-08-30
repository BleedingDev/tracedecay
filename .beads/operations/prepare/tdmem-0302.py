#!/usr/bin/env python3
"""Materialize the capability-driven memory fabric crate for tdmem-0302."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
CRATE = ROOT / "crates/tracedecay-memory-fabric"
FLOOR = "08fbe33a7c7f403191fd5d6e356c7b6681b96403"

CARGO = '''[package]
name = "tracedecay-memory-fabric"
version.workspace = true
edition.workspace = true
publish = false
license = "MIT"
description = "Bounded capability-driven orchestration for TraceDecay cognitive memory providers"
repository = "https://github.com/BleedingDev/tracedecay"

[dependencies]
tracedecay-memory-provider-api = { path = "../tracedecay-memory-provider-api" }

[lints.rust]
missing_docs = "deny"
unsafe_code = "forbid"
warnings = "deny"

[lints.clippy]
dbg_macro = "deny"
expect_used = "deny"
panic = "deny"
print_stderr = "deny"
print_stdout = "deny"
todo = "deny"
unimplemented = "deny"
unwrap_used = "deny"
'''

LIB = r'''#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(warnings)]
#![deny(clippy::dbg_macro)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(clippy::print_stderr)]
#![deny(clippy::print_stdout)]
#![deny(clippy::todo)]
#![deny(clippy::unimplemented)]
#![deny(clippy::unwrap_used)]
//! Bounded, capability-driven orchestration for memory providers.
//!
//! This crate owns registration, mode selection, finite concurrency admission,
//! exact capability checks, request preflight, and structural observer
//! isolation. It contains no provider implementation, persistence, transport,
//! TraceDecay database, code-index, daemon, dashboard, or host integration.

use std::collections::{BTreeMap, btree_map::Entry};
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use tracedecay_memory_provider_api::contract::{
    CommittedEffectState, TerminalCode,
};
use tracedecay_memory_provider_api::{
    ApiError, HandshakeRequest, HandshakeResponse, MemoryProvider,
    OwnedProviderId, ProviderCall, ProviderDescriptor, ProviderReply,
};

/// Provider participation mode selected by TraceDecay configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderMode {
    /// Registered but unreachable for handshake, observation, or recall.
    Disabled,
    /// Receives admitted observations but cannot contribute product output.
    Observer,
    /// May receive observations and answer explicitly routed active calls.
    Active,
}

/// Finite memory-fabric resource limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FabricConfig {
    /// Maximum registered providers.
    pub max_registered_providers: usize,
    /// Maximum concurrent provider calls admitted by this fabric instance.
    pub max_in_flight: usize,
}

impl FabricConfig {
    /// Creates finite non-zero fabric limits.
    pub fn new(
        max_registered_providers: usize,
        max_in_flight: usize,
    ) -> Result<Self, FabricError> {
        if max_registered_providers == 0 {
            return Err(FabricError::InvalidConfig(
                "max_registered_providers must be positive",
            ));
        }
        if max_in_flight == 0 {
            return Err(FabricError::InvalidConfig(
                "max_in_flight must be positive",
            ));
        }
        Ok(Self {
            max_registered_providers,
            max_in_flight,
        })
    }
}

/// Typed orchestration failure before or after a provider call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FabricError {
    /// Product configuration was not finite or valid.
    InvalidConfig(&'static str),
    /// A provider-neutral API value was invalid.
    Api(ApiError),
    /// The registry lock was poisoned by an aborted owner.
    RegistryPoisoned,
    /// A provider ID already has a registration.
    DuplicateProvider(String),
    /// The finite registry capacity has been reached.
    RegistryCapacityExhausted,
    /// No exact provider registration exists.
    ProviderUnknown(String),
    /// The requested registration revision differs from the accepted revision.
    RegistrationRevisionMismatch {
        /// Accepted registry revision.
        accepted: u64,
        /// Requested revision.
        requested: u64,
    },
    /// The registered provider descriptor used a different provider ID.
    ProviderDescriptorMismatch {
        /// Registry-selected provider ID.
        selected: String,
        /// Descriptor-declared provider ID.
        declared: String,
    },
    /// The selected provider is disabled.
    ProviderDisabled(String),
    /// An observer-only provider was asked to influence active output.
    ProviderObserverOnly(String),
    /// Observation routing was requested for a non-observation operation.
    OperationNotObservation,
    /// The registration does not declare a required capability.
    MissingCapability(String),
    /// The finite concurrent-call budget is exhausted.
    CapacityExhausted,
    /// Request cancellation was already terminal before provider contact.
    Cancelled,
    /// The deadline was already exhausted before provider contact.
    DeadlineExceeded,
    /// The provider returned a result for another operation identity.
    ResponseOperationMismatch {
        /// Expected operation identity.
        expected: String,
        /// Returned operation identity.
        returned: String,
    },
    /// A successful handshake omitted its compatible descriptor.
    SuccessfulHandshakeMissingDescriptor,
    /// A successful handshake returned another provider identity.
    SuccessfulHandshakeProviderMismatch,
    /// A successful handshake accepted another coding scope.
    SuccessfulHandshakeScopeMismatch,
}

impl fmt::Display for FabricError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => formatter.write_str(message),
            Self::Api(error) => write!(formatter, "provider API error: {error}"),
            Self::RegistryPoisoned => formatter.write_str("provider registry lock is poisoned"),
            Self::DuplicateProvider(provider) => {
                write!(formatter, "provider {provider} is already registered")
            }
            Self::RegistryCapacityExhausted => {
                formatter.write_str("provider registry capacity is exhausted")
            }
            Self::ProviderUnknown(provider) => write!(formatter, "provider {provider} is unknown"),
            Self::RegistrationRevisionMismatch {
                accepted,
                requested,
            } => write!(
                formatter,
                "provider registration revision mismatch: accepted {accepted}, requested {requested}"
            ),
            Self::ProviderDescriptorMismatch { selected, declared } => write!(
                formatter,
                "provider descriptor mismatch: selected {selected}, declared {declared}"
            ),
            Self::ProviderDisabled(provider) => write!(formatter, "provider {provider} is disabled"),
            Self::ProviderObserverOnly(provider) => {
                write!(formatter, "provider {provider} is observer-only")
            }
            Self::OperationNotObservation => {
                formatter.write_str("observer delivery requires observation.accept.v1")
            }
            Self::MissingCapability(capability) => {
                write!(formatter, "provider does not declare capability {capability}")
            }
            Self::CapacityExhausted => {
                formatter.write_str("provider call capacity is exhausted")
            }
            Self::Cancelled => formatter.write_str("provider call was cancelled before dispatch"),
            Self::DeadlineExceeded => {
                formatter.write_str("provider call deadline was exhausted before dispatch")
            }
            Self::ResponseOperationMismatch { expected, returned } => write!(
                formatter,
                "provider response operation mismatch: expected {expected}, returned {returned}"
            ),
            Self::SuccessfulHandshakeMissingDescriptor => {
                formatter.write_str("successful handshake omitted provider descriptor")
            }
            Self::SuccessfulHandshakeProviderMismatch => {
                formatter.write_str("successful handshake returned another provider")
            }
            Self::SuccessfulHandshakeScopeMismatch => {
                formatter.write_str("successful handshake accepted another exact scope")
            }
        }
    }
}

impl Error for FabricError {}

impl From<ApiError> for FabricError {
    fn from(value: ApiError) -> Self {
        Self::Api(value)
    }
}

#[derive(Clone)]
struct Registration {
    revision: u64,
    mode: ProviderMode,
    descriptor: ProviderDescriptor,
    provider: Arc<dyn MemoryProvider>,
}

struct PermitCounter {
    current: AtomicUsize,
    maximum: usize,
}

impl PermitCounter {
    fn new(maximum: usize) -> Self {
        Self {
            current: AtomicUsize::new(0),
            maximum,
        }
    }

    fn try_acquire(&self) -> Result<Permit<'_>, FabricError> {
        let mut current = self.current.load(Ordering::Acquire);
        loop {
            if current >= self.maximum {
                return Err(FabricError::CapacityExhausted);
            }
            match self.current.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(Permit { counter: self }),
                Err(observed) => current = observed,
            }
        }
    }
}

struct Permit<'a> {
    counter: &'a PermitCounter,
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        self.counter.current.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Immutable provider status returned in canonical provider-ID order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderStatus {
    /// Stable logical provider identity.
    pub provider_id: OwnedProviderId,
    /// Accepted registry revision.
    pub registration_revision: u64,
    /// Current participation mode.
    pub mode: ProviderMode,
    /// Descriptor captured when the registration was accepted.
    pub descriptor: ProviderDescriptor,
}

/// Structurally isolated result of observation delivery.
///
/// The receipt intentionally has no result payload or opaque extensions, so an
/// observer result cannot be handed to the context compiler by type accident.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObserverReceipt {
    /// Target provider identity.
    pub provider_id: OwnedProviderId,
    /// Accepted registration revision.
    pub registration_revision: u64,
    /// Stable operation identity.
    pub operation_id: String,
    /// Typed terminal result.
    pub terminal_code: TerminalCode,
    /// Truthful provider-local committed effect.
    pub committed_effect: CommittedEffectState,
    /// Optional provider receipt digest.
    pub provider_receipt_sha256: Option<String>,
    /// Provider-local state generation after delivery.
    pub state_generation: u64,
    /// Bounded diagnostics that cannot affect product output.
    pub warnings: Vec<String>,
}

/// Capability-driven provider registry and bounded call router.
pub struct MemoryFabric {
    config: FabricConfig,
    registrations: RwLock<BTreeMap<OwnedProviderId, Registration>>,
    permits: PermitCounter,
}

impl MemoryFabric {
    /// Creates an empty fabric with finite limits and no background work.
    pub fn new(config: FabricConfig) -> Result<Self, FabricError> {
        let config = FabricConfig::new(
            config.max_registered_providers,
            config.max_in_flight,
        )?;
        Ok(Self {
            config,
            registrations: RwLock::new(BTreeMap::new()),
            permits: PermitCounter::new(config.max_in_flight),
        })
    }

    /// Registers one concrete provider behind its stable identity.
    pub fn register(
        &self,
        provider_id: OwnedProviderId,
        registration_revision: u64,
        mode: ProviderMode,
        provider: Arc<dyn MemoryProvider>,
    ) -> Result<(), FabricError> {
        if registration_revision == 0 {
            return Err(FabricError::InvalidConfig(
                "registration_revision must be positive",
            ));
        }
        let descriptor = provider.descriptor();
        if descriptor.provider_id != provider_id {
            return Err(FabricError::ProviderDescriptorMismatch {
                selected: provider_id.as_str().to_owned(),
                declared: descriptor.provider_id.as_str().to_owned(),
            });
        }
        for mandatory in [
            "provider.health.v1",
            "observation.accept.v1",
            "recall.query.v1",
        ] {
            if !descriptor.supports(mandatory) {
                return Err(FabricError::MissingCapability(mandatory.to_owned()));
            }
        }
        let mut registrations = self
            .registrations
            .write()
            .map_err(|_| FabricError::RegistryPoisoned)?;
        let at_capacity = registrations.len() >= self.config.max_registered_providers;
        match registrations.entry(provider_id) {
            Entry::Occupied(entry) => Err(FabricError::DuplicateProvider(
                entry.key().as_str().to_owned(),
            )),
            Entry::Vacant(entry) if at_capacity => {
                let _ = entry;
                Err(FabricError::RegistryCapacityExhausted)
            }
            Entry::Vacant(entry) => {
                entry.insert(Registration {
                    revision: registration_revision,
                    mode,
                    descriptor,
                    provider,
                });
                Ok(())
            }
        }
    }

    /// Changes participation mode only for the accepted registration revision.
    pub fn set_mode(
        &self,
        provider_id: &OwnedProviderId,
        registration_revision: u64,
        mode: ProviderMode,
    ) -> Result<(), FabricError> {
        let mut registrations = self
            .registrations
            .write()
            .map_err(|_| FabricError::RegistryPoisoned)?;
        let registration = registrations.get_mut(provider_id).ok_or_else(|| {
            FabricError::ProviderUnknown(provider_id.as_str().to_owned())
        })?;
        Self::require_revision(registration, registration_revision)?;
        registration.mode = mode;
        Ok(())
    }

    /// Returns deterministic status for every registration.
    pub fn statuses(&self) -> Result<Vec<ProviderStatus>, FabricError> {
        let registrations = self
            .registrations
            .read()
            .map_err(|_| FabricError::RegistryPoisoned)?;
        Ok(registrations
            .iter()
            .map(|(provider_id, registration)| ProviderStatus {
                provider_id: provider_id.clone(),
                registration_revision: registration.revision,
                mode: registration.mode,
                descriptor: registration.descriptor.clone(),
            })
            .collect())
    }

    /// Performs a bounded read-only handshake for a non-disabled provider.
    pub fn handshake(
        &self,
        request: &HandshakeRequest,
    ) -> Result<HandshakeResponse, FabricError> {
        let registration = self.registration(&request.provider_id)?;
        Self::require_revision(&registration, request.registration_revision)?;
        Self::require_enabled(&request.provider_id, registration.mode)?;
        Self::require_capabilities(
            &registration.descriptor,
            request
                .required_capabilities
                .iter()
                .map(|capability| capability.as_str()),
        )?;
        Self::preflight(&request.control)?;
        let _permit = self.permits.try_acquire()?;
        let response = registration.provider.handshake(request);
        Self::validate_operation_id(&request.request_id, &response.terminal.operation_id)?;
        if response.terminal.terminal_code == TerminalCode::Success {
            let descriptor = response
                .descriptor
                .as_ref()
                .ok_or(FabricError::SuccessfulHandshakeMissingDescriptor)?;
            if descriptor.provider_id != request.provider_id {
                return Err(FabricError::SuccessfulHandshakeProviderMismatch);
            }
            if response.accepted_scope.as_ref() != Some(&request.exact_scope) {
                return Err(FabricError::SuccessfulHandshakeScopeMismatch);
            }
        }
        Ok(response)
    }

    /// Invokes one operation that is allowed to influence active product flow.
    pub fn invoke_active(&self, call: &ProviderCall) -> Result<ProviderReply, FabricError> {
        let registration = self.registration(&call.provider_id)?;
        Self::require_revision(&registration, call.registration_revision)?;
        match registration.mode {
            ProviderMode::Active => {}
            ProviderMode::Observer => {
                return Err(FabricError::ProviderObserverOnly(
                    call.provider_id.as_str().to_owned(),
                ));
            }
            ProviderMode::Disabled => {
                return Err(FabricError::ProviderDisabled(
                    call.provider_id.as_str().to_owned(),
                ));
            }
        }
        Self::require_call_capabilities(&registration.descriptor, call)?;
        Self::preflight(&call.control)?;
        let _permit = self.permits.try_acquire()?;
        let reply = registration.provider.invoke(call);
        Self::validate_operation_id(&call.operation_id, &reply.terminal.operation_id)?;
        Ok(reply)
    }

    /// Delivers one observation to an observer or active provider.
    ///
    /// The return type strips payloads and extensions so observer execution is
    /// structurally unable to contribute context through this route.
    pub fn deliver_observation(
        &self,
        call: &ProviderCall,
    ) -> Result<ObserverReceipt, FabricError> {
        if call.operation.capability_id() != "observation.accept.v1" {
            return Err(FabricError::OperationNotObservation);
        }
        let registration = self.registration(&call.provider_id)?;
        Self::require_revision(&registration, call.registration_revision)?;
        Self::require_enabled(&call.provider_id, registration.mode)?;
        Self::require_call_capabilities(&registration.descriptor, call)?;
        Self::preflight(&call.control)?;
        let _permit = self.permits.try_acquire()?;
        let reply = registration.provider.invoke(call);
        Self::validate_operation_id(&call.operation_id, &reply.terminal.operation_id)?;
        Ok(ObserverReceipt {
            provider_id: call.provider_id.clone(),
            registration_revision: call.registration_revision,
            operation_id: reply.terminal.operation_id,
            terminal_code: reply.terminal.terminal_code,
            committed_effect: reply.terminal.committed_effect,
            provider_receipt_sha256: reply.terminal.provider_receipt_sha256,
            state_generation: reply.state_generation,
            warnings: reply.warnings,
        })
    }

    fn registration(
        &self,
        provider_id: &OwnedProviderId,
    ) -> Result<Registration, FabricError> {
        let registrations = self
            .registrations
            .read()
            .map_err(|_| FabricError::RegistryPoisoned)?;
        registrations.get(provider_id).cloned().ok_or_else(|| {
            FabricError::ProviderUnknown(provider_id.as_str().to_owned())
        })
    }

    fn require_revision(
        registration: &Registration,
        requested: u64,
    ) -> Result<(), FabricError> {
        if registration.revision == requested {
            Ok(())
        } else {
            Err(FabricError::RegistrationRevisionMismatch {
                accepted: registration.revision,
                requested,
            })
        }
    }

    fn require_enabled(
        provider_id: &OwnedProviderId,
        mode: ProviderMode,
    ) -> Result<(), FabricError> {
        if mode == ProviderMode::Disabled {
            Err(FabricError::ProviderDisabled(
                provider_id.as_str().to_owned(),
            ))
        } else {
            Ok(())
        }
    }

    fn require_capabilities<'a>(
        descriptor: &ProviderDescriptor,
        capabilities: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), FabricError> {
        for capability in capabilities {
            if !descriptor.supports(capability) {
                return Err(FabricError::MissingCapability(capability.to_owned()));
            }
        }
        Ok(())
    }

    fn require_call_capabilities(
        descriptor: &ProviderDescriptor,
        call: &ProviderCall,
    ) -> Result<(), FabricError> {
        Self::require_capabilities(
            descriptor,
            call.required_capabilities
                .iter()
                .map(|capability| capability.as_str()),
        )?;
        if descriptor.supports(call.operation.capability_id()) {
            Ok(())
        } else {
            Err(FabricError::MissingCapability(
                call.operation.capability_id().to_owned(),
            ))
        }
    }

    fn preflight(
        control: &tracedecay_memory_provider_api::OperationControl,
    ) -> Result<(), FabricError> {
        match control.snapshot() {
            Ok(_) => Ok(()),
            Err(TerminalCode::Cancelled) => Err(FabricError::Cancelled),
            Err(TerminalCode::DeadlineExceeded) => Err(FabricError::DeadlineExceeded),
            Err(_) => Err(FabricError::InvalidConfig(
                "request control returned an invalid preflight terminal",
            )),
        }
    }

    fn validate_operation_id(expected: &str, returned: &str) -> Result<(), FabricError> {
        if expected == returned {
            Ok(())
        } else {
            Err(FabricError::ResponseOperationMismatch {
                expected: expected.to_owned(),
                returned: returned.to_owned(),
            })
        }
    }
}

#[cfg(test)]
mod permit_tests {
    use super::{FabricError, PermitCounter};

    #[test]
    fn finite_permit_counter_rejects_excess_concurrency() -> Result<(), FabricError> {
        let counter = PermitCounter::new(1);
        let first = counter.try_acquire()?;
        assert!(matches!(
            counter.try_acquire(),
            Err(FabricError::CapacityExhausted)
        ));
        drop(first);
        let second = counter.try_acquire()?;
        drop(second);
        Ok(())
    }
}
'''

TESTS = r'''//! Behavioral tests for capability-driven memory-fabric orchestration.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tracedecay_memory_fabric::{
    FabricConfig, FabricError, MemoryFabric, ProviderMode,
};
use tracedecay_memory_provider_api::contract::{
    CommittedEffectState, FallbackEligibility, TerminalCode,
};
use tracedecay_memory_provider_api::{
    ApiError, CancellationToken, CanonicalPayload, HandshakeRequest,
    HandshakeRequestParts, HandshakeResponse, MemoryProvider, OperationControl,
    OwnedExactScope, OwnedProviderId, OwnedVersionedId, ProviderCall,
    ProviderCallParts, ProviderDescriptor, ProviderLimits, ProviderOperation,
    ProviderReply, TerminalRecord,
};

const DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn provider_id(value: &str) -> Result<OwnedProviderId, ApiError> {
    OwnedProviderId::new(value)
}

fn capability(value: &str) -> Result<OwnedVersionedId, ApiError> {
    OwnedVersionedId::new(value)
}

fn scope() -> Result<OwnedExactScope, ApiError> {
    OwnedExactScope::new(
        "profile-1",
        "project-1",
        "repo-1",
        "worktree-1",
        "refs/heads/main",
        "session-1",
        9,
    )
}

fn limits() -> ProviderLimits {
    ProviderLimits {
        request_bytes: 4096,
        response_bytes: 8192,
        observation_batch_items: 8,
        recall_candidates: 16,
        concurrent_operations: 2,
        operation_millis: 1000,
        snapshot_bytes: 16384,
        inspection_items: 32,
    }
}

fn payload() -> Result<CanonicalPayload, ApiError> {
    CanonicalPayload::new(
        capability("tracedecay.memory.test-request.v1")?,
        br#"{}"#.to_vec(),
        DIGEST,
    )
}

fn terminal(
    code: TerminalCode,
    effect: CommittedEffectState,
    operation_id: &str,
) -> TerminalRecord {
    TerminalRecord {
        terminal_code: code,
        committed_effect: effect,
        fallback: FallbackEligibility::Forbidden,
        operation_id: operation_id.to_owned(),
        exact_scope_sha256: DIGEST.to_owned(),
        provider_receipt_sha256: None,
        diagnostic_id: None,
    }
}

struct TestProvider {
    descriptor: ProviderDescriptor,
    invocations: AtomicUsize,
}

impl TestProvider {
    fn new(provider: &str, extra: &[&str]) -> Result<Self, ApiError> {
        let mut capabilities = vec![
            capability("provider.health.v1")?,
            capability("observation.accept.v1")?,
            capability("recall.query.v1")?,
        ];
        for value in extra {
            capabilities.push(capability(value)?);
        }
        Ok(Self {
            descriptor: ProviderDescriptor::new(
                provider_id(provider)?,
                DIGEST,
                "state.v1",
                0,
                capabilities,
                limits(),
            )?,
            invocations: AtomicUsize::new(0),
        })
    }

    fn invocation_count(&self) -> usize {
        self.invocations.load(Ordering::Acquire)
    }
}

impl MemoryProvider for TestProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        HandshakeResponse {
            terminal: terminal(
                TerminalCode::Success,
                CommittedEffectState::None,
                &request.request_id,
            ),
            descriptor: Some(self.descriptor.clone()),
            provider_instance_id: Some("test.provider.instance-1".to_owned()),
            state_namespace: Some("scope-1".to_owned()),
            accepted_scope: Some(request.exact_scope.clone()),
            effective_limits: Some(request.host_limits.minimum(self.descriptor.limits)),
            ready_receipt_sha256: Some(DIGEST.to_owned()),
            warnings: Vec::new(),
        }
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        self.invocations.fetch_add(1, Ordering::AcqRel);
        let (code, effect, receipt) = if call.operation == ProviderOperation::Observe {
            (
                TerminalCode::Success,
                CommittedEffectState::Applied,
                Some(DIGEST.to_owned()),
            )
        } else {
            (
                TerminalCode::SuccessZeroResults,
                CommittedEffectState::None,
                None,
            )
        };
        let mut terminal = terminal(code, effect, &call.operation_id);
        terminal.provider_receipt_sha256 = receipt;
        ProviderReply {
            terminal,
            payload: Some(call.payload.clone()),
            warnings: vec!["test-warning".to_owned()],
            extensions: call.extensions.clone(),
            state_generation: self.descriptor.state_generation + 1,
        }
    }
}

fn handshake_request(provider: &str) -> Result<HandshakeRequest, ApiError> {
    HandshakeRequest::new(HandshakeRequestParts {
        provider_id: provider_id(provider)?,
        registration_revision: 1,
        exact_scope: scope()?,
        request_id: "handshake-1".to_owned(),
        required_capabilities: vec![
            capability("provider.health.v1")?,
            capability("observation.accept.v1")?,
            capability("recall.query.v1")?,
        ],
        host_limits: limits(),
        control: OperationControl::new(123, 100, CancellationToken::new()),
        challenge_nonce: [3; 32],
    })
}

fn call(
    provider: &str,
    operation: ProviderOperation,
    idempotency_key: Option<&str>,
    required: &[&str],
    control: OperationControl,
) -> Result<ProviderCall, ApiError> {
    let capabilities = required
        .iter()
        .map(|value| capability(value))
        .collect::<Result<Vec<_>, _>>()?;
    ProviderCall::new(ProviderCallParts {
        operation,
        provider_id: provider_id(provider)?,
        registration_revision: 1,
        ready_receipt_sha256: DIGEST.to_owned(),
        exact_scope: scope()?,
        request_id: format!("request-{}", operation.capability_id()),
        operation_id: format!("operation-{}", operation.capability_id()),
        expected_state_generation: 0,
        idempotency_key: idempotency_key.map(str::to_owned),
        control,
        payload: payload()?,
        required_capabilities: capabilities,
        extensions: Vec::new(),
    })
}

#[test]
fn registry_is_bounded_and_rejects_duplicate_or_mismatched_identity() -> Result<(), Box<dyn Error>> {
    let fabric = MemoryFabric::new(FabricConfig::new(1, 1)?)?;
    let provider = Arc::new(TestProvider::new("provider.one", &[])?);
    fabric.register(
        provider_id("provider.one")?,
        1,
        ProviderMode::Disabled,
        provider.clone(),
    )?;
    assert!(matches!(
        fabric.register(
            provider_id("provider.one")?,
            1,
            ProviderMode::Disabled,
            provider.clone(),
        ),
        Err(FabricError::DuplicateProvider(_))
    ));
    let second = Arc::new(TestProvider::new("provider.two", &[])?);
    assert!(matches!(
        fabric.register(
            provider_id("provider.two")?,
            1,
            ProviderMode::Disabled,
            second,
        ),
        Err(FabricError::RegistryCapacityExhausted)
    ));

    let other_fabric = MemoryFabric::new(FabricConfig::new(2, 1)?)?;
    assert!(matches!(
        other_fabric.register(
            provider_id("selected.provider")?,
            1,
            ProviderMode::Disabled,
            provider,
        ),
        Err(FabricError::ProviderDescriptorMismatch { .. })
    ));
    Ok(())
}

#[test]
fn active_provider_is_selected_by_identity_and_capability() -> Result<(), Box<dyn Error>> {
    let fabric = MemoryFabric::new(FabricConfig::new(4, 2)?)?;
    let provider = Arc::new(TestProvider::new("provider.active", &[])?);
    fabric.register(
        provider_id("provider.active")?,
        1,
        ProviderMode::Active,
        provider.clone(),
    )?;
    let response = fabric.handshake(&handshake_request("provider.active")?)?;
    assert_eq!(response.terminal.terminal_code, TerminalCode::Success);

    let recall = call(
        "provider.active",
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(123, 100, CancellationToken::new()),
    )?;
    let reply = fabric.invoke_active(&recall)?;
    assert_eq!(reply.terminal.terminal_code, TerminalCode::SuccessZeroResults);
    assert_eq!(provider.invocation_count(), 1);

    let feedback = call(
        "provider.active",
        ProviderOperation::Feedback,
        Some(DIGEST),
        &["feedback.record.v1"],
        OperationControl::new(123, 100, CancellationToken::new()),
    )?;
    assert!(matches!(
        fabric.invoke_active(&feedback),
        Err(FabricError::MissingCapability(capability))
            if capability == "feedback.record.v1"
    ));
    assert_eq!(provider.invocation_count(), 1);
    Ok(())
}

#[test]
fn observer_delivery_is_structurally_isolated_from_active_output() -> Result<(), Box<dyn Error>> {
    let fabric = MemoryFabric::new(FabricConfig::new(4, 2)?)?;
    let provider = Arc::new(TestProvider::new("provider.observer", &[])?);
    fabric.register(
        provider_id("provider.observer")?,
        1,
        ProviderMode::Observer,
        provider.clone(),
    )?;
    let observe = call(
        "provider.observer",
        ProviderOperation::Observe,
        Some(DIGEST),
        &["observation.accept.v1"],
        OperationControl::new(123, 100, CancellationToken::new()),
    )?;
    let receipt = fabric.deliver_observation(&observe)?;
    assert_eq!(receipt.terminal_code, TerminalCode::Success);
    assert_eq!(receipt.committed_effect, CommittedEffectState::Applied);
    assert_eq!(provider.invocation_count(), 1);

    let recall = call(
        "provider.observer",
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(123, 100, CancellationToken::new()),
    )?;
    assert!(matches!(
        fabric.invoke_active(&recall),
        Err(FabricError::ProviderObserverOnly(_))
    ));
    assert_eq!(provider.invocation_count(), 1);
    Ok(())
}

#[test]
fn disabled_provider_and_wrong_revision_fail_before_provider_contact() -> Result<(), Box<dyn Error>> {
    let fabric = MemoryFabric::new(FabricConfig::new(4, 2)?)?;
    let provider = Arc::new(TestProvider::new("provider.disabled", &[])?);
    fabric.register(
        provider_id("provider.disabled")?,
        1,
        ProviderMode::Disabled,
        provider.clone(),
    )?;
    assert!(matches!(
        fabric.handshake(&handshake_request("provider.disabled")?),
        Err(FabricError::ProviderDisabled(_))
    ));
    fabric.set_mode(&provider_id("provider.disabled")?, 1, ProviderMode::Active)?;
    let mut recall = call(
        "provider.disabled",
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(123, 100, CancellationToken::new()),
    )?;
    recall.registration_revision = 2;
    assert!(matches!(
        fabric.invoke_active(&recall),
        Err(FabricError::RegistrationRevisionMismatch {
            accepted: 1,
            requested: 2
        })
    ));
    assert_eq!(provider.invocation_count(), 0);
    Ok(())
}

#[test]
fn cancellation_and_deadline_are_terminal_before_provider_contact() -> Result<(), Box<dyn Error>> {
    let fabric = MemoryFabric::new(FabricConfig::new(4, 2)?)?;
    let provider = Arc::new(TestProvider::new("provider.controlled", &[])?);
    fabric.register(
        provider_id("provider.controlled")?,
        1,
        ProviderMode::Active,
        provider.clone(),
    )?;

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = call(
        "provider.controlled",
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(123, 100, cancellation),
    )?;
    assert_eq!(fabric.invoke_active(&cancelled), Err(FabricError::Cancelled));

    let expired = call(
        "provider.controlled",
        ProviderOperation::Recall,
        None,
        &["recall.query.v1"],
        OperationControl::new(123, 0, CancellationToken::new()),
    )?;
    assert_eq!(
        fabric.invoke_active(&expired),
        Err(FabricError::DeadlineExceeded)
    );
    assert_eq!(provider.invocation_count(), 0);
    Ok(())
}

#[test]
fn statuses_are_deterministic_and_mode_changes_require_revision() -> Result<(), Box<dyn Error>> {
    let fabric = MemoryFabric::new(FabricConfig::new(4, 2)?)?;
    let provider_b = Arc::new(TestProvider::new("provider.b", &[])?);
    let provider_a = Arc::new(TestProvider::new("provider.a", &[])?);
    fabric.register(
        provider_id("provider.b")?,
        1,
        ProviderMode::Observer,
        provider_b,
    )?;
    fabric.register(
        provider_id("provider.a")?,
        1,
        ProviderMode::Disabled,
        provider_a,
    )?;
    assert!(matches!(
        fabric.set_mode(&provider_id("provider.a")?, 2, ProviderMode::Active),
        Err(FabricError::RegistrationRevisionMismatch { .. })
    ));
    fabric.set_mode(&provider_id("provider.a")?, 1, ProviderMode::Active)?;
    let statuses = fabric.statuses()?;
    assert_eq!(statuses.len(), 2);
    assert_eq!(statuses[0].provider_id.as_str(), "provider.a");
    assert_eq!(statuses[0].mode, ProviderMode::Active);
    assert_eq!(statuses[1].provider_id.as_str(), "provider.b");
    Ok(())
}
'''

README = '''# tracedecay-memory-fabric

Capability-driven, bounded orchestration over `tracedecay-memory-provider-api`.

The fabric registers concrete providers behind stable identities, checks exact registration revisions and declared capabilities, performs cancellation/deadline preflight, enforces finite registration and concurrent-call budgets, routes active calls, and returns structurally isolated observer receipts with no payload or extension channel into final context.

The crate has no provider implementation, provider-name conditional, persistence, TraceDecay DB/code-index/daemon/dashboard/host dependency, background worker, queue, or fallback policy. Native and NCM remain adapters outside this crate.
'''


def write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def update_workspace() -> None:
    path = ROOT / "Cargo.toml"
    content = path.read_text(encoding="utf-8")
    member = '    "crates/tracedecay-memory-fabric",\n'
    if member not in content:
        marker = '    "crates/tracedecay-memory-provider-api",\n'
        if marker not in content:
            raise SystemExit("provider API workspace member is missing")
        content = content.replace(marker, marker + member, 1)
        path.write_text(content, encoding="utf-8")


def update_product_policy() -> None:
    policy_path = ROOT / "product/upstream/patch-footprint-policy.json"
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    pattern = "crates/tracedecay-memory-fabric/**"
    if pattern not in policy["product_owned_paths"]:
        index = policy["product_owned_paths"].index(
            "crates/tracedecay-memory-provider-api/**"
        )
        policy["product_owned_paths"].insert(index + 1, pattern)
    policy_path.write_text(
        json.dumps(policy, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    checker_path = ROOT / "scripts/product/check-patch-footprint-policy.py"
    checker = checker_path.read_text(encoding="utf-8")
    literal = '    "crates/tracedecay-memory-fabric/**",\n'
    if literal not in checker:
        marker = '    "crates/tracedecay-memory-provider-api/**",\n'
        if marker not in checker:
            raise SystemExit("patch-policy product pattern marker is missing")
        checker = checker.replace(marker, marker + literal, 1)
        checker_path.write_text(checker, encoding="utf-8")


def update_lock() -> None:
    subprocess.run(
        ["cargo", "metadata", "--format-version", "1"],
        cwd=ROOT,
        check=True,
        stdout=subprocess.DEVNULL,
    )


def update_convergence_map() -> None:
    path = ROOT / "product/upstream/convergence-map.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    entries = {row["path"]: row for row in document["entries"]}
    for target in ("Cargo.toml", "Cargo.lock"):
        entry = entries[target]
        if "tdmem-0302" not in entry["bead_ids"]:
            entry["bead_ids"].append("tdmem-0302")
        verification = [
            "cargo metadata --locked --format-version 1 --no-deps",
            "cargo clippy -p tracedecay-memory-fabric --all-targets --locked -- -D warnings",
            "cargo test -p tracedecay-memory-fabric --locked",
        ]
        for command in verification:
            if command not in entry["verification"]:
                entry["verification"].append(command)
    document["entries"] = [entries[key] for key in sorted(entries)]

    result = subprocess.run(
        [
            "git",
            "diff",
            "--no-renames",
            "--numstat",
            FLOOR,
            "--",
            "Cargo.toml",
            "Cargo.lock",
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    files = 0
    changed_lines = 0
    for raw in result.stdout.splitlines():
        if not raw.strip():
            continue
        added, deleted, _target = raw.split("\t", 2)
        files += 1
        changed_lines += int(added) + int(deleted)
    document["snapshot"] = {
        "upstream_existing_production_files": files,
        "upstream_existing_test_or_fixture_files": 0,
        "total_upstream_changed_lines": changed_lines,
        "composition_root_files": 0,
        "exception_zone_files": 0,
        "observed_state": (
            "The product branch changes only additive root workspace membership and "
            "generated path-package lock entries; provider API and fabric remain product-owned."
        ),
    }
    path.write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


write(CRATE / "Cargo.toml", CARGO)
write(CRATE / "src/lib.rs", LIB)
write(CRATE / "tests/fabric.rs", TESTS)
write(CRATE / "README.md", README)
update_workspace()
update_product_policy()
update_lock()
subprocess.run(
    ["cargo", "fmt", "--package", "tracedecay-memory-fabric"],
    cwd=ROOT,
    check=True,
)
update_convergence_map()

manifest = [
    {
        "path": "crates/tracedecay-memory-fabric",
        "message": "feat(memory): add capability-driven fabric",
    },
    {
        "path": "Cargo.toml",
        "message": "build(memory): register memory fabric workspace member",
    },
    {
        "path": "Cargo.lock",
        "message": "build(memory): lock memory fabric path package",
    },
    {
        "path": "product/upstream/patch-footprint-policy.json",
        "message": "docs(upstream): register memory fabric ownership",
    },
    {
        "path": "scripts/product/check-patch-footprint-policy.py",
        "message": "test(upstream): recognize memory fabric ownership",
    },
    {
        "path": "product/upstream/convergence-map.json",
        "message": "docs(upstream): map memory fabric workspace wiring",
    },
]
write(
    ROOT / ".beads/operations/prepared-files.json",
    json.dumps(manifest, indent=2) + "\n",
)
Path(__file__).unlink()
