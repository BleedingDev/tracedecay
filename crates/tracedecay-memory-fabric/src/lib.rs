#![forbid(unsafe_code)]
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

use tracedecay_memory_provider_api::contract::{CommittedEffectState, TerminalCode};
use tracedecay_memory_provider_api::{
    ApiError, HandshakeRequest, HandshakeResponse, MemoryProvider, OwnedProviderId, ProviderCall,
    ProviderDescriptor, ProviderReply,
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
    pub fn new(max_registered_providers: usize, max_in_flight: usize) -> Result<Self, FabricError> {
        if max_registered_providers == 0 {
            return Err(FabricError::InvalidConfig(
                "max_registered_providers must be positive",
            ));
        }
        if max_in_flight == 0 {
            return Err(FabricError::InvalidConfig("max_in_flight must be positive"));
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
            Self::ProviderDisabled(provider) => {
                write!(formatter, "provider {provider} is disabled")
            }
            Self::ProviderObserverOnly(provider) => {
                write!(formatter, "provider {provider} is observer-only")
            }
            Self::OperationNotObservation => {
                formatter.write_str("observer delivery requires observation.accept.v1")
            }
            Self::MissingCapability(capability) => {
                write!(
                    formatter,
                    "provider does not declare capability {capability}"
                )
            }
            Self::CapacityExhausted => formatter.write_str("provider call capacity is exhausted"),
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
        let config = FabricConfig::new(config.max_registered_providers, config.max_in_flight)?;
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
            Entry::Vacant(_) if at_capacity => Err(FabricError::RegistryCapacityExhausted),
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
        let registration = registrations
            .get_mut(provider_id)
            .ok_or_else(|| FabricError::ProviderUnknown(provider_id.as_str().to_owned()))?;
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
    pub fn handshake(&self, request: &HandshakeRequest) -> Result<HandshakeResponse, FabricError> {
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
    pub fn deliver_observation(&self, call: &ProviderCall) -> Result<ObserverReceipt, FabricError> {
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

    fn registration(&self, provider_id: &OwnedProviderId) -> Result<Registration, FabricError> {
        let registrations = self
            .registrations
            .read()
            .map_err(|_| FabricError::RegistryPoisoned)?;
        registrations
            .get(provider_id)
            .cloned()
            .ok_or_else(|| FabricError::ProviderUnknown(provider_id.as_str().to_owned()))
    }

    fn require_revision(registration: &Registration, requested: u64) -> Result<(), FabricError> {
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
