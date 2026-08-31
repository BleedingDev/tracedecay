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

use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use tracedecay_memory_provider_api::contract::{CAPABILITIES, FallbackEligibility, TerminalCode};
use tracedecay_memory_provider_api::{
    ApiError, FallbackDirective, HandshakeRequest, HandshakeResponse, MemoryProvider,
    OwnedExactScope, OwnedProviderId, OwnedVersionedId, ProviderCall, ProviderDescriptor,
    ProviderLimits, ProviderOperation, ProviderReply, TerminalRecord,
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
    /// A provider-local dispatch gate was poisoned by an aborted call.
    ProviderGatePoisoned,
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
    /// The provider has no successful readiness bound to this registration.
    ProviderNotReady(String),
    /// The call did not carry the currently accepted ready receipt.
    ReadyReceiptMismatch,
    /// The call scope differed from the scope bound by the ready receipt.
    ReadyScopeMismatch,
    /// The call generation differed from the generation bound by readiness.
    ReadyStateGenerationMismatch {
        /// Generation retained by readiness.
        ready: u64,
        /// Generation requested by the call.
        requested: u64,
    },
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
    /// A provider returned a terminal record for another operation kind.
    ResponseOperationKindMismatch {
        /// Expected provider operation.
        expected: ProviderOperation,
        /// Returned provider operation.
        returned: ProviderOperation,
    },
    /// A provider returned a terminal record attributed to another provider.
    ResponseProviderMismatch {
        /// Registry-selected provider ID.
        expected: String,
        /// Terminal-attributed provider ID.
        returned: String,
    },
    /// A provider returned a terminal record for another exact scope.
    ResponseScopeMismatch {
        /// Expected TraceDecay-owned exact-scope digest.
        expected: String,
        /// Returned exact-scope digest.
        returned: String,
    },
    /// A provider reply generation contradicted its committed-effect evidence.
    ResponseStateGenerationMismatch {
        /// Generation retained by the committed-effect evidence.
        evidence: u64,
        /// Generation reported by the provider reply or descriptor.
        reported: u64,
    },
    /// Provider effect evidence omitted the call's required starting generation.
    ResponseStateGenerationBeforeMissing {
        /// Generation required by the admitted call.
        expected: u64,
    },
    /// Provider effect evidence omitted the reported settled generation.
    ResponseStateGenerationAfterMissing {
        /// Generation reported by the reply or handshake descriptor.
        reported: u64,
    },
    /// A successful handshake omitted its compatible descriptor.
    SuccessfulHandshakeMissingDescriptor,
    /// A successful handshake returned another provider identity.
    SuccessfulHandshakeProviderMismatch,
    /// A successful handshake descriptor differed from the registered descriptor.
    SuccessfulHandshakeDescriptorMismatch,
    /// A successful handshake descriptor regressed the accepted state generation.
    SuccessfulHandshakeStateGenerationRegressed {
        /// Generation most recently accepted by the fabric.
        accepted: u64,
        /// Generation returned by the handshake.
        returned: u64,
    },
    /// A successful handshake accepted another coding scope.
    SuccessfulHandshakeScopeMismatch,
    /// A successful handshake did not negotiate the exact lower ceilings.
    SuccessfulHandshakeEffectiveLimitsMismatch,
    /// A failed handshake carried fields reserved for accepted readiness.
    FailedHandshakeCarriedReadiness,
}

impl fmt::Display for FabricError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => formatter.write_str(message),
            Self::Api(error) => write!(formatter, "provider API error: {error}"),
            Self::RegistryPoisoned => formatter.write_str("provider registry lock is poisoned"),
            Self::ProviderGatePoisoned => formatter.write_str("provider dispatch gate is poisoned"),
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
            Self::ProviderNotReady(provider) => {
                write!(formatter, "provider {provider} has no accepted readiness")
            }
            Self::ReadyReceiptMismatch => {
                formatter.write_str("provider call ready receipt is not current")
            }
            Self::ReadyScopeMismatch => {
                formatter.write_str("provider call scope differs from accepted readiness")
            }
            Self::ReadyStateGenerationMismatch { ready, requested } => write!(
                formatter,
                "provider call generation mismatch: ready {ready}, requested {requested}"
            ),
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
            Self::ResponseOperationKindMismatch { expected, returned } => write!(
                formatter,
                "provider response operation-kind mismatch: expected {}, returned {}",
                expected.as_wire(),
                returned.as_wire()
            ),
            Self::ResponseProviderMismatch { expected, returned } => write!(
                formatter,
                "provider response identity mismatch: expected {expected}, returned {returned}"
            ),
            Self::ResponseScopeMismatch { expected, returned } => write!(
                formatter,
                "provider response exact-scope mismatch: expected {expected}, returned {returned}"
            ),
            Self::ResponseStateGenerationMismatch { evidence, reported } => write!(
                formatter,
                "provider response state-generation mismatch: effect evidence {evidence}, reported {reported}"
            ),
            Self::ResponseStateGenerationBeforeMissing { expected } => write!(
                formatter,
                "provider response omitted state-generation-before; expected {expected}"
            ),
            Self::ResponseStateGenerationAfterMissing { reported } => write!(
                formatter,
                "provider response omitted state-generation-after; reported {reported}"
            ),
            Self::SuccessfulHandshakeMissingDescriptor => {
                formatter.write_str("successful handshake omitted provider descriptor")
            }
            Self::SuccessfulHandshakeProviderMismatch => {
                formatter.write_str("successful handshake returned another provider")
            }
            Self::SuccessfulHandshakeDescriptorMismatch => {
                formatter.write_str("successful handshake changed immutable descriptor fields")
            }
            Self::SuccessfulHandshakeStateGenerationRegressed { accepted, returned } => write!(
                formatter,
                "successful handshake state generation regressed: accepted {accepted}, returned {returned}"
            ),
            Self::SuccessfulHandshakeScopeMismatch => {
                formatter.write_str("successful handshake accepted another exact scope")
            }
            Self::SuccessfulHandshakeEffectiveLimitsMismatch => {
                formatter.write_str("successful handshake returned incorrect effective limits")
            }
            Self::FailedHandshakeCarriedReadiness => {
                formatter.write_str("failed handshake carried readiness metadata")
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
    dispatch_gate: Arc<Mutex<()>>,
    accepted_state_generation: u64,
    readiness_epoch: u64,
    readiness: Option<Readiness>,
}

#[derive(Clone)]
struct Readiness {
    epoch: u64,
    registration_revision: u64,
    provider_instance_id: String,
    state_namespace: String,
    exact_scope: OwnedExactScope,
    state_generation: u64,
    capabilities: BTreeSet<OwnedVersionedId>,
    effective_limits: ProviderLimits,
    ready_receipt_sha256: String,
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

/// Whether a provider registration has a retained, successful handshake.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderReadiness {
    /// A successful handshake is retained for the accepted registration.
    Ready,
    /// The registration has no currently retained successful handshake.
    NotReady,
}

impl ProviderReadiness {
    /// Returns whether this registration is ready for provider operations.
    #[must_use]
    pub const fn is_ready(self) -> bool {
        matches!(self, Self::Ready)
    }
}

/// Availability of one provider capability in a status projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderCapabilityAvailability {
    /// The accepted descriptor declares a known capability and readiness is retained.
    SupportedReady,
    /// The accepted descriptor declares a known capability without retained readiness.
    SupportedNotReady,
    /// The accepted descriptor does not declare this capability.
    Undeclared,
    /// The provider declared an identifier outside the canonical capability catalog.
    DataUnavailable,
}

impl ProviderCapabilityAvailability {
    /// Returns whether the accepted descriptor declares this capability.
    #[must_use]
    pub const fn is_supported(self) -> bool {
        matches!(self, Self::SupportedReady | Self::SupportedNotReady)
    }

    /// Returns whether this capability is supported by a ready registration.
    #[must_use]
    pub const fn is_ready(self) -> bool {
        matches!(self, Self::SupportedReady)
    }
}

/// Compatibility alias for callers that refer to capability states as status.
pub type ProviderCapabilityStatus = ProviderCapabilityAvailability;

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
    /// Truthful readiness derived from retained successful handshake state.
    pub readiness: ProviderReadiness,
    /// Per-capability availability, including canonical undeclared capabilities.
    pub capabilities: BTreeMap<OwnedVersionedId, ProviderCapabilityAvailability>,
    /// Effective limits from the retained successful handshake, if any.
    pub effective_limits: Option<ProviderLimits>,
    /// Ready-receipt digest from the retained successful handshake, if any.
    pub ready_receipt_sha256: Option<String>,
}

impl ProviderStatus {
    /// Returns the typed availability of a queried capability identifier.
    #[must_use]
    pub fn capability_availability(&self, capability_id: &str) -> ProviderCapabilityAvailability {
        self.capabilities
            .iter()
            .find(|(declared, _)| declared.as_str() == capability_id)
            .map(|(_, availability)| *availability)
            .unwrap_or(ProviderCapabilityAvailability::Undeclared)
    }

    /// Returns the typed availability of a queried capability identifier.
    #[must_use]
    pub fn capability_status(&self, capability_id: &str) -> ProviderCapabilityAvailability {
        self.capability_availability(capability_id)
    }
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
    /// Validated terminal result, including complete effect and fallback evidence.
    pub terminal: TerminalRecord,
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
        descriptor.validate()?;
        let accepted_state_generation = descriptor.state_generation;
        if descriptor.provider_id != provider_id {
            return Err(FabricError::ProviderDescriptorMismatch {
                selected: provider_id.as_str().to_owned(),
                declared: descriptor.provider_id.as_str().to_owned(),
            });
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
                    dispatch_gate: Arc::new(Mutex::new(())),
                    accepted_state_generation,
                    readiness_epoch: 0,
                    readiness: None,
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
        let snapshot = self.registration(provider_id)?;
        Self::require_revision(&snapshot, registration_revision)?;
        let _dispatch = snapshot
            .dispatch_gate
            .lock()
            .map_err(|_| FabricError::ProviderGatePoisoned)?;
        let mut registrations = self
            .registrations
            .write()
            .map_err(|_| FabricError::RegistryPoisoned)?;
        let registration = registrations
            .get_mut(provider_id)
            .ok_or_else(|| FabricError::ProviderUnknown(provider_id.as_str().to_owned()))?;
        Self::require_revision(registration, registration_revision)?;
        registration.readiness_epoch = registration
            .readiness_epoch
            .checked_add(1)
            .ok_or(FabricError::InvalidConfig("readiness epoch is exhausted"))?;
        registration.readiness = None;
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
            .map(|(provider_id, registration)| {
                let readiness = Self::retained_readiness(registration);
                ProviderStatus {
                    provider_id: provider_id.clone(),
                    registration_revision: registration.revision,
                    mode: registration.mode,
                    descriptor: registration.descriptor.clone(),
                    readiness: if readiness.is_some() {
                        ProviderReadiness::Ready
                    } else {
                        ProviderReadiness::NotReady
                    },
                    capabilities: Self::status_capabilities(&registration.descriptor, readiness),
                    effective_limits: readiness.map(|readiness| readiness.effective_limits),
                    ready_receipt_sha256: readiness
                        .map(|readiness| readiness.ready_receipt_sha256.clone()),
                }
            })
            .collect())
    }

    fn retained_readiness(registration: &Registration) -> Option<&Readiness> {
        let readiness = registration.readiness.as_ref()?;
        if registration.mode == ProviderMode::Disabled
            || readiness.epoch != registration.readiness_epoch
            || readiness.registration_revision != registration.revision
            || readiness.capabilities != registration.descriptor.capabilities
            || readiness.state_generation != registration.accepted_state_generation
            || readiness.provider_instance_id.trim().is_empty()
            || readiness.state_namespace.trim().is_empty()
        {
            None
        } else {
            Some(readiness)
        }
    }

    fn status_capabilities(
        descriptor: &ProviderDescriptor,
        readiness: Option<&Readiness>,
    ) -> BTreeMap<OwnedVersionedId, ProviderCapabilityAvailability> {
        let mut capabilities = BTreeMap::new();
        for capability in &descriptor.capabilities {
            capabilities.insert(
                capability.clone(),
                Self::status_capability_availability(descriptor, readiness, capability),
            );
        }
        for specification in CAPABILITIES {
            if let Ok(capability) = OwnedVersionedId::new(specification.capability_id) {
                capabilities.entry(capability.clone()).or_insert_with(|| {
                    Self::status_capability_availability(descriptor, readiness, &capability)
                });
            }
        }
        capabilities
    }

    fn status_capability_availability(
        descriptor: &ProviderDescriptor,
        readiness: Option<&Readiness>,
        capability: &OwnedVersionedId,
    ) -> ProviderCapabilityAvailability {
        if !descriptor.capabilities.contains(capability) {
            return ProviderCapabilityAvailability::Undeclared;
        }
        if !CAPABILITIES
            .iter()
            .any(|specification| specification.capability_id == capability.as_str())
        {
            return ProviderCapabilityAvailability::DataUnavailable;
        }
        if readiness.is_some_and(|readiness| readiness.capabilities.contains(capability)) {
            ProviderCapabilityAvailability::SupportedReady
        } else {
            ProviderCapabilityAvailability::SupportedNotReady
        }
    }

    /// Performs a bounded read-only handshake for a non-disabled provider.
    pub fn handshake(&self, request: &HandshakeRequest) -> Result<HandshakeResponse, FabricError> {
        request.validate()?;
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
        let dispatch_gate = Arc::clone(&registration.dispatch_gate);
        let _dispatch = dispatch_gate
            .lock()
            .map_err(|_| FabricError::ProviderGatePoisoned)?;
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
        let readiness_epoch =
            self.invalidate_readiness(&request.provider_id, request.registration_revision)?;
        let response = registration.provider.handshake(request);
        Self::validate_terminal(
            ProviderOperation::Handshake,
            &request.provider_id,
            &request.request_id,
            &request.exact_scope,
            &response.terminal,
            None,
            None,
        )?;
        if response.warnings.len() > 32 {
            return Err(ApiError::TooManyBoundaryItems {
                field: "warnings",
                maximum: 32,
            }
            .into());
        }
        if response.terminal.terminal_code() != TerminalCode::Success {
            if response.descriptor.is_some()
                || response.provider_instance_id.is_some()
                || response.state_namespace.is_some()
                || response.accepted_scope.is_some()
                || response.effective_limits.is_some()
                || response.ready_receipt_sha256.is_some()
            {
                return Err(FabricError::FailedHandshakeCarriedReadiness);
            }
            return Ok(response);
        }
        let descriptor = response
            .descriptor
            .as_ref()
            .ok_or(FabricError::SuccessfulHandshakeMissingDescriptor)?;
        descriptor.validate()?;
        if descriptor.provider_id != request.provider_id {
            return Err(FabricError::SuccessfulHandshakeProviderMismatch);
        }
        Self::validate_handshake_descriptor(
            &registration.descriptor,
            registration.accepted_state_generation,
            descriptor,
        )?;
        if response.accepted_scope.as_ref() != Some(&request.exact_scope) {
            return Err(FabricError::SuccessfulHandshakeScopeMismatch);
        }
        let provider_instance_id = Self::require_handshake_text(
            response.provider_instance_id.as_deref(),
            "provider_instance_id",
            None,
        )?
        .to_owned();
        let state_namespace = Self::require_handshake_text(
            response.state_namespace.as_deref(),
            "state_namespace",
            Some(128),
        )?
        .to_owned();
        let effective_limits = response
            .effective_limits
            .ok_or(ApiError::EmptyField("effective_limits"))?
            .validate()?;
        let expected_limits = request.host_limits.minimum(registration.descriptor.limits);
        if effective_limits != expected_limits {
            return Err(FabricError::SuccessfulHandshakeEffectiveLimitsMismatch);
        }
        let ready_receipt_sha256 = Self::require_handshake_text(
            response.ready_receipt_sha256.as_deref(),
            "ready_receipt_sha256",
            None,
        )?
        .to_owned();
        Self::require_sha256(&ready_receipt_sha256, "ready_receipt_sha256")?;
        Self::validate_state_generation(&response.terminal, Some(descriptor.state_generation))?;
        self.install_readiness(
            &request.provider_id,
            Readiness {
                epoch: readiness_epoch,
                registration_revision: request.registration_revision,
                provider_instance_id,
                state_namespace,
                exact_scope: request.exact_scope.clone(),
                state_generation: descriptor.state_generation,
                capabilities: descriptor.capabilities.clone(),
                effective_limits,
                ready_receipt_sha256,
            },
        )?;
        Ok(response)
    }

    /// Invokes one operation that is allowed to influence active product flow.
    pub fn invoke_active(&self, call: &ProviderCall) -> Result<ProviderReply, FabricError> {
        call.validate()?;
        let registration = self.registration(&call.provider_id)?;
        let dispatch_gate = Arc::clone(&registration.dispatch_gate);
        let _dispatch = dispatch_gate
            .lock()
            .map_err(|_| FabricError::ProviderGatePoisoned)?;
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
        let readiness = Self::require_readiness(&registration, call)?;
        call.validate_request_bytes(readiness.effective_limits.request_bytes)?;
        Self::preflight(&call.control)?;
        let _permit = self.permits.try_acquire()?;
        let reply = registration.provider.invoke(call);
        if let Err(error) = reply.validate(readiness.effective_limits.response_bytes) {
            self.invalidate_matching_readiness(call)?;
            return Err(error.into());
        }
        if let Err(error) = Self::validate_terminal(
            call.operation,
            &call.provider_id,
            &call.operation_id,
            &call.exact_scope,
            &reply.terminal,
            Some(call.expected_state_generation),
            Some(reply.state_generation),
        ) {
            self.invalidate_matching_readiness(call)?;
            return Err(error);
        }
        self.settle_readiness(
            call,
            reply.terminal.committed_effect().state_generation_after(),
        )?;
        Ok(reply)
    }

    /// Delivers one observation to an observer or active provider.
    ///
    /// The return type strips payloads and extensions so observer execution is
    /// structurally unable to contribute context through this route.
    pub fn deliver_observation(&self, call: &ProviderCall) -> Result<ObserverReceipt, FabricError> {
        call.validate()?;
        if call.operation.capability_id() != "observation.accept.v1" {
            return Err(FabricError::OperationNotObservation);
        }
        let registration = self.registration(&call.provider_id)?;
        let dispatch_gate = Arc::clone(&registration.dispatch_gate);
        let _dispatch = dispatch_gate
            .lock()
            .map_err(|_| FabricError::ProviderGatePoisoned)?;
        let registration = self.registration(&call.provider_id)?;
        Self::require_revision(&registration, call.registration_revision)?;
        Self::require_enabled(&call.provider_id, registration.mode)?;
        Self::require_call_capabilities(&registration.descriptor, call)?;
        let readiness = Self::require_readiness(&registration, call)?;
        call.validate_request_bytes(readiness.effective_limits.request_bytes)?;
        Self::preflight(&call.control)?;
        let _permit = self.permits.try_acquire()?;
        let reply = registration.provider.invoke(call);
        if let Err(error) = reply.validate(readiness.effective_limits.response_bytes) {
            self.invalidate_matching_readiness(call)?;
            return Err(error.into());
        }
        if let Err(error) = Self::validate_terminal(
            call.operation,
            &call.provider_id,
            &call.operation_id,
            &call.exact_scope,
            &reply.terminal,
            Some(call.expected_state_generation),
            Some(reply.state_generation),
        ) {
            self.invalidate_matching_readiness(call)?;
            return Err(error);
        }
        self.settle_readiness(
            call,
            reply.terminal.committed_effect().state_generation_after(),
        )?;
        Ok(ObserverReceipt {
            provider_id: call.provider_id.clone(),
            registration_revision: call.registration_revision,
            terminal: reply.terminal,
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

    fn invalidate_readiness(
        &self,
        provider_id: &OwnedProviderId,
        registration_revision: u64,
    ) -> Result<u64, FabricError> {
        let mut registrations = self
            .registrations
            .write()
            .map_err(|_| FabricError::RegistryPoisoned)?;
        let registration = registrations
            .get_mut(provider_id)
            .ok_or_else(|| FabricError::ProviderUnknown(provider_id.as_str().to_owned()))?;
        Self::require_revision(registration, registration_revision)?;
        registration.readiness_epoch = registration
            .readiness_epoch
            .checked_add(1)
            .ok_or(FabricError::InvalidConfig("readiness epoch is exhausted"))?;
        registration.readiness = None;
        Ok(registration.readiness_epoch)
    }

    fn install_readiness(
        &self,
        provider_id: &OwnedProviderId,
        readiness: Readiness,
    ) -> Result<(), FabricError> {
        let mut registrations = self
            .registrations
            .write()
            .map_err(|_| FabricError::RegistryPoisoned)?;
        let registration = registrations
            .get_mut(provider_id)
            .ok_or_else(|| FabricError::ProviderUnknown(provider_id.as_str().to_owned()))?;
        Self::require_revision(registration, readiness.registration_revision)?;
        Self::require_enabled(provider_id, registration.mode)?;
        if registration.readiness_epoch != readiness.epoch
            || registration.descriptor.capabilities != readiness.capabilities
            || readiness.provider_instance_id.trim().is_empty()
            || readiness.state_namespace.trim().is_empty()
        {
            return Err(FabricError::ProviderNotReady(
                provider_id.as_str().to_owned(),
            ));
        }
        registration.accepted_state_generation = readiness.state_generation;
        registration.readiness = Some(readiness);
        Ok(())
    }

    fn require_readiness(
        registration: &Registration,
        call: &ProviderCall,
    ) -> Result<Readiness, FabricError> {
        let readiness = registration
            .readiness
            .as_ref()
            .ok_or_else(|| FabricError::ProviderNotReady(call.provider_id.as_str().to_owned()))?;
        if readiness.epoch != registration.readiness_epoch
            || readiness.registration_revision != registration.revision
            || readiness.capabilities != registration.descriptor.capabilities
            || readiness.state_generation != registration.accepted_state_generation
            || readiness.provider_instance_id.trim().is_empty()
            || readiness.state_namespace.trim().is_empty()
        {
            return Err(FabricError::ProviderNotReady(
                call.provider_id.as_str().to_owned(),
            ));
        }
        if readiness.ready_receipt_sha256 != call.ready_receipt_sha256 {
            return Err(FabricError::ReadyReceiptMismatch);
        }
        if readiness.exact_scope != call.exact_scope {
            return Err(FabricError::ReadyScopeMismatch);
        }
        if readiness.state_generation != call.expected_state_generation {
            return Err(FabricError::ReadyStateGenerationMismatch {
                ready: readiness.state_generation,
                requested: call.expected_state_generation,
            });
        }
        Ok(readiness.clone())
    }

    fn invalidate_matching_readiness(&self, call: &ProviderCall) -> Result<(), FabricError> {
        let mut registrations = self
            .registrations
            .write()
            .map_err(|_| FabricError::RegistryPoisoned)?;
        let registration = registrations
            .get_mut(&call.provider_id)
            .ok_or_else(|| FabricError::ProviderUnknown(call.provider_id.as_str().to_owned()))?;
        if registration.readiness.as_ref().is_some_and(|readiness| {
            readiness.registration_revision == call.registration_revision
                && readiness.ready_receipt_sha256 == call.ready_receipt_sha256
                && readiness.exact_scope == call.exact_scope
        }) {
            registration.readiness = None;
        }
        Ok(())
    }

    fn settle_readiness(
        &self,
        call: &ProviderCall,
        state_generation_after: Option<u64>,
    ) -> Result<(), FabricError> {
        let mut registrations = self
            .registrations
            .write()
            .map_err(|_| FabricError::RegistryPoisoned)?;
        let registration = registrations
            .get_mut(&call.provider_id)
            .ok_or_else(|| FabricError::ProviderUnknown(call.provider_id.as_str().to_owned()))?;
        let matches_call = registration.readiness.as_ref().is_some_and(|readiness| {
            readiness.registration_revision == call.registration_revision
                && readiness.ready_receipt_sha256 == call.ready_receipt_sha256
                && readiness.exact_scope == call.exact_scope
                && readiness.state_generation == call.expected_state_generation
        });
        if matches_call {
            if let Some(state_generation_after) = state_generation_after {
                registration.accepted_state_generation = state_generation_after;
            }
            if state_generation_after != Some(call.expected_state_generation) {
                registration.readiness = None;
            }
        }
        Ok(())
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

    fn validate_handshake_descriptor(
        registered: &ProviderDescriptor,
        accepted_state_generation: u64,
        returned: &ProviderDescriptor,
    ) -> Result<(), FabricError> {
        let immutable_fields_match = registered.provider_id == returned.provider_id
            && registered.implementation_identity_sha256 == returned.implementation_identity_sha256
            && registered.state_schema_version == returned.state_schema_version
            && registered.protocol_major == returned.protocol_major
            && registered.protocol_minor == returned.protocol_minor
            && registered.capabilities == returned.capabilities
            && registered.limits == returned.limits;
        if !immutable_fields_match {
            return Err(FabricError::SuccessfulHandshakeDescriptorMismatch);
        }
        if returned.state_generation < accepted_state_generation {
            return Err(FabricError::SuccessfulHandshakeStateGenerationRegressed {
                accepted: accepted_state_generation,
                returned: returned.state_generation,
            });
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

    fn require_handshake_text<'a>(
        value: Option<&'a str>,
        field: &'static str,
        maximum: Option<usize>,
    ) -> Result<&'a str, FabricError> {
        match value {
            None | Some("") => Err(ApiError::EmptyField(field).into()),
            Some(value) if value.trim() != value || value.chars().any(char::is_control) => {
                Err(ApiError::NonCanonicalTerminalText(field).into())
            }
            Some(value) if maximum.is_some_and(|maximum| value.len() > maximum) => {
                Err(ApiError::TerminalTextTooLong {
                    field,
                    maximum: maximum.unwrap_or(usize::MAX),
                }
                .into())
            }
            Some(value) => Ok(value),
        }
    }

    fn require_sha256(value: &str, field: &'static str) -> Result<(), FabricError> {
        let valid = value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
        if valid {
            Ok(())
        } else {
            Err(ApiError::InvalidSha256(field).into())
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

    fn validate_terminal(
        expected_operation: ProviderOperation,
        provider_id: &OwnedProviderId,
        expected_operation_id: &str,
        exact_scope: &OwnedExactScope,
        terminal: &TerminalRecord,
        expected_state_generation: Option<u64>,
        reported_state_generation: Option<u64>,
    ) -> Result<(), FabricError> {
        if terminal.operation() != expected_operation {
            return Err(FabricError::ResponseOperationKindMismatch {
                expected: expected_operation,
                returned: terminal.operation(),
            });
        }
        if terminal.provider_id() != provider_id {
            return Err(FabricError::ResponseProviderMismatch {
                expected: provider_id.as_str().to_owned(),
                returned: terminal.provider_id().as_str().to_owned(),
            });
        }
        Self::validate_operation_id(expected_operation_id, terminal.operation_id())?;
        let expected_scope_sha256 = exact_scope.exact_scope_sha256();
        if terminal.exact_scope_sha256() != expected_scope_sha256.as_str() {
            return Err(FabricError::ResponseScopeMismatch {
                expected: expected_scope_sha256,
                returned: terminal.exact_scope_sha256().to_owned(),
            });
        }

        let fallback = match terminal.fallback().eligibility() {
            FallbackEligibility::Forbidden => FallbackDirective::forbidden(),
            FallbackEligibility::ExplicitPolicyOnly => {
                let policy = terminal
                    .fallback()
                    .policy()
                    .ok_or(ApiError::EmptyField("fallback_policy"))?
                    .clone();
                let reason = terminal
                    .fallback()
                    .reason()
                    .ok_or(ApiError::EmptyField("fallback_reason"))?;
                FallbackDirective::explicit_policy_only(provider_id, policy, reason)?
            }
        };
        let _validated = TerminalRecord::new(
            terminal.operation(),
            terminal.provider_id().clone(),
            terminal.terminal_code(),
            terminal.committed_effect().clone(),
            fallback,
            terminal.operation_id(),
            terminal.exact_scope_sha256(),
            terminal.diagnostic_id().map(str::to_owned),
        )?;
        if let Some(expected) = expected_state_generation {
            match terminal.committed_effect().state_generation_before() {
                Some(evidence) if evidence != expected => {
                    return Err(FabricError::ResponseStateGenerationMismatch {
                        evidence,
                        reported: expected,
                    });
                }
                None => {
                    return Err(FabricError::ResponseStateGenerationBeforeMissing { expected });
                }
                Some(_) => {}
            }
        }
        Self::validate_state_generation(terminal, reported_state_generation)
    }

    fn validate_state_generation(
        terminal: &TerminalRecord,
        reported_state_generation: Option<u64>,
    ) -> Result<(), FabricError> {
        match (
            terminal.committed_effect().state_generation_after(),
            reported_state_generation,
        ) {
            (Some(evidence), Some(reported)) if evidence != reported => {
                Err(FabricError::ResponseStateGenerationMismatch { evidence, reported })
            }
            (None, Some(reported)) => {
                Err(FabricError::ResponseStateGenerationAfterMissing { reported })
            }
            _ => Ok(()),
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
