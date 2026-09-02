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
//! Product-owned composition for configured memory providers.
//!
//! This crate is the narrow layer allowed to construct concrete adapters. It
//! accepts an existing Native application port explicitly, derives the stable
//! Native identity internally, and registers the adapter in a bounded fabric.
//! The resulting registry exposes only provider-neutral status and call
//! operations; registration and mode mutation remain inside composition.
//! Handshake and active-call replies preserve the complete provider-neutral
//! terminal record. Observation delivery strips provider payloads, opaque
//! extensions, and warning text while retaining the same structured
//! committed-effect and fallback evidence in its observer receipt. Terminal
//! provider and operation identities stay bound to the selected route. The
//! registry never interprets a fallback directive as authority to dispatch
//! another provider.
//! Disabled composition carries no config or port and therefore creates no
//! fabric, provider adapter, storage, background work, or provider
//! registration.
//!
//! A successful handshake can additionally be reduced to
//! [`ProviderReadinessTargetV1`]: a provider-neutral identity built only from
//! the selected provider, its self-reported runtime instance, the
//! product-owned registration revision, and the fabric-validated
//! ready-receipt digest. This keeps the coupling to any root
//! observation-journal or retained-memory target authority one-way — this
//! crate returns the neutral identity and never imports the root's concrete
//! target type — and it cannot be produced from a disabled composition or an
//! unsuccessful handshake.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use tracedecay_memory_fabric::MemoryFabric;

pub mod recall_admission;
pub mod recall_port;
pub use recall_admission::{
    AdmittedRecallCandidate, AdmittedTemporalQuery, DeniedRecallCandidate,
    RECALL_PAYLOAD_CONTRACT_ID, RECALL_QUERY_CAPABILITY_ID, RecallAdmission, RecallAdmissionError,
    RecallAdmissionReport, RecallBudgetsV1, RecallCandidateContent, RecallCandidateV1,
    RecallDenialReason, RecallOutcomeScopeV1, RecallOutcomeV1, RecallRequestParts,
    RecallScopeBindingsV1, RecallScopeIdentityV1, RecallValidityV1, ScopeBinding, ScopeField,
    TemporalState, UnknownValidityPolicy, admit_recall_candidates, admit_recall_reply,
    build_recall_request_payload, decode_recall_outcome, parse_rfc3339_nanos, rfc3339_utc_micros,
};
pub use recall_port::{
    CognitiveRecallAdmittedOutcomeV1, CognitiveRecallPortError, CognitiveRecallPortInputsV1,
    ExactScopeBinding, ExactScopeBindingError, ProjectCognitiveRecallPortV1,
    RecallAdmissionAuditError, RecallAdmissionObserver, RecallRoutePlanError,
};
pub use tracedecay_memory_fabric::{
    ActiveCallPlan, ActiveRoutingPolicy, FabricConfig, FabricError, FallbackDecision,
    FallbackDeclinedReason, FallbackRule, ObserverReceipt, ProviderCapabilityAvailability,
    ProviderMode, ProviderReadiness, ProviderStatus, ReadyRouteTarget, RouteTarget,
    RoutedActiveReply, RoutedProviderIdentity, RoutingError, RoutingPolicyError,
};
// Re-export the narrow provider-neutral surface that product composition needs
// to implement an application port. The product crate deliberately depends on
// this registry crate only; concrete provider crates stay behind this boundary.
pub use tracedecay_memory_provider_api::contract::{
    CommittedEffectState, TemporalMode, TerminalCode,
};
pub use tracedecay_memory_provider_api::{
    ApiError, CancellationToken, CanonicalPayload, CommittedEffectEvidence, FallbackDirective,
    HandshakeRequest, HandshakeRequestParts, HandshakeResponse, OperationControl, OwnedExactScope,
    OwnedProviderId, OwnedVersionedId, PayloadSanitizationReceipt, PayloadSanitizationReceiptParts,
    PinnedFallbackPolicy, ProviderCall, ProviderCallParts, ProviderDescriptor, ProviderLimits,
    ProviderOperation, ProviderReply, SanitizationDisposition, TerminalRecord, WithheldReason,
};
pub use tracedecay_memory_provider_native::{
    NATIVE_FACT_PROMOTION_OBSERVATION_KIND, NATIVE_FACT_PROMOTION_PAYLOAD_CONTRACT_ID,
    NATIVE_PROVIDER_ID, NATIVE_RECALL_SCOPE_BINDINGS, NativeAdapterError,
    NativeMemoryApplicationPort, NativeObservation, NativeProvider, OBSERVATION_CONTRACT_ID,
};

/// A non-disabled Native participation mode.
///
/// Keeping `Disabled` out of this type prevents an enabled adapter from being
/// constructed only to receive a disabled fabric registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnabledProviderMode {
    /// Receive admitted observations without contributing active output.
    Observer,
    /// Receive admitted observations and explicitly routed active calls.
    Active,
}

impl EnabledProviderMode {
    fn fabric_mode(self) -> ProviderMode {
        match self {
            Self::Observer => ProviderMode::Observer,
            Self::Active => ProviderMode::Active,
        }
    }
}

/// Explicit Native provider selection for one product composition.
pub enum NativeProviderActivation {
    /// Do not construct any provider or fabric infrastructure.
    Disabled,
    /// Construct Native from the injected application port and register it.
    Enabled {
        /// Finite fabric limits used only by enabled composition.
        fabric_config: FabricConfig,
        /// Existing TraceDecay Native application authority.
        port: Arc<dyn NativeMemoryApplicationPort>,
        /// Positive product-owned registration revision.
        registration_revision: u64,
        /// Enabled observer or active participation.
        mode: EnabledProviderMode,
    },
}

/// Explicit result of configured product provider composition.
pub enum ProjectMemoryProviderComposition {
    /// Provider infrastructure is absent.
    Disabled,
    /// Provider infrastructure was explicitly enabled and constructed.
    Enabled(ProjectMemoryProviderRegistry),
}

impl ProjectMemoryProviderComposition {
    /// Applies the explicit activation without constructing disabled
    /// infrastructure.
    pub fn compose(native: NativeProviderActivation) -> Result<Self, RegistryError> {
        match native {
            NativeProviderActivation::Disabled => Ok(Self::Disabled),
            NativeProviderActivation::Enabled {
                fabric_config,
                port,
                registration_revision,
                mode,
            } => Ok(Self::Enabled(
                ProjectMemoryProviderRegistry::compose_native(
                    fabric_config,
                    port,
                    registration_revision,
                    mode,
                )?,
            )),
        }
    }

    /// Borrows the enabled registry, or returns `None` when disabled.
    #[must_use]
    pub fn registry(&self) -> Option<&ProjectMemoryProviderRegistry> {
        match self {
            Self::Disabled => None,
            Self::Enabled(registry) => Some(registry),
        }
    }
}

/// Provider-neutral identity produced by one validated readiness handshake.
///
/// Every field is copied unchanged from values the fabric itself already
/// requires to be present and mutually consistent before it returns a
/// successful [`HandshakeResponse`]: the selected provider identity bound to
/// the accepted terminal, the provider-reported runtime-instance identity,
/// the product-owned registration revision the handshake was admitted
/// under, and the fabric-validated ready-receipt digest. No field is
/// fabricated, defaulted, or read from configuration or test support — a
/// value can only be constructed by
/// [`ProjectMemoryProviderRegistry::readiness_target`] from a real,
/// successful handshake.
///
/// This is **readiness evidence, not a delivery address**. The durable
/// observation journal owns its own `ProviderTargetV1`, whose fields are
/// public because a persisted row has to be reconstructed on read; naming
/// this type after that one would put two differently-shaped structs with
/// one name on the composition root's import list. The root is the only
/// place both can exist. A production observation mount must map this value
/// into the journal's target: `provider_id`, `provider_instance_id`, and
/// `registration_revision` carry over unchanged, and
/// [`Self::ready_receipt_sha256`] is the bare lowercase 64-hex digest the
/// journal stores as `ready_receipt_digest`. Deriving the journal target any
/// other way would let a target exist without a successful handshake behind
/// it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderReadinessTargetV1 {
    provider_id: OwnedProviderId,
    provider_instance_id: String,
    registration_revision: u64,
    ready_receipt_sha256: String,
}

impl ProviderReadinessTargetV1 {
    /// Returns the selected provider identity the handshake was bound to.
    #[must_use]
    pub fn provider_id(&self) -> &OwnedProviderId {
        &self.provider_id
    }

    /// Returns the provider-reported runtime-instance identity.
    #[must_use]
    pub fn provider_instance_id(&self) -> &str {
        &self.provider_instance_id
    }

    /// Returns the product-owned registration revision this target was
    /// derived under.
    #[must_use]
    pub const fn registration_revision(&self) -> u64 {
        self.registration_revision
    }

    /// Returns the fabric-validated ready-receipt digest bound to this
    /// target.
    #[must_use]
    pub fn ready_receipt_sha256(&self) -> &str {
        &self.ready_receipt_sha256
    }
}

/// Failure deriving a [`ProviderReadinessTargetV1`] from a readiness handshake.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadinessTargetError {
    /// The fabric rejected the handshake before any terminal existed.
    Fabric(FabricError),
    /// The handshake terminal was not successful, so no readiness target
    /// exists to derive.
    HandshakeNotReady,
}

impl fmt::Display for ReadinessTargetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fabric(error) => write!(formatter, "readiness handshake failed: {error}"),
            Self::HandshakeNotReady => {
                formatter.write_str("handshake did not reach a successful terminal")
            }
        }
    }
}

impl Error for ReadinessTargetError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Fabric(error) => Some(error),
            Self::HandshakeNotReady => None,
        }
    }
}

impl From<FabricError> for ReadinessTargetError {
    fn from(value: FabricError) -> Self {
        Self::Fabric(value)
    }
}

/// Failure while composing or registering product-owned providers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegistryError {
    /// The product-owned stable provider identity was invalid.
    Api(ApiError),
    /// The injected Native application port could not construct an adapter.
    NativeAdapter(NativeAdapterError),
    /// The bounded fabric rejected construction or registration.
    Fabric(FabricError),
    /// A provider's declared recall scope bindings fall outside the closed
    /// contract vocabulary, so the host refuses to record any authorization.
    RecallScopeBindings(RecallAdmissionError),
}

impl fmt::Display for RegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Api(error) => write!(formatter, "provider registry API error: {error}"),
            Self::NativeAdapter(error) => {
                write!(formatter, "Native provider construction failed: {error}")
            }
            Self::Fabric(error) => write!(formatter, "memory fabric error: {error}"),
            Self::RecallScopeBindings(error) => {
                write!(formatter, "provider recall scope bindings invalid: {error}")
            }
        }
    }
}

impl Error for RegistryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Api(error) => Some(error),
            Self::NativeAdapter(error) => Some(error),
            Self::Fabric(error) => Some(error),
            Self::RecallScopeBindings(error) => Some(error),
        }
    }
}

impl From<ApiError> for RegistryError {
    fn from(value: ApiError) -> Self {
        Self::Api(value)
    }
}

impl From<NativeAdapterError> for RegistryError {
    fn from(value: NativeAdapterError) -> Self {
        Self::NativeAdapter(value)
    }
}

impl From<FabricError> for RegistryError {
    fn from(value: FabricError) -> Self {
        Self::Fabric(value)
    }
}

/// Retained product-owned provider composition.
///
/// Values can only be produced through
/// [`ProjectMemoryProviderComposition::compose`]. Concrete adapter
/// registration and the mutable fabric surface are intentionally private.
///
/// ```compile_fail,E0624
/// use tracedecay_memory_provider_registry::ProjectMemoryProviderRegistry;
///
/// let _private_constructor = ProjectMemoryProviderRegistry::compose_native;
/// ```
///
/// ```compile_fail,E0624
/// use tracedecay_memory_provider_registry::ProjectMemoryProviderRegistry;
///
/// let _private_registration = ProjectMemoryProviderRegistry::register_native;
/// ```
///
/// ```compile_fail,E0599
/// use tracedecay_memory_provider_registry::ProjectMemoryProviderRegistry;
///
/// fn cannot_escape_fabric(registry: &ProjectMemoryProviderRegistry) {
///     let _ = registry.fabric();
/// }
/// ```
pub struct ProjectMemoryProviderRegistry {
    fabric: Arc<MemoryFabric>,
    /// Recall scope bindings the host recorded per provider at registration,
    /// from the provider's declared `recall_scope_bindings` manifest attribute.
    /// Admission reads this record through the admitted call; a provider
    /// reply can never widen it.
    recall_scope_bindings: BTreeMap<OwnedProviderId, RecallScopeBindingsV1>,
}

impl ProjectMemoryProviderRegistry {
    fn compose_native(
        fabric_config: FabricConfig,
        port: Arc<dyn NativeMemoryApplicationPort>,
        registration_revision: u64,
        mode: EnabledProviderMode,
    ) -> Result<Self, RegistryError> {
        let mut registry = Self {
            fabric: Arc::new(MemoryFabric::new(fabric_config)?),
            recall_scope_bindings: BTreeMap::new(),
        };
        registry.register_native(port, registration_revision, mode)?;
        Ok(registry)
    }

    /// Returns the recall scope bindings the host recorded for `provider_id`
    /// at registration, or `None` when the provider is not registered here.
    ///
    /// This is the only authorization source recall admission accepts.
    #[must_use]
    pub fn recall_scope_bindings(
        &self,
        provider_id: &OwnedProviderId,
    ) -> Option<&RecallScopeBindingsV1> {
        self.recall_scope_bindings.get(provider_id)
    }

    /// Returns deterministic status for every configured provider in
    /// canonical provider-ID order.
    pub fn statuses(&self) -> Result<Vec<ProviderStatus>, FabricError> {
        self.fabric.statuses()
    }

    /// Performs a bounded provider-neutral readiness handshake, preserving
    /// its complete structured terminal evidence.
    pub fn handshake(&self, request: &HandshakeRequest) -> Result<HandshakeResponse, FabricError> {
        self.fabric.handshake(request)
    }

    /// Performs a bounded provider-neutral readiness handshake and, only on
    /// a successful terminal, derives the [`ProviderReadinessTargetV1`] identity the
    /// root composition can map into its own target.
    ///
    /// This method never activates readiness for disabled composition: a
    /// [`ProjectMemoryProviderRegistry`] value exists only inside
    /// [`ProjectMemoryProviderComposition::Enabled`], so there is no
    /// receiver to call it on when composition chose
    /// [`NativeProviderActivation::Disabled`]. It also never weakens an
    /// active-mode safety gate — the derived target reuses exactly the
    /// fields the fabric already validated as present and mutually
    /// consistent before returning `Ok`; a rejected or unsuccessful
    /// handshake yields [`ReadinessTargetError`] and no target.
    pub fn readiness_target(
        &self,
        request: &HandshakeRequest,
    ) -> Result<ProviderReadinessTargetV1, ReadinessTargetError> {
        let response = self.fabric.handshake(request)?;
        if response.terminal.terminal_code() != TerminalCode::Success {
            return Err(ReadinessTargetError::HandshakeNotReady);
        }
        let provider_instance_id = response
            .provider_instance_id
            .ok_or(ReadinessTargetError::HandshakeNotReady)?;
        let ready_receipt_sha256 = response
            .ready_receipt_sha256
            .ok_or(ReadinessTargetError::HandshakeNotReady)?;
        Ok(ProviderReadinessTargetV1 {
            provider_id: response.terminal.provider_id().clone(),
            provider_instance_id,
            registration_revision: request.registration_revision,
            ready_receipt_sha256,
        })
    }

    /// Invokes one operation admitted to influence active product flow.
    ///
    /// The provider-neutral reply, including committed-effect and fallback
    /// evidence and provider/operation identity, is returned unchanged after
    /// fabric validation.
    pub fn invoke_active(&self, call: &ProviderCall) -> Result<ProviderReply, FabricError> {
        self.fabric.invoke_active(call)
    }

    /// Routes one active call under an explicit host routing policy.
    ///
    /// The configured provider is refused before any contact unless it is
    /// registered under the pinned revision in active mode with the routed
    /// capability; observer and disabled registrations can never answer. A
    /// fallback directive on the reply is honoured only when the host rule
    /// pins the identical policy and the target is itself a registered active
    /// provider that passes a fresh handshake — otherwise the original
    /// provider's reply is returned with a typed declined reason. Every
    /// returned reply names the provider that produced it.
    pub fn route_active<P: ActiveCallPlan>(
        &self,
        policy: &ActiveRoutingPolicy,
        capability_id: &str,
        plan: &P,
    ) -> Result<RoutedActiveReply, RoutingError<P::Error>> {
        self.fabric.route_active(policy, capability_id, plan)
    }

    /// Delivers an observation while structurally stripping provider output.
    ///
    /// The observer receipt retains the complete validated terminal record,
    /// including its provider and observation-operation binding; it cannot
    /// carry a provider result payload, opaque extensions, or warning text.
    pub fn deliver_observation(&self, call: &ProviderCall) -> Result<ObserverReceipt, FabricError> {
        self.fabric.deliver_observation(call)
    }

    fn register_native(
        &mut self,
        port: Arc<dyn NativeMemoryApplicationPort>,
        registration_revision: u64,
        mode: EnabledProviderMode,
    ) -> Result<(), RegistryError> {
        let provider_id = OwnedProviderId::new(NATIVE_PROVIDER_ID)?;
        let bindings =
            RecallScopeBindingsV1::from_wire(NATIVE_RECALL_SCOPE_BINDINGS.iter().copied())
                .map_err(RegistryError::RecallScopeBindings)?;
        let provider = Arc::new(NativeProvider::new(port)?);
        self.fabric.register(
            provider_id.clone(),
            registration_revision,
            mode.fabric_mode(),
            provider,
        )?;
        self.recall_scope_bindings.insert(provider_id, bindings);
        Ok(())
    }
}
