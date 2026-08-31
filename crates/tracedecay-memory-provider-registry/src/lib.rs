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

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use tracedecay_memory_fabric::MemoryFabric;
pub use tracedecay_memory_fabric::{
    FabricConfig, FabricError, ObserverReceipt, ProviderMode, ProviderStatus,
};
// Re-export the narrow provider-neutral surface that product composition needs
// to implement an application port. The product crate deliberately depends on
// this registry crate only; concrete provider crates stay behind this boundary.
pub use tracedecay_memory_provider_api::contract::{CommittedEffectState, TerminalCode};
pub use tracedecay_memory_provider_api::{
    ApiError, CancellationToken, CanonicalPayload, CommittedEffectEvidence, FallbackDirective,
    HandshakeRequest, HandshakeRequestParts, HandshakeResponse, OperationControl, OwnedExactScope,
    OwnedProviderId, OwnedVersionedId, ProviderCall, ProviderCallParts, ProviderDescriptor,
    ProviderLimits, ProviderOperation, ProviderReply, TerminalRecord,
};
pub use tracedecay_memory_provider_native::{
    NATIVE_FACT_PROMOTION_OBSERVATION_KIND, NATIVE_FACT_PROMOTION_PAYLOAD_CONTRACT_ID,
    NATIVE_PROVIDER_ID, NativeAdapterError, NativeMemoryApplicationPort, NativeObservation,
    NativeProvider, OBSERVATION_CONTRACT_ID,
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

/// Failure while composing or registering product-owned providers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegistryError {
    /// The product-owned stable provider identity was invalid.
    Api(ApiError),
    /// The injected Native application port could not construct an adapter.
    NativeAdapter(NativeAdapterError),
    /// The bounded fabric rejected construction or registration.
    Fabric(FabricError),
}

impl fmt::Display for RegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Api(error) => write!(formatter, "provider registry API error: {error}"),
            Self::NativeAdapter(error) => {
                write!(formatter, "Native provider construction failed: {error}")
            }
            Self::Fabric(error) => write!(formatter, "memory fabric error: {error}"),
        }
    }
}

impl Error for RegistryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Api(error) => Some(error),
            Self::NativeAdapter(error) => Some(error),
            Self::Fabric(error) => Some(error),
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
}

impl ProjectMemoryProviderRegistry {
    fn compose_native(
        fabric_config: FabricConfig,
        port: Arc<dyn NativeMemoryApplicationPort>,
        registration_revision: u64,
        mode: EnabledProviderMode,
    ) -> Result<Self, RegistryError> {
        let registry = Self {
            fabric: Arc::new(MemoryFabric::new(fabric_config)?),
        };
        registry.register_native(port, registration_revision, mode)?;
        Ok(registry)
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

    /// Invokes one operation admitted to influence active product flow.
    ///
    /// The provider-neutral reply, including committed-effect and fallback
    /// evidence and provider/operation identity, is returned unchanged after
    /// fabric validation.
    pub fn invoke_active(&self, call: &ProviderCall) -> Result<ProviderReply, FabricError> {
        self.fabric.invoke_active(call)
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
        &self,
        port: Arc<dyn NativeMemoryApplicationPort>,
        registration_revision: u64,
        mode: EnabledProviderMode,
    ) -> Result<(), RegistryError> {
        let provider_id = OwnedProviderId::new(NATIVE_PROVIDER_ID)?;
        let provider = Arc::new(NativeProvider::new(port)?);
        self.fabric.register(
            provider_id,
            registration_revision,
            mode.fabric_mode(),
            provider,
        )?;
        Ok(())
    }
}
