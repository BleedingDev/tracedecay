//! Explicit, default-off composition for the product-owned Memory Fabric.
//!
//! This module is compiled only by the `memory-provider-fabric` feature. Merely
//! enabling that feature creates no provider, thread, queue, state, catalog
//! entry, context contribution, or process-global registration. The caller must
//! explicitly supply the existing Native application authority and invoke
//! [`compose_native_memory_fabric`].

use std::sync::Arc;

pub use tracedecay_memory_fabric::{FabricError, MemoryFabric, ProviderMode, ProviderStatus};
use tracedecay_memory_fabric::FabricConfig;
use tracedecay_memory_provider_api::OwnedProviderId;
pub use tracedecay_memory_provider_native::NativeMemoryApplicationPort;
use tracedecay_memory_provider_native::{
    NATIVE_PROVIDER_ID, NativeAdapterError, NativeProvider,
};

/// Enabled Native-provider participation selected by the composition root.
///
/// Disabled operation is represented by leaving `memory-provider-fabric` off
/// and never calling [`compose_native_memory_fabric`]. Therefore this enum has
/// no disabled variant that could accidentally instantiate infrastructure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeMemoryMode {
    /// Admit observations without allowing provider output into active flow.
    Observer,
    /// Permit explicitly routed active calls in addition to observations.
    Active,
}

impl From<NativeMemoryMode> for ProviderMode {
    fn from(value: NativeMemoryMode) -> Self {
        match value {
            NativeMemoryMode::Observer => Self::Observer,
            NativeMemoryMode::Active => Self::Active,
        }
    }
}

/// Finite settings for one explicit Native Memory Fabric mount.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeMemoryFabricConfig {
    /// Maximum providers retained by this fabric instance.
    pub max_registered_providers: usize,
    /// Maximum concurrent calls admitted by this fabric instance.
    pub max_in_flight: usize,
    /// Positive configuration revision accepted by the registration.
    pub registration_revision: u64,
    /// Enabled Native participation mode.
    pub mode: NativeMemoryMode,
}

impl NativeMemoryFabricConfig {
    /// Creates explicit finite settings for one Native mount.
    #[must_use]
    pub const fn new(
        max_registered_providers: usize,
        max_in_flight: usize,
        registration_revision: u64,
        mode: NativeMemoryMode,
    ) -> Self {
        Self {
            max_registered_providers,
            max_in_flight,
            registration_revision,
            mode,
        }
    }
}

/// Failure while explicitly constructing the Native Memory Fabric mount.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum NativeMemoryMountError {
    /// Registration revision zero is never an admitted configuration.
    #[error("registration_revision must be positive")]
    InvalidRegistrationRevision,
    /// The supplied Native authority did not expose the reserved identity.
    #[error(transparent)]
    NativeAdapter(#[from] NativeAdapterError),
    /// Provider-neutral fabric construction or registration failed.
    #[error(transparent)]
    Fabric(#[from] FabricError),
}

/// One explicitly composed Native registration behind a provider-neutral fabric.
///
/// The concrete [`NativeProvider`] stays private to this module. Host routes
/// receive only the provider-neutral [`MemoryFabric`] and stable registration
/// metadata.
pub struct NativeMemoryFabricMount {
    fabric: MemoryFabric,
    registration_revision: u64,
    mode: NativeMemoryMode,
}

/// Constructs the feature-gated fabric and registers the supplied Native port.
///
/// Invalid revision and finite-resource settings fail before the supplied
/// Native port is inspected. The function performs no process-global
/// registration and starts no background work.
pub fn compose_native_memory_fabric(
    port: Arc<dyn NativeMemoryApplicationPort>,
    config: NativeMemoryFabricConfig,
) -> Result<NativeMemoryFabricMount, NativeMemoryMountError> {
    if config.registration_revision == 0 {
        return Err(NativeMemoryMountError::InvalidRegistrationRevision);
    }
    let fabric = MemoryFabric::new(FabricConfig {
        max_registered_providers: config.max_registered_providers,
        max_in_flight: config.max_in_flight,
    })?;
    let provider_id = OwnedProviderId::new(NATIVE_PROVIDER_ID).map_err(FabricError::from)?;
    let provider = Arc::new(NativeProvider::new(port)?);
    fabric.register(
        provider_id,
        config.registration_revision,
        config.mode.into(),
        provider,
    )?;
    Ok(NativeMemoryFabricMount {
        fabric,
        registration_revision: config.registration_revision,
        mode: config.mode,
    })
}

impl NativeMemoryFabricMount {
    /// Returns the provider-neutral fabric containing the Native registration.
    #[must_use]
    pub const fn fabric(&self) -> &MemoryFabric {
        &self.fabric
    }

    /// Returns the reserved stable Native provider identity.
    #[must_use]
    pub const fn provider_id(&self) -> &'static str {
        NATIVE_PROVIDER_ID
    }

    /// Returns the accepted positive registration revision.
    #[must_use]
    pub const fn registration_revision(&self) -> u64 {
        self.registration_revision
    }

    /// Returns the selected enabled participation mode.
    #[must_use]
    pub const fn mode(&self) -> NativeMemoryMode {
        self.mode
    }
}
