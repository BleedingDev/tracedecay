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
//! Narrow, default-off composition for TraceDecay memory providers.
//!
//! The registry owns no provider behavior. It constructs one bounded
//! [`MemoryFabric`], wraps the existing Native application port in the Native
//! adapter, and registers that provider at one explicit revision and mode.
//! Concrete adapter types stay inside this crate and never reach public
//! transports or host-specific integration crates.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

pub use tracedecay_memory_fabric::{FabricConfig, MemoryFabric, ProviderMode, ProviderStatus};
use tracedecay_memory_fabric::FabricError;
use tracedecay_memory_provider_api::{ApiError, MemoryProvider, OwnedProviderId};
pub use tracedecay_memory_provider_native::NativeMemoryApplicationPort;
use tracedecay_memory_provider_native::{
    NATIVE_PROVIDER_ID, NativeAdapterError, NativeProvider,
};

/// Construction failure before a composed Native fabric can be retained.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompositionError {
    /// The requested registration revision was zero.
    InvalidRegistrationRevision,
    /// A provider-neutral identity was malformed.
    Api(ApiError),
    /// The bounded fabric rejected construction or registration.
    Fabric(FabricError),
    /// The supplied Native application port declared an incompatible identity.
    Native(NativeAdapterError),
}

impl fmt::Display for CompositionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRegistrationRevision => {
                formatter.write_str("Native registration revision must be positive")
            }
            Self::Api(error) => write!(formatter, "provider API error: {error}"),
            Self::Fabric(error) => write!(formatter, "memory fabric error: {error}"),
            Self::Native(error) => write!(formatter, "Native adapter error: {error}"),
        }
    }
}

impl Error for CompositionError {}

impl From<ApiError> for CompositionError {
    fn from(value: ApiError) -> Self {
        Self::Api(value)
    }
}

impl From<FabricError> for CompositionError {
    fn from(value: FabricError) -> Self {
        Self::Fabric(value)
    }
}

impl From<NativeAdapterError> for CompositionError {
    fn from(value: NativeAdapterError) -> Self {
        Self::Native(value)
    }
}

/// Explicit finite settings for one Native provider registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeCompositionConfig {
    /// Bounded provider registry and concurrent-call limits.
    pub fabric: FabricConfig,
    /// Positive revision used by every request routed to this registration.
    pub registration_revision: u64,
    /// Disabled, observer-only, or active participation selected by TraceDecay.
    pub mode: ProviderMode,
}

impl NativeCompositionConfig {
    /// Creates explicit settings without consulting environment or global state.
    pub fn new(
        fabric: FabricConfig,
        registration_revision: u64,
        mode: ProviderMode,
    ) -> Result<Self, CompositionError> {
        if registration_revision == 0 {
            return Err(CompositionError::InvalidRegistrationRevision);
        }
        Ok(Self {
            fabric,
            registration_revision,
            mode,
        })
    }
}

/// Retained result of the single Native memory composition path.
pub struct NativeMemoryComposition {
    fabric: MemoryFabric,
    native_provider_id: OwnedProviderId,
    registration_revision: u64,
}

impl NativeMemoryComposition {
    /// Returns the bounded provider-neutral fabric.
    #[must_use]
    pub const fn fabric(&self) -> &MemoryFabric {
        &self.fabric
    }

    /// Returns the stable Native provider identity registered in the fabric.
    #[must_use]
    pub const fn native_provider_id(&self) -> &OwnedProviderId {
        &self.native_provider_id
    }

    /// Returns the accepted Native registration revision.
    #[must_use]
    pub const fn registration_revision(&self) -> u64 {
        self.registration_revision
    }
}

/// Constructs the bounded fabric and registers exactly one Native adapter.
///
/// The function starts no worker, opens no database, reads no ambient
/// configuration, and performs no provider handshake. The existing Native
/// application owner remains authoritative behind `port`.
pub fn compose_native_memory(
    port: Arc<dyn NativeMemoryApplicationPort>,
    config: NativeCompositionConfig,
) -> Result<NativeMemoryComposition, CompositionError> {
    let native = NativeProvider::new(port)?;
    let provider_id = OwnedProviderId::new(NATIVE_PROVIDER_ID)?;
    let fabric = MemoryFabric::new(config.fabric)?;
    let provider: Arc<dyn MemoryProvider> = Arc::new(native);
    fabric.register(
        provider_id.clone(),
        config.registration_revision,
        config.mode,
        provider,
    )?;
    Ok(NativeMemoryComposition {
        fabric,
        native_provider_id: provider_id,
        registration_revision: config.registration_revision,
    })
}
