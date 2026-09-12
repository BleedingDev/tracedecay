//! Trusted host metadata for the canonical Native context route.
//!
//! This module deliberately contains no provider payload or descriptor. The
//! host/composition boundary creates a marker after the canonical
//! `memory_matches` contribution has already run and binds it to the selected
//! registration, exact scope, and canonical request. A provider reply cannot
//! create or extend that binding by naming itself or by returning a payload.

use crate::{ApiError, OwnedExactScope, OwnedProviderId, require_sha256};

/// Stable identity of the host route that delivers the canonical Native
/// contribution to model-visible context.
pub const NATIVE_CONTEXT_DELIVERY_ROUTE_ID: &str = "tracedecay.memory.native.context-delivery.v1";

/// Stable operation identity of the canonical Native context contribution.
pub const NATIVE_CONTEXT_DELIVERY_OPERATION_ID: &str = "memory_matches";

/// Host/composition evidence that the canonical Native context contribution
/// was delivered exactly once for one selected registration and request.
///
/// The marker is runtime metadata rather than a wire or provider result type.
/// Its private fields prevent callers from changing a binding after
/// construction. The host/composition owner must construct it only after the
/// original canonical `memory_matches` route has completed, and must retain
/// one marker per canonical request. Providers cannot self-assert delivery:
/// this type accepts no provider descriptor, provider payload, display name, or
/// provider-local receipt, and consumers must call [`Self::verify_for`] with
/// the trusted host/composition registration decision and both host-computed
/// digests before bypassing an additional Native query. The public constructor
/// and private fields are data assembly only; they are not authorization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeContextDeliveryMarker {
    selected_provider_id: OwnedProviderId,
    registration_revision: u64,
    exact_scope_sha256: String,
    canonical_request_sha256: String,
    canonical_contribution_sha256: String,
}

impl NativeContextDeliveryMarker {
    /// Creates host/composition evidence from an explicit selected
    /// registration and the canonical contribution that was delivered.
    ///
    /// `canonical_request_sha256` is the digest of the canonical request bytes
    /// used by the context route. `canonical_contribution_sha256` is the
    /// digest of the canonical contribution bytes delivered by that route.
    /// Both values are host-computed evidence; neither is read from a provider
    /// descriptor or response.
    pub fn from_host_registration(
        selected_provider_id: OwnedProviderId,
        registration_revision: u64,
        exact_scope: &OwnedExactScope,
        canonical_request_sha256: impl Into<String>,
        canonical_contribution_sha256: impl Into<String>,
    ) -> Result<Self, ApiError> {
        exact_scope.validate()?;
        let marker = Self {
            selected_provider_id,
            registration_revision,
            exact_scope_sha256: exact_scope.exact_scope_sha256(),
            canonical_request_sha256: canonical_request_sha256.into(),
            canonical_contribution_sha256: canonical_contribution_sha256.into(),
        };
        marker.validate()?;
        Ok(marker)
    }

    /// Revalidates marker syntax and its fixed canonical route identity.
    pub fn validate(&self) -> Result<(), ApiError> {
        if self.registration_revision == 0 {
            return Err(ApiError::InvalidRegistrationRevision);
        }
        require_sha256(
            &self.exact_scope_sha256,
            "native_context.exact_scope_sha256",
        )?;
        require_sha256(
            &self.canonical_request_sha256,
            "native_context.canonical_request_sha256",
        )?;
        require_sha256(
            &self.canonical_contribution_sha256,
            "native_context.canonical_contribution_sha256",
        )?;
        Ok(())
    }

    /// Verifies that this marker belongs to the trusted host-selected
    /// registration, exact scope, canonical request, and canonical
    /// contribution currently being evaluated.
    ///
    /// The comparison is against caller-supplied identity and digests. It
    /// never infers Native behavior from a provider name or trusts a provider
    /// descriptor or response field.
    pub fn verify_for(
        &self,
        selected_provider_id: &OwnedProviderId,
        registration_revision: u64,
        exact_scope: &OwnedExactScope,
        canonical_request_sha256: &str,
        canonical_contribution_sha256: &str,
    ) -> Result<(), ApiError> {
        self.validate()?;
        exact_scope.validate()?;
        if &self.selected_provider_id != selected_provider_id {
            return Err(ApiError::NativeContextDeliveryBindingMismatch(
                "selected_provider_id",
            ));
        }
        if self.registration_revision != registration_revision {
            return Err(ApiError::NativeContextDeliveryBindingMismatch(
                "registration_revision",
            ));
        }
        if self.exact_scope_sha256 != exact_scope.exact_scope_sha256() {
            return Err(ApiError::NativeContextDeliveryBindingMismatch(
                "exact_scope_sha256",
            ));
        }
        if self.canonical_request_sha256 != canonical_request_sha256 {
            return Err(ApiError::NativeContextDeliveryBindingMismatch(
                "canonical_request_sha256",
            ));
        }
        require_sha256(
            canonical_request_sha256,
            "native_context.canonical_request_sha256",
        )?;
        if self.canonical_contribution_sha256 != canonical_contribution_sha256 {
            return Err(ApiError::NativeContextDeliveryBindingMismatch(
                "canonical_contribution_sha256",
            ));
        }
        require_sha256(
            canonical_contribution_sha256,
            "native_context.canonical_contribution_sha256",
        )?;
        Ok(())
    }

    /// Returns the fixed route identity of this marker.
    #[must_use]
    pub const fn route_id(&self) -> &'static str {
        NATIVE_CONTEXT_DELIVERY_ROUTE_ID
    }

    /// Returns the fixed operation identity of this marker.
    #[must_use]
    pub const fn operation_id(&self) -> &'static str {
        NATIVE_CONTEXT_DELIVERY_OPERATION_ID
    }

    /// Returns the explicitly selected provider identity.
    #[must_use]
    pub fn selected_provider_id(&self) -> &OwnedProviderId {
        &self.selected_provider_id
    }

    /// Returns the accepted provider registration revision.
    #[must_use]
    pub const fn registration_revision(&self) -> u64 {
        self.registration_revision
    }

    /// Returns the exact admitted scope digest.
    #[must_use]
    pub fn exact_scope_sha256(&self) -> &str {
        &self.exact_scope_sha256
    }

    /// Returns the host-computed canonical request digest.
    #[must_use]
    pub fn canonical_request_sha256(&self) -> &str {
        &self.canonical_request_sha256
    }

    /// Returns the host-computed canonical contribution digest.
    #[must_use]
    pub fn canonical_contribution_sha256(&self) -> &str {
        &self.canonical_contribution_sha256
    }
}
