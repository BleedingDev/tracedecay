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
use tracedecay_memory_provider_api::MemoryProvider;

pub mod recall_admission;
pub mod recall_context_pack;
pub mod recall_explain_trace;
pub mod recall_normalization;
pub mod recall_port;
pub mod recall_provenance_hydration;
pub mod recall_selection;
pub mod state_capability;
pub mod supervised_readiness;
pub mod supervisor;
pub use recall_admission::{
    AdmittedRecallCandidate, AdmittedTemporalQuery, DeniedRecallCandidate,
    RECALL_PAYLOAD_CONTRACT_ID, RECALL_QUERY_CAPABILITY_ID, RecallAdmission, RecallAdmissionError,
    RecallAdmissionReport, RecallBudgetsV1, RecallCandidateContent, RecallCandidateV1,
    RecallDenialReason, RecallOutcomeScopeV1, RecallOutcomeV1, RecallRequestParts,
    RecallScopeBindingsV1, RecallScopeIdentityV1, RecallValidityV1, ScopeBinding, ScopeField,
    TemporalState, UnknownValidityPolicy, admit_recall_candidates, admit_recall_reply,
    build_recall_request_payload, decode_recall_outcome, parse_rfc3339_nanos, rfc3339_utc_micros,
};
pub use recall_context_pack::{
    ADVISORY_CONTEXT_PACK_JSON_KEY, AdvisoryLaneV1, CANONICAL_CONTEXT_TOKENIZER_ID,
    CANONICAL_CONTEXT_TOKENIZER_REVISION, ContextItemProvenanceV1, ContextPackError,
    ContextPackItemV1, ContextPackPolicyError, ContextPackPolicyV1, ContextPackReceiptV1,
    ContextPackRenderFormV1, ContextPackSectionV1, ContextPackV1, ContextSectionKind,
    ContextTokenizer, ExcludedProviderItemV1, HOST_CONTEXT_PACK_POLICY_ID,
    HOST_CONTEXT_PACK_POLICY_REVISION, HostContextItemV1, NATIVE_FACTS_HOST_AUTHORITY,
    O200kBaseContextTokenizer, ProviderContextItemV1, ProviderContributionV1,
    ProviderExclusionReason, ProviderItemProvenanceV1, ProviderMetadataFieldV1,
    compile_context_pack, uncontained_item_identity,
};
pub use recall_explain_trace::{
    ContainedExplanationRedactorV1, EXPLAIN_TRACE_BOUNDARY_LABEL, MAX_EXPLAIN_EXPLANATION_CHARS,
    RecallExplainHostDecisionV1, RecallExplainHostWithholdingV1, RecallExplainItemV1,
    RecallExplainProviderExplanationV1, RecallExplainStageV1, RecallExplainTokenSummaryV1,
    RecallExplainTraceError, RecallExplainTraceInputsV1, RecallExplainTraceV1,
    RecallExplanationRedactorV1, build_recall_explain_trace, explanation_source_sha256,
    is_contained_explanation,
};
pub use recall_normalization::{
    HOST_NORMALIZATION_POLICY_ID, HOST_NORMALIZATION_POLICY_REVISION, HostNormalizedScoreV1,
    MAX_SCORE_COMPONENTS, NativeScoreDefect, NativeScoreV1, NormalizationUnavailableReason,
    NormalizedRecallCandidateV1, RecallNormalizationError, RecallNormalizationPolicyV1,
    RecallNormalizationV1, RecallRelevanceV1, ScoreCalibrationEvidence, ScoreCalibrationState,
    ScoreDirection, ValidatedNativeScoreV1, normalize_admitted_candidates, normalize_native_score,
    validate_native_score,
};
pub use recall_port::{
    BoundCognitiveRecallPortV1, CognitiveRecallAdmittedOutcomeV1, CognitiveRecallPortError,
    CognitiveRecallPortInputsV1, ExactScopeBinding, ExactScopeBindingError,
    ProjectCognitiveRecallPortV1, RecallAdmissionAuditError, RecallAdmissionObserver,
    RecallRoutePlanError,
};
pub use recall_provenance_hydration::{
    DEFAULT_PROVENANCE_HYDRATION_MAX_ATTEMPTS, HostCanonicalRecordStore, HostEvidenceControlV1,
    HostEvidenceLookupErrorV1, HostEvidenceRefV1, HostEvidenceScopeError, HostEvidenceScopeV1,
    HostProvenanceAuthority, HostProviderLocalAttestationStore, HostSessionEvidenceStore,
    HostSourceEvidenceStore, MountedHostProvenanceAuthorityV1, ProvenanceHydrationDecisionV1,
    ProvenanceHydrationDegradationV1, ProvenanceHydrationError, ProvenanceHydrationOutcome,
    ProvenanceHydrationPassV1, ProvenanceHydrationPolicyError, ProvenanceHydrationPolicyV1,
};
pub use recall_selection::{
    BudgetExcludedCandidateV1, BudgetExclusionReason, DEFAULT_DIVERSITY_SIMILARITY_THRESHOLD_PPM,
    DEFAULT_DUPLICATE_SIMILARITY_THRESHOLD_PPM, DeduplicatedCandidateV1,
    DiversityExcludedCandidateV1, DuplicateReason, HOST_SELECTION_POLICY_ID,
    HOST_SELECTION_POLICY_REVISION, NEGATION_MARKERS, RecallSelectionError,
    RecallSelectionPolicyError, RecallSelectionPolicyV1, RecallSelectionV1, SIMILARITY_UNIT,
    select_recall_candidates,
};
pub use state_capability::{
    ProviderStateAccessError, ProviderStateAuthorityError, ProviderStateAuthorityV1,
    ProviderStateCapabilityV1,
};
pub use supervised_readiness::{
    BoundedCallRefusalV1, BoundedProviderCallV1, CompositionLifecycleAdapterV1,
    CompositionLifecycleError, ProviderHandshakeWorkV1, QuarantinedScopeV1,
    SupervisedProviderReadinessV1, SupervisedReadinessConfigV1, SupervisedReadinessError,
    SupervisedScopeReadinessV1,
};
pub use supervisor::{
    AdapterOperationV1, DegradationCauseV1, DegradationKindV1, DegradationRecordV1,
    PredecessorStateV1, ProviderAvailabilityV1, ProviderLifecycleAdapterV1, ProviderSupervisorV1,
    QuarantinePolicyV1, QuarantineRecordV1, QuarantineReleaseError, ReadinessDefectV1,
    ReadinessEvidenceV1, ReproveOutcomeV1, RestartBudgetV1, ScopeFieldV1, ShutdownBudgetV1,
    ShutdownReportV1, SupervisedScopeV1, SupervisorConfigError, SupervisorOutcomeV1,
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
    HandshakeRequest, HandshakeRequestParts, HandshakeResponse, MemoryProvider as MemoryProviderV1,
    OperationControl, OwnedExactScope, OwnedProviderId, OwnedVersionedId,
    PayloadSanitizationReceipt, PayloadSanitizationReceiptParts, PinnedFallbackPolicy,
    ProviderCall, ProviderCallParts, ProviderDescriptor, ProviderLimits, ProviderOperation,
    ProviderReply, SanitizationDisposition, TerminalRecord, WithheldReason,
};
pub use tracedecay_memory_provider_native::{
    NATIVE_FACT_PROMOTION_OBSERVATION_KIND, NATIVE_FACT_PROMOTION_PAYLOAD_CONTRACT_ID,
    NATIVE_PROVIDER_ID, NATIVE_RECALL_SCOPE_BINDINGS, NATIVE_STAGED_SESSION_OBSERVATION_KIND,
    NATIVE_STAGED_SESSION_PAYLOAD_CONTRACT_ID, NativeAdapterError, NativeMemoryApplicationPort,
    NativeObservation, NativeObservationEnvelope, NativeProvider, OBSERVATION_CONTRACT_ID,
};

/// The adapter this registry mounts for a configured active-provider name.
///
/// This is a *typed kind*, not a name. Provider-identity recognition lives
/// here, in the registry/adapter layer, and nowhere else: a composition root
/// that compared a configured provider name against a hard-coded identity
/// would have to be edited every time an adapter is added or renamed, which
/// is exactly the provider-name branching the provider boundary exists to
/// prevent. Callers ask [`mountable_active_provider`] and then branch on this
/// enum, so adding an adapter changes this file and nothing in the daemon.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum MountableProviderKindV1 {
    /// The TraceDecay Native adapter over the host's own memory authority.
    Native,
}

impl MountableProviderKindV1 {
    /// Returns the stable provider identity this kind registers under.
    #[must_use]
    pub const fn provider_id(self) -> &'static str {
        match self {
            Self::Native => NATIVE_PROVIDER_ID,
        }
    }

    /// Returns the recall scope bindings the adapter declares.
    #[must_use]
    pub const fn declared_recall_scope_bindings(self) -> &'static [&'static str] {
        match self {
            Self::Native => NATIVE_RECALL_SCOPE_BINDINGS,
        }
    }
}

/// Maps a configured active-provider name onto the adapter that can serve it.
///
/// `None` means this registry has no adapter for the name and the caller must
/// refuse the configuration rather than substituting a provider.
#[must_use]
pub fn mountable_active_provider(provider: &str) -> Option<MountableProviderKindV1> {
    [MountableProviderKindV1::Native]
        .into_iter()
        .find(|kind| kind.provider_id() == provider)
}

/// Whether this registry can mount `provider` as an *active* recall provider.
#[must_use]
pub fn is_mountable_active_provider(provider: &str) -> bool {
    mountable_active_provider(provider).is_some()
}

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

/// Explicit selection of the one provider a composition may register in a
/// non-observer role.
///
/// The composition root chooses a variant from the typed
/// [`MountableProviderKindV1`] the registry returned for the configured
/// active-provider name, so no layer above this one compares provider names.
/// Each enabled variant carries the authority that adapter needs and cannot
/// be constructed without it.
pub enum SelectedProviderActivationV1 {
    /// Construct no provider, no fabric, and no state.
    Disabled,
    /// Register the TraceDecay Native adapter over the host's memory port.
    Native {
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

impl From<NativeProviderActivation> for SelectedProviderActivationV1 {
    fn from(value: NativeProviderActivation) -> Self {
        match value {
            NativeProviderActivation::Disabled => Self::Disabled,
            NativeProviderActivation::Enabled {
                fabric_config,
                port,
                registration_revision,
                mode,
            } => Self::Native {
                fabric_config,
                port,
                registration_revision,
                mode,
            },
        }
    }
}

/// The concrete adapter one composition registers in a non-observer role.
enum SelectedRegistration {
    Native(Arc<dyn NativeMemoryApplicationPort>),
}

impl SelectedRegistration {
    /// Resolves the typed kind this registration registers under.
    ///
    /// The kind is derived from the registration itself, so the registry
    /// never branches on a provider name supplied by the caller.
    const fn kind(&self) -> MountableProviderKindV1 {
        match self {
            Self::Native(_) => MountableProviderKindV1::Native,
        }
    }

    fn into_provider(self) -> Result<Arc<dyn MemoryProvider>, RegistryError> {
        match self {
            Self::Native(port) => Ok(Arc::new(NativeProvider::new(port)?)),
        }
    }
}

/// One Observer registration in a composed provider set.
///
/// The concrete adapter is *injected by the composition root*, which is the
/// only layer allowed to construct a concrete provider. This registry records
/// it under the identity its own descriptor declares, so nothing here — and
/// nothing in the daemon — branches on a provider name to decide that a
/// provider is an observer. Mode is carried by the registration itself, not
/// inferred.
///
/// An observer is registered with **no** `recall_scope_bindings` entry. Recall
/// admission accepts a candidate only against bindings this registry recorded
/// at registration, so an observer has no authorized scope binding to recall
/// under even if a route ever reached it. That is a second, independent
/// refusal behind the fabric's `ProviderMode::Observer` gate.
pub struct ObserverProviderRegistration {
    /// The concrete observer adapter the composition root constructed.
    pub provider: Arc<dyn MemoryProvider>,
    /// Positive product-owned registration revision for this observer.
    pub registration_revision: u64,
}

/// Explicit result of configured product provider composition.
pub enum ProjectMemoryProviderComposition {
    /// Provider infrastructure is absent.
    Disabled,
    /// Provider infrastructure was explicitly enabled and constructed.
    Enabled(ProjectMemoryProviderRegistry),
}

impl ProjectMemoryProviderComposition {
    /// Applies the explicit activation with no observer registrations.
    ///
    /// Equivalent to [`Self::compose_with_observers`] with an empty set.
    pub fn compose(native: NativeProviderActivation) -> Result<Self, RegistryError> {
        Self::compose_with_observers(native, Vec::new())
    }

    /// Applies the explicit activation as a **bounded provider set**: one
    /// separately selected Native provider in its configured mode, plus zero
    /// or more injected Observer registrations.
    ///
    /// The active provider and the observer set are chosen independently —
    /// the Native activation names the mode of the provider that may answer
    /// product calls, and each [`ObserverProviderRegistration`] is registered
    /// in [`ProviderMode::Observer`] and can never be selected for product
    /// output by any route this registry exposes. The set is refused before
    /// any registration when it does not fit the fabric's finite registry
    /// capacity, when an observer declares the Native identity, or when two
    /// observers declare the same identity, so a partially registered
    /// composition can never be observed.
    ///
    /// A disabled activation with a non-empty observer set is a configuration
    /// error, not a silently dropped set: observers exist only inside an
    /// enabled composition.
    pub fn compose_with_observers(
        native: NativeProviderActivation,
        observers: Vec<ObserverProviderRegistration>,
    ) -> Result<Self, RegistryError> {
        Self::compose_selected(native.into(), observers)
    }

    /// Applies an explicit provider-neutral selection as a bounded provider
    /// set: one selected adapter in its configured mode, plus zero or more
    /// injected Observer registrations.
    ///
    /// This is the general form [`Self::compose`] and
    /// [`Self::compose_with_observers`] delegate to. The selected adapter is
    /// resolved to a typed
    /// [`MountableProviderKindV1`] from its own descriptor, never from a
    /// provider string supplied by the caller, and it is registered under
    /// that identity with the recall scope bindings the kind authorizes.
    pub fn compose_selected(
        selection: SelectedProviderActivationV1,
        observers: Vec<ObserverProviderRegistration>,
    ) -> Result<Self, RegistryError> {
        // Refused before the activation match so the disabled arm stays the
        // single, unconditional `Ok(Self::Disabled)` that constructs no
        // provider, no fabric, and no state.
        if !observers.is_empty() && matches!(selection, SelectedProviderActivationV1::Disabled) {
            return Err(RegistryError::ObserverWithoutEnabledComposition {
                observers: observers.len(),
            });
        }
        let (fabric_config, selected, registration_revision, mode) = match selection {
            SelectedProviderActivationV1::Disabled => return Ok(Self::Disabled),
            SelectedProviderActivationV1::Native {
                fabric_config,
                port,
                registration_revision,
                mode,
            } => (
                fabric_config,
                SelectedRegistration::Native(port),
                registration_revision,
                mode,
            ),
        };
        Ok(Self::Enabled(
            ProjectMemoryProviderRegistry::compose_provider_set(
                fabric_config,
                selected,
                registration_revision,
                mode,
                observers,
            )?,
        ))
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
    /// Observer registrations were supplied for a disabled composition.
    ObserverWithoutEnabledComposition {
        /// Observer registrations that were supplied.
        observers: usize,
    },
    /// The composed provider set does not fit the fabric's finite registry.
    ProviderSetExceedsRegistryCapacity {
        /// Providers the composition would register.
        providers: usize,
        /// Finite registry capacity the fabric configuration allows.
        maximum: usize,
    },
    /// An observer registration declared the separately selected provider's
    /// own identity, which would make one identity both active and observer.
    ObserverDuplicatesSelectedProvider(String),
    /// Two observer registrations declared the same provider identity.
    DuplicateObserverProvider(String),
    /// The constructed adapter declared an identity other than the selected
    /// kind's own, so it was never registered.
    SelectedProviderIdentityMismatch {
        /// Identity the selected kind declares.
        expected: &'static str,
        /// Identity the constructed adapter's descriptor declared.
        declared: String,
    },
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
            Self::ObserverWithoutEnabledComposition { observers } => write!(
                formatter,
                "{observers} observer registration(s) were supplied for a disabled composition"
            ),
            Self::ProviderSetExceedsRegistryCapacity { providers, maximum } => write!(
                formatter,
                "composed provider set of {providers} exceeds the finite registry capacity of \
                 {maximum}"
            ),
            Self::ObserverDuplicatesSelectedProvider(provider) => write!(
                formatter,
                "observer registration declares the selected provider identity {provider}"
            ),
            Self::DuplicateObserverProvider(provider) => write!(
                formatter,
                "observer provider {provider} is registered more than once"
            ),
            Self::SelectedProviderIdentityMismatch { expected, declared } => write!(
                formatter,
                "selected provider adapter declared identity {declared}, expected {expected}"
            ),
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
            Self::ObserverWithoutEnabledComposition { .. }
            | Self::ProviderSetExceedsRegistryCapacity { .. }
            | Self::ObserverDuplicatesSelectedProvider(_)
            | Self::DuplicateObserverProvider(_)
            | Self::SelectedProviderIdentityMismatch { .. } => None,
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
/// let _private_constructor = ProjectMemoryProviderRegistry::compose_provider_set;
/// ```
///
/// ```compile_fail,E0624
/// use tracedecay_memory_provider_registry::ProjectMemoryProviderRegistry;
///
/// let _private_registration = ProjectMemoryProviderRegistry::register_selected;
/// ```
///
/// ```compile_fail,E0624
/// use tracedecay_memory_provider_registry::ProjectMemoryProviderRegistry;
///
/// let _private_observer = ProjectMemoryProviderRegistry::register_observer;
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
    fn compose_provider_set(
        fabric_config: FabricConfig,
        selected: SelectedRegistration,
        registration_revision: u64,
        mode: EnabledProviderMode,
        observers: Vec<ObserverProviderRegistration>,
    ) -> Result<Self, RegistryError> {
        let selected_kind = selected.kind();
        let selected_provider_id = OwnedProviderId::new(selected_kind.provider_id())?;
        // The fabric owns finite-configuration validation, so it is
        // constructed first and its typed `InvalidConfig` reaches the caller
        // unchanged. Constructing a fabric registers nothing, so the whole
        // provider set is still validated before any registration happens and
        // a refused configuration never leaves a half-composed registry
        // behind.
        let fabric = Arc::new(MemoryFabric::new(fabric_config)?);
        let providers = observers.len().saturating_add(1);
        if providers > fabric_config.max_registered_providers {
            return Err(RegistryError::ProviderSetExceedsRegistryCapacity {
                providers,
                maximum: fabric_config.max_registered_providers,
            });
        }
        let mut declared: BTreeMap<OwnedProviderId, ()> = BTreeMap::new();
        for observer in &observers {
            let declared_id = observer.provider.descriptor().provider_id;
            if declared_id == selected_provider_id {
                return Err(RegistryError::ObserverDuplicatesSelectedProvider(
                    declared_id.as_str().to_owned(),
                ));
            }
            if declared.insert(declared_id.clone(), ()).is_some() {
                return Err(RegistryError::DuplicateObserverProvider(
                    declared_id.as_str().to_owned(),
                ));
            }
        }
        let mut registry = Self {
            fabric,
            recall_scope_bindings: BTreeMap::new(),
        };
        registry.register_selected(selected, registration_revision, mode)?;
        for observer in observers {
            registry.register_observer(observer)?;
        }
        Ok(registry)
    }

    /// Registers one injected adapter in observer mode under the identity its
    /// own descriptor declares. No recall scope binding is recorded, so the
    /// observer is unauthorized for recall admission independently of the
    /// fabric mode gate.
    fn register_observer(
        &mut self,
        observer: ObserverProviderRegistration,
    ) -> Result<(), RegistryError> {
        let provider_id = observer.provider.descriptor().provider_id;
        self.fabric.register(
            provider_id,
            observer.registration_revision,
            ProviderMode::Observer,
            observer.provider,
        )?;
        Ok(())
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

    /// Registers the one selected adapter under the identity and the recall
    /// scope bindings that adapter itself declares.
    fn register_selected(
        &mut self,
        selected: SelectedRegistration,
        registration_revision: u64,
        mode: EnabledProviderMode,
    ) -> Result<(), RegistryError> {
        let kind = selected.kind();
        let bindings =
            RecallScopeBindingsV1::from_wire(kind.declared_recall_scope_bindings().iter().copied())
                .map_err(RegistryError::RecallScopeBindings)?;
        let provider = selected.into_provider()?;
        // The identity is read back from the constructed adapter's own
        // descriptor and compared with the kind's declared identity, so a
        // misdeclared adapter is refused instead of being registered under a
        // name it does not answer to.
        let provider_id = provider.descriptor().provider_id;
        if provider_id.as_str() != kind.provider_id() {
            return Err(RegistryError::SelectedProviderIdentityMismatch {
                expected: kind.provider_id(),
                declared: provider_id.as_str().to_owned(),
            });
        }
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
