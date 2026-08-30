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
//! Provider-neutral runtime boundary for TraceDecay cognitive memory.
//!
//! The canonical JSON contracts remain the sole wire authority. This crate
//! exposes those generated values plus owned runtime identities, exact coding
//! scope, live cancellation, bounded operation envelopes, typed terminal
//! results, and the object-safe provider trait used by orchestration and
//! adapters. It contains no provider implementation, transport, persistence,
//! TraceDecay database, code-index, daemon, dashboard, or host dependency.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[rustfmt::skip]
#[path = "../../../product/contracts/memory-provider-v1/generated/rust/memory_provider_v1.rs"]
/// Generated dependency-free values from the canonical Memory Provider V1 contracts.
pub mod contract;

use contract::{
    CancellationState, CapabilityId, CommittedEffectState, ExactScopeIdentity, FallbackEligibility,
    IdentifierError, OpaqueExtension, RequestControl, TerminalCode,
};

/// Stable API validation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApiError {
    /// A provider identifier did not satisfy the canonical generated validator.
    InvalidProviderId(IdentifierError),
    /// A capability or versioned contract identifier was invalid.
    InvalidVersionedId(IdentifierError),
    /// A required string field was empty.
    EmptyField(&'static str),
    /// A lowercase SHA-256 digest was malformed.
    InvalidSha256(&'static str),
    /// A capability was declared more than once.
    DuplicateCapability(String),
    /// A mandatory capability was absent from a provider descriptor.
    MandatoryCapabilityMissing(&'static str),
    /// A finite limit was zero.
    ZeroLimit(&'static str),
    /// A mutating operation lacked a deterministic idempotency key.
    MissingIdempotencyKey,
    /// The operation capability was not present in the request requirements.
    MissingOperationCapability(&'static str),
    /// An opaque extension version was zero.
    InvalidExtensionVersion,
}

impl fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidProviderId(error) => write!(formatter, "invalid provider id: {error}"),
            Self::InvalidVersionedId(error) => {
                write!(formatter, "invalid versioned identifier: {error}")
            }
            Self::EmptyField(field) => write!(formatter, "required field {field} is empty"),
            Self::InvalidSha256(field) => {
                write!(formatter, "field {field} is not lowercase SHA-256 hex")
            }
            Self::DuplicateCapability(capability) => {
                write!(formatter, "capability {capability} is duplicated")
            }
            Self::MandatoryCapabilityMissing(capability) => {
                write!(formatter, "mandatory capability {capability} is missing")
            }
            Self::ZeroLimit(limit) => write!(formatter, "finite limit {limit} is zero"),
            Self::MissingIdempotencyKey => {
                formatter.write_str("mutating operation has no idempotency key")
            }
            Self::MissingOperationCapability(capability) => {
                write!(
                    formatter,
                    "operation requires undeclared capability {capability}"
                )
            }
            Self::InvalidExtensionVersion => {
                formatter.write_str("opaque extension version must be positive")
            }
        }
    }
}

impl Error for ApiError {}

fn require_non_empty(value: &str, field: &'static str) -> Result<(), ApiError> {
    if value.is_empty() {
        Err(ApiError::EmptyField(field))
    } else {
        Ok(())
    }
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn require_sha256(value: &str, field: &'static str) -> Result<(), ApiError> {
    if is_lowercase_sha256(value) {
        Ok(())
    } else {
        Err(ApiError::InvalidSha256(field))
    }
}

/// Owned stable logical provider identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OwnedProviderId(String);

impl OwnedProviderId {
    /// Validates and owns a canonical provider ID.
    pub fn new(value: impl Into<String>) -> Result<Self, ApiError> {
        let value = value.into();
        contract::ProviderId::new(&value).map_err(ApiError::InvalidProviderId)?;
        Ok(Self(value))
    }

    /// Returns the canonical provider ID.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Owned stable versioned capability or contract identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OwnedVersionedId(String);

impl OwnedVersionedId {
    /// Validates and owns a canonical versioned ID.
    pub fn new(value: impl Into<String>) -> Result<Self, ApiError> {
        let value = value.into();
        CapabilityId::new(&value).map_err(ApiError::InvalidVersionedId)?;
        Ok(Self(value))
    }

    /// Returns the canonical versioned identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Exact TraceDecay-owned coding scope in owned runtime form.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct OwnedExactScope {
    /// Profile authority identity.
    pub profile_id: String,
    /// Project authority identity.
    pub project_id: String,
    /// Repository authority identity.
    pub repository_identity: String,
    /// Exact linked-worktree identity.
    pub worktree_identity: String,
    /// Exact branch or detached-reference identity.
    pub branch_identity: String,
    /// Exact coding-agent session identity.
    pub agent_session_id: String,
    /// Monotonic TraceDecay scope revision.
    pub scope_revision: u64,
}

impl OwnedExactScope {
    /// Validates one complete exact coding scope.
    pub fn new(
        profile_id: impl Into<String>,
        project_id: impl Into<String>,
        repository_identity: impl Into<String>,
        worktree_identity: impl Into<String>,
        branch_identity: impl Into<String>,
        agent_session_id: impl Into<String>,
        scope_revision: u64,
    ) -> Result<Self, ApiError> {
        let scope = Self {
            profile_id: profile_id.into(),
            project_id: project_id.into(),
            repository_identity: repository_identity.into(),
            worktree_identity: worktree_identity.into(),
            branch_identity: branch_identity.into(),
            agent_session_id: agent_session_id.into(),
            scope_revision,
        };
        require_non_empty(&scope.profile_id, "profile_id")?;
        require_non_empty(&scope.project_id, "project_id")?;
        require_non_empty(&scope.repository_identity, "repository_identity")?;
        require_non_empty(&scope.worktree_identity, "worktree_identity")?;
        require_non_empty(&scope.branch_identity, "branch_identity")?;
        require_non_empty(&scope.agent_session_id, "agent_session_id")?;
        Ok(scope)
    }

    /// Borrows the generated exact-scope representation.
    #[must_use]
    pub fn borrowed(&self) -> ExactScopeIdentity<'_> {
        ExactScopeIdentity {
            profile_id: &self.profile_id,
            project_id: &self.project_id,
            repository_identity: &self.repository_identity,
            worktree_identity: &self.worktree_identity,
            branch_identity: &self.branch_identity,
            agent_session_id: &self.agent_session_id,
            scope_revision: self.scope_revision,
        }
    }
}

/// Thread-safe cooperative cancellation signal.
#[derive(Clone, Debug, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    /// Creates a live cancellation token.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Marks the token cancelled. Repeated cancellation is idempotent.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Returns whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Live deadline and cancellation budget for one provider operation.
#[derive(Clone, Debug)]
pub struct OperationControl {
    deadline_utc_micros: i64,
    remaining_millis: u64,
    cancellation: CancellationToken,
}

impl OperationControl {
    /// Creates request control with a finite monotonic remaining budget.
    #[must_use]
    pub fn new(
        deadline_utc_micros: i64,
        remaining_millis: u64,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            deadline_utc_micros,
            remaining_millis,
            cancellation,
        }
    }

    /// Returns an immutable wire snapshot or a terminal preflight failure.
    pub fn snapshot(&self) -> Result<RequestControl, TerminalCode> {
        if self.cancellation.is_cancelled() {
            Err(TerminalCode::Cancelled)
        } else if self.remaining_millis == 0 {
            Err(TerminalCode::DeadlineExceeded)
        } else {
            Ok(RequestControl {
                deadline_utc_micros: self.deadline_utc_micros,
                remaining_millis: self.remaining_millis,
                cancellation: CancellationState::Live,
            })
        }
    }

    /// Returns the shared live cancellation token.
    #[must_use]
    pub fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// Returns the finite remaining budget at dispatch.
    #[must_use]
    pub const fn remaining_millis(&self) -> u64 {
        self.remaining_millis
    }
}

/// Provider operation routed by one versioned capability.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ProviderOperation {
    /// Read-only compatible handshake.
    Handshake,
    /// Mandatory provider health.
    Health,
    /// Idempotent provider-local observation acceptance.
    Observe,
    /// Advisory provider recall.
    Recall,
    /// Provider-local feedback recording.
    Feedback,
    /// Provider-local maintenance.
    Maintenance,
    /// Redacted provider inspection.
    Inspection,
    /// Provider-local correction.
    Correction,
    /// Provider-local deletion by admitted source identity.
    DeleteBySource,
    /// Provider-local snapshot export.
    SnapshotExport,
    /// Provider-local snapshot restore.
    SnapshotRestore,
    /// Provider-local deterministic replay.
    Replay,
}

impl ProviderOperation {
    /// Returns the versioned capability required for this operation.
    #[must_use]
    pub const fn capability_id(self) -> &'static str {
        match self {
            Self::Handshake | Self::Health => "provider.health.v1",
            Self::Observe => "observation.accept.v1",
            Self::Recall => "recall.query.v1",
            Self::Feedback => "feedback.record.v1",
            Self::Maintenance => "maintenance.run.v1",
            Self::Inspection => "inspection.read.v1",
            Self::Correction => "correction.apply.v1",
            Self::DeleteBySource => "deletion.by_source.v1",
            Self::SnapshotExport => "snapshot.export.v1",
            Self::SnapshotRestore => "snapshot.restore.v1",
            Self::Replay => "replay.apply.v1",
        }
    }

    /// Returns whether the operation may mutate provider-local state.
    #[must_use]
    pub const fn mutates_provider_state(self) -> bool {
        matches!(
            self,
            Self::Observe
                | Self::Feedback
                | Self::Maintenance
                | Self::Correction
                | Self::DeleteBySource
                | Self::SnapshotRestore
                | Self::Replay
        )
    }
}

/// Finite provider ceilings negotiated during handshake.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderLimits {
    /// Maximum canonical request bytes.
    pub request_bytes: u64,
    /// Maximum canonical response bytes.
    pub response_bytes: u64,
    /// Maximum observations in one batch.
    pub observation_batch_items: u64,
    /// Maximum recall candidates.
    pub recall_candidates: u64,
    /// Maximum concurrent operations.
    pub concurrent_operations: u64,
    /// Maximum operation duration in milliseconds.
    pub operation_millis: u64,
    /// Maximum snapshot bytes.
    pub snapshot_bytes: u64,
    /// Maximum inspection items.
    pub inspection_items: u64,
}

impl ProviderLimits {
    /// Validates that every negotiated limit is finite and positive.
    pub fn validate(self) -> Result<Self, ApiError> {
        for (name, value) in [
            ("request_bytes", self.request_bytes),
            ("response_bytes", self.response_bytes),
            ("observation_batch_items", self.observation_batch_items),
            ("recall_candidates", self.recall_candidates),
            ("concurrent_operations", self.concurrent_operations),
            ("operation_millis", self.operation_millis),
            ("snapshot_bytes", self.snapshot_bytes),
            ("inspection_items", self.inspection_items),
        ] {
            if value == 0 {
                return Err(ApiError::ZeroLimit(name));
            }
        }
        Ok(self)
    }

    /// Negotiates the lower host/provider ceiling for every limit.
    #[must_use]
    pub fn minimum(self, other: Self) -> Self {
        Self {
            request_bytes: self.request_bytes.min(other.request_bytes),
            response_bytes: self.response_bytes.min(other.response_bytes),
            observation_batch_items: self
                .observation_batch_items
                .min(other.observation_batch_items),
            recall_candidates: self.recall_candidates.min(other.recall_candidates),
            concurrent_operations: self.concurrent_operations.min(other.concurrent_operations),
            operation_millis: self.operation_millis.min(other.operation_millis),
            snapshot_bytes: self.snapshot_bytes.min(other.snapshot_bytes),
            inspection_items: self.inspection_items.min(other.inspection_items),
        }
    }
}

/// Immutable provider implementation and capability descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderDescriptor {
    /// Stable logical provider identity.
    pub provider_id: OwnedProviderId,
    /// SHA-256 over immutable implementation identity.
    pub implementation_identity_sha256: String,
    /// Provider-local state schema identity.
    pub state_schema_version: String,
    /// Current provider-local state generation.
    pub state_generation: u64,
    /// Compatible provider protocol major.
    pub protocol_major: u16,
    /// Compatible provider protocol minor.
    pub protocol_minor: u16,
    /// Real declared capabilities.
    pub capabilities: BTreeSet<OwnedVersionedId>,
    /// Finite provider ceilings.
    pub limits: ProviderLimits,
}

impl ProviderDescriptor {
    /// Builds a validated descriptor and requires every mandatory capability.
    pub fn new(
        provider_id: OwnedProviderId,
        implementation_identity_sha256: impl Into<String>,
        state_schema_version: impl Into<String>,
        state_generation: u64,
        capabilities: impl IntoIterator<Item = OwnedVersionedId>,
        limits: ProviderLimits,
    ) -> Result<Self, ApiError> {
        let implementation_identity_sha256 = implementation_identity_sha256.into();
        let state_schema_version = state_schema_version.into();
        require_sha256(
            &implementation_identity_sha256,
            "implementation_identity_sha256",
        )?;
        require_non_empty(&state_schema_version, "state_schema_version")?;
        let mut capability_set = BTreeSet::new();
        for capability in capabilities {
            let capability_name = capability.as_str().to_owned();
            if !capability_set.insert(capability) {
                return Err(ApiError::DuplicateCapability(capability_name));
            }
        }
        for mandatory in [
            "provider.health.v1",
            "observation.accept.v1",
            "recall.query.v1",
        ] {
            if !capability_set
                .iter()
                .any(|capability| capability.as_str() == mandatory)
            {
                return Err(ApiError::MandatoryCapabilityMissing(mandatory));
            }
        }
        Ok(Self {
            provider_id,
            implementation_identity_sha256,
            state_schema_version,
            state_generation,
            protocol_major: 1,
            protocol_minor: 0,
            capabilities: capability_set,
            limits: limits.validate()?,
        })
    }

    /// Returns whether this descriptor declares one capability.
    #[must_use]
    pub fn supports(&self, capability_id: &str) -> bool {
        self.capabilities
            .iter()
            .any(|capability| capability.as_str() == capability_id)
    }
}

/// Canonical payload bytes with an externally verified digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalPayload {
    /// Versioned payload contract identity.
    pub contract_id: OwnedVersionedId,
    /// Canonical payload bytes.
    pub bytes: Vec<u8>,
    /// Lowercase SHA-256 of canonical payload bytes.
    pub sha256: String,
}

impl CanonicalPayload {
    /// Creates a validated canonical payload envelope.
    pub fn new(
        contract_id: OwnedVersionedId,
        bytes: Vec<u8>,
        sha256: impl Into<String>,
    ) -> Result<Self, ApiError> {
        let sha256 = sha256.into();
        require_sha256(&sha256, "payload_sha256")?;
        if bytes.is_empty() {
            return Err(ApiError::EmptyField("canonical_payload"));
        }
        Ok(Self {
            contract_id,
            bytes,
            sha256,
        })
    }
}

/// Owned opaque extension retained without activating behavior.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedOpaqueExtension {
    /// Stable extension identity.
    pub extension_id: OwnedVersionedId,
    /// Positive extension version.
    pub extension_version: u32,
    /// Whether unknown support is mandatory.
    pub required: bool,
    /// Lowercase SHA-256 of canonical opaque bytes.
    pub payload_sha256: String,
    /// Canonical opaque extension bytes.
    pub canonical_payload: Vec<u8>,
}

impl OwnedOpaqueExtension {
    /// Validates one owned extension.
    pub fn new(
        extension_id: OwnedVersionedId,
        extension_version: u32,
        required: bool,
        payload_sha256: impl Into<String>,
        canonical_payload: Vec<u8>,
    ) -> Result<Self, ApiError> {
        if extension_version == 0 {
            return Err(ApiError::InvalidExtensionVersion);
        }
        let payload_sha256 = payload_sha256.into();
        require_sha256(&payload_sha256, "extension_payload_sha256")?;
        if canonical_payload.is_empty() {
            return Err(ApiError::EmptyField("extension_canonical_payload"));
        }
        Ok(Self {
            extension_id,
            extension_version,
            required,
            payload_sha256,
            canonical_payload,
        })
    }

    /// Borrows the generated extension representation.
    #[must_use]
    pub fn borrowed(&self) -> OpaqueExtension<'_> {
        OpaqueExtension {
            extension_id: self.extension_id.as_str(),
            extension_version: self.extension_version,
            required: self.required,
            payload_sha256: &self.payload_sha256,
            canonical_payload: &self.canonical_payload,
        }
    }
}

/// Owned runtime provider call built from canonical wire bytes.
#[derive(Clone, Debug)]
pub struct ProviderCall {
    /// Operation capability.
    pub operation: ProviderOperation,
    /// Target provider identity.
    pub provider_id: OwnedProviderId,
    /// Accepted registration revision.
    pub registration_revision: u64,
    /// Compatible ready-receipt digest.
    pub ready_receipt_sha256: String,
    /// Exact TraceDecay-owned coding scope.
    pub exact_scope: OwnedExactScope,
    /// Stable request identity.
    pub request_id: String,
    /// Stable operation identity.
    pub operation_id: String,
    /// Expected provider-local state generation.
    pub expected_state_generation: u64,
    /// Deterministic idempotency key for provider-local mutations.
    pub idempotency_key: Option<String>,
    /// Live request control.
    pub control: OperationControl,
    /// Canonical operation payload.
    pub payload: CanonicalPayload,
    /// Required capabilities, including the operation capability.
    pub required_capabilities: BTreeSet<OwnedVersionedId>,
    /// Opaque extensions.
    pub extensions: Vec<OwnedOpaqueExtension>,
}

impl ProviderCall {
    /// Validates a complete runtime call.
    pub fn new(parts: ProviderCallParts) -> Result<Self, ApiError> {
        require_sha256(&parts.ready_receipt_sha256, "ready_receipt_sha256")?;
        require_non_empty(&parts.request_id, "request_id")?;
        require_non_empty(&parts.operation_id, "operation_id")?;
        if parts.operation.mutates_provider_state()
            && parts.idempotency_key.as_deref().is_none_or(str::is_empty)
        {
            return Err(ApiError::MissingIdempotencyKey);
        }
        let mut required_capabilities = BTreeSet::new();
        for capability in parts.required_capabilities {
            let capability_name = capability.as_str().to_owned();
            if !required_capabilities.insert(capability) {
                return Err(ApiError::DuplicateCapability(capability_name));
            }
        }
        let operation_capability = parts.operation.capability_id();
        if !required_capabilities
            .iter()
            .any(|capability| capability.as_str() == operation_capability)
        {
            return Err(ApiError::MissingOperationCapability(operation_capability));
        }
        Ok(Self {
            operation: parts.operation,
            provider_id: parts.provider_id,
            registration_revision: parts.registration_revision,
            ready_receipt_sha256: parts.ready_receipt_sha256,
            exact_scope: parts.exact_scope,
            request_id: parts.request_id,
            operation_id: parts.operation_id,
            expected_state_generation: parts.expected_state_generation,
            idempotency_key: parts.idempotency_key,
            control: parts.control,
            payload: parts.payload,
            required_capabilities,
            extensions: parts.extensions,
        })
    }
}

/// Builder payload for one provider call.
#[derive(Clone, Debug)]
pub struct ProviderCallParts {
    /// Operation capability.
    pub operation: ProviderOperation,
    /// Target provider.
    pub provider_id: OwnedProviderId,
    /// Accepted registration revision.
    pub registration_revision: u64,
    /// Compatible ready-receipt digest.
    pub ready_receipt_sha256: String,
    /// Exact coding scope.
    pub exact_scope: OwnedExactScope,
    /// Stable request identity.
    pub request_id: String,
    /// Stable operation identity.
    pub operation_id: String,
    /// Expected provider-local generation.
    pub expected_state_generation: u64,
    /// Deterministic idempotency key for mutations.
    pub idempotency_key: Option<String>,
    /// Live request control.
    pub control: OperationControl,
    /// Canonical payload.
    pub payload: CanonicalPayload,
    /// Required capabilities.
    pub required_capabilities: Vec<OwnedVersionedId>,
    /// Opaque extensions.
    pub extensions: Vec<OwnedOpaqueExtension>,
}

/// Provider-neutral owned terminal record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TerminalRecord {
    /// Closed terminal code.
    pub terminal_code: TerminalCode,
    /// Truthful provider-local committed effect.
    pub committed_effect: CommittedEffectState,
    /// Explicit fallback eligibility.
    pub fallback: FallbackEligibility,
    /// Stable operation identity.
    pub operation_id: String,
    /// Exact scope digest.
    pub exact_scope_sha256: String,
    /// Optional provider receipt when an effect may have committed.
    pub provider_receipt_sha256: Option<String>,
    /// Optional stable diagnostic identity.
    pub diagnostic_id: Option<String>,
}

impl TerminalRecord {
    /// Creates a validated terminal record. Fallback remains explicit.
    pub fn new(
        terminal_code: TerminalCode,
        committed_effect: CommittedEffectState,
        fallback: FallbackEligibility,
        operation_id: impl Into<String>,
        exact_scope_sha256: impl Into<String>,
        provider_receipt_sha256: Option<String>,
        diagnostic_id: Option<String>,
    ) -> Result<Self, ApiError> {
        let operation_id = operation_id.into();
        let exact_scope_sha256 = exact_scope_sha256.into();
        require_non_empty(&operation_id, "operation_id")?;
        require_sha256(&exact_scope_sha256, "exact_scope_sha256")?;
        if let Some(receipt) = &provider_receipt_sha256 {
            require_sha256(receipt, "provider_receipt_sha256")?;
        }
        Ok(Self {
            terminal_code,
            committed_effect,
            fallback,
            operation_id,
            exact_scope_sha256,
            provider_receipt_sha256,
            diagnostic_id,
        })
    }
}

/// Provider-neutral operation response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderReply {
    /// Typed terminal information.
    pub terminal: TerminalRecord,
    /// Optional canonical successful or partial result payload.
    pub payload: Option<CanonicalPayload>,
    /// Bounded diagnostic warnings.
    pub warnings: Vec<String>,
    /// Opaque optional extensions.
    pub extensions: Vec<OwnedOpaqueExtension>,
    /// Provider-local generation observed after the call.
    pub state_generation: u64,
}

/// Compatible handshake request.
#[derive(Clone, Debug)]
pub struct HandshakeRequest {
    /// Selected provider identity.
    pub provider_id: OwnedProviderId,
    /// Accepted registration revision.
    pub registration_revision: u64,
    /// Exact TraceDecay-owned scope.
    pub exact_scope: OwnedExactScope,
    /// Stable request identity.
    pub request_id: String,
    /// Mandatory requested capabilities.
    pub required_capabilities: BTreeSet<OwnedVersionedId>,
    /// Finite host ceilings.
    pub host_limits: ProviderLimits,
    /// Live request control.
    pub control: OperationControl,
    /// Canonical 32-byte challenge nonce.
    pub challenge_nonce: [u8; 32],
}

/// Builder payload for one provider handshake request.
#[derive(Clone, Debug)]
pub struct HandshakeRequestParts {
    /// Selected provider identity.
    pub provider_id: OwnedProviderId,
    /// Accepted registration revision.
    pub registration_revision: u64,
    /// Exact TraceDecay-owned scope.
    pub exact_scope: OwnedExactScope,
    /// Stable request identity.
    pub request_id: String,
    /// Mandatory requested capabilities.
    pub required_capabilities: Vec<OwnedVersionedId>,
    /// Finite host ceilings.
    pub host_limits: ProviderLimits,
    /// Live request control.
    pub control: OperationControl,
    /// Canonical 32-byte challenge nonce.
    pub challenge_nonce: [u8; 32],
}

impl HandshakeRequest {
    /// Validates one handshake request assembled from explicit parts.
    pub fn new(parts: HandshakeRequestParts) -> Result<Self, ApiError> {
        require_non_empty(&parts.request_id, "request_id")?;
        let mut capability_set = BTreeSet::new();
        for capability in parts.required_capabilities {
            let capability_name = capability.as_str().to_owned();
            if !capability_set.insert(capability) {
                return Err(ApiError::DuplicateCapability(capability_name));
            }
        }
        Ok(Self {
            provider_id: parts.provider_id,
            registration_revision: parts.registration_revision,
            exact_scope: parts.exact_scope,
            request_id: parts.request_id,
            required_capabilities: capability_set,
            host_limits: parts.host_limits.validate()?,
            control: parts.control,
            challenge_nonce: parts.challenge_nonce,
        })
    }
}

/// Successful or failed handshake response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HandshakeResponse {
    /// Typed terminal information.
    pub terminal: TerminalRecord,
    /// Provider descriptor only when identity and compatibility were verified.
    pub descriptor: Option<ProviderDescriptor>,
    /// Opaque runtime instance identity.
    pub provider_instance_id: Option<String>,
    /// Provider-local state namespace.
    pub state_namespace: Option<String>,
    /// Accepted exact coding scope.
    pub accepted_scope: Option<OwnedExactScope>,
    /// Effective lower host/provider ceilings.
    pub effective_limits: Option<ProviderLimits>,
    /// Expiring ready-receipt digest.
    pub ready_receipt_sha256: Option<String>,
    /// Bounded warnings.
    pub warnings: Vec<String>,
}

/// Object-safe provider implementation boundary.
pub trait MemoryProvider: Send + Sync + 'static {
    /// Returns the provider's current real descriptor without fabricating readiness.
    fn descriptor(&self) -> ProviderDescriptor;

    /// Performs the read-only compatible handshake for an exact scope.
    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse;

    /// Executes one capability-routed provider operation.
    fn invoke(&self, call: &ProviderCall) -> ProviderReply;
}
