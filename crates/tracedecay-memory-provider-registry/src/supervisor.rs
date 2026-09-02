//! Provider lifecycle supervision: start, monitor, restart, and stop one
//! provider instance for **one exact coding scope** without ever letting its
//! crash, hang, panic, or handshake failure crash or wedge the host.
//!
//! # One owner, one exact scope
//!
//! A supervisor is bound to a [`SupervisedScopeV1`] at construction: the
//! selected provider identity, the product-owned registration revision, and
//! the complete exact profile/project/repository/worktree/reference/session/
//! digest scope. Every [`ProviderSupervisorV1::start_or_restart`] request is
//! compared against that binding *before the adapter is contacted at all*, so
//! one supervisor can never start an instance under one scope and restart it
//! under another. A differing request returns
//! [`DegradationCauseV1::ScopeMismatch`] naming the offending field, and no
//! `start`, `handshake`, `request_stop`, or `kill` call is made.
//!
//! # What proves readiness
//!
//! Readiness is a fresh [`HandshakeResponse`] whose **every** success
//! invariant was verified — never process existence, never a spawn call
//! returning, never `terminal_code == Success` on its own. A successful
//! terminal with a missing instance identity, an absent or malformed
//! ready receipt, a foreign terminal provider or operation, a scope the
//! provider did not accept exactly, an absent descriptor, a missing required
//! capability, or effective limits above the host's own ceilings is a
//! [`DegradationCauseV1::HandshakeContractViolation`] carrying the exact
//! [`ReadinessDefectV1`] — never a silently promoted `Ready`. The only way to
//! obtain a [`ReadinessEvidenceV1`] is for the validator to have accepted
//! every one of those fields.
//!
//! # What bounds a restart loop
//!
//! Two independent bounds, both enforced inside `start_or_restart` rather
//! than advertised to a caller who may ignore them:
//!
//! * [`RestartBudgetV1::max_attempts_per_window`] spawn attempts inside a
//!   rolling `window_micros`; beyond it the outcome is
//!   [`DegradationCauseV1::RestartBudgetExhausted`] and no adapter call is
//!   made.
//! * A capped exponential *next-eligible instant*. Every pass that touches
//!   the adapter arms it; a call before it elapses returns
//!   [`DegradationCauseV1::BackoffNotElapsed`] and makes no adapter call. A
//!   caller spinning at one instant therefore cannot consume the budget, and
//!   cannot respawn faster than the schedule.
//!
//! # No overlapping owners
//!
//! The supervisor tracks the predecessor instance explicitly
//! ([`PredecessorStateV1`]). A replacement is spawned only after the
//! predecessor's death is confirmed through bounded
//! `request_stop`-then-`kill` escalation. A start call that failed may still
//! have spawned a child, so it leaves the predecessor `Live` and the next
//! pass terminates before spawning. A termination that could not be confirmed
//! leaves [`PredecessorStateV1::DeathUnknown`], and no spawn happens until a
//! later pass confirms death.
//!
//! # Crash and panic isolation
//!
//! Every adapter call runs inside an unwind boundary. An adapter that panics
//! yields [`DegradationCauseV1::AdapterPanicked`] and the host keeps running;
//! it never propagates a panic through the supervisor. For a process topology
//! (ADR-0009) the OS process is the primary boundary and the unwind boundary
//! is the secondary one for the in-host transport code; for an in-process
//! adapter the unwind boundary is the only one, and it depends on the
//! `unwind` panic strategy this workspace builds with.
//!
//! Every degradation is also **persisted**: [`ProviderSupervisorV1::current_degradation`]
//! reports the current typed [`DegradationRecordV1`] with no adapter call, so
//! a host that observed a crash through [`ProviderSupervisorV1::report_crash`]
//! can read back [`DegradationKindV1::Crashed`] rather than only a coarse
//! `Unavailable`.

use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::state_capability::{
    ProviderStateAuthorityV1, ProviderStateCapabilityV1, charset_usable, escapes_containment,
};
use crate::{
    ApiError, HandshakeRequest, HandshakeResponse, OwnedExactScope, OwnedProviderId,
    ProviderLimits, ProviderOperation, TerminalCode,
};

/// Maximum bytes one provider-supplied handshake warning may occupy.
const MAX_HANDSHAKE_WARNING_BYTES: usize = 512;
/// Maximum handshake warnings a successful response may carry.
const MAX_HANDSHAKE_WARNINGS: usize = 16;
/// Maximum bytes a provider-reported opaque runtime instance identity may
/// occupy.
const MAX_PROVIDER_INSTANCE_ID_BYTES: usize = 256;
/// Maximum bytes a provider-reported state namespace may occupy.
const MAX_STATE_NAMESPACE_BYTES: usize = 256;
/// Mandatory health capability every readiness handshake must prove.
const HEALTH_CAPABILITY_ID: &str = "provider.health.v1";
/// The capability whose contract is the only sanctioned channel for a
/// provider-local replay position (`acknowledged_sequence`). A provider that
/// declares it retains one; a provider that does not, does not — and the host
/// records which of the two it is rather than treating an absent position as
/// permission to skip verification.
const REPLAY_CAPABILITY_ID: &str = "replay.apply.v1";

/// The transport-agnostic seam a supervised provider adapter implements.
///
/// The adapter owns whatever mechanism actually starts, probes, and stops a
/// provider instance — an isolated local process for NCM's first topology
/// (ADR-0009), an in-process construction for a provider that needs none of
/// this. The supervisor owns none of that mechanism; it owns only the bounded
/// decisions of when to call it, and the invariants a call's answer must
/// satisfy before readiness is claimed.
///
/// Every method is bounded by an explicit deadline the supervisor computed
/// from its own configuration. An adapter must not run past it; the
/// supervisor does not enforce that internally because it has no thread to
/// enforce it with — the adapter's own transport (a process wait with a
/// timeout, an RPC deadline) is the enforcement mechanism.
pub trait ProviderLifecycleAdapterV1 {
    /// The adapter's own failure type, preserved whole when it fails.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Starts a fresh instance. Returns once the instance has been asked to
    /// start; it must not claim readiness — only [`Self::handshake`] proves
    /// that. Must respect `deadline_unix_micros`.
    ///
    /// An `Err` does **not** promise no child exists: the supervisor treats a
    /// failed start as a possibly-live predecessor and terminates before it
    /// spawns again.
    fn start(&self, deadline_unix_micros: i64) -> Result<(), Self::Error>;

    /// Performs the readiness handshake against the instance most recently
    /// started. A protocol-level failure to reach the instance is `Err`; a
    /// reachable instance that declines the handshake still returns `Ok` with
    /// the declining [`HandshakeResponse`] terminal, because that is a
    /// provider-neutral typed outcome the supervisor inspects, not a transport
    /// failure the adapter should hide.
    fn handshake(
        &self,
        request: &HandshakeRequest,
        deadline_unix_micros: i64,
    ) -> Result<HandshakeResponse, Self::Error>;

    /// Requests a graceful stop of the instance most recently started.
    /// Returns `Ok(true)` only when the instance is **confirmed dead** before
    /// `deadline_unix_micros`; `Ok(false)` means the deadline was reached
    /// with death not confirmed, which is not itself a failure — the
    /// supervisor escalates to [`Self::kill`] on that answer.
    fn request_stop(&self, deadline_unix_micros: i64) -> Result<bool, Self::Error>;

    /// Forcibly terminates the instance most recently started and confirms
    /// death. `Ok(())` is a confirmation of death, not merely of a signal
    /// having been sent: the supervisor spawns a replacement on it. Called
    /// only once [`Self::request_stop`]'s grace budget has elapsed without a
    /// confirmed stop, or to reconcile a predecessor whose death is unknown.
    /// Must not block past `deadline_unix_micros`.
    fn kill(&self, deadline_unix_micros: i64) -> Result<(), Self::Error>;
}

/// The authoritative identity one supervisor owns for its whole life.
///
/// Bound at construction and never re-read from a request. This is the
/// one-owner exact-scope invariant in type form: a supervisor for one
/// worktree's session cannot be talked into supervising another's.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupervisedScopeV1 {
    provider_id: OwnedProviderId,
    registration_revision: u64,
    exact_scope: OwnedExactScope,
    exact_scope_sha256: String,
    host_limits: ProviderLimits,
    pinned_implementation_identity_sha256: Option<String>,
    pinned_state_schema_version: Option<String>,
    admitted_state_namespace_prefix: Option<String>,
    state_authority: Option<ProviderStateAuthorityV1>,
}

impl SupervisedScopeV1 {
    /// Binds one provider identity, registration revision, exact scope, and
    /// the host's own finite ceilings.
    ///
    /// `host_limits` is the ceiling every negotiated effective limit is
    /// compared against at readiness; a provider may only ever negotiate
    /// *down*.
    pub fn new(
        provider_id: OwnedProviderId,
        registration_revision: u64,
        exact_scope: OwnedExactScope,
        host_limits: ProviderLimits,
    ) -> Result<Self, SupervisorConfigError> {
        if registration_revision == 0 {
            return Err(SupervisorConfigError::InvalidField {
                field: "registration_revision",
            });
        }
        exact_scope
            .validate()
            .map_err(|source| SupervisorConfigError::InvalidScope {
                detail: source.to_string(),
            })?;
        host_limits
            .validate()
            .map_err(|source| SupervisorConfigError::InvalidScope {
                detail: source.to_string(),
            })?;
        let exact_scope_sha256 = exact_scope.exact_scope_sha256();
        Ok(Self {
            provider_id,
            registration_revision,
            exact_scope,
            exact_scope_sha256,
            host_limits,
            pinned_implementation_identity_sha256: None,
            pinned_state_schema_version: None,
            admitted_state_namespace_prefix: None,
            state_authority: None,
        })
    }

    /// Pins the immutable build and state-schema identity the provider must
    /// report at readiness. ADR-0009 requires the reported build and state
    /// identity to be compared with the supervisor's pinned values; a
    /// supervisor with nothing pinned compares nothing, so pinning is how a
    /// topology that knows its executable enforces it.
    #[must_use]
    pub fn with_pinned_identity(
        mut self,
        implementation_identity_sha256: Option<String>,
        state_schema_version: Option<String>,
    ) -> Self {
        self.pinned_implementation_identity_sha256 = implementation_identity_sha256;
        self.pinned_state_schema_version = state_schema_version;
        self
    }

    /// Admits exactly one state-namespace prefix this provider may own.
    ///
    /// A provider reports the provider-local namespace its incarnation loaded
    /// at handshake, and that namespace is what the host then treats as this
    /// provider's state. Without an admitted prefix the only thing stopping a
    /// provider from naming another authority's namespace is the structural
    /// containment rule, which rejects traversal but not a plausible foreign
    /// name. Binding the prefix here makes a foreign claim a fail-closed
    /// [`ReadinessDefectV1::StateNamespaceNotAdmitted`] instead of an accepted
    /// readiness.
    ///
    /// A namespace is admitted when it is the prefix itself or begins with the
    /// prefix followed by `.` or `/`, so `native` never admits `natively`.
    pub fn with_admitted_state_namespace_prefix(
        mut self,
        prefix: &str,
    ) -> Result<Self, SupervisorConfigError> {
        validate_admitted_state_namespace_prefix(prefix)?;
        self.admitted_state_namespace_prefix = Some(prefix.to_owned());
        Ok(self)
    }

    /// Returns the state-namespace prefix this provider is admitted to own,
    /// or `None` when only structural containment is enforced.
    #[must_use]
    pub fn admitted_state_namespace_prefix(&self) -> Option<&str> {
        self.admitted_state_namespace_prefix.as_deref()
    }

    /// Binds the host-owned filesystem authority this provider's state is
    /// contained by.
    ///
    /// Validating the reported namespace proves what the provider *claims*;
    /// this is what the host actually *grants*. With an authority bound, a
    /// validated readiness also mints a
    /// [`ProviderStateCapabilityV1`](crate::state_capability::ProviderStateCapabilityV1)
    /// rooted at the admitted namespace under the host's own root, and that
    /// capability is the only provider state path this crate produces.
    #[must_use]
    pub fn with_state_authority(mut self, authority: ProviderStateAuthorityV1) -> Self {
        self.state_authority = Some(authority);
        self
    }

    /// Returns the host-owned state authority bound to this scope, or `None`
    /// when the host granted none.
    #[must_use]
    pub const fn state_authority(&self) -> Option<&ProviderStateAuthorityV1> {
        self.state_authority.as_ref()
    }

    /// Returns the bound provider identity.
    #[must_use]
    pub const fn provider_id(&self) -> &OwnedProviderId {
        &self.provider_id
    }

    /// Returns the bound registration revision.
    #[must_use]
    pub const fn registration_revision(&self) -> u64 {
        self.registration_revision
    }

    /// Returns the bound exact coding scope.
    #[must_use]
    pub const fn exact_scope(&self) -> &OwnedExactScope {
        &self.exact_scope
    }

    /// Returns the canonical digest of the bound exact coding scope.
    #[must_use]
    pub fn exact_scope_sha256(&self) -> &str {
        &self.exact_scope_sha256
    }

    /// Returns the host ceilings a negotiated effective limit may not exceed.
    #[must_use]
    pub const fn host_limits(&self) -> ProviderLimits {
        self.host_limits
    }

    /// First field of `request` that differs from this binding, or `None`
    /// when the request is for exactly this owner's scope.
    #[must_use]
    fn first_mismatch(&self, request: &HandshakeRequest) -> Option<ScopeFieldV1> {
        if request.provider_id != self.provider_id {
            return Some(ScopeFieldV1::ProviderId);
        }
        if request.registration_revision != self.registration_revision {
            return Some(ScopeFieldV1::RegistrationRevision);
        }
        Self::scope_mismatch(&self.exact_scope, &request.exact_scope)
    }

    /// First differing field between two exact scopes.
    #[must_use]
    fn scope_mismatch(
        expected: &OwnedExactScope,
        actual: &OwnedExactScope,
    ) -> Option<ScopeFieldV1> {
        for (field, expected_value, actual_value) in [
            (
                ScopeFieldV1::ProfileId,
                &expected.profile_id,
                &actual.profile_id,
            ),
            (
                ScopeFieldV1::ProjectId,
                &expected.project_id,
                &actual.project_id,
            ),
            (
                ScopeFieldV1::RepositoryIdentity,
                &expected.repository_identity,
                &actual.repository_identity,
            ),
            (
                ScopeFieldV1::WorktreeIdentity,
                &expected.worktree_identity,
                &actual.worktree_identity,
            ),
            (
                ScopeFieldV1::BranchIdentity,
                &expected.branch_identity,
                &actual.branch_identity,
            ),
            (
                ScopeFieldV1::AgentSessionId,
                &expected.agent_session_id,
                &actual.agent_session_id,
            ),
            (
                ScopeFieldV1::ResolvedScopeDigest,
                &expected.resolved_scope_digest,
                &actual.resolved_scope_digest,
            ),
        ] {
            if expected_value != actual_value {
                return Some(field);
            }
        }
        None
    }
}

/// Which bound identity field a request or response disagreed on.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScopeFieldV1 {
    /// Selected provider identity.
    ProviderId,
    /// Product-owned registration revision.
    RegistrationRevision,
    /// Profile authority identity.
    ProfileId,
    /// Project authority identity.
    ProjectId,
    /// Repository authority identity.
    RepositoryIdentity,
    /// Exact linked-worktree identity.
    WorktreeIdentity,
    /// Exact branch or detached-reference identity.
    BranchIdentity,
    /// Exact coding-agent session identity.
    AgentSessionId,
    /// Canonical resolved-scope digest.
    ResolvedScopeDigest,
}

impl ScopeFieldV1 {
    /// Returns the stable diagnostic name of the field.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProviderId => "provider_id",
            Self::RegistrationRevision => "registration_revision",
            Self::ProfileId => "profile_id",
            Self::ProjectId => "project_id",
            Self::RepositoryIdentity => "repository_identity",
            Self::WorktreeIdentity => "worktree_identity",
            Self::BranchIdentity => "branch_identity",
            Self::AgentSessionId => "agent_session_id",
            Self::ResolvedScopeDigest => "resolved_scope_digest",
        }
    }
}

impl fmt::Display for ScopeFieldV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Finite restart ceiling inside a rolling window, plus the capped
/// exponential the supervisor *enforces* between attempts.
///
/// The two window fields are validated together: a window with no positive
/// length or a ceiling of zero can never let a second attempt happen and
/// exists only to wedge every crash into `RestartBudgetExhausted` forever,
/// which is indistinguishable from a supervisor that gave up on the first
/// crash — so it is refused rather than accepted as an unusually strict
/// policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RestartBudgetV1 {
    /// Maximum spawn attempts counted inside `window_micros`.
    pub max_attempts_per_window: u32,
    /// Rolling window length attempts are counted within.
    pub window_micros: i64,
    /// Delay after the first attempt.
    pub backoff_base_micros: i64,
    /// Ceiling the doubling saturates at.
    pub backoff_max_micros: i64,
}

impl RestartBudgetV1 {
    /// Rejects a budget that cannot admit a second attempt or cannot delay.
    pub fn validate(&self) -> Result<(), SupervisorConfigError> {
        if self.max_attempts_per_window == 0 {
            return Err(SupervisorConfigError::InvalidField {
                field: "max_attempts_per_window",
            });
        }
        if self.window_micros <= 0 {
            return Err(SupervisorConfigError::InvalidField {
                field: "window_micros",
            });
        }
        if self.backoff_base_micros <= 0 {
            return Err(SupervisorConfigError::InvalidField {
                field: "backoff_base_micros",
            });
        }
        if self.backoff_max_micros < self.backoff_base_micros {
            return Err(SupervisorConfigError::InvalidField {
                field: "backoff_max_micros",
            });
        }
        Ok(())
    }

    /// Capped exponential delay for the given one-based attempt number inside
    /// the current window.
    #[must_use]
    pub fn delay_for(&self, attempt_number_in_window: u32) -> i64 {
        let shift = attempt_number_in_window.saturating_sub(1).min(31);
        let multiplier = 1i64.checked_shl(shift).unwrap_or(i64::MAX);
        self.backoff_base_micros
            .checked_mul(multiplier)
            .unwrap_or(i64::MAX)
            .min(self.backoff_max_micros)
    }
}

/// Configuration error for a supervisor's bound scope or budgets.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SupervisorConfigError {
    /// The named field cannot bound anything at the value supplied.
    #[error("provider supervisor field {field} is not a usable bound")]
    InvalidField {
        /// Offending field name.
        field: &'static str,
    },
    /// The bound exact scope or host ceilings are not a valid contract value.
    #[error("provider supervisor scope binding is invalid: {detail}")]
    InvalidScope {
        /// Contract validation detail.
        detail: String,
    },
}

/// Bounded shutdown: how long a graceful stop is given before the supervisor
/// escalates to a forced kill, and how long the kill itself may run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShutdownBudgetV1 {
    /// Grace period handed to [`ProviderLifecycleAdapterV1::request_stop`].
    pub grace_micros: i64,
    /// Additional bound handed to [`ProviderLifecycleAdapterV1::kill`] once
    /// grace has elapsed without a confirmed stop.
    pub kill_micros: i64,
}

impl ShutdownBudgetV1 {
    /// Rejects a budget that cannot bound anything.
    pub fn validate(&self) -> Result<(), SupervisorConfigError> {
        if self.grace_micros <= 0 {
            return Err(SupervisorConfigError::InvalidField {
                field: "grace_micros",
            });
        }
        if self.kill_micros <= 0 {
            return Err(SupervisorConfigError::InvalidField {
                field: "kill_micros",
            });
        }
        Ok(())
    }
}

/// Finite ceiling on how many provider-attributable violations a provider may
/// produce before it is quarantined.
///
/// The restart budget bounds a crash loop *inside* one rolling window and then
/// forgives it: attempts age out and the provider is spawned again. That is
/// the right shape for a provider that fails transiently, and the wrong shape
/// for one that is broken, malicious, or protocol-violating — it would be
/// respawned once per window for the life of the host. This policy is the
/// terminal bound the window ceiling is not: violations are counted **across**
/// windows and never age out, and crossing the ceiling stops every adapter
/// call until a human-driven release.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QuarantinePolicyV1 {
    /// Provider-attributable violations, counted across restart windows, that
    /// quarantine the provider.
    pub max_provider_violations: u32,
}

impl QuarantinePolicyV1 {
    /// The ceiling a supervisor uses when its host pins none.
    ///
    /// Chosen to sit above any single window's attempt ceiling a host is
    /// likely to configure, so a provider that recovers within a window or two
    /// is never quarantined, while one that never reaches a validated
    /// readiness is stopped in bounded time rather than never.
    pub const DEFAULT: Self = Self {
        max_provider_violations: 8,
    };

    /// Rejects a ceiling that cannot bound anything.
    pub const fn validate(&self) -> Result<(), SupervisorConfigError> {
        if self.max_provider_violations == 0 {
            return Err(SupervisorConfigError::InvalidField {
                field: "max_provider_violations",
            });
        }
        Ok(())
    }
}

impl Default for QuarantinePolicyV1 {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// The persisted evidence of why a provider is quarantined.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuarantineRecordV1 {
    violations: u32,
    first_violation: DegradationKindV1,
    last_violation: DegradationKindV1,
    detail: String,
}

impl QuarantineRecordV1 {
    /// Provider-attributable violations counted when quarantine engaged.
    #[must_use]
    pub const fn violations(&self) -> u32 {
        self.violations
    }

    /// First violation kind of the run that led to quarantine.
    #[must_use]
    pub const fn first_violation(&self) -> DegradationKindV1 {
        self.first_violation
    }

    /// Violation kind that crossed the ceiling.
    #[must_use]
    pub const fn last_violation(&self) -> DegradationKindV1 {
        self.last_violation
    }

    /// Detail captured from the violation that crossed the ceiling.
    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for QuarantineRecordV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "quarantined after {} provider violation(s) from {} to {}: {}",
            self.violations, self.first_violation, self.last_violation, self.detail
        )
    }
}

/// Why an explicit quarantine release was refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum QuarantineReleaseError {
    /// The provider is not quarantined, so there is nothing to release.
    #[error("provider is not quarantined")]
    NotQuarantined,
    /// The quarantined instance's death is not confirmed. Releasing now could
    /// leave two owners for one provider namespace, so the caller must run a
    /// bounded shutdown first.
    #[error("provider quarantine cannot be released while the instance's death is unconfirmed")]
    InstanceNotConfirmedDead,
}

/// Live degradation state the supervisor reports instead of ever fabricating
/// readiness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderAvailabilityV1 {
    /// No instance has ever been started, or the most recent one was stopped
    /// and no replacement has been started since.
    NotStarted,
    /// An instance is running but has not yet completed a validated
    /// handshake in its current incarnation.
    Starting,
    /// The current incarnation completed a fully validated handshake and has
    /// not since crashed, failed a handshake, or been stopped.
    Ready,
    /// The current incarnation is degraded. [`ProviderSupervisorV1::current_degradation`]
    /// names why.
    Unavailable,
    /// The provider produced more provider-attributable violations than
    /// [`QuarantinePolicyV1`] admits. No further adapter call is made for this
    /// provider until an operator releases the quarantine explicitly through
    /// [`ProviderSupervisorV1::release_quarantine`].
    Quarantined,
}

/// What the supervisor knows about the instance it most recently asked to
/// start.
///
/// This is the no-overlapping-owners invariant in type form: a replacement is
/// only ever spawned from [`Self::None`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PredecessorStateV1 {
    /// No instance exists: nothing was ever started, or the last one's death
    /// was confirmed.
    None,
    /// An instance may exist. Either a start succeeded, or a start failed
    /// after possibly spawning a child, or a crash was reported without a
    /// death confirmation.
    Live,
    /// A termination was attempted and could not be confirmed. No replacement
    /// is spawned from this state; a later pass must confirm death first.
    DeathUnknown,
}

/// Exactly which success invariant a `Success` handshake failed.
///
/// Every variant is a distinct fail-closed contract violation, never a
/// defaulted value. Missing identity is not an empty identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadinessDefectV1 {
    /// The terminal names a provider other than the bound one.
    ForeignTerminalProvider {
        /// Provider identity the terminal carried.
        terminal_provider_id: String,
    },
    /// The terminal names an operation other than `handshake`.
    ForeignTerminalOperation {
        /// Operation the terminal carried.
        terminal_operation: &'static str,
    },
    /// The terminal's exact-scope digest is not the bound scope's digest.
    TerminalScopeMismatch,
    /// No opaque runtime instance identity was reported.
    MissingInstanceIdentity,
    /// The reported instance identity is empty, oversized, or carries control
    /// characters.
    InvalidInstanceIdentity,
    /// No provider-local state namespace was reported.
    MissingStateNamespace,
    /// The reported state namespace is empty or oversized.
    InvalidStateNamespace,
    /// The reported state namespace is shaped so it could address storage
    /// outside the host-owned namespace root: an absolute path, a parent
    /// traversal, a dotted or empty segment, or a separator the host does not
    /// own. This is refused before any state is bound to the incarnation.
    StateNamespaceEscapesContainment {
        /// Namespace the provider reported, preserved for diagnosis.
        state_namespace: String,
    },
    /// The reported state namespace is well shaped but lies outside the
    /// namespace prefix this provider was admitted to own, so accepting it
    /// would let one provider claim another authority's state.
    StateNamespaceNotAdmitted {
        /// Namespace the provider reported.
        state_namespace: String,
        /// Prefix the supervisor admitted this provider to own.
        admitted_prefix: String,
    },
    /// The reported state namespace was admitted, but the host could not
    /// grant the state capability that contains it, so no state root exists
    /// for this incarnation and readiness is refused rather than claimed
    /// without containment.
    StateCapabilityUnavailable {
        /// Namespace the provider reported.
        state_namespace: String,
        /// Why the grant failed.
        detail: String,
    },
    /// No accepted exact scope was reported.
    MissingAcceptedScope,
    /// The accepted exact scope differs from the bound scope at this field.
    AcceptedScopeMismatch {
        /// The first differing exact-scope field.
        field: ScopeFieldV1,
    },
    /// No ready receipt was reported.
    MissingReadyReceipt,
    /// The ready receipt is not a bare lowercase 64-hex digest.
    InvalidReadyReceipt,
    /// No provider descriptor was reported, so no build, state, or capability
    /// identity exists to verify.
    MissingDescriptor,
    /// The reported descriptor is not a valid contract value.
    InvalidDescriptor {
        /// Contract validation detail.
        detail: String,
    },
    /// The descriptor names a provider other than the bound one.
    DescriptorProviderMismatch {
        /// Provider identity the descriptor carried.
        descriptor_provider_id: String,
    },
    /// The descriptor does not declare a capability the request required.
    MissingRequiredCapability {
        /// Capability the host required.
        capability_id: String,
    },
    /// The descriptor does not declare the mandatory health capability, so
    /// health can never be probed for this incarnation.
    MissingHealthCapability,
    /// The reported immutable build identity is not the pinned one.
    PinnedIdentityMismatch {
        /// Build identity the descriptor reported.
        reported: String,
    },
    /// The reported state-schema identity is not the pinned one.
    PinnedStateSchemaMismatch {
        /// State-schema identity the descriptor reported.
        reported: String,
    },
    /// No negotiated effective limits were reported.
    MissingEffectiveLimits,
    /// The reported effective limits are not a valid contract value.
    InvalidEffectiveLimits {
        /// Contract validation detail.
        detail: String,
    },
    /// A negotiated effective limit is above the host's own ceiling, so the
    /// provider negotiated *up*.
    EffectiveLimitAboveHostCeiling {
        /// Offending limit name.
        limit: &'static str,
    },
    /// The successful response carries more warnings, or a longer warning,
    /// than the host will hold.
    UnboundedWarnings {
        /// Warnings the response carried.
        warnings: usize,
    },
}

impl fmt::Display for ReadinessDefectV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ForeignTerminalProvider {
                terminal_provider_id,
            } => write!(
                formatter,
                "handshake terminal named foreign provider {terminal_provider_id}"
            ),
            Self::ForeignTerminalOperation { terminal_operation } => write!(
                formatter,
                "handshake terminal named foreign operation {terminal_operation}"
            ),
            Self::TerminalScopeMismatch => {
                formatter.write_str("handshake terminal carried a foreign exact-scope digest")
            }
            Self::MissingInstanceIdentity => {
                formatter.write_str("handshake reported no provider instance identity")
            }
            Self::InvalidInstanceIdentity => {
                formatter.write_str("handshake reported an unusable provider instance identity")
            }
            Self::MissingStateNamespace => {
                formatter.write_str("handshake reported no provider state namespace")
            }
            Self::InvalidStateNamespace => {
                formatter.write_str("handshake reported an unusable provider state namespace")
            }
            Self::StateNamespaceEscapesContainment { state_namespace } => write!(
                formatter,
                "handshake reported state namespace {state_namespace}, which can address \
                 storage outside the host-owned namespace root"
            ),
            Self::StateNamespaceNotAdmitted {
                state_namespace,
                admitted_prefix,
            } => write!(
                formatter,
                "handshake reported state namespace {state_namespace}, which is outside the \
                 admitted namespace {admitted_prefix}"
            ),
            Self::StateCapabilityUnavailable {
                state_namespace,
                detail,
            } => write!(
                formatter,
                "host could not grant a contained state capability for namespace \
                 {state_namespace}: {detail}"
            ),
            Self::MissingAcceptedScope => {
                formatter.write_str("handshake reported no accepted exact scope")
            }
            Self::AcceptedScopeMismatch { field } => write!(
                formatter,
                "handshake accepted a different exact scope at {field}"
            ),
            Self::MissingReadyReceipt => formatter.write_str("handshake reported no ready receipt"),
            Self::InvalidReadyReceipt => {
                formatter.write_str("handshake ready receipt is not a lowercase 64-hex digest")
            }
            Self::MissingDescriptor => {
                formatter.write_str("handshake reported no provider descriptor")
            }
            Self::InvalidDescriptor { detail } => {
                write!(formatter, "handshake descriptor is invalid: {detail}")
            }
            Self::DescriptorProviderMismatch {
                descriptor_provider_id,
            } => write!(
                formatter,
                "handshake descriptor named foreign provider {descriptor_provider_id}"
            ),
            Self::MissingRequiredCapability { capability_id } => write!(
                formatter,
                "handshake descriptor does not declare required capability {capability_id}"
            ),
            Self::MissingHealthCapability => formatter
                .write_str("handshake descriptor does not declare the mandatory health capability"),
            Self::PinnedIdentityMismatch { reported } => write!(
                formatter,
                "handshake reported build identity {reported}, which is not the pinned one"
            ),
            Self::PinnedStateSchemaMismatch { reported } => write!(
                formatter,
                "handshake reported state schema {reported}, which is not the pinned one"
            ),
            Self::MissingEffectiveLimits => {
                formatter.write_str("handshake reported no negotiated effective limits")
            }
            Self::InvalidEffectiveLimits { detail } => {
                write!(
                    formatter,
                    "handshake effective limits are invalid: {detail}"
                )
            }
            Self::EffectiveLimitAboveHostCeiling { limit } => write!(
                formatter,
                "handshake negotiated effective limit {limit} above the host ceiling"
            ),
            Self::UnboundedWarnings { warnings } => write!(
                formatter,
                "handshake carried {warnings} warning(s) beyond the host bound"
            ),
        }
    }
}

/// Which adapter call the supervisor was inside.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterOperationV1 {
    /// [`ProviderLifecycleAdapterV1::start`].
    Start,
    /// [`ProviderLifecycleAdapterV1::handshake`].
    Handshake,
    /// [`ProviderLifecycleAdapterV1::request_stop`].
    RequestStop,
    /// [`ProviderLifecycleAdapterV1::kill`].
    Kill,
}

impl AdapterOperationV1 {
    /// Returns the stable diagnostic name of the adapter call.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Handshake => "handshake",
            Self::RequestStop => "request_stop",
            Self::Kill => "kill",
        }
    }
}

impl fmt::Display for AdapterOperationV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Why the current incarnation is `Unavailable`, preserved for diagnosis.
///
/// This is typed degradation evidence, not an error the caller must unwrap:
/// the host stays usable while a supervised provider sits in exactly one of
/// these states.
#[derive(Debug)]
pub enum DegradationCauseV1<E> {
    /// The request's provider, revision, or exact scope is not this
    /// supervisor's binding. No adapter call was made.
    ScopeMismatch {
        /// The first field that differed from the binding.
        field: ScopeFieldV1,
    },
    /// The restart budget's attempt ceiling was reached inside the current
    /// window; no adapter call was made.
    RestartBudgetExhausted {
        /// Attempts already made inside the current window.
        attempts_in_window: u32,
    },
    /// The enforced capped-exponential backoff has not elapsed; no adapter
    /// call was made.
    BackoffNotElapsed {
        /// Earliest instant a further pass may touch the adapter.
        retry_at_unix_micros: i64,
        /// Micros remaining until that instant.
        remaining_micros: i64,
    },
    /// A predecessor instance existed and its bounded termination failed, so
    /// no replacement was spawned.
    PredecessorTerminationFailed(E),
    /// A predecessor's death is still unconfirmed, so no replacement was
    /// spawned.
    PredecessorDeathUnknown,
    /// [`ProviderLifecycleAdapterV1::start`] failed. The instance may or may
    /// not exist; the next pass terminates before spawning.
    StartFailed(E),
    /// [`ProviderLifecycleAdapterV1::handshake`] failed at the transport
    /// level.
    HandshakeTransportFailed(E),
    /// The handshake reached the provider but its terminal was not
    /// [`TerminalCode::Success`].
    HandshakeRefused {
        /// Terminal code the provider returned.
        terminal_code: TerminalCode,
    },
    /// The handshake terminal was `Success` but a required readiness
    /// invariant did not hold, so readiness was refused fail-closed.
    HandshakeContractViolation(ReadinessDefectV1),
    /// An adapter call panicked and was contained by the supervisor's unwind
    /// boundary. The host was not terminated.
    AdapterPanicked {
        /// Adapter call that panicked.
        operation: AdapterOperationV1,
    },
    /// The caller reported the running instance crashed or became
    /// unreachable outside of a handshake attempt (for example a health
    /// probe or an in-flight operation observed the instance gone).
    Crashed,
    /// A crash was reported while the supervisor knows of no live
    /// incarnation: nothing was ever started, or the last instance's death was
    /// already confirmed. This is a **host-side** refusal — no adapter call
    /// was made, no readiness was invalidated, and it is not attributable to
    /// the provider.
    CrashReportWithoutLiveIncarnation,
    /// A crash was already recorded for the live incarnation this report
    /// names. Repeating it changes nothing: one incarnation crashes once. This
    /// is a host-side refusal and is not attributable to the provider.
    CrashAlreadyRecorded {
        /// Incarnation whose crash is already recorded.
        incarnation: u64,
    },
    /// The provider is quarantined. No adapter call was made, and none will be
    /// until [`ProviderSupervisorV1::release_quarantine`] is called
    /// explicitly.
    Quarantined {
        /// Provider-attributable violations counted when quarantine engaged.
        violations: u32,
        /// First violation kind of the run that led to quarantine.
        first_violation: DegradationKindV1,
        /// Violation kind that crossed the ceiling.
        last_violation: DegradationKindV1,
    },
}

impl<E> DegradationCauseV1<E> {
    /// Returns the payload-free kind of this cause, which is what the
    /// supervisor persists and reports through
    /// [`ProviderSupervisorV1::current_degradation`].
    #[must_use]
    pub const fn kind(&self) -> DegradationKindV1 {
        match self {
            Self::ScopeMismatch { .. } => DegradationKindV1::ScopeMismatch,
            Self::RestartBudgetExhausted { .. } => DegradationKindV1::RestartBudgetExhausted,
            Self::BackoffNotElapsed { .. } => DegradationKindV1::BackoffNotElapsed,
            Self::PredecessorTerminationFailed(_) => {
                DegradationKindV1::PredecessorTerminationFailed
            }
            Self::PredecessorDeathUnknown => DegradationKindV1::PredecessorDeathUnknown,
            Self::StartFailed(_) => DegradationKindV1::StartFailed,
            Self::HandshakeTransportFailed(_) => DegradationKindV1::HandshakeTransportFailed,
            Self::HandshakeRefused { .. } => DegradationKindV1::HandshakeRefused,
            Self::HandshakeContractViolation(_) => DegradationKindV1::HandshakeContractViolation,
            Self::AdapterPanicked { .. } => DegradationKindV1::AdapterPanicked,
            Self::Crashed => DegradationKindV1::Crashed,
            Self::CrashReportWithoutLiveIncarnation => {
                DegradationKindV1::CrashReportWithoutLiveIncarnation
            }
            Self::CrashAlreadyRecorded { .. } => DegradationKindV1::CrashAlreadyRecorded,
            Self::Quarantined { .. } => DegradationKindV1::Quarantined,
        }
    }
}

impl<E: fmt::Display> fmt::Display for DegradationCauseV1<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ScopeMismatch { field } => write!(
                formatter,
                "handshake request is for a different exact scope at {field}"
            ),
            Self::RestartBudgetExhausted { attempts_in_window } => write!(
                formatter,
                "provider restart budget exhausted after {attempts_in_window} attempt(s) in \
                 the current window"
            ),
            Self::BackoffNotElapsed {
                retry_at_unix_micros,
                remaining_micros,
            } => write!(
                formatter,
                "provider restart backoff has not elapsed: {remaining_micros}us remain until \
                 {retry_at_unix_micros}"
            ),
            Self::PredecessorTerminationFailed(cause) => write!(
                formatter,
                "predecessor provider instance could not be terminated: {cause}"
            ),
            Self::PredecessorDeathUnknown => formatter
                .write_str("predecessor provider instance death is unconfirmed; no replacement"),
            Self::StartFailed(cause) => write!(formatter, "provider start failed: {cause}"),
            Self::HandshakeTransportFailed(cause) => {
                write!(formatter, "provider handshake transport failed: {cause}")
            }
            Self::HandshakeRefused { terminal_code } => write!(
                formatter,
                "provider handshake terminated with {}",
                terminal_code.as_wire()
            ),
            Self::HandshakeContractViolation(defect) => write!(
                formatter,
                "provider handshake reported success but violated the readiness contract: \
                 {defect}"
            ),
            Self::AdapterPanicked { operation } => write!(
                formatter,
                "provider lifecycle adapter panicked inside {operation} and was contained"
            ),
            Self::Crashed => formatter.write_str("provider instance crashed"),
            Self::CrashReportWithoutLiveIncarnation => formatter.write_str(
                "crash report refused: the supervisor knows of no live provider incarnation",
            ),
            Self::CrashAlreadyRecorded { incarnation } => write!(
                formatter,
                "crash report refused: incarnation {incarnation} already has a recorded crash"
            ),
            Self::Quarantined {
                violations,
                first_violation,
                last_violation,
            } => write!(
                formatter,
                "provider is quarantined after {violations} provider violation(s) from \
                 {first_violation} to {last_violation}; no adapter call was made"
            ),
        }
    }
}

/// Payload-free kind of a [`DegradationCauseV1`], suitable for persisting and
/// for a host to branch on without owning the adapter's error type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DegradationKindV1 {
    /// [`DegradationCauseV1::ScopeMismatch`].
    ScopeMismatch,
    /// [`DegradationCauseV1::RestartBudgetExhausted`].
    RestartBudgetExhausted,
    /// [`DegradationCauseV1::BackoffNotElapsed`].
    BackoffNotElapsed,
    /// [`DegradationCauseV1::PredecessorTerminationFailed`].
    PredecessorTerminationFailed,
    /// [`DegradationCauseV1::PredecessorDeathUnknown`].
    PredecessorDeathUnknown,
    /// [`DegradationCauseV1::StartFailed`].
    StartFailed,
    /// [`DegradationCauseV1::HandshakeTransportFailed`].
    HandshakeTransportFailed,
    /// [`DegradationCauseV1::HandshakeRefused`].
    HandshakeRefused,
    /// [`DegradationCauseV1::HandshakeContractViolation`].
    HandshakeContractViolation,
    /// [`DegradationCauseV1::AdapterPanicked`].
    AdapterPanicked,
    /// [`DegradationCauseV1::Crashed`].
    Crashed,
    /// [`DegradationCauseV1::CrashReportWithoutLiveIncarnation`].
    CrashReportWithoutLiveIncarnation,
    /// [`DegradationCauseV1::CrashAlreadyRecorded`].
    CrashAlreadyRecorded,
    /// [`DegradationCauseV1::Quarantined`].
    Quarantined,
}

impl DegradationKindV1 {
    /// Returns the stable diagnostic name of the kind.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ScopeMismatch => "scope_mismatch",
            Self::RestartBudgetExhausted => "restart_budget_exhausted",
            Self::BackoffNotElapsed => "backoff_not_elapsed",
            Self::PredecessorTerminationFailed => "predecessor_termination_failed",
            Self::PredecessorDeathUnknown => "predecessor_death_unknown",
            Self::StartFailed => "start_failed",
            Self::HandshakeTransportFailed => "handshake_transport_failed",
            Self::HandshakeRefused => "handshake_refused",
            Self::HandshakeContractViolation => "handshake_contract_violation",
            Self::AdapterPanicked => "adapter_panicked",
            Self::Crashed => "crashed",
            Self::CrashReportWithoutLiveIncarnation => "crash_report_without_live_incarnation",
            Self::CrashAlreadyRecorded => "crash_already_recorded",
            Self::Quarantined => "quarantined",
        }
    }

    /// Whether this degradation is attributable to the provider itself rather
    /// than to the host's own refusal to touch it.
    ///
    /// Only provider-attributable degradations count toward
    /// [`QuarantinePolicyV1`]. A scope mismatch, an exhausted restart budget,
    /// a backoff that has not elapsed, a refused crash report, and quarantine
    /// itself all make **no** adapter call, so counting them would let a
    /// caller loop the host into quarantining a provider that never
    /// misbehaved.
    #[must_use]
    pub const fn is_provider_attributable(self) -> bool {
        match self {
            Self::ScopeMismatch
            | Self::RestartBudgetExhausted
            | Self::BackoffNotElapsed
            | Self::CrashReportWithoutLiveIncarnation
            | Self::CrashAlreadyRecorded
            | Self::Quarantined => false,
            Self::PredecessorTerminationFailed
            | Self::PredecessorDeathUnknown
            | Self::StartFailed
            | Self::HandshakeTransportFailed
            | Self::HandshakeRefused
            | Self::HandshakeContractViolation
            | Self::AdapterPanicked
            | Self::Crashed => true,
        }
    }
}

impl fmt::Display for DegradationKindV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The persisted, adapter-error-free record of the current degradation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DegradationRecordV1 {
    kind: DegradationKindV1,
    detail: String,
}

impl DegradationRecordV1 {
    /// Returns the typed kind of the current degradation.
    #[must_use]
    pub const fn kind(&self) -> DegradationKindV1 {
        self.kind
    }

    /// Returns the human-readable detail captured when it was recorded.
    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for DegradationRecordV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.kind, self.detail)
    }
}

/// Validated readiness evidence for one incarnation.
///
/// A value of this type is proof that every success invariant of the
/// handshake contract held: it cannot be constructed from a
/// [`HandshakeResponse`] any other way than through the supervisor's
/// validator, and it holds no `Option` a caller has to default.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadinessEvidenceV1 {
    provider_instance_id: String,
    state_namespace: String,
    state_capability: Option<ProviderStateCapabilityV1>,
    ready_receipt_sha256: String,
    implementation_identity_sha256: String,
    state_schema_version: String,
    state_generation: u64,
    retains_replay_position: bool,
    effective_limits: ProviderLimits,
}

impl ReadinessEvidenceV1 {
    /// Returns the provider-reported opaque runtime instance identity.
    #[must_use]
    pub fn provider_instance_id(&self) -> &str {
        &self.provider_instance_id
    }

    /// Returns the provider-local state namespace this incarnation owns.
    #[must_use]
    pub fn state_namespace(&self) -> &str {
        &self.state_namespace
    }

    /// Returns the host-granted state capability that contains this
    /// incarnation's state, or `None` when the host bound no state authority
    /// to the scope.
    ///
    /// This is the only provider state path this crate produces: it is rooted
    /// at the host's own directory, resolved from the namespace the readiness
    /// validator admitted, and every path it hands out is proven to stay
    /// under that root.
    #[must_use]
    pub const fn state_capability(&self) -> Option<&ProviderStateCapabilityV1> {
        self.state_capability.as_ref()
    }

    /// Returns the bare lowercase 64-hex ready-receipt digest.
    #[must_use]
    pub fn ready_receipt_sha256(&self) -> &str {
        &self.ready_receipt_sha256
    }

    /// Returns the immutable implementation identity this incarnation
    /// reported.
    #[must_use]
    pub fn implementation_identity_sha256(&self) -> &str {
        &self.implementation_identity_sha256
    }

    /// Returns the provider-local state-schema identity.
    #[must_use]
    pub fn state_schema_version(&self) -> &str {
        &self.state_schema_version
    }

    /// Returns the provider-local state generation.
    #[must_use]
    pub const fn state_generation(&self) -> u64 {
        self.state_generation
    }

    /// Whether this incarnation declares the replay capability, and therefore
    /// keeps a provider-local acknowledged position a host may compare against
    /// its own durable watermark.
    ///
    /// This is validated evidence, not a host guess: it is read from the same
    /// descriptor the handshake proved, so a caller can tell "this provider
    /// keeps no replay position" apart from "nobody asked".
    #[must_use]
    pub const fn retains_replay_position(&self) -> bool {
        self.retains_replay_position
    }

    /// Returns the negotiated effective ceilings, each already proven to be
    /// at or below the host's own.
    #[must_use]
    pub const fn effective_limits(&self) -> ProviderLimits {
        self.effective_limits
    }
}

/// One bounded readiness re-proof's outcome.
///
/// Re-proving is the steady-state path: the current incarnation is already
/// `Ready` and the host wants a fresh receipt for it. It is deliberately not
/// a restart — it consumes no restart attempt, arms no backoff, spawns
/// nothing, and terminates nothing — so a healthy provider answering many
/// requests can never exhaust the budget that exists to bound crash loops.
#[derive(Debug)]
pub enum ReproveOutcomeV1<E> {
    /// The supervisor is not currently `Ready`, so there is no incarnation to
    /// re-prove. No adapter call was made; the caller must go through
    /// [`ProviderSupervisorV1::start_or_restart`].
    NotReady,
    /// The current incarnation answered a fully validated handshake again.
    Ready(ReadinessEvidenceV1),
    /// The current incarnation lost its readiness for the given cause. The
    /// instance is left possibly-live, so the next replacement confirms death
    /// before it spawns.
    Unavailable(DegradationCauseV1<E>),
}

/// One bounded start-or-restart attempt's outcome.
#[derive(Debug)]
pub enum SupervisorOutcomeV1<E> {
    /// The instance is now `Ready` behind a fully validated handshake.
    Ready(ReadinessEvidenceV1),
    /// The instance is `Unavailable` for the given cause. The host remains
    /// usable; no readiness is claimed.
    Unavailable(DegradationCauseV1<E>),
}

/// Supervises exactly one provider instance's lifecycle, for exactly one
/// exact coding scope, across restarts.
///
/// Not `Send`/`Sync` by construction requirement — a caller that needs shared
/// access wraps this behind its own mutex, exactly as every other stateful
/// runtime in this program is composed. The type owns no background thread;
/// every transition is driven by an explicit caller call with an explicit
/// clock reading, so it is deterministic under test.
pub struct ProviderSupervisorV1<A: ProviderLifecycleAdapterV1> {
    adapter: A,
    scope: SupervisedScopeV1,
    restart_budget: RestartBudgetV1,
    shutdown_budget: ShutdownBudgetV1,
    availability: ProviderAvailabilityV1,
    predecessor: PredecessorStateV1,
    /// Unix-micros instants of spawn attempts still inside the rolling
    /// window, oldest first. Bounded by `restart_budget.max_attempts_per_window`
    /// entries by construction: an attempt beyond the ceiling is refused
    /// before it is recorded here.
    attempts_in_window: Vec<i64>,
    /// Earliest instant a further pass may touch the adapter. Armed on every
    /// pass that does, cleared on a validated readiness.
    next_eligible_unix_micros: Option<i64>,
    degradation: Option<DegradationRecordV1>,
    readiness: Option<ReadinessEvidenceV1>,
    quarantine_policy: QuarantinePolicyV1,
    /// Provider-attributable violations since the last validated readiness or
    /// explicit release. Deliberately **not** pruned by the restart window:
    /// this is the counter that makes a cross-window crash loop terminal.
    provider_violations: u32,
    first_violation: Option<DegradationKindV1>,
    quarantine: Option<QuarantineRecordV1>,
    /// Monotonic identity of provider instances this supervisor has asked to
    /// start. Never reused, so a crash report can be bound to exactly the
    /// incarnation that was live when it was observed.
    incarnations_started: u64,
    /// The incarnation the supervisor believes may still be alive, or `None`
    /// when nothing was ever started or the last death was confirmed.
    live_incarnation: Option<u64>,
    /// Incarnation whose crash is already recorded, so a duplicate report is
    /// refused instead of counted a second time.
    crash_reported_incarnation: Option<u64>,
}

impl<A: ProviderLifecycleAdapterV1> ProviderSupervisorV1<A> {
    /// Binds one adapter to one exact scope under a validated restart and
    /// shutdown budget. `NotStarted` is the only initial state — a supervisor
    /// is never constructed already claiming readiness.
    pub fn new(
        adapter: A,
        scope: SupervisedScopeV1,
        restart_budget: RestartBudgetV1,
        shutdown_budget: ShutdownBudgetV1,
    ) -> Result<Self, SupervisorConfigError> {
        restart_budget.validate()?;
        shutdown_budget.validate()?;
        Ok(Self {
            adapter,
            scope,
            restart_budget,
            shutdown_budget,
            availability: ProviderAvailabilityV1::NotStarted,
            predecessor: PredecessorStateV1::None,
            attempts_in_window: Vec::new(),
            next_eligible_unix_micros: None,
            degradation: None,
            readiness: None,
            quarantine_policy: QuarantinePolicyV1::DEFAULT,
            provider_violations: 0,
            first_violation: None,
            quarantine: None,
            incarnations_started: 0,
            live_incarnation: None,
            crash_reported_incarnation: None,
        })
    }

    /// Pins the quarantine ceiling this supervisor enforces, replacing
    /// [`QuarantinePolicyV1::DEFAULT`].
    pub fn with_quarantine_policy(
        mut self,
        policy: QuarantinePolicyV1,
    ) -> Result<Self, SupervisorConfigError> {
        policy.validate()?;
        self.quarantine_policy = policy;
        Ok(self)
    }

    /// Restores a quarantine this provider scope already earned, so a
    /// supervisor rebuilt for the same scope starts quarantined instead of
    /// forgiven.
    ///
    /// A supervisor is a live object with a finite lifetime; the evidence that
    /// a provider scope is quarantined must outlive it, or a host could clear
    /// a quarantine simply by retiring and recreating the owner. The restored
    /// supervisor makes no adapter call and needs the same explicit release
    /// as the one that earned the quarantine.
    #[must_use]
    pub fn with_restored_quarantine(mut self, record: QuarantineRecordV1) -> Self {
        self.provider_violations = record.violations();
        self.first_violation = Some(record.first_violation());
        self.availability = ProviderAvailabilityV1::Quarantined;
        self.degradation = Some(DegradationRecordV1 {
            kind: DegradationKindV1::Quarantined,
            detail: record.to_string(),
        });
        self.quarantine = Some(record);
        self
    }

    /// The quarantine ceiling this supervisor enforces.
    #[must_use]
    pub const fn quarantine_policy(&self) -> QuarantinePolicyV1 {
        self.quarantine_policy
    }

    /// The persisted quarantine record, or `None` while the provider is not
    /// quarantined. Makes no adapter call.
    #[must_use]
    pub fn quarantine(&self) -> Option<&QuarantineRecordV1> {
        self.quarantine.as_ref()
    }

    /// Whether the provider is quarantined. Makes no adapter call.
    #[must_use]
    pub const fn is_quarantined(&self) -> bool {
        self.quarantine.is_some()
    }

    /// Provider-attributable violations counted since the last validated
    /// readiness or explicit release.
    #[must_use]
    pub const fn provider_violations(&self) -> u32 {
        self.provider_violations
    }

    /// Releases a quarantine explicitly, returning the record that is being
    /// cleared so the release is auditable.
    ///
    /// Release is never automatic and never time-based: a quarantine exists
    /// because the provider produced more violations than the host will
    /// tolerate, and only an operator decision — a new build, a repaired
    /// state, a deliberate retry — makes another attempt worth making. The
    /// release is refused while the quarantined instance's death is
    /// unconfirmed, because spawning a replacement over a live malicious
    /// instance is exactly what supervision exists to prevent; run
    /// [`Self::shutdown`] first.
    pub fn release_quarantine(&mut self) -> Result<QuarantineRecordV1, QuarantineReleaseError> {
        if self.predecessor != PredecessorStateV1::None {
            return Err(QuarantineReleaseError::InstanceNotConfirmedDead);
        }
        let record = self
            .quarantine
            .take()
            .ok_or(QuarantineReleaseError::NotQuarantined)?;
        self.provider_violations = 0;
        self.first_violation = None;
        self.attempts_in_window.clear();
        self.next_eligible_unix_micros = None;
        self.degradation = None;
        self.readiness = None;
        self.live_incarnation = None;
        self.crash_reported_incarnation = None;
        self.availability = ProviderAvailabilityV1::NotStarted;
        Ok(record)
    }

    /// Returns the typed quarantine refusal when the provider is quarantined,
    /// persisting it as the current degradation. Makes no adapter call.
    fn quarantined_cause(&mut self) -> Option<DegradationCauseV1<A::Error>> {
        let record = self.quarantine.as_ref()?;
        let (violations, first_violation, last_violation) = (
            record.violations,
            record.first_violation,
            record.last_violation,
        );
        let cause = DegradationCauseV1::Quarantined {
            violations,
            first_violation,
            last_violation,
        };
        self.readiness = None;
        self.availability = ProviderAvailabilityV1::Quarantined;
        self.degradation = Some(DegradationRecordV1 {
            kind: DegradationKindV1::Quarantined,
            detail: cause.to_string(),
        });
        Some(cause)
    }

    /// Current degradation state without making any adapter call.
    #[must_use]
    pub const fn current_availability(&self) -> ProviderAvailabilityV1 {
        self.availability
    }

    /// The current typed degradation record, or `None` when nothing is
    /// currently degraded. Makes no adapter call.
    #[must_use]
    pub fn current_degradation(&self) -> Option<&DegradationRecordV1> {
        self.degradation.as_ref()
    }

    /// What the supervisor knows about the instance it most recently asked to
    /// start.
    #[must_use]
    pub const fn predecessor_state(&self) -> PredecessorStateV1 {
        self.predecessor
    }

    /// The exact scope and provider identity this supervisor owns.
    #[must_use]
    pub const fn scope(&self) -> &SupervisedScopeV1 {
        &self.scope
    }

    /// Borrows the bound adapter. Exposed so a host or test can inspect the
    /// concrete adapter's own diagnostics; the supervisor never uses this
    /// itself to bypass its own bounded transitions.
    #[must_use]
    pub const fn adapter(&self) -> &A {
        &self.adapter
    }

    /// Validated readiness evidence of the current `Ready` incarnation, or
    /// `None` when not currently ready. Never carried across a restart:
    /// [`Self::start_or_restart`] clears it before attempting the fresh
    /// handshake that would repopulate it.
    #[must_use]
    pub fn ready_evidence(&self) -> Option<&ReadinessEvidenceV1> {
        self.readiness.as_ref()
    }

    /// Opaque runtime instance identity of the current `Ready` incarnation,
    /// or `None` when not currently ready.
    #[must_use]
    pub fn ready_provider_instance_id(&self) -> Option<&str> {
        self.readiness
            .as_ref()
            .map(ReadinessEvidenceV1::provider_instance_id)
    }

    /// Reports a crash or unreachability observed outside a handshake
    /// attempt (a health probe, an in-flight operation).
    ///
    /// The report is accepted only **for the live incarnation the supervisor
    /// actually started**, and only once for it. A report while nothing is
    /// live ([`DegradationCauseV1::CrashReportWithoutLiveIncarnation`]) and a
    /// repeat of a crash already recorded for that incarnation
    /// ([`DegradationCauseV1::CrashAlreadyRecorded`]) are host-side refusals:
    /// they change no state, invalidate no readiness, and — because they are
    /// not provider-attributable — count nothing toward
    /// [`QuarantinePolicyV1`]. That is what stops a looping host caller from
    /// quarantining a provider that was never started or never misbehaved.
    ///
    /// An accepted report immediately invalidates any `Ready` claim, persists
    /// [`DegradationKindV1::Crashed`], and returns the typed
    /// [`DegradationCauseV1::Crashed`] outcome. The instance is *not* assumed
    /// dead: the predecessor stays [`PredecessorStateV1::Live`] so the next
    /// replacement confirms death first.
    pub fn report_crash(&mut self) -> SupervisorOutcomeV1<A::Error> {
        if let Some(cause) = self.quarantined_cause() {
            return SupervisorOutcomeV1::Unavailable(cause);
        }
        let Some(incarnation) = self.live_incarnation else {
            return SupervisorOutcomeV1::Unavailable(
                DegradationCauseV1::CrashReportWithoutLiveIncarnation,
            );
        };
        if self.crash_reported_incarnation == Some(incarnation) {
            return SupervisorOutcomeV1::Unavailable(DegradationCauseV1::CrashAlreadyRecorded {
                incarnation,
            });
        }
        self.crash_reported_incarnation = Some(incarnation);
        self.readiness = None;
        if self.predecessor == PredecessorStateV1::None {
            self.predecessor = PredecessorStateV1::Live;
        }
        self.degrade(DegradationCauseV1::Crashed)
    }

    /// The live provider incarnation identity, or `None` when the supervisor
    /// knows of no instance that could still be alive. Makes no adapter call.
    #[must_use]
    pub const fn live_incarnation(&self) -> Option<u64> {
        self.live_incarnation
    }

    /// Records that no provider incarnation can still be alive: the death of
    /// the last one was confirmed. A crash report after this point names no
    /// incarnation and is refused host-side.
    fn forget_live_incarnation(&mut self) {
        self.predecessor = PredecessorStateV1::None;
        self.live_incarnation = None;
        self.crash_reported_incarnation = None;
    }

    /// Prunes attempts that have fallen outside the rolling window as of
    /// `now_unix_micros`.
    fn prune_window(&mut self, now_unix_micros: i64) {
        let window_start = now_unix_micros.saturating_sub(self.restart_budget.window_micros);
        self.attempts_in_window
            .retain(|instant| *instant > window_start);
    }

    /// Records `cause` as the current degradation, moves availability to
    /// `Unavailable`, and returns the typed outcome carrying the cause whole.
    fn degrade(&mut self, cause: DegradationCauseV1<A::Error>) -> SupervisorOutcomeV1<A::Error> {
        let kind = cause.kind();
        let detail = cause.to_string();
        self.degradation = Some(DegradationRecordV1 {
            kind,
            detail: detail.clone(),
        });
        if kind.is_provider_attributable() {
            self.provider_violations = self.provider_violations.saturating_add(1);
            let first_violation = *self.first_violation.get_or_insert(kind);
            if self.quarantine.is_none()
                && self.provider_violations >= self.quarantine_policy.max_provider_violations
            {
                self.quarantine = Some(QuarantineRecordV1 {
                    violations: self.provider_violations,
                    first_violation,
                    last_violation: kind,
                    detail,
                });
            }
        }
        // The pass that crosses the ceiling still reports the violation it
        // actually observed — that is the diagnosis — while the state it
        // leaves behind is `Quarantined`, so the next pass makes no adapter
        // call at all.
        self.availability = if self.quarantine.is_some() {
            ProviderAvailabilityV1::Quarantined
        } else {
            ProviderAvailabilityV1::Unavailable
        };
        SupervisorOutcomeV1::Unavailable(cause)
    }

    /// Clears the violation run after a fully validated readiness.
    fn clear_violations(&mut self) {
        self.provider_violations = 0;
        self.first_violation = None;
    }

    /// Runs one adapter call inside the supervisor's unwind boundary.
    fn guarded<T>(
        operation: AdapterOperationV1,
        call: impl FnOnce() -> T,
    ) -> Result<T, AdapterOperationV1> {
        catch_unwind(AssertUnwindSafe(call)).map_err(|_| operation)
    }

    /// Confirms the predecessor instance is dead, escalating from a graceful
    /// stop to a forced kill inside the shutdown budget.
    ///
    /// `Ok(())` means no instance exists any more, which is the only state a
    /// replacement is ever spawned from. Every failure path leaves
    /// [`PredecessorStateV1::DeathUnknown`].
    fn confirm_predecessor_death(
        &mut self,
        now_unix_micros: i64,
    ) -> Result<bool, DegradationCauseV1<A::Error>> {
        let entered_unknown = match self.predecessor {
            PredecessorStateV1::None => return Ok(false),
            PredecessorStateV1::Live => false,
            PredecessorStateV1::DeathUnknown => true,
        };
        let grace_deadline = now_unix_micros.saturating_add(self.shutdown_budget.grace_micros);
        let kill_deadline = grace_deadline.saturating_add(self.shutdown_budget.kill_micros);

        // A predecessor whose death is already unknown skips the graceful
        // request: the graceful path is what failed to prove anything, and
        // repeating it would let an unreachable instance stall every pass.
        if !entered_unknown {
            match Self::guarded(AdapterOperationV1::RequestStop, || {
                self.adapter.request_stop(grace_deadline)
            }) {
                Err(operation) => {
                    self.predecessor = PredecessorStateV1::DeathUnknown;
                    return Err(DegradationCauseV1::AdapterPanicked { operation });
                }
                Ok(Err(cause)) => {
                    self.predecessor = PredecessorStateV1::DeathUnknown;
                    return Err(DegradationCauseV1::PredecessorTerminationFailed(cause));
                }
                Ok(Ok(true)) => {
                    self.forget_live_incarnation();
                    return Ok(false);
                }
                Ok(Ok(false)) => {}
            }
        }

        match Self::guarded(AdapterOperationV1::Kill, || {
            self.adapter.kill(kill_deadline)
        }) {
            Err(operation) => {
                self.predecessor = PredecessorStateV1::DeathUnknown;
                Err(DegradationCauseV1::AdapterPanicked { operation })
            }
            Ok(Err(cause)) => {
                self.predecessor = PredecessorStateV1::DeathUnknown;
                if entered_unknown {
                    Err(DegradationCauseV1::PredecessorDeathUnknown)
                } else {
                    Err(DegradationCauseV1::PredecessorTerminationFailed(cause))
                }
            }
            Ok(Ok(())) => {
                self.forget_live_incarnation();
                Ok(true)
            }
        }
    }

    /// One bounded start-or-restart pass at `now_unix_micros`.
    ///
    /// The pass refuses, **without contacting the adapter at all**, when:
    ///
    /// * the request is not for this supervisor's bound provider, revision,
    ///   and exact scope ([`DegradationCauseV1::ScopeMismatch`]);
    /// * the restart budget's attempt ceiling is already reached inside the
    ///   rolling window ([`DegradationCauseV1::RestartBudgetExhausted`]);
    /// * the enforced capped-exponential backoff has not elapsed
    ///   ([`DegradationCauseV1::BackoffNotElapsed`]).
    ///
    /// Otherwise it confirms any predecessor's death first, and only then
    /// consumes one attempt and spawns. A successful start still requires a
    /// fully validated handshake before the instance is `Ready`; a start that
    /// succeeds but a handshake that fails, refuses, or violates the
    /// readiness contract still consumes that attempt, because the instance
    /// it started is one this window is now responsible for.
    pub fn start_or_restart(
        &mut self,
        request: &HandshakeRequest,
        now_unix_micros: i64,
        start_deadline_unix_micros: i64,
        handshake_deadline_unix_micros: i64,
    ) -> SupervisorOutcomeV1<A::Error> {
        // Quarantine is checked before anything else, including the scope
        // binding: a quarantined provider must produce exactly one answer for
        // every caller, and no caller — not even one presenting a foreign
        // scope — may move it out of that state.
        if let Some(cause) = self.quarantined_cause() {
            return SupervisorOutcomeV1::Unavailable(cause);
        }
        if let Some(field) = self.scope.first_mismatch(request) {
            self.readiness = None;
            return self.degrade(DegradationCauseV1::ScopeMismatch { field });
        }

        self.prune_window(now_unix_micros);
        if self.attempts_in_window.len() >= self.restart_budget.max_attempts_per_window as usize {
            self.readiness = None;
            let attempts_in_window =
                u32::try_from(self.attempts_in_window.len()).unwrap_or(u32::MAX);
            return self.degrade(DegradationCauseV1::RestartBudgetExhausted { attempts_in_window });
        }

        if let Some(retry_at_unix_micros) = self.next_eligible_unix_micros
            && now_unix_micros < retry_at_unix_micros
        {
            self.readiness = None;
            let remaining_micros = retry_at_unix_micros.saturating_sub(now_unix_micros);
            return self.degrade(DegradationCauseV1::BackoffNotElapsed {
                retry_at_unix_micros,
                remaining_micros,
            });
        }

        // Arm the next-eligible instant before any adapter call, so a pass
        // that panics, hangs its adapter, or fails termination still paces
        // the next one. `attempts_in_window.len() + 1` is the attempt number
        // this pass is about to become.
        let attempt_number =
            u32::try_from(self.attempts_in_window.len().saturating_add(1)).unwrap_or(u32::MAX);
        self.next_eligible_unix_micros =
            Some(now_unix_micros.saturating_add(self.restart_budget.delay_for(attempt_number)));
        self.readiness = None;

        if let Err(cause) = self.confirm_predecessor_death(now_unix_micros) {
            return self.degrade(cause);
        }

        self.attempts_in_window.push(now_unix_micros);
        self.availability = ProviderAvailabilityV1::Starting;
        // The incarnation exists from the moment the adapter is asked to
        // start it: a start that fails may still have spawned a child, and a
        // crash reported against that child must be attributable.
        self.incarnations_started = self.incarnations_started.saturating_add(1);
        self.live_incarnation = Some(self.incarnations_started);
        self.crash_reported_incarnation = None;

        match Self::guarded(AdapterOperationV1::Start, || {
            self.adapter.start(start_deadline_unix_micros)
        }) {
            Err(operation) => {
                // A panic inside start says nothing about whether a child was
                // spawned, so the instance is possibly live.
                self.predecessor = PredecessorStateV1::Live;
                return self.degrade(DegradationCauseV1::AdapterPanicked { operation });
            }
            Ok(Err(cause)) => {
                // Neither does an error: the child may have spawned before
                // the adapter's own failure. Treat it as possibly live so the
                // next pass terminates before spawning a second owner.
                self.predecessor = PredecessorStateV1::Live;
                return self.degrade(DegradationCauseV1::StartFailed(cause));
            }
            Ok(Ok(())) => {
                self.predecessor = PredecessorStateV1::Live;
            }
        }

        let response = match Self::guarded(AdapterOperationV1::Handshake, || {
            self.adapter
                .handshake(request, handshake_deadline_unix_micros)
        }) {
            Err(operation) => {
                return self.degrade(DegradationCauseV1::AdapterPanicked { operation });
            }
            Ok(Err(cause)) => {
                return self.degrade(DegradationCauseV1::HandshakeTransportFailed(cause));
            }
            Ok(Ok(response)) => response,
        };

        let terminal_code = response.terminal.terminal_code();
        if terminal_code != TerminalCode::Success {
            return self.degrade(DegradationCauseV1::HandshakeRefused { terminal_code });
        }

        match validate_readiness(&self.scope, request, response) {
            Err(defect) => self.degrade(DegradationCauseV1::HandshakeContractViolation(defect)),
            Ok(evidence) => {
                self.availability = ProviderAvailabilityV1::Ready;
                self.degradation = None;
                self.clear_violations();
                // A validated readiness is the only thing that clears the
                // enforced pacing: a healthy incarnation should not be made
                // to wait out a backoff it never earned.
                self.next_eligible_unix_micros = None;
                self.readiness = Some(evidence.clone());
                SupervisorOutcomeV1::Ready(evidence)
            }
        }
    }

    /// Re-proves the readiness of the incarnation that is already `Ready`,
    /// with one handshake and nothing else.
    ///
    /// This exists because a readiness *re-proof* is not a *restart*. A host
    /// that proves readiness per request would otherwise spend its whole
    /// crash-loop budget on a perfectly healthy provider. Admitted only while
    /// [`ProviderAvailabilityV1::Ready`]; every other state returns
    /// [`ReproveOutcomeV1::NotReady`] without touching the adapter, and a
    /// request outside the bound scope is refused typed, also without
    /// touching the adapter.
    ///
    /// A re-proof that fails invalidates readiness and persists the typed
    /// degradation, leaving the instance possibly-live so the next
    /// [`Self::start_or_restart`] confirms its death before spawning.
    pub fn reprove_readiness(
        &mut self,
        request: &HandshakeRequest,
        handshake_deadline_unix_micros: i64,
    ) -> ReproveOutcomeV1<A::Error> {
        if let Some(cause) = self.quarantined_cause() {
            return ReproveOutcomeV1::Unavailable(cause);
        }
        if let Some(field) = self.scope.first_mismatch(request) {
            self.readiness = None;
            self.availability = ProviderAvailabilityV1::Unavailable;
            let cause = DegradationCauseV1::ScopeMismatch { field };
            self.degradation = Some(DegradationRecordV1 {
                kind: cause.kind(),
                detail: cause.to_string(),
            });
            return ReproveOutcomeV1::Unavailable(cause);
        }
        if self.availability != ProviderAvailabilityV1::Ready {
            return ReproveOutcomeV1::NotReady;
        }

        let response = match Self::guarded(AdapterOperationV1::Handshake, || {
            self.adapter
                .handshake(request, handshake_deadline_unix_micros)
        }) {
            Err(operation) => {
                self.readiness = None;
                return match self.degrade(DegradationCauseV1::AdapterPanicked { operation }) {
                    SupervisorOutcomeV1::Unavailable(cause) => ReproveOutcomeV1::Unavailable(cause),
                    SupervisorOutcomeV1::Ready(evidence) => ReproveOutcomeV1::Ready(evidence),
                };
            }
            Ok(Err(cause)) => {
                self.readiness = None;
                return match self.degrade(DegradationCauseV1::HandshakeTransportFailed(cause)) {
                    SupervisorOutcomeV1::Unavailable(cause) => ReproveOutcomeV1::Unavailable(cause),
                    SupervisorOutcomeV1::Ready(evidence) => ReproveOutcomeV1::Ready(evidence),
                };
            }
            Ok(Ok(response)) => response,
        };

        let terminal_code = response.terminal.terminal_code();
        if terminal_code != TerminalCode::Success {
            self.readiness = None;
            return match self.degrade(DegradationCauseV1::HandshakeRefused { terminal_code }) {
                SupervisorOutcomeV1::Unavailable(cause) => ReproveOutcomeV1::Unavailable(cause),
                SupervisorOutcomeV1::Ready(evidence) => ReproveOutcomeV1::Ready(evidence),
            };
        }

        match validate_readiness(&self.scope, request, response) {
            Err(defect) => {
                self.readiness = None;
                match self.degrade(DegradationCauseV1::HandshakeContractViolation(defect)) {
                    SupervisorOutcomeV1::Unavailable(cause) => ReproveOutcomeV1::Unavailable(cause),
                    SupervisorOutcomeV1::Ready(evidence) => ReproveOutcomeV1::Ready(evidence),
                }
            }
            Ok(evidence) => {
                self.degradation = None;
                self.clear_violations();
                self.readiness = Some(evidence.clone());
                ReproveOutcomeV1::Ready(evidence)
            }
        }
    }

    /// Earliest instant a further [`Self::start_or_restart`] pass may touch
    /// the adapter, or `None` when no pacing is currently armed.
    #[must_use]
    pub const fn next_restart_eligible_at_unix_micros(&self) -> Option<i64> {
        self.next_eligible_unix_micros
    }

    /// Micros a caller must still wait before a further pass will touch the
    /// adapter, given attempts already recorded inside the window as of
    /// `now_unix_micros`.
    ///
    /// `None` when the budget is exhausted for the window — the caller must
    /// wait until attempts age out rather than retry sooner. `Some(0)` means
    /// a pass will be admitted now.
    #[must_use]
    pub fn next_restart_delay_micros(&mut self, now_unix_micros: i64) -> Option<i64> {
        self.prune_window(now_unix_micros);
        if self.attempts_in_window.len() >= self.restart_budget.max_attempts_per_window as usize {
            return None;
        }
        Some(
            self.next_eligible_unix_micros
                .map_or(0, |instant| instant.saturating_sub(now_unix_micros))
                .max(0),
        )
    }

    /// One bounded shutdown pass: requests a graceful stop within the grace
    /// budget from `now_unix_micros`, and escalates to a forced kill only
    /// when that budget elapses without a confirmed stop.
    ///
    /// A shutdown that could not confirm death leaves
    /// [`PredecessorStateV1::DeathUnknown`] and a persisted typed
    /// degradation, so a later restart cannot spawn an overlapping owner. A
    /// confirmed shutdown, graceful or forced, always ends in no claimed
    /// instance.
    pub fn shutdown(
        &mut self,
        now_unix_micros: i64,
    ) -> Result<ShutdownReportV1, DegradationCauseV1<A::Error>> {
        match self.confirm_predecessor_death(now_unix_micros) {
            Ok(escalated_to_kill) => {
                self.readiness = None;
                // A confirmed death is exactly what a quarantined provider
                // needs before it can be released — and exactly what must not
                // be mistaken for the release itself. The quarantine and its
                // record survive the shutdown.
                if self.quarantine.is_some() {
                    self.availability = ProviderAvailabilityV1::Quarantined;
                } else {
                    self.availability = ProviderAvailabilityV1::NotStarted;
                    self.degradation = None;
                }
                Ok(ShutdownReportV1 {
                    escalated_to_kill,
                    confirmed_dead: true,
                })
            }
            Err(cause) => {
                self.readiness = None;
                self.availability = if self.quarantine.is_some() {
                    ProviderAvailabilityV1::Quarantined
                } else {
                    ProviderAvailabilityV1::Unavailable
                };
                self.degradation = Some(DegradationRecordV1 {
                    kind: cause.kind(),
                    detail: cause.to_string(),
                });
                Err(cause)
            }
        }
    }
}

/// What one bounded shutdown pass did.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShutdownReportV1 {
    /// Whether the grace period elapsed without a confirmed graceful stop,
    /// requiring escalation to a forced kill.
    pub escalated_to_kill: bool,
    /// Whether the instance's death was actually confirmed. A shutdown that
    /// returns `Ok` always confirmed it; the field exists so a persisted
    /// report cannot be read as a confirmation it never carried.
    pub confirmed_dead: bool,
}

/// Verifies every success invariant of one `Success` handshake response
/// against the supervisor's own binding, and produces the only
/// [`ReadinessEvidenceV1`] that exists.
///
/// This is the fail-closed readiness gate ADR-0009 requires: the provider
/// instance, immutable build and state identity, loaded state namespace,
/// exact accepted scope, ready receipt, negotiated capabilities, and
/// negotiated limits are each compared with the supervisor's pinned and
/// requested values. Nothing is defaulted; a missing value is a defect, not
/// an empty string.
fn validate_readiness(
    scope: &SupervisedScopeV1,
    request: &HandshakeRequest,
    response: HandshakeResponse,
) -> Result<ReadinessEvidenceV1, ReadinessDefectV1> {
    let HandshakeResponse {
        terminal,
        descriptor,
        provider_instance_id,
        state_namespace,
        accepted_scope,
        effective_limits,
        ready_receipt_sha256,
        warnings,
    } = response;

    if terminal.provider_id() != &scope.provider_id {
        return Err(ReadinessDefectV1::ForeignTerminalProvider {
            terminal_provider_id: terminal.provider_id().as_str().to_owned(),
        });
    }
    if terminal.operation() != ProviderOperation::Handshake {
        return Err(ReadinessDefectV1::ForeignTerminalOperation {
            terminal_operation: terminal.operation().as_wire(),
        });
    }
    if terminal.exact_scope_sha256() != scope.exact_scope_sha256 {
        return Err(ReadinessDefectV1::TerminalScopeMismatch);
    }

    if warnings.len() > MAX_HANDSHAKE_WARNINGS
        || warnings
            .iter()
            .any(|warning| warning.len() > MAX_HANDSHAKE_WARNING_BYTES)
    {
        return Err(ReadinessDefectV1::UnboundedWarnings {
            warnings: warnings.len(),
        });
    }

    let provider_instance_id =
        provider_instance_id.ok_or(ReadinessDefectV1::MissingInstanceIdentity)?;
    if provider_instance_id.is_empty()
        || provider_instance_id.len() > MAX_PROVIDER_INSTANCE_ID_BYTES
        || provider_instance_id
            .chars()
            .any(|character| character.is_control())
    {
        return Err(ReadinessDefectV1::InvalidInstanceIdentity);
    }

    let state_namespace = state_namespace.ok_or(ReadinessDefectV1::MissingStateNamespace)?;
    validate_state_namespace(
        &state_namespace,
        scope.admitted_state_namespace_prefix.as_deref(),
    )?;
    // Containment, not merely validation: when the host bound a state
    // authority, the admitted namespace is resolved into a host-created root
    // and the capability rooted there is the only state path this incarnation
    // is ever handed. A grant that cannot be contained refuses readiness.
    let state_capability = match &scope.state_authority {
        Some(authority) => Some(authority.grant(&state_namespace).map_err(|source| {
            ReadinessDefectV1::StateCapabilityUnavailable {
                state_namespace: state_namespace.clone(),
                detail: source.to_string(),
            }
        })?),
        None => None,
    };

    let accepted_scope = accepted_scope.ok_or(ReadinessDefectV1::MissingAcceptedScope)?;
    if let Some(field) = SupervisedScopeV1::scope_mismatch(&scope.exact_scope, &accepted_scope) {
        return Err(ReadinessDefectV1::AcceptedScopeMismatch { field });
    }

    let ready_receipt_sha256 =
        ready_receipt_sha256.ok_or(ReadinessDefectV1::MissingReadyReceipt)?;
    if !is_bare_lowercase_sha256(&ready_receipt_sha256) {
        return Err(ReadinessDefectV1::InvalidReadyReceipt);
    }

    let descriptor = descriptor.ok_or(ReadinessDefectV1::MissingDescriptor)?;
    descriptor
        .validate()
        .map_err(|source: ApiError| ReadinessDefectV1::InvalidDescriptor {
            detail: source.to_string(),
        })?;
    if descriptor.provider_id != scope.provider_id {
        return Err(ReadinessDefectV1::DescriptorProviderMismatch {
            descriptor_provider_id: descriptor.provider_id.as_str().to_owned(),
        });
    }
    if !descriptor.supports(HEALTH_CAPABILITY_ID) {
        return Err(ReadinessDefectV1::MissingHealthCapability);
    }
    for capability in &request.required_capabilities {
        if !descriptor.supports(capability.as_str()) {
            return Err(ReadinessDefectV1::MissingRequiredCapability {
                capability_id: capability.as_str().to_owned(),
            });
        }
    }
    if let Some(pinned) = &scope.pinned_implementation_identity_sha256
        && pinned != &descriptor.implementation_identity_sha256
    {
        return Err(ReadinessDefectV1::PinnedIdentityMismatch {
            reported: descriptor.implementation_identity_sha256.clone(),
        });
    }
    if let Some(pinned) = &scope.pinned_state_schema_version
        && pinned != &descriptor.state_schema_version
    {
        return Err(ReadinessDefectV1::PinnedStateSchemaMismatch {
            reported: descriptor.state_schema_version.clone(),
        });
    }

    let effective_limits = effective_limits.ok_or(ReadinessDefectV1::MissingEffectiveLimits)?;
    effective_limits.validate().map_err(|source: ApiError| {
        ReadinessDefectV1::InvalidEffectiveLimits {
            detail: source.to_string(),
        }
    })?;
    if let Some(limit) = first_limit_above_ceiling(effective_limits, scope.host_limits) {
        return Err(ReadinessDefectV1::EffectiveLimitAboveHostCeiling { limit });
    }

    let retains_replay_position = descriptor.supports(REPLAY_CAPABILITY_ID);
    Ok(ReadinessEvidenceV1 {
        provider_instance_id,
        state_namespace,
        state_capability,
        ready_receipt_sha256,
        implementation_identity_sha256: descriptor.implementation_identity_sha256,
        state_schema_version: descriptor.state_schema_version,
        state_generation: descriptor.state_generation,
        retains_replay_position,
        effective_limits,
    })
}

/// Returns the first negotiated limit that exceeds the host's own ceiling.
fn first_limit_above_ceiling(
    negotiated: ProviderLimits,
    ceiling: ProviderLimits,
) -> Option<&'static str> {
    [
        (
            "request_bytes",
            negotiated.request_bytes,
            ceiling.request_bytes,
        ),
        (
            "response_bytes",
            negotiated.response_bytes,
            ceiling.response_bytes,
        ),
        (
            "observation_batch_items",
            negotiated.observation_batch_items,
            ceiling.observation_batch_items,
        ),
        (
            "recall_candidates",
            negotiated.recall_candidates,
            ceiling.recall_candidates,
        ),
        (
            "concurrent_operations",
            negotiated.concurrent_operations,
            ceiling.concurrent_operations,
        ),
        (
            "operation_millis",
            negotiated.operation_millis,
            ceiling.operation_millis,
        ),
        (
            "snapshot_bytes",
            negotiated.snapshot_bytes,
            ceiling.snapshot_bytes,
        ),
        (
            "inspection_items",
            negotiated.inspection_items,
            ceiling.inspection_items,
        ),
    ]
    .into_iter()
    .find_map(|(limit, value, ceiling)| (value > ceiling).then_some(limit))
}

/// Refuses a state-namespace prefix a host cannot admit, because a prefix that
/// is not itself a contained namespace can never bound one.
pub fn validate_admitted_state_namespace_prefix(prefix: &str) -> Result<(), SupervisorConfigError> {
    contained_state_namespace(prefix).map_err(|_| SupervisorConfigError::InvalidField {
        field: "admitted_state_namespace_prefix",
    })
}

/// Refuses a provider-reported state namespace that is unusable, that could
/// address storage outside the host-owned namespace root, or that lies
/// outside the prefix this provider was admitted to own.
///
/// The containment rule is deliberately structural rather than filesystem
/// aware: the supervisor does not know how a given topology materializes a
/// namespace, so it refuses every shape that could *become* an escape once
/// joined to a root — absolute forms, parent traversal, dotted or empty
/// segments, foreign separators, and percent-encoding a later decoder could
/// turn back into any of those.
fn validate_state_namespace(
    state_namespace: &str,
    admitted_prefix: Option<&str>,
) -> Result<(), ReadinessDefectV1> {
    contained_state_namespace(state_namespace)?;
    if let Some(prefix) = admitted_prefix
        && !is_admitted_state_namespace(state_namespace, prefix)
    {
        return Err(ReadinessDefectV1::StateNamespaceNotAdmitted {
            state_namespace: state_namespace.to_owned(),
            admitted_prefix: prefix.to_owned(),
        });
    }
    Ok(())
}

/// Whether `state_namespace` is a canonical, non-escaping namespace path.
fn contained_state_namespace(state_namespace: &str) -> Result<(), ReadinessDefectV1> {
    if state_namespace.is_empty() || state_namespace.len() > MAX_STATE_NAMESPACE_BYTES {
        return Err(ReadinessDefectV1::InvalidStateNamespace);
    }
    if escapes_containment(state_namespace) {
        return Err(ReadinessDefectV1::StateNamespaceEscapesContainment {
            state_namespace: state_namespace.to_owned(),
        });
    }
    if charset_usable(state_namespace) {
        Ok(())
    } else {
        Err(ReadinessDefectV1::InvalidStateNamespace)
    }
}

/// Whether `state_namespace` is the admitted prefix itself or sits beneath it
/// at a real segment boundary.
fn is_admitted_state_namespace(state_namespace: &str, admitted_prefix: &str) -> bool {
    if state_namespace == admitted_prefix {
        return true;
    }
    match state_namespace.strip_prefix(admitted_prefix) {
        Some(remainder) => remainder.starts_with('.') || remainder.starts_with('/'),
        None => false,
    }
}

/// Whether `value` is a bare lowercase 64-hex SHA-256 digest.
fn is_bare_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
