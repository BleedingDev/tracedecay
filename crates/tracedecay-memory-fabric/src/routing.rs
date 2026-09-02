//! Explicit, policy-driven selection of the provider that answers one active
//! call, and the only place a fallback directive can turn into a second
//! provider contact.
//!
//! Routing is capability- and policy-driven: the host pins one active
//! provider identity and registration revision, plus a fallback rule that is
//! [`FallbackRule::Forbidden`] unless an operator pinned a complete
//! [`PinnedFallbackPolicy`]. A provider is refused *before any contact* when
//! it is not registered under the configured revision, is not in
//! [`ProviderMode::Active`] (observer and disabled registrations are never
//! selectable for product output), or does not declare the routed
//! capability. Every reply the router returns names the provider that
//! produced it, so a caller can never mistake one provider's outcome for
//! another's.
//!
//! Fallback is a second explicit route, never a substitution: it is
//! attempted only when the provider's own terminal permitted it
//! (`explicit_policy_only`) *and* the host rule pins the identical policy
//! identity, revision, and target, *and* the target is itself registered
//! active with the capability, *and* a fresh handshake against the target
//! succeeds. Any other condition is a typed
//! [`FallbackDeclinedReason`] carried alongside the original provider reply.
//! Empty successful results are never a fallback signal, and nothing here
//! routes to a different provider than the two the policy names.

use std::error::Error;
use std::fmt;

use tracedecay_memory_provider_api::contract::{FallbackEligibility, TerminalCode};
use tracedecay_memory_provider_api::{
    HandshakeRequest, OwnedProviderId, PinnedFallbackPolicy, ProviderCall, ProviderDescriptor,
    ProviderReply,
};

use crate::{FabricError, MemoryFabric, ProviderMode};

/// Host-configured rule for whether a provider's fallback directive may be
/// honoured at all.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FallbackRule {
    /// No fallback, whatever a provider terminal suggests. This is the
    /// product default.
    Forbidden,
    /// Fallback is permitted only when the provider terminal carries exactly
    /// this pinned policy and the target is a registered active provider.
    ExplicitPinned(PinnedFallbackPolicy),
}

/// Failure constructing an [`ActiveRoutingPolicy`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RoutingPolicyError {
    /// A registration revision of zero can never match a real registration.
    RegistrationRevisionZero,
    /// The pinned fallback target names the active provider itself.
    FallbackTargetMatchesActiveProvider,
}

impl fmt::Display for RoutingPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RegistrationRevisionZero => {
                formatter.write_str("routing policy registration revision must be positive")
            }
            Self::FallbackTargetMatchesActiveProvider => {
                formatter.write_str("routing policy fallback target equals the active provider")
            }
        }
    }
}

impl Error for RoutingPolicyError {}

/// Pinned host configuration naming the one provider allowed to answer an
/// active call and the only fallback rule that may extend it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveRoutingPolicy {
    active_provider: OwnedProviderId,
    registration_revision: u64,
    fallback: FallbackRule,
}

impl ActiveRoutingPolicy {
    /// Creates a validated policy: a positive registration revision and a
    /// fallback target that differs from the active provider.
    pub fn new(
        active_provider: OwnedProviderId,
        registration_revision: u64,
        fallback: FallbackRule,
    ) -> Result<Self, RoutingPolicyError> {
        if registration_revision == 0 {
            return Err(RoutingPolicyError::RegistrationRevisionZero);
        }
        if let FallbackRule::ExplicitPinned(policy) = &fallback
            && policy.target_provider_id() == &active_provider
        {
            return Err(RoutingPolicyError::FallbackTargetMatchesActiveProvider);
        }
        Ok(Self {
            active_provider,
            registration_revision,
            fallback,
        })
    }

    /// Returns the configured active provider identity.
    #[must_use]
    pub const fn active_provider(&self) -> &OwnedProviderId {
        &self.active_provider
    }

    /// Returns the registration revision the active provider must be
    /// registered under.
    #[must_use]
    pub const fn registration_revision(&self) -> u64 {
        self.registration_revision
    }

    /// Returns the host fallback rule.
    #[must_use]
    pub const fn fallback(&self) -> &FallbackRule {
        &self.fallback
    }
}

/// A provider registration the router intends to contact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteTarget {
    /// Provider identity to contact.
    pub provider_id: OwnedProviderId,
    /// Registration revision the contact is admitted under.
    pub registration_revision: u64,
}

/// A route target whose readiness handshake succeeded.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadyRouteTarget {
    /// Provider identity to contact.
    pub provider_id: OwnedProviderId,
    /// Registration revision the contact is admitted under.
    pub registration_revision: u64,
    /// Provider-reported runtime instance identity from the handshake.
    pub provider_instance_id: String,
    /// Ready-receipt digest the call must carry.
    pub ready_receipt_sha256: String,
    /// Descriptor accepted by the handshake, including the state generation
    /// the call must expect.
    pub descriptor: ProviderDescriptor,
}

/// Host-owned construction of the handshake and call for whichever provider
/// the router selects.
///
/// The router never fabricates scope, deadline, cancellation, or payload: the
/// plan builds them for the named target, and the same plan is asked again
/// for a fallback target so a fresh handshake and a target-bound call exist
/// before any second provider contact.
pub trait ActiveCallPlan {
    /// Typed failure while building a request.
    type Error: Error + 'static;

    /// Builds the readiness handshake for `target`.
    fn handshake_request(&self, target: &RouteTarget) -> Result<HandshakeRequest, Self::Error>;

    /// Builds the active call for a target whose handshake succeeded.
    fn provider_call(&self, target: &ReadyRouteTarget) -> Result<ProviderCall, Self::Error>;
}

/// The provider that produced a routed reply.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutedProviderIdentity {
    /// Stable logical provider identity.
    pub provider_id: OwnedProviderId,
    /// Registration revision the reply was admitted under.
    pub registration_revision: u64,
    /// Provider-reported runtime instance identity from the readiness
    /// handshake that preceded the call.
    pub provider_instance_id: String,
}

/// Why a fallback directive was not honoured.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FallbackDeclinedReason {
    /// The provider terminal itself forbade fallback.
    DirectiveForbidden,
    /// The host rule is [`FallbackRule::Forbidden`].
    HostRuleForbidden,
    /// The provider terminal claimed explicit eligibility without a policy.
    DirectivePolicyMissing,
    /// The provider's directive was evaluated for a different source
    /// provider than the one that answered.
    SourceProviderMismatch {
        /// Source provider the directive named, if any.
        directive_source: Option<OwnedProviderId>,
    },
    /// The provider's pinned policy differs from the host rule.
    PolicyMismatch {
        /// Policy the provider terminal carried.
        directive: PinnedFallbackPolicy,
        /// Policy the host configured.
        configured: PinnedFallbackPolicy,
    },
    /// The pinned target has no registration.
    TargetNotRegistered {
        /// Configured target.
        target: OwnedProviderId,
    },
    /// The pinned target is registered but not active.
    TargetNotActive {
        /// Configured target.
        target: OwnedProviderId,
        /// Its registered mode.
        mode: ProviderMode,
    },
    /// The pinned target does not declare the routed capability.
    TargetCapabilityUndeclared {
        /// Configured target.
        target: OwnedProviderId,
        /// Capability the route requires.
        capability: String,
    },
    /// The fabric refused the fresh handshake against the target.
    TargetHandshakeRefused {
        /// Configured target.
        target: OwnedProviderId,
        /// Fabric refusal.
        error: FabricError,
    },
    /// The target's fresh handshake did not reach a successful terminal.
    TargetHandshakeNotReady {
        /// Configured target.
        target: OwnedProviderId,
        /// Handshake terminal code.
        terminal_code: TerminalCode,
    },
    /// The target's successful handshake omitted readiness evidence.
    TargetHandshakeIncomplete {
        /// Configured target.
        target: OwnedProviderId,
    },
    /// The fabric refused the call against the target.
    TargetCallRefused {
        /// Configured target.
        target: OwnedProviderId,
        /// Fabric refusal.
        error: FabricError,
    },
}

impl fmt::Display for FallbackDeclinedReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DirectiveForbidden => formatter.write_str("provider terminal forbade fallback"),
            Self::HostRuleForbidden => formatter.write_str("host fallback rule is forbidden"),
            Self::DirectivePolicyMissing => {
                formatter.write_str("provider terminal carried no fallback policy")
            }
            Self::SourceProviderMismatch { directive_source } => write!(
                formatter,
                "fallback directive was evaluated for {} rather than the answering provider",
                directive_source
                    .as_ref()
                    .map_or("no provider", OwnedProviderId::as_str)
            ),
            Self::PolicyMismatch {
                directive,
                configured,
            } => write!(
                formatter,
                "provider fallback policy {}@{} -> {} differs from configured {}@{} -> {}",
                directive.policy_id(),
                directive.policy_revision(),
                directive.target_provider_id().as_str(),
                configured.policy_id(),
                configured.policy_revision(),
                configured.target_provider_id().as_str(),
            ),
            Self::TargetNotRegistered { target } => {
                write!(
                    formatter,
                    "fallback target {} is not registered",
                    target.as_str()
                )
            }
            Self::TargetNotActive { target, mode } => write!(
                formatter,
                "fallback target {} is registered as {mode:?}, not active",
                target.as_str()
            ),
            Self::TargetCapabilityUndeclared { target, capability } => write!(
                formatter,
                "fallback target {} does not declare {capability}",
                target.as_str()
            ),
            Self::TargetHandshakeRefused { target, error } => write!(
                formatter,
                "fallback target {} handshake refused: {error}",
                target.as_str()
            ),
            Self::TargetHandshakeNotReady {
                target,
                terminal_code,
            } => write!(
                formatter,
                "fallback target {} handshake terminated with {}",
                target.as_str(),
                terminal_code.as_wire()
            ),
            Self::TargetHandshakeIncomplete { target } => write!(
                formatter,
                "fallback target {} handshake omitted readiness evidence",
                target.as_str()
            ),
            Self::TargetCallRefused { target, error } => write!(
                formatter,
                "fallback target {} call refused: {error}",
                target.as_str()
            ),
        }
    }
}

/// What the router decided about fallback for one routed reply.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FallbackDecision {
    /// The reply was a successful, zero-result, or partial terminal; fallback
    /// is not a question these terminals raise.
    NotApplicable,
    /// The reply is a failure terminal and fallback was declined for the
    /// stated reason. The reply is the original provider's.
    Declined(FallbackDeclinedReason),
    /// Fallback was dispatched under the pinned policy; the reply is the
    /// target's, and `from` names the provider whose failure preceded it.
    Dispatched {
        /// Provider whose failure terminal permitted fallback.
        from: RoutedProviderIdentity,
        /// Terminal code that provider returned.
        from_terminal_code: TerminalCode,
        /// Policy under which the second route was admitted.
        policy: PinnedFallbackPolicy,
    },
}

/// One routed reply together with the identity of the provider that produced
/// it and the fallback decision the router made.
#[derive(Clone, Debug)]
pub struct RoutedActiveReply {
    /// Provider that produced `reply`.
    pub identity: RoutedProviderIdentity,
    /// The exact call the fabric dispatched to that provider, so a caller can
    /// admit the reply against the scope, request, and budget it actually
    /// carried rather than against a call rebuilt after the fact.
    pub call: ProviderCall,
    /// Fabric-validated provider reply.
    pub reply: ProviderReply,
    /// Fallback decision for this route.
    pub fallback: FallbackDecision,
}

impl RoutedActiveReply {
    /// Returns the terminal code of the reply.
    #[must_use]
    pub fn terminal_code(&self) -> TerminalCode {
        self.reply.terminal.terminal_code()
    }
}

/// Typed failure of routing before a reply existed.
#[derive(Debug)]
pub enum RoutingError<E> {
    /// The fabric refused the handshake or the active call.
    Fabric(FabricError),
    /// The host plan could not build a request.
    Plan(E),
    /// The configured provider has no registration.
    ProviderNotRegistered {
        /// Configured provider.
        provider_id: OwnedProviderId,
    },
    /// The configured provider is registered under another revision.
    RegistrationRevisionMismatch {
        /// Configured provider.
        provider_id: OwnedProviderId,
        /// Revision the policy pins.
        configured: u64,
        /// Revision the registry accepted.
        registered: u64,
    },
    /// The configured provider is not in active mode, so it can never be
    /// selected for product output.
    ProviderNotActive {
        /// Configured provider.
        provider_id: OwnedProviderId,
        /// Its registered mode.
        mode: ProviderMode,
    },
    /// The configured provider does not declare the routed capability.
    CapabilityUndeclared {
        /// Configured provider.
        provider_id: OwnedProviderId,
        /// Capability the route requires.
        capability: String,
    },
    /// The readiness handshake did not reach a successful terminal.
    HandshakeNotReady {
        /// Configured provider.
        provider_id: OwnedProviderId,
        /// Handshake terminal code.
        terminal_code: TerminalCode,
    },
    /// The successful handshake omitted readiness evidence.
    HandshakeIncomplete {
        /// Configured provider.
        provider_id: OwnedProviderId,
    },
}

impl<E: fmt::Display> fmt::Display for RoutingError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fabric(error) => write!(formatter, "memory fabric refused the route: {error}"),
            Self::Plan(error) => write!(formatter, "route plan could not build a request: {error}"),
            Self::ProviderNotRegistered { provider_id } => write!(
                formatter,
                "configured provider {} is not registered",
                provider_id.as_str()
            ),
            Self::RegistrationRevisionMismatch {
                provider_id,
                configured,
                registered,
            } => write!(
                formatter,
                "configured provider {} is pinned at revision {configured} but registered at \
                 {registered}",
                provider_id.as_str()
            ),
            Self::ProviderNotActive { provider_id, mode } => write!(
                formatter,
                "configured provider {} is registered as {mode:?} and cannot answer active calls",
                provider_id.as_str()
            ),
            Self::CapabilityUndeclared {
                provider_id,
                capability,
            } => write!(
                formatter,
                "configured provider {} does not declare {capability}",
                provider_id.as_str()
            ),
            Self::HandshakeNotReady {
                provider_id,
                terminal_code,
            } => write!(
                formatter,
                "configured provider {} handshake terminated with {}",
                provider_id.as_str(),
                terminal_code.as_wire()
            ),
            Self::HandshakeIncomplete { provider_id } => write!(
                formatter,
                "configured provider {} handshake omitted readiness evidence",
                provider_id.as_str()
            ),
        }
    }
}

impl<E: Error + 'static> Error for RoutingError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Fabric(error) => Some(error),
            Self::Plan(error) => Some(error),
            _ => None,
        }
    }
}

impl<E> From<FabricError> for RoutingError<E> {
    fn from(value: FabricError) -> Self {
        Self::Fabric(value)
    }
}

/// Outcome of the pre-contact admission of one route target.
enum TargetAdmission {
    Admitted,
    NotRegistered,
    RevisionMismatch { registered: u64 },
    NotActive { mode: ProviderMode },
    CapabilityUndeclared,
}

/// One reply from an admitted target together with the identity and the
/// exact call that produced it.
struct TargetReply {
    identity: RoutedProviderIdentity,
    call: ProviderCall,
    reply: ProviderReply,
}

/// Outcome of one fallback dispatch: the target's reply, or the typed reason
/// the target was declined.
type FallbackDispatch = Result<TargetReply, FallbackDeclinedReason>;

/// Outcome of one handshake-then-call against an admitted target.
enum TargetContact {
    Replied(Box<TargetReply>),
    HandshakeRefused(FabricError),
    HandshakeNotReady(TerminalCode),
    HandshakeIncomplete,
    CallRefused(FabricError),
}

impl MemoryFabric {
    /// Routes one active call to the provider the policy names, refusing
    /// non-active or capability-less registrations before any contact, and
    /// honouring a fallback directive only under the pinned host rule.
    ///
    /// The returned reply always carries the identity of the provider that
    /// produced it. A failure terminal from the configured provider is
    /// returned as that provider's reply with a typed
    /// [`FallbackDecision::Declined`] unless every fallback condition holds;
    /// it is never converted into another provider's output silently.
    pub fn route_active<P: ActiveCallPlan>(
        &self,
        policy: &ActiveRoutingPolicy,
        capability_id: &str,
        plan: &P,
    ) -> Result<RoutedActiveReply, RoutingError<P::Error>> {
        let primary = RouteTarget {
            provider_id: policy.active_provider.clone(),
            registration_revision: policy.registration_revision,
        };
        match self.admit_target(&primary, capability_id)? {
            TargetAdmission::Admitted => {}
            TargetAdmission::NotRegistered => {
                return Err(RoutingError::ProviderNotRegistered {
                    provider_id: primary.provider_id,
                });
            }
            TargetAdmission::RevisionMismatch { registered } => {
                return Err(RoutingError::RegistrationRevisionMismatch {
                    provider_id: primary.provider_id,
                    configured: primary.registration_revision,
                    registered,
                });
            }
            TargetAdmission::NotActive { mode } => {
                return Err(RoutingError::ProviderNotActive {
                    provider_id: primary.provider_id,
                    mode,
                });
            }
            TargetAdmission::CapabilityUndeclared => {
                return Err(RoutingError::CapabilityUndeclared {
                    provider_id: primary.provider_id,
                    capability: capability_id.to_owned(),
                });
            }
        }

        let (identity, call, reply) = match self.contact_target(&primary, plan)? {
            TargetContact::Replied(replied) => {
                let TargetReply {
                    identity,
                    call,
                    reply,
                } = *replied;
                (identity, call, reply)
            }
            TargetContact::HandshakeRefused(error) | TargetContact::CallRefused(error) => {
                return Err(RoutingError::Fabric(error));
            }
            TargetContact::HandshakeNotReady(terminal_code) => {
                return Err(RoutingError::HandshakeNotReady {
                    provider_id: primary.provider_id,
                    terminal_code,
                });
            }
            TargetContact::HandshakeIncomplete => {
                return Err(RoutingError::HandshakeIncomplete {
                    provider_id: primary.provider_id,
                });
            }
        };

        let directive = reply.terminal.fallback();
        let terminal_code = reply.terminal.terminal_code();
        let decision = match directive.eligibility() {
            FallbackEligibility::Forbidden => {
                if matches!(
                    terminal_code,
                    TerminalCode::Success
                        | TerminalCode::SuccessZeroResults
                        | TerminalCode::Partial
                ) {
                    FallbackDecision::NotApplicable
                } else {
                    FallbackDecision::Declined(FallbackDeclinedReason::DirectiveForbidden)
                }
            }
            FallbackEligibility::ExplicitPolicyOnly => {
                let configured = match &policy.fallback {
                    FallbackRule::Forbidden => {
                        return Ok(RoutedActiveReply {
                            identity,
                            call,
                            reply,
                            fallback: FallbackDecision::Declined(
                                FallbackDeclinedReason::HostRuleForbidden,
                            ),
                        });
                    }
                    FallbackRule::ExplicitPinned(configured) => configured,
                };
                let Some(pinned) = directive.policy() else {
                    return Ok(RoutedActiveReply {
                        identity,
                        call,
                        reply,
                        fallback: FallbackDecision::Declined(
                            FallbackDeclinedReason::DirectivePolicyMissing,
                        ),
                    });
                };
                if directive.source_provider_id() != Some(&identity.provider_id) {
                    let directive_source = directive.source_provider_id().cloned();
                    return Ok(RoutedActiveReply {
                        identity,
                        call,
                        reply,
                        fallback: FallbackDecision::Declined(
                            FallbackDeclinedReason::SourceProviderMismatch { directive_source },
                        ),
                    });
                }
                if pinned != configured {
                    let declined = FallbackDeclinedReason::PolicyMismatch {
                        directive: pinned.clone(),
                        configured: configured.clone(),
                    };
                    return Ok(RoutedActiveReply {
                        identity,
                        call,
                        reply,
                        fallback: FallbackDecision::Declined(declined),
                    });
                }
                let policy = configured.clone();
                match self.dispatch_fallback(&policy, capability_id, plan)? {
                    Ok(target) => {
                        return Ok(RoutedActiveReply {
                            identity: target.identity,
                            call: target.call,
                            reply: target.reply,
                            fallback: FallbackDecision::Dispatched {
                                from: identity,
                                from_terminal_code: terminal_code,
                                policy,
                            },
                        });
                    }
                    Err(declined) => FallbackDecision::Declined(declined),
                }
            }
        };
        Ok(RoutedActiveReply {
            identity,
            call,
            reply,
            fallback: decision,
        })
    }

    /// Admits the pinned fallback target and contacts it with a fresh
    /// handshake. A declined outcome preserves the original reply; only host
    /// plan failures and poisoned registry state abort the route.
    fn dispatch_fallback<P: ActiveCallPlan>(
        &self,
        policy: &PinnedFallbackPolicy,
        capability_id: &str,
        plan: &P,
    ) -> Result<FallbackDispatch, RoutingError<P::Error>> {
        let target_id = policy.target_provider_id().clone();
        let registered_revision = match self.registration(&target_id) {
            Ok(registration) => registration.revision,
            Err(FabricError::ProviderUnknown(_)) => {
                return Ok(Err(FallbackDeclinedReason::TargetNotRegistered {
                    target: target_id,
                }));
            }
            Err(error) => return Err(RoutingError::Fabric(error)),
        };
        let target = RouteTarget {
            provider_id: target_id.clone(),
            registration_revision: registered_revision,
        };
        match self.admit_target(&target, capability_id)? {
            TargetAdmission::Admitted => {}
            TargetAdmission::NotRegistered | TargetAdmission::RevisionMismatch { .. } => {
                return Ok(Err(FallbackDeclinedReason::TargetNotRegistered {
                    target: target_id,
                }));
            }
            TargetAdmission::NotActive { mode } => {
                return Ok(Err(FallbackDeclinedReason::TargetNotActive {
                    target: target_id,
                    mode,
                }));
            }
            TargetAdmission::CapabilityUndeclared => {
                return Ok(Err(FallbackDeclinedReason::TargetCapabilityUndeclared {
                    target: target_id,
                    capability: capability_id.to_owned(),
                }));
            }
        }
        Ok(match self.contact_target(&target, plan)? {
            TargetContact::Replied(replied) => Ok(*replied),
            TargetContact::HandshakeRefused(error) => {
                Err(FallbackDeclinedReason::TargetHandshakeRefused {
                    target: target_id,
                    error,
                })
            }
            TargetContact::HandshakeNotReady(terminal_code) => {
                Err(FallbackDeclinedReason::TargetHandshakeNotReady {
                    target: target_id,
                    terminal_code,
                })
            }
            TargetContact::HandshakeIncomplete => {
                Err(FallbackDeclinedReason::TargetHandshakeIncomplete { target: target_id })
            }
            TargetContact::CallRefused(error) => Err(FallbackDeclinedReason::TargetCallRefused {
                target: target_id,
                error,
            }),
        })
    }

    /// Pre-contact admission: registered under the expected revision, active,
    /// and declaring the routed capability.
    fn admit_target<E>(
        &self,
        target: &RouteTarget,
        capability_id: &str,
    ) -> Result<TargetAdmission, RoutingError<E>> {
        let registration = match self.registration(&target.provider_id) {
            Ok(registration) => registration,
            Err(FabricError::ProviderUnknown(_)) => return Ok(TargetAdmission::NotRegistered),
            Err(error) => return Err(RoutingError::Fabric(error)),
        };
        if registration.revision != target.registration_revision {
            return Ok(TargetAdmission::RevisionMismatch {
                registered: registration.revision,
            });
        }
        if registration.mode != ProviderMode::Active {
            return Ok(TargetAdmission::NotActive {
                mode: registration.mode,
            });
        }
        if !registration.descriptor.supports(capability_id) {
            return Ok(TargetAdmission::CapabilityUndeclared);
        }
        Ok(TargetAdmission::Admitted)
    }

    /// Fresh handshake followed by the plan's call against one admitted
    /// target. Fabric refusals are returned as data so the caller decides
    /// whether they abort the route or decline a fallback.
    fn contact_target<P: ActiveCallPlan>(
        &self,
        target: &RouteTarget,
        plan: &P,
    ) -> Result<TargetContact, RoutingError<P::Error>> {
        let handshake = plan.handshake_request(target).map_err(RoutingError::Plan)?;
        let response = match self.handshake(&handshake) {
            Ok(response) => response,
            Err(error) => return Ok(TargetContact::HandshakeRefused(error)),
        };
        let terminal_code = response.terminal.terminal_code();
        if terminal_code != TerminalCode::Success {
            return Ok(TargetContact::HandshakeNotReady(terminal_code));
        }
        let (Some(provider_instance_id), Some(ready_receipt_sha256), Some(descriptor)) = (
            response.provider_instance_id,
            response.ready_receipt_sha256,
            response.descriptor,
        ) else {
            return Ok(TargetContact::HandshakeIncomplete);
        };
        let ready = ReadyRouteTarget {
            provider_id: target.provider_id.clone(),
            registration_revision: target.registration_revision,
            provider_instance_id,
            ready_receipt_sha256,
            descriptor,
        };
        let call = plan.provider_call(&ready).map_err(RoutingError::Plan)?;
        let reply = match self.invoke_active(&call) {
            Ok(reply) => reply,
            Err(error) => return Ok(TargetContact::CallRefused(error)),
        };
        Ok(TargetContact::Replied(Box::new(TargetReply {
            identity: RoutedProviderIdentity {
                provider_id: ready.provider_id,
                registration_revision: ready.registration_revision,
                provider_instance_id: ready.provider_instance_id,
            },
            call,
            reply,
        })))
    }
}
