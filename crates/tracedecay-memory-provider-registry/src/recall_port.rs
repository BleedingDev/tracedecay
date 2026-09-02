//! Production implementation of the application cognitive-recall port over
//! the product provider composition.
//!
//! The port owns the direction of authority end to end:
//!
//! * the exact coding scope is derived by the host-supplied
//!   [`ExactScopeBinding`] from the application's resolved scope — never from
//!   anything a provider returns — and the composition root is the only place
//!   that implements the binding, so one derivation of `agent_session_id`
//!   exists;
//! * the application deadline is carried into the provider call unchanged,
//!   and the caller's *live* cancellation identity — the host-owned
//!   [`CancellationSignal`] whose token id the request names — is bridged
//!   into one [`CancellationToken`] that every handshake, call, and pinned
//!   fallback call of that recall shares. The adapter never mints a
//!   replacement identity: a signal whose token id differs from the request's
//!   is refused, and cancellation requested *after* dispatch reaches the
//!   provider through the token it is already holding. A request that is
//!   already cancelled or past its deadline is answered with a typed
//!   degradation without contacting the provider;
//! * every provider reply is passed through rank-final
//!   [`admit_recall_reply`], so a candidate that fails exact-scope, identity,
//!   validity, or revocation checks can only reach the caller as a row in the
//!   [`RecallAdmissionReport`] ledger, never as advisory content;
//! * non-success terminals and fabric errors are typed: lane degradations
//!   (`deadline_exceeded`, `cancelled`, `provider_unavailable`,
//!   `capability_unsupported`, `capacity_exceeded`) become
//!   [`CognitiveRecallDegradation`], and a provider that rejects the host's
//!   own scope is an error, never an empty success;
//! * admitted candidates are normalized and then *selected* before anything
//!   becomes advisory content: host-owned deduplication and diversity
//!   selection ([`select_recall_candidates`]) run on every mounted recall, so
//!   the same memory returned twice can never consume the result budget
//!   twice, and the complete selection receipt — including why each dropped
//!   candidate was dropped — travels with the outcome. A normalization that
//!   does not describe the admitted slice is a typed refusal, never a
//!   silently unpruned stream;
//! * provider selection is the host's [`ActiveRoutingPolicy`], never a
//!   provider name baked into the port: the pinned provider must be
//!   registered active under the pinned revision with the recall capability
//!   or the request is refused before any contact, and a fallback directive
//!   is honoured only under the identical host-pinned rule. Every result —
//!   complete, degraded before contact, or degraded by a terminal — carries
//!   the [`CognitiveRecallProviderIdentity`] of the provider it is attributed
//!   to, and Native facts are never an implicit fallback target.
//!
//! The application port trait returns only [`CognitiveRecallResult`]; the
//! admission report is returned alongside it by
//! [`ProjectCognitiveRecallPortV1::recall_admitted`] and observed through the
//! [`RecallAdmissionObserver`] the composition root installs.

use std::fmt;
use std::sync::Arc;

use tracedecay_application::memory::{
    CognitiveRecallCandidate, CognitiveRecallDegradation, CognitiveRecallPort,
    CognitiveRecallPortResult, CognitiveRecallProvenance, CognitiveRecallProviderIdentity,
    CognitiveRecallRequest, CognitiveRecallResult,
};
use tracedecay_application::{
    ApplicationContractError, CancellationSignal, ClockError, ResolvedScope, try_now_micros,
};
use tracedecay_memory_fabric::{
    ActiveCallPlan, ActiveRoutingPolicy, FabricError, FallbackDecision, ProviderMode,
    ReadyRouteTarget, RouteTarget, RoutedProviderIdentity, RoutingError,
};
use tracedecay_memory_provider_api::contract::TerminalCode;
use tracedecay_memory_provider_api::{
    ApiError, CancellationToken, HandshakeRequest, HandshakeRequestParts, OperationControl,
    OwnedExactScope, OwnedVersionedId, ProviderCall, ProviderCallParts, ProviderLimits,
    ProviderOperation, ProviderReply,
};

use crate::ProjectMemoryProviderComposition;
use crate::recall_admission::{
    AdmittedTemporalQuery, RECALL_QUERY_CAPABILITY_ID, RecallAdmissionError, RecallAdmissionReport,
    RecallBudgetsV1, RecallCandidateContent, RecallCandidateV1, RecallRequestParts,
    admit_recall_reply, build_recall_request_payload, rfc3339_utc_micros,
};
use crate::recall_normalization::{
    RecallNormalizationError, RecallNormalizationPolicyV1, RecallNormalizationV1,
    normalize_admitted_candidates,
};
use crate::recall_selection::{
    RecallSelectionError, RecallSelectionPolicyError, RecallSelectionPolicyV1, RecallSelectionV1,
    select_recall_candidates,
};

/// Objective sent with every recall. The contract carries the objective as
/// bounded free text, but the Native adapter interprets it as its retrieval
/// kind (`search`, `probe`, `related`, `reason`) and answers any other value
/// with `capability_unsupported`; the application port is a search, so that
/// is the one vocabulary word it sends. The query itself is the
/// application-owned request query.
const RECALL_OBJECTIVE: &str = "search";

/// Failure deriving the exact coding scope from an application resolved scope.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ExactScopeBindingError {
    /// The resolved scope has no exact branch or detached reference.
    #[error("resolved scope for project {project_id} carries no exact reference")]
    ReferenceUnavailable {
        /// Project whose scope lacked a reference.
        project_id: String,
    },
    /// The resolved scope is not the checkout the binding is mounted for. The
    /// host refuses the request before any provider contact rather than
    /// asking a provider about a foreign project, repository, or worktree.
    #[error(
        "resolved scope disagrees with the mounted checkout on {field}: expected {expected}, \
         received {received}"
    )]
    ScopeDisagreement {
        /// Which identity disagreed.
        field: &'static str,
        /// The authoritative value.
        expected: String,
        /// The value the request carried.
        received: String,
    },
    /// The assembled scope failed provider-neutral validation.
    #[error("exact scope contract violation: {0}")]
    Contract(#[from] ApiError),
}

/// Host authority that binds an application resolved scope to the exact
/// provider-visible coding scope.
///
/// The composition root implements this once; the port never assembles a
/// scope from anything a provider returns.
pub trait ExactScopeBinding: Send + Sync + 'static {
    /// Derives the exact scope for `scope`, including the profile and the
    /// provider-qualified session identity the host owns.
    fn bind_exact_scope(
        &self,
        scope: &ResolvedScope,
    ) -> Result<OwnedExactScope, ExactScopeBindingError>;
}

/// The audit sink could not retain one admission report.
///
/// The port treats this as a terminal failure of the recall rather than
/// returning admitted content whose denials are no longer audit-visible.
#[derive(Debug, thiserror::Error)]
#[error("recall admission audit sink refused the report for request {request_id}: {source}")]
pub struct RecallAdmissionAuditError {
    /// Request whose report could not be retained.
    pub request_id: String,
    /// Sink-specific failure.
    #[source]
    pub source: Box<dyn std::error::Error + Send + Sync + 'static>,
}

/// Sink for admission reports, so denied candidates remain audit-visible even
/// though the application port result carries only admitted content.
///
/// A sink is durable by contract: it either retains the report or returns a
/// typed error, and the port then refuses to deliver the result.
pub trait RecallAdmissionObserver: Send + Sync + 'static {
    /// Retains the report of one completed admission.
    fn observe_admission(
        &self,
        report: &RecallAdmissionReport,
    ) -> Result<(), RecallAdmissionAuditError>;
}

/// Failure of one bridged recall attempt. Every variant is a typed terminal
/// outcome; none of them is ever reported as an empty successful recall.
#[derive(Debug, thiserror::Error)]
pub enum CognitiveRecallPortError {
    /// The composition root mounted a disabled provider composition.
    #[error("memory-provider composition is disabled; no recall route exists")]
    CompositionDisabled,
    /// The exact scope could not be derived from the resolved scope.
    #[error("exact coding scope could not be derived: {0}")]
    Scope(#[source] ExactScopeBindingError),
    /// The host clock could not produce an evaluation instant.
    #[error("host clock unavailable for recall admission: {0}")]
    Clock(#[source] ClockError),
    /// The readiness handshake nonce could not be generated.
    #[error("host entropy unavailable for the recall readiness handshake")]
    EntropyUnavailable,
    /// A provider-neutral runtime value failed validation.
    #[error("recall call contract violation: {0}")]
    Contract(#[source] ApiError),
    /// The fabric refused the readiness handshake or the active call.
    #[error("memory fabric refused the recall: {0}")]
    Fabric(#[source] FabricError),
    /// The configured provider is registered in a non-active mode. Observer
    /// and disabled registrations can never be selected for product output;
    /// the refusal happens before any provider contact.
    #[error("configured recall provider {provider_id} is registered as {mode:?}, not active")]
    ProviderNotActive {
        /// Configured provider identity.
        provider_id: String,
        /// Its registered mode.
        mode: ProviderMode,
    },
    /// Routing refused the configured provider before any contact for a
    /// reason other than mode: it is not registered, is registered under
    /// another revision, or does not declare the recall capability.
    #[error("recall routing refused the configured provider: {0}")]
    Routing(#[source] RoutingError<RecallRoutePlanError>),
    /// The readiness handshake did not reach a successful terminal.
    #[error("recall readiness handshake terminated with {}", .terminal_code.as_wire())]
    HandshakeNotReady {
        /// Handshake terminal code.
        terminal_code: TerminalCode,
    },
    /// The provider rejected the host's own admitted scope or identity.
    #[error(
        "provider rejected the admitted scope with {} (diagnostic {diagnostic_id:?})",
        .terminal_code.as_wire()
    )]
    ScopeRejected {
        /// Terminal code the provider returned.
        terminal_code: TerminalCode,
        /// Provider diagnostic identity, when supplied.
        diagnostic_id: Option<String>,
    },
    /// The admitted candidates could not be normalized into the host
    /// candidate space.
    #[error("recall candidates could not be normalized: {0}")]
    Normalization(#[source] RecallNormalizationError),
    /// The normalized candidate set does not describe the admitted slice it
    /// was built from, so no honest deduplication is possible over it. The
    /// recall is refused rather than delivered with an unpruned or
    /// mis-attributed candidate stream.
    #[error("recall candidates could not be selected: {0}")]
    Selection(#[source] RecallSelectionError),
    /// The host selection budget for this recall is not a usable policy.
    #[error("recall selection policy is invalid: {0}")]
    SelectionPolicy(#[source] RecallSelectionPolicyError),
    /// The provider returned a failure terminal that is not a lane degradation.
    #[error(
        "provider recall failed with {} (diagnostic {diagnostic_id:?})",
        .terminal_code.as_wire()
    )]
    TerminalFailed {
        /// Terminal code the provider returned.
        terminal_code: TerminalCode,
        /// Provider diagnostic identity, when supplied.
        diagnostic_id: Option<String>,
    },
    /// The registry holds no recorded recall scope bindings for the provider
    /// that answered, so no candidate can be authorized and the reply is
    /// withheld rather than admitted on the provider's own say-so.
    #[error("recall provider {provider_id} has no recorded recall scope bindings")]
    ScopeBindingsUnrecorded {
        /// Provider that answered the recall.
        provider_id: String,
    },
    /// Host admission could not evaluate the reply.
    #[error("recall admission failed: {0}")]
    Admission(#[source] RecallAdmissionError),
    /// The admission report could not be retained by the audit sink, so the
    /// admitted result is withheld rather than delivered without its ledger.
    #[error("recall admission audit failed: {0}")]
    AdmissionAudit(#[source] RecallAdmissionAuditError),
    /// The admitted candidates could not be expressed as application values.
    #[error("recall result violates the application contract: {0}")]
    Application(#[source] ApplicationContractError),
    /// The blocking provider invocation did not complete.
    #[error("recall invocation task did not complete: {0}")]
    Invocation(#[source] tokio::task::JoinError),
    /// The live cancellation signal handed to the port is a different
    /// cancellation identity than the one the request names. An adapter may
    /// not replace the caller's cancellation identity, so the recall is
    /// refused rather than run under a token the runtime cannot cancel.
    #[error(
        "recall cancellation identity mismatch: request names {expected}, live signal is \
         {received}"
    )]
    CancellationIdentityMismatch {
        /// Cancellation token identity the application request carries.
        expected: String,
        /// Cancellation token identity of the live signal supplied.
        received: String,
    },
}

impl CognitiveRecallPortError {
    /// Stable machine-readable code of this terminal outcome.
    ///
    /// A recall failure crosses several boundaries before it reaches a
    /// receipt or an agent-visible notice. The typed variant is the authority
    /// on what went wrong, and this code is how that stays true after the
    /// error has been rendered: callers branch on the code, never on the
    /// message text.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::CompositionDisabled => "recall_composition_disabled",
            Self::Scope(_) => "recall_scope_not_derivable",
            Self::Clock(_) => "recall_host_clock_unavailable",
            Self::EntropyUnavailable => "recall_host_entropy_unavailable",
            Self::Contract(_) => "recall_call_contract_violation",
            Self::Fabric(_) => "recall_fabric_refused",
            Self::ProviderNotActive { .. } => "recall_provider_not_active",
            Self::Routing(_) => "recall_routing_refused",
            Self::HandshakeNotReady { .. } => "recall_handshake_not_ready",
            Self::ScopeRejected { .. } => "recall_scope_rejected",
            Self::Normalization(_) => "recall_normalization_failed",
            Self::Selection(_) => "recall_selection_failed",
            Self::SelectionPolicy(_) => "recall_selection_policy_invalid",
            Self::TerminalFailed { .. } => "recall_provider_terminal_failed",
            Self::ScopeBindingsUnrecorded { .. } => "recall_scope_bindings_unrecorded",
            Self::Admission(_) => "recall_admission_failed",
            Self::AdmissionAudit(_) => "recall_admission_audit_failed",
            Self::Application(_) => "recall_application_contract_violation",
            Self::Invocation(_) => "recall_invocation_incomplete",
            Self::CancellationIdentityMismatch { .. } => "recall_cancellation_identity_mismatch",
        }
    }
}

/// Failure building the handshake or call for one routed target.
#[derive(Debug, thiserror::Error)]
pub enum RecallRoutePlanError {
    /// A provider-neutral runtime value failed validation.
    #[error("recall call contract violation: {0}")]
    Contract(#[source] ApiError),
    /// The request payload could not be admitted.
    #[error("recall request payload could not be built: {0}")]
    Admission(#[source] RecallAdmissionError),
    /// The readiness handshake nonce could not be generated.
    #[error("host entropy unavailable for the recall readiness handshake")]
    EntropyUnavailable,
}

/// Inputs the composition root supplies to mount one project's recall port.
pub struct CognitiveRecallPortInputsV1 {
    /// Enabled provider composition. A disabled composition is refused at
    /// mount time.
    pub composition: Arc<ProjectMemoryProviderComposition>,
    /// Host authority that derives the exact coding scope.
    pub scope_binding: Arc<dyn ExactScopeBinding>,
    /// Audit sink for admission reports.
    pub admission_observer: Arc<dyn RecallAdmissionObserver>,
    /// Host-pinned routing policy: the one provider allowed to answer, the
    /// registration revision it must be registered under, and the fallback
    /// rule (forbidden unless explicitly pinned).
    pub routing: ActiveRoutingPolicy,
    /// Host limits the readiness handshake negotiates against.
    pub host_limits: ProviderLimits,
    /// Pinned recall policy revision carried in every request.
    pub policy_revision: u64,
    /// Admitted per-request budgets; `maximum_candidates` is clamped to the
    /// application request budget per call.
    pub budgets: RecallBudgetsV1,
}

/// One project's production cognitive-recall port over the provider
/// composition.
pub struct ProjectCognitiveRecallPortV1 {
    composition: Arc<ProjectMemoryProviderComposition>,
    scope_binding: Arc<dyn ExactScopeBinding>,
    admission_observer: Arc<dyn RecallAdmissionObserver>,
    routing: ActiveRoutingPolicy,
    host_limits: ProviderLimits,
    policy_revision: u64,
    budgets: RecallBudgetsV1,
    normalization: RecallNormalizationPolicyV1,
    selection: RecallSelectionPolicyV1,
}

impl fmt::Debug for ProjectCognitiveRecallPortV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProjectCognitiveRecallPortV1")
            .field("routing", &self.routing)
            .field("policy_revision", &self.policy_revision)
            .field("budgets", &self.budgets)
            .field("normalization", &self.normalization)
            .field("selection", &self.selection)
            .finish_non_exhaustive()
    }
}

/// A bridged recall result together with its audit-visible admission report.
#[derive(Clone, Debug)]
pub struct CognitiveRecallAdmittedOutcomeV1 {
    /// Application-facing result carrying only admitted inline candidates, in
    /// the host order [`Self::normalization`] recorded.
    pub result: CognitiveRecallResult,
    /// The admitted candidates in the host's common candidate space: the
    /// provider's native score and explanation retained verbatim alongside a
    /// separately labelled, deterministic host relevance. `None` when the lane
    /// degraded before any provider outcome existed.
    pub normalization: Option<RecallNormalizationV1>,
    /// The host selection receipt over [`Self::normalization`]: which
    /// normalized candidates were retained, and for every candidate that was
    /// not, whether it was a duplicate, redundant with a selected candidate,
    /// or did not fit the selection budget. `None` when the lane degraded
    /// before any provider outcome existed. The four ledgers account for
    /// every normalized candidate exactly once, so
    /// [`CognitiveRecallResult::candidates`] can always be reconciled back to
    /// the admitted set.
    pub selection: Option<RecallSelectionV1>,
    /// Rank-final admission ledger. `None` when the lane degraded before any
    /// provider outcome existed (cancelled, deadline elapsed, provider
    /// unavailable).
    pub report: Option<RecallAdmissionReport>,
    /// Admitted candidates that carried a content reference instead of
    /// inline content and were therefore withheld pending scope-revalidated
    /// hydration, which this port does not perform.
    pub unhydrated_reference_candidate_ids: Vec<String>,
    /// The router's fallback decision for this recall:
    /// [`FallbackDecision::NotApplicable`] for successful terminals and for
    /// lanes degraded before any provider contact, a typed declined reason
    /// when a failure terminal was returned as-is, or the dispatch evidence
    /// when the host-pinned rule admitted a second provider.
    pub fallback: FallbackDecision,
}

impl ProjectCognitiveRecallPortV1 {
    /// Mounts the port. A disabled composition has no recall route and is
    /// refused here rather than at first use.
    pub fn mount(inputs: CognitiveRecallPortInputsV1) -> Result<Self, CognitiveRecallPortError> {
        inputs
            .composition
            .registry()
            .ok_or(CognitiveRecallPortError::CompositionDisabled)?;
        inputs
            .budgets
            .validate()
            .map_err(CognitiveRecallPortError::Admission)?;
        // The mounted advisory-context budget is the host's own candidate
        // budget: no recall may ever select more candidates than the host
        // allowed the provider to return. Per call it is narrowed again to
        // the budget that recall actually dispatched.
        let selection = RecallSelectionPolicyV1::new(
            usize::try_from(inputs.budgets.maximum_candidates).unwrap_or(usize::MAX),
        )
        .map_err(CognitiveRecallPortError::SelectionPolicy)?;
        Ok(Self {
            composition: inputs.composition,
            scope_binding: inputs.scope_binding,
            admission_observer: inputs.admission_observer,
            routing: inputs.routing,
            host_limits: inputs.host_limits,
            policy_revision: inputs.policy_revision,
            budgets: inputs.budgets,
            normalization: RecallNormalizationPolicyV1::default(),
            selection,
        })
    }

    /// Pins a different host normalization policy revision.
    ///
    /// The policy is host-owned configuration, never a provider claim: the
    /// mounted default is the accepted revision and this setter exists so a
    /// revision change is an explicit, reviewable act.
    #[must_use]
    pub const fn with_normalization_policy(
        mut self,
        normalization: RecallNormalizationPolicyV1,
    ) -> Self {
        self.normalization = normalization;
        self
    }

    /// Pins a different host selection policy: the advisory-context budget
    /// and the similarity bars deduplication and diversity run at.
    ///
    /// The mounted default carries the host's own candidate budget and the
    /// pinned default thresholds. Every recall narrows the budget again to
    /// the candidate budget that recall dispatched, so this setter can only
    /// tighten what a recall selects, never widen it.
    #[must_use]
    pub const fn with_selection_policy(mut self, selection: RecallSelectionPolicyV1) -> Self {
        self.selection = selection;
        self
    }

    /// The host selection policy every recall of this port is pruned under.
    #[must_use]
    pub const fn selection_policy(&self) -> RecallSelectionPolicyV1 {
        self.selection
    }

    /// The host normalization policy every recall of this port is scored
    /// under.
    #[must_use]
    pub const fn normalization_policy(&self) -> RecallNormalizationPolicyV1 {
        self.normalization
    }

    /// Binds this port to one live host cancellation identity so it satisfies
    /// the application [`CognitiveRecallPort`] contract.
    ///
    /// The application trait carries only the request, and a recall must run
    /// under a cancellation signal the host runtime can still cancel while
    /// the provider is working. Binding is therefore the only way to obtain
    /// the trait object, and the bound signal is checked against the
    /// request's own token identity on every call.
    #[must_use]
    pub const fn bound(&self, cancellation: CancellationSignal) -> BoundCognitiveRecallPortV1<'_> {
        BoundCognitiveRecallPortV1 {
            port: self,
            cancellation,
        }
    }

    /// Performs one bridged recall and returns the result with its admission
    /// report. The report is also delivered to the admission observer.
    ///
    /// `cancellation` is the caller's live signal, not a copy: its token
    /// identity must equal the one the request carries, and cancelling it
    /// while the provider is working cancels the in-flight handshake, call,
    /// and any pinned fallback call.
    pub async fn recall_admitted(
        &self,
        request: CognitiveRecallRequest,
        cancellation: &CancellationSignal,
    ) -> Result<CognitiveRecallAdmittedOutcomeV1, CognitiveRecallPortError> {
        let outcome = self.recall_uncounted(request, cancellation).await?;
        if let Some(report) = &outcome.report {
            self.admission_observer
                .observe_admission(report)
                .map_err(CognitiveRecallPortError::AdmissionAudit)?;
        }
        Ok(outcome)
    }

    /// The identity of the host-configured provider before any contact: the
    /// pinned provider and revision, with no runtime instance yet.
    fn configured_identity(
        &self,
    ) -> Result<CognitiveRecallProviderIdentity, CognitiveRecallPortError> {
        CognitiveRecallProviderIdentity::configured(
            self.routing.active_provider().as_str(),
            self.routing.registration_revision(),
        )
        .map_err(CognitiveRecallPortError::Application)
    }

    async fn recall_uncounted(
        &self,
        request: CognitiveRecallRequest,
        cancellation: &CancellationSignal,
    ) -> Result<CognitiveRecallAdmittedOutcomeV1, CognitiveRecallPortError> {
        request
            .validate()
            .map_err(CognitiveRecallPortError::Application)?;
        // The live signal must be the request's own cancellation identity.
        // Accepting a foreign token here would be exactly the substitution
        // the application contract forbids: the provider would then hold a
        // token nothing in the runtime cancels.
        let live_identity = cancellation.context().token_id;
        if live_identity.as_str() != request.cancellation().token_id.as_str() {
            return Err(CognitiveRecallPortError::CancellationIdentityMismatch {
                expected: request.cancellation().token_id.as_str().to_owned(),
                received: live_identity.as_str().to_owned(),
            });
        }
        let now = try_now_micros().map_err(CognitiveRecallPortError::Clock)?;
        if request.cancellation().is_cancelled() || cancellation.is_cancelled() {
            return degraded_outcome(
                &request,
                self.configured_identity()?,
                CognitiveRecallDegradation::Cancelled,
            );
        }
        if request.deadline().is_elapsed_at(now) {
            return degraded_outcome(
                &request,
                self.configured_identity()?,
                CognitiveRecallDegradation::TimedOut,
            );
        }

        let exact_scope = self
            .scope_binding
            .bind_exact_scope(request.scope())
            .map_err(CognitiveRecallPortError::Scope)?;
        self.composition
            .registry()
            .ok_or(CognitiveRecallPortError::CompositionDisabled)?;

        let deadline_utc_micros = request.deadline().expires_at.0;
        let remaining_millis = u64::try_from(
            deadline_utc_micros
                .saturating_sub(now.0)
                .checked_div(1_000)
                .unwrap_or(0),
        )
        .unwrap_or(0);
        let evaluation_time = rfc3339_utc_micros(now.0).ok_or(CognitiveRecallPortError::Clock(
            ClockError::OverflowsI64Micros,
        ))?;
        let temporal = AdmittedTemporalQuery::current(&evaluation_time)
            .map_err(CognitiveRecallPortError::Admission)?;
        // The candidate budget the provider is told about is the smaller of
        // the application request and the host-owned budget; admission below
        // enforces exactly that dispatched value, never the unclamped request.
        let budgets = RecallBudgetsV1 {
            maximum_candidates: u64::try_from(request.maximum_candidates())
                .unwrap_or(u64::MAX)
                .min(self.budgets.maximum_candidates),
            ..self.budgets
        };
        let admitted_budget = usize::try_from(budgets.maximum_candidates).unwrap_or(usize::MAX);
        let recall_capability = OwnedVersionedId::new(RECALL_QUERY_CAPABILITY_ID)
            .map_err(CognitiveRecallPortError::Contract)?;
        // One live token for this recall, cancelled by the caller's own
        // signal. The bridge task is aborted on every exit path by the guard,
        // so a completed recall leaves nothing waiting on the signal.
        let (cancellation_token, _cancellation_bridge) = bridge_cancellation(cancellation);
        let plan = RecallCallPlan {
            cancellation: cancellation_token,
            exact_scope,
            request_id: request.request_id().as_str().to_owned(),
            query: request.query().to_owned(),
            temporal: temporal.clone(),
            budgets,
            policy_revision: self.policy_revision,
            host_limits: self.host_limits,
            deadline_utc_micros,
            remaining_millis,
            recall_capability,
        };

        // Provider ports perform synchronous store reads; keep them off the
        // async executor while the call's own deadline bounds them. Routing
        // itself — pre-contact admission of the pinned provider, the fresh
        // handshake, the call, and any explicitly pinned fallback — happens
        // inside the fabric so no second selection authority exists here.
        let composition = Arc::clone(&self.composition);
        let routing = self.routing.clone();
        let routed = tokio::task::spawn_blocking(move || {
            composition
                .registry()
                .ok_or(CognitiveRecallPortError::CompositionDisabled)
                .and_then(|registry| {
                    registry
                        .route_active(&routing, RECALL_QUERY_CAPABILITY_ID, &plan)
                        .map_err(routing_error)
                })
        })
        .await
        .map_err(CognitiveRecallPortError::Invocation)??;
        let identity = attributed_identity(&routed.identity)?;
        let call = routed.call;
        let reply = routed.reply;
        let fallback = routed.fallback;

        match classify_terminal(&reply) {
            TerminalDisposition::Admit => {}
            TerminalDisposition::Degrade(degradation) => {
                let mut outcome = degraded_outcome(&request, identity, degradation)?;
                outcome.fallback = fallback;
                return Ok(outcome);
            }
            TerminalDisposition::ScopeRejected => {
                return Err(CognitiveRecallPortError::ScopeRejected {
                    terminal_code: reply.terminal.terminal_code(),
                    diagnostic_id: reply.terminal.diagnostic_id().map(str::to_owned),
                });
            }
            TerminalDisposition::Failed => {
                return Err(CognitiveRecallPortError::TerminalFailed {
                    terminal_code: reply.terminal.terminal_code(),
                    diagnostic_id: reply.terminal.diagnostic_id().map(str::to_owned),
                });
            }
        }

        // Authorization for candidate scope bindings is the registry's
        // record from registration, looked up by the provider the admitted
        // call targeted; nothing in the reply can supply or widen it.
        let authorized = self
            .composition
            .registry()
            .ok_or(CognitiveRecallPortError::CompositionDisabled)?
            .recall_scope_bindings(&call.provider_id)
            .cloned()
            .ok_or_else(|| CognitiveRecallPortError::ScopeBindingsUnrecorded {
                provider_id: call.provider_id.as_str().to_owned(),
            })?;
        let admission = admit_recall_reply(&call, &temporal, admitted_budget, &authorized, &reply)
            .map_err(CognitiveRecallPortError::Admission)?;
        // Host order is the normalization's order, not the provider's: the
        // provider's own rank is retained inside the normalized set so the
        // reordering stays explainable, and no raw provider score is ever
        // compared against another provider's.
        let normalization = normalize_admitted_candidates(self.normalization, &admission.admitted)
            .map_err(CognitiveRecallPortError::Normalization)?;
        // Redundant evidence is pruned before anything consumes the
        // application's result budget: a duplicate the provider returned twice
        // must never displace a distinct candidate. The selection budget is
        // the host policy narrowed to the candidate budget this recall
        // actually dispatched, and the whole decision receipt travels with the
        // outcome so every dropped candidate stays explainable.
        let selection = select_recall_candidates(
            self.selection
                .narrowed_to(admitted_budget)
                .map_err(CognitiveRecallPortError::SelectionPolicy)?,
            &normalization,
            &admission.admitted,
        )
        .map_err(CognitiveRecallPortError::Selection)?;
        let mut candidates = Vec::with_capacity(selection.selected.len());
        let mut unhydrated_reference_candidate_ids = Vec::new();
        for selected in &selection.selected {
            let Some(admitted) = admission.admitted.get(selected.provider_rank) else {
                // Selection verified every rank against this exact slice, so
                // reaching this is a host-internal inconsistency. It is
                // reported, never skipped.
                return Err(CognitiveRecallPortError::Selection(
                    RecallSelectionError::ProviderRankOutOfRange {
                        candidate_id: selected.candidate_id.clone(),
                        provider_rank: selected.provider_rank,
                        admitted_len: admission.admitted.len(),
                    },
                ));
            };
            let candidate = admitted.candidate();
            let content = match admitted.content() {
                RecallCandidateContent::Inline(content) => content,
                RecallCandidateContent::Reference(_) => {
                    unhydrated_reference_candidate_ids.push(candidate.candidate_id.clone());
                    continue;
                }
            };
            let provenance =
                application_provenance(candidate).map_err(CognitiveRecallPortError::Application)?;
            let mut application_candidate =
                CognitiveRecallCandidate::new(candidate.candidate_id.clone(), content, provenance)
                    .map_err(CognitiveRecallPortError::Application)?;
            if let Some(stable_memory_ref) = &candidate.stable_memory_ref {
                application_candidate = application_candidate
                    .with_stable_reference(stable_memory_ref.clone())
                    .map_err(CognitiveRecallPortError::Application)?;
            }
            if let Some(summary) = candidate
                .explanation
                .get("summary")
                .and_then(serde_json::Value::as_str)
                .filter(|summary| !summary.is_empty())
            {
                application_candidate = application_candidate
                    .with_explanation(summary)
                    .map_err(CognitiveRecallPortError::Application)?;
            }
            candidates.push(application_candidate);
        }

        let degradation = if reply.terminal.terminal_code() == TerminalCode::Partial
            || !unhydrated_reference_candidate_ids.is_empty()
        {
            Some(CognitiveRecallDegradation::Partial)
        } else if admission.report.degraded {
            Some(CognitiveRecallDegradation::Stale)
        } else {
            None
        };
        let result = CognitiveRecallResult::new(
            request.scope().clone(),
            request.request_id().clone(),
            identity,
            candidates,
            degradation,
        )
        .map_err(CognitiveRecallPortError::Application)?;
        result
            .validate_for(&request)
            .map_err(CognitiveRecallPortError::Application)?;
        Ok(CognitiveRecallAdmittedOutcomeV1 {
            result,
            normalization: Some(normalization),
            selection: Some(selection),
            report: Some(admission.report),
            unhydrated_reference_candidate_ids,
            fallback,
        })
    }
}

/// A mounted recall port bound to one live host cancellation identity.
///
/// This, and not the bare port, is what implements the application
/// [`CognitiveRecallPort`] trait: the trait's method signature carries no
/// cancellation handle, and manufacturing one inside the adapter would leave
/// the provider holding a token no runtime can cancel. Binding forces the
/// caller to hand over the signal it will actually cancel.
pub struct BoundCognitiveRecallPortV1<'port> {
    port: &'port ProjectCognitiveRecallPortV1,
    cancellation: CancellationSignal,
}

impl fmt::Debug for BoundCognitiveRecallPortV1<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundCognitiveRecallPortV1")
            .field("port", &self.port)
            .field("cancellation", &self.cancellation.context().token_id)
            .finish_non_exhaustive()
    }
}

impl BoundCognitiveRecallPortV1<'_> {
    /// Performs one bridged recall under the bound live cancellation
    /// identity and returns the result with its admission report.
    pub async fn recall_admitted(
        &self,
        request: CognitiveRecallRequest,
    ) -> Result<CognitiveRecallAdmittedOutcomeV1, CognitiveRecallPortError> {
        self.port.recall_admitted(request, &self.cancellation).await
    }
}

impl CognitiveRecallPort for BoundCognitiveRecallPortV1<'_> {
    type Error = CognitiveRecallPortError;

    async fn recall(
        &self,
        request: CognitiveRecallRequest,
    ) -> Result<CognitiveRecallPortResult, Self::Error> {
        Ok(self.recall_admitted(request).await?.result)
    }
}

/// A guard that stops the cancellation bridge on every exit path.
struct CancellationBridgeGuard(tokio::task::JoinHandle<()>);

impl Drop for CancellationBridgeGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Bridges the caller's live application cancellation signal onto the one
/// provider-facing token this recall dispatches with.
///
/// The token is not a snapshot: cancelling `signal` at any point — before
/// dispatch, between the handshake and the call, or while the provider is
/// blocked in its store — cancels this exact token, which is the token every
/// control of the recall already holds.
fn bridge_cancellation(
    signal: &CancellationSignal,
) -> (CancellationToken, CancellationBridgeGuard) {
    let token = CancellationToken::new();
    if signal.is_cancelled() {
        token.cancel();
    }
    let bridged = token.clone();
    let signal = signal.clone();
    let task = tokio::spawn(async move {
        signal.cancelled().await;
        bridged.cancel();
    });
    (token, CancellationBridgeGuard(task))
}

/// Host-owned request construction for whichever provider the router
/// selects. The same plan is asked again for a pinned fallback target, so a
/// second provider only ever sees a handshake and a call bound to its own
/// identity, registration revision, ready receipt, and state generation.
struct RecallCallPlan {
    /// The one live cancellation token of this recall, cloned into every
    /// handshake and call control so a cancellation requested after dispatch
    /// is observed by whichever provider is currently working.
    cancellation: CancellationToken,
    exact_scope: OwnedExactScope,
    request_id: String,
    query: String,
    temporal: AdmittedTemporalQuery,
    budgets: RecallBudgetsV1,
    policy_revision: u64,
    host_limits: ProviderLimits,
    deadline_utc_micros: i64,
    remaining_millis: u64,
    recall_capability: OwnedVersionedId,
}

impl RecallCallPlan {
    fn control(&self) -> OperationControl {
        OperationControl::new(
            self.deadline_utc_micros,
            self.remaining_millis,
            // Clone, never `new()`: the handshake, the primary call, and any
            // pinned fallback call all observe the caller's one live token.
            self.cancellation.clone(),
        )
    }
}

impl ActiveCallPlan for RecallCallPlan {
    type Error = RecallRoutePlanError;

    fn handshake_request(&self, target: &RouteTarget) -> Result<HandshakeRequest, Self::Error> {
        HandshakeRequest::new(HandshakeRequestParts {
            provider_id: target.provider_id.clone(),
            registration_revision: target.registration_revision,
            exact_scope: self.exact_scope.clone(),
            request_id: format!("recall-readiness.{}", self.request_id),
            required_capabilities: vec![self.recall_capability.clone()],
            host_limits: self.host_limits,
            control: self.control(),
            challenge_nonce: challenge_nonce()?,
        })
        .map_err(RecallRoutePlanError::Contract)
    }

    fn provider_call(&self, target: &ReadyRouteTarget) -> Result<ProviderCall, Self::Error> {
        // The request payload must echo exactly the control the call carries:
        // the provider verifies the deadline and refuses a remaining budget
        // larger than the one the host actually dispatched.
        let call_control = self.control();
        let payload = build_recall_request_payload(&RecallRequestParts {
            provider_id: target.provider_id.clone(),
            registration_revision: target.registration_revision,
            ready_receipt_sha256: target.ready_receipt_sha256.clone(),
            exact_scope: self.exact_scope.clone(),
            request_id: self.request_id.clone(),
            objective: RECALL_OBJECTIVE.to_owned(),
            query: self.query.clone(),
            temporal: self.temporal.clone(),
            budgets: self.budgets,
            policy_revision: self.policy_revision,
            deadline_utc_micros: call_control.deadline_utc_micros(),
            remaining_millis: call_control.remaining_millis(),
        })
        .map_err(RecallRoutePlanError::Admission)?;
        ProviderCall::new(ProviderCallParts {
            operation: ProviderOperation::Recall,
            provider_id: target.provider_id.clone(),
            registration_revision: target.registration_revision,
            ready_receipt_sha256: target.ready_receipt_sha256.clone(),
            exact_scope: self.exact_scope.clone(),
            request_id: self.request_id.clone(),
            operation_id: format!("recall.{}", self.request_id),
            expected_state_generation: target.descriptor.state_generation,
            idempotency_key: None,
            control: call_control,
            payload,
            required_capabilities: vec![self.recall_capability.clone()],
            extensions: Vec::new(),
        })
        .map_err(RecallRoutePlanError::Contract)
    }
}

/// Draws the readiness-handshake challenge nonce from host entropy.
fn challenge_nonce() -> Result<[u8; 32], RecallRoutePlanError> {
    let mut nonce = [0u8; 32];
    getrandom::getrandom(&mut nonce).map_err(|_| RecallRoutePlanError::EntropyUnavailable)?;
    Ok(nonce)
}

/// Maps a routing refusal onto the port's typed terminal failures without
/// collapsing distinct causes.
fn routing_error(error: RoutingError<RecallRoutePlanError>) -> CognitiveRecallPortError {
    match error {
        RoutingError::Fabric(error) => CognitiveRecallPortError::Fabric(error),
        RoutingError::Plan(RecallRoutePlanError::Contract(error)) => {
            CognitiveRecallPortError::Contract(error)
        }
        RoutingError::Plan(RecallRoutePlanError::Admission(error)) => {
            CognitiveRecallPortError::Admission(error)
        }
        RoutingError::Plan(RecallRoutePlanError::EntropyUnavailable) => {
            CognitiveRecallPortError::EntropyUnavailable
        }
        RoutingError::ProviderNotActive { provider_id, mode } => {
            CognitiveRecallPortError::ProviderNotActive {
                provider_id: provider_id.as_str().to_owned(),
                mode,
            }
        }
        RoutingError::HandshakeNotReady { terminal_code, .. } => {
            CognitiveRecallPortError::HandshakeNotReady { terminal_code }
        }
        other @ (RoutingError::ProviderNotRegistered { .. }
        | RoutingError::RegistrationRevisionMismatch { .. }
        | RoutingError::CapabilityUndeclared { .. }
        | RoutingError::HandshakeIncomplete { .. }) => CognitiveRecallPortError::Routing(other),
    }
}

/// The identity of the provider that actually answered, as the router bound
/// it: the fresh handshake's runtime instance is attached to the pinned
/// provider and revision.
fn attributed_identity(
    identity: &RoutedProviderIdentity,
) -> Result<CognitiveRecallProviderIdentity, CognitiveRecallPortError> {
    CognitiveRecallProviderIdentity::configured(
        identity.provider_id.as_str(),
        identity.registration_revision,
    )
    .and_then(|configured| configured.with_instance(identity.provider_instance_id.clone()))
    .map_err(CognitiveRecallPortError::Application)
}

enum TerminalDisposition {
    Admit,
    Degrade(CognitiveRecallDegradation),
    ScopeRejected,
    Failed,
}

fn classify_terminal(reply: &ProviderReply) -> TerminalDisposition {
    match reply.terminal.terminal_code() {
        TerminalCode::Success | TerminalCode::SuccessZeroResults | TerminalCode::Partial => {
            TerminalDisposition::Admit
        }
        TerminalCode::DeadlineExceeded => {
            TerminalDisposition::Degrade(CognitiveRecallDegradation::TimedOut)
        }
        TerminalCode::Cancelled => {
            TerminalDisposition::Degrade(CognitiveRecallDegradation::Cancelled)
        }
        TerminalCode::ProviderUnavailable => {
            TerminalDisposition::Degrade(CognitiveRecallDegradation::Unavailable)
        }
        TerminalCode::CapabilityUnsupported => {
            TerminalDisposition::Degrade(CognitiveRecallDegradation::Unsupported)
        }
        TerminalCode::CapacityExceeded => {
            TerminalDisposition::Degrade(CognitiveRecallDegradation::BudgetExhausted)
        }
        TerminalCode::ScopeUnavailable
        | TerminalCode::ScopeMismatch
        | TerminalCode::StaleIdentity => TerminalDisposition::ScopeRejected,
        _ => TerminalDisposition::Failed,
    }
}

fn degraded_outcome(
    request: &CognitiveRecallRequest,
    provider: CognitiveRecallProviderIdentity,
    degradation: CognitiveRecallDegradation,
) -> Result<CognitiveRecallAdmittedOutcomeV1, CognitiveRecallPortError> {
    let result = CognitiveRecallResult::degraded(
        request.scope().clone(),
        request.request_id().clone(),
        provider,
        Vec::new(),
        degradation,
    )
    .map_err(CognitiveRecallPortError::Application)?;
    Ok(CognitiveRecallAdmittedOutcomeV1 {
        result,
        normalization: None,
        selection: None,
        report: None,
        unhydrated_reference_candidate_ids: Vec::new(),
        fallback: FallbackDecision::NotApplicable,
    })
}

/// Projects the provider provenance record onto the application's explicit
/// provenance states without fabricating a source label.
fn application_provenance(
    candidate: &RecallCandidateV1,
) -> Result<CognitiveRecallProvenance, ApplicationContractError> {
    let state = candidate
        .provenance
        .get("state")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unavailable");
    match state {
        "available" => {
            let refs_at = |key: &str| {
                candidate
                    .provenance
                    .get(key)
                    .and_then(serde_json::Value::as_array)
                    .map(|refs| {
                        refs.iter()
                            .filter_map(serde_json::Value::as_str)
                            .map(str::to_owned)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            };
            // Ordered by evidential value, not by convenience.
            // `stable_memory_ref` is a *dedup identity*; the provenance
            // record's own refs are what the provider offered as evidence.
            // The host prefers whichever of them names one of its own
            // reference shapes, so a candidate that carries a citable
            // reference is never demoted because a dedup key happened to
            // sort first. Shape recognition here is only a preference: the
            // reference still has to be confirmed against real host storage
            // in `recall_provenance_hydration`.
            let mut ordered = Vec::new();
            ordered.extend(refs_at("origin_refs"));
            ordered.extend(refs_at("source_refs"));
            ordered.extend(candidate.source_refs.iter().cloned());
            let citable = ordered
                .iter()
                .find(|reference| {
                    crate::recall_provenance_hydration::HostEvidenceRefV1::parse(reference).is_ok()
                })
                .cloned();
            let source = citable
                .or_else(|| candidate.stable_memory_ref.clone())
                .or_else(|| ordered.into_iter().next());
            match source {
                Some(source) => CognitiveRecallProvenance::available(source),
                // A provider that claims availability without naming any
                // source has not established provenance; say so explicitly.
                None => Ok(CognitiveRecallProvenance::unavailable()),
            }
        }
        "redacted" => {
            let reason = candidate
                .provenance
                .get("redaction_reason")
                .and_then(serde_json::Value::as_str)
                .filter(|reason| !reason.trim().is_empty())
                .unwrap_or("provider_redacted");
            CognitiveRecallProvenance::redacted(reason)
        }
        _ => Ok(CognitiveRecallProvenance::unavailable()),
    }
}
