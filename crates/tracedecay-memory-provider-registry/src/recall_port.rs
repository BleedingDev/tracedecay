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
//! * the application deadline and cancellation snapshot are carried into the
//!   provider call unchanged, and a request that is already cancelled or past
//!   its deadline is answered with a typed degradation without contacting the
//!   provider;
//! * every provider reply is passed through rank-final
//!   [`admit_recall_reply`], so a candidate that fails exact-scope, identity,
//!   validity, or revocation checks can only reach the caller as a row in the
//!   [`RecallAdmissionReport`] ledger, never as advisory content;
//! * non-success terminals and fabric errors are typed: lane degradations
//!   (`deadline_exceeded`, `cancelled`, `provider_unavailable`,
//!   `capability_unsupported`, `capacity_exceeded`) become
//!   [`CognitiveRecallDegradation`], and a provider that rejects the host's
//!   own scope is an error, never an empty success;
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
use tracedecay_application::{ApplicationContractError, ClockError, ResolvedScope, try_now_micros};
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
}

impl fmt::Debug for ProjectCognitiveRecallPortV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProjectCognitiveRecallPortV1")
            .field("routing", &self.routing)
            .field("policy_revision", &self.policy_revision)
            .field("budgets", &self.budgets)
            .finish_non_exhaustive()
    }
}

/// A bridged recall result together with its audit-visible admission report.
#[derive(Clone, Debug)]
pub struct CognitiveRecallAdmittedOutcomeV1 {
    /// Application-facing result carrying only admitted inline candidates.
    pub result: CognitiveRecallResult,
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
        Ok(Self {
            composition: inputs.composition,
            scope_binding: inputs.scope_binding,
            admission_observer: inputs.admission_observer,
            routing: inputs.routing,
            host_limits: inputs.host_limits,
            policy_revision: inputs.policy_revision,
            budgets: inputs.budgets,
        })
    }

    /// Performs one bridged recall and returns the result with its admission
    /// report. The report is also delivered to the admission observer.
    pub async fn recall_admitted(
        &self,
        request: CognitiveRecallRequest,
    ) -> Result<CognitiveRecallAdmittedOutcomeV1, CognitiveRecallPortError> {
        let outcome = self.recall_uncounted(request).await?;
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
    ) -> Result<CognitiveRecallAdmittedOutcomeV1, CognitiveRecallPortError> {
        request
            .validate()
            .map_err(CognitiveRecallPortError::Application)?;
        let now = try_now_micros().map_err(CognitiveRecallPortError::Clock)?;
        if request.cancellation().is_cancelled() {
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
        let plan = RecallCallPlan {
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
        let mut candidates = Vec::with_capacity(admission.admitted.len());
        let mut unhydrated_reference_candidate_ids = Vec::new();
        for admitted in &admission.admitted {
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
            report: Some(admission.report),
            unhydrated_reference_candidate_ids,
            fallback,
        })
    }
}

impl CognitiveRecallPort for ProjectCognitiveRecallPortV1 {
    type Error = CognitiveRecallPortError;

    async fn recall(
        &self,
        request: CognitiveRecallRequest,
    ) -> Result<CognitiveRecallPortResult, Self::Error> {
        Ok(self.recall_admitted(request).await?.result)
    }
}

/// Host-owned request construction for whichever provider the router
/// selects. The same plan is asked again for a pinned fallback target, so a
/// second provider only ever sees a handshake and a call bound to its own
/// identity, registration revision, ready receipt, and state generation.
struct RecallCallPlan {
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
            CancellationToken::new(),
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
            let first_ref = |key: &str| {
                candidate
                    .provenance
                    .get(key)
                    .and_then(serde_json::Value::as_array)
                    .and_then(|refs| refs.first())
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            };
            let source = candidate
                .stable_memory_ref
                .clone()
                .or_else(|| first_ref("origin_refs"))
                .or_else(|| first_ref("source_refs"))
                .or_else(|| candidate.source_refs.first().cloned());
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
