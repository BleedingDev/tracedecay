//! Authenticated retained provider-control execution over existing project mounts.
//!
//! Public references select retained host evidence. They never supply provider
//! authority, a replacement scope, or a provider common request header.

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use serde_json::Value;
use tracedecay_contracts::retained_surfaces::{
    ProviderControlRequestV1, ProviderControlResultV1, ProviderControlSourceSelectorV1,
    ProviderControlStateSelectorV1, RetainedProviderControlExecutionPortV1,
    RetainedSurfaceOperation,
};
use tracedecay_contracts::{
    EffectReceipt, ResolvedScope, RetainedSurfaceExecutionContextV1,
    RetainedSurfaceExecutionErrorV1, RetainedSurfaceExecutionFutureV1,
};
use tracedecay_domain::{ManifestDigest, ProjectId, UserProfileId, canonical_json_bytes};
use tracedecay_global_db::RegisteredGlobalDbLeaseV1;
use tracedecay_memory_provider_registry::{
    GrantedHistorySource, HistoryGrant, LifecycleTarget, LifecycleTargetReference,
    OperationControl, OwnedProviderId, ProjectMemoryProviderComposition, ProviderOperation,
    TerminalCode,
};
use tracedecay_runtime_core::db::Database;

use super::cognitive_recall::{
    RecallAdmissionLedgerV1,
    control_attribution::{
        RecallControlAttributionErrorV1, RecallControlItemRefV1, RecallControlTraceRefV1,
        RetainedRecallControlScopeV1,
    },
};
use super::observation_journey::{
    ProjectObservationJourneyV1,
    control_dispatch::{
        JourneyControlDispatchErrorV1, JourneyControlDispatchReplyV1,
        JourneyControlDispatchRequestV1, JourneyControlNotDispatchedV1,
    },
};
use super::provider_history::ProviderHistoryErrorV1;
use authority::{AuthorizedRetainedControlSourceV1, ProviderControlAuthorityV1};

pub(crate) mod authority;
mod feedback_receipt;
mod outcome;
pub(crate) mod portability;
mod projection;
mod source_controls;
mod state_controls;

/// Revision of the built-in host provider-control admission and redaction rules.
pub(crate) const PROVIDER_CONTROL_POLICY_REVISION_V1: u64 = 1;

pub(super) type ControlResult<T> = Result<T, ControlFailureV1>;

/// Actual mounted project authorities, assembled once during full project open.
pub(crate) struct ProviderControlMountInputsV1 {
    pub(crate) authority: Option<Arc<ProviderControlAuthorityV1>>,
    pub(crate) composition: Arc<ProjectMemoryProviderComposition>,
    pub(crate) journeys: Vec<Arc<ProjectObservationJourneyV1>>,
    pub(crate) profile_id: UserProfileId,
    pub(crate) mounted_scope: ResolvedScope,
    pub(crate) authoritative_project_id: ProjectId,
    pub(crate) project_root: PathBuf,
    pub(crate) configuration_digest: ManifestDigest,
    pub(crate) canonical_session_db: RegisteredGlobalDbLeaseV1,
    pub(crate) canonical_dispositions: Arc<Database>,
}

pub(super) struct ProjectProviderControlPortV1 {
    pub(super) inputs: ProviderControlMountInputsV1,
}

pub(crate) fn project_provider_control_port(
    inputs: ProviderControlMountInputsV1,
) -> Arc<dyn RetainedProviderControlExecutionPortV1> {
    Arc::new(ProjectProviderControlPortV1 { inputs })
}

/// Provider-only attempts get a UUIDv7. Durable source commands instead pass
/// their accepted command identity unchanged into dispatch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ControlOperationIdentityV1 {
    pub(super) operation_id: String,
    pub(super) idempotency_key: Option<String>,
}

pub(super) struct CompletedProviderControlV1 {
    pub(super) result: ProviderControlResultV1,
    /// The actual host deletion fence receipt, never a provider-effect guess.
    pub(super) host_receipt: Option<EffectReceipt>,
}

pub(super) struct ControlFailureV1 {
    pub(super) stage: ControlFailureStageV1,
    pub(super) host_receipt: Option<EffectReceipt>,
}

/// Preserve post-contact evidence until the outer outcome owner interprets it.
pub(super) enum ControlFailureStageV1 {
    Request(RetainedSurfaceExecutionErrorV1),
    Attribution(RecallControlAttributionErrorV1),
    Authority(ProviderHistoryErrorV1),
    Control(TerminalCode),
    InvalidBinding(&'static str),
    MissingAuthority(&'static str),
    BlockingRead(&'static str),
    Offline {
        state: RetainedRecallControlScopeV1,
        identity: ControlOperationIdentityV1,
        reason: &'static str,
    },
    /// A host intent write was attempted, but no durable outcome was confirmed.
    HostIntentUnknown {
        state: RetainedRecallControlScopeV1,
        identity: ControlOperationIdentityV1,
        detail: &'static str,
    },
    Dispatch {
        state: RetainedRecallControlScopeV1,
        identity: ControlOperationIdentityV1,
        error: JourneyControlDispatchErrorV1,
    },
    /// A blocking worker disappeared after it could have entered dispatch.
    DispatchWorker {
        state: RetainedRecallControlScopeV1,
        identity: ControlOperationIdentityV1,
    },
    /// A verified provider reply preceded a host artifact retention failure.
    /// Preserve both pieces of evidence for the outer durability outcome.
    PortabilityArtifact {
        error: portability::PortabilityErrorV1,
        dispatched: JourneyControlDispatchReplyV1,
    },
    Projection {
        field: &'static str,
        dispatched: JourneyControlDispatchReplyV1,
    },
}

impl std::fmt::Debug for ControlFailureV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stage = match &self.stage {
            ControlFailureStageV1::Request(_) => "request",
            ControlFailureStageV1::Attribution(_) => "attribution",
            ControlFailureStageV1::Authority(_) => "authority",
            ControlFailureStageV1::Control(_) => "control",
            ControlFailureStageV1::InvalidBinding(_) => "binding",
            ControlFailureStageV1::MissingAuthority(_) => "missing_authority",
            ControlFailureStageV1::BlockingRead(_) => "blocking_read",
            ControlFailureStageV1::Offline { .. } => "offline",
            ControlFailureStageV1::HostIntentUnknown { .. } => "host_intent_unknown",
            ControlFailureStageV1::Dispatch { .. } => "dispatch",
            ControlFailureStageV1::DispatchWorker { .. } => "dispatch_worker",
            ControlFailureStageV1::PortabilityArtifact { .. } => "portability_artifact",
            ControlFailureStageV1::Projection { .. } => "projection",
        };
        formatter
            .debug_struct("ControlFailureV1")
            .field("stage", &stage)
            .field("has_host_receipt", &self.host_receipt.is_some())
            .finish()
    }
}

impl ControlFailureV1 {
    pub(super) fn new(stage: ControlFailureStageV1) -> Self {
        Self {
            stage,
            host_receipt: None,
        }
    }

    pub(super) fn with_host_receipt(mut self, receipt: EffectReceipt) -> Self {
        self.host_receipt = Some(receipt);
        self
    }
}

impl From<RetainedSurfaceExecutionErrorV1> for ControlFailureV1 {
    fn from(error: RetainedSurfaceExecutionErrorV1) -> Self {
        Self::new(ControlFailureStageV1::Request(error))
    }
}

impl From<RecallControlAttributionErrorV1> for ControlFailureV1 {
    fn from(error: RecallControlAttributionErrorV1) -> Self {
        Self::new(ControlFailureStageV1::Attribution(error))
    }
}

impl From<ProviderHistoryErrorV1> for ControlFailureV1 {
    fn from(error: ProviderHistoryErrorV1) -> Self {
        Self::new(ControlFailureStageV1::Authority(error))
    }
}

pub(super) struct ResolvedControlStateV1 {
    pub(super) retained: RetainedRecallControlScopeV1,
    pub(super) journey: Option<Arc<ProjectObservationJourneyV1>>,
}

pub(super) struct AuthorizedControlSourceV1 {
    pub(super) authorized: AuthorizedRetainedControlSourceV1,
    pub(super) target: LifecycleTarget,
}

impl AuthorizedControlSourceV1 {
    pub(super) fn granted_source(&self) -> ControlResult<&GrantedHistorySource> {
        let [source] = self.authorized.grant.sources.as_slice() else {
            return Err(ControlFailureV1::new(
                ControlFailureStageV1::InvalidBinding("one authorized original source"),
            ));
        };
        Ok(source)
    }

    pub(super) fn projection_evidence<'a>(
        &'a self,
        selector: &'a ProviderControlSourceSelectorV1,
    ) -> ControlResult<projection::ResolvedControlSourceV1<'a>> {
        let source = self.granted_source()?;
        Ok(projection::ResolvedControlSourceV1 {
            selector,
            target: &self.target,
            original_attribution: &source.attribution,
            current_disposition: &source.current_disposition,
        })
    }
}

#[repr(u8)]
#[derive(Clone, Copy)]
enum ControlStopCauseV1 {
    Caller = 1,
    Deadline = 2,
}

/// One authenticated request's controls. The monotonic deadline is fixed once;
/// every clone of OperationControl shares the original elapsed budget and token.
pub(super) struct ControlInvocationV1<'a, 'context> {
    pub(super) context: &'a RetainedSurfaceExecutionContextV1<'context>,
    pub(super) request: &'a ProviderControlRequestV1,
    pub(super) control: OperationControl,
    pub(super) identity: ControlOperationIdentityV1,
    deadline: tokio::time::Instant,
    /// Shared by nested stages; the first observed host stop cause wins.
    stop_cause: AtomicU8,
}

impl<'a, 'context> ControlInvocationV1<'a, 'context> {
    fn new(
        context: &'a RetainedSurfaceExecutionContextV1<'context>,
        request: &'a ProviderControlRequestV1,
    ) -> ControlResult<Self> {
        context.request_context.validate().map_err(|_| {
            ControlFailureV1::new(ControlFailureStageV1::InvalidBinding("request context"))
        })?;
        request.validate_at(context.observed_at).map_err(|_| {
            ControlFailureV1::new(ControlFailureStageV1::Request(
                RetainedSurfaceExecutionErrorV1::InvalidRequest,
            ))
        })?;
        let expected =
            tracedecay_contracts::retained_surface_application_operation(request.operation())
                .map_err(|_| {
                    ControlFailureV1::new(ControlFailureStageV1::InvalidBinding(
                        "operation catalog",
                    ))
                })?;
        if &expected != context.operation {
            return Err(ControlFailureV1::new(
                ControlFailureStageV1::InvalidBinding("selected operation"),
            ));
        }
        if !context
            .request_context
            .allows(expected.capability_id(), expected.use_case_id())
            || context.request_context.cancellation().token_id
                != context.cancellation_signal.context().token_id
        {
            return Err(ControlFailureV1::new(
                ControlFailureStageV1::InvalidBinding("admitted grant and cancellation identity"),
            ));
        }
        let now = tracedecay_contracts::now_micros();
        if context.request_context.grant().issued_at > now {
            return Err(ControlFailureV1::new(
                ControlFailureStageV1::InvalidBinding("grant issuance time"),
            ));
        }
        let expires_at = context
            .request_context
            .deadline()
            .expires_at
            .min(context.request_context.grant().expires_at);
        let remaining = u64::try_from(expires_at.0.saturating_sub(now.0)).unwrap_or(0);
        let operation_id = super::observation_journey::mint_observation_id(now.0)
            .map_err(|error| {
                ControlFailureV1::new(ControlFailureStageV1::Request(
                    RetainedSurfaceExecutionErrorV1::unavailable(error.to_string()),
                ))
            })?
            .as_str()
            .to_owned();
        let invocation = Self {
            context,
            request,
            control: OperationControl::new(
                expires_at.0,
                remaining / 1_000,
                tracedecay_memory_provider_registry::CancellationToken::new(),
            ),
            identity: ControlOperationIdentityV1 {
                operation_id,
                idempotency_key: provider_mutation_key(
                    context.request_context,
                    request.operation(),
                )?,
            },
            deadline: tokio::time::Instant::now() + std::time::Duration::from_micros(remaining),
            stop_cause: AtomicU8::new(0),
        };
        invocation.check()?;
        Ok(invocation)
    }

    fn stop(&self, cause: ControlStopCauseV1) {
        let _ =
            self.stop_cause
                .compare_exchange(0, cause as u8, Ordering::AcqRel, Ordering::Acquire);
        self.control.cancellation().cancel();
    }

    fn normalize_control_refusal<T>(&self, result: ControlResult<T>) -> ControlResult<T> {
        if self.stop_cause.load(Ordering::Acquire) != ControlStopCauseV1::Deadline as u8 {
            return result;
        }
        result.map_err(|mut failure| {
            // OperationControl checks its cancellation token first. A host
            // deadline uses that token to stop work, but remains a timeout.
            // Change only explicit control refusals, preserving all receipts,
            // actual provider replies, and possible post-contact effects.
            let code = match &mut failure.stage {
                ControlFailureStageV1::Control(code)
                | ControlFailureStageV1::Attribution(RecallControlAttributionErrorV1::Control(
                    code,
                ))
                | ControlFailureStageV1::Authority(ProviderHistoryErrorV1::Control(code)) => {
                    Some(code)
                }
                ControlFailureStageV1::Dispatch {
                    error:
                        JourneyControlDispatchErrorV1::NotDispatched(
                            JourneyControlNotDispatchedV1::Control(code),
                        ),
                    ..
                } => Some(code),
                _ => None,
            };
            if let Some(code) = code {
                if *code == TerminalCode::Cancelled {
                    *code = TerminalCode::DeadlineExceeded;
                }
            }
            failure
        })
    }

    pub(super) fn check(&self) -> ControlResult<()> {
        if self.context.cancellation_signal.is_cancelled()
            || self.context.request_context.cancellation().is_cancelled()
        {
            self.stop(ControlStopCauseV1::Caller);
        }
        self.normalize_control_refusal(
            self.control
                .snapshot()
                .map(|_| ())
                .map_err(|code| ControlFailureV1::new(ControlFailureStageV1::Control(code))),
        )
    }

    /// Forward cancellation/deadline but retain ownership of the stage until it
    /// settles. This never drops an accepted command write or a provider call.
    pub(super) async fn run_controlled<T>(
        &self,
        future: impl Future<Output = ControlResult<T>>,
    ) -> ControlResult<T> {
        self.check()?;
        tokio::pin!(future);
        let result = tokio::select! {
            biased;
            () = self.context.cancellation_signal.cancelled() => {
                self.stop(ControlStopCauseV1::Caller);
                future.await
            }
            () = tokio::time::sleep_until(self.deadline) => {
                self.stop(ControlStopCauseV1::Deadline);
                future.await
            }
            result = &mut future => result,
        };
        self.normalize_control_refusal(result)
    }
}

impl Drop for ControlInvocationV1<'_, '_> {
    fn drop(&mut self) {
        self.control.cancellation().cancel();
    }
}

fn provider_mutation_key(
    context: &tracedecay_contracts::RequestContext,
    operation: RetainedSurfaceOperation,
) -> ControlResult<Option<String>> {
    if !tracedecay_contracts::retained_surface_operation_is_effect(operation) {
        return Ok(None);
    }
    // The mutable public body is deliberately excluded. Reusing a caller's
    // identity with changed parameters must reach the producer's conflict check.
    let bytes = canonical_json_bytes(&(
        "tracedecay.provider-control.idempotency.v1",
        context.actor(),
        context.scope(),
        operation.as_str(),
        context.request_id(),
    ))
    .map_err(|_| {
        ControlFailureV1::new(ControlFailureStageV1::InvalidBinding(
            "idempotency encoding",
        ))
    })?;
    Ok(Some(tracedecay_domain::canonical_text::sha256_hex(&bytes)))
}

fn provider_operation(operation: RetainedSurfaceOperation) -> ControlResult<ProviderOperation> {
    Ok(match operation {
        RetainedSurfaceOperation::ProviderFeedback => ProviderOperation::Feedback,
        RetainedSurfaceOperation::ProviderCorrection => ProviderOperation::Correction,
        RetainedSurfaceOperation::ProviderDeleteBySource => ProviderOperation::DeleteBySource,
        RetainedSurfaceOperation::ProviderHealth => ProviderOperation::Health,
        RetainedSurfaceOperation::ProviderInspection => ProviderOperation::Inspection,
        RetainedSurfaceOperation::ProviderMaintenance => ProviderOperation::Maintenance,
        RetainedSurfaceOperation::ProviderSnapshotExport => ProviderOperation::SnapshotExport,
        RetainedSurfaceOperation::ProviderSnapshotRestore => ProviderOperation::SnapshotRestore,
        RetainedSurfaceOperation::ProviderReplay => ProviderOperation::Replay,
        _ => {
            return Err(ControlFailureV1::new(
                ControlFailureStageV1::InvalidBinding("provider operation"),
            ));
        }
    })
}

impl ProjectProviderControlPortV1 {
    pub(super) fn ledger(&self) -> ControlResult<Arc<RecallAdmissionLedgerV1>> {
        self.inputs
            .authority
            .as_ref()
            .and_then(|authority| authority.ledger())
            .ok_or_else(|| {
                ControlFailureV1::new(ControlFailureStageV1::MissingAuthority(
                    "retained recall ledger",
                ))
            })
    }

    pub(super) fn authority(&self) -> ControlResult<&Arc<ProviderControlAuthorityV1>> {
        self.inputs.authority.as_ref().ok_or_else(|| {
            ControlFailureV1::new(ControlFailureStageV1::MissingAuthority(
                "original source authority",
            ))
        })
    }

    fn validate_invocation(&self, invocation: &ControlInvocationV1<'_, '_>) -> ControlResult<()> {
        invocation.check()?;
        if invocation.context.request_context.scope() != &self.inputs.mounted_scope
            || self.inputs.mounted_scope.project_id != self.inputs.authoritative_project_id
        {
            return Err(ControlFailureV1::new(
                ControlFailureStageV1::InvalidBinding("mounted request scope"),
            ));
        }
        Ok(())
    }

    fn mount_for(
        &self,
        retained: RetainedRecallControlScopeV1,
    ) -> ControlResult<ResolvedControlStateV1> {
        let mut matches = self.inputs.journeys.iter().filter(|journey| {
            journey.matches_control_owner(&retained.provider_id, retained.registration_revision)
        });
        let journey = matches.next().cloned();
        if matches.next().is_some() {
            return Err(ControlFailureV1::new(
                ControlFailureStageV1::InvalidBinding("duplicate mounted provider owner"),
            ));
        }
        if let Some(journey) = &journey {
            journey
                .validate_history_mount(
                    &retained.provider_id,
                    retained.registration_revision,
                    &self.inputs.profile_id,
                    &self.inputs.mounted_scope,
                )
                .map_err(|_| {
                    ControlFailureV1::new(ControlFailureStageV1::InvalidBinding(
                        "provider journey mount",
                    ))
                })?;
        }
        Ok(ResolvedControlStateV1 { retained, journey })
    }

    pub(super) fn resolved_state_for_source(
        &self,
        source: &AuthorizedControlSourceV1,
    ) -> ControlResult<ResolvedControlStateV1> {
        self.mount_for(source.authorized.retained.scope.clone())
    }

    pub(super) async fn resolve_state(
        &self,
        selector: &ProviderControlStateSelectorV1,
        invocation: &ControlInvocationV1<'_, '_>,
    ) -> ControlResult<ResolvedControlStateV1> {
        self.mount_for(self.resolve_retained_state(selector, invocation).await?)
    }

    /// Resolve actual scope authority independently of a live provider mount.
    pub(super) async fn resolve_retained_state(
        &self,
        selector: &ProviderControlStateSelectorV1,
        invocation: &ControlInvocationV1<'_, '_>,
    ) -> ControlResult<RetainedRecallControlScopeV1> {
        self.validate_invocation(invocation)?;
        let authority = self.authority()?;
        let retained = match selector {
            ProviderControlStateSelectorV1::RecallScope { trace_ref } => {
                let trace = RecallControlTraceRefV1::parse(trace_ref)?;
                let ledger = self.ledger()?;
                let control = invocation.control.clone();
                let retained = invocation
                    .run_controlled(async move {
                        tokio::task::spawn_blocking(move || {
                            ledger.read_retained_control_scope(&trace, &control)
                        })
                        .await
                        .map_err(|_| {
                            ControlFailureV1::new(ControlFailureStageV1::BlockingRead(
                                "retained control scope",
                            ))
                        })?
                        .map_err(ControlFailureV1::from)
                    })
                    .await?;
                invocation
                    .run_controlled(async {
                        authority
                            .authorize_scope(&retained, &invocation.control)
                            .await
                            .map_err(ControlFailureV1::from)
                    })
                    .await?
            }
            ProviderControlStateSelectorV1::CanonicalSession {
                provider_id,
                registration_revision,
                canonical_provider_id,
                session_id,
            } => {
                // The canonical session is an untrusted lookup key. The mounted
                // authority must prove its session and hook-origin boundary.
                let provider_id = OwnedProviderId::new(provider_id.clone()).map_err(|_| {
                    ControlFailureV1::new(ControlFailureStageV1::InvalidBinding(
                        "provider identity",
                    ))
                })?;
                invocation
                    .run_controlled(async {
                        authority
                            .authorize_canonical_session(
                                &provider_id,
                                *registration_revision,
                                canonical_provider_id,
                                session_id,
                                &invocation.control,
                            )
                            .await
                            .map_err(ControlFailureV1::from)
                    })
                    .await?
            }
        };
        invocation.check()?;
        Ok(retained)
    }

    pub(super) async fn resolve_source(
        &self,
        selector: &ProviderControlSourceSelectorV1,
        include_unavailable: bool,
        control: &OperationControl,
    ) -> ControlResult<AuthorizedControlSourceV1> {
        let authority = self.authority()?;
        let trace = RecallControlTraceRefV1::parse(&selector.trace_ref)?;
        let item = RecallControlItemRefV1::parse(&selector.item_ref)?;
        let ledger = self.ledger()?;
        let observation_id = selector.observation_id.clone();
        let read_control = control.clone();
        let retained = tokio::task::spawn_blocking(move || {
            ledger.read_retained_control_source(&trace, &item, &observation_id, &read_control)
        })
        .await
        .map_err(|_| {
            ControlFailureV1::new(ControlFailureStageV1::BlockingRead(
                "retained control source",
            ))
        })??;
        let authorized = authority
            .authorize_source(&retained, include_unavailable, control)
            .await?;
        let original = authorized
            .retained
            .original_source
            .to_owned_attribution()
            .map_err(|_| {
                ControlFailureV1::new(ControlFailureStageV1::InvalidBinding(
                    "retained source attribution",
                ))
            })?;
        let [granted] = authorized.grant.sources.as_slice() else {
            return Err(ControlFailureV1::new(
                ControlFailureStageV1::InvalidBinding("one authorized original source"),
            ));
        };
        if granted.attribution != original
            || authorized.grant.destination_scope != authorized.retained.scope.delivery_scope
        {
            return Err(ControlFailureV1::new(
                ControlFailureStageV1::InvalidBinding("fresh retained source binding"),
            ));
        }
        let target = LifecycleTarget {
            provider_id: authorized.retained.scope.provider_id.clone(),
            registration_revision: authorized.retained.scope.registration_revision,
            original_scope: original.origin_scope,
            delivery_scope: authorized.retained.scope.delivery_scope.clone(),
            source: original.source,
            reference: LifecycleTargetReference::StableMemoryRef(
                authorized.retained.stable_memory_ref.clone(),
            ),
        };
        target.validate().map_err(|_| {
            ControlFailureV1::new(ControlFailureStageV1::InvalidBinding(
                "provider lifecycle target",
            ))
        })?;
        Ok(AuthorizedControlSourceV1 { authorized, target })
    }

    pub(super) fn require_same_state(
        &self,
        expected: &ResolvedControlStateV1,
        source: &AuthorizedControlSourceV1,
    ) -> ControlResult<()> {
        require_same_producer_scope(&expected.retained, &source.authorized.retained.scope)
    }

    pub(super) fn unavailable_for(
        &self,
        invocation: &ControlInvocationV1<'_, '_>,
        state: &ResolvedControlStateV1,
        reason: &'static str,
    ) -> ControlFailureV1 {
        ControlFailureV1::new(ControlFailureStageV1::Offline {
            state: state.retained.clone(),
            identity: invocation.identity.clone(),
            reason,
        })
    }

    pub(super) async fn dispatch(
        &self,
        invocation: &ControlInvocationV1<'_, '_>,
        state: &ResolvedControlStateV1,
        operation_body: Value,
        identity: &ControlOperationIdentityV1,
        expected_state_generation: Option<u64>,
    ) -> ControlResult<JourneyControlDispatchReplyV1> {
        self.dispatch_with_source_grant(
            invocation,
            state,
            operation_body,
            identity,
            expected_state_generation,
            None,
        )
        .await
    }

    /// Carry source-grant claims through the private dispatch seam for
    /// downstream revalidation without adding them to the public operation body.
    pub(super) async fn dispatch_with_source_grant(
        &self,
        invocation: &ControlInvocationV1<'_, '_>,
        state: &ResolvedControlStateV1,
        operation_body: Value,
        identity: &ControlOperationIdentityV1,
        expected_state_generation: Option<u64>,
        history_grant: Option<HistoryGrant>,
    ) -> ControlResult<JourneyControlDispatchReplyV1> {
        self.validate_invocation(invocation)?;
        let Some(journey) = state.journey.as_ref().cloned() else {
            return Err(ControlFailureV1::new(ControlFailureStageV1::Offline {
                state: state.retained.clone(),
                identity: identity.clone(),
                reason: "original provider is not mounted",
            }));
        };
        let request = JourneyControlDispatchRequestV1 {
            provider_id: state.retained.provider_id.clone(),
            registration_revision: state.retained.registration_revision,
            exact_scope: state.retained.delivery_scope.clone(),
            operation: provider_operation(invocation.request.operation())?,
            request_id: invocation
                .context
                .request_context
                .request_id()
                .as_str()
                .to_owned(),
            operation_id: identity.operation_id.clone(),
            idempotency_key: identity.idempotency_key.clone(),
            operation_body,
            history_grant,
            policy_revision: PROVIDER_CONTROL_POLICY_REVISION_V1,
            control: invocation.control.clone(),
            expected_state_generation,
        };
        // The journey alone checks readiness and builds the canonical header.
        // Its fixed isolation ceiling bounds provider waits; this owner joins
        // the blocking caller and retains post-contact uncertainty intact.
        invocation
            .run_controlled(async {
                match tokio::task::spawn_blocking(move || journey.dispatch_control(request)).await {
                    Ok(Ok(reply)) => Ok(reply),
                    Ok(Err(error)) => Err(ControlFailureV1::new(ControlFailureStageV1::Dispatch {
                        state: state.retained.clone(),
                        identity: identity.clone(),
                        error,
                    })),
                    Err(_) => Err(ControlFailureV1::new(
                        ControlFailureStageV1::DispatchWorker {
                            state: state.retained.clone(),
                            identity: identity.clone(),
                        },
                    )),
                }
            })
            .await
    }

    pub(super) fn project(
        &self,
        invocation: &ControlInvocationV1<'_, '_>,
        dispatched: JourneyControlDispatchReplyV1,
        evidence: projection::HostControlEvidence<'_>,
    ) -> ControlResult<CompletedProviderControlV1> {
        match projection::project_provider_control_reply(
            projection::ProviderControlProjectionInput {
                context: invocation.context.request_context,
                request: invocation.request,
                dispatched: &dispatched,
            },
            evidence,
        ) {
            Ok(result) => Ok(CompletedProviderControlV1 {
                result,
                host_receipt: None,
            }),
            Err(error) => {
                let field = error.field;
                Err(ControlFailureV1::new(ControlFailureStageV1::Projection {
                    field,
                    dispatched,
                }))
            }
        }
    }
}

fn require_same_producer_scope(
    expected: &RetainedRecallControlScopeV1,
    actual: &RetainedRecallControlScopeV1,
) -> ControlResult<()> {
    if expected != actual {
        return Err(ControlFailureV1::new(
            ControlFailureStageV1::InvalidBinding("source inspection state"),
        ));
    }
    Ok(())
}

impl RetainedProviderControlExecutionPortV1 for ProjectProviderControlPortV1 {
    fn execute_provider_control<'a>(
        &'a self,
        context: RetainedSurfaceExecutionContextV1<'a>,
        request: &'a ProviderControlRequestV1,
    ) -> RetainedSurfaceExecutionFutureV1<'a> {
        Box::pin(async move {
            let result = async {
                let invocation = ControlInvocationV1::new(&context, request)?;
                self.validate_invocation(&invocation)?;
                invocation
                    .run_controlled(async {
                        match request {
                            ProviderControlRequestV1::Feedback(request) => {
                                source_controls::feedback(self, &invocation, request).await
                            }
                            ProviderControlRequestV1::Correction(request) => {
                                source_controls::correction(self, &invocation, request).await
                            }
                            ProviderControlRequestV1::DeleteBySource(request) => {
                                source_controls::delete_by_source(self, &invocation, request).await
                            }
                            ProviderControlRequestV1::Health(request) => {
                                state_controls::health(self, &invocation, request).await
                            }
                            ProviderControlRequestV1::Inspection(request) => {
                                state_controls::inspection(self, &invocation, request).await
                            }
                            ProviderControlRequestV1::Maintenance(request) => {
                                state_controls::maintenance(self, &invocation, request).await
                            }
                            ProviderControlRequestV1::SnapshotExport(request) => {
                                portability::snapshot_export(self, &invocation, request).await
                            }
                            ProviderControlRequestV1::SnapshotRestore(request) => {
                                portability::snapshot_restore(self, &invocation, request).await
                            }
                            ProviderControlRequestV1::Replay(request) => {
                                portability::replay(self, &invocation, request).await
                            }
                        }
                    })
                    .await
            }
            .await;
            outcome::assemble_outcome(&context, request, result, &self.inputs.configuration_digest)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use tracedecay_contracts::retained_surfaces::{
        ProviderControlHealthCheckV1, ProviderHealthRequestV1,
    };
    use tracedecay_contracts::{
        CancellationContext, CancellationSignal, CapabilityGrantId, CapabilityGrantSnapshot,
        Deadline, DisclosureClass, RequestContext, RequestId,
    };
    use tracedecay_domain::{ActorId, RefId, RepositoryId, UtcMicros, WorktreeId};

    fn context(
        actor: &str,
        request_id: &str,
        worktree: &str,
    ) -> (RequestContext, CancellationSignal) {
        let now = tracedecay_contracts::now_micros();
        let expiry = UtcMicros(now.0 + 60_000_000);
        let scope = ResolvedScope::new(
            ProjectId::new("project.control").unwrap(),
            RepositoryId::new("repository.control").unwrap(),
            WorktreeId::new(worktree).unwrap(),
            Some(RefId::new("refs/heads/master").unwrap()),
        )
        .unwrap();
        let operation = tracedecay_contracts::retained_surface_application_operation(
            RetainedSurfaceOperation::ProviderHealth,
        )
        .unwrap();
        let grant = CapabilityGrantSnapshot::new(
            CapabilityGrantId::new("grant.control").unwrap(),
            7,
            ManifestDigest::new(format!("sha256:{}", "a".repeat(64))).unwrap(),
            ActorId::new("actor.issuer").unwrap(),
            UtcMicros(now.0 - 1),
            expiry,
            scope.clone(),
            BTreeSet::from([operation.capability_id().clone()]),
            BTreeSet::from([operation.use_case_id().clone()]),
            DisclosureClass::Evidence,
        )
        .unwrap();
        (
            RequestContext::new(
                ActorId::new(actor).unwrap(),
                scope,
                grant,
                RequestId::new(request_id).unwrap(),
                Deadline::new(expiry).unwrap(),
                CancellationContext::active("cancel.control").unwrap(),
            )
            .unwrap(),
            CancellationSignal::active("cancel.control").unwrap(),
        )
    }

    fn health_request() -> ProviderControlRequestV1 {
        ProviderControlRequestV1::Health(ProviderHealthRequestV1 {
            state: ProviderControlStateSelectorV1::CanonicalSession {
                provider_id: "native".into(),
                registration_revision: 1,
                canonical_provider_id: "claude".into(),
                session_id: "session.original".into(),
            },
            requested_checks: vec![ProviderControlHealthCheckV1::State],
        })
    }

    #[test]
    fn mutation_identity_binds_actor_full_scope_operation_and_caller_identity() {
        let (original, _) = context("actor.one", "request.one", "worktree.one");
        let key = provider_mutation_key(&original, RetainedSurfaceOperation::ProviderMaintenance)
            .unwrap()
            .unwrap();
        assert_eq!(key.len(), 64);
        assert!(
            key.bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        );
        assert_eq!(
            provider_mutation_key(&original, RetainedSurfaceOperation::ProviderMaintenance)
                .unwrap()
                .as_deref(),
            Some(key.as_str())
        );
        for (actor, request, worktree) in [
            ("actor.two", "request.one", "worktree.one"),
            ("actor.one", "request.two", "worktree.one"),
            ("actor.one", "request.one", "worktree.two"),
        ] {
            let (other, _) = context(actor, request, worktree);
            assert_ne!(
                provider_mutation_key(&other, RetainedSurfaceOperation::ProviderMaintenance)
                    .unwrap()
                    .as_deref(),
                Some(key.as_str())
            );
        }
        assert_ne!(
            provider_mutation_key(&original, RetainedSurfaceOperation::ProviderReplay)
                .unwrap()
                .as_deref(),
            Some(key.as_str())
        );
        assert!(
            provider_mutation_key(&original, RetainedSurfaceOperation::ProviderHealth)
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn cancelled_stage_is_joined_and_keeps_its_actual_result() {
        let (request_context, signal) = context("actor.one", "request.one", "worktree.one");
        let request = health_request();
        let operation =
            tracedecay_contracts::retained_surface_application_operation(request.operation())
                .unwrap();
        let context = RetainedSurfaceExecutionContextV1 {
            request_context: &request_context,
            cancellation_signal: &signal,
            operation: &operation,
            observed_at: tracedecay_contracts::now_micros(),
        };
        let invocation = ControlInvocationV1::new(&context, &request).unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (settle_tx, settle_rx) = tokio::sync::oneshot::channel();
        let run = invocation.run_controlled(async {
            entered_tx.send(()).unwrap();
            settle_rx.await.unwrap();
            assert!(invocation.control.cancellation().is_cancelled());
            Ok("actual settled result")
        });
        let cancel = async {
            entered_rx.await.unwrap();
            signal.cancel(tracedecay_contracts::now_micros());
            tokio::task::yield_now().await;
            settle_tx.send(()).unwrap();
        };
        let (result, ()) = tokio::join!(run, cancel);
        assert_eq!(result.unwrap(), "actual settled result");
        // A nested stage may see the deadline after the caller stopped it.
        // The original caller cancellation still owns the refusal cause.
        invocation.stop(ControlStopCauseV1::Deadline);
        let refusal: ControlResult<()> = invocation.normalize_control_refusal(Err(
            ControlFailureV1::new(ControlFailureStageV1::Authority(
                ProviderHistoryErrorV1::Control(TerminalCode::Cancelled),
            )),
        ));
        assert!(matches!(
            refusal,
            Err(ControlFailureV1 {
                stage: ControlFailureStageV1::Authority(ProviderHistoryErrorV1::Control(
                    TerminalCode::Cancelled
                )),
                ..
            })
        ));
    }

    #[tokio::test]
    async fn deadline_joins_nested_stage_and_only_normalizes_control_refusal() {
        for refuse in [true, false] {
            let (request_context, signal) = context("actor.one", "request.one", "worktree.one");
            let request_context = request_context.with_deadline(
                Deadline::new(UtcMicros(tracedecay_contracts::now_micros().0 + 100_000)).unwrap(),
            );
            let request = health_request();
            let operation =
                tracedecay_contracts::retained_surface_application_operation(request.operation())
                    .unwrap();
            let context = RetainedSurfaceExecutionContextV1 {
                request_context: &request_context,
                cancellation_signal: &signal,
                operation: &operation,
                observed_at: tracedecay_contracts::now_micros(),
            };
            let invocation = ControlInvocationV1::new(&context, &request).unwrap();
            let settled = std::sync::atomic::AtomicBool::new(false);
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let (settle_tx, settle_rx) = tokio::sync::oneshot::channel();
            let run = invocation.run_controlled(invocation.run_controlled(async {
                entered_tx.send(()).unwrap();
                settle_rx.await.unwrap();
                assert_eq!(
                    invocation.control.snapshot().unwrap_err(),
                    TerminalCode::Cancelled
                );
                settled.store(true, Ordering::Release);
                if refuse {
                    Err(ControlFailureV1::new(ControlFailureStageV1::Authority(
                        ProviderHistoryErrorV1::Control(TerminalCode::Cancelled),
                    )))
                } else {
                    Ok("actual settled result")
                }
            }));
            let release = async {
                entered_rx.await.unwrap();
                tokio::time::sleep_until(invocation.deadline).await;
                tokio::task::yield_now().await;
                assert!(invocation.control.cancellation().is_cancelled());
                assert!(!signal.is_cancelled());
                assert!(
                    !settled.load(Ordering::Acquire),
                    "deadline must keep joining the stage"
                );
                settle_tx.send(()).unwrap();
            };
            let (result, ()) = tokio::join!(run, release);
            assert!(settled.load(Ordering::Acquire));
            if refuse {
                assert!(matches!(
                    result,
                    Err(ControlFailureV1 {
                        stage: ControlFailureStageV1::Authority(ProviderHistoryErrorV1::Control(
                            TerminalCode::DeadlineExceeded
                        )),
                        ..
                    })
                ));
            } else {
                assert_eq!(result.unwrap(), "actual settled result");
            }
            assert!(matches!(
                invocation.check(),
                Err(ControlFailureV1 {
                    stage: ControlFailureStageV1::Control(TerminalCode::DeadlineExceeded),
                    ..
                })
            ));
        }
    }

    #[tokio::test]
    async fn mismatched_cancellation_identity_is_rejected_before_dispatch() {
        let (request_context, _) = context("actor.one", "request.one", "worktree.one");
        let signal = CancellationSignal::active("cancel.foreign").unwrap();
        let request = health_request();
        let operation =
            tracedecay_contracts::retained_surface_application_operation(request.operation())
                .unwrap();
        let context = RetainedSurfaceExecutionContextV1 {
            request_context: &request_context,
            cancellation_signal: &signal,
            operation: &operation,
            observed_at: tracedecay_contracts::now_micros(),
        };
        assert!(matches!(
            ControlInvocationV1::new(&context, &request),
            Err(ControlFailureV1 {
                stage: ControlFailureStageV1::InvalidBinding(
                    "admitted grant and cancellation identity"
                ),
                ..
            })
        ));
    }
}
