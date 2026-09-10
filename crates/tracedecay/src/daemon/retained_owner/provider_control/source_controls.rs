//! Source controls over retained attribution and current canonical authority.
//!
//! Accepted feedback and deletion commands fix retry identity before provider
//! contact. A deletion fence belongs to the original journal and is independent
//! of whether its producing provider can currently execute erasure.

use serde_json::{Value, json};
use tracedecay_contracts::RetainedSurfaceExecutionErrorV1;
use tracedecay_contracts::retained_surfaces::*;
use tracedecay_domain::UtcMicros;
use tracedecay_memory_observation::{
    ObservationJournalError, ProviderSourceDeletionIntentV1, ProviderSourceIntentReceiptV1,
};
use tracedecay_memory_provider_registry::{
    CanonicalPayload, DeletionMode, HistoryGrant, OwnedExactScope, SourceAttribution,
    rfc3339_utc_micros,
};

use super::super::cognitive_recall::control_attribution::RetainedRecallControlScopeV1;
use super::super::provider_history::{
    original_source_fence_digest, resolved_replay_observation, retained_history_source,
};
use super::authority::ProviderSourceIntentActionErrorV1;
use super::feedback_receipt::{
    AcceptedDeletionCommandV1, AcceptedFeedbackAssertionV1, HostSourceCommandErrorV1,
};
use super::portability::{
    HostSnapshotCleanupResultV1, HostSnapshotCleanupStateV1, cleanup_source_snapshots,
};
use super::projection::{CorrectionEvidenceV1, HostControlEvidence};
use super::{
    AuthorizedControlSourceV1, CompletedProviderControlV1, ControlFailureStageV1, ControlFailureV1,
    ControlInvocationV1, ControlOperationIdentityV1, ControlResult, ProjectProviderControlPortV1,
    ResolvedControlStateV1, outcome, projection,
};

/// Built-in provider-local retention-lock policy. Version one adds no host lock
/// claims to a deletion request. A provider's explicit lock report remains its
/// own typed outcome evidence; canonical facts and transcripts are unaffected.
const PROVIDER_LOCAL_RETENTION_LOCK_POLICY_REVISION_V1: u64 = 1;

pub(super) async fn feedback(
    port: &ProjectProviderControlPortV1,
    invocation: &ControlInvocationV1<'_, '_>,
    request: &ProviderFeedbackRequestV1,
) -> ControlResult<CompletedProviderControlV1> {
    let source = port
        .resolve_source(&request.source, false, &invocation.control)
        .await?;
    let accepted = accept_feedback(port, invocation, request, &source).await?;
    let identity = feedback_identity(&accepted);
    let retained = source.authorized.retained.scope.clone();
    let source = match refresh_source(port, invocation, &request.source, &source, false).await {
        Ok(source) => source,
        Err(failure) => return Ok(accepted_failure(invocation, &retained, &identity, failure)),
    };
    let prepared = (|| -> ControlResult<_> {
        if !accepted
            .matches(
                invocation.context.request_context,
                request,
                &source.target,
                &source.granted_source()?.attribution,
            )
            .map_err(command_error)?
        {
            return Err(binding("accepted feedback source changed"));
        }
        let evidence = source.projection_evidence(&request.source)?;
        let body = feedback_body(request, evidence, &accepted)?;
        Ok((evidence, body))
    })();
    let (evidence, body) = match prepared {
        Ok(prepared) => prepared,
        Err(failure) => return Ok(accepted_failure(invocation, &retained, &identity, failure)),
    };
    let state = match port.resolved_state_for_source(&source) {
        Ok(state) => state,
        Err(failure) => return Ok(accepted_failure(invocation, &retained, &identity, failure)),
    };
    let dispatched = match port
        .dispatch_with_source_grant(
            invocation,
            &state,
            body,
            &identity,
            None,
            Some(source.authorized.grant.clone()),
        )
        .await
    {
        Ok(dispatched) => dispatched,
        Err(failure) => return Ok(accepted_failure(invocation, &retained, &identity, failure)),
    };
    port.project(
        invocation,
        dispatched,
        HostControlEvidence::Feedback {
            source: evidence,
            assertion: &accepted,
        },
    )
}

fn feedback_body(
    request: &ProviderFeedbackRequestV1,
    source: projection::ResolvedControlSourceV1<'_>,
    accepted: &AcceptedFeedbackAssertionV1,
) -> ControlResult<Value> {
    Ok(json!({
        "target": projection::lifecycle_target_wire(source).map_err(binding)?,
        "signal": request.signal,
        "weight": request.weight,
        "canonical_outcome_receipt": accepted.canonical_outcome_receipt(),
        // These remain the caller's bounded claims; they confer no authority.
        "evidence_refs": request.evidence_refs,
        "occurred_at": timestamp(request.occurred_at)?,
    }))
}

pub(super) async fn correction(
    port: &ProjectProviderControlPortV1,
    invocation: &ControlInvocationV1<'_, '_>,
    request: &ProviderCorrectionRequestV1,
) -> ControlResult<CompletedProviderControlV1> {
    let source = port
        .resolve_source(&request.source, false, &invocation.control)
        .await?;
    source
        .target
        .validate_expected_revision(&request.expected_source_revision)
        .map_err(|_| ControlFailureV1::from(RetainedSurfaceExecutionErrorV1::Conflict))?;
    let state = port.resolved_state_for_source(&source)?;
    let change = prepare_correction(port, invocation, request, &state, &source).await?;
    let source = refresh_source(port, invocation, &request.source, &source, false).await?;
    source
        .target
        .validate_expected_revision(&request.expected_source_revision)
        .map_err(|_| ControlFailureV1::from(RetainedSurfaceExecutionErrorV1::Conflict))?;
    let source_evidence = source.projection_evidence(&request.source)?;
    let body = json!({
        "target": projection::lifecycle_target_wire(source_evidence).map_err(binding)?,
        "correction_kind": request.correction.kind(),
        "replacement": change.body()?,
        "expected_target_revision": request.expected_source_revision,
        "reason": request.reason,
        "evidence_refs": request.evidence_refs,
    });
    let change_evidence = change.evidence()?;
    let history_grant = change
        .history_grant(port, invocation, &state, &source)
        .await?;
    let dispatched = port
        .dispatch_with_source_grant(
            invocation,
            &state,
            body,
            &invocation.identity,
            None,
            Some(history_grant),
        )
        .await?;
    port.project(
        invocation,
        dispatched,
        HostControlEvidence::Correction {
            source: source_evidence,
            change: change_evidence,
        },
    )
}

enum PreparedCorrectionV1<'request> {
    Metadata(Value),
    Replacement {
        selector: &'request ProviderControlSourceSelectorV1,
        source: AuthorizedControlSourceV1,
        observation: CanonicalPayload,
    },
    RestrictScope {
        selector: &'request ProviderControlStateSelectorV1,
        scope: OwnedExactScope,
    },
}

impl PreparedCorrectionV1<'_> {
    async fn history_grant(
        &self,
        port: &ProjectProviderControlPortV1,
        invocation: &ControlInvocationV1<'_, '_>,
        state: &ResolvedControlStateV1,
        target: &AuthorizedControlSourceV1,
    ) -> ControlResult<HistoryGrant> {
        let Self::Replacement {
            source: replacement,
            ..
        } = self
        else {
            return Ok(target.authorized.grant.clone());
        };
        port.require_same_state(state, target)?;
        port.require_same_state(state, replacement)?;
        let mut originals = vec![target.authorized.retained.original_source.clone()];
        if replacement.authorized.retained.original_source != originals[0] {
            originals.push(replacement.authorized.retained.original_source.clone());
        }
        // Reuse the existing canonical inventory authority to derive one actual
        // relation and checkpoint for both originals. The installed provider
        // authority still treats this private per-call value as an untrusted
        // claim and revalidates every full source immediately before use.
        let inventory = port
            .authority()?
            .authorize_retained_source_inventory(&state.retained, &originals, &invocation.control)
            .await?;
        let grant = inventory
            .grant
            .ok_or_else(|| binding("replacement history inventory"))?;
        if grant
            .sources
            .iter()
            .any(|source| !retained_history_source(source.current_disposition.state))
        {
            return Err(
                super::super::provider_history::ProviderHistoryErrorV1::Ineligible(
                    "current correction source disposition",
                )
                .into(),
            );
        }
        Ok(grant)
    }

    fn body(&self) -> ControlResult<Value> {
        match self {
            Self::Metadata(value) => Ok(value.clone()),
            Self::Replacement { observation, .. } => serde_json::from_slice(&observation.bytes)
                .map_err(|_| binding("canonical replacement payload")),
            Self::RestrictScope { scope, .. } => {
                Ok(json!({"exact_scope_identity":projection::scope(scope)}))
            }
        }
    }

    fn evidence(&self) -> ControlResult<CorrectionEvidenceV1<'_>> {
        match self {
            Self::Metadata(_) => Ok(CorrectionEvidenceV1::Metadata),
            Self::Replacement {
                selector,
                source,
                observation,
            } => Ok(CorrectionEvidenceV1::Replacement {
                source: source.projection_evidence(selector)?,
                observation,
            }),
            Self::RestrictScope { selector, scope } => Ok(CorrectionEvidenceV1::RestrictScope {
                selector,
                exact_scope: scope,
            }),
        }
    }
}

async fn prepare_correction<'request>(
    port: &ProjectProviderControlPortV1,
    invocation: &ControlInvocationV1<'_, '_>,
    request: &'request ProviderCorrectionRequestV1,
    target_state: &ResolvedControlStateV1,
    target: &AuthorizedControlSourceV1,
) -> ControlResult<PreparedCorrectionV1<'request>> {
    let original = &target.granted_source()?.attribution;
    match &request.correction {
        ProviderControlCorrectionV1::Supersede { replacement_source }
        | ProviderControlCorrectionV1::ReplaceContent { replacement_source } => {
            let mut source = port
                .resolve_source(replacement_source, false, &invocation.control)
                .await?;
            // A replacement is an already admitted envelope in the same exact
            // producing namespace. This path never retargets or enqueues it.
            port.require_same_state(target_state, &source)?;
            validate_replacement_lineage(original, &source.granted_source()?.attribution)?;
            let resolved = port
                .authority()?
                .prepare_replacement_observation(&source.authorized, &invocation.control)
                .await?;
            source.authorized = resolved.source;
            port.require_same_state(target_state, &source)?;
            validate_replacement_lineage(original, &source.granted_source()?.attribution)?;
            let replay = resolved_replay_observation(&resolved.admitted)?;
            let envelope = replay
                .get("observation")
                .ok_or_else(|| binding("resolved canonical replacement envelope"))?;
            let bytes = tracedecay_domain::canonical_json_bytes(envelope)
                .map_err(|_| binding("canonical replacement encoding"))?;
            let digest = tracedecay_domain::canonical_sha256(envelope)
                .map_err(|_| binding("canonical replacement digest"))?;
            let observation = CanonicalPayload::new(
                resolved.admitted.payload.contract_id.clone(),
                bytes,
                digest.as_str().trim_start_matches("sha256:"),
            )
            .map_err(|_| binding("canonical replacement envelope"))?;
            Ok(PreparedCorrectionV1::Replacement {
                selector: replacement_source,
                source,
                observation,
            })
        }
        ProviderControlCorrectionV1::ChangeValidity {
            valid_from,
            valid_until,
        } => {
            let mut validity = original.validity.clone();
            validity.valid_from_utc_nanos = Some(nanos(*valid_from)?);
            validity.valid_until_utc_nanos = valid_until.map(nanos).transpose()?;
            validity.validate().map_err(|_| invalid_request())?;
            Ok(PreparedCorrectionV1::Metadata(json!({
                "valid_from": timestamp(*valid_from)?,
                "valid_until": valid_until.map(timestamp).transpose()?,
            })))
        }
        ProviderControlCorrectionV1::MarkIncorrect { revoked_at } => {
            let mut validity = original.validity.clone();
            validity.revoked_at_utc_nanos = Some(nanos(*revoked_at)?);
            validity.validate().map_err(|_| invalid_request())?;
            Ok(PreparedCorrectionV1::Metadata(
                json!({"revoked_at":timestamp(*revoked_at)?}),
            ))
        }
        ProviderControlCorrectionV1::RestrictScope { destination } => {
            let destination_state = port.resolve_state(destination, invocation).await?;
            // Full exact scope is already the smallest host namespace. Moving
            // to another session or checkout is not a restriction. The same
            // namespace can reach the provider's truthful no-change/refusal path.
            if destination_state.retained != target_state.retained {
                return Err(binding(
                    "scope restriction would move the original namespace",
                ));
            }
            Ok(PreparedCorrectionV1::RestrictScope {
                selector: destination,
                scope: destination_state.retained.delivery_scope,
            })
        }
    }
}

fn validate_replacement_lineage(
    original: &SourceAttribution,
    replacement: &SourceAttribution,
) -> ControlResult<()> {
    original
        .validate()
        .map_err(|_| binding("original correction attribution"))?;
    replacement
        .validate()
        .map_err(|_| binding("replacement correction attribution"))?;
    let old = &original.source;
    let new = &replacement.source;
    if old.canonical_provider_id != new.canonical_provider_id
        || old.canonical_session_id != new.canonical_session_id
        || old.source_key != new.source_key
        || old.stable_record_id != new.stable_record_id
        || original.origin_scope != replacement.origin_scope
        || new.source_revision.as_deref().is_none_or(str::is_empty)
        || new.source_revision == old.source_revision
    {
        return Err(binding(
            "replacement changes canonical source lineage or has no new revision",
        ));
    }
    let Some(at) = replacement.validity.valid_from_utc_nanos else {
        return Err(invalid_request());
    };
    if original.validity.superseded_at_utc_nanos.is_some()
        || original.validity.revoked_at_utc_nanos.is_some()
        || original
            .validity
            .valid_from_utc_nanos
            .is_some_and(|from| from >= at)
        || original
            .validity
            .valid_until_utc_nanos
            .is_some_and(|until| until < at)
    {
        return Err(RetainedSurfaceExecutionErrorV1::Conflict.into());
    }
    Ok(())
}

pub(super) async fn delete_by_source(
    port: &ProjectProviderControlPortV1,
    invocation: &ControlInvocationV1<'_, '_>,
    request: &ProviderDeleteBySourceRequestV1,
) -> ControlResult<CompletedProviderControlV1> {
    // Existing unavailable canonical sources may still require provider cleanup.
    let source = port
        .resolve_source(&request.source, true, &invocation.control)
        .await?;
    let accepted = accept_deletion(port, invocation, request, &source).await?;
    let identity = deletion_identity(&accepted);
    let retained = source.authorized.retained.scope.clone();
    let source = match refresh_source(port, invocation, &request.source, &source, true).await {
        Ok(source) => source,
        Err(failure) => return Ok(accepted_failure(invocation, &retained, &identity, failure)),
    };
    // Resolve all public source bindings before the durable fence transition.
    let prepared = (|| -> ControlResult<_> {
        let evidence = source.projection_evidence(&request.source)?;
        let target = projection::public_target(evidence).map_err(binding)?;
        if !accepted
            .matches(
                invocation.context.request_context,
                request,
                &source.target,
                evidence.original_attribution,
            )
            .map_err(command_error)?
        {
            return Err(binding("accepted deletion source changed"));
        }
        let source_digest = original_source_fence_digest(evidence.original_attribution)?;
        Ok((target, source_digest))
    })();
    let (public_target, source_digest) = match prepared {
        Ok(prepared) => prepared,
        Err(failure) => return Ok(accepted_failure(invocation, &retained, &identity, failure)),
    };
    let original = &source.granted_source()?.attribution;
    let intent_request = ProviderSourceDeletionIntentV1 {
        operation_id: accepted.operation_id(),
        provider_id: source.target.provider_id.as_str(),
        original_source_sha256: &source_digest,
        expected_fence_revision: request.expected_fence_revision,
        mode: deletion_mode(request.mode),
        source_revision: original.source.source_revision.as_deref(),
        authority_ref: accepted.operation_id(),
        accepted_at_utc_micros: accepted.accepted_at().0,
    };
    let recorded = match record_intent(&source, &intent_request, invocation, &identity).await {
        Ok(recorded) => recorded,
        Err(failure) => return Ok(accepted_failure(invocation, &retained, &identity, failure)),
    };
    let host_receipt = match outcome::deletion_intent_receipt(
        invocation,
        &source,
        &accepted,
        &recorded,
        &port.inputs.configuration_digest,
    ) {
        Ok(receipt) => receipt,
        Err(_) => {
            return Ok(accepted_failure(
                invocation,
                &retained,
                &identity,
                ControlFailureV1::new(ControlFailureStageV1::HostIntentUnknown {
                    state: retained.clone(),
                    identity: identity.clone(),
                    detail: "journal intent returned but its application effect receipt binding could not be confirmed",
                }),
            ));
        }
    };
    let intent = ProviderControlDeletionIntentV1 {
        fence_revision_before: request.expected_fence_revision,
        fence_revision_after: recorded.fence.revision,
        host_receipt,
    };
    // The original journal intent has settled before provider lookup. Fresh
    // canonical authorization includes its now-unavailable disposition.
    let source = match refresh_source(port, invocation, &request.source, &source, true).await {
        Ok(source) => source,
        Err(failure) => {
            return deletion_failure(
                invocation,
                request,
                &retained,
                &public_target,
                &identity,
                &intent,
                &unconfirmed_cleanup(request.include_snapshots),
                failure,
            );
        }
    };
    // Host artifact removal precedes provider lookup, including an offline lane.
    // Its actual partial result is never used as proof of provider erasure.
    let (source, cleanup) = match cleanup_snapshots(
        port,
        invocation,
        source,
        recorded,
        request.include_snapshots,
    )
    .await
    {
        Ok(result) => result,
        Err(failure) => {
            return deletion_failure(
                invocation,
                request,
                &retained,
                &public_target,
                &identity,
                &intent,
                &unconfirmed_cleanup(request.include_snapshots),
                failure,
            );
        }
    };
    let source_evidence = match source.projection_evidence(&request.source) {
        Ok(evidence) => evidence,
        Err(failure) => {
            return deletion_failure(
                invocation,
                request,
                &retained,
                &public_target,
                &identity,
                &intent,
                &cleanup,
                failure,
            );
        }
    };
    let state = match port.resolved_state_for_source(&source) {
        Ok(state) => state,
        Err(failure) => {
            return deletion_failure(
                invocation,
                request,
                &retained,
                &public_target,
                &identity,
                &intent,
                &cleanup,
                failure,
            );
        }
    };
    let body = deletion_body(request, source_evidence.original_attribution);
    let dispatched = match port
        .dispatch_with_source_grant(
            invocation,
            &state,
            body,
            &identity,
            None,
            Some(source.authorized.grant.clone()),
        )
        .await
    {
        Ok(dispatched) => dispatched,
        Err(failure) => {
            return deletion_failure(
                invocation,
                request,
                &retained,
                &public_target,
                &identity,
                &intent,
                &cleanup,
                failure,
            );
        }
    };
    match port.project(
        invocation,
        dispatched,
        HostControlEvidence::DeleteBySource {
            source: source_evidence,
            command: &accepted,
            intent: &intent,
            cleanup: &cleanup,
        },
    ) {
        Ok(mut completed) => {
            completed.host_receipt = Some(intent.host_receipt);
            Ok(completed)
        }
        Err(failure) => deletion_failure(
            invocation,
            request,
            &retained,
            &public_target,
            &identity,
            &intent,
            &cleanup,
            failure,
        ),
    }
}

async fn cleanup_snapshots(
    port: &ProjectProviderControlPortV1,
    invocation: &ControlInvocationV1<'_, '_>,
    source: AuthorizedControlSourceV1,
    intent: ProviderSourceIntentReceiptV1,
    requested: bool,
) -> ControlResult<(AuthorizedControlSourceV1, HostSnapshotCleanupResultV1)> {
    if !requested {
        return Ok((source, HostSnapshotCleanupResultV1::not_requested()));
    }
    let ledger = port.ledger()?;
    let control = invocation.control.clone();
    // The enclosing executor keeps forwarding the original cancellation while
    // this existing blocking-pool operation settles all witnessed removals.
    tokio::task::spawn_blocking(move || {
        let cleanup = cleanup_source_snapshots(&ledger, &source.authorized, &intent, &control);
        (source, cleanup)
    })
    .await
    .map_err(|_| {
        ControlFailureV1::new(ControlFailureStageV1::BlockingRead(
            "host snapshot cleanup worker",
        ))
    })
}

fn unconfirmed_cleanup(requested: bool) -> HostSnapshotCleanupResultV1 {
    if requested {
        HostSnapshotCleanupResultV1 {
            state: HostSnapshotCleanupStateV1::Unverifiable,
            removed_snapshot_refs: Vec::new(),
            matched_count: 0,
            unverifiable_count: 1,
        }
    } else {
        HostSnapshotCleanupResultV1::not_requested()
    }
}

fn deletion_body(request: &ProviderDeleteBySourceRequestV1, source: &SourceAttribution) -> Value {
    json!({
        "forget_source_keys": [source.source.source_key],
        "mode": request.mode,
        "include_snapshots": request.include_snapshots,
        "retention_lock_policy_revision": PROVIDER_LOCAL_RETENTION_LOCK_POLICY_REVISION_V1,
        // The canonical protocol hashes the exact string bytes, not a JSON object.
        "verification_query": source.source.source_key,
    })
}

async fn record_intent(
    source: &AuthorizedControlSourceV1,
    request: &ProviderSourceDeletionIntentV1<'_>,
    invocation: &ControlInvocationV1<'_, '_>,
    identity: &ControlOperationIdentityV1,
) -> ControlResult<ProviderSourceIntentReceiptV1> {
    // The enclosing executor forwards the original caller control while still
    // joining this bounded write. Never wrap it in a cancellation-dropping select.
    match source
        .authorized
        .record_deletion_intent(request, &invocation.control)
        .await
    {
        Ok(receipt) => Ok(receipt),
        Err(ProviderSourceIntentActionErrorV1::Authority(error)) => Err(error.into()),
        Err(ProviderSourceIntentActionErrorV1::Refused(error)) => Err(intent_refusal(error)),
        Err(
            ProviderSourceIntentActionErrorV1::StorageUnconfirmed(_)
            | ProviderSourceIntentActionErrorV1::WorkerUnavailable,
        ) => {
            match source.authorized.read_deletion_intent_receipt(request, &invocation.control).await {
                Ok(Some(receipt)) => Ok(receipt),
                Ok(None) => Err(RetainedSurfaceExecutionErrorV1::unavailable(
                    "the host intent write failed and the exact receipt lookup confirmed no committed intent",
                ).into()),
                Err(_) => Err(ControlFailureV1::new(ControlFailureStageV1::HostIntentUnknown {
                    state: source.authorized.retained.scope.clone(),
                    identity: ControlOperationIdentityV1 {
                        operation_id: request.operation_id.to_owned(),
                        idempotency_key: identity.idempotency_key.clone(),
                    },
                    detail: "host intent write and exact receipt reconciliation could not be confirmed",
                })),
            }
        }
    }
}

fn intent_refusal(error: ObservationJournalError) -> ControlFailureV1 {
    match error {
        ObservationJournalError::UnsettledSource { .. } => {
            RetainedSurfaceExecutionErrorV1::Conflict.into()
        }
        ObservationJournalError::BudgetExhausted { .. } => {
            ControlFailureV1::new(ControlFailureStageV1::Control(
                tracedecay_memory_provider_registry::TerminalCode::DeadlineExceeded,
            ))
        }
        ObservationJournalError::OperationCancelled { .. } => {
            ControlFailureV1::new(ControlFailureStageV1::Control(
                tracedecay_memory_provider_registry::TerminalCode::Cancelled,
            ))
        }
        _ => RetainedSurfaceExecutionErrorV1::unavailable(
            "the host intent was refused before commit",
        )
        .into(),
    }
}

fn deletion_failure(
    invocation: &ControlInvocationV1<'_, '_>,
    request: &ProviderDeleteBySourceRequestV1,
    retained: &RetainedRecallControlScopeV1,
    target: &ProviderControlSourceTargetV1,
    identity: &ControlOperationIdentityV1,
    intent: &ProviderControlDeletionIntentV1,
    cleanup: &HostSnapshotCleanupResultV1,
    failure: ControlFailureV1,
) -> ControlResult<CompletedProviderControlV1> {
    let failure = normalize_caught_failure(invocation, failure);
    let public = invocation.request;
    let mut result = outcome::result_after_failure(public, retained, identity, &failure.stage);
    let reason_code = serde_json::to_value(result.terminal)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "provider_result_unconfirmed".to_owned());
    result.result =
        ProviderControlOperationResultV1::DeleteBySource(Some(ProviderDeleteBySourceResultV1 {
            source: request.source.clone(),
            target: target.clone(),
            mode: request.mode,
            include_snapshots: request.include_snapshots,
            intent: intent.clone(),
            host_snapshot_cleanup: projection::host_snapshot_cleanup(cleanup),
            erasure: ProviderControlErasureV1::Pending { reason_code },
        }));
    result
        .validate_for(public)
        .map_err(|_| binding("committed intent pending result"))?;
    Ok(CompletedProviderControlV1 {
        result,
        host_receipt: Some(intent.host_receipt.clone()),
    })
}

fn accepted_failure(
    invocation: &ControlInvocationV1<'_, '_>,
    state: &RetainedRecallControlScopeV1,
    identity: &ControlOperationIdentityV1,
    failure: ControlFailureV1,
) -> CompletedProviderControlV1 {
    let failure = normalize_caught_failure(invocation, failure);
    CompletedProviderControlV1 {
        result: outcome::result_after_failure(invocation.request, state, identity, &failure.stage),
        host_receipt: failure.host_receipt,
    }
}

fn normalize_caught_failure(
    invocation: &ControlInvocationV1<'_, '_>,
    failure: ControlFailureV1,
) -> ControlFailureV1 {
    // These failures become a typed completed outcome before the enclosing
    // executor sees them; preserve its first observed caller/deadline cause.
    match invocation.normalize_control_refusal::<()>(Err(failure)) {
        Err(failure) => failure,
        Ok(()) => unreachable!("normalizing a failure cannot produce success"),
    }
}

async fn refresh_source(
    port: &ProjectProviderControlPortV1,
    invocation: &ControlInvocationV1<'_, '_>,
    selector: &ProviderControlSourceSelectorV1,
    previous: &AuthorizedControlSourceV1,
    include_unavailable: bool,
) -> ControlResult<AuthorizedControlSourceV1> {
    let fresh = port
        .resolve_source(selector, include_unavailable, &invocation.control)
        .await?;
    if previous.target != fresh.target
        || previous.authorized.retained.scope != fresh.authorized.retained.scope
        || previous.granted_source()?.attribution != fresh.granted_source()?.attribution
    {
        return Err(binding(
            "retained source changed during host control admission",
        ));
    }
    Ok(fresh)
}

async fn accept_feedback(
    port: &ProjectProviderControlPortV1,
    invocation: &ControlInvocationV1<'_, '_>,
    request: &ProviderFeedbackRequestV1,
    source: &AuthorizedControlSourceV1,
) -> ControlResult<AcceptedFeedbackAssertionV1> {
    let ledger = port.ledger()?;
    let context = invocation.context.request_context.clone();
    let request = request.clone();
    let target = source.target.clone();
    let attribution = source.granted_source()?.attribution.clone();
    let control = invocation.control.clone();
    tokio::task::spawn_blocking(move || {
        ledger.accept_feedback_assertion(&context, &request, &target, &attribution, &control)
    })
    .await
    .map_err(|_| {
        ControlFailureV1::new(ControlFailureStageV1::BlockingRead(
            "feedback assertion acceptance worker",
        ))
    })?
    .map_err(command_error)
}

async fn accept_deletion(
    port: &ProjectProviderControlPortV1,
    invocation: &ControlInvocationV1<'_, '_>,
    request: &ProviderDeleteBySourceRequestV1,
    source: &AuthorizedControlSourceV1,
) -> ControlResult<AcceptedDeletionCommandV1> {
    let ledger = port.ledger()?;
    let context = invocation.context.request_context.clone();
    let request = request.clone();
    let target = source.target.clone();
    let attribution = source.granted_source()?.attribution.clone();
    let control = invocation.control.clone();
    tokio::task::spawn_blocking(move || {
        ledger.accept_deletion_command(&context, &request, &target, &attribution, &control)
    })
    .await
    .map_err(|_| {
        ControlFailureV1::new(ControlFailureStageV1::BlockingRead(
            "deletion command acceptance worker",
        ))
    })?
    .map_err(command_error)
}

fn feedback_identity(accepted: &AcceptedFeedbackAssertionV1) -> ControlOperationIdentityV1 {
    ControlOperationIdentityV1 {
        operation_id: accepted.operation_id().to_owned(),
        idempotency_key: Some(accepted.idempotency_key().to_owned()),
    }
}

fn deletion_identity(accepted: &AcceptedDeletionCommandV1) -> ControlOperationIdentityV1 {
    ControlOperationIdentityV1 {
        operation_id: accepted.operation_id().to_owned(),
        idempotency_key: Some(accepted.idempotency_key().to_owned()),
    }
}

fn deletion_mode(mode: ProviderControlDeletionModeV1) -> DeletionMode {
    match mode {
        ProviderControlDeletionModeV1::RemoveInfluence => DeletionMode::RemoveInfluence,
        ProviderControlDeletionModeV1::HardDelete => DeletionMode::HardDelete,
        ProviderControlDeletionModeV1::Anonymize => DeletionMode::Anonymize,
    }
}

fn command_error(error: HostSourceCommandErrorV1) -> ControlFailureV1 {
    match error {
        HostSourceCommandErrorV1::Invalid(_) => invalid_request(),
        HostSourceCommandErrorV1::Conflict => RetainedSurfaceExecutionErrorV1::Conflict.into(),
        HostSourceCommandErrorV1::Control(code) => {
            ControlFailureV1::new(ControlFailureStageV1::Control(code))
        }
        HostSourceCommandErrorV1::CapacityExceeded => {
            RetainedSurfaceExecutionErrorV1::Saturated.into()
        }
        error => RetainedSurfaceExecutionErrorV1::unavailable(format!(
            "the host source command was not confirmed; the provider was not contacted: {error}"
        ))
        .into(),
    }
}

fn timestamp(at: UtcMicros) -> ControlResult<String> {
    rfc3339_utc_micros(at.0).ok_or_else(invalid_request)
}

fn nanos(at: UtcMicros) -> ControlResult<i64> {
    at.0.checked_mul(1_000).ok_or_else(invalid_request)
}

fn binding(field: &'static str) -> ControlFailureV1 {
    ControlFailureV1::new(ControlFailureStageV1::InvalidBinding(field))
}

fn invalid_request() -> ControlFailureV1 {
    RetainedSurfaceExecutionErrorV1::InvalidRequest.into()
}

#[cfg(test)]
#[path = "tests/source_controls.rs"]
mod tests;
