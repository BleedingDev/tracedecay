//! Cross-field result validation selected by generated operation metadata.

use serde_json::Value;
use tracedecay_contracts::retained_surfaces::{
    FactStoreCurateRequestV1, FactStoreCurateResultV1,
    ProviderControlEffectStateV1 as ProviderEffectState, ProviderControlFeedbackSignalV1,
    ProviderControlMutationReceiptV1, ProviderControlOperationResultV1, ProviderControlRequestV1,
    ProviderControlResultV1, ProviderControlTerminalV1 as ProviderTerminal,
    RetainedSurfaceOperation, RetainedSurfaceResultV1, SdkResultSemanticsV1,
    retained_surface_outcome_matches_terminal,
};
use tracedecay_contracts::{
    ApplicationEnvelope, ApplicationOutcome, EffectResult, EffectTermination, OperationTermination,
    ReconciliationState, RequestId,
};

pub(crate) fn response_matches(
    semantics: SdkResultSemanticsV1,
    operation_id: &str,
    request_id: &str,
    expected_request_id: Option<&RequestId>,
    request: &Value,
    result: &Value,
    envelope: Option<&Value>,
) -> bool {
    if expected_request_id.is_some_and(|expected| expected.as_str() != request_id) {
        return false;
    }
    match semantics {
        SdkResultSemanticsV1::SchemaOnly => true,
        SdkResultSemanticsV1::ProviderControlTerminal => {
            // SDK requests are the concrete operation body, without the retained
            // union tag. Only the generated caller metadata selects that tag.
            let Some(operation) = operation_id
                .strip_prefix("operation.application.")
                .and_then(RetainedSurfaceOperation::from_operation_name)
            else {
                return false;
            };
            let operation = match operation {
                RetainedSurfaceOperation::ProviderFeedback => "feedback",
                RetainedSurfaceOperation::ProviderCorrection => "correction",
                RetainedSurfaceOperation::ProviderDeleteBySource => "delete_by_source",
                RetainedSurfaceOperation::ProviderHealth => "health",
                RetainedSurfaceOperation::ProviderInspection => "inspection",
                RetainedSurfaceOperation::ProviderMaintenance => "maintenance",
                RetainedSurfaceOperation::ProviderSnapshotExport => "snapshot_export",
                RetainedSurfaceOperation::ProviderSnapshotRestore => "snapshot_restore",
                RetainedSurfaceOperation::ProviderReplay => "replay",
                _ => return false,
            };
            let Ok(request) = serde_json::from_value::<ProviderControlRequestV1>(
                serde_json::json!({"operation": operation, "request": request}),
            ) else {
                return false;
            };
            if let Some(envelope) = envelope {
                provider_http_response_matches(&request, request_id, envelope)
            } else {
                serde_json::from_value::<ProviderControlResultV1>(result.clone())
                    .is_ok_and(|result| result.validate_for(&request).is_ok())
            }
        }
        SdkResultSemanticsV1::FactStoreCurateReceipt => {
            let Ok(request_id) = RequestId::new(request_id.to_owned()) else {
                return false;
            };
            let Ok(request) = serde_json::from_value::<FactStoreCurateRequestV1>(request.clone())
            else {
                return false;
            };
            let Ok(admission) = request.automation_request(&request_id) else {
                return false;
            };
            serde_json::from_value::<FactStoreCurateResultV1>(result.clone())
                .is_ok_and(|result| result.matches_admission(&admission))
        }
    }
}

fn provider_http_response_matches(
    request: &ProviderControlRequestV1,
    request_id: &str,
    envelope: &Value,
) -> bool {
    // The HTTP binding is transport metadata outside ApplicationEnvelope.
    let mut envelope = envelope.clone();
    let Some(object) = envelope.as_object_mut() else {
        return false;
    };
    object.remove("binding_id");
    let Ok(envelope) =
        serde_json::from_value::<ApplicationEnvelope<ProviderControlResultV1>>(envelope)
    else {
        return false;
    };
    if envelope.request_id.as_str() != request_id {
        return false;
    }
    let Some(result) = envelope.outcome.payload() else {
        return false;
    };
    if result.validate_for(request).is_err() {
        return false;
    }
    let lifecycle_matches = match &envelope.outcome {
        ApplicationOutcome::Evidence(packet) => {
            packet.execution.validate().is_ok()
                && packet.execution.termination == provider_read_termination(result.terminal)
        }
        ApplicationOutcome::Effect(effect) => {
            let termination = match &result.result {
                // The host fence is durable even when provider erasure is pending.
                ProviderControlOperationResultV1::DeleteBySource(Some(_)) => {
                    Some(EffectTermination::Completed)
                }
                _ => provider_effect_termination(result),
            };
            termination.is_some_and(|termination| {
                effect.receipt.outcome == termination
                    && effect.effect_id.as_str()
                        == format!("effect.provider-control.{}", result.operation_id)
                    && Some(effect.idempotency_key.as_str()) == result.idempotency_key.as_deref()
                    && effect.reconciliation
                        == if matches!(
                            termination,
                            EffectTermination::Partial | EffectTermination::EffectUnknown
                        ) {
                            ReconciliationState::Pending
                        } else {
                            ReconciliationState::Reconciled
                        }
                    && EffectResult::<()>::new(
                        effect.effect_id.clone(),
                        effect.effect_class,
                        effect.idempotency_key.clone(),
                        effect.authority.clone(),
                        effect.expected_state.clone(),
                        effect.execution.clone(),
                        effect.reconciliation,
                        effect.receipt.clone(),
                        None,
                    )
                    .is_ok()
            })
        }
        ApplicationOutcome::Preview(_) | ApplicationOutcome::Result(_) => false,
    };
    lifecycle_matches
        && retained_surface_outcome_matches_terminal(
            request.operation(),
            &envelope.request_id,
            &envelope.scope,
            &envelope
                .outcome
                .map_payload(RetainedSurfaceResultV1::ProviderControl),
        )
}

// These projections follow provider_control/outcome.rs read_termination and
// effect_termination, including its distinct completed deletion host intent.
fn provider_read_termination(terminal: ProviderTerminal) -> OperationTermination {
    match terminal {
        ProviderTerminal::Success | ProviderTerminal::SuccessZeroResults => {
            OperationTermination::Completed
        }
        ProviderTerminal::Partial | ProviderTerminal::PartialEffect => {
            OperationTermination::Partial
        }
        ProviderTerminal::Cancelled => OperationTermination::Cancelled,
        ProviderTerminal::DeadlineExceeded => OperationTermination::TimedOut,
        ProviderTerminal::ProviderUnavailable | ProviderTerminal::ScopeUnavailable => {
            OperationTermination::Unavailable
        }
        ProviderTerminal::EffectUnknown => OperationTermination::EffectUnknown,
        _ => OperationTermination::Failed,
    }
}

fn provider_effect_termination(result: &ProviderControlResultV1) -> Option<EffectTermination> {
    match result.effect.state {
        ProviderEffectState::Unknown => Some(EffectTermination::EffectUnknown),
        ProviderEffectState::Partial => Some(EffectTermination::Partial),
        ProviderEffectState::Committed | ProviderEffectState::Duplicate => {
            Some(if result.terminal == ProviderTerminal::Partial {
                EffectTermination::Partial
            } else {
                EffectTermination::Completed
            })
        }
        ProviderEffectState::None => match result.terminal {
            ProviderTerminal::Success | ProviderTerminal::SuccessZeroResults => {
                let unchanged = |receipt: &ProviderControlMutationReceiptV1| {
                    receipt.state_generation_before == receipt.state_generation_after
                };
                let verified = match &result.result {
                    ProviderControlOperationResultV1::Feedback(Some(data)) => {
                        unchanged(&data.receipt)
                            && data.signal == ProviderControlFeedbackSignalV1::Ignored
                    }
                    ProviderControlOperationResultV1::Correction(Some(data)) => {
                        unchanged(&data.receipt) && data.affected_provider_effects == 0
                    }
                    ProviderControlOperationResultV1::Maintenance(Some(data)) => {
                        unchanged(&data.receipt)
                            && (data.dry_run
                                || (data.changed_items == 0 && data.removed_items == 0))
                            && data.state_changed != Some(true)
                            && !data.partial
                    }
                    ProviderControlOperationResultV1::SnapshotRestore(Some(data)) => {
                        unchanged(&data.receipt)
                    }
                    ProviderControlOperationResultV1::Replay(Some(data)) => {
                        unchanged(&data.receipt)
                            && data.applied_observations == 0
                            && data.rejected_observations == 0
                            && data.effect_unknown_observations == 0
                            && !data.partial
                    }
                    _ => false,
                };
                (verified
                    && result.effect.committed_item_refs.is_empty()
                    && result.effect.committed_boundary.is_none())
                .then_some(EffectTermination::NoChange)
            }
            ProviderTerminal::Cancelled => Some(EffectTermination::Cancelled),
            ProviderTerminal::DeadlineExceeded => Some(EffectTermination::TimedOut),
            _ => Some(EffectTermination::Failed),
        },
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tracedecay_contracts::retained_surfaces::{
        FactStoreCurateRequestV1, FactStoreCurateResultV1, SdkResultSemanticsV1,
    };
    use tracedecay_contracts::{RequestId, ResolvedScope};
    use tracedecay_domain::{ProjectId, RepositoryId, WorktreeId};

    use super::response_matches;

    fn provider_effect_envelope(operation: &str, result: &serde_json::Value) -> serde_json::Value {
        let scope = ResolvedScope::new(
            ProjectId::new("project-1").expect("project"),
            RepositoryId::new("repo-1").expect("repository"),
            WorktreeId::new("worktree-1").expect("worktree"),
            None,
        )
        .expect("canonical scope");
        let digest = format!("sha256:{}", "a".repeat(64));
        json!({
            "binding_id":format!("binding.http.{operation}.v1"),
            "contract":{"schema_id":format!("schema.application.retained.{}.result", operation.replace('_', "-")),"schema_revision":1},
            "request_id":"request.sdk.provider","scope":scope,
            "outcome":{"outcome":"effect","value":{
                "effect_id":format!("effect.provider-control.{}",result["operation_id"].as_str().expect("provider operation")),
                "effect_class":"administrative","idempotency_key":"d".repeat(64),
                "expected_state":digest,"reconciliation":"reconciled",
                "authority":{"grant_id":"grant.sdk","grant_revision":1,"grant_digest":digest,
                    "authorized_scope_digest":scope.scope_digest,"disclosure":"evidence",
                    "policy":{"decision_id":"policy.sdk","revision":1,"digest":digest,"evaluator_revision":"policy.v1"},
                    "revalidated_at":10},
                "receipt":{"operation":format!("use-case.application.retained.{}",operation.replace('_', "-")),
                    "request_id":"request.sdk.provider","actor":"actor.sdk","scope":scope,
                    "effect_class":"administrative","idempotency_key":"d".repeat(64),
                    "input_digest":digest,"expected_state":digest,"policy_digest":digest,
                    "configuration_digest":digest,"catalog_digest":digest,"privacy_digest":digest,
                    "outcome":"completed","committed_state":digest,"external_proof":null},
                "execution":{"started_at":10,"ended_at":20,"effective_deadline":{"expires_at":30},
                    "cancellation":null,"budget":{"units_consumed":1,"bytes_consumed":1,"elapsed_micros":1},
                    "termination":"completed"},
                "payload":result
            }}
        })
    }

    #[test]
    fn curate_semantics_accept_only_the_receipt_for_the_admitted_run() {
        let request = json!({ "fact_review_limit": 4 });
        let admission = serde_json::from_value::<FactStoreCurateRequestV1>(request.clone())
            .expect("curate request")
            .automation_request(&RequestId::new("request.sdk.curate").expect("request id"))
            .expect("admission");
        let receipt =
            serde_json::to_value(FactStoreCurateResultV1::started(&admission).expect("receipt"))
                .expect("receipt json");
        assert_eq!(receipt["run_id"], "request.sdk.curate");
        assert_eq!(receipt["task"], "memory_curator");
        assert_eq!(receipt["state"], "started");
        let matches = |request_id: &str, result: &serde_json::Value| {
            response_matches(
                SdkResultSemanticsV1::FactStoreCurateReceipt,
                "operation.application.fact_store_curate",
                request_id,
                None,
                &request,
                result,
                None,
            )
        };
        assert!(matches("request.sdk.curate", &receipt));
        assert!(!matches("request.sdk.foreign", &receipt));

        let mut terminal = receipt.clone();
        let object = terminal.as_object_mut().expect("receipt object");
        object.remove("state");
        object.insert(
            "terminal".to_owned(),
            json!({"status": "completed", "summary": {
                "reviewed_count": 0, "accepted_count": 0, "rejected_count": 0, "skipped_count": 0
            }}),
        );
        object.insert("committed_receipts".to_owned(), json!([]));
        assert!(
            !matches("request.sdk.curate", &terminal),
            "the run terminal is no longer the curate result"
        );
    }

    #[test]
    fn provider_control_semantics_bind_generated_operation_source_and_terminal_effect() {
        use crate::operations::{
            ApplicationProviderFeedback, ApplicationProviderHealth, TypedOperation,
        };
        use tracedecay_contracts::RequestId;
        use tracedecay_contracts::retained_surfaces::ProviderControlResultV1;

        let source = json!({"trace_ref":"trace.retained.1","item_ref":"item.1","observation_id":"observation.1"});
        let request: <ApplicationProviderFeedback as TypedOperation>::Request =
            serde_json::from_value(json!({"source":source,"signal":"helpful","weight":"1","evidence_refs":[],"occurred_at":1})).expect("concrete SDK request body");
        let request = serde_json::to_value(request).expect("SDK serialized request");
        assert!(request.get("operation").is_none());
        let expected_id = RequestId::new("request.sdk.provider").expect("request ID");
        let none_effect = json!({"state":"none","committed_boundary":null,"state_generation_before":null,"state_generation_after":null,"committed_item_refs":[],"uncommitted_item_refs":[],"provider_receipt_digest":null,"reconciliation_action":null,"verification_digest":null,"duplicate_of_idempotency_key":null,"duplicate_of_operation_id":null});
        let mut effect = none_effect.clone();
        effect["state"] = json!("committed");
        effect["state_generation_before"] = json!(3);
        effect["state_generation_after"] = json!(4);
        effect["provider_receipt_digest"] = json!("b".repeat(64));
        effect["verification_digest"] = json!("c".repeat(64));
        let result = json!({
            "provider_id":"native","registration_revision":7,
            "scope":{"profile_id":"profile-1","project_id":"project-1","repository_identity":"repo-1","worktree_identity":"worktree-1","branch_identity":"refs/heads/main","agent_session_id":"session-1","resolved_scope_digest":format!("sha256:{}","1".repeat(64))},
            "operation_id":"018f22c2-77cd-7000-8000-000000000001","idempotency_key":"d".repeat(64),
            "terminal":"success","diagnostic_id":null,"domain_detail":null,"effect":effect,"warnings":[],
            "result":{"operation":"feedback","data":{
                "source":source,
                "target":{"stable_memory_ref":"provider.memory.1","source":{"canonical_provider_id":"native","canonical_session_id":"session-1","source_key":"source.1","stable_record_id":null,"observation_id":"observation.1","source_revision":"revision.1","content_sha256":"a".repeat(64)}},
                "target_digest":"a".repeat(64),"signal":"helpful","applied_effect":"recorded",
                "receipt":{"state_generation_before":3,"state_generation_after":4,"provider_receipt_digest":"b".repeat(64)}
            }}
        });
        let matches = |result: &serde_json::Value| {
            response_matches(
                ApplicationProviderFeedback::RESULT_SEMANTICS,
                ApplicationProviderFeedback::OPERATION_ID,
                expected_id.as_str(),
                Some(&expected_id),
                &request,
                result,
                None,
            )
        };
        assert!(
            matches(&result),
            "actual typed request and committing feedback"
        );

        let mut foreign_operation = result.clone();
        foreign_operation["result"] = json!({"operation":"health","data":null});
        foreign_operation["terminal"] = json!("provider_unavailable");
        foreign_operation["effect"] = none_effect;
        foreign_operation["idempotency_key"] = serde_json::Value::Null;
        serde_json::from_value::<ProviderControlResultV1>(foreign_operation.clone())
            .expect("typed foreign terminal")
            .validate()
            .expect("independently valid health result");
        assert!(
            !matches(&foreign_operation),
            "a valid result for another operation is refused"
        );
        assert!(
            !response_matches(
                ApplicationProviderFeedback::RESULT_SEMANTICS,
                ApplicationProviderHealth::OPERATION_ID,
                expected_id.as_str(),
                Some(&expected_id),
                &request,
                &result,
                None,
            ),
            "the response cannot choose its own operation"
        );

        let mut foreign_source = result.clone();
        foreign_source["result"]["data"]["source"]["observation_id"] = json!("observation.other");
        foreign_source["result"]["data"]["target"]["source"]["observation_id"] =
            json!("observation.other");
        serde_json::from_value::<ProviderControlResultV1>(foreign_source.clone())
            .expect("typed foreign source")
            .validate()
            .expect("internally consistent foreign source result");
        assert!(
            !matches(&foreign_source),
            "the target must belong to the requested source"
        );

        let mut invalid_terminal = result.clone();
        invalid_terminal["terminal"] = json!("provider_unavailable");
        assert!(
            !matches(&invalid_terminal),
            "unavailable cannot claim committed feedback"
        );
        assert!(
            !response_matches(
                ApplicationProviderFeedback::RESULT_SEMANTICS,
                ApplicationProviderFeedback::OPERATION_ID,
                "request.foreign",
                Some(&expected_id),
                &request,
                &result,
                None,
            ),
            "expected request ID remains mandatory"
        );
        assert!(
            !response_matches(
                ApplicationProviderFeedback::RESULT_SEMANTICS,
                "operation.application.fact_store_curate",
                expected_id.as_str(),
                Some(&expected_id),
                &request,
                &result,
                None,
            ),
            "non-provider metadata cannot select a provider validator"
        );

        let http_matches = |envelope: &serde_json::Value| {
            response_matches(
                ApplicationProviderFeedback::RESULT_SEMANTICS,
                ApplicationProviderFeedback::OPERATION_ID,
                expected_id.as_str(),
                Some(&expected_id),
                &request,
                &result,
                Some(envelope),
            )
        };
        let mut envelope = provider_effect_envelope("provider_feedback", &result);
        assert!(http_matches(&envelope), "canonical HTTP effect is accepted");
        envelope["outcome"]["value"]["execution"]["termination"] = json!("unavailable");
        assert!(
            !http_matches(&envelope),
            "outer execution cannot contradict provider success"
        );
        envelope = provider_effect_envelope("provider_feedback", &result);
        envelope["outcome"]["value"]["receipt"]["request_id"] = json!("request.foreign");
        assert!(
            !http_matches(&envelope),
            "outer receipt belongs to the admitted HTTP request"
        );

        let delete_request = json!({"source":source,"mode":"remove_influence","expected_fence_revision":0,"include_snapshots":false});
        let mut deletion = result.clone();
        deletion["terminal"] = json!("provider_unavailable");
        deletion["effect"]["state"] = json!("none");
        for field in [
            "state_generation_before",
            "state_generation_after",
            "provider_receipt_digest",
            "verification_digest",
        ] {
            deletion["effect"][field] = serde_json::Value::Null;
        }
        deletion["result"] = json!({"operation":"delete_by_source","data":{
            "source":source,"target":result["result"]["data"]["target"],"mode":"remove_influence","include_snapshots":false,
            "intent":{"fence_revision_before":0,"fence_revision_after":1,"host_receipt":null},
            "host_snapshot_cleanup":{"state":"not_requested","removed_snapshot_refs":[],"matched_count":0,"unverifiable_count":0},
            "erasure":{"state":"pending","reason_code":"provider_unavailable"}
        }});
        let mut envelope = provider_effect_envelope("provider_delete_by_source", &deletion);
        deletion["result"]["data"]["intent"]["host_receipt"] =
            envelope["outcome"]["value"]["receipt"].clone();
        envelope["outcome"]["value"]["payload"] = deletion.clone();
        let http_delete_matches = |envelope: &serde_json::Value| {
            response_matches(
                ApplicationProviderFeedback::RESULT_SEMANTICS,
                "operation.application.provider_delete_by_source",
                expected_id.as_str(),
                Some(&expected_id),
                &delete_request,
                &deletion,
                Some(envelope),
            )
        };
        assert!(
            http_delete_matches(&envelope),
            "host fence completed despite unavailable provider erasure"
        );
        envelope["outcome"]["value"]["receipt"]["input_digest"] =
            json!(format!("sha256:{}", "b".repeat(64)));
        assert!(
            !http_delete_matches(&envelope),
            "inner deletion intent must equal the full outer receipt"
        );
        envelope["outcome"]["value"]["receipt"] =
            deletion["result"]["data"]["intent"]["host_receipt"].clone();
        envelope["outcome"]["value"]["execution"]["termination"] = json!("unavailable");
        assert!(
            !http_delete_matches(&envelope),
            "provider denial cannot undo the completed host fence"
        );
    }
}
