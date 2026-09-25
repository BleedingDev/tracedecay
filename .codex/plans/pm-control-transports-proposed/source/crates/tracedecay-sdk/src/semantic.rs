//! Cross-field result validation selected by generated operation metadata.

use serde_json::Value;
use tracedecay_contracts::RequestId;
use tracedecay_contracts::retained_surfaces::{
    AutomationRunResultV1, FactStoreCurateRequestV1, ProviderControlRequestV1,
    ProviderControlResultV1, ProviderCorrectionRequestV1, ProviderDeleteBySourceRequestV1,
    ProviderFeedbackRequestV1, ProviderHealthRequestV1, ProviderInspectionRequestV1,
    ProviderMaintenanceRequestV1, ProviderReplayRequestV1, ProviderSnapshotExportRequestV1,
    ProviderSnapshotRestoreRequestV1, RetainedSurfaceOperation, SdkResultSemanticsV1,
};

pub(crate) fn response_matches(
    semantics: SdkResultSemanticsV1,
    selected_operation_id: &str,
    request_id: &str,
    expected_request_id: Option<&RequestId>,
    request: &Value,
    result: &Value,
) -> bool {
    if expected_request_id.is_some_and(|expected| expected.as_str() != request_id) {
        return false;
    }
    match semantics {
        SdkResultSemanticsV1::SchemaOnly => true,
        SdkResultSemanticsV1::ProviderControlTerminal => {
            let Some(operation) = selected_operation_id
                .strip_prefix("operation.application.")
                .and_then(RetainedSurfaceOperation::from_operation_name)
            else {
                return false;
            };
            // The generated operation selects the request type. The shared
            // result schema and returned operation never choose that type.
            macro_rules! decode {
                ($request:ty, $variant:ident) => {
                    serde_json::from_value::<$request>(request.clone())
                        .ok()
                        .map(ProviderControlRequestV1::$variant)
                };
            }
            let request = match operation {
                RetainedSurfaceOperation::ProviderFeedback => {
                    decode!(ProviderFeedbackRequestV1, Feedback)
                }
                RetainedSurfaceOperation::ProviderCorrection => {
                    decode!(ProviderCorrectionRequestV1, Correction)
                }
                RetainedSurfaceOperation::ProviderDeleteBySource => {
                    decode!(ProviderDeleteBySourceRequestV1, DeleteBySource)
                }
                RetainedSurfaceOperation::ProviderHealth => {
                    decode!(ProviderHealthRequestV1, Health)
                }
                RetainedSurfaceOperation::ProviderInspection => {
                    decode!(ProviderInspectionRequestV1, Inspection)
                }
                RetainedSurfaceOperation::ProviderMaintenance => {
                    decode!(ProviderMaintenanceRequestV1, Maintenance)
                }
                RetainedSurfaceOperation::ProviderSnapshotExport => {
                    decode!(ProviderSnapshotExportRequestV1, SnapshotExport)
                }
                RetainedSurfaceOperation::ProviderSnapshotRestore => {
                    decode!(ProviderSnapshotRestoreRequestV1, SnapshotRestore)
                }
                RetainedSurfaceOperation::ProviderReplay => {
                    decode!(ProviderReplayRequestV1, Replay)
                }
                _ => return false,
            };
            let Some(request) = request else {
                return false;
            };
            serde_json::from_value::<ProviderControlResultV1>(result.clone())
                .is_ok_and(|result| result.validate_for(&request).is_ok())
        }

        SdkResultSemanticsV1::FactStoreCurateTerminal => {
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
            serde_json::from_value::<AutomationRunResultV1>(result.clone())
                .is_ok_and(|result| result.matches_admission(&admission))
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tracedecay_contracts::retained_surfaces::SdkResultSemanticsV1;

    use super::response_matches;

    #[test]
    fn curate_semantics_reject_a_structural_terminal_with_foreign_run_identity() {
        let terminal = json!({
            "run_id": "request.foreign",
            "task": "memory_curator",
            "request_digest": concat!(
                "sha256:",
                "0000000000000000000000000000000000000000000000000000000000000000"
            ),
            "terminal": {
                "status": "completed",
                "summary": {
                    "reviewed_count": 0,
                    "accepted_count": 0,
                    "rejected_count": 0,
                    "skipped_count": 0
                }
            },
            "committed_receipts": []
        });
        assert!(!response_matches(
            SdkResultSemanticsV1::FactStoreCurateTerminal,
            "operation.application.fact_store_curate",
            "request.sdk.curate",
            None,
            &json!({}),
            &terminal,
        ));
    }
    fn inspection_request() -> serde_json::Value {
        json!({
            "state": {"kind":"canonical_session","provider_id":"native","registration_revision":7,"canonical_provider_id":"claude","session_id":"session.original"},
            "selection": {"view":"source_influence","source":{
                "trace_ref":"trace.original","item_ref":"item.1","observation_id":"observation.1"
            }},
            "maximum_items":10,"maximum_bytes":65536
        })
    }

    fn inspection_result(request: &serde_json::Value) -> serde_json::Value {
        json!({
            "provider_id":"native","registration_revision":7,
            "scope":{
                "profile_id":"profile.1","project_id":"project.1",
                "repository_identity":"repository.1","worktree_identity":"worktree.1",
                "branch_identity":"refs/heads/master","agent_session_id":"session.1",
                "resolved_scope_digest":format!("sha256:{}", "a".repeat(64))
            },
            "operation_id":"018f22c2-77cd-7000-8000-000000000001",
            "idempotency_key":null,"terminal":"success_zero_results","domain_detail":null,
            "effect":{
                "state":"none","committed_boundary":null,
                "state_generation_before":null,"state_generation_after":null,
                "committed_item_refs":[],"uncommitted_item_refs":[],
                "provider_receipt_digest":null,"reconciliation_action":null,
                "verification_digest":null,"duplicate_of_idempotency_key":null,
                "duplicate_of_operation_id":null
            },
            "result":{"operation":"inspection","data":{
                "selection":request["selection"],
                "items":{"view":"source_influence","items":[]},
                "coverage":"complete","next_cursor":null,"redactions":[],"state_generation":3
            }},
            "warnings":[]
        })
    }

    #[test]
    fn provider_semantics_reject_cross_operation_results_with_a_shared_result_schema() {
        let request = inspection_request();
        let result = inspection_result(&request);
        let typed: tracedecay_contracts::retained_surfaces::ProviderControlResultV1 =
            serde_json::from_value(result.clone()).expect("structurally valid result");
        typed
            .validate()
            .expect("independently valid inspection evidence");
        assert!(response_matches(
            SdkResultSemanticsV1::ProviderControlTerminal,
            "operation.application.provider_inspection",
            "request.inspection",
            None,
            &request,
            &result,
        ));
        // Both the selected health request and the returned inspection result
        // are individually valid. Their operation identities must still bind.
        let health_request = json!({"state":request["state"],"requested_checks":["state"]});
        let _: tracedecay_contracts::retained_surfaces::ProviderHealthRequestV1 =
            serde_json::from_value(health_request.clone()).expect("valid health request");
        assert!(!response_matches(
            SdkResultSemanticsV1::ProviderControlTerminal,
            "operation.application.provider_health",
            "request.health",
            None,
            &health_request,
            &result,
        ));
        for operation in [
            "provider_inspection",
            "operation.application.memory_status",
            "operation.application.unknown",
        ] {
            assert!(!response_matches(
                SdkResultSemanticsV1::ProviderControlTerminal,
                operation,
                "request.inspection",
                None,
                &request,
                &result,
            ));
        }
    }

    #[test]
    fn provider_semantics_bind_selection_registration_and_application_request_identity() {
        let request = inspection_request();
        let result = inspection_result(&request);
        for (field, foreign) in [
            ("provider_id", json!("ncm")),
            ("registration_revision", json!(8)),
        ] {
            let mut altered = request.clone();
            altered["state"][field] = foreign;
            assert!(!response_matches(
                SdkResultSemanticsV1::ProviderControlTerminal,
                "operation.application.provider_inspection",
                "request.inspection",
                None,
                &altered,
                &result,
            ));
        }
        let expected =
            tracedecay_contracts::RequestId::new("request.inspection").expect("request ID");
        assert!(!response_matches(
            SdkResultSemanticsV1::ProviderControlTerminal,
            "operation.application.provider_inspection",
            "request.foreign",
            Some(&expected),
            &request,
            &result,
        ));
        let mut foreign_source = request.clone();
        foreign_source["selection"]["source"]["observation_id"] = json!("observation.foreign");
        assert!(!response_matches(
            SdkResultSemanticsV1::ProviderControlTerminal,
            "operation.application.provider_inspection",
            "request.inspection",
            Some(&expected),
            &foreign_source,
            &result,
        ));
        let wrapped = json!({"operation":"inspection","request":request});
        assert!(!response_matches(
            SdkResultSemanticsV1::ProviderControlTerminal,
            "operation.application.provider_inspection",
            "request.inspection",
            Some(&expected),
            &wrapped,
            &result,
        ));
    }
}
