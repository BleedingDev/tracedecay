//! The retained HTTP replay-collision terminal.

use axum::response::{IntoResponse, Response};
use tracedecay_contracts::retained_surfaces::RetainedSurfaceOperation;
use tracedecay_tool_catalog::RouteExposureV1;

use tracedecay_daemon_protocol::ApplicationSurfaceAdapterError;

/// Terminal for a replayed request id that is still active.
///
/// The operation is the retained operation the request-control layer admitted
/// a caller-supplied request id for (`fact_store_curate` upstream, plus the
/// provider-control operations when the memory-provider host mounts them), so
/// the conflict envelope names that operation's own result contract.
pub(super) fn active_request_conflict_response(
    operation: RetainedSurfaceOperation,
    request_id: tracedecay_contracts::RequestId,
) -> Response {
    match active_request_conflict(operation, request_id) {
        Ok(result) => result.into_http_response(),
        Err(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

fn active_request_conflict(
    operation: RetainedSurfaceOperation,
    request_id: tracedecay_contracts::RequestId,
) -> Result<
    tracedecay_api::CanonicalInvocationResult<serde_json::Value>,
    ApplicationSurfaceAdapterError,
> {
    let registry = tracedecay_contracts::application_http_executable_binding_registry()?;
    let operation_id = tracedecay_tool_catalog::OperationId::new(format!(
        "operation.application.{}",
        operation.as_str()
    ))
    .map_err(ApplicationSurfaceAdapterError::Identifier)?;
    let binding = registry
        .get(&operation_id)
        .and_then(|availability| availability.binding())
        .ok_or(ApplicationSurfaceAdapterError::UnknownOrNotAuthorized)?;
    let RouteExposureV1::Public { binding_id, .. } = binding.exposure() else {
        return Err(ApplicationSurfaceAdapterError::UnknownOrNotAuthorized);
    };
    let contract = tracedecay_contracts::ResultContractRef::new(
        binding.result_schema().schema_ref().schema_id().clone(),
        binding.result_schema().schema_ref().revision(),
    )?;
    let problem = tracedecay_contracts::ApplicationProblemEnvelope::new(
        contract,
        request_id,
        tracedecay_contracts::ApplicationProblem::Conflict {
            diagnostic: tracedecay_contracts::SafeDiagnostic {
                code: "retained.request_already_active".to_owned(),
                message: "The retained application request is already active".to_owned(),
            },
            retry: tracedecay_contracts::RetryDirective::SameRequest,
            legal_actions: vec![tracedecay_contracts::LegalAction::Retry],
        },
    )?
    .with_owning_layer(tracedecay_contracts::ProblemOwningLayer::Adapter);
    Ok(tracedecay_api::CanonicalInvocationResult::new(
        binding_id.clone(),
        Err(problem),
    ))
}

#[cfg(test)]
mod conflict_tests {
    use super::{RetainedSurfaceOperation, active_request_conflict};

    #[test]
    fn active_replay_collision_preserves_same_request_retry_authority() {
        let request_id =
            tracedecay_contracts::RequestId::new("request.sdk.curate").expect("request id");
        let envelope = serde_json::to_value(
            active_request_conflict(RetainedSurfaceOperation::FactStoreCurate, request_id)
                .expect("conflict")
                .into_http_json(),
        )
        .expect("wire envelope");
        assert_eq!(envelope["value"]["problem"]["retry"], "same_request");
        assert_eq!(envelope["value"]["problem"]["retry_scope"], "same_request");
        assert_eq!(
            envelope["value"]["problem"]["legal_actions"],
            serde_json::json!(["retry"])
        );
    }
}
