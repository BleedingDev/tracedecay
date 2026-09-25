//! Controlled RPC transport for an already constructed provider-control request.
//!
//! This module selects no action, source, revision, state generation, or receipt.
//! Its response artifact is a serialization of the typed RPC client's result,
//! not original socket bytes or subprocess output. The client may itself return
//! an indeterminate response after an interrupted authoritative operation.

use super::{FixtureResult, HostFixture, io_result, write_durable};
use serde_json::{Value, json};
use tracedecay_contracts::retained_surfaces::{
    ProviderControlRequestV1, RetainedSurfaceOperation, RetainedSurfaceRequestV1,
    retained_surface_operation_is_effect,
};
use tracedecay_contracts::{CancellationSignal, Deadline, RequestId};
use tracedecay_daemon_protocol::{
    DaemonClientIdentity, DaemonConnection, DaemonHandshake, DaemonInvocationClient,
    DaemonInvocationError, DaemonInvocationExecutor, DaemonInvocationRequest,
    DaemonInvocationResponse, InvocationCancellationPolicy, MovedStoreAdoption,
};
use tracedecay_domain::UtcMicros;

/// A returned client result stays available even if its artifact cannot be saved.
/// The caller must treat `capture_error` as failed evidence capture, independently
/// of the operation's actual result. Neither field establishes provider contact.
pub(super) struct ControlledRpcEvidence {
    pub(super) result: Result<DaemonInvocationResponse, DaemonInvocationError>,
    pub(super) record: Value,
    pub(super) capture_error: Option<String>,
}

impl HostFixture<'_> {
    /// Invokes only the supplied typed request using this fixture's owned daemon.
    /// Reuse the same request ID and request body for an intentional same-request
    /// retry. This helper performs no retry, readiness query, or daemon startup.
    pub(super) fn controlled_provider_rpc(
        &mut self,
        request_id: RequestId,
        request: ProviderControlRequestV1,
        observed_at: UtcMicros,
        deadline: Deadline,
        cancellation: CancellationSignal,
    ) -> FixtureResult<ControlledRpcEvidence> {
        if self.project_id.is_empty() {
            return Err("controlled RPC requires the fixture's observed project identity".into());
        }
        let authority = self.authority()?;
        let policy = cancellation_policy(request.operation());
        let invocation = DaemonInvocationRequest::retained_application(
            request_id.as_str(),
            RetainedSurfaceRequestV1::ProviderControl(request),
            observed_at,
            deadline.clone(),
            cancellation.context(),
        );
        let connection = DaemonConnection::new(
            authority.endpoint.clone(),
            Some(authority.auth_token.clone()),
        )
        .with_daemon_version(authority.version.clone());
        let handshake = DaemonHandshake {
            project_path: Some(self.journey.project.clone()),
            scope_prefix: None,
            timings: false,
            allow_init: false,
            allow_initialize_root_routing: false,
            client_identity: DaemonClientIdentity::new(
                authority.profile_root.clone(),
                self.journey.profile.join("global.db"),
            ),
            client_version: env!("CARGO_PKG_VERSION").to_owned(),
            client_instance_id: format!("host-comparison-rpc-{}", std::process::id()),
            tool_list_changed_capable: false,
            catalog_version: String::new(),
            moved_store_adoption: MovedStoreAdoption::Never,
        };
        let client = DaemonInvocationClient::new(connection, handshake);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("create controlled RPC runtime: {error}"))?;

        // Retain every attempt separately, including intentional same-ID retries.
        // No change to the fixture's raw CLI command sequence is needed.
        let attempt = io_result(
            tempfile::Builder::new()
                .prefix("controlled-rpc-")
                .disable_cleanup(true)
                .tempdir_in(&self.archive),
            "allocate retained controlled RPC artifacts",
        )?;
        io_result(
            io_result(
                std::fs::File::open(&self.archive),
                "open controlled RPC artifact parent",
            )?
            .sync_all(),
            "fsync controlled RPC attempt directory entry",
        )?;
        let request_bytes = serde_json::to_vec(&invocation)
            .map_err(|error| format!("serialize typed controlled RPC request: {error}"))?;
        let request_artifact = write_durable(&attempt.path().join("request.json"), &request_bytes)?;

        let started = self.clock_ns();
        let result = runtime.block_on(DaemonInvocationExecutor::invoke_controlled(
            &client,
            invocation,
            deadline,
            cancellation.clone(),
            policy,
        ));
        let ended = self.clock_ns();
        let cancellation_at_client_return = cancellation.context();

        // After dispatch, a capture failure must not discard the actual typed
        // outcome or relabel an accepted mutation as an unexecuted request.
        let capture = decoded_client_result(&result)
            .and_then(|value| {
                serde_json::to_vec(&value)
                    .map_err(|error| format!("serialize decoded controlled RPC result: {error}"))
            })
            .and_then(|bytes| write_durable(&attempt.path().join("decoded-result.json"), &bytes));
        let (result_artifact, capture_error) = match capture {
            Ok(artifact) => (artifact, None),
            Err(error) => (Value::Null, Some(error)),
        };
        let record = json!({
            "evidence_kind": "controlled_rpc_decoded_evidence",
            "request_id": request_id.as_str(),
            "target": {
                "project_id": self.project_id,
                "project_path": self.journey.project,
                "profile_root": authority.profile_root,
                "daemon_pid": authority.pid,
                "daemon_process_run_id": authority.process_run_id,
                "daemon_version": authority.version,
            },
            "request": {"representation": "serialized_typed_rpc_request", "artifact": request_artifact},
            "result": {"representation": "serialized_typed_rpc_client_result", "artifact": result_artifact},
            "cancellation_policy": match policy {
                InvocationCancellationPolicy::ReadOnly => "read_only",
                InvocationCancellationPolicy::AuthoritativeEffect => "authoritative_effect",
            },
            "cancellation_at_client_return": cancellation_at_client_return,
            "capture_status": if capture_error.is_none() { "durable" } else { "failed" },
            "capture_error": capture_error,
            "timing": {
                "status": "measured", "clock": "fixture_process_monotonic_elapsed",
                "phase": "controlled_rpc_invocation",
                "start_monotonic_ns": started, "end_monotonic_ns": ended,
                "elapsed_ns": ended.saturating_sub(started),
            },
        });
        Ok(ControlledRpcEvidence {
            result,
            record,
            capture_error,
        })
    }
}

fn cancellation_policy(operation: RetainedSurfaceOperation) -> InvocationCancellationPolicy {
    if retained_surface_operation_is_effect(operation) {
        InvocationCancellationPolicy::AuthoritativeEffect
    } else {
        InvocationCancellationPolicy::ReadOnly
    }
}

fn decoded_client_result(
    result: &Result<DaemonInvocationResponse, DaemonInvocationError>,
) -> FixtureResult<Value> {
    match result {
        Ok(response) => {
            let response = serde_json::to_value(response)
                .map_err(|error| format!("encode decoded controlled RPC response: {error}"))?;
            Ok(json!({"kind": "decoded_rpc_client_response", "response": response}))
        }
        Err(error) => {
            // This enum has no Serialize implementation. Preserve each existing
            // variant and its fields instead of flattening it into a CLI status.
            let error = match error {
                DaemonInvocationError::Cancelled { stage } => {
                    json!({"kind": "cancelled", "stage": stage})
                }
                DaemonInvocationError::TimedOut { stage } => {
                    json!({"kind": "timed_out", "stage": stage})
                }
                DaemonInvocationError::Unavailable => json!({"kind": "unavailable"}),
                DaemonInvocationError::Unreachable {
                    reason_code,
                    detail,
                } => json!({"kind": "unreachable", "reason_code": reason_code, "detail": detail}),
            };
            Ok(json!({"kind": "rpc_client_error", "error": error}))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracedecay_contracts::CancellationStage;

    #[test]
    fn provider_mutations_keep_authoritative_settlement_policy() {
        for operation in [
            RetainedSurfaceOperation::ProviderFeedback,
            RetainedSurfaceOperation::ProviderCorrection,
            RetainedSurfaceOperation::ProviderDeleteBySource,
            RetainedSurfaceOperation::ProviderMaintenance,
            RetainedSurfaceOperation::ProviderSnapshotRestore,
            RetainedSurfaceOperation::ProviderReplay,
        ] {
            assert_eq!(
                cancellation_policy(operation),
                InvocationCancellationPolicy::AuthoritativeEffect,
            );
        }
        for operation in [
            RetainedSurfaceOperation::ProviderHealth,
            RetainedSurfaceOperation::ProviderInspection,
            RetainedSurfaceOperation::ProviderSnapshotExport,
        ] {
            assert_eq!(
                cancellation_policy(operation),
                InvocationCancellationPolicy::ReadOnly,
            );
        }
    }

    #[test]
    fn capture_keeps_transport_uncertainty_and_cancellation_stage_distinct() {
        let unavailable = decoded_client_result(&Err(DaemonInvocationError::Unavailable))
            .expect("encode unknown transport outcome");
        let unreachable = decoded_client_result(&Err(DaemonInvocationError::Unreachable {
            reason_code: "daemon_connect_down".into(),
            detail: "endpoint did not accept the connection".into(),
        }))
        .expect("encode connect failure");
        assert_eq!(unavailable["error"], json!({"kind": "unavailable"}));
        assert_eq!(
            unreachable["error"],
            json!({"kind": "unreachable", "reason_code": "daemon_connect_down",
                "detail": "endpoint did not accept the connection"}),
        );
        let cancelled = decoded_client_result(&Err(DaemonInvocationError::Cancelled {
            stage: CancellationStage::EffectInFlight,
        }))
        .expect("encode cancellation stage");
        assert_eq!(
            cancelled["error"],
            json!({"kind": "cancelled", "stage": "effect_in_flight"}),
        );
        assert!(cancelled.get("response").is_none());
    }
}
