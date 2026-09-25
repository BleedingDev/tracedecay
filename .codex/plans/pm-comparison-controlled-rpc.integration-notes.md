# Controlled provider RPC fixture helper — draft only

Only new production-path file in this draft:
`crates/tracedecay-cli/tests/product_memory_provider_claude_host_journey/comparison_fixture/controlled_rpc.rs`.

The draft is held in the separate `pm-comparison-controlled-rpc-proposed` mirror. The initial and corrected fixture mirrors and all live source files remain unchanged by this task. Root reviews before applying. The fixture owner owns the later `mod controlled_rpc;` hook in `comparison_fixture.rs`; no accessor or field changes are needed because this child can access ancestor-private items.

API:

```rust
HostFixture::controlled_provider_rpc(
    &mut self,
    request_id: tracedecay_contracts::RequestId,
    request: tracedecay_contracts::retained_surfaces::ProviderControlRequestV1,
    observed_at: tracedecay_domain::UtcMicros,
    deadline: tracedecay_contracts::Deadline,
    cancellation: tracedecay_contracts::CancellationSignal,
) -> FixtureResult<controlled_rpc::ControlledRpcEvidence>
```

`ControlledRpcEvidence` exposes the full typed `result: Result<DaemonInvocationResponse, DaemonInvocationError>`, an artifact `record: Value`, and `capture_error: Option<String>` to the parent module. An outer error means preparation failed before invocation. Once invoked, result-artifact serialization or persistence failure is returned in `capture_error` without discarding the actual typed client result. The caller must fail evidence capture separately from interpreting the operation result; it must not relabel a committed effect as an unexecuted action. `record.capture_status` describes the request/result artifacts; the parent retains this metadata through its existing channel.

Identity and controls:

- Reuses `HostFixture::authority()` to validate the currently owned daemon PID and isolated profile.
- Uses the existing fixture project path and previously observed project ID; does not query, initialize, adopt a moved store, or construct registered application authority.
- Builds the authenticated public `DaemonInvocationClient` from the actual authority endpoint/token and fixture profile/project handshake. Credentials are not included in evidence.
- Passes the supplied request ID to `DaemonInvocationRequest::retained_application` unchanged, and passes the original deadline and shared live cancellation signal to `DaemonInvocationExecutor::invoke_controlled`.
- Reuses public `retained_surface_operation_is_effect` for the same read/effect cancellation distinction as the retained HTTP adapter. The existing transport owns authoritative settlement after interruption.
- Performs no retries. Each call retains a distinct attempt directory even when the caller intentionally repeats the same request ID/body.

Evidence:

- `request.json`: serialized complete typed request envelope, durable before invocation.
- `decoded-result.json`: full serialization of the typed RPC client's returned response, or an exact variant/field projection of its non-Serialize transport error enum.
- Result is explicitly `decoded_rpc_client_response` or `rpc_client_error`. It is never represented as original socket bytes, daemon-origin bytes, or CLI stdout; the client can synthesize an indeterminate response after interrupted settlement.
- Full response serialization preserves actual operation IDs, duplicate effect identities, diagnostics, warnings, and operation-specific data without inference or normalization. Unavailable and unreachable client failures stay distinct.
- The record includes measured invocation duration and a cancellation snapshot taken immediately at client return. Neither the record nor the result invents provider-contact evidence.

This helper constructs no source/state selectors, revisions, receipt refs, or scheduled actions. It adds no action calls, corruption, recall/readiness queries, producer changes, or scenario changes.

Reuse/recon: graph duplicate search returned no indexed `invoke_controlled` match; scoped source search confirmed the existing public client/executor, authority validator, artifact writer, and cancellation-effect classifier above. There was no existing comparison fixture controlled-RPC wrapper. The new error projection is necessary because `DaemonInvocationError` lacks Serialize and its ApplicationProblem conversion combines transport distinctions.

Two offline unit tests are included: all nine provider operations retain the required read/effect settlement policy; captured transport uncertainty, exact connection diagnostic, and cancellation stage remain distinct. No test constructs source selectors or fake authority.

Validation: rustfmt --edition 2024 --check and git apply --check on the standalone new-file patch. No Cargo, compiler, tests, daemon requests, models, or held-out output inspection. No live apply, commit, or push.
