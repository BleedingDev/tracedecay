//! Conformance and adversarial evidence for the supervised Rust NCM backend.
#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::print_stderr,
    clippy::unwrap_used
)]

#[cfg(feature = "rust-backend")]
mod enabled {
    use std::collections::{BTreeMap, BTreeSet};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Barrier, OnceLock};
    use std::thread;
    use std::time::{Duration, Instant};

    use serde_json::{Value, json};
    use sha2::{Digest, Sha256};
    use tracedecay_memory_conformance::{
        AdversarialProviderInputsV1, AdversarialProviderV1, AdversarialScriptV1, ConformanceStatus,
        ExpectedCommittedEffect, HandshakeMisbehaviourV1, MisbehaviourV1, NoPayloadSourceV1,
        OptionalTextExpectation, PayloadExpectation, ProviderHarness, ScenarioFixture,
        mandatory_conformance_fixture,
    };
    use tracedecay_memory_ncm_runtime::client::{ClientError, WorkerClient};
    use tracedecay_memory_ncm_runtime::wire::{Operation, Request};
    use tracedecay_memory_provider_api::contract::{CommittedEffectState, TerminalCode};
    use tracedecay_memory_provider_api::{
        CancellationToken, CanonicalPayload, HandshakeRequest, HandshakeRequestParts,
        MemoryProvider, OperationControl, OwnedExactScope, OwnedProviderId, OwnedVersionedId,
        PayloadSanitizationReceipt, PayloadSanitizationReceiptParts, ProviderCall,
        ProviderCallParts, ProviderLimits, ProviderOperation, ProviderReply,
        observation_extensions_digest,
    };
    use tracedecay_memory_provider_ncm::{
        NCM_PROVIDER_ID, NcmCognitiveSurface, NcmNamespace, NcmProviderAdapter, NcmSurfaceCall,
        NcmSurfaceHandshakeRequest, NcmSurfaceHandshakeResponse, RustNcmConfig, RustNcmSurface,
        StateRoot, WorkerOptions,
    };

    const REGISTRATION_REVISION: u64 = 1;
    const RESOLVED_SCOPE_DIGEST: &str =
        "sha256:1111111111111111111111111111111111111111111111111111111111111111";
    const TEST_SANITIZER_REVISION: &str = "tracedecay.memory.observation.hygiene.v1+ncm-rs-019";
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    static WORKER_BINARY: OnceLock<PathBuf> = OnceLock::new();

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new(label: &str) -> Self {
            let serial = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let scratch = PathBuf::from(
                std::env::var_os("TEST_TMPDIR").expect("Bazel writable test directory"),
            );
            let path = scratch.join("test-profile").join(format!(
                "ncm-rs-019-{label}-{}-{serial}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("create conformance state root");
            Self(path)
        }

        fn state_root(&self) -> StateRoot {
            StateRoot::new(self.0.clone()).expect("absolute test state root")
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn worker_binary() -> PathBuf {
        WORKER_BINARY
            .get_or_init(|| {
                let path = std::env::var_os("TRACEDECAY_NCM_WORKER")
                    .or_else(|| std::env::var_os("CARGO_BIN_EXE_tracedecay-ncm-worker"))
                    .map(PathBuf::from)
                    .expect("supply an absolute TRACEDECAY_NCM_WORKER or Bazel worker executable");
                assert!(path.is_absolute(), "NCM worker executable must be absolute");
                assert!(path.is_file(), "NCM worker executable must exist");
                path
            })
            .clone()
    }

    fn surface(root: &TestRoot) -> Arc<RustNcmSurface> {
        Arc::new(
            RustNcmSurface::new(RustNcmConfig {
                worker_binary: worker_binary(),
                state_root: root.state_root(),
                worker_options: WorkerOptions {
                    test_double: true,
                    reconciliation_deadline: Duration::from_secs(3),
                    ..WorkerOptions::default()
                },
            })
            .expect("construct Rust NCM surface"),
        )
    }

    fn adapter(root: &TestRoot) -> Arc<NcmProviderAdapter> {
        Arc::new(NcmProviderAdapter::new(surface(root)).expect("construct NCM adapter"))
    }

    struct CancelAfterCommitSurface {
        inner: Arc<RustNcmSurface>,
        cancel_next_observe: AtomicBool,
    }

    impl NcmCognitiveSurface for CancelAfterCommitSurface {
        fn descriptor(&self) -> tracedecay_memory_provider_api::ProviderDescriptor {
            self.inner.descriptor()
        }

        fn handshake(&self, request: &NcmSurfaceHandshakeRequest) -> NcmSurfaceHandshakeResponse {
            self.inner.handshake(request)
        }

        fn invoke(&self, call: &NcmSurfaceCall) -> ProviderReply {
            let reply = self.inner.invoke(call);
            if call.operation == ProviderOperation::Observe
                && self.cancel_next_observe.swap(false, Ordering::SeqCst)
            {
                assert_eq!(reply.terminal.terminal_code(), TerminalCode::Success);
                assert_eq!(
                    reply.terminal.committed_effect().state(),
                    CommittedEffectState::Committed
                );
                call.control.cancellation().cancel();
            }
            reply
        }
    }

    fn cancel_after_commit_adapter(root: &TestRoot) -> Arc<NcmProviderAdapter> {
        let surface = Arc::new(CancelAfterCommitSurface {
            inner: surface(root),
            cancel_next_observe: AtomicBool::new(true),
        });
        Arc::new(NcmProviderAdapter::new(surface).expect("construct NCM adapter"))
    }

    fn scope_parts(
        profile: &str,
        project: &str,
        worktree: &str,
        branch: &str,
        session: &str,
    ) -> OwnedExactScope {
        OwnedExactScope::new(
            profile,
            project,
            "repository-rust-backend",
            worktree,
            branch,
            session,
            RESOLVED_SCOPE_DIGEST,
        )
        .expect("valid exact scope")
    }

    fn scope(project: &str) -> OwnedExactScope {
        scope_parts(
            "profile-rust-backend",
            project,
            "worktree-rust-backend",
            "refs/heads/feat/ncm-biomem-rust-v1",
            "session-rust-backend",
        )
    }

    fn limits() -> ProviderLimits {
        ProviderLimits {
            request_bytes: 256 * 1024,
            response_bytes: 1024 * 1024,
            observation_batch_items: 16,
            recall_candidates: 16,
            concurrent_operations: 1,
            operation_millis: 30_000,
            snapshot_bytes: 256 * 1024 * 1024,
            inspection_items: 1_000,
        }
    }

    fn supported_operations() -> [ProviderOperation; 10] {
        [
            ProviderOperation::Health,
            ProviderOperation::Observe,
            ProviderOperation::Recall,
            ProviderOperation::Feedback,
            ProviderOperation::Maintenance,
            ProviderOperation::Inspection,
            ProviderOperation::Correction,
            ProviderOperation::DeleteBySource,
            ProviderOperation::SnapshotExport,
            ProviderOperation::SnapshotRestore,
        ]
    }

    fn all_capabilities() -> Vec<OwnedVersionedId> {
        supported_operations()
            .into_iter()
            .map(|operation| {
                OwnedVersionedId::new(operation.capability_id()).expect("known capability")
            })
            .collect()
    }

    fn handshake_once(
        provider: &dyn MemoryProvider,
        exact_scope: &OwnedExactScope,
    ) -> tracedecay_memory_provider_api::HandshakeResponse {
        provider.handshake(
            &HandshakeRequest::new(HandshakeRequestParts {
                provider_id: OwnedProviderId::new(NCM_PROVIDER_ID).expect("provider id"),
                registration_revision: REGISTRATION_REVISION,
                exact_scope: exact_scope.clone(),
                request_id: format!("handshake-{}", NEXT_ID.fetch_add(1, Ordering::Relaxed)),
                required_capabilities: all_capabilities(),
                host_limits: limits(),
                control: OperationControl::new(i64::MAX, 10_000, CancellationToken::new()),
                challenge_nonce: [0x19; 32],
            })
            .expect("valid handshake"),
        )
    }

    fn ready_parts(provider: &dyn MemoryProvider, exact_scope: &OwnedExactScope) -> (String, u64) {
        for _ in 0..3 {
            let response = handshake_once(provider, exact_scope);
            if response.terminal.terminal_code() == TerminalCode::Success {
                return (
                    response.ready_receipt_sha256.expect("ready receipt"),
                    response
                        .descriptor
                        .expect("ready descriptor")
                        .state_generation,
                );
            }
            assert_eq!(
                response.terminal.terminal_code(),
                TerminalCode::StaleIdentity,
                "unexpected handshake failure: {:?}",
                response.terminal.diagnostic_id()
            );
        }
        panic!("readiness did not stabilize")
    }

    fn operation_contract(operation: ProviderOperation) -> &'static str {
        match operation {
            ProviderOperation::Handshake => "tracedecay.memory.provider.handshake.v1",
            ProviderOperation::Health => "tracedecay.memory.provider.health.v1",
            ProviderOperation::Observe => "tracedecay.memory.provider.observation.v1",
            ProviderOperation::Recall => "tracedecay.memory.provider.recall.v1",
            ProviderOperation::Feedback => "tracedecay.memory.provider.feedback.v1",
            ProviderOperation::Maintenance => "tracedecay.memory.provider.maintenance.v1",
            ProviderOperation::Inspection => "tracedecay.memory.provider.inspection.v1",
            ProviderOperation::Correction => "tracedecay.memory.provider.correction.v1",
            ProviderOperation::DeleteBySource => "tracedecay.memory.provider.deletion-by-source.v1",
            ProviderOperation::SnapshotExport => "tracedecay.memory.provider.snapshot-export.v1",
            ProviderOperation::SnapshotRestore => "tracedecay.memory.provider.snapshot-restore.v1",
            ProviderOperation::Replay => "tracedecay.memory.provider.replay.v1",
        }
    }

    fn canonical_payload(contract: &str, value: Value) -> CanonicalPayload {
        let bytes = serde_json::to_vec(&value).expect("serialize payload");
        CanonicalPayload::new(
            OwnedVersionedId::new(contract).expect("payload contract"),
            bytes.clone(),
            sha256_hex(&bytes),
        )
        .expect("canonical payload")
    }

    #[allow(clippy::too_many_arguments)]
    fn call_with(
        operation: ProviderOperation,
        exact_scope: &OwnedExactScope,
        receipt: &str,
        generation: u64,
        idempotency_key: Option<&str>,
        value: Value,
        contract: &str,
        token: CancellationToken,
        remaining_millis: u64,
    ) -> ProviderCall {
        let call = ProviderCall::new(ProviderCallParts {
            operation,
            provider_id: OwnedProviderId::new(NCM_PROVIDER_ID).expect("provider id"),
            registration_revision: REGISTRATION_REVISION,
            ready_receipt_sha256: receipt.to_owned(),
            exact_scope: exact_scope.clone(),
            request_id: format!("request-{}", NEXT_ID.fetch_add(1, Ordering::Relaxed)),
            operation_id: format!(
                "operation-{}-{}",
                operation.as_wire(),
                NEXT_ID.fetch_add(1, Ordering::Relaxed)
            ),
            expected_state_generation: generation,
            idempotency_key: idempotency_key.map(str::to_owned),
            control: OperationControl::new(i64::MAX, remaining_millis, token),
            payload: canonical_payload(contract, value),
            required_capabilities: vec![
                OwnedVersionedId::new(operation.capability_id()).expect("operation capability"),
            ],
            extensions: Vec::new(),
        })
        .expect("valid provider call");
        if operation == ProviderOperation::Observe {
            admit_observation(call)
        } else {
            call
        }
    }

    fn call(
        operation: ProviderOperation,
        exact_scope: &OwnedExactScope,
        receipt: &str,
        generation: u64,
        idempotency_key: Option<&str>,
        value: Value,
    ) -> ProviderCall {
        call_with(
            operation,
            exact_scope,
            receipt,
            generation,
            idempotency_key,
            value,
            operation_contract(operation),
            CancellationToken::new(),
            10_000,
        )
    }

    fn scope_value(exact_scope: &OwnedExactScope) -> Value {
        json!({
            "profile_id": exact_scope.profile_id,
            "project_id": exact_scope.project_id,
            "repository_identity": exact_scope.repository_identity,
            "worktree_identity": exact_scope.worktree_identity,
            "branch_identity": exact_scope.branch_identity,
            "agent_session_id": exact_scope.agent_session_id,
            "resolved_scope_digest": exact_scope.resolved_scope_digest,
        })
    }

    fn common_maintenance_call(
        exact_scope: &OwnedExactScope,
        receipt: &str,
        generation: u64,
        idempotency_key: &str,
        task: &str,
        maximum_items: u64,
        resume_cursor: Option<&str>,
    ) -> ProviderCall {
        let mut call = call(
            ProviderOperation::Maintenance,
            exact_scope,
            receipt,
            generation,
            Some(idempotency_key),
            json!({}),
        );
        let common_request = json!({
            "provider_id": NCM_PROVIDER_ID,
            "registration_revision": REGISTRATION_REVISION,
            "ready_receipt_digest": receipt,
            "exact_scope_identity": scope_value(exact_scope),
            "operation_id": call.operation_id,
            "idempotency_key": idempotency_key,
            "expected_state_generation": generation,
            "request_identity": call.request_id,
            "policy_revision": 1,
            "deadline": {
                "deadline_utc_micros": i64::MAX,
                "remaining_millis": 10_000,
            },
            "cancellation": "live",
            "extensions": [],
        });
        call.payload = canonical_payload(
            operation_contract(ProviderOperation::Maintenance),
            json!({
                "common_request": common_request,
                "task": task,
                "maximum_items": maximum_items,
                "maximum_bytes": 1_048_576,
                "maximum_duration_millis": 60_000,
                "dry_run": false,
                "resume_cursor": resume_cursor,
            }),
        );
        call
    }

    fn invoke_common_maintenance(
        provider: &dyn MemoryProvider,
        exact_scope: &OwnedExactScope,
        idempotency_key: &str,
        task: &str,
        maximum_items: u64,
        resume_cursor: Option<&str>,
    ) -> ProviderReply {
        let (receipt, generation) = ready_parts(provider, exact_scope);
        provider.invoke(&common_maintenance_call(
            exact_scope,
            &receipt,
            generation,
            idempotency_key,
            task,
            maximum_items,
            resume_cursor,
        ))
    }

    fn admit_observation(call: ProviderCall) -> ProviderCall {
        let extensions_digest =
            observation_extensions_digest(&call.extensions).expect("extension digest");
        let receipt = PayloadSanitizationReceipt::new(
            PayloadSanitizationReceiptParts::accepted_unmodified_with_extensions(
                TEST_SANITIZER_REVISION,
                call.payload.sha256.clone(),
                extensions_digest,
            ),
        )
        .expect("sanitization receipt");
        call.with_sanitization(receipt)
    }

    fn observe_value(source: &str, key: &str, value: &str) -> Value {
        json!({
            "observation_kind": "tool.execution_settled.v1",
            "payload_contract": "tracedecay.memory.observation.tool-execution.v1",
            "canonical_payload": {
                "forget_source_key": source,
                "command": key,
                "outcome_summary": value
            }
        })
    }

    fn current_observe_value(
        exact_scope: &OwnedExactScope,
        session: &str,
        stable_record: &str,
        text: &str,
    ) -> Value {
        let observation_id = format!(
            "sha256:{}",
            sha256_hex(
                &serde_json::to_vec(&json!([
                    exact_scope.project_id,
                    "claude",
                    session,
                    stable_record
                ]))
                .expect("canonical observation identity serializes")
            )
        );
        json!({
            "observation_kind": "session.message_committed.v1",
            "payload_contract": "tracedecay.memory.observation.session-message.v1",
            "source_identity": {"canonical_observation_id": observation_id},
            "canonical_payload": {
                "version": 1,
                "provider": "claude",
                "native_record_kind": "message",
                "stable_record_id": stable_record,
                "relations": {"session_id": session, "project_id": exact_scope.project_id},
                "facts": [{"kind": "message", "role": "assistant", "content": text}]
            }
        })
    }

    fn payload_for(operation: ProviderOperation) -> Value {
        match operation {
            ProviderOperation::Health
            | ProviderOperation::Inspection
            | ProviderOperation::SnapshotExport => json!({}),
            ProviderOperation::Observe => {
                observe_value("source-matrix", "matrix key", "matrix value")
            }
            ProviderOperation::Recall => json!({"query_text": "matrix key", "top_k": 5}),
            ProviderOperation::Feedback => json!({"record_ids": [1]}),
            ProviderOperation::Maintenance => json!({"kind": "checkpoint"}),
            ProviderOperation::Correction => json!({
                "superseded_record_id": 1,
                "superseding_record_id": 2,
                "evidence_sha256": sha256_hex(b"matrix evidence")
            }),
            ProviderOperation::DeleteBySource => json!({"source_id": "source-matrix"}),
            ProviderOperation::SnapshotRestore => json!({"snapshot": [1]}),
            ProviderOperation::Handshake | ProviderOperation::Replay => json!({}),
        }
    }

    fn invoke_ready(
        provider: &dyn MemoryProvider,
        exact_scope: &OwnedExactScope,
        operation: ProviderOperation,
        idempotency_key: Option<&str>,
        value: Value,
    ) -> ProviderReply {
        let (receipt, generation) = ready_parts(provider, exact_scope);
        provider.invoke(&call(
            operation,
            exact_scope,
            &receipt,
            generation,
            idempotency_key,
            value,
        ))
    }

    fn response_json(reply: &ProviderReply) -> Value {
        serde_json::from_slice(&reply.payload.as_ref().expect("response payload").bytes)
            .expect("response JSON")
    }

    fn find_string(value: &Value, field: &str, expected: &str) -> bool {
        match value {
            Value::Object(object) => {
                object.get(field).and_then(Value::as_str) == Some(expected)
                    || object
                        .values()
                        .any(|child| find_string(child, field, expected))
            }
            Value::Array(items) => items
                .iter()
                .any(|child| find_string(child, field, expected)),
            _ => false,
        }
    }

    fn find_u64(value: &Value, field: &str) -> Option<u64> {
        match value {
            Value::Object(object) => object
                .get(field)
                .and_then(Value::as_u64)
                .or_else(|| object.values().find_map(|child| find_u64(child, field))),
            Value::Array(items) => items.iter().find_map(|child| find_u64(child, field)),
            _ => None,
        }
    }

    fn snapshot_bytes(reply: &ProviderReply) -> Vec<Value> {
        response_json(reply)
            .get("bytes")
            .and_then(Value::as_array)
            .expect("snapshot bytes")
            .clone()
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        let digest = Sha256::digest(bytes);
        let mut output = String::with_capacity(64);
        for byte in digest {
            use std::fmt::Write as _;
            write!(&mut output, "{byte:02x}").expect("write digest");
        }
        output
    }

    fn mandatory_operation_fixture(
        provider: &dyn MemoryProvider,
        exact_scope: &OwnedExactScope,
        step_id: &str,
    ) -> ScenarioFixture {
        let harness = ProviderHarness::new(provider).expect("bind conformance harness");
        let source = mandatory_conformance_fixture(
            harness.fixture_identity(),
            exact_scope.clone(),
            REGISTRATION_REVISION,
        )
        .expect("mandatory conformance definition");
        let mut operation = source
            .operations()
            .iter()
            .find(|operation| operation.step_id == step_id)
            .expect("mandatory step")
            .clone();
        operation.expectation.payload = if matches!(
            operation.step_id.as_str(),
            "mandatory.cancelled_recall" | "mandatory.expired_recall"
        ) {
            PayloadExpectation::Absent
        } else {
            PayloadExpectation::Present
        };
        match operation.step_id.as_str() {
            "mandatory.health" => {
                operation.payload =
                    canonical_payload(operation_contract(operation.operation), json!({}))
            }
            "mandatory.observe" | "mandatory.observe_duplicate" => {
                operation.payload = canonical_payload(
                    operation_contract(operation.operation),
                    observe_value("source-mandatory", "mandatory-memory", "mandatory-value"),
                );
                operation.expectation.committed_effect = if operation.step_id.ends_with("duplicate")
                {
                    ExpectedCommittedEffect::duplicate(
                        OptionalTextExpectation::Exact("mandatory-observation-key".to_owned()),
                        OptionalTextExpectation::Present,
                    )
                } else {
                    ExpectedCommittedEffect::committed()
                };
            }
            "mandatory.recall" => {
                operation.payload = canonical_payload(
                    operation_contract(operation.operation),
                    json!({"query_text": "mandatory-memory", "top_k": 16}),
                );
            }
            _ => {}
        }
        ScenarioFixture::new(
            format!("ncm-rs-019.{}", operation.step_id),
            source.identity().clone(),
            source.exact_scope().clone(),
            source.registration_revision(),
            source.handshake().clone(),
            vec![operation],
        )
        .expect("split mandatory fixture")
    }

    #[test]
    fn mandatory_conformance_definition_runs_on_real_adapter() {
        let root = TestRoot::new("mandatory");
        let provider = adapter(&root);
        let exact_scope = scope("project-mandatory");
        for step_id in [
            "mandatory.health",
            "mandatory.observe",
            "mandatory.observe_duplicate",
            "mandatory.recall",
            "mandatory.cancelled_recall",
            "mandatory.expired_recall",
        ] {
            let fixture = mandatory_operation_fixture(provider.as_ref(), &exact_scope, step_id);
            let harness = ProviderHarness::new(provider.as_ref()).expect("bind harness");
            let report = harness
                .run_product(&fixture)
                .expect("run mandatory fixture");
            assert_eq!(
                report.summary().status,
                ConformanceStatus::Pass,
                "{step_id}: {:?}",
                report.steps()
            );
            assert_eq!(report.summary().failed_steps, 0);
            assert_eq!(report.summary().not_run_steps, 0);
        }
    }

    #[test]
    fn every_supported_operation_rejects_all_wrong_scope_dimensions() {
        let root = TestRoot::new("scope-matrix");
        let provider = adapter(&root);
        let accepted = scope("project-scope-matrix");
        let (receipt, generation) = ready_parts(provider.as_ref(), &accepted);
        let variants = [
            scope_parts(
                "wrong-profile",
                "project-scope-matrix",
                "worktree-rust-backend",
                "refs/heads/feat/ncm-biomem-rust-v1",
                "session-rust-backend",
            ),
            scope_parts(
                "profile-rust-backend",
                "wrong-project",
                "worktree-rust-backend",
                "refs/heads/feat/ncm-biomem-rust-v1",
                "session-rust-backend",
            ),
            scope_parts(
                "profile-rust-backend",
                "project-scope-matrix",
                "wrong-worktree",
                "refs/heads/feat/ncm-biomem-rust-v1",
                "session-rust-backend",
            ),
            scope_parts(
                "profile-rust-backend",
                "project-scope-matrix",
                "worktree-rust-backend",
                "refs/heads/wrong-branch",
                "session-rust-backend",
            ),
            scope_parts(
                "profile-rust-backend",
                "project-scope-matrix",
                "worktree-rust-backend",
                "refs/heads/feat/ncm-biomem-rust-v1",
                "wrong-session",
            ),
        ];
        let mut tested = BTreeSet::new();
        for operation in supported_operations() {
            for (variant_index, wrong_scope) in variants.iter().enumerate() {
                let reply = provider.invoke(&call(
                    operation,
                    wrong_scope,
                    &receipt,
                    generation,
                    operation
                        .mutates_provider_state()
                        .then_some("scope-matrix-key"),
                    payload_for(operation),
                ));
                assert_eq!(reply.terminal.terminal_code(), TerminalCode::StaleIdentity);
                assert!(reply.payload.is_none());
                tested.insert((operation.as_wire(), variant_index));
            }
        }
        assert_eq!(tested.len(), 50);
    }

    #[test]
    fn stale_generation_replay_conflict_and_contradictory_contracts_are_fail_closed() {
        let root = TestRoot::new("generation-replay");
        let provider = adapter(&root);
        let exact_scope = scope("project-generation-replay");
        let (receipt, generation) = ready_parts(provider.as_ref(), &exact_scope);
        let stale = provider.invoke(&call(
            ProviderOperation::Health,
            &exact_scope,
            &receipt,
            generation.saturating_add(1),
            None,
            json!({}),
        ));
        assert_eq!(stale.terminal.terminal_code(), TerminalCode::StaleIdentity);

        for operation in supported_operations() {
            let bad = provider.invoke(&call_with(
                operation,
                &exact_scope,
                &receipt,
                generation,
                operation.mutates_provider_state().then_some("bad-contract"),
                payload_for(operation),
                "tracedecay.memory.provider.terminal.v1",
                CancellationToken::new(),
                10_000,
            ));
            assert_eq!(
                bad.terminal.terminal_code(),
                TerminalCode::InvalidRequest,
                "{}",
                operation.as_wire()
            );
        }

        let observation = observe_value("source-replay", "replay key", "replay value");
        let first = invoke_ready(
            provider.as_ref(),
            &exact_scope,
            ProviderOperation::Observe,
            Some("replay-key"),
            observation.clone(),
        );
        assert_eq!(first.terminal.terminal_code(), TerminalCode::Success);
        let generation_after = first.state_generation;
        let duplicate = invoke_ready(
            provider.as_ref(),
            &exact_scope,
            ProviderOperation::Observe,
            Some("replay-key"),
            observation,
        );
        assert_eq!(duplicate.terminal.terminal_code(), TerminalCode::Success);
        assert_eq!(
            duplicate.terminal.committed_effect().state(),
            CommittedEffectState::Duplicate
        );
        assert_eq!(duplicate.state_generation, generation_after);
        let conflict = invoke_ready(
            provider.as_ref(),
            &exact_scope,
            ProviderOperation::Observe,
            Some("replay-key"),
            observe_value("source-replay", "replay key", "contradictory value"),
        );
        assert_eq!(conflict.terminal.terminal_code(), TerminalCode::Conflict);
        assert_eq!(conflict.state_generation, generation_after);
        let inspection = invoke_ready(
            provider.as_ref(),
            &exact_scope,
            ProviderOperation::Inspection,
            None,
            json!({}),
        );
        let inspection = response_json(&inspection);
        assert_eq!(find_u64(&inspection, "records"), Some(1));
        assert_eq!(find_u64(&inspection, "tick"), Some(1));
    }

    #[test]
    fn common_maintenance_pages_preserve_cursor_until_final_commit() {
        let root = TestRoot::new("maintenance-pages");
        let provider = adapter(&root);
        let exact_scope = scope("project-maintenance-pages");
        for number in 1..=3 {
            let observed = invoke_ready(
                provider.as_ref(),
                &exact_scope,
                ProviderOperation::Observe,
                Some(&format!("maintenance-observe-{number}")),
                current_observe_value(
                    &exact_scope,
                    "source-maintenance-pages",
                    &format!("maintenance-message-{number}"),
                    &format!("maintenance key {number}: maintenance value"),
                ),
            );
            assert_eq!(observed.terminal.terminal_code(), TerminalCode::Success);
        }

        let (receipt, generation) = ready_parts(provider.as_ref(), &exact_scope);
        let direct = provider.invoke(&call(
            ProviderOperation::Maintenance,
            &exact_scope,
            &receipt,
            generation,
            Some("direct-maintenance-bypass"),
            json!({"kind": "checkpoint"}),
        ));
        assert_eq!(
            direct.terminal.terminal_code(),
            TerminalCode::InvalidRequest
        );
        assert_eq!(
            direct.terminal.diagnostic_id(),
            Some("ncm.rust.maintenance_requires_common_control")
        );
        assert!(direct.payload.is_none());

        let mut cursor = None;
        let mut partial_pages = 0;
        loop {
            let page_key = format!("paged-maintenance-{partial_pages}");
            let reply = invoke_common_maintenance(
                provider.as_ref(),
                &exact_scope,
                &page_key,
                "repair",
                1,
                cursor.as_deref(),
            );
            match reply.terminal.terminal_code() {
                TerminalCode::Partial => {
                    assert_eq!(
                        reply.terminal.committed_effect().state(),
                        CommittedEffectState::None
                    );
                    let payload = response_json(&reply);
                    assert_eq!(payload["partial"], true);
                    assert!(payload.get("no_change").is_none());
                    let next = payload["resume_cursor"]
                        .as_str()
                        .expect("maintenance cursor")
                        .to_owned();
                    assert_ne!(cursor.as_deref(), Some(next.as_str()));
                    if cursor.is_none() {
                        let reused_identity = invoke_common_maintenance(
                            provider.as_ref(),
                            &exact_scope,
                            &page_key,
                            "repair",
                            1,
                            Some(&next),
                        );
                        assert_eq!(
                            reused_identity.terminal.terminal_code(),
                            TerminalCode::Conflict
                        );
                        assert_eq!(
                            reused_identity.terminal.committed_effect().state(),
                            CommittedEffectState::None
                        );
                        assert_eq!(reused_identity.state_generation, generation);
                    }
                    cursor = Some(next);
                    partial_pages += 1;
                    assert!(partial_pages <= 32, "maintenance did not converge");
                }
                TerminalCode::Success => {
                    assert_eq!(
                        reply.terminal.committed_effect().state(),
                        CommittedEffectState::Committed
                    );
                    let payload = response_json(&reply);
                    assert_eq!(payload["partial"], false);
                    assert!(payload["resume_cursor"].is_null());
                    assert_eq!(payload["state_generation_before"], generation);
                    assert_eq!(payload["state_generation_after"], generation + 1);
                    break;
                }
                code => panic!("unexpected maintenance terminal code: {code:?}"),
            }
        }
        assert!(
            partial_pages >= 2,
            "fixture must exercise multiple maintenance pages"
        );
    }

    #[test]
    fn cancelled_calls_never_return_success_and_committed_observe_reconciles_once() {
        let root = TestRoot::new("cancel");
        let provider = cancel_after_commit_adapter(&root);
        let exact_scope = scope("project-cancel");
        let (receipt, generation) = ready_parts(provider.as_ref(), &exact_scope);
        for operation in supported_operations() {
            let token = CancellationToken::new();
            token.cancel();
            let reply = provider.invoke(&call_with(
                operation,
                &exact_scope,
                &receipt,
                generation,
                operation
                    .mutates_provider_state()
                    .then_some("cancelled-key"),
                payload_for(operation),
                operation_contract(operation),
                token,
                10_000,
            ));
            assert_eq!(reply.terminal.terminal_code(), TerminalCode::Cancelled);
        }

        let token = CancellationToken::new();
        let value = observe_value("source-cancel", "cancel key", "cancel value");
        let (receipt, generation) = ready_parts(provider.as_ref(), &exact_scope);
        let unknown = provider.invoke(&call_with(
            ProviderOperation::Observe,
            &exact_scope,
            &receipt,
            generation,
            Some("cancel-after-commit"),
            value.clone(),
            operation_contract(ProviderOperation::Observe),
            token,
            10_000,
        ));
        assert_eq!(
            unknown.terminal.terminal_code(),
            TerminalCode::EffectUnknown
        );
        assert_eq!(
            unknown.terminal.committed_effect().state(),
            CommittedEffectState::Unknown
        );
        let replay = invoke_ready(
            provider.as_ref(),
            &exact_scope,
            ProviderOperation::Observe,
            Some("cancel-after-commit"),
            value,
        );
        assert_eq!(replay.terminal.terminal_code(), TerminalCode::Success);
        assert_eq!(
            replay.terminal.committed_effect().state(),
            CommittedEffectState::Duplicate
        );
    }

    #[test]
    fn corrupted_namespace_file_fails_closed_after_process_restart() {
        let root = TestRoot::new("corrupt");
        let exact_scope = scope("project-corrupt");
        {
            let provider = adapter(&root);
            let observed = invoke_ready(
                provider.as_ref(),
                &exact_scope,
                ProviderOperation::Observe,
                Some("corrupt-observe"),
                observe_value("source-corrupt", "corrupt key", "corrupt value"),
            );
            assert_eq!(observed.terminal.terminal_code(), TerminalCode::Success);
        }
        let namespace = NcmNamespace::from_exact_scope(&exact_scope);
        let store = root
            .state_root()
            .namespace_dir(namespace.as_str())
            .expect("namespace directory")
            .join("ncm.sqlite");
        fs::write(&store, b"not a sqlite database").expect("corrupt namespace store");
        let provider = adapter(&root);
        let response = handshake_once(provider.as_ref(), &exact_scope);
        assert_eq!(
            response.terminal.terminal_code(),
            TerminalCode::ResetRequired
        );
        assert!(response.ready_receipt_sha256.is_none());
    }

    #[test]
    fn all_supported_operations_and_stale_snapshot_deletion_are_non_resurrecting() {
        let root = TestRoot::new("operations-privacy");
        let provider = adapter(&root);
        let exact_scope = scope("project-operations-privacy");
        let health = invoke_ready(
            provider.as_ref(),
            &exact_scope,
            ProviderOperation::Health,
            None,
            json!({}),
        );
        assert_eq!(health.terminal.terminal_code(), TerminalCode::Success);
        let deleted_text = "shared merged key: deleted outcome";
        let kept_text = "shared merged key: kept outcome";
        let first = invoke_ready(
            provider.as_ref(),
            &exact_scope,
            ProviderOperation::Observe,
            Some("privacy-observe-one"),
            current_observe_value(
                &exact_scope,
                "source-delete",
                "privacy-message-one",
                deleted_text,
            ),
        );
        let first_id = find_u64(&response_json(&first), "record_id").expect("first record id");
        let second = invoke_ready(
            provider.as_ref(),
            &exact_scope,
            ProviderOperation::Observe,
            Some("privacy-observe-two"),
            current_observe_value(
                &exact_scope,
                "source-keep",
                "privacy-message-two",
                kept_text,
            ),
        );
        let second_id = find_u64(&response_json(&second), "record_id").expect("second record id");
        let feedback = invoke_ready(
            provider.as_ref(),
            &exact_scope,
            ProviderOperation::Feedback,
            Some("privacy-feedback"),
            json!({"record_ids": [first_id, second_id]}),
        );
        assert_eq!(feedback.terminal.terminal_code(), TerminalCode::Success);
        let correction = invoke_ready(
            provider.as_ref(),
            &exact_scope,
            ProviderOperation::Correction,
            Some("privacy-correction"),
            json!({
                "superseded_record_id": first_id,
                "superseding_record_id": second_id,
                "evidence_sha256": sha256_hex(b"privacy correction")
            }),
        );
        assert_eq!(correction.terminal.terminal_code(), TerminalCode::Success);
        for (key, kind) in [
            ("privacy-consolidate", "consolidate"),
            ("privacy-merge", "prune_expired"),
            ("privacy-checkpoint", "repair"),
        ] {
            let reply =
                invoke_common_maintenance(provider.as_ref(), &exact_scope, key, kind, 100, None);
            assert_eq!(
                reply.terminal.terminal_code(),
                TerminalCode::Success,
                "maintenance {kind}"
            );
        }
        let inspection = invoke_ready(
            provider.as_ref(),
            &exact_scope,
            ProviderOperation::Inspection,
            None,
            json!({}),
        );
        assert_eq!(inspection.terminal.terminal_code(), TerminalCode::Success);
        let exported = invoke_ready(
            provider.as_ref(),
            &exact_scope,
            ProviderOperation::SnapshotExport,
            None,
            json!({}),
        );
        assert_eq!(exported.terminal.terminal_code(), TerminalCode::Success);
        let snapshot = snapshot_bytes(&exported);
        let deleted = invoke_ready(
            provider.as_ref(),
            &exact_scope,
            ProviderOperation::DeleteBySource,
            Some("privacy-delete"),
            json!({"source_id": exact_scope.session_forget_source_key("source-delete")}),
        );
        assert_eq!(deleted.terminal.terminal_code(), TerminalCode::Success);
        let before = invoke_ready(
            provider.as_ref(),
            &exact_scope,
            ProviderOperation::Inspection,
            None,
            json!({}),
        );
        assert_eq!(before.terminal.terminal_code(), TerminalCode::Success);
        let before_snapshot = invoke_ready(
            provider.as_ref(),
            &exact_scope,
            ProviderOperation::SnapshotExport,
            None,
            json!({}),
        );
        assert_eq!(
            before_snapshot.terminal.terminal_code(),
            TerminalCode::Success
        );
        let before_snapshot_bytes = snapshot_bytes(&before_snapshot);
        let assert_surviving_records = |provider: &dyn MemoryProvider| {
            let recall = invoke_ready(
                provider,
                &exact_scope,
                ProviderOperation::Recall,
                None,
                json!({"query_text": kept_text, "top_k": 16}),
            );
            assert_eq!(recall.terminal.terminal_code(), TerminalCode::Success);
            assert_eq!(recall.state_generation, before.state_generation);
            let value = response_json(&recall);
            assert!(!find_string(
                &value,
                "value_text",
                &format!("assistant: {deleted_text}")
            ));
            assert!(find_string(
                &value,
                "value_text",
                &format!("assistant: {kept_text}")
            ));
            let deleted_recall = invoke_ready(
                provider,
                &exact_scope,
                ProviderOperation::Recall,
                None,
                json!({"query_text": deleted_text, "top_k": 16}),
            );
            assert!(matches!(
                deleted_recall.terminal.terminal_code(),
                TerminalCode::Success | TerminalCode::SuccessZeroResults
            ));
            assert_eq!(deleted_recall.state_generation, before.state_generation);
            if deleted_recall.terminal.terminal_code() == TerminalCode::Success {
                assert!(!find_string(
                    &response_json(&deleted_recall),
                    "value_text",
                    &format!("assistant: {deleted_text}")
                ));
            }
        };
        assert_surviving_records(provider.as_ref());
        for bytes in [snapshot.clone(), snapshot] {
            let restored = invoke_ready(
                provider.as_ref(),
                &exact_scope,
                ProviderOperation::SnapshotRestore,
                Some("privacy-restore-stale"),
                json!({"snapshot": bytes}),
            );
            assert_eq!(
                restored.terminal.terminal_code(),
                TerminalCode::ProviderUnavailable
            );
            assert_eq!(
                restored.terminal.diagnostic_id(),
                Some("ncm.current_advisory_admission_refused")
            );
            assert_eq!(
                restored.terminal.committed_effect().state(),
                CommittedEffectState::None
            );
            assert!(
                restored
                    .terminal
                    .committed_effect()
                    .provider_receipt_sha256()
                    .is_none()
            );
            assert_eq!(restored.state_generation, before.state_generation);
            assert!(restored.payload.is_none());
            let after = invoke_ready(
                provider.as_ref(),
                &exact_scope,
                ProviderOperation::Inspection,
                None,
                json!({}),
            );
            assert_eq!(after.terminal.terminal_code(), TerminalCode::Success);
            assert_eq!(response_json(&after), response_json(&before));
            let after_snapshot = invoke_ready(
                provider.as_ref(),
                &exact_scope,
                ProviderOperation::SnapshotExport,
                None,
                json!({}),
            );
            assert_eq!(
                after_snapshot.terminal.terminal_code(),
                TerminalCode::Success
            );
            assert_eq!(snapshot_bytes(&after_snapshot), before_snapshot_bytes);
            assert_surviving_records(provider.as_ref());
        }
        drop(provider);
        let provider = adapter(&root);
        assert_surviving_records(provider.as_ref());
    }

    fn direct_observe_payload(source: &str, key: &str, value: &str, idempotency: &str) -> Value {
        let source_json = serde_json::to_string(source).expect("source JSON");
        let key_json = serde_json::to_string(key).expect("key JSON");
        let value_json = serde_json::to_string(value).expect("value JSON");
        let canonical = format!(
            "{{\"source\":{source_json},\"key_text\":{key_json},\"value_text\":{value_json},\"affect\":null,\"surprise\":0.0,\"intensity\":1.0,\"provenance\":{{}}}}"
        );
        json!({
            "idempotency_key": idempotency,
            "payload_sha256": sha256_hex(canonical.as_bytes()),
            "source": source,
            "key_text": key,
            "value_text": value,
            "affect": null,
            "surprise": 0.0,
            "intensity": 1.0,
            "provenance": {}
        })
    }

    #[test]
    fn real_worker_mailbox_and_namespace_catalog_saturate_without_hanging() {
        let mailbox_root = TestRoot::new("mailbox");
        let client = Arc::new(
            WorkerClient::spawn(
                worker_binary(),
                mailbox_root.state_root().path(),
                WorkerOptions {
                    test_double: true,
                    ..WorkerOptions::default()
                },
            )
            .expect("spawn mailbox worker client"),
        );
        let threads = 40usize;
        let barrier = Arc::new(Barrier::new(threads));
        let started = Instant::now();
        let mut handles = Vec::new();
        for index in 0..threads {
            let client = Arc::clone(&client);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                client.call(
                    Request::new(
                        u64::try_from(index).expect("request id"),
                        3_000,
                        Operation::Health,
                        sha256_hex(b"mailbox-namespace"),
                        json!({"test_sleep_before_ms": 500}),
                    ),
                    Duration::from_secs(3),
                )
            }));
        }
        let results = handles
            .into_iter()
            .map(|handle| handle.join().expect("mailbox caller joins"))
            .collect::<Vec<_>>();
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(
            results
                .iter()
                .any(|result| matches!(result, Err(ClientError::Busy)))
        );
        assert!(results.iter().all(|result| matches!(
            result,
            Ok(_) | Err(ClientError::Busy | ClientError::Cancelled)
        )));

        let catalog_root = TestRoot::new("catalog");
        let catalog = WorkerClient::spawn(
            worker_binary(),
            catalog_root.state_root().path(),
            WorkerOptions {
                test_double: true,
                ..WorkerOptions::default()
            },
        )
        .expect("spawn catalog worker client");
        let mut outcomes = BTreeMap::new();
        for index in 0..33usize {
            let namespace = sha256_hex(format!("catalog-{index}").as_bytes());
            let reply = catalog
                .call(
                    Request::new(
                        u64::try_from(index + 1).expect("catalog request id"),
                        5_000,
                        Operation::Observe,
                        &namespace,
                        direct_observe_payload(
                            "catalog-source",
                            "catalog key",
                            "catalog value",
                            &format!("catalog-key-{index}"),
                        ),
                    ),
                    Duration::from_secs(5),
                )
                .expect("catalog request returns typed reply");
            *outcomes
                .entry(format!("{:?}", reply.outcome))
                .or_insert(0usize) += 1;
        }
        assert_eq!(outcomes.get("Success"), Some(&32));
        assert_eq!(outcomes.get("BudgetExceeded"), Some(&1));
    }

    #[test]
    fn same_conformance_harness_rejects_foreign_scope_faulty_provider() {
        let root = TestRoot::new("fault-control");
        let real = adapter(&root);
        let descriptor = real.descriptor();
        let faulty = AdversarialProviderV1::new(AdversarialProviderInputsV1 {
            descriptor,
            provider_instance_id: "ncm-rs-019-faulty".to_owned(),
            state_namespace: sha256_hex(b"faulty-namespace"),
            ready_receipt_sha256: sha256_hex(b"faulty-ready"),
            handshake_script: AdversarialScriptV1::always(
                HandshakeMisbehaviourV1::AcceptsForeignScope,
            ),
            invoke_script: AdversarialScriptV1::always(MisbehaviourV1::Compliant),
            payloads: Arc::new(NoPayloadSourceV1),
        });
        let harness = ProviderHarness::new(&faulty).expect("bind faulty harness");
        let fixture = mandatory_conformance_fixture(
            harness.fixture_identity(),
            scope("project-fault-control"),
            REGISTRATION_REVISION,
        )
        .expect("mandatory definition for negative control");
        let report = harness.run_product(&fixture).expect("run negative control");
        assert!(!report.passed());
        assert_eq!(report.summary().status, ConformanceStatus::Fail);
        assert_eq!(report.summary().failed_steps, 1);
        assert_eq!(report.summary().not_run_steps, 6);
    }

    #[test]
    #[ignore = "requires the pinned MiniLM artifacts under TRACEDECAY_NCM_REAL_MODEL_ROOT"]
    fn real_encoder_process_population() {
        let root = std::env::var_os("TRACEDECAY_NCM_REAL_MODEL_ROOT")
            .map(PathBuf::from)
            .expect(
                "set TRACEDECAY_NCM_REAL_MODEL_ROOT to an isolated installed pinned model root",
            );
        let open_provider = || {
            let surface = RustNcmSurface::new(RustNcmConfig {
                worker_binary: worker_binary(),
                state_root: StateRoot::new(root.clone()).expect("real model root must be absolute"),
                worker_options: WorkerOptions::default(),
            })
            .expect("open real encoder worker");
            NcmProviderAdapter::new(Arc::new(surface)).expect("real encoder adapter")
        };
        let provider = open_provider();
        let exact_scope = scope(&format!(
            "project-real-encoder-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let source = "real-encoder-source";
        let value = "atomic commit succeeded";
        let text = format!("database transaction: {value}");
        let projected_text = format!("assistant: {text}");
        let recall_query = json!({"query_text": "durable database commit", "top_k": 5});
        let health = invoke_ready(
            &provider,
            &exact_scope,
            ProviderOperation::Health,
            None,
            json!({}),
        );
        assert_eq!(health.terminal.terminal_code(), TerminalCode::Success);
        let observed = invoke_ready(
            &provider,
            &exact_scope,
            ProviderOperation::Observe,
            Some("real-encoder-observe"),
            current_observe_value(&exact_scope, source, "database-transaction", &text),
        );
        assert_eq!(observed.terminal.terminal_code(), TerminalCode::Success);
        let recalled = invoke_ready(
            &provider,
            &exact_scope,
            ProviderOperation::Recall,
            None,
            recall_query.clone(),
        );
        assert_eq!(recalled.terminal.terminal_code(), TerminalCode::Success);
        assert!(find_string(
            &response_json(&recalled),
            "value_text",
            &projected_text
        ));

        let kept_source = "real-encoder-surviving-source";
        let kept_value = "unrelated source is retained";
        let kept_text = format!("solar panels generate electricity from sunlight: {kept_value}");
        let kept_projected_text = format!("assistant: {kept_text}");
        let kept = invoke_ready(
            &provider,
            &exact_scope,
            ProviderOperation::Observe,
            Some("real-encoder-observe-kept"),
            current_observe_value(&exact_scope, kept_source, "solar-panels", &kept_text),
        );
        assert_eq!(kept.terminal.terminal_code(), TerminalCode::Success);
        let assert_kept_present = |provider: &NcmProviderAdapter| {
            let recalled = invoke_ready(
                provider,
                &exact_scope,
                ProviderOperation::Recall,
                None,
                json!({
                    "query_text": "solar panels generate electricity from sunlight",
                    "top_k": 5,
                }),
            );
            assert_eq!(recalled.terminal.terminal_code(), TerminalCode::Success);
            assert!(
                find_string(
                    &response_json(&recalled),
                    "value_text",
                    &kept_projected_text
                ),
                "the undeleted source was lost"
            );
        };
        assert_kept_present(&provider);

        let consolidated = invoke_common_maintenance(
            &provider,
            &exact_scope,
            "real-encoder-consolidate",
            "consolidate",
            100,
            None,
        );
        assert_eq!(consolidated.terminal.terminal_code(), TerminalCode::Success);
        let inspection = invoke_ready(
            &provider,
            &exact_scope,
            ProviderOperation::Inspection,
            None,
            json!({}),
        );
        assert_eq!(inspection.terminal.terminal_code(), TerminalCode::Success);
        let ltm_active =
            find_u64(&response_json(&inspection), "ltm_active").expect("LTM inspection count");
        assert!(ltm_active > 0, "explicit consolidation left LTM empty");
        let exported = invoke_ready(
            &provider,
            &exact_scope,
            ProviderOperation::SnapshotExport,
            None,
            json!({}),
        );
        assert_eq!(exported.terminal.terminal_code(), TerminalCode::Success);
        let snapshot = snapshot_bytes(&exported);
        drop(provider);

        let provider = open_provider();
        let recalled = invoke_ready(
            &provider,
            &exact_scope,
            ProviderOperation::Recall,
            None,
            recall_query.clone(),
        );
        assert_eq!(recalled.terminal.terminal_code(), TerminalCode::Success);
        assert!(
            find_string(&response_json(&recalled), "value_text", &projected_text),
            "restart recall lost the consolidated record"
        );
        let assert_deleted_absent = |provider: &NcmProviderAdapter| {
            let recalled = invoke_ready(
                provider,
                &exact_scope,
                ProviderOperation::Recall,
                None,
                recall_query.clone(),
            );
            match recalled.terminal.terminal_code() {
                TerminalCode::Success => assert!(
                    !find_string(&response_json(&recalled), "value_text", &projected_text),
                    "deleted source was recalled"
                ),
                TerminalCode::SuccessZeroResults => {}
                code => panic!("unexpected recall terminal after source deletion: {code:?}"),
            }
        };
        let deleted = invoke_ready(
            &provider,
            &exact_scope,
            ProviderOperation::DeleteBySource,
            Some("real-encoder-delete"),
            json!({"source_id": exact_scope.session_forget_source_key(source)}),
        );
        assert_eq!(deleted.terminal.terminal_code(), TerminalCode::Success);
        assert_deleted_absent(&provider);
        drop(provider);

        let provider = open_provider();
        assert_deleted_absent(&provider);
        let replay = invoke_ready(
            &provider,
            &exact_scope,
            ProviderOperation::Replay,
            Some("real-encoder-replay-revoked"),
            json!({}),
        );
        assert_eq!(
            replay.terminal.terminal_code(),
            TerminalCode::CapabilityUnsupported
        );
        assert_deleted_absent(&provider);
        let before = invoke_ready(
            &provider,
            &exact_scope,
            ProviderOperation::Inspection,
            None,
            json!({}),
        );
        assert_eq!(before.terminal.terminal_code(), TerminalCode::Success);
        let before_snapshot = invoke_ready(
            &provider,
            &exact_scope,
            ProviderOperation::SnapshotExport,
            None,
            json!({}),
        );
        assert_eq!(
            before_snapshot.terminal.terminal_code(),
            TerminalCode::Success
        );
        let before_snapshot_bytes = snapshot_bytes(&before_snapshot);
        for bytes in [snapshot.clone(), snapshot] {
            let restored = invoke_ready(
                &provider,
                &exact_scope,
                ProviderOperation::SnapshotRestore,
                Some("real-encoder-restore-stale"),
                json!({"snapshot": bytes}),
            );
            assert_eq!(
                restored.terminal.terminal_code(),
                TerminalCode::ProviderUnavailable
            );
            assert_eq!(
                restored.terminal.diagnostic_id(),
                Some("ncm.current_advisory_admission_refused")
            );
            assert_eq!(
                restored.terminal.committed_effect().state(),
                CommittedEffectState::None
            );
            assert!(
                restored
                    .terminal
                    .committed_effect()
                    .provider_receipt_sha256()
                    .is_none()
            );
            assert_eq!(restored.state_generation, before.state_generation);
            assert!(restored.payload.is_none());
            let after = invoke_ready(
                &provider,
                &exact_scope,
                ProviderOperation::Inspection,
                None,
                json!({}),
            );
            assert_eq!(after.terminal.terminal_code(), TerminalCode::Success);
            assert_eq!(after.state_generation, before.state_generation);
            assert_eq!(response_json(&after), response_json(&before));
            let after_snapshot = invoke_ready(
                &provider,
                &exact_scope,
                ProviderOperation::SnapshotExport,
                None,
                json!({}),
            );
            assert_eq!(
                after_snapshot.terminal.terminal_code(),
                TerminalCode::Success
            );
            assert_eq!(
                after_snapshot.state_generation,
                before_snapshot.state_generation
            );
            assert_eq!(snapshot_bytes(&after_snapshot), before_snapshot_bytes);
            assert_deleted_absent(&provider);
            assert_kept_present(&provider);
        }
        drop(provider);

        let provider = open_provider();
        assert_deleted_absent(&provider);
        assert_kept_present(&provider);
    }

    #[test]
    fn canonical_current_observation_recall_preserves_identity_exclusions_and_scope() {
        let root = TestRoot::new("current-observation");
        let provider = adapter(&root);
        let exact_scope = scope_parts(
            "profile-rust-backend",
            "project-current-observation",
            "worktree-rust-backend",
            "refs/heads/feat/ncm-biomem-rust-v1",
            "session-current",
        );
        let text = "The build cache key includes the compiler version, target triple, and dependency lockfile digest.";
        let observation = current_observe_value(&exact_scope, "session-current", "message1", text);
        let observation_id = observation["source_identity"]["canonical_observation_id"]
            .as_str()
            .expect("canonical observation identity")
            .to_owned();
        let source_ref = format!("record:{observation_id}");
        let observed = invoke_ready(
            provider.as_ref(),
            &exact_scope,
            ProviderOperation::Observe,
            Some("current-observation-commit"),
            observation,
        );
        assert_eq!(observed.terminal.terminal_code(), TerminalCode::Success);
        assert_eq!(
            observed.terminal.committed_effect().state(),
            CommittedEffectState::Committed
        );

        let recall = |destination: &OwnedExactScope, observation_ids: &[&str], policy: &str| {
            let (receipt, generation) = ready_parts(provider.as_ref(), destination);
            let mut request = call(
                ProviderOperation::Recall,
                destination,
                &receipt,
                generation,
                None,
                json!({}),
            );
            request.payload = canonical_payload(
                operation_contract(ProviderOperation::Recall),
                json!({
                    "provider_id": NCM_PROVIDER_ID,
                    "registration_revision": request.registration_revision,
                    "ready_receipt_digest": request.ready_receipt_sha256,
                    "exact_scope_identity": scope_value(destination),
                    "request_identity": request.request_id,
                    "objective": "Retrieve the committed build cache guidance",
                    "query": text,
                    "temporal_query": {
                        "mode": "current", "evaluation_time": "2026-01-02T00:00:00Z",
                        "as_of": null, "interval_start": null, "interval_end": null,
                        "include_superseded": false, "include_revoked": false,
                        "unknown_validity_policy": policy
                    },
                    "budgets": {
                        "maximum_candidates": 4, "maximum_candidate_content_bytes": 8192,
                        "maximum_total_content_bytes": 16384,
                        "maximum_source_refs_per_candidate": 4,
                        "maximum_trace_refs_per_candidate": 4, "maximum_warnings": 4,
                        "maximum_extensions_per_candidate": 4
                    },
                    "exclusions": {
                        "stable_memory_refs": [], "candidate_ids": [], "source_refs": [],
                        "trace_refs": [], "observation_ids": observation_ids, "content_sha256": []
                    },
                    "required_capabilities": ["recall.query.v1"],
                    "policy_revision": 1, "extensions": [],
                    "deadline": {
                        "deadline_utc_micros": request.control.deadline_utc_micros(),
                        "remaining_millis": request.control.remaining_millis()
                    },
                    "cancellation": "live"
                }),
            );
            provider.invoke(&request)
        };
        let recalled = recall(&exact_scope, &[], "degrade");
        assert_eq!(recalled.terminal.terminal_code(), TerminalCode::Partial);
        assert_eq!(recalled.state_generation, observed.state_generation);
        let result = response_json(&recalled);
        let candidates = result["candidates"]
            .as_array()
            .expect("canonical candidates");
        assert_eq!(candidates.len(), 1);
        let candidate = &candidates[0];
        assert_eq!(candidate["content"], format!("assistant: {text}"));
        assert_eq!(candidate["source_refs"], json!([source_ref]));
        assert_eq!(
            candidate["provenance"]["observation_refs"],
            json!([observation_id])
        );
        assert_eq!(candidate["provenance"]["original_sources"], json!([]));
        let validity = candidate["validity"]
            .as_object()
            .expect("canonical validity");
        for field in ["observed_at", "valid_from", "source_revision"] {
            assert_eq!(validity.get(field), Some(&Value::Null), "{field}");
        }
        assert_eq!(validity.get("temporal_state"), Some(&json!("unknown")));
        for (field, value) in scope_value(&exact_scope).as_object().expect("exact scope") {
            assert_eq!(candidate["exact_scope_identity"].get(field), Some(value));
        }

        let unknown_excluded = recall(&exact_scope, &[], "exclude");
        assert_eq!(
            unknown_excluded.terminal.terminal_code(),
            TerminalCode::Partial
        );
        assert_eq!(unknown_excluded.state_generation, observed.state_generation);
        let unknown_result = response_json(&unknown_excluded);
        assert_eq!(unknown_result["candidates"], json!([]));
        assert_eq!(unknown_result["coverage"]["state"], "partial");
        assert_eq!(unknown_result["coverage"]["returned_items"], 0);
        assert!(
            unknown_result["coverage"]["scanned_items"]
                .as_u64()
                .expect("scanned item count")
                > 0
        );
        assert!(
            unknown_result["coverage"]["reasons"]
                .as_array()
                .expect("unknown coverage reasons")
                .contains(&json!("ncm.original_source_evidence_incomplete"))
        );

        let excluded = recall(&exact_scope, &[observation_id.as_str()], "degrade");
        assert_eq!(excluded.terminal.terminal_code(), TerminalCode::Partial);
        assert_eq!(excluded.state_generation, observed.state_generation);
        let excluded_result = response_json(&excluded);
        assert_eq!(excluded_result["candidates"], json!([]));
        assert!(
            excluded_result["coverage"]["excluded_items"]
                .as_u64()
                .expect("excluded item count")
                > 0
        );

        let (receipt, generation) = ready_parts(provider.as_ref(), &exact_scope);
        let mut inspection_call = call(
            ProviderOperation::Inspection,
            &exact_scope,
            &receipt,
            generation,
            None,
            json!({}),
        );
        inspection_call.payload = canonical_payload(
            operation_contract(ProviderOperation::Inspection),
            json!({
                "common_request": {
                    "provider_id": NCM_PROVIDER_ID,
                    "registration_revision": inspection_call.registration_revision,
                    "ready_receipt_digest": inspection_call.ready_receipt_sha256,
                    "exact_scope_identity": scope_value(&exact_scope),
                    "operation_id": inspection_call.operation_id,
                    "idempotency_key": inspection_call.idempotency_key,
                    "expected_state_generation": inspection_call.expected_state_generation,
                    "request_identity": inspection_call.request_id,
                    "policy_revision": 1,
                    "deadline": {
                        "deadline_utc_micros": inspection_call.control.deadline_utc_micros(),
                        "remaining_millis": inspection_call.control.remaining_millis()
                    },
                    "cancellation": "live", "extensions": []
                },
                "view": "state_summary", "selector": {}, "maximum_items": 4,
                "maximum_bytes": 65536, "redaction_policy_revision": 1, "cursor": null
            }),
        );
        let inspected = provider.invoke(&inspection_call);
        assert_eq!(inspected.terminal.terminal_code(), TerminalCode::Success);
        assert_eq!(inspected.state_generation, observed.state_generation);
        let inspection_result = response_json(&inspected);
        let items = inspection_result["items"]
            .as_array()
            .expect("state summary items");
        assert_eq!(items.len(), 1);
        let source = &items[0]["source"];
        assert_eq!(source["canonical_session_id"], "session-current");
        assert_eq!(source["observation_id"], observation_id);
        assert_eq!(source["canonical_provider_id"], "claude");
        assert_eq!(source["stable_record_id"], "message1");
        assert_eq!(
            source["source_key"],
            exact_scope.session_forget_source_key("session-current")
        );
        assert_eq!(source["source_refs"], json!([source_ref]));
        let item = &items[0];
        assert!(item.get("target").is_none());
        assert_eq!(item["delivery_scope"], scope_value(&exact_scope));
        assert_eq!(item["stable_memory_ref"], candidate["stable_memory_ref"]);
        assert!(
            item["stable_memory_ref"]
                .as_str()
                .expect("stable memory reference")
                .starts_with("ncm-memory:")
        );
        let mut inspected_values = vec![&inspection_result];
        while let Some(value) = inspected_values.pop() {
            match value {
                Value::Object(object) => {
                    assert!(!object.contains_key("original_source"));
                    inspected_values.extend(object.values());
                }
                Value::Array(values) => inspected_values.extend(values.iter()),
                _ => {}
            }
        }

        let mut foreign_scope = exact_scope.clone();
        foreign_scope.agent_session_id = "session-foreign".to_owned();
        let foreign = recall(&foreign_scope, &[], "degrade");
        assert_eq!(
            foreign.terminal.terminal_code(),
            TerminalCode::SuccessZeroResults
        );
        assert_eq!(response_json(&foreign)["candidates"], json!([]));
    }
}

#[cfg(not(feature = "rust-backend"))]
#[test]
fn rust_backend_conformance_requires_feature() {
    assert!(option_env!("CARGO_FEATURE_RUST_BACKEND").is_none());
}
