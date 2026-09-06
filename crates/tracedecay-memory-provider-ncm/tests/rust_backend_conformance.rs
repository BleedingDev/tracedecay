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
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::atomic::{AtomicU64, Ordering};
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
        NCM_PROVIDER_ID, NcmNamespace, NcmProviderAdapter, RustNcmConfig, RustNcmSurface,
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
            let path = target_dir().join("test-profile").join(format!(
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

    fn repository_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("provider crate under repository crates")
            .to_path_buf()
    }

    fn target_dir() -> PathBuf {
        std::env::var_os("CARGO_TARGET_DIR").map_or_else(
            || repository_root().join("target"),
            |path| {
                let path = PathBuf::from(path);
                if path.is_absolute() {
                    path
                } else {
                    repository_root().join(path)
                }
            },
        )
    }

    fn worker_binary() -> PathBuf {
        WORKER_BINARY
            .get_or_init(|| {
                if let Some(path) = std::env::var_os("TRACEDECAY_NCM_WORKER") {
                    let path = PathBuf::from(path);
                    assert!(path.is_absolute(), "TRACEDECAY_NCM_WORKER must be absolute");
                    return path;
                }
                let status = Command::new(env!("CARGO"))
                    .current_dir(repository_root())
                    .env("RUSTC_WRAPPER", "")
                    .args([
                        "build",
                        "-p",
                        "tracedecay-memory-ncm-runtime",
                        "--bin",
                        "tracedecay-ncm-worker",
                        "--no-default-features",
                    ])
                    .status()
                    .expect("build test-double worker");
                assert!(status.success(), "worker build must succeed");
                target_dir().join("debug").join("tracedecay-ncm-worker")
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
    fn cancelled_calls_never_return_success_and_committed_observe_reconciles_once() {
        let root = TestRoot::new("cancel");
        let provider = adapter(&root);
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
        let mut value = observe_value("source-cancel", "cancel key", "cancel value");
        value
            .as_object_mut()
            .expect("observe object")
            .insert("test_sleep_after_commit_ms".to_owned(), json!(250));
        let (receipt, generation) = ready_parts(provider.as_ref(), &exact_scope);
        let observe = call_with(
            ProviderOperation::Observe,
            &exact_scope,
            &receipt,
            generation,
            Some("cancel-after-commit"),
            value.clone(),
            operation_contract(ProviderOperation::Observe),
            token.clone(),
            10_000,
        );
        let threaded = Arc::clone(&provider);
        let handle = thread::spawn(move || threaded.invoke(&observe));
        thread::sleep(Duration::from_millis(40));
        token.cancel();
        let unknown = handle.join().expect("cancelled observe joins");
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
        let first = invoke_ready(
            provider.as_ref(),
            &exact_scope,
            ProviderOperation::Observe,
            Some("privacy-observe-one"),
            observe_value("source-delete", "shared merged key", "deleted outcome"),
        );
        let first_id = find_u64(&response_json(&first), "record_id").expect("first record id");
        let second = invoke_ready(
            provider.as_ref(),
            &exact_scope,
            ProviderOperation::Observe,
            Some("privacy-observe-two"),
            observe_value("source-keep", "shared merged key", "kept outcome"),
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
            ("privacy-merge", "merge_prune"),
            ("privacy-checkpoint", "checkpoint"),
        ] {
            let reply = invoke_ready(
                provider.as_ref(),
                &exact_scope,
                ProviderOperation::Maintenance,
                Some(key),
                json!({"kind": kind}),
            );
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
            json!({"source_id": "source-delete"}),
        );
        assert_eq!(deleted.terminal.terminal_code(), TerminalCode::Success);
        let restored = invoke_ready(
            provider.as_ref(),
            &exact_scope,
            ProviderOperation::SnapshotRestore,
            Some("privacy-restore-stale"),
            json!({"snapshot": snapshot}),
        );
        assert_eq!(restored.terminal.terminal_code(), TerminalCode::Success);
        let recall = invoke_ready(
            provider.as_ref(),
            &exact_scope,
            ProviderOperation::Recall,
            None,
            json!({"query_text": "shared merged key", "top_k": 16}),
        );
        assert!(matches!(
            recall.terminal.terminal_code(),
            TerminalCode::Success | TerminalCode::SuccessZeroResults
        ));
        if recall.terminal.terminal_code() == TerminalCode::Success {
            let value = response_json(&recall);
            assert!(!find_string(&value, "source", "source-delete"));
            assert!(!find_string(&value, "value_text", "deleted outcome"));
            assert!(find_string(&value, "source", "source-keep"));
        }
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
            .expect("set TRACEDECAY_NCM_REAL_MODEL_ROOT to an installed pinned model root");
        let state_root = StateRoot::new(root).expect("real model root must be absolute");
        let surface = RustNcmSurface::new(RustNcmConfig {
            worker_binary: worker_binary(),
            state_root,
            worker_options: WorkerOptions::default(),
        })
        .expect("open real encoder worker");
        let provider = NcmProviderAdapter::new(Arc::new(surface)).expect("real encoder adapter");
        let exact_scope = scope("project-real-encoder");
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
            observe_value(
                "real-encoder-source",
                "database transaction",
                "atomic commit succeeded",
            ),
        );
        assert_eq!(observed.terminal.terminal_code(), TerminalCode::Success);
        let recalled = invoke_ready(
            &provider,
            &exact_scope,
            ProviderOperation::Recall,
            None,
            json!({"query_text": "durable database commit", "top_k": 5}),
        );
        assert_eq!(recalled.terminal.terminal_code(), TerminalCode::Success);
    }
}

#[cfg(not(feature = "rust-backend"))]
#[test]
fn rust_backend_conformance_requires_feature() {
    assert!(option_env!("CARGO_FEATURE_RUST_BACKEND").is_none());
}
