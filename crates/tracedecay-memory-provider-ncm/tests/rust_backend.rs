//! End-to-end tests for the feature-gated Rust NCM worker surface.
#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

#[cfg(not(feature = "rust-backend"))]
#[test]
fn feature_off_compiles_without_starting_a_worker() {
    assert!(option_env!("CARGO_FEATURE_RUST_BACKEND").is_none());
}

#[cfg(feature = "rust-backend")]
mod enabled {
    use std::collections::BTreeSet;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, OnceLock};
    use std::time::Duration;

    use serde_json::{Value, json};
    use sha2::{Digest, Sha256};
    use tracedecay_memory_provider_api::contract::{CommittedEffectState, TerminalCode};
    use tracedecay_memory_provider_api::{
        CancellationToken, CanonicalPayload, HandshakeRequest, HandshakeRequestParts,
        MemoryProvider, OperationControl, OwnedExactScope, OwnedProviderId, OwnedVersionedId,
        PayloadSanitizationReceipt, PayloadSanitizationReceiptParts, ProviderCall,
        ProviderCallParts, ProviderLimits, ProviderOperation, ProviderReply,
        observation_extensions_digest,
    };
    use tracedecay_memory_provider_ncm::{
        NCM_PROVIDER_ID, NcmCognitiveSurface, NcmProviderAdapter, NcmSurfaceCall,
        NcmSurfaceHandshakeRequest, NcmSurfaceHandshakeResponse, RustNcmConfig, RustNcmSurface,
        StateRoot, WorkerOptions,
    };

    const RESOLVED_SCOPE_DIGEST: &str =
        "sha256:1111111111111111111111111111111111111111111111111111111111111111";
    const TEST_SANITIZER_REVISION: &str =
        "tracedecay.memory.observation.hygiene.v1+ncm-rust-backend-test";
    static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);
    static WORKER_BINARY: OnceLock<PathBuf> = OnceLock::new();

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new(label: &str) -> Self {
            let serial = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
            let root = target_dir().join("test-profile").join(format!(
                "ncm-rs-018-{label}-{}-{serial}",
                std::process::id()
            ));
            fs::create_dir_all(&root).expect("create test state root");
            Self(root)
        }

        fn state_root(&self) -> StateRoot {
            StateRoot::new(self.0.clone()).expect("valid absolute state root")
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
            .expect("provider crate is under repository crates directory")
            .to_path_buf()
    }

    fn target_dir() -> PathBuf {
        match std::env::var_os("CARGO_TARGET_DIR") {
            Some(path) => {
                let path = PathBuf::from(path);
                if path.is_absolute() {
                    path
                } else {
                    repository_root().join(path)
                }
            }
            None => repository_root().join("target"),
        }
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
                    .expect("run worker build");
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

    fn scope(project_id: &str) -> OwnedExactScope {
        OwnedExactScope::new(
            "profile-rust-backend",
            project_id,
            "repository-rust-backend",
            "worktree-rust-backend",
            "refs/heads/feat/ncm-biomem-rust-v1",
            "session-rust-backend",
            RESOLVED_SCOPE_DIGEST,
        )
        .expect("valid scope")
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

    fn all_capabilities() -> Vec<OwnedVersionedId> {
        [
            "provider.health.v1",
            "observation.accept.v1",
            "recall.query.v1",
            "feedback.record.v1",
            "maintenance.run.v1",
            "inspection.read.v1",
            "correction.apply.v1",
            "deletion.by_source.v1",
            "snapshot.export.v1",
            "snapshot.restore.v1",
        ]
        .into_iter()
        .map(|value| OwnedVersionedId::new(value).expect("known capability"))
        .collect()
    }

    fn handshake(
        adapter: &NcmProviderAdapter,
        exact_scope: &OwnedExactScope,
    ) -> tracedecay_memory_provider_api::HandshakeResponse {
        adapter.handshake(
            &HandshakeRequest::new(HandshakeRequestParts {
                provider_id: OwnedProviderId::new(NCM_PROVIDER_ID).expect("NCM provider id"),
                registration_revision: 1,
                exact_scope: exact_scope.clone(),
                request_id: format!("handshake-{}", NEXT_ROOT.fetch_add(1, Ordering::Relaxed)),
                required_capabilities: all_capabilities(),
                host_limits: limits(),
                control: OperationControl::new(i64::MAX, 10_000, CancellationToken::new()),
                challenge_nonce: [7; 32],
            })
            .expect("valid handshake request"),
        )
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

    fn payload(operation: ProviderOperation, value: Value) -> CanonicalPayload {
        let bytes = serde_json::to_vec(&value).expect("serialize test payload");
        CanonicalPayload::new(
            OwnedVersionedId::new(operation_contract(operation)).expect("payload contract"),
            bytes.clone(),
            hex_sha256(&bytes),
        )
        .expect("canonical payload")
    }

    fn call(
        operation: ProviderOperation,
        exact_scope: &OwnedExactScope,
        ready_receipt: &str,
        generation: u64,
        idempotency_key: Option<&str>,
        value: Value,
        remaining_millis: u64,
    ) -> ProviderCall {
        let call = ProviderCall::new(ProviderCallParts {
            operation,
            provider_id: OwnedProviderId::new(NCM_PROVIDER_ID).expect("NCM provider id"),
            registration_revision: 1,
            ready_receipt_sha256: ready_receipt.to_owned(),
            exact_scope: exact_scope.clone(),
            request_id: format!("request-{}", NEXT_ROOT.fetch_add(1, Ordering::Relaxed)),
            operation_id: format!(
                "operation-{}-{}",
                operation.as_wire(),
                NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
            ),
            expected_state_generation: generation,
            idempotency_key: idempotency_key.map(str::to_owned),
            control: OperationControl::new(i64::MAX, remaining_millis, CancellationToken::new()),
            payload: payload(operation, value),
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

    fn admit_observation(call: ProviderCall) -> ProviderCall {
        let extensions_digest =
            observation_extensions_digest(&call.extensions).expect("empty extension digest");
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

    fn ready_parts(response: &tracedecay_memory_provider_api::HandshakeResponse) -> (String, u64) {
        assert_eq!(response.terminal.terminal_code(), TerminalCode::Success);
        let receipt = response
            .ready_receipt_sha256
            .clone()
            .expect("ready receipt");
        let generation = response
            .descriptor
            .as_ref()
            .expect("ready descriptor")
            .state_generation;
        (receipt, generation)
    }

    fn invoke_after_handshake(
        adapter: &NcmProviderAdapter,
        exact_scope: &OwnedExactScope,
        operation: ProviderOperation,
        idempotency_key: Option<&str>,
        value: Value,
    ) -> ProviderReply {
        let ready = handshake(adapter, exact_scope);
        let (receipt, generation) = ready_parts(&ready);
        adapter.invoke(&call(
            operation,
            exact_scope,
            &receipt,
            generation,
            idempotency_key,
            value,
            10_000,
        ))
    }

    fn response_json(reply: &ProviderReply) -> Value {
        serde_json::from_slice(
            &reply
                .payload
                .as_ref()
                .expect("successful response payload")
                .bytes,
        )
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

    fn hex_sha256(bytes: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let digest = Sha256::digest(bytes);
        let mut output = String::with_capacity(64);
        for byte in digest {
            output.push(char::from(HEX[usize::from(byte >> 4)]));
            output.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        output
    }

    #[test]
    fn descriptor_and_handshake_expose_reserved_identity_and_exact_capabilities() {
        let root = TestRoot::new("handshake");
        let surface = surface(&root);
        let descriptor = surface.descriptor();
        assert_eq!(descriptor.provider_id.as_str(), NCM_PROVIDER_ID);
        assert!(
            descriptor
                .state_schema_version
                .starts_with("ncm-biomem-rs.v1+")
        );
        let capabilities = descriptor
            .capabilities
            .iter()
            .map(|capability| capability.as_str().to_owned())
            .collect::<BTreeSet<_>>();
        let expected = all_capabilities()
            .into_iter()
            .map(|capability| capability.as_str().to_owned())
            .collect::<BTreeSet<_>>();
        assert_eq!(capabilities, expected);
        assert!(!capabilities.contains("replay.execute.v1"));

        let adapter = NcmProviderAdapter::new(surface).expect("construct adapter");
        let response = handshake(&adapter, &scope("project-handshake"));
        assert_eq!(response.terminal.terminal_code(), TerminalCode::Success);
        assert_eq!(
            response
                .descriptor
                .as_ref()
                .expect("descriptor")
                .provider_id
                .as_str(),
            NCM_PROVIDER_ID
        );
        assert!(response.ready_receipt_sha256.is_some());
    }

    #[test]
    fn mutation_generation_requires_rehandshake_and_duplicate_replays_receipt() {
        let root = TestRoot::new("generation");
        let adapter = NcmProviderAdapter::new(surface(&root)).expect("construct adapter");
        let exact_scope = scope("project-generation");
        let initial = handshake(&adapter, &exact_scope);
        let (receipt, generation) = ready_parts(&initial);
        let observation = observe_value("source-generation", "cargo test", "all tests passed");
        let first_call = call(
            ProviderOperation::Observe,
            &exact_scope,
            &receipt,
            generation,
            Some("observe-generation"),
            observation.clone(),
            10_000,
        );
        let first = adapter.invoke(&first_call);
        assert_eq!(first.terminal.terminal_code(), TerminalCode::Success);
        assert_eq!(
            first.terminal.committed_effect().state(),
            CommittedEffectState::Committed
        );
        assert!(first.state_generation > generation);
        let first_receipt = first
            .terminal
            .committed_effect()
            .provider_receipt_sha256()
            .expect("committed receipt")
            .to_owned();

        let refused = adapter.invoke(&call(
            ProviderOperation::Recall,
            &exact_scope,
            &receipt,
            first.state_generation,
            None,
            json!({"query_text": "cargo test", "top_k": 5}),
            10_000,
        ));
        assert_eq!(
            refused.terminal.terminal_code(),
            TerminalCode::StaleIdentity
        );

        let readmitted = handshake(&adapter, &exact_scope);
        let (receipt, generation) = ready_parts(&readmitted);
        assert_eq!(generation, first.state_generation);
        let duplicate = adapter.invoke(&call(
            ProviderOperation::Observe,
            &exact_scope,
            &receipt,
            generation,
            Some("observe-generation"),
            observation,
            10_000,
        ));
        assert_eq!(duplicate.terminal.terminal_code(), TerminalCode::Success);
        assert_eq!(
            duplicate.terminal.committed_effect().state(),
            CommittedEffectState::Duplicate
        );
        assert_eq!(duplicate.state_generation, generation);
        assert_eq!(
            duplicate
                .terminal
                .committed_effect()
                .provider_receipt_sha256(),
            Some(first_receipt.as_str())
        );

        let recall = invoke_after_handshake(
            &adapter,
            &exact_scope,
            ProviderOperation::Recall,
            None,
            json!({"query_text": "cargo test", "top_k": 5}),
        );
        assert_eq!(recall.terminal.terminal_code(), TerminalCode::Success);
        assert!(find_string(
            &response_json(&recall),
            "value_text",
            "all tests passed"
        ));
    }

    #[test]
    fn feedback_correction_maintenance_inspection_and_delete_use_durable_state() {
        let root = TestRoot::new("operations");
        let adapter = NcmProviderAdapter::new(surface(&root)).expect("construct adapter");
        let exact_scope = scope("project-operations");
        let first = invoke_after_handshake(
            &adapter,
            &exact_scope,
            ProviderOperation::Observe,
            Some("observe-one"),
            observe_value("source-delete", "first command", "first outcome"),
        );
        let first_id = find_u64(&response_json(&first), "record_id").expect("first record id");
        let second = invoke_after_handshake(
            &adapter,
            &exact_scope,
            ProviderOperation::Observe,
            Some("observe-two"),
            observe_value("source-keep", "second command", "second outcome"),
        );
        let second_id = find_u64(&response_json(&second), "record_id").expect("second record id");

        let feedback = invoke_after_handshake(
            &adapter,
            &exact_scope,
            ProviderOperation::Feedback,
            Some("feedback-one"),
            json!({"record_ids": [first_id]}),
        );
        assert_eq!(feedback.terminal.terminal_code(), TerminalCode::Success);

        let correction = invoke_after_handshake(
            &adapter,
            &exact_scope,
            ProviderOperation::Correction,
            Some("correction-one"),
            json!({
                "superseded_record_id": first_id,
                "superseding_record_id": second_id,
                "evidence_sha256": hex_sha256(b"correction evidence")
            }),
        );
        assert_eq!(correction.terminal.terminal_code(), TerminalCode::Success);

        let maintenance = invoke_after_handshake(
            &adapter,
            &exact_scope,
            ProviderOperation::Maintenance,
            Some("maintenance-checkpoint"),
            json!({"kind": "checkpoint"}),
        );
        assert_eq!(maintenance.terminal.terminal_code(), TerminalCode::Success);

        let inspection = invoke_after_handshake(
            &adapter,
            &exact_scope,
            ProviderOperation::Inspection,
            None,
            json!({}),
        );
        assert_eq!(inspection.terminal.terminal_code(), TerminalCode::Success);
        assert!(find_u64(&response_json(&inspection), "records").is_some());

        let deleted = invoke_after_handshake(
            &adapter,
            &exact_scope,
            ProviderOperation::DeleteBySource,
            Some("delete-source"),
            json!({"source_id": "source-delete"}),
        );
        assert_eq!(deleted.terminal.terminal_code(), TerminalCode::Success);
        let recall = invoke_after_handshake(
            &adapter,
            &exact_scope,
            ProviderOperation::Recall,
            None,
            json!({"query_text": "first command", "top_k": 5}),
        );
        if recall.terminal.terminal_code() == TerminalCode::Success {
            assert!(!find_string(
                &response_json(&recall),
                "source",
                "source-delete"
            ));
        } else {
            assert_eq!(
                recall.terminal.terminal_code(),
                TerminalCode::SuccessZeroResults
            );
        }
    }

    #[test]
    fn snapshot_export_and_restore_replace_later_state() {
        let root = TestRoot::new("snapshot");
        let adapter = NcmProviderAdapter::new(surface(&root)).expect("construct adapter");
        let exact_scope = scope("project-snapshot");
        let first = invoke_after_handshake(
            &adapter,
            &exact_scope,
            ProviderOperation::Observe,
            Some("snapshot-observe-one"),
            observe_value("source-snapshot-one", "alpha command", "alpha outcome"),
        );
        assert_eq!(first.terminal.terminal_code(), TerminalCode::Success);
        let exported = invoke_after_handshake(
            &adapter,
            &exact_scope,
            ProviderOperation::SnapshotExport,
            None,
            json!({}),
        );
        assert_eq!(
            exported.terminal.terminal_code(),
            TerminalCode::Success,
            "snapshot export diagnostic: {:?}",
            exported.terminal.diagnostic_id()
        );
        let snapshot = response_json(&exported)
            .get("bytes")
            .and_then(Value::as_array)
            .expect("snapshot byte array")
            .clone();

        let second = invoke_after_handshake(
            &adapter,
            &exact_scope,
            ProviderOperation::Observe,
            Some("snapshot-observe-two"),
            observe_value("source-snapshot-two", "beta command", "beta outcome"),
        );
        assert_eq!(second.terminal.terminal_code(), TerminalCode::Success);
        let restored = invoke_after_handshake(
            &adapter,
            &exact_scope,
            ProviderOperation::SnapshotRestore,
            Some("snapshot-restore"),
            json!({"snapshot": snapshot}),
        );
        assert_eq!(
            restored.terminal.terminal_code(),
            TerminalCode::Success,
            "snapshot restore diagnostic: {:?}",
            restored.terminal.diagnostic_id()
        );

        let alpha = invoke_after_handshake(
            &adapter,
            &exact_scope,
            ProviderOperation::Recall,
            None,
            json!({"query_text": "alpha command", "top_k": 5}),
        );
        assert_eq!(alpha.terminal.terminal_code(), TerminalCode::Success);
        assert!(find_string(
            &response_json(&alpha),
            "value_text",
            "alpha outcome"
        ));
        let beta = invoke_after_handshake(
            &adapter,
            &exact_scope,
            ProviderOperation::Recall,
            None,
            json!({"query_text": "beta command", "top_k": 5}),
        );
        if beta.terminal.terminal_code() == TerminalCode::Success {
            assert!(!find_string(
                &response_json(&beta),
                "source",
                "source-snapshot-two"
            ));
        } else {
            assert_eq!(
                beta.terminal.terminal_code(),
                TerminalCode::SuccessZeroResults
            );
        }
    }

    #[test]
    fn deadline_after_commit_returns_effect_unknown_then_retry_replays() {
        let root = TestRoot::new("effect-unknown");
        let adapter = NcmProviderAdapter::new(surface(&root)).expect("construct adapter");
        let exact_scope = scope("project-effect-unknown");
        let ready = handshake(&adapter, &exact_scope);
        let (receipt, generation) = ready_parts(&ready);
        let mut observation = observe_value(
            "source-effect-unknown",
            "deadline command",
            "deadline outcome",
        );
        observation
            .as_object_mut()
            .expect("observation object")
            .insert("test_sleep_after_commit_ms".to_owned(), json!(250));
        let unknown = adapter.invoke(&call(
            ProviderOperation::Observe,
            &exact_scope,
            &receipt,
            generation,
            Some("observe-effect-unknown"),
            observation.clone(),
            50,
        ));
        assert_eq!(
            unknown.terminal.terminal_code(),
            TerminalCode::EffectUnknown
        );
        assert_eq!(
            unknown.terminal.committed_effect().state(),
            CommittedEffectState::Unknown
        );
        assert!(
            unknown
                .terminal
                .committed_effect()
                .reconciliation_action()
                .is_some()
        );

        let mut retry_ready = handshake(&adapter, &exact_scope);
        if retry_ready.terminal.terminal_code() == TerminalCode::StaleIdentity {
            retry_ready = handshake(&adapter, &exact_scope);
        }
        let (retry_receipt, retry_generation) = ready_parts(&retry_ready);
        let retried = adapter.invoke(&call(
            ProviderOperation::Observe,
            &exact_scope,
            &retry_receipt,
            retry_generation,
            Some("observe-effect-unknown"),
            observation,
            10_000,
        ));
        assert_eq!(retried.terminal.terminal_code(), TerminalCode::Success);
        assert_eq!(
            retried.terminal.committed_effect().state(),
            CommittedEffectState::Duplicate
        );
        let recall = invoke_after_handshake(
            &adapter,
            &exact_scope,
            ProviderOperation::Recall,
            None,
            json!({"query_text": "deadline command", "top_k": 5}),
        );
        assert_eq!(recall.terminal.terminal_code(), TerminalCode::Success);
        assert!(find_string(
            &response_json(&recall),
            "value_text",
            "deadline outcome"
        ));
    }

    struct LeakingSurface {
        inner: Arc<RustNcmSurface>,
        raw_project_id: String,
    }

    impl NcmCognitiveSurface for LeakingSurface {
        fn descriptor(&self) -> tracedecay_memory_provider_api::ProviderDescriptor {
            self.inner.descriptor()
        }

        fn handshake(&self, request: &NcmSurfaceHandshakeRequest) -> NcmSurfaceHandshakeResponse {
            self.inner.handshake(request)
        }

        fn invoke(&self, call: &NcmSurfaceCall) -> ProviderReply {
            let mut reply = self.inner.invoke(call);
            if reply.terminal.terminal_code() == TerminalCode::Success {
                reply.payload = Some(payload(
                    call.operation,
                    json!({"raw_project_id": self.raw_project_id}),
                ));
            }
            reply
        }
    }

    #[test]
    fn adapter_rejects_raw_project_identity_in_surface_reply() {
        let root = TestRoot::new("scope-leak");
        let raw_project_id = "project-scope-leak";
        let leaker = Arc::new(LeakingSurface {
            inner: surface(&root),
            raw_project_id: raw_project_id.to_owned(),
        });
        let adapter = NcmProviderAdapter::new(leaker).expect("construct adapter");
        let exact_scope = scope(raw_project_id);
        let reply = invoke_after_handshake(
            &adapter,
            &exact_scope,
            ProviderOperation::Health,
            None,
            json!({}),
        );
        assert_eq!(
            reply.terminal.terminal_code(),
            TerminalCode::ContractViolation
        );
        assert!(reply.payload.is_none());
    }
}
