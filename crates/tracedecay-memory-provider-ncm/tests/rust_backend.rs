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
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
        RustNcmWorkerOwner, StateRoot, WorkerOptions,
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

    #[test]
    fn production_declaration_starts_no_child_and_refuses_test_double_identity() {
        let root = TestRoot::new("lazy-production-identity");
        let owner = Arc::new(
            RustNcmWorkerOwner::new(RustNcmConfig {
                worker_binary: worker_binary(),
                state_root: root.state_root(),
                worker_options: WorkerOptions {
                    test_double: true,
                    ..WorkerOptions::default()
                },
            })
            .unwrap(),
        );
        let surface = Arc::new(RustNcmSurface::from_production_worker(Arc::clone(&owner)).unwrap());
        assert!(owner.worker_pid().is_none());
        assert!(surface.provider_instance_id().unwrap().is_none());
        let declared = surface.descriptor();
        assert_eq!(declared.state_generation, 0);
        assert_eq!(
            surface.prove_provider_instance(
                std::time::Instant::now() + Duration::from_secs(5),
                Arc::new(|| true),
            ),
            Err(TerminalCode::Cancelled)
        );
        assert_eq!(
            surface.prove_provider_instance(std::time::Instant::now(), Arc::new(|| false),),
            Err(TerminalCode::DeadlineExceeded)
        );
        assert!(owner.worker_pid().is_none());
        assert_eq!(
            surface.prove_provider_instance(
                std::time::Instant::now() + Duration::from_secs(5),
                Arc::new(|| false),
            ),
            Err(TerminalCode::StateIncompatible)
        );
        assert_eq!(surface.descriptor(), declared);
        assert!(surface.provider_instance_id().unwrap().is_none());
        let adapter = NcmProviderAdapter::new(surface.clone()).unwrap();
        for _ in 0..2 {
            let reply = handshake(&adapter, &scope("lazy-production-identity"));
            assert_eq!(
                reply.terminal.terminal_code(),
                TerminalCode::StateIncompatible
            );
            assert_eq!(surface.descriptor(), declared);
            assert!(surface.provider_instance_id().unwrap().is_none());
        }
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "requires TRACEDECAY_NCM_WORKER and TRACEDECAY_NCM_REAL_MODEL_ROOT with pinned offline model"]
    fn lazy_production_declaration_is_proved_by_real_worker_handshake() {
        let root = TestRoot::new("lazy-production-real-model");
        let installed = PathBuf::from(std::env::var_os("TRACEDECAY_NCM_REAL_MODEL_ROOT").unwrap());
        assert!(installed.is_absolute());
        std::os::unix::fs::symlink(installed.join("models"), root.0.join("models")).unwrap();
        let owner = Arc::new(
            RustNcmWorkerOwner::new(RustNcmConfig {
                worker_binary: worker_binary(),
                state_root: root.state_root(),
                worker_options: WorkerOptions::default(),
            })
            .unwrap(),
        );
        let surface = Arc::new(RustNcmSurface::from_production_worker(Arc::clone(&owner)).unwrap());
        let declared = surface.descriptor();
        assert!(owner.worker_pid().is_none());
        assert!(surface.provider_instance_id().unwrap().is_none());
        let adapter = NcmProviderAdapter::new(surface.clone()).unwrap();
        let proved = surface
            .prove_provider_instance(
                std::time::Instant::now() + Duration::from_secs(5),
                Arc::new(|| false),
            )
            .unwrap();
        assert!(proved.is_some());
        assert_eq!(surface.descriptor(), declared);
        assert!(
            surface.provider_instance_id().unwrap().is_none(),
            "global proof must not install session identity"
        );
        let ready = handshake(&adapter, &scope("lazy-production-real-model"));
        assert_eq!(ready.terminal.terminal_code(), TerminalCode::Success);
        assert_eq!(surface.descriptor(), declared);
        assert!(surface.provider_instance_id().unwrap().is_some());
        assert!(ready.ready_receipt_sha256.is_some());
    }

    #[test]
    fn missing_worker_retains_real_adapter_and_reports_unavailable_handshake() {
        let root = TestRoot::new("missing-worker-observer");
        let surface = RustNcmSurface::new(RustNcmConfig {
            worker_binary: root.0.join("absent-ncm-worker"),
            state_root: root.state_root(),
            worker_options: WorkerOptions::default(),
        })
        .expect("missing executable retains lazy real surface");
        assert!(surface.provider_instance_id().unwrap().is_none());
        let provider = NcmProviderAdapter::new(Arc::new(surface)).expect("real adapter");
        let response = handshake(&provider, &scope("missing-worker-observer"));
        assert_eq!(
            response.terminal.terminal_code(),
            TerminalCode::ProviderUnavailable
        );
        assert_eq!(
            response.terminal.diagnostic_id(),
            Some("ncm.rust.worker_spawn_failed")
        );
        assert!(response.provider_instance_id.is_none());
        assert!(response.ready_receipt_sha256.is_none());
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

    fn exercise_shared_worker_project_surfaces(root: &TestRoot, options: WorkerOptions) {
        let owner = Arc::new(
            RustNcmWorkerOwner::new(RustNcmConfig {
                worker_binary: worker_binary(),
                state_root: root.state_root(),
                worker_options: options,
            })
            .expect("shared worker owner"),
        );
        assert!(owner.worker_pid().is_none(), "owner construction is lazy");
        let barrier = std::sync::Barrier::new(2);
        let (surface_a, surface_b) = std::thread::scope(|threads| {
            let mount = || {
                barrier.wait();
                Arc::new(
                    RustNcmSurface::from_worker(Arc::clone(&owner)).expect("project-local surface"),
                )
            };
            let a = threads.spawn(mount);
            let b = threads.spawn(mount);
            (a.join().unwrap(), b.join().unwrap())
        });
        let pid = owner.worker_pid().expect("one live worker process");
        assert_eq!(surface_a.worker_pid(), Some(pid));
        assert_eq!(surface_b.worker_pid(), Some(pid));
        let adapter_a = NcmProviderAdapter::new(surface_a.clone()).unwrap();
        let adapter_b = NcmProviderAdapter::new(surface_b.clone()).unwrap();
        let scope_a = scope("project-shared-worker-a");
        let scope_b = scope("project-shared-worker-b");
        for (id, key) in [
            ("a-first", "alpha first command"),
            ("a-second", "alpha second command"),
        ] {
            let reply = invoke_after_handshake(
                &adapter_a,
                &scope_a,
                ProviderOperation::Observe,
                Some(id),
                observe_value(id, key, "project A only"),
            );
            assert_eq!(reply.terminal.terminal_code(), TerminalCode::Success);
        }

        // B's handshake must not replace A's accepted readiness or descriptor
        // generation. A and B intentionally start this pair at different generations.
        let (receipt_a, generation_a) = ready_parts(&handshake(&adapter_a, &scope_a));
        let (receipt_b, generation_b) = ready_parts(&handshake(&adapter_b, &scope_b));
        assert!(generation_a > generation_b);
        let a = adapter_a.invoke(&call(
            ProviderOperation::Observe,
            &scope_a,
            &receipt_a,
            generation_a,
            Some("a-third"),
            observe_value("a-third", "cargo test", "project A only"),
            10_000,
        ));
        assert_eq!(a.terminal.terminal_code(), TerminalCode::Success);
        let b = adapter_b.invoke(&call(
            ProviderOperation::Observe,
            &scope_b,
            &receipt_b,
            generation_b,
            Some("b-first"),
            observe_value("b-first", "cargo test", "project B only"),
            10_000,
        ));
        assert_eq!(b.terminal.terminal_code(), TerminalCode::Success);
        assert_ne!(a.state_generation, b.state_generation);

        let (receipt_b, generation_b) = ready_parts(&handshake(&adapter_b, &scope_b));
        let (receipt_a, generation_a) = ready_parts(&handshake(&adapter_a, &scope_a));
        let descriptor_before_proof = surface_a.descriptor();
        let instance_before_proof = surface_a.provider_instance_id().unwrap();
        assert_eq!(
            surface_a
                .prove_provider_instance(
                    std::time::Instant::now() + Duration::from_secs(5),
                    Arc::new(|| false),
                )
                .unwrap(),
            instance_before_proof
        );
        assert_eq!(surface_a.descriptor(), descriptor_before_proof);
        // The existing accepted session receipts below must remain usable.
        let recall_b = adapter_b.invoke(&call(
            ProviderOperation::Recall,
            &scope_b,
            &receipt_b,
            generation_b,
            None,
            json!({"query_text": "cargo test", "top_k": 5}),
            10_000,
        ));
        let recall_a = adapter_a.invoke(&call(
            ProviderOperation::Recall,
            &scope_a,
            &receipt_a,
            generation_a,
            None,
            json!({"query_text": "cargo test", "top_k": 5}),
            10_000,
        ));
        assert_eq!(recall_a.terminal.terminal_code(), TerminalCode::Success);
        assert_eq!(recall_b.terminal.terminal_code(), TerminalCode::Success);
        let recalled_a = response_json(&recall_a);
        let recalled_b = response_json(&recall_b);
        assert!(find_string(&recalled_a, "value_text", "project A only"));
        assert!(!find_string(&recalled_a, "value_text", "project B only"));
        assert!(find_string(&recalled_b, "value_text", "project B only"));
        assert!(!find_string(&recalled_b, "value_text", "project A only"));
        assert_ne!(
            handshake(&adapter_a, &scope_a).state_namespace,
            handshake(&adapter_b, &scope_b).state_namespace
        );

        drop(adapter_a);
        drop(surface_a);
        assert_eq!(
            owner.worker_pid(),
            Some(pid),
            "closing A must leave B's worker alive"
        );
        let retained = invoke_after_handshake(
            &adapter_b,
            &scope_b,
            ProviderOperation::Recall,
            None,
            json!({"query_text": "cargo test", "top_k": 5}),
        );
        assert_eq!(retained.terminal.terminal_code(), TerminalCode::Success);
        assert!(find_string(
            &response_json(&retained),
            "value_text",
            "project B only"
        ));
        assert_eq!(
            owner.worker_pid(),
            Some(pid),
            "B must not need a replacement worker"
        );
        drop(adapter_b);
        drop(surface_b);
        let witness = Arc::downgrade(&owner);
        let started = std::time::Instant::now();
        drop(owner);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "last owner uses bounded client teardown"
        );
        assert!(witness.upgrade().is_none());
    }

    #[test]
    fn shared_worker_keeps_project_readiness_generations_and_namespaces_independent() {
        let root = TestRoot::new("shared-project-surfaces");
        exercise_shared_worker_project_surfaces(
            &root,
            WorkerOptions {
                test_double: true,
                ..WorkerOptions::default()
            },
        );
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "requires TRACEDECAY_NCM_WORKER and TRACEDECAY_NCM_REAL_MODEL_ROOT with pinned offline model"]
    fn real_shared_worker_keeps_project_readiness_generations_and_namespaces_independent() {
        let root = TestRoot::new("shared-project-real-model");
        let installed = PathBuf::from(
            std::env::var_os("TRACEDECAY_NCM_REAL_MODEL_ROOT")
                .expect("installed pinned model fixture"),
        );
        assert!(installed.is_absolute());
        std::os::unix::fs::symlink(installed.join("models"), root.0.join("models"))
            .expect("share immutable models only");
        exercise_shared_worker_project_surfaces(&root, WorkerOptions::default());
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
    fn canonical_message_embeds_content_and_deletes_by_same_source_after_restart() {
        let root = TestRoot::new("canonical-message-restart");
        let exact_scope = scope("project-canonical-message");
        let canonical_session = "host-session-canonical-message";
        let text = "Remember the violet telescope calibration procedure";
        {
            let adapter = NcmProviderAdapter::new(surface(&root)).expect("adapter");
            let observed = invoke_after_handshake(
                &adapter,
                &exact_scope,
                ProviderOperation::Observe,
                Some("canonical-message"),
                json!({
                    "observation_kind": "session.message_committed.v1",
                    "payload_contract": "tracedecay.memory.observation.session-message.v1",
                    "canonical_payload": {
                        "version": 1, "provider": "claude", "native_record_kind": "message",
                        "stable_record_id": "private-canonical-message-record",
                        "relations": {"session_id": canonical_session, "project_id": exact_scope.project_id},
                        "facts": [{"kind": "message", "role": "assistant", "content": [{"type": "text", "text": text}]}]
                    }
                }),
            );
            assert_eq!(observed.terminal.terminal_code(), TerminalCode::Success);
            let recalled = invoke_after_handshake(
                &adapter,
                &exact_scope,
                ProviderOperation::Recall,
                None,
                json!({"query_text": text, "top_k": 5}),
            );
            assert_eq!(recalled.terminal.terminal_code(), TerminalCode::Success);
            let recalled = response_json(&recalled);
            assert!(find_string(&recalled, "key_text", text));
            assert!(find_string(
                &recalled,
                "value_text",
                &format!("assistant: {text}")
            ));
        }
        let adapter = NcmProviderAdapter::new(surface(&root)).expect("restarted adapter");
        // Construction preflights an empty namespace. Loading this persisted
        // namespace refreshes the new surface's generation once before admission.
        let mut reopened_ready = handshake(&adapter, &exact_scope);
        if reopened_ready.terminal.terminal_code() == TerminalCode::StaleIdentity {
            reopened_ready = handshake(&adapter, &exact_scope);
        }
        assert_eq!(
            reopened_ready.terminal.terminal_code(),
            TerminalCode::Success,
            "persisted namespace must be ready after at most one identity refresh"
        );
        let (receipt, generation) = ready_parts(&reopened_ready);
        let recalled = adapter.invoke(&call(
            ProviderOperation::Recall,
            &exact_scope,
            &receipt,
            generation,
            None,
            json!({"query_text": text, "top_k": 5}),
            10_000,
        ));
        assert_eq!(recalled.terminal.terminal_code(), TerminalCode::Success);
        assert!(find_string(&response_json(&recalled), "key_text", text));
        let deleted = invoke_after_handshake(
            &adapter,
            &exact_scope,
            ProviderOperation::DeleteBySource,
            Some("canonical-message-delete"),
            json!({"forget_source_key": exact_scope.session_forget_source_key(canonical_session)}),
        );
        assert_eq!(deleted.terminal.terminal_code(), TerminalCode::Success);
        let recalled = invoke_after_handshake(
            &adapter,
            &exact_scope,
            ProviderOperation::Recall,
            None,
            json!({"query_text": text, "top_k": 5}),
        );
        assert_eq!(
            recalled.terminal.terminal_code(),
            TerminalCode::SuccessZeroResults
        );
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
                "value_text",
                "first outcome"
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
                "value_text",
                "beta outcome"
            ));
        } else {
            assert_eq!(
                beta.terminal.terminal_code(),
                TerminalCode::SuccessZeroResults
            );
        }
    }

    /// Withholds a successful observation from the caller by cancelling only
    /// after the real worker has returned evidence that its effect committed.
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

    #[test]
    fn cancellation_after_commit_returns_effect_unknown_then_retry_is_duplicate() {
        let root = TestRoot::new("effect-unknown");
        let controlled = Arc::new(CancelAfterCommitSurface {
            inner: surface(&root),
            cancel_next_observe: AtomicBool::new(true),
        });
        let adapter = NcmProviderAdapter::new(controlled).expect("construct adapter");
        let exact_scope = scope("project-effect-unknown");
        let ready = handshake(&adapter, &exact_scope);
        let (receipt, generation) = ready_parts(&ready);
        let observation = observe_value(
            "source-effect-unknown",
            "cancellation command",
            "cancellation outcome",
        );
        // The real worker commits before the decorator cancels this call.
        // No scheduler-dependent deadline decides whether dispatch happened.
        let unknown = adapter.invoke(&call(
            ProviderOperation::Observe,
            &exact_scope,
            &receipt,
            generation,
            Some("observe-effect-unknown"),
            observation.clone(),
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
            observation.clone(),
            10_000,
        ));
        assert_eq!(retried.terminal.terminal_code(), TerminalCode::Success);
        assert_eq!(
            retried.terminal.committed_effect().state(),
            CommittedEffectState::Duplicate
        );
        let replayed = invoke_after_handshake(
            &adapter,
            &exact_scope,
            ProviderOperation::Observe,
            Some("observe-effect-unknown"),
            observation,
        );
        assert_eq!(replayed.terminal.terminal_code(), TerminalCode::Success);
        assert_eq!(
            replayed.terminal.committed_effect().state(),
            CommittedEffectState::Duplicate
        );
        let recall = invoke_after_handshake(
            &adapter,
            &exact_scope,
            ProviderOperation::Recall,
            None,
            json!({"query_text": "cancellation command", "top_k": 5}),
        );
        assert_eq!(recall.terminal.terminal_code(), TerminalCode::Success);
        let recalled = response_json(&recall);
        let candidates = recalled["Candidates"]["candidates"]
            .as_array()
            .expect("recall candidate array");
        assert_eq!(candidates.len(), 1, "observation must commit exactly once");
        assert_eq!(candidates[0]["value_text"], "cancellation outcome");
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
