# rn-ncm-tests — root acceptance

Authoring accepted 2026-09-11. Runtime execution is deferred to rn-build and rn-verify-ncm.

Reviewed changes: new namespace test for profile/project/repository/resolved_scope_digest in crates/tracedecay-memory-provider-ncm/tests/ncm_adapter.rs; remaining worktree/branch/session coverage already exists. Corrected only Replay row in product/ncm/spec/CONTRACT.md to current canonical page replay, fencing/rebuild and receipt semantics. No runtime/model/namespace implementation changes. Worker reported rustfmt and git diff --check pass; no Cargo.

Build filters: `cargo test -p tracedecay-memory-provider-ncm --test ncm_adapter namespace_isolates_`. Real backend tests in integration binary rust_backend include enabled::descriptor_and_handshake_expose_reserved_identity_and_exact_capabilities, enabled::canonical_replay_retries_through_projection_preserve_receipt_and_reject_changed_item, enabled::snapshot_export_and_restore_replace_later_state, enabled::explicit_worker_owner_lifecycle_stops_reaps_and_restarts_same_owner, enabled::shared_worker_keeps_project_readiness_generations_and_namespaces_independent, enabled::cancellation::inflight_handshake_cancellation_reaps_worker_and_recovers, enabled::cancellation::inflight_recall_cancellation_reaps_worker_and_retains_observation, enabled::mandatory_conformance_definition_runs_on_real_adapter, enabled::every_supported_operation_rejects_all_wrong_scope_dimensions. Verify exact discovery/nonvacuous execution.

Existing production host fixtures in product_memory_provider_claude_host_journey (memory-provider-host,test-transport): real_ncm_observer_receives_shipped_claude_hooks_while_native_answers_context; real_ncm_active_recalls_shipped_claude_session_history_with_native_disabled. Shared canonical host journals remain active. Real model prerequisites: absolute TRACEDECAY_NCM_WORKER and pinned TRACEDECAY_NCM_REAL_MODEL_ROOT, Unix, ps, sqlite3.

Earlier intermittent incomplete recall (2/4 then 4/4) remains unresolved; this node makes no quality-fix claim.
