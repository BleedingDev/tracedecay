Unapplied NoChange receipt draft audit

Scope: additive successful verified-no-effect terminal. No producer, abort,
policy, catalog, effect-class, store, or anchor behavior changed.
No Cargo, live edits, commit, or generator execution performed.

Five exhaustive EffectTermination conversions require changes across four
files. Every added NoChange arm maps to OperationTermination::Completed.

Current Rust usage inventory: 37 files.
- crates/tracedecay-automation-runtime/src/automation/effect_runtime/journal.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay-code-index-runtime/src/git_transactions/service.rs: operation_termination at baseline line 880: one exhaustive match.
- crates/tracedecay-contracts/src/git/transactions.rs: expected_operation_termination at baseline line 595: one exhaustive match.
- crates/tracedecay-contracts/src/lib.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay-contracts/src/remote/auth.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay-contracts/src/remote/capture_protocol.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay-contracts/src/remote/recovery/service.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay-contracts/src/remote/replay.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay-contracts/src/result/envelope.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay-contracts/src/result/mod.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay-contracts/src/result/problem.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay-contracts/src/result/receipt.rs: From<EffectTermination> at baseline line 131: one exhaustive match; enum and validation owner.
- crates/tracedecay-contracts/src/retained_receipts.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay-contracts/src/retained_surfaces/sdk/results/automation/outer_partial.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay-contracts/src/retained_surfaces/sdk/results/automation/tests.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay-contracts/src/retained_surfaces/service.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay-contracts/tests/effect_receipts.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay-daemon-protocol/src/contract.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay-daemon-service/src/invocation.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay-daemon-service/src/invocation/administrative_effect.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay-daemon-service/src/invocation/configuration/settlement.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay-daemon-service/src/invocation/work/outcome.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay-daemon-service/src/invocation/work/workflow_effect_journal.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay-sdk/src/codegen.rs: Hardcoded TypeScript EffectTermination union line 1029: additive no_change only.
- crates/tracedecay-source-edit/src/control.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay-source-edit/src/execute.rs: Cancellation/timeout-only control_stop_outcome match line 186 retains wildcard; no change.
- crates/tracedecay-source-edit/src/reconcile.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay-source-edit/src/records.rs: Baseline lines 178 and 248: two exhaustive matches. Cancellation/timeout-only match line 132 retains its wildcard.
- crates/tracedecay-source-edit/src/rollback.rs: Cancellation/timeout-only match line 77 retains wildcard; no change.
- crates/tracedecay/src/application_surface/retained_http_identity_tests.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay/src/daemon/automation_effect/journal/tests.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay/src/daemon/remote_protocol.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay/src/daemon/retained_owner/session/retained_effect_tests.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay/src/mcp/tools/handlers/retained_timeout_dispatch_tests.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay/src/tracedecay/edits/execute_tests.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay/src/tracedecay/edits/reconcile_tests.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.
- crates/tracedecay/src/tracedecay/edits/test_support.rs: No exhaustive EffectTermination match; construction, comparison, import, or type reference only.

Generated artifacts drafted by exact additive edits, NOT regenerated:
- sdks/typescript/src/types.ts: common terminal union.
- sdks/typescript/src/operations.ts: EffectTermination_2 alias and schema enum.
- dashboard/codegen/schemas/dashboard-contracts.schema.json: EffectTermination enum.
- dashboard/src/contracts/generated.ts: alphabetically sorted Zod enum.

Later generator verification, when the root authorizes Cargo:
- sdks/codegen/generate.sh (cargo run --manifest-path sdks/codegen/Cargo.toml --bin generate -- <repo-root>).
- scripts/check-sdk-codegen.sh invokes the SDK generator and verifies no SDK source diff; account for existing dirty SDK changes.
- dashboard npm run contracts:check invokes codegen/src/cli.ts --check. Its exporter uses cargo test -p tracedecay-dashboard-api --lib contract_schema::tests::writes_dashboard_contract_schema -- --ignored --exact, then compares fresh schema and generated Zod source. Retain required isolated target/data environment.

Checks completed on the draft only:
- git apply --check --whitespace=error-all passed against live worktree.
- rustfmt --check passed for all six proposed Rust files.
- JSON parses; entire dashboard schema differs by only no_change enum entry.
- All ten live files remain byte-identical to captured baseline.

Tests drafted, not executed:
- no_change_effect_has_a_distinct_wire_state_and_completed_operation.
- no_change_is_a_completed_admitted_result_without_commit_proof (serde receipt round-trip, completed execution, Reconciled result, retained identities).
- no_change_rejects_committed_state_or_external_proof (each proof and both).
- completed_still_requires_committed_state_or_external_proof (None/None rejected, each proof and both accepted).
- Existing effect_unknown test retained; its receipt fixture is extracted for reuse.
