# Rust NCM provider surface

The `rust-backend` feature supplies `RustNcmSurface`, a supervised-worker implementation of the existing topology-neutral `NcmCognitiveSurface`. The feature is off by default. With it disabled, the provider crate has no runtime dependency, exports no `rust_backend` module, and starts no worker.

## Construction

`RustNcmSurface::new(RustNcmConfig)` requires:

- an absolute `worker_binary` path;
- an admitted absolute `StateRoot`;
- `WorkerOptions` controlling launch, restart, encoder mode, and reconciliation.

Production uses the worker's offline pinned MiniLM encoder. Tests set `WorkerOptions::test_double = true`, which selects the deterministic hash encoder explicitly; this identity is rejected by production model gates and is not a production fallback.

The descriptor keeps the reserved provider ID `ncm`. Its version is `ncm-biomem-rs.v1+<config-sha256-prefix>`. Capabilities are limited to health, observation, recall, feedback, maintenance, inspection, correction, deletion by source, and own-format snapshot export/restore. Replay is not advertised and returns a typed unsupported terminal. Limits remain the frozen v1 limits: 256 KiB request, 1 MiB reply, 16 observation items, 16 recall candidates, one concurrent operation, 30 seconds, 256 MiB snapshot storage, and 1,000 inspection items.

## Readiness and state generation

Readiness lifetime belongs to `NcmProviderAdapter`; the backend does not alter it. A mutation retires the accepted readiness before dispatch. The required sequence is:

1. handshake;
2. observe, returning success and the new engine commit sequence as `state_generation`;
3. attempt a second call without re-handshaking, which the adapter refuses as stale readiness;
4. handshake again, admitting the new commit sequence;
5. recall successfully.

Every later mutation follows the same rule. Duplicate delivery uses the same idempotency key and effect-bearing payload, reports `CommittedEffectState::Duplicate`, reproduces the provider receipt, and does not advance `state_generation`.

The ready receipt hashes the opaque namespace, algorithm/config identity, persisted projection digest, encoder model/artifact identity, and state epoch. The challenge response additionally binds that receipt to the adapter's challenge and descriptor.

## Observation text extraction

The adapter accepts only the frozen observation kinds and matching payload contracts. It reads the nested `canonical_payload.payload` object when present, otherwise `canonical_payload` itself. Empty or missing text is rejected.

- `session.message_committed.v1`: role/message-kind/summary to content/message/text/summary.
- `tool.execution_settled.v1`: command/tool name/summary to outcome/result/output/summary.
- `source.edit_settled.v1`: change/path summary to result/diff/content summary.
- `test.execution_settled.v1`: test name/command/summary to outcome/result/summary.
- `diagnostic.observed.v1`: code/diagnostic/summary to message/detail/summary.
- `git.evidence_observed.v1`: commit/ref/summary to message/evidence/summary.
- `native.fact_promoted.v1`: subject/key/summary to fact/value/content/summary.
- feedback and automation outcomes: action/job/summary to outcome/result/summary.

The source is the admitted `forget_source_key`, or the admitted opaque source identity fields when that key is absent. Raw profile, project, repository, worktree, branch, session, request, operation, and readiness identities never enter the worker payload or a validated worker reply.

## Snapshot file transport

The 1 MiB worker reply frame remains unchanged. Snapshot export writes the `ncm-snapshot.v1` JSON bytes atomically (temporary file plus rename) under `<state-root>/namespaces/<namespace>/snapshots/` and returns only the absolute file path, byte length, content sha256, and state generation over the worker pipe. `WorkerClient` verifies that the path resolves directly inside that namespace directory, verifies length and sha256, deletes the file, and replaces the transport metadata with the snapshot bytes before `RustNcmSurface` sees the reply.

Snapshot restore stays inline when its encoded worker request fits the 256 KiB frame. Otherwise `WorkerClient` atomically writes `restore-<id>.json` in the same namespace snapshot directory and sends the path, byte length, and sha256. The worker validates the directory, length, digest, and 256 MiB snapshot budget, consumes the file, and then runs the unchanged restore semantics. Transport files are best-effort removed on client-side failures as well.

At the provider boundary, successful `SnapshotExport` payloads are validated against the negotiated `snapshot_bytes` limit rather than the ordinary `response_bytes` limit. Other replies remain under `response_bytes`. Symmetrically, `SnapshotRestore` request payloads are validated against `snapshot_bytes` rather than `request_bytes`, because the restored snapshot travels inside that request; every other request stays under `request_bytes`.

## Test worker resolution

`tests/rust_backend.rs` resolves the worker once per test binary:

1. if `TRACEDECAY_NCM_WORKER` is set, it must name an absolute executable path;
2. otherwise the test runs `cargo build -p tracedecay-memory-ncm-runtime --bin tracedecay-ncm-worker --no-default-features`, inheriting `CARGO_TARGET_DIR` when present, and uses `<target>/debug/tracedecay-ncm-worker`.

The acceptance recipe is:

```sh
RUSTC_WRAPPER= cargo test -p tracedecay-memory-provider-ncm --features rust-backend --test rust_backend
RUSTC_WRAPPER= cargo test -p tracedecay-memory-provider-ncm --features rust-backend --test ncm_adapter
RUSTC_WRAPPER= cargo test -p tracedecay-memory-provider-ncm --test ncm_adapter
RUSTC_WRAPPER= cargo test -p tracedecay-memory-provider-ncm --test rust_backend
RUSTC_WRAPPER= cargo clippy -p tracedecay-memory-provider-ncm --features rust-backend --all-targets
RUSTC_WRAPPER= cargo clippy -p tracedecay-memory-provider-ncm --all-targets
```

The feature-on integration suite uses the full `NcmProviderAdapter`, not the surface directly. Its negative controls cover stale readiness after mutation, unchanged generation and receipt on duplicate delivery, source deletion, post-commit deadline uncertainty, and rejection of a reply containing the raw project identity.
