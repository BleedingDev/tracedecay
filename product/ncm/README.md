# Biomem-based Rust NCM backend

**Status label after a passing backend receipt:** `backend-accepted, host-not-yet-integrated`.

This is never a `demo-ready` claim. The standalone worker and adapter are accepted only when
`scripts/product/ncm/check-backend.py` exits 0 and writes a passing receipt for the exact source
commit. Host mounting, active recall, usefulness, packaging, and release remain tasks 023–026.

## What this package is

The backend is an independent Rust implementation of the frozen `ncm-biomem-rs.v1` contract,
using Biomem commit `500847ff65b5d9548b3826fa29bf3ccf8d221147` as the executable reference.
It consists of:

- `tracedecay-memory-ncm-core`: deterministic f32 kernels and state;
- `tracedecay-memory-ncm-runtime`: pinned MiniLM inference, SQLite durability, worker wire/client,
  deletion, snapshots, maintenance, and recovery;
- `tracedecay-memory-provider-ncm` with opt-in feature `rust-backend`: the existing
  provider-neutral adapter backed by the Rust worker.

The provider ID remains `ncm`. The implementation version is
`ncm-biomem-rs.v1+<canonical-config-sha256-prefix>`. The feature is default-off and nothing in this
backend acceptance mounts or registers it in the host.

## Build the production worker

From the repository root:

```bash
RUSTC_WRAPPER= cargo build --locked \
  -p tracedecay-memory-ncm-runtime \
  --bin tracedecay-ncm-worker
```

The production artifact is `target/debug/tracedecay-ncm-worker` unless `CARGO_TARGET_DIR` is set.
Do not pass `--no-default-features`: the default `real-encoder` feature is the production shape.
`--test-double` is test transport only and identifies itself with the literal `test-double/hash`;
it cannot satisfy backend acceptance.

## Acquire the pinned model locally

The worker never downloads a model. The only permitted download path is
`tracedecay_memory_ncm_runtime::embedding::install`, which the backend checker invokes with
`--install-model`. Choose a dedicated absolute model root:

```bash
export TRACEDECAY_NCM_REAL_MODEL_ROOT="$PWD/target/ncm-backend-model-root"
python3 scripts/product/ncm/check-backend.py \
  --model-root "$TRACEDECAY_NCM_REAL_MODEL_ROOT" \
  --install-model
```

The installer publishes `models/ncm-encoder-manifest.json`, resolves the Xenova snapshot under
`models/models--Xenova--paraphrase-multilingual-MiniLM-L12-v2/`, and verifies every byte:

| Relative file | Bytes | SHA-256 |
|---|---:|---|
| `onnx/model.onnx` | 470268510 | `185ae63f47e17a7e8d30d0e6a3cde6a6e4b79bc5b81666ecffc279a6856ca113` |
| `tokenizer.json` | 17082913 | `b60b6b43406a48bf3638526314f3d232d97058bc93472ff2de930d43686fa441` |
| `config.json` | 673 | `05b570bff786faa5c4604152aa16f19f77ed6dfc31e47dd0f3dd987078693ac7` |
| `special_tokens_map.json` | 280 | `06e405a36dfe4b9604f484f6a1e619af1a7f7d09e34a8555eb0b77b66318067f` |
| `tokenizer_config.json` | 496 | `3f5961b9ac86288cccdb97f32fb848d6187c78e1603958c53f3ea1f296b7d8a2` |

The frozen model profile is `paraphrase-multilingual-MiniLM-L12-v2`, 384 dimensions, 128-token
maximum, masked mean pooling, and L2 normalization. See
`product/ncm/reference/embedding-manifest.json` for the machine-readable pin.

## Run the backend-only gate

Use an empty journey root. The model root may be reused after its hashes are verified.

```bash
RUSTC_WRAPPER= python3 scripts/product/ncm/check-backend.py \
  --repo . \
  --model-root "$TRACEDECAY_NCM_REAL_MODEL_ROOT" \
  --state-root "$PWD/target/test-profile/ncm-backend-journey"
```

If the model is not present, add `--install-model`. Absence is a blocker, never a skip.

The checker uses only the Python standard library. It:

1. builds the real-feature worker and checks for `fastembed` and ONNX Runtime artifact strings;
2. launches that same artifact twice and proves through the handshake that a production launch reports the pinned MiniLM identity while only a `--test-double` launch reports `test-double/hash` (the double is a launch flag frozen by task 017, so the literal is present in the bytes of every build);
3. lists every Cargo population before running it and rejects empty selections;
4. executes core/runtime no-default-feature tests and the adapter with `rust-backend`;
5. explicitly executes the normally ignored real-encoder conformance population;
6. runs real observe → consolidate → worker restart → persisted recall → source deletion → absent
   recall → restart → still absent → replay refused → pre-deletion snapshot restore without
   resurrection through `NcmProviderAdapter` and `RustNcmSurface`;
7. runs the journey with `PATH` containing no Python executable;
8. verifies the NCM branch footprint against task ownership from product base
   `25778c7443cd0cfe257da363da01a56ea1d45d3f`;
9. writes `product/ncm/receipts/backend/<source-commit>.json`, including blockers on failure.

There is no public STM-mask seam at the engine/adapter boundary. The LTM-only intent is therefore
shown by explicit consolidation, inspection proving `ltm_active > 0`, restart, and successful
persisted recall. The receipt records this seam limitation rather than claiming an unavailable mask.

Required prerequisite evidence:

- `product/ncm/conformance/receipt.json`;
- `product/ncm/performance/results-sathanovo-2-local.json`;
- `product/ncm/evaluation/results-2026-09-06.json`.

A missing prerequisite, model, mandatory test, artifact identity, journey assertion, or ownership
violation produces exit code 2 and a blocked receipt.

## Exact adapter API requirements for task 023

The host integration owner must preserve these boundaries:

- Construct `RustNcmSurface::new(RustNcmConfig { worker_binary, state_root, worker_options })`.
  `worker_binary` and `state_root` must be absolute; production `WorkerOptions::default()` must not
  set `test_double`.
- Wrap the surface with `NcmProviderAdapter::new(Arc::new(surface))`; do not bypass exact-scope,
  sanitization, readiness, terminal, or payload-containment checks.
- Keep `NcmNamespace::from_exact_scope` unchanged, including `agent_session_id` and
  `resolved_scope_digest`. Cross-session reuse requires the separately authorized task-024 bridge.
- Re-handshake after every committed mutation. The current adapter retires readiness before
  mutation, and `expected_state_generation` is the worker `CommitSequence`.
- Negotiate all current capabilities exactly: Health, Observe, Recall, Feedback, Maintenance,
  Inspection, Correction, DeleteBySource, SnapshotExport, and SnapshotRestore. Replay remains typed
  `CapabilityUnsupported` in v1.
- Set host snapshot limits high enough for the file-backed snapshot transport. A one-record raw
  snapshot is about 1.1 MiB and its provider JSON byte array about 3.3 MiB; use the frozen
  `snapshot_bytes` ceiling of 256 MiB rather than the 1 MiB normal reply-frame ceiling.
- Launch one supervised worker per profile. The worker performs no model download; installation is
  an explicit preflight and normal open is verified-local/offline.
- Preserve provider ID `ncm`, algorithm profile `ncm-biomem-rs.v1`, projection digest, encoder model
  and artifact digest, epoch, and commit sequence in readiness/receipt identity checks.

## Narrow shared-file patch for task 023

Apply this as an owner-reviewed semantic patch on the pinned stabilized host tree, not by replacing
shared files wholesale:

1. Root `Cargo.toml`: add workspace members
   `crates/tracedecay-memory-ncm-core` and `crates/tracedecay-memory-ncm-runtime`.
2. `Cargo.lock`: carry the resolved entries for the two NCM crates and their pinned dependency graph,
   including `fastembed 5.17.3`, ONNX Runtime/`ort`, `rusqlite 0.40.1`, and tokenizer dependencies.
   Regenerate on the joined tree; do not copy an obsolete lockfile over host stabilization changes.
3. `crates/tracedecay-memory-provider-ncm/Cargo.toml`: retain `default = []`, add
   `rust-backend = ["dep:tracedecay-memory-ncm-runtime"]`, and add the optional runtime dependency
   with `default-features = false`. Keep conformance as a dev dependency only.
4. Carry `crates/tracedecay-memory-provider-ncm/src/rust_backend/mod.rs` and its small feature-gated
   exports in `src/lib.rs`. Do not enable the feature by default or register/mount NCM yet.
5. Reconcile, rather than overwrite, the stabilized host ownership/dependency manifests and the
   accepted upstream floor. Rerun this backend checker on the exact joined tree.

The backend commits containing the adapter boundary begin with `e3d36af35`; task 023 must consume
a specifically reviewed source commit/tree and record the joined tree separately.

## Remaining gates

- **023 — host join:** merge onto one exact stabilized host commit, reconcile Cargo/lock and policy
  ownership, and rerun backend plus host checks.
- **024 — Observer mount:** opt in through the registry/supervisor, prove exactly-once observations,
  no prompt/action influence, and authorized later-session reuse without namespace widening.
- **025 — guarded active mode:** prove host admission, provenance, stale/deletion/corruption/timeout
  safety, rollback, and predeclared usefulness thresholds. Native facts remain authoritative.
- **026 — installed release:** package and verify the worker/model acquisition path on every claimed
  platform, test upgrades and disablement, and issue separate fidelity, backend, host-safety,
  platform, and usefulness verdicts.

Until all four are complete on the joined tree, the only valid successful label is
`backend-accepted, host-not-yet-integrated`; it is not demo-ready or release-ready.
