# Biomem-based Rust NCM backend

The standalone backend is accepted only after the checker runs the real worker and adapter
journey with the pinned model. That result does not establish shipped host behavior, active
recall usefulness, or release readiness; those require their own production journeys.

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
backend acceptance mounts or registers it in the host. The optional daemon observer mount is
a separate integration.

## Build the production worker

From the repository root:

```bash
bazel build //product/ncm:worker_bundle //crates/tracedecay-cli:tracedecay
bazel cquery //product/ncm:worker --output=files
```

The query prints the worker's path in Bazel's output tree. Its directory also contains
`worker-manifest.json` and `model-acquisition-manifest.json`. The worker uses the production
`real-encoder` feature. `--test-double` identifies itself with the literal `test-double/hash`;
it cannot satisfy backend acceptance. Keep Bazel's output tree and dependency caches on the
configured external volume when they exceed the workspace's disk limit.

The checked-in `product/ncm/reference/worker-manifest.json` has `"targets": []` and admits no
executable. Bazel first compiles the worker against that empty trust root, then passes its exact
bytes to `scripts/product/ncm/build-worker-bundle.py`. The bundle writer pins the worker name,
protocol version and identity, target triple metadata, byte length, and SHA-256. Host runtime
libraries embed this generated manifest as a declared build input. The worker does not embed its
own pin, so rebuilding it does not create a hash dependency cycle.

Production launch checks the executable and its regular sibling `worker-manifest.json` against
the host's embedded trust root before creating a child process. A missing, unsupported, or
mismatched artifact returns typed worker-unavailable state and performs no download or
replacement. Ship the three bundle files together and rebuild the host whenever the pinned
worker changes. A generated worker pin proves artifact identity; it does not prove that the local
model is installed or that NCM recall is ready.

The machine-readable release policy is
`product/ncm/reference/worker-platforms.json`. It deliberately advertises the NCM worker only for
the `aarch64-macos` release target. This policy permits a separately built, verified sidecar for
that target; the empty checked-in manifest does not supply one. Other release rows remain
`native-only` for NCM. A local build does not change the supported release matrix, and compiling
portable Rust code on another target does not establish released worker support.

The policy also records the package boundary: the worker is a separate sidecar, the standard CLI
archive does not include it, and every installed worker must carry `worker-manifest.json` beside its
executable. A supported target therefore means that its separately distributed worker identity is
pinned and admitted; it does not silently turn a CLI-only archive into an NCM bundle.

The policy is checked with:

```bash
python3 scripts/product/ncm/check-worker-platform.py
```

The release feature gate runs the same matrix check through
`scripts/check-distribution-feature-wiring.py`. Its `runtime_policy` entry
must name the provider's `rust-backend` feature, the runtime `real-encoder`
feature, the separate worker manifest, and the pinned embedding manifest. The
gate rejects an NCM host capability when that feature chain is incomplete or
when any current release target other than `aarch64-macos` claims the worker;
those targets must remain explicitly Native-only. It also audits both release
workflows for the arm64-only worker build, manifest companion archive,
post-extraction manifest comparison, worker byte/SHA-256 checks, and archive
checksum publication.

Adding another supported target requires the actual worker bytes, their byte count and lowercase
SHA-256 in `worker-manifest.json`, plus the matching release-target policy entry. No placeholder
hash or source-only capability is accepted. The standard CLI archives do not acquire an NCM worker
sidecar implicitly; the policy checker rejects a release matrix that advertises a worker without the
matching pin and manifest sidecar requirement.

## Acquire the pinned model locally

The worker opens verified local models offline. Model acquisition is an explicit operation
through the shipped CLI, which calls `tracedecay_memory_ncm_runtime::embedding::install` after
project, profile, state-root, and confirmation checks. Select a registered project and a dedicated
absolute state root. Put large model and build caches on the configured external volume:

```bash
bazel run //crates/tracedecay-cli:tracedecay -- \
  --yes ncm install-model --path "$REGISTERED_PROJECT" \
  --state-root "$TRACEDECAY_NCM_REAL_MODEL_ROOT" --json
```

Installation writes an acquisition receipt and preserves observer configuration, Native state,
and recall routing. When the observer is enabled, its configured state root must match and a
live profile daemon must be stopped before model mutation. A missing model remains typed
unavailable state during ordinary worker startup.

The installer publishes `models/ncm-encoder-manifest.json`, resolves the exact Xenova snapshot
`2c4055b12046f11709e9df2c122e59ffbdc2f900` under
`models/models--Xenova--paraphrase-multilingual-MiniLM-L12-v2/`, and verifies every byte. The
revision is backed by the tracked backend receipt referenced by the machine-readable manifest:

| Relative file | Bytes | SHA-256 |
|---|---:|---|
| `onnx/model.onnx` | 470268510 | `185ae63f47e17a7e8d30d0e6a3cde6a6e4b79bc5b81666ecffc279a6856ca113` |
| `tokenizer.json` | 17082913 | `b60b6b43406a48bf3638526314f3d232d97058bc93472ff2de930d43686fa441` |
| `config.json` | 673 | `05b570bff786faa5c4604152aa16f19f77ed6dfc31e47dd0f3dd987078693ac7` |
| `special_tokens_map.json` | 280 | `06e405a36dfe4b9604f484f6a1e619af1a7f7d09e34a8555eb0b77b66318067f` |
| `tokenizer_config.json` | 496 | `3f5961b9ac86288cccdb97f32fb848d6187c78e1603958c53f3ea1f296b7d8a2` |

The frozen model profile is `paraphrase-multilingual-MiniLM-L12-v2`, 384 dimensions, 128-token
maximum, masked mean pooling, and L2 normalization. Runtime opening reads the five verified files
from this exact state root through fastembed's user-defined model API; it never asks fastembed to
resolve a repository or use ambient caches. `HF_HOME`, `HF_ENDPOINT`, and `FASTEMBED_CACHE_DIR`
are rejected when the NCM runtime is opened or installed, so an ambient cache or mirror cannot
silently replace the admitted bytes. See
`product/ncm/reference/embedding-manifest.json` for the machine-readable pin.

## Experimental daemon observer

A build with `memory-provider-host` can mount the real NCM worker beside Native as an observer.
When enabled, the daemon invocation retains one `RustNcmWorkerOwner` bound to its profile, worker
binary, and state root. Enabled project surfaces reuse its serialized worker/model process, while
each mount keeps its own descriptor and accepted readiness. Conflicting profile or configured
path values are refused. Closing a project retains the daemon owner; releasing the final owner
uses the existing bounded worker-client shutdown. Declaring the observer starts no worker or
model. Its existing delivery thread performs one bounded attempt to prove the real worker's
identity. With Native selected, startup does not wait for that proof or NCM replay. Declaration
alone establishes no readiness.

The canonical project setting `memory.provider_ncm_observer.v1` is JSON text and defaults to
`{"mode":"disabled"}`. To enable it, commit an enabled document through the configuration API:

```json
{
  "mode": "enabled",
  "worker_binary": "/absolute/path/to/tracedecay-ncm-worker",
  "state_root": "/absolute/path/to/ncm-state"
}
```

Both paths must be absolute; parent traversal is refused. Install the pinned model using the
existing installer into that same `state_root` first, so its artifacts are under
`state_root/models`. The daemon does not download or copy models. Restart the daemon after
changing this setting or installing a previously unavailable worker/model.

Native is enabled and selected by default; NCM is disabled. Enabling NCM leaves recall routing
unchanged. To select its advisory recall, explicitly set `memory.provider_recall_routing.v1` to
`{"active_provider":"ncm","fallback":null,"degradation":null}` through the configuration API and
restart the daemon. NCM participation is independent of `memory.provider_native_enabled.v1`.
The current provider choices are Native and NCM; OCEAN is not implemented.

Context queries with NCM selected retain canonical Native `memory_matches` and add NCM
`advisory_provider_memory`. Benchmark this arm as Native + NCM. `include_memory:false` suppresses
both contributions for that request; canonical capture and enabled observers continue.

NCM receives canonical commits through its bounded observation journey. Under the canonical
store data root, it uses `memory-observation-ncm-journal-v1.sqlite3` with durable replay cursors and
receipts. Canonical capture remains independent of provider selection. NCM runtime state lives
under `state_root/namespaces/<bare-64-hex-namespace>`, with the existing exact-scope binding.
A missing worker or model reports typed unavailable readiness, produces no successful observer
receipt, and leaves undelivered observations unacknowledged. A failed bootstrap remains
unavailable until the daemon is recreated; it does not retry in the background. With Native
selected, Native remains available.

The real Claude and Codex host fixtures require the production worker built above and the
installed model root. On Unix, including macOS, run both from the repository root:

```bash
export TRACEDECAY_NCM_WORKER="$PWD/$(bazel cquery //product/ncm:worker --output=files)"
bazel test //crates/tracedecay-cli:product_memory_provider_claude_host_journey \
  --test_env=TRACEDECAY_NCM_WORKER \
  --test_env=TRACEDECAY_NCM_REAL_MODEL_ROOT --test_output=all \
  --test_arg=real_ncm_observer_receives_shipped_ \
  --test_arg=--ignored --test_arg=--test-threads=1
```

The two tests drive shipped Claude and Codex hooks, check canonical capture and NCM receipts, and
keep Native selected for context. They isolate mutable provider state and share only installed
model artifacts. A real host pass requires both tests to finish successfully with those artifacts;
compilation alone does not establish it.

## Run the backend checker

Choose an absolute scratch parent on the external volume. The checker creates and removes its
own temporary worker, model, and provider state there; the original verified model is preserved.
It leaves a runtime receipt under the scratch parent's `receipts` directory, or `--receipt-dir`.

```bash
bazel run //scripts:product/ncm/check-backend.py -- \
  --repo . --model-root "$TRACEDECAY_NCM_REAL_MODEL_ROOT" \
  --state-root "$NCM_SCRATCH_ROOT"
```

To acquire a missing model first, add `--install-model --project-root "$REGISTERED_PROJECT"`.
The selected project and profile must already be registered through the normal CLI journey.
Absence is a blocker, never a skip.

The checker builds the declared Bazel bundle and queries its actual output paths. It verifies
worker size and SHA-256 against the same generated manifest embedded by the host, verifies all
five pinned model files, and runs maintained core/runtime/adapter tests with nonzero assertions.
It executes the real encoder conformance test directly from Bazel's test executable with an empty
`PATH`, the production worker, and temporary model/state roots. That test asserts observe,
consolidate, persisted recall after restart, deletion, absence after restart, replay refusal, and
stale snapshot restore without resurrecting deleted data. A separate undeleted record proves
that restore preserved useful state.

There is no public STM-mask seam at the engine/adapter boundary. The real test proves LTM
activity through consolidation, inspection, restart, and recall. The receipt records this limit and
actual command results. A missing artifact/model, empty test selection, or failing assertion
produces exit code 2 with a blocked receipt.

## Production adapter requirements

The host integration owner must preserve these boundaries:

- Construct one `RustNcmWorkerOwner::new(RustNcmConfig { worker_binary, state_root, worker_options })`
  per daemon invocation/profile, and retain it in an `Arc`. Create each project surface with
  `RustNcmSurface::from_production_worker(Arc::clone(&worker))` to declare pinned identity without
  a startup preflight. Run `prove_provider_instance` once on the retained delivery thread before
  observation delivery; descriptors and accepted readiness remain local to each mount.
  `worker_binary` and `state_root` must be absolute; production builds do not expose
  `WorkerOptions::test_double` (the field is available only with `test-transport`).
- Wrap the surface with `NcmProviderAdapter::new(Arc::new(surface))`; do not bypass exact-scope,
  sanitization, readiness, terminal, or payload-containment checks.
- Keep `NcmNamespace::from_exact_scope` unchanged, including `agent_session_id` and
  `resolved_scope_digest`. Cross-session reuse requires an explicit scope policy.
- Re-handshake after every committed mutation. The current adapter retires readiness before
  mutation, and `expected_state_generation` is the worker `CommitSequence`.
- Negotiate all current capabilities exactly: Health, Observe, Recall, Feedback, Maintenance,
  Inspection, Correction, DeleteBySource, SnapshotExport, and SnapshotRestore. Replay remains typed
  `CapabilityUnsupported` in v1.
- Set host snapshot limits high enough for the file-backed snapshot transport. A one-record raw
  snapshot is about 1.1 MiB and its provider JSON byte array about 3.3 MiB; use the frozen
  `snapshot_bytes` ceiling of 256 MiB rather than the 1 MiB normal reply-frame ceiling.
- Reuse the retained supervised worker across projects with the same profile and configured paths;
  conflicting bindings are refused until daemon restart. The worker performs no model download;
  installation is an explicit preflight and normal open is verified-local/offline.
- Preserve provider ID `ncm`, algorithm profile `ncm-biomem-rs.v1`, projection digest, encoder model
  and artifact digest, epoch, and commit sequence in readiness/receipt identity checks.
