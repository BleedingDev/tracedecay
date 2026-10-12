# NCM evaluation runners

These scripts implement ncm-rs-021 without invoking the TraceDecay host. Read
`product/ncm/evaluation/protocol.md` first; it freezes the split, thresholds, budgets, safety
ceilings, and ablations.

## Prerequisites

- Python 3.11+.
- The oracle environment at `~/.cache/ncm-oracle/venv` with torch and sentence-transformers.
- The pinned Biomem checkout at `~/workspace/bleedingdev/projects/biomem/code/biomem`.
- A real-encoder worker built from this checkout.

```sh
RUSTC_WRAPPER= cargo build -p tracedecay-memory-ncm-runtime --bin tracedecay-ncm-worker
```

The worker never downloads a model. Supply an absolute evaluation state root whose `models/`
contains the runtime cache and `ncm-encoder-manifest.json`. Passing `--install-model` builds a transient helper that calls the runtime crate's explicit `embedding::install`, then verifies every byte count and SHA-256 before starting the
offline worker. Production worker operation remains download-free.

## Run

Use a fresh state root for each complete run.

```sh
ROOT="$(pwd)"
STATE="/absolute/path/to/ncm-eval-state"
WORKER="$ROOT/target/debug/tracedecay-ncm-worker"
PY="$HOME/.cache/ncm-oracle/venv/bin/python"

"$PY" scripts/product/ncm/evaluate/ncm_reference_comparison.py \
  --worker "$WORKER" \
  --state-root "$STATE" \
  --install-model \
  --output "$STATE/reference.json"

"$PY" scripts/product/ncm/evaluate/ncm_coding_memory_evaluation.py \
  --worker "$WORKER" \
  --state-root "$STATE" \
  --reference-results "$STATE/reference.json" \
  --output product/ncm/evaluation/results-$(date +%F).json
```

`ncm_reference_comparison.py` executes real-text store/recall, paraphrase, 512-center STM
interference, stale supersession, and real-encoder LTM-only retention after consolidation and
restart. It compares matched projected keys and raw read activation against Biomem with the
persisted Rust matrices injected; torch/Rust RNG byte parity is intentionally not a requirement.

`ncm_coding_memory_evaluation.py` executes all nine checked-in scenarios for Rust NCM, Python
Biomem, no memory, and explicit documentation. Native and joined-host lanes remain typed pending.
The final JSON keeps kernel fidelity, corrections, provider behavior, and observer task benefit in
separate blocks and explicitly forbids an improved-agent-outcomes claim.

## Negative controls

The runners abort if consolidation is omitted from the LTM-only path, if the corrected blur does not
spread an asymmetric impulse, if a hash encoder substitutes for the pinned model, or if a 513th STM
center appears. Admission reports fail their zero safety ceilings when stale, cross-scope, corrupt,
or deleted content is allowed. Empty evidence is indeterminate rather than a safety pass.
