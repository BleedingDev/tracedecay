# Biomem NCM reference oracle

This directory contains an executable, CPU-only Python oracle for the frozen
`ncm-biomem-rs.v1` contract. The oracle imports the unmodified Biomem source at
commit `500847ff65b5d9548b3826fa29bf3ccf8d221147`; it does not modify that
checkout. Generated JSON is explicit: initialized tensors, masks, counters,
operation inputs, and intermediate outputs are serialized rather than inferred
from a seed alone.

## Pinned environment

- Python `3.12.11`
- PyTorch `2.8.0`
- sentence-transformers `5.1.0`
- transformers `4.57.6`
- tokenizers `0.22.2`
- huggingface-hub `0.36.2`
- NumPy `2.5.2`
- Device: CPU
- PyTorch intra-op and inter-op threads: `1`
- Default tensor dtype: `float32`
- Deterministic algorithms: enabled
- Reference source: `~/workspace/bleedingdev/projects/biomem/code/biomem`
- Model: `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`
- Model revision: `e8f8c211226b894fcb81acc59f3b34ba3efd5f42`
- Model max sequence length: `128`
- Pooling: masked mean; final embedding: L2 normalized 384-D vector

The model snapshot is checked locally before export. These SHA-256 digests are
part of the pin and are also stored in `real_embeddings.json`:

```text
1_Pooling/config.json       4be450dde3b0273bb9787637cfbd28fe04a7ba6ab9d36ac48e92b11e350ffc23
config.json                 6300193cb75e01cf80c96decef7187dfb33094d97cc1490b7ead6ff134476e4e
config_sentence_transformers.json
                            b8c64b5cece00d8424b4896ea75b512b6008576088497609dfeb6bd63e6d36b8
model.safetensors           eaa086f0ffee582aeb45b36e34cdd1fe2d6de2bef61f8a559a1bbc9bd955917b
modules.json                8f4b264b80206c830bebbdcae377e137925650a433b689343a63bdc9b3145460
sentence_bert_config.json   70f4448f31320443fe3557cacea5abf2dcc4915dda8c80646becf3bb0aa5a1f
sentencepiece.bpe.model     cfc8146abe2a0488e9e2a0c56de7952f7c11ab059eca145a0a727afce0db2865
special_tokens_map.json     378eb3bf733eb16e65792d7e3fda5b8a4631387ca04d2015199c4d4f22ae554d
tokenizer.json              2c3387be76557bd40970cec13153b3bbf80407865484b209e655e5e4729076b8
tokenizer_config.json       5036ea374ffedd706e3bef33e2e0d6953cb868ef8a490e76e32ba0faa37a6b9b
unigram.json                71b44701d7efd054205115acfa6ef126c5d2f84bd3affe0c59e48163674d19a6
```

Each fixture uses a dedicated deterministic seed (200 through 209 in filename
order), but the seed is only provenance; the fixture includes the resulting
state and inputs. Single-step fixtures declare `atol=1e-6, rtol=1e-5` and
multi-step fixtures declare `atol=2e-5, rtol=2e-4` in the JSON before outputs.

## Exact commands

Run these commands from the NCM checkout root. The pinned virtual environment
is expected at `~/.cache/ncm-oracle/venv`, and the local model snapshot must be
available offline at the pinned revision.

```sh
cd /Users/satan/side/experiments/tracedecay/.worktrees/ncm-biomem-rust-v1
~/.cache/ncm-oracle/venv/bin/python --version
~/.cache/ncm-oracle/venv/bin/python -c 'import torch; print(torch.__version__)'
~/.cache/ncm-oracle/venv/bin/python scripts/product/ncm/reference/export_fixtures.py
~/.cache/ncm-oracle/venv/bin/python scripts/product/ncm/reference/export_fixtures.py
~/.cache/ncm-oracle/venv/bin/python scripts/product/ncm/reference/negative_controls.py
~/.cache/ncm-oracle/venv/bin/python scripts/product/ncm/reference/reference_oracle_test.py
```

To verify that the two exports are byte-identical without canonicalizing JSON,
run the exporter once, capture sorted hashes, run it again, and compare:

```sh
cd /Users/satan/side/experiments/tracedecay/.worktrees/ncm-biomem-rust-v1
hashes() { shasum -a 256 product/ncm/reference/oracle/*.json | sort; }
before="$(hashes)"
~/.cache/ncm-oracle/venv/bin/python scripts/product/ncm/reference/export_fixtures.py >/dev/null
after="$(hashes)"
test "$before" = "$after"
```

The controls must finish successfully and report detected differences for the
no-op blur, constant recall, disabled consolidation, and random projection
mutants. The replay test must report 10 passing tests; the control test must
report 9 passing tests.

## Fixtures

The exporter writes exactly these bounded JSON files under
`product/ncm/reference/oracle/`:

- `projections.json`: initialized seven-projection bundle, four embeddings, and
  all projection outputs.
- `rbf_read.json`: empty through 100-active-center RBF/hybrid reads, intensity
  imbalance, distant query, and tie probe.
- `compound_read.json`: semantic/context reads with terrain absent and present.
- `write.json`: allocation, reinforcement, zero-intensity ignore, and capacity
  exhaustion, including the `sigma_read` write-candidate evidence for D02.
- `write_strength.json`: write-strength math, affect sanitization, keyword
  extraction, presets, and dictionary inputs.
- `terrain.json`: resolution-16 splats, diffusion, samples, reference no-op
  blur, corrected separable blur, merge, and the asymmetric D09 axis probe.
- `merge_prune_normalize.json`: greedy merge, age/usage prune boundaries,
  normalization, and homeostasis.
- `consolidation.json`: fatigue and automatic cadence boundaries plus complete
  top-8-of-12 consolidation state transitions.
- `trace_long.json`: a 60-operation synthetic-embedding trace with per-operation
  tensor digests and complete snapshots at operations 0, 30, and 60.
- `real_embeddings.json`: 16 English/Czech/code/empty/long/emoji inputs with
  token IDs, masks, masked means, and normalized 384-D embeddings.

The exporter rejects any fixture at or above 2 MiB. The generated files include
all dimensions required by the contract, with terrain resolution 16 and small
center capacities where the fixture explicitly bounds state size.

## Known reference behavior and intended corrections

The JSON preserves executed reference behavior, including the confirmed no-op
terrain blur, the `sigma_read` write width, the independently initialized STM to
LTM projection, the reference terrain sampling axis order, and the merge tensor
aliasing captured by the approved deviation records. Corrected v1 behavior is
exported separately where required, notably `blur_corrected` and the
non-aliased merge analytic value. Tie comparisons use the approved D13 tie-group
policy rather than treating unstable `torch.topk` order as portable.

## Not covered

- This is not a Rust implementation and does not prove Rust numerical or wire
  compatibility by itself.
- It does not benchmark latency, memory budgets, worker deadlines, SQLite
  durability, crash recovery, deletion erasure, or provider/namespace policy.
- It does not claim paper conformance beyond the executable Biomem source.
- It does not infer affect, mental state, decoder surprise, or terrain priors;
  keyword extraction is only the optional reference heuristic.
- It does not pin or validate the Rust fastembed ONNX artifact. That artifact is
  a separate delivery and must be recorded in `embedding-manifest.json`.
- It does not encode non-finite JSON inputs; the sanitization fixture records a
  symbolic non-finite probe and the contract's typed-error expectation.
