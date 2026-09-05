# ncm-rs-012 — Implement the real native multilingual text encoder

Status: planned, not executed.
Owner: Runtime / embedding.
Dependencies: ncm-rs-002, ncm-rs-004.
External gates: none.

## Objective

Ensure production uses actual model inference and the same input representation as the reference.

## Owned paths

- `crates/tracedecay-memory-ncm-runtime/src/embedding/`
- `crates/tracedecay-memory-ncm-runtime/tests/real_embeddings.rs`
- `product/ncm/reference/embedding-manifest.json`

## Implementation

1. Pin sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2 model revision, tokenizer, weights/export, pooling, normalization and truncation. Use real native Rust inference through the reviewed backend; Python must not be launched by production.
2. Compare token IDs, attention mask, masked mean pooling and normalized 384D outputs against Python. The primary model card describes max_seq_length=128; do not substitute 512 based on unrelated host settings.
3. Load only verified local artifacts during operations. Explicit installation may fetch approved artifacts, but handshake/recall cannot download models or silently fall back online.
4. Port the inspected EmotionExtractor behavior only as a versioned heuristic; preserve explicit four-channel inputs and neutral defaults. Never call heuristic salience a measurement of a human emotional state.

## Acceptance

1. English/Czech, code identifiers, Unicode, empty input and truncation-boundary fixtures pass with real weights.
2. Missing model, corrupt tokenizer, dimension mismatch and incompatible projection identity return typed not-ready/incompatible outcomes.
3. Hash/random/token-count stand-ins are confined to named unit-test doubles and cannot satisfy real-model or release gates.
4. Warm/cold inference, cancellation/kill response, model memory and artifact identity are measured.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-runtime --test real_embeddings --locked

## Sources

- [S02: Biomem configuration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/config.py)
- [S06: Biomem projections](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/projections.py)
- [S08: Biomem real text embedding and emotion extraction](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/embedder.py)
- [S18: MiniLM primary model card](https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
