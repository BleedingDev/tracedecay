# ToolResult delivery draft for root review

Frozen implementation patch: `.codex/plans/pm-tool-result-delivery.patch`

SHA-256: `98672b3a9f1907100597bf8dae04c3deafe1b95810f9866edb065f6ec4aa571e`

This is a draft only. No live source was applied. Every existing live file still equals its captured base bytes; all four new live paths remain absent. Exact base/proposed digests are in `pm-tool-result-delivery-proposed/review-manifest.json`.

## Exact implementation ownership

- `scripts/product/memory-comparison/runner.py`
- `scripts/product/memory-comparison/adjudicate.py`
- `scripts/product/memory-comparison/delivery_evidence.py` (new)
- `scripts/product/memory-comparison/test_delivery_evidence.py` (new)
- `crates/tracedecay-memory-evaluation/src/host_retrieval.rs`
- `crates/tracedecay-memory-evaluation/src/host_retrieval/tool_result.rs` (new)
- `crates/tracedecay-memory-evaluation/tests/host_retrieval.rs`
- `crates/tracedecay-memory-evaluation/tests/fixtures/tool_result_v1.json` (new, development-only synthetic bytes)
- `product/evaluation/host-comparison/comparison-capture.schema.json`
- `product/evaluation/host-comparison/comparison-report.schema.json`

## Semantics

`tool_result_v1` represents exactly one actual CLI ToolResult carrier per action delivery. It retains the original raw UTF-8 carrier, its artifact reference/hash/length, the complete parsed result including nontext blocks, and every decoded text block in original content order. Embedded original bytes keep evaluator inputs portable; the artifact path is retained attribution, not filesystem reauthentication. The trusted fixture remains responsible for capturing the actual host boundary.

The Python and Rust parsers independently retain original UTF-8 value/member spans. They reject duplicate decoded object keys, invalid Unicode/numbers, excessive depth/bytes, trailing data, moved spans, and semantic-pointer disagreement. Candidate spans cover the whole original candidate object, including IDs, provenance, explanation and unknown fields. Canonical spans cover original top-level member lexemes in observed order; explicit syntax gaps account for every remaining payload delimiter/whitespace byte. Canonical comparison uses original bytes and attribution without reserialization.

The Rust verifier independently recounts pinned o200k tokens. Full cost is the sum of separate decoded text-block counts, capped at 128000. Advisory cost conservatively charges the entire observed `advisory_provider_memory` member, including its key/colon, receipts and lane prose. Other text blocks are explicitly `unclassified_text`, also conservatively charged without asserting provider authorship. Within each block, original charged spans concatenate without inserted bytes; independent per-block counts then sum, capped at 1024. Canonical and candidate-body counts remain distinct. Actual model-input framing and downstream task benefit remain unmeasured.

The final candidate join uses the actual emitted `provenance_evidence.recall.{trace_ref,item_ref}`. Canonical `recall-item-v1:<provider rank>` resolves one original retained item; trace/request/provider/registration/full-scope metadata and artifact bytes must agree. The final emitted body is separately checked against its exact decoded string and full presentation span. The retained `injected` stage is retained as compiled-stage evidence, not inferred from output presence. No hardened-ID alias is reconstructed. Missing final bindings remain delivered-but-unresolved, with only missing/indeterminate/unverifiable labels permitted. Full retained trace bytes preserve withheld rows and typed producer decisions.

Annotations must bind both the exact full candidate-object digest and a length-prefixed, ordered advisory-block review digest. They must explicitly affirm review of candidate metadata, shared text, source attribution and prohibited claims; digest equality alone is insufficient. Existing frozen-source quote/fact checks and label vocabulary remain in force. A prohibited shared claim prevents a passing query. Unreviewed substantive prose with no candidate is unresolved, including expected-empty queries; it does not create a synthetic candidate or a denominator entry.

The legacy rendered-text verifier remains strict, including exact reconstruction and the whitespace-only framing rule. The Rust outer type adds optional `representation`/`tool_result` fields; new-branch captures must have empty legacy text/section/candidate fields. Existing Rust fixture construction gains only those two `None` fields. Existing metrics, labels, ceilings, 432 case trials, 1344 scheduled queries, actions and paired denominators are unchanged.

## Verification performed

- `python3 -S .codex/plans/pm-tool-result-delivery-proposed/scripts/product/memory-comparison/test_delivery_evidence.py -v`: **23 passed**.
- Python AST parsing and JSON syntax checks: passed.
- `rustfmt --edition 2024 --check` on the proposed Rust verifier and legacy test file: passed (includes parsing/formatting the child module).
- `git apply --check .codex/plans/pm-tool-result-delivery.patch`: passed.
- Added **9 Rust regressions** for strict spans, escaped Unicode, duplicate keys, independent block token recounts, aliased original-rank binding, full-presentation annotation, unresolved joins, compiled-stage separation and control-lane rejection. They were **not compiled or run**, because Cargo/build execution remains owned by root. The synthetic JSON fixture's placeholder token observations are replaced by the pinned tokenizer inside these Rust tests.

No model, real host trial, held-out corpus read/run, Cargo invocation, host fixture/projector edit, documentation-read behavior edit, manifest edit, live apply, commit or push occurred.

## Deliberate follow-up boundary

The current version is one carrier only. Multiple actual CLI results in one action, documentation-read body attribution, typed host-framing classifications, and resolving clean zero-candidate notice surfaces require separate review. A follow-up-only proposal is in `pm-tool-result-delivery.multi-carrier-proposal.md`; it is not part of this patch.
