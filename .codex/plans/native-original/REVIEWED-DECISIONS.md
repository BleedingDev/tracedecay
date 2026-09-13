# Reviewed decisions for restoring original Native

## Active PR707 reference and protected implementation

Use unmodified upstream PR707 tip `57006f60cb45bcee8487e73a40d4fad1a12ee2b6` as the active original reference. Candidate metadata is audited merge `1fe250fed7ca615f1dfdcd580befeb276328e910` on the continuously evolving `feat/pluggable-memory-providers-v2` worktree. Preserve PR707 Native/session/LCM/runtime/storage improvements; do not roll them back. The b3 revision `b3b43410e47115056f2066449aafa1822bbb6049` and product head `571daf3a9612e5247443e4da3a107b542686c1ef` remain historical audit evidence, including their original checkout.

Protect the whole original memory implementation: `tracedecay-session-memory` (facts, memory, tracking, sessions and transcript services), `tracedecay-lcm`, original session/runtime/ingestion and retained LCM services, fact/store contracts, database authority, and `tracedecay/src/tracedecay/facts.rs`. No original algorithms, tests, helpers, schemas, transactions, ranking, summaries or lifecycle changes. Product wiring changes only inside each node's exact scope. The existing `pub(super)` visibility difference in `tracedecay-store-runtime/src/retained_memory.rs` is recorded; it does not authorize further changes there.

The detailed source inventory and exceptions are defined by [SOURCE-BOUNDARY.md](SOURCE-BOUNDARY.md). Existing shared host extensions are not falsely claimed byte-identical to 570. The active retrieval-anchor gate is a completed read-only equality proof: both 570 and candidate blobs are `d976e2a23c19441a60412420a1db5e68f7ccc5ca`; no source write is authorized. No new original implementation is authorized.

## Full Native and the common interface

Native is the complete original memory application, including facts, sessions and LCM. The common advisory protocol is one projection, not its feature definition. Preserve existing typed fact/session/LCM routes to original owner-bound services through Native composition. Do not funnel every operation into Recall or invent generic lifecycle equivalents.

Reuse existing public ports and contracts. Add a product-side interface only for a demonstrated connection gap, outside protected code. Every original public operation and background responsibility needs a concrete original route in the completed coverage map.

A generic snapshot, source deletion, maintenance or observation request with no exact original equivalent is typed unsupported. Losing an operation that original Native supports is a failed restoration. Do not advertise staged behavior as an original capability.

Preserve the existing per-registration common-profile bit: Native is not forced into the common profile; NCM retains its current validation. Preserve pinned identity/revision, scope, cancellation/deadline, readiness and explicit fallback. No provider-name inference, new version or blanket effect change.

## Operation map

| Boundary | Required behavior |
| --- | --- |
| Direct retained explicit fact Search | Use the original retained operation, including nonempty retrieval recording, refreshed projections, idempotency and original read-only/degraded outcomes. |
| Automatic context and MemoryApplication search/probe/related/reason | Use the corresponding original read service and query fields. Do not add explicit-search telemetry. Keep scores, order, provenance, omissions and failures. |
| Add/update/remove/supersede/feedback/curation/privacy/automatic facts | Keep original typed commands, ownership, transactions, trust, lineage, receipts and scheduling. Verify a settled FactPromotion; never commit it twice. |
| Session ingestion/retrieval/refresh/task-session lookup | Keep canonical host ingestion and original session services, scope/cursors/freshness/coverage and durable refresh behavior. |
| LCM load/search/describe/expand/expand-query and ingest/compact/status/doctor/retention | Use the original services, protected raw/payload authority, summary lineage and lifecycle. |
| Generic session/source/test/feedback observations | Canonical ingestion and journaling continue. Native claims no new staged consequence; use typed unsupported absent an exact original provider operation. NCM retains authorized observation delivery. |

An inaccessible required operation is a product connection problem for its interface owner. It does not create a writer lane inside original code.

## Model-visible context

Native selection preserves original context behavior at the original host boundary. The existing canonical `memory_matches` path already invokes Native. Use that path once; do not append another Native query/result that changes effective limits, ordering or model-visible facts. Identify this route through typed routing evidence, not a fabricated empty-success provider invocation. Independently invoked generic Native recall still delegates to its corresponding original read service.

Preserve existing host admission/provenance protections. Record changed omissions as differences; do not call them original behavior. Do not redesign token budgets, memory injection or generic duplicate policy.

NCM retains its existing selected-provider contribution. Canonical facts/session/LCM remain shared host authorities. Selection does not shut them down or stop configured NCM observers. Comparisons separate shared canonical contributions from provider output; shared Native output is not isolated NCM quality.

## Remove the substitute and preserve saved data

Remove Native's StagedSession classification, staged file opening/commands, custom scorer, normal merged results, common-mode empty canonical page, staged provenance, lifecycle receipts and generation. Remove the added staged implementation and obsolete staged tests once replacement coverage exists. Native Mod is not an option or hidden supplement.

Leave persisted staged files and sidecars untouched. Preserve canonical facts, sessions, journal/cursors, receipts and settings. No quarantine, migration, copy, backfill, dual-write, new replay or cursor reset. ResetRequired remains only at original store boundaries.

## Verification and authority

Compare a separately built, unmodified 570 reference with restored Native through corresponding production entry points and equivalent isolated state. Mocks or two paths sharing the changed backend cannot establish original equivalence. Compare relevant public results and semantic state/receipts, including reopen, scope and failure behavior; not physical SQLite/WAL bytes or every table.

Require original source preservation, complete behavior coverage and real host delivery. Native/NCM conformance and usefulness are separate findings. Preserve the earlier intermittent NCM result; no tuning or unproved fix.

This root-reviewed decision supersedes provisional audit suggestions: no original-code wrappers, staged supplement, migration/use-audit gate, universal mutation-on-recall, forced generic lifecycle mapping or unresolved baseline.

Execution authorized by the user on 2026-09-11: execute the validated plans now. Root marks nodes complete only after review. Root orchestrates and reviews. Every execution, test and review agent uses `gpt-5.6-luna`, `reasoning_effort=max`, a fresh bounded handoff and no child agents. Root reviews diffs before releasing dependent writers and before any future push.

## Historical privacy restoration record and active gate

The earlier b3-to-571 acceptance of `crates/tracedecay-privacy/src/detector_kernel.rs` and the typed Claude history source-field design remain historical. Under active 570, the detector blobs already match (`9ce4488a34c5bd121d43c35c33f1daf925ab38ba`); rn-privacy-audit and rn-privacy-restore therefore perform read-only current seam review before rn-session-delivery. Generic admission, original Native and LCM privacy stay unchanged. `observation_journey.rs` remains ordered behind the active privacy gate.

## Current user-directed readiness scope

The latest request requires a plan to complete Native/NCM verification, repair intermittent NCM recall and make semantic retrieval operational. READINESS-PLAN.md and the explicit new node scopes supersede preservation-only instructions that would leave NCM misses permanently unresolved. Only a demonstrated, reviewed causal NCM bug repair is permitted; model/ranking tuning, namespace changes, migrations and unrelated features remain excluded. Semantic code-search lifecycle/setup repair does not authorize changes to original Native fact/session/LCM algorithms. Planning launches no implementation agents. Stay in Codex and use available native models/tools; no other agent CLI may be launched without an explicit cross-host request.
