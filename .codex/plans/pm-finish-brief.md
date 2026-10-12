# Finish interchangeable Native and NCM memory

Baseline: `6010f31e7` on `feat/pluggable-memory-providers-v2` in this checkout. This document is the accepted implementation direction for the adjacent `pm-*.plan.md` files. It is not a runnable plan node.

## Outcome

An operator can explicitly select either `tracedecay.native` or `ncm` for advisory memory through the same configuration, host admission, context, feedback, and lifecycle surfaces. The other provider may be disabled or observe independently. Selecting NCM does not require activating the Native advisory provider. Both implementations pass one behavioral compatibility suite and run through one comparative evaluation and performance harness.

TraceDecay continues to own canonical facts, source and session evidence, scope admission, privacy decisions, and context assembly. Those common authorities remain available under either provider selection. A provider switch cannot relabel canonical fact reads as a fallback or grant a provider permission to change accepted facts.

The matched advisory profile contains provider-local observations and derived memory. Existing canonical project-fact reads already run independently in the host. Keep identical canonical inputs and count each canonical fact/source revision once; Native's optional legacy fact-projection extension is outside this matched profile. Historical provider recall must not mix in current facts as historical evidence. The host owns time-aware admission/withholding of the separate fact contribution and reports missing historical coverage; new canonical-fact history search is outside this completion task.

## What is already implemented

- Both concrete adapters implement `tracedecay_memory_provider_api::MemoryProvider`.
- Native stages committed session observations and recalls them across sessions in the same checkout, preserving original scope metadata.
- Real NCM worker, model, durable namespaces, idempotency, bounded shutdown and recovery exist.
- The optional NCM observer receives real Claude/Codex hooks with independent durable observation journals. Native remains the active recall provider.
- Five hook-delivery tests and both Native/both NCM observer host journeys pass; the four host journeys passed twice with retries disabled. Do not rerun unrelated checks to reconstruct this history.

## Concrete gaps

- `MountableProviderKindV1` in registry `src/lib.rs` exposes only Native; host composition requires Native and injects Native limits and scope bindings into active recall.
- Native's application port currently returns unsupported for provider-local feedback, maintenance, inspection, correction, source deletion, snapshot export/restore, and replay. Existing canonical fact operations are separate and must remain so.
- NCM `rust_backend::translate_payload` reduces recall to query text and top-k; canonical request budgets, exclusions, temporal semantics and complete admissible provenance require a real mapping.
- NCM's exact session namespaces do not by themselves authorize a later session to reuse an earlier session's observations. Native's checkout binding cannot simply be relabeled or its scope fields dropped.
- Existing conformance, corpus, metrics, backend benchmarks and real host fixtures exist, but there is no complete matched Native/NCM host comparison.

The completed [Native audit](pm-native-evidence.md), [NCM audit](pm-ncm-evidence.md), and [host audit](pm-host-evidence.md) identify exact producers and tests. They also found Native exclusion/generation/migration gaps, NCM live-cancellation and response-construction gaps, incompatible host/source identity encodings, delayed-ingest origin ambiguity, and missing durable host fanout for provider-local deletion. Root's reviewed ownership and dependencies are in [pm-orchestration.md](pm-orchestration.md).

## Compatibility target

Compatibility concerns observable semantics, not equal ranking scores or shared database layouts.

| Surface | Required result for both providers |
|---|---|
| Handshake and health | Truthful supported capabilities, scope and identity, resource limits, state generation, unavailable/quarantined state |
| Observe | Same canonical input envelope; durable idempotent provider-local effect; exact accounting for accepted, rejected and unknown effects |
| Recall | Same request schema, all budgets and exclusions honored, deterministic order within one provider state, explicit coverage and typed failures |
| Time | Current/as-of/interval/history semantics from retained validity evidence; never substitute current data for historical requests; privacy deletion wins over history |
| Session reuse | Host-authorized same-checkout replay/share with original session/source attribution; no sibling worktree, branch or unrelated project leak |
| Feedback and correction | Provider-local stable source/candidate attribution, idempotent effects, correction lineage and read-after-write behavior; no implicit canonical fact trust changes |
| Delete by source | Removal from recall and derived state, durable tombstones, replay/snapshot anti-resurrection behavior |
| Inspection and maintenance | Bounded, redacted, truthful state and effects under shared control semantics; algorithm-specific details stay optional |
| Snapshot and replay | Provider-specific versioned state restore plus provider-neutral canonical-history replay for switching; never load one provider's internal snapshot into another |
| Lifecycle | Disable, observe, active, quarantine, restart, cancellation and shutdown work without hidden provider fallback or unbounded child processes |
| Public product path | Same operator and tool surfaces; provider identity appears as selection and attribution, not provider-specific request schemas |

The common profile is a completion requirement. Do not earn compatibility by returning unsupported for a required row, stripping fields until a call succeeds, routing to the other implementation, or implementing a successful no-op. Optional algorithm-specific features must be declared separately. If a row cannot be implemented safely with retained data, report that exact capability as incomplete and finish its producer; do not claim parity.

## Comparison target

- Compare Native and real-model Rust NCM through the same real host path and the existing provider-neutral corpus/metric evaluator. Include no-memory and explicit-documentation controls; Python Biomem remains a backend fidelity reference, not a substitute host lane.
- Pin the same observations, source revisions, session transitions, queries, policy, tokenizer, candidate/context budgets and trial schedule. Common canonical facts/code/session lanes are identical across providers and their cost/contribution is reported separately.
- Preserve provider-native scores without pretending they share a numeric scale. Compare admitted content, source lineage, safety outcomes, task outcomes, token cost, latency and resources.
- Core comparison measures source-grounded retrieval usefulness, safety, provenance, tokens, latency and resources. Downstream model-driven task benefit is an optional separate experiment requiring an authorized runner; it remains explicitly unmeasured otherwise and is never inferred from retrieval rubric checks.
- Freeze development and held-out inputs, labels and thresholds before reading new results. Existing nine scenarios were previously inspected and are regression data; fresh held-out cases must not be mislabeled as unseen.
- Report p50/p95/p99 for cold startup, first recall, warm recall, durable observation, feedback/correction/deletion, replay and shutdown; incremental RSS and peak RSS, worker counts, disk/WAL/snapshot bytes, queue pressure and cancellation completion.
- Start with existing budgets: kernel recall p95 <=25 ms at 4096 LTM centers, warm text recall p95 <=250 ms, durable observe p95 <=500 ms, existing hook wall-clock envelope and shutdown bounds unchanged. Measure the actual host boundary separately from kernel/encoding/IPC. Performance failures stay visible; never increase budgets merely to turn a result green.
- Existing useful-recall precision minimum is 0.60 for resolved positive recalls; safety ceilings are zero for stale harmful recall, scope leakage, deleted-source recall and corrupt-state recall. Indeterminate or vacuous safety results do not pass. Report quality differences honestly; compatible does not mean NCM must win.

## Boundaries and execution

- Continue in this product worktree. Do not change the master checkout, contact upstream, redo prior merges, install a running operator build, change user configuration or restart the user's daemon.
- No dynamic-library ABI, remote provider service, OCEAN implementation, UI redesign, new database authority, or algorithm rewrite is required.
- Existing public contracts and stored Native/NCM data need a compatibility/migration path. Preserve legacy default-off behavior and existing configuration until an explicit supported transition is committed.
- Root owns interface decisions, manifest/lock changes, root composition integration, heavy Cargo/model runs, cross-lane diff review, commits and push. Workers receive exact write scopes, are not alone, and must not revert others.
- One heavy process at a time through cargo-hauler. Check session status before Cargo and attach to existing work. Reuse this checkout's target/model artifacts; isolate mutable fixture state. Run real host/model journeys in their exclusive subprocess group.
- Existing checks are reused; validate changed behavior and contract boundaries. Administrative hashes, receipts, status markers or broad unchanged suites are not prerequisites for useful work. The graph metadata is only the requested execution aid.

## Definition of done

Both provider configurations independently complete the same real Claude/Codex session-A to session-B journey, correction/feedback/deletion and restart/replay/switch/rollback journeys. Required capabilities pass non-vacuous common conformance; observer on/off cannot change selected output. The comparison runner produces real, matched quality/safety/performance/resource results. Operator configuration and dependency checks prove the host is provider-neutral and defaults remain safe. Root reviews and publishes the implementation with explicit measured limitations; no unimplemented required capability is called complete.

The execution graph contains 17 runnable plans and 23 inter-plan dependencies. Four source/protocol lanes are completed. The first production node is `pm-contract`; no production implementation or new model measurement was performed while preparing this plan.

## References

- `product/contracts/memory-provider-v1/` and `product/architecture/adr/ADR-0001-provider-boundary.md`
- `product/architecture/adr/ADR-0002-authority-and-advisory-semantics.md`, `ADR-0007-observer-isolation-and-activation.md`, `ADR-0010-native-provider-parity-projection.md`
- `product/evaluation/README.md`, `product/ncm/evaluation/protocol.md`, `product/ncm/performance/README.md`
- `product/ncm/plan/tasks/ncm-rs-024.md` and `ncm-rs-025.md`; use their substantive outcomes, not stale planned status or receipt gates

`CONTEXT.md`, `docs/adr/` and `docs/agents/domain.md` are absent in this checkout. Relevant architecture decisions live under `product/architecture/adr/`.
