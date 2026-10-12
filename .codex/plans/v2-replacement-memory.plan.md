---
name: V2 Native and NCM memory replacement
overview: Deliver one provider-neutral memory boundary with a canonical Native implementation and an explicit optional NCM implementation, including durable restart behavior, source authority, privacy, controls, and real nonvacuous conformance.
todos:
  - id: port-provider-boundary
    content: "Port provider declarations, registry, configuration, lifecycle, capability negotiation, and host admission into the latest PR #707 ownership boundaries without creating a second daemon, store selector, or retrieval kernel."
    status: pending
  - id: define-provider-authorities
    content: "Make canonical events, facts, controls, source content, authorization, final ranking, pagination, hydration, and scope daemon-owned; make NCM indexes and checkpoints derived state with exact source watermarks; prohibit the NCM worker from opening canonical writable stores; and admit provider candidates only through the one canonical retrieval kernel."
    status: pending
  - id: replace-staged-native-store
    content: "Replace the active Native StagedObservationStore path with an adapter over the exact canonical V2 fact, session, LCM, source, journal, and control authorities so Native owns no parallel compatibility database."
    status: pending
  - id: close-native-authority-gaps
    content: "Make controlled Native staging, recall, feedback, correction, deletion, inspection, maintenance, replay, and restore fail closed on missing host lineage; bind original source and sanitized provider-view identities; survive re-registration and restart without accepting stale authority."
    status: pending
  - id: harden-retained-privacy
    content: "Identify each real untrusted caller or provider boundary, prefer daemon-owned identity lookup and typed opaque handles for local authority, and retain keyed binding only where that boundary requires it; finish locator re-derivation, provider and revision attribution, exact-one mutation, tombstone, source-fence, snapshot, and anti-resurrection behavior with adversarial reopen tests."
    status: pending
  - id: isolate-ncm-runtime
    content: "Port NCM as an explicitly selected provider whose worker, model, tokenizer, projection, state, and namespaces are provider-local; keep the default V2 installation model-free and make missing NCM assets typed unavailable without Native fallback."
    status: pending
  - id: package-pinned-ncm-assets
    content: "Define and implement an opt-in offline NCM bundle with repository-pinned worker, model, tokenizer, hashes, licenses, and platform support so NCM startup never downloads code or model data."
    status: pending
  - id: define-ncm-recall-oracle
    content: "Freeze a compact real-input NCM oracle with independently specified positive IDs, negative and near-confusable queries, scope, temporal and exclusion behavior, ordering and coverage obligations, and the known seed regressions before further threshold or ranking changes."
    status: pending
  - id: verify-ncm-recall-stability
    content: "Re-run the real NCM encoder after integration across the known failing seeds, a 32-seed sweep, 100 identical-seed repeats, negative and near-confusable queries, cold and warm starts, daemon and worker restart, cancellation, concurrency, and budget pressure with zero unexplained required misses or scope leaks."
    status: pending
  - id: implement-provider-switching
    content: "Make Native-to-NCM and NCM-to-Native selection explicit and restart-safe: replay canonical events plus corrections, tombstones, exclusions, and relevant controls through a frozen watermark, catch up admitted work, atomically activate a configuration revision and provider epoch, reject stale results, and recover interruption, deletion races, missing replay material, lost replies, and restart on either side of activation."
    status: pending
  - id: run-common-memory-conformance
    content: "Run one unchanged provider-neutral direct suite against real Native and real NCM for observe, temporal recall, exclusions, provenance, feedback, correction, deletion, inspection, maintenance, snapshot and restore, replay, cancellation, corruption, lost reply after commit, restart, project-wide facts across branches and worktrees, and scope isolation; reject unexpected empty, no-op, Unsupported, or fallback outcomes while accepting correctly empty oracle negatives."
    status: pending
isProject: false
---

# V2 Native and NCM memory replacement

## Execution Notes

Native is the canonical V2 implementation over final V2 authorities. NCM is an optional provider behind the same host contract. Its worker computes over admitted inputs while the daemon retains canonical storage, authorization, ranking, cursor, hydration, and publication authority. NCM may use provider-local embeddings, but those assets and dependencies must never reintroduce dense code search, code vectors, semantic activation, or a dependency from lexical or graph retrieval.

The pushed NCM recall floor fix at `613d9d4ff` is useful evidence, not final acceptance after the upstream port. Current uncommitted Native/privacy work includes known review blockers around missing host journals, original-source mutation, re-registration identity, and alias authority; those cases are explicit acceptance tests here.

## Constraints

- One daemon remains the only local mutable-store authority.
- Graph topology is persisted only through `tracedecay-graph-db`; relational storage may not shadow it.
- Native may not retain the staged substitute as a second long-lived database.
- Provider switching uses canonical source replay and exact final stores, never storage conversion, dual write, or shadow reads.
- The default V2 package starts without model acquisition or a network download.
- NCM failure never blocks lexical, graph, session, fact, LCM, or Native operation and never silently falls back while reporting NCM success.
- Tests must use populated real paths and assert identities, effects, omissions, and durable reopen state; correctly empty negative-oracle answers remain valid.

## Operator Guidance

Depends on completion of `V2 replacement baseline and PR 707 reconciliation`. After the provider boundary is stable, Native authority work and NCM runtime work may proceed in parallel, while shared contracts, generated bindings, root composition, and `Cargo.lock` remain serialized under the lead. Commit and push each reviewed slice before starting the next shared-boundary change.
