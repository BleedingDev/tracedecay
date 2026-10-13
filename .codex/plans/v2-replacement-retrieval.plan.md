---
name: V2 code retrieval replacement
overview: Replace V1 search expectations with V2 exact, lexical, graph, verified shared-code, and opt-in dense semantic authorities, then prove useful deterministic search through every public surface.
todos:
  - id: define-search-oracle
    content: "Freeze a compact real-repository search corpus with independently specified exact and conceptual positives, negatives, semantic modes, identity mismatches, scope, ordering, coverage, partial-result obligations, and allowed relevance differences from V1 before changing retrieval behavior."
    status: pending
  - id: implement-opt-in-dense-semantic-authority
    content: "Implement the opt-in fourth dense semantic code-search authority with CPU FastEmbed/Jina embedding, immutable exact-flat vectors, verified identities, and typed lifecycle/readiness outcomes."
    status: pending
  - id: separate-semantic-lifecycle
    content: "Keep code-semantic model acquisition, projection, checkpoints, generations, activation, restart, and rollback separate from NCM semantic-memory lifecycle; never download or discover artifacts on the query path."
    status: pending
  - id: enforce-semantic-modes-and-fallback
    content: "Define explicit fallback and strict semantic modes; preserve exact, lexical, and graph ranking/readiness/fallback semantics and never silently alias semantic to lexical or substitute a model."
    status: pending
  - id: finish-lexical-query-semantics
    content: "Implement and directly test all-terms-first lexical matching with graduated relaxation, exact identifier preservation, field and phrase behavior, proximity, bounded identifier and path splitting, deterministic ordering, stable cursors, and truthful truncation or partial coverage."
    status: pending
  - id: verify-code-graph-retrieval
    content: "Exercise graph symbol, relation, caller, callee, impact, and traversal results through the canonical retrieval kernel with exact scope, generation, source anchors, cancellation, stale-state, and hydration behavior."
    status: pending
  - id: implement-shared-code-authority
    content: "Complete source-bound shared-code detection in the parse and sealed-generation pipeline using conservative and rename-normalized body tokens, content-addressed clone payloads, occurrence bindings, exact digest postings, bounded candidate admission, and token-verified exact and renamed-copy results."
    status: pending
  - id: wire-similar-and-redundancy
    content: "Redefine tracedecay_similar and tracedecay_redundancy over verified shared-code facts with left and right coverage, exact differences, source anchors, typed partial results, and no claims of dead code, equivalence, merge safety, or a single similarity percentage."
    status: pending
  - id: prove-one-retrieval-kernel
    content: "Run identical representative queries through MCP, CLI, dashboard, host integrations, LSP, API, and SDK adapters and prove they delegate to one ranking, cursor, hydration, authorization, and store-selection implementation."
    status: pending
  - id: establish-search-quality-floor
    content: "Run the versioned oracle across exact symbols, concepts, phrases, paths, relations, renamed copies, semantic positives/negatives, near misses, unrelated code, parse errors, deletion, stale generations, identity mismatch, budget exhaustion, restart/rollback, and wrong scope; require semantic quality floors, no protected baseline regression, stable relevance, and truthful coverage."
    status: pending
isProject: false
---

# V2 code retrieval replacement

## Execution Notes

The 2026-09-13 dense-retrieval retirement is superseded by the V2 decision
recorded in the plan-set index. The replacement includes a fourth, opt-in dense
semantic code-search authority alongside exact/lexical search, the code graph,
and source-bound shared-code detection. NCM semantic memory remains a separate
provider and lifecycle concern.

The first semantic implementation is CPU-only FastEmbed/Jina with immutable
exact-flat vector search. It must be independently admitted, identity-bound,
and quality-gated; it never delays or changes the exact/lexical/graph baseline.
ANN, GPU execution, and reranking are outside this plan.

## Constraints

- Four independent code-intelligence authorities: exact/lexical search, the code graph, source-bound shared-code detection, and opt-in dense semantic search.
- The initial dense lane is CPU FastEmbed/Jina embedding plus immutable exact-flat cosine/dot-product search. Do not add ANN, GPU execution, or reranking scope.
- Model, projection, vector-generation, and source identities are authenticated by verified manifests/content digests and daemon authorization; mismatches are typed unavailable.
- Code-semantic artifacts, checkpoints, generations, activation, restart, and rollback are separate from NCM semantic-memory state. Query paths never download, discover, or network-fallback to model artifacts.
- `fallback` mode preserves the exact/lexical/graph result when semantic state is missing or unusable; `strict` mode returns typed unavailable/failed semantic outcome and never silently aliases to lexical or substitutes a model.
- Digests are lookup keys and corruption checks, not proof that two implementations are equivalent.
- Budget exhaustion and unavailable hydration report partial coverage and preserved rank; they never become empty-complete answers.
- Keep tests on real parse-to-generation-to-retrieval journeys, including semantic generation publication/restart/rollback and the versioned quality oracle, rather than direct ad hoc scanners.

## Operator Guidance

Depends on completion of `V2 replacement baseline and PR 707 reconciliation` and may run in parallel with the memory plan. Serialize changes to the shared retrieval kernel and public schemas. Use independent review for ranking and scope semantics, then commit and push each complete query or shared-code slice.
