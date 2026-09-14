---
name: V2 code retrieval replacement
overview: Replace V1 semantic-search expectations with the latest V2 lexical, graph, and verified shared-code authorities, remove the retired dense stack, and prove useful deterministic search through every public surface.
todos:
  - id: define-search-oracle
    content: "Freeze a compact real-repository search corpus with independently specified exact and conceptual positives, negatives, scope, ordering, coverage and partial-result obligations, and allowed relevance differences from V1 before changing lexical or shared-code behavior."
    status: pending
  - id: delete-retired-dense-stack
    content: "Remove all remaining dense code-search crates, dependencies, lifecycle state, model acquisition, vector generations, activation and rollback paths, fixtures, configuration, release handling, and public API, CLI, MCP, LSP, SDK, and dashboard surfaces after the PR #707 merge."
    status: pending
  - id: enforce-no-semantic-alias
    content: "Remove or return a typed explicit disposition for legacy semantic-only requests and configuration; never silently route a semantic name to lexical search."
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
    content: "Run the versioned oracle across exact symbols, concepts, phrases, paths, relations, renamed copies, near misses, unrelated code, parse errors, deletion, stale generations, budget exhaustion, and wrong scope; require the expected positives, correctly empty negatives, stable relevance, and truthful coverage without a model runtime."
    status: pending
isProject: false
---

# V2 code retrieval replacement

## Execution Notes

PR #707 decision 11 retires neural dense code retrieval. The replacement goal called “semantic search fixed” is therefore satisfied by useful lexical search, code-graph traversal, and verified source-bound shared-code detection. NCM semantic memory recall is a separate provider concern and does not become code search.

Latest #707 appears to have completed much of the removal and lexical routing, while the shared-code authority still needs implementation and full product wiring. A deletion-only merge would leave `tracedecay_similar` and `tracedecay_redundancy` without their intended replacement behavior.

## Constraints

- Exactly three code-intelligence authorities: lexical search, the code graph, and source-bound shared-code detection.
- No FastEmbed, ONNX Runtime, Model2Vec, vector index, vector generation, activation, calibration, neural reranking, or dense fusion in code retrieval.
- Digests are lookup keys and corruption checks, not proof that two implementations are equivalent.
- Budget exhaustion and unavailable hydration report partial coverage and preserved rank; they never become empty-complete answers.
- Keep tests on real parse-to-generation-to-retrieval journeys rather than direct ad hoc scanners.

## Operator Guidance

Depends on completion of `V2 replacement baseline and PR 707 reconciliation` and may run in parallel with the memory plan. Serialize changes to the shared retrieval kernel and public schemas. Use independent review for ranking and scope semantics, then commit and push each complete query or shared-code slice.
