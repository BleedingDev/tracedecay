---
name: pm-contract
overview: Turn the audited gaps into one versioned common memory profile and shared data contracts that both adapters and host wiring can implement independently.
todos:
  - id: settle-common-memory-semantics
    content: Resolve the audit matrices into the common capability profile, authority and temporal rules, neutral source/candidate attribution, provider-local feedback semantics and compatibility migration decisions.
    status: completed
  - id: implement-shared-contract-seams
    content: Add the minimum shared typed request, response and history-authorization contracts and registration metadata needed by both implementations while preserving existing caller/data compatibility.
    status: completed
  - id: validate-contract-consumers
    content: Update canonical schema/binding consumers and discriminating fixtures for required fields, unsupported extensions, scope, time, mutation outcomes and source-attribution validation.
    status: completed
isProject: false
---

# pm-contract

## Execution Notes

Read the brief and three source audits. Reuse `MemoryProvider` and current payload contracts; do not introduce a competing interface. Define one mandatory completion profile covering the brief's table and an explicit optional-extension set. Preserve canonical facts as a shared host authority. Cross-session reuse requires host-granted original-source attribution, not provider-edited scope. Snapshots stay provider-specific; canonical replay is the compatibility mechanism across providers. Historical answers require recorded validity; never infer historical truth from current state.

Settle these concrete producer/consumer questions before authoring contracts: source content revision versus envelope schema version; accepted common observation kinds and message projection; original-scope authorization across the two repository identity schemes; source-wide deletion including unseen/replayed copies; revision and feedback targets independent of request-specific candidate IDs; same-delivery duplicate versus source-already-applied replay accounting; monotone live generation across restore; legacy unknown validity; and how a fresh restore obtains deletion history newer than its snapshot. Reuse existing `ProviderOperation` capability IDs and effect/control types. Required capabilities must execute successfully on populated fixtures; unsupported responses are compatibility failures, not negotiated parity.

## Constraints

Single writer for `crates/tracedecay-memory-provider-api/`, applicable contract documents/schemas/generated bindings under `product/contracts/memory-provider-v1/`, their focused Python validators, and a minimal architecture decision clarifying changed semantics. Registry shared type changes are drafted here and handed to root for serial integration. Do not edit adapter implementations, root composition, evaluator or global manifests. Generated product serialization identities may change when their semantic input changes; no administrative receipt work.

This single writer also owns additive shared request declarations in `crates/tracedecay-contracts/src/memory/recall.rs` for temporal/exclusion inputs and the exact retained provider-control declarations assigned by root. Their real host mapping belongs to `pm-host-recall`. The common advisory profile covers provider-local observations and derived memory. Canonical fact projection is excluded from that matched profile and remains a separately declared legacy/optional extension; preserve its compatibility outside the common profile. This avoids making historical staged recall depend on a new canonical-fact history search. For a historical host request, current canonical facts cannot be presented as target-time memory: the host slice owns temporal admission/withholding of that separate contribution and explicit missing historical coverage.

## Operator Guidance

Dependencies: all three audits. Root resolves the contract and assigns an execution worker exact changes; the worker is not allowed to invent public semantics. Validate the changed schema and consumer tests, then one scoped Cargo type/conformance check through root. Downstream Native, NCM, registration, history, conformance and harness lanes start only when these interfaces are concrete and reviewed.
