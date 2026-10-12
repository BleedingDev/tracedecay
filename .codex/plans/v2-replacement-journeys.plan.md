---
name: V2 replacement product journeys
overview: Prove V2 replacement through direct production journeys covering fresh-store admission, hosts, daemon restart, Native and NCM operations, code retrieval, indexing, privacy, failure recovery, and observable operator state.
todos:
  - id: prove-final-store-admission
    content: "Exercise each database, spool, journal, checkpoint, snapshot, receipt, and projection with exact final V2, representative V1, earlier-V2, partial, corrupt, and foreign fixtures; require ResetRequired only for incompatible shape before decode or use, preserve distinct Corruption for damaged admitted shapes, and preserve DurabilityUncertain for observable sync, close, or recovery failure."
    status: pending
  - id: prove-graph-publication-durability
    content: "Fault-inject graph convergence to prove canonical replay input is durable before projection, recovered entities, relations, source generation, and watermark pass a full close and reopen digest before publication, failed convergence preserves the prior verified graph, and observable durability uncertainty closes the affected runtime."
    status: pending
  - id: prove-daemon-restart-surfaces
    content: "Run physical daemon stop and restart journeys across CLI, MCP, HTTP, SDK, and dashboard for facts, sessions, LCM, Native recall, NCM recall, code search, cursors, pending projection, cancellation, and durable committed effects."
    status: pending
  - id: prove-host-lifecycle
    content: "Run isolated install, update, ownership drift, doctor and status, disable, and uninstall journeys for every supported Codex, Claude, Cursor, Hermes, Kiro, and Cline-family integration using real checked-in host fixtures and preserving unrelated files byte-for-byte."
    status: pending
  - id: prove-provider-host-delivery
    content: "For Native and NCM, run real host event to sanitized admission to durable source journal to observation to oracle-expected recall, feedback, and controls, then duplicate, restart, retry, cancellation, provider unavailable, deletion, and replay cases with exact source identity and typed outcomes."
    status: pending
  - id: prove-indexing-evolution
    content: "Exercise fresh index, unchanged reopen, add, modify, rename, delete, branch and linked-worktree changes, interrupted refresh, prior-generation serving, rebuild after corruption, and shared-code updates with bounded scheduling and truthful freshness."
    status: pending
  - id: prove-flagship-surfaces
    content: "Run direct dashboard and Settings journeys, callable Work and workflow-runtime journeys, and public SDK conformance required by the latest #707 roadmap so replacement is not declared from memory and search alone."
    status: pending
  - id: prove-privacy-and-authority
    content: "Run direct adversarial cases for cross-project and cross-profile scope, mutable metadata, forged locators, wrong keys, stale revisions, missing grants or journals, symlink and path substitution, deleted-source resurrection, hostile provider details, logs, telemetry, errors, and persisted explain traces."
    status: pending
  - id: replace-gate-scaffolding
    content: "Delete or archive the Native-original giant comparison matrix, gate manifests, local attestations, and proof-packet machinery as release authority; retain only production receipts needed for real atomic effects or daemon operations and replace every retained product assertion with a direct test or ordinary benchmark."
    status: pending
  - id: expose-truthful-operator-status
    content: "Make status and read-only doctor report binary and daemon version, exact store state, source freshness and coverage, host registration state, active provider, NCM asset and worker readiness, typed unavailable or reset-required causes, and recovery guidance without performing repair."
    status: pending
isProject: false
---

# V2 replacement product journeys

## Execution Notes

This plan is the acceptance core. It replaces historical giant matrices with a smaller set of production journeys that reach real entrypoints, the daemon/application kernel, durable state or computation, and visible results. Runtime receipts remain only where the product itself needs a durable atomic-effect or daemon-operation receipt.

## Constraints

- Every journey uses isolated temporary home, profile, project, socket, host, and provider paths; never run against the operator's live V1 profile.
- Synthetic value fixtures may test isolated parsing, but only real checked-in host/provider fixtures satisfy host and provider acceptance.
- A zero-test filter, empty fixture, unexpected empty recall, Unsupported response, no-op, fallback, compile-only pass, or stale artifact cannot close a replacement requirement; an independently specified negative oracle may correctly return empty.
- Doctor remains read-only; reset, configuration, provider selection, install, and recovery are separate named operations.
- Preserve failure types and partial coverage through all adapters.

## Operator Guidance

Depends on both `V2 Native and NCM memory replacement` and `V2 code retrieval replacement`. The individual journey groups can run in parallel after their production owners are stable. Give each execution subagent exact journey and file ownership; central fixtures, generated contracts, host installers, and shared daemon composition stay lead-owned.
