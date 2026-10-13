---
name: rn-history-owner-followup
overview: "Validate and bound ResolvedScope propagation for original history-owner ingestion."
todos:
  - id: rn-history-owner-followup-done
    content: "Trace the concrete historical-ingest authority and return consumed/not-consumed scope evidence with a bounded owner and assertion."
    status: pending
isProject: false
---

# Validate history-owner scope propagation

## Execution Notes

Worktree: /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2. Active original baseline: 57006f60cb45bcee8487e73a40d4fad1a12ee2b6; candidate: 1fe250fed7ca615f1dfdcd580befeb276328e910. This is a read-only bounded validation; no product code, Cargo, test or contract edits belong here.

Own `execution-results/history-owner-validation.md`. Read `crates/tracedecay-session-runtime/src/session_temporal_refresh_scheduler/history.rs` at `ProjectSessionHistoricalIngestor::new`, `with_original_provenance_resolver` and `run_pass`, then follow `DaemonSessionSyncConfig.scope` and `SessionSyncProjectContext` in `crates/tracedecay-session-runtime/src/session_sync.rs` into the concrete ingest authority/request. The result must prove whether the authoritative `tracedecay_contracts::ResolvedScope` reaches historical ingestion unchanged, while preserving resolver identity, transcript source home, background CPU, cancellation and provenance.

cc1424 compile evidence reports `transcript_source_home`, `background_cpu` and `original_provenance_resolver` fields unused in session-sync/project-lifecycle context while equivalent authorities are consumed in the history ingestor. Explain that boundary or name the smallest product owner and propagation point; do not perform unrelated cleanup. If scope is not consumed directly, cite the indirect request/field or assign one acceptance assertion and a bounded fix owner. No fabricated parity is allowed. Session cases, session delivery and product build remain gated on this result.

## Stop condition

Return concrete symbols/fields, consumed/not-consumed conclusion, exact gap owner and acceptance assertion. Do not implement the fix in this node.

## Current dependency contract

Prerequisites: rn-source-docs, rn-contract, rn-integration-readiness. The exact edges in ../execution-selection.json are authoritative; this section supersedes older prerequisite prose. Read ../READINESS-PLAN.md for the latest NCM repair and semantic scope.
