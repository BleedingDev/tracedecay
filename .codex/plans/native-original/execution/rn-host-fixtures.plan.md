---
name: rn-host-fixtures
overview: "Prepare real Claude and Codex Native comparison journeys. Preserve the complete original upstream Native implementation from 570 and its operation boundaries."
todos:
  - id: rn-host-fixtures-done
    content: "Prepare real Claude and Codex Native comparison journeys and return the required reviewable evidence."
    status: pending
isProject: false
---

# Prepare real Claude and Codex Native comparison journeys

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

Model: gpt-5.6-luna. Reasoning: max. Spawn with fork_turns=none and a bounded handoff. You are a leaf; no subagents. You are not alone in the codebase: preserve peers' edits, never revert/reformat/stage them, and send cross-scope needs to root.

Mode: write-capable.
Prerequisites: rn-reference, rn-contract. Every named predecessor must be accepted by root before dependent work starts. Original baseline: 57006f60cb45bcee8487e73a40d4fad1a12ee2b6; audited product head: 1fe250fed7ca615f1dfdcd580befeb276328e910.

Read these reports under ../evidence/: host-delivery.md, verification.md, native-sessions.md. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

## Ownership and Constraints

Write only:

- crates/tracedecay-cli/tests/product_memory_provider_claude_host_journey.rs
- scripts/product/native-original/cases/hosts/**

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

Required runner handoff: `scripts/product/native-original/case-contract.json`, owned and published by rn-reference. Use its exact schema and retained operation shapes. Every original-operation case must invoke the route through selected Native production composition, not only call an unmounted helper. Reference extension-only host checks separately when no original upstream counterpart exists.

## Steps

1. Reuse the existing production journey and comparison connector, including production_comparison:factory. Check executable/artifact prerequisites; do not replace the runner with a private synthetic port.
2. Add original-versus-restored Native cases for Claude and Codex through actual hooks/MCP, canonical session ingestion and one scheduled recall action, plus a refusal/failure case.
3. Preserve native host IDs/transcripts, installed global/local differences, final delivered UTF-8 capture, typed source attribution and process cleanup. Do not infer a installed Codex hook from its empty source scaffold.
4. Remove expectations that successful Native means returning four staged messages. Derive expected fact/session/context behavior from the active 570 reference and matching host action.
5. Keep current Native/NCM/control evaluation lanes and existing captures intact. Full comparative quality expansion is not part of fixture authoring.

## Acceptance Checklist

- Journeys use actual host/MCP boundaries and preserve exact final delivered bytes and source evidence.
- Original Native behavior determines expected output; staged counts do not.
- Claude and Codex remain separately attributable and cleanup is test-owned.

## Operator Guidance

Root launches this node from the saved execution graph only when its predecessors are accepted and its exact files are free. Review this diff before releasing dependent writers. Root alone updates plan status.

Return: node ID; exact changed paths and reasons; diff; verification commands and actual results; build tickets if applicable; protected-behavior evidence; unresolved dependencies/failures with exact next owner. Do not claim an unrun check passed.

Stop condition: Return fixture/journey edits and prerequisite list. No live host or model run until the build/verification nodes.
