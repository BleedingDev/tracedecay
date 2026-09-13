---
name: rn-native-tests
overview: "Replace substitute-backed Native tests with original behavior checks. Preserve the complete original upstream Native implementation from 570 and its operation boundaries."
todos:
  - id: rn-native-tests-done
    content: "Replace substitute-backed Native tests with original behavior checks and return the required reviewable evidence."
    status: pending
isProject: false
---

# Replace substitute-backed Native tests with original behavior checks

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

Execution host: Codex. Use an available Codex-native execution/review agent when assigned; adapt unavailable model preferences within Codex. You are a leaf; no child agents or cross-host agent CLI launches. Preserve peers' edits and send cross-scope needs to the lead.

Mode: write-capable.
Prerequisites: rn-native-port, rn-native-bridge. Every named predecessor must be accepted by root before dependent work starts. Original baseline: 57006f60cb45bcee8487e73a40d4fad1a12ee2b6; audited product head: 1fe250fed7ca615f1dfdcd580befeb276328e910. The owner may read the accepted wrapper and prepare non-writing notes early; it does not start dependent test edits against an unfinished private bridge API.

Read these reports under ../evidence/: native-facts.md, native-sessions.md, native-adapter.md, saved-data.md. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

## Ownership and Constraints

Write only:

- crates/tracedecay/src/daemon/retained_owner/native_provider_tests.rs
- crates/tracedecay/src/daemon/retained_owner/native_common_factory_tests.rs
- crates/tracedecay/src/daemon/retained_owner/native_original_state_tests.rs (new if needed)

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

## Steps

1. Read the accepted route/wrapper contracts and align tests to the original operation boundary. Prepare disjoint fixtures while the bridge owner works; do not guess unfinished private signatures.
2. Replace staged durability/ranking/lifecycle success expectations with original delegation, truthful unsupported generic behavior and absence of staged side effects.
3. Keep all still-relevant malformed input, scope, provenance, cancellation, deadline, privacy, replay and lifecycle assertions. Do not weaken unrelated NCM/common-profile tests.
4. Add relevant in-process checks for original canonical receipts/telemetry boundaries and saved-state continuity, using real original services where available. These checks complement the external 570 differential runner.
5. Send module declarations to composition and exact Cargo filters to the build owner; run only non-build fixture/style checks in this node.

## Acceptance Checklist

- Tests no longer prove staged behavior as Native.
- Positive canonical effects and read-only automatic-query behavior are both checked.
- No ignored test, blanket skip, zero-case command or mock-only parity claim is introduced.

## Operator Guidance

Root launches this node from the saved execution graph only when its predecessors are accepted and its exact files are free. Review this diff before releasing dependent writers. Root alone updates plan status.

Return: node ID; exact changed paths and reasons; diff; verification commands and actual results; build tickets if applicable; protected-behavior evidence; unresolved dependencies/failures with exact next owner. Do not claim an unrun check passed.

Stop condition: Return the owned tests and helper-signature requests. Bridge-dependent private calls wait for the accepted bridge output; no peer-file edits.

## Current dependency contract

Prerequisites: rn-native-port, rn-native-bridge. The exact edges in ../execution-selection.json are authoritative; this section supersedes older prerequisite prose. Read ../READINESS-PLAN.md for the latest NCM repair and semantic scope.
