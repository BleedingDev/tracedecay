---
name: pm-operations
overview: Make provider selection, status, disablement, recovery and packaging usable through the existing product surfaces without provider-specific transport coupling.
todos:
  - id: operator-provider-controls
    content: Expose truthful supported provider selection, capabilities, readiness, degradation and lifecycle controls through existing configuration and CLI/MCP surfaces.
    status: pending
  - id: feature-and-packaging-compatibility
    content: Validate default and Native-only builds plus NCM-enabled packaging, worker/model availability, legacy settings/data reopen and disable/rollback behavior on supported targets.
    status: pending
  - id: publish-operator-instructions
    content: Document exact Native/NCM selection, installation prerequisites, replay-based switching, provider-specific snapshots, measured costs and recovery using the tested product paths.
    status: pending
isProject: false
---

# pm-operations

## Execution Notes

Read the brief and completed neutral host routing. Use existing configuration authority and public command contracts. Provider implementation details belong in setup/help and truthful capability status, not new per-provider command schemas. Keep default-off NCM behavior. Preserve private paths/auth boundaries and model pinning; no implicit download during ordinary startup.

## Constraints

Own narrowly identified CLI operator commands/tests and product provider docs after root assigns exact files. Root owns manifests, lockfile, central config and packaging workflow patches. Do not edit the host journey fixture, registry/adapter implementation, evaluator, user's configuration or installed daemon. Platform claims require actual tests; record unavailable platforms honestly.

## Operator Guidance

Dependency: `pm-host-recall`. Root batches feature/dependency checks and isolated packaging smoke tests; workers do not launch builds. Run setup/disable/reopen in fixture profiles only. A missing platform runner limits that platform claim but does not invalidate measured local capability or trigger broad unchanged rebuilds.
