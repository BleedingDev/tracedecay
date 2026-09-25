---
name: pm-registration
overview: Remove Native-only active-registration assumptions and expose neutral configured providers without scattering implementation names through the host or transports.
todos:
  - id: neutral-provider-registration
    content: Replace Native-only active mounting assumptions with injected provider registrations carrying actual limits, execution shape, scope bindings and lifecycle ownership.
    status: completed
  - id: neutral-selection-configuration
    content: Support explicit Native or NCM active selection plus independent disabled or observer participation, with legacy setting migration and no implicit fallback or Native-advisory prerequisite.
    status: completed
  - id: registration-isolation-tests
    content: Verify disabled creates no worker, observer contributes no active output, NCM active is independent of Native advisory activation, and unknown or incompatible providers fail explicitly.
    status: completed
isProject: false
---

# pm-registration

## Execution Notes

Read the brief, host audit and common contracts. The registry already exposes neutral fabric, invocation and readiness operations: extend these instead of creating another routing authority. Concrete adapters are injected at the composition boundary. Native canonical facts remain a host service even with the Native advisory provider disabled. Preserve configured provider identity through results and failure handling.

## Constraints

Own registry `src/lib.rs`, `supervised_readiness.rs`, `supervisor.rs` and their direct tests, plus `tracedecay-domain/src/configuration.rs`, configuration parsing and global-db configuration registry/store tests. Root owns `Cargo.toml`, `Cargo.lock`, `project_composition.rs` and invocation-state wiring; send an exact integration patch for those files. Do not edit recall admission/packing/provenance modules, provider implementations or host journey fixtures.

## Operator Guidance

Dependency: `pm-contract`. Root serially integrates central config/manifest/composition changes before declaring this node complete. Check existing configuration scope/revision/restart semantics and old serialized settings. Do not make changes to actual operator settings. Stop at a reviewed registration/configuration seam and its conformance tests; active context hookup belongs to `pm-host-recall`.

Root validation: configuration/domain/persistence tests passed (cc-1316); neutral registration and supervised readiness tests passed (cc-1324), including the Disabled stop/kill correction. Reviewed composition and authority injection patches are applied. The combined root library test build passed (cc-1330), and all five root memory_provider_routing_tests passed from that binary. Real active context delivery is the downstream host responsibility.
