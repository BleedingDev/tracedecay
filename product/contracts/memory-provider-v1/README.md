# TraceDecay Memory Provider Contract V1

This directory owns the product-level, provider-neutral contracts for pluggable cognitive memory. M1 defines behavior before M2 creates Rust crates or concrete adapters.

## Current contract

`provider-registry-contract.json` defines:

- stable `MemoryProviderIdV1` identity;
- versioned capability identity and the initial capability catalog;
- authoritative, revisioned registration;
- explicit capability requirements and fail-closed provider resolution;
- bootstrap identity slots for TraceDecay Native, NCM, and future OCEAN;
- the prohibition on provider-name branching outside adapter construction;
- the prohibition on silent fallback or successful empty resolution.

`provider-registry-contract.schema.json` is the strict structural schema. `scripts/product/check-provider-registry-contract.py` adds semantic checks that JSON Schema cannot express, including Beads references, reserved-slot rules, authority limits, and contract consistency.

## Authority boundary

The registry composition root is authoritative only for provider registration, concrete-adapter construction, and exact selection resolution. It is **not** authoritative for:

- current source code;
- repository, worktree, branch, profile, or session identity;
- admitted session evidence;
- accepted TraceDecay Native facts, lineage, privacy, feedback, or trust;
- curated rules or configuration settlement;
- final coding-context assembly.

Every capability in the V1 catalog is advisory with respect to those TraceDecay authorities. Provider-local mutation means mutation of the selected provider's own cognitive state only.

## Identity rules

Provider ID is stable machine identity, not presentation. It cannot be derived from display name, process/socket/database location, state digest, configuration order, or runtime order. Upgrades retain the same provider ID when they are the same logical provider and use explicit implementation/protocol versions for compatibility.

Capability IDs include a major version suffix such as `recall.query.v1`. Unknown and duplicate capability IDs are rejected. A provider name never implies capabilities; only an accepted registration revision and compatible handshake can declare them.

## Registry and selection rules

The provider cannot self-register. TraceDecay's composition root creates one registration with an exact revision. Duplicate IDs are ambiguous and fail closed.

A selection request names one provider ID and every required capability. Resolution succeeds only when:

1. the provider ID matches exactly;
2. registration state is usable;
3. adapter/protocol versions are compatible;
4. every requested capability is declared;
5. exact coding scope is admitted;
6. deadline and cancellation are live.

All non-success states are typed. There is no implicit fallback to `tracedecay.native`, no first-success provider search, and no empty “ready” result.

## Bootstrap slots

- `tracedecay.native`: declared identity; implementation and parity remain gated by `tdmem-0401`–`tdmem-0404`.
- `ncm`: reserved identity; capabilities and topology remain gated by licensed-surface audit and ADR (`tdmem-0701`, `tdmem-0702`) before observer integration.
- `ocean`: identity reservation only. It has no implementation gate, capabilities, or delivered status until a versioned specification exists.

None of the bootstrap slots counts as implemented in this contract bead.

## Follow-on contracts

- `tdmem-0202`: handshake, implementation identity, protocol compatibility, limits, deadline, and cancellation.
- `tdmem-0203`: normalized observation envelope and idempotency.
- `tdmem-0204`: recall request, candidates, scores, provenance, warnings, and coverage.
- `tdmem-0205`: feedback, maintenance, correction, forgetting, inspection, snapshot, and restore.
- `tdmem-0206`: typed terminal/degradation/retry/partial-effect outcomes.
- `tdmem-0209`: provider conformance model.

Concrete Native/NCM adapters remain out of scope until these provider-neutral contracts and M2 dependency guards are accepted.
