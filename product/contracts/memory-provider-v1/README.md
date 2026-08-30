# Memory provider contract V1

`tdmem-0201` establishes the provider-neutral capability registry used by every later provider contract. It defines identity and semantics only; no Native, NCM, or OCEAN adapter is implemented here.

## Stable identities

A provider uses stable `MemoryProviderIdV1` identity. Display names, process IDs, sockets, database paths, configuration order, and state digests are never identity. Capability IDs are versioned behavior names, not provider names. Only the registry/composition boundary may branch on `provider_id`; CLI, MCP, SDK, dashboard, context, storage, and application surfaces remain provider-neutral.

## Mandatory versus optional

The authoritative registry has two non-overlapping sets:

- Mandatory: `provider.health.v1`, `observation.accept.v1`, and `recall.query.v1`. A registered provider cannot become ready without all three.
- Optional: feedback, maintenance, temporal recall, associative activation, explicit-fact projection, explainability, correction, source deletion, snapshot export/restore, replay, and inspection.

Every entry carries canonical input and output contract identities, bounded typed failure modes, and explicit compatibility rules. Exact capability major versions are required; unknown optional fields are preserved; implicit downgrade is forbidden; behavior activates only after a known catalog entry, an accepted registration revision, and explicit selection.

For compatibility with already-landed M1 validators, `capability_catalog` is a derived ID-only projection of both authoritative sets. It is not an activation source; `capability_registry` remains authoritative.

## Unknown capability round-trip

A syntactically valid unknown capability is decoded as `OpaqueMemoryProviderCapabilityV1` with its canonical payload preserved. Re-encoding must round-trip that payload without semantic rewriting. Presence never means support: the declaration is retained as opaque, cannot count as mandatory, cannot satisfy a required capability, cannot infer behavior from its name, and cannot activate anything. Explicit selection returns typed `capability_unsupported`. Promotion requires a future accepted catalog revision, a new registration revision, and explicit selection.

## Fail-closed resolution

Resolution requires exact provider identity, accepted registration revision, compatible adapter contract, all mandatory capabilities, every explicitly required known capability, exact TraceDecay scope, a live deadline, and live cancellation. There is no implicit fallback and no successful empty resolution.

## Reserved provider slots

- `tracedecay.native` is declared but remains gated by parity work.
- `ncm` is reserved; surface audit precedes transport/topology selection.
- `ocean` reserves identity only because no versioned specification exists.

None of the bootstrap slots counts as implemented. Concrete Native/NCM adapters remain out of scope for this bead.
