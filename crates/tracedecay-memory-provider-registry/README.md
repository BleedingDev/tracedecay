# TraceDecay memory provider registry

This product-owned crate is the narrow composition layer for concrete memory
providers. Only explicit enabled composition creates a bounded `MemoryFabric`
and constructs the Native adapter from an injected
`NativeMemoryApplicationPort`.

Disabled composition contains no config, Native port, registry, or fabric.
Enabled composition derives the stable Native identity internally and accepts
only observer or active participation. The crate owns no storage, state
directory, transport, background task, or default-on activation.

The public registry surface returns provider-neutral API and fabric results
without exposing the mutable fabric or a concrete provider handle. Handshake
and active replies retain their complete structured terminal records.
Observation delivery removes provider payloads, opaque extensions, and warning
text but retains committed-effect, reconciliation, verification, and fallback
evidence inside its observer receipt. Terminal provider and operation
identities remain bound to the selected route. Fallback directives are
evidence only; this composition layer never executes an alternate-provider
dispatch.
