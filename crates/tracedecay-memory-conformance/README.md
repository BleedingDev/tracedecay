# tracedecay-memory-conformance

Provider-neutral fixtures, scenario execution, conformance reports, and
differential reports for implementations of `MemoryProvider`.

Fixtures bind the exact Memory Provider contract set, logical provider,
and immutable provider build/implementation digest. The runner builds
calls from the provider's real handshake receipt, so the same fixture can be
used with Native, NCM, or a future provider without provider-name branches.

Active reports may retain typed provider outputs for product-path tests.
Observer reports retain the complete validated terminal consequence—including
structured effect evidence, receipt, generation linkage, and fallback policy—
plus conformance findings and an immutable fixture-controlled scenario
identity. Their Rust types have no field capable of carrying a provider-returned
operation payload or active product output.

The crate depends only on `tracedecay-memory-provider-api`; it has no storage,
code-index, dashboard, daemon, or provider-adapter dependency.

The focused integration test reuses the exact isolated canonical dummy-provider
source through test-only `#[path]` inclusion. That does not add a normal crate
dependency: the conformance crate still depends only on the provider API, while
the standalone dummy workspace and its product checker remain independent.

## The adversarial provider double

`adversarial::AdversarialProviderV1` is a `MemoryProvider` that misbehaves on
demand: it is scripted per contact, and it records every contact it received —
including whether the caller's cancellation token was already cancelled when it
chose to answer anyway — so a host test asserts what the double actually did
rather than inferring it from the host's answer.

It exists to be registered where a real provider is registered, so the
misbehaviour travels through the host's production dispatch path. It is
deliberately payload-agnostic: the host injects an
`AdversarialPayloadSourceV1`, which is what lets the crate that owns a payload
contract forge candidate-level misbehaviour without teaching this crate a
schema. *Timed* blocking behaviours are bounded by a hard ceiling inside the
double, so a scripted "slow provider" can never wedge the suite that uses it.

The genuinely non-returning provider is a separate behaviour:
`MisbehaviourV1::NeverRepliesUntilReleased` parks the call on a `ReleaseLatchV1`
the test owns and returns at no other moment — no timer and no cancellation
ends it. That is what lets a host test prove containment of a call that is
*still running* when the host answers its own caller, instead of a call that
happened to finish first. Releasing the latch is cleanup: after the containment
assertions a test releases it so every borrowed worker leaves the provider and
the host's own worker census can be checked back to its baseline.
