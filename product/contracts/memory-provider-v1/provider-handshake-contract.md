# Memory Provider Handshake Contract V1

`provider-handshake-contract.json` binds readiness to compatible identity, exact scope, capabilities, limits, deadline, and cancellation. A running process, open socket, database path, or non-empty state never proves provider readiness.

## Preconditions

TraceDecay first resolves exactly one registration through `tracedecay.memory.provider.registry.v1`. The request carries the exact provider ID and registration revision plus:

- supported protocol ranges and adapter contract version;
- host implementation identity;
- every required capability;
- finite host ceilings for all V1 limits;
- exact profile/project/repository/worktree/branch/agent-session identity and scope revision;
- request identity;
- absolute deadline plus monotonic remaining budget;
- a live cancellation token;
- a 32-byte challenge nonce.

An expired deadline or already-cancelled token terminates before provider contact.

## Provider response

The selected adapter returns the exact provider ID, opaque runtime instance ID, verifiable implementation identity, selected protocol version, provider state identity, declared capabilities, finite provider ceilings, accepted exact scope, challenge response, readiness state, and bounded warnings.

Implementation identity includes version, build identity, artifact SHA-256, license identity, source provenance, adapter contract version, and state schema version. Runtime location is diagnostic only: PID, socket, database path, or process order cannot become identity.

State identity binds provider ID, namespace, schema version, exact scope digest, and state generation. Paths are never authority.

## Compatibility and negotiation

Handshake readiness is deterministic:

1. Resolve the accepted registration revision.
2. Reject terminal deadline/cancellation before provider contact.
3. Require exact provider-ID echo and valid challenge response.
4. Require compatible adapter contract and a mutually supported protocol major/minor.
5. Require implementation identity and state schema compatibility.
6. Require provider state owner, namespace, and exact scope match.
7. Require every requested capability in both registration and handshake declarations.
8. Negotiate each effective limit as `min(host_ceiling, provider_ceiling)`.
9. Reject missing, zero, unknown, overflowed, or out-of-catalog limits.
10. Issue an expiring scoped readiness receipt only after all checks pass.

Protocol major compatibility needs an exact intersection. Minor selection chooses the highest mutually supported minor within that major. There is no implicit cross-major downgrade.

## Exact scope

The scope contains profile, project, repository, worktree, branch, agent session, and scope revision. Wildcards, CWD inference, provider path inference, and “closest project” routing are forbidden. A missing authority is `scope_unavailable`; a different authority is `scope_mismatch`.

The canonical exact-scope digest is SHA-256 over this byte sequence, without separators or any additional canonicalization:

1. the ASCII bytes of `tracedecay.memory-provider.exact-scope.v1` followed by one NUL byte (`0x00`);
2. `profile_id`, `project_id`, `repository_identity`, `worktree_identity`, `branch_identity`, and `agent_session_id`, in that order, each encoded as its UTF-8 byte length in one unsigned 64-bit big-endian integer followed immediately by those UTF-8 bytes; and
3. `scope_revision` as one unsigned 64-bit big-endian integer.

The digest output is exactly 64 lowercase hexadecimal characters. Lengths count UTF-8 bytes, not Unicode scalar values or characters. No field may be omitted, reordered, normalized, NUL-terminated, or concatenated without its length prefix.

The fixed golden vector is `profile-1`, `project-1`, `repo-1`, `worktree-1`, `refs/heads/main`, `session-1`, revision `7`; its digest is `aa2f1ac9c33a448fb824abf783a6d40ab52050d91bcc580d907e6b0a3303938e`.

## Limits

Every handshake negotiates finite positive ceilings for request bytes, response bytes, observation batch items, recall candidates, concurrent operations, operation duration, snapshot bytes, and inspection items. The host may clamp further per request; a provider may never exceed the effective value.

“Unlimited”, zero, absent, unknown, overflowed, or provider-only limit fields fail closed.

## Deadline and cancellation

Deadline and cancellation reach the concrete provider operation in every execution topology. The provider receives remaining time, not a fresh timeout. Deadline extension is forbidden. Cancellation requires bounded stop and cannot be converted into success.

A later contract (`tdmem-0206`) defines terminal and partial-effect semantics for operations that may already have committed provider-local state. Handshake itself is read-only, so its cancellation/deadline outcomes have no committed effect.

## Readiness receipt

A ready result is an expiring `MemoryProviderReadyReceiptV1` bound to:

- provider and runtime instance;
- registration revision;
- implementation identity;
- selected protocol;
- state identity/generation;
- exact scope revision;
- declared capabilities;
- effective limits;
- canonical handshake transcript.

The receipt is not portable across provider restart, registration revision, scope revision, or incompatible state generation. Observation, recall, or provider-local mutation requires a current compatible receipt.

## No side effects and no fallback

Handshake cannot mutate provider state, TraceDecay state, Native facts, configuration, or final context. Every non-ready condition is typed. Failure never silently falls back to another provider, including `tracedecay.native`, and never returns an empty successful readiness result.

## NCM and OCEAN gates

This contract is topology-neutral. It does not select an NCM transport or process model; `tdmem-0701` and `tdmem-0702` remain mandatory. OCEAN remains an identity reservation without a versioned implementation specification.
