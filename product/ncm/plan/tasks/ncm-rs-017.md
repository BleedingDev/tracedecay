# ncm-rs-017 — Implement the supervised Rust worker and bounded wire transport

Status: planned, not executed.
Owner: Runtime / lifecycle.
Dependencies: ncm-rs-014.
External gates: none.

## Objective

Isolate native inference/storage failures while fitting the existing host supervision contract.

## Owned paths

- `crates/tracedecay-memory-ncm-runtime/src/worker/`
- `crates/tracedecay-memory-ncm-runtime/src/wire/`
- `crates/tracedecay-memory-ncm-runtime/src/client/`
- `crates/tracedecay-memory-ncm-runtime/src/bin/tracedecay-ncm-worker.rs`
- `crates/tracedecay-memory-ncm-runtime/tests/worker.rs`

## Implementation

1. Provide a real Rust worker executable with a versioned length-delimited local pipe protocol. Wire payloads contain opaque namespace/source handles and typed engine requests, not raw host scope or caller filesystem paths.
2. Use one bounded owner/mailbox and explicit state-root admission. Reuse host launch/cancellation/restart facilities through a narrow client contract; do not build a second general supervisor.
3. Bound process count, queued frames/bytes, stdout framing, stderr diagnostics, readers, loaded namespaces and model residency. Reject oversized frames before allocation.
4. On deadline/cancellation, stop cooperative work or terminate the worker within a fixed escalation budget. Return unknown effect if commit cannot be determined; reconcile by idempotency receipt after restart.

## Acceptance

1. Real process death, hangs, malformed replies, full pipe/backpressure, deadline expiry and restart budget exhaustion are exercised.
2. No orphan worker or unbounded detached task survives shutdown; native inference cannot monopolize a Tokio host thread.
3. The worker refuses wrong protocol/epoch/model identities and cannot launch when the provider is Disabled.
4. Process-alive is distinct from loaded-state and observe/recall readiness.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-runtime --test worker --locked

## Sources

- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
