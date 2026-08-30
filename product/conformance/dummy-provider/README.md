# TraceDecay Memory Provider V1 Dummy Provider

This crate is the executable conformance proof for the mandatory Memory Provider V1 surface. It is intentionally small, deterministic, capability-poor, and independent from TraceDecay runtime/storage/code-index crates.

## Implemented capabilities

Mandatory:

- `provider.health.v1`
- `observation.accept.v1`
- `recall.query.v1`

Optional only to prove deterministic persistence:

- `snapshot.export.v1`
- `snapshot.restore.v1`

Every other optional V1 lifecycle capability returns the typed `capability_unsupported` terminal result. Unsupported behavior never silently falls back to Native or another provider.

## State model

Provider-local state is a deterministic `BTreeMap` keyed by the observation idempotency key. New observations require the next monotonic source sequence and the expected provider-state generation. A retry with the same key and identical canonical fingerprint returns `DuplicateAcknowledged` without a second effect. The same key with different canonical content returns typed conflict.

Recall is a bounded, case-sensitive substring search over the deterministic map order. It returns advisory candidates only. Zero matches produce `SuccessZeroResults`, not failure or fallback.

## Request control and scope

Every operation checks exact scope digest before reading or mutating provider state. Already-cancelled calls return `Cancelled`; zero remaining budget returns `DeadlineExceeded`. Cancellation and timeout are distinct. Health, recall, and snapshot export are read-only and never advance state generation.

## Snapshot and restart

Snapshots use a canonical length-prefixed binary encoding with a fixed magic/version header. They bind provider ID, exact scope, state generation, acknowledged sequence, idempotency keys, observations, payload digests, and opaque optional extensions. Snapshot bytes and SHA-256 are deterministic for identical provider state.

Restore requires matching provider ID, scope, digest, and snapshot metadata. It is idempotent for an identical current snapshot and refuses implicit overwrite of different non-empty state. Corrupt or incompatible snapshots return typed contract or state incompatibility outcomes rather than resetting silently.

## Unknown extensions

Unknown optional observation extensions are preserved byte-for-byte in provider state, snapshot, restore, and recall, but never activate behavior. Unknown required extensions return typed unsupported before any provider effect.

## Rust discipline

The crate:

- uses Rust 2024 and a pinned minimum Rust version;
- forbids unsafe code;
- denies warnings, missing documentation, `unwrap`, `expect`, `panic`, `todo`, `unimplemented`, `dbg`, and direct stdout/stderr printing;
- has no path dependency on TraceDecay internals;
- includes the deterministic generated provider-neutral contract bindings;
- is an isolated Cargo workspace so M1 can prove implementability before M2 mounts crates into the main monorepo.

## Verification

```bash
cargo fmt --manifest-path product/conformance/dummy-provider/Cargo.toml --all -- --check
cargo clippy --manifest-path product/conformance/dummy-provider/Cargo.toml --all-targets --locked -- -D warnings
cargo test --manifest-path product/conformance/dummy-provider/Cargo.toml --locked
python3 scripts/product/check-dummy-provider-conformance.py --repo .
```

The conformance suite covers compatible handshake and health, exact scope, cancellation, deadline, idempotent duplicate observation, idempotency conflict, source-sequence and generation conflicts, deterministic recall and zero results, optional-extension round-trip, required-extension rejection, deterministic snapshot/restart, incompatible restore, and explicit unsupported optional lifecycle calls.
