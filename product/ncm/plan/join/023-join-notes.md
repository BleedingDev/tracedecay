# ncm-rs-023 join notes

**Update 2026-09-06 (afternoon):** at the user's instruction the host head 3881741df
(`origin/feat/pluggable-memory-providers-v2`, identical to the local branch) was merged into
`feat/ncm-biomem-rust-v1` as dc0166d23 with no conflicts, the fixture patch below was applied to
`tests/product_memory_dependency_direction_test.py`, and `scripts/product/ncm/check-backend.py`
now fences the branch footprint against `host_base_commit` from `product/ncm/bootstrap/ownership.json`
(the joined host head) instead of the product checkpoint. This is a merge the user asked for, not a
HOST-STABLE declaration by the stabilization owner, and the Beads import is still pending. Verification
of the joined tree is recorded in `product/ncm/receipts/integration/`.


Prepared 2026-09-06 by the backend coordinator so the join owner can consume the
backend branch without rediscovering the policy work. Nothing here claims task 023
or any later task complete.

## What the backend branch already carries (023-owned paths)

- `product/architecture/memory-dependency-policy.json`
  - package contracts for `tracedecay-memory-ncm-core` (serde only) and
    `tracedecay-memory-ncm-runtime` (core, serde, serde_json, sha2, rusqlite[bundled],
    fastembed[hf-hub-rustls-tls, ort-download-binaries-rustls-tls] behind `real-encoder`);
  - `tracedecay-memory-provider-ncm` allows the one optional, default-features-off edge to
    `tracedecay-memory-ncm-runtime` (feature `rust-backend`);
  - rule `ncm-backend-is-host-blind`: the backend crates may not name `tracedecay`,
    `tracedecay-*`, or `ocean*`; the only in-workspace edge is runtime -> core;
  - rule `ncm-adapter-cannot-reach-tracedecay-internals` admits exactly that runtime edge;
  - a source contract for `tracedecay-memory-provider-ncm` pinning the nine runtime items the
    adapter may import (worker client/options/error, state root, wire operation/request/reply,
    engine outcome/rejection). The adapter names no process, filesystem, socket, store, or
    executor symbol; the worker process is owned by the runtime crate.
- `product/upstream/convergence-map.json`: area `ncm_backend` (product_owned, feature
  `ncm-rust-backend`) covering both backend crates and `product/ncm/**`, hanging off bead
  `tdmem-0304` until the work package is imported into Beads at 023.
- `product/upstream/patch-footprint-policy.json`: unchanged. Adding the crates to
  `product_owned_paths` is refused by the existing footprint unit test's exact pattern list;
  the area alone classifies every backend path, so no change is needed there.

## Checker state on the backend branch (HEAD of feat/ncm-biomem-rust-v1)

| Check | Result on backend branch | Result on host branch 3881741df |
|---|---|---|
| `scripts/product/check-memory-dependency-direction.py` (real graph) | 2 violations, both pre-existing at base 25778c744 and unrelated to NCM (`fabric -> tracing`, registry `serde_json raw_value`) | fixed there (workstream A) |
| `tests/product_memory_dependency_policy_test.py` | OK (9) | OK |
| `tests/product_memory_dependency_direction_test.py` | 2 pre-existing failures (tokio `time` feature, registry executor call site) plus 4 caused by the synthetic fixture not knowing the backend crates or the second source contract (see fixture deltas) | OK |
| `tests/product_patch_footprint_policy_test.py` | 830 diagnostics, identical count to the host branch HEAD; zero mention NCM paths | 830 (floor advance pending) |
| `tests/product_ncm_surface_audit_test.py` | OK (27) | n/a |
| `git merge-tree --write-tree 4e35bef18 3881741df` | clean, no conflicts | |

## Fixture deltas the join owner must apply with the policy (test edits are 023 work)

`tests/product_memory_dependency_direction_test.py`:

1. `valid_metadata()` must add managed packages `tracedecay-memory-ncm-core`
   (`serde[derive]`; dev `serde_json`) and `tracedecay-memory-ncm-runtime`
   (`tracedecay-memory-ncm-core`, `serde[derive]`, `serde_json`, `sha2`, `rusqlite[bundled]`,
   optional `fastembed[hf-hub-rustls-tls, ort-download-binaries-rustls-tls]`; dev `tempfile`),
   and give `tracedecay-memory-provider-ncm` its optional `tracedecay-memory-ncm-runtime`
   edge with `uses_default_features = false`. Otherwise the checker reports
   "managed package is missing from cargo metadata".
2. `check_with_source()` copies only the registry crate into the temporary repo. It must also
   copy `crates/tracedecay-memory-provider-ncm/src`, or the second source contract reports
   no readable source and stale allowances.
3. `test_stale_import_allowance_is_refused` appends to
   `contract["allowed_imports"]["tracedecay_application"]` for every source contract; it must
   skip contracts without that root (the adapter contract only has
   `tracedecay_memory_ncm_runtime`).

The host branch's own fixture changes (54 lines in the same test) merge cleanly with these.
The exact deltas are in `023-direction-test-fixture.patch` next to this file. Verified on a
rehearsal merge of the backend branch (54bdf0c1c) with host head 3881741df: with the patch
applied the direction unit test passes 27/27, the real-graph checker reports
"memory dependency direction verified", and the footprint checker mentions no NCM path.

## Still gated

- HOST-STABLE has not been declared by the stabilization agent; 3881741df is the candidate
  host head at the time of writing, not an accepted tree.
- Beads import of the `ncm-biomem-rs.v1` work package (`product/ncm/plan/task_dag.json`).
- Task 024 mount; the host has no reference to `tracedecay-memory-provider-ncm` yet, so
  host-level NCM behaviour is unreachable on the joined tree.

## Join verification (ce4b8fb20)

The actual merge (dc0166d23, host head 3881741df) landed with the fixture patch and the
scoped footprint fence in f94a67076. On ce4b8fb20: workspace `cargo check --all-targets
--locked` ok, all five host lanes pass (hygiene, registry-contract, provider-neutral crates,
retention nextest lane, Claude host journey), and `check-backend.py` passes with zero
blockers (six populations green, journey pass, ownership fence clean). Receipt:
`product/ncm/receipts/integration/join-verification-ce4b8fb20.json`.
