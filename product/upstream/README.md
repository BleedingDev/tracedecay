# Product-owned upstream provenance

This directory records the immutable upstream floor beneath the product-owned
TraceDecay memory-provider branch. It is deliberately outside Zack-owned crates.

`tracedecay-v2-pr707.json` separates two facts that must never be conflated:

- `pinned_floor.sha` is the exact commit the product branch was created from.
- `observed_pull_request.head_sha` is only a dated observation of moving PR #707.

The current pinned floor is
`08fbe33a7c7f403191fd5d6e356c7b6681b96403`. The verifier requires that commit
to exist locally and remain an ancestor of the checked-out product head.

## Verify

```bash
python3 scripts/check-product-upstream-floor.py \
  --repo . \
  --metadata product/upstream/tracedecay-v2-pr707.json

bash tests/product_upstream_floor_test.sh
```

Use `--require-product-branch` when the checkout must be the declared product
branch rather than a detached CI commit or a review branch.

## Refresh the observed PR snapshot

1. Read PR #707 metadata from GitHub.
2. Update only `observed_pull_request`, including `retrieved_at`, base SHA, and
   head SHA.
3. Run both verification commands above.
4. Commit the snapshot refresh with its bead ID and evidence.

## Move the pinned floor

Do not edit `pinned_floor.sha` as routine maintenance. A new floor requires a
separate convergence bead that records old/new SHAs, explains whether the
change is ancestry-preserving or a deliberate transplant, runs the upstream
baseline, updates the convergence map, and receives review before merge.
