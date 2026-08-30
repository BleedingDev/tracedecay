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

python3 tests/product_upstream_vendor_floor_test.py

python3 scripts/product/check-upstream-ownership-registry.py --repo .

python3 tests/product_upstream_ownership_registry_test.py
```

Use `--require-product-branch` when the checkout must be the declared product
branch rather than a detached CI commit or a review branch.

The schema-v2 `convergence-map.json` is the machine-readable ownership
authority for current M2 paths. The ownership checker classifies every changed
path from the immutable floor through the working tree, including untracked
files. Product-owned paths must resolve to exactly one active area; every
upstream-owned change requires one exact active entry bound to its policy touch
point. Planned and retired rows grant no current authority. Computed counts are
printed to stdout and are not copied into stored snapshots or receipts.

## Remote, ref, and ownership contract

`sync-policy.json` is the executable contract for upstream discovery and sync
isolation:

- `origin` fetches the product repository, `BleedingDev/tracedecay`.
- `upstream` fetches Zack's source repository,
  `ScriptedAlchemy/tracedecay`; the sync workflow never pushes to it.
- `refs/remotes/upstream/master` and
  `refs/remotes/upstream/pr/707-current` are moving discovery refs. Resolve one
  to a commit before analysis; never use the moving name as an accepted floor.
- `refs/heads/feat/pluggable-memory-providers-v2` is the product integration
  branch. A sync runs only on a new branch beneath
  `refs/heads/sync/upstream/`; `main`, `master`, and the product integration
  branch itself are rejected as direct sync targets.
- `BleedingDev` is the one sync owner and owns product patches.
  `ScriptedAlchemy` owns review of claims about upstream intent. Ownership does
  not replace focused behavioral verification.

The immutable accepted floor remains PR #707 creation head
`08fbe33a7c7f403191fd5d6e356c7b6681b96403`. The dated
`observed_pull_request` head is discovery evidence and does not move that
floor.

## Start an isolated sync

Fetch the moving discovery refs, resolve the intended candidate, then create a
clean isolated branch at the current product head:

```bash
git fetch upstream +refs/heads/master:refs/remotes/upstream/master
git fetch upstream +refs/pull/707/head:refs/remotes/upstream/pr/707-current

candidate_ref=refs/remotes/upstream/master
candidate_sha=$(git rev-parse --verify "${candidate_ref}^{commit}")
candidate_short=$(printf '%.12s' "$candidate_sha")
git switch feat/pluggable-memory-providers-v2
git switch -c "sync/upstream/$candidate_short"

python3 scripts/product/check-upstream-vendor-floor.py \
  --repo . \
  --source-ref "$candidate_ref"
```

The preflight refuses detached heads, dirty tracked or untracked trees,
unapproved moving refs, mismatched remotes, non-descendant product history,
branches not under `sync/upstream/`, direct `main`/`master` work, and sync
branches that do not start exactly at the current product head. Its output
identifies the resolved candidate SHA; it does not write a receipt or mutate
the checkout. A candidate may be a floor descendant, behind the PR floor, or
diverged from it; the output reports that relationship and the common merge
base for downstream classification. A candidate with no common ancestry is
rejected.

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
