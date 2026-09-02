# Product-owned upstream provenance

This directory records the immutable upstream floor beneath the product-owned
TraceDecay memory-provider branch. It is deliberately outside Zack-owned crates.
The machine-readable sync contract is [`sync-policy.json`](sync-policy.json),
and every isolated train uses the strict
[`sync-train-receipt.schema.json`](sync-train-receipt.schema.json) contract.

`tracedecay-v2-pr707.json` separates two facts that must never be conflated:

- `pinned_floor.sha` is the exact commit the product branch was created from.
- `observed_pull_request.head_sha` is only a dated observation of moving PR #707.

The current pinned floor is
<!-- pinned-floor -->`08fbe33a7c7f403191fd5d6e356c7b6681b96403`.
The verifier requires that commit to exist locally and remain an ancestor of
the checked-out product head. The marker before the SHA is the anchored floor
pin the sync train rewrites; no other prose in this directory may quote the
floor.

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

The policy revision is `sync-train.v1` and the workflow name is
`run-upstream-sync-train`. A receipt binds that revision and schema to one
terminal train. It records the product repository/ref, immutable starting
product head and floor SHA, resolved candidate upstream ref/SHA, isolated sync
branch and strategy, every conflict's exact path/source/owner/resolution and
rationale, ordered gate results, terminal state, and final ref/commit outcome.

## Run an isolated sync train

The train is reviewable and single-directional:

1. Run the [vendor-floor preflight](../../scripts/product/check-upstream-vendor-floor.py)
   from a clean checkout and resolve one approved moving discovery ref to an
   exact candidate SHA. Moving ref names are discovery inputs only; they are
   never accepted as a floor.
2. Create `refs/heads/sync/upstream/<candidate-short-sha>` at the current
   product branch head. The product branch and released `main`/`master` refs
   are never sync targets.
3. Merge or rebase the pinned candidate on that isolated branch. Record an
   entry in the receipt for every conflict path, including which side supplied
   the source, the owning authority, the selected resolution, and the
   semantic rationale. An unresolved conflict fails the train.
4. Run `advance-floor`. It rewrites the canonical metadata and every declared
   floor pin in the sync worktree, hashes the resulting working tree, records
   that candidate tree SHA in train state, commits the candidate tree on the
   isolated sync ref (compare-and-swap from the starting product head, parents
   = product head and pinned source) and checks it out, so ancestry-based
   gates see the candidate floor as `HEAD`. The released product ref does not
   move. The floor moves before any gate runs because gates are evidence
   about that tree.
5. Run required upstream gates first, followed by product contracts, Native
   parity, provider conformance, scope/crash/security journeys, and generated
   drift checks, each through `record-gate`. A gate id is only evidence
   because `sync-policy.json` `gates.lanes` binds it to one job of
   `.github/workflows/product-upstream.yml` and to the exact command lines
   that job runs; `record-gate` accepts only a declared command and passes
   the gate only once every declared command passed (see "Gate lanes are the
   proof"). Every gate is bound to the candidate tree: the working tree must
   hash to the recorded SHA before and after the gate command, and externally
   executed evidence must name that SHA with `--tree-sha`. A required failure
   cannot be hidden by a later product pass, and a gate that changes the tree
   fails.
6. Run `publish`. It refuses if the working tree no longer hashes to the gated
   candidate tree or if any gate was recorded against another tree, then
   commits the candidate tree plus the convergence receipt (the only permitted
   difference, proven with `git diff-tree`) and updates the isolated sync ref
   with compare-and-swap against the recorded starting product head and the
   pinned candidate. The released product ref remains unchanged in this
   workflow. The receipt records the gated tree SHA; the commit SHA is recorded
   after the ref transaction because a commit cannot embed its own SHA. A later
   promotion may only fast-forward a released ref; force and non-fast-forward
   updates remain prohibited.
7. On any failure, conflict, gate failure, or CAS mismatch, abort without
   changing the canonical floor metadata or released product ref. A retained
   sync branch is review evidence only and is not a floor update.

The first actual floor advancement remains the separate `tdmem-1208`
rehearsal. This bead defines the contract and workflow policy; it does not
move `pinned_floor.sha`.

The accepted floor is only ever `pinned_floor.sha` in the canonical metadata;
the first floor was the PR #707 creation head. The dated
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

## Floor pins advance together

`sync-policy.json` `floor.pins` declares every file that hard-pins the accepted
floor SHA outside the canonical metadata, and each pin names exactly where:

- `json_pointer` pins list the JSON pointers whose string value embeds the
  floor (`sync-policy.json` `/floor/sha`, the convergence map
  `/upstream_floor_sha`, the patch-footprint policy `/upstream_floor/sha` and
  `/verification/branch_diff_command`). The document must already be canonical
  two-space JSON; it is re-serialized with key order preserved so the diff is
  only the moved values. `each_pointers` (the convergence map's
  `/areas/*/last_verified_upstream_sha` and
  `/entries/*/last_verified_upstream_sha`) are wildcard pointers: every
  active area and mapped patch must stamp the accepted floor as its last
  verified upstream commit or `check-upstream-ownership-registry.py` fails, so
  the train advances every stamp together with `upstream_floor_sha`. The stamp
  is a claim the train must then prove, and the proof is enforced rather than
  delegated: `advance-floor` collects every `verification`/`tests` command of
  each stamped area and entry and refuses, before any ref moves, if some
  command is run by no declared gate lane; `publish` refuses unless every one
  of those commands passed against the candidate tree, and the receipt's
  `floor_advancement.verification_coverage` reports the stamped targets,
  required, covered and uncovered commands, and per-lane counts. A failing
  gate aborts the train and no stamp is published. The receipt reports how
  many occurrences came from wildcard stamps as `each_occurrences`.
- `derived_metadata_receipt` (`pr707-floor.json`) advances `/pinned_floor_sha`
  and recomputes `/canonical_metadata_blob_sha` from the advanced metadata.
- `anchored_line` pins replace one exact line: `EXPECTED_FLOOR = "…"` in the
  footprint checker and vendor-floor test, `FLOOR = "…"` in the ownership
  registry test, the expected `pinned_floor_sha` line in
  `product_upstream_floor_test.sh`, and the `<!-- pinned-floor -->` line in
  this README. Other prose references the canonical metadata instead of quoting
  the floor.

Each pin declares its exact `occurrences`; every SHA a pin accounts for must
occur in the file exactly as often as its declared targets explain, so an
undeclared literal cannot hide inside a declared file. The pins contract is
enforced, not asserted: `prepare` runs `git grep` for the floor over the
product head and refuses any hit that is not the canonical metadata, a
declared pin, declared `archival_provenance`, or a file under a declared
`historical_record_prefixes` directory (`.beads/` receipts and operation logs
quote the floor they were produced under). `advance-floor` re-sweeps the
candidate tree for both the previous and the candidate floor, and `publish`
sweeps the published tree again. Files under `floor.archival_provenance` (the
measured V2 baseline, its checker and fixture, and the M0 go/no-go report) are
recorded by blob SHA and must be byte-identical before and after the train: a
new floor needs a re-measured baseline in a follow-up commit.

## Gate lanes are the proof

`sync-policy.json` `gates.lanes.<gate_id>` names the workflow file, the job
id, and the exact command lines (the `for` loop of `product_contracts`
expanded to one `python3 <test>` line each) for all six required gates. The
binding is checked in both directions:

- `record-gate` reads the workflow file from the candidate commit and refuses
  a gate whose declared commands are not lines of the bound job, so the policy
  cannot claim commands CI does not run and the workflow cannot silently drop
  one the policy still counts on.
- `--command-json` executes one declared command, either as that line's argv
  or as `bash -euo pipefail -c "<line>"`, the exact prelude the CI lane uses.
  An argv that matches no declared command is refused, not recorded.
- External evidence (`--status passed --tree-sha <candidate tree>`) must
  name what it proves: `--command <JSON argv>` for one declared command that
  ran elsewhere, or `--ci-run https://github.com/BleedingDev/tracedecay/actions/runs/<id>`
  with `--ci-head-sha <candidate commit>` for a run of the bound lane in the
  product repository, which stands for every declared command of that lane.
  Free-text evidence alone is refused.
- A gate is `in_progress` until every declared command has a passed record
  against the candidate tree; `publish` refuses an `in_progress` gate the same
  way it refuses `not_run`. Each gate record carries its `commands` (command,
  source, status, exit code, evidence, tree SHA, time) and a `coverage`
  summary.

Reconciling the convergence map with the lanes is therefore part of the
train, not prose: a map command that no lane runs verbatim (`kache cargo --`
wrappers, alternate flag orders) blocks `advance-floor` until either the map
or the workflow and policy are changed in a reviewed commit.

## Rehearsal records

`product/upstream/rehearsals/` holds reviewable records of rehearsed trains
that did not advance the floor (the archived classification, every conflict
receipt with owner/resolution/rationale, the refusals, and the aborted
terminal receipt). They quote the floor they rehearsed against and are
declared under `floor.archival_provenance`, so a later train leaves them
byte-identical.

## Roll back a finalized train

Scope: rollback covers only an unpromoted train, one whose published commit
still lives solely on `refs/heads/sync/upstream/<short>`. The released product
ref is never an update target of this workflow, so such a train is withdrawn,
not reverted:

```bash
python3 scripts/product/run-upstream-sync-train.py rollback \
  --repo . --train-dir "$train_dir" [--retain-sync-ref]
```

`rollback` requires a `finalized` train (an unfinalized one is `abort`ed),
verifies the product branch still equals the recorded starting head, proves
the canonical metadata and every declared pin at that head still carry the
starting floor, restores the product checkout if the sync branch was checked
out, and deletes the sync ref with a compare-and-swap on the exact finalized
commit (or keeps it as review evidence with `--retain-sync-ref`). It writes a
`rolled_back` terminal receipt naming the withdrawn commit. A product ref that
root already fast-forwarded to the train is a promotion: reversing a promoted
floor would need a reverse train that reverts the merge and moves every pin
back through the same structured mechanism, and that path does not exist yet.
`rollback` refuses a promoted ref with a typed error and never force-updates a
released ref.

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
baseline, updates the convergence map, and receives review before merge. The
only executable path is the sync train above: `prepare`, one `record-conflict`
per Git conflict, `advance-floor`, the ordered `record-gate` runs that cover
every declared lane command against the candidate tree, and `publish`, which
commits that gated tree with the receipt.
