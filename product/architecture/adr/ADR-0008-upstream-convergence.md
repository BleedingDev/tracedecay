# ADR-0008: Converge with upstream through additive product ownership, mapped exceptions, and isolated sync trains

Status: Accepted  
Date: 2026-08-30

## Context

The product starts from the immutable TraceDecay V2 PR #707 floor while Zack's upstream continues to evolve. Product work must remain easy to identify, remove, compare, and rebase. Continuous opportunistic rebasing or unexplained edits to existing upstream files would make regressions and authority drift impossible to attribute.

`product/upstream/patch-footprint-policy.json` defines initial quantitative caps, allowed touch points, forbidden zones, dependency directions, and the convergence-map contract.

## Decision

Keep product-owned implementation additive wherever possible. Every intentional edit to an existing upstream-owned file requires exactly one active entry in `product/upstream/convergence-map.json` before the bead closes. The entry names the allowed touch point, rationale, semantic invariants, executable verification, owning beads, line budget, and rebase/removal plan.

A forbidden/exception-zone edit requires a versioned ADR before implementation, rejected port-level alternatives, explicit policy revision when a hard cap changes, focused parity/recovery evidence, and rollback plan.

Advance the accepted upstream floor only through an isolated sync train:

1. pin a reviewable candidate upstream commit;
2. create an isolated sync branch from the current product floor;
3. apply upstream changes according to the declared strategy;
4. classify conflicts by upstream/product ownership and authority;
5. preserve source and resolution rationale in conflict receipts;
6. run upstream-required parity first, then product contracts, Native parity, provider conformance, scope/crash/security journeys, and generated-drift checks;
7. atomically update floor metadata, convergence receipts, and code only after all required gates pass;
8. never force-update the released product branch.

External lessons are source-linked by repository, commit, license, extracted invariant, neutral tests, target capability, implementation bead, and rejection rationale where applicable.

## Consequences

- Upstream failures and product regressions remain attributable.
- Most provider implementation can be removed by deleting product-owned crates and narrow mounts.
- Syncs happen in reviewable trains rather than every upstream commit.
- Small upstream mounts carry documentation and test overhead.
- The floor may lag moving upstream intentionally until a train is justified and reviewable.
- Generated outputs are reproduced from their owner, never hand-patched.

## Rejected alternatives

- **Continuous unreviewed rebases on every upstream change.** Rejected because conflict decisions and parity evidence would be fragmented and hard to reproduce.
- **Merge moving upstream branch names without pinning commits.** Rejected because the resulting floor would not be reproducible.
- **Unmapped upstream-owned edits.** Rejected because future sync agents could not distinguish product invariant from accidental drift.
- **Treat every conflict as product-owned.** Rejected because upstream behavior must remain authoritative outside accepted product seams.
- **Patch generated files manually.** Rejected because source/generator authority and reproducibility would be lost.
- **Force-update the product branch after a sync.** Rejected because released history and receipts must remain auditable.

## Invariants

1. The current product floor is an immutable commit SHA, never a moving branch name.
2. Product-owned additions and upstream-owned edits are reported separately.
3. Every current upstream existing-file diff has exactly one active convergence-map entry; every active entry has a current diff.
4. Hard patch caps cannot be raised silently.
5. Exception-zone work has an ADR before the edit and a rollback/removal plan.
6. Upstream-required suites run before product suites and their failures cannot be hidden.
7. Generated output matches its declared generator/resolver.
8. A failed/aborted sync train leaves the accepted floor and product branch unchanged.
9. Successful floor updates change metadata, code, and receipts atomically.

## Verification

Executable beads:

- `tdmem-0308` — product ownership and dependency/convergence guards.
- `tdmem-1201` and `tdmem-1202` — upstream observation and candidate-floor pinning.
- `tdmem-1203` and `tdmem-1204` — patch budget plus invariant/parity enforcement.
- `tdmem-1205` — isolated sync-train workflow and conflict receipts.
- `tdmem-1206` — upstream parity plus product regression CI.
- `tdmem-1208` — first full convergence-train rehearsal.

The patch-footprint checker validates the live diff against the pinned floor and rejects unmapped edits, stale entries, cap violations, forbidden dependencies, unsupported exceptions, and generated drift.

## Review triggers

Review when the first sync train runs, upstream restructures an allowed mount, a hard patch cap is approached, a new exception zone is proposed, an external source is imported, or release history strategy changes. Any force-update or moving-floor proposal requires a superseding ADR.
