# Approved legacy and replay changes — applied handoff

Root approved both constituent patches after full review and authorized exact
application after cc1385 ended. The combined patch was applied once. All five
files matched their reviewed candidates immediately afterward; reverse-apply,
formatting and scoped diff checks passed. Later owner changes remain separate.

`apply.patch` is the exact original baseline → approved legacy → approved replay
composition. Apply this combined patch once; do not also apply the constituent
patches. The manifest pins each approved input patch and every baseline/final
file hash. The five final files are readable under `candidate/`. The two
constituent directories retain their full protocol/evidence/validation details.

This preserves all 192 proposed actions and the immutable original 190-action
failure evidence. Formatting and patch checks pass. No Cargo, models, actual
provider runs or real-store actions have been performed by this worker.


## cc1391 physical-audit limitation

The actual NCM result at
`target/test-profile/ncm-common-real-84460-1789050181653195000/summary.json`
remains 192 planned, 170 passed, 1 failed, 9 unknown and 12 degraded; legacy delivery
identity is unknown. The first physical migration audit fails with SQLite
`database is locked (5)` and blocks the eight following migration actions.

This is persistent ownership, not transient startup contention.
`crates/tracedecay-memory-ncm-runtime/src/store/mod.rs:1235` configures zero busy
timeout, requires `locking_mode=EXCLUSIVE` at line1239, enables WAL at line1247,
and acquires the lock with `BEGIN IMMEDIATE; ROLLBACK` at line1260.
`common_advisory_factory.rs:1194` reopens that namespace before the live external
point read at line1262; the separate sqlite3 connection is created at line1642.
A helper busy timeout would consume the same deadline without releasing the
worker's exclusive connection. A safe fix requires a separately authorized owner
read mechanism, beyond the bounded helper-only scope.

Root stopped this investigation. No retry patch, new production inspection seam,
extra restart, worker stop, database change, fabricated identity or assertion
weakening was made. All nine unknown results and the 192-action denominator are
retained. The older 190-action failure artifacts remain unchanged as well.
