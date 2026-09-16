# TraceDecay V2 Root Compatibility Migration and Replacement

## Status / role

This plan owns the accepted V1-to-V2 profile replacement contract. The final
V2 shape remains the only serving shape, but replacing a supported V1 profile
is an explicit, data-preserving operation with a bounded cutover, backup,
validation, crash recovery, and rollback path.

The currently evidenced compatibility set is the released
`v0.1.0-beta.37` profile, including the exact tagged project-store inventories
stamped as V34 and V35, into the current V2 final shape. This is a bounded
compatibility set; it does not establish support for every older, partial,
foreign, or drifted store. An input outside the accepted inventory is refused
with typed `ResetRequired` or `Incompatible` and is left untouched.

**No silent migration.** Ordinary V2 startup, open, read-only inspection, and
background maintenance validate the final shape and refuse a non-final shape.
They do not infer a version, migrate bytes, reset a target, or fall back to a
different store. Migration is available only through an explicit operator
opt-in, currently the `storage replace-v1` operation (with its visible
`migrate-v1` alias), an approved worker, and explicit confirmation. Its
`--dry-run` form performs no writes.

The replacement contract applies to the complete profile boundary, not just a
single database. The required authorities are:

- `global.db`
- `user-sessions.db`
- `user-memory.db`
- `projects`
- `enrollment.json`
- `config.toml`
- `migration-inventory`
- `profile-identity.json`

The profile is quiesced under its exclusive lifecycle fence before any
replacement phase. The old profile remains available as an immutable source
until publication succeeds and remains in an external, verified backup for
rollback and inspection.

**Memory model correction.** Facts are project-wide. Branch retirement neither
moves nor merges memory facts. The replacement worker may carry durable facts
through the explicit profile operation when they have a V2 authority; branch
selection itself has no special persistence workflow.

**Root lifecycle correction.** Direct project init, project open, read-only
open, and branch open acquire the exact profile's owned exclusive maintenance
scope and delegate to registered production authorities. The replacement
operation uses that same lifecycle boundary while it quiesces and publishes.

**Localized integrity repair.** A repair is admissible only for a corrupt,
deterministically rebuildable derivative within an otherwise exact final shape.
Whole-store or authoritative-data corruption remains fail-closed and cannot
become a hidden migration or reset.

Earlier fixture names, family inventories, packet layouts, and transition
scaffolding are historical evidence only. They do not expand the supported
replacement inventory or become runtime machinery without a separately
accepted contract.

## Accepted replacement contract

### Explicit opt-in and preflight

The operator first runs a no-write dry run and reviews the source identity,
supported shape, required authorities, destination, and planned cutover. An
apply requires explicit confirmation. The replacement coordinator then:

1. acquires the profile's exclusive lifecycle lease and quiesces writers;
2. creates a complete backup outside the profile root;
3. verifies the backup manifest, checksums, identities, and authority list;
4. rehearses restoration into an isolated rollback tree and verifies that
   rehearsal before touching the live profile;
5. runs worker preflight against the exact source inventory; and
6. records the operation, phase, source digest, backup digest, and target
   identity in a cutover journal.

The complete backup covers every required authority and the source material
needed to account for historical data. It is checksummed and independently
restorable. A backup that is incomplete, corrupt, on the live profile root, or
not bound to the exact brain/profile/project identities blocks the operation.

### Data-preserving migration

The worker reads only the explicitly admitted V1 source under the replacement
lease and writes an isolated V2 target. It must account for every required
authority, preserve canonical identities, provenance, ownership, timestamps,
receipts, source material, and durable rows for which the V2 model has an
authority, and report counts and digests that can be checked before publish.
Historical transcripts and repositories enter V2 through their ordinary,
bounded ingestion or replay path, with their source provenance retained.

Derived projections may be rebuilt in the target. Retired indexes, semantic
vector projections, normalized copies, and other non-authoritative derivatives
do not have to remain byte-identical when their canonical source and
rebuildable meaning are preserved. V1-only or unsupported records remain in
the untouched backup and are reported with a typed disposition. Unless the
preflight contract explicitly classifies such a record as out of scope and
the operator accepts that disposition, it blocks publication. It is never
silently discarded or represented as successfully migrated.

The worker must fail before publication when source validation, authority
accounting, identity binding, preservation checks, or target verification
fails. A failure leaves the V1 source and external backup usable.

### Validation and cutover

Migration is staged in an isolated target. Before publication, validation
checks the exact final V2 inventories, namespace ownership, identities,
authority coverage, source and backup tree digests, canonical row counts,
provenance, replay receipts, and the worker's committed phase report. The
coordinator rechecks the source and backup digests between phases so a source
change cannot be mistaken for a successful replacement.

Publication is a journaled cutover: quarantine the old serving root, move the
verified V2 target into its place, and synchronize the parent directory before
post-publication verification. The old root, untouched external backup, and
cutover journal remain available until the operation is verified and the
operator's retention policy permits cleanup. The V2 daemon is the only serving
authority after publication.

### Crash recovery and rollback

Every backup, rehearsal, and cutover phase has a durable marker or journal
state. After a process or machine crash, recovery inspects that state and
either resumes an idempotent incomplete phase or returns a typed
recovery-required/unavailable outcome that leaves both source and backup
preserved. An ambiguous publication is never resolved by guessing, deleting
the old root, or silently starting over.

Before successful post-publication verification, rollback restores the
untouched V1 rehearsal or external backup into the serving location, preserves
the failed V2 target in a quarantine/rollback location, synchronizes the
parent, and records `RolledBack`. A deliberate binary rollback after a
successful cutover is also explicit: stop V2, select the V1 binary and its
registered host/configuration, and restore the preserved V1 profile. V1 must
never open V2-created bytes. Writes made after a successful V2 cutover are a
new boundary and cannot be retroactively claimed as V1-compatible.

## User outcome

An operator can replace a supported V1 profile and retain its durable logical
data, source provenance, and identity while keeping a complete recovery copy
of the original bytes. The operation is reviewable before it writes,
verifiable before it publishes, recoverable after interruption, and reversible
until the operator accepts the cutover. A clean V2 reset/recreation remains
available for an unsupported or corrupt input, but it is an explicit choice
and never the hidden result of opening the profile.

## End-to-end production path

1. Discover and quiesce the exact brain/profile/project root under its
   exclusive lifecycle fence.
2. Run `storage replace-v1 --dry-run`, confirm the supported release and exact
   authority inventory, and obtain explicit operator confirmation for apply.
3. Create and verify the complete external backup, then rehearse its restore
   into an isolated rollback tree.
4. Preflight the V1 source and worker protocol. Abort without mutation on any
   unsupported shape, drift, missing authority, identity mismatch, or digest
   change.
5. Build the V2 target in isolation through the first-party replacement
   worker. Replay historical source material through ordinary V2 ingress and
   emit preservation and verification receipts.
6. Verify the target's exact final shape, authority coverage, identity,
   provenance, counts, digests, and reopen behavior.
7. Publish through the cutover journal, quarantine the old root, and fsync the
   publication boundary.
8. Reopen through the canonical V2 daemon, verify the published profile, and
   retain the old root and external backup for the rollback retention window.
9. If any phase fails or is interrupted, recover from the journal or roll back
   from the untouched rehearsal; preserve all source and failure evidence for
   diagnosis.

## Implementation slices

### Admit only an explicitly requested replacement

- Keep ordinary V2 open and maintenance paths final-shape-only and
  no-silent-migration.
- Expose one explicit replacement coordinator and a versioned worker protocol;
  do not add compatibility readers to every storage open boundary.
- Reject unknown, partial, unversioned, foreign, or drifted inventories before
  decoding or mutation, with typed `ResetRequired` or `Incompatible`.

### Preserve and validate the complete profile

- Inventory all required profile authorities and bind them to exact identities
  and source digests.
- Make external backup creation, checksum verification, restore rehearsal,
  target validation, and post-publish verification mandatory phases.
- Carry canonical logical data and source provenance through V2 authorities;
  rebuild only derivatives whose canonical source is retained.
- Keep unsupported or unmigratable records in the backup and surface a typed
  disposition rather than dropping them.

### Cut over, recover, and roll back

- Keep the source immutable while the V2 target is built and verified.
- Journal quarantine, publication, synchronization, verification, and
  rollback so crashes cannot create an unclassified mixed profile.
- Keep the external backup and failed-target quarantine until acceptance and
  the rollback retention window are complete.
- Exercise binary/service/host rollback using the preserved V1 rehearsal and
  prove that V1 never consumes V2 bytes.

## Direct acceptance and current gates

The replacement contract is accepted. The release implementation gate remains
pending. The current branch contains focused source-shape/transaction
rollback coverage and explicit replacement dry-run/confirmation and backup
rehearsal scaffolding; those checks are evidence for the contract, not a
completed release qualification.

The following gates remain pending until they pass in the normal test matrix
and in a stable aggregate run:

- a first-party worker artifact performs a successful beta.37 full-profile
  replacement, including every authority listed above;
- preserved logical data, provenance, identities, receipts, and source
  material are checked before publication, with typed dispositions for data
  that cannot be migrated;
- fault-injected backup, rehearsal, journal, publication, and restart paths
  recover without source or backup loss and without an ambiguous serving root;
- pre-publication failure and deliberate post-cutover binary/service/host
  rollback restore the V1 profile and never expose V2 bytes to V1;
- all supported CLI, daemon, hook, MCP, API, LSP, dashboard, and SDK restart
  paths observe the same cutover and recovery boundary; and
- release packaging, cross-platform behavior, and repeated canary/soak runs
  are green and reproducible.

## Explicit non-goals

- Silent background migration, implicit opt-in, automatic reset, or fallback to
  another profile.
- Unbounded support for historical or drifted store inventories.
- In-place mutation of an unverified V1 source or publication of an unverified
  V2 target.
- Treating a rebuildable derivative as canonical data, or declaring a logical
  record preserved without an authority, provenance, or typed disposition.
- Feeding V2-created state to a V1 binary during rollback.
- Declaring the replacement release complete from a dry run, a zero-data
  fixture, a focused test, or a single flaky pass.

## Optional measured package-boundary candidates

This plan does not prescribe crate-breakup sequencing, source moves, package
counts, worktrees, commits, or delivery gates. Query, code-index, convergence,
and build-performance boundaries remain independently measured capabilities.

A package boundary is retained only when a direct same-host developer journey
improves and production callers preserve public contracts, generated schemas,
packaging, feature behavior, runtime authority, and normal CI. Source scans,
line/file counts, dependency-shape tables, and moved-module layouts are
diagnostic observations, not replacement acceptance.
