# Native source baseline and protected boundary

## Answer

The strongest source baseline for the complete original Native implementation is
`b3b43410e47115056f2066449aafa1822bbb6049` (`refs/remotes/upstream/pr-707-latest`).
It is authored by ScriptedAlchemy, is the second parent of the current product
merge `571daf3a9612e5247443e4da3a107b542686c1ef`, and is an ancestor of that
head. The baseline is therefore present in the checked-out tree and includes the
upstream moves that put the fact store and memory model in
`tracedecay-session-memory`.

This is the fixed restoration baseline already authorized and merged into the
product head. Product metadata still pins the historical August floor
`5749e4fcfe268e17bd19a0e6ef90c646f7b37289`; that floor is not an ancestor of b3
(their merge base is `b6cdc66277598d1c07d335e895908397e9763636`). The stale
metadata must be reconciled truthfully in the future product plan; it does not
supersede b3, require a rollback, or gate restoration work. Use b3 as the
immutable code/behavior reference for the Native restoration plan.

Native is the complete upstream fact-store and memory-use-case implementation,
not the provider wrapper. The protected implementation is the full
`crates/tracedecay-session-memory/src/fact_store/**` and
`crates/tracedecay-session-memory/src/memory/**` trees, including their tests and
helpers. `facts.rs` and the runtime/store layers select and host that authority;
they do not provide a replacement algorithm. Product provider code must delegate
to these Native operations and retain their ownership, ranking, state effects,
failures, and recovery behavior.

## Revision and ancestry evidence

| Question | Evidence | Finding |
|---|---|---|
| What is the source revision? | `b3b43410e47115056f2066449aafa1822bbb6049`, local ref `upstream/pr-707-latest`, author `ScriptedAlchemy`, commit `fix(tracedecay): retire the context scout owner on standalone close`; immutable [upstream commit](https://github.com/ScriptedAlchemy/tracedecay/commit/b3b43410e47115056f2066449aafa1822bbb6049) | Latest locally available upstream-authored revision selected for source review. Its one-file change is in `tracedecay/queries/meta.rs`, outside the Native implementation trees. |
| Is it integrated in the current product head? | `git rev-list --parents -n 1 HEAD` returns `571daf3a9612e5247443e4da3a107b542686c1ef ad589107fa9d7e5d915fd416144ca325ef630d1d b3b43410e47115056f2066449aafa1822bbb6049`; `git merge-base --is-ancestor b3 HEAD` succeeds | b3 is the direct second parent and a real ancestor. The first parent carries product additions; b3 remains the upstream side of the merge. |
| What does product metadata record? | [`tracedecay-v2-pr707.json:15-19`](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/product/upstream/tracedecay-v2-pr707.json#L15-L19) pins `5749e4fc…` and requires ancestry; [`pr707-floor.json:11-15`](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/product/upstream/pr707-floor.json#L11-L15) repeats that pin | F is a stale historical metadata floor. It does not supersede the fixed b3 restoration baseline. |
| Do F and b3 describe one linear floor? | `git merge-base F b3` = `b6cdc66277598d1c07d335e895908397e9763636`; `git merge-base --is-ancestor F b3` fails | They diverge. Record the divergence and reconcile the metadata explicitly in the future product plan; no new upstream train is required for this restoration baseline. |
| What does the older integrated candidate record? | [`sync-policy.json:161-189`](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/product/upstream/sync-policy.json#L161-L189) records `U=9bc7dedf…`, `P=25778c74…`, status `unaccepted`, and a blocked review | This older U/P observation is stale context and does not supersede b3 or gate the restoration plan. |

The current-tree comparison is narrow and reproducible:

```text
git diff --name-status b3 HEAD -- \
  crates/tracedecay-session-memory \
  crates/tracedecay-runtime-core/src/memory \
  crates/tracedecay-store/src/memory \
  crates/tracedecay/src/tracedecay/facts.rs
```

It is empty. The same comparison including retained routing shows one product
visibility change in `crates/tracedecay-store-runtime/src/retained_memory.rs`
(`memory_application` becomes `pub(super)`) and product additions under
`crates/tracedecay/src/daemon/retained_owner/`. Thus the Native source trees are
unchanged between b3 and the current head; the additions are integration/provider
surface and must not be mistaken for a replacement Native backend.

## Native source ownership map

The b3 source is also available through immutable upstream blobs. The current
line links below point at product head `571daf3a…`; these paths are unchanged from
b3 unless stated otherwise.

| Original source area at b3 | Current path and evidence | Original behavior to preserve | Integration requirement / difference |
|---|---|---|---|
| `tracedecay-session-memory` crate root and `fact_store/mod.rs` | [crate root L1-28](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-memory/src/lib.rs#L1-L28); [fact-store root L1-12, L101-127](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-memory/src/fact_store/mod.rs#L1-L12) / [upstream b3](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-session-memory/src/fact_store/mod.rs#L101-L127) | The crate owns session retrieval, project memory, provider usage and runtime telemetry, while refusing dependencies on application, semantic, code-index, search-eval and LSP. `DatabaseFactStore` is an authority-bound adapter over an already-open `Database`; it does not resolve paths or open a competing store. | Preserve the crate boundary and the authority-bound constructor. A provider adapter may depend on the Native application/use-case port; it must not make a provider database the Native authority. |
| Entire `fact_store/**` implementation | [module declarations and imports L38-94](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-memory/src/fact_store/mod.rs#L38-L94); [delegating owned project store L872-970](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-memory/src/fact_store/mod.rs#L872-L970); [b3 fact-store tree](https://github.com/ScriptedAlchemy/tracedecay/tree/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-session-memory/src/fact_store) | This is the canonical append-only fact, evidence and provenance authority. It includes CRUD, current/as-of/lineage queries, update/remove/supersede, feedback/history, curation/merge, privacy purge, dashboard/status, graph publication, automatic-fact decisions and automation-run receipts. `ProjectMemoryDbHandle` and `ProjectFactStore` keep one project store selected by the retained layout and delegate every trait method to `DatabaseFactStore`. | Preserve every file, including tests and “derived” helpers that implement Native behavior. Do not recreate `ProjectMemoryFactStore` or `DatabaseFactStore` in a provider crate, and do not narrow the method set to recall. |
| Search, candidates, scoring and projections | [candidate discovery L1-31, L397-475](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-memory/src/fact_store/candidates.rs#L1-L31); [scoring tokenizer and weights L1-34, L229-269](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-memory/src/fact_store/scoring.rs#L1-L34); [search/ranking L122-161, L500-539](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-memory/src/fact_store/search.rs#L122-L161); [b3 search source](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-session-memory/src/fact_store/search.rs#L500-L539) | Native search combines its FTS, term coverage, Jaccard, holographic, trust, temporal-decay and retrieval-reinforcement components, with deterministic score/timestamp/fact-id ordering and resumable cursors. Probe, related, reason and contradiction paths have distinct candidate semantics. Projection loading enforces read control and current validity. | A wrapper must return Native IDs, canonical content, score components, explanations, provenance and validity exactly. It must not inject lexical/recency ranking, reorder results, drop graph assistance, or turn a read/write Native operation into a read-only projection. |
| Retrieval tracking and explicit side effects | [fact-store retrieval write L722-781](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-memory/src/fact_store/mod.rs#L722-L781); [retrieval transaction L745-765](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-memory/src/fact_store/search.rs#L745-L765); [memory use case L444-474](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-memory/src/memory/project_memory.rs#L444-L474) | `record_project_memory_fact_retrieval` is an explicit Native write with an idempotent operation receipt and updated retrieval projection. Feedback, automatic-fact application, curation, lineage and graph reconciliation likewise have observable state/receipt effects. | Do not apply ADR-0010's read-only adapter rule to the complete Native implementation. A provider recall operation may be advisory/read-only only where the provider contract explicitly defines that projection; direct Native operations still retain their original effects. If a common interface cannot express a required Native operation, fix the interface or keep the direct Native route rather than suppressing the effect. |
| Memory model and use cases | [memory module L1-44, L116-153](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-memory/src/memory/mod.rs#L1-L44); [b3 memory module](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-session-memory/src/memory/mod.rs#L116-L153); [canonical ports L1-20, L144-175](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-memory/src/memory/canonical.rs#L1-L20); [project operations L39-150, L296-475](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-memory/src/memory/project_memory.rs#L39-L150) / [b3 project use cases](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-session-memory/src/memory/project_memory.rs#L296-L475) | `MemoryApplication` binds a `FactOwnerV1` and validates every request owner. The canonical adapters carry current/as-of/lineage/anchor reads; project use cases cover search/probe/related/reason/contradictions, get/exact/history/inspection, add/update/remove/supersede, feedback, retrieval, automatic-fact receipts, dashboard/status, curation and privacy remediation. | Keep the whole `memory/**` tree, including `canonical.rs`, `project_memory.rs`, `hygiene.rs`, `sanitize.rs`, `encoding.rs`, `entities.rs`, `similarity.rs`, `trust.rs`, `diff.rs`, `user.rs`, privacy remediation and tests. The adapter should call this owner-bound application, not copy only its current recall methods. |
| Native root integration and project-store routing | [facts.rs L1-47](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay/src/tracedecay/facts.rs#L1-L47); [b3 facts.rs](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay/src/tracedecay/facts.rs#L12-L47) | `TraceDecay::project_memory_owner`, `project_memory_db` and `project_memory_application` select the one project/profile owner and shared project database. A branch shard may resolve to an owned project-store handle; code-index routing does not change the memory identity. | The provider mount must obtain the same owner-bound application/database through this integration, preserve project-wide sharing across branches/linked worktrees, and fail closed on owner/path errors. It must not open a provider-selected alternate database. |
| Storage contracts and kernel dependencies | [store memory exports L1-81](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-store/src/memory/mod.rs#L1-L81); [store crate boundary L1-5](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-store/src/lib.rs#L1-L5); [runtime-core boundary L1-27](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-runtime-core/src/lib.rs#L1-L27) | `tracedecay-store/src/memory/**` supplies Native DTOs, errors, read/write controls and `FactStore`/`ProjectMemoryFactStore` traits. Runtime-core supplies the database facade, migrations, read snapshots, leases and recovery substrate. Neither is a second Native algorithm after the upstream moves. | Providers may consume the contracts and kernel, but must not implement the Native traits as a cognitive backend or move persistence ownership back into runtime-core/store. Keep runtime-core DB/migration/graph reconciliation and store contract files under their existing owners. |
| Retained route and composition | [retained memory dispatch L35-100, L140-165](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-store-runtime/src/retained_memory.rs#L35-L100); [Native application construction L1048-1054](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-store-runtime/src/retained_memory.rs#L1048-L1054); [root assembly L1-3, L116-143](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay/src/daemon/retained_owner.rs#L1-L3) / [b3 retained route](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-store-runtime/src/retained_memory.rs#L1048-L1054) | The retained route admits project/profile scope and dispatches fact operations to `MemoryApplication<DatabaseFactStore>`. The root only assembles authorities and mounts the retained surface. | The current `pub(super)` change is a product seam for routing, not a Native implementation change. Shared route/root files require a separate composition owner. Provider selection belongs in the product fabric/registry and narrow mount; the route must continue calling Native for Native operations. |

## Stale or conflicting evidence

* `product/architecture/native-memory-surface-map.md` and `.json` retain useful
  invariants—`FactOwnerV1` ownership, append-only lineage, feedback/trust,
  automatic receipts, separate session observations and rebuildable derived
  surfaces—but name deleted or moved paths such as
  `crates/tracedecay-runtime-core/src/store/memory/**` and
  `crates/tracedecay/src/daemon/retained_owner/memory.rs` ([map markdown
  L31-36](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/product/architecture/native-memory-surface-map.md#L31-L36),
  [JSON authority L14-35](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/product/architecture/native-memory-surface-map.json#L14-L35)). Neither
  old path exists in b3/current; `tracedecay-store/src/memory/**` is the live
  contract/DTO location. Reconcile the map in its documentation owner before
  using path rows as implementation authority.
* The upstream move commits are source evidence, not a reason to recreate an
  old layout. `12c6f1d43154f9925e3a7d1b9a7ce8f3353793f5` moves the fact-store
  implementation to `tracedecay-session-memory/src/fact_store` (the diff records
  high-confidence renames), and
  `7a5cca7298e9a26794c82fa97034fbe15459e1c0` moves the memory model and helpers
  to `tracedecay-session-memory/src/memory`. The later
  `a889fc25b2d7f3c94241bdb7e0f37b8b252584bc` privacy extraction is a separate
  privacy owner; it is not permission to claim privacy as a provider backend.
* ADR-0001 correctly places the provider boundary above Native contracts and
  says Native remains the only canonical fact authority ([ADR lines 14-28,
  39-45](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/product/architecture/adr/ADR-0001-provider-boundary.md#L14-L45)).
  ADR-0010 additionally describes a read-only current-recall projection and
  marks retrieval telemetry, history/as-of, feedback, maintenance, correction
  and delete unsupported ([lines 24-29](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/product/architecture/adr/ADR-0010-native-provider-parity-projection.md#L24-L29),
  [55-62](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/product/architecture/adr/ADR-0010-native-provider-parity-projection.md#L55-L62)). Those restrictions may describe one advisory provider capability, but conflict with the user's requirement for the complete original Native behavior. They must not be copied into or used to narrow the Native baseline.
* The V2 roadmap is authoritative for delivery shape: project memory is shared
  across branches/linked worktrees ([lines 58-60](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/docs/plans/tracedecay-v2/00-plan-set-index.md#L58-L60)),
  current path/scaffold descriptions are historical ([119-126](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/docs/plans/tracedecay-v2/00-plan-set-index.md#L119-L126)),
  and V2 is a fresh-store final shape without migration, compatibility reader,
  backfill or dual-write ([16-22](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/docs/plans/tracedecay-v2/00-plan-set-index.md#L16-L22)). Do not add those mechanisms while mounting Native.

## Exact future write ownership

The source-restoration implementation lane may write only product-owned adapter
and provider-selection paths:

* `crates/tracedecay-memory-provider-native/**` owns the Native provider
  adapter and its focused conformance/parity fixtures. It delegates to the
  owner-bound Native application and carries no fact tables, alternate scorer,
  staged-observation store or provider persistence that could become Native.
* `crates/tracedecay-memory-provider-registry/**` owns provider construction and
  capability selection; `crates/tracedecay-memory-fabric/**` owns provider-neutral
  composition/lifecycle. These are separate product owners from the adapter.
* The existing root-private product modules
  `crates/tracedecay/src/daemon/retained_owner/native_provider.rs` and its
  focused `native_*` tests may be changed only by the Native adapter owner, and
  only to delegate complete Native behavior. `native_staged_observations.rs`
  remains a separate session-observation/provider surface and cannot be used as
  the Native fact authority.

The following shared files require a separate named composition/contract owner:

* `crates/tracedecay/src/daemon/retained_owner.rs`;
  `crates/tracedecay-store-runtime/src/retained_memory.rs`; and any retained
  target/lifecycle module. They own admission, target selection, lifecycle and
  route mounting, not Native semantics.
* Provider API contracts, generated Rust/TypeScript bindings, workspace
  `Cargo.toml`/`Cargo.lock`, and public CLI/MCP/SDK/dashboard dispatchers each
  keep their existing single owner. The adapter lane may propose changes but
  must not edit them opportunistically.

These source and dependency areas stay untouched by the provider implementation:

* all of `crates/tracedecay-session-memory/src/fact_store/**` and
  `src/memory/**`, including tests and helpers;
* `crates/tracedecay/src/tracedecay/facts.rs`;
* `crates/tracedecay-store/src/memory/**` contracts/DTOs;
* runtime-core database, migration, read-snapshot, lease and memory-graph
  reconciliation code; and
* `crates/tracedecay-privacy/**` and the separate session observation/LCM
  authorities.

## Behavioral acceptance checklist

An implementation is acceptable only when direct Native and provider-routed
Native use equivalent isolated state and the same admitted `FactOwnerV1`, and
the following are observable:

1. Add, update, remove, supersede, feedback, curation and automatic-fact
   operations preserve Native lineage, trust transitions, idempotency, receipts,
   privacy decisions and typed failures. A provider route cannot report a
   successful effect that Native did not commit.
2. Current, as-of, lineage, exact, list, search, probe, related, reason,
   contradiction, status, graph and inspection results retain Native ownership,
   validity, canonical content, provenance, deterministic ordering/cursors and
   score/explanation components. The Native FTS/Jaccard/FHRR/trust/temporal
   behavior is observable at the same entry point.
3. Retrieval tracking is exercised as its own operation: where Native records a
   retrieval write, the direct Native path retains the receipt and projection
   change; a provider capability explicitly defined as advisory/read-only does
   not silently claim to have performed that Native write.
4. A project fact is visible through the shared project store from another
   branch/linked worktree, while profile and project owners remain isolated.
   No route falls back to a branch shard, provider database or empty successful
   result when the selected authority is unavailable.
5. Cancellation, deadline, owner mismatch, invalid request, unavailable
   capability, stale/invalid projection and storage failures return typed
   terminal outcomes with the same committed/no-effect semantics as direct
   Native. Reopening/restarting preserves committed state and only rebuilds
   derived projections according to Native rules.
6. The comparison uses an immutable b3 source reference and does not rely on
   matching fixtures alone: at least one positive mutation/receipt path, one
   retrieval/telemetry path and one negative scope/failure path execute through
   independent direct and provider routes.

## Unknowns and blockers

* This audit proves local Git ancestry and source identity. It does not fetch the
  upstream remote or independently verify whether b3 is the upstream PR's final
  public head. The local ref name, author and ancestry make it the strongest
  available source reference; no remote fetch or upstream update is needed for
  this restoration plan.
* b3 is not copied into the old product floor pins or convergence-map stamps.
  Those August records are stale metadata to reconcile truthfully in the future
  product plan; they do not make F the restoration baseline or gate this work.
* The current product additions include broad provider and staged-observation
  modules. This source lane does not establish that their current behavior is
  complete or parity-correct; the adapter/interface and verification lanes must
  review them against this boundary.
* ADR-0010's unsupported/read-only projection rules and the complete Native
  operation set cannot both be the universal provider contract. Root must decide
  whether to broaden the common interface or retain direct Native operations as
  separate capabilities. Until decided, do not mark unsupported operations as
  Native parity.

## Bounded Luna Max implementation proposal

**Goal.** Mount Native and NCM as alternatives while preserving b3's complete
Native code and behavior. This lane implements only the Native adapter seam and
does not replace the fact store.

**Prerequisites.** The handoff records b3 as the fixed protected source
reference and the F/b3 metadata mismatch, reviews the facts/interface/adapter/
data/NCM evidence, and assigns the shared mount, registry, contract and
generated-file owners. Metadata reconciliation is a future bookkeeping step,
not an upstream-acceptance or implementation-permission gate.

**Files.** Write only the Native provider crate and its explicitly assigned
root-private adapter/tests. Use the existing Native application and
`FactStore`/`ProjectMemoryFactStore` contracts as inputs. Leave the entire Native
source trees, runtime/store contracts, shared route, manifests and generated
outputs to their named owners.

**Steps.**

1. Pin b3 in the implementation handoff and create an operation/effect matrix
   from `MemoryApplication` and both fact-store traits. Include retrieval
   tracking, lineage, feedback/trust, automatic receipts, curation, graph,
   privacy, cancellation and owner checks.
2. Implement the adapter as a delegating boundary over the existing owner-bound
   application. Preserve Native payloads, scores, explanations, provenance,
   validity, receipts and errors; do not introduce a second tokenizer, scorer,
   persistence model or observation promotion path.
3. Mount it through the separately owned registry/fabric/retained route with the
   exact project/profile admission and one shared project-store identity.
4. Compare direct and provider-routed Native behavior using separately isolated
   equivalent state. Exercise positive mutations and retrieval effects, negative
   scope/failure/cancellation behavior, cross-branch project sharing and
   restart/reopen projection behavior.
5. Run the source-preservation diff and the repository's focused Native,
   provider-conformance and relevant direct acceptance checks. A build owner
   schedules Cargo work; this evidence lane ran no builds or tests.

**Forbidden shortcuts.** Do not call staged observations, NCM state, a custom
lexical/recency scorer or an empty-success fallback Native. Do not suppress
Native writes because the wrapper is easier to implement. Do not erase or
silently rewrite the historical floor; reconcile its metadata truthfully in the
future product plan. Do not recreate stale path/scaffold artifacts, add migration
or dual-write behavior, or hand-edit generated contracts.

**Stop condition.** Stop and report a bounded interface or ownership gap if any
Native operation cannot be represented without changing its behavior, if the
provider route would alter ranking/ownership/state effects, if new evidence
contradicts the fixed b3 source identity, or if a required shared owner/
behavioral fixture is missing. Do not invent an algorithm or label partial
projection as complete Native.

No tests or builds were run in this read-only source audit.
