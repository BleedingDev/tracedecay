# TraceDecay V2 Native memory production surface map

Bead: `tdmem-0103`

Machine-readable authority: [`native-memory-surface-map.json`](./native-memory-surface-map.json). The exhaustive b3-to-head hunk ledger is [`native-original-source-inventory.md`](./native-original-source-inventory.md).

## Scope

This map covers production memory behavior used by coding agents on `feat/pluggable-memory-providers-v2`. The fixed Native restoration reference is b3 (`b3b43410e47115056f2066449aafa1822bbb6049`), while the audited product head is `571daf3a9612e5247443e4da3a107b542686c1ef`. The provider boundary remains planned until its behavioral gates pass.

The governing rules are:

1. `FactOwnerV1` is the immutable profile/project scope key for every Native fact operation.
2. Explicit facts have one canonical writer: the owner-bound Native memory application over the retained database authority.
3. Session observations and LCM history are separate canonical domains. They are evidence, not accepted explicit facts.
4. Search scores, FHRR vectors, trust summaries, dashboard payloads, and the Grafeo relation graph are derived and rebuildable.
5. External cognitive-provider recall is advisory. It cannot silently mutate Native facts or current-code truth.

## Baseline and restoration status

- **Original Native reference:** b3 (`b3b43410e47115056f2066449aafa1822bbb6049`), the upstream side of the product merge. The complete `tracedecay-session-memory/src/fact_store/**`, `src/memory/**`, LCM, store/memory contracts, memory-v2, and facts authorities are the original implementation and remain protected.
- **Product extensions:** existing host admission, provenance, cursor, transcript, configuration, storage, privacy, maintenance, and provider-composition changes are retained as separate extensions. They are not claimed to be byte-identical to b3.
- **Shared privacy exception:** the direct Native and LCM source files have no scoped hunks, but their sanitization reaches the changed `tracedecay-privacy::detector_kernel::looks_high_entropy_token` algorithm. Its exact `-sha256-<64 lowercase hex>` suffix peeling is an escalated original algorithm delta; Native memory hygiene and LCM callers remain unaccepted until that algorithm is restored exactly or isolated behind a reviewed product boundary.
- **Exact restoration:** `crates/tracedecay-runtime-core/src/db/retrieval_anchor_authority.rs` has one authorized b3 restoration, owned by rn-restore-anchor. No adapter may replace an original Native algorithm.
- **Staged substitute:** `provider-state/native/staged-observations-v1.sqlite3` and its sidecars are provider-local stale bytes. Removing the staged Native route leaves them untouched and does not read, migrate, replay, or reset them.
- **Verification status:** planned. The map records required checks; it does not claim that the restoration or parity suites have passed.

## Authority matrix

| State | Class | Write owner | Read authority | Provider rule |
|---|---|---|---|---|
| Explicit facts, lineage, provenance | Canonical | `MemoryApplication` → `DatabaseFactStore` / `ProjectMemoryFactStore`, selected by `FactOwnerV1` | Same owner-bound Native store | Providers never write these tables |
| Fact feedback and trust transitions | Canonical | Owner-bound feedback command through `DatabaseFactStore` | Native feedback history/status readers | Provider feedback may be observed, never applied directly to Native trust |
| Automatic-fact and automation receipts | Canonical receipt | Daemon automation effect and Native automatic-fact application | Automation receipt/run-ledger readers | A proposal is not an accepted fact until durable settlement |
| Transcript observations, sessions, LCM messages | Separate canonical domain | Host admission and session/transcript ingest authorities | Admitted session/LCM retrieval service | Never relabel raw observations as explicit facts |
| Verified fact relation topology | Derived | Post-commit graph publication and reconciliation | Generation-pinned graph readers | Rebuildable; graph presence does not establish canonicality |
| FTS/Jaccard/FHRR/trust ranking | Derived | Native projection code | Native search/probe/reason/contradiction readers | Scores are advisory ordering, not truth |
| Status, trust bands, feedback funnel, dashboard payloads | Derived | Calculated on read | Native status/dashboard APIs | Inspection output is not a write authority |

Primary ownership code:

- `crates/tracedecay-session-memory/src/memory/mod.rs`
- `crates/tracedecay-session-memory/src/fact_store/mod.rs`
- `crates/tracedecay/src/tracedecay/facts.rs`
- `crates/tracedecay-store-runtime/src/retained_memory.rs`

## Public production entry points

The retained application catalog is the operation authority. HTTP, MCP, dynamic CLI, and SDK transports resolve to the same daemon-owned application operation; none owns an alternate store.

| Operation | Effect | HTTP | MCP | CLI | Canonical owner/read authority |
|---|---|---|---|---|---|
| `fact_store_curate` | Administrative | `/application/retained/fact_store_curate` | `tracedecay_fact_store_curate` | `tracedecay tool fact_store_curate` | Memory Curator may settle reviewed changes only through Native explicit-fact authority |
| `fact_store_add` | Administrative | `/application/retained/fact_store_add` | `tracedecay_fact_store_add` | `tracedecay tool fact_store_add` | Native explicit-fact authority |
| `fact_store_search` | Read | `/application/retained/fact_store_search` | `tracedecay_fact_store_search` | `tracedecay tool fact_store_search` | Canonical facts plus rebuildable search projection |
| `fact_store_probe` | Read | `/application/retained/fact_store_probe` | `tracedecay_fact_store_probe` | `tracedecay tool fact_store_probe` | Canonical facts plus rebuildable search projection |
| `fact_store_related` | Read | `/application/retained/fact_store_related` | `tracedecay_fact_store_related` | `tracedecay tool fact_store_related` | Canonical facts plus verified relation graph |
| `fact_store_reason` | Read | `/application/retained/fact_store_reason` | `tracedecay_fact_store_reason` | `tracedecay tool fact_store_reason` | Canonical facts plus rebuildable search projection |
| `fact_store_contradict` | Read | `/application/retained/fact_store_contradict` | `tracedecay_fact_store_contradict` | `tracedecay tool fact_store_contradict` | Canonical facts plus rebuildable search projection |
| `fact_store_get` | Read | `/application/retained/fact_store_get` | `tracedecay_fact_store_get` | `tracedecay tool fact_store_get` | Native explicit-fact authority |
| `fact_store_update` | Administrative | `/application/retained/fact_store_update` | `tracedecay_fact_store_update` | `tracedecay tool fact_store_update` | Native explicit-fact authority; append-only lineage |
| `fact_store_remove` | Administrative | `/application/retained/fact_store_remove` | `tracedecay_fact_store_remove` | `tracedecay tool fact_store_remove` | Native explicit-fact authority; append-only tombstone/lineage |
| `fact_store_list` | Read | `/application/retained/fact_store_list` | `tracedecay_fact_store_list` | `tracedecay tool fact_store_list` | Native explicit-fact authority |
| `fact_feedback` | Administrative | `/application/retained/fact_feedback` | `tracedecay_fact_feedback` | `tracedecay tool fact_feedback` | Native feedback authority |
| `memory_status` | Read | `/application/retained/memory_status` | `tracedecay_memory_status` | `tracedecay tool memory_status` | Derived Native status projection |

The machine-readable `public_operations` list in
`native-memory-surface-map.json` is intentionally limited to these 13 retained
explicit-fact/memory catalog operations. It is not a complete
list of the original typed session, LCM, refresh, supersede, or background
operations. Complete original route coverage is recorded in
`execution-results/native-operation-routes.md` from rn-contract; do not expand
this list or duplicate that route catalog here.

`canonical_mutation=false` means that an operation does not mutate canonical
fact state; it does not mean that the operation has no telemetry effect. A
direct retained nonempty `fact_store_search` writes the original retrieval
telemetry/receipt, whereas generic automatic reads do not.

SDK operation IDs are `operation.application.<operation>` and use the generated descriptors in `crates/tracedecay-sdk/src/operations.rs`.

Transport and dispatch ownership:

1. `crates/tracedecay-contracts/src/retained_surfaces.rs` defines catalog operations, schemas, effects, deadlines, terminal states, and public HTTP bindings.
2. `crates/tracedecay-cli/src/tool_command.rs` resolves dynamic CLI names through the same catalog and daemon route.
3. MCP resolves the same named bindings through the root MCP dispatcher.
4. `crates/tracedecay-sdk/src/operations.rs` and `client.rs` invoke the daemon-owned HTTP binding.
5. `crates/tracedecay-store-runtime/src/retained_memory.rs` selects the exact project/profile authority and executes the memory application.

## Call paths

### Explicit add/update/remove

```text
HTTP | MCP | CLI | SDK
  → retained operation catalog and request schema
  → daemon exact project/profile admission
  → DirectRetainedMemoryPortV1
  → FactOwnerV1-bound MemoryApplication
  → DatabaseFactStore / ProjectMemoryFactStore
  → append-only fact, lineage, provenance, durable receipt
  → derived graph publication/reconciliation trigger
```
Stable logical operation identity is derived before retriable writes. Deadline, cancellation, commit admission, terminal outcome, and reconciliation remain explicit.

### Search/probe/related/reason/contradict/list/get

```text
transport
  → retained operation catalog
  → exact owner-bound MemoryApplication
  → canonical Native fact snapshot
  → optional rebuildable search or relation-graph assistance
  → bounded, typed page/terminal outcome
```

A search score or graph edge only ranks/explains a canonical fact. It cannot manufacture one.

### Feedback

```text
transport
  → exact fact owner + fact ID validation
  → stable operation identity
  → Native feedback event and trust transition
  → feedback history/status projection
```

Future provider outcome fan-out belongs after Native settlement. Provider failure must not retroactively change a completed Native result.

### Memory Curator and automatic promotion

`fact_store_curate` launches the durable Memory Curator adapter in `crates/tracedecay/src/daemon/dashboard_automation/retained_curator.rs`. It runs under automation effect admission, cancellation/deadline control, durable settlement, and run-ledger observation. Reviewed mutations still enter through the Native fact application.

Session-reflector/automatic-fact paths may promote evidence only through idempotent automatic-fact commands and receipts. The original session observation remains in its separate authority.

### Host hooks and session evidence

`crates/tracedecay/src/mcp/tools/handlers/hook_runtime/ingest.rs` admits Claude, Codex, Cursor, and other host transcript material into session/LCM authorities. It does **not** directly write explicit Native facts.

`SessionApplicationRetrievalPortV1` in `crates/tracedecay-session-runtime/src/session_retrieval/admitted.rs` performs exact mounted-scope retrieval with immutable request identity, cancellation, deadlines, and bounded results before context use.

### Dashboard inspection

`/api/plugins/holographic/*` retains a fixed `memory_owner` and `mem_db` in `DashboardState`. The dashboard reads canonical facts through the shared memory application and builds relation, FHRR, similarity, trust, status, and overview payloads as derived surfaces. Dashboard output is never a persistence authority.

### Privacy and maintenance

- `crates/tracedecay-privacy/src/privacy_remediation.rs` performs bounded, receipt-backed quarantine/purge against retained Native/session authorities after admission.
- `crates/tracedecay-maintenance/src/store_maintenance/` owns repair, replay, retention, and store lifecycle work.
- `crates/tracedecay-store-runtime/src/session_registry/memory_graph_reconciliation_tasks.rs` owns cancellation, shutdown, and retirement of rebuildable graph workers.

These are Native administrative responsibilities, not cognitive-provider extension points. Provider controls with no lossless Native mapping use typed `capability_unsupported`; generic unmatched controls do not become fake Native operations.

## Canonical versus generated/derived

Canonical:

- accepted explicit fact payloads;
- fact lineage and provenance;
- fact feedback events and trust transitions;
- automatic-fact/automation settlement receipts;
- separately scoped host/session/LCM evidence.

Derived or generated:

- Grafeo fact relation graph;
- FTS/Jaccard/FHRR/trust ranking scores;
- FHRR vectors;
- dashboard overview/status/similarity payloads;
- trust-band and feedback-funnel summaries;
- automatic-fact and automation inspection views;
- SDK operation descriptors and generated schemas.

Deleting and rebuilding a derived surface must not lose accepted fact truth. Replacing a canonical authority would.

## Ranked provider seams

| Rank | Seam | Invasiveness | Decision |
|---:|---|---|---|
| 1 | Normalized observation fan-out after host admission | Lowest | Preferred observer mount. Bounded/idempotent provider-local observation only; no prompt or Native-fact effect. |
| 2 | Advisory recall contributor at the context compiler/retrieval composition | Low | Preferred active-read mount. Requires exact scope, provenance, budgets, deadlines, cancellation, and typed degradation. |
| 3 | Outcome/feedback fan-out after Native settlement | Medium-low | Useful observer learning seam. Never rewrite Native trust or turn post-commit provider failure into Native failure. |
| 4 | Capability registry plus narrow daemon composition mount | Medium | Required product mount. Selection belongs here; provider-name branching elsewhere is prohibited. |
| 5 | Implement/replace `ProjectMemoryFactStore` or `DatabaseFactStore` | High | Rejected. This conflates advisory cognition with canonical facts, lineage, privacy, and graph publication. |
| 6 | Provider branching in CLI, MCP, HTTP, dashboard, or projections | Forbidden | Never. It creates multiple unverifiable authorities and provider-specific public contracts. |

## Boundary recommendation

The viable provider boundary sits **above** Native fact-store contracts:

- observations fan out from normalized, admitted host/session events;
- recall providers return bounded advisory candidates to a shared context compiler;
- feedback/outcomes fan out after the authoritative Native result settles;
- a capability registry and narrow daemon composition root own provider selection and lifecycle;
- Native explicit facts remain authoritative and unchanged.

Validation:

```bash
python3 scripts/product/check-native-memory-surface-map.py \
  --repo . \
  --map product/architecture/native-memory-surface-map.json
python3 tests/product_native_memory_surface_map_test.py
```
