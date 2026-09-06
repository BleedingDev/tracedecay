# ncm-biomem-rs.v1 — frozen backend contract (ncm-rs-003)

Status: frozen for tasks 004–022. Changes go through the coordinator and bump
`ALGORITHM_IDENTITY`. Reference = Biomem `500847ff…` (see
`product/ncm/reference/source-manifest.json`). Host = tracedecay `25778c744…`.

## 1. Packages and dependency direction

```
provider-registry → provider-ncm (existing adapter, + opt-in `rust-backend` feature)
                        → tracedecay-memory-ncm-runtime (client + wire; engine, store, embedding, worker bin)
                              → tracedecay-memory-ncm-core (pure math/state; serde only)
```

* core: no tokio/fs/process/network/host-store deps. `serde` only.
* runtime: `core`, `serde`, `serde_json`, `rusqlite` (bundled), `sha2`, `fastembed` (pinned
  workspace fork, `ort-download-binaries-rustls-tls` + `hf-hub-rustls-tls`). Never depends on
  provider-ncm or registry.
* provider-ncm: adds `rust-backend = ["dep:tracedecay-memory-ncm-runtime"]` (default off).
  Feature off ⇒ no NCM registration, route, process, or state dir. Policy JSON reconciliation is
  task 023 (expected checker diff on this branch is recorded, not made permissive).

## 2. Identities

| Type | Definition |
|---|---|
| `NamespaceId` | the adapter's `NcmNamespace` sha256 hex (full exact-scope hash incl. `agent_session_id`, `resolved_scope_digest`). Opaque to core. Never widened (D11). |
| `SourceId` | opaque host-admitted source identity string carried in the Observe payload; core stores it as an opaque string. Deletion keys on it. |
| `RecordId` | `u64` allocated monotonically per namespace, never reused. One observed key/value pair = one record. Survives center movement/merge. |
| `CenterSlot` | `{ index: u32, incarnation: u32 }`; incarnation increments each time a slot is (re)activated; stale handles never resolve. |
| `AlgorithmIdentity` | `"ncm-biomem-rs.v1"` + sha256 of the canonical `NcmConfig` JSON. |
| `ProjectionIdentity` | sha256 of the persisted projection matrices (per namespace; generated once at namespace creation from a recorded seed, never regenerated on open/restore/rebuild). |
| `StateEpoch` | `u64`, bumps on namespace creation, snapshot restore, and sanitized rebuild (deletion). |
| `CommitSequence` | `u64` per namespace, +1 per committed mutation. |
| `LogicalTick` | `u64` learning ticks (see §5). |

Public `state_generation` (ProviderReply / `expected_state_generation`) = `CommitSequence`.
`StateEpoch` and identities are folded into the ready-receipt digest.

Readiness lifetime is owned by the existing `NcmProviderAdapter` and is unchanged in v1 (task 018
finding, 2026-09-06): `AcceptedReadiness` admits a call only while `expected_state_generation`
equals the handshake generation, and the adapter retires readiness before every mutation
(`mutation_dispatch_retires_the_accepted_ready_receipt`). Consequently the host re-handshakes after
each committed mutation; the surface reports the new `CommitSequence` in the reply and answers the
next handshake with it. Relaxing this (advance the accepted generation on a validated mutation reply)
is a host-side change deferred to task 023. Descriptor `provider_id` stays the reserved `ncm`; the
implementation identity `ncm-biomem-rs.v1` is carried in the descriptor version/receipt, never as
the provider id.

## 3. Operations (map to existing `ProviderOperation` only)

| ProviderOperation | Engine op | Mutates | Idempotency |
|---|---|---|---|
| Handshake | open-if-exists + compatibility check (algorithm/projection/model/epoch). **No disk writes.** A not-yet-materialized namespace is `ready(empty)`. | no | – |
| Health | encoder loaded? store open? budgets? | no | – |
| Observe | capsule + embed + core transition + 1 tick | yes | key ⇒ (payload digest ⇒ receipt replay / conflict) |
| Recall | read committed immutable view; **no counters change** (D05) | no | – |
| Feedback | explicit usage/strength signal for cited `RecordId`s | yes | as Observe |
| Maintenance | bounded: `advance{ticks≤N}`, `consolidate`, `merge_prune`, `checkpoint`, `compact` | yes | as Observe |
| Inspection | redacted stats (counts, fatigue, tick, epoch, budgets) | no | – |
| Correction | link superseding record → superseded record (lineage), optional re-weight | yes | as Observe |
| DeleteBySource | fence → rebuild excluding revoked sources → atomic publish + epoch bump | yes | as Observe |
| SnapshotExport | versioned export (state + capsules + epoch; **not** the revocation authority) | no | – |
| SnapshotRestore | verify identity, apply current revocations (refuse/strip revoked sources), epoch bump | yes | as Observe |
| Replay | typed `Unsupported` in v1 | – | – |

Idempotency row = `(namespace, idempotency_key) → (payload_sha256, receipt, commit_seq)`.
Same key + same digest ⇒ replay receipt, zero effect. Same key + different digest ⇒ `Conflict`.
Receipt and durable commit are one SQLite transaction; success is acknowledged only after COMMIT.
Publication to the live view happens after COMMIT; if publication fails the reply is
`effect-unknown` and reopen reconciles from the store (never "no effect" + re-learn).
Cancellation before COMMIT ⇒ no effect. Cancellation/transport loss after COMMIT ⇒ committed
(client sees unknown; replaying the same key returns the receipt).

## 4. Algorithm profile (production, single profile)

Reference-compatible unless listed under "corrected". Config = Biomem `MemoryConfig` defaults
(`NcmConfig::default()` in `types.rs`).

Reference-compatible: plain RBF read (cosine, `2-2cos`, `exp(-d²/2σ²)`, log-space intensity
softmax with `1e-8`); hybrid preselection when `n_active > 64` (p=0.5 Minkowski, batch-global max
normalization, 0.7/0.3 combine, lowest-combined top-k, final cosine RBF weights); compound read
(0.60/0.25/0.15, missing part = zeros); write (max-weight novelty threshold, local normalization,
`h += w`, EMA on V/e, metadata to argmax center, first-free-slot allocation, `omega<1e-6` ignored);
merge (0.95, h-weighted, newer ctx/terrain, greedy pair removal); prune (`max(thr, 0.011)`, age >
min_age, usage < 5); normalization (`log1p`, `V/(1+‖V‖/c_v)`, `tanh(e)`, floor 0.01); homeostasis
(h,V toward 0; e toward 1; age+1); fatigue `F ← (1-λ_F)F + 0.1·intensity`; sleep when `F > 2.5`
and `steps_since_consolidation ≥ 100`; consolidation top-128 by h, `ω = max(κh, 0.3)`, age+1,
LTM write with 0.78 threshold, LTM+STM normalization, terrain pour `ξ_H=0.005, ξ_E=0.003`,
`F ← 0.2F`; write strength `3·σ(2·novelty + 0.3·surprise + 0.3·salience − 1)·intensity`,
novelty from LTM top-1 unnormalized RBF weight (1.0 if LTM empty); affect neutral `[1,1,1,1]`.

**Corrected (each has a negative-control test and a `deviations.json` entry):**

| ID | v1 behavior |
|---|---|
| D01 | Real separable Gaussian blur (`kernel_size = max(3, int(6σ)|1)`, normalized 1-D kernel, replicate boundary) applied to H and E before pour. σ=2.0 grid cells. |
| D02 | Executed reference behavior is kept (write candidates use `sigma_read`; `sigma_write` retained in config, marked unused). Reason: thresholds were tuned against executed behavior. Test asserts the width actually used. |
| D03 | Text recall passes `terrain_queries = None` (terrain part contributes 0). Profile exposes `terrain_read_influence = "none"`. No TerrainPrior claim. |
| D04 | Recall returns distinct records (no key-text dedup); merge across layers by `RecordId`, STM/LTM entries for the same record are both reported with their layer. |
| D05 | Reads never mutate (`usage`, `read_count`, tick). `Feedback` is the only learning-from-use path. |
| D06 | Open of corrupt/incompatible state fails closed (`Corrupt`/`Incompatible`); fresh-empty only via explicit create. |
| D07 | `steps_since_consolidation`, fatigue, tick, epoch, seq, last-maintenance persisted in every checkpoint. |
| D08 | Every record retains its canonical LTM key (`to_ltm_key(embed(key))`) at observe time; consolidation writes that key into LTM (not `U·k_stm`). `U` is kept only for reference fixtures. Test: store → consolidate → mask STM → recall from LTM succeeds. |
| D09 | One axis convention: position component `i` ↔ grid axis `i` for splat **and** trilinear sample (`align_corners=true`, border clamp); flattened index `(x·G + y)·G + z`. Reference sample/splat axis mismatch recorded. |
| D10 | Schedule: one learning tick per unique committed Observe = `stm.write → stm_terrain.splat → fatigue/scheduler step → homeostasis step (both center sets + both terrains)`. Duplicate/replayed observes and all reads advance nothing. Explicit `Maintenance{advance: n ≤ 10_000}` ticks are bounded. |
| D11 | Namespace unchanged; cross-session reuse only via host-authorized replay/share (task 024). |

Tolerances for reference fixtures: `atol=1e-6, rtol=1e-5` single-step f32; multi-step
tolerances declared per fixture before inspection. Counts, masks, indices, op codes exact.
Ties: stable ascending index order.

## 5. Budgets (v1 proposals; 020 measures)

record ≤ 16 KiB key+value; batch ≤ 16 records; wire request ≤ 256 KiB, reply ≤ 1 MiB; mailbox
≤ 32 requests / 8 MiB; source basis ≤ 64 MiB per namespace (ingestion refused at quota, never
lineage loss); controlled storage ≤ 256 MiB per namespace, ≤ 2 GiB per profile, 16 MiB reserve;
resident namespaces ≤ 4, catalog ≤ 32; one worker per profile; kill escalation ≤ 250 ms after
deadline; kernel recall p95 ≤ 25 ms at 4096 LTM centers; warm text recall p95 ≤ 250 ms; durable
observe p95 ≤ 500 ms. Host deadline always wins (`min(remaining, provider limit)`).

## 6. Storage (runtime `store/`)

One SQLite file per namespace under the admitted state root:
`<root>/namespaces/<namespace>/ncm.sqlite` (WAL, `synchronous=FULL`, `locking_mode=EXCLUSIVE`).
Tables: `meta` (identities, epoch, seq, tick, projection blob+digest, config digest, seed),
`capsules` (record_id, source_id, key_text, value_text, affect[4], surprise, intensity,
key_embedding f32[384], value_embedding f32[384], provenance json, status valid/superseded/revoked,
commit_seq), `events` (seq, kind, idempotency_key, payload_sha256, receipt json), `checkpoints`
(seq, epoch, kernel state bytes), `revocations` (source_id, epoch, seq). Checkpoint every 32
committed events and on close; open = latest checkpoint + deterministic replay of later events
(capsules carry embeddings ⇒ no encoder needed for replay). Every capsule/event byte counts
toward the basis budget.

## 7. Deletion (runtime `privacy/`)

`DeleteBySource(source_id)`: mark revocation (epoch+1) in the same transaction as a fence flag;
serve `Unavailable{rebuilding}` for the namespace; rebuild kernel from scratch replaying all
non-revoked capsules in original commit order under the same schedule; write new checkpoint,
delete all prior checkpoints, clear fence, COMMIT; publish. Revoked capsule text/embeddings are
overwritten with zero-length and status `revoked` in the same commit. Export never contains the
revocation table; restore consults it. Erasure boundary: this store, its WAL, checkpoints, and
worker memory. Not: externally exported files or physical remanence.

## 8. Worker/wire (runtime `worker/`, `wire/`, `client/`)

Binary `tracedecay-ncm-worker`, stdio, frames `u32 BE length + JSON`, request
`{id, deadline_ms, op, namespace, payload}`, reply `{id, status, payload | error}`. Client:
spawn on first use, one worker per profile, per-request deadline ⇒ SIGKILL + respawn, in-flight
mutations become `effect-unknown` (resolved by idempotent replay). Worker validates frame size
before allocation. No model download inside the worker; `embedding::install` is an explicit
separate step.

## 9. Encoder (runtime `embedding/`)

fastembed `EmbeddingModel::ParaphraseMLMiniLML12V2` (`Xenova/paraphrase-multilingual-MiniLM-L12-v2`,
`onnx/model.onnx`), `max_length = 128`, mean pooling, L2 normalize, 384-D. Artifacts live under
`<root>/models/` and are pinned by sha256 in `product/ncm/reference/embedding-manifest.json`.
Handshake/recall never download. Test doubles (`HashEncoder`) are `cfg(test)`/named doubles and
cannot satisfy `real_embeddings`.

## 10. Affect

Input `affect: [f32;4] | preset name | None`; None ⇒ `[1,1,1,1]`; non-finite ⇒ typed error.
Presets and the Czech/English keyword heuristic are ported from `EmotionExtractor` as an
optional, versioned `signal_source = "keyword-heuristic.v1"`; never described as a measurement.
