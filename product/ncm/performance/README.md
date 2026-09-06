# NCM performance evidence

This directory contains raw, host-qualified evidence for `ncm-biomem-rs.v1` task `ncm-rs-020`.
The benchmark is a plain `harness = false` Cargo bench target with no new third-party dependency.

## Run

```sh
RUSTC_WRAPPER= python3 scripts/product/ncm/benchmark/run.py
```

Use `--quick` only for smoke validation. A real encoder run requires an admitted state root whose
`models/` directory contains all five pinned Xenova artifacts and the runtime manifest:

```sh
RUSTC_WRAPPER= python3 scripts/product/ncm/benchmark/run.py --model-root /absolute/ncm/state-root
```

Every child is capped at 590 seconds. `/usr/bin/time -l` supplies peak RSS; the benchmark also records
a point-in-time process RSS through `ps`. Allocated bytes are intentionally absent because the lockfile
contains no allocator instrumentation suitable for this binary. Disk accounting includes the SQLite
main file and WAL; snapshot/checkpoint payload bytes are reported by the store.

## Populations and interpretation

- `kernel`: empty, sparse (64 STM / 512 LTM), and full (512 STM / 4096 LTM), with a 100k pre-embedded
  operation trace. Observe is sampled separately and full-capacity writes must reject atomically.
- `flat_vector_ltm_top16`: a deliberately narrow matched comparison over the same normalized active
  LTM center keys. It is distance-only and does not include projection, support lookup, record hydration,
  encoder work, IPC, or durability.
- `python_reference_kernel`: the pinned Biomem `MemoryCenters.read` path at the same 64-D LTM capacities,
  top-k 16, pre-embedded CPU input, and `increment_stats=False`. A missing PyTorch environment is recorded
  as `blocked_environment`, never silently omitted.
- `store`: 10k durable attempts at 64 B, 4 KiB, and 16 KiB records, including source-basis saturation,
  WAL/main-file accounting, compaction, and reopen retention.
- `engine`: 1 and 4 resident namespaces at each record size, with HashEncoder so encoding mechanics are
  deterministic but not presented as production model latency.
- `ipc`: worker round-trip, 41-way queue saturation against the 32-request mailbox, and deadline kill
  completion against the 250 ms escalation budget.
- `encoder`: default-feature offline MiniLM open, single encode, and batch 16. Missing artifacts produce
  `blocked_environment`; that population is never treated as passed or waived.
- `mixed`: observe/recall/consolidate/merge-prune/delete/compact/restart under the frozen config.

Kernel-only latency is **not** full-text recall latency. Production warm text recall includes ONNX
encoding plus the kernel and may include IPC depending on the measured surface.

## Frozen limits and acceptance

The effective predeclared limits are CONTRACT §5: record 16 KiB, request 256 KiB, reply 1 MiB, top-k 16,
advance 10,000, resident/catalog namespaces 4/32, mailbox 32 requests / 8 MiB, source basis 64 MiB,
controlled namespace storage 256 MiB with a 16 MiB reserve, kernel recall p95 25 ms at 4096 LTM centers,
warm text recall p95 250 ms, and durable observe p95 500 ms.

The task text refers to an “N03 budget manifest”, but no separately named N03 manifest is present in
`product/ncm/`; this evidence therefore evaluates the frozen values in CONTRACT §5 without changing them.

Unique idempotency events are not pruned by checkpoint or SQLite compaction. Growth is bounded by the
64 MiB source-basis quota, after which ingestion rejects before effect. This is a disclosed retention
limit, not an infinite lossless-memory claim.

`self-test` provides discriminating controls for record size, fixed center capacity, source quota,
wire size, top-k, advance, resident namespace LRU, checkpoint/compact/reopen digest equality, bounded
mailbox backpressure, and cancellation completion. Each control states the exact assertion a mutant
would fail.

## Current host result

`results-sathanovo-2-local.json` was captured on the recorded Apple host with 100k kernel operations
and 10k operations/attempts for the runtime populations. The full 4096-LTM kernel recall p95 was
0.460 ms against the 25 ms budget. Durable commit p95 was 15.3 ms (64 B), 44.0 ms (4 KiB), and
42.2 ms (16 KiB); 4 KiB and 16 KiB traces saturated the 64 MiB source basis after 8,898 and 3,384
commits respectively, and every remaining attempt rejected before effect. The slowest measured
HashEncoder end-to-end observe p95 was 59.4 ms against the 500 ms durable-observe budget. IPC
cancellation completed in 37.6 ms and a 41-way burst produced eight `Busy` results at the 32-request
mailbox. The largest measured peak RSS was 1,294,073,856 bytes in the one-namespace 16 KiB engine
trace. All checkpoint/compact/reopen digest, quota, capacity, wire, top-k, advance, LRU, mailbox, and
cancellation controls passed.

Acceptance remains environment-blocked for two required comparison populations: the pinned MiniLM
manifest/artifacts are absent from the admitted model root, and the available Python interpreter has
no PyTorch installation for the pinned Biomem `MemoryCenters.read` comparison. These are recorded in
the raw result rather than waived. Consequently no warm production full-text recall number is claimed.
