# TraceDecay V2 baseline receipts

This directory contains product-owned evidence for the clean TraceDecay V2
baseline beneath the pluggable-memory-provider work. Receipts describe what
actually ran; they are not prose claims and they do not modify Zack-owned
runtime code.

The current Linux receipt is generated from branch
`feat/pluggable-memory-providers-v2`, whose runtime tree is required to remain
identical to the pinned PR #707 floor while this baseline is captured.

## Capture

The CI lane performs a full clean build and the focused memory, retrieval,
host, daemon, dashboard, and dashboard-API suites:

```bash
python3 scripts/product/capture-v2-baseline.py \
  --repo . \
  --output product/baseline/tracedecay-v2-pr707-linux.json
```

The runner records, for every command:

- exact argv and working directory;
- start/completion timestamps and duration;
- exit status and timeout state;
- stdout/stderr byte counts and SHA-256 digests;
- bounded diagnostic tails;
- a stable failure fingerprint and summary when a focused upstream test fails.

It also records the Rust, Cargo, Nextest, Node, npm, Python, Git, and ast-grep
versions, the OS identity, the product head, and the immutable upstream floor.

## Verify

```bash
python3 scripts/product/check-v2-baseline.py \
  --repo . \
  --receipt product/baseline/tracedecay-v2-pr707-linux.json
```

The verifier fails unless:

- the receipt head descends from the pinned floor;
- capture began from a clean checkout;
- no runtime-owned path differs from the floor;
- toolchain, setup, clean Rust CLI build, dashboard contracts, and dashboard
  build all passed;
- every required focused lane produced evidence;
- any focused failure is explicitly classified as an upstream failure with a
  summary and fingerprint.

## Result states

- `passed`: clean builds and every focused lane passed.
- `degraded`: clean builds passed; one or more focused upstream tests failed
  and are isolated in the receipt. This is still reviewable baseline evidence,
  not permission to hide or normalize the failure.
- `failed`: a build/setup prerequisite failed, a required lane is absent, the
  checkout was dirty, or product runtime code already diverged from the floor.

Only `passed` and evidence-complete `degraded` receipts are closure-eligible.
Any later product runtime edit belongs to subsequent beads and must not rewrite
this historical baseline receipt.
