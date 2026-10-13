# Original Native source boundary

Active reference: `57006f60cb45bcee8487e73a40d4fad1a12ee2b6` (unmodified upstream PR707 tip). Candidate metadata: `1fe250fed7ca615f1dfdcd580befeb276328e910`. Historical reference/audited head: `b3b43410e47115056f2066449aafa1822bbb6049` → `571daf3a9612e5247443e4da3a107b542686c1ef`.

## Preserve original implementation

The original `tracedecay-session-memory` and `tracedecay-lcm` crates, `tracedecay-store/src/memory`, `tracedecay/src/tracedecay/facts.rs`, original retained LCM services, and original memory-v2 schema/transactions are protected. The prior b3-to-571 comparisons are historical; the active 570-to-candidate inventory must refresh the current memory/session application and LCM boundaries without rolling back PR707.

The inventory must also include their retained routes, runtime-core database/storage support and shared session ingestion. Do not infer that an unchanged application crate proves every supporting authority is unchanged.

## Current retrieval-anchor equality gate

The active 570 and candidate `crates/tracedecay-runtime-core/src/db/retrieval_anchor_authority.rs` blobs both equal `d976e2a23c19441a60412420a1db5e68f7ccc5ca`; root's `git diff --exit-code` proof returned exit 0. rn-restore-anchor is therefore complete as a read-only current gate. The older b3 method-restoration finding remains historical. Preserve ordinary/missing/invalid-history behavior in later external fixtures; no source edit is authorized by this plan.

## Preserve pre-existing shared host extensions

The 23 existing differences under `tracedecay-sessions` and `tracedecay-session-runtime` are a **historical b3-to-571 classification**, recorded in [native-sessions.md](evidence/native-sessions.md). They are not a current 570 difference count. rn-source-docs reopens the current inventory and retains this table as historical evidence:

| Extension | Preservation requirement |
| --- | --- |
| Root-provided scope and original-provenance resolver in session sync and historical refresh ingestion | Preserve exact identity and ancillary resolver wiring; do not re-derive scope from a mutable path. Original temporal refresh state machine stays unchanged. |
| Strict Codex live transcript lookup and sealed JSONL source capture/admission | Preserve no-follow identity, native session IDs, bounds, source replacement checks and no-write deferral. Ordinary historical catch-up remains the original route. |
| Canonical transcript cursor encoding and exact CAS | Preserve checked ordinary cursors, full-width reserved frontiers and rejection of inappropriate monotonic writes. |
| Atomic live session locator registration | Preserve project/path identity, existing metadata, rollback and reopen behavior. Do not register or ingest the same session again inside Native. |
| Original provenance attachment and sanitized diagnostics | Preserve exact matching and privacy behavior; no blanket rollback. |

These are pre-existing product host/storage extensions, not byte-identical b3 host behavior and not a replacement Native memory engine. Keep them at the audited product behavior during restoration. Extension-only cases are separate regression evidence; do not fabricate an original b3 counterpart or count a changed safety policy as equal original behavior.

Additional reviewed support differences:

- `tracedecay-runtime-core/src/storage/paths_and_io.rs` adds interruptible directory creation. The original non-interruptible entry passes no callback and retains its existing lock/durability behavior. Preserve the extension and its test in `storage/tests.rs`; it is shared execution infrastructure, not a Native store/scorer.
- `tracedecay-store-runtime/src/retained_memory.rs` changes only `memory_application` visibility to `pub(super)`. Record that existing product seam; do not change the original retained operation bodies.

## Complete inventory before interface work

rn-source-docs owns `product/architecture/native-original-source-inventory.md`. It must enumerate all 570-to-candidate changed hunks in the protected/native-adjacent surface, with original path, current path, classification, original behavior, permitted action and associated check. The historical 85-path/278-hunk b3-to-571 audit remains untouched. Include:

- complete session-memory and LCM crates, including shared privacy/sanitization dependencies used by original Native operations;
- original memory/store contracts and facts.rs;
- runtime-core database/memory-v2/retrieval-anchor/storage support;
- retained fact and LCM routing;
- shared session/runtime/ingest/store-access, temporal refresh and host admission paths.

Use the existing Git/source evidence and this classification; do not create a permissive glob allowlist. Any additional original algorithm, authority, schema or lifecycle change blocks the affected lane until root assigns an exact restoration or external integration correction. Do not silently bless it as a historical difference.

rn-build validates the final inventory and source diffs. rn-review-fidelity independently checks it. Source preservation supplements full original behavior tests; it does not replace them.

## Verification split

Original Native cases compare active 570 and the candidate at the same original operation boundary. Shared host extensions additionally need focused tests for stable/mismatched sealed admission, ordinary and reserved cursor round-trip, exact CAS, locator conflict/rollback/reopen, historical catch-up and refresh begin/status/cancel after restart. Keep original source-sensitive checks and host-extension regressions separately identifiable.

No writer edits the existing shared ingestion/storage extensions in this plan. Native composition uses their existing mounted authorities; it does not add a new locator, parser, resolver, ingest transaction or summary engine.


## Historical privacy record and active seam gate

The earlier b3-to-571 acceptance of `crates/tracedecay-privacy/src/detector_kernel.rs` and typed Claude history admission remains historical. Under active 570, detector equality is recorded in `execution-results/privacy-audit-57006f60.md`; the typed history seam still needs review before rn-session-delivery. Generic admission, original Native and LCM privacy stay unchanged.
