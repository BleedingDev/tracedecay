---
name: pm-live-origin
overview: Record bounded live transcript origin evidence so delayed catch-up cannot inherit a later checkout or branch identity.
todos:
  - id: record-live-origin-boundaries
    content: Extend existing durable hook admission metadata with bounded transcript identity/frontier and monotonic worktree branch-transition evidence, then seal only continuously authorized new frames.
    status: completed
  - id: verify-origin-discontinuity
    content: Prove old prefixes and partial frames cannot gain origin, branch A-to-B-to-A and generation changes invalidate open intervals, and unavailable evidence degrades safely.
    status: completed
isProject: false
---

# pm-live-origin

Root decision: use the existing durable hook admission ledger after live binding validation and canonical event admission. Own hooks admission_ledger.rs and focused tests, plus root hook_runtime/origin.rs, narrow hook_runtime/admission.rs calls and parent module declaration only. Coordinate provider-neutral low-level metadata with pm-history, which exclusively owns session capture/proof reading, provider_history.rs and observation_journey.rs. No provider, registry, session, store, manifests, or operator edits.

First live baseline excludes all old complete bytes and the physical EOF of any partial frame. Later live admissions seal only a contiguous interval whose file identity, generation, protected host/session, exact scope and bounded prefix continuity match. Worktree HEAD reflog identity/frontier/fingerprint or equivalent existing monotonic branch evidence must stay unchanged; missing/disabled/ambiguous evidence withholds origin. A branch transition, including A-to-B-to-A, discards the open interval and rebaselines. Ordinary ingestion cursors and current HEAD equality are not origin proof. Keep original hook budget and bounded reads. Store only bounded metadata, no transcript contents or new authority/database.

Leaf execution worker, no Cargo/model runs, commits, push, operator state or child agents. Root owns status and final review. Stop once implemented and focused fixtures are ready for root verification.

Root validation: bounded source/helper and ledger regressions passed in cc-1320, cc-1322 and cc-1324; the one nextest leak flag did not recur in isolated cc-1326 and remains noted for the final representative cohort. Combined root build cc-1330 passed. All four actual Git origin tests passed from its binary: checkout away/back, disabled/missing/rewritten reflogs, long bounded reflog tails, and linked worktree isolation. The producer implementation is complete; full host journeys consume it downstream.
