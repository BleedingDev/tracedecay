# NCM and semantic readiness extension

The parent [READINESS-PLAN.md](../READINESS-PLAN.md) remains the authoritative
Native/NCM/semantic definition of done. This execution-local plan splits its
two broad repair nodes into evidence-backed, non-overlapping lanes while
preserving the single `feat/pluggable-memory-providers-v2` branch and existing
worktree.

## NCM repair lanes

The historical `rn-ncm-recall-fix` file remains available for audit history but
is excluded from the active selection. Its work is now explicit:

1. `rn-ncm-byte-budget-fix` owns the deterministic core regression where an
   oversized ranked row must not hide a later row that fits the remaining
   UTF-8 text budget.
2. `rn-ncm-stage-trace` owns the real-worker causal trace. It must carry a
   synthetic correlation ID from host admission and journal sequence through
   ACK/watermark, worker readiness, common recall, adapter reconstruction,
   filtering and final delivery.
3. `rn-ncm-real-fix` owns only the exact runtime/worker/observer seam identified
   by the stage trace. It cannot guess from the deterministic budget result or
   a passing rerun.
4. `rn-ncm-proof-retry` owns the transient instance-proof negative-cache
   correction after the serialized `observation_journey.rs` handoff from
   `rn-session-delivery`.
5. `rn-provider-semantics` owns the NCM provider common/source-binding semantic
   boundary and gates the broader NCM tests and candidate build.

All four NCM code-fix lanes retain a failing-before/passing-after regression,
preserve scope/provenance/idempotency/cancellation/state/model invariants, and
release the focused NCM suite before build. `rn-verify-ncm` remains the only
closure campaign: at least 100 frozen attempts are required, split into 25
cold worker, 25 warm worker, 25 worker/daemon restart and 25 concurrent/load
trials. All attempts, failures and actual executed counts remain in the
ledger; no retry or sleep converts an unavailable result into success.

## Semantic repair lanes

The historical `rn-semantic-fix` file remains available for audit history but
is excluded from the active selection. Its work is now explicit:

1. `rn-semantic-runtime-dynamic` executes the pinned Jina fixture through the
   real candidate runtime and records the first unavailable-to-ready transition
   across acquisition, projection, calibration, activation, strict serving,
   restart and source-update/failure recovery.
2. `rn-semantic-acquisition-fix` owns the diagnosed model lifecycle and
   application acquisition/projection/calibration/activation seam.
3. `rn-semantic-serving-fix` owns the diagnosed code-index/query authority and
   strict semantic serving seam.

Dynamic evidence must be accepted before either fix or `rn-verify-semantic` is
released. A lexical result, generic availability text, fabricated calibration,
or hybrid fallback mislabeled as semantic is not evidence. Valid compatible
artifacts must commit the exact model, generation and calibration identity;
missing, corrupt, mismatched, offline and cancelled artifacts must remain
truthful. Final semantic verification repeats critical cold/warm/restart cases
and includes the fixed paraphrase oracle and unrelated negatives.

## Privacy and provider gates

`rn-privacy-trace-id` is an explicit gate for the accepted persisted explain
trace raw-ID blocker. It consumes the source changes from `rn-host-context` and
`rn-registry`, owns only privacy-safe hostile-ID fixtures and its acceptance
report, and must prove raw IDs are absent from serialized and reopened trace
rows. It gates `rn-privacy-restore`, which then gates the existing privacy and
session-delivery path.

`rn-provider-semantics` remains provider-local and advisory. Its acceptance
does not grant canonical Native, session or LCM authority and does not replace
the real-worker NCM campaign.

## Global gates

- One branch/worktree only; excluded historical plans are retained, not
  deleted.
- No Cargo command is submitted outside the designated cargo-hauler owner.
- No stable V1 profile, operator database, global model installation or Native
  reference checkout is changed.
- No active graph node is accepted from a zero-test filter, mock-only result,
  skipped fixture, favorable rerun or unreviewed generated snapshot.
- Root reviews and pushes accepted checkpoints frequently, then regenerates
  tracked graph artifacts only after this extension's plan selection and edge
  set are accepted.
