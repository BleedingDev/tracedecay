---
name: pm-history
overview: Provide one host-authorized canonical history replay/share path so either provider can reuse prior-session knowledge without weakening exact session namespaces.
todos:
  - id: produce-source-authority-and-deletion-state
    content: Implement the admitted repository identity bridge and bounded current canonical disposition projection, plus a durable provider-specific source-deletion fence in the existing host observation journal that survives provider unavailability and fresh-namespace restore.
    status: completed
  - id: authorize-history-source-selection
    content: Bridge authoritative canonical repository provenance to the mounted coding scope, then select bounded eligible history and bind requesting scope to original same-checkout sources without comparing incompatible identity hashes.
    status: completed
  - id: replay-history-with-origin
    content: Deliver eligible history into the selected provider's exact namespace with stable idempotency, original-source attribution, independent progress and truthful partial coverage.
    status: completed
  - id: history-correction-deletion-fences
    content: Propagate source corrections and deletion/tombstone state through replay and provider switching, proving no branch/worktree leak or deleted-source resurrection after restart.
    status: completed
isProject: false
---

# pm-history

Verified producer: the root library build passed; all 11 host history tests passed, including receipt-backed delivery waiting, bounded recent selection, live authority refresh, exact original attribution, cancellation and three journal lifetimes. The five exact-source journal evidence tests and bounded lock/cancellation test also passed. The public retained-control consumer remains part of host integration.

## Execution Notes

Read the brief, host audit and shared grant/source contracts. Reuse admitted canonical session/observation storage and the existing bounded delivery journey. Do not create another authoritative transcript store or a free-standing route cache. Native and NCM may represent the replay differently, but admitted source eligibility, time, deletion and attribution are the same. Replaying into session B never claims that session B authored session A's content.

The source audit found distinct identity schemes: canonical repository provenance uses project-salted repository/worktree IDs, while mounted `code_index.scope` uses daemon IDs. Consume validated `StoredObservation.repository_provenance_attachment()` and the authoritative marker context through one reviewed bridge; byte equality, path equality and relabeling cannot establish origin authority. Where retained evidence cannot authorize a legacy record, report missing coverage and leave it out. A fresh namespace restore needs current host deletion history after the snapshot; an old snapshot alone cannot prove that no source was later deleted. Do not declare a restored namespace ready until that bounded synchronization has a truthful outcome.

This lane owns the missing source authority producer as well as its replay consumer. Canonical privacy deletion reads the existing retrieval-anchor disposition authority; provider-local deletion writes a provider-specific source fence through the existing host observation journal, without changing canonical source/fact state. The later host control route records that fence before attempting provider dispatch, including when the provider is disabled or unavailable. It reports accepted host intent separately from verified provider erasure; a stored fence alone is never deletion success. Replay, readiness, restore and recall revalidate the fence; original source keys survive copies into B and C. A permitted new canonical source revision requires an explicit admitted revision transition under the common deletion mode/policy, never just new bytes or a fresh delivery key.

## Constraints

Own new root `retained_owner/provider_history.rs`, existing `observation_journey.rs` and dedicated tests. Also own exact root-reviewed source-port patches in `tracedecay-store/src/observation/mod.rs`, `observation/anchored_write.rs`; `tracedecay-sessions/src/repository_provenance.rs`, `observation.rs`, `runtime/ingest/project.rs`; bounded reads in `tracedecay-runtime-core/src/db/retrieval_anchor_authority.rs`; and provider-specific fence persistence in `tracedecay-memory-observation/src/sqlite/retention.rs` with its tests. Root reviews the precise hunks before these existing-authority extensions; no other lane edits them concurrently. Do not edit Native/NCM internals, registry `lib.rs`, cognitive recall, CLI fixtures or composition directly. Existing user profile/session/disposition authorities remain sole canonical selectors. No new database or registry is needed.

## Operator Guidance

Dependency: `pm-contract`; implement and verify its admitted source API before dependent replay work within this lane. Verify exact positive reuse with original attribution and negative different-profile/project/repository/worktree/branch/session-authorization cases, including delayed catch-up ingestion on a different branch. Verify deletion while the provider is unavailable, partial history, retention, replay cancellation and duplicate effects across three lifetimes. Return a small composition/control hookup to root, integrated later with active recall. No direct provider DB inspection in host code.
