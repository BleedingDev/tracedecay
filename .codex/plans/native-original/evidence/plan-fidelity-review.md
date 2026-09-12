# Native plan-fidelity review

**Initial decision: BLOCKED pending the corrections below.** The draft correctly fixes
the b3 reference, removes the staged Native substitute, keeps NCM separate, and
does not add a migration or upstream-sync gate. It is not yet executable enough
to establish the complete original Native behavior.

## 1. The protected source boundary has no executable baseline inventory

`.codex/plans/native-original/execution/rn-build.plan.md:36-40` only asks the
build owner to verify the b3 source, while
`.codex/plans/native-original/execution/rn-source-docs.plan.md:36-47` records
identity and documentation. Neither requires a complete b3-to-candidate
classification of all protected Native-adjacent source. The existing
`.codex/plans/native-original/evidence/source-baseline.md:40-55` comparison is
narrow; the session evidence records additive behavioral changes in shared
`tracedecay-sessions` and `tracedecay-session-runtime` files, including the
live-origin resolver, sealed admission, transcript cursor/storage transaction,
and Codex locator (`native-sessions.md:252-278`). Those are approved outer
integration seams, not evidence that the whole shared host/session tree is
byte-identical to b3.

Without an explicit inventory, a later worker can treat those changes as part
of “unchanged original” or accidentally revert them while removing staged
mount assumptions. That risks losing source identity, privacy, cursor CAS, or
Codex live admission while all Native memory files remain untouched.

**Minimal correction:** add a required baseline artifact to
`rn-source-docs`/`rn-build` and a final check in
`rn-review-fidelity`: compare b3 with the candidate across the complete
protected surface (`tracedecay-session-memory`, `tracedecay-lcm`,
`tracedecay-sessions` runtime/ingest/store-access, `tracedecay-session-runtime`,
retained LCM routing, store contracts/runtime, and `facts.rs`); classify every
existing hunk as approved outer integration or forbidden Native algorithm,
authority, schema, or lifecycle change. Require preservation tests for the
live-origin/sealed-admission and cursor transaction cases. Do not use a blanket
allowlist or roll back the recorded privacy/Codex fixes.

## 2. “Native context once” is stated but not enforced at the core seam

`.codex/plans/native-original/execution/rn-contract.plan.md:37-41`,
`rn-registry.plan.md:35-46`, and
`rn-host-context.plan.md:35-47` say to identify the already-executed
canonical `memory_matches` contribution and avoid a second Native query.
However, they do not require the post-handler advisory path to short-circuit
for that route. The host evidence says canonical `memory_matches` is produced
first and the selected-provider advisory lane runs afterward
(`evidence/host-delivery.md:35-47, 80-86`). A worker could therefore invoke
Native `Recall`, deduplicate its output, and still satisfy the prose while
changing query count, limits, ordering, telemetry, omissions, or model-visible
bytes. “No extra candidate copy” is not sufficient.

**Minimal correction:** make the contract/registry handoff carry a typed,
identity-bound marker that the canonical Native contribution was delivered.
Require `tool_dispatch.rs`/`cognitive_recall.rs` to skip
`advisory_context_call` for that marker, while retaining the separately
callable generic Native route and the advisory call for selected NCM/injected
providers. Add an acceptance test asserting exactly one canonical
`memory_matches` execution, zero Native provider invocations, unchanged query
fields/limit/order/omissions and exact model-visible output; capture no fake
provider terminal. NCM selected/observer cases must still retain shared
canonical facts, sessions, and LCM.

## 3. The complete route/effect gate is too abstract and permits dead routes

`.codex/plans/native-original/execution/rn-native-port.plan.md:36-46` and
`rn-native-bridge.plan.md:38-49` allow “typed unsupported” and say original
commands remain reachable “by the accepted route design.”
`rn-composition.plan.md:37-47` only asks the worker to “record which” fact,
session, and LCM routes remain reachable. The direct case plans exercise many
original services, but they do not make selected Native composition prove that
each operation reaches its owner-bound route. This leaves a path where Native
works for fact reads, while explicit Search effects, mutations, session
refresh/task-session, or LCM lifecycle are dead or silently reduced to generic
unsupported. It also risks applying the generic read-only Recall policy to an
original effectful operation.

**Minimal correction:** make the `rn-contract` operation-to-route table a
required handoff, enumerating each b3 fact operation (including explicit
Search/retrieval receipt, mutation, feedback/trust, history/lineage,
curation/privacy/automatic receipts), session/refresh/task-session route, and
LCM operation with its exact public caller, owner, effect/receipt, and case.
Require bridge/composition acceptance to execute or otherwise directly invoke
every mapped route through production composition. Permit `unsupported` only
after the independent b3 runner proves that no exact original feature exists;
an absent product connection remains a blocking gap. Preserve explicit Search
effects and keep automatic/semantic reads’ effect boundaries separate.

Until these three corrections are in the execution handoffs and acceptance
criteria, the plan could preserve the b3 fact crate yet still ship a partial
Native adapter, an extra Native context query, or dead/stripped original
routes.

## Bounded rereview after plan corrections

**Final decision: APPROVED WITH LIMITS.** The requested corrections are now
present in the plan bundle; no additional plan-level blocker remains.

- `.codex/plans/native-original/SOURCE-BOUNDARY.md` now distinguishes the
  unchanged b3 memory/LCM engines from the pre-existing shared host/session
  extensions, requires a complete hunk inventory, and authorizes only the
  exact retrieval-anchor restoration. `rn-source-docs` must produce the
  inventory before `rn-contract`/`rn-restore-anchor`; `rn-build` and
  `rn-review-fidelity` must recheck every final hunk. This preserves the
  sealed admission, provenance, cursor/CAS, locator, privacy, and Codex
  extensions without a blanket rollback.
- The contract and registry now define a trusted host-owned
  identity/revision-bound canonical-delivery marker. `rn-host-context` has an
  explicit dispatch rule to bypass `advisory_context_call` for Native and to
  test one eligible canonical `memory_matches` execution with zero Native
  provider invocations, including memory-disabled, refusal, and empty cases.
  NCM/injected advisory delivery and shared canonical services remain intact.
- `rn-contract` now owns the exact operation-route/effect/receipt/case map,
  and bridge/composition require each mapped route to be invoked through
  selected Native production composition. Missing product connections block;
  `unsupported` is allowed only when the independent b3 runner proves no
  exact original feature.

The approval is limited to the plan: execution must still produce and root
accept the inventory and route-map artifacts, restore the anchor byte-for-byte,
and pass independent b3-versus-product route/effect, host-delivery, restart,
failure, and NCM-coexistence evidence. Plan text or source checks alone cannot
claim complete Native equivalence.
