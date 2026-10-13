# Review checklist for the execution plan

## Native fidelity reviewer

Review the plan independently of the implementation proposals. Find concrete ways it could still deliver modified Native. Check the pinned original source, complete facts and session/LCM coverage, read-only versus mutating original operation boundaries, ranking and result order, privacy and memory ownership, feedback/trust, restart, real host delivery, original feature availability and context-added filtering. Verify that old ADRs do not override the user's requirement. Do not accept a current-tree shared implementation as the sole original baseline. Return a specific blocking flaw or a source-backed no-blocker result for each disputed boundary. Propose only the smallest plan correction.

## Graph and handoff reviewer

Validate exact selection and dependency overlay. Every selected plan must be linked, every node must have a clear owner and stop condition, and every concurrently active write lane must have disjoint files. The original implementation has no write owner. Shared files, generated contracts and Cargo.lock each have exactly one writer. Check actual dependencies rather than broadly serializing all implementation. Check that reviewers and baseline/fixture preparation can run alongside independent writers without guessing their interfaces. All agents must use Luna Max and root must remain orchestrator/reviewer. Check the peak against 50 available threads and keep all agents as leaves. Every handoff must include exact inputs, files, steps, forbidden shortcuts, behavioral acceptance and return format. The build broker must prevent duplicate compilation.

## Completion reviewer

Verify that the plan delivers one coherent Native/NCM cutover, removes active substitute routes, preserves existing data, keeps original Native intact, and verifies real user journeys. Reject a plan that ends at contracts, mocks, comments, checksums or one passing fact query. Ensure comparative claims match what the tested configurations actually include. All implementation todos remain pending during the planning task.
