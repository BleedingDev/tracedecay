# Execution DAG and ownership rereview

Review target: `native-original-execution-20260911`, current selection `a22a65832e`, 28 plans and 55 edges. The saved snapshot reports strict validation with no errors or warnings. The prior findings and the final anchor-case edge review are resolved.

Resolved:

- `rn-native-bridge -> rn-native-tests` is explicit. The test plan permits only disjoint preparation before the bridge is accepted; private bridge calls wait. `rn-composition` waits for both.
- `rn-reference -> rn-build-reference -> rn-build` is explicit. Both build plans name one designated Cargo owner; rn-build reuses the b3 artifact and does not duplicate compilation. `rn-build` now owns only `target/task-scratch/native-original/candidate-build/`; `reference-build/` and each verification subdirectory have separate owners. The build/verification artifact handoff is exact-path/digest based.
- The runner owns `scripts/product/native-original/case-contract.json`. Facts, sessions, state and host-fixture plans name that exact handoff, and `rn-contract -> rn-cases-{facts,sessions,state}` edges now gate their contract-dependent route/case identity work. Reference-to-fixture edges remain sufficient; no composition edge is needed for fixture authoring because build waits for both.
- The contract plan owns the route/output handoffs, including `execution-results/contract-outputs.md`, and requires the generator input/output inventory before generation. Generated output remains confined to the contract tree or provider-API crate; any other path requires a root ownership/DAG update. Cargo manifests and lockfile remain solely with rn-build.
- The source inventory is owned by rn-source-docs and gates both rn-contract and rn-restore-anchor. The exact b3 retrieval-anchor restoration has one writer and gates rn-build. No other node owns that original file. rn-restore-anchor sends restoration/test filters only to rn-build; rn-cases-facts derives anchor expectations independently from immutable b3 and SOURCE-BOUNDARY.md, so no restore-anchor-to-cases edge is required.
- Root-only `rn-close`, Luna Max, no-child execution/review roles, and the independent host-fixture/composition writers remain correct.

The anchor-case edge is approved as currently modeled. `rn-restore-anchor.plan.md` sends restoration/test filters only to rn-build, while `rn-cases-facts.plan.md` derives anchor expectations independently from immutable b3 and SOURCE-BOUNDARY.md and has no rn-restore-anchor output prerequisite. The saved dependency set therefore correctly contains `rn-reference -> rn-cases-facts`, `rn-contract -> rn-cases-facts`, and `rn-restore-anchor -> rn-build`, with no restore-anchor-to-cases edge.

The graph has disjoint writers, exact runner/contract/build handoffs, safe reference reuse, and no hidden dependency identified in this rereview. No Cargo or product-code check was run.
