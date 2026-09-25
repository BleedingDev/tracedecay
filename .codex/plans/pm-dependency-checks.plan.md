---
name: pm-dependency-checks
overview: Keep dependency enforcement precise for the test-only registry edge and the runtime numeric API imports without weakening adapter or registry capability boundaries.
todos:
  - id: enforce-reviewed-edge-kinds
    content: Preserve dependency kinds in the footprint checker and add exact import-only edge contracts that cannot replace mandatory full source contracts.
    status: in_progress
  - id: verify-boundary-negative-controls
    content: Reject production registry reachability, unlisted runtime API imports and any attempt to bypass the existing forbidden-symbol or executor-site floors.
    status: pending
isProject: false
---

# pm-dependency-checks

Root assigns scripts/product/check-memory-dependency-policy.py, check-memory-dependency-direction.py and their existing Python test files, plus the single relevant rule in product/upstream/patch-footprint-policy.json. Root retains product/architecture/memory-dependency-policy.json and manifests. New import-only contracts are additional checks only; they cannot satisfy or disable any mandatory full source contract or forbidden-symbol/executor floor. Keep existing defaults all dependency kinds; declare only the NCM adapter rule applies normal/build while exact metadata dev allowlists govern its registry test edge. No Cargo/model/operator/commit/child agents. Return precise root-owned policy JSON delta and run focused Python tests.
