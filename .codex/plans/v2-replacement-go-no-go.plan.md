---
name: V2 replacement go or no-go
overview: Make the final replacement decision from the integrated branch and tested release artifacts, with explicit blockers, independent architecture review, rollback evidence, and no unresolved Native, NCM, retrieval, host, store, or platform requirement.
todos:
  - id: audit-final-architecture
    content: "Review the final tree against the pinned latest PR #707 operating model and prove one daemon authority, exact final stores, one retrieval kernel, canonical Native ownership, isolated optional NCM, and exactly three code-intelligence authorities with no rejected mechanisms restored."
    status: pending
  - id: audit-capability-replacement
    content: "Audit the early replacement contract against final direct V2 journeys and results, including facts, sessions, LCM, code search, similar and redundancy, hosts and feedback, Native, NCM, controls, dashboard and Settings, Work and workflows, SDKs, restart, indexing, status, install, update, uninstall, and recovery; reject any unapproved scope loss."
    status: pending
  - id: review-verification-evidence
    content: "Have independent functional, security, release, and GPT-6 Astra reviewers inspect the exact final commit, direct tests, CI, package smokes, quality and resource measurements, repeated soak results, cutover runbook, and rollback drill for false passes and architectural drift."
    status: pending
  - id: close-all-blockers
    content: "Resolve every critical or blocking finding and repeat affected direct journeys and aggregate passes until no required capability is pending, blocked, unknown, unsupported, invalid, censored, silently partial, fallback-only, or effect-unknown."
    status: pending
  - id: freeze-replacement-candidate
    content: "Tag one exact clean commit and release artifact set only after origin matches the local branch, the worktree has no product changes, generated files and lockfiles are current, and all required CI and repeated product journeys are green."
    status: pending
  - id: authorize-v1-replacement
    content: "Present the exact candidate, evidence, known limits, fresh-store consequences, NCM packaging choice, cutover steps, and rollback result for the user's final replacement authorization before changing the stable V1 installation."
    status: pending
isProject: false
---

# V2 replacement go or no-go

## Execution Notes

The goal is a concrete operator decision: this exact V2 build and artifact set can replace stable V1 for supported workflows. “Compiles,” “runs locally,” or “most tests pass” is insufficient. The decision requires proven installability, useful retrieval, full Native and NCM behavior, fresh-store handling, host operation, physical restart, repeatability, and a rehearsed binary rollback.

## Constraints

- No stable replacement while PR #707 integration, required platforms, Native, NCM, retrieval, hosts, final-store admission, or rollback is unresolved.
- No required result may be established by stale artifacts, report-only claims, synthetic provider lookalikes, zero tests, empty outputs, or a single aggregate pass.
- Replacement authorization applies to the exact frozen commit and assets reviewed; later source changes reopen affected gates.
- Actual replacement of the user's stable V1 installation is a final external action and requires explicit user authorization after the candidate is concrete and reviewable.

## Operator Guidance

Depends on `V2 release and fresh-store cutover`. Run all independent reviews against the same commit and artifacts. Findings that change code or packaging reopen the affected plan and the three-pass aggregate soak. The final todo is a user decision, not an implementation subtask.
