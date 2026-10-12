# Establish the original Native source and protected boundary

Read research-common.md in this directory first.

Question: Pin the authoritative original v2 source revision and identify all native memory-owned source areas using existing upstream metadata and Git history. Distinguish the original PR 707 floor, later accepted upstream imports, current branch additions, and inaccurate stale design documents.

Inputs: product/upstream/**; product/architecture/adr/ADR-0008-upstream-convergence.md; Git commit and merge ancestry; original upstream files reachable through local Git objects.

Sole output file: ../evidence/source-baseline.md.

Do not edit any other file. Do not implement your proposal. Do not duplicate other reports. Send root the output path, the strongest evidence and any unresolved dependency when finished.
