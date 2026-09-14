---
name: V2 release and fresh-store cutover
overview: Produce and test installable V2 release artifacts, an opt-in offline NCM bundle, repeated cross-platform stability evidence, and a safe operator procedure that replaces V1 while preserving old bytes for inspection and rollback.
todos:
  - id: reconcile-release-packaging
    content: "Update release manifests, archive and MCPB builders, installers, direct upgrade, cloud asset selection, licenses, notices, and package metadata for the latest #707 layout plus the explicit offline NCM bundle; remove all retired dense code-search companions."
    status: pending
  - id: verify-release-artifacts
    content: "Build each advertised Linux, macOS, and Windows artifact from one commit and test clean extraction, version identity, daemon startup, default model-free operation, optional NCM assets, CLI and MCP launch, upgrade, and owned-only uninstall in isolated environments."
    status: pending
  - id: run-normal-cross-platform-ci
    content: "Make normal Linux, macOS, and Windows CI pass for default features, provider-host features, generated SDK and dashboard surfaces, host installers, release packaging, and the supported NCM configurations without skipped or zero-test replacement gates."
    status: pending
  - id: write-fresh-store-runbook
    content: "Document and test the replacement procedure: stop V1, back up and checksum the complete V1 profile, install V2, observe ResetRequired without touching incompatible bytes, explicitly reset or recreate the V2 targets, re-register hosts, replay historical transcripts and repositories as ordinary ingress, select Native or NCM, and verify restart and recall."
    status: pending
  - id: drill-binary-rollback
    content: "Test rollback by stopping both versions, preserving the V2 profile, restoring V1 binary selection, service or launch-agent state, sockets, host registrations, owned configuration, and the untouched V1 profile, then prove hosts launch V1; never import V2-created state into V1 and document that post-cutover work does not cross the rollback boundary."
    status: pending
  - id: measure-quality-performance-resources
    content: "Run simple reproducible Linux benchmarks and evaluations for lexical, graph, shared-code, Native, and NCM quality, latency, RSS, disk and WAL growth, worker count, backpressure, cancellation, shutdown, cold start, warm reuse, and restart; report failures separately from latency and keep cross-platform correctness in ordinary CI."
    status: pending
  - id: publish-beta-candidate
    content: "Produce a beta candidate and run clean install, V1 binary upgrade plus fresh-store recreation, Native and NCM recall, code retrieval, host lifecycle, restart, rollback, and uninstall smoke tests before stable promotion is considered."
    status: pending
  - id: run-repeated-canary-soak
    content: "Run the full direct product-journey set three consecutive times from the exact beta source and artifact set with fresh isolated profiles on the supported platform matrix; any unexplained failure or vacuous result resets the count."
    status: pending
isProject: false
---

# V2 release and fresh-store cutover

## Execution Notes

V2 replacement is a fresh-store cutover. Historical transcripts, repositories, and supported host sources are replayable ingress; V1 databases and other persisted shapes are inspection-only. The standard V2 artifact remains model-free. NCM ships as an explicit offline bundle or platform variant with all required assets pinned and tested; selecting NCM without those assets yields a typed unavailable result.

## Constraints

- No V1 or earlier-V2 reader, migration, conversion, backfill, census, shadow read, dual write, cutover database, or transition dashboard.
- The V1 backup must stay byte-identical and must never be opened by V2.
- Release acceptance uses direct package tests, simple benchmarks, and ordinary CI rather than local signing or self-authored attestation systems.
- Do not promote after one lucky pass; require stable aggregate evidence.
- Do not remove a platform or install method required by the early replacement contract merely to obtain green results; a deliberately narrower candidate must be named and cannot claim full V1 replacement.
- Every non-required advertised platform and install method must either pass or be explicitly removed from the release surface with a typed unsupported disposition.

## Operator Guidance

Depends on `V2 replacement product journeys`. Packaging, platform CI, measurements, and runbook preparation may proceed in parallel once their production contracts are frozen. Run the final soak only after all paths use one exact commit and release asset set. Commit and push packaging fixes in reviewable slices; do not combine host, NCM, and release-manifest changes without an aggregate review.
