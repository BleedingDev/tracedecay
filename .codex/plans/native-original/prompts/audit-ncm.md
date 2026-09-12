# Keep the NCM implementation isolated from the Native repair

Read research-common.md in this directory first.

Question: Map NCM dependencies on the current shared profile and identify only integration changes needed if the profile is split or relaxed. Preserve its model, learning, scope identity and lifecycle behavior; account for the already observed unstable recall result without claiming a fix.

Inputs: crates/tracedecay-memory-provider-ncm/**; product/ncm/spec/CONTRACT.md; NCM registration/host tests; shared API call sites.

Sole output file: ../evidence/ncm-boundary.md.

Do not edit any other file. Do not implement your proposal. Do not duplicate other reports. Send root the output path, the strongest evidence and any unresolved dependency when finished.
