---
name: rn-privacy-restore
overview: "Restore original privacy and isolate trusted Claude history identifiers at the product boundary."
todos:
  - id: rn-privacy-restore-done
    content: "Restore detector exactly to b3 and implement bounded typed host admission with regression coverage."
    status: in_progress
isProject: false
---

# Restore original privacy with a product history boundary

Workdir: /Users/satan/side/experiments/tracedecay/.worktrees/pluggable-memory-providers-v2 on every command. HEAD571daf3a9612e5247443e4da3a107b542686c1ef; original b3b43410e47115056f2066449aafa1822bbb6049. Read AGENTS.md, ../WORKER-RULES.md, ../REVIEWED-DECISIONS.md, ../SOURCE-BOUNDARY.md and the complete accepted ../execution-results/privacy-isolation-design.md. Prerequisite rn-privacy-audit accepted by root. Worker gpt-5.6-luna, reasoning max, fork none; no children/Cargo/commits/push. You are not alone; preserve peers. Root owns graph/status.

Exclusive writes:
- crates/tracedecay-privacy/src/detector_kernel.rs: restore complete file exactly to b3, no new original code/tests.
- crates/tracedecay-memory-hygiene/src/lib.rs and at most one private adjacent module: typed trusted source-field admission and focused product unit tests only; credentials.rs/recall_text.rs read-only.
- crates/tracedecay/src/daemon/retained_owner/observation_journey.rs: validated history admission call site and focused seam tests only. rn-session-delivery is serialized after this node.
- ../execution-results/privacy-restoration.md: exact imports/call signatures, source proof and test filters.

Implement all six invariants in the accepted audit. Shield only validated exact Claude source IDs at the named metadata source_key paths, never content/extensions/other scalar occurrences. Use a typed host entry point; strict generic and recall admission remain b3. Multiple grant sources require each source's provider/identity validation, not a blanket pointer exemption. Placeholder mapping must be collision-free, survive canonical sanitization, and fail closed on mismatch. Restore metadata before disposition comparison and final sanitized digest; original digest binds untouched input. Preserve limits, findings, error classification, provenance, and receipt verification. Do not copy privacy algorithms, broaden suffix allowance, add a manifest/generated contract, or alter NCM.

Restore source equality and implement product fix in this same lane; no intermediate successful restoration claim while host admission is broken. Unit checks must cover strict withholding, accepted byte-identical full-envelope receipt, malformed/provider/case/extra-tail rejection, duplicate grant fields, content containing matching ID, and existing credential-plus-suffix safeguards. Add meaningful private projection tests if needed; no Cargo until build owner. Run rustfmt scoped to owned product files and git diff --check. Do not format restored original file beyond exact b3.

Return exact paths/diff, actual lightweight checks, untouched original/NCM evidence, public seam contract, focused runtime filters and any exact blocker. Stop after bounded authoring. Root reviews before releasing session-delivery and build.
