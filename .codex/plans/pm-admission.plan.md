---
name: pm-admission
overview: Align existing host recall admission with the frozen common temporal and source-attribution contract while provider implementations proceed independently.
todos:
  - id: align-canonical-temporal-and-source-admission
    content: Implement root-approved requested-time lifecycle admission, explicit-null source revision handling and structurally validated original source DTOs without changing scope or granting provider claims authority.
    status: in_progress
  - id: verify-common-admission-vectors
    content: Verify all temporal modes, mixed known/unknown validity, strict candidate claims, legacy decoding, source metadata and original-source structural invariants with focused registry tests.
    status: pending
isProject: false
---

# pm-admission

The shared API/wire contract is the prerequisite. This task is implementation of the existing host admission consumer, not another API design gate. Root assigns the existing contract worker this additional disjoint scope after its shared contract checkpoint. Provider producers can implement the frozen API concurrently. Host integration waits for this admission behavior.

Own only registry `src/recall_admission.rs` temporal/source validation, required optional provenance DTOs and their focused tests. No registry registration/configuration, recall port routing/packing, Native/NCM code, composition or manifests. Keep the old source JSON property required; explicit null revision is independent of recorded validity and produces an explicit warning/degraded contribution. Existing known-revision inputs remain valid. Requested-time supersession/revocation must match the shared semantics, including past as-of and overlapping interval queries. Privacy deletion remains an existing host-authority decision; no reply flag can authorize deleted data. Structurally validated original_sources remain untrusted claims until host revalidation.

No Cargo/model run, operator mutation, commit/push or child agent. Root reviews exact changes, owns one heavy check, and marks status. Preserve other agents' edits and request any new seam through root.
