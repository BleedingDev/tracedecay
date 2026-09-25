# Operator ledger

Graph: pluggable-memory-compat-v1. Exact selection and 40-edge overlay in pm-orchestration.md. Root owns statuses/manifests/builds/publication. All work in product checkout; master and operator state untouched.

| Lane | Agent | Status / evidence | Next |
|---|---|---|---|
| Root | /root | Combined lib compilation cc-1321 active; 52 dependency-check unit tests pass | Compile fixes; source-authority injection review |
| Contract/admission | pm_contract_impl | Shared API/admission/conformance checkpoint 28 tests pass; schema/generator checks pass | Frozen |
| Native | pm_native_impl | Common lifecycle/store/portability ready; 7 real application tests added | Root compilation + independent portability review |
| NCM | pm_ncm_impl | Canonical mapping, authority and controls checkpoint ready | Final receipt/trace and portability hookup |
| NCM portability | pm_ncm_restore_identity | New adapter/runtime portability modules active | Precise hookup via main NCM owner |
| Registration | pm_registration_impl | Reviewed root composition patch applied; 7 configuration tests pass | Recheck Disabled guard and root integration |
| History | pm_history_impl | Journal/source 7 tests pass; authority module freeze | Unapplied exact late-binding composition patch |
| Live origin | pm_live_origin | Producer complete; initial exclusion floor survives continuous partial frames | Root hook/branch tests |
| Conformance | pm_conformance_impl | Stable action/result interface; independent review found 7 concrete assertion corrections | Finish corrections and root tests |
| Host audit | pm_host_audit | Composition and conformance review complete | Findings returned to owners |
| Native audit | pm_native_audit | Verifying original five fixes + new portability | Concrete blockers only |
| Comparison harness | pm_comparison_harness | Preparation active on stable interfaces; no model runs | Controlled runner/integrity tests, real connection downstream |
| Prep lanes | NCM cancel/delete/restore + Native integrity | Source reviewed; NCM 28 runtime + 3 cancellation pass | Parent implementation owners retain files |
| Held-out/protocol | pm_heldout_a/b/c + pm_eval_protocol | Frozen 18 cases, 56 queries; independent ambiguity review complete | No new output read |
| Measurement | pm_measurement_capture | Source reviewed; 20 tests pass | Harness reuse |

No complete-provider or performance claims yet. Native integrity runtime waits root-library validation. No commits since published 6010f31e7.
