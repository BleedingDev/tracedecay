#!/usr/bin/env python3
"""Contract tests for provider-neutral recall semantics."""

from __future__ import annotations

import copy
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
CONTRACT = REPO / "product/contracts/memory-provider-v1/provider-recall-contract.json"
SCHEMA = REPO / "product/contracts/memory-provider-v1/provider-recall-contract.schema.json"
DOC = REPO / "product/contracts/memory-provider-v1/provider-recall-contract.md"
ISSUES = REPO / ".beads/issues.jsonl"
CHECKER = REPO / "scripts/product/check-provider-recall-contract.py"


class ProviderRecallContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
        cls.schema = json.loads(SCHEMA.read_text(encoding="utf-8"))

    def run_checker(
        self,
        contract: dict[str, Any] | None = None,
        schema: dict[str, Any] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        if contract is None and schema is None:
            contract_path = CONTRACT
            schema_path = SCHEMA
            temporary = None
        else:
            temporary = tempfile.TemporaryDirectory()
            root = Path(temporary.name)
            contract_path = root / "contract.json"
            schema_path = root / "schema.json"
            contract_path.write_text(
                json.dumps(contract or self.contract, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            schema_path.write_text(
                json.dumps(schema or self.schema, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        try:
            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--contract",
                    str(contract_path),
                    "--schema",
                    str(schema_path),
                    "--doc",
                    str(DOC),
                    "--issues",
                    str(ISSUES),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
        finally:
            if temporary is not None:
                temporary.cleanup()

    def assert_rejected(
        self,
        contract: dict[str, Any],
        marker: str,
        schema: dict[str, Any] | None = None,
    ) -> None:
        result = self.run_checker(contract, schema)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertFalse(receipt["ok"])
        self.assertIn(marker, "\n".join(receipt["errors"]))

    def test_real_repository_contract_is_valid(self) -> None:
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertTrue(receipt["ok"])
        self.assertEqual(
            receipt["contract_id"], "tracedecay.memory.provider.recall.v1"
        )
        self.assertEqual(receipt["bead_id"], "tdmem-0204")
        self.assertFalse(receipt["stable_memory_ref_required"])
        self.assertFalse(receipt["native_scores_cross_provider_comparable"])
        self.assertEqual(receipt["normalized_score_owner"], "TraceDecay context compiler")
        self.assertEqual(
            receipt["provenance_states"], ["available", "redacted", "unavailable"]
        )
        self.assertEqual(
            receipt["temporal_modes"], ["current", "as_of", "interval", "history"]
        )
        self.assertEqual(receipt["terminal_state_count"], 17)
        self.assertFalse(receipt["provider_may_inject_context"])

    def test_candidate_scope_binding_is_required_and_closed(self) -> None:
        contract = copy.deepcopy(self.contract)
        del contract["candidate_scope_binding"]
        self.assert_rejected(contract, "candidate_scope_binding must be an object")

        contract = copy.deepcopy(self.contract)
        contract["candidate_scope_binding"]["bindings"].append("repository_facts")
        self.assert_rejected(
            contract,
            "candidate scope bindings must mirror the authority-matrix namespaces",
        )

        contract = copy.deepcopy(self.contract)
        contract["candidate_scope_binding"]["required_fields"].remove("scope_binding")
        self.assert_rejected(
            contract, "candidate scope fields must be scope_binding plus the exact scope"
        )

    def test_candidate_scope_binding_authorization_is_host_owned(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["candidate_scope_binding"]["provider_may_widen_binding"] = True
        self.assert_rejected(
            contract, "candidate_scope_binding.provider_may_widen_binding must be false"
        )

        contract = copy.deepcopy(self.contract)
        contract["candidate_scope_binding"]["authorization_carried_by"] = "provider_reply"
        self.assert_rejected(
            contract, "scope binding authorization must travel with the admitted call"
        )

        contract = copy.deepcopy(self.contract)
        contract["candidate_scope_binding"]["unauthorized_binding_policy"] = "allow"
        self.assert_rejected(
            contract,
            "candidate_scope_binding.unauthorized_binding_policy must be "
            "reject_scope_binding_unauthorized",
        )

    def test_candidate_scope_binding_rules_mirror_authority_matrix(self) -> None:
        contract = copy.deepcopy(self.contract)
        rules = {
            rule["binding"]: rule
            for rule in contract["candidate_scope_binding"]["binding_rules"]
        }
        rules["project_facts"]["forbidden"].remove("agent_session_id")
        rules["project_facts"]["optional_empty_or_equal"].append("agent_session_id")
        self.assert_rejected(
            contract, "binding rule project_facts.optional_empty_or_equal drifted"
        )

        contract = copy.deepcopy(self.contract)
        rules = {
            rule["binding"]: rule
            for rule in contract["candidate_scope_binding"]["binding_rules"]
        }
        rules["profile_facts"]["required_equal"].append("project_id")
        rules["profile_facts"]["forbidden"].remove("project_id")
        self.assert_rejected(
            contract, "binding rule profile_facts.required_equal drifted"
        )

        contract = copy.deepcopy(self.contract)
        contract["candidate_scope_binding"]["binding_rules"].pop()
        self.assert_rejected(
            contract, "binding rules must cover every binding in contract order"
        )

    def test_request_must_carry_exact_scope(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["recall_request"]["required_fields"].remove("exact_scope_identity")
        self.assert_rejected(
            contract,
            "recall request required fields must remain canonical and ordered",
        )

    def test_request_must_carry_deadline_and_cancellation(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["recall_request"]["required_fields"].remove("deadline")
        contract["recall_request"]["required_fields"].remove("cancellation")
        self.assert_rejected(
            contract,
            "recall request required fields must remain canonical and ordered",
        )

    def test_empty_query_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["recall_request"]["empty_query_allowed"] = True
        self.assert_rejected(contract, "recall_request.empty_query_allowed must be false")

    def test_provider_cannot_widen_scope(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["recall_request"]["provider_may_widen_scope"] = True
        self.assert_rejected(
            contract, "recall_request.provider_may_widen_scope must be false"
        )

    def test_scope_wildcards_are_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["exact_scope_semantics"]["wildcards_allowed"] = True
        self.assert_rejected(
            contract, "exact_scope_semantics.wildcards_allowed must be false"
        )

    def test_repository_only_scope_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["exact_scope_semantics"]["repository_only_match_allowed"] = True
        self.assert_rejected(
            contract,
            "exact_scope_semantics.repository_only_match_allowed must be false",
        )

    def test_cross_worktree_recall_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["exact_scope_semantics"]["cross_worktree_recall_allowed"] = True
        self.assert_rejected(
            contract,
            "exact_scope_semantics.cross_worktree_recall_allowed must be false",
        )

    def test_cross_session_recall_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["exact_scope_semantics"]["cross_session_recall_allowed"] = True
        self.assert_rejected(
            contract,
            "exact_scope_semantics.cross_session_recall_allowed must be false",
        )

    def test_temporal_modes_are_explicit(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["temporal_query"]["modes"].remove("as_of")
        self.assert_rejected(
            contract, "temporal query modes must remain canonical and ordered"
        )

    def test_as_of_mode_requires_timestamp(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["temporal_query"]["as_of_required_for_mode"] = "optional"
        self.assert_rejected(contract, "as_of timestamp must be required for as_of mode")

    def test_invalid_interval_must_fail(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["temporal_query"]["invalid_interval_policy"] = "swap_bounds"
        self.assert_rejected(contract, "invalid temporal interval must reject request")

    def test_unknown_validity_defaults_to_exclude(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["temporal_query"]["default_unknown_validity_policy"] = "allow"
        self.assert_rejected(contract, "unknown validity must be excluded by default")

    def test_provider_cannot_exceed_budget(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["budgets"]["provider_may_exceed_budget"] = True
        self.assert_rejected(contract, "provider cannot exceed recall budget")

    def test_zero_budget_must_fail(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["budgets"]["zero_budget_policy"] = "return_empty"
        self.assert_rejected(
            contract, "budgets.zero_budget_policy must reject invalid request"
        )

    def test_ignored_exclusion_is_contract_violation(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["exclusions"]["ignored_exclusion_policy"] = "warning"
        self.assert_rejected(
            contract, "ignored exclusion must be contract violation"
        )

    def test_unknown_optional_extension_round_trips_inertly(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["extension_contract"][
            "unknown_optional_extension_policy"
        ] = "drop"
        self.assert_rejected(
            contract, "unknown optional recall extensions must round-trip inertly"
        )

    def test_unknown_required_extension_fails_explicitly(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["extension_contract"][
            "unknown_required_extension_policy"
        ] = "ignore"
        self.assert_rejected(
            contract, "unknown required recall extensions must fail explicitly"
        )

    def test_extension_cannot_change_scope(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["extension_contract"]["unknown_extension_may_change_scope"] = True
        self.assert_rejected(
            contract,
            "extension_contract.unknown_extension_may_change_scope must be false",
        )

    def test_stable_memory_ref_remains_optional(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["provider_candidate"]["stable_memory_ref_required"] = True
        self.assert_rejected(contract, "stable memory references must be optional")

    def test_candidate_id_is_not_stable_memory_identity(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["provider_candidate"][
            "candidate_id_stable_across_requests"
        ] = True
        self.assert_rejected(
            contract, "candidate ID must not be stable across requests"
        )

    def test_candidate_confidence_contract_is_required_nullable(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["provider_candidate"]["required_fields"].remove("confidence")
        self.assert_rejected(contract, "provider candidate fields drifted")

        cases = [
            ("confidence_required_nullable", False, "must be required-nullable"),
            (
                "confidence_null_semantics",
                "missing_is_zero",
                "confidence null semantics drifted",
            ),
            (
                "confidence_number_semantics",
                "any_number",
                "confidence number semantics drifted",
            ),
        ]
        for key, value, marker in cases:
            with self.subTest(key=key):
                contract = copy.deepcopy(self.contract)
                contract["provider_candidate"][key] = value
                self.assert_rejected(contract, marker)

    def test_candidate_requires_exactly_one_content_form(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["provider_candidate"]["content_selection_rule"] = "both_allowed"
        self.assert_rejected(
            contract, "candidate must contain exactly one content form"
        )

    def test_provider_candidate_cannot_mutate_context(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["provider_candidate"]["provider_candidate_may_mutate_context"] = True
        self.assert_rejected(
            contract,
            "provider_candidate.provider_candidate_may_mutate_context must be false",
        )

    def test_content_hydration_revalidates_scope(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["content_reference"]["hydration_requires_scope_revalidation"] = False
        self.assert_rejected(contract, "content hydration must revalidate scope")

    def test_native_scores_are_not_cross_provider_comparable(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["native_score"][
            "provider_native_scores_cross_provider_comparable"
        ] = True
        self.assert_rejected(
            contract, "native scores must not be cross-provider comparable"
        )

    def test_native_scores_are_not_cross_domain_comparable(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["native_score"][
            "provider_native_scores_cross_domain_comparable"
        ] = True
        self.assert_rejected(
            contract, "native scores must not be cross-domain comparable"
        )

    def test_non_finite_native_scores_are_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["native_score"]["non_finite_score_allowed"] = True
        self.assert_rejected(contract, "non-finite native score must be forbidden")

    def test_trace_decay_owns_normalized_score(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["host_normalized_score"]["owner"] = "provider"
        self.assert_rejected(
            contract, "TraceDecay context compiler must own normalized score"
        )

    def test_provider_cannot_supply_normalized_score(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["host_normalized_score"][
            "provider_may_supply_normalized_score"
        ] = True
        self.assert_rejected(
            contract, "provider cannot supply host-normalized score"
        )

    def test_cross_provider_comparison_requires_normalization(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["host_normalized_score"][
            "normalization_required_before_cross_provider_comparison"
        ] = False
        self.assert_rejected(
            contract, "normalization must precede cross-provider comparison"
        )

    def test_revoked_candidate_defaults_to_exclude(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["validity"]["revoked_candidate_default_policy"] = "allow"
        self.assert_rejected(
            contract, "validity.revoked_candidate_default_policy must be exclude"
        )

    def test_missing_provenance_is_explicit(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["provenance"]["missing_provenance_is_explicit"] = False
        self.assert_rejected(contract, "missing provenance must be explicit")

    def test_provider_cannot_fabricate_provenance(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["provenance"]["provider_may_fabricate_provenance"] = True
        self.assert_rejected(contract, "provider cannot fabricate provenance")

    def test_unavailable_provenance_defaults_to_exclude(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["provenance"]["default_unavailable_policy_action"] = "allow"
        self.assert_rejected(
            contract, "unavailable provenance must default to exclude"
        )

    def test_explanation_is_not_proof(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["explanation"]["explanation_is_proof"] = True
        self.assert_rejected(contract, "provider explanation must not be proof")

    def test_provider_cannot_inject_context(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["recall_response"]["provider_may_inject_context"] = True
        self.assert_rejected(
            contract, "recall_response.provider_may_inject_context must be false"
        )

    def test_empty_candidate_list_is_not_fallback_signal(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["recall_response"][
            "empty_candidate_list_is_not_failure_or_fallback_signal"
        ] = False
        self.assert_rejected(
            contract, "empty candidate list must not imply failure or fallback"
        )

    def test_zero_results_requires_complete_search(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["coverage"][
            "zero_results_requires_successful_complete_search"
        ] = False
        self.assert_rejected(
            contract, "coverage.zero_results_requires_successful_complete_search must be true"
        )

    def test_provider_order_has_no_cross_provider_authority(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["ordering"]["provider_order_cross_provider_authority"] = True
        self.assert_rejected(
            contract, "provider order has no cross-provider authority"
        )

    def test_terminal_states_cannot_hide_scope_mismatch(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["recall_specific_terminal_states"].remove("scope_mismatch")
        self.assert_rejected(
            contract, "recall terminal states must exactly cover V1 outcomes"
        )

    def test_unknown_verification_bead_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["verification_beads"].append("tdmem-9999")
        self.assert_rejected(
            contract,
            "verification_beads references unknown Beads issue tdmem-9999",
        )

    def test_schema_root_is_strict(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["additionalProperties"] = True
        self.assert_rejected(
            copy.deepcopy(self.contract),
            "recall schema root must be a strict object",
            schema,
        )

    def test_schema_requires_native_score(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["required"].remove("native_score")
        self.assert_rejected(
            copy.deepcopy(self.contract),
            "recall schema required fields must match the contract",
            schema,
        )


if __name__ == "__main__":
    unittest.main()
