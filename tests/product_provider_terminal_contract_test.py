#!/usr/bin/env python3
"""Contract tests for typed provider terminal outcomes."""

from __future__ import annotations

import copy
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
CONTRACT = REPO / "product/contracts/memory-provider-v1/provider-terminal-contract.json"
SCHEMA = REPO / "product/contracts/memory-provider-v1/provider-terminal-contract.schema.json"
DOC = REPO / "product/contracts/memory-provider-v1/provider-terminal-contract.md"
ISSUES = REPO / ".beads/issues.jsonl"
CHECKER = REPO / "scripts/product/check-provider-terminal-contract.py"


class ProviderTerminalContractTest(unittest.TestCase):
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

    def terminal(self, contract: dict[str, Any], code: str) -> dict[str, Any]:
        return next(row for row in contract["terminal_codes"] if row["code"] == code)

    def test_real_repository_contract_is_valid(self) -> None:
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertTrue(receipt["ok"])
        self.assertEqual(
            receipt["contract_id"], "tracedecay.memory.provider.terminal.v1"
        )
        self.assertEqual(receipt["bead_id"], "tdmem-0206")
        self.assertEqual(receipt["terminal_code_count"], 20)
        self.assertEqual(receipt["mandatory_operation_count"], 3)
        self.assertFalse(receipt["automatic_retry_default"])
        self.assertEqual(receipt["fallback_default"], "forbidden")
        self.assertEqual(receipt["current_fallback_policy"], "no_automatic_fallback")
        self.assertEqual(
            receipt["effect_states"],
            ["none", "committed", "duplicate", "partial", "unknown"],
        )
        self.assertTrue(receipt["cancelled_distinct_from_timeout"])

    def test_terminal_envelope_cannot_be_omitted(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["terminal_envelope"]["provider_may_omit_terminal_envelope"] = True
        self.assert_rejected(contract, "provider may not omit terminal envelope")

    def test_empty_response_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["terminal_envelope"]["empty_response_allowed"] = True
        self.assert_rejected(contract, "empty provider response must be forbidden")

    def test_missing_mandatory_identity_field_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["terminal_envelope"]["required_fields"].remove("exact_scope_digest")
        self.assert_rejected(contract, "terminal envelope required fields drifted")

    def test_terminal_table_is_closed(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["terminal_codes"].append(
            {
                "code": "mystery",
                "class": "unknown",
                "effect_expectation": "unknown",
                "retry_class": "never",
                "fallback_eligibility": "forbidden",
            }
        )
        self.assert_rejected(
            contract, "terminal-code table must exactly contain the twenty V1 codes"
        )

    def test_cancelled_is_not_timeout(self) -> None:
        contract = copy.deepcopy(self.contract)
        self.terminal(contract, "cancelled")["code"] = "deadline_exceeded"
        self.assert_rejected(contract, "duplicate terminal_codes code deadline_exceeded")

    def test_partial_effect_requires_partial_effect_state(self) -> None:
        contract = copy.deepcopy(self.contract)
        self.terminal(contract, "partial_effect")["effect_expectation"] = "none"
        self.assert_rejected(contract, "terminal partial_effect semantics drifted")

    def test_effect_unknown_requires_unknown_state(self) -> None:
        contract = copy.deepcopy(self.contract)
        self.terminal(contract, "effect_unknown")["effect_expectation"] = "partial"
        self.assert_rejected(contract, "terminal effect_unknown semantics drifted")

    def test_domain_detail_cannot_change_retry(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["domain_detail"]["detail_may_change_retry_or_fallback"] = True
        self.assert_rejected(
            contract, "domain_detail.detail_may_change_retry_or_fallback must be false"
        )

    def test_unknown_optional_detail_round_trips_inertly(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["domain_detail"]["unknown_optional_detail_policy"] = "drop"
        self.assert_rejected(contract, "unknown optional detail must round-trip inertly")

    def test_failure_result_payload_is_forbidden(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["result_payload"]["failure_result_payload_allowed"] = True
        self.assert_rejected(contract, "failure result payload must be forbidden in V1")

    def test_result_payload_is_handshake_bounded(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["result_payload"]["maximum_payload_bytes_source"] = "unbounded"
        self.assert_rejected(contract, "result payload limit must come from handshake")

    def test_zero_results_requires_zero_coverage(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["coverage"][
            "success_zero_results_requires_zero_results_coverage"
        ] = False
        self.assert_rejected(
            contract,
            "coverage.success_zero_results_requires_zero_results_coverage must be true",
        )

    def test_failure_cannot_claim_complete_coverage(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["coverage"]["failure_cannot_claim_complete_coverage"] = False
        self.assert_rejected(
            contract, "coverage.failure_cannot_claim_complete_coverage must be true"
        )

    def test_automatic_retry_defaults_off(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["retry"]["automatic_retry_default"] = True
        self.assert_rejected(contract, "automatic retry must default to false")

    def test_unbounded_retry_attempts_are_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["retry"]["unbounded_attempts_allowed"] = True
        self.assert_rejected(contract, "retry.unbounded_attempts_allowed must be false")

    def test_retry_cannot_change_idempotency_key(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["retry"][
            "retry_may_reuse_new_idempotency_key_for_same_mutation"
        ] = True
        self.assert_rejected(
            contract,
            "retry.retry_may_reuse_new_idempotency_key_for_same_mutation must be false",
        )

    def test_retry_waits_for_effect_reconciliation(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["retry"][
            "retry_may_begin_before_unknown_or_partial_effect_reconciliation"
        ] = True
        self.assert_rejected(
            contract,
            "retry.retry_may_begin_before_unknown_or_partial_effect_reconciliation must be false",
        )

    def test_fallback_defaults_forbidden(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["fallback"]["default_eligibility"] = "explicit_policy_only"
        self.assert_rejected(contract, "fallback must default to forbidden")

    def test_fallback_not_inferred_from_empty_result(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["fallback"]["fallback_may_be_inferred_from_empty_result"] = True
        self.assert_rejected(
            contract, "fallback.fallback_may_be_inferred_from_empty_result must be false"
        )

    def test_fallback_not_inferred_from_unavailable(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["fallback"][
            "fallback_may_be_inferred_from_provider_unavailable"
        ] = True
        self.assert_rejected(
            contract,
            "fallback.fallback_may_be_inferred_from_provider_unavailable must be false",
        )

    def test_fallback_requires_new_handshake(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["fallback"][
            "fallback_requires_new_handshake_and_scope_admission"
        ] = False
        self.assert_rejected(
            contract,
            "fallback.fallback_requires_new_handshake_and_scope_admission must be true",
        )

    def test_current_policy_has_no_automatic_fallback(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["fallback"]["current_product_policy"] = "fallback_to_native"
        self.assert_rejected(
            contract, "current product policy must forbid automatic fallback"
        )

    def test_read_only_operation_requires_no_effect(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["committed_effect"]["read_only_operation_requires_none"] = False
        self.assert_rejected(
            contract, "committed_effect.read_only_operation_requires_none must be true"
        )

    def test_partial_effect_requires_boundary(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["committed_effect"]["partial_requires_committed_boundary"] = False
        self.assert_rejected(
            contract, "committed_effect.partial_requires_committed_boundary must be true"
        )

    def test_unknown_effect_requires_reconciliation(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["committed_effect"]["unknown_requires_reconciliation_action"] = False
        self.assert_rejected(
            contract,
            "committed_effect.unknown_requires_reconciliation_action must be true",
        )

    def test_effect_receipt_is_required(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["committed_effect"][
            "effect_receipt_required_when_state_not_none"
        ] = False
        self.assert_rejected(
            contract,
            "committed_effect.effect_receipt_required_when_state_not_none must be true",
        )

    def test_duplicate_acknowledgement_must_bind_the_request_key(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["committed_effect"][
            "duplicate_requires_matching_request_idempotency_key"
        ] = False
        self.assert_rejected(
            contract,
            "committed_effect.duplicate_requires_matching_request_idempotency_key must be true",
        )

    def test_duplicate_acknowledgement_must_name_the_committing_operation(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["committed_effect"][
            "duplicate_requires_original_operation_identity"
        ] = False
        self.assert_rejected(
            contract,
            "committed_effect.duplicate_requires_original_operation_identity must be true",
        )

    def test_duplicate_cannot_be_inferred_from_an_absent_effect(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["committed_effect"]["duplicate_may_be_inferred_from_absent_effect"] = True
        self.assert_rejected(
            contract,
            "committed_effect.duplicate_may_be_inferred_from_absent_effect must be false",
        )

    def test_duplicate_state_must_stay_in_the_closed_effect_table(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["committed_effect"]["states"] = [
            "none",
            "committed",
            "partial",
            "unknown",
        ]
        self.assert_rejected(contract, "committed-effect states drifted")

    def test_already_cancelled_never_calls_provider(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["request_control_precedence"][
            "already_cancelled_before_dispatch"
        ] = "call_provider"
        self.assert_rejected(
            contract, "already-cancelled request must not call provider"
        )

    def test_expired_deadline_never_calls_provider(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["request_control_precedence"][
            "expired_deadline_before_dispatch"
        ] = "call_provider"
        self.assert_rejected(contract, "expired deadline must not call provider")

    def test_cancellation_cannot_be_timeout(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["request_control_precedence"][
            "cancellation_may_be_reported_as_timeout"
        ] = True
        self.assert_rejected(
            contract,
            "request_control_precedence.cancellation_may_be_reported_as_timeout must be false",
        )

    def test_request_control_cannot_be_success(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["request_control_precedence"][
            "request_control_may_be_reported_as_success"
        ] = True
        self.assert_rejected(
            contract,
            "request_control_precedence.request_control_may_be_reported_as_success must be false",
        )

    def test_mandatory_health_mapping_cannot_disappear(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["mandatory_operation_mapping"] = [
            row
            for row in contract["mandatory_operation_mapping"]
            if row["capability_id"] != "provider.health.v1"
        ]
        self.assert_rejected(
            contract, "mandatory operation map must exactly cover health, observe, recall"
        )

    def test_operation_failure_cannot_bypass_envelope(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["mandatory_operation_rules"][
            "operation_specific_failure_may_bypass_terminal_envelope"
        ] = True
        self.assert_rejected(
            contract,
            "operation-specific failure cannot bypass terminal envelope",
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
            "terminal schema root must be a strict object",
            schema,
        )

    def test_schema_requires_terminal_table(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["required"].remove("terminal_codes")
        self.assert_rejected(
            copy.deepcopy(self.contract),
            "terminal schema required fields must match contract",
            schema,
        )


if __name__ == "__main__":
    unittest.main()
