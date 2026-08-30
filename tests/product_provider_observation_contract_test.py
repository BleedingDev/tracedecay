#!/usr/bin/env python3
"""Contract tests for provider observation normalization and idempotency."""

from __future__ import annotations

import copy
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
CONTRACT = REPO / "product/contracts/memory-provider-v1/provider-observation-contract.json"
SCHEMA = REPO / "product/contracts/memory-provider-v1/provider-observation-contract.schema.json"
DOC = REPO / "product/contracts/memory-provider-v1/provider-observation-contract.md"
ISSUES = REPO / ".beads/issues.jsonl"
CHECKER = REPO / "scripts/product/check-provider-observation-contract.py"


class ProviderObservationContractTest(unittest.TestCase):
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

    def kind(self, contract: dict[str, Any], kind_id: str) -> dict[str, Any]:
        return next(row for row in contract["observation_kinds"] if row["id"] == kind_id)

    def test_real_repository_contract_is_valid(self) -> None:
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertTrue(receipt["ok"])
        self.assertEqual(
            receipt["contract_id"], "tracedecay.memory.provider.observation.v1"
        )
        self.assertEqual(receipt["bead_id"], "tdmem-0203")
        self.assertEqual(receipt["observation_kind_count"], 9)
        self.assertEqual(receipt["acceptance_outcome_count"], 13)
        self.assertEqual(receipt["delivery_semantics"], "at_least_once_idempotent")
        self.assertTrue(receipt["canonical_source_settlement_precedes_observation"])
        self.assertFalse(receipt["silent_success_without_acknowledgement"])

    def test_canonical_settlement_receipt_is_required(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["source_identity"]["canonical_settlement_receipt_required"] = False
        self.assert_rejected(contract, "canonical settlement receipt must be required")

    def test_unsettled_source_cannot_be_accepted(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["source_identity"]["unsettled_source_policy"] = "accept_pending"
        self.assert_rejected(contract, "unsettled source events must be rejected")

    def test_path_cannot_be_source_identity(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["source_identity"]["path_is_source_identity"] = True
        self.assert_rejected(contract, "path must not be source identity")

    def test_unknown_observation_kind_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        self.kind(contract, "git.evidence_observed.v1")["id"] = "provider.custom.v1"
        self.assert_rejected(
            contract,
            "observation kinds must exactly contain the nine V1 coding-agent events",
        )

    def test_observation_kind_cannot_change_source_authority(self) -> None:
        contract = copy.deepcopy(self.contract)
        self.kind(contract, "source.edit_settled.v1")["source_authority"] = "host_session"
        self.assert_rejected(
            contract,
            "observation kind source.edit_settled.v1 authority or payload contract drifted",
        )

    def test_floating_point_payloads_are_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["normalization"]["floating_point_values_allowed"] = True
        self.assert_rejected(
            contract,
            "normalization.floating_point_values_allowed must be false",
        )

    def test_duplicate_json_keys_are_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["normalization"]["duplicate_object_keys_allowed"] = True
        self.assert_rejected(
            contract,
            "normalization.duplicate_object_keys_allowed must be false",
        )

    def test_transport_metadata_cannot_change_digest(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["normalization"]["transport_metadata_in_digest"] = True
        self.assert_rejected(
            contract,
            "normalization.transport_metadata_in_digest must be false",
        )

    def test_retry_key_cannot_be_random(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["idempotency"]["random_retry_key_allowed"] = True
        self.assert_rejected(contract, "idempotency.random_retry_key_allowed must be false")

    def test_timestamp_only_idempotency_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["idempotency"]["timestamp_only_key_allowed"] = True
        self.assert_rejected(
            contract,
            "idempotency.timestamp_only_key_allowed must be false",
        )

    def test_same_key_same_payload_must_be_duplicate(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["idempotency"]["same_key_same_payload_outcome"] = "applied_again"
        self.assert_rejected(
            contract,
            "same key/same payload must acknowledge duplicate",
        )

    def test_same_key_different_payload_must_conflict(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["idempotency"]["same_key_different_payload_outcome"] = "overwrite"
        self.assert_rejected(
            contract,
            "same key/different payload must be idempotency conflict",
        )

    def test_provider_must_persist_deduplication(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["idempotency"]["provider_must_persist_deduplication"] = False
        self.assert_rejected(
            contract,
            "idempotency.provider_must_persist_deduplication must be true",
        )

    def test_provider_cannot_rewrite_provenance(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["provenance"]["provider_may_rewrite_origin"] = True
        self.assert_rejected(
            contract,
            "provider cannot rewrite origin or drop transform chain",
        )

    def test_raw_secrets_are_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["privacy"]["raw_secret_material_allowed"] = True
        self.assert_rejected(contract, "privacy.raw_secret_material_allowed must be false")

    def test_provider_cannot_extend_expiry(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["privacy"]["provider_may_extend_expiry"] = True
        self.assert_rejected(contract, "privacy.provider_may_extend_expiry must be false")

    def test_delivery_order_cannot_be_assumed(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["ordering"]["delivery_order_guaranteed"] = True
        self.assert_rejected(contract, "ordering.delivery_order_guaranteed must be false")

    def test_provider_must_tolerate_duplicates(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["ordering"]["provider_must_tolerate_duplicate_delivery"] = False
        self.assert_rejected(
            contract,
            "ordering.provider_must_tolerate_duplicate_delivery must be true",
        )

    def test_batch_cannot_assume_atomicity(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["batch_contract"]["atomic_batch_required"] = True
        self.assert_rejected(
            contract,
            "batch contract must report partial commit and not assume atomicity",
        )

    def test_partial_batch_commit_must_be_reported(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["batch_contract"]["partial_batch_commit_must_be_reported"] = False
        self.assert_rejected(
            contract,
            "batch contract must report partial commit and not assume atomicity",
        )

    def test_unknown_effect_outcome_cannot_be_hidden(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["provider_acceptance_outcomes"].remove("effect_unknown")
        self.assert_rejected(
            contract,
            "provider acceptance outcomes must exactly cover V1 receipt states",
        )

    def test_success_without_provider_ack_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["delivery_receipt"][
            "success_without_provider_acknowledgement_allowed"
        ] = True
        self.assert_rejected(
            contract,
            "success without provider acknowledgement must be false",
        )

    def test_observer_failure_cannot_change_source_outcome(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["observer_non_interference"][
            "provider_failure_may_change_source_outcome"
        ] = True
        self.assert_rejected(
            contract,
            "observer_non_interference.provider_failure_may_change_source_outcome must be false",
        )

    def test_observer_output_cannot_enter_context(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["observer_non_interference"][
            "provider_output_may_enter_context_in_observer_mode"
        ] = True
        self.assert_rejected(
            contract,
            "observer_non_interference.provider_output_may_enter_context_in_observer_mode must be false",
        )

    def test_unknown_verification_bead_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["verification_beads"].append("tdmem-9999")
        self.assert_rejected(
            contract,
            "verification_beads references unknown Beads issue tdmem-9999",
        )

    def test_schema_root_cannot_allow_unknown_fields(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["additionalProperties"] = True
        self.assert_rejected(
            copy.deepcopy(self.contract),
            "observation schema root must be a strict object",
            schema,
        )


if __name__ == "__main__":
    unittest.main()
