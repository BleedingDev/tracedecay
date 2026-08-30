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
CONTRACT = (
    REPO
    / "product/contracts/memory-provider-v1/provider-observation-contract.json"
)
SCHEMA = (
    REPO
    / "product/contracts/memory-provider-v1/provider-observation-contract.schema.json"
)
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
        return next(
            row for row in contract["observation_kinds"] if row["id"] == kind_id
        )

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
        self.assertEqual(receipt["acceptance_outcome_count"], 14)
        self.assertEqual(
            receipt["extension_policy"], "preserve_opaque_inert_round_trip"
        )
        self.assertFalse(receipt["observation_is_memory_record"])
        self.assertFalse(receipt["stable_memory_ref_required"])
        self.assertEqual(
            receipt["delivery_semantics"], "at_least_once_idempotent"
        )
        self.assertTrue(
            receipt["canonical_source_settlement_precedes_observation"]
        )
        self.assertFalse(receipt["silent_success_without_acknowledgement"])

    def test_exact_scope_field_is_required_in_envelope(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["observation_envelope"]["required_fields"].remove(
            "exact_scope_identity"
        )
        self.assert_rejected(
            contract,
            "observation envelope required fields must remain canonical and ordered",
        )

    def test_extension_field_is_required_in_envelope(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["observation_envelope"]["required_fields"].remove("extensions")
        self.assert_rejected(
            contract,
            "observation envelope required fields must remain canonical and ordered",
        )

    def test_canonical_settlement_receipt_is_required(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["source_identity"]["canonical_settlement_receipt_required"] = False
        self.assert_rejected(
            contract, "canonical settlement receipt must be required"
        )

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
        self.kind(contract, "source.edit_settled.v1")[
            "source_authority"
        ] = "host_session"
        self.assert_rejected(
            contract,
            "observation kind source.edit_settled.v1 authority or payload contract drifted",
        )

    def test_unknown_optional_extension_must_round_trip_inertly(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["extension_contract"][
            "unknown_optional_extension_policy"
        ] = "drop_unknown"
        self.assert_rejected(
            contract, "unknown optional extensions must round-trip inertly"
        )

    def test_unknown_required_extension_must_fail_explicitly(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["extension_contract"][
            "unknown_required_extension_policy"
        ] = "ignore"
        self.assert_rejected(
            contract, "unknown required extensions must fail explicitly"
        )

    def test_unknown_extension_cannot_activate_behavior(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["extension_contract"][
            "unknown_extension_may_activate_behavior"
        ] = True
        self.assert_rejected(
            contract,
            "extension_contract.unknown_extension_may_activate_behavior must be false",
        )

    def test_preserved_extension_cannot_be_dropped(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["extension_contract"][
            "provider_may_drop_preserved_extension"
        ] = True
        self.assert_rejected(
            contract,
            "extension_contract.provider_may_drop_preserved_extension must be false",
        )

    def test_observation_cannot_become_memory_record(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["memory_effect_semantics"]["observation_is_memory_record"] = True
        self.assert_rejected(
            contract, "memory_effect_semantics.observation_is_memory_record must be false"
        )

    def test_observation_id_cannot_be_provider_memory_id(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["memory_effect_semantics"][
            "observation_id_is_provider_memory_id"
        ] = True
        self.assert_rejected(
            contract,
            "memory_effect_semantics.observation_id_is_provider_memory_id must be false",
        )

    def test_stable_provider_memory_id_must_remain_optional(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["memory_effect_semantics"][
            "stable_provider_memory_id_required"
        ] = True
        self.assert_rejected(
            contract,
            "memory_effect_semantics.stable_provider_memory_id_required must be false",
        )

    def test_effect_cardinality_must_support_latent_providers(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["memory_effect_semantics"]["provider_effect_cardinality"] = "one"
        self.assert_rejected(
            contract, "provider effect cardinality must allow zero, one, or many"
        )

    def test_native_promotion_must_remain_separate(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["memory_effect_semantics"][
            "native_fact_promotion_is_separate_authorized_operation"
        ] = False
        self.assert_rejected(
            contract,
            "memory_effect_semantics.native_fact_promotion_is_separate_authorized_operation must be true",
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

    def test_idempotency_must_include_payload_digest(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["idempotency"]["derivation"] = contract["idempotency"][
            "derivation"
        ].replace("_payload_sha256", "")
        self.assert_rejected(contract, "idempotency derivation is missing payload_sha256")

    def test_idempotency_must_include_extension_digest(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["idempotency"]["derivation"] = contract["idempotency"][
            "derivation"
        ].replace("_extensions_digest", "")
        self.assert_rejected(
            contract, "idempotency derivation is missing extensions_digest"
        )

    def test_retry_key_cannot_be_random(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["idempotency"]["random_retry_key_allowed"] = True
        self.assert_rejected(
            contract, "idempotency.random_retry_key_allowed must be false"
        )

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
        contract["idempotency"][
            "same_key_different_payload_outcome"
        ] = "overwrite"
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
        self.assert_rejected(contract, "provider cannot rewrite provenance origin")

    def test_raw_secrets_are_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["privacy"]["raw_secret_material_allowed"] = True
        self.assert_rejected(
            contract, "privacy.raw_secret_material_allowed must be false"
        )

    def test_provider_cannot_extend_expiry(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["privacy"]["provider_may_extend_expiry"] = True
        self.assert_rejected(
            contract, "privacy.provider_may_extend_expiry must be false"
        )

    def test_delivery_order_cannot_be_assumed(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["ordering"]["delivery_order_guaranteed"] = True
        self.assert_rejected(
            contract, "ordering.delivery_order_guaranteed must be false"
        )

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
        self.assert_rejected(contract, "batch contract must not assume atomicity")

    def test_partial_batch_commit_must_be_reported(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["batch_contract"]["partial_batch_commit_must_be_reported"] = False
        self.assert_rejected(contract, "partial batch commit must be reported")

    def test_unknown_effect_outcome_cannot_be_hidden(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["provider_acceptance_outcomes"].remove("effect_unknown")
        self.assert_rejected(
            contract,
            "provider acceptance outcomes must exactly cover V1 receipt states",
        )

    def test_required_extension_failure_cannot_be_hidden(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["provider_acceptance_outcomes"].remove(
            "rejected_extension_unsupported"
        )
        self.assert_rejected(
            contract,
            "provider acceptance outcomes must exactly cover V1 receipt states",
        )

    def test_stable_memory_refs_must_be_optional(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["delivery_receipt"]["stable_memory_refs_optional"] = False
        self.assert_rejected(
            contract, "stable memory references must remain optional"
        )

    def test_provider_effect_summary_is_required(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["delivery_receipt"]["required_fields"].remove(
            "provider_effect_summary"
        )
        self.assert_rejected(contract, "delivery receipt required fields drifted")

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

    def test_schema_must_include_extension_contract(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["required"].remove("extension_contract")
        self.assert_rejected(
            copy.deepcopy(self.contract),
            "observation schema required fields must match the contract",
            schema,
        )


if __name__ == "__main__":
    unittest.main()
