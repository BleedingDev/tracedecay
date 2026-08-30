#!/usr/bin/env python3
"""Contract tests for memory-provider identity and capability resolution V1."""

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
    / "product/contracts/memory-provider-v1/provider-registry-contract.json"
)
SCHEMA = (
    REPO
    / "product/contracts/memory-provider-v1/provider-registry-contract.schema.json"
)
README = REPO / "product/contracts/memory-provider-v1/README.md"
ISSUES = REPO / ".beads/issues.jsonl"
CHECKER = REPO / "scripts/product/check-provider-registry-contract.py"


class ProviderRegistryContractTest(unittest.TestCase):
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
            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--contract",
                    str(CONTRACT),
                    "--schema",
                    str(SCHEMA),
                    "--readme",
                    str(README),
                    "--issues",
                    str(ISSUES),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

        with tempfile.TemporaryDirectory() as temp_dir:
            contract_path = Path(temp_dir) / "provider-registry-contract.json"
            schema_path = Path(temp_dir) / "provider-registry-contract.schema.json"
            contract_path.write_text(
                json.dumps(contract or self.contract, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            schema_path.write_text(
                json.dumps(schema or self.schema, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
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
                    "--readme",
                    str(README),
                    "--issues",
                    str(ISSUES),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

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

    def capability(
        self, contract: dict[str, Any], capability_id: str
    ) -> dict[str, Any]:
        return next(
            row for row in contract["capability_catalog"] if row["id"] == capability_id
        )

    def slot(self, contract: dict[str, Any], provider_id: str) -> dict[str, Any]:
        return next(
            row for row in contract["bootstrap_slots"] if row["provider_id"] == provider_id
        )

    def test_real_repository_contract_is_valid(self) -> None:
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertTrue(receipt["ok"])
        self.assertEqual(
            receipt["contract_id"], "tracedecay.memory.provider.registry.v1"
        )
        self.assertEqual(receipt["bead_id"], "tdmem-0201")
        self.assertEqual(receipt["status"], "accepted")
        self.assertEqual(receipt["capability_count"], 9)
        self.assertEqual(
            receipt["bootstrap_provider_ids"],
            ["ncm", "ocean", "tracedecay.native"],
        )
        self.assertFalse(receipt["silent_fallback"])
        self.assertEqual(receipt["ncm_topology"], "deferred")
        self.assertFalse(receipt["ocean_counts_as_implemented"])

    def test_duplicate_capability_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["capability_catalog"].append(
            copy.deepcopy(contract["capability_catalog"][0])
        )
        self.assert_rejected(
            contract,
            "duplicate capability_catalog id observation.accept.v1",
        )

    def test_unversioned_capability_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        self.capability(contract, "recall.query.v1")["id"] = "recall.query"
        self.assert_rejected(contract, "capability ID is non-canonical: recall.query")

    def test_capability_cannot_mutate_tracedecay_authority(self) -> None:
        contract = copy.deepcopy(self.contract)
        self.capability(contract, "feedback.record.v1")[
            "may_mutate_tracedecay_authority"
        ] = True
        self.assert_rejected(
            contract,
            "capability feedback.record.v1 must not mutate TraceDecay authority",
        )

    def test_capability_cannot_stop_being_advisory(self) -> None:
        contract = copy.deepcopy(self.contract)
        self.capability(contract, "recall.query.v1")["advisory_only"] = False
        self.assert_rejected(
            contract,
            "capability recall.query.v1 must remain advisory_only",
        )

    def test_provider_self_registration_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["registration_contract"]["provider_self_registration"] = True
        self.assert_rejected(contract, "provider self-registration must be false")

    def test_public_provider_name_branching_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["registration_contract"]["public_surface_provider_branching"] = True
        self.assert_rejected(
            contract,
            "public-surface provider branching must be false",
        )

    def test_selection_must_carry_deadline_and_cancellation(self) -> None:
        contract = copy.deepcopy(self.contract)
        fields = contract["selection_contract"]["required_request_fields"]
        fields.remove("deadline")
        fields.remove("cancellation")
        self.assert_rejected(
            contract,
            "selection request must carry provider, capabilities, exact scope, revision, identity, deadline, and cancellation",
        )

    def test_silent_fallback_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        selection = contract["selection_contract"]
        selection["silent_fallback"] = True
        selection["fallback_provider"] = "tracedecay.native"
        self.assert_rejected(contract, "silent provider fallback must be false")

    def test_successful_empty_resolution_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["selection_contract"]["successful_empty_resolution"] = True
        self.assert_rejected(
            contract,
            "successful empty provider resolution must be false",
        )

    def test_resolution_outcome_cannot_hide_unknown_provider(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["selection_contract"]["resolution_states"].remove(
            "provider_unknown"
        )
        self.assert_rejected(
            contract,
            "selection resolution states must exactly cover the V1 fail-closed outcomes",
        )

    def test_duplicate_bootstrap_provider_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["bootstrap_slots"].append(
            copy.deepcopy(self.slot(contract, "tracedecay.native"))
        )
        self.assert_rejected(
            contract,
            "duplicate bootstrap_slots provider_id tracedecay.native",
        )

    def test_ncm_topology_cannot_be_preselected(self) -> None:
        contract = copy.deepcopy(self.contract)
        self.slot(contract, "ncm")["execution_topology"] = "in_process"
        self.assert_rejected(contract, "bootstrap_slot[ncm] fields drifted")

    def test_ncm_audit_must_precede_topology_and_observer(self) -> None:
        contract = copy.deepcopy(self.contract)
        self.slot(contract, "ncm")["implementation_gate_beads"] = [
            "tdmem-0702",
            "tdmem-0701",
            "tdmem-0703",
        ]
        self.assert_rejected(contract, "bootstrap slot ncm implementation gates drifted")

    def test_ocean_cannot_gain_speculative_implementation(self) -> None:
        contract = copy.deepcopy(self.contract)
        ocean = self.slot(contract, "ocean")
        ocean["implementation_gate_beads"] = ["tdmem-0703"]
        ocean["counts_as_implemented"] = True
        self.assert_rejected(contract, "bootstrap slot ocean implementation gates drifted")

    def test_native_slot_does_not_claim_implementation_before_parity(self) -> None:
        contract = copy.deepcopy(self.contract)
        self.slot(contract, "tracedecay.native")["counts_as_implemented"] = True
        self.assert_rejected(
            contract,
            "bootstrap slot tracedecay.native must not count as implemented",
        )

    def test_unknown_verification_bead_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["verification_beads"].append("tdmem-9999")
        self.assert_rejected(
            contract,
            "verification_beads references unknown Beads issue tdmem-9999",
        )

    def test_schema_cannot_allow_unknown_root_fields(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["additionalProperties"] = True
        self.assert_rejected(
            copy.deepcopy(self.contract),
            "provider registry schema root must deny additional properties",
            schema,
        )

    def test_schema_cannot_drop_provider_identity(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["required"].remove("provider_identity")
        self.assert_rejected(
            copy.deepcopy(self.contract),
            "provider registry schema required fields must match the contract",
            schema,
        )


if __name__ == "__main__":
    unittest.main()
