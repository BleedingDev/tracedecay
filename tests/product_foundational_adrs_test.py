#!/usr/bin/env python3
"""Contract tests for the foundational pluggable-memory ADR set."""

from __future__ import annotations

import copy
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
MANIFEST = REPO / "product/architecture/adr/manifest.json"
ISSUES = REPO / ".beads/issues.jsonl"
CHECKER = REPO / "scripts/product/check-foundational-adrs.py"


class FoundationalAdrsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.document = json.loads(MANIFEST.read_text(encoding="utf-8"))

    def run_checker(
        self, document: dict[str, Any] | None = None
    ) -> subprocess.CompletedProcess[str]:
        if document is None:
            manifest_path = MANIFEST
            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--manifest",
                    str(manifest_path),
                    "--issues",
                    str(ISSUES),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "manifest.json"
            manifest_path.write_text(
                json.dumps(document, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--manifest",
                    str(manifest_path),
                    "--issues",
                    str(ISSUES),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

    def assert_rejected(self, document: dict[str, Any], marker: str) -> None:
        result = self.run_checker(document)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertFalse(receipt["ok"])
        self.assertIn(marker, "\n".join(receipt["errors"]))

    def decision(self, document: dict[str, Any], decision_id: str) -> dict[str, Any]:
        return next(row for row in document["decisions"] if row["id"] == decision_id)

    def test_real_repository_adr_set_is_valid(self) -> None:
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertTrue(receipt["ok"])
        self.assertEqual(receipt["bead_id"], "tdmem-0106")
        self.assertEqual(receipt["status"], "accepted")
        self.assertEqual(receipt["decision_count"], 8)
        self.assertGreaterEqual(receipt["verification_bead_count"], 30)
        self.assertEqual(receipt["ncm_topology_state"], "deferred")

    def test_missing_foundational_decision_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        document["decisions"] = [
            row for row in document["decisions"] if row["id"] != "ADR-0006"
        ]
        self.assert_rejected(document, "foundational ADRs missing")

    def test_duplicate_adr_id_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        document["decisions"][-1]["id"] = "ADR-0007"
        self.assert_rejected(document, "duplicate ADR id ADR-0007")

    def test_required_sections_cannot_be_weakened(self) -> None:
        document = copy.deepcopy(self.document)
        document["required_sections"].remove("Rejected alternatives")
        self.assert_rejected(document, "required_sections must be exactly")

    def test_unknown_verification_bead_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        self.decision(document, "ADR-0001")["verification_beads"].append(
            "tdmem-9999"
        )
        self.assert_rejected(document, "references unknown verification bead tdmem-9999")

    def test_direct_fact_store_rejection_is_mandatory(self) -> None:
        document = copy.deepcopy(self.document)
        decision = self.decision(document, "ADR-0001")
        decision["required_rejections"][0] = "Use a common database table"
        self.assert_rejected(
            document,
            "rejected-alternatives section is missing manifest rejection",
        )

    def test_provider_name_branching_rejection_is_mandatory(self) -> None:
        document = copy.deepcopy(self.document)
        decision = self.decision(document, "ADR-0003")
        decision["required_rejections"][1] = "Dispatch through transport names"
        self.assert_rejected(
            document,
            "rejected-alternatives section is missing manifest rejection",
        )

    def test_ncm_topology_cannot_be_preselected(self) -> None:
        document = copy.deepcopy(self.document)
        topology = self.decision(document, "ADR-0004")["ncm_topology"]
        topology["state"] = "selected"
        topology["selected_topology"] = "in_process_crate"
        self.assert_rejected(
            document,
            "NCM execution topology must remain deferred",
        )

    def test_ncm_gate_beads_are_fixed(self) -> None:
        document = copy.deepcopy(self.document)
        topology = self.decision(document, "ADR-0004")["ncm_topology"]
        topology["decision_gate_beads"] = ["tdmem-0702"]
        self.assert_rejected(
            document,
            "NCM topology decision gate must be tdmem-0701 then tdmem-0702",
        )

    def test_forward_adr_dependency_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        self.decision(document, "ADR-0002")["depends_on_adrs"].append("ADR-0008")
        self.assert_rejected(
            document,
            "must depend only on an earlier foundational ADR",
        )

    def test_missing_m0_source_authority_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        document["source_authorities"][-1] = "product/does-not-exist.json"
        self.assert_rejected(
            document,
            "source_authorities must exactly match the M0 evidence authorities",
        )

    def test_adr_status_drift_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        self.decision(document, "ADR-0007")["status"] = "draft"
        self.assert_rejected(document, "ADR-0007.status must be accepted")


if __name__ == "__main__":
    unittest.main()
