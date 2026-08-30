#!/usr/bin/env python3
"""Contract tests for the M0 pluggable-memory GO/NO-GO decision."""

from __future__ import annotations

import copy
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
DECISION = REPO / "product/architecture/m0-go-no-go.json"
REPORT = REPO / "product/architecture/m0-go-no-go.md"
ISSUES = REPO / ".beads/issues.jsonl"
CHECKER = REPO / "scripts/product/check-m0-go-no-go.py"


class M0GoNoGoTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.document = json.loads(DECISION.read_text(encoding="utf-8"))

    def run_checker(
        self, document: dict[str, Any] | None = None
    ) -> subprocess.CompletedProcess[str]:
        if document is None:
            decision_path = DECISION
            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--decision",
                    str(decision_path),
                    "--report",
                    str(REPORT),
                    "--issues",
                    str(ISSUES),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

        with tempfile.TemporaryDirectory() as temp_dir:
            decision_path = Path(temp_dir) / "m0-go-no-go.json"
            decision_path.write_text(
                json.dumps(document, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--decision",
                    str(decision_path),
                    "--report",
                    str(REPORT),
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

    def row(self, document: dict[str, Any], field: str, row_id: str) -> dict[str, Any]:
        return next(row for row in document[field] if row["id"] == row_id)

    def stage(self, document: dict[str, Any], milestone: str) -> dict[str, Any]:
        return next(
            row for row in document["implementation_order"] if row["milestone"] == milestone
        )

    def test_real_repository_decision_is_valid(self) -> None:
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertTrue(receipt["ok"])
        self.assertEqual(receipt["bead_id"], "tdmem-0107")
        self.assertEqual(receipt["verdict"], "go")
        self.assertEqual(receipt["next_executable_bead"], "tdmem-0201")
        self.assertEqual(receipt["evidence_count"], 7)
        self.assertEqual(receipt["risk_count"], 7)
        self.assertGreaterEqual(receipt["implementation_stage_count"], 6)
        self.assertEqual(receipt["hard_gate_count"], 6)
        self.assertEqual(receipt["ncm_topology_state"], "deferred")
        self.assertEqual(receipt["ocean_state"], "deferred")

    def test_no_verdict_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        document["verdict"] = "no_go"
        self.assert_rejected(document, "M0 verdict must be go")

    def test_next_bead_is_locked(self) -> None:
        document = copy.deepcopy(self.document)
        document["next_executable_bead"] = "tdmem-0301"
        self.assert_rejected(document, "next_executable_bead must be tdmem-0201")

    def test_false_go_condition_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        document["conditions"]["provider_recall_is_advisory"] = False
        self.assert_rejected(
            document,
            "condition provider_recall_is_advisory must be true",
        )

    def test_missing_m0_evidence_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        document["evidence"] = [
            row for row in document["evidence"] if row["id"] != "authority_matrix"
        ]
        self.assert_rejected(
            document,
            "evidence must exactly include the seven accepted M0 authorities",
        )

    def test_required_risk_cannot_be_removed(self) -> None:
        document = copy.deepcopy(self.document)
        document["residual_risks"] = [
            row
            for row in document["residual_risks"]
            if row["id"] != "observer_influence"
        ]
        self.assert_rejected(
            document,
            "residual_risks must exactly cover the seven M0 risk classes",
        )

    def test_unknown_blocking_bead_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        self.row(document, "residual_risks", "native_parity_drift")[
            "blocking_beads"
        ].append("tdmem-9999")
        self.assert_rejected(document, "references unknown Beads issue tdmem-9999")

    def test_ncm_topology_cannot_be_selected_in_m0(self) -> None:
        document = copy.deepcopy(self.document)
        self.row(document, "deferred_decisions", "ncm_execution_topology")[
            "state"
        ] = "selected"
        self.assert_rejected(
            document,
            "deferred decision ncm_execution_topology.state must be deferred",
        )

    def test_ncm_gate_order_is_fixed(self) -> None:
        document = copy.deepcopy(self.document)
        self.row(document, "deferred_decisions", "ncm_execution_topology")[
            "decision_gate"
        ] = ["tdmem-0702", "tdmem-0701"]
        self.assert_rejected(
            document,
            "NCM topology decision gate must be tdmem-0701 then tdmem-0702",
        )

    def test_ocean_cannot_gain_a_speculative_gate(self) -> None:
        document = copy.deepcopy(self.document)
        self.row(document, "deferred_decisions", "ocean_implementation")[
            "decision_gate"
        ] = ["tdmem-0702"]
        self.assert_rejected(
            document,
            "OCEAN must have no speculative implementation decision gate",
        )

    def test_implementation_train_must_start_at_m1_contracts(self) -> None:
        document = copy.deepcopy(self.document)
        first = document["implementation_order"][0]
        first["entry_bead"] = "tdmem-0301"
        first["required_before_next"].append("tdmem-0301")
        self.assert_rejected(
            document,
            "implementation order must begin with M1 / tdmem-0201",
        )

    def test_m1_contract_gate_cannot_be_skipped(self) -> None:
        document = copy.deepcopy(self.document)
        first = document["implementation_order"][0]
        first["required_before_next"].remove("tdmem-0204")
        self.assert_rejected(document, "M1 gate is missing required contract bead tdmem-0204")

    def test_m6_must_audit_before_topology_selection(self) -> None:
        document = copy.deepcopy(self.document)
        m6 = self.stage(document, "M6")
        m6["required_before_next"][0:2] = ["tdmem-0702", "tdmem-0701"]
        self.assert_rejected(document, "M6 must audit NCM before selecting its topology")

    def test_hard_gate_cannot_be_removed(self) -> None:
        document = copy.deepcopy(self.document)
        document["hard_gates"] = [
            row
            for row in document["hard_gates"]
            if row["id"] != "no_observer_influence"
        ]
        self.assert_rejected(
            document,
            "hard_gates must exactly include the six M0 implementation gates",
        )

    def test_no_go_triggers_cannot_be_collapsed(self) -> None:
        document = copy.deepcopy(self.document)
        document["no_go_triggers"] = document["no_go_triggers"][:2]
        self.assert_rejected(
            document,
            "no_go_triggers must name at least seven stop conditions",
        )


if __name__ == "__main__":
    unittest.main()
