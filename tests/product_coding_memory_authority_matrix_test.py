#!/usr/bin/env python3
"""Contract tests for the coding-memory authority matrix."""

from __future__ import annotations

import copy
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
MATRIX = REPO / "product/architecture/coding-memory-authority-matrix.json"
CHECKER = REPO / "scripts/product/check-coding-memory-authority-matrix.py"


class CodingMemoryAuthorityMatrixTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.document = json.loads(MATRIX.read_text(encoding="utf-8"))

    def run_checker(
        self,
        document: dict[str, Any] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        if document is None:
            matrix_path = MATRIX
            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--matrix",
                    str(matrix_path),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

        with tempfile.TemporaryDirectory() as temp_dir:
            matrix_path = Path(temp_dir) / "matrix.json"
            matrix_path.write_text(
                json.dumps(document, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--matrix",
                    str(matrix_path),
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

    def domain(self, document: dict[str, Any], domain_id: str) -> dict[str, Any]:
        return next(row for row in document["authority_domains"] if row["id"] == domain_id)

    def test_real_repository_matrix_is_valid(self) -> None:
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertTrue(receipt["ok"])
        self.assertEqual(receipt["bead_id"], "tdmem-0104")
        self.assertEqual(receipt["namespace_axes"], 10)
        self.assertEqual(receipt["authority_domains"], 11)
        self.assertEqual(receipt["durable_domains"], 9)
        self.assertEqual(receipt["cross_domain_rules"], 9)
        self.assertEqual(receipt["context_lanes"], 5)

    def test_durable_domain_without_one_writer_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        self.domain(document, "explicit_facts")["canonical_writer"] = None
        self.assert_rejected(document, "must name exactly one canonical writer")

    def test_plural_canonical_writers_are_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        self.domain(document, "session_evidence")["co_writers"] = [
            "TraceDecay",
            "provider",
        ]
        self.assert_rejected(document, "must not define plural or alternate canonical writers")

    def test_native_fact_authority_drift_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        explicit = self.domain(document, "explicit_facts")
        explicit["native_surface_authority"] = "provider_fact_log"
        explicit["canonical_writer"] = "Selected provider fact store"
        self.assert_rejected(document, "explicit_facts must map to native_explicit_fact_log")

    def test_provider_recall_must_remain_advisory(self) -> None:
        document = copy.deepcopy(self.document)
        recall = self.domain(document, "provider_recall_candidates")
        recall["authority_class"] = "canonical"
        recall["provider_semantics"] = "authoritative"
        recall["canonical_writer"] = "provider"
        self.assert_rejected(document, "provider recall must be explicitly advisory_only")

    def test_provider_recall_cannot_gain_source_edit_effect(self) -> None:
        document = copy.deepcopy(self.document)
        recall = self.domain(document, "provider_recall_candidates")
        recall["prohibited_side_effects"] = [
            value
            for value in recall["prohibited_side_effects"]
            if value != "direct source edit"
        ]
        self.assert_rejected(document, "provider recall must prohibit 'direct source edit'")

    def test_final_context_owner_drift_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        self.domain(document, "final_compiled_context")["owner"] = "provider"
        self.assert_rejected(
            document,
            "TraceDecay context compiler must solely own final context assembly",
        )

    def test_missing_provider_namespace_axis_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        document["namespace_axes"] = [
            row for row in document["namespace_axes"] if row["id"] != "provider_id"
        ]
        self.assert_rejected(document, "namespace axes missing")

    def test_namespace_overlap_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        variant = self.domain(document, "worktree_identity")["namespace_variants"][0]
        variant["optional"].append("worktree_id")
        self.assert_rejected(document, "places axes in both required and optional")

    def test_context_lane_precedence_drift_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        document["context_lane_order"][0]["domain"] = "provider_recall_candidates"
        document["context_lane_order"][4]["domain"] = "current_code_truth"
        self.assert_rejected(document, "context lane precedence must be")

    def test_silent_fallback_rule_cannot_be_weakened(self) -> None:
        document = copy.deepcopy(self.document)
        rule = next(
            row for row in document["cross_domain_rules"] if row["id"] == "no_silent_fallback"
        )
        rule["rule"] = "The runtime may choose any available provider."
        self.assert_rejected(
            document,
            "no_silent_fallback rule must reject implicit provider switching",
        )

    def test_missing_current_source_path_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        self.domain(document, "current_code_truth")["source_paths"].append(
            "crates/does-not-exist/src/source.rs"
        )
        self.assert_rejected(document, "references a missing repository path")


if __name__ == "__main__":
    unittest.main()
