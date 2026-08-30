#!/usr/bin/env python3
"""Contract tests for the upstream patch-footprint policy and convergence map."""

from __future__ import annotations

import copy
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
POLICY = REPO / "product/upstream/patch-footprint-policy.json"
CONVERGENCE_MAP = REPO / "product/upstream/convergence-map.json"
CHECKER = REPO / "scripts/product/check-patch-footprint-policy.py"


class PatchFootprintPolicyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.policy = json.loads(POLICY.read_text(encoding="utf-8"))
        cls.convergence_map = json.loads(CONVERGENCE_MAP.read_text(encoding="utf-8"))

    def run_checker(
        self,
        policy: dict[str, Any] | None = None,
        convergence_map: dict[str, Any] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        if policy is None and convergence_map is None:
            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--policy",
                    str(POLICY),
                    "--map",
                    str(CONVERGENCE_MAP),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

        policy = copy.deepcopy(self.policy if policy is None else policy)
        convergence_map = copy.deepcopy(
            self.convergence_map if convergence_map is None else convergence_map
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            policy_path = temp / "policy.json"
            map_path = temp / "convergence-map.json"
            policy_path.write_text(
                json.dumps(policy, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            map_path.write_text(
                json.dumps(convergence_map, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--policy",
                    str(policy_path),
                    "--map",
                    str(map_path),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

    def assert_rejected(
        self,
        marker: str,
        *,
        policy: dict[str, Any] | None = None,
        convergence_map: dict[str, Any] | None = None,
    ) -> None:
        result = self.run_checker(policy, convergence_map)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertFalse(receipt["ok"])
        self.assertIn(marker, "\n".join(receipt["errors"]))

    def touch_point(self, policy: dict[str, Any], touch_id: str) -> dict[str, Any]:
        return next(row for row in policy["allowed_touch_points"] if row["id"] == touch_id)

    def test_real_repository_policy_is_valid_and_footprint_is_zero(self) -> None:
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertTrue(receipt["ok"])
        self.assertEqual(receipt["bead_id"], "tdmem-0105")
        self.assertEqual(receipt["policy_revision"], "patch-footprint.v1")
        self.assertEqual(receipt["allowed_touch_points"], 7)
        self.assertEqual(receipt["exception_zones"], 5)
        self.assertEqual(receipt["dependency_direction_rules"], 6)
        self.assertGreater(receipt["workspace_manifests_checked"], 30)
        self.assertEqual(
            receipt["footprint"],
            {
                "composition_root_files": 0,
                "exception_zone_files": 0,
                "total_upstream_changed_lines": 0,
                "upstream_existing_production_files": 0,
                "upstream_existing_test_or_fixture_files": 0,
            },
        )

    def test_budget_cannot_be_silently_loosened(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["initial_budget"]["max_total_upstream_changed_lines"] = 901
        self.assert_rejected(
            "initial_budget.max_total_upstream_changed_lines must be 900",
            policy=policy,
        )

    def test_broad_product_owned_pattern_is_rejected(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["product_owned_paths"].append("crates/**")
        self.assert_rejected(
            "product-owned paths must not hide upstream tree 'crates/**'",
            policy=policy,
        )

    def test_missing_allowed_touch_point_is_rejected(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["allowed_touch_points"] = [
            row
            for row in policy["allowed_touch_points"]
            if row["id"] != "recall_context_mount"
        ]
        self.assert_rejected("allowed touch points missing", policy=policy)

    def test_missing_dependency_direction_rule_is_rejected(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["dependency_direction_rules"] = [
            row
            for row in policy["dependency_direction_rules"]
            if row["id"] != "ncm_adapter_does_not_reach_native_store"
        ]
        self.assert_rejected("dependency direction rules missing", policy=policy)

    def test_upstream_floor_drift_is_rejected(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["upstream_floor"]["sha"] = "0" * 40
        self.assert_rejected("upstream floor must remain", policy=policy)

    def test_convergence_snapshot_must_match_actual_diff(self) -> None:
        convergence_map = copy.deepcopy(self.convergence_map)
        convergence_map["snapshot"]["upstream_existing_production_files"] = 1
        self.assert_rejected(
            "does not match actual 0",
            convergence_map=convergence_map,
        )

    def test_active_entry_without_actual_diff_is_rejected(self) -> None:
        convergence_map = copy.deepcopy(self.convergence_map)
        convergence_map["entries"] = [
            {
                "path": "Cargo.toml",
                "touch_point": "workspace_wiring",
                "rationale": "Register a future additive product crate.",
                "semantic_invariants": ["Upstream feature defaults remain unchanged."],
                "verification": ["cargo metadata --locked --format-version 1"],
                "bead_ids": ["tdmem-test"],
                "line_budget": 20,
                "rebase_or_removal_plan": "Remove the member when the product crate is removed.",
                "status": "active",
            }
        ]
        self.assert_rejected(
            "active convergence entry has no current upstream diff: Cargo.toml",
            convergence_map=convergence_map,
        )

    def test_exception_entry_requires_adr_and_exception_evidence(self) -> None:
        convergence_map = copy.deepcopy(self.convergence_map)
        convergence_map["entries"] = [
            {
                "path": "crates/tracedecay-store/src/lib.rs",
                "touch_point": "exception",
                "rationale": "Deliberately incomplete fixture.",
                "semantic_invariants": ["Native persistence remains authoritative."],
                "verification": ["cargo test -p tracedecay-store"],
                "bead_ids": ["tdmem-test"],
                "line_budget": 10,
                "rebase_or_removal_plan": "Remove before merge.",
                "status": "active",
            }
        ]
        self.assert_rejected(
            "must include exception evidence",
            convergence_map=convergence_map,
        )

    def test_declared_allowed_touch_path_must_exist(self) -> None:
        policy = copy.deepcopy(self.policy)
        touch = self.touch_point(policy, "application_contract_mount")
        touch["paths"].append("crates/does-not-exist/src/mount.rs")
        self.assert_rejected(
            "allowed touch point references missing path",
            policy=policy,
        )

    def test_duplicate_convergence_path_is_rejected(self) -> None:
        convergence_map = copy.deepcopy(self.convergence_map)
        entry = {
            "path": "Cargo.toml",
            "touch_point": "workspace_wiring",
            "rationale": "Duplicate fixture.",
            "semantic_invariants": ["Workspace semantics remain unchanged."],
            "verification": ["cargo metadata --locked --format-version 1"],
            "bead_ids": ["tdmem-test"],
            "line_budget": 10,
            "rebase_or_removal_plan": "Delete fixture.",
            "status": "retired",
        }
        convergence_map["entries"] = [entry, copy.deepcopy(entry)]
        self.assert_rejected(
            "duplicate convergence-map path 'Cargo.toml'",
            convergence_map=convergence_map,
        )

    def test_exception_zone_cannot_drop_adr_requirement(self) -> None:
        policy = copy.deepcopy(self.policy)
        zone = next(
            row
            for row in policy["exception_zones"]
            if row["id"] == "native_database_internals"
        )
        zone["required_exception_evidence"] = [
            value
            for value in zone["required_exception_evidence"]
            if "ADR" not in value
        ]
        self.assert_rejected("must require ADR evidence", policy=policy)


if __name__ == "__main__":
    unittest.main()
