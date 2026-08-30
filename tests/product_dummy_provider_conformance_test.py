#!/usr/bin/env python3
"""Mutation tests for the standalone M1 dummy-provider conformance proof."""

from __future__ import annotations

import copy
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
MANIFEST = REPO / "product/conformance/dummy-provider/conformance-manifest.json"
ISSUES = REPO / ".beads/issues.jsonl"
CHECKER = REPO / "scripts/product/check-dummy-provider-conformance.py"


class DummyProviderConformanceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))

    def run_checker(
        self, manifest: dict[str, Any] | None = None
    ) -> subprocess.CompletedProcess[str]:
        if manifest is None:
            manifest_path = MANIFEST
            temporary = None
        else:
            temporary = tempfile.TemporaryDirectory()
            manifest_path = Path(temporary.name) / "conformance-manifest.json"
            manifest_path.write_text(
                json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        try:
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
        finally:
            if temporary is not None:
                temporary.cleanup()

    def assert_rejected(self, manifest: dict[str, Any], marker: str) -> None:
        result = self.run_checker(manifest)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertFalse(receipt["ok"])
        self.assertIn(marker, "\n".join(receipt["errors"]))

    def test_real_repository_conformance_is_valid(self) -> None:
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertTrue(receipt["ok"])
        self.assertEqual(
            receipt["manifest_id"],
            "tracedecay.memory.provider.dummy-conformance.v1",
        )
        self.assertEqual(receipt["bead_id"], "tdmem-0209")
        self.assertEqual(receipt["status"], "accepted")
        self.assertEqual(receipt["provider_id"], "test.dummy")
        self.assertEqual(receipt["mandatory_capabilities"], 3)
        self.assertEqual(receipt["implemented_optional_capabilities"], 2)
        self.assertEqual(receipt["explicitly_unsupported_optional_capabilities"], 10)
        self.assertEqual(receipt["required_test_cases"], 23)
        self.assertEqual(receipt["rust_version"], "1.97.1")

    def test_candidate_status_is_rejected(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["status"] = "accepted_candidate"
        self.assert_rejected(manifest, "dummy conformance status must be accepted")

    def test_rust_version_drift_is_rejected(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["implementation"]["minimum_rust_version"] = "1.85"
        self.assert_rejected(manifest, "implementation.minimum_rust_version")

    def test_missing_mandatory_capability_is_rejected(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["capabilities"]["mandatory"].remove("provider.health.v1")
        self.assert_rejected(
            manifest,
            "dummy mandatory capabilities must be health, observation, and recall",
        )

    def test_snapshot_capability_partition_drift_is_rejected(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["capabilities"]["implemented_optional"].remove(
            "snapshot.restore.v1"
        )
        self.assert_rejected(
            manifest,
            "dummy implemented optional capabilities must be snapshot export/restore",
        )

    def test_optional_capability_partition_must_match_registry(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["capabilities"]["explicitly_unsupported_optional"].remove(
            "recall.associative.v1"
        )
        self.assert_rejected(
            manifest,
            "dummy optional capability partition does not match registry authority",
        )

    def test_capability_classes_must_be_disjoint(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["capabilities"]["explicitly_unsupported_optional"].append(
            "snapshot.export.v1"
        )
        self.assert_rejected(manifest, "dummy capability classes must be disjoint")

    def test_silent_fallback_is_rejected(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["capabilities"]["silent_fallback"] = True
        self.assert_rejected(manifest, "dummy provider silent fallback must be false")

    def test_missing_source_path_is_rejected(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["source_paths"].append(
            "product/conformance/dummy-provider/src/does-not-exist.rs"
        )
        self.assert_rejected(manifest, "dummy source path is missing or unsafe")

    def test_required_journey_cannot_be_removed(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["required_test_cases"].remove(
            "duplicate_observation_is_idempotent"
        )
        self.assert_rejected(
            manifest,
            "required_test_cases must exactly cover the 23 mandatory journeys",
        )

    def test_observer_authority_cannot_be_widened(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["authority_and_isolation"][
            "provider_may_mutate_tracedecay_authority"
        ] = True
        self.assert_rejected(
            manifest,
            "authority_and_isolation.provider_may_mutate_tracedecay_authority must be False",
        )

    def test_provider_database_access_cannot_be_enabled(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["authority_and_isolation"][
            "provider_may_access_tracedecay_database"
        ] = True
        self.assert_rejected(
            manifest,
            "authority_and_isolation.provider_may_access_tracedecay_database must be False",
        )

    def test_snapshot_determinism_cannot_be_weakened(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["state_model"]["snapshot_deterministic"] = False
        self.assert_rejected(manifest, "state_model.snapshot_deterministic must be True")

    def test_implicit_reset_cannot_be_enabled(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["state_model"]["implicit_reset"] = True
        self.assert_rejected(manifest, "state_model.implicit_reset must be False")

    def test_missing_checker_is_rejected(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["verification"]["checker"] = "scripts/product/missing.py"
        self.assert_rejected(manifest, "verification.checker is missing")

    def test_unknown_bead_id_is_rejected(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["bead_id"] = "tdmem-9999"
        self.assert_rejected(manifest, "dummy conformance bead_id must be tdmem-0209")


if __name__ == "__main__":
    unittest.main()
