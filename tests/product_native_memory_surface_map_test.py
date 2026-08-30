#!/usr/bin/env python3
"""Contract tests for the TraceDecay Native memory production surface map."""

from __future__ import annotations

import copy
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
MAP = REPO / "product/architecture/native-memory-surface-map.json"
CHECKER = REPO / "scripts/product/check-native-memory-surface-map.py"


class NativeMemorySurfaceMapTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.document = json.loads(MAP.read_text(encoding="utf-8"))

    def run_checker(self, document: dict[str, Any] | None = None) -> subprocess.CompletedProcess[str]:
        if document is None:
            map_path = MAP
            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--map",
                    str(map_path),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

        with tempfile.TemporaryDirectory() as temp_dir:
            map_path = Path(temp_dir) / "map.json"
            map_path.write_text(
                json.dumps(document, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--map",
                    str(map_path),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

    def assert_rejected(self, document: dict[str, Any], marker: str) -> None:
        result = self.run_checker(document)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        receipt = json.loads(result.stdout)
        self.assertFalse(receipt["ok"])
        self.assertIn(marker, "\n".join(receipt["errors"]))

    def test_real_repository_map_is_valid(self) -> None:
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertTrue(receipt["ok"])
        self.assertEqual(receipt["bead_id"], "tdmem-0103")
        self.assertEqual(receipt["authorities"], 7)
        self.assertEqual(receipt["public_operations"], 13)
        self.assertEqual(receipt["internal_entry_points"], 11)
        self.assertEqual(receipt["derived_surfaces"], 6)
        self.assertEqual(receipt["provider_seams"], 6)

    def test_missing_public_operation_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        document["public_operations"] = [
            row for row in document["public_operations"] if row["id"] != "fact_store_add"
        ]
        self.assert_rejected(document, "public operations missing")

    def test_transport_surface_drift_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        operation = next(
            row for row in document["public_operations"] if row["id"] == "fact_store_search"
        )
        operation["surfaces"]["http"] = "/application/retained/search"
        self.assert_rejected(document, "surfaces do not match")

    def test_unknown_read_authority_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        operation = next(
            row for row in document["public_operations"] if row["id"] == "fact_store_related"
        )
        operation["read_authority"] = "provider_graph"
        self.assert_rejected(document, "unknown read authority")

    def test_non_rebuildable_projection_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        surface = next(
            row for row in document["derived_surfaces"] if row["id"] == "fhrr_vectors"
        )
        surface["rebuildable"] = False
        self.assert_rejected(document, "fhrr_vectors must be rebuildable")

    def test_observation_ingest_cannot_become_fact_writer(self) -> None:
        document = copy.deepcopy(self.document)
        entry = next(
            row
            for row in document["internal_entry_points"]
            if row["id"] == "host_observation_ingest"
        )
        entry["canonical_mutation"] = True
        self.assert_rejected(
            document,
            "host_observation_ingest must not mutate canonical explicit facts",
        )

    def test_seam_ranking_drift_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        seam = next(row for row in document["provider_seams"] if row["rank"] == 6)
        seam["rank"] = 5
        self.assert_rejected(document, "provider seam ranks must be exactly")

    def test_missing_source_path_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        document["authorities"][0]["source_paths"].append(
            "crates/does-not-exist/src/memory.rs"
        )
        self.assert_rejected(document, "referenced source path does not exist")


if __name__ == "__main__":
    unittest.main()
