#!/usr/bin/env python3
"""Contract tests for the canonical Memory Provider V1 contract set and goldens."""

from __future__ import annotations

import copy
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
CONTRACT_SET = REPO / "product/contracts/memory-provider-v1/contract-set.json"
CONTRACT_SET_SCHEMA = (
    REPO / "product/contracts/memory-provider-v1/contract-set.schema.json"
)
SCENARIOS = REPO / "product/contracts/memory-provider-v1/golden-scenarios.json"
SCENARIO_SCHEMA = (
    REPO / "product/contracts/memory-provider-v1/golden-scenarios.schema.json"
)
GOLDENS = REPO / "product/contracts/memory-provider-v1/goldens"
CHECKER = REPO / "scripts/product/check-memory-provider-contract-set.py"
GENERATOR = REPO / "scripts/product/generate-memory-provider-goldens.py"
TEMP_ROOT = REPO / ".beads" / "contract-set-test-tmp"


class MemoryProviderContractSetTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.contract_set = json.loads(CONTRACT_SET.read_text(encoding="utf-8"))
        cls.contract_set_schema = json.loads(
            CONTRACT_SET_SCHEMA.read_text(encoding="utf-8")
        )
        cls.scenarios = json.loads(SCENARIOS.read_text(encoding="utf-8"))
        cls.scenario_schema = json.loads(
            SCENARIO_SCHEMA.read_text(encoding="utf-8")
        )
        TEMP_ROOT.mkdir(parents=True, exist_ok=True)

    @classmethod
    def tearDownClass(cls) -> None:
        if TEMP_ROOT.exists():
            shutil.rmtree(TEMP_ROOT)

    def run_checker(
        self,
        *,
        contract_set: dict[str, Any] | None = None,
        contract_set_schema: dict[str, Any] | None = None,
        scenarios: dict[str, Any] | None = None,
        scenario_schema: dict[str, Any] | None = None,
        mutate_goldens: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        if all(
            value is None
            for value in (
                contract_set,
                contract_set_schema,
                scenarios,
                scenario_schema,
                mutate_goldens,
            )
        ):
            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--contract-set",
                    str(CONTRACT_SET),
                    "--contract-set-schema",
                    str(CONTRACT_SET_SCHEMA),
                    "--scenarios",
                    str(SCENARIOS),
                    "--scenario-schema",
                    str(SCENARIO_SCHEMA),
                    "--goldens-dir",
                    str(GOLDENS),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

        with tempfile.TemporaryDirectory(dir=TEMP_ROOT) as temp_dir:
            root = Path(temp_dir)
            contract_set_path = root / "contract-set.json"
            contract_set_schema_path = root / "contract-set.schema.json"
            scenarios_path = root / "golden-scenarios.json"
            scenario_schema_path = root / "golden-scenarios.schema.json"
            goldens_path = root / "goldens"
            contract_set_path.write_text(
                json.dumps(
                    contract_set or self.contract_set,
                    indent=2,
                    sort_keys=True,
                    allow_nan=True,
                )
                + "\n",
                encoding="utf-8",
            )
            contract_set_schema_path.write_text(
                json.dumps(
                    contract_set_schema or self.contract_set_schema,
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            scenarios_path.write_text(
                json.dumps(
                    scenarios or self.scenarios,
                    indent=2,
                    sort_keys=True,
                    allow_nan=True,
                )
                + "\n",
                encoding="utf-8",
            )
            scenario_schema_path.write_text(
                json.dumps(
                    scenario_schema or self.scenario_schema,
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            shutil.copytree(GOLDENS, goldens_path)
            if mutate_goldens == "fixtures":
                fixtures = goldens_path / "fixtures.jsonl"
                fixtures.write_bytes(fixtures.read_bytes() + b"{}\n")
            elif mutate_goldens == "manifest":
                manifest = json.loads(
                    (goldens_path / "manifest.json").read_text(encoding="utf-8")
                )
                manifest["fixtures_sha256"] = "0" * 64
                (goldens_path / "manifest.json").write_text(
                    json.dumps(manifest, separators=(",", ":"), sort_keys=True)
                    + "\n",
                    encoding="utf-8",
                )

            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--contract-set",
                    str(contract_set_path),
                    "--contract-set-schema",
                    str(contract_set_schema_path),
                    "--scenarios",
                    str(scenarios_path),
                    "--scenario-schema",
                    str(scenario_schema_path),
                    "--goldens-dir",
                    str(goldens_path),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

    def assert_rejected(
        self,
        marker: str,
        **kwargs: Any,
    ) -> None:
        result = self.run_checker(**kwargs)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertFalse(receipt["ok"])
        self.assertIn(marker, "\n".join(receipt["errors"]))

    def fixture(self, scenarios: dict[str, Any], fixture_id: str) -> dict[str, Any]:
        return next(
            row for row in scenarios["fixtures"] if row["fixture_id"] == fixture_id
        )

    def test_real_repository_contract_set_and_goldens_are_valid(self) -> None:
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertTrue(receipt["ok"])
        self.assertEqual(
            receipt["contract_set_id"],
            "tracedecay.memory.provider.contract-set.v1",
        )
        self.assertEqual(receipt["bead_id"], "tdmem-0207")
        self.assertEqual(receipt["contract_count"], 6)
        self.assertEqual(receipt["fixture_count"], 25)
        self.assertEqual(receipt["category_count"], 14)
        self.assertEqual(receipt["compatibility_rule_count"], 8)
        self.assertRegex(receipt["fixtures_sha256"], r"^[0-9a-f]{64}$")
        self.assertRegex(receipt["generator_sha256"], r"^[0-9a-f]{64}$")

    def test_generator_check_passes_without_writing(self) -> None:
        before = {
            path.name: path.read_bytes()
            for path in sorted(GOLDENS.iterdir())
            if path.is_file()
        }
        result = subprocess.run(
            [
                "python3",
                str(GENERATOR),
                "--repo",
                str(REPO),
                "--check",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        after = {
            path.name: path.read_bytes()
            for path in sorted(GOLDENS.iterdir())
            if path.is_file()
        }
        self.assertEqual(before, after)

    def test_contract_cannot_be_removed(self) -> None:
        contract_set = copy.deepcopy(self.contract_set)
        contract_set["contracts"] = contract_set["contracts"][:-1]
        self.assert_rejected(
            "contract-set must contain exactly six contracts",
            contract_set=contract_set,
        )

    def test_contract_order_is_authoritative(self) -> None:
        contract_set = copy.deepcopy(self.contract_set)
        contract_set["contracts"][0], contract_set["contracts"][1] = (
            contract_set["contracts"][1],
            contract_set["contracts"][0],
        )
        self.assert_rejected(
            "contract-set order, IDs, or Beads drifted",
            contract_set=contract_set,
        )

    def test_contract_identity_drift_is_rejected(self) -> None:
        contract_set = copy.deepcopy(self.contract_set)
        contract_set["contracts"][0]["contract_id"] = (
            "tracedecay.memory.provider.registry.v2"
        )
        self.assert_rejected(
            "contract-set IDs do not match the six accepted M1 contracts",
            contract_set=contract_set,
        )

    def test_contract_set_schema_cannot_allow_unknown_fields(self) -> None:
        schema = copy.deepcopy(self.contract_set_schema)
        schema["additionalProperties"] = True
        self.assert_rejected(
            "contract-set schema root must be strict",
            contract_set_schema=schema,
        )

    def test_compatibility_rule_cannot_be_removed(self) -> None:
        contract_set = copy.deepcopy(self.contract_set)
        contract_set["compatibility_rules"] = contract_set[
            "compatibility_rules"
        ][1:]
        self.assert_rejected(
            "compatibility rules must exactly contain the eight V1 rules",
            contract_set=contract_set,
        )

    def test_required_fixture_cannot_be_removed(self) -> None:
        scenarios = copy.deepcopy(self.scenarios)
        scenarios["fixtures"] = [
            row
            for row in scenarios["fixtures"]
            if row["fixture_id"] != "terminal.effect-unknown"
        ]
        self.assert_rejected(
            "scenario-set missing required fixtures",
            scenarios=scenarios,
        )

    def test_required_category_cannot_disappear(self) -> None:
        scenarios = copy.deepcopy(self.scenarios)
        scenarios["fixtures"] = [
            row
            for row in scenarios["fixtures"]
            if row["category"] != "effect_unknown"
        ]
        self.assert_rejected(
            "scenario categories drifted",
            scenarios=scenarios,
        )

    def test_fixture_unknown_field_is_rejected(self) -> None:
        scenarios = copy.deepcopy(self.scenarios)
        self.fixture(scenarios, "recall.zero-results")["surprise"] = True
        self.assert_rejected(
            "fields drifted",
            scenarios=scenarios,
        )

    def test_fixture_unknown_contract_is_rejected(self) -> None:
        scenarios = copy.deepcopy(self.scenarios)
        self.fixture(scenarios, "recall.zero-results")["contract_id"] = (
            "tracedecay.memory.provider.future.v1"
        )
        self.assert_rejected(
            "references unknown contract",
            scenarios=scenarios,
        )

    def test_non_finite_fixture_value_is_rejected(self) -> None:
        scenarios = copy.deepcopy(self.scenarios)
        self.fixture(scenarios, "recall.zero-results")["input"]["bad"] = float(
            "nan"
        )
        self.assert_rejected(
            "not canonical JSON",
            scenarios=scenarios,
        )

    def test_unknown_optional_fixture_must_have_opaque_payload(self) -> None:
        scenarios = copy.deepcopy(self.scenarios)
        fixture = self.fixture(
            scenarios, "observation.unknown-optional-extension.roundtrip"
        )
        fixture["input"] = {"not_an_extension": {"opaque": "lost"}}
        self.assert_rejected(
            "has no opaque payload",
            scenarios=scenarios,
        )

    def test_generated_fixture_drift_is_rejected(self) -> None:
        self.assert_rejected(
            "golden generator check failed",
            mutate_goldens="fixtures",
        )

    def test_generated_manifest_drift_is_rejected(self) -> None:
        self.assert_rejected(
            "golden generator check failed",
            mutate_goldens="manifest",
        )

    def test_scenario_schema_cannot_allow_unknown_fields(self) -> None:
        schema = copy.deepcopy(self.scenario_schema)
        schema["additionalProperties"] = True
        self.assert_rejected(
            "scenario schema root must be strict",
            scenario_schema=schema,
        )


if __name__ == "__main__":
    unittest.main()
