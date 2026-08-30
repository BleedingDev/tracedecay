#!/usr/bin/env python3
"""Focused tests for exact Memory Fabric dependency-direction enforcement."""

from __future__ import annotations

import copy
import importlib.util
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from typing import Any

REPO = Path(__file__).resolve().parents[1]
CHECKER = REPO / "scripts/product/check-memory-dependency-policy.py"
POLICY = REPO / "product/upstream/patch-footprint-policy.json"


def load_checker() -> ModuleType:
    spec = importlib.util.spec_from_file_location("memory_dependency_policy", CHECKER)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load memory dependency checker")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CHECKER_MODULE = load_checker()


def policy_fixture() -> dict[str, Any]:
    return {
        "dependency_direction_rules": [
            {
                "id": "provider_api_is_inward",
                "from_packages": ["tracedecay-memory-provider-api"],
                "forbidden_dependencies": [
                    "tracedecay-memory-provider-native",
                    "tracedecay-memory-provider-ncm",
                    "tracedecay-store",
                ],
                "reason": "API remains below implementations.",
            },
            {
                "id": "ncm_adapter_does_not_reach_native_store",
                "from_packages": ["tracedecay-memory-provider-ncm"],
                "forbidden_dependencies": ["tracedecay-store", "tracedecay-code-index*"],
                "reason": "NCM remains outside Native persistence and code truth.",
            },
        ],
        "dependency_direction_exception_contract": {
            "required_fields": [
                "id",
                "rule_id",
                "from_package",
                "to_package",
                "adr",
                "rationale",
                "reviewed_by",
                "status",
            ],
            "status_values": ["active", "retired"],
            "exact_edge_only": True,
            "adr_prefix": "product/architecture/adr/",
        },
        "dependency_direction_exceptions": [],
    }


def write_package(root: Path, name: str, dependencies: list[str]) -> None:
    path = root / "crates" / name / "Cargo.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    dependency_lines = "\n".join(
        f'{dependency} = {{ path = "../{dependency}" }}' for dependency in dependencies
    )
    path.write_text(
        f'[package]\nname = "{name}"\nversion = "0.1.0"\nedition = "2024"\n\n'
        f"[dependencies]\n{dependency_lines}\n",
        encoding="utf-8",
    )


def exact_exception() -> dict[str, str]:
    return {
        "id": "temporary-api-to-ncm",
        "rule_id": "provider_api_is_inward",
        "from_package": "tracedecay-memory-provider-api",
        "to_package": "tracedecay-memory-provider-ncm",
        "adr": "product/architecture/adr/0999-test-exception.md",
        "rationale": "Temporary fixture proving exact reviewed suppression.",
        "reviewed_by": "tdmem-0306-test",
        "status": "active",
    }


class MemoryDependencyPolicyTest(unittest.TestCase):
    def validate(
        self,
        root: Path,
        policy: dict[str, Any],
    ) -> tuple[list[str], dict[str, int]]:
        return CHECKER_MODULE.validate_repository(root, policy)

    def test_valid_provider_graph_passes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_package(root, "tracedecay-memory-provider-api", [])
            write_package(
                root,
                "tracedecay-memory-provider-ncm",
                ["tracedecay-memory-provider-api"],
            )
            errors, stats = self.validate(root, policy_fixture())
            self.assertEqual(errors, [])
            self.assertEqual(stats["manifests_checked"], 2)

    def test_provider_api_cannot_depend_on_adapter(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_package(
                root,
                "tracedecay-memory-provider-api",
                ["tracedecay-memory-provider-ncm"],
            )
            errors, _ = self.validate(root, policy_fixture())
            self.assertIn(
                "provider_api_is_inward violated: tracedecay-memory-provider-api -> tracedecay-memory-provider-ncm",
                "\n".join(errors),
            )

    def test_ncm_cannot_reach_native_store(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_package(
                root,
                "tracedecay-memory-provider-ncm",
                ["tracedecay-store"],
            )
            errors, _ = self.validate(root, policy_fixture())
            self.assertIn(
                "ncm_adapter_does_not_reach_native_store violated: tracedecay-memory-provider-ncm -> tracedecay-store",
                "\n".join(errors),
            )

    def test_exact_adr_exception_suppresses_only_one_edge(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_package(
                root,
                "tracedecay-memory-provider-api",
                ["tracedecay-memory-provider-ncm"],
            )
            adr = root / "product/architecture/adr/0999-test-exception.md"
            adr.parent.mkdir(parents=True, exist_ok=True)
            adr.write_text("# Test exception\n", encoding="utf-8")
            policy = policy_fixture()
            policy["dependency_direction_exceptions"] = [exact_exception()]
            errors, stats = self.validate(root, policy)
            self.assertEqual(errors, [])
            self.assertEqual(stats["used_exceptions"], 1)

    def test_exception_requires_rationale_and_existing_adr(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_package(
                root,
                "tracedecay-memory-provider-api",
                ["tracedecay-memory-provider-ncm"],
            )
            policy = policy_fixture()
            row = exact_exception()
            row["rationale"] = ""
            policy["dependency_direction_exceptions"] = [row]
            errors, _ = self.validate(root, policy)
            text = "\n".join(errors)
            self.assertIn("rationale must be non-empty", text)
            self.assertIn("adr does not exist", text)

    def test_exception_cannot_use_package_globs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_package(
                root,
                "tracedecay-memory-provider-api",
                ["tracedecay-memory-provider-ncm"],
            )
            policy = policy_fixture()
            row = exact_exception()
            row["to_package"] = "tracedecay-memory-provider-*"
            policy["dependency_direction_exceptions"] = [row]
            errors, _ = self.validate(root, policy)
            self.assertIn("must name one exact package, not a glob", "\n".join(errors))

    def test_active_exception_cannot_outlive_the_edge(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_package(root, "tracedecay-memory-provider-api", [])
            adr = root / "product/architecture/adr/0999-test-exception.md"
            adr.parent.mkdir(parents=True, exist_ok=True)
            adr.write_text("# Test exception\n", encoding="utf-8")
            policy = policy_fixture()
            policy["dependency_direction_exceptions"] = [exact_exception()]
            errors, _ = self.validate(root, policy)
            self.assertIn("active dependency exception is stale", "\n".join(errors))

    def test_real_repository_policy_passes(self) -> None:
        result = subprocess.run(
            [
                "python3",
                str(CHECKER),
                "--repo",
                str(REPO),
                "--policy",
                str(POLICY),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertTrue(receipt["ok"])
        self.assertEqual(receipt["active_exceptions"], 0)

    def test_contract_field_removal_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_package(root, "tracedecay-memory-provider-api", [])
            policy = copy.deepcopy(policy_fixture())
            policy["dependency_direction_exception_contract"]["required_fields"].remove(
                "rationale"
            )
            errors, _ = self.validate(root, policy)
            self.assertIn("required_fields do not match", "\n".join(errors))


if __name__ == "__main__":
    unittest.main()
