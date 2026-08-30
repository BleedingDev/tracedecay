#!/usr/bin/env python3
"""Focused positive and negative tests for memory dependency direction."""

from __future__ import annotations

import copy
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from typing import Any

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts/product/check-memory-dependency-direction.py"
POLICY = REPO / "product/architecture/memory-dependency-policy.json"


def load_checker() -> ModuleType:
    spec = importlib.util.spec_from_file_location("memory_dependency_checker", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load memory dependency checker")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CHECKER = load_checker()


def dependency(name: str) -> dict[str, Any]:
    return {"name": name}


def package(name: str, dependencies: list[str]) -> dict[str, Any]:
    return {
        "name": name,
        "dependencies": [dependency(value) for value in dependencies],
    }


def valid_metadata() -> dict[str, Any]:
    return {
        "packages": [
            package("tracedecay-memory-provider-api", []),
            package(
                "tracedecay-memory-fabric",
                ["tracedecay-memory-provider-api"],
            ),
            package(
                "tracedecay-memory-provider-native",
                ["tracedecay-memory-provider-api"],
            ),
            package(
                "tracedecay-memory-provider-ncm",
                ["sha2", "tracedecay-memory-provider-api"],
            ),
            package(
                "tracedecay-memory-conformance",
                ["tracedecay-memory-provider-api"],
            ),
            package("tracedecay-cli", []),
            package("tracedecay-dashboard-api", []),
            package("tracedecay-mcp", []),
            package("tracedecay-sdk", []),
        ]
    }


class MemoryDependencyDirectionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = json.loads(POLICY.read_text(encoding="utf-8"))

    def test_valid_product_graph_passes(self) -> None:
        self.assertEqual(CHECKER.check_policy(REPO, self.policy, valid_metadata()), [])

    def test_ncm_store_edge_fails_closed(self) -> None:
        metadata = valid_metadata()
        ncm = next(
            value
            for value in metadata["packages"]
            if value["name"] == "tracedecay-memory-provider-ncm"
        )
        ncm["dependencies"].append(dependency("tracedecay-store"))
        errors = CHECKER.check_policy(REPO, self.policy, metadata)
        self.assertTrue(
            any(
                "tracedecay-memory-provider-ncm -> tracedecay-store" in error
                for error in errors
            )
        )

    def test_provider_api_cannot_depend_on_fabric(self) -> None:
        metadata = valid_metadata()
        api = next(
            value
            for value in metadata["packages"]
            if value["name"] == "tracedecay-memory-provider-api"
        )
        api["dependencies"].append(dependency("tracedecay-memory-fabric"))
        errors = CHECKER.check_policy(REPO, self.policy, metadata)
        self.assertTrue(
            any(
                "tracedecay-memory-provider-api -> tracedecay-memory-fabric" in error
                for error in errors
            )
        )

    def test_incomplete_exception_is_rejected(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["exceptions"] = [
            {
                "id": "bad",
                "rule_id": "package-contract:tracedecay-memory-provider-ncm",
                "from_package": "tracedecay-memory-provider-ncm",
                "to_package": "tracedecay-store",
            }
        ]
        errors = CHECKER.check_policy(REPO, policy, valid_metadata())
        self.assertTrue(
            any("rationale must be a non-empty string" in error for error in errors)
        )

    def test_complete_exact_exception_can_authorize_one_edge(self) -> None:
        metadata = valid_metadata()
        ncm = next(
            value
            for value in metadata["packages"]
            if value["name"] == "tracedecay-memory-provider-ncm"
        )
        ncm["dependencies"].append(dependency("tracedecay-store"))
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            adr = repo / "product/architecture/adr/ADR-test-memory-edge.md"
            adr.parent.mkdir(parents=True)
            adr.write_text("# Test-only reviewed edge\n", encoding="utf-8")
            policy = copy.deepcopy(self.policy)
            policy["exceptions"] = [
                {
                    "id": "test-ncm-store-edge",
                    "rule_id": "package-contract:tracedecay-memory-provider-ncm",
                    "from_package": "tracedecay-memory-provider-ncm",
                    "to_package": "tracedecay-store",
                    "adr": "product/architecture/adr/ADR-test-memory-edge.md",
                    "rationale": "Test fixture proving one exact reviewed exception.",
                    "owner": "architecture-review",
                    "verification": ["python3 focused-negative-test"],
                    "review_after": "2027-01-01",
                },
                {
                    "id": "test-ncm-store-rule-edge",
                    "rule_id": "ncm-adapter-cannot-reach-tracedecay-internals",
                    "from_package": "tracedecay-memory-provider-ncm",
                    "to_package": "tracedecay-store",
                    "adr": "product/architecture/adr/ADR-test-memory-edge.md",
                    "rationale": "The same exact edge is reviewed against the explicit NCM rule.",
                    "owner": "architecture-review",
                    "verification": ["python3 focused-negative-test"],
                    "review_after": "2027-01-01",
                },
            ]
            self.assertEqual(CHECKER.check_policy(repo, policy, metadata), [])

    def test_unused_exception_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            adr = repo / "product/architecture/adr/ADR-test-memory-edge.md"
            adr.parent.mkdir(parents=True)
            adr.write_text("# Test-only reviewed edge\n", encoding="utf-8")
            policy = copy.deepcopy(self.policy)
            policy["exceptions"] = [
                {
                    "id": "unused",
                    "rule_id": "provider-api-is-inward",
                    "from_package": "tracedecay-memory-provider-api",
                    "to_package": "tracedecay-store",
                    "adr": "product/architecture/adr/ADR-test-memory-edge.md",
                    "rationale": "This edge is absent and must not remain pre-authorized.",
                    "owner": "architecture-review",
                    "verification": ["python3 focused-negative-test"],
                    "review_after": "2027-01-01",
                }
            ]
            errors = CHECKER.check_policy(repo, policy, valid_metadata())
            self.assertTrue(any("unused dependency exception" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
