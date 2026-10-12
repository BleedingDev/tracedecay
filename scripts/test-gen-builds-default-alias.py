#!/usr/bin/env python3
"""Behavioral tests for Bazel default-feature alias collapse."""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parent / "bazel" / "gen_builds.py"


def load_gen_builds():
    spec = importlib.util.spec_from_file_location("gen_builds", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class DefaultAliasNormalizationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.gen = load_gen_builds()

    def test_adds_default_when_implied_features_are_already_on(self) -> None:
        table = {
            "default": ["production"],
            "production": ["lite", "full", "token-counting"],
            "lite": [],
            "full": [],
            "token-counting": [],
        }
        feats = {"production", "lite", "full", "token-counting"}
        self.assertEqual(
            self.gen.normalize_default_alias(feats, table),
            {"default", "production", "lite", "full", "token-counting"},
        )

    def test_leaves_lean_sets_without_default(self) -> None:
        table = {
            "default": ["production"],
            "production": ["lite"],
            "lite": [],
        }
        self.assertEqual(self.gen.normalize_default_alias(set(), table), set())
        self.assertEqual(self.gen.normalize_default_alias({"lite"}, table), {"lite"})

    def test_empty_default_stays_off(self) -> None:
        table = {"default": []}
        self.assertEqual(
            self.gen.normalize_default_alias({"lite"}, table),
            {"lite"},
        )

    def test_preserves_default_dependency_edges(self) -> None:
        table = {
            "default": ["production", "dep:optional-dep", "other/feat"],
            "production": ["lite"],
            "lite": [],
        }
        self.assertEqual(
            self.gen.normalize_default_alias({"production", "lite"}, table),
            {"production", "lite"},
        )

    def test_collapses_local_alias_with_resolved_dependency_edges(self) -> None:
        table = {
            "default": ["production"],
            "production": ["dep:optional-dep", "other/feat"],
        }
        self.assertEqual(
            self.gen.normalize_default_alias({"production"}, table),
            {"default", "production"},
        )


class BinaryFeatureSelectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.gen = load_gen_builds()

    def package(self, selections):
        return {
            "name": "product-cli",
            "features": {"memory-provider-host": [], "transport": []},
            "metadata": {"bazel": {"binary_features": selections}},
            "targets": [
                {"name": "product", "kind": ["bin"]},
                {"name": "fixture", "kind": ["bin"]},
                {"name": "product", "kind": ["lib"]},
            ],
        }

    def test_selects_only_the_named_production_binary(self) -> None:
        package = self.package({"product": ["memory-provider-host"]})
        self.gen.validate_binary_features(package)
        self.assertEqual(
            self.gen.target_features(package, package["targets"][0]),
            {"memory-provider-host"},
        )
        for target in package["targets"][1:]:
            self.assertEqual(self.gen.target_features(package, target), set())

    def test_preserves_cargo_required_features(self) -> None:
        package = self.package({"product": ["memory-provider-host"]})
        target = dict(package["targets"][0], **{"required-features": ["transport"]})
        self.assertEqual(
            self.gen.target_features(package, target),
            {"memory-provider-host", "transport"},
        )

    def test_rejects_unknown_binary(self) -> None:
        with self.assertRaisesRegex(SystemExit, "unknown binary"):
            self.gen.validate_binary_features(
                self.package({"missing": ["memory-provider-host"]})
            )

    def test_rejects_unknown_feature(self) -> None:
        with self.assertRaisesRegex(SystemExit, "declared package features"):
            self.gen.validate_binary_features(self.package({"product": ["missing"]}))

    def test_rejects_malformed_selection(self) -> None:
        for selections in (["memory-provider-host"], {"product": "memory-provider-host"}):
            with self.subTest(selections=selections), self.assertRaises(SystemExit):
                self.gen.validate_binary_features(self.package(selections))


if __name__ == "__main__":
    unittest.main()
