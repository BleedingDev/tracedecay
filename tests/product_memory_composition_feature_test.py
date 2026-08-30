#!/usr/bin/env python3
"""Prove the Memory Fabric composition mount remains explicit and default-off."""

from __future__ import annotations

import tomllib
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "crates/tracedecay/Cargo.toml"
LIB = ROOT / "crates/tracedecay/src/lib.rs"
RUNTIME_PORTS = ROOT / "crates/tracedecay/src/runtime_ports.rs"
COMPOSITION = ROOT / "crates/tracedecay/src/memory_provider_composition.rs"

FEATURE = "memory-provider-fabric"
DEPENDENCIES = {
    "tracedecay-memory-fabric",
    "tracedecay-memory-provider-api",
    "tracedecay-memory-provider-native",
}


class MemoryCompositionFeatureTest(unittest.TestCase):
    def setUp(self) -> None:
        self.manifest = tomllib.loads(MANIFEST.read_text(encoding="utf-8"))

    def test_default_and_production_do_not_enable_memory_fabric(self) -> None:
        features = self.manifest["features"]
        self.assertNotIn(FEATURE, features["default"])
        self.assertNotIn(FEATURE, features["production"])
        self.assertEqual(
            set(features[FEATURE]),
            {f"dep:{dependency}" for dependency in DEPENDENCIES},
        )

    def test_all_memory_dependencies_are_optional(self) -> None:
        dependencies = self.manifest["dependencies"]
        for dependency in DEPENDENCIES:
            with self.subTest(dependency=dependency):
                self.assertTrue(dependencies[dependency]["optional"])
                self.assertEqual(
                    dependencies[dependency]["path"],
                    f"../{dependency}",
                )

    def test_composition_integration_target_requires_the_feature(self) -> None:
        targets = {
            target["name"]: target for target in self.manifest.get("test", [])
        }
        target = targets["memory_provider_composition"]
        self.assertEqual(
            target["path"],
            "tests/product_memory_provider/composition.rs",
        )
        self.assertEqual(target["required-features"], [FEATURE])

    def test_feature_gate_is_not_reached_from_default_runtime_ports(self) -> None:
        library = LIB.read_text(encoding="utf-8")
        runtime_ports = RUNTIME_PORTS.read_text(encoding="utf-8")
        self.assertIn(f'#[cfg(feature = "{FEATURE}")]', library)
        self.assertIn("mod memory_provider_composition;", library)
        self.assertNotIn("compose_native_memory_fabric", runtime_ports)
        self.assertNotIn("tracedecay_memory_fabric", runtime_ports)
        self.assertNotIn("tracedecay_memory_provider_native", runtime_ports)

    def test_composition_has_no_ambient_or_background_activation(self) -> None:
        source = COMPOSITION.read_text(encoding="utf-8")
        for forbidden in (
            "OnceLock",
            "static mut",
            "thread::spawn",
            "tokio::spawn",
            "register_runtime_ports",
            "catalog_composition",
            "context::",
            "tracedecay_store",
            "tracedecay_global_db",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, source)
        self.assertIn("compose_native_memory_fabric", source)


if __name__ == "__main__":
    unittest.main()
