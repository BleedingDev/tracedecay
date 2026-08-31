#!/usr/bin/env python3
"""Source contracts for the inert product memory-provider host mode."""

from __future__ import annotations

import re
import tomllib
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "crates/tracedecay/Cargo.toml"
REGISTRY = ROOT / "crates/tracedecay-memory-provider-registry/src/lib.rs"
COMPOSITION = ROOT / "crates/tracedecay/src/daemon/project_composition.rs"
CONSTRUCTION = ROOT / "crates/tracedecay/src/mcp/server/construction.rs"


def _matching_brace(source: str, opening: int) -> int:
    depth = 0
    index = opening
    while index < len(source):
        if source.startswith("//", index):
            newline = source.find("\n", index + 2)
            index = len(source) if newline == -1 else newline + 1
            continue
        if source.startswith("/*", index):
            closing = source.find("*/", index + 2)
            index = len(source) if closing == -1 else closing + 2
            continue
        if source[index] == '"':
            index += 1
            while index < len(source):
                if source[index] == "\\":
                    index += 2
                elif source[index] == '"':
                    index += 1
                    break
                else:
                    index += 1
            continue
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return index
        index += 1
    raise AssertionError("unclosed Rust block")


def _rust_body(source: str, declaration: str) -> str:
    start = source.index(declaration)
    opening = source.index("{", start)
    return source[opening + 1 : _matching_brace(source, opening)]


def _compact(source: str) -> str:
    return re.sub(r"\s+", "", source)


class MemoryFabricDisabledModeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = tomllib.loads(MANIFEST.read_text(encoding="utf-8"))
        cls.registry = REGISTRY.read_text(encoding="utf-8")
        cls.composition = COMPOSITION.read_text(encoding="utf-8")
        cls.construction = CONSTRUCTION.read_text(encoding="utf-8")

    def test_disabled_compose_arm_returns_before_provider_pipeline(self) -> None:
        compose = _rust_body(self.registry, "    pub fn compose(")
        match = _rust_body(compose, "match native")
        disabled_start = match.index("NativeProviderActivation::Disabled")
        enabled_start = match.index("NativeProviderActivation::Enabled")
        disabled_arm = match[disabled_start:enabled_start]
        enabled_arm = match[enabled_start:]

        self.assertLess(disabled_start, enabled_start)
        self.assertEqual(
            _compact(disabled_arm),
            "NativeProviderActivation::Disabled=>Ok(Self::Disabled),",
        )
        self.assertIn(
            "ProjectMemoryProviderRegistry::compose_native(",
            enabled_arm,
        )

        compose_native = _rust_body(self.registry, "    fn compose_native(")
        self.assertLess(
            compose_native.index("MemoryFabric::new("),
            compose_native.index("registry.register_native("),
        )
        register_native = _rust_body(self.registry, "    fn register_native(")
        self.assertLess(
            register_native.index("NativeProvider::new("),
            register_native.index("self.fabric.register("),
        )

    def test_disabled_composition_exposes_no_registry(self) -> None:
        registry = _rust_body(self.registry, "    pub fn registry(")
        self.assertEqual(
            _compact(registry),
            "matchself{Self::Disabled=>None,Self::Enabled(registry)=>Some(registry),}",
        )

    def test_production_mount_supplies_explicit_disabled_activation(self) -> None:
        calls = re.findall(
            r"let memory_provider_host_mount\s*=\s*"
            r"mount_project_memory_provider_host\((.*?)\)\?;",
            self.composition,
            flags=re.DOTALL,
        )
        self.assertEqual(len(calls), 1)
        self.assertEqual(
            _compact(calls[0]),
            "tracedecay_memory_provider_registry::NativeProviderActivation::Disabled,",
        )

        mount = _rust_body(
            self.composition,
            "fn mount_project_memory_provider_host(",
        )
        self.assertEqual(
            mount.count(
                "tracedecay_memory_provider_registry::"
                "ProjectMemoryProviderComposition::compose(activation)"
            ),
            1,
        )
        self.assertIn("Ok(Arc::new(composition))", _compact(mount))

    def test_provider_host_dependency_is_optional_and_not_directly_defaulted(self) -> None:
        features = self.manifest["features"]
        self.assertEqual(features["default"], ["production"])
        self.assertNotIn("memory-provider-host", features["default"])
        self.assertEqual(
            features["memory-provider-host"],
            ["dep:tracedecay-memory-provider-registry"],
        )
        self.assertIn("memory-provider-host", features["production"])

        dependency = self.manifest["dependencies"][
            "tracedecay-memory-provider-registry"
        ]
        self.assertTrue(dependency["optional"])
        self.assertNotIn("features", dependency)
        self.assertNotIn("default-features", dependency)

    def test_disabled_branch_owns_no_state_path_or_background_task(self) -> None:
        compose = _rust_body(self.registry, "    pub fn compose(")
        match = _rust_body(compose, "match native")
        disabled_start = match.index("NativeProviderActivation::Disabled")
        enabled_start = match.index("NativeProviderActivation::Enabled")
        disabled_arm = match[disabled_start:enabled_start]

        for forbidden in (
            "PathBuf",
            "state_path",
            "storage",
            "tokio::spawn",
            "spawn(",
            "background",
            "register",
            "fabric",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden.lower(), disabled_arm.lower())

        self.assertRegex(
            self.construction,
            r'(?s)#\[cfg\(feature = "memory-provider-host"\)\]\s+'
            r"pub\(crate\) type MemoryProviderHostMount\s*=\s*"
            r"Arc<tracedecay_memory_provider_registry::"
            r"ProjectMemoryProviderComposition>;",
        )
        self.assertRegex(
            self.construction,
            r'(?s)#\[cfg\(feature = "memory-provider-host"\)\]\s+'
            r"pub\(crate\) memory_provider_host_mount:\s*"
            r"Option<MemoryProviderHostMount>,",
        )
        retained_values = re.findall(
            r"(?m)^\s+memory_provider_host_mount:\s*([^,\n]+),",
            self.construction,
        )
        self.assertTrue(retained_values)
        self.assertTrue(all(_compact(value) == "None" for value in retained_values))
        mount = _rust_body(
            self.composition,
            "fn mount_project_memory_provider_host(",
        )
        for forbidden in (
            "PathBuf",
            "tokio::spawn",
            "spawn(",
            "state_path",
            "storage",
            "background",
        ):
            with self.subTest(mount_forbidden=forbidden):
                self.assertNotIn(forbidden.lower(), mount.lower())


if __name__ == "__main__":
    unittest.main()
