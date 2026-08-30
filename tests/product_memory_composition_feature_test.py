#!/usr/bin/env python3
"""Focused tests for the default-off Memory Fabric feature verifier."""

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from types import ModuleType

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts/product/check-memory-composition-feature.py"


def load_checker() -> ModuleType:
    spec = importlib.util.spec_from_file_location("memory_composition_checker", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load memory composition checker")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CHECKER = load_checker()

VALID_MANIFEST = '''[features]
default = ["production"]
production = []
memory-fabric = ["dep:tracedecay-memory-provider-registry"]

[dependencies]
tracedecay-memory-provider-registry = { path = "../tracedecay-memory-provider-registry", optional = true }
'''

VALID_MOUNT = '''#[cfg(feature = "memory-fabric")]
pub(crate) fn compose_native_memory_fabric(
    port: std::sync::Arc<dyn tracedecay_memory_provider_registry::NativeMemoryApplicationPort>,
    config: tracedecay_memory_provider_registry::NativeCompositionConfig,
) -> Result<
    tracedecay_memory_provider_registry::NativeMemoryComposition,
    tracedecay_memory_provider_registry::CompositionError,
> {
    tracedecay_memory_provider_registry::compose_native_memory(port, config)
}
'''


class MemoryCompositionFeatureTest(unittest.TestCase):
    def fixture(self) -> tuple[tempfile.TemporaryDirectory[str], Path]:
        directory = tempfile.TemporaryDirectory()
        repo = Path(directory.name)
        manifest = repo / "crates/tracedecay/Cargo.toml"
        mount = repo / "crates/tracedecay/src/runtime_ports.rs"
        manifest.parent.mkdir(parents=True)
        mount.parent.mkdir(parents=True)
        manifest.write_text(VALID_MANIFEST, encoding="utf-8")
        mount.write_text(VALID_MOUNT, encoding="utf-8")
        (mount.parent / "lib.rs").write_text("pub mod stable;\n", encoding="utf-8")
        return directory, repo

    def test_valid_feature_and_mount_pass(self) -> None:
        directory, repo = self.fixture()
        with directory:
            self.assertEqual(CHECKER.check_repository(repo), [])

    def test_default_feature_activation_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            manifest = repo / "crates/tracedecay/Cargo.toml"
            manifest.write_text(
                VALID_MANIFEST.replace(
                    'default = ["production"]',
                    'default = ["production", "memory-fabric"]',
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("default must not enable" in error for error in errors))

    def test_non_optional_registry_dependency_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            manifest = repo / "crates/tracedecay/Cargo.toml"
            manifest.write_text(
                VALID_MANIFEST.replace(", optional = true", ""),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("must be optional" in error for error in errors))

    def test_concrete_adapter_in_mount_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            mount = repo / "crates/tracedecay/src/runtime_ports.rs"
            mount.write_text(
                VALID_MOUNT + "use tracedecay_memory_provider_native::NativeProvider;\n",
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("NativeProvider" in error for error in errors))

    def test_registry_leak_outside_mount_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            leaked = repo / "crates/tracedecay/src/dashboard.rs"
            leaked.write_text(
                "use tracedecay_memory_provider_registry::NativeMemoryComposition;\n",
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("leaked outside" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
