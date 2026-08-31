#!/usr/bin/env python3
"""Focused tests for the dormant-by-default memory-provider host verifier."""

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
production = ["memory-provider-host"]
memory-provider-host = ["dep:tracedecay-memory-provider-registry"]

[dependencies]
tracedecay-memory-provider-registry = { path = "../tracedecay-memory-provider-registry", optional = true }
'''

VALID_MOUNT = '''#[cfg(feature = "memory-provider-host")]
fn mount_project_memory_provider_host(
    activation: tracedecay_memory_provider_registry::NativeProviderActivation,
) -> Result<crate::mcp::server::MemoryProviderHostMount> {
    let composition =
        tracedecay_memory_provider_registry::ProjectMemoryProviderComposition::compose(activation)
            .map_err(|error| TraceDecayError::Config {
                message: format!("could not compose project memory-provider host: {error}"),
            })?;
    Ok(Arc::new(composition))
}

fn production_mount() -> Result<crate::mcp::server::MemoryProviderHostMount> {
    mount_project_memory_provider_host(
        tracedecay_memory_provider_registry::NativeProviderActivation::Disabled,
    )
}
'''

VALID_RETENTION = '''#[cfg(feature = "memory-provider-host")]
pub(crate) type MemoryProviderHostMount =
    Arc<tracedecay_memory_provider_registry::ProjectMemoryProviderComposition>;
'''


class MemoryCompositionFeatureTest(unittest.TestCase):
    def fixture(self) -> tuple[tempfile.TemporaryDirectory[str], Path]:
        directory = tempfile.TemporaryDirectory()
        repo = Path(directory.name)
        manifest = repo / "crates/tracedecay/Cargo.toml"
        mount = repo / "crates/tracedecay/src/daemon/project_composition.rs"
        retention = repo / "crates/tracedecay/src/mcp/server/construction.rs"
        manifest.parent.mkdir(parents=True)
        mount.parent.mkdir(parents=True)
        retention.parent.mkdir(parents=True)
        manifest.write_text(VALID_MANIFEST, encoding="utf-8")
        mount.write_text(VALID_MOUNT, encoding="utf-8")
        retention.write_text(VALID_RETENTION, encoding="utf-8")
        (repo / "crates/tracedecay/src/lib.rs").write_text(
            "pub mod stable;\n", encoding="utf-8"
        )
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
                    'default = ["production", "memory-provider-host"]',
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any("default features must remain exactly" in error for error in errors)
            )

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
            mount = repo / "crates/tracedecay/src/daemon/project_composition.rs"
            mount.write_text(
                VALID_MOUNT + "use tracedecay_memory_provider_native::NativeProvider;\n",
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("NativeProvider" in error for error in errors))

    def test_activation_seam_does_not_trip_adapter_heuristic(self) -> None:
        directory, repo = self.fixture()
        with directory:
            errors = CHECKER.check_repository(repo)
            self.assertFalse(
                any("concrete Native adapter" in error for error in errors)
            )

    def test_registry_leak_outside_mount_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            leaked = repo / "crates/tracedecay/src/dashboard.rs"
            leaked.write_text(
                "use tracedecay_memory_provider_registry::ProjectMemoryProviderComposition;\n",
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("leaked outside" in error for error in errors))

    def test_retention_mount_must_not_compose(self) -> None:
        directory, repo = self.fixture()
        with directory:
            retention = repo / "crates/tracedecay/src/mcp/server/construction.rs"
            retention.write_text(
                VALID_RETENTION
                + "fn sneak() { let _ = tracedecay_memory_provider_registry::"
                "ProjectMemoryProviderComposition::compose(activation); }\n",
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any("must not compose providers" in error for error in errors)
            )

    def test_enabled_activation_in_root_source_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            enabled = repo / "crates/tracedecay/src/eager.rs"
            enabled.write_text(
                "fn eager() { activate(NativeProviderActivation::Enabled { port }); }\n",
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any("must keep the provider host dormant" in error for error in errors)
            )


if __name__ == "__main__":
    unittest.main()
