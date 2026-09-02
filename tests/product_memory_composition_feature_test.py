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
enum ProjectMemoryProviderActivation {
    Disabled,
    #[cfg(any(test, feature = "test-transport"))]
    NativeActive,
}

#[cfg(feature = "memory-provider-host")]
fn mount_project_memory_provider_host(
    activation: ProjectMemoryProviderActivation,
) -> Result<crate::mcp::server::MemoryProviderHostMount> {
    let activation = match activation {
        ProjectMemoryProviderActivation::Disabled =>
            tracedecay_memory_provider_registry::NativeProviderActivation::Disabled,
        #[cfg(any(test, feature = "test-transport"))]
        ProjectMemoryProviderActivation::NativeActive => {
            tracedecay_memory_provider_registry::NativeProviderActivation::Enabled { port }
        }
    };
    let composition =
        tracedecay_memory_provider_registry::ProjectMemoryProviderComposition::compose(activation)
            .map_err(|error| TraceDecayError::Config {
                message: format!("could not compose project memory-provider host: {error}"),
            })?;
    Ok(Arc::new(composition))
}

pub(super) async fn production_project_server() {
    production_project_server_with_activation(
        ProjectMemoryProviderActivation::Disabled,
    )
}
'''

VALID_ACTIVATION_HARNESS = '''#[cfg(any(test, feature = "test-transport"))]
#[doc(hidden)]
pub async fn open_with_native_provider_for_test() {
    activate(ProjectMemoryProviderActivation::NativeActive);
}
'''

VALID_RETENTION = '''#[cfg(feature = "memory-provider-host")]
pub(crate) type MemoryProviderHostMount =
    Arc<tracedecay_memory_provider_registry::ProjectMemoryProviderComposition>;
'''

VALID_RETAINED_OWNER = '''#[cfg(feature = "memory-provider-host")]
pub(crate) mod native_provider;
#[cfg(all(test, feature = "memory-provider-host"))]
#[path = "retained_owner/native_provider_parity_tests.rs"]
mod native_provider_parity_tests;
'''

VALID_NATIVE_ADAPTER = '''use tracedecay_memory_provider_native::NativeProvider;
use tracedecay_memory_provider_registry::NativeMemoryApplicationPort;
'''

VALID_NATIVE_PROVIDER = (
    VALID_NATIVE_ADAPTER
    + '#[cfg(test)]\n'
    + '#[path = "native_baseline_tests.rs"]\n'
    + "mod baseline_tests;\n"
    + '#[cfg(test)]\n'
    + '#[path = "native_provider_tests.rs"]\n'
    + "mod tests;\n"
)

NATIVE_ADAPTER_PATHS = (
    Path("crates/tracedecay/src/daemon/retained_owner/native_provider.rs"),
    Path("crates/tracedecay/src/daemon/retained_owner/native_provider_tests.rs"),
    Path(
        "crates/tracedecay/src/daemon/retained_owner/native_provider_parity_tests.rs"
    ),
    Path("crates/tracedecay/src/daemon/retained_owner/native_baseline_tests.rs"),
)


class MemoryCompositionFeatureTest(unittest.TestCase):
    def fixture(self) -> tuple[tempfile.TemporaryDirectory[str], Path]:
        directory = tempfile.TemporaryDirectory()
        repo = Path(directory.name)
        manifest = repo / "crates/tracedecay/Cargo.toml"
        mount = repo / "crates/tracedecay/src/daemon/project_composition.rs"
        retention = repo / "crates/tracedecay/src/mcp/server/construction.rs"
        harness = repo / "crates/tracedecay/src/daemon/production_harness.rs"
        manifest.parent.mkdir(parents=True)
        mount.parent.mkdir(parents=True)
        retention.parent.mkdir(parents=True)
        manifest.write_text(VALID_MANIFEST, encoding="utf-8")
        mount.write_text(VALID_MOUNT, encoding="utf-8")
        harness.write_text(VALID_ACTIVATION_HARNESS, encoding="utf-8")
        retention.write_text(VALID_RETENTION, encoding="utf-8")
        (repo / "crates/tracedecay/src/lib.rs").write_text(
            "pub mod stable;\n", encoding="utf-8"
        )
        return directory, repo

    def write_feature_gated_native_adapters(self, repo: Path) -> None:
        retained_owner = repo / "crates/tracedecay/src/daemon/retained_owner.rs"
        retained_owner.write_text(VALID_RETAINED_OWNER, encoding="utf-8")
        for relative in NATIVE_ADAPTER_PATHS:
            path = repo / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                VALID_NATIVE_PROVIDER
                if relative == NATIVE_ADAPTER_PATHS[0]
                else VALID_NATIVE_ADAPTER,
                encoding="utf-8",
            )

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

    def test_test_only_native_activation_requires_exact_cfg_gate(self) -> None:
        directory, repo = self.fixture()
        with directory:
            mount = repo / "crates/tracedecay/src/daemon/project_composition.rs"
            mount.write_text(
                VALID_MOUNT.replace(
                    '#[cfg(any(test, feature = "test-transport"))]\n'
                    "        ProjectMemoryProviderActivation::NativeActive => {",
                    "        ProjectMemoryProviderActivation::NativeActive => {",
                    1,
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("exact test-transport-gated arm" in error for error in errors))

    def test_production_selector_cannot_enable_native_provider(self) -> None:
        directory, repo = self.fixture()
        with directory:
            mount = repo / "crates/tracedecay/src/daemon/project_composition.rs"
            mount.write_text(
                VALID_MOUNT.replace(
                    "ProjectMemoryProviderActivation::Disabled,\n    )",
                    "ProjectMemoryProviderActivation::NativeActive,\n    )",
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("explicitly select the Disabled" in error for error in errors))

    def test_native_active_call_outside_gated_harness_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            leaked = repo / "crates/tracedecay/src/eager.rs"
            leaked.write_text(
                "fn eager() { activate(ProjectMemoryProviderActivation::NativeActive); }\n",
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("activation leaked" in error for error in errors))

    def test_feature_gated_native_adapter_allowlist_passes(self) -> None:
        directory, repo = self.fixture()
        with directory:
            self.write_feature_gated_native_adapters(repo)
            self.assertEqual(CHECKER.check_repository(repo), [])

    def test_native_adapter_lookalikes_still_fail(self) -> None:
        directory, repo = self.fixture()
        with directory:
            lookalikes = (
                Path(
                    "crates/tracedecay/src/daemon/retained_owner/"
                    "native_provider_copy.rs"
                ),
                Path("crates/tracedecay/src/daemon/foreign_native_provider.rs"),
            )
            for relative in lookalikes:
                path = repo / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(VALID_NATIVE_ADAPTER, encoding="utf-8")
            errors = CHECKER.check_repository(repo)
            for relative in lookalikes:
                self.assertTrue(
                    any(str(relative) in error for error in errors),
                    (relative, errors),
                )

    def test_allowlisted_native_adapter_requires_feature_gate(self) -> None:
        directory, repo = self.fixture()
        with directory:
            self.write_feature_gated_native_adapters(repo)
            retained_owner = repo / "crates/tracedecay/src/daemon/retained_owner.rs"
            retained_owner.write_text(
                VALID_RETAINED_OWNER.replace(
                    '#[cfg(feature = "memory-provider-host")]\n', "", 1
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("must be feature-gated" in error for error in errors))

    def test_enabled_activation_in_allowlisted_adapter_still_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            self.write_feature_gated_native_adapters(repo)
            adapter = repo / NATIVE_ADAPTER_PATHS[0]
            adapter.write_text(
                VALID_NATIVE_ADAPTER
                + "fn eager() { activate(NativeProviderActivation::Enabled { port }); }\n",
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "must keep the provider host dormant" in error
                    and str(NATIVE_ADAPTER_PATHS[0]) in error
                    for error in errors
                )
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
