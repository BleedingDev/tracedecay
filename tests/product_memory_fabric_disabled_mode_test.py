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
        # `compose` is the no-observer convenience over `compose_with_observers`,
        # which is itself the Native-shaped convenience over `compose_selected`.
        # `compose_selected` owns the single activation match, and every other
        # entry point must delegate to it, so the dormancy invariant cannot be
        # relocated or bypassed by adding an adapter.
        delegating = _rust_body(self.registry, "    pub fn compose(")
        self.assertEqual(
            _compact(delegating),
            "Self::compose_with_observers(native,Vec::new())",
        )
        with_observers = _rust_body(
            self.registry, "    pub fn compose_with_observers("
        )
        self.assertEqual(
            _compact(with_observers),
            "Self::compose_selected(native.into(),observers)",
        )
        compose = _rust_body(self.registry, "    pub fn compose_selected(")
        match = _rust_body(compose, "match selection")
        disabled_start = match.index("SelectedProviderActivationV1::Disabled")
        enabled_start = match.index("SelectedProviderActivationV1::Native")
        disabled_arm = match[disabled_start:enabled_start]

        self.assertLess(disabled_start, enabled_start)
        self.assertEqual(
            _compact(disabled_arm),
            "SelectedProviderActivationV1::Disabled=>returnOk(Self::Disabled),",
        )
        self.assertIn(
            "ProjectMemoryProviderRegistry::compose_provider_set(",
            compose,
        )

        compose_provider_set = _rust_body(
            self.registry, "    fn compose_provider_set("
        )
        self.assertLess(
            compose_provider_set.index("MemoryFabric::new("),
            compose_provider_set.index("registry.register_selected("),
        )
        # Observers are registered only after the separately selected active
        # provider, and only inside the enabled construction path.
        self.assertLess(
            compose_provider_set.index("registry.register_selected("),
            compose_provider_set.index("registry.register_observer("),
        )
        register_selected = _rust_body(self.registry, "    fn register_selected(")
        self.assertLess(
            register_selected.index("selected.into_provider()"),
            register_selected.index("self.fabric.register("),
        )
        register_observer = _rust_body(self.registry, "    fn register_observer(")
        self.assertIn("ProviderMode::Observer", register_observer)
        self.assertNotIn("ProviderMode::Active", register_observer)

    def test_disabled_composition_exposes_no_registry(self) -> None:
        registry = _rust_body(self.registry, "    pub fn registry(")
        self.assertEqual(
            _compact(registry),
            "matchself{Self::Disabled=>None,Self::Enabled(registry)=>Some(registry),}",
        )

    def test_production_mount_supplies_configuration_resolved_activation(self) -> None:
        # Shape note: production used to hand the mount a literal `Disabled`
        # activation.  The mounted journey now resolves activation from the
        # authoritative runtime configuration instead, so this test proves the
        # same dormancy invariant one step earlier: the production entry can
        # only pass `FromRuntimeConfiguration`, never a pinned activation, and
        # the resolver maps default-false configuration to `Disabled` while
        # refusing a routing gate that names a provider with the host off.
        calls = re.findall(
            r"let memory_provider_host_mount\s*=\s*"
            r"mount_project_memory_provider_host\((.*?)\)\?;",
            self.composition,
            flags=re.DOTALL,
        )
        self.assertEqual(len(calls), 1)
        self.assertEqual(
            _compact(calls[0]),
            "memory_provider_activation,&cg,canonical_project_path,"
            "profile_identity.profile_id(),",
        )

        production_entry = _rust_body(
            self.composition,
            "pub(super) async fn production_project_server(",
        )
        self.assertIn(
            "ProjectMemoryProviderActivationSelector::FromRuntimeConfiguration,",
            production_entry,
        )
        self.assertNotIn("Pinned", production_entry)

        resolver = _rust_body(
            self.composition,
            "fn resolve_memory_provider_activation(",
        )
        compact_resolver = _compact(resolver)
        self.assertIn(
            "(false,None)=>Ok(ProjectMemoryProviderActivation::Disabled),",
            compact_resolver,
        )
        self.assertIn(
            "(false,Some(provider))=>Err(TraceDecayError::Config{",
            compact_resolver,
        )
        self.assertIn(
            "(true,None)=>Ok(ProjectMemoryProviderActivation::NativeObserver),",
            compact_resolver,
        )
        # The guarded arm resolves to exactly one activation and to nothing
        # else: every mountable adapter is Native. Pinning the whole arm keeps
        # a second outcome from being smuggled in beside it.
        self.assertIn(
            "(true,Some(provider))ifis_mountable_active_provider(provider)=>{"
            "Ok(ProjectMemoryProviderActivation::NativeActive)}",
            compact_resolver,
        )

        selector = _rust_body(
            self.composition,
            "pub(super) enum ProjectMemoryProviderActivationSelector {",
        )
        self.assertIn(
            '#[cfg(any(test, feature = "test-transport"))]\n'
            "    Pinned(ProjectMemoryProviderActivation),",
            selector,
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

    def test_provider_host_feature_is_outside_the_default_feature_closure(self) -> None:
        features = self.manifest["features"]
        self.assertEqual(features["default"], ["production"])
        # Shape note: the host feature used to carry a single registry edge.
        # Mounting the observation journey added the durable journal and the
        # hygiene pipeline to the same feature on purpose -- a mounted host
        # that cannot journal or sanitize would be a dispatch path with no
        # outbox and no secret gate.  The edges are still named exactly and
        # all three are still optional.
        self.assertEqual(
            features["memory-provider-host"],
            [
                "dep:tracedecay-memory-provider-registry",
                "dep:tracedecay-memory-observation",
                "dep:tracedecay-memory-hygiene",
            ],
        )

        # The host is opt-in, so it must be absent from the *transitive*
        # closure a plain `cargo build` selects -- `default -> production ->
        # ... -> memory-provider-host` would compile the provider host into
        # every shipped binary while `"memory-provider-host" not in
        # features["production"]` still held.  `product/upstream/
        # convergence-map.json` and `product/upstream/patch-footprint-
        # policy.md` both record this mount as the "default-off host feature".
        def closure(root: str) -> set[str]:
            reached: set[str] = set()
            pending = [root]
            while pending:
                name = pending.pop()
                if name in reached:
                    continue
                reached.add(name)
                for entry in features.get(name, []):
                    if entry.startswith("dep:") or "/" in entry:
                        reached.add(entry)
                    else:
                        pending.append(entry)
            return reached

        for root in ("default", "production"):
            with self.subTest(root=root):
                self.assertNotIn("memory-provider-host", closure(root))

        for package in (
            "tracedecay-memory-provider-registry",
            "tracedecay-memory-observation",
            "tracedecay-memory-hygiene",
        ):
            with self.subTest(package=package):
                self.assertNotIn(package, closure("default"))
                self.assertNotIn(f"dep:{package}", closure("default"))
                # No second door: only the host feature may name a support dep.
                for name, entries in features.items():
                    if name == "memory-provider-host":
                        continue
                    self.assertNotIn(package, entries)
                    self.assertNotIn(f"dep:{package}", entries)
                dependency = self.manifest["dependencies"][package]
                self.assertTrue(dependency["optional"])
                self.assertNotIn("features", dependency)
                self.assertNotIn("default-features", dependency)

    def test_disabled_branch_owns_no_state_path_or_background_task(self) -> None:
        compose = _rust_body(self.registry, "    pub fn compose_selected(")
        match = _rust_body(compose, "match selection")
        disabled_start = match.index("SelectedProviderActivationV1::Disabled")
        enabled_start = match.index("SelectedProviderActivationV1::Native")
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
