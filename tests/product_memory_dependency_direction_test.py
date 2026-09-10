#!/usr/bin/env python3
"""Focused positive and negative tests for memory dependency direction.

Two layers are locked down here, because the gate has two layers:

* Cargo metadata: exact names split by dependency kind, and an exact allowed
  feature set per production edge.
* Crate source: an exact item-path import allowlist, executor call sites pinned
  by enclosing function, and the checker's forbidden-capability-symbol floor.

The contracts crate has no optional git reader. Source checks still pin executor
sites and reject unlisted imports and capability symbols. Tests of the optional
capability/unification obligation explicitly inject an optional gix dependency;
they do not assume that capability exists in the production contracts closure.
"""

from __future__ import annotations

import copy
import importlib.util
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from typing import Any, Callable

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts/product/check-memory-dependency-direction.py"
POLICY = REPO / "product/architecture/memory-dependency-policy.json"

REGISTRY = "tracedecay-memory-provider-registry"
CONTRACTS = "tracedecay-contracts"
CONTRACTS_RULE = "application-contract-crate-capability-closure"
NCM = "tracedecay-memory-provider-ncm"
RUNTIME = "tracedecay-memory-ncm-runtime"
API = "tracedecay-memory-provider-api"

# Every concrete capability family the composition registry must never reach,
# named exactly rather than matched by a glob. Each one is injected on its own
# and must still be refused by composition-registry-is-narrow.
FORBIDDEN_REGISTRY_EDGES = (
    "tracedecay-application",
    "tracedecay",
    "tracedecay-store",
    "tracedecay-session-temporal-store",
    "tracedecay-graph-db",
    "tracedecay-code-index",
    "tracedecay-code-index-rust",
    "tracedecay-agent-hosts",
    "tracedecay-host-runtime",
    "tracedecay-mcp",
    "tracedecay-cli",
    "tracedecay-dashboard-api",
    "tracedecay-sdk",
    "tracedecay-daemon",
    "tracedecay-memory-provider-ncm",
    "ncm-sdk",
    "ocean-core",
)

# Concrete capabilities that must not become reachable through the one contract
# crate the registry is allowed to depend on. External crates are included
# deliberately: a prefix rule over tracedecay-*/ncm*/ocean* would let an
# external store or HTTP stack in.
FORBIDDEN_CONTRACTS_EDGES = (
    "gix",
    "tempfile",
    "tracedecay-application",
    "tracedecay",
    "tracedecay-store",
    "tracedecay-code-index",
    "tracedecay-mcp",
    "tracedecay-host-runtime",
    "rusqlite",
    "hyper",
    "reqwest",
    "sled",
    "tantivy",
    "ncm-sdk",
    "ocean-core",
)

# Capability symbols the registry source may never name, whatever the Cargo
# graph says. Each is injected into a copy of the real crate source.
FORBIDDEN_SOURCE_SNIPPETS = (
    ("filesystem", 'fn probe() { let _ = std::fs::read("x"); }'),
    ("network", "fn probe() { let _: Option<std::net::TcpStream> = None; }"),
    ("process", 'fn probe() { let _ = std::process::Command::new("ls"); }'),
    ("os thread", "fn probe() { std::thread::spawn(|| ()); }"),
    ("runtime builder", "fn probe() { let _ = tokio::runtime::Builder::new_current_thread(); }"),
    ("runtime new", "fn probe() { let _ = Runtime::new(); }"),
    ("block_on", "fn probe() { block_on(async {}); }"),
    ("spawn_local", "fn probe() { spawn_local(async {}); }"),
    ("LocalSet", "fn probe() { let _ = LocalSet::new(); }"),
    ("JoinSet", "fn probe() { let _ = JoinSet::new(); }"),
    ("embedded store", "fn probe() { let _ = rusqlite::Connection::open_in_memory(); }"),
    ("socket", "fn probe() { let _: Option<UnixStream> = None; }"),
    ("http stack", "fn probe() { let _ = reqwest::get(\"http://x\"); }"),
    ("git object store", 'fn probe() { let _ = gix::open("/repo"); }'),
    ("git blob reader", "fn probe() { let _: Option<NativeHistoricalBlobReaderV1> = None; }"),
)


def load_checker() -> ModuleType:
    spec = importlib.util.spec_from_file_location("memory_dependency_checker", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load memory dependency checker")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CHECKER = load_checker()


def dependency(
    name: str,
    kind: str | None = None,
    features: list[str] | None = None,
    uses_default_features: bool = True,
    optional: bool = False,
) -> dict[str, Any]:
    entry: dict[str, Any] = {"name": name, "kind": kind}
    if features is not None:
        entry["features"] = features
    entry["uses_default_features"] = uses_default_features
    entry["optional"] = optional
    return entry


def package(name: str, dependencies: list[str]) -> dict[str, Any]:
    return {
        "name": name,
        "dependencies": [dependency(value) for value in dependencies],
    }


def package_of(name: str, dependencies: list[dict[str, Any]]) -> dict[str, Any]:
    return {"name": name, "dependencies": dependencies}


def registry_package() -> dict[str, Any]:
    """The composition registry exactly as the locked Cargo graph reports it."""
    return package_of(
        REGISTRY,
        [
            dependency("chrono", features=["std"], uses_default_features=False),
            dependency("getrandom"),
            dependency("hex"),
            dependency("serde", features=["derive"]),
            dependency("serde_json", features=["raw_value"]),
            dependency("sha2"),
            dependency("thiserror"),
            dependency("tiktoken-rs"),
            dependency(
                "tokio", features=["macros", "rt", "sync", "time"], uses_default_features=False
            ),
            dependency(CONTRACTS, features=[], uses_default_features=False),
            dependency("tracedecay-memory-fabric"),
            dependency("tracedecay-memory-provider-api"),
            dependency("tracedecay-memory-provider-native"),
            dependency(
                "tokio", kind="dev", features=["macros", "rt", "rt-multi-thread", "sync", "time"]
            ),
            dependency("tracedecay-domain", kind="dev"),
            dependency("tracedecay-memory-conformance", kind="dev"),
        ],
    )


def contracts_package() -> dict[str, Any]:
    """The capability-free contract crate the registry adapts, as Cargo reports it."""
    return package_of(
        CONTRACTS,
        [
            dependency("getrandom"),
            dependency("hex"),
            dependency("sha2"),
            dependency("hotpath", features=["threads"], uses_default_features=False),
            dependency("schemars"),
            dependency("serde", features=["derive"]),
            dependency("serde_json"),
            dependency("thiserror"),
            dependency("tracedecay-domain"),
            dependency("tracedecay-policy"),
            dependency("tracedecay-tool-catalog"),
            dependency("tokio", kind="dev", features=["macros", "rt"]),
        ],
    )


def valid_metadata() -> dict[str, Any]:
    return {
        "packages": [
            package("tracedecay-memory-provider-api", ["sha2"]),
            package(
                "tracedecay-memory-fabric",
                ["tracing", "tracedecay-memory-provider-api"],
            ),
            package(
                "tracedecay-memory-provider-native",
                ["serde_json", "tracedecay-memory-provider-api"],
            ),
            package_of(
                "tracedecay-memory-provider-ncm",
                [
                    dependency("chrono", features=["std"], uses_default_features=False),
                    dependency("serde_json"),
                    dependency("sha2"),
                    dependency("tracedecay-memory-provider-api"),
                    dependency(
                        "tracedecay-memory-ncm-runtime",
                        features=[],
                        uses_default_features=False,
                        optional=True,
                    ),
                    dependency("tracedecay-memory-conformance", kind="dev"),
                    dependency(REGISTRY, kind="dev"),
                ],
            ),
            package_of(
                "tracedecay-memory-ncm-core",
                [
                    dependency("serde", features=["derive"]),
                    dependency("serde_json", kind="dev"),
                ],
            ),
            package_of(
                "tracedecay-memory-ncm-runtime",
                [
                    dependency(
                        "fastembed",
                        features=["hf-hub-rustls-tls", "ort-download-binaries-rustls-tls"],
                        uses_default_features=False,
                        optional=True,
                    ),
                    dependency("rusqlite", features=["bundled"], uses_default_features=False),
                    dependency("serde", features=["derive"]),
                    dependency("serde_json"),
                    dependency("sha2"),
                    dependency("tracedecay-memory-ncm-core"),
                    dependency(API),
                    dependency("tempfile", kind="dev"),
                ],
            ),
            package_of(
                "tracedecay-memory-conformance",
                [
                    dependency("serde", features=["derive"]),
                    dependency("serde_json"),
                    dependency("sha2"),
                    dependency("tiktoken-rs"),
                    dependency("tracedecay-memory-provider-api"),
                ],
            ),
            package_of(
                "tracedecay-memory-evaluation",
                [
                    dependency("serde", features=["derive"]),
                    dependency("serde_json"),
                    dependency("thiserror"),
                    dependency("tracedecay-memory-conformance"),
                    dependency("tracedecay-memory-provider-api"),
                ],
            ),
            registry_package(),
            contracts_package(),
            package_of(
                "tracedecay-memory-observation",
                [
                    dependency(
                        "rusqlite",
                        features=["bundled", "cache"],
                        uses_default_features=False,
                    ),
                    dependency("serde", features=["derive"]),
                    dependency("serde_json"),
                    dependency("sha2"),
                    dependency("thiserror"),
                    dependency("tracedecay-memory-provider-api"),
                ],
            ),
            package_of(
                "tracedecay-memory-hygiene",
                [
                    dependency("regex"),
                    dependency("serde", features=["derive"]),
                    dependency("serde_json"),
                    dependency("sha2"),
                    dependency("thiserror"),
                    dependency("tracedecay-domain"),
                    dependency("tracedecay-memory-provider-api"),
                    dependency("tracedecay-runtime-core"),
                ],
            ),
            package("tracedecay-cli", []),
            package("tracedecay-dashboard-api", []),
            package("tracedecay-mcp", []),
            package("tracedecay-sdk", []),
        ]
    }


def find(metadata: dict[str, Any], name: str) -> dict[str, Any]:
    return next(value for value in metadata["packages"] if value["name"] == name)


class MemoryDependencyDirectionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = json.loads(POLICY.read_text(encoding="utf-8"))

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def check_with_source(
        self,
        mutate: Callable[[Path], None],
        policy: dict[str, Any] | None = None,
    ) -> list[str]:
        """Evaluate the real policy against a mutated copy of the real source."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            destination = root / "crates" / REGISTRY / "src"
            destination.parent.mkdir(parents=True)
            shutil.copytree(REPO / "crates" / REGISTRY / "src", destination)
            for name in {
                contracted["package"]
                for contracted in self.policy["source_contracts"]
                + self.policy.get("edge_source_contracts", [])
            }:
                if name != REGISTRY:
                    shutil.copytree(
                        REPO / "crates" / name / "src", root / "crates" / name / "src"
                    )
            mutate(destination)
            return CHECKER.check_policy(
                REPO,
                self.policy if policy is None else policy,
                valid_metadata(),
                source_repo=root,
            )

    @staticmethod
    def append_source(text: str) -> Callable[[Path], None]:
        def mutate(source: Path) -> None:
            target = source / "lib.rs"
            target.write_text(
                target.read_text(encoding="utf-8") + "\n" + text + "\n", encoding="utf-8"
            )

        return mutate

    # ------------------------------------------------------------------
    # metadata layer
    # ------------------------------------------------------------------
    def test_valid_product_graph_passes(self) -> None:
        self.assertEqual(CHECKER.check_policy(REPO, self.policy, valid_metadata()), [])

    def test_ncm_registry_dev_edge_requires_both_exact_dev_allowances(self) -> None:
        for kind in ("package_contracts", "rules"):
            with self.subTest(policy_kind=kind):
                policy = copy.deepcopy(self.policy)
                for row in policy[kind]:
                    if row.get("package") == NCM or row.get("id") == "ncm-adapter-cannot-reach-tracedecay-internals":
                        row["allowed_dev_dependencies"].remove(REGISTRY)
                errors = CHECKER.check_policy(REPO, policy, valid_metadata())
                self.assertTrue(any(f"{NCM} -> {REGISTRY}" in error for error in errors), errors)

    def test_ncm_registry_dev_allowance_cannot_authorize_production_edges(self) -> None:
        for kind in (None, "build"):
            with self.subTest(kind=kind):
                metadata = valid_metadata()
                find(metadata, NCM)["dependencies"].append(dependency(REGISTRY, kind=kind))
                errors = CHECKER.check_policy(REPO, self.policy, metadata)
                self.assertTrue(any(f"{NCM} -> {REGISTRY}" in error for error in errors), errors)

    def test_ncm_store_edge_fails_closed(self) -> None:
        metadata = valid_metadata()
        ncm = find(metadata, "tracedecay-memory-provider-ncm")
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
        api = find(metadata, "tracedecay-memory-provider-api")
        api["dependencies"].append(dependency("tracedecay-memory-fabric"))
        errors = CHECKER.check_policy(REPO, self.policy, metadata)
        self.assertTrue(
            any(
                "tracedecay-memory-provider-api -> tracedecay-memory-fabric" in error
                for error in errors
            )
        )

    def test_registry_capability_edges_each_fail_closed(self) -> None:
        """Every store, session store, code index, host, transport, other
        provider, and root-crate edge is refused on its own by the narrow rule."""
        for target in FORBIDDEN_REGISTRY_EDGES:
            with self.subTest(target=target):
                metadata = valid_metadata()
                find(metadata, REGISTRY)["dependencies"].append(dependency(target))
                errors = CHECKER.check_policy(REPO, self.policy, metadata)
                self.assertTrue(
                    any(
                        f"{REGISTRY} -> {target} (composition-registry-is-narrow)" in error
                        for error in errors
                    ),
                    f"{target} was not refused by composition-registry-is-narrow: {errors}",
                )
                self.assertTrue(
                    any(
                        f"{REGISTRY} -> {target} (package-contract:{REGISTRY})" in error
                        for error in errors
                    ),
                    f"{target} was not refused by the package allowlist: {errors}",
                )

    def test_registry_dev_only_capability_edge_fails_closed(self) -> None:
        """The dev-dependency allowance is an exact name list, not a test exemption."""
        metadata = valid_metadata()
        find(metadata, REGISTRY)["dependencies"].append(
            dependency("tracedecay-store", kind="dev")
        )
        errors = CHECKER.check_policy(REPO, self.policy, metadata)
        self.assertTrue(
            any(
                f"{REGISTRY} -> tracedecay-store (package-contract-dev:{REGISTRY})" in error
                for error in errors
            ),
            errors,
        )
        self.assertTrue(
            any(
                f"{REGISTRY} -> tracedecay-store (composition-registry-is-narrow)" in error
                for error in errors
            ),
            errors,
        )

    def test_registry_test_only_domain_edge_does_not_authorize_a_shipped_edge(self) -> None:
        """tracedecay-domain is allowed for tests only; a production edge still fails."""
        metadata = valid_metadata()
        find(metadata, REGISTRY)["dependencies"].append(dependency("tracedecay-domain"))
        errors = CHECKER.check_policy(REPO, self.policy, metadata)
        self.assertTrue(
            any(
                f"{REGISTRY} -> tracedecay-domain (composition-registry-is-narrow)" in error
                for error in errors
            ),
            errors,
        )

    def test_reviewed_registry_features_pass(self) -> None:
        """Exercise every reviewed feature, not just a minimal feature subset."""
        registry_contract = next(
            contract for contract in self.policy["package_contracts"]
            if contract["package"] == REGISTRY
        )
        metadata = valid_metadata()
        for entry in find(metadata, REGISTRY)["dependencies"]:
            if entry.get("kind") is None:
                self.assertEqual(
                    set(entry.get("features", [])),
                    set(registry_contract["allowed_dependency_features"][entry["name"]]),
                    entry["name"],
                )
        self.assertEqual(CHECKER.check_policy(REPO, self.policy, metadata), [])

    def test_dependency_features_are_an_exact_allowlist_not_a_denylist(self) -> None:
        """An unlisted feature is refused even when nobody predicted its name.

        `chrono/clock` is the concrete case the policy rationale depends on (no
        ambient clock in the registry); the invented names stand for a future
        capability feature that no denylist could have enumerated.
        """
        cases = (
            ("chrono", "clock"),
            ("chrono", "now"),
            (CONTRACTS, "native-git"),
            (CONTRACTS, "native-sqlite-store"),
            (CONTRACTS, "embedded-index"),
            ("tokio", "net"),
            ("tokio", "process"),
            ("tokio", "fs"),
            ("tokio", "signal"),
            ("tokio", "rt-multi-thread"),
            ("tokio", "full"),
            ("tokio", "io-util"),
            ("getrandom", "js"),
            ("serde_json", "arbitrary_precision"),
        )
        for name, feature in cases:
            with self.subTest(dependency=name, feature=feature):
                metadata = valid_metadata()
                for entry in find(metadata, REGISTRY)["dependencies"]:
                    if entry["name"] == name and entry.get("kind") is None:
                        entry["features"] = sorted(set(entry.get("features", [])) | {feature})
                errors = CHECKER.check_policy(REPO, self.policy, metadata)
                self.assertTrue(
                    any(
                        error.startswith("[dependency-feature-not-allowed]")
                        and f"{REGISTRY} -> {name} enables {feature}" in error
                        for error in errors
                    ),
                    errors,
                )

    def test_registry_contracts_edge_must_disable_default_features(self) -> None:
        metadata = valid_metadata()
        for entry in find(metadata, REGISTRY)["dependencies"]:
            if entry["name"] == CONTRACTS:
                entry["uses_default_features"] = True
        errors = CHECKER.check_policy(REPO, self.policy, metadata)
        self.assertTrue(
            any(
                f"default-features = false: {REGISTRY} -> {CONTRACTS}" in error
                for error in errors
            ),
            errors,
        )

    def test_contracts_closure_refuses_every_concrete_capability(self) -> None:
        """The one contract-crate edge cannot become a transitive reach.

        The closure is default-deny over ALL packages, external crates
        included, so an external store or HTTP stack is refused exactly like a
        tracedecay-* one.
        """
        for target in FORBIDDEN_CONTRACTS_EDGES:
            with self.subTest(target=target):
                metadata = valid_metadata()
                find(metadata, CONTRACTS)["dependencies"].append(dependency(target))
                errors = CHECKER.check_policy(REPO, self.policy, metadata)
                self.assertTrue(
                    any(
                        f"{CONTRACTS} -> {target} ({CONTRACTS_RULE})" in error
                        for error in errors
                    ),
                    f"{target} escaped the capability closure rule: {errors}",
                )
                self.assertTrue(
                    any(
                        f"{CONTRACTS} -> {target} (package-contract:{CONTRACTS})" in error
                        for error in errors
                    ),
                    f"{target} escaped the contracts package allowlist: {errors}",
                )

    def test_missing_required_registry_dependency_fails(self) -> None:
        metadata = valid_metadata()
        registry = find(metadata, REGISTRY)
        registry["dependencies"] = [
            entry
            for entry in registry["dependencies"]
            if entry["name"] != "tracedecay-memory-provider-native"
        ]
        errors = CHECKER.check_policy(REPO, self.policy, metadata)
        self.assertTrue(
            any(
                "required dependency is missing: "
                f"{REGISTRY} -> tracedecay-memory-provider-native" in error
                for error in errors
            ),
            errors,
        )

    # ------------------------------------------------------------------
    # source layer
    # ------------------------------------------------------------------
    def test_source_contract_is_mandatory_and_cannot_be_deleted_from_policy(self) -> None:
        """Deleting the source contract fails the gate; the need is derived.

        The obligation is computed from Cargo metadata -- an executor crate, or
        a crate that declares an optional dependency and can therefore export
        more than its contract surface under feature unification -- so no policy
        edit can remove it.
        """
        policy = copy.deepcopy(self.policy)
        policy["source_contracts"] = []
        metadata = valid_metadata()
        # Hypothetical regression: contracts gain an optional capability carrier.
        find(metadata, CONTRACTS)["dependencies"].append(
            dependency("gix", optional=True, uses_default_features=False)
        )
        errors = CHECKER.check_policy(REPO, policy, metadata)
        self.assertTrue(
            any(
                f"{REGISTRY} allows production dependency {CONTRACTS} but declares no"
                " source contract" in error
                for error in errors
            ),
            errors,
        )
        self.assertTrue(
            any(
                f"{REGISTRY} allows production dependency tokio but declares no source"
                " contract" in error
                for error in errors
            ),
            errors,
        )

    def test_dropping_one_import_entry_still_fails(self) -> None:
        policy = copy.deepcopy(self.policy)
        for contract in policy["source_contracts"]:
            contract["allowed_imports"].pop("tracedecay_contracts", None)
        metadata = valid_metadata()
        # Exercise metadata-derived obligations independently of today's closure.
        find(metadata, CONTRACTS)["dependencies"].append(
            dependency("gix", optional=True, uses_default_features=False)
        )
        errors = CHECKER.check_policy(REPO, policy, metadata)
        self.assertTrue(
            any(
                f"{REGISTRY} source contract has no allowed_imports entry for"
                f" {CONTRACTS}" in error
                for error in errors
            ),
            errors,
        )

    def test_edge_contract_cannot_replace_a_mandatory_full_contract(self) -> None:
        policy = copy.deepcopy(self.policy)
        registry = next(row for row in policy["source_contracts"] if row["package"] == REGISTRY)
        policy["source_contracts"].remove(registry)
        policy.setdefault("edge_source_contracts", []).append({
            "package": REGISTRY,
            "dependency": CONTRACTS,
            "allowed_imports": registry["allowed_imports"]["tracedecay_contracts"],
        })
        metadata = valid_metadata()
        find(metadata, CONTRACTS)["dependencies"].append(dependency("gix", optional=True))
        errors = CHECKER.check_policy(REPO, policy, metadata)
        for target in (CONTRACTS, "tokio"):
            self.assertTrue(any(f"{REGISTRY} allows production dependency {target} but declares no source contract" in error for error in errors), errors)

    def test_additional_edge_contract_preserves_executor_and_forbidden_symbol_floors(self) -> None:
        policy = copy.deepcopy(self.policy)
        registry = next(row for row in policy["source_contracts"] if row["package"] == REGISTRY)
        policy.setdefault("edge_source_contracts", []).append({
            "package": REGISTRY,
            "dependency": CONTRACTS,
            "allowed_imports": registry["allowed_imports"]["tracedecay_contracts"],
        })
        for snippet, diagnostic in (
            ("fn probe() { let _ = tokio::spawn(async {}); }", "[executor-site-not-reviewed]"),
            ("fn probe() { let _ = rusqlite::Connection::open_in_memory(); }", "forbidden source symbol"),
        ):
            with self.subTest(snippet=snippet):
                errors = self.check_with_source(self.append_source(snippet), policy)
                self.assertTrue(any(diagnostic in error for error in errors), errors)

    def test_registry_cannot_import_the_unified_native_git_reader(self) -> None:
        """Reject a hypothetical reader export, including one enabled by unification.

        Production contracts do not export this reader; the source allowlist
        must still refuse it if a future capability feature introduces it.
        """
        errors = self.check_with_source(
            self.append_source("use tracedecay_contracts::NativeHistoricalBlobReaderV1;")
        )
        self.assertTrue(
            any(
                "forbidden source import" in error
                and "NativeHistoricalBlobReaderV1" in error
                for error in errors
            ),
            errors,
        )

    def test_registry_import_allowlist_is_exact(self) -> None:
        for snippet in (
            "use tracedecay_contracts::code_index::CodeIndexPort;",
            "use tracedecay_contracts::store::SessionStore;",
            "use tracedecay_contracts::{memory::CognitiveRecallPort, git::GitReader};",
            "fn probe() { let _ = tracedecay_contracts::host::HostRuntime::new(); }",
        ):
            with self.subTest(snippet=snippet):
                errors = self.check_with_source(self.append_source(snippet))
                self.assertTrue(
                    any("forbidden source import" in error for error in errors),
                    f"{snippet} was admitted: {errors}",
                )

    def test_registry_glob_import_can_never_satisfy_the_allowlist(self) -> None:
        errors = self.check_with_source(
            self.append_source("use tracedecay_contracts::*;")
        )
        self.assertTrue(
            any(
                "forbidden source import" in error and "tracedecay_contracts::*" in error
                for error in errors
            ),
            errors,
        )

    def test_blocking_offload_import_is_refused(self) -> None:
        errors = self.check_with_source(
            self.append_source(
                "fn dormant_offload() { let _ = tokio::task::spawn_blocking(|| ()); }"
            )
        )
        self.assertEqual(len(errors), 1, errors)
        self.assertTrue(errors[0].startswith("[source-import-not-allowed]"), errors)
        self.assertIn("tokio::task::spawn_blocking", errors[0])

    def test_new_executor_call_site_is_refused_anywhere_in_the_crate(self) -> None:
        """An allowed executor import still needs a reviewed call site.

        This is the dormancy bound the gate can actually enforce: a spawn added
        on any path -- including one reachable while the composition is
        Disabled -- needs no Cargo change, so the source contract pins every
        executor entry point to its exact reviewed enclosing function.
        """
        for snippet in (
            "fn dormant_background_sweeper() { let _ = tokio::spawn(async {}); }",
            "impl Anything { fn poll_forever(&self) { let _ = tokio::spawn(async {}); } }",
        ):
            with self.subTest(snippet=snippet):
                errors = self.check_with_source(self.append_source(snippet))
                self.assertEqual(len(errors), 1, errors)
                self.assertTrue(
                    errors[0].startswith("[executor-site-not-reviewed]"),
                    f"expected executor-site rejection for {snippet}: {errors}",
                )
                self.assertIn("tokio::spawn", errors[0])

    def test_executor_item_must_be_pinned_to_call_sites(self) -> None:
        policy = copy.deepcopy(self.policy)
        for contract in policy["source_contracts"]:
            contract["executor_call_sites"].pop("tokio::spawn", None)
        errors = CHECKER.check_policy(REPO, policy, valid_metadata())
        self.assertTrue(
            any(
                "admits executor item tokio::spawn without pinning it" in error
                for error in errors
            ),
            errors,
        )

    def test_forbidden_capability_symbols_are_refused_in_source(self) -> None:
        for label, snippet in FORBIDDEN_SOURCE_SNIPPETS:
            with self.subTest(capability=label):
                errors = self.check_with_source(self.append_source(snippet))
                self.assertTrue(
                    any("forbidden source symbol" in error for error in errors),
                    f"{label} was admitted: {errors}",
                )

    def test_forbidden_symbol_floor_survives_an_emptied_policy_list(self) -> None:
        """The capability floor lives in the checker, not the policy."""
        policy = copy.deepcopy(self.policy)
        for contract in policy["source_contracts"]:
            contract["forbidden_source_symbols"] = []
        errors = self.check_with_source(
            self.append_source("fn probe() { let _ = rusqlite::Connection::open_in_memory(); }"),
            policy=policy,
        )
        self.assertTrue(
            any("forbidden source symbol" in error for error in errors), errors
        )

    def test_comments_are_not_scanned_as_imports(self) -> None:
        """Prose naming a banned symbol is not a violation; only code is."""
        errors = self.check_with_source(
            self.append_source(
                "// Never call Runtime::new, std::thread::spawn, or gix::open here.\n"
                "/* NativeHistoricalBlobReaderV1 and rusqlite are banned in this crate. */"
            )
        )
        self.assertEqual(errors, [])

    def test_stale_import_allowance_is_refused(self) -> None:
        policy = copy.deepcopy(self.policy)
        for contract in policy["source_contracts"]:
            if "tracedecay_contracts" not in contract["allowed_imports"]:
                continue
            contract["allowed_imports"]["tracedecay_contracts"].append(
                "tracedecay_contracts::NeverUsedContractType"
            )
        errors = CHECKER.check_policy(REPO, policy, valid_metadata())
        self.assertTrue(
            any("stale source import allowance" in error for error in errors), errors
        )

    def test_missing_crate_source_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            errors = CHECKER.check_policy(
                REPO, self.policy, valid_metadata(), source_repo=Path(directory)
            )
        self.assertTrue(
            any("no readable crate source" in error for error in errors), errors
        )

    # ------------------------------------------------------------------
    # exceptions
    # ------------------------------------------------------------------
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
        ncm = find(metadata, "tracedecay-memory-provider-ncm")
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
            # The ADR tree is staged in a temporary directory; crate source is
            # still read from the real repository.
            self.assertEqual(
                CHECKER.check_policy(repo, policy, metadata, source_repo=REPO), []
            )

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
            errors = CHECKER.check_policy(
                repo, policy, valid_metadata(), source_repo=REPO
            )
            self.assertTrue(any("unused dependency exception" in error for error in errors))


class EdgeSourceContractTest(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.source = self.root / "crates" / RUNTIME / "src" / "lib.rs"
        self.source.parent.mkdir(parents=True)
        self.source.write_text("use tracedecay_memory_provider_api::RecordedValidity;\n", encoding="utf-8")
        self.edge = {
            "package": RUNTIME,
            "dependency": API,
            "allowed_imports": ["tracedecay_memory_provider_api::RecordedValidity"],
        }
        self.policy = {"schema_version": 1, "edge_source_contracts": [self.edge]}
        self.metadata = {
            "packages": [
                {**package(RUNTIME, [API]), "id": RUNTIME},
                {**package(API, []), "id": API},
            ],
            "workspace_members": [RUNTIME, API],
        }

    def check(self) -> list[str]:
        return CHECKER.check_policy(self.root, self.policy, self.metadata)

    def test_runtime_can_own_filesystem_process_and_database_beside_numeric_import(self) -> None:
        self.source.write_text(
            self.source.read_text(encoding="utf-8")
            + 'fn worker() { let _ = std::fs::read("x"); '
            + 'let _ = std::process::Command::new("worker"); '
            + 'let _ = rusqlite::Connection::open_in_memory(); }\n',
            encoding="utf-8",
        )
        self.assertEqual(self.check(), [])

    def test_unlisted_host_authority_and_glob_imports_are_rejected(self) -> None:
        for snippet in (
            "use tracedecay_memory_provider_api::{RecordedValidity, SourceIdentity};",
            "fn worker() { let _ = tracedecay_memory_provider_api::HostAuthority::new(); }",
            "use tracedecay_memory_provider_api::*;",
        ):
            with self.subTest(snippet=snippet):
                self.source.write_text(snippet, encoding="utf-8")
                self.assertIn("[source-import-not-allowed]", "\n".join(self.check()))

    def test_stale_numeric_allowance_is_rejected(self) -> None:
        self.edge["allowed_imports"].append("tracedecay_memory_provider_api::TemporalEligibility")
        self.assertIn("stale source import allowance", "\n".join(self.check()))

    def test_contract_requires_a_declared_normal_edge(self) -> None:
        for dependencies in ([], [dependency(API, kind="dev")], [dependency(API, kind="build")]):
            with self.subTest(dependencies=dependencies):
                find(self.metadata, RUNTIME)["dependencies"] = dependencies
                self.assertIn("requires a declared normal dependency edge", "\n".join(self.check()))

    def test_target_specific_renamed_normal_edge_uses_its_declared_root(self) -> None:
        find(self.metadata, RUNTIME)["dependencies"] = [
            {**dependency(API), "rename": "numeric_api", "target": "cfg(unix)"}
        ]
        self.edge["allowed_imports"] = ["numeric_api::RecordedValidity"]
        self.source.write_text("use numeric_api::RecordedValidity;", encoding="utf-8")
        self.assertEqual(self.check(), [])

    def test_dependency_must_be_a_workspace_package(self) -> None:
        self.metadata["workspace_members"].remove(API)
        self.assertIn("must name an existing package and workspace dependency", "\n".join(self.check()))

    def test_invalid_import_allowances_fail_closed(self) -> None:
        for paths in (
            None,
            [],
            ["tracedecay_memory_provider_api::*"],
            ["other_api::RecordedValidity"],
            ["tracedecay_memory_provider_api::RecordedValidity"] * 2,
            [{}],
        ):
            with self.subTest(paths=paths):
                self.edge["allowed_imports"] = paths
                self.assertIn("distinct exact item paths rooted at its declared dependency", "\n".join(self.check()))

    def test_duplicate_edge_contract_is_rejected(self) -> None:
        self.policy["edge_source_contracts"].append(copy.deepcopy(self.edge))
        self.assertIn("duplicate edge source contract", "\n".join(self.check()))

    def test_malformed_edge_contracts_are_rejected(self) -> None:
        for rows, diagnostic in (
            ({}, "policy edge_source_contracts must be an array"),
            ([None], "edge source contract must be an object"),
            ([{"package": RUNTIME}], "package and dependency must name exact packages"),
        ):
            with self.subTest(rows=rows):
                self.policy["edge_source_contracts"] = rows
                self.assertIn(diagnostic, "\n".join(self.check()))


if __name__ == "__main__":
    unittest.main()
