#!/usr/bin/env python3
"""Focused positive and negative tests for memory dependency direction.

Two layers are locked down here, because the gate has two layers:

* Cargo metadata: exact names split by dependency kind, and an exact allowed
  feature set per production edge.
* Crate source: an exact item-path import allowlist, executor call sites pinned
  by enclosing function, and the checker's forbidden-capability-symbol floor.

The source layer exists because the metadata layer provably cannot finish the
job -- `tracedecay-application` mounts a gix reader behind its optional
`native-git` feature and the production root enables it, so Cargo feature
unification puts that reader in scope for the registry no matter what the
registry's own manifest requests. The tests below assert that the reader, and
every other unlisted item, is refused at the source layer.
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
APPLICATION = "tracedecay-application"
APPLICATION_RULE = "application-contract-crate-capability-closure"

# Every concrete capability family the composition registry must never reach,
# named exactly rather than matched by a glob. Each one is injected on its own
# and must still be refused by composition-registry-is-narrow.
FORBIDDEN_REGISTRY_EDGES = (
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
FORBIDDEN_APPLICATION_EDGES = (
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
            dependency("serde_json"),
            dependency("sha2"),
            dependency("thiserror"),
            dependency("tiktoken-rs"),
            dependency("tokio", features=["rt"], uses_default_features=False),
            dependency(APPLICATION, features=[], uses_default_features=False),
            dependency("tracedecay-memory-fabric"),
            dependency("tracedecay-memory-provider-api"),
            dependency("tracedecay-memory-provider-native"),
            dependency("tokio", kind="dev", features=["macros", "rt"]),
            dependency("tracedecay-domain", kind="dev"),
        ],
    )


def application_package() -> dict[str, Any]:
    """The contract crate the registry adapts, as Cargo reports it.

    `gix` is optional and real. It is what makes this crate a capability
    carrier, and therefore what makes a source contract mandatory for anyone
    who depends on it.
    """
    return package_of(
        APPLICATION,
        [
            dependency(
                "gix",
                features=["revision", "blob-diff", "parallel", "sha1", "sha256", "status"],
                uses_default_features=False,
                optional=True,
            ),
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
                ["tracedecay-memory-provider-api"],
            ),
            package(
                "tracedecay-memory-provider-native",
                ["serde_json", "tracedecay-memory-provider-api"],
            ),
            package(
                "tracedecay-memory-provider-ncm",
                ["serde_json", "sha2", "tracedecay-memory-provider-api"],
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
            application_package(),
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

    def test_dependency_features_are_an_exact_allowlist_not_a_denylist(self) -> None:
        """An unlisted feature is refused even when nobody predicted its name.

        `chrono/clock` is the concrete case the policy rationale depends on (no
        ambient clock in the registry); the invented names stand for a future
        capability feature that no denylist could have enumerated.
        """
        cases = (
            ("chrono", "clock"),
            ("chrono", "now"),
            (APPLICATION, "native-git"),
            (APPLICATION, "native-sqlite-store"),
            (APPLICATION, "embedded-index"),
            ("tokio", "net"),
            ("tokio", "process"),
            ("tokio", "fs"),
            ("tokio", "signal"),
            ("tokio", "rt-multi-thread"),
            ("tokio", "full"),
            ("tokio", "time"),
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
                        f"unreviewed dependency feature: {REGISTRY} -> {name} enables"
                        f" {feature}" in error
                        for error in errors
                    ),
                    errors,
                )

    def test_registry_application_edge_must_disable_default_features(self) -> None:
        metadata = valid_metadata()
        for entry in find(metadata, REGISTRY)["dependencies"]:
            if entry["name"] == APPLICATION:
                entry["uses_default_features"] = True
        errors = CHECKER.check_policy(REPO, self.policy, metadata)
        self.assertTrue(
            any(
                f"default-features = false: {REGISTRY} -> {APPLICATION}" in error
                for error in errors
            ),
            errors,
        )

    def test_application_closure_refuses_every_concrete_capability(self) -> None:
        """The one contract-crate edge cannot become a transitive reach.

        The closure is default-deny over ALL packages, external crates
        included, so an external store or HTTP stack is refused exactly like a
        tracedecay-* one.
        """
        for target in FORBIDDEN_APPLICATION_EDGES:
            with self.subTest(target=target):
                metadata = valid_metadata()
                find(metadata, APPLICATION)["dependencies"].append(dependency(target))
                errors = CHECKER.check_policy(REPO, self.policy, metadata)
                self.assertTrue(
                    any(
                        f"{APPLICATION} -> {target} ({APPLICATION_RULE})" in error
                        for error in errors
                    ),
                    f"{target} escaped the capability closure rule: {errors}",
                )
                self.assertTrue(
                    any(
                        f"{APPLICATION} -> {target} (package-contract:{APPLICATION})" in error
                        for error in errors
                    ),
                    f"{target} escaped the application package allowlist: {errors}",
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
        errors = CHECKER.check_policy(REPO, policy, valid_metadata())
        self.assertTrue(
            any(
                f"{REGISTRY} allows production dependency {APPLICATION} but declares no"
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
            contract["allowed_imports"].pop("tracedecay_application", None)
        errors = CHECKER.check_policy(REPO, policy, valid_metadata())
        self.assertTrue(
            any(
                f"{REGISTRY} source contract has no allowed_imports entry for"
                f" {APPLICATION}" in error
                for error in errors
            ),
            errors,
        )

    def test_registry_cannot_import_the_unified_native_git_reader(self) -> None:
        """The reader Cargo unification makes visible is refused at the source.

        This is the case a manifest-only gate cannot catch: the production root
        enables tracedecay-application/native-git, so NativeHistoricalBlobReaderV1
        is in scope for the registry whatever its own edge requests.
        """
        errors = self.check_with_source(
            self.append_source("use tracedecay_application::NativeHistoricalBlobReaderV1;")
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
            "use tracedecay_application::code_index::CodeIndexPort;",
            "use tracedecay_application::store::SessionStore;",
            "use tracedecay_application::{memory::CognitiveRecallPort, git::GitReader};",
            "fn probe() { let _ = tracedecay_application::host::HostRuntime::new(); }",
        ):
            with self.subTest(snippet=snippet):
                errors = self.check_with_source(self.append_source(snippet))
                self.assertTrue(
                    any("forbidden source import" in error for error in errors),
                    f"{snippet} was admitted: {errors}",
                )

    def test_registry_glob_import_can_never_satisfy_the_allowlist(self) -> None:
        errors = self.check_with_source(
            self.append_source("use tracedecay_application::*;")
        )
        self.assertTrue(
            any(
                "forbidden source import" in error and "tracedecay_application::*" in error
                for error in errors
            ),
            errors,
        )

    def test_new_executor_call_site_is_refused_anywhere_in_the_crate(self) -> None:
        """No new task-spawning or blocking-offload site may appear unreviewed.

        This is the dormancy bound the gate can actually enforce: a spawn added
        on any path -- including one reachable while the composition is
        Disabled -- needs no Cargo change, so the source contract pins every
        executor entry point to its exact reviewed enclosing function.
        """
        for snippet in (
            "fn dormant_background_sweeper() { let _ = tokio::spawn(async {}); }",
            "fn dormant_offload() { let _ = tokio::task::spawn_blocking(|| ()); }",
            "impl Anything { fn poll_forever(&self) { let _ = tokio::spawn(async {}); } }",
        ):
            with self.subTest(snippet=snippet):
                errors = self.check_with_source(self.append_source(snippet))
                self.assertTrue(
                    any("unreviewed executor call site" in error for error in errors),
                    f"{snippet} was admitted: {errors}",
                )

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
            contract["allowed_imports"]["tracedecay_application"].append(
                "tracedecay_application::NeverUsedContractType"
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


if __name__ == "__main__":
    unittest.main()
