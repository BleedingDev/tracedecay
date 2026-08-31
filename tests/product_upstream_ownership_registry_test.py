#!/usr/bin/env python3
"""Behavioral tests for the schema-v2 upstream ownership registry."""

from __future__ import annotations

import copy
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
CHECKER = REPO / "scripts/product/check-upstream-ownership-registry.py"
SCHEMA = REPO / "product/upstream/convergence-map.schema.json"
FLOOR = "08fbe33a7c7f403191fd5d6e356c7b6681b96403"
OLD_FLOOR = "0000000000000000000000000000000000000001"
KNOWN_BEADS = {
    "tdmem-0301",
    "tdmem-0304",
    "tdmem-0305",
    "tdmem-0307",
    "tdmem-0308",
}


def area(
    area_id: str,
    ownership_class: str,
    patterns: list[str],
    touch_points: list[str],
    *,
    status: str = "active",
    feature: str | None = None,
) -> dict[str, Any]:
    owner = "BleedingDev" if ownership_class == "product_owned" else "ScriptedAlchemy"
    return {
        "id": area_id,
        "status": status,
        "owner": owner,
        "ownership_class": ownership_class,
        "feature": feature or area_id.replace("_", "-"),
        "path_patterns": patterns,
        "touch_points": touch_points,
        "bead_ids": ["tdmem-0308"],
        "rationale": "This bounded area records one explicit M2 ownership decision.",
        "semantic_invariants": [
            "Every classified path preserves the declared canonical ownership boundary."
        ],
        "tests": ["python3 tests/product_upstream_ownership_registry_test.py"],
        "last_verified_upstream_sha": FLOOR,
        "upstreamability": {
            "kind": (
                "product_only"
                if ownership_class == "product_owned"
                else "minimal_mount"
            ),
            "rationale": "The selected path keeps the upstream patch surface narrow and removable.",
        },
    }


def entry(path: str, area_id: str, touch_point: str) -> dict[str, Any]:
    return {
        "path": path,
        "area_id": area_id,
        "owner": "BleedingDev",
        "upstream_owner": "ScriptedAlchemy",
        "touch_point": touch_point,
        "rationale": "This exact upstream edit mounts product behavior through a bounded seam.",
        "semantic_invariants": [
            "Removing this exact mount restores the unchanged upstream behavior completely."
        ],
        "verification": ["git diff --check"],
        "tests": ["cargo test -p tracedecay-memory-provider-registry"],
        "bead_ids": ["tdmem-0307", "tdmem-0308"],
        "line_budget": 80,
        "rebase_or_removal_plan": "Remove only this exact mount and retain all unrelated upstream behavior.",
        "status": "active",
        "last_verified_upstream_sha": FLOOR,
        "upstreamability": {
            "kind": "minimal_mount",
            "rationale": "The exact edit can be proposed upstream or removed without widening the seam.",
        },
    }


def base_schema() -> dict[str, Any]:
    return json.loads(SCHEMA.read_text(encoding="utf-8"))


def base_sync_policy() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "schema_version": 1,
        "authority": "product-owned",
        "ownership": {
            "sync_owner": "BleedingDev",
            "review_owner": "ScriptedAlchemy",
            "product_patch_owners": ["BleedingDev"],
        },
        "remotes": {
            "product": {
                "name": "origin",
                "repository": "BleedingDev/tracedecay",
            },
            "upstream": {
                "name": "upstream",
                "repository": "ScriptedAlchemy/tracedecay",
            },
        },
        "floor": {
            "metadata": "product/upstream/tracedecay-v2-pr707.json",
            "pull_request": 707,
            "sha": FLOOR,
        },
    }


def base_metadata() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "schema_version": 1,
        "source": {
            "repository": "ScriptedAlchemy/tracedecay",
            "pull_request": 707,
        },
        "product": {
            "repository": "BleedingDev/tracedecay",
            "branch": "feat/pluggable-memory-providers-v2",
        },
        "pinned_floor": {
            "sha": FLOOR,
            "must_be_ancestor_of_product_head": True,
        },
    }


def base_policy() -> dict[str, Any]:
    product_patterns = [
        ".beads/**",
        "product/**",
        "scripts/product/**",
        "scripts/check-product-upstream-floor.py",
        "tests/product_*",
        ".github/workflows/apply-beads-operation.yml",
        ".github/workflows/materialize-beads.yml",
        ".github/workflows/product-*.yml",
        "crates/tracedecay-memory-provider-api/**",
        "crates/tracedecay-memory-fabric/**",
        "crates/tracedecay-memory-provider-registry/**",
        "crates/tracedecay-memory-provider-native/**",
        "crates/tracedecay-memory-provider-ncm/**",
        "crates/tracedecay-memory-observation/**",
        "crates/tracedecay-memory-context/**",
        "crates/tracedecay-memory-conformance/**",
        "crates/tracedecay/tests/product_memory_provider/**",
        "crates/tracedecay/tests/product_memory_provider_*.rs",
        "crates/tracedecay/src/daemon/retained_owner/native_provider.rs",
        "crates/tracedecay/src/daemon/retained_owner/native_provider_tests.rs",
        "crates/tracedecay/src/daemon/retained_owner/native_provider_parity_tests.rs",
    ]
    return {
        "schema_version": 1,
        "bead_id": "tdmem-0301",
        "policy_revision": "patch-footprint.v1",
        "administrative_paths_excluded_from_footprint": [".codex/**"],
        "product_owned_paths": product_patterns,
        "upstream_floor": {
            "metadata": "product/upstream/tracedecay-v2-pr707.json",
            "pull_request": 707,
            "repository": "ScriptedAlchemy/tracedecay",
            "sha": FLOOR,
        },
        "allowed_touch_points": [
            {
                "id": "workspace_wiring",
                "paths": ["Cargo.toml", "Cargo.lock"],
            },
            {
                "id": "daemon_composition_mount",
                "paths": [
                    "crates/tracedecay/src/daemon/project_composition.rs",
                    "crates/tracedecay/src/daemon/service/project_runtime.rs",
                ],
            },
            {
                "id": "configuration_registry_mount",
                "paths": ["crates/tracedecay/src/config.rs"],
            },
        ],
    }


def base_registry() -> dict[str, Any]:
    product_areas = [
        area(
            "provider_api",
            "product_owned",
            ["crates/tracedecay-memory-provider-api/**"],
            ["workspace_wiring"],
        ),
        area(
            "memory_fabric",
            "product_owned",
            ["crates/tracedecay-memory-fabric/**"],
            ["workspace_wiring"],
        ),
        area(
            "provider_registry",
            "product_owned",
            ["crates/tracedecay-memory-provider-registry/**"],
            ["workspace_wiring"],
        ),
        area(
            "native_adapter",
            "product_owned",
            ["crates/tracedecay-memory-provider-native/**"],
            ["workspace_wiring"],
        ),
        area(
            "ncm_adapter",
            "product_owned",
            ["crates/tracedecay-memory-provider-ncm/**"],
            ["workspace_wiring"],
        ),
        area(
            "memory_conformance",
            "product_owned",
            ["crates/tracedecay-memory-conformance/**"],
            ["workspace_wiring"],
        ),
        area(
            "memory_observation",
            "product_owned",
            ["crates/tracedecay-memory-observation/**"],
            ["workspace_wiring"],
            status="planned",
        ),
        area(
            "memory_context",
            "product_owned",
            ["crates/tracedecay-memory-context/**"],
            ["workspace_wiring"],
            status="planned",
        ),
        area(
            "upstream_governance",
            "product_owned",
            [
                ".beads/**",
                "product/**",
                "scripts/product/**",
                "scripts/check-product-upstream-floor.py",
                "tests/product_*",
                ".github/workflows/apply-beads-operation.yml",
                ".github/workflows/materialize-beads.yml",
                ".github/workflows/product-*.yml",
                "crates/tracedecay/tests/product_memory_provider/**",
                "crates/tracedecay/tests/product_memory_provider_*.rs",
            ],
            ["workspace_wiring"],
        ),
    ]
    upstream_areas = [
        area(
            "workspace_wiring",
            "upstream_owned",
            ["Cargo.toml", "Cargo.lock"],
            ["workspace_wiring"],
        ),
        area(
            "composition_mount",
            "upstream_owned",
            [
                "crates/tracedecay/src/daemon/project_composition.rs",
                "crates/tracedecay/src/daemon/service/project_runtime.rs",
            ],
            ["daemon_composition_mount"],
        ),
    ]
    lock_entry = entry("Cargo.lock", "workspace_wiring", "workspace_wiring")
    lock_entry["generated"] = {
        "generator_path": "rust-toolchain.toml",
        "reproduction": "cargo metadata --format-version 1 --no-deps",
        "zero_drift_check": "cargo metadata --locked --format-version 1 --no-deps",
    }
    lock_entry["upstreamability"] = {
        "kind": "generated_resolution",
        "rationale": "The lockfile entry is regenerated from the pinned workspace manifests.",
    }
    return {
        "$schema": "product/upstream/convergence-map.schema.json",
        "schema_version": 2,
        "bead_id": "tdmem-0308",
        "policy_revision": "patch-footprint.v1",
        "upstream_floor_sha": FLOOR,
        "owners": {
            "product": {
                "id": "BleedingDev",
                "repository": "BleedingDev/tracedecay",
            },
            "upstream": {
                "id": "ScriptedAlchemy",
                "repository": "ScriptedAlchemy/tracedecay",
            },
        },
        "classification_contract": {
            "path_format": "repo-relative-posix",
            "precedence": [
                "active_upstream_entry_exact_path",
                "product_area_path_pattern",
                "policy_touch_point_path",
            ],
            "ambiguous_match": "error",
            "unclassified_path": "error",
        },
        "areas": product_areas + upstream_areas,
        "entries": [
            entry("Cargo.toml", "workspace_wiring", "workspace_wiring"),
            lock_entry,
        ],
        "entry_contract": {
            "rules": [
                "Product paths resolve through exactly one active ownership area.",
                "Upstream paths require one exact active entry before authorization.",
                "Retired rows preserve history without granting current execution authority.",
            ],
            "area_status_values": ["active", "planned", "retired"],
            "entry_status_values": ["active", "retired"],
        },
    }


def bind_floor(
    floor: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    registry = base_registry()
    registry["upstream_floor_sha"] = floor
    for row in registry["areas"]:
        if row["status"] in {"active", "planned"}:
            row["last_verified_upstream_sha"] = floor
    for row in registry["entries"]:
        if row["status"] == "active":
            row["last_verified_upstream_sha"] = floor
    sync_policy = base_sync_policy()
    sync_policy["floor"]["sha"] = floor
    policy = base_policy()
    policy["upstream_floor"]["sha"] = floor
    metadata = base_metadata()
    metadata["pinned_floor"]["sha"] = floor
    return registry, policy, sync_policy, metadata


class UpstreamOwnershipRegistryTest(unittest.TestCase):
    maxDiff = None

    def run_checker(
        self,
        *,
        registry: dict[str, Any] | None = None,
        policy: dict[str, Any] | None = None,
        sync_policy: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        schema: dict[str, Any] | None = None,
        classify_paths: list[str] | None = None,
        beads_text: str | None = None,
        classify_changed_paths: bool = False,
        repo_root: Path | None = None,
    ) -> tuple[subprocess.CompletedProcess[str], dict[str, Any]]:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            artifacts = {
                "schema.json": base_schema() if schema is None else schema,
                "registry.json": base_registry() if registry is None else registry,
                "policy.json": base_policy() if policy is None else policy,
                "sync-policy.json": (
                    base_sync_policy() if sync_policy is None else sync_policy
                ),
                "metadata.json": base_metadata() if metadata is None else metadata,
            }
            paths: dict[str, Path] = {}
            for name, value in artifacts.items():
                path = temp / name
                path.write_text(
                    json.dumps(value, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                paths[name] = path
            beads = temp / "issues.jsonl"
            if beads_text is None:
                beads_text = "".join(
                    json.dumps({"id": bead_id}) + "\n"
                    for bead_id in sorted(KNOWN_BEADS)
                )
            beads.write_text(beads_text, encoding="utf-8")
            command = [
                "python3",
                str(CHECKER),
                "--repo",
                str(REPO if repo_root is None else repo_root),
                "--schema",
                str(paths["schema.json"]),
                "--map",
                str(paths["registry.json"]),
                "--policy",
                str(paths["policy.json"]),
                "--sync-policy",
                str(paths["sync-policy.json"]),
                "--floor-metadata",
                str(paths["metadata.json"]),
                "--beads",
                str(beads),
            ]
            if not classify_changed_paths:
                command.append("--skip-changed-path-classification")
            for path in classify_paths or []:
                command.extend(["--classify-path", path])
            before = sorted(path.name for path in temp.iterdir())
            result = subprocess.run(
                command, check=False, capture_output=True, text=True
            )
            after = sorted(path.name for path in temp.iterdir())
            self.assertEqual(before, after, "checker must not generate receipts or snapshots")
            try:
                payload = json.loads(result.stdout)
            except json.JSONDecodeError as exc:
                self.fail(f"checker stdout is not JSON: {exc}\n{result.stdout}\n{result.stderr}")
            return result, payload

    def assert_rejected(
        self,
        marker: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        result, payload = self.run_checker(**kwargs)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(payload["ok"])
        self.assertIn(marker, "\n".join(payload["errors"]))
        return payload

    def test_complete_v2_fixture_and_classifications_pass(self) -> None:
        paths = [
            "crates/tracedecay-memory-provider-api/src/lib.rs",
            "crates/tracedecay-memory-fabric/src/lib.rs",
            "crates/tracedecay-memory-provider-registry/src/lib.rs",
            "crates/tracedecay-memory-provider-native/src/lib.rs",
            "crates/tracedecay-memory-provider-ncm/src/lib.rs",
            "crates/tracedecay-memory-conformance/src/lib.rs",
            "product/upstream/convergence-map.json",
            "scripts/product/check-upstream-ownership-registry.py",
            "tests/product_upstream_ownership_registry_test.py",
            "Cargo.toml",
            "Cargo.lock",
        ]
        result, payload = self.run_checker(classify_paths=paths)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["counts"]["areas"], {
            "active": 9,
            "planned": 2,
            "retired": 0,
            "total": 11,
        })
        self.assertEqual(payload["counts"]["entries"]["active"], 2)
        self.assertEqual(payload["counts"]["classifications"], {
            "product_area": 9,
            "upstream_entry": 2,
            "total": 11,
        })

    def test_schema_v1_fixture_fails_explicitly_without_mutation(self) -> None:
        registry = base_registry()
        registry["schema_version"] = 1
        self.assert_rejected("schema v1 must be migrated", registry=registry)

    def test_changed_paths_are_classified_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = Path(temp_dir) / "repo"
            repo.mkdir()
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            subprocess.run(
                ["git", "-C", str(repo), "config", "user.email", "test@example.com"],
                check=True,
            )
            subprocess.run(
                ["git", "-C", str(repo), "config", "user.name", "Fixture"],
                check=True,
            )
            test_path = repo / "tests/product_upstream_ownership_registry_test.py"
            test_path.parent.mkdir(parents=True)
            test_path.write_text("# fixture\n", encoding="utf-8")
            (repo / "README.md").write_text("floor\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(repo),
                    "-c",
                    "core.hooksPath=/dev/null",
                    "commit",
                    "-q",
                    "-m",
                    "floor",
                ],
                check=True,
            )
            floor = subprocess.run(
                ["git", "-C", str(repo), "rev-parse", "HEAD"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            changed = repo / "crates/tracedecay-memory-provider-registry/src/lib.rs"
            changed.parent.mkdir(parents=True)
            changed.write_text("pub struct Fixture;\n", encoding="utf-8")
            excluded = repo / ".codex" / "plans" / "working.md"
            excluded.parent.mkdir(parents=True)
            excluded.write_text("administrative plan\n", encoding="utf-8")
            registry, policy, sync_policy, metadata = bind_floor(floor)
            result, payload = self.run_checker(
                repo_root=repo,
                registry=registry,
                policy=policy,
                sync_policy=sync_policy,
                metadata=metadata,
                classify_changed_paths=True,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(
                payload["counts"]["changed_paths"],
                {"classified": 1, "total": 2},
            )
            registry["areas"] = [
                row for row in registry["areas"] if row["id"] != "provider_registry"
            ]
            self.assert_rejected(
                "unclassified",
                repo_root=repo,
                registry=registry,
                policy=policy,
                sync_policy=sync_policy,
                metadata=metadata,
                classify_changed_paths=True,
            )

    def test_administrative_paths_follow_policy_and_nearby_paths_require_classification(
        self,
    ) -> None:
        result, payload = self.run_checker(
            classify_paths=[".codex/plans/working.md"]
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(payload["classifications"], [])

        policy = base_policy()
        policy["administrative_paths_excluded_from_footprint"] = [".agent-state/**"]
        result, payload = self.run_checker(
            policy=policy,
            classify_paths=[".agent-state/cache.json"],
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(payload["classifications"], [])
        self.assert_rejected(
            "unclassified by the M2 ownership registry",
            policy=policy,
            classify_paths=[".codex/plans/working.md"],
        )
        self.assert_rejected(
            "unclassified by the M2 ownership registry",
            classify_paths=[".codex-adjacent/working.md"],
        )

    def test_malformed_administrative_exclusions_fail_closed(self) -> None:
        malformed = base_policy()
        malformed.pop("administrative_paths_excluded_from_footprint")
        payload = self.assert_rejected(
            "must be an array",
            policy=malformed,
            classify_paths=[".codex/plans/working.md"],
        )
        self.assertIn(
            "unclassified by the M2 ownership registry",
            "\n".join(payload["errors"]),
        )

        for exclusions, marker in (
            ([], "must not be empty"),
            ([".codex/**", "../outside/**"], "normalized repo-relative POSIX"),
            ([".codex/**", 7], "must be a non-empty string"),
        ):
            with self.subTest(exclusions=exclusions):
                policy = base_policy()
                policy["administrative_paths_excluded_from_footprint"] = exclusions
                payload = self.assert_rejected(
                    marker,
                    policy=policy,
                    classify_paths=[".codex/plans/working.md"],
                )
                self.assertIn(
                    "unclassified by the M2 ownership registry",
                    "\n".join(payload["errors"]),
                )

    def test_root_is_closed_and_all_fields_are_required(self) -> None:
        for field in sorted(base_registry()):
            with self.subTest(missing=field):
                registry = base_registry()
                del registry[field]
                self.assert_rejected("missing required fields", registry=registry)
        for legacy in ("snapshot", "receipt", "generated_at", "observed_state"):
            with self.subTest(unknown=legacy):
                registry = base_registry()
                registry[legacy] = {}
                self.assert_rejected("contains unknown fields", registry=registry)

    def test_nested_shapes_are_closed(self) -> None:
        mutations = []
        for label, mutate in (
            ("owners", lambda row: row["owners"].__setitem__("unexpected", True)),
            ("owner", lambda row: row["owners"]["product"].__setitem__("unexpected", True)),
            ("classification", lambda row: row["classification_contract"].__setitem__("unexpected", True)),
            ("area", lambda row: row["areas"][0].__setitem__("unexpected", True)),
            ("upstreamability", lambda row: row["areas"][0]["upstreamability"].__setitem__("unexpected", True)),
            ("entry", lambda row: row["entries"][0].__setitem__("unexpected", True)),
            ("generated", lambda row: row["entries"][1]["generated"].__setitem__("unexpected", True)),
            ("entry_contract", lambda row: row["entry_contract"].__setitem__("unexpected", True)),
        ):
            registry = base_registry()
            mutate(registry)
            mutations.append((label, registry))
        for label, registry in mutations:
            with self.subTest(shape=label):
                self.assert_rejected("contains unknown fields", registry=registry)

    def test_nested_schema_shapes_cannot_drift_from_runtime_contract(self) -> None:
        schema = base_schema()
        schema["$defs"]["area"]["additionalProperties"] = True
        self.assert_rejected("area definition must be closed", schema=schema)
        schema = base_schema()
        schema["$defs"]["entry"]["properties"]["path"] = {
            "$ref": "#/$defs/relative_path"
        }
        self.assert_rejected("entry path must use exact_path", schema=schema)

    def test_schema_version_is_exact_integer_and_contract_arrays_are_ordered(self) -> None:
        for value in (True, 2.0, "2", 1, 3):
            with self.subTest(schema_version=value):
                registry = base_registry()
                registry["schema_version"] = value
                self.assert_rejected("must be integer 2", registry=registry)
        registry = base_registry()
        registry["$schema"] = "convergence-map.schema.json"
        self.assert_rejected("convergence map $schema", registry=registry)
        registry = base_registry()
        registry["bead_id"] = "tdmem-0307"
        self.assert_rejected("must be tdmem-0308", registry=registry)
        for field in ("area_status_values", "entry_status_values"):
            registry = base_registry()
            registry["entry_contract"][field].reverse()
            self.assert_rejected(f"entry_contract.{field} must equal", registry=registry)
        registry = base_registry()
        registry["entry_contract"]["rules"][0] = (
            "Product paths may match multiple active areas without producing errors."
        )
        self.assert_rejected("executable rules", registry=registry)

    def test_owner_and_repository_authorities_are_cross_checked(self) -> None:
        mutations = [
            ("owners.product.id", "OtherProduct", "sync_owner"),
            ("owners.product.repository", "Elsewhere/tracedecay", "canonical repository"),
            ("owners.upstream.id", "OtherUpstream", "review_owner"),
            ("owners.upstream.repository", "Elsewhere/tracedecay", "canonical repository"),
        ]
        for dotted, value, marker in mutations:
            with self.subTest(field=dotted):
                registry = base_registry()
                _, role, field = dotted.split(".")
                registry["owners"][role][field] = value
                self.assert_rejected(marker, registry=registry)
        registry = base_registry()
        del registry["owners"]["product"]["id"]
        self.assert_rejected("owners.product missing required fields", registry=registry)
        registry = base_registry()
        registry["areas"][0]["owner"] = "ScriptedAlchemy"
        self.assert_rejected("canonical product_owned owner", registry=registry)
        registry = base_registry()
        registry["entries"][0]["upstream_owner"] = "BleedingDev"
        self.assert_rejected("canonical upstream owner", registry=registry)

    def test_floor_and_policy_identity_must_match_canonical_metadata(self) -> None:
        registry = base_registry()
        registry["upstream_floor_sha"] = OLD_FLOOR
        self.assert_rejected("canonical pinned floor", registry=registry)
        sync_policy = base_sync_policy()
        sync_policy["floor"]["sha"] = OLD_FLOOR
        self.assert_rejected("sync policy.floor.sha", sync_policy=sync_policy)
        policy = base_policy()
        policy["upstream_floor"]["sha"] = OLD_FLOOR
        self.assert_rejected("patch policy.upstream_floor.sha", policy=policy)
        registry = base_registry()
        registry["policy_revision"] = "patch-footprint.v2"
        self.assert_rejected("must equal patch policy revision", registry=registry)

    def test_enums_references_and_identity_types_are_exact(self) -> None:
        registry = base_registry()
        registry["areas"][0]["status"] = "ACTIVE"
        self.assert_rejected("areas[0].status must be one of", registry=registry)
        registry = base_registry()
        registry["areas"][0]["ownership_class"] = "product"
        self.assert_rejected("ownership_class is invalid", registry=registry)
        registry = base_registry()
        registry["areas"][0]["upstreamability"]["kind"] = "temporary"
        self.assert_rejected("upstreamability.kind is invalid", registry=registry)
        registry = base_registry()
        registry["entries"][0]["status"] = "planned"
        self.assert_rejected("entries[0].status must be one of", registry=registry)
        registry = base_registry()
        registry["entries"][0]["area_id"] = "unknown_area"
        self.assert_rejected("references unknown area", registry=registry)
        registry = base_registry()
        registry["areas"][0]["touch_points"] = ["unknown_mount"]
        self.assert_rejected("touch_points references unknown", registry=registry)

    def test_every_area_and_entry_field_is_required(self) -> None:
        for field in sorted(base_registry()["areas"][0]):
            with self.subTest(area_field=field):
                registry = base_registry()
                del registry["areas"][0][field]
                self.assert_rejected("missing required fields", registry=registry)
        for field in sorted(base_registry()["entries"][0]):
            with self.subTest(entry_field=field):
                registry = base_registry()
                del registry["entries"][0][field]
                self.assert_rejected("missing required fields", registry=registry)

    def test_paths_are_lexically_normalized_and_entry_paths_are_exact(self) -> None:
        bad_paths = [
            "/Cargo.toml",
            "../Cargo.toml",
            "product/../Cargo.toml",
            "./Cargo.toml",
            "Cargo//toml",
            "Cargo.toml/",
            "product\\x",
            "C:\\x",
            "Cargo.toml\x00",
        ]
        for path in bad_paths:
            with self.subTest(area_path=repr(path)):
                registry = base_registry()
                registry["areas"][0]["path_patterns"] = [path]
                self.assert_rejected("normalized repo-relative POSIX", registry=registry)
        for path in ("Cargo.*", "Cargo.?oml", "Cargo.[t]oml"):
            with self.subTest(entry_path=path):
                registry = base_registry()
                registry["entries"][0]["path"] = path
                self.assert_rejected("exact path without glob", registry=registry)

    def test_global_ids_paths_and_evidence_arrays_are_unique(self) -> None:
        registry = base_registry()
        registry["areas"].append(copy.deepcopy(registry["areas"][0]))
        self.assert_rejected("duplicate id", registry=registry)
        registry = base_registry()
        registry["areas"][1]["path_patterns"] = registry["areas"][0]["path_patterns"][:]
        self.assert_rejected("area path pattern", registry=registry)
        registry = base_registry()
        registry["entries"][1]["path"] = registry["entries"][0]["path"]
        self.assert_rejected("duplicate path", registry=registry)
        for target, field in (("areas", "tests"), ("areas", "bead_ids"), ("entries", "semantic_invariants")):
            with self.subTest(target=target, field=field):
                registry = base_registry()
                registry[target][0][field].append(registry[target][0][field][0])
                self.assert_rejected("contains duplicate value", registry=registry)

    def test_beads_are_formatted_known_and_parseable(self) -> None:
        registry = base_registry()
        registry["areas"][0]["bead_ids"] = ["TDMEM-0308"]
        self.assert_rejected("malformed bead id", registry=registry)
        registry = base_registry()
        registry["entries"][0]["bead_ids"] = ["tdmem-9999"]
        self.assert_rejected("unknown bead id", registry=registry)
        self.assert_rejected(
            "invalid JSON",
            beads_text='{"id":"tdmem-0308"}\nnot-json\n',
        )

    def test_substantive_rationale_tests_sha_and_budget_are_enforced(self) -> None:
        for target in ("areas", "entries"):
            with self.subTest(target=target, evidence="rationale"):
                registry = base_registry()
                registry[target][0]["rationale"] = "TBD"
                self.assert_rejected("substantive prose", registry=registry)
            with self.subTest(target=target, evidence="tests"):
                registry = base_registry()
                registry[target][0]["tests"] = ["test -f Cargo.toml"]
                self.assert_rejected("executable behavioral test", registry=registry)
            with self.subTest(target=target, evidence="sha"):
                registry = base_registry()
                registry[target][0]["last_verified_upstream_sha"] = "A" * 40
                self.assert_rejected("lowercase 40-character SHA", registry=registry)
        registry = base_registry()
        registry["areas"][0]["rationale"] = (
            "This boundary explicitly forbids placeholder success in every provider operation."
        )
        result, payload = self.run_checker(registry=registry)
        self.assertEqual(result.returncode, 0, payload)
        for command in (
            "cargo test --no-run",
            "python3 tests/../../definitely-not-a-test.py",
            "python3 tests/definitely-not-a-test.py",
        ):
            with self.subTest(fake_test=command):
                registry = base_registry()
                registry["areas"][0]["tests"] = [command]
                self.assert_rejected("executable behavioral test", registry=registry)
        for budget in (True, 0, -1, 1.5):
            with self.subTest(budget=budget):
                registry = base_registry()
                registry["entries"][0]["line_budget"] = budget
                self.assert_rejected("positive integer", registry=registry)

    def test_active_and_planned_rows_reject_stale_floor_but_retired_history_does_not_authorize(self) -> None:
        for status in ("active", "planned"):
            with self.subTest(area_status=status):
                registry = base_registry()
                registry["areas"][0]["status"] = status
                registry["areas"][0]["last_verified_upstream_sha"] = OLD_FLOOR
                self.assert_rejected("canonical pinned floor", registry=registry)
        registry = base_registry()
        registry["entries"][0]["last_verified_upstream_sha"] = OLD_FLOOR
        self.assert_rejected("canonical pinned floor", registry=registry)
        registry = base_registry()
        registry["entries"][0]["status"] = "retired"
        registry["entries"][0]["last_verified_upstream_sha"] = OLD_FLOOR
        self.assert_rejected(
            "only a retired entry",
            registry=registry,
            classify_paths=["Cargo.toml"],
        )

    def test_product_path_requires_exactly_one_active_product_area(self) -> None:
        path = "crates/tracedecay-memory-provider-registry/src/lib.rs"
        registry = base_registry()
        registry["areas"] = [
            row for row in registry["areas"] if row["id"] != "provider_registry"
        ]
        self.assert_rejected("unclassified", registry=registry, classify_paths=[path])
        for status in ("planned", "retired"):
            registry = base_registry()
            next(row for row in registry["areas"] if row["id"] == "provider_registry")["status"] = status
            self.assert_rejected("unclassified", registry=registry, classify_paths=[path])
        registry = base_registry()
        overlap = area(
            "registry_source",
            "product_owned",
            ["crates/tracedecay-memory-provider-registry/src/**"],
            ["workspace_wiring"],
        )
        registry["areas"].append(overlap)
        self.assert_rejected("ambiguously matches active product areas", registry=registry, classify_paths=[path])
        overlap["status"] = "planned"
        result, payload = self.run_checker(registry=registry, classify_paths=[path])
        self.assertEqual(result.returncode, 0, payload)

    def test_broad_product_area_cannot_reclassify_upstream_tree(self) -> None:
        registry = base_registry()
        registry["areas"][0]["path_patterns"] = ["crates/**"]
        self.assert_rejected("outside canonical product-owned paths", registry=registry)
        registry = base_registry()
        registry["areas"].append(
            area(
                "host_config_bypass",
                "product_owned",
                ["crates/tracedecay/src/config.rs"],
                ["configuration_registry_mount"],
            )
        )
        self.assert_rejected("outside canonical product-owned paths", registry=registry)
        policy = base_policy()
        policy["product_owned_paths"].append("crates/**")
        registry = base_registry()
        registry["areas"].append(
            area(
                "joint_policy_bypass",
                "product_owned",
                ["crates/tracedecay/src/**"],
                ["configuration_registry_mount"],
            )
        )
        self.assert_rejected(
            "canonical product pattern set",
            registry=registry,
            policy=policy,
            classify_paths=["crates/tracedecay/src/config.rs"],
        )

    def test_upstream_path_requires_exact_active_entry_and_active_upstream_area(self) -> None:
        path = "crates/tracedecay/src/daemon/project_composition.rs"
        self.assert_rejected(
            "lacks an active exact convergence entry",
            classify_paths=[path],
        )
        registry = base_registry()
        registry["entries"].append(entry(path, "composition_mount", "daemon_composition_mount"))
        result, payload = self.run_checker(registry=registry, classify_paths=[path])
        self.assertEqual(result.returncode, 0, payload)
        registry["entries"][-1]["status"] = "retired"
        self.assert_rejected("only a retired entry", registry=registry, classify_paths=[path])
        registry = base_registry()
        registry["entries"].append(entry(path, "composition_mount", "daemon_composition_mount"))
        next(row for row in registry["areas"] if row["id"] == "composition_mount")["status"] = "planned"
        self.assert_rejected("active entry must reference an active area", registry=registry)
        registry = base_registry()
        registry["entries"].append(
            entry(
                "crates/tracedecay/src/config.rs",
                "composition_mount",
                "configuration_registry_mount",
            )
        )
        self.assert_rejected("outside its referenced area", registry=registry)
        registry = base_registry()
        registry["entries"].append(entry(path, "composition_mount", "daemon_composition_mount"))
        overlap = area(
            "overlapping_composition_mount",
            "upstream_owned",
            [path],
            ["daemon_composition_mount"],
        )
        registry["areas"].append(overlap)
        self.assert_rejected(
            "exactly its active upstream area",
            registry=registry,
            classify_paths=[path],
        )

    def test_touch_point_binding_rejects_wrong_unknown_and_ambiguous_paths(self) -> None:
        registry = base_registry()
        registry["entries"][0]["touch_point"] = "daemon_composition_mount"
        self.assert_rejected("not declared by its area", registry=registry)
        registry = base_registry()
        registry["entries"][0]["touch_point"] = "unknown_mount"
        self.assert_rejected("unknown policy touch point", registry=registry)
        policy = base_policy()
        policy["allowed_touch_points"].append(
            {"id": "duplicate_workspace_mount", "paths": ["Cargo.toml"]}
        )
        self.assert_rejected("exactly one policy touch point", policy=policy)

    def test_unclassified_m2_and_unmapped_prospective_host_mount_fail(self) -> None:
        self.assert_rejected(
            "unclassified by the M2 ownership registry",
            classify_paths=["crates/tracedecay-memory-provider-ocean/src/lib.rs"],
        )
        self.assert_rejected(
            "lacks an active exact convergence entry",
            classify_paths=["crates/tracedecay/src/daemon/service/project_runtime.rs"],
        )

    def test_copied_or_renamed_upstream_paths_do_not_inherit_authority(self) -> None:
        for path in (
            "Cargo-copy.toml",
            "crates/tracedecay/src/daemon/project_composition-copy.rs",
        ):
            with self.subTest(path=path):
                self.assert_rejected(
                    "unclassified by the M2 ownership registry",
                    classify_paths=[path],
                )

    def test_generated_entry_shape_and_generator_path_are_strict(self) -> None:
        registry = base_registry()
        registry["entries"][1]["generated"]["unexpected"] = True
        self.assert_rejected("contains unknown fields", registry=registry)
        registry = base_registry()
        del registry["entries"][1]["generated"]["reproduction"]
        self.assert_rejected("missing required fields", registry=registry)
        registry = base_registry()
        registry["entries"][1]["generated"]["generator_path"] = "../toolchain"
        self.assert_rejected("normalized repo-relative POSIX", registry=registry)

    def test_mixed_path_batch_fails_as_a_whole_on_any_unclassified_input(self) -> None:
        payload = self.assert_rejected(
            "unclassified by the M2 ownership registry",
            classify_paths=[
                "crates/tracedecay-memory-provider-api/src/lib.rs",
                "Cargo.toml",
                "crates/tracedecay-memory-provider-ocean/src/lib.rs",
            ],
        )
        self.assertNotIn("classifications", payload)


if __name__ == "__main__":
    unittest.main()
