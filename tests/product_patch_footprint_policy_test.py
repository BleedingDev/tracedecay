#!/usr/bin/env python3
"""Contract tests for the upstream patch-footprint policy and convergence map."""

from __future__ import annotations

import copy
import importlib.util
import json
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
POLICY = REPO / "product/upstream/patch-footprint-policy.json"
CONVERGENCE_MAP = REPO / "product/upstream/convergence-map.json"
CHECKER = REPO / "scripts/product/check-patch-footprint-policy.py"

CHECKER_SPEC = importlib.util.spec_from_file_location(
    "product_patch_footprint_policy_checker", CHECKER
)
if CHECKER_SPEC is None or CHECKER_SPEC.loader is None:
    raise RuntimeError(f"could not load checker module from {CHECKER}")
CHECKER_MODULE = importlib.util.module_from_spec(CHECKER_SPEC)
CHECKER_SPEC.loader.exec_module(CHECKER_MODULE)


class PatchFootprintPolicyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.policy = json.loads(POLICY.read_text(encoding="utf-8"))
        cls.convergence_map = json.loads(CONVERGENCE_MAP.read_text(encoding="utf-8"))

    def run_checker(
        self,
        policy: dict[str, Any] | None = None,
        convergence_map: dict[str, Any] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        if policy is None and convergence_map is None:
            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--policy",
                    str(POLICY),
                    "--map",
                    str(CONVERGENCE_MAP),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

        policy = copy.deepcopy(self.policy if policy is None else policy)
        convergence_map = copy.deepcopy(
            self.convergence_map if convergence_map is None else convergence_map
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            policy_path = temp / "policy.json"
            map_path = temp / "convergence-map.json"
            policy_path.write_text(
                json.dumps(policy, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            map_path.write_text(
                json.dumps(convergence_map, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--policy",
                    str(policy_path),
                    "--map",
                    str(map_path),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

    def assert_rejected(
        self,
        marker: str,
        *,
        policy: dict[str, Any] | None = None,
        convergence_map: dict[str, Any] | None = None,
    ) -> None:
        result = self.run_checker(policy, convergence_map)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertFalse(receipt["ok"])
        self.assertIn(marker, "\n".join(receipt["errors"]))

    def touch_point(self, policy: dict[str, Any], touch_id: str) -> dict[str, Any]:
        return next(
            row for row in policy["allowed_touch_points"] if row["id"] == touch_id
        )

    def dependency_rules(
        self, policy: dict[str, Any] | None = None
    ) -> dict[str, dict[str, Any]]:
        selected = self.policy if policy is None else policy
        return {row["id"]: row for row in selected["dependency_direction_rules"]}

    def bound_dependency_adr(self, exception: dict[str, Any]) -> str:
        return textwrap.dedent(
            f"""
            # Synthetic dependency decision

            ## Dependency-direction exception

            - Rule: `{exception["rule"]}`
            - Source: `{exception["source"]}`
            - Dependency: `{exception["dependency"]}`

            ## Decision

            Permit this exact dependency edge only for the documented bounded case.

            ## Rationale

            The normal provider boundary cannot satisfy this case without a narrower seam.
            """
        ).lstrip()

    def validate_dependency_fixture(
        self,
        manifests: dict[str, str],
        *,
        exceptions: list[dict[str, Any]] | None = None,
        workspace_manifest: str | None = None,
        adrs: dict[str, str] | None = None,
    ) -> tuple[list[str], int]:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = Path(temp_dir)
            root_manifest = (
                workspace_manifest
                or """
                [workspace]
                members = ["crates/*"]
                resolver = "2"
            """
            )
            (repo / "Cargo.toml").write_text(
                textwrap.dedent(root_manifest).lstrip(), encoding="utf-8"
            )
            for relative, contents in manifests.items():
                path = repo / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(textwrap.dedent(contents).lstrip(), encoding="utf-8")
            for relative, contents in (adrs or {}).items():
                path = repo / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(contents, encoding="utf-8")
            errors: list[str] = []
            count = CHECKER_MODULE.validate_dependency_directions(
                repo,
                self.dependency_rules(),
                errors,
                copy.deepcopy(exceptions or []),
            )
            return errors, count

    def test_real_repository_has_no_nonbudget_policy_errors(self) -> None:
        errors, footprint, manifest_count = CHECKER_MODULE.validate_document(
            REPO,
            copy.deepcopy(self.policy),
            copy.deepcopy(self.convergence_map),
        )
        measured_budget_prefixes = (
            "upstream production files ",
            "upstream test/fixture files ",
            "total upstream changed lines ",
            "composition-root files ",
            "exception-zone files ",
            "workspace manifest files ",
            "touch-point category ",
            "upstream file ",
            "ADR ",
        )
        nonbudget_errors = [
            error for error in errors if not error.startswith(measured_budget_prefixes)
        ]
        self.assertEqual(nonbudget_errors, [], "\n".join(errors))
        self.assertEqual(len(self.policy["dependency_direction_rules"]), 8)
        self.assertGreater(manifest_count, 30)
        self.assertEqual(
            set(footprint),
            {
                "composition_root_files",
                "exception_zone_files",
                "total_upstream_changed_lines",
                "upstream_existing_production_files",
                "upstream_existing_test_or_fixture_files",
            },
        )

    def test_budget_cannot_be_silently_loosened(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["initial_budget"]["max_total_upstream_changed_lines"] = 3301
        self.assert_rejected(
            "initial_budget.max_total_upstream_changed_lines must be 3300",
            policy=policy,
        )

    def test_broad_product_owned_pattern_is_rejected(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["product_owned_paths"].append("crates/**")
        self.assert_rejected(
            "product-owned paths must not hide upstream tree 'crates/**'",
            policy=policy,
        )

    def test_administrative_footprint_exclusion_is_exact(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["administrative_paths_excluded_from_footprint"] = []
        self.assert_rejected(
            "administrative footprint exclusions missing: ['.codex/**']",
            policy=policy,
        )

        policy = copy.deepcopy(self.policy)
        policy["administrative_paths_excluded_from_footprint"].append("crates/**")
        self.assert_rejected(
            "unexpected/broad administrative footprint exclusions: ['crates/**']",
            policy=policy,
        )

    def test_missing_allowed_touch_point_is_rejected(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["allowed_touch_points"] = [
            row
            for row in policy["allowed_touch_points"]
            if row["id"] != "recall_context_mount"
        ]
        self.assert_rejected("allowed touch points missing", policy=policy)

    def test_shutdown_and_harness_touch_points_cannot_be_removed(self) -> None:
        for touch_id in (
            "daemon_shutdown_deadline",
            "production_harness_shutdown",
            "integration_test_runtime_isolation",
        ):
            with self.subTest(touch_id=touch_id):
                policy = copy.deepcopy(self.policy)
                policy["allowed_touch_points"] = [
                    row
                    for row in policy["allowed_touch_points"]
                    if row["id"] != touch_id
                ]
                self.assert_rejected(
                    f"allowed touch points missing: ['{touch_id}']",
                    policy=policy,
                )

    def test_shutdown_and_harness_live_paths_have_exact_authority(self) -> None:
        expected = {
            "crates/tracedecay/src/daemon/bootstrap.rs": (
                "daemon_shutdown_deadline",
                "shutdown_deadline",
            ),
            "crates/tracedecay/src/daemon/engine/shutdown.rs": (
                "daemon_shutdown_deadline",
                "shutdown_deadline",
            ),
            "crates/tracedecay/src/daemon/invocation_state.rs": (
                "daemon_shutdown_deadline",
                "shutdown_deadline",
            ),
            "crates/tracedecay/src/daemon/production_harness.rs": (
                "production_harness_shutdown",
                "shutdown_deadline",
            ),
            "crates/tracedecay-daemon-service/src/invocation/lsp.rs": (
                "daemon_shutdown_deadline",
                "shutdown_deadline",
            ),
            "crates/tracedecay-daemon-service/src/project_runtime/shutdown.rs": (
                "daemon_shutdown_deadline",
                "shutdown_deadline",
            ),
            "crates/tracedecay/tests/common/mod.rs": (
                "integration_test_runtime_isolation",
                "integration_test_harness",
            ),
            "crates/tracedecay/tests/memory_suite/memory_eval_test.rs": (
                "integration_test_runtime_isolation",
                "integration_test_harness",
            ),
        }
        touches = {
            row["id"]: row for row in self.policy["allowed_touch_points"]
        }
        areas = {row["id"]: row for row in self.convergence_map["areas"]}
        entries = {
            row["path"]: row for row in self.convergence_map["entries"]
        }
        errors: list[str] = []
        live_diff = CHECKER_MODULE.diff_numstat(
            REPO, CHECKER_MODULE.EXPECTED_FLOOR, errors
        )
        self.assertEqual(errors, [])
        for path, (touch_id, area_id) in expected.items():
            with self.subTest(path=path):
                self.assertIn(path, live_diff)
                self.assertEqual(
                    CHECKER_MODULE.matching_touch_points(path, touches), [touch_id]
                )
                self.assertEqual(
                    CHECKER_MODULE.matching_active_area_ids(
                        path, areas, "upstream_owned"
                    ),
                    [area_id],
                )
                self.assertEqual(entries[path]["touch_point"], touch_id)
                self.assertEqual(entries[path]["area_id"], area_id)

    def test_cognitive_recall_contract_paths_have_exact_authority(self) -> None:
        paths = {
            "crates/tracedecay-application/src/memory.rs",
            "crates/tracedecay-application/src/memory/recall.rs",
            "crates/tracedecay-application/tests/cognitive_recall_port.rs",
            "crates/tracedecay/tests/application_production_reachability.rs",
        }
        touches = {
            row["id"]: row for row in self.policy["allowed_touch_points"]
        }
        areas = {row["id"]: row for row in self.convergence_map["areas"]}
        entries = {
            row["path"]: row for row in self.convergence_map["entries"]
        }
        for path in paths:
            with self.subTest(path=path):
                self.assertEqual(
                    CHECKER_MODULE.matching_touch_points(path, touches),
                    ["cognitive_recall_contract"],
                )
                self.assertEqual(
                    CHECKER_MODULE.matching_active_area_ids(
                        path, areas, "upstream_owned"
                    ),
                    ["application_recall_contract"],
                )
                self.assertEqual(
                    entries[path]["touch_point"], "cognitive_recall_contract"
                )
                self.assertEqual(
                    entries[path]["area_id"], "application_recall_contract"
                )

    def test_missing_dependency_direction_rule_is_rejected(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["dependency_direction_rules"] = [
            row
            for row in policy["dependency_direction_rules"]
            if row["id"] != "ncm_adapter_does_not_reach_native_store"
        ]
        self.assert_rejected("dependency direction rules missing", policy=policy)

    def test_canonical_dependency_rule_cannot_be_weakened_or_bypassed(self) -> None:
        policy = copy.deepcopy(self.policy)
        api_rule = next(
            row
            for row in policy["dependency_direction_rules"]
            if row["id"] == "provider_api_is_inward"
        )
        api_rule["forbidden_dependencies"].remove("tracedecay-*")
        self.assert_rejected(
            "provider_api_is_inward.forbidden_dependencies must match the canonical",
            policy=policy,
        )

        policy = copy.deepcopy(self.policy)
        api_rule = next(
            row
            for row in policy["dependency_direction_rules"]
            if row["id"] == "provider_api_is_inward"
        )
        api_rule["except_packages"] = ["*"]
        self.assert_rejected(
            "provider_api_is_inward.except_packages must match the canonical",
            policy=policy,
        )

        policy = copy.deepcopy(self.policy)
        fabric_rule = next(
            row
            for row in policy["dependency_direction_rules"]
            if row["id"] == "memory_fabric_is_provider_neutral"
        )
        fabric_rule["allowed_dependencies"].append("tracedecay-memory-provider-biomem")
        self.assert_rejected(
            "memory_fabric_is_provider_neutral.allowed_dependencies must match "
            "the canonical",
            policy=policy,
        )

    def test_forbidden_dependency_is_rejected_in_every_cargo_section(self) -> None:
        sections = (
            "[dependencies]",
            "[dev-dependencies]",
            "[build-dependencies]",
            "[target.'cfg(unix)'.dependencies]",
            "[target.'cfg(unix)'.dev-dependencies]",
            "[target.'cfg(unix)'.build-dependencies]",
        )
        for section in sections:
            with self.subTest(section=section):
                errors, count = self.validate_dependency_fixture(
                    {
                        "crates/api/Cargo.toml": f"""
                            [package]
                            name = "tracedecay-memory-provider-api"
                            version = "0.0.0"

                            {section}
                            fabric = {{ package = "tracedecay-memory-fabric", version = "1" }}
                        """
                    }
                )
                violations = [
                    error
                    for error in errors
                    if "dependency direction provider_api_is_inward violated" in error
                ]
                self.assertEqual(count, 1)
                self.assertEqual(len(violations), 1, "\n".join(errors))
                self.assertIn(
                    "tracedecay-memory-provider-api -> tracedecay-memory-fabric",
                    violations[0],
                )
                self.assertIn("key 'fabric'", violations[0])

    def test_renamed_dependencies_use_resolved_package_names(self) -> None:
        errors, _ = self.validate_dependency_fixture(
            {
                "crates/api/Cargo.toml": """
                    [package]
                    name = "tracedecay-memory-provider-api"
                    version = "0.0.0"

                    [dependencies]
                    harmless_alias = { package = "tracedecay-memory-fabric", version = "1" }
                    tracedecay-memory-context = { package = "serde", version = "1" }
                """
            }
        )
        joined = "\n".join(errors)
        self.assertIn(
            "provider_api_is_inward violated: tracedecay-memory-provider-api -> "
            "tracedecay-memory-fabric",
            joined,
        )
        self.assertIn("key 'harmless_alias'", joined)
        self.assertNotIn("-> serde", joined)

    def test_workspace_inherited_renamed_dependency_is_resolved(self) -> None:
        errors, count = self.validate_dependency_fixture(
            {
                "crates/api/Cargo.toml": """
                    [package]
                    name = "tracedecay-memory-provider-api"
                    version = "0.0.0"

                    [dependencies]
                    stealth.workspace = true
                """
            },
            workspace_manifest="""
                [workspace]
                members = ["crates/*"]
                resolver = "2"

                [workspace.dependencies]
                stealth = { package = "tracedecay-memory-fabric", version = "1" }
            """,
        )
        self.assertEqual(count, 1)
        joined = "\n".join(errors)
        self.assertIn(
            "provider_api_is_inward violated: tracedecay-memory-provider-api -> "
            "tracedecay-memory-fabric",
            joined,
        )
        self.assertIn("key 'stealth'", joined)

    def test_protected_source_package_rename_cannot_evade_rules(self) -> None:
        errors, _ = self.validate_dependency_fixture(
            {
                "crates/tracedecay-memory-provider-api/Cargo.toml": """
                    [package]
                    name = "harmless-api"
                    version = "0.0.0"

                    [dependencies]
                    tracedecay-memory-fabric = "1"
                """
            }
        )
        joined = "\n".join(errors)
        self.assertIn(
            "protected package identity mismatch: "
            "crates/tracedecay-memory-provider-api/Cargo.toml must declare "
            "[package].name = 'tracedecay-memory-provider-api'; found 'harmless-api'",
            joined,
        )
        self.assertIn(
            "provider_api_is_inward violated: tracedecay-memory-provider-api -> "
            "tracedecay-memory-fabric",
            joined,
        )

    def test_protected_target_package_rename_cannot_evade_rules(self) -> None:
        errors, _ = self.validate_dependency_fixture(
            {
                "crates/tracedecay-memory-provider-api/Cargo.toml": """
                    [package]
                    name = "tracedecay-memory-provider-api"
                    version = "0.0.0"

                    [dependencies]
                    fabric = { package = "harmless-fabric", path = "../tracedecay-memory-fabric" }
                """,
                "crates/tracedecay-memory-fabric/Cargo.toml": """
                    [package]
                    name = "harmless-fabric"
                    version = "0.0.0"
                """,
            }
        )
        joined = "\n".join(errors)
        self.assertIn(
            "protected package identity mismatch: "
            "crates/tracedecay-memory-fabric/Cargo.toml must declare "
            "[package].name = 'tracedecay-memory-fabric'; found 'harmless-fabric'",
            joined,
        )
        self.assertIn(
            "provider_api_is_inward violated: tracedecay-memory-provider-api -> "
            "tracedecay-memory-fabric",
            joined,
        )

    def test_excluded_path_target_rename_cannot_hide_its_canonical_identity(
        self,
    ) -> None:
        errors, count = self.validate_dependency_fixture(
            {
                "crates/tracedecay-memory-provider-api/Cargo.toml": """
                    [package]
                    name = "tracedecay-memory-provider-api"
                    version = "0.0.0"

                    [dependencies]
                    fabric = { package = "harmless-fabric", path = "../tracedecay-memory-fabric" }
                """,
                "crates/tracedecay-memory-fabric/Cargo.toml": """
                    [package]
                    name = "harmless-fabric"
                    version = "0.0.0"
                """,
            },
            workspace_manifest="""
                [workspace]
                members = ["crates/tracedecay-memory-provider-api"]
                exclude = ["crates/tracedecay-memory-fabric"]
                resolver = "2"
            """,
        )
        joined = "\n".join(errors)
        self.assertEqual(count, 1)
        self.assertIn(
            "protected package identity mismatch: "
            "crates/tracedecay-memory-fabric/Cargo.toml must declare "
            "[package].name = 'tracedecay-memory-fabric'; found 'harmless-fabric'",
            joined,
        )
        self.assertIn(
            "provider_api_is_inward violated: tracedecay-memory-provider-api -> "
            "tracedecay-memory-fabric",
            joined,
        )

    def test_every_protected_boundary_path_has_a_canonical_package_name(self) -> None:
        for (
            path,
            expected,
        ) in CHECKER_MODULE.EXPECTED_PROTECTED_PACKAGE_IDENTITIES.items():
            with self.subTest(path=path.as_posix()):
                errors, _ = self.validate_dependency_fixture(
                    {
                        path.as_posix(): """
                            [package]
                            name = "renamed-boundary"
                            version = "0.0.0"
                        """
                    }
                )
                self.assertIn(
                    f"{path.as_posix()} must declare [package].name = {expected!r}; "
                    "found 'renamed-boundary'",
                    "\n".join(errors),
                )

    def test_nested_workspace_member_is_scanned(self) -> None:
        errors, count = self.validate_dependency_fixture(
            {
                "components/memory/api/Cargo.toml": """
                    [package]
                    name = "tracedecay-memory-provider-api"
                    version = "0.0.0"

                    [dependencies]
                    tracedecay-memory-fabric = "1"
                """
            },
            workspace_manifest="""
                [workspace]
                members = ["components/memory/api"]
                resolver = "2"
            """,
        )
        self.assertEqual(count, 1)
        self.assertIn("provider_api_is_inward violated", "\n".join(errors))

    def test_in_tree_path_dependency_is_discovered_as_an_automatic_member(
        self,
    ) -> None:
        errors, count = self.validate_dependency_fixture(
            {
                "crates/host/Cargo.toml": """
                    [package]
                    name = "host"
                    version = "0.0.0"

                    [dependencies]
                    hidden = { path = "../hidden" }
                """,
                "crates/hidden/Cargo.toml": """
                    [package]
                    name = "tracedecay-memory-provider-api"
                    version = "0.0.0"

                    [dependencies]
                    tracedecay-memory-fabric = "1"
                """,
            },
            workspace_manifest="""
                [workspace]
                members = ["crates/host"]
                resolver = "2"
            """,
        )
        self.assertEqual(count, 2)
        self.assertIn("provider_api_is_inward violated", "\n".join(errors))

    def test_workspace_exclude_prevents_automatic_path_member_scan(self) -> None:
        errors, count = self.validate_dependency_fixture(
            {
                "crates/host/Cargo.toml": """
                    [package]
                    name = "host"
                    version = "0.0.0"

                    [dependencies]
                    hidden = { path = "../hidden" }
                """,
                "crates/hidden/Cargo.toml": """
                    [package]
                    name = "excluded-package"
                    version = "0.0.0"

                    [dependencies]
                    tracedecay-memory-fabric = "1"
                """,
            },
            workspace_manifest="""
                [workspace]
                members = ["crates/host"]
                exclude = ["crates/hidden"]
                resolver = "2"
            """,
        )
        self.assertEqual(count, 1)
        self.assertEqual(errors, [])

    def test_workspace_dependency_path_is_discovered_as_an_automatic_member(
        self,
    ) -> None:
        errors, count = self.validate_dependency_fixture(
            {
                "crates/host/Cargo.toml": """
                    [package]
                    name = "host"
                    version = "0.0.0"
                """,
                "crates/hidden/Cargo.toml": """
                    [package]
                    name = "tracedecay-memory-provider-api"
                    version = "0.0.0"

                    [dependencies]
                    tracedecay-memory-context = "1"
                """,
            },
            workspace_manifest="""
                [workspace]
                members = ["crates/host"]
                resolver = "2"

                [workspace.dependencies]
                hidden = { path = "crates/hidden" }
            """,
        )
        self.assertEqual(count, 2)
        self.assertIn("provider_api_is_inward violated", "\n".join(errors))

    def test_root_package_path_dependency_is_not_shadowed_by_workspace_scan(
        self,
    ) -> None:
        errors, count = self.validate_dependency_fixture(
            {
                "crates/hidden/Cargo.toml": """
                    [package]
                    name = "tracedecay-memory-provider-api"
                    version = "0.0.0"

                    [dependencies]
                    tracedecay-memory-fabric = "1"
                """,
            },
            workspace_manifest="""
                [package]
                name = "root-host"
                version = "0.0.0"

                [dependencies]
                hidden = { path = "crates/hidden" }

                [workspace]
                resolver = "2"
            """,
        )
        self.assertEqual(count, 2)
        self.assertIn("provider_api_is_inward violated", "\n".join(errors))

    def test_unsafe_workspace_exclude_cannot_hide_a_member(self) -> None:
        errors, count = self.validate_dependency_fixture(
            {
                "crates/api/Cargo.toml": """
                    [package]
                    name = "tracedecay-memory-provider-api"
                    version = "0.0.0"

                    [dependencies]
                    tracedecay-memory-fabric = "1"
                """,
            },
            workspace_manifest="""
                [workspace]
                members = ["crates/*"]
                exclude = ["components/../crates/api"]
                resolver = "2"
            """,
        )
        joined = "\n".join(errors)
        self.assertEqual(count, 1)
        self.assertIn("workspace.exclude entry must be a relative", joined)
        self.assertIn("provider_api_is_inward violated", joined)

    def test_escaped_workspace_member_is_rejected_but_scanned_when_in_tree(
        self,
    ) -> None:
        errors, count = self.validate_dependency_fixture(
            {
                "crates/api/Cargo.toml": """
                    [package]
                    name = "tracedecay-memory-provider-api"
                    version = "0.0.0"

                    [dependencies]
                    tracedecay-memory-fabric = "1"
                """,
            },
            workspace_manifest="""
                [workspace]
                members = ["components/../crates/api"]
                resolver = "2"
            """,
        )
        joined = "\n".join(errors)
        self.assertEqual(count, 1)
        self.assertIn("workspace member must be a relative in-repo pattern", joined)
        self.assertIn("provider_api_is_inward violated", joined)

    def test_absolute_workspace_member_is_rejected_but_scanned_when_in_tree(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = Path(temp_dir)
            member = repo / "crates/api"
            member.mkdir(parents=True)
            (member / "Cargo.toml").write_text(
                textwrap.dedent(
                    """
                    [package]
                    name = "tracedecay-memory-provider-api"
                    version = "0.0.0"

                    [dependencies]
                    tracedecay-memory-fabric = "1"
                    """
                ).lstrip(),
                encoding="utf-8",
            )
            (repo / "Cargo.toml").write_text(
                textwrap.dedent(
                    f"""
                    [workspace]
                    members = [{member.as_posix()!r}]
                    resolver = "2"
                    """
                ).lstrip(),
                encoding="utf-8",
            )
            errors: list[str] = []
            count = CHECKER_MODULE.validate_dependency_directions(
                repo, self.dependency_rules(), errors, []
            )
        joined = "\n".join(errors)
        self.assertEqual(count, 1)
        self.assertIn("workspace member must be a relative in-repo pattern", joined)
        self.assertIn("provider_api_is_inward violated", joined)

    def test_escaped_workspace_member_outside_repository_is_never_scanned(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            sandbox = Path(temp_dir)
            repo = sandbox / "repo"
            outside = sandbox / "outside"
            repo.mkdir()
            outside.mkdir()
            (outside / "Cargo.toml").write_text(
                textwrap.dedent(
                    """
                    [package]
                    name = "tracedecay-memory-provider-api"
                    version = "0.0.0"

                    [dependencies]
                    tracedecay-memory-fabric = "1"
                    """
                ).lstrip(),
                encoding="utf-8",
            )
            (repo / "Cargo.toml").write_text(
                textwrap.dedent(
                    """
                    [workspace]
                    members = ["../outside"]
                    resolver = "2"
                    """
                ).lstrip(),
                encoding="utf-8",
            )
            errors: list[str] = []
            count = CHECKER_MODULE.validate_dependency_directions(
                repo, self.dependency_rules(), errors, []
            )
        joined = "\n".join(errors)
        self.assertEqual(count, 0)
        self.assertIn("workspace member must be a relative in-repo pattern", joined)
        self.assertIn("resolves outside the repository and was not scanned", joined)
        self.assertNotIn("provider_api_is_inward violated", joined)

    def test_legacy_project_and_underscore_dependency_tables_are_rejected_and_scanned(
        self,
    ) -> None:
        errors, count = self.validate_dependency_fixture(
            {
                "crates/api/Cargo.toml": """
                    [project]
                    name = "tracedecay-memory-provider-api"
                    version = "0.0.0"

                    [dev_dependencies]
                    fabric = { package = "tracedecay-memory-fabric", version = "1" }

                    [target.'cfg(unix)'.build_dependencies]
                    context = { package = "tracedecay-memory-context", version = "1" }
                """,
            }
        )
        joined = "\n".join(errors)
        self.assertEqual(count, 1)
        self.assertIn("uses unsupported legacy [project]", joined)
        self.assertIn("uses unsupported legacy [dev_dependencies]", joined)
        self.assertIn(
            "uses unsupported legacy [target.cfg(unix).build_dependencies]", joined
        )
        self.assertIn(
            "tracedecay-memory-provider-api -> tracedecay-memory-fabric", joined
        )
        self.assertIn(
            "tracedecay-memory-provider-api -> tracedecay-memory-context", joined
        )

    def test_registry_is_composition_not_a_concrete_adapter(self) -> None:
        errors, count = self.validate_dependency_fixture(
            {
                "crates/registry/Cargo.toml": """
                    [package]
                    name = "tracedecay-memory-provider-registry"
                    version = "0.0.0"

                    [dependencies]
                    tracedecay-memory-fabric = "1"
                    tracedecay-memory-provider-native = "1"
                """,
            }
        )
        self.assertEqual(count, 1)
        self.assertEqual(errors, [])

    def test_api_fabric_and_concrete_adapter_boundaries_are_enforced(self) -> None:
        api_upper_layers = (
            "tracedecay-memory-fabric",
            "tracedecay-memory-provider-registry",
            "tracedecay-memory-provider-native",
            "tracedecay-memory-provider-ncm",
            "tracedecay-memory-observation",
            "tracedecay-memory-context",
            "tracedecay-memory-conformance",
            "tracedecay-memory-future-layer",
        )
        for dependency in api_upper_layers:
            with self.subTest(boundary="api", dependency=dependency):
                errors, _ = self.validate_dependency_fixture(
                    {
                        "crates/source/Cargo.toml": f"""
                            [package]
                            name = "tracedecay-memory-provider-api"
                            version = "0.0.0"

                            [dependencies]
                            edge = {{ package = "{dependency}", version = "1" }}
                        """
                    }
                )
                self.assertIn(
                    f"provider_api_is_inward violated: "
                    f"tracedecay-memory-provider-api -> {dependency}",
                    "\n".join(errors),
                )

        fabric_concrete_dependencies = (
            "tracedecay-memory-provider-native",
            "tracedecay-memory-provider-ncm",
            "tracedecay-memory-provider-ocean",
            "tracedecay-memory-provider-biomem",
            "biomem-sdk",
            "ncm-sdk",
            "ocean-sdk",
        )
        for dependency in fabric_concrete_dependencies:
            with self.subTest(boundary="fabric", dependency=dependency):
                errors, _ = self.validate_dependency_fixture(
                    {
                        "crates/source/Cargo.toml": f"""
                            [package]
                            name = "tracedecay-memory-fabric"
                            version = "0.0.0"

                            [dependencies]
                            edge = {{ package = "{dependency}", version = "1" }}
                        """
                    }
                )
                self.assertIn(
                    f"memory_fabric_is_provider_neutral violated: "
                    f"tracedecay-memory-fabric -> {dependency}",
                    "\n".join(errors),
                )

    def test_provider_neutral_rules_reject_future_adapters_and_sdks_but_allow_api(
        self,
    ) -> None:
        neutral_sources = (
            (
                "tracedecay-memory-fabric",
                "memory_fabric_is_provider_neutral",
            ),
            (
                "tracedecay-memory-context",
                "context_compiler_is_provider_neutral",
            ),
            ("tracedecay-cli", "transports_are_adapter_blind"),
            (
                "tracedecay-application",
                "upstream_crates_do_not_import_concrete_adapters",
            ),
        )
        forbidden = (
            "tracedecay-memory-provider-biomem",
            "biomem-sdk",
            "ncm-sdk",
            "ocean-sdk",
        )
        for source, rule_id in neutral_sources:
            for dependency in forbidden:
                with self.subTest(source=source, dependency=dependency):
                    errors, _ = self.validate_dependency_fixture(
                        {
                            "crates/source/Cargo.toml": f"""
                                [package]
                                name = "{source}"
                                version = "0.0.0"

                                [dependencies]
                                edge = {{ package = "{dependency}", version = "1" }}
                            """
                        }
                    )
                    self.assertIn(
                        f"dependency direction {rule_id} violated: "
                        f"{source} -> {dependency}",
                        "\n".join(errors),
                    )

            with self.subTest(source=source, dependency="provider-api"):
                errors, _ = self.validate_dependency_fixture(
                    {
                        "crates/source/Cargo.toml": f"""
                            [package]
                            name = "{source}"
                            version = "0.0.0"

                            [dependencies]
                            tracedecay-memory-provider-api = "1"
                        """
                    }
                )
                self.assertEqual(errors, [])

    def test_ncm_and_native_adapters_cannot_reach_internal_layers(self) -> None:
        ncm_forbidden = (
            "tracedecay",
            "tracedecay-runtime-core",
            "tracedecay-store",
            "tracedecay-session-temporal-store",
            "tracedecay-global-db",
            "tracedecay-graph-db",
            "tracedecay-rusqlite-runtime",
            "tracedecay-code-index",
            "tracedecay-code-index-runtime",
            "tracedecay-code-extraction",
            "tracedecay-query",
            "tracedecay-temporal-query",
            "tracedecay-semantic",
            "rusqlite",
            "grafeo-engine",
            "libsql-client",
            "private-fs-runtime",
            "tracedecay-memory-provider-native",
            "tracedecay-memory-fabric",
            "ncm-sdk",
            "ocean-sdk",
        )
        for dependency in ncm_forbidden:
            with self.subTest(adapter="ncm", dependency=dependency):
                errors, _ = self.validate_dependency_fixture(
                    {
                        "crates/source/Cargo.toml": f"""
                            [package]
                            name = "tracedecay-memory-provider-ncm"
                            version = "0.0.0"

                            [dependencies]
                            edge = {{ package = "{dependency}", version = "1" }}
                        """
                    }
                )
                self.assertIn(
                    f"ncm_adapter_does_not_reach_native_store violated: "
                    f"tracedecay-memory-provider-ncm -> {dependency}",
                    "\n".join(errors),
                )

        errors, _ = self.validate_dependency_fixture(
            {
                "crates/source/Cargo.toml": """
                    [package]
                    name = "tracedecay-memory-provider-native"
                    version = "0.0.0"

                    [dependencies]
                    query = { package = "tracedecay-query", version = "1" }
                """
            }
        )
        self.assertIn(
            "concrete_adapters_do_not_reach_tracedecay_internals violated: "
            "tracedecay-memory-provider-native -> tracedecay-query",
            "\n".join(errors),
        )

        errors, _ = self.validate_dependency_fixture(
            {
                "crates/source/Cargo.toml": """
                    [package]
                    name = "tracedecay-memory-provider-ncm"
                    version = "0.0.0"

                    [dependencies]
                    biomem-sdk = "1"
                    biomem-client = "1"
                    tracedecay-memory-provider-api = "1"
                """
            }
        )
        self.assertEqual(errors, [])

    def test_raw_storage_engines_are_rejected_through_workspace_aliases(self) -> None:
        errors, _ = self.validate_dependency_fixture(
            {
                "crates/ncm/Cargo.toml": """
                    [package]
                    name = "tracedecay-memory-provider-ncm"
                    version = "0.0.0"

                    [dependencies]
                    sqlite_alias.workspace = true

                    [target.'cfg(unix)'.build-dependencies]
                    graph_alias.workspace = true
                """
            },
            workspace_manifest="""
                [workspace]
                members = ["crates/*"]
                resolver = "2"

                [workspace.dependencies]
                sqlite_alias = { package = "rusqlite", version = "1" }
                graph_alias = { package = "grafeo-engine", version = "1" }
            """,
        )
        joined = "\n".join(errors)
        for dependency, alias in (
            ("rusqlite", "sqlite_alias"),
            ("grafeo-engine", "graph_alias"),
        ):
            with self.subTest(dependency=dependency):
                self.assertIn(
                    "dependency direction "
                    "ncm_adapter_does_not_reach_native_store violated: "
                    f"tracedecay-memory-provider-ncm -> {dependency}",
                    joined,
                )
                self.assertIn(f"key '{alias}'", joined)

    def test_exact_adr_bound_dependency_exception_suppresses_only_its_edge(
        self,
    ) -> None:
        exception = {
            "rule": "provider_api_is_inward",
            "source": "tracedecay-memory-provider-api",
            "dependency": "tracedecay-memory-fabric",
            "adr": "product/architecture/adr/9999-test-edge.md",
            "rationale": "Synthetic proof that only this exact edge is waived.",
        }
        manifests = {
            "crates/api/Cargo.toml": """
                [package]
                name = "tracedecay-memory-provider-api"
                version = "0.0.0"

                [dependencies]
                tracedecay-memory-fabric = "1"
            """
        }
        adrs = {exception["adr"]: self.bound_dependency_adr(exception)}
        errors, _ = self.validate_dependency_fixture(
            manifests, exceptions=[exception], adrs=adrs
        )
        self.assertEqual(errors, [])

        mutations = {
            "rule": "memory_fabric_is_provider_neutral",
            "source": "tracedecay-memory-provider-api-typo",
            "dependency": "tracedecay-memory-context",
        }
        for field, value in mutations.items():
            with self.subTest(field=field):
                changed = copy.deepcopy(exception)
                changed[field] = value
                errors, _ = self.validate_dependency_fixture(
                    manifests, exceptions=[changed], adrs=adrs
                )
                self.assertTrue(errors)
                self.assertIn("provider_api_is_inward violated", "\n".join(errors))

    def test_dependency_exception_adr_must_bind_and_decide_the_exact_edge(
        self,
    ) -> None:
        exception = {
            "rule": "provider_api_is_inward",
            "source": "tracedecay-memory-provider-api",
            "dependency": "tracedecay-memory-fabric",
            "adr": "product/architecture/adr/9999-test-edge.md",
            "rationale": "A narrow temporary bridge is required for this bounded dependency edge.",
        }
        manifests = {
            "crates/api/Cargo.toml": """
                [package]
                name = "tracedecay-memory-provider-api"
                version = "0.0.0"

                [dependencies]
                tracedecay-memory-fabric = "1"
            """
        }

        unrelated = "# Unrelated decision\n"
        errors, _ = self.validate_dependency_fixture(
            manifests,
            exceptions=[exception],
            adrs={exception["adr"]: unrelated},
        )
        joined = "\n".join(errors)
        self.assertIn(
            "ADR must contain exactly one '## Dependency-direction exception' section",
            joined,
        )
        self.assertIn("provider_api_is_inward violated", joined)

        for field, label in (
            ("rule", "rule"),
            ("source", "source"),
            ("dependency", "dependency"),
        ):
            with self.subTest(binding=field):
                wrong_binding = copy.deepcopy(exception)
                wrong_binding[field] = f"wrong-{field}"
                errors, _ = self.validate_dependency_fixture(
                    manifests,
                    exceptions=[exception],
                    adrs={exception["adr"]: self.bound_dependency_adr(wrong_binding)},
                )
                self.assertIn(
                    f"ADR {label} binding must be exactly {exception[field]!r}",
                    "\n".join(errors),
                )

        valid_text = self.bound_dependency_adr(exception)
        hidden_documents = {
            "fenced": f"```markdown\n{valid_text}```\n",
            "long-fenced-with-short-inner-close": (
                f"````markdown\n{valid_text}```\nstill fenced\n````\n"
            ),
            "tilde-fenced": f"~~~~markdown\n{valid_text}~~~~\n",
            "html-comment": f"<!--\n{valid_text}-->\n",
            "unterminated-html-comment": f"<!--\n{valid_text}",
            "indented-code": "".join(
                f"    {line}\n" for line in valid_text.splitlines()
            ),
        }
        for kind, hidden in hidden_documents.items():
            with self.subTest(hidden=kind):
                errors, _ = self.validate_dependency_fixture(
                    manifests,
                    exceptions=[exception],
                    adrs={exception["adr"]: hidden},
                )
                self.assertIn(
                    "ADR must contain exactly one "
                    "'## Dependency-direction exception' section",
                    "\n".join(errors),
                )

        placeholder = textwrap.dedent(
            f"""
            # Placeholder decision

            ## Dependency-direction exception

            - Rule: `{exception["rule"]}`
            - Source: `{exception["source"]}`
            - Dependency: `{exception["dependency"]}`

            ## Decision

            TBD

            ## Rationale

            ### This heading alone must not count as substantive rationale text
            """
        ).lstrip()
        errors, _ = self.validate_dependency_fixture(
            manifests,
            exceptions=[exception],
            adrs={exception["adr"]: placeholder},
        )
        joined = "\n".join(errors)
        self.assertIn("ADR decision must be substantive prose", joined)
        self.assertIn("ADR rationale must be substantive prose", joined)

        negative_decision = textwrap.dedent(
            f"""
            # Negative decision

            ## Dependency-direction exception

            - Rule: `{exception["rule"]}`
            - Source: `{exception["source"]}`
            - Dependency: `{exception["dependency"]}`

            ## Decision

            Reject this exact dependency edge because it must never be accepted.

            ## Rationale

            The normal provider boundary already satisfies this case without any exception.
            """
        ).lstrip()
        errors, _ = self.validate_dependency_fixture(
            manifests,
            exceptions=[exception],
            adrs={exception["adr"]: negative_decision},
        )
        self.assertIn(
            "ADR decision must explicitly and affirmatively authorize the exact "
            "dependency edge",
            "\n".join(errors),
        )

        bounded_grant = self.bound_dependency_adr(exception).replace(
            "Permit this exact dependency edge only for the documented bounded case.",
            "Permit this exact dependency edge for the documented bounded case. "
            "Do not permit any other dependency edge.",
        )
        errors, _ = self.validate_dependency_fixture(
            manifests,
            exceptions=[exception],
            adrs={exception["adr"]: bounded_grant},
        )
        self.assertEqual(errors, [])

    def test_dependency_exception_shape_is_literal_and_unique(self) -> None:
        base = {
            "rule": "provider_api_is_inward",
            "source": "tracedecay-memory-provider-api",
            "dependency": "tracedecay-memory-fabric",
            "adr": "product/architecture/adr/9999-test-edge.md",
            "rationale": "Synthetic exact edge.",
        }
        for field in ("rule", "source", "dependency"):
            with self.subTest(field=field):
                policy = copy.deepcopy(self.policy)
                exception = copy.deepcopy(base)
                exception[field] += "*"
                policy["dependency_direction_exceptions"] = [exception]
                self.assert_rejected("globs are forbidden", policy=policy)

        policy = copy.deepcopy(self.policy)
        policy["dependency_direction_exceptions"] = [base, copy.deepcopy(base)]
        self.assert_rejected("duplicate dependency direction exception", policy=policy)

        policy = copy.deepcopy(self.policy)
        unknown = copy.deepcopy(base)
        unknown["rule"] = "not_a_rule"
        policy["dependency_direction_exceptions"] = [unknown]
        self.assert_rejected("names unknown dependency rule", policy=policy)

        policy = copy.deepcopy(self.policy)
        blank_rationale = copy.deepcopy(base)
        blank_rationale["rationale"] = "   "
        policy["dependency_direction_exceptions"] = [blank_rationale]
        self.assert_rejected("rationale must be a non-empty string", policy=policy)

    def test_dependency_exception_rejects_missing_out_of_tree_and_stale_evidence(
        self,
    ) -> None:
        manifests = {
            "crates/api/Cargo.toml": """
                [package]
                name = "tracedecay-memory-provider-api"
                version = "0.0.0"

                [dependencies]
                tracedecay-memory-fabric = "1"
            """
        }
        base = {
            "rule": "provider_api_is_inward",
            "source": "tracedecay-memory-provider-api",
            "dependency": "tracedecay-memory-fabric",
            "adr": "product/architecture/adr/missing.md",
            "rationale": "Synthetic exact edge.",
        }
        errors, _ = self.validate_dependency_fixture(manifests, exceptions=[base])
        self.assertIn("ADR is missing", "\n".join(errors))
        self.assertIn("provider_api_is_inward violated", "\n".join(errors))

        outside = copy.deepcopy(base)
        outside["adr"] = "docs/decision.md"
        errors, _ = self.validate_dependency_fixture(
            manifests,
            exceptions=[outside],
            adrs={"docs/decision.md": "# Outside\n"},
        )
        self.assertIn("must be an exact path under", "\n".join(errors))

        stale = copy.deepcopy(base)
        stale["adr"] = "product/architecture/adr/stale.md"
        errors, _ = self.validate_dependency_fixture(
            {
                "crates/api/Cargo.toml": """
                    [package]
                    name = "tracedecay-memory-provider-api"
                    version = "0.0.0"
                """
            },
            exceptions=[stale],
            adrs={stale["adr"]: self.bound_dependency_adr(stale)},
        )
        self.assertIn("is stale/unused", "\n".join(errors))

    def test_dependency_exception_rejects_unknown_and_nonmatching_edges(self) -> None:
        adr = "product/architecture/adr/9999-test-edge.md"
        unknown_source = {
            "rule": "provider_api_is_inward",
            "source": "missing-package",
            "dependency": "tracedecay-memory-fabric",
            "adr": adr,
            "rationale": "Synthetic unknown source.",
        }
        errors, _ = self.validate_dependency_fixture(
            {},
            exceptions=[unknown_source],
            adrs={adr: self.bound_dependency_adr(unknown_source)},
        )
        self.assertIn("names unknown source package", "\n".join(errors))

        nonmatching_source = copy.deepcopy(unknown_source)
        nonmatching_source["source"] = "tracedecay-memory-fabric"
        nonmatching_source["dependency"] = "tracedecay-memory-provider-native"
        errors, _ = self.validate_dependency_fixture(
            {
                "crates/fabric/Cargo.toml": """
                    [package]
                    name = "tracedecay-memory-fabric"
                    version = "0.0.0"

                    [dependencies]
                    tracedecay-memory-provider-native = "1"
                """
            },
            exceptions=[nonmatching_source],
            adrs={adr: self.bound_dependency_adr(nonmatching_source)},
        )
        self.assertIn("does not match dependency rule", "\n".join(errors))

        nonmatching_dependency = copy.deepcopy(unknown_source)
        nonmatching_dependency["source"] = "tracedecay-memory-provider-api"
        nonmatching_dependency["dependency"] = "serde"
        errors, _ = self.validate_dependency_fixture(
            {
                "crates/api/Cargo.toml": """
                    [package]
                    name = "tracedecay-memory-provider-api"
                    version = "0.0.0"

                    [dependencies]
                    serde = "1"
                """
            },
            exceptions=[nonmatching_dependency],
            adrs={adr: self.bound_dependency_adr(nonmatching_dependency)},
        )
        self.assertIn("is not a forbidden edge", "\n".join(errors))

    def test_upstream_floor_drift_is_rejected(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["upstream_floor"]["sha"] = "0" * 40
        self.assert_rejected("upstream floor must remain", policy=policy)

    def test_schema_v1_convergence_map_is_rejected(self) -> None:
        convergence_map = copy.deepcopy(self.convergence_map)
        convergence_map["schema_version"] = 1
        self.assert_rejected(
            "convergence-map schema_version must be integer 2",
            convergence_map=convergence_map,
        )

    def test_diff_includes_committed_staged_unstaged_and_untracked_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = Path(temp_dir)

            def run_git(*args: str) -> str:
                result = subprocess.run(
                    ["git", *args],
                    cwd=repo,
                    check=True,
                    capture_output=True,
                    text=True,
                )
                return result.stdout.strip()

            run_git("init", "--quiet")
            run_git("config", "user.name", "Patch Footprint Test")
            run_git("config", "user.email", "patch-footprint@example.invalid")
            for name in (
                "committed.rs",
                "staged.rs",
                "unstaged.rs",
                "cancelled.rs",
            ):
                (repo / name).write_text("before\n", encoding="utf-8")
            run_git("add", ".")
            run_git("commit", "--quiet", "-m", "floor")
            floor = run_git("rev-parse", "HEAD")

            (repo / "committed.rs").write_text("after\n", encoding="utf-8")
            run_git("add", "committed.rs")
            run_git("commit", "--quiet", "-m", "committed change")
            (repo / "staged.rs").write_text("after\n", encoding="utf-8")
            run_git("add", "staged.rs")
            (repo / "unstaged.rs").write_text("after\n", encoding="utf-8")
            (repo / "cancelled.rs").write_text("staged value\n", encoding="utf-8")
            run_git("add", "cancelled.rs")
            (repo / "cancelled.rs").write_text("before\n", encoding="utf-8")
            (repo / "untracked.rs").write_text("one\ntwo\n", encoding="utf-8")

            errors: list[str] = []
            stats = CHECKER_MODULE.diff_numstat(repo, floor, errors)
            self.assertEqual(errors, [])
            self.assertEqual(
                set(stats),
                {
                    "cancelled.rs",
                    "committed.rs",
                    "staged.rs",
                    "unstaged.rs",
                    "untracked.rs",
                },
            )
            self.assertEqual(stats["committed.rs"], (1, 1))
            self.assertEqual(stats["staged.rs"], (1, 1))
            self.assertEqual(stats["unstaged.rs"], (1, 1))
            self.assertEqual(stats["cancelled.rs"], (1, 1))
            self.assertEqual(stats["untracked.rs"], (2, 0))

    def test_codex_administration_is_measured_but_not_classified(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = Path(temp_dir)

            def run_git(*args: str) -> str:
                result = subprocess.run(
                    ["git", *args],
                    cwd=repo,
                    check=True,
                    capture_output=True,
                    text=True,
                )
                return result.stdout.strip()

            run_git("init", "--quiet")
            run_git("config", "user.name", "Patch Footprint Test")
            run_git("config", "user.email", "patch-footprint@example.invalid")
            (repo / "README.md").write_text("floor\n", encoding="utf-8")
            run_git("add", ".")
            run_git("commit", "--quiet", "-m", "floor")
            floor = run_git("rev-parse", "HEAD")

            plan = repo / ".codex" / "plans" / "bead.plan.md"
            plan.parent.mkdir(parents=True)
            plan.write_text("committed plan\n", encoding="utf-8")
            run_git("add", ".codex/plans/bead.plan.md")
            run_git("commit", "--quiet", "-m", "add administrative plan")
            snapshot = repo / ".codex" / "plan-graphs" / "snapshot.json"
            snapshot.parent.mkdir(parents=True)
            snapshot.write_text("{}\n", encoding="utf-8")

            errors: list[str] = []
            measured = CHECKER_MODULE.diff_numstat(repo, floor, errors)
            self.assertEqual(errors, [])
            self.assertEqual(
                set(measured),
                {
                    ".codex/plan-graphs/snapshot.json",
                    ".codex/plans/bead.plan.md",
                },
            )
            footprint = CHECKER_MODULE.validate_actual_footprint(
                repo,
                floor,
                copy.deepcopy(self.policy),
                {},
                {},
                {},
                {},
                errors,
            )
            self.assertEqual(errors, [])
            self.assertEqual(footprint["total_upstream_changed_lines"], 0)
            self.assertEqual(footprint["upstream_existing_production_files"], 0)

            nearby_source = repo / ".codex-source.rs"
            nearby_source.write_text("fn main() {}\n", encoding="utf-8")
            errors = []
            CHECKER_MODULE.validate_actual_footprint(
                repo,
                floor,
                copy.deepcopy(self.policy),
                {},
                {},
                {},
                {},
                errors,
            )
            self.assertIn(
                "upstream-owned changed file lacks active convergence entry: .codex-source.rs",
                errors,
            )
            self.assertNotIn(".codex/plans/bead.plan.md", "\n".join(errors))
            self.assertNotIn(".codex/plan-graphs/snapshot.json", "\n".join(errors))

    def test_v2_product_area_classifies_dirty_product_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = Path(temp_dir)
            subprocess.run(
                ["git", "init", "--quiet"], cwd=repo, check=True, capture_output=True
            )
            subprocess.run(
                ["git", "config", "user.name", "Patch Footprint Test"],
                cwd=repo,
                check=True,
            )
            subprocess.run(
                [
                    "git",
                    "config",
                    "user.email",
                    "patch-footprint@example.invalid",
                ],
                cwd=repo,
                check=True,
            )
            (repo / "README.md").write_text("floor\n", encoding="utf-8")
            subprocess.run(["git", "add", "."], cwd=repo, check=True)
            subprocess.run(
                ["git", "commit", "--quiet", "-m", "floor"], cwd=repo, check=True
            )
            floor = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=repo,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            product_file = repo / "feature" / "new.py"
            product_file.parent.mkdir()
            product_file.write_text("print('product')\n", encoding="utf-8")
            policy = {
                "product_owned_paths": [],
                "initial_budget": copy.deepcopy(self.policy["initial_budget"]),
                "convergence_map": copy.deepcopy(self.policy["convergence_map"]),
            }
            areas = {
                "feature": {
                    "status": "active",
                    "ownership_class": "product_owned",
                    "path_patterns": ["feature/**"],
                }
            }
            errors: list[str] = []
            footprint = CHECKER_MODULE.validate_actual_footprint(
                repo,
                floor,
                policy,
                {},
                {},
                {},
                areas,
                errors,
            )
            self.assertEqual(errors, [])
            self.assertEqual(
                footprint,
                {
                    "composition_root_files": 0,
                    "exception_zone_files": 0,
                    "total_upstream_changed_lines": 0,
                    "upstream_existing_production_files": 0,
                    "upstream_existing_test_or_fixture_files": 0,
                },
            )

    def test_active_entry_without_actual_diff_is_rejected(self) -> None:
        convergence_map = copy.deepcopy(self.convergence_map)
        path = "crates/tracedecay/src/daemon/service/project_runtime.rs"
        area = next(
            row for row in convergence_map["areas"] if row["id"] == "composition_mount"
        )
        area["path_patterns"].append(path)
        entry = copy.deepcopy(
            next(
                row
                for row in convergence_map["entries"]
                if row["path"] == "crates/tracedecay/src/daemon/project_composition.rs"
            )
        )
        entry["path"] = path
        convergence_map["entries"].append(entry)
        self.assert_rejected(
            f"active convergence entry has no current upstream diff: {path}",
            convergence_map=convergence_map,
        )

    def test_exception_entry_requires_adr_and_exception_evidence(self) -> None:
        convergence_map = copy.deepcopy(self.convergence_map)
        entry = copy.deepcopy(convergence_map["entries"][1])
        entry["path"] = "crates/tracedecay-store/src/lib.rs"
        entry["touch_point"] = "exception"
        entry["line_budget"] = 10
        entry.pop("generated", None)
        convergence_map["entries"].append(entry)
        self.assert_rejected(
            "must include exception evidence",
            convergence_map=convergence_map,
        )

    def test_duplicate_convergence_path_is_rejected(self) -> None:
        convergence_map = copy.deepcopy(self.convergence_map)
        entry = copy.deepcopy(convergence_map["entries"][0])
        convergence_map["entries"].append(copy.deepcopy(entry))
        self.assert_rejected(
            f"duplicate convergence-map path {entry['path']!r}",
            convergence_map=convergence_map,
        )

    def test_exception_zone_cannot_drop_adr_requirement(self) -> None:
        policy = copy.deepcopy(self.policy)
        zone = next(
            row
            for row in policy["exception_zones"]
            if row["id"] == "native_database_internals"
        )
        zone["required_exception_evidence"] = [
            value for value in zone["required_exception_evidence"] if "ADR" not in value
        ]
        self.assert_rejected("must require ADR evidence", policy=policy)


if __name__ == "__main__":
    unittest.main()
