#!/usr/bin/env python3
"""Contract and mutation tests for tdmem-0201 provider capability registry."""
from __future__ import annotations

import copy
import json
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parents[1]
CONTRACT_REL = Path("product/contracts/memory-provider-v1/provider-registry-contract.json")
SCHEMA_REL = Path("product/contracts/memory-provider-v1/provider-registry-contract.schema.json")
README_REL = Path("product/contracts/memory-provider-v1/README.md")
VALIDATOR_REL = Path("scripts/product/check-provider-registry-contract.py")
ISSUES_REL = Path(".beads/issues.jsonl")
BEAD_RE = re.compile(r"tdmem-[0-9]{4}")


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise AssertionError(f"expected object at {path}")
    return value


class ProviderRegistryContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="tdmem-0201-")
        self.repo = Path(self._tmp.name)
        for relative in (CONTRACT_REL, SCHEMA_REL, README_REL, VALIDATOR_REL):
            source = HERE / relative
            target = self.repo / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)

        contract_text = (self.repo / CONTRACT_REL).read_text(encoding="utf-8")
        issue_ids = sorted(set(BEAD_RE.findall(contract_text)) | {"tdmem-0201"})
        issues = self.repo / ISSUES_REL
        issues.parent.mkdir(parents=True, exist_ok=True)
        issues.write_text(
            "".join(json.dumps({"id": issue_id}, separators=(",", ":")) + "\n" for issue_id in issue_ids),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def run_validator(self) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                "python3",
                str(self.repo / VALIDATOR_REL),
                "--repo",
                str(self.repo),
                "--contract",
                str(CONTRACT_REL),
                "--schema",
                str(SCHEMA_REL),
                "--readme",
                str(README_REL),
                "--issues",
                str(ISSUES_REL),
            ],
            cwd=self.repo,
            text=True,
            capture_output=True,
            timeout=30,
            check=False,
        )

    def mutate_contract(self, mutation: Callable[[dict[str, Any]], None]) -> None:
        path = self.repo / CONTRACT_REL
        contract = load_json(path)
        mutation(contract)
        path.write_text(json.dumps(contract, indent=2) + "\n", encoding="utf-8")

    def mutate_schema(self, mutation: Callable[[dict[str, Any]], None]) -> None:
        path = self.repo / SCHEMA_REL
        schema = load_json(path)
        mutation(schema)
        path.write_text(json.dumps(schema, indent=2) + "\n", encoding="utf-8")

    def assert_rejected(self, expected: str | None = None) -> None:
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        if expected is not None:
            self.assertIn(expected, result.stderr)

    def capability(self, capability_id: str) -> dict[str, Any]:
        contract = load_json(self.repo / CONTRACT_REL)
        registry = contract["capability_registry"]
        for group in ("mandatory", "optional"):
            for capability in registry[group]:
                if capability["id"] == capability_id:
                    return capability
        raise AssertionError(f"capability {capability_id} not found")

    def test_repository_contract_is_valid(self) -> None:
        result = self.run_validator()
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = json.loads(result.stdout)
        self.assertEqual(summary["mandatory_capabilities"], 3)
        self.assertEqual(summary["optional_capabilities"], 12)
        self.assertEqual(summary["bootstrap_slots"], 3)

    def test_mandatory_and_optional_capabilities_are_disjoint(self) -> None:
        contract = load_json(self.repo / CONTRACT_REL)
        mandatory = {row["id"] for row in contract["capability_registry"]["mandatory"]}
        optional = {row["id"] for row in contract["capability_registry"]["optional"]}
        self.assertEqual(mandatory, {"provider.health.v1", "observation.accept.v1", "recall.query.v1"})
        self.assertFalse(mandatory & optional)
        projected = {row["id"] for row in contract["capability_catalog"]}
        self.assertEqual(projected, mandatory | optional)

    def test_every_capability_has_io_failures_and_compatibility(self) -> None:
        contract = load_json(self.repo / CONTRACT_REL)
        for group in ("mandatory", "optional"):
            for capability in contract["capability_registry"][group]:
                with self.subTest(capability=capability["id"]):
                    self.assertGreaterEqual(len(capability["inputs"]), 1)
                    self.assertGreaterEqual(len(capability["outputs"]), 1)
                    self.assertIn("capability_unsupported", capability["failure_modes"])
                    self.assertEqual(capability["compatibility_rules"]["capability_major"], 1)
                    self.assertEqual(
                        capability["compatibility_rules"]["activation_rule"],
                        "known_catalog_entry_and_explicit_registration_revision_and_explicit_selection",
                    )

    def test_unknown_capability_round_trips_opaque_and_inert(self) -> None:
        contract = load_json(self.repo / CONTRACT_REL)
        policy = contract["unknown_capability_contract"]
        unknown = {
            "id": "vendor.experimental.v9",
            "canonical_payload": {
                "nested": [1, {"opaque": True}],
                "bytes_b64": "AAECAw==",
                "future_field": "retained verbatim",
            },
        }
        wire = json.dumps(unknown, sort_keys=True, separators=(",", ":"))
        decoded = json.loads(wire)
        encoded = json.dumps(decoded, sort_keys=True, separators=(",", ":"))
        self.assertEqual(encoded, wire)
        self.assertEqual(policy["decode_policy"], "preserve_canonical_payload_opaque")
        self.assertEqual(policy["encode_policy"], "round_trip_canonical_payload_without_semantic_rewrite")
        self.assertFalse(policy["may_activate_from_presence"])
        self.assertFalse(policy["may_satisfy_required_capability"])
        self.assertEqual(policy["selection_policy"], "return_typed_capability_unsupported")

    def test_rejects_mandatory_optional_overlap(self) -> None:
        def mutation(contract: dict[str, Any]) -> None:
            contract["capability_registry"]["optional"].append(
                copy.deepcopy(contract["capability_registry"]["mandatory"][0])
            )
        self.mutate_contract(mutation)
        self.assert_rejected("sets overlap")

    def test_rejects_catalog_projection_drift(self) -> None:
        self.mutate_contract(lambda contract: contract["capability_catalog"].pop())
        self.assert_rejected("must exactly project")

    def test_rejects_missing_mandatory_health(self) -> None:
        self.mutate_contract(
            lambda contract: contract["capability_registry"].__setitem__(
                "mandatory",
                [row for row in contract["capability_registry"]["mandatory"] if row["id"] != "provider.health.v1"],
            )
        )
        self.assert_rejected("mandatory capability set drifted")

    def test_rejects_missing_input_contract(self) -> None:
        self.mutate_contract(lambda contract: contract["capability_registry"]["mandatory"][0].__setitem__("inputs", []))
        self.assert_rejected("inputs must define at least one field")

    def test_rejects_missing_output_contract(self) -> None:
        self.mutate_contract(lambda contract: contract["capability_registry"]["mandatory"][0].__setitem__("outputs", []))
        self.assert_rejected("outputs must define at least one field")

    def test_rejects_missing_standard_failure_modes(self) -> None:
        self.mutate_contract(
            lambda contract: contract["capability_registry"]["mandatory"][0].__setitem__(
                "failure_modes", ["provider_unavailable"]
            )
        )
        self.assert_rejected("common typed terminal modes")

    def test_rejects_implicit_capability_activation(self) -> None:
        self.mutate_contract(
            lambda contract: contract["capability_registry"]["mandatory"][0]["compatibility_rules"].__setitem__(
                "activation_rule", "activate_from_presence"
            )
        )
        self.assert_rejected("compatibility_rules.activation_rule")

    def test_rejects_unknown_declaration_rejection(self) -> None:
        self.mutate_contract(
            lambda contract: contract["capability_identity"].__setitem__("unknown_declaration_policy", "reject")
        )
        self.assert_rejected("preserve opaque inert")

    def test_rejects_unknown_capability_activation(self) -> None:
        self.mutate_contract(
            lambda contract: contract["unknown_capability_contract"].__setitem__("may_activate_from_presence", True)
        )
        self.assert_rejected("must be False")

    def test_rejects_unknown_capability_as_supported(self) -> None:
        self.mutate_contract(
            lambda contract: contract["unknown_capability_contract"].__setitem__("may_satisfy_required_capability", True)
        )
        self.assert_rejected("must be False")

    def test_rejects_unknown_capability_selection_success(self) -> None:
        self.mutate_contract(
            lambda contract: contract["unknown_capability_contract"].__setitem__("selection_policy", "resolve")
        )
        self.assert_rejected("selection_policy")

    def test_rejects_unversioned_capability_identity(self) -> None:
        self.mutate_contract(
            lambda contract: contract["capability_registry"]["mandatory"][0].__setitem__("id", "provider.health")
        )
        self.assert_rejected("not canonical versioned capability identity")

    def test_rejects_provider_named_capability_identity(self) -> None:
        self.mutate_contract(
            lambda contract: contract["capability_registry"]["mandatory"][0].__setitem__("id", "ncm.recall.v1")
        )
        self.assert_rejected("branches on a concrete provider name")

    def test_rejects_provider_self_declared_or_widened_recall_scope_bindings(self) -> None:
        self.mutate_contract(
            lambda contract: contract["registration_contract"]["recall_scope_bindings"].__setitem__(
                "provider_may_self_declare", True
            )
        )
        self.assert_rejected("providers cannot self-declare recall scope bindings")

    def test_rejects_widened_native_recall_scope_bindings(self) -> None:
        self.mutate_contract(
            lambda contract: contract["registration_contract"]["recall_scope_bindings"][
                "provider_declarations"
            ]["tracedecay.native"].append("exact_coding_scope")
        )
        self.assert_rejected("tracedecay.native must be authorized for project_facts and profile_facts only")

    def test_rejects_ncm_recall_scope_bindings_beyond_exact_coding_scope(self) -> None:
        self.mutate_contract(
            lambda contract: contract["registration_contract"]["recall_scope_bindings"][
                "provider_declarations"
            ].__setitem__("ncm", ["project_facts"])
        )
        self.assert_rejected("ncm must be authorized for exact_coding_scope only")

    def test_rejects_registration_without_recall_scope_bindings_field(self) -> None:
        self.mutate_contract(
            lambda contract: contract["registration_contract"]["required_fields"].remove(
                "recall_scope_bindings"
            )
        )
        self.assert_rejected("registration required_fields drifted")

    def test_rejects_silent_fallback(self) -> None:
        self.mutate_contract(lambda contract: contract["selection_contract"].__setitem__("silent_fallback", True))
        self.assert_rejected("silent_fallback must be False")

    def test_rejects_ncm_gate_reordering(self) -> None:
        def mutation(contract: dict[str, Any]) -> None:
            slot = next(row for row in contract["bootstrap_slots"] if row["provider_id"] == "ncm")
            slot["implementation_gate_beads"] = ["tdmem-0702", "tdmem-0701", "tdmem-0703"]
        self.mutate_contract(mutation)
        self.assert_rejected("NCM must remain reserved")

    def test_rejects_speculative_ocean_implementation(self) -> None:
        def mutation(contract: dict[str, Any]) -> None:
            slot = next(row for row in contract["bootstrap_slots"] if row["provider_id"] == "ocean")
            slot["counts_as_implemented"] = True
        self.mutate_contract(mutation)
        self.assert_rejected("must not count as implemented")

    def test_rejects_schema_that_allows_unknown_top_level_fields(self) -> None:
        self.mutate_schema(lambda schema: schema.__setitem__("additionalProperties", True))
        self.assert_rejected("schema root must be strict object")

    def test_rejects_schema_that_allows_unknown_activation(self) -> None:
        def mutation(schema: dict[str, Any]) -> None:
            schema["$defs"]["unknownCapability"]["properties"]["may_activate_from_presence"] = {"type": "boolean"}
        self.mutate_schema(mutation)
        self.assert_rejected("schema must forbid activation")

    def test_rejects_unknown_verification_bead(self) -> None:
        self.mutate_contract(lambda contract: contract["verification_beads"].append("tdmem-9999"))
        self.assert_rejected("references unknown Beads issue tdmem-9999")

    def test_rejects_missing_round_trip_documentation(self) -> None:
        path = self.repo / README_REL
        path.write_text(path.read_text(encoding="utf-8").replace("Unknown capability round-trip", "Unknown capability handling"), encoding="utf-8")
        self.assert_rejected("README missing required phrase 'Unknown capability round-trip'")


if __name__ == "__main__":
    unittest.main()
