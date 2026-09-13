#!/usr/bin/env python3
"""Contract tests for provider health and lifecycle operations."""

from __future__ import annotations

import copy
import json
import re
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
CONTRACT = REPO / "product/contracts/memory-provider-v1/provider-lifecycle-contract.json"
SCHEMA = REPO / "product/contracts/memory-provider-v1/provider-lifecycle-contract.schema.json"
DOC = REPO / "product/contracts/memory-provider-v1/provider-lifecycle-contract.md"
ISSUES = REPO / ".beads/issues.jsonl"
CHECKER = REPO / "scripts/product/check-provider-lifecycle-contract.py"


class LocalSchemaValidator:
    """Minimal local-schema validator for concrete target-reference fixtures."""

    def __init__(self, schema: dict[str, Any]) -> None:
        self.schema = schema

    def resolve(self, reference: str) -> dict[str, Any]:
        if not reference.startswith("#/"):
            raise ValueError(f"unsupported external reference {reference}")
        node: Any = self.schema
        for part in reference[2:].split("/"):
            node = node[part]
        if not isinstance(node, dict):
            raise ValueError(f"reference does not resolve to an object: {reference}")
        return node

    def errors(
        self, value: Any, schema: dict[str, Any], path: str = "$"
    ) -> list[str]:
        if "$ref" in schema:
            return self.errors(value, self.resolve(schema["$ref"]), path)
        problems: list[str] = []
        if "enum" in schema and value not in schema["enum"]:
            problems.append(f"{path}: value is outside the closed enum")
        expected_type = schema.get("type")
        if expected_type is not None:
            types = expected_type if isinstance(expected_type, list) else [expected_type]
            if not any(self._is_type(value, type_name) for type_name in types):
                problems.append(f"{path}: wrong JSON type")
                return problems
        if isinstance(value, str):
            if "minLength" in schema and len(value) < schema["minLength"]:
                problems.append(f"{path}: shorter than minimum")
            if "maxLength" in schema and len(value) > schema["maxLength"]:
                problems.append(f"{path}: longer than maximum")
            if "pattern" in schema and re.search(schema["pattern"], value) is None:
                problems.append(f"{path}: does not match pattern")
        if isinstance(value, dict):
            for key in schema.get("required", []):
                if key not in value:
                    problems.append(f"{path}: missing required field {key}")
            properties = schema.get("properties", {})
            for key, nested in value.items():
                if key in properties:
                    problems.extend(self.errors(nested, properties[key], f"{path}.{key}"))
                elif schema.get("additionalProperties") is False:
                    problems.append(f"{path}: unexpected field {key}")
        return problems

    @staticmethod
    def _is_type(value: Any, type_name: str) -> bool:
        if type_name == "object":
            return isinstance(value, dict)
        if type_name == "string":
            return isinstance(value, str)
        return False


class ProviderLifecycleContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
        cls.schema = json.loads(SCHEMA.read_text(encoding="utf-8"))

    def run_checker(
        self,
        contract: dict[str, Any] | None = None,
        schema: dict[str, Any] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        if contract is None and schema is None:
            contract_path = CONTRACT
            schema_path = SCHEMA
            temporary = None
        else:
            temporary = tempfile.TemporaryDirectory()
            root = Path(temporary.name)
            contract_path = root / "contract.json"
            schema_path = root / "schema.json"
            contract_path.write_text(
                json.dumps(contract or self.contract, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            schema_path.write_text(
                json.dumps(schema or self.schema, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        try:
            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--contract",
                    str(contract_path),
                    "--schema",
                    str(schema_path),
                    "--doc",
                    str(DOC),
                    "--issues",
                    str(ISSUES),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
        finally:
            if temporary is not None:
                temporary.cleanup()

    def assert_rejected(
        self,
        contract: dict[str, Any],
        marker: str,
        schema: dict[str, Any] | None = None,
    ) -> None:
        result = self.run_checker(contract, schema)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertFalse(receipt["ok"])
        self.assertIn(marker, "\n".join(receipt["errors"]))

    def capability(self, contract: dict[str, Any], capability_id: str) -> dict[str, Any]:
        return next(
            row
            for row in contract["capability_gating"]["capability_to_operation"]
            if row["capability_id"] == capability_id
        )

    def test_real_repository_contract_is_valid(self) -> None:
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertTrue(receipt["ok"])
        self.assertEqual(
            receipt["contract_id"], "tracedecay.memory.provider.lifecycle.v1"
        )
        self.assertEqual(receipt["bead_id"], "tdmem-0205")
        self.assertEqual(receipt["lifecycle_capability_count"], 11)
        self.assertEqual(
            receipt["feedback_target_kinds"],
            [
                "stable_memory_ref",
                "retained_source_locator",
                "recall_trace_ref",
                "context_pack_item_ref",
            ],
        )
        self.assertEqual(
            receipt["correction_target_kinds"],
            [
                "stable_memory_ref",
                "retained_source_locator",
                "recall_trace_ref",
                "source_ref",
            ],
        )
        self.assertEqual(receipt["maintenance_task_count"], 6)
        self.assertTrue(receipt["forget_postcondition_required"])
        self.assertEqual(receipt["terminal_state_count"], 25)
        self.assertEqual(receipt["unsupported_outcome"], "capability_unsupported")

    def test_common_request_requires_deadline_and_cancellation(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["common_request"]["required_fields"].remove("deadline")
        contract["common_request"]["required_fields"].remove("cancellation")
        self.assert_rejected(contract, "lifecycle common request fields drifted")

    def test_provider_cannot_widen_scope(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["common_request"]["provider_may_widen_scope"] = True
        self.assert_rejected(contract, "common_request.provider_may_widen_scope must be false")

    def test_provider_cannot_mutate_tracedecay_authority(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["common_request"]["provider_may_mutate_tracedecay_authority"] = True
        self.assert_rejected(
            contract,
            "common_request.provider_may_mutate_tracedecay_authority must be false",
        )

    def test_unknown_optional_extension_must_round_trip(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["common_request"]["unknown_optional_extension_policy"] = "drop"
        self.assert_rejected(
            contract, "unknown optional lifecycle extensions must round-trip inertly"
        )

    def test_unknown_required_extension_must_fail(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["common_request"]["unknown_required_extension_policy"] = "ignore"
        self.assert_rejected(
            contract, "unknown required lifecycle extensions must fail explicitly"
        )

    def test_missing_capability_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["capability_gating"]["capability_to_operation"] = [
            row
            for row in contract["capability_gating"]["capability_to_operation"]
            if row["capability_id"] != "inspection.read.v1"
        ]
        self.assert_rejected(
            contract,
            "lifecycle capability map must exactly cover V1 lifecycle capabilities",
        )

    def test_health_remains_mandatory(self) -> None:
        contract = copy.deepcopy(self.contract)
        self.capability(contract, "provider.health.v1")["requirement"] = "optional"
        self.assert_rejected(
            contract, "capability provider.health.v1 requirement must be mandatory"
        )

    def test_unsupported_operation_never_falls_back(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["capability_gating"]["unsupported_operation_may_fallback"] = True
        self.assert_rejected(
            contract,
            "capability_gating.unsupported_operation_may_fallback must be false",
        )

    def test_health_process_existence_is_not_readiness(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["health"]["process_existence_proves_ready"] = True
        self.assert_rejected(contract, "health.process_existence_proves_ready must be false")

    def test_health_cannot_mutate_state(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["health"]["health_may_mutate_state"] = True
        self.assert_rejected(contract, "health.health_may_mutate_state must be false")

    def test_feedback_target_is_exactly_one(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["feedback"]["target"]["selection_rule"] = "one_or_more"
        self.assert_rejected(contract, "feedback target must select exactly one kind")

    def test_feedback_supports_context_pack_target(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["feedback"]["target"]["target_kinds"].remove(
            "context_pack_item_ref"
        )
        self.assert_rejected(contract, "feedback target kinds drifted")

    def test_feedback_supports_retained_source_locator_target(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["feedback"]["target"]["target_kinds"].remove(
            "retained_source_locator"
        )
        self.assert_rejected(contract, "feedback target kinds drifted")

    def test_correction_supports_retained_source_locator_target(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["correction"]["target_kinds"].remove("retained_source_locator")
        self.assert_rejected(contract, "correction target kinds drifted")

    def test_feedback_requires_settled_outcome(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["feedback"]["canonical_outcome_receipt_required"] = False
        self.assert_rejected(
            contract, "feedback requires canonically settled outcome receipt"
        )

    def test_feedback_cannot_change_native_trust(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["feedback"]["provider_may_change_native_trust"] = True
        self.assert_rejected(contract, "provider feedback cannot change Native trust")

    def test_feedback_is_idempotent(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["feedback"]["idempotent"] = False
        self.assert_rejected(contract, "feedback must be idempotent")

    def test_maintenance_is_bounded(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["maintenance"]["all_limits_positive_and_finite"] = False
        self.assert_rejected(
            contract, "maintenance.all_limits_positive_and_finite must be true"
        )

    def test_maintenance_cancellation_reaches_provider_loop(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["maintenance"]["deadline_and_cancellation_reach_provider_loop"] = False
        self.assert_rejected(
            contract,
            "maintenance.deadline_and_cancellation_reach_provider_loop must be true",
        )

    def test_unbounded_maintenance_scan_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["maintenance"]["unbounded_scan_allowed"] = True
        self.assert_rejected(contract, "unbounded maintenance scan must be false")

    def test_concurrent_maintenance_is_explicitly_busy(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["maintenance"]["concurrent_mutation_policy"] = "queue_unbounded"
        self.assert_rejected(
            contract, "concurrent maintenance mutation must be maintenance_busy"
        )

    def test_inspection_cannot_expose_credentials(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["inspection"]["raw_credentials_allowed"] = True
        self.assert_rejected(contract, "inspection.raw_credentials_allowed must be false")

    def test_inspection_requires_redaction(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["inspection"]["redaction_required"] = False
        self.assert_rejected(contract, "inspection redaction must be required")

    def test_correction_requires_expected_revision(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["correction"]["expected_target_revision_required"] = False
        self.assert_rejected(
            contract, "correction expected target revision must be required"
        )

    def test_correction_cannot_mutate_native_fact(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["correction"]["native_fact_mutation_allowed"] = True
        self.assert_rejected(
            contract, "correction.native_fact_mutation_allowed must be false"
        )

    def test_deletion_requires_snapshot_decision(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["deletion_by_source"]["include_snapshots_required"] = False
        self.assert_rejected(
            contract, "deletion request must explicitly address snapshots"
        )

    def test_deletion_cannot_report_unverified_success(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["deletion_by_source"][
            "provider_may_report_success_without_verification"
        ] = True
        self.assert_rejected(
            contract, "deletion cannot report success without verification"
        )

    def test_deletion_requires_zero_remaining_influence(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["deletion_by_source"]["verifiable_postcondition"][
            "successful_remove_requires_remaining_influence_zero"
        ] = False
        self.assert_rejected(
            contract,
            "deletion postcondition successful_remove_requires_remaining_influence_zero must be true",
        )

    def test_deleted_source_cannot_reappear(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["deletion_by_source"]["verifiable_postcondition"][
            "provider_recall_may_return_deleted_source"
        ] = True
        self.assert_rejected(
            contract,
            "deletion postcondition provider_recall_may_return_deleted_source must be false",
        )

    def test_deletion_cannot_delete_native_facts(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["deletion_by_source"]["native_fact_deletion_allowed"] = True
        self.assert_rejected(
            contract, "deletion_by_source.native_fact_deletion_allowed must be false"
        )

    def test_snapshot_restore_requires_exact_scope(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["snapshot"]["restore_requires_exact_scope"] = False
        self.assert_rejected(contract, "snapshot.restore_requires_exact_scope must be true")

    def test_snapshot_restore_cannot_implicitly_reset(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["snapshot"]["implicit_reset_allowed"] = True
        self.assert_rejected(contract, "snapshot.implicit_reset_allowed must be false")

    def test_snapshot_restore_cannot_implicitly_overwrite(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["snapshot"]["implicit_overwrite_allowed"] = True
        self.assert_rejected(contract, "snapshot.implicit_overwrite_allowed must be false")

    def test_replay_requires_monotonic_sequence(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["replay"]["sequence_monotonic_required"] = False
        self.assert_rejected(contract, "replay.sequence_monotonic_required must be true")

    def test_replay_gap_is_explicit(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["replay"]["sequence_gap_policy"] = "skip"
        self.assert_rejected(contract, "replay sequence gap must be explicit")

    def test_provider_projection_is_not_native_authority(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["provider_local_projection"][
            "explicit_projection_is_native_fact_authority"
        ] = True
        self.assert_rejected(
            contract,
            "provider_local_projection.explicit_projection_is_native_fact_authority must be false",
        )

    def test_cancellation_is_distinct_from_deadline(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["lifecycle_specific_terminal_states"].remove("cancelled")
        self.assert_rejected(
            contract, "lifecycle terminal states must exactly cover V1 outcomes"
        )

    def test_partial_and_unknown_effect_remain_explicit(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["lifecycle_specific_terminal_states"].remove("effect_unknown")
        self.assert_rejected(
            contract, "lifecycle terminal states must exactly cover V1 outcomes"
        )

    def test_unknown_verification_bead_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["verification_beads"].append("tdmem-9999")
        self.assert_rejected(
            contract,
            "verification_beads references unknown Beads issue tdmem-9999",
        )

    def test_schema_root_is_strict(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["additionalProperties"] = True
        self.assert_rejected(
            copy.deepcopy(self.contract),
            "lifecycle schema root must be a strict object",
            schema,
        )

    def test_schema_requires_deletion_contract(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["required"].remove("deletion_by_source")
        self.assert_rejected(
            copy.deepcopy(self.contract),
            "lifecycle schema required fields must match contract",
            schema,
        )

    def test_schema_supports_retained_source_locator_reference(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["$defs"]["lifecycleTargetReference"]["properties"]["kind"][
            "enum"
        ].remove("retained_source_locator")
        self.assert_rejected(
            copy.deepcopy(self.contract),
            "lifecycle target reference kinds drifted",
            schema,
        )

    def test_target_reference_schema_partitions_feedback_and_correction_kinds(self) -> None:
        validator = LocalSchemaValidator(self.schema)
        feedback_schema = self.schema["$defs"]["feedbackTargetReference"]
        correction_schema = self.schema["$defs"]["correctionTargetReference"]
        lifecycle_schema = self.schema["$defs"]["lifecycleTargetReference"]

        for kind in ("stable_memory_ref", "retained_source_locator", "recall_trace_ref"):
            target = {"kind": kind, "reference": "opaque-reference"}
            self.assertEqual(validator.errors(target, feedback_schema), [])
            self.assertEqual(validator.errors(target, correction_schema), [])
            self.assertEqual(validator.errors(target, lifecycle_schema), [])

        context_target = {
            "kind": "context_pack_item_ref",
            "reference": "opaque-reference",
        }
        self.assertEqual(validator.errors(context_target, feedback_schema), [])
        self.assertNotEqual(validator.errors(context_target, correction_schema), [])

        source_target = {"kind": "source_ref", "reference": "source-key"}
        self.assertEqual(validator.errors(source_target, correction_schema), [])
        self.assertNotEqual(validator.errors(source_target, feedback_schema), [])
        self.assertNotEqual(validator.errors(source_target, lifecycle_schema), [])

    def test_checker_requires_correction_only_source_ref_schema(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["$defs"]["correctionTargetReference"]["properties"]["kind"][
            "enum"
        ].remove("source_ref")
        self.assert_rejected(
            copy.deepcopy(self.contract),
            "correctionTargetReference kinds or shape drifted",
            schema,
        )

    def test_checker_requires_feedback_target_lifecycle_alias(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["$defs"]["feedbackTarget"] = {"$ref": "#/$defs/correctionTarget"}
        self.assert_rejected(
            copy.deepcopy(self.contract),
            "feedbackTarget must be an exact lifecycleTarget alias",
            schema,
        )

    def test_checker_requires_feedback_target_definition(self) -> None:
        schema = copy.deepcopy(self.schema)
        del schema["$defs"]["feedbackTarget"]
        self.assert_rejected(
            copy.deepcopy(self.contract),
            "feedbackTarget must be an exact lifecycleTarget alias",
            schema,
        )

    def test_checker_rejects_malformed_reference_kind_type(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["$defs"]["lifecycleTargetReference"]["properties"]["kind"][
            "type"
        ] = "integer"
        self.assert_rejected(
            copy.deepcopy(self.contract),
            "lifecycleTargetReference kind type must be string",
            schema,
        )

    def test_checker_rejects_malformed_correction_reference_link(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["$defs"]["correctionTargetReference"]["properties"]["reference"][
            "$ref"
        ] = "#/$defs/correctionTargetReference/properties/reference"
        self.assert_rejected(
            copy.deepcopy(self.contract),
            "correctionTargetReference reference mapping drifted",
            schema,
        )

    def test_checker_rejects_source_ref_in_shared_feedback_schema(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["$defs"]["lifecycleTargetReference"]["properties"]["kind"][
            "enum"
        ].append("source_ref")
        self.assert_rejected(
            copy.deepcopy(self.contract),
            "lifecycle target reference kinds drifted",
            schema,
        )


if __name__ == "__main__":
    unittest.main()
