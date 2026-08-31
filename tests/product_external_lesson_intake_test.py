#!/usr/bin/env python3
"""Contract tests for source-linked external lesson intake."""

from __future__ import annotations

import copy
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any, Callable


REPO = Path(__file__).resolve().parents[1]
INTAKE = REPO / "product/upstream/external-lesson-intake.json"
SCHEMA = REPO / "product/upstream/external-lesson-intake.schema.json"
ISSUES = REPO / ".beads/issues.jsonl"
CHECKER = REPO / "scripts/product/check-external-lesson-intake.py"


class ExternalLessonIntakeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.intake = json.loads(INTAKE.read_text(encoding="utf-8"))
        cls.schema = json.loads(SCHEMA.read_text(encoding="utf-8"))

    def run_checker(
        self,
        intake: dict[str, Any] | None = None,
        schema: dict[str, Any] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        if intake is None and schema is None:
            intake_path = INTAKE
            schema_path = SCHEMA
            temporary = None
        else:
            temporary = tempfile.TemporaryDirectory()
            root = Path(temporary.name)
            intake_path = root / "external-lesson-intake.json"
            schema_path = root / "external-lesson-intake.schema.json"
            intake_path.write_text(
                json.dumps(intake or self.intake, indent=2) + "\n",
                encoding="utf-8",
            )
            schema_path.write_text(
                json.dumps(schema or self.schema, indent=2) + "\n",
                encoding="utf-8",
            )
        try:
            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--intake",
                    str(intake_path),
                    "--schema",
                    str(schema_path),
                    "--issues",
                    str(ISSUES),
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=30,
            )
        finally:
            if temporary is not None:
                temporary.cleanup()

    def mutate_lesson(
        self, index: int, mutation: Callable[[dict[str, Any]], None]
    ) -> dict[str, Any]:
        intake = copy.deepcopy(self.intake)
        mutation(intake["lessons"][index])
        return intake

    def assert_rejected(
        self,
        marker: str,
        *,
        intake: dict[str, Any] | None = None,
        schema: dict[str, Any] | None = None,
    ) -> None:
        result = self.run_checker(intake, schema)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(marker, result.stderr)

    def test_real_repository_intake_is_valid(self) -> None:
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(
            result.stdout.strip(),
            "external lesson intake valid: 2 lesson(s), 1 accepted, 1 rejected",
        )
        self.assertNotIn("sha256", result.stdout.casefold())
        self.assertNotIn("receipt", result.stdout.casefold())

    def test_source_commit_must_be_exact(self) -> None:
        intake = self.mutate_lesson(
            0, lambda lesson: lesson["source"].__setitem__("commit", "main")
        )
        self.assert_rejected(
            "must be an exact 40-character lowercase commit", intake=intake
        )

    def test_source_repository_must_be_stable_https(self) -> None:
        intake = self.mutate_lesson(
            0,
            lambda lesson: lesson["source"].__setitem__(
                "repository", "http://github.com/bleedingDev/biomem#main"
            ),
        )
        self.assert_rejected("must be a stable https repository URL", intake=intake)

    def test_source_link_must_pin_the_same_commit_and_path(self) -> None:
        intake = self.mutate_lesson(
            0,
            lambda lesson: lesson["source"]["evidence"][0].__setitem__(
                "source_url",
                "https://github.com/bleedingDev/biomem/blob/main/src/memory_module/text_memory.py",
            ),
        )
        self.assert_rejected(
            "source_url must link source_path at the exact commit", intake=intake
        )

    def test_source_evidence_links_must_be_unique(self) -> None:
        def mutate(lesson: dict[str, Any]) -> None:
            lesson["source"]["evidence"].append(
                copy.deepcopy(lesson["source"]["evidence"][0])
            )

        intake = self.mutate_lesson(0, mutate)
        self.assert_rejected(
            "evidence must not contain duplicate source links", intake=intake
        )

    def test_license_provenance_must_name_a_real_file(self) -> None:
        intake = self.mutate_lesson(
            0,
            lambda lesson: lesson["source"]["license"].__setitem__(
                "evidence_path", "product/upstream/missing-license-evidence.md"
            ),
        )
        self.assert_rejected("does not name a real file", intake=intake)

    def test_license_evidence_must_record_the_identity(self) -> None:
        intake = self.mutate_lesson(
            0,
            lambda lesson: lesson["source"]["license"].__setitem__(
                "identity", "Apache-2.0"
            ),
        )
        self.assert_rejected(
            "evidence_path does not record the license identity", intake=intake
        )

    def test_generic_invariant_cannot_embed_source_identity(self) -> None:
        intake = self.mutate_lesson(
            0,
            lambda lesson: lesson.__setitem__(
                "extracted_generic_invariant",
                "Biomem search must always be selected for every provider recall request.",
            ),
        )
        self.assert_rejected(
            "extracted_generic_invariant is source-specific", intake=intake
        )

    def test_source_identifiers_are_derived_not_self_declared(self) -> None:
        intake = self.mutate_lesson(
            0,
            lambda lesson: lesson["source"].__setitem__(
                "identifiers", ["external-source"]
            ),
        )
        self.assert_rejected(
            "identifiers must exactly match repository and adapter identifiers",
            intake=intake,
        )

    def test_target_must_be_provider_neutral(self) -> None:
        intake = self.mutate_lesson(
            0,
            lambda lesson: lesson["target"].__setitem__("id", "ncm.recall.v1"),
        )
        self.assert_rejected("target.id is source-specific", intake=intake)

    def test_target_contract_must_record_target_id(self) -> None:
        intake = self.mutate_lesson(
            0,
            lambda lesson: lesson["target"].__setitem__("id", "recall.unknown.v1"),
        )
        self.assert_rejected("contract_path does not record target id", intake=intake)

    def test_capability_target_rejects_contract_readme(self) -> None:
        intake = self.mutate_lesson(
            0,
            lambda lesson: lesson["target"].__setitem__(
                "contract_path", "product/contracts/memory-provider-v1/README.md"
            ),
        )
        self.assert_rejected(
            "must name a canonical *-contract.json capability authority",
            intake=intake,
        )

    def test_policy_target_rejects_unrelated_architecture_file(self) -> None:
        def mutate(lesson: dict[str, Any]) -> None:
            lesson["target"]["kind"] = "policy"
            lesson["target"]["id"] = "native_explicit_fact_log"
            lesson["target"]["contract_path"] = (
                "product/architecture/native-memory-surface-map.json"
            )

        intake = self.mutate_lesson(0, mutate)
        self.assert_rejected(
            "must name a canonical *-policy artifact or ADR authority",
            intake=intake,
        )

    def test_target_cannot_self_reference_the_intake(self) -> None:
        def mutate(lesson: dict[str, Any]) -> None:
            lesson["target"]["kind"] = "policy"
            lesson["target"]["contract_path"] = (
                "product/upstream/external-lesson-intake.json"
            )

        intake = self.mutate_lesson(0, mutate)
        self.assert_rejected(
            "contract_path cannot reference the intake itself", intake=intake
        )

    def test_source_assumption_must_stay_in_external_adapter(self) -> None:
        intake = self.mutate_lesson(
            0,
            lambda lesson: lesson["source_assumptions"][0].__setitem__(
                "adapter_path", "crates/tracedecay-memory-provider-api/src/lib.rs"
            ),
        )
        self.assert_rejected(
            "must stay inside a concrete external provider adapter", intake=intake
        )

    def test_accepted_lesson_requires_neutral_regression_test(self) -> None:
        intake = self.mutate_lesson(
            0, lambda lesson: lesson.__setitem__("neutral_regression_tests", [])
        )
        self.assert_rejected(
            "must contain a real neutral test for an accepted lesson", intake=intake
        )

    def test_regression_test_must_be_a_real_file(self) -> None:
        intake = self.mutate_lesson(
            0,
            lambda lesson: lesson["neutral_regression_tests"][0].__setitem__(
                "path", "tests/not-a-real-neutral-test.py"
            ),
        )
        self.assert_rejected("does not name a real file", intake=intake)

    def test_repository_paths_must_be_normalized_literals(self) -> None:
        for path in (
            "../tests/product_provider_recall_contract_test.py",
            "tests//product_provider_recall_contract_test.py",
            "tests/*.py",
            "C:/tests/product_provider_recall_contract_test.py",
        ):
            with self.subTest(path=path):
                intake = self.mutate_lesson(
                    0,
                    lambda lesson: lesson["neutral_regression_tests"][0].__setitem__(
                        "path", path
                    ),
                )
                self.assert_rejected(
                    "must be a normalized literal repository-relative POSIX path",
                    intake=intake,
                )

    def test_regression_test_path_cannot_be_source_specific(self) -> None:
        intake = self.mutate_lesson(
            0,
            lambda lesson: lesson["neutral_regression_tests"][0].__setitem__(
                "path", "tests/product_ncm_surface_audit_test.py"
            ),
        )
        self.assert_rejected("regression tests must be provider-neutral", intake=intake)

    def test_regression_test_path_must_be_executable_test_source(self) -> None:
        intake = self.mutate_lesson(
            0,
            lambda lesson: lesson["neutral_regression_tests"][0].__setitem__(
                "path", "tests/fixtures/context_eval_labeled.json"
            ),
        )
        self.assert_rejected(
            "must name an executable behavioral test file", intake=intake
        )

    def test_implementation_bead_must_exist(self) -> None:
        intake = self.mutate_lesson(
            0, lambda lesson: lesson.__setitem__("implementation_bead", "tdmem-9999")
        )
        self.assert_rejected("references unknown Beads issue tdmem-9999", intake=intake)

    def test_rejected_lesson_requires_substantive_rejection_rationale(self) -> None:
        intake = self.mutate_lesson(
            1,
            lambda lesson: lesson["decision"].__setitem__("rejection_rationale", "No."),
        )
        self.assert_rejected(
            "rejection_rationale must be a substantive explanation", intake=intake
        )

    def test_accepted_lesson_cannot_carry_rejection_rationale(self) -> None:
        intake = self.mutate_lesson(
            0,
            lambda lesson: lesson["decision"].__setitem__(
                "rejection_rationale", "This should not be present on acceptance."
            ),
        )
        self.assert_rejected(
            "rejection_rationale must be null for an accepted lesson", intake=intake
        )

    def test_copied_external_code_requires_provenance_records(self) -> None:
        def mutate(lesson: dict[str, Any]) -> None:
            lesson["code_use"]["mode"] = "copied_external_code"
            lesson["code_use"]["external_code_copied"] = True

        intake = self.mutate_lesson(0, mutate)
        self.assert_rejected(
            "copy_records must record provenance for copied external code",
            intake=intake,
        )

    def test_copy_record_source_requires_exact_commit_link(self) -> None:
        def mutate(lesson: dict[str, Any]) -> None:
            lesson["code_use"]["mode"] = "copied_external_code"
            lesson["code_use"]["external_code_copied"] = True
            lesson["code_use"]["copy_records"] = [
                {
                    "source_path": "src/memory_module/unlinked.py",
                    "destination_path": "product/upstream/external-lesson-intake.md",
                    "license_notice_path": "crates/tracedecay-memory-provider-ncm/audits/tdmem-0701-capability-matrix.json",
                }
            ]

        intake = self.mutate_lesson(0, mutate)
        self.assert_rejected(
            "must have an exact-commit source evidence link", intake=intake
        )

    def test_copy_record_license_notice_must_record_source_license(self) -> None:
        def mutate(lesson: dict[str, Any]) -> None:
            lesson["code_use"]["mode"] = "copied_external_code"
            lesson["code_use"]["external_code_copied"] = True
            lesson["code_use"]["copy_records"] = [
                {
                    "source_path": "src/memory_module/text_memory.py",
                    "destination_path": "product/upstream/external-lesson-intake.md",
                    "license_notice_path": "product/contracts/memory-provider-v1/provider-recall-contract.json",
                }
            ]

        intake = self.mutate_lesson(0, mutate)
        self.assert_rejected(
            "license_notice_path does not record the source license identity",
            intake=intake,
        )

    def test_copy_record_notice_must_bind_the_destination(self) -> None:
        def mutate(lesson: dict[str, Any]) -> None:
            lesson["code_use"]["mode"] = "copied_external_code"
            lesson["code_use"]["external_code_copied"] = True
            lesson["code_use"]["copy_records"] = [
                {
                    "source_path": "src/memory_module/text_memory.py",
                    "destination_path": "product/upstream/external-lesson-intake.md",
                    "license_notice_path": "crates/tracedecay-memory-provider-ncm/audits/tdmem-0701-capability-matrix.json",
                }
            ]

        intake = self.mutate_lesson(0, mutate)
        self.assert_rejected(
            "license_notice_path does not bind copied destination path", intake=intake
        )

    def test_rejected_lesson_cannot_copy_external_code(self) -> None:
        def mutate(lesson: dict[str, Any]) -> None:
            lesson["code_use"]["mode"] = "copied_external_code"
            lesson["code_use"]["external_code_copied"] = True

        intake = self.mutate_lesson(1, mutate)
        self.assert_rejected(
            "cannot copy external code for a rejected lesson", intake=intake
        )

    def test_lesson_fields_are_closed(self) -> None:
        intake = self.mutate_lesson(
            0, lambda lesson: lesson.__setitem__("approval_receipt", "not allowed")
        )
        self.assert_rejected("has unexpected fields", intake=intake)

    def test_lesson_ids_are_unique(self) -> None:
        intake = self.mutate_lesson(
            1,
            lambda lesson: lesson.__setitem__(
                "lesson_id", self.intake["lessons"][0]["lesson_id"]
            ),
        )
        self.assert_rejected("intake lesson IDs must be unique", intake=intake)

    def test_schema_root_remains_closed(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["additionalProperties"] = True
        self.assert_rejected(
            "intake schema root must reject additional properties", schema=schema
        )

    def test_schema_keeps_exact_commit_pattern(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["$defs"]["source"]["properties"]["commit"]["pattern"] = ".+"
        self.assert_rejected(
            "source commit must require an exact lowercase SHA-1", schema=schema
        )

    def test_nested_schema_records_remain_closed(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["$defs"]["source"]["additionalProperties"] = True
        self.assert_rejected(
            "source definition must reject additional properties", schema=schema
        )

    def test_schema_keeps_exact_decision_conditions(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["$defs"]["lesson"]["allOf"] = [{}, {}]
        self.assert_rejected("accepted/rejected conditions drifted", schema=schema)

    def test_schema_keeps_neutral_test_path_constraint(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["$defs"]["regression_test"]["properties"]["path"]["pattern"] = ".*"
        self.assert_rejected("regression_test.path pattern drifted", schema=schema)


if __name__ == "__main__":
    unittest.main()
