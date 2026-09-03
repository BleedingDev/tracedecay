#!/usr/bin/env python3
"""Contract tests for the foundational pluggable-memory ADR set."""

from __future__ import annotations

import copy
import json
import runpy
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
MANIFEST = REPO / "product/architecture/adr/manifest.json"
ISSUES = REPO / ".beads/issues.jsonl"
CHECKER = REPO / "scripts/product/check-foundational-adrs.py"
ADR_0009 = REPO / "product/architecture/adr/ADR-0009-ncm-isolated-local-process.md"
ADR_0013 = (
    REPO
    / "product/architecture/adr/ADR-0013-daemon-shutdown-touch-point-expansion.md"
)
ADR_0016 = (
    REPO
    / "product/architecture/adr/ADR-0016-daemon-shutdown-receipt-ordering-headroom.md"
)


class FoundationalAdrsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.document = json.loads(MANIFEST.read_text(encoding="utf-8"))

    def run_checker(
        self, document: dict[str, Any] | None = None
    ) -> subprocess.CompletedProcess[str]:
        if document is None:
            manifest_path = MANIFEST
            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--manifest",
                    str(manifest_path),
                    "--issues",
                    str(ISSUES),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "manifest.json"
            manifest_path.write_text(
                json.dumps(document, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--manifest",
                    str(manifest_path),
                    "--issues",
                    str(ISSUES),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

    def assert_rejected(self, document: dict[str, Any], marker: str) -> None:
        result = self.run_checker(document)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertFalse(receipt["ok"])
        self.assertIn(marker, "\n".join(receipt["errors"]))

    def decision(self, document: dict[str, Any], decision_id: str) -> dict[str, Any]:
        return next(row for row in document["decisions"] if row["id"] == decision_id)

    def topology_document_errors(self, text: str) -> list[str]:
        checker_module = runpy.run_path(
            str(CHECKER), run_name="foundational_adr_checker"
        )
        decision = copy.deepcopy(self.decision(self.document, "ADR-0009"))
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = Path(temp_dir)
            target = repo / decision["path"]
            target.parent.mkdir(parents=True)
            target.write_text(text, encoding="utf-8")
            errors: list[str] = []
            checker_module["validate_adr_files"](
                repo,
                {"ADR-0009": decision},
                errors,
            )
        return errors

    def cap_revision_document_errors(
        self, text: str, decision_id: str = "ADR-0013"
    ) -> list[str]:
        checker_module = runpy.run_path(
            str(CHECKER), run_name="foundational_adr_checker"
        )
        decision = copy.deepcopy(self.decision(self.document, decision_id))
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = Path(temp_dir)
            target = repo / decision["path"]
            target.parent.mkdir(parents=True)
            target.write_text(text, encoding="utf-8")
            errors: list[str] = []
            checker_module["validate_adr_files"](
                repo,
                {decision_id: decision},
                errors,
            )
        return errors

    def test_daemon_shutdown_cap_adr_cannot_be_dropped(self) -> None:
        document = copy.deepcopy(self.document)
        document["decisions"] = [
            row for row in document["decisions"] if row["id"] != "ADR-0013"
        ]
        self.assert_rejected(document, "foundational ADRs missing: ['ADR-0013']")

    def test_cap_adr_must_keep_the_numbers_it_approved(self) -> None:
        text = ADR_0013.read_text(encoding="utf-8")
        self.assertEqual(self.cap_revision_document_errors(text), [])
        weakened = text.replace(
            "Approved max changed lines: `320`",
            "Approved max changed lines: `900`",
        )
        self.assertNotEqual(weakened, text)
        self.assertIn(
            "ADR-0013 is missing required phrase 'Approved max changed lines: `320`'",
            self.cap_revision_document_errors(weakened),
        )

    def test_cap_adr_must_keep_the_adr_0011_sequencing_rule(self) -> None:
        text = ADR_0013.read_text(encoding="utf-8")
        weakened = text.replace(
            "ADR-0011 invariant 2 is upheld rather than amended",
            "ADR-0011 invariant 2 is relaxed for touch-point-local caps",
        )
        self.assertNotEqual(weakened, text)
        self.assertIn(
            "ADR-0013 is missing required phrase "
            "'ADR-0011 invariant 2 is upheld rather than amended'",
            self.cap_revision_document_errors(weakened),
        )

    def test_receipt_ordering_cap_adr_keeps_exact_measurement_and_order(self) -> None:
        text = ADR_0016.read_text(encoding="utf-8")
        self.assertEqual(self.cap_revision_document_errors(text, "ADR-0016"), [])
        weakened = text.replace(
            "Measured changed lines: `416`", "Measured changed lines: `400`"
        )
        self.assertNotEqual(weakened, text)
        self.assertIn(
            "ADR-0016 is missing required phrase 'Measured changed lines: `416`'",
            self.cap_revision_document_errors(weakened, "ADR-0016"),
        )

    def test_real_repository_adr_set_is_valid(self) -> None:
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertTrue(receipt["ok"])
        self.assertEqual(receipt["bead_id"], "tdmem-0106")
        self.assertEqual(receipt["status"], "accepted")
        # The foundational set grows as the program takes new decisions
        # (ADR-0010 parity projection, ADR-0011 patch-footprint v2, ADR-0012
        # configuration-registry exception). The floor is what matters.
        self.assertGreaterEqual(receipt["decision_count"], 9)
        self.assertGreaterEqual(receipt["verification_bead_count"], 30)
        self.assertEqual(receipt["ncm_topology_state"], "selected")
        self.assertEqual(receipt["ncm_production_admission"], "blocked")

    def test_missing_foundational_decision_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        document["decisions"] = [
            row for row in document["decisions"] if row["id"] != "ADR-0006"
        ]
        self.assert_rejected(document, "foundational ADRs missing")

    def test_duplicate_adr_id_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        document["decisions"][-1]["id"] = "ADR-0007"
        self.assert_rejected(document, "duplicate ADR id ADR-0007")

    def test_required_sections_cannot_be_weakened(self) -> None:
        document = copy.deepcopy(self.document)
        document["required_sections"].remove("Rejected alternatives")
        self.assert_rejected(document, "required_sections must be exactly")

    def test_unknown_verification_bead_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        self.decision(document, "ADR-0001")["verification_beads"].append(
            "tdmem-9999"
        )
        self.assert_rejected(document, "references unknown verification bead tdmem-9999")

    def test_direct_fact_store_rejection_is_mandatory(self) -> None:
        document = copy.deepcopy(self.document)
        decision = self.decision(document, "ADR-0001")
        decision["required_rejections"][0] = "Use a common database table"
        self.assert_rejected(
            document,
            "rejected-alternatives section is missing manifest rejection",
        )

    def test_provider_name_branching_rejection_is_mandatory(self) -> None:
        document = copy.deepcopy(self.document)
        decision = self.decision(document, "ADR-0003")
        decision["required_rejections"][1] = "Dispatch through transport names"
        self.assert_rejected(
            document,
            "rejected-alternatives section is missing manifest rejection",
        )

    def test_ncm_topology_cannot_be_preselected(self) -> None:
        document = copy.deepcopy(self.document)
        topology = self.decision(document, "ADR-0004")["ncm_topology"]
        topology["state"] = "selected"
        topology["selected_topology"] = "in_process_crate"
        self.assert_rejected(
            document,
            "NCM execution topology must remain deferred",
        )

    def test_ncm_gate_beads_are_fixed(self) -> None:
        document = copy.deepcopy(self.document)
        topology = self.decision(document, "ADR-0004")["ncm_topology"]
        topology["decision_gate_beads"] = ["tdmem-0702"]
        self.assert_rejected(
            document,
            "NCM topology decision gate must be tdmem-0701 then tdmem-0702",
        )

    def test_ncm_selected_topology_cannot_drift(self) -> None:
        document = copy.deepcopy(self.document)
        topology = self.decision(document, "ADR-0009")["ncm_topology"]
        topology["selected_topology"] = "in_process_crate"
        self.assert_rejected(
            document,
            "ADR-0009.ncm_topology.selected_topology must be isolated_local_process",
        )

    def test_ncm_production_admission_cannot_be_claimed_early(self) -> None:
        document = copy.deepcopy(self.document)
        topology = self.decision(document, "ADR-0009")["ncm_topology"]
        topology["production_admission"] = "ready"
        self.assert_rejected(
            document,
            "ADR-0009.ncm_topology.production_admission must be blocked",
        )

    def test_ncm_restart_must_invalidate_readiness(self) -> None:
        document = copy.deepcopy(self.document)
        topology = self.decision(document, "ADR-0009")["ncm_topology"]
        topology["restart_readiness"] = "reuse_if_healthy"
        self.assert_rejected(
            document,
            "ADR-0009.ncm_topology.restart_readiness must be invalidated",
        )

    def test_ncm_restart_must_preserve_effect_evidence(self) -> None:
        document = copy.deepcopy(self.document)
        topology = self.decision(document, "ADR-0009")["ncm_topology"]
        topology["restart_effect_evidence"] = "discarded_with_incarnation"
        self.assert_rejected(
            document,
            "ADR-0009.ncm_topology.restart_effect_evidence must be durable_and_queryable",
        )

    def test_ncm_child_authority_denials_cannot_be_weakened(self) -> None:
        document = copy.deepcopy(self.document)
        topology = self.decision(document, "ADR-0009")["ncm_topology"]
        topology["denied_authorities"].remove("tracedecay_databases")
        self.assert_rejected(
            document,
            "denied authorities must exactly forbid host and TraceDecay authority",
        )

    def test_ncm_child_authority_denials_reject_non_strings(self) -> None:
        document = copy.deepcopy(self.document)
        topology = self.decision(document, "ADR-0009")["ncm_topology"]
        topology["denied_authorities"][0] = {"path_exists": True}
        self.assert_rejected(
            document,
            "denied authorities must exactly forbid host and TraceDecay authority",
        )

    def test_ncm_production_blockers_cannot_be_weakened(self) -> None:
        document = copy.deepcopy(self.document)
        topology = self.decision(document, "ADR-0009")["ncm_topology"]
        topology["required_blockers"].remove("atomic_persistence")
        self.assert_rejected(
            document,
            "production blockers must preserve exact-scope, identity, cancellation/effect, dedupe, persistence, and supervisor gaps",
        )

    def test_ncm_exact_scope_isolation_remains_a_production_blocker(self) -> None:
        document = copy.deepcopy(self.document)
        topology = self.decision(document, "ADR-0009")["ncm_topology"]
        topology["required_blockers"].remove("exact_scope_isolation")
        self.assert_rejected(
            document,
            "production blockers must preserve exact-scope, identity, cancellation/effect, dedupe, persistence, and supervisor gaps",
        )

    def test_ncm_rejected_alternatives_are_exact(self) -> None:
        document = copy.deepcopy(self.document)
        decision = self.decision(document, "ADR-0009")
        decision["required_rejections"].append("shared daemon with tags")
        self.assert_rejected(
            document,
            "required_rejections must exactly reject in-process Biomem and MCP stdio",
        )

    def test_ncm_topology_evidence_cannot_be_replaced_by_presence(self) -> None:
        document = copy.deepcopy(self.document)
        decision = self.decision(document, "ADR-0009")
        decision["evidence_sources"][0] = "product/architecture/adr/manifest.json"
        self.assert_rejected(
            document,
            "evidence_sources must exactly bind the audit and contracts",
        )

    def test_ncm_migration_path_is_semantically_required(self) -> None:
        text = ADR_0009.read_text(encoding="utf-8")
        start = text.index("\n## Migration path\n")
        end = text.index("\n## Invariants\n", start)
        errors = self.topology_document_errors(text[:start] + text[end:])
        self.assertIn(
            "ADR-0009 is missing non-empty topology section 'Migration path'",
            errors,
        )

    def test_ncm_production_non_admission_cannot_be_negated_in_prose(self) -> None:
        text = ADR_0009.read_text(encoding="utf-8").replace(
            "It does not admit NCM to production.",
            "It admits NCM to production.",
        )
        errors = self.topology_document_errors(text)
        self.assertIn(
            "ADR-0009 'Decision' section is missing semantic requirement 'It does not admit NCM to production.'",
            errors,
        )

    def test_ncm_process_isolation_cannot_claim_source_protection(self) -> None:
        text = ADR_0009.read_text(encoding="utf-8").replace(
            "Process isolation is not a source-protection or IP boundary; it is selected",
            "Process isolation protects the installed source and is selected",
        )
        errors = self.topology_document_errors(text)
        self.assertIn(
            "ADR-0009 'Consequences' section is missing semantic requirement 'Process isolation is not a source-protection or IP boundary'",
            errors,
        )

    def test_malformed_verification_beads_return_structured_errors(self) -> None:
        document = copy.deepcopy(self.document)
        self.decision(document, "ADR-0009")["verification_beads"] = None
        self.assert_rejected(document, "ADR-0009.verification_beads must be an array")

    def test_malformed_adr_dependencies_return_structured_errors(self) -> None:
        document = copy.deepcopy(self.document)
        self.decision(document, "ADR-0009")["depends_on_adrs"] = [{}]
        self.assert_rejected(
            document,
            "ADR-0009 depends_on_adrs entries must be ADR ids",
        )

    def test_forward_adr_dependency_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        self.decision(document, "ADR-0002")["depends_on_adrs"].append("ADR-0008")
        self.assert_rejected(
            document,
            "must depend only on an earlier foundational ADR",
        )

    def test_missing_m0_source_authority_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        document["source_authorities"][-1] = "product/does-not-exist.json"
        self.assert_rejected(
            document,
            "source_authorities must exactly match the M0 evidence authorities",
        )

    def test_adr_status_drift_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        self.decision(document, "ADR-0007")["status"] = "draft"
        self.assert_rejected(document, "ADR-0007.status must be accepted")


if __name__ == "__main__":
    unittest.main()
