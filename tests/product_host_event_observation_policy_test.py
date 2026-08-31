#!/usr/bin/env python3
"""Contract tests for the tdmem-0501 host event observation policy.

This test deliberately uses only the Python standard library (no
``jsonschema`` dependency), mirroring the structural validation approach in
``tests/product_provider_observation_contract_test.py``: it walks the policy
document by hand and enforces the shape the schema demands, plus the
cross-contract invariants that a pure schema check cannot express.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
POLICY = REPO / "product/observations/host-event-observation-policy.json"
SCHEMA = REPO / "product/observations/host-event-observation-policy.schema.json"
CONTRACT = (
    REPO
    / "product/contracts/memory-provider-v1/provider-observation-contract.json"
)


class HostEventObservationPolicyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.policy: dict[str, Any] = json.loads(POLICY.read_text(encoding="utf-8"))
        cls.schema: dict[str, Any] = json.loads(SCHEMA.read_text(encoding="utf-8"))
        cls.contract: dict[str, Any] = json.loads(
            CONTRACT.read_text(encoding="utf-8")
        )

    # -- basic document shape -------------------------------------------------

    def test_documents_parse_as_json_objects(self) -> None:
        self.assertIsInstance(self.policy, dict)
        self.assertIsInstance(self.schema, dict)
        self.assertIsInstance(self.contract, dict)

    def test_policy_carries_required_top_level_keys(self) -> None:
        required = self.schema["required"]
        for key in required:
            self.assertIn(key, self.policy, f"policy is missing required key {key!r}")

    def test_policy_identity_matches_schema_constants(self) -> None:
        self.assertEqual(self.policy["schema_version"], 1)
        self.assertEqual(
            self.policy["contract_id"], "tracedecay.observation-policy.v1"
        )
        self.assertEqual(self.policy["bead_id"], "tdmem-0501")
        self.assertEqual(self.policy["scope"], "coding_agents_only")

    def test_event_classes_cover_exactly_twenty_seven_events(self) -> None:
        event_classes = self.policy["event_classes"]
        self.assertEqual(len(event_classes), 27)
        event_ids = [row["event_id"] for row in event_classes]
        self.assertEqual(len(event_ids), len(set(event_ids)), "duplicate event_id")

    # -- admit/exclude invariant ------------------------------------------------

    def test_admit_exclude_invariant_holds_for_every_event_class(self) -> None:
        for row in self.policy["event_classes"]:
            disposition = row["disposition"]
            self.assertIn(disposition, ("admit", "exclude"))
            if disposition == "admit":
                self.assertIsNotNone(
                    row.get("canonical_commit_point"),
                    f"{row['event_id']} is admitted but has no canonical_commit_point",
                )
                self.assertIsInstance(row["canonical_commit_point"], dict)
                self.assertIsNotNone(
                    row.get("observation_kind"),
                    f"{row['event_id']} is admitted but has no observation_kind",
                )
                self.assertIsNone(
                    row.get("exclusion_reason"),
                    f"{row['event_id']} is admitted but carries an exclusion_reason",
                )
                self.assertEqual(
                    row["scope_derivation"]["mode"], "required_authoritative_receipt"
                )
                self.assertEqual(
                    row["source_provenance_derivation"]["mode"],
                    "required_authoritative_receipt",
                )
            else:
                self.assertIsNotNone(
                    row.get("exclusion_reason"),
                    f"{row['event_id']} is excluded but has no exclusion_reason",
                )
                self.assertIsInstance(row["exclusion_reason"], dict)
                self.assertIn("code", row["exclusion_reason"])
                self.assertIn("description", row["exclusion_reason"])
                self.assertIn("boundary", row["exclusion_reason"])
                self.assertIn("retry_policy", row["exclusion_reason"])
                self.assertIsNone(
                    row.get("observation_kind"),
                    f"{row['event_id']} is excluded but carries an observation_kind",
                )
                self.assertIsNone(
                    row.get("canonical_commit_point"),
                    f"{row['event_id']} is excluded but carries a canonical_commit_point",
                )
                self.assertEqual(
                    row["scope_derivation"]["mode"], "not_applicable_before_exclusion"
                )
                self.assertEqual(
                    row["source_provenance_derivation"]["mode"],
                    "not_applicable_before_exclusion",
                )

    def test_admitted_and_excluded_counts_match_event_coverage(self) -> None:
        event_classes = self.policy["event_classes"]
        admitted = [row for row in event_classes if row["disposition"] == "admit"]
        excluded = [row for row in event_classes if row["disposition"] == "exclude"]
        coverage = self.policy["event_coverage"]
        self.assertEqual(
            sorted(row["event_id"] for row in admitted),
            sorted(coverage["admitted_event_ids"]),
        )
        self.assertEqual(
            sorted(row["event_id"] for row in excluded),
            sorted(coverage["excluded_event_ids"]),
        )
        self.assertEqual(len(admitted) + len(excluded), 27)

    # -- cross-contract alignment with provider-observation-contract.json -------

    def test_every_admitted_observation_kind_is_registered_in_the_contract(
        self,
    ) -> None:
        contract_pairs = {
            (row["id"], row["source_authority"])
            for row in self.contract["observation_kinds"]
        }
        admitted = [
            row for row in self.policy["event_classes"] if row["disposition"] == "admit"
        ]
        self.assertTrue(admitted)
        for row in admitted:
            pair = (row["observation_kind"], row["source_authority"])
            self.assertIn(
                pair,
                contract_pairs,
                f"{row['event_id']} declares (observation_kind, source_authority) "
                f"{pair!r} which is not registered in the provider observation "
                "contract's observation_kinds",
            )

    def test_admitted_source_provenance_required_fields_match_contract_source_identity(
        self,
    ) -> None:
        contract_required_fields = self.contract["source_identity"]["required_fields"]
        admitted = [
            row for row in self.policy["event_classes"] if row["disposition"] == "admit"
        ]
        self.assertTrue(admitted)
        for row in admitted:
            self.assertEqual(
                row["source_provenance_derivation"]["required_fields"],
                contract_required_fields,
                f"{row['event_id']} source_provenance_derivation.required_fields "
                "does not byte-for-byte match provider-observation-contract.json "
                "source_identity.required_fields",
            )

    # -- filesystem grounding for canonical_commit_point -------------------------

    def test_every_canonical_commit_point_source_path_exists_on_disk(self) -> None:
        admitted = [
            row for row in self.policy["event_classes"] if row["disposition"] == "admit"
        ]
        self.assertTrue(admitted)
        checked = 0
        for row in admitted:
            commit_point = row["canonical_commit_point"]
            for source_path in commit_point["source_paths"]:
                checked += 1
                resolved = REPO / source_path
                self.assertTrue(
                    resolved.exists(),
                    f"{row['event_id']} canonical_commit_point.source_paths "
                    f"references {source_path!r} which does not exist relative "
                    "to the repository root",
                )
        self.assertGreater(checked, 0)

    def test_host_family_source_paths_exist_on_disk(self) -> None:
        for host in self.policy["host_families"]:
            for source_path in host["source_paths"]:
                resolved = REPO / source_path
                self.assertTrue(
                    resolved.exists(),
                    f"host family {host['host_id']} source_paths references "
                    f"{source_path!r} which does not exist relative to the "
                    "repository root",
                )


if __name__ == "__main__":
    unittest.main()
