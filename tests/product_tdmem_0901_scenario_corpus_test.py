#!/usr/bin/env python3
"""Focused validation for the tdmem-0901 deterministic scenario corpus.

This test deliberately uses only the Python standard library.  It validates
the corpus contract and fixture properties without starting Cargo, a provider,
an agent, or a network service.
"""

from __future__ import annotations

import hashlib
import json
import re
import unittest
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable


REPO = Path(__file__).resolve().parents[1]
CORPUS = REPO / "product/evaluation/coding-memory-scenarios.v1.json"
SCHEMA = REPO / "product/evaluation/coding-memory-scenarios.v1.schema.json"
TERMINAL_CONTRACT = (
    REPO / "product/contracts/memory-provider-v1/provider-terminal-contract.json"
)

EXPECTED_SCENARIOS = {
    "stale_project_change",
    "failed_approach",
    "cross_agent_reuse",
    "project_worktree_scope",
    "contradiction",
    "restart",
    "cancellation",
    "provider_corruption",
    "privacy_deletion",
}

SCENARIO_FIELDS = {
    "id",
    "category",
    "title",
    "fixture_id",
    "task",
    "source_scope_id",
    "target_scope_id",
    "steps",
    "observations",
    "code_evidence_revisions",
    "expected_admissible_behavior",
    "adjudication_rubric",
}

CANONICAL_TERMINAL_CODES = frozenset(
    entry["code"]
    for entry in json.loads(TERMINAL_CONTRACT.read_text(encoding="utf-8"))[
        "terminal_codes"
    ]
)

DIGEST = re.compile(r"^[0-9a-f]{64}$")
PROVIDER_IMPLEMENTATION_NAMES = re.compile(r"(?i)\b(?:native|ncm|ocean)\b")
SECRET_MARKERS = (
    re.compile(r"-----BEGIN(?: [A-Z]+)? PRIVATE KEY-----"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b"),
    re.compile(r"\bsk-[A-Za-z0-9]{20,}\b"),
    re.compile(r"(?i)\b(?:api[_-]?key|access[_-]?token|password|private[_-]?key)\s*[:=]"),
)


def strings(value: Any) -> Iterable[str]:
    """Yield every string nested in a JSON value for policy linting."""

    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for key, nested in value.items():
            yield from strings(key)
            yield from strings(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from strings(nested)


class CodingMemoryScenarioCorpusTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.raw = CORPUS.read_bytes()
        cls.corpus = json.loads(cls.raw.decode("utf-8"))
        cls.schema = json.loads(SCHEMA.read_text(encoding="utf-8"))

        cls.scopes = {
            scope["scope_id"]: scope for scope in cls.corpus["scope_catalog"]
        }
        cls.fixture_revisions: dict[str, dict[str, Any]] = {}
        cls.fixture_paths: dict[str, str] = {}
        for fixture in cls.corpus["fixtures"]:
            for source_file in fixture["files"]:
                for revision in source_file["revisions"]:
                    cls.fixture_revisions[revision["revision_id"]] = revision
                    cls.fixture_paths[revision["revision_id"]] = source_file["path"]

    def test_corpus_is_utf8_without_bom_and_has_stable_newline(self) -> None:
        self.assertFalse(self.raw.startswith(b"\xef\xbb\xbf"))
        self.assertNotIn(b"\r", self.raw)
        self.assertTrue(self.raw.endswith(b"\n"))
        self.assertEqual(json.loads(self.raw.decode("utf-8")), self.corpus)

    def test_metadata_declares_provider_neutral_deterministic_execution(self) -> None:
        self.assertEqual(self.corpus["schema_version"], 1)
        self.assertEqual(
            self.corpus["corpus_id"], "tracedecay.coding-memory.scenarios.v1"
        )
        self.assertEqual(self.corpus["bead_id"], "tdmem-0901")
        self.assertTrue(self.corpus["provider_neutral"])
        self.assertEqual(
            self.corpus["canonical_encoding"],
            "utf8_rfc8785_json_without_bom_with_lf",
        )
        self.assertEqual(
            self.corpus["provider_selection"],
            {
                "mode": "runner_supplied",
                "same_fixture_and_task_for_each_provider": True,
                "provider_identity_is_run_metadata": True,
                "provider_output_is_advisory": True,
                "observer_output_participates_in_adjudication": False,
            },
        )
        self.assertEqual(
            self.corpus["fixture_policy"],
            {
                "clock": "fixed_utc_timestamps",
                "randomness": "none",
                "network": "forbidden",
                "filesystem": "temporary_fixture_only",
                "external_processes": "forbidden",
                "credentials": "none",
                "source_material": "synthetic_and_versioned",
            },
        )

    def test_scenario_inventory_matches_bead_design(self) -> None:
        scenarios = self.corpus["scenarios"]
        self.assertEqual(len(scenarios), len(EXPECTED_SCENARIOS))
        self.assertEqual({scenario["id"] for scenario in scenarios}, EXPECTED_SCENARIOS)
        self.assertEqual(
            len({scenario["fixture_id"] for scenario in scenarios}), 1
        )
        for scenario in scenarios:
            self.assertEqual(set(scenario), SCENARIO_FIELDS)

    def test_schema_declares_the_same_required_top_level_contract(self) -> None:
        required = set(self.schema["required"])
        self.assertEqual(
            required,
            {
                "schema_version",
                "corpus_id",
                "bead_id",
                "canonical_encoding",
                "provider_neutral",
                "provider_selection",
                "fixture_policy",
                "adjudication_policy",
                "scope_catalog",
                "fixtures",
                "recall_requests",
                "scenarios",
            },
        )
        self.assertEqual(
            set(self.schema["$defs"]["scenario"]["required"]), SCENARIO_FIELDS
        )

    def test_fixture_bytes_and_digests_are_reproducible(self) -> None:
        self.assertEqual(len(self.corpus["fixtures"]), 1)
        fixture = self.corpus["fixtures"][0]
        self.assertEqual(
            hashlib.sha256(fixture["fixture_id"].encode("utf-8")).hexdigest(),
            fixture["fixture_digest"],
        )
        for source_file in fixture["files"]:
            revisions = source_file["revisions"]
            self.assertEqual(
                [revision["revision"] for revision in revisions],
                list(range(1, len(revisions) + 1)),
            )
            for revision in revisions:
                self.assertRegex(revision["content_sha256"], DIGEST)
                actual = hashlib.sha256(
                    revision["content"].encode("utf-8")
                ).hexdigest()
                self.assertEqual(actual, revision["content_sha256"])

    def test_scope_catalog_is_explicit_and_distinguishes_project_worktree_session(self) -> None:
        self.assertGreaterEqual(len(self.scopes), 4)
        for scope in self.scopes.values():
            for key in (
                "profile_id",
                "project_id",
                "repository_id",
                "worktree_id",
                "branch_ref",
                "agent_session_id",
            ):
                self.assertTrue(scope[key], key)
            self.assertGreaterEqual(scope["scope_revision"], 1)
        self.assertNotEqual(
            self.scopes["scope_main_agent_a"]["agent_session_id"],
            self.scopes["scope_main_agent_b"]["agent_session_id"],
        )
        self.assertNotEqual(
            self.scopes["scope_main_agent_a"]["worktree_id"],
            self.scopes["scope_sibling_worktree"]["worktree_id"],
        )
        self.assertNotEqual(
            self.scopes["scope_main_agent_a"]["project_id"],
            self.scopes["scope_other_project"]["project_id"],
        )

    def test_each_scenario_has_observations_revisions_behavior_and_rubric(self) -> None:
        for scenario in self.corpus["scenarios"]:
            with self.subTest(scenario=scenario["id"]):
                self.assertGreaterEqual(len(scenario["observations"]), 1)
                self.assertGreaterEqual(
                    len(scenario["code_evidence_revisions"]), 1
                )
                self.assertGreaterEqual(len(scenario["steps"]), 2)

                behavior = scenario["expected_admissible_behavior"]
                self.assertTrue(behavior["admission"])
                self.assertGreaterEqual(len(behavior["must"]), 2)
                self.assertGreaterEqual(len(behavior["must_not"]), 2)
                self.assertTrue(behavior["allowed_terminal_outcomes"])
                self.assertTrue(behavior["provider_effect"])

                rubric = scenario["adjudication_rubric"]
                self.assertEqual(rubric["rubric_id"], scenario["id"])
                self.assertEqual(rubric["version"], 1)
                self.assertEqual(rubric["mode"], "weighted_all_safety_checks")
                self.assertEqual(rubric["pass_threshold"], 1.0)
                self.assertGreaterEqual(len(rubric["checks"]), 3)
                self.assertEqual(
                    len({check["check_id"] for check in rubric["checks"]}),
                    len(rubric["checks"]),
                )
                self.assertAlmostEqual(
                    sum(check["weight"] for check in rubric["checks"]), 1.0
                )
                for check in rubric["checks"]:
                    self.assertGreater(check["weight"], 0)
                    self.assertTrue(check["pass_if"])
                    self.assertTrue(check["fail_if"])

    def test_observations_are_settled_scoped_ordered_and_digest_bound(self) -> None:
        for scenario in self.corpus["scenarios"]:
            with self.subTest(scenario=scenario["id"]):
                self.assertIn(scenario["source_scope_id"], self.scopes)
                self.assertIn(scenario["target_scope_id"], self.scopes)
                grouped: dict[str, list[int]] = defaultdict(list)
                observation_ids: set[str] = set()
                for observation in scenario["observations"]:
                    self.assertNotIn(observation["observation_id"], observation_ids)
                    observation_ids.add(observation["observation_id"])
                    self.assertEqual(observation["settlement"], "settled")
                    self.assertIn(observation["scope_id"], self.scopes)
                    self.assertRegex(observation["source_digest"], DIGEST)
                    grouped[observation["scope_id"]].append(
                        observation["source_sequence"]
                    )
                    self.assertIn(observation["source_revision"], self.fixture_revisions)
                    self.assertRegex(
                        observation["occurred_at"],
                        r"^2026-08-30T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
                    )
                    if observation["event_type"] != "provider_state_checked":
                        self.assertEqual(
                            observation["source_digest"],
                            self.fixture_revisions[observation["source_revision"]][
                                "content_sha256"
                            ],
                        )
                for sequence in grouped.values():
                    self.assertEqual(sequence, list(range(1, len(sequence) + 1)))

    def test_code_evidence_revisions_have_fixture_paths_and_parent_lineage(self) -> None:
        for scenario in self.corpus["scenarios"]:
            with self.subTest(scenario=scenario["id"]):
                revision_ids = {
                    revision["revision_id"]
                    for revision in scenario["code_evidence_revisions"]
                }
                self.assertEqual(
                    len(revision_ids), len(scenario["code_evidence_revisions"])
                )
                for revision in scenario["code_evidence_revisions"]:
                    self.assertIn(revision["revision_id"], self.fixture_revisions)
                    self.assertIn(revision["scope_id"], self.scopes)
                    self.assertTrue(revision["changed_files"])
                    for path in revision["changed_files"]:
                        self.assertRegex(path, r"^(src|tests|docs|notes)/")
                    parent = revision["parent_revision_id"]
                    if parent is not None:
                        self.assertIn(parent, self.fixture_revisions)
                    self.assertTrue(revision["evidence"])
                    for evidence in revision["evidence"]:
                        self.assertRegex(evidence["digest"], DIGEST)
                        self.assertIn(
                            evidence["status"], {"pass", "fail", "indeterminate"}
                        )

    def test_steps_are_a_reproducible_one_based_action_sequence(self) -> None:
        for scenario in self.corpus["scenarios"]:
            with self.subTest(scenario=scenario["id"]):
                self.assertEqual(
                    [step["step"] for step in scenario["steps"]],
                    list(range(1, len(scenario["steps"]) + 1)),
                )
                self.assertEqual(
                    scenario["steps"][-1]["action"], "adjudicate"
                )
                self.assertEqual(
                    scenario["steps"][-1]["rubric"], scenario["id"]
                )

    def test_every_step_matches_exactly_one_typed_action_shape(self) -> None:
        shapes = {
            "observe": ({"observation_id"}, {"operation_id"}),
            "advance_code": ({"revision_id"}, set()),
            "recall": ({"request_id"}, {"scope_id"}),
            "adjudicate": ({"rubric"}, set()),
            "open_new_agent_session": ({"scope_id"}, set()),
            "restart_provider": ({"restart_id"}, set()),
            "replay": ({"observation_id", "operation_id"}, set()),
            "begin_observation_batch": ({"batch_id"}, set()),
            "commit_item": ({"item_id"}, set()),
            "cancel": ({"cancel_id", "at_item"}, set()),
            "resume": ({"resume_cursor"}, set()),
            "load_provider_state": ({"state_id", "digest_status"}, set()),
            "health": ({"request_id"}, set()),
            "delete_by_source": ({"forget_source_key"}, set()),
            "verify_absence": ({"request_id"}, set()),
        }
        schema_actions = {
            shape["properties"]["action"]["const"]
            for shape in self.schema["$defs"]["step"]["oneOf"]
        }
        self.assertEqual(schema_actions, set(shapes))
        for scenario in self.corpus["scenarios"]:
            revision_ids = {
                revision["revision_id"]
                for revision in scenario["code_evidence_revisions"]
            }
            observation_ids = {
                observation["observation_id"]
                for observation in scenario["observations"]
            }
            for step in scenario["steps"]:
                with self.subTest(scenario=scenario["id"], step=step["step"]):
                    required, optional = shapes[step["action"]]
                    keys = set(step) - {"step", "action"}
                    self.assertTrue(required <= keys, step)
                    self.assertTrue(keys <= required | optional, step)
                    if "observation_id" in step:
                        self.assertIn(step["observation_id"], observation_ids)
                    if "revision_id" in step:
                        self.assertIn(step["revision_id"], revision_ids)
                    if "scope_id" in step:
                        self.assertIn(step["scope_id"], self.scopes)

    def test_recall_request_catalog_fully_specifies_every_request_step(self) -> None:
        catalog = {
            request["request_id"]: request
            for request in self.corpus["recall_requests"]
        }
        self.assertEqual(len(catalog), len(self.corpus["recall_requests"]))
        referenced: dict[str, int] = defaultdict(int)
        step_operations = {
            "recall": "recall",
            "verify_absence": "verify_absence",
            "health": "health",
        }
        for scenario in self.corpus["scenarios"]:
            latest_observation = max(
                observation["occurred_at"] for observation in scenario["observations"]
            )
            for step in scenario["steps"]:
                if step["action"] not in step_operations:
                    continue
                with self.subTest(scenario=scenario["id"], step=step["step"]):
                    self.assertIn(step["request_id"], catalog, step)
                    request = catalog[step["request_id"]]
                    referenced[step["request_id"]] += 1
                    self.assertEqual(request["scenario_id"], scenario["id"])
                    self.assertEqual(
                        request["operation"], step_operations[step["action"]]
                    )
                    expected_scope = step.get("scope_id", scenario["target_scope_id"])
                    self.assertEqual(request["scope_id"], expected_scope)
                    self.assertIn(request["scope_id"], self.scopes)
                    self.assertEqual(request["temporal_query"]["mode"], "current")
                    self.assertGreaterEqual(
                        request["temporal_query"]["evaluation_time"],
                        latest_observation,
                    )
                    self.assertRegex(
                        request["temporal_query"]["evaluation_time"],
                        r"^2026-08-30T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
                    )
                    self.assertGreaterEqual(request["policy_revision"], 1)
                    if request["operation"] == "health":
                        for absent in ("objective", "query", "budgets", "exclusions"):
                            self.assertNotIn(absent, request)
                        continue
                    self.assertEqual(request["objective"], "search")
                    self.assertEqual(request["query"], request["query"].strip())
                    self.assertTrue(request["query"])
                    self.assertTrue(
                        all(value >= 1 for value in request["budgets"].values()),
                        request["budgets"],
                    )
                    self.assertTrue(
                        all(value == [] for value in request["exclusions"].values()),
                        request["exclusions"],
                    )
        self.assertEqual(set(referenced), set(catalog))
        self.assertTrue(all(count == 1 for count in referenced.values()), referenced)

    def test_corpus_has_no_concrete_provider_names(self) -> None:
        for value in strings(self.corpus):
            self.assertIsNone(
                PROVIDER_IMPLEMENTATION_NAMES.search(value),
                value,
            )

    def test_corpus_has_no_credentials_or_secret_material(self) -> None:
        for value in strings(self.corpus):
            for marker in SECRET_MARKERS:
                self.assertIsNone(marker.search(value), value)

    def test_scenario_specific_safety_properties_are_present(self) -> None:
        required_terms = {
            "stale_project_change": ("stale", "superseded", "current"),
            "failed_approach": ("failed", "negative", "flaky"),
            "cross_agent_reuse": ("session", "author", "scope"),
            "project_worktree_scope": ("worktree", "project", "leak"),
            "contradiction": ("conflict", "authority", "current"),
            "restart": ("restart", "replay", "idempotent"),
            "cancellation": ("cancel", "partial", "resume"),
            "provider_corruption": ("corrupt", "checksum", "reset"),
            "privacy_deletion": ("delete", "influence", "snapshot"),
        }
        for scenario in self.corpus["scenarios"]:
            text = " ".join(strings(scenario)).lower()
            with self.subTest(scenario=scenario["id"]):
                for term in required_terms[scenario["id"]]:
                    self.assertIn(term, text)

    def test_allowed_terminal_outcomes_use_only_canonical_wire_values(self) -> None:
        for scenario in self.corpus["scenarios"]:
            with self.subTest(scenario=scenario["id"]):
                outcomes = scenario["expected_admissible_behavior"][
                    "allowed_terminal_outcomes"
                ]
                for outcome in outcomes:
                    self.assertIn(outcome, CANONICAL_TERMINAL_CODES, outcome)


if __name__ == "__main__":
    unittest.main()
