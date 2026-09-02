#!/usr/bin/env python3
"""Focused validation for the tdmem-0904 coding-memory metric catalog.

This test deliberately uses only the Python standard library.  It validates
the catalog against its schema, pins the versioned metric identities and
denominators, and proves the catalog binds every rubric check of the
tdmem-0901 scenario corpus without starting Cargo or any provider.
"""

from __future__ import annotations

import json
import re
import unittest
from collections import Counter
from pathlib import Path
from typing import Any, Iterable


REPO = Path(__file__).resolve().parents[1]
CATALOG = REPO / "product/evaluation/coding-memory-metrics.v1.json"
SCHEMA = REPO / "product/evaluation/coding-memory-metrics.v1.schema.json"
CORPUS = REPO / "product/evaluation/coding-memory-scenarios.v1.json"
TERMINAL_CONTRACT = (
    REPO / "product/contracts/memory-provider-v1/provider-terminal-contract.json"
)
RUST_CATALOG_INCLUDE = REPO / "crates/tracedecay-memory-evaluation/src/catalog.rs"
WORKFLOW = REPO / ".github/workflows/product-memory-contracts.yml"

EXPECTED_METRICS = {
    "task_outcome": ("quality", False),
    "useful_recall_precision": ("quality", False),
    "harmful_stale_recall_rate": ("safety", True),
    "correction_latency": ("quality", False),
    "repeated_discovery_rate": ("cost", False),
    "context_tokens": ("cost", False),
    "recall_latency_p50": ("latency", False),
    "recall_latency_p95": ("latency", False),
    "human_curation_time": ("cost", False),
    "provenance_completeness": ("quality", False),
    "scope_leakage": ("safety", True),
    "corrupt_state_recall": ("safety", True),
    "deleted_source_recall": ("safety", True),
}

NONDETERMINISTIC_METRICS = {
    "correction_latency",
    "context_tokens",
    "recall_latency_p50",
    "recall_latency_p95",
    "human_curation_time",
}

PROVIDER_IMPLEMENTATION_NAMES = re.compile(r"(?i)\b(?:native|ncm|ocean)\b")


def strings(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for key, nested in value.items():
            yield from strings(key)
            yield from strings(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from strings(nested)


class SchemaValidator:
    """Minimal draft-2020-12 subset validator: enough for this catalog schema."""

    def __init__(self, schema: dict[str, Any]) -> None:
        self.root = schema

    def resolve(self, ref: str) -> dict[str, Any]:
        if not ref.startswith("#/"):
            raise ValueError(f"unsupported $ref {ref}")
        node: Any = self.root
        for part in ref[2:].split("/"):
            node = node[part]
        return node

    def errors(self, value: Any, schema: dict[str, Any], path: str = "$") -> list[str]:
        if "$ref" in schema:
            return self.errors(value, self.resolve(schema["$ref"]), path)
        problems: list[str] = []
        if "const" in schema and value != schema["const"]:
            problems.append(f"{path}: expected const {schema['const']!r}, got {value!r}")
        if "enum" in schema and value not in schema["enum"]:
            problems.append(f"{path}: {value!r} not in {schema['enum']!r}")
        expected_type = schema.get("type")
        if expected_type is not None:
            types = expected_type if isinstance(expected_type, list) else [expected_type]
            if not any(self._is_type(value, name) for name in types):
                problems.append(f"{path}: expected type {types}, got {type(value).__name__}")
                return problems
        if isinstance(value, str):
            if "minLength" in schema and len(value) < schema["minLength"]:
                problems.append(f"{path}: shorter than {schema['minLength']}")
            if "pattern" in schema and re.search(schema["pattern"], value) is None:
                problems.append(f"{path}: {value!r} does not match {schema['pattern']}")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if "minimum" in schema and value < schema["minimum"]:
                problems.append(f"{path}: {value} below minimum {schema['minimum']}")
        if isinstance(value, list):
            if "minItems" in schema and len(value) < schema["minItems"]:
                problems.append(f"{path}: fewer than {schema['minItems']} items")
            if schema.get("uniqueItems"):
                seen = [json.dumps(item, sort_keys=True) for item in value]
                if len(set(seen)) != len(seen):
                    problems.append(f"{path}: items repeat")
            if "items" in schema:
                for index, item in enumerate(value):
                    problems.extend(self.errors(item, schema["items"], f"{path}[{index}]"))
        if isinstance(value, dict):
            for key in schema.get("required", []):
                if key not in value:
                    problems.append(f"{path}: missing required {key}")
            properties = schema.get("properties", {})
            for key, nested in value.items():
                if key in properties:
                    problems.extend(self.errors(nested, properties[key], f"{path}.{key}"))
                elif schema.get("additionalProperties") is False:
                    problems.append(f"{path}: unexpected property {key}")
        return problems

    @staticmethod
    def _is_type(value: Any, name: str) -> bool:
        if name == "object":
            return isinstance(value, dict)
        if name == "array":
            return isinstance(value, list)
        if name == "string":
            return isinstance(value, str)
        if name == "integer":
            return isinstance(value, int) and not isinstance(value, bool)
        if name == "number":
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        if name == "boolean":
            return isinstance(value, bool)
        if name == "null":
            return value is None
        raise ValueError(f"unsupported type {name}")


class CodingMemoryMetricCatalogTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.raw = CATALOG.read_bytes()
        cls.catalog = json.loads(cls.raw.decode("utf-8"))
        cls.schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
        cls.corpus = json.loads(CORPUS.read_text(encoding="utf-8"))
        cls.terminal = json.loads(TERMINAL_CONTRACT.read_text(encoding="utf-8"))
        cls.metrics = {metric["metric_id"]: metric for metric in cls.catalog["metrics"]}
        cls.rubric_checks = {
            scenario["id"]: {
                check["check_id"] for check in scenario["adjudication_rubric"]["checks"]
            }
            for scenario in cls.corpus["scenarios"]
        }

    def test_catalog_bytes_are_utf8_lf_json(self) -> None:
        self.assertFalse(self.raw.startswith(b"\xef\xbb\xbf"))
        self.assertNotIn(b"\r", self.raw)
        self.assertTrue(self.raw.endswith(b"\n"))

    def test_catalog_validates_against_its_schema(self) -> None:
        problems = SchemaValidator(self.schema).errors(self.catalog, self.schema)
        self.assertEqual(problems, [])

    def test_schema_validator_rejects_a_mutated_catalog(self) -> None:
        validator = SchemaValidator(self.schema)
        mutated = json.loads(self.raw.decode("utf-8"))
        mutated["metrics"][0]["safety_gating"] = "yes"
        self.assertTrue(validator.errors(mutated, self.schema))
        mutated = json.loads(self.raw.decode("utf-8"))
        del mutated["metrics"][0]["denominator"]
        self.assertTrue(validator.errors(mutated, self.schema))
        mutated = json.loads(self.raw.decode("utf-8"))
        mutated["safety_gate"]["aggregate_task_score_can_hide_safety"] = True
        self.assertTrue(validator.errors(mutated, self.schema))

    def test_catalog_is_versioned_and_bound_to_corpus_and_terminal_contract(self) -> None:
        self.assertEqual(self.catalog["schema_version"], 1)
        self.assertEqual(self.catalog["catalog_id"], "tracedecay.coding-memory.metrics.v1")
        self.assertGreaterEqual(self.catalog["catalog_version"], 1)
        self.assertEqual(self.catalog["bead_id"], "tdmem-0904")
        self.assertEqual(
            self.catalog["corpus_binding"],
            {
                "corpus_id": self.corpus["corpus_id"],
                "schema_version": self.corpus["schema_version"],
                "bead_id": self.corpus["bead_id"],
            },
        )
        self.assertEqual(
            self.catalog["terminal_contract_binding"],
            {
                "contract_id": self.terminal["contract_id"],
                "schema_version": self.terminal["schema_version"],
            },
        )
        policy = self.corpus["adjudication_policy"]
        gate = self.catalog["safety_gate"]
        self.assertEqual(gate["indeterminate_policy"], policy["indeterminate_policy"])
        self.assertEqual(gate["missing_evidence_policy"], policy["missing_evidence_policy"])
        self.assertEqual(gate["provider_failure_policy"], policy["provider_failure_policy"])
        self.assertIs(gate["aggregate_task_score_can_hide_safety"], False)

    def test_metric_ids_are_unique_versioned_and_match_the_bead_design(self) -> None:
        ids = [metric["metric_id"] for metric in self.catalog["metrics"]]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(set(ids), set(EXPECTED_METRICS))
        for metric_id, (metric_class, gating) in EXPECTED_METRICS.items():
            metric = self.metrics[metric_id]
            with self.subTest(metric=metric_id):
                self.assertGreaterEqual(metric["version"], 1)
                self.assertEqual(metric["class"], metric_class)
                self.assertIs(metric["safety_gating"], gating)
                self.assertEqual(
                    metric["determinism"],
                    "nondeterministic" if metric_id in NONDETERMINISTIC_METRICS else "deterministic",
                )

    def test_every_metric_has_an_explicit_numerator_and_denominator(self) -> None:
        for metric_id, metric in self.metrics.items():
            with self.subTest(metric=metric_id):
                self.assertTrue(metric["numerator"].strip())
                denominator = metric["denominator"]
                self.assertTrue(denominator["population"].strip())
                self.assertIn(
                    denominator["unresolved_label_policy"],
                    self.catalog["unresolved_label_policies"],
                )
                self.assertIn(
                    denominator["zero_population_policy"],
                    self.catalog["zero_population_policies"],
                )
                self.assertTrue(metric["inputs"])

    def test_safety_metrics_gate_with_ceilings_and_cannot_be_hidden(self) -> None:
        for metric_id, metric in self.metrics.items():
            with self.subTest(metric=metric_id):
                self.assertEqual(metric["class"] == "safety", metric["safety_gating"])
                if metric["safety_gating"]:
                    self.assertIsNotNone(metric["ceiling"])
                    self.assertGreaterEqual(metric["ceiling"], 0)
                    self.assertEqual(metric["direction"], "lower_is_better")
                else:
                    self.assertIsNone(metric["ceiling"])
        self.assertIn("regardless of aggregate_task_score", self.catalog["verdict_rule"])
        self.assertIn("indeterminate", self.catalog["safety_gate"]["rule"])

    def test_unresolved_labels_are_never_coerced(self) -> None:
        vocabulary = self.catalog["label_vocabulary"]
        self.assertEqual(
            set(vocabulary["resolved_labels"]) | set(vocabulary["unresolved_labels"]),
            set(vocabulary["labels"]),
        )
        self.assertFalse(set(vocabulary["resolved_labels"]) & set(vocabulary["unresolved_labels"]))
        self.assertEqual(set(vocabulary["unresolved_labels"]), {"indeterminate", "missing"})
        self.assertIn("tdmem-0802", vocabulary["reconciliation"])
        self.assertEqual(
            self.metrics["harmful_stale_recall_rate"]["denominator"]["unresolved_label_policy"],
            "indeterminate_if_any",
        )
        self.assertEqual(
            self.metrics["useful_recall_precision"]["denominator"]["unresolved_label_policy"],
            "exclude_and_report",
        )
        for metric_id, metric in self.metrics.items():
            if metric_id not in {"harmful_stale_recall_rate", "useful_recall_precision"}:
                self.assertEqual(
                    metric["denominator"]["unresolved_label_policy"], "not_label_based", metric_id
                )
        self.assertEqual(self.catalog["percentile_method"]["name"], "nearest_rank")
        self.assertIn("never fabricated", self.catalog["percentile_method"]["definition"])

    def test_every_corpus_rubric_check_maps_to_at_least_one_metric(self) -> None:
        bound: dict[str, set[str]] = {scenario: set() for scenario in self.rubric_checks}
        for metric_id, metric in self.metrics.items():
            scenarios = Counter(b["scenario_id"] for b in metric["rubric_check_bindings"])
            self.assertTrue(all(count == 1 for count in scenarios.values()), metric_id)
            for binding in metric["rubric_check_bindings"]:
                self.assertIn(binding["scenario_id"], self.rubric_checks, metric_id)
                for check_id in binding["check_ids"]:
                    self.assertIn(
                        check_id, self.rubric_checks[binding["scenario_id"]], (metric_id, check_id)
                    )
                    bound[binding["scenario_id"]].add(check_id)
            if metric["applicable_scenarios"] is not None:
                for scenario_id in metric["applicable_scenarios"]:
                    self.assertIn(scenario_id, self.rubric_checks, metric_id)
        for scenario_id, checks in self.rubric_checks.items():
            with self.subTest(scenario=scenario_id):
                self.assertEqual(bound[scenario_id], checks)

    def test_safety_critical_checks_cover_every_scenario_with_real_check_ids(self) -> None:
        listed = {entry["scenario_id"] for entry in self.catalog["safety_critical_checks"]}
        self.assertEqual(listed, set(self.rubric_checks))
        self.assertEqual(len(listed), len(self.catalog["safety_critical_checks"]))
        for entry in self.catalog["safety_critical_checks"]:
            with self.subTest(scenario=entry["scenario_id"]):
                checks = set(entry["check_ids"])
                self.assertTrue(checks)
                self.assertEqual(len(checks), len(entry["check_ids"]))
                self.assertTrue(checks <= self.rubric_checks[entry["scenario_id"]])

    def test_catalog_has_no_concrete_provider_names(self) -> None:
        for value in strings(self.catalog):
            self.assertIsNone(PROVIDER_IMPLEMENTATION_NAMES.search(value), value)

    def test_rust_crate_embeds_this_catalog_and_ci_runs_this_gate(self) -> None:
        source = RUST_CATALOG_INCLUDE.read_text(encoding="utf-8")
        self.assertIn(
            'include_str!("../../../product/evaluation/coding-memory-metrics.v1.json")', source
        )
        for metric_id in EXPECTED_METRICS:
            self.assertIn(f'"{metric_id}"', source, metric_id)
        workflow = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("tests/product_tdmem_0904_metric_catalog_test.py", workflow)


if __name__ == "__main__":
    unittest.main()
