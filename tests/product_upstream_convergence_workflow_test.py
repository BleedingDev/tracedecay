#!/usr/bin/env python3
"""Structural contract tests for fail-closed upstream convergence CI."""

from __future__ import annotations

import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
WORKFLOW = REPO / ".github/workflows/product-upstream.yml"
REQUIRED = (
    "upstream_required",
    "product_contracts",
    "native_parity",
    "provider_conformance",
    "scope_crash_security",
    "generated_drift",
)


def nested_block(text: str, marker: str, indent: int) -> str:
    lines = text.splitlines()
    target = f"{' ' * indent}{marker}"
    start = lines.index(target) + 1
    block: list[str] = []
    for line in lines[start:]:
        if line.strip() and len(line) - len(line.lstrip()) <= indent:
            break
        block.append(line)
    return "\n".join(block)


class ProductUpstreamConvergenceWorkflowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW.read_text(encoding="utf-8")

    def job(self, job_id: str) -> str:
        return nested_block(self.workflow, f"{job_id}:", 2)

    def test_runs_without_path_filter_bypasses(self) -> None:
        triggers = nested_block(self.workflow, "on:", 0)
        self.assertIn("  pull_request:", triggers)
        self.assertIn("  workflow_dispatch:", triggers)
        self.assertIn("feat/pluggable-memory-providers-v2", triggers)
        self.assertIn('"sync/upstream/**"', triggers)
        self.assertNotIn("paths:", triggers)
        self.assertNotIn("paths-ignore:", triggers)

    def test_required_gates_form_the_exact_policy_order(self) -> None:
        for index, gate in enumerate(REQUIRED):
            block = self.job(gate)
            self.assertIn(f"[tdmem-1206][required][{gate}]", block)
            if index == 0:
                self.assertNotIn("needs:", block)
            else:
                self.assertIn(f"needs: {REQUIRED[index - 1]}", block)
            self.assertIn(f"bead=tdmem-1206 area={gate} required=true", block)
            self.assertNotIn("continue-on-error:", block)
            self.assertNotIn("|| true", block)

    def test_aggregate_rejects_failed_cancelled_and_skipped_required_jobs(self) -> None:
        aggregate = self.job("convergence_result")
        self.assertIn("if: always()", aggregate)
        for gate in REQUIRED:
            self.assertIn(f"- {gate}", aggregate)
            self.assertIn(f"needs.{gate}.result", aggregate)
        self.assertIn('if [[ "$result" != "success" ]]', aggregate)
        self.assertIn("exit 1", aggregate)
        self.assertNotIn("informational_macos_floor", aggregate)

    def test_informational_lane_is_explicit_and_nonblocking(self) -> None:
        informational = self.job("informational_macos_floor")
        self.assertIn("[tdmem-1206][informational][macos_floor]", informational)
        self.assertIn("continue-on-error: true", informational)
        self.assertIn("bead=tdmem-1206 area=macos_floor required=false", informational)

    def test_checkout_and_permissions_are_read_only_and_exact(self) -> None:
        permissions = nested_block(self.workflow, "permissions:", 0)
        self.assertEqual(permissions.strip(), "contents: read")
        self.assertNotIn("contents: write", self.workflow)
        self.assertEqual(self.workflow.count("persist-credentials: false"), 7)
        self.assertEqual(self.workflow.count("fetch-depth: 0"), 7)
        self.assertEqual(
            self.workflow.count("github.event.pull_request.head.sha || github.sha"),
            7,
        )
        self.assertNotIn("actions/checkout@v", self.workflow)
        self.assertNotIn("dtolnay/rust-toolchain@stable", self.workflow)
        self.assertNotIn("taiki-e/install-action@nextest", self.workflow)

    def test_upstream_gate_runs_real_parity_suites(self) -> None:
        upstream = self.job("upstream_required")
        self.assertIn("cargo nextest run --workspace --profile ci --locked", upstream)
        self.assertIn("--features tracedecay/test-helpers", upstream)
        self.assertIn("cargo check --workspace --all-targets --features test-transport --locked", upstream)
        self.assertIn("cargo check --workspace --all-targets --features hotpath,hotpath-mcp --locked", upstream)

    def test_validation_workflow_never_publishes_or_finalizes_a_train(self) -> None:
        self.assertNotIn("run-upstream-sync-train.py", self.workflow)
        self.assertNotIn("git push", self.workflow)
        self.assertNotIn("git update-ref", self.workflow)
        self.assertIn("cancel-in-progress: false", self.workflow)


if __name__ == "__main__":
    unittest.main()
