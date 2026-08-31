#!/usr/bin/env python3
"""Contract tests for the product memory dependency workflow triggers."""

from __future__ import annotations

import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
WORKFLOW = REPO / ".github/workflows/product-memory-dependencies.yml"


def indented_block(lines: list[str], marker: str, indent: int) -> list[str]:
    """Return the lines nested beneath an exact YAML key marker."""
    start = lines.index(f"{' ' * indent}{marker}") + 1
    block: list[str] = []
    for line in lines[start:]:
        if line.strip() and len(line) - len(line.lstrip()) <= indent:
            break
        block.append(line)
    return block


class ProductMemoryDependenciesWorkflowTest(unittest.TestCase):
    def test_runs_for_integration_pushes_feature_pushes_and_pull_requests(self) -> None:
        lines = WORKFLOW.read_text(encoding="utf-8").splitlines()
        triggers = indented_block(lines, "on:", 0)

        self.assertIn("  pull_request:", triggers)
        push = indented_block(triggers, "push:", 2)
        branches = indented_block(push, "branches:", 4)
        branch_names = {line.strip().removeprefix("- ") for line in branches}
        self.assertIn("master", branch_names)
        self.assertIn("feat/pluggable-memory-providers-v2", branch_names)


if __name__ == "__main__":
    unittest.main()
