#!/usr/bin/env python3
"""Focused tests for the vendor-floor sync preflight."""

from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parents[1]
CHECKER = REPO / "scripts/product/check-upstream-vendor-floor.py"
CHECKED_POLICY = REPO / "product/upstream/sync-policy.json"
CHECKED_METADATA = REPO / "product/upstream/tracedecay-v2-pr707.json"
EXPECTED_FLOOR = "08fbe33a7c7f403191fd5d6e356c7b6681b96403"


class VendorFloorPreflightTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo = self.root / "repo"
        self.config = self.root / "config"
        self.repo.mkdir()
        self.config.mkdir()

        self.git("init", "-q", "-b", "feat/pluggable-memory-providers-v2")
        self.git("config", "user.name", "Vendor Floor Test")
        self.git("config", "user.email", "vendor-floor@example.invalid")
        self.git(
            "remote",
            "add",
            "origin",
            "https://github.com/BleedingDev/tracedecay.git",
        )
        self.git(
            "remote",
            "add",
            "upstream",
            "git@github.com:ScriptedAlchemy/tracedecay.git",
        )

        (self.repo / "history.txt").write_text("base\n", encoding="utf-8")
        self.git("add", "history.txt")
        self.git("commit", "-q", "-m", "base")
        self.base = self.git("rev-parse", "HEAD").stdout.strip()
        (self.repo / "history.txt").write_text("base\nfloor\n", encoding="utf-8")
        self.git("commit", "-q", "-am", "floor")
        self.floor = self.git("rev-parse", "HEAD").stdout.strip()
        (self.repo / "history.txt").write_text(
            "base\nfloor\nproduct\n", encoding="utf-8"
        )
        self.git("commit", "-q", "-am", "product")
        self.product_head = self.git("rev-parse", "HEAD").stdout.strip()
        self.git("update-ref", "refs/remotes/upstream/master", self.product_head)
        self.git(
            "update-ref", "refs/remotes/upstream/pr/707-current", self.product_head
        )
        self.git("switch", "-q", "-c", "sync/upstream/test")

        self.metadata_path = self.config / "provenance.json"
        self.policy_path = self.config / "policy.json"
        self.metadata = {
            "schema_version": 1,
            "product": {
                "repository": "BleedingDev/tracedecay",
                "branch": "feat/pluggable-memory-providers-v2",
            },
            "source": {
                "repository": "ScriptedAlchemy/tracedecay",
                "pull_request": 707,
            },
            "pinned_floor": {
                "sha": self.floor,
                "must_be_ancestor_of_product_head": True,
            },
        }
        self.policy: dict[str, Any] = {
            "schema_version": 1,
            "authority": "product-owned",
            "ownership": {
                "sync_owner": "BleedingDev",
                "review_owner": "ScriptedAlchemy",
                "product_patch_owners": ["BleedingDev"],
            },
            "remotes": {
                "product": {
                    "name": "origin",
                    "repository": "BleedingDev/tracedecay",
                },
                "upstream": {
                    "name": "upstream",
                    "repository": "ScriptedAlchemy/tracedecay",
                },
            },
            "refs": {
                "product_branch": "refs/heads/feat/pluggable-memory-providers-v2",
                "sync_branch_prefix": "refs/heads/sync/upstream/",
                "upstream_discovery": [
                    "refs/remotes/upstream/master",
                    "refs/remotes/upstream/pr/707-current",
                ],
            },
            "floor": {
                "metadata": str(self.metadata_path),
                "pull_request": 707,
                "sha": self.floor,
            },
            "preflight": {
                "requires_clean_worktree": True,
                "requires_floor_ancestor": True,
                "forbidden_direct_targets": [
                    "refs/heads/main",
                    "refs/heads/master",
                    "refs/remotes/origin/main",
                    "refs/remotes/origin/master",
                ],
            },
        }

    def git(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            ["git", "-C", str(self.repo), *arguments],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def run_checker(self, *, source_ref: str | None = None) -> subprocess.CompletedProcess[str]:
        self.metadata_path.write_text(
            json.dumps(self.metadata, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        self.policy_path.write_text(
            json.dumps(self.policy, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        command = [
            "python3",
            str(CHECKER),
            "--repo",
            str(self.repo),
            "--policy",
            str(self.policy_path),
        ]
        if source_ref is not None:
            command.extend(["--source-ref", source_ref])
        return subprocess.run(command, check=False, capture_output=True, text=True)

    def assert_rejected(self, marker: str, **kwargs: str) -> None:
        result = self.run_checker(**kwargs)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        evidence = json.loads(result.stdout)
        self.assertFalse(evidence["ok"])
        self.assertIn(marker, "\n".join(evidence["errors"]))

    def test_checked_in_contract_pins_pr707_creation_head(self) -> None:
        policy = json.loads(CHECKED_POLICY.read_text(encoding="utf-8"))
        metadata = json.loads(CHECKED_METADATA.read_text(encoding="utf-8"))
        self.assertEqual(policy["floor"]["pull_request"], 707)
        self.assertEqual(policy["floor"]["sha"], EXPECTED_FLOOR)
        self.assertEqual(metadata["pinned_floor"]["sha"], EXPECTED_FLOOR)
        self.assertEqual(
            policy["ownership"],
            {
                "sync_owner": "BleedingDev",
                "review_owner": "ScriptedAlchemy",
                "product_patch_owners": ["BleedingDev"],
            },
        )
        self.assertEqual(
            policy["remotes"],
            {
                "product": {
                    "name": "origin",
                    "repository": "BleedingDev/tracedecay",
                },
                "upstream": {
                    "name": "upstream",
                    "repository": "ScriptedAlchemy/tracedecay",
                },
            },
        )
        self.assertEqual(
            policy["refs"],
            {
                "product_branch": "refs/heads/feat/pluggable-memory-providers-v2",
                "sync_branch_prefix": "refs/heads/sync/upstream/",
                "upstream_discovery": [
                    "refs/remotes/upstream/master",
                    "refs/remotes/upstream/pr/707-current",
                ],
            },
        )
        self.assertEqual(
            set(policy["preflight"]["forbidden_direct_targets"]),
            {
                "refs/heads/main",
                "refs/heads/master",
                "refs/remotes/origin/main",
                "refs/remotes/origin/master",
            },
        )

    def test_clean_isolated_sync_branch_passes(self) -> None:
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        evidence = json.loads(result.stdout)
        self.assertTrue(evidence["ok"])
        self.assertEqual(evidence["tree_state"], "clean")
        self.assertEqual(evidence["floor_sha"], self.floor)
        self.assertEqual(evidence["product_head_sha"], self.product_head)
        self.assertEqual(evidence["sync_head_sha"], self.product_head)
        self.assertEqual(evidence["source_ref"], "refs/remotes/upstream/master")
        self.assertEqual(evidence["source_relationship"], "descendant_of_floor")
        self.assertEqual(evidence["source_merge_base"], self.floor)
        self.assertEqual(evidence["product_repository"], "BleedingDev/tracedecay")
        self.assertEqual(
            evidence["upstream_repository"], "ScriptedAlchemy/tracedecay"
        )

    def test_untracked_tree_change_is_rejected(self) -> None:
        (self.repo / "untracked.txt").write_text("dirty\n", encoding="utf-8")
        self.assert_rejected("working tree is not clean")

    def test_unstaged_tree_change_is_rejected(self) -> None:
        (self.repo / "history.txt").write_text(
            "base\nfloor\nproduct\nunstaged\n", encoding="utf-8"
        )
        self.assert_rejected("working tree is not clean")

    def test_staged_tree_change_is_rejected(self) -> None:
        (self.repo / "staged.txt").write_text("staged\n", encoding="utf-8")
        self.git("add", "staged.txt")
        self.assert_rejected("working tree is not clean")

    def test_detached_head_is_rejected(self) -> None:
        self.git("switch", "-q", "--detach")
        self.assert_rejected("requires an attached isolated sync branch")

    def test_direct_product_branch_is_rejected(self) -> None:
        self.git("switch", "-q", "feat/pluggable-memory-providers-v2")
        self.assert_rejected("must not operate directly on the product branch")

    def test_direct_master_target_is_rejected(self) -> None:
        self.git("switch", "-q", "-c", "master")
        self.assert_rejected("direct sync target 'refs/heads/master' is forbidden")

    def test_direct_main_target_is_rejected(self) -> None:
        self.git("switch", "-q", "-c", "main")
        self.assert_rejected("direct sync target 'refs/heads/main' is forbidden")

    def test_sync_branch_must_start_at_product_head(self) -> None:
        (self.repo / "sync.txt").write_text("premature change\n", encoding="utf-8")
        self.git("add", "sync.txt")
        self.git("commit", "-q", "-m", "premature sync")
        self.assert_rejected("must start at the current product branch head")

    def test_non_ancestor_floor_is_rejected(self) -> None:
        tree = self.git("write-tree").stdout.strip()
        orphan = subprocess.run(
            ["git", "-C", str(self.repo), "commit-tree", tree],
            input="orphan floor\n",
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        self.metadata["pinned_floor"]["sha"] = orphan
        self.policy["floor"]["sha"] = orphan
        self.assert_rejected("floor ancestry failed")

    def test_unapproved_discovery_ref_is_rejected(self) -> None:
        self.assert_rejected(
            "is not an approved upstream discovery ref",
            source_ref="refs/remotes/upstream/not-approved",
        )

    def test_unrelated_source_candidate_is_rejected(self) -> None:
        tree = self.git("write-tree").stdout.strip()
        orphan = subprocess.run(
            ["git", "-C", str(self.repo), "commit-tree", tree],
            input="unrelated source\n",
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        self.git("update-ref", "refs/remotes/upstream/master", orphan)
        self.assert_rejected("has no common ancestry with pinned floor")

    def test_source_behind_floor_is_reported(self) -> None:
        self.git("update-ref", "refs/remotes/upstream/master", self.base)
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        evidence = json.loads(result.stdout)
        self.assertEqual(evidence["source_relationship"], "behind_floor")
        self.assertEqual(evidence["source_merge_base"], self.base)

    def test_source_divergence_is_reported_with_merge_base(self) -> None:
        tree = self.git("write-tree").stdout.strip()
        candidate = subprocess.run(
            [
                "git",
                "-C",
                str(self.repo),
                "commit-tree",
                tree,
                "-p",
                self.base,
            ],
            input="diverged source\n",
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        self.git("update-ref", "refs/remotes/upstream/master", candidate)
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        evidence = json.loads(result.stdout)
        self.assertEqual(evidence["source_relationship"], "diverged_from_floor")
        self.assertEqual(evidence["source_merge_base"], self.base)

    def test_discovery_ref_cannot_contain_a_revision_expression(self) -> None:
        expression = "refs/remotes/upstream/master~1"
        self.policy["refs"]["upstream_discovery"][0] = expression
        self.assert_rejected("must be a valid full Git ref", source_ref=expression)

    def test_remote_repository_mismatch_is_rejected(self) -> None:
        self.git("remote", "set-url", "upstream", "https://github.com/other/fork.git")
        self.assert_rejected("points to 'other/fork'")

    def test_insecure_remote_transport_is_rejected(self) -> None:
        self.git(
            "remote", "set-url", "upstream", "http://github.com/ScriptedAlchemy/tracedecay.git"
        )
        self.assert_rejected("must use HTTPS or SSH")

    def test_remote_credentials_are_rejected_without_echoing_them(self) -> None:
        secret = "never-print-this-token"
        self.git(
            "remote",
            "set-url",
            "upstream",
            f"https://{secret}@github.com/ScriptedAlchemy/tracedecay.git",
        )
        result = self.run_checker()
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn(secret, result.stdout + result.stderr)
        self.assertIn("must not contain embedded credentials", result.stdout)


if __name__ == "__main__":
    unittest.main()
