#!/usr/bin/env python3
"""Behavioral tests for the product-owned isolated upstream sync train."""

from __future__ import annotations

import json
import shlex
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parents[1]
RUNNER = REPO / "scripts/product/run-upstream-sync-train.py"
PRODUCT_REF = "refs/heads/product"
SOURCE_REF = "refs/remotes/upstream/master"
FLOOR_PATH = "product/upstream/tracedecay-v2-pr707.json"
POLICY_PATH = "product/upstream/sync-policy.json"
RECEIPT_PREFIX = "product/upstream/sync-train-receipts/sync-train-"
DERIVED_PIN_PATH = "product/upstream/pr707-floor.json"
LITERAL_PIN_PATH = "docs/floor-literal.md"
MAP_PIN_PATH = "product/upstream/map.json"
ARCHIVAL_PATH = "product/baseline/measured.json"
HISTORICAL_PREFIX = "history/"
HISTORICAL_PATH = "history/receipt.json"
WORKFLOW_PATH = ".github/workflows/product-upstream.yml"
LANE_EXTRA_ARGV = ["python3", "-c", "raise SystemExit(0)"]
PIN_PATHS = (POLICY_PATH, DERIVED_PIN_PATH, MAP_PIN_PATH, LITERAL_PIN_PATH)
GATE_ORDER = (
    "upstream_required",
    "product_contracts",
    "native_parity",
    "provider_conformance",
    "scope_crash_security",
    "generated_drift",
)


class UpstreamSyncTrainTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo = self.root / "repo"
        self.train = self.root / "train"
        self.repo.mkdir()
        self.git("init", "-q", "-b", "product")
        self.git("config", "user.name", "Sync Train Test")
        self.git("config", "user.email", "sync-train@example.invalid")

        (self.repo / "code.txt").write_text("base\n", encoding="utf-8")
        (self.repo / "history.txt").write_text("base\n", encoding="utf-8")
        (self.repo / FLOOR_PATH).parent.mkdir(parents=True)
        (self.repo / FLOOR_PATH).write_text(
            json.dumps({"pinned_floor": {"sha": "0" * 40}}, indent=2) + "\n",
            encoding="utf-8",
        )
        self.git("add", ".")
        self.git("commit", "-q", "-m", "base")
        self.base_sha = self.git("rev-parse", "HEAD").stdout.strip()

        (self.repo / FLOOR_PATH).write_text(
            json.dumps({"pinned_floor": {"sha": self.base_sha}}, indent=2) + "\n",
            encoding="utf-8",
        )
        metadata_blob = self.git("hash-object", FLOOR_PATH).stdout.strip()
        (self.repo / DERIVED_PIN_PATH).write_text(
            json.dumps(
                {
                    "canonical_metadata": FLOOR_PATH,
                    "canonical_metadata_blob_sha": metadata_blob,
                    "pinned_floor_sha": self.base_sha,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        (self.repo / LITERAL_PIN_PATH).parent.mkdir(parents=True)
        (self.repo / LITERAL_PIN_PATH).write_text(
            f"Notes.\nThe accepted floor is `{self.base_sha}`.\nMore notes.\n", encoding="utf-8"
        )
        # One declared gate command whose behavior is steered from outside the
        # repository so the candidate tree stays exact: an exit-code file and a
        # mutate marker under the temporary root (never inside the repo).
        self.gate_exit = self.root / "gate-exit"
        self.gate_mutate = self.root / "gate-mutate"
        self.gate_argv = [
            "python3",
            "-c",
            "import pathlib, sys; "
            f'mutate = pathlib.Path("{self.gate_mutate}"); '
            'mutate.exists() and pathlib.Path("generated.txt").write_text("drift"); '
            f'exit_file = pathlib.Path("{self.gate_exit}"); '
            "sys.exit(int(exit_file.read_text()) if exit_file.exists() else 0)",
        ]
        self.gate_command = shlex.join(self.gate_argv)
        self.lane_extra = shlex.join(LANE_EXTRA_ARGV)
        self.lanes = {
            gate_id: {
                "workflow": WORKFLOW_PATH,
                "job": gate_id,
                "commands": (
                    [self.lane_extra, self.gate_command]
                    if gate_id == "upstream_required"
                    else [self.gate_command]
                ),
            }
            for gate_id in GATE_ORDER
        }
        self.write_workflow(self.lanes)
        (self.repo / MAP_PIN_PATH).write_text(
            json.dumps(
                {
                    "upstream_floor_sha": self.base_sha,
                    "entries": [
                        {
                            "id": "alpha",
                            "tests": [self.gate_command],
                            "last_verified_upstream_sha": self.base_sha,
                        },
                        {
                            "id": "beta",
                            "verification": [self.lane_extra],
                            "last_verified_upstream_sha": self.base_sha,
                        },
                    ],
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        (self.repo / ARCHIVAL_PATH).parent.mkdir(parents=True)
        (self.repo / ARCHIVAL_PATH).write_text(
            json.dumps({"measured_floor_sha": self.base_sha}, indent=2) + "\n",
            encoding="utf-8",
        )
        (self.repo / HISTORICAL_PATH).parent.mkdir(parents=True)
        (self.repo / HISTORICAL_PATH).write_text(
            json.dumps({"produced_under_floor": self.base_sha}, indent=2) + "\n",
            encoding="utf-8",
        )
        (self.repo / POLICY_PATH).write_text(
            json.dumps(
                {
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "schema_version": 1,
                    "policy_revision": "sync-train.v1",
                    "authority": "product-owned",
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
                        "product_branch": PRODUCT_REF,
                        "sync_branch_prefix": "refs/heads/sync/upstream/",
                        "upstream_discovery": [SOURCE_REF],
                    },
                    "floor": {
                        "metadata": FLOOR_PATH,
                        "pull_request": 707,
                        "sha": self.base_sha,
                        "advancement_authority": "tdmem-1208",
                        "immutable_until_approved_train": True,
                        "pins": [
                            {
                                "path": POLICY_PATH,
                                "kind": "json_pointer",
                                "occurrences": 1,
                                "pointers": ["/floor/sha"],
                            },
                            {
                                "path": DERIVED_PIN_PATH,
                                "kind": "derived_metadata_receipt",
                                "occurrences": 1,
                                "pointers": ["/pinned_floor_sha"],
                                "metadata_pointer": "/canonical_metadata",
                                "blob_pointer": "/canonical_metadata_blob_sha",
                            },
                            {
                                "path": MAP_PIN_PATH,
                                "kind": "json_pointer",
                                "occurrences": 1,
                                "pointers": ["/upstream_floor_sha"],
                                "each_pointers": ["/entries/*/last_verified_upstream_sha"],
                                "each_reason": "per-entry stamps advance with the floor and are proven by the tree-bound gates",
                            },
                            {
                                "path": LITERAL_PIN_PATH,
                                "kind": "anchored_line",
                                "occurrences": 1,
                                "line": "The accepted floor is `{floor}`.",
                            },
                        ],
                        "archival_provenance": [
                            {
                                "path": ARCHIVAL_PATH,
                                "reason": "measured against the floor it was produced on",
                            }
                        ],
                        "historical_record_prefixes": [HISTORICAL_PREFIX],
                    },
                    "preflight": {
                        "requires_clean_worktree": True,
                        "requires_floor_ancestor": True,
                        "forbidden_direct_targets": [
                            "refs/heads/main",
                            "refs/heads/master",
                        ],
                    },
                    "workflow": {
                        "name": "run-upstream-sync-train",
                        "policy_path": POLICY_PATH,
                        "receipt_schema_path": "product/upstream/sync-train-receipt.schema.json",
                        "receipt_path_template": "product/upstream/sync-train-receipts/{train_id}.json",
                        "sync_branch_template": "refs/heads/sync/upstream/{candidate_short_sha}",
                        "allowed_strategies": ["merge", "rebase"],
                        "candidate_must_be_immutable_sha": True,
                        "moving_refs_are_discovery_only": True,
                        "first_floor_advancement_bead": "tdmem-1208",
                    },
                    "conflicts": {
                        "path_format": "repo-relative-posix",
                        "required_fields": ["path", "source", "owner", "resolution", "rationale"],
                        "receipt_required_even_when_empty": True,
                        "unresolved_conflict_is_terminal_failure": True,
                    },
                    "gates": {
                        "upstream_required_first": True,
                        "fail_closed": True,
                        "required_order": list(GATE_ORDER),
                        "required_gate_status": "passed",
                        "lanes": self.lanes,
                    },
                    "finalization": {
                        "method": "compare_and_swap",
                        "sync_train_publication_target": "isolated_sync_ref",
                        "released_branch_update_in_this_workflow": "unchanged",
                        "cas": {
                            "required": True,
                            "compare_refs": [PRODUCT_REF],
                            "compare_values": [
                                "product.starting_head_sha",
                                "product.starting_floor_sha",
                            ],
                        },
                        "released_refs": [PRODUCT_REF],
                        "force_update_allowed": False,
                        "non_fast_forward_update_allowed": False,
                        "same_commit_bundle": {
                            "required": True,
                            "members": ["code", "floor_metadata", "convergence_receipt"],
                            "metadata_path": FLOOR_PATH,
                            "receipt_schema_path": "product/upstream/sync-train-receipt.schema.json",
                        },
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        self.git("add", ".")
        self.git("commit", "-q", "-m", "pin floor")
        self.floor_sha = self.git("rev-parse", "HEAD").stdout.strip()

        (self.repo / "code.txt").write_text("base\nproduct change\n", encoding="utf-8")
        self.git("commit", "-q", "-am", "product change")
        self.product_sha = self.git("rev-parse", "HEAD").stdout.strip()

        self.git("switch", "-q", "-c", "upstream-source", self.floor_sha)
        (self.repo / "code.txt").write_text("base\nupstream change\n", encoding="utf-8")
        self.git("commit", "-q", "-am", "upstream change")
        self.source_sha = self.git("rev-parse", "HEAD").stdout.strip()
        self.git("update-ref", SOURCE_REF, self.source_sha)
        self.git("switch", "-q", "product")

    def write_workflow(self, lanes: dict[str, dict[str, Any]]) -> None:
        """Write the fixture convergence workflow: one job per gate whose run
        block lists exactly the lane's commands."""

        lines = ["name: Product upstream convergence (fixture)", "jobs:"]
        for gate_id, lane in lanes.items():
            lines.extend(
                [
                    f"  {lane['job']}:",
                    "    runs-on: ubuntu-latest",
                    "    steps:",
                    f"      - name: Verify {gate_id}",
                    "        shell: bash",
                    "        run: |",
                    "          set -euo pipefail",
                    *[f"          {command}" for command in lane["commands"]],
                ]
            )
        path = self.repo / WORKFLOW_PATH
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def git_at(
        self,
        repo: Path,
        *arguments: str,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            ["git", "-C", str(repo), *arguments],
            check=False,
            capture_output=True,
            text=True,
        )
        if check:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def git(self, *arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return self.git_at(self.repo, *arguments, check=check)

    def run_train_at(
        self,
        repo: Path,
        train_dir: Path,
        command: str,
        *arguments: str,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                "python3",
                str(RUNNER),
                command,
                "--repo",
                str(repo),
                "--train-dir",
                str(train_dir),
                *arguments,
            ],
            check=False,
            capture_output=True,
            text=True,
        )

    def run_train(self, command: str, *arguments: str) -> subprocess.CompletedProcess[str]:
        return self.run_train_at(self.repo, self.train, command, *arguments)

    def result_json(self, result: subprocess.CompletedProcess[str]) -> dict[str, Any]:
        try:
            value = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            self.fail(f"runner did not produce JSON: {result.stdout!r}{result.stderr!r}: {error}")
        self.assertIsInstance(value, dict)
        return value

    def prepare(self) -> dict[str, Any]:
        result = self.run_train(
            "prepare",
            "--product-branch",
            PRODUCT_REF,
            "--source-ref",
            SOURCE_REF,
            "--floor-metadata",
            FLOOR_PATH,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return self.result_json(result)

    def record_product_conflict(self) -> dict[str, Any]:
        result = self.run_train(
            "record-conflict",
            "--path",
            "code.txt",
            "--owner",
            "product",
            "--resolution",
            "retain_product_mount",
            "--rationale",
            "the product-owned seam must remain removable and explicit",
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return self.result_json(result)

    def resolve_product_conflict(self) -> None:
        self.record_product_conflict()
        (self.repo / "code.txt").write_text("base\nproduct change\nresolved\n", encoding="utf-8")
        self.git("add", "code.txt")

    def advance_floor(self) -> dict[str, Any]:
        result = self.run_train("advance-floor")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        evidence = self.result_json(result)
        self.assertEqual(evidence["status"], "advanced")
        self.assertRegex(evidence["candidate_tree_sha"], r"^[0-9a-f]{40}$")
        return evidence

    def record_gate(self, gate_id: str, argv: list[str]) -> subprocess.CompletedProcess[str]:
        return self.run_train("record-gate", "--id", gate_id, "--command-json", json.dumps(argv))

    def record_gates(self) -> None:
        for gate_id in GATE_ORDER:
            if gate_id == "upstream_required":
                extra = self.record_gate(gate_id, LANE_EXTRA_ARGV)
                self.assertEqual(extra.returncode, 0, extra.stdout + extra.stderr)
                self.assertEqual(self.result_json(extra)["gate"]["status"], "in_progress")
            result = self.record_gate(gate_id, self.gate_argv)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(self.result_json(result)["gate"]["status"], "passed")

    def publish_train(self) -> dict[str, Any]:
        """Prepare, resolve, advance, gate, and publish one train; return publish evidence."""

        self.prepare()
        self.resolve_product_conflict()
        advanced = self.advance_floor()
        self.record_gates()
        published = self.run_train("publish")
        self.assertEqual(published.returncode, 0, published.stdout + published.stderr)
        evidence = self.result_json(published)
        self.assertEqual(evidence["candidate_tree_sha"], advanced["candidate_tree_sha"])
        return evidence

    def assert_product_head_carries_floor(self, floor: str, *, absent: str) -> None:
        """Every declared pin at the product head carries ``floor`` and not ``absent``."""

        for path in (FLOOR_PATH, *PIN_PATHS):
            blob = self.git("show", f"{PRODUCT_REF}:{path}").stdout
            self.assertIn(floor, blob, path)
            self.assertNotIn(absent, blob, path)

    def test_publish_advances_every_declared_pin_in_the_train_commit(self) -> None:
        archival_before = self.git("rev-parse", f"{PRODUCT_REF}:{ARCHIVAL_PATH}").stdout.strip()
        evidence = self.publish_train()
        final_sha = evidence["sync_head_sha"]
        self.assertEqual(evidence["floor_after_sha"], self.source_sha)
        candidate_tree = evidence["candidate_tree_sha"]
        # The published tree differs from the gated candidate tree only by the receipt.
        receipt_path = RECEIPT_PREFIX + self.source_sha[:12] + ".json"
        self.assertEqual(
            self.git("diff-tree", "-r", "--name-status", candidate_tree, f"{final_sha}^{{tree}}").stdout,
            f"A\t{receipt_path}\n",
        )
        # Fixed pointers and every wildcard per-entry stamp moved; nothing else in the document changed.
        pinned_map = json.loads(self.git("show", f"{final_sha}:{MAP_PIN_PATH}").stdout)
        self.assertEqual(pinned_map["upstream_floor_sha"], self.source_sha)
        self.assertEqual(
            [(entry["id"], entry["last_verified_upstream_sha"]) for entry in pinned_map["entries"]],
            [("alpha", self.source_sha), ("beta", self.source_sha)],
        )
        self.assertEqual(
            self.git("diff", "--numstat", f"{PRODUCT_REF}:{MAP_PIN_PATH}", f"{final_sha}:{MAP_PIN_PATH}").stdout.split("\t")[:2],
            ["3", "3"],
        )
        literal = self.git("show", f"{final_sha}:{LITERAL_PIN_PATH}").stdout
        self.assertEqual(literal, f"Notes.\nThe accepted floor is `{self.source_sha}`.\nMore notes.\n")
        self.assertEqual(
            self.git("show", f"{final_sha}:{HISTORICAL_PATH}").stdout,
            self.git("show", f"{PRODUCT_REF}:{HISTORICAL_PATH}").stdout,
        )

        policy = json.loads(self.git("show", f"{final_sha}:{POLICY_PATH}").stdout)
        self.assertEqual(policy["floor"]["sha"], self.source_sha)
        metadata_blob = self.git("rev-parse", f"{final_sha}:{FLOOR_PATH}").stdout.strip()
        derived = json.loads(self.git("show", f"{final_sha}:{DERIVED_PIN_PATH}").stdout)
        self.assertEqual(derived["pinned_floor_sha"], self.source_sha)
        self.assertEqual(derived["canonical_metadata_blob_sha"], metadata_blob)
        self.assertEqual(
            self.git("rev-parse", f"{final_sha}:{ARCHIVAL_PATH}").stdout.strip(),
            archival_before,
        )
        metadata = json.loads(self.git("show", f"{final_sha}:{FLOOR_PATH}").stdout)
        self.assertEqual(metadata["pinned_floor"]["sha"], self.source_sha)
        self.assertIn(self.source_sha[:12], metadata["pinned_floor"]["selection_basis"])
        self.assertIn(SOURCE_REF, metadata["pinned_floor"]["selection_basis"])
        self.assertTrue(metadata["pinned_floor"]["selected_at"])

        receipt = json.loads(self.git("show", f"{final_sha}:{receipt_path}").stdout)
        advancement = receipt["floor_advancement"]
        self.assertEqual(advancement["outcome"], "advanced")
        self.assertEqual(advancement["previous_floor_sha"], self.base_sha)
        self.assertEqual(advancement["candidate_floor_sha"], self.source_sha)
        self.assertEqual(advancement["gated_tree_sha"], candidate_tree)
        self.assertEqual([gate["tree_sha"] for gate in receipt["gates"]], [candidate_tree] * 6)
        # Every stamped map target's verification command passed in a declared lane.
        coverage = advancement["verification_coverage"]
        self.assertEqual(coverage["stamped_targets"], 2)
        self.assertEqual(coverage["required_commands"], 2)
        self.assertEqual(coverage["covered_commands"], 2)
        self.assertEqual(coverage["uncovered_commands"], [])
        self.assertEqual(coverage["lane_commands"]["upstream_required"], {"declared": 2, "passed": 2})
        self.assertEqual(coverage["lane_commands"]["generated_drift"], {"declared": 1, "passed": 1})
        for gate in receipt["gates"]:
            self.assertEqual(gate["command"], f"{WORKFLOW_PATH}#{gate['id']}")
            self.assertEqual(gate["coverage"]["missing"], [])
            self.assertEqual(
                [(record["command"], record["source"], record["status"]) for record in gate["commands"]],
                [(command, "executed", "passed") for command in self.lanes[gate["id"]]["commands"]],
            )
        self.assertEqual(
            [
                (pin["path"], pin["kind"], pin["occurrences"], pin["each_occurrences"])
                for pin in advancement["pins"]
            ],
            [
                (POLICY_PATH, "json_pointer", 1, 0),
                (DERIVED_PIN_PATH, "derived_metadata_receipt", 1, 0),
                (MAP_PIN_PATH, "json_pointer", 3, 2),
                (LITERAL_PIN_PATH, "anchored_line", 1, 0),
            ],
        )
        self.assertEqual(
            [(entry["path"], entry["blob_sha"]) for entry in advancement["archival_provenance"]],
            [(ARCHIVAL_PATH, archival_before)],
        )
        # The released product ref and all of its pins are untouched.
        self.assertEqual(self.git("rev-parse", PRODUCT_REF).stdout.strip(), self.product_sha)
        self.assert_product_head_carries_floor(self.base_sha, absent=self.source_sha)

    def test_advance_floor_refuses_a_pin_that_lost_the_previous_floor(self) -> None:
        before_metadata = (self.repo / FLOOR_PATH).read_bytes()
        self.prepare()
        self.resolve_product_conflict()
        # Poison one pin: the merge tree no longer states the previous floor.
        (self.repo / LITERAL_PIN_PATH).write_text("The accepted floor is unknown.\n", encoding="utf-8")
        self.git("add", LITERAL_PIN_PATH)
        advanced = self.run_train("advance-floor")
        self.assertEqual(advanced.returncode, 1, advanced.stdout)
        self.assertIn("floor pin", self.result_json(advanced)["error"])
        self.assertIn(LITERAL_PIN_PATH, self.result_json(advanced)["error"])
        sync_ref = "refs/heads/sync/upstream/" + self.source_sha[:12]
        self.assertEqual(self.git("rev-parse", sync_ref).stdout.strip(), self.product_sha)
        self.assertEqual(self.git("rev-parse", PRODUCT_REF).stdout.strip(), self.product_sha)
        state = json.loads((self.train / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["status"], "conflicted")
        self.assertNotIn("candidate_tree_sha", state)
        # A refused advance writes nothing: metadata and the other pins are untouched.
        self.assertEqual((self.repo / FLOOR_PATH).read_bytes(), before_metadata)
        self.assertNotIn(self.source_sha, (self.repo / POLICY_PATH).read_text(encoding="utf-8"))
        self.assertFalse((self.repo / RECEIPT_PREFIX).parent.exists(), RECEIPT_PREFIX)
        # An undeclared literal inside a declared pin is refused as well.
        self.git("checkout", self.product_sha, "--", LITERAL_PIN_PATH)
        (self.repo / LITERAL_PIN_PATH).write_text(
            f"Notes.\nThe accepted floor is `{self.base_sha}`.\nAlso {self.base_sha}.\n", encoding="utf-8"
        )
        self.git("add", LITERAL_PIN_PATH)
        advanced = self.run_train("advance-floor")
        self.assertEqual(advanced.returncode, 1, advanced.stdout)
        self.assertIn("declared targets explain 1", self.result_json(advanced)["error"])

    def test_publish_refuses_a_tree_mutated_after_gates_passed(self) -> None:
        self.prepare()
        self.resolve_product_conflict()
        advanced = self.advance_floor()
        candidate_tree = advanced["candidate_tree_sha"]
        self.assertEqual(
            json.loads((self.repo / POLICY_PATH).read_text(encoding="utf-8"))["floor"]["sha"],
            self.source_sha,
        )
        self.record_gates()
        state = json.loads((self.train / "state.json").read_text(encoding="utf-8"))
        self.assertEqual([gate["tree_sha"] for gate in state["gates"]], [candidate_tree] * 6)
        # Mutate a pin after every gate passed against the candidate tree.
        (self.repo / LITERAL_PIN_PATH).write_text(
            f"Notes.\nThe accepted floor is `{self.source_sha}`.\nMore notes.\nlate edit\n",
            encoding="utf-8",
        )
        sync_ref = "refs/heads/sync/upstream/" + self.source_sha[:12]
        published = self.run_train("publish")
        self.assertEqual(published.returncode, 1, published.stdout)
        error = self.result_json(published)["error"]
        self.assertIn("changed after the gates passed", error)
        self.assertIn(candidate_tree, error)
        self.assertEqual(
            self.git("rev-parse", sync_ref).stdout.strip(), advanced["candidate_commit_sha"]
        )
        self.assertEqual(self.git("rev-parse", PRODUCT_REF).stdout.strip(), self.product_sha)
        self.assertEqual(
            self.git("for-each-ref", "--format=%(refname)", "refs/heads/sync/upstream").stdout.strip(),
            sync_ref,
        )
        state = json.loads((self.train / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["status"], "advanced")
        # A gate cannot be re-recorded against the mutated tree either.
        regate = self.run_train(
            "record-gate",
            "--id",
            "generated_drift",
            "--command-json",
            json.dumps(["python3", "-c", "raise SystemExit(0)"]),
        )
        self.assertEqual(regate.returncode, 1, regate.stdout)
        self.assertIn("differs from the advanced candidate tree", self.result_json(regate)["error"])
        # Restoring the exact candidate tree makes publish acceptable again.
        (self.repo / LITERAL_PIN_PATH).write_text(
            f"Notes.\nThe accepted floor is `{self.source_sha}`.\nMore notes.\n", encoding="utf-8"
        )
        published = self.run_train("publish")
        self.assertEqual(published.returncode, 0, published.stdout + published.stderr)
        self.assertEqual(self.result_json(published)["candidate_tree_sha"], candidate_tree)

    def test_external_gate_evidence_must_name_the_candidate_tree(self) -> None:
        self.prepare()
        self.resolve_product_conflict()
        candidate_tree = self.advance_floor()["candidate_tree_sha"]
        unbound = self.run_train(
            "record-gate", "--id", "upstream_required", "--status", "passed", "--evidence", "ran elsewhere"
        )
        self.assertEqual(unbound.returncode, 1, unbound.stdout)
        self.assertIn("--tree-sha", self.result_json(unbound)["error"])
        wrong = self.run_train(
            "record-gate",
            "--id",
            "upstream_required",
            "--status",
            "passed",
            "--evidence",
            "ran elsewhere",
            "--tree-sha",
            self.product_sha,
        )
        self.assertEqual(wrong.returncode, 1, wrong.stdout)
        self.assertIn("not the advanced candidate tree", self.result_json(wrong)["error"])
        candidate_commit = json.loads((self.train / "state.json").read_text(encoding="utf-8"))[
            "candidate_commit_sha"
        ]
        bound = self.run_train(
            "record-gate",
            "--id",
            "upstream_required",
            "--status",
            "passed",
            "--evidence",
            "cargo lane ran by root",
            "--tree-sha",
            candidate_tree,
            "--ci-run",
            "https://github.com/BleedingDev/tracedecay/actions/runs/123456",
            "--ci-head-sha",
            candidate_commit,
        )
        self.assertEqual(bound.returncode, 0, bound.stdout + bound.stderr)
        self.assertEqual(self.result_json(bound)["gate"]["tree_sha"], candidate_tree)
        self.assertEqual(self.result_json(bound)["gate"]["status"], "passed")
        # A gate command that mutates the candidate tree fails closed.
        self.gate_mutate.write_text("1", encoding="utf-8")
        mutating = self.record_gate("product_contracts", self.gate_argv)
        self.assertEqual(mutating.returncode, 1, mutating.stdout)
        state = json.loads((self.train / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["status"], "failed")
        self.assertEqual(state["gates"][1]["status"], "failed")
        self.assertTrue(any("changed the candidate tree" in item for item in state["gates"][1]["evidence"]))

    def test_prepare_refuses_undeclared_files_that_pin_the_floor(self) -> None:
        sync_ref = "refs/heads/sync/upstream/" + self.source_sha[:12]
        (self.repo / "docs/undeclared.md").write_text(
            f"Floor: {self.base_sha}\n", encoding="utf-8"
        )
        self.git("add", "docs/undeclared.md")
        self.git("commit", "-q", "-m", "add an undeclared pin")
        result = self.run_train("prepare", "--product-branch", PRODUCT_REF, "--source-ref", SOURCE_REF)
        self.assertEqual(result.returncode, 1, result.stdout)
        error = self.result_json(result)["error"]
        self.assertIn("undeclared paths", error)
        self.assertIn("docs/undeclared.md", error)
        self.assertEqual(self.git("rev-parse", "--verify", "--quiet", sync_ref, check=False).returncode, 1)
        self.assertEqual(self.git("branch", "--show-current").stdout.strip(), "product")
        self.git("rm", "-q", "docs/undeclared.md")
        self.git("commit", "-q", "-m", "drop the undeclared pin")
        prepared = self.prepare()
        self.assertEqual(
            prepared["floor_references"],
            {
                "archival_provenance": [ARCHIVAL_PATH],
                "canonical_metadata": [FLOOR_PATH],
                "floor_pin": sorted(PIN_PATHS),
                "historical_record": [HISTORICAL_PATH],
            },
        )

    def test_prepare_refuses_undeclared_or_missing_floor_pins(self) -> None:
        policy = json.loads((self.repo / POLICY_PATH).read_text(encoding="utf-8"))
        sync_ref = "refs/heads/sync/upstream/" + self.source_sha[:12]

        missing = json.loads(json.dumps(policy))
        missing["floor"]["pins"].append(
            {"path": "docs/absent.md", "kind": "anchored_line", "occurrences": 1, "line": "floor `{floor}`"}
        )
        (self.repo / POLICY_PATH).write_text(json.dumps(missing, indent=2) + "\n", encoding="utf-8")
        self.git("commit", "-q", "-am", "declare an absent pin")
        result = self.run_train(
            "prepare", "--product-branch", PRODUCT_REF, "--source-ref", SOURCE_REF
        )
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("docs/absent.md", self.result_json(result)["error"])
        self.assertEqual(self.git("rev-parse", "--verify", "--quiet", sync_ref, check=False).returncode, 1)
        self.assertEqual(self.git("branch", "--show-current").stdout.strip(), "product")

        without_policy = json.loads(json.dumps(policy))
        without_policy["floor"]["pins"] = [
            {"path": LITERAL_PIN_PATH, "kind": "anchored_line", "occurrences": 1, "line": "The accepted floor is `{floor}`."}
        ]
        (self.repo / POLICY_PATH).write_text(
            json.dumps(without_policy, indent=2) + "\n", encoding="utf-8"
        )
        self.git("commit", "-q", "-am", "drop the policy pin")
        result = self.run_train(
            "prepare", "--product-branch", PRODUCT_REF, "--source-ref", SOURCE_REF
        )
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("must include the sync policy itself", self.result_json(result)["error"])

        stale = json.loads(json.dumps(policy))
        (self.repo / POLICY_PATH).write_text(json.dumps(stale, indent=2) + "\n", encoding="utf-8")
        (self.repo / LITERAL_PIN_PATH).write_text("floor unknown\n", encoding="utf-8")
        self.git("commit", "-q", "-am", "pin drifted away from the floor")
        result = self.run_train(
            "prepare", "--product-branch", PRODUCT_REF, "--source-ref", SOURCE_REF
        )
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("does not carry floor", self.result_json(result)["error"])
        self.assertEqual(self.git("rev-parse", "--verify", "--quiet", sync_ref, check=False).returncode, 1)

    def test_rollback_after_publish_restores_prior_floor_and_withdraws_sync_ref(self) -> None:
        evidence = self.publish_train()
        final_sha = evidence["sync_head_sha"]
        sync_ref = evidence["sync_ref"]
        self.assertEqual(self.git("rev-parse", sync_ref).stdout.strip(), final_sha)
        self.assertEqual(self.git("branch", "--show-current").stdout.strip(), sync_ref.removeprefix("refs/heads/"))

        rolled = self.run_train("rollback")
        self.assertEqual(rolled.returncode, 0, rolled.stdout + rolled.stderr)
        outcome = self.result_json(rolled)
        self.assertEqual(outcome["status"], "rolled_back")
        self.assertTrue(outcome["sync_ref_removed"])
        self.assertFalse(outcome["sync_ref_retained"])
        self.assertEqual(outcome["withdrawn_commit_sha"], final_sha)
        self.assertEqual(outcome["restored_floor_sha"], self.base_sha)
        self.assertEqual(outcome["checkout_mode"], "product_branch")
        self.assertEqual(outcome["checkout_head_sha"], self.product_sha)
        self.assertTrue(outcome["worktree_clean"])
        self.assertEqual(outcome["restored_pins"], list(PIN_PATHS))
        self.assertEqual(self.git("rev-parse", "--verify", "--quiet", sync_ref, check=False).returncode, 1)
        self.assertEqual(self.git("rev-parse", PRODUCT_REF).stdout.strip(), self.product_sha)
        self.assertEqual(self.git("branch", "--show-current").stdout.strip(), "product")
        self.assertEqual(self.git("status", "--porcelain=v1").stdout, "")
        self.assert_product_head_carries_floor(self.base_sha, absent=self.source_sha)
        self.assertNotIn(self.source_sha, (self.repo / POLICY_PATH).read_text(encoding="utf-8"))

        receipt = json.loads(Path(outcome["terminal_receipt"]).read_text(encoding="utf-8"))
        self.assertEqual(receipt["terminal"]["state"], "rolled_back")
        self.assertEqual(receipt["floor_advancement"]["outcome"], "withdrawn")
        self.assertEqual(receipt["finalization"]["outcome"], "withdrawn")
        self.assertIsNone(receipt["finalization"]["sync_ref"])
        self.assertEqual(receipt["finalization"]["sync_head_sha"], final_sha)
        self.assertEqual(receipt["finalization"]["cas"]["result"], "matched_and_withdrawn")
        self.assertEqual(receipt["finalization"]["released_ref_update"]["mode"], "unchanged")
        self.assertEqual(receipt["finalization"]["released_head_sha"], self.product_sha)
        self.assertTrue(all(gate["status"] == "passed" for gate in receipt["gates"]))
        self.assertEqual(receipt["floor_advancement"]["gated_tree_sha"], evidence["candidate_tree_sha"])

        again = self.run_train("rollback")
        self.assertEqual(again.returncode, 0, again.stdout + again.stderr)
        self.assertEqual(self.result_json(again)["status"], "rolled_back")
        # The withdrawn train no longer blocks a fresh train in the same directory.
        prepared = self.prepare()
        self.assertEqual(prepared["sync_head_sha"], self.product_sha)

    def test_rollback_can_retain_the_withdrawn_sync_ref_as_review_evidence(self) -> None:
        evidence = self.publish_train()
        final_sha = evidence["sync_head_sha"]
        rolled = self.run_train("rollback", "--retain-sync-ref")
        self.assertEqual(rolled.returncode, 0, rolled.stdout + rolled.stderr)
        outcome = self.result_json(rolled)
        self.assertFalse(outcome["sync_ref_removed"])
        self.assertTrue(outcome["sync_ref_retained"])
        self.assertEqual(self.git("rev-parse", evidence["sync_ref"]).stdout.strip(), final_sha)
        self.assertEqual(self.git("rev-parse", PRODUCT_REF).stdout.strip(), self.product_sha)
        receipt = json.loads(Path(outcome["terminal_receipt"]).read_text(encoding="utf-8"))
        self.assertEqual(receipt["finalization"]["sync_ref"], evidence["sync_ref"])
        self.assertEqual(receipt["finalization"]["cas"]["result"], "not_attempted")
        self.assertEqual(receipt["terminal"]["state"], "rolled_back")

    def test_rollback_refuses_unfinalized_trains_and_promoted_product_refs(self) -> None:
        self.prepare()
        early = self.run_train("rollback")
        self.assertEqual(early.returncode, 1, early.stdout)
        self.assertIn("use abort", self.result_json(early)["error"])
        self.assertEqual(self.git("rev-parse", PRODUCT_REF).stdout.strip(), self.product_sha)

        aborted = self.run_train("abort")
        self.assertEqual(aborted.returncode, 0, aborted.stdout + aborted.stderr)
        evidence = self.publish_train()
        final_sha = evidence["sync_head_sha"]
        # Root promotes the train by fast-forwarding the released ref; that is
        # a promoted floor, which this workflow cannot reverse (no reverse
        # train exists) and therefore refuses instead of force-updating.
        self.git("update-ref", PRODUCT_REF, final_sha, self.product_sha)
        rolled = self.run_train("rollback")
        self.assertEqual(rolled.returncode, 1, rolled.stdout)
        self.assertIn("forward train", self.result_json(rolled)["error"])
        self.assertEqual(self.git("rev-parse", PRODUCT_REF).stdout.strip(), final_sha)
        self.assertEqual(self.git("rev-parse", evidence["sync_ref"]).stdout.strip(), final_sha)
        state = json.loads((self.train / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["status"], "finalized")

    def test_prepare_isolated_and_publish_is_one_atomic_train_commit(self) -> None:
        before_product = self.git("rev-parse", PRODUCT_REF).stdout.strip()
        before_metadata = (self.repo / FLOOR_PATH).read_bytes()
        prepared = self.prepare()
        self.assertEqual(prepared["status"], "conflicted")
        sync_ref = prepared["sync_ref"]
        self.assertEqual(prepared["product_head_sha"], self.product_sha)
        self.assertEqual(
            json.loads((self.train / "state.json").read_text(encoding="utf-8"))["bead_id"],
            "tdmem-1208",
        )
        self.assertEqual(self.git("rev-parse", sync_ref).stdout.strip(), self.product_sha)
        self.assertEqual(self.git("rev-parse", PRODUCT_REF).stdout.strip(), before_product)
        self.assertEqual((self.repo / FLOOR_PATH).read_bytes(), before_metadata)

        self.resolve_product_conflict()
        advanced = self.advance_floor()
        # advance-floor commits the ungated candidate on the isolated sync ref
        # only; the product ref and its metadata do not move.
        candidate_commit = advanced["candidate_commit_sha"]
        self.assertEqual(self.git("rev-parse", sync_ref).stdout.strip(), candidate_commit)
        self.assertEqual(self.git("rev-parse", "HEAD").stdout.strip(), candidate_commit)
        self.assertEqual(
            self.git("rev-parse", f"{candidate_commit}^{{tree}}").stdout.strip(),
            advanced["candidate_tree_sha"],
        )
        self.assertEqual(advanced["candidate_parents"], [self.product_sha, self.source_sha])
        self.assertEqual(self.git("rev-parse", PRODUCT_REF).stdout.strip(), before_product)
        self.assertEqual(
            self.git("show", f"{PRODUCT_REF}:{FLOOR_PATH}").stdout.encode(), before_metadata
        )
        self.assertEqual(
            json.loads((self.repo / FLOOR_PATH).read_text(encoding="utf-8"))["pinned_floor"]["sha"],
            self.source_sha,
        )
        self.assertEqual(self.advance_floor()["candidate_tree_sha"], advanced["candidate_tree_sha"])
        self.record_gates()
        published = self.run_train("publish")
        self.assertEqual(published.returncode, 0, published.stdout + published.stderr)
        evidence = self.result_json(published)
        self.assertEqual(evidence["status"], "finalized")
        final_sha = evidence["sync_head_sha"]
        receipt_path = RECEIPT_PREFIX + self.source_sha[:12] + ".json"
        self.assertEqual(self.git("rev-parse", sync_ref).stdout.strip(), final_sha)
        self.assertEqual(self.git("rev-parse", PRODUCT_REF).stdout.strip(), before_product)
        self.assertEqual(
            self.git("show", f"{PRODUCT_REF}:{FLOOR_PATH}").stdout.encode(),
            before_metadata,
        )
        metadata = json.loads(
            self.git("show", f"{final_sha}:{FLOOR_PATH}").stdout
        )
        self.assertEqual(metadata["pinned_floor"]["sha"], self.source_sha)
        receipt = json.loads(self.git("show", f"{final_sha}:{receipt_path}").stdout)
        self.assertEqual(receipt["upstream"]["candidate_ref"], SOURCE_REF)
        self.assertEqual(receipt["upstream"]["candidate_sha"], self.source_sha)
        self.assertEqual(receipt["bead_id"], "tdmem-1208")
        self.assertEqual(
            receipt["conflicts"][0]["source"],
            f"{SOURCE_REF}:{self.source_sha}",
        )
        self.assertEqual(receipt["conflicts"][0]["source_sha"], self.source_sha)
        self.assertEqual(receipt["conflicts"][0]["owner"], "product")
        self.assertTrue(receipt["conflicts"][0]["rationale"])
        self.assertEqual([gate["id"] for gate in receipt["gates"]], list(GATE_ORDER))
        self.assertTrue(all(gate["required"] for gate in receipt["gates"]))
        self.assertTrue(all(gate["status"] == "passed" for gate in receipt["gates"]))
        self.assertEqual(receipt["finalization"]["sync_ref"], sync_ref)
        self.assertIsNone(receipt["finalization"]["sync_head_sha"])
        self.assertEqual(
            receipt["finalization"]["released_ref_update"]["mode"], "unchanged"
        )
        self.assertTrue(receipt["finalization"]["same_commit"]["verified"])
        self.assertIsNone(
            receipt["finalization"]["same_commit"]["bundle_commit_sha"]
        )
        self.assertEqual(
            self.git("show", f"{final_sha}:code.txt").stdout,
            "base\nproduct change\nresolved\n",
        )
        self.assertEqual(
            self.git("log", "--first-parent", "--format=%H", f"{PRODUCT_REF}..{sync_ref}").stdout.splitlines(),
            [final_sha],
        )
        self.assertEqual(self.git("status", "--porcelain=v1").stdout, "")

    def test_prepare_records_missing_old_rename_path_for_source_correction(self) -> None:
        source_lines = "".join(f"line-{index}\n" for index in range(1, 10)) + "source\n"
        product_lines = "".join(f"line-{index}\n" for index in range(1, 10)) + "product\n"

        self.git("switch", "-q", "-c", "rename-source", self.floor_sha)
        (self.repo / "history.txt").write_text(source_lines, encoding="utf-8")
        self.git("commit", "-q", "-am", "upstream history change")
        self.git("mv", "history.txt", "renamed-history.txt")
        self.git("commit", "-q", "-m", "upstream history rename")
        source_sha = self.git("rev-parse", "HEAD").stdout.strip()
        self.git("update-ref", SOURCE_REF, source_sha)

        self.git("switch", "-q", "product")
        (self.repo / "history.txt").write_text(product_lines, encoding="utf-8")
        self.git("commit", "-q", "-am", "product history change")
        product_sha = self.git("rev-parse", "HEAD").stdout.strip()
        # Exercise the conflict path that Git emits when rename detection is
        # disabled.  The provenance lookup below still uses an explicit -M.
        self.git("config", "merge.renames", "false")

        prepared = self.prepare()
        self.assertEqual(prepared["status"], "conflicted")
        self.assertEqual(prepared["conflict_paths"], ["history.txt"])
        self.assertEqual(prepared["product_head_sha"], product_sha)
        state_path = self.train / "state.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertIsNone(state["conflicts"][0]["source"]["blob_sha"])

        result = self.run_train(
            "record-conflict",
            "--path",
            "history.txt",
            "--source-path",
            "renamed-history.txt",
            "--owner",
            "product",
            "--resolution",
            "retain_product_mount",
            "--rationale",
            "retain the product-owned history path after the upstream rename",
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        updated = json.loads(state_path.read_text(encoding="utf-8"))
        source = updated["conflicts"][0]["source"]
        self.assertEqual(source["path"], "renamed-history.txt")
        self.assertRegex(source["blob_sha"], r"^[0-9a-f]{40}$")

    def test_abort_preserves_product_and_invalidates_partial_train(self) -> None:
        before_product = self.git("rev-parse", PRODUCT_REF).stdout.strip()
        before_metadata = (self.repo / FLOOR_PATH).read_bytes()
        prepared = self.prepare()
        sync_ref = prepared["sync_ref"]
        result = self.run_train("abort")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        evidence = self.result_json(result)
        self.assertEqual(evidence["status"], "aborted")
        self.assertEqual(self.git("rev-parse", PRODUCT_REF).stdout.strip(), before_product)
        self.assertEqual((self.repo / FLOOR_PATH).read_bytes(), before_metadata)
        self.assertNotEqual(self.git("show-ref", "--verify", sync_ref, check=False).returncode, 0)
        state = json.loads((self.train / "state.json").read_text(encoding="utf-8"))
        self.assertTrue(state["invalidated"])
        self.assertEqual(state["status"], "aborted")
        receipt = json.loads(
            Path(evidence["terminal_receipt"]).read_text(encoding="utf-8")
        )
        self.assertEqual(receipt["terminal"]["state"], "aborted")
        self.assertEqual(receipt["finalization"]["outcome"], "not_published")
        self.assertEqual(self.git("symbolic-ref", "--quiet", "HEAD").stdout.strip(), PRODUCT_REF)

    def test_abort_from_linked_worktree_detaches_at_product_sha(self) -> None:
        linked_repo = self.root / "sync-worktree"
        linked_train = self.root / "linked-train"
        self.git("worktree", "add", "-q", "--detach", str(linked_repo), self.product_sha)
        self.addCleanup(
            lambda: self.git("worktree", "remove", "--force", str(linked_repo), check=False)
        )

        prepared_result = self.run_train_at(
            linked_repo,
            linked_train,
            "prepare",
            "--product-branch",
            PRODUCT_REF,
            "--source-ref",
            SOURCE_REF,
            "--floor-metadata",
            FLOOR_PATH,
        )
        self.assertEqual(
            prepared_result.returncode,
            0,
            prepared_result.stdout + prepared_result.stderr,
        )
        prepared = self.result_json(prepared_result)
        sync_ref = prepared["sync_ref"]
        self.assertEqual(
            self.git_at(linked_repo, "symbolic-ref", "--quiet", "HEAD").stdout.strip(),
            sync_ref,
        )

        aborted_result = self.run_train_at(linked_repo, linked_train, "abort")
        self.assertEqual(
            aborted_result.returncode,
            0,
            aborted_result.stdout + aborted_result.stderr,
        )
        evidence = self.result_json(aborted_result)
        self.assertEqual(evidence["status"], "aborted")
        self.assertEqual(evidence["checkout_mode"], "detached_product_sha")
        self.assertIsNone(evidence["current_branch"])
        self.assertEqual(evidence["checkout_head_sha"], self.product_sha)
        self.assertTrue(evidence["worktree_clean"])
        self.assertNotEqual(
            self.git_at(linked_repo, "symbolic-ref", "--quiet", "HEAD", check=False).returncode,
            0,
        )
        self.assertEqual(
            self.git_at(linked_repo, "rev-parse", "HEAD").stdout.strip(),
            self.product_sha,
        )
        self.assertEqual(self.git_at(linked_repo, "status", "--porcelain=v1").stdout, "")
        self.assertEqual(self.git("rev-parse", PRODUCT_REF).stdout.strip(), self.product_sha)
        self.assertNotEqual(self.git("show-ref", "--verify", sync_ref, check=False).returncode, 0)

        state = json.loads((linked_train / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["status"], "aborted")
        self.assertTrue(state["invalidated"])
        receipt = json.loads(
            (linked_train / "terminal-receipt.json").read_text(encoding="utf-8")
        )
        self.assertEqual(receipt["terminal"]["state"], "aborted")
        self.assertEqual(receipt["product"]["starting_head_sha"], self.product_sha)
        self.assertEqual(receipt["finalization"]["outcome"], "not_published")
        self.assertIsNone(receipt["finalization"]["sync_ref"])
        self.assertEqual(
            receipt["finalization"]["released_ref_update"]["mode"], "unchanged"
        )

        inspected_result = self.run_train_at(linked_repo, linked_train, "inspect")
        self.assertEqual(
            inspected_result.returncode,
            0,
            inspected_result.stdout + inspected_result.stderr,
        )
        inspected = self.result_json(inspected_result)
        self.assertTrue(inspected["ok"])
        self.assertIsNone(inspected["observed"]["current_branch"])
        self.assertEqual(inspected["observed"]["product_head_sha"], self.product_sha)
        self.assertIsNone(inspected["observed"]["sync_head_sha"])
        self.assertEqual(inspected["observed"]["unresolved_paths"], [])

    def test_advance_floor_rejects_unresolved_conflicts_and_publish_rejects_unadvanced(self) -> None:
        before_product = self.git("rev-parse", PRODUCT_REF).stdout.strip()
        self.prepare()
        result = self.run_train("advance-floor")
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        evidence = self.result_json(result)
        self.assertFalse(evidence["ok"])
        self.assertIn("unresolved Git conflicts", evidence["error"])
        published = self.run_train("publish")
        self.assertNotEqual(published.returncode, 0, published.stdout + published.stderr)
        self.assertIn("cannot publish a conflicted train", self.result_json(published)["error"])
        self.assertEqual(self.git("rev-parse", PRODUCT_REF).stdout.strip(), before_product)

    def test_advance_floor_rejects_wrong_branch(self) -> None:
        self.prepare()
        self.git("merge", "--abort")
        self.git("switch", "-q", "product")
        result = self.run_train("advance-floor")
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        evidence = self.result_json(result)
        self.assertIn("requires the isolated sync branch", evidence["error"])

    def test_advance_floor_rejects_product_branch_race(self) -> None:
        self.prepare()
        race_sha = self.git("rev-parse", self.source_sha).stdout.strip()
        self.git("update-ref", PRODUCT_REF, race_sha)
        result = self.run_train("advance-floor")
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        evidence = self.result_json(result)
        self.assertIn("product branch moved", evidence["error"])

    def test_advance_floor_rejects_moving_upstream_ref(self) -> None:
        self.prepare()
        # Moving the discovery ref to an already-existing commit is enough to
        # model a moving upstream observation and does not disturb the
        # conflicted isolated worktree.
        self.git("update-ref", SOURCE_REF, self.product_sha)
        result = self.run_train("advance-floor")
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        evidence = self.result_json(result)
        self.assertIn("upstream source ref moved", evidence["error"])

    def test_record_conflict_requires_nonempty_rationale(self) -> None:
        self.prepare()
        result = self.run_train(
            "record-conflict",
            "--path",
            "code.txt",
            "--owner",
            "product",
            "--resolution",
            "keep_product",
            "--rationale",
            "   ",
        )
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("non-empty string", self.result_json(result)["error"])

    def test_required_gates_are_ordered_and_publish_fails_closed(self) -> None:
        before_product = self.git("rev-parse", PRODUCT_REF).stdout.strip()
        self.prepare()
        self.resolve_product_conflict()
        unadvanced = self.run_train(
            "record-gate",
            "--id",
            "upstream_required",
            "--command-json",
            json.dumps(["python3", "-c", "raise SystemExit(0)"]),
        )
        self.assertNotEqual(unadvanced.returncode, 0)
        self.assertIn("run advance-floor", self.result_json(unadvanced)["error"])
        self.advance_floor()
        out_of_order = self.run_train(
            "record-gate",
            "--id",
            "product_contracts",
            "--command-json",
            json.dumps(["python3", "-c", "raise SystemExit(0)"]),
        )
        self.assertNotEqual(out_of_order.returncode, 0)
        self.assertIn("earlier required gates", self.result_json(out_of_order)["error"])
        published = self.run_train("publish")
        self.assertNotEqual(published.returncode, 0)
        self.assertIn("is 'not_run'", self.result_json(published)["error"])
        self.assertEqual(self.git("rev-parse", PRODUCT_REF).stdout.strip(), before_product)

    def test_failed_gate_is_durable_and_blocks_later_gates(self) -> None:
        self.prepare()
        self.resolve_product_conflict()
        self.advance_floor()
        self.gate_exit.write_text("7", encoding="utf-8")
        failed = self.record_gate("upstream_required", self.gate_argv)
        self.assertNotEqual(failed.returncode, 0)
        state = json.loads((self.train / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["status"], "failed")
        self.assertEqual(state["gates"][0]["status"], "failed")
        self.assertEqual(state["gates"][0]["exit_code"], 7)
        terminal_receipt = json.loads(
            (self.train / "terminal-receipt.json").read_text(encoding="utf-8")
        )
        self.assertEqual(terminal_receipt["terminal"]["state"], "failed")
        self.assertEqual(
            terminal_receipt["finalization"]["released_ref_update"]["mode"],
            "unchanged",
        )
        later = self.run_train(
            "record-gate",
            "--id",
            "product_contracts",
            "--command-json",
            json.dumps(["python3", "-c", "raise SystemExit(0)"]),
        )
        self.assertNotEqual(later.returncode, 0)
        self.assertIn("failed train", self.result_json(later)["error"])

    def test_conflict_receipt_rejects_fabricated_path_and_unknown_owner(self) -> None:
        self.prepare()
        fabricated = self.run_train(
            "record-conflict",
            "--path",
            "history.txt",
            "--owner",
            "product",
            "--resolution",
            "retain_product_mount",
            "--rationale",
            "this path was not in the merge conflict set",
        )
        self.assertNotEqual(fabricated.returncode, 0)
        self.assertIn("was not produced", self.result_json(fabricated)["error"])
        unknown_owner = self.run_train(
            "record-conflict",
            "--path",
            "code.txt",
            "--owner",
            "mystery-owner",
            "--resolution",
            "retain_product_mount",
            "--rationale",
            "unknown authority must fail closed",
        )
        self.assertNotEqual(unknown_owner.returncode, 0)
        self.assertIn("owner allowed by sync policy", self.result_json(unknown_owner)["error"])
        unrelated_source = self.run_train(
            "record-conflict",
            "--path",
            "code.txt",
            "--source-path",
            "history.txt",
            "--owner",
            "product",
            "--resolution",
            "retain_product_mount",
            "--rationale",
            "unrelated candidate files are not rename provenance",
        )
        self.assertNotEqual(unrelated_source.returncode, 0)
        self.assertIn("not Git rename provenance", self.result_json(unrelated_source)["error"])

    def test_prepare_rejects_refs_outside_sync_policy(self) -> None:
        unsafe_product = self.run_train(
            "prepare",
            "--product-branch",
            "refs/heads/master",
            "--source-ref",
            SOURCE_REF,
            "--floor-metadata",
            FLOOR_PATH,
        )
        self.assertNotEqual(unsafe_product.returncode, 0)
        self.assertIn("differs from sync policy", self.result_json(unsafe_product)["error"])
        unsafe_source = self.run_train(
            "prepare",
            "--product-branch",
            PRODUCT_REF,
            "--source-ref",
            "refs/heads/upstream-source",
            "--floor-metadata",
            FLOOR_PATH,
        )
        self.assertNotEqual(unsafe_source.returncode, 0)
        self.assertIn("approved policy discovery ref", self.result_json(unsafe_source)["error"])

    def test_prepare_derives_and_validates_policy_authority(self) -> None:
        stale = self.run_train(
            "prepare",
            "--product-branch",
            PRODUCT_REF,
            "--source-ref",
            SOURCE_REF,
            "--floor-metadata",
            FLOOR_PATH,
            "--bead-id",
            "tdmem-1205",
        )
        self.assertNotEqual(stale.returncode, 0)
        self.assertIn("bead id must be tdmem-1208 from sync policy", self.result_json(stale)["error"])

        policy_path = self.repo / POLICY_PATH
        policy = json.loads(policy_path.read_text(encoding="utf-8"))
        policy["workflow"]["first_floor_advancement_bead"] = "tdmem-1205"
        policy_path.write_text(json.dumps(policy, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        mismatched = self.run_train(
            "prepare",
            "--product-branch",
            PRODUCT_REF,
            "--source-ref",
            SOURCE_REF,
            "--floor-metadata",
            FLOOR_PATH,
        )
        self.assertNotEqual(mismatched.returncode, 0)
        self.assertIn("differs from workflow authority", self.result_json(mismatched)["error"])

    def test_publish_failure_leaves_floor_and_refs_unchanged(self) -> None:
        before_product = self.git("rev-parse", PRODUCT_REF).stdout.strip()
        before_floor = (self.repo / FLOOR_PATH).read_bytes()
        prepared = self.prepare()
        sync_ref = prepared["sync_ref"]
        self.resolve_product_conflict()
        self.advance_floor()
        self.record_gates()
        candidate_commit = self.git("rev-parse", sync_ref).stdout.strip()
        objects = self.repo / ".git/objects"
        objects.chmod(0o500)
        try:
            failed = self.run_train("publish")
            self.assertNotEqual(failed.returncode, 0)
            self.assertEqual(self.git("rev-parse", PRODUCT_REF).stdout.strip(), before_product)
            self.assertEqual(self.git("rev-parse", sync_ref).stdout.strip(), candidate_commit)
            self.assertEqual(
                self.git("show", f"{PRODUCT_REF}:{FLOOR_PATH}").stdout.encode(), before_floor
            )
        finally:
            objects.chmod(0o700)

    def test_late_publish_failure_then_abort_unpublishes_candidate(self) -> None:
        before_product = self.git("rev-parse", PRODUCT_REF).stdout.strip()
        before_tree = self.git("rev-parse", f"{PRODUCT_REF}^{{tree}}").stdout.strip()
        before_floor = (self.repo / FLOOR_PATH).read_bytes()
        prepared = self.prepare()
        sync_ref = prepared["sync_ref"]
        self.resolve_product_conflict()
        self.advance_floor()
        self.record_gates()

        objects = self.repo / ".git/objects"
        original_mode = objects.stat().st_mode & 0o777
        objects.chmod(0o500)
        try:
            failed = self.run_train("publish")
        finally:
            objects.chmod(original_mode)

        self.assertNotEqual(failed.returncode, 0, failed.stdout + failed.stderr)
        self.assertEqual(self.git("rev-parse", PRODUCT_REF).stdout.strip(), before_product)
        self.assertEqual(
            self.git("rev-parse", f"{PRODUCT_REF}^{{tree}}").stdout.strip(),
            before_tree,
        )
        self.assertEqual(
            self.git("show", f"{PRODUCT_REF}:{FLOOR_PATH}").stdout.encode(), before_floor
        )

        aborted = self.run_train("abort")
        self.assertEqual(aborted.returncode, 0, aborted.stdout + aborted.stderr)
        evidence = self.result_json(aborted)
        self.assertEqual(evidence["status"], "aborted")
        self.assertTrue(evidence["sync_ref_removed"])
        self.assertNotEqual(self.git("show-ref", "--verify", sync_ref, check=False).returncode, 0)
        self.assertEqual(
            self.git("for-each-ref", "--format=%(refname)", "refs/heads/sync/upstream").stdout,
            "",
        )
        self.assertEqual(self.git("rev-parse", PRODUCT_REF).stdout.strip(), before_product)
        self.assertEqual(
            self.git("rev-parse", f"{PRODUCT_REF}^{{tree}}").stdout.strip(),
            before_tree,
        )
        self.assertEqual((self.repo / FLOOR_PATH).read_bytes(), before_floor)
        self.assertEqual(self.git("status", "--porcelain=v1").stdout, "")
        self.assertEqual(
            self.git("symbolic-ref", "--quiet", "HEAD").stdout.strip(), PRODUCT_REF
        )

        receipt = json.loads(
            Path(evidence["terminal_receipt"]).read_text(encoding="utf-8")
        )
        self.assertEqual(receipt["terminal"]["state"], "aborted")
        self.assertEqual(receipt["finalization"]["outcome"], "not_published")
        self.assertIsNone(receipt["finalization"]["sync_ref"])
        self.assertEqual(
            receipt["finalization"]["released_ref_update"]["mode"], "unchanged"
        )

    def test_receipt_path_cannot_overwrite_product_code(self) -> None:
        result = self.run_train(
            "prepare",
            "--product-branch",
            PRODUCT_REF,
            "--source-ref",
            SOURCE_REF,
            "--floor-metadata",
            FLOOR_PATH,
            "--receipt-path",
            "code.txt",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("receipt path differs from sync policy", self.result_json(result)["error"])

    def test_gates_require_resolved_and_advanced_candidate_tree(self) -> None:
        self.prepare()
        self.record_product_conflict()
        advanced = self.run_train("advance-floor")
        self.assertNotEqual(advanced.returncode, 0)
        self.assertIn("unresolved Git conflicts remain", self.result_json(advanced)["error"])
        gate = self.run_train(
            "record-gate",
            "--id",
            "upstream_required",
            "--command-json",
            json.dumps(["python3", "-c", "raise SystemExit(0)"]),
        )
        self.assertNotEqual(gate.returncode, 0)
        self.assertIn("run advance-floor", self.result_json(gate)["error"])

    def test_gate_evidence_must_be_a_declared_lane_command_or_a_ci_run(self) -> None:
        self.prepare()
        self.resolve_product_conflict()
        advanced = self.advance_floor()
        candidate_tree = advanced["candidate_tree_sha"]
        candidate_commit = advanced["candidate_commit_sha"]
        self.assertEqual(advanced["verification_coverage"]["uncovered_commands"], [])
        # An executed command outside the bound lane is refused, not recorded.
        undeclared = self.record_gate("upstream_required", ["true"])
        self.assertEqual(undeclared.returncode, 1, undeclared.stdout)
        self.assertIn("is not one of the 2 commands", self.result_json(undeclared)["error"])
        # Free-text external evidence with a bound tree is still not proof.
        free_text = self.run_train(
            "record-gate", "--id", "upstream_required", "--status", "passed",
            "--evidence", "ran everything", "--tree-sha", candidate_tree,
        )
        self.assertEqual(free_text.returncode, 1, free_text.stdout)
        self.assertIn("free-text evidence is not proof", self.result_json(free_text)["error"])
        external_undeclared = self.run_train(
            "record-gate", "--id", "upstream_required", "--status", "passed",
            "--evidence", "ran true", "--tree-sha", candidate_tree,
            "--command", json.dumps(["true"]),
        )
        self.assertEqual(external_undeclared.returncode, 1, external_undeclared.stdout)
        self.assertIn("is not one of the 2 commands", self.result_json(external_undeclared)["error"])
        # A CI run must belong to the product repository and have run the candidate commit.
        foreign_run = self.run_train(
            "record-gate", "--id", "upstream_required", "--status", "passed",
            "--evidence", "ci", "--tree-sha", candidate_tree,
            "--ci-run", "https://github.com/ScriptedAlchemy/tracedecay/actions/runs/1",
            "--ci-head-sha", candidate_commit,
        )
        self.assertEqual(foreign_run.returncode, 1, foreign_run.stdout)
        self.assertIn("product repository", self.result_json(foreign_run)["error"])
        stale_run = self.run_train(
            "record-gate", "--id", "upstream_required", "--status", "passed",
            "--evidence", "ci", "--tree-sha", candidate_tree,
            "--ci-run", "https://github.com/BleedingDev/tracedecay/actions/runs/1",
            "--ci-head-sha", self.product_sha,
        )
        self.assertEqual(stale_run.returncode, 1, stale_run.stdout)
        self.assertIn("not the advanced candidate commit", self.result_json(stale_run)["error"])
        state = json.loads((self.train / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["gates"][0]["status"], "not_run")
        self.assertEqual(state["gates"][0]["commands"], [])
        # One declared command of a two-command lane leaves the gate in progress
        # and publish refuses; the second command completes the lane.
        first = self.record_gate("upstream_required", self.gate_argv)
        self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
        self.assertEqual(self.result_json(first)["gate"]["status"], "in_progress")
        self.assertEqual(self.result_json(first)["missing_commands"], [self.lane_extra])
        again = self.record_gate("upstream_required", self.gate_argv)
        self.assertEqual(again.returncode, 1, again.stdout)
        self.assertIn("already passed", self.result_json(again)["error"])
        blocked = self.record_gate("product_contracts", self.gate_argv)
        self.assertEqual(blocked.returncode, 1, blocked.stdout)
        self.assertIn("earlier required gates", self.result_json(blocked)["error"])
        published = self.run_train("publish")
        self.assertEqual(published.returncode, 1, published.stdout)
        self.assertIn("is 'in_progress'", self.result_json(published)["error"])
        # The prelude form runs the declared line exactly as the CI lane does.
        second = self.record_gate(
            "upstream_required", ["bash", "-euo", "pipefail", "-c", self.lane_extra]
        )
        self.assertEqual(second.returncode, 0, second.stdout + second.stderr)
        self.assertEqual(self.result_json(second)["gate"]["status"], "passed")
        self.assertEqual(self.result_json(second)["gate"]["coverage"], {"declared": 2, "passed": 2, "missing": []})
        exhausted = self.run_train(
            "record-gate", "--id", "upstream_required", "--status", "passed",
            "--evidence", "ci", "--tree-sha", candidate_tree,
            "--ci-run", "https://github.com/BleedingDev/tracedecay/actions/runs/1",
            "--ci-head-sha", candidate_commit,
        )
        self.assertEqual(exhausted.returncode, 1, exhausted.stdout)
        self.assertIn("already passed", self.result_json(exhausted)["error"])

    def test_stamped_targets_whose_commands_no_lane_runs_block_the_train(self) -> None:
        pinned_map = json.loads((self.repo / MAP_PIN_PATH).read_text(encoding="utf-8"))
        pinned_map["entries"][1]["verification"] = ["cargo test -p nothing --locked"]
        (self.repo / MAP_PIN_PATH).write_text(json.dumps(pinned_map, indent=2) + "\n", encoding="utf-8")
        self.git("commit", "-q", "-am", "stamp an unverifiable entry")
        product_before = self.git("rev-parse", PRODUCT_REF).stdout.strip()
        self.prepare()
        self.resolve_product_conflict()
        sync_ref = "refs/heads/sync/upstream/" + self.source_sha[:12]
        advanced = self.run_train("advance-floor")
        self.assertEqual(advanced.returncode, 1, advanced.stdout)
        error = self.result_json(advanced)["error"]
        self.assertIn("run by no gate lane", error)
        self.assertIn("cargo test -p nothing --locked <- product/upstream/map.json#/entries/1/verification", error)
        # No ref moved and no gate can be recorded; publish has nothing to publish.
        self.assertEqual(self.git("rev-parse", sync_ref).stdout.strip(), product_before)
        self.assertEqual(self.git("rev-parse", PRODUCT_REF).stdout.strip(), product_before)
        state = json.loads((self.train / "state.json").read_text(encoding="utf-8"))
        self.assertNotEqual(state["status"], "advanced")
        gate = self.record_gate("upstream_required", self.gate_argv)
        self.assertEqual(gate.returncode, 1, gate.stdout)
        self.assertIn("run advance-floor", self.result_json(gate)["error"])
        published = self.run_train("publish")
        self.assertEqual(published.returncode, 1, published.stdout)
        self.assertIn("advance-floor must produce", self.result_json(published)["error"])
        self.assertEqual(self.git("rev-parse", PRODUCT_REF).stdout.strip(), product_before)

    def test_lane_commands_must_match_the_workflow_job_in_the_candidate(self) -> None:
        drifted = {gate_id: dict(lane) for gate_id, lane in self.lanes.items()}
        drifted["upstream_required"] = {
            **drifted["upstream_required"],
            "commands": [self.gate_command],
        }
        self.write_workflow(drifted)
        self.git("commit", "-q", "-am", "workflow drops a lane command the policy still declares")
        self.prepare()
        self.resolve_product_conflict()
        self.advance_floor()
        gate = self.record_gate("upstream_required", self.gate_argv)
        self.assertEqual(gate.returncode, 1, gate.stdout)
        error = self.result_json(gate)["error"]
        self.assertIn("workflow job at the candidate commit does not run it", error)
        self.assertIn(self.lane_extra, error)
        state = json.loads((self.train / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["gates"][0]["status"], "not_run")


if __name__ == "__main__":
    unittest.main()
