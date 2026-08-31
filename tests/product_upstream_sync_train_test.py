#!/usr/bin/env python3
"""Behavioral tests for the product-owned isolated upstream sync train."""

from __future__ import annotations

import json
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
                        "immutable_until_approved_train": True,
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

    def git(self, *arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            ["git", "-C", str(self.repo), *arguments],
            check=False,
            capture_output=True,
            text=True,
        )
        if check:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def run_train(self, command: str, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                "python3",
                str(RUNNER),
                command,
                "--repo",
                str(self.repo),
                "--train-dir",
                str(self.train),
                *arguments,
            ],
            check=False,
            capture_output=True,
            text=True,
        )

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

    def record_gates(self) -> None:
        command = json.dumps(["python3", "-c", "raise SystemExit(0)"])
        for gate_id in GATE_ORDER:
            result = self.run_train(
                "record-gate",
                "--id",
                gate_id,
                "--command-json",
                command,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_prepare_isolated_and_finalize_is_one_atomic_train_commit(self) -> None:
        before_product = self.git("rev-parse", PRODUCT_REF).stdout.strip()
        before_metadata = (self.repo / FLOOR_PATH).read_bytes()
        prepared = self.prepare()
        self.assertEqual(prepared["status"], "conflicted")
        sync_ref = prepared["sync_ref"]
        self.assertEqual(prepared["product_head_sha"], self.product_sha)
        self.assertEqual(self.git("rev-parse", sync_ref).stdout.strip(), self.product_sha)
        self.assertEqual(self.git("rev-parse", PRODUCT_REF).stdout.strip(), before_product)
        self.assertEqual((self.repo / FLOOR_PATH).read_bytes(), before_metadata)

        self.record_product_conflict()
        (self.repo / "code.txt").write_text("base\nproduct change\nresolved\n", encoding="utf-8")
        self.git("add", "code.txt")
        self.record_gates()
        finalized = self.run_train("finalize")
        self.assertEqual(finalized.returncode, 0, finalized.stdout + finalized.stderr)
        evidence = self.result_json(finalized)
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

    def test_finalize_rejects_unresolved_conflicts(self) -> None:
        before_product = self.git("rev-parse", PRODUCT_REF).stdout.strip()
        self.prepare()
        result = self.run_train("finalize")
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        evidence = self.result_json(result)
        self.assertFalse(evidence["ok"])
        self.assertIn("unresolved Git conflicts", evidence["error"])
        self.assertEqual(self.git("rev-parse", PRODUCT_REF).stdout.strip(), before_product)

    def test_finalize_rejects_wrong_branch(self) -> None:
        self.prepare()
        self.git("merge", "--abort")
        self.git("switch", "-q", "product")
        result = self.run_train("finalize")
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        evidence = self.result_json(result)
        self.assertIn("requires the isolated sync branch", evidence["error"])

    def test_finalize_rejects_product_branch_race(self) -> None:
        self.prepare()
        race_sha = self.git("rev-parse", self.source_sha).stdout.strip()
        self.git("update-ref", PRODUCT_REF, race_sha)
        result = self.run_train("finalize")
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        evidence = self.result_json(result)
        self.assertIn("product branch moved", evidence["error"])

    def test_finalize_rejects_moving_upstream_ref(self) -> None:
        self.prepare()
        # Moving the discovery ref to an already-existing commit is enough to
        # model a moving upstream observation and does not disturb the
        # conflicted isolated worktree.
        self.git("update-ref", SOURCE_REF, self.product_sha)
        result = self.run_train("finalize")
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

    def test_required_gates_are_ordered_and_finalize_fails_closed(self) -> None:
        before_product = self.git("rev-parse", PRODUCT_REF).stdout.strip()
        self.prepare()
        self.record_product_conflict()
        (self.repo / "code.txt").write_text(
            "base\nproduct change\nresolved\n", encoding="utf-8"
        )
        self.git("add", "code.txt")
        out_of_order = self.run_train(
            "record-gate",
            "--id",
            "product_contracts",
            "--command-json",
            json.dumps(["python3", "-c", "raise SystemExit(0)"]),
        )
        self.assertNotEqual(out_of_order.returncode, 0)
        self.assertIn("earlier required gates", self.result_json(out_of_order)["error"])
        finalize = self.run_train("finalize")
        self.assertNotEqual(finalize.returncode, 0)
        self.assertIn("is 'not_run'", self.result_json(finalize)["error"])
        self.assertEqual(self.git("rev-parse", PRODUCT_REF).stdout.strip(), before_product)

    def test_failed_gate_is_durable_and_blocks_later_gates(self) -> None:
        self.prepare()
        self.record_product_conflict()
        (self.repo / "code.txt").write_text(
            "base\nproduct change\nresolved\n", encoding="utf-8"
        )
        self.git("add", "code.txt")
        failed = self.run_train(
            "record-gate",
            "--id",
            "upstream_required",
            "--command-json",
            json.dumps(["python3", "-c", "raise SystemExit(7)"]),
        )
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

    def test_finalize_failure_leaves_floor_and_refs_unchanged(self) -> None:
        before_product = self.git("rev-parse", PRODUCT_REF).stdout.strip()
        before_floor = (self.repo / FLOOR_PATH).read_bytes()
        prepared = self.prepare()
        sync_ref = prepared["sync_ref"]
        self.record_product_conflict()
        (self.repo / "code.txt").write_text(
            "base\nproduct change\nresolved\n", encoding="utf-8"
        )
        self.git("add", "code.txt")
        self.record_gates()
        objects = self.repo / ".git/objects"
        objects.chmod(0o500)
        try:
            failed = self.run_train("finalize")
            self.assertNotEqual(failed.returncode, 0)
            self.assertEqual(self.git("rev-parse", PRODUCT_REF).stdout.strip(), before_product)
            self.assertEqual(self.git("rev-parse", sync_ref).stdout.strip(), before_product)
            self.assertEqual((self.repo / FLOOR_PATH).read_bytes(), before_floor)
        finally:
            objects.chmod(0o700)

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

    def test_gates_require_resolved_conflict_tree(self) -> None:
        self.prepare()
        self.record_product_conflict()
        gate = self.run_train(
            "record-gate",
            "--id",
            "upstream_required",
            "--command-json",
            json.dumps(["python3", "-c", "raise SystemExit(0)"]),
        )
        self.assertNotEqual(gate.returncode, 0)
        self.assertIn("conflicts remain unresolved", self.result_json(gate)["error"])


if __name__ == "__main__":
    unittest.main()
