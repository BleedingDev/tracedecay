#!/usr/bin/env python3
"""Behavioral tests for upstream floor change classification."""

from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
CLASSIFIER = REPO / "scripts/product/classify-upstream-changes.py"


def upstream_area(
    area_id: str,
    patterns: list[str],
    touch_points: list[str],
    *,
    ownership_class: str = "upstream_owned",
) -> dict[str, Any]:
    return {
        "id": area_id,
        "status": "active",
        "owner": (
            "ScriptedAlchemy"
            if ownership_class == "upstream_owned"
            else "BleedingDev"
        ),
        "ownership_class": ownership_class,
        "path_patterns": patterns,
        "touch_points": touch_points,
    }


def patch(
    path: str,
    area_id: str,
    touch_point: str,
    *,
    generated: bool = False,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "path": path,
        "area_id": area_id,
        "owner": "BleedingDev",
        "upstream_owner": "ScriptedAlchemy",
        "touch_point": touch_point,
        "bead_ids": ["tdmem-0308"],
        "status": "active",
    }
    if generated:
        row["generated"] = {
            "generator_path": "Cargo.toml",
            "reproduction": "cargo metadata --format-version 1 --no-deps",
            "zero_drift_check": "cargo metadata --locked --format-version 1 --no-deps",
        }
    return row


class GitFixture:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.git("init", "--initial-branch=master")
        self.git("config", "user.name", "Upstream Classifier Test")
        self.git("config", "user.email", "classifier@example.invalid")

    def git(self, *args: str) -> str:
        result = subprocess.run(
            ["git", "-C", str(self.root), *args],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()

    def write(self, relative: str, contents: str | bytes) -> None:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(contents, bytes):
            path.write_bytes(contents)
        else:
            path.write_text(contents, encoding="utf-8")

    def commit(self, message: str) -> str:
        self.git("add", "--all")
        self.git("commit", "--no-gpg-sign", "-m", message)
        return self.git("rev-parse", "HEAD")

    def authorities(
        self,
        old_floor: str,
        areas: list[dict[str, Any]],
        entries: list[dict[str, Any]],
        touch_points: list[dict[str, Any]],
        zones: list[dict[str, Any]] | None = None,
    ) -> tuple[Path, Path]:
        map_path = self.root / "classification-map.json"
        policy_path = self.root / "classification-policy.json"
        map_path.write_text(
            json.dumps(
                {
                    "schema_version": 2,
                    "policy_revision": "patch-footprint.test.v1",
                    "upstream_floor_sha": old_floor,
                    "owners": {
                        "product": {"id": "BleedingDev"},
                        "upstream": {"id": "ScriptedAlchemy"},
                    },
                    "areas": [
                        {**row, "last_verified_upstream_sha": old_floor}
                        for row in areas
                    ],
                    "entries": [
                        {**row, "last_verified_upstream_sha": old_floor}
                        for row in entries
                    ],
                    "classification_contract": {
                        "path_format": "repo-relative-posix",
                        "precedence": [
                            "active_upstream_entry_exact_path",
                            "product_area_path_pattern",
                            "policy_touch_point_path",
                        ],
                        "ambiguous_match": "error",
                        "unclassified_path": "error",
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        policy_path.write_text(
            json.dumps(
                {
                    "policy_revision": "patch-footprint.test.v1",
                    "upstream_floor": {"sha": old_floor},
                    "allowed_touch_points": touch_points,
                    "exception_zones": zones or [],
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        return map_path, policy_path


class UpstreamChangeClassificationTest(unittest.TestCase):
    def run_classifier(
        self,
        fixture: GitFixture,
        old: str,
        candidate: str,
        map_path: Path,
        policy_path: Path,
    ) -> tuple[subprocess.CompletedProcess[str], dict[str, Any]]:
        result = subprocess.run(
            [
                "python3",
                str(CLASSIFIER),
                "--repo",
                str(fixture.root),
                "--old-floor",
                old,
                "--candidate-floor",
                candidate,
                "--map",
                str(map_path),
                "--policy",
                str(policy_path),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        return result, json.loads(result.stdout)

    def test_deterministic_report_lists_first_middle_last_and_changed_crate(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = GitFixture(Path(temp_dir))
            fixture.write(
                "Cargo.toml",
                '[workspace]\nmembers = ["crates/upstream"]\nresolver = "2"\n',
            )
            fixture.write(
                "crates/upstream/Cargo.toml",
                '[package]\nname = "upstream-crate"\nversion = "0.1.0"\n',
            )
            for index in range(9):
                fixture.write(f"crates/upstream/src/file_{index:02}.rs", "old\n")
            old = fixture.commit("old floor")
            for index in range(9):
                fixture.write(f"crates/upstream/src/file_{index:02}.rs", "candidate\n")
            candidate = fixture.commit("candidate floor")
            map_path, policy_path = fixture.authorities(
                old,
                [
                    upstream_area(
                        "memory_seam",
                        ["crates/upstream/**"],
                        ["memory_mount"],
                    )
                ],
                [
                    patch(
                        "crates/upstream/src/file_04.rs",
                        "memory_seam",
                        "memory_mount",
                    )
                ],
                [{"id": "memory_mount", "paths": ["crates/upstream/**"]}],
            )

            result, report = self.run_classifier(
                fixture, old, candidate, map_path, policy_path
            )
            repeated, repeated_report = self.run_classifier(
                fixture, old, candidate, map_path, policy_path
            )

            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(result.stdout, repeated.stdout)
            self.assertEqual(report, repeated_report)
            self.assertEqual(report["inputs"]["old_floor"]["commit"], old)
            self.assertEqual(report["inputs"]["candidate_floor"]["commit"], candidate)
            self.assertEqual(report["summary"]["changed_file_count"], 9)
            paths = [row["path"] for row in report["changed_files"]]
            self.assertEqual(paths[0], "crates/upstream/src/file_00.rs")
            self.assertEqual(paths[4], "crates/upstream/src/file_04.rs")
            self.assertEqual(paths[-1], "crates/upstream/src/file_08.rs")
            self.assertEqual(
                report["changed_crates"],
                [
                    {
                        "changed_file_count": 9,
                        "manifest": "crates/upstream/Cargo.toml",
                        "name": "upstream-crate",
                        "present_after": True,
                        "present_before": True,
                        "root": "crates/upstream",
                    }
                ],
            )
            self.assertEqual(report["summary"]["change_set_kind"], "semantic")
            self.assertEqual(
                [row["path"] for row in report["mapped_product_patches"]],
                ["crates/upstream/src/file_04.rs"],
            )
            self.assertEqual(report["summary"]["unmapped_file_count"], 8)

    def test_unmapped_failures_have_complete_counts_and_bounded_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = GitFixture(Path(temp_dir))
            for index in range(80):
                fixture.write(f"crates/seam/src/file_{index:03}.rs", "old\n")
            old = fixture.commit("old floor")
            for index in range(80):
                fixture.write(f"crates/seam/src/file_{index:03}.rs", "candidate\n")
            candidate = fixture.commit("candidate floor")
            map_path, policy_path = fixture.authorities(
                old,
                [upstream_area("seam", ["crates/seam/**"], ["memory_mount"])],
                [],
                [{"id": "memory_mount", "paths": ["crates/seam/**"]}],
            )

            result, report = self.run_classifier(
                fixture, old, candidate, map_path, policy_path
            )

            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            self.assertEqual(report["summary"]["review_gate"], "fail")
            self.assertEqual(report["summary"]["unmapped_file_count"], 80)
            self.assertEqual(len(report["changed_files"]), 80)
            self.assertEqual(report["diagnostics"]["total"], 80)
            self.assertEqual(report["diagnostics"]["shown"], 50)
            self.assertEqual(report["diagnostics"]["truncated"], 30)
            self.assertEqual(
                report["diagnostics"]["items"][0]["path"],
                "crates/seam/src/file_000.rs",
            )
            self.assertEqual(
                report["diagnostics"]["items"][-1]["path"],
                "crates/seam/src/file_049.rs",
            )

    def test_generated_only_and_mixed_changes_are_distinct_and_not_auto_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = GitFixture(Path(temp_dir))
            fixture.write("Cargo.toml", '[workspace]\nmembers = []\nresolver = "2"\n')
            fixture.write("Cargo.lock", "# old lock\n")
            old = fixture.commit("old floor")
            fixture.write("Cargo.lock", "# generated lock\n")
            generated_candidate = fixture.commit("generated candidate")
            fixture.write("Cargo.toml", '[workspace]\nmembers = ["crates/new"]\nresolver = "2"\n')
            mixed_candidate = fixture.commit("mixed candidate")
            fixture.git("checkout", "--detach", generated_candidate)
            fixture.git("rm", "Cargo.lock")
            deleted_generated_candidate = fixture.commit("delete generated output")
            map_path, policy_path = fixture.authorities(
                old,
                [upstream_area("workspace", ["Cargo.toml", "Cargo.lock"], ["workspace"])],
                [patch("Cargo.lock", "workspace", "workspace", generated=True)],
                [{"id": "workspace", "paths": ["Cargo.toml", "Cargo.lock"]}],
                [
                    {
                        "id": "generated_contracts",
                        "default_policy": "generated_only",
                        "paths": ["Cargo.lock"],
                    }
                ],
            )

            generated_result, generated = self.run_classifier(
                fixture, old, generated_candidate, map_path, policy_path
            )
            mixed_result, mixed = self.run_classifier(
                fixture, old, mixed_candidate, map_path, policy_path
            )
            deleted_result, deleted = self.run_classifier(
                fixture, old, deleted_generated_candidate, map_path, policy_path
            )

            self.assertEqual(
                generated_result.returncode,
                0,
                generated_result.stdout + generated_result.stderr,
            )
            self.assertEqual(generated["summary"]["change_set_kind"], "generated_only")
            self.assertEqual(generated["summary"]["generated_file_count"], 1)
            self.assertEqual(generated["summary"]["semantic_file_count"], 0)
            self.assertFalse(generated["summary"]["auto_accept"])
            self.assertEqual(mixed_result.returncode, 1, mixed_result.stdout + mixed_result.stderr)
            self.assertEqual(mixed["summary"]["change_set_kind"], "mixed")
            self.assertEqual(mixed["summary"]["generated_file_count"], 1)
            self.assertEqual(mixed["summary"]["semantic_file_count"], 1)
            cargo_toml = next(
                row for row in mixed["changed_files"] if row["path"] == "Cargo.toml"
            )
            self.assertEqual(cargo_toml["classification_status"], "unmapped")
            self.assertEqual(cargo_toml["change_kind"], "semantic")
            self.assertEqual(deleted_result.returncode, 0)
            self.assertEqual(deleted["summary"]["change_set_kind"], "generated_only")
            self.assertEqual(deleted["changed_files"][0]["status"], "deleted")
            self.assertEqual(
                deleted["changed_files"][0]["generated_evidence"]["source"],
                "convergence_entry",
            )

    def test_unrelated_upstream_change_is_reported_without_blocking_review(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = GitFixture(Path(temp_dir))
            fixture.write("README.md", "old\n")
            old = fixture.commit("old floor")
            fixture.write("README.md", "new\n")
            candidate = fixture.commit("candidate floor")
            map_path, policy_path = fixture.authorities(old, [], [], [])

            result, report = self.run_classifier(
                fixture, old, candidate, map_path, policy_path
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(report["summary"]["review_gate"], "pass")
            self.assertEqual(report["summary"]["unmapped_file_count"], 0)
            self.assertEqual(
                report["changed_files"][0]["classification_status"],
                "unrelated_upstream",
            )
            self.assertEqual(report["mapped_product_patches"], [])

            stale_map = json.loads(map_path.read_text(encoding="utf-8"))
            stale_map["upstream_floor_sha"] = candidate
            map_path.write_text(
                json.dumps(stale_map, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            stale_result, stale_report = self.run_classifier(
                fixture, old, candidate, map_path, policy_path
            )
            self.assertEqual(stale_result.returncode, 1)
            self.assertEqual(stale_report["summary"]["review_gate"], "fail")
            self.assertIn(
                "stale_authority",
                [row["code"] for row in stale_report["diagnostics"]["items"]],
            )

    def test_deleted_crate_is_attributed_from_the_old_commit_tree(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = GitFixture(Path(temp_dir))
            fixture.write(
                "Cargo.toml",
                '[workspace]\nmembers = ["crates/obsolete"]\nresolver = "2"\n',
            )
            fixture.write(
                "crates/obsolete/Cargo.toml",
                '[package]\nname = "obsolete-crate"\nversion = "0.1.0"\n',
            )
            fixture.write("crates/obsolete/src/lib.rs", "pub fn old() {}\n")
            old = fixture.commit("old floor")
            fixture.git("rm", "-r", "crates/obsolete")
            candidate = fixture.commit("delete crate")
            map_path, policy_path = fixture.authorities(
                old,
                [
                    upstream_area(
                        "obsolete",
                        ["crates/obsolete/**"],
                        ["obsolete_mount"],
                    )
                ],
                [
                    patch(
                        "crates/obsolete/Cargo.toml",
                        "obsolete",
                        "obsolete_mount",
                    ),
                    patch(
                        "crates/obsolete/src/lib.rs",
                        "obsolete",
                        "obsolete_mount",
                    ),
                ],
                [{"id": "obsolete_mount", "paths": ["crates/obsolete/**"]}],
            )

            result, report = self.run_classifier(
                fixture, old, candidate, map_path, policy_path
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(
                report["changed_crates"],
                [
                    {
                        "changed_file_count": 2,
                        "manifest": "crates/obsolete/Cargo.toml",
                        "name": "obsolete-crate",
                        "present_after": False,
                        "present_before": True,
                        "root": "crates/obsolete",
                    }
                ],
            )
            for row in report["changed_files"]:
                self.assertEqual(row["status"], "deleted")
                self.assertEqual(row["crate"]["name"], "obsolete-crate")
                self.assertIsNone(row["crate_after"])
                self.assertEqual(row["crate_before"]["name"], "obsolete-crate")

    def test_unrelated_candidate_history_fails_with_complete_relationship(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = GitFixture(Path(temp_dir))
            fixture.write("README.md", "old history\n")
            old = fixture.commit("old floor")
            fixture.git("checkout", "--orphan", "unrelated-candidate")
            fixture.git("rm", "-rf", ".")
            fixture.write("UNRELATED.md", "unrelated history\n")
            candidate = fixture.commit("unrelated candidate")
            map_path, policy_path = fixture.authorities(old, [], [], [])

            result, report = self.run_classifier(
                fixture, old, candidate, map_path, policy_path
            )

            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            self.assertEqual(report["summary"]["review_gate"], "fail")
            self.assertFalse(report["relationship"]["old_is_ancestor"])
            self.assertIsNone(report["relationship"]["merge_base"])
            self.assertEqual(report["summary"]["changed_file_count"], 2)
            self.assertEqual(
                [row["path"] for row in report["changed_files"]],
                ["README.md", "UNRELATED.md"],
            )
            self.assertIn(
                "non_descendant_candidate",
                [row["code"] for row in report["diagnostics"]["items"]],
            )

    def test_divergent_and_behind_candidates_fail_with_their_merge_base(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = GitFixture(Path(temp_dir))
            fixture.write("COMMON.md", "common\n")
            base = fixture.commit("common base")
            fixture.write("OLD.md", "old floor branch\n")
            old = fixture.commit("old floor")
            fixture.git("checkout", "-b", "divergent-candidate", base)
            fixture.write("CANDIDATE.md", "candidate branch\n")
            candidate = fixture.commit("divergent candidate")
            map_path, policy_path = fixture.authorities(old, [], [], [])

            divergent_result, divergent = self.run_classifier(
                fixture, old, candidate, map_path, policy_path
            )
            behind_result, behind = self.run_classifier(
                fixture, old, base, map_path, policy_path
            )

            for result, report in (
                (divergent_result, divergent),
                (behind_result, behind),
            ):
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertEqual(report["summary"]["review_gate"], "fail")
                self.assertFalse(report["relationship"]["old_is_ancestor"])
                self.assertEqual(report["relationship"]["merge_base"], base)
                self.assertIn(
                    "non_descendant_candidate",
                    [row["code"] for row in report["diagnostics"]["items"]],
                )
            self.assertEqual(
                [row["path"] for row in divergent["changed_files"]],
                ["CANDIDATE.md", "OLD.md"],
            )
            self.assertEqual(
                [row["path"] for row in behind["changed_files"]], ["OLD.md"]
            )


if __name__ == "__main__":
    unittest.main()
