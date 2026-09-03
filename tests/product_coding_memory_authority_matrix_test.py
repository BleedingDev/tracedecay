#!/usr/bin/env python3
"""Contract tests for the coding-memory authority matrix."""

from __future__ import annotations

import copy
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
MATRIX = REPO / "product/architecture/coding-memory-authority-matrix.json"
CHECKER = REPO / "scripts/product/check-coding-memory-authority-matrix.py"
CORE_CHECKER = REPO / "scripts/product/check-coding-memory-authority-matrix-core.py"
RUNTIME_RS = Path("crates/tracedecay-configuration/src/configuration/runtime.rs")


def load_core_module():
    """Import the core checker (its filename is not a valid module name)."""
    spec = importlib.util.spec_from_file_location("authority_matrix_core", CORE_CHECKER)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    # `@dataclass` resolves annotations through sys.modules, so register first.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


CORE = load_core_module()

# The exact single-writer configuration-authority assertion the gate defends.
RUNTIME_ASSERTION = next(
    marker
    for marker in CORE.SOURCE_MARKERS[str(RUNTIME_RS)]
    if isinstance(marker, CORE.DocAssertion)
)


def mirror_repo_overriding(temp_root: Path, rel: Path, contents: str) -> None:
    """Build a symlink mirror of REPO in which exactly `rel` is replaced.

    Every directory on the way to `rel` becomes a real directory whose other
    children are symlinks back into REPO, so the checker sees a full, valid
    repository that differs only in the one production file under test.
    """
    source = REPO
    destination = temp_root
    destination.mkdir(parents=True, exist_ok=True)
    parts = rel.parts
    for depth, part in enumerate(parts):
        for child in source.iterdir():
            if child.name == part:
                continue
            link = destination / child.name
            if not link.exists():
                link.symlink_to(child)
        source = source / part
        destination = destination / part
        if depth < len(parts) - 1:
            destination.mkdir()
    destination.write_text(contents, encoding="utf-8")


class CodingMemoryAuthorityMatrixTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.document = json.loads(MATRIX.read_text(encoding="utf-8"))

    def run_checker(
        self,
        document: dict[str, Any] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        if document is None:
            matrix_path = MATRIX
            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--matrix",
                    str(matrix_path),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

        with tempfile.TemporaryDirectory() as temp_dir:
            matrix_path = Path(temp_dir) / "matrix.json"
            matrix_path.write_text(
                json.dumps(document, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--matrix",
                    str(matrix_path),
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

    def domain(self, document: dict[str, Any], domain_id: str) -> dict[str, Any]:
        return next(row for row in document["authority_domains"] if row["id"] == domain_id)

    def test_real_repository_matrix_is_valid(self) -> None:
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertTrue(receipt["ok"])
        self.assertEqual(receipt["bead_id"], "tdmem-0104")
        self.assertEqual(receipt["namespace_axes"], 10)
        self.assertEqual(receipt["authority_domains"], 12)
        self.assertEqual(receipt["durable_domains"], 10)
        self.assertEqual(receipt["cross_domain_rules"], 9)
        self.assertEqual(receipt["context_lanes"], 5)

    def test_durable_domain_without_one_writer_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        self.domain(document, "explicit_facts")["canonical_writer"] = None
        self.assert_rejected(document, "must name exactly one canonical writer")

    def test_plural_canonical_writers_are_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        self.domain(document, "session_evidence")["co_writers"] = [
            "TraceDecay",
            "provider",
        ]
        self.assert_rejected(document, "must not define plural or alternate canonical writers")

    def test_native_fact_authority_drift_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        explicit = self.domain(document, "explicit_facts")
        explicit["native_surface_authority"] = "provider_fact_log"
        explicit["canonical_writer"] = "Selected provider fact store"
        self.assert_rejected(document, "explicit_facts must map to native_explicit_fact_log")

    def test_provider_recall_must_remain_advisory(self) -> None:
        document = copy.deepcopy(self.document)
        recall = self.domain(document, "provider_recall_candidates")
        recall["authority_class"] = "canonical"
        recall["provider_semantics"] = "authoritative"
        recall["canonical_writer"] = "provider"
        self.assert_rejected(document, "provider recall must be explicitly advisory_only")

    def test_provider_recall_cannot_gain_source_edit_effect(self) -> None:
        document = copy.deepcopy(self.document)
        recall = self.domain(document, "provider_recall_candidates")
        recall["prohibited_side_effects"] = [
            value
            for value in recall["prohibited_side_effects"]
            if value != "direct source edit"
        ]
        self.assert_rejected(document, "provider recall must prohibit 'direct source edit'")

    def test_final_context_owner_drift_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        self.domain(document, "final_compiled_context")["owner"] = "provider"
        self.assert_rejected(
            document,
            "TraceDecay context compiler must solely own final context assembly",
        )

    def test_missing_provider_namespace_axis_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        document["namespace_axes"] = [
            row for row in document["namespace_axes"] if row["id"] != "provider_id"
        ]
        self.assert_rejected(document, "namespace axes missing")

    def test_namespace_overlap_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        variant = self.domain(document, "worktree_identity")["namespace_variants"][0]
        variant["optional"].append("worktree_id")
        self.assert_rejected(document, "places axes in both required and optional")

    def test_context_lane_precedence_drift_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        document["context_lane_order"][0]["domain"] = "provider_recall_candidates"
        document["context_lane_order"][4]["domain"] = "current_code_truth"
        self.assert_rejected(document, "context lane precedence must be")

    def test_silent_fallback_rule_cannot_be_weakened(self) -> None:
        document = copy.deepcopy(self.document)
        rule = next(
            row for row in document["cross_domain_rules"] if row["id"] == "no_silent_fallback"
        )
        rule["rule"] = "The runtime may choose any available provider."
        self.assert_rejected(
            document,
            "no_silent_fallback rule must reject implicit provider switching",
        )

    def test_core_checker_passes_when_invoked_directly(self) -> None:
        # Regression guard for wrapper/core marker drift: the wrapper used to
        # patch SOURCE_MARKERS at runpy time, so the core script alone could
        # fail (a stale marker) while every test here still passed against
        # the wrapper. Running the core binary directly, unmodified, closes
        # that gap.
        result = subprocess.run(
            [
                "python3",
                str(CORE_CHECKER),
                "--repo",
                str(REPO),
                "--matrix",
                str(MATRIX),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertTrue(receipt["ok"])

    def test_missing_current_source_path_is_rejected(self) -> None:
        document = copy.deepcopy(self.document)
        self.domain(document, "current_code_truth")["source_paths"].append(
            "crates/does-not-exist/src/source.rs"
        )
        self.assert_rejected(document, "references a missing repository path")


REAL_RUNTIME_HEAD = """\
/// Retained project-level control-plane runtime. It owns the one opened
/// transactional store handle and the one application operation facade used by every
/// local transport.
pub struct ProjectConfigurationRuntime {
    target: RuntimeConfigurationTarget,
}
"""

# The genuine production shape: the assertion wraps across `///` lines and is
# attached to the accessor it constrains.
COMPLIANT_RUNTIME = (
    REAL_RUNTIME_HEAD
    + """
impl ProjectConfigurationRuntime {
    /// Immutable routing identity only. Effective values and revisions must be
    /// read from [`Self::client`] so the retained store remains the sole
    /// runtime configuration authority.
    pub fn configuration_target(&self) -> &RuntimeConfigurationTarget {
        &self.target
    }
}
"""
)

# The reviewer's bypass: BOTH old fragments are still present in the file, but
# the accessor now documents a SECOND configuration authority and the
# "retained store remains the sole" wording survives only in an unrelated
# legacy note. A per-fragment substring gate passes this; the invariant is gone.
SPLIT_AUTHORITY_RUNTIME = (
    REAL_RUNTIME_HEAD
    + """
/// Legacy migration shim. Before the split the retained store remains the sole
/// writer of record; this helper only exists for replay.
pub fn legacy_replay_shim() {}

impl ProjectConfigurationRuntime {
    /// Immutable routing identity only. Effective values and revisions may also
    /// be served from the secondary revision cache, so the
    /// runtime configuration authority is intentionally distributed across
    /// both handles.
    pub fn configuration_target(&self) -> &RuntimeConfigurationTarget {
        &self.target
    }
}
"""
)

# The whole sentence is present and contiguous, but detached from any item by a
# blank line, so it documents nothing the compiler or a reader binds it to.
DETACHED_ASSERTION_RUNTIME = (
    REAL_RUNTIME_HEAD
    + """
/// Immutable routing identity only. Effective values and revisions must be
/// read from [`Self::client`] so the retained store remains the sole
/// runtime configuration authority.

impl ProjectConfigurationRuntime {
    pub fn configuration_target(&self) -> &RuntimeConfigurationTarget {
        &self.target
    }
}
"""
)

# The whole sentence is present and attached, but to an unrelated private
# helper rather than to the public accessor whose reads it governs.
MISPLACED_ASSERTION_RUNTIME = (
    REAL_RUNTIME_HEAD
    + """
/// Effective values and revisions must be read from [`Self::client`] so the
/// retained store remains the sole runtime configuration authority.
fn unrelated_private_helper() {}

impl ProjectConfigurationRuntime {
    pub fn configuration_target(&self) -> &RuntimeConfigurationTarget {
        &self.target
    }
}
"""
)


class RuntimeConfigurationAuthorityAssertionTest(unittest.TestCase):
    """The single-writer configuration-authority assertion must stay whole.

    `crates/tracedecay-configuration/src/configuration/runtime.rs` is the production
    evidence that the retained transactional store is the ONLY runtime
    configuration authority. These tests pin the gate's source-evidence check
    so it cannot be reduced back to independent word fragments that a
    split-authority refactor would satisfy.
    """

    def holds(self, source: str) -> bool:
        return CORE.doc_assertion_holds(source, RUNTIME_ASSERTION)

    def run_core_against(self, repo_root: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["python3", str(CORE_CHECKER), "--repo", str(repo_root)],
            check=False,
            capture_output=True,
            text=True,
        )

    def test_marker_is_a_whole_attached_assertion_not_word_fragments(self) -> None:
        self.assertIsInstance(RUNTIME_ASSERTION, CORE.DocAssertion)
        self.assertIn("sole runtime configuration authority", RUNTIME_ASSERTION.phrase)
        self.assertIn("must be read from", RUNTIME_ASSERTION.phrase)
        self.assertEqual(RUNTIME_ASSERTION.item_prefix, "pub fn configuration_target")

    def test_real_production_source_satisfies_the_assertion(self) -> None:
        source = (REPO / RUNTIME_RS).read_text(encoding="utf-8")
        self.assertTrue(self.holds(source))

    def test_line_wrapped_rustdoc_is_accepted(self) -> None:
        self.assertTrue(self.holds(COMPLIANT_RUNTIME))

    def test_split_authority_source_is_rejected(self) -> None:
        # Both legacy fragments are present; the invariant is not.
        self.assertIn("retained store remains the sole", SPLIT_AUTHORITY_RUNTIME)
        self.assertIn("runtime configuration authority", SPLIT_AUTHORITY_RUNTIME)
        self.assertFalse(self.holds(SPLIT_AUTHORITY_RUNTIME))

    def test_detached_assertion_is_rejected(self) -> None:
        self.assertFalse(self.holds(DETACHED_ASSERTION_RUNTIME))

    def test_assertion_attached_to_the_wrong_item_is_rejected(self) -> None:
        self.assertFalse(self.holds(MISPLACED_ASSERTION_RUNTIME))

    def test_reordered_clauses_are_rejected(self) -> None:
        reordered = (
            REAL_RUNTIME_HEAD
            + """
impl ProjectConfigurationRuntime {
    /// The retained store remains the sole runtime configuration authority,
    /// although effective values and revisions must be read from the cache
    /// rather than from [`Self::client`].
    pub fn configuration_target(&self) -> &RuntimeConfigurationTarget {
        &self.target
    }
}
"""
        )
        self.assertFalse(self.holds(reordered))

    def test_gate_rejects_a_split_authority_repository(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir) / "repo"
            mirror_repo_overriding(repo_root, RUNTIME_RS, SPLIT_AUTHORITY_RUNTIME)
            result = self.run_core_against(repo_root)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertFalse(receipt["ok"])
        joined = "\n".join(receipt["errors"])
        self.assertIn("configuration/runtime.rs", joined)
        self.assertIn("rustdoc assertion on `pub fn configuration_target`", joined)

    def test_gate_accepts_an_unmodified_mirrored_repository(self) -> None:
        # Positive control: the mirror itself is faithful, so the rejection
        # above is caused by the substituted source and nothing else.
        real = (REPO / RUNTIME_RS).read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir) / "repo"
            mirror_repo_overriding(repo_root, RUNTIME_RS, real)
            result = self.run_core_against(repo_root)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(json.loads(result.stdout)["ok"])


if __name__ == "__main__":
    unittest.main()
