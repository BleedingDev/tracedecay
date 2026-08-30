#!/usr/bin/env python3
"""Mutation tests for the substantive NCM/Biomem surface audit."""

from __future__ import annotations

import copy
import json
import os
import runpy
import subprocess
import sys
import tempfile
import unittest
import venv
from pathlib import Path
from typing import Any, Callable
from unittest import mock


REPO = Path(__file__).resolve().parents[1]
CHECKER = REPO / "scripts/product/check-ncm-surface-audit.py"
PROBE = REPO / "scripts/product/probe-ncm-surface.py"
REGISTRY = REPO / "product/contracts/memory-provider-v1/provider-registry-contract.json"
AUDIT = (
    REPO
    / "crates/tracedecay-memory-provider-ncm/audits"
    / "tdmem-0701-capability-matrix.json"
)


def capability_requirements(registry: dict[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    for requirement in ("mandatory", "optional"):
        for row in registry["capability_registry"][requirement]:
            result[row["id"]] = requirement
    return result


def valid_audit(registry: dict[str, Any]) -> dict[str, Any]:
    requirements = capability_requirements(registry)
    matrix = []
    for capability_id, requirement in sorted(requirements.items()):
        classification = {
            "provider.health.v1": "adaptable",
            "observation.accept.v1": "blocking",
            "recall.query.v1": "adaptable",
        }.get(capability_id, "unsupported")
        matrix.append(
            {
                "capability_id": capability_id,
                "requirement": requirement,
                "classification": classification,
                "evidence_ids": {
                    "provider.health.v1": ["source-health"],
                    "observation.accept.v1": ["source-observe"],
                    "recall.query.v1": ["source-search"],
                }.get(capability_id, []),
                "adapter_requirements": [],
                "ncm_change_required": capability_id == "observation.accept.v1",
            }
        )
    return {
        "schema_version": 1,
        "bead_id": "tdmem-0701",
        "capability_matrix": matrix,
        "mandatory_operations": [
            {
                "operation": "health",
                "capability_id": "provider.health.v1",
                "classification": "adaptable",
                "mandatory": True,
                "licensed_primitive": "status",
                "conformance_gaps": ["loaded state identity is not proven"],
                "adapter_requirements": ["validate loaded state identity"],
                "ncm_change_required": False,
                "evidence_ids": ["source-health"],
            },
            {
                "operation": "observe",
                "capability_id": "observation.accept.v1",
                "classification": "blocking",
                "mandatory": True,
                "licensed_primitive": "store_record",
                "conformance_gaps": ["identical retries reinforce state"],
                "adapter_requirements": ["preserve idempotency identity"],
                "ncm_change_required": True,
                "evidence_ids": ["source-observe"],
            },
            {
                "operation": "recall",
                "capability_id": "recall.query.v1",
                "classification": "adaptable",
                "mandatory": True,
                "licensed_primitive": "side-effect-free search",
                "conformance_gaps": ["exact scope is not native"],
                "adapter_requirements": ["enforce opaque exact-scope namespace"],
                "ncm_change_required": False,
                "evidence_ids": ["source-search"],
            },
        ],
        "persistence": {
            "state_identity": {
                "observed": "no provider-contract state identity is exposed",
                "production_compatible": False,
            },
            "compatibility": [
                {
                    "dimension": "state format",
                    "observed": "version stamp only",
                    "required": "explicit schema and configuration compatibility",
                }
            ],
            "load_failure_policy": "fail closed: reject readiness; never silently start fresh",
            "implicit_reset_allowed": False,
            "observed_outcomes": [
                {
                    "outcome": "load failures are swallowed by the licensed surface",
                    "evidence_ids": ["source-load"],
                }
            ],
        },
        "lifecycle": {
            "readiness": "ready is reported without a verified loaded-state identity",
            "evidence_ids": ["source-health", "source-load"],
        },
        "threading_and_cancellation": {
            "threading_observations": [
                {
                    "outcome": "same-instance calls serialize under one lock",
                    "evidence_ids": ["probe-threading"],
                }
            ],
            "cancellation_observations": [
                {
                    "outcome": "client cancellation does not stop running daemon work",
                    "evidence_ids": ["probe-cancellation"],
                }
            ],
        },
        "production_gate": {
            "status": "blocked",
            "fake_readiness_allowed": False,
            "state_identity_required": True,
            "blockers": [
                {
                    "id": "exact-scope",
                    "title": "enforce exact-scope namespace",
                    "owner_boundary": "adapter",
                    "evidence_ids": ["source-search"],
                },
                {
                    "id": "atomic-persistence",
                    "title": "make state publication crash-safe",
                    "owner_boundary": "biomem",
                    "evidence_ids": ["source-load"],
                },
            ],
        },
        "authority_boundary": {
            "exclusions": {
                "git_and_repository_resolution": False,
                "codebase_navigation": False,
                "tracedecay_storage": False,
                "canonical_tracedecay_authority": False,
            },
            "ncm_assigned_authorities": [
                "admitted memory records",
                "provider-local latent state",
            ],
            "tracedecay_retained_authorities": [
                "Git evidence and repository/worktree identity",
                "code navigation and current-code truth",
                "TraceDecay storage and Native facts",
            ],
        },
        "evidence": [
            {
                "id": "source-health",
                "kind": "source_symbol",
                "path": "src/memory_module/protocol.py",
                "symbol": "CommandHandler.status",
            },
            {
                "id": "source-observe",
                "kind": "source_symbol",
                "path": "src/memory_module/text_memory.py",
                "symbol": "TextMemory.store_record",
            },
            {
                "id": "source-search",
                "kind": "source_symbol",
                "path": "src/memory_module/text_memory.py",
                "symbol": "TextMemory.search",
            },
            {
                "id": "source-load",
                "kind": "source_symbol",
                "path": "src/memory_module/text_memory.py",
                "symbol": "TextMemory.load",
            },
            {
                "id": "probe-threading",
                "kind": "measured_probe",
                "probe_id": "same-instance-threading",
                "observed": {
                    "parallel_requests": 4,
                    "attempted": 4,
                    "completed": 4,
                    "max_active": 1,
                    "serialized": True,
                },
            },
            {
                "id": "probe-cancellation",
                "kind": "measured_probe",
                "probe_id": "mid-flight-cancellation",
                "observed": {
                    "attempted": 1,
                    "cancelled": 0,
                    "effect_unknown": 1,
                },
            },
        ],
    }


class NcmSurfaceAuditTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.registry = json.loads(REGISTRY.read_text(encoding="utf-8"))
        cls.fixture = json.loads(AUDIT.read_text(encoding="utf-8"))

    def run_checker(
        self,
        audit: dict[str, Any],
        registry: dict[str, Any] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            audit_path = temp / "audit.json"
            registry_path = temp / "registry.json"
            audit_path.write_text(
                json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
            registry_path.write_text(
                json.dumps(registry or self.registry, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            return subprocess.run(
                [
                    "python3",
                    "-S",
                    str(CHECKER),
                    "--repo",
                    str(temp),
                    "--audit",
                    str(audit_path),
                    "--registry",
                    str(registry_path),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

    def assert_rejected(
        self, marker: str, mutate: Callable[[dict[str, Any]], None]
    ) -> None:
        baseline = self.run_checker(copy.deepcopy(self.fixture))
        self.assertEqual(
            baseline.returncode, 0, "invalid mutation baseline: " + baseline.stderr
        )
        audit = copy.deepcopy(self.fixture)
        mutate(audit)
        result = self.run_checker(audit)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(marker, result.stderr)

    def capability(self, audit: dict[str, Any], capability_id: str) -> dict[str, Any]:
        return next(
            row
            for row in audit["capability_matrix"]
            if row["capability_id"] == capability_id
        )

    def surface_evidence(self, audit: dict[str, Any]) -> dict[str, Any]:
        return next(row for row in audit["evidence"] if row["id"] == "probe-surface")

    def measurement(self, audit: dict[str, Any], probe_id: str) -> dict[str, Any]:
        return next(
            row
            for row in self.surface_evidence(audit)["observed"]["measurements"]
            if row["probe_id"] == probe_id
        )

    def add_ncm_authority(self, audit: dict[str, Any], authority: str) -> None:
        boundary = audit["authority_boundary"]
        boundary["ncm_assigned_authorities"] = [boundary["ncm_role"], authority]

    def test_complete_substantive_fixture_passes(self) -> None:
        result = self.run_checker(copy.deepcopy(self.fixture))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("15 capabilities", result.stdout)

    def test_checked_in_audit_passes(self) -> None:
        result = subprocess.run(
            [
                "python3",
                "-S",
                str(CHECKER),
                "--repo",
                str(REPO),
                "--audit",
                str(AUDIT),
                "--registry",
                str(REGISTRY),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_missing_and_duplicate_capability_rows_are_rejected(self) -> None:
        self.assert_rejected(
            "missing canonical capabilities",
            lambda audit: audit["capability_matrix"].pop(),
        )

        def duplicate(audit: dict[str, Any]) -> None:
            audit["capability_matrix"].append(
                copy.deepcopy(audit["capability_matrix"][0])
            )

        self.assert_rejected("more than once", duplicate)

    def test_unsupported_by_name_assumption_is_rejected(self) -> None:
        def mutate(audit: dict[str, Any]) -> None:
            row = self.capability(audit, "recall.query.v1")
            row["capability_id"] = "biomem.search.v1"

        self.assert_rejected("non-canonical capabilities", mutate)

    def test_mandatory_operation_cannot_be_unsupported(self) -> None:
        def mutate(audit: dict[str, Any]) -> None:
            self.capability(audit, "observation.accept.v1")["classification"] = (
                "unsupported"
            )
            audit["mandatory_operations"][1]["classification"] = "unsupported"

        self.assert_rejected("cannot be unsupported", mutate)

    def test_supported_or_adaptable_claim_requires_evidence(self) -> None:
        def mutate(audit: dict[str, Any]) -> None:
            self.capability(audit, "provider.health.v1")["evidence_ids"] = []

        self.assert_rejected("must cite source-symbol or measured-probe", mutate)

    def test_supported_or_adaptable_claim_requires_licensed_evidence(self) -> None:
        def mutate(audit: dict[str, Any]) -> None:
            row = self.capability(audit, "provider.health.v1")
            row["evidence_ids"] = ["contract-only"]
            audit["evidence"].append(
                {
                    "id": "contract-only",
                    "kind": "source_symbol",
                    "path": "product/contracts/provider.json",
                    "symbol": "/health",
                }
            )

        self.assert_rejected("licensed surface or a measured probe", mutate)

    def test_recall_cannot_use_mutating_retrieve(self) -> None:
        def mutate(audit: dict[str, Any]) -> None:
            audit["mandatory_operations"][2]["licensed_primitive"] = "retrieve"

        self.assert_rejected("side-effect-free search", mutate)

    def test_unmeasured_threading_and_cancellation_claims_are_rejected(self) -> None:
        def mutate_threading(audit: dict[str, Any]) -> None:
            audit["threading_and_cancellation"]["threading_observations"][0][
                "evidence_ids"
            ] = ["source-health"]

        self.assert_rejected("must cite only probe-surface", mutate_threading)

        def mutate_cancellation(audit: dict[str, Any]) -> None:
            audit["threading_and_cancellation"]["cancellation_observations"][0][
                "evidence_ids"
            ] = ["source-health"]

        self.assert_rejected("must cite only probe-surface", mutate_cancellation)

        def cross_swap(audit: dict[str, Any]) -> None:
            audit["threading_and_cancellation"]["cancellation_observations"][0][
                "evidence_ids"
            ] = ["probe-threading"]

        self.assert_rejected("must cite only probe-surface", cross_swap)

    def test_fake_readiness_is_rejected(self) -> None:
        self.assert_rejected(
            "fake_readiness_allowed must be false",
            lambda audit: audit["production_gate"].__setitem__(
                "fake_readiness_allowed", True
            ),
        )
        self.assert_rejected(
            "current unverified readiness signal",
            lambda audit: audit["lifecycle"].__setitem__(
                "readiness", "the provider is fully ready"
            ),
        )

    def test_implicit_reset_is_rejected(self) -> None:
        self.assert_rejected(
            "implicit_reset_allowed must be false",
            lambda audit: audit["persistence"].__setitem__(
                "implicit_reset_allowed", True
            ),
        )
        self.assert_rejected(
            "must require fail-closed rejection",
            lambda audit: audit["persistence"].__setitem__(
                "load_failure_policy", "silently start fresh and report ready"
            ),
        )

    def test_absent_state_identity_is_rejected(self) -> None:
        self.assert_rejected(
            "state_identity must be an object",
            lambda audit: audit["persistence"].pop("state_identity"),
        )
        self.assert_rejected(
            "state_identity_required must be true",
            lambda audit: audit["production_gate"].__setitem__(
                "state_identity_required", False
            ),
        )

    def test_blockers_must_split_adapter_and_biomem_ownership(self) -> None:
        def mutate(audit: dict[str, Any]) -> None:
            for blocker in audit["production_gate"]["blockers"]:
                blocker["owner_boundary"] = "adapter"

        self.assert_rejected("split between adapter and biomem", mutate)

        def swap(audit: dict[str, Any]) -> None:
            next(
                blocker
                for blocker in audit["production_gate"]["blockers"]
                if blocker["id"] == "exact-scope-isolation"
            )["owner_boundary"] = "biomem"

        self.assert_rejected("owner_boundary must be adapter", swap)

    def test_ncm_cannot_receive_git_navigation_or_storage_authority(self) -> None:
        for forbidden in (
            "Git repository resolution",
            "code navigation",
            "TraceDecay storage",
        ):
            with self.subTest(forbidden=forbidden):
                self.assert_rejected(
                    "NCM must not own",
                    lambda audit, value=forbidden: self.add_ncm_authority(audit, value),
                )

        for synonym in (
            "source control checkout discovery",
            "symbol lookup for current code",
            "Native facts",
        ):
            with self.subTest(synonym=synonym):
                self.assert_rejected(
                    "NCM must not own",
                    lambda audit, value=synonym: self.add_ncm_authority(audit, value),
                )

    def test_benign_digital_authority_is_not_a_git_false_positive(self) -> None:
        audit = copy.deepcopy(self.fixture)
        self.add_ncm_authority(audit, "digital memory scoring")
        result = self.run_checker(audit)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_authority_exclusions_must_be_explicit(self) -> None:
        self.assert_rejected(
            "exclusions.tracedecay_storage must be false",
            lambda audit: audit["authority_boundary"]["exclusions"].pop(
                "tracedecay_storage"
            ),
        )

    def test_probe_identity_revision_and_exact_measurement_ids_are_enforced(
        self,
    ) -> None:
        self.assert_rejected(
            "probe_id must be 'tracedecay.ncm.surface-probe.v2'",
            lambda audit: self.surface_evidence(audit).__setitem__(
                "probe_id", "tracedecay.ncm.surface-probe.v1"
            ),
        )
        self.assert_rejected(
            "observed.probe_id must be",
            lambda audit: self.surface_evidence(audit)["observed"].__setitem__(
                "probe_id", "changed"
            ),
        )
        self.assert_rejected(
            "measurement_ids must be exactly",
            lambda audit: self.surface_evidence(audit)["measurement_ids"].pop(),
        )
        self.assert_rejected(
            "exact probe sequence",
            lambda audit: self.surface_evidence(audit)["observed"][
                "measurements"
            ].pop(),
        )

        def duplicate_probe_id(audit: dict[str, Any]) -> None:
            measurements = self.surface_evidence(audit)["observed"]["measurements"]
            measurements[-1]["probe_id"] = measurements[0]["probe_id"]

        self.assert_rejected("more than once", duplicate_probe_id)

        def extra_measured_probe(audit: dict[str, Any]) -> None:
            audit["evidence"].append(
                {
                    "id": "arbitrary-probe",
                    "kind": "measured_probe",
                    "probe_id": "arbitrary",
                    "observed": {"result": True},
                }
            )

        self.assert_rejected(
            "must contain exactly the pinned revision", extra_measured_probe
        )

        def orphan_source(audit: dict[str, Any]) -> None:
            audit["evidence"].append(
                {
                    "id": "orphan-source",
                    "kind": "source_symbol",
                    "repository": "tracedecay worktree",
                    "path": "product/contracts/orphan.json",
                    "symbol": "/orphan",
                }
            )

        self.assert_rejected("unreferenced evidence", orphan_source)
        self.assert_rejected(
            "immutable Biomem revision",
            lambda audit: audit["audit_subject"].__setitem__("revision", "0" * 40),
        )

    def test_measurement_envelopes_and_summary_are_typed_and_conserving(self) -> None:
        self.assert_rejected(
            "elapsed_ms must be a non-negative integer",
            lambda audit: self.measurement(audit, "python_syntax").__setitem__(
                "elapsed_ms", True
            ),
        )
        self.assert_rejected(
            "diagnostic must be null for measured evidence",
            lambda audit: self.measurement(audit, "python_syntax").__setitem__(
                "diagnostic", "fabricated"
            ),
        )
        self.assert_rejected(
            "summary must be exactly",
            lambda audit: self.surface_evidence(audit)["observed"][
                "summary"
            ].__setitem__("total", 999),
        )

    def test_http_parallel_matrix_rejects_impossible_or_stale_results(self) -> None:
        def corrupt_counts(audit: dict[str, Any]) -> None:
            row = self.measurement(audit, "http_parallel_requests")["observed"][
                "matrix"
            ][-1]
            row["completed"] = 7
            row["errors"] = 1

        self.assert_rejected("completed must be 8", corrupt_counts)

        def impossible_active(audit: dict[str, Any]) -> None:
            self.measurement(audit, "http_parallel_requests")["observed"]["matrix"][-1][
                "max_active"
            ] = 9

        self.assert_rejected("max_active must be 8", impossible_active)

        def boolean_count(audit: dict[str, Any]) -> None:
            self.measurement(audit, "http_parallel_requests")["observed"]["matrix"][0][
                "attempted"
            ] = True

        self.assert_rejected("must be a non-negative integer", boolean_count)

        def stale_projection(audit: dict[str, Any]) -> None:
            audit["threading_and_cancellation"]["observed_results"][
                "transport_concurrency"
            ]["matrix"][0]["elapsed_ms"] += 1

        self.assert_rejected("transport projection is stale", stale_projection)

    def test_disconnect_cancellation_polarity_and_projection_are_enforced(self) -> None:
        for field, value in (
            ("timeout_seen", False),
            ("server_completed_after_disconnect", False),
            ("server_observed_cancellation", True),
            ("handler_started", 2),
            ("handler_completed", 0),
            ("handler_cancelled", 1),
        ):
            with self.subTest(field=field):
                self.assert_rejected(
                    f"client_disconnect {field} must be",
                    lambda audit, key=field, replacement=value: self.measurement(
                        audit, "client_disconnect"
                    )["observed"].__setitem__(key, replacement),
                )

        def stale_disconnect(audit: dict[str, Any]) -> None:
            audit["threading_and_cancellation"]["observed_results"][
                "client_disconnect"
            ]["elapsed_ms"] += 1

        self.assert_rejected("disconnect projection is stale", stale_disconnect)

    def test_exact_four_production_blockers_are_required(self) -> None:
        blocker_ids = [
            "state-readiness",
            "exact-scope-isolation",
            "server-cancellation-effect-reconciliation",
            "crash-safe-persistence",
        ]
        for blocker_id in blocker_ids:
            with self.subTest(blocker_id=blocker_id):
                self.assert_rejected(
                    "exactly the declared four IDs",
                    lambda audit, target=blocker_id: audit["production_gate"][
                        "blockers"
                    ].__setitem__(
                        slice(None),
                        [
                            row
                            for row in audit["production_gate"]["blockers"]
                            if row["id"] != target
                        ],
                    ),
                )

        def add_blocker(audit: dict[str, Any]) -> None:
            audit["production_gate"]["blockers"].append(
                {
                    "id": "generic-placeholder",
                    "title": "generic placeholder",
                    "owner_boundary": "biomem",
                    "evidence_ids": ["biomem-load"],
                }
            )

        self.assert_rejected("exactly the declared four IDs", add_blocker)

    def test_structured_readiness_and_classification_cannot_be_upgraded_by_prose(
        self,
    ) -> None:
        self.assert_rejected(
            "required_load_failure_behavior",
            lambda audit: audit["persistence"].__setitem__(
                "required_load_failure_behavior", "continue_ready"
            ),
        )
        self.assert_rejected(
            "readiness_verification",
            lambda audit: audit["lifecycle"].__setitem__(
                "readiness_verification", "verified"
            ),
        )

        def upgrade_supported(audit: dict[str, Any]) -> None:
            self.capability(audit, "provider.health.v1")["classification"] = "supported"
            next(
                row
                for row in audit["mandatory_operations"]
                if row["capability_id"] == "provider.health.v1"
            )["classification"] = "supported"

        self.assert_rejected("cannot be supported while", upgrade_supported)

    def test_typed_measured_core_cancellation_is_accepted_in_auto_mode(self) -> None:
        audit = copy.deepcopy(self.fixture)
        surface = self.surface_evidence(audit)
        self.assertEqual(surface["observed"]["input"]["core_mode"], "auto")
        self.assertEqual(
            surface["observed"]["summary"],
            {"blocked": 0, "measured": 13, "unsupported": 0, "total": 13},
        )
        self.assertTrue(
            all(
                measurement["availability"] == "measured"
                for measurement in surface["observed"]["measurements"]
            )
        )
        result = self.run_checker(audit)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

        self.assert_rejected(
            "settlement-after-timeout requires a timeout",
            lambda mutated: self.measurement(
                mutated, "cancellation_deadline_observation"
            )["observed"].__setitem__("caller_wait_timeout_seen", False),
        )

        def impossible_core_parallel(mutated: dict[str, Any]) -> None:
            self.measurement(mutated, "core_parallel_operations")["observed"][
                "read_matrix"
            ][-1]["max_callers_inflight"] = 9

        self.assert_rejected(
            "max_callers_inflight is implausible", impossible_core_parallel
        )

    def test_probe_v2_schema_and_core_settlement_semantics(self) -> None:
        namespace = runpy.run_path(str(PROBE))
        self.assertEqual(namespace["SCHEMA_VERSION"], 2)
        self.assertEqual(namespace["PROBE_ID"], "tracedecay.ncm.surface-probe.v2")
        self.assertEqual(
            tuple(namespace["PROBE_SEQUENCE"]),
            tuple(namespace["SURFACE_PROBES"]) + tuple(namespace["CORE_PROBES"]),
        )

        measurement = namespace["measurement"]
        probe_error = namespace["ProbeError"]
        with self.assertRaises(probe_error):
            measurement(
                "python_syntax",
                "measured",
                claim_scope="immutable_biomem_python_source",
                expectation="typed",
                observed={"files_checked": 1},
                elapsed_ms=-1,
            )

        class ImmediateMemory:
            def search(self, _query: str, _limit: int, _source: str) -> list[Any]:
                return []

        cancellation = namespace["_core_cancel"](ImmediateMemory(), 1)
        self.assertTrue(cancellation["normal_return_observed"])
        self.assertFalse(cancellation["error_observed"])
        self.assertNotIn("operation_completed_after_timeout", cancellation)
        self.assertNotIn("provider_cancellation_observation", cancellation)

    def test_core_worker_uses_selected_venv_without_ambient_pythonpath(self) -> None:
        namespace = runpy.run_path(str(PROBE))
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            venv_root = temp / "selected-venv"
            venv.EnvBuilder(with_pip=False, clear=True).create(venv_root)
            interpreter = venv_root / (
                "Scripts/python.exe" if os.name == "nt" else "bin/python"
            )
            purelib_result = subprocess.run(
                [
                    str(interpreter),
                    "-s",
                    "-c",
                    "import sysconfig; print(sysconfig.get_path('purelib'))",
                ],
                check=True,
                capture_output=True,
                text=True,
                env=namespace["minimal_child_environment"](),
            )
            purelib = Path(purelib_result.stdout.strip())
            purelib.mkdir(parents=True, exist_ok=True)
            (purelib / "tdmem_0701_selected_venv_8d357e.py").write_text(
                'ORIGIN = "selected-venv"\n', encoding="utf-8"
            )

            ambient = temp / "ambient-pythonpath"
            ambient.mkdir()
            (ambient / "tdmem_0701_ambient_only_8d357e.py").write_text(
                'ORIGIN = "ambient-pythonpath"\n', encoding="utf-8"
            )
            source_root = temp / "source"
            module_root = source_root / "src/memory_module"
            module_root.mkdir(parents=True)
            (module_root / "__init__.py").write_text(
                '__version__ = "test"\n', encoding="utf-8"
            )
            (module_root / "text_memory.py").write_text(
                "import importlib.util\n"
                "import os\n"
                "import socket\n"
                "from pathlib import Path\n"
                "from tdmem_0701_selected_venv_8d357e import ORIGIN\n"
                "hub = Path(os.environ['HF_HUB_CACHE'])\n"
                "snapshot = hub / 'models--sentence-transformers--probe-model' / 'snapshots' / 'pinned' / 'config.json'\n"
                "try:\n"
                "    socket.create_connection(('example.com', 443), timeout=0.01)\n"
                "    network_denied = False\n"
                "except OSError:\n"
                "    network_denied = True\n"
                "raise RuntimeError(\n"
                "    f'selected={ORIGIN};'\n"
                "    f'ambient={importlib.util.find_spec(\"tdmem_0701_ambient_only_8d357e\") is not None};'\n"
                "    f'pythonpath={os.environ.get(\"PYTHONPATH\")};'\n"
                "    f'socket={socket.socket.__name__};'\n"
                "    f'model_visible={snapshot.is_file()};'\n"
                '    f\'hub_aliases_equal={os.environ.get("HF_HUB_CACHE") == os.environ.get("HUGGINGFACE_HUB_CACHE")};\'\n'
                "    f'sentence_transformers_home={os.environ.get(\"SENTENCE_TRANSFORMERS_HOME\")};'\n"
                "    f'offline={os.environ.get(\"HF_HUB_OFFLINE\")};'\n"
                "    f'network_denied={network_denied}'\n"
                ")\n",
                encoding="utf-8",
            )
            methods = {
                method: ["self"]
                for requirements in namespace["CORE_REQUIREMENTS"].values()
                for method in requirements
            }
            state_root = temp / "state"
            state_root.mkdir()
            model_cache = temp / "model-cache"
            model_cache.mkdir()
            snapshot = (
                model_cache
                / "hub/models--sentence-transformers--probe-model/snapshots/pinned/config.json"
            )
            snapshot.parent.mkdir(parents=True)
            snapshot.write_text("{}\n", encoding="utf-8")

            with mock.patch.dict(os.environ, {"PYTHONPATH": str(ambient)}, clear=False):
                measurements = namespace["run_core_child"](
                    source_root,
                    state_root,
                    model_cache,
                    methods,
                    timeout_seconds=10,
                    max_recall_results=1,
                    replacements=(),
                    interpreter=interpreter,
                )

        self.assertEqual(len(measurements), len(namespace["CORE_PROBES"]))
        self.assertTrue(all(row["availability"] == "blocked" for row in measurements))
        for row in measurements:
            diagnostic = row["diagnostic"]
            self.assertIn("selected=selected-venv", diagnostic)
            self.assertIn("ambient=False", diagnostic)
            self.assertIn("pythonpath=None", diagnostic)
            self.assertIn("socket=GuardedSocket", diagnostic)
            self.assertIn("model_visible=True", diagnostic)
            self.assertIn("hub_aliases_equal=True", diagnostic)
            self.assertIn("sentence_transformers_home=None", diagnostic)
            self.assertIn("offline=1", diagnostic)
            self.assertIn("network_denied=True", diagnostic)

    def test_core_worker_emits_canonical_ids_after_running_cancellation_last(
        self,
    ) -> None:
        namespace = runpy.run_path(str(PROBE))
        expected = tuple(namespace["CORE_PROBES"])
        cancellation = "cancellation_deadline_observation"
        execution_order = tuple(
            probe_id for probe_id in expected if probe_id != cancellation
        ) + (cancellation,)
        self.assertNotEqual(execution_order, expected)

        canonical = namespace["canonical_core_measurements"](
            [{"probe_id": probe_id} for probe_id in execution_order]
        )
        self.assertEqual(
            tuple(measurement["probe_id"] for measurement in canonical), expected
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            source_root = temp / "source"
            module_root = source_root / "src/memory_module"
            module_root.mkdir(parents=True)
            (module_root / "__init__.py").write_text(
                '__version__ = "test"\n', encoding="utf-8"
            )
            (module_root / "text_memory.py").write_text(
                "from pathlib import Path\n"
                "class MemoryConfig:\n"
                "    def __init__(self, **values): self.values = values\n"
                "class TextMemory:\n"
                "    def __init__(self, config, state_file, device, auto_load):\n"
                "        self.state_file = state_file\n"
                "        self.records = {}\n"
                "    def load(self): return None\n"
                "    def get_stats(self):\n"
                "        return {'writes': len(self.records), 'device': 'cpu'}\n"
                "    def store_record(self, key, value, memory_id=None, provenance=None):\n"
                "        self.records[memory_id] = {\n"
                "            'memory_id': memory_id, 'key': key, 'value': value,\n"
                "            'source': 'fake', 'provenance': provenance}\n"
                "        return {'index': len(self.records) - 1}\n"
                "    def list_memories(self, source='both', limit=64):\n"
                "        return list(self.records.values())[:limit]\n"
                "    def search(self, query, top_k=10, source='both'):\n"
                "        return self.list_memories(source, top_k)\n"
                "    def save(self, path): Path(path).write_text('state')\n"
                "    def restore(self, path): raise ValueError('incompatible')\n",
                encoding="utf-8",
            )
            state_root = temp / "state"
            state_root.mkdir()
            model_cache = temp / "model-cache"
            model_cache.mkdir()
            config = {
                "source_root": str(source_root),
                "state_root": str(state_root),
                "model_cache": str(model_cache),
                "max_recall_results": 2,
            }
            child = subprocess.run(
                [str(sys.executable), "-s", str(PROBE), "--internal-core-worker"],
                input=json.dumps(config),
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
                env=namespace["minimal_child_environment"](),
                cwd=source_root,
            )
            self.assertEqual(child.returncode, 0, child.stdout + child.stderr)
            marker = "__TRACEDECAY_NCM_PROBE_JSON__"
            payload_line = next(
                line[len(marker) :]
                for line in child.stdout.splitlines()
                if line.startswith(marker)
            )
            payload = json.loads(payload_line)
            self.assertTrue(payload["initialized"], payload)
            self.assertEqual(
                tuple(row["probe_id"] for row in payload["measurements"]), expected
            )

        malformed = execution_order[:-1] + (execution_order[0],)
        diagnostic = namespace["core_probe_id_diagnostic"](
            "model-backed child measurement ID sequence mismatch", malformed
        )
        self.assertLessEqual(len(diagnostic), namespace["MAX_DIAGNOSTIC_CHARS"])
        self.assertIn('"duplicates":["health_load_state_identity"]', diagnostic)
        self.assertIn('"missing":["cancellation_deadline_observation"]', diagnostic)
        self.assertIn('"received"', diagnostic)

    def test_mandatory_operation_mapping_tracks_registry(self) -> None:
        registry = copy.deepcopy(self.registry)
        extra = copy.deepcopy(registry["capability_registry"]["optional"][0])
        extra["id"] = "new.mandatory.v1"
        extra["requirement"] = "mandatory"
        registry["capability_registry"]["mandatory"].append(extra)
        result = self.run_checker(copy.deepcopy(self.fixture), registry)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("mandatory operation mapping is out of sync", result.stderr)


if __name__ == "__main__":
    unittest.main()
